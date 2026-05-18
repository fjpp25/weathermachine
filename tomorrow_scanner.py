"""
tomorrow_scanner.py
-------------------
Rolling HIGHT T-bracket scanner. Monitors the next day's market for each
city in parallel with the main pipeline, using market convergence — not
a time window — to determine which date to act on.

State machine per city
----------------------
Each city tracks a single "next date": the market currently being watched
for a T-bracket signal. This advances when today's market converges.

  Before today converges:   next_date = tomorrow (calendar day + 1)
  After  today converges:   next_date = day after tomorrow (calendar day + 2)

The transition happens automatically in run_scan() the first time a
poll detects today's market as fully settled.

Startup (initialise())
----------------------
Called once before the main loop. Derives state from live data:

  1. Fetch open portfolio positions. Any HIGHT position on a future-dated
     ticker means that (city, date) pair has already been entered — mark
     it in _fired_signals and record the active next_date from the ticker.

  2. For cities not covered by open positions, fetch today's HIGH market
     live prices and call _compute_next_date() to determine the correct
     next_date based on convergence state.

Signal conditions  (backtest HIGHT data, Apr 6-17 2026, 20 cities)
-------------------------------------------------------------------
  No >= 0.80, other T < 0.60  →  T win 100.0%,  adj-B win 96.3%,  n=56
  No >= 0.80, other T < 0.65  →  T win  98.5%,  adj-B win 95.2%,  n=67
"""

from __future__ import annotations

from datetime import timedelta, datetime
from zoneinfo import ZoneInfo
from typing import Optional

from cities import TRADING_CITIES as _CITY_REGISTRY

# ---------------------------------------------------------------------------
# Tunable parameters
# ---------------------------------------------------------------------------

NO_TRIGGER            = 0.80   # T bracket No price that fires the signal
NO_CONVERGE_THRESHOLD = 0.60   # opposing T must be BELOW this to confirm signal
SETTLED_THRESHOLD     = 0.97   # max(yes, no) >= this → bracket is settled

# Order sizing for tomorrow scanner signals.
# Backtest: 100% WR on T bracket (n=56), 96.3% WR on adjacent B bracket.
# Both brackets are entered on each signal.
TOMORROW_CONTRACTS    = 3      # contracts per bracket (T and adj-B each get this)
NO_MAX_ENTRY          = 0.94   # raised from 0.92 — 100% WR backtest on dismissed-T and gradient

# ---------------------------------------------------------------------------
# Sweep signal parameters
# ---------------------------------------------------------------------------
# Per-city minimum No-price floor derived from backtest (Apr–May 2026).
# Only cities with a clean 100% WR floor are included; others are excluded.
# Floor of 0.75 raised to 0.80 across the board: drops 10 trades but
# eliminates the lowest-confidence entries. Seattle (0.91) is provisional
# — only 3 observations.
#
# Excluded cities (no clean floor): Austin, Boston, Dallas, Houston,
#   Miami, New Orleans, San Antonio.
SWEEP_FLOORS: dict[str, float] = {
    "Philadelphia":  0.80,
    "Las Vegas":     0.80,
    "Phoenix":       0.80,
    "San Francisco": 0.85,
    "Minneapolis":   0.88,
    "New York":      0.88,
    "Chicago":       0.90,
    "Los Angeles":   0.90,
    "Atlanta":       0.91,
    "Denver":        0.91,
    "Seattle":       0.91,
    "Washington DC": 0.91,
    "Oklahoma City": 0.94,
}
SWEEP_CEILING   = 0.97   # brackets at/above this are effectively settled — skip
SWEEP_CONTRACTS = 3      # contracts per bracket

# ---------------------------------------------------------------------------
# Per-city state
# ---------------------------------------------------------------------------

# { city: market_date_str }  — the date currently being watched for a signal
# e.g. { "Chicago": "26APR20" }
_active_next_date: dict[str, str] = {}

# (city, market_date_str) pairs already entered — never re-enter
_fired_signals: set[tuple[str, str]] = set()

# Individual tickers already swept this session — prevents re-entry on the
# same bracket across multiple poll cycles. Keyed on ticker string directly.
_sweep_entered: set[str] = set()


# ---------------------------------------------------------------------------
# Date helpers
# ---------------------------------------------------------------------------

def _date_str(d) -> str:
    """Convert date to Kalshi ticker format: '26APR19'."""
    from datetime import date as _date
    return d.strftime("%y") + d.strftime("%b").upper() + d.strftime("%d")


def _today(tz_name: str):
    return datetime.now(ZoneInfo(tz_name)).date()


def _today_str(tz_name: str) -> str:
    return _date_str(_today(tz_name))


def _tomorrow_str(tz_name: str) -> str:
    return _date_str(_today(tz_name) + timedelta(days=1))


def _day_after_tomorrow_str(tz_name: str) -> str:
    return _date_str(_today(tz_name) + timedelta(days=2))


def _bracket_num(bracket: str) -> Optional[float]:
    try:
        return float(bracket[1:])
    except ValueError:
        return None


def _high_series(meta: dict) -> Optional[str]:
    return meta.get("high_series") or meta.get("high")


# ---------------------------------------------------------------------------
# Market fetching
# ---------------------------------------------------------------------------

def _fetch_markets(client, series: str, mdate: str) -> list[dict]:
    """
    Fetch all open brackets for a series + date.
    Returns list of {ticker, bracket, yes_price, no_price}.
    Returns [] if the market doesn't exist yet or fetch fails.
    """
    try:
        resp = client.get("markets", params={"series_ticker": series, "status": "open"})
        markets_raw = resp.get("markets", []) if isinstance(resp, dict) else []
        results = []
        for m in markets_raw:
            ticker = m.get("ticker", "")
            if mdate not in ticker.upper():
                continue
            bracket = ticker.split("-")[-1] if "-" in ticker else ""
            yes_p   = float(m.get("yes_bid_dollars") or m.get("yes_ask") or 0)
            no_p    = float(m.get("no_bid_dollars")  or m.get("no_ask")  or 0)
            results.append({
                "ticker":    ticker,
                "bracket":   bracket,
                "yes_price": yes_p,
                "no_price":  no_p,
            })
        return results
    except Exception:
        return []


def _is_converged(markets: list[dict]) -> bool:
    """
    True if every bracket has max(yes, no) >= SETTLED_THRESHOLD.
    Requires >= 4 brackets to guard against incomplete fetches.
    """
    if len(markets) < 4:
        return False
    return all(
        max(m["yes_price"], m["no_price"]) >= SETTLED_THRESHOLD
        for m in markets
    )


# ---------------------------------------------------------------------------
# Startup
# ---------------------------------------------------------------------------

def initialise(client, city_filter: str = None) -> None:
    """
    Derive scanner state from live data. Call once before the main loop.

    For each city:
      - If an open portfolio position exists on a future HIGHT ticker →
        mark (city, date) fired and set _active_next_date from the ticker.
      - Otherwise → fetch today's HIGH market and compute next_date from
        convergence state.
    """
    cities = {
        name: meta for name, meta in _CITY_REGISTRY.items()
        if city_filter is None or name.lower() == city_filter.lower()
    }

    # ── Step 1: infer state from open positions ───────────────────────────
    covered: set[str] = set()   # cities whose state is already resolved
    try:
        import trader as _trader
        live_positions = _trader.sync_from_kalshi(client) or {}
        for pos in live_positions.values():
            ticker = pos.get("ticker", "")
            if "KXHIGH" not in ticker.upper():
                continue
            # Extract city and date from ticker, e.g. KXHIGHNY-26APR19-B76.5
            parts = ticker.split("-")
            if len(parts) < 3:
                continue
            mdate = parts[1]
            # Match ticker code back to a city
            for city, meta in cities.items():
                series = _high_series(meta)
                if not series:
                    continue
                # series is e.g. 'KXHIGHNY' — check if it appears in the ticker
                if series.upper() in ticker.upper() and city not in covered:
                    _fired_signals.add((city, mdate))
                    _active_next_date[city] = mdate
                    covered.add(city)
                    print(f"  [scanner init] {city}: found open position on {mdate} "
                          f"→ marked fired, next_date={mdate}")
                    break
    except Exception as e:
        print(f"  [scanner init] portfolio fetch error: {e}")

    # ── Step 2: compute next_date from convergence for remaining cities ───
    for city, meta in cities.items():
        if city in covered:
            continue
        tz     = meta["tz"]
        series = _high_series(meta)
        if not series:
            continue

        today_markets = _fetch_markets(client, series, _today_str(tz))
        if _is_converged(today_markets):
            next_date = _day_after_tomorrow_str(tz)
            state_label = "today already converged → watching day+2"
        else:
            next_date = _tomorrow_str(tz)
            state_label = "today not yet converged → watching day+1"

        _active_next_date[city] = next_date
        print(f"  [scanner init] {city}: {state_label}, next_date={next_date}")


# ---------------------------------------------------------------------------
# Core scanner
# ---------------------------------------------------------------------------

def run_scan(client, city_filter: str = None, paper: bool = False) -> None:
    """
    Check each city's next market for a T-bracket signal.
    Called every poll cycle — runs in parallel with run_pipeline().

    For each city:
      1. Fetch today's market. If newly converged, advance next_date.
      2. Fetch next_date market and evaluate signal conditions.
      3. If signal fires, call on_signal() and mark the pair as fired.
    """
    cities = {
        name: meta for name, meta in _CITY_REGISTRY.items()
        if city_filter is None or name.lower() == city_filter.lower()
    }

    for city, meta in cities.items():
        tz     = meta["tz"]
        series = _high_series(meta)
        if not series:
            continue

        # Ensure state is initialised (defensive — initialise() should have run)
        if city not in _active_next_date:
            _active_next_date[city] = _tomorrow_str(tz)

        # ── Check if today's market has just converged ────────────────────
        today_str = _today_str(tz)
        today_markets = _fetch_markets(client, series, today_str)
        if today_markets and _is_converged(today_markets):
            expected_next = _day_after_tomorrow_str(tz)
            if _active_next_date[city] != expected_next:
                print(f"  [scanner] {city}: today ({today_str}) converged "
                      f"→ advancing next_date to {expected_next}")
                _active_next_date[city] = expected_next

        # ── Skip if already fired for this next date ──────────────────────
        next_date = _active_next_date[city]
        sig_key   = (city, next_date)
        if sig_key in _fired_signals:
            # Directional signal already fired — still check gradient/dismissed
            pass

        # ── Fetch and evaluate next market ────────────────────────────────
        markets = _fetch_markets(client, series, next_date)
        if not markets:
            continue   # market not listed yet

        T_markets = sorted(
            [m for m in markets if m["bracket"].startswith("T")],
            key=lambda m: _bracket_num(m["bracket"]) or 0,
        )
        B_markets = sorted(
            [m for m in markets if m["bracket"].startswith("B")],
            key=lambda m: _bracket_num(m["bracket"]) or 0,
        )

        if len(T_markets) < 2 or not B_markets:
            continue

        t_low       = T_markets[0]
        t_high      = T_markets[-1]
        b_adj_low   = B_markets[0]
        b_adj_high  = B_markets[-1]

        # ── Forecast shift detection ───────────────────────────────────────
        _forecast_shift = None
        try:
            from nws_feed import forecast_shift_tracker as _fst
            import nws_feed as _nws
            _tmr_nws = _nws.snapshot(city_filter=city)
            _tmr_fcst = (_tmr_nws.get(city) or {}).get("forecast_high_f")
            _forecast_shift = _fst.update_high(f"{city}_tomorrow", _tmr_fcst)
            if _forecast_shift is not None and _forecast_shift <= -1.5:
                from log_setup import get_logger as _gl
                _gl("tomorrow_scanner").info(
                    "FORECAST_SHIFT_TMR  %s  %s  Δ%+.1f°F — "
                    "scanning tomorrow brackets immediately",
                    city, next_date, _forecast_shift,
                )
        except Exception:
            pass

        if sig_key not in _fired_signals:
            signal = _check_signal(t_low, t_high, b_adj_low, b_adj_high)

            if signal:
                trigger_t, adj_b, t_no, b_no = signal
                print(
                    f"\n  ★ SIGNAL [{city}] {next_date} | "
                    f"{trigger_t['bracket']} No={t_no:.2f}  "
                    f"adj-B {adj_b['bracket']} No={b_no:.2f}"
                )
                on_signal(
                    client    = client,
                    city      = city,
                    sig_key   = sig_key,
                    trigger_t = trigger_t,
                    adj_b     = adj_b,
                    paper     = paper,
                )

        # ── Dismissed-T signal ─────────────────────────────────────────────
        if len(T_markets) >= 2:
            dismissed = _check_dismissed_signal(
                t_low  = T_markets[0],
                t_high = T_markets[-1],
                city   = city,
            )
            if dismissed:
                d_ticker   = dismissed.get("ticker", "")
                d_no_price = float(dismissed.get("no_price", 0.0) or 0.0)
                from log_setup import get_logger as _gl
                _gl("tomorrow_scanner").info(
                    "  ★ DISMISSED_T [%s] %s | %s  Yes=%.2f  No=%.2f",
                    city, next_date, dismissed.get("bracket", ""),
                    float(dismissed.get("yes_price", 0.0) or 0.0), d_no_price,
                )
                on_dismissed_signal(
                    client   = client, city  = city,
                    ticker   = d_ticker, no_price = d_no_price, paper = paper,
                )

        # ── Gradient open signal ───────────────────────────────────────────
        all_markets = T_markets + B_markets
        gradient_candidates = _check_gradient_signal(all_markets, city)

        if _forecast_shift is not None and _forecast_shift <= -1.5 and not gradient_candidates:
            gradient_candidates = _check_gradient_signal(
                all_markets, city, yes_override=YES_DISMISSED * 3
            )
            if gradient_candidates:
                from log_setup import get_logger as _gl
                _gl("tomorrow_scanner").info(
                    "  ★ GRADIENT_SHIFT [%s] %s | Δ%+.1f°F → %d bracket(s)",
                    city, next_date, _forecast_shift, len(gradient_candidates),
                )

        if gradient_candidates:
            from log_setup import get_logger as _gl
            _gl("tomorrow_scanner").info(
                "  ★ GRADIENT_OPEN [%s] %s | %d bracket(s) qualify",
                city, next_date, len(gradient_candidates),
            )
        for bracket in gradient_candidates:
            g_ticker   = bracket.get("ticker", "")
            g_no_price = float(bracket.get("no_price", 0.0) or 0.0)
            on_dismissed_signal(
                client   = client, city  = city,
                ticker   = g_ticker, no_price = g_no_price, paper = paper,
            )

        # ── Sweep signal ───────────────────────────────────────────────────
        if city in SWEEP_FLOORS:
            sweep_candidates = _check_sweep_signal(markets, city)
            for candidate in sweep_candidates:
                s_ticker   = candidate.get("ticker", "")
                s_no_price = float(candidate.get("no_price", 0.0) or 0.0)
                from log_setup import get_logger as _gl
                _gl("tomorrow_scanner").info(
                    "  ★ SWEEP [%s] %s | %s  No=%.2f  %dc",
                    city, next_date, candidate.get("bracket", ""),
                    s_no_price, SWEEP_CONTRACTS,
                )
                on_sweep_signal(
                    client   = client, city     = city,
                    ticker   = s_ticker, no_price = s_no_price, paper = paper,
                )


# ---------------------------------------------------------------------------
# Signal parameters and helper functions
# ---------------------------------------------------------------------------

YES_DISMISSED         = 0.07
YES_DISMISSED_T_OTHER = 0.10
DISMISSED_NO_MIN      = 0.75
DISMISSED_NO_MAX      = 0.94
DISMISSED_HOUR_MAX    = 18
OPEN_LEAN_MIN         = 0.10

_bracket_entered: set[str] = set()


def _check_dismissed_signal(t_low: dict, t_high: dict, city: str) -> Optional[dict]:
    from zoneinfo import ZoneInfo
    local_hour = datetime.now(ZoneInfo(
        (_CITY_REGISTRY.get(city) or {}).get("tz", "UTC")
    )).hour
    if local_hour >= DISMISSED_HOUR_MAX:
        return None
    for dismissed, other in [(t_low, t_high), (t_high, t_low)]:
        yes_p     = float(dismissed.get("yes_price", 1.0) or 1.0)
        no_p      = float(dismissed.get("no_price",  0.0) or 0.0)
        other_yes = float(other.get("yes_price", 1.0) or 1.0)
        if (yes_p <= YES_DISMISSED
                and DISMISSED_NO_MIN <= no_p < DISMISSED_NO_MAX
                and other_yes >= YES_DISMISSED_T_OTHER):
            return dismissed
    return None


def _check_gradient_signal(
    all_brackets: list[dict],
    city: str,
    yes_override: float | None = None,
) -> list[dict]:
    from zoneinfo import ZoneInfo
    local_hour = datetime.now(ZoneInfo(
        (_CITY_REGISTRY.get(city) or {}).get("tz", "UTC")
    )).hour
    if local_hour >= DISMISSED_HOUR_MAX:
        return []

    def floor_of(b: dict) -> Optional[float]:
        s = b.get("bracket", "")
        try: return float(s[1:]) if s else None
        except (ValueError, IndexError): return None

    sortable = [(floor_of(b), b) for b in all_brackets]
    sortable = [(f, b) for f, b in sortable if f is not None]
    if len(sortable) < 6:
        return []
    sortable.sort(key=lambda x: x[0])
    sorted_b = [b for _, b in sortable]
    bottom3, top3 = sorted_b[:3], sorted_b[-3:]

    def avg_yes(brackets):
        vals = [float(b.get("yes_price", 0.0) or 0.0) for b in brackets]
        return sum(vals) / len(vals) if vals else 0.0

    lean     = avg_yes(bottom3) - avg_yes(top3)
    lean_abs = abs(lean)
    if lean_abs < OPEN_LEAN_MIN:
        return []

    far_end  = top3 if lean > 0 else bottom3
    yes_gate = yes_override if yes_override is not None else YES_DISMISSED
    return [
        b for b in far_end
        if float(b.get("yes_price", 1.0) or 1.0) <= yes_gate
        and DISMISSED_NO_MIN <= float(b.get("no_price", 0.0) or 0.0) < DISMISSED_NO_MAX
    ]


def on_dismissed_signal(
    client, city: str, ticker: str, no_price: float, paper: bool = False,
) -> None:
    import trader as _trader
    from log_setup import get_logger
    _log = get_logger("tomorrow_scanner")

    if ticker in _bracket_entered:
        return

    cost = round(no_price * TOMORROW_CONTRACTS, 4)
    try:
        deployable = _trader.get_tomorrow_deployable(ticker)
        if deployable < cost:
            _log.debug("TOMORROW_DIS skip %s %s — budget exhausted", city, ticker)
            return
    except Exception as e:
        _log.warning("TOMORROW_DIS capital check failed: %s", e)
        return

    tag = "[PAPER] " if paper else ""
    _log.info("%sTOMORROW_DIS  %s  %s  No=%.2f  %dc",
              tag, city, ticker, no_price, TOMORROW_CONTRACTS)

    if not paper:
        try:
            import datetime as _dt
            _trader.place_order(
                client=client, ticker=ticker, side="no",
                price_dollars=no_price, contracts=TOMORROW_CONTRACTS, paper=False,
            )
            _trader._append_trade_log({
                "ticker": ticker, "city": city, "side": "no",
                "market_type": "high", "score": 5,
                "score_detail": ["tomorrow_open_signal", f"no_price={no_price:.2f}"],
                "entry_price": no_price, "contracts": TOMORROW_CONTRACTS,
                "placed_at": _dt.datetime.now(_dt.timezone.utc).isoformat(),
                "paper": False, "entry_tier": "tomorrow_dismissed",
            })
            _trader.record_tomorrow_deployed(cost, ticker)
            _bracket_entered.add(ticker)
        except Exception as e:
            _log.error("TOMORROW_DIS order failed %s: %s", ticker, e)
    else:
        _log.info("  [PAPER] would place No %dc @ $%.2f on %s",
                  TOMORROW_CONTRACTS, no_price, ticker)
        _bracket_entered.add(ticker)


# ---------------------------------------------------------------------------
# Sweep signal
# ---------------------------------------------------------------------------

def _check_sweep_signal(markets: list[dict], city: str) -> list[dict]:
    """
    Scan all brackets for a city's tomorrow market and return those that
    qualify for a sweep entry.

    Rules:
      - City must be in SWEEP_FLOORS (caller already checks this).
      - Skip the forecast bracket: the one with the highest Yes price.
        That bracket is where the market thinks the temperature will land —
        entering No there is the directional bet, not a tail sweep.
      - For every remaining bracket qualify if:
          SWEEP_FLOORS[city] <= no_price < SWEEP_CEILING
      - Skip tickers already in _sweep_entered (once-per-session guard).

    Returns a list of qualifying bracket dicts, possibly empty.
    """
    if not markets:
        return []

    floor = SWEEP_FLOORS.get(city)
    if floor is None:
        return []

    # Identify the forecast bracket (highest Yes price across all brackets).
    forecast = max(markets, key=lambda m: float(m.get("yes_price", 0.0) or 0.0))
    forecast_ticker = forecast.get("ticker", "")

    candidates = []
    for m in markets:
        ticker   = m.get("ticker", "")
        no_price = float(m.get("no_price", 0.0) or 0.0)

        if ticker == forecast_ticker:
            continue                          # skip forecast bracket
        if ticker in _sweep_entered:
            continue                          # already entered this session
        if floor <= no_price < SWEEP_CEILING:
            candidates.append(m)

    return candidates


def on_sweep_signal(
    client, city: str, ticker: str, no_price: float, paper: bool = False,
) -> None:
    """
    Place a No order for a sweep entry and record it.

    Capital is drawn from the session-scoped tomorrow budget (same pool as
    dismissed / gradient signals). The ticker is marked in _sweep_entered
    immediately on a successful order so it is never re-entered this session.
    Top-ups on these positions are handled by the existing top-up engine in
    trader.py once the position appears in today's markets (TOPUP_TOTAL_CAP=9
    ensures headroom exists after the initial 3-contract entry).
    """
    import trader as _trader
    from log_setup import get_logger
    _log = get_logger("tomorrow_scanner")

    if ticker in _sweep_entered:
        return

    cost = round(no_price * SWEEP_CONTRACTS, 4)
    try:
        deployable = _trader.get_tomorrow_deployable(ticker)
        if deployable < cost:
            _log.debug("SWEEP skip %s %s — budget exhausted (cost=$%.2f)",
                       city, ticker, cost)
            return
    except Exception as e:
        _log.warning("SWEEP capital check failed %s: %s", ticker, e)
        return

    tag = "[PAPER] " if paper else ""
    _log.info("%sSWEEP  %s  %s  No=%.2f  %dc",
              tag, city, ticker, no_price, SWEEP_CONTRACTS)

    if not paper:
        try:
            import datetime as _dt
            _trader.place_order(
                client        = client,
                ticker        = ticker,
                side          = "no",
                price_dollars = no_price,
                contracts     = SWEEP_CONTRACTS,
                paper         = False,
            )
            _trader._append_trade_log({
                "ticker":       ticker,
                "city":         city,
                "side":         "no",
                "market_type":  "high",
                "score":        5,
                "score_detail": ["tomorrow_sweep", f"no_price={no_price:.2f}"],
                "entry_price":  no_price,
                "contracts":    SWEEP_CONTRACTS,
                "placed_at":    _dt.datetime.now(_dt.timezone.utc).isoformat(),
                "paper":        False,
                "entry_tier":   "tomorrow_sweep",
            })
            _trader.record_tomorrow_deployed(cost, ticker)
            _sweep_entered.add(ticker)
        except Exception as e:
            _log.error("SWEEP order failed %s: %s", ticker, e)
    else:
        _log.info("  [PAPER] would place No %dc @ $%.2f on %s",
                  SWEEP_CONTRACTS, no_price, ticker)
        _sweep_entered.add(ticker)


def _check_signal(
    t_low: dict, t_high: dict,
    b_adj_low: dict, b_adj_high: dict,
) -> Optional[tuple[dict, dict, float, float]]:
    """
    Returns (trigger_T, adj_B, T_no_price, B_no_price) if signal fires, else None.

    Conditions:
      - Exactly one T has No >= NO_TRIGGER
      - The other T has No < NO_CONVERGE_THRESHOLD
    """
    t_low_no  = t_low.get("no_price",  0.0) or 0.0
    t_high_no = t_high.get("no_price", 0.0) or 0.0

    if t_low_no >= NO_TRIGGER and t_high_no < NO_CONVERGE_THRESHOLD:
        return t_low, b_adj_low, t_low_no, b_adj_low.get("no_price", 0.0) or 0.0

    if t_high_no >= NO_TRIGGER and t_low_no < NO_CONVERGE_THRESHOLD:
        return t_high, b_adj_high, t_high_no, b_adj_high.get("no_price", 0.0) or 0.0

    return None


# ---------------------------------------------------------------------------
# Signal handler
# ---------------------------------------------------------------------------

def on_signal(
    client,
    city:      str,
    sig_key:   tuple,
    trigger_t: dict,
    adj_b:     dict,
    paper:     bool = False,
) -> None:
    """
    Called when the signal fires. Places No orders on:
      - The trigger T bracket (strong directional confirmation)
      - The adjacent B bracket (complementary range bet)

    _fired_signals is only marked after at least one order succeeds.
    If both orders fail (API error, price moved), the signal is not
    marked fired and will be retried on the next poll cycle.

    Capital is drawn from the session-scoped tomorrow budget in trader.py.
    Positions are held overnight and managed by check_exits() on their
    settlement date. The date guard in check_exits() ensures today's NWS
    data does not incorrectly trigger an exit on a future-dated position.
    """
    import trader as _trader
    from log_setup import get_logger
    _log = get_logger("tomorrow_scanner")

    tag = "[PAPER] " if paper else ""
    orders_placed = 0

    for bracket_dict, label in [(trigger_t, "T-trigger"), (adj_b, "adj-B")]:
        ticker   = bracket_dict["ticker"]
        no_price = bracket_dict.get("no_price", 0.0) or 0.0

        if no_price <= 0.0 or no_price > NO_MAX_ENTRY:
            _log.info("TOMORROW skip %s %s — No price %.2f out of range",
                      city, ticker, no_price)
            continue

        # Capital check against session-scoped tomorrow budget
        cost = round(no_price * TOMORROW_CONTRACTS, 4)
        try:
            deployable = _trader.get_tomorrow_deployable()
            if deployable < cost:
                _log.debug("TOMORROW skip %s %s — tomorrow budget exhausted "
                           "(cost=$%.2f  remaining=$%.2f)",
                           city, ticker, cost, deployable)
                continue
        except Exception as e:
            _log.warning("TOMORROW capital check failed: %s — skipping %s", e, ticker)
            continue

        _log.info("%sTOMORROW ENTRY  %s  %s  No=%.2f  %dc",
                  tag, city, ticker, no_price, TOMORROW_CONTRACTS)

        if not paper:
            try:
                _trader.place_order(
                    client        = client,
                    ticker        = ticker,
                    side          = "no",
                    price_dollars = no_price,
                    contracts     = TOMORROW_CONTRACTS,
                    paper         = False,
                )
                _trader._append_trade_log({
                    "ticker":       ticker,
                    "city":         city,
                    "side":         "no",
                    "market_type":  "high",
                    "score":        5,
                    "score_detail": ["tomorrow_scanner_T" if "T" in label else "tomorrow_scanner_B"],
                    "entry_price":  no_price,
                    "contracts":    TOMORROW_CONTRACTS,
                    "placed_at":    __import__("datetime").datetime.now(
                                        __import__("datetime").timezone.utc).isoformat(),
                    "paper":        False,
                    "entry_tier":   "tomorrow",
                })
                _trader.record_tomorrow_deployed(cost)
                orders_placed += 1
            except Exception as e:
                _log.error("TOMORROW order failed %s: %s", ticker, e)
        else:
            _log.info("  [PAPER] would place No %dc @ $%.2f on %s",
                      TOMORROW_CONTRACTS, no_price, ticker)
            orders_placed += 1

    # Only mark the signal as fired once at least one order succeeded.
    if orders_placed > 0:
        _fired_signals.add(sig_key)
        _log.debug("TOMORROW signal fired: %s → marked as done (%d order(s))",
                   sig_key, orders_placed)
    else:
        _log.warning("TOMORROW signal for %s produced no orders — will retry next poll",
                     city)
