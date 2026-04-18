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

# ---------------------------------------------------------------------------
# Per-city state
# ---------------------------------------------------------------------------

# { city: market_date_str }  — the date currently being watched for a signal
# e.g. { "Chicago": "26APR20" }
_active_next_date: dict[str, str] = {}

# (city, market_date_str) pairs already entered — never re-enter
_fired_signals: set[tuple[str, str]] = set()


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
        resp = client.get_markets(series_ticker=series, status="open")
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
            continue

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
        b_adj_low   = B_markets[0]    # adjacent to t_low
        b_adj_high  = B_markets[-1]   # adjacent to t_high

        signal = _check_signal(t_low, t_high, b_adj_low, b_adj_high)

        if signal:
            trigger_t, adj_b, t_no, b_no = signal
            print(
                f"\n  ★ SIGNAL [{city}] {next_date} | "
                f"{trigger_t['bracket']} No={t_no:.2f}  "
                f"adj-B {adj_b['bracket']} No={b_no:.2f}"
            )
            _fired_signals.add(sig_key)
            on_signal(
                client    = client,
                city      = city,
                trigger_t = trigger_t,
                adj_b     = adj_b,
                paper     = paper,
            )


# ---------------------------------------------------------------------------
# Signal logic
# ---------------------------------------------------------------------------

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
# Signal handler — wire in order execution here
# ---------------------------------------------------------------------------

def on_signal(
    client,
    city:      str,
    trigger_t: dict,
    adj_b:     dict,
    paper:     bool = False,
) -> None:
    """
    Called when the signal fires. Currently logs only.
    Swap in trader.place_order() calls when ready to go live.
    """
    tag = "[PAPER] " if paper else ""
    print(f"    → {tag}Would buy No on {trigger_t['ticker']} @ {trigger_t['no_price']:.2f}")
    print(f"    → {tag}Would buy No on {adj_b['ticker']} @ {adj_b['no_price']:.2f}")

    # TODO: wire in order execution, e.g.:
    # trader.place_order(client, ticker=trigger_t["ticker"], side="no", ...)
    # trader.place_order(client, ticker=adj_b["ticker"],     side="no", ...)
