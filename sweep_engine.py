"""
sweep_engine.py
---------------
Unified bracket sweep engine for HIGH temperature markets.

Combines three complementary signal paths into one engine with one capital
allocation and one session dedup set:

Signal A — Directional pre-market  (was: tomorrow_scanner on_signal)
----------------------------------------------------------------------
Fires on the NEXT DAY's market when T-bracket convergence indicates a
clear directional lean. Enters No on the trigger T bracket and the
adjacent B bracket.

  Conditions:
    - One T bracket has No >= NO_TRIGGER (0.80) — strong directional signal
    - The opposing T has No < NO_CONVERGE_THRESHOLD (0.60) — confirms direction
    - Entry No price in [NO_MIN_ENTRY, NO_MAX_ENTRY]

  Backtest: 100% WR on T trigger, 96.3% WR on adj-B (n=56 T, n=54 B)

Signal B — Near-dead sweep  (was: tomorrow_scanner sweep)
----------------------------------------------------------
Fires on ANY open HIGH market when a bracket is priced in the sweep zone.
Per-city No floors are derived from backtest; cities with no clean floor
are excluded.

  Conditions:
    - City in SWEEP_FLOORS
    - No price in [SWEEP_FLOORS[city], SWEEP_CEILING=0.97)
    - Skip forecast bracket (highest Yes price)

  Backtest: 100% WR at floor >= 0.80, n=72 live trades

Signal C — Dead bracket  (was: dead_sweep)
-------------------------------------------
Fires on ANY open HIGH market when a far bracket is effectively dead.
Uses NWS forecast distance rank to identify rank-4 and rank-5 brackets.

  Conditions:
    - No price in [DEAD_FLOOR=0.97, DEAD_CEILING=0.989]
    - Bracket is rank-4 or rank-5 by NWS forecast distance
    - Passes city × month safety filter

  Backtest (Apr–Jun 2026): 1,080 signals, 99.2% WR, EV +$0.017/contract × 5c

Signal B/C interaction
----------------------
Sweep (B) handles No < 0.97, dead bracket (C) handles No >= 0.97 — the
two price ranges are strictly complementary with no overlap.

Capital
-------
All three signals draw from the 'sweep' engine allocation. One shared
session dedup set (_sweep_entered) prevents double-entry across all paths.

Dismissed-T and gradient-open signals
--------------------------------------
Also absorbed from tomorrow_scanner. These fire on dismissed (Yes → near-0)
T brackets and on gradient market opens (top-3 vs bottom-3 Yes lean).
Both use the same budget and dedup set.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from typing import Optional

from cities import TRADING_CITIES as _CITY_REGISTRY, CITIES as _CITIES
from log_setup import get_logger
from market_utils import (
    local_hour      as _local_hour,
    no_price        as _no_price,
    yes_price       as _yes_price,
    bracket_val     as _bracket_val,
    load_config_env,
)

log = get_logger(__name__)

# ---------------------------------------------------------------------------
# Signal A parameters (directional pre-market)
# ---------------------------------------------------------------------------

NO_TRIGGER            = 0.80   # T bracket No that fires the directional signal
NO_CONVERGE_THRESHOLD = 0.60   # opposing T must be below this to confirm
NO_MIN_ENTRY          = 0.75   # minimum No price for any entry (prevents 50/50 adj-B)
NO_MAX_ENTRY          = 0.94   # maximum No price for Signal A entries
DIRECTIONAL_CONTRACTS = 3      # contracts per bracket (T and adj-B each)
SETTLED_THRESHOLD     = 0.97   # max(yes, no) >= this → bracket is settled

# ---------------------------------------------------------------------------
# Signal B parameters (near-dead sweep)
# ---------------------------------------------------------------------------

# Per-city minimum No-price floors from backtest (Apr–May 2026).
# Cities without a clean floor (Austin, Boston, Dallas, Houston,
# Miami, New Orleans, San Antonio) are excluded.
# Floor of 0.75 raised to 0.80 across the board — eliminates lowest-
# confidence entries while preserving 100% WR.
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
SWEEP_CEILING   = 0.97   # upper bound for Signal B — Signal C starts here
SWEEP_CONTRACTS = 3      # contracts per Signal B entry

# ---------------------------------------------------------------------------
# Signal C parameters (dead bracket)
# ---------------------------------------------------------------------------

DEAD_FLOOR    = 0.97    # minimum No price — below this is Signal B territory
DEAD_CEILING  = 0.989   # above this, Yes is $0.01 — fills collapse, skip
DEAD_CONTRACTS = 5      # flat sizing per dead bracket entry

# Dead bracket safety filter
# Always safe: 0% historical chance of >6°F upside forecast miss
_SAFE_ALWAYS: frozenset[str] = frozenset({
    "Phoenix", "Las Vegas", "Seattle", "Miami", "Washington DC",
})
# (city, month) → skip for top-T and far-B. Safe outside these months.
_CAUTION_SKIP: dict[str, frozenset[int]] = {
    "San Francisco": frozenset({5}),
    "Houston":       frozenset({5}),
    "Atlanta":       frozenset({5}),
    "Los Angeles":   frozenset({4}),
    "New Orleans":   frozenset({4}),
}

# ---------------------------------------------------------------------------
# Dismissed-T / gradient-open parameters
# ---------------------------------------------------------------------------

YES_DISMISSED         = 0.07
YES_DISMISSED_T_OTHER = 0.10
DISMISSED_NO_MIN      = 0.75
DISMISSED_NO_MAX      = 0.94
DISMISSED_HOUR_MAX    = 18
OPEN_LEAN_MIN         = 0.10
DISMISSED_CONTRACTS   = 3

# ---------------------------------------------------------------------------
# Session state
# ---------------------------------------------------------------------------

# Unified dedup set — all signal paths check and populate this.
# Resets on service restart; recovered from live positions in initialise().
_sweep_entered: set[str] = set()

# Signal A state — tracks which (city, next_date) directional signal has fired
_active_next_date: dict[str, str] = {}
_fired_signals:    set[tuple[str, str]] = set()


# ---------------------------------------------------------------------------
# Date helpers
# ---------------------------------------------------------------------------

def _date_str(d) -> str:
    return d.strftime("%y%b%d").upper()


def _today(tz_name: str):
    from zoneinfo import ZoneInfo
    return datetime.now(ZoneInfo(tz_name))


def _today_str(tz_name: str) -> str:
    return _date_str(_today(tz_name))


def _tomorrow_str(tz_name: str) -> str:
    from datetime import timedelta
    return _date_str(_today(tz_name) + timedelta(days=1))


def _day_after_tomorrow_str(tz_name: str) -> str:
    from datetime import timedelta
    return _date_str(_today(tz_name) + timedelta(days=2))


def _high_series(meta: dict) -> Optional[str]:
    return meta.get("high_series")


# ---------------------------------------------------------------------------
# Market fetch helpers
# ---------------------------------------------------------------------------

def _fetch_markets(client, series: str, mdate: str) -> list[dict]:
    """Fetch all brackets for a specific market date."""
    try:
        resp = client.get("/markets", params={
            "series_ticker": series,
            "status":        "open",
        })
        all_markets = resp.get("markets", [])
        return [
            m for m in all_markets
            if m.get("event_ticker", "").endswith(mdate)
        ]
    except Exception as e:
        log.debug("sweep: fetch_markets failed for %s %s: %s", series, mdate, e)
        return []


def _is_converged(markets: list[dict]) -> bool:
    """True when all brackets in a market have settled."""
    if not markets:
        return False
    return all(
        max(
            float(m.get("yes_price", 0) or 0),
            float(m.get("no_price",  0) or 0),
        ) >= SETTLED_THRESHOLD
        for m in markets
    )


# ---------------------------------------------------------------------------
# Startup: recover _sweep_entered from live positions
# ---------------------------------------------------------------------------

def initialise(client, city_filter: str = None) -> None:
    """
    Recover session state on startup.

    Populates _sweep_entered from live positions so we never re-enter a
    bracket already held after a service restart. Also initialises
    _active_next_date for each city's directional signal tracking.
    """
    from zoneinfo import ZoneInfo

    cities = {
        name: meta for name, meta in _CITY_REGISTRY.items()
        if city_filter is None or name.lower() == city_filter.lower()
    }

    # Recover _sweep_entered from live positions
    try:
        import trader as _trader
        live = _trader.get_positions(client)
        for pos in live:
            ticker = pos.get("ticker", "")
            if ticker.startswith("KXHIGH") and float(pos.get("position_fp", 0) or 0) != 0:
                _sweep_entered.add(ticker)
                log.debug("sweep: init — marked %s as entered", ticker)
    except Exception as e:
        log.warning("sweep: init position recovery failed: %s", e)

    # Initialise next_date tracking for directional signal
    for city, meta in cities.items():
        tz     = meta["tz"]
        series = _high_series(meta)
        if not series:
            continue

        try:
            today_markets = _fetch_markets(client, series, _today_str(tz))
            if today_markets and _is_converged(today_markets):
                _active_next_date[city] = _day_after_tomorrow_str(tz)
                print(f"  [scanner init] {city}: today converged → "
                      f"watching day+1, next_date={_active_next_date[city]}")
            else:
                # Check for existing positions on next_date markets
                next_date = _tomorrow_str(tz)
                next_markets = _fetch_markets(client, series, next_date)
                has_pos = any(
                    m.get("ticker", "") in _sweep_entered
                    for m in (next_markets or [])
                )
                if has_pos:
                    _active_next_date[city] = next_date
                    print(f"  [scanner init] {city}: found open position on "
                          f"{next_date} → marked fired, next_date={next_date}")
                else:
                    _active_next_date[city] = next_date
                    print(f"  [scanner init] {city}: today not yet converged "
                          f"→ watching day+1, next_date={next_date}")
        except Exception as e:
            _active_next_date[city] = _tomorrow_str(tz)
            log.debug("sweep: init failed for %s: %s", city, e)


# ---------------------------------------------------------------------------
# Signal C helpers (dead bracket)
# ---------------------------------------------------------------------------

def _compute_fcst_rank(brackets: list[dict], forecast_high: float) -> dict[str, int]:
    """Assign forecast-distance rank to each bracket. Rank 0 = closest."""
    def dist(b: dict) -> float:
        code = b.get("bracket") or b.get("ticker", "").split("-")[-1]
        val  = _bracket_val(code)
        return abs(val - forecast_high) if val is not None else 0.0

    sorted_b = sorted(brackets, key=dist)
    return {b["ticker"]: i for i, b in enumerate(sorted_b)}


def _is_bottom_t(bracket_code: str, forecast_high: float) -> bool:
    """True if T bracket threshold is below the forecast — structurally safe."""
    if not bracket_code.startswith("T"):
        return False
    val = _bracket_val(bracket_code)
    return val is not None and val < forecast_high


def _dead_is_safe(bracket_code: str, city: str, month: int, fcst: float) -> bool:
    """Return True if this bracket passes the dead-sweep city safety filter."""
    if _is_bottom_t(bracket_code, fcst):
        return True
    if city in _SAFE_ALWAYS:
        return True
    if city in _CAUTION_SKIP:
        return month not in _CAUTION_SKIP[city]
    return False


# ---------------------------------------------------------------------------
# Signal checks
# ---------------------------------------------------------------------------

def _check_directional(
    t_low: dict, t_high: dict,
    b_adj_low: dict, b_adj_high: dict,
) -> Optional[tuple[dict, dict, float, float]]:
    """
    Signal A: returns (trigger_T, adj_B, T_no, B_no) or None.
    Exactly one T must have No >= NO_TRIGGER while the other is < NO_CONVERGE_THRESHOLD.
    """
    t_low_no  = float(t_low.get("no_price",  0.0) or 0.0)
    t_high_no = float(t_high.get("no_price", 0.0) or 0.0)

    if t_low_no >= NO_TRIGGER and t_high_no < NO_CONVERGE_THRESHOLD:
        return t_low, b_adj_low, t_low_no, float(b_adj_low.get("no_price", 0.0) or 0.0)
    if t_high_no >= NO_TRIGGER and t_low_no < NO_CONVERGE_THRESHOLD:
        return t_high, b_adj_high, t_high_no, float(b_adj_high.get("no_price", 0.0) or 0.0)
    return None


def _check_dismissed(t_low: dict, t_high: dict, city: str) -> Optional[dict]:
    """Dismissed-T signal: one T has collapsed to near-zero Yes."""
    from zoneinfo import ZoneInfo
    lh = datetime.now(ZoneInfo((_CITY_REGISTRY.get(city) or {}).get("tz", "UTC"))).hour
    if lh >= DISMISSED_HOUR_MAX:
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


def _check_gradient(
    all_brackets: list[dict],
    city: str,
    yes_override: float | None = None,
) -> list[dict]:
    """Gradient-open signal: strong lean between top-3 and bottom-3 Yes prices."""
    from zoneinfo import ZoneInfo
    lh = datetime.now(ZoneInfo((_CITY_REGISTRY.get(city) or {}).get("tz", "UTC"))).hour
    if lh >= DISMISSED_HOUR_MAX:
        return []

    def floor_of(b: dict) -> Optional[float]:
        s = b.get("bracket", "")
        try:
            return float(s[1:]) if s else None
        except (ValueError, IndexError):
            return None

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

    lean = avg_yes(bottom3) - avg_yes(top3)
    if abs(lean) < OPEN_LEAN_MIN:
        return []

    far_end  = top3 if lean > 0 else bottom3
    yes_gate = yes_override if yes_override is not None else YES_DISMISSED
    return [
        b for b in far_end
        if float(b.get("yes_price", 1.0) or 1.0) <= yes_gate
        and DISMISSED_NO_MIN <= float(b.get("no_price", 0.0) or 0.0) < DISMISSED_NO_MAX
    ]


def _check_sweep(markets: list[dict], city: str) -> list[dict]:
    """Signal B: near-dead sweep candidates in [SWEEP_FLOORS[city], SWEEP_CEILING)."""
    floor = SWEEP_FLOORS.get(city)
    if not markets or floor is None:
        return []
    forecast_ticker = max(
        markets,
        key=lambda m: float(m.get("yes_price", 0.0) or 0.0),
    ).get("ticker", "")
    return [
        m for m in markets
        if m.get("ticker", "") != forecast_ticker
        and m.get("ticker", "") not in _sweep_entered
        and floor <= float(m.get("no_price", 0.0) or 0.0) < SWEEP_CEILING
    ]


def _check_dead(
    brackets: list[dict],
    city: str,
    month: int,
    forecast_high: float,
) -> list[dict]:
    """Signal C: dead bracket candidates in [DEAD_FLOOR, DEAD_CEILING]."""
    if len(brackets) != 6:
        return []
    rank_map = _compute_fcst_rank(brackets, forecast_high)
    results = []
    for b in brackets:
        ticker = b.get("ticker", "")
        if not ticker or ticker in _sweep_entered:
            continue
        if rank_map.get(ticker, -1) not in (4, 5):
            continue
        no_p = float(b.get("no_price", 0.0) or 0.0)
        if not (DEAD_FLOOR <= no_p <= DEAD_CEILING):
            continue
        bracket_code = b.get("bracket") or ticker.split("-")[-1]
        if not _dead_is_safe(bracket_code, city, month, forecast_high):
            continue
        results.append(b)
    return results


# ---------------------------------------------------------------------------
# Order placement helpers
# ---------------------------------------------------------------------------

def _place(
    client,
    ticker:     str,
    city:       str,
    no_p:       float,
    contracts:  int,
    entry_tier: str,
    score_detail: list,
    paper:      bool,
    _trader,
) -> bool:
    """Place one No order and record it. Returns True on success."""
    cost = round(no_p * contracts, 4)

    try:
        deployable = _trader.get_sweep_deployable()
        if deployable < cost:
            log.debug("sweep: %s — budget exhausted (cost=$%.2f remaining=$%.2f)",
                      ticker, cost, deployable)
            return False
    except Exception as e:
        log.warning("sweep: capital check failed %s: %s", ticker, e)
        return False

    if paper:
        log.info("  [PAPER] would place No %dc @ $%.2f on %s", contracts, no_p, ticker)
        _sweep_entered.add(ticker)
        return True

    try:
        _trader.place_order(
            client        = client,
            ticker        = ticker,
            side          = "no",
            price_dollars = no_p,
            contracts     = contracts,
            paper         = False,
        )
        _trader._append_trade_log({
            "ticker":       ticker,
            "city":         city,
            "side":         "no",
            "market_type":  "high",
            "score":        5,
            "score_detail": score_detail,
            "entry_price":  no_p,
            "contracts":    contracts,
            "placed_at":    datetime.now(timezone.utc).isoformat(),
            "paper":        False,
            "entry_tier":   entry_tier,
        })
        _trader.record_sweep_deployed(cost)
        _sweep_entered.add(ticker)
        return True
    except Exception as e:
        log.error("sweep: order failed %s: %s", ticker, e)
        return False


# ---------------------------------------------------------------------------
# Core scan
# ---------------------------------------------------------------------------

def run_scan(
    client,
    city_filter:     str  = None,
    paper:           bool = False,
    kalshi_snapshot: dict = None,
    nws_snapshot:    dict = None,
) -> None:
    """
    Run all three sweep signal paths across all HIGH markets.

    kalshi_snapshot is accepted for interface compatibility with the
    scheduler pre-fetch pattern but is not used here — the directional
    signal (A) fetches specific next-date markets, while sweep (B) and
    dead bracket (C) operate on today's markets via kalshi_snapshot.
    """
    import trader as _trader
    import kalshi_scanner as _ks

    cities = {
        name: meta for name, meta in _CITY_REGISTRY.items()
        if city_filter is None or name.lower() == city_filter.lower()
    }

    # Ensure today's Kalshi snapshot is available for B and C
    if kalshi_snapshot is None:
        try:
            kalshi_snapshot = _ks.scan_all(city_filter=city_filter, market_type="high")
        except Exception as e:
            log.warning("sweep: Kalshi scan failed: %s", e)
            kalshi_snapshot = {}

    # NWS snapshot for Signal C dead bracket rank computation
    if nws_snapshot is None:
        try:
            import nws_feed
            nws_snapshot = nws_feed.snapshot(city_filter)
        except Exception as e:
            log.warning("sweep: NWS fetch failed: %s", e)
            nws_snapshot = {}

    month = datetime.now(timezone.utc).month

    for city, meta in cities.items():
        tz     = meta["tz"]
        series = _high_series(meta)
        if not series:
            continue

        # ── Ensure next_date is initialised ──────────────────────────────
        if city not in _active_next_date:
            _active_next_date[city] = _tomorrow_str(tz)

        # ── Check if today's market has converged → advance next_date ─────
        today_str     = _today_str(tz)
        today_markets = _fetch_markets(client, series, today_str)
        if today_markets and _is_converged(today_markets):
            expected_next = _day_after_tomorrow_str(tz)
            if _active_next_date[city] != expected_next:
                log.info("sweep: %s today (%s) converged → advancing to %s",
                         city, today_str, expected_next)
                _active_next_date[city] = expected_next

        next_date = _active_next_date[city]

        # ── Signal A: directional pre-market ─────────────────────────────
        markets = _fetch_markets(client, series, next_date)
        if markets:
            T_markets = sorted(
                [m for m in markets if m.get("bracket", "").startswith("T")],
                key=lambda m: _bracket_val(m.get("bracket", "")) or 0,
            )
            B_markets = sorted(
                [m for m in markets if m.get("bracket", "").startswith("B")],
                key=lambda m: _bracket_val(m.get("bracket", "")) or 0,
            )
            all_markets = T_markets + B_markets

            if len(T_markets) >= 2 and B_markets:
                t_low, t_high = T_markets[0], T_markets[-1]
                b_adj_low     = B_markets[0]
                b_adj_high    = B_markets[-1]
                sig_key       = (city, next_date)

                # Forecast shift detection
                _forecast_shift = None
                try:
                    from nws_feed import forecast_shift_tracker as _fst
                    import nws_feed as _nws
                    _tmr_nws  = _nws.snapshot(city_filter=city)
                    _tmr_fcst = (_tmr_nws.get(city) or {}).get("forecast_high_f")
                    _forecast_shift = _fst.update_high(f"{city}_tomorrow", _tmr_fcst)
                    if _forecast_shift is not None and _forecast_shift <= -1.5:
                        log.info("FORECAST_SHIFT_TMR  %s  %s  Δ%+.1f°F — scanning immediately",
                                 city, next_date, _forecast_shift)
                except Exception:
                    pass

                # Directional signal
                if sig_key not in _fired_signals:
                    signal = _check_directional(t_low, t_high, b_adj_low, b_adj_high)
                    if signal:
                        trigger_t, adj_b, t_no, b_no = signal
                        log.info("★ DIRECTIONAL [%s] %s | %s No=%.2f  adj-B %s No=%.2f",
                                 city, next_date, trigger_t.get("bracket",""),
                                 t_no, adj_b.get("bracket",""), b_no)
                        orders = 0
                        for bracket_dict, label, tier in [
                            (trigger_t, "T-trigger", "tomorrow"),
                            (adj_b,     "adj-B",     "tomorrow"),
                        ]:
                            ticker = bracket_dict["ticker"]
                            no_p   = float(bracket_dict.get("no_price", 0.0) or 0.0)
                            if no_p <= 0.0 or no_p > NO_MAX_ENTRY:
                                log.info("sweep: skip %s — No=%.2f out of range", ticker, no_p)
                                continue
                            if no_p < NO_MIN_ENTRY:
                                log.info("sweep: skip %s — No=%.2f below floor %.2f (%s)",
                                         ticker, no_p, NO_MIN_ENTRY, label)
                                continue
                            if _place(client, ticker, city, no_p,
                                      DIRECTIONAL_CONTRACTS, tier,
                                      ["directional_signal", f"label={label}",
                                       f"no_price={no_p:.2f}"],
                                      paper, _trader):
                                orders += 1
                        if orders > 0:
                            _fired_signals.add(sig_key)

                # Dismissed-T signal
                dismissed = _check_dismissed(t_low, t_high, city)
                if dismissed:
                    ticker = dismissed.get("ticker", "")
                    no_p   = float(dismissed.get("no_price", 0.0) or 0.0)
                    log.info("★ DISMISSED_T [%s] %s | %s No=%.2f",
                             city, next_date, dismissed.get("bracket",""), no_p)
                    _place(client, ticker, city, no_p,
                           DISMISSED_CONTRACTS, "tomorrow_dismissed",
                           ["dismissed_t", f"no_price={no_p:.2f}"],
                           paper, _trader)

                # Gradient-open signal
                gradient = _check_gradient(all_markets, city)
                if not gradient and _forecast_shift is not None and _forecast_shift <= -1.5:
                    gradient = _check_gradient(
                        all_markets, city, yes_override=YES_DISMISSED * 3)
                    if gradient:
                        log.info("★ GRADIENT_SHIFT [%s] %s | Δ%+.1f°F → %d bracket(s)",
                                 city, next_date, _forecast_shift, len(gradient))
                if gradient:
                    log.info("★ GRADIENT_OPEN [%s] %s | %d bracket(s)",
                             city, next_date, len(gradient))
                    for b in gradient:
                        ticker = b.get("ticker", "")
                        no_p   = float(b.get("no_price", 0.0) or 0.0)
                        _place(client, ticker, city, no_p,
                               DISMISSED_CONTRACTS, "tomorrow_dismissed",
                               ["gradient_open", f"no_price={no_p:.2f}"],
                               paper, _trader)

        # ── Signal B: near-dead sweep (today's markets) ───────────────────
        today_data = kalshi_snapshot.get(city, {})
        if not today_data.get("error"):
            today_brackets = today_data.get("brackets", [])
            for candidate in _check_sweep(today_brackets, city):
                ticker = candidate.get("ticker", "")
                no_p   = float(candidate.get("no_price", 0.0) or 0.0)
                log.info("★ SWEEP [%s] %s | No=%.2f  %dc",
                         city, ticker.split("-")[-1], no_p, SWEEP_CONTRACTS)
                _place(client, ticker, city, no_p,
                       SWEEP_CONTRACTS, "tomorrow_sweep",
                       ["near_dead_sweep", f"no_price={no_p:.2f}"],
                       paper, _trader)

        # ── Signal C: dead bracket (today's markets) ──────────────────────
        nws_city  = nws_snapshot.get(city, {})
        fcst_high = nws_city.get("forecast_high_f")
        if fcst_high is not None and not today_data.get("error"):
            today_brackets = today_data.get("brackets", [])
            for candidate in _check_dead(today_brackets, city, month, fcst_high):
                ticker = candidate.get("ticker", "")
                no_p   = float(candidate.get("no_price", 0.0) or 0.0)
                bracket_code = candidate.get("bracket") or ticker.split("-")[-1]
                btype = ("bottom_T" if _is_bottom_t(bracket_code, fcst_high)
                         else "top_T" if bracket_code.startswith("T") else "B")
                log.info("★ DEAD [%s] %s | No=%.3f  type=%s  %dc",
                         city, ticker.split("-")[-1], no_p, btype, DEAD_CONTRACTS)
                _place(client, ticker, city, no_p,
                       DEAD_CONTRACTS, "dead_sweep",
                       ["dead_bracket", f"type={btype}",
                        f"no_price={no_p:.3f}", f"fcst={fcst_high:.1f}"],
                       paper, _trader)


# ---------------------------------------------------------------------------
# Config log
# ---------------------------------------------------------------------------

def log_config() -> None:
    log.info(
        "sweep_engine: "
        "A=directional[No %.2f-%.2f, %dc]  "
        "B=sweep[city_floor-%.2f, %dc, %d cities]  "
        "C=dead[%.3f-%.3f, %dc, rank 4-5]",
        NO_MIN_ENTRY, NO_MAX_ENTRY, DIRECTIONAL_CONTRACTS,
        SWEEP_CEILING, SWEEP_CONTRACTS, len(SWEEP_FLOORS),
        DEAD_FLOOR, DEAD_CEILING, DEAD_CONTRACTS,
    )


# ---------------------------------------------------------------------------
# Standalone entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Unified bracket sweep engine")
    parser.add_argument("--paper", action="store_true")
    parser.add_argument("--city",  type=str, default=None)
    args = parser.parse_args()

    load_config_env()

    import trader
    client = trader.make_client()
    initialise(client=client, city_filter=args.city)
    log_config()
    run_scan(client=client, city_filter=args.city, paper=args.paper)
