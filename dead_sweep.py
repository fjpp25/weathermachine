"""
dead_sweep.py
-------------
Dead-bracket sweep engine for HIGH temperature markets.

Targets rank-5 brackets (farthest from the NWS forecast across all 6
brackets in a market) that are priced at No >= 0.97 at the time of
observation. Based on analysis of 1,464 dead-at-open rank-5 observations
(Apr–May 2026), these have a 99.93% win rate — 2 genuine failures, both
in the same city+month combination (Houston May, San Francisco May).

Relationship to tomorrow_scanner sweep
---------------------------------------
tomorrow_scanner._get_sweep_candidates() handles brackets with No in
[city_floor, 0.97). This engine handles No >= 0.97 — the two are
complementary with no overlap (SWEEP_CEILING = 0.97 in tomorrow_scanner).

Signal conditions
-----------------
  1. No price >= 0.97 (bracket is dead)
  2. Bracket is rank-5 across the 6-bracket market (farthest from NWS
     forecast by absolute distance)
  3. Passes the city + bracket-type safety filter

Safety filter (from forecast error analysis)
--------------------------------------------
  SAFE (always enter):
    Phoenix, Las Vegas, Seattle, Miami, Washington DC
    → 0% chance of >6°F upside miss; 0 failures in dataset

  CAUTION (skip in peak-risk month only):
    San Francisco, Houston, Atlanta  → skip in May
    Los Angeles, New Orleans         → skip in April
    → failure rate below break-even outside bad month

  AVOID (never enter top-T or far-B):
    All other cities (Chicago, NY, Boston, Philadelphia, Denver,
    Minneapolis, Oklahoma City, Dallas, Austin, San Antonio)
    → right-tail overshoot rate >= 2%, EV negative

  Bottom-T brackets (value < forecast): ALWAYS safe in any city.
    → 0 failures across 590 observations in all cities and months.
    The NWS downward bias means temperatures almost never fall further
    below an already-cold forecast.

Timing
------
Kalshi opens HIGH markets at 10:00am EDT (14:00 UTC) the day before
the market date. The sweep fires whenever a qualifying bracket appears
during any regular poll. _fired ensures each ticker is entered once.

Capital
-------
Uses the 'tomorrow' budget (shared with tomorrow_scanner). Dead-sweep
cost per entry is ~$4.90 (5c × $0.98), payout ~$0.09. Negligible
impact on the tomorrow budget.

Backtest summary (safe cities, rank-5, No >= 0.97)
---------------------------------------------------
  Observations : 1,464
  Win rate     : 99.93%  (2 genuine failures, both top-T in bad month)
  Avg payout   : 1.68¢ per contract
  EV           : +1.67¢ per contract
  Break-even   : failure rate must stay below ~1.7% to remain +EV
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from typing import Optional

from cities import CITIES as _CITIES
from log_setup import get_logger
from market_utils import (
    local_hour      as _local_hour,
    no_price        as _no_price,
    yes_price       as _yes_price,
    load_config_env,
)

log = get_logger(__name__)

# ---------------------------------------------------------------------------
# Parameters
# ---------------------------------------------------------------------------

NO_DEAD_FLOOR   = 0.97   # bracket must be at or above this — anything lower is
                          # already handled by tomorrow_scanner's sweep (ceiling=0.97)
NO_DEAD_CEILING = 0.989  # above this, Yes is $0.01 — fills collapse, skip
MAX_CONTRACTS  = 5       # flat sizing — small absolute risk per bracket

# ---------------------------------------------------------------------------
# Safety filter
# ---------------------------------------------------------------------------

# Always safe: 0% historical chance of >6°F upside forecast miss
_SAFE_ALWAYS: frozenset[str] = frozenset({
    "Phoenix", "Las Vegas", "Seattle", "Miami", "Washington DC",
})

# (city, month) combinations to SKIP for top-T and far-B brackets.
# Outside these months, these cities are below the break-even failure rate.
_CAUTION_SKIP: dict[str, frozenset[int]] = {
    "San Francisco": frozenset({5}),   # May: 2.0% >6°F miss rate
    "Houston":       frozenset({5}),   # May: 1.25%
    "Atlanta":       frozenset({5}),   # May: 1.35%
    "Los Angeles":   frozenset({4}),   # April: 1.9%
    "New Orleans":   frozenset({4}),   # April: 1.2%
}

# ---------------------------------------------------------------------------
# Session state
# ---------------------------------------------------------------------------

# Tickers entered this session — prevents re-entry across poll cycles
_fired: set[str] = set()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _bracket_val(bracket_code: str) -> Optional[float]:
    """
    Extract the numeric temperature value from a bracket code.
    B82.5 → 82.5,  T69 → 69.0,  T55 → 55.0
    """
    if bracket_code and bracket_code[0] in ("B", "T"):
        try:
            return float(bracket_code[1:])
        except ValueError:
            pass
    return None


def _is_bottom_t(bracket_code: str, forecast_high: float) -> bool:
    """
    True if this is a T bracket whose threshold is BELOW the forecast.
    Bottom-T on a HIGH market = resolves YES only if temp falls far below
    forecast — structurally safe regardless of city or season.
    """
    if not bracket_code.startswith("T"):
        return False
    val = _bracket_val(bracket_code)
    return val is not None and val < forecast_high


def _is_safe(
    bracket_code:  str,
    city:          str,
    month:         int,
    forecast_high: float,
) -> bool:
    """
    Return True if this bracket qualifies for the dead sweep.

    Bottom-T brackets are always safe (structural, not city-dependent).
    For all others, apply the city × month safety tier.
    """
    # Bottom-T: safe everywhere, always
    if _is_bottom_t(bracket_code, forecast_high):
        return True

    # Safe-always cities: enter without restriction
    if city in _SAFE_ALWAYS:
        return True

    # Caution cities: skip only in the identified bad month
    if city in _CAUTION_SKIP:
        return month not in _CAUTION_SKIP[city]

    # All other cities: avoid (>= 2% right-tail overshoot rate)
    return False


def _compute_fcst_rank(brackets: list[dict], forecast_high: float) -> dict[str, int]:
    """
    Assign a forecast-distance rank to each bracket ticker.
    Rank 0 = closest to forecast, rank 5 = farthest (for a 6-bracket market).
    Returns {ticker: rank}.
    """
    def dist(b: dict) -> float:
        code = b.get("bracket") or b.get("ticker", "").split("-")[-1]
        val  = _bracket_val(code)
        return abs(val - forecast_high) if val is not None else 0.0

    sorted_brackets = sorted(brackets, key=dist)
    return {b["ticker"]: i for i, b in enumerate(sorted_brackets)}


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
    Scan all HIGH markets for dead rank-5 bracket sweep opportunities.

    Args:
        client:           Authenticated KalshiClient instance.
        city_filter:      Optional city name to restrict scan to one city.
        paper:            If True, log but do not place orders.
        kalshi_snapshot:  Pre-fetched kalshi_scanner.scan_all (HIGH) results.
        nws_snapshot:     Pre-fetched nws_feed.snapshot() results.
    """
    import trader as _trader
    import kalshi_scanner as _ks

    cities = list(_CITIES.keys())
    if city_filter:
        cities = [c for c in cities if c.lower() == city_filter.lower()]

    if kalshi_snapshot is None:
        try:
            kalshi_snapshot = _ks.scan_all(city_filter=city_filter, market_type="high")
        except Exception as e:
            log.warning("dead_sweep: Kalshi scan failed: %s", e)
            return

    if nws_snapshot is None:
        try:
            import nws_feed
            nws_snapshot = nws_feed.snapshot(city_filter)
        except Exception as e:
            log.warning("dead_sweep: NWS fetch failed: %s", e)
            nws_snapshot = {}

    month = datetime.now(timezone.utc).month

    for city in cities:
        scan_data = kalshi_snapshot.get(city, {})
        if scan_data.get("error"):
            log.debug("dead_sweep: %s scan error: %s", city, scan_data["error"])
            continue

        brackets = scan_data.get("brackets", [])
        if len(brackets) < 4:   # guard against incomplete fetches
            continue

        # NWS forecast for this city — required for rank computation
        nws_city  = nws_snapshot.get(city, {}) if nws_snapshot else {}
        fcst_high = nws_city.get("forecast_high_f")
        if fcst_high is None:
            log.debug("dead_sweep: %s — no NWS forecast, skipping", city)
            continue

        _check_city(city, brackets, fcst_high, month, client, paper, _trader)


def _check_city(
    city:      str,
    brackets:  list[dict],
    fcst_high: float,
    month:     int,
    client,
    paper:     bool,
    _trader,
) -> None:
    """
    Evaluate one city's brackets for dead-sweep entries.
    """
    # Only act on 6-bracket markets (structure assumed by rank analysis)
    if len(brackets) != 6:
        log.debug("dead_sweep: %s SKIP — %d brackets (expected 6)", city, len(brackets))
        return

    # Rank all brackets by distance from NWS forecast
    rank_map = _compute_fcst_rank(brackets, fcst_high)

    for bracket in brackets:
        ticker = bracket.get("ticker", "")
        if not ticker:
            continue

        # Only rank-5 (farthest from forecast)
        if rank_map.get(ticker) != 5:
            continue

        # Must be dead and within fillable range
        no_p = _no_price(bracket)
        if not (NO_DEAD_FLOOR <= no_p <= NO_DEAD_CEILING):
            log.debug(
                "dead_sweep: %s %s SKIP — No=%.3f outside [%.3f, %.3f]",
                city, ticker, no_p, NO_DEAD_FLOOR, NO_DEAD_CEILING,
            )
            continue

        # De-duplicate: one entry per ticker per session
        if ticker in _fired:
            continue

        bracket_code = bracket.get("bracket") or ticker.split("-")[-1]

        # Safety filter
        if not _is_safe(bracket_code, city, month, fcst_high):
            log.debug(
                "dead_sweep: %s %s SKIP — city/month not safe "
                "(bracket=%s city=%s month=%d)",
                city, ticker, bracket_code, city, month,
            )
            continue

        # Determine bracket type for logging
        is_bot_t = _is_bottom_t(bracket_code, fcst_high)
        btype    = "bottom_T" if is_bot_t else (
                   "top_T"    if bracket_code.startswith("T") else "B")

        log.info(
            "DEAD_SWEEP  %s  %s  No=%.3f  rank=5  type=%s  fcst=%.1f°F  %dc",
            city, ticker, no_p, btype, fcst_high, MAX_CONTRACTS,
        )

        # Capital check — uses tomorrow budget
        cost = round(no_p * MAX_CONTRACTS, 4)
        try:
            deployable = _trader.get_tomorrow_deployable(ticker)
            if deployable < cost:
                log.debug(
                    "dead_sweep: %s %s — tomorrow budget exhausted "
                    "(need=$%.2f  have=$%.2f)",
                    city, ticker, cost, deployable,
                )
                continue
        except Exception as e:
            log.warning("dead_sweep: capital check failed %s: %s", ticker, e)
            continue

        _fired.add(ticker)

        if not paper:
            try:
                _trader.place_order(
                    client        = client,
                    ticker        = ticker,
                    side          = "no",
                    price_dollars = no_p,
                    contracts     = MAX_CONTRACTS,
                    paper         = False,
                )
                _trader._append_trade_log({
                    "ticker":       ticker,
                    "city":         city,
                    "side":         "no",
                    "market_type":  "high",
                    "score":        5,
                    "score_detail": [
                        "dead_sweep",
                        f"rank=5",
                        f"type={btype}",
                        f"no_price={no_p:.3f}",
                        f"fcst={fcst_high:.1f}",
                    ],
                    "entry_price":  no_p,
                    "contracts":    MAX_CONTRACTS,
                    "placed_at":    datetime.now(timezone.utc).isoformat(),
                    "paper":        False,
                    "entry_tier":   "dead_sweep",
                })
                _trader.record_tomorrow_deployed(cost, ticker)
            except Exception as e:
                log.error("dead_sweep: order failed %s: %s", ticker, e)
                _fired.discard(ticker)   # allow retry on next poll if order failed
        else:
            log.info(
                "  [PAPER] would place No %dc @ $%.3f on %s  (type=%s)",
                MAX_CONTRACTS, no_p, ticker, btype,
            )


# ---------------------------------------------------------------------------
# Config log — called once at scheduler startup
# ---------------------------------------------------------------------------

def log_config() -> None:
    log.info(
        "dead_sweep: No=[%.3f, %.3f]  contracts=%d  safe=%s  caution=%s",
        NO_DEAD_FLOOR, NO_DEAD_CEILING, MAX_CONTRACTS,
        sorted(_SAFE_ALWAYS),
        {c: list(m) for c, m in _CAUTION_SKIP.items()},
    )


# ---------------------------------------------------------------------------
# Standalone entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Dead-bracket sweep scanner")
    parser.add_argument("--paper", action="store_true")
    parser.add_argument("--city",  type=str, default=None)
    args = parser.parse_args()

    load_config_env()

    import trader
    client = trader.make_client()
    log_config()
    run_scan(client=client, city_filter=args.city, paper=args.paper)
