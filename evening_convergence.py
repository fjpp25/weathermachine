"""
evening_convergence.py
----------------------
Evening bracket convergence signal engine for HIGH temperature markets.

Signal logic
------------
Fires when, in the evening window (>=19h local), exactly 3 brackets remain
active in a HIGH market and a non-forecast B bracket has No >= 0.85.

Derived from backtest analysis of the lowt_observations dataset
(Apr 6 – May 7 2026, 27 trading days, 20 cities):

  Master signal conditions:
    - B brackets only (T brackets show only 33.5% No-win in this context)
    - Exactly 3 active brackets (neither side >= 0.95)
    - This bracket is NOT the highest Yes-price bracket (not the forecast)
    - Local hour >= 19
    - No price in [0.85, 0.97]

  Results: 456 poll-signals, 98.7% No-win, EV@0.93 = +$0.057

Structural explanation
----------------------
With exactly 3 active brackets in the evening, the market has converged
to one of two configurations:

  HEAD & SHOULDERS  (middle bracket hottest):
    Temperature is expected to settle in the middle bracket. Both flanking
    brackets — the one already passed and the one not yet reached — are
    safe No bets. The market structure itself confirms the thesis; no
    additional temperature check is required (100% No-win in backtest
    for both flanking brackets in this configuration at these parameters).

  LOWEST/HIGHEST HOT  (extreme bracket hottest):
    Temperature has peaked at one end. The two brackets on the other side
    are structurally eliminated. Same result: 99-100% No-win in backtest.

The streak condition was dropped: with 3 active brackets at 7pm, the
market has already done the repricing work across the full day. Streak
added latency without improving win rate (removing it improved WR from
93.9% to 94.6% in the backtest grid search).

Capital
-------
Draws from the 'econv' engine allocation (5% of day-open balance).
Flat sizing: MAX_CONTRACTS per signal regardless of price.

Backtest summary (B only, non-forecast, 3 active, hr>=19, No 0.85-0.97):
  Trades: ~254 unique (city, date, bracket) entries over 27 days
  Win rate: 94.8%   EV@0.93: +$0.018
  With no-streak condition: 98.7% WR on the 456-poll master signal subset
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from typing import Optional

from cities import CITIES as _CITIES
from log_setup import get_logger
from market_utils import (
    local_hour   as _local_hour,
    no_price     as _no_price,
    yes_price    as _yes_price,
    is_resolved  as _is_resolved,
    is_b_bracket as _is_b_bracket,
    load_config_env,
)

log = get_logger(__name__)

# ---------------------------------------------------------------------------
# Parameters
# ---------------------------------------------------------------------------

NO_MIN_ENTRY        = 0.85   # minimum No price — backtest shows same WR to 0.82,
                              # modest cushion below 0.85 for safety
NO_MAX_ENTRY        = 0.97   # maximum No price — EV compresses sharply above 0.97
RESOLVED_THRESHOLD  = 0.95   # bracket is resolved when either side >= this
MIN_LOCAL_HOUR      = 19     # entry window opens (local) — below 19h, WR degrades
MAX_ACTIVE_BRACKETS = 3      # signal only fires when exactly this many are active
MAX_CONTRACTS       = 4      # flat sizing per entry

# ---------------------------------------------------------------------------
# Session state
# ---------------------------------------------------------------------------

# (city, ticker) pairs already traded this session — one entry per bracket per day
_fired: set[tuple[str, str]] = set()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _forecast_bracket(active: list[dict]) -> Optional[dict]:
    """
    Return the bracket with the highest Yes price among the active set.
    This is the market's best guess for where the temperature will settle.
    """
    if not active:
        return None
    return max(active, key=_yes_price)


# ---------------------------------------------------------------------------
# Core scan
# ---------------------------------------------------------------------------

def run_scan(
    client,
    city_filter:      str  = None,
    paper:            bool = False,
    kalshi_snapshot:  dict = None,
) -> None:
    """
    Scan all HIGH markets for evening convergence setups and place orders.

    Args:
        client:           Authenticated KalshiClient instance.
        city_filter:      Optional city name to restrict scan to one city.
        paper:            If True, log signal but do not place orders.
        kalshi_snapshot:  Pre-fetched kalshi_scanner.scan_all results.
                          If None, a fresh scan is performed.
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
            log.warning("evening_convergence: Kalshi scan failed: %s", e)
            return

    for city in cities:
        local_hour = _local_hour(city)
        if local_hour < MIN_LOCAL_HOUR:
            continue

        scan_data = kalshi_snapshot.get(city, {})
        if scan_data.get("error"):
            log.debug("evening_convergence: %s scan error: %s",
                      city, scan_data["error"])
            continue

        brackets = scan_data.get("brackets", [])
        if not brackets:
            continue

        _check_city(city, brackets, client, paper, _trader, local_hour)


def _check_city(
    city:       str,
    brackets:   list[dict],
    client,
    paper:      bool,
    _trader,
    local_hour: int,
) -> None:
    """
    Evaluate one city's brackets for the evening convergence setup.
    """
    # ── Count active brackets ─────────────────────────────────────────────
    active = [b for b in brackets if not _is_resolved(b)]

    if len(active) != MAX_ACTIVE_BRACKETS:
        log.debug(
            "evening_convergence: %s SKIP — %d active brackets (need %d)",
            city, len(active), MAX_ACTIVE_BRACKETS,
        )
        return

    # ── Identify the forecast bracket ────────────────────────────────────
    forecast = _forecast_bracket(active)
    forecast_ticker = forecast.get("ticker", "") if forecast else ""

    # ── Check each non-forecast B bracket ────────────────────────────────
    for bracket in active:
        ticker = bracket.get("ticker", "")

        # B brackets only — T brackets excluded (33.5% No-win in backtest)
        if not _is_b_bracket(bracket):
            log.debug(
                "evening_convergence: %s %s SKIP — T bracket excluded",
                city, ticker,
            )
            continue

        # Skip the forecast bracket — this is the contested one
        if ticker == forecast_ticker:
            continue

        no_p  = _no_price(bracket)
        yes_p = _yes_price(bracket)

        # Price gate
        if not (NO_MIN_ENTRY <= no_p <= NO_MAX_ENTRY):
            log.debug(
                "evening_convergence: %s %s SKIP — No=%.2f outside [%.2f, %.2f]",
                city, ticker, no_p, NO_MIN_ENTRY, NO_MAX_ENTRY,
            )
            continue

        # De-duplicate — one entry per (city, ticker) per session
        if (city, ticker) in _fired:
            continue

        # ── Classify the market structure for logging ─────────────────────
        # Sort active brackets by No price descending to infer position
        sorted_active = sorted(active, key=lambda b: _no_price(b), reverse=True)
        forecast_pos  = next(
            (i for i, b in enumerate(sorted_active) if b.get("ticker") == forecast_ticker),
            -1,
        )
        if forecast_pos == 0:
            structure = "LOWEST_HOT"     # hottest = top No = lowest bracket
        elif forecast_pos == len(sorted_active) - 1:
            structure = "HIGHEST_HOT"    # hottest = bottom No = highest bracket
        else:
            structure = "HEAD_SHOULDERS"

        log.info(
            "ECONV  %s  %s  No=%.2f  hour=%dh  active=3  "
            "structure=%s  forecast=%s  %dc",
            city, ticker, no_p, local_hour,
            structure, forecast_ticker.split("-")[-1] if forecast_ticker else "?",
            MAX_CONTRACTS,
        )

        _fired.add((city, ticker))

        # ── Capital check ─────────────────────────────────────────────────
        cost = round(no_p * MAX_CONTRACTS, 4)
        try:
            cap = _trader.get_engine_capital()
            if not cap.can_deploy("econv", cost):
                log.debug(
                    "evening_convergence: %s — econv budget exhausted "
                    "(cost=$%.2f)",
                    ticker, cost,
                )
                continue
        except Exception:
            pass   # proceed without capital check if unavailable

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
                try:
                    cap.record("econv", cost)
                    _trader.record_econv_deployed(cost)
                except Exception:
                    pass
                _trader._append_trade_log({
                    "ticker":       ticker,
                    "city":         city,
                    "side":         "no",
                    "market_type":  "high",
                    "score":        0,
                    "score_detail": [
                        "evening_convergence",
                        f"structure={structure}",
                        f"active=3",
                        f"hour={local_hour}h",
                        f"no_price={no_p:.2f}",
                    ],
                    "entry_price":  no_p,
                    "contracts":    MAX_CONTRACTS,
                    "placed_at":    datetime.now(timezone.utc).isoformat(),
                    "paper":        False,
                    "entry_tier":   "econv",
                })
            except Exception as e:
                log.error("evening_convergence: order failed %s: %s", ticker, e)
        else:
            log.info(
                "  [PAPER] would place No %dc @ $%.2f on %s  (structure=%s)",
                MAX_CONTRACTS, no_p, ticker, structure,
            )


# ---------------------------------------------------------------------------
# Config log — called once at scheduler startup
# ---------------------------------------------------------------------------

def log_config() -> None:
    log.info(
        "evening_convergence: NO=[%.2f, %.2f]  active=%d  min_hour=%dh  "
        "contracts=%d  cities=%d",
        NO_MIN_ENTRY, NO_MAX_ENTRY, MAX_ACTIVE_BRACKETS,
        MIN_LOCAL_HOUR, MAX_CONTRACTS, len(_CITIES),
    )


# ---------------------------------------------------------------------------
# Standalone entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Evening convergence signal scanner"
    )
    parser.add_argument("--paper", action="store_true")
    parser.add_argument("--city",  type=str, default=None)
    args = parser.parse_args()

    load_config_env()

    import trader
    client = trader.make_client()
    log_config()
    run_scan(client=client, city_filter=args.city, paper=args.paper)
