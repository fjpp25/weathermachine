"""
peak_scanner.py
---------------
Intraday bracket capture engine. Enters No on brackets where the current
observed high is already AT the bracket floor, and the city's daily peak
temperature is reliably in the afternoon — meaning the temperature will
almost certainly continue rising past that floor before end of day.

Signal logic
------------
For each poll:
  1. Get current observed high for the city (from NWS snapshot)
  2. Find any B bracket whose floor is within 0.5F below obs_high
     (temperature is right at the floor — about to break through)
  3. Check: local hour < city P90 peak hour (still before typical peak)
  4. Check: No price >= NO_MIN_ENTRY (0.80)
  5. Enter No

Backtest (Apr 7-27, 2026 — 10 clean cities):
  179 signals  WR=92.2%  EV=+$0.039  Total=+$6.88  0 bad days

Cities (low peak-hour variance, predictable afternoon peak):
  Atlanta, Boston, Chicago, Denver, Houston,
  Los Angeles, New Orleans, Phoenix, San Francisco, Seattle

Excluded (high variance): Miami, Minneapolis, New York, Philadelphia,
  Washington DC, Austin, Dallas, Las Vegas, Oklahoma City, San Antonio
"""

from __future__ import annotations
import argparse
from datetime import datetime, timezone
from typing import Optional
from log_setup import get_logger
from market_utils import (
    local_hour as _local_hour,
    no_price   as _no_price,
    load_config_env,
)

log = get_logger(__name__)

NO_MIN_ENTRY  = 0.80
NO_MAX_ENTRY  = 0.92
MAX_CONTRACTS = 3
OBS_FLOOR_GAP = 0.5   # obs_high must be within this of bracket floor

PEAK_CITIES: dict[str, int] = {
    "Atlanta":       17,
    "Boston":        17,
    "Chicago":       19,
    "Denver":        18,
    "Houston":       19,
    "Los Angeles":   17,
    "New Orleans":   15,
    "Phoenix":       17,
    "San Francisco": 17,
    "Seattle":       17,
}

_fired: set[tuple[str, str]] = set()

def _bracket_floor(bracket: dict) -> Optional[float]:
    floor = bracket.get("floor")
    cap   = bracket.get("cap")
    if floor is not None and cap is not None:
        return float(floor)
    return None


def _check_city(city, obs_high, brackets, local_hour):
    if local_hour >= PEAK_CITIES[city]:
        return []
    signals = []
    for bracket in brackets:
        ticker = bracket.get("ticker", "")
        if not ticker:
            continue
        # B brackets only
        bracket_code = ticker.split("-")[-1] if "-" in ticker else ""
        if not bracket_code.startswith("B"):
            continue
        floor = _bracket_floor(bracket)
        if floor is None:
            continue
        if not (floor - OBS_FLOOR_GAP <= obs_high <= floor + 0.1):
            continue
        no_p = _no_price(bracket)
        if not (NO_MIN_ENTRY <= no_p <= NO_MAX_ENTRY):
            continue
        if (city, ticker) in _fired:
            continue
        signals.append(bracket)
    return signals


def log_config() -> None:
    log.info(
        "peak_scanner: NO=[%.2f, %.2f]  contracts=%d  gap=%.1fF  cities=%s",
        NO_MIN_ENTRY, NO_MAX_ENTRY, MAX_CONTRACTS, OBS_FLOOR_GAP,
        list(PEAK_CITIES.keys()),
    )


def run_scan(client, city_filter=None, paper=False, nws_snapshot=None, kalshi_snapshot=None):
    import trader as _trader
    import kalshi_scanner as _ks

    cities = {c: p for c, p in PEAK_CITIES.items()
              if city_filter is None or c.lower() == city_filter.lower()}
    if not cities:
        return

    if nws_snapshot is None:
        try:
            import nws_feed
            nws_snapshot = nws_feed.snapshot()
        except Exception as e:
            log.warning("peak_scanner: NWS fetch failed: %s", e)
            nws_snapshot = {}

    try:
        kalshi_results = kalshi_snapshot if kalshi_snapshot is not None else \
                         _ks.scan_all(city_filter=city_filter, market_type="high")
    except Exception as e:
        log.warning("peak_scanner: Kalshi scan failed: %s", e)
        return

    for city in cities:
        nws_data   = nws_snapshot.get(city, {})
        obs_high   = nws_data.get("observed_high_f")
        if obs_high is None or obs_high <= 0:
            continue

        local_hour = _local_hour(city)
        brackets   = kalshi_results.get(city, {}).get("brackets", [])
        if not brackets:
            continue

        for bracket in _check_city(city, obs_high, brackets, local_hour):
            ticker = bracket.get("ticker", "")
            no_p   = _no_price(bracket)
            floor  = _bracket_floor(bracket)

            log.info(
                "PEAK  %s  %s  No=%.2f  obs=%.1fF  floor=%.1fF  "
                "hour=%d  safe_until=%dh  %dc",
                city, ticker, no_p, obs_high, floor,
                local_hour, PEAK_CITIES[city], MAX_CONTRACTS,
            )

            _fired.add((city, ticker))

            # Capital check against market-date-scoped peak budget
            try:
                from trader import get_peak_deployable as _gpd, record_peak_deployed as _rpd
                cost = no_p * MAX_CONTRACTS
                if _gpd(ticker) < cost:
                    log.debug("peak_scanner: %s — peak budget exhausted "
                              "(cost=$%.2f  remaining=$%.2f)",
                              ticker, cost, _gpd(ticker))
                    continue
            except Exception:
                pass  # proceed without capital check if unavailable

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
                        "score":        3,
                        "score_detail": ["peak_obs_at_floor",
                                         "before_peak_hour",
                                         "no_price_quality"],
                        "entry_price":  no_p,
                        "contracts":    MAX_CONTRACTS,
                        "placed_at":    datetime.now(timezone.utc).isoformat(),
                        "paper":        False,
                        "entry_tier":   "peak",
                    })
                    try:
                        from trader import record_peak_deployed as _rpd
                        _rpd(no_p * MAX_CONTRACTS, ticker)
                    except Exception:
                        pass
                except Exception as e:
                    log.error("peak_scanner: order failed %s: %s", ticker, e)
            else:
                log.info("  [PAPER] would place No %dc @ $%.2f on %s",
                         MAX_CONTRACTS, no_p, ticker)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--paper", action="store_true")
    parser.add_argument("--city",  type=str, default=None)
    args = parser.parse_args()

    load_config_env()

    import trader
    client = trader.make_client()
    log_config()
    run_scan(client=client, city_filter=args.city, paper=args.paper)
