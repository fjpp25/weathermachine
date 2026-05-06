"""
last_bracket.py
---------------
Last Bracket signal engine for HIGH temperature markets.

Signal logic
------------
A "Last Bracket" setup exists when, on a HIGH temperature market:
  1. Exactly 2 brackets remain open (neither resolved Yes nor No)
  2. The upper bracket has No >= NO_MIN_ENTRY (0.80)
  3. The 2-bracket phase appeared for the FIRST TIME at or after
     MIN_LOCAL_HOUR (16h) local — "fresh" signal only

The structural edge: by 4pm local, the daily high is almost always
established. The upper bracket is still priced with residual uncertainty
(No ~80–85¢) even though the temperature is no longer climbing.
The market underprices this certainty — our backtest shows 95.2% WR
on fresh entries vs 93.5% on stale ones.

Backtest (HIGH markets, Apr 6 – May 3 2026, fresh entries >= 16h):
  126 resolved trades  |  WR 95.2%  |  Avg PnL +7.93¢/$1

Entry logic:
  - Enter NO on the upper of the two open brackets
  - Only when the 2-bracket phase is FIRST detected at >= MIN_LOCAL_HOUR
  - Markets already in 2-bracket phase before MIN_LOCAL_HOUR are skipped
  - Each (city, ticker) pair fires at most once per session

Excluded from this signal:
  The main hight_decision_engine and cascade_engine handle all other
  HIGH market entries. Last Bracket supplements them for the late-day
  convergence window only.

Cities: all 20 HIGH trading cities (same set as hight_decision_engine).
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from typing import Optional

from log_setup import get_logger

log = get_logger(__name__)

# ---------------------------------------------------------------------------
# Parameters
# ---------------------------------------------------------------------------

NO_MIN_ENTRY       = 0.80    # minimum No price to trigger the signal
NO_MAX_ENTRY       = 0.94    # skip if already priced past actionable range
RESOLVED_THRESHOLD = 0.95    # above this → bracket is resolved
MIN_LOCAL_HOUR     = 16      # only fire if 2-bracket phase first seen at >= this hour
MAX_CONTRACTS      = 3       # contracts per order

# ---------------------------------------------------------------------------
# City timezone registry
# ---------------------------------------------------------------------------

_CITY_TZ: dict[str, str] = {
    "New York":      "America/New_York",
    "Chicago":       "America/Chicago",
    "Miami":         "America/New_York",
    "Austin":        "America/Chicago",
    "Los Angeles":   "America/Los_Angeles",
    "Denver":        "America/Denver",
    "Philadelphia":  "America/New_York",
    "San Francisco": "America/Los_Angeles",
    "Boston":        "America/New_York",
    "Las Vegas":     "America/Los_Angeles",
    "Atlanta":       "America/New_York",
    "Oklahoma City": "America/Chicago",
    "Phoenix":       "America/Phoenix",
    "Washington DC": "America/New_York",
    "Seattle":       "America/Los_Angeles",
    "Houston":       "America/Chicago",
    "Dallas":        "America/Chicago",
    "San Antonio":   "America/Chicago",
    "New Orleans":   "America/Chicago",
    "Minneapolis":   "America/Chicago",
}

# ---------------------------------------------------------------------------
# Session state
# ---------------------------------------------------------------------------

# (city, ticker) pairs already traded this session — prevents re-entry
_fired: set[tuple[str, str]] = set()

# market_key -> local hour when 2-bracket phase was first observed
# Used to enforce the "fresh" condition: only trade markets that entered
# the 2-bracket phase at or after MIN_LOCAL_HOUR
_two_bracket_first_seen: dict[str, int] = {}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _local_hour(city: str) -> int:
    tz = _CITY_TZ.get(city, "UTC")
    return datetime.now(ZoneInfo(tz)).hour


def _no_price(bracket: dict) -> float:
    return float(
        bracket.get("ob_no_ask") or bracket.get("ob_no_bid")
        or bracket.get("no_price") or 0.0
    )


def _yes_price(bracket: dict) -> float:
    return float(
        bracket.get("ob_yes_ask") or bracket.get("ob_yes_bid")
        or bracket.get("yes_price") or 0.0
    )


def _is_resolved(bracket: dict) -> bool:
    """A bracket is resolved when either side has collapsed to near-certainty."""
    return _no_price(bracket) >= RESOLVED_THRESHOLD or _yes_price(bracket) >= RESOLVED_THRESHOLD


def _market_key(city: str, brackets: list[dict]) -> Optional[str]:
    """
    Derive a stable market key from the city and any bracket ticker.
    Format: <series_prefix>-<date>  e.g. 'KXHIGHNYC-26MAY05'
    Returns None if no tickers are available.
    """
    for b in brackets:
        ticker = b.get("ticker", "")
        parts = ticker.split("-")
        if len(parts) >= 2:
            return f"{parts[0]}-{parts[1]}"
    return None


def _upper_open_bracket(open_brackets: list[dict]) -> Optional[dict]:
    """
    Return the upper of the two open brackets — the one the market
    must still reach to resolve Yes. For HIGH markets this is the
    bracket with the higher floor (or the one without a floor, i.e. T bracket).
    """
    def sort_key(b: dict) -> float:
        floor = b.get("floor")
        cap   = b.get("cap")
        if floor is not None:
            return float(floor)
        if cap is not None:
            return float(cap)
        return 0.0

    return max(open_brackets, key=sort_key)


# ---------------------------------------------------------------------------
# Core scan
# ---------------------------------------------------------------------------

def run_scan(
    client,
    city_filter: str = None,
    paper: bool = False,
    kalshi_snapshot: dict = None,
) -> None:
    """
    Scan all HIGH markets for Last Bracket setups and place orders.

    Args:
        client:           Authenticated KalshiClient instance.
        city_filter:      Optional city name to restrict scan to one city.
        paper:            If True, log the signal but do not place orders.
        kalshi_snapshot:  Pre-fetched kalshi_scanner.scan_all results. If
                          None, a fresh scan is performed (one extra API call).
    """
    import trader as _trader
    import kalshi_scanner as _ks

    cities = list(_CITY_TZ.keys())
    if city_filter:
        cities = [c for c in cities if c.lower() == city_filter.lower()]

    if kalshi_snapshot is None:
        try:
            kalshi_snapshot = _ks.scan_all(city_filter=city_filter, market_type="high")
        except Exception as e:
            log.warning("last_bracket: Kalshi scan failed: %s", e)
            return

    for city in cities:
        scan_data = kalshi_snapshot.get(city, {})
        if scan_data.get("error"):
            log.debug("last_bracket: %s scan error: %s", city, scan_data["error"])
            continue

        brackets = scan_data.get("brackets", [])
        if not brackets:
            continue

        _check_city(city, brackets, client, paper, _trader)


def _check_city(
    city:     str,
    brackets: list[dict],
    client,
    paper:    bool,
    _trader,
) -> None:
    """
    Evaluate one city's brackets for a Last Bracket setup.
    Mutates _fired and _two_bracket_first_seen session state.
    """
    local_hour = _local_hour(city)
    mkey       = _market_key(city, brackets)
    if not mkey:
        return

    open_brackets = [b for b in brackets if not _is_resolved(b)]

    # Track when the 2-bracket phase begins for this market
    if len(open_brackets) == 2:
        if mkey not in _two_bracket_first_seen:
            _two_bracket_first_seen[mkey] = local_hour
            log.debug(
                "last_bracket: %s entered 2-bracket phase at hour %d",
                city, local_hour,
            )
    elif len(open_brackets) != 2:
        # Not in 2-bracket phase — nothing to do
        return

    # Enforce freshness: skip if the 2-bracket phase started before MIN_LOCAL_HOUR
    first_seen_hour = _two_bracket_first_seen.get(mkey)
    if first_seen_hour is None or first_seen_hour < MIN_LOCAL_HOUR:
        log.debug(
            "last_bracket: %s SKIP — 2-bracket phase first seen at %sh "
            "(before cutoff %dh) — stale setup",
            city, first_seen_hour, MIN_LOCAL_HOUR,
        )
        return

    # Identify the upper bracket — the one we sell No on
    upper = _upper_open_bracket(open_brackets)
    ticker = upper.get("ticker", "")
    no_p   = _no_price(upper)

    # Price gate
    if not (NO_MIN_ENTRY <= no_p <= NO_MAX_ENTRY):
        log.debug(
            "last_bracket: %s %s SKIP — No=%.2f outside range [%.2f, %.2f]",
            city, ticker, no_p, NO_MIN_ENTRY, NO_MAX_ENTRY,
        )
        return

    # De-duplicate: each (city, ticker) fires once per session
    if (city, ticker) in _fired:
        return

    floor = upper.get("floor")
    cap   = upper.get("cap")

    log.info(
        "LAST_BRACKET  %s  %s  No=%.2f  hour=%dh  fresh_since=%dh  %dc",
        city, ticker, no_p, local_hour, first_seen_hour, MAX_CONTRACTS,
    )

    _fired.add((city, ticker))

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
                "score_detail": [
                    "last_bracket_fresh",
                    f"two_bracket_since={first_seen_hour}h",
                    f"no_price={no_p:.2f}",
                ],
                "entry_price":  no_p,
                "contracts":    MAX_CONTRACTS,
                "placed_at":    datetime.now(timezone.utc).isoformat(),
                "paper":        False,
                "entry_tier":   "last_bracket",
            })
        except Exception as e:
            log.error("last_bracket: order failed %s: %s", ticker, e)
    else:
        log.info(
            "  [PAPER] would place No %dc @ $%.2f on %s  (floor=%s cap=%s)",
            MAX_CONTRACTS, no_p, ticker, floor, cap,
        )


# ---------------------------------------------------------------------------
# Config log — called once at scheduler startup
# ---------------------------------------------------------------------------

def log_config() -> None:
    log.info(
        "last_bracket: NO=[%.2f, %.2f]  min_hour=%dh  contracts=%d  cities=%d",
        NO_MIN_ENTRY, NO_MAX_ENTRY, MIN_LOCAL_HOUR, MAX_CONTRACTS, len(_CITY_TZ),
    )


# ---------------------------------------------------------------------------
# Standalone entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import os
    import json
    from pathlib import Path

    parser = argparse.ArgumentParser(description="Last Bracket signal scanner")
    parser.add_argument("--paper", action="store_true")
    parser.add_argument("--city",  type=str, default=None)
    args = parser.parse_args()

    config_file = Path("data/config.json")
    if config_file.exists():
        config = json.loads(config_file.read_text())
        if config.get("key_id"):
            os.environ.setdefault("KALSHI_KEY_ID", config["key_id"])
        if config.get("key_file"):
            os.environ.setdefault("KALSHI_KEY_FILE", config["key_file"])
        os.environ["KALSHI_DEMO"] = "false" if config.get("live_mode") else "true"

    import trader
    client = trader.make_client()
    log_config()
    run_scan(client=client, city_filter=args.city, paper=args.paper)
