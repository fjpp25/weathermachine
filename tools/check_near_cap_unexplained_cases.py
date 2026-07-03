"""
tools/check_near_cap_unexplained_cases.py

Follow-up to tools/probe_near_cap_reachability.py. That probe found 5
qualifying near_cap opportunities within the last 30 days that produced NO
corresponding "NEAR_CAP" log line — genuinely unexplained, not rarity.

This script checks each of those 5 cases against data/trade_log.json (the
LIVE file, not a stale snapshot) for three things:

  1. Any trade at all (any engine, any market_type) for that city on that
     date — a total absence suggests a citywide pause/outage that day, not
     a near_cap-specific bug.
  2. A 'main'-tier trade for the EXACT target ticker near_cap wanted, timed
     before the near_cap detection poll — this is the already_traded
     dedup hypothesis (hight_decision_engine.py's near_cap block skips a
     ticker BEFORE logging if the main engine's own NO-signal selection
     already claimed it that scan). This is checkable directly and was
     already confirmed for one case (New Orleans 2026-06-06) using a
     partial trade_log.json snapshot; this script re-verifies it against
     the live file and checks the other four.
  3. Whether the city is CURRENTLY in PAUSED_CITIES (informational only —
     PAUSED_CITIES is built once at hight_decision_engine.py import time,
     so this reflects current state, not necessarily state on the
     historical date in question. Flagged as a caveat, not proof.).

Usage (on the Pi, from repo root):
    python3 tools/check_near_cap_unexplained_cases.py
"""

import json
import sys
from datetime import datetime, timedelta
from pathlib import Path

CASES = [
    # (city, ticker_code, date, target_ticker, near_cap_detection_time_utc)
    ("New Orleans",   "NOLA", "2026-06-04", "KXHIGHTNOLA-26JUN04-B86.5", "2026-06-04T15:23:45"),
    ("New Orleans",   "NOLA", "2026-06-06", "KXHIGHTNOLA-26JUN06-B88.5", "2026-06-06T16:22:27"),
    ("New York",      "NY",   "2026-06-10", "KXHIGHNY-26JUN10-B79.5",    "2026-06-10T15:58:30"),
    ("Los Angeles",   "LAX",  "2026-06-19", "KXHIGHLAX-26JUN19-B72.5",   "2026-06-19T18:17:25"),
    ("San Francisco", "SFO",  "2026-07-02", "KXHIGHTSFO-26JUL02-B68.5",  "2026-07-02T18:32:11"),
]

TRADE_LOG_PATH = Path("data/trade_log.json")


def load_trade_log():
    if not TRADE_LOG_PATH.exists():
        print(f"ERROR: {TRADE_LOG_PATH} not found. Run this from the weathermachine repo root.")
        sys.exit(1)
    return json.loads(TRADE_LOG_PATH.read_text())


def city_ticker_matches(ticker: str, code: str) -> bool:
    return ticker.startswith(f"KXHIGH{code}-") or ticker.startswith(f"KXHIGHT{code}-")


def check_paused_cities():
    """Best-effort import of PAUSED_CITIES for current-state context only."""
    try:
        import hight_decision_engine as hde
        return hde.PAUSED_CITIES
    except Exception as e:
        print(f"(Could not import hight_decision_engine to check PAUSED_CITIES: {e})")
        return None


def main():
    trades = load_trade_log()
    paused_cities = check_paused_cities()

    for city, code, date, target_ticker, detect_time_str in CASES:
        print(f"{'='*70}")
        print(f"{city} ({code})  {date}   target={target_ticker}")
        print(f"near_cap detection poll (from probe output): {detect_time_str} UTC")

        detect_time = datetime.fromisoformat(detect_time_str)

        # 1. Any trade at all for this city on this date (any engine)
        same_city_day = [t for t in trades
                          if t["placed_at"].startswith(date)
                          and city_ticker_matches(t["ticker"], code)]
        if not same_city_day:
            print("  [1] No trades AT ALL for this city on this date (any engine).")
            print("      -> Consistent with a citywide pause/outage that day, not a")
            print("         near_cap-specific bug. Check data/config.json history / logs")
            print("         for this city around this date if you want to confirm.")
        else:
            print(f"  [1] {len(same_city_day)} trade(s) found for this city/date:")
            for t in sorted(same_city_day, key=lambda x: x["placed_at"]):
                print(f"        {t['placed_at']}  {t['ticker']}  tier={t['entry_tier']}")

        # 2. Exact-ticker main-tier trade before the detection time
        exact_ticker_main = [
            t for t in same_city_day
            if t["ticker"] == target_ticker and t["entry_tier"] == "main"
        ]
        if exact_ticker_main:
            earliest = min(exact_ticker_main, key=lambda x: x["placed_at"])
            earliest_dt = datetime.fromisoformat(earliest["placed_at"].split("+")[0])
            before = earliest_dt < detect_time
            print(f"  [2] MAIN-tier trade found for the EXACT target ticker, "
                  f"earliest at {earliest['placed_at']}"
                  f"  ({'BEFORE' if before else 'AFTER'} the near_cap detection poll)")
            if before:
                print("      -> Supports the already_traded dedup hypothesis: near_cap's "
                      "own code explicitly skips (before logging) any target ticker "
                      "already claimed by the main engine's own NO-signal selection "
                      "that scan.")
        else:
            print("  [2] No main-tier trade for the exact target ticker found.")
            print("      -> already_traded dedup does NOT explain this case (at least "
                  "not via the persisted trade log — in-memory-only skipped signals "
                  "from the same scan wouldn't appear here either way).")

        # 3. Current pause status (informational, not historical proof)
        if paused_cities is not None:
            is_paused_now = city in paused_cities
            print(f"  [3] Currently in PAUSED_CITIES: {is_paused_now}  "
                  f"(reflects CURRENT state only — PAUSED_CITIES is built once at "
                  f"import time, not historical state on {date})")
        print()


if __name__ == "__main__":
    main()
