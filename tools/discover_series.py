"""
discover_series.py
------------------
Queries the Kalshi API for all active temperature market series (HIGH and LOWT)
and compares them against the series configured in kalshi_scanner.py.

Reports:
  - All active series found on Kalshi
  - Series we have configured (and whether they're still active)
  - Series on Kalshi that we're NOT tracking (potential gaps)

Usage:
  python discover_series.py
  python discover_series.py --raw     # dump full API response for debugging

No auth required — series data is public.
"""

import json
import time
import argparse
import requests
from datetime import datetime, timezone

API_BASE = "https://api.elections.kalshi.com/trade-api/v2"

# Our current configuration from kalshi_scanner.py
OUR_SERIES = {
    "New York":      {"high": "KXHIGHNY",    "low": "KXLOWTNYC"},
    "Chicago":       {"high": "KXHIGHCHI",   "low": "KXLOWTCHI"},
    "Miami":         {"high": "KXHIGHMIA",   "low": "KXLOWTMIA"},
    "Austin":        {"high": "KXHIGHAUS",   "low": "KXLOWTAUS"},
    "Los Angeles":   {"high": "KXHIGHLAX",   "low": "KXLOWTLAX"},
    "San Francisco": {"high": "KXHIGHTSFO",  "low": None},
    "Denver":        {"high": "KXHIGHDEN",   "low": "KXLOWTDEN"},
    "Philadelphia":  {"high": "KXHIGHPHIL",  "low": "KXLOWTPHIL"},
    "Atlanta":       {"high": "KXHIGHTATL",  "low": None},
    "Houston":       {"high": "KXHIGHTHOU",  "low": None},
    "Phoenix":       {"high": "KXHIGHTPHX",  "low": None},
    "Las Vegas":     {"high": "KXHIGHTLV",   "low": None},
    "Dallas":        {"high": "KXHIGHTDAL",  "low": None},
    "Boston":        {"high": "KXHIGHTBOS",  "low": None},
    "Washington DC": {"high": "KXHIGHTDC",   "low": None},
    "Minneapolis":   {"high": "KXHIGHTMIN",  "low": None},
    "Oklahoma City": {"high": "KXHIGHTOKC",  "low": None},
    "New Orleans":   {"high": "KXHIGHTNOLA", "low": None},
    "Seattle":       {"high": "KXHIGHTSEA",  "low": None},
    "San Antonio":   {"high": "KXHIGHTSATX", "low": None},
}

# Flat set of all series tickers we currently track
OUR_TICKERS = set()
for city, series in OUR_SERIES.items():
    for mtype, ticker in series.items():
        if ticker:
            OUR_TICKERS.add(ticker)


def fetch_all_series(prefix: str) -> list[dict]:
    """
    Fetch all series matching a prefix from the Kalshi API.
    Paginates until all results are retrieved.
    """
    all_series = []
    cursor     = None
    page       = 0

    while True:
        params = {"limit": 100}
        if cursor:
            params["cursor"] = cursor

        try:
            resp = requests.get(
                f"{API_BASE}/series",
                params=params,
                timeout=15,
            )
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            print(f"  [error] Failed to fetch series page {page+1}: {e}")
            break

        batch  = data.get("series", [])
        cursor = data.get("cursor")
        page  += 1

        # Filter to temperature-related series
        temp_batch = [
            s for s in batch
            if any(
                kw in s.get("ticker", "").upper()
                for kw in ("HIGH", "LOWT", "TEMP")
            )
        ]
        all_series.extend(temp_batch)

        print(f"  page {page}: {len(batch)} series total, "
              f"{len(temp_batch)} temperature-related (running: {len(all_series)})")

        if not cursor or len(batch) < 100:
            break

        time.sleep(0.2)

    return all_series


def fetch_series_detail(ticker: str) -> dict:
    """Fetch detail for a single series."""
    try:
        resp = requests.get(
            f"{API_BASE}/series/{ticker}",
            timeout=10,
        )
        resp.raise_for_status()
        return resp.json().get("series", {})
    except Exception as e:
        return {"error": str(e)}


def analyse(all_series: list[dict], raw: bool = False):
    if raw:
        print(json.dumps(all_series, indent=2, default=str))
        return

    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    # Index by ticker
    found_tickers = {s["ticker"]: s for s in all_series}

    # Separate HIGH and LOWT
    high_series = {t: s for t, s in found_tickers.items()
                   if "HIGH" in t and "LOWT" not in t}
    lowt_series = {t: s for t, s in found_tickers.items()
                   if "LOWT" in t}
    other       = {t: s for t, s in found_tickers.items()
                   if t not in high_series and t not in lowt_series}

    print(f"\n{'='*72}")
    print(f"  KALSHI TEMPERATURE SERIES DISCOVERY  —  {now_utc}")
    print(f"{'='*72}")
    print(f"  Total temperature series found : {len(found_tickers)}")
    print(f"    HIGH series                  : {len(high_series)}")
    print(f"    LOWT series                  : {len(lowt_series)}")
    if other:
        print(f"    Other                        : {len(other)}")
    print(f"  Series we currently track      : {len(OUR_TICKERS)}")

    # ── HIGH series ───────────────────────────────────────────────────────────
    print(f"\n{'─'*72}")
    print(f"  HIGH SERIES")
    print(f"{'─'*72}")
    print(f"  {'Ticker':<20} {'Title':<40} {'Tracked'}")
    print(f"  {'-'*65}")

    for ticker in sorted(high_series):
        s       = high_series[ticker]
        title   = s.get("title", "")[:38]
        tracked = "✓" if ticker in OUR_TICKERS else "✗  ← NOT TRACKED"
        print(f"  {ticker:<20} {title:<40} {tracked}")

    # ── LOWT series ───────────────────────────────────────────────────────────
    print(f"\n{'─'*72}")
    print(f"  LOWT SERIES")
    print(f"{'─'*72}")
    print(f"  {'Ticker':<20} {'Title':<40} {'Tracked'}")
    print(f"  {'-'*65}")

    for ticker in sorted(lowt_series):
        s       = lowt_series[ticker]
        title   = s.get("title", "")[:38]
        tracked = "✓" if ticker in OUR_TICKERS else "✗  ← NOT TRACKED"
        print(f"  {ticker:<20} {title:<40} {tracked}")

    # ── Gaps — on Kalshi but not in our config ─────────────────────────────────
    untracked = {t: s for t, s in found_tickers.items() if t not in OUR_TICKERS}

    if untracked:
        print(f"\n{'─'*72}")
        print(f"  ⚠  UNTRACKED SERIES ({len(untracked)}) — on Kalshi but not in our config")
        print(f"{'─'*72}")
        for ticker in sorted(untracked):
            s = untracked[ticker]
            print(f"  {ticker:<20}  {s.get('title', '')}")

    # ── Our config vs Kalshi ───────────────────────────────────────────────────
    missing_from_kalshi = OUR_TICKERS - set(found_tickers)
    if missing_from_kalshi:
        print(f"\n{'─'*72}")
        print(f"  ⚠  IN OUR CONFIG BUT NOT FOUND ON KALSHI ({len(missing_from_kalshi)})")
        print(f"{'─'*72}")
        for ticker in sorted(missing_from_kalshi):
            print(f"  {ticker}")

    print(f"\n{'='*72}\n")


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Discover all Kalshi temperature series and compare to our config"
    )
    parser.add_argument("--raw", action="store_true",
                        help="Dump raw API response as JSON")
    args = parser.parse_args()

    print("\nFetching all series from Kalshi API...")
    all_series = fetch_all_series(prefix="KX")
    print(f"\n  {len(all_series)} temperature series found.\n")

    analyse(all_series, raw=args.raw)
