#!/usr/bin/env python3
"""
cli_fetcher.py — NWS CLI (daily climate summary) fetcher and logger.

OBSERVE-ONLY. This does not gate or inform any live trading decision. It logs
the NWS-curated official MAXIMUM/MINIMUM for a station alongside our own
observation-derived value, so we can later measure (via `analytics`) how often
they agree with each other and with authoritative settlement — same pattern
as the LOWT signals before they went live, same pattern as the per-band
backtest before any gate changed.

WHY THIS EXISTS (2026-06-30 DC investigation):
  Our own observation-derived "observed high" is built from `max()` over a
  raw 5-min ASOS feed, which we discovered can carry whole-Celsius
  quantization noise that artificially inflates the max (a true ~31.7C
  reading can throw a tick to 32.0C / 89.6F depending on which side of the
  rounding boundary the sensor lands on). The CLI product is NWS's own
  curated daily summary and is NOT susceptible to that same artifact in the
  same way — worth comparing systematically rather than trusting either
  source blindly.

ENDPOINT (confirmed working 2026-06-30):
  GET /products/types/CLI/locations/{station}/latest
  {station} = the same ICAO code already in cities.py (e.g. "DCA") — NOT the
  forecast office (e.g. "LWX"). Confirmed via /products/types/CLI/locations
  listing, which indexes by station, not office.

KNOWN OPEN QUESTIONS (do not assume — verify against more real output):
  - Whether a CORRECTED/final CLI is issued after local midnight with a
    distinguishable signal (issuanceTime rolling past midnight local is the
    working heuristic here; not yet confirmed against a real final issuance).
  - Whether MM (missing data) or other edge-case markers appear in the
    MAXIMUM/MINIMUM fields under any real conditions.
  - Whether some stations' CLI bulletins ever omit the MAXIMUM line entirely
    (e.g. early in the day before the first PM CLI is issued).
  Treat parse failures as "no data yet", not errors — log them and move on.

USAGE:
  python3 cli_fetcher.py --station DCA              # print parsed result
  python3 cli_fetcher.py --station DCA --log        # also append to log file
  python3 cli_fetcher.py --station DCA --raw        # print unparsed productText
"""
from __future__ import annotations

import argparse
import json
import re
from datetime import datetime, timezone
from pathlib import Path

import requests

API_BASE = "https://api.weather.gov"
USER_AGENT = "kalshi-weather-trader/1.0 (research project)"
LOG_FILE = Path("data/cli_reports.jsonl")

# Matches lines like:
#   MAXIMUM         89   3:59 PM 100    1959  89      0       92
#   MINIMUM         71   5:14 AM  50    1919  71      0       76
# Captures: value (int, may be negative via leading '-'), time (h:mm AM/PM).
# 'MM' (missing) will simply fail this pattern -> field stays None, by design.
_TEMP_LINE_RE = re.compile(
    r"\b(MAXIMUM|MINIMUM)\s+(-?\d+)\s+(\d{1,2}:\d{2}\s*[AP]M)", re.IGNORECASE
)

# Matches: "VALID TODAY AS OF 0500 PM LOCAL TIME."
_VALID_AS_OF_RE = re.compile(
    r"VALID TODAY AS OF (\d{3,4})\s*(AM|PM)\s*LOCAL TIME", re.IGNORECASE
)


def get(url: str, timeout: int = 15) -> dict:
    resp = requests.get(url, headers={
        "User-Agent": USER_AGENT,
        "Accept": "application/ld+json",
    }, timeout=timeout)
    resp.raise_for_status()
    return resp.json()


def fetch_latest_cli(station: str) -> dict:
    """Fetch the most recent CLI product for a station (ICAO code, matches
    cities.py's `icao` field directly)."""
    return get(f"{API_BASE}/products/types/CLI/locations/{station}/latest")


def parse_cli_text(text: str) -> dict:
    """Extract MAXIMUM/MINIMUM value+time and the 'valid as of' cutoff from
    raw CLI productText. Returns a dict with None for any field not found —
    callers should treat missing fields as 'not available', not an error."""
    result = {
        "maximum_f": None, "maximum_time": None,
        "minimum_f": None, "minimum_time": None,
        "valid_as_of": None,
    }
    for label, value, time_str in _TEMP_LINE_RE.findall(text):
        label = label.upper()
        if label == "MAXIMUM" and result["maximum_f"] is None:
            result["maximum_f"] = int(value)
            result["maximum_time"] = time_str.strip()
        elif label == "MINIMUM" and result["minimum_f"] is None:
            result["minimum_f"] = int(value)
            result["minimum_time"] = time_str.strip()

    m = _VALID_AS_OF_RE.search(text)
    if m:
        result["valid_as_of"] = f"{m.group(1)} {m.group(2).upper()}"

    return result


def fetch_and_parse(station: str) -> dict:
    """Fetch + parse in one call. Returns parsed fields plus metadata
    (issuanceTime, product id) for logging/comparison."""
    product = fetch_latest_cli(station)
    parsed = parse_cli_text(product.get("productText", ""))
    return {
        "station": station,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "issuance_time": product.get("issuanceTime"),
        "product_id": product.get("id"),
        **parsed,
    }


def log_result(result: dict):
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(LOG_FILE, "a") as f:
        f.write(json.dumps(result) + "\n")


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--station", required=True, help="ICAO code, e.g. DCA")
    ap.add_argument("--log", action="store_true",
                     help="append parsed result to data/cli_reports.jsonl")
    ap.add_argument("--raw", action="store_true",
                     help="print unparsed productText instead of parsing")
    args = ap.parse_args()

    if args.raw:
        product = fetch_latest_cli(args.station)
        print(product.get("productText", "(no productText)"))
        return

    result = fetch_and_parse(args.station)
    print(json.dumps(result, indent=2))

    if args.log:
        log_result(result)
        print(f"\nLogged to {LOG_FILE}")


if __name__ == "__main__":
    main()
