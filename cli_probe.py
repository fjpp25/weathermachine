#!/usr/bin/env python3
"""
cli_probe.py — PROBE SCRIPT, not production code.

Fetches the raw CLI (daily climate summary) text product for a city's
forecast office via api.weather.gov, and prints it unparsed.

WHY THIS EXISTS:
  The CLI product is NWS's official daily max/min summary — the same one
  visible at forecast.weather.gov/product.php?site=LWX&product=CLI&issuedby=DCA.
  It's a categorically different source from the raw ASOS observation feed
  nws_feed.py already polls: CLI is curated/summarized, ASOS is continuous
  raw ticks (and we just found the raw feed can carry whole-Celsius
  quantization noise at certain temperatures).

  Before building a parser or wiring this into anything live, we need to see
  a REAL response to know:
    1. Does /products/types/CLI/locations/{office}/latest return one bulletin
       covering all stations for that office, or do we need a different
       identifier to isolate a single station (e.g. DCA vs IAD, both under LWX)?
    2. Exact text format of the MAXIMUM/MINIMUM lines (varies by office —
       don't assume a fixed column layout without seeing it).
    3. Whether "preliminary" vs "corrected" CLI issuances are distinguishable
       via the API response (issuanceTime / @id), since some stations issue
       a same-day preliminary CLI and a later corrected one.

  Run this manually first. Do NOT build a regex parser from assumptions —
  paste real output back and we'll parse what's actually there.

USAGE:
  python3 cli_probe.py --office LWX
  python3 cli_probe.py --office LWX --list     # see all recent CLI issuances
"""
from __future__ import annotations

import argparse
import json

import requests

API_BASE = "https://api.weather.gov"
USER_AGENT = "kalshi-weather-trader/1.0 (research project)"


def get(url: str, timeout: int = 15) -> dict:
    resp = requests.get(url, headers={
        "User-Agent": USER_AGENT,
        "Accept": "application/ld+json",
    }, timeout=timeout)
    resp.raise_for_status()
    return resp.json()


def fetch_latest(office: str) -> dict:
    """Fetch the most recent CLI product for a forecast office."""
    url = f"{API_BASE}/products/types/CLI/locations/{office}/latest"
    return get(url)


def fetch_recent_list(office: str) -> dict:
    """List recent CLI issuances for an office (to see preliminary vs
    corrected pairs, and confirm how often these actually post)."""
    url = f"{API_BASE}/products/types/CLI/locations/{office}"
    return get(url)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--office", required=True, help="WFO code, e.g. LWX")
    ap.add_argument("--list", action="store_true",
                     help="list recent CLI issuances instead of fetching latest")
    args = ap.parse_args()

    if args.list:
        data = fetch_recent_list(args.office)
        graph = data.get("@graph", data.get("features", data))
        print(f"Recent CLI products for office {args.office}:\n")
        print(json.dumps(graph, indent=2)[:4000])
        print("\n... (truncated if longer) ...")
        return

    data = fetch_latest(args.office)
    print("=" * 80)
    print(f"RAW RESPONSE KEYS: {list(data.keys())}")
    print("=" * 80)
    # Print everything except the giant productText field first, so the
    # metadata (issuance time, product id, station) is easy to read.
    meta = {k: v for k, v in data.items() if k != "productText"}
    print(json.dumps(meta, indent=2))
    print("=" * 80)
    print("PRODUCT TEXT:")
    print("=" * 80)
    print(data.get("productText", "(no productText field — check raw keys above)"))


if __name__ == "__main__":
    main()
