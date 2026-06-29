"""
fetch_brackets.py
-----------------
Fetches all bracket tickers for today's KXHIGHLAX market and prints
the ticker alongside Kalshi's own label — use this to verify the
floor/cap display convention.

Usage:
    python fetch_brackets.py
    python fetch_brackets.py --series KXHIGHCHI --date 26APR28
"""

import json
import argparse
import requests

API_BASE = "https://api.elections.kalshi.com/trade-api/v2"


def fetch_brackets(series: str, date_str: str):
    resp = requests.get(
        f"{API_BASE}/markets",
        params={"series_ticker": series, "status": "open"},
        timeout=10,
    )
    resp.raise_for_status()
    markets = resp.json().get("markets", [])

    filtered = [
        m for m in markets
        if date_str.upper() in m.get("ticker", "").upper()
    ]

    if not filtered:
        print(f"No open brackets found for {series} on {date_str}")
        return

    print(f"\n{series}  —  {date_str}")
    print(f"{'Ticker':<32}  {'Subtitle / Title'}")
    print("-" * 70)
    for m in sorted(filtered, key=lambda x: x["ticker"]):
        label = m.get("subtitle") or m.get("title") or ""
        print(f"  {m['ticker']:<30}  {label}")
    print()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--series", default="KXHIGHMIA")
    parser.add_argument("--date",   default="26APR29")
    args = parser.parse_args()

    fetch_brackets(args.series, args.date)
