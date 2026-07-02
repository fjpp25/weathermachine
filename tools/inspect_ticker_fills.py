#!/usr/bin/env python3
"""
tools/inspect_ticker_fills.py — diagnostic only. READ-ONLY (GET requests only,
never places or cancels an order).

WHY THIS EXISTS
----------------
dashboard.py's /api/performance classifies a ticker as "EARLY EXIT" if it has
both a buy and a sell fill before that ticker's settlement time. 24 tickers
were flagged this way over the past 12 days, all NO-side. A full audit of
every engine in this repo (trader.py, cascade_engine.py, sweep_engine.py,
peak_scanner.py, evening_convergence.py) found ZERO code paths that can sell
an existing NO position — check_exits()'s NO-side exit logic was
intentionally disabled in May 2026 after a backtest showed it was net -$93
vs holding to settlement (see trader.py's own comment above the disabled
block). Xico confirmed manually closing exactly one position (today,
believed Minneapolis LOW) and nothing else — meaning the other 23+ flagged
exits are unexplained by either automated code or manual action, UNLESS the
CLASSIFICATION ITSELF has a bug.

One concrete candidate: dashboard.py's _fetch_settlements() has an
opposite-side fallback —

    if not oes:
        leg = "yes" if our == "no" else "no"
        oes = [f for f in esells if f.get("side") == leg]

If no sell fill matches the side we actually bought, it accepts a sell fill
on the OPPOSITE side as evidence we exited. That could misclassify unrelated
same-ticker activity as "our NO position was closed" when it wasn't.

This script pulls the raw fills and settlement for one ticker directly from
Kalshi — no classification logic applied — so we can see ground truth before
trusting either the dashboard's label or the theory above.

USAGE (repo root, on the Pi):
    python3 tools/inspect_ticker_fills.py KXHIGHTATL-26JUN26-B93.5
"""
import argparse
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def load_credentials():
    """Mirrors fetch_settlements.py's load_credentials() — same convention,
    same precedence (data/config.json first, then .env)."""
    config_file = Path("data/config.json")
    if config_file.exists():
        config = json.loads(config_file.read_text())
        if config.get("key_id"):
            os.environ.setdefault("KALSHI_KEY_ID", config["key_id"])
        if config.get("key_file"):
            os.environ.setdefault("KALSHI_KEY_FILE", config["key_file"])
        os.environ["KALSHI_DEMO"] = "false" if config.get("live_mode") else "true"
        return
    env_file = Path(".env")
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            if "=" in line and not line.startswith("#"):
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())


def fetch_fills_for_ticker(client, ticker: str) -> list:
    """Try server-side ticker filtering first; if the API ignores it or
    doesn't support it (returns nothing), fall back to an unfiltered pull
    with client-side filtering — slower, but always correct regardless of
    what the API actually supports."""
    all_f, cursor = [], None
    for _ in range(5):
        p = {"limit": 200, "ticker": ticker}
        if cursor:
            p["cursor"] = cursor
        d = client.get("portfolio/fills", params=p)
        b = d.get("fills", [])
        all_f.extend(b)
        cursor = d.get("cursor")
        if not cursor or len(b) < 200:
            break

    if all_f:
        return all_f

    print("  (server-side ticker filter returned nothing — falling back to "
          "an unfiltered pull, client-side filtered; slower)")
    all_f, cursor = [], None
    for _ in range(15):
        p = {"limit": 200}
        if cursor:
            p["cursor"] = cursor
        d = client.get("portfolio/fills", params=p)
        b = d.get("fills", [])
        all_f.extend([f for f in b if f.get("ticker") == ticker])
        cursor = d.get("cursor")
        if not cursor or len(b) < 200:
            break
    return all_f


def fetch_settlement_for_ticker(client, ticker: str) -> list:
    matches, cursor = [], None
    for _ in range(15):
        p = {"limit": 200}
        if cursor:
            p["cursor"] = cursor
        d = client.get("portfolio/settlements", params=p)
        b = d.get("settlements", [])
        matches.extend(s for s in b if s.get("ticker") == ticker)
        cursor = d.get("cursor")
        if not cursor or len(b) < 200:
            break
    return matches


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("ticker", help="e.g. KXHIGHTATL-26JUN26-B93.5")
    args = ap.parse_args()

    load_credentials()
    import trader
    client = trader.make_client(skip_confirmation=True)

    print(f"\n{'='*80}\n  RAW FILLS — {args.ticker}\n{'='*80}")
    fills = fetch_fills_for_ticker(client, args.ticker)
    if not fills:
        print("  No fills found for this ticker at all.")
    for f in sorted(fills, key=lambda x: x.get("created_time", "")):
        print(f"\n  action={f.get('action')!r}  side={f.get('side')!r}  "
              f"count={f.get('count_fp')}  "
              f"yes_price=${f.get('yes_price_dollars')}  "
              f"created={f.get('created_time')}")
        print(f"  {json.dumps(f, indent=4)}")

    print(f"\n{'='*80}\n  SETTLEMENT — {args.ticker}\n{'='*80}")
    settlements = fetch_settlement_for_ticker(client, args.ticker)
    if not settlements:
        print("  No settlement record found for this ticker.")
    for s in settlements:
        print(json.dumps(s, indent=2))

    print(f"\n{'='*80}")
    print(f"  {len(fills)} fill(s), {len(settlements)} settlement record(s) "
          f"— compare against what dashboard.py's classification concluded.")


if __name__ == "__main__":
    main()
