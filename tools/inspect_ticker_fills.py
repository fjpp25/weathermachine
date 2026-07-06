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

ADDED: also pulls the ticker's ORDER history (not just fills) and checks
data/trade_log.json for a matching entry. This matters because a resting
limit order's created_time (when we placed it) can be well before its
fill's created_time (when it actually matched) — if you're trying to find
which engine placed a trade by searching journald around a timestamp,
search around the ORDER's created_time, not the fill's.

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


def fetch_orders_for_ticker(client, ticker: str) -> list:
    """
    Fetch ALL orders (any status — executed, resting, canceled) for one
    ticker. This is the actual order-placement record, distinct from
    fills: a resting limit order can sit for a while before it fills, so
    an order's created_time can be well before its matching fill's
    created_time. When cross-referencing against journald to find which
    engine placed a trade, this is the timestamp to search around — NOT
    the fill's created_time.

    Same server-side-filter-then-fallback pattern as fetch_fills_for_ticker,
    since it's unverified whether the orders endpoint honours a ticker
    filter server-side either.
    """
    all_o, cursor = [], None
    for _ in range(5):
        p = {"limit": 200, "ticker": ticker}
        if cursor:
            p["cursor"] = cursor
        d = client.get("portfolio/orders", params=p)
        b = d.get("orders", [])
        all_o.extend(b)
        cursor = d.get("cursor")
        if not cursor or len(b) < 200:
            break

    if all_o:
        return all_o

    print("  (server-side ticker filter returned nothing for orders — "
          "falling back to an unfiltered pull, client-side filtered; slower)")
    all_o, cursor = [], None
    for _ in range(15):
        p = {"limit": 200}
        if cursor:
            p["cursor"] = cursor
        d = client.get("portfolio/orders", params=p)
        b = d.get("orders", [])
        all_o.extend([o for o in b if o.get("ticker") == ticker])
        cursor = d.get("cursor")
        if not cursor or len(b) < 200:
            break
    return all_o


def check_trade_log(ticker: str) -> list:
    """Return every data/trade_log.json entry for this exact ticker, if any."""
    path = Path("data/trade_log.json")
    if not path.exists():
        return []
    try:
        entries = json.loads(path.read_text())
    except Exception:
        return []
    return [e for e in entries if e.get("ticker") == ticker]


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("ticker", help="e.g. KXHIGHTATL-26JUN26-B93.5")
    args = ap.parse_args()

    load_credentials()
    import trader
    client = trader.make_client(skip_confirmation=True)

    print(f"\n{'='*80}\n  ORDERS — {args.ticker}\n{'='*80}")
    orders = fetch_orders_for_ticker(client, args.ticker)
    if not orders:
        print("  No orders found for this ticker at all — this ticker was "
              "never submitted as an order by this account. If a fill "
              "exists for it (see below), that fill must belong to an "
              "order placed for a DIFFERENT ticker that got matched here, "
              "or the orders endpoint's retention window doesn't reach "
              "this far back.")
    for o in sorted(orders, key=lambda x: x.get("created_time", "")):
        print(
            f"\n  order_id={o.get('order_id')}  status={o.get('status')}  "
            f"side={o.get('side')}  action={o.get('action')}  "
            f"created={o.get('created_time')}  "
            f"yes_price=${o.get('yes_price_dollars')}  "
            f"count={o.get('initial_count') or o.get('remaining_count')}"
        )

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

    print(f"\n{'='*80}\n  data/trade_log.json — {args.ticker}\n{'='*80}")
    tl_matches = check_trade_log(args.ticker)
    if not tl_matches:
        print("  No trade_log.json entry for this ticker.")
    for e in tl_matches:
        print(json.dumps(e, indent=2))

    print(f"\n{'='*80}")
    print(f"  {len(orders)} order(s), {len(fills)} fill(s), "
          f"{len(settlements)} settlement record(s), "
          f"{len(tl_matches)} trade_log.json entr(ies) "
          f"— the earliest order's created_time above is what to search "
          f"journald around, not the fill's created_time.")


if __name__ == "__main__":
    main()
