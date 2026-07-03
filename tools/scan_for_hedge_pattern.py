#!/usr/bin/env python3
"""
tools/scan_for_hedge_pattern.py — diagnostic only. READ-ONLY (GET requests
only, never places or cancels an order).

WHY THIS EXISTS
----------------
Two confirmed incidents — KXHIGHTSEA-26JUN08-B61.5 and KXHIGHAUS-26JUN21-
B92.5 — showed the same anomalous shape: after buying NO heavily, an
aggressive (is_taker=true) SELL fill on the OPPOSITE side (side=yes)
appeared, creating a separate yes_count_fp holding at settlement rather
than closing or reducing the no position. Both are now outside journalctl's
retention window (oldest retained entry: 2026-06-25), so neither can be
directly confirmed against live logs.

Rather than keep checking previously-flagged "early exit" tickers one at a
time via inspect_ticker_fills.py, this scans the ENTIRE fills history in a
single pass for the same signature, across every temperature-market ticker
— not just the 24 dashboard.py originally flagged, since that classification
has its own known blind spots (see the dashboard.py fix earlier this
session). Any hit dated 2026-06-25 or later is flagged as still being
within journalctl's retention window, with a ready-to-run journalctl command
printed for it — that's the one lever left that can actually confirm
whether check_exits() (or something else) is producing this fill pattern,
versus continuing to infer from fills data alone.

USAGE (repo root, on the Pi):
    python3 tools/scan_for_hedge_pattern.py
"""
from __future__ import annotations

import json
import os
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

LOG_RETENTION_START = "2026-06-25"   # oldest entry per `journalctl -u weathermachine | head`


def load_credentials():
    """Mirrors fetch_settlements.py's load_credentials()."""
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


def fetch_all_fills(client) -> list:
    all_f, cursor = [], None
    for _ in range(15):
        p = {"limit": 200}
        if cursor:
            p["cursor"] = cursor
        d = client.get("portfolio/fills", params=p)
        b = d.get("fills", [])
        all_f.extend(b)
        cursor = d.get("cursor")
        if not cursor or len(b) < 200:
            break
    return all_f


def is_temp_ticker(t: str) -> bool:
    return t.startswith("KX") and ("HIGH" in t or "LOWT" in t) and "TEMPNYCH" not in t


def journalctl_window(created_time: str) -> tuple[str, str]:
    """+/- 2 minutes around the fill, formatted for journalctl --since/--until."""
    try:
        dt = datetime.fromisoformat(created_time.replace("Z", "+00:00"))
    except ValueError:
        return "", ""
    start = (dt - timedelta(minutes=2)).strftime("%Y-%m-%d %H:%M:%S")
    end = (dt + timedelta(minutes=2)).strftime("%Y-%m-%d %H:%M:%S")
    return start, end


def main():
    load_credentials()
    import trader
    client = trader.make_client(skip_confirmation=True)

    print("Fetching full fills history...")
    all_f = fetch_all_fills(client)
    fbt = defaultdict(list)
    for f in all_f:
        if is_temp_ticker(f.get("ticker", "")):
            fbt[f["ticker"]].append(f)
    print(f"  {len(all_f)} total fills, {len(fbt)} temperature-market tickers with fills\n")

    hits = []
    for ticker, fills in fbt.items():
        buys = [f for f in fills if f.get("action") == "buy"]
        sells = [f for f in fills if f.get("action") == "sell"]
        if not buys or not sells:
            continue
        sides = [f.get("side") for f in buys]
        our = max(set(sides), key=sides.count)
        # The anomalous signature: an aggressive (taker) sell on the side
        # OPPOSITE what we actually bought. A same-side sell, or a passive
        # (is_taker=false, someone else's order filling ours) sell, is the
        # ordinary/benign trim pattern already handled correctly by the
        # dashboard.py fix — not what we're hunting for here.
        opposite_taker_sells = [
            f for f in sells
            if f.get("side") != our and f.get("is_taker")
        ]
        for s in opposite_taker_sells:
            hits.append({
                "ticker": ticker,
                "our_side": our,
                "sell_side": s.get("side"),
                "count": s.get("count_fp"),
                "price": s.get("yes_price_dollars"),
                "created": s.get("created_time", ""),
                "order_id": s.get("order_id"),
            })

    hits.sort(key=lambda h: h["created"])

    print(f"{'='*88}")
    print(f"  {len(hits)} opposite-side TAKER sell fill(s) found across full fills history")
    print(f"{'='*88}\n")
    for h in hits:
        recent = h["created"][:10] >= LOG_RETENTION_START
        flag = "  <<< LOG COVERAGE AVAILABLE" if recent else ""
        print(f"  {h['created']}  {h['ticker']:<32} bought={h['our_side']:<3} "
              f"sold={h['sell_side']:<3} x{h['count']:>6} @ ${h['price']}{flag}")

    recent_hits = [h for h in hits if h["created"][:10] >= LOG_RETENTION_START]
    print(f"\n{'='*88}")
    if recent_hits:
        print(f"  {len(recent_hits)} hit(s) within journalctl's retention window — "
              f"run these to check for a matching check_exits() log line:")
        print(f"{'='*88}\n")
        for h in recent_hits:
            start, end = journalctl_window(h["created"])
            series = h["ticker"].split("-")[0]
            print(f"  # {h['ticker']}  @ {h['created']}")
            print(f'  journalctl -u weathermachine --since "{start}" --until "{end}" '
                  f'| grep -iE "{series}|stop loss|exit"')
            print()
    else:
        print("  No hits within log retention — every occurrence found (if any) "
              "predates 2026-06-25 and cannot be confirmed against journalctl.")
        print("  If the list above is empty entirely, this pattern hasn't recurred")
        print("  since Austin (06-21) — worth treating as dormant rather than active,")
        print("  though 'hasn't happened again yet' is not the same as 'can't happen'.")
    print(f"{'='*88}")


if __name__ == "__main__":
    main()
