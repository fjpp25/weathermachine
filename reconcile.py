"""
reconcile.py
------------
Reconciles locally tracked positions against Kalshi's settlement history
using the authenticated GET /portfolio/settlements endpoint.

This is the correct approach — rather than polling individual market statuses,
we fetch Kalshi's own record of what settled and at what payout, then match
against our local position tracker.

Run each morning after markets settle (typically 6-9am ET).
Does not place any orders — read-only.

Usage:
  python reconcile.py           # reconcile all open positions
  python reconcile.py --dry-run # show what would be reconciled without saving
"""

import os
import argparse
from datetime import datetime, timezone
from pathlib import Path

import trader


def fetch_settlements(client: trader.KalshiClient, limit: int = 200) -> list[dict]:
    """
    Fetch recent settlement history from Kalshi portfolio.
    Returns list of settlement dicts, each containing:
      ticker, market_result, yes_count_fp, no_count_fp, revenue, fee_cost, settled_time
    """
    data = client.get("portfolio/settlements", params={"limit": limit})
    return data.get("settlements", [])


def reconcile(dry_run: bool = False):
    env_file = Path(".env")
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            if "=" in line and not line.startswith("#"):
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())

    client     = trader.make_client()
    local_open = [p for p in trader.load_positions() if p["status"] == "open"]

    if not local_open:
        print("  No open positions to reconcile.")
        return

    print(f"\n{'='*65}")
    print(f"  Reconcile  —  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"  Fetching settlement history from Kalshi...")
    if dry_run:
        print(f"  [DRY RUN — no changes will be saved]")
    print(f"{'='*65}\n")

    settlements       = fetch_settlements(client)
    settled_by_ticker = {s["ticker"]: s for s in settlements}

    print(f"  {len(settlements)} settlements found on Kalshi")
    print(f"  Matching against {len(local_open)} local open position(s)...\n")

    reconciled = 0
    pending    = 0
    total_pnl  = 0.0

    for pos in local_open:
        ticker    = pos["ticker"]
        side      = pos["side"]
        entry     = pos["entry_price"]
        contracts = pos["contracts"]

        if ticker not in settled_by_ticker:
            print(f"  {ticker:<42} [pending]")
            pending += 1
            continue

        s            = settled_by_ticker[ticker]
        result       = s.get("market_result", "").lower()
        settled_time = s.get("settled_time", "")
        fee_cost     = float(s.get("fee_cost") or 0)

        won        = (result == side)
        exit_price = 1.00 if won else 0.00
        pnl        = round((exit_price - entry) * contracts - fee_cost, 2)
        total_pnl += pnl

        status_str = "WON ✓" if won else "LOST ✗"
        fee_str    = f"  fee=${fee_cost:.4f}" if fee_cost else ""
        print(
            f"  {ticker:<42} [{status_str}]  "
            f"result={result.upper():<4}  "
            f"entry=${entry:.2f}  "
            f"pnl=${pnl:+.2f}"
            f"{fee_str}"
        )

        if not dry_run:
            trader.record_exit(pos["id"], exit_price, f"settled_{result}")
            trader.log_trade("settled", {
                "ticker":       ticker,
                "side":         side,
                "entry_price":  entry,
                "exit_price":   exit_price,
                "contracts":    contracts,
                "result":       result,
                "pnl":          pnl,
                "fee_cost":     fee_cost,
                "settled_time": settled_time,
            })

        reconciled += 1

    print(f"\n{'='*65}")
    print(f"  Reconciled : {reconciled}")
    print(f"  Pending    : {pending}")
    print(f"  Session PnL: ${total_pnl:+.2f}")
    if dry_run:
        print(f"  [DRY RUN — nothing was saved]")
    print(f"{'='*65}\n")

    if not dry_run and reconciled > 0:
        print("  Updated position summary:")
        trader.display_positions()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Reconcile settled Kalshi positions")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be reconciled without saving")
    args = parser.parse_args()
    reconcile(dry_run=args.dry_run)
