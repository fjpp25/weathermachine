"""
reconcile.py
------------
Diagnostic utility — fetches settlement history from Kalshi and prints a summary.
No local file writes. Kalshi is the source of truth.

Usage:
  python reconcile.py  # show recent temperature market settlements
"""

import os
from datetime import datetime, timezone
from pathlib import Path

import trader


def fetch_settlements(client: trader.KalshiClient, limit: int = 200) -> list[dict]:
    """Fetch recent settlement history from Kalshi portfolio."""
    data = client.get("portfolio/settlements", params={"limit": limit})
    return data.get("settlements", [])


if __name__ == "__main__":
    env_file = Path(".env")
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            if "=" in line and not line.startswith("#"):
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())

    client      = trader.make_client()
    settlements = fetch_settlements(client)

    temp = [
        s for s in settlements
        if s.get("ticker", "").startswith("KX")
        and ("HIGH" in s.get("ticker", "") or "LOWT" in s.get("ticker", ""))
    ]

    print(f"\n{'='*65}")
    print(f"  Kalshi Settlements  —  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"  {len(temp)} temperature market settlements found")
    print(f"{'='*65}\n")

    for s in sorted(temp, key=lambda x: x.get("settled_time", ""), reverse=True):
        ticker  = s.get("ticker", "")
        result  = s.get("market_result", "").upper()
        fee     = float(s.get("fee_cost") or 0)
        settled = s.get("settled_time", "")[:10]
        print(f"  {settled}  {ticker:<42}  {result:<4}  fee=${fee:.4f}")

    print(f"\n{'='*65}\n")
