"""
probe_enrich.py
---------------
The settlement EXISTS and the ticker matches — yet the position shows OPEN.
That means the ticker is being DROPPED during enrichment in _fetch_settlements.

There are exactly two silent drop gates:
    A)  yes_count_fp == 0 AND no_count_fp == 0      -> `else: continue`
    B)  nc == 0 OR cost == 0                         -> `continue`

This probe prints the raw settlement fields those gates inspect, for the
specific tickers that render OPEN, so we can see which gate fires.

Run from project root on the Pi:
    python3 probe_enrich.py
"""

import sys, json
sys.path.insert(0, ".")

from market_utils import load_config_env
import trader

load_config_env()
c = trader.make_client(skip_confirmation=True)

# Tickers from the screenshot / trade log that are showing OPEN.
# Add/adjust as needed — these are the JUN20 Chicago suspects.
SUSPECTS = {
    "KXHIGHCHI-26JUN20-B80.5",
    "KXHIGHCHI-26JUN20-B81.5",   # 81-82 bracket label from the screenshot row
}

all_s, cursor = [], None
for _ in range(15):
    p = {"limit": 200}
    if cursor: p["cursor"] = cursor
    d = c.get("portfolio/settlements", params=p)
    b = d.get("settlements", [])
    all_s.extend(b); cursor = d.get("cursor")
    if not cursor or len(b) < 200: break

by_ticker = {s.get("ticker", ""): s for s in all_s}

print("=== RAW SETTLEMENT FIELDS for suspect tickers ===\n")
for tk in sorted(SUSPECTS):
    s = by_ticker.get(tk)
    if not s:
        print(f"[{tk}]  *** NOT in settlements at all ***\n")
        continue
    fields = ["ticker", "market_result", "settled_time",
              "yes_count_fp", "no_count_fp",
              "yes_total_cost_dollars", "no_total_cost_dollars",
              "fee_cost", "revenue_dollars"]
    print(f"[{tk}]")
    for f in fields:
        print(f"    {f:28s} = {s.get(f)!r}")

    # Replicate the enrichment gates exactly
    yes_c = float(s.get("yes_count_fp") or 0)
    no_c  = float(s.get("no_count_fp")  or 0)
    yes_cost = float(s.get("yes_total_cost_dollars") or 0)
    no_cost  = float(s.get("no_total_cost_dollars")  or 0)

    if yes_c > 0 and no_c == 0:
        our, nc, cost = "yes", int(yes_c), round(yes_cost, 4)
    elif no_c > 0 and yes_c == 0:
        our, nc, cost = "no", int(no_c), round(no_cost, 4)
    elif yes_c > 0 and no_c > 0:
        if yes_c >= no_c: our, nc, cost = "yes", int(yes_c), round(yes_cost, 4)
        else:             our, nc, cost = "no",  int(no_c),  round(no_cost, 4)
    else:
        print("    >>> DROPPED at GATE A (both counts zero)\n")
        continue

    if nc == 0 or cost == 0:
        print(f"    >>> DROPPED at GATE B (nc={nc}, cost={cost})\n")
        continue

    print(f"    >>> SURVIVES enrichment: side={our} nc={nc} cost={cost}\n")

# Also dump the full raw keys of one suspect so we see every field name
print("=== ALL KEYS present on first suspect settlement ===")
for tk in sorted(SUSPECTS):
    s = by_ticker.get(tk)
    if s:
        print(f"[{tk}] keys: {sorted(s.keys())}")
        break
