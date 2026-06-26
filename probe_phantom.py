"""
probe_phantom.py
----------------
Finding so far: a trade-log entry (KXHIGHCHI-26JUN20-B80.5) shows OPEN because
it has NO fill and NO held settlement — the order was logged but never executed.
get_settlements() is correct to drop it; the trade log contains a phantom entry.

Question this answers: is that a one-off, or a recurring pattern?

For every trade-log entry it checks:
  - do we have a FILL for that ticker?            (did it actually execute?)
  - is there a settlement with nonzero counts?    (did we hold a position?)
An entry with neither is a PHANTOM (logged, never traded).

Run from project root on the Pi:
    python3 probe_phantom.py
"""

import sys, json
sys.path.insert(0, ".")

from market_utils import load_config_env
import trader

load_config_env()
c = trader.make_client(skip_confirmation=True)

# ── fills ────────────────────────────────────────────────────────────────
all_f, cursor = [], None
for _ in range(25):
    p = {"limit": 200}
    if cursor: p["cursor"] = cursor
    d = c.get("portfolio/fills", params=p)
    b = d.get("fills", [])
    all_f.extend(b); cursor = d.get("cursor")
    if not cursor or len(b) < 200: break
fill_tickers = {f.get("ticker") for f in all_f}

# ── settlements with a real held position (nonzero counts) ───────────────
all_s, cursor = [], None
for _ in range(15):
    p = {"limit": 200}
    if cursor: p["cursor"] = cursor
    d = c.get("portfolio/settlements", params=p)
    b = d.get("settlements", [])
    all_s.extend(b); cursor = d.get("cursor")
    if not cursor or len(b) < 200: break
held_settle_tickers = {
    s.get("ticker") for s in all_s
    if (float(s.get("yes_count_fp") or 0) > 0 or float(s.get("no_count_fp") or 0) > 0)
}

# ── trade log ────────────────────────────────────────────────────────────
with open("data/trade_log.json") as f:
    trades = json.load(f)

phantoms, real, paper = [], [], []
for t in trades:
    if t.get("paper"):
        paper.append(t); continue
    tk = t.get("ticker", "")
    has_fill   = tk in fill_tickers
    has_settle = tk in held_settle_tickers
    if has_fill or has_settle:
        real.append(t)
    else:
        phantoms.append(t)

print("=== TRADE-LOG RECONCILIATION ===")
print(f"  total entries        : {len(trades)}")
print(f"  paper entries         : {len(paper)}")
print(f"  real (fill/settlement): {len(real)}")
print(f"  PHANTOM (neither)     : {len(phantoms)}")

# Break phantoms down by engine/tier and by date to see if it's clustered
from collections import Counter
by_tier = Counter((t.get("entry_tier") or "main") for t in phantoms)
by_date = Counter((t.get("placed_at","")[:10]) for t in phantoms)

print("\n  phantoms by engine/tier:")
for tier, n in by_tier.most_common():
    print(f"    {tier:24s} {n}")

print("\n  phantoms by date (top 15):")
for dt, n in by_date.most_common(15):
    print(f"    {dt}  {n}")

print("\n  first 25 phantom tickers:")
for t in phantoms[:25]:
    print(f"    {t.get('ticker'):26s} tier={t.get('entry_tier') or 'main':16s} "
          f"placed={t.get('placed_at','')[:16]} "
          f"price={t.get('entry_price')} contracts={t.get('contracts')}")
