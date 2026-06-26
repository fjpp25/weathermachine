"""
probe_match.py
--------------
Pinpoints WHY a settled position shows OPEN on the History tab.

The dashboard matches trade-log tickers to settlement tickers by exact
string equality. This probe lays the two sources side by side for the
Chicago positions around Jun 19-21 so we can see the mismatch directly.

Run from project root on the Pi:
    python3 probe_match.py
"""

import sys, json
sys.path.insert(0, ".")

from market_utils import load_config_env
import trader

load_config_env()
c = trader.make_client(skip_confirmation=True)

# ── 1. Settlement tickers from Kalshi ────────────────────────────────────
all_s, cursor = [], None
for _ in range(15):
    p = {"limit": 200}
    if cursor: p["cursor"] = cursor
    d = c.get("portfolio/settlements", params=p)
    b = d.get("settlements", [])
    all_s.extend(b); cursor = d.get("cursor")
    if not cursor or len(b) < 200: break

settle_tickers = {s.get("ticker", "") for s in all_s}
chi_settle = sorted(t for t in settle_tickers if "CHI" in t and "JUN" in t)

print("=== SETTLEMENT tickers (Kalshi) — Chicago JUN ===")
for t in chi_settle:
    print(f"  [{t}]  len={len(t)}")

# ── 2. Trade-log tickers ─────────────────────────────────────────────────
TRADE_LOG = "data/trade_log.json"
try:
    with open(TRADE_LOG) as f:
        trades = json.load(f)
except Exception as e:
    print("could not read trade_log.json:", e)
    trades = []

chi_trades = [t for t in trades
              if "CHI" in t.get("ticker", "") and "JUN" in t.get("ticker", "")]

print("\n=== TRADE-LOG tickers — Chicago JUN ===")
for t in chi_trades:
    tk = t.get("ticker", "")
    print(f"  [{tk}]  len={len(tk)}  city={t.get('city')!r}  "
          f"placed={t.get('placed_at','')[:10]}")

# ── 3. Direct membership test ────────────────────────────────────────────
print("\n=== MATCH TEST (trade-log ticker in settlement set?) ===")
for t in chi_trades:
    tk = t.get("ticker", "")
    hit = tk in settle_tickers
    print(f"  {'MATCH ' if hit else 'MISS  '} [{tk}]")
    if not hit:
        # look for a near-miss settlement to expose the difference
        near = [s for s in settle_tickers if s[:18] == tk[:18]]
        for n in near:
            print(f"        closest settlement: [{n}]")
