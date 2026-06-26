"""
probe_gateA.py
--------------
The JUN20 Chicago settlement exists, resolved 'no', but has zero counts/costs
in the fields the enrichment loop reads — so it's dropped (Gate A) and the
position shows OPEN.

Two competing explanations:
  (1) Kalshi reports settled position size in a DIFFERENT field (value/revenue),
      and the loop reads the wrong ones -> systemic: many held positions dropped.
  (2) Counts are genuinely zero (no held position at settlement) -> the trade-log
      entry shouldn't be OPEN for a different reason (exit/fills reconciliation).

This probe:
  A) dumps value/revenue for the JUN20 ticker (full raw record)
  B) cross-references fills: did we actually hold a position into settlement?
  C) counts how PERVASIVE the zero-count-but-resolved shape is across ALL
     temp settlements, and how many of those we have fills for.

Run from project root on the Pi:
    python3 probe_gateA.py
"""

import sys, json
sys.path.insert(0, ".")

from market_utils import load_config_env
import trader

load_config_env()
c = trader.make_client(skip_confirmation=True)

TARGET = "KXHIGHCHI-26JUN20-B80.5"

# ── settlements ──────────────────────────────────────────────────────────
all_s, cursor = [], None
for _ in range(15):
    p = {"limit": 200}
    if cursor: p["cursor"] = cursor
    d = c.get("portfolio/settlements", params=p)
    b = d.get("settlements", [])
    all_s.extend(b); cursor = d.get("cursor")
    if not cursor or len(b) < 200: break

by_ticker = {s.get("ticker", ""): s for s in all_s}

print("=== FULL RAW RECORD for", TARGET, "===")
print(json.dumps(by_ticker.get(TARGET, {}), indent=2))

# ── fills for the target ─────────────────────────────────────────────────
all_f, cursor = [], None
for _ in range(20):
    p = {"limit": 200}
    if cursor: p["cursor"] = cursor
    d = c.get("portfolio/fills", params=p)
    b = d.get("fills", [])
    all_f.extend(b); cursor = d.get("cursor")
    if not cursor or len(b) < 200: break

tgt_fills = [f for f in all_f if f.get("ticker") == TARGET]
print(f"\n=== FILLS for {TARGET}: {len(tgt_fills)} found ===")
for f in tgt_fills:
    print(f"  action={f.get('action'):4s} side={f.get('side'):3s} "
          f"count_fp={f.get('count_fp')} "
          f"yes_price={f.get('yes_price_dollars')} "
          f"created={f.get('created_time','')[:19]}")

# Net contracts held (buys - sells) per side
def net_by_side(fills):
    net = {}
    for f in fills:
        side = f.get("side")
        cnt  = float(f.get("count_fp") or 0)
        sign = 1 if f.get("action") == "buy" else -1
        net[side] = net.get(side, 0) + sign * cnt
    return net
print("  net held by side:", net_by_side(tgt_fills))

# ── pervasiveness across ALL temp settlements ────────────────────────────
temp = [s for s in all_s if s.get("ticker","").startswith("KX")
        and ("HIGH" in s.get("ticker","") or "LOWT" in s.get("ticker",""))
        and "TEMPNYCH" not in s.get("ticker","")]

def is_zero(s):
    return (float(s.get("yes_count_fp") or 0) == 0 and
            float(s.get("no_count_fp")  or 0) == 0)

zero_shape = [s for s in temp if is_zero(s)]
nonzero    = [s for s in temp if not is_zero(s)]

fills_tickers = {f.get("ticker") for f in all_f}
zero_with_fills = [s for s in zero_shape if s.get("ticker") in fills_tickers]

print(f"\n=== PERVASIVENESS (temp settlements) ===")
print(f"  total temp settlements      : {len(temp)}")
print(f"  zero-count shape (Gate A)    : {len(zero_shape)}")
print(f"  normal (survives enrichment) : {len(nonzero)}")
print(f"  zero-count BUT we have fills : {len(zero_with_fills)}  "
      f"<- these are WRONGLY dropped if >0")

# Show a few zero-with-fills examples — these are the smoking gun for Explanation 1
print("\n  examples of zero-count settlements we DID trade (have fills):")
for s in zero_with_fills[:10]:
    tk = s.get("ticker")
    nb = net_by_side([f for f in all_f if f.get("ticker") == tk])
    print(f"    {tk}  result={s.get('market_result')}  "
          f"value={s.get('value')!r}  net_held={nb}")
