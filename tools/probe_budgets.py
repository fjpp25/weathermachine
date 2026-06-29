"""
probe_budgets.py
----------------
The "available per engine" strip reads EngineCapital, whose budget = LIVE CASH
x share -> collapses to ~0 when capital is deployed. Meanwhile a SEPARATE system
(get_*_deployable, based on _day_open_balance) governs cascade/peak/sweep/econv/
lowt. There may also be TWO deployed ledgers. Before building the display we map
which source is authoritative per engine and whether they agree.

Run from project root on the Pi (ideally in the SAME process as the scheduler is
NOT possible, so this shows what a fresh process sees — which is what the
dashboard process also sees):

    python3 probe_budgets.py
"""

import sys, json
sys.path.insert(0, ".")
from pathlib import Path

from market_utils import load_config_env
import trader

load_config_env()
c = trader.make_client(skip_confirmation=True)

bal = trader.get_balance(c)
pos = trader.get_positions(c)
cur = sum(p.get("current_price", 0) * p.get("contracts", 1) for p in pos)
total_value = round(bal + cur, 2)

print(f"live balance (free cash) : ${bal:.2f}")
print(f"open position value      : ${cur:.2f}")
print(f"total account value      : ${total_value:.2f}")
print(f"_day_open_balance (mem)  : ${trader._day_open_balance:.2f}  "
      f"(0.00 expected in a fresh/dashboard process)")

print("\n=== ENGINE_ALLOCATIONS ===")
for e, share in trader.ENGINE_ALLOCATIONS.items():
    print(f"  {e:10s} share={share:.4f}  "
          f"=> total_value-based budget ${round(total_value*share,2):7.2f}")

print("\n=== engine_capital_deployed.json (on disk) ===")
f = Path("data/engine_capital_deployed.json")
if f.exists():
    print(json.dumps(json.loads(f.read_text()), indent=2))
else:
    print("  (file absent)")

print("\n=== Authoritative get_*_deployable() (day-open based) ===")
fns = {
    "cascade": getattr(trader, "get_cascade_deployable", None),
    "peak":    getattr(trader, "get_peak_deployable", None),
    "sweep":   getattr(trader, "get_sweep_deployable", None),
    "econv":   getattr(trader, "get_econv_deployable", None),
    "lowt":    getattr(trader, "get_lowt_deployable", None),
}
for e, fn in fns.items():
    if fn:
        try:
            print(f"  {e:10s} get_{e}_deployable() = ${fn():.2f}")
        except Exception as ex:
            print(f"  {e:10s} ERROR: {ex}")
    else:
        print(f"  {e:10s} (no such function)")

print("\n=== EngineCapital singleton (what the dashboard currently reads) ===")
cap = trader.get_engine_capital(client=c)
for e in trader.ENGINE_ALLOCATIONS:
    print(f"  {e:10s} budget=${cap.budget(e):7.2f}  remaining=${cap.remaining(e):7.2f}")

print("\n=== day-open deployed globals ===")
for g in ["_deployed_cascade","_deployed_peak","_deployed_sweep",
          "_deployed_econv","_deployed_lowt","_deployed_tomorrow"]:
    if hasattr(trader, g):
        print(f"  trader.{g} = {getattr(trader, g)}")
