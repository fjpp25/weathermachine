"""
probe_engines.py
----------------
The capital strip shows an empty dash, meaning /api/status returned an empty
`engines` dict. That dict is built in a try/except that SWALLOWS the real error.
This probe runs the exact same code path with the exception exposed.

Run from project root on the Pi:
    python3 probe_engines.py
"""

import sys, traceback
sys.path.insert(0, ".")

from market_utils import load_config_env
import trader

load_config_env()

print("ENGINE_ALLOCATIONS keys:", list(trader.ENGINE_ALLOCATIONS.keys()))

try:
    cap = trader.get_engine_capital()
    print("get_engine_capital() ->", type(cap).__name__)
except Exception:
    print(">>> get_engine_capital() THREW:")
    traceback.print_exc()
    sys.exit(1)

engines = {}
available = 0.0
for e in trader.ENGINE_ALLOCATIONS:
    try:
        rem = round(cap.remaining(e), 2)
        bud = round(cap.budget(e), 2)
        engines[e] = {"remaining": rem, "budget": bud}
        available += rem
        print(f"  {e:10s} remaining=${rem:7.2f}  budget=${bud:7.2f}")
    except Exception:
        print(f"  {e:10s} >>> THREW:")
        traceback.print_exc()

print(f"\nengines dict has {len(engines)} entries")
print(f"available (sum of remaining) = ${available:.2f}")

# Does the singleton actually have a balance? A zero day-open balance would
# make every budget $0 -> every remaining $0 -> strip shows $0.00 (NOT a dash,
# but worth knowing).
for attr in ("_balance", "_day_open_balance", "_budget", "_deployed"):
    if hasattr(cap, attr):
        v = getattr(cap, attr)
        print(f"  cap.{attr} = {v if not isinstance(v, dict) else dict(v)}")
