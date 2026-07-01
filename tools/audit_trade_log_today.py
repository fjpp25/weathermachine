#!/usr/bin/env python3
"""
tools/audit_trade_log_today.py — cheap, local, no-API-call check for
evidence of the trade_log.json race condition (see trader.py's
_locked_json_rmw docstring for the full mechanism).

WHAT THIS CHECKS: data/engine_capital_deployed.json only ever holds TODAY's
deployed totals (it resets/overwrites at midnight — see EngineCapital._load_
deployed's date check), so this can only audit the CURRENT day, not history.
Run it once, ideally later in the day after most trading activity has
happened, not as a substitute for the historical Kalshi cross-check (see
tools/audit_trade_log_vs_kalshi.py) which is the only way to check the past.

LOGIC: sum entry_price * contracts across all non-paper trade_log.json
entries placed today, and compare against the SUM of engine_capital_
deployed.json's per-engine "deployed" totals for today. If deployed capital
is meaningfully larger than what's logged, that's evidence SOME order was
placed (capital was really deployed) with NO corresponding trade_log.json
record — a lost write.

CAVEAT: this checks the TOTAL only, not per-engine, because trade_log.json
entries don't carry an explicit "engine" field — only entry_tier, and only
sweep_engine.py's entry_tier taxonomy has been verified to map onto the
"sweep" capital bucket in this repo so far (directional/sweep/dead_bracket/
tomorrow_dismissed/tomorrow_dismissed_b all draw from "sweep"). cascade/
main/peak/econv/lowt/hourly's entry_tier -> bucket mapping hasn't been
checked, so a per-engine breakdown would be guessing for those. The total-
vs-total check doesn't need that mapping and is honest about not attributing
blame to a specific engine.

USAGE (on the Pi, from repo root):
    python3 tools/audit_trade_log_today.py
"""
import json
from datetime import date, datetime
from pathlib import Path

TRADE_LOG = Path("data/trade_log.json")
CAPITAL_FILE = Path("data/engine_capital_deployed.json")

TOLERANCE = 0.05   # dollars — rounding slack, not a real discrepancy below this


def main():
    if not CAPITAL_FILE.exists():
        print(f"{CAPITAL_FILE} not found — nothing to check yet today.")
        return
    cap = json.loads(CAPITAL_FILE.read_text())
    cap_date = cap.get("date")
    today = str(date.today())

    if cap_date != today:
        print(f"engine_capital_deployed.json's date ({cap_date}) isn't today "
              f"({today}) — either no trades yet today, or this ran right at "
              f"a midnight boundary. Nothing to compare yet.")
        return

    deployed = cap.get("deployed", {})
    total_deployed = sum(deployed.values())

    if not TRADE_LOG.exists():
        print(f"deployed today: ${total_deployed:.2f} across {len(deployed)} "
              f"engines, but {TRADE_LOG} doesn't exist at all — "
              f"ALL of today's trades are unaccounted for if total_deployed > 0.")
        return

    entries = json.loads(TRADE_LOG.read_text())
    today_entries = []
    for e in entries:
        placed_at = e.get("placed_at", "")
        if not placed_at:
            continue
        try:
            d = datetime.fromisoformat(placed_at.replace("Z", "+00:00")).date()
        except ValueError:
            continue
        if str(d) == today and not e.get("paper", False):
            today_entries.append(e)

    total_logged = sum(
        float(e.get("entry_price", 0)) * float(e.get("contracts", 0))
        for e in today_entries
    )

    print(f"=== TODAY'S TRADE LOG RECONCILIATION ({today}) ===")
    print(f"  engine_capital_deployed.json total deployed: ${total_deployed:.2f}")
    print(f"  by engine: " + "  ".join(f"{k}=${v:.2f}" for k, v in deployed.items() if v > 0))
    print(f"  trade_log.json non-paper entries today: {len(today_entries)}, "
          f"summing to ${total_logged:.2f}")

    gap = total_deployed - total_logged
    if gap > TOLERANCE:
        print(f"\n  *** MISMATCH: ${gap:.2f} MORE capital deployed than logged. ***")
        print(f"  This is consistent with a lost trade_log.json write. Cross-check")
        print(f"  against Kalshi's own fills history for today with:")
        print(f"    python3 tools/audit_trade_log_vs_kalshi.py")
    elif gap < -TOLERANCE:
        print(f"\n  Logged (${total_logged:.2f}) exceeds deployed (${total_deployed:.2f}) "
              f"by ${-gap:.2f} — unexpected the other direction; worth understanding "
              f"(possibly a paper entry incorrectly marked paper=False, or capital "
              f"file reset mid-day) but not the race condition this script targets.")
    else:
        print(f"\n  Reconciles within ${TOLERANCE:.2f} tolerance — no evidence of a "
              f"lost write today.")


if __name__ == "__main__":
    main()
