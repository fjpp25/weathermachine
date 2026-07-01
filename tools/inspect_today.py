#!/usr/bin/env python3
"""
tools/inspect_today.py — dump today's trade_log.json entries in full, to
diagnose the logged-vs-deployed mismatch found by audit_trade_log_today.py.

Not a permanent tool — a throwaway diagnostic for the specific $6.45
discrepancy found on 2026-07-01. Run once, read the output, decide what
(if anything) needs fixing based on what's actually there rather than
guessing among several plausible causes.

USAGE (on the Pi, from repo root):
    python3 tools/inspect_today.py
"""
import json
from datetime import date, datetime
from pathlib import Path

TRADE_LOG = Path("data/trade_log.json")


def main():
    entries = json.loads(TRADE_LOG.read_text())
    today = str(date.today())
    todays = []
    for e in entries:
        pa = e.get("placed_at", "")
        try:
            d = datetime.fromisoformat(pa.replace("Z", "+00:00")).date()
        except Exception:
            continue
        if str(d) == today:
            todays.append(e)

    todays.sort(key=lambda e: e.get("placed_at", ""))
    print(f"{'placed_at':26} {'paper':6} {'tier':22} {'ticker':28} "
          f"{'price':>7} {'contracts':>10} {'market_type':12}")
    for e in todays:
        print(f"{e.get('placed_at','?'):26} "
              f"{str(e.get('paper')):6} "
              f"{e.get('entry_tier','?'):22} "
              f"{e.get('ticker','?'):28} "
              f"{e.get('entry_price', 0):>7} "
              f"{e.get('contracts', 0):>10} "
              f"{e.get('market_type','?'):12}")

    print(f"\ntotal entries today: {len(todays)}")

    tickers = [e.get("ticker") for e in todays]
    dupes = sorted(set(t for t in tickers if tickers.count(t) > 1))
    if dupes:
        print(f"\nDUPLICATE tickers today (same ticker logged more than once):")
        for t in dupes:
            matches = [e for e in todays if e.get("ticker") == t]
            print(f"  {t}: {len(matches)}x")
            for m in matches:
                print(f"    {m.get('placed_at')}  tier={m.get('entry_tier')}  "
                      f"price={m.get('entry_price')}  contracts={m.get('contracts')}")
    else:
        print("no duplicate tickers today.")

    # Break out by entry_tier prefix as a rough engine attribution.
    #
    # CORRECTED (previous version guessed these tier names from memory
    # instead of checking source — every one of that day's 19 entries
    # ended up in "other(...)" as a result, since 4 of 5 guessed names were
    # wrong). Verified directly against sweep_engine.py's actual _place()
    # call sites: Signal A="tomorrow", dismissed-T/gradient-open both share
    # "tomorrow_dismissed", dismissed-B="tomorrow_dismissed_b", Signal B
    # (near-dead sweep)="tomorrow_sweep", Signal C (dead bracket)=
    # "dead_sweep". ALL of these draw from the same "sweep" capital bucket
    # regardless of which one fired — confirmed via the single _cap.record
    # ("sweep", cost) call site inside _place()'s live branch.
    #
    # cascade_engine.py / lowt_decision_engine.py / hight_decision_engine.py
    # (main) do NOT call EngineCapital.record() at all — they use a
    # separate, IN-MEMORY-ONLY legacy tracking mechanism in trader.py
    # (_deployed_cascade / _deployed_lowt / etc.) that never persists to
    # engine_capital_deployed.json and resets on every process restart, not
    # just at midnight. Their entries will NEVER reconcile against that
    # file — that's an architecture fact, not evidence of a lost write.
    SWEEP_TIERS = {"tomorrow", "tomorrow_dismissed", "tomorrow_dismissed_b",
                   "tomorrow_sweep", "dead_sweep"}
    NOT_DISK_TRACKED = {"cascade_lowt_bu", "cascade_ovn_dist", "cascade_tomorrow",
                         "lowt_main", "lowt_a", "near_cap"}
    by_bucket = {}
    for e in todays:
        tier = e.get("entry_tier", "?")
        if tier in SWEEP_TIERS:
            bucket = "sweep"
        elif tier in NOT_DISK_TRACKED:
            bucket = f"{tier} [NOT in engine_capital_deployed.json — legacy in-memory tracking, don't expect this to reconcile]"
        else:
            bucket = f"{tier} [unverified — check source before trusting this attribution]"
        cost = float(e.get("entry_price", 0)) * float(e.get("contracts", 0))
        by_bucket.setdefault(bucket, [0.0, 0])
        by_bucket[bucket][0] += cost
        by_bucket[bucket][1] += 1

    print("\nlogged total by verified bucket:")
    for bucket, (total, n) in sorted(by_bucket.items()):
        print(f"  ${total:6.2f}  ({n} entries)  {bucket}")


if __name__ == "__main__":
    main()
