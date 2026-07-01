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

    # Break out by entry_tier prefix as a rough engine attribution —
    # sweep_engine.py's tiers (directional/sweep/dead_bracket/
    # tomorrow_dismissed/tomorrow_dismissed_b) all draw from the "sweep"
    # capital bucket; anything else is presumably peak_scanner or another
    # engine not yet reviewed for this specific bug pattern.
    SWEEP_TIERS = {"directional", "sweep", "dead_bracket",
                   "tomorrow_dismissed", "tomorrow_dismissed_b"}
    by_bucket = {}
    for e in todays:
        tier = e.get("entry_tier", "?")
        bucket = "sweep" if tier in SWEEP_TIERS else f"other({tier})"
        cost = float(e.get("entry_price", 0)) * float(e.get("contracts", 0))
        by_bucket.setdefault(bucket, [0.0, 0])
        by_bucket[bucket][0] += cost
        by_bucket[bucket][1] += 1

    print("\nlogged total by inferred bucket:")
    for bucket, (total, n) in sorted(by_bucket.items()):
        print(f"  {bucket:30} ${total:.2f}  ({n} entries)")


if __name__ == "__main__":
    main()
