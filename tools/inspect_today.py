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

    # Break out by entry_tier, mapped to capital bucket.
    #
    # THIRD ATTEMPT at this mapping — the previous two were wrong (guessed
    # tier names from memory; then left "peak" marked unverified despite
    # having already confirmed it). This version is traced through actual
    # call sites in trader.py, not guessed:
    #
    #   - sweep_engine.py's _place() calls _cap.record("sweep", cost)
    #     directly for ALL its own tiers: "tomorrow" (Signal A),
    #     "tomorrow_dismissed" (dismissed-T + gradient-open),
    #     "tomorrow_dismissed_b" (dismissed-B), "tomorrow_sweep" (Signal B),
    #     "dead_sweep" (Signal C). Confirmed via direct source read.
    #   - peak_scanner.py calls .record("peak", ...) directly (tier="peak").
    #   - evening_convergence.py calls cap.record("econv", cost) directly
    #     (tier="econv").
    #   - trader.py's TOPUP function (~line 1950) calls
    #     cap.record("topup", cost) directly (tier="topup").
    #   - trader.py's _post_exit_scan function (~line 1400, tier=
    #     "post_exit_scan") calls NEITHER cap.record() NOR any budget
    #     check at all — not "legacy tracking", genuinely untracked. Verify
    #     this is intentional (topup positions bounded by existing-position
    #     headroom rather than fresh capital) before assuming it's a gap.
    #   - main/cascade/lowt signals flow through trader.py's central
    #     run_pipeline dispatcher (~line 2147), which derives engine_key
    #     from the tier prefix:
    #       tier.startswith("cascade")  -> "cascade"
    #       tier.startswith("lowt")     -> "lowt"
    #       tier.startswith("tomorrow") or tier in ("dead_sweep","sweep")
    #                                    -> "sweep"  (this branch appears to
    #          be dead code in current practice — decision_engine.run() and
    #          lowt_decision_engine.run() are the only signal sources fed
    #          into this dispatcher, sweep_engine.py bypasses it entirely —
    #          but this is NOT proven with full certainty, just the best-
    #          supported reading so far)
    #       else                        -> "main"
    #     and DOES call cap.record(engine_key, cost) for all of these —
    #     contradicting what I told you last turn. If engine_capital_
    #     deployed.json still shows $0 for cascade/lowt/main despite clear
    #     trade_log evidence of their trades today, that's now a genuinely
    #     open question (stale snapshot timing vs a real persistence gap in
    #     the cross-thread capital write), not something to assume either
    #     way without checking audit_trade_log_today.py fresh.
    SWEEP_DIRECT_TIERS = {"tomorrow", "tomorrow_dismissed", "tomorrow_dismissed_b",
                           "tomorrow_sweep", "dead_sweep"}
    UNTRACKED_TIERS = {"post_exit_scan"}

    def bucket_for(tier: str) -> str:
        if tier in SWEEP_DIRECT_TIERS:
            return "sweep"
        if tier == "peak":
            return "peak"
        if tier == "econv":
            return "econv"
        if tier == "topup":
            return "topup"
        if tier in UNTRACKED_TIERS:
            return f"{tier} [genuinely untracked — no .record() call anywhere for this tier]"
        if tier.startswith("cascade"):
            return "cascade"
        if tier.startswith("lowt"):
            return "lowt"
        return "main"   # trader.py dispatcher's own fallback for anything else

    by_bucket = {}
    for e in todays:
        tier = e.get("entry_tier", "?")
        bucket = bucket_for(tier)
        cost = float(e.get("entry_price", 0)) * float(e.get("contracts", 0))
        by_bucket.setdefault(bucket, [0.0, 0])
        by_bucket[bucket][0] += cost
        by_bucket[bucket][1] += 1

    print("\nlogged total by verified bucket:")
    for bucket, (total, n) in sorted(by_bucket.items()):
        print(f"  ${total:6.2f}  ({n} entries)  {bucket}")


if __name__ == "__main__":
    main()
