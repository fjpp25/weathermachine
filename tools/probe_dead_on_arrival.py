#!/usr/bin/env python3
"""
tools/probe_dead_on_arrival.py — v3

v3 CHANGES FROM v2
--------------------
1. ADDED: timing-gap diagnostic. v2 fixed the no-quote-artifact bug by
   skipping ahead to the first poll with a trusted ladder sum, but never
   reported HOW FAR ahead that skip typically goes. If it's usually the very
   next poll (~15 min), "dead on arrival" is still a fair description. If
   it's routinely hours later, we're measuring something further from real
   market open than intended, and that needs to be said plainly rather than
   assumed away.
2. ADDED: miss-reason breakdown. Rather than assume DISMISSED_NO_MAX=0.94 is
   why live coverage is thin, this classifies every MISSED candidate against
   the actual live gates in sweep_engine.py:
     - bracket type (T vs B) — _check_dismissed only ever looks at T brackets
     - No price >= DISMISSED_NO_MAX (0.94) — excluded by the ceiling
       regardless of type
     - "other" — type-eligible (T) and under the ceiling, but still missed
       (this bucket is the interesting one — it's neither the type gap nor
       the price ceiling, so if it's non-trivial, something else is going on
       and needs its own explanation before any threshold gets moved)
3. ADDED: loss concentration check (same pattern as the earlier furthest-3
   scar check) — at this win rate, losses are rare enough that a handful of
   bad city-days could be doing most of the damage. Only meaningful once
   n_settled is large enough per threshold; skipped otherwise.

USAGE (on the Pi, from repo root):
    python3 tools/probe_dead_on_arrival.py
    python3 tools/probe_dead_on_arrival.py --market lowt
"""
import argparse
import json
import math
import sqlite3
from collections import defaultdict, Counter

DB = "data/observations.db"
TRADE_LOG = "data/trade_log.json"

THRESHOLDS = [0.05, 0.07, 0.10, 0.15]

LADDER_SUM_BAND = (0.70, 1.30)
RUNG_SUM_BAND = (0.85, 1.10)

TICKER_PREFIX = {"high": "KXHIGH", "lowt": "KXLOWT"}

# Mirrors sweep_engine.py's DISMISSED_NO_MAX — kept as a local constant
# rather than imported, since this script must run standalone without the
# live trading environment. If that constant changes in sweep_engine.py,
# update it here too.
DISMISSED_NO_MAX = 0.94


def wilson_lower(wins, n, z=1.96):
    if n == 0:
        return 0.0
    p = wins / n
    denom = 1 + z * z / n
    centre = p + z * z / (2 * n)
    margin = z * math.sqrt((p * (1 - p) + z * z / (4 * n)) / n)
    return (centre - margin) / denom


def fee(price):
    return math.ceil(0.07 * price * (1 - price) * 100) / 100.0


def market_date(ticker):
    parts = ticker.split("-")
    return parts[1] if len(parts) >= 2 else "?"


def bracket_code_type(code):
    """'T' or 'B' from the stored bracket code string, or None."""
    if code and code[0] in ("T", "B"):
        return code[0]
    return None


def load_tomorrow_opens(con, market_type):
    """
    Returns:
      opens: dict (city, market_date) -> (accepted_poll_time, rungs,
             polls_skipped, first_poll_time)
      incomplete: count of market-days that never had a complete ladder
      total_days: count of market-days seen at all
      ladder_sum_rejected: total rejected first-poll-onward attempts
    """
    mt = f"{market_type}_tomorrow"
    rows = con.execute("""
        SELECT city, poll_time_utc, ticker, bracket, yes_price, no_price
        FROM observations
        WHERE market_type = ?
          AND bracket IS NOT NULL
        ORDER BY city, poll_time_utc
    """, (mt,)).fetchall()

    grouped = defaultdict(lambda: defaultdict(list))
    for city, pt, ticker, code, yes_p, no_p in rows:
        md = market_date(ticker)
        grouped[(city, md)][pt].append((ticker, code, yes_p, no_p))

    opens = {}
    incomplete = 0
    ladder_sum_rejected = 0
    for key, polls in grouped.items():
        sorted_polls = sorted(polls)
        first_pt = sorted_polls[0] if sorted_polls else None
        skipped = 0
        for pt in sorted_polls:
            rungs = polls[pt]
            if len(rungs) != 6:
                continue
            ladder_sum = sum((yp or 0.0) for _, _, yp, _ in rungs)
            if not (LADDER_SUM_BAND[0] <= ladder_sum <= LADDER_SUM_BAND[1]):
                ladder_sum_rejected += 1
                skipped += 1
                continue
            opens[key] = (pt, rungs, skipped, first_pt)
            break
        else:
            incomplete += 1
    return opens, incomplete, len(grouped), ladder_sum_rejected


def load_trade_log_tier_counts(market_type):
    try:
        raw = json.load(open(TRADE_LOG))
    except FileNotFoundError:
        return {}
    prefix = TICKER_PREFIX[market_type]
    by_ticker = defaultdict(list)
    for t in raw:
        if t.get("entry_tier") in ("tomorrow_dismissed",) \
                and str(t.get("ticker", "")).startswith(prefix):
            by_ticker[t.get("ticker", "")].append(t)
    return by_ticker


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--market", choices=["high", "lowt"], default="high")
    args = ap.parse_args()

    con = sqlite3.connect(DB)
    settled = dict(con.execute(
        "SELECT ticker, result FROM settlements WHERE result IN ('yes','no')"))

    opens, incomplete, total_days, ladder_rejected = load_tomorrow_opens(con, args.market)
    print(f"=== OPEN-LADDER COVERAGE ({args.market}_tomorrow) ===")
    print(f"  market-days seen (any data):        {total_days}")
    print(f"  market-days with a trusted open ladder: {len(opens)}")
    print(f"  market-days never had a complete 6-bracket ladder: {incomplete}")
    print(f"  total no-quote-artifact rejections across all market-days: {ladder_rejected}")

    # --- NEW: timing gap diagnostic ---
    print("\n=== TIMING GAP: how many polls before we trust the ladder? ===")
    print("  (0 = the very first poll was already trustworthy; higher = we")
    print("   skipped ahead — if this skews high, 'open' is later than we think)")
    skip_hist = Counter(v[2] for v in opens.values())
    for k in sorted(skip_hist):
        print(f"    skipped {k} poll(s): {skip_hist[k]} market-days")
    # wall-clock gap for the skipped cases
    from datetime import datetime as _dt
    gaps_min = []
    for pt, rungs, skipped, first_pt in opens.values():
        if skipped > 0 and first_pt:
            try:
                t1 = _dt.strptime(first_pt, "%Y-%m-%dT%H:%M:%SZ")
                t2 = _dt.strptime(pt, "%Y-%m-%dT%H:%M:%SZ")
                gaps_min.append((t2 - t1).total_seconds() / 60)
            except Exception:
                pass
    if gaps_min:
        gaps_min.sort()
        n = len(gaps_min)
        print(f"  wall-clock gap when we DID skip (minutes): "
              f"median={gaps_min[n//2]:.0f}  p75={gaps_min[3*n//4]:.0f}  "
              f"p95={gaps_min[int(n*0.95)]:.0f}  max={gaps_min[-1]:.0f}")

    by_ticker = load_trade_log_tier_counts(args.market)
    print(f"\n=== RE-ENTRY BUG CHECK (entry_tier='tomorrow_dismissed', "
          f"{args.market} tickers only) ===")
    repeats = {tk: recs for tk, recs in by_ticker.items() if len(recs) > 1}
    print(f"  distinct tickers ever entered under this tier: {len(by_ticker)}")
    print(f"  tickers entered MORE THAN ONCE:                {len(repeats)}")
    for tk, recs in list(repeats.items())[:10]:
        prices = [r.get("entry_price") for r in recs]
        print(f"    {tk}: {len(recs)}x  prices={prices}")

    live_tickers = set(by_ticker.keys())

    for thresh in THRESHOLDS:
        candidates = []
        rung_rejected = 0
        for (city, md), (pt, rungs, skipped, first_pt) in opens.items():
            for ticker, code, yes_p, no_p in rungs:
                if yes_p is None or no_p is None:
                    continue
                if yes_p > thresh:
                    continue
                if not (RUNG_SUM_BAND[0] <= (yes_p + no_p) <= RUNG_SUM_BAND[1]):
                    rung_rejected += 1
                    continue
                candidates.append({
                    "city": city, "market_date": md, "ticker": ticker,
                    "code": code, "yes_p": yes_p, "no_p": no_p,
                    "result": settled.get(ticker),
                    "caught_live": ticker in live_tickers,
                })

        n = len(candidates)
        caught = [c for c in candidates if c["caught_live"]]
        missed = [c for c in candidates if not c["caught_live"]]
        settled_c = [c for c in candidates if c["result"] in ("no", "yes")]
        wins = sum(1 for c in settled_c if c["result"] == "no")
        pnl = sum(
            (1 - c["no_p"]) - fee(c["no_p"]) if c["result"] == "no"
            else -c["no_p"] - fee(c["no_p"])
            for c in settled_c
        )
        n_settled = len(settled_c)
        wr = wins / n_settled * 100 if n_settled else 0
        lb = wilson_lower(wins, n_settled) * 100 if n_settled else 0

        # --- NEW: miss-reason breakdown ---
        reason_counts = Counter()
        for c in missed:
            btype = bracket_code_type(c["code"])
            over_ceiling = c["no_p"] >= DISMISSED_NO_MAX
            if btype == "B":
                reason_counts["B-bracket (dismissed_t is T-only)"] += 1
            elif over_ceiling:
                reason_counts["T-bracket, No >= 0.94 (excluded by ceiling)"] += 1
            else:
                reason_counts["T-bracket, under ceiling — UNEXPLAINED MISS"] += 1

        print(f"\n=== threshold Yes <= {thresh:.2f} ===")
        print(f"  candidates: {n}   caught live: {len(caught)}   missed: {len(missed)}")
        print(f"  settled: {n_settled}  WR: {wr:.0f}%  Wilson_LB: {lb:.0f}%  "
              f"PnL if we'd entered ALL: ${pnl:+.2f}")
        print("  miss reasons:")
        for reason, cnt in reason_counts.most_common():
            print(f"    {reason}: {cnt}")

        # --- NEW: loss concentration, only if enough losses to be meaningful ---
        losses = [c for c in settled_c if c["result"] == "yes"]
        if len(losses) >= 5:
            for axis_name, axis_fn in (("city", lambda c: c["city"]),
                                        ("market_date", lambda c: c["market_date"])):
                loss_by_key = defaultdict(float)
                for c in losses:
                    loss_by_key[axis_fn(c)] += (-c["no_p"] - fee(c["no_p"]))
                total_loss = sum(loss_by_key.values())
                worst_key = min(loss_by_key, key=lambda k: loss_by_key[k])
                conc = loss_by_key[worst_key] / total_loss if total_loss else 0
                print(f"  loss concentration by {axis_name}: worst={worst_key!r} "
                      f"(${loss_by_key[worst_key]:+.2f}), "
                      f"{len(loss_by_key)} distinct loss-{axis_name}s, "
                      f"concentration={conc:.0%}"
                      + ("  <-- SCAR-LIKE" if conc >= 0.5 and len(loss_by_key) >= 3 else ""))
        else:
            print(f"  ({len(losses)} losses — too few for a concentration check)")

    con.close()


if __name__ == "__main__":
    main()
