#!/usr/bin/env python3
"""
tools/probe_dead_on_arrival.py — quantify the "dead on arrival" bracket
opportunity, and check whether the suspected _check_dismissed/_check_gradient
re-entry bug is actually happening in trade_log.json.

WHY: before touching DISMISSED_NO_MAX or adding a dedup guard's *effect* on
capital allocation, we need real numbers on (a) how many bracket-instances
per market-day actually open dead (Yes <= threshold) across a few candidate
thresholds, (b) how many of those are currently being caught live vs missed
entirely, and (c) whether any single ticker has multiple trade_log entries
under entry_tier in ('tomorrow_dismissed',) — the direct fingerprint of the
missing-dedup bug in _check_dismissed / _check_gradient.

DATA ASSUMPTION: market_type='high_tomorrow' rows are now captured from
market creation (per your confirmation — this was mitigated some weeks ago).
Market-days from BEFORE that mitigation simply won't have these rows at all,
so this naturally only scores the post-mitigation window. No manual date
cutoff needed.

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


def load_tomorrow_opens(con, market_type):
    """
    market_type: 'high' or 'lowt'. Queries market_type + '_tomorrow' rows,
    which are captured continuously from market creation — a trustworthy
    open proxy now, unlike the same-day rows probed previously.

    Returns dict (city, market_date) -> list of (ticker, code, yes_price,
    no_price) at the EARLIEST poll with a complete 6-bracket ladder.
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
    for key, polls in grouped.items():
        for pt in sorted(polls):
            rungs = polls[pt]
            if len(rungs) == 6:
                opens[key] = rungs
                break
        else:
            incomplete += 1
    return opens, incomplete, len(grouped)


def load_trade_log_tier_counts():
    """ticker -> list of trade_log records with entry_tier in the
    dismissed/gradient family, so we can directly check for repeat entries."""
    try:
        raw = json.load(open(TRADE_LOG))
    except FileNotFoundError:
        return {}
    by_ticker = defaultdict(list)
    for t in raw:
        if t.get("entry_tier") in ("tomorrow_dismissed",):
            by_ticker[t.get("ticker", "")].append(t)
    return by_ticker


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--market", choices=["high", "lowt"], default="high")
    args = ap.parse_args()

    con = sqlite3.connect(DB)
    settled = dict(con.execute(
        "SELECT ticker, result FROM settlements WHERE result IN ('yes','no')"))

    opens, incomplete, total_days = load_tomorrow_opens(con, args.market)
    print(f"=== OPEN-LADDER COVERAGE ({args.market}_tomorrow) ===")
    print(f"  market-days seen (any data):        {total_days}")
    print(f"  market-days with a complete open ladder: {len(opens)}")
    print(f"  market-days never had a complete 6-bracket ladder: {incomplete}")

    # -----------------------------------------------------------------
    # Re-entry bug check — direct evidence, not inference from code
    # -----------------------------------------------------------------
    by_ticker = load_trade_log_tier_counts()
    print("\n=== RE-ENTRY BUG CHECK (entry_tier='tomorrow_dismissed') ===")
    repeats = {tk: recs for tk, recs in by_ticker.items() if len(recs) > 1}
    print(f"  distinct tickers ever entered under this tier: {len(by_ticker)}")
    print(f"  tickers entered MORE THAN ONCE:                {len(repeats)}")
    if repeats:
        print("  (each repeat below is a separate order on the SAME ticker —")
        print("   this is the direct fingerprint of the missing dedup guard)")
        for tk, recs in list(repeats.items())[:10]:
            prices = [r.get("entry_price") for r in recs]
            times = [r.get("placed_at", "")[-8:] for r in recs]
            print(f"    {tk}: {len(recs)}x  prices={prices}  times={times}")
    else:
        print("  none found — either the bug isn't firing in practice, or")
        print("  trade_log.json doesn't cover the affected period. Don't")
        print("  conclude the bug is harmless from this alone; check budget")
        print("  burn per city-day too (see below).")

    # -----------------------------------------------------------------
    # Opportunity size across thresholds
    # -----------------------------------------------------------------
    live_tickers = set(by_ticker.keys())

    for thresh in THRESHOLDS:
        candidates = []
        for (city, md), rungs in opens.items():
            for ticker, code, yes_p, no_p in rungs:
                if yes_p is not None and yes_p <= thresh:
                    candidates.append({
                        "city": city, "market_date": md, "ticker": ticker,
                        "code": code, "yes_p": yes_p, "no_p": no_p,
                        "result": settled.get(ticker),
                        "caught_live": ticker in live_tickers,
                    })

        n = len(candidates)
        caught = sum(1 for c in candidates if c["caught_live"])
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

        # per-market-day distribution: how many dead-on-arrival brackets
        # typically show up together at this threshold
        per_day = Counter()
        for (city, md), rungs in opens.items():
            cnt = sum(1 for _, _, yp, _ in rungs if yp is not None and yp <= thresh)
            per_day[cnt] += 1

        print(f"\n=== threshold Yes <= {thresh:.2f} ===")
        print(f"  candidates found:  {n}   caught live: {caught}   "
              f"MISSED: {n - caught}")
        print(f"  settled: {n_settled}  WR: {wr:.0f}%  Wilson_LB: {lb:.0f}%  "
              f"PnL if we'd entered ALL of them: ${pnl:+.2f}")
        print("  per-market-day count of qualifying brackets:")
        for cnt in sorted(per_day):
            print(f"    {cnt} bracket(s)/day: {per_day[cnt]} market-days")

    con.close()


if __name__ == "__main__":
    main()
