#!/usr/bin/env python3
"""
crumbs_backtest.py — Stage 1: is there a (No-price, local-hour) combination where
brackets settle No reliably enough, net of Kalshi fees, to justify entering at
high prices (0.95-0.99)?

Uses ONLY authoritative settlement (the `settlements` table) joined to the
observation history (`observations` table) in observations.db. No temperature
inference, no bracket geometry — so this stage is bulletproof. (The structural
split — *why* a cell works — is Stage 2, and is geometry-dependent.)

Unit of analysis: per-observation. Each poll where we saw a bracket at some
no_price/hour is one row, labelled win (settled No) or loss (settled Yes).
NOTE: rows are correlated (the same bracket appears in many polls), so the
effective sample is smaller than N. The Wilson lower bound is read with that
in mind — treat it as optimistic, not gospel.

Kalshi fee: round_up_to_cent(0.07 * C * P * (1-P)) per order, charged on the
trade regardless of outcome. Lowest at price extremes, but the per-order round-up
can dominate at 1c of edge. We show taker and maker (~1/4) variants since you
rest limit orders.

USAGE (on the Pi):
    python3 crumbs_backtest.py --db data/observations.db
    python3 crumbs_backtest.py --db data/observations.db --min-n 200
"""
import argparse
import math
import sqlite3
import sys
from pathlib import Path


PRICE_BANDS = [(0.95, 0.96), (0.96, 0.97), (0.97, 0.98), (0.98, 0.99), (0.99, 1.00)]
HOUR_BUCKETS = [
    ("overnight", 0, 6),
    ("morning",   6, 12),
    ("afternoon", 12, 18),
    ("evening",   18, 24),
]


def wilson_lower(wins, n, z=1.96):
    if n == 0:
        return 0.0
    p = wins / n
    denom = 1 + z * z / n
    centre = p + z * z / (2 * n)
    margin = z * math.sqrt((p * (1 - p) + z * z / (4 * n)) / n)
    return (centre - margin) / denom


def fee_per_contract(price, maker=False):
    """Kalshi fee, rounded up to the cent, per contract. maker ~ 1/4 taker."""
    raw = 0.07 * price * (1 - price)
    if maker:
        raw *= 0.25
    return math.ceil(raw * 100) / 100.0   # round up to next cent


def breakeven_wr(price, maker=False):
    """
    No bet at `price`: win pays (1-price), loss costs price, plus fee either way.
    Breakeven WR solves: wr*(1-price) - (1-wr)*price - fee = 0
      -> wr = (price + fee) / 1.0   (since payout span is 1.0)
    """
    fee = fee_per_contract(price, maker)
    return price + fee


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="data/observations.db")
    ap.add_argument("--min-n", type=int, default=100,
                    help="cells below this N are shown but flagged unreliable")
    args = ap.parse_args()

    if not Path(args.db).exists():
        sys.exit(f"DB not found: {args.db}")
    con = sqlite3.connect(args.db)

    # Overall base rate, for context.
    row = con.execute("""
        SELECT
          SUM(CASE WHEN s.result='no'  THEN 1 ELSE 0 END),
          SUM(CASE WHEN s.result='yes' THEN 1 ELSE 0 END)
        FROM observations o
        JOIN settlements s ON o.ticker = s.ticker
        WHERE s.result IN ('yes','no')
          AND o.no_price IS NOT NULL
    """).fetchone()
    base_no, base_yes = row[0] or 0, row[1] or 0
    base_total = base_no + base_yes
    base_rate = base_no / base_total if base_total else 0
    print(f"Base rate over ALL joined observations: "
          f"{base_no:,} No / {base_total:,} = {base_rate*100:.1f}% No")
    print(f"(price bands shown only for No-price >= 0.95)\n")

    # Reference breakevens
    print("Fee-adjusted breakeven WR per price band:")
    print(f"  {'band':14} {'taker_BE':>9} {'maker_BE':>9}")
    for lo, hi in PRICE_BANDS:
        mid = (lo + hi) / 2
        print(f"  [{lo:.2f},{hi:.2f})   {breakeven_wr(mid,False)*100:8.1f}% "
              f"{breakeven_wr(mid,True)*100:8.1f}%")
    print()

    header = (f"  {'hour':10} {'price':14} {'N':>7} {'No%':>6} "
              f"{'Wilson_LB':>10} {'maker_BE':>9} {'PASS?':>6}")

    for hname, h0, h1 in HOUR_BUCKETS:
        print(f"=== {hname} ({h0:02d}:00-{h1:02d}:00 local) ===")
        print(header)
        any_row = False
        for lo, hi in PRICE_BANDS:
            r = con.execute("""
                SELECT
                  COUNT(*),
                  SUM(CASE WHEN s.result='no' THEN 1 ELSE 0 END)
                FROM observations o
                JOIN settlements s ON o.ticker = s.ticker
                WHERE s.result IN ('yes','no')
                  AND o.no_price >= ? AND o.no_price < ?
                  AND o.local_hour >= ? AND o.local_hour < ?
            """, (lo, hi, h0, h1)).fetchone()
            n = r[0] or 0
            wins = r[1] or 0
            if n == 0:
                continue
            any_row = True
            rate = wins / n
            lb = wilson_lower(wins, n)
            mid = (lo + hi) / 2
            be_maker = breakeven_wr(mid, True)
            # Pass = Wilson lower bound clears the maker breakeven AND enough N
            passes = (lb >= be_maker) and (n >= args.min_n)
            flag = "PASS" if passes else ("low-N" if n < args.min_n else "no")
            print(f"  {hname:10} [{lo:.2f},{hi:.2f})  {n:>7,} {rate*100:5.1f}% "
                  f"{lb*100:9.1f}% {be_maker*100:8.1f}% {flag:>6}")
        if not any_row:
            print("  (no observations in these price bands for this hour bucket)")
        print()

    con.close()
    print("Reading guide:")
    print("  - 'No%' is the raw settle-No rate in the cell.")
    print("  - 'Wilson_LB' is the conservative true-rate floor (read THIS, not No%).")
    print("  - A cell PASSES only if Wilson_LB >= the fee-adjusted maker breakeven")
    print("    AND N >= min-n. Passing cells are the candidate harvestable edge.")
    print("  - Remember rows are correlated (same bracket, many polls), so the")
    print("    effective sample is smaller than N — treat PASS as necessary, not")
    print("    sufficient. Stage 2 (structural split) explains WHY a cell passes.")


if __name__ == "__main__":
    main()
