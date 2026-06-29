#!/usr/bin/env python3
"""
station_mismatch.py — per-city rate at which our OBSERVED low implies a different
LOWT settlement than Kalshi's AUTHORITATIVE result. Tests the station-mismatch
hypothesis (e.g. Chicago KMDW vs KORD) directly.

METHOD: for each settled LOWT B bracket, take the last observed_low_f we recorded,
decide which bracket that low implies (Yes if low in [floor-0.5, cap+0.5)), and
compare "did THIS bracket settle Yes by observation" vs Kalshi's result.

CAVEAT (read before trusting absolutes): observed_low_f is the temp at poll time,
not a guaranteed running daily minimum, so EVERY city has baseline observation
noise. Therefore judge each city RELATIVE to the median city, not against an
absolute threshold. An outlier city = real station divergence; everyone-similar
= just observation noise.

USAGE (on the Pi, from repo root):
    python3 tools/station_mismatch.py
"""
import sqlite3
import statistics
from collections import defaultdict


def bracket_val(ticker):
    seg = ticker.split("-")[-1]
    if seg.startswith("B"):
        try:
            return float(seg[1:])
        except ValueError:
            return None
    return None


def implied_yes(observed_low, bval):
    """B bracket midpoint bval (e.g. 72.5 -> floor 72, cap 73). Yes if low in
    [floor-0.5, cap+0.5) = [bval-1, bval+1)."""
    return (bval - 1.0) <= observed_low < (bval + 1.0)


def main():
    con = sqlite3.connect("data/observations.db")
    settled = dict(con.execute(
        "SELECT ticker, result FROM settlements WHERE result IN ('yes','no')"))

    # last observed_low per (ticker) — use MAX(rowid) as "latest poll"
    rows = con.execute("""
        SELECT o.city, o.ticker, o.observed_low_f
        FROM observations o
        JOIN (SELECT ticker, MAX(rowid) mr FROM observations
              WHERE (market_type='lowt' OR ticker LIKE 'KXLOWT%')
                AND ticker LIKE '%-B%' AND observed_low_f IS NOT NULL
              GROUP BY ticker) last
          ON o.ticker = last.ticker AND o.rowid = last.mr
    """).fetchall()
    con.close()

    by_city = defaultdict(lambda: {"n": 0, "disagree": 0})
    for city, ticker, obs_low in rows:
        res = settled.get(ticker)
        if res not in ("yes", "no"):
            continue
        bval = bracket_val(ticker)
        if bval is None or obs_low is None:
            continue
        imp = "yes" if implied_yes(float(obs_low), bval) else "no"
        c = by_city[city]
        c["n"] += 1
        if imp != res:
            c["disagree"] += 1

    print("Per-city: observed-low-implied settlement vs Kalshi authoritative\n")
    print(f"{'city':16}{'N':>6}{'disagree':>10}{'rate':>8}")
    print("-" * 42)
    rates = []
    table = []
    for city, d in by_city.items():
        if d["n"] == 0:
            continue
        rate = d["disagree"]/d["n"]
        rates.append(rate)
        table.append((city, d["n"], d["disagree"], rate))
    med = statistics.median(rates) if rates else 0
    for city, n, dis, rate in sorted(table, key=lambda x: -x[3]):
        flag = "  <- OUTLIER" if rate > 2*med and rate > 0.10 else ""
        print(f"{city:16}{n:>6}{dis:>10}{rate*100:>7.0f}%{flag}")
    print("-" * 42)
    print(f"median city disagreement rate: {med*100:.0f}%")
    print("\nJudge each city RELATIVE to the median (all share observation noise).")
    print("A city well above 2x median is a real station-divergence candidate;")
    print("Chicago near the median means the -$4 LOWT-UP result was just noise.")


if __name__ == "__main__":
    main()
