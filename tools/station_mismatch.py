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

============================================================================
PRE/POST-FIX CONTAMINATION WARNING
============================================================================
nws_feed.py's fetch_observed_high_low() had a ~48-observation (~3-4h) window
bug that made observed_low_f rise 20-40+ degrees across the day instead of
tracking a true running minimum. This was fixed in commit bde2629 (2026-07-02
15:54 local / 14:54 UTC), then a same-day URL-encoding hotfix in commit
49cf2a4 (2026-07-02 18:33 local / 17:33:09 UTC) — between those two commits
observed_low_f/observed_high_f were NULL for every poll (silently swallowed
exception), so those rows simply drop out of this script's queries rather
than corrupting results.

The genuinely trustworthy cutover point is 2026-07-02T17:33:09Z. Running
this script with no --since filter blends that trustworthy data in with
months of pre-fix, window-truncated data — and given the fix landed only a
few days ago, pre-fix rows will vastly outnumber post-fix rows, so an
unfiltered run is measuring the OLD bug almost exclusively, not current
reality. Always pass --since when you actually want to know "is it fixed
now" rather than "how bad was the old bug".
============================================================================

USAGE (on the Pi, from repo root):
    python3 tools/station_mismatch.py                              # ALL history (pre+post fix blended — see warning above)
    python3 tools/station_mismatch.py --since 2026-07-02T17:35:00Z  # post-fix only
"""
import argparse
import sqlite3
import statistics
from collections import defaultdict

# The instant nws_feed.py's window fix became genuinely effective (commit
# 49cf2a4, "fixed nws feed" — the URL-encoding hotfix on top of bde2629's
# original window fix). Rows before this are pre-fix or mid-outage; do not
# blend them with post-fix rows when judging current reliability.
KNOWN_FIX_CUTOVER_UTC = "2026-07-02T17:33:09Z"


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
    parser = argparse.ArgumentParser(
        description="Per-city observed-low-implied-settlement vs Kalshi authoritative result")
    parser.add_argument("--since", default=None,
                         help="ISO8601 UTC timestamp (e.g. 2026-07-02T17:35:00Z). "
                              "Only include observations polled at/after this time. "
                              f"Known post-fix cutover: {KNOWN_FIX_CUTOVER_UTC}. "
                              "Omit to use ALL history (pre+post fix blended).")
    args = parser.parse_args()

    con = sqlite3.connect("data/observations.db")
    settled = dict(con.execute(
        "SELECT ticker, result FROM settlements WHERE result IN ('yes','no')"))

    # last observed_low per (ticker) — use MAX(rowid) as "latest poll".
    # When --since is given, both the row selection AND the "latest poll"
    # window are restricted to rows at/after the cutoff, so a ticker whose
    # only observations are pre-cutoff is correctly excluded entirely rather
    # than falling back to a stale pre-fix value.
    since_clause = ""
    params: list = []
    if args.since:
        since_clause = "AND poll_time_utc >= ?"
        params = [args.since]

    query = f"""
        SELECT o.city, o.ticker, o.observed_low_f
        FROM observations o
        JOIN (SELECT ticker, MAX(rowid) mr FROM observations
              WHERE (market_type='lowt' OR ticker LIKE 'KXLOWT%')
                AND ticker LIKE '%-B%' AND observed_low_f IS NOT NULL
                {since_clause}
              GROUP BY ticker) last
          ON o.ticker = last.ticker AND o.rowid = last.mr
    """
    rows = con.execute(query, params).fetchall()
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
    if args.since:
        print(f"Window: poll_time_utc >= {args.since}\n")
    else:
        print("Window: ALL HISTORY — pre-fix and post-fix data are BLENDED.")
        print(f"        (nws_feed.py fix cutover was {KNOWN_FIX_CUTOVER_UTC} — pass")
        print(f"        --since {KNOWN_FIX_CUTOVER_UTC} to see post-fix-only numbers.)\n")
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
    if not args.since:
        print("\n⚠ Re-run with --since before acting on this — see WINDOW note above.")


if __name__ == "__main__":
    main()
