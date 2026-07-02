#!/usr/bin/env python3
"""
tools/inspect_ticker_day.py — diagnostic only. Writes nothing.

WHY: tools/probe_observed_final_method.py showed a bizarre asymmetry —
last_poll beats all_polls for HIGH (as expected) but is catastrophically
WORSE for LOWT, disagreeing with all_polls on 88-100% of days for nearly
every city. That's too large and too universal to be noise, and doesn't
match the "glitch persistence" theory that explained the HIGH result. Rather
than guess at a second theory blind, this dumps the actual raw poll
sequence for one (city, market_date) so the real pattern is visible instead
of inferred.

Prints every observations row for the given city + market_type + market_date,
in chronological order: poll_time_utc, local_hour, observed value, forecast
value, ticker. If --date is omitted, auto-picks the market_date for that
city where all_polls-min and last-poll disagree the most (the worst-case
example), so there's always something informative to look at without
needing to already know a bad date.

USAGE (repo root):
    python3 tools/inspect_ticker_day.py --city "San Francisco" --market lowt
    python3 tools/inspect_ticker_day.py --city "San Francisco" --market lowt --date 2026-06-15
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from analytics import OBS_DB
from analytics.wm_time import market_date_iso


def find_worst_date(con, city: str, market_type: str, obs_col: str, extreme_fn) -> str | None:
    rows = con.execute(f"""
        SELECT ticker, poll_time_utc, {obs_col}
        FROM observations
        WHERE city = ? AND market_type = ? AND {obs_col} IS NOT NULL
    """, (city, market_type)).fetchall()

    by_date: dict[str, list[tuple[str, float]]] = {}
    for ticker, poll_time, obs in rows:
        md = market_date_iso(ticker)
        if not md:
            continue
        by_date.setdefault(md, []).append((poll_time, float(obs)))

    worst_date, worst_gap = None, -1.0
    for date, points in by_date.items():
        if len(points) < 2:
            continue
        all_polls_val = extreme_fn(p[1] for p in points)
        last_poll_val = max(points, key=lambda p: p[0])[1]
        gap = abs(all_polls_val - last_poll_val)
        if gap > worst_gap:
            worst_gap, worst_date = gap, date
    return worst_date


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--city", required=True)
    ap.add_argument("--market", choices=["high", "lowt"], required=True)
    ap.add_argument("--date", default=None, help="ISO date, e.g. 2026-06-15. "
                     "If omitted, auto-picks the worst-disagreement date for this city.")
    args = ap.parse_args()

    obs_col = "observed_high_f" if args.market == "high" else "observed_low_f"
    fcst_col = "forecast_high_f" if args.market == "high" else "forecast_low_f"
    extreme_fn = max if args.market == "high" else min

    if not OBS_DB.exists():
        raise SystemExit(f"No database at {OBS_DB}.")
    con = sqlite3.connect(f"file:{OBS_DB}?mode=ro", uri=True)

    date = args.date or find_worst_date(con, args.city, args.market, obs_col, extreme_fn)
    if not date:
        raise SystemExit(f"No data found for {args.city} / {args.market}.")

    rows = con.execute(f"""
        SELECT DISTINCT poll_time_utc, ticker, local_hour, {obs_col}, {fcst_col}
        FROM observations
        WHERE city = ? AND market_type = ?
        ORDER BY poll_time_utc
    """, (args.city, args.market)).fetchall()

    matching = [r for r in rows if market_date_iso(r[1]) == date]

    print(f"\n{args.city} / {args.market.upper()} / market_date={date}  "
          f"({len(matching)} poll rows, deduped across brackets)\n")
    print(f"  {'poll_time_utc':<22} {'ticker':<28} {'local_hr':>8}  "
          f"{'observed':>9}  {'forecast':>9}")
    print(f"  {'-'*84}")
    seen_polls = set()
    for poll_time, ticker, local_hour, obs, fcst in matching:
        # One row per poll (dedupe the 6x-per-bracket duplication) for readability.
        key = (poll_time, obs, fcst)
        if key in seen_polls:
            continue
        seen_polls.add(key)
        obs_s = f"{obs:.1f}" if obs is not None else "—"
        fcst_s = f"{fcst:.1f}" if fcst is not None else "—"
        print(f"  {poll_time:<22} {ticker:<28} {local_hour!s:>8}  {obs_s:>9}  {fcst_s:>9}")

    if matching:
        obs_vals = [r[3] for r in matching if r[3] is not None]
        if obs_vals:
            all_polls_val = extreme_fn(obs_vals)
            last_poll_val = matching[-1][3]
            print(f"\n  all_polls {'max' if args.market=='high' else 'min'}: {all_polls_val}")
            print(f"  last_poll value:            {last_poll_val}")

    con.close()


if __name__ == "__main__":
    main()
