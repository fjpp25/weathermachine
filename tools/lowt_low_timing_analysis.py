#!/usr/bin/env python3
"""
tools/lowt_low_timing_analysis.py

WHY THIS EXISTS: every trade_end_lowt cutoff in cities.py is derived
formulaically from Kalshi's market expiry time ("10am ET expiry -> local
hour, minus 1h buffer") — NOT from empirical analysis of when each city's
true daily low actually stops falling. The NYC 2026-07-07 case (true low
recorded 09:28 AM, AFTER our own 09:00 cutoff) showed this formula can be
unsafe. This script uses real historical observation data — untouched by
any of the trade_log/execution bugs investigated elsewhere this session,
since it reads directly from the NWS-derived observations DB export, not
trade_log.json or Kalshi fills — to test that per city, at scale, instead
of reasoning from one anecdote.

DATA SOURCE: data/lowt_observations.csv, produced by tools/export_csv.py
from data/observations.db. The 'ticker' column encodes which calendar day
each row's observed_low_f pertains to (e.g. KXLOWTNYC-26JUL08-B63.5 ->
2026-07-08) — this is used to group rows into "trading days" instead of
re-deriving LST day-boundaries here, since the ticker is already the
authoritative label Kalshi itself uses for that grouping.

WHAT IT COMPUTES, per city:
  1. Distribution (median / p90 / max) of the LOCAL HOUR at which
     observed_low_f recorded its FINAL value for that trading day — i.e.
     the last time a new (colder) record low was set. This is the
     empirical answer to "how late can the true low still be moving."
  2. How many trading days had observed_low_f STILL FALLING at/after the
     city's own configured trade_end_lowt — directly quantifies the July
     7 NYC pattern across full history, not just one day.
  3. Monotonicity violations: any case where observed_low_f INCREASES
     from one poll to the next within the same trading day. observed_low_f
     is meant to be a running minimum — a real increase should be
     impossible and points at a data/window bug, not a temperature event.
     Split pre/post 2026-07-02 (the nws_feed.py observation-window fix) to
     see whether that fix actually reduced these.

USAGE (repo root, on the Pi):
    python3 tools/lowt_low_timing_analysis.py
    python3 tools/lowt_low_timing_analysis.py --csv data/lowt_observations.csv
    python3 tools/lowt_low_timing_analysis.py --city "New York"
"""
from __future__ import annotations

import argparse
import re
import sys
from datetime import date, datetime
from pathlib import Path

import pandas as pd

TICKER_DATE_RE = re.compile(r"-(\d{2})([A-Z]{3})(\d{2})-")
MONTH_MAP = {
    "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
    "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
}
WINDOW_FIX_DATE = date(2026, 7, 2)  # nws_feed.py observation-window fix


def _ticker_to_date(ticker: str):
    if not isinstance(ticker, str):
        return None
    m = TICKER_DATE_RE.search(ticker)
    if not m:
        return None
    yy, mon, dd = m.groups()
    month = MONTH_MAP.get(mon)
    if month is None:
        return None
    try:
        return date(2000 + int(yy), month, int(dd))
    except ValueError:
        return None


def _parse_local_minutes(local_time: str):
    """'09:28 EDT' -> 568 (minutes since midnight). None if unparseable."""
    if not isinstance(local_time, str):
        return None
    m = re.match(r"(\d{1,2}):(\d{2})", local_time.strip())
    if not m:
        return None
    h, mi = int(m.group(1)), int(m.group(2))
    return h * 60 + mi


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--csv", default="data/lowt_observations.csv")
    ap.add_argument("--city", default=None, help="restrict to one city")
    ap.add_argument("--since", default=None,
                    help="YYYY-MM-DD — only include trading days on/after this "
                         "date. Use --since 2026-07-02 to check whether "
                         "still_falling/med_drop hold up on clean data alone, "
                         "since 15,642 of 15,783 total violations happened "
                         "before that date's observation-window fix.")
    args = ap.parse_args()

    if not Path(args.csv).exists():
        sys.exit(f"{args.csv} not found — run tools/export_csv.py --full first.")

    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from cities import CITIES

    print(f"Loading {args.csv} ...")
    df = pd.read_csv(args.csv, low_memory=False)
    df = df[df["market_type"] == "lowt"].copy()
    if args.city:
        df = df[df["city"] == args.city]
    df = df.dropna(subset=["observed_low_f", "poll_time_utc", "ticker"])
    df["trading_date"] = df["ticker"].apply(_ticker_to_date)
    df = df.dropna(subset=["trading_date"])
    if args.since:
        since_date = datetime.strptime(args.since, "%Y-%m-%d").date()
        df = df[df["trading_date"] >= since_date]

    # Exclude the most recent trading date entirely. It's very likely still
    # in progress (this script gets run mid-day), and an INCOMPLETE day's
    # "last observed_low_f update" is trivially whatever the most recent
    # poll happened to be — i.e. "now" — not a genuine signal about when
    # the true low locks in. Confirmed via a live run: with --since applied
    # (small sample), every city's "final low hour" clustered at exactly
    # the local-time equivalent of one shared UTC timestamp (~23:00-24:00
    # UTC) — the export's own run time, not a weather pattern. Only
    # completed days (settlement already happened, no more polls coming)
    # give a real answer.
    max_date = df["trading_date"].max()
    n_before = len(df)
    df = df[df["trading_date"] < max_date]
    n_excluded = n_before - len(df)
    if n_excluded:
        print(f"Excluding {max_date} (most recent date, {n_excluded:,} rows) "
              f"as still in-progress — see note in source.")
    df["poll_time_utc"] = pd.to_datetime(df["poll_time_utc"], utc=True, errors="coerce")
    df = df.dropna(subset=["poll_time_utc"])
    # Precise minutes-since-midnight for the cutoff comparison — NOT
    # local_hour (an integer bucket: 09:00 through 09:59 all round to 9,
    # which would wrongly count a 09:28 reading as "before" a 09:00
    # cutoff — exactly the real NYC case this script exists to catch).
    df["local_minutes"] = df["local_time"].apply(_parse_local_minutes)
    df = df.sort_values(["city", "trading_date", "poll_time_utc"])

    print(f"  {len(df):,} LOWT observation rows across "
          f"{df['city'].nunique()} cities, "
          f"{df.groupby(['city', 'trading_date']).ngroups:,} city-days.\n")

    final_low_hours: dict[str, list] = {}
    still_falling_at_cutoff: dict[str, int] = {}
    drop_magnitudes: dict[str, list] = {}
    total_days: dict[str, int] = {}
    violations: dict[str, list] = {}  # city -> list of (date, pre/post-fix bool)

    for (city, tdate), g in df.groupby(["city", "trading_date"]):
        g = g.sort_values("poll_time_utc")
        cutoff = CITIES.get(city, {}).get("trade_end_lowt")
        total_days.setdefault(city, 0)
        total_days[city] += 1

        running_min = None
        final_low_hour = None
        low_at_cutoff = None
        prev_val = None
        cutoff_minutes = cutoff * 60 if cutoff is not None else None

        for _, row in g.iterrows():
            val = row["observed_low_f"]
            hour = row["local_hour"]
            minutes = row["local_minutes"]
            if running_min is None or val < running_min - 1e-9:
                running_min = val
                final_low_hour = hour
            if (cutoff_minutes is not None and minutes is not None
                    and minutes <= cutoff_minutes):
                low_at_cutoff = running_min
            if prev_val is not None and val > prev_val + 1e-9:
                violations.setdefault(city, []).append(
                    (tdate, tdate >= WINDOW_FIX_DATE)
                )
            prev_val = val

        if (cutoff is not None and low_at_cutoff is not None
                and running_min is not None and running_min < low_at_cutoff - 1e-9):
            drop_magnitudes.setdefault(city, []).append(low_at_cutoff - running_min)

        if final_low_hour is not None:
            final_low_hours.setdefault(city, []).append(final_low_hour)

        if cutoff is not None and low_at_cutoff is not None and running_min is not None:
            if running_min < low_at_cutoff - 1e-9:
                still_falling_at_cutoff.setdefault(city, 0)
                still_falling_at_cutoff[city] += 1

    print(f"{'city':<16}{'cutoff':>7}{'days':>6}{'med_hr':>8}{'p90_hr':>8}"
          f"{'max_hr':>8}{'still_falling':>15}{'med_drop':>10}{'violations':>12}")
    print("-" * 98)
    for city in sorted(total_days.keys()):
        cutoff = CITIES.get(city, {}).get("trade_end_lowt")
        hours = sorted(final_low_hours.get(city, []))
        n = len(hours)
        med = hours[n // 2] if n else None
        p90 = hours[int(n * 0.9)] if n else None
        mx = max(hours) if hours else None
        falling = still_falling_at_cutoff.get(city, 0)
        n_viol = len(violations.get(city, []))
        drops = sorted(drop_magnitudes.get(city, []))
        med_drop = f"{drops[len(drops)//2]:.2f}°F" if drops else "-"
        print(f"{city:<16}{str(cutoff):>7}{total_days[city]:>6}"
              f"{str(med):>8}{str(p90):>8}{str(mx):>8}"
              f"{falling:>15}{med_drop:>10}{n_viol:>12}")

    print(f"\n'med_drop' = median size of the post-cutoff drop, in the "
          f"'still_falling' cases only (low_at_cutoff minus the eventual "
          f"final low). This is the key number for telling two very "
          f"different explanations apart: TINY drops (well under 1°F) look "
          f"like a rolling-observation-window artifact re-surfacing a "
          f"nearby reading, not a real event — the fix would be in how "
          f"observed_low_f is computed, not the trading cutoff. LARGE "
          f"drops (multiple °F) look like genuine late-day cooling (a cold "
          f"front or rain arriving after the cutoff) — the fix would be "
          f"pushing trade_end_lowt later for that city. Check this before "
          f"acting on 'still_falling' alone.")

    all_violations = [v for vs in violations.values() for v in vs]
    if all_violations:
        pre_fix = sum(1 for _, post in all_violations if not post)
        post_fix = sum(1 for _, post in all_violations if post)
        print(f"\nMonotonicity violations (observed_low_f increased within a "
              f"trading day — should be impossible if it's a true running "
              f"minimum): {len(all_violations)} total, {pre_fix} before "
              f"2026-07-02 (nws_feed.py window fix), {post_fix} on/after. "
              f"A big pre/post gap would confirm that fix actually helped; "
              f"a similar rate on both sides means something else is "
              f"causing these.")
    else:
        print(f"\nNo monotonicity violations found — observed_low_f behaves "
              f"as a true running minimum throughout, at least in this data.")

    print(f"\n'still_falling' = trading days where the true low kept "
          f"dropping AFTER the city's own trade_end_lowt cutoff — i.e. "
          f"entries made near the cutoff could have been acting on a low "
          f"that hadn't fully arrived yet, the same pattern found in the "
          f"NYC 2026-07-07 case. A city with a high count here is a "
          f"candidate for pushing trade_end_lowt later, independent of "
          f"anything found in trade_log.json this session.")


if __name__ == "__main__":
    main()
