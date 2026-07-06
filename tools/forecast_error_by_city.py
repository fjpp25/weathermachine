#!/usr/bin/env python3
"""
tools/forecast_error_by_city.py — per-city NWS forecast-vs-observed error,
for both HIGH and LOWT markets, computed directly from observations.db.

WHY THIS EXISTS
----------------
tools/bias_calculator.py already computes something adjacent to this, but:
  - HIGH only — no LOWT equivalent.
  - Still reads data/lowt_observations.csv, not the migrated observations.db.
  - Groups by the poll's own local calendar date, which is fine for HIGH but
    wrong for LOWT: a LOWT-30JUN market's overnight low is priced in the
    evening of the 29th and observed into the early morning of the 30th —
    two different local calendar dates for ONE market.

This script groups by the ticker's market_date (analytics.wm_time.
market_date_iso — the settlement date), not the poll's local date, so every
poll belonging to one market stays in one bucket regardless of which side of
local midnight it landed on.

METHOD
------
  HIGH:
    forecast reference = mean(forecast_high_f) over polls at local_hour in
                          MORNING_HOURS (9-11) for that market_date.
    observed final      = observed_high_f from the LAST poll (by poll_time_utc)
                          of that market_date. Validated against
                          tools/probe_observed_final_method.py: this beats
                          max-across-all-polls by +10.6pp average AuthMatch
                          (17/20 cities improved). Reasoning: nws_feed.py's
                          fetch_observed_high_low() recomputes max(temps)
                          from only the ~48 most recent station observations
                          each poll — NOT a running accumulator, NOT a full-day
                          window (confirmed empirically: 48 records spanned
                          just 3h39m for KSFO). The afternoon high is usually
                          still inside that short window by the time the last
                          poll of the day runs, so a single late read beats
                          the max of many earlier, more glitch-exposed reads.

  LOWT — ⚠ SEE CAVEAT BELOW, THIS METHOD IS KNOWN TO BE UNRELIABLE PRE-FIX ⚠
    forecast reference = mean(forecast_low_f) over polls at local_hour in
                          EVENING_HOURS (18-21) for that market_date.
    observed final      = min(observed_low_f) over ALL polls for that
                          market_date. Pre-fix this was the less-bad of two
                          bad options, not a validated method — see caveat.
                          Post-fix (see PRE/POST-FIX section below), nws_feed.py
                          bounds the query window to local-midnight-through-now,
                          so min-across-all-polls should now be a genuine
                          running minimum rather than a window artifact — but
                          this has NOT yet been independently re-validated
                          with a large post-fix sample. Use --since to check.

  error = forecast_reference - observed_final
    Positive -> forecast ran WARM (overestimated)
    Negative -> forecast ran COOL (underestimated)

No trimming is applied (bias_calculator.py trims the single high/low outlier
per city — deliberately not done here so you can see the raw distribution,
including forecast busts, per city before deciding whether trimming is
appropriate).

============================================================================
CONFIRMED CAVEAT — LOWT OBSERVED VALUES WERE NOT TRUSTWORTHY PRE-FIX
============================================================================
nws_feed.py's fetch_observed_high_low() used to compute max/min over only the
last ~48 station observations (confirmed empirically at ~3h39m of coverage
for KSFO — likely varies by station/weather, but nowhere near a full day).
For HIGH this was usually fine (the peak is typically only a few hours
before the last poll of the day). For LOWT it was not: the overnight low
happens 10+ hours before the last poll, so by evening the window had
completely forgotten it. tools/probe_observed_final_method.py confirmed this
directly: switching LOWT to last-poll made match rates COLLAPSE (avg
-38.8pp, some cities to ~15%), and tools/inspect_ticker_day.py showed
observed_low_f rising by 20-40+ degrees across the day in concrete examples —
mathematically impossible for a genuine running minimum.

This was fixed in commit bde2629 (2026-07-02 15:54 local / 14:54 UTC — bounds
the query to local-midnight-through-now instead of a fixed record count),
then a same-day URL-encoding hotfix in commit 49cf2a4 (2026-07-02 18:33
local / 17:33:09 UTC — between these two commits observed_low_f was NULL for
every poll, so those rows simply drop out of the queries below rather than
corrupting anything).

============================================================================
PRE/POST-FIX CONTAMINATION WARNING
============================================================================
The fix only repairs data collected from 2026-07-02T17:33:09Z forward. It
does NOT retroactively repair rows already in observations.db from before
that instant. Running this script (or --validate) with no --since filter
blends months of pre-fix, window-truncated LOWT rows together with a
handful of post-fix days — and given the fix landed only a few days ago,
an unfiltered run is measuring the OLD bug almost exclusively, not current
reality. The original ~87.6% NWS-vs-authoritative figure referenced in
project history, and any AuthMatch number produced without --since, should
be read the same way: mostly a description of the old bug, not of where
things stand now. Pass --since 2026-07-02T17:35:00Z to isolate the
post-fix-only picture (note: as of early July 2026 this will only be a
handful of days per city — treat post-fix numbers as a small, growing
sample, not a final verdict, until more days accumulate).
============================================================================

Separately, even a clean post-fix AuthMatch number only confirms our
MEASUREMENT is trustworthy (does observed_low_f agree with Kalshi's
settlement). It says nothing about whether the market's No/Yes PRICE at
entry time was a good bet given the true outcome distribution — that is a
distinct pricing-efficiency question this script does not answer.

USAGE (repo root, on the Pi, after fetch_settlements.py +
load_settlements_to_db.py + tools/build_market_days.py have run):
    python3 tools/forecast_error_by_city.py
    python3 tools/forecast_error_by_city.py --market high
    python3 tools/forecast_error_by_city.py --market lowt --validate
    python3 tools/forecast_error_by_city.py --market lowt --validate --since 2026-07-02T17:35:00Z
"""
from __future__ import annotations

import argparse
import math
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path

# Same fix as tools/build_market_days.py / tools/audit_trade_log_vs_kalshi.py —
# running as `python3 tools/forecast_error_by_city.py` only puts tools/ on
# sys.path, not the repo root, so `from analytics import ...` fails otherwise
# (analytics/__init__.py itself adds the repo root to sys.path, but only
# *after* it's been successfully imported — chicken-and-egg on first import).
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from analytics import OBS_DB
from analytics.wm_time import market_date_iso

MORNING_HOURS = {9, 10, 11}       # HIGH: pre-market forecast reference window
EVENING_HOURS = {18, 19, 20, 21}  # LOWT: evening-before forecast reference window
MIN_DAYS_RELIABLE = 5

# The instant nws_feed.py's window fix became genuinely effective (commit
# 49cf2a4, "fixed nws feed" — the URL-encoding hotfix on top of bde2629's
# original window fix). Rows before this are pre-fix or mid-outage; do not
# blend them with post-fix rows when judging current reliability.
KNOWN_FIX_CUTOVER_UTC = "2026-07-02T17:33:09Z"


# ---------------------------------------------------------------------------
# Core computation
# ---------------------------------------------------------------------------

def _fetch_rows(con, market_type: str, fcst_col: str, obs_col: str, since: str | None):
    """Pull raw rows for the given market_type, including poll_time_utc so
    HIGH can identify its last poll (see METHOD in module docstring). NOTE:
    each poll is written once per bracket (6x), so this returns 6 identical
    (poll_time, fcst, obs) rows per poll. Wasteful but not wrong for
    MEAN/MAX/MIN aggregates; for the last-poll lookup, duplicate identical
    timestamps resolve to the same value regardless of which duplicate wins
    the max().

    If `since` is given, only rows with poll_time_utc >= since are returned —
    see PRE/POST-FIX CONTAMINATION WARNING in the module docstring for why
    this matters for LOWT."""
    q = f"""
        SELECT city, ticker, local_hour, poll_time_utc, {fcst_col}, {obs_col}
        FROM observations
        WHERE market_type = ?
    """
    params: list = [market_type]
    if since:
        q += " AND poll_time_utc >= ?"
        params.append(since)
    return con.execute(q, params).fetchall()


def _compute(con, market_type: str, fcst_col: str, obs_col: str,
             ref_hours: set[int], since: str | None) -> dict[str, dict]:
    rows = _fetch_rows(con, market_type, fcst_col, obs_col, since)

    # city -> market_date -> {"fcst": [...], "obs": [(poll_time_utc, value), ...]}
    by_city_date: dict[str, dict[str, dict]] = defaultdict(
        lambda: defaultdict(lambda: {"fcst": [], "obs": []}))

    for city, ticker, local_hour, poll_time, fcst, obs in rows:
        if not city or not ticker:
            continue
        market_date = market_date_iso(ticker)
        if not market_date:
            continue
        bucket = by_city_date[city][market_date]
        if fcst is not None and local_hour in ref_hours:
            bucket["fcst"].append(float(fcst))
        if obs is not None:
            bucket["obs"].append((poll_time, float(obs)))

    results: dict[str, dict] = {}
    for city, by_date in sorted(by_city_date.items()):
        day_errors = []
        for market_date, bucket in sorted(by_date.items()):
            if not bucket["fcst"] or not bucket["obs"]:
                continue
            fcst_ref = sum(bucket["fcst"]) / len(bucket["fcst"])
            if market_type == "high":
                # Last poll by poll_time_utc — see METHOD docstring for why
                # this beats max-across-all-polls for HIGH specifically.
                obs_final = max(bucket["obs"], key=lambda p: p[0])[1]
            else:
                # LOWT: min-across-all-polls. Pre-fix this was the less-bad
                # of two bad options; post-fix (bounded local-midnight-to-now
                # window) it should be a genuine running minimum — see
                # CONFIRMED CAVEAT / PRE-POST-FIX WARNING in module docstring.
                obs_final = min(v for _, v in bucket["obs"])
            day_errors.append({
                "date": market_date,
                "forecast": round(fcst_ref, 2),
                "observed": round(obs_final, 2),
                "error": round(fcst_ref - obs_final, 2),
            })
        if not day_errors:
            continue
        vals = [d["error"] for d in day_errors]
        mean_err = sum(vals) / len(vals)
        mae = sum(abs(v) for v in vals) / len(vals)
        sd = (math.sqrt(sum((v - mean_err) ** 2 for v in vals) / len(vals))
              if len(vals) > 1 else 0.0)
        results[city] = {
            "n_days": len(vals),
            "mean_error": round(mean_err, 2),
            "mae": round(mae, 2),
            "stddev": round(sd, 2),
            "reliable": len(vals) >= MIN_DAYS_RELIABLE,
            "days": day_errors,
        }
    return results


def _validate(con, market_type: str, results: dict) -> dict:
    """Cross-check each day's NWS-derived observed value against the
    authoritative winning-bracket range in market_days. Read-only sanity
    check — does not touch `results`. Returns per-city match-rate stats.

    No separate `since` filtering needed here: `results` already only
    contains days whose observations passed through _compute()'s (possibly
    since-filtered) query, so this naturally inherits the same window."""
    rows = con.execute(
        "SELECT city, market_date, settle_lo, settle_hi "
        "FROM market_days WHERE market_type = ? AND n_yes = 1",
        (market_type,)
    ).fetchall()
    auth = {(city, date): (lo, hi) for city, date, lo, hi in rows}

    validation: dict[str, dict] = {}
    for city, data in results.items():
        matched = checked = 0
        for day in data["days"]:
            key = (city, day["date"])
            if key not in auth:
                continue
            lo, hi = auth[key]
            checked += 1
            obs = day["observed"]
            # Interval convention: [settle_lo, settle_hi) — see
            # tools/build_market_days.py / market_utils.bracket_interval.
            lo_ok = lo is None or obs >= lo
            hi_ok = hi is None or obs < hi
            if lo_ok and hi_ok:
                matched += 1
        if checked:
            validation[city] = {
                "checked": checked,
                "matched": matched,
                "match_rate": round(matched / checked, 3),
            }
    return validation


# ---------------------------------------------------------------------------
# Display
# ---------------------------------------------------------------------------

def display(results: dict, label: str, validation: dict | None = None,
            since: str | None = None):
    print(f"\n{'='*88}")
    print(f"  Forecast vs Observed — {label}  (source: observations.db)")
    if since:
        print(f"  Window: poll_time_utc >= {since}")
    else:
        print(f"  Window: ALL HISTORY — pre-fix and post-fix data are BLENDED.")
        print(f"  (nws_feed.py fix cutover was {KNOWN_FIX_CUTOVER_UTC} — pass")
        print(f"   --since {KNOWN_FIX_CUTOVER_UTC} for post-fix-only numbers.)")
    if label.upper().startswith("LOW") and not since:
        print(f"  \u26a0  UNFILTERED — see PRE/POST-FIX CONTAMINATION WARNING in docstring.")
        print(f"  \u26a0  This mostly reflects the OLD ~48-observation window bug, not")
        print(f"  \u26a0  current reality. Re-run with --since before acting on this.")
    print(f"{'='*88}")
    hdr = f"  {'City':<16} {'MeanErr':>8}  {'MAE':>6}  {'StdDev':>7}  {'Days':>5}  {'Reliable':>9}"
    if validation is not None:
        hdr += f"  {'AuthMatch':>10}"
    print(hdr)
    print(f"  {'-' * (len(hdr) - 2)}")

    if not results:
        print("  (no qualifying data)")
        return

    for city, data in sorted(results.items(), key=lambda x: x[1]["mean_error"]):
        line = (f"  {city:<16} {data['mean_error']:>+8.2f}  {data['mae']:>6.2f}  "
                f"{data['stddev']:>7.2f}  {data['n_days']:>5}  "
                f"{'yes' if data['reliable'] else 'no':>9}")
        if validation is not None:
            v = validation.get(city)
            line += f"  {v['match_rate']*100:>9.1f}%" if v else f"  {'—':>10}"
        print(line)

    direction = "high" if "High" in label or "HIGH" in label else "low"
    print(f"\n  Positive mean error -> forecast runs WARM vs NWS-observed (overestimates {direction})")
    print(f"  Negative mean error -> forecast runs COOL vs NWS-observed (underestimates {direction})")
    if validation is not None:
        print(f"\n  AuthMatch = % of days where the NWS-observed value used above actually falls")
        print(f"  inside that day's Kalshi-authoritative winning-bracket range (market_days).")
        print(f"  Low match rate -> the bias number for that city is measuring noise. Discount it.")
        print(f"  NOTE: AuthMatch confirms MEASUREMENT accuracy only, not market pricing efficiency.")


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Per-city forecast-vs-observed temperature error, from observations.db")
    parser.add_argument("--market", choices=["high", "lowt", "both"], default="both")
    parser.add_argument("--validate", action="store_true",
                         help="cross-check observed values against authoritative market_days "
                              "(requires tools/build_market_days.py to have been run)")
    parser.add_argument("--since", default=None,
                         help="ISO8601 UTC timestamp (e.g. 2026-07-02T17:35:00Z). "
                              "Only include observations polled at/after this time. "
                              f"Known nws_feed.py post-fix cutover: {KNOWN_FIX_CUTOVER_UTC}. "
                              "Omit to use ALL history (pre+post fix blended — see warning above).")
    args = parser.parse_args()

    if not OBS_DB.exists():
        raise SystemExit(f"No database at {OBS_DB}.")

    con = sqlite3.connect(f"file:{OBS_DB}?mode=ro", uri=True)

    has_market_days = con.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='market_days'"
    ).fetchone() is not None
    if args.validate and not has_market_days:
        print("  [WARN] --validate requested but market_days table not found — "
              "run tools/build_market_days.py first. Skipping validation.\n")

    if args.market in ("high", "both"):
        high_results = _compute(con, "high", "forecast_high_f", "observed_high_f",
                                 MORNING_HOURS, args.since)
        high_validation = (_validate(con, "high", high_results)
                            if (args.validate and has_market_days) else None)
        display(high_results, "HIGH", high_validation, args.since)

    if args.market in ("lowt", "both"):
        lowt_results = _compute(con, "lowt", "forecast_low_f", "observed_low_f",
                                 EVENING_HOURS, args.since)
        lowt_validation = (_validate(con, "lowt", lowt_results)
                            if (args.validate and has_market_days) else None)
        display(lowt_results, "LOW", lowt_validation, args.since)

    con.close()


if __name__ == "__main__":
    main()
