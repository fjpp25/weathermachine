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
                          MORNING_HOURS (9-11) for that market_date. Matches
                          bias_calculator.py's existing convention — this is
                          the forecast the decision engine actually reads at
                          entry time.
    observed final      = max(observed_high_f) over ALL polls for that
                          market_date (running max -> settles at/after the
                          true high).

  LOWT:
    forecast reference = mean(forecast_low_f) over polls at local_hour in
                          EVENING_HOURS (18-21) for that market_date — the
                          same evening window RATCHET / Signal B trade
                          against, so this lines up with what the engines
                          see, not an arbitrary reference point.
    observed final      = min(observed_low_f) over ALL polls for that
                          market_date (running min -> bottoms out at the
                          true overnight low).

  error = forecast_reference - observed_final
    Positive -> forecast ran WARM (overestimated)
    Negative -> forecast ran COOL (underestimated)

No trimming is applied (bias_calculator.py trims the single high/low outlier
per city — deliberately not done here so you can see the raw distribution,
including forecast busts, per city before deciding whether trimming is
appropriate).

IMPORTANT CAVEAT — READ BEFORE TRUSTING THESE NUMBERS AT FACE VALUE
----------------------------------------------------------------------
"observed_final" here comes from the NWS ASOS feed recorded in
observations.db, NOT from the Kalshi-authoritative settlement in the
`settlements` table. Project history has already established these two
sources agree only ~87.6% of the time (station mismatches, ASOS
quantization, bracket-geometry parsing — see the open "canonical
ticker/bracket-to-observation reconciliation" task). That means for
cities/days where the NWS observation is wrong, this script is measuring
"forecast vs NWS's own bad reading," not "forecast vs what actually
happened." Run with --validate to see, per city, what fraction of days the
NWS-derived observed value actually falls inside that day's Kalshi-
authoritative winning-bracket range (from market_days). Treat any city with
a low match rate skeptically — its bias number may be noise, not signal.

USAGE (repo root, on the Pi, after fetch_settlements.py +
load_settlements_to_db.py + tools/build_market_days.py have run):
    python3 tools/forecast_error_by_city.py
    python3 tools/forecast_error_by_city.py --market high
    python3 tools/forecast_error_by_city.py --market lowt --validate
"""
from __future__ import annotations

import argparse
import math
import sqlite3
from collections import defaultdict

from analytics import OBS_DB
from analytics.wm_time import market_date_iso

MORNING_HOURS = {9, 10, 11}       # HIGH: pre-market forecast reference window
EVENING_HOURS = {18, 19, 20, 21}  # LOWT: evening-before forecast reference window
MIN_DAYS_RELIABLE = 5


# ---------------------------------------------------------------------------
# Core computation
# ---------------------------------------------------------------------------

def _fetch_rows(con, market_type: str, fcst_col: str, obs_col: str):
    """Pull raw rows for the given market_type. NOTE: each poll is written
    once per bracket (6x), so this returns 6 identical (fcst, obs) values per
    poll. That's wasteful but not wrong: MEAN/MAX/MIN over duplicated
    identical values equals the same over deduplicated values, so the
    aggregates below are unaffected. Not worth a DISTINCT-on-poll-time here
    given the DB sizes involved."""
    q = f"""
        SELECT city, ticker, local_hour, {fcst_col}, {obs_col}
        FROM observations
        WHERE market_type = ?
    """
    return con.execute(q, (market_type,)).fetchall()


def _compute(con, market_type: str, fcst_col: str, obs_col: str,
             ref_hours: set[int]) -> dict[str, dict]:
    rows = _fetch_rows(con, market_type, fcst_col, obs_col)

    # city -> market_date -> {"fcst": [...], "obs": [...]}
    by_city_date: dict[str, dict[str, dict]] = defaultdict(
        lambda: defaultdict(lambda: {"fcst": [], "obs": []}))

    for city, ticker, local_hour, fcst, obs in rows:
        if not city or not ticker:
            continue
        market_date = market_date_iso(ticker)
        if not market_date:
            continue
        bucket = by_city_date[city][market_date]
        if fcst is not None and local_hour in ref_hours:
            bucket["fcst"].append(float(fcst))
        if obs is not None:
            bucket["obs"].append(float(obs))

    results: dict[str, dict] = {}
    for city, by_date in sorted(by_city_date.items()):
        day_errors = []
        for market_date, bucket in sorted(by_date.items()):
            if not bucket["fcst"] or not bucket["obs"]:
                continue
            fcst_ref = sum(bucket["fcst"]) / len(bucket["fcst"])
            obs_final = max(bucket["obs"]) if market_type == "high" else min(bucket["obs"])
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
    check — does not touch `results`. Returns per-city match-rate stats."""
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

def display(results: dict, label: str, validation: dict | None = None):
    print(f"\n{'='*88}")
    print(f"  Forecast vs Observed — {label}  (source: observations.db)")
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
        high_results = _compute(con, "high", "forecast_high_f", "observed_high_f", MORNING_HOURS)
        high_validation = (_validate(con, "high", high_results)
                            if (args.validate and has_market_days) else None)
        display(high_results, "HIGH", high_validation)

    if args.market in ("lowt", "both"):
        lowt_results = _compute(con, "lowt", "forecast_low_f", "observed_low_f", EVENING_HOURS)
        lowt_validation = (_validate(con, "lowt", lowt_results)
                            if (args.validate and has_market_days) else None)
        display(lowt_results, "LOW", lowt_validation)

    con.close()


if __name__ == "__main__":
    main()
