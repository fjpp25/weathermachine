#!/usr/bin/env python3
"""
tools/probe_observed_final_method.py — diagnostic only. Writes nothing.

WHY: tools/forecast_error_by_city.py --validate just came back with AuthMatch
rates far below the ~87.6% documented elsewhere in this project (most HIGH
cities 50-70%, some as low as 49%). Before concluding NWS observation data is
this unreliable, or touching data/forecast_bias.json, this tests one specific
methodology hypothesis:

  forecast_error_by_city.py computes observed_final as max(observed_high_f)
  ACROSS ALL POLLS in a market_date. But nws_feed.py's
  fetch_observed_high_low() does NOT maintain a running accumulator — every
  poll re-queries the station's last 48 raw observations and recomputes
  max(temps_today) fresh. So a single poll that catches a transient ASOS
  glitch produces a high observed_high_f value; even if a LATER poll's fresh
  48-observation window no longer contains that glitch (self-corrected or
  aged out), taking max() ACROSS ALL POLLS still locks onto it permanently.
  This is "max of maxes" — strictly more exposed to a single bad poll than
  trusting any one poll's own already-deduplicated value.

This script computes AuthMatch under TWO definitions of observed_final and
prints them side by side, per city:
  (a) "all_polls"  — max/min across every poll of the day (current method)
  (b) "last_poll"  — the single LAST poll of the day's own observed value
                     (chronologically last poll_time_utc in the group)

If (b) recovers meaningfully higher match rates than (a), that points at the
aggregation method, not at NWS/Kalshi fundamentally disagreeing this often.
If (b) is barely different from (a), the low match rate is more likely real
disagreement (station mismatch, quantization, genuine bracket misses) and
the aggregation method is not the culprit.

Also reports, per city, what fraction of days the two methods actually landed
on a DIFFERENT observed value at all — if that fraction is small, neither
method can explain a large AuthMatch gap regardless of which one is "right".

USAGE (repo root, after fetch_settlements.py + load_settlements_to_db.py +
tools/build_market_days.py have run):
    python3 tools/probe_observed_final_method.py
    python3 tools/probe_observed_final_method.py --market lowt
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from analytics import OBS_DB
from analytics.wm_time import market_date_iso


def _fetch(con, market_type: str, obs_col: str):
    """poll_time_utc included so we can find the chronologically-last poll
    per (city, market_date)."""
    return con.execute(f"""
        SELECT city, ticker, poll_time_utc, {obs_col}
        FROM observations
        WHERE market_type = ?
    """, (market_type,)).fetchall()


def _group(con, market_type: str, obs_col: str):
    rows = _fetch(con, market_type, obs_col)
    by_city_date = defaultdict(lambda: defaultdict(list))  # city -> date -> [(poll_time, obs)]
    for city, ticker, poll_time, obs in rows:
        if not city or not ticker or obs is None:
            continue
        md = market_date_iso(ticker)
        if not md:
            continue
        by_city_date[city][md].append((poll_time, float(obs)))
    return by_city_date


def _auth_lookup(con, market_type: str) -> dict:
    rows = con.execute(
        "SELECT city, market_date, settle_lo, settle_hi "
        "FROM market_days WHERE market_type = ? AND n_yes = 1",
        (market_type,)
    ).fetchall()
    return {(city, date): (lo, hi) for city, date, lo, hi in rows}


def _in_range(obs: float, lo: float | None, hi: float | None) -> bool:
    lo_ok = lo is None or obs >= lo
    hi_ok = hi is None or obs < hi
    return lo_ok and hi_ok


def run(con, market_type: str, obs_col: str, extreme_fn):
    by_city_date = _group(con, market_type, obs_col)
    auth = _auth_lookup(con, market_type)

    results = {}
    for city, by_date in sorted(by_city_date.items()):
        n_checked = n_match_all = n_match_last = n_differ = 0
        for date, points in by_date.items():
            key = (city, date)
            if key not in auth:
                continue
            lo, hi = auth[key]
            n_checked += 1

            obs_all_polls = extreme_fn(p[1] for p in points)
            obs_last_poll = max(points, key=lambda p: p[0])[1]  # latest poll_time_utc

            if obs_all_polls != obs_last_poll:
                n_differ += 1
            if _in_range(obs_all_polls, lo, hi):
                n_match_all += 1
            if _in_range(obs_last_poll, lo, hi):
                n_match_last += 1

        if n_checked:
            results[city] = {
                "checked": n_checked,
                "match_rate_all_polls": round(n_match_all / n_checked, 3),
                "match_rate_last_poll": round(n_match_last / n_checked, 3),
                "pct_days_methods_differ": round(n_differ / n_checked, 3),
            }
    return results


def display(results: dict, label: str):
    print(f"\n{'='*92}")
    print(f"  Observed-final method comparison — {label}")
    print(f"{'='*92}")
    hdr = (f"  {'City':<16} {'Checked':>7}  {'AuthMatch':>10}  {'AuthMatch':>10}  "
           f"{'Delta':>7}  {'% days differ':>13}")
    print(hdr)
    print(f"  {'':<16} {'':>7}  {'(all_polls)':>10}  {'(last_poll)':>10}  "
          f"{'':>7}  {'':>13}")
    print(f"  {'-'*88}")
    for city, d in sorted(results.items(), key=lambda x: x[1]["match_rate_all_polls"]):
        delta = d["match_rate_last_poll"] - d["match_rate_all_polls"]
        print(f"  {city:<16} {d['checked']:>7}  "
              f"{d['match_rate_all_polls']*100:>9.1f}%  "
              f"{d['match_rate_last_poll']*100:>9.1f}%  "
              f"{delta*100:>+6.1f}%  "
              f"{d['pct_days_methods_differ']*100:>12.1f}%")

    avg_delta = (sum(d["match_rate_last_poll"] - d["match_rate_all_polls"]
                      for d in results.values()) / len(results)) if results else 0
    print(f"\n  Average delta (last_poll - all_polls): {avg_delta*100:+.1f} percentage points")
    print(f"  If this is large and positive, the all_polls aggregation method")
    print(f"  is a real contributor to the low AuthMatch rate — switch")
    print(f"  forecast_error_by_city.py's observed_final to last_poll.")
    print(f"  If this is small, the low match rate is more likely genuine")
    print(f"  NWS-vs-authoritative disagreement (station mismatch, ASOS")
    print(f"  quantization, real bracket misses) — the aggregation method")
    print(f"  is not the primary culprit.")


def main():
    parser = argparse.ArgumentParser(description="Probe observed_final aggregation method vs AuthMatch")
    parser.add_argument("--market", choices=["high", "lowt", "both"], default="both")
    args = parser.parse_args()

    if not OBS_DB.exists():
        raise SystemExit(f"No database at {OBS_DB}.")

    con = sqlite3.connect(f"file:{OBS_DB}?mode=ro", uri=True)

    has_market_days = con.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='market_days'"
    ).fetchone() is not None
    if not has_market_days:
        con.close()
        raise SystemExit("market_days table not found — run tools/build_market_days.py first.")

    if args.market in ("high", "both"):
        results = run(con, "high", "observed_high_f", max)
        display(results, "HIGH")

    if args.market in ("lowt", "both"):
        results = run(con, "lowt", "observed_low_f", min)
        display(results, "LOW")

    con.close()


if __name__ == "__main__":
    main()
