#!/usr/bin/env python3
"""
tools/watch_lowt_observed.py — unattended forecast/observed LOW watcher.

WHY THIS EXISTS
----------------
Validates the nws_feed.py fetch_observed_high_low() fix (see project
history) by logging forecast_low_f / observed_low_f for every city over
several unattended hours, so the actual trajectory is visible afterward —
not just two isolated before/after snapshots, which would show "changed or
didn't" but not the shape (a dip-then-recover looks identical to a flat
line if you only sample the endpoints, and shape is exactly what exposed
the original bug in tools/inspect_ticker_day.py).

ASSUMES the nws_feed.py fix is already deployed. If run against the old
(unfixed) nws_feed.py, this will just re-confirm the known bug rather than
validate anything new.

USAGE (repo root, on the Pi):
    # Unattended continuous watch (recommended) — polls every 15 min
    # (matches the live system's own cadence) until Ctrl+C. Safe to leave
    # running while away from the machine.
    python3 tools/watch_lowt_observed.py

    # Custom interval
    python3 tools/watch_lowt_observed.py --interval 20

    # Single snapshot only (for a manual "run now, run again later" plan)
    python3 tools/watch_lowt_observed.py --once

    # Single city
    python3 tools/watch_lowt_observed.py --city "San Francisco"

    # Review whatever's been logged so far — safe to run this at any time,
    # including WHILE a watch is still running in another terminal
    python3 tools/watch_lowt_observed.py --report

Log file: data/lowt_watch_log.jsonl (one JSON record per line, append-only —
safe to run multiple times, nothing is overwritten).
"""
from __future__ import annotations

import argparse
import json
import sys
import time as time_mod
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

# Same fix as every other tools/ script in this repo — running as
# `python3 tools/watch_lowt_observed.py` only puts tools/ on sys.path, not
# the repo root, so `import nws_feed` / `import cities` fail otherwise.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import nws_feed
from cities import CITIES

LOG_FILE = Path("data/lowt_watch_log.jsonl")
DEFAULT_INTERVAL_MIN = 15   # matches the live scheduler's own poll cadence


# ---------------------------------------------------------------------------
# Polling
# ---------------------------------------------------------------------------

def poll_once(city_filter: str | None) -> list[dict]:
    """One poll across all (or one) cities. Returns the records written."""
    snap = nws_feed.snapshot(city_filter=city_filter)
    ts_utc = datetime.now(timezone.utc).isoformat()

    records = []
    for city, data in snap.items():
        if data.get("error"):
            records.append({
                "ts_utc": ts_utc,
                "city": city,
                "error": data["error"],
            })
            continue
        records.append({
            "ts_utc": ts_utc,
            "city": city,
            "local_hour": data.get("city_local_hour"),
            "forecast_low_f": data.get("forecast_low_f"),
            "observed_low_f": data.get("observed_low_f"),
            "current_temp_f": data.get("current_temp_f"),
        })
    return records


def append_log(records: list[dict]):
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(LOG_FILE, "a") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")


def run_watch(interval_min: int, city_filter: str | None, once: bool):
    n = 0
    try:
        while True:
            n += 1
            print(f"\n[{datetime.now(timezone.utc).strftime('%H:%M:%S')} UTC] "
                  f"Poll #{n}...")
            records = poll_once(city_filter)
            append_log(records)

            ok = [r for r in records if "error" not in r]
            errs = [r for r in records if "error" in r]
            for r in sorted(ok, key=lambda x: x["city"]):
                obs = r["observed_low_f"]
                fcst = r["forecast_low_f"]
                obs_s = f"{obs:.1f}" if obs is not None else "  N/A"
                fcst_s = f"{fcst:.1f}" if fcst is not None else "  N/A"
                print(f"  {r['city']:<16} local_hr={r['local_hour']!s:>4}  "
                      f"obs_low={obs_s:>7}  fcst_low={fcst_s:>7}")
            if errs:
                print(f"  ({len(errs)} cities errored this poll — see log)")

            print(f"  Logged {len(records)} records -> {LOG_FILE}")

            if once:
                print("\n--once specified, exiting after single snapshot.")
                return

            print(f"  Next poll in {interval_min} min. Ctrl+C to stop "
                  f"(logged data is safe either way).")
            time_mod.sleep(interval_min * 60)
    except KeyboardInterrupt:
        print(f"\nStopped by user after {n} poll(s). "
              f"Run with --report to review {LOG_FILE}.")


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------

def load_log() -> list[dict]:
    if not LOG_FILE.exists():
        return []
    records = []
    with open(LOG_FILE) as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


def report():
    records = load_log()
    if not records:
        print(f"No data in {LOG_FILE} yet — run a poll first.")
        return

    by_city: dict[str, list[dict]] = defaultdict(list)
    for r in records:
        if "error" not in r:
            by_city[r["city"]].append(r)

    print(f"\n{'='*100}")
    print(f"  LOWT Observed/Forecast Watch — {LOG_FILE}")
    print(f"{'='*100}")
    hdr = (f"  {'City':<16} {'#Polls':>6}  {'First':>7}  {'Last':>7}  "
           f"{'Delta':>7}  {'Min':>7}  {'Max':>7}  {'Rose after min?':>16}")
    print(hdr)
    print(f"  {'-'*96}")

    any_flag = False
    for city, recs in sorted(by_city.items()):
        recs = sorted(recs, key=lambda r: r["ts_utc"])
        obs_vals = [r["observed_low_f"] for r in recs if r["observed_low_f"] is not None]
        if not obs_vals:
            continue

        first, last = obs_vals[0], obs_vals[-1]
        delta = last - first
        vmin, vmax = min(obs_vals), max(obs_vals)

        # Flag: did the value rise at all AFTER hitting its minimum so far?
        # A genuine running minimum should never do this. This is the exact
        # shape that exposed the original bug.
        running_min = obs_vals[0]
        rose_after_min = False
        for v in obs_vals[1:]:
            if v < running_min:
                running_min = v
            elif v > running_min:
                rose_after_min = True
        flag = "YES \u26a0" if rose_after_min else "no"
        if rose_after_min:
            any_flag = True

        print(f"  {city:<16} {len(recs):>6}  {first:>7.1f}  {last:>7.1f}  "
              f"{delta:>+7.1f}  {vmin:>7.1f}  {vmax:>7.1f}  {flag:>16}")

    print(f"\n  'Rose after min?' = YES means observed_low_f increased at some point")
    print(f"  after reaching a new low — mathematically wrong for a genuine running")
    print(f"  minimum. If you see YES here, the fix did not fully resolve the issue")
    print(f"  and it's worth another look before trusting Signal A.")
    if not any_flag:
        print(f"\n  No cities flagged. Consistent with the fix working as intended —")
        print(f"  but the strongest test is still whichever cities were BEFORE their")
        print(f"  true overnight low when you started polling; check local_hour in")
        print(f"  the raw log ({LOG_FILE}) for those cities specifically.")


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Unattended forecast/observed LOW watcher")
    parser.add_argument("--interval", type=int, default=DEFAULT_INTERVAL_MIN,
                         help=f"minutes between polls (default {DEFAULT_INTERVAL_MIN}, "
                              f"matching the live scheduler)")
    parser.add_argument("--city", type=str, default=None, help="filter to one city")
    parser.add_argument("--once", action="store_true",
                         help="single snapshot then exit, instead of continuous watch")
    parser.add_argument("--report", action="store_true",
                         help="print a summary of the log so far, without polling")
    args = parser.parse_args()

    if args.report:
        report()
        return

    if args.city and args.city not in CITIES:
        print(f"Unknown city '{args.city}'. Known cities: {sorted(CITIES.keys())}")
        return

    run_watch(args.interval, args.city, args.once)


if __name__ == "__main__":
    main()
