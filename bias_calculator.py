"""
bias_calculator.py
------------------
Computes per-city NWS forecast high bias from lowt_observations.json data.

Method:
  For each (city, date) pair recorded in lowt_observations.json:
    1. Morning forecast  : mean forecast_f at local_hour in MORNING_HOURS (9-11)
                           This mirrors the forecast the decision engine reads at entry time.
    2. Settled temp proxy: max observed_f across all polls at or after LATE_HOUR_MIN (15:00)
                           The running observed_high peaks late afternoon — this is the closest
                           proxy we have to the CLI-settled final temperature without a separate
                           results feed.
    3. Error             : morning_forecast - settled_temp
                           Positive  -> NWS overestimated (ran warm)
                           Negative  -> NWS underestimated (ran cool)

  Per-city bias = trimmed mean(error), dropping the single highest and lowest value
                 when n >= TRIM_MIN_SAMPLE (5). This reduces sensitivity to forecast
                 busts (frontal passages, sensor faults) that would otherwise dominate
                 a small sample.

Output:
  data/forecast_bias.json  -- {city: {"bias": float, "stddev": float}}
  Used by hight_decision_engine.py at import time.

Usage:
  python bias_calculator.py           # compute and save
  python bias_calculator.py --show    # print results without saving
"""

import json
import math
import argparse
from pathlib import Path
from collections import defaultdict
from datetime import datetime
from zoneinfo import ZoneInfo

from cities import CITIES

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

OBS_FILE  = Path("data/lowt_observations.csv")
OUT_FILE  = Path("data/forecast_bias.json")

# Local hours considered "morning" for sampling the day's forecast
MORNING_HOURS = {9, 10, 11}

# Minimum local hour for a poll to count as a late/settled reading
LATE_HOUR_MIN = 15

# Minimum number of polls for a (city, date) pair to be included
MIN_POLLS_PER_DAY = 3

# Minimum days for a city's bias to be flagged as reliable
# 5 days gives directional confidence; 7+ gives statistical stability
MIN_DAYS_RELIABLE = 5

# Trim one high + one low outlier when sample size is at least this large
TRIM_MIN_SAMPLE = 5


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _to_float(val):
    try:
        return float(val) if val not in (None, "", "nan") else None
    except (ValueError, TypeError):
        return None


def load_observations() -> list[dict]:
    if not OBS_FILE.exists():
        raise FileNotFoundError(
            f"No observations file at {OBS_FILE}.\n"
            "Run lowt_observer.py first to collect data."
        )

    suffix = OBS_FILE.suffix.lower()

    if suffix == ".json":
        return json.loads(OBS_FILE.read_text())

    if suffix == ".csv":
        import csv
        rows = []
        with open(OBS_FILE, newline="", encoding="utf-8") as f:
            for r in csv.DictReader(f):
                try:
                    r["local_hour"] = int(r["local_hour"]) if r.get("local_hour") else None
                except (ValueError, KeyError):
                    continue

                # Normalise _high_f / _low_f fields to internal observed_f / forecast_f
                mtype = r.get("market_type", "")
                if "observed_high_f" in r or "forecast_high_f" in r:
                    if mtype == "high":
                        r["observed_f"] = _to_float(r.get("observed_high_f"))
                        r["forecast_f"] = _to_float(r.get("forecast_high_f"))
                    else:
                        r["observed_f"] = _to_float(r.get("observed_low_f"))
                        r["forecast_f"] = _to_float(r.get("forecast_low_f"))
                # Old schema already has observed_f / forecast_f

                rows.append(r)
        return rows

    raise ValueError(f"Unsupported observations file format: {suffix}")


def local_date_for(poll_time_utc: str, tz_name: str) -> str:
    """Convert a UTC ISO timestamp to YYYY-MM-DD in the city's local timezone."""
    try:
        dt = datetime.fromisoformat(poll_time_utc.replace("Z", "+00:00"))
        return dt.astimezone(ZoneInfo(tz_name)).strftime("%Y-%m-%d")
    except Exception:
        return poll_time_utc[:10]   # fallback: UTC date prefix


def trimmed_mean(values: list[float]) -> float:
    """
    Mean with the single highest and lowest values removed when len >= TRIM_MIN_SAMPLE.
    Falls back to a plain mean for smaller samples.
    """
    if len(values) >= TRIM_MIN_SAMPLE:
        trimmed = sorted(values)[1:-1]   # drop min and max
    else:
        trimmed = values
    return sum(trimmed) / len(trimmed)


def stddev(values: list[float]) -> float:
    """Population standard deviation."""
    if len(values) < 2:
        return 0.0
    mean = sum(values) / len(values)
    return math.sqrt(sum((v - mean) ** 2 for v in values) / len(values))


# ---------------------------------------------------------------------------
# Core computation
# ---------------------------------------------------------------------------

def compute_bias() -> dict[str, dict]:
    """
    Compute per-city forecast bias from observation history.

    Returns:
        {
          city: {
            "bias":     float,   # trimmed mean(forecast - settled), degrees F
            "stddev":   float,   # population stddev of per-day errors (noise indicator)
            "days":     int,     # qualifying day count (before trimming)
            "reliable": bool,    # True if days >= MIN_DAYS_RELIABLE
            "errors":   [float], # per-day errors, chronological, for inspection
          }
        }
    """
    obs = load_observations()

    # Only HIGH markets -- that's what we trade and what we apply bias to
    obs = [o for o in obs if o.get("market_type") == "high"]

    city_tz = {name: meta["tz"] for name, meta in CITIES.items()}

    # Group: city -> local_date -> list of records
    by_city_date: dict[str, dict[str, list]] = defaultdict(lambda: defaultdict(list))

    for o in obs:
        city = o.get("city")
        if not city or city not in city_tz:
            continue
        if o.get("forecast_f") is None or o.get("observed_f") is None:
            continue
        local_date = local_date_for(o["poll_time_utc"], city_tz[city])
        by_city_date[city][local_date].append(o)

    results = {}

    for city, by_date in sorted(by_city_date.items()):
        errors = []

        for date, records in sorted(by_date.items()):
            # Require minimum poll coverage to avoid noisy days
            if len(records) < MIN_POLLS_PER_DAY:
                continue

            # Morning forecast: mean forecast_f during MORNING_HOURS
            morning_fcsts = [
                r["forecast_f"]
                for r in records
                if r.get("local_hour") in MORNING_HOURS
                and r.get("forecast_f") is not None
            ]
            if not morning_fcsts:
                continue

            morning_forecast = sum(morning_fcsts) / len(morning_fcsts)

            # Settled proxy: max observed_f from polls at or after LATE_HOUR_MIN
            # observed_f for HIGH markets = running observed_high -> peaks near settlement
            late_obs = [
                r["observed_f"]
                for r in records
                if r.get("local_hour", 0) >= LATE_HOUR_MIN
                and r.get("observed_f") is not None
            ]
            if not late_obs:
                continue

            settled = max(late_obs)
            errors.append(round(morning_forecast - settled, 2))

        if not errors:
            continue

        bias    = round(trimmed_mean(errors), 2)
        err_std = round(stddev(errors), 2)

        results[city] = {
            "bias":     bias,
            "stddev":   err_std,
            "days":     len(errors),
            "reliable": len(errors) >= MIN_DAYS_RELIABLE,
            "errors":   errors,
        }

    return results


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

def save_bias(results: dict) -> dict:
    """
    Save {city: {"bias": float, "stddev": float}} to OUT_FILE.
    Returns the saved dict.
    """
    flat = {
        city: {"bias": data["bias"], "stddev": data["stddev"]}
        for city, data in results.items()
    }
    OUT_FILE.parent.mkdir(exist_ok=True)
    OUT_FILE.write_text(json.dumps(flat, indent=2))
    return flat


def display(results: dict):
    print(f"\n{'='*80}")
    print(f"  NWS Forecast Bias  --  HIGH markets only  (trimmed mean, n>={TRIM_MIN_SAMPLE})")
    print(f"{'='*80}")
    print(f"  {'City':<16} {'Bias':>7}  {'StdDev':>7}  {'Days':>5}  {'Reliable':>9}  Recent errors (last 5)")
    print(f"  {'-'*76}")

    known_cities = set(CITIES.keys())
    covered      = set(results.keys())
    missing      = known_cities - covered

    for city, data in sorted(results.items(), key=lambda x: x[1]["bias"]):
        reliable_str = "yes" if data["reliable"] else "no"
        tail         = data["errors"][-5:]
        errors_str   = ("... " if len(data["errors"]) > 5 else "") + \
                       "  ".join(f"{e:+.1f}" for e in tail)
        print(
            f"  {city:<16} {data['bias']:>+7.2f}  {data['stddev']:>7.2f}  "
            f"{data['days']:>5}  {reliable_str:>9}  {errors_str}"
        )

    if missing:
        print(f"\n  Cities with no data (will fall back to global FORECAST_BIAS_CORRECTION):")
        for city in sorted(missing):
            print(f"    {city}")

    print(f"\n  Positive bias -> NWS runs warm (overestimates high)")
    print(f"  Negative bias -> NWS runs cool (underestimates high)")
    print(f"  StdDev -> noise level; high stddev cities warrant extra caution")
    print(f"  Reliable threshold: {MIN_DAYS_RELIABLE}+ qualifying days")


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Compute per-city NWS forecast bias")
    parser.add_argument("--show", action="store_true", help="Print results without saving")
    args = parser.parse_args()

    results = compute_bias()

    if not results:
        print("No qualifying data found. Check that lowt_observations.json has HIGH market records.")
    else:
        display(results)
        if not args.show:
            flat = save_bias(results)
            print(f"\n  Saved {len(flat)} cities -> {OUT_FILE}")
        else:
            print("\n  (dry run -- not saved)")
