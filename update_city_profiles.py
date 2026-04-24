"""
update_city_profiles.py
-----------------------
Updates data/city_profiles.json with stats derived from recent
observations in data/lowt_observations.csv (or the path you specify).

Fields updated for each city/month with sufficient data:
  tmax_normal      — blended mean (prior + observed, weighted by n_days)
  tmax_stddev      — blended stddev (prior + observed, weighted by n_days)
  bracket_difficulty — recalculated as 2 / tmax_stddev

Fields left unchanged (require external data sources):
  diurnal_range, afternoon_climb — need both high + low obs on same day
  tmin_normal                    — use lowt_observations; not critical for HIGH engine

Blending logic:
  new_mean   = (prior_weight * prior_mean + n_obs * obs_mean) / (prior_weight + n_obs)
  new_stddev = sqrt((prior_weight * prior_std² + n_obs * obs_std²) / (prior_weight + n_obs))

  prior_weight defaults to 30 (approximates 30-year normals).
  Set --prior-weight 0 to fully replace with observed data (not recommended
  with fewer than ~30 days of observations).

Usage:
  python update_city_profiles.py
  python update_city_profiles.py --obs data/lowt_observations.csv
  python update_city_profiles.py --profiles data/city_profiles.json
  python update_city_profiles.py --prior-weight 0   # replace, don't blend
  python update_city_profiles.py --dry-run          # print changes, don't write
"""

import json
import argparse
import numpy as np
import pandas as pd
from datetime import datetime, timezone
from pathlib import Path


# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------

DEFAULT_OBS      = Path("data/lowt_observations.csv")
DEFAULT_PROFILES = Path("data/city_profiles.json")
PRIOR_WEIGHT     = 30    # equivalent sample size for the existing normals
MIN_DAYS         = 5     # don't update a month with fewer than this many days


# ---------------------------------------------------------------------------
# Load observations and compute daily highs
# ---------------------------------------------------------------------------

def load_daily_highs(obs_path: Path) -> pd.DataFrame:
    """
    Returns a DataFrame with columns: city, date, month, obs_high_f
    One row per city per day — the maximum observed_high_f for that day.
    """
    print(f"  Loading observations from {obs_path}...")
    obs = pd.read_csv(obs_path, low_memory=False)
    obs['poll_time_utc'] = pd.to_datetime(obs['poll_time_utc'], utc=True)
    obs['date']  = obs['poll_time_utc'].dt.date
    obs['month'] = obs['poll_time_utc'].dt.month

    high = obs[obs['market_type'] == 'high'].copy()

    # Daily max: the observed high accumulates through the day, peak = end of day
    daily = (high.groupby(['city', 'date', 'month'])['observed_high_f']
             .max()
             .reset_index()
             .dropna(subset=['observed_high_f'])
             .rename(columns={'observed_high_f': 'obs_high_f'}))

    print(f"  {len(daily)} city-day observations across "
          f"{daily['city'].nunique()} cities, "
          f"months: {sorted(daily['month'].unique())}")
    return daily


# ---------------------------------------------------------------------------
# Blended update
# ---------------------------------------------------------------------------

def blended_stats(
    prior_mean: float, prior_std: float, prior_weight: float,
    obs_values: list[float],
) -> tuple[float, float]:
    """
    Combine prior (mean, std) with new observations using weighted pooling.
    Returns (new_mean, new_std).
    """
    n_obs = len(obs_values)
    obs_mean = float(np.mean(obs_values))
    obs_std  = float(np.std(obs_values, ddof=1)) if n_obs > 1 else prior_std

    total_w  = prior_weight + n_obs
    new_mean = (prior_weight * prior_mean + n_obs * obs_mean) / total_w
    new_var  = (prior_weight * prior_std**2 + n_obs * obs_std**2) / total_w
    new_std  = float(np.sqrt(new_var))

    return round(new_mean, 1), round(new_std, 1)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def update_profiles(
    obs_path:     Path,
    profiles_path: Path,
    prior_weight: float,
    dry_run:      bool,
) -> None:

    daily    = load_daily_highs(obs_path)
    profiles = json.loads(profiles_path.read_text())

    changes = []

    for city, city_data in profiles.items():
        monthly = city_data.get("monthly", {})

        # Group observations for this city by month
        city_obs = daily[daily['city'] == city]
        if city_obs.empty:
            print(f"  [{city}] no observations found — skipping")
            continue

        for month_str, month_data in monthly.items():
            month_obs = city_obs[city_obs['month'] == int(month_str)]
            n_days    = len(month_obs)

            if n_days < MIN_DAYS:
                continue   # not enough data to update this month

            prior_mean = month_data.get("tmax_normal", 70.0)
            prior_std  = month_data.get("tmax_stddev", 4.0)
            obs_values = month_obs['obs_high_f'].tolist()

            new_mean, new_std = blended_stats(
                prior_mean, prior_std, prior_weight, obs_values
            )
            new_difficulty = round(2.0 / new_std, 3) if new_std > 0 else month_data.get("bracket_difficulty")

            # Record changes
            delta_mean = new_mean - prior_mean
            delta_std  = new_std  - prior_std

            changes.append({
                'city':       city,
                'month':      month_data.get("month_name", f"M{month_str}"),
                'n_obs':      n_days,
                'old_mean':   prior_mean, 'new_mean':   new_mean,
                'old_std':    prior_std,  'new_std':    new_std,
                'delta_mean': delta_mean, 'delta_std':  delta_std,
            })

            if not dry_run:
                month_data["tmax_normal"]        = new_mean
                month_data["tmax_stddev"]         = new_std
                month_data["bracket_difficulty"]  = new_difficulty

        if not dry_run:
            city_data["fetched_at"] = datetime.now(timezone.utc).isoformat()

    # ── Print summary ─────────────────────────────────────────────────────
    if not changes:
        print("\n  No months with sufficient observations to update.")
        return

    print(f"\n  {'City':<16} {'Mo':>4}  {'N':>4}  "
          f"{'tmax_normal':>12}  {'tmax_stddev':>12}  {'bracket_diff':>13}")
    print(f"  {'-'*72}")
    for c in changes:
        mean_arrow = f"{c['old_mean']:.1f}→{c['new_mean']:.1f}"
        std_arrow  = f"{c['old_std']:.1f}→{c['new_std']:.1f}"
        diff_new   = round(2.0 / c['new_std'], 3) if c['new_std'] > 0 else '?'
        diff_old   = round(2.0 / c['old_std'], 3) if c['old_std'] > 0 else '?'
        print(f"  {c['city']:<16} {c['month']:>4}  {c['n_obs']:>4}  "
              f"{mean_arrow:>12}  {std_arrow:>12}  {diff_old}→{diff_new:>6}")

    if dry_run:
        print(f"\n  [DRY RUN] No changes written.")
    else:
        profiles_path.write_text(json.dumps(profiles, indent=2))
        print(f"\n  Updated {profiles_path}  ({len(changes)} month(s) across "
              f"{len(set(c['city'] for c in changes))} cities)")


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Update city_profiles.json from observations")
    parser.add_argument("--obs",          type=Path, default=DEFAULT_OBS,
                        help=f"Path to observations CSV (default: {DEFAULT_OBS})")
    parser.add_argument("--profiles",     type=Path, default=DEFAULT_PROFILES,
                        help=f"Path to city_profiles.json (default: {DEFAULT_PROFILES})")
    parser.add_argument("--prior-weight", type=float, default=PRIOR_WEIGHT,
                        help=f"Equivalent sample size of existing normals (default: {PRIOR_WEIGHT})")
    parser.add_argument("--dry-run",      action="store_true",
                        help="Print changes without writing")
    args = parser.parse_args()

    update_profiles(
        obs_path      = args.obs,
        profiles_path = args.profiles,
        prior_weight  = args.prior_weight,
        dry_run       = args.dry_run,
    )
