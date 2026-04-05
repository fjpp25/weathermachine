"""
lowt_analyzer.py
----------------
Morning review of overnight low temperature observations.
Run after lowt_observer.py has collected data overnight.

  python lowt_analyzer.py

Shows:
  - Price evolution per city across the night
  - When each market converged (price > 90%)
  - Whether our boundary buffer would have found signals
  - Best trading windows if any existed
"""

import json
import csv
from pathlib import Path
from datetime import datetime
from collections import defaultdict

BOUNDARY_BUFFER    = 3.0   # same as main strategy
NO_MAX_YES_PRICE   = 0.25
NO_MAX_ENTRY_PRICE = 0.87

OUTPUT_JSON = Path("data/lowt_observations.json")

# ---------------------------------------------------------------------------

def load() -> list[dict]:
    if not OUTPUT_JSON.exists():
        print("No observation file found. Run lowt_observer.py first.")
        return []
    return json.loads(OUTPUT_JSON.read_text())


def analyze():
    obs = load()
    if not obs:
        return

    print("=" * 75)
    print(f"  LOWT Overnight Analysis  —  {len(obs)} observations")
    print("=" * 75)

    # Group by city → ticker → poll_time
    by_city = defaultdict(lambda: defaultdict(list))
    for o in obs:
        by_city[o["city"]][o["ticker"]].append(o)

    for city, tickers in sorted(by_city.items()):
        print(f"\n{'─'*75}")
        print(f"  {city.upper()}")
        print(f"{'─'*75}")

        # Find the leading bracket at each poll time
        poll_times = sorted(set(o["poll_time_utc"]
                                for o in obs if o["city"] == city))

        print(f"\n  Price evolution (YES% of each bracket):")
        print(f"  {'Time':>8}", end="")

        # Get unique brackets
        all_tickers = sorted(tickers.keys())
        for t in all_tickers:
            bracket = t.split("-")[-1]
            print(f"  {bracket:>8}", end="")
        print()

        for pt in poll_times:
            time_str = pt[11:16]  # HH:MM
            print(f"  {time_str:>8}", end="")
            for t in all_tickers:
                poll_obs = [o for o in tickers[t] if o["poll_time_utc"] == pt]
                if poll_obs:
                    yes = poll_obs[0]["yes_price"]
                    print(f"  {yes:>7.0%}", end="")
                else:
                    print(f"  {'—':>8}", end="")
            print()

        # Find convergence time (when any bracket hit >90%)
        convergence_time = None
        for pt in poll_times:
            city_obs = [o for o in obs
                        if o["city"] == city and o["poll_time_utc"] == pt]
            if any(o["yes_price"] >= 0.90 for o in city_obs):
                convergence_time = pt[11:16]
                break

        print(f"\n  Converged at: {convergence_time or 'not yet'}")

        # Check if boundary buffer signals would have fired
        tradeable_windows = []
        for pt in poll_times:
            city_obs = [o for o in obs
                        if o["city"] == city and o["poll_time_utc"] == pt]
            fcst_low = city_obs[0].get("forecast_low_f") if city_obs else None
            obs_low  = city_obs[0].get("observed_low_f") if city_obs else None

            if fcst_low is None:
                continue

            for o in city_obs:
                yes = o["yes_price"]
                no  = o["no_price"]

                # Would our NO entry gates pass?
                if not (NO_MAX_YES_PRICE >= yes > 0.02):
                    continue
                if no > NO_MAX_ENTRY_PRICE:
                    continue

                # Rough bracket floor/cap from ticker
                bracket = o["bracket"]
                try:
                    if bracket.startswith("B"):
                        floor = float(bracket[1:]) - 1
                        cap   = float(bracket[1:])
                    elif bracket.startswith("T"):
                        floor = float(bracket[1:])
                        cap   = None
                    else:
                        continue
                except ValueError:
                    continue

                # Boundary buffer check
                if cap and abs(fcst_low - cap) < BOUNDARY_BUFFER:
                    continue
                if abs(fcst_low - floor) < BOUNDARY_BUFFER:
                    continue

                tradeable_windows.append({
                    "time":    pt[11:16],
                    "ticker":  o["ticker"],
                    "yes":     yes,
                    "no":      no,
                    "fcst_lo": fcst_low,
                    "obs_lo":  obs_low,
                })

        if tradeable_windows:
            print(f"\n  ✓ Potential NO signals found:")
            seen = set()
            for w in tradeable_windows:
                key = (w["time"], w["ticker"])
                if key not in seen:
                    seen.add(key)
                    print(f"    {w['time']}  {w['ticker'][-10:]:>12}  "
                          f"YES={w['yes']:.0%}  NO={w['no']:.2f}  "
                          f"fcst_lo={w['fcst_lo']}°")
        else:
            print(f"  ✗ No tradeable signals found (gates too strict or market converged early)")

    print(f"\n{'='*75}")
    print(f"  Summary: review the price evolution tables above.")
    print(f"  Key questions:")
    print(f"    1. Did any city have genuine uncertainty past midnight local?")
    print(f"    2. Were there NO signals before the market converged?")
    print(f"    3. Is the convergence pattern consistent across cities?")
    print(f"{'='*75}\n")


if __name__ == "__main__":
    analyze()
