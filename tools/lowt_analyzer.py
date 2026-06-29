"""
lowt_analyzer.py
----------------
Review of temperature market observations collected by lowt_observer.py.
Analyzes both HIGH and LOWT markets.

  python lowt_analyzer.py              # analyze all data
  python lowt_analyzer.py --type high  # only high temperature markets
  python lowt_analyzer.py --type lowt  # only low temperature markets

Schema note
-----------
Handles both v1 rows (observed_f / forecast_f) and v2 rows
(observed_high_f / forecast_high_f / observed_low_f / forecast_low_f).
normalize_row() is applied at load time so the rest of the analysis
always sees the v2 field names regardless of when the row was recorded.
"""

import json
import argparse
from pathlib import Path
from collections import defaultdict

BOUNDARY_BUFFER    = 3.0
NO_MAX_YES_PRICE   = 0.25
NO_MAX_ENTRY_PRICE = 0.87

OUTPUT_JSON = Path("data/lowt_observations.json")

# ---------------------------------------------------------------------------
# Schema normalisation
# ---------------------------------------------------------------------------

def normalize_row(row: dict) -> dict:
    """
    Upgrade a v1 row (observed_f / forecast_f) to the v2 schema
    (observed_high_f / forecast_high_f / observed_low_f / forecast_low_f).

    v2 rows are returned unchanged. A new dict is returned — the original
    is never mutated.

    v1 mapping logic:
      market_type == "high"  → observed_f  → observed_high_f
                                forecast_f  → forecast_high_f
                                observed_low_f / forecast_low_f = None
      market_type == "lowt"  → observed_f  → observed_low_f
                                forecast_f  → forecast_low_f
                                observed_high_f / forecast_high_f = None
    """
    if "observed_high_f" in row or "observed_low_f" in row:
        if "forecast_issued_at" not in row:
            row = {**row, "forecast_issued_at": None}
        return row

    market_type = row.get("market_type", "high")
    obs_f  = row.get("observed_f")
    fcst_f = row.get("forecast_f")

    upgraded = {**row, "forecast_issued_at": None}

    if market_type == "high":
        upgraded["observed_high_f"] = obs_f
        upgraded["forecast_high_f"] = fcst_f
        upgraded["observed_low_f"]  = None
        upgraded["forecast_low_f"]  = None
    else:
        upgraded["observed_high_f"] = None
        upgraded["forecast_high_f"] = None
        upgraded["observed_low_f"]  = obs_f
        upgraded["forecast_low_f"]  = fcst_f

    upgraded.pop("observed_f", None)
    upgraded.pop("forecast_f", None)

    return upgraded


# ---------------------------------------------------------------------------
# Load
# ---------------------------------------------------------------------------

def load() -> list[dict]:
    if not OUTPUT_JSON.exists():
        print("No observation file found. Run lowt_observer.py first.")
        return []
    raw = json.loads(OUTPUT_JSON.read_text())
    return [normalize_row(r) for r in raw]


# ---------------------------------------------------------------------------
# Analysis
# ---------------------------------------------------------------------------

def analyze(market_filter: str = None):
    obs = load()
    if not obs:
        return

    if market_filter:
        obs = [o for o in obs if o.get("market_type") == market_filter]

    print("=" * 75)
    print(f"  Temperature Market Analysis  —  {len(obs)} observations")
    if market_filter:
        print(f"  Market type filter: {market_filter.upper()}")
    print("=" * 75)

    by_type_city = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    for o in obs:
        by_type_city[o.get("market_type", "?")][o["city"]][o["ticker"]].append(o)

    for market_type in sorted(by_type_city.keys()):
        print(f"\n{'#'*75}")
        print(f"  {'HIGH TEMPERATURE' if market_type == 'high' else 'LOW TEMPERATURE'} MARKETS")
        print(f"{'#'*75}")

        by_city = by_type_city[market_type]

        for city in sorted(by_city.keys()):
            tickers = by_city[city]
            print(f"\n{'─'*75}")
            print(f"  {city.upper()}")
            print(f"{'─'*75}")

            poll_times = sorted(set(
                o["poll_time_utc"]
                for ticker_obs in tickers.values()
                for o in ticker_obs
            ))

            all_tickers    = sorted(tickers.keys())
            bracket_labels = [t.split("-")[-1] for t in all_tickers]

            print(f"\n  Price evolution (YES%):")
            print(f"  {'Time':>8}", end="")
            for b in bracket_labels:
                print(f"  {b:>8}", end="")
            print()

            for pt in poll_times:
                time_str = pt[11:16]
                print(f"  {time_str:>8}", end="")
                for t in all_tickers:
                    pt_obs = [o for o in tickers[t] if o["poll_time_utc"] == pt]
                    if pt_obs:
                        yes = pt_obs[0]["yes_price"]
                        print(f"  {yes:>7.0%}", end="")
                    else:
                        print(f"  {'—':>8}", end="")
                print()

            # Convergence detection (>90%)
            convergence_time = None
            for pt in poll_times:
                pt_obs = [
                    o for ticker_obs in tickers.values()
                    for o in ticker_obs
                    if o["poll_time_utc"] == pt
                ]
                if any(o["yes_price"] >= 0.90 for o in pt_obs):
                    convergence_time = pt[11:16]
                    break

            print(f"\n  Converged at: {convergence_time or 'not yet'}")

            # Tradeable signal detection
            # Use explicit v2 field names — normalize_row guarantees they exist.
            obs_key  = "observed_high_f"  if market_type == "high" else "observed_low_f"
            fcst_key = "forecast_high_f"  if market_type == "high" else "forecast_low_f"

            tradeable = []
            for pt in poll_times:
                pt_obs = [
                    o for ticker_obs in tickers.values()
                    for o in ticker_obs
                    if o["poll_time_utc"] == pt
                ]

                fcst_f = pt_obs[0].get(fcst_key) if pt_obs else None
                obs_f  = pt_obs[0].get(obs_key)  if pt_obs else None

                ref_f = fcst_f or obs_f
                if ref_f is None:
                    continue

                for o in pt_obs:
                    yes = o["yes_price"]
                    no  = o["no_price"]

                    if not (NO_MAX_YES_PRICE >= yes > 0.02):
                        continue
                    if no > NO_MAX_ENTRY_PRICE:
                        continue

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

                    if cap and abs(ref_f - cap) < BOUNDARY_BUFFER:
                        continue
                    if abs(ref_f - floor) < BOUNDARY_BUFFER:
                        continue

                    tradeable.append({
                        "time":   pt[11:16],
                        "ticker": o["ticker"],
                        "yes":    yes,
                        "no":     no,
                        "ref_f":  ref_f,
                        "obs_f":  obs_f,
                    })

            if tradeable:
                print(f"\n  ✓ Potential NO signals:")
                seen = set()
                for w in tradeable:
                    key = (w["time"], w["ticker"])
                    if key not in seen:
                        seen.add(key)
                        print(f"    {w['time']}  {w['ticker'][-12:]:>14}  "
                              f"YES={w['yes']:.0%}  NO={w['no']:.2f}  "
                              f"ref={w['ref_f']}°  obs={w['obs_f']}°")
            else:
                print(f"  ✗ No tradeable signals found")

    print(f"\n{'='*75}")
    print(f"  Key questions:")
    print(f"    1. When does each market converge?")
    print(f"    2. Are there NO signals before convergence?")
    print(f"    3. Does HIGH converge differently from LOWT?")
    print(f"{'='*75}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--type", choices=["high", "lowt"],
                        help="Filter to one market type")
    parser.add_argument("--output", default="data/analysis_output.txt",
                        help="Output file path (default: data/analysis_output.txt)")
    args = parser.parse_args()

    out_path = Path(args.output)
    out_path.parent.mkdir(exist_ok=True)

    import sys
    original_stdout = sys.stdout
    with open(out_path, "w", encoding="utf-8") as f:
        sys.stdout = f
        analyze(market_filter=args.type)
    sys.stdout = original_stdout

    print(f"Analysis written to: {out_path}")
    print(f"Open with: notepad {out_path}  (or your editor of choice)")
