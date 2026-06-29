"""
entry_window_analysis.py
------------------------
Analyses lowt_observations.csv (and optionally a trades CSV) to recommend
optimal entry windows per city and market type.

For each city + market type it computes per local-hour:
  - Convergence pressure   : how fast the leading bracket is climbing
  - Forecast stability     : how much the NWS forecast shifts during that hour
  - Signal availability    : how many brackets are in our NO entry range
  - NO price quality       : average NO price of tradeable brackets
  - Entry risk score       : composite flag combining the above

Output:
  - Per-city detailed report (console + CSV)
  - Cross-city summary table with recommended entry windows
  - Optional trade outcome overlay if trades CSV is provided

Usage:
  python entry_window_analysis.py
  python entry_window_analysis.py --trades data/trades.csv
  python entry_window_analysis.py --type high
  python entry_window_analysis.py --min-days 5   # skip cities with < N data-days
  python entry_window_analysis.py --city "New York" --verbose

Schema note
-----------
Handles both observation CSV schemas transparently:

  Old schema (lowt_observer.py before Apr 7 2026):
    observed_f, forecast_f   — generic field, meaning depends on market_type

  New schema (lowt_observer.py after Apr 7 2026 fix):
    observed_high_f, forecast_high_f  — for HIGH market rows
    observed_low_f,  forecast_low_f   — for LOWT market rows

Both are normalised to observed_f / forecast_f internally so all
downstream analysis code is unchanged.
"""

import csv
import argparse
from pathlib import Path
from collections import defaultdict
from datetime import datetime, timezone
from statistics import mean, stdev, median


# ---------------------------------------------------------------------------
# Parameters — match decision_engine thresholds
# ---------------------------------------------------------------------------

NO_MIN_YES_PRICE    = 0.02
NO_MAX_YES_PRICE    = 0.25
NO_MIN_ENTRY_PRICE  = 0.75
NO_MAX_ENTRY_PRICE  = 0.92

CONVERGENCE_THRESHOLD = 0.80   # leading YES above this = market has decided
RISKY_FORECAST_SWING  = 2.0    # °F — forecast change above this in one hour = unstable
                                # Note: this metric measures intra-day revision, not forecast
                                # quality. A stale overnight forecast scores 0 here because
                                # it hasn't changed — not because it's accurate. Use
                                # avg_forecast_age_h as the primary freshness gate instead.

STALE_FORECAST_HOURS  = 10.0   # hours — forecasts older than this at entry are unreliable.
                                # NWS morning update typically posts 06:00–08:00 local.
                                # A 10h threshold clears the ~07:00 update by 09:00 at worst.
                                # Requires forecast_issued_at field in observations (v2+ schema).

TRADE_WINDOW_FLOOR    = 9      # global earliest hour floor, matching hight_decision_engine.py.
                                # Even if forecast looks fresh and stable, don't recommend
                                # entry before 09:00 — obs signals are sparse and the morning
                                # NWS grid update may not have propagated to all city grids yet.

INPUT_OBS    = Path("data/lowt_observations.csv")
OUTPUT_CSV   = Path("data/entry_window_recommendations.csv")
OUTPUT_DAILY = Path("data/entry_window_daily.csv")


# ---------------------------------------------------------------------------
# Loader
# ---------------------------------------------------------------------------

def _normalise_obs_fields(r: dict) -> dict:
    """
    Normalise observed/forecast temperature fields to the canonical internal
    names (observed_f, forecast_f) regardless of which CSV schema the row
    came from.

    Old schema  : observed_f / forecast_f  (single generic field per row)
    New schema  : observed_high_f + forecast_high_f  for HIGH rows
                  observed_low_f  + forecast_low_f   for LOWT rows

    The new schema was introduced when lowt_observer.py was updated to write
    explicit field names per market type. Both schemas coexist in the same
    file during the transition period, so this function handles either.
    """
    mtype = r.get("market_type", "")

    # Detect new schema by presence of any of the explicit column names.
    # An empty string from the CSV counts as "present but null" — we still
    # treat it as new schema and let the float-parse below handle the null.
    has_new_schema = (
        "observed_high_f" in r or
        "observed_low_f"  in r or
        "forecast_high_f" in r or
        "forecast_low_f"  in r
    )

    if has_new_schema:
        if mtype == "high":
            raw_obs  = r.get("observed_high_f") or ""
            raw_fcst = r.get("forecast_high_f") or ""
        else:
            raw_obs  = r.get("observed_low_f")  or ""
            raw_fcst = r.get("forecast_low_f")  or ""
    else:
        # Old schema — fields already named observed_f / forecast_f
        raw_obs  = r.get("observed_f")  or ""
        raw_fcst = r.get("forecast_f")  or ""

    r["observed_f"] = float(raw_obs)  if raw_obs  else None
    r["forecast_f"] = float(raw_fcst) if raw_fcst else None
    return r


def load_observations(path: Path) -> list[dict]:
    rows = []
    with open(path, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            # Skip rows with no market type or no price
            if not r.get("market_type") or not r.get("yes_price"):
                continue
            try:
                r["yes_price"]  = float(r["yes_price"])
                r["no_price"]   = float(r["no_price"])  if r.get("no_price")  else 0.0
                r["local_hour"] = int(r["local_hour"])
                r["volume"]     = float(r["volume"])    if r.get("volume")    else 0.0
                r["date"]       = r["poll_time_utc"][:10]

                # Normalise observed/forecast fields — handles both schemas
                r = _normalise_obs_fields(r)

                rows.append(r)
            except (ValueError, KeyError):
                continue
    return rows


def load_trades(path: Path) -> list[dict]:
    if not path or not path.exists():
        return []
    rows = []
    with open(path, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            try:
                r["entry_price"] = float(r.get("entry_price") or 0)
                r["net_pnl"]     = float(r.get("net_pnl")     or 0)
                rows.append(r)
            except (ValueError, KeyError):
                continue
    return rows


# ---------------------------------------------------------------------------
# Core analysis
# ---------------------------------------------------------------------------

def analyse_city(
    city: str,
    market_type: str,
    obs: list[dict],
    trades: list[dict],
    min_days: int,
) -> dict | None:
    """
    Build a per-hour profile for one city + market_type combination.
    Returns a result dict, or None if insufficient data.
    """
    city_obs = [r for r in obs
                if r["city"] == city and r["market_type"] == market_type]
    if not city_obs:
        return None

    dates = sorted({r["date"] for r in city_obs})
    n_days = len(dates)
    if n_days < min_days:
        return None

    # ── Per-hour aggregation ──────────────────────────────────────────────
    # Group by (date, local_hour, poll_time_utc) to avoid double-counting
    # brackets from the same poll
    by_hour: dict[int, dict] = {}

    for h in range(24):
        hour_obs = [r for r in city_obs if r["local_hour"] == h]
        if not hour_obs:
            continue

        # Leading YES per poll (max yes_price across all brackets at that moment)
        polls = defaultdict(list)
        for r in hour_obs:
            polls[r["poll_time_utc"]].append(r)

        leading_yes_per_poll = []
        safe_no_per_poll     = []
        forecasts_per_poll   = []
        no_prices_tradeable  = []

        forecast_ages_per_poll = []

        for pt, pt_rows in polls.items():
            # Leading bracket
            max_yes = max(r["yes_price"] for r in pt_rows)
            leading_yes_per_poll.append(max_yes)

            # Tradeable NO brackets (within our entry criteria)
            safe = [
                r for r in pt_rows
                if NO_MIN_YES_PRICE < r["yes_price"] <= NO_MAX_YES_PRICE
                and NO_MIN_ENTRY_PRICE <= r["no_price"] <= NO_MAX_ENTRY_PRICE
            ]
            safe_no_per_poll.append(len(safe))
            no_prices_tradeable.extend(r["no_price"] for r in safe)

            # Forecast
            fcsts = [r["forecast_f"] for r in pt_rows if r["forecast_f"] is not None]
            if fcsts:
                forecasts_per_poll.append(mean(fcsts))

            # Forecast age — how old is the NWS grid at this poll?
            # forecast_issued_at is present in v2+ rows. Missing in old data → skip.
            issued_vals = [
                r.get("forecast_issued_at") for r in pt_rows
                if r.get("forecast_issued_at") and r.get("forecast_issued_at") != ""
            ]
            if issued_vals:
                try:
                    poll_dt   = datetime.fromisoformat(pt.replace("Z", "+00:00"))
                    issued_dt = datetime.fromisoformat(issued_vals[0].replace("Z", "+00:00"))
                    age_h = (poll_dt - issued_dt).total_seconds() / 3600
                    if 0 <= age_h < 72:   # sanity-check: ignore implausible values
                        forecast_ages_per_poll.append(age_h)
                except Exception:
                    pass

        # Forecast instability: mean intra-day forecast swing within this hour.
        #
        # Old approach (broken): stddev of forecast across ALL polls at this hour,
        # which mixes polls from different days — dominated by day-to-day temperature
        # variance (e.g. Chicago swings 40°F across April) and not by genuine NWS
        # model-run noise. Result: every hour flagged ⚠ for volatile cities.
        #
        # New approach: for each day separately, compute max-min of forecast values
        # observed at this local hour. Average those per-day swings across all days.
        # This measures "how much does the forecast revise within one hour of trading,
        # on a typical day?" — which is the actual risk we care about.
        # A swing >= RISKY_FORECAST_SWING°F on average means NWS is still actively
        # updating its model during this hour and entry prices are unreliable.
        daily_swings = []
        for date in dates:
            day_fcsts = []
            for pt, pt_rows in polls.items():
                if pt[:10] != date:
                    continue
                fcsts = [r["forecast_f"] for r in pt_rows if r["forecast_f"] is not None]
                if fcsts:
                    day_fcsts.append(mean(fcsts))
            if len(day_fcsts) >= 2:
                daily_swings.append(max(day_fcsts) - min(day_fcsts))
            elif len(day_fcsts) == 1:
                daily_swings.append(0.0)   # single poll — no swing evidence

        fcst_instability = round(mean(daily_swings), 2) if daily_swings else 0.0

        # Average forecast level (still uses cross-poll mean — fine for display)
        avg_forecast = mean(forecasts_per_poll) if forecasts_per_poll else None

        avg_forecast_age_h = round(mean(forecast_ages_per_poll), 1) if forecast_ages_per_poll else None
        forecast_stale     = (avg_forecast_age_h is not None
                              and avg_forecast_age_h >= STALE_FORECAST_HOURS)

        by_hour[h] = {
            "n_polls":             len(polls),
            "avg_leading_yes":     mean(leading_yes_per_poll),
            "max_leading_yes":     max(leading_yes_per_poll),
            "avg_safe_count":      mean(safe_no_per_poll),
            "avg_no_price":        mean(no_prices_tradeable) if no_prices_tradeable else 0.0,
            "n_tradeable_obs":     len(no_prices_tradeable),
            "fcst_instability":    round(fcst_instability, 2),
            "avg_forecast":        avg_forecast,
            "avg_forecast_age_h":  avg_forecast_age_h,   # None if forecast_issued_at not in data
            "forecast_stale":      forecast_stale,
        }

    # ── Convergence hour: first hour where avg leading YES > threshold ────
    convergence_hour = None
    for h in sorted(by_hour.keys()):
        if by_hour[h]["avg_leading_yes"] >= CONVERGENCE_THRESHOLD:
            convergence_hour = h
            break

    # ── Forecast instability: hours where forecast swings ≥ threshold ────
    unstable_hours = [
        h for h, d in by_hour.items()
        if d["fcst_instability"] >= RISKY_FORECAST_SWING
    ]

    # ── Trade outcomes overlay ────────────────────────────────────────────
    city_trades = [
        t for t in trades
        if t.get("city", "").lower() == city.lower()
        and t.get("side", "").lower() == "no"
    ]

    trade_by_hour: dict[int, list] = defaultdict(list)
    for t in city_trades:
        entry_time = t.get("entry_time_utc", "")
        if not entry_time:
            continue
        try:
            dt = datetime.fromisoformat(entry_time.replace("Z", "+00:00"))
        except ValueError:
            continue
        # Convert UTC to approximate local hour using timezone offset
        # (rough — good enough for hour-level analysis)
        tz_offsets = {
            "America/New_York":    -4,  # EDT
            "America/Chicago":     -5,  # CDT
            "America/Denver":      -6,  # MDT
            "America/Los_Angeles": -7,  # PDT
            "America/Phoenix":     -7,  # MST
        }
        # Find city timezone from observations
        city_tz_offsets = list({
            r.get("local_hour", 0) - dt.hour
            for r in city_obs
            if r["poll_time_utc"][:13] == entry_time[:13]
        })
        local_h = (dt.hour + (city_tz_offsets[0] if city_tz_offsets else 0)) % 24
        trade_by_hour[local_h].append(float(t.get("net_pnl", 0)))

    trade_hour_summary = {}
    for h, pnls in trade_by_hour.items():
        wins = sum(1 for p in pnls if p > 0)
        trade_hour_summary[h] = {
            "n_trades":  len(pnls),
            "win_rate":  wins / len(pnls) if pnls else 0,
            "avg_pnl":   mean(pnls),
        }

    # ── Recommended entry window ──────────────────────────────────────────
    # Logic: start = first stable hour at or after TRADE_WINDOW_FLOOR where
    #        signals are consistently available and forecast is fresh.
    #        end   = one hour before convergence (market is deciding).
    #
    # Forecast freshness: prefer hours where avg_forecast_age_h < STALE_FORECAST_HOURS.
    # When forecast_issued_at is absent from the data (old rows), forecast_stale=False
    # for all hours — the filter is a no-op and we fall back to the instability metric.

    candidate_hours = [
        h for h in sorted(by_hour.keys())
        if by_hour[h]["fcst_instability"] < RISKY_FORECAST_SWING
        and not by_hour[h]["forecast_stale"]
        and by_hour[h]["avg_safe_count"] >= 1
        and by_hour[h]["avg_leading_yes"] < CONVERGENCE_THRESHOLD
        and TRADE_WINDOW_FLOOR <= h <= 20   # respect global engine floor; cap at 20:00
    ]

    # Non-converging cities: convergence_hour is None, meaning the market never
    # reliably decides intraday. Don't recommend entry — these cities need manual review.
    if convergence_hour is None:
        rec_start = None
        rec_end   = None
    else:
        rec_start = candidate_hours[0]  if candidate_hours else None
        rec_end   = (convergence_hour - 1) if convergence_hour is not None else None
        if rec_end is not None and rec_start is not None and rec_end <= rec_start:
            rec_end = rec_start + 2   # minimum 2-hour window

    return {
        "city":              city,
        "market_type":       market_type,
        "n_days":            n_days,
        "dates":             dates,
        "by_hour":           by_hour,
        "convergence_hour":  convergence_hour,
        "unstable_hours":    unstable_hours,
        "rec_start":         rec_start,
        "rec_end":           rec_end,
        "trade_by_hour":     trade_hour_summary,
        "n_trades":          len(city_trades),
    }


# ---------------------------------------------------------------------------
# Display helpers
# ---------------------------------------------------------------------------

def fmt_hour(h: int | None) -> str:
    return f"{h:02d}:00" if h is not None else "  N/A"


def risk_flag(hour_data: dict, unstable_hours: list) -> str:
    h = hour_data
    flags = []
    if h["forecast_stale"]:
        flags.append("⚠ stale")
    elif h["fcst_instability"] >= RISKY_FORECAST_SWING:
        flags.append("⚠ fcst")
    if h["avg_leading_yes"] >= CONVERGENCE_THRESHOLD:
        flags.append("✗ conv")
    elif h["avg_leading_yes"] >= 0.60:
        flags.append("~ conv")
    if h["avg_safe_count"] < 0.5:
        flags.append("✗ sig")
    return "  ".join(flags) if flags else "✓ ok"


def print_city_report(result: dict):
    city      = result["city"]
    mtype     = result["market_type"].upper()
    n_days    = result["n_days"]
    by_hour   = result["by_hour"]
    conv_hr   = result["convergence_hour"]
    unstable  = result["unstable_hours"]
    rec_start = result["rec_start"]
    rec_end   = result["rec_end"]
    trades    = result["trade_by_hour"]

    print(f"\n{'─'*80}")
    print(f"  {city}  [{mtype}]  —  {n_days} day(s) of data")
    print(f"{'─'*80}")
    print(f"  Convergence hour   : {fmt_hour(conv_hr)}")
    print(f"  Unstable fcst hrs  : {[fmt_hour(h) for h in unstable] or 'none'}")
    print(f"  Recommended window : {fmt_hour(rec_start)} – {fmt_hour(rec_end)}")
    if n_days < 5:
        print(f"  ⚠  LOW CONFIDENCE — only {n_days} day(s). Collect ≥5 days before relying on this.")

    has_ages  = any(d.get("avg_forecast_age_h") is not None for d in by_hour.values())
    has_trades = bool(trades)
    age_col    = "  FcstAge" if has_ages else ""
    trade_cols = "  Trades  WinRate  AvgPnL" if has_trades else ""

    if conv_hr is None:
        print(f"  ⚠  NON-CONVERGING — no entry window recommended. Monitor only.")

    print()
    print(f"  {'Hr':>3}  {'LeadYES':>8}  {'SafeSig':>7}  {'AvgNO':>6}  {'FcstSd':>7}  {'Status':<18}{age_col}{trade_cols}")
    print(f"  {'─'*3}  {'─'*8}  {'─'*7}  {'─'*6}  {'─'*7}  {'─'*18}", end="")
    if has_ages:
        print(f"  {'─'*7}", end="")
    if has_trades:
        print(f"  {'─'*6}  {'─'*7}  {'─'*6}", end="")
    print()

    for h in sorted(by_hour.keys()):
        d      = by_hour[h]
        status = risk_flag(d, unstable)
        in_rec = (rec_start is not None and rec_end is not None
                  and rec_start <= h <= rec_end)
        marker = "►" if in_rec else " "

        age_str = ""
        if has_ages:
            age_val = d.get("avg_forecast_age_h")
            age_str = f"  {age_val:>6.1f}h" if age_val is not None else "       N/A"

        line = (f"{marker} {h:02d}  "
                f"{d['avg_leading_yes']:>8.0%}  "
                f"{d['avg_safe_count']:>7.1f}  "
                f"{d['avg_no_price']:>6.2f}  "
                f"{d['fcst_instability']:>7.2f}  "
                f"{status:<18}"
                f"{age_str}")

        if has_trades and h in trades:
            t = trades[h]
            line += (f"  {t['n_trades']:>6}  "
                     f"{t['win_rate']:>7.0%}  "
                     f"${t['avg_pnl']:>+5.2f}")
        print(line)


def print_summary_table(results: list[dict]):
    print(f"\n{'='*80}")
    print(f"  ENTRY WINDOW RECOMMENDATIONS  —  CROSS-CITY SUMMARY")
    print(f"{'='*80}")

    data_warn = any(r["n_days"] < 5 for r in results)
    if data_warn:
        print(f"  ⚠  Cities marked * have < 5 days of data. Treat as indicative only.")

    print()
    print(f"  {'City':<16} {'Type':>5} {'Days':>5}  "
          f"{'Rec Start':>9}  {'Rec End':>7}  {'Conv Hr':>7}  "
          f"{'Trades':>7}  Notes")
    print(f"  {'─'*16} {'─'*5} {'─'*5}  {'─'*9}  {'─'*7}  {'─'*7}  {'─'*7}  {'─'*20}")

    for r in sorted(results, key=lambda x: (x["city"], x["market_type"])):
        warn   = "*" if r["n_days"] < 5 else " "
        notes  = []
        if r["convergence_hour"] is None:
            notes.append("NON-CONVERGING — monitor only")
        else:
            if r["unstable_hours"]:
                notes.append(f"fcst unstable {[fmt_hour(h) for h in r['unstable_hours'][:2]]}")
            if r["convergence_hour"] is not None and r["convergence_hour"] <= 11:
                notes.append("early convergence")

        print(f"  {r['city']:<16}{warn} {r['market_type'].upper():>5} "
              f"{r['n_days']:>5}  "
              f"{fmt_hour(r['rec_start']):>9}  "
              f"{fmt_hour(r['rec_end']):>7}  "
              f"{fmt_hour(r['convergence_hour']):>7}  "
              f"{r['n_trades']:>7}  "
              f"{'; '.join(notes)[:40]}")

    print(f"\n  ► = hour is within recommended window")
    print(f"  ⚠ stale = avg forecast age ≥{STALE_FORECAST_HOURS:.0f}h at this hour (pre-morning-update data)")
    print(f"  ⚠ fcst  = avg intra-day forecast swing ≥{RISKY_FORECAST_SWING}°F (NWS actively revising)")
    print(f"  ✗ conv  = leading bracket already ≥{CONVERGENCE_THRESHOLD:.0%} YES (market decided)")
    print(f"  ✗ sig   = < 1 tradeable NO signal on average")
    print(f"  Window floor: {TRADE_WINDOW_FLOOR:02d}:00 local (matches hight_decision_engine.py TRADE_WINDOW_START)")
    print()


def write_recommendations_csv(results: list[dict], path: Path):
    rows = []
    for r in results:
        rows.append({
            "city":             r["city"],
            "market_type":      r["market_type"],
            "n_days":           r["n_days"],
            "rec_start_hour":   r["rec_start"],
            "rec_end_hour":     r["rec_end"],
            "convergence_hour": r["convergence_hour"],
            "unstable_hours":   str(r["unstable_hours"]),
            "n_trades":         r["n_trades"],
            "low_confidence":   r["n_days"] < 5,
        })
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    print(f"  Recommendations saved → {path}")


def write_daily_csv(results: list[dict], path: Path):
    """Write per-hour detail for all cities to a flat CSV for further analysis."""
    rows = []
    for r in results:
        for h, d in r["by_hour"].items():
            in_rec = (r["rec_start"] is not None and r["rec_end"] is not None
                      and r["rec_start"] <= h <= r["rec_end"])
            rows.append({
                "city":             r["city"],
                "market_type":      r["market_type"],
                "local_hour":       h,
                "avg_leading_yes":  round(d["avg_leading_yes"], 3),
                "avg_safe_count":   round(d["avg_safe_count"], 2),
                "avg_no_price":     round(d["avg_no_price"], 4),
                "fcst_instability": d["fcst_instability"],
                "n_polls":          d["n_polls"],
                "in_rec_window":    in_rec,
                "convergence_hour": r["convergence_hour"],
            })
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    print(f"  Per-hour detail saved → {path}")


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Entry window analysis for Kalshi temperature markets"
    )
    parser.add_argument("--obs",      type=Path, default=INPUT_OBS,
                        help=f"Path to lowt_observations.csv (default: {INPUT_OBS})")
    parser.add_argument("--trades",   type=Path, default=None,
                        help="Path to trades CSV (optional — overlays win rates per hour)")
    parser.add_argument("--type",     choices=["high", "lowt", "both"], default="both",
                        help="Market type to analyse (default: both)")
    parser.add_argument("--min-days", type=int, default=1,
                        help="Minimum data-days required per city (default: 1)")
    parser.add_argument("--city",     type=str, default=None,
                        help="Filter to one city (e.g. 'New York')")
    parser.add_argument("--verbose",  action="store_true",
                        help="Print per-hour detail for every city")
    args = parser.parse_args()

    if not args.obs.exists():
        raise SystemExit(f"Observations file not found: {args.obs}")

    print(f"\nLoading observations from {args.obs}...")
    obs    = load_observations(args.obs)
    trades = load_trades(args.trades) if args.trades else []

    # Report schema mix so the user knows what's in the file
    new_schema = sum(1 for r in obs if r.get("observed_f") is not None
                     and ("observed_high_f" in r or "observed_low_f" in r
                          or r.get("observed_f") is not None))
    print(f"  {len(obs):,} rows loaded.")

    cities = sorted({r["city"] for r in obs})
    if args.city:
        cities = [c for c in cities if c.lower() == args.city.lower()]

    types = []
    if args.type in ("high", "both"):
        types.append("high")
    if args.type in ("lowt", "both"):
        types.append("lowt")

    print(f"  Analysing {len(cities)} cities × {len(types)} market type(s)...\n")

    results = []
    for city in cities:
        for mtype in types:
            r = analyse_city(city, mtype, obs, trades, args.min_days)
            if r:
                results.append(r)

    if not results:
        print("No results — try lowering --min-days.")
        return

    # ── Detailed per-city output ──────────────────────────────────────────
    if args.verbose or args.city:
        for r in results:
            print_city_report(r)

    # ── Summary table ─────────────────────────────────────────────────────
    print_summary_table(results)

    # ── Data sufficiency note ─────────────────────────────────────────────
    days_counts = sorted({r["n_days"] for r in results})
    print(f"  Data coverage: {min(days_counts)}–{max(days_counts)} days per city.")
    print(f"  Recommendation reliability improves significantly at ≥5 days.")
    print(f"  At ≥15 days, hour-level win rates become statistically meaningful.")
    print()

    # ── CSV outputs ───────────────────────────────────────────────────────
    write_recommendations_csv(results, OUTPUT_CSV)
    write_daily_csv(results, OUTPUT_DAILY)

    # ── Quick wins: cities where early entry is clearly risky ─────────────
    print(f"\n  IMMEDIATE ACTIONS (based on current data):")
    print(f"  {'─'*60}")
    risky_early = [
        r for r in results
        if r["rec_start"] is not None and r["rec_start"] >= 10
        and r["market_type"] == "high"
    ]
    for r in risky_early:
        print(f"  {r['city']:<16} HIGH: don't enter before {fmt_hour(r['rec_start'])} local")

    stable_early = [
        r for r in results
        if r["rec_start"] is not None and r["rec_start"] <= 8
        and r["market_type"] == "high"
        and not r["unstable_hours"]
    ]
    for r in stable_early:
        print(f"  {r['city']:<16} HIGH: early entry ({fmt_hour(r['rec_start'])}) looks stable")
    print()


if __name__ == "__main__":
    main()
