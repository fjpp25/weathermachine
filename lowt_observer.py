"""
lowt_observer.py
----------------
Passive observer for Kalshi temperature markets — both HIGH and LOWT.
No trading — read-only. Polls every 15 minutes and records bracket
prices alongside NWS observed temperatures and forecasts.

Run before bed or throughout the day:
  python lowt_observer.py

Output: data/lowt_observations.json
        data/lowt_observations.csv

Runs continuously — Ctrl+C to stop.
Auto-switches to new day's markets at UTC midnight.

Schema note
-----------
New rows (v2) use explicit per-type field names:
  HIGH rows  → observed_high_f, forecast_high_f  (observed_low_f / forecast_low_f = null)
  LOWT rows  → observed_low_f,  forecast_low_f   (observed_high_f / forecast_high_f = null)

Old rows (v1) used a single pair of fields regardless of market_type:
  observed_f, forecast_f

normalize_row() is applied at load time to upgrade v1 rows to the v2
schema so all downstream consumers always see the same field names.
New rows are passed through unchanged.

forecast_issued_at records when the NWS forecast office last issued the
grid at the time of each poll. Allows computing forecast_age_hours at
analysis time to stratify forecast error by forecast freshness.
"""

import json
import time
import csv
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

try:
    import requests
except ImportError:
    raise SystemExit("Please install requests:  pip install requests")

from cities import OBSERVE_CITIES as CITIES
import nws_feed

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

POLL_INTERVAL_SECS = 15 * 60   # 15 minutes

OUTPUT_JSON = Path("data/lowt_observations.json")
OUTPUT_CSV  = Path("data/lowt_observations.csv")
OUTPUT_DB   = Path("data/observations.db")

# Dual-write safety flag.
#   True  → write new rows to the DB AND keep rewriting the old JSON/CSV, so the
#           two can be compared for parity during cutover validation.
#   False → DB only (the destination state). The giant JSON/CSV rewrites stop;
#           the evaluation CSV is then produced from the DB by export_csv.py.
# Flip to False once parity is confirmed.
DUAL_WRITE = True

# Column order for the SQLite table — matches migrate_observations_to_sqlite.py.
# current_temp_f is included (the live writer collects it; the old CSV_FIELDS
# dropped it, which also broke the dashboard's d.get("current_temp_f")).
DB_COLUMNS = [
    "poll_time_utc", "city", "market_type", "local_time", "local_hour",
    "observed_high_f", "forecast_high_f",
    "observed_low_f",  "forecast_low_f",
    "forecast_issued_at", "hazards",
    "ticker", "bracket", "yes_price", "no_price",
    "spread", "volume", "open_interest",
    "current_temp_f",
]

CSV_FIELDS = [
    "poll_time_utc", "city", "market_type", "local_time", "local_hour",
    "observed_high_f", "forecast_high_f",
    "observed_low_f",  "forecast_low_f",
    "forecast_issued_at", "hazards",
    "ticker", "bracket", "yes_price", "no_price",
    "spread", "volume", "open_interest",
    "current_temp_f",
]

# ---------------------------------------------------------------------------
# Schema normalisation
# ---------------------------------------------------------------------------

def normalize_row(row: dict) -> dict:
    """
    Upgrade a v1 row (observed_f / forecast_f) to the v2 schema
    (observed_high_f / forecast_high_f / observed_low_f / forecast_low_f).

    v2 rows are returned unchanged. The original row dict is not mutated —
    a new dict is returned so callers can safely modify it.

    v1 mapping logic:
      market_type == "high"  → observed_f  → observed_high_f
                                forecast_f  → forecast_high_f
                                observed_low_f / forecast_low_f = None
      market_type == "lowt"  → observed_f  → observed_low_f
                                forecast_f  → forecast_low_f
                                observed_high_f / forecast_high_f = None
    """
    # Already v2 — has at least one of the explicit fields present as a key
    if "observed_high_f" in row or "observed_low_f" in row:
        # Ensure newer fields exist even if absent (older v2 rows)
        if "forecast_issued_at" not in row:
            row = {**row, "forecast_issued_at": None}
        if "hazards" not in row:
            row = {**row, "hazards": []}
        return row

    # v1 row — promote observed_f / forecast_f to explicit fields
    market_type = row.get("market_type", "high")
    obs_f  = row.get("observed_f")
    fcst_f = row.get("forecast_f")

    upgraded = {**row, "forecast_issued_at": None, "hazards": []}

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

    # Remove the old keys so consumers never see the ambiguous fields
    upgraded.pop("observed_f", None)
    upgraded.pop("forecast_f", None)

    return upgraded


# ---------------------------------------------------------------------------
# Kalshi data fetching
# ---------------------------------------------------------------------------

def fetch_brackets(series: str) -> list[dict]:
    """Fetch today's open brackets for a given series from Kalshi."""
    today = datetime.now(timezone.utc).strftime("%y%b%d").upper()
    try:
        resp = requests.get(
            "https://api.elections.kalshi.com/trade-api/v2/markets",
            params={"series_ticker": series, "status": "open"},
            timeout=10,
        )
        markets = resp.json().get("markets", [])
        return [m for m in markets if today in m.get("ticker", "").upper()]
    except Exception as e:
        print(f"  Kalshi error for {series}: {e}")
        return []


def fetch_tomorrows_brackets(series: str) -> list[dict]:
    """
    Fetch tomorrow's open brackets for a given series from Kalshi.
    Called when today's market has converged — tomorrow's market has
    been running since ~10am ET and already has meaningful price discovery.
    """
    from datetime import timedelta
    tomorrow = (datetime.now(timezone.utc) + timedelta(days=1)).strftime("%y%b%d").upper()
    try:
        resp = requests.get(
            "https://api.elections.kalshi.com/trade-api/v2/markets",
            params={"series_ticker": series, "status": "open"},
            timeout=10,
        )
        markets = resp.json().get("markets", [])
        return [m for m in markets if tomorrow in m.get("ticker", "").upper()]
    except Exception as e:
        print(f"  Kalshi error (tomorrow) for {series}: {e}")
        return []


# Convergence threshold — same value as kalshi_scanner.CONVERGENCE_THRESHOLD.
# When all brackets have max(yes_bid, no_bid) >= this, today's market is
# effectively settled and tomorrow's market is worth observing.
CONVERGENCE_THRESHOLD = 0.97


def _is_converged(brackets: list[dict]) -> bool:
    """
    Return True if every bracket has either YES or NO at >= CONVERGENCE_THRESHOLD.
    Requires at least 4 brackets to guard against incomplete data.
    """
    if len(brackets) < 4:
        return False
    for m in brackets:
        yes = float(m.get("yes_bid_dollars") or 0)
        no  = float(m.get("no_bid_dollars")  or 0)
        if max(yes, no) < CONVERGENCE_THRESHOLD:
            return False
    return True


# ---------------------------------------------------------------------------
# Observation recorder
# ---------------------------------------------------------------------------

def load_observations() -> list[dict]:
    """
    Load observations into memory.

    Only needed while DUAL_WRITE is on (the JSON/CSV rewrite needs full history
    in memory). Once DB-only, poll_once just appends to the DB and never reads
    history, so we start empty — avoiding the multi-GB JSON deserialize on every
    startup. (poll_once only appends to this list; it never reads it.)
    """
    if not DUAL_WRITE:
        return []
    if OUTPUT_JSON.exists():
        try:
            raw = json.loads(OUTPUT_JSON.read_text())
            return [normalize_row(r) for r in raw]
        except Exception:
            return []
    return []


def _hazards_to_str(h) -> str:
    """Serialize hazards as a pipe-joined, comma-free string (CSV/DB-safe)."""
    if h is None or h == "":
        return ""
    if isinstance(h, (list, tuple)):
        return "|".join(str(x) for x in h)
    return str(h)


def _ensure_db():
    """Create the observations table if the DB doesn't have it yet."""
    OUTPUT_DB.parent.mkdir(exist_ok=True)
    con = sqlite3.connect(OUTPUT_DB)
    cols_ddl = ", ".join(f"{c} {'REAL' if c in _REAL_COLS else 'TEXT'}"
                         for c in DB_COLUMNS)
    con.execute(f"CREATE TABLE IF NOT EXISTS observations ({cols_ddl})")
    con.commit()
    return con


_REAL_COLS = {
    "local_hour", "observed_high_f", "forecast_high_f",
    "observed_low_f", "forecast_low_f",
    "yes_price", "no_price", "spread", "volume", "open_interest",
    "current_temp_f",
}


def _row_to_db_tuple(row: dict) -> tuple:
    """Build an insert tuple in DB_COLUMNS order, hazards pipe-joined."""
    out = []
    for c in DB_COLUMNS:
        if c == "hazards":
            out.append(_hazards_to_str(row.get("hazards")))
        else:
            out.append(row.get(c))
    return tuple(out)


def append_observations_to_db(new_rows: list[dict]) -> int:
    """INSERT this poll's new rows into the DB. Cheap: no rewrite of history."""
    if not new_rows:
        return 0
    con = _ensure_db()
    placeholders = ", ".join(["?"] * len(DB_COLUMNS))
    sql = (f"INSERT INTO observations ({', '.join(DB_COLUMNS)}) "
           f"VALUES ({placeholders})")
    con.executemany(sql, [_row_to_db_tuple(r) for r in new_rows])
    con.commit()
    con.close()
    return len(new_rows)


def save_observations(obs: list[dict], added: int = 0):
    """
    Persist observations.

    New permanent path: append only this poll's `added` new rows (the last
    `added` entries of `obs`) to the SQLite DB — no full-history rewrite.

    During DUAL_WRITE, ALSO rewrite the old JSON + CSV in full, so the DB can
    be compared against them for parity. Once parity is confirmed, set
    DUAL_WRITE = False and the giant rewrites stop; the evaluation CSV is then
    produced from the DB by export_csv.py.
    """
    new_rows = obs[-added:] if added > 0 else []
    append_observations_to_db(new_rows)

    if DUAL_WRITE:
        OUTPUT_JSON.parent.mkdir(exist_ok=True)
        OUTPUT_JSON.write_text(json.dumps(obs, indent=2))
        with open(OUTPUT_CSV, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_FIELDS,
                                    extrasaction="ignore")
            writer.writeheader()
            writer.writerows(obs)


def poll_once(observations: list[dict]) -> int:
    """
    Run one poll cycle. Returns number of bracket rows recorded.

    NWS data is fetched once per city via nws_feed.snapshot(), which:
      - uses the shared CITIES registry (no duplication)
      - applies LST boundary logic correctly (not a 24-hr rolling window)
      - benefits from retry logic and the grid cache

    Tomorrow's brackets are fetched on every poll for both HIGH and LOWT —
    not just after today converges. The dismissed-T signal window is brief
    (1–3 polls) and appears throughout the day, so gating on convergence
    was causing most of the actionable data to be missed.
    """
    poll_time  = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    rows_added = 0

    # Single NWS pass for all cities — reuse results across HIGH and LOWT loops.
    print(f"\n[{poll_time}] Fetching NWS data for {len(CITIES)} cities...")
    nws_results = nws_feed.snapshot()

    for city, cfg in CITIES.items():
        tz         = ZoneInfo(cfg["tz"])
        local_now  = datetime.now(tz)
        local_time = local_now.strftime("%H:%M %Z")
        local_hour = local_now.hour

        # Pull pre-fetched NWS data — no extra HTTP calls per city.
        nws = nws_results.get(city, {})

        # forecast_issued_at and hazards are shared across HIGH and LOWT for
        # the same city since both use the same NWS grid.
        forecast_issued_at = nws.get("forecast_issued_at")
        hazards            = nws.get("hazards", [])

        # Cache fetched brackets so the summary section reuses them
        # without making redundant API calls (was 2 extra fetches per city).
        fetched_today    = {}   # market_type -> list[dict]
        fetched_tomorrow = {}   # market_type -> list[dict]

        for market_type in ("high", "lowt"):
            series = cfg.get(f"{market_type}_series") or cfg.get(market_type)
            if not series:
                continue

            brackets = fetch_brackets(series)
            fetched_today[market_type] = brackets
            if not brackets:
                continue

            # Use explicit per-type field names so rows are self-documenting.
            # Null out the other market type's fields for schema consistency.
            if market_type == "high":
                observed_high_f = nws.get("observed_high_f")
                forecast_high_f = nws.get("forecast_high_f")
                observed_low_f  = None
                forecast_low_f  = None
                if observed_high_f is None:
                    print(f"  [WARN] {city} HIGH: observed_high_f is None "
                          f"(NWS error: {nws.get('error')})")
            else:
                observed_high_f = None
                forecast_high_f = None
                observed_low_f  = nws.get("observed_low_f")
                forecast_low_f  = nws.get("forecast_low_f")
                if observed_low_f is None:
                    print(f"  [WARN] {city} LOWT: observed_low_f is None "
                          f"(NWS error: {nws.get('error')})")

            for m in brackets:
                ticker        = m.get("ticker", "")
                yes_price     = float(m.get("yes_bid_dollars") or 0)
                no_price      = float(m.get("no_bid_dollars")  or 0)
                volume        = float(m.get("volume_fp") or 0)
                open_interest = float(m.get("open_interest_fp") or 0)
                bracket       = ticker.split("-")[-1] if "-" in ticker else ticker
                yes_ask       = round(1.0 - no_price, 4) if no_price > 0 else None
                spread        = round(yes_ask - yes_price, 4) if yes_ask and yes_price > 0 else None

                observations.append({
                    "poll_time_utc":      poll_time,
                    "city":               city,
                    "market_type":        market_type,
                    "local_time":         local_time,
                    "local_hour":         local_hour,
                    "observed_high_f":    observed_high_f,
                    "forecast_high_f":    forecast_high_f,
                    "observed_low_f":     observed_low_f,
                    "forecast_low_f":     forecast_low_f,
                    "forecast_issued_at": forecast_issued_at,
                    "hazards":            hazards,
                    "ticker":             ticker,
                    "bracket":            bracket,
                    "yes_price":          yes_price,
                    "no_price":           no_price,
                    "spread":             spread,
                    "volume":             volume,
                    "open_interest":      open_interest,
                    # Added 2026-05-06: instantaneous temperature reading.
                    # Rows before this date have None here.
                    # Use for afternoon-decline signal analysis (filter notna).
                    "current_temp_f":     nws.get("current_temp_f"),
                })
                rows_added += 1

            # Always fetch tomorrow's brackets — for both HIGH and LOWT.
            # NWS fields are null (no same-day observations for tomorrow yet).
            tomorrow_brackets = fetch_tomorrows_brackets(series)
            fetched_tomorrow[market_type] = tomorrow_brackets
            if tomorrow_brackets:
                tmr_type = f"{market_type}_tomorrow"
                for m in tomorrow_brackets:
                    ticker        = m.get("ticker", "")
                    yes_price     = float(m.get("yes_bid_dollars") or 0)
                    no_price      = float(m.get("no_bid_dollars")  or 0)
                    volume        = float(m.get("volume_fp") or 0)
                    open_interest = float(m.get("open_interest_fp") or 0)
                    bracket       = ticker.split("-")[-1] if "-" in ticker else ticker
                    yes_ask       = round(1.0 - no_price, 4) if no_price > 0 else None
                    spread        = round(yes_ask - yes_price, 4) if yes_ask and yes_price > 0 else None

                    observations.append({
                        "poll_time_utc":      poll_time,
                        "city":               city,
                        "market_type":        tmr_type,
                        "local_time":         local_time,
                        "local_hour":         local_hour,
                        "observed_high_f":    None,
                        "forecast_high_f":    None,
                        "observed_low_f":     None,
                        "forecast_low_f":     None,
                        "forecast_issued_at": None,
                        "hazards":            [],
                        "ticker":             ticker,
                        "bracket":            bracket,
                        "yes_price":          yes_price,
                        "no_price":           no_price,
                        "spread":             spread,
                        "volume":             volume,
                        "open_interest":      open_interest,
                    })
                    rows_added += 1

        # Summary per city — two lines:
        #   Line 1: today's market — observed temp, leading HIGH bracket, leading LOWT bracket
        #   Line 2: tomorrow's brackets — full price list with dismissed brackets flagged
        high_brackets = fetched_today.get("high", [])
        lowt_brackets = fetched_today.get("lowt", [])

        def leading(brackets):
            if not brackets:
                return "—", 0, None, 0
            top     = max(brackets, key=lambda x: float(x.get("yes_bid_dollars") or 0))
            bracket = top.get("ticker", "").split("-")[-1]
            yes_p   = float(top.get("yes_bid_dollars") or 0)
            no_p    = float(top.get("no_bid_dollars")  or 0)
            yes_ask = round(1.0 - no_p, 4) if no_p > 0 else None
            spread  = round(yes_ask - yes_p, 4) if yes_ask and yes_p > 0 else None
            volume  = float(top.get("volume_fp") or 0)
            return bracket, yes_p, spread, volume

        def bracket_summary(brackets):
            """Return a compact price list: 'B52.5:Y35/N64  B54.5:Y55/N44 ...'
            Brackets with Yes<=0.07 are flagged with * to indicate dismissed."""
            if not brackets:
                return "—"
            parts = []
            for m in brackets:
                label = m.get("ticker", "").split("-")[-1]
                yes_p = float(m.get("yes_bid_dollars") or 0)
                no_p  = float(m.get("no_bid_dollars")  or 0)
                flag  = "*" if yes_p <= 0.07 else " "
                parts.append(f"{flag}{label}:Y{yes_p:.2f}/N{no_p:.2f}")
            return "  ".join(parts)

        hi_bracket, hi_pct, hi_spread, hi_vol = leading(high_brackets)
        lo_bracket, lo_pct, lo_spread, lo_vol = leading(lowt_brackets)

        hi_spread_str = f"spd={hi_spread:.2f}" if hi_spread else "spd=—"
        lo_spread_str = f"spd={lo_spread:.2f}" if lo_spread else "spd=—"

        current_temp = nws.get("current_temp_f", "?")
        obs_hi       = nws.get("observed_high_f")
        obs_lo       = nws.get("observed_low_f")
        obs_hi_str   = f"hi={obs_hi:.1f}°" if obs_hi else ""
        obs_lo_str   = f"lo={obs_lo:.1f}°" if obs_lo else ""
        obs_str      = "  ".join(x for x in [obs_hi_str, obs_lo_str] if x) or f"curr={current_temp}°"

        fcst_age_str = ""
        if forecast_issued_at:
            try:
                issued  = datetime.fromisoformat(forecast_issued_at)
                now_utc = datetime.now(timezone.utc)
                age_h   = round((now_utc - issued).total_seconds() / 3600, 1)
                fcst_age_str = f" age={age_h}h"
            except Exception:
                pass

        hazard_str = f"  ⚠ {','.join(hazards)}" if hazards else ""

        # Line 1: today
        print(f"  {city:<14} {local_time}  {obs_str}{fcst_age_str}{hazard_str}")
        print(f"    TODAY  HIGH: {hi_bracket}@{hi_pct:.0%} {hi_spread_str} vol={hi_vol:.0f}  "
              f"LOWT: {lo_bracket}@{lo_pct:.0%} {lo_spread_str} vol={lo_vol:.0f}")

        # Line 2: tomorrow (only if brackets are available)
        tmr_high = fetched_tomorrow.get("high", [])
        tmr_lowt = fetched_tomorrow.get("lowt", [])
        if tmr_high or tmr_lowt:
            tmr_high_str = bracket_summary(sorted(
                tmr_high, key=lambda m: float(m.get("no_bid_dollars") or 0)
            )) if tmr_high else "—"
            tmr_lowt_str = bracket_summary(sorted(
                tmr_lowt, key=lambda m: float(m.get("no_bid_dollars") or 0)
            )) if tmr_lowt else "—"
            print(f"    TOMORROW  HIGH: {tmr_high_str}")
            print(f"              LOWT: {tmr_lowt_str}")
        print()

    return rows_added


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def main():
    print("=" * 70)
    print("  Temperature Market Observer  —  HIGH + LOWT")
    print(f"  Poll interval : {POLL_INTERVAL_SECS // 60} min")
    print(f"  Cities        : {len(CITIES)} ({', '.join(CITIES.keys())})")
    print(f"  Output        : {OUTPUT_JSON}")
    print("  Runs continuously — Ctrl+C to stop.")
    print("  Auto-switches to new day's markets at UTC midnight.")
    print("=" * 70)

    observations = load_observations()
    print(f"  Loaded {len(observations)} existing observations.\n")

    try:
        while True:
            added = poll_once(observations)
            save_observations(observations, added)
            print(f"  Saved {added} new rows ({len(observations)} total). "
                  f"Next poll in {POLL_INTERVAL_SECS // 60} min.")
            time.sleep(POLL_INTERVAL_SECS)

    except KeyboardInterrupt:
        print("\nStopped by user. Observations saved.")


if __name__ == "__main__":
    main()
