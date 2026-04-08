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

Stops automatically at STOP_HOUR_LISBON.
"""

import json
import time
import csv
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

try:
    import requests
except ImportError:
    raise SystemExit("Please install requests:  pip install requests")

# City registry and NWS feed are now shared modules — no local duplicates.
from cities import OBSERVE_CITIES as CITIES
import nws_feed

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

POLL_INTERVAL_SECS = 15 * 60   # 15 minutes

OUTPUT_JSON = Path("data/lowt_observations.json")
OUTPUT_CSV  = Path("data/lowt_observations.csv")

CSV_FIELDS = [
    "poll_time_utc", "city", "market_type", "local_time", "local_hour",
    "observed_f", "forecast_f",
    "ticker", "bracket", "yes_price", "no_price",
    "spread", "volume", "open_interest",
]

# ---------------------------------------------------------------------------
# Kalshi data fetching (unchanged — only NWS fetching was duplicated)
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


# ---------------------------------------------------------------------------
# Observation recorder
# ---------------------------------------------------------------------------

def load_observations() -> list[dict]:
    if OUTPUT_JSON.exists():
        try:
            return json.loads(OUTPUT_JSON.read_text())
        except Exception:
            return []
    return []


def save_observations(obs: list[dict]):
    OUTPUT_JSON.parent.mkdir(exist_ok=True)
    OUTPUT_JSON.write_text(json.dumps(obs, indent=2))
    with open(OUTPUT_CSV, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(obs)


def poll_once(observations: list[dict]) -> int:
    """
    Run one poll cycle. Returns number of bracket rows recorded.

    NWS data is fetched once per city via nws_feed.snapshot(), which:
      - uses the shared CITIES registry (no duplication)
      - applies LST boundary logic correctly (not a 24-hr rolling window)
      - benefits from retry logic and the grid cache
    """
    poll_time  = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    rows_added = 0

    # Single NWS pass for all cities — reuse results across HIGH and LOWT loops.
    print(f"\n[{poll_time}] Fetching NWS data for {len(CITIES)} cities...")
    nws_results = nws_feed.snapshot()

    for city, cfg in CITIES.items():
        tz        = ZoneInfo(cfg["tz"])
        local_now = datetime.now(tz)
        local_time = local_now.strftime("%H:%M %Z")
        local_hour = local_now.hour

        # Pull pre-fetched NWS data — no extra HTTP calls per city.
        nws = nws_results.get(city, {})

        for market_type in ("high", "lowt"):
            series   = cfg.get(f"{market_type}_series") or cfg.get(market_type)
            if not series:
                continue
            brackets = fetch_brackets(series)

            if not brackets:
                continue

            # Map nws_feed.snapshot() keys to the values we need.
            # nws_feed uses LST boundary logic — correct for CLI settlement.
            if market_type == "high":
                obs_f  = nws.get("observed_high_f")
                fcst_f = nws.get("forecast_high_f")
            else:
                obs_f  = nws.get("observed_low_f")
                fcst_f = nws.get("forecast_low_f")

            for m in brackets:
                ticker        = m.get("ticker", "")
                yes_price     = float(m.get("yes_bid_dollars") or 0)
                no_price      = float(m.get("no_bid_dollars")  or 0)
                volume        = float(m.get("volume_fp") or 0)
                open_interest = float(m.get("open_interest_fp") or 0)
                bracket       = ticker.split("-")[-1] if "-" in ticker else ticker

                yes_ask = round(1.0 - no_price, 4) if no_price > 0 else None
                spread  = round(yes_ask - yes_price, 4) if yes_ask and yes_price > 0 else None

                observations.append({
                    "poll_time_utc": poll_time,
                    "city":          city,
                    "market_type":   market_type,
                    "local_time":    local_time,
                    "local_hour":    local_hour,
                    "observed_f":    obs_f,
                    "forecast_f":    fcst_f,
                    "ticker":        ticker,
                    "bracket":       bracket,
                    "yes_price":     yes_price,
                    "no_price":      no_price,
                    "spread":        spread,
                    "volume":        volume,
                    "open_interest": open_interest,
                })
                rows_added += 1

        # Summary line per city
        high_brackets = fetch_brackets(cfg.get("high_series") or cfg.get("high", ""))
        lowt_brackets = fetch_brackets(cfg.get("lowt_series") or cfg.get("lowt", ""))

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

        hi_bracket, hi_pct, hi_spread, hi_vol = leading(high_brackets)
        lo_bracket, lo_pct, lo_spread, lo_vol = leading(lowt_brackets)

        hi_spread_str = f"spd={hi_spread:.2f}" if hi_spread else "spd=—"
        lo_spread_str = f"spd={lo_spread:.2f}" if lo_spread else "spd=—"

        current_temp = nws.get("current_temp_f", "?")
        print(f"  {city:<14} {local_time}  obs={current_temp}°  "
              f"HIGH: {hi_bracket}@{hi_pct:.0%} {hi_spread_str} vol={hi_vol:.0f}  "
              f"LOWT: {lo_bracket}@{lo_pct:.0%} {lo_spread_str} vol={lo_vol:.0f}")

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
            save_observations(observations)
            print(f"  Saved {added} new rows ({len(observations)} total). "
                  f"Next poll in {POLL_INTERVAL_SECS // 60} min.")
            time.sleep(POLL_INTERVAL_SECS)

    except KeyboardInterrupt:
        print("\nStopped by user. Observations saved.")


if __name__ == "__main__":
    main()
