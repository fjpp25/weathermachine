"""
lowt_observer.py
----------------
Passive overnight observer for Kalshi low temperature markets.
No trading — read-only. Polls every 15 minutes and records bracket
prices alongside NWS observed and forecast lows.

Run before bed, review in the morning:
  python lowt_observer.py

Output: data/lowt_observations.json
        data/lowt_observations.csv   (for easy spreadsheet review)

Stops automatically at 8am local Lisbon time (UTC+1).
"""

import json
import time
import csv
import requests
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

POLL_INTERVAL_SECS = 15 * 60   # 15 minutes
STOP_HOUR_LISBON   = 23         # stop at 8am Lisbon time

OUTPUT_JSON = Path("data/lowt_observations.json")
OUTPUT_CSV  = Path("data/lowt_observations.csv")

CITIES = {
    "New York":      {"series": "KXLOWTNYC",  "icao": "KNYC", "tz": "America/New_York"},
    "Chicago":       {"series": "KXLOWTCHI",  "icao": "KMDW", "tz": "America/Chicago"},
    "Miami":         {"series": "KXLOWTMIA",  "icao": "KMIA", "tz": "America/New_York"},
    "Austin":        {"series": "KXLOWTAUS",  "icao": "KAUS", "tz": "America/Chicago"},
    "Los Angeles":   {"series": "KXLOWTLAX",  "icao": "KLAX", "tz": "America/Los_Angeles"},
    "Denver":        {"series": "KXLOWTDEN",  "icao": "KDEN", "tz": "America/Denver"},
    "Philadelphia":  {"series": "KXLOWTPHIL", "icao": "KPHL", "tz": "America/New_York"},
}

CSV_FIELDS = [
    "poll_time_utc", "city", "local_time", "local_hour",
    "observed_low_f", "forecast_low_f",
    "ticker", "bracket", "yes_price", "no_price", "volume",
]

# ---------------------------------------------------------------------------
# Data fetching
# ---------------------------------------------------------------------------

def fetch_brackets(series: str) -> list[dict]:
    """Fetch today's low temperature brackets from Kalshi public API."""
    today = datetime.now(timezone.utc).strftime("%y%b%d").upper()
    try:
        resp = requests.get(
            "https://api.elections.kalshi.com/trade-api/v2/markets",
            params={"series_ticker": series, "status": "open"},
            timeout=10,
        )
        markets = resp.json().get("markets", [])
        # Filter to today's event only
        return [m for m in markets if today in m.get("ticker", "").upper()]
    except Exception as e:
        print(f"  Kalshi error for {series}: {e}")
        return []


def fetch_nws_low(icao: str) -> tuple[float | None, float | None]:
    """
    Fetch current observed low and forecast overnight low from NWS.
    Returns (observed_low_f, forecast_low_f).
    """
    observed_low = None
    forecast_low = None

    # Observed low — minimum of today's hourly observations
    try:
        resp = requests.get(
            f"https://api.weather.gov/stations/{icao}/observations",
            params={"limit": 24},
            headers={"User-Agent": "WeatherMachine/1.0"},
            timeout=10,
        )
        features = resp.json().get("features", [])
        now_utc  = datetime.now(timezone.utc)
        temps    = []
        for f in features:
            props = f.get("properties", {})
            ts    = props.get("timestamp", "")
            t     = props.get("temperature", {}).get("value")
            if t is None:
                continue
            try:
                obs_time = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                # Only include observations from today (past 24h)
                if (now_utc - obs_time).total_seconds() < 86400:
                    temps.append(t * 9/5 + 32)
            except Exception:
                temps.append(t * 9/5 + 32)
        if temps:
            observed_low = round(min(temps), 1)
    except Exception:
        pass

    # Forecast overnight low — from NWS station forecast
    # Use the /stations/{icao}/observations latest to get coordinates,
    # then hit the gridpoint hourly forecast for tonight's minimum
    try:
        resp = requests.get(
            f"https://api.weather.gov/stations/{icao}/observations/latest",
            headers={"User-Agent": "WeatherMachine/1.0"},
            timeout=10,
        )
        props = resp.json().get("properties", {})
        lat   = props.get("station", {})

        # Get station metadata for grid coordinates
        station_url = resp.json().get("properties", {}).get("station", "")
        if station_url:
            st_resp = requests.get(
                station_url,
                headers={"User-Agent": "WeatherMachine/1.0"},
                timeout=10,
            )
            st_props  = st_resp.json().get("properties", {})
            coords    = st_resp.json().get("geometry", {}).get("coordinates", [])
            if coords and len(coords) >= 2:
                lon, lat = coords[0], coords[1]
                # Get NWS grid point
                pt_resp = requests.get(
                    f"https://api.weather.gov/points/{lat:.4f},{lon:.4f}",
                    headers={"User-Agent": "WeatherMachine/1.0"},
                    timeout=10,
                )
                forecast_url = pt_resp.json().get("properties", {}).get("forecast")
                if forecast_url:
                    fc_resp  = requests.get(
                        forecast_url,
                        headers={"User-Agent": "WeatherMachine/1.0"},
                        timeout=10,
                    )
                    periods = fc_resp.json().get("properties", {}).get("periods", [])
                    # Find the next overnight (isDaytime=False) period
                    for p in periods[:6]:
                        if not p.get("isDaytime", True):
                            forecast_low = float(p.get("temperature", 0))
                            break
    except Exception:
        pass

    return observed_low, forecast_low


# ---------------------------------------------------------------------------
# Observation recorder
# ---------------------------------------------------------------------------

def load_observations() -> list[dict]:
    if OUTPUT_JSON.exists():
        return json.loads(OUTPUT_JSON.read_text())
    return []


def save_observations(obs: list[dict]):
    OUTPUT_JSON.parent.mkdir(exist_ok=True)
    OUTPUT_JSON.write_text(json.dumps(obs, indent=2))

    # Also write CSV for easy review
    with open(OUTPUT_CSV, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(obs)


def poll_once(observations: list[dict]) -> int:
    """Run one poll cycle. Returns number of bracket rows recorded."""
    poll_time = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    rows_added = 0

    for city, cfg in CITIES.items():
        tz         = ZoneInfo(cfg["tz"])
        local_now  = datetime.now(tz)
        local_time = local_now.strftime("%H:%M %Z")
        local_hour = local_now.hour

        # Fetch NWS data
        obs_low, fcst_low = fetch_nws_low(cfg["icao"])

        # Fetch Kalshi brackets
        brackets = fetch_brackets(cfg["series"])
        if not brackets:
            print(f"  {city}: no open brackets found")
            continue

        for m in brackets:
            ticker    = m.get("ticker", "")
            yes_price = float(m.get("yes_bid_dollars") or 0)
            no_price  = float(m.get("no_bid_dollars")  or 0)
            volume    = float(m.get("volume_fp") or 0)

            # Extract bracket label from ticker suffix
            parts   = ticker.split("-")
            bracket = parts[-1] if parts else ticker

            observations.append({
                "poll_time_utc":  poll_time,
                "city":           city,
                "local_time":     local_time,
                "local_hour":     local_hour,
                "observed_low_f": obs_low,
                "forecast_low_f": fcst_low,
                "ticker":         ticker,
                "bracket":        bracket,
                "yes_price":      yes_price,
                "no_price":       no_price,
                "volume":         volume,
            })
            rows_added += 1

        # Brief summary per city
        leading = max(brackets, key=lambda x: float(x.get("yes_bid_dollars") or 0))
        lead_yes = float(leading.get("yes_bid_dollars") or 0)
        lead_ticker = leading.get("ticker", "")[-6:]
        print(f"  {city:<14} local={local_time}  "
              f"obs_lo={obs_low}°  fcst_lo={fcst_low}°  "
              f"leading={lead_ticker} @ {lead_yes:.0%}")

    return rows_added


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def main():
    print("=" * 65)
    print("  LOWT Observer  —  Passive overnight data collection")
    print(f"  Poll interval : {POLL_INTERVAL_SECS // 60} min")
    print(f"  Output        : {OUTPUT_JSON}")
    print(f"  Stops at      : {STOP_HOUR_LISBON}:00 Lisbon time")
    print("  No trades will be placed.")
    print("=" * 65)

    observations = load_observations()
    print(f"\n  Loaded {len(observations)} existing observations.\n")

    poll_count = 0

    while True:
        # Stop at configured hour Lisbon time
        lisbon_hour = datetime.now(ZoneInfo("Europe/Lisbon")).hour
        if lisbon_hour >= STOP_HOUR_LISBON:
            print(f"\nIt's {STOP_HOUR_LISBON}:00 Lisbon time — stopping observer.")
            print(f"Total observations recorded: {len(observations)}")
            print(f"Review with: python lowt_analyzer.py")
            break

        poll_count += 1
        now_str = datetime.now(timezone.utc).strftime("%H:%M UTC")
        print(f"\n[{now_str}] Poll #{poll_count}")
        print("-" * 45)

        rows = poll_once(observations)
        save_observations(observations)

        print(f"\n  Saved {len(observations)} total observations (+{rows} this poll)")
        print(f"  Next poll in {POLL_INTERVAL_SECS // 60} min — "
              f"Ctrl+C to stop early")

        time.sleep(POLL_INTERVAL_SECS)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nObserver stopped manually.")
