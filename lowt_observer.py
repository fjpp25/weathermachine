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
import requests
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo  # still needed for local_time display per city

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

POLL_INTERVAL_SECS = 15 * 60   # 15 minutes

OUTPUT_JSON = Path("data/lowt_observations.json")
OUTPUT_CSV  = Path("data/lowt_observations.csv")

CITIES = {
    # Currently trading
    "New York":      {"icao": "KNYC", "tz": "America/New_York",
                      "high": "KXHIGHNY",   "lowt": "KXLOWTNYC"},
    "Chicago":       {"icao": "KMDW", "tz": "America/Chicago",
                      "high": "KXHIGHCHI",  "lowt": "KXLOWTCHI"},
    "Miami":         {"icao": "KMIA", "tz": "America/New_York",
                      "high": "KXHIGHMIA",  "lowt": "KXLOWTMIA"},
    "Austin":        {"icao": "KAUS", "tz": "America/Chicago",
                      "high": "KXHIGHAUS",  "lowt": "KXLOWTAUS"},
    "Los Angeles":   {"icao": "KLAX", "tz": "America/Los_Angeles",
                      "high": "KXHIGHLAX",  "lowt": "KXLOWTLAX"},
    "San Francisco": {"icao": "KSFO", "tz": "America/Los_Angeles",
                      "high": "KXHIGHSFO",  "lowt": "KXLOWTSFO"},
    "Denver":        {"icao": "KDEN", "tz": "America/Denver",
                      "high": "KXHIGHDEN",  "lowt": "KXLOWTDEN"},
    "Philadelphia":  {"icao": "KPHL", "tz": "America/New_York",
                      "high": "KXHIGHPHIL", "lowt": "KXLOWTPHIL"},
    # New cities — observe only for now
    "Boston":        {"icao": "KBOS", "tz": "America/New_York",
                      "high": "KXHIGHBOS",  "lowt": "KXLOWTBOS"},
    "Las Vegas":     {"icao": "KLAS", "tz": "America/Los_Angeles",
                      "high": "KXHIGHLV",   "lowt": "KXLOWTLV"},
    "Atlanta":       {"icao": "KATL", "tz": "America/New_York",
                      "high": "KXHIGHATL",  "lowt": "KXLOWTATL"},
    "Oklahoma City": {"icao": "KOKC", "tz": "America/Chicago",
                      "high": "KXHIGHTOKC", "lowt": "KXLOWTTOKC"},
    "Minneapolis":   {"icao": "KMSP", "tz": "America/Chicago",
                      "high": "KXHIGHMSP",  "lowt": "KXLOWTMSP"},
    "Phoenix":       {"icao": "KPHX", "tz": "America/Phoenix",
                      "high": "KXHIGHPHX",  "lowt": "KXLOWTPHX"},
    "Washington DC": {"icao": "KDCA", "tz": "America/New_York",
                      "high": "KXHIGHDC",   "lowt": "KXLOWTDC"},
    "Dallas":        {"icao": "KDFW", "tz": "America/Chicago",
                      "high": "KXHIGHDFW",  "lowt": "KXLOWTDFW"},
    "San Antonio":   {"icao": "KSAT", "tz": "America/Chicago",
                      "high": "KXHIGHSAT",  "lowt": "KXLOWTSAT"},
    "Seattle":       {"icao": "KSEA", "tz": "America/Los_Angeles",
                      "high": "KXHIGHSEA",  "lowt": "KXLOWTSEA"},
    "New Orleans":   {"icao": "KMSY", "tz": "America/Chicago",
                      "high": "KXHIGHMSY",  "lowt": "KXLOWTMSY"},
    "Houston":       {"icao": "KHOU", "tz": "America/Chicago",
                      "high": "KXHIGHHOU",  "lowt": "KXLOWTHOU"},
}

CSV_FIELDS = [
    "poll_time_utc", "city", "market_type", "local_time", "local_hour",
    "observed_f", "forecast_f",
    "ticker", "bracket", "yes_price", "no_price",
    "spread", "volume", "open_interest",
]

# ---------------------------------------------------------------------------
# Data fetching
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


def fetch_nws_data(icao: str) -> dict:
    """
    Fetch current observed temp, observed low, and forecast high/low from NWS.
    Returns dict with: obs_temp, obs_low, obs_high, fcst_high, fcst_low
    """
    result = {
        "obs_temp":  None,
        "obs_low":   None,
        "obs_high":  None,
        "fcst_high": None,
        "fcst_low":  None,
    }

    # Observed temps — scan today's hourly observations
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
                if (now_utc - obs_time).total_seconds() < 86400:
                    temps.append(t * 9/5 + 32)
            except Exception:
                temps.append(t * 9/5 + 32)

        if temps:
            result["obs_temp"]  = round(temps[0], 1)   # most recent
            result["obs_low"]   = round(min(temps), 1)
            result["obs_high"]  = round(max(temps), 1)
    except Exception:
        pass

    # Forecast high and low from NWS gridpoint forecast
    try:
        resp = requests.get(
            f"https://api.weather.gov/stations/{icao}/observations/latest",
            headers={"User-Agent": "WeatherMachine/1.0"},
            timeout=10,
        )
        station_url = resp.json().get("properties", {}).get("station", "")
        if station_url:
            st_resp = requests.get(station_url,
                                   headers={"User-Agent": "WeatherMachine/1.0"},
                                   timeout=10)
            coords  = st_resp.json().get("geometry", {}).get("coordinates", [])
            if coords and len(coords) >= 2:
                lon, lat = coords[0], coords[1]
                pt_resp  = requests.get(
                    f"https://api.weather.gov/points/{lat:.4f},{lon:.4f}",
                    headers={"User-Agent": "WeatherMachine/1.0"},
                    timeout=10,
                )
                forecast_url = pt_resp.json().get("properties", {}).get("forecast")
                if forecast_url:
                    fc_resp = requests.get(forecast_url,
                                           headers={"User-Agent": "WeatherMachine/1.0"},
                                           timeout=10)
                    periods = fc_resp.json().get("properties", {}).get("periods", [])
                    for p in periods[:6]:
                        t = float(p.get("temperature", 0))
                        if p.get("isDaytime", True) and result["fcst_high"] is None:
                            result["fcst_high"] = t
                        elif not p.get("isDaytime", True) and result["fcst_low"] is None:
                            result["fcst_low"] = t
                        if result["fcst_high"] and result["fcst_low"]:
                            break
    except Exception:
        pass

    return result


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
    """Run one poll cycle. Returns number of bracket rows recorded."""
    poll_time = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    rows_added = 0

    for city, cfg in CITIES.items():
        tz         = ZoneInfo(cfg["tz"])
        local_now  = datetime.now(tz)
        local_time = local_now.strftime("%H:%M %Z")
        local_hour = local_now.hour

        # Fetch NWS data once per city (shared by both market types)
        nws = fetch_nws_data(cfg["icao"])

        for market_type in ("high", "lowt"):
            series   = cfg[market_type]
            brackets = fetch_brackets(series)

            if not brackets:
                continue

            obs_f  = nws["obs_high"]  if market_type == "high" else nws["obs_low"]
            fcst_f = nws["fcst_high"] if market_type == "high" else nws["fcst_low"]

            for m in brackets:
                ticker        = m.get("ticker", "")
                yes_price     = float(m.get("yes_bid_dollars") or 0)
                no_price      = float(m.get("no_bid_dollars")  or 0)
                volume        = float(m.get("volume_fp") or 0)
                open_interest = float(m.get("open_interest_fp") or 0)
                bracket       = ticker.split("-")[-1] if "-" in ticker else ticker

                # Spread: difference between yes ask and yes bid
                # yes_ask = 1 - no_bid (complement), so spread = yes_ask - yes_bid
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
        high_brackets = fetch_brackets(cfg["high"])
        lowt_brackets = fetch_brackets(cfg["lowt"])

        def leading(brackets):
            if not brackets:
                return "—", 0, None, 0
            top       = max(brackets, key=lambda x: float(x.get("yes_bid_dollars") or 0))
            bracket   = top.get("ticker", "").split("-")[-1]
            yes_p     = float(top.get("yes_bid_dollars") or 0)
            no_p      = float(top.get("no_bid_dollars") or 0)
            yes_ask   = round(1.0 - no_p, 4) if no_p > 0 else None
            spread    = round(yes_ask - yes_p, 4) if yes_ask and yes_p > 0 else None
            volume    = float(top.get("volume_fp") or 0)
            return bracket, yes_p, spread, volume

        hi_bracket, hi_pct, hi_spread, hi_vol  = leading(high_brackets)
        lo_bracket, lo_pct, lo_spread, lo_vol  = leading(lowt_brackets)

        hi_spread_str = f"spd={hi_spread:.2f}" if hi_spread else "spd=—"
        lo_spread_str = f"spd={lo_spread:.2f}" if lo_spread else "spd=—"

        print(f"  {city:<14} {local_time}  obs={nws['obs_temp']}°  "
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
    print(f"  Output        : {OUTPUT_JSON}")
    print("  Runs continuously — Ctrl+C to stop.")
    print("  Auto-switches to new day's markets at UTC midnight.")
    print("=" * 70)

    observations = load_observations()
    print(f"\n  Loaded {len(observations)} existing observations.\n")

    poll_count = 0

    while True:
        poll_count += 1
        now_str = datetime.now(timezone.utc).strftime("%H:%M UTC")
        print(f"\n[{now_str}] Poll #{poll_count}")
        print("-" * 50)

        rows = poll_once(observations)
        save_observations(observations)

        print(f"\n  Saved {len(observations)} total observations (+{rows} this poll)")
        print(f"  Next poll in {POLL_INTERVAL_SECS // 60} min — Ctrl+C to stop")

        time.sleep(POLL_INTERVAL_SECS)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nObserver stopped manually.")
