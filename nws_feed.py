"""
nws_feed.py
-----------
Fetches two live data points for each of the 8 Kalshi settlement stations:

  1. current_temp_f   : Latest observed temperature (°F) at the ASOS station
  2. forecast_high_f  : NWS forecast high for today (°F)
  3. forecast_low_f   : NWS forecast low for today (°F)
  4. observed_high_f  : Highest temperature observed so far today (°F)
  5. as_of            : Timestamp of the latest observation (local time)

All data comes from api.weather.gov — no API key required.

Two endpoints used:
  - /stations/{ICAO}/observations/latest       → current observation
  - /stations/{ICAO}/observations?limit=24     → today's observed high
  - /points/{lat},{lon}                        → resolves forecast grid URL
  - /gridpoints/{office}/{x},{y}/forecast      → today's forecast high/low

The /points lookup result is cached per city (it never changes) to avoid
redundant calls. Only observations and forecasts are re-fetched each poll.

Usage:
  python nws_feed.py              # print live snapshot for all 8 cities
  python nws_feed.py --city Miami # single city
  python nws_feed.py --watch 300  # poll every 5 minutes
"""

import json
import time
import argparse
from datetime import datetime, timezone, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

try:
    import requests
except ImportError:
    raise SystemExit("Please install requests:  pip install requests")

# ---------------------------------------------------------------------------
# City config: ICAO station, coordinates, timezone
# Coordinates are for the settlement station itself (not city center)
# ---------------------------------------------------------------------------
CITIES = {
    "New York": {
        "icao":        "KNYC",
        "lat":         40.7789,
        "lon":         -73.9692,
        "tz":          "America/New_York",
        "lst_offset":  -5,   # UTC-5 always (EST), used for CLI day boundary
    },
    "Chicago": {
        "icao":        "KMDW",
        "lat":         41.7868,
        "lon":         -87.7522,
        "tz":          "America/Chicago",
        "lst_offset":  -6,   # UTC-6 always (CST)
    },
    "Miami": {
        "icao":        "KMIA",
        "lat":         25.7959,
        "lon":         -80.2870,
        "tz":          "America/New_York",
        "lst_offset":  -5,   # UTC-5 always (EST)
    },
    "Austin": {
        "icao":        "KAUS",
        "lat":         30.1945,
        "lon":         -97.6699,
        "tz":          "America/Chicago",
        "lst_offset":  -6,   # UTC-6 always (CST)
    },
    "Los Angeles": {
        "icao":        "KLAX",
        "lat":         33.9425,
        "lon":         -118.4081,
        "tz":          "America/Los_Angeles",
        "lst_offset":  -8,   # UTC-8 always (PST)
    },
    "San Francisco": {
        "icao":        "KSFO",
        "lat":         37.6213,
        "lon":         -122.3790,
        "tz":          "America/Los_Angeles",
        "lst_offset":  -8,   # UTC-8 always (PST)
    },
    "Denver": {
        "icao":        "KDEN",
        "lat":         39.8561,
        "lon":         -104.6737,
        "tz":          "America/Denver",
        "lst_offset":  -7,   # UTC-7 always (MST)
    },
    "Philadelphia": {
        "icao":        "KPHL",
        "lat":         39.8721,
        "lon":         -75.2411,
        "tz":          "America/New_York",
        "lst_offset":  -5,
    },
    "Atlanta": {
        "icao":        "KATL",
        "lat":         33.6407,
        "lon":         -84.4277,
        "tz":          "America/New_York",
        "lst_offset":  -5,
    },
    "Houston": {
        "icao":        "KHOU",
        "lat":         29.6454,
        "lon":         -95.2789,
        "tz":          "America/Chicago",
        "lst_offset":  -6,
    },
    "Phoenix": {
        "icao":        "KPHX",
        "lat":         33.4343,
        "lon":         -112.0078,
        "tz":          "America/Phoenix",
        "lst_offset":  -7,   # UTC-7 always (MST, no DST)
    },
    "Las Vegas": {
        "icao":        "KLAS",
        "lat":         36.0800,
        "lon":         -115.1522,
        "tz":          "America/Los_Angeles",
        "lst_offset":  -8,
    },
}

API_BASE    = "https://api.weather.gov"
GRID_CACHE  = Path("data/nws_grid_cache.json")
USER_AGENT  = "kalshi-weather-trader/1.0 (research project)"  # NWS requires a User-Agent

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get(url: str, timeout: int = 15) -> dict:
    """GET with NWS-required User-Agent header."""
    headers = {"User-Agent": USER_AGENT, "Accept": "application/geo+json"}
    resp = requests.get(url, headers=headers, timeout=timeout)
    resp.raise_for_status()
    return resp.json()


def c_to_f(celsius) -> float | None:
    """Convert Celsius to Fahrenheit. NWS observation temps are in Celsius."""
    if celsius is None:
        return None
    return round(celsius * 9 / 5 + 32, 1)


def local_now(tz_name: str) -> datetime:
    return datetime.now(ZoneInfo(tz_name))


# ---------------------------------------------------------------------------
# Grid cache: resolve /points once per city, store gridpoint URL forever
# ---------------------------------------------------------------------------

def load_grid_cache() -> dict:
    GRID_CACHE.parent.mkdir(parents=True, exist_ok=True)
    if GRID_CACHE.exists():
        with open(GRID_CACHE) as f:
            return json.load(f)
    return {}


def save_grid_cache(cache: dict):
    with open(GRID_CACHE, "w") as f:
        json.dump(cache, f, indent=2)


def get_forecast_url(city: str, meta: dict, cache: dict) -> str | None:
    """
    Return the NWS gridpoint forecast URL for a city.
    Looks up /points/{lat},{lon} on first call, then caches forever.
    """
    if city in cache:
        return cache[city]["forecast_url"]

    print(f"  Resolving grid for {city}...", end=" ", flush=True)
    try:
        data = get(f"{API_BASE}/points/{meta['lat']},{meta['lon']}")
        props = data["properties"]
        forecast_url = props["forecast"]
        cache[city] = {
            "forecast_url": forecast_url,
            "office":       props["cwa"],
            "grid_x":       props["gridX"],
            "grid_y":       props["gridY"],
        }
        save_grid_cache(cache)
        print("OK")
        return forecast_url
    except Exception as e:
        print(f"FAILED: {e}")
        return None


# ---------------------------------------------------------------------------
# Live data fetchers
# ---------------------------------------------------------------------------

def fetch_current_observation(icao: str) -> dict:
    """
    Fetch the latest ASOS observation for a station.
    Returns dict with current_temp_f and as_of (UTC ISO string).
    """
    data = get(f"{API_BASE}/stations/{icao}/observations/latest")
    props = data["properties"]

    temp_c = props.get("temperature", {}).get("value")
    timestamp = props.get("timestamp")

    return {
        "current_temp_f": c_to_f(temp_c),
        "as_of_utc":      timestamp,
    }


def fetch_observed_high(icao: str, tz_name: str, lst_offset: int) -> float | None:
    """
    Fetch last 48 hours of observations and extract today's high so far.

    Uses LST (Local Standard Time) for the day boundary — the fixed UTC offset,
    never adjusted for DST — because that's what Kalshi's NWS CLI report uses.
    This prevents a 1-hour miscounting of observations during DST periods.
    """
    data = get(f"{API_BASE}/stations/{icao}/observations?limit=48")
    features = data.get("features", [])

    # LST timezone: fixed offset, no DST adjustment
    lst_tz     = timezone(timedelta(hours=lst_offset))
    today_lst  = datetime.now(lst_tz).date()

    temps_today = []
    for feature in features:
        props  = feature.get("properties", {})
        ts     = props.get("timestamp")
        temp_c = props.get("temperature", {}).get("value")

        if ts is None or temp_c is None:
            continue

        obs_time_lst = datetime.fromisoformat(ts).astimezone(lst_tz)
        if obs_time_lst.date() == today_lst:
            temps_today.append(c_to_f(temp_c))

    return max(temps_today) if temps_today else None


def fetch_observed_low(icao: str, tz_name: str, lst_offset: int) -> float | None:
    """
    Fetch last 48 hours of observations and extract today's low so far.
    Same LST boundary logic as fetch_observed_high.
    """
    data = get(f"{API_BASE}/stations/{icao}/observations?limit=48")
    features = data.get("features", [])

    lst_tz    = timezone(timedelta(hours=lst_offset))
    today_lst = datetime.now(lst_tz).date()

    temps_today = []
    for feature in features:
        props  = feature.get("properties", {})
        ts     = props.get("timestamp")
        temp_c = props.get("temperature", {}).get("value")

        if ts is None or temp_c is None:
            continue

        obs_time_lst = datetime.fromisoformat(ts).astimezone(lst_tz)
        if obs_time_lst.date() == today_lst:
            temps_today.append(c_to_f(temp_c))

    return min(temps_today) if temps_today else None


def fetch_observed_high_low(icao: str, tz_name: str, lst_offset: int) -> tuple:
    """
    Fetch observations once and return (observed_high, observed_low) for today.
    More efficient than calling fetch_observed_high and fetch_observed_low separately.
    """
    data = get(f"{API_BASE}/stations/{icao}/observations?limit=48")
    features = data.get("features", [])

    lst_tz    = timezone(timedelta(hours=lst_offset))
    today_lst = datetime.now(lst_tz).date()

    temps_today = []
    for feature in features:
        props  = feature.get("properties", {})
        ts     = props.get("timestamp")
        temp_c = props.get("temperature", {}).get("value")

        if ts is None or temp_c is None:
            continue

        obs_time_lst = datetime.fromisoformat(ts).astimezone(lst_tz)
        if obs_time_lst.date() == today_lst:
            temps_today.append(c_to_f(temp_c))

    if temps_today:
        return max(temps_today), min(temps_today)
    return None, None


def fetch_forecast_high_low(forecast_url: str, tz_name: str) -> dict:
    """
    Fetch the NWS daily forecast and extract today's high and low.
    The forecast periods alternate Day/Night. We want:
      - high: the first daytime period for today
      - low:  the first nighttime period for today
    """
    data = get(forecast_url)
    periods = data["properties"]["periods"]

    local_tz   = ZoneInfo(tz_name)
    today_date = datetime.now(local_tz).date()

    forecast_high = None
    forecast_low  = None

    for period in periods:
        start = datetime.fromisoformat(period["startTime"]).astimezone(local_tz)
        if start.date() != today_date:
            continue

        temp = period.get("temperature")
        is_day = period.get("isDaytime", True)

        if is_day and forecast_high is None and temp is not None:
            # NWS forecast temps are already in °F when units=us
            unit = period.get("temperatureUnit", "F")
            forecast_high = c_to_f(temp) if unit == "C" else float(temp)

        if not is_day and forecast_low is None and temp is not None:
            unit = period.get("temperatureUnit", "F")
            forecast_low = c_to_f(temp) if unit == "C" else float(temp)

    return {
        "forecast_high_f": forecast_high,
        "forecast_low_f":  forecast_low,
    }


# ---------------------------------------------------------------------------
# Main snapshot function
# ---------------------------------------------------------------------------

def snapshot(city_filter: str = None) -> dict:
    """
    Fetch live data for all cities (or a single city if city_filter is set).
    Returns dict keyed by city name.
    """
    grid_cache = load_grid_cache()
    results    = {}

    cities = {k: v for k, v in CITIES.items()
              if city_filter is None or k.lower() == city_filter.lower()}

    for city, meta in cities.items():
        result = {"city": city, "icao": meta["icao"]}

        try:
            # Current observation
            obs = fetch_current_observation(meta["icao"])
            result.update(obs)

            # Today's observed high and low so far (single API call, LST boundary)
            obs_hi, obs_lo = fetch_observed_high_low(
                meta["icao"], meta["tz"], meta["lst_offset"]
            )
            result["observed_high_f"] = obs_hi
            result["observed_low_f"]  = obs_lo

            # Forecast high/low
            forecast_url = get_forecast_url(city, meta, grid_cache)
            if forecast_url:
                fcst = fetch_forecast_high_low(forecast_url, meta["tz"])
                result.update(fcst)
            else:
                result["forecast_high_f"] = None
                result["forecast_low_f"]  = None

            result["error"] = None

        except Exception as e:
            result["error"] = str(e)

        # Local time context — wall clock (for display) and LST (for CLI logic)
        local_now_dt        = local_now(meta["tz"])
        lst_tz              = timezone(timedelta(hours=meta["lst_offset"]))
        lst_now_dt          = datetime.now(lst_tz)

        result["local_time"]        = local_now_dt.strftime("%H:%M %Z")
        result["local_date"]        = local_now_dt.strftime("%Y-%m-%d")
        result["city_local_hour"]   = local_now_dt.hour        # wall clock hour (0-23)
        result["city_lst_hour"]     = lst_now_dt.hour          # LST hour (0-23), for CLI boundary logic
        result["utc_now"]           = datetime.now(timezone.utc).isoformat()

        results[city] = result
        time.sleep(0.3)   # gentle on the API

    return results


# ---------------------------------------------------------------------------
# Display
# ---------------------------------------------------------------------------

def display(results: dict):
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    print(f"\n{'='*72}")
    print(f"  NWS Live Feed  —  {now_utc}")
    print(f"{'='*72}")
    print(f"{'City':<16} {'LocalTime':>12} {'LSTHr':>6} {'CurrF':>6} {'ObsHi':>6} {'FcstHi':>7} {'FcstLo':>7}")
    print(f"{'-'*72}")

    for city, d in results.items():
        if d.get("error"):
            print(f"{city:<16}  ERROR: {d['error'][:50]}")
            continue

        print(
            f"{city:<16} "
            f"{d.get('local_time','?'):>12} "
            f"{d.get('city_lst_hour','?'):>6} "
            f"{fmt(d.get('current_temp_f')):>6} "
            f"{fmt(d.get('observed_high_f')):>6} "
            f"{fmt(d.get('forecast_high_f')):>7} "
            f"{fmt(d.get('forecast_low_f')):>7}"
        )

    print(f"{'='*72}")
    print("  LocalTime=wall clock  LSTHr=CLI reporting hour  ObsHi=today's high so far")


def fmt(val) -> str:
    return f"{val:.1f}" if val is not None else "  N/A"


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="NWS live feed for Kalshi settlement stations")
    parser.add_argument("--city",  type=str, default=None,
                        help="Filter to a single city (e.g. 'Miami')")
    parser.add_argument("--watch", type=int, default=None, metavar="SECONDS",
                        help="Poll repeatedly every N seconds (e.g. --watch 300)")
    args = parser.parse_args()

    if args.watch:
        print(f"Watching — polling every {args.watch}s. Ctrl+C to stop.")
        while True:
            results = snapshot(args.city)
            display(results)
            time.sleep(args.watch)
    else:
        results = snapshot(args.city)
        display(results)
