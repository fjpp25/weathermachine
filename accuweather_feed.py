"""
accuweather_feed.py
-------------------
Fetches daily high/low temperature forecasts from AccuWeather for all
20 trading cities. Results are cached to data/accuweather_forecasts.json
and refreshed every REFRESH_INTERVAL_HOURS hours.

API key is read from data/config.json under "accuweather_api_key".
Location keys are cached permanently in data/accuweather_locations.json
— fetched once per city on first run, never again unless the cache is
deleted.

Usage (standalone):
    python accuweather_feed.py              # fetch all cities, print table
    python accuweather_feed.py --city Miami # single city

Usage (from other modules):
    import accuweather_feed
    forecasts = accuweather_feed.snapshot()
    miami = forecasts.get("Miami", {})
    high_f = miami.get("forecast_high_f")   # e.g. 91.0
    low_f  = miami.get("forecast_low_f")    # e.g. 75.0
    fetched = miami.get("fetched_at")       # ISO timestamp
"""

from __future__ import annotations

import json
import time
import argparse
from datetime import datetime, timezone, timedelta
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

from log_setup import get_logger

log = get_logger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

BASE_URL              = "https://dataservice.accuweather.com"
REFRESH_INTERVAL_HRS  = 2       # re-fetch if cached forecast is older than this
MAX_WORKERS           = 5       # parallel city fetches (gentle on API)
REQUEST_TIMEOUT       = 10      # seconds per HTTP call

LOCATIONS_CACHE       = Path("data/accuweather_locations.json")
FORECASTS_CACHE       = Path("data/accuweather_forecasts.json")
CONFIG_PATH           = Path("data/config.json")

# City search queries — matched to our 20 trading cities
# Using specific search strings to ensure the right location key is returned
CITY_SEARCH_QUERIES: dict[str, str] = {
    "New York":      "New York, NY",
    "Chicago":       "Chicago, IL",
    "Miami":         "Miami, FL",
    "Austin":        "Austin, TX",
    "Los Angeles":   "Los Angeles, CA",
    "San Francisco": "San Francisco, CA",
    "Denver":        "Denver, CO",
    "Philadelphia":  "Philadelphia, PA",
    "Atlanta":       "Atlanta, GA",
    "Boston":        "Boston, MA",
    "Washington DC": "Washington, DC",
    "Houston":       "Houston, TX",
    "Phoenix":       "Phoenix, AZ",
    "Las Vegas":     "Las Vegas, NV",
    "Dallas":        "Dallas, TX",
    "San Antonio":   "San Antonio, TX",
    "Seattle":       "Seattle, WA",
    "New Orleans":   "New Orleans, LA",
    "Minneapolis":   "Minneapolis, MN",
    "Oklahoma City": "Oklahoma City, OK",
}


# ---------------------------------------------------------------------------
# API key
# ---------------------------------------------------------------------------

def _api_key() -> str | None:
    """Read AccuWeather API key from data/config.json."""
    try:
        config = json.loads(CONFIG_PATH.read_text())
        return config.get("accuweather_api_key")
    except Exception as e:
        log.warning("accuweather: could not read config.json: %s", e)
        return None


# ---------------------------------------------------------------------------
# Location key cache
# ---------------------------------------------------------------------------

def _load_location_cache() -> dict[str, str]:
    """Load {city: location_key} from cache file."""
    try:
        if LOCATIONS_CACHE.exists():
            return json.loads(LOCATIONS_CACHE.read_text())
    except Exception:
        pass
    return {}


def _save_location_cache(cache: dict[str, str]) -> None:
    LOCATIONS_CACHE.parent.mkdir(parents=True, exist_ok=True)
    LOCATIONS_CACHE.write_text(json.dumps(cache, indent=2))


def _fetch_location_key(city: str, api_key: str) -> str | None:
    """
    Fetch AccuWeather location key for a city via the city search endpoint.
    Returns the key string (e.g. "349727") or None on failure.
    """
    query = CITY_SEARCH_QUERIES.get(city, city)
    try:
        resp = requests.get(
            f"{BASE_URL}/locations/v1/cities/search",
            params={"apikey": api_key, "q": query, "language": "en-us"},
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        results = resp.json()
        if results:
            key = results[0].get("Key")
            log.info("accuweather: location key for %s → %s", city, key)
            return key
        log.warning("accuweather: no location results for %s (query=%s)", city, query)
        return None
    except Exception as e:
        log.error("accuweather: location key fetch failed for %s: %s", city, e)
        return None


def ensure_location_keys(api_key: str) -> dict[str, str]:
    """
    Return {city: location_key} for all 20 cities.
    Fetches missing keys and updates the cache.
    """
    cache = _load_location_cache()
    missing = [c for c in CITY_SEARCH_QUERIES if c not in cache]

    if missing:
        log.info("accuweather: fetching location keys for %d cities...", len(missing))
        for city in missing:
            key = _fetch_location_key(city, api_key)
            if key:
                cache[city] = key
            time.sleep(0.3)   # gentle on the API
        _save_location_cache(cache)
        log.info("accuweather: location keys cached to %s", LOCATIONS_CACHE)

    return cache


# ---------------------------------------------------------------------------
# Forecast fetching
# ---------------------------------------------------------------------------

def _fetch_city_forecast(
    city: str,
    location_key: str,
    api_key: str,
) -> tuple[str, dict]:
    """
    Fetch today's high/low forecast for a single city.
    Returns (city, result_dict).
    """
    result = {
        "city":            city,
        "forecast_high_f": None,
        "forecast_low_f":  None,
        "fetched_at":      datetime.now(timezone.utc).isoformat(),
        "error":           None,
    }
    try:
        resp = requests.get(
            f"{BASE_URL}/forecasts/v1/daily/1day/{location_key}",
            params={
                "apikey":  api_key,
                "details": "true",
                "metric":  "false",   # imperial (°F)
            },
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()

        forecasts = data.get("DailyForecasts", [])
        if not forecasts:
            result["error"] = "No DailyForecasts in response"
            return city, result

        today = forecasts[0]
        temp  = today.get("Temperature", {})

        high = temp.get("Maximum", {}).get("Value")
        low  = temp.get("Minimum", {}).get("Value")

        result["forecast_high_f"] = float(high) if high is not None else None
        result["forecast_low_f"]  = float(low)  if low  is not None else None

    except requests.exceptions.HTTPError as e:
        result["error"] = f"HTTP {e.response.status_code}: {e}"
        log.warning("accuweather: HTTP error for %s: %s", city, e)
    except Exception as e:
        result["error"] = str(e)
        log.error("accuweather: forecast fetch failed for %s: %s", city, e)

    return city, result


# ---------------------------------------------------------------------------
# Cache management
# ---------------------------------------------------------------------------

def _load_forecast_cache() -> dict[str, dict]:
    try:
        if FORECASTS_CACHE.exists():
            return json.loads(FORECASTS_CACHE.read_text())
    except Exception:
        pass
    return {}


def _save_forecast_cache(forecasts: dict[str, dict]) -> None:
    FORECASTS_CACHE.parent.mkdir(parents=True, exist_ok=True)
    FORECASTS_CACHE.write_text(json.dumps(forecasts, indent=2))


def _cache_is_fresh(city_data: dict) -> bool:
    """Return True if the cached forecast was fetched within REFRESH_INTERVAL_HRS."""
    fetched_at = city_data.get("fetched_at")
    if not fetched_at:
        return False
    try:
        fetched = datetime.fromisoformat(fetched_at)
        age = datetime.now(timezone.utc) - fetched
        return age < timedelta(hours=REFRESH_INTERVAL_HRS)
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def snapshot(city_filter: str = None, force: bool = False) -> dict[str, dict]:
    """
    Return AccuWeather forecasts for all cities (or one if city_filter is set).

    Results are cached in data/accuweather_forecasts.json and refreshed
    every REFRESH_INTERVAL_HRS hours. Pass force=True to bypass the cache.

    Returns:
        {
          "Miami":         {"forecast_high_f": 91.0, "forecast_low_f": 74.0, ...},
          "Boston":        {"forecast_high_f": 61.0, "forecast_low_f": 44.0, ...},
          ...
        }
    """
    api_key = _api_key()
    if not api_key:
        log.error("accuweather: no API key found in config.json "
                  "(add 'accuweather_api_key' field)")
        return {}

    cities = list(CITY_SEARCH_QUERIES.keys())
    if city_filter:
        cities = [c for c in cities if c.lower() == city_filter.lower()]

    # Load existing cache
    cache = _load_forecast_cache()

    # Determine which cities need a fresh fetch
    stale = [c for c in cities if force or not _cache_is_fresh(cache.get(c, {}))]

    if stale:
        log.info("accuweather: refreshing %d cities (cache age > %dh)",
                 len(stale), REFRESH_INTERVAL_HRS)

        # Ensure location keys for stale cities
        location_keys = ensure_location_keys(api_key)
        missing_keys  = [c for c in stale if c not in location_keys]
        if missing_keys:
            log.warning("accuweather: no location key for: %s", missing_keys)

        # Parallel fetch
        to_fetch = [(c, location_keys[c]) for c in stale if c in location_keys]

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {
                executor.submit(_fetch_city_forecast, city, key, api_key): city
                for city, key in to_fetch
            }
            for future in as_completed(futures):
                try:
                    city, result = future.result(timeout=15)
                    cache[city] = result
                    if result["error"]:
                        log.warning("accuweather: %s error: %s",
                                    city, result["error"])
                    else:
                        log.debug("accuweather: %s  high=%.1f°F  low=%.1f°F",
                                  city,
                                  result["forecast_high_f"] or 0,
                                  result["forecast_low_f"]  or 0)
                except Exception as e:
                    city = futures[future]
                    log.error("accuweather: future failed for %s: %s", city, e)

        _save_forecast_cache(cache)

    # Return only requested cities
    return {c: cache[c] for c in cities if c in cache}


def get_forecast_high(city: str) -> float | None:
    """Convenience: return today's AccuWeather forecast high for a city."""
    data = snapshot(city_filter=city)
    return data.get(city, {}).get("forecast_high_f")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="AccuWeather forecast fetcher")
    parser.add_argument("--city",  type=str, default=None)
    parser.add_argument("--force", action="store_true",
                        help="Bypass cache and force a fresh fetch")
    args = parser.parse_args()

    forecasts = snapshot(city_filter=args.city, force=args.force)

    if not forecasts:
        print("No forecasts returned — check API key in data/config.json")
    else:
        print(f"\nAccuWeather Forecasts  —  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
        print(f"{'City':<16}  {'High (°F)':>10}  {'Low (°F)':>9}  {'Age':>8}  {'Status':>8}")
        print("-" * 60)
        for city, data in sorted(forecasts.items()):
            high   = data.get("forecast_high_f")
            low    = data.get("forecast_low_f")
            err    = data.get("error")
            fetched = data.get("fetched_at","")
            try:
                age_mins = int((datetime.now(timezone.utc) -
                                datetime.fromisoformat(fetched)).total_seconds() / 60)
                age_str = f"{age_mins}m ago"
            except Exception:
                age_str = "—"
            status = "ERROR" if err else "ok"
            high_s = f"{high:.1f}" if high is not None else "—"
            low_s  = f"{low:.1f}"  if low  is not None else "—"
            print(f"  {city:<14}  {high_s:>10}  {low_s:>9}  {age_str:>8}  {status:>8}")
        print()
