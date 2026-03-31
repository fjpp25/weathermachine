"""
city_profiles.py
----------------
Fetches and caches 30-year NOAA Climate Normals (1991-2020) for the 8 cities
whose temperature markets are listed on Kalshi.

Each city is mapped to its exact NWS settlement station — the same station
Kalshi uses to resolve temperature markets via the CLI (Climatological Report).

Data fetched per station per month:
  - MLY-TMAX-NORMAL   : Mean daily maximum temperature (°F)
  - MLY-TMIN-NORMAL   : Mean daily minimum temperature (°F)
  - MLY-TMAX-STDDEV   : Std deviation of daily max (measure of variability)
  - MLY-DUTR-NORMAL   : Mean diurnal temperature range (max - min)

Output: data/city_profiles.json  (cached, re-fetch only if missing or stale)

Usage:
  python city_profiles.py                  # fetch and cache all cities
  python city_profiles.py --show           # pretty-print cached profiles
"""

import json
import time
import argparse
from pathlib import Path
from datetime import datetime

try:
    import requests
except ImportError:
    raise SystemExit("Please install requests:  pip install requests")

DEBUG = False   # set to True (or pass --debug) to print raw API responses

# ---------------------------------------------------------------------------
# Station map: city -> NWS ASOS station used by Kalshi for settlement
# ---------------------------------------------------------------------------
CITIES = {
    "New York":      {"station_id": "USW00094728", "icao": "KNYC",  "tz": "America/New_York",    "note": "Central Park — NOT JFK or LGA"},
    "Chicago":       {"station_id": "USW00014819", "icao": "KMDW",  "tz": "America/Chicago",     "note": "Midway Airport — NOT O'Hare"},
    "Miami":         {"station_id": "USW00012839", "icao": "KMIA",  "tz": "America/New_York",    "note": "Miami International Airport"},
    "Austin":        {"station_id": "USW00013904", "icao": "KAUS",  "tz": "America/Chicago",     "note": "Bergstrom Airport"},
    "Los Angeles":   {"station_id": "USW00023174", "icao": "KLAX",  "tz": "America/Los_Angeles", "note": "LAX Airport"},
    "San Francisco": {"station_id": "USW00023234", "icao": "KSFO",  "tz": "America/Los_Angeles", "note": "SFO Airport"},
    "Denver":        {"station_id": "USW00003017", "icao": "KDEN",  "tz": "America/Denver",      "note": "Denver International Airport"},
    "Philadelphia":  {"station_id": "USW00013739", "icao": "KPHL",  "tz": "America/New_York",    "note": "Philadelphia International Airport"},
}

# Data types we want from the normals API
DATA_TYPES = [
    "MLY-TMAX-NORMAL",   # mean daily max per month
    "MLY-TMIN-NORMAL",   # mean daily min per month
    "MLY-TMAX-STDDEV",   # std dev of daily max (forecast uncertainty proxy)
    "MLY-DUTR-NORMAL",   # mean diurnal range (typical daily swing)
]

MONTHS = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]

CACHE_FILE = Path("data/city_profiles.json")
API_BASE   = "https://www.ncei.noaa.gov/access/services/data/v1"


# ---------------------------------------------------------------------------
# API helpers
# ---------------------------------------------------------------------------

def fetch_normals(station_id: str) -> dict:
    """
    Fetch monthly climate normals for a single station from NOAA NCEI.
    Returns a dict keyed by month index (1-12), each containing the data types.

    The NOAA API returns a JSON array where each element is one row (one month).
    The DATE field for the normals dataset is formatted as "YYYY-MM-01" where
    the month encodes which calendar month the normal applies to.
    Field names match the requested dataTypes exactly.
    """
    params = {
        "dataset":              "normals-monthly-1991-2020",
        "stations":             station_id,
        "dataTypes":            ",".join(DATA_TYPES),
        "format":               "json",
        "units":                "standard",      # Fahrenheit
        "includeStationName":   "true",
    }

    resp = requests.get(API_BASE, params=params, timeout=30)
    resp.raise_for_status()

    raw = resp.json()

    if DEBUG:
        print("\n--- RAW API RESPONSE (first 3 rows) ---")
        print(json.dumps(raw[:3] if isinstance(raw, list) else raw, indent=2))
        print("--- END RAW ---\n")

    # The API can return either:
    #   A) A plain list of row dicts  [ {"DATE": "...", "MLY-TMAX-NORMAL": "...", ...}, ... ]
    #   B) A dict with a "results" key containing that list
    if isinstance(raw, dict):
        rows = raw.get("results", raw.get("data", []))
    elif isinstance(raw, list):
        rows = raw
    else:
        raise ValueError(f"Unexpected response type: {type(raw)}")

    if not rows:
        raise ValueError(f"No data returned for station {station_id}")

    monthly = {}
    for row in rows:
        # DATE can be "0001-01-01" (month encoded in MM) or "1991-01-01" etc.
        date_str = row.get("DATE", "")
        if not date_str:
            # Some rows may use lowercase keys
            date_str = row.get("date", "")
        if not date_str:
            continue

        # DATE is just "01" through "12" (not "YYYY-MM-DD" as the docs imply)
        try:
            month = int(date_str.strip())
        except ValueError:
            continue

        if not (1 <= month <= 12):
            continue

        monthly[month] = {
            "tmax_normal":   safe_float(row.get("MLY-TMAX-NORMAL")),
            "tmin_normal":   safe_float(row.get("MLY-TMIN-NORMAL")),
            "tmax_stddev":   safe_float(row.get("MLY-TMAX-STDDEV")),
            "diurnal_range": safe_float(row.get("MLY-DUTR-NORMAL")),
        }

    if not monthly:
        raise ValueError(
            f"Parsed 0 monthly rows from {len(rows)} API rows for {station_id}. "
            f"Re-run with DEBUG=True to inspect the raw response."
        )

    return monthly


def safe_float(val):
    """Convert NOAA value to float, return None if missing/trace."""
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# Derived metrics
# ---------------------------------------------------------------------------

def enrich_monthly(monthly: dict) -> dict:
    """
    Add derived fields useful for bracket probability estimation.

    afternoon_climb:
        Typical temperature gain from overnight low to afternoon peak.
        Proxy for "how much can the temperature still rise from current reading?"

    bracket_difficulty:
        Kalshi brackets are 2°F wide. Expressed in units of tmax_stddev,
        this tells us how "fine-grained" the market is relative to natural
        variability. Lower = harder to predict exact bracket.

    Keys are stored as strings ("1"–"12") so they survive JSON round-trips.
    """
    enriched = {}
    for month, d in monthly.items():
        d = d.copy()
        month_int = int(month)   # handle both int and string keys from caller

        if d["tmax_normal"] is not None and d["tmin_normal"] is not None:
            d["afternoon_climb"] = round(d["tmax_normal"] - d["tmin_normal"], 1)
        else:
            d["afternoon_climb"] = None

        if d["tmax_stddev"] is not None and d["tmax_stddev"] > 0:
            d["bracket_difficulty"] = round(2.0 / d["tmax_stddev"], 3)
            # < 0.5  → bracket is smaller than 1 stddev → hard to predict
            # > 1.0  → bracket covers more than 1 stddev → easier
        else:
            d["bracket_difficulty"] = None

        d["month_name"] = MONTHS[month_int - 1]
        enriched[str(month_int)] = d   # always store as string key

    return enriched


# ---------------------------------------------------------------------------
# Main fetch + cache logic
# ---------------------------------------------------------------------------

def build_profiles(force_refresh: bool = False) -> dict:
    CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)

    if CACHE_FILE.exists() and not force_refresh:
        print(f"Loading cached profiles from {CACHE_FILE}")
        with open(CACHE_FILE) as f:
            return json.load(f)

    profiles = {}
    for city, meta in CITIES.items():
        print(f"Fetching normals for {city} ({meta['icao']})...", end=" ", flush=True)
        try:
            monthly_raw = fetch_normals(meta["station_id"])
            monthly     = enrich_monthly(monthly_raw)
            profiles[city] = {
                "station_id": meta["station_id"],
                "icao":       meta["icao"],
                "timezone":   meta["tz"],
                "note":       meta["note"],
                "monthly":    monthly,
                "fetched_at": datetime.utcnow().isoformat(),
            }
            print("OK")
        except Exception as e:
            print(f"FAILED: {e}")
            profiles[city] = {"error": str(e), **meta}

        time.sleep(0.5)   # be polite to the API

    with open(CACHE_FILE, "w") as f:
        json.dump(profiles, f, indent=2)
    print(f"\nProfiles saved to {CACHE_FILE}")

    return profiles


# ---------------------------------------------------------------------------
# CLI display helper
# ---------------------------------------------------------------------------

def show_profiles(profiles: dict):
    for city, data in profiles.items():
        if "error" in data:
            print(f"\n{'='*60}\n{city}  ← ERROR: {data['error']}")
            continue

        print(f"\n{'='*60}")
        print(f"{city}  |  {data['icao']}  |  {data['note']}")
        print(f"{'='*60}")
        print(f"{'Month':<6} {'TMax':>6} {'TMin':>6} {'Swing':>6} {'StdDev':>7} {'Difficulty':>11}")
        print(f"{'-'*50}")

        for m in range(1, 13):
            d = data["monthly"].get(str(m), {})
            print(
                f"{d.get('month_name','?'):<6} "
                f"{fmt(d.get('tmax_normal')):>6} "
                f"{fmt(d.get('tmin_normal')):>6} "
                f"{fmt(d.get('afternoon_climb')):>6} "
                f"{fmt(d.get('tmax_stddev')):>7} "
                f"{fmt(d.get('bracket_difficulty')):>11}"
            )


def fmt(val):
    return f"{val:.1f}" if val is not None else "  N/A"


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch NOAA climate normals for Kalshi weather markets")
    parser.add_argument("--refresh", action="store_true", help="Force re-fetch even if cache exists")
    parser.add_argument("--show",    action="store_true", help="Print cached profiles to terminal")
    parser.add_argument("--debug",   action="store_true", help="Print raw API responses for diagnosis")
    args = parser.parse_args()

    if args.debug:
        import city_profiles as _self
        _self.DEBUG = True
        # Also monkey-patch the module-level DEBUG in this script
        DEBUG = True

    profiles = build_profiles(force_refresh=args.refresh or args.debug)

    if args.show or args.debug:
        show_profiles(profiles)
    else:
        print("\nRun with --show to display the profiles.")
        print(f"Data is at: {CACHE_FILE.resolve()}")
