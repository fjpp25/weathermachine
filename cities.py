"""
cities.py
---------
Canonical city registry for the Kalshi weather trading system.

This is the single source of truth for all city and station configuration.
All other modules (nws_feed, lowt_observer, kalshi_scanner, city_profiles)
import from here instead of maintaining their own copies.

Each entry contains:
  icao         — NWS ASOS station Kalshi uses for CLI settlement
  lat / lon    — Station coordinates (used for NWS /points grid lookup)
  tz           — IANA timezone name (wall clock — for display and local hour)
  lst_offset   — Fixed UTC offset in hours, no DST (used for CLI day boundary)
  note         — Settlement clarification (e.g. "NOT JFK or LGA")
  station_id   — NOAA NCEI station ID for climate normals (city_profiles.py)
  high_series  — Kalshi series ticker for daily HIGH temperature market
  lowt_series  — Kalshi series ticker for daily LOW temperature market
  trading      — True = actively trading HIGH markets right now
  observe      — True = passively observing (lowt_observer / paper mode)

Convenience views at the bottom of this file:
  TRADING_CITIES  — subset where trading=True
  OBSERVE_CITIES  — subset where observe=True
  NWS_CITIES      — all cities that have lat/lon (used by nws_feed)
"""

# fmt: off
CITIES: dict[str, dict] = {

    # -------------------------------------------------------------------------
    # Core 8 — actively traded HIGH markets
    # -------------------------------------------------------------------------

    "New York": {
        "icao":         "KNYC",
        "lat":          40.7789,
        "lon":          -73.9692,
        "tz":           "America/New_York",
        "lst_offset":   -5,          # UTC-5 always (EST), no DST adjustment
        "note":         "Central Park — NOT JFK or LGA",
        "station_id":   "USW00094728",
        "high_series":  "KXHIGHNY",
        "lowt_series":  "KXLOWTNYC",
        "trading":      True,
        "observe":      True,
    },
    "Chicago": {
        "icao":         "KMDW",
        "lat":          41.7868,
        "lon":          -87.7522,
        "tz":           "America/Chicago",
        "lst_offset":   -6,          # UTC-6 always (CST)
        "note":         "Midway Airport — NOT O'Hare",
        "station_id":   "USW00014819",
        "high_series":  "KXHIGHCHI",
        "lowt_series":  "KXLOWTCHI",
        "trading":      True,
        "observe":      True,
    },
    "Miami": {
        "icao":         "KMIA",
        "lat":          25.7959,
        "lon":          -80.2870,
        "tz":           "America/New_York",
        "lst_offset":   -5,
        "note":         "Miami International Airport",
        "station_id":   "USW00012839",
        "high_series":  "KXHIGHMIA",
        "lowt_series":  "KXLOWTMIA",
        "trading":      True,
        "observe":      True,
    },
    "Austin": {
        "icao":         "KAUS",
        "lat":          30.1945,
        "lon":          -97.6699,
        "tz":           "America/Chicago",
        "lst_offset":   -6,
        "note":         "Bergstrom Airport",
        "station_id":   "USW00013904",
        "high_series":  "KXHIGHAUS",
        "lowt_series":  "KXLOWTAUS",
        "trading":      True,
        "observe":      True,
    },
    "Los Angeles": {
        "icao":         "KLAX",
        "lat":          33.9425,
        "lon":          -118.4081,
        "tz":           "America/Los_Angeles",
        "lst_offset":   -8,          # UTC-8 always (PST)
        "note":         "LAX Airport",
        "station_id":   "USW00023174",
        "high_series":  "KXHIGHLAX",
        "lowt_series":  "KXLOWTLAX",
        "trading":      False,       # PAUSED: 50% WR, -$1.79 across 8 trades
        "observe":      True,
    },
    "San Francisco": {
        "icao":         "KSFO",
        "lat":          37.6213,
        "lon":          -122.3790,
        "tz":           "America/Los_Angeles",
        "lst_offset":   -8,
        "note":         "SFO Airport",
        "station_id":   "USW00023234",
        # NOTE: Kalshi uses both KXHIGHTSFO and KXHIGHSFO — scanner handles both
        "high_series":  "KXHIGHSFO",
        "lowt_series":  "KXLOWTSFO",
        "trading":      True,
        "observe":      True,
    },
    "Denver": {
        "icao":         "KDEN",
        "lat":          39.8561,
        "lon":          -104.6737,
        "tz":           "America/Denver",
        "lst_offset":   -7,          # UTC-7 always (MST)
        "note":         "Denver International Airport",
        "station_id":   "USW00003017",
        "high_series":  "KXHIGHDEN",
        "lowt_series":  "KXLOWTDEN",
        "trading":      True,
        "observe":      True,
    },
    "Philadelphia": {
        "icao":         "KPHL",
        "lat":          39.8721,
        "lon":          -75.2411,
        "tz":           "America/New_York",
        "lst_offset":   -5,
        "note":         "Philadelphia International Airport",
        "station_id":   "USW00013739",
        "high_series":  "KXHIGHPHIL",
        "lowt_series":  "KXLOWTPHIL",
        "trading":      False,       # PAUSED: 0% WR, -$2.15 across 2 trades
        "observe":      True,
    },

    # -------------------------------------------------------------------------
    # Extended — observation / future trading candidates
    # -------------------------------------------------------------------------

    "Atlanta": {
        "icao":         "KATL",
        "lat":          33.6407,
        "lon":          -84.4277,
        "tz":           "America/New_York",
        "lst_offset":   -5,
        "note":         "Hartsfield-Jackson Atlanta International",
        "station_id":   "USW00013874",
        "high_series":  "KXHIGHTATL",
        "lowt_series":  "KXLOWTATL",
        "trading":      False,
        "observe":      True,
    },
    "Boston": {
        "icao":         "KBOS",
        "lat":          42.3606,
        "lon":          -71.0097,
        "tz":           "America/New_York",
        "lst_offset":   -5,
        "note":         "Boston Logan International",
        "station_id":   "USW00014739",
        "high_series":  "KXHIGHTBOS",
        "lowt_series":  "KXLOWTBOS",
        "trading":      False,
        "observe":      True,
    },
    "Washington DC": {
        "icao":         "KDCA",
        "lat":          38.8521,
        "lon":          -77.0377,
        "tz":           "America/New_York",
        "lst_offset":   -5,
        "note":         "Reagan National Airport",
        "station_id":   "USW00013743",
        "high_series":  "KXHIGHTDC",
        "lowt_series":  "KXLOWTDC",
        "trading":      False,
        "observe":      True,
    },
    "Houston": {
        "icao":         "KHOU",
        "lat":          29.6454,
        "lon":          -95.2789,
        "tz":           "America/Chicago",
        "lst_offset":   -6,
        "note":         "William P. Hobby Airport",
        "station_id":   "USW00012918",
        "high_series":  "KXHIGHTHOU",
        "lowt_series":  "KXLOWTHOU",
        "trading":      False,
        "observe":      True,
    },
    "Phoenix": {
        "icao":         "KPHX",
        "lat":          33.4343,
        "lon":          -112.0078,
        "tz":           "America/Phoenix",
        "lst_offset":   -7,          # UTC-7 always (MST, no DST in Arizona)
        "note":         "Phoenix Sky Harbor International",
        "station_id":   "USW00023183",
        "high_series":  "KXHIGHTPHX",
        "lowt_series":  "KXLOWTPHX",
        "trading":      False,
        "observe":      True,
    },
    "Las Vegas": {
        "icao":         "KLAS",
        "lat":          36.0800,
        "lon":          -115.1522,
        "tz":           "America/Los_Angeles",
        "lst_offset":   -8,
        "note":         "Harry Reid International Airport",
        "station_id":   "USW00023169",
        "high_series":  "KXHIGHTLV",
        "lowt_series":  "KXLOWTLV",
        "trading":      False,
        "observe":      True,
    },
    "Dallas": {
        "icao":         "KDFW",
        "lat":          32.8998,
        "lon":          -97.0403,
        "tz":           "America/Chicago",
        "lst_offset":   -6,
        "note":         "Dallas/Fort Worth International",
        "station_id":   "USW00003927",
        "high_series":  "KXHIGHTDAL",
        "lowt_series":  "KXLOWTDFW",
        "trading":      False,
        "observe":      True,
    },
    "San Antonio": {
        "icao":         "KSAT",
        "lat":          29.5337,
        "lon":          -98.4698,
        "tz":           "America/Chicago",
        "lst_offset":   -6,
        "note":         "San Antonio International",
        "station_id":   "USW00012921",
        "high_series":  "KXHIGHTSATX",
        "lowt_series":  "KXLOWTSAT",
        "trading":      False,
        "observe":      True,
    },
    "Seattle": {
        "icao":         "KSEA",
        "lat":          47.4502,
        "lon":          -122.3088,
        "tz":           "America/Los_Angeles",
        "lst_offset":   -8,
        "note":         "Seattle-Tacoma International",
        "station_id":   "USW00024233",
        "high_series":  "KXHIGHTSEA",
        "lowt_series":  "KXLOWTSEA",
        "trading":      False,
        "observe":      True,
    },
    "New Orleans": {
        "icao":         "KMSY",
        "lat":          29.9934,
        "lon":          -90.2580,
        "tz":           "America/Chicago",
        "lst_offset":   -6,
        "note":         "Louis Armstrong New Orleans International",
        "station_id":   "USW00012916",
        "high_series":  "KXHIGHTNOLA",
        "lowt_series":  "KXLOWTMSY",
        "trading":      False,
        "observe":      True,
    },
    "Minneapolis": {
        "icao":         "KMSP",
        "lat":          44.8848,
        "lon":          -93.2223,
        "tz":           "America/Chicago",
        "lst_offset":   -6,
        "note":         "Minneapolis-St. Paul International",
        "station_id":   "USW00014922",
        "high_series":  "KXHIGHTMIN",
        "lowt_series":  "KXLOWTMSP",
        "trading":      False,
        "observe":      True,
    },
    "Oklahoma City": {
        "icao":         "KOKC",
        "lat":          35.3931,
        "lon":          -97.6007,
        "tz":           "America/Chicago",
        "lst_offset":   -6,
        "note":         "Will Rogers World Airport",
        "station_id":   "USW00013967",
        "high_series":  "KXHIGHTOKC",
        "lowt_series":  "KXLOWTTOKC",
        "trading":      False,
        "observe":      True,
    },
}
# fmt: on

# ---------------------------------------------------------------------------
# Convenience views — import these instead of filtering manually
# ---------------------------------------------------------------------------

# Cities with active HIGH trading (trading=True)
TRADING_CITIES: dict[str, dict] = {
    k: v for k, v in CITIES.items() if v.get("trading")
}

# Cities being passively observed (observe=True) — superset of TRADING_CITIES
OBSERVE_CITIES: dict[str, dict] = {
    k: v for k, v in CITIES.items() if v.get("observe")
}

# All cities with NWS coordinates — used by nws_feed.snapshot()
NWS_CITIES: dict[str, dict] = {
    k: v for k, v in CITIES.items() if v.get("lat") is not None
}

# Cities with NOAA climate normals station IDs — used by city_profiles.py
PROFILE_CITIES: dict[str, dict] = {
    k: v for k, v in CITIES.items() if v.get("station_id")
}


# ---------------------------------------------------------------------------
# Reverse lookup: Kalshi series ticker → city name
# Used by app.py and anywhere a ticker prefix needs to resolve to a city.
# ---------------------------------------------------------------------------

def build_series_map() -> dict[str, str]:
    """Return {series_ticker: city_name} for all configured series."""
    result = {}
    for city, meta in CITIES.items():
        if meta.get("high_series"):
            result[meta["high_series"]] = city
        if meta.get("lowt_series"):
            result[meta["lowt_series"]] = city
    return result

SERIES_TO_CITY: dict[str, str] = build_series_map()


if __name__ == "__main__":
    print(f"{'City':<16} {'ICAO':<6} {'TZ':<28} {'LST':>4} {'Trading':>8} {'High series':<14} {'Low series'}")
    print("-" * 100)
    for city, m in CITIES.items():
        print(
            f"{city:<16} {m['icao']:<6} {m['tz']:<28} {m['lst_offset']:>4} "
            f"{'YES' if m.get('trading') else 'no':>8}  "
            f"{m.get('high_series','—'):<14} {m.get('lowt_series','—')}"
        )
    print(f"\n{len(TRADING_CITIES)} trading  |  {len(OBSERVE_CITIES)} observing  |  {len(CITIES)} total")
