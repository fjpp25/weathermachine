"""
cities.py
---------
Canonical city registry for the Kalshi weather trading system.

This is the single source of truth for all city and station configuration.
All other modules (nws_feed, lowt_observer, kalshi_scanner, city_profiles)
import from here instead of maintaining their own copies.

Each entry contains:
  icao              — NWS ASOS station Kalshi uses for CLI settlement
  lat / lon         — Station coordinates (used for NWS /points grid lookup)
  tz                — IANA timezone name (wall clock — for display and local hour)
  lst_offset        — Fixed UTC offset in hours, no DST (used for CLI day boundary)
  note              — Settlement clarification (e.g. "NOT JFK or LGA")
  station_id        — NOAA NCEI station ID for climate normals (city_profiles.py)
  high_series       — Kalshi series ticker for daily HIGH temperature market
  lowt_series       — Kalshi series ticker for daily LOW temperature market
  trading           — True = actively trading HIGH markets right now
  observe           — True = passively observing (lowt_observer / paper mode)
  trade_start_high  — Earliest local hour for HIGH market NO entries (None = global default)
                      Based on entry_window_analysis.py — update as data accumulates.
                      Rationale: before this hour the NWS forecast is still settling from
                      the overnight model run and NO entry prices are noisy.
  trade_end_high    — Latest local hour for HIGH market entries (None = global default).
                      Set ~2h after median convergence — no new entries after this hour
                      since the market has already resolved and there is no edge left.
  trade_start_lowt  — Earliest local hour for LOWT market entries (None = global default)
                      Not yet set — LOWT markets are observe-only. Will be populated once
                      entry_window_analysis has ≥15 days of LOWT observations.

Convenience views at the bottom of this file:
  TRADING_CITIES      — subset where trading=True
  OBSERVE_CITIES      — subset where observe=True
  NWS_CITIES          — all cities that have lat/lon (used by nws_feed)
  CITIES_WEST_TO_EAST — display order for home tab city grid
"""

# fmt: off
CITIES: dict[str, dict] = {

    # -------------------------------------------------------------------------
    # Core 8 — actively traded HIGH markets
    # -------------------------------------------------------------------------

    "New York": {
        "icao":             "KNYC",
        "lat":              40.7789,
        "lon":              -73.9692,
        "tz":               "America/New_York",
        "lst_offset":       -5,          # UTC-5 always (EST), no DST adjustment
        "note":             "Central Park — NOT JFK or LGA",
        "station_id":       "USW00094728",
        "high_series":      "KXHIGHNY",
        "lowt_series":      "KXLOWTNYC",
        "trading":          True,
        "observe":          True,
        "trade_start_high": 9,           # 3 days: conv@14:00, fcst unstable overnight
        "trade_end_high":    17,          # conv@15:00 median, no edge after
        "trade_start_lowt": None,        # observe-only — not yet calibrated
    },
    "Chicago": {
        "icao":             "KMDW",
        "lat":              41.7868,
        "lon":              -87.7522,
        "tz":               "America/Chicago",
        "lst_offset":       -6,          # UTC-6 always (CST)
        "note":             "Midway Airport — NOT O'Hare",
        "station_id":       "USW00014819",
        "high_series":      "KXHIGHCHI",
        "lowt_series":      "KXLOWTCHI",
        "trading":          True,
        "observe":          True,
        "trade_start_high": 10,          # 3 days: sharp fcst revision at 07:00–08:00 local
        "trade_end_high":    18,          # conv@16:00 median, no edge after
        "trade_start_lowt": None,
    },
    "Miami": {
        "icao":             "KMIA",
        "lat":              25.7959,
        "lon":              -80.2870,
        "tz":               "America/New_York",
        "lst_offset":       -5,
        "note":             "Miami International Airport",
        "station_id":       "USW00012839",
        "high_series":      "KXHIGHMIA",
        "lowt_series":      "KXLOWTMIA",
        "trading":          True,
        "observe":          True,
        "trade_start_high": 9,           # 3 days: conv@13:00, no forecast instability
        "trade_end_high":    18,          # conv@16:00 median, no edge after
        "trade_start_lowt": None,
    },
    "Austin": {
        "icao":             "KAUS",
        "lat":              30.1945,
        "lon":              -97.6699,
        "tz":               "America/Chicago",
        "lst_offset":       -6,
        "note":             "Bergstrom Airport",
        "station_id":       "USW00013904",
        "high_series":      "KXHIGHAUS",
        "lowt_series":      "KXLOWTAUS",
        "trading":          True,
        "observe":          True,
        "trade_start_high": 9,           # 3 days: conv@17:00, overnight instability clears by 09:00
        "trade_end_high":    19,          # conv@17:00 median, no edge after
        "trade_start_lowt": None,
    },
    "Los Angeles": {
        "icao":             "KLAX",
        "lat":              33.9425,
        "lon":              -118.4081,
        "tz":               "America/Los_Angeles",
        "lst_offset":       -8,          # UTC-8 always (PST)
        "note":             "LAX Airport",
        "station_id":       "USW00023174",
        "high_series":      "KXHIGHLAX",
        "lowt_series":      "KXLOWTLAX",
        "trading":          True,        # RE-ENABLED: avg NO 0.94, 100% conv, 4 days obs
        "observe":          True,
        "trade_start_high": 10,          # 4 days: conv@12:00, best signal quality in dataset
        "trade_end_high":    15,          # conv@13:00 median, no edge after
        "trade_start_lowt": None,
    },
    "San Francisco": {
        "icao":             "KSFO",
        "lat":              37.6213,
        "lon":              -122.3790,
        "tz":               "America/Los_Angeles",
        "lst_offset":       -8,
        "note":             "SFO Airport",
        "station_id":       "USW00023234",
        # NOTE: Kalshi uses both KXHIGHTSFO and KXHIGHSFO — scanner handles both
        "high_series":      "KXHIGHTSFO",
        "lowt_series":      "KXLOWTSFO",
        "trading":          True,
        "observe":          True,
        "trade_start_high": 10,          # 2 days: non-convergence risk (marine layer), provisional
        "trade_end_high":    20,          # conv@18:00 median, no edge after
        "trade_start_lowt": None,
    },
    "Denver": {
        "icao":             "KDEN",
        "lat":              39.8561,
        "lon":              -104.6737,
        "tz":               "America/Denver",
        "lst_offset":       -7,          # UTC-7 always (MST)
        "note":             "Denver International Airport",
        "station_id":       "USW00003017",
        "high_series":      "KXHIGHDEN",
        "lowt_series":      "KXLOWTDEN",
        "trading":          True,
        "observe":          True,
        "trade_start_high": 9,           # 3 days: conv@16:00, no significant instability
        "trade_end_high":    18,          # conv@16:00 median, no edge after
        "trade_start_lowt": None,
    },
    "Philadelphia": {
        "icao":             "KPHL",
        "lat":              39.8721,
        "lon":              -75.2411,
        "tz":               "America/New_York",
        "lst_offset":       -5,
        "note":             "Philadelphia International Airport",
        "station_id":       "USW00013739",
        "high_series":      "KXHIGHPHIL",
        "lowt_series":      "KXLOWTPHIL",
        "trading":          True,        # RE-ENABLED: 100% conv rate, 4 days obs
        "observe":          True,
        "trade_start_high": 9,           # 4 days: conv@16:30, same pattern as New York
        "trade_end_high":    19,          # conv@17:00 median, no edge after
        "trade_start_lowt": None,
    },

    # -------------------------------------------------------------------------
    # Extended cities — mix of enabled and paused based on observation data
    # -------------------------------------------------------------------------

    "Atlanta": {
        "icao":             "KATL",
        "lat":              33.6407,
        "lon":              -84.4277,
        "tz":               "America/New_York",
        "lst_offset":       -5,
        "note":             "Hartsfield-Jackson Atlanta International",
        "station_id":       "USW00013874",
        "high_series":      "KXHIGHTATL",
        "lowt_series":      "KXLOWTATL",
        "trading":          True,        # ENABLED: 100% conv, 2 days obs
        "observe":          True,
        "trade_start_high": 9,           # 2 days: conv@17:00, stable NO from ~08:00
        "trade_end_high":    19,          # conv@17:00 median, no edge after
        "trade_start_lowt": None,
    },
    "Boston": {
        "icao":             "KBOS",
        "lat":              42.3606,
        "lon":              -71.0097,
        "tz":               "America/New_York",
        "lst_offset":       -5,
        "note":             "Boston Logan International",
        "station_id":       "USW00014739",
        "high_series":      "KXHIGHTBOS",
        "lowt_series":      "KXLOWTBOS",
        "trading":          True,        # ENABLED: 100% conv, 2 days obs
        "observe":          True,
        "trade_start_high": 9,           # 2 days: conv@16:00, stable NO from ~08:00
        "trade_end_high":    18,          # conv@16:00 median, no edge after
        "trade_start_lowt": None,
    },
    "Washington DC": {
        "icao":             "KDCA",
        "lat":              38.8521,
        "lon":              -77.0377,
        "tz":               "America/New_York",
        "lst_offset":       -5,
        "note":             "Reagan National Airport",
        "station_id":       "USW00013743",
        "high_series":      "KXHIGHTDC",
        "lowt_series":      "KXLOWTDC",
        "trading":          True,        # ENABLED: 50% conv, 2 days — cautious
        "observe":          True,
        "trade_start_high": 10,          # 2 days: conv@16:00, later start for safety
        "trade_end_high":    18,          # conv@16:00 median, no edge after
        "trade_start_lowt": None,
    },
    "Houston": {
        "icao":             "KHOU",
        "lat":              29.6454,
        "lon":              -95.2789,
        "tz":               "America/Chicago",
        "lst_offset":       -6,
        "note":             "William P. Hobby Airport",
        "station_id":       "USW00012918",
        "high_series":      "KXHIGHTHOU",
        "lowt_series":      "KXLOWTHOU",
        "trading":          True,        # ENABLED: 50% conv, 2 days — cautious
        "observe":          True,
        "trade_start_high": 10,          # 2 days: conv@16:00, later start for safety
        "trade_end_high":    18,          # conv@16:00 median, no edge after
        "trade_start_lowt": None,
    },
    "Phoenix": {
        "icao":             "KPHX",
        "lat":              33.4343,
        "lon":              -112.0078,
        "tz":               "America/Phoenix",
        "lst_offset":       -7,          # UTC-7 always (MST, no DST in Arizona)
        "note":             "Phoenix Sky Harbor International",
        "station_id":       "USW00023183",
        "high_series":      "KXHIGHTPHX",
        "lowt_series":      "KXLOWTPHX",
        "trading":          True,        # ENABLED: 50% conv, 2 days — cautious
        "observe":          True,
        "trade_start_high": 10,          # 2 days: conv@14:00, non-conv risk
        "trade_end_high":    17,          # conv@15:00 median, no edge after
        "trade_start_lowt": None,
    },
    "Las Vegas": {
        "icao":             "KLAS",
        "lat":              36.0800,
        "lon":              -115.1522,
        "tz":               "America/Los_Angeles",
        "lst_offset":       -8,
        "note":             "Harry Reid International Airport",
        "station_id":       "USW00023169",
        "high_series":      "KXHIGHTLV",
        "lowt_series":      "KXLOWTLV",
        "trading":          False,       # PAUSED: 0% convergence over 2 days
        "observe":          True,
        "trade_start_high": None,        # non-converging — keep paused
        "trade_end_high":    None,        # paused — no close needed
        "trade_start_lowt": None,
    },
    "Dallas": {
        "icao":             "KDFW",
        "lat":              32.8998,
        "lon":              -97.0403,
        "tz":               "America/Chicago",
        "lst_offset":       -6,
        "note":             "Dallas/Fort Worth International",
        "station_id":       "USW00003927",
        "high_series":      "KXHIGHTDAL",
        "lowt_series":      "KXLOWTDAL",
        "trading":          True,        # ENABLED: 100% conv, 2 days obs
        "observe":          True,
        "trade_start_high": 9,           # 2 days: conv@15:30, tight spreads
        "trade_end_high":    18,          # conv@16:00 median, no edge after
        "trade_start_lowt": None,
    },
    "San Antonio": {
        "icao":             "KSAT",
        "lat":              29.5337,
        "lon":              -98.4698,
        "tz":               "America/Chicago",
        "lst_offset":       -6,
        "note":             "San Antonio International",
        "station_id":       "USW00012921",
        "high_series":      "KXHIGHTSATX",
        "lowt_series":      "KXLOWTSATX",
        "trading":          False,       # PAUSED: 50% conv, wider spreads
        "observe":          True,
        "trade_start_high": None,        # insufficient data
        "trade_end_high":    None,        # paused — no close needed
        "trade_start_lowt": None,
    },
    "Seattle": {
        "icao":             "KSEA",
        "lat":              47.4502,
        "lon":              -122.3088,
        "tz":               "America/Los_Angeles",
        "lst_offset":       -8,
        "note":             "Seattle-Tacoma International",
        "station_id":       "USW00024233",
        "high_series":      "KXHIGHTSEA",
        "lowt_series":      "KXLOWTSEA",
        "trading":          False,       # PAUSED: 0% convergence over 2 days
        "observe":          True,
        "trade_start_high": None,        # non-converging
        "trade_end_high":    None,        # paused — no close needed
        "trade_start_lowt": None,
    },
    "New Orleans": {
        "icao":             "KMSY",
        "lat":              29.9934,
        "lon":              -90.2580,
        "tz":               "America/Chicago",
        "lst_offset":       -6,
        "note":             "Louis Armstrong New Orleans International",
        "station_id":       "USW00012916",
        "high_series":      "KXHIGHTNOLA",
        "lowt_series":      "KXLOWTNOLA",
        "trading":          True,        # ENABLED: 50% conv, 2 days — cautious
        "observe":          True,
        "trade_start_high": 10,          # 2 days: conv@14:00, later start for safety
        "trade_end_high":    17,          # conv@15:00 median, no edge after
        "trade_start_lowt": None,
    },
    "Minneapolis": {
        "icao":             "KMSP",
        "lat":              44.8848,
        "lon":              -93.2223,
        "tz":               "America/Chicago",
        "lst_offset":       -6,
        "note":             "Minneapolis-St. Paul International",
        "station_id":       "USW00014922",
        "high_series":      "KXHIGHTMIN",
        "lowt_series":      "KXLOWTMIN",
        "trading":          True,        # ENABLED: 100% conv, 2 days obs
        "observe":          True,
        "trade_start_high": 9,           # 2 days: conv@14:00, strong early NO quality
        "trade_end_high":    17,          # conv@15:00 median, no edge after
        "trade_start_lowt": None,
    },
    "Oklahoma City": {
        "icao":             "KOKC",
        "lat":              35.3931,
        "lon":              -97.6007,
        "tz":               "America/Chicago",
        "lst_offset":       -6,
        "note":             "Will Rogers World Airport",
        "station_id":       "USW00013967",
        "high_series":      "KXHIGHTOKC",
        "lowt_series":      "KXLOWTOKC",
        "trading":          True,        # ENABLED: 100% conv, 3 days obs
        "observe":          True,
        "trade_start_high": 9,           # 3 days: conv@17:00, 80% pct_80 at peak
        "trade_end_high":    19,          # conv@17:00 median, no edge after
        "trade_start_lowt": None,
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


# ---------------------------------------------------------------------------
# West-to-east display order — for the app home page city card grid.
# Ordered by approximate longitude so cards read like a map left→right.
# Within each timezone, ordered roughly south→north for visual coherence.
# ---------------------------------------------------------------------------

CITIES_WEST_TO_EAST: list[str] = [
    # UTC-8  Pacific
    "Los Angeles",
    "San Francisco",
    "Las Vegas",
    "Seattle",
    # UTC-7  Mountain
    "Phoenix",
    "Denver",
    # UTC-6  Central
    "San Antonio",
    "Austin",
    "Dallas",
    "Houston",
    "New Orleans",
    "Oklahoma City",
    "Minneapolis",
    "Chicago",
    # UTC-5  Eastern
    "Miami",
    "Atlanta",
    "Washington DC",
    "Philadelphia",
    "New York",
    "Boston",
]


if __name__ == "__main__":
    print(f"{'City':<16} {'ICAO':<6} {'TZ':<28} {'LST':>4} {'Trading':>8} "
          f"{'HighStart':>9}  {'High series':<14} {'Low series'}")
    print("-" * 110)
    for city, m in CITIES.items():
        start = str(m.get("trade_start_high")) if m.get("trade_start_high") is not None else "—"
        print(
            f"{city:<16} {m['icao']:<6} {m['tz']:<28} {m['lst_offset']:>4} "
            f"{'YES' if m.get('trading') else 'no':>8}  "
            f"{start:>9}  "
            f"{m.get('high_series','—'):<14} {m.get('lowt_series','—')}"
        )
    print(f"\n{len(TRADING_CITIES)} trading  |  {len(OBSERVE_CITIES)} observing  |  {len(CITIES)} total")
