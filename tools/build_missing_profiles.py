"""
build_missing_profiles.py
-------------------------
Adds the 12 missing cities to data/city_profiles.json using
NOAA 1991-2020 Climate Normals.

Only adds cities that are not already present — safe to re-run.

Usage:
  python build_missing_profiles.py
  python build_missing_profiles.py --dry-run
"""

import json
import argparse
from datetime import datetime, timezone
from pathlib import Path

PROFILES_PATH = Path("data/city_profiles.json")

# ---------------------------------------------------------------------------
# NOAA 1991-2020 Climate Normals for the 12 missing cities
# tmax/tmin in °F, stddev estimated from historical day-to-day variability
# diurnal_range = tmax_normal - tmin_normal
# bracket_difficulty = 2.0 / tmax_stddev
# ---------------------------------------------------------------------------

MISSING_CITIES = {
    "Boston": {
        "station_id": "USW00014739",
        "icao":       "KBOS",
        "timezone":   "America/New_York",
        "note":       "Logan International Airport",
        "monthly": {
            "1":  {"tmax_normal": 37.0, "tmin_normal": 22.7, "tmax_stddev": 5.2,  "month_name": "Jan"},
            "2":  {"tmax_normal": 39.8, "tmin_normal": 24.9, "tmax_stddev": 5.8,  "month_name": "Feb"},
            "3":  {"tmax_normal": 46.9, "tmin_normal": 31.6, "tmax_stddev": 4.5,  "month_name": "Mar"},
            "4":  {"tmax_normal": 57.5, "tmin_normal": 41.1, "tmax_stddev": 3.5,  "month_name": "Apr"},
            "5":  {"tmax_normal": 67.1, "tmin_normal": 51.0, "tmax_stddev": 3.3,  "month_name": "May"},
            "6":  {"tmax_normal": 76.5, "tmin_normal": 60.9, "tmax_stddev": 2.5,  "month_name": "Jun"},
            "7":  {"tmax_normal": 82.1, "tmin_normal": 67.3, "tmax_stddev": 2.0,  "month_name": "Jul"},
            "8":  {"tmax_normal": 80.9, "tmin_normal": 66.2, "tmax_stddev": 2.3,  "month_name": "Aug"},
            "9":  {"tmax_normal": 73.8, "tmin_normal": 58.7, "tmax_stddev": 2.8,  "month_name": "Sep"},
            "10": {"tmax_normal": 63.2, "tmin_normal": 47.7, "tmax_stddev": 3.5,  "month_name": "Oct"},
            "11": {"tmax_normal": 52.7, "tmin_normal": 38.4, "tmax_stddev": 4.8,  "month_name": "Nov"},
            "12": {"tmax_normal": 41.6, "tmin_normal": 28.1, "tmax_stddev": 5.5,  "month_name": "Dec"},
        },
    },
    "Las Vegas": {
        "station_id": "USW00023169",
        "icao":       "KLAS",
        "timezone":   "America/Los_Angeles",
        "note":       "Harry Reid International Airport",
        "monthly": {
            "1":  {"tmax_normal": 57.6, "tmin_normal": 36.4, "tmax_stddev": 4.5,  "month_name": "Jan"},
            "2":  {"tmax_normal": 63.3, "tmin_normal": 41.4, "tmax_stddev": 4.8,  "month_name": "Feb"},
            "3":  {"tmax_normal": 71.7, "tmin_normal": 48.4, "tmax_stddev": 4.2,  "month_name": "Mar"},
            "4":  {"tmax_normal": 81.2, "tmin_normal": 57.0, "tmax_stddev": 3.8,  "month_name": "Apr"},
            "5":  {"tmax_normal": 90.7, "tmin_normal": 65.8, "tmax_stddev": 3.5,  "month_name": "May"},
            "6":  {"tmax_normal": 101.8,"tmin_normal": 76.0, "tmax_stddev": 3.0,  "month_name": "Jun"},
            "7":  {"tmax_normal": 106.6,"tmin_normal": 81.4, "tmax_stddev": 2.2,  "month_name": "Jul"},
            "8":  {"tmax_normal": 103.9,"tmin_normal": 79.4, "tmax_stddev": 2.5,  "month_name": "Aug"},
            "9":  {"tmax_normal": 95.9, "tmin_normal": 71.6, "tmax_stddev": 3.2,  "month_name": "Sep"},
            "10": {"tmax_normal": 82.6, "tmin_normal": 59.0, "tmax_stddev": 3.8,  "month_name": "Oct"},
            "11": {"tmax_normal": 67.5, "tmin_normal": 45.9, "tmax_stddev": 4.5,  "month_name": "Nov"},
            "12": {"tmax_normal": 57.2, "tmin_normal": 36.4, "tmax_stddev": 4.5,  "month_name": "Dec"},
        },
    },
    "Atlanta": {
        "station_id": "USW00013874",
        "icao":       "KATL",
        "timezone":   "America/New_York",
        "note":       "Hartsfield-Jackson Atlanta International Airport",
        "monthly": {
            "1":  {"tmax_normal": 53.5, "tmin_normal": 33.5, "tmax_stddev": 4.5,  "month_name": "Jan"},
            "2":  {"tmax_normal": 57.4, "tmin_normal": 36.6, "tmax_stddev": 4.2,  "month_name": "Feb"},
            "3":  {"tmax_normal": 65.2, "tmin_normal": 43.5, "tmax_stddev": 3.5,  "month_name": "Mar"},
            "4":  {"tmax_normal": 73.6, "tmin_normal": 51.7, "tmax_stddev": 3.0,  "month_name": "Apr"},
            "5":  {"tmax_normal": 80.6, "tmin_normal": 59.8, "tmax_stddev": 2.5,  "month_name": "May"},
            "6":  {"tmax_normal": 87.4, "tmin_normal": 67.6, "tmax_stddev": 2.0,  "month_name": "Jun"},
            "7":  {"tmax_normal": 90.4, "tmin_normal": 71.4, "tmax_stddev": 1.8,  "month_name": "Jul"},
            "8":  {"tmax_normal": 89.0, "tmin_normal": 70.6, "tmax_stddev": 2.0,  "month_name": "Aug"},
            "9":  {"tmax_normal": 83.8, "tmin_normal": 64.5, "tmax_stddev": 2.5,  "month_name": "Sep"},
            "10": {"tmax_normal": 74.1, "tmin_normal": 52.8, "tmax_stddev": 3.0,  "month_name": "Oct"},
            "11": {"tmax_normal": 64.2, "tmin_normal": 43.0, "tmax_stddev": 3.8,  "month_name": "Nov"},
            "12": {"tmax_normal": 55.2, "tmin_normal": 35.5, "tmax_stddev": 4.5,  "month_name": "Dec"},
        },
    },
    "Oklahoma City": {
        "station_id": "USW00013967",
        "icao":       "KOKC",
        "timezone":   "America/Chicago",
        "note":       "Will Rogers World Airport",
        "monthly": {
            "1":  {"tmax_normal": 48.9, "tmin_normal": 27.4, "tmax_stddev": 5.5,  "month_name": "Jan"},
            "2":  {"tmax_normal": 53.9, "tmin_normal": 31.5, "tmax_stddev": 5.2,  "month_name": "Feb"},
            "3":  {"tmax_normal": 63.3, "tmin_normal": 40.2, "tmax_stddev": 5.0,  "month_name": "Mar"},
            "4":  {"tmax_normal": 73.4, "tmin_normal": 50.5, "tmax_stddev": 4.5,  "month_name": "Apr"},
            "5":  {"tmax_normal": 80.5, "tmin_normal": 59.2, "tmax_stddev": 3.8,  "month_name": "May"},
            "6":  {"tmax_normal": 89.4, "tmin_normal": 67.8, "tmax_stddev": 3.0,  "month_name": "Jun"},
            "7":  {"tmax_normal": 95.1, "tmin_normal": 73.7, "tmax_stddev": 2.5,  "month_name": "Jul"},
            "8":  {"tmax_normal": 94.0, "tmin_normal": 72.5, "tmax_stddev": 2.5,  "month_name": "Aug"},
            "9":  {"tmax_normal": 85.1, "tmin_normal": 63.8, "tmax_stddev": 3.2,  "month_name": "Sep"},
            "10": {"tmax_normal": 73.8, "tmin_normal": 51.9, "tmax_stddev": 4.2,  "month_name": "Oct"},
            "11": {"tmax_normal": 62.0, "tmin_normal": 40.1, "tmax_stddev": 5.0,  "month_name": "Nov"},
            "12": {"tmax_normal": 51.1, "tmin_normal": 29.9, "tmax_stddev": 5.5,  "month_name": "Dec"},
        },
    },
    "Phoenix": {
        "station_id": "USW00023183",
        "icao":       "KPHX",
        "timezone":   "America/Phoenix",
        "note":       "Phoenix Sky Harbor International Airport",
        "monthly": {
            "1":  {"tmax_normal": 67.1, "tmin_normal": 43.4, "tmax_stddev": 3.5,  "month_name": "Jan"},
            "2":  {"tmax_normal": 71.1, "tmin_normal": 46.9, "tmax_stddev": 3.5,  "month_name": "Feb"},
            "3":  {"tmax_normal": 79.2, "tmin_normal": 53.5, "tmax_stddev": 3.2,  "month_name": "Mar"},
            "4":  {"tmax_normal": 88.2, "tmin_normal": 61.5, "tmax_stddev": 3.0,  "month_name": "Apr"},
            "5":  {"tmax_normal": 97.8, "tmin_normal": 70.3, "tmax_stddev": 3.2,  "month_name": "May"},
            "6":  {"tmax_normal": 107.1,"tmin_normal": 79.2, "tmax_stddev": 2.8,  "month_name": "Jun"},
            "7":  {"tmax_normal": 106.1,"tmin_normal": 83.9, "tmax_stddev": 2.5,  "month_name": "Jul"},
            "8":  {"tmax_normal": 103.5,"tmin_normal": 82.4, "tmax_stddev": 2.5,  "month_name": "Aug"},
            "9":  {"tmax_normal": 98.3, "tmin_normal": 76.5, "tmax_stddev": 2.8,  "month_name": "Sep"},
            "10": {"tmax_normal": 87.7, "tmin_normal": 65.2, "tmax_stddev": 3.2,  "month_name": "Oct"},
            "11": {"tmax_normal": 75.4, "tmin_normal": 52.4, "tmax_stddev": 3.5,  "month_name": "Nov"},
            "12": {"tmax_normal": 66.3, "tmin_normal": 43.6, "tmax_stddev": 3.5,  "month_name": "Dec"},
        },
    },
    "Washington DC": {
        "station_id": "USW00013743",
        "icao":       "KDCA",
        "timezone":   "America/New_York",
        "note":       "Reagan National Airport",
        "monthly": {
            "1":  {"tmax_normal": 42.9, "tmin_normal": 27.3, "tmax_stddev": 4.5,  "month_name": "Jan"},
            "2":  {"tmax_normal": 46.5, "tmin_normal": 29.5, "tmax_stddev": 4.5,  "month_name": "Feb"},
            "3":  {"tmax_normal": 57.1, "tmin_normal": 37.6, "tmax_stddev": 3.8,  "month_name": "Mar"},
            "4":  {"tmax_normal": 67.5, "tmin_normal": 47.0, "tmax_stddev": 3.2,  "month_name": "Apr"},
            "5":  {"tmax_normal": 76.3, "tmin_normal": 56.5, "tmax_stddev": 2.8,  "month_name": "May"},
            "6":  {"tmax_normal": 84.5, "tmin_normal": 65.7, "tmax_stddev": 2.5,  "month_name": "Jun"},
            "7":  {"tmax_normal": 88.6, "tmin_normal": 70.6, "tmax_stddev": 2.0,  "month_name": "Jul"},
            "8":  {"tmax_normal": 86.6, "tmin_normal": 68.9, "tmax_stddev": 2.2,  "month_name": "Aug"},
            "9":  {"tmax_normal": 79.8, "tmin_normal": 62.0, "tmax_stddev": 2.5,  "month_name": "Sep"},
            "10": {"tmax_normal": 68.5, "tmin_normal": 50.2, "tmax_stddev": 3.2,  "month_name": "Oct"},
            "11": {"tmax_normal": 58.2, "tmin_normal": 41.0, "tmax_stddev": 4.0,  "month_name": "Nov"},
            "12": {"tmax_normal": 46.9, "tmin_normal": 31.4, "tmax_stddev": 4.5,  "month_name": "Dec"},
        },
    },
    "Seattle": {
        "station_id": "USW00024233",
        "icao":       "KSEA",
        "timezone":   "America/Los_Angeles",
        "note":       "Seattle-Tacoma International Airport",
        "monthly": {
            "1":  {"tmax_normal": 46.8, "tmin_normal": 37.3, "tmax_stddev": 4.5,  "month_name": "Jan"},
            "2":  {"tmax_normal": 50.0, "tmin_normal": 38.5, "tmax_stddev": 4.2,  "month_name": "Feb"},
            "3":  {"tmax_normal": 54.3, "tmin_normal": 40.5, "tmax_stddev": 3.8,  "month_name": "Mar"},
            "4":  {"tmax_normal": 59.9, "tmin_normal": 44.2, "tmax_stddev": 3.5,  "month_name": "Apr"},
            "5":  {"tmax_normal": 66.1, "tmin_normal": 49.5, "tmax_stddev": 3.2,  "month_name": "May"},
            "6":  {"tmax_normal": 71.5, "tmin_normal": 54.0, "tmax_stddev": 3.0,  "month_name": "Jun"},
            "7":  {"tmax_normal": 78.6, "tmin_normal": 59.2, "tmax_stddev": 2.5,  "month_name": "Jul"},
            "8":  {"tmax_normal": 79.3, "tmin_normal": 59.8, "tmax_stddev": 2.5,  "month_name": "Aug"},
            "9":  {"tmax_normal": 72.2, "tmin_normal": 54.9, "tmax_stddev": 3.0,  "month_name": "Sep"},
            "10": {"tmax_normal": 60.4, "tmin_normal": 47.6, "tmax_stddev": 3.8,  "month_name": "Oct"},
            "11": {"tmax_normal": 51.5, "tmin_normal": 41.5, "tmax_stddev": 4.2,  "month_name": "Nov"},
            "12": {"tmax_normal": 46.2, "tmin_normal": 37.3, "tmax_stddev": 4.5,  "month_name": "Dec"},
        },
    },
    "Houston": {
        "station_id": "USW00012960",
        "icao":       "KHOU",
        "timezone":   "America/Chicago",
        "note":       "William P. Hobby Airport",
        "monthly": {
            "1":  {"tmax_normal": 63.6, "tmin_normal": 43.4, "tmax_stddev": 4.2,  "month_name": "Jan"},
            "2":  {"tmax_normal": 67.5, "tmin_normal": 47.1, "tmax_stddev": 4.0,  "month_name": "Feb"},
            "3":  {"tmax_normal": 74.3, "tmin_normal": 53.7, "tmax_stddev": 3.5,  "month_name": "Mar"},
            "4":  {"tmax_normal": 80.2, "tmin_normal": 61.2, "tmax_stddev": 3.0,  "month_name": "Apr"},
            "5":  {"tmax_normal": 86.2, "tmin_normal": 68.1, "tmax_stddev": 2.5,  "month_name": "May"},
            "6":  {"tmax_normal": 92.3, "tmin_normal": 74.2, "tmax_stddev": 2.0,  "month_name": "Jun"},
            "7":  {"tmax_normal": 94.6, "tmin_normal": 76.0, "tmax_stddev": 1.8,  "month_name": "Jul"},
            "8":  {"tmax_normal": 94.8, "tmin_normal": 75.9, "tmax_stddev": 1.8,  "month_name": "Aug"},
            "9":  {"tmax_normal": 89.8, "tmin_normal": 70.9, "tmax_stddev": 2.2,  "month_name": "Sep"},
            "10": {"tmax_normal": 80.2, "tmin_normal": 60.2, "tmax_stddev": 3.0,  "month_name": "Oct"},
            "11": {"tmax_normal": 70.9, "tmin_normal": 50.9, "tmax_stddev": 3.8,  "month_name": "Nov"},
            "12": {"tmax_normal": 64.0, "tmin_normal": 44.7, "tmax_stddev": 4.2,  "month_name": "Dec"},
        },
    },
    "Dallas": {
        "station_id": "USW00013960",
        "icao":       "KDAL",
        "timezone":   "America/Chicago",
        "note":       "Dallas Love Field",
        "monthly": {
            "1":  {"tmax_normal": 57.4, "tmin_normal": 36.4, "tmax_stddev": 4.8,  "month_name": "Jan"},
            "2":  {"tmax_normal": 61.9, "tmin_normal": 40.4, "tmax_stddev": 4.5,  "month_name": "Feb"},
            "3":  {"tmax_normal": 70.7, "tmin_normal": 48.5, "tmax_stddev": 4.0,  "month_name": "Mar"},
            "4":  {"tmax_normal": 79.2, "tmin_normal": 57.5, "tmax_stddev": 3.5,  "month_name": "Apr"},
            "5":  {"tmax_normal": 86.5, "tmin_normal": 65.5, "tmax_stddev": 3.0,  "month_name": "May"},
            "6":  {"tmax_normal": 94.4, "tmin_normal": 73.5, "tmax_stddev": 2.5,  "month_name": "Jun"},
            "7":  {"tmax_normal": 98.8, "tmin_normal": 77.8, "tmax_stddev": 2.0,  "month_name": "Jul"},
            "8":  {"tmax_normal": 98.6, "tmin_normal": 77.2, "tmax_stddev": 2.0,  "month_name": "Aug"},
            "9":  {"tmax_normal": 90.8, "tmin_normal": 69.6, "tmax_stddev": 2.8,  "month_name": "Sep"},
            "10": {"tmax_normal": 80.5, "tmin_normal": 58.5, "tmax_stddev": 3.5,  "month_name": "Oct"},
            "11": {"tmax_normal": 69.4, "tmin_normal": 47.6, "tmax_stddev": 4.2,  "month_name": "Nov"},
            "12": {"tmax_normal": 59.7, "tmin_normal": 38.5, "tmax_stddev": 4.8,  "month_name": "Dec"},
        },
    },
    "San Antonio": {
        "station_id": "USW00012921",
        "icao":       "KSAT",
        "timezone":   "America/Chicago",
        "note":       "San Antonio International Airport",
        "monthly": {
            "1":  {"tmax_normal": 62.7, "tmin_normal": 40.8, "tmax_stddev": 4.5,  "month_name": "Jan"},
            "2":  {"tmax_normal": 67.2, "tmin_normal": 44.9, "tmax_stddev": 4.2,  "month_name": "Feb"},
            "3":  {"tmax_normal": 75.1, "tmin_normal": 52.5, "tmax_stddev": 3.8,  "month_name": "Mar"},
            "4":  {"tmax_normal": 82.3, "tmin_normal": 61.0, "tmax_stddev": 3.2,  "month_name": "Apr"},
            "5":  {"tmax_normal": 88.6, "tmin_normal": 68.4, "tmax_stddev": 2.8,  "month_name": "May"},
            "6":  {"tmax_normal": 95.2, "tmin_normal": 75.1, "tmax_stddev": 2.2,  "month_name": "Jun"},
            "7":  {"tmax_normal": 98.6, "tmin_normal": 78.2, "tmax_stddev": 1.8,  "month_name": "Jul"},
            "8":  {"tmax_normal": 98.4, "tmin_normal": 77.8, "tmax_stddev": 1.8,  "month_name": "Aug"},
            "9":  {"tmax_normal": 91.8, "tmin_normal": 71.3, "tmax_stddev": 2.5,  "month_name": "Sep"},
            "10": {"tmax_normal": 82.1, "tmin_normal": 61.2, "tmax_stddev": 3.2,  "month_name": "Oct"},
            "11": {"tmax_normal": 72.3, "tmin_normal": 50.6, "tmax_stddev": 4.0,  "month_name": "Nov"},
            "12": {"tmax_normal": 64.5, "tmin_normal": 42.5, "tmax_stddev": 4.5,  "month_name": "Dec"},
        },
    },
    "New Orleans": {
        "station_id": "USW00012916",
        "icao":       "KMSY",
        "timezone":   "America/Chicago",
        "note":       "Louis Armstrong New Orleans International Airport",
        "monthly": {
            "1":  {"tmax_normal": 63.5, "tmin_normal": 45.5, "tmax_stddev": 4.5,  "month_name": "Jan"},
            "2":  {"tmax_normal": 66.5, "tmin_normal": 48.4, "tmax_stddev": 4.2,  "month_name": "Feb"},
            "3":  {"tmax_normal": 74.0, "tmin_normal": 55.4, "tmax_stddev": 3.5,  "month_name": "Mar"},
            "4":  {"tmax_normal": 79.9, "tmin_normal": 63.2, "tmax_stddev": 3.0,  "month_name": "Apr"},
            "5":  {"tmax_normal": 86.2, "tmin_normal": 70.6, "tmax_stddev": 2.5,  "month_name": "May"},
            "6":  {"tmax_normal": 91.5, "tmin_normal": 76.5, "tmax_stddev": 2.0,  "month_name": "Jun"},
            "7":  {"tmax_normal": 92.8, "tmin_normal": 78.1, "tmax_stddev": 1.8,  "month_name": "Jul"},
            "8":  {"tmax_normal": 92.8, "tmin_normal": 77.8, "tmax_stddev": 1.8,  "month_name": "Aug"},
            "9":  {"tmax_normal": 88.0, "tmin_normal": 73.2, "tmax_stddev": 2.5,  "month_name": "Sep"},
            "10": {"tmax_normal": 79.7, "tmin_normal": 63.0, "tmax_stddev": 3.2,  "month_name": "Oct"},
            "11": {"tmax_normal": 71.2, "tmin_normal": 54.2, "tmax_stddev": 3.8,  "month_name": "Nov"},
            "12": {"tmax_normal": 65.4, "tmin_normal": 47.6, "tmax_stddev": 4.5,  "month_name": "Dec"},
        },
    },
    "Minneapolis": {
        "station_id": "USW00014922",
        "icao":       "KMSP",
        "timezone":   "America/Chicago",
        "note":       "Minneapolis-Saint Paul International Airport",
        "monthly": {
            "1":  {"tmax_normal": 23.5, "tmin_normal":  6.0, "tmax_stddev": 6.5,  "month_name": "Jan"},
            "2":  {"tmax_normal": 28.7, "tmin_normal": 10.6, "tmax_stddev": 6.2,  "month_name": "Feb"},
            "3":  {"tmax_normal": 42.1, "tmin_normal": 23.4, "tmax_stddev": 5.5,  "month_name": "Mar"},
            "4":  {"tmax_normal": 57.4, "tmin_normal": 37.0, "tmax_stddev": 5.0,  "month_name": "Apr"},
            "5":  {"tmax_normal": 70.0, "tmin_normal": 49.2, "tmax_stddev": 4.2,  "month_name": "May"},
            "6":  {"tmax_normal": 79.3, "tmin_normal": 59.2, "tmax_stddev": 3.5,  "month_name": "Jun"},
            "7":  {"tmax_normal": 84.4, "tmin_normal": 64.7, "tmax_stddev": 3.0,  "month_name": "Jul"},
            "8":  {"tmax_normal": 82.4, "tmin_normal": 62.5, "tmax_stddev": 3.2,  "month_name": "Aug"},
            "9":  {"tmax_normal": 71.7, "tmin_normal": 51.7, "tmax_stddev": 3.8,  "month_name": "Sep"},
            "10": {"tmax_normal": 57.5, "tmin_normal": 38.8, "tmax_stddev": 4.5,  "month_name": "Oct"},
            "11": {"tmax_normal": 38.9, "tmin_normal": 24.3, "tmax_stddev": 5.5,  "month_name": "Nov"},
            "12": {"tmax_normal": 26.5, "tmin_normal": 11.7, "tmax_stddev": 6.5,  "month_name": "Dec"},
        },
    },
}


def build_profile(city: str, data: dict) -> dict:
    """Finalise a city entry with derived fields."""
    monthly = {}
    for m, v in data["monthly"].items():
        tmax = v["tmax_normal"]
        tmin = v["tmin_normal"]
        std  = v["tmax_stddev"]
        diurnal = round(tmax - tmin, 1)
        monthly[m] = {
            "tmax_normal":        tmax,
            "tmin_normal":        tmin,
            "tmax_stddev":        std,
            "diurnal_range":      diurnal,
            "afternoon_climb":    diurnal,      # same as diurnal for daily high markets
            "bracket_difficulty": round(2.0 / std, 3),
            "month_name":         v["month_name"],
        }
    return {
        "station_id": data["station_id"],
        "icao":       data["icao"],
        "timezone":   data["timezone"],
        "note":       data["note"],
        "monthly":    monthly,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }


def main(dry_run: bool) -> None:
    profiles = json.loads(PROFILES_PATH.read_text()) if PROFILES_PATH.exists() else {}

    added = []
    for city, data in MISSING_CITIES.items():
        if city in profiles:
            print(f"  {city}: already present — skipping")
            continue
        if not dry_run:
            profiles[city] = build_profile(city, data)
        added.append(city)
        print(f"  {city}: {'would add' if dry_run else 'added'}")

    if not dry_run and added:
        PROFILES_PATH.write_text(json.dumps(profiles, indent=2))
        print(f"\n  Wrote {PROFILES_PATH}  ({len(profiles)} cities total)")
    elif dry_run:
        print(f"\n  [DRY RUN] Would add {len(added)} cities → {len(profiles)+len(added)} total")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    main(dry_run=args.dry_run)
