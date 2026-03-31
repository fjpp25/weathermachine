"""
debug_noaa.py
-------------
Prints the raw NOAA API response for a single station so you can inspect
the exact field names and structure before the main parser runs.

Usage:
    python debug_noaa.py                        # uses KNYC (New York) by default
    python debug_noaa.py --station USW00014819  # Chicago Midway
"""

import json
import argparse
import requests

API_BASE   = "https://www.ncei.noaa.gov/access/services/data/v1"
DATA_TYPES = [
    "MLY-TMAX-NORMAL",
    "MLY-TMIN-NORMAL",
    "MLY-TMAX-STDDEV",
    "MLY-DUTR-NORMAL",
]

def debug_fetch(station_id: str):
    params = {
        "dataset":            "normals-monthly-1991-2020",
        "stations":           station_id,
        "dataTypes":          ",".join(DATA_TYPES),
        "format":             "json",
        "units":              "standard",
        "includeStationName": "true",
    }

    print(f"\nGET {API_BASE}")
    print(f"Params: {json.dumps(params, indent=2)}\n")

    resp = requests.get(API_BASE, params=params, timeout=30)
    print(f"HTTP status: {resp.status_code}")
    print(f"Content-Type: {resp.headers.get('Content-Type', 'unknown')}\n")

    try:
        data = resp.json()
    except Exception as e:
        print(f"Failed to parse JSON: {e}")
        print("Raw response (first 2000 chars):")
        print(resp.text[:2000])
        return

    print(f"Response type: {type(data).__name__}")

    if isinstance(data, list):
        print(f"Number of rows: {len(data)}")
        if data:
            print(f"\nFirst row keys: {list(data[0].keys())}")
            print(f"\nFirst row:\n{json.dumps(data[0], indent=2)}")
            if len(data) > 1:
                print(f"\nSecond row:\n{json.dumps(data[1], indent=2)}")
    elif isinstance(data, dict):
        print(f"Top-level keys: {list(data.keys())}")
        print(f"\nFull response:\n{json.dumps(data, indent=2)[:3000]}")
    else:
        print(f"Unexpected type: {data}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Debug NOAA API response structure")
    parser.add_argument("--station", default="USW00094728", help="NOAA station ID (default: KNYC)")
    args = parser.parse_args()
    debug_fetch(args.station)
