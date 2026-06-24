"""
accuweather_logger.py
---------------------
Silent background logger for AccuWeather daily HIGH/LOW forecasts.

Appends one row per city to data/accuweather_forecast_log.csv whenever
the AccuWeather cache refreshes (every ~2 hours). Rows are deduplicated
by (city, fetched_at) so multiple calls within the same cache window
produce no duplicate writes.

Purpose
-------
Provides time-aligned AccuWeather forecast history for future analysis,
specifically to enable a proper ±N°F skip zone study using the forecast
source the engine actually uses (AccuWeather), rather than NWS forecasts
which are logged in lowt_observations.csv.

Schema
------
logged_at_utc, city, forecast_high_f, forecast_low_f, fetched_at

Usage
-----
Called once per scheduler poll cycle — cheap, no extra API calls since
accuweather_feed.snapshot() reads from its in-memory/file cache.

    import accuweather_logger
    accuweather_logger.log_snapshot()
"""

from __future__ import annotations

import csv
from datetime import datetime, timezone
from pathlib import Path

from log_setup import get_logger

log = get_logger(__name__)

LOG_PATH = Path("data/accuweather_forecast_log.csv")

FIELDNAMES = [
    "logged_at_utc",
    "city",
    "forecast_high_f",
    "forecast_low_f",
    "fetched_at",
]

# In-memory record of the last fetched_at we wrote per city.
# Prevents duplicate rows within the same 2-hour cache window.
_last_written: dict[str, str] = {}


def _ensure_header() -> None:
    """Write CSV header if the file doesn't exist yet."""
    if not LOG_PATH.exists():
        LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(LOG_PATH, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
            writer.writeheader()


def log_snapshot(city_filter: str = None) -> int:
    """
    Read the current AccuWeather cache and append any new forecasts to the log.

    Returns the number of rows written (0 if cache hasn't refreshed since last call).
    """
    import accuweather_feed

    try:
        forecasts = accuweather_feed.snapshot(city_filter=city_filter)
    except Exception as e:
        log.warning("accuweather_logger: snapshot failed: %s", e)
        return 0

    if not forecasts:
        return 0

    _ensure_header()

    now_utc = datetime.now(timezone.utc).isoformat()
    rows_written = 0

    with open(LOG_PATH, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)

        for city, data in forecasts.items():
            fetched_at = data.get("fetched_at", "")
            if not fetched_at:
                continue

            # Skip if we already wrote this exact fetch for this city
            if _last_written.get(city) == fetched_at:
                continue

            high = data.get("forecast_high_f")
            low  = data.get("forecast_low_f")

            # Skip cities with errors or missing forecast
            if high is None and low is None:
                continue

            writer.writerow({
                "logged_at_utc":   now_utc,
                "city":            city,
                "forecast_high_f": high if high is not None else "",
                "forecast_low_f":  low  if low  is not None else "",
                "fetched_at":      fetched_at,
            })
            _last_written[city] = fetched_at
            rows_written += 1

    if rows_written:
        log.debug("accuweather_logger: wrote %d row(s) to %s", rows_written, LOG_PATH)

    return rows_written
