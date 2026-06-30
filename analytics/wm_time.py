#!/usr/bin/env python3
"""
analytics.wm_time — canonical local-time helper for the Weather Machine.

Single source of truth for converting UTC timestamps to a city's wall-clock
time. tz logic was previously re-implemented in several places (the observer's
local_hour, bias_calculator.local_date_for, the engines' START_HOUR_CAP gates);
new code should depend on THIS. Existing modules can converge onto it over time
— no need to refactor working code now. Live code would import it as
`from analytics.wm_time import local_hour`.

Reads the tz map from cities.py (the canonical city registry in the repo root;
importable because the package __init__ puts the root on sys.path).

KEY DISTINCTION (matters for the overnight LOWT product):
  - entry_local_hour : the city wall-clock hour an order was PLACED. A 02:00 UTC
    entry is the *previous evening* local time in the US.
  - market_date (parsed from the ticker, e.g. KXLOWTNYC-26JUN30-B67.5 -> 26JUN30)
    is which MARKET it is — the settlement date. For an overnight-low market this
    spans the night BEFORE that date, so entry-local-date and market_date legit
    differ. Keep them orthogonal; conflating them scrambles hour-of-day analysis.
"""
from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from cities import CITIES  # resolves via root on sys.path (see package __init__)

_TZ = {name: ZoneInfo(meta["tz"]) for name, meta in CITIES.items() if meta.get("tz")}


def tz_for(city: str):
    """Return the ZoneInfo for a city, or None if unknown."""
    return _TZ.get(city)


def parse_utc(ts: str) -> datetime | None:
    """Parse an ISO8601 UTC timestamp (trailing Z or +00:00) to aware datetime."""
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return None


def to_local(ts: str, city: str) -> datetime | None:
    """UTC ISO timestamp -> city-local aware datetime. Falls back to UTC for an
    unknown city (use tz_for to detect that case explicitly)."""
    dt = parse_utc(ts)
    if dt is None:
        return None
    tz = _TZ.get(city)
    return dt.astimezone(tz) if tz is not None else dt


def local_hour(ts: str, city: str) -> int | None:
    """City-local hour-of-day (0-23). None if unparseable."""
    dt = to_local(ts, city)
    return dt.hour if dt is not None else None


def local_date(ts: str, city: str) -> str | None:
    """City-local calendar date (YYYY-MM-DD). None if unparseable."""
    dt = to_local(ts, city)
    return dt.strftime("%Y-%m-%d") if dt is not None else None


_MON = {m: i for i, m in enumerate(
    ["JAN","FEB","MAR","APR","MAY","JUN","JUL","AUG","SEP","OCT","NOV","DEC"], 1)}


def market_date_raw(ticker: str) -> str | None:
    """Raw date segment of a Kalshi ticker: KXLOWTNYC-26JUN30-B67.5 -> '26JUN30'."""
    parts = ticker.split("-")
    return parts[1] if len(parts) >= 2 else None


def market_date_iso(ticker: str) -> str | None:
    """Ticker date segment -> ISO date. '26JUN30' -> '2026-06-30'. None if bad."""
    raw = market_date_raw(ticker)
    if not raw or len(raw) < 7:
        return None
    try:
        yy = int(raw[:2])
        mon = _MON.get(raw[2:5].upper())
        dd = int(raw[5:7])
        if mon is None:
            return None
        return f"20{yy:02d}-{mon:02d}-{dd:02d}"
    except (ValueError, KeyError):
        return None
