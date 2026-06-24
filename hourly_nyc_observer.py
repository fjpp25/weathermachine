"""
hourly_nyc_observer.py
----------------------
Passive observer for Kalshi's NYC hourly temperature markets (KXTEMPNYCH).
No trading — read-only. Polls every 5 minutes and records bracket prices
alongside AccuWeather current conditions and hourly forecasts.

Market structure
----------------
  Series ticker   : KXTEMPNYCH
  Brackets        : T-type only  (XºF or above) — directional, ~20 per market
  Settlement      : AccuWeather current conditions at Central Park
                    (40.7812, -73.9665) recorded at market close
  Trading window  : opens at H-1:00 EDT, closes at H:00 EDT
  Settlement payout: H+1:05 EDT
  Ticker format   : KXTEMPNYCH-26MAY0511
                    └─ 26MAY05 = May 5 2026  |  11 = settlement hour (EDT)

Settlement note
---------------
Kalshi settles on the AccuWeather value published at market close.
Preliminary AccuWeather data may differ from the final reported value due
to rounding. This observer records both current conditions and the hourly
forecast so the gap can be measured empirically.

What we are trying to learn
---------------------------
  1. How closely does AccuWeather current conditions match final settlement?
  2. What is the typical bid-ask spread on these thin markets?
  3. How many minutes before close does the border bracket converge to No ≥ 0.90?
  4. Is there a systematic mispricing window (market leaves certainty unpriced)?
  5. Do overnight hours produce any meaningful bracket activity?

Output
------
  data/hourly_nyc_observations.json  — full record list (rewritten each poll)
  data/hourly_nyc_observations.csv   — append-only log for analysis

API budget
----------
  Normal polling (5 min): 1 Kalshi call + 1 current-conditions (cached 15 min)
                           + 1 hourly-forecast (cached 30 min)
  Burst window (< 15 min to close): interval tightens to 1 min automatically.
    Current-conditions cache means no extra AccuWeather calls during burst.
  Estimated AccuWeather usage: ~384 calls/day → ~11,500/month (Starter plan OK).
  AccuWeather is NOT called when Kalshi returns no open market.

Usage
-----
  python hourly_nyc_observer.py       # dynamic interval (5 min normal, 1 min near close)
  python hourly_nyc_observer.py --poll 2   # override: fixed 2 min interval
"""

from __future__ import annotations

import csv
import json
import time
import argparse
from datetime import datetime, timezone, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo
from typing import Optional

import requests

import accuweather_feed
from log_setup import get_logger

log = get_logger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

SERIES_TICKER      = "KXTEMPNYCH"
NYC_TZ             = ZoneInfo("America/New_York")
POLL_INTERVAL_SEC       = 5 * 60   # normal polling interval
POLL_INTERVAL_BURST_SEC = 1 * 60   # tightened interval when < BURST_WINDOW_MINS to close
BURST_WINDOW_MINS       = 15       # switch to burst polling this many minutes before close
KALSHI_API_BASE    = "https://api.elections.kalshi.com/trade-api/v2"
REQUEST_TIMEOUT    = 10
HOURLY_CACHE_SECS      = 30 * 60   # re-fetch AccuWeather hourly forecast every 30 min
CURRENT_COND_CACHE_SECS = 15 * 60  # re-fetch current conditions every 15 min

OUTPUT_JSON = Path("data/hourly_nyc_observations.json")
OUTPUT_CSV  = Path("data/hourly_nyc_observations.csv")

CSV_FIELDS = [
    "poll_time_utc",           # ISO UTC timestamp of this poll
    "market_ticker",           # Kalshi event ticker  e.g. KXTEMPNYCH-26MAY0511
    "market_hour_edt",         # integer settlement hour in EDT  e.g. 11
    "minutes_to_close",        # minutes remaining in the trading window
    "accuweather_current_f",   # AccuWeather current conditions (°F)
    "accuweather_forecast_f",  # AccuWeather hourly forecast for settlement hour (°F)
    "market_direction",        # "rising" forecast>current | "falling" forecast<current | "flat"
    "ticker",                  # bracket ticker  e.g. KXTEMPNYCH-26MAY0511-T73
    "threshold_f",             # temperature threshold (X in "X°F or above")
    "yes_bid",                 # highest YES bid price
    "yes_ask",                 # implied YES ask  (= 1 - no_bid)
    "no_bid",                  # highest NO bid price
    "no_ask",                  # implied NO ask   (= 1 - yes_bid)
    "spread",                  # yes_ask - yes_bid  (bid-ask spread in dollars)
    "volume",                  # total contracts traded this market
    "open_interest",           # open contracts
    "is_border",               # True = most contested bracket (yes_bid closest to 0.50)
    "current_resolves_yes",    # True = current_f >= threshold AND market is rising
                               # (meaningful only when temp is still climbing to settlement)
    "forecast_resolves_yes",   # True = forecast_f >= threshold
                               # (valid for both rising and falling markets)
]


# ---------------------------------------------------------------------------
# AccuWeather helpers
# ---------------------------------------------------------------------------

# In-process cache for the hourly forecast: {hour_edt: (fetched_at_utc, temp_f)}
_hourly_forecast_cache: dict[int, tuple[datetime, Optional[float]]] = {}

# In-process cache for current conditions: (fetched_at_utc, temp_f)
# Refreshed every CURRENT_COND_CACHE_SECS — temperature moves slowly enough
# that a 15-minute cache is accurate and saves ~200 AccuWeather calls/day.
_current_cond_cache: tuple[datetime, Optional[float]] | None = None


def _aw_api_key() -> Optional[str]:
    """Reuse the AccuWeather API key from accuweather_feed."""
    return accuweather_feed._api_key()


def _nyc_location_key() -> Optional[str]:
    """
    Return the AccuWeather location key for New York City.
    Reuses the persistent cache from accuweather_feed — no extra API calls
    unless the cache was cleared.
    """
    cache = accuweather_feed._load_location_cache()
    key   = cache.get("New York")
    if key:
        return key
    # Cache miss — fetch and persist via the existing helper
    api_key = _aw_api_key()
    if not api_key:
        return None
    loc_keys = accuweather_feed.ensure_location_keys(api_key)
    return loc_keys.get("New York")


def fetch_accuweather_current(location_key: str, api_key: str) -> Optional[float]:
    """
    Fetch the AccuWeather current conditions temperature (°F) for NYC,
    cached for CURRENT_COND_CACHE_SECS (15 min) to limit API usage.

    Temperature moves slowly enough that a 15-minute cache is accurate
    for observation purposes while cutting calls from ~288/day to ~96/day.
    The cache is bypassed on first call and after TTL expiry.

    Returns the temperature in °F, or the last cached value on failure.
    """
    global _current_cond_cache
    now_utc = datetime.now(timezone.utc)

    # Return cached value if still fresh
    if _current_cond_cache is not None:
        fetched_at, cached_val = _current_cond_cache
        if (now_utc - fetched_at).total_seconds() < CURRENT_COND_CACHE_SECS:
            log.debug("current-conditions cache hit: %.1f°F (age=%.0fs)",
                      cached_val or 0,
                      (now_utc - fetched_at).total_seconds())
            return cached_val

    try:
        resp = requests.get(
            f"{accuweather_feed.BASE_URL}/currentconditions/v1/{location_key}",
            params={"apikey": api_key, "details": "false"},
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()
        if data and isinstance(data, list):
            temp = (
                data[0]
                .get("Temperature", {})
                .get("Imperial", {})
                .get("Value")
            )
            val = float(temp) if temp is not None else None
            _current_cond_cache = (now_utc, val)
            log.info("current-conditions refreshed: %.1f°F", val or 0)
            return val
    except Exception as e:
        log.warning("accuweather current-conditions failed: %s", e)
        # Return stale cache on failure rather than None — better than nothing
        if _current_cond_cache is not None:
            return _current_cond_cache[1]
    return None


def fetch_accuweather_hourly(
    location_key: str,
    api_key: str,
    target_hour_edt: int,
) -> Optional[float]:
    """
    Return AccuWeather's forecast temperature (°F) for the given EDT hour,
    using the 12-hour hourly forecast endpoint.

    Results are cached for HOURLY_CACHE_SECS to limit API usage — the
    hourly forecast doesn't meaningfully change minute-to-minute.

    Returns the forecast temperature in °F, or None if the target hour is
    outside the 12-hour window or on failure.
    """
    now_utc = datetime.now(timezone.utc)

    # Return cached value if still fresh
    cached = _hourly_forecast_cache.get(target_hour_edt)
    if cached:
        fetched_at, cached_val = cached
        if (now_utc - fetched_at).total_seconds() < HOURLY_CACHE_SECS:
            log.debug("accuweather hourly forecast for hour %d: %.1f°F (cached)",
                      target_hour_edt, cached_val or 0)
            return cached_val

    try:
        resp = requests.get(
            f"{accuweather_feed.BASE_URL}/forecasts/v1/hourly/12hour/{location_key}",
            params={"apikey": api_key, "details": "false", "metric": "false"},
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        hourly = resp.json()

        # Refresh all hours from this response to maximise cache utility
        for entry in hourly:
            dt_str = entry.get("DateTime", "")
            if not dt_str:
                continue
            try:
                dt_edt = datetime.fromisoformat(dt_str).astimezone(NYC_TZ)
                val    = entry.get("Temperature", {}).get("Value")
                temp_f = float(val) if val is not None else None
                _hourly_forecast_cache[dt_edt.hour] = (now_utc, temp_f)
            except Exception:
                continue

        result = _hourly_forecast_cache.get(target_hour_edt)
        return result[1] if result else None

    except Exception as e:
        log.warning("accuweather hourly forecast failed: %s", e)
        return None


# ---------------------------------------------------------------------------
# Market timing helpers
# ---------------------------------------------------------------------------

def _active_market_hour_and_minutes() -> tuple[int, int]:
    """
    Return (market_hour_edt, minutes_to_close) for the currently live market.

    The active market settles AT the top of the next full hour EDT.
      e.g. at 10:37am EDT → market_hour=11, minutes_to_close=23
           at 23:58 EDT   → market_hour=0,  minutes_to_close=2

    minutes_to_close is integer-truncated (ignores seconds — good enough for logs).
    """
    now_edt          = datetime.now(NYC_TZ)
    market_hour      = (now_edt.hour + 1) % 24
    minutes_to_close = 60 - now_edt.minute
    return market_hour, minutes_to_close


def _event_ticker(market_hour_edt: int) -> str:
    """
    Build the Kalshi event ticker for the market settling at market_hour_edt.

    Format  : KXTEMPNYCH-{YYMONDD}{HH}
    Example : KXTEMPNYCH-26MAY0511  (settlement at 11am EDT, May 5 2026)

    Note: the date component is the CURRENT calendar date in EDT, which is
    correct as long as we're not straddling midnight. Near midnight the hour
    wraps to 0 and the date is still "today" in EDT — that's intentional,
    as the 00:00 market (midnight) is still today's date in Kalshi's naming.
    """
    now_edt  = datetime.now(NYC_TZ)
    date_str = now_edt.strftime("%y%b%d").upper()   # e.g. "26MAY05"
    return f"{SERIES_TICKER}-{date_str}{market_hour_edt:02d}"


# ---------------------------------------------------------------------------
# Kalshi helpers
# ---------------------------------------------------------------------------

def _fetch_brackets(event_ticker: str) -> list[dict]:
    """
    Fetch all open bracket markets for the given event ticker from Kalshi.

    Uses the public /markets endpoint — no auth required.
    Filters to the exact event_ticker to exclude other hourly markets that
    might be open concurrently (e.g. the next hour's market which opens
    shortly before the current one closes).
    """
    try:
        resp = requests.get(
            f"{KALSHI_API_BASE}/markets",
            params={"series_ticker": SERIES_TICKER, "status": "open"},
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        markets = resp.json().get("markets", [])
        return [
            m for m in markets
            if m.get("event_ticker", "").upper() == event_ticker.upper()
        ]
    except Exception as e:
        log.warning("kalshi fetch failed for %s: %s", event_ticker, e)
        return []


def _threshold_from_ticker(ticker: str) -> Optional[float]:
    """
    Parse the temperature threshold from a T-bracket ticker.
      "KXTEMPNYCH-26MAY0511-T73"  →  73.0
      "KXTEMPNYCH-26MAY0511-T68"  →  68.0
    Returns None if the suffix is absent or non-numeric.
    """
    parts = ticker.split("-")
    if not parts:
        return None
    code = parts[-1]
    if code.startswith("T"):
        try:
            return float(code[1:])
        except ValueError:
            pass
    return None


def _threshold_from_market(market: dict) -> Optional[float]:
    """
    Extract threshold from market dict. Tries floor_strike first (the
    canonical Kalshi field for the lower bound of a T-bracket), then
    falls back to parsing the ticker suffix.
    """
    floor = market.get("floor_strike")
    if floor is not None:
        try:
            return float(floor)
        except (ValueError, TypeError):
            pass
    return _threshold_from_ticker(market.get("ticker", ""))


def _parse_bracket_row(
    market:       dict,
    poll_time:    str,
    event_ticker: str,
    market_hour:  int,
    mins_to_close:int,
    current_f:    Optional[float],
    forecast_f:   Optional[float],
    is_border:    bool,
) -> dict:
    """
    Build one observation row from a raw Kalshi market dict and current context.
    Prices are derived as: yes_ask = 1 - no_bid  |  no_ask = 1 - yes_bid
    This is the standard Kalshi approximation — the true ask is the complement
    of the other side's best bid.
    """
    ticker      = market.get("ticker", "")
    threshold_f = _threshold_from_market(market)

    yes_bid = _safe_float(market.get("yes_bid_dollars"))
    no_bid  = _safe_float(market.get("no_bid_dollars"))

    # Derive implied ask prices
    yes_ask = round(1.0 - no_bid,  4) if no_bid  is not None else None
    no_ask  = round(1.0 - yes_bid, 4) if yes_bid is not None else None

    # Bid-ask spread (yes side) — key metric for thin-market analysis
    spread = (
        round(yes_ask - yes_bid, 4)
        if yes_ask is not None and yes_bid is not None and yes_bid > 0
        else None
    )

    # Market direction inferred from forecast vs current reading.
    # Determines whether the current reading is meaningful as a settlement
    # proxy — only reliable when the market is still rising toward settlement.
    if current_f is not None and forecast_f is not None:
        if forecast_f > current_f:
            market_direction = "rising"
        elif forecast_f < current_f:
            market_direction = "falling"
        else:
            market_direction = "flat"
    else:
        market_direction = None

    # current_resolves_yes: only meaningful in a rising market.
    # If the temperature is falling toward settlement, being above the
    # threshold NOW says nothing about whether the settlement reading will be.
    current_resolves_yes = (
        current_f is not None
        and threshold_f is not None
        and current_f >= threshold_f
        and market_direction in ("rising", "flat")
    )

    # forecast_resolves_yes: the AccuWeather hourly forecast for the
    # settlement hour exceeds the threshold. Valid in both directions —
    # this is the cleaner oracle since the forecast is always for the
    # settlement hour, not the current moment.
    forecast_resolves_yes = (
        forecast_f is not None
        and threshold_f is not None
        and forecast_f >= threshold_f
    )

    return {
        "poll_time_utc":          poll_time,
        "market_ticker":          event_ticker,
        "market_hour_edt":        market_hour,
        "minutes_to_close":       mins_to_close,
        "accuweather_current_f":  current_f,
        "accuweather_forecast_f": forecast_f,
        "market_direction":       market_direction,
        "ticker":                 ticker,
        "threshold_f":            threshold_f,
        "yes_bid":                yes_bid,
        "yes_ask":                yes_ask,
        "no_bid":                 no_bid,
        "no_ask":                 no_ask,
        "spread":                 spread,
        "volume":                 _safe_float(market.get("volume_fp")),
        "open_interest":          _safe_float(market.get("open_interest_fp")),
        "is_border":              is_border,
        "current_resolves_yes":   current_resolves_yes,
        "forecast_resolves_yes":  forecast_resolves_yes,
    }


def _safe_float(val) -> Optional[float]:
    """Convert a value to float, returning None on failure or zero."""
    if val is None:
        return None
    try:
        f = float(val)
        return f if f != 0.0 else None
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# I/O helpers
# ---------------------------------------------------------------------------

def load_observations() -> list[dict]:
    """Load existing observations from JSON, or return an empty list."""
    if OUTPUT_JSON.exists():
        try:
            data = json.loads(OUTPUT_JSON.read_text())
            if isinstance(data, list):
                return data
        except Exception as e:
            log.warning("could not load %s: %s", OUTPUT_JSON, e)
    return []


def _migrate_csv_header() -> None:
    """
    If the CSV exists with an old header (fewer columns than CSV_FIELDS),
    rewrite the header row in-place so new rows align correctly.
    Called once at startup.
    """
    if not OUTPUT_CSV.exists():
        return
    with open(OUTPUT_CSV, "r", encoding="utf-8") as f:
        first_line = f.readline()
    existing_fields = [c.strip() for c in first_line.split(",")]
    if existing_fields == CSV_FIELDS:
        return  # already up to date
    # Header is stale — rewrite with current CSV_FIELDS
    log.info("hourly_nyc_observer: migrating CSV header (%d → %d fields)",
             len(existing_fields), len(CSV_FIELDS))
    import shutil, tempfile
    with open(OUTPUT_CSV, "r", encoding="utf-8") as src,          tempfile.NamedTemporaryFile("w", delete=False,
                                     encoding="utf-8", newline="") as tmp:
        src.readline()  # skip old header
        tmp.write(",".join(CSV_FIELDS) + "\n")
        shutil.copyfileobj(src, tmp)
        tmp_path = tmp.name
    shutil.move(tmp_path, OUTPUT_CSV)
    log.info("hourly_nyc_observer: CSV header migrated")


def _append_csv_rows(rows: list[dict]) -> None:
    """Append new rows to the CSV file, writing the header if the file is new."""
    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    write_header = not OUTPUT_CSV.exists()
    with open(OUTPUT_CSV, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS, extrasaction="ignore")
        if write_header:
            writer.writeheader()
        writer.writerows(rows)


def _save_json(observations: list[dict]) -> None:
    OUTPUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_JSON.write_text(json.dumps(observations, indent=2, default=str))


# ---------------------------------------------------------------------------
# Poll
# ---------------------------------------------------------------------------

def poll_once(
    observations:  list[dict],
    api_key:       str,
    location_key:  str,
) -> int:
    """
    Execute one poll cycle. Fetches AccuWeather current conditions, the
    hourly forecast, and all open brackets for the current active market.
    Appends new rows to observations and the CSV.

    Returns the number of rows added.
    """
    poll_time                = datetime.now(timezone.utc).isoformat()
    market_hour, mins_close  = _active_market_hour_and_minutes()
    evt_ticker               = _event_ticker(market_hour)

    log.info("poll  %s  hour=%02d EDT  closes_in=%dmin",
             evt_ticker, market_hour, mins_close)

    # ── Kalshi brackets first — skip AccuWeather entirely if no market ────
    # Avoids burning AccuWeather calls during the overnight dead zone when
    # Kalshi hasn't opened the next hour's market yet.
    brackets = _fetch_brackets(evt_ticker)

    if not brackets:
        log.warning("no open brackets for %s — skipping AccuWeather", evt_ticker)
        sentinel = {k: None for k in CSV_FIELDS}
        sentinel.update({
            "poll_time_utc":    poll_time,
            "market_ticker":    evt_ticker,
            "market_hour_edt":  market_hour,
            "minutes_to_close": mins_close,
        })
        observations.append(sentinel)
        _append_csv_rows([sentinel])
        print(f"  {_now_local()}  {evt_ticker}  NO MARKET")
        return 1

    # ── AccuWeather — only fetched when market exists ─────────────────────
    current_f  = fetch_accuweather_current(location_key, api_key)
    forecast_f = fetch_accuweather_hourly(location_key, api_key, market_hour)

    aw_str = (
        f"cur={'?' if current_f is None else f'{current_f:.1f}'}°F  "
        f"fcst_h{market_hour:02d}={'?' if forecast_f is None else f'{forecast_f:.1f}'}°F"
    )

    # ── Identify the border bracket ───────────────────────────────────────
    # The border bracket is the most contested one — yes_bid closest to 0.50.
    # Using max(yes_bid) was wrong: it picked the most-resolved bracket
    # (priced at 0.99), not the most uncertain one.
    def _yes_bid_distance(m: dict) -> float:
        yb = m.get("yes_bid_dollars")
        if yb is None:
            return 1.0   # push brackets with no yes_bid to the back
        try:
            return abs(float(yb) - 0.5)
        except (ValueError, TypeError):
            return 1.0

    border_ticker = min(brackets, key=_yes_bid_distance).get("ticker", "")

    # ── Build rows ────────────────────────────────────────────────────────
    new_rows: list[dict] = []
    for m in brackets:
        row = _parse_bracket_row(
            market        = m,
            poll_time     = poll_time,
            event_ticker  = evt_ticker,
            market_hour   = market_hour,
            mins_to_close = mins_close,
            current_f     = current_f,
            forecast_f    = forecast_f,
            is_border     = (m.get("ticker") == border_ticker),
        )
        new_rows.append(row)

    observations.extend(new_rows)
    _append_csv_rows(new_rows)

    # ── Summary line ──────────────────────────────────────────────────────
    # Safe formatters: the Kalshi API may return floor_strike as a string,
    # and any price field may be None on illiquid brackets.
    def _fmt_f(val, spec) -> str:
        if val is None:
            return "?"
        try:
            return format(float(val), spec)
        except (ValueError, TypeError):
            return str(val)

    border_row = next((r for r in new_rows if r["is_border"]), None)
    if border_row:
        border_str = (
            f"border=T{_fmt_f(border_row['threshold_f'], '.0f')}"
            f"  yes_bid={_fmt_f(border_row['yes_bid'], '.2f')}"
            f"  no_bid={_fmt_f(border_row['no_bid'], '.2f')}"
            f"  spread={_fmt_f(border_row['spread'], '.4f')}"
        )
        # Log the raw floor_strike so we can verify the API field type
        log.debug("border bracket raw: ticker=%s  floor_strike=%r  threshold_f=%r",
                  border_row.get("ticker"), border_ticker,
                  border_row.get("threshold_f"))
    else:
        border_str = "border=?"

    fcst_resolved  = sum(1 for r in new_rows if r.get("forecast_resolves_yes"))
    direction      = new_rows[0].get("market_direction", "?") if new_rows else "?"

    print(
        f"  {_now_local()}  {evt_ticker}"
        f"  {len(new_rows)}brk  {border_str}"
        f"  fcst_resolved={fcst_resolved}/{len(new_rows)}"
        f"  dir={direction}"
        f"  {aw_str}"
    )

    return len(new_rows)


def _now_local() -> str:
    """Return a compact local (EDT) time string for console output."""
    return datetime.now(NYC_TZ).strftime("%H:%M:%S EDT")


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def main(poll_interval_sec: int = POLL_INTERVAL_SEC) -> None:
    print("=" * 70)
    print("  Hourly NYC Temperature Market Observer  —  KXTEMPNYCH")
    print(f"  Poll interval : {poll_interval_sec // 60}m {poll_interval_sec % 60}s")
    print(f"  Output JSON   : {OUTPUT_JSON}")
    print(f"  Output CSV    : {OUTPUT_CSV}")
    print("  Read-only — no orders placed.")
    print("  Ctrl+C to stop.")
    print("=" * 70)

    # ── Credentials ───────────────────────────────────────────────────────
    api_key = _aw_api_key()
    if not api_key:
        print(
            "\nERROR: AccuWeather API key not found.\n"
            "  Add 'accuweather_api_key' to data/config.json and restart."
        )
        return

    location_key = _nyc_location_key()
    if not location_key:
        print(
            "\nERROR: Could not resolve AccuWeather location key for New York.\n"
            "  Delete data/accuweather_locations.json and restart to re-fetch."
        )
        return

    log.info("AccuWeather: api_key=%s...  nyc_key=%s",
             api_key[:6], location_key)

    # ── Migrate CSV header if needed ─────────────────────────────────────
    _migrate_csv_header()

    # ── Load history ──────────────────────────────────────────────────────
    observations = load_observations()
    log.info("loaded %d existing observations from %s", len(observations), OUTPUT_JSON)

    # ── Poll loop ─────────────────────────────────────────────────────────
    # Interval is dynamic when poll_interval_sec == POLL_INTERVAL_SEC (default):
    #   normal:       5 min  (>= BURST_WINDOW_MINS minutes to close)
    #   burst window: 1 min  (< BURST_WINDOW_MINS minutes to close)
    # A fixed --poll override disables dynamic behaviour entirely.
    use_dynamic = (poll_interval_sec == POLL_INTERVAL_SEC)

    try:
        while True:
            t0 = time.monotonic()
            _, mins_to_close = _active_market_hour_and_minutes()

            # Dynamic interval: tighten to 1 min in the burst window
            if use_dynamic:
                interval = (
                    POLL_INTERVAL_BURST_SEC
                    if mins_to_close < BURST_WINDOW_MINS
                    else POLL_INTERVAL_SEC
                )
            else:
                interval = poll_interval_sec

            added = poll_once(observations, api_key, location_key)
            _save_json(observations)

            elapsed = time.monotonic() - t0
            next_at = datetime.now(timezone.utc) + timedelta(seconds=interval)
            log.info("  +%d rows  (%d total)  %.1fs  interval=%dmin  next at %s",
                     added, len(observations), elapsed, interval // 60,
                     next_at.strftime("%H:%M:%S UTC"))

            time.sleep(max(0, interval - elapsed))

    except KeyboardInterrupt:
        _save_json(observations)
        log.info("stopped by user. %d total observations saved.", len(observations))


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import os

    parser = argparse.ArgumentParser(
        description="Passive observer for Kalshi hourly NYC temperature markets"
    )
    parser.add_argument(
        "--poll",
        type    = int,
        default = None,
        metavar = "MINUTES",
        help    = (
            "Fixed poll interval in minutes. Omit for dynamic mode: "
            f"{POLL_INTERVAL_SEC//60}min normally, "
            f"{POLL_INTERVAL_BURST_SEC//60}min when < {BURST_WINDOW_MINS}min to close."
        ),
    )
    args = parser.parse_args()

    # Load credentials from data/config.json if present
    config_file = Path("data/config.json")
    if config_file.exists():
        try:
            config = json.loads(config_file.read_text())
            if config.get("key_id"):
                os.environ.setdefault("KALSHI_KEY_ID",   config["key_id"])
            if config.get("key_file"):
                os.environ.setdefault("KALSHI_KEY_FILE", config["key_file"])
        except Exception:
            pass

    main(poll_interval_sec=args.poll * 60 if args.poll else POLL_INTERVAL_SEC)
