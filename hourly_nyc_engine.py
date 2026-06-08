"""
hourly_nyc_engine.py
--------------------
Trading engine for Kalshi's NYC hourly temperature markets (KXTEMPNYCH).

Signal
------
Buy No on brackets where AccuWeather's hourly forecast for the settlement
hour says the temperature will NOT reach the threshold:

    forecast_resolves_yes = False  (forecast_f < threshold_f)
    no_ask in [NO_MIN, NO_MAX)
    not already entered this ticker

Backtest (May 5 – Jun 5 2026, 34 days, 10,076 resolved brackets):
  forecast No + no in [0.75, 0.95):  n=311  WR=99.4%  EV=+$0.13/contract

By price band (forecast signal):
  [0.75, 0.80): 100.0% WR  EV=+$0.23
  [0.80, 0.85): 100.0% WR  EV=+$0.18
  [0.85, 0.90): 100.0% WR  EV=+$0.13
  [0.90, 0.95):  98.3% WR  EV=+$0.06

Why it works
------------
Kalshi settles on AccuWeather's current conditions reading at market close.
When the AccuWeather hourly forecast (the SAME data source) says the
temperature won't reach a threshold, the market's residual uncertainty
is almost always noise. The forecast is the settlement oracle — when it
disagrees with a threshold, that bracket is structurally dead.

Market structure
----------------
  Series      : KXTEMPNYCH
  Format      : KXTEMPNYCH-{YYMONDD}{HH}  e.g. KXTEMPNYCH-26MAY0514
  Brackets    : T-type only (X°F or above), ~20 per market, 1°F spacing
  Window      : opens H-1:00 EDT, closes at H:00 EDT
  Settlement  : AccuWeather current conditions at Central Park at close

Capital
-------
Uses the 'hourly' budget allocation. Flat sizing: MAX_CONTRACTS per entry.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional
from zoneinfo import ZoneInfo

import requests

import accuweather_feed
from log_setup import get_logger

log = get_logger(__name__)

# ---------------------------------------------------------------------------
# Parameters
# ---------------------------------------------------------------------------

SERIES_TICKER    = "KXTEMPNYCH"
NYC_TZ           = ZoneInfo("America/New_York")
KALSHI_API_BASE  = "https://api.elections.kalshi.com/trade-api/v2"
REQUEST_TIMEOUT  = 10

NO_MIN_ENTRY     = 0.75    # minimum No ask to consider
NO_MAX_ENTRY     = 0.95    # above this, payout too small (< 5¢/contract)
MAX_CONTRACTS    = 3       # flat sizing — thin markets, don't move the book
MIN_MINUTES_LEFT = 5       # don't enter with < 5 min to close

# ---------------------------------------------------------------------------
# Session state
# ---------------------------------------------------------------------------

_fired: set[str] = set()   # tickers entered this session

# ---------------------------------------------------------------------------
# AccuWeather helpers
# ---------------------------------------------------------------------------

# In-process cache: {hour_edt: (fetched_at_utc, temp_f)}
_hourly_cache: dict[int, tuple[datetime, Optional[float]]] = {}
_CACHE_SECS = 1800   # 30 min


def _api_key() -> Optional[str]:
    return accuweather_feed._api_key()


def _nyc_location_key() -> Optional[str]:
    cache = accuweather_feed._load_location_cache()
    key = cache.get("New York")
    if key:
        return key
    api_key = _api_key()
    if not api_key:
        return None
    return accuweather_feed.ensure_location_keys(api_key).get("New York")


def _fetch_hourly_forecast(location_key: str, api_key: str,
                           target_hour_edt: int) -> Optional[float]:
    """Fetch AccuWeather hourly forecast for the given EDT hour (cached 30 min)."""
    now_utc = datetime.now(timezone.utc)
    cached = _hourly_cache.get(target_hour_edt)
    if cached:
        fetched_at, val = cached
        if (now_utc - fetched_at).total_seconds() < _CACHE_SECS:
            return val
    try:
        resp = requests.get(
            f"{accuweather_feed.BASE_URL}/forecasts/v1/hourly/12hour/{location_key}",
            params={"apikey": api_key, "details": "false", "metric": "false"},
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        for entry in resp.json():
            dt_str = entry.get("DateTime", "")
            if not dt_str:
                continue
            try:
                dt_edt = datetime.fromisoformat(dt_str).astimezone(NYC_TZ)
                val = entry.get("Temperature", {}).get("Value")
                temp_f = float(val) if val is not None else None
                _hourly_cache[dt_edt.hour] = (now_utc, temp_f)
            except Exception:
                continue
        result = _hourly_cache.get(target_hour_edt)
        return result[1] if result else None
    except Exception as e:
        log.warning("hourly_nyc_engine: forecast fetch failed: %s", e)
        return None


# ---------------------------------------------------------------------------
# Market helpers
# ---------------------------------------------------------------------------

def _active_market_hour() -> tuple[int, int]:
    """Return (market_hour_edt, minutes_to_close)."""
    now_edt = datetime.now(NYC_TZ)
    market_hour = (now_edt.hour + 1) % 24
    minutes_to_close = 60 - now_edt.minute
    return market_hour, minutes_to_close


def _event_ticker(market_hour_edt: int) -> str:
    now_edt = datetime.now(NYC_TZ)
    date_str = now_edt.strftime("%y%b%d").upper()
    return f"{SERIES_TICKER}-{date_str}{market_hour_edt:02d}"


def _fetch_brackets(event_ticker: str) -> list[dict]:
    try:
        resp = requests.get(
            f"{KALSHI_API_BASE}/markets",
            params={"series_ticker": SERIES_TICKER, "status": "open"},
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        markets = resp.json().get("markets", [])
        return [m for m in markets
                if m.get("event_ticker", "").upper() == event_ticker.upper()]
    except Exception as e:
        log.warning("hourly_nyc_engine: bracket fetch failed for %s: %s",
                    event_ticker, e)
        return []


def _threshold(market: dict) -> Optional[float]:
    floor = market.get("floor_strike")
    if floor is not None:
        try:
            return float(floor)
        except (ValueError, TypeError):
            pass
    ticker = market.get("ticker", "")
    code = ticker.split("-")[-1] if "-" in ticker else ""
    if code.startswith("T"):
        try:
            return float(code[1:])
        except ValueError:
            pass
    return None


def _no_ask(market: dict) -> Optional[float]:
    """No ask = 1 - yes_bid (standard Kalshi approximation)."""
    yes_bid = market.get("yes_bid_dollars")
    if yes_bid is not None:
        try:
            v = float(yes_bid)
            if v > 0:
                return round(1.0 - v, 4)
        except (ValueError, TypeError):
            pass
    return None


# ---------------------------------------------------------------------------
# Core scan
# ---------------------------------------------------------------------------

def run_scan(
    client,
    paper: bool = False,
) -> None:
    """
    Scan the currently active NYC hourly market and place No orders
    on brackets where the AccuWeather forecast says the temperature
    won't reach the threshold.
    """
    import trader as _trader

    market_hour, mins_to_close = _active_market_hour()

    if mins_to_close < MIN_MINUTES_LEFT:
        log.debug("hourly_nyc: %d min to close — too late to enter", mins_to_close)
        return

    evt_ticker = _event_ticker(market_hour)

    # ── AccuWeather forecast ──────────────────────────────────────────────
    api_key = _api_key()
    if not api_key:
        log.warning("hourly_nyc: no AccuWeather API key")
        return

    location_key = _nyc_location_key()
    if not location_key:
        log.warning("hourly_nyc: no NYC location key")
        return

    forecast_f = _fetch_hourly_forecast(location_key, api_key, market_hour)
    if forecast_f is None:
        log.debug("hourly_nyc: no forecast for hour %d — skipping", market_hour)
        return

    # ── Kalshi brackets ───────────────────────────────────────────────────
    brackets = _fetch_brackets(evt_ticker)
    if not brackets:
        log.debug("hourly_nyc: no open brackets for %s", evt_ticker)
        return

    # ── Evaluate each bracket ─────────────────────────────────────────────
    signals_fired = 0
    for market in brackets:
        ticker = market.get("ticker", "")
        if not ticker or ticker in _fired:
            continue

        threshold_f = _threshold(market)
        if threshold_f is None:
            continue

        # Core signal: forecast says temperature won't reach this threshold
        forecast_resolves_yes = forecast_f >= threshold_f
        if forecast_resolves_yes:
            continue   # forecast says Yes — skip

        # Price gate
        no_p = _no_ask(market)
        if no_p is None or not (NO_MIN_ENTRY <= no_p < NO_MAX_ENTRY):
            continue

        # Capital check
        cost = round(no_p * MAX_CONTRACTS, 4)
        try:
            deployable = _trader.get_engine_capital().remaining("hourly")
            if deployable < cost:
                log.debug("hourly_nyc: hourly budget exhausted (need=$%.2f)", cost)
                continue
        except Exception:
            pass   # proceed without cap check if unavailable

        log.info(
            "HOURLY_NYC  %s  No=%.2f  threshold=%.1f°F  forecast=%.1f°F  "
            "dist=%.1f°F  %dmin_left  %dc",
            ticker, no_p, threshold_f, forecast_f,
            threshold_f - forecast_f, mins_to_close, MAX_CONTRACTS,
        )

        _fired.add(ticker)

        if not paper:
            try:
                _trader.place_order(
                    client        = client,
                    ticker        = ticker,
                    side          = "no",
                    price_dollars = no_p,
                    contracts     = MAX_CONTRACTS,
                    paper         = False,
                )
                _trader._append_trade_log({
                    "ticker":       ticker,
                    "city":         "New York",
                    "side":         "no",
                    "market_type":  "hourly_high",
                    "score":        4,
                    "score_detail": [
                        "forecast_resolves_yes=False",
                        f"forecast={forecast_f:.1f}°F",
                        f"threshold={threshold_f:.1f}°F",
                        f"dist={threshold_f-forecast_f:.1f}°F",
                        f"mins_left={mins_to_close}",
                    ],
                    "entry_price":  no_p,
                    "contracts":    MAX_CONTRACTS,
                    "placed_at":    datetime.now(timezone.utc).isoformat(),
                    "paper":        False,
                    "entry_tier":   "hourly_nyc",
                })
                try:
                    _trader.get_engine_capital().record("hourly", cost)
                except Exception:
                    pass
                signals_fired += 1
            except Exception as e:
                log.error("hourly_nyc: order failed %s: %s", ticker, e)
                _fired.discard(ticker)
        else:
            log.info("  [PAPER] would place No %dc @ $%.2f on %s",
                     MAX_CONTRACTS, no_p, ticker)
            signals_fired += 1

    if signals_fired:
        log.info("hourly_nyc: %d order(s) placed for %s  (forecast=%.1f°F)",
                 signals_fired, evt_ticker, forecast_f)


# ---------------------------------------------------------------------------
# Config log — called once at scheduler startup
# ---------------------------------------------------------------------------

def log_config() -> None:
    log.info(
        "hourly_nyc: NO=[%.2f, %.2f)  contracts=%d  signal=forecast_resolves_yes=False",
        NO_MIN_ENTRY, NO_MAX_ENTRY, MAX_CONTRACTS,
    )


# ---------------------------------------------------------------------------
# Standalone entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import json, os

    parser = argparse.ArgumentParser(
        description="NYC hourly temperature market trader"
    )
    parser.add_argument("--paper", action="store_true")
    args = parser.parse_args()

    config_file = Path("data/config.json")
    if config_file.exists():
        try:
            config = json.loads(config_file.read_text())
            if config.get("key_id"):
                os.environ.setdefault("KALSHI_KEY_ID", config["key_id"])
            if config.get("key_file"):
                os.environ.setdefault("KALSHI_KEY_FILE", config["key_file"])
            os.environ["KALSHI_DEMO"] = "false" if config.get("live_mode") else "true"
        except Exception:
            pass

    import trader
    client = trader.make_client(skip_confirmation=True)
    log_config()
    run_scan(client=client, paper=args.paper)
