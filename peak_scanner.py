"""
peak_scanner.py
---------------
Watches the intraday temperature trajectory for each city and enters
No positions on brackets that become physically unreachable once the
daily high has been confirmed.

How peak confirmation works
---------------------------
  observed_high_f (from NWS) is the cumulative daily maximum — it only
  ever increases. When it stops moving AND current_temp_f has dropped
  meaningfully below it, the day's high is almost certainly locked in.

  Confirmation requires ALL of:
    1. local hour ≥ PEAK_MIN_HOUR (don't call peaks before 1pm)
    2. observed_high_f unchanged for ≥ PEAK_STABLE_POLLS consecutive polls
    3. current_temp_f ≤ observed_high_f − PEAK_DROP_THRESHOLD

  Once confirmed, any HIGH bracket whose floor > observed_high_f + BRACKET_BUFFER
  is physically impossible — a valid No entry.

State (in-memory, intentionally ephemeral)
------------------------------------------
  _obs_history  : per-city deque of (poll_time, obs_high_f, current_temp_f)
  _peak_state   : per-city dict tracking confirmed peak and stability count
  _fired_tickers: set of tickers already entered this session

Architecture
------------
  Mirrors tomorrow_scanner — runs in parallel with run_pipeline() in the
  scheduler. Has no shared mutable state with other modules. Calls
  nws_feed.snapshot() independently (free API, no rate concern).

  on_signal() is currently a stub — wire in trader.place_order() when ready.
"""

from __future__ import annotations

import logging
from collections import deque
from datetime import datetime, timezone
from typing import Optional
from zoneinfo import ZoneInfo

import nws_feed
from cities import TRADING_CITIES as _CITY_REGISTRY

# ---------------------------------------------------------------------------
# Logging — structured, timestamped, easy to grep
# ---------------------------------------------------------------------------

log = logging.getLogger("peak_scanner")

if not log.handlers:
    _handler = logging.StreamHandler()
    _handler.setFormatter(logging.Formatter(
        "%(asctime)s  [peak_scanner]  %(levelname)-8s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S UTC",
    ))
    _handler.formatter.converter = lambda *a: datetime.now(timezone.utc).timetuple()
    log.addHandler(_handler)
    log.setLevel(logging.DEBUG)


# ---------------------------------------------------------------------------
# Tunable parameters
# ---------------------------------------------------------------------------

PEAK_MIN_HOUR        = 13     # local hour — never confirm a peak before 1pm
PEAK_STABLE_POLLS    = 3      # observed_high_f must be unchanged for this many polls
PEAK_DROP_THRESHOLD  = 2.0    # current_temp must be this many °F below observed_high
BRACKET_BUFFER       = 1.0    # bracket floor must be this far above observed_high
HISTORY_MAXLEN       = 24     # keep last 24 readings per city (~2h at 5-min polling)

# Minimum No price to enter — don't chase already-converged brackets
MIN_NO_PRICE         = 0.75
MAX_NO_PRICE         = 0.97   # skip if essentially already at 1.0


# ---------------------------------------------------------------------------
# Per-city state
# ---------------------------------------------------------------------------

# { city: deque[(poll_time_utc, obs_high_f, current_temp_f)] }
_obs_history: dict[str, deque] = {}

# { city: {"confirmed": bool, "peak_f": float, "confirmed_at": datetime,
#           "stable_count": int, "last_obs_high": float} }
_peak_state: dict[str, dict] = {}

# Tickers already entered this session — never re-enter
_fired_tickers: set[str] = set()


def _init_city(city: str) -> None:
    if city not in _obs_history:
        _obs_history[city] = deque(maxlen=HISTORY_MAXLEN)
    if city not in _peak_state:
        _peak_state[city] = {
            "confirmed":    False,
            "peak_f":       None,
            "confirmed_at": None,
            "stable_count": 0,
            "last_obs_high": None,
        }


def _city_local_hour(tz_name: str) -> int:
    return datetime.now(ZoneInfo(tz_name)).hour


def _today_str(tz_name: str) -> str:
    dt = datetime.now(ZoneInfo(tz_name))
    return dt.strftime("%y") + dt.strftime("%b").upper() + dt.strftime("%d")


def _high_series(meta: dict) -> Optional[str]:
    return meta.get("high_series") or meta.get("high")


# ---------------------------------------------------------------------------
# Market fetching (today's HIGH brackets)
# ---------------------------------------------------------------------------

def _fetch_today_brackets(client, series: str, today: str) -> list[dict]:
    """
    Fetch all open HIGH brackets for today's market.
    Returns list of {ticker, bracket, floor, cap, no_price, yes_price}.
    """
    try:
        resp = client.get_markets(series_ticker=series, status="open")
        markets_raw = resp.get("markets", []) if isinstance(resp, dict) else []
        results = []
        for m in markets_raw:
            ticker = m.get("ticker", "")
            if today.upper() not in ticker.upper():
                continue
            bracket = ticker.split("-")[-1] if "-" in ticker else ""
            yes_p   = float(m.get("yes_bid_dollars") or m.get("yes_ask") or 0)
            no_p    = float(m.get("no_bid_dollars")  or m.get("no_ask")  or 0)

            # Parse floor/cap from bracket string
            floor, cap = None, None
            try:
                if bracket.startswith("B"):
                    cap   = float(bracket[1:])
                    floor = round(cap - 2.0, 1)
                elif bracket.startswith("T"):
                    val = float(bracket[1:])
                    cap = round(val - 0.5, 1)   # bottom T: "below X°F"
            except ValueError:
                pass

            results.append({
                "ticker":    ticker,
                "bracket":   bracket,
                "floor":     floor,
                "cap":       cap,
                "yes_price": yes_p,
                "no_price":  no_p,
            })
        return results
    except Exception as e:
        log.warning("bracket fetch failed for %s: %s", series, e)
        return []


# ---------------------------------------------------------------------------
# Peak detection
# ---------------------------------------------------------------------------

def _update_peak_state(
    city: str,
    poll_time: datetime,
    obs_high: float,
    current_temp: float,
    local_hour: int,
) -> bool:
    """
    Update temperature history and peak state for a city.
    Returns True if peak just became confirmed this poll.
    """
    _init_city(city)
    state   = _peak_state[city]
    history = _obs_history[city]

    history.append((poll_time, obs_high, current_temp))

    # ── Track stability of observed_high_f ───────────────────────────────
    last_obs = state["last_obs_high"]
    if last_obs is None or obs_high > last_obs:
        # New high — reset stability counter
        if last_obs is not None and obs_high > last_obs:
            log.debug("%-16s  new obs_high: %.1f°F (was %.1f°F)  "
                      "hour=%d  resetting stability count",
                      city, obs_high, last_obs, local_hour)
        state["stable_count"]  = 1
        state["last_obs_high"] = obs_high
        state["confirmed"]     = False   # peak moved, re-evaluate
    else:
        state["stable_count"] += 1

    # ── Log current status every poll ────────────────────────────────────
    drop = obs_high - current_temp
    log.debug("%-16s  obs_high=%.1f°F  current=%.1f°F  drop=%.1f°F  "
              "stable=%d/%d  hour=%d  peak_confirmed=%s",
              city, obs_high, current_temp, drop,
              state["stable_count"], PEAK_STABLE_POLLS,
              local_hour, state["confirmed"])

    # ── Check confirmation conditions ─────────────────────────────────────
    if state["confirmed"]:
        return False   # already confirmed — nothing new

    if local_hour < PEAK_MIN_HOUR:
        return False   # too early

    stable_enough = state["stable_count"] >= PEAK_STABLE_POLLS
    drop_enough   = drop >= PEAK_DROP_THRESHOLD

    if stable_enough and drop_enough:
        state["confirmed"]    = True
        state["peak_f"]       = obs_high
        state["confirmed_at"] = poll_time
        log.info("%-16s  PEAK CONFIRMED  %.1f°F  "
                 "(stable %d polls, drop %.1f°F, hour %d)",
                 city, obs_high, state["stable_count"], drop, local_hour)
        return True

    if not stable_enough:
        log.debug("%-16s  waiting for stability: %d/%d polls",
                  city, state["stable_count"], PEAK_STABLE_POLLS)
    if not drop_enough:
        log.debug("%-16s  waiting for drop: %.1f°F / %.1f°F required",
                  city, drop, PEAK_DROP_THRESHOLD)

    return False


# ---------------------------------------------------------------------------
# Core scanner
# ---------------------------------------------------------------------------

def run_scan(client, city_filter: str = None, paper: bool = False) -> None:
    """
    Called every poll cycle. For each city:
      1. Fetch current NWS temps
      2. Update peak state
      3. If peak confirmed, scan for No entries above the confirmed peak
    """
    poll_time = datetime.now(timezone.utc)
    log.debug("── poll %s ──────────────────────────────────────────────────",
              poll_time.strftime("%H:%M:%S UTC"))

    # ── Fetch NWS snapshot ────────────────────────────────────────────────
    try:
        nws_data = nws_feed.snapshot(city_filter)
    except Exception as e:
        log.error("NWS snapshot failed: %s — skipping poll", e)
        return

    cities = {
        name: meta for name, meta in _CITY_REGISTRY.items()
        if city_filter is None or name.lower() == city_filter.lower()
    }

    for city, meta in cities.items():
        tz         = meta["tz"]
        local_hour = _city_local_hour(tz)
        nws        = nws_data.get(city, {})
        obs_high   = nws.get("observed_high_f")
        current_t  = nws.get("current_temp_f")

        if obs_high is None or current_t is None:
            log.debug("%-16s  no NWS data (obs_high=%s  current=%s) — skipping",
                      city, obs_high, current_t)
            continue

        # ── Update peak detection state ───────────────────────────────────
        just_confirmed = _update_peak_state(
            city, poll_time, obs_high, current_t, local_hour
        )

        # ── If peak confirmed (now or previously), scan brackets ──────────
        state = _peak_state.get(city, {})
        if not state.get("confirmed"):
            continue

        confirmed_peak = state["peak_f"]
        series         = _high_series(meta)
        if not series:
            log.warning("%-16s  no HIGH series in city config — skipping", city)
            continue

        today    = _today_str(tz)
        brackets = _fetch_today_brackets(client, series, today)

        if not brackets:
            log.debug("%-16s  no open brackets for %s", city, today)
            continue

        # ── Find eligible brackets ────────────────────────────────────────
        eligible = [
            b for b in brackets
            if b["floor"] is not None
            and b["floor"] > confirmed_peak + BRACKET_BUFFER
            and MIN_NO_PRICE <= b["no_price"] <= MAX_NO_PRICE
            and b["ticker"] not in _fired_tickers
        ]

        if just_confirmed or eligible:
            log.info("%-16s  peak=%.1f°F  confirmed_at=%s  "
                     "open brackets=%d  eligible=%d",
                     city, confirmed_peak,
                     state["confirmed_at"].strftime("%H:%M UTC"),
                     len(brackets), len(eligible))

        for b in eligible:
            log.info("%-16s  SIGNAL  %s  No=%.2f  "
                     "(floor=%.1f°F  peak=%.1f°F  gap=+%.1f°F)%s",
                     city, b["ticker"], b["no_price"],
                     b["floor"], confirmed_peak,
                     b["floor"] - confirmed_peak,
                     "  [PAPER]" if paper else "")
            _fired_tickers.add(b["ticker"])
            on_signal(client, city, b, confirmed_peak, paper)


# ---------------------------------------------------------------------------
# Signal handler — wire in order execution here
# ---------------------------------------------------------------------------

def on_signal(
    client,
    city:           str,
    bracket:        dict,
    confirmed_peak: float,
    paper:          bool = False,
) -> None:
    """
    Called when a No entry signal fires for a bracket above the confirmed peak.
    Currently logs only. Wire in trader.place_order() when ready.

    Args:
        bracket:        {ticker, bracket, floor, cap, no_price, yes_price}
        confirmed_peak: the confirmed daily high in °F
    """
    tag = "[PAPER] " if paper else ""
    log.info("%-16s  %swould buy No on %s @ %.2f  "
             "(floor=%.1f°F > peak=%.1f°F)",
             city, tag, bracket["ticker"], bracket["no_price"],
             bracket["floor"], confirmed_peak)

    # TODO: wire in order execution, e.g.:
    # try:
    #     trader.place_order(
    #         client        = client,
    #         ticker        = bracket["ticker"],
    #         side          = "no",
    #         price_dollars = bracket["no_price"],
    #         contracts     = 1,
    #         paper         = paper,
    #     )
    # except Exception as e:
    #     log.error("order failed for %s: %s", bracket["ticker"], e)


# ---------------------------------------------------------------------------
# Startup summary
# ---------------------------------------------------------------------------

def log_config() -> None:
    """Print tunable parameters at startup."""
    log.info("peak scanner initialised  "
             "PEAK_MIN_HOUR=%dh  PEAK_STABLE_POLLS=%d  "
             "PEAK_DROP_THRESHOLD=%.1f°F  BRACKET_BUFFER=%.1f°F  "
             "entry=[%.2f–%.2f]",
             PEAK_MIN_HOUR, PEAK_STABLE_POLLS,
             PEAK_DROP_THRESHOLD, BRACKET_BUFFER,
             MIN_NO_PRICE, MAX_NO_PRICE)
