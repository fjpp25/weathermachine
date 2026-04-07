"""
scheduler.py
------------
Runs the full trading pipeline on a polling loop with a dynamic interval
that tightens around the peak trading window.

Interval schedule (per city local time):
  Before 10am  → 15 min  (just waiting)
  10am–11am    →  5 min  (window just opened)
  11am–1pm     →  3 min  (peak — forecasts updating, most movement)
  1pm–2pm      →  5 min  (approaching cutoff)
  After 2pm    → 10 min  (exit monitoring only)

Usage:
  python scheduler.py                   # live, dynamic interval
  python scheduler.py --paper           # paper mode, no real orders
  python scheduler.py --interval 5      # override interval (minutes)
  python scheduler.py --city Miami      # single city only
"""

import os
import time
import argparse
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from pathlib import Path

import trader
import decision_engine

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

ACTIVITY_START_HOUR = 9    # local city time — start polling
ACTIVITY_END_HOUR   = 17   # local city time — stop polling

CITY_TIMEZONES = {
    "New York":      "America/New_York",
    "Chicago":       "America/Chicago",
    "Miami":         "America/New_York",
    "Austin":        "America/Chicago",
    "Los Angeles":   "America/Los_Angeles",
    "San Francisco": "America/Los_Angeles",
    "Denver":        "America/Denver",
    "Philadelphia":  "America/New_York",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _filter_cities(city_filter: str = None) -> dict:
    if city_filter:
        return {k: v for k, v in CITY_TIMEZONES.items()
                if k.lower() == city_filter.lower()}
    return CITY_TIMEZONES


def local_hour(tz_name: str) -> int:
    return datetime.now(ZoneInfo(tz_name)).hour


def any_city_active(city_filter: str = None) -> bool:
    cities = _filter_cities(city_filter)
    return any(
        ACTIVITY_START_HOUR <= local_hour(tz) < ACTIVITY_END_HOUR
        for tz in cities.values()
    )


def all_cities_done(city_filter: str = None) -> bool:
    cities = _filter_cities(city_filter)
    return all(local_hour(tz) >= ACTIVITY_END_HOUR for tz in cities.values())


def next_active_city_time(city_filter: str = None) -> str:
    cities        = _filter_cities(city_filter)
    earliest_utc  = None
    earliest_city = None

    for city, tz in cities.items():
        now_local = datetime.now(ZoneInfo(tz))
        if now_local.hour < ACTIVITY_START_HOUR:
            opens_local = now_local.replace(
                hour=ACTIVITY_START_HOUR, minute=0, second=0, microsecond=0
            )
            opens_utc = opens_local.astimezone(timezone.utc)
            if earliest_utc is None or opens_utc < earliest_utc:
                earliest_utc  = opens_utc
                earliest_city = city

    if earliest_utc:
        wait_mins = int((earliest_utc - datetime.now(timezone.utc)).total_seconds() / 60)
        return f"{earliest_city} opens in ~{wait_mins} min ({earliest_utc.strftime('%H:%M UTC')})"

    return "all windows closed for today"


def dynamic_interval(city_filter: str = None) -> int:
    """
    Returns poll interval in seconds based on the most active city phase.
    Takes the minimum (most frequent) interval across all active cities.

    11am-1pm local is the peak window — NWS model runs land around this
    time and the market reprices fastest, so we poll most aggressively.
    """
    cities       = _filter_cities(city_filter)
    min_interval = 15 * 60   # default: 15 min

    for tz in cities.values():
        h = local_hour(tz)
        if 11 <= h < 13:
            min_interval = min(min_interval, 3 * 60)    # peak — 3 min
        elif h == 10 or h == 13:
            min_interval = min(min_interval, 5 * 60)    # opening/closing — 5 min
        elif 14 <= h < ACTIVITY_END_HOUR:
            min_interval = min(min_interval, 10 * 60)   # exits only — 10 min

    return min_interval


def fmt_now() -> str:
    utc_now    = datetime.now(timezone.utc)
    lisbon_now = utc_now.astimezone(ZoneInfo("Europe/Lisbon"))
    return (f"{utc_now.strftime('%H:%M UTC')} "
            f"/ {lisbon_now.strftime('%H:%M %Z')}")


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def run_scheduler(
    paper:             bool = False,
    city_filter:       str  = None,
    interval_override: int  = None,
):
    # Load credentials from config file if present, fall back to env vars
    config_file = Path("data/config.json")
    if config_file.exists():
        try:
            import json
            config = json.loads(config_file.read_text())
            if config.get("key_id"):
                os.environ.setdefault("KALSHI_KEY_ID", config["key_id"])
            if config.get("key_file"):
                os.environ.setdefault("KALSHI_KEY_FILE", config["key_file"])
            os.environ["KALSHI_DEMO"] = "false" if config.get("live_mode") else "true"
        except Exception:
            pass
    else:
        # Fall back to .env file for terminal use
        env_file = Path(".env")
        if env_file.exists():
            for line in env_file.read_text().splitlines():
                if "=" in line and not line.startswith("#"):
                    k, v = line.split("=", 1)
                    os.environ.setdefault(k.strip(), v.strip())

    client   = trader.make_client()
    mode_str = "PAPER" if paper else ("DEMO" if client.demo else "LIVE")

    print(f"\n{'='*65}")
    print(f"  Kalshi Weather Scheduler  [{mode_str}]")
    print(f"  Interval      : {'dynamic' if not interval_override else f'{interval_override} min (override)'}")
    print(f"  Trading window: {decision_engine.TRADE_WINDOW_START}:00–"
          f"{decision_engine.TRADE_WINDOW_END}:00 local per city")
    print(f"  Activity range: {ACTIVITY_START_HOUR}:00–{ACTIVITY_END_HOUR}:00 local per city")
    if city_filter:
        print(f"  City filter   : {city_filter}")
    print(f"{'='*65}\n")

    poll_count = 0

    while True:
        now_str = fmt_now()

        # ── Wait if no city is active yet ────────────────────────────────
        if not any_city_active(city_filter):
            if all_cities_done(city_filter):
                print(f"[{now_str}] All cities past activity window. Done for today.")
                break
            else:
                next_str = next_active_city_time(city_filter)
                print(f"[{now_str}] No city active yet — {next_str}")
                time.sleep(15 * 60)
                continue

        # ── Determine interval for this cycle ────────────────────────────
        interval_secs = (
            interval_override * 60
            if interval_override
            else dynamic_interval(city_filter)
        )

        poll_count += 1
        print(f"\n[{now_str}] Poll #{poll_count}  "
              f"(interval: {interval_secs // 60} min)")
        print(f"{'-'*55}")

        # ── Run decision engine + execute signals ────────────────────────
        try:
            trader.run_pipeline(
                client      = client,
                city_filter = city_filter,
                paper       = paper,
            )
        except Exception as e:
            print(f"  Pipeline error: {e}")

        # ── Check exits — count from Kalshi, not local file ───────────────
        try:
            live_positions = trader.sync_from_kalshi(client)
            if live_positions:
                print(f"\n  Checking exits ({len(live_positions)} open positions)...")
                trader.check_exits(client, paper=paper)
            else:
                print(f"  No open positions to monitor.")
        except Exception as e:
            print(f"  Exit check error: {e}")

        # ── Balance summary ───────────────────────────────────────────────
        try:
            balance    = trader.get_balance(client)
            deployable = round(balance * 0.70, 2)
            print(f"\n  Balance: ${balance:.2f}  |  Deployable: ${deployable:.2f}")
        except Exception:
            pass

        # ── Sleep ────────────────────────────────────────────────────────
        next_poll = datetime.now(timezone.utc) + timedelta(seconds=interval_secs)
        print(f"  Next poll: {next_poll.strftime('%H:%M UTC')}")
        time.sleep(interval_secs)

    print(f"\n[{fmt_now()}] Scheduler finished.")


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kalshi weather trading scheduler")
    parser.add_argument("--paper",    action="store_true",
                        help="Paper mode — no real orders")
    parser.add_argument("--city",     type=str, default=None,
                        help="Filter to one city (e.g. 'Miami')")
    parser.add_argument("--interval", type=int, default=None, metavar="MINUTES",
                        help="Override poll interval in minutes (default: dynamic)")
    args = parser.parse_args()

    try:
        run_scheduler(
            paper             = args.paper,
            city_filter       = args.city,
            interval_override = args.interval,
        )
    except KeyboardInterrupt:
        print(f"\n\n  Interrupted. Final position summary:")
        trader.display_positions()
