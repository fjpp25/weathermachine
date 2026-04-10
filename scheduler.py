"""
scheduler.py
------------
Runs the full trading pipeline on a polling loop with a dynamic interval
that tightens around the peak trading window.

Interval schedule (per city local time):
  Overnight (midnight–9am)  → 10 min  (bracket elimination from obs temps)
  9am–11am                  →  5 min  (approaching peak, forecasts updating)
  11am–1pm                  →  3 min  (peak — NWS model runs, market reprices fastest)
  1pm–3pm                   →  5 min  (post-peak, convergence settling)
  3pm–midnight               → 10 min  (exit monitoring, slow convergence)

Usage:
  python scheduler.py                   # live, dynamic interval
  python scheduler.py --paper           # paper mode, no real orders
  python scheduler.py --interval 5      # override interval (minutes)
  python scheduler.py --city Miami      # single city only
"""

import os
import json
import time
import argparse
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from pathlib import Path

import trader
import decision_engine
from cities import TRADING_CITIES as _CITY_REGISTRY

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

ACTIVITY_START_HOUR = 0    # local city time — poll from midnight
ACTIVITY_END_HOUR   = 23   # local city time — poll all day


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _filter_cities(city_filter: str = None) -> dict[str, str]:
    """Return {city: tz_name} for active trading cities, optionally filtered."""
    cities = {name: meta["tz"] for name, meta in _CITY_REGISTRY.items()}
    if city_filter:
        return {k: v for k, v in cities.items() if k.lower() == city_filter.lower()}
    return cities


def local_hour(tz_name: str) -> int:
    return datetime.now(ZoneInfo(tz_name)).hour


def dynamic_interval(city_filter: str = None) -> int:
    """
    Returns poll interval in seconds based on the most active city phase.
    Takes the minimum (most frequent) interval across all active cities.

    Overnight (midnight–9am): 10 min — bracket elimination from obs temps
    9am–11am:  5 min  — approaching peak, forecasts updating
    11am–1pm:  3 min  — peak — NWS model runs, market reprices fastest
    1pm–3pm:   5 min  — post-peak, convergence settling
    3pm–11pm: 10 min  — exit monitoring, slow convergence
    """
    cities       = _filter_cities(city_filter)
    min_interval = 10 * 60   # default: 10 min

    for tz in cities.values():
        h = local_hour(tz)
        if 11 <= h < 13:
            min_interval = min(min_interval, 3 * 60)    # peak — 3 min
        elif h in (9, 10, 13, 14):
            min_interval = min(min_interval, 5 * 60)    # shoulder — 5 min

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

        # ── Check exits — reuse positions already fetched by run_pipeline ──
        try:
            live_positions = trader.sync_from_kalshi(client)
            exited = trader.check_exits(client, paper=paper, live_positions=live_positions)
            if exited:
                print(f"\n  Exited {len(exited)} position(s): {list(exited.values())}")
            else:
                print(f"  No exits triggered.")
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
                        help="Override dynamic interval (minutes)")
    args = parser.parse_args()

    run_scheduler(
        paper             = args.paper,
        city_filter       = args.city,
        interval_override = args.interval,
    )
