"""
scheduler.py
------------
Runs the full trading pipeline on a polling loop with a dynamic interval
that tightens around the peak trading window.

Interval schedule (per city local time):
  Overnight (midnight–9am)  →  5 min  (bracket elimination + next-market scan)
  9am–11am                  →  5 min  (approaching peak, forecasts updating)
  11am–1pm                  →  3 min  (peak — NWS model runs, market reprices fastest)
  1pm–3pm                   →  5 min  (post-peak, convergence settling)
  3pm–midnight               →  5 min  (next-market may appear at any time)

Logging:
  All output uses Python logging with UTC timestamps.
  Set LOG_LEVEL=DEBUG for verbose per-city detail.
  Set LOG_FILE=logs/scheduler.log to write a rotating file alongside stdout.

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
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from pathlib import Path

from log_setup import get_logger

import trader
import hight_decision_engine as decision_engine
import tomorrow_scanner
import peak_scanner
from cities import TRADING_CITIES as _CITY_REGISTRY

log = get_logger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

ACTIVITY_START_HOUR = 0
ACTIVITY_END_HOUR   = 23


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _filter_cities(city_filter: str = None) -> dict[str, str]:
    cities = {name: meta["tz"] for name, meta in _CITY_REGISTRY.items()}
    if city_filter:
        return {k: v for k, v in cities.items() if k.lower() == city_filter.lower()}
    return cities


def local_hour(tz_name: str) -> int:
    return datetime.now(ZoneInfo(tz_name)).hour


def dynamic_interval(city_filter: str = None) -> int:
    cities       = _filter_cities(city_filter)
    min_interval = 5 * 60
    for tz in cities.values():
        if 11 <= local_hour(tz) < 13:
            min_interval = min(min_interval, 3 * 60)
    return min_interval


def fmt_local() -> str:
    lisbon = datetime.now(timezone.utc).astimezone(ZoneInfo("Europe/Lisbon"))
    return lisbon.strftime("%H:%M %Z")


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def run_scheduler(
    paper:             bool = False,
    city_filter:       str  = None,
    interval_override: int  = None,
):
    # ── Load credentials ──────────────────────────────────────────────────
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
        env_file = Path(".env")
        if env_file.exists():
            for line in env_file.read_text().splitlines():
                if "=" in line and not line.startswith("#"):
                    k, v = line.split("=", 1)
                    os.environ.setdefault(k.strip(), v.strip())

    client   = trader.make_client()
    mode_str = "PAPER" if paper else ("DEMO" if client.demo else "LIVE")

    log.info("=" * 60)
    log.info("Kalshi Weather Scheduler  [%s]", mode_str)
    log.info("interval       : %s",
             "dynamic" if not interval_override else f"{interval_override} min (override)")
    log.info("trading window : %d:00–%d:00 local",
             decision_engine.TRADE_WINDOW_START, decision_engine.TRADE_WINDOW_END)
    if city_filter:
        log.info("city filter    : %s", city_filter)
    log.info("=" * 60)

    # ── Initialise scanners ───────────────────────────────────────────────
    log.info("initialising next-market scanner...")
    try:
        tomorrow_scanner.initialise(client=client, city_filter=city_filter)
    except Exception as e:
        log.warning("scanner init error (non-fatal): %s", e)

    peak_scanner.log_config()

    poll_count = 0

    while True:
        interval_secs = (
            interval_override * 60
            if interval_override
            else dynamic_interval(city_filter)
        )

        poll_count += 1
        poll_start = time.monotonic()

        log.info("─" * 55)
        log.info("poll #%d  |  interval=%dmin  |  local=%s",
                 poll_count, interval_secs // 60, fmt_local())

        # ── Parallel tasks ────────────────────────────────────────────────
        def _run_pipeline():
            t0 = time.monotonic()
            log.info("[pipeline] starting")
            trader.run_pipeline(client=client, city_filter=city_filter, paper=paper)
            log.info("[pipeline] done  (%.1fs)", time.monotonic() - t0)

        def _run_scan():
            t0 = time.monotonic()
            log.debug("[next_market_scan] starting")
            tomorrow_scanner.run_scan(client=client, city_filter=city_filter, paper=paper)
            log.debug("[next_market_scan] done  (%.1fs)", time.monotonic() - t0)

        def _run_peak():
            t0 = time.monotonic()
            log.debug("[peak_scan] starting")
            peak_scanner.run_scan(client=client, city_filter=city_filter, paper=paper)
            log.debug("[peak_scan] done  (%.1fs)", time.monotonic() - t0)

        with ThreadPoolExecutor(max_workers=3) as pool:
            futures = {
                pool.submit(_run_pipeline): "pipeline",
                pool.submit(_run_scan):     "next_market_scan",
                pool.submit(_run_peak):     "peak_scan",
            }
            for fut in as_completed(futures):
                name = futures[fut]
                try:
                    fut.result()
                except Exception as e:
                    log.error("[%s] unhandled error: %s", name, e, exc_info=True)

        # ── Exit check ────────────────────────────────────────────────────
        try:
            t0             = time.monotonic()
            live_positions = trader.sync_from_kalshi(client)
            exited         = trader.check_exits(
                client, paper=paper, live_positions=live_positions
            )
            elapsed = time.monotonic() - t0
            if exited:
                log.info("exits: %d closed  (%.1fs)  %s",
                         len(exited), elapsed, list(exited.values()))
            else:
                log.info("exits: none  (%.1fs)", elapsed)
        except Exception as e:
            log.error("exit check failed: %s", e, exc_info=True)

        # ── Balance ───────────────────────────────────────────────────────
        try:
            balance    = trader.get_balance(client)
            deployable = round(balance * 0.70, 2)
            log.info("balance: $%.2f  |  deployable: $%.2f", balance, deployable)
        except Exception as e:
            log.warning("balance fetch failed: %s", e)

        # ── Poll summary + sleep ──────────────────────────────────────────
        elapsed_total = time.monotonic() - poll_start
        next_poll     = datetime.now(timezone.utc) + timedelta(seconds=interval_secs)
        log.info("poll #%d done  (%.1fs)  |  next at %s",
                 poll_count, elapsed_total,
                 next_poll.strftime("%H:%M:%S UTC"))

        time.sleep(interval_secs)


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kalshi weather trading scheduler")
    parser.add_argument("--paper",    action="store_true")
    parser.add_argument("--city",     type=str, default=None)
    parser.add_argument("--interval", type=int, default=None, metavar="MINUTES")
    args = parser.parse_args()

    run_scheduler(
        paper             = args.paper,
        city_filter       = args.city,
        interval_override = args.interval,
    )
