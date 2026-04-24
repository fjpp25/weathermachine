"""
log_setup.py
------------
Shared logging configuration for the weathermachine scheduler.

All modules import get_logger() from here to get a consistently
formatted logger. Every line carries a UTC timestamp, module name,
and level — making it easy to grep, tail, and correlate events.

Output format:
  2026-04-20 14:32:11 UTC  [scheduler]     INFO   Poll #42 starting
  2026-04-20 14:32:11 UTC  [trader]        INFO   Executing: Chicago KXHIGHCHI-26APR20-B82.5
  2026-04-20 14:32:12 UTC  [trader]        INFO     NO 2x @ $0.88  score=2/5  (0.41s)
  2026-04-20 14:32:15 UTC  [peak_scanner]  DEBUG  Chicago  obs_high=82.4°F  stable=2/3

Log levels:
  DEBUG   — per-city detail, price ticks, skip reasons (verbose, off by default)
  INFO    — signals, orders, exits, poll summaries, timing
  WARNING — non-fatal failures, fallbacks, unexpected conditions
  ERROR   — exceptions, order failures, data fetch failures

File output:
  Set LOG_FILE in environment to write to a rotating file alongside console.
  e.g. LOG_FILE=logs/scheduler.log python scheduler.py

Usage:
  from log_setup import get_logger
  log = get_logger(__name__)
  log.info("order placed: %s", ticker)
"""

import logging
import logging.handlers
import os
import sys
from datetime import datetime, timezone


# ---------------------------------------------------------------------------
# UTC formatter
# ---------------------------------------------------------------------------

class _UTCFormatter(logging.Formatter):
    """Always formats timestamps in UTC regardless of system timezone."""
    converter = lambda *args: datetime.now(timezone.utc).timetuple()

    def formatTime(self, record, datefmt=None):
        ct = datetime.fromtimestamp(record.created, tz=timezone.utc)
        return ct.strftime("%Y-%m-%d %H:%M:%S UTC")


_FMT = "%(asctime)s  [%(name)-16s]  %(levelname)-7s  %(message)s"
_formatter = _UTCFormatter(_FMT)


# ---------------------------------------------------------------------------
# Root configuration — called once at import time
# ---------------------------------------------------------------------------

def _configure_root() -> None:
    root = logging.getLogger()
    if root.handlers:
        return   # already configured

    # ── Console handler ───────────────────────────────────────────────────
    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(_formatter)
    root.addHandler(console)

    # ── Optional file handler ─────────────────────────────────────────────
    log_file = os.environ.get("LOG_FILE")
    if log_file:
        os.makedirs(os.path.dirname(log_file) or ".", exist_ok=True)
        fh = logging.handlers.RotatingFileHandler(
            log_file,
            maxBytes    = 10 * 1024 * 1024,   # 10 MB per file
            backupCount = 5,                   # keep last 5 rotations
            encoding    = "utf-8",
        )
        fh.setFormatter(_formatter)
        root.addHandler(fh)
        root.info("log_setup: file handler → %s", log_file)

    # ── Level ─────────────────────────────────────────────────────────────
    level_name = os.environ.get("LOG_LEVEL", "INFO").upper()
    level      = getattr(logging, level_name, logging.INFO)
    root.setLevel(level)

    # Silence noisy third-party libs
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)


_configure_root()


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def get_logger(name: str) -> logging.Logger:
    """
    Return a named logger. Use __name__ as the name so log lines
    show the module they came from.

      from log_setup import get_logger
      log = get_logger(__name__)
    """
    # Strip package prefix for display (e.g. "weathermachine.trader" → "trader")
    short = name.split(".")[-1]
    return logging.getLogger(short)
