#!/usr/bin/env python3
"""
analytics — the Weather Machine analytics subsystem.

The standing instruments for "how are we doing": per-engine / per-city / per-band
performance, interaction slicing, and (later) fill-rate and per-dollar-EV. All
read from ONE canonical source of truth — the trade log joined to the
AUTHORITATIVE settlements table.

WHY A PACKAGE (not loose scripts):
  This subsystem must work from THREE different working directories:
    1. the repo root (CLI: `python3 -m analytics`)
    2. inside analytics/ (editing/testing)
    3. the dashboard systemd service (its own cwd) — Step 4 imports these funcs
  So paths are anchored to THIS FILE's location, never the current directory.
  Relative paths like "data/trade_log.json" would break in cases 2 and 3.

  Importing this package also puts the repo ROOT on sys.path, so analytics
  modules can `from cities import CITIES` (the live registry in the repo root)
  regardless of where the process was launched.

Canonical paths exported for all submodules:
  ROOT       — repo root (parent of this package dir)
  DATA       — ROOT / "data"
  TRADE_LOG  — DATA / "trade_log.json"
  OBS_DB     — DATA / "observations.db"
"""
from __future__ import annotations

import sys
from pathlib import Path

# Anchor everything to this file, NOT the cwd. analytics/ lives at <repo>/analytics,
# so the repo root is one level up. This holds no matter where python is invoked.
ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data"
TRADE_LOG = DATA / "trade_log.json"
OBS_DB = DATA / "observations.db"

# Make the repo root importable so `from cities import CITIES` resolves even when
# the process was started elsewhere (e.g. the dashboard service).
_root_str = str(ROOT)
if _root_str not in sys.path:
    sys.path.insert(0, _root_str)

__all__ = ["ROOT", "DATA", "TRADE_LOG", "OBS_DB"]
