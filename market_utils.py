"""
market_utils.py
---------------
Shared low-level helpers for all signal engines.

Previously each engine (cascade_engine, last_bracket, evening_convergence,
lowt_decision_engine, peak_scanner) defined its own copies of these functions,
sometimes with small inconsistencies. This module is the single source of truth.

Import pattern in each engine:
    from market_utils import (
        local_hour as _local_hour,
        no_price   as _no_price,
        yes_price  as _yes_price,
        is_resolved,
        is_b_bracket,
        load_config_env,
    )
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from cities import CITIES as _CITIES


# ---------------------------------------------------------------------------
# City / time helpers
# ---------------------------------------------------------------------------

def local_hour(city: str) -> int:
    """Return the current local hour for a city. Falls back to UTC hour."""
    tz = _CITIES.get(city, {}).get("tz", "UTC")
    return datetime.now(ZoneInfo(tz)).hour


# ---------------------------------------------------------------------------
# Bracket price helpers
#
# Field priority mirrors the order in which kalshi_scanner populates them:
#   ob_no_bid / ob_no_ask  — order-book best bid/ask (most current)
#   no_ask / no_bid        — top-level market fields (sometimes present)
#   no_price               — last_price fallback
# Using `or` short-circuits at the first truthy value; a field of 0.0
# correctly falls through to the next since we treat 0 as "no data".
# ---------------------------------------------------------------------------

def no_price(bracket: dict) -> float:
    """Return the best available No price for a bracket, or 0.0."""
    return float(
        bracket.get("ob_no_bid") or bracket.get("ob_no_ask") or
        bracket.get("no_ask")    or bracket.get("no_bid")    or
        bracket.get("no_price")  or 0.0
    )


def yes_price(bracket: dict) -> float:
    """Return the best available Yes price for a bracket, or 0.0."""
    return float(
        bracket.get("ob_yes_ask") or bracket.get("ob_yes_bid") or
        bracket.get("yes_ask")    or bracket.get("yes_price")  or 0.0
    )


# ---------------------------------------------------------------------------
# Bracket classification helpers
# ---------------------------------------------------------------------------

def is_resolved(bracket: dict, threshold: float = 0.95) -> bool:
    """Return True when either side has collapsed to near-certainty."""
    return no_price(bracket) >= threshold or yes_price(bracket) >= threshold


def is_b_bracket(bracket: dict) -> bool:
    """
    Return True if this is a B (range) bracket.
    T brackets are excluded from the evening_convergence signal.

    Checks the 'bracket' field first, then falls back to the trailing
    segment of the ticker. The parenthesisation below is intentional —
    the `or` must bind tighter than the ternary to avoid the bug where
    a valid bracket field would be silently ignored for tickers without
    a hyphen.
    """
    b = bracket.get("bracket", "")
    if not b:
        ticker = bracket.get("ticker", "")
        b = ticker.split("-")[-1] if "-" in ticker else ""
    return str(b).startswith("B")


# ---------------------------------------------------------------------------
# Bracket value parser — shared by all engines
# ---------------------------------------------------------------------------

def bracket_val(bracket_code: str) -> float | None:
    """
    Extract the numeric temperature value from a bracket code.
    B82.5 → 82.5,  T69 → 69.0,  B46 → 46.0
    Returns None if the code is not a valid bracket string.
    """
    if bracket_code and bracket_code[0] in ("B", "T"):
        try:
            return float(bracket_code[1:])
        except ValueError:
            pass
    return None


# ---------------------------------------------------------------------------
# Standalone-mode credential loader
# ---------------------------------------------------------------------------

def load_config_env() -> None:
    """
    Load Kalshi credentials from data/config.json into environment variables.

    Called from the __main__ blocks of standalone scanner modules so they
    can be run directly without manually exporting environment variables.
    Safe to call multiple times — os.environ.setdefault never overwrites
    an existing value.
    """
    config_file = Path("data/config.json")
    if not config_file.exists():
        return
    try:
        config = json.loads(config_file.read_text())
        if config.get("key_id"):
            os.environ.setdefault("KALSHI_KEY_ID", config["key_id"])
        if config.get("key_file"):
            os.environ.setdefault("KALSHI_KEY_FILE", config["key_file"])
        os.environ["KALSHI_DEMO"] = "false" if config.get("live_mode") else "true"
    except Exception:
        pass
