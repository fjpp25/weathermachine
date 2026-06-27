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
import math
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
# Canonical bracket → temperature mapping
# ---------------------------------------------------------------------------
#
# Kalshi temperature markets use the floor_strike / cap_strike API fields as
# the authoritative bracket geometry. The ticker number and the human-readable
# label both carry display offsets that vary by bracket type, so neither is a
# reliable source for temperature math — only the strikes are.
#
# Validated against today's open markets across all 20 HIGH cities (45 brackets,
# 0 discrepancies): the rule below reproduces the exact integer settlement
# outcomes implied by every market's human label.
#
# Bracket geometry (uniform across all cities):
#
#   B bracket  — floor and cap both present, label "X° to X+1°"
#       floor = X, cap = X+1.  Settles Yes on integer highs {X, X+1}.
#       Continuous interval: [floor - 0.5, cap + 0.5)
#       e.g. B82.5 floor=82 cap=83 → [81.5, 83.5) → {82, 83} → "82° to 83°"
#
#   T-top      — floor present, cap None, label "N° or above"
#       Settles Yes on integer highs >= N, where N = floor + 1.
#       Continuous interval: [floor + 0.5, +inf)
#       e.g. T83 floor=83 → [83.5, inf) → >= 84 → "84° or above"
#
#   T-bottom   — cap present, floor None, label "N° or below"
#       Settles Yes on integer highs <= N, where N = cap - 1.
#       Continuous interval: (-inf, cap - 0.5)
#       e.g. T76 cap=76 → (-inf, 75.5) → <= 75 → "75° or below"
#
# Reads both the enriched shape (floor / cap, as produced by
# kalshi_scanner._scan_brackets) and the raw Kalshi shape
# (floor_strike / cap_strike). Falls back to ticker parsing only when no
# strikes are present, which should not occur for well-formed markets.
# ---------------------------------------------------------------------------

def _bracket_strikes(bracket: dict) -> tuple[float | None, float | None]:
    """Return (floor, cap) reading enriched or raw Kalshi field names."""
    floor = bracket.get("floor")
    cap   = bracket.get("cap")
    if floor is None:
        floor = bracket.get("floor_strike")
    if cap is None:
        cap = bracket.get("cap_strike")
    try:
        floor = float(floor) if floor is not None else None
    except (TypeError, ValueError):
        floor = None
    try:
        cap = float(cap) if cap is not None else None
    except (TypeError, ValueError):
        cap = None
    return floor, cap


def bracket_interval(bracket: dict) -> tuple[float | None, float | None]:
    """
    Return the (lo, hi) continuous temperature interval a bracket settles over,
    as a half-open interval [lo, hi). Open ends are represented by -math.inf /
    math.inf. Returns (None, None) if the bracket geometry cannot be determined.

    This is the single source of truth for bracket temperature geometry;
    bracket_temp() is derived from it.
    """
    floor, cap = _bracket_strikes(bracket)

    # B bracket: both strikes present
    if floor is not None and cap is not None:
        return (floor - 0.5, cap + 0.5)

    # T-top "or above": floor present, cap open
    if floor is not None and cap is None:
        return (floor + 0.5, math.inf)

    # T-bottom "or below": cap present, floor open
    if cap is not None and floor is None:
        return (-math.inf, cap - 0.5)

    # Fallback: derive from the ticker code (heuristic; should not be needed
    # for well-formed markets, which always carry strikes).
    code = str(bracket.get("ticker", "")).split("-")[-1]
    val  = bracket_val(code)
    if val is not None and code[:1] == "B":
        # B ticker number is the range midpoint → 1°-wide integer span
        return (val - 1.0, val + 1.0)
    return (None, None)


def bracket_temp(bracket: dict) -> float | None:
    """
    Return a single representative temperature for a bracket, for use in
    forecast-distance calculations.

      B bracket  → interval midpoint           (e.g. 82.5)
      T-top      → lower threshold edge         (e.g. 83.5  for ">= 84")
      T-bottom   → upper threshold edge         (e.g. 75.5  for "<= 75")

    For T brackets the returned value is the finite settlement boundary — the
    temperature at which the bracket flips between Yes and No — which is the
    quantity that matters when judging how far a tail bracket is from forecast.
    Returns None if the geometry cannot be determined.
    """
    lo, hi = bracket_interval(bracket)
    if lo is None and hi is None:
        return None
    if lo == -math.inf:        # T-bottom → finite upper edge
        return hi
    if hi == math.inf:         # T-top → finite lower edge
        return lo
    return (lo + hi) / 2.0     # B → midpoint


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
