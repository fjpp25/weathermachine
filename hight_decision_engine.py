"""
hight_decision_engine.py
------------------------
Synthesizes city profiles, NWS live feed, and Kalshi scanner data into
actionable NO trade signals for temperature HIGH markets.

Only NO trades are generated — buying NO on brackets the forecast or
observed temperatures make unlikely, collecting premium as the market
converges to resolution.

Gates (applied in order — any failure skips to next bracket):
  1. Timing      — city_local_hour ≥ TRADE_WINDOW_START (global floor, currently 6am local).
                   No per-city overrides — 6am is early enough for obs signals and the
                   NWS morning forecast, while keeping overnight noise out.
  2. Liquidity   — spread ≤ MAX_SPREAD and depth ≥ MIN_DEPTH
  3. Boundary    — bias-corrected forecast is ≥ dynamic_buffer°F from both bracket edges
                   (dynamic_buffer scales with city's tmax_stddev from city_profiles.json)

Signal scoring (−1 to +5):
  +1 obs_eliminates_bracket  — observed high ≥ bracket cap (bracket physically impossible)
  +1 obs_below_floor         — observed high < bracket floor − buffer (bracket not yet in play)
  −1 obs_inside_bracket      — observed high already within [floor, cap) (bracket may be resolving YES)
  +1 forecast_well_clear     — corrected forecast ≥ FORECAST_WELL_CLEAR°F from nearest edge
  +1 momentum_flat_or_down   — no upward price momentum in recent candles (market signal)
  +1 yes_price_quality       — YES ≤ YES_HIGH_CONFIDENCE (12¢) — market strongly agrees (market signal)
  Minimum score to trade: ≥ 1

Usage:
  python hight_decision_engine.py                 # run full analysis, all cities
  python hight_decision_engine.py --city Miami    # single city
  python hight_decision_engine.py --paper         # paper-trade mode (log only)

Dependencies:
  city_profiles.py   (data/city_profiles.json must exist)
  bias_calculator.py (data/forecast_bias.json used when available)
  nws_feed.py
  kalshi_scanner.py
  cities.py
"""

import json
import argparse
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import nws_feed
import kalshi_scanner
try:
    import accuweather_feed as _aw_feed
    _AW_AVAILABLE = True
except ImportError:
    _AW_AVAILABLE = False
import cascade_engine
from cities import CITIES as _CITY_REGISTRY

# ---------------------------------------------------------------------------
# Parameters — tune these as you gather data
# ---------------------------------------------------------------------------

# Gate thresholds
TRADE_WINDOW_START  = 9        # local hour — global floor, no trading before 9am.
                               # Before 9am the NWS morning forecast hasn't settled and
                               # there's no meaningful obs signal to confirm direction.
                               # Per-city trade_start_high values in cities.py override
                               # this upward (e.g. Chicago=10 for forecast stability).
TRADE_WINDOW_END    = 24       # local hour — no global hard close needed.
                               # Per-city trade_end_high values in cities.py control close.
                               # Price gate ($0.75–$0.92) and date filter handle edge cases.
MAX_SPREAD          = 0.05     # max acceptable bid-ask spread ($)
                               # relaxed from 0.03 — was blocking too many valid signals
MIN_DEPTH           = 500      # min contracts on the side we're buying

# Dynamic boundary buffer — scales with tmax_stddev from city_profiles.json
# dynamic_buffer = clamp(BOUNDARY_BUFFER_STDDEV_FACTOR * stddev, MIN, MAX)
# Higher stddev cities (Chicago winter) get a wider buffer than stable cities (Miami summer).
BOUNDARY_BUFFER_STDDEV_FACTOR = 0.6
BOUNDARY_BUFFER_MIN           = 2.0   # °F floor
BOUNDARY_BUFFER_MAX           = 5.0   # °F ceiling

# Fallback buffer if city profile or stddev is unavailable
BOUNDARY_BUFFER_FALLBACK      = 3.0   # °F

# NO trade parameters
NO_MIN_YES_PRICE    = 0.02     # skip if YES is basically zero (already dead)
NO_MAX_YES_PRICE    = 0.25     # never enter NO if YES is above this
NO_MIN_ENTRY_PRICE  = 0.75     # never pay less than this for a NO contract
                               # data: below 0.75 market is pricing in real uncertainty
NO_MAX_ENTRY_PRICE  = 0.92     # never pay more than this for a NO contract
                               # data: 0.75–0.92 gives 86.5% WR across 37 trades
MAX_NO_PER_CITY     = 2        # max NO positions to open per city per day
NO_BAN_ABOVE_BRACKETS = False  # top T brackets ("above X°F") now allowed with gates

# Top T bracket gates (above X°F brackets — floor set, cap=None):
# Backtest (147 signals, Apr 6-29):
#   No 0.75-0.85: WR=70-77%, negative EV — too much uncertainty
#   No 0.85-0.92: WR=95%+, positive EV — market has priced it out
#   Forecast within 2°F of cap: WR=55%, -$0.28 EV — danger zone
#   Forecast >= 2°F below cap:  WR=93%+, positive EV — safe
TOP_T_NO_MIN         = 0.85   # higher entry threshold than B brackets
TOP_T_FORECAST_DIST  = 2.0    # forecast must be >= this many °F BELOW the T floor
TOP_T_HOUR_CAP       = 18     # no top-T entries at or after this local hour

# Bottom T bracket forecast distance gate (cap set, floor=None).
# The corrected forecast must exceed the T cap by at least this many degrees.
# Backtest: dist 0-1°F → WR=67%, -$0.19 EV  dist 1-2°F → WR=92%
T_BRACKET_FORECAST_MIN_ABOVE = 1.0   # °F above T cap required
MAX_CONTRACTS       = 3        # hard cap on contracts per position
                               # data: 3-contract losses average -$1.74 each, far worse than 1-2

MIN_SCORE           = 1        # minimum score to enter a trade

# ---------------------------------------------------------------------------
# Near-cap signal parameters
# ---------------------------------------------------------------------------
# When obs_high is >= 80% of the way through a bracket (near its cap),
# the two brackets above (N+2 and N+3) are very unlikely to be reached.
# Backtest (HIGH, Apr 6 – May 4 2026, exc. one manual-trade day):
#   n=85  WR=100%  EV=+$0.111  total_pnl=+$9.40  (rank N+2 + N+3 combined)
NEAR_CAP_INTRA_MIN  = 0.80    # obs must be >= this fraction through the bracket
NEAR_CAP_HOUR_MAX   = 12      # only fire before noon local (morning establishment)
NEAR_CAP_NO_MIN     = 0.75    # minimum No price (same as main engine floor)
NEAR_CAP_NO_MAX     = 0.95    # slightly wider than main engine — these are high-confidence
                               # score=0 has 100% settled WR — gate kept at 1 as minimal
                               # sanity check only (requires at least one positive signal)

# Momentum detection
MIN_CANDLES_FOR_MOMENTUM = 3   # need at least this many candles to score momentum
MOMENTUM_LOOKBACK        = 3   # look at last N candles for direction

# Global NWS forecast bias fallback (used when data/forecast_bias.json has no entry for city)
# Overridden per city by data/forecast_bias.json when available.
# Sign convention: positive = NWS forecast runs warm (overestimates the actual high).
# Applied in engine as:  corrected = forecast - bias
# So FORECAST_BIAS_CORRECTION = 1.0 means "subtract 1°F from NWS forecast".
FORECAST_BIAS_CORRECTION = 1.0    # °F — NWS globally runs ~1°F warm for HIGH markets

# Forecast well-clear threshold for scoring
# Bracket must be this far from the corrected forecast to score the forecast point
# (higher bar than boundary buffer gate — gate≈3°F, score=6°F)
FORECAST_WELL_CLEAR = 6.0

# Market-derived confidence bonus
# YES price at or below this threshold earns an extra score point — the market
# is pricing in very high NO probability, which is a stronger signal than any
# single temperature reading. Complements momentum_flat_or_down (which only
# checks direction, not level).
YES_HIGH_CONFIDENCE = 0.12   # YES ≤ 12¢ → market strongly agrees with our thesis

# Temperature penalty threshold
# If observed high is already inside the bracket range [floor, cap), the
# market may be in the process of resolving YES. This deducts one score point
# to require stronger market confirmation before entering. It does NOT block
# the trade outright — a YES price of 0.10 with flat momentum is still valid
# even if obs is marginally inside the range.


# ---------------------------------------------------------------------------
# Per-city bias — loaded from data/forecast_bias.json at import time
# ---------------------------------------------------------------------------

_BIAS_FILE = Path("data/forecast_bias.json")

def _load_forecast_bias() -> dict[str, dict]:
    """
    Load per-city bias computed by bias_calculator.py.
    Accepts both the new format {city: {"bias": float, "stddev": float}}
    and the legacy flat format {city: float} for backwards compatibility.
    Falls back gracefully — missing cities use FORECAST_BIAS_CORRECTION.
    """
    if not _BIAS_FILE.exists():
        return {}
    try:
        raw = json.loads(_BIAS_FILE.read_text())
        normalised = {}
        for city, val in raw.items():
            if isinstance(val, (int, float)):
                normalised[city] = {"bias": float(val), "stddev": 0.0}
            else:
                normalised[city] = val
        return normalised
    except Exception as e:
        print(f"[decision_engine] Warning: could not load {_BIAS_FILE}: {e}")
        return {}

_FORECAST_BIAS: dict[str, dict] = _load_forecast_bias()


def _city_bias(city: str) -> float:
    """Return per-city bias correction (°F). Positive = NWS runs warm.
    Applied by the engine as: corrected = forecast - bias.
    Falls back to FORECAST_BIAS_CORRECTION for cities with no data."""
    entry = _FORECAST_BIAS.get(city)
    if entry is not None:
        return entry["bias"]
    return FORECAST_BIAS_CORRECTION


def _city_bias_stddev(city: str) -> float:
    """Return the stddev of per-city bias errors (0.0 if unknown)."""
    entry = _FORECAST_BIAS.get(city)
    if entry is not None:
        return entry.get("stddev", 0.0)
    return 0.0


# ---------------------------------------------------------------------------
# Dynamic boundary buffer — derived from city profile stddev
# ---------------------------------------------------------------------------

def _dynamic_buffer(city: str, profiles: dict) -> float:
    """
    Compute the boundary buffer for a city based on its current-month tmax_stddev.

    Higher stddev (e.g. Chicago in March) → wider buffer → fewer but higher-confidence trades.
    Lower stddev (e.g. Miami in July)     → tighter buffer → more trades with less edge dilution.

    Falls back to BOUNDARY_BUFFER_FALLBACK if profile data is unavailable.
    """
    try:
        month   = str(datetime.now().month)
        monthly = profiles.get(city, {}).get("monthly", {}).get(month, {})
        stddev  = monthly.get("tmax_stddev")
        if stddev is not None and stddev > 0:
            raw = BOUNDARY_BUFFER_STDDEV_FACTOR * stddev
            return round(max(BOUNDARY_BUFFER_MIN, min(BOUNDARY_BUFFER_MAX, raw)), 1)
    except Exception:
        pass
    return BOUNDARY_BUFFER_FALLBACK


# ---------------------------------------------------------------------------
# Paused cities — derived from cities.py + optional config.json override
# ---------------------------------------------------------------------------

def _build_paused_cities() -> set[str]:
    """
    Derive paused cities from two sources, merged:

    1. cities.py — cities with trading_high=False are the static default.
    2. data/config.json (optional) — 'paused_cities' key allows runtime
       pauses without touching source code.

    NOTE: PAUSED_CITIES is built once at import time.
    Restart the scheduler to pick up config.json changes.
    """
    paused = {name for name, meta in _CITY_REGISTRY.items()
              if not meta.get("trading_high", meta.get("trading", True))}

    config_file = Path("data/config.json")
    if config_file.exists():
        try:
            config = json.loads(config_file.read_text())
            paused.update(config.get("paused_cities", []))
        except Exception:
            pass

    return paused


PAUSED_CITIES: set[str] = _build_paused_cities()


# ---------------------------------------------------------------------------
# Per-city entry window helpers
# ---------------------------------------------------------------------------

def _trade_start_for(city: str, market_type: str = "high") -> int:
    """
    Return the earliest local hour for HIGH entries for a city.

    Uses the per-city trade_start_high value from cities.py, with
    TRADE_WINDOW_START (9am) as the global floor. No city should
    start before 9am — before that the NWS forecast is still
    settling from the overnight model run and obs signals are sparse.
    """
    per_city = _CITY_REGISTRY.get(city, {}).get("trade_start_high")
    if per_city is not None:
        return max(per_city, TRADE_WINDOW_START)
    return TRADE_WINDOW_START


def _trade_end_for(city: str, market_type: str = "high") -> int:
    """
    Return the latest local hour for HIGH entries for a city.

    Uses the per-city trade_end_high value from cities.py.
    Falls back to TRADE_WINDOW_END (24) if not set — the price gate
    and date filter prevent bad late entries in that case.
    """
    per_city = _CITY_REGISTRY.get(city, {}).get("trade_end_high")
    if per_city is not None:
        return per_city
    return TRADE_WINDOW_END


# ---------------------------------------------------------------------------
# Load cached city profiles
# ---------------------------------------------------------------------------

PROFILES_FILE = Path("data/city_profiles.json")

def load_profiles() -> dict:
    if not PROFILES_FILE.exists():
        print(f"City profiles not found at {PROFILES_FILE} — generating now...")
        try:
            import city_profiles
            city_profiles.build_profiles()
            print("City profiles generated successfully.")
        except Exception as e:
            raise FileNotFoundError(
                f"City profiles not found at {PROFILES_FILE} and auto-generation failed: {e}\n"
                "Run manually: python city_profiles.py"
            )
    with open(PROFILES_FILE) as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# Core analysis functions
# ---------------------------------------------------------------------------

# Buffer applied to forecast bracket identification.
# If the corrected forecast is within this many °F of a bracket floor,
# the bracket is treated as the forecast bracket and skipped.
# Prevents entries on brackets where the forecast is ambiguously close
# to the floor (e.g. corrected=82.5°F with floor=83.5°F → skip B83.5).
FORECAST_BRACKET_BUFFER = 1.0   # °F

def get_forecast_bracket(forecast_high: float, brackets: list[dict],
                          buffer: float = FORECAST_BRACKET_BUFFER) -> dict | None:
    """
    Find which bracket the (bias-corrected) forecast high falls into.
    Also flags brackets whose floor is within `buffer`°F above the forecast
    as contested — avoids entries like corrected=82.5°F entering B83.5.
    """
    for bracket in brackets:
        floor = bracket.get("floor")
        cap   = bracket.get("cap")
        if floor is not None and cap is not None:
            # Standard: forecast inside bracket (use <= cap to catch exact cap match)
            if floor <= forecast_high <= cap:
                return bracket
            # Buffer: forecast within buffer°F below the floor
            if floor - buffer <= forecast_high < floor:
                return bracket
        elif floor is not None:
            if forecast_high >= floor:
                return bracket
        elif cap is not None:
            if forecast_high < cap:
                return bracket
    return None


def score_momentum(candles: list[dict]) -> bool:
    """
    Returns True if price is moving upward (toward YES) over recent candles.
    Uses close prices from the last MOMENTUM_LOOKBACK candles.
    Returns False (no momentum) if there are fewer than MIN_CANDLES_FOR_MOMENTUM candles.
    """
    if len(candles) < MIN_CANDLES_FOR_MOMENTUM:
        return False
    recent = candles[-MOMENTUM_LOOKBACK:]
    closes = [c.get("yes_price_close") or c.get("close") or 0 for c in recent]
    if len(closes) < 2:
        return False
    return closes[-1] > closes[0]


def is_forecast_inside_boundary(bracket: dict, forecast: float, buffer: float) -> bool:
    """Check forecast is at least `buffer`°F inside both bracket edges."""
    floor = bracket.get("floor")
    cap   = bracket.get("cap")
    if floor is not None and forecast - floor < buffer:
        return False
    if cap is not None and cap - forecast < buffer:
        return False
    return True


# ---------------------------------------------------------------------------
# Per-bracket evaluator (NO trades only)
# ---------------------------------------------------------------------------

def evaluate_bracket(
    bracket:            dict,
    forecast_high:      float,
    observed_high:      float | None,
    city_local_hour:    int,
    trade_start_hour:   int,
    trade_end_hour:     int,
    city_bias:          float,
    dynamic_buffer:     float,
    corrected_forecast: float | None = None,
) -> dict:
    """
    Evaluate a single bracket for a NO trade signal.

    Forecast brackets are never traded — caller should skip them before calling here.

    Args:
        bracket:          Bracket dict from kalshi_scanner (includes orderbook fields).
        forecast_high:    Raw NWS forecast high (°F) — bias correction applied internally.
        observed_high:    Observed high so far today (°F), or None if not yet available.
        city_local_hour:  Current local hour for the city.
        trade_start_hour: Earliest local hour for entering trades (per-city, floor 9am).
        trade_end_hour:   Latest local hour for entering trades (per-city from cities.py).
        city_bias:        Per-city NWS bias correction (°F). Positive = NWS runs warm.
                          Applied as: corrected = forecast - bias.
        dynamic_buffer:   Boundary buffer (°F) scaled to city stddev.

    Returns:
        Signal dict with trade_type="NO" if actionable, else trade_type=None with skip_reason.
    """
    signal = {
        "ticker":       bracket["ticker"],
        "title":        bracket.get("title", ""),
        "floor":        bracket.get("floor"),
        "cap":          bracket.get("cap"),
        "yes_ask":      bracket.get("ob_yes_ask"),
        "no_ask":       bracket.get("ob_no_ask"),
        "yes_bid":      bracket.get("ob_yes_bid"),
        "no_bid":       bracket.get("ob_no_bid"),
        "spread":       bracket.get("ob_spread"),
        "yes_depth":    bracket.get("ob_yes_depth"),
        "no_depth":     bracket.get("ob_no_depth"),
        "volume":       bracket.get("volume"),
        "candle_count": bracket.get("candle_count", 0),
        "score":        0,
        "score_detail": [],
        "trade_type":   None,
        "skip_reason":  None,
        "city_bias":    city_bias,
        "dynamic_buffer": dynamic_buffer,
    }

    # --- Gate 0: Market must be active ---
    if bracket.get("status") not in (None, "active"):
        signal["skip_reason"] = f"Market not active (status={bracket.get('status')})"
        return signal

    # --- Gate 0b: Top T bracket gates ("above X°F" — floor set, cap=None) ---
    # These brackets are now allowed but require stricter conditions than B brackets:
    #   1. No price >= TOP_T_NO_MIN (0.85) — market must have mostly priced it out
    #   2. Corrected forecast must be >= TOP_T_FORECAST_DIST below the floor
    #   3. Entry before TOP_T_HOUR_CAP local (evening pricing noise after 18h)
    if bracket.get("cap") is None and bracket.get("floor") is not None:
        t_floor = bracket["floor"]
        no_p    = bracket.get("ob_no_ask") or bracket.get("no_ask") or 0.0

        # Gate: No price must meet higher threshold
        if no_p < TOP_T_NO_MIN:
            signal["skip_reason"] = (
                f"Top T bracket No too low "
                f"(no={no_p:.2f} < min={TOP_T_NO_MIN})"
            )
            return signal

        # Gate: forecast must be well below the floor
        _cf_0b = corrected_forecast if corrected_forecast is not None else (forecast_high - city_bias)
        if _cf_0b is not None:
            t_dist = t_floor - _cf_0b   # positive = forecast below floor (safe)
            if t_dist < TOP_T_FORECAST_DIST:
                signal["skip_reason"] = (
                    f"Top T bracket forecast too close to floor "
                    f"(corrected={_cf_0b:.1f}°  floor={t_floor}°  "
                    f"dist={t_dist:+.1f}°  min={TOP_T_FORECAST_DIST}°F below)"
                )
                return signal

        # Gate: time of day
        if city_local_hour >= TOP_T_HOUR_CAP:
            signal["skip_reason"] = (
                f"Top T bracket outside time window "
                f"(hour={city_local_hour} >= cap={TOP_T_HOUR_CAP})"
            )
            return signal

    # --- Gate 0c: T bracket forecast distance gate ---
    # T brackets (cap only, no floor): corrected forecast must exceed
    # the cap by at least T_BRACKET_FORECAST_MIN_ABOVE degrees.
    if (
        bracket.get("cap") is not None
        and bracket.get("floor") is None
        and forecast_high is not None
    ):
        _cf_0c = corrected_forecast if corrected_forecast is not None else (forecast_high - city_bias)
        t_cap  = bracket["cap"]
        t_dist = _cf_0c - t_cap   # positive = forecast above cap
        if t_dist < T_BRACKET_FORECAST_MIN_ABOVE:
            signal["skip_reason"] = (
                f"T bracket forecast too close to cap "
                f"(corrected={_cf_0c:.1f}°  cap={t_cap}°  "
                f"dist={t_dist:+.1f}°  min={T_BRACKET_FORECAST_MIN_ABOVE}°F)"
            )
            return signal

    # --- Gate 1: Timing — per-city entry window ---
    if not (trade_start_hour <= city_local_hour < trade_end_hour):
        signal["skip_reason"] = (
            f"Outside entry window (local hour={city_local_hour}, "
            f"window={trade_start_hour:02d}:00–{trade_end_hour:02d}:00)"
        )
        return signal

    # --- Gate 2: Liquidity ---
    spread    = bracket.get("ob_spread")
    no_depth  = bracket.get("ob_no_depth") or 0
    yes_depth = bracket.get("ob_yes_depth") or 0

    if spread is None or spread > MAX_SPREAD:
        reason = ("One-sided book (no spread available)" if spread is None
                  else f"Spread too wide ({spread:.2f} > {MAX_SPREAD:.2f})")
        signal["skip_reason"] = reason
        return signal

    if no_depth < MIN_DEPTH and yes_depth < MIN_DEPTH:
        signal["skip_reason"] = f"Insufficient depth (yes={yes_depth}, no={no_depth})"
        return signal

    # --- Gate 3: Boundary buffer (using per-city bias and dynamic buffer) ---
    corrected = corrected_forecast if corrected_forecast is not None else (forecast_high - city_bias)
    floor     = bracket.get("floor")
    cap       = bracket.get("cap")

    distances = []
    if floor is not None:
        distances.append(abs(corrected - floor))
    if cap is not None:
        distances.append(abs(corrected - cap))

    if distances and min(distances) < dynamic_buffer:
        signal["skip_reason"] = (
            f"NO bracket too close to corrected forecast "
            f"(corrected={corrected:.1f}°, buffer={dynamic_buffer}°F)"
        )
        return signal

    # --- Signal scoring ---
    # Score range: −1 to +5.
    # Temperature signals — derived from NWS obs, subject to bias uncertainty.
    # Market signals — derived from live Kalshi prices, weighted more heavily
    # since they aggregate all available information continuously.
    score   = 0
    details = []

    if observed_high is not None:
        if cap is not None and observed_high >= cap:
            score += 1
            details.append("obs_eliminates_bracket")
        elif floor is not None and observed_high < floor - dynamic_buffer:
            score += 1
            details.append("obs_below_floor")
        elif floor is not None and cap is not None and floor <= observed_high < cap:
            # Observed high is inside the bracket range — bracket may be resolving YES.
            # Penalise rather than block: market price still determines whether
            # we enter, but we require stronger market confirmation.
            score -= 1
            details.append("obs_inside_bracket")

    if distances and min(distances) >= FORECAST_WELL_CLEAR:
        score += 1
        details.append("forecast_well_clear")

    candles = bracket.get("candles", [])
    if not score_momentum(candles):
        score += 1
        details.append("momentum_flat_or_down")

    # yes_ask computed early for scoring — reused in trade decision below.
    yes_ask = bracket.get("ob_yes_ask")
    if yes_ask is not None and 0 < yes_ask <= YES_HIGH_CONFIDENCE:
        score += 1
        details.append("yes_price_quality")

    signal["score"]        = score
    signal["score_detail"] = details

    # --- Trade type decision ---
    # yes_ask already computed above.
    no_ask = bracket.get("ob_no_ask")

    if (
        no_ask is not None
        and NO_MIN_ENTRY_PRICE <= no_ask <= NO_MAX_ENTRY_PRICE
        and yes_ask is not None
        and NO_MIN_YES_PRICE < yes_ask <= NO_MAX_YES_PRICE
        and no_depth >= MIN_DEPTH
    ):
        # Score range is −1 to +5. Require score ≥ MIN_SCORE to trade.
        # obs_inside_bracket penalty means a position with obs in range
        # needs at least two positive signals to remain actionable.
        if score < MIN_SCORE:
            signal["skip_reason"] = (
                f"Score {score} below minimum ({MIN_SCORE} required) — "
                f"details: {details}"
            )
        else:
            signal["trade_type"]    = "NO"
            signal["entry_price"]   = no_ask
            signal["stop_loss"]     = None
            signal["max_contracts"] = MAX_CONTRACTS

    return signal


# ---------------------------------------------------------------------------
# Per-city evaluator
# ---------------------------------------------------------------------------

def evaluate_city(
    city:        str,
    nws_data:    dict,
    scan_data:   dict,
    profiles:    dict,
    market_type: str = "high",
) -> dict:
    """
    Full evaluation for one city.
    Returns a structured result with all bracket signals.
    """
    result = {
        "city":         city,
        "evaluated_at": datetime.now(timezone.utc).isoformat(),
        "nws_snapshot": {
            "current_temp_f":  nws_data.get("current_temp_f"),
            "observed_high_f": nws_data.get("observed_high_f"),
            "forecast_high_f": nws_data.get("forecast_high_f"),
            "forecast_low_f":  nws_data.get("forecast_low_f"),
            "local_time":      nws_data.get("local_time"),
            "city_local_hour": nws_data.get("city_local_hour"),
        },
        "signals": [],
        "error":   None,
    }

    if city in PAUSED_CITIES:
        result["error"] = "City paused (insufficient edge — see cities.py or config.json)"
        return result

    if nws_data.get("error"):
        result["error"] = f"NWS error: {nws_data['error']}"
        return result

    if scan_data.get("error"):
        result["error"] = f"Kalshi error: {scan_data['error']}"
        return result

    forecast_high   = nws_data.get("forecast_high_f")
    observed_high   = nws_data.get("observed_high_f")
    city_local_hour = nws_data.get("city_local_hour", 0)
    brackets        = scan_data.get("brackets", [])

    if forecast_high is None:
        result["error"] = "No forecast high — NWS grid not cached yet (will retry next poll)"
        return result

    # Resolve per-city bias, dynamic buffer, and entry window
    city_bias      = _city_bias(city)
    dyn_buffer     = _dynamic_buffer(city, profiles)
    trade_start    = _trade_start_for(city, market_type)
    trade_end      = _trade_end_for(city, market_type)

    # Primary forecast: AccuWeather (MAE 1.22°F vs NWS 2.56°F across 254 settled city-days)
    # Falls back to NWS corrected if AccuWeather unavailable or not yet fetched.
    aw_high = None
    if _AW_AVAILABLE:
        try:
            aw_data = _aw_feed.snapshot(city_filter=city)
            aw_high = (aw_data.get(city) or {}).get("forecast_high_f")
        except Exception:
            pass

    nws_corrected      = forecast_high - city_bias
    corrected_forecast = aw_high if aw_high is not None else nws_corrected
    result["corrected_forecast"]    = corrected_forecast
    result["accuweather_forecast"]  = aw_high

    # Log significant divergence between AccuWeather and NWS
    if aw_high is not None and abs(aw_high - nws_corrected) >= 2.0:
        import logging as _log_aw
        _log_aw.getLogger("hight_decision_engine").info(
            "%s: AccuWeather=%.1f°F  NWS_corrected=%.1f°F  "
            "divergence=%+.1f°F — using AccuWeather",
            city, aw_high, nws_corrected, aw_high - nws_corrected
        )

    forecast_bracket   = get_forecast_bracket(corrected_forecast, brackets)

    result["city_bias"]      = city_bias
    result["dynamic_buffer"] = dyn_buffer

    # Pre-sort B brackets by floor for near-cap signal and any other
    # multi-bracket logic that needs ordered access.
    b_brackets_sorted = sorted(
        [b for b in brackets
         if b.get("ticker", "").split("-")[-1].startswith("B")
         and b.get("floor") is not None and b.get("cap") is not None],
        key=lambda b: b["floor"]
    )

    for bracket in brackets:
        # Skip the forecast bracket — we don't trade YES, and NO on the
        # forecast bracket is the highest-risk position (most likely to resolve YES)
        is_forecast = (
            forecast_bracket is not None
            and bracket["ticker"] == forecast_bracket["ticker"]
        )
        if is_forecast:
            continue

        signal = evaluate_bracket(
            bracket             = bracket,
            forecast_high       = forecast_high,
            observed_high       = observed_high,
            city_local_hour     = city_local_hour,
            trade_start_hour    = trade_start,
            trade_end_hour      = trade_end,
            city_bias           = city_bias,
            dynamic_buffer      = dyn_buffer,
            corrected_forecast  = corrected_forecast,
        )
        if signal:
            result["signals"].append(signal)

    # Limit NO signals to MAX_NO_PER_CITY, keeping those furthest from corrected forecast
    no_signals = [s for s in result["signals"] if s.get("trade_type") == "NO"]
    if len(no_signals) > MAX_NO_PER_CITY:
        def distance_from_forecast(signal):
            floor = signal.get("floor")
            cap   = signal.get("cap")
            if floor is not None and cap is not None:
                midpoint = (floor + cap) / 2
            elif floor is not None:
                midpoint = floor + 1
            elif cap is not None:
                midpoint = cap - 1
            else:
                midpoint = corrected_forecast
            return abs(midpoint - corrected_forecast)

        no_signals.sort(key=distance_from_forecast, reverse=True)
        allowed_tickers = {s["ticker"] for s in no_signals[:MAX_NO_PER_CITY]}

        for signal in result["signals"]:
            if (signal.get("trade_type") == "NO"
                    and signal["ticker"] not in allowed_tickers):
                signal["trade_type"] = None
                signal["skip_reason"] = f"Exceeded MAX_NO_PER_CITY ({MAX_NO_PER_CITY})"

    # ── Near-cap signal ───────────────────────────────────────────────────
    # When obs_high is >= 80% through a bracket's range, the temperature has
    # almost certainly peaked at or below that bracket's cap. The two brackets
    # above (N+2 and N+3) are structurally excluded — they require the
    # temperature to climb 2+ more degrees, which is physically unlikely when
    # the morning high is already near the cap of the current bracket.
    #
    # Only fires before noon local (morning high establishment) and only
    # on brackets not already covered by the main engine signal above.
    if (observed_high is not None
            and city_local_hour < NEAR_CAP_HOUR_MAX
            and len(b_brackets_sorted) >= 4):

        # Find which B bracket obs_high is currently inside
        current_bracket = None
        current_idx     = None
        for idx, bkt in enumerate(b_brackets_sorted):
            bkt_floor = bkt.get("floor")
            bkt_cap   = bkt.get("cap")
            if bkt_floor is not None and bkt_cap is not None:
                if bkt_floor <= observed_high < bkt_cap:
                    current_bracket = bkt
                    current_idx     = idx
                    break

        if current_bracket is not None and current_idx is not None:
            bkt_floor = current_bracket["floor"]
            bkt_cap   = current_bracket["cap"]
            bkt_width = bkt_cap - bkt_floor
            intra_pos = (observed_high - bkt_floor) / bkt_width if bkt_width > 0 else 0.0

            if intra_pos >= NEAR_CAP_INTRA_MIN:
                # Target ranks N+2 and N+3 above the current bracket
                already_traded = {
                    s["ticker"] for s in result["signals"]
                    if s.get("trade_type") == "NO"
                }
                for rank_offset in [2, 3]:
                    target_idx = current_idx + rank_offset
                    if target_idx >= len(b_brackets_sorted):
                        continue
                    target = b_brackets_sorted[target_idx]
                    t_ticker  = target.get("ticker", "")
                    t_no_ask  = float(target.get("ob_no_ask") or target.get("no_price") or 0)
                    t_floor   = target.get("floor")

                    if t_ticker in already_traded:
                        continue
                    if not (NEAR_CAP_NO_MIN <= t_no_ask <= NEAR_CAP_NO_MAX):
                        continue

                    log.info(
                        "NEAR_CAP  %s  %s  obs=%.1f°  intra=%.2f  N+%d  No=%.2f",
                        city, t_ticker, observed_high, intra_pos, rank_offset, t_no_ask,
                    )
                    result["signals"].append({
                        "ticker":       t_ticker,
                        "title":        target.get("title", ""),
                        "floor":        t_floor,
                        "cap":          target.get("cap"),
                        "yes_ask":      float(target.get("ob_yes_ask") or 0),
                        "no_ask":       t_no_ask,
                        "spread":       target.get("ob_spread"),
                        "yes_depth":    target.get("ob_yes_depth"),
                        "no_depth":     target.get("ob_no_depth"),
                        "volume":       target.get("volume"),
                        "score":        4,
                        "score_detail": [
                            "near_cap_obs",
                            f"intra_pos={intra_pos:.2f}",
                            f"rank=N+{rank_offset}",
                            f"obs={observed_high:.1f}F",
                        ],
                        "trade_type":    "NO",
                        "entry_price":   t_no_ask,
                        "entry_tier":    "near_cap",
                        "market_type":   "high",
                        "max_contracts": MAX_CONTRACTS,
                        "skip_reason":   None,
                    })

    return result


# ---------------------------------------------------------------------------
# Full pipeline
# ---------------------------------------------------------------------------

def run(city_filter: str = None, paper: bool = False) -> tuple[list[dict], dict]:
    """
    Run the HIGH decision engine for all cities.
    Returns (evaluations, nws_snapshot) — the snapshot is passed to the LOWT
    engine to avoid a redundant full NWS fetch each poll cycle.
    """
    profiles = load_profiles()

    print("Fetching NWS live data...")
    nws_results = nws_feed.snapshot(city_filter)

    print("Scanning Kalshi markets...")
    kalshi_results = kalshi_scanner.scan_all(city_filter, market_type="high")

    print("\nEvaluating signals...\n")
    evaluations = []

    for city in list(nws_results.keys()):
        nws_data    = nws_results.get(city, {})
        scan_data   = kalshi_results.get(city, {})
        eval_result = evaluate_city(city, nws_data, scan_data, profiles, market_type="high")
        evaluations.append(eval_result)

    # ── Cascade tier — pure market-confirmation signals ───────────────────────
    # Reuses kalshi_results already fetched above — no extra API calls.
    # Passes nws_results so the cascade can use the corrected forecast to
    # skip the forecast bracket (previously nws_data was always {}).
    cascade_evals = cascade_engine.run(kalshi_results, city_filter, nws_results=nws_results)
    evaluations.extend(cascade_evals)

    return evaluations, nws_results, kalshi_results


# ---------------------------------------------------------------------------
# Display
# ---------------------------------------------------------------------------

def display(evaluations: list[dict]):
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    bias_source = "per-city (forecast_bias.json)" if _FORECAST_BIAS else f"global fallback ({FORECAST_BIAS_CORRECTION:+.1f}°F)"

    print(f"\n{'='*72}")
    print(f"  Decision Engine  —  {now_utc}")
    print(f"  Bias source: {bias_source}")
    print(f"{'='*72}")

    any_signal = False

    for ev in evaluations:
        # Cascade evals have their own display — skip them here
        if ev.get("cascade"):
            continue

        city = ev["city"]

        if ev.get("error"):
            print(f"\n{city}: ERROR — {ev['error']}")
            continue

        snap        = ev.get("nws_snapshot", {})
        trade_start = _trade_start_for(city)
        trade_end   = _trade_end_for(city)
        bias        = ev.get("city_bias", FORECAST_BIAS_CORRECTION)
        bias_std    = _city_bias_stddev(city)
        buf         = ev.get("dynamic_buffer", BOUNDARY_BUFFER_FALLBACK)

        fmt = lambda v: f"{v:.1f}" if v is not None else "N/A"
        std_str = f" ±{bias_std:.1f}" if bias_std > 0 else ""
        # Show corrected forecast (AccuWeather if available, else NWS+bias)
        corrected_fcst = ev.get("corrected_forecast")
        nws_fcst       = snap.get("forecast_high_f")
        aw_fcst        = ev.get("accuweather_forecast")
        if aw_fcst is not None:
            fcst_str = f"{fmt(aw_fcst)}° (AW)"
        elif corrected_fcst is not None:
            fcst_str = f"{fmt(corrected_fcst)}° (NWS+bias)"
        else:
            fcst_str = f"{fmt(nws_fcst)}° (NWS raw)"
        print(
            f"\n{city}  |  local: {snap.get('local_time','?')}  "
            f"curr: {fmt(snap.get('current_temp_f'))}°  "
            f"obs_hi: {fmt(snap.get('observed_high_f'))}°  "
            f"fcst_hi: {fcst_str}  "
            f"bias: {bias:+.2f}{std_str}°  buf: {buf}°  "
            f"window: {trade_start:02d}:00–{trade_end:02d}:00"
        )

        active_signals = [s for s in ev["signals"] if s.get("trade_type")]
        skipped        = [s for s in ev["signals"] if not s.get("trade_type")]

        if not active_signals:
            skip_reasons = set(s.get("skip_reason") or "no trade type" for s in skipped)
            print(f"  No signals — {'; '.join(skip_reasons)}")
            continue

        any_signal = True
        print(f"  {'Bracket':<22} {'Type':>5} {'Entry':>7} {'Score':>6}  Details")
        print(f"  {'-'*60}")

        for s in active_signals:
            floor = s.get("floor")
            cap   = s.get("cap")
            if floor is not None and cap is not None:
                bracket_str = f"{floor}–{cap}°F"
            elif floor is not None:
                bracket_str = f">{floor}°F"
            elif cap is not None:
                bracket_str = f"<{cap}°F"
            else:
                bracket_str = "?"

            detail_str = ", ".join(s.get("score_detail", []))
            print(
                f"  {bracket_str:<22} "
                f"{s['trade_type']:>5} "
                f"${s['entry_price']:.2f}  "
                f"{s['score']}/5    "
                f"{detail_str}"
            )

    print(f"\n{'='*72}")
    if not any_signal:
        print("  No actionable signals at this time.")

    # Show cascade signals separately
    cascade_engine.display(evaluations)


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kalshi weather trading decision engine")
    parser.add_argument("--city",  type=str, default=None, help="Filter to one city")
    parser.add_argument("--paper", action="store_true",    help="Paper trade mode (log only, no orders)")
    args = parser.parse_args()

    evaluations, _ = run(city_filter=args.city, paper=args.paper)
    display(evaluations)

    if args.paper:
        out = Path("data/paper_trades.json")
        out.parent.mkdir(parents=True, exist_ok=True)
        existing = json.loads(out.read_text()) if out.exists() else []
        existing.extend(evaluations)
        out.write_text(json.dumps(existing, indent=2, default=str))
        print(f"\n  Paper trade log saved to {out}")
