"""
decision_engine.py
------------------
Synthesizes city profiles, NWS live feed, and Kalshi scanner data into
actionable NO trade signals for temperature HIGH markets.

Only NO trades are generated — buying NO on brackets the forecast
makes unlikely, collecting premium as the market converges to resolution.

Gates (applied in order — any failure skips to next bracket):
  1. Timing      — city_local_hour must be within the city's entry window.
                   Per-city start hours are defined in cities.py (trade_start_high /
                   trade_start_lowt), calibrated from entry_window_analysis.py.
                   TRADE_WINDOW_END is currently 24 (gate disabled — scheduler handles timing).
  2. Liquidity   — spread ≤ MAX_SPREAD and depth ≥ MIN_DEPTH
  3. Boundary    — bias-corrected forecast is ≥ dynamic_buffer°F from both bracket edges
                   (dynamic_buffer scales with city's tmax_stddev from city_profiles.json)

Signal scoring (0–3):
  +1 obs_eliminates_bracket  — observed high ≥ bracket cap (bracket physically impossible)
  +1 obs_below_floor         — observed high < bracket floor − buffer (bracket not yet in play)
  +1 forecast_well_clear     — corrected forecast ≥ FORECAST_WELL_CLEAR°F from nearest edge
  +1 momentum_flat_or_down   — no upward price momentum in recent candles

Usage:
  python decision_engine.py                 # run full analysis, all cities
  python decision_engine.py --city Miami    # single city
  python decision_engine.py --paper         # paper-trade mode (log only)

Dependencies:
  city_profiles.py   (data/city_profiles.json must exist)
  bias_calculator.py (data/forecast_bias.json used when available)
  nws_feed.py
  kalshi_scanner.py
  cities.py          (trade_start_high / trade_start_lowt per city)
"""

import json
import argparse
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import nws_feed
import kalshi_scanner
from cities import CITIES as _CITY_REGISTRY

# ---------------------------------------------------------------------------
# Parameters — tune these as you gather data
# ---------------------------------------------------------------------------

# Gate thresholds
TRADE_WINDOW_START  = 0        # local hour — global fallback when no per-city value set
TRADE_WINDOW_END    = 24       # local hour — gate currently DISABLED (0–24 = always open)
                               # Timing is handled by scheduler.py's dynamic interval.
                               # To re-enable a hard close: set TRADE_WINDOW_END = 14
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
NO_BAN_ABOVE_BRACKETS = True   # never trade NO on "above X°" (T) brackets for HIGH markets
                               # spring/summer: temps trending up → asymmetric risk upward
                               # data: 29% WR, -$6.52 across 7 trades
MAX_CONTRACTS       = 2        # hard cap on contracts per position
                               # data: 3-contract losses average -$1.74 each, far worse than 1-2

# Momentum detection
MIN_CANDLES_FOR_MOMENTUM = 3   # need at least this many candles to score momentum
MOMENTUM_LOOKBACK        = 3   # look at last N candles for direction

# Global NWS forecast bias fallback (used when data/forecast_bias.json has no entry for city)
# Overridden per city by data/forecast_bias.json when available.
FORECAST_BIAS_CORRECTION = -1.0   # °F — subtract from NWS forecast high

# Forecast well-clear threshold for scoring
# Bracket must be this far from the corrected forecast to score the forecast point
# (higher bar than boundary buffer gate — gate≈3°F, score=6°F)
FORECAST_WELL_CLEAR = 6.0


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
    """Return per-city bias correction, falling back to global constant."""
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

    1. cities.py — cities with trading=False are the static default.
    2. data/config.json (optional) — 'paused_cities' key allows runtime
       pauses without touching source code.

    NOTE: PAUSED_CITIES is built once at import time.
    Restart the scheduler to pick up config.json changes.
    """
    paused = {name for name, meta in _CITY_REGISTRY.items() if not meta.get("trading")}

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
    Return the earliest local hour for entering trades in this city.

    Looks up trade_start_high or trade_start_lowt from cities.py.
    Falls back to TRADE_WINDOW_START (global default) if not set.

    The per-city values are calibrated from entry_window_analysis.py and
    reflect when the NWS morning forecast has stabilised enough for NO
    entries to be reliable.
    """
    meta = _CITY_REGISTRY.get(city, {})
    key  = "trade_start_high" if market_type == "high" else "trade_start_lowt"
    val  = meta.get(key)
    return val if val is not None else TRADE_WINDOW_START


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

def get_forecast_bracket(forecast_high: float, brackets: list[dict]) -> dict | None:
    """Find which bracket the (bias-corrected) forecast high falls into."""
    for bracket in brackets:
        floor = bracket.get("floor")
        cap   = bracket.get("cap")
        if floor is not None and cap is not None:
            if floor <= forecast_high < cap:
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
    bracket:          dict,
    forecast_high:    float,
    observed_high:    float | None,
    city_local_hour:  int,
    trade_start_hour: int,
    city_bias:        float,
    dynamic_buffer:   float,
) -> dict:
    """
    Evaluate a single bracket for a NO trade signal.

    Forecast brackets are never traded — caller should skip them before calling here.

    Args:
        bracket:          Bracket dict from kalshi_scanner (includes orderbook fields).
        forecast_high:    Raw NWS forecast high (°F) — bias correction applied internally.
        observed_high:    Observed high so far today (°F), or None if not yet available.
        city_local_hour:  Current local hour for the city.
        trade_start_hour: Earliest local hour for entering trades (per-city calibrated).
        city_bias:        Per-city NWS bias correction (°F). Applied as: corrected = forecast + bias.
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

    # --- Gate 0b: Ban NO trades on above-threshold (T) brackets for HIGH markets ---
    # cap=None + floor set means "above X°F" — asymmetric upside risk in spring/summer
    if NO_BAN_ABOVE_BRACKETS and bracket.get("cap") is None and bracket.get("floor") is not None:
        signal["skip_reason"] = "NO on above-threshold bracket banned (spring/summer upward bias)"
        return signal

    # --- Gate 1: Timing — per-city entry window ---
    if not (trade_start_hour <= city_local_hour < TRADE_WINDOW_END):
        signal["skip_reason"] = (
            f"Before entry window (local hour={city_local_hour}, "
            f"window opens at {trade_start_hour:02d}:00)"
        )
        return signal

    # --- Gate 2: Liquidity ---
    spread    = bracket.get("ob_spread")
    no_depth  = bracket.get("ob_no_depth") or 0
    yes_depth = bracket.get("ob_yes_depth") or 0

    if spread is None or spread > MAX_SPREAD:
        signal["skip_reason"] = f"Spread too wide or missing ({spread})"
        return signal

    if no_depth < MIN_DEPTH and yes_depth < MIN_DEPTH:
        signal["skip_reason"] = f"Insufficient depth (yes={yes_depth}, no={no_depth})"
        return signal

    # --- Gate 3: Boundary buffer (using per-city bias and dynamic buffer) ---
    corrected = forecast_high + city_bias
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
    score   = 0
    details = []

    if observed_high is not None:
        if cap is not None and observed_high >= cap:
            score += 1
            details.append("obs_eliminates_bracket")
        elif floor is not None and observed_high < floor - dynamic_buffer:
            score += 1
            details.append("obs_below_floor")

    if distances and min(distances) >= FORECAST_WELL_CLEAR:
        score += 1
        details.append("forecast_well_clear")

    candles = bracket.get("candles", [])
    if not score_momentum(candles):
        score += 1
        details.append("momentum_flat_or_down")

    signal["score"]        = score
    signal["score_detail"] = details

    # --- Trade type decision ---
    yes_ask = bracket.get("ob_yes_ask")
    no_ask  = bracket.get("ob_no_ask")

    if (
        no_ask is not None
        and NO_MIN_ENTRY_PRICE <= no_ask <= NO_MAX_ENTRY_PRICE
        and yes_ask is not None
        and NO_MIN_YES_PRICE < yes_ask <= NO_MAX_YES_PRICE
        and no_depth >= MIN_DEPTH
    ):
        if no_ask < 0.75 and score < 3:
            signal["skip_reason"] = (
                f"Entry ${no_ask:.2f} < 0.75 requires score 3/3 "
                f"(got {score}/3)"
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

    # Resolve per-city bias and dynamic buffer
    city_bias      = _city_bias(city)
    dyn_buffer     = _dynamic_buffer(city, profiles)
    trade_start    = _trade_start_for(city, market_type)

    # Identify the forecast bracket so we can skip it — we only trade NO on non-forecast brackets
    corrected_forecast = forecast_high + city_bias
    forecast_bracket   = get_forecast_bracket(corrected_forecast, brackets)

    result["city_bias"]      = city_bias
    result["dynamic_buffer"] = dyn_buffer

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
            bracket          = bracket,
            forecast_high    = forecast_high,
            observed_high    = observed_high,
            city_local_hour  = city_local_hour,
            trade_start_hour = trade_start,
            city_bias        = city_bias,
            dynamic_buffer   = dyn_buffer,
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

    return result


# ---------------------------------------------------------------------------
# Full pipeline
# ---------------------------------------------------------------------------

def run(city_filter: str = None, paper: bool = False) -> list[dict]:
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

    return evaluations


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
        city = ev["city"]

        if ev.get("error"):
            print(f"\n{city}: ERROR — {ev['error']}")
            continue

        snap        = ev["nws_snapshot"]
        trade_start = _trade_start_for(city)
        bias        = ev.get("city_bias", FORECAST_BIAS_CORRECTION)
        bias_std    = _city_bias_stddev(city)
        buf         = ev.get("dynamic_buffer", BOUNDARY_BUFFER_FALLBACK)

        fmt = lambda v: f"{v:.1f}" if v is not None else "N/A"
        std_str = f" ±{bias_std:.1f}" if bias_std > 0 else ""
        print(
            f"\n{city}  |  local: {snap.get('local_time','?')}  "
            f"curr: {fmt(snap.get('current_temp_f'))}°  "
            f"obs_hi: {fmt(snap.get('observed_high_f'))}°  "
            f"fcst_hi: {fmt(snap.get('forecast_high_f'))}°  "
            f"bias: {bias:+.2f}{std_str}°  buf: {buf}°  "
            f"window: {trade_start:02d}:00+"
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
                f"{s['score']}/3    "
                f"{detail_str}"
            )

    print(f"\n{'='*72}")
    if not any_signal:
        print("  No actionable signals at this time.")


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kalshi weather trading decision engine")
    parser.add_argument("--city",  type=str, default=None, help="Filter to one city")
    parser.add_argument("--paper", action="store_true",    help="Paper trade mode (log only, no orders)")
    args = parser.parse_args()

    evaluations = run(city_filter=args.city, paper=args.paper)
    display(evaluations)

    if args.paper:
        out = Path("data/paper_trades.json")
        out.parent.mkdir(parents=True, exist_ok=True)
        existing = json.loads(out.read_text()) if out.exists() else []
        existing.extend(evaluations)
        out.write_text(json.dumps(existing, indent=2, default=str))
        print(f"\n  Paper trade log saved to {out}")
