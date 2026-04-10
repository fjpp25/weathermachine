"""
decision_engine.py
------------------
Synthesizes city profiles, NWS live feed, and Kalshi scanner data into
actionable trade signals for temperature markets.

Two trade types are evaluated per bracket:

  YES trade  — buy YES on the bracket the forecast points to
               requires strong signal score (≥2/3)
               exit when price appreciates Y% or at a stop-loss

  NO trade   — buy NO on brackets that are already unlikely
               lower signal bar, driven by forecast + observed floor
               collect premium as market converges toward resolution

Gates (applied in order, any failure skips to next bracket):
  1. Timing      — city_local_hour must be within the city's entry window.
                   Per-city start hours are defined in cities.py (trade_start_high /
                   trade_start_lowt), calibrated from entry_window_analysis.py.
                   Falls back to TRADE_WINDOW_START/END when no per-city value is set.
                   Currently TRADE_WINDOW_END = 24 (no hard close — scheduler handles timing).
  2. Liquidity   — spread ≤ MAX_SPREAD and depth ≥ MIN_DEPTH
  3. Boundary    — forecast is ≥ BOUNDARY_BUFFER°F inside bracket edges

Signal scoring (0–3, used to decide YES vs NO and position size):
  +1 forecast signal   — NWS forecast high falls in this bracket
  +1 obs floor signal  — observed high so far today ≥ bracket floor
  +1 momentum signal   — price has moved toward YES in last N candles
                         (only scored if candle data is available)

Usage:
  python decision_engine.py                 # run full analysis, all cities
  python decision_engine.py --city Miami    # single city
  python decision_engine.py --paper         # paper-trade mode (log only)

Dependencies:
  city_profiles.py   (data/city_profiles.json must exist)
  nws_feed.py        (imported directly)
  kalshi_scanner.py  (imported directly)
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

BOUNDARY_BUFFER     = 3.0      # °F — forecast must be this far inside bracket edges
                               # 2.0 → too loose (NYC 76-77° loss, Mar 31 2026)
                               # 4.0 → too strict (blocks ~67% of bracket range)
                               # 3.0 → balanced starting point, revisit with backtest data

# NO trade parameters
NO_MIN_YES_PRICE    = 0.02     # skip if YES is basically zero (already dead)
NO_MAX_YES_PRICE    = 0.25     # never enter NO if YES is above this
NO_MIN_ENTRY_PRICE  = 0.75     # never pay less than this for a NO contract
                               # data: below 0.75 market is pricing in real uncertainty
NO_MAX_ENTRY_PRICE  = 0.92     # never pay more than this for a NO contract
                               # data: 0.75-0.92 gives 86.5% WR across 37 trades
MAX_NO_PER_CITY     = 2        # max NO positions to open per city per day
NO_BAN_ABOVE_BRACKETS = True   # never trade NO on "above X°" (T) brackets for HIGH markets
                               # spring/summer: temps trending up → asymmetric risk upward
                               # data: 29% WR, -$6.52 across 7 trades
MAX_CONTRACTS       = 2        # hard cap on contracts per position
                               # data: 3-contract losses average -$1.74 each, far worse than 1-2

# Exit targets
YES_EXIT_TARGET     = 0.25     # take profit when YES price rises 25%
YES_STOP_LOSS       = 0.40     # stop loss if YES price falls 40% from entry
NO_EXIT_TARGET      = 0.15     # take profit when NO price rises 15%

# Momentum detection
MIN_CANDLES_FOR_MOMENTUM = 3   # need at least this many candles to score momentum
MOMENTUM_LOOKBACK        = 3   # look at last N candles for direction

# NWS forecast warm bias correction (from research: forecasts run ~1°F warm)
FORECAST_BIAS_CORRECTION = -1.0   # subtract this from NWS forecast high

# Forecast well-clear threshold for NO trade scoring
# Bracket must be this far from the corrected forecast to score the forecast point
# (higher bar than BOUNDARY_BUFFER gate — gate=3°F, score=4°F)
# Data: fcst_gap >= 4° covers 30% of early obs at avg NO $0.944 vs 9.4% at 6°
FORECAST_WELL_CLEAR = 4.0


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
    Falls back to TRADE_WINDOW_START (global default) if not set.
    """
    meta = _CITY_REGISTRY.get(city, {})
    key  = "trade_start_high" if market_type == "high" else "trade_start_lowt"
    val  = meta.get(key)
    return val if val is not None else TRADE_WINDOW_START


def _trade_end_for(city: str, market_type: str = "high") -> int:
    """
    Return the latest local hour for entering trades in this city.
    Falls back to TRADE_WINDOW_END (global default = 24, disabled) if not set.
    """
    meta = _CITY_REGISTRY.get(city, {})
    key  = "trade_end_high" if market_type == "high" else "trade_end_lowt"
    val  = meta.get(key)
    return val if val is not None else TRADE_WINDOW_END


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
    corrected = forecast_high + FORECAST_BIAS_CORRECTION
    for b in brackets:
        floor = b.get("floor")
        cap   = b.get("cap")
        if floor is None and cap is not None:
            if corrected < cap:
                return b
        elif cap is None and floor is not None:
            if corrected >= floor:
                return b
        elif floor is not None and cap is not None:
            if floor <= corrected < cap:
                return b
    return None


def score_momentum(candles: list[dict]) -> int:
    """
    Returns +1 if price has been trending upward in the last N candles, else 0.
    Returns 0 (no score) if insufficient candle data.
    """
    if len(candles) < MIN_CANDLES_FOR_MOMENTUM:
        return 0

    recent = candles[-MOMENTUM_LOOKBACK:]
    closes = [c["price_close"] for c in recent if c.get("price_close") is not None]

    if len(closes) < 2:
        return 0

    return 1 if closes[-1] > closes[0] else 0


def bracket_contains_temp(bracket: dict, temp: float) -> bool:
    """Does a temperature value fall within this bracket?"""
    floor = bracket.get("floor")
    cap   = bracket.get("cap")
    if floor is None and cap is not None:
        return temp < cap
    elif cap is None and floor is not None:
        return temp >= floor
    elif floor is not None and cap is not None:
        return floor <= temp < cap
    return False


def is_forecast_inside_boundary(bracket: dict, forecast_high: float) -> bool:
    """
    Returns True if the bias-corrected forecast is at least BOUNDARY_BUFFER°F
    away from either edge of the bracket.
    """
    corrected = forecast_high + FORECAST_BIAS_CORRECTION
    floor = bracket.get("floor")
    cap   = bracket.get("cap")

    if floor is not None and (corrected - floor) < BOUNDARY_BUFFER:
        return False
    if cap is not None and (cap - corrected) < BOUNDARY_BUFFER:
        return False
    return True


# ---------------------------------------------------------------------------
# Per-bracket evaluator
# ---------------------------------------------------------------------------

def evaluate_bracket(
    bracket:             dict,
    forecast_high:       float,
    observed_high:       float,
    city_local_hour:     int,
    is_forecast_bracket: bool,
    trade_start_hour:    int = 0,    # per-city entry window start (from cities.py)
    trade_end_hour:      int = 24,   # per-city entry window end (from cities.py)
) -> dict | None:
    """
    Run all gates and scoring for a single bracket.
    Returns a signal dict, or None.
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
    }

    # --- Gate 0: Market must be active ---
    if bracket.get("status") not in (None, "active"):
        signal["skip_reason"] = f"Market not active (status={bracket.get('status')})"
        return signal

    # --- Gate 0b: Ban NO trades on above-threshold (T) brackets for HIGH markets ---
    if (not is_forecast_bracket and NO_BAN_ABOVE_BRACKETS
            and bracket.get("cap") is None and bracket.get("floor") is not None):
        signal["skip_reason"] = "NO on above-threshold bracket banned (spring upward bias)"
        return signal

    # --- Gate 1: Timing — per-city entry window ---
    # trade_start_hour and trade_end_hour from cities.py.
    if city_local_hour < trade_start_hour:
        signal["skip_reason"] = (
            f"Before entry window (local={city_local_hour:02d}:00, "
            f"opens {trade_start_hour:02d}:00)"
        )
        return signal
    if city_local_hour >= trade_end_hour:
        signal["skip_reason"] = (
            f"After entry window (local={city_local_hour:02d}:00, "
            f"closed {trade_end_hour:02d}:00)"
        )
        return signal

    # --- Gate 2: Liquidity ---
    spread = bracket.get("ob_spread")
    if spread is None:
        signal["skip_reason"] = "Spread missing (no resting orders on one or both sides)"
        return signal
    if spread > MAX_SPREAD:
        signal["skip_reason"] = f"Spread too wide ({spread:.2f} > {MAX_SPREAD})"
        return signal

    no_depth  = bracket.get("ob_no_depth") or 0
    yes_depth = bracket.get("ob_yes_depth") or 0

    if no_depth < MIN_DEPTH and yes_depth < MIN_DEPTH:
        signal["skip_reason"] = f"Insufficient depth (yes={yes_depth}, no={no_depth})"
        return signal

    # --- Gate 3: Boundary buffer ---
    if is_forecast_bracket:
        if not is_forecast_inside_boundary(bracket, forecast_high):
            signal["skip_reason"] = f"Forecast too close to bracket edge (buffer={BOUNDARY_BUFFER}°F)"
            return signal
    else:
        corrected = forecast_high + FORECAST_BIAS_CORRECTION
        floor     = bracket.get("floor")
        cap       = bracket.get("cap")
        distances = []
        if floor is not None:
            distances.append(abs(corrected - floor))
        if cap is not None:
            distances.append(abs(corrected - cap))
        if distances and min(distances) < BOUNDARY_BUFFER:
            signal["skip_reason"] = f"NO bracket too close to forecast (buffer={BOUNDARY_BUFFER}°F)"
            return signal

    # --- Signal scoring ---
    score   = 0
    details = []
    floor   = bracket.get("floor")
    cap     = bracket.get("cap")

    if is_forecast_bracket:
        # ── YES trade scoring ─────────────────────────────────────────────

        score += 1
        details.append("forecast_match")

        if observed_high is not None and floor is not None and observed_high >= floor:
            score += 1
            details.append("obs_floor_cleared")

        candles = bracket.get("candles", [])
        if score_momentum(candles):
            score += 1
            details.append("momentum_up")

    else:
        # ── NO trade scoring ──────────────────────────────────────────────

        if observed_high is not None:
            if cap is not None and observed_high >= cap:
                score += 1
                details.append("obs_eliminates_bracket")
            elif floor is not None and observed_high < floor - BOUNDARY_BUFFER:
                score += 1
                details.append("obs_below_floor")

        corrected = forecast_high + FORECAST_BIAS_CORRECTION
        distances = []
        if floor is not None:
            distances.append(abs(corrected - floor))
        if cap is not None:
            distances.append(abs(corrected - cap))
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

    if is_forecast_bracket and score >= 2 and yes_ask is not None:
        signal["trade_type"]  = "YES"
        signal["entry_price"] = yes_ask
        signal["exit_target"] = round(yes_ask * (1 + YES_EXIT_TARGET), 2)
        signal["stop_loss"]   = round(yes_ask * (1 - YES_STOP_LOSS),   2)

    elif (
        not is_forecast_bracket
        and no_ask is not None
        and NO_MIN_ENTRY_PRICE <= no_ask <= NO_MAX_ENTRY_PRICE
        and yes_ask is not None
        and NO_MIN_YES_PRICE < yes_ask <= NO_MAX_YES_PRICE
        and no_depth >= MIN_DEPTH
    ):
        if no_ask < 0.75 and signal.get("score", 0) < 3:
            signal["trade_type"]  = None
            signal["skip_reason"] = (
                f"Entry ${no_ask:.2f} < 0.75 requires score 3/3 "
                f"(got {signal.get('score', 0)}/3)"
            )
        else:
            signal["trade_type"]    = "NO"
            signal["entry_price"]   = no_ask
            signal["exit_target"]   = min(round(no_ask + 0.04, 2), 0.99)
            signal["stop_loss"]     = None
            signal["max_contracts"] = MAX_CONTRACTS

    return signal


# ---------------------------------------------------------------------------
# Per-city evaluator
# ---------------------------------------------------------------------------

def evaluate_city(
    city:      str,
    nws_data:  dict,
    scan_data: dict,
    profiles:  dict,
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

    # Look up per-city entry window from cities.py
    trade_start = _trade_start_for(city, market_type)
    trade_end   = _trade_end_for(city, market_type)

    forecast_bracket = get_forecast_bracket(forecast_high, brackets)

    for bracket in brackets:
        is_forecast = (
            forecast_bracket is not None
            and bracket["ticker"] == forecast_bracket["ticker"]
        )
        signal = evaluate_bracket(
            bracket             = bracket,
            forecast_high       = forecast_high,
            observed_high       = observed_high,
            city_local_hour     = city_local_hour,
            is_forecast_bracket = is_forecast,
            trade_start_hour    = trade_start,
            trade_end_hour      = trade_end,
        )
        if signal:
            result["signals"].append(signal)

    # Limit NO signals to MAX_NO_PER_CITY, keeping those furthest from forecast
    no_signals = [s for s in result["signals"] if s.get("trade_type") == "NO"]
    if len(no_signals) > MAX_NO_PER_CITY:
        corrected_forecast = (forecast_high or 0) + FORECAST_BIAS_CORRECTION

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

def run(city_filter: str = None) -> list[dict]:
    profiles = load_profiles()

    print("Fetching NWS live data...")
    nws_results = nws_feed.snapshot(city_filter)

    print("Scanning Kalshi markets...")
    kalshi_results = kalshi_scanner.scan_all(city_filter, market_type="high")

    print("\nEvaluating signals...\n")
    evaluations = []

    for city in list(nws_results.keys()):
        nws_data  = nws_results.get(city, {})
        scan_data = kalshi_results.get(city, {})
        eval_result = evaluate_city(city, nws_data, scan_data, profiles, market_type="high")
        evaluations.append(eval_result)

    return evaluations


def run_lowt_observe(city_filter: str = None) -> list[dict]:
    """
    Scan LOWT markets and evaluate signals — but mark all as observe-only.
    No orders will ever be placed from these evaluations.
    """
    profiles = load_profiles()

    nws_results    = nws_feed.snapshot(city_filter)
    kalshi_results = kalshi_scanner.scan_all(city_filter, market_type="low")

    evaluations = []
    for city, nws_data in nws_results.items():
        scan_data = kalshi_results.get(city, {})
        if not scan_data or scan_data.get("error"):
            continue

        eval_result = evaluate_city(city, nws_data, scan_data, profiles, market_type="lowt")

        for signal in eval_result.get("signals", []):
            if signal.get("trade_type"):
                signal["observe_only"] = True
                signal["skip_reason"]  = "LOWT observe-only mode"
                signal["trade_type"]   = None

        eval_result["market_type"] = "lowt"
        evaluations.append(eval_result)

    return evaluations


# ---------------------------------------------------------------------------
# Display
# ---------------------------------------------------------------------------

def display(evaluations: list[dict]):
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    print(f"\n{'='*72}")
    print(f"  Decision Engine  —  {now_utc}")
    print(f"{'='*72}")

    any_signal = False

    for ev in evaluations:
        city = ev["city"]

        if ev.get("error"):
            print(f"\n{city}: ERROR — {ev['error']}")
            continue

        snap = ev["nws_snapshot"]
        trade_start = _trade_start_for(city)
        trade_end   = _trade_end_for(city)
        window_str  = f"{trade_start:02d}:00–{trade_end:02d}:00" if trade_end < 24 else f"{trade_start:02d}:00+"
        print(f"\n{city}  |  local: {snap.get('local_time','?')}  "
              f"curr: {fmt(snap.get('current_temp_f'))}°  "
              f"obs_hi: {fmt(snap.get('observed_high_f'))}°  "
              f"fcst_hi: {fmt(snap.get('forecast_high_f'))}°  "
              f"window: {window_str}")

        active_signals = [s for s in ev["signals"] if s.get("trade_type")]
        skipped        = [s for s in ev["signals"] if not s.get("trade_type")]

        if not active_signals:
            skip_reasons = set(s.get("skip_reason") or "no trade type" for s in skipped)
            print(f"  No signals — {'; '.join(skip_reasons)}")
            continue

        any_signal = True
        print(f"  {'Bracket':<22} {'Type':>5} {'Entry':>7} {'Target':>8} {'Score':>6}  Details")
        print(f"  {'-'*68}")

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
                f"${s['exit_target']:.2f}    "
                f"{s['score']}/3    "
                f"{detail_str}"
            )

    print(f"\n{'='*72}")
    if not any_signal:
        print("  No actionable signals at this time.")
    print(f"  Bias correction applied: {FORECAST_BIAS_CORRECTION:+.1f}°F to all NWS forecasts")


def fmt(val) -> str:
    return f"{val:.1f}" if val is not None else "N/A"


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
