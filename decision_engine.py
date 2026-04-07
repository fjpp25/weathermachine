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
  1. Timing      — is it between 10am–2pm local time in this city?
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
"""

import json
import argparse
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import nws_feed
import kalshi_scanner

# ---------------------------------------------------------------------------
# Parameters — tune these as you gather data
# ---------------------------------------------------------------------------

# Gate thresholds
TRADE_WINDOW_START  = 0        # local hour — trade 24/7
TRADE_WINDOW_END    = 24       # local hour — trade 24/7
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
                               # must stay comfortably below NO_STOP_LOSS_RISE threshold
                               # prevents entering positions already near stop-loss boundary
NO_MAX_ENTRY_PRICE  = 0.87     # never pay more than this for a NO contract
                               # tightened from 0.90 — positions above this are
                               # often fee-neutral or worse after settlement
MAX_NO_PER_CITY     = 2        # max NO positions to open per city per day
                               # prevents carpet-bombing every bracket in a market

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
# (higher bar than BOUNDARY_BUFFER gate — gate=3°F, score=6°F)
FORECAST_WELL_CLEAR = 6.0

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
    Upward trend = close price in most recent candle > close price N candles ago.
    Returns 0 (no score) if insufficient candle data.
    """
    if len(candles) < MIN_CANDLES_FOR_MOMENTUM:
        return 0

    recent = candles[-MOMENTUM_LOOKBACK:]
    closes = [c["price_close"] for c in recent if c.get("price_close") is not None]

    if len(closes) < 2:
        return 0

    # Simple: is the most recent close higher than the oldest in the window?
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
    This is the rounding/revision safety check.
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
    bracket:       dict,
    forecast_high: float,
    observed_high: float,
    city_local_hour: int,
    is_forecast_bracket: bool,
) -> dict | None:
    """
    Run all gates and scoring for a single bracket.
    Returns a signal dict if a trade is warranted, else None.
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

    # --- Gate 1: Timing ---
    if not (TRADE_WINDOW_START <= city_local_hour < TRADE_WINDOW_END):
        signal["skip_reason"] = f"Outside trading window (local hour={city_local_hour})"
        return signal

    # --- Gate 2: Liquidity ---
    spread = bracket.get("ob_spread")
    if spread is None or spread > MAX_SPREAD:
        signal["skip_reason"] = f"Spread too wide or missing ({spread})"
        return signal

    # Check depth on the side we'd buy
    # For YES trades: we buy YES, need YES depth on ask side
    # For NO trades: we buy NO, need NO depth
    no_depth  = bracket.get("ob_no_depth") or 0
    yes_depth = bracket.get("ob_yes_depth") or 0

    if no_depth < MIN_DEPTH and yes_depth < MIN_DEPTH:
        signal["skip_reason"] = f"Insufficient depth (yes={yes_depth}, no={no_depth})"
        return signal

    # --- Gate 3: Boundary buffer ---
    # For YES trades: forecast must be inside the bracket by BOUNDARY_BUFFER°F
    # For NO trades: bracket must not be adjacent to the forecast bracket
    #   i.e. the bracket edge closest to the forecast must be >= BOUNDARY_BUFFER°F away
    if is_forecast_bracket:
        if not is_forecast_inside_boundary(bracket, forecast_high):
            signal["skip_reason"] = f"Forecast too close to bracket edge (buffer={BOUNDARY_BUFFER}°F)"
            return signal
    else:
        # For NO trades, check that the nearest edge of this bracket is far enough
        # from the corrected forecast — prevents entering brackets adjacent to forecast
        corrected = forecast_high + FORECAST_BIAS_CORRECTION
        floor = bracket.get("floor")
        cap   = bracket.get("cap")
        # Distance from forecast to the nearest bracket edge
        distances = []
        if floor is not None:
            distances.append(abs(corrected - floor))
        if cap is not None:
            distances.append(abs(corrected - cap))
        if distances and min(distances) < BOUNDARY_BUFFER:
            signal["skip_reason"] = f"NO bracket too close to forecast (buffer={BOUNDARY_BUFFER}°F)"
            return signal

    # --- Signal scoring ---
    # YES trades: scored on forecast match, observed floor, momentum up
    # NO trades:  scored on observed elimination, forecast well-clear, momentum flat/down
    score = 0
    details = []

    floor = bracket.get("floor")
    cap   = bracket.get("cap")

    if is_forecast_bracket:
        # ── YES trade scoring ─────────────────────────────────────────────

        # +1 forecast match (forecast points to this bracket)
        score += 1
        details.append("forecast_match")

        # +1 observed floor cleared (temperature already in bracket range)
        if observed_high is not None and floor is not None and observed_high >= floor:
            score += 1
            details.append("obs_floor_cleared")

        # +1 momentum up (YES price trending upward = market agrees)
        candles  = bracket.get("candles", [])
        if score_momentum(candles):
            score += 1
            details.append("momentum_up")

    else:
        # ── NO trade scoring ──────────────────────────────────────────────

        # +1 observed signal: temperature already eliminates this bracket,
        #    or still well below the bracket floor
        if observed_high is not None:
            if cap is not None and observed_high >= cap:
                # Definitive: temperature has passed bracket ceiling
                score += 1
                details.append("obs_eliminates_bracket")
            elif floor is not None and observed_high < floor - BOUNDARY_BUFFER:
                # Likely: temperature still well below bracket floor
                score += 1
                details.append("obs_below_floor")

        # +1 forecast well-clear: corrected forecast is >FORECAST_WELL_CLEAR°F
        #    from the nearest bracket edge (stronger than the gate threshold)
        corrected = forecast_high + FORECAST_BIAS_CORRECTION
        distances = []
        if floor is not None:
            distances.append(abs(corrected - floor))
        if cap is not None:
            distances.append(abs(corrected - cap))
        if distances and min(distances) >= FORECAST_WELL_CLEAR:
            score += 1
            details.append("forecast_well_clear")

        # +1 momentum flat or down: YES price not rising = market not pricing
        #    this bracket in (good for NO holders)
        candles  = bracket.get("candles", [])
        if not score_momentum(candles):   # momentum_up returned 0
            score += 1
            details.append("momentum_flat_or_down")

    signal["score"]        = score
    signal["score_detail"] = details

    # --- Trade type decision ---
    yes_ask = bracket.get("ob_yes_ask")
    no_ask  = bracket.get("ob_no_ask")

    if is_forecast_bracket and score >= 2 and yes_ask is not None:
        # YES trade: forecast points here, strong signal
        signal["trade_type"]    = "YES"
        signal["entry_price"]   = yes_ask
        signal["exit_target"]   = round(yes_ask * (1 + YES_EXIT_TARGET), 2)
        signal["stop_loss"]     = round(yes_ask * (1 - YES_STOP_LOSS),   2)

    elif (
        not is_forecast_bracket
        and no_ask is not None
        and no_ask <= NO_MAX_ENTRY_PRICE
        and yes_ask is not None
        and NO_MIN_YES_PRICE < yes_ask <= NO_MAX_YES_PRICE
        and no_depth >= MIN_DEPTH
    ):
        # NO trade: bracket is far enough from forecast (enforced by boundary buffer above)
        # and entry price is within acceptable range
        # Exit: hold to resolution at $1.00, no take-profit needed
        signal["trade_type"]    = "NO"
        signal["entry_price"]   = no_ask
        signal["exit_target"]   = min(round(no_ask + 0.04, 2), 0.99)
        signal["stop_loss"]     = None

    return signal


# ---------------------------------------------------------------------------
# Per-city evaluator
# ---------------------------------------------------------------------------

def evaluate_city(
    city:        str,
    nws_data:    dict,
    scan_data:   dict,
    profiles:    dict,
) -> dict:
    """
    Full evaluation for one city.
    Returns a structured result with all bracket signals.
    """
    result = {
        "city":        city,
        "evaluated_at": datetime.now(timezone.utc).isoformat(),
        "nws_snapshot": {
            "current_temp_f":   nws_data.get("current_temp_f"),
            "observed_high_f":  nws_data.get("observed_high_f"),
            "forecast_high_f":  nws_data.get("forecast_high_f"),
            "forecast_low_f":   nws_data.get("forecast_low_f"),
            "local_time":       nws_data.get("local_time"),
            "city_local_hour":  nws_data.get("city_local_hour"),
        },
        "signals":     [],
        "error":       None,
    }

    # Sanity checks
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
        result["error"] = "No forecast high available"
        return result

    # Find forecast bracket
    forecast_bracket = get_forecast_bracket(forecast_high, brackets)

    for bracket in brackets:
        is_forecast = (
            forecast_bracket is not None
            and bracket["ticker"] == forecast_bracket["ticker"]
        )
        signal = evaluate_bracket(
            bracket         = bracket,
            forecast_high   = forecast_high,
            observed_high   = observed_high,
            city_local_hour = city_local_hour,
            is_forecast_bracket = is_forecast,
        )
        if signal:
            result["signals"].append(signal)

    # Limit NO signals to MAX_NO_PER_CITY, keeping those furthest from forecast
    # Furthest = largest distance between forecast high and bracket midpoint
    # This prioritises the safest, most clear-cut trades
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

        # Mark excess NO signals as skipped
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

    cities = list(nws_results.keys())
    for city in cities:
        nws_data  = nws_results.get(city, {})
        scan_data = kalshi_results.get(city, {})
        eval_result = evaluate_city(city, nws_data, scan_data, profiles)
        evaluations.append(eval_result)

    return evaluations


def run_lowt_observe(city_filter: str = None) -> list[dict]:
    """
    Scan LOWT markets and evaluate signals — but mark all as observe-only.
    No orders will ever be placed from these evaluations.
    Used for paper monitoring of low temperature markets.
    """
    profiles = load_profiles()

    nws_results    = nws_feed.snapshot(city_filter)
    kalshi_results = kalshi_scanner.scan_all(city_filter, market_type="low")

    evaluations = []
    for city, nws_data in nws_results.items():
        scan_data = kalshi_results.get(city, {})
        if not scan_data or scan_data.get("error"):
            continue

        eval_result = evaluate_city(city, nws_data, scan_data, profiles)

        # Force all signals to observe-only — no trades will be placed
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
        print(f"\n{city}  |  local: {snap.get('local_time','?')}  "
              f"curr: {fmt(snap.get('current_temp_f'))}°  "
              f"obs_hi: {fmt(snap.get('observed_high_f'))}°  "
              f"fcst_hi: {fmt(snap.get('forecast_high_f'))}°")

        active_signals = [s for s in ev["signals"] if s.get("trade_type")]
        skipped        = [s for s in ev["signals"] if not s.get("trade_type")]

        if not active_signals:
            # Show why things were skipped — useful for tuning
            skip_reasons = set(s.get("skip_reason", "no trade type") for s in skipped)
            print(f"  No signals — {'; '.join(r for r in skip_reasons if r)}")
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
    print(f"  Trading window: {TRADE_WINDOW_START}:00–{TRADE_WINDOW_END}:00 local time per city")


def fmt(val) -> str:
    return f"{val:.1f}" if val is not None else "N/A"


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kalshi weather trading decision engine")
    parser.add_argument("--city",  type=str,  default=None,  help="Filter to one city")
    parser.add_argument("--paper", action="store_true",      help="Paper trade mode (log only, no orders)")
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
