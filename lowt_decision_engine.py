"""
lowt_decision_engine.py
-----------------------
Generates NO trade signals for Kalshi daily low-temperature (LOWT) markets.

Strategy:
  Buy NO on "low below X°F" (B-prefix) brackets where the current observed
  temperature is already well above the bracket cap, making it near-impossible
  for the day's low to settle inside that bracket.

  This is obs-driven, not forecast-driven. The signal is the overnight reading
  itself — if it's 89°F in Miami at midnight and the bracket cap is 64.5°F,
  no forecast is needed to know that position is dead.

Signal scoring (0–3):
  +1  obs_eliminates_bracket    — max(current_temp, observed_low) > cap + OBS_ELIM_MARGIN
                                   Overnight temp is already far enough above cap that
                                   cooling to it would require an extraordinary event.
  +1  tonight_fcst_well_clear   — NWS forecast low for tonight > cap + FORECAST_LOW_MARGIN
                                   Note: NWS "forecast low" is the nighttime period for today
                                   (tonight's low), not this morning's low. Both the observed
                                   morning low AND tonight's forecast being above the cap
                                   gives double confirmation.
  +1  momentum_flat_or_down     — No upward price pressure on YES in recent candles.
                                 Market isn't repricing toward YES resolution.

Gates (applied in order — any failure skips to next bracket):
  1. City whitelist  — only LOWT_CITIES are traded (pilot: NY, LA, Chicago)
  2. Bracket type    — B-prefix (low BELOW X°F) only; T-prefix banned.
                       T-brackets ("low above X°F") have unbounded downside risk.
  3. Liquidity       — spread ≤ MAX_SPREAD and no_depth ≥ MIN_DEPTH_LOWT
  4. Signal present  — at least one of obs_eliminates_bracket OR forecast_well_clear
                       must be true before considering price gates.
  5. Price gates     — no_ask in [NO_MIN_ENTRY, NO_MAX_ENTRY]
                       yes_ask in (NO_MIN_YES, NO_MAX_YES]

Exit strategy:
  Hold to settlement. No take-profit trigger — LOWT convergence is decisive
  (NO goes $0.85 → $0.99 in one poll cycle when the low is locked in).
  Stop-loss only: handled by trader.py's check_exits() via NO_STOP_LOSS_RISE.

Pilot cities: New York, Los Angeles, Chicago.
  These are the three most liquid LOWT markets (avg depth 1,100–1,180 in
  the tradeable range). Expand LOWT_CITIES once edge is confirmed.

Usage:
  python lowt_decision_engine.py                  # all pilot cities
  python lowt_decision_engine.py --city Chicago   # single city
  python lowt_decision_engine.py --paper          # paper mode (log only)

Dependencies:
  nws_feed.py       — current_temp_f, observed_low_f, forecast_low_f
  kalshi_scanner.py — brackets with orderbook (market_type="low")
  cities.py         — CITIES registry
  trader.py         — place_order, _append_trade_log (called by run() when not paper)
"""

import json
import argparse
from datetime import datetime, timezone
from pathlib import Path

import nws_feed
import kalshi_scanner
from cities import CITIES as _CITY_REGISTRY

# ---------------------------------------------------------------------------
# Parameters
# ---------------------------------------------------------------------------

# Pilot city whitelist — expand after edge confirmed
LOWT_CITIES: set[str] = {"New York", "Los Angeles", "Chicago"}

# Liquidity
MAX_SPREAD      = 0.05   # max bid-ask spread ($)
MIN_DEPTH_LOWT  = 100    # min contracts on buying side
                          # LOWT avg depth ~518 vs HIGH ~3,237 — separate threshold needed

# Obs elimination gate
OBS_ELIM_MARGIN     = 15.0   # current/observed temp must be this many °F above bracket cap
                              # at 15°F margin a 1-in-50 event would need a >15°F overnight crash
                              # data: overnight signals at margins of +15–25°F had ~0% loss rate

# Forecast confirmation gate
# NWS "forecast low" is actually tonight's overnight low (the nighttime period
# for today's date), not the low that already occurred this morning.
# Both are useful: if tonight's low is still well above the cap, the bracket
# is doubly confirmed. But the observed morning low is the primary signal.
FORECAST_LOW_MARGIN = 6.0    # tonight's NWS forecast low must be this many °F above bracket cap
                              # mirrors FORECAST_WELL_CLEAR from HIGH engine

# Price gates — identical to HIGH engine
NO_MIN_ENTRY_PRICE = 0.75    # never pay less (below here, market prices in real uncertainty)
NO_MAX_ENTRY_PRICE = 0.92    # never pay more (86.5% WR in this band from HIGH data)
NO_MIN_YES_PRICE   = 0.02    # skip if YES is already dead
NO_MAX_YES_PRICE   = 0.25    # skip if YES is too alive

# Position limits
MAX_NO_PER_CITY  = 2   # max NO positions per city per day
MAX_CONTRACTS    = 2   # hard cap per position

# Momentum — same parameters as HIGH engine
MIN_CANDLES_FOR_MOMENTUM = 3
MOMENTUM_LOOKBACK        = 3


# ---------------------------------------------------------------------------
# Helpers shared with HIGH engine logic
# ---------------------------------------------------------------------------

def _score_momentum(candles: list[dict]) -> bool:
    """True if YES price is moving upward (momentum against our NO position)."""
    if len(candles) < MIN_CANDLES_FOR_MOMENTUM:
        return False
    recent = candles[-MOMENTUM_LOOKBACK:]
    closes = [c.get("yes_price_close") or c.get("close") or 0 for c in recent]
    if len(closes) < 2:
        return False
    return closes[-1] > closes[0]


# ---------------------------------------------------------------------------
# Per-bracket evaluator
# ---------------------------------------------------------------------------

def evaluate_bracket_lowt(
    bracket:       dict,
    current_temp:  float | None,
    observed_low:  float | None,
    forecast_low:  float | None,
) -> dict:
    """
    Evaluate a single LOWT bracket for a NO trade signal.

    Only B-brackets (low BELOW X°F) are considered.
    T-brackets (low ABOVE X°F) are rejected — banned by design.

    Args:
        bracket:      Bracket dict from kalshi_scanner (includes orderbook fields).
        current_temp: Current observed temperature at the ASOS station (°F).
        observed_low: Day's lowest temperature observed so far (°F), or None.
        forecast_low: NWS forecast low for today (°F), or None.

    Returns:
        Signal dict: trade_type="NO" if actionable, else trade_type=None + skip_reason.
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
        "market_type":  "lowt",
    }

    # --- Gate: Market must be active ---
    if bracket.get("status") not in (None, "active"):
        signal["skip_reason"] = f"Market not active (status={bracket.get('status')})"
        return signal

    cap   = bracket.get("cap")
    floor = bracket.get("floor")

    # --- Gate: B-brackets only (low BELOW X°F) ---
    # B-bracket: has a cap (the threshold), may also have a floor.
    # T-bracket: floor set, cap=None ("low above X°F") — unbounded downside risk, banned.
    if cap is None:
        signal["skip_reason"] = "T-bracket (low above X°F) — banned, asymmetric downside risk"
        return signal

    # --- Gate: Liquidity ---
    spread   = bracket.get("ob_spread")
    no_depth = bracket.get("ob_no_depth") or 0

    if spread is None or spread > MAX_SPREAD:
        signal["skip_reason"] = f"Spread too wide or missing ({spread})"
        return signal

    if no_depth < MIN_DEPTH_LOWT:
        signal["skip_reason"] = f"Insufficient depth (no_depth={no_depth} < {MIN_DEPTH_LOWT})"
        return signal

    # --- Signal scoring ---
    score   = 0
    details = []

    # Best available temperature reference: take the warmest of current temp and observed low.
    # For LOWT: observed_low is the day's minimum so far. If it's already well above the cap,
    # the bracket cannot resolve YES. current_temp is used when observed_low is unavailable
    # (e.g. early in the day before the daily min is computed by NWS).
    best_obs = None
    if current_temp is not None and observed_low is not None:
        best_obs = max(current_temp, observed_low)
    elif current_temp is not None:
        best_obs = current_temp
    elif observed_low is not None:
        best_obs = observed_low

    if best_obs is not None and (best_obs - cap) >= OBS_ELIM_MARGIN:
        score += 1
        details.append("obs_eliminates_bracket")

    if forecast_low is not None and (forecast_low - cap) >= FORECAST_LOW_MARGIN:
        score += 1
        details.append("tonight_fcst_well_clear")

    candles = bracket.get("candles", [])
    if not _score_momentum(candles):
        score += 1
        details.append("momentum_flat_or_down")

    signal["score"]        = score
    signal["score_detail"] = details

    # --- Gate: At least one positive signal before price check ---
    # Momentum alone doesn't justify entry — need obs or forecast confirmation.
    has_positive_signal = (
        "obs_eliminates_bracket" in details or "tonight_fcst_well_clear" in details
    )
    if not has_positive_signal:
        signal["skip_reason"] = (
            f"No elimination signal: "
            f"best_obs={best_obs}°F, cap={cap}°F, margin={OBS_ELIM_MARGIN}°F required; "
            f"tonight_fcst_low={forecast_low}°F, margin={FORECAST_LOW_MARGIN}°F required"
        )
        return signal

    # --- Price gates ---
    yes_ask = bracket.get("ob_yes_ask")
    no_ask  = bracket.get("ob_no_ask")

    if no_ask is None or yes_ask is None:
        signal["skip_reason"] = "Missing orderbook prices"
        return signal

    if not (NO_MIN_ENTRY_PRICE <= no_ask <= NO_MAX_ENTRY_PRICE):
        signal["skip_reason"] = (
            f"NO price ${no_ask:.2f} outside entry range "
            f"[${NO_MIN_ENTRY_PRICE}–${NO_MAX_ENTRY_PRICE}]"
        )
        return signal

    if not (NO_MIN_YES_PRICE < yes_ask <= NO_MAX_YES_PRICE):
        signal["skip_reason"] = (
            f"YES price ${yes_ask:.2f} outside allowed range "
            f"(${NO_MIN_YES_PRICE}–${NO_MAX_YES_PRICE}]"
        )
        return signal

    # All gates passed — actionable signal
    signal["trade_type"]    = "NO"
    signal["entry_price"]   = no_ask
    signal["stop_loss"]     = None
    signal["max_contracts"] = MAX_CONTRACTS

    return signal


# ---------------------------------------------------------------------------
# Per-city evaluator
# ---------------------------------------------------------------------------

def evaluate_city_lowt(
    city:      str,
    nws_data:  dict,
    scan_data: dict,
) -> dict:
    """
    Full LOWT evaluation for one city.
    Returns structured result with all bracket signals.
    """
    result = {
        "city":         city,
        "market_type":  "lowt",
        "evaluated_at": datetime.now(timezone.utc).isoformat(),
        "nws_snapshot": {
            "current_temp_f":  nws_data.get("current_temp_f"),
            "observed_low_f":  nws_data.get("observed_low_f"),
            "forecast_low_f":  nws_data.get("forecast_low_f"),
            "local_time":      nws_data.get("local_time"),
            "city_local_hour": nws_data.get("city_local_hour"),
        },
        "signals": [],
        "error":   None,
    }

    if city not in LOWT_CITIES:
        result["error"] = f"City not in LOWT pilot list ({sorted(LOWT_CITIES)})"
        return result

    if nws_data.get("error"):
        result["error"] = f"NWS error: {nws_data['error']}"
        return result

    if scan_data.get("error"):
        result["error"] = f"Kalshi error: {scan_data['error']}"
        return result

    current_temp = nws_data.get("current_temp_f")
    observed_low = nws_data.get("observed_low_f")
    forecast_low = nws_data.get("forecast_low_f")
    brackets     = scan_data.get("brackets", [])

    if current_temp is None and observed_low is None:
        result["error"] = "No temperature data available"
        return result

    for bracket in brackets:
        signal = evaluate_bracket_lowt(
            bracket      = bracket,
            current_temp = current_temp,
            observed_low = observed_low,
            forecast_low = forecast_low,
        )
        result["signals"].append(signal)

    # Limit to MAX_NO_PER_CITY, keeping those with the largest obs margin
    no_signals = [s for s in result["signals"] if s.get("trade_type") == "NO"]
    if len(no_signals) > MAX_NO_PER_CITY:
        def obs_margin(signal):
            cap      = signal.get("cap")
            best_obs = max(
                x for x in [current_temp, observed_low] if x is not None
            ) if any(x is not None for x in [current_temp, observed_low]) else 0
            return (best_obs - cap) if cap is not None else 0

        no_signals.sort(key=obs_margin, reverse=True)
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
    """
    Run the LOWT decision engine for all pilot cities (or one if city_filter set).
    Returns evaluations list. Signals with trade_type="NO" are placed as orders
    unless paper=True.
    """
    print("Fetching NWS live data (LOWT)...")
    nws_results = nws_feed.snapshot(city_filter)

    print("Scanning Kalshi LOWT markets...")
    kalshi_results = kalshi_scanner.scan_all(city_filter, market_type="low")

    print("\nEvaluating LOWT signals...\n")
    evaluations = []

    cities = list(LOWT_CITIES)
    if city_filter:
        cities = [c for c in cities if c.lower() == city_filter.lower()]

    for city in cities:
        nws_data  = nws_results.get(city, {})
        scan_data = kalshi_results.get(city, {})
        result    = evaluate_city_lowt(city, nws_data, scan_data)
        evaluations.append(result)

    return evaluations


# ---------------------------------------------------------------------------
# Display
# ---------------------------------------------------------------------------

def display(evaluations: list[dict]):
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    print(f"\n{'='*72}")
    print(f"  LOWT Decision Engine  —  {now_utc}")
    print(f"  Pilot cities: {sorted(LOWT_CITIES)}")
    print(f"  OBS_ELIM_MARGIN={OBS_ELIM_MARGIN}°F  FORECAST_LOW_MARGIN={FORECAST_LOW_MARGIN}°F  MIN_DEPTH={MIN_DEPTH_LOWT}")
    print(f"{'='*72}")

    any_signal = False

    for ev in evaluations:
        city = ev["city"]

        if ev.get("error"):
            print(f"\n{city}: SKIP — {ev['error']}")
            continue

        snap = ev["nws_snapshot"]
        fmt  = lambda v: f"{v:.1f}" if v is not None else "N/A"

        print(
            f"\n{city}  |  local: {snap.get('local_time','?')}  "
            f"curr: {fmt(snap.get('current_temp_f'))}°  "
            f"obs_lo: {fmt(snap.get('observed_low_f'))}°  "
            f"fcst_lo: {fmt(snap.get('forecast_low_f'))}°"
        )

        active_signals = [s for s in ev["signals"] if s.get("trade_type")]
        skipped        = [s for s in ev["signals"] if not s.get("trade_type")]

        if not active_signals:
            skip_reasons = set(s.get("skip_reason") or "no signal" for s in skipped)
            # Only show the most relevant skip reason (not all the depth/spread failures)
            interesting = [r for r in skip_reasons
                           if r and "Insufficient depth" not in r and "Spread" not in r
                           and "T-bracket" not in r and "active" not in r]
            summary = "; ".join(sorted(interesting)[:3]) if interesting else "no tradeable brackets"
            print(f"  No signals — {summary}")
            continue

        any_signal = True
        print(f"  {'Bracket':<20} {'Type':>5} {'Entry':>7} {'Score':>6}  Details")
        print(f"  {'-'*62}")

        for s in active_signals:
            cap   = s.get("cap")
            floor = s.get("floor")
            if floor is not None and cap is not None:
                bracket_str = f"{floor}–{cap}°F"
            elif cap is not None:
                bracket_str = f"<{cap}°F"
            else:
                bracket_str = "?"

            detail_str = ", ".join(s.get("score_detail", []))
            print(
                f"  {bracket_str:<20} "
                f"{s['trade_type']:>5} "
                f"${s['entry_price']:.2f}  "
                f"{s['score']}/3    "
                f"{detail_str}"
            )

    print(f"\n{'='*72}")
    if not any_signal:
        print("  No actionable LOWT signals at this time.")


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kalshi LOWT weather trading decision engine")
    parser.add_argument("--city",  type=str,        default=None, help="Filter to one city")
    parser.add_argument("--paper", action="store_true",           help="Paper trade mode (log only)")
    args = parser.parse_args()

    evaluations = run(city_filter=args.city, paper=args.paper)
    display(evaluations)

    if args.paper:
        out = Path("data/paper_trades_lowt.json")
        out.parent.mkdir(parents=True, exist_ok=True)
        existing = json.loads(out.read_text()) if out.exists() else []
        existing.extend(evaluations)
        out.write_text(json.dumps(existing, indent=2, default=str))
        print(f"\n  LOWT paper trade log saved to {out}")
