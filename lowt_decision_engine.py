"""
lowt_decision_engine.py
-----------------------
LOWT signal engine — two complementary entry paths.

Signal A: Structural Elimination (primary, any hour)
-----------------------------------------------------
The observed low for the day is already ABOVE the bracket cap, meaning
the temperature has not reached that bracket and is now warming. It is
physically impossible for the bracket to resolve Yes — the overnight low
does not fall, rise, and fall again.

Backtest (Apr 6 – Jun 24 2026, all cities, resolved tickers):
  1,914 signals  |  99.9% WR  |  Avg No 0.884  |  EV +$0.116/contract
  1 loss (San Francisco, Apr 18, hour 17)

Signal B: Forecast Distance (secondary, evening only)
------------------------------------------------------
For brackets not yet physically eliminated, the NWS forecast says the
overnight low won't reach them. Evening window only (18–23h local) when
the market is most actively pricing in tonight's low.

  - B bracket only
  - Skip the forecast bracket (highest Yes price = most contested)
  - Bracket floor must be >= forecast_low - 3°F (no deep cold brackets)
  - Skip Philadelphia and Chicago (historically weaker WR)

Backtest (evening, resolved, B non-forecast, dist >= -3°F):
  590 resolved signals  |  99.8% WR  |  Avg No 0.837  |  EV +$0.161/contract

Signal interaction
------------------
Signal A is checked first every poll. If a bracket qualifies for A it
is entered and marked fired. Signal B only fires on brackets that have
NOT already triggered A. Session dedup prevents double-entry as brackets
migrate from B-eligible to A-eligible during the night.

Capital
-------
Drawn from the 'lowt' engine allocation. Flat sizing: MAX_CONTRACTS per
entry. Per-city cap: MAX_NO_PER_CITY open positions.

Integration
-----------
run() returns the same evaluation list structure as the old engine.
trader.py's run_pipeline() consumes it unchanged.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from typing import Optional

from cities import CITIES as _CITIES
from log_setup import get_logger
from market_utils import (
    local_hour as _local_hour,
    no_price   as _no_price,
    yes_price  as _yes_price,
    load_config_env,
)

log = get_logger(__name__)

# ---------------------------------------------------------------------------
# Parameters
# ---------------------------------------------------------------------------

# Signal A — structural elimination
A_NO_MIN        = 0.75   # minimum No price to enter
A_NO_MAX        = 0.97   # maximum No price (above this, payout too thin)
A_CAP_BUFFER    = 2.0    # bracket width assumption (°F); obs_low must exceed bval + this

# Signal B — forecast distance, evening only
B_NO_MIN        = 0.75
B_NO_MAX        = 0.92
B_EVENING_START = 18
B_EVENING_END   = 23
B_DIST_MIN      = -3.0   # bracket floor must be >= forecast_low + this
B_SKIP_CITIES   = frozenset({"Philadelphia", "Chicago"})

# Shared
MAX_CONTRACTS   = 2      # flat sizing per entry — conservative restart
MAX_NO_PER_CITY = 2      # max open LOWT positions per city

# ---------------------------------------------------------------------------
# Session state
# ---------------------------------------------------------------------------

_fired: set[str] = set()   # tickers entered this session

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _bval(bracket: dict) -> Optional[float]:
    """Extract numeric temperature value from bracket code or ticker suffix."""
    for src in [
        bracket.get("bracket", ""),
        bracket.get("ticker", "").split("-")[-1],
    ]:
        if src and src[0] in "BT":
            try:
                return float(src[1:])
            except ValueError:
                pass
    cap = bracket.get("cap")
    return float(cap) if cap is not None else None


def _btype(bracket: dict) -> str:
    for src in [
        bracket.get("bracket", ""),
        bracket.get("ticker", "").split("-")[-1],
    ]:
        if src and src[0] in "BT":
            return src[0]
    return ""


def _forecast_bracket(b_brackets: list[dict]) -> Optional[dict]:
    """Return the B bracket with the highest Yes price (market's forecast)."""
    if not b_brackets:
        return None
    return max(b_brackets, key=_yes_price)


def _bracket_cap(bracket: dict) -> Optional[float]:
    """
    Return the cap (upper temperature boundary) of a B bracket.
    Uses the Kalshi API cap field when available; falls back to bval + A_CAP_BUFFER.
    """
    cap = bracket.get("cap")
    if cap is not None:
        try:
            return float(cap)
        except (ValueError, TypeError):
            pass
    val = _bval(bracket)
    return (val + A_CAP_BUFFER) if val is not None else None


# ---------------------------------------------------------------------------
# Signal A evaluation
# ---------------------------------------------------------------------------

def _check_signal_a(
    bracket:   dict,
    obs_low_f: float,
) -> bool:
    """
    Return True if this bracket qualifies for Signal A.

    Condition: observed low is already above the bracket cap.
    The temperature has not reached this bracket — physically cannot resolve Yes.
    """
    cap = _bracket_cap(bracket)
    if cap is None:
        return False
    return obs_low_f > cap


# ---------------------------------------------------------------------------
# Signal B evaluation
# ---------------------------------------------------------------------------

def _check_signal_b(
    bracket:      dict,
    city:         str,
    local_hour:   int,
    fcst_low_f:   Optional[float],
    fcst_bracket: Optional[dict],
    obs_low_f:    Optional[float],
) -> tuple[bool, str]:
    """
    Return (qualifies, skip_reason) for Signal B.

    Gates:
      - Evening window
      - B bracket only
      - Not the forecast bracket
      - Not already physically eliminated (Signal A would cover it)
      - Distance from forecast >= B_DIST_MIN
      - Not a skip city
      - No price in [B_NO_MIN, B_NO_MAX)
    """
    if city in B_SKIP_CITIES:
        return False, f"skip city ({city})"

    if not (B_EVENING_START <= local_hour <= B_EVENING_END):
        return False, f"outside evening window (hour={local_hour})"

    if fcst_bracket and bracket.get("ticker") == fcst_bracket.get("ticker"):
        return False, "forecast bracket — skip"

    # Don't double-count brackets Signal A would handle
    if obs_low_f is not None:
        cap = _bracket_cap(bracket)
        if cap is not None and obs_low_f > cap:
            return False, "already obs-eliminated (Signal A)"

    if fcst_low_f is not None:
        val = _bval(bracket)
        if val is not None:
            dist = val - fcst_low_f
            if dist < B_DIST_MIN:
                return False, f"too close to forecast (dist={dist:.1f}°F < {B_DIST_MIN})"

    return True, ""


# ---------------------------------------------------------------------------
# City evaluation
# ---------------------------------------------------------------------------

def evaluate_city_lowt(
    city:      str,
    scan_data: dict,
    nws_data:  dict = None,
) -> dict:
    """
    Evaluate one city's LOWT market for Signal A and Signal B entries.
    Returns an evaluation dict compatible with trader.py's run_pipeline.
    """
    result = {
        "city":         city,
        "market_type":  "lowt",
        "evaluated_at": datetime.now(timezone.utc).isoformat(),
        "signals":      [],
        "error":        None,
        "window":       None,
    }

    if scan_data.get("error"):
        result["error"] = f"Kalshi error: {scan_data['error']}"
        return result

    brackets = scan_data.get("brackets", [])
    if not brackets:
        result["error"] = "No brackets"
        return result

    b_brackets = sorted(
        [b for b in brackets if _btype(b) == "B" and _bval(b) is not None],
        key=_bval,
    )
    if not b_brackets:
        result["error"] = "No B brackets"
        return result

    local_hr    = _local_hour(city)
    nws         = nws_data or {}
    obs_low_f   = nws.get("observed_low_f")
    fcst_low_f  = nws.get("forecast_low_f")
    fcst_br     = _forecast_bracket(b_brackets)

    # Determine window label for logging
    if B_EVENING_START <= local_hr <= B_EVENING_END:
        window = "evening"
    elif 0 <= local_hr <= 8:
        window = "overnight"
    else:
        window = "day"
    result["window"] = window

    for bracket in b_brackets:
        ticker = bracket.get("ticker", "")
        if not ticker:
            continue

        no_p  = _no_price(bracket)
        yes_p = _yes_price(bracket)
        val   = _bval(bracket)

        # ── Signal A: structural elimination ─────────────────────────────
        signal_type = None
        skip_reason = None

        if obs_low_f is not None and no_p >= A_NO_MIN and no_p < A_NO_MAX:
            if _check_signal_a(bracket, obs_low_f):
                signal_type = "A"

        # ── Signal B: forecast distance (if A didn't fire) ────────────────
        if signal_type is None and B_NO_MIN <= no_p < B_NO_MAX:
            b_qual, b_reason = _check_signal_b(
                bracket, city, local_hr, fcst_low_f, fcst_br, obs_low_f
            )
            if b_qual:
                signal_type = "B"
            else:
                skip_reason = b_reason

        # ── Price gate (shared) ───────────────────────────────────────────
        if signal_type == "A" and not (A_NO_MIN <= no_p < A_NO_MAX):
            signal_type = None
            skip_reason = f"No price out of gate (no={no_p:.2f})"
        elif signal_type == "B" and not (B_NO_MIN <= no_p < B_NO_MAX):
            signal_type = None
            skip_reason = f"No price out of gate (no={no_p:.2f})"

        cap = _bracket_cap(bracket)
        obs_gap = (obs_low_f - val) if obs_low_f is not None and val is not None else None
        dist    = (val - fcst_low_f) if fcst_low_f is not None and val is not None else None

        signal = {
            "ticker":       ticker,
            "title":        bracket.get("title", ""),
            "floor":        bracket.get("floor"),
            "cap":          bracket.get("cap"),
            "yes_ask":      yes_p,
            "no_ask":       no_p,
            "spread":       bracket.get("ob_spread"),
            "yes_depth":    bracket.get("ob_yes_depth"),
            "no_depth":     bracket.get("ob_no_depth"),
            "volume":       bracket.get("volume"),
            "score":        0,
            "score_detail": [],
            "trade_type":   None,
            "entry_price":  no_p,
            "entry_tier":   "lowt_main",
            "market_type":  "lowt",
            "skip_reason":  skip_reason,
            "signal_type":  signal_type,
            "window":       window,
            "obs_gap":      round(obs_gap, 1) if obs_gap is not None else None,
            "dist_fcst":    round(dist, 1)    if dist    is not None else None,
        }

        if signal_type is not None:
            tier   = f"lowt_{signal_type.lower()}"
            detail = [f"signal={signal_type}"]
            if signal_type == "A":
                detail += [
                    f"obs_low={obs_low_f:.1f}°F",
                    f"bracket_cap={cap:.1f}°F",
                    f"obs_gap={obs_gap:.1f}°F",
                ]
            else:
                detail += [
                    f"dist_fcst={dist:.1f}°F",
                    f"window={window}",
                    f"obs_low={obs_low_f:.1f}°F" if obs_low_f else "obs_low=N/A",
                ]

            signal["trade_type"]    = "NO"
            signal["max_contracts"] = MAX_CONTRACTS
            signal["score"]         = 2 if signal_type == "A" else 1
            signal["score_detail"]  = detail
            signal["entry_tier"]    = tier

            log.info(
                "LOWT_%s  %s  %s  No=%.2f  %s",
                signal_type, city, ticker, no_p,
                "  ".join(detail),
            )

        result["signals"].append(signal)

    return result


# ---------------------------------------------------------------------------
# Run — called by trader.py's run_pipeline
# ---------------------------------------------------------------------------

def run(
    kalshi_results: dict,
    city_filter:    str  = None,
    nws_results:    dict = None,
    paper:          bool = False,
) -> list[dict]:
    """
    Evaluate all cities and return a list of evaluation dicts.
    Optionally includes LOWT cascade evaluations.
    """
    evaluations = []
    cities = list(_CITIES.keys())
    if city_filter:
        cities = [c for c in cities if c.lower() == city_filter.lower()]

    nws = nws_results or {}

    for city in cities:
        scan_data = kalshi_results.get(city, {})
        nws_data  = nws.get(city, {})
        result    = evaluate_city_lowt(city, scan_data, nws_data)
        evaluations.append(result)

    # LOWT cascade — kept for future use, non-fatal if unavailable
    try:
        import cascade_engine
        lowt_cascade = cascade_engine.run_lowt(kalshi_results, city_filter)
        evaluations.extend(lowt_cascade)
    except Exception as e:
        log.debug("LOWT cascade skipped (non-fatal): %s", e)

    return evaluations


# ---------------------------------------------------------------------------
# Display — called by trader.py's run_pipeline
# ---------------------------------------------------------------------------

def display(evaluations: list[dict]) -> None:
    main_evals = [
        e for e in evaluations
        if e.get("market_type") == "lowt" and not e.get("cascade")
    ]
    if not main_evals:
        return

    print(f"\n{'─'*70}")
    print(f"  LOWT Decision Engine")
    print(f"{'─'*70}")

    any_signal = False
    for ev in main_evals:
        city    = ev.get("city", "?")
        signals = [s for s in ev.get("signals", []) if s.get("trade_type")]
        for s in signals:
            any_signal  = True
            floor       = s.get("floor")
            cap         = s.get("cap")
            sig_type    = s.get("signal_type", "?")
            obs_gap     = s.get("obs_gap")
            dist_fcst   = s.get("dist_fcst")
            bracket_str = (
                f"{floor:.0f}-{cap:.0f}°F" if floor and cap
                else f"<{cap:.0f}°F"        if cap
                else f">{floor:.0f}°F"      if floor
                else "?"
            )
            extra = (
                f"obs_gap={obs_gap:+.1f}°F" if obs_gap is not None
                else f"dist={dist_fcst:+.1f}°F" if dist_fcst is not None
                else ""
            )
            print(
                f"  {city:<16}  [{sig_type}]  {bracket_str:<12}  "
                f"NO  ${s['no_ask']:.2f}  {extra}"
            )

    if not any_signal:
        print("  No LOWT signals at this time.")


# ---------------------------------------------------------------------------
# Config log
# ---------------------------------------------------------------------------

def log_config() -> None:
    log.info(
        "lowt: Signal A: obs_low>cap  No=[%.2f,%.2f)  | "
        "Signal B: evening %d-%dh  No=[%.2f,%.2f)  dist>=%.1f°F  "
        "skip=%s  | contracts=%d  max_per_city=%d",
        A_NO_MIN, A_NO_MAX,
        B_EVENING_START, B_EVENING_END, B_NO_MIN, B_NO_MAX, B_DIST_MIN,
        sorted(B_SKIP_CITIES),
        MAX_CONTRACTS, MAX_NO_PER_CITY,
    )


# ---------------------------------------------------------------------------
# Standalone entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="LOWT decision engine")
    parser.add_argument("--city",  type=str, default=None)
    parser.add_argument("--paper", action="store_true")
    args = parser.parse_args()

    load_config_env()

    import kalshi_scanner
    import nws_feed
    import trader

    client = trader.make_client()
    log_config()

    print("Scanning Kalshi LOWT markets...")
    kalshi_results = kalshi_scanner.scan_all(city_filter=args.city, market_type="lowt")
    nws_results    = nws_feed.snapshot(city_filter=args.city)

    evaluations = run(
        kalshi_results = kalshi_results,
        city_filter    = args.city,
        nws_results    = nws_results,
        paper          = args.paper,
    )
    display(evaluations)
