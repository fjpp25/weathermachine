"""
cascade_engine.py
-----------------
Additive cascade signal module for Kalshi HIGH temperature markets.

Generates NO trade signals based purely on market price confirmation —
no NWS forecast or observation data required. Works alongside the main
hight_decision_engine.py as a separate signal path.

CONVERGENCE TIER  (any hour, bottom-up only)
─────────────────────────────────────────────
Mechanism:
  Temperature markets follow a roughly normal distribution centred on the
  NWS forecast. As the day progresses, brackets successively confirm No
  from the bottom up (temperature climbing). Each confirmation is a
  Bayesian update — the remaining probability mass shifts away from
  confirmed brackets.

  Bottom-up only for spring/summer (rising temperatures). Top-down
  (autumn/winter) can be added when seasonal data supports it.

Trigger:
  When any B bracket crosses No >= CONV_THRESHOLD (0.97), the bracket
  immediately above it becomes the target. The first trigger locks the
  direction for that city-day.

Entry gates:
  - No price in [0.60, 0.90]
  - Skip the forecast bracket (bracket containing corrected NWS forecast)
  - No new cascades after START_HOUR_CAP (15:00 local)
  - Max 1 entry if trigger fires after LATE_HOUR (13:00 local)
  - Max 2 entries default; 3 if top-T confirms before 13:00

Contract sizing (scales with entry price / conviction):
  - No 0.60–0.70: 2 contracts
  - No 0.71–0.80: 4 contracts
  - No 0.81–0.90: 6 contracts

Backtest (Apr 6–23, 18 days, 1 bad day):
  108 signals  WR=86.1%  EV=+$0.065/signal
  Scaled PnL (2/4/6 contracts): ~$40 over 18 days
"""

from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from cities import CITIES as _CITY_REGISTRY

# ---------------------------------------------------------------------------
# Parameters
# ---------------------------------------------------------------------------

CONV_THRESHOLD       = 0.97
NO_MIN_ENTRY         = 0.60
NO_MAX_ENTRY         = 0.90
START_HOUR_CAP       = 15
LATE_HOUR            = 13
MAX_ENTRIES_DEFAULT  = 2
MAX_ENTRIES_EXTENDED = 3
MAX_ENTRIES_LATE     = 1

CONTRACT_TIERS = [
    (0.60, 0.70, 2),
    (0.71, 0.80, 4),
    (0.81, 0.90, 6),
]

# Afternoon tier parameters (retained)
CONFIRM_THRESHOLD = 0.98
AFTERNOON_START   = 14
AFTERNOON_END     = 15
AFTERNOON_MAX_YES = 0.15

# ---------------------------------------------------------------------------
# Session state
# ---------------------------------------------------------------------------

_direction_locked:    dict[tuple[str, str], str]  = {}
_entries_made:        dict[tuple[str, str], int]  = {}
_trigger_hour:        dict[tuple[str, str], int]  = {}
_cascade_entered:     set[str]                    = set()
_afternoon_triggered: dict[tuple[str, str, str], bool] = {}


def _market_date(ticker: str) -> str:
    try:
        return ticker.split("-")[1]
    except (IndexError, AttributeError):
        return ""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _local_hour(city: str) -> int:
    tz_name = _CITY_REGISTRY.get(city, {}).get("tz")
    if tz_name:
        return datetime.now(ZoneInfo(tz_name)).hour
    return datetime.now(timezone.utc).hour


def _is_paused(city: str) -> bool:
    return not _CITY_REGISTRY.get(city, {}).get("trading", True)


def _no_price(bracket: dict) -> float:
    return (
        bracket.get("ob_no_bid") or
        bracket.get("ob_no_ask") or
        bracket.get("no_ask")    or
        bracket.get("no_bid")    or
        bracket.get("no_price")  or
        0.0
    )


def _yes_price(bracket: dict) -> float:
    return (
        bracket.get("ob_yes_ask") or
        bracket.get("yes_ask")    or
        bracket.get("yes_price")  or
        0.0
    )


def _contracts_for(no_price: float) -> int:
    for lo, hi, contracts in CONTRACT_TIERS:
        if lo <= no_price <= hi:
            return contracts
    return 2


def _corrected_forecast(city: str, nws_data: dict) -> float | None:
    fcst = nws_data.get("forecast_high_f")
    if fcst is None:
        return None
    try:
        from hight_decision_engine import _city_bias as _get_bias
        return fcst + _get_bias(city)
    except Exception:
        return fcst


def _forecast_bracket_idx(corrected_fcst: float | None,
                           b_sorted: list[dict]) -> int | None:
    if corrected_fcst is None:
        return None
    n = len(b_sorted)
    for i, b in enumerate(b_sorted):
        floor = b.get("floor", 0)
        cap   = b.get("cap", 999)
        if floor <= corrected_fcst <= cap:
            return i
    return n - 1 if corrected_fcst > (b_sorted[-1].get("cap") or 999) else 0


def _make_signal(bracket: dict, tier: str, trigger_info: str,
                 contracts: int) -> dict:
    no_ask = _no_price(bracket)
    return {
        "ticker":        bracket["ticker"],
        "title":         bracket.get("title", ""),
        "floor":         bracket.get("floor"),
        "cap":           bracket.get("cap"),
        "yes_ask":       _yes_price(bracket),
        "no_ask":        no_ask,
        "yes_bid":       bracket.get("ob_yes_bid"),
        "no_bid":        bracket.get("ob_no_bid"),
        "spread":        bracket.get("ob_spread"),
        "yes_depth":     bracket.get("ob_yes_depth"),
        "no_depth":      bracket.get("ob_no_depth"),
        "volume":        bracket.get("volume"),
        "score":         0,
        "score_detail":  [],
        "trade_type":    "NO",
        "entry_price":   no_ask,
        "entry_tier":    tier,
        "trigger_info":  trigger_info,
        "max_contracts": contracts,
        "skip_reason":   None,
    }


# ---------------------------------------------------------------------------
# Convergence scanner
# ---------------------------------------------------------------------------

def _convergence_signals(city: str, brackets: list[dict],
                         nws_data: dict) -> list[dict]:
    if not brackets or len(brackets) < 3:
        return []

    sample_ticker = next((b["ticker"] for b in brackets if b.get("ticker")), "")
    market_date   = _market_date(sample_ticker)
    city_key      = (city, market_date)
    local_hour    = _local_hour(city)

    locked_dir   = _direction_locked.get(city_key)
    entries_made = _entries_made.get(city_key, 0)
    trig_hour    = _trigger_hour.get(city_key)

    # No new cascade starts at or after START_HOUR_CAP
    if locked_dir is None and local_hour >= START_HOUR_CAP:
        return []

    # Determine max entries
    if trig_hour is not None:
        if trig_hour >= LATE_HOUR:
            max_e = MAX_ENTRIES_LATE
        else:
            top_t_confirmed = any(
                b.get("floor") is not None and b.get("cap") is None and
                _no_price(b) >= CONV_THRESHOLD
                for b in brackets
            )
            max_e = MAX_ENTRIES_EXTENDED if top_t_confirmed else MAX_ENTRIES_DEFAULT
    else:
        max_e = MAX_ENTRIES_DEFAULT

    if entries_made >= max_e:
        return []

    # Sort B brackets by floor low→high
    b_brackets = sorted(
        [b for b in brackets
         if b.get("floor") is not None and b.get("cap") is not None],
        key=lambda b: b["floor"]
    )
    if len(b_brackets) < 2:
        return []

    cf       = _corrected_forecast(city, nws_data)
    fcst_idx = _forecast_bracket_idx(cf, b_brackets)

    signals = []

    for i, confirmed_b in enumerate(b_brackets):
        if _no_price(confirmed_b) < CONV_THRESHOLD:
            continue
        if i >= len(b_brackets) - 1:
            continue

        target = b_brackets[i + 1]
        ticker = target.get("ticker", "")

        if not ticker or ticker in _cascade_entered:
            continue
        if fcst_idx is not None and i + 1 == fcst_idx:
            continue

        no_ask  = _no_price(target)
        yes_ask = _yes_price(target)

        if not (NO_MIN_ENTRY <= no_ask <= NO_MAX_ENTRY):
            continue

        # Lock direction and record trigger
        if locked_dir is None:
            _direction_locked[city_key] = 'up'
            _trigger_hour[city_key]     = local_hour
            trig_hour = local_hour

        _cascade_entered.add(ticker)
        _entries_made[city_key] = entries_made + 1
        entries_made += 1

        contracts    = _contracts_for(no_ask)
        trigger_info = (
            f"bottom-up: {confirmed_b['ticker'].split('-')[-1]} "
            f"No={_no_price(confirmed_b):.2f} → "
            f"{ticker.split('-')[-1]} No={no_ask:.2f} "
            f"YES={yes_ask:.2f}  {contracts}c"
        )

        signals.append(_make_signal(target, "cascade_directional_up",
                                    trigger_info, contracts))

        if entries_made >= max_e:
            break

    return signals


# ---------------------------------------------------------------------------
# Afternoon tier
# ---------------------------------------------------------------------------

def _afternoon_signal(city: str, brackets: list[dict],
                      local_hour: int) -> list[dict]:
    if not (AFTERNOON_START <= local_hour <= AFTERNOON_END):
        return []

    sample_ticker = next((b["ticker"] for b in brackets if b.get("ticker")), "")
    market_date   = _market_date(sample_ticker)

    all_floors = sorted(
        set(b["floor"] for b in brackets if b.get("floor") is not None)
    )
    if len(all_floors) < 4:
        return []

    mid_idx = len(all_floors) // 2
    confirmed_low, confirmed_high = [], []

    for b in brackets:
        floor  = b.get("floor")
        no_bid = _no_price(b)
        if floor is None or no_bid < CONFIRM_THRESHOLD:
            continue
        if floor in all_floors:
            if all_floors.index(floor) < mid_idx:
                confirmed_low.append(floor)
            else:
                confirmed_high.append(floor)

    n_low, n_high = len(confirmed_low), len(confirmed_high)
    if n_low == n_high or (n_low == 0 and n_high == 0):
        return []

    signals = []
    for direction, n_dom, n_other in [("low", n_low, n_high),
                                       ("high", n_high, n_low)]:
        if n_dom <= n_other:
            continue

        city_dir_key = (city, market_date, direction)
        if _afternoon_triggered.get(city_dir_key):
            continue

        if direction == "low":
            candidates = [
                f for f in all_floors
                if f not in confirmed_low and f not in confirmed_high
                and all_floors.index(f) < mid_idx + 1
            ]
        else:
            candidates = [
                f for f in reversed(all_floors)
                if f not in confirmed_low and f not in confirmed_high
                and all_floors.index(f) >= mid_idx - 1
            ]

        if not candidates:
            continue

        target = next(
            (b for b in brackets
             if b.get("floor") is not None
             and abs(b["floor"] - candidates[0]) < 0.1),
            None
        )
        if target is None:
            continue

        yes_ask = _yes_price(target)
        no_ask  = _no_price(target)

        if not (0.01 < yes_ask <= AFTERNOON_MAX_YES):
            continue
        if no_ask > NO_MAX_ENTRY or no_ask <= 0.0:
            continue

        _afternoon_triggered[city_dir_key] = True
        trigger_info = (
            f"{direction}-side dominant ({n_dom} confirmed vs {n_other}) "
            f"-> B{candidates[0]:.1f} YES={yes_ask:.2f}"
        )
        signals.append(_make_signal(target, "cascade_afternoon",
                                    trigger_info, _contracts_for(no_ask)))

    return signals


# ---------------------------------------------------------------------------
# Per-city evaluator
# ---------------------------------------------------------------------------

def evaluate_city_cascade(city: str, scan_data: dict) -> dict:
    result = {
        "city":         city,
        "evaluated_at": datetime.now(timezone.utc).isoformat(),
        "signals":      [],
        "error":        None,
        "cascade":      True,
    }

    if _is_paused(city):
        result["error"] = "City paused — cascade skipped"
        return result

    if scan_data.get("error"):
        result["error"] = f"Kalshi error: {scan_data['error']}"
        return result

    brackets   = scan_data.get("brackets", [])
    nws_data   = scan_data.get("nws_data", {})
    local_hour = _local_hour(city)

    # Convergence scanner — today
    result["signals"].extend(_convergence_signals(city, brackets, nws_data))

    # Convergence scanner — tomorrow (when today fully converged)
    tomorrow_brackets = scan_data.get("tomorrow_brackets", [])
    if tomorrow_brackets and scan_data.get("today_converged"):
        tmr_sigs = _convergence_signals(city, tomorrow_brackets, nws_data)
        for s in tmr_sigs:
            s["trigger_info"] = "[TOMORROW] " + s.get("trigger_info", "")
            s["entry_tier"]   = "cascade_tomorrow"
        result["signals"].extend(tmr_sigs)

    # Afternoon tier
    result["signals"].extend(_afternoon_signal(city, brackets, local_hour))

    return result


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run(kalshi_results: dict, city_filter: str = None) -> list[dict]:
    evaluations = []
    for city, scan_data in kalshi_results.items():
        if city_filter and city.lower() != city_filter.lower():
            continue
        evaluations.append(evaluate_city_cascade(city, scan_data))
    return evaluations


# ---------------------------------------------------------------------------
# Display
# ---------------------------------------------------------------------------

def display(evaluations: list[dict]):
    cascade_evals = [e for e in evaluations if e.get("cascade")]
    if not cascade_evals:
        return

    any_signal = False
    print(f"\n{'─'*72}")
    print(f"  Cascade Engine")
    print(f"{'─'*72}")

    for ev in cascade_evals:
        signals = [s for s in ev.get("signals", []) if s.get("trade_type")]
        if not signals:
            continue
        any_signal = True
        for s in signals:
            floor = s.get("floor")
            cap   = s.get("cap")
            bracket_str = (
                f"{floor}-{cap}F" if floor and cap
                else f">{floor}F" if floor
                else f"<{cap}F"
            )
            tier = s.get("entry_tier", "")
            tier_label = (
                "DIRECTIONAL-UP" if "directional_up" in tier else
                "TOMORROW"       if "tomorrow"       in tier else
                "AFTERNOON"      if "afternoon"      in tier else
                tier.upper()
            )
            print(
                f"  {ev['city']:<16} [{tier_label}]  "
                f"{bracket_str:<14}  NO  "
                f"${s['entry_price']:.2f}  "
                f"max={s['max_contracts']}c  "
                f"{s['trigger_info']}"
            )

    if not any_signal:
        print("  No cascade signals at this time.")
