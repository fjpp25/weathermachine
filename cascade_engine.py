"""
cascade_engine.py
-----------------
Additive cascade signal module for Kalshi HIGH temperature markets.

Generates NO trade signals based purely on market price confirmation —
no NWS forecast or observation data required. Works alongside the main
hight_decision_engine.py as a separate signal path.

Two tiers:

  MORNING TIER  (07:00–11:59 local)
  ─────────────────────────────────
  Trigger: the bottom T bracket (e.g. "78 or below") hits NO ≥ CONFIRM_THRESHOLD.
  Signal:  enter NO on the adjacent B bracket immediately above it (e.g. "79–80").
  Logic:   T settling at $0.98 means the market is 98% confident the high will
           exceed the T floor. The adjacent B bracket is the next price target.
  Limit:   1 signal per city per day (the trigger fires once).

  AFTERNOON TIER  (14:00–15:59 local)
  ─────────────────────────────────────
  Trigger: count brackets confirmed (NO ≥ CONFIRM_THRESHOLD) on each side of
           the market midpoint. Enter on the next unconfirmed bracket on the
           dominant side (the side with more confirmed brackets).
  Signal:  target bracket must have YES ≤ AFTERNOON_MAX_YES and NO ≥ AFTERNOON_MIN_NO.
  Logic:   by 14:00 the day's high is largely established. Confirmed brackets on
           one side reveal direction; the next unconfirmed bracket is a near-certain
           follow-on.
  Limit:   1 signal per city per day per direction.

Both tiers:
  - Max 1 contract (cascade signals are additive — do not compete with main engine)
  - No score requirement (market confirmation replaces signal scoring)
  - Signals tagged with entry_tier="cascade_morning" or "cascade_afternoon"
  - Skips Seattle (non-converging) and any city in PAUSED_CITIES

Usage (called from hight_decision_engine.run()):
  import cascade_engine
  cascade_evals = cascade_engine.run(kalshi_results, city_filter)
  evaluations.extend(cascade_evals)

Data source: kalshi_results dict already fetched by hight_decision_engine.run()
  — zero additional API calls.
"""

from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from cities import CITIES as _CITY_REGISTRY

# ---------------------------------------------------------------------------
# Parameters
# ---------------------------------------------------------------------------

CONFIRM_THRESHOLD    = 0.98   # NO price at which a bracket is considered confirmed
                               # by the market. 0.98 = 98% confidence. Data shows
                               # dropping below 0.98 degrades win rate sharply.

MORNING_START        = 7      # local hour — cascade morning window opens
MORNING_END          = 11     # local hour — cascade morning window closes (inclusive)

AFTERNOON_START      = 14     # local hour — cascade afternoon window opens
AFTERNOON_END        = 15     # local hour — cascade afternoon window closes (inclusive)

AFTERNOON_MAX_YES    = 0.15   # skip afternoon target if YES above this
                               # at 0.15 the market still prices 15% uncertainty — too noisy
AFTERNOON_MIN_NO     = 0.75   # skip afternoon target if NO below this

CASCADE_MAX_CONTRACTS = 3     # hard cap — cascade is additive, not a replacement

# Cities excluded from cascade signals (non-converging behaviour)
CASCADE_SKIP_CITIES  = {"Seattle", "Las Vegas"}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _local_hour(city: str) -> int:
    tz_name = _CITY_REGISTRY.get(city, {}).get("tz")
    if tz_name:
        return datetime.now(ZoneInfo(tz_name)).hour
    return datetime.now(timezone.utc).hour


def _is_paused(city: str) -> bool:
    meta = _CITY_REGISTRY.get(city, {})
    return not meta.get("trading", True)


def _bracket_side(floor: float, all_floors: list[float]) -> str:
    """Return 'low' or 'high' based on position relative to midpoint."""
    mid = len(all_floors) // 2
    return "low" if all_floors.index(floor) < mid else "high"


def _make_signal(bracket: dict, tier: str, trigger_info: str) -> dict:
    """Build a standardised cascade signal dict."""
    floor = bracket.get("floor")
    cap   = bracket.get("cap")
    return {
        "ticker":         bracket["ticker"],
        "title":          bracket.get("title", ""),
        "floor":          floor,
        "cap":            cap,
        "yes_ask":        bracket.get("ob_yes_ask"),
        "no_ask":         bracket.get("ob_no_ask"),
        "yes_bid":        bracket.get("ob_yes_bid"),
        "no_bid":         bracket.get("ob_no_bid"),
        "spread":         bracket.get("ob_spread"),
        "yes_depth":      bracket.get("ob_yes_depth"),
        "no_depth":       bracket.get("ob_no_depth"),
        "volume":         bracket.get("volume"),
        "score":          0,
        "score_detail":   [],
        "trade_type":     "NO",
        "entry_price":    bracket.get("ob_no_ask"),
        "entry_tier":     tier,
        "trigger_info":   trigger_info,
        "max_contracts":  CASCADE_MAX_CONTRACTS,
        "skip_reason":    None,
    }


# ---------------------------------------------------------------------------
# Morning tier
# ---------------------------------------------------------------------------

def _morning_signal(city: str, brackets: list[dict], local_hour: int) -> dict | None:
    """
    Check for a morning cascade signal.

    Finds the bottom T bracket. If its NO bid >= CONFIRM_THRESHOLD, returns
    a NO signal on the adjacent B bracket immediately above it.

    Returns None if no signal qualifies.
    """
    if not (MORNING_START <= local_hour <= MORNING_END):
        return None

    t_brackets = [b for b in brackets if b.get("cap") is None and b.get("floor") is not None]
    b_brackets = [b for b in brackets if b.get("cap") is not None and b.get("floor") is not None]

    if not t_brackets or not b_brackets:
        return None

    # Bottom T = lowest floor among T brackets
    bottom_t = min(t_brackets, key=lambda b: b["floor"])
    t_no_bid = bottom_t.get("ob_no_bid") or bottom_t.get("no_bid") or 0.0

    if t_no_bid < CONFIRM_THRESHOLD:
        return None

    # Adjacent B bracket: floor = bottom_t floor + 0.5
    adj_floor = bottom_t["floor"] + 0.5
    adj = next((b for b in b_brackets if abs(b["floor"] - adj_floor) < 0.1), None)
    if adj is None:
        return None

    yes_ask = adj.get("ob_yes_ask") or adj.get("yes_ask") or 0.0
    no_ask  = adj.get("ob_no_ask")  or adj.get("no_ask")  or 0.0

    if not (0.01 < yes_ask <= 0.25):
        return None
    if no_ask < AFTERNOON_MIN_NO:
        return None
    if no_ask is None or no_ask == 0:
        return None

    trigger_info = (f"bottom T{bottom_t['floor']:.0f} NO={t_no_bid:.2f} "
                    f"→ B{adj_floor:.1f} YES={yes_ask:.2f}")
    return _make_signal(adj, "cascade_morning", trigger_info)


# ---------------------------------------------------------------------------
# Afternoon tier
# ---------------------------------------------------------------------------

def _afternoon_signal(city: str, brackets: list[dict], local_hour: int) -> list[dict]:
    """
    Check for afternoon cascade signals.

    Counts confirmed brackets (NO >= CONFIRM_THRESHOLD) on each side of
    the market. If one side has more confirmed brackets, returns a NO signal
    on the next unconfirmed bracket on the dominant side.

    Returns list of signals (0, 1 or 2 — one per direction at most).
    """
    if not (AFTERNOON_START <= local_hour <= AFTERNOON_END):
        return []

    all_floors = sorted(
        set(b["floor"] for b in brackets if b.get("floor") is not None)
    )
    if len(all_floors) < 4:
        return []

    mid_idx = len(all_floors) // 2

    confirmed_low  = []
    confirmed_high = []

    for b in brackets:
        floor  = b.get("floor")
        no_bid = b.get("ob_no_bid") or b.get("no_bid") or 0.0
        if floor is None or no_bid < CONFIRM_THRESHOLD:
            continue
        if floor in all_floors:
            if all_floors.index(floor) < mid_idx:
                confirmed_low.append(floor)
            else:
                confirmed_high.append(floor)

    n_low  = len(confirmed_low)
    n_high = len(confirmed_high)

    if n_low == n_high or (n_low == 0 and n_high == 0):
        return []

    signals = []

    for direction, n_dom, n_other, dom_confirmed in [
        ("low",  n_low,  n_high, confirmed_low),
        ("high", n_high, n_low,  confirmed_high),
    ]:
        if n_dom <= n_other:
            continue

        # Next unconfirmed bracket on the dominant side
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

        target_floor = candidates[0]
        target = next((b for b in brackets
                       if b.get("floor") is not None
                       and abs(b["floor"] - target_floor) < 0.1), None)
        if target is None:
            continue

        yes_ask = target.get("ob_yes_ask") or target.get("yes_ask") or 0.0
        no_ask  = target.get("ob_no_ask")  or target.get("no_ask")  or 0.0

        if not (0.01 < yes_ask <= AFTERNOON_MAX_YES):
            continue
        if no_ask < AFTERNOON_MIN_NO:
            continue
        if no_ask is None or no_ask == 0:
            continue

        trigger_info = (f"{direction}-side dominant ({n_dom} confirmed vs {n_other}) "
                        f"→ B{target_floor:.1f} YES={yes_ask:.2f}")
        signals.append(_make_signal(target, "cascade_afternoon", trigger_info))

    return signals


# ---------------------------------------------------------------------------
# Per-city evaluator
# ---------------------------------------------------------------------------

def evaluate_city_cascade(city: str, scan_data: dict) -> dict:
    """
    Run both cascade tiers for one city.
    Returns an eval dict in the same format as hight_decision_engine.evaluate_city().
    """
    result = {
        "city":         city,
        "evaluated_at": datetime.now(timezone.utc).isoformat(),
        "signals":      [],
        "error":        None,
        "cascade":      True,
    }

    if city in CASCADE_SKIP_CITIES or _is_paused(city):
        result["error"] = f"City skipped for cascade ({city})"
        return result

    if scan_data.get("error"):
        result["error"] = f"Kalshi error: {scan_data['error']}"
        return result

    brackets   = scan_data.get("brackets", [])
    local_hour = _local_hour(city)

    # Morning tier
    morning_sig = _morning_signal(city, brackets, local_hour)
    if morning_sig:
        result["signals"].append(morning_sig)

    # Afternoon tier
    afternoon_sigs = _afternoon_signal(city, brackets, local_hour)
    result["signals"].extend(afternoon_sigs)

    return result


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run(kalshi_results: dict, city_filter: str = None) -> list[dict]:
    """
    Run cascade evaluation for all cities.

    Args:
        kalshi_results: dict from kalshi_scanner.scan_all() — already fetched
                        by hight_decision_engine.run(), no extra API calls.
        city_filter:    optional city name to restrict evaluation.

    Returns:
        list of eval dicts, same format as hight_decision_engine.run().
    """
    evaluations = []
    for city, scan_data in kalshi_results.items():
        if city_filter and city.lower() != city_filter.lower():
            continue
        eval_result = evaluate_city_cascade(city, scan_data)
        evaluations.append(eval_result)
    return evaluations


def display(evaluations: list[dict]):
    """Print cascade signals to console."""
    cascade_evals = [e for e in evaluations if e.get("cascade")]
    if not cascade_evals:
        return

    any_signal = False
    print(f"\n{'─'*72}")
    print(f"  Cascade Engine")
    print(f"{'─'*72}")

    for ev in cascade_evals:
        city    = ev["city"]
        signals = [s for s in ev.get("signals", []) if s.get("trade_type")]
        if not signals:
            continue

        any_signal = True
        for s in signals:
            floor = s.get("floor")
            cap   = s.get("cap")
            bracket_str = (f"{floor}–{cap}°F" if floor and cap
                           else f">{floor}°F" if floor else f"<{cap}°F")
            tier_label  = "MORNING" if "morning" in s.get("entry_tier","") else "AFTERNOON"
            print(
                f"  {city:<16} [{tier_label}]  "
                f"{bracket_str:<14}  NO  "
                f"${s['entry_price']:.2f}  "
                f"max={s['max_contracts']}c  "
                f"{s['trigger_info']}"
            )

    if not any_signal:
        print("  No cascade signals at this time.")
