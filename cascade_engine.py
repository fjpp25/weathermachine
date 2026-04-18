"""
cascade_engine.py
-----------------
Additive cascade signal module for Kalshi HIGH temperature markets.

Generates NO trade signals based purely on market price confirmation —
no NWS forecast or observation data required. Works alongside the main
hight_decision_engine.py as a separate signal path.

Two tiers:

  FIRST-CLOSE TIER  (any hour)
  ─────────────────────────────
  Trigger: the FIRST T bracket in a city to hit NO >= T_CLOSE_THRESHOLD (0.99).
           "First" is tracked per city per session — once triggered, the city
           is cooldown-locked for the rest of the day.
  Signal:  enter NO on the adjacent B bracket immediately above the closed T.
           Adjacent B = floor at T_floor + 0.5 (Kalshi bracket spacing).
  Logic:   data validation across 8 days / 148 city-days shows:
             - 39% of adjacent B brackets are in $0.75-$0.92 when first T hits $0.99
             - 94% of those in-band entries go on to resolve NO
           The first T closing at $0.99 is a near-certain directional signal.
  Timing:  fires at any hour — first-T crossings are distributed throughout
           the day, many overnight. No time-window restriction.
  Limit:   1 signal per city per session. Multiple T brackets simultaneously
           at 0.99 → take the lowest-floor one (most informative).

  AFTERNOON TIER  (14:00-15:59 local)
  ─────────────────────────────────────
  Trigger: count brackets confirmed (NO >= CONFIRM_THRESHOLD) on each side.
           Enter on the next unconfirmed bracket on the dominant side.
  Signal:  target bracket must have YES <= AFTERNOON_MAX_YES and
           NO in [NO_MIN_ENTRY_PRICE, NO_MAX_ENTRY_PRICE].
  Limit:   1 signal per city per day per direction.

Both tiers:
  - NO price must be in [0.75, 0.92] — same band as main engine
  - Max CASCADE_MAX_CONTRACTS per signal
  - Signals tagged entry_tier="cascade_first_close" or "cascade_afternoon"
  - Skips paused cities
"""

from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from cities import CITIES as _CITY_REGISTRY

# ---------------------------------------------------------------------------
# Parameters
# ---------------------------------------------------------------------------

T_CLOSE_THRESHOLD    = 0.99   # T bracket NO threshold for first-close signal
                               # Data: 94% win rate on adjacent B in entry band

CONFIRM_THRESHOLD    = 0.98   # afternoon tier: bracket considered confirmed
AFTERNOON_START      = 14     # local hour — afternoon window opens
AFTERNOON_END        = 15     # local hour — afternoon window closes (inclusive)
AFTERNOON_MAX_YES    = 0.15   # skip afternoon target if YES above this

NO_MIN_ENTRY_PRICE   = 0.75   # never pay less for NO
NO_MAX_ENTRY_PRICE   = 0.92   # never pay more — calibrated entry band

CASCADE_MAX_CONTRACTS = 3     # hard cap per signal


# ---------------------------------------------------------------------------
# Session-scoped first-close trigger memory
# Key: (city, market_date) → T bracket ticker that triggered.
# market_date extracted from ticker e.g. "KXHIGHNY-26APR16-T84" → "26APR16"
# Keyed by date so yesterday's trigger never blocks today's market.
# Resets on app restart (fine — daily markets reset anyway).
# ---------------------------------------------------------------------------

_first_close_triggered: dict[tuple[str, str], str] = {}


def _market_date(ticker: str) -> str:
    """Extract the market date portion from a Kalshi ticker.
    e.g. 'KXHIGHNY-26APR16-T84' → '26APR16'. Returns '' on failure."""
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


def _make_signal(bracket: dict, tier: str, trigger_info: str) -> dict:
    no_ask = bracket.get("ob_no_ask")
    return {
        "ticker":        bracket["ticker"],
        "title":         bracket.get("title", ""),
        "floor":         bracket.get("floor"),
        "cap":           bracket.get("cap"),
        "yes_ask":       bracket.get("ob_yes_ask"),
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
        "max_contracts": CASCADE_MAX_CONTRACTS,
        "skip_reason":   None,
    }


# ---------------------------------------------------------------------------
# First-close tier
# ---------------------------------------------------------------------------

def _first_close_signal(city: str, brackets: list[dict]) -> dict | None:
    """
    Fire once per city per market-date when the first T bracket reaches
    NO >= T_CLOSE_THRESHOLD.

    Keyed by (city, market_date) so yesterday's trigger never blocks
    today's market, even when the app runs continuously overnight.
    """
    if not brackets:
        return None

    # Derive market date from any bracket ticker in this set
    sample_ticker = next((b["ticker"] for b in brackets if b.get("ticker")), "")
    market_date   = _market_date(sample_ticker)
    trigger_key   = (city, market_date)

    if trigger_key in _first_close_triggered:
        return None

    bottom_ts = [b for b in brackets if b.get("floor") is None and b.get("cap") is not None]
    top_ts    = [b for b in brackets if b.get("cap") is None  and b.get("floor") is not None]
    b_brackets = [b for b in brackets if b.get("cap") is not None and b.get("floor") is not None]

    if not b_brackets:
        return None

    # Find all T brackets at or above threshold
    closed_ts = [
        b for b in bottom_ts + top_ts
        if (b.get("ob_no_bid") or b.get("no_bid") or 0.0) >= T_CLOSE_THRESHOLD
    ]
    if not closed_ts:
        return None

    # Take the most-confirmed T bracket (highest NO price)
    trigger_t = max(closed_ts, key=lambda b: b.get("ob_no_bid") or 0.0)
    t_no_bid  = trigger_t.get("ob_no_bid") or trigger_t.get("no_bid") or 0.0

    # Find the adjacent B bracket using boundary matching
    # Use 0.6°F tolerance to handle any minor API rounding on T bracket boundaries
    adj = None
    if trigger_t.get("floor") is None:
        # Bottom T: adjacent B is the one whose floor ≈ T's cap
        t_cap = trigger_t["cap"]
        adj = next(
            (b for b in b_brackets if abs(b["floor"] - t_cap) < 0.6),
            None
        )
        adj_desc = f"B above bottom T{t_cap:.0f}"
    else:
        # Top T: adjacent B is the one whose cap ≈ T's floor
        t_floor = trigger_t["floor"]
        adj = next(
            (b for b in b_brackets
             if b.get("cap") is not None and abs(b["cap"] - t_floor) < 0.6),
            None
        )
        adj_desc = f"B below top T{t_floor:.0f}"

    if adj is None:
        return None

    yes_ask = adj.get("ob_yes_ask") or adj.get("yes_ask") or 0.0
    no_ask  = adj.get("ob_no_ask")  or adj.get("no_ask")  or 0.0

    if not (NO_MIN_ENTRY_PRICE <= no_ask <= NO_MAX_ENTRY_PRICE):
        return None
    if not (0.01 < yes_ask <= 0.25):
        return None

    # Lock city+date for rest of session
    _first_close_triggered[trigger_key] = trigger_t["ticker"]

    trigger_info = (
        f"first T close: {trigger_t['ticker'].split('-')[-1]} NO={t_no_bid:.2f} "
        f"-> {adj_desc} YES={yes_ask:.2f}"
    )
    return _make_signal(adj, "cascade_first_close", trigger_info)


# ---------------------------------------------------------------------------
# Afternoon tier
# ---------------------------------------------------------------------------

def _afternoon_signal(city: str, brackets: list[dict], local_hour: int) -> list[dict]:
    """
    Check for afternoon cascade signals (14:00-15:59 local).

    Counts confirmed brackets on each side of the midpoint. If one side
    dominates, enters on the next unconfirmed bracket in that direction.
    """
    if not (AFTERNOON_START <= local_hour <= AFTERNOON_END):
        return []

    all_floors = sorted(
        set(b["floor"] for b in brackets if b.get("floor") is not None)
    )
    if len(all_floors) < 4:
        return []

    mid_idx = len(all_floors) // 2
    confirmed_low, confirmed_high = [], []

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

    n_low, n_high = len(confirmed_low), len(confirmed_high)
    if n_low == n_high or (n_low == 0 and n_high == 0):
        return []

    signals = []
    for direction, n_dom, n_other in [("low", n_low, n_high), ("high", n_high, n_low)]:
        if n_dom <= n_other:
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

        yes_ask = target.get("ob_yes_ask") or target.get("yes_ask") or 0.0
        no_ask  = target.get("ob_no_ask")  or target.get("no_ask")  or 0.0

        if not (0.01 < yes_ask <= AFTERNOON_MAX_YES):
            continue
        if not (NO_MIN_ENTRY_PRICE <= no_ask <= NO_MAX_ENTRY_PRICE):
            continue

        trigger_info = (
            f"{direction}-side dominant ({n_dom} confirmed vs {n_other}) "
            f"-> B{candidates[0]:.1f} YES={yes_ask:.2f}"
        )
        signals.append(_make_signal(target, "cascade_afternoon", trigger_info))

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
    local_hour = _local_hour(city)

    # First-close on today's brackets
    fc_sig = _first_close_signal(city, brackets)
    if fc_sig:
        result["signals"].append(fc_sig)

    # When today's market has fully converged, run first-close on tomorrow's
    # brackets — the next day's market has been open for hours and may already
    # have B brackets in the entry band with T brackets confirming direction.
    tomorrow_brackets = scan_data.get("tomorrow_brackets", [])
    if tomorrow_brackets and scan_data.get("today_converged"):
        tmr_sig = _first_close_signal(city, tomorrow_brackets)
        if tmr_sig:
            tmr_sig["trigger_info"] = "[TOMORROW] " + tmr_sig.get("trigger_info", "")
            tmr_sig["entry_tier"]   = "cascade_tomorrow"
            result["signals"].append(tmr_sig)

    # Afternoon tier on today's brackets (time-gated)
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
                "FIRST-CLOSE" if "first_close" in tier else
                "TOMORROW"    if "tomorrow"    in tier else
                "AFTERNOON"   if "afternoon"   in tier else
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
