"""
cascade_engine.py
-----------------
Additive cascade signal module for Kalshi HIGH temperature markets.

Generates NO trade signals based purely on market price confirmation —
no NWS forecast or observation data required. Works alongside the main
hight_decision_engine.py as a separate signal path.

OVERNIGHT DISTANCE ENGINE  (22:00–09:00 local)
───────────────────────────────────────────────
Mechanism:
  Overnight, temperature markets have already priced in where tomorrow's
  high will land. Brackets 2+ ranks away from the forecast bracket are
  structurally eliminated — the temperature would need to deviate by
  multiple standard deviations from the overnight consensus to settle YES.
  No observation data required; the signal is purely structural.

Entry gates (all must pass):
  - Local hour in [22, 23] or [0, 8]
  - Bracket rank >= OVN_MIN_RANK (2) from the market forecast bracket
    (forecast bracket = highest current Yes price among B brackets)
  - max_yes_this  <= OVN_MAX_YES_THIS  (0.15): target bracket is not contested
  - n1_avg_yes    <= OVN_N1_AVG_YES_MAX (0.30): N+1 bracket (between target and
    forecast) also not contested — average over overnight polls this session
  - forecast_conf >= OVN_FORECAST_CONF  (0.45): market has meaningful conviction
    in the forecast bracket
  - No price in [OVN_NO_MIN_ENTRY, OVN_NO_MAX_ENTRY] = [0.78, 0.95]
  - At least OVN_MIN_POLLS (1) overnight readings for the N+1 bracket.
    Data shows n1_avg_yes drifts only 0.013 across the full overnight window
    and WR at 22:00 (97.9%) is at least as good as later hours — no benefit
    in waiting for more polls.

Sizing: OVN_CONTRACTS (2) flat — conservative start, scale once validated live.

Backtest (Apr 6 – May 12 2026, 37 days):
  N+2 base: 282 cases  WR=94.7%
  2-filter (max_yes_this<=0.15, forecast_conf>=0.45): 176 cases  WR=97.7%
  3-filter (add n1_avg_yes<=0.30): 118 cases  WR=99.2%  (1 loss in 118)
  WR by entry hour: 22:00=97.9%  23:00=99.3%  00-08:00 range 95.7–98.6%

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
from market_utils import (
    local_hour as _local_hour,
    no_price   as _no_price,
    yes_price  as _yes_price,
)

# ---------------------------------------------------------------------------
# Parameters
# ---------------------------------------------------------------------------

CONV_THRESHOLD       = 0.97
NO_MIN_ENTRY         = 0.60
NO_MAX_ENTRY         = 0.90
MAX_RANK_FROM_BOTTOM = 2     # only enter the 3 lowest B brackets (ranks 0,1,2)
                              # prevents entries near the forecast zone in spring
START_HOUR_CAP       = 15
LATE_HOUR            = 13
MAX_ENTRIES_DEFAULT  = 2
MAX_ENTRIES_EXTENDED = 3
MAX_ENTRIES_LATE     = 1

CONTRACT_TIERS = [
    (0.60, 0.70, 2),   # low conviction
    (0.71, 0.80, 4),   # mid conviction
    (0.81, 0.90, 6),   # high conviction
]

# Top-down cascade — more conservative than bottom-up
# Cold days have higher variance in landing temperature.
# Backtest: max=0.85, 2/4c -> $14.26 total, 3 bad days, worst -$2.65
NO_MAX_ENTRY_TOPDOWN   = 0.85
MAX_RANK_FROM_TOP      = 2     # only enter the 3 highest B brackets
CONTRACT_TIERS_TOPDOWN = [
    (0.60, 0.70, 2),
    (0.71, 0.85, 4),
]

# Afternoon tier parameters (retained)
CONFIRM_THRESHOLD = 0.98
AFTERNOON_START   = 14
AFTERNOON_END     = 15
AFTERNOON_MAX_YES = 0.15

# ---------------------------------------------------------------------------
# RATCHET parameters
# ---------------------------------------------------------------------------
# A bracket whose No price climbs non-strictly for RATCHET_MIN_STREAK
# consecutive polls and ends at or above RATCHET_NO_FLOOR settles No
# with ~99% probability (backtest: 3,606 cases, 99.2% WR, evening window).
#
# Evening window only (18–21 local) — the overnight low starts pricing in
# during this window, making the signal most reliable.

RATCHET_MIN_STREAK  = 5      # consecutive non-decreasing polls required
RATCHET_NO_FLOOR    = 0.88   # No price must be at least this at trigger
RATCHET_NO_MAX      = 0.92   # don't enter if No has already moved past this
RATCHET_EVENING_START = 18
RATCHET_EVENING_END   = 21
RATCHET_CONTRACTS   = 3      # flat sizing — high conviction, conservative start

# ---------------------------------------------------------------------------
# OVERNIGHT DISTANCE ENGINE parameters
# ---------------------------------------------------------------------------
# Fires during the overnight window on brackets structurally far from the
# market's forecast. Requires price-history accumulation across polls
# (n1_avg_yes) — see _overnight_distance_signals() for full logic.
#
# Thresholds derived from backtest grid search on Apr 6 – May 12 2026
# observations (20 cities, 37 days):
#   max_yes_this <= 0.15 AND n1_avg_yes <= 0.30 AND forecast_conf >= 0.45
#   → 118 cases, 99.2% WR (1 loss), keeping 67% of the 2-filter base.

OVN_START_HOUR      = 22    # inclusive; together with OVN_END_HOUR forms
OVN_END_HOUR        = 9     # exclusive — window = [22,23] ∪ [0,8]
OVN_NO_MIN_ENTRY    = 0.78  # tighter floor than main engine (no obs support)
OVN_NO_MAX_ENTRY    = 0.95
OVN_MIN_RANK        = 2     # N+1 excluded entirely (87.8% WR — structurally unsafe)
OVN_MAX_YES_THIS    = 0.15  # target bracket overnight Yes cap
OVN_N1_AVG_YES_MAX  = 0.30  # N+1 bracket average overnight Yes cap
OVN_FORECAST_CONF   = 0.45  # forecast bracket Yes floor (market conviction)
OVN_MIN_POLLS       = 1     # minimum overnight readings before firing.
                            # Data shows n1_avg_yes is stable from the first poll
                            # (drifts only 0.013 across the full overnight window),
                            # and WR at hour 22 (97.9%) is at least as good as later
                            # hours. Requiring more polls adds latency without benefit.
OVN_CONTRACTS       = 2     # flat sizing — conservative; scale after live validation


# ---------------------------------------------------------------------------
# Session state
# ---------------------------------------------------------------------------

_direction_locked:    dict[tuple[str, str], str]  = {}
_entries_made:        dict[tuple[str, str], int]  = {}
_trigger_hour:        dict[tuple[str, str], int]  = {}
_cascade_entered:     set[str]                    = set()
_afternoon_triggered: dict[tuple[str, str, str], bool] = {}

# Top-down session state
_td_direction_locked: dict[tuple[str, str], str]  = {}
_td_entries_made:     dict[tuple[str, str], int]  = {}
_td_trigger_hour:     dict[tuple[str, str], int]  = {}
_td_entered:          set[str]                    = set()

# ---------------------------------------------------------------------------
# LOWT session state — separate namespace from HIGH to avoid cross-market
# direction locks. LOWT markets have inverted dynamics:
#   - Bottom-up is primary (low temp already set, temp rises through brackets)
#   - Top-down is secondary (unexpectedly cold days)
# Excluded cities for LOWT cascade (poor historical performance):
#   - Bottom-up: Philadelphia, San Francisco
#   - Top-down:  Austin, Chicago
# ---------------------------------------------------------------------------
_LOWT_BU_EXCLUDED = {"Philadelphia", "San Francisco"}
_LOWT_TD_EXCLUDED = {"Austin", "Chicago"}

_lowt_bu_entered:   set[str]                   = set()
_lowt_bu_made:      dict[tuple[str, str], int] = {}
_lowt_bu_locked:    dict[tuple[str, str], str] = {}
_lowt_bu_trigger:   dict[tuple[str, str], int] = {}

_lowt_td_entered:   set[str]                   = set()
_lowt_td_made:      dict[tuple[str, str], int] = {}
_lowt_td_locked:    dict[tuple[str, str], str] = {}
_lowt_td_trigger:   dict[tuple[str, str], int] = {}

# RATCHET session state — keyed by ticker (naturally date-scoped)
_ratchet_streaks:  dict[str, int]   = {}   # ticker → current consecutive non-decreasing count
_ratchet_last_no:  dict[str, float] = {}   # ticker → no_price seen last poll
_ratchet_entered:  set[str]         = set()  # tickers already traded this session

# OVERNIGHT DISTANCE session state
# _ovn_yes_by_hour[ticker][hour] = latest yes_price seen at that local hour.
# Using hour as key means one reading per hour per ticker per session, which
# prevents a single busy poll from dominating the average while naturally
# accumulating signal across the overnight window.
_ovn_yes_by_hour:  dict[str, dict[int, float]] = {}
_ovn_entered:      set[str]                    = set()


def _market_date(ticker: str) -> str:
    try:
        return ticker.split("-")[1]
    except (IndexError, AttributeError):
        return ""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _is_paused(city: str, market_type: str = "high") -> bool:
    """Check if trading is disabled for a city/market-type combination."""
    meta = _CITY_REGISTRY.get(city, {})
    if market_type == "lowt":
        return not meta.get("trading_lowt", meta.get("trading_high", True))
    return not meta.get("trading_high", meta.get("trading", True))


def _contracts_for(no_price: float) -> int:
    for lo, hi, contracts in CONTRACT_TIERS:
        if lo <= no_price <= hi:
            return contracts
    return 2


def _contracts_for_topdown(no_price: float) -> int:
    for lo, hi, contracts in CONTRACT_TIERS_TOPDOWN:
        if lo <= no_price <= hi:
            return contracts
    return 2


def _corrected_forecast(city: str, nws_data: dict) -> float | None:
    """
    Return the best available forecast high for a city.
    Priority: AccuWeather > NWS corrected (NWS + bias).
    AccuWeather is more accurate (MAE 1.22°F vs 2.56°F for NWS).
    """
    # Try AccuWeather first
    try:
        import accuweather_feed as _aw
        aw_data = _aw.snapshot(city_filter=city)
        aw_high = (aw_data.get(city) or {}).get("forecast_high_f")
        if aw_high is not None:
            return float(aw_high)
    except Exception:
        pass

    # Fall back to NWS corrected forecast
    fcst = nws_data.get("forecast_high_f")
    if fcst is None:
        return None
    try:
        from hight_decision_engine import _city_bias as _get_bias
        return fcst + _get_bias(city)
    except Exception:
        return fcst


def _forecast_bracket_idx(corrected_fcst: float | None,
                           b_sorted: list[dict],
                           buffer: float = 1.0) -> int | None:
    """
    Find the forecast bracket index with a 1°F buffer below the floor.
    Mirrors the get_forecast_bracket() logic in hight_decision_engine.
    """
    if corrected_fcst is None:
        return None
    n = len(b_sorted)
    for i, b in enumerate(b_sorted):
        floor = b.get("floor", 0)
        cap   = b.get("cap", 999)
        # Standard: forecast inside bracket
        if floor <= corrected_fcst <= cap:
            return i
        # Buffer: forecast within buffer°F below the floor
        if (floor - buffer) <= corrected_fcst < floor:
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

    # Market forecast bracket: highest Yes price = what the market predicts
    try:
        _mkt_fcst_idx = max(range(len(b_brackets)),
                            key=lambda i: _yes_price(b_brackets[i]))
    except Exception:
        _mkt_fcst_idx = None

    from log_setup import get_logger as _gl
    _log = _gl("cascade_engine")
    _log.debug("CASCADE BU %s: mkt_fcst_idx=%s yes_prices=%s",
               city, _mkt_fcst_idx,
               [(b.get("ticker","").split("-")[-1], _yes_price(b))
                for b in b_brackets])

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
        # Market distance gate: skip if target is within 1 rank of
        # the market's own forecast bracket (highest Yes price)
        if _mkt_fcst_idx is not None and abs((i + 1) - _mkt_fcst_idx) < 2:
            continue

        # Rank restriction: only enter the N lowest brackets
        # (i+1 is the rank of the target bracket from the bottom)
        if i + 1 > MAX_RANK_FROM_BOTTOM:
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

def _topdown_signals(city: str, brackets: list[dict],
                     nws_data: dict) -> list[dict]:
    """
    Top-down directional cascade scanner.

    Mirror of _convergence_signals but scanning from the top down.
    Fires on cold days when the highest B bracket crosses No >= CONV_THRESHOLD,
    entering the bracket immediately below it.

    More conservative than bottom-up:
    - Max entry price: 0.85 (vs 0.90 for bottom-up)
    - Max contracts: 4 (vs 6 for bottom-up)
    - Only the 3 highest B brackets eligible (MAX_RANK_FROM_TOP = 2)

    Direction lock: once top-down fires for a city-day, bottom-up is blocked
    and vice versa (enforced by shared _direction_locked dict).
    """
    if not brackets or len(brackets) < 3:
        return []

    sample_ticker = next((b["ticker"] for b in brackets if b.get("ticker")), "")
    market_date   = _market_date(sample_ticker)
    city_key      = (city, market_date)
    local_hour    = _local_hour(city)

    # Check shared direction lock — don't fire top-down if bottom-up locked
    locked_dir = _direction_locked.get(city_key)
    if locked_dir == 'up':
        return []

    # Top-down specific state
    td_locked  = _td_direction_locked.get(city_key)
    td_entries = _td_entries_made.get(city_key, 0)
    td_trigger = _td_trigger_hour.get(city_key)

    # No new cascade starts at or after START_HOUR_CAP
    if td_locked is None and local_hour >= START_HOUR_CAP:
        return []

    # Max entries
    if td_trigger is not None and td_trigger >= LATE_HOUR:
        max_e = MAX_ENTRIES_LATE
    else:
        max_e = MAX_ENTRIES_DEFAULT

    if td_entries >= max_e:
        return []

    # Sort B brackets high→low for top-down scanning
    b_brackets = sorted(
        [b for b in brackets
         if b.get("floor") is not None and b.get("cap") is not None],
        key=lambda b: b["floor"],
        reverse=True   # high to low
    )
    if len(b_brackets) < 2:
        return []

    cf       = _corrected_forecast(city, nws_data)
    fcst_idx_asc = _forecast_bracket_idx(cf, list(reversed(b_brackets)))
    # Convert to descending index
    n = len(b_brackets)

    # Market forecast bracket (ascending index for comparison)
    try:
        _b_asc = list(reversed(b_brackets))
        _mkt_fcst_idx_td = max(range(len(_b_asc)),
                               key=lambda i: _yes_price(_b_asc[i]))
    except Exception:
        _mkt_fcst_idx_td = None

    signals = []

    for i, confirmed_b in enumerate(b_brackets):
        if _no_price(confirmed_b) < CONV_THRESHOLD:
            continue
        if i >= n - 1:
            continue   # no bracket below in descending order

        # Rank restriction: only top 3 brackets eligible as triggers
        if i > MAX_RANK_FROM_TOP:
            continue

        target = b_brackets[i + 1]   # next bracket downward
        ticker = target.get("ticker", "")

        if not ticker or ticker in _td_entered:
            continue

        # Skip forecast bracket
        target_rank_asc = n - 1 - (i + 1)
        if fcst_idx_asc is not None and target_rank_asc == fcst_idx_asc:
            continue
        # Market distance gate (top-down)
        if _mkt_fcst_idx_td is not None and abs(target_rank_asc - _mkt_fcst_idx_td) < 2:
            continue

        no_ask  = _no_price(target)
        yes_ask = _yes_price(target)

        if not (NO_MIN_ENTRY <= no_ask <= NO_MAX_ENTRY_TOPDOWN):
            continue

        # Lock direction
        if td_locked is None:
            _td_direction_locked[city_key] = 'down'
            _direction_locked[city_key]    = 'down'  # block bottom-up
            _td_trigger_hour[city_key]     = local_hour
            td_trigger = local_hour

        _td_entered.add(ticker)
        _td_entries_made[city_key] = td_entries + 1
        td_entries += 1

        contracts    = _contracts_for_topdown(no_ask)
        trigger_info = (
            f"top-down: {confirmed_b['ticker'].split('-')[-1]} "
            f"No={_no_price(confirmed_b):.2f} → "
            f"{ticker.split('-')[-1]} No={no_ask:.2f} "
            f"YES={yes_ask:.2f}  {contracts}c"
        )

        signals.append(_make_signal(target, "cascade_directional_down",
                                    trigger_info, contracts))

        if td_entries >= max_e:
            break

    return signals


def _lowt_bu_signals(city: str, brackets: list[dict]) -> list[dict]:
    """
    LOWT bottom-up cascade scanner.

    For LOWT markets the overnight low is set before dawn. As the day
    progresses, temperature rises and bottom brackets (coldest lows) confirm
    No first — a confirmed fact that the low did not land that low.

    Backtest (Apr 6-24, 18 days):
      143 signals  WR=97.2%  EV=+$0.155/signal  0 bad days
      Scaled PnL (2/4/6c): +$80.19 over 18 days

    Excluded: Philadelphia, San Francisco (poor WR).
    """
    if not brackets or len(brackets) < 3:
        return []
    if city in _LOWT_BU_EXCLUDED:
        return []

    sample_ticker = next((b["ticker"] for b in brackets if b.get("ticker")), "")
    market_date   = _market_date(sample_ticker)
    city_key      = (city, market_date)
    local_hour    = _local_hour(city)

    locked   = _lowt_bu_locked.get(city_key)
    entries  = _lowt_bu_made.get(city_key, 0)
    trig_h   = _lowt_bu_trigger.get(city_key)

    if locked is None and local_hour >= START_HOUR_CAP:
        return []

    max_e = MAX_ENTRIES_LATE if (trig_h is not None and trig_h >= LATE_HOUR)             else MAX_ENTRIES_DEFAULT
    if entries >= max_e:
        return []

    # Sort B brackets low→high
    b_brackets = sorted(
        [b for b in brackets if b.get("floor") is not None and b.get("cap") is not None],
        key=lambda b: b["floor"]
    )
    if len(b_brackets) < 2:
        return []

    signals = []
    for i, confirmed_b in enumerate(b_brackets):
        if _no_price(confirmed_b) < CONV_THRESHOLD:
            continue
        if i >= len(b_brackets) - 1:
            continue
        if i + 1 > MAX_RANK_FROM_BOTTOM:
            continue

        target = b_brackets[i + 1]
        ticker = target.get("ticker", "")
        if not ticker or ticker in _lowt_bu_entered:
            continue

        no_ask  = _no_price(target)
        yes_ask = _yes_price(target)
        if not (NO_MIN_ENTRY <= no_ask <= NO_MAX_ENTRY):
            continue

        if locked is None:
            _lowt_bu_locked[city_key]   = 'up'
            _lowt_bu_trigger[city_key]  = local_hour
            trig_h = local_hour

        _lowt_bu_entered.add(ticker)
        _lowt_bu_made[city_key] = entries + 1
        entries += 1

        contracts    = _contracts_for(no_ask)
        trigger_info = (
            f"LOWT bottom-up: {confirmed_b['ticker'].split('-')[-1]} "
            f"No={_no_price(confirmed_b):.2f} → "
            f"{ticker.split('-')[-1]} No={no_ask:.2f} "
            f"YES={yes_ask:.2f}  {contracts}c"
        )
        signals.append(_make_signal(target, "cascade_lowt_bu", trigger_info, contracts))

        if entries >= max_e:
            break

    return signals


def _lowt_td_signals(city: str, brackets: list[dict]) -> list[dict]:
    """
    LOWT top-down cascade scanner.

    Fires on cold nights when the top bracket (warmest low) confirms No first.
    More conservative than bottom-up — cold nights are harder to predict.

    Backtest (Apr 6-24, 18 days):
      33 signals  WR=87.9%  EV=+$0.143/signal  3 bad days
      Scaled PnL (2/4c): +$14.05 over 18 days

    Excluded: Austin, Chicago (poor WR on top-down LOWT).
    """
    if not brackets or len(brackets) < 3:
        return []
    if city in _LOWT_TD_EXCLUDED:
        return []

    sample_ticker = next((b["ticker"] for b in brackets if b.get("ticker")), "")
    market_date   = _market_date(sample_ticker)
    city_key      = (city, market_date)
    local_hour    = _local_hour(city)

    locked   = _lowt_td_locked.get(city_key)
    entries  = _lowt_td_made.get(city_key, 0)
    trig_h   = _lowt_td_trigger.get(city_key)

    # Block if bottom-up already locked for this city-day
    if _lowt_bu_locked.get(city_key) == 'up':
        return []
    if locked is None and local_hour >= START_HOUR_CAP:
        return []

    max_e = MAX_ENTRIES_LATE if (trig_h is not None and trig_h >= LATE_HOUR)             else MAX_ENTRIES_DEFAULT
    if entries >= max_e:
        return []

    # Sort B brackets high→low for top-down
    b_brackets = sorted(
        [b for b in brackets if b.get("floor") is not None and b.get("cap") is not None],
        key=lambda b: b["floor"],
        reverse=True
    )
    if len(b_brackets) < 2:
        return []
    n = len(b_brackets)

    signals = []
    for i, confirmed_b in enumerate(b_brackets):
        if _no_price(confirmed_b) < CONV_THRESHOLD:
            continue
        if i >= n - 1:
            continue
        if i > MAX_RANK_FROM_TOP:
            continue

        target = b_brackets[i + 1]
        ticker = target.get("ticker", "")
        if not ticker or ticker in _lowt_td_entered:
            continue

        no_ask  = _no_price(target)
        yes_ask = _yes_price(target)
        if not (NO_MIN_ENTRY <= no_ask <= NO_MAX_ENTRY_TOPDOWN):
            continue

        if locked is None:
            _lowt_td_locked[city_key]  = 'down'
            _lowt_td_trigger[city_key] = local_hour
            trig_h = local_hour

        _lowt_td_entered.add(ticker)
        _lowt_td_made[city_key] = entries + 1
        entries += 1

        contracts    = _contracts_for_topdown(no_ask)
        trigger_info = (
            f"LOWT top-down: {confirmed_b['ticker'].split('-')[-1]} "
            f"No={_no_price(confirmed_b):.2f} → "
            f"{ticker.split('-')[-1]} No={no_ask:.2f} "
            f"YES={yes_ask:.2f}  {contracts}c"
        )
        signals.append(_make_signal(target, "cascade_lowt_td", trigger_info, contracts))

        if entries >= max_e:
            break

    return signals


def _ratchet_signals(city: str, brackets: list[dict]) -> list[dict]:
    """
    RATCHET signal — steady No climb over consecutive polls.

    For each bracket, maintain a streak counter across polls. When the
    No price has been non-decreasing for RATCHET_MIN_STREAK consecutive
    polls AND has reached RATCHET_NO_FLOOR, fire a No entry.

    Restricted to the evening window (18–21 local) where LOWT markets
    are most informative — the overnight low starts pricing in during
    this period.

    Backtest: 3,606 cases in evening window, 99.2% WR at streak=5 +
    floor=0.88. One of the highest-confidence signals in the system.

    State persists across polls via module-level dicts keyed by ticker.
    Tickers include the market date so state naturally expires overnight.
    """
    if not brackets:
        return []

    local_hour = _local_hour(city)
    if not (RATCHET_EVENING_START <= local_hour <= RATCHET_EVENING_END):
        return []

    from log_setup import get_logger as _gl
    _log = _gl("cascade_engine")

    signals = []

    for bracket in brackets:
        ticker = bracket.get("ticker", "")
        if not ticker:
            continue

        # B brackets only — T brackets have different dynamics
        bracket_code = ticker.split("-")[-1] if "-" in ticker else ""
        if not bracket_code.startswith("B"):
            continue

        if ticker in _ratchet_entered:
            continue

        no_p = _no_price(bracket)
        if no_p <= 0.0:
            continue

        # Update streak
        prev_no = _ratchet_last_no.get(ticker)
        if prev_no is None:
            # First time we see this ticker — start streak at 1
            _ratchet_streaks[ticker] = 1
        else:
            if no_p >= prev_no:
                _ratchet_streaks[ticker] = _ratchet_streaks.get(ticker, 1) + 1
            else:
                _ratchet_streaks[ticker] = 1   # reset on any dip

        _ratchet_last_no[ticker] = no_p

        streak = _ratchet_streaks[ticker]

        _log.debug(
            "RATCHET  %s  %s  No=%.2f  streak=%d",
            city, ticker, no_p, streak,
        )

        # Check trigger conditions
        if streak < RATCHET_MIN_STREAK:
            continue
        if no_p < RATCHET_NO_FLOOR:
            continue
        if no_p > RATCHET_NO_MAX:
            continue

        _ratchet_entered.add(ticker)

        trigger_info = (
            f"ratchet: No={no_p:.2f}  streak={streak}  "
            f"floor={RATCHET_NO_FLOOR}  window=evening  {RATCHET_CONTRACTS}c"
        )

        _log.info(
            "RATCHET  %s  %s  No=%.2f  streak=%d  → ENTRY",
            city, ticker, no_p, streak,
        )

        signals.append(_make_signal(bracket, "cascade_ratchet", trigger_info,
                                    RATCHET_CONTRACTS))

    return signals


def _overnight_distance_signals(city: str, brackets: list[dict],
                                local_hour: int) -> list[dict]:
    """
    Overnight distance engine — enters NO on brackets >= N+2 from the market
    forecast bracket during the overnight window (22:00–09:00 local).

    Three-filter entry gate (all must pass):
      1. max_yes_this  <= OVN_MAX_YES_THIS  (0.15)
         The target bracket's current Yes price is low — market is not
         treating it as a candidate.
      2. n1_avg_yes    <= OVN_N1_AVG_YES_MAX (0.30)
         The N+1 bracket (immediately between target and forecast) has also
         been priced with low Yes throughout the overnight window. A high N+1
         Yes signals real uncertainty about how far the temperature travels,
         which flows through to our N+2 target. This filter eliminates all
         4 surviving N+2 losses from the 2-filter backtest.
      3. forecast_conf >= OVN_FORECAST_CONF (0.45)
         The forecast bracket has a meaningful Yes price — the market has
         converged on a view. Diffuse markets (low forecast_conf) have
         unreliable bracket structure overnight.

    State:
      _ovn_yes_by_hour accumulates one yes_price reading per local hour
      per ticker. n1_avg_yes is computed from overnight hours only
      (h >= 22 or h <= 8) and requires OVN_MIN_POLLS readings before firing.

    Sizing: OVN_CONTRACTS (2) flat. Scale after live validation.
    """
    # ── Window check ──────────────────────────────────────────────────────
    if not (local_hour >= OVN_START_HOUR or local_hour < OVN_END_HOUR):
        return []

    if not brackets or len(brackets) < 3:
        return []

    from log_setup import get_logger as _gl
    _log = _gl("cascade_engine")

    # ── Build sorted B-bracket list ───────────────────────────────────────
    b_brackets = sorted(
        [b for b in brackets
         if b.get("floor") is not None and b.get("cap") is not None],
        key=lambda b: b["floor"],
    )
    if len(b_brackets) < 3:
        return []

    # ── Update yes-price history for all B brackets ───────────────────────
    for b in b_brackets:
        ticker = b.get("ticker", "")
        if not ticker:
            continue
        yes_p = _yes_price(b)
        if yes_p > 0:
            if ticker not in _ovn_yes_by_hour:
                _ovn_yes_by_hour[ticker] = {}
            _ovn_yes_by_hour[ticker][local_hour] = yes_p

    # ── Identify forecast bracket (highest current Yes price) ─────────────
    # Using current yes price rather than stored forecast — more responsive
    # to any late-evening NWS/AccuWeather update, and consistent with how
    # the backtest identified the forecast bracket.
    forecast_idx = max(range(len(b_brackets)),
                       key=lambda i: _yes_price(b_brackets[i]))
    forecast_b   = b_brackets[forecast_idx]
    forecast_tk  = forecast_b.get("ticker", "")

    # Forecast confidence: current Yes of the forecast bracket
    forecast_conf = _yes_price(forecast_b)
    if forecast_conf < OVN_FORECAST_CONF:
        _log.debug(
            "OVN_DIST %s: SKIP — forecast_conf=%.2f < %.2f",
            city, forecast_conf, OVN_FORECAST_CONF,
        )
        return []

    signals = []

    for i, bracket in enumerate(b_brackets):
        ticker = bracket.get("ticker", "")
        if not ticker or ticker in _ovn_entered:
            continue

        rank_dist = abs(i - forecast_idx)
        if rank_dist < OVN_MIN_RANK:
            continue

        # ── Price gate ────────────────────────────────────────────────────
        no_p  = _no_price(bracket)
        yes_p = _yes_price(bracket)
        if not (OVN_NO_MIN_ENTRY <= no_p <= OVN_NO_MAX_ENTRY):
            continue

        # ── Filter 1: max_yes_this ─────────────────────────────────────────
        if yes_p > OVN_MAX_YES_THIS:
            _log.debug(
                "OVN_DIST %s %s: SKIP — yes=%.3f > %.2f",
                city, ticker, yes_p, OVN_MAX_YES_THIS,
            )
            continue

        # ── Filter 2: n1_avg_yes ───────────────────────────────────────────
        # N+1 bracket sits between this bracket and the forecast bracket.
        n1_idx = forecast_idx + (1 if i > forecast_idx else -1)
        if not (0 <= n1_idx < len(b_brackets)):
            continue  # edge case: target is N+2 but N+1 doesn't exist
        n1_ticker = b_brackets[n1_idx].get("ticker", "")

        n1_history = _ovn_yes_by_hour.get(n1_ticker, {})
        n1_overnight = [
            p for h, p in n1_history.items()
            if h >= OVN_START_HOUR or h < OVN_END_HOUR
        ]
        if len(n1_overnight) < OVN_MIN_POLLS:
            _log.debug(
                "OVN_DIST %s %s: SKIP — n1 polls=%d < %d (accumulating)",
                city, ticker, len(n1_overnight), OVN_MIN_POLLS,
            )
            continue
        n1_avg_yes = sum(n1_overnight) / len(n1_overnight)
        if n1_avg_yes > OVN_N1_AVG_YES_MAX:
            _log.debug(
                "OVN_DIST %s %s: SKIP — n1_avg_yes=%.3f > %.2f",
                city, ticker, n1_avg_yes, OVN_N1_AVG_YES_MAX,
            )
            continue

        # ── All filters passed ─────────────────────────────────────────────
        _ovn_entered.add(ticker)

        direction = "above" if i > forecast_idx else "below"
        trigger_info = (
            f"ovn_dist: rank=N+{rank_dist} {direction} fcst "
            f"[{forecast_tk.split('-')[-1]}]  "
            f"No={no_p:.2f}  yes={yes_p:.3f}  "
            f"n1_avg_yes={n1_avg_yes:.3f} (n={len(n1_overnight)})  "
            f"fcst_conf={forecast_conf:.2f}  "
            f"{OVN_CONTRACTS}c"
        )

        _log.info(
            "OVN_DIST  %s  %s  No=%.2f  rank=N+%d %s  "
            "n1_avg=%.3f  fcst_conf=%.2f  %dc",
            city, ticker, no_p, rank_dist, direction,
            n1_avg_yes, forecast_conf, OVN_CONTRACTS,
        )

        signals.append(_make_signal(bracket, "cascade_ovn_dist",
                                    trigger_info, OVN_CONTRACTS))

    return signals


def evaluate_city_cascade_lowt(city: str, scan_data: dict) -> dict:
    """Evaluate LOWT cascade signals for a single city."""
    result = {
        "city":         city,
        "evaluated_at": datetime.now(timezone.utc).isoformat(),
        "signals":      [],
        "error":        None,
        "cascade":      True,
        "market_type":  "lowt",
    }

    if _is_paused(city, market_type="lowt"):
        result["error"] = "City paused for LOWT — cascade skipped"
        return result

    if scan_data.get("error"):
        result["error"] = f"Kalshi error: {scan_data['error']}"
        return result

    brackets = scan_data.get("brackets", [])
    if not brackets:
        return result

    result["signals"].extend(_lowt_bu_signals(city, brackets))
    result["signals"].extend(_lowt_td_signals(city, brackets))
    result["signals"].extend(_ratchet_signals(city, brackets))

    return result


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

    # Overnight distance engine — fires 22:00–09:00 on N+2+ brackets
    result["signals"].extend(
        _overnight_distance_signals(city, brackets, local_hour)
    )

    # Convergence scanner — today (bottom-up)
    result["signals"].extend(_convergence_signals(city, brackets, nws_data))

    # Top-down scanner — today (cold days)
    result["signals"].extend(_topdown_signals(city, brackets, nws_data))

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

def run(kalshi_results: dict, city_filter: str = None, nws_results: dict = None) -> list[dict]:
    evaluations = []
    for city, scan_data in kalshi_results.items():
        if city_filter and city.lower() != city_filter.lower():
            continue
        evaluations.append(evaluate_city_cascade(city, scan_data))
    return evaluations


# ---------------------------------------------------------------------------
# Display
# ---------------------------------------------------------------------------

def run_lowt(kalshi_lowt_results: dict, city_filter: str = None) -> list[dict]:
    """Run LOWT cascade evaluation for all cities."""
    evaluations = []
    for city, scan_data in kalshi_lowt_results.items():
        if city_filter and city.lower() != city_filter.lower():
            continue
        evaluations.append(evaluate_city_cascade_lowt(city, scan_data))
    return evaluations


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
                "OVN_DIST"    if "ovn_dist"          in tier else
                "DIRECT-UP"   if "directional_up"    in tier else
                "DIRECT-DOWN" if "directional_down"  in tier else
                "LOWT-UP"     if "lowt_bu"           in tier else
                "LOWT-DOWN"   if "lowt_td"           in tier else
                "RATCHET"     if "ratchet"            in tier else
                "TOMORROW"    if "tomorrow"           in tier else
                "AFTERNOON"   if "afternoon"          in tier else
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
