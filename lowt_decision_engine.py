"""
lowt_decision_engine.py
-----------------------
LOWT signal engine — market distance gate, evening window.

The overnight low follows a bimodal daily pattern:
  Morning (6-10h): previous night's low establishing
  Evening (18-21h): tonight's low starting to price in

The evening market at 18-21h is near-perfect (MAE 0.13F vs NWS 6.04F).
Enter No on B brackets >= MIN_MARKET_DIST ranks from the evening forecast.

Backtest: 79 signals, 100% WR, EV=+$0.113, $8.92 total, 0 bad days

City windows:
  EVENING (re-open >= 60%): Atlanta, Boston, Las Vegas, LA, New York,
    Phoenix, Seattle, Washington DC
  BOTH (re-open 36-58%): all others — evening preferred, morning fallback
"""
from __future__ import annotations
import argparse
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from typing import Optional
from log_setup import get_logger

log = get_logger(__name__)

NO_MIN_ENTRY    = 0.65
NO_MAX_ENTRY    = 0.92
MIN_MARKET_DIST = 2
T_CONFIRM_THRESHOLD = 0.90
T_CONFIRM_MIN   = 0
MAX_NO_PER_CITY = 2
MAX_CONTRACTS   = 3
EVENING_START   = 18
EVENING_END     = 21
MORNING_START   = 6
MORNING_END     = 10

CITY_WINDOWS: dict[str, str] = {
    "Atlanta":"EVENING","Boston":"EVENING","Las Vegas":"EVENING",
    "Los Angeles":"EVENING","New York":"EVENING","Phoenix":"EVENING",
    "Seattle":"EVENING","Washington DC":"EVENING",
    "Austin":"BOTH","Chicago":"BOTH","Dallas":"BOTH","Denver":"BOTH",
    "Houston":"BOTH","Miami":"BOTH","Minneapolis":"BOTH","New Orleans":"BOTH",
    "Oklahoma City":"BOTH","Philadelphia":"BOTH","San Antonio":"BOTH",
    "San Francisco":"BOTH",
}

_CITY_TZ: dict[str, str] = {
    "New York":"America/New_York","Chicago":"America/Chicago",
    "Miami":"America/New_York","Austin":"America/Chicago",
    "Los Angeles":"America/Los_Angeles","Denver":"America/Denver",
    "Philadelphia":"America/New_York","San Francisco":"America/Los_Angeles",
    "Boston":"America/New_York","Las Vegas":"America/Los_Angeles",
    "Atlanta":"America/New_York","Oklahoma City":"America/Chicago",
    "Phoenix":"America/Phoenix","Washington DC":"America/New_York",
    "Seattle":"America/Los_Angeles","Houston":"America/Chicago",
    "Dallas":"America/Chicago","San Antonio":"America/Chicago",
    "New Orleans":"America/Chicago","Minneapolis":"America/Chicago",
}

def _local_hour(city: str) -> int:
    return datetime.now(ZoneInfo(_CITY_TZ.get(city,"UTC"))).hour

def _no_price(b: dict) -> float:
    return float(b.get("ob_no_bid") or b.get("ob_no_ask") or
                 b.get("no_ask") or b.get("no_bid") or b.get("no_price") or 0.0)

def _yes_price(b: dict) -> float:
    return float(b.get("ob_yes_ask") or b.get("yes_ask") or b.get("yes_price") or 0.0)

def _bval(bracket: dict) -> Optional[float]:
    for src in [bracket.get("bracket",""),
                bracket.get("ticker","").split("-")[-1]]:
        if src and src[0] in "BT":
            try: return float(src[1:])
            except ValueError: pass
    cap = bracket.get("cap")
    return float(cap) if cap is not None else None

def _btype(bracket: dict) -> str:
    for src in [bracket.get("bracket",""),
                bracket.get("ticker","").split("-")[-1]]:
        if src and src[0] in "BT": return src[0]
    return ""

def _in_entry_window(city: str) -> tuple[bool, str]:
    lh      = _local_hour(city)
    verdict = CITY_WINDOWS.get(city, "BOTH")
    if EVENING_START <= lh <= EVENING_END: return True, "evening"
    if verdict == "BOTH" and MORNING_START <= lh <= MORNING_END: return True, "morning"
    return False, "none"


def evaluate_city_lowt(city: str, scan_data: dict) -> dict:
    result = {
        "city": city, "market_type": "lowt",
        "evaluated_at": datetime.now(timezone.utc).isoformat(),
        "signals": [], "error": None, "window": None,
    }

    in_window, window_type = _in_entry_window(city)
    result["window"] = window_type
    if not in_window:
        lh = _local_hour(city)
        result["error"] = (f"Outside entry window (local={lh}h "
                           f"verdict={CITY_WINDOWS.get(city,'BOTH')})")
        return result
    if scan_data.get("error"):
        result["error"] = f"Kalshi error: {scan_data['error']}"
        return result

    brackets = scan_data.get("brackets", [])
    if not brackets:
        result["error"] = "No brackets"
        return result

    b_brackets = sorted([b for b in brackets
                          if _btype(b)=="B" and _bval(b) is not None],
                         key=lambda b: _bval(b))
    t_brackets = [b for b in brackets if _btype(b)=="T"]

    if len(b_brackets) < 3:
        result["error"] = f"Too few B brackets ({len(b_brackets)})"
        return result

    mkt_fcst_bracket = max(b_brackets, key=lambda b: _yes_price(b))
    mkt_fcst_rank    = b_brackets.index(mkt_fcst_bracket)
    mkt_fcst_bval    = _bval(mkt_fcst_bracket)
    t_confirmed      = sum(1 for t in t_brackets if _no_price(t) >= T_CONFIRM_THRESHOLD)

    if T_CONFIRM_MIN > 0 and t_confirmed < T_CONFIRM_MIN:
        result["error"] = f"Insufficient T confirmation ({t_confirmed}<{T_CONFIRM_MIN})"
        return result

    for i, bracket in enumerate(b_brackets):
        ticker = bracket.get("ticker","")
        no_p   = _no_price(bracket)
        yes_p  = _yes_price(bracket)
        dist   = abs(i - mkt_fcst_rank)

        signal = {
            "ticker": ticker, "title": bracket.get("title",""),
            "floor": bracket.get("floor"), "cap": bracket.get("cap"),
            "yes_ask": yes_p, "no_ask": no_p,
            "spread": bracket.get("ob_spread"),
            "yes_depth": bracket.get("ob_yes_depth"),
            "no_depth": bracket.get("ob_no_depth"),
            "volume": bracket.get("volume"),
            "score": 0, "score_detail": [],
            "trade_type": None, "entry_price": no_p,
            "entry_tier": "lowt_main", "market_type": "lowt",
            "skip_reason": None, "market_dist": dist,
            "window": window_type, "t_confirmed": t_confirmed,
        }

        if not (NO_MIN_ENTRY <= no_p <= NO_MAX_ENTRY):
            signal["skip_reason"] = (f"No price out of range "
                                     f"(no={no_p:.2f} range=[{NO_MIN_ENTRY},{NO_MAX_ENTRY}])")
            result["signals"].append(signal)
            continue

        if dist < MIN_MARKET_DIST:
            signal["skip_reason"] = (f"Too close to market forecast "
                                     f"(rank={i} fcst_rank={mkt_fcst_rank} "
                                     f"dist={dist} min={MIN_MARKET_DIST})")
            result["signals"].append(signal)
            continue

        signal["trade_type"]    = "NO"
        signal["max_contracts"] = MAX_CONTRACTS
        signal["score"]         = dist
        signal["score_detail"]  = [f"market_dist={dist}", f"window={window_type}",
                                   f"t_confirmed={t_confirmed}"]
        log.info("LOWT  %s  %s  No=%.2f  dist=%d  fcst=%s  window=%s",
                 city, ticker, no_p, dist,
                 mkt_fcst_bracket.get("ticker","").split("-")[-1], window_type)
        result["signals"].append(signal)

    return result


def run(kalshi_results: dict, city_filter: str = None,
        nws_results: dict = None, paper: bool = False) -> list[dict]:
    import cascade_engine
    evaluations = []
    cities = list(CITY_WINDOWS.keys())
    if city_filter:
        cities = [c for c in cities if c.lower() == city_filter.lower()]
    for city in cities:
        result = evaluate_city_lowt(city, kalshi_results.get(city, {}))
        evaluations.append(result)
    try:
        lowt_cascade = cascade_engine.run_lowt(kalshi_results, city_filter)
        evaluations.extend(lowt_cascade)
        cascade_engine.display(lowt_cascade)
    except Exception as e:
        log.warning("LOWT cascade error (non-fatal): %s", e)
    return evaluations


def display(evaluations: list[dict]) -> None:
    main_evals = [e for e in evaluations
                  if e.get("market_type")=="lowt" and not e.get("cascade")]
    if not main_evals: return
    print(f"\n{'─'*65}")
    print(f"  LOWT Decision Engine")
    print(f"{'─'*65}")
    any_signal = False
    for ev in main_evals:
        city   = ev.get("city","?")
        window = ev.get("window","none")
        signals = [s for s in ev.get("signals",[]) if s.get("trade_type")]
        for s in signals:
            any_signal = True
            floor = s.get("floor"); cap = s.get("cap")
            bracket_str = (f"{floor:.0f}-{cap:.0f}°F" if floor and cap
                           else f"<{cap:.0f}°F" if cap else f">{floor:.0f}°F")
            print(f"  {city:<16} [{window.upper():<7}]  "
                  f"{bracket_str:<12}  NO  ${s['no_ask']:.2f}  "
                  f"dist={s['market_dist']}  T_conf={s['t_confirmed']}")
    if not any_signal:
        print("  No LOWT signals at this time.")


if __name__ == "__main__":
    import os, json
    from pathlib import Path
    parser = argparse.ArgumentParser()
    parser.add_argument("--city",  type=str, default=None)
    parser.add_argument("--paper", action="store_true")
    args = parser.parse_args()
    config_file = Path("data/config.json")
    if config_file.exists():
        config = json.loads(config_file.read_text())
        if config.get("key_id"): os.environ.setdefault("KALSHI_KEY_ID", config["key_id"])
        if config.get("key_file"): os.environ.setdefault("KALSHI_KEY_FILE", config["key_file"])
        os.environ["KALSHI_DEMO"] = "false" if config.get("live_mode") else "true"
    import kalshi_scanner, trader
    client = trader.make_client()
    print("Scanning Kalshi LOWT markets...")
    kalshi_results = kalshi_scanner.scan_all(city_filter=args.city, market_type="lowt")
    evaluations = run(kalshi_results, city_filter=args.city)
    display(evaluations)
