"""
dashboard.py
------------
Mobile-first web dashboard for the WeatherMachine trading system.
Mirrors all 5 tabs of app.py, adapted for phone-browser viewing.
Served over HTTP — access via Tailscale from any device.

Read-only: this process never places orders. The scheduler.py must
be running separately. This dashboard reads data files and calls the
Kalshi API for positions, balance, and settlements.

Usage:
    python dashboard.py              # default port 5050
    python dashboard.py --port 8080
    python dashboard.py --debug      # verbose Flask errors

Access via Tailscale:
    http://<machine-tailscale-ip>:5050

Tip: set LOG_FILE=logs/scheduler.log when running scheduler.py so the
Log tab tails the same file the scheduler writes to.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import sys
import math
import threading
import time as _time
import time
from collections import defaultdict, deque
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

try:
    from flask import Flask, jsonify, render_template_string, request
except ImportError:
    raise SystemExit("Flask required: pip install flask")

sys.path.insert(0, str(Path(__file__).parent))

try:
    from log_setup import get_logger
    from cities import (
        CITIES as _ALL_CITIES,
        SERIES_TO_CITY as _SERIES_TO_CITY,
        CITIES_WEST_TO_EAST as _CITIES_ORDERED,
    )
except ImportError as e:
    raise SystemExit(f"Could not import trading modules: {e}\n"
                     "Run dashboard.py from the project root.")

log = get_logger("dashboard")
app = Flask(__name__)

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
DATA_DIR    = Path("data")
TRADE_LOG   = DATA_DIR / "trade_log.json"
BIAS_FILE   = DATA_DIR / "forecast_bias.json"
OBS_FILE    = DATA_DIR / "lowt_observations.csv"
CONFIG_FILE = DATA_DIR / "config.json"

# ---------------------------------------------------------------------------
# Config + Kalshi client
# ---------------------------------------------------------------------------
def _load_config() -> dict:
    try:
        if CONFIG_FILE.exists():
            return json.loads(CONFIG_FILE.read_text())
    except Exception:
        pass
    return {}

_config = _load_config()
if _config.get("key_id"):   os.environ.setdefault("KALSHI_KEY_ID",   _config["key_id"])
if _config.get("key_file"): os.environ.setdefault("KALSHI_KEY_FILE", _config["key_file"])
os.environ["KALSHI_DEMO"] = "false" if _config.get("live_mode") else "true"

_client      = None
_client_lock = threading.Lock()

# Bracket map cache — avoids hammering Kalshi API on every dashboard refresh
# Cache TTL: 10 minutes (bracket structures rarely change intraday)
_bmap_cache:      dict = {}
_bmap_cache_ts:   float = 0.0
_bmap_cache_lock: threading.Lock = threading.Lock()
_BMAP_CACHE_TTL:  float = 600.0  # seconds

def get_client():
    global _client
    with _client_lock:
        if _client is None:
            try:
                import trader
                _client = trader.make_client(skip_confirmation=True)
                log.info("dashboard: Kalshi client ready (%s)",
                         "LIVE" if not _client.demo else "DEMO")
            except Exception as e:
                log.warning("dashboard: Kalshi client unavailable: %s", e)
    return _client

# ---------------------------------------------------------------------------
# Trade-log helpers
# ---------------------------------------------------------------------------
_tlog_cache: list  = []
_tlog_mtime: float = 0.0

def _load_trade_log() -> list:
    global _tlog_cache, _tlog_mtime
    try:
        if TRADE_LOG.exists():
            mt = TRADE_LOG.stat().st_mtime
            if mt != _tlog_mtime:
                _tlog_cache = json.loads(TRADE_LOG.read_text())
                _tlog_mtime = mt
    except Exception:
        pass
    return _tlog_cache

def _city_from_ticker(ticker: str, bare: bool = False) -> str | None:
    prefix = ticker.split("-")[0]
    city   = _SERIES_TO_CITY.get(prefix)
    if not city:
        return None
    if bare:
        return city
    mtype = "HIGH" if "HIGH" in prefix else "LOW"
    return f"{city} ({mtype})"

def _entry_tier(ticker: str) -> str:
    for e in reversed(_load_trade_log()):
        if e.get("ticker") == ticker:
            return (e.get("entry_tier", "") or "")
    return ""

def _fmt_bracket(bracket: str, mtype: str = "HIGH",
                 bmap_entry: dict = None) -> str:
    """
    Format a bracket code for display using floor_strike from the Kalshi API
    when available (most accurate), falling back to ticker-code heuristics.

    bmap_entry: one entry from _market_brackets_map, i.e.
        {"codes": [all bracket codes in the market], "floors": {code: floor_strike}}

    For B brackets, floor_strike from the API is the actual lower temperature
    boundary. Kalshi's B ticker naming is inconsistent across markets — the
    ticker value can be either the floor or the midpoint of the range. Using
    math.ceil(floor_strike) always gives the correct first whole-degree temp.

    For T brackets, the display is derived from the adjacent B brackets:
      bottom T: ≤(first_B_lo - 1)°
      top T:    ≥(last_B_hi  + 1)°
    where B_lo values are themselves derived from floor_strike when available.
    """
    if not bracket:
        return bracket
    try:
        codes  = (bmap_entry or {}).get("codes", [])
        floors = (bmap_entry or {}).get("floors", {})

        if bracket.startswith("B"):
            if bracket in floors:
                # Use actual floor_strike from API — most reliable
                lo = math.ceil(floors[bracket])
            else:
                # Fallback: Kalshi usually stores the range midpoint as ticker
                # so ticker 66.5 → range [65.5, 67.5) → lo = ceil(65.5) = 66
                # But some markets use floor as ticker (Boston B71.5 → lo=72)
                # Without floor_strike we can't distinguish — use ticker+0.5 heuristic
                f  = float(bracket[1:])
                lo = int(f + 0.5)
            hi = lo + 1
            return f"{lo}–{hi}°"

        elif bracket.startswith("T"):
            v = float(bracket[1:])
            if mtype == "LOW":
                return f"<{v:.0f}°"

            b_codes = [b for b in codes if b.startswith("B")
                       and b[1:].replace(".","").isdigit()]

            if b_codes:
                # Compute lo for each B bracket using floor_strike when available
                b_los = []
                for bc in b_codes:
                    if bc in floors:
                        b_los.append(math.ceil(floors[bc]))
                    else:
                        b_los.append(int(float(bc[1:]) + 0.5))

                first_b_lo = min(b_los)
                last_b_hi  = max(b_los) + 1

                t_codes = [b for b in codes if b.startswith("T")
                           and b[1:].replace(".","").isdigit()]
                t_vals  = sorted([float(b[1:]) for b in t_codes])

                if len(t_vals) >= 2:
                    if v == t_vals[0]:   # bottom T
                        return f"≤{first_b_lo - 1}°"
                    else:                # top T
                        return f"≥{last_b_hi + 1}°"
                elif len(t_vals) == 1:
                    # Only one T in context — use position relative to B range
                    if v < first_b_lo:   # below B range = bottom T
                        return f"≤{first_b_lo - 1}°"
                    else:                # above B range = top T
                        return f"≥{last_b_hi + 1}°"

            # Final fallback — no bracket context at all
            return f"≥{v:.0f}°"

    except (ValueError, TypeError):
        pass
    return bracket

# ---------------------------------------------------------------------------
# Thread-safe TTL cache
# ---------------------------------------------------------------------------
_cache: dict = {}
_clock       = threading.Lock()

def cached(key: str, ttl: int, fn):
    with _clock:
        e = _cache.get(key)
        if e and (time.time() - e["ts"]) < ttl:
            return e["data"]
    r = fn()
    with _clock:
        _cache[key] = {"data": r, "ts": time.time()}
    return r

# ---------------------------------------------------------------------------
# Data fetchers
# ---------------------------------------------------------------------------
def _fetch_nws() -> dict:
    try:
        import nws_feed
        return nws_feed.snapshot()
    except Exception as e:
        log.warning("NWS fetch failed: %s", e)
        return {}

def _fetch_positions() -> list:
    c = get_client()
    if not c: return []
    try:
        import trader
        return trader.sync_from_kalshi(c)
    except Exception as e:
        log.warning("positions fetch failed: %s", e)
        return []

def _fetch_balance() -> float:
    c = get_client()
    if not c: return 0.0
    try:
        import trader
        return trader.get_balance(c)
    except Exception:
        return 0.0

def _fetch_settlements() -> tuple:
    """
    Mirrors PnLTab.load_data() + _populate() from app.py exactly.
    Returns (enriched_list, fills_by_ticker).
    """
    c = get_client()
    if not c: return [], {}
    try:
        # settlements
        all_s, cursor = [], None
        for _ in range(15):
            p = {"limit": 200}
            if cursor: p["cursor"] = cursor
            d = c.get("portfolio/settlements", params=p)
            b = d.get("settlements", [])
            all_s.extend(b); cursor = d.get("cursor")
            if not cursor or len(b) < 200: break

        temp = [s for s in all_s if s.get("ticker","").startswith("KX")
                and ("HIGH" in s.get("ticker","") or "LOWT" in s.get("ticker",""))
                and "TEMPNYCH" not in s.get("ticker","")]

        # fills
        all_f, cursor = [], None
        for _ in range(15):
            p = {"limit": 200}
            if cursor: p["cursor"] = cursor
            d = c.get("portfolio/fills", params=p)
            b = d.get("fills", [])
            all_f.extend(b); cursor = d.get("cursor")
            if not cursor or len(b) < 200: break

        fbt: dict = defaultdict(list)
        for f in all_f:
            if f.get("ticker","").startswith("KX") and \
               ("HIGH" in f.get("ticker","") or "LOWT" in f.get("ticker","")) and \
               "TEMPNYCH" not in f.get("ticker",""):
                fbt[f["ticker"]].append(f)

        sdates = {s["ticker"]: s.get("settled_time","")
                  for s in all_s if s.get("ticker","").startswith("KX")}

        # early exits
        exits = []
        for ticker, fills in fbt.items():
            buys  = [f for f in fills if f.get("action") == "buy"]
            sells = [f for f in fills if f.get("action") == "sell"]
            if not buys or not sells: continue
            st = sdates.get(ticker, "")
            esells = [f for f in sells if not st or f.get("created_time","") < st]
            if not esells: continue
            sides = [f.get("side") for f in buys]
            our   = max(set(sides), key=sides.count)
            obuys = [f for f in buys if f.get("side") == our]
            oes   = [f for f in esells if f.get("side") == our]
            if not oes:
                leg = "yes" if our == "no" else "no"
                oes = [f for f in esells if f.get("side") == leg]
            if not oes: continue
            def fp(f): yp=float(f.get("yes_price_dollars") or 0); return yp if our=="yes" else 1-yp
            bc = sum(float(f.get("count_fp") or 0) for f in obuys)
            sc = sum(float(f.get("count_fp") or 0) for f in oes)
            if bc == 0: continue
            ab = sum(fp(f)*float(f.get("count_fp") or 0) for f in obuys) / bc
            ae = sum(fp(f)*float(f.get("count_fp") or 0) for f in oes)   / max(sc,1)
            nc = int(min(bc, sc))
            fee= sum(float(f.get("fee_cost") or 0) for f in obuys+oes)
            date = sorted(obuys, key=lambda f: f.get("created_time",""))[0].get("created_time","")[:10]
            exits.append({"ticker":ticker,"date":date,"side":our.upper(),
                "contracts":nc,"avg_buy":round(ab,4),"avg_sell":round(ae,4),
                "fee":round(fee,4),"net_pnl":round(ae*nc - ab*nc - fee,4)})

        etickers = {e["ticker"] for e in exits}
        temp = [s for s in temp if s["ticker"] not in etickers]

        # enrich settled
        # Side and cost come from settlement fields (yes/no_count_fp,
        # yes/no_total_cost_dollars) — available for ALL trades regardless of
        # Kalshi's fills historical cutoff. Fills are used only to refine
        # entry_date; settled_time[:10] is the fallback.
        enriched = []
        for s in temp:
            tk  = s.get("ticker",""); res = s.get("market_result","").lower()
            fee = float(s.get("fee_cost") or 0)
            raw = s.get("settled_time","")
            try:
                dt = datetime.fromisoformat(raw.replace("Z","+00:00"))
                sts = dt.astimezone(ZoneInfo("Europe/Lisbon")).strftime("%Y-%m-%d %H:%M")
            except Exception: sts = raw[:16].replace("T"," ") if raw else ""

            yes_c    = float(s.get("yes_count_fp") or 0)
            no_c     = float(s.get("no_count_fp")  or 0)
            yes_cost = float(s.get("yes_total_cost_dollars") or 0)
            no_cost  = float(s.get("no_total_cost_dollars")  or 0)

            if yes_c > 0 and no_c == 0:
                our = "yes"; nc = int(yes_c); cost = round(yes_cost, 4)
            elif no_c > 0 and yes_c == 0:
                our = "no";  nc = int(no_c);  cost = round(no_cost, 4)
            elif yes_c > 0 and no_c > 0:
                if yes_c >= no_c: our = "yes"; nc = int(yes_c); cost = round(yes_cost, 4)
                else:             our = "no";  nc = int(no_c);  cost = round(no_cost, 4)
            else:
                continue  # no position data (e.g. cancelled before fill)

            if nc == 0 or cost == 0: continue

            # Use market date from ticker (e.g. KXHIGHTSEA-26JUN07-B61.5 → 2026-06-07).
            # This ensures tomorrow_sweep entries placed on day D for a day D+1 market
            # appear in the D+1 row, not the D row. Falls back to settled_time date.
            edate = raw[:10]  # fallback: settled_time date
            try:
                parts = tk.split("-")
                if len(parts) >= 2:
                    mdate_parsed = datetime.strptime(parts[1], "%y%b%d")
                    edate = mdate_parsed.strftime("%Y-%m-%d")
            except Exception:
                if tk in fbt:
                    bfs = [f for f in fbt[tk] if f.get("action") == "buy"]
                    if bfs:
                        edate = sorted(bfs, key=lambda f: f.get("created_time",""))[0].get("created_time","")[:10]

            won = (res == our)
            pnl = round(nc - cost - fee, 4) if won else round(-cost - fee, 4)
            enriched.append({"ticker":tk,"date":edate,"settled_ts":sts,"side":our.upper(),
                "contracts":nc,"result":res.upper(),"won":won,"cost":cost,"fee":fee,
                "net_pnl":pnl,"avg_entry":round(cost/nc,4)})

        for ex in exits:
            enriched.append({"ticker":ex["ticker"],"date":ex["date"],"settled_ts":"",
                "side":ex["side"],"contracts":ex["contracts"],"result":"EARLY EXIT",
                "won":False,"cost":round(ex["avg_buy"]*ex["contracts"],4),
                "fee":ex["fee"],"net_pnl":ex["net_pnl"],"avg_entry":ex.get("avg_buy",0)})

        return enriched, dict(fbt)
    except Exception as e:
        log.error("settlements failed: %s", e, exc_info=True)
        return [], {}

get_nws        = lambda: cached("nws",         300, _fetch_nws)
get_positions  = lambda: cached("positions",    30,  _fetch_positions)
get_balance    = lambda: cached("balance",      60,  _fetch_balance)
get_settlements= lambda: cached("settlements", 300,  _fetch_settlements)

# ---------------------------------------------------------------------------
# Pending settlement fetcher
# ---------------------------------------------------------------------------
def _fetch_pending() -> list:
    c = get_client()
    if not c: return []
    try:
        import requests as _req
        raw = c.get("portfolio/positions", params={"count_filter": "position"})
        all_pos = raw.get("market_positions", [])
        candidates = [
            p for p in all_pos
            if float(p.get("position_fp") or 0) != 0
            and p.get("ticker","").startswith("KX")
            and ("HIGH" in p.get("ticker","") or "LOWT" in p.get("ticker",""))
            and "TEMPNYCH" not in p.get("ticker","")
        ]
        if not candidates:
            return []
        tickers = [p["ticker"] for p in candidates]
        try:
            resp = _req.get(
                "https://api.elections.kalshi.com/trade-api/v2/markets",
                params={"tickers": ",".join(tickers)},
                timeout=15,
            ).json()
            market_info = {m["ticker"]: m for m in resp.get("markets", [])}
        except Exception as e:
            log.warning("pending: batch price fetch failed: %s", e)
            market_info = {}
        pending_tickers  = set()
        expiry_by_ticker = {}
        result_by_ticker = {}
        for tk, m in market_info.items():
            status = (m.get("status") or "").lower()
            result = (m.get("result") or "").strip()
            # Two pending states:
            #   "closed"     + no result  : market closed, result not yet published
            #   "determined" + result set : result known, settlement payment pending
            if status in ("closed", "determined"):
                pending_tickers.add(tk)
                expiry_by_ticker[tk] = m.get("expected_expiration_time","")
                result_by_ticker[tk] = result
        pending_pos = [p for p in candidates if p["ticker"] in pending_tickers]
        if not pending_pos:
            return []
        fills_by_ticker: dict = {}
        try:
            resp = c.get("portfolio/fills", params={"limit": 200})
            for f in resp.get("fills", []):
                t = f.get("ticker","")
                if t in pending_tickers:
                    fills_by_ticker.setdefault(t, []).append(f)
        except Exception as e:
            log.warning("pending: fills fetch failed: %s", e)
        enriched = []
        for pos in pending_pos:
            ticker      = pos["ticker"]
            position_fp = float(pos.get("position_fp") or 0)
            side        = "no" if position_fp < 0 else "yes"
            contracts   = int(abs(position_fp))
            avg_cost    = 0.0
            buy_fills   = [
                f for f in fills_by_ticker.get(ticker, [])
                if f.get("action") == "buy" and f.get("side") == side
            ]
            if buy_fills:
                if side == "yes":
                    total_cost = sum(
                        float(f.get("yes_price_dollars") or 0) *
                        float(f.get("count_fp") or 0) for f in buy_fills
                    )
                else:
                    total_cost = sum(
                        (1.0 - float(f.get("yes_price_dollars") or 0)) *
                        float(f.get("count_fp") or 0) for f in buy_fills
                    )
                total_c = sum(float(f.get("count_fp") or 0) for f in buy_fills)
                if total_c > 0:
                    avg_cost = round(total_cost / total_c, 4)
            if avg_cost == 0 and contracts > 0:
                exposure = float(pos.get("market_exposure_dollars") or 0)
                if exposure > 0:
                    avg_cost = round(exposure / contracts, 4)
            raw_exp = expiry_by_ticker.get(ticker, "")
            try:
                dt_exp  = datetime.fromisoformat(raw_exp.replace("Z","+00:00"))
                exp_str = dt_exp.astimezone(ZoneInfo("Europe/Lisbon")).strftime("%H:%M %Z")
            except Exception:
                exp_str = raw_exp[:16] if raw_exp else "unknown"
            br     = ticker.split("-")[-1] if "-" in ticker else ticker
            mt     = "HIGH" if "HIGH" in ticker else "LOW"
            result = result_by_ticker.get(ticker, "")
            if result:
                won        = (result.lower() == side)
                outcome    = "WON ✓"  if won else "LOST ✗"
                outcome_cls = "green" if won else "red"
            else:
                won         = None
                outcome     = "Pending"
                outcome_cls = "yellow"
            enriched.append({
                "ticker":         ticker,
                "market":         _city_from_ticker(ticker) or ticker,
                "city":           _city_from_ticker(ticker, bare=True) or ticker,
                "market_type":    mt,
                "bracket":        _fmt_bracket(br, mt),
                "side":           side.upper(),
                "contracts":      contracts,
                "avg_cost":       avg_cost,
                "cost_total":     round(avg_cost * contracts, 2),
                "est_settlement": exp_str,
                "result":         result,
                "won":            won,
                "outcome":        outcome,
                "outcome_cls":    outcome_cls,
            })
        return enriched
    except Exception as e:
        log.error("pending: fetch failed: %s", e, exc_info=True)
        return []

get_pending = lambda: cached("pending", 300, _fetch_pending)

# ---------------------------------------------------------------------------
# Hourly NYC settlements fetcher
# ---------------------------------------------------------------------------
def _fetch_hourly_settlements() -> list:
    """
    Fetch and enrich settled KXTEMPNYCH (NYC hourly temperature) positions.
    Same enrichment pattern as _fetch_settlements() but filtered to the
    KXTEMPNYCH series only. Returns enriched list newest-first.
    """
    c = get_client()
    if not c: return []
    try:
        all_s, cursor = [], None
        for _ in range(15):
            p = {"limit": 200}
            if cursor: p["cursor"] = cursor
            d = c.get("portfolio/settlements", params=p)
            b = d.get("settlements", [])
            all_s.extend(b); cursor = d.get("cursor")
            if not cursor or len(b) < 200: break

        temp = [s for s in all_s if "KXTEMPNYCH" in s.get("ticker","")]

        all_f, cursor = [], None
        for _ in range(10):
            p = {"limit": 200}
            if cursor: p["cursor"] = cursor
            d = c.get("portfolio/fills", params=p)
            b = d.get("fills", [])
            all_f.extend(b); cursor = d.get("cursor")
            if not cursor or len(b) < 200: break

        fbt: dict = defaultdict(list)
        for f in all_f:
            if "KXTEMPNYCH" in f.get("ticker",""):
                fbt[f["ticker"]].append(f)

        enriched = []
        for s in temp:
            tk  = s.get("ticker",""); res = s.get("market_result","").lower()
            fee = float(s.get("fee_cost") or 0)
            raw = s.get("settled_time","")
            try:
                dt  = datetime.fromisoformat(raw.replace("Z","+00:00"))
                sts = dt.astimezone(ZoneInfo("Europe/Lisbon")).strftime("%Y-%m-%d %H:%M")
            except Exception:
                sts = raw[:16].replace("T"," ") if raw else ""

            yes_c    = float(s.get("yes_count_fp") or 0)
            no_c     = float(s.get("no_count_fp")  or 0)
            yes_cost = float(s.get("yes_total_cost_dollars") or 0)
            no_cost  = float(s.get("no_total_cost_dollars")  or 0)

            if yes_c > 0 and no_c == 0:
                our = "yes"; nc = int(yes_c); cost = round(yes_cost, 4)
            elif no_c > 0 and yes_c == 0:
                our = "no";  nc = int(no_c);  cost = round(no_cost, 4)
            elif yes_c > 0 and no_c > 0:
                if yes_c >= no_c: our = "yes"; nc = int(yes_c); cost = round(yes_cost, 4)
                else:             our = "no";  nc = int(no_c);  cost = round(no_cost, 4)
            else:
                continue

            if nc == 0 or cost == 0: continue

            edate = raw[:10]
            if tk in fbt:
                bfs = [f for f in fbt[tk] if f.get("action") == "buy"]
                if bfs:
                    edate = sorted(bfs, key=lambda f: f.get("created_time",""))[0].get("created_time","")[:10]

            # Extract settlement hour from ticker: KXTEMPNYCH-26JUN2214 -> 14
            mkt_hour = None
            try:
                parts = tk.split("-"); mkt_hour = int(parts[1][-2:]) if len(parts) >= 2 else None
            except Exception:
                pass

            won = (res == our)
            pnl = round(nc - cost - fee, 4) if won else round(-cost - fee, 4)
            enriched.append({
                "ticker":      tk,
                "date":        edate,
                "settled_ts":  sts,
                "side":        our.upper(),
                "contracts":   nc,
                "result":      res.upper(),
                "won":         won,
                "cost":        cost,
                "fee":         fee,
                "net_pnl":     pnl,
                "avg_entry":   round(cost/nc, 4),
                "mkt_hour":    mkt_hour,
                "threshold_f": None,
                "forecast_f":  None,
                "dist_f":      None,
            })

        # Enrich with trade_log score_detail (threshold, forecast, dist)
        trades = _load_trade_log()
        tlog   = {t.get("ticker",""): t for t in trades
                  if t.get("entry_tier") == "hourly_nyc"}
        for e in enriched:
            for item in tlog.get(e["ticker"], {}).get("score_detail", []):
                if not isinstance(item, str): continue
                if item.startswith("threshold="):
                    try: e["threshold_f"] = float(item.split("=")[1].rstrip("degF").rstrip("°F"))
                    except Exception: pass
                elif item.startswith("forecast="):
                    try: e["forecast_f"] = float(item.split("=")[1].rstrip("degF").rstrip("°F"))
                    except Exception: pass
                elif item.startswith("dist="):
                    try: e["dist_f"] = float(item.split("=")[1].rstrip("degF").rstrip("°F"))
                    except Exception: pass

        enriched.sort(key=lambda x: x["date"], reverse=True)
        return enriched
    except Exception as e:
        log.error("hourly settlements failed: %s", e, exc_info=True)
        return []

get_hourly_settlements = lambda: cached("hourly_settlements", 120, _fetch_hourly_settlements)

# ---------------------------------------------------------------------------
# In-process log buffer
# ---------------------------------------------------------------------------
class _LogBuf(logging.Handler):
    def __init__(self, n=600):
        super().__init__(); self._l=[]; self._n=n; self._lk=threading.Lock()
        self.setFormatter(logging.Formatter(
            "%(asctime)s  [%(name)-16s]  %(levelname)-7s  %(message)s"))
    def emit(self, r):
        with self._lk:
            self._l.append(self.format(r))
            if len(self._l) > self._n: self._l = self._l[-self._n:]
    def tail(self, n=250):
        with self._lk: return list(self._l[-n:])

_lb = _LogBuf()
logging.getLogger().addHandler(_lb)

# ---------------------------------------------------------------------------
# API — /api/status
# ---------------------------------------------------------------------------
def _market_brackets_map(tickers: list[str]) -> dict[str, dict]:
    """
    Build {market_key: {"codes": [str], "floors": {code: float}}} by fetching
    ALL brackets for each unique market from the Kalshi API.

    Results are cached for 10 minutes to avoid contributing to rate limiting.
    Falls back to extracting bracket codes from the provided tickers if the
    API call fails, in which case "floors" will be empty.

    market_key = first two dash-separated parts, e.g. "KXHIGHTSEA-26JUN07"
    """
    global _bmap_cache, _bmap_cache_ts

    # Collect unique market keys from the provided tickers
    keys: set[str] = set()
    for tk in tickers:
        parts = str(tk).split("-")
        if len(parts) >= 3:
            keys.add("-".join(parts[:2]))

    now = _time.monotonic()

    with _bmap_cache_lock:
        # Return cached result if fresh and covers all requested keys
        if (now - _bmap_cache_ts < _BMAP_CACHE_TTL
                and keys.issubset(_bmap_cache.keys())):
            return {k: _bmap_cache[k] for k in keys if k in _bmap_cache}

        # Fetch only the keys not already in cache (or if cache is stale)
        if now - _bmap_cache_ts >= _BMAP_CACHE_TTL:
            _bmap_cache = {}
            _bmap_cache_ts = now

        keys_to_fetch = keys - _bmap_cache.keys()

    result: dict[str, dict] = {}

    try:
        client = get_client()
        for event_key in keys_to_fetch:
            try:
                resp = client.get("/markets", params={
                    "event_ticker": event_key,
                    "status":       "open",
                    "limit":        25,
                })
                markets = resp.get("markets", [])
                codes: list[str] = []
                floors: dict[str, float] = {}
                for m in markets:
                    tk = m.get("ticker", "")
                    br = tk.split("-")[-1] if "-" in tk else tk
                    if br:
                        codes.append(br)
                        fs = m.get("floor_strike")
                        if fs is not None:
                            try:
                                floors[br] = float(fs)
                            except (TypeError, ValueError):
                                pass
                result[event_key] = {"codes": codes, "floors": floors}
            except Exception:
                pass
    except Exception:
        pass

    # Fallback: populate from provided tickers for any keys not already fetched
    for tk in tickers:
        parts = str(tk).split("-")
        if len(parts) >= 3:
            key = "-".join(parts[:2])
            br  = parts[-1]
            if key not in result:
                result.setdefault(key, {"codes": [], "floors": {}})
                if br not in result[key]["codes"]:
                    result[key]["codes"].append(br)

    # Merge new results into cache
    with _bmap_cache_lock:
        _bmap_cache.update(result)

    # Return combined result from cache + new fetches
    with _bmap_cache_lock:
        return {k: _bmap_cache.get(k, result.get(k, {"codes": [], "floors": {}}))
                for k in keys}


@app.route("/api/status")
def api_status():
    bal = get_balance(); pos = get_positions()
    unr = sum(p.get("unrealised_pnl",0) for p in pos)
    cur = sum(p.get("current_price",0)*p.get("contracts",1) for p in pos)
    # Per-engine capital breakdown from EngineCapital singleton
    engines = {}
    available = 0.0
    try:
        import trader
        # Pass the client so the EngineCapital singleton refreshes its balance
        # against the live account. Without a client it defaults to $0 balance
        # -> all budgets $0 -> the capital strip shows empty. get_engine_capital
        # preserves per-engine _deployed while recomputing budgets.
        cap = trader.get_engine_capital(client=get_client())
        for e in trader.ENGINE_ALLOCATIONS:
            rem = round(cap.remaining(e), 2)
            bud = round(cap.budget(e), 2)
            engines[e] = {"remaining": rem, "budget": bud}
            available += rem
    except Exception as _e:
        # Keep the strip's empty-fallback behavior, but never fail silently —
        # a blank capital strip previously masked a NameError here for a long
        # time. Log so any future failure in this block is visible.
        log.warning("dashboard /api/status: per-engine capital unavailable: %s", _e)
        engines = {}
        available = 0.0
    pending     = get_pending()
    pend_cost   = round(sum(p.get("cost_total",0) for p in pending), 2)
    return jsonify({
        "balance":      bal,
        "available":    round(available, 2),
        "portfolio":    round(bal+cur,2),  "unrealised": round(unr,2),
        "mode":         "LIVE" if os.environ.get("KALSHI_DEMO","true")=="false" else "DEMO",
        "open":         len(pos),
        "pending":      len(pending),
        "pending_cost": pend_cost,
        "engines":      engines,
    })

# ---------------------------------------------------------------------------
# API — /api/positions
# ---------------------------------------------------------------------------
@app.route("/api/positions")
def api_positions():
    out = []
    positions = get_positions()
    _bmap = _market_brackets_map([p.get("ticker","") for p in positions])
    for p in positions:
        tk = p.get("ticker",""); mt = "HIGH" if "HIGH" in tk else "LOW"
        br = tk.split("-")[-1] if "-" in tk else tk
        ti = _entry_tier(tk)
        eg = "CASCADE" if "cascade" in ti.lower() else ("MAIN" if not ti else ti.upper())
        _mkey = "-".join(tk.split("-")[:2]) if "-" in tk else tk
        out.append({"ticker":tk,"market":_city_from_ticker(tk) or tk,
            "city":_city_from_ticker(tk,bare=True) or tk,"market_type":mt,
            "bracket":_fmt_bracket(br,mt,_bmap.get(_mkey)),"engine":eg,"side":p.get("side","").upper(),
            "contracts":p.get("contracts",1),"avg_cost":round(p.get("avg_cost",0),2),
            "current_price":round(p.get("current_price",0),2),
            "unrealised":round(p.get("unrealised_pnl",0),2),
            "last_updated":p.get("last_updated",""),"live":p.get("live",True)})
    return jsonify(out)

# ---------------------------------------------------------------------------
# API — /api/cities
# ---------------------------------------------------------------------------
@app.route("/api/cities")
def api_cities():
    nws = get_nws(); out = []
    for city in _CITIES_ORDERED:
        tz  = _ALL_CITIES.get(city,{}).get("tz","UTC")
        now = datetime.now(ZoneInfo(tz)); h = now.hour
        d   = nws.get(city,{})
        ha  = 9 <= h < 15; la = h < 8 or h >= 22
        win = ("HIGH + LOWT" if (ha and la) else "HIGH ▲" if ha else "LOWT ▼" if la else "between windows")
        out.append({"city":city,"local_time":now.strftime("%H:%M"),"tz_abbr":now.strftime("%Z"),
            "local_hour":h,"obs_hi":d.get("observed_high_f"),"fcst_hi":d.get("forecast_high_f"),
            "obs_lo":d.get("observed_low_f"),"fcst_lo":d.get("forecast_low_f"),
            "now":d.get("current_temp_f"),
            "window":win,"high_active":ha,"lowt_active":la})
    return jsonify(out)

# ---------------------------------------------------------------------------
# API — /api/session
# ---------------------------------------------------------------------------
@app.route("/api/session")
def api_session():
    positions = get_positions(); trades = _load_trade_log()
    tlog = {t.get("ticker",""): t for t in reversed(trades)}
    entries = []
    _bmap2 = _market_brackets_map([p.get("ticker","") for p in positions])
    for p in positions:
        tk = p.get("ticker",""); t = tlog.get(tk,{})
        ti = (t.get("entry_tier","") or "").lower()
        eg = "CASCADE" if "cascade" in ti else ("MAIN" if not ti else ti.upper())
        sc = t.get("score",0); mt = "HIGH" if "HIGH" in tk else "LOW"
        br = tk.split("-")[-1] if "-" in tk else tk
        pa = t.get("placed_at","")
        _mkey2 = "-".join(tk.split("-")[:2]) if "-" in tk else tk
        entries.append({"ticker":tk,"market":_city_from_ticker(tk) or tk,
            "city":_city_from_ticker(tk,bare=True) or tk,"market_type":mt,
            "bracket":_fmt_bracket(br,mt,_bmap2.get(_mkey2)),"engine":eg,"side":p.get("side","").upper(),
            "contracts":p.get("contracts",1),"avg_cost":round(p.get("avg_cost",0),2),
            "unrealised":round(p.get("unrealised_pnl",0),2),
            "score":f"{sc}/5" if not ti else "—",
            "entered_at":pa[:16].replace("T"," ") if pa else "—","status":"Open"})
    unr = sum(e["unrealised"] for e in entries)
    ms  = [tlog.get(e["ticker"],{}).get("score",0) for e in entries
           if not (tlog.get(e["ticker"],{}).get("entry_tier","") or "")]
    return jsonify({"entries":len(entries),"open":len(entries),"stopped":0,
        "avg_score":f"{sum(ms)/len(ms):.1f}/5" if ms else "—",
        "unrealised":round(unr,2),"positions":entries})

# ---------------------------------------------------------------------------
# API — /api/performance
# ---------------------------------------------------------------------------
@app.route("/api/performance")
def api_performance():
    enriched, _ = get_settlements()
    if not enriched:
        return jsonify({"stats":{},"by_day":[],"chart":{"equity":[],"win_rate":[]},"all_settlements":[]})

    # Win/loss accounting.
    # An EARLY EXIT is a position closed before settlement — it is neither a
    # predictive win nor a predictive loss, so it is excluded from the win-rate
    # denominator. Its realized PnL is still fully counted in net_pnl. We surface
    # the exit count so every row reconciles: wins + losses + exits == total.
    def _is_exit(e) -> bool:
        return e.get("result") == "EARLY EXIT"

    def _wr(items) -> float:
        """Win rate over settled (non-exit) positions only: wins/(wins+losses)."""
        settled = [t for t in items if not _is_exit(t)]
        return round(sum(1 for t in settled if t["won"]) / len(settled) * 100, 1) \
            if settled else 0.0

    total    = len(enriched)
    wins     = [e for e in enriched if e["won"] and not _is_exit(e)]
    losses   = [e for e in enriched if not e["won"] and not _is_exit(e)]
    exits    = [e for e in enriched if _is_exit(e)]
    win_rate = _wr(enriched)
    net_pnl  = round(sum(e["net_pnl"] for e in enriched),2)
    fees     = round(sum(e["fee"]     for e in enriched),2)
    by_day: dict = defaultdict(list)
    for e in enriched: by_day[e["date"]].append(e)
    dpnls     = {d: round(sum(t["net_pnl"] for t in v),2) for d,v in by_day.items()}
    best_day  = max(dpnls.values(), default=0)
    worst_day = min(dpnls.values(), default=0)

    cum=0.0; day_rows=[]
    for day in sorted(by_day.keys()):
        ts=by_day[day]
        dw=[t for t in ts if t["won"] and not _is_exit(t)]
        dl=[t for t in ts if not t["won"] and not _is_exit(t)]
        dx=[t for t in ts if _is_exit(t)]
        dpnl=round(sum(t["net_pnl"] for t in ts),2); cum+=dpnl
        day_rows.append({"date":day,"trades":len(ts),"wins":len(dw),
            "losses":len(dl),
            "stopped":len(dx),
            "win_pct":f"{_wr(ts)}%",
            "net_pnl":dpnl,"cum_pnl":round(cum,2)})
    day_rows.reverse()

    sorted_days = sorted(by_day.keys())
    cum=0.0; equity=[]
    for day in sorted_days:
        cum += sum(e["net_pnl"] for e in by_day[day])
        equity.append({"x":day,"y":round(cum,4)})

    win_win = deque(); wr_data=[]
    for day in sorted_days:
        win_win.append(by_day[day])
        if len(win_win) > 7: win_win.popleft()
        dt_ = [t for batch in win_win for t in batch]
        settled_ = [t for t in dt_ if not _is_exit(t)]
        if settled_:
            wr_data.append({"x":day,
                "y":round(sum(1 for t in settled_ if t["won"])/len(settled_)*100,1)})

    def rl(e):
        if e.get("result")=="EARLY EXIT": return "EXIT ↩","yellow"
        return ("WON ✓","green") if e["won"] else ("LOST ✗","red")

    all_s=[]
    for e in sorted(enriched, key=lambda x: x["date"], reverse=True)[:300]:
        label,cls=rl(e)
        all_s.append({**e,"result_label":label,"result_class":cls,
            "market_label":_city_from_ticker(e["ticker"]) or e["ticker"]})

    return jsonify({"stats":{"total":total,"win_rate":win_rate,
        "wins":len(wins),"losses":len(losses),"exits":len(exits),
        "net_pnl":net_pnl,
        "total_fees":fees,"best_day":best_day,"worst_day":worst_day},
        "by_day":day_rows,"chart":{"equity":equity,"win_rate":wr_data},
        "all_settlements":all_s})

# ---------------------------------------------------------------------------
# API — /api/city/<city>
# ---------------------------------------------------------------------------
@app.route("/api/city/<path:city>")
def api_city(city: str):
    enriched, fills_by_ticker = get_settlements(); trades = _load_trade_log()
    bias_data: dict = {}
    try:
        if BIAS_FILE.exists():
            raw=json.loads(BIAS_FILE.read_text())
            for c,v in raw.items():
                bias_data[c]=v if isinstance(v,dict) else {"bias":float(v),"stddev":0.0}
    except Exception: pass

    obs_hi=None; conv_hour: dict={}
    try:
        if OBS_FILE.exists():
            with open(OBS_FILE,newline="",encoding="utf-8") as f:
                for row in csv.DictReader(f):
                    if row.get("city")!=city: continue
                    try:
                        oh=row.get("observed_high_f")
                        if oh: obs_hi=float(oh)
                    except (ValueError,TypeError): pass
                    tk=row.get("ticker","")
                    if tk and tk not in conv_hour:
                        try:
                            if float(row.get("no_price") or 0)>=0.97:
                                lh=row.get("local_hour")
                                if lh: conv_hour[tk]=int(float(lh))
                        except (ValueError,TypeError): pass
    except Exception: pass

    tlog_by_ticker = {t.get("ticker",""): t for t in trades if t.get("city")==city}
    enriched_city  = [{**e,"_src":"settled"} for e in enriched
                      if _city_from_ticker(e.get("ticker",""),bare=True)==city]
    for item in enriched_city:
        if not item.get("placed_at"):
            tl=tlog_by_ticker.get(item.get("ticker",""),{})
            if tl.get("placed_at"): item["placed_at"]=tl["placed_at"]

    etickers={e["ticker"] for e in enriched_city}; seen=set()
    open_trades=[]
    for t in sorted(trades, key=lambda x: x.get("placed_at","") or ""):
        if t.get("city")!=city: continue
        tk=t.get("ticker","")
        # An entry only counts as a live open position if it actually filled.
        # Placed-but-never-filled orders (price moved, order left hanging) get
        # written to trade_log.json but have no fill and no held settlement —
        # they must NOT render as perpetual OPEN. A genuinely-open position has
        # a fill but no settlement yet, so the fills check keeps those.
        if tk not in etickers and tk not in seen and fills_by_ticker.get(tk):
            seen.add(tk); open_trades.append({**t,"_src":"open"})

    all_items=sorted(enriched_city+open_trades,
        key=lambda i:(i.get("placed_at") or i.get("date") or ""),reverse=True)

    def outcome(i):
        if i["_src"]=="open": return "open"
        if i.get("result")=="EARLY EXIT": return "exit"
        return "win" if i.get("won") else "loss"
    def ipnl(i): return float(i.get("net_pnl",0) or 0) if i["_src"]!="open" else 0.0

    settled=[i for i in all_items if outcome(i) in ("win","loss","exit")]
    wins_l =[i for i in settled if outcome(i)=="win"]
    wr_pct = len(wins_l)/len(settled)*100 if settled else 0
    total_pnl=sum(ipnl(i) for i in all_items)
    conv_hours=[conv_hour[i.get("ticker","")] for i in all_items if i.get("ticker","") in conv_hour]
    avg_conv=f"{sum(conv_hours)/len(conv_hours):.1f}h" if conv_hours else "—"
    be=bias_data.get(city)
    bias_str=f"{be['bias']:+.2f}°F" if be else "—"
    obs_str=f"{obs_hi:.1f}°F" if obs_hi is not None else "—"

    by_ds: dict=defaultdict(lambda: {"wins":0,"total":0})
    for i in settled:
        d=(i.get("placed_at","") or i.get("date",""))[:10]
        if d: by_ds[d]["total"]+=1; by_ds[d]["wins"]+= 1 if outcome(i)=="win" else 0
    dk=sorted(by_ds.keys()); rolling_wr=[]
    for idx,d in enumerate(dk):
        w=sum(by_ds[dd]["wins"]  for dd in dk[max(0,idx-6):idx+1])
        t_=sum(by_ds[dd]["total"] for dd in dk[max(0,idx-6):idx+1])
        rolling_wr.append({"x":d,"y":round(w/t_*100,1) if t_ else None})

    dated_pnl=sorted([(( i.get("placed_at") or i.get("date") or "")[:10],ipnl(i))
        for i in all_items if i["_src"]!="open" and (i.get("placed_at") or i.get("date"))],
        key=lambda x: x[0])
    running=0.0; cum_pnl=[]
    for d,v in dated_pnl: running+=v; cum_pnl.append({"x":d,"y":round(running,4)})

    tz_name=_ALL_CITIES.get(city,{}).get("tz"); by_hour: dict=defaultdict(list)
    for i in all_items:
        pa=i.get("placed_at","") or ""
        if not pa: continue
        try:
            dt=(datetime.fromisoformat(pa.replace("Z","+00:00")) if ("T" in pa or len(pa)>10)
                else datetime.fromisoformat(pa+"T12:00:00+00:00"))
            if tz_name: dt=dt.astimezone(ZoneInfo(tz_name))
            by_hour[dt.hour].append(ipnl(i))
        except Exception: pass
    hours_data=[{"hour":h,"avg":round(sum(v)/len(v),4)} for h,v in sorted(by_hour.items())]

    omap={"win":("WON ✓","green"),"loss":("LOST ✗","red"),
          "exit":("EXIT ↩","yellow"),"open":("OPEN","dim")}
    _bmap3 = _market_brackets_map([i.get("ticker","") for i in all_items])
    pos_list=[]
    for i in all_items:
        tk=i.get("ticker","") or ""; br=tk.split("-")[-1] if "-" in tk else tk
        mt="HIGH" if "HIGH" in tk else "LOW"
        tl=tlog_by_ticker.get(tk,{}); ti=(tl.get("entry_tier","") or "").lower()
        eg="CASCADE" if "cascade" in ti else "MAIN"
        out=outcome(i); pnl=ipnl(i)
        ep=i.get("avg_entry") or float(i.get("entry_price",0) or 0)
        xs=("—" if out=="open" else
            f"${ep+pnl/max(i.get('contracts',1),1):.2f}" if out=="exit" else
            "$1.00" if i.get("won") else "$0.00")
        ol,oc=omap.get(out,(out.upper(),"dim"))
        _mkey3 = "-".join(tk.split("-")[:2]) if "-" in tk else tk
        pos_list.append({"date":(i.get("placed_at") or i.get("date") or "")[:10],
            "bracket":_fmt_bracket(br,mt,_bmap3.get(_mkey3)),"market_type":mt,"engine":eg,
            "side":(i.get("side","NO") or "NO").upper(),
            "entry":f"${ep:.2f}","exit":xs,"pnl":round(pnl,2),
            "contracts":i.get("contracts",1),"outcome":out,
            "outcome_label":ol,"outcome_class":oc})

    return jsonify({"stats":{"win_rate":round(wr_pct,1),"total_pnl":round(total_pnl,2),
        "positions":len(all_items),"avg_conv":avg_conv,"bias":bias_str,"obs_hi":obs_str},
        "chart":{"rolling_wr":rolling_wr,"cum_pnl":cum_pnl,"by_hour":hours_data},
        "positions":pos_list})

# ---------------------------------------------------------------------------
# API — /api/log
# ---------------------------------------------------------------------------
@app.route("/api/log")
def api_log():
    n = min(int(request.args.get("n", 200)), 500)
    # Try journalctl first — gives exact same output as the command line
    try:
        import subprocess
        result = subprocess.run(
            ["journalctl", "-u", "weathermachine", f"-n{n}", "--no-pager",
             "--output=short-iso"],
            capture_output=True, text=True, timeout=5
        )
        if result.returncode == 0 and result.stdout.strip():
            lines = result.stdout.splitlines()
            return jsonify({"lines": lines, "source": "journalctl"})
    except Exception:
        pass
    # Fallback: log file or in-process buffer
    lp = os.environ.get("LOG_FILE") or _config.get("log_file")
    if lp:
        p = Path(lp)
        if p.exists():
            try:
                lines = p.read_text(encoding="utf-8", errors="replace").splitlines()
                return jsonify({"lines": lines[-n:], "source": str(p)})
            except Exception:
                pass
    return jsonify({"lines": _lb.tail(n), "source": "in-process"})


# ---------------------------------------------------------------------------
# API — /api/pending
# ---------------------------------------------------------------------------
@app.route("/api/pending")
def api_pending():
    return jsonify(get_pending())

# ---------------------------------------------------------------------------
# API — /api/hourly
# ---------------------------------------------------------------------------
@app.route("/api/hourly")
def api_hourly():
    # Open hourly positions
    all_positions = get_positions()
    open_pos = [p for p in all_positions if "KXTEMPNYCH" in p.get("ticker","")]
    trades   = _load_trade_log()
    tlog     = {t.get("ticker",""): t for t in trades
                if t.get("entry_tier") == "hourly_nyc"}

    open_rows = []
    for p in open_pos:
        tk  = p.get("ticker","")
        tl  = tlog.get(tk, {})
        threshold_f = None; forecast_f = None; dist_f = None
        for item in tl.get("score_detail", []):
            if not isinstance(item, str): continue
            if item.startswith("threshold="):
                try: threshold_f = float(item.split("=")[1].rstrip("°F"))
                except Exception: pass
            elif item.startswith("forecast="):
                try: forecast_f = float(item.split("=")[1].rstrip("°F"))
                except Exception: pass
            elif item.startswith("dist="):
                try: dist_f = float(item.split("=")[1].rstrip("°F"))
                except Exception: pass
        mkt_hour = None
        try:
            _p = tk.split("-"); mkt_hour = int(_p[1][-2:]) if len(_p) >= 2 else None
        except Exception: pass
        pa = tl.get("placed_at","")
        open_rows.append({
            "ticker":      tk,
            "side":        p.get("side","").upper(),
            "contracts":   p.get("contracts", 1),
            "avg_cost":    round(p.get("avg_cost", 0), 2),
            "unrealised":  round(p.get("unrealised_pnl", 0), 2),
            "mkt_hour":    mkt_hour,
            "threshold_f": threshold_f,
            "forecast_f":  forecast_f,
            "dist_f":      dist_f,
            "entered_at":  pa[:16].replace("T"," ") if pa else "—",
        })

    # Settled history
    enriched = get_hourly_settlements()
    total    = len(enriched)
    wins     = [e for e in enriched if e["won"]]
    win_rate = round(len(wins) / total * 100, 1) if total else 0
    net_pnl  = round(sum(e["net_pnl"] for e in enriched), 2)
    fees     = round(sum(e["fee"]     for e in enriched), 2)

    # By-hour breakdown
    by_hour: dict = defaultdict(lambda: {"wins": 0, "total": 0, "pnl": 0.0})
    for e in enriched:
        h = e.get("mkt_hour")
        if h is None: continue
        by_hour[h]["total"] += 1
        by_hour[h]["pnl"]   += e["net_pnl"]
        if e["won"]: by_hour[h]["wins"] += 1
    hour_rows = [
        {"hour": h, "label": f"{h:02d}:00 EDT",
         "total": v["total"], "wins": v["wins"],
         "losses": v["total"] - v["wins"],
         "win_pct": f"{round(v['wins']/v['total']*100,1)}%" if v["total"] else "—",
         "net_pnl": round(v["pnl"], 2)}
        for h, v in sorted(by_hour.items())
    ]

    # By-day breakdown
    by_day: dict = defaultdict(lambda: {"wins": 0, "total": 0, "pnl": 0.0})
    for e in enriched:
        d = e.get("date","")
        if not d: continue
        by_day[d]["total"] += 1
        by_day[d]["pnl"]   += e["net_pnl"]
        if e["won"]: by_day[d]["wins"] += 1
    cum = 0.0; day_rows = []
    for day in sorted(by_day.keys()):
        v = by_day[day]; cum += v["pnl"]
        day_rows.append({
            "date": day, "total": v["total"], "wins": v["wins"],
            "losses": v["total"] - v["wins"],
            "win_pct": f"{round(v['wins']/v['total']*100,1)}%" if v["total"] else "—",
            "net_pnl": round(v["pnl"], 2), "cum_pnl": round(cum, 2)})
    day_rows.reverse()

    # Settlement rows
    settled_rows = []
    for e in enriched[:200]:
        settled_rows.append({
            **e,
            "result_label": "WON ✓" if e["won"] else "LOST ✗",
            "result_class": "green" if e["won"] else "red",
            "thresh_label": f"{e['threshold_f']:.0f}°F" if e.get("threshold_f") is not None else "—",
            "fcst_label":   f"{e['forecast_f']:.1f}°F"  if e.get("forecast_f")  is not None else "—",
            "dist_label":   f"{e['dist_f']:.1f}°F"      if e.get("dist_f")      is not None else "—",
            "hour_label":   f"{e['mkt_hour']:02d}:00 EDT" if e.get("mkt_hour") is not None else "—",
        })

    return jsonify({
        "open":    open_rows,
        "stats":   {"total": total, "win_rate": win_rate, "net_pnl": net_pnl,
                    "total_fees": fees, "open_count": len(open_rows)},
        "by_hour": hour_rows,
        "by_day":  day_rows,
        "settled": settled_rows,
    })


# ---------------------------------------------------------------------------
# HTML template
# ---------------------------------------------------------------------------
_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>⛅ WeatherMachine</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:ital,wght@0,300;0,400;0,500;0,600;0,700;1,400&display=swap" rel="stylesheet">
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/d3@7.9.0/dist/d3.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/topojson-client@3.1.0/dist/topojson-client.min.js"></script>
<style>
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
:root{
  --bg:#0b0d12;--panel:#111318;--panel2:#161920;--alt:#13161e;
  --ac:#00d4a0;--acd:#009970;--ac2:rgba(0,212,160,.12);
  --red:#ff4d6a;--red2:rgba(255,77,106,.12);
  --yel:#f5c518;--yel2:rgba(245,197,24,.12);
  --blu:#4d9fff;
  /* per-engine colors — shared by Session badges and the capital strip */
  --eng-sweep:#ff9f43;--eng-peak:#22d3ee;--eng-topup:#c084fc;
  --eng-econv:#f472b6;--eng-hourly:#2dd4bf;--eng-lowt:#fbbf24;
  --pri:#dde2ee;--sec:#5a6278;--ter:#3a3f52;
  --bdr:#1e2230;--bdr2:#252a3a;
  --f:'JetBrains Mono','Consolas',monospace;
  --r:6px;--r2:10px;
  --hdr:52px;--tab:42px;
}
html,body{height:100%;background:var(--bg);color:var(--pri);font-family:var(--f);font-size:13px;-webkit-font-smoothing:antialiased}
a{color:inherit;text-decoration:none}
button,select,input{font-family:var(--f)}

/* ── Layout ── */
#app{display:flex;flex-direction:column;min-height:100vh}

/* ── Header ── */
#hdr{height:var(--hdr);background:var(--panel);border-bottom:1px solid var(--bdr);
  display:flex;align-items:center;padding:0 20px;gap:16px;flex-shrink:0;position:sticky;top:0;z-index:200}
#logo{color:var(--ac);font-weight:700;font-size:15px;letter-spacing:2px;flex:1}
#logo small{color:var(--sec);font-weight:400;font-size:9px;letter-spacing:2px;display:block;margin-top:1px}
.mode-badge{font-size:9px;letter-spacing:2px;padding:3px 8px;border-radius:4px;font-weight:700;border:1px solid var(--ac);color:var(--ac)}
.mode-badge.live{border-color:var(--yel);color:var(--yel)}
#hdr-status{color:var(--sec);font-size:11px;min-width:160px;text-align:right}
#hdr-status.polling{color:var(--ac)}

/* ── Tab bar ── */
#tabbar{height:var(--tab);background:var(--panel);border-bottom:1px solid var(--bdr);
  display:flex;align-items:stretch;padding:0 8px;flex-shrink:0;overflow-x:auto;
  position:sticky;top:var(--hdr);z-index:198}
.tb{padding:0 16px;font-size:11px;letter-spacing:1px;color:var(--sec);background:none;
  border:none;border-bottom:2px solid transparent;cursor:pointer;white-space:nowrap;
  transition:color .15s,border-color .15s}
.tb:hover{color:var(--pri)}
.tb.on{color:var(--ac);border-bottom-color:var(--ac)}

/* ── Content ── */
#content{flex:1;overflow:hidden}
.tab{display:none;height:100%;overflow-y:auto;padding:20px}
.tab.on{display:block}

/* ── Section heading ── */
.sh{color:var(--sec);font-size:9px;letter-spacing:2.5px;text-transform:uppercase;
  margin:24px 0 12px;padding-bottom:8px;border-bottom:1px solid var(--bdr)}
.sh:first-child{margin-top:0}

/* ── Balance bar ── */
#bal-bar{display:flex;gap:24px;padding:14px 20px;background:var(--panel);
  border-bottom:1px solid var(--bdr);flex-wrap:wrap;flex-shrink:0;
  position:sticky;top:var(--hdr);z-index:199}
.bal-item{}
.bal-k{color:var(--sec);font-size:9px;letter-spacing:2px;text-transform:uppercase;margin-bottom:3px}
.bal-v{font-size:16px;font-weight:600;color:var(--pri)}
.bal-v.g{color:var(--ac)}
.bal-v.r{color:var(--red)}

/* ── City grid ── */
#cgrid{display:grid;grid-template-rows:repeat(4,1fr);grid-auto-flow:column;gap:8px;margin-bottom:20px}
@media(max-width:900px){#cgrid{grid-template-rows:repeat(5,1fr)}}
@media(max-width:600px){#cgrid{grid-template-rows:repeat(10,1fr)}}
.cc{background:var(--panel);border:1px solid var(--bdr);border-radius:var(--r2);
  padding:12px 14px;cursor:pointer;transition:border-color .2s,background .2s}
.cc:hover{border-color:var(--ter);background:var(--panel2)}
.cc.ha{border-color:var(--acd)}.cc.la{border-color:#2a4a88}
.cn{color:#a8c4e0;font-size:12px;font-weight:600;letter-spacing:.4px;text-transform:uppercase}
.ctime{color:var(--pri);font-size:11px;font-weight:500;margin:4px 0 2px}
.cnow{color:var(--ac);font-size:12px;font-weight:600}
.chi{color:var(--ac);font-size:11px}
.clo{color:#7a90b8;font-size:11px}
.cwin{font-size:10px;margin-top:4px;color:var(--sec)}
.cwin.ha{color:var(--ac)}.cwin.la{color:var(--blu)}
/* ── Map styles ── */
#map-wrap{position:relative;width:100%;margin-bottom:20px;user-select:none}
#us-svg{width:100%;display:block;border-radius:8px}
.map-state{fill:#161a24;stroke:#252d42;stroke-width:.5}
.map-chip{position:absolute;transform:translate(-50%,-50%);cursor:pointer;
  background:rgba(15,18,28,.88);border:1px solid #252d42;border-radius:6px;
  padding:4px 6px;min-width:54px;text-align:center;
  transition:border-color .15s,background .15s;backdrop-filter:blur(4px)}
.map-chip:hover{border-color:var(--ac);background:rgba(22,26,38,.96);z-index:10}
.map-chip.active-hi{border-color:var(--acd)}
.map-chip.active-lo{border-color:#2a4a88}
.map-chip.active-both{border-color:var(--ac)}
.chip-name{font-size:9px;font-weight:700;letter-spacing:.8px;color:#a8c4e0;
  text-transform:uppercase;line-height:1}
.chip-now{font-size:13px;font-weight:700;color:var(--ac);line-height:1.3}
.chip-fcst{font-size:9px;color:#7a90b8;line-height:1}
/* chip-now = observed high so far today (big teal)  */
/* chip-fcst = forecast high (small blue, ▲ prefix)  */
.chip-dot{width:5px;height:5px;border-radius:50%;background:var(--acd);
  display:inline-block;margin-right:3px;vertical-align:middle}

/* ── Positions table ── */
.tbl-wrap{overflow-x:auto;border:1px solid var(--bdr);border-radius:var(--r2);margin-bottom:20px}
table{width:100%;border-collapse:collapse;font-size:12px;white-space:nowrap}
thead th{background:var(--bg);color:var(--sec);padding:10px 14px;text-align:left;
  font-size:9px;letter-spacing:1.5px;text-transform:uppercase;border-bottom:1px solid var(--bdr);font-weight:500}
tbody td{padding:10px 14px;border-bottom:1px solid var(--bdr);color:var(--pri)}
tbody tr:last-child td{border-bottom:none}
tbody tr:hover td{background:var(--panel2)}
tbody tr.total-row td{background:var(--bg);color:var(--sec);font-weight:600}
tbody tr.total-row td.qty-total{color:var(--ac)}
td.center{text-align:center}
.eng{font-size:10px;padding:2px 6px;border-radius:3px;border:1px solid;font-weight:600;letter-spacing:.5px}
.eng.main{color:var(--ac);border-color:var(--acd)}
.eng.cascade{color:var(--blu);border-color:#2a4a88}
.eng.near_cap{color:var(--yel);border-color:#7a6200}
.eng.tomorrow{color:#c084fc;border-color:#6b21a8}
.eng.sweep{color:var(--eng-sweep);border-color:#8a5418}
.eng.peak{color:var(--eng-peak);border-color:#0e6b7a}
.eng.topup{color:var(--eng-topup);border-color:#6b21a8}
.eng.econv{color:var(--eng-econv);border-color:#8a2c5e}
.eng.hourly{color:var(--eng-hourly);border-color:#13705f}
.eng.lowt{color:var(--eng-lowt);border-color:#8a6510}
/* ── Capital strip (per-engine remaining budget) ── */
#cap-strip{display:flex;gap:6px;flex-wrap:wrap;align-items:center}
.cap-eng{display:flex;align-items:center;gap:3px;font-size:11px;white-space:nowrap}
.cap-let{display:inline-flex;align-items:center;justify-content:center;
  width:16px;height:16px;border-radius:3px;border:1px solid;font-size:9px;
  font-weight:700;line-height:1}
.cap-amt{color:var(--pri);font-variant-numeric:tabular-nums}
.cap-eng.exhausted .cap-amt{color:var(--sec)}
.cap-let.main{color:var(--ac);border-color:var(--acd)}
.cap-let.cascade{color:var(--blu);border-color:#2a4a88}
.cap-let.sweep{color:var(--eng-sweep);border-color:#8a5418}
.cap-let.peak{color:var(--eng-peak);border-color:#0e6b7a}
.cap-let.topup{color:var(--eng-topup);border-color:#6b21a8}
.cap-let.econv{color:var(--eng-econv);border-color:#8a2c5e}
.cap-let.hourly{color:var(--eng-hourly);border-color:#13705f}
.cap-let.lowt{color:var(--eng-lowt);border-color:#8a6510}
.side-no{color:var(--ac)}.side-yes{color:var(--yel)}
.pnl-pos{color:var(--ac)}.pnl-neg{color:var(--red)}
.status-live{color:var(--ac)}.status-settled{color:var(--sec)}

/* ── Sub-tabs ── */
.stabs{display:flex;border-bottom:1px solid var(--bdr);margin-bottom:16px;gap:0}
.stb{padding:8px 16px;font-size:11px;color:var(--sec);background:none;border:none;
  border-bottom:2px solid transparent;cursor:pointer;letter-spacing:.5px;transition:color .15s}
.stb:hover{color:var(--pri)}.stb.on{color:var(--ac);border-bottom-color:var(--ac)}
.sp{display:none}.sp.on{display:block}
/* city-modal tabs — scoped variant of .stb/.sp, mirrors the same look */
.stb-row{display:flex;gap:4px;border-bottom:1px solid #252d42}
.mtb{padding:8px 16px;font-size:11px;color:var(--sec);background:none;border:none;
  border-bottom:2px solid transparent;cursor:pointer;letter-spacing:.5px;transition:color .15s}
.mtb:hover{color:var(--pri)}.mtb.on{color:var(--ac);border-bottom-color:var(--ac)}
.msp{display:none}.msp.on{display:block}

/* ── Buttons ── */
.btn{padding:6px 14px;border-radius:var(--r);border:1px solid var(--bdr);background:none;
  color:var(--sec);cursor:pointer;font-size:11px;transition:all .15s}
.btn:hover{border-color:var(--ac);color:var(--ac)}
.btn.active{border-color:var(--ac);color:var(--ac);background:var(--ac2)}

/* ── Log ── */
#log-out{background:var(--bg);border:1px solid var(--bdr);border-radius:var(--r2);
  padding:14px;font-size:11px;color:var(--sec);line-height:1.7;white-space:pre-wrap;
  word-break:break-all;height:calc(100vh - 200px);overflow-y:auto}
.log-info{color:var(--sec)}.log-warn{color:var(--yel)}.log-err{color:var(--red)}
.log-sig{color:var(--ac)}.log-trade{color:#c084fc}

/* ── Stat cards ── */
.stat-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(160px,1fr));gap:10px;margin-bottom:20px}
.stat-card{background:var(--panel);border:1px solid var(--bdr);border-radius:var(--r2);padding:14px 16px}
.stat-k{color:var(--sec);font-size:9px;letter-spacing:2px;text-transform:uppercase;margin-bottom:6px}
/* Weather-tab label override — larger + bold for readability (city modal only) */
.stat-k-wx{font-size:12px;font-weight:700;letter-spacing:1.5px;color:#a8c4e0}
.stat-v{font-size:22px;font-weight:600}

/* ── City modal ── */
#modal-bg{display:none;position:fixed;inset:0;background:rgba(0,0,0,.7);z-index:500;
  align-items:center;justify-content:center}
#modal-bg.on{display:flex}
#modal{background:var(--panel);border:1px solid var(--bdr2);border-radius:var(--r2);
  width:min(700px,95vw);max-height:85vh;overflow-y:auto;padding:24px}
.modal-close{float:right;background:none;border:none;color:var(--sec);font-size:20px;
  cursor:pointer;line-height:1}
.modal-close:hover{color:var(--pri)}

/* ── Refresh bar ── */
.refresh-bar{display:flex;align-items:center;gap:10px;margin-bottom:16px}
.refresh-bar .btn{padding:5px 12px}
#refresh-cd{color:var(--sec);font-size:11px}

/* ── Empty / spinner ── */
.empty{color:var(--sec);text-align:center;padding:32px;font-size:12px}
.spin{width:20px;height:20px;border:2px solid var(--bdr);border-top-color:var(--ac);
  border-radius:50%;animation:spin .7s linear infinite;display:inline-block;vertical-align:middle;margin-right:8px}
@keyframes spin{to{transform:rotate(360deg)}}

/* ── Charts ── */
.chart-grid{display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-bottom:16px}
.chart-box{background:var(--panel);border:1px solid var(--bdr);border-radius:var(--r2);
  padding:16px}
.chart-lbl{color:var(--sec);font-size:9px;letter-spacing:2px;text-transform:uppercase;margin-bottom:12px}
.chart-box canvas{max-height:180px}
</style>
</head>
<body>
<div id="app">

<!-- Header -->
<header id="hdr">
  <div id="logo">⛅ WEATHERMACHINE<small>KALSHI TEMPERATURE MARKETS</small></div>
  <span class="mode-badge" id="mode-badge">DEMO</span>
  <button class="btn" onclick="refreshAll()" title="Refresh all data">↻ Refresh</button>
  <div id="hdr-status">Initialising...</div>
</header>

<!-- Balance bar -->
<div id="bal-bar">
  <div class="bal-item"><div class="bal-k">Balance</div><div class="bal-v" id="b-bal">—</div></div>
  <div class="bal-item" style="flex:1 1 auto;min-width:0"><div class="bal-k">Available · per engine</div><div id="cap-strip" style="margin-top:3px"></div></div>
  <div class="bal-item"><div class="bal-k">Portfolio</div><div class="bal-v" id="b-prt">—</div></div>
  <div class="bal-item"><div class="bal-k">Unrealised</div><div class="bal-v" id="b-unr">—</div></div>
  <div class="bal-item"><div class="bal-k">Open</div><div class="bal-v" id="b-open">—</div></div>
  <div class="bal-item" id="b-pending-wrap" style="display:none"><div class="bal-k">Pending</div><div class="bal-v" id="b-pending" style="color:var(--yel)">—</div></div>
</div>

<!-- Tab bar -->
<div id="tabbar">
  <button class="tb on" data-tab="home" onclick="switchTab('home',this)">Home</button>
  <button class="tb" data-tab="session" onclick="switchTab('session',this)">Session</button>
  <button class="tb" data-tab="hourly" onclick="switchTab('hourly',this)">Hourly NYC</button>
  <button class="tb" data-tab="log" onclick="switchTab('log',this)">Log</button>
  <button class="tb" data-tab="perf" onclick="switchTab('perf',this)">Performance</button>
</div>

<!-- Content -->
<div id="content">

<!-- ── HOME ── -->
<div class="tab on" id="tab-home">
  <div class="sh">ENGINE BUDGETS</div>
  <div id="eng-grid" style="display:grid;grid-template-columns:repeat(auto-fill,minmax(130px,1fr));gap:8px;margin-bottom:16px"></div>
  <div class="sh">CITY STATUS</div>
  <div id="map-wrap">
    <svg id="us-svg" viewBox="0 0 960 580"></svg>
  </div>
  <div id="cgrid" style="display:none"></div>
</div>

<!-- ── SESSION ── -->
<div class="tab" id="tab-session">
  <div class="refresh-bar">
    <button class="btn" onclick="loadSession()">↻ Refresh</button>
    <span id="refresh-cd"></span>
    <span id="sess-summary" style="color:var(--sec);font-size:11px;margin-left:auto"></span>
  </div>
  <div class="stabs">
    <button class="stb on" onclick="switchSess('all',this)">All</button>
    <button class="stb" onclick="switchSess('high',this)">High</button>
    <button class="stb" onclick="switchSess('low',this)">Low</button>
  </div>
  <div class="sp on" id="sp-all"></div>
  <div class="sp" id="sp-high"></div>
  <div class="sp" id="sp-low"></div>
  <div id="pending-section" style="display:none;margin-top:24px">
    <div class="sh" style="color:var(--yel);border-color:var(--yel2)">⏳ PENDING SETTLEMENT</div>
    <div id="pending-table"></div>
  </div>
</div>

<!-- ── LOG ── -->
<div class="tab" id="tab-log">
  <div class="refresh-bar">
    <button class="btn" onclick="loadLog()">↻ Refresh</button>
    <button class="btn" id="log-follow-btn" onclick="toggleFollow()" title="Auto-scroll to bottom">Follow</button>
    <select id="log-lines" onchange="loadLog()" style="background:var(--s2);color:var(--pri);border:1px solid var(--s3);border-radius:4px;padding:4px 8px;font-size:12px;cursor:pointer">
      <option value="100">100 lines</option>
      <option value="200" selected>200 lines</option>
      <option value="500">500 lines</option>
    </select>
    <span id="log-refresh-cd" style="font-size:11px;color:var(--sec);margin-left:8px"></span>
  </div>
  <pre id="log-out">Loading...</pre>
</div>

<!-- ── PERFORMANCE ── -->
<div class="tab" id="tab-perf">
  <div class="sh">PERFORMANCE</div>
  <div class="stat-grid" id="perf-stats"></div>
  <div class="stabs">
    <button class="stb on" onclick="switchPerf('charts',this)">Charts</button>
    <button class="stb" onclick="switchPerf('by-day',this)">By Day</button>
    <button class="stb" onclick="switchPerf('settlements',this)">All Settlements</button>
  </div>
  <div class="sp on" id="sp-charts">
    <div class="chart-grid">
      <div class="chart-box"><div class="chart-lbl">EQUITY CURVE</div><canvas id="chart-equity"></canvas></div>
      <div class="chart-box"><div class="chart-lbl">7-DAY ROLLING WIN RATE</div><canvas id="chart-wr"></canvas></div>
      <div class="chart-box"><div class="chart-lbl">DAILY PnL</div><canvas id="chart-daily-pnl"></canvas></div>
      <div class="chart-box"><div class="chart-lbl">WIN RATE BY DAY</div><canvas id="chart-daily-wr"></canvas></div>
    </div>
  </div>
  <div class="sp" id="sp-by-day">
    <div class="tbl-wrap" id="perf-byDay-wrap"></div>
  </div>
  <div class="sp" id="sp-settlements">
    <div class="tbl-wrap" id="perf-table-wrap"></div>
  </div>
</div>

<!-- HOURLY NYC -->
<div class="tab" id="tab-hourly">
  <div class="refresh-bar">
    <button class="btn" onclick="loadHourly()">↻ Refresh</button>
    <span id="hourly-summary" style="color:var(--sec);font-size:11px;margin-left:auto"></span>
  </div>
  <div class="stat-grid" id="hourly-stats"></div>
  <div class="stabs">
    <button class="stb on" onclick="switchHourly('open',this)">Open</button>
    <button class="stb" onclick="switchHourly('by-hour',this)">By Hour</button>
    <button class="stb" onclick="switchHourly('by-day',this)">By Day</button>
    <button class="stb" onclick="switchHourly('settled',this)">History</button>
  </div>
  <div class="sp on" id="hsp-open"></div>
  <div class="sp" id="hsp-by-hour"></div>
  <div class="sp" id="hsp-by-day"></div>
  <div class="sp" id="hsp-settled"></div>
</div>

</div><!-- /content -->

<!-- City modal -->
<div id="modal-bg" onclick="closeModal(event)">
  <div id="modal">
    <button class="modal-close" onclick="closeModal()">×</button>
    <div id="modal-content"></div>
  </div>
</div>

</div><!-- /app -->

<script>
// ── State ──────────────────────────────────────────────────────────────────
let _sessData = [];
let _citiesData = {};   // {city: row} from /api/cities — reused by the city modal
let _sessFilt = 'all';
let _autoRefresh;
let _cdSecs = 60;
let _cdInt;
let _logFollow = true;
let _charts = {};

const fmt$ = v => v == null ? '—' : '$' + v.toFixed(2);
const fmtPct = v => v == null ? '—' : v.toFixed(1) + '%';
const clsPnl = v => v > 0 ? 'pnl-pos' : v < 0 ? 'pnl-neg' : '';
// Canonicalize any raw engine/tier string to one of the known engine classes.
// Tier strings in the trade log are messy (cascade_afternoon, tomorrow_sweep,
// lowt_main, hourly_nyc...), so we resolve by substring priority. Shared by
// the Session-tab badges and the capital strip so both always agree.
const _engCanon = e => {
  const s = (e || '').toLowerCase();
  if (s.includes('cascade'))   return 'cascade';
  if (s.includes('sweep') || s.includes('tomorrow')) return 'sweep';
  if (s.includes('peak'))      return 'peak';
  if (s.includes('topup'))     return 'topup';
  if (s.includes('econv'))     return 'econv';
  if (s.includes('hourly'))    return 'hourly';
  if (s.includes('lowt'))      return 'lowt';
  if (s.includes('near_cap'))  return 'near_cap';
  return 'main';
};
const clsEng = e => _engCanon(e);
// Single-letter labels for the compact capital strip / badges.
const engLetter = e => ({main:'M', near_cap:'N', cascade:'C', sweep:'S',
  peak:'P', topup:'T', econv:'E', hourly:'H', lowt:'L'}[_engCanon(e)] ?? 'M');

// ── Tab switching ──────────────────────────────────────────────────────────
function switchTab(id, btn) {
  document.querySelectorAll('.tab').forEach(t => t.classList.remove('on'));
  document.querySelectorAll('.tb').forEach(b => b.classList.remove('on'));
  document.getElementById('tab-' + id).classList.add('on');
  btn.classList.add('on');
  if (id === 'session') { stopLogRefresh(); loadSession(); }
  if (id === 'hourly')  { stopLogRefresh(); loadHourly(); }
  if (id === 'log')     { loadLog(); startLogRefresh(); }
  if (id === 'perf')    { stopLogRefresh(); loadPerf(); }
  if (id === 'home')    { stopLogRefresh(); }
}

function switchSess(filt, btn) {
  document.querySelectorAll('.stb').forEach(b => b.classList.remove('on'));
  document.querySelectorAll('.sp').forEach(s => s.classList.remove('on'));
  btn.classList.add('on');
  document.getElementById('sp-' + filt).classList.add('on');
  _sessFilt = filt;
  renderSessTable(filt);
}

// ── Refresh cycle ──────────────────────────────────────────────────────────
function startCountdown() {
  clearInterval(_cdInt);
  _cdSecs = 60;
  _cdInt = setInterval(() => {
    _cdSecs--;
    const el = document.getElementById('refresh-cd');
    if (el) el.textContent = `Next refresh in ${_cdSecs}s`;
    if (_cdSecs <= 0) { refreshAll(); startCountdown(); }
  }, 1000);
}

// Reposition map chips when the SVG container resizes
const _mapObs = new ResizeObserver(() => loadCities());
document.addEventListener('DOMContentLoaded', () => {
  const svgEl = document.getElementById('us-svg');
  if (svgEl) _mapObs.observe(svgEl);
  syncStickyOffsets();
});

// The balance bar wraps to two rows on narrow/mobile screens, so its height
// isn't fixed. Measure the real rendered heights of the header + balance bar
// and pin the tab bar directly beneath them. Re-run on resize because that's
// exactly when the balance bar wraps/unwraps.
function syncStickyOffsets() {
  const hdr = document.getElementById('hdr');
  const bal = document.getElementById('bal-bar');
  const tab = document.getElementById('tabbar');
  if (!hdr || !bal || !tab) return;
  const offset = hdr.offsetHeight + bal.offsetHeight;
  tab.style.top = offset + 'px';
}
window.addEventListener('resize', syncStickyOffsets);

function refreshAll() {
  setStatus('Refreshing...');
  Promise.all([loadStatus(), loadCities(), loadSession()]).then(() => {
    setStatus('Ready');
    startCountdown();
  });
}

function setStatus(msg, polling=false) {
  const el = document.getElementById('hdr-status');
  el.textContent = msg;
  el.className = polling ? 'polling' : '';
}

// ── Status / balance ───────────────────────────────────────────────────────
// Render the per-engine capital strip in the balance bar. Fixed canonical
// order (object key order isn't guaranteed). Each engine: colored letter
// badge + remaining $. near_cap is intentionally absent — it's a state of
// the main engine, not a separately-budgeted line.
const _CAP_ORDER = ['main','cascade','sweep','peak','topup','econv','hourly','lowt'];
function renderCapStrip(engines) {
  const strip = document.getElementById('cap-strip');
  if (!strip) return;
  const parts = [];
  for (const e of _CAP_ORDER) {
    const info = engines[e];
    if (!info) continue;                       // engine not present in payload
    const rem = Number(info.remaining ?? 0);
    const exhausted = rem <= 0.005 ? ' exhausted' : '';
    parts.push(
      `<span class="cap-eng${exhausted}" title="${e}: $${rem.toFixed(2)} of `
      + `$${Number(info.budget ?? 0).toFixed(2)}">`
      + `<span class="cap-let ${e}">${engLetter(e)}</span>`
      + `<span class="cap-amt">$${rem.toFixed(2)}</span></span>`
    );
  }
  strip.innerHTML = parts.length
    ? parts.join('')
    : '<span style="color:var(--sec);font-size:11px">—</span>';
  // Strip height can change the balance bar height (wrapping) — re-pin tab bar.
  if (typeof syncStickyOffsets === 'function') syncStickyOffsets();
}

async function loadStatus() {
  try {
    const d = await fetch('/api/status').then(r => r.json());
    document.getElementById('b-bal').textContent  = fmt$(d.balance);
    renderCapStrip(d.engines || {});
    document.getElementById('b-prt').textContent  = fmt$(d.portfolio);
    // Engine budget cards
    const eg = document.getElementById('eng-grid');
    if (eg && d.engines) {
      eg.innerHTML = Object.entries(d.engines).map(([name, v]) => {
        const pct = v.budget > 0 ? Math.round((v.remaining / v.budget) * 100) : 0;
        const col = pct > 50 ? 'var(--ac)' : pct > 20 ? 'var(--pri)' : 'var(--red)';
        return `<div style="background:var(--s2);border-radius:6px;padding:8px 10px">
          <div style="font-size:10px;color:var(--sec);text-transform:uppercase;letter-spacing:.05em;margin-bottom:4px">${name}</div>
          <div style="font-size:15px;font-weight:600;color:${col}">${fmt$(v.remaining)}</div>
          <div style="font-size:10px;color:var(--sec);margin-top:2px">of ${fmt$(v.budget)}</div>
          <div style="height:3px;background:var(--s1);border-radius:2px;margin-top:6px">
            <div style="height:100%;width:${pct}%;background:${col};border-radius:2px;transition:width .3s"></div>
          </div>
        </div>`;
      }).join('');
    }
    document.getElementById('b-open').textContent = d.open ?? '—';
    const unr = d.unrealised ?? 0;
    const uEl = document.getElementById('b-unr');
    uEl.textContent = (unr >= 0 ? '+' : '') + fmt$(unr);
    uEl.className   = 'bal-v ' + clsPnl(unr);
    // Pending settlement balance bar item — only visible when there are pending positions
    const pendWrap = document.getElementById('b-pending-wrap');
    if (pendWrap) {
      if (d.pending > 0) {
        pendWrap.style.display = '';
        document.getElementById('b-pending').textContent =
          d.pending + ' pos · ' + fmt$(d.pending_cost);
      } else {
        pendWrap.style.display = 'none';
      }
    }
    // The Pending field appearing/disappearing can change the balance bar's
    // height (it may wrap to a new row), so re-pin the tab bar beneath it.
    syncStickyOffsets();
    const mb = document.getElementById('mode-badge');
    mb.textContent  = d.mode;
    mb.className    = 'mode-badge' + (d.mode === 'LIVE' ? ' live' : '');
  } catch(e) { console.warn('status', e); }
}

// ── City coordinates (Albers USA — lat/lon for D3 projection) ──────────────
const CITY_LL = {
  'New York':      [40.71,-74.01], 'Chicago':      [41.88,-87.63],
  'Miami':         [25.77,-80.19], 'Austin':       [30.27,-97.74],
  'Los Angeles':   [34.05,-118.24],'San Francisco': [37.77,-122.42],
  'Denver':        [39.74,-104.98],'Philadelphia':  [39.95,-75.17],
  'Atlanta':       [33.75,-84.39], 'Houston':       [29.76,-95.37],
  'Phoenix':       [33.45,-112.07],'Las Vegas':     [36.17,-115.14],
  'Dallas':        [32.78,-96.80], 'Boston':        [42.36,-71.06],
  'Washington DC': [38.91,-77.04], 'Seattle':       [47.61,-122.33],
  'Minneapolis':   [44.98,-93.27], 'Oklahoma City': [35.47,-97.52],
  'New Orleans':   [29.95,-90.07], 'San Antonio':   [29.42,-98.49],
};
// Pixel nudges for crowded northeast and other overlapping cities
const CITY_NUDGE = {
  'Boston':        [-14,-14], 'New York':      [10, 6],
  'Philadelphia':  [14, 10],  'Washington DC': [14,-4],
  'New Orleans':   [-6, 10],  'San Antonio':   [-8, 8],
  'Houston':       [8,  8],   'Dallas':        [6, -8],
};
// Short abbreviation for map chip
const CITY_ABB = {
  'New York':'NYC','Chicago':'CHI','Miami':'MIA','Austin':'AUS',
  'Los Angeles':'LAX','San Francisco':'SFO','Denver':'DEN',
  'Philadelphia':'PHL','Atlanta':'ATL','Houston':'HOU','Phoenix':'PHX',
  'Las Vegas':'LAS','Dallas':'DAL','Boston':'BOS','Washington DC':'DCA',
  'Seattle':'SEA','Minneapolis':'MSP','Oklahoma City':'OKC',
  'New Orleans':'MSY','San Antonio':'SAT',
};

let _mapReady = false;
let _mapProj  = null;

async function initMap() {
  if (_mapReady) return;
  try {
    const svg = d3.select('#us-svg');
    // Albers USA projection scaled to viewBox 960×580
    _mapProj = d3.geoAlbersUsa().scale(1280).translate([480,290]);
    const path = d3.geoPath().projection(_mapProj);
    const us = await d3.json(
      'https://cdn.jsdelivr.net/npm/us-atlas@3/states-10m.json');
    svg.append('g').selectAll('path')
      .data(topojson.feature(us, us.objects.states).features)
      .join('path').attr('class','map-state').attr('d', path);
    // Subtle border between states
    svg.append('path')
      .datum(topojson.mesh(us, us.objects.states, (a,b) => a !== b))
      .attr('fill','none').attr('stroke','#1e2535')
      .attr('stroke-width',.8).attr('d', path);
    _mapReady = true;
  } catch(e) { console.warn('map init', e); }
}

// ── Cities ─────────────────────────────────────────────────────────────────
async function loadCities() {
  try {
    await initMap();
    const cities = await fetch('/api/cities').then(r => r.json());
    const wrap = document.getElementById('map-wrap');
    // Remove old chips
    wrap.querySelectorAll('.map-chip').forEach(el => el.remove());

    const svgEl  = document.getElementById('us-svg');
    const svgW   = svgEl.viewBox.baseVal.width  || 960;
    const svgH   = svgEl.viewBox.baseVal.height || 580;
    const rect   = svgEl.getBoundingClientRect();
    const scaleX = rect.width  / svgW;
    const scaleY = rect.height / svgH;

    for (const d of cities) {
      const city = d.city;
      _citiesData[city] = d;   // stash for the city modal's Weather tab
      const ll   = CITY_LL[city];
      if (!ll || !_mapProj) continue;

      const proj = _mapProj(ll.slice().reverse()); // D3 wants [lon,lat]
      if (!proj) continue;
      const [px, py] = proj;

      // Apply nudge
      const nudge = CITY_NUDGE[city] || [0,0];
      const left  = (px * scaleX + nudge[0]) + 'px';
      const top   = (py * scaleY + nudge[1]) + 'px';

      const ha  = d.high_active, la = d.lowt_active;
      const obsH = d.obs_hi  != null ? Number(d.obs_hi).toFixed(0)  + '°' : '—';
      const fcsH = d.fcst_hi != null ? Number(d.fcst_hi).toFixed(0) + '°' : '—';
      const now  = d.now     != null ? Number(d.now).toFixed(0)     + '°' : '—';
      const dot  = (ha || la) ? '<span class="chip-dot"></span>' : '';

      const chip = document.createElement('div');
      chip.className = 'map-chip'
        + (ha && la ? ' active-both' : ha ? ' active-hi' : la ? ' active-lo' : '');
      chip.style.cssText = `left:${left};top:${top}`;
      chip.title = `${city}\nnow: ${now}  obs hi: ${obsH}  fcst hi: ${fcsH}`;
      chip.innerHTML =
        `<div class="chip-name">${dot}${CITY_ABB[city]||city}</div>` +
        `<div class="chip-now">${obsH}</div>` +
        `<div class="chip-fcst">▲${fcsH}</div>`;
      chip.onclick = () => openCityModal(city);
      wrap.appendChild(chip);
    }
  } catch(e) { console.warn('cities', e); }
}

// ── Session ────────────────────────────────────────────────────────────────
async function loadSession() {
  try {
    const d = await fetch('/api/session').then(r => r.json());
    _sessData = d.positions || [];
    const unr = d.unrealised ?? 0;
    document.getElementById('sess-summary').textContent =
      `${d.open} open  |  unrealised ${unr >= 0 ? '+' : ''}${fmt$(unr)}`;
    renderSessTable(_sessFilt);
  } catch(e) { console.warn('session', e); }
  loadPending();
}

async function loadPending() {
  try {
    const rows = await fetch('/api/pending').then(r => r.json());
    const sec  = document.getElementById('pending-section');
    const tbl  = document.getElementById('pending-table');
    if (!sec || !tbl) return;
    if (!rows.length) { sec.style.display = 'none'; return; }
    sec.style.display = '';
    const totalCost = rows.reduce((s, r) => s + (r.cost_total || 0), 0);
    let html = `<div class="tbl-wrap"><table><thead><tr>
      <th>Market</th><th>Bracket</th><th>Side</th><th>Qty</th>
      <th>Entry</th><th>At Risk</th><th>Est. Settlement</th><th>Result</th>
    </tr></thead><tbody>`;
    for (const r of rows) {
      const resCls = r.outcome_cls === 'green' ? 'pnl-pos'
                   : r.outcome_cls === 'red'   ? 'pnl-neg'
                   : 'style="color:var(--yel)"';
      const resSpan = r.outcome_cls === 'green'
        ? `<span class="pnl-pos">${r.outcome}</span>`
        : r.outcome_cls === 'red'
        ? `<span class="pnl-neg">${r.outcome}</span>`
        : `<span style="color:var(--yel)">${r.outcome}</span>`;
      html += `<tr style="color:var(--sec)">
        <td>${r.market || r.ticker}</td>
        <td>${r.bracket || '—'}</td>
        <td class="center"><span class="${r.side==='NO'?'side-no':'side-yes'}">${r.side}</span></td>
        <td class="center">${r.contracts}</td>
        <td>${fmt$(r.avg_cost)}</td>
        <td style="color:var(--yel)">${fmt$(r.cost_total)}</td>
        <td style="color:var(--sec)">${r.est_settlement}</td>
        <td>${resSpan}</td>
      </tr>`;
    }
    // Total row
    html += `<tr class="total-row">
      <td colspan="4"></td>
      <td></td>
      <td style="color:var(--yel);font-weight:600">${fmt$(totalCost)}</td>
      <td colspan="2"></td>
    </tr>`;
    html += '</tbody></table></div>';
    tbl.innerHTML = html;
  } catch(e) { console.warn('pending', e); }
}

function renderSessTable(filt) {
  let rows = _sessData;
  if (filt === 'high') rows = rows.filter(r => r.market_type === 'HIGH');
  if (filt === 'low')  rows = rows.filter(r => r.market_type !== 'HIGH');
  const sp = document.getElementById('sp-' + filt);
  if (!rows.length) { sp.innerHTML = '<div class="empty">No open positions</div>'; return; }

  const totalQty = rows.reduce((s, r) => s + (r.contracts || 1), 0);
  const cols = ['Time','Market','Bracket','Engine','Side','Qty','Entry','Score','Unreal. PnL','Status'];
  let html = `<div class="tbl-wrap"><table><thead><tr>${cols.map(c=>`<th>${c}</th>`).join('')}</tr></thead><tbody>`;
  for (const r of rows) {
    const unr = r.unrealised ?? 0;
    const eng = r.engine || 'MAIN';
    const time = (r.entered_at || '').split(' ')[1] || '—';
    html += `<tr>
      <td>${time} UTC</td>
      <td>${r.market || '—'}</td>
      <td>${r.bracket || '—'}</td>
      <td><span class="eng ${clsEng(eng)}">${eng}</span></td>
      <td class="center"><span class="${r.side==='NO'?'side-no':'side-yes'}">${r.side||'—'}</span></td>
      <td class="center">${r.contracts || 1}</td>
      <td>${fmt$(r.avg_cost)}</td>
      <td class="center">${r.score || '—'}</td>
      <td class="${clsPnl(unr)}">${unr>=0?'+':''}${fmt$(unr)}</td>
      <td><span class="status-live">Open</span></td>
    </tr>`;
  }
  // Total row
  html += `<tr class="total-row"><td colspan="4"></td><td></td><td class="center qty-total">${totalQty}</td><td colspan="4"></td></tr>`;
  html += '</tbody></table></div>';
  sp.innerHTML = html;
}

// ── Log ────────────────────────────────────────────────────────────────────
let _logRefreshInt = null;
let _logRefreshSecs = 0;

async function loadLog() {
  try {
    const n = document.getElementById('log-lines')?.value || 200;
    const d = await fetch(`/api/log?n=${n}`).then(r => r.json());
    const el = document.getElementById('log-out');
    el.innerHTML = (d.lines || []).map(line => {
      let cls = 'log-info';
      if (/WARNING|WARN/.test(line))                  cls = 'log-warn';
      if (/ERROR|FAIL|order failed/.test(line))        cls = 'log-err';
      if (/★|SIGNAL|NEAR_CAP|GRADIENT/.test(line))    cls = 'log-sig';
      if (/order.*placed|SWEEP|DISMISSED/.test(line))  cls = 'log-trade';
      if (/INFO.*placed|orders placed/.test(line))     cls = 'log-trade';
      return `<span class="${cls}">${escHtml(line)}</span>`;
    }).join('\n');
    if (_logFollow) el.scrollTop = el.scrollHeight;
  } catch(e) { console.warn('log', e); }
}

function startLogRefresh() {
  stopLogRefresh();
  _logRefreshSecs = 15;
  _logRefreshInt = setInterval(() => {
    _logRefreshSecs--;
    const cd = document.getElementById('log-refresh-cd');
    if (cd) cd.textContent = `auto-refresh in ${_logRefreshSecs}s`;
    if (_logRefreshSecs <= 0) {
      loadLog();
      _logRefreshSecs = 15;
    }
  }, 1000);
}

function stopLogRefresh() {
  if (_logRefreshInt) { clearInterval(_logRefreshInt); _logRefreshInt = null; }
  const cd = document.getElementById('log-refresh-cd');
  if (cd) cd.textContent = '';
}

function toggleFollow() {
  _logFollow = !_logFollow;
  document.getElementById('log-follow-btn').classList.toggle('active', _logFollow);
}

function escHtml(s) {
  return s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
}

function switchPerf(id, btn) {
  document.querySelectorAll('#tab-perf .stb').forEach(b => b.classList.remove('on'));
  document.querySelectorAll('#tab-perf .sp').forEach(s => s.classList.remove('on'));
  btn.classList.add('on');
  document.getElementById('sp-' + id).classList.add('on');
}

// ── Performance ────────────────────────────────────────────────────────────
async function loadPerf() {
  try {
    const d = await fetch('/api/performance').then(r => r.json());
    const s = d.stats || {};
    const stats = [
      ['Win Rate',    fmtPct(s.win_rate),   s.win_rate >= 85 ? 'g' : ''],
      ['Net PnL',     fmt$(s.net_pnl),       s.net_pnl  >= 0  ? 'g' : 'r'],
      ['Total Fees',  fmt$(s.total_fees),    ''],
      ['Total Trades',s.total ?? '—',        ''],
      ['Best Day',    fmt$(s.best_day),      'g'],
      ['Worst Day',   fmt$(s.worst_day),     s.worst_day < 0 ? 'r' : ''],
    ];
    document.getElementById('perf-stats').innerHTML = stats.map(([k,v,c])=>
      `<div class="stat-card"><div class="stat-k">${k}</div><div class="stat-v ${c}">${v}</div></div>`
    ).join('');

    // Charts
    _buildChart('chart-equity', d.chart?.equity    || [], 'Equity ($)',       'var(--ac)',  'var(--ac2)');
    _buildChart('chart-wr',     d.chart?.win_rate  || [], 'Win Rate (%)',     'var(--blu)', 'rgba(77,159,255,.1)');

    // Daily PnL bar chart
    const days = d.by_day || [];
    _buildBarChart('chart-daily-pnl', days.map(r => ({x: r.date, y: r.net_pnl})));

    // Win rate by day line chart
    _buildChart('chart-daily-wr',
      days.map(r => ({x: r.date, y: parseFloat(r.win_pct)})),
      'Win Rate (%)', 'var(--grn)', 'rgba(56,201,138,.1)'
    );

    // By Day table
    const days2 = d.by_day || [];
    if (days2.length) {
      let html = `<table><thead><tr>
        <th>Date</th><th>Trades</th><th>Wins</th><th>Losses</th><th>Win %</th><th>Net PnL</th><th>Cum PnL</th>
      </tr></thead><tbody>`;
      for (const r of days2) {
        const cls = r.net_pnl >= 0 ? 'pnl-pos' : 'pnl-neg';
        html += `<tr>
          <td>${r.date}</td><td class="center">${r.trades}</td>
          <td class="center pnl-pos">${r.wins}</td>
          <td class="center pnl-neg">${r.losses}</td>
          <td class="center">${r.win_pct}</td>
          <td class="${cls}">${r.net_pnl >= 0 ? '+' : ''}${fmt$(r.net_pnl)}</td>
          <td class="${r.cum_pnl >= 0 ? 'pnl-pos' : 'pnl-neg'}">${r.cum_pnl >= 0 ? '+' : ''}${fmt$(r.cum_pnl)}</td>
        </tr>`;
      }
      html += '</tbody></table>';
      document.getElementById('perf-byDay-wrap').innerHTML = html;
    } else {
      document.getElementById('perf-byDay-wrap').innerHTML = '<div class="empty">No data yet</div>';
    }

    // All Settlements table
    const rows = d.all_settlements || [];
    if (rows.length) {
      let html = `<table><thead><tr>
        <th>Date</th><th>Market</th><th>Bracket</th><th>Entry</th><th>Net PnL</th><th>Result</th>
      </tr></thead><tbody>`;
      for (const r of rows) {
        const cls = r.result_class === 'green' ? 'pnl-pos' : r.result_class === 'red' ? 'pnl-neg' : '';
        html += `<tr>
          <td>${r.date || '—'}</td>
          <td>${r.market_label || r.ticker}</td>
          <td>${(r.ticker||'').split('-').pop()}</td>
          <td>${fmt$(r.avg_entry || r.entry_price)}</td>
          <td class="${cls}">${r.net_pnl >= 0 ? '+' : ''}${fmt$(r.net_pnl)}</td>
          <td class="${cls}">${r.result_label}</td>
        </tr>`;
      }
      html += '</tbody></table>';
      document.getElementById('perf-table-wrap').innerHTML = html;
    } else {
      document.getElementById('perf-table-wrap').innerHTML = '<div class="empty">No settlements yet</div>';
    }
  } catch(e) { console.warn('perf', e); }
}

function _buildBarChart(id, data) {
  const canvas = document.getElementById(id);
  if (!canvas) return;
  if (_charts[id]) { _charts[id].destroy(); }
  _charts[id] = new Chart(canvas, {
    type: 'bar',
    data: { datasets: [{
      label: 'Daily PnL ($)',
      data,
      backgroundColor: data.map(d => d.y >= 0 ? 'rgba(56,201,138,.7)' : 'rgba(255,80,80,.7)'),
      borderColor:     data.map(d => d.y >= 0 ? 'var(--grn)' : 'rgba(255,80,80,1)'),
      borderWidth: 1,
      borderRadius: 2,
    }] },
    options: {
      responsive: true, maintainAspectRatio: true,
      scales: {
        x: { type:'category', ticks:{ color:'#5a6278', font:{family:'JetBrains Mono',size:9}, maxRotation:45 },
             grid:{ color:'#1e2230' } },
        y: { ticks:{ color:'#5a6278', font:{family:'JetBrains Mono',size:10},
                     callback: v => '$'+v.toFixed(2) },
             grid:{ color:'#1e2230' } }
      },
      plugins: { legend:{ display:false } }
    }
  });
}

function _buildChart(id, data, label, color, fill) {
  const canvas = document.getElementById(id);
  if (!canvas) return;
  if (_charts[id]) { _charts[id].destroy(); }
  _charts[id] = new Chart(canvas, {
    type: 'line',
    data: { datasets: [{ label, data, borderColor: color, backgroundColor: fill,
      borderWidth: 1.5, pointRadius: 0, fill: true, tension: 0.3 }] },
    options: {
      responsive: true, maintainAspectRatio: true,
      scales: {
        x: { type:'category', ticks:{ color:'#5a6278', font:{family:'JetBrains Mono',size:10} },
             grid:{ color:'#1e2230' } },
        y: { ticks:{ color:'#5a6278', font:{family:'JetBrains Mono',size:10} },
             grid:{ color:'#1e2230' } }
      },
      plugins: { legend:{ display:false } }
    }
  });
}

// ── City modal ─────────────────────────────────────────────────────────────
// Scoped tab switcher for the city modal. Kept separate from switchSess/
// switchPerf/switchHourly so the page-level `.stb`/`.sp` queries can never
// touch the modal's tabs (and vice versa) — all selectors are scoped to
// #modal-content.
function switchCityTab(id, btn) {
  const root = document.getElementById('modal-content');
  if (!root) return;
  root.querySelectorAll('.mtb').forEach(b => b.classList.remove('on'));
  root.querySelectorAll('.msp').forEach(s => s.classList.remove('on'));
  btn.classList.add('on');
  const pane = document.getElementById('msp-' + id);
  if (pane) pane.classList.add('on');
}

function _cityWeatherHTML(city) {
  // Reuse the live snapshot already fetched by loadCities() — no extra round
  // trip, and the numbers stay identical to the chip the user just tapped.
  const w = _citiesData[city];
  const f1 = v => (v != null ? Number(v).toFixed(1) + '°F' : '—');
  if (!w) {
    return '<div class="empty">No live weather snapshot yet — '
         + 'the map is still loading. Try again in a moment.</div>';
  }
  const cells = [
    ['Now',          f1(w.now)],
    ['Observed High', f1(w.obs_hi)],
    ['Observed Low',  f1(w.obs_lo)],
    ['Forecast High', f1(w.fcst_hi)],
    ['Forecast Low',  f1(w.fcst_lo)],
    ['Local Time',   (w.local_time || '—') + (w.tz_abbr ? ' ' + w.tz_abbr : '')],
  ];
  return `<div class="stat-grid" style="grid-template-columns:repeat(2,1fr);margin-bottom:4px">
    ${cells.map(([k,v]) =>
      `<div class="stat-card"><div class="stat-k stat-k-wx">${k}</div>`
      + `<div class="stat-v" style="font-size:18px">${v}</div></div>`).join('')}
  </div>
  <div style="font-size:10px;color:var(--sec);margin-top:10px">
    Window: ${w.window || '—'} · snapshot shared with map markers</div>`;
}

async function openCityModal(city) {
  document.getElementById('modal-bg').classList.add('on');
  const mc = document.getElementById('modal-content');

  // Header + tab bar render immediately; Weather tab is instant (stashed
  // snapshot), History tab fills in once /api/city returns.
  mc.innerHTML =
    `<h2 style="color:var(--ac);font-size:16px;font-weight:700;margin-bottom:12px">⛅ ${city}</h2>
     <div class="stb-row" style="margin-bottom:14px">
       <button class="mtb on" onclick="switchCityTab('weather',this)">Weather</button>
       <button class="mtb" onclick="switchCityTab('history',this)">Trade History</button>
     </div>
     <div class="msp on" id="msp-weather">${_cityWeatherHTML(city)}</div>
     <div class="msp" id="msp-history">
       <div style="text-align:center;padding:24px">
         <span class="spin"></span> Loading history...</div>
     </div>`;

  try {
    const d = await fetch('/api/city/' + encodeURIComponent(city)).then(r => r.json());
    const s = d.stats || {};
    const pos = d.positions || [];
    let html = `<div class="stat-grid" style="grid-template-columns:repeat(3,1fr);margin-bottom:16px">
      ${[['Win Rate',fmtPct(s.win_rate),''],['Net PnL',fmt$(s.total_pnl),s.total_pnl>=0?'g':'r'],
         ['Positions',s.positions,''],['Avg Conv',s.avg_conv,''],['Bias',s.bias,''],['Obs Hi',s.obs_hi,'']]
        .map(([k,v,c])=>`<div class="stat-card"><div class="stat-k">${k}</div><div class="stat-v ${c}" style="font-size:16px">${v}</div></div>`).join('')}
    </div>`;
    if (pos.length) {
      html += `<div class="sh" style="margin-top:0">POSITION HISTORY</div>
        <div class="tbl-wrap"><table><thead><tr>
          <th>Date</th><th>Bracket</th><th>Engine</th><th>Entry</th><th>PnL</th><th>Result</th>
        </tr></thead><tbody>`;
      for (const p of pos.slice(0,20)) {
        const cls = p.outcome==='win'?'pnl-pos':p.outcome==='loss'?'pnl-neg':'';
        html += `<tr><td>${p.date||'—'}</td><td>${p.bracket||'—'}</td>
          <td><span class="eng ${clsEng(p.engine)}">${p.engine||'MAIN'}</span></td>
          <td>${p.entry||'—'}</td>
          <td class="${cls}">${p.pnl>=0?'+':''}${fmt$(p.pnl)}</td>
          <td class="${cls}">${p.outcome_label||'—'}</td></tr>`;
      }
      html += '</tbody></table></div>';
    } else {
      html += '<div class="empty">No position history for this city</div>';
    }
    // Only replace the history pane — leave the Weather tab (and which tab is
    // currently selected) untouched.
    const hist = document.getElementById('msp-history');
    if (hist) hist.innerHTML = html;
  } catch(e) {
    const hist = document.getElementById('msp-history');
    if (hist) hist.innerHTML = '<div class="empty">Failed to load trade history</div>';
  }
}

function closeModal(e) {
  if (!e || e.target === document.getElementById('modal-bg'))
    document.getElementById('modal-bg').classList.remove('on');
}

document.addEventListener('keydown', e => { if (e.key==='Escape') closeModal(); });

// ── Hourly NYC ─────────────────────────────────────────────────────────────
let _hourlyData   = {};
let _hourlySubtab = 'open';

function switchHourly(id, btn) {
  document.querySelectorAll('#tab-hourly .stb').forEach(b => b.classList.remove('on'));
  document.querySelectorAll('#tab-hourly .sp').forEach(s => s.classList.remove('on'));
  btn.classList.add('on');
  document.getElementById('hsp-' + id).classList.add('on');
  _hourlySubtab = id;
  renderHourly(id);
}

async function loadHourly() {
  try {
    const d = await fetch('/api/hourly').then(r => r.json());
    _hourlyData = d;
    const s = d.stats || {};
    document.getElementById('hourly-stats').innerHTML = [
      ['Win Rate',   fmtPct(s.win_rate),  s.win_rate >= 85 ? 'g' : ''],
      ['Net PnL',    fmt$(s.net_pnl),     s.net_pnl  >= 0  ? 'g' : 'r'],
      ['Settled',    s.total ?? '—',      ''],
      ['Open Now',   s.open_count ?? '—', ''],
      ['Total Fees', fmt$(s.total_fees),  ''],
    ].map(([k,v,c]) =>
      `<div class="stat-card"><div class="stat-k">${k}</div><div class="stat-v ${c}">${v}</div></div>`
    ).join('');
    const unr = (d.open || []).reduce((s, r) => s + (r.unrealised || 0), 0);
    document.getElementById('hourly-summary').textContent =
      `${s.open_count} open  |  unrealised ${unr >= 0 ? '+' : ''}${fmt$(unr)}`;
    renderHourly(_hourlySubtab);
  } catch(e) { console.warn('hourly', e); }
}

function renderHourly(id) {
  if (id === 'open')    renderHourlyOpen();
  if (id === 'by-hour') renderHourlyByHour();
  if (id === 'by-day')  renderHourlyByDay();
  if (id === 'settled') renderHourlySettled();
}

function renderHourlyOpen() {
  const rows = _hourlyData.open || [];
  const el   = document.getElementById('hsp-open');
  if (!rows.length) { el.innerHTML = '<div class="empty">No open hourly positions</div>'; return; }
  let html = `<div class="tbl-wrap"><table><thead><tr>
    <th>Entered</th><th>Hour (EDT)</th><th>Threshold</th><th>Forecast</th><th>Dist</th>
    <th>Side</th><th>Qty</th><th>Entry</th><th>Unreal. PnL</th>
  </tr></thead><tbody>`;
  for (const r of rows) {
    const unr  = r.unrealised ?? 0;
    const time = (r.entered_at || '').split(' ')[1] || '—';
    html += `<tr>
      <td>${time} UTC</td>
      <td>${r.mkt_hour != null ? r.mkt_hour + ':00 EDT' : '—'}</td>
      <td>${r.threshold_f != null ? r.threshold_f + '°F' : '—'}</td>
      <td>${r.forecast_f  != null ? r.forecast_f.toFixed(1) + '°F' : '—'}</td>
      <td>${r.dist_f      != null ? r.dist_f.toFixed(1)     + '°F' : '—'}</td>
      <td class="center"><span class="${r.side==='NO'?'side-no':'side-yes'}">${r.side||'—'}</span></td>
      <td class="center">${r.contracts || 1}</td>
      <td>${fmt$(r.avg_cost)}</td>
      <td class="${clsPnl(unr)}">${unr>=0?'+':''}${fmt$(unr)}</td>
    </tr>`;
  }
  html += '</tbody></table></div>';
  el.innerHTML = html;
}

function renderHourlyByHour() {
  const rows = _hourlyData.by_hour || [];
  const el   = document.getElementById('hsp-by-hour');
  if (!rows.length) { el.innerHTML = '<div class="empty">No data yet</div>'; return; }
  let html = `<div class="tbl-wrap"><table><thead><tr>
    <th>Hour (EDT)</th><th>Trades</th><th>Wins</th><th>Losses</th><th>Win %</th><th>Net PnL</th>
  </tr></thead><tbody>`;
  for (const r of rows) {
    const cls = r.net_pnl >= 0 ? 'pnl-pos' : 'pnl-neg';
    html += `<tr>
      <td>${r.label}</td><td class="center">${r.total}</td>
      <td class="center pnl-pos">${r.wins}</td>
      <td class="center pnl-neg">${r.losses}</td>
      <td class="center">${r.win_pct}</td>
      <td class="${cls}">${r.net_pnl >= 0 ? '+' : ''}${fmt$(r.net_pnl)}</td>
    </tr>`;
  }
  html += '</tbody></table></div>';
  el.innerHTML = html;
}

function renderHourlyByDay() {
  const rows = _hourlyData.by_day || [];
  const el   = document.getElementById('hsp-by-day');
  if (!rows.length) { el.innerHTML = '<div class="empty">No data yet</div>'; return; }
  let html = `<div class="tbl-wrap"><table><thead><tr>
    <th>Date</th><th>Trades</th><th>Wins</th><th>Losses</th><th>Win %</th><th>Net PnL</th><th>Cum PnL</th>
  </tr></thead><tbody>`;
  for (const r of rows) {
    const cls = r.net_pnl >= 0 ? 'pnl-pos' : 'pnl-neg';
    html += `<tr>
      <td>${r.date}</td><td class="center">${r.total}</td>
      <td class="center pnl-pos">${r.wins}</td>
      <td class="center pnl-neg">${r.losses}</td>
      <td class="center">${r.win_pct}</td>
      <td class="${cls}">${r.net_pnl >= 0 ? '+' : ''}${fmt$(r.net_pnl)}</td>
      <td class="${r.cum_pnl >= 0 ? 'pnl-pos' : 'pnl-neg'}">${r.cum_pnl >= 0 ? '+' : ''}${fmt$(r.cum_pnl)}</td>
    </tr>`;
  }
  html += '</tbody></table></div>';
  el.innerHTML = html;
}

function renderHourlySettled() {
  const rows = _hourlyData.settled || [];
  const el   = document.getElementById('hsp-settled');
  if (!rows.length) { el.innerHTML = '<div class="empty">No settled positions yet</div>'; return; }
  let html = `<div class="tbl-wrap"><table><thead><tr>
    <th>Date</th><th>Hour (EDT)</th><th>Threshold</th><th>Forecast</th><th>Dist</th>
    <th>Entry</th><th>Qty</th><th>Net PnL</th><th>Result</th>
  </tr></thead><tbody>`;
  for (const r of rows) {
    const cls = r.result_class === 'green' ? 'pnl-pos' : 'pnl-neg';
    html += `<tr>
      <td>${r.date || '—'}</td><td>${r.hour_label}</td>
      <td>${r.thresh_label}</td><td>${r.fcst_label}</td><td>${r.dist_label}</td>
      <td>${fmt$(r.avg_entry)}</td><td class="center">${r.contracts}</td>
      <td class="${cls}">${r.net_pnl >= 0 ? '+' : ''}${fmt$(r.net_pnl)}</td>
      <td class="${cls}">${r.result_label}</td>
    </tr>`;
  }
  html += '</tbody></table></div>';
  el.innerHTML = html;
}

// ── Boot ───────────────────────────────────────────────────────────────────
document.getElementById('log-follow-btn').classList.add('active');
refreshAll();
startCountdown();
</script>
</body>
</html>"""


# ---------------------------------------------------------------------------
# Main route

# ---------------------------------------------------------------------------
@app.route("/")
def index():
    from flask import Response
    return Response(_HTML, mimetype='text/html')


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="WeatherMachine mobile dashboard")
    parser.add_argument("--port",  type=int, default=5050)
    parser.add_argument("--host",  type=str, default="0.0.0.0")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    log.info("=" * 55)
    log.info("WeatherMachine Dashboard")
    log.info("  http://localhost:%d", args.port)
    log.info("  Tailscale: http://<machine-ip>:%d", args.port)
    log.info("  Mode: %s", "LIVE" if _config.get("live_mode") else "DEMO")
    log.info("=" * 55)

    threading.Thread(target=get_client, daemon=True).start()

    app.run(
        host         = args.host,
        port         = args.port,
        debug        = args.debug,
        threaded     = True,
        use_reloader = False,
    )
