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
import threading
import time
from collections import defaultdict, deque
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

try:
    from flask import Flask, jsonify, render_template_string
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

def _fmt_bracket(bracket: str, mtype: str = "HIGH") -> str:
    if not bracket:
        return bracket
    try:
        if bracket.startswith("B"):
            f = float(bracket[1:])
            return f"{f:.0f}–{f+2:.0f}°"
        elif bracket.startswith("T"):
            v = float(bracket[1:])
            return f"<{v:.0f}°" if mtype == "LOW" else f">{v:.0f}°"
    except ValueError:
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
                and ("HIGH" in s.get("ticker","") or "LOWT" in s.get("ticker",""))]

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
               ("HIGH" in f.get("ticker","") or "LOWT" in f.get("ticker","")):
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
        enriched = []
        for s in temp:
            tk  = s.get("ticker",""); res = s.get("market_result","").lower()
            fee = float(s.get("fee_cost") or 0)
            raw = s.get("settled_time","")
            try:
                dt = datetime.fromisoformat(raw.replace("Z","+00:00"))
                sts = dt.astimezone(ZoneInfo("Europe/Lisbon")).strftime("%Y-%m-%d %H:%M")
            except Exception: sts = raw[:16].replace("T"," ") if raw else ""
            if tk not in fbt: continue
            bfs = [f for f in fbt[tk] if f.get("action") == "buy"]
            if not bfs: continue
            edate = sorted(bfs, key=lambda f: f.get("created_time",""))[0].get("created_time","")[:10]
            sides = [f.get("side") for f in bfs]; our = max(set(sides), key=sides.count)
            of = [f for f in bfs if f.get("side") == our]
            nc = int(sum(float(f.get("count_fp") or 0) for f in of))
            cost = round(sum(
                (float(f.get("yes_price_dollars") or 0) if our=="yes" else
                 1-float(f.get("yes_price_dollars") or 0))
                * float(f.get("count_fp") or 0) for f in of), 4)
            if nc == 0 or cost == 0: continue
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
@app.route("/api/status")
def api_status():
    bal = get_balance(); pos = get_positions()
    unr = sum(p.get("unrealised_pnl",0) for p in pos)
    cur = sum(p.get("current_price",0)*p.get("contracts",1) for p in pos)
    return jsonify({
        "balance":    bal, "deployable": round(bal*0.70,2),
        "portfolio":  round(bal+cur,2),  "unrealised": round(unr,2),
        "mode":       "LIVE" if os.environ.get("KALSHI_DEMO","true")=="false" else "DEMO",
        "open":       len(pos),
    })

# ---------------------------------------------------------------------------
# API — /api/positions
# ---------------------------------------------------------------------------
@app.route("/api/positions")
def api_positions():
    out = []
    for p in get_positions():
        tk = p.get("ticker",""); mt = "HIGH" if "HIGH" in tk else "LOW"
        br = tk.split("-")[-1] if "-" in tk else tk
        ti = _entry_tier(tk)
        eg = "CASCADE" if "cascade" in ti.lower() else ("MAIN" if not ti else ti.upper())
        out.append({"ticker":tk,"market":_city_from_ticker(tk) or tk,
            "city":_city_from_ticker(tk,bare=True) or tk,"market_type":mt,
            "bracket":_fmt_bracket(br,mt),"engine":eg,"side":p.get("side","").upper(),
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
    nws = get_nws(); out = {}
    for city in _CITIES_ORDERED:
        tz  = _ALL_CITIES.get(city,{}).get("tz","UTC")
        now = datetime.now(ZoneInfo(tz)); h = now.hour
        d   = nws.get(city,{})
        ha  = 9 <= h < 15; la = h < 8 or h >= 22
        win = ("HIGH + LOWT" if (ha and la) else "HIGH ▲" if ha else "LOWT ▼" if la else "between")
        out[city] = {"local_time":now.strftime("%H:%M"),"tz_abbr":now.strftime("%Z"),
            "local_hour":h,"obs_hi":d.get("observed_high_f"),"fcst_hi":d.get("forecast_high_f"),
            "obs_lo":d.get("observed_low_f"),"fcst_lo":d.get("forecast_low_f"),
            "window":win,"high_active":ha,"lowt_active":la}
    return jsonify(out)

# ---------------------------------------------------------------------------
# API — /api/session
# ---------------------------------------------------------------------------
@app.route("/api/session")
def api_session():
    positions = get_positions(); trades = _load_trade_log()
    tlog = {t.get("ticker",""): t for t in reversed(trades)}
    entries = []
    for p in positions:
        tk = p.get("ticker",""); t = tlog.get(tk,{})
        ti = (t.get("entry_tier","") or "").lower()
        eg = "CASCADE" if "cascade" in ti else ("MAIN" if not ti else ti.upper())
        sc = t.get("score",0); mt = "HIGH" if "HIGH" in tk else "LOW"
        br = tk.split("-")[-1] if "-" in tk else tk
        pa = t.get("placed_at","")
        entries.append({"ticker":tk,"market":_city_from_ticker(tk) or tk,
            "city":_city_from_ticker(tk,bare=True) or tk,"market_type":mt,
            "bracket":_fmt_bracket(br,mt),"engine":eg,"side":p.get("side","").upper(),
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

    total    = len(enriched); wins=[e for e in enriched if e["won"]]
    win_rate = round(len(wins)/total*100,1) if total else 0
    net_pnl  = round(sum(e["net_pnl"] for e in enriched),2)
    fees     = round(sum(e["fee"]     for e in enriched),2)
    by_day: dict = defaultdict(list)
    for e in enriched: by_day[e["date"]].append(e)
    dpnls     = {d: round(sum(t["net_pnl"] for t in v),2) for d,v in by_day.items()}
    best_day  = max(dpnls.values(), default=0)
    worst_day = min(dpnls.values(), default=0)

    cum=0.0; day_rows=[]
    for day in sorted(by_day.keys()):
        ts=by_day[day]; dw=[t for t in ts if t["won"]]
        dpnl=round(sum(t["net_pnl"] for t in ts),2); cum+=dpnl
        day_rows.append({"date":day,"trades":len(ts),"wins":len(dw),
            "losses":sum(1 for t in ts if not t["won"] and t.get("result")!="EARLY EXIT"),
            "stopped":sum(1 for t in ts if t.get("result")=="EARLY EXIT"),
            "win_pct":f"{round(len(dw)/len(ts)*100,1)}%" if ts else "0%",
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
        if dt_: wr_data.append({"x":day,"y":round(sum(1 for t in dt_ if t["won"])/len(dt_)*100,1)})

    def rl(e):
        if e.get("result")=="EARLY EXIT": return "EXIT ↩","yellow"
        return ("WON ✓","green") if e["won"] else ("LOST ✗","red")

    all_s=[]
    for e in sorted(enriched, key=lambda x: x["date"], reverse=True)[:300]:
        label,cls=rl(e)
        all_s.append({**e,"result_label":label,"result_class":cls,
            "market_label":_city_from_ticker(e["ticker"]) or e["ticker"]})

    return jsonify({"stats":{"total":total,"win_rate":win_rate,"net_pnl":net_pnl,
        "total_fees":fees,"best_day":best_day,"worst_day":worst_day},
        "by_day":day_rows,"chart":{"equity":equity,"win_rate":wr_data},
        "all_settlements":all_s})

# ---------------------------------------------------------------------------
# API — /api/city/<city>
# ---------------------------------------------------------------------------
@app.route("/api/city/<path:city>")
def api_city(city: str):
    enriched,_ = get_settlements(); trades = _load_trade_log()
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
        if tk not in etickers and tk not in seen:
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
        pos_list.append({"date":(i.get("placed_at") or i.get("date") or "")[:10],
            "bracket":_fmt_bracket(br,mt),"market_type":mt,"engine":eg,
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
    lp = os.environ.get("LOG_FILE") or _config.get("log_file")
    if lp:
        p = Path(lp)
        if p.exists():
            try:
                lines = p.read_text(encoding="utf-8",errors="replace").splitlines()
                return jsonify({"lines":lines[-250:],"source":str(p)})
            except Exception: pass
    return jsonify({"lines":_lb.tail(250),"source":"in-process"})

_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<meta name="apple-mobile-web-app-capable" content="yes">
<meta name="apple-mobile-web-app-status-bar-style" content="black-translucent">
<title>WeatherMachine</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@300;400;500;600;700&display=swap" rel="stylesheet">
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
:root{
  --bg:#0d0f14;--panel:#141720;--alt:#181c24;
  --ac:#00d4a0;--acd:#00896a;
  --red:#ff4d6a;--yel:#f5c842;--blu:#5599ff;
  --pri:#e8eaf0;--sec:#7a8099;--bdr:#232736;
  --nav:60px;--hdr:52px;--r:8px;
  --f:'JetBrains Mono','Consolas',monospace;
}
html,body{height:100%;background:var(--bg);color:var(--pri);font-family:var(--f);
  font-size:13px;overscroll-behavior:none;-webkit-font-smoothing:antialiased}
button{font-family:var(--f)}
#app{display:flex;flex-direction:column;height:100dvh}
/* Header */
#hdr{flex-shrink:0;height:var(--hdr);background:var(--panel);
  border-bottom:1px solid var(--bdr);display:flex;align-items:center;
  padding:0 14px;gap:10px;z-index:100}
#logo{color:var(--ac);font-weight:700;font-size:14px;letter-spacing:1.5px;flex:1}
#logo small{color:var(--sec);font-weight:400;font-size:10px;display:block;letter-spacing:1px}
#hdr-r{display:flex;align-items:center;gap:8px}
#mode-b{font-size:10px;letter-spacing:1.5px;padding:3px 7px;border-radius:4px;
  border:1px solid var(--ac);color:var(--ac);font-weight:600}
#mode-b.live{border-color:var(--yel);color:var(--yel)}
#rbtn{background:none;border:1px solid var(--bdr);color:var(--sec);
  padding:5px 9px;border-radius:6px;cursor:pointer;font-size:14px;line-height:1}
#rbtn:hover{border-color:var(--ac);color:var(--ac)}
#cd{color:var(--sec);font-size:11px;min-width:28px;text-align:right}
/* Content + tabs */
#content{flex:1;overflow:hidden}
.tab{display:none;height:100%;overflow-y:auto;
  padding:14px 14px calc(var(--nav) + env(safe-area-inset-bottom) + 10px);
  -webkit-overflow-scrolling:touch}
.tab.on{display:block}
/* Bottom nav */
#nav{position:fixed;bottom:0;left:0;right:0;z-index:200;
  height:calc(var(--nav) + env(safe-area-inset-bottom));
  background:var(--panel);border-top:1px solid var(--bdr);
  display:flex;align-items:flex-start;padding-top:4px}
.nb{flex:1;display:flex;flex-direction:column;align-items:center;justify-content:center;
  gap:3px;background:none;border:none;color:var(--sec);cursor:pointer;
  transition:color .15s;-webkit-tap-highlight-color:transparent;padding:6px 2px}
.nb.on{color:var(--ac)}
.nb svg{width:20px;height:20px}
.nb span{font-size:9px;letter-spacing:.5px}
/* Shared helpers */
.sh{color:var(--sec);font-size:10px;letter-spacing:2px;text-transform:uppercase;
  margin:18px 0 10px}
.sh:first-child{margin-top:0}
.sg{display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-bottom:14px}
.sg3{grid-template-columns:1fr 1fr 1fr}
.sc{background:var(--panel);border:1px solid var(--bdr);border-radius:var(--r);padding:11px 13px}
.sk{color:var(--sec);font-size:9px;letter-spacing:1.5px;text-transform:uppercase;margin-bottom:4px}
.sv{color:var(--pri);font-size:19px;font-weight:600}
.g{color:var(--ac)}.r{color:var(--red)}.y{color:var(--yel)}.b{color:var(--blu)}.d{color:var(--sec)}
/* Balance bar */
#bal{background:var(--panel);border:1px solid var(--bdr);border-radius:var(--r);
  padding:12px 14px;margin-bottom:14px;display:grid;grid-template-columns:1fr 1fr;gap:10px}
.bk{color:var(--sec);font-size:10px;letter-spacing:1px;margin-bottom:2px}
.bv{font-size:17px;font-weight:600}
/* City grid */
#cgrid{display:grid;grid-template-columns:1fr 1fr;gap:7px;margin-bottom:14px}
.cc{background:var(--panel);border:1px solid var(--bdr);border-radius:var(--r);
  padding:9px 11px;transition:border-color .2s}
.cc.ha{border-color:var(--acd)}.cc.la{border-color:#3366aa}.cc.ba{border-color:var(--acd)}
.cn{color:var(--sec);font-size:9px;letter-spacing:1.5px;text-transform:uppercase}
.ct{color:var(--pri);font-size:12px;font-weight:600;margin:3px 0 2px}
.chi{color:var(--ac);font-size:11px}.clo{color:var(--sec);font-size:11px}
.cw{font-size:10px;margin-top:3px;color:var(--sec)}
.cw.h{color:var(--ac)}.cw.l{color:var(--blu)}.cw.b{color:var(--ac)}
/* Position cards */
.pc{background:var(--panel);border:1px solid var(--bdr);border-radius:var(--r);
  padding:12px 13px;margin-bottom:8px}
.ph{display:flex;align-items:baseline;gap:8px;margin-bottom:7px}
.pm{color:var(--pri);font-size:13px;font-weight:500}
.pe{font-size:10px;padding:1px 6px;border-radius:3px;border:1px solid;font-weight:600}
.pe.m{color:var(--ac);border-color:var(--acd)}.pe.c{color:var(--blu);border-color:#335599}
.pr{display:flex;justify-content:space-between;font-size:12px;color:var(--sec);margin-top:3px}
.pv.no{color:var(--ac)}.pv.yes{color:var(--yel)}
.ppnl{font-size:15px;font-weight:600}
.ppnl.pos{color:var(--ac)}.ppnl.neg{color:var(--red)}
/* Tables */
.tw{overflow-x:auto;-webkit-overflow-scrolling:touch;border:1px solid var(--bdr);
  border-radius:var(--r);margin-bottom:14px}
table{width:100%;border-collapse:collapse;min-width:420px;font-size:12px}
th{background:var(--bg);color:var(--sec);padding:8px 10px;text-align:left;
  font-size:10px;letter-spacing:1px;text-transform:uppercase;white-space:nowrap;
  border-bottom:1px solid var(--bdr);font-weight:500}
td{padding:9px 10px;border-bottom:1px solid var(--bdr);white-space:nowrap;color:var(--pri)}
tr:last-child td{border-bottom:none}
tr:nth-child(even) td{background:var(--alt)}
td.c{text-align:center}
/* Charts */
.cbox{background:var(--panel);border:1px solid var(--bdr);border-radius:var(--r);
  padding:12px;margin-bottom:11px}
.cl{color:var(--sec);font-size:10px;letter-spacing:1.5px;text-transform:uppercase;margin-bottom:9px}
.cbox canvas{max-height:155px}
/* Sub-tabs */
.stabs{display:flex;border-bottom:1px solid var(--bdr);margin-bottom:0}
.stb{padding:8px 16px;font-size:11px;font-family:var(--f);color:var(--sec);
  background:none;border:none;cursor:pointer;border-bottom:2px solid transparent;
  margin-bottom:-1px;transition:color .15s;letter-spacing:.5px}
.stb.on{color:var(--ac);border-bottom-color:var(--ac)}
.sp{display:none;padding-top:12px}.sp.on{display:block}
/* City select */
.csel{width:100%;background:var(--panel);color:var(--pri);border:1px solid var(--bdr);
  border-radius:var(--r);padding:10px 28px 10px 12px;font-family:var(--f);font-size:13px;
  margin-bottom:14px;appearance:none;
  background-image:url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='11' height='7'%3E%3Cpath d='M1 1l4.5 5L10 1' stroke='%237a8099' stroke-width='1.5' fill='none' stroke-linecap='round'/%3E%3C/svg%3E");
  background-repeat:no-repeat;background-position:right 11px center}
/* Log */
#log-out{background:var(--bg);border:1px solid var(--bdr);border-radius:var(--r);
  padding:12px;font-size:11px;color:var(--sec);line-height:1.65;white-space:pre-wrap;
  word-break:break-all;min-height:260px;max-height:65vh;overflow-y:auto;
  -webkit-overflow-scrolling:touch}
/* Misc */
.empty{color:var(--sec);font-size:12px;padding:16px;text-align:center;
  border:1px solid var(--bdr);border-radius:var(--r)}
.spin{width:24px;height:24px;border:2px solid var(--bdr);border-top-color:var(--ac);
  border-radius:50%;animation:spin .8s linear infinite;margin:24px auto}
@keyframes spin{to{transform:rotate(360deg)}}
.fbar{display:flex;gap:6px;margin-bottom:12px}
.fb{padding:5px 12px;border-radius:4px;font-family:var(--f);font-size:11px;
  border:1px solid var(--bdr);background:none;color:var(--sec);cursor:pointer}
.fb.on{border-color:var(--ac);color:var(--ac);background:rgba(0,212,160,.08)}
</style>
</head>
<body>
<div id="app">
<header id="hdr">
  <div id="logo">⛅ WeatherMachine<small>KALSHI TEMPERATURE MARKETS</small></div>
  <div id="hdr-r">
    <span id="cd">60s</span>
    <button id="rbtn" onclick="manualRefresh()">↻</button>
    <div id="mode-b">DEMO</div>
  </div>
</header>

<div id="content">
  <!-- HOME -->
  <div id="tab-home" class="tab on">
    <div id="bal">
      <div><div class="bk">Balance</div><div class="bv" id="b-bal">—</div></div>
      <div><div class="bk">Deployable</div><div class="bv g" id="b-dep">—</div></div>
      <div><div class="bk">Portfolio</div><div class="bv" id="b-port">—</div></div>
      <div><div class="bk">Unrealised</div><div class="bv" id="b-unr">—</div></div>
    </div>
    <div class="sh">City Status</div>
    <div id="cgrid">
      {% for city in cities %}
      <div class="cc" id="cc-{{ city|replace(' ','_') }}">
        <div class="cn">{{ city }}</div>
        <div class="ct">--:--</div>
        <div class="chi">hi: --°  fcst: --°</div>
        <div class="clo">lo: --°  fcst: --°</div>
        <div class="cw">—</div>
      </div>
      {% endfor %}
    </div>
    <div class="sh">Open Positions</div>
    <div id="home-pos"><div class="empty">No open positions</div></div>
  </div>

  <!-- SESSION -->
  <div id="tab-session" class="tab">
    <div class="sg sg3">
      <div class="sc"><div class="sk">Entries</div><div class="sv" id="se-ent">—</div></div>
      <div class="sc"><div class="sk">Open</div><div class="sv g" id="se-opn">—</div></div>
      <div class="sc"><div class="sk">Stopped</div><div class="sv" id="se-stp">—</div></div>
      <div class="sc"><div class="sk">Avg Score</div><div class="sv" id="se-scr">—</div></div>
      <div class="sc" style="grid-column:span 2">
        <div class="sk">Unrealised PnL</div><div class="sv" id="se-unr">—</div>
      </div>
    </div>
    <div class="fbar">
      <button class="fb on" onclick="sfilt('all',this)">All</button>
      <button class="fb" onclick="sfilt('HIGH',this)">High</button>
      <button class="fb" onclick="sfilt('LOW',this)">Low</button>
    </div>
    <div id="sess-pos"><div class="empty">No positions this session</div></div>
  </div>

  <!-- PERFORMANCE -->
  <div id="tab-performance" class="tab">
    <div class="sg sg3">
      <div class="sc"><div class="sk">Trades</div><div class="sv" id="pp-tot">—</div></div>
      <div class="sc"><div class="sk">Win Rate</div><div class="sv" id="pp-wr">—</div></div>
      <div class="sc"><div class="sk">Net PnL</div><div class="sv" id="pp-pnl">—</div></div>
      <div class="sc"><div class="sk">Total Fees</div><div class="sv d" id="pp-fee">—</div></div>
      <div class="sc"><div class="sk">Best Day</div><div class="sv g" id="pp-best">—</div></div>
      <div class="sc"><div class="sk">Worst Day</div><div class="sv r" id="pp-worst">—</div></div>
    </div>
    <div class="cbox"><div class="cl">Equity Curve — Cumulative Net PnL</div><canvas id="ch-eq"></canvas></div>
    <div class="cbox"><div class="cl">Rolling 7-Day Win Rate</div><canvas id="ch-wr"></canvas></div>
    <div class="stabs">
      <button class="stb on" onclick="stab('perf','byday',this)">By Day</button>
      <button class="stb" onclick="stab('perf','sett',this)">All Settlements</button>
    </div>
    <div id="perf-byday" class="sp on">
      <div class="tw"><table id="t-byday">
        <thead><tr><th>Date</th><th>T</th><th>W</th><th>L</th><th>Win%</th><th>Net PnL</th><th>Cum PnL</th></tr></thead>
        <tbody></tbody></table></div>
    </div>
    <div id="perf-sett" class="sp">
      <div class="tw"><table id="t-sett">
        <thead><tr><th>Date</th><th>Market</th><th>Side</th><th>Qty</th><th>Result</th><th>Net PnL</th></tr></thead>
        <tbody></tbody></table></div>
    </div>
  </div>

  <!-- CITY HISTORY -->
  <div id="tab-city" class="tab">
    <select class="csel" id="city-sel" onchange="loadCity(this.value)">
      <option value="">— select city —</option>
      {% for city in all_cities %}<option value="{{ city }}">{{ city }}</option>{% endfor %}
    </select>
    <div id="city-body" style="display:none">
      <div class="sg sg3">
        <div class="sc"><div class="sk">Win Rate</div><div class="sv" id="ch-wrs">—</div></div>
        <div class="sc"><div class="sk">Total PnL</div><div class="sv" id="ch-pnl">—</div></div>
        <div class="sc"><div class="sk">Positions</div><div class="sv" id="ch-pos">—</div></div>
        <div class="sc"><div class="sk">Avg Conv Hour</div><div class="sv" id="ch-conv">—</div></div>
        <div class="sc"><div class="sk">Forecast Bias</div><div class="sv" id="ch-bias">—</div></div>
        <div class="sc"><div class="sk">Latest Obs Hi</div><div class="sv" id="ch-obs">—</div></div>
      </div>
      <div class="cbox"><div class="cl">7-Day Rolling Win Rate</div><canvas id="ch-c-wr"></canvas></div>
      <div class="cbox"><div class="cl">Cumulative PnL ($)</div><canvas id="ch-c-pnl"></canvas></div>
      <div class="cbox"><div class="cl">Avg PnL by Entry Hour (Local)</div><canvas id="ch-c-hr"></canvas></div>
      <div class="sh">Position History</div>
      <div class="tw"><table id="t-city">
        <thead><tr><th>Date</th><th>Bracket</th><th>Eng</th><th>Side</th><th>Entry</th><th>Exit</th><th>PnL</th><th>Outcome</th></tr></thead>
        <tbody></tbody></table></div>
    </div>
    <div id="city-spin" style="display:none"><div class="spin"></div></div>
  </div>

  <!-- LOG -->
  <div id="tab-log" class="tab">
    <div class="sh">Activity Log</div>
    <pre id="log-out">Loading...</pre>
  </div>
</div>

<nav id="nav">
  <button class="nb on" onclick="go('home',this)">
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8">
      <path d="M3 12L12 3l9 9M5 10v9a1 1 0 001 1h4v-4h4v4h4a1 1 0 001-1v-9"/>
    </svg><span>Home</span>
  </button>
  <button class="nb" onclick="go('session',this)">
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8">
      <rect x="3" y="4" width="18" height="16" rx="2"/>
      <path d="M7 8h10M7 12h10M7 16h6"/>
    </svg><span>Session</span>
  </button>
  <button class="nb" onclick="go('performance',this)">
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8">
      <polyline points="22 12 18 12 15 21 9 3 6 12 2 12"/>
    </svg><span>Perf</span>
  </button>
  <button class="nb" onclick="go('city',this)">
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8">
      <path d="M12 2C8.13 2 5 5.13 5 9c0 5.25 7 13 7 13s7-7.75 7-13c0-3.87-3.13-7-7-7z"/>
      <circle cx="12" cy="9" r="2.5"/>
    </svg><span>Cities</span>
  </button>
  <button class="nb" onclick="go('log',this)">
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8">
      <path d="M14 2H6a2 2 0 00-2 2v16a2 2 0 002 2h12a2 2 0 002-2V8z"/>
      <polyline points="14 2 14 8 20 8"/>
      <line x1="16" y1="13" x2="8" y2="13"/><line x1="16" y1="17" x2="8" y2="17"/>
    </svg><span>Log</span>
  </button>
</nav>
</div>

<script>
const AC='#00d4a0',RD='#ff4d6a',YL='#f5c842',BL='#5599ff',SC='#7a8099',BD='#232736',PN='#141720';
let activeTab='home', sf='all', perfLoaded=false, sessData=null, cCharts={}, pCharts={};
let countdown=60, cdTimer=null;

// Chart.js defaults
const asc=(cb)=>({
  x:{ticks:{color:SC,font:{family:'JetBrains Mono',size:9},maxTicksLimit:7},grid:{color:BD}},
  y:{ticks:{color:SC,font:{family:'JetBrains Mono',size:9},callback:cb||undefined},grid:{color:BD}}
});
const baseOpts=(cb)=>({responsive:true,maintainAspectRatio:true,
  plugins:{legend:{display:false},tooltip:{mode:'index',intersect:false,
    backgroundColor:PN,titleColor:SC,bodyColor:AC,borderColor:BD,borderWidth:1}},
  scales:asc(cb)});

function mkLine(id,labels,data,color,fill,cb,extra){
  return new Chart(document.getElementById(id).getContext('2d'),{type:'line',
    data:{labels,datasets:[{data,borderColor:color,borderWidth:2,pointRadius:0,
      fill:fill?'origin':false,backgroundColor:fill?`${color}22`:undefined,tension:0.3},
      ...(extra||[])]},options:baseOpts(cb)});
}
function mkBar(id,labels,data,colors,cb){
  return new Chart(document.getElementById(id).getContext('2d'),{type:'bar',
    data:{labels,datasets:[{data,backgroundColor:colors,borderRadius:3}]},options:baseOpts(cb)});
}

// Nav
function go(tab,btn){
  document.querySelectorAll('.tab').forEach(p=>p.classList.remove('on'));
  document.querySelectorAll('.nb').forEach(b=>b.classList.remove('on'));
  document.getElementById('tab-'+tab).classList.add('on');
  btn.classList.add('on'); activeTab=tab; loadTab(tab);
}
function stab(g,n,btn){
  const pre=`#tab-${g} `;
  document.querySelectorAll(pre+'.stb').forEach(b=>b.classList.remove('on'));
  document.querySelectorAll(pre+'.sp').forEach(p=>p.classList.remove('on'));
  btn.classList.add('on');
  document.getElementById(g+'-'+n).classList.add('on');
}
function sfilt(t,btn){
  document.querySelectorAll('.fb').forEach(b=>b.classList.remove('on'));
  btn.classList.add('on'); sf=t;
  if(sessData) renderSessPos(sessData.positions);
}

// Auto-refresh
function startCd(){
  clearInterval(cdTimer); countdown=60;
  cdTimer=setInterval(()=>{
    countdown--; document.getElementById('cd').textContent=countdown+'s';
    if(countdown<=0){refreshAll();countdown=60;}
  },1000);
}
function manualRefresh(){refreshAll();countdown=60;document.getElementById('cd').textContent='60s';}
function refreshAll(){loadTab(activeTab,true);if(activeTab!=='home')loadHome();}
function loadTab(tab,force){
  if(tab==='home')        loadHome();
  if(tab==='session')     loadSession();
  if(tab==='performance') loadPerf(force);
  if(tab==='log')         loadLog();
}

// Home
async function loadHome(){
  const [st,ci,po]=await Promise.all([
    fetch('/api/status').then(r=>r.json()).catch(()=>({})),
    fetch('/api/cities').then(r=>r.json()).catch(()=>({})),
    fetch('/api/positions').then(r=>r.json()).catch(()=>[]),
  ]);
  renderStatus(st); renderCities(ci); renderHomePos(po);
}
function fmt$(v){return v!=null?'$'+v.toFixed(2):'—';}
function renderStatus(s){
  if(!s.balance && s.balance!==0) return;
  document.getElementById('b-bal').textContent=fmt$(s.balance);
  document.getElementById('b-dep').textContent=fmt$(s.deployable);
  document.getElementById('b-port').textContent=fmt$(s.portfolio);
  const u=s.unrealised||0, el=document.getElementById('b-unr');
  el.textContent=(u>=0?'+':'')+fmt$(Math.abs(u));
  el.className='bv '+(u>0?'g':u<0?'r':'');
  const mb=document.getElementById('mode-b');
  mb.textContent=s.mode; mb.className=s.mode==='LIVE'?'live':'';
}
function renderCities(cities){
  for(const[city,d] of Object.entries(cities)){
    const id='cc-'+city.replace(/ /g,'_');
    const el=document.getElementById(id); if(!el) continue;
    const ch=el.children;
    ch[1].textContent=d.local_time+' '+d.tz_abbr;
    const hi=d.obs_hi!=null?d.obs_hi.toFixed(0)+'°':'--°';
    const fhi=d.fcst_hi!=null?d.fcst_hi.toFixed(0)+'°':'--°';
    ch[2].textContent='hi: '+hi+'  fcst: '+fhi;
    const lo=d.obs_lo!=null?d.obs_lo.toFixed(0)+'°':'--°';
    const flo=d.fcst_lo!=null?d.fcst_lo.toFixed(0)+'°':'--°';
    ch[3].textContent='lo: '+lo+'  fcst: '+flo;
    el.className='cc';
    if(d.high_active&&d.lowt_active){el.classList.add('ba');ch[4].className='cw b';ch[4].textContent=d.window;}
    else if(d.high_active){el.classList.add('ha');ch[4].className='cw h';ch[4].textContent=d.window;}
    else if(d.lowt_active){el.classList.add('la');ch[4].className='cw l';ch[4].textContent=d.window;}
    else{ch[4].className='cw';ch[4].textContent='between windows';}
  }
}
function posCard(p){
  const u=p.unrealised||0,s=u>=0?'+':'',cls=u>0?'pos':u<0?'neg':'';
  const eg=(p.engine||'MAIN').toUpperCase(),ec=eg==='CASCADE'?'c':'m';
  const cur=p.current_price?` · curr $${p.current_price.toFixed(2)}`:'';
  return `<div class="pc">
    <div class="ph"><span class="pm">${p.market}</span><span class="pe ${ec}">${eg}</span></div>
    <div class="pr"><span>Side</span><span class="pv ${p.side.toLowerCase()}">${p.side}</span></div>
    <div class="pr"><span>${p.contracts}c @ $${p.avg_cost.toFixed(2)}${cur}</span>
      <span class="ppnl ${cls}">${s}$${Math.abs(u).toFixed(2)}</span></div>
    <div class="pr"><span>${p.bracket} · ${p.market_type}</span>
      <span class="d" style="font-size:11px">${p.last_updated?p.last_updated.slice(0,16).replace('T',' '):'—'}</span>
    </div></div>`;
}
function renderHomePos(pos){
  const el=document.getElementById('home-pos');
  el.innerHTML=pos.length?pos.map(posCard).join(''):'<div class="empty">No open positions</div>';
}

// Session
async function loadSession(){
  sessData=await fetch('/api/session').then(r=>r.json()).catch(()=>null);
  if(!sessData) return;
  document.getElementById('se-ent').textContent=sessData.entries;
  document.getElementById('se-opn').textContent=sessData.open;
  document.getElementById('se-stp').textContent=sessData.stopped;
  document.getElementById('se-scr').textContent=sessData.avg_score;
  const u=sessData.unrealised||0, el=document.getElementById('se-unr');
  el.textContent=(u>=0?'+':'')+'$'+Math.abs(u).toFixed(2);
  el.className='sv '+(u>0?'g':u<0?'r':'');
  renderSessPos(sessData.positions);
}
function renderSessPos(pos){
  let f=pos;
  if(sf==='HIGH') f=pos.filter(p=>p.market_type==='HIGH');
  if(sf==='LOW')  f=pos.filter(p=>p.market_type==='LOW');
  const el=document.getElementById('sess-pos');
  el.innerHTML=f.length?f.map(p=>{
    const u=p.unrealised||0,s=u>=0?'+':'',cls=u>0?'pos':u<0?'neg':'';
    const eg=(p.engine||'MAIN').toUpperCase(),ec=eg==='CASCADE'?'c':'m';
    return `<div class="pc">
      <div class="ph"><span class="pm">${p.market}</span><span class="pe ${ec}">${eg}</span></div>
      <div class="pr"><span>Side</span><span class="pv ${p.side.toLowerCase()}">${p.side}</span></div>
      <div class="pr"><span>${p.contracts}c @ $${p.avg_cost.toFixed(2)} · ${p.score}</span>
        <span class="ppnl ${cls}">${s}$${Math.abs(u).toFixed(2)}</span></div>
      <div class="pr"><span>${p.bracket} · ${p.market_type}</span>
        <span class="d">${p.entered_at||'—'}</span></div></div>`;
  }).join(''):'<div class="empty">No positions</div>';
}

// Performance
async function loadPerf(force){
  if(perfLoaded&&!force) return;
  const d=await fetch('/api/performance').then(r=>r.json()).catch(()=>null);
  if(!d) return; perfLoaded=true;
  if(d.stats&&d.stats.total) renderPerfStats(d.stats);
  if(d.chart) renderPerfCharts(d.chart);
  if(d.by_day) renderByDay(d.by_day);
  if(d.all_settlements) renderSett(d.all_settlements);
}
function renderPerfStats(s){
  document.getElementById('pp-tot').textContent=s.total;
  const we=document.getElementById('pp-wr');
  we.textContent=s.win_rate+'%'; we.className='sv '+(s.win_rate>=70?'g':s.win_rate>=50?'y':'r');
  const pe=document.getElementById('pp-pnl');
  pe.textContent=(s.net_pnl>=0?'+':'')+'$'+s.net_pnl.toFixed(2);
  pe.className='sv '+(s.net_pnl>=0?'g':'r');
  document.getElementById('pp-fee').textContent='$'+s.total_fees.toFixed(2);
  document.getElementById('pp-best').textContent='+$'+s.best_day.toFixed(2);
  document.getElementById('pp-worst').textContent='$'+s.worst_day.toFixed(2);
}
function renderPerfCharts(c){
  if(pCharts.eq){pCharts.eq.destroy();pCharts.wr.destroy();}
  const ref={data:c.win_rate.map(()=>70),borderColor:'rgba(0,137,106,.4)',
    borderWidth:1,borderDash:[4,4],pointRadius:0,fill:false};
  pCharts.eq=mkLine('ch-eq',c.equity.map(p=>p.x),c.equity.map(p=>p.y),AC,true,v=>'$'+v.toFixed(2));
  pCharts.wr=mkLine('ch-wr',c.win_rate.map(p=>p.x),c.win_rate.map(p=>p.y),YL,true,v=>v+'%',[ref]);
  pCharts.wr.options.scales.y.min=0;pCharts.wr.options.scales.y.max=100;pCharts.wr.update();
}
function renderByDay(rows){
  document.querySelector('#t-byday tbody').innerHTML=rows.map(r=>`<tr>
    <td class="d">${r.date}</td><td class="c">${r.trades}</td>
    <td class="c g">${r.wins}</td>
    <td class="c ${r.losses>0?'r':'d'}">${r.losses}</td>
    <td class="c">${r.win_pct}</td>
    <td class="${r.net_pnl>=0?'g':'r'}">${r.net_pnl>=0?'+':''}$${r.net_pnl.toFixed(2)}</td>
    <td class="${r.cum_pnl>=0?'g':'r'}">${r.cum_pnl>=0?'+':''}$${r.cum_pnl.toFixed(2)}</td>
  </tr>`).join('');
}
function renderSett(rows){
  document.querySelector('#t-sett tbody').innerHTML=rows.map(r=>`<tr>
    <td class="d">${r.date}</td><td>${r.market_label||r.ticker}</td>
    <td class="${r.side==='NO'?'g':'y'}">${r.side}</td>
    <td class="c">${r.contracts}</td>
    <td class="${r.result_class}">${r.result_label}</td>
    <td class="${r.net_pnl>=0?'g':'r'}">${r.net_pnl>=0?'+':''}$${r.net_pnl.toFixed(2)}</td>
  </tr>`).join('');
}

// City history
async function loadCity(city){
  if(!city) return;
  document.getElementById('city-body').style.display='none';
  document.getElementById('city-spin').style.display='block';
  const d=await fetch('/api/city/'+encodeURIComponent(city)).then(r=>r.json()).catch(()=>null);
  document.getElementById('city-spin').style.display='none';
  if(!d) return;
  document.getElementById('city-body').style.display='block';
  renderCityStats(d.stats); renderCityCharts(d.chart); renderCityPos(d.positions);
}
function renderCityStats(s){
  const we=document.getElementById('ch-wrs');
  we.textContent=s.win_rate+'%'; we.className='sv '+(s.win_rate>=90?'g':s.win_rate>=75?'y':'r');
  const pe=document.getElementById('ch-pnl');
  pe.textContent=(s.total_pnl>=0?'+':'')+'$'+s.total_pnl.toFixed(2);
  pe.className='sv '+(s.total_pnl>=0?'g':'r');
  document.getElementById('ch-pos').textContent=s.positions;
  document.getElementById('ch-conv').textContent=s.avg_conv;
  document.getElementById('ch-bias').textContent=s.bias;
  document.getElementById('ch-obs').textContent=s.obs_hi;
}
function renderCityCharts(c){
  for(const k of Object.keys(cCharts)) if(cCharts[k]){cCharts[k].destroy();cCharts[k]=null;}
  cCharts.wr=mkLine('ch-c-wr',c.rolling_wr.map(p=>p.x),c.rolling_wr.map(p=>p.y),AC,true,v=>v+'%');
  cCharts.wr.options.scales.y.min=0;cCharts.wr.options.scales.y.max=100;cCharts.wr.update();
  cCharts.pnl=mkLine('ch-c-pnl',c.cum_pnl.map(p=>p.x),c.cum_pnl.map(p=>p.y),AC,true,v=>'$'+v.toFixed(2));
  const hd=c.by_hour,avgs=hd.map(p=>p.avg);
  cCharts.hr=mkBar('ch-c-hr',hd.map(p=>p.hour+'h'),avgs,
    avgs.map(v=>v>=0?'rgba(0,212,160,.7)':'rgba(255,77,106,.7)'),v=>'$'+v.toFixed(2));
}
function renderCityPos(pos){
  document.querySelector('#t-city tbody').innerHTML=pos.map(p=>`<tr>
    <td class="d">${p.date}</td><td>${p.bracket}</td>
    <td class="${p.engine==='CASCADE'?'b':'g'}">${p.engine}</td>
    <td class="${p.side==='NO'?'g':'y'}">${p.side}</td>
    <td>${p.entry}</td><td class="d">${p.exit}</td>
    <td class="${p.pnl>0?'g':p.pnl<0?'r':'d'}">${p.pnl!==0?(p.pnl>0?'+':'')+'$'+p.pnl.toFixed(2):'—'}</td>
    <td class="${p.outcome_class}">${p.outcome_label}</td>
  </tr>`).join('');
}

// Log
async function loadLog(){
  const d=await fetch('/api/log').then(r=>r.json()).catch(()=>null);
  const el=document.getElementById('log-out');
  if(!d){el.textContent='Failed to load log';return;}
  el.textContent=d.lines.join('\n'); el.scrollTop=el.scrollHeight;
}

// Boot
loadHome(); startCd();
</script>
</body>
</html>"""


# ---------------------------------------------------------------------------
# Main route
# ---------------------------------------------------------------------------
@app.route("/")
def index():
    return render_template_string(
        _HTML,
        cities=_CITIES_ORDERED,
        all_cities=sorted(_ALL_CITIES.keys()),
    )


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
