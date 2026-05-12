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
<style>
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
:root{
  --bg:#0b0d12;--panel:#111318;--panel2:#161920;--alt:#13161e;
  --ac:#00d4a0;--acd:#009970;--ac2:rgba(0,212,160,.12);
  --red:#ff4d6a;--red2:rgba(255,77,106,.12);
  --yel:#f5c518;--yel2:rgba(245,197,24,.12);
  --blu:#4d9fff;
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
  display:flex;align-items:stretch;padding:0 8px;flex-shrink:0;overflow-x:auto}
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
  border-bottom:1px solid var(--bdr);flex-wrap:wrap;flex-shrink:0}
.bal-item{}
.bal-k{color:var(--sec);font-size:9px;letter-spacing:2px;text-transform:uppercase;margin-bottom:3px}
.bal-v{font-size:16px;font-weight:600;color:var(--pri)}
.bal-v.g{color:var(--ac)}
.bal-v.r{color:var(--red)}

/* ── City grid ── */
#cgrid{display:grid;grid-template-columns:repeat(5,1fr);gap:8px;margin-bottom:20px}
@media(max-width:1200px){#cgrid{grid-template-columns:repeat(4,1fr)}}
@media(max-width:900px){#cgrid{grid-template-columns:repeat(3,1fr)}}
@media(max-width:600px){#cgrid{grid-template-columns:repeat(2,1fr)}}
.cc{background:var(--panel);border:1px solid var(--bdr);border-radius:var(--r2);
  padding:12px 14px;cursor:pointer;transition:border-color .2s,background .2s}
.cc:hover{border-color:var(--ter);background:var(--panel2)}
.cc.ha{border-color:var(--acd)}.cc.la{border-color:#2a4a88}
.cn{color:var(--sec);font-size:9px;letter-spacing:1.5px;text-transform:uppercase}
.ctime{color:var(--pri);font-size:11px;font-weight:500;margin:4px 0 2px}
.cnow{color:var(--ac);font-size:12px;font-weight:600}
.chi{color:var(--ac);font-size:11px}
.clo{color:var(--sec);font-size:11px}
.cwin{font-size:10px;margin-top:4px;color:var(--sec)}
.cwin.ha{color:var(--ac)}.cwin.la{color:var(--blu)}

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
.side-no{color:var(--ac)}.side-yes{color:var(--yel)}
.pnl-pos{color:var(--ac)}.pnl-neg{color:var(--red)}
.status-live{color:var(--ac)}.status-settled{color:var(--sec)}

/* ── Sub-tabs ── */
.stabs{display:flex;border-bottom:1px solid var(--bdr);margin-bottom:16px;gap:0}
.stb{padding:8px 16px;font-size:11px;color:var(--sec);background:none;border:none;
  border-bottom:2px solid transparent;cursor:pointer;letter-spacing:.5px;transition:color .15s}
.stb:hover{color:var(--pri)}.stb.on{color:var(--ac);border-bottom-color:var(--ac)}
.sp{display:none}.sp.on{display:block}

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
.chart-box{background:var(--panel);border:1px solid var(--bdr);border-radius:var(--r2);
  padding:16px;margin-bottom:16px}
.chart-lbl{color:var(--sec);font-size:9px;letter-spacing:2px;text-transform:uppercase;margin-bottom:12px}
.chart-box canvas{max-height:200px}
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
  <div class="bal-item"><div class="bal-k">Deployable</div><div class="bal-v" id="b-dep">—</div></div>
  <div class="bal-item"><div class="bal-k">Portfolio</div><div class="bal-v" id="b-prt">—</div></div>
  <div class="bal-item"><div class="bal-k">Unrealised</div><div class="bal-v" id="b-unr">—</div></div>
  <div class="bal-item"><div class="bal-k">Open</div><div class="bal-v" id="b-open">—</div></div>
</div>

<!-- Tab bar -->
<div id="tabbar">
  <button class="tb on" data-tab="home" onclick="switchTab('home',this)">Home</button>
  <button class="tb" data-tab="session" onclick="switchTab('session',this)">Session</button>
  <button class="tb" data-tab="log" onclick="switchTab('log',this)">Log</button>
  <button class="tb" data-tab="perf" onclick="switchTab('perf',this)">Performance</button>
</div>

<!-- Content -->
<div id="content">

<!-- ── HOME ── -->
<div class="tab on" id="tab-home">
  <div class="sh">CITY STATUS</div>
  <div id="cgrid"></div>
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
</div>

<!-- ── LOG ── -->
<div class="tab" id="tab-log">
  <div class="refresh-bar">
    <button class="btn" onclick="loadLog()">↻ Refresh</button>
    <button class="btn" id="log-follow-btn" onclick="toggleFollow()" title="Auto-scroll to bottom">Follow</button>
  </div>
  <pre id="log-out">Loading...</pre>
</div>

<!-- ── PERFORMANCE ── -->
<div class="tab" id="tab-perf">
  <div class="sh">PERFORMANCE</div>
  <div class="stat-grid" id="perf-stats"></div>
  <div class="chart-box"><div class="chart-lbl">EQUITY CURVE</div><canvas id="chart-equity"></canvas></div>
  <div class="chart-box"><div class="chart-lbl">7-DAY ROLLING WIN RATE</div><canvas id="chart-wr"></canvas></div>
  <div class="sh">SETTLEMENT HISTORY</div>
  <div class="tbl-wrap" id="perf-table-wrap"></div>
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
let _sessFilt = 'all';
let _autoRefresh;
let _cdSecs = 60;
let _cdInt;
let _logFollow = true;
let _charts = {};

const fmt$ = v => v == null ? '—' : '$' + v.toFixed(2);
const fmtPct = v => v == null ? '—' : v.toFixed(1) + '%';
const clsPnl = v => v > 0 ? 'pnl-pos' : v < 0 ? 'pnl-neg' : '';
const clsEng = e => ({'MAIN':'main','CASCADE':'cascade','NEAR_CAP':'near_cap',
  'TOMORROW_DISMISSED':'tomorrow'}[e?.toUpperCase()] ?? 'main');

// ── Tab switching ──────────────────────────────────────────────────────────
function switchTab(id, btn) {
  document.querySelectorAll('.tab').forEach(t => t.classList.remove('on'));
  document.querySelectorAll('.tb').forEach(b => b.classList.remove('on'));
  document.getElementById('tab-' + id).classList.add('on');
  btn.classList.add('on');
  if (id === 'session') loadSession();
  if (id === 'log')     loadLog();
  if (id === 'perf')    loadPerf();
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
async function loadStatus() {
  try {
    const d = await fetch('/api/status').then(r => r.json());
    document.getElementById('b-bal').textContent  = fmt$(d.balance);
    document.getElementById('b-dep').textContent  = fmt$(d.deployable);
    document.getElementById('b-prt').textContent  = fmt$(d.portfolio);
    document.getElementById('b-open').textContent = d.open ?? '—';
    const unr = d.unrealised ?? 0;
    const uEl = document.getElementById('b-unr');
    uEl.textContent = (unr >= 0 ? '+' : '') + fmt$(unr);
    uEl.className   = 'bal-v ' + clsPnl(unr);
    const mb = document.getElementById('mode-badge');
    mb.textContent  = d.mode;
    mb.className    = 'mode-badge' + (d.mode === 'LIVE' ? ' live' : '');
  } catch(e) { console.warn('status', e); }
}

// ── Cities ─────────────────────────────────────────────────────────────────
async function loadCities() {
  try {
    const cities = await fetch('/api/cities').then(r => r.json());
    const grid = document.getElementById('cgrid');
    grid.innerHTML = '';
    {% for city in cities %}
    (function() {
      const city = {{ city | tojson }};
      const d = cities[city] || {};
      const div = document.createElement('div');
      const ha = d.high_active, la = d.lowt_active;
      div.className = 'cc' + (ha ? ' ha' : '') + (la ? ' la' : '');
      div.onclick = () => openCityModal(city);
      const now  = d.now  != null ? d.now.toFixed(0) + '°' : '—';
      const obsH = d.obs_hi  != null ? d.obs_hi.toFixed(0)  + '°' : '--°';
      const fcsH = d.fcst_hi != null ? d.fcst_hi.toFixed(0) + '°' : '--°';
      const obsL = d.obs_lo  != null ? d.obs_lo.toFixed(0)  + '°' : '--°';
      const fcsL = d.fcst_lo != null ? d.fcst_lo.toFixed(0) + '°' : '--°';
      const win  = d.window || 'between windows';
      const winCls = ha && la ? 'ba' : ha ? 'ha' : la ? 'la' : '';
      div.innerHTML = `
        <div class="cn">${city}</div>
        <div class="ctime">${d.local_time || '--:--'} ${d.tz_abbr || ''}</div>
        <div class="cnow">now: ${now}</div>
        <div class="chi">hi: ${obsH}&nbsp; fcst: ${fcsH}</div>
        <div class="clo">lo: ${obsL}&nbsp; fcst: ${fcsL}</div>
        <div class="cwin ${winCls}">${win}</div>`;
      grid.appendChild(div);
    })();
    {% endfor %}
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
async function loadLog() {
  try {
    const d = await fetch('/api/log').then(r => r.json());
    const el = document.getElementById('log-out');
    const lines = (d.lines || []).join('\n');
    // Colour-code log lines
    el.innerHTML = lines.split('\n').map(line => {
      let cls = 'log-info';
      if (/WARNING|WARN/.test(line))  cls = 'log-warn';
      if (/ERROR|FAIL/.test(line))    cls = 'log-err';
      if (/★|SIGNAL|NEAR_CAP|GRAD/.test(line)) cls = 'log-sig';
      if (/order|placed|BUY|SELL/.test(line))  cls = 'log-trade';
      return `<span class="${cls}">${escHtml(line)}</span>`;
    }).join('\n');
    if (_logFollow) el.scrollTop = el.scrollHeight;
  } catch(e) { console.warn('log', e); }
}

function toggleFollow() {
  _logFollow = !_logFollow;
  document.getElementById('log-follow-btn').classList.toggle('active', _logFollow);
}

function escHtml(s) {
  return s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
}

// ── Performance ────────────────────────────────────────────────────────────
async function loadPerf() {
  try {
    const d = await fetch('/api/performance').then(r => r.json());
    const s = d.stats || {};
    const stats = [
      ['Win Rate', fmtPct(s.win_rate), s.win_rate >= 85 ? 'g' : ''],
      ['Net PnL',  fmt$(s.net_pnl),    s.net_pnl  >= 0  ? 'g' : 'r'],
      ['Total Fees', fmt$(s.total_fees), ''],
      ['Total Trades', s.total ?? '—', ''],
      ['Best Day',  fmt$(s.best_day),  'g'],
      ['Worst Day', fmt$(s.worst_day), s.worst_day < 0 ? 'r' : ''],
    ];
    document.getElementById('perf-stats').innerHTML = stats.map(([k,v,c])=>
      `<div class="stat-card"><div class="stat-k">${k}</div><div class="stat-v ${c}">${v}</div></div>`
    ).join('');

    // Equity chart
    _buildChart('chart-equity', d.chart?.equity || [], 'Equity ($)', 'var(--ac)', 'var(--ac2)');
    _buildChart('chart-wr',     d.chart?.win_rate || [], 'Win Rate (%)', 'var(--blu)', 'rgba(77,159,255,.1)');

    // Settlement table
    const rows = d.all_settlements || [];
    if (!rows.length) {
      document.getElementById('perf-table-wrap').innerHTML = '<div class="empty">No settlements yet</div>';
      return;
    }
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
  } catch(e) { console.warn('perf', e); }
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
async function openCityModal(city) {
  document.getElementById('modal-bg').classList.add('on');
  document.getElementById('modal-content').innerHTML =
    '<div style="text-align:center;padding:24px"><span class="spin"></span> Loading ' + city + '...</div>';
  try {
    const d = await fetch('/api/city/' + encodeURIComponent(city)).then(r => r.json());
    const s = d.stats || {};
    const pos = d.positions || [];
    let html = `<h2 style="color:var(--ac);font-size:16px;font-weight:700;margin-bottom:16px">⛅ ${city}</h2>`;
    html += `<div class="stat-grid" style="grid-template-columns:repeat(3,1fr);margin-bottom:16px">
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
    document.getElementById('modal-content').innerHTML = html;
  } catch(e) {
    document.getElementById('modal-content').innerHTML = '<div class="empty">Failed to load city data</div>';
  }
}

function closeModal(e) {
  if (!e || e.target === document.getElementById('modal-bg'))
    document.getElementById('modal-bg').classList.remove('on');
}

document.addEventListener('keydown', e => { if (e.key==='Escape') closeModal(); });

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
