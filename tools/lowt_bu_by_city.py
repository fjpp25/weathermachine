#!/usr/bin/env python3
"""
lowt_bu_by_city.py — per-city net contribution of LOW (sub-0.85) cascade_lowt_bu
entries, to decide whether specific cities should be excluded/floored rather than
applying a blanket entry floor.

The clustering probe showed low-entry losses concentrate in NYC/Chicago/LA/Seattle
(coastal + station-quirky cities). But loss COUNT isn't the verdict — a city can
have many losses and still be net +EV if it has many more wins. This reports each
city's net PnL and Wilson-floored win rate for sub-0.85 entries, so only the
genuinely unprofitable cities are flagged.

USAGE (on the Pi, from repo root):
    python3 tools/lowt_bu_by_city.py
"""
import math
import sqlite3
import importlib.util
from collections import defaultdict

spec = importlib.util.spec_from_file_location("bt", "tools/backtest_lowt_bu.py")
bt = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bt)


def wilson_lower(w, n, z=1.96):
    if n == 0:
        return 0.0
    p = w/n
    return (p + z*z/(2*n) - z*math.sqrt((p*(1-p)+z*z/(4*n))/n)) / (1 + z*z/n)


def pnl_of(e):
    f = bt.fee(e["entry_no"]) * e["contracts"]
    if e["result"] == "no":
        return (1-e["entry_no"])*e["contracts"] - f
    if e["result"] == "yes":
        return -e["entry_no"]*e["contracts"] - f
    return 0.0


def main():
    con = sqlite3.connect("data/observations.db")
    entries = bt.replay(con, floor=0.60)
    con.close()

    # SUB-0.85 entries only — the contested low band.
    low = [e for e in entries if e["entry_no"] < 0.85 and e["result"] in ("no", "yes")]

    by_city = defaultdict(lambda: {"w": 0, "l": 0, "pnl": 0.0})
    for e in low:
        c = by_city[e["city"]]
        if e["result"] == "no":
            c["w"] += 1
        else:
            c["l"] += 1
        c["pnl"] += pnl_of(e)

    print("Sub-0.85 cascade_lowt_bu entries, per city (settled only):\n")
    print(f"{'city':16}{'N':>5}{'win':>5}{'loss':>5}{'WR':>6}{'Wil_LB':>8}{'netPnL':>9}")
    print("-" * 54)
    # sort by net PnL ascending — worst (exclusion candidates) at top
    for city, d in sorted(by_city.items(), key=lambda kv: kv[1]["pnl"]):
        n = d["w"] + d["l"]
        wr = d["w"]/n if n else 0
        flag = ""
        if d["pnl"] < 0:
            flag = "  <- NET NEGATIVE"
        elif wilson_lower(d["w"], n) < 0.5:
            flag = "  <- weak floor"
        print(f"{city:16}{n:>5}{d['w']:>5}{d['l']:>5}{wr*100:>5.0f}%"
              f"{wilson_lower(d['w'],n)*100:>7.0f}%{d['pnl']:>+9.2f}{flag}")

    # totals + what excluding the net-negative cities would do
    neg_cities = [c for c, d in by_city.items() if d["pnl"] < 0]
    tot = sum(d["pnl"] for d in by_city.values())
    saved = -sum(by_city[c]["pnl"] for c in neg_cities)
    print("-" * 54)
    print(f"total sub-0.85 net PnL: ${tot:+.2f}")
    print(f"net-negative cities: {neg_cities}")
    print(f"excluding them would change sub-0.85 PnL by: ${saved:+.2f} "
          f"-> ${tot+saved:+.2f}")
    print("\nNOTE: a city being net-negative on sub-0.85 is the exclusion signal.\n"
          "High loss COUNT alone (e.g. a high-volume city) is not — check netPnL.\n"
          "Small N per city still applies; treat single-digit-N cities as weak.")


if __name__ == "__main__":
    main()
