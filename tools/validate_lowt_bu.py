#!/usr/bin/env python3
"""
validate_lowt_bu.py — re-validate the cascade_lowt_bu signal against AUTHORITATIVE
settlements (not temperature-derived), binned by entry price.

The docstring backtest claimed 97.2% WR, but was scored against a settlement
proxy we know is ~88% accurate. This re-scores the REAL entries (from the trade
log) against the settlements table — ground truth — to see whether the win rate
holds, and specifically how the sub-0.85 entries (the manage_open_orders floor
question) actually performed.

Reads data/trade_log.json + data/observations.db (settlements table).

USAGE (on the Pi, from repo root):
    python3 tools/validate_lowt_bu.py
"""
import json
import math
import sqlite3
from collections import defaultdict
from pathlib import Path

BANDS = [(0.00, 0.70), (0.70, 0.75), (0.75, 0.80), (0.80, 0.85),
         (0.85, 0.90), (0.90, 0.95), (0.95, 1.01)]


def wilson_lower(wins, n, z=1.96):
    if n == 0:
        return 0.0
    p = wins / n
    denom = 1 + z*z/n
    centre = p + z*z/(2*n)
    margin = z*math.sqrt((p*(1-p) + z*z/(4*n))/n)
    return (centre - margin)/denom


def fee(price):
    return math.ceil(0.07*price*(1-price)*100)/100.0


def ev_per_contract(price, wr):
    """No bet: win pays (1-price), lose costs price, fee charged either way."""
    return wr*(1-price) - (1-wr)*price - fee(price)


def band_of(p):
    for lo, hi in BANDS:
        if lo <= p < hi:
            return (lo, hi)
    return None


def main():
    trades = json.load(open("data/trade_log.json"))
    bu = [t for t in trades if t.get("entry_tier") == "cascade_lowt_bu"]
    con = sqlite3.connect("data/observations.db")
    settled = dict(con.execute(
        "SELECT ticker, result FROM settlements WHERE result IN ('yes','no')"))

    by_band = defaultdict(lambda: {"w": 0, "l": 0, "unsettled": 0})
    for t in bu:
        p = t.get("entry_price")
        if p is None:
            continue
        b = band_of(float(p))
        if b is None:
            continue
        res = settled.get(t.get("ticker"))
        cell = by_band[b]
        if res == "no":
            cell["w"] += 1
        elif res == "yes":
            cell["l"] += 1
        else:
            cell["unsettled"] += 1

    print(f"cascade_lowt_bu — real entries scored vs AUTHORITATIVE settlement\n")
    print(f"{'band':14} {'settled':>7} {'win':>4} {'loss':>5} {'WR':>6} "
          f"{'Wilson_LB':>10} {'EV/contract':>12} {'open':>5}")
    print("-" * 72)

    tot_w = tot_l = 0
    sub_w = sub_l = 0
    for lo, hi in BANDS:
        c = by_band.get((lo, hi), {"w": 0, "l": 0, "unsettled": 0})
        w, l, u = c["w"], c["l"], c["unsettled"]
        n = w + l
        tot_w += w; tot_l += l
        if hi <= 0.85:
            sub_w += w; sub_l += l
        if n == 0:
            wr_s = lb_s = ev_s = "—"
        else:
            wr = w/n
            mid = (lo+hi)/2
            wr_s = f"{wr*100:.0f}%"
            lb_s = f"{wilson_lower(w,n)*100:.0f}%"
            ev_s = f"${ev_per_contract(mid, wr):+.3f}"
        print(f"[{lo:.2f},{hi:.2f})  {n:>7} {w:>4} {l:>5} {wr_s:>6} "
              f"{lb_s:>10} {ev_s:>12} {u:>5}")

    print("-" * 72)
    tn = tot_w + tot_l
    print(f"{'ALL settled':14} {tn:>7} {tot_w:>4} {tot_l:>5} "
          f"{(tot_w/tn*100 if tn else 0):.0f}%")
    sn = sub_w + sub_l
    print(f"{'sub-0.85':14} {sn:>7} {sub_w:>4} {sub_l:>5} "
          f"{(sub_w/sn*100 if sn else 0):.0f}%  "
          f"Wilson_LB={wilson_lower(sub_w,sn)*100:.0f}%" if sn else
          f"{'sub-0.85':14} {sn:>7} (none settled yet)")

    print(f"\nClaimed backtest WR: 97.2% (vs proxy). Above is vs ground truth.")
    print(f"NOTE: small N per band — read Wilson_LB, not raw WR. The sub-0.85")
    print(f"rows are the manage_open_orders floor question; {sn} settled so far")
    print(f"is too few to set a floor — directional only. {sum(c['unsettled'] for c in by_band.values())} entries still open.")


if __name__ == "__main__":
    main()
