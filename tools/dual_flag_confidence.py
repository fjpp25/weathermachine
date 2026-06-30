#!/usr/bin/env python3
"""
dual_flag_confidence.py — does a LOWT bracket flagged by BOTH lowt_a and
cascade_lowt_bu settle No more often than a bracket flagged by only one?

Tests the "two engines agreeing = higher conviction" hypothesis with data
before we decide whether the current double-ordering is a feature (size up
deliberately) or a bug (de-dup to one).

Groups every real LOWT entry by ticker, labels each ticker as flagged by
{lowt_a only, cascade_lowt_bu only, BOTH}, joins to authoritative settlement,
and compares win rates with Wilson floors.

USAGE (on the Pi, from repo root):
    python3 tools/dual_flag_confidence.py
"""
import json
import math
import sqlite3
from collections import defaultdict


def wilson_lower(w, n, z=1.96):
    if n == 0:
        return 0.0
    p = w/n
    return (p + z*z/(2*n) - z*math.sqrt((p*(1-p)+z*z/(4*n))/n)) / (1 + z*z/n)


def main():
    trades = json.load(open("data/trade_log.json"))
    con = sqlite3.connect("data/observations.db")
    settled = dict(con.execute(
        "SELECT ticker, result FROM settlements WHERE result IN ('yes','no')"))
    con.close()

    # which engines flagged each ticker (LOWT only)
    flags = defaultdict(set)
    for t in trades:
        tier = t.get("entry_tier", "")
        tk = t.get("ticker", "")
        if tier in ("lowt_a", "cascade_lowt_bu") and tk.startswith("KXLOWT"):
            flags[tk].add(tier)

    groups = {"lowt_a only": ("lowt_a",), "cascade_lowt_bu only": ("cascade_lowt_bu",),
              "BOTH": None}
    stats = {g: {"w": 0, "l": 0} for g in groups}

    for tk, engs in flags.items():
        res = settled.get(tk)
        if res not in ("yes", "no"):
            continue
        if engs == {"lowt_a"}:
            g = "lowt_a only"
        elif engs == {"cascade_lowt_bu"}:
            g = "cascade_lowt_bu only"
        elif engs == {"lowt_a", "cascade_lowt_bu"}:
            g = "BOTH"
        else:
            continue
        if res == "no":
            stats[g]["w"] += 1
        else:
            stats[g]["l"] += 1

    print("Does dual-engine agreement predict a higher No-settle rate?\n")
    print(f"{'group':22}{'N':>5}{'win':>5}{'loss':>5}{'WR':>7}{'Wilson_LB':>11}")
    print("-" * 55)
    for g in ("lowt_a only", "cascade_lowt_bu only", "BOTH"):
        w, l = stats[g]["w"], stats[g]["l"]
        n = w + l
        wr = f"{w/n*100:.1f}%" if n else "—"
        lb = f"{wilson_lower(w,n)*100:.1f}%" if n else "—"
        print(f"{g:22}{n:>5}{w:>5}{l:>5}{wr:>7}{lb:>11}")
    print("-" * 55)
    both = stats["BOTH"]
    bn = both["w"] + both["l"]
    print(f"\nInterpretation:")
    print(f"  - If BOTH's WR (and Wilson_LB) is clearly above the single-engine")
    print(f"    groups -> agreement IS a confidence signal; size up deliberately.")
    print(f"  - If BOTH is similar to single -> agreement adds no edge; de-dup.")
    print(f"  - BOTH N={bn}. If small, this is directional only — note and revisit.")


if __name__ == "__main__":
    main()
