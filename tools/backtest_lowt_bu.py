#!/usr/bin/env python3
"""
backtest_lowt_bu.py — replay the cascade_lowt_bu signal over the full observation
history, scored against AUTHORITATIVE settlements, with the entry floor as a swept
parameter.

WHY: real cascade_lowt_bu trades are too few (esp. sub-0.85) to set an entry floor.
This replays the engine's gates over ALL observed LOWT market-days to generate
every entry the engine WOULD have made, scores each against ground truth, and
sweeps the entry floor so we can see WR/EV/PnL as a function of where the bar sits.

FIDELITY (ported from cascade_engine._lowt_bu_signals):
  - trigger: a B bracket confirms No >= CONV_THRESHOLD (0.97); target = bracket
    immediately above it (rank i+1), within MAX_RANK_FROM_BOTTOM (2).
  - entry band: NO_MIN_ENTRY <= target No <= NO_MAX_ENTRY (floor is swept).
  - per (city, market_date) state, reset each market-date (mirrors midnight reset):
      direction lock (first trigger locks 'up'), entries-made cap, entered set.
  - hour gates: no NEW cascade once local_hour >= START_HOUR_CAP (15);
    max_e = MAX_ENTRIES_LATE (1) if trigger hour >= LATE_HOUR (13) else
    MAX_ENTRIES_DEFAULT (2).  [MAX_ENTRIES_EXTENDED(3) path needs top-T confirm;
    we implement the default/late split, which covers the documented behaviour.]
  - excluded cities: Philadelphia, San Francisco.
  - NO LOOK-AHEAD: at each poll we use only that poll's prices.

CRITICAL: groups with 8 B brackets = two markets (today + tomorrow) in one poll.
We split by market_date (the date segment of the ticker) so ladders never mix.

USAGE (on the Pi, from repo root):
    python3 tools/backtest_lowt_bu.py            # validate vs real trades, then sweep
    python3 tools/backtest_lowt_bu.py --floor 0.60   # single floor, verbose entries
"""
import argparse
import json
import math
import sqlite3
from collections import defaultdict
from pathlib import Path

# ---- ported constants (must match cascade_engine.py) ----
CONV_THRESHOLD       = 0.97
NO_MAX_ENTRY         = 0.94
MAX_RANK_FROM_BOTTOM = 2
START_HOUR_CAP       = 15
LATE_HOUR            = 13
MAX_ENTRIES_DEFAULT  = 2
MAX_ENTRIES_LATE     = 1
EXCLUDED             = {"Philadelphia", "San Francisco"}
CONTRACT_TIERS = [(0.60, 0.70, 2), (0.71, 0.80, 4), (0.81, 0.90, 6)]

DB = "data/observations.db"


def contracts_for(no_price):
    for lo, hi, c in CONTRACT_TIERS:
        if lo <= no_price <= hi:
            return c
    return 2


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


def bracket_val(ticker):
    """Parse the numeric value from a B bracket ticker: KXLOWTATL-26APR07-B44.5 -> 44.5"""
    seg = ticker.split("-")[-1]
    if seg.startswith("B"):
        try:
            return float(seg[1:])
        except ValueError:
            return None
    return None


def market_date(ticker):
    parts = ticker.split("-")
    return parts[1] if len(parts) >= 2 else "?"


def load_polls(con):
    """
    Yield ((city, market_date), [ordered list of poll snapshots]).
    Each snapshot: (poll_time, local_hour, {ticker: no_price for B brackets}).
    Splits 8-B groups into per-market-date ladders.
    """
    rows = con.execute("""
        SELECT city, poll_time_utc, local_hour, ticker, no_price
        FROM observations
        WHERE (market_type='lowt' OR ticker LIKE 'KXLOWT%')
          AND ticker LIKE '%-B%'
          AND no_price IS NOT NULL
        ORDER BY city, poll_time_utc
    """).fetchall()

    # group by (city, market_date) -> poll_time -> {ticker: no_price, hour}
    grouped = defaultdict(lambda: defaultdict(dict))
    hours = {}
    for city, pt, lh, ticker, no in rows:
        md = market_date(ticker)
        key = (city, md)
        grouped[key][pt][ticker] = float(no)
        hours[(key, pt)] = lh

    for key, polls in grouped.items():
        snaps = []
        for pt in sorted(polls):
            snaps.append((pt, hours.get((key, pt), 0), polls[pt]))
        yield key, snaps


def replay(con, floor, collect_entries=False):
    """Replay all LOWT market-days at a given entry floor. Returns list of entries."""
    settled = dict(con.execute(
        "SELECT ticker, result FROM settlements WHERE result IN ('yes','no')"))
    entries = []

    for (city, md), snaps in load_polls(con):
        if city in EXCLUDED:
            continue
        # per market-date state (resets naturally per loop)
        locked = None
        made = 0
        entered = set()
        trig_h = None

        for pt, hour, ladder in snaps:
            # build sorted B ladder for THIS poll (low -> high)
            rungs = sorted(
                [(bracket_val(tk), tk, no) for tk, no in ladder.items()
                 if bracket_val(tk) is not None],
                key=lambda x: x[0])
            if len(rungs) < 2:
                continue

            # no NEW cascade once past the hour cap and not yet locked
            if locked is None and hour >= START_HOUR_CAP:
                continue
            max_e = MAX_ENTRIES_LATE if (trig_h is not None and trig_h >= LATE_HOUR) \
                else MAX_ENTRIES_DEFAULT
            if made >= max_e:
                continue

            for i, (val, tk, no) in enumerate(rungs):
                if no < CONV_THRESHOLD:        # this rung must be confirmed
                    continue
                if i >= len(rungs) - 1:         # no rung above
                    continue
                if i + 1 > MAX_RANK_FROM_BOTTOM:
                    continue
                tval, tticker, tno = rungs[i+1]
                if tticker in entered:
                    continue
                if not (floor <= tno <= NO_MAX_ENTRY):
                    continue
                # fire
                if locked is None:
                    locked = 'up'
                    trig_h = hour
                entered.add(tticker)
                made += 1
                res = settled.get(tticker)
                entries.append({
                    "city": city, "market_date": md, "ticker": tticker,
                    "entry_no": tno, "hour": hour, "contracts": contracts_for(tno),
                    "result": res,
                })
                # recompute cap after first trigger (late-trigger tightens it)
                max_e = MAX_ENTRIES_LATE if (trig_h is not None and trig_h >= LATE_HOUR) \
                    else MAX_ENTRIES_DEFAULT
                if made >= max_e:
                    break
    return entries


def summarize(entries, label):
    """Print WR / Wilson / EV / PnL over settled entries, by price band."""
    BANDS = [(0.60,0.70),(0.70,0.75),(0.75,0.80),(0.80,0.85),
             (0.85,0.90),(0.90,0.95)]
    print(f"\n=== {label} ===")
    print(f"{'band':14}{'sig':>5}{'settled':>8}{'win':>5}{'loss':>5}"
          f"{'WR':>6}{'Wil_LB':>7}{'EV/ct':>8}{'PnL':>9}")
    tot = defaultdict(float)
    for lo, hi in BANDS:
        sig = win = loss = 0
        pnl = 0.0
        for e in entries:
            if not (lo <= e["entry_no"] < hi):
                continue
            sig += 1
            if e["result"] == "no":
                win += 1
                pnl += (1 - e["entry_no"]) * e["contracts"] - fee(e["entry_no"])*e["contracts"]
            elif e["result"] == "yes":
                loss += 1
                pnl += -e["entry_no"] * e["contracts"] - fee(e["entry_no"])*e["contracts"]
        n = win + loss
        tot["sig"] += sig; tot["win"] += win; tot["loss"] += loss; tot["pnl"] += pnl
        wr = f"{win/n*100:.0f}%" if n else "—"
        lb = f"{wilson_lower(win,n)*100:.0f}%" if n else "—"
        mid = (lo+hi)/2
        ev = f"${(win/n*(1-mid)-(1-win/n)*mid-fee(mid)):+.3f}" if n else "—"
        print(f"[{lo:.2f},{hi:.2f}) {sig:>5}{n:>8}{win:>5}{loss:>5}"
              f"{wr:>6}{lb:>7}{ev:>8}{pnl:>+9.2f}")
    N = tot["win"] + tot["loss"]
    print(f"{'TOTAL':14}{int(tot['sig']):>5}{int(N):>8}{int(tot['win']):>5}"
          f"{int(tot['loss']):>5}{(tot['win']/N*100 if N else 0):>5.0f}%"
          f"{wilson_lower(int(tot['win']),int(N))*100:>6.0f}%{'':>8}{tot['pnl']:>+9.2f}")


def validate_vs_real(con):
    """Step 1: replay current gates over traded days, compare to real trades."""
    trades = json.load(open("data/trade_log.json"))
    real = {(t["ticker"]) for t in trades if t.get("entry_tier") == "cascade_lowt_bu"}
    real_days = {(t["city"], market_date(t["ticker"]))
                 for t in trades if t.get("entry_tier") == "cascade_lowt_bu"}
    # replay at the engine's real floor (0.60)
    sim = replay(con, floor=0.60)
    sim_on_traded_days = [e for e in sim if (e["city"], e["market_date"]) in real_days]
    sim_tickers = {e["ticker"] for e in sim_on_traded_days}

    print("\n=== STEP 1: replay vs real trades (validation) ===")
    print(f"real cascade_lowt_bu entries:        {len(real)}")
    print(f"sim entries on those same city-days:  {len(sim_tickers)}")
    matched = real & sim_tickers
    print(f"  matched (sim reproduced real):      {len(matched)}")
    print(f"  real but sim MISSED:                {len(real - sim_tickers)}")
    print(f"  sim but NOT real (extra/phantom):   {len(sim_tickers - real)}")
    if real - sim_tickers:
        print("  examples missed:", list(real - sim_tickers)[:5])
    if sim_tickers - real:
        print("  examples extra:", list(sim_tickers - real)[:5])
    print("  (perfect fidelity = 0 missed, 0 extra. Some drift is expected from\n"
          "   observer-vs-live price timing; large drift means investigate before\n"
          "   trusting the sweep.)")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--floor", type=float, default=None,
                    help="run a single floor verbosely instead of the sweep")
    args = ap.parse_args()
    con = sqlite3.connect(DB)

    if args.floor is not None:
        entries = replay(con, floor=args.floor)
        summarize(entries, f"floor={args.floor:.2f}")
        con.close()
        return

    # Step 1: fidelity check
    validate_vs_real(con)

    # Step 2: sweep the floor
    print("\n=== STEP 2: floor sweep (full observation history) ===")
    print("Each floor = replay all LOWT market-days entering down to that No price.")
    for floor in [0.60, 0.65, 0.70, 0.75, 0.80, 0.85]:
        entries = replay(con, floor=floor)
        n_settled = sum(1 for e in entries if e["result"] in ("no", "yes"))
        wins = sum(1 for e in entries if e["result"] == "no")
        pnl = 0.0
        for e in entries:
            if e["result"] == "no":
                pnl += (1-e["entry_no"])*e["contracts"] - fee(e["entry_no"])*e["contracts"]
            elif e["result"] == "yes":
                pnl += -e["entry_no"]*e["contracts"] - fee(e["entry_no"])*e["contracts"]
        wr = wins/n_settled*100 if n_settled else 0
        print(f"  floor={floor:.2f}: {len(entries):>4} signals  {n_settled:>4} settled  "
              f"WR={wr:>3.0f}%  Wilson_LB={wilson_lower(wins,n_settled)*100:>3.0f}%  "
              f"PnL=${pnl:>+8.2f}")

    # detailed band table at the engine's current floor
    summarize(replay(con, floor=0.60), "DETAIL @ current floor 0.60")
    con.close()


if __name__ == "__main__":
    main()
