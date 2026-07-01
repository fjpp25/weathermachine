#!/usr/bin/env python3
"""
tools/backtest_furthest3.py — backtest: "at market open (earliest observed poll
for a given city/market_date), rank all brackets by |distance to forecast|,
enter the 3 furthest, hold to settlement."

WHY THIS EXISTS
----------------
This is a variant of cascade_engine's OVN_DIST signal (rank >= OVN_MIN_RANK
from forecast, entered pre-market) but with two differences that need to be
measured, not assumed:
  1. Fixed count (3) instead of a rank threshold. With HIGH markets always
     6 brackets wide (T-B-B-B-B-T), "3 furthest" = HALF the ladder, not just
     the tail — it will include at least one bracket that's only moderately
     off-forecast.
  2. NO price gate, NO forecast-confidence gate, NO adjacent-bracket check.
     OVN_DIST requires max_yes_this <= 0.15, n1_avg_yes <= 0.30,
     forecast_conf >= 0.45, No in [0.78, 0.95]. This replay enters at
     whatever No price existed on the earliest poll, unconditionally — by
     design, to test the raw idea as stated before adding any gates back in.

DATA CAVEATS (read before trusting the output)
------------------------------------------------
- observations.db only logs the NWS forecast_high_f/forecast_low_f, not
  AccuWeather (the live engine's primary source). This backtests "what if we
  ran on raw NWS", not "what the live engine would have done".
- observations.db does NOT store floor_strike/cap_strike, only the display
  `bracket` code (e.g. B82.5, T83). market_utils.bracket_temp() needs strikes
  to be authoritative and explicitly warns the code carries "display offsets"
  without them. This script derives the representative temperature
  POSITIONALLY instead of trusting the code in isolation:
      - B bracket: code value IS the interval midpoint (validated elsewhere
        in the repo against real floor/cap strikes for X.5-format codes).
      - Lowest rung in a full 6-bracket ladder: T-bottom, edge = code - 0.5
      - Highest rung in a full 6-bracket ladder: T-top,    edge = code + 0.5
  This is only valid for a COMPLETE T-B-B-B-B-T ladder. Partial ladders
  (observer downtime, mid-cutover, etc.) are excluded rather than guessed at
  — see `n_incomplete_ladder` in the diagnostics.
- "Market open" = earliest poll_time_utc for that (city, market_date). This
  is only a good proxy if the observer was already running before Kalshi
  opened next-day markets. The script prints the local-hour distribution of
  the first poll so you can sanity-check this before trusting anything else.
- Outcome is joined from the AUTHORITATIVE settlements table only, per
  analytics/core.py's rule — never derived from observed temperature.

USAGE (on the Pi, from repo root):
    python3 tools/backtest_furthest3.py                  # HIGH markets (default)
    python3 tools/backtest_furthest3.py --market lowt     # LOWT markets
    python3 tools/backtest_furthest3.py --n 2             # furthest N instead of 3
    python3 tools/backtest_furthest3.py --gate 0.85 0.94  # optional price gate,
                                                            # to compare vs blind entry
"""
import argparse
import math
import sqlite3
from collections import defaultdict, Counter

DB = "data/observations.db"


# ---------------------------------------------------------------------------
# shared math (kept local so this runs standalone, same formulas as
# analytics/core.py and tools/backtest_lowt_bu.py — do not let these drift)
# ---------------------------------------------------------------------------

def wilson_lower(wins, n, z=1.96):
    if n == 0:
        return 0.0
    p = wins / n
    denom = 1 + z * z / n
    centre = p + z * z / (2 * n)
    margin = z * math.sqrt((p * (1 - p) + z * z / (4 * n)) / n)
    return (centre - margin) / denom


def fee(price):
    return math.ceil(0.07 * price * (1 - price) * 100) / 100.0


def bracket_val(code):
    """B82.5 -> 82.5, T83 -> 83.0. None if not a valid code."""
    if code and code[0] in ("B", "T"):
        try:
            return float(code[1:])
        except ValueError:
            return None
    return None


def market_date(ticker):
    parts = ticker.split("-")
    return parts[1] if len(parts) >= 2 else "?"


def rep_temps(rungs):
    """
    rungs: list of (code, ticker, no_price) for ONE market, any order.
    Returns dict ticker -> representative temperature, using positional
    T/B geometry (see module docstring). Returns {} if the ladder isn't a
    complete, well-formed 6-bracket T-B-B-B-B-T (won't guess at partial data).
    """
    parsed = [(bracket_val(code), code, tk, no) for code, tk, no in rungs]
    if any(v is None for v, *_ in parsed):
        return {}
    parsed.sort(key=lambda x: x[0])
    if len(parsed) != 6:
        return {}
    codes_ok = parsed[0][1].startswith("T") and parsed[-1][1].startswith("T") \
        and all(parsed[i][1].startswith("B") for i in range(1, 5))
    if not codes_ok:
        return {}
    out = {}
    for i, (val, code, tk, no) in enumerate(parsed):
        if i == 0:
            out[tk] = val - 0.5          # T-bottom edge
        elif i == len(parsed) - 1:
            out[tk] = val + 0.5          # T-top edge
        else:
            out[tk] = val                # B midpoint (validated == code value)
    return out


# ---------------------------------------------------------------------------
# load earliest poll per (city, market_date)
# ---------------------------------------------------------------------------

def load_open_snapshots(con, market_type):
    fcst_col = "forecast_high_f" if market_type == "high" else "forecast_low_f"
    rows = con.execute(f"""
        SELECT city, poll_time_utc, local_hour, ticker, bracket, no_price, {fcst_col}
        FROM observations
        WHERE market_type = ?
          AND bracket IS NOT NULL
          AND no_price IS NOT NULL
        ORDER BY city, poll_time_utc
    """, (market_type,)).fetchall()

    grouped = defaultdict(lambda: defaultdict(list))
    meta = {}  # (city, md, poll_time) -> (local_hour, forecast)
    for city, pt, lh, ticker, code, no, fcst in rows:
        md = market_date(ticker)
        key = (city, md)
        grouped[key][pt].append((code, ticker, float(no)))
        meta[(key, pt)] = (lh, fcst)

    diagnostics = Counter()
    opens = {}  # (city, md) -> (poll_time, local_hour, forecast, rungs)
    for key, polls in grouped.items():
        for pt in sorted(polls):        # first poll_time seen = proxy for open
            lh, fcst = meta[(key, pt)]
            rungs = polls[pt]
            if fcst is None:
                diagnostics["n_missing_forecast_at_first_poll"] += 1
                continue
            if len(rungs) != 6:
                diagnostics["n_incomplete_ladder"] += 1
                continue
            opens[key] = (pt, lh, fcst, rungs)
            break
        else:
            diagnostics["n_no_usable_open_poll"] += 1
    diagnostics["n_market_days_used"] = len(opens)
    return opens, diagnostics


# ---------------------------------------------------------------------------
# build entries: furthest-N-from-forecast at open
# ---------------------------------------------------------------------------

def build_entries(opens, settled, n_furthest, gate):
    entries = []
    open_hours = []
    for (city, md), (pt, lh, fcst, rungs) in opens.items():
        open_hours.append(lh)
        temps = rep_temps(rungs)
        if not temps:
            continue
        no_price = {tk: no for _, tk, no in rungs}
        dist = sorted(temps.items(), key=lambda kv: abs(kv[1] - fcst), reverse=True)
        picks = dist[:n_furthest]
        for tk, temp in picks:
            no = no_price[tk]
            if gate and not (gate[0] <= no <= gate[1]):
                continue
            entries.append({
                "city": city, "market_date": md, "ticker": tk,
                "entry_no": no, "distance": abs(temp - fcst),
                "open_hour": lh, "result": settled.get(tk),
            })
    return entries, open_hours


def summarize(entries, label):
    BANDS = [(0.00, 0.50), (0.50, 0.70), (0.70, 0.80), (0.80, 0.85),
             (0.85, 0.90), (0.90, 0.95), (0.95, 1.01)]
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
                pnl += (1 - e["entry_no"]) - fee(e["entry_no"])
            elif e["result"] == "yes":
                loss += 1
                pnl += -e["entry_no"] - fee(e["entry_no"])
        n = win + loss
        tot["sig"] += sig
        tot["win"] += win
        tot["loss"] += loss
        tot["pnl"] += pnl
        wr = f"{win/n*100:.0f}%" if n else "\u2014"
        lb = f"{wilson_lower(win, n)*100:.0f}%" if n else "\u2014"
        mid = (lo + hi) / 2
        ev = f"${(win/n*(1-mid) - (1-win/n)*mid - fee(mid)):+.3f}" if n else "\u2014"
        print(f"[{lo:.2f},{hi:.2f}) {sig:>5}{n:>8}{win:>5}{loss:>5}"
              f"{wr:>6}{lb:>7}{ev:>8}{pnl:>+9.2f}")
    N = tot["win"] + tot["loss"]
    print(f"{'TOTAL':14}{int(tot['sig']):>5}{int(N):>8}{int(tot['win']):>5}"
          f"{int(tot['loss']):>5}{(tot['win']/N*100 if N else 0):>5.0f}%"
          f"{wilson_lower(int(tot['win']), int(N))*100:>6.0f}%{'':>8}{tot['pnl']:>+9.2f}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--market", choices=["high", "lowt"], default="high")
    ap.add_argument("--n", type=int, default=3, help="enter the N furthest brackets")
    ap.add_argument("--gate", type=float, nargs=2, default=None,
                     metavar=("MIN", "MAX"),
                     help="optional No-price gate, e.g. --gate 0.85 0.94")
    args = ap.parse_args()

    con = sqlite3.connect(DB)
    settled = dict(con.execute(
        "SELECT ticker, result FROM settlements WHERE result IN ('yes','no')"))

    opens, diag = load_open_snapshots(con, args.market)
    print("=== DATA QUALITY DIAGNOSTICS ===")
    for k, v in diag.items():
        print(f"  {k}: {v}")

    entries, open_hours = build_entries(opens, settled, args.n, args.gate)

    print("\n=== 'market open' local-hour distribution (sanity check) ===")
    print("  If this skews toward midday/afternoon, the earliest-poll proxy")
    print("  isn't capturing true market open and results below are suspect.")
    hc = Counter(open_hours)
    for h in sorted(hc):
        print(f"  hour {h:>2}: {hc[h]}")

    gate_label = f" gate={args.gate}" if args.gate else " (BLIND, no price gate)"
    summarize(entries, f"furthest {args.n} of ladder, market={args.market}{gate_label}")

    # entry price distribution — shows whether "furthest" already implies
    # "already cheap" or "still genuinely priced with uncertainty"
    print("\n=== entry No-price distribution (all picks, incl. unsettled) ===")
    prices = sorted(e["entry_no"] for e in entries)
    if prices:
        n = len(prices)
        print(f"  n={n}  min={prices[0]:.2f}  p25={prices[n//4]:.2f}  "
              f"median={prices[n//2]:.2f}  p75={prices[3*n//4]:.2f}  max={prices[-1]:.2f}")

    con.close()


if __name__ == "__main__":
    main()
