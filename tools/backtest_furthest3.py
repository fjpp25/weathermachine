#!/usr/bin/env python3
"""
tools/backtest_furthest3.py — backtest: "at market open (earliest observed poll
for a given city/market_date), rank all brackets by |distance to forecast|,
enter the 3 furthest, hold to settlement."

v2 CHANGES FROM THE FIRST VERSION
----------------------------------
1. FIXED: n_missing_forecast_at_first_poll was incrementing once per SKIPPED
   POLL, not once per market-day. Now counted per (city, market_date).
2. ADDED: check_tomorrow_coverage() — queries the DB directly to confirm
   (not assume from reading the source) whether market_type LIKE '%_tomorrow'
   rows carry a forecast. If they do, this script can be pointed at them for
   a real day-ahead test; if not (expected, per lowt_observer.py's own
   comment that NWS fields are nulled for _tomorrow rows), this prints the
   coverage numbers so that's verified against your actual data, not my
   reading of the code.
3. ADDED: loss_concentration() — same diagnostic your analytics/core.py
   already uses (Cell.loss_by_day / worst_day / loss_concentration / is_scar)
   to tell whether a net-negative result is a diffuse structural problem or
   a "healed bug-day" scar concentrated in one or two dates/cities. Applied
   before any WR/PnL number in this script should be read as a verdict on
   the underlying idea.

WHAT THIS SCRIPT ACTUALLY TESTS (re-read this before trusting any of the
output)
--------------------------------------------------------------------------
market_type='high'/'lowt' rows in observations.db are the SAME-DAY market —
the ticker resolving that calendar day, which (per the repo's own comments
elsewhere) has typically already been open and trading since ~10am ET the
PRIOR afternoon by the time we see it. This script's "earliest poll" proxy
therefore does NOT capture true market-open / day-ahead entry — it captures
whichever poll first had a populated NWS forecast for that ticker, which can
be many hours into that ticker's trading life. The TRUE day-ahead ladder is
stored under market_type='high_tomorrow'/'lowt_tomorrow', and as of this
writing those rows have no forecast value attached at all (verify with
--check-coverage below; don't take this on faith).

Until the observer is patched to stamp a forecast on the _tomorrow rows and
new data accumulates, this script measures a real but DIFFERENT idea:
"enter the 3 furthest-from-forecast brackets in the same-day market, at
whichever poll first has usable NWS data." Treat all output accordingly.

DATA CAVEATS (unchanged from v1 — still apply)
------------------------------------------------
- Only the NWS forecast is available here, not AccuWeather (the live
  engine's primary source).
- No floor_strike/cap_strike in this table, only the display `bracket` code.
  Representative temperature is derived POSITIONALLY (lowest rung of a full
  6-bracket ladder = T-bottom edge = code-0.5; highest = T-top edge =
  code+0.5; the 4 middle rungs = B, code IS the midpoint). Only valid for a
  complete T-B-B-B-B-T ladder — partial ladders are excluded, not guessed at.
- Outcome is joined from the AUTHORITATIVE settlements table only, never
  derived from observed temperature (per analytics/core.py's rule).

USAGE (on the Pi, from repo root):
    python3 tools/backtest_furthest3.py --check-coverage    # run this FIRST
    python3 tools/backtest_furthest3.py
    python3 tools/backtest_furthest3.py --market lowt
    python3 tools/backtest_furthest3.py --n 2
    python3 tools/backtest_furthest3.py --gate 0.85 0.94
"""
import argparse
import math
import sqlite3
from collections import defaultdict, Counter

DB = "data/observations.db"


# ---------------------------------------------------------------------------
# shared math (kept local so this runs standalone — do not let these drift
# from analytics/core.py / tools/backtest_lowt_bu.py)
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
    Returns dict ticker -> representative temperature using positional T/B
    geometry. Returns {} if the ladder isn't a complete, well-formed
    6-bracket T-B-B-B-B-T (won't guess at partial data).
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
            out[tk] = val                # B midpoint
    return out


# ---------------------------------------------------------------------------
# coverage check — verify against real data, don't assume from source
# ---------------------------------------------------------------------------

def check_tomorrow_coverage(con):
    print("=== TOMORROW-ROW FORECAST COVERAGE (verify before trusting anything) ===")
    for mt, fcol in (("high_tomorrow", "forecast_high_f"),
                     ("lowt_tomorrow", "forecast_low_f")):
        total, non_null = con.execute(f"""
            SELECT COUNT(*), SUM(CASE WHEN {fcol} IS NOT NULL THEN 1 ELSE 0 END)
            FROM observations WHERE market_type = ?
        """, (mt,)).fetchone()
        non_null = non_null or 0
        pct = (non_null / total * 100) if total else 0.0
        print(f"  {mt:15} total_rows={total:>8}  {fcol}_non_null={non_null:>8}  ({pct:.1f}%)")
    print("  If both percentages are ~0%, the day-ahead idea cannot be backtested")
    print("  from this table yet — the observer needs a code change first, and")
    print("  historical days are unrecoverable regardless.\n")


# ---------------------------------------------------------------------------
# load earliest poll per (city, market_date) — see module docstring for what
# "earliest poll" actually means here (NOT true market open)
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
    meta = {}  # (key, poll_time) -> (local_hour, forecast)
    for city, pt, lh, ticker, code, no, fcst in rows:
        md = market_date(ticker)
        key = (city, md)
        grouped[key][pt].append((code, ticker, float(no)))
        meta[(key, pt)] = (lh, fcst)

    diagnostics = Counter()
    opens = {}
    for key, polls in grouped.items():
        found = False
        for pt in sorted(polls):        # first poll_time seen for this market-day
            lh, fcst = meta[(key, pt)]
            rungs = polls[pt]
            if fcst is None:
                continue
            if len(rungs) != 6:
                continue
            opens[key] = (pt, lh, fcst, rungs)
            found = True
            break
        if not found:
            # figure out WHY, once per market-day (not once per skipped poll)
            any_fcst = any(meta[(key, pt)][1] is not None for pt in polls)
            any_full = any(len(polls[pt]) == 6 for pt in polls)
            if not any_fcst:
                diagnostics["market_days_never_got_a_forecast"] += 1
            elif not any_full:
                diagnostics["market_days_never_had_a_complete_ladder"] += 1
            else:
                diagnostics["market_days_no_poll_with_both"] += 1
    diagnostics["n_market_days_used"] = len(opens)
    diagnostics["n_market_days_seen_total"] = len(grouped)
    return opens, diagnostics


# ---------------------------------------------------------------------------
# build entries: furthest-N-from-forecast at "open"
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
        note = "  <-- thin sample, don't trust WR/EV" if 0 < n < 40 else ""
        print(f"[{lo:.2f},{hi:.2f}) {sig:>5}{n:>8}{win:>5}{loss:>5}"
              f"{wr:>6}{lb:>7}{ev:>8}{pnl:>+9.2f}{note}")
    N = tot["win"] + tot["loss"]
    print(f"{'TOTAL':14}{int(tot['sig']):>5}{int(N):>8}{int(tot['win']):>5}"
          f"{int(tot['loss']):>5}{(tot['win']/N*100 if N else 0):>5.0f}%"
          f"{wilson_lower(int(tot['win']), int(N))*100:>6.0f}%{'':>8}{tot['pnl']:>+9.2f}")


def loss_concentration(entries, label):
    """
    Same diagnostic as analytics/core.py's Cell: is a net-negative result a
    diffuse structural problem, or a scar concentrated in one day / one city?
    Prints both axes since city concentration matters as much as date
    concentration here (DC and Atlanta already have known scars in other
    engines — worth checking whether this shows up as the same pattern
    rather than a new independent finding).
    """
    settled = [e for e in entries if e["result"] in ("no", "yes")]
    if not settled:
        print(f"\n=== SCAR CHECK: {label} — no settled entries, skipping ===")
        return

    def net_pnl(e):
        if e["result"] == "no":
            return (1 - e["entry_no"]) - fee(e["entry_no"])
        return -e["entry_no"] - fee(e["entry_no"])

    total_pnl = sum(net_pnl(e) for e in settled)
    print(f"\n=== SCAR CHECK: {label} (total PnL ${total_pnl:+.2f}) ===")
    if total_pnl >= 0:
        print("  Net positive — scar check is about explaining losses, not gains. Skipping.")
        return

    for axis_name, axis_fn in (("market_date", lambda e: e["market_date"]),
                                ("city", lambda e: e["city"])):
        loss_by_key = defaultdict(float)
        for e in settled:
            pnl = net_pnl(e)
            if pnl < 0:
                loss_by_key[axis_fn(e)] += pnl
        total_loss = sum(loss_by_key.values())
        if total_loss == 0:
            continue
        worst_key = min(loss_by_key, key=lambda k: loss_by_key[k])
        worst_val = loss_by_key[worst_key]
        conc = worst_val / total_loss
        n_loss_keys = len(loss_by_key)
        print(f"  by {axis_name:12}: worst={worst_key!r} (${worst_val:+.2f}), "
              f"{n_loss_keys} distinct loss-{axis_name}s, "
              f"concentration={conc:.0%}"
              + ("  <-- SCAR-LIKE (>=70% from one)" if conc >= 0.70 and n_loss_keys >= 3 else ""))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--market", choices=["high", "lowt"], default="high")
    ap.add_argument("--n", type=int, default=3, help="enter the N furthest brackets")
    ap.add_argument("--gate", type=float, nargs=2, default=None,
                     metavar=("MIN", "MAX"),
                     help="optional No-price gate, e.g. --gate 0.85 0.94")
    ap.add_argument("--check-coverage", action="store_true",
                     help="only run the tomorrow-row forecast coverage check, then exit")
    args = ap.parse_args()

    con = sqlite3.connect(DB)

    if args.check_coverage:
        check_tomorrow_coverage(con)
        con.close()
        return

    settled = dict(con.execute(
        "SELECT ticker, result FROM settlements WHERE result IN ('yes','no')"))

    opens, diag = load_open_snapshots(con, args.market)
    print("=== DATA QUALITY DIAGNOSTICS (per market-day, not per poll) ===")
    for k, v in diag.items():
        print(f"  {k}: {v}")

    entries, open_hours = build_entries(opens, settled, args.n, args.gate)

    print("\n=== 'earliest usable poll' local-hour distribution ===")
    print("  Read this as 'when did the same-day market first show a usable")
    print("  forecast', NOT 'when did the market open' — see module docstring.")
    hc = Counter(open_hours)
    for h in sorted(hc):
        print(f"  hour {h:>2}: {hc[h]}")

    gate_label = f" gate={args.gate}" if args.gate else " (BLIND, no price gate)"
    label = f"furthest {args.n} of ladder, market={args.market}{gate_label}"
    summarize(entries, label)
    loss_concentration(entries, label)

    print("\n=== entry No-price distribution (all picks, incl. unsettled) ===")
    prices = sorted(e["entry_no"] for e in entries)
    if prices:
        n = len(prices)
        print(f"  n={n}  min={prices[0]:.2f}  p25={prices[n//4]:.2f}  "
              f"median={prices[n//2]:.2f}  p75={prices[3*n//4]:.2f}  max={prices[-1]:.2f}")

    con.close()


if __name__ == "__main__":
    main()
