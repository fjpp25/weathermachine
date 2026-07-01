#!/usr/bin/env python3
"""
tools/probe_dead_on_arrival.py — v4

v4 CHANGES FROM v3
--------------------
v3 left ~15-18% of type-eligible, under-ceiling T-bracket candidates in an
"UNEXPLAINED MISS" bucket. Rather than guess why, this checks the two
remaining live gates directly against the data:
  - DISMISSED_HOUR_MAX (18): if the trusted-open snapshot's local_hour is
    >= 18, _check_dismissed would never have fired regardless of price.
  - opposing-T guard (other_yes >= YES_DISMISSED_T_OTHER=0.10): if the OTHER
    T bracket was also cheap that day, the live "don't trust a market where
    both tails look dead" guard would block this one too.
Adds local_hour to the query and, for each candidate, looks up its opposing
T bracket's yes_price from the same open snapshot to test both conditions
explicitly. Whatever's left after this is genuinely unexplained (most likely
candidate: the confirmed dedup bug burning the day's sweep budget on an
earlier entry before this one was ever reachable) — not assumed.

USAGE (on the Pi, from repo root):
    python3 tools/probe_dead_on_arrival.py
    python3 tools/probe_dead_on_arrival.py --market lowt
"""
import argparse
import json
import math
import sqlite3
from collections import defaultdict, Counter

DB = "data/observations.db"
TRADE_LOG = "data/trade_log.json"

THRESHOLDS = [0.05, 0.07, 0.10, 0.15]

LADDER_SUM_BAND = (0.70, 1.30)
RUNG_SUM_BAND = (0.85, 1.10)

TICKER_PREFIX = {"high": "KXHIGH", "lowt": "KXLOWT"}

# Mirrors sweep_engine.py constants. Kept local so this script runs
# standalone. Update here if those constants change live.
DISMISSED_NO_MAX = 0.94
DISMISSED_HOUR_MAX = 18
YES_DISMISSED_T_OTHER = 0.10


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


def market_date(ticker):
    parts = ticker.split("-")
    return parts[1] if len(parts) >= 2 else "?"


def bracket_code_type(code):
    if code and code[0] in ("T", "B"):
        return code[0]
    return None


def bracket_val(code):
    if code and code[0] in ("T", "B"):
        try:
            return float(code[1:])
        except ValueError:
            return None
    return None


def load_tomorrow_opens(con, market_type):
    """
    Returns opens: dict (city, market_date) ->
        (accepted_poll_time, local_hour, rungs, polls_skipped, first_poll_time)
    rungs: list of (ticker, code, yes_price, no_price)
    """
    mt = f"{market_type}_tomorrow"
    rows = con.execute("""
        SELECT city, poll_time_utc, local_hour, ticker, bracket, yes_price, no_price
        FROM observations
        WHERE market_type = ?
          AND bracket IS NOT NULL
        ORDER BY city, poll_time_utc
    """, (mt,)).fetchall()

    grouped = defaultdict(lambda: defaultdict(list))
    hour_of = {}
    for city, pt, lh, ticker, code, yes_p, no_p in rows:
        md = market_date(ticker)
        key = (city, md)
        grouped[key][pt].append((ticker, code, yes_p, no_p))
        hour_of[(key, pt)] = lh

    opens = {}
    incomplete = 0
    ladder_sum_rejected = 0
    for key, polls in grouped.items():
        sorted_polls = sorted(polls)
        first_pt = sorted_polls[0] if sorted_polls else None
        skipped = 0
        for pt in sorted_polls:
            rungs = polls[pt]
            if len(rungs) != 6:
                continue
            ladder_sum = sum((yp or 0.0) for _, _, yp, _ in rungs)
            if not (LADDER_SUM_BAND[0] <= ladder_sum <= LADDER_SUM_BAND[1]):
                ladder_sum_rejected += 1
                skipped += 1
                continue
            lh = hour_of.get((key, pt))
            opens[key] = (pt, lh, rungs, skipped, first_pt)
            break
        else:
            incomplete += 1
    return opens, incomplete, len(grouped), ladder_sum_rejected


def load_trade_log_tier_counts(market_type):
    try:
        raw = json.load(open(TRADE_LOG))
    except FileNotFoundError:
        return {}
    prefix = TICKER_PREFIX[market_type]
    by_ticker = defaultdict(list)
    for t in raw:
        if t.get("entry_tier") in ("tomorrow_dismissed",) \
                and str(t.get("ticker", "")).startswith(prefix):
            by_ticker[t.get("ticker", "")].append(t)
    return by_ticker


def opposing_t_yes(rungs, this_ticker):
    """Find the OTHER T bracket's yes_price in the same 6-rung ladder.
    Positional geometry: lowest bracket_val = T-bottom, highest = T-top."""
    parsed = [(bracket_val(code), ticker, code, yp, np) for ticker, code, yp, np in rungs]
    if any(v is None for v, *_ in parsed):
        return None
    parsed.sort(key=lambda x: x[0])
    if len(parsed) != 6:
        return None
    t_low, t_high = parsed[0], parsed[-1]
    if not (t_low[2].startswith("T") and t_high[2].startswith("T")):
        return None
    if this_ticker == t_low[1]:
        return t_high[3]   # yes_price of the OTHER (t_high)
    if this_ticker == t_high[1]:
        return t_low[3]
    return None   # this_ticker wasn't actually a T bracket


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--market", choices=["high", "lowt"], default="high")
    args = ap.parse_args()

    con = sqlite3.connect(DB)
    settled = dict(con.execute(
        "SELECT ticker, result FROM settlements WHERE result IN ('yes','no')"))

    opens, incomplete, total_days, ladder_rejected = load_tomorrow_opens(con, args.market)
    print(f"=== OPEN-LADDER COVERAGE ({args.market}_tomorrow) ===")
    print(f"  market-days seen (any data):        {total_days}")
    print(f"  market-days with a trusted open ladder: {len(opens)}")
    print(f"  market-days never had a complete 6-bracket ladder: {incomplete}")
    print(f"  total no-quote-artifact rejections: {ladder_rejected}")

    skip_hist = Counter(v[3] for v in opens.values())
    print("\n=== TIMING GAP ===")
    for k in sorted(skip_hist):
        print(f"    skipped {k} poll(s): {skip_hist[k]} market-days")

    by_ticker = load_trade_log_tier_counts(args.market)
    print(f"\n=== RE-ENTRY BUG CHECK (entry_tier='tomorrow_dismissed', "
          f"{args.market} tickers only) ===")
    repeats = {tk: recs for tk, recs in by_ticker.items() if len(recs) > 1}
    print(f"  distinct tickers ever entered: {len(by_ticker)}   "
          f"entered MORE THAN ONCE: {len(repeats)}")

    live_tickers = set(by_ticker.keys())

    for thresh in THRESHOLDS:
        candidates = []
        rung_rejected = 0
        for (city, md), (pt, lh, rungs, skipped, first_pt) in opens.items():
            for ticker, code, yes_p, no_p in rungs:
                if yes_p is None or no_p is None:
                    continue
                if yes_p > thresh:
                    continue
                if not (RUNG_SUM_BAND[0] <= (yes_p + no_p) <= RUNG_SUM_BAND[1]):
                    rung_rejected += 1
                    continue
                candidates.append({
                    "city": city, "market_date": md, "ticker": ticker,
                    "code": code, "yes_p": yes_p, "no_p": no_p,
                    "local_hour": lh, "rungs": rungs,
                    "result": settled.get(ticker),
                    "caught_live": ticker in live_tickers,
                })

        n = len(candidates)
        missed = [c for c in candidates if not c["caught_live"]]
        settled_c = [c for c in candidates if c["result"] in ("no", "yes")]
        wins = sum(1 for c in settled_c if c["result"] == "no")
        pnl = sum(
            (1 - c["no_p"]) - fee(c["no_p"]) if c["result"] == "no"
            else -c["no_p"] - fee(c["no_p"])
            for c in settled_c
        )
        n_settled = len(settled_c)
        wr = wins / n_settled * 100 if n_settled else 0
        lb = wilson_lower(wins, n_settled) * 100 if n_settled else 0

        reason_counts = Counter()
        for c in missed:
            btype = bracket_code_type(c["code"])
            if btype == "B":
                reason_counts["B-bracket (dismissed_t is T-only)"] += 1
                continue
            if c["no_p"] >= DISMISSED_NO_MAX:
                reason_counts["T-bracket, No >= 0.94 (ceiling)"] += 1
                continue
            if c["local_hour"] is not None and c["local_hour"] >= DISMISSED_HOUR_MAX:
                reason_counts["T-bracket, under ceiling, hour >= 18 (hour gate)"] += 1
                continue
            opp_yes = opposing_t_yes(c["rungs"], c["ticker"])
            if opp_yes is not None and opp_yes < YES_DISMISSED_T_OTHER:
                reason_counts["T-bracket, under ceiling, hour OK, "
                              "opposing T also cheap (guard)"] += 1
                continue
            reason_counts["T-bracket, under ceiling, hour OK, opposing T fine "
                          "— STILL unexplained (likely budget/dedup)"] += 1

        print(f"\n=== threshold Yes <= {thresh:.2f} ===")
        print(f"  candidates: {n}   caught live: {n - len(missed)}   missed: {len(missed)}")
        print(f"  settled: {n_settled}  WR: {wr:.0f}%  Wilson_LB: {lb:.0f}%  "
              f"PnL if we'd entered ALL: ${pnl:+.2f}")
        print("  miss reasons:")
        for reason, cnt in reason_counts.most_common():
            print(f"    {reason}: {cnt}")

    con.close()


if __name__ == "__main__":
    main()
