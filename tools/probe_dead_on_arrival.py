#!/usr/bin/env python3
"""
tools/probe_dead_on_arrival.py — v2

v2 CHANGES FROM v1
--------------------
1. FIXED THE BIG ONE: v1 treated any bracket with yes_price <= threshold as
   "dead on arrival". But observations.db stores yes_price/no_price via
   `float(m.get("yes_bid_dollars") or 0)` — a bracket with NO quote yet
   (brand-new market, no market-maker has posted a bid) is stored as
   yes_price=0, no_price=0, IDENTICAL to a genuinely dead, liquid bracket.
   sweep_engine.py's own live code already guards against this (_has_price(),
   "we must not treat 'no data' as 'fully dismissed'") — that guard was
   missing here. Evidence this was happening: v1 reported 582/1160 LOWT
   market-days (50%!) with ALL 6 brackets simultaneously <= 0.05 Yes. Six
   mutually exclusive, exhaustive outcomes can't all genuinely price at
   <=5% (they'd sum to <=30%, not ~100%) — that's a no-quote artifact, not
   a signal.

   Fix: a snapshot's whole 6-bracket ladder is only trusted if
   sum(yes_price) is within LADDER_SUM_BAND of 1.0 (a real priced market),
   AND a candidate bracket is only counted as "dead" if its own
   yes_price + no_price is within RUNG_SUM_BAND of 1.0 (a real two-sided
   quote on THAT rung specifically, not just a plausible ladder-wide sum).
2. FIXED: trade-log tier lookup wasn't filtered by ticker prefix per
   --market, so a lowt run would (harmlessly, but incorrectly) check
   against every "tomorrow_dismissed" ticker regardless of market. Now
   filtered by KXHIGH*/KXLOWT* prefix matching --market.
3. ADDED: reports how many candidate snapshots got REJECTED by the new
   liquidity filters, at each threshold, so the size of the artifact from
   v1 is visible rather than silently corrected away.

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

# A real, priced 6-bracket ladder should have yes_prices summing near 1.0
# (mutually exclusive, exhaustive outcomes). Wide band because early-market
# pricing is noisy, not because we want to be lenient about no-quote rows.
LADDER_SUM_BAND = (0.70, 1.30)

# A specific rung is only trusted as a genuine two-sided quote if its own
# yes_price + no_price is close to 1.0 (real market, real spread) rather
# than both sitting at/near 0 (no quote posted yet on that rung).
RUNG_SUM_BAND = (0.85, 1.10)

TICKER_PREFIX = {"high": "KXHIGH", "lowt": "KXLOWT"}


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


def load_tomorrow_opens(con, market_type):
    """
    Returns dict (city, market_date) -> list of (ticker, code, yes_price,
    no_price) at the EARLIEST poll with a complete 6-bracket ladder, PLUS
    diagnostics on how many of those ladders look like real priced markets
    vs. no-quote artifacts.
    """
    mt = f"{market_type}_tomorrow"
    rows = con.execute("""
        SELECT city, poll_time_utc, ticker, bracket, yes_price, no_price
        FROM observations
        WHERE market_type = ?
          AND bracket IS NOT NULL
        ORDER BY city, poll_time_utc
    """, (mt,)).fetchall()

    grouped = defaultdict(lambda: defaultdict(list))
    for city, pt, ticker, code, yes_p, no_p in rows:
        md = market_date(ticker)
        grouped[(city, md)][pt].append((ticker, code, yes_p, no_p))

    opens = {}
    incomplete = 0
    ladder_sum_rejected = 0
    for key, polls in grouped.items():
        for pt in sorted(polls):
            rungs = polls[pt]
            if len(rungs) == 6:
                ladder_sum = sum((yp or 0.0) for _, _, yp, _ in rungs)
                if not (LADDER_SUM_BAND[0] <= ladder_sum <= LADDER_SUM_BAND[1]):
                    ladder_sum_rejected += 1
                    continue   # keep looking at later polls for this market-day
                opens[key] = rungs
                break
        else:
            incomplete += 1
    return opens, incomplete, len(grouped), ladder_sum_rejected


def load_trade_log_tier_counts(market_type):
    """ticker -> list of trade_log records with entry_tier in the
    dismissed/gradient family, filtered to this market's ticker prefix."""
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
    print(f"  market-days with a trusted open ladder (sum-of-Yes in "
          f"{LADDER_SUM_BAND}): {len(opens)}")
    print(f"  market-days never had a complete 6-bracket ladder: {incomplete}")
    print(f"  market-day FIRST snapshots rejected as no-quote artifacts "
          f"(kept looking at later polls): {ladder_rejected}")
    print("  ^ this last number is the size of the v1 bug — if it's large,")
    print("    v1's 'market open' proxy was frequently a pre-liquidity moment,")
    print("    not a real priced ladder.")

    by_ticker = load_trade_log_tier_counts(args.market)
    print(f"\n=== RE-ENTRY BUG CHECK (entry_tier='tomorrow_dismissed', "
          f"{args.market} tickers only) ===")
    repeats = {tk: recs for tk, recs in by_ticker.items() if len(recs) > 1}
    print(f"  distinct tickers ever entered under this tier: {len(by_ticker)}")
    print(f"  tickers entered MORE THAN ONCE:                {len(repeats)}")
    for tk, recs in list(repeats.items())[:10]:
        prices = [r.get("entry_price") for r in recs]
        print(f"    {tk}: {len(recs)}x  prices={prices}")
    if not by_ticker:
        print("  (no live entries under this tier for this market — expected "
              "for LOWT, which has no dismissed/gradient signal implemented "
              "at all yet)")

    live_tickers = set(by_ticker.keys())

    for thresh in THRESHOLDS:
        candidates = []
        rung_rejected = 0
        for (city, md), rungs in opens.items():
            for ticker, code, yes_p, no_p in rungs:
                if yes_p is None or no_p is None:
                    continue
                if yes_p > thresh:
                    continue
                # per-rung liquidity check: is this a real two-sided quote?
                if not (RUNG_SUM_BAND[0] <= (yes_p + no_p) <= RUNG_SUM_BAND[1]):
                    rung_rejected += 1
                    continue
                candidates.append({
                    "city": city, "market_date": md, "ticker": ticker,
                    "code": code, "yes_p": yes_p, "no_p": no_p,
                    "result": settled.get(ticker),
                    "caught_live": ticker in live_tickers,
                })

        n = len(candidates)
        caught = sum(1 for c in candidates if c["caught_live"])
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

        per_day = Counter()
        for (city, md), rungs in opens.items():
            cnt = sum(
                1 for _, _, yp, np in rungs
                if yp is not None and np is not None and yp <= thresh
                and RUNG_SUM_BAND[0] <= (yp + np) <= RUNG_SUM_BAND[1]
            )
            per_day[cnt] += 1

        print(f"\n=== threshold Yes <= {thresh:.2f} ===")
        print(f"  rung-level candidates REJECTED as no-quote artifacts: {rung_rejected}")
        print(f"  candidates found (after liquidity filter):  {n}   "
              f"caught live: {caught}   MISSED: {n - caught}")
        print(f"  settled: {n_settled}  WR: {wr:.0f}%  Wilson_LB: {lb:.0f}%  "
              f"PnL if we'd entered ALL of them: ${pnl:+.2f}")
        print("  per-market-day count of qualifying (liquid) brackets:")
        for cnt in sorted(per_day):
            print(f"    {cnt} bracket(s)/day: {per_day[cnt]} market-days")

    con.close()


if __name__ == "__main__":
    main()
