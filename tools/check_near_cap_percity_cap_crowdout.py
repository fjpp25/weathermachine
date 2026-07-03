"""
tools/check_near_cap_percity_cap_crowdout.py

Final piece of the near_cap investigation. Verified against trader.py
source (run_pipeline, ~line 2172-2229): the per-city cap that gates every
HIGH-market signal (MAX_NO_PER_CITY = 2, from hight_decision_engine.py)
is built from LIVE OPEN POSITIONS across the WHOLE account at the moment
each poll cycle starts (via sync_from_kalshi) — not from same-session
signal ranking, and not restricted to any one engine. This means a
near_cap candidate can be silently blocked even if its OWN target ticker
was never traded by anyone (the "NEW" group from
tools/check_near_cap_overlap.py) — it just needs 2 OTHER HIGH tickers,
from ANY engine, already open for that same city at that moment.

This script approximates "already open" as: any HIGH-market trade (any
entry_tier) for the same city, on the same market_date, with placed_at
strictly before the opportunity's poll_time. This is a reasonable proxy
given exits are rare/disabled for most tiers in this codebase (per your
own backtest finding: "removing exit anchors was net positive") — a
same-day HIGH position opened earlier is very likely still open later
that day. It is an approximation, not a perfect reconstruction of live
open_contracts state, and is reported as such.

For each of the 156 "NEW" (non-overlapping) opportunities, counts
distinct other HIGH tickers already open for that city before the
opportunity's poll_time. If that count >= MAX_NO_PER_CITY (2), the
opportunity would very likely have been silently blocked by the per-city
cap regardless of its own ticker never being traded — a DIFFERENT
crowd-out mechanism than the same-ticker overlap already checked.

Uses data/observations.db (read-only) and data/trade_log.json
(read-only). No writes, no trading impact.

Usage (on the Pi, from repo root):
    python3 tools/check_near_cap_percity_cap_crowdout.py
"""

import argparse
import json
import re
import sqlite3
from collections import defaultdict
from datetime import datetime
from pathlib import Path

NEAR_CAP_INTRA_MIN = 0.80
NEAR_CAP_NO_MIN    = 0.75
NEAR_CAP_NO_MAX    = 0.95
MAX_NO_PER_CITY    = 2   # hight_decision_engine.MAX_NO_PER_CITY, confirmed in source

TICKER_B_RE = re.compile(r"-B(\d+(?:\.\d+)?)$")


def parse_b_bracket(ticker: str):
    m = TICKER_B_RE.search(ticker)
    if not m:
        return None
    val = float(m.group(1))
    return (val - 0.5, val + 0.5)


def market_date_from_ticker(ticker: str):
    parts = ticker.split("-")
    return parts[1] if len(parts) >= 2 else None


def parse_utc(ts: str):
    """Parse either observations.db-style ('...Z') or trade_log-style
    ('...+00:00' with microseconds) timestamps into a naive UTC datetime."""
    ts = ts.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(ts).replace(tzinfo=None)
    except ValueError:
        return None


def find_qualifying_opportunities(db_path):
    con = sqlite3.connect(db_path)
    cur = con.cursor()
    cur.execute("""
        SELECT poll_time_utc, city, ticker, local_hour, observed_high_f, no_price
        FROM observations
        WHERE market_type = 'high' AND observed_high_f IS NOT NULL
    """)
    rows = cur.fetchall()
    con.close()

    groups = defaultdict(list)
    for poll_time_utc, city, ticker, local_hour, obs_high, no_p in rows:
        if not ticker:
            continue
        mdate = market_date_from_ticker(ticker)
        if mdate is None:
            continue
        groups[(city, mdate, poll_time_utc)].append((ticker, obs_high, no_p, local_hour))

    qualifying_opportunities = {}

    for (city, mdate, poll_time), brackets in groups.items():
        b_brackets = []
        obs_high = None
        no_price_by_ticker = {}
        for ticker, oh, no_p, lh in brackets:
            if oh is not None:
                obs_high = oh
            no_price_by_ticker[ticker] = no_p
            fc = parse_b_bracket(ticker)
            if fc is not None:
                b_brackets.append((ticker, fc[0], fc[1]))

        if obs_high is None or len(b_brackets) < 4:
            continue

        b_brackets.sort(key=lambda x: x[1])

        current_idx = None
        for idx, (ticker, floor, cap) in enumerate(b_brackets):
            if floor <= obs_high < cap:
                current_idx = idx
                break
        if current_idx is None:
            continue

        _, cfloor, ccap = b_brackets[current_idx]
        width = ccap - cfloor
        intra_pos = (obs_high - cfloor) / width if width > 0 else 0.0
        if intra_pos < NEAR_CAP_INTRA_MIN:
            continue

        for rank_offset in (2, 3):
            target_idx = current_idx + rank_offset
            if target_idx >= len(b_brackets):
                continue
            t_ticker, t_floor, t_cap = b_brackets[target_idx]
            no_p = no_price_by_ticker.get(t_ticker)
            if no_p is None:
                continue
            if NEAR_CAP_NO_MIN <= no_p <= NEAR_CAP_NO_MAX:
                opp_key = (city, mdate, t_ticker)
                if opp_key not in qualifying_opportunities:
                    qualifying_opportunities[opp_key] = poll_time

    return qualifying_opportunities


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default="data/observations.db")
    parser.add_argument("--trade-log", default="data/trade_log.json")
    args = parser.parse_args()

    qualifying_opportunities = find_qualifying_opportunities(args.db)

    trade_log_path = Path(args.trade_log)
    if not trade_log_path.exists():
        print(f"ERROR: {args.trade_log} not found.")
        return
    trades = json.loads(trade_log_path.read_text())

    tiers_by_ticker = defaultdict(set)
    for t in trades:
        tiers_by_ticker[t["ticker"]].add(t["entry_tier"])

    # Index trades by (city_code_in_ticker, market_date) for the crowd-out check.
    # We match on the ticker's own embedded city code + date rather than a
    # separate city-name field, since that's what's directly comparable
    # between observations.db opportunities and trade_log.json entries.
    def city_date_key(ticker):
        mdate = market_date_from_ticker(ticker)
        if mdate is None:
            return None
        prefix = ticker.split("-")[0]  # e.g. KXHIGHTATL, KXLOWTAUS
        return (prefix, mdate)

    trades_by_city_date = defaultdict(list)
    for t in trades:
        if "LOWT" in t["ticker"]:
            continue  # HIGH-only cap is what matters here
        key = city_date_key(t["ticker"])
        if key is None:
            continue
        pt = parse_utc(t["placed_at"])
        if pt is None:
            continue
        trades_by_city_date[key].append((t["ticker"], t["entry_tier"], pt))

    overlap = {}
    new_only = {}
    for opp_key, poll_time in qualifying_opportunities.items():
        city, mdate, target_ticker = opp_key
        if tiers_by_ticker.get(target_ticker):
            overlap[opp_key] = poll_time
        else:
            new_only[opp_key] = poll_time

    print(f"Total qualifying opportunities: {len(qualifying_opportunities)}")
    print(f"OVERLAP (already checked in check_near_cap_overlap.py): {len(overlap)}")
    print(f"NEW (checking per-city cap crowd-out for these): {len(new_only)}\n")

    would_be_capped = []
    clear = []
    unparseable = []

    for (city, mdate, target_ticker), poll_time in new_only.items():
        opp_dt = parse_utc(poll_time)
        if opp_dt is None:
            unparseable.append((city, mdate, target_ticker))
            continue

        prefix = target_ticker.split("-")[0]
        key = (prefix, mdate)
        other_tickers_before = set()
        for other_ticker, tier, placed_dt in trades_by_city_date.get(key, []):
            if other_ticker == target_ticker:
                continue  # shouldn't happen for NEW group, but be safe
            if placed_dt < opp_dt:
                other_tickers_before.add(other_ticker)

        n_other = len(other_tickers_before)
        record = (city, mdate, target_ticker, poll_time, n_other, other_tickers_before)
        if n_other >= MAX_NO_PER_CITY:
            would_be_capped.append(record)
        else:
            clear.append(record)

    print(f"Would likely have been BLOCKED by per-city cap "
          f"(>= {MAX_NO_PER_CITY} other same-city HIGH tickers already open): {len(would_be_capped)}")
    print(f"CLEAR of the per-city cap (< {MAX_NO_PER_CITY} other tickers open at that moment): {len(clear)}")
    if unparseable:
        print(f"Unparseable poll_time (excluded from either bucket): {len(unparseable)}")

    # Full distribution, not just the binary threshold check — a "0 blocked"
    # result could mean "genuinely quiet days" (mostly 0-other-ticker cases)
    # or "lots of near-misses" (mostly 1-other-ticker cases, one short of the
    # cap) — those tell very different stories about how fragile this result
    # is, and the binary check alone can't distinguish them.
    from collections import Counter as _Counter
    n_other_dist = _Counter()
    for city, mdate, ticker, poll_time, n_other, others in (would_be_capped + clear):
        n_other_dist[n_other] += 1
    print(f"\nFull distribution of 'other same-city HIGH tickers already open' count "
          f"across all {len(would_be_capped) + len(clear)} NEW opportunities:")
    for n in sorted(n_other_dist):
        count = n_other_dist[n]
        pct = 100.0 * count / (len(would_be_capped) + len(clear))
        flag = " (at/above cap)" if n >= MAX_NO_PER_CITY else ""
        print(f"  {n} other ticker(s) open:  {count:4d}  ({pct:5.1f}%){flag}")

    print(f"\nTrue genuinely-incremental estimate: {len(clear)} / {len(qualifying_opportunities)} "
          f"({100.0*len(clear)/len(qualifying_opportunities):.1f}% of all 180 opportunities)")

    if would_be_capped:
        print(f"\nSample likely-capped cases (up to 10):")
        for city, mdate, ticker, poll_time, n_other, others in would_be_capped[:10]:
            print(f"  {city} {mdate}  {ticker}  poll={poll_time}  "
                  f"{n_other} other HIGH ticker(s) already open: {sorted(others)}")


if __name__ == "__main__":
    main()
