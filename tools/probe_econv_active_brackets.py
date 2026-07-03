"""
tools/probe_econv_active_brackets.py

Diagnostic (read-only) probe: is evening_convergence.py's core structural
condition — exactly 3 unresolved (active) B/T brackets, at local_hour >= 19,
in a HIGH market — ever actually reached in real market history? Tested
independently of the No-price gate, so we can tell "the structural setup
never happens" apart from "the setup happens but the price gate rejects it".

Uses only data/observations.db (per-poll bracket snapshots already logged
by lowt_observer/scheduler). No live trading, no order placement, no writes.

Resolution rule mirrors market_utils.is_resolved(): a bracket is "resolved"
once either side's price >= 0.95 (evening_convergence.py's RESOLVED_THRESHOLD
constant is defined but never actually passed to is_resolved(), so 0.95 —
market_utils's own default — is what really governs live behavior; this
probe intentionally matches that live behavior, not the unused constant).

Usage (on the Pi, from repo root):
    python3 tools/probe_econv_active_brackets.py
    python3 tools/probe_econv_active_brackets.py --db path/to/other.db   # for testing
"""

import argparse
import sqlite3
from collections import defaultdict, Counter

RESOLVED_THRESHOLD = 0.95   # matches market_utils.is_resolved()'s actual default
NO_MIN_ENTRY = 0.85         # evening_convergence.py's live gate
NO_MAX_ENTRY = 0.97
MIN_LOCAL_HOUR = 19


def market_date_from_ticker(ticker: str):
    parts = ticker.split("-")
    return parts[1] if len(parts) >= 2 else None


def is_b_bracket(ticker: str) -> bool:
    parts = ticker.split("-")
    if len(parts) < 3:
        return False
    return parts[2].startswith("B")


def is_resolved(no_p, yes_p, threshold=RESOLVED_THRESHOLD) -> bool:
    return (no_p is not None and no_p >= threshold) or (yes_p is not None and yes_p >= threshold)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default="data/observations.db")
    args = parser.parse_args()

    con = sqlite3.connect(args.db)
    cur = con.cursor()
    cur.execute("""
        SELECT poll_time_utc, city, ticker, local_hour, yes_price, no_price
        FROM observations
        WHERE market_type = 'high' AND local_hour >= ?
    """, (MIN_LOCAL_HOUR,))
    rows = cur.fetchall()
    con.close()
    print(f"Loaded {len(rows)} evening (local_hour>={MIN_LOCAL_HOUR}) HIGH observation rows.\n")

    # Group into (city, market_date, poll_time_utc) — one group per scan cycle
    # per city per day, exactly what evening_convergence._check_city() sees
    # in a single call.
    groups = defaultdict(list)
    skipped_no_ticker = 0
    for poll_time_utc, city, ticker, local_hour, yes_p, no_p in rows:
        if not ticker:
            skipped_no_ticker += 1
            continue
        mdate = market_date_from_ticker(ticker)
        if mdate is None:
            skipped_no_ticker += 1
            continue
        groups[(city, mdate, poll_time_utc)].append((ticker, yes_p, no_p))

    if skipped_no_ticker:
        print(f"(skipped {skipped_no_ticker} rows with unparseable/missing ticker)\n")

    active_count_dist = Counter()
    econv_qualifying_polls = 0
    examples = []

    # This is the number that actually matters: evening_convergence.py's
    # _fired set means the live engine attempts at most ONE entry per
    # (city, ticker) per service session — not once per poll. A bracket
    # sitting in the qualifying price range for a 3-hour evening window,
    # polled every ~15 minutes, shows up as ~12 rows in the raw poll count
    # but is exactly ONE real trading opportunity. Counting raw polls
    # overstates opportunity volume by roughly (window_length / poll_interval).
    qualifying_opportunities = set()   # {(city, market_date, ticker)}
    first_qualifying_poll = {}          # (city, market_date, ticker) -> (poll_time, no_price)

    for key, brackets in groups.items():
        city, mdate, poll_time = key
        active = [(t, y, n) for (t, y, n) in brackets if not is_resolved(n, y)]
        active_count_dist[len(active)] += 1

        if len(active) != 3:
            continue

        # forecast bracket = highest Yes price among the active set
        forecast = max(active, key=lambda x: (x[1] if x[1] is not None else -1.0))
        forecast_ticker = forecast[0]

        for (t, y, n) in active:
            if t == forecast_ticker:
                continue
            if not is_b_bracket(t):
                continue
            if n is None:
                continue
            if NO_MIN_ENTRY <= n <= NO_MAX_ENTRY:
                econv_qualifying_polls += 1
                opp_key = (city, mdate, t)
                if opp_key not in qualifying_opportunities:
                    qualifying_opportunities.add(opp_key)
                    first_qualifying_poll[opp_key] = (poll_time, n)
                if len(examples) < 15:
                    examples.append((key, t, n))

    print("Distribution of active-bracket counts at evening HIGH polls:")
    for k in sorted(active_count_dist):
        print(f"  active={k:2d}:  {active_count_dist[k]:6d} (city, date, poll) groups")

    total = sum(active_count_dist.values())
    exactly_3 = active_count_dist.get(3, 0)
    pct = (100.0 * exactly_3 / total) if total else 0.0

    print(f"\nTotal (city, date, poll) groups examined: {total}")
    print(f"Groups with exactly 3 active brackets:     {exactly_3}  ({pct:.2f}%)")
    print(f"Raw qualifying POLLS (B-only, non-forecast, No in [{NO_MIN_ENTRY},{NO_MAX_ENTRY}]): "
          f"{econv_qualifying_polls}  <- inflated, one bracket counted once per poll")
    print(f"DISTINCT qualifying opportunities (city, market_date, ticker), first-touch only: "
          f"{len(qualifying_opportunities)}  <- this is the number to compare against "
          f"'0 live econv trades ever'")

    if first_qualifying_poll:
        print("\nSample distinct opportunities (up to 15, first poll each qualified):")
        for i, (opp_key, (poll_time, n)) in enumerate(sorted(first_qualifying_poll.items())):
            if i >= 15:
                break
            city, mdate, t = opp_key
            print(f"  {city} {mdate}  {t}  first_qualified_at={poll_time}  no={n:.2f}")


if __name__ == "__main__":
    main()
