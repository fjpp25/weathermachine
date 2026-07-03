"""
tools/probe_near_cap_no_hour_limit.py

Follow-up to tools/probe_near_cap_reachability.py. That probe restricted
near_cap's structural condition (obs_high >=80% through a B bracket) to
local_hour < 12, matching the live NEAR_CAP_HOUR_MAX gate, and found it
almost never reachable (0.14% of pre-noon polls).

This script asks: how often would the SAME structural condition fire with
NO hour restriction at all, and what would its win rate have been?

IMPORTANT INTERPRETIVE CAVEAT (read before trusting the raw counts):
By afternoon/evening, obs_high has usually already settled near its final
value for the day, so ">=80% through a bracket" becomes close to
trivially true late in the day. Removing the hour cutoff may not surface
a "looser version of the same signal" — it may just re-derive something
close to what last_bracket.py / peak_scanner.py / cascade_ovn_dist already
cover. This script buckets results by hour-of-day specifically so that
can be checked directly rather than assumed either way.

Win rate is computed by joining each qualifying opportunity's target
ticker against market_days.winning_ticker for that (city, market_date,
market_type='high'). If the target ticker != the winning ticker, the NO
position would have won (the bracket did not resolve Yes). If it DOES
match, the NO position would have lost.

Note: market_days must actually have a row for that (city, market_date)
for an outcome to be computable — some qualifying opportunities may fall
on days without a settled market_days row yet (e.g. very recent, unsettled
markets). These are reported separately as "no settlement data", not
silently dropped from the count or silently treated as wins/losses.

Uses only data/observations.db. No live trading, no writes.

Usage (on the Pi, from repo root):
    python3 tools/probe_near_cap_no_hour_limit.py
    python3 tools/probe_near_cap_no_hour_limit.py --db path/to/other.db   # for testing
"""

import argparse
import re
import sqlite3
from collections import defaultdict, Counter

NEAR_CAP_INTRA_MIN = 0.80
NEAR_CAP_NO_MIN    = 0.75
NEAR_CAP_NO_MAX    = 0.95

TICKER_B_RE = re.compile(r"-B(\d+(?:\.\d+)?)$")

HOUR_BUCKETS = [
    (0, 6,  "00-06 (overnight)"),
    (6, 12, "06-12 (morning, matches live gate)"),
    (12, 15, "12-15 (early afternoon)"),
    (15, 18, "15-18 (mid afternoon)"),
    (18, 21, "18-21 (evening)"),
    (21, 24, "21-24 (late evening)"),
]


def bucket_for_hour(h):
    for lo, hi, label in HOUR_BUCKETS:
        if lo <= h < hi:
            return label
    return "other"


def parse_b_bracket(ticker: str):
    m = TICKER_B_RE.search(ticker)
    if not m:
        return None
    val = float(m.group(1))
    return (val - 0.5, val + 0.5)


def market_date_from_ticker(ticker: str):
    parts = ticker.split("-")
    return parts[1] if len(parts) >= 2 else None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default="data/observations.db")
    args = parser.parse_args()

    con = sqlite3.connect(args.db)
    cur = con.cursor()

    # No local_hour restriction this time — only market_type and a non-null obs_high.
    cur.execute("""
        SELECT poll_time_utc, city, ticker, local_hour, observed_high_f, no_price
        FROM observations
        WHERE market_type = 'high' AND observed_high_f IS NOT NULL
    """)
    rows = cur.fetchall()
    print(f"Loaded {len(rows)} HIGH observation rows with a non-null observed_high_f "
          f"(all hours, no restriction).\n")

    groups = defaultdict(list)
    for poll_time_utc, city, ticker, local_hour, obs_high, no_p in rows:
        if not ticker:
            continue
        mdate = market_date_from_ticker(ticker)
        if mdate is None:
            continue
        groups[(city, mdate, poll_time_utc)].append((ticker, obs_high, no_p, local_hour))

    reachable_hour_dist = Counter()
    qualifying_opportunities = {}  # (city, mdate, target_ticker) -> (poll_time, no_p, intra_pos, local_hour)

    for (city, mdate, poll_time), brackets in groups.items():
        b_brackets = []
        obs_high = None
        no_price_by_ticker = {}
        local_hour = None
        for ticker, oh, no_p, lh in brackets:
            if oh is not None:
                obs_high = oh
            if lh is not None:
                local_hour = lh
            no_price_by_ticker[ticker] = no_p
            fc = parse_b_bracket(ticker)
            if fc is not None:
                b_brackets.append((ticker, fc[0], fc[1]))

        if obs_high is None or len(b_brackets) < 4 or local_hour is None:
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

        reachable_hour_dist[bucket_for_hour(local_hour)] += 1

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
                    qualifying_opportunities[opp_key] = (poll_time, no_p, intra_pos, local_hour)

    print(f"Total (city, date, poll) groups examined: {len(groups)}")
    print(f"\nReachable-condition poll count by hour-of-day bucket "
          f"(before price gate, before dedup — raw poll count, so this shows WHERE "
          f"in the day the structural condition tends to hold):")
    total_reachable = sum(reachable_hour_dist.values())
    for lo, hi, label in HOUR_BUCKETS:
        n = reachable_hour_dist.get(label, 0)
        pct = 100.0 * n / total_reachable if total_reachable else 0
        print(f"  {label:38s}  {n:6d}  ({pct:5.1f}%)")
    print(f"  {'TOTAL':38s}  {total_reachable:6d}")

    print(f"\nDISTINCT qualifying opportunities (structural + price gate, no hour "
          f"restriction, first-touch dedup): {len(qualifying_opportunities)}")

    # Hour distribution of the DISTINCT opportunities specifically (not raw polls)
    opp_hour_dist = Counter()
    for (poll_time, no_p, intra_pos, local_hour) in qualifying_opportunities.values():
        opp_hour_dist[bucket_for_hour(local_hour)] += 1
    print("\nDistinct opportunity count by hour-of-day bucket:")
    for lo, hi, label in HOUR_BUCKETS:
        n = opp_hour_dist.get(label, 0)
        print(f"  {label:38s}  {n:6d}")

    # --- Win rate, joined against market_days.winning_ticker ---
    cur.execute("SELECT city, market_date, winning_ticker FROM market_days WHERE market_type = 'high'")
    winning_ticker_by_day = {}
    for city, mdate, winning_ticker in cur.fetchall():
        winning_ticker_by_day[(city, mdate)] = winning_ticker
    con.close()

    wins, losses, no_settlement = 0, 0, 0
    loss_examples = []
    no_settlement_examples = []

    for (city, mdate, target_ticker), (poll_time, no_p, intra_pos, local_hour) in qualifying_opportunities.items():
        winning_ticker = winning_ticker_by_day.get((city, mdate))
        if winning_ticker is None:
            no_settlement += 1
            if len(no_settlement_examples) < 10:
                no_settlement_examples.append((city, mdate, target_ticker))
            continue
        if winning_ticker == target_ticker:
            losses += 1
            if len(loss_examples) < 10:
                loss_examples.append((city, mdate, target_ticker, poll_time, no_p, intra_pos, local_hour))
        else:
            wins += 1

    decided = wins + losses
    wr = (100.0 * wins / decided) if decided else 0.0
    print(f"\n--- Win rate (NO position, joined against market_days.winning_ticker) ---")
    print(f"Wins (target bracket did NOT win):  {wins}")
    print(f"Losses (target bracket DID win):    {losses}")
    print(f"No settlement data available:       {no_settlement}")
    print(f"Win rate (of decided outcomes only): {wr:.1f}%  (n={decided})")

    if loss_examples:
        print("\nLoss examples (up to 10):")
        for city, mdate, ticker, poll_time, no_p, intra, lh in loss_examples:
            print(f"  {city} {mdate}  {ticker}  no={no_p:.2f}  intra_pos={intra:.2f}  "
                  f"local_hour={lh:.0f}h  poll={poll_time}")

    if no_settlement_examples:
        print("\nNo-settlement-data examples (up to 10) — likely very recent/unsettled days:")
        for city, mdate, ticker in no_settlement_examples:
            print(f"  {city} {mdate}  {ticker}")


if __name__ == "__main__":
    main()
