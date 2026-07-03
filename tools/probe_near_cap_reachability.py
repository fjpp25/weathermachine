"""
tools/probe_near_cap_reachability.py

Diagnostic (read-only) probe: is hight_decision_engine.py's near_cap
structural condition — observed_high sitting >=80% through a B bracket's
range, AND local_hour < 12 (before noon) — ever actually reached in real
market history? Tested independently of the No-price gate.

Bracket floor/cap derived directly from the ticker suffix, matching the
exact relationship confirmed in market_utils.py's own docstring:
  B82.5 -> floor=82, cap=83   (i.e. floor = val - 0.5, cap = val + 0.5)

Uses only data/observations.db. No live trading, no writes.

Usage (on the Pi, from repo root):
    python3 tools/probe_near_cap_reachability.py
    python3 tools/probe_near_cap_reachability.py --db path/to/other.db   # for testing
"""

import argparse
import re
import sqlite3
from collections import defaultdict

NEAR_CAP_HOUR_MAX  = 12     # only fires before noon local
NEAR_CAP_INTRA_MIN = 0.80   # obs must be >= this fraction through the bracket
NEAR_CAP_NO_MIN     = 0.75
NEAR_CAP_NO_MAX     = 0.95

TICKER_B_RE = re.compile(r"-B(\d+(?:\.\d+)?)$")


def parse_b_bracket(ticker: str):
    """Return (floor, cap) for a B ticker, or None if not a B bracket."""
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
    cur.execute("""
        SELECT poll_time_utc, city, ticker, local_hour, observed_high_f, no_price
        FROM observations
        WHERE market_type = 'high' AND local_hour < ? AND observed_high_f IS NOT NULL
    """, (NEAR_CAP_HOUR_MAX,))
    rows = cur.fetchall()
    con.close()
    print(f"Loaded {len(rows)} pre-noon (local_hour<{NEAR_CAP_HOUR_MAX}) HIGH observation rows "
          f"with a non-null observed_high_f.\n")

    groups = defaultdict(list)
    for poll_time_utc, city, ticker, local_hour, obs_high, no_p in rows:
        if not ticker:
            continue
        mdate = market_date_from_ticker(ticker)
        if mdate is None:
            continue
        groups[(city, mdate, poll_time_utc)].append((ticker, obs_high, no_p))

    reachable_polls = 0                      # intra_pos >= 0.80 reached, regardless of price
    qualifying_opportunities = set()         # (city, market_date, target_ticker) passing full gate
    examples = []

    for key, brackets in groups.items():
        city, mdate, poll_time = key
        # Build sorted list of B brackets with parsed (floor, cap) and obs_high (same for all rows in group)
        b_brackets = []
        obs_high = None
        no_price_by_ticker = {}
        for ticker, oh, no_p in brackets:
            if oh is not None:
                obs_high = oh
            no_price_by_ticker[ticker] = no_p
            fc = parse_b_bracket(ticker)
            if fc is not None:
                b_brackets.append((ticker, fc[0], fc[1]))

        if obs_high is None or len(b_brackets) < 4:
            continue

        b_brackets.sort(key=lambda x: x[1])  # sort by floor

        # find current bracket obs_high sits in
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

        reachable_polls += 1

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
                    qualifying_opportunities.add(opp_key)
                    if len(examples) < 15:
                        examples.append((key, t_ticker, no_p, intra_pos))

    total_groups = len(groups)
    print(f"Total pre-noon (city, date, poll) groups examined: {total_groups}")
    print(f"Groups where obs_high reached >= {NEAR_CAP_INTRA_MIN:.0%} through its current "
          f"B bracket: {reachable_polls}  ({100.0*reachable_polls/total_groups if total_groups else 0:.3f}%)")
    print(f"DISTINCT qualifying opportunities (city, market_date, target_ticker) passing "
          f"the full gate (structural + No price): {len(qualifying_opportunities)}")

    if examples:
        print("\nSample qualifying opportunities (up to 15):")
        for (city, mdate, poll_time), t, no_p, intra in examples:
            print(f"  {city} {mdate} {poll_time}  {t}  no={no_p:.2f}  intra_pos={intra:.2f}")
    else:
        print("\nNo qualifying opportunities found at all in this window — worth checking "
              "whether the >=80%-before-noon condition is close to structurally unreachable "
              "given how US daily highs typically develop (afternoon peak, not morning).")


if __name__ == "__main__":
    main()
