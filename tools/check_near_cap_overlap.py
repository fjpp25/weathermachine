"""
tools/check_near_cap_overlap.py

Decisive follow-up to probe_near_cap_no_hour_limit.py. That probe found
180 distinct qualifying opportunities (structural condition + price gate,
no hour restriction) with a 93.8% blended win rate.

The open question: how many of those 180 target tickers were ALREADY
being traded by some other engine (main, peak, last_bracket,
cascade_ovn_dist, etc.) that same day? If most overlap, near_cap isn't
adding incremental trades — it's rediscovering what you'd have caught
anyway. If a meaningful fraction DON'T overlap, that's the real case for
keeping/redesigning it.

Method:
  1. Reproduce the same 180 qualifying opportunities from observations.db
     (identical logic to probe_near_cap_no_hour_limit.py).
  2. For each target ticker, check data/trade_log.json for ANY trade
     (any entry_tier) on that EXACT ticker — direct string match, no date
     conversion needed since trade_log.json tickers already use the same
     raw format as observations.db.
  3. Split into OVERLAP (some other trade exists on that exact ticker) vs
     NEW (no trade exists anywhere in the log for that ticker), and report
     win rate separately for each group using the same market_days join
     as probe_near_cap_no_hour_limit.py.

Uses data/observations.db (read-only) and data/trade_log.json (read-only).
No writes, no trading impact.

Usage (on the Pi, from repo root):
    python3 tools/check_near_cap_overlap.py
"""

import argparse
import json
import re
import sqlite3
from collections import defaultdict, Counter
from pathlib import Path

NEAR_CAP_INTRA_MIN = 0.80
NEAR_CAP_NO_MIN    = 0.75
NEAR_CAP_NO_MAX    = 0.95

TICKER_B_RE = re.compile(r"-B(\d+(?:\.\d+)?)$")

_MONTH_ABBR = {
    "JAN": "01", "FEB": "02", "MAR": "03", "APR": "04", "MAY": "05", "JUN": "06",
    "JUL": "07", "AUG": "08", "SEP": "09", "OCT": "10", "NOV": "11", "DEC": "12",
}
_TICKER_DATE_RE = re.compile(r"^(\d{2})([A-Z]{3})(\d{2})$")


def ticker_date_to_iso(ticker_date: str):
    m = _TICKER_DATE_RE.match(ticker_date)
    if not m:
        return None
    yy, mon, dd = m.groups()
    month_num = _MONTH_ABBR.get(mon)
    if month_num is None:
        return None
    return f"20{yy}-{month_num}-{dd}"


def parse_b_bracket(ticker: str):
    m = TICKER_B_RE.search(ticker)
    if not m:
        return None
    val = float(m.group(1))
    return (val - 0.5, val + 0.5)


def market_date_from_ticker(ticker: str):
    parts = ticker.split("-")
    return parts[1] if len(parts) >= 2 else None


def find_qualifying_opportunities(db_path):
    con = sqlite3.connect(db_path)
    cur = con.cursor()
    cur.execute("""
        SELECT poll_time_utc, city, ticker, local_hour, observed_high_f, no_price
        FROM observations
        WHERE market_type = 'high' AND observed_high_f IS NOT NULL
    """)
    rows = cur.fetchall()

    groups = defaultdict(list)
    for poll_time_utc, city, ticker, local_hour, obs_high, no_p in rows:
        if not ticker:
            continue
        mdate = market_date_from_ticker(ticker)
        if mdate is None:
            continue
        groups[(city, mdate, poll_time_utc)].append((ticker, obs_high, no_p, local_hour))

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

    # market_days win/loss lookup, same ISO conversion fix as the win-rate probe
    cur.execute("SELECT city, market_date, winning_ticker FROM market_days WHERE market_type = 'high'")
    winning_ticker_by_day = {}
    for city, mdate, winning_ticker in cur.fetchall():
        winning_ticker_by_day[(city, mdate)] = winning_ticker
    con.close()

    return qualifying_opportunities, winning_ticker_by_day


def load_trade_log(path):
    p = Path(path)
    if not p.exists():
        return None
    return json.loads(p.read_text())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default="data/observations.db")
    parser.add_argument("--trade-log", default="data/trade_log.json")
    args = parser.parse_args()

    qualifying_opportunities, winning_ticker_by_day = find_qualifying_opportunities(args.db)
    trades = load_trade_log(args.trade_log)
    if trades is None:
        print(f"ERROR: {args.trade_log} not found.")
        return

    # Build a ticker -> set of entry_tiers that ever traded it, for ANY tier.
    tiers_by_ticker = defaultdict(set)
    for t in trades:
        tiers_by_ticker[t["ticker"]].add(t["entry_tier"])

    print(f"Total distinct qualifying opportunities: {len(qualifying_opportunities)}")

    overlap = {}   # opp_key -> set of tiers that already traded this exact ticker
    new_only = {}

    for opp_key in qualifying_opportunities:
        city, mdate, target_ticker = opp_key
        tiers = tiers_by_ticker.get(target_ticker)
        if tiers:
            overlap[opp_key] = tiers
        else:
            new_only[opp_key] = None

    print(f"\nOVERLAP (exact target ticker already traded by some other tier): {len(overlap)}"
          f"  ({100.0*len(overlap)/len(qualifying_opportunities):.1f}%)")
    print(f"NEW (target ticker never appears in trade_log.json under any tier): {len(new_only)}"
          f"  ({100.0*len(new_only)/len(qualifying_opportunities):.1f}%)")

    tier_counts = Counter()
    for tiers in overlap.values():
        for tier in tiers:
            tier_counts[tier] += 1
    print("\nOverlap breakdown by which tier already claimed the ticker "
          "(a ticker can be claimed by more than one tier, e.g. main + topup):")
    for tier, n in tier_counts.most_common():
        print(f"  {tier:20s}  {n}")

    def win_rate_for(opp_dict, label):
        wins, losses, no_settlement = 0, 0, 0
        for (city, mdate, target_ticker) in opp_dict:
            iso_date = ticker_date_to_iso(mdate)
            winning_ticker = winning_ticker_by_day.get((city, iso_date)) if iso_date else None
            if winning_ticker is None:
                no_settlement += 1
                continue
            if winning_ticker == target_ticker:
                losses += 1
            else:
                wins += 1
        decided = wins + losses
        wr = (100.0 * wins / decided) if decided else float("nan")
        print(f"\n--- Win rate: {label} ---")
        print(f"Wins: {wins}  Losses: {losses}  No settlement data: {no_settlement}")
        if decided:
            print(f"Win rate: {wr:.1f}%  (n={decided})")
        else:
            print("No decided outcomes in this group.")

    win_rate_for(overlap, "OVERLAP opportunities (already traded elsewhere)")
    win_rate_for(new_only, "NEW opportunities (not traded by any other tier)")

    print("\n--- Sample of NEW (non-overlapping) opportunities, up to 15 ---")
    for i, (city, mdate, target_ticker) in enumerate(sorted(new_only)):
        if i >= 15:
            break
        poll_time, no_p, intra_pos, local_hour = qualifying_opportunities[(city, mdate, target_ticker)]
        print(f"  {city} {mdate}  {target_ticker}  no={no_p:.2f}  local_hour={local_hour:.0f}h")


if __name__ == "__main__":
    main()
