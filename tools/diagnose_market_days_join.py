"""
tools/diagnose_market_days_join.py

Follow-up to the near_cap win-rate probe, which found ZERO settlement
matches for all 180 qualifying opportunities against market_days.
Before trusting either "market_days doesn't cover that far back" or
"there's a city-name mismatch" as the explanation, check both directly.

Read-only against data/observations.db. No writes.

Usage (on the Pi, from repo root):
    python3 tools/diagnose_market_days_join.py
"""

import sqlite3

DB = "data/observations.db"


def main():
    con = sqlite3.connect(DB)
    cur = con.cursor()

    print("=== 1. market_days date coverage ===")
    cur.execute("SELECT MIN(market_date), MAX(market_date), COUNT(*) FROM market_days WHERE market_type='high'")
    min_d, max_d, n = cur.fetchone()
    print(f"market_days (market_type='high'): {n} rows, date range {min_d} to {max_d}")

    cur.execute("""
        SELECT MIN(SUBSTR(ticker, INSTR(ticker, '-')+1, INSTR(SUBSTR(ticker, INSTR(ticker,'-')+1), '-')-1)),
               MAX(SUBSTR(ticker, INSTR(ticker, '-')+1, INSTR(SUBSTR(ticker, INSTR(ticker,'-')+1), '-')-1))
        FROM observations WHERE market_type='high'
    """)
    obs_min, obs_max = cur.fetchone()
    print(f"observations ticker date range (rough, string min/max): {obs_min} to {obs_max}")
    print("(NOTE: these are string min/max on the raw ticker date segment, e.g. '26APR07' — not")
    print(" chronologically sorted, just a sanity check that data exists across a wide range.)")

    print("\n=== 2. City name comparison: observations.city vs market_days.city ===")
    cur.execute("SELECT DISTINCT city FROM observations WHERE market_type='high' ORDER BY city")
    obs_cities = set(r[0] for r in cur.fetchall())
    cur.execute("SELECT DISTINCT city FROM market_days WHERE market_type='high' ORDER BY city")
    md_cities = set(r[0] for r in cur.fetchall())

    print(f"Distinct cities in observations: {len(obs_cities)}")
    print(f"Distinct cities in market_days:   {len(md_cities)}")

    only_in_obs = obs_cities - md_cities
    only_in_md = md_cities - obs_cities
    if only_in_obs:
        print(f"\nCities in observations but NOT in market_days ({len(only_in_obs)}):")
        for c in sorted(only_in_obs):
            print(f"  '{c}'")
    if only_in_md:
        print(f"\nCities in market_days but NOT in observations ({len(only_in_md)}):")
        for c in sorted(only_in_md):
            print(f"  '{c}'")
    if not only_in_obs and not only_in_md:
        print("\nCity names match exactly between both tables — not a naming mismatch.")

    print("\n=== 3. Direct spot-check: one of the 180 unmatched opportunities ===")
    test_city, test_date = "Los Angeles", "26APR07"
    cur.execute("SELECT * FROM market_days WHERE city=? AND market_date=? AND market_type='high'",
                (test_city, test_date))
    row = cur.fetchone()
    if row is None:
        print(f"No market_days row at all for city='{test_city}' market_date='{test_date}'.")
        # Try a fuzzy check: does ANY row exist for this city, any date, to confirm the
        # city name itself is right and it's purely a missing-date issue?
        cur.execute("SELECT COUNT(*), MIN(market_date), MAX(market_date) FROM market_days WHERE city=? AND market_type='high'",
                    (test_city,))
        cnt, mn, mx = cur.fetchone()
        print(f"market_days rows for city='{test_city}' at ANY date: {cnt} "
              f"(range {mn} to {mx})" if cnt else f"ZERO market_days rows for city='{test_city}' at any date at all.")
    else:
        cols = [d[0] for d in cur.description]
        print(f"FOUND a market_days row (so the earlier probe's join logic itself may have a bug, "
              f"not a data-coverage gap):")
        print(dict(zip(cols, row)))

    con.close()


if __name__ == "__main__":
    main()
