#!/usr/bin/env python3
"""
probe_ladder.py — foundation check for the LOWT-UP replay harness.

The replay must reconstruct "what did the market look like at this poll" — i.e.
all of a city's LOWT B brackets and their No prices at a single poll_time — so it
can evaluate the cascade trigger (bracket-below confirmed No>=0.97). This probe
verifies that grouping observations by (city, poll_time_utc) yields a clean,
consistent bracket ladder, and reports anything that would break the replay.

USAGE (on the Pi, from repo root):
    python3 tools/probe_ladder.py
"""
import sqlite3
from collections import defaultdict

con = sqlite3.connect("data/observations.db")
cur = con.cursor()

# Confirm columns we rely on exist.
cols = [r[1] for r in cur.execute("PRAGMA table_info(observations)").fetchall()]
need = ["city", "poll_time_utc", "ticker", "no_price", "market_type", "local_hour"]
print("columns present:", {c: (c in cols) for c in need})
print()

# LOWT observations only.
where_lowt = "WHERE (market_type='lowt' OR ticker LIKE 'KXLOWT%')"

# How many LOWT rows, cities, distinct polls.
n = cur.execute(f"SELECT COUNT(*) FROM observations {where_lowt}").fetchone()[0]
ncity = cur.execute(f"SELECT COUNT(DISTINCT city) FROM observations {where_lowt}").fetchone()[0]
print(f"LOWT rows: {n:,}  cities: {ncity}")

# For a sample of (city, poll_time) groups, how many brackets appear?
rows = cur.execute(f"""
    SELECT city, poll_time_utc, COUNT(*) AS nbr,
           SUM(CASE WHEN ticker LIKE '%-B%' THEN 1 ELSE 0 END) AS nb,
           SUM(CASE WHEN ticker LIKE '%-T%' THEN 1 ELSE 0 END) AS nt
    FROM observations {where_lowt}
    GROUP BY city, poll_time_utc
""").fetchall()

dist = defaultdict(int)
b_dist = defaultdict(int)
for _, _, nbr, nb, nt in rows:
    dist[nbr] += 1
    b_dist[nb] += 1
print(f"\ndistinct (city, poll_time) groups: {len(rows):,}")
print("brackets-per-group distribution (total T+B):")
for k in sorted(dist):
    print(f"   {k} brackets: {dist[k]:,} groups")
print("B-brackets-per-group distribution (the ladder we sort on):")
for k in sorted(b_dist):
    print(f"   {k} B-brackets: {b_dist[k]:,} groups")

# Does each B bracket have floor/cap so we can sort the ladder low->high?
# Check whether floor/cap columns exist and are populated for LOWT B rows.
has_floor = "floor" in cols
has_cap = "cap" in cols
print(f"\nfloor column present: {has_floor}   cap column present: {has_cap}")
if not (has_floor and has_cap):
    print("  -> ladder sort will need to parse the bracket value from the ticker"
          " (e.g. B72.5 -> 72.5), since floor/cap aren't stored. Check a sample:")
    for r in cur.execute(f"SELECT ticker, no_price FROM observations {where_lowt} "
                         f"AND ticker LIKE '%-B%' LIMIT 5"):
        print("    ", r)

# Sample one city-day-poll ladder to eyeball it.
sample = cur.execute(f"""
    SELECT city, poll_time_utc FROM observations {where_lowt}
    AND ticker LIKE '%-B%'
    GROUP BY city, poll_time_utc HAVING COUNT(*) >= 4 LIMIT 1
""").fetchone()
if sample:
    city, pt = sample
    print(f"\nsample ladder — {city} @ {pt}:")
    for r in cur.execute(f"""
        SELECT ticker, no_price, local_hour FROM observations
        WHERE city=? AND poll_time_utc=? {where_lowt.replace('WHERE','AND')}
        ORDER BY ticker
    """, (city, pt)):
        print("    ", r)

con.close()
print("\nWhat we need for the replay to work:")
print("  - Most groups should have ~6 brackets (4 B + 2 T), or at least 4+ B.")
print("  - We need each B bracket's numeric value (floor/cap or parse from ticker)")
print("    to sort the ladder low->high and find 'the bracket below'.")
print("  - poll_time_utc must group a city's brackets into one market snapshot.")
