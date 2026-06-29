#!/usr/bin/env python3
"""
migrate_observations_to_sqlite.py — one-time backfill of the observation history
from the intact JSON into a SQLite database.

This is the safe FIRST step of the storage migration: it creates a brand-new
file (data/observations.db) and touches nothing the live system reads. Run it
while everything else keeps running.

DESIGN (keeps it light on the Pi):
  - STREAMS the 1.1GB JSON with ijson (constant ~hundreds of MB RAM, not 2-3GB).
  - Inserts in ONE big transaction, committed every BATCH rows, so SQLite isn't
    fsync-ing to the SSD after every single row (that would be slow and wear the
    disk). 2M rows -> ~1-2 minutes, ~400MB written once.
  - Builds indexes AFTER all rows are in, not during — building an index once on
    a full table is far cheaper than updating it 2M times.
  - DROP-AND-REBUILD: re-running gives a clean import from the JSON source of
    truth, no duplicates. The DB is a faithful copy of the JSON; the live writer
    will append going forward (separate change).

hazards is stored pipe-joined (BH.S|CF.Y) — carrying forward the corruption fix,
so it can never break a CSV export with an embedded comma again.

USAGE (on the Pi):
    python3 migrate_observations_to_sqlite.py \
        --json data/lowt_observations.json \
        --db   data/observations.db
"""
import argparse
import json
import sqlite3
import sys
from pathlib import Path

try:
    import ijson
except ImportError:
    ijson = None

# Columns, in CSV order, with SQLite types.
# TEXT for ids/labels/timestamps (ISO strings sort correctly as text).
# REAL for anything numeric we might compute on.
COLUMNS = [
    ("poll_time_utc",      "TEXT"),
    ("city",               "TEXT"),
    ("market_type",        "TEXT"),
    ("local_time",         "TEXT"),
    ("local_hour",         "REAL"),
    ("observed_high_f",    "REAL"),
    ("forecast_high_f",    "REAL"),
    ("observed_low_f",     "REAL"),
    ("forecast_low_f",     "REAL"),
    ("forecast_issued_at", "TEXT"),
    ("hazards",            "TEXT"),
    ("ticker",             "TEXT"),
    ("bracket",            "TEXT"),
    ("yes_price",          "REAL"),
    ("no_price",           "REAL"),
    ("spread",             "REAL"),
    ("volume",             "REAL"),
    ("open_interest",      "REAL"),
    ("current_temp_f",     "REAL"),
]
COL_NAMES = [c[0] for c in COLUMNS]
BATCH = 50_000


def hazards_to_str(h):
    """List -> 'A|B|C' (comma-free). Handles list, None, '', or stringified list."""
    if h is None or h == "":
        return ""
    if isinstance(h, (list, tuple)):
        return "|".join(str(x) for x in h)
    s = str(h).strip()
    if s.startswith("[") and s.endswith("]"):
        try:
            parsed = json.loads(s.replace("'", '"'))
            return "|".join(str(x) for x in parsed)
        except Exception:
            return s.strip("[]").replace("'", "").replace(", ", "|")
    return s


def to_real(v):
    if v in (None, "", "None"):
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def row_tuple(row):
    out = []
    for name, typ in COLUMNS:
        if name == "hazards":
            out.append(hazards_to_str(row.get("hazards")))
        elif typ == "REAL":
            out.append(to_real(row.get(name)))
        else:
            v = row.get(name)
            out.append(None if v is None else str(v))
    return tuple(out)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--json", default="data/lowt_observations.json")
    ap.add_argument("--db", default="data/observations.db")
    ap.add_argument("--table", default="observations")
    ap.add_argument("--no-stream", action="store_true",
                    help="load whole JSON at once (needs ~3GB RAM) instead of "
                         "streaming with ijson; use if ijson isn't installed")
    args = ap.parse_args()

    if not Path(args.json).exists():
        sys.exit(f"JSON not found: {args.json}")

    if ijson is None and not args.no_stream:
        sys.exit("ijson not installed. Either `pip3 install ijson`, or re-run "
                 "with --no-stream to load the file with plain json (heavier RAM, "
                 "but you confirmed the Pi handles the full load).")

    con = sqlite3.connect(args.db)
    cur = con.cursor()

    # Drop-and-rebuild for a clean, idempotent import.
    cur.execute(f"DROP TABLE IF EXISTS {args.table}")
    cols_ddl = ", ".join(f"{n} {t}" for n, t in COLUMNS)
    cur.execute(f"CREATE TABLE {args.table} ({cols_ddl})")

    # Speed pragmas for a bulk load. WAL + NORMAL is safe and fast; we commit in
    # batches so a crash mid-load just means re-run (the JSON is untouched).
    cur.execute("PRAGMA journal_mode=WAL")
    cur.execute("PRAGMA synchronous=NORMAL")

    placeholders = ", ".join(["?"] * len(COL_NAMES))
    insert_sql = (f"INSERT INTO {args.table} "
                  f"({', '.join(COL_NAMES)}) VALUES ({placeholders})")

    n = 0
    batch = []
    print(f"Streaming {args.json} -> {args.db} (table '{args.table}') ...")
    if args.no_stream:
        rows_iter = iter(json.load(open(args.json)))
    else:
        rows_iter = ijson.items(open(args.json, "rb"), "item")
    for row in rows_iter:
        batch.append(row_tuple(row))
        if len(batch) >= BATCH:
            cur.executemany(insert_sql, batch)
            con.commit()
            n += len(batch)
            batch.clear()
            print(f"  ...{n:,} rows")
    if batch:
        cur.executemany(insert_sql, batch)
        con.commit()
        n += len(batch)

    print(f"Inserted {n:,} rows. Building indexes ...")
    # Indexes match real reader access patterns:
    #   dashboard.py filters by city
    #   enrich_trade_log.py needs last row per ticker -> MAX(rowid) per ticker
    cur.execute(f"CREATE INDEX idx_city ON {args.table}(city)")
    cur.execute(f"CREATE INDEX idx_ticker ON {args.table}(ticker)")
    con.commit()

    # ---- verification ----
    print("\n=== verification ===")
    cur.execute(f"SELECT COUNT(*) FROM {args.table}")
    total = cur.fetchone()[0]
    print(f"  total rows in DB: {total:,}")

    cur.execute(f"SELECT COUNT(DISTINCT ticker) FROM {args.table}")
    print(f"  distinct tickers: {cur.fetchone()[0]:,}")

    cur.execute(f"SELECT COUNT(DISTINCT city) FROM {args.table}")
    print(f"  distinct cities:  {cur.fetchone()[0]}")

    # last no_price per ticker for a few tickers (the enrich pattern)
    print("  sample last-no_price-per-ticker (MAX(rowid) per ticker):")
    cur.execute(f"""
        SELECT o.ticker, o.no_price
        FROM {args.table} o
        JOIN (SELECT ticker, MAX(rowid) AS mr FROM {args.table} GROUP BY ticker) m
          ON o.rowid = m.mr
        LIMIT 5
    """)
    for tk, np_ in cur.fetchall():
        print(f"    {tk:30} final_no={np_}")

    # check hazards are clean (no stray commas/brackets)
    cur.execute(f"SELECT DISTINCT hazards FROM {args.table} "
                f"WHERE hazards != '' LIMIT 8")
    print("  sample non-empty hazards (should be pipe-joined, no commas):")
    for (h,) in cur.fetchall():
        print(f"    {h!r}")

    con.close()
    print(f"\nDone. Compare 'total rows in DB' above against the JSON's row count "
          f"(expected 2,088,648). If they match, the DB is a faithful copy.")


if __name__ == "__main__":
    main()
