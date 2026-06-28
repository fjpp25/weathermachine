#!/usr/bin/env python3
"""
migrate_hourly_to_sqlite.py — load hourly NYC observation history from JSON into
a `hourly_observations` table in the SAME observations.db (single-database goal).

Mirrors migrate_observations_to_sqlite.py but for the NYC hourly schema. The
hourly data is a different shape (threshold/forecast/direction/border fields,
no hazards), so it gets its OWN table — one database, two purpose-specific
tables, joinable if ever needed.

Notes from inspecting the real data:
  - Schema drifted over time: early records lack `forecast_resolves_yes` and
    `market_direction`. Missing keys are stored as NULL (row.get handles this).
  - Booleans (is_border, *_resolves_yes) are real True/False -> stored as 0/1.
  - 317k rows / 185MB: small enough that --no-stream (plain json.load) is fine;
    streaming kept as default for consistency, ijson optional.

USAGE (on the Pi):
    python3 migrate_hourly_to_sqlite.py \
        --json data/hourly_nyc_observations.json \
        --db   data/observations.db --no-stream
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

# (column, type) in a stable order. Booleans -> INTEGER (0/1).
COLUMNS = [
    ("poll_time_utc",          "TEXT"),
    ("market_ticker",          "TEXT"),
    ("market_hour_edt",        "INTEGER"),
    ("minutes_to_close",       "INTEGER"),
    ("accuweather_current_f",  "REAL"),
    ("accuweather_forecast_f", "REAL"),
    ("market_direction",       "TEXT"),
    ("ticker",                 "TEXT"),
    ("threshold_f",            "REAL"),
    ("yes_bid",                "REAL"),
    ("yes_ask",                "REAL"),
    ("no_bid",                 "REAL"),
    ("no_ask",                 "REAL"),
    ("spread",                 "REAL"),
    ("volume",                 "REAL"),
    ("open_interest",          "REAL"),
    ("is_border",              "INTEGER"),
    ("current_resolves_yes",   "INTEGER"),
    ("forecast_resolves_yes",  "INTEGER"),
]
COL_NAMES = [c[0] for c in COLUMNS]
BOOL_COLS = {"is_border", "current_resolves_yes", "forecast_resolves_yes"}
INT_COLS = {"market_hour_edt", "minutes_to_close"}
REAL_COLS = {c for c, t in COLUMNS if t == "REAL"}
BATCH = 50_000


def to_cell(col, val):
    if val is None or val == "":
        return None
    if col in BOOL_COLS:
        if isinstance(val, bool):
            return 1 if val else 0
        s = str(val).strip().lower()
        return 1 if s in ("true", "1") else 0 if s in ("false", "0") else None
    if col in INT_COLS:
        try:
            return int(float(val))
        except (TypeError, ValueError):
            return None
    if col in REAL_COLS:
        try:
            return float(val)
        except (TypeError, ValueError):
            return None
    return str(val)


def row_tuple(row):
    return tuple(to_cell(c, row.get(c)) for c in COL_NAMES)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--json", default="data/hourly_nyc_observations.json")
    ap.add_argument("--db", default="data/observations.db")
    ap.add_argument("--table", default="hourly_observations")
    ap.add_argument("--no-stream", action="store_true")
    args = ap.parse_args()

    if not Path(args.json).exists():
        sys.exit(f"JSON not found: {args.json}")
    if ijson is None and not args.no_stream:
        sys.exit("ijson not installed; re-run with --no-stream.")

    con = sqlite3.connect(args.db)
    cur = con.cursor()
    cur.execute(f"DROP TABLE IF EXISTS {args.table}")
    cols_ddl = ", ".join(f"{n} {t}" for n, t in COLUMNS)
    cur.execute(f"CREATE TABLE {args.table} ({cols_ddl})")
    cur.execute("PRAGMA journal_mode=WAL")
    cur.execute("PRAGMA synchronous=NORMAL")

    placeholders = ", ".join(["?"] * len(COL_NAMES))
    insert_sql = (f"INSERT INTO {args.table} ({', '.join(COL_NAMES)}) "
                  f"VALUES ({placeholders})")

    n = 0
    batch = []
    print(f"Loading {args.json} -> {args.db} (table '{args.table}') ...")
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
    cur.execute(f"CREATE INDEX idx_hourly_ticker ON {args.table}(ticker)")
    cur.execute(f"CREATE INDEX idx_hourly_market ON {args.table}(market_ticker)")
    con.commit()

    print("\n=== verification ===")
    total = cur.execute(f"SELECT COUNT(*) FROM {args.table}").fetchone()[0]
    print(f"  total rows: {total:,}")
    dt = cur.execute(f"SELECT COUNT(DISTINCT ticker) FROM {args.table}").fetchone()[0]
    print(f"  distinct bracket tickers: {dt:,}")
    print("  sample recent rows:")
    for r in cur.execute(
        f"SELECT ticker, threshold_f, no_bid, forecast_resolves_yes, is_border "
        f"FROM {args.table} ORDER BY rowid DESC LIMIT 5"):
        print(f"    {r}")
    # confirm both tables now coexist in the one DB
    tbls = [r[0] for r in cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
    print(f"  tables in {args.db}: {tbls}")

    con.close()
    print(f"\nDone. Compare total rows against the JSON's 317,649.")


if __name__ == "__main__":
    main()
