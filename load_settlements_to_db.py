#!/usr/bin/env python3
"""
load_settlements_to_db.py — persist settlement outcomes into observations.db as
a permanent, owned record.

WHY: Kalshi delists old markets (~April brackets now return 404), so settlement
data fetched from their API is a *depreciating* asset — wait too long and it's
gone. Capturing settlements into our own DB makes them permanent: once a bracket's
outcome is in the `settlements` table, it survives regardless of Kalshi delisting.

This loads a settlements JSON (produced by fetch_settlements.py) into a
`settlements` table keyed by ticker, now including floor_strike / cap_strike /
title alongside result. Idempotent: re-running UPSERTs, so you can run it
repeatedly as new settlements are fetched, and it only ever adds/updates —
never loses a previously-captured outcome (an existing yes/no is NOT overwritten
by a later 'unsettled', guarding against a re-fetch of a since-delisted ticker).

SCHEMA MIGRATION: if `settlements` already exists from an older version of this
script (ticker, result, updated only — no geometry columns), the missing
floor_strike / cap_strike / title columns are added in place via ALTER TABLE.
Existing yes/no results are untouched; the new columns start NULL until the
next load backfills them. This is what lets tools/build_market_days.py's
`SELECT ... floor_strike, cap_strike FROM settlements` work without requiring
a manual DROP/recreate first.

USAGE (on the Pi):
    python3 load_settlements_to_db.py \
        --json settlements_full.json --db data/observations.db
"""
import argparse
import json
import sqlite3
import sys
from pathlib import Path

GEOMETRY_COLUMNS = (
    ("floor_strike", "REAL"),
    ("cap_strike", "REAL"),
    ("title", "TEXT"),
)


def _migrate_schema(con, table: str):
    """Add any geometry columns missing from an existing table, in place."""
    existing_cols = {row[1] for row in con.execute(f"PRAGMA table_info({table})")}
    for col, coltype in GEOMETRY_COLUMNS:
        if col not in existing_cols:
            con.execute(f"ALTER TABLE {table} ADD COLUMN {col} {coltype}")
            print(f"  [migrate] added missing column: {col} {coltype}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--json", default="settlements_full.json")
    ap.add_argument("--db", default="data/observations.db")
    ap.add_argument("--table", default="settlements")
    args = ap.parse_args()

    if not Path(args.json).exists():
        sys.exit(f"settlements JSON not found: {args.json}")
    if not Path(args.db).exists():
        sys.exit(f"DB not found: {args.db}")

    records = json.load(open(args.json))
    con = sqlite3.connect(args.db)
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {args.table} (
            ticker        TEXT PRIMARY KEY,
            result        TEXT,
            floor_strike  REAL,
            cap_strike    REAL,
            title         TEXT,
            updated       TEXT DEFAULT (datetime('now'))
        )
    """)
    _migrate_schema(con, args.table)

    # Count what we start with, for an honest before/after.
    before = con.execute(
        f"SELECT COUNT(*) FROM {args.table} "
        f"WHERE result IN ('yes','no')").fetchone()[0]
    before_geometry = con.execute(
        f"SELECT COUNT(*) FROM {args.table} "
        f"WHERE floor_strike IS NOT NULL OR cap_strike IS NOT NULL").fetchone()[0]

    settled_in = 0
    upserted = 0
    for rec in records:
        ticker = rec.get("ticker")
        result = str(rec.get("result", "")).lower()
        floor_strike = rec.get("floor_strike")
        cap_strike = rec.get("cap_strike")
        title = rec.get("title")
        if not ticker:
            continue
        if result in ("yes", "no"):
            settled_in += 1

        # Guard: never let a later 'unsettled' clobber an existing yes/no.
        # (A re-fetch of a since-delisted ticker would return empty; we keep
        #  the outcome we already captured.) Geometry is still backfilled in
        # this case via COALESCE — a since-delisted ticker's strikes don't
        # change, so there's no reason to withhold them just because the
        # result itself is being protected.
        existing = con.execute(
            f"SELECT result FROM {args.table} WHERE ticker = ?",
            (ticker,)).fetchone()
        if existing and existing[0] in ("yes", "no") and result not in ("yes", "no"):
            con.execute(f"""
                UPDATE {args.table} SET
                    floor_strike = COALESCE(floor_strike, ?),
                    cap_strike   = COALESCE(cap_strike, ?),
                    title        = COALESCE(title, ?),
                    updated      = datetime('now')
                WHERE ticker = ?
            """, (floor_strike, cap_strike, title, ticker))
            upserted += 1
            continue

        con.execute(
            f"""INSERT INTO {args.table}
                    (ticker, result, floor_strike, cap_strike, title, updated)
                VALUES (?, ?, ?, ?, ?, datetime('now'))
                ON CONFLICT(ticker) DO UPDATE SET
                    result       = excluded.result,
                    floor_strike = excluded.floor_strike,
                    cap_strike   = excluded.cap_strike,
                    title        = excluded.title,
                    updated      = datetime('now')""",
            (ticker, result if result in ("yes", "no") else "unsettled",
             floor_strike, cap_strike, title))
        upserted += 1

    con.commit()

    after = con.execute(
        f"SELECT COUNT(*) FROM {args.table} "
        f"WHERE result IN ('yes','no')").fetchone()[0]
    after_geometry = con.execute(
        f"SELECT COUNT(*) FROM {args.table} "
        f"WHERE floor_strike IS NOT NULL OR cap_strike IS NOT NULL").fetchone()[0]
    total = con.execute(f"SELECT COUNT(*) FROM {args.table}").fetchone()[0]
    con.close()

    print(f"Loaded {len(records)} records from {args.json}")
    print(f"  settled (yes/no) in source: {settled_in}")
    print(f"  rows upserted: {upserted}")
    print(f"  settlements table now: {total} tickers, "
          f"{after} settled (was {before}), "
          f"{after_geometry} with geometry (was {before_geometry})")
    print(f"\nSettlements are now permanent in {args.db} — safe from Kalshi "
          f"delisting. Backtest can join observations + {args.table} in one DB.")


if __name__ == "__main__":
    main()
