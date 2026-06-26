#!/usr/bin/env python3
"""
load_settlements_to_db.py — persist settlement outcomes into observations.db as
a permanent, owned record.

WHY: Kalshi delists old markets (~April brackets now return 404), so settlement
data fetched from their API is a *depreciating* asset — wait too long and it's
gone. Capturing settlements into our own DB makes them permanent: once a bracket's
outcome is in the `settlements` table, it survives regardless of Kalshi delisting.

This loads a settlements JSON (produced by fetch_settlements.py) into a
`settlements` table keyed by ticker. Idempotent: re-running UPSERTs, so you can
run it repeatedly as new settlements are fetched, and it only ever adds/updates —
never loses a previously-captured outcome (an existing yes/no is NOT overwritten
by a later 'unsettled', guarding against a re-fetch of a since-delisted ticker).

USAGE (on the Pi):
    python3 load_settlements_to_db.py \
        --json settlements_full.json --db data/observations.db
"""
import argparse
import json
import sqlite3
import sys
from pathlib import Path


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
            ticker   TEXT PRIMARY KEY,
            result   TEXT,
            updated  TEXT DEFAULT (datetime('now'))
        )
    """)

    # Count what we start with, for an honest before/after.
    before = con.execute(
        f"SELECT COUNT(*) FROM {args.table} "
        f"WHERE result IN ('yes','no')").fetchone()[0]

    settled_in = 0
    upserted = 0
    for rec in records:
        ticker = rec.get("ticker")
        result = str(rec.get("result", "")).lower()
        if not ticker:
            continue
        if result in ("yes", "no"):
            settled_in += 1
        # Guard: never let a later 'unsettled' clobber an existing yes/no.
        # (A re-fetch of a since-delisted ticker would return empty; we keep
        #  the outcome we already captured.)
        existing = con.execute(
            f"SELECT result FROM {args.table} WHERE ticker = ?",
            (ticker,)).fetchone()
        if existing and existing[0] in ("yes", "no") and result not in ("yes", "no"):
            continue
        con.execute(
            f"""INSERT INTO {args.table} (ticker, result, updated)
                VALUES (?, ?, datetime('now'))
                ON CONFLICT(ticker) DO UPDATE SET
                    result = excluded.result,
                    updated = datetime('now')""",
            (ticker, result if result in ("yes", "no") else "unsettled"))
        upserted += 1

    con.commit()

    after = con.execute(
        f"SELECT COUNT(*) FROM {args.table} "
        f"WHERE result IN ('yes','no')").fetchone()[0]
    total = con.execute(f"SELECT COUNT(*) FROM {args.table}").fetchone()[0]
    con.close()

    print(f"Loaded {len(records)} records from {args.json}")
    print(f"  settled (yes/no) in source: {settled_in}")
    print(f"  rows upserted: {upserted}")
    print(f"  settlements table now: {total} tickers, "
          f"{after} settled (was {before})")
    print(f"\nSettlements are now permanent in {args.db} — safe from Kalshi "
          f"delisting. Backtest can join observations + {args.table} in one DB.")


if __name__ == "__main__":
    main()
