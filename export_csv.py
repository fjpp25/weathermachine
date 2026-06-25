#!/usr/bin/env python3
"""
export_csv.py — regenerate or incrementally append the evaluation CSV from the
SQLite observations DB.

This is the bridge that lets the live observer stop maintaining the CSV: once the
observer writes only to the DB, THIS script keeps the CSV fresh. It must exist and
be proven before the observer is cut over, or the CSV would go stale.

WHY rowid watermark (not timestamp): rowid is the DB's guaranteed-monotonic
insertion order. Two observations can share a poll_time_utc; they can't share a
rowid. So resuming from "last exported rowid" never misses or double-writes a row.

MODES:
  (default) incremental : append rows with rowid > watermark, advance watermark.
  --full                : regenerate the whole CSV from scratch (first build, or
                          if you ever suspect the CSV drifted from the DB).
  --verify-against FILE  : compare row counts (and spot-check) against an existing
                          CSV — used to validate the DB reproduces the repair CSV.

USAGE (on the Pi):
  first build / authoritative regenerate:
    python3 export_csv.py --db data/observations.db --out data/lowt_observations.csv --full
  later refreshes (cron-friendly):
    python3 export_csv.py --db data/observations.db --out data/lowt_observations.csv
  validate against the repair CSV:
    python3 export_csv.py --db data/observations.db --out /tmp/from_db.csv --full \
        --verify-against data/lowt_observations_clean.csv
"""
import argparse
import csv
import sqlite3
import sys
from pathlib import Path

CSV_FIELDS = [
    "poll_time_utc", "city", "market_type", "local_time", "local_hour",
    "observed_high_f", "forecast_high_f",
    "observed_low_f", "forecast_low_f",
    "forecast_issued_at", "hazards",
    "ticker", "bracket", "yes_price", "no_price",
    "spread", "volume", "open_interest",
]


def watermark_path(out_csv):
    # sidecar next to the CSV, e.g. data/.lowt_observations.csv.watermark
    p = Path(out_csv)
    return p.parent / f".{p.name}.watermark"


def read_watermark(out_csv):
    wp = watermark_path(out_csv)
    if not wp.exists():
        return None
    try:
        return int(wp.read_text().strip())
    except (ValueError, OSError):
        return None


def write_watermark(out_csv, rowid):
    watermark_path(out_csv).write_text(str(rowid))


def fetch_rows(con, after_rowid=None):
    cols = ", ".join(CSV_FIELDS)
    if after_rowid is None:
        sql = f"SELECT rowid, {cols} FROM observations ORDER BY rowid"
        cur = con.execute(sql)
    else:
        sql = (f"SELECT rowid, {cols} FROM observations "
               f"WHERE rowid > ? ORDER BY rowid")
        cur = con.execute(sql, (after_rowid,))
    return cur


def export_full(con, out_csv):
    last_rowid = 0
    n = 0
    with open(out_csv, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(CSV_FIELDS)
        for row in fetch_rows(con, after_rowid=None):
            last_rowid = row[0]
            w.writerow(row[1:])
            n += 1
    if n:
        write_watermark(out_csv, last_rowid)
    print(f"Full export: wrote {n:,} rows to {out_csv} "
          f"(watermark rowid={last_rowid:,})")
    return n


def export_incremental(con, out_csv):
    wm = read_watermark(out_csv)
    if wm is None or not Path(out_csv).exists():
        sys.exit("No watermark or CSV missing — run with --full first to build "
                 "the authoritative CSV, then incremental refreshes will work.")
    last_rowid = wm
    n = 0
    with open(out_csv, "a", newline="") as f:
        w = csv.writer(f)
        for row in fetch_rows(con, after_rowid=wm):
            last_rowid = row[0]
            w.writerow(row[1:])
            n += 1
    if n:
        write_watermark(out_csv, last_rowid)
    print(f"Incremental export: appended {n:,} new rows to {out_csv} "
          f"(watermark {wm:,} -> {last_rowid:,})")
    return n


def verify_against(out_csv, other_csv):
    def count(p):
        with open(p) as f:
            return sum(1 for _ in f)
    a = count(out_csv)
    b = count(other_csv)
    print("\n=== verify ===")
    print(f"  {out_csv}: {a:,} lines")
    print(f"  {other_csv}: {b:,} lines")
    if a == b:
        print("  ROW COUNTS MATCH")
    else:
        print(f"  DIFFER by {abs(a-b):,} lines "
              f"(may be live growth between the two builds — check direction)")
    # spot-check: same header?
    with open(out_csv) as f1, open(other_csv) as f2:
        h1, h2 = f1.readline().strip(), f2.readline().strip()
    print(f"  headers {'match' if h1 == h2 else 'DIFFER'}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="data/observations.db")
    ap.add_argument("--out", default="data/lowt_observations.csv")
    ap.add_argument("--full", action="store_true",
                    help="regenerate the whole CSV (first build or drift repair)")
    ap.add_argument("--verify-against", default=None,
                    help="compare row count against another CSV after export")
    args = ap.parse_args()

    if not Path(args.db).exists():
        sys.exit(f"DB not found: {args.db}")

    con = sqlite3.connect(args.db)
    if args.full:
        export_full(con, args.out)
    else:
        export_incremental(con, args.out)
    con.close()

    if args.verify_against:
        verify_against(args.out, args.verify_against)


if __name__ == "__main__":
    main()
