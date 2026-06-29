#!/usr/bin/env python3
"""
export_hourly_csv.py — regenerate the portable hourly NYC CSV from the
`hourly_observations` table in observations.db.

WHY: once the hourly observer goes DB-only (DUAL_WRITE=False), the live CSV
stops being written. This exporter rebuilds/refreshes it on demand from the DB,
so the portable file stays available for offline analysis (pandas/cut on the
train). Mirrors export_csv.py (the LOWT exporter): incremental by default via a
rowid watermark, --full to rebuild from scratch.

Column order is pinned to the observer's CSV_FIELDS so existing analysis scripts
read it unchanged. NOTE: booleans export as 0/1 (SQLite has no bool), where the
old JSON-era CSV may have written True/False — adjust any script that string-
matched "True"/"False" to test == 1 instead.

USAGE (on the Pi):
    # one-time full rebuild (sets the watermark):
    python3 export_hourly_csv.py --db data/observations.db \
        --out data/hourly_nyc_observations.csv --full

    # later, incremental refresh (appends only new rows):
    python3 export_hourly_csv.py --db data/observations.db \
        --out data/hourly_nyc_observations.csv

    # verify row parity against another file:
    python3 export_hourly_csv.py --db data/observations.db \
        --out /tmp/check.csv --full --verify-against data/hourly_nyc_observations.csv
"""
import argparse
import csv
import sqlite3
import sys
from pathlib import Path

# Must match hourly_nyc_observer.CSV_FIELDS exactly (column order for the CSV).
CSV_FIELDS = [
    "poll_time_utc", "market_ticker", "market_hour_edt", "minutes_to_close",
    "accuweather_current_f", "accuweather_forecast_f", "market_direction",
    "ticker", "threshold_f", "yes_bid", "yes_ask", "no_bid", "no_ask",
    "spread", "volume", "open_interest",
    "is_border", "current_resolves_yes", "forecast_resolves_yes",
]
TABLE = "hourly_observations"


def watermark_path(out: Path) -> Path:
    return out.parent / f".{out.name}.watermark"


def read_watermark(out: Path) -> int:
    wm = watermark_path(out)
    if wm.exists():
        try:
            return int(wm.read_text().strip())
        except Exception:
            return 0
    return 0


def write_watermark(out: Path, rowid: int) -> None:
    watermark_path(out).write_text(str(rowid))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="data/observations.db")
    ap.add_argument("--out", default="data/hourly_nyc_observations.csv")
    ap.add_argument("--full", action="store_true",
                    help="rebuild the whole CSV from scratch (resets watermark)")
    ap.add_argument("--verify-against", default=None,
                    help="compare line counts against another CSV")
    args = ap.parse_args()

    if not Path(args.db).exists():
        sys.exit(f"DB not found: {args.db}")
    out = Path(args.out)
    con = sqlite3.connect(args.db)
    cols = ", ".join(CSV_FIELDS)

    if args.full:
        rows = con.execute(
            f"SELECT rowid, {cols} FROM {TABLE} ORDER BY rowid").fetchall()
        out.parent.mkdir(parents=True, exist_ok=True)
        with open(out, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(CSV_FIELDS)
            max_rowid = 0
            for r in rows:
                max_rowid = r[0]
                w.writerow(r[1:])
        if rows:
            write_watermark(out, max_rowid)
        print(f"Full export: wrote {len(rows):,} rows to {out} "
              f"(watermark rowid={max_rowid:,})")
    else:
        wm = read_watermark(out)
        rows = con.execute(
            f"SELECT rowid, {cols} FROM {TABLE} "
            f"WHERE rowid > ? ORDER BY rowid", (wm,)).fetchall()
        if not rows:
            print(f"Incremental export: no new rows (watermark rowid={wm:,})")
        else:
            header_needed = not out.exists()
            out.parent.mkdir(parents=True, exist_ok=True)
            with open(out, "a", newline="") as f:
                w = csv.writer(f)
                if header_needed:
                    w.writerow(CSV_FIELDS)
                max_rowid = wm
                for r in rows:
                    max_rowid = r[0]
                    w.writerow(r[1:])
            write_watermark(out, max_rowid)
            print(f"Incremental export: appended {len(rows):,} new rows to {out} "
                  f"(watermark {wm:,} -> {max_rowid:,})")

    if args.verify_against:
        other = Path(args.verify_against)
        if not other.exists():
            print(f"  verify: {other} not found")
        else:
            n_out = sum(1 for _ in open(out))
            n_other = sum(1 for _ in open(other))
            print(f"  verify: {out.name}={n_out:,} lines  "
                  f"{other.name}={n_other:,} lines  "
                  f"{'MATCH' if n_out == n_other else f'DIFFER by {abs(n_out-n_other):,}'}")

    con.close()


if __name__ == "__main__":
    main()
