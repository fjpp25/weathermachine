#!/usr/bin/env python3
"""
repair_observations_csv.py — regenerate a clean observations CSV from the
intact JSON source of truth.

WHY: the live writer serialized the `hazards` field as a raw Python list
(e.g. ['BH.S', 'CF.Y']). The embedded comma corrupted the CSV on multi-hazard
rows, shifting every later column right and scattering hazard fragments into the
ticker/price columns. The JSON store is unaffected (lists are native there), so
the reliable repair is to regenerate the CSV FROM the JSON rather than try to
un-scramble the corrupted CSV.

hazards is written here as a pipe-joined string (BH.S|CF.Y): comma-free (so it
can never break the CSV again), human-readable, and greppable. Empty -> "".

READ-ONLY on the JSON; writes a NEW csv (does not overwrite in place unless you
point --out at the live file, which you should only do with a backup in hand).

USAGE (on the Pi):
    python3 repair_observations_csv.py \
        --json data/lowt_observations.json \
        --out  data/lowt_observations_clean.csv

Then eyeball the clean file, diff row counts, and only then replace the live CSV.
"""
import argparse
import csv
import ijson  # streaming JSON parser — avoids loading 1.1GB into RAM
import json
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


def hazards_to_str(h):
    """List -> 'A|B|C'. Comma-free, greppable. Handles already-string input."""
    if h is None or h == "":
        return ""
    if isinstance(h, str):
        # Could be a stringified list from old data e.g. "['BH.S', 'CF.Y']"
        s = h.strip()
        if s.startswith("[") and s.endswith("]"):
            try:
                parsed = json.loads(s.replace("'", '"'))
                return "|".join(str(x) for x in parsed)
            except Exception:
                return s.strip("[]").replace("'", "").replace(", ", "|")
        return s
    if isinstance(h, (list, tuple)):
        return "|".join(str(x) for x in h)
    return str(h)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--json", default="data/lowt_observations.json")
    ap.add_argument("--out", default="data/lowt_observations_clean.csv")
    ap.add_argument("--no-stream", action="store_true",
                    help="load whole JSON at once instead of streaming "
                         "(simpler, but needs ~3GB RAM for the 1.1GB file)")
    args = ap.parse_args()

    if not Path(args.json).exists():
        sys.exit(f"JSON not found: {args.json}")

    n = 0
    multi_hazard = 0
    with open(args.out, "w", newline="") as out_f:
        writer = csv.DictWriter(out_f, fieldnames=CSV_FIELDS,
                                extrasaction="ignore")
        writer.writeheader()

        if args.no_stream:
            rows = json.load(open(args.json))
            iterator = iter(rows)
        else:
            # ijson streams the top-level array item by item
            iterator = ijson.items(open(args.json, "rb"), "item")

        for row in iterator:
            hz = row.get("hazards")
            hs = hazards_to_str(hz)
            if "|" in hs:
                multi_hazard += 1
            clean = {k: row.get(k) for k in CSV_FIELDS}
            clean["hazards"] = hs
            writer.writerow(clean)
            n += 1
            if n % 200000 == 0:
                print(f"  ...{n} rows written")

    print(f"\nDone. Wrote {n} rows to {args.out}")
    print(f"  rows with multi-hazard (would have corrupted old CSV): {multi_hazard}")
    print(f"  hazards serialized as pipe-joined (comma-free).")
    print(f"\nNext: sanity-check before replacing the live file:")
    print(f"  wc -l {args.out}                 # expect {n + 1} (incl header)")
    print(f"  cut -d',' -f12 {args.out} | sort -u | head   # should be clean KX... tickers only")


if __name__ == "__main__":
    main()
