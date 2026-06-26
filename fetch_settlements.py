#!/usr/bin/env python3
"""
fetch_settlements.py — pull authoritative Kalshi settlement for every bracket
that appears in an observations CSV, using an existing resolutions file as cache.

WHY: temperature-derived settlement only validated ~87.6% vs authoritative Kalshi
results (station mismatches + bracket-geometry edge cases). For any analysis whose
conclusion depends on distinguishing 99% from 97% certainty, we need the real
settled outcome, not an inference. This fetches market `result` (∈ {"yes","no",""})
for the full ticker universe. READ-ONLY: only GET /markets is ever called.

USAGE (on the Pi, from the repo dir):
    python3 fetch_settlements.py lowt_observations.csv \
        --cache resolutions.json --out settlements_full.json

The cache is optional but recommended — any ticker already settled in it is
skipped, so re-runs are cheap and the original 443-ticker pull isn't wasted.
"""
import argparse
import csv
import json
import os
import sys
from pathlib import Path


def load_credentials():
    """data/config.json first, then .env — mirrors scheduler.py."""
    config_file = Path("data/config.json")
    if config_file.exists():
        config = json.loads(config_file.read_text())
        if config.get("key_id"):
            os.environ.setdefault("KALSHI_KEY_ID", config["key_id"])
        if config.get("key_file"):
            os.environ.setdefault("KALSHI_KEY_FILE", config["key_file"])
        os.environ["KALSHI_DEMO"] = "false" if config.get("live_mode") else "true"
        return
    env_file = Path(".env")
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            if "=" in line and not line.startswith("#"):
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())


def tickers_from_csv(path):
    seen = set()
    with open(path, newline="") as f:
        r = csv.DictReader(f)
        if "ticker" not in r.fieldnames:
            sys.exit(f"No 'ticker' column in {path}. Columns: {r.fieldnames}")
        for row in r:
            t = (row.get("ticker") or "").strip()
            if t:
                seen.add(t)
    return seen


def tickers_from_db(path, table="observations"):
    """Pull the distinct ticker universe from the observations SQLite DB."""
    import sqlite3
    con = sqlite3.connect(path)
    rows = con.execute(f"SELECT DISTINCT ticker FROM {table} "
                       f"WHERE ticker IS NOT NULL AND ticker != ''").fetchall()
    con.close()
    return {r[0].strip() for r in rows if r[0]}


def load_cache(path):
    if not path or not Path(path).exists():
        return {}
    data = json.load(open(path))
    out = {}
    for rec in data:
        res = str(rec.get("result", "")).lower()
        if res in ("yes", "no"):          # only cache real settlements
            out[rec["ticker"]] = res
    return out


def chunks(seq, size):
    for i in range(0, len(seq), size):
        yield seq[i:i + size]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("observations_csv", nargs="?",
                    help="observations CSV (or use --db to read tickers from DB)")
    ap.add_argument("--db", default=None,
                    help="read the ticker universe from this SQLite DB instead "
                         "of a CSV (e.g. data/observations.db)")
    ap.add_argument("--cache", default="settlements_full.json",
                    help="existing settlements file to reuse (skips re-fetching)")
    ap.add_argument("--out", default="settlements_full.json")
    ap.add_argument("--batch-size", type=int, default=100)
    ap.add_argument("--checkpoint-every", type=int, default=2000,
                    help="write partial output every N newly-fetched tickers")
    args = ap.parse_args()

    if args.db:
        all_tickers = tickers_from_db(args.db)
    elif args.observations_csv:
        all_tickers = tickers_from_csv(args.observations_csv)
    else:
        sys.exit("Provide an observations CSV path, or --db data/observations.db")
    cache = load_cache(args.cache)
    print(f"Observed tickers: {len(all_tickers)}")
    print(f"Cached settlements reused: "
          f"{len(set(cache) & all_tickers)}")

    todo = sorted(all_tickers - set(cache))
    print(f"To fetch: {len(todo)}")

    load_credentials()
    try:
        import trader
    except ImportError as e:
        sys.exit(f"Could not import trader.py ({e}). Run from the repo dir.")
    client = trader.make_client(skip_confirmation=True)
    print(f"Client base: {client.base_url}")

    results = dict(cache)            # start from cache
    fetched = 0
    failures = []

    for batch in chunks(todo, args.batch_size):
        got = {}
        try:
            data = client.get("markets", params={"tickers": ",".join(batch)})
            for m in data.get("markets", []):
                got[m.get("ticker")] = (m.get("result") or "").lower()
        except Exception as e:                      # noqa: BLE001
            print(f"  batch failed ({e}); falling back to singles")
        # Singles for any ticker the batch didn't return
        for t in batch:
            if t in got:
                continue
            try:
                d = client.get(f"markets/{t}")
                m = d.get("market", d)
                got[t] = (m.get("result") or "").lower()
            except Exception as e:                  # noqa: BLE001
                failures.append((t, str(e)))
                got[t] = ""

        for t, r in got.items():
            results[t] = r
        fetched += len(batch)

        if fetched % args.checkpoint_every < args.batch_size:
            _write(results, args.out)
            settled = sum(1 for v in results.values() if v in ("yes", "no"))
            print(f"  ...{fetched}/{len(todo)} fetched, "
                  f"{settled} settled so far (checkpointed)")

    _write(results, args.out)
    settled = sum(1 for v in results.values() if v in ("yes", "no"))
    unsettled = sum(1 for v in results.values() if v not in ("yes", "no"))
    print(f"\nDone. {len(results)} tickers, {settled} settled, "
          f"{unsettled} unsettled/empty.")
    if failures:
        print(f"  {len(failures)} fetch failures (left empty). "
              f"First few: {failures[:3]}")
    print(f"  wrote {args.out}")


def _write(results, out):
    recs = [{"ticker": t, "result": (r if r in ("yes", "no") else "unsettled")}
            for t, r in sorted(results.items())]
    with open(out, "w") as f:
        json.dump(recs, f, indent=2)


if __name__ == "__main__":
    main()
