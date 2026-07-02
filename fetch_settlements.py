#!/usr/bin/env python3
"""
fetch_settlements.py — pull authoritative Kalshi settlement for every bracket
that appears in an observations CSV or DB, using an existing resolutions file
as cache.

WHY: temperature-derived settlement only validated ~87.6% vs authoritative Kalshi
results (station mismatches + bracket-geometry edge cases). For any analysis whose
conclusion depends on distinguishing 99% from 97% certainty, we need the real
settled outcome, not an inference. This fetches market `result` (∈ {"yes","no",""})
for the full ticker universe. READ-ONLY: only GET /markets is ever called.

Also captures floor_strike / cap_strike / title from the same market object —
these are the REAL Kalshi strikes (see market_utils.py's _bracket_strikes,
kalshi_scanner.py), not positionally inferred. tools/build_market_days.py reads
these columns to build bracket geometry without guessing T/B edges from sort
order. A B bracket has both strikes; a T-top has only floor_strike; a T-bottom
has only cap_strike — that's expected, not missing data.

CACHE FORMAT: each cached record now carries a `has_geometry` flag. A cache
entry from an OLDER run of this script (result only, no strikes) will not have
this flag set, so it is treated as still needing a fetch — geometry gets
backfilled on the next run instead of silently staying null forever.

USAGE (on the Pi, from the repo dir):
    python3 fetch_settlements.py --db data/observations.db \
        --cache settlements_full.json --out settlements_full.json

    # or, from a CSV instead of the DB:
    python3 fetch_settlements.py lowt_observations.csv \
        --cache resolutions.json --out settlements_full.json

The cache is optional but recommended — any ticker already settled AND already
carrying geometry is skipped, so re-runs are cheap.
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
    """Returns {ticker: {"result", "floor_strike", "cap_strike", "title",
    "has_geometry"}}. Cache records written by the pre-geometry version of
    this script won't have has_geometry set — they come back with
    has_geometry=False so the caller re-fetches and backfills them."""
    if not path or not Path(path).exists():
        return {}
    data = json.load(open(path))
    out = {}
    for rec in data:
        res = str(rec.get("result", "")).lower()
        if res in ("yes", "no"):
            out[rec["ticker"]] = {
                "result": res,
                "floor_strike": rec.get("floor_strike"),
                "cap_strike": rec.get("cap_strike"),
                "title": rec.get("title"),
                "has_geometry": bool(rec.get("has_geometry", False)),
            }
    return out


def chunks(seq, size):
    for i in range(0, len(seq), size):
        yield seq[i:i + size]


def _record_from_market(m: dict) -> dict:
    return {
        "result": (m.get("result") or "").lower(),
        "floor_strike": m.get("floor_strike"),
        "cap_strike": m.get("cap_strike"),
        "title": m.get("title"),
        "has_geometry": True,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("observations_csv", nargs="?",
                    help="observations CSV (or use --db to read tickers from DB)")
    ap.add_argument("--db", default=None,
                    help="read the ticker universe from this SQLite DB instead "
                         "of a CSV (e.g. data/observations.db)")
    ap.add_argument("--cache", default="settlements_full.json",
                    help="existing settlements file to reuse (skips re-fetching "
                         "tickers that are both settled AND already have geometry)")
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
    fully_cached = {t for t, r in cache.items() if r["has_geometry"]}
    print(f"Observed tickers: {len(all_tickers)}")
    print(f"Cached settlements reused (settled + has geometry): "
          f"{len(fully_cached & all_tickers)}")

    # Re-fetch anything not cached at all, AND anything cached without
    # geometry (backfill case — see load_cache docstring).
    todo = sorted(t for t in all_tickers if t not in fully_cached)
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
                t = m.get("ticker")
                if t:
                    got[t] = _record_from_market(m)
        except Exception as e:                      # noqa: BLE001
            print(f"  batch failed ({e}); falling back to singles")
        # Singles for any ticker the batch didn't return
        for t in batch:
            if t in got:
                continue
            try:
                d = client.get(f"markets/{t}")
                m = d.get("market", d)
                got[t] = _record_from_market(m)
            except Exception as e:                  # noqa: BLE001
                failures.append((t, str(e)))
                got[t] = {"result": "", "floor_strike": None, "cap_strike": None,
                          "title": None, "has_geometry": False}

        for t, r in got.items():
            results[t] = r
        fetched += len(batch)

        if fetched % args.checkpoint_every < args.batch_size:
            _write(results, args.out)
            settled = sum(1 for v in results.values() if v["result"] in ("yes", "no"))
            print(f"  ...{fetched}/{len(todo)} fetched, "
                  f"{settled} settled so far (checkpointed)")

    _write(results, args.out)
    settled = sum(1 for v in results.values() if v["result"] in ("yes", "no"))
    unsettled = sum(1 for v in results.values() if v["result"] not in ("yes", "no"))
    with_geometry = sum(1 for v in results.values() if v["has_geometry"])
    print(f"\nDone. {len(results)} tickers, {settled} settled, "
          f"{unsettled} unsettled/empty, {with_geometry} with geometry captured.")
    if failures:
        print(f"  {len(failures)} fetch failures (left empty, has_geometry=False — "
              f"will retry next run). First few: {failures[:3]}")
    print(f"  wrote {args.out}")


def _write(results, out):
    recs = []
    for t, r in sorted(results.items()):
        recs.append({
            "ticker": t,
            "result": r["result"] if r["result"] in ("yes", "no") else "unsettled",
            "floor_strike": r.get("floor_strike"),
            "cap_strike": r.get("cap_strike"),
            "title": r.get("title"),
            "has_geometry": bool(r.get("has_geometry", False)),
        })
    with open(out, "w") as f:
        json.dump(recs, f, indent=2)


if __name__ == "__main__":
    main()
