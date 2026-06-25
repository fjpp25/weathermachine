#!/usr/bin/env python3
"""
city_performance.py

Joins the entry-only trade_log.json against a Kalshi resolution export and
reports per-city performance (win rate + realized PnL) with sample-size gating
and Wilson confidence intervals.

WHY THIS EXISTS
---------------
trade_log.json records only ENTRIES (ticker, city, side, entry_price, tier).
It has no outcome field. Win/loss must come from Kalshi's authoritative `result`
field. This script joins the two on `ticker` and derives money from
(entry_price, outcome) rather than trusting any PnL field in the export.

USAGE
-----
    python3 city_performance.py trade_log.json resolutions.json
    python3 city_performance.py trade_log.json resolutions.json --min-n 25
    python3 city_performance.py trade_log.json resolutions.json --by-tier

RESOLUTION EXPORT FORMAT
------------------------
A JSON list (or dict keyed by ticker) where each record has, at minimum:
    - a ticker field  (one of: ticker, market_ticker, event_ticker)
    - an outcome field (one of: result, settlement, outcome) whose value is
      "yes"/"no" (case-insensitive), or 1/0, or "settled_yes"/"settled_no".

If the script can't find these fields it prints what keys it DID see and exits,
so you can tell it the right names rather than getting silent wrong answers.
"""

import argparse
import json
import math
import sys
from collections import defaultdict

# ----- field-name candidates (defensive auto-detection) --------------------
TICKER_KEYS = ("ticker", "market_ticker", "market", "marketTicker")
OUTCOME_KEYS = ("result", "settlement", "outcome", "settled_result", "market_result")

YES_TOKENS = {"yes", "y", "1", "true", "settled_yes", "settled-yes"}
NO_TOKENS = {"no", "n", "0", "false", "settled_no", "settled-no"}

# ----- engine grouping ------------------------------------------------------
# entry_tier is the per-signal-path label. Several paths belong to one engine
# (e.g. the cascade engine fires four distinct paths). TIER_TO_ENGINE collapses
# paths -> engine so the report can show both a SPLIT view (by tier, which path
# is carrying/dragging) and a COLLAPSED view (by engine, larger N, stabler WR).
# Any tier not listed maps to itself.
TIER_TO_ENGINE = {
    "main": "main",
    "topup": "main",                       # top-up adds to a main position
    "peak": "peak",
    "last_bracket": "last_bracket",
    "hourly_nyc": "hourly_nyc",
    "cascade_afternoon": "cascade",
    "cascade_ovn_dist": "cascade",
    "cascade_directional_up": "cascade",
    "cascade_directional_down": "cascade",
    "tomorrow": "sweep",
    "tomorrow_sweep": "sweep",
    "tomorrow_dismissed": "sweep",
}


def tier_to_engine(tier):
    return TIER_TO_ENGINE.get(tier, tier)


def load_json(path):
    try:
        with open(path) as f:
            return json.load(f)
    except FileNotFoundError:
        sys.exit(
            f"File not found: {path}\n"
            "  - If this is the resolutions file, it must exist first. Either "
            "build it, or run with --fetch to pull outcomes from Kalshi.\n"
            "  - On Windows, check the file isn't actually named "
            f"'{path}.txt' (extension hiding) and is in this directory."
        )
    except json.JSONDecodeError as e:
        sys.exit(f"{path} is not valid JSON: {e}")


def first_present(d, keys):
    for k in keys:
        if k in d and d[k] not in (None, ""):
            return k
    return None


def normalise_outcome(v):
    """Return 'yes', 'no', or None (unsettled/unknown)."""
    s = str(v).strip().lower()
    if s in YES_TOKENS:
        return "yes"
    if s in NO_TOKENS:
        return "no"
    return None


def build_resolution_map(resolutions):
    """Return {ticker: 'yes'|'no'} and report detected field names."""
    if isinstance(resolutions, dict):
        # could be {ticker: record} or {ticker: "no"}
        records = []
        for k, v in resolutions.items():
            if isinstance(v, dict):
                v.setdefault("ticker", k)
                records.append(v)
            else:
                records.append({"ticker": k, "result": v})
    else:
        records = resolutions

    if not records:
        sys.exit("Resolution export is empty.")

    sample = records[0]
    tkey = first_present(sample, TICKER_KEYS)
    okey = first_present(sample, OUTCOME_KEYS)

    if tkey is None or okey is None:
        print("Could not auto-detect required fields in resolution export.")
        print(f"  Ticker field found: {tkey}")
        print(f"  Outcome field found: {okey}")
        print(f"  Keys present on first record: {sorted(sample.keys())}")
        sys.exit("Re-run after confirming field names (edit TICKER_KEYS / OUTCOME_KEYS).")

    res = {}
    skipped = 0
    for r in records:
        t = r.get(tkey)
        o = normalise_outcome(r.get(okey))
        if t is None or o is None:
            skipped += 1
            continue
        res[t] = o
    print(f"Resolution export: ticker='{tkey}', outcome='{okey}', "
          f"{len(res)} settled, {skipped} skipped (unsettled/unparseable).")
    return res


def wilson_ci(wins, n, z=1.96):
    """Wilson score interval for a binomial proportion. Returns (lo, hi)."""
    if n == 0:
        return (0.0, 0.0)
    p = wins / n
    denom = 1 + z * z / n
    centre = p + z * z / (2 * n)
    margin = z * math.sqrt((p * (1 - p) + z * z / (4 * n)) / n)
    return ((centre - margin) / denom, (centre + margin) / denom)


def per_contract_pnl(side, entry_price, outcome):
    """
    Realized PnL per contract, derived from price + outcome.
    Convention: a NO contract pays $1 if it settles 'no', else $0.
                a YES contract pays $1 if it settles 'yes', else $0.
    """
    win = (side == outcome)
    if win:
        return 1.0 - entry_price
    return -entry_price


def summarise(rows):
    """rows: list of dicts with keys win(bool), pnl(float), contracts(int)."""
    n = len(rows)
    wins = sum(1 for r in rows if r["win"])
    pnl = sum(r["pnl"] * r["contracts"] for r in rows)
    contracts = sum(r["contracts"] for r in rows)
    wr = wins / n if n else 0.0
    lo, hi = wilson_ci(wins, n)
    return {
        "n": n,
        "wins": wins,
        "wr": wr,
        "wr_lo": lo,
        "wr_hi": hi,
        "pnl": pnl,
        "contracts": contracts,
        "pnl_per_contract": pnl / contracts if contracts else 0.0,
    }


def fmt_block(title, stats_by_key, min_n):
    ranked = {k: v for k, v in stats_by_key.items() if v["n"] >= min_n}
    held = {k: v for k, v in stats_by_key.items() if v["n"] < min_n}

    print(f"\n=== {title} (ranked, N >= {min_n}) ===")
    if not ranked:
        print("  (no cells meet the sample-size gate)")
    else:
        order = sorted(ranked.items(), key=lambda kv: kv[1]["pnl_per_contract"])
        print(f"  {'key':20} {'N':>4} {'WR':>7} {'95% CI':>17} "
              f"{'PnL/ctr':>9} {'PnL$':>9}")
        for k, s in order:
            ci = f"[{s['wr_lo']*100:4.1f},{s['wr_hi']*100:4.1f}]"
            print(f"  {k:20} {s['n']:>4} {s['wr']*100:6.1f}% {ci:>17} "
                  f"{s['pnl_per_contract']:>9.3f} {s['pnl']:>9.2f}")
    if held:
        print(f"  --- insufficient data (N < {min_n}), not ranked ---")
        for k, s in sorted(held.items(), key=lambda kv: -kv[1]["n"]):
            print(f"  {k:20} {s['n']:>4}  (WR {s['wr']*100:.0f}% — too few to trust)")


# ---------------------------------------------------------------------------
# Optional: pull settled outcomes directly from Kalshi
# ---------------------------------------------------------------------------
# We resolve each logged ticker via the MARKET object's `result` field
# (∈ {"yes","no",""}), NOT via portfolio/positions. portfolio/positions returns
# only currently-OPEN positions, so it would miss all already-settled history.
# The market `result` field is populated once a market settles and is the
# authoritative outcome for a No bet (result=="no" => our No won).
#
# Reuses the project's own KalshiClient (RSA-PSS auth) from trader.py, so auth
# and the KALSHI_DEMO safety default behave exactly like the live system. This
# is read-only: only GET /markets is called, never an order endpoint.

def _chunks(seq, size):
    for i in range(0, len(seq), size):
        yield seq[i:i + size]


def _load_credentials():
    """
    Populate os.environ with Kalshi credentials, mirroring scheduler.py's
    own loading order EXACTLY:

      1. data/config.json  (keys: key_id, key_file, live_mode)  -- primary
      2. .env fallback     (KEY=VALUE lines)                    -- only if no config.json

    This is how the live system actually loads creds. There is no .env on the
    Pi; the credentials live in data/config.json. live_mode=true maps to
    KALSHI_DEMO=false (real prod endpoint).
    """
    import os
    from pathlib import Path
    config_file = Path("data/config.json")
    if config_file.exists():
        try:
            config = json.loads(config_file.read_text())
            if config.get("key_id"):
                os.environ.setdefault("KALSHI_KEY_ID", config["key_id"])
            if config.get("key_file"):
                os.environ.setdefault("KALSHI_KEY_FILE", config["key_file"])
            os.environ["KALSHI_DEMO"] = "false" if config.get("live_mode") else "true"
            return
        except Exception as e:   # noqa: BLE001
            print(f"  warning: could not parse data/config.json ({e}); "
                  "trying .env")
    env_file = Path(".env")
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            if "=" in line and not line.startswith("#"):
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())


def fetch_resolutions_from_kalshi(tickers, out_path=None, batch_size=100):
    """
    Query Kalshi for each ticker's market result. Returns a list of
    {"ticker":..., "result": "yes"|"no"|"unsettled"} and optionally writes it.

    Batches via GET /markets?tickers=a,b,c and falls back to single-ticker
    GET /markets/{ticker} for any ticker missing from a batch response.
    """
    try:
        import trader  # the project's client lives here
    except ImportError as e:
        sys.exit(
            "Could not import trader.py to reach KalshiClient.\n"
            f"  ({e})\n"
            "  Run this from the weathermachine project directory, or pass a "
            "pre-built resolutions file instead of --fetch."
        )

    _load_credentials()   # config.json first, then .env — matches scheduler.py
    client = trader.make_client(skip_confirmation=True)
    uniq = sorted(set(tickers))
    print(f"Fetching results for {len(uniq)} unique tickers from Kalshi "
          f"(base: {client.base_url}) ...")

    found = {}   # ticker -> result string ("" if open/unsettled)

    for batch in _chunks(uniq, batch_size):
        try:
            data = client.get("markets", params={"tickers": ",".join(batch)})
            for m in data.get("markets", []):
                found[m.get("ticker")] = (m.get("result") or "").lower()
        except Exception as e:   # noqa: BLE001 - batch failure shouldn't abort run
            print(f"  batch failed ({e}); falling back to singles for "
                  f"{len(batch)} tickers")

    # Single-ticker fallback for anything the batches didn't return
    missing = [t for t in uniq if t not in found]
    if missing:
        print(f"  resolving {len(missing)} tickers individually ...")
    for t in missing:
        try:
            data = client.get(f"markets/{t}")
            m = data.get("market", data)
            found[t] = (m.get("result") or "").lower()
        except Exception as e:   # noqa: BLE001
            print(f"    {t}: fetch failed ({e})")
            found[t] = ""

    records = []
    settled = 0
    for t in uniq:
        r = found.get(t, "")
        if r in ("yes", "no"):
            settled += 1
            records.append({"ticker": t, "result": r})
        else:
            records.append({"ticker": t, "result": "unsettled"})

    print(f"  {settled}/{len(uniq)} settled; "
          f"{len(uniq) - settled} still open/unsettled.")

    if out_path:
        with open(out_path, "w") as f:
            json.dump(records, f, indent=2)
        print(f"  wrote {out_path}")

    return records


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("trade_log")
    ap.add_argument("resolutions", nargs="?",
                    help="resolution export JSON. Optional when --fetch is set; "
                         "then it's the output path (default resolutions.json).")
    ap.add_argument("--fetch", action="store_true",
                    help="pull settled outcomes live from Kalshi via trader.py "
                         "and write them to the resolutions path before analysing")
    ap.add_argument("--min-n", type=int, default=25)
    ap.add_argument("--by-tier", action="store_true",
                    help="also break each city down by entry_tier")
    ap.add_argument("--include-paper", action="store_true")
    args = ap.parse_args()

    trades = load_json(args.trade_log)

    if args.fetch:
        out_path = args.resolutions or "resolutions.json"
        tickers = [t.get("ticker") for t in trades
                   if t.get("ticker") and
                   (args.include_paper or not t.get("paper", False))]
        records = fetch_resolutions_from_kalshi(tickers, out_path=out_path)
        res = build_resolution_map(records)
    else:
        if not args.resolutions:
            sys.exit("Provide a resolutions file, or pass --fetch to pull from "
                     "Kalshi. e.g. python city_performance.py trade_log.json "
                     "resolutions.json  (or add --fetch)")
        res = build_resolution_map(load_json(args.resolutions))

    rows_by_city = defaultdict(list)
    rows_by_tier = defaultdict(list)
    rows_by_engine = defaultdict(list)
    rows_by_city_tier = defaultdict(list)
    unmatched = 0
    paper_skipped = 0

    for t in trades:
        if t.get("paper", False) and not args.include_paper:
            paper_skipped += 1
            continue
        ticker = t.get("ticker")
        outcome = res.get(ticker)
        if outcome is None:
            unmatched += 1
            continue
        side = str(t.get("side", "")).lower()
        entry = float(t.get("entry_price", 0.0))
        contracts = int(t.get("contracts", 1))
        win = (side == outcome)
        pnl = per_contract_pnl(side, entry, outcome)
        row = {"win": win, "pnl": pnl, "contracts": contracts}
        rows_by_city[t.get("city", "?")].append(row)
        rows_by_tier[t.get("entry_tier", "?")].append(row)
        rows_by_engine[tier_to_engine(t.get("entry_tier", "?"))].append(row)
        rows_by_city_tier[(t.get("city", "?"), t.get("entry_tier", "?"))].append(row)

    matched = sum(len(v) for v in rows_by_city.values())
    print(f"\nMatched {matched} settled trades. "
          f"Unmatched (no resolution): {unmatched}. "
          f"Paper skipped: {paper_skipped}.")
    if matched == 0:
        sys.exit("No trades matched a resolution. Check ticker formats line up.")

    overall = summarise([r for rows in rows_by_city.values() for r in rows])
    print(f"\nOVERALL: N={overall['n']}  WR={overall['wr']*100:.1f}%  "
          f"PnL=${overall['pnl']:.2f}  PnL/ctr={overall['pnl_per_contract']:.3f}")

    fmt_block("BY CITY", {c: summarise(r) for c, r in rows_by_city.items()},
              args.min_n)
    fmt_block("BY TIER (signal path, split)",
              {c: summarise(r) for c, r in rows_by_tier.items()},
              args.min_n)
    fmt_block("BY ENGINE (collapsed)",
              {c: summarise(r) for c, r in rows_by_engine.items()},
              args.min_n)

    if args.by_tier:
        cell_stats = {f"{c} / {tier}": summarise(r)
                      for (c, tier), r in rows_by_city_tier.items()}
        fmt_block("BY CITY x TIER", cell_stats, args.min_n)

    print("\nNote: 'worst city' by point estimate is expected even from pure "
          "noise across ~20 cities. Trust a difference only when the Wilson "
          "intervals do not overlap and N clears the gate.")


if __name__ == "__main__":
    main()
