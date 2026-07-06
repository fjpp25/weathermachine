#!/usr/bin/env python3
"""
tools/lowt_autopsy.py
----------------------
Autopsy for recent LOWT performance, cross-checked against the two known
data-corruption windows that overlap "the last few days":

  1. nws_feed.py's fetch_observed_high_low() window bug (hardcoded
     limit=48, producing observation windows as short as ~3.5h for
     high-frequency stations — too short to capture overnight lows).
     Fixed by commit 49cf2a4 (2026-07-02T17:33:09Z). Signal A in
     lowt_decision_engine.py reads obs_low_f directly to decide entries
     ("observed low is already above the bracket cap"). Any Signal A
     entry placed before this timestamp may have acted on a truncated
     or otherwise wrong observed low.

  2. cascade_engine.py's evaluate_city_cascade_lowt() market_type
     mislabel. Fixed by commit 8c3e387 (2026-07-03T11:22:09Z). Before
     this fix, every LOWT cascade signal (cascade_lowt_bu,
     cascade_lowt_td, cascade_ratchet) was logged with
     market_type="high" and its per-city cap was therefore checked
     against the HIGH cap instead of the LOWT cap — i.e. LOWT per-city
     concentration was effectively uncapped for these three tiers
     before the fix.

WHY THIS SCRIPT: neither audit_trade_log_today.py (today-only, checks
for lost writes) nor audit_trade_log_vs_kalshi.py (checks for lost
writes, not win/loss) answer "how did LOWT actually perform lately,
broken down by tier and by which side of the two fixes each trade
landed on". This script does, using Kalshi's own settlement data (via
settlement_audit.analyse()) as the source of truth for win/loss — NOT
trade_log.json's market_type field, since that field is exactly what
fix #2 above made unreliable pre-fix for cascade tiers. LOWT markets
are identified by ticker string ("LOWT" in ticker), which Kalshi
assigns and our bug never touched, so this filter is reliable
regardless of the mislabel bug.

trade_log.json is used only for the metadata Kalshi's fills don't
carry — entry_tier (lowt_main / lowt_a / lowt_b / cascade_lowt_bu /
cascade_lowt_td / cascade_ratchet) — joined to each settlement record
by ticker, picking whichever trade_log entry has the placed_at closest
to the fill's entry_time_utc (a ticker can recur across a top-up or a
second tier firing on the same bracket, so ticker alone isn't always
unique).

CAVEAT — read before trusting the pre/post split: the two cutover
timestamps below are COMMIT timestamps, not deploy timestamps. The Pi
only runs what's actually been `git pull`ed and service-restarted. If
either fix sat committed-but-not-yet-pulled for a while, some trades
tagged POST_FIX here were still running the old code. Cross-check
against actual restart history (journald / shell history on the Pi)
before treating the split as exact — it's a strong prior, not proof.

USAGE (on the Pi, from repo root):
    python3 tools/lowt_autopsy.py --since 2026-06-29
    python3 tools/lowt_autopsy.py --since 2026-06-29 --city Chicago
    python3 tools/lowt_autopsy.py --since 2026-06-29 --csv
"""
from __future__ import annotations

import sys
import csv
import json
import argparse
from pathlib import Path
from datetime import datetime, timezone
from collections import defaultdict

# Running as `python3 tools/lowt_autopsy.py` puts tools/ on sys.path
# automatically (settlement_audit.py lives there too), but not the repo
# root — `import trader` needs that inserted explicitly.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import settlement_audit as sa  # tools/settlement_audit.py

TRADE_LOG = Path("data/trade_log.json")

# Commit timestamps for the two known corruption windows (see module
# docstring). Both are commit times, NOT deploy times — see CAVEAT above.
NWS_WINDOW_FIX_UTC       = datetime(2026, 7, 2, 17, 33, 9, tzinfo=timezone.utc)
CASCADE_MISLABEL_FIX_UTC = datetime(2026, 7, 3, 11, 22, 9, tzinfo=timezone.utc)

CASCADE_LOWT_TIERS = {"cascade_lowt_bu", "cascade_lowt_td", "cascade_ratchet"}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_dt(raw: str) -> datetime | None:
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def load_trade_log() -> list[dict]:
    if not TRADE_LOG.exists():
        print(f"  WARNING: {TRADE_LOG} not found — entry_tier will be "
              f"'unmatched' for every record.")
        return []
    try:
        return json.loads(TRADE_LOG.read_text())
    except Exception as e:
        print(f"  WARNING: could not parse {TRADE_LOG}: {e}")
        return []


def _index_trade_log_by_ticker(entries: list[dict]) -> dict[str, list[dict]]:
    idx: dict[str, list[dict]] = defaultdict(list)
    for e in entries:
        t = e.get("ticker", "")
        if t:
            idx[t].append(e)
    return idx


def join_entry_tier(record: dict, tl_by_ticker: dict[str, list[dict]]) -> dict:
    """
    Attach entry_tier (and paper flag) from trade_log.json to a settlement
    record, by ticker, picking the trade_log entry whose placed_at is
    closest to the fill's entry_time_utc.
    """
    candidates = tl_by_ticker.get(record["ticker"], [])
    if not candidates:
        record["entry_tier"] = "unmatched"
        record["paper"] = None
        return record

    entry_dt = _parse_dt(record.get("entry_time_utc", ""))
    if entry_dt is None or len(candidates) == 1:
        best = candidates[0]
    else:
        def _delta(e):
            dt = _parse_dt(e.get("placed_at", ""))
            return abs((dt - entry_dt).total_seconds()) if dt else float("inf")
        best = min(candidates, key=_delta)

    record["entry_tier"] = best.get("entry_tier") or "unknown"
    record["paper"] = best.get("paper")
    return record


def classify_cutover(record: dict) -> dict:
    entry_dt = _parse_dt(record.get("entry_time_utc", ""))
    tier = record.get("entry_tier", "")

    if entry_dt is None:
        record["nws_window"] = "unknown"
        record["mislabel_window"] = "unknown"
        return record

    record["nws_window"] = "PRE_FIX" if entry_dt < NWS_WINDOW_FIX_UTC else "POST_FIX"

    if tier in CASCADE_LOWT_TIERS:
        record["mislabel_window"] = (
            "PRE_FIX" if entry_dt < CASCADE_MISLABEL_FIX_UTC else "POST_FIX"
        )
    else:
        record["mislabel_window"] = "n/a"

    return record


def summarize(records: list[dict], group_key: str) -> list[tuple]:
    buckets: dict[str, dict] = defaultdict(
        lambda: {"n": 0, "wins": 0, "losses": 0, "pnl": 0.0, "open": 0, "other": 0}
    )
    for r in records:
        key = r.get(group_key) or "unknown"
        b = buckets[key]
        b["n"] += 1
        outcome = r.get("outcome", "")
        if outcome == "SETTLED_WIN":
            b["wins"] += 1
        elif outcome == "SETTLED_LOSS":
            b["losses"] += 1
        elif outcome == "OPEN":
            b["open"] += 1
        else:
            b["other"] += 1  # WOULD_HAVE_WON / WOULD_HAVE_LOST / EXITED_TODAY
        b["pnl"] += r.get("net_pnl", 0.0) or 0.0

    rows = []
    for key, b in sorted(buckets.items()):
        decided = b["wins"] + b["losses"]
        wr = (b["wins"] / decided * 100) if decided else None
        rows.append((key, b["n"], b["wins"], b["losses"], b["open"], b["other"], wr,
                     round(b["pnl"], 2)))
    return rows


def print_table(title: str, rows: list[tuple]):
    print(f"\n{'─'*78}\n  {title}\n{'─'*78}")
    print(f"  {'':<24}{'n':>5}{'W':>5}{'L':>5}{'open':>6}{'other':>7}{'WR%':>9}{'PnL$':>10}")
    for key, n, w, l, op, other, wr, pnl in rows:
        wr_str = f"{wr:7.1f}%" if wr is not None else "     n/a"
        print(f"  {key:<24}{n:>5}{w:>5}{l:>5}{op:>6}{other:>7}  {wr_str}{pnl:>10.2f}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="LOWT-specific autopsy, cross-checked vs. Kalshi settlements"
    )
    parser.add_argument("--since", type=str, default=None,
                         help="ISO date/datetime (UTC), e.g. 2026-06-29. "
                              "Only entries at/after this are included.")
    parser.add_argument("--city", type=str, default=None)
    parser.add_argument("--csv", action="store_true",
                         help="Write data/lowt_autopsy.csv")
    args = parser.parse_args()

    since_dt = None
    if args.since:
        since_dt = _parse_dt(args.since)
        if since_dt is None:
            print(f"Could not parse --since value: {args.since}")
            sys.exit(1)

    sa._load_credentials()

    import trader
    client = trader.make_client(skip_confirmation=True)

    print("\nFetching settlements...")
    settlements = sa.fetch_all_settlements(client)
    print(f"  {len(settlements)} settlements.")

    print("Fetching fills...")
    fills = sa.fetch_all_fills(client)
    print(f"  {len(fills)} fills.")

    print("Fetching open positions...")
    open_positions = sa.fetch_open_positions(client)
    print(f"  {len(open_positions)} open positions.")

    records = sa.analyse(settlements, fills, open_positions, city_filter=args.city)

    # LOWT filter by ticker string — authoritative, independent of our own
    # market_type logging bug (see module docstring).
    records = [r for r in records if "LOWT" in r.get("ticker", "")]

    if since_dt:
        def _after(r):
            dt = _parse_dt(r.get("entry_time_utc", ""))
            return dt is not None and dt >= since_dt
        records = [r for r in records if _after(r)]

    if not records:
        print("\nNo LOWT settlement records found for the given filters.")
        return

    tl_entries = load_trade_log()
    tl_by_ticker = _index_trade_log_by_ticker(tl_entries)
    n_lowt_tl = sum(1 for e in tl_entries if "LOWT" in e.get("ticker", ""))
    print(f"\n  {len(tl_entries)} trade_log.json entries loaded "
          f"({n_lowt_tl} with LOWT-ticker).")

    records = [join_entry_tier(r, tl_by_ticker) for r in records]
    records = [classify_cutover(r) for r in records]

    n_unmatched = sum(1 for r in records if r["entry_tier"] == "unmatched")
    if n_unmatched:
        print(f"  WARNING: {n_unmatched}/{len(records)} records had no "
              f"trade_log.json match by ticker — entry_tier unknown for "
              f"these (shown as 'unmatched'). This can happen if "
              f"trade_log.json has rotated/been trimmed since these "
              f"trades were placed.")

    print_table("By entry_tier", summarize(records, "entry_tier"))
    print_table(
        "By NWS observation-window fix cutover (Signal A obs_low_f risk)",
        summarize(records, "nws_window"),
    )

    cascade_records = [r for r in records if r["entry_tier"] in CASCADE_LOWT_TIERS]
    if cascade_records:
        print_table(
            "Cascade LOWT tiers only — by market_type-mislabel fix cutover",
            summarize(cascade_records, "mislabel_window"),
        )

    print_table("By city", summarize(records, "city"))

    losses = [r for r in records if r.get("outcome") == "SETTLED_LOSS"]
    if losses:
        print(f"\n{'─'*78}\n  Individual SETTLED_LOSS records ({len(losses)})\n{'─'*78}")
        for r in sorted(losses, key=lambda x: x.get("entry_time_utc", "")):
            print(
                f"  {r.get('entry_time_utc', '?'):<26}  {r['city']:<14}  "
                f"{r['ticker']:<28}  tier={r['entry_tier']:<16}  "
                f"entry=${r['entry_price']:.2f}  nws={r['nws_window']:<8}  "
                f"mislabel={r['mislabel_window']:<8}  pnl=${r['net_pnl']:.2f}"
            )
    else:
        print("\n  No SETTLED_LOSS records in this window. If Xico's "
              "impression of a bad run comes from OPEN or WOULD_HAVE_LOST "
              "positions, re-run once those settle, or widen --since.")

    if args.csv:
        out = Path("data/lowt_autopsy.csv")
        out.parent.mkdir(parents=True, exist_ok=True)
        fieldnames = [
            "entry_time_utc", "date", "city", "ticker", "entry_tier",
            "entry_price", "contracts", "outcome", "net_pnl",
            "nws_window", "mislabel_window", "paper",
        ]
        with open(out, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            w.writeheader()
            w.writerows(records)
        print(f"\n  Saved → {out}")


if __name__ == "__main__":
    main()
