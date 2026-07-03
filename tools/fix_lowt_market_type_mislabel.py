"""
fix_lowt_market_type_mislabel.py

One-off migration: relabel historical data/trade_log.json records that were
mislabeled market_type="high" for signals that actually came from the LOWT
cascade path (cascade_lowt_bu, cascade_lowt_td, cascade_ratchet).

ROOT CAUSE (fixed going forward in cascade_engine.py):
_make_signal() never stamped market_type on individual signal dicts.
evaluate_city_cascade_lowt() stamped market_type="lowt" on the *outer*
result dict but not on each signal inside result["signals"]. trader.py
then read signal.get("market_type", "high") per-signal when logging the
trade and when choosing which per-city cap (HIGH vs LOWT) to enforce
against — so every LOWT cascade signal silently defaulted to "high".

This script only fixes the historical DATA (trade_log.json). It does not
touch code — the code fix lives in cascade_engine.py
(evaluate_city_cascade_lowt), which must be deployed separately via the
normal git pull + service restart.

SAFE TO RUN LIVE: uses the same fcntl.flock() exclusive lock over the
whole read-modify-write cycle that trader.py's _locked_json_rmw() uses,
so it serializes correctly against the running services rather than
racing them.

Usage (on the Pi):
    cd ~/weathermachine
    python3 tools/fix_lowt_market_type_mislabel.py            # dry run, reports only
    python3 tools/fix_lowt_market_type_mislabel.py --apply    # applies the fix

Idempotent: running it again after applying finds zero records to change.
"""

import argparse
import json
import os
import sys
from pathlib import Path

try:
    import fcntl

    def _lock_file(f):
        fcntl.flock(f, fcntl.LOCK_EX)

    def _unlock_file(f):
        fcntl.flock(f, fcntl.LOCK_UN)
except ImportError:                                        # pragma: no cover
    import msvcrt

    def _lock_file(f):
        f.seek(0)
        msvcrt.locking(f.fileno(), msvcrt.LK_LOCK, 1)

    def _unlock_file(f):
        f.seek(0)
        try:
            msvcrt.locking(f.fileno(), msvcrt.LK_UNLCK, 1)
        except OSError:
            pass


TRADE_LOG_FILE = Path("data/trade_log.json")


def _is_lowt_ticker(ticker: str) -> bool:
    return "LOWT" in ticker


def _is_high_or_hourly_ticker(ticker: str) -> bool:
    return "HIGH" in ticker or "TEMPNYCH" in ticker


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply", action="store_true",
        help="Actually write changes. Without this flag, only reports what would change.",
    )
    args = parser.parse_args()

    if not TRADE_LOG_FILE.exists():
        print(f"ERROR: {TRADE_LOG_FILE} not found. Run this from the weathermachine repo root.")
        sys.exit(1)

    with open(TRADE_LOG_FILE, "r+") as f:
        _lock_file(f)
        try:
            f.seek(0)
            raw = f.read()
            data = json.loads(raw) if raw.strip() else []

            fixed = []
            unexpected_reverse = []  # HIGH/hourly ticker but market_type == "lowt" — flagged, not touched

            for entry in data:
                ticker = entry.get("ticker", "")
                mt = entry.get("market_type")

                if _is_lowt_ticker(ticker) and mt != "lowt":
                    fixed.append({
                        "ticker": ticker,
                        "entry_tier": entry.get("entry_tier"),
                        "placed_at": entry.get("placed_at"),
                        "old_market_type": mt,
                    })
                    entry["market_type"] = "lowt"
                elif _is_high_or_hourly_ticker(ticker) and mt == "lowt":
                    unexpected_reverse.append({
                        "ticker": ticker,
                        "entry_tier": entry.get("entry_tier"),
                        "placed_at": entry.get("placed_at"),
                    })

            print(f"Total records:            {len(data)}")
            print(f"To relabel high -> lowt:  {len(fixed)}")
            for r in fixed:
                print(f"    {r['placed_at']}  {r['ticker']}  tier={r['entry_tier']}")

            print(f"Unexpected reverse cases (NOT auto-fixed, needs manual review): {len(unexpected_reverse)}")
            for r in unexpected_reverse:
                print(f"    {r['placed_at']}  {r['ticker']}  tier={r['entry_tier']}")

            if not args.apply:
                print("\nDry run only — no changes written. Re-run with --apply to write changes.")
                return

            if not fixed:
                print("\nNothing to apply — file already clean.")
                return

            f.seek(0)
            f.truncate()
            json.dump(data, f, indent=2, default=str)
            f.flush()
            os.fsync(f.fileno())
            print(f"\nApplied. Relabeled {len(fixed)} record(s) in {TRADE_LOG_FILE}.")

        finally:
            _unlock_file(f)


if __name__ == "__main__":
    main()
