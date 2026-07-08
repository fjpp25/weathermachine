#!/usr/bin/env python3
"""
tools/audit_trade_log_vs_kalshi.py — cross-check trade_log.json against
Kalshi's own fills history, the only authoritative external record of what
was actually transacted.

WHY THIS IS THE STRONGER CHECK: tools/audit_trade_log_today.py can only
audit the CURRENT day (engine_capital_deployed.json overwrites at
midnight, retaining no history). This script hits Kalshi's own
/portfolio/fills endpoint directly, which is unaffected by anything our
own logging did or didn't record.

LIMITATION, stated up front: per this repo's own prior notes, /portfolio/
fills has a ROLLING CUTOFF — it does not return arbitrarily old fills. This
can only check RECENT history (however far back Kalshi actually retains),
not the full multi-month backtest window this conversation has been
analyzing. Treat a clean result here as "no evidence of loss recently",
not "no evidence of loss ever" — the older history is not checkable this
way, and isn't checkable at all if it was never captured (same lesson as
observations.db's historical gaps).

UPGRADED (2026-07-08): this used to only check ticker PRESENCE — "does
trade_log.json have at least one entry for this ticker". That misses a
real, confirmed failure mode: a ticker can have MULTIPLE separate entry
fills over time (e.g. an initial lowt_a entry, then a later, larger
cascade entry on the same bracket), where only SOME of those entries make
it into trade_log.json. Presence-only checking calls that ticker "clean"
because *an* entry exists, even though Kalshi shows more contracts bought
than we ever logged. Confirmed real case: KXLOWTNYC-26JUL06-B63.5 had a
1-contract fill that WAS logged (lowt_a) and a later 2-contract fill that
WAS NOT (untagged) — 3 total contracts on Kalshi, 1 in trade_log.json.
This script now compares CONTRACT COUNTS per ticker, not just presence,
which catches partial losses the old version was structurally blind to.

LOGIC: fetch recent "no"-side BUY fills (entries only — sells are exits,
which are never expected to have a trade_log.json entry, see FIXED note
below). For each ticker, sum Kalshi's fill contracts and compare against
the sum of trade_log.json's logged contracts for that same ticker:
  - Kalshi has the ticker, trade_log.json has NOTHING for it  → fully missing
  - Kalshi's contract count > trade_log.json's logged count   → partially missing
  - trade_log.json's count > Kalshi's                          → over-logged (flagged, lower priority)

READ-ONLY: only GET /portfolio/fills is called. No orders are placed or
modified.

USAGE (on the Pi, from repo root):
    python3 tools/audit_trade_log_vs_kalshi.py
"""
import json
import sys
from pathlib import Path

# Running as `python3 tools/audit_trade_log_vs_kalshi.py` only puts tools/
# on sys.path, not the repo root — `import trader` fails otherwise. Insert
# the repo root explicitly rather than requiring PYTHONPATH=. or a
# different invocation style, to stay consistent with how every other
# tools/*.py script in this repo is actually run.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

TRADE_LOG = Path("data/trade_log.json")


def load_credentials():
    import os
    config_file = Path("data/config.json")
    if config_file.exists():
        config = json.loads(config_file.read_text())
        if config.get("key_id"):
            os.environ.setdefault("KALSHI_KEY_ID", config["key_id"])
        if config.get("key_file"):
            os.environ.setdefault("KALSHI_KEY_FILE", config["key_file"])
        os.environ["KALSHI_DEMO"] = "false" if config.get("live_mode") else "true"


def main():
    load_credentials()
    import trader
    client = trader.make_client(skip_confirmation=True)

    # Per-ticker LOGGED CONTRACT TOTALS, not just presence — a ticker can
    # legitimately have more than one trade_log.json entry (topup, a second
    # tier firing on the same bracket), so sum across all of them.
    logged_contracts: dict[str, float] = {}
    logged_entries_count: dict[str, int] = {}
    if TRADE_LOG.exists():
        for e in json.loads(TRADE_LOG.read_text()):
            if e.get("paper", False):
                continue
            ticker = e.get("ticker", "")
            if not ticker:
                continue
            logged_contracts[ticker] = (
                logged_contracts.get(ticker, 0.0) + float(e.get("contracts", 1) or 1)
            )
            logged_entries_count[ticker] = logged_entries_count.get(ticker, 0) + 1

    print(f"trade_log.json: {len(logged_contracts)} distinct non-paper tickers logged "
          f"({sum(logged_entries_count.values())} total entries).")

    try:
        resp = client.get("portfolio/fills", params={"limit": 200})
    except Exception as e:
        print(f"Fills fetch failed: {e}")
        return

    fills = resp.get("fills", [])

    # FIXED: this previously filtered only on side=="no" (which side of the
    # market — Yes vs No), completely ignoring "action" (buy vs sell). That
    # silently mixed ENTRIES (buy No — what trade_log.json is meant to
    # capture) with EXITS (sell No — closing an existing No position).
    # Confirmed via source: trader.py's check_exits() calls place_order(...,
    # action="sell", ...) with NO _append_trade_log() call anywhere nearby —
    # exits are placed but never logged, by design. Comparing a sell fill
    # against an entry-only log and calling the absence "evidence of a lost
    # write" was a methodology error, not a real finding. Every "missing
    # fills" number this script has reported so far may be inflated by an
    # unknown amount from this — re-run and treat the corrected numbers as
    # authoritative, not the earlier ones.
    entry_fills = [f for f in fills
                   if str(f.get("side", "")).lower() == "no"
                   and str(f.get("action", "")).lower() == "buy"]
    exit_fills_excluded = sum(
        1 for f in fills
        if str(f.get("side", "")).lower() == "no"
        and str(f.get("action", "")).lower() != "buy"
    )
    print(f"Kalshi /portfolio/fills: {len(fills)} total fills returned "
          f"({len(entry_fills)} No-side BUY/entry fills, "
          f"{exit_fills_excluded} No-side non-buy fills excluded — likely "
          f"exits, which are never expected to have a trade_log.json entry). "
          f"Note the rolling cutoff — see module docstring.")

    if entry_fills:
        print(f"\nSample raw fill object (to verify field names below are "
              f"correct, since prior guesses for count/price/time were wrong "
              f"— they all printed as None):")
        print(json.dumps(entry_fills[0], indent=2))

    # Per-ticker KALSHI CONTRACT TOTALS — sum count_fp across every entry
    # fill for that ticker, however many partial-fill records it took.
    kalshi_contracts: dict[str, float] = {}
    kalshi_fill_records: dict[str, list] = {}
    for f in entry_fills:
        ticker = f.get("ticker", "")
        if not ticker:
            continue
        kalshi_contracts[ticker] = (
            kalshi_contracts.get(ticker, 0.0) + float(f.get("count_fp", 0) or 0)
        )
        kalshi_fill_records.setdefault(ticker, []).append(f)

    fully_missing: dict[str, list] = {}
    partially_missing: dict[str, dict] = {}
    over_logged: dict[str, dict] = {}

    for ticker, k_contracts in kalshi_contracts.items():
        l_contracts = logged_contracts.get(ticker, 0.0)
        if ticker not in logged_contracts:
            fully_missing[ticker] = kalshi_fill_records[ticker]
        elif k_contracts > l_contracts + 1e-9:
            partially_missing[ticker] = {
                "kalshi": k_contracts,
                "logged": l_contracts,
                "shortfall": k_contracts - l_contracts,
                "fills": kalshi_fill_records[ticker],
                "n_trade_log_entries": logged_entries_count.get(ticker, 0),
            }
        elif l_contracts > k_contracts + 1e-9:
            over_logged[ticker] = {"kalshi": k_contracts, "logged": l_contracts}

    if fully_missing:
        n_tickers = len(fully_missing)
        n_raw = sum(len(v) for v in fully_missing.values())
        n_contracts = sum(
            sum(float(f.get("count_fp", 0) or 0) for f in flist)
            for flist in fully_missing.values()
        )
        print(f"\n*** {n_tickers} DISTINCT ticker(s) with a 'no' fill on Kalshi "
              f"but ZERO trade_log.json entries ({n_raw} raw fill records, "
              f"{n_contracts:.0f} total contracts) — fully lost write(s): ***")
        for ticker, flist in list(fully_missing.items())[:20]:
            times = [f.get("created_time", f.get("created_at", "?")) for f in flist]
            contracts = sum(float(f.get("count_fp", 0) or 0) for f in flist)
            print(f"  {ticker}  ({contracts:.0f} contract(s), {len(flist)} fill "
                  f"record(s))  first_seen={min(times)}")
        if n_tickers > 20:
            print(f"  ... and {n_tickers - 20} more distinct tickers")
    else:
        print(f"\nNo fully-missing tickers in this window "
              f"(every ticker with a fill has AT LEAST ONE trade_log.json entry).")

    if partially_missing:
        n_tickers = len(partially_missing)
        n_shortfall = sum(v["shortfall"] for v in partially_missing.values())
        print(f"\n*** {n_tickers} ticker(s) where Kalshi shows MORE contracts "
              f"than trade_log.json has logged for them — a PARTIAL lost "
              f"write. These were INVISIBLE to presence-only checking, since "
              f"they do have at least one trade_log.json entry, just not "
              f"enough. {n_shortfall:.0f} total under-logged contracts: ***")
        for ticker, info in sorted(partially_missing.items(),
                                   key=lambda kv: -kv[1]["shortfall"]):
            times = [f.get("created_time", "?") for f in info["fills"]]
            print(f"  {ticker}  kalshi={info['kalshi']:.0f}c  "
                  f"logged={info['logged']:.0f}c  "
                  f"({info['n_trade_log_entries']} trade_log.json entr(y/ies))  "
                  f"shortfall={info['shortfall']:.0f}c  fill_times={times}")
    else:
        print(f"\nNo partial-logging mismatches in this window.")

    if over_logged:
        print(f"\n{len(over_logged)} ticker(s) where trade_log.json shows MORE "
              f"contracts than Kalshi's fills — worth a look (possible "
              f"duplicate write?), but lower priority than under-counting, "
              f"since over-counting doesn't hide real exposure:")
        for ticker, info in over_logged.items():
            print(f"  {ticker}  kalshi={info['kalshi']:.0f}c  logged={info['logged']:.0f}c")

    if fully_missing or partially_missing:
        # Dollar-value summary, filtered to today — directly tests whether
        # today's missing/under-logged contracts are large enough to explain
        # a specific deployed-vs-logged gap seen elsewhere (e.g. via
        # tools/audit_trade_log_today.py).
        from datetime import date as _date, datetime as _dt
        today = str(_date.today())
        today_total = 0.0
        today_tickers = set()

        def _accumulate(flist):
            nonlocal today_total
            for f in flist:
                ct = f.get("created_time", "")
                try:
                    if _dt.fromisoformat(ct.replace("Z", "+00:00")).date() != _date.today():
                        continue
                except ValueError:
                    continue
                count = float(f.get("count_fp", 0) or 0)
                price = float(f.get("no_price_dollars", 0) or 0)
                today_total += count * price
                today_tickers.add(f.get("ticker", ""))

        for flist in fully_missing.values():
            _accumulate(flist)
        for info in partially_missing.values():
            _accumulate(info["fills"])

        print(f"\n  Missing/under-logged fills dated TODAY ({today}): "
              f"{len(today_tickers)} distinct ticker(s), ${today_total:.2f} "
              f"total notional (this is the FULL fill value for "
              f"partially-missing tickers, not just the shortfall — compare "
              f"loosely, not exactly).")
        print(f"  Compare against any known same-day deployed-vs-logged gap "
              f"(e.g. via tools/audit_trade_log_today.py) — a close match is "
              f"strong evidence those specific missing fills explain that gap.")
    else:
        print(f"\nNo mismatches of any kind found in the fills window Kalshi "
              f"returned. Per the rolling-cutoff limitation above, this "
              f"covers only recent history, not the full backtest period.")


if __name__ == "__main__":
    main()
