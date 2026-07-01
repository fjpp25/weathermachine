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

LOGIC: fetch recent "no"-side fills. For each fill's ticker, check whether
ANY trade_log.json entry mentions that exact ticker with paper=False. A
fill with NO matching entry, from ANY engine, is direct, unambiguous
evidence of a lost trade_log.json write — this doesn't require knowing
which engine's entry_tier maps to which capital bucket (unlike the
today-only script), since it only checks ticker presence, not attribution.

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

    logged_tickers = set()
    if TRADE_LOG.exists():
        for e in json.loads(TRADE_LOG.read_text()):
            if not e.get("paper", False):
                logged_tickers.add(e.get("ticker", ""))

    print(f"trade_log.json: {len(logged_tickers)} distinct non-paper tickers logged.")

    try:
        resp = client.get("portfolio/fills", params={"limit": 200})
    except Exception as e:
        print(f"Fills fetch failed: {e}")
        return

    fills = resp.get("fills", [])
    no_fills = [f for f in fills if str(f.get("side", "")).lower() == "no"]
    print(f"Kalshi /portfolio/fills: {len(fills)} total fills returned "
          f"({len(no_fills)} on the 'no' side — the only side these engines "
          f"place). Note the rolling cutoff — see module docstring.")

    if no_fills:
        print(f"\nSample raw fill object (to verify field names below are "
              f"correct, since prior guesses for count/price/time were wrong "
              f"— they all printed as None):")
        print(json.dumps(no_fills[0], indent=2))

    # FIXED: previously iterated per raw fill record and flagged each one
    # individually. A single placed order can generate MULTIPLE partial-fill
    # records on Kalshi's side (filled by several different counterparties)
    # while our side only ever writes ONE trade_log entry per placed order.
    # The original version double/triple/quadruple-counted this as separate
    # "missing" incidents — confirmed directly: of the first 20 raw rows
    # printed in one run, only 15 were distinct tickers. Group by ticker
    # first; report distinct-ticker count as the headline number, with the
    # raw fill count as secondary context.
    missing_by_ticker: dict[str, list] = {}
    for f in no_fills:
        ticker = f.get("ticker", "")
        if ticker and ticker not in logged_tickers:
            missing_by_ticker.setdefault(ticker, []).append(f)

    if missing_by_ticker:
        n_tickers = len(missing_by_ticker)
        n_raw = sum(len(v) for v in missing_by_ticker.values())
        print(f"\n*** {n_tickers} DISTINCT ticker(s) with a 'no' fill on Kalshi "
              f"but NO matching trade_log.json entry ({n_raw} raw fill records "
              f"— some tickers have multiple partial fills, not separate "
              f"incidents) — evidence of lost write(s): ***")
        for ticker, fills in list(missing_by_ticker.items())[:20]:
            times = [f.get("created_time", f.get("created_at", "?")) for f in fills]
            print(f"  {ticker}  ({len(fills)} fill record(s))  first_seen={min(times)}")
        if n_tickers > 20:
            print(f"  ... and {n_tickers - 20} more distinct tickers")
    else:
        print(f"\nNo mismatches found in the fills window Kalshi returned — "
              f"every 'no' fill has a matching trade_log.json entry. This is "
              f"reassuring for the checkable window, but per the rolling-cutoff "
              f"limitation above, doesn't clear the full historical period.")


if __name__ == "__main__":
    main()
