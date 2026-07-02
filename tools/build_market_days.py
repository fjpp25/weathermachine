#!/usr/bin/env python3
"""
tools/build_market_days.py — materialize a canonical "market history" table:
one row per (city, market_date, market_type) with the winning bracket, its
real geometry, and settlement type — the thing this whole conversation kept
re-deriving from scratch, differently, in every probe/backtest script.

WHY THIS EXISTS
----------------
This session repeatedly re-derived "who won" and bracket geometry ad hoc:
  - tools/backtest_furthest3.py inferred T/B edges POSITIONALLY (lowest rung
    of a sorted 6-bracket ladder = T-bottom, code-0.5; highest = T-top,
    code+0.5) because observations.db never stored floor_strike/cap_strike.
  - tools/probe_dead_on_arrival.py's opposing_t_yes() did the same.
  - Every script independently derived "settled = dict(SELECT ticker, result
    FROM settlements ...)" and joined on ticker.
Positional inference works but is a real source of risk — get the sort or
the T/B assumption wrong once and every downstream number is quietly off.
Now that fetch_settlements.py / load_settlements_to_db.py capture REAL
floor_strike/cap_strike (see those files' docstrings), this script builds
the single derived table every future probe should join against instead of
re-inferring anything.

DATE FORMAT FIX (post-hoc): market_date was originally stored as the raw
ticker segment (e.g. "26JUN30"), parsed locally in this file. Every other
consumer of "market_date" in this codebase — including
tools/forecast_error_by_city.py's --validate join — uses the canonical
analytics.wm_time.market_date_iso() converter, which returns ISO
("2026-06-30"). The mismatch meant a join against this table on market_date
silently matched ZERO rows, ever, for any city — not a "no violations
found" result, a "the key format never matched" result. Fixed by using the
same canonical converter here. Because this whole table is 100%
re-derivable from `settlements` on every run (never incrementally
maintained from any other source), the table is now dropped and rebuilt
from scratch each run instead of INSERT-ON-CONFLICT — a data-format change
like this one can never again leave stale rows from an old format sitting
alongside rows in the new one.

WHAT THIS DOES NOT FIX
------------------------
This is a settlement/geometry table, not a price-liquidity table. It would
NOT have caught the phantom zero-quote artifact from tools/probe_dead_on_
arrival.py (v1) — that was an entry-PRICE problem in observations.db, not an
outcome problem. It would NOT have caught the sweep_engine.py dedup bug —
that was live trading logic. Don't treat this table as a general fix for
"the issues we had"; it fixes the specific, real risk of inferring bracket
geometry positionally instead of reading it (and, as of this update, the
specific risk of a non-canonical date format silently breaking joins).

SCHEMA (table: market_days)
-----------------------------
  city, market_date, market_type   — identity (market_date is canonical ISO,
                                      via analytics.wm_time.market_date_iso —
                                      matches every other consumer of
                                      "market_date" in this codebase)
  winning_ticker, winning_code     — the bracket that settled 'yes'
  bracket_type                     — 'T-bottom' | 'T-top' | 'B'
  floor_strike, cap_strike         — REAL strikes from Kalshi, not inferred
  settle_lo, settle_hi             — the continuous interval this bracket
                                      settles over (market_utils.bracket_
                                      interval geometry, using REAL strikes)
  n_brackets_settled               — sanity check: should be 6 for a
                                      well-formed market
  n_yes                            — sanity check: should be exactly 1;
                                      rows with n_yes != 1 are flagged, not
                                      silently dropped or guessed at

USAGE (on the Pi, from repo root — run AFTER fetch_settlements.py +
load_settlements_to_db.py have captured geometry):
    python3 tools/build_market_days.py
"""
import math
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path

# Same fix as tools/audit_trade_log_vs_kalshi.py — running as
# `python3 tools/build_market_days.py` only puts tools/ on sys.path, not
# the repo root, so `import cities` fails otherwise.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from cities import CITIES
from analytics.wm_time import market_date_iso

DB = "data/observations.db"

# series ticker -> (city, market_type), built from the same registry every
# other module in this repo already treats as canonical — no duplicated
# mapping to drift out of sync.
_SERIES_TO_CITY: dict[str, tuple[str, str]] = {}
for _city, _meta in CITIES.items():
    if _meta.get("high_series"):
        _SERIES_TO_CITY[_meta["high_series"]] = (_city, "high")
    if _meta.get("lowt_series"):
        _SERIES_TO_CITY[_meta["lowt_series"]] = (_city, "lowt")


def parse_ticker(ticker: str):
    """KXHIGHATL-26JUN15-B82.5 -> (series, market_date_iso, code).
    market_date is the CANONICAL ISO date (analytics.wm_time.market_date_iso),
    not the raw ticker segment — see module docstring's DATE FORMAT FIX."""
    parts = ticker.split("-")
    if len(parts) < 3:
        return None
    series, code = parts[0], parts[-1]
    market_date = market_date_iso(ticker)
    if market_date is None:
        return None
    return series, market_date, code


def bracket_val(code: str):
    if code and code[0] in ("B", "T"):
        try:
            return float(code[1:])
        except ValueError:
            return None
    return None


def geometry(floor_strike, cap_strike):
    """Mirrors market_utils.bracket_interval, using REAL strikes — no
    positional inference. Returns (bracket_type, lo, hi)."""
    if floor_strike is not None and cap_strike is not None:
        return "B", floor_strike - 0.5, cap_strike + 0.5
    if floor_strike is not None and cap_strike is None:
        return "T-top", floor_strike + 0.5, math.inf
    if cap_strike is not None and floor_strike is None:
        return "T-bottom", -math.inf, cap_strike - 0.5
    return None, None, None


def rebuild_table(con):
    """Drop and recreate market_days. Safe: the table is 100% re-derived
    from `settlements` every run — nothing else writes to it, and nothing
    incrementally accumulates here. See DATE FORMAT FIX in the module
    docstring for why a clean rebuild (vs INSERT-ON-CONFLICT) matters."""
    con.execute("DROP TABLE IF EXISTS market_days")
    con.execute("""
        CREATE TABLE market_days (
            city               TEXT,
            market_date        TEXT,
            market_type        TEXT,
            winning_ticker      TEXT,
            winning_code        TEXT,
            bracket_type        TEXT,
            floor_strike        REAL,
            cap_strike          REAL,
            settle_lo           REAL,
            settle_hi           REAL,
            n_brackets_settled  INTEGER,
            n_yes               INTEGER,
            PRIMARY KEY (city, market_date, market_type)
        )
    """)


def main():
    con = sqlite3.connect(DB)
    rebuild_table(con)

    rows = con.execute("""
        SELECT ticker, result, floor_strike, cap_strike
        FROM settlements
        WHERE result IN ('yes', 'no')
    """).fetchall()

    grouped = defaultdict(list)   # (city, market_date, market_type) -> [rows]
    unmapped_series = set()
    for ticker, result, floor_strike, cap_strike in rows:
        parsed = parse_ticker(ticker)
        if not parsed:
            continue
        series, market_date, code = parsed
        mapping = _SERIES_TO_CITY.get(series)
        if not mapping:
            unmapped_series.add(series)
            continue
        city, market_type = mapping
        grouped[(city, market_date, market_type)].append(
            (ticker, result, code, floor_strike, cap_strike))

    inserted = 0
    flagged_multi_yes = 0
    flagged_zero_yes = 0
    flagged_no_geometry = 0
    for (city, market_date, market_type), bracket_rows in grouped.items():
        n_settled = len(bracket_rows)
        yes_rows = [r for r in bracket_rows if r[1] == "yes"]
        n_yes = len(yes_rows)

        if n_yes != 1:
            if n_yes == 0:
                flagged_zero_yes += 1
            else:
                flagged_multi_yes += 1
            # Still record it — with NULL winner fields — so the anomaly is
            # visible in the table itself rather than silently absent.
            con.execute("""
                INSERT INTO market_days
                    (city, market_date, market_type, winning_ticker,
                     winning_code, bracket_type, floor_strike, cap_strike,
                     settle_lo, settle_hi, n_brackets_settled, n_yes)
                VALUES (?, ?, ?, NULL, NULL, NULL, NULL, NULL, NULL, NULL, ?, ?)
            """, (city, market_date, market_type, n_settled, n_yes))
            inserted += 1
            continue

        ticker, result, code, floor_strike, cap_strike = yes_rows[0]
        if floor_strike is None and cap_strike is None:
            flagged_no_geometry += 1
            btype, lo, hi = None, None, None
        else:
            btype, lo, hi = geometry(floor_strike, cap_strike)
            lo = None if lo == -math.inf else lo
            hi = None if hi == math.inf else hi

        con.execute("""
            INSERT INTO market_days
                (city, market_date, market_type, winning_ticker,
                 winning_code, bracket_type, floor_strike, cap_strike,
                 settle_lo, settle_hi, n_brackets_settled, n_yes)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (city, market_date, market_type, ticker, code, btype,
              floor_strike, cap_strike, lo, hi, n_settled, n_yes))
        inserted += 1

    con.commit()

    total = con.execute("SELECT COUNT(*) FROM market_days").fetchone()[0]
    clean = con.execute(
        "SELECT COUNT(*) FROM market_days WHERE n_yes = 1").fetchone()[0]
    con.close()

    print(f"market_days: {inserted} market-days processed, {total} rows total")
    print(f"  clean (exactly 1 winner): {clean}")
    print(f"  flagged — 0 winners (n_yes=0): {flagged_zero_yes}")
    print(f"  flagged — multiple winners (n_yes>1, should be impossible, "
          f"investigate): {flagged_multi_yes}")
    print(f"  clean winner but NO geometry captured yet "
          f"(re-run fetch_settlements.py): {flagged_no_geometry}")
    if unmapped_series:
        print(f"  NOTE: {len(unmapped_series)} series tickers not found in "
              f"cities.py (expected for non-HIGH/LOWT series, e.g. NYC "
              f"hourly): {sorted(unmapped_series)[:10]}")


if __name__ == "__main__":
    main()
