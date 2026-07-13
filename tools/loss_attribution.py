"""
tools/loss_attribution.py

Diagnostic probe — NOT wired into any engine. Run standalone on the Pi.

Purpose:
    The Performance tab shows win-rate and net PnL per day, but win count
    and loss count are weak proxies for what actually drives PnL, because
    the payout is asymmetric (buy No at 0.60-0.92; a win nets 8-40c/contract,
    a loss costs 60-92c/contract). Two days with the same loss COUNT can
    have wildly different PnL depending on which trades lost and at what
    entry price.

    This script pulls settled trades (same enrichment logic as dashboard.py
    / app.py's PnL tab), then slices losses (and wins, for contrast) by:
        - engine tier (main / cascade / econv / near_cap / etc., from
          trade_log.json entry_tier)
        - city
        - bracket type (T-top / T-bottom / B), via the same floor/cap
          parsing as trader.py's _bracket_floor_ceiling()
        - entry price band
        - local entry hour bucket (morning / midday / afternoon / evening)

    Output: aggregate table per slice (n, win rate, Wilson lower bound,
    total PnL, avg PnL/trade, cost-weighted EV = total_pnl/total_cost),
    sorted by total PnL ascending (worst slices first), plus a list of the
    N most expensive individual losses with full context for manual read.

Usage:
    cd ~/weathermachine
    python3 tools/loss_attribution.py                 # last 60 days
    python3 tools/loss_attribution.py --days 30
    python3 tools/loss_attribution.py --min-n 5        # hide tiny slices
    python3 tools/loss_attribution.py --top-losses 25
    python3 tools/loss_attribution.py --csv out.csv    # also dump raw rows

This does not modify any engine config or trade_log. Read-only against the
Kalshi API (settlements + fills) and local data/trade_log.json.
"""

from __future__ import annotations

import argparse
import csv as csvmod
import json
import math
import sys
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

# Make the repo root importable regardless of cwd. This file lives at
# <repo_root>/tools/loss_attribution.py, so the root is one level up from
# this file's directory — running `python3 tools/loss_attribution.py` only
# puts tools/ on sys.path by default, not the root.
_REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO_ROOT))

# Reuse the authenticated client + city metadata from the existing codebase.
try:
    import trader
    from cities import CITIES as _ALL_CITIES, SERIES_TO_CITY as _SERIES_TO_CITY
except ImportError as e:
    print(f"ERROR: could not import repo modules from {_REPO_ROOT} "
          f"(needs trader.py, cities.py): {e}")
    sys.exit(1)


# ---------------------------------------------------------------------------
# Bracket type parsing — mirrors trader.py::_bracket_floor_ceiling exactly,
# but also returns a label (B / T-bottom / T-top) instead of just floor/cap.
# For LOWT, whether a T bracket is top or bottom depends on the sibling T
# bracket's value (same ambiguity noted in app.py's ticker formatter), so
# we resolve it the same way when sibling tickers are available; otherwise
# we fall back to the same default trader.py uses (HIGH T = bottom, LOWT T
# assumed bottom unless proven otherwise).
# ---------------------------------------------------------------------------

def classify_bracket(ticker: str, sibling_tickers: list[str] | None = None) -> str:
    try:
        parts = ticker.split("-")
        raw = parts[-1]
        is_lowt = "LOWT" in ticker.upper()

        if raw.startswith("B"):
            return "B"

        if raw.startswith("T"):
            if is_lowt and sibling_tickers:
                try:
                    my_val = float(raw[1:])
                    other_t_vals = [
                        float(t.split("-")[-1][1:])
                        for t in sibling_tickers
                        if t != ticker and t.split("-")[-1].startswith("T")
                    ]
                    if other_t_vals:
                        return "T-top" if my_val > min(other_t_vals) else "T-bottom"
                except ValueError:
                    pass
            # Fallback (matches trader.py default): HIGH T = bottom always
            # (top-T "above X" NO trades are banned per hight_decision_engine).
            # LOWT T defaults to bottom if we can't disambiguate.
            return "T-bottom" if not is_lowt else "T-bottom"
    except (ValueError, IndexError):
        pass
    return "?"


def price_band(entry_price: float) -> str:
    if entry_price < 0.60:
        return "<0.60"
    elif entry_price < 0.70:
        return "0.60-0.70"
    elif entry_price < 0.80:
        return "0.70-0.80"
    elif entry_price < 0.85:
        return "0.80-0.85"
    elif entry_price < 0.90:
        return "0.85-0.90"
    elif entry_price < 0.95:
        return "0.90-0.95"
    else:
        return "0.95+"


def hour_bucket(local_hour: int | None) -> str:
    if local_hour is None:
        return "unknown"
    if local_hour < 9:
        return "pre-9"
    elif local_hour < 12:
        return "9-12"
    elif local_hour < 15:
        return "12-15"
    elif local_hour < 19:
        return "15-19"
    else:
        return "19+"


def wilson_lower_bound(wins: int, n: int, z: float = 1.96) -> float:
    """Wilson score lower bound on win rate — same conservative-floor pattern
    used elsewhere in the analytics package, so small-sample slices don't
    look falsely damning or falsely safe."""
    if n == 0:
        return 0.0
    p = wins / n
    denom = 1 + z * z / n
    centre = p + z * z / (2 * n)
    margin = z * math.sqrt((p * (1 - p) + z * z / (4 * n)) / n)
    return max(0.0, (centre - margin) / denom)


# ---------------------------------------------------------------------------
# Data loading — mirrors dashboard.py/_fetch_settlements enrichment
# ---------------------------------------------------------------------------

def load_trade_log() -> dict:
    """ticker -> most recent trade_log entry (entry_tier, placed_at, city, score)."""
    p = Path("data/trade_log.json")
    if not p.exists():
        return {}
    try:
        entries = json.loads(p.read_text())
    except Exception:
        return {}
    out = {}
    for e in entries:
        tk = e.get("ticker")
        if tk:
            out[tk] = e  # last one wins (list is chronological)
    return out


def city_from_ticker(ticker: str) -> str:
    # cities.py already builds this reverse map (series_ticker -> city) via
    # build_series_map(), exported as SERIES_TO_CITY. Match on prefix rather
    # than substring-anywhere, since some series tickers are substrings of
    # others (e.g. KXHIGHNY could false-match inside a longer unrelated
    # ticker if checked with plain `in`).
    upper = ticker.upper()
    for series, city in _SERIES_TO_CITY.items():
        if upper.startswith(series.upper() + "-"):
            return city
    return "?"


def local_entry_hour(placed_at: str, city: str) -> int | None:
    if not placed_at:
        return None
    tz = _ALL_CITIES.get(city, {}).get("tz")
    if not tz:
        return None
    try:
        dt = datetime.fromisoformat(placed_at.replace("Z", "+00:00"))
        return dt.astimezone(ZoneInfo(tz)).hour
    except Exception:
        return None


def fetch_settlements(client, days: int) -> list[dict]:
    """Pull settlements + fills from Kalshi, enrich exactly like the
    dashboard PnL tab does (cost / net_pnl / won), restricted to temperature
    markets and to the last `days` days."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)

    all_settlements = []
    cursor = None
    for _ in range(50):
        params = {"limit": 200}
        if cursor:
            params["cursor"] = cursor
        data = client.get("portfolio/settlements", params=params)
        batch = data.get("settlements", [])
        all_settlements.extend(batch)
        cursor = data.get("cursor")
        if not cursor or not batch:
            break

    temp = [
        s for s in all_settlements
        if s.get("ticker", "").startswith("KX")
        and ("HIGH" in s.get("ticker", "") or "LOWT" in s.get("ticker", ""))
        and s.get("settled_time", "") >= cutoff.isoformat()
    ]
    if not temp:
        return []

    tickers = {s["ticker"] for s in temp}

    all_fills = []
    cursor = None
    for _ in range(50):
        params = {"limit": 200}
        if cursor:
            params["cursor"] = cursor
        data = client.get("portfolio/fills", params=params)
        batch = data.get("fills", [])
        all_fills.extend(batch)
        cursor = data.get("cursor")
        if not cursor or len(batch) < 200:
            break

    fills_by_ticker = defaultdict(list)
    for f in all_fills:
        t = f.get("ticker", "")
        if t in tickers:
            fills_by_ticker[t].append(f)

    trade_log = load_trade_log()

    enriched = []
    for s in temp:
        ticker = s.get("ticker", "")
        result = s.get("market_result", "").lower()
        fee = float(s.get("fee_cost") or 0)

        buy_fills = [f for f in fills_by_ticker.get(ticker, []) if f.get("action") == "buy"]
        if not buy_fills:
            continue

        sides = [f.get("side") for f in buy_fills]
        our_side = max(set(sides), key=sides.count)
        our_fills = [f for f in buy_fills if f.get("side") == our_side]
        contracts = int(sum(float(f.get("count_fp") or 0) for f in our_fills))

        cost = round(sum(
            (float(f.get("yes_price_dollars") or 0) if our_side == "yes"
             else (1.0 - float(f.get("yes_price_dollars") or 0)))
            * float(f.get("count_fp") or 0)
            for f in our_fills
        ), 4)

        if contracts == 0 or cost == 0:
            continue

        won = (result == our_side)
        net_pnl = round(contracts * 1.0 - cost - fee, 4) if won else round(-cost - fee, 4)
        entry_price = round(cost / contracts, 4)

        tlog = trade_log.get(ticker, {})
        entry_tier = (tlog.get("entry_tier", "") or "main").lower()
        city = tlog.get("city") or city_from_ticker(ticker)
        placed_at = tlog.get("placed_at", "")
        lh = local_entry_hour(placed_at, city)

        market_type = "LOW" if "LOWT" in ticker else "HIGH"
        entry_date = sorted(our_fills, key=lambda f: f.get("created_time", ""))[0].get(
            "created_time", "")[:10]

        enriched.append({
            "ticker": ticker,
            "date": entry_date,
            "city": city,
            "market_type": market_type,
            "bracket_type": classify_bracket(ticker),
            "entry_tier": entry_tier,
            "entry_price": entry_price,
            "price_band": price_band(entry_price),
            "local_hour": lh,
            "hour_bucket": hour_bucket(lh),
            "contracts": contracts,
            "cost": cost,
            "fee": fee,
            "won": won,
            "net_pnl": net_pnl,
        })

    return enriched


# ---------------------------------------------------------------------------
# Aggregation
# ---------------------------------------------------------------------------

def aggregate(rows: list[dict], key_fn, min_n: int = 1) -> list[dict]:
    buckets = defaultdict(list)
    for r in rows:
        buckets[key_fn(r)].append(r)

    out = []
    for key, items in buckets.items():
        n = len(items)
        if n < min_n:
            continue
        wins = sum(1 for i in items if i["won"])
        total_pnl = round(sum(i["net_pnl"] for i in items), 2)
        total_cost = round(sum(i["cost"] for i in items), 2)
        out.append({
            "key": key,
            "n": n,
            "wins": wins,
            "losses": n - wins,
            "win_rate": round(wins / n * 100, 1),
            "wilson_lb": round(wilson_lower_bound(wins, n) * 100, 1),
            "total_pnl": total_pnl,
            "avg_pnl": round(total_pnl / n, 3),
            "ev_per_dollar": round(total_pnl / total_cost, 4) if total_cost else 0.0,
        })
    out.sort(key=lambda x: x["total_pnl"])
    return out


def print_table(title: str, rows: list[dict]):
    print(f"\n{'─'*100}")
    print(f"  {title}")
    print(f"{'─'*100}")
    print(f"  {'SLICE':<22}{'N':>5}{'W':>5}{'L':>5}{'WR%':>8}{'WilsonLB%':>11}"
          f"{'TotalPnL':>11}{'Avg/Trd':>10}{'EV/$':>9}")
    for r in rows:
        print(f"  {str(r['key']):<22}{r['n']:>5}{r['wins']:>5}{r['losses']:>5}"
              f"{r['win_rate']:>8.1f}{r['wilson_lb']:>11.1f}"
              f"{r['total_pnl']:>11.2f}{r['avg_pnl']:>10.3f}{r['ev_per_dollar']:>9.3f}")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--days", type=int, default=60, help="lookback window (default 60)")
    ap.add_argument("--min-n", type=int, default=3, help="hide slices with fewer than N trades")
    ap.add_argument("--top-losses", type=int, default=20, help="show N most expensive losses")
    ap.add_argument("--csv", type=str, default=None, help="dump raw enriched rows to CSV")
    args = ap.parse_args()

    print(f"Fetching settlements (last {args.days} days)...")
    client = trader.make_client(skip_confirmation=True)
    rows = fetch_settlements(client, args.days)

    if not rows:
        print("No settled trades found in this window.")
        return

    print(f"Loaded {len(rows)} settled trades "
          f"({sum(1 for r in rows if r['won'])} wins / {sum(1 for r in rows if not r['won'])} losses)")

    print_table("BY ENGINE TIER", aggregate(rows, lambda r: r["entry_tier"], args.min_n))
    print_table("BY PRICE BAND", aggregate(rows, lambda r: r["price_band"], args.min_n))
    print_table("BY BRACKET TYPE", aggregate(rows, lambda r: r["bracket_type"], args.min_n))
    print_table("BY CITY", aggregate(rows, lambda r: r["city"], args.min_n))
    print_table("BY LOCAL ENTRY HOUR", aggregate(rows, lambda r: r["hour_bucket"], args.min_n))
    print_table("BY MARKET TYPE (HIGH/LOW)", aggregate(rows, lambda r: r["market_type"], args.min_n))

    # Two-dimensional cut on the two variables most likely to explain the
    # payout asymmetry: engine tier x price band.
    print_table(
        "BY ENGINE TIER x PRICE BAND",
        aggregate(rows, lambda r: f"{r['entry_tier']}/{r['price_band']}", args.min_n)
    )

    # Most expensive individual losses — read these manually. This is the
    # actual answer to "why do some 4-loss days cost 7x more than others."
    losses = [r for r in rows if not r["won"]]
    losses.sort(key=lambda r: r["cost"], reverse=True)
    print(f"\n{'─'*100}")
    print(f"  TOP {args.top_losses} MOST EXPENSIVE LOSSES")
    print(f"{'─'*100}")
    print(f"  {'DATE':<12}{'TICKER':<28}{'TIER':<14}{'CITY':<8}{'PRICE':>7}{'HR':>4}{'COST':>8}")
    for r in losses[:args.top_losses]:
        print(f"  {r['date']:<12}{r['ticker']:<28}{r['entry_tier']:<14}{r['city']:<8}"
              f"{r['entry_price']:>7.2f}{str(r['local_hour'] or '—'):>4}{r['cost']:>8.2f}")

    if args.csv:
        with open(args.csv, "w", newline="") as f:
            w = csvmod.DictWriter(f, fieldnames=list(rows[0].keys()))
            w.writeheader()
            w.writerows(rows)
        print(f"\nRaw rows written to {args.csv}")


if __name__ == "__main__":
    main()
