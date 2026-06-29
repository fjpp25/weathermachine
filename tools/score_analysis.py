"""
score_analysis.py
-----------------
Measures win rate, EV, and ROI broken down by signal score (1/2/3)
and by individual score_detail components.

Data sources:
  data/trade_log.json   — every signal placed, with score + score_detail
  Kalshi settlements API — outcome per ticker (YES / NO)
  Kalshi fills API       — actual fill price per trade

Usage:
  python score_analysis.py
  python score_analysis.py --days 14   # restrict to last N days
  python score_analysis.py --paper     # include paper trades
"""

import os
import csv
import json
import argparse
from datetime import datetime, timezone, timedelta
from pathlib import Path
from collections import defaultdict
from zoneinfo import ZoneInfo

try:
    import requests
except ImportError:
    raise SystemExit("pip install requests")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

TRADE_LOG   = Path("data/trade_log.json")
PROD_BASE   = "https://api.elections.kalshi.com/trade-api/v2"
FEE_PER_WIN = 0.025   # midpoint of observed 2-3¢ fee

EXPORT_CSV  = Path("data/trades_for_entry_analysis.csv")

# City → timezone, for converting placed_at UTC → local entry hour
CITY_TZ = {
    "New York":      "America/New_York",
    "Chicago":       "America/Chicago",
    "Miami":         "America/New_York",
    "Austin":        "America/Chicago",
    "Los Angeles":   "America/Los_Angeles",
    "Denver":        "America/Denver",
    "Philadelphia":  "America/New_York",
    "San Francisco": "America/Los_Angeles",
    "Boston":        "America/New_York",
    "Las Vegas":     "America/Los_Angeles",
    "Atlanta":       "America/New_York",
    "Oklahoma City": "America/Chicago",
    "Phoenix":       "America/Phoenix",
    "Washington DC": "America/New_York",
    "Seattle":       "America/Los_Angeles",
    "Houston":       "America/Chicago",
    "Dallas":        "America/Chicago",
    "San Antonio":   "America/Chicago",
    "New Orleans":   "America/Chicago",
    "Minneapolis":   "America/Chicago",
}

# ---------------------------------------------------------------------------
# Load trade log
# ---------------------------------------------------------------------------

def load_trades(days: int = None, include_paper: bool = False) -> list[dict]:
    if not TRADE_LOG.exists():
        raise SystemExit(f"Trade log not found: {TRADE_LOG}")

    trades = json.loads(TRADE_LOG.read_text())

    if not include_paper:
        trades = [t for t in trades if not t.get("paper")]

    if days:
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        trades = [
            t for t in trades
            if datetime.fromisoformat(t["placed_at"]) >= cutoff
        ]

    print(f"  Loaded {len(trades)} trades from log"
          + (f" (last {days} days)" if days else "")
          + (" [including paper]" if include_paper else ""))
    return trades


# ---------------------------------------------------------------------------
# Fetch settlement outcomes from Kalshi
# ---------------------------------------------------------------------------

def fetch_settlements(tickers: list[str]) -> dict[str, str]:
    """
    Returns {ticker: 'yes' | 'no' | 'open'} for each ticker.
    Batches requests to avoid URL length limits.
    """
    outcomes = {}
    batch_size = 50

    for i in range(0, len(tickers), batch_size):
        batch = tickers[i:i + batch_size]
        try:
            resp = requests.get(
                f"{PROD_BASE}/markets",
                params={"tickers": ",".join(batch)},
                timeout=15,
            ).json()
            for m in resp.get("markets", []):
                ticker = m.get("ticker", "")
                result = (m.get("result") or "").lower()
                status = m.get("status", "")
                if result in ("yes", "no"):
                    outcomes[ticker] = result
                elif status == "active":
                    outcomes[ticker] = "open"
                else:
                    # Settled but result not in batch — try individual fetch
                    outcomes[ticker] = "unknown"
        except Exception as e:
            print(f"  [warn] Batch fetch failed: {e}")

    # Individual fallback for unknowns
    unknowns = [t for t, v in outcomes.items() if v == "unknown"]
    for ticker in unknowns:
        try:
            resp = requests.get(
                f"{PROD_BASE}/markets/{ticker}", timeout=10
            ).json()
            result = (resp.get("market", {}).get("result") or "").lower()
            outcomes[ticker] = result if result in ("yes", "no") else "open"
        except Exception:
            pass

    return outcomes


# ---------------------------------------------------------------------------
# Fetch actual fill prices
# ---------------------------------------------------------------------------

def fetch_fill_prices(client_headers_fn, tickers: set[str]) -> dict[str, float]:
    """
    Returns {ticker: avg_fill_price} from the fills endpoint.
    Falls back to trade_log entry_price if fills not available.
    """
    fill_prices = {}
    try:
        resp = requests.get(
            f"{PROD_BASE}/portfolio/fills",
            params={"limit": 200},
            headers=client_headers_fn("GET", "/trade-api/v2/portfolio/fills"),
            timeout=15,
        ).json()
        fills_by_ticker = defaultdict(list)
        for f in resp.get("fills", []):
            t = f.get("ticker", "")
            if t in tickers and f.get("action") == "buy":
                side = f.get("side", "")
                yes_p = float(f.get("yes_price_dollars") or 0)
                price = yes_p if side == "yes" else round(1.0 - yes_p, 4)
                fills_by_ticker[t].append(price)
        for ticker, prices in fills_by_ticker.items():
            fill_prices[ticker] = sum(prices) / len(prices)
    except Exception as e:
        print(f"  [warn] Fills fetch failed: {e}")
    return fill_prices


# ---------------------------------------------------------------------------
# Analysis
# ---------------------------------------------------------------------------

def analyse(trades: list[dict], outcomes: dict[str, str]) -> None:

    # Attach outcomes
    settled = []
    for t in trades:
        ticker  = t.get("ticker", "")
        outcome = outcomes.get(ticker, "open")
        if outcome == "open":
            continue   # skip unsettled
        entry  = t.get("entry_price", 0)
        score  = t.get("score", 0)
        detail = t.get("score_detail", [])
        mtype  = t.get("market_type", "high")
        side   = t.get("side", "no")
        won    = (outcome == "no" and side == "no") or \
                 (outcome == "yes" and side == "yes")
        gross  = (1.0 - entry) if won else -entry
        net    = gross - FEE_PER_WIN if won else gross

        settled.append({
            "ticker":    ticker,
            "city":      t.get("city", ""),
            "placed_at": t.get("placed_at", ""),
            "score":     score,
            "detail":    detail,
            "entry":     entry,
            "mtype":     mtype,
            "side":      side,
            "outcome":   outcome,
            "won":       won,
            "gross":     gross,
            "net":       net,
        })

    if not settled:
        print("\n  No settled trades to analyse.")
        return

    total = len(settled)
    wins  = sum(1 for t in settled if t["won"])
    print(f"\n  {'='*60}")
    print(f"  TRADE LOG ANALYSIS  —  {total} settled trades")
    print(f"  {'='*60}")
    print(f"  Overall win rate:  {wins}/{total} ({wins/total*100:.1f}%)")
    print(f"  Overall net EV:    ${sum(t['net'] for t in settled)/total:.4f} per trade")

    # ── By score ──────────────────────────────────────────────────────────
    print(f"\n  ── Win rate by score ────────────────────────────────────")
    print(f"  {'Score':>6}  {'N':>5}  {'Wins':>6}  {'Win%':>7}  "
          f"{'Avg entry':>10}  {'Net EV':>8}  {'ROI':>7}")
    print(f"  {'-'*62}")
    for score in sorted(set(t["score"] for t in settled)):
        sub   = [t for t in settled if t["score"] == score]
        n     = len(sub)
        w     = sum(1 for t in sub if t["won"])
        wr    = w / n
        avg_e = sum(t["entry"] for t in sub) / n
        ev    = sum(t["net"] for t in sub) / n
        roi   = ev / avg_e * 100
        bar   = "█" * int(wr * 20)
        print(f"  {score:>6}  {n:>5}  {w:>6}  {wr*100:>6.1f}%  "
              f"${avg_e:>9.2f}  ${ev:>7.4f}  {roi:>6.1f}%  {bar}")

    # ── By score_detail component ─────────────────────────────────────────
    print(f"\n  ── Win rate by score_detail component ───────────────────")
    component_stats = defaultdict(lambda: {"n":0,"wins":0,"net":0.0,"entry":0.0})
    for t in settled:
        for comp in t["detail"]:
            s = component_stats[comp]
            s["n"]     += 1
            s["wins"]  += int(t["won"])
            s["net"]   += t["net"]
            s["entry"] += t["entry"]

    print(f"  {'Component':<30}  {'N':>5}  {'Win%':>7}  {'Net EV':>8}")
    print(f"  {'-'*55}")
    for comp, s in sorted(component_stats.items(),
                          key=lambda x: x[1]["wins"]/x[1]["n"], reverse=True):
        n  = s["n"]
        wr = s["wins"] / n
        ev = s["net"] / n
        print(f"  {comp:<30}  {n:>5}  {wr*100:>6.1f}%  ${ev:>7.4f}")

    # ── By market type ────────────────────────────────────────────────────
    print(f"\n  ── Win rate by market type ──────────────────────────────")
    for mtype in sorted(set(t["mtype"] for t in settled)):
        sub = [t for t in settled if t["mtype"] == mtype]
        n   = len(sub)
        w   = sum(1 for t in sub if t["won"])
        ev  = sum(t["net"] for t in sub) / n
        print(f"  {mtype:<10}  {w}/{n} ({w/n*100:.1f}%)  net EV ${ev:.4f}/trade")

    # ── Worst losses ──────────────────────────────────────────────────────
    losses = sorted([t for t in settled if not t["won"]],
                    key=lambda t: t["net"])
    if losses:
        print(f"\n  ── Losses ({len(losses)} total) ─────────────────────────────")
        print(f"  {'Ticker':<32}  {'Score':>6}  {'Entry':>7}  {'Net PnL':>9}")
        print(f"  {'-'*60}")
        for t in losses[:10]:
            print(f"  {t['ticker']:<32}  {t['score']:>6}  "
                  f"${t['entry']:>6.2f}  ${t['net']:>8.4f}")
        if len(losses) > 10:
            print(f"  ... and {len(losses)-10} more")

    # ── By city + local entry hour ────────────────────────────────────────
    # Uses placed_at from trade_log converted to city local time.
    # Only trades with a known city timezone are included.
    print(f"\n  ── Win rate by local entry hour ─────────────────────────")
    hour_stats = defaultdict(lambda: {"n": 0, "wins": 0, "net": 0.0})
    skipped_city = 0
    for t in settled:
        city    = t.get("city", "")
        tz_name = CITY_TZ.get(city)
        placed  = t.get("placed_at", "")
        if not tz_name or not placed:
            skipped_city += 1
            continue
        try:
            dt         = datetime.fromisoformat(placed)
            local_hour = dt.astimezone(ZoneInfo(tz_name)).hour
            hour_stats[local_hour]["n"]    += 1
            hour_stats[local_hour]["wins"] += int(t["won"])
            hour_stats[local_hour]["net"]  += t["net"]
        except Exception:
            skipped_city += 1

    if hour_stats:
        print(f"  {'Hour':>6}  {'N':>5}  {'Wins':>6}  {'Win%':>7}  {'Net EV':>8}")
        print(f"  {'-'*42}")
        for h in sorted(hour_stats):
            s  = hour_stats[h]
            n  = s["n"]
            w  = s["wins"]
            wr = w / n
            ev = s["net"] / n
            bar = "█" * int(wr * 20)
            print(f"  {h:02d}:xx  {n:>5}  {w:>6}  {wr*100:>6.1f}%  ${ev:>7.4f}  {bar}")
        if skipped_city:
            print(f"  ({skipped_city} trades skipped — city or timestamp missing)")
    else:
        print(f"  No trades with city + timestamp data.")

    return settled


def export_trades_csv(settled: list[dict], path: Path) -> None:
    """
    Export settled trades to CSV for use with entry_window_analysis.py --trades.

    Columns match what entry_window_analysis.py expects:
      ticker, city, market_type, side, score, entry_price, net_pnl,
      placed_at, local_hour, won
    """
    rows = []
    for t in settled:
        city    = t.get("city", "")
        tz_name = CITY_TZ.get(city, "")
        placed  = t.get("placed_at", "")
        local_hour = ""
        if tz_name and placed:
            try:
                dt         = datetime.fromisoformat(placed)
                local_hour = dt.astimezone(ZoneInfo(tz_name)).hour
            except Exception:
                pass
        rows.append({
            "ticker":      t.get("ticker", ""),
            "city":        city,
            "market_type": t.get("mtype", ""),
            "side":        t.get("side", ""),
            "score":       t.get("score", ""),
            "entry_price": t.get("entry", ""),
            "net_pnl":     round(t.get("net", 0), 4),
            "placed_at":   placed,
            "local_hour":  local_hour,
            "won":         int(t.get("won", False)),
        })

    if not rows:
        print("  No rows to export.")
        return

    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    print(f"\n  Trades exported → {path}  ({len(rows)} rows)")


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Score vs win rate analysis")
    parser.add_argument("--days",   type=int, default=None,
                        help="Restrict to last N days")
    parser.add_argument("--paper",  action="store_true",
                        help="Include paper trades")
    parser.add_argument("--export", action="store_true",
                        help=f"Export trades CSV for entry_window_analysis.py → {EXPORT_CSV}")
    args = parser.parse_args()

    # Load env / config
    config_file = Path("data/config.json")
    if config_file.exists():
        config = json.loads(config_file.read_text())
        if config.get("key_id"):
            os.environ.setdefault("KALSHI_KEY_ID", config["key_id"])
    env_file = Path(".env")
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            if "=" in line and not line.startswith("#"):
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())

    trades = load_trades(days=args.days, include_paper=args.paper)
    if not trades:
        raise SystemExit("No trades found.")

    tickers = list({t["ticker"] for t in trades})
    print(f"  Fetching outcomes for {len(tickers)} unique tickers...")
    outcomes = fetch_settlements(tickers)

    settled_count = sum(1 for v in outcomes.values() if v in ("yes","no"))
    open_count    = sum(1 for v in outcomes.values() if v == "open")
    print(f"  Settled: {settled_count}  |  Still open: {open_count}")

    # Attach city from trade log (needed for entry hour analysis)
    for t in trades:
        if "city" not in t:
            # Infer city from ticker series if missing
            t["city"] = t.get("city", "")

    settled = analyse(trades, outcomes)

    if args.export and settled:
        export_trades_csv(settled, EXPORT_CSV)
        print(f"\n  Run entry_window_analysis.py --trades {EXPORT_CSV} to overlay win rates.")
