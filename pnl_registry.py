"""
pnl_registry.py
---------------
Read-only reporting layer. Derives all metrics from the existing
positions.json and trade_log.json files — never writes to them.

Outputs:
  data/trades.csv        — one row per closed trade, full detail
  data/daily_summary.csv — one row per calendar day, aggregated metrics

Both files are fully regenerated on every run, so they are always
consistent with the source files (no sync risk).

Failure mode taxonomy
---------------------
  none             — trade was profitable (closed at exit target or settlement)
  stop_loss        — YES trade cut early by stop-loss rule
  resolved_against — held to settlement, outcome went the wrong way
  unfilled         — order never matched (counted separately from losses)

Fee model
---------
Kalshi charges FEE_RATE cents per contract on the WINNING side only.
Adjust FEE_RATE_PER_CONTRACT below if Kalshi changes their fee schedule.
The current default (0.07) reflects ~7¢ per contract.
All P&L figures in this module are NET of fees.

Usage:
  python pnl_registry.py                  # regenerate both CSVs
  python pnl_registry.py --trades         # print trades table to console
  python pnl_registry.py --summary        # print daily summary to console
  python pnl_registry.py --score-report   # print per-score performance
"""

import csv
import json
import argparse
from collections import defaultdict
from datetime import datetime, timezone, date
from pathlib import Path

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

POSITIONS_FILE  = Path("data/positions.json")
TRADE_LOG_FILE  = Path("data/trade_log.json")
TRADES_CSV      = Path("data/trades.csv")
SUMMARY_CSV     = Path("data/daily_summary.csv")

# Kalshi fee: charged per contract on the winning side.
# A YES win on N contracts at entry price P pays out N*(1-P) in profit.
# The fee is applied to that profit: fee = N * FEE_RATE_PER_CONTRACT
# A losing trade pays no fee (you already lost the premium).
# Update this constant if Kalshi's schedule changes.
FEE_RATE_PER_CONTRACT = 0.07   # dollars per contract on winning side

# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_positions() -> list[dict]:
    if not POSITIONS_FILE.exists():
        return []
    with open(POSITIONS_FILE) as f:
        return json.load(f)


def load_trade_log() -> list[dict]:
    if not TRADE_LOG_FILE.exists():
        return []
    with open(TRADE_LOG_FILE) as f:
        return json.load(f)


def build_entry_log_index(trade_log: list[dict]) -> dict[str, dict]:
    """
    Index trade_log "entry" events by ticker so we can enrich
    position rows with fields that positions.json doesn't store
    (city, forecast_high_at_entry, etc.).

    If multiple entry events exist for the same ticker (e.g. after a
    position was closed and re-opened on a later day), we keep the
    most recent one — it will align with the most recent position row.
    """
    index = {}
    for event in trade_log:
        if event.get("event") == "entry":
            ticker = event.get("ticker")
            if ticker:
                index[ticker] = event   # last write wins
    return index

# ---------------------------------------------------------------------------
# Fee calculation
# ---------------------------------------------------------------------------

def compute_fee(side: str, contracts: int, entry_price: float,
                exit_price: float, gross_pnl: float) -> float:
    """
    Fee is charged only on winning trades, per contract.
    Returns fee in dollars (always >= 0).
    """
    if gross_pnl <= 0:
        return 0.0
    return round(contracts * FEE_RATE_PER_CONTRACT, 4)

# ---------------------------------------------------------------------------
# Failure mode classification
# ---------------------------------------------------------------------------

def classify_failure(exit_reason: str, gross_pnl: float) -> str:
    """
    Map an exit reason + outcome to a failure mode label.
    Returns "none" for profitable trades.
    """
    if gross_pnl > 0:
        return "none"

    reason_map = {
        "exit_target":       "none",            # shouldn't be negative, but guard it
        "stop_loss":         "stop_loss",
        "resolved_win":      "none",
        "resolved_against":  "resolved_against",
        "unfilled":          "unfilled",
    }
    return reason_map.get(exit_reason, "resolved_against")   # safe default

# ---------------------------------------------------------------------------
# Build trade rows
# ---------------------------------------------------------------------------

def build_trade_rows(
    positions: list[dict],
    log_index: dict[str, dict],
) -> list[dict]:
    """
    Convert closed positions into enriched, fee-adjusted trade rows,
    sorted by exit (closed_at) date ascending.
    """
    rows = []

    for pos in positions:
        if pos.get("status") != "closed":
            continue

        ticker      = pos["ticker"]
        side        = pos.get("side", "?")
        contracts   = pos.get("contracts", 0)
        entry_price = pos.get("entry_price", 0.0)
        exit_price  = pos.get("exit_price") or 0.0
        gross_pnl   = pos.get("pnl") or 0.0          # already computed in trader.py
        exit_reason = pos.get("exit_reason", "unknown")
        score       = pos.get("score", "?")
        score_detail = ",".join(pos.get("score_detail", []))
        trade_type  = side.upper()                    # YES or NO

        # Enrich from trade log
        log_entry           = log_index.get(ticker, {})
        city                = log_entry.get("city", "unknown")
        forecast_high       = log_entry.get("forecast_high_at_entry")   # may be None
        settlement_temp     = pos.get("settlement_temp")                # may be None

        # Fee and net P&L
        fee     = compute_fee(side, contracts, entry_price, exit_price, gross_pnl)
        net_pnl = round(gross_pnl - fee, 4)

        # Net P&L as % of capital deployed
        capital_deployed = round(entry_price * contracts, 4)
        net_pnl_pct = (
            round(net_pnl / capital_deployed * 100, 2)
            if capital_deployed > 0 else 0.0
        )

        failure_mode = classify_failure(exit_reason, gross_pnl)

        # Parse dates
        opened_at = pos.get("opened_at", "")
        closed_at = pos.get("closed_at", "")
        try:
            exit_date = datetime.fromisoformat(closed_at).date().isoformat()
        except Exception:
            exit_date = "unknown"
        try:
            entry_date = datetime.fromisoformat(opened_at).date().isoformat()
        except Exception:
            entry_date = "unknown"

        rows.append({
            "trade_id":              pos.get("id", "?"),
            "ticker":                ticker,
            "city":                  city,
            "side":                  trade_type,
            "score":                 score,
            "score_detail":          score_detail,
            "trade_type":            trade_type,
            "entry_date":            entry_date,
            "exit_date":             exit_date,
            "entry_time_utc":        opened_at,
            "exit_time_utc":         closed_at,
            "entry_price":           entry_price,
            "exit_price":            exit_price,
            "contracts":             contracts,
            "capital_deployed":      capital_deployed,
            "forecast_high_at_entry":forecast_high if forecast_high is not None else "",
            "settlement_temp":       settlement_temp if settlement_temp is not None else "",
            "exit_reason":           exit_reason,
            "failure_mode":          failure_mode,
            "gross_pnl":             round(gross_pnl, 4),
            "fee":                   fee,
            "net_pnl":               net_pnl,
            "net_pnl_pct":           net_pnl_pct,
            "cumulative_net_pnl":    None,   # filled in next pass
        })

    # Sort by exit date ascending, then fill cumulative P&L
    rows.sort(key=lambda r: r["exit_time_utc"])
    cumulative = 0.0
    for row in rows:
        cumulative += row["net_pnl"]
        row["cumulative_net_pnl"] = round(cumulative, 4)

    return rows

# ---------------------------------------------------------------------------
# Build daily summary rows
# ---------------------------------------------------------------------------

def build_daily_summary(trade_rows: list[dict]) -> list[dict]:
    """
    Aggregate trade rows by exit date.
    Also accumulates running cumulative P&L across days.
    """
    # Group by exit_date
    by_day: dict[str, list[dict]] = defaultdict(list)
    for row in trade_rows:
        by_day[row["exit_date"]].append(row)

    summary_rows = []
    cumulative   = 0.0

    for day in sorted(by_day.keys()):
        day_trades = by_day[day]

        wins              = [t for t in day_trades if t["net_pnl"] > 0]
        losses_stop       = [t for t in day_trades if t["failure_mode"] == "stop_loss"]
        losses_resolved   = [t for t in day_trades if t["failure_mode"] == "resolved_against"]
        losses_unfilled   = [t for t in day_trades if t["failure_mode"] == "unfilled"]
        total_losses      = losses_stop + losses_resolved + losses_unfilled

        total_trades      = len(day_trades)
        win_rate          = round(len(wins) / total_trades * 100, 1) if total_trades else 0.0

        capital_deployed  = round(sum(t["capital_deployed"] for t in day_trades), 4)
        gross_pnl         = round(sum(t["gross_pnl"]  for t in day_trades), 4)
        total_fees        = round(sum(t["fee"]         for t in day_trades), 4)
        net_pnl           = round(sum(t["net_pnl"]     for t in day_trades), 4)
        net_roi_pct       = (
            round(net_pnl / capital_deployed * 100, 2)
            if capital_deployed > 0 else 0.0
        )

        cumulative       += net_pnl

        # Average net P&L per signal score (only scores present that day)
        score_buckets: dict[int, list[float]] = defaultdict(list)
        for t in day_trades:
            try:
                score_buckets[int(t["score"])].append(t["net_pnl"])
            except (ValueError, TypeError):
                pass

        avg_score1 = _avg(score_buckets.get(1, []))
        avg_score2 = _avg(score_buckets.get(2, []))
        avg_score3 = _avg(score_buckets.get(3, []))

        summary_rows.append({
            "date":                   day,
            "trades_closed":          total_trades,
            "wins":                   len(wins),
            "losses_stop_loss":       len(losses_stop),
            "losses_resolved_against":len(losses_resolved),
            "losses_unfilled":        len(losses_unfilled),
            "total_losses":           len(total_losses),
            "win_rate_pct":           win_rate,
            "capital_deployed":       capital_deployed,
            "gross_pnl":              gross_pnl,
            "total_fees":             total_fees,
            "net_pnl":                net_pnl,
            "net_roi_pct":            net_roi_pct,
            "cumulative_net_pnl":     round(cumulative, 4),
            "avg_net_pnl_score_1":    avg_score1,
            "avg_net_pnl_score_2":    avg_score2,
            "avg_net_pnl_score_3":    avg_score3,
        })

    return summary_rows


def _avg(values: list[float]) -> str:
    """Return formatted average or empty string if no values."""
    if not values:
        return ""
    return str(round(sum(values) / len(values), 4))

# ---------------------------------------------------------------------------
# CSV writers
# ---------------------------------------------------------------------------

TRADES_FIELDS = [
    "trade_id", "ticker", "city", "side", "score", "score_detail",
    "trade_type", "entry_date", "exit_date", "entry_time_utc", "exit_time_utc",
    "entry_price", "exit_price", "contracts", "capital_deployed",
    "forecast_high_at_entry", "settlement_temp",
    "exit_reason", "failure_mode",
    "gross_pnl", "fee", "net_pnl", "net_pnl_pct", "cumulative_net_pnl",
]

SUMMARY_FIELDS = [
    "date", "trades_closed", "wins",
    "losses_stop_loss", "losses_resolved_against", "losses_unfilled", "total_losses",
    "win_rate_pct",
    "capital_deployed", "gross_pnl", "total_fees", "net_pnl", "net_roi_pct",
    "cumulative_net_pnl",
    "avg_net_pnl_score_1", "avg_net_pnl_score_2", "avg_net_pnl_score_3",
]


def write_csv(path: Path, fields: list[str], rows: list[dict]):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

# ---------------------------------------------------------------------------
# Console display helpers
# ---------------------------------------------------------------------------

def display_trades(rows: list[dict]):
    if not rows:
        print("No closed trades on record.")
        return

    print(f"\n{'='*95}")
    print(f"  Trades ({len(rows)} closed)")
    print(f"{'='*95}")
    print(
        f"  {'Date':<12} {'City':<14} {'Ticker':<28} {'Side':>4} {'Sc':>3} "
        f"{'Entry':>6} {'Exit':>6} {'Qty':>4} "
        f"{'GrossPnL':>9} {'Fee':>6} {'NetPnL':>8} {'ROI%':>6} "
        f"{'FailureMode':<18} {'CumPnL':>8}"
    )
    print(f"  {'-'*93}")

    for t in rows:
        print(
            f"  {t['exit_date']:<12} "
            f"{t['city']:<14} "
            f"{t['ticker']:<28} "
            f"{t['side']:>4} "
            f"{str(t['score']):>3} "
            f"${t['entry_price']:.2f} "
            f"${t['exit_price']:.2f} "
            f"{t['contracts']:>4} "
            f"${t['gross_pnl']:>+7.2f} "
            f"${t['fee']:>4.2f} "
            f"${t['net_pnl']:>+6.2f} "
            f"{t['net_pnl_pct']:>+5.1f}% "
            f"{t['failure_mode']:<18} "
            f"${t['cumulative_net_pnl']:>+6.2f}"
        )

    total_net = rows[-1]["cumulative_net_pnl"] if rows else 0.0
    total_fees = sum(r["fee"] for r in rows)
    print(f"\n  Total fees paid: ${total_fees:.2f}  |  Cumulative net P&L: ${total_net:+.2f}")
    print(f"{'='*95}")


def display_summary(rows: list[dict]):
    if not rows:
        print("No daily summary data.")
        return

    print(f"\n{'='*95}")
    print(f"  Daily Summary ({len(rows)} days)")
    print(f"{'='*95}")
    print(
        f"  {'Date':<12} {'Trades':>6} {'Wins':>5} {'SL':>4} {'Res':>4} {'Unf':>4} "
        f"{'Win%':>5} {'Capital':>8} {'GrossPnL':>9} {'Fees':>6} "
        f"{'NetPnL':>8} {'ROI%':>6} {'CumPnL':>8}"
    )
    print(f"  {'-'*93}")

    for d in rows:
        print(
            f"  {d['date']:<12} "
            f"{d['trades_closed']:>6} "
            f"{d['wins']:>5} "
            f"{d['losses_stop_loss']:>4} "
            f"{d['losses_resolved_against']:>4} "
            f"{d['losses_unfilled']:>4} "
            f"{d['win_rate_pct']:>4.0f}% "
            f"${d['capital_deployed']:>7.2f} "
            f"${d['gross_pnl']:>+7.2f}  "
            f"${d['total_fees']:>4.2f} "
            f"${d['net_pnl']:>+6.2f} "
            f"{d['net_roi_pct']:>+5.1f}% "
            f"${d['cumulative_net_pnl']:>+6.2f}"
        )

    print(f"\n  Column guide: SL=stop_loss  Res=resolved_against  Unf=unfilled")
    print(f"{'='*95}")


def display_score_report(trade_rows: list[dict]):
    """Per-score performance breakdown across all closed trades."""
    from collections import defaultdict

    buckets: dict[int, list[dict]] = defaultdict(list)
    for t in trade_rows:
        try:
            buckets[int(t["score"])].append(t)
        except (ValueError, TypeError):
            pass

    if not buckets:
        print("No scored trades on record.")
        return

    print(f"\n{'='*65}")
    print(f"  Performance by Signal Score")
    print(f"{'='*65}")
    print(
        f"  {'Score':>6} {'Trades':>7} {'Wins':>6} {'Win%':>6} "
        f"{'AvgNetPnL':>10} {'TotalNetPnL':>12}"
    )
    print(f"  {'-'*55}")

    for score in sorted(buckets.keys()):
        trades      = buckets[score]
        wins        = [t for t in trades if t["net_pnl"] > 0]
        win_rate    = round(len(wins) / len(trades) * 100, 1) if trades else 0.0
        avg_net     = round(sum(t["net_pnl"] for t in trades) / len(trades), 4)
        total_net   = round(sum(t["net_pnl"] for t in trades), 4)
        print(
            f"  {score:>6} "
            f"{len(trades):>7} "
            f"{len(wins):>6} "
            f"{win_rate:>5.1f}% "
            f"${avg_net:>+9.4f} "
            f"${total_net:>+11.4f}"
        )

    print(f"{'='*65}")

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run(verbose: bool = False) -> tuple[list[dict], list[dict]]:
    """
    Full pipeline: load → enrich → compute → write CSVs.
    Returns (trade_rows, summary_rows) for programmatic use.
    """
    positions  = load_positions()
    trade_log  = load_trade_log()
    log_index  = build_entry_log_index(trade_log)

    trade_rows   = build_trade_rows(positions, log_index)
    summary_rows = build_daily_summary(trade_rows)

    write_csv(TRADES_CSV,  TRADES_FIELDS,  trade_rows)
    write_csv(SUMMARY_CSV, SUMMARY_FIELDS, summary_rows)

    if verbose:
        print(f"  Wrote {len(trade_rows)} rows  → {TRADES_CSV}")
        print(f"  Wrote {len(summary_rows)} rows → {SUMMARY_CSV}")

    return trade_rows, summary_rows


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kalshi weather P&L registry")
    parser.add_argument("--trades",       action="store_true",
                        help="Print trade-level table to console")
    parser.add_argument("--summary",      action="store_true",
                        help="Print daily summary table to console")
    parser.add_argument("--score-report", action="store_true",
                        help="Print per-score performance breakdown")
    args = parser.parse_args()

    trade_rows, summary_rows = run(verbose=True)

    if args.trades:
        display_trades(trade_rows)

    if args.summary:
        display_summary(summary_rows)

    if args.score_report:
        display_score_report(trade_rows)

    if not any([args.trades, args.summary, args.score_report]):
        # Default: show everything
        display_trades(trade_rows)
        display_summary(summary_rows)
        display_score_report(trade_rows)
