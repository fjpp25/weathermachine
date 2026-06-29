"""
enrich_trade_log.py
-------------------
Enriches trade_log.json with settlement outcomes fetched from the Kalshi API,
then falls back to the observation CSV for any tickers not yet settled.

Output: enriched_trade_log.json — all original fields preserved, plus:

  settlement_source  : "kalshi" | "observation_csv" | "unresolved"
  won                : True / False / None
  net_pnl            : float — net PnL per contract * contracts - fee
  fee                : float — Kalshi fee charged at settlement
  settled_time       : ISO timestamp (from Kalshi) or None
  final_no_price     : float — last observed No price (from obs CSV)
  avg_entry_from_fills: float — avg entry price computed from fills data
  contracts_filled   : int   — total contracts filled per Kalshi fills

Usage:
    python enrich_trade_log.py
    python enrich_trade_log.py --trade-log path/to/trade_log.json
    python enrich_trade_log.py --obs-csv path/to/lowt_observations.csv
    python enrich_trade_log.py --out enriched.json
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# Resolve paths and bootstrap the Kalshi client using existing trader.py
# ---------------------------------------------------------------------------

CONFIG_FILE = Path("data/config.json")


def _load_config() -> dict:
    if CONFIG_FILE.exists():
        try:
            return json.loads(CONFIG_FILE.read_text())
        except Exception:
            pass
    return {}


def _make_client():
    config = _load_config()
    if config.get("key_id"):
        os.environ.setdefault("KALSHI_KEY_ID", config["key_id"])
    if config.get("key_file"):
        os.environ.setdefault("KALSHI_KEY_FILE", config["key_file"])
    os.environ["KALSHI_DEMO"] = "false" if config.get("live_mode") else "true"
    import trader
    return trader.make_client(skip_confirmation=True)


# ---------------------------------------------------------------------------
# Kalshi data fetchers
# ---------------------------------------------------------------------------

def fetch_all_settlements(client) -> list[dict]:
    """Fetch all settled temperature market positions from Kalshi."""
    settlements = []
    cursor = None
    page = 0
    while True:
        params = {"limit": 200, "settlement_status": "settled"}
        if cursor:
            params["cursor"] = cursor
        try:
            data  = client.get("portfolio/settlements", params=params)
        except Exception as e:
            print(f"  [WARN] settlements fetch error (page {page}): {e}")
            break
        batch  = data.get("settlements", [])
        settlements.extend(batch)
        cursor = data.get("cursor")
        page  += 1
        print(f"  settlements: fetched {len(settlements)} so far (page {page})")
        if not cursor or len(batch) < 200:
            break

    # Filter to temperature markets only
    temp = [
        s for s in settlements
        if "HIGH" in s.get("ticker", "").upper()
        or "LOWT" in s.get("ticker", "").upper()
    ]
    print(f"  settlements: {len(temp)} temperature market settlements")
    return temp


def fetch_all_fills(client) -> list[dict]:
    """Fetch all fills from Kalshi (paginated)."""
    fills  = []
    cursor = None
    page   = 0
    while True:
        params = {"limit": 200}
        if cursor:
            params["cursor"] = cursor
        try:
            data  = client.get("portfolio/fills", params=params)
        except Exception as e:
            print(f"  [WARN] fills fetch error (page {page}): {e}")
            break
        batch  = data.get("fills", [])
        fills.extend(batch)
        cursor = data.get("cursor")
        page  += 1
        print(f"  fills: fetched {len(fills)} so far (page {page})")
        if not cursor or len(batch) < 200:
            break

    # Filter to temperature markets only
    temp = [
        f for f in fills
        if "HIGH" in f.get("ticker", "").upper()
        or "LOWT" in f.get("ticker", "").upper()
    ]
    print(f"  fills: {len(temp)} temperature market fills")
    return temp


# ---------------------------------------------------------------------------
# Enrichment logic
# ---------------------------------------------------------------------------

def build_settlement_index(settlements: list[dict]) -> dict[str, dict]:
    """Return {ticker: settlement_record}."""
    index = {}
    for s in settlements:
        t = s.get("ticker", "")
        if t:
            index[t] = s
    return index


def build_fills_index(fills: list[dict]) -> dict[str, list[dict]]:
    """Return {ticker: [fill, ...]}."""
    index: dict[str, list] = {}
    for f in fills:
        t = f.get("ticker", "")
        if t:
            index.setdefault(t, []).append(f)
    return index


def _enrich_from_kalshi(
    entry: dict,
    settlement: dict,
    fills: list[dict],
    entry_contracts: int,
    total_contracts_for_ticker: int,
) -> dict:
    """
    Enrich one trade log entry using Kalshi settlement + fills data.

    When multiple log entries share the same ticker (the engine entered the
    same bracket on successive polls), the settlement covers the entire
    position. Each entry's share of net_pnl and fee is:

        share = entry_contracts / total_contracts_for_ticker

    Returns the enriched dict (does not mutate entry).
    """
    enriched = dict(entry)
    our_side = entry.get("side", "no").lower()
    result   = settlement.get("market_result", "").lower()
    fee_total      = float(settlement.get("fee_cost") or 0)
    settled_time   = settlement.get("settled_time") or settlement.get("updated_time")

    # Fills for this ticker — only buy-side fills matching our side
    buy_fills = [
        f for f in fills
        if f.get("action") == "buy" and f.get("side", "").lower() == our_side
    ]
    contracts_filled_total = int(sum(float(f.get("count_fp") or 0) for f in buy_fills))

    if buy_fills and contracts_filled_total > 0:
        cost_total = round(sum(
            (float(f.get("yes_price_dollars") or 0) if our_side == "yes"
             else (1.0 - float(f.get("yes_price_dollars") or 0)))
            * float(f.get("count_fp") or 0)
            for f in buy_fills
        ), 4)
        avg_entry_from_fills = round(cost_total / contracts_filled_total, 4)
    else:
        contracts_filled_total = total_contracts_for_ticker
        cost_total             = round(entry.get("entry_price", 0) * total_contracts_for_ticker, 4)
        avg_entry_from_fills   = entry.get("entry_price", 0)

    # Split settlement PnL and fee proportionally across log entries
    weight   = entry_contracts / total_contracts_for_ticker if total_contracts_for_ticker > 0 else 1.0
    won      = (result == our_side)
    net_pnl_total = round(contracts_filled_total * 1.0 - cost_total - fee_total, 4) if won \
                    else round(-cost_total - fee_total, 4)

    net_pnl_entry = round(net_pnl_total * weight, 4)
    fee_entry     = round(fee_total     * weight, 4)

    enriched.update({
        "settlement_source":         "kalshi",
        "won":                       won,
        "net_pnl":                   net_pnl_entry,
        "fee":                       fee_entry,
        "settled_time":              settled_time,
        "market_result":             result,
        "contracts_filled":          contracts_filled_total,
        "avg_entry_from_fills":      avg_entry_from_fills,
        "final_no_price":            None,
        # Metadata for auditing
        "position_net_pnl_total":    net_pnl_total,
        "position_contracts_total":  total_contracts_for_ticker,
        "entry_weight":              round(weight, 4),
    })
    return enriched


def build_obs_index(obs_csv: str | None) -> dict[str, float]:
    """Return {ticker: final_no_price} from the observation CSV."""
    if not obs_csv or not Path(obs_csv).exists():
        return {}
    try:
        import pandas as pd
        df = pd.read_csv(obs_csv, low_memory=False, usecols=["ticker", "no_price"])
        last = df.groupby("ticker")["no_price"].last()
        return last.to_dict()
    except Exception as e:
        print(f"  [WARN] obs CSV read failed: {e}")
        return {}


def _enrich_from_obs(entry: dict, final_no: float) -> dict:
    """Enrich from observation CSV only — Kalshi data not available."""
    enriched = dict(entry)
    won  = final_no >= 0.95 if final_no is not None else None
    lost = final_no <= 0.05 if final_no is not None else None

    if won:
        contracts = entry.get("contracts", 1)
        cost      = round(entry.get("entry_price", 0) * contracts, 4)
        net_pnl   = round(contracts - cost, 4)
    elif lost:
        contracts = entry.get("contracts", 1)
        cost      = round(entry.get("entry_price", 0) * contracts, 4)
        net_pnl   = round(-cost, 4)
    else:
        net_pnl = None
        won     = None

    enriched.update({
        "settlement_source":    "observation_csv",
        "won":                  won,
        "net_pnl":              net_pnl,
        "fee":                  None,
        "settled_time":         None,
        "market_result":        None,
        "contracts_filled":     entry.get("contracts", 1),
        "avg_entry_from_fills": entry.get("entry_price"),
        "final_no_price":       final_no,
    })
    return enriched


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def enrich(
    trade_log_path: str = "data/trade_log.json",
    obs_csv_path:   str | None = "data/lowt_observations.csv",
    out_path:       str = "data/enriched_trade_log.json",
) -> None:
    # Load trade log
    log_path = Path(trade_log_path)
    if not log_path.exists():
        print(f"ERROR: trade log not found at {log_path}")
        sys.exit(1)
    entries: list[dict] = json.loads(log_path.read_text())
    print(f"Loaded {len(entries)} entries from {log_path}")

    # Connect to Kalshi
    print("\nConnecting to Kalshi...")
    try:
        client = _make_client()
        print("  OK\n")
    except Exception as e:
        print(f"  [ERROR] could not connect: {e}")
        print("  Falling back to observation CSV only.")
        client = None

    # Fetch Kalshi data
    settlements_idx: dict[str, dict]       = {}
    fills_idx:       dict[str, list[dict]] = {}

    if client:
        print("Fetching settlements...")
        settlements_idx = build_settlement_index(fetch_all_settlements(client))
        print(f"\nFetching fills...")
        fills_idx = build_fills_index(fetch_all_fills(client))

    # Load observation CSV fallback
    print(f"\nLoading observation CSV fallback ({obs_csv_path})...")
    obs_idx = build_obs_index(obs_csv_path)
    print(f"  {len(obs_idx)} tickers in obs CSV")

    # Pre-compute total log contracts per ticker so PnL can be split correctly
    # when multiple entries share the same ticker (engine entered same bracket
    # on successive polls — one Kalshi settlement covers all of them).
    from collections import defaultdict
    ticker_total_contracts: dict[str, int] = defaultdict(int)
    for entry in entries:
        ticker_total_contracts[entry.get("ticker", "")] += (entry.get("contracts") or 1)

    # Enrich each entry
    enriched_entries = []
    stats = {"kalshi": 0, "obs": 0, "unresolved": 0}

    for entry in entries:
        ticker          = entry.get("ticker", "")
        entry_contracts = entry.get("contracts") or 1
        total_contracts = ticker_total_contracts.get(ticker, entry_contracts)

        if ticker in settlements_idx:
            fills = fills_idx.get(ticker, [])
            enriched = _enrich_from_kalshi(
                entry, settlements_idx[ticker], fills,
                entry_contracts, total_contracts,
            )
            stats["kalshi"] += 1

        elif ticker in obs_idx:
            enriched = _enrich_from_obs(entry, obs_idx[ticker])
            stats["obs"] += 1

        else:
            enriched = dict(entry)
            enriched.update({
                "settlement_source":         "unresolved",
                "won":                       None,
                "net_pnl":                   None,
                "fee":                       None,
                "settled_time":              None,
                "market_result":             None,
                "contracts_filled":          entry_contracts,
                "avg_entry_from_fills":      entry.get("entry_price"),
                "final_no_price":            None,
                "position_net_pnl_total":    None,
                "position_contracts_total":  total_contracts,
                "entry_weight":              round(entry_contracts / total_contracts, 4),
            })
            stats["unresolved"] += 1

        enriched_entries.append(enriched)

    # Summary
    print(f"\nEnrichment summary:")
    print(f"  Kalshi settlements:  {stats['kalshi']}")
    print(f"  Observation CSV:     {stats['obs']}")
    print(f"  Unresolved:          {stats['unresolved']}")

    resolved = [e for e in enriched_entries if e.get("won") is not None]
    wins     = [e for e in resolved if e["won"]]
    losses   = [e for e in resolved if not e["won"]]
    total_pnl = sum(e.get("net_pnl") or 0 for e in resolved)

    print(f"\n  Resolved: {len(resolved)} / {len(enriched_entries)}")
    print(f"  Win rate: {len(wins)/len(resolved):.1%} ({len(wins)} W / {len(losses)} L)")
    print(f"  Net PnL:  ${total_pnl:+.2f}")

    # Write output
    out = Path(out_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(enriched_entries, indent=2))
    print(f"\nSaved to {out}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Enrich trade_log.json with Kalshi settlement data")
    parser.add_argument("--trade-log", default="data/trade_log.json",
                        help="Path to trade_log.json (default: data/trade_log.json)")
    parser.add_argument("--obs-csv",   default="data/lowt_observations.csv",
                        help="Path to observation CSV fallback")
    parser.add_argument("--out",       default="data/enriched_trade_log.json",
                        help="Output path (default: data/enriched_trade_log.json)")
    args = parser.parse_args()

    enrich(
        trade_log_path = args.trade_log,
        obs_csv_path   = args.obs_csv,
        out_path       = args.out,
    )
