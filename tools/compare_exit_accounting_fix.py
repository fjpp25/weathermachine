#!/usr/bin/env python3
"""
tools/compare_exit_accounting_fix.py — diagnostic only. READ-ONLY (GET
requests only, never places or cancels an order).

WHY THIS EXISTS
----------------
dashboard.py's _fetch_settlements() had a bug: any ticker with a detected
buy+sell pair in fills had its ENTIRE settlement record deleted from the
win/loss list (etickers exclusion), on the wrong assumption that a visible
sell fill meant the whole position was closed early. Verified concretely
against KXHIGHTATL-26JUN26-B93.5: only 5 of its buy contracts were visible
in fills, but its settlement recorded 8 held at close — the position was
TRIMMED by 3, not closed, and the remaining 8 rode to settlement and WON.
The old code hid that entire win, showing only the -$0.04 trim.

This script pulls the same live settlements + fills dashboard.py uses, and
computes stats TWICE from the same data — once with the OLD exclusion logic,
once with the NEW (fixed) logic — so the actual historical size of what was
being hidden is visible directly, rather than inferred from a single ticker
or compared across two separately-timed dashboard snapshots.

Mirrors dashboard.py's _fetch_settlements() logic exactly (duplicated here
rather than imported, since dashboard.py is a Flask app module not designed
to be imported as a library). If dashboard.py's logic changes again in the
future, re-diff this script's OLD/NEW builders against it before trusting
this comparison.

USAGE (repo root, on the Pi):
    python3 tools/compare_exit_accounting_fix.py
"""
from __future__ import annotations

import json
import os
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def load_credentials():
    """Mirrors fetch_settlements.py's load_credentials()."""
    config_file = Path("data/config.json")
    if config_file.exists():
        config = json.loads(config_file.read_text())
        if config.get("key_id"):
            os.environ.setdefault("KALSHI_KEY_ID", config["key_id"])
        if config.get("key_file"):
            os.environ.setdefault("KALSHI_KEY_FILE", config["key_file"])
        os.environ["KALSHI_DEMO"] = "false" if config.get("live_mode") else "true"
        return
    env_file = Path(".env")
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            if "=" in line and not line.startswith("#"):
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())


def fetch_all_settlements(client) -> list:
    all_s, cursor = [], None
    for _ in range(15):
        p = {"limit": 200}
        if cursor:
            p["cursor"] = cursor
        d = client.get("portfolio/settlements", params=p)
        b = d.get("settlements", [])
        all_s.extend(b)
        cursor = d.get("cursor")
        if not cursor or len(b) < 200:
            break
    return all_s


def fetch_all_fills(client) -> list:
    all_f, cursor = [], None
    for _ in range(15):
        p = {"limit": 200}
        if cursor:
            p["cursor"] = cursor
        d = client.get("portfolio/fills", params=p)
        b = d.get("fills", [])
        all_f.extend(b)
        cursor = d.get("cursor")
        if not cursor or len(b) < 200:
            break
    return all_f


def is_temp_ticker(t: str) -> bool:
    return t.startswith("KX") and ("HIGH" in t or "LOWT" in t) and "TEMPNYCH" not in t


def build_exits(fbt: dict, sdates: dict) -> list:
    """Identical to dashboard.py's early-exit detection — this part of the
    logic was NOT the bug and is unchanged here."""
    exits = []
    for ticker, fills in fbt.items():
        buys = [f for f in fills if f.get("action") == "buy"]
        sells = [f for f in fills if f.get("action") == "sell"]
        if not buys or not sells:
            continue
        st = sdates.get(ticker, "")
        esells = [f for f in sells if not st or f.get("created_time", "") < st]
        if not esells:
            continue
        sides = [f.get("side") for f in buys]
        our = max(set(sides), key=sides.count)
        obuys = [f for f in buys if f.get("side") == our]
        oes = [f for f in esells if f.get("side") == our]
        if not oes:
            leg = "yes" if our == "no" else "no"
            oes = [f for f in esells if f.get("side") == leg]
        if not oes:
            continue

        def fp(f):
            yp = float(f.get("yes_price_dollars") or 0)
            return yp if our == "yes" else 1 - yp

        bc = sum(float(f.get("count_fp") or 0) for f in obuys)
        sc = sum(float(f.get("count_fp") or 0) for f in oes)
        if bc == 0:
            continue
        ab = sum(fp(f) * float(f.get("count_fp") or 0) for f in obuys) / bc
        ae = sum(fp(f) * float(f.get("count_fp") or 0) for f in oes) / max(sc, 1)
        nc = int(min(bc, sc))
        fee = sum(float(f.get("fee_cost") or 0) for f in obuys + oes)
        date = sorted(obuys, key=lambda f: f.get("created_time", ""))[0].get("created_time", "")[:10]
        exits.append({
            "ticker": ticker, "date": date, "side": our.upper(),
            "contracts": nc, "avg_buy": round(ab, 4), "avg_sell": round(ae, 4),
            "fee": round(fee, 4), "net_pnl": round(ae * nc - ab * nc - fee, 4),
        })
    return exits


def build_settled_records(settlements: list) -> list:
    """Identical to dashboard.py's settlement-enrichment step."""
    records = []
    for s in settlements:
        res = s.get("market_result", "").lower()
        fee = float(s.get("fee_cost") or 0)
        yes_c = float(s.get("yes_count_fp") or 0)
        no_c = float(s.get("no_count_fp") or 0)
        yes_cost = float(s.get("yes_total_cost_dollars") or 0)
        no_cost = float(s.get("no_total_cost_dollars") or 0)

        if yes_c > 0 and no_c == 0:
            our, nc, cost = "yes", int(yes_c), round(yes_cost, 4)
        elif no_c > 0 and yes_c == 0:
            our, nc, cost = "no", int(no_c), round(no_cost, 4)
        elif yes_c > 0 and no_c > 0:
            if yes_c >= no_c:
                our, nc, cost = "yes", int(yes_c), round(yes_cost, 4)
            else:
                our, nc, cost = "no", int(no_c), round(no_cost, 4)
        else:
            continue
        if nc == 0 or cost == 0:
            continue

        won = (res == our)
        pnl = round(nc - cost - fee, 4) if won else round(-cost - fee, 4)
        records.append({
            "ticker": s.get("ticker", ""), "won": won, "net_pnl": pnl, "fee": fee,
        })
    return records


def stats_for(records: list) -> dict:
    wins = [r for r in records if r["won"]]
    losses = [r for r in records if not r["won"]]
    total = len(records)
    return {
        "total": total,
        "wins": len(wins),
        "losses": len(losses),
        "win_rate": round(len(wins) / total * 100, 1) if total else 0.0,
        "net_pnl": round(sum(r["net_pnl"] for r in records), 4),
        "fees": round(sum(r["fee"] for r in records), 4),
    }


def main():
    load_credentials()
    import trader
    client = trader.make_client(skip_confirmation=True)

    print("Fetching settlements...")
    all_s = fetch_all_settlements(client)
    temp = [s for s in all_s if is_temp_ticker(s.get("ticker", ""))]
    print(f"  {len(all_s)} total settlements, {len(temp)} temperature-market settlements")

    print("Fetching fills...")
    all_f = fetch_all_fills(client)
    fbt = defaultdict(list)
    for f in all_f:
        if is_temp_ticker(f.get("ticker", "")):
            fbt[f["ticker"]].append(f)
    print(f"  {len(all_f)} total fills, {sum(len(v) for v in fbt.values())} temperature-market fills")

    sdates = {s["ticker"]: s.get("settled_time", "") for s in all_s if s.get("ticker", "").startswith("KX")}
    exits = build_exits(fbt, sdates)
    etickers = {e["ticker"] for e in exits}
    print(f"  {len(exits)} tickers flagged as early exits")

    exit_records = [{"ticker": e["ticker"], "won": False, "net_pnl": e["net_pnl"], "fee": e["fee"]} for e in exits]

    # OLD (buggy): exclude any ticker in etickers from settlement accounting
    old_settled = [s for s in build_settled_records([s for s in temp if s["ticker"] not in etickers])]
    old_records = old_settled + exit_records

    # NEW (fixed): include every settlement, plus exits additionally
    new_settled = build_settled_records(temp)
    new_records = new_settled + exit_records

    old_stats = stats_for(old_records)
    new_stats = stats_for(new_records)

    # Which specific tickers were being hidden, and what they were worth
    hidden = [r for r in new_settled if r["ticker"] in etickers]

    print(f"\n{'='*72}")
    print(f"  BEFORE (old/buggy) vs AFTER (new/fixed)")
    print(f"{'='*72}")
    print(f"  {'Metric':<14} {'OLD':>12} {'NEW':>12} {'DELTA':>12}")
    print(f"  {'-'*50}")
    for key, label in [("total", "Total"), ("wins", "Wins"), ("losses", "Losses"),
                        ("win_rate", "Win Rate %"), ("net_pnl", "Net PnL $"), ("fees", "Fees $")]:
        o, n = old_stats[key], new_stats[key]
        d = round(n - o, 4) if isinstance(o, float) else n - o
        print(f"  {label:<14} {o:>12} {n:>12} {d:>+12}")

    print(f"\n  {len(hidden)} previously-hidden settlement(s) now counted:")
    hidden_wins = [h for h in hidden if h["won"]]
    hidden_losses = [h for h in hidden if not h["won"]]
    print(f"    {len(hidden_wins)} wins, {len(hidden_losses)} losses")
    print(f"    Total PnL from previously-hidden settlements: "
          f"${sum(h['net_pnl'] for h in hidden):+.4f}")
    print(f"\n  Individual hidden records:")
    for h in sorted(hidden, key=lambda x: x["net_pnl"]):
        print(f"    {h['ticker']:<32} {'WON ' if h['won'] else 'LOST'}  ${h['net_pnl']:+.4f}")


if __name__ == "__main__":
    main()
