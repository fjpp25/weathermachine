"""
settlement_audit.py
-------------------
Fetches complete Kalshi position history for temperature markets and
produces a full breakdown across four states:

  SETTLED_WIN   — held to settlement, market resolved in our favour
  SETTLED_LOSS  — held to settlement, market resolved against us
  EARLY_EXIT    — sold before settlement (stop loss / anchor / ceiling)
                  sub-labelled WOULD_HAVE_WON or WOULD_HAVE_LOST once settled
  OPEN          — position still live (unrealised PnL shown)
  EXITED_TODAY  — sold today, market not yet settled (outcome pending)

Sources:
  portfolio/settlements  — fully settled positions (historical)
  portfolio/fills        — all buy/sell fills (used for OPEN + EXITED_TODAY)
  portfolio/positions    — current open positions (for current price / PnL)

Usage:
  python settlement_audit.py              # all temperature markets
  python settlement_audit.py --city Denver
  python settlement_audit.py --csv        # also write data/settlement_audit.csv
"""

import os
import json
import csv
import argparse
import requests
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

# ---------------------------------------------------------------------------
# Bootstrap
# ---------------------------------------------------------------------------

def _load_credentials():
    config_file = Path("data/config.json")
    if config_file.exists():
        try:
            config = json.loads(config_file.read_text())
            if config.get("key_id"):
                os.environ.setdefault("KALSHI_KEY_ID", config["key_id"])
            if config.get("key_file"):
                os.environ.setdefault("KALSHI_KEY_FILE", config["key_file"])
            os.environ["KALSHI_DEMO"] = "false" if config.get("live_mode") else "true"
            return
        except Exception:
            pass
    env_file = Path(".env")
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            if "=" in line and not line.startswith("#"):
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

DISPLAY_TZ = ZoneInfo("Europe/Lisbon")

# City → IANA timezone (mirrors cities.py, kept local to avoid import dependency)
_CITY_TZ: dict[str, str] = {
    "Atlanta":       "America/New_York",
    "Austin":        "America/Chicago",
    "Boston":        "America/New_York",
    "Chicago":       "America/Chicago",
    "Dallas":        "America/Chicago",
    "Denver":        "America/Denver",
    "Houston":       "America/Chicago",
    "Las Vegas":     "America/Los_Angeles",
    "Los Angeles":   "America/Los_Angeles",
    "Miami":         "America/New_York",
    "Minneapolis":   "America/Chicago",
    "New Orleans":   "America/Chicago",
    "New York":      "America/New_York",
    "Oklahoma City": "America/Chicago",
    "Philadelphia":  "America/New_York",
    "Phoenix":       "America/Phoenix",
    "San Antonio":   "America/Chicago",
    "San Francisco": "America/Los_Angeles",
    "Seattle":       "America/Los_Angeles",
    "Washington DC": "America/New_York",
}


def _entry_fields(buy_fills: list[dict], city: str) -> dict:
    """
    Extract entry_time and entry_local_hour from the earliest buy fill.

    Returns:
        entry_time       — ISO string in Lisbon time (for display), e.g. "2026-04-28 13:06"
        entry_time_utc   — raw UTC ISO string (for sorting/analysis)
        entry_local_hour — integer local hour in the city's timezone (for by-hour analysis)
    """
    if not buy_fills:
        return {"entry_time": "", "entry_time_utc": "", "entry_local_hour": None}

    earliest_raw = min(
        (f.get("created_time", "") for f in buy_fills),
        default="",
    )
    if not earliest_raw:
        return {"entry_time": "", "entry_time_utc": "", "entry_local_hour": None}

    try:
        dt_utc  = datetime.fromisoformat(earliest_raw.replace("Z", "+00:00"))
        dt_lisbon = dt_utc.astimezone(DISPLAY_TZ)
        city_tz   = ZoneInfo(_CITY_TZ.get(city, "America/New_York"))
        dt_local  = dt_utc.astimezone(city_tz)
        return {
            "entry_time":       dt_lisbon.strftime("%Y-%m-%d %H:%M"),
            "entry_time_utc":   dt_utc.isoformat(),
            "entry_local_hour": dt_local.hour,
        }
    except Exception:
        return {"entry_time": earliest_raw[:16], "entry_time_utc": earliest_raw,
                "entry_local_hour": None}


def _ts(raw) -> str:
    if not raw:
        return ""
    try:
        dt = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
        return dt.astimezone(DISPLAY_TZ).strftime("%Y-%m-%d %H:%M")
    except Exception:
        return str(raw)[:16]


def _fill_price(fill: dict, our_side: str) -> float:
    yp = float(fill.get("yes_price_dollars") or 0)
    return yp if our_side == "yes" else round(1.0 - yp, 4)


def _city_from_ticker(ticker: str) -> str:
    try:
        from cities import SERIES_TO_CITY as _S2C
        prefix = ticker.split("-")[0]
        return _S2C.get(prefix, prefix)
    except Exception:
        return ticker.split("-")[0]


def _bracket_label(ticker: str) -> str:
    parts = ticker.split("-")
    return parts[-1] if parts else ticker


def _today_local() -> str:
    return datetime.now(DISPLAY_TZ).strftime("%Y-%m-%d")


# ---------------------------------------------------------------------------
# Data fetchers
# ---------------------------------------------------------------------------

def fetch_all_settlements(client) -> list[dict]:
    all_s, cursor, page = [], None, 0
    while True:
        params = {"limit": 200}
        if cursor:
            params["cursor"] = cursor
        data   = client.get("portfolio/settlements", params=params)
        batch  = data.get("settlements", [])
        all_s.extend(batch)
        cursor = data.get("cursor")
        page  += 1
        print(f"  settlements page {page}: {len(batch)} rows  (total: {len(all_s)})")
        if not cursor or len(batch) < 200:
            break
    return all_s


def fetch_all_fills(client) -> list[dict]:
    all_f, cursor, page = [], None, 0
    while True:
        params = {"limit": 200}
        if cursor:
            params["cursor"] = cursor
        data   = client.get("portfolio/fills", params=params)
        batch  = data.get("fills", [])
        all_f.extend(batch)
        cursor = data.get("cursor")
        page  += 1
        print(f"  fills page {page}: {len(batch)} rows  (total: {len(all_f)})")
        if not cursor or len(batch) < 200:
            break
    return all_f


def fetch_open_positions(client) -> list[dict]:
    try:
        import trader
        return trader.sync_from_kalshi(client)
    except Exception as e:
        print(f"  [warn] Could not fetch open positions: {e}")
        return []


def fetch_market_prices(tickers: list[str]) -> dict[str, dict]:
    if not tickers:
        return {}
    prices = {}
    try:
        resp = requests.get(
            "https://api.elections.kalshi.com/trade-api/v2/markets",
            params={"tickers": ",".join(tickers)},
            timeout=15,
        ).json()
        for m in resp.get("markets", []):
            t       = m["ticker"]
            yes_bid = float(m.get("yes_bid_dollars") or 0)
            no_bid  = float(m.get("no_bid_dollars")  or 0)
            result  = (m.get("result") or "").lower()
            if yes_bid == 0 and no_bid == 0 and result:
                yes_bid = 0.99 if result == "yes" else 0.01
                no_bid  = 0.01 if result == "yes" else 0.99
            prices[t] = {
                "yes_bid": yes_bid,
                "no_bid":  no_bid,
                "result":  result,
                "status":  m.get("status", "active"),
            }
    except Exception as e:
        print(f"  [warn] Batch price fetch failed: {e}")
    return prices


# ---------------------------------------------------------------------------
# Core analysis
# ---------------------------------------------------------------------------

def analyse(
    settlements:    list[dict],
    fills:          list[dict],
    open_positions: list[dict],
    city_filter:    str = None,
) -> list[dict]:
    # Filter to temperature markets
    temp_s = [
        s for s in settlements
        if s.get("ticker", "").startswith("KX")
        and ("HIGH" in s.get("ticker", "") or "LOWT" in s.get("ticker", ""))
    ]
    if city_filter:
        temp_s = [
            s for s in temp_s
            if _city_from_ticker(s.get("ticker", "")).lower() == city_filter.lower()
        ]

    settled_tickers = {s["ticker"] for s in temp_s}
    settled_times   = {s["ticker"]: s.get("settled_time", "") for s in temp_s}
    settled_results = {s["ticker"]: s.get("market_result", "").lower() for s in temp_s}

    # Index fills by ticker
    fills_by_ticker: dict[str, list] = defaultdict(list)
    for f in fills:
        t = f.get("ticker", "")
        if t.startswith("KX") and ("HIGH" in t or "LOWT" in t):
            if city_filter is None or _city_from_ticker(t).lower() == city_filter.lower():
                fills_by_ticker[t].append(f)

    open_by_ticker = {p["ticker"]: p for p in open_positions}
    records        = []
    today          = _today_local()

    # ── 1. Settled tickers ────────────────────────────────────────────────────
    for s in temp_s:
        ticker        = s.get("ticker", "")
        market_result = settled_results.get(ticker, "")
        settled_time  = settled_times.get(ticker, "")
        city          = _city_from_ticker(ticker)
        bracket       = _bracket_label(ticker)
        date          = settled_time[:10] if settled_time else ""

        all_f_t    = fills_by_ticker.get(ticker, [])
        buy_fills  = [f for f in all_f_t if f.get("action") == "buy"]
        sell_fills = [f for f in all_f_t if f.get("action") == "sell"]

        if not buy_fills:
            continue

        sides    = [f.get("side") for f in buy_fills]
        our_side = max(set(sides), key=sides.count)
        our_buys = [f for f in buy_fills if f.get("side") == our_side]

        buy_contracts = sum(float(f.get("count_fp") or 0) for f in our_buys)
        if buy_contracts == 0:
            continue

        avg_entry = sum(
            _fill_price(f, our_side) * float(f.get("count_fp") or 0)
            for f in our_buys
        ) / buy_contracts
        fee_total = sum(float(f.get("fee_cost") or 0) for f in all_f_t)

        early_sells = [
            f for f in sell_fills
            if f.get("side") == our_side
            and settled_time
            and f.get("created_time", "") < settled_time
        ]
        if not early_sells:
            legacy = "yes" if our_side == "no" else "no"
            early_sells = [
                f for f in sell_fills
                if f.get("side") == legacy
                and settled_time
                and f.get("created_time", "") < settled_time
            ]

        if early_sells:
            sell_contracts = sum(float(f.get("count_fp") or 0) for f in early_sells)
            avg_exit = sum(
                _fill_price(f, our_side) * float(f.get("count_fp") or 0)
                for f in early_sells
            ) / max(sell_contracts, 1)
            contracts      = int(min(buy_contracts, sell_contracts))
            net_pnl        = round(avg_exit * contracts - avg_entry * contracts - fee_total, 4)
            would_have_won = (market_result == our_side)
            foregone_pnl   = 0.0
            if would_have_won:
                foregone_pnl = round((contracts - avg_entry * contracts - fee_total) - net_pnl, 4)

            exit_time = max(early_sells, key=lambda f: f.get("created_time", "")).get("created_time", "")
            records.append({
                "date": date, "city": city, "ticker": ticker, "bracket": bracket,
                "side": our_side.upper(), "contracts": contracts,
                "entry_price": round(avg_entry, 4), "exit_price": round(avg_exit, 4),
                "exit_time": _ts(exit_time), "exit_type": "EARLY_EXIT",
                "market_result": market_result.upper(),
                "outcome": "WOULD_HAVE_WON" if would_have_won else "WOULD_HAVE_LOST",
                "net_pnl": net_pnl, "foregone_pnl": foregone_pnl, "unrealised_pnl": None,
                **_entry_fields(our_buys, city),
            })
        else:
            contracts = int(buy_contracts)
            won       = (market_result == our_side)
            net_pnl   = round(contracts - avg_entry * contracts - fee_total, 4) if won else round(-avg_entry * contracts - fee_total, 4)
            records.append({
                "date": date, "city": city, "ticker": ticker, "bracket": bracket,
                "side": our_side.upper(), "contracts": contracts,
                "entry_price": round(avg_entry, 4), "exit_price": None,
                "exit_time": "", "exit_type": "SETTLEMENT",
                "market_result": market_result.upper(),
                "outcome": "SETTLED_WIN" if won else "SETTLED_LOSS",
                "net_pnl": net_pnl, "foregone_pnl": 0.0, "unrealised_pnl": None,
                **_entry_fields(our_buys, city),
            })

    # ── 2. Unsettled tickers — open or exited today ────────────────────────────
    unsettled = {t for t in fills_by_ticker if t not in settled_tickers}
    prices    = fetch_market_prices(list(unsettled))

    for ticker in unsettled:
        city    = _city_from_ticker(ticker)
        bracket = _bracket_label(ticker)

        all_f_t    = fills_by_ticker[ticker]
        buy_fills  = [f for f in all_f_t if f.get("action") == "buy"]
        sell_fills = [f for f in all_f_t if f.get("action") == "sell"]

        if not buy_fills:
            continue

        sides    = [f.get("side") for f in buy_fills]
        our_side = max(set(sides), key=sides.count)
        our_buys = [f for f in buy_fills if f.get("side") == our_side]

        buy_contracts = sum(float(f.get("count_fp") or 0) for f in our_buys)
        if buy_contracts == 0:
            continue

        avg_entry = sum(
            _fill_price(f, our_side) * float(f.get("count_fp") or 0)
            for f in our_buys
        ) / buy_contracts
        fee_total = sum(float(f.get("fee_cost") or 0) for f in all_f_t)

        # Entry date
        date = today
        earliest_ts = min((f.get("created_time", "") for f in our_buys), default="")
        if earliest_ts:
            try:
                dt   = datetime.fromisoformat(earliest_ts.replace("Z", "+00:00"))
                date = dt.astimezone(DISPLAY_TZ).strftime("%Y-%m-%d")
            except Exception:
                pass

        our_sells      = [f for f in sell_fills if f.get("side") == our_side]
        if not our_sells:
            legacy    = "yes" if our_side == "no" else "no"
            our_sells = [f for f in sell_fills if f.get("side") == legacy]

        sell_contracts = sum(float(f.get("count_fp") or 0) for f in our_sells)
        net_held       = buy_contracts - sell_contracts

        if net_held > 0.5:
            # Still open
            p             = open_by_ticker.get(ticker, {})
            unrealised    = p.get("unrealised_pnl") if p else None
            if unrealised is None:
                mkt = prices.get(ticker, {})
                cur = mkt.get("yes_bid" if our_side == "yes" else "no_bid", 0)
                unrealised = round((cur - avg_entry) * net_held, 4) if cur else None

            records.append({
                "date": date, "city": city, "ticker": ticker, "bracket": bracket,
                "side": our_side.upper(), "contracts": int(net_held),
                "entry_price": round(avg_entry, 4), "exit_price": None,
                "exit_time": "", "exit_type": "OPEN",
                "market_result": "", "outcome": "OPEN",
                "net_pnl": 0.0, "foregone_pnl": 0.0, "unrealised_pnl": unrealised,
                **_entry_fields(our_buys, city),
            })

        elif sell_contracts >= buy_contracts * 0.9:
            # Fully exited, not yet settled
            avg_exit  = sum(
                _fill_price(f, our_side) * float(f.get("count_fp") or 0)
                for f in our_sells
            ) / max(sell_contracts, 1)
            contracts = int(min(buy_contracts, sell_contracts))
            net_pnl   = round(avg_exit * contracts - avg_entry * contracts - fee_total, 4)
            exit_time = max(our_sells, key=lambda f: f.get("created_time", "")).get("created_time", "")

            records.append({
                "date": date, "city": city, "ticker": ticker, "bracket": bracket,
                "side": our_side.upper(), "contracts": contracts,
                "entry_price": round(avg_entry, 4), "exit_price": round(avg_exit, 4),
                "exit_time": _ts(exit_time), "exit_type": "EARLY_EXIT",
                "market_result": "", "outcome": "EXITED_TODAY",
                "net_pnl": net_pnl, "foregone_pnl": 0.0, "unrealised_pnl": None,
                **_entry_fields(our_buys, city),
            })

    return sorted(records, key=lambda r: (r["date"], r["city"]), reverse=True)


# ---------------------------------------------------------------------------
# Display
# ---------------------------------------------------------------------------

OUTCOME_COLOURS = {
    "SETTLED_WIN":    "\033[92m",
    "SETTLED_LOSS":   "\033[91m",
    "WOULD_HAVE_WON": "\033[93m",
    "WOULD_HAVE_LOST":"\033[90m",
    "OPEN":           "\033[96m",
    "EXITED_TODAY":   "\033[94m",
}
RESET = "\033[0m"


def _display_by_hour(records: list[dict]):
    """
    Print a breakdown of settled outcomes (+ would-have counterfactuals)
    grouped by local entry hour. Excludes OPEN and EXITED_TODAY rows.
    Rows with no entry_local_hour (pre-fill-API data) are grouped as '?'.
    """
    from collections import defaultdict

    buckets: dict = defaultdict(list)
    for r in records:
        if r["outcome"] in ("OPEN", "EXITED_TODAY"):
            continue
        h = r.get("entry_local_hour")
        buckets[h].append(r)

    print(f"\n  ── BY LOCAL ENTRY HOUR ──")
    print(f"  {'Hour':>6}  {'SW':>4}  {'SL':>4}  {'WHW':>5}  {'WHL':>5}"
          f"  {'WR(S)':>7}  {'PnL(S)':>8}  {'PnL(CF)':>9}")
    print(f"  {'-'*62}")

    for h in sorted(buckets.keys(), key=lambda x: (x is None, x)):
        sub  = buckets[h]
        sw   = sum(1 for r in sub if r["outcome"] == "SETTLED_WIN")
        sl   = sum(1 for r in sub if r["outcome"] == "SETTLED_LOSS")
        whw  = sum(1 for r in sub if r["outcome"] == "WOULD_HAVE_WON")
        whl  = sum(1 for r in sub if r["outcome"] == "WOULD_HAVE_LOST")
        s    = sw + sl
        wr   = f"{sw/s*100:.1f}%" if s else "—"
        # PnL (settled only)
        pnl_s  = sum(r["net_pnl"] for r in sub if r["outcome"] in ("SETTLED_WIN", "SETTLED_LOSS"))
        # PnL counterfactual (settled + would-haves as if held to settlement)
        pnl_cf = sum(r["net_pnl"] for r in sub)
        label  = f"{h:>4}h" if h is not None else "   ?"
        print(f"  {label:>6}  {sw:>4}  {sl:>4}  {whw:>5}  {whl:>5}"
              f"  {wr:>7}  ${pnl_s:>+7.2f}  ${pnl_cf:>+8.2f}")

    # Before/after 13:00 split
    print(f"\n  ── BEFORE vs AFTER 13:00 LOCAL (settled only) ──")
    for label, fn in [("Before 13:00", lambda h: h is not None and h < 13),
                      ("13:00+",       lambda h: h is not None and h >= 13),
                      ("No timestamp", lambda h: h is None)]:
        sub = [r for r in records
               if fn(r.get("entry_local_hour"))
               and r["outcome"] in ("SETTLED_WIN", "SETTLED_LOSS")]
        if not sub:
            continue
        sw  = sum(1 for r in sub if r["outcome"] == "SETTLED_WIN")
        sl  = len(sub) - sw
        wr  = f"{sw/len(sub)*100:.1f}%"
        pnl = sum(r["net_pnl"] for r in sub)
        print(f"  {label:<16}  {len(sub):>3} settled  ({sw}W/{sl}L)  WR: {wr}  PnL: ${pnl:+.2f}")


def display(records: list[dict], by_hour: bool = False):
    if not records:
        print("  No temperature market positions found.")
        return

    sw    = [r for r in records if r["outcome"] == "SETTLED_WIN"]
    sl    = [r for r in records if r["outcome"] == "SETTLED_LOSS"]
    whw   = [r for r in records if r["outcome"] == "WOULD_HAVE_WON"]
    whl   = [r for r in records if r["outcome"] == "WOULD_HAVE_LOST"]
    open_ = [r for r in records if r["outcome"] == "OPEN"]
    et    = [r for r in records if r["outcome"] == "EXITED_TODAY"]

    realised   = round(sum(r["net_pnl"] for r in records if r["outcome"] != "OPEN"), 2)
    unrealised = round(sum(r["unrealised_pnl"] or 0 for r in records if r["outcome"] == "OPEN"), 2)
    foregone   = round(sum(r["foregone_pnl"] for r in records), 2)
    wr_denom   = len(sw) + len(sl)
    wr         = f"{100*len(sw)//wr_denom}%" if wr_denom else "—"

    print(f"\n{'='*84}")
    print(f"  FULL POSITION AUDIT  —  {len(records)} temperature market positions")
    print(f"{'='*84}")
    print(f"  Settled wins / losses   : {len(sw)} / {len(sl)}  (WR: {wr})")
    print(f"  Early exits (historical): {len(whw)+len(whl)} total"
          f"  →  {len(whw)} would-have-won  /  {len(whl)} would-have-lost")
    print(f"  Exited today (pending)  : {len(et)}")
    print(f"  Still open              : {len(open_)}")
    print(f"  Realised PnL            : ${realised:+.2f}")
    print(f"  Unrealised PnL          : ${unrealised:+.2f}  (open positions at current price)")
    if foregone > 0:
        print(f"  Foregone (WHW exits)    : ${foregone:+.2f}")
    print(f"{'='*84}")

    print(f"\n  {'Date':<12} {'Entered':>11} {'City':<14} {'Bracket':<10} {'Side':>4}"
          f"  {'Entry':>6}  {'Exit/Unreal':>11}  {'PnL':>8}  Outcome")
    print(f"  {'-'*96}")

    for r in records:
        col = OUTCOME_COLOURS.get(r["outcome"], "")
        if r["outcome"] == "OPEN":
            exit_str = f"{r['unrealised_pnl']:+.2f}" if r["unrealised_pnl"] is not None else "open"
            pnl_str  = f"({exit_str})"
        else:
            exit_str = f"${r['exit_price']:.2f}" if r["exit_price"] is not None else "—"
            pnl_str  = f"${r['net_pnl']:>+.2f}"

        fg_str   = f"  [+${r['foregone_pnl']:.2f} foregone]" if r["foregone_pnl"] > 0 else ""
        ent_time = r.get("entry_time", "")[-5:] or "—"   # show HH:MM only; date already in col 1

        print(
            f"  {r['date']:<12} {ent_time:>11} {r['city']:<14} {r['bracket']:<10} {r['side']:>4}"
            f"  ${r['entry_price']:.2f}  {exit_str:>11}  {pnl_str:>8}"
            f"  {col}{r['outcome']}{RESET}{fg_str}"
        )

    if whw:
        print(f"\n  ── EARLY EXITS THAT WOULD HAVE WON ──")
        for r in sorted(whw, key=lambda x: x["foregone_pnl"], reverse=True):
            print(f"  {r['date']}  {r['city']:<14} {r['bracket']:<8}"
                  f"  exit@${r['exit_price']:.2f} → settled {r['market_result']}"
                  f"  foregone: +${r['foregone_pnl']:.2f}")
        print(f"  Total foregone: ${sum(r['foregone_pnl'] for r in whw):.2f}")

    if et:
        print(f"\n  ── EXITED TODAY (outcome pending) ──")
        for r in sorted(et, key=lambda x: x["net_pnl"]):
            print(f"  {r['city']:<14} {r['bracket']:<8}"
                  f"  entry@${r['entry_price']:.2f}  exit@${r['exit_price']:.2f}"
                  f"  PnL: ${r['net_pnl']:+.2f}  at: {r['exit_time']}")

    if by_hour:
        _display_by_hour(records)

    print()


# ---------------------------------------------------------------------------
# CSV export
# ---------------------------------------------------------------------------

CSV_FIELDS = [
    "date", "city", "ticker", "bracket", "side", "contracts",
    "entry_time", "entry_time_utc", "entry_local_hour",
    "entry_price", "exit_price", "exit_time", "exit_type",
    "market_result", "outcome", "net_pnl", "foregone_pnl", "unrealised_pnl",
]

def write_csv(records: list[dict], path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(records)
    print(f"  Saved → {path}")


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Full position audit for Kalshi temperature markets"
    )
    parser.add_argument("--city", type=str, default=None,
                        help="Filter to one city (e.g. 'Denver')")
    parser.add_argument("--csv",  action="store_true",
                        help="Write results to data/settlement_audit.csv")
    parser.add_argument("--by-hour", action="store_true",
                        help="Print win rate and PnL breakdown by local entry hour")
    args = parser.parse_args()

    _load_credentials()

    import trader
    client = trader.make_client(skip_confirmation=True)

    print("\nFetching settlements...")
    settlements = fetch_all_settlements(client)
    print(f"  {len(settlements)} settlements.\n")

    print("Fetching fills...")
    fills = fetch_all_fills(client)
    print(f"  {len(fills)} fills.\n")

    print("Fetching open positions...")
    open_positions = fetch_open_positions(client)
    print(f"  {len(open_positions)} open positions.\n")

    records = analyse(settlements, fills, open_positions, city_filter=args.city)
    display(records, by_hour=args.by_hour)

    if args.csv:
        write_csv(records, Path("data/settlement_audit.csv"))
