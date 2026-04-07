"""
kalshi_scanner.py
-----------------
Fetches live Kalshi market data for temperature markets across 8 cities.

For each city's today market, we fetch:
  - All brackets + their yes/no prices
  - Orderbook depth per bracket (liquidity check)
  - 1-minute candlestick history for today (price momentum)
  - Market close time (for lowest temp markets only)

Auth:
  - Market data (prices, orderbook, candlesticks) is PUBLIC — no key needed
  - Trading endpoints require auth — handled in a later module

Usage:
  python kalshi_scanner.py                   # snapshot all cities
  python kalshi_scanner.py --city Miami      # single city
  python kalshi_scanner.py --watch 120       # poll every 2 minutes
  python kalshi_scanner.py --raw             # dump raw JSON for debugging
"""

import json
import time
import argparse
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from pathlib import Path
from decimal import Decimal

try:
    import requests
except ImportError:
    raise SystemExit("Please install requests:  pip install requests")

# ---------------------------------------------------------------------------
# Series tickers: maps our city names to Kalshi series tickers
# Each series has one market per day (today's event)
# ---------------------------------------------------------------------------
CITY_SERIES = {
    "New York":      {"high": "KXHIGHNY",    "low": "KXLOWTNYC"},
    "Chicago":       {"high": "KXHIGHCHI",   "low": "KXLOWTCHI"},
    "Miami":         {"high": "KXHIGHMIA",   "low": "KXLOWTMIA"},
    "Austin":        {"high": "KXHIGHAUS",   "low": "KXLOWTAUS"},
    "Los Angeles":   {"high": "KXHIGHLAX",   "low": "KXLOWTLAX"},
    "San Francisco": {"high": "KXHIGHTSFO",  "low": None},
    "Denver":        {"high": "KXHIGHDEN",   "low": "KXLOWTDEN"},
    "Philadelphia":  {"high": "KXHIGHPHIL",  "low": "KXLOWTPHIL"},
    "Atlanta":       {"high": "KXHIGHTATL",  "low": None},
    "Houston":       {"high": "KXHIGHTHOU",  "low": None},
    "Phoenix":       {"high": "KXHIGHTPHX",  "low": None},
    "Las Vegas":     {"high": "KXHIGHTLV",   "low": None},
}

API_BASE   = "https://api.elections.kalshi.com/trade-api/v2"
CANDLE_MIN = 60    # 1-hour candles for intraday history (60 = 1hr in minutes)
                   # switch to 1 for 1-minute candles (more data, more API calls)

# ---------------------------------------------------------------------------
# HTTP helper
# ---------------------------------------------------------------------------

def get(url: str, params: dict = None, timeout: int = 15) -> dict:
    resp = requests.get(url, params=params, timeout=timeout)
    resp.raise_for_status()
    return resp.json()


# ---------------------------------------------------------------------------
# Step 1: Find today's open market for a series
# ---------------------------------------------------------------------------

def get_todays_markets(series_ticker: str) -> list[dict]:
    """
    Fetch all currently open markets for a series, filtered to today only.
    Uses the event ticker date suffix to match today's date.
    Ticker format: KXHIGHNY-26MAR31-B74.5 → date portion is 26MAR31
    """
    data = get(
        f"{API_BASE}/markets",
        params={"series_ticker": series_ticker, "status": "open"}
    )
    markets = data.get("markets", [])

    # Build today's date suffix in Kalshi's format: e.g. "26APR01"
    today = datetime.now(timezone.utc)
    date_suffix = today.strftime("%y%b%d").upper()   # e.g. "26APR01"

    today_markets = [
        m for m in markets
        if date_suffix in m.get("event_ticker", "")
    ]

    # Fall back to all open markets if date filtering yields nothing
    # (handles timezone edge cases near midnight)
    return today_markets if today_markets else markets


# ---------------------------------------------------------------------------
# Step 2: Fetch orderbook for a market (gives us yes/no prices + depth)
# ---------------------------------------------------------------------------

def get_orderbook(market_ticker: str) -> dict:
    """
    Returns parsed orderbook with best bid/ask and depth for yes and no sides.

    Kalshi only returns bids. The implied ask for YES = $1.00 - best NO bid.
    """
    data   = get(f"{API_BASE}/markets/{market_ticker}/orderbook")
    ob     = data.get("orderbook_fp", {})

    yes_bids = ob.get("yes_dollars", [])   # list of [price_str, count_str], worst to best
    no_bids  = ob.get("no_dollars",  [])

    def parse_side(bids):
        if not bids:
            return {"best_bid": None, "depth_contracts": 0, "levels": []}
        parsed = [(Decimal(p), Decimal(c)) for p, c in bids]
        best   = parsed[-1][0]   # last = highest bid
        depth  = sum(c for _, c in parsed)
        return {
            "best_bid":        float(best),
            "depth_contracts": float(depth),
            "levels":          [(float(p), float(c)) for p, c in parsed],
        }

    yes_side = parse_side(yes_bids)
    no_side  = parse_side(no_bids)

    # Implied prices:
    #   YES ask = 1.00 - best NO bid  (what you pay to buy YES)
    #   NO ask  = 1.00 - best YES bid (what you pay to buy NO)
    yes_ask = round(1.0 - no_side["best_bid"],  2) if no_side["best_bid"]  is not None else None
    no_ask  = round(1.0 - yes_side["best_bid"], 2) if yes_side["best_bid"] is not None else None

    spread = round(yes_ask - yes_side["best_bid"], 2) if (yes_ask and yes_side["best_bid"]) else None

    return {
        "yes_bid":         yes_side["best_bid"],
        "yes_ask":         yes_ask,
        "no_bid":          no_side["best_bid"],
        "no_ask":          no_ask,
        "spread":          spread,
        "yes_depth":       yes_side["depth_contracts"],
        "no_depth":        no_side["depth_contracts"],
        "yes_levels":      yes_side["levels"],
        "no_levels":       no_side["levels"],
    }


# ---------------------------------------------------------------------------
# Step 3: Fetch today's 1-hour candlesticks for a market
# ---------------------------------------------------------------------------

def get_candles(series_ticker: str, market_ticker: str, period_minutes: int = CANDLE_MIN) -> list[dict]:
    """
    Fetch intraday candlesticks for a market.
    Returns list of candle dicts sorted oldest-first.

    Each candle has:
      ts, yes_bid_close, yes_ask_close, price_close, volume, open_interest
    """
    # Start from midnight UTC today (we want today's full price history)
    today_midnight = datetime.now(timezone.utc).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    start_ts = int(today_midnight.timestamp())
    end_ts   = int(datetime.now(timezone.utc).timestamp())

    try:
        data = get(
            f"{API_BASE}/series/{series_ticker}/markets/{market_ticker}/candlesticks",
            params={
                "start_ts":     start_ts,
                "end_ts":       end_ts,
                "period_minutes": period_minutes,
            }
        )
    except Exception as e:
        return []   # candlesticks may not be available for very new markets

    raw_candles = data.get("candlesticks", [])

    parsed = []
    for c in raw_candles:
        parsed.append({
            "ts":              c.get("end_period_ts"),
            "yes_bid_close":   safe_decimal(c.get("yes_bid", {}).get("close_dollars")),
            "yes_ask_close":   safe_decimal(c.get("yes_ask", {}).get("close_dollars")),
            "price_close":     safe_decimal(c.get("price",   {}).get("close_dollars")),
            "price_open":      safe_decimal(c.get("price",   {}).get("open_dollars")),
            "volume":          safe_decimal(c.get("volume_fp")),
            "open_interest":   safe_decimal(c.get("open_interest_fp")),
        })

    return sorted(parsed, key=lambda x: x["ts"] or 0)


def safe_decimal(val) -> float | None:
    try:
        return float(Decimal(val))
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Step 4: Assemble full market snapshot for one city
# ---------------------------------------------------------------------------

def scan_city(city: str, market_type: str = "high") -> dict:
    """
    Full scan for one city and market type ("high" or "low").
    Returns a structured dict with all brackets and their data.
    """
    series_ticker = CITY_SERIES.get(city, {}).get(market_type)
    if not series_ticker:
        return {"city": city, "type": market_type, "error": "No series ticker configured"}

    result = {
        "city":           city,
        "type":           market_type,
        "series_ticker":  series_ticker,
        "scanned_at_utc": datetime.now(timezone.utc).isoformat(),
        "brackets":       [],
        "error":          None,
    }

    try:
        markets = get_todays_markets(series_ticker)

        if not markets:
            result["error"] = "No open markets found for today"
            return result

        result["event_ticker"] = markets[0].get("event_ticker")
        result["close_time"]   = markets[0].get("close_time")   # relevant for LOW markets

        for market in markets:
            ticker = market["ticker"]
            title  = market.get("title", "")

            # Parse bracket range from market fields
            bracket = {
                "ticker":       ticker,
                "title":        title,
                "status":       market.get("status", "active"),
                "floor":        market.get("floor_strike"),
                "cap":          market.get("cap_strike"),
                "volume":       safe_decimal(market.get("volume_fp")),
                "open_interest":safe_decimal(market.get("open_interest_fp")),
                "yes_bid":      safe_decimal(market.get("yes_bid_dollars")),
                "yes_ask":      safe_decimal(market.get("yes_ask_dollars")),
                "close_time":   market.get("close_time"),
            }

            # Orderbook depth
            try:
                ob = get_orderbook(ticker)
                bracket.update({
                    "ob_yes_bid":   ob["yes_bid"],
                    "ob_yes_ask":   ob["yes_ask"],
                    "ob_no_bid":    ob["no_bid"],
                    "ob_no_ask":    ob["no_ask"],
                    "ob_spread":    ob["spread"],
                    "ob_yes_depth": ob["yes_depth"],
                    "ob_no_depth":  ob["no_depth"],
                })
            except Exception as e:
                bracket["ob_error"] = str(e)

            # Candlestick history
            try:
                candles = get_candles(series_ticker, ticker)
                bracket["candles"] = candles
                bracket["candle_count"] = len(candles)
            except Exception as e:
                bracket["candles"]      = []
                bracket["candle_count"] = 0
                bracket["candle_error"] = str(e)

            result["brackets"].append(bracket)
            time.sleep(0.15)   # gentle on the API

        # Sort brackets by floor price (ascending) for display
        result["brackets"].sort(key=lambda b: b.get("floor") or 0)

    except Exception as e:
        result["error"] = str(e)

    return result


# ---------------------------------------------------------------------------
# Scan all cities
# ---------------------------------------------------------------------------

def scan_all(city_filter: str = None, market_type: str = "high") -> dict:
    cities = list(CITY_SERIES.keys())
    if city_filter:
        cities = [c for c in cities if c.lower() == city_filter.lower()]

    results = {}
    for city in cities:
        print(f"  Scanning {city} ({market_type})...", end=" ", flush=True)
        result = scan_city(city, market_type)
        if result.get("error"):
            print(f"ERROR: {result['error']}")
        else:
            print(f"{len(result['brackets'])} brackets")
        results[city] = result
        time.sleep(0.2)

    return results


# ---------------------------------------------------------------------------
# Display
# ---------------------------------------------------------------------------

def display(results: dict):
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    print(f"\n{'='*75}")
    print(f"  Kalshi Scanner  —  {now_utc}")
    print(f"{'='*75}")

    for city, data in results.items():
        if data.get("error"):
            print(f"\n{city}: ERROR — {data['error']}")
            continue

        close_str = ""
        if data.get("close_time"):
            close_str = f"  closes: {data['close_time']}"

        print(f"\n{city}  [{data['series_ticker']}]{close_str}")
        print(f"  {'Bracket':<22} {'YesBid':>7} {'YesAsk':>7} {'NoBid':>7} {'NoAsk':>7} {'Spread':>7} {'Vol':>8} {'Candles':>8}")
        print(f"  {'-'*73}")

        for b in data["brackets"]:
            floor = b.get("floor")
            cap   = b.get("cap")
            if floor is not None and cap is not None:
                bracket_str = f"{floor}° – {cap}°F"
            elif floor is not None:
                bracket_str = f"{floor}°F or above"
            elif cap is not None:
                bracket_str = f"{cap}°F or below"
            else:
                bracket_str = b.get("title", "?")[:22]

            print(
                f"  {bracket_str:<22} "
                f"{fmt(b.get('ob_yes_bid')):>7} "
                f"{fmt(b.get('ob_yes_ask')):>7} "
                f"{fmt(b.get('ob_no_bid')):>7} "
                f"{fmt(b.get('ob_no_ask')):>7} "
                f"{fmt(b.get('ob_spread')):>7} "
                f"{fmt_vol(b.get('volume')):>8} "
                f"{b.get('candle_count', 0):>8}"
            )

    print(f"\n{'='*75}")
    print("  Prices in dollars ($0.01–$0.99)  Vol=contracts traded today  Candles=hourly bars")


def fmt(val) -> str:
    return f"${val:.2f}" if val is not None else "   N/A"

def fmt_vol(val) -> str:
    if val is None:
        return "N/A"
    if val >= 1000:
        return f"{val/1000:.1f}k"
    return f"{val:.0f}"


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kalshi temperature market scanner")
    parser.add_argument("--city",  type=str,   default=None,   help="Filter to one city")
    parser.add_argument("--type",  type=str,   default="high", choices=["high", "low"],
                        help="Market type: high (default) or low")
    parser.add_argument("--watch", type=int,   default=None,   metavar="SECONDS",
                        help="Poll repeatedly every N seconds")
    parser.add_argument("--raw",   action="store_true",
                        help="Dump raw JSON output instead of formatted table")
    args = parser.parse_args()

    def run():
        results = scan_all(args.city, args.type)
        if args.raw:
            print(json.dumps(results, indent=2, default=str))
        else:
            display(results)

    if args.watch:
        print(f"Watching — polling every {args.watch}s. Ctrl+C to stop.")
        while True:
            run()
            time.sleep(args.watch)
    else:
        run()
