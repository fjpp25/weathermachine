"""
trader.py
---------
Handles authenticated Kalshi API interactions:
  - Order execution (place YES or NO limit orders)
  - Position tracking (what we hold and at what price)
  - Exit monitoring (watch open positions, trigger exits)

Auth:
  Kalshi uses RSA-PSS signed requests. You need:
    - KALSHI_KEY_ID    : your API key ID (from Kalshi dashboard)
    - KALSHI_KEY_FILE  : path to your private key PEM file

  Set these via the app's Settings dialog, or as environment variables:
    KALSHI_KEY_ID=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
    KALSHI_KEY_FILE=kalshi_private_key.pem
    KALSHI_DEMO=true   # use demo environment (default: true for safety)

Usage:
  python trader.py --balance              # check account balance
  python trader.py --positions            # show open positions
  python trader.py --monitor              # start exit monitor loop
  python trader.py --run                  # full pipeline: signal + execute
  python trader.py --run --paper          # dry run (no real orders)

IMPORTANT: KALSHI_DEMO defaults to true. You must explicitly set
KALSHI_DEMO=false to trade real money.
"""

import os
import json
import time
import base64
import argparse
import uuid
from datetime import datetime, timezone
from pathlib import Path
from decimal import Decimal

try:
    import requests
    from cryptography.hazmat.primitives import serialization, hashes
    from cryptography.hazmat.primitives.asymmetric import padding
    from cryptography.hazmat.backends import default_backend
except ImportError:
    raise SystemExit(
        "Missing dependencies. Run:\n"
        "  pip install requests cryptography"
    )

import decision_engine

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DEMO_BASE_URL = "https://demo-api.kalshi.co/trade-api/v2"
PROD_BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"

# Max contracts per single order — hard safety cap
MAX_CONTRACTS_PER_ORDER = 10

# Default contracts per signal
BASE_CONTRACTS = 1

# Score-based position sizing multiplier
# Kept flat at 1x for now — with a small account, size consistency matters more than scaling
SCORE_SIZING = {1: 1.0, 2: 1.0, 3: 1.0}

# Exit monitor poll interval (seconds)
MONITOR_INTERVAL = 60


# ---------------------------------------------------------------------------
# Auth client
# ---------------------------------------------------------------------------

class KalshiClient:
    """
    Authenticated Kalshi REST client using RSA-PSS request signing.
    Handles all auth header generation transparently.
    """

    def __init__(
        self,
        key_id:      str,
        key_file:    str,
        demo:        bool = True,
    ):
        self.key_id   = key_id
        self.base_url = DEMO_BASE_URL if demo else PROD_BASE_URL
        self.demo     = demo

        with open(key_file, "rb") as f:
            self.private_key = serialization.load_pem_private_key(
                f.read(),
                password=None,
                backend=default_backend(),
            )

        env = "DEMO" if demo else "PRODUCTION"
        print(f"  KalshiClient ready [{env}]  key_id={key_id[:8]}...")

    def _sign(self, timestamp: str, method: str, path: str) -> str:
        """Create RSA-PSS SHA256 signature for a request."""
        path_no_query = path.split("?")[0]
        message       = f"{timestamp}{method}{path_no_query}".encode("utf-8")
        signature     = self.private_key.sign(
            message,
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.DIGEST_LENGTH,
            ),
            hashes.SHA256(),
        )
        return base64.b64encode(signature).decode("utf-8")

    def _headers(self, method: str, path: str) -> dict:
        ts = str(int(datetime.now(timezone.utc).timestamp() * 1000))
        return {
            "Content-Type":            "application/json",
            "KALSHI-ACCESS-KEY":       self.key_id,
            "KALSHI-ACCESS-TIMESTAMP": ts,
            "KALSHI-ACCESS-SIGNATURE": self._sign(ts, method, path),
        }

    def _api_path(self, endpoint: str) -> str:
        """Return just the path portion for signing."""
        return f"/trade-api/v2/{endpoint.lstrip('/')}"

    def _request_with_backoff(self, method: str, url: str,
                              headers: dict, timeout: int,
                              params: dict = None, json: dict = None) -> requests.Response:
        """
        Execute an HTTP request with exponential backoff retry.
        Retries on: connection errors, timeouts, 429 rate-limit, 5xx server errors.
        Does NOT retry on 4xx client errors (bad request, auth failure etc).
        """
        MAX_RETRIES = 4
        BASE_DELAY  = 2   # seconds

        for attempt in range(MAX_RETRIES):
            try:
                resp = requests.request(
                    method, url,
                    headers=headers,
                    params=params,
                    json=json,
                    timeout=timeout,
                )
                # Retry on rate-limit or server error
                if resp.status_code == 429 or resp.status_code >= 500:
                    if attempt < MAX_RETRIES - 1:
                        delay = BASE_DELAY * (2 ** attempt)
                        print(f"  [{resp.status_code}] Retrying in {delay}s "
                              f"(attempt {attempt + 1}/{MAX_RETRIES})...")
                        time.sleep(delay)
                        continue
                resp.raise_for_status()
                return resp

            except requests.exceptions.Timeout:
                if attempt < MAX_RETRIES - 1:
                    delay = BASE_DELAY * (2 ** attempt)
                    print(f"  [timeout] Retrying in {delay}s "
                          f"(attempt {attempt + 1}/{MAX_RETRIES})...")
                    time.sleep(delay)
                else:
                    raise

            except requests.exceptions.ConnectionError:
                if attempt < MAX_RETRIES - 1:
                    delay = BASE_DELAY * (2 ** attempt)
                    print(f"  [connection error] Retrying in {delay}s "
                          f"(attempt {attempt + 1}/{MAX_RETRIES})...")
                    time.sleep(delay)
                else:
                    raise

        raise RuntimeError("Max retries exceeded")

    def get(self, endpoint: str, params: dict = None) -> dict:
        path = self._api_path(endpoint)
        resp = self._request_with_backoff(
            "GET",
            self.base_url + "/" + endpoint.lstrip("/"),
            headers=self._headers("GET", path),
            timeout=15,
            params=params,
        )
        return resp.json()

    def post(self, endpoint: str, body: dict) -> dict:
        path = self._api_path(endpoint)
        resp = self._request_with_backoff(
            "POST",
            self.base_url + "/" + endpoint.lstrip("/"),
            headers=self._headers("POST", path),
            timeout=15,
            json=body,
        )
        return resp.json()

    def delete(self, endpoint: str) -> dict:
        path = self._api_path(endpoint)
        resp = self._request_with_backoff(
            "DELETE",
            self.base_url + "/" + endpoint.lstrip("/"),
            headers=self._headers("DELETE", path),
            timeout=15,
        )
        return resp.json()


def make_client(skip_confirmation: bool = False) -> KalshiClient:
    """Build KalshiClient from environment variables."""
    key_id   = os.environ.get("KALSHI_KEY_ID")
    key_file = os.environ.get("KALSHI_KEY_FILE")
    demo     = os.environ.get("KALSHI_DEMO", "true").lower() != "false"

    if not key_id:
        raise SystemExit(
            "KALSHI_KEY_ID environment variable not set.\n"
            "Export it or add to a .env file:\n"
            "  KALSHI_KEY_ID=your-key-id-here"
        )
    if not key_file or not Path(key_file).exists():
        raise SystemExit(
            f"KALSHI_KEY_FILE not set or file not found: {key_file}\n"
            "Export it:\n"
            "  KALSHI_KEY_FILE=path/to/kalshi_private_key.pem"
        )

    if not demo and not skip_confirmation:
        confirm = input(
            "\n  *** LIVE TRADING MODE — real money at risk ***\n"
            "  Type 'yes' to confirm: "
        ).strip().lower()
        if confirm != "yes":
            raise SystemExit("Aborted.")

    return KalshiClient(key_id=key_id, key_file=key_file, demo=demo)


# ---------------------------------------------------------------------------
# Account queries
# ---------------------------------------------------------------------------

def get_balance(client: KalshiClient) -> float:
    """Returns account balance in dollars."""
    data    = client.get("portfolio/balance")
    # balance is returned in cents
    balance = data.get("balance", 0)
    return balance / 100


def get_positions(client: KalshiClient) -> list[dict]:
    """Returns all open market positions from Kalshi."""
    data = client.get("portfolio/positions", params={"count_filter": "position"})
    return data.get("market_positions", [])


def sync_from_kalshi(client: KalshiClient) -> list[dict]:
    """
    Fetch live positions directly from Kalshi and enrich with market data.
    Returns a list of dicts ready for display — this is the source of truth,
    not positions.json.

    Each returned dict contains:
      ticker, side, contracts, avg_cost, current_price,
      unrealised_pnl, fees_paid, last_updated
    """
    raw_positions = get_positions(client)

    # Filter to temperature markets only (KX prefix)
    temp_positions = [
        p for p in raw_positions
        if p.get("ticker", "").startswith("KX")
    ]

    enriched = []
    if not temp_positions:
        return enriched

    # Batch fetch current prices in one API call
    tickers    = [pos["ticker"] for pos in temp_positions
                  if float(pos.get("position_fp") or 0) != 0]
    prices     = {}
    if tickers:
        try:
            resp = requests.get(
                "https://api.elections.kalshi.com/trade-api/v2/markets",
                params={"tickers": ",".join(tickers)},
                timeout=15,
            ).json()
            for m in resp.get("markets", []):
                yes_bid = float(m.get("yes_bid_dollars") or 0)
                no_bid  = float(m.get("no_bid_dollars")  or 0)
                # If one side has no resting bid, derive from complement
                if yes_bid > 0 and no_bid == 0:
                    no_bid = round(1.0 - yes_bid, 4)
                elif no_bid > 0 and yes_bid == 0:
                    yes_bid = round(1.0 - no_bid, 4)
                prices[m["ticker"]] = {
                    "yes_bid": yes_bid,
                    "no_bid":  no_bid,
                }
        except Exception:
            pass  # prices will be 0 if fetch fails — still show positions

    for pos in temp_positions:
        ticker       = pos["ticker"]
        # position_fp is a signed string: positive = long YES, negative = long NO
        position_fp  = float(pos.get("position_fp") or 0)
        fees_paid    = float(pos.get("fees_paid_dollars") or 0)
        total_cost   = float(pos.get("total_traded_dollars") or 0)
        last_updated = pos.get("last_updated_ts", "")

        if position_fp == 0:
            continue

        side      = "yes" if position_fp > 0 else "no"
        contracts = int(abs(position_fp))
        avg_cost  = round(total_cost / contracts, 4) if contracts else 0

        # Look up price from batch result
        if ticker in prices:
            current_price  = prices[ticker]["yes_bid"] if side == "yes" else prices[ticker]["no_bid"]
            unrealised_pnl = round((current_price - avg_cost) * contracts, 4)
        else:
            current_price  = 0
            unrealised_pnl = 0

        enriched.append({
            "ticker":         ticker,
            "side":           side,
            "contracts":      contracts,
            "avg_cost":       avg_cost,
            "current_price":  current_price,
            "unrealised_pnl": unrealised_pnl,
            "fees_paid":      fees_paid,
            "last_updated":   last_updated[:16].replace("T", " ") if last_updated else "",
        })

    return enriched


# ---------------------------------------------------------------------------
# Order placement
# ---------------------------------------------------------------------------

def place_order(
    client:     KalshiClient,
    ticker:     str,
    side:       str,        # "yes" or "no"
    price_dollars: float,   # e.g. 0.35
    contracts:  int,
    paper:      bool = False,
) -> dict:
    """
    Place a limit order on Kalshi.

    side:  "yes" to buy YES contracts, "no" to buy NO contracts
    price: limit price in dollars ($0.01–$0.99)
    """
    # Kalshi uses integer cent prices
    price_cents = int(round(price_dollars * 100))

    order = {
        "ticker":          ticker,
        "action":          "buy",
        "side":            side,
        "type":            "limit",
        "count":           contracts,
        "yes_price":       price_cents if side == "yes" else (100 - price_cents),
        "client_order_id": f"kw-{uuid.uuid4().hex[:12]}",
    }

    if paper:
        print(f"    [PAPER] Would place: {side.upper()} {contracts}x {ticker} @ ${price_dollars:.2f}")
        return {"paper": True, "order": order}

    try:
        result = client.post("portfolio/orders", order)
        return result
    except requests.exceptions.HTTPError as e:
        try:
            detail = e.response.json()
        except Exception:
            detail = e.response.text
        raise RuntimeError(f"Order failed {e.response.status_code}: {detail}") from e


def test_order(client: KalshiClient):
    """
    Full round-trip order test — place then immediately cancel.

    Places a YES limit order at $0.01 on the first open climate market
    found. At $0.01 it will never fill. Cancels it immediately after
    confirming the API accepted it.

    Proves: auth works, order body is valid, cancel works.
    """
    print("\n  Finding an open climate market...")

    # Find any open temperature market to use as the test vehicle
    data = requests.get(
        "https://api.elections.kalshi.com/trade-api/v2/markets",
        params={"series_ticker": "KXHIGHNY", "status": "open"},
        timeout=10,
    ).json()

    markets = data.get("markets", [])
    if not markets:
        print("  No open KXHIGHNY markets found — trying KXHIGHMIA...")
        data = requests.get(
            "https://api.elections.kalshi.com/trade-api/v2/markets",
            params={"series_ticker": "KXHIGHMIA", "status": "open"},
            timeout=10,
        ).json()
        markets = data.get("markets", [])

    if not markets:
        print("  Could not find any open market to test against.")
        return

    # Pick a middle bracket (not the <1% ones at the extremes)
    ticker = markets[len(markets) // 2]["ticker"]
    print(f"  Test market: {ticker}")

    # Step 1: Place a $0.01 YES limit order (will never fill)
    print("\n  Step 1 — placing YES limit order @ $0.01 (1 contract)...")
    order_body = {
        "ticker":          ticker,
        "action":          "buy",
        "side":            "yes",
        "type":            "limit",
        "count":           1,
        "yes_price":       1,    # 1 cent — will never fill
        "client_order_id": f"kw-test-{uuid.uuid4().hex[:8]}",
    }

    try:
        result   = client.post("portfolio/orders", order_body)
        order    = result.get("order", {})
        order_id = order.get("order_id") or order.get("id")

        if not order_id:
            print(f"  Unexpected response — no order_id found:")
            print(f"  {json.dumps(result, indent=2)}")
            return

        print(f"  Order accepted — order_id: {order_id}")
        print(f"  Status: {order.get('status', '?')}")

    except requests.exceptions.HTTPError as e:
        print(f"  Order placement FAILED: {e}")
        try:
            print(f"  Response body: {e.response.json()}")
        except Exception:
            print(f"  Response text: {e.response.text}")
        return
    except Exception as e:
        print(f"  Order placement FAILED: {e}")
        return

    # Step 2: Immediately cancel it
    print(f"\n  Step 2 — cancelling order {order_id}...")
    try:
        cancel_result = client.delete(f"portfolio/orders/{order_id}")
        cancelled     = cancel_result.get("order", {})
        print(f"  Cancelled — status: {cancelled.get('status', '?')}")
        print(f"\n  Test PASSED — full round trip (place → cancel) works correctly.")
    except Exception as e:
        print(f"  Cancel FAILED: {e}")
        print(f"  WARNING: Order {order_id} may still be resting. "
              f"Cancel manually at kalshi.com/portfolio.")


def contracts_for_signal(signal: dict) -> int:
    """Scale position size by signal score."""
    score      = signal.get("score", 1)
    multiplier = SCORE_SIZING.get(score, 1.0)
    contracts  = int(BASE_CONTRACTS * multiplier)
    return min(contracts, MAX_CONTRACTS_PER_ORDER)


# ---------------------------------------------------------------------------
# Exit monitor
# ---------------------------------------------------------------------------

NO_STOP_LOSS_RISE   = 0.15   # exit NO if YES rises more than this above entry YES price
YES_EXIT_TARGET     = 0.50   # take profit if YES rises 50% from entry
YES_STOP_LOSS       = 0.30   # stop loss if YES falls 30% from entry

def check_exits(client: KalshiClient, paper: bool = False):
    """
    Check all open positions and trigger exits where appropriate.
    Sources positions directly from Kalshi — not from local positions.json.

    NO trades:
      - Stop loss if YES price has risen NO_STOP_LOSS_RISE above entry YES price
        Entry YES price = 1.0 - avg_cost (derived from Kalshi position data)

    YES trades:
      - Take profit when price rises YES_EXIT_TARGET% from entry
      - Stop loss when price falls YES_STOP_LOSS% from entry
    """
    # Get live positions from Kalshi — this catches ALL open positions
    # regardless of whether they were recorded in positions.json
    try:
        live_positions = sync_from_kalshi(client)
    except Exception as e:
        print(f"  Could not fetch live positions for exit check: {e}")
        return

    if not live_positions:
        return

    # Prices already fetched by sync_from_kalshi — use current_price directly
    # But we need yes_bid specifically for NO stop-loss check, so batch fetch
    tickers   = [p["ticker"] for p in live_positions]
    prices    = {}
    try:
        resp = requests.get(
            "https://api.elections.kalshi.com/trade-api/v2/markets",
            params={"tickers": ",".join(tickers)},
            timeout=15,
        ).json()
        for m in resp.get("markets", []):
            yes_bid = float(m.get("yes_bid_dollars") or 0)
            no_bid  = float(m.get("no_bid_dollars")  or 0)
            if yes_bid > 0 and no_bid == 0:
                no_bid = round(1.0 - yes_bid, 4)
            elif no_bid > 0 and yes_bid == 0:
                yes_bid = round(1.0 - no_bid, 4)
            prices[m["ticker"]] = {
                "yes_bid": yes_bid,
                "no_bid":  no_bid,
                "status":  m.get("status", "active"),
            }
    except Exception as e:
        print(f"  Batch price fetch failed: {e} — skipping exit check")
        return

    for pos in live_positions:
        ticker    = pos["ticker"]
        side      = pos["side"]           # "yes" or "no"
        avg_cost  = pos["avg_cost"]       # what we paid per contract
        contracts = pos["contracts"]

        if ticker not in prices:
            continue

        market   = prices[ticker]
        status   = market["status"]
        yes_price = market["yes_bid"]
        no_price  = market["no_bid"]

        # Skip markets that are no longer active
        if status not in ("active", "initialized"):
            continue

        exit_reason = None
        exit_price  = None

        if side == "no":
            # Entry YES price = 1.0 - what we paid for NO
            entry_yes_price = round(1.0 - avg_cost, 2)
            stop_threshold  = round(entry_yes_price + NO_STOP_LOSS_RISE, 2)

            if yes_price >= stop_threshold:
                exit_reason = "no_stop_loss"
                exit_price  = no_price
                print(f"  NO STOP LOSS: {ticker} "
                      f"YES rose to ${yes_price:.2f} "
                      f"(entry YES=${entry_yes_price:.2f} + {NO_STOP_LOSS_RISE:.2f} "
                      f"= threshold ${stop_threshold:.2f})")
                print(f"    Avg cost=${avg_cost:.2f} → Exit=${exit_price:.2f} "
                      f"PnL=${round((exit_price - avg_cost) * contracts, 2):+.2f}")

        else:  # YES trade
            take_profit = round(avg_cost * (1 + YES_EXIT_TARGET), 2)
            stop_loss   = round(avg_cost * (1 - YES_STOP_LOSS),   2)

            if yes_price >= take_profit:
                exit_reason = "take_profit"
                exit_price  = yes_price
                print(f"  TAKE PROFIT: {ticker} YES "
                      f"${avg_cost:.2f} → ${yes_price:.2f}")
            elif yes_price <= stop_loss:
                exit_reason = "stop_loss"
                exit_price  = yes_price
                print(f"  STOP LOSS: {ticker} YES "
                      f"${avg_cost:.2f} → ${yes_price:.2f}")

        if exit_reason:
            if not paper:
                exit_side = "yes" if side == "no" else "no"
                try:
                    place_order(
                        client        = client,
                        ticker        = ticker,
                        side          = exit_side,
                        price_dollars = exit_price,
                        contracts     = contracts,
                        paper         = False,
                    )
                    print(f"  Exit order placed: {ticker} {exit_side.upper()} "
                          f"@ ${exit_price:.2f}  reason={exit_reason}")
                except Exception as e:
                    print(f"  Exit order failed for {ticker}: {e}")
                    continue
            else:
                print(f"    [PAPER] Would exit {ticker} {side.upper()} "
                      f"@ ${exit_price:.2f}  reason={exit_reason}")

        time.sleep(0.1)


# ---------------------------------------------------------------------------
# Full pipeline: signals → execute
# ---------------------------------------------------------------------------

def run_pipeline(client: KalshiClient, city_filter: str = None, paper: bool = False):
    """Run decision engine, then execute any actionable signals."""
    evaluations = decision_engine.run(city_filter=city_filter, paper=False)
    decision_engine.display(evaluations)

    balance    = get_balance(client)
    deployable = round(balance * 0.70, 2)
    print(f"\n  Account balance: ${balance:.2f}  |  Deployable (70%): ${deployable:.2f}")

    # Open tickers sourced directly from Kalshi — no local file
    try:
        live_positions = sync_from_kalshi(client)
        open_tickers   = {p["ticker"] for p in live_positions}
    except Exception:
        open_tickers = set()

    executed = 0
    deployed = 0.0
    for ev in evaluations:
        city    = ev["city"]
        signals = [s for s in ev.get("signals", []) if s.get("trade_type")]

        for signal in signals:
            contracts = contracts_for_signal(signal)
            side      = signal["trade_type"].lower()
            price     = signal["entry_price"]
            ticker    = signal["ticker"]

            if ticker in open_tickers:
                print(f"  Skipping {ticker} — already holding this position")
                continue

            cost = price * contracts
            if deployed + cost > deployable:
                print(f"  Skipping {ticker} — would exceed 70% deployable cap "
                      f"(deployed=${deployed:.2f} + cost=${cost:.2f} > ${deployable:.2f})")
                continue

            print(f"\n  Executing: {city} {ticker}")
            print(f"    {side.upper()} {contracts}x @ ${price:.2f}  "
                  f"target=${signal['exit_target']:.2f}  score={signal['score']}/3")

            try:
                place_order(
                    client        = client,
                    ticker        = ticker,
                    side          = side,
                    price_dollars = price,
                    contracts     = contracts,
                    paper         = paper,
                )
                open_tickers.add(ticker)
                deployed += cost
                executed += 1
            except Exception as e:
                print(f"  Order failed for {ticker}: {e}")

    print(f"\n  {executed} order(s) placed.")


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kalshi weather trader")
    parser.add_argument("--balance",    action="store_true", help="Show account balance")
    parser.add_argument("--monitor",    action="store_true", help="Start exit monitor loop")
    parser.add_argument("--run",        action="store_true", help="Run full pipeline (signals + execute)")
    parser.add_argument("--test-order", action="store_true", help="Place + immediately cancel a $0.01 test order")
    parser.add_argument("--paper",      action="store_true", help="Paper mode — no real orders")
    parser.add_argument("--city",       type=str, default=None, help="Filter to one city")
    args = parser.parse_args()

    env_file = Path(".env")
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            if "=" in line and not line.startswith("#"):
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())

    if args.balance or args.run or args.monitor or args.test_order:
        client = make_client()

        if args.balance:
            bal = get_balance(client)
            print(f"\n  Account balance: ${bal:.2f}")

        if args.test_order:
            test_order(client)

        if args.run:
            run_pipeline(client, city_filter=args.city, paper=args.paper)

        if args.monitor:
            print(f"\nExit monitor running — polling every {MONITOR_INTERVAL}s. Ctrl+C to stop.")
            while True:
                print(f"\n[{datetime.now(timezone.utc).strftime('%H:%M UTC')}] Checking exits...")
                check_exits(client, paper=args.paper)
                time.sleep(MONITOR_INTERVAL)

    else:
        parser.print_help()
