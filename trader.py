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

  Set these as environment variables or in a .env file:
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

POSITIONS_FILE  = Path("data/positions.json")
TRADE_LOG_FILE  = Path("data/trade_log.json")

# Max contracts per single order — hard safety cap
MAX_CONTRACTS_PER_ORDER = 100

# Default contracts per signal (will be sized by score later)
BASE_CONTRACTS = 20

# Score-based position sizing multiplier
SCORE_SIZING = {1: 0.5, 2: 1.0, 3: 1.5}

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

    def get(self, endpoint: str, params: dict = None) -> dict:
        path = self._api_path(endpoint)
        resp = requests.get(
            self.base_url + "/" + endpoint.lstrip("/"),
            headers=self._headers("GET", path),
            params=params,
            timeout=15,
        )
        resp.raise_for_status()
        return resp.json()

    def post(self, endpoint: str, body: dict) -> dict:
        path = self._api_path(endpoint)
        resp = requests.post(
            self.base_url + "/" + endpoint.lstrip("/"),
            headers=self._headers("POST", path),
            json=body,
            timeout=15,
        )
        resp.raise_for_status()
        return resp.json()

    def delete(self, endpoint: str) -> dict:
        path = self._api_path(endpoint)
        resp = requests.delete(
            self.base_url + "/" + endpoint.lstrip("/"),
            headers=self._headers("DELETE", path),
            timeout=15,
        )
        resp.raise_for_status()
        return resp.json()


def make_client() -> KalshiClient:
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

    if not demo:
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
    """Returns all open market positions."""
    data = client.get("portfolio/positions")
    return data.get("market_positions", [])


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
# Position store (local JSON)
# ---------------------------------------------------------------------------

def load_positions() -> list[dict]:
    POSITIONS_FILE.parent.mkdir(parents=True, exist_ok=True)
    if POSITIONS_FILE.exists():
        with open(POSITIONS_FILE) as f:
            return json.load(f)
    return []


def save_positions(positions: list[dict]):
    with open(POSITIONS_FILE, "w") as f:
        json.dump(positions, f, indent=2, default=str)


def record_entry(signal: dict, contracts: int, order_result: dict):
    """Add a new position to the local store after entry."""
    positions = load_positions()
    positions.append({
        "id":           order_result.get("order", {}).get("client_order_id", "paper"),
        "ticker":       signal["ticker"],
        "side":         signal["trade_type"].lower(),
        "entry_price":  signal["entry_price"],
        "exit_target":  signal["exit_target"],
        "stop_loss":    signal.get("stop_loss"),
        "contracts":    contracts,
        "score":        signal["score"],
        "score_detail": signal["score_detail"],
        "status":       "open",
        "opened_at":    datetime.now(timezone.utc).isoformat(),
        "closed_at":    None,
        "exit_price":   None,
        "pnl":          None,
    })
    save_positions(positions)


def record_exit(position_id: str, exit_price: float, reason: str):
    """Mark a position as closed and compute PnL."""
    positions = load_positions()
    for pos in positions:
        if pos["id"] == position_id and pos["status"] == "open":
            contracts   = pos["contracts"]
            entry_price = pos["entry_price"]
            pnl         = round((exit_price - entry_price) * contracts, 2)
            pos.update({
                "status":     "closed",
                "closed_at":  datetime.now(timezone.utc).isoformat(),
                "exit_price": exit_price,
                "exit_reason":reason,
                "pnl":        pnl,
            })
            print(f"  Closed {pos['ticker']} @ ${exit_price:.2f}  PnL: ${pnl:+.2f}  ({reason})")
    save_positions(positions)


# ---------------------------------------------------------------------------
# Trade log
# ---------------------------------------------------------------------------

def log_trade(event: str, data: dict):
    TRADE_LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    log = json.loads(TRADE_LOG_FILE.read_text()) if TRADE_LOG_FILE.exists() else []
    log.append({
        "ts":    datetime.now(timezone.utc).isoformat(),
        "event": event,
        **data,
    })
    TRADE_LOG_FILE.write_text(json.dumps(log, indent=2, default=str))


# ---------------------------------------------------------------------------
# Exit monitor
# ---------------------------------------------------------------------------

def check_exits(client: KalshiClient, paper: bool = False):
    """
    Check all open positions against current Kalshi prices.
    Trigger exits when exit_target or stop_loss is hit.
    """
    positions = [p for p in load_positions() if p["status"] == "open"]
    if not positions:
        return

    for pos in positions:
        ticker = pos["ticker"]
        side   = pos["side"]

        try:
            # Get current market price
            market_data = requests.get(
                f"https://api.elections.kalshi.com/trade-api/v2/markets/{ticker}",
                timeout=10,
            ).json().get("market", {})

            if side == "yes":
                current_price = float(market_data.get("yes_bid_dollars") or 0)
            else:
                current_price = float(market_data.get("no_bid_dollars") or 0)

            entry_price  = pos["entry_price"]
            exit_target  = pos["exit_target"]
            stop_loss    = pos.get("stop_loss")

            # Check exit target
            if current_price >= exit_target:
                print(f"  EXIT TARGET hit: {ticker} {side.upper()} "
                      f"entry=${entry_price:.2f} current=${current_price:.2f} "
                      f"target=${exit_target:.2f}")
                if not paper:
                    # Place market sell order (limit at best bid)
                    place_order(
                        client    = client,
                        ticker    = ticker,
                        side      = side,
                        price_dollars = current_price,
                        contracts = pos["contracts"],
                        paper     = paper,
                    )
                record_exit(pos["id"], current_price, "exit_target")
                log_trade("exit_target", {
                    "ticker":        ticker,
                    "side":          side,
                    "entry_price":   entry_price,
                    "exit_price":    current_price,
                    "contracts":     pos["contracts"],
                })

            # Check stop loss (YES trades only — NO trades held to resolution)
            elif stop_loss and current_price <= stop_loss:
                print(f"  STOP LOSS hit: {ticker} {side.upper()} "
                      f"entry=${entry_price:.2f} current=${current_price:.2f} "
                      f"stop=${stop_loss:.2f}")
                if not paper:
                    place_order(
                        client    = client,
                        ticker    = ticker,
                        side      = side,
                        price_dollars = current_price,
                        contracts = pos["contracts"],
                        paper     = paper,
                    )
                record_exit(pos["id"], current_price, "stop_loss")
                log_trade("stop_loss", {
                    "ticker":      ticker,
                    "side":        side,
                    "entry_price": entry_price,
                    "exit_price":  current_price,
                    "contracts":   pos["contracts"],
                })

        except Exception as e:
            print(f"  Error checking exit for {ticker}: {e}")

        time.sleep(0.2)


# ---------------------------------------------------------------------------
# Full pipeline: signals → execute
# ---------------------------------------------------------------------------

def run_pipeline(client: KalshiClient, city_filter: str = None, paper: bool = False):
    """Run decision engine, then execute any actionable signals."""
    evaluations = decision_engine.run(city_filter=city_filter, paper=False)
    decision_engine.display(evaluations)

    balance = get_balance(client)
    print(f"\n  Account balance: ${balance:.2f}")

    executed = 0
    for ev in evaluations:
        city    = ev["city"]
        signals = [s for s in ev.get("signals", []) if s.get("trade_type")]

        for signal in signals:
            contracts = contracts_for_signal(signal)
            side      = signal["trade_type"].lower()
            price     = signal["entry_price"]
            ticker    = signal["ticker"]

            cost = price * contracts
            if cost > balance * 0.15:   # never risk more than 15% of balance on one trade
                print(f"  Skipping {ticker} — cost ${cost:.2f} exceeds 15% balance cap")
                continue

            print(f"\n  Executing: {city} {ticker}")
            print(f"    {side.upper()} {contracts}x @ ${price:.2f}  "
                  f"target=${signal['exit_target']:.2f}  score={signal['score']}/3")

            try:
                result = place_order(
                    client        = client,
                    ticker        = ticker,
                    side          = side,
                    price_dollars = price,
                    contracts     = contracts,
                    paper         = paper,
                )
                record_entry(signal, contracts, result)
                log_trade("entry", {
                    "city":        city,
                    "ticker":      ticker,
                    "side":        side,
                    "price":       price,
                    "contracts":   contracts,
                    "score":       signal["score"],
                    "score_detail":signal["score_detail"],
                    "paper":       paper,
                })
                executed += 1

            except Exception as e:
                print(f"  Order failed for {ticker}: {e}")
                log_trade("entry_failed", {"ticker": ticker, "error": str(e)})

    print(f"\n  {executed} order(s) placed.")


# ---------------------------------------------------------------------------
# Display helpers
# ---------------------------------------------------------------------------

def display_positions():
    positions = load_positions()
    if not positions:
        print("No positions on record.")
        return

    open_pos   = [p for p in positions if p["status"] == "open"]
    closed_pos = [p for p in positions if p["status"] == "closed"]

    print(f"\n{'='*65}")
    print(f"  Positions  ({len(open_pos)} open, {len(closed_pos)} closed)")
    print(f"{'='*65}")

    if open_pos:
        print(f"\n  Open:")
        print(f"  {'Ticker':<30} {'Side':>5} {'Entry':>7} {'Target':>8} {'Qty':>5}")
        print(f"  {'-'*58}")
        for p in open_pos:
            print(
                f"  {p['ticker']:<30} "
                f"{p['side'].upper():>5} "
                f"${p['entry_price']:.2f}  "
                f"${p['exit_target']:.2f}   "
                f"{p['contracts']:>5}"
            )

    if closed_pos:
        total_pnl = sum(p.get("pnl") or 0 for p in closed_pos)
        print(f"\n  Closed (total PnL: ${total_pnl:+.2f}):")
        print(f"  {'Ticker':<30} {'Side':>5} {'Entry':>7} {'Exit':>7} {'PnL':>8}  Reason")
        print(f"  {'-'*65}")
        for p in closed_pos:
            print(
                f"  {p['ticker']:<30} "
                f"{p['side'].upper():>5} "
                f"${p['entry_price']:.2f}  "
                f"${p.get('exit_price') or 0:.2f}  "
                f"${p.get('pnl') or 0:>+.2f}   "
                f"{p.get('exit_reason','?')}"
            )

    print(f"{'='*65}")


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kalshi weather trader")
    parser.add_argument("--balance",    action="store_true", help="Show account balance")
    parser.add_argument("--positions",  action="store_true", help="Show open/closed positions")
    parser.add_argument("--monitor",    action="store_true", help="Start exit monitor loop")
    parser.add_argument("--run",        action="store_true", help="Run full pipeline (signals + execute)")
    parser.add_argument("--test-order", action="store_true", help="Place + immediately cancel a $0.01 test order")
    parser.add_argument("--paper",      action="store_true", help="Paper mode — no real orders")
    parser.add_argument("--city",       type=str, default=None, help="Filter to one city")
    args = parser.parse_args()

    # Load .env if present
    env_file = Path(".env")
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            if "=" in line and not line.startswith("#"):
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())

    if args.positions:
        display_positions()

    elif args.balance or args.run or args.monitor or args.test_order:
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
