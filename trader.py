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
from zoneinfo import ZoneInfo

# Timestamps in positions table are shown in this timezone
DISPLAY_TZ = ZoneInfo("Europe/Lisbon")

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

import hight_decision_engine as decision_engine
import lowt_decision_engine

from log_setup import get_logger
log = get_logger(__name__)

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
# Kept flat at 1x — with a small account, size consistency matters more than scaling
SCORE_SIZING = {1: 1.0, 2: 1.0, 3: 1.0}

# Exit monitor poll interval (seconds)
MONITOR_INTERVAL = 30


# ---------------------------------------------------------------------------
# Trade log — persists signal metadata for post-hoc score analysis
# ---------------------------------------------------------------------------

TRADE_LOG_FILE = Path("data/trade_log.json")


def _append_trade_log(entry: dict):
    """
    Append one trade entry to data/trade_log.json.

    Each entry captures the signal metadata at the moment of order placement
    so outcomes can later be joined against settlements by ticker.

    Fields saved:
      ticker, city, side, market_type, score, score_detail,
      entry_price, contracts, placed_at (UTC ISO), paper (bool)
    """
    TRADE_LOG_FILE.parent.mkdir(exist_ok=True)
    existing: list = []
    if TRADE_LOG_FILE.exists():
        try:
            existing = json.loads(TRADE_LOG_FILE.read_text())
        except Exception:
            existing = []
    existing.append(entry)
    TRADE_LOG_FILE.write_text(json.dumps(existing, indent=2, default=str))


# ---------------------------------------------------------------------------
# Auth client
# ---------------------------------------------------------------------------

class KalshiClient:
    """
    Authenticated Kalshi REST client using RSA-PSS request signing.
    Handles all auth header generation transparently.
    """

    def __init__(self, key_id: str, key_file: str, demo: bool = True):
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
        log.info("KalshiClient ready [%s]  key_id=%s...", env, key_id[:8])

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
        return f"/trade-api/v2/{endpoint.lstrip('/')}"

    def _request_with_backoff(
        self, method: str, url: str, headers: dict, timeout: int,
        params: dict = None, json: dict = None,
    ) -> requests.Response:
        """
        Execute an HTTP request with exponential backoff retry.
        Retries on: connection errors, timeouts, 429 rate-limit, 5xx server errors.
        Does NOT retry on 4xx client errors.
        """
        MAX_RETRIES = 4
        BASE_DELAY  = 2

        for attempt in range(MAX_RETRIES):
            try:
                resp = requests.request(
                    method, url,
                    headers=headers,
                    params=params,
                    json=json,
                    timeout=timeout,
                )
                if resp.status_code == 429 or resp.status_code >= 500:
                    if attempt < MAX_RETRIES - 1:
                        delay = BASE_DELAY * (2 ** attempt)
                        log.warning("[%d] retrying in %ds" +  # 
                              f"(attempt {attempt + 1}/{MAX_RETRIES})...")
                        time.sleep(delay)
                        continue
                resp.raise_for_status()
                return resp

            except requests.exceptions.Timeout:
                if attempt < MAX_RETRIES - 1:
                    delay = BASE_DELAY * (2 ** attempt)
                    log.warning("[timeout] retrying in %ds" +  # 
                          f"(attempt {attempt + 1}/{MAX_RETRIES})...")
                    time.sleep(delay)
                else:
                    raise

            except requests.exceptions.ConnectionError:
                if attempt < MAX_RETRIES - 1:
                    delay = BASE_DELAY * (2 ** attempt)
                    log.warning("[connection error] retrying in %ds" +  # 
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
    balance = data.get("balance", 0)
    return balance / 100   # Kalshi returns cents


def get_positions(client: KalshiClient) -> list[dict]:
    """Returns all open market positions from Kalshi."""
    data = client.get("portfolio/positions", params={"count_filter": "position"})
    return data.get("market_positions", [])


def _parse_kalshi_ts(raw) -> str:
    """
    Parse a Kalshi timestamp into a local display string (DISPLAY_TZ).

    Kalshi returns timestamps in multiple formats depending on the endpoint:
      - ISO 8601 string: "2026-04-08T13:30:00Z" or "2026-04-08T13:30:00.000000+00:00"
      - Unix seconds:    1744112400
      - Unix milliseconds: 1744112400000
    """
    if not raw:
        return ""
    try:
        s  = str(raw).strip()
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return dt.astimezone(DISPLAY_TZ).strftime("%Y-%m-%d %H:%M")
    except (ValueError, TypeError):
        pass
    try:
        ms = int(raw)
        if ms > 1_000_000_000_000:   # milliseconds
            ms = ms / 1000
        dt = datetime.fromtimestamp(ms, tz=timezone.utc)
        return dt.astimezone(DISPLAY_TZ).strftime("%Y-%m-%d %H:%M")
    except (ValueError, TypeError, OSError):
        pass
    return str(raw)[:16].replace("T", " ")   # last resort


def _normalise_prices(m: dict) -> dict:
    """
    Extract and normalise yes/no bid prices from a Kalshi market dict.
    Handles settled markets, last_price fallback, and one-sided books.
    Shared by _market_price() and the batch fetch in sync_from_kalshi().
    """
    result  = (m.get("result") or "").lower()
    status  = m.get("status", "active")

    if result == "yes":
        return {"yes_bid": 0.99, "no_bid": 0.01, "status": status, "result": result}
    if result == "no":
        return {"yes_bid": 0.01, "no_bid": 0.99, "status": status, "result": result}

    yes_bid = float(m.get("yes_bid_dollars") or 0)
    no_bid  = float(m.get("no_bid_dollars")  or 0)

    if yes_bid == 0 and no_bid == 0:
        lp_cents = float(m.get("last_price") or 0)
        if 1 <= lp_cents <= 99:
            yes_bid = round(lp_cents / 100, 4)
            no_bid  = round(1.0 - yes_bid, 4)

    if yes_bid > 0 and no_bid == 0:
        no_bid = round(1.0 - yes_bid, 4)
    elif no_bid > 0 and yes_bid == 0:
        yes_bid = round(1.0 - no_bid, 4)

    return {"yes_bid": yes_bid, "no_bid": no_bid, "status": status, "result": result}


def _market_price(ticker: str) -> dict:
    """
    Fetch a single market's price data from the individual Kalshi endpoint.
    The individual endpoint always returns result and last_price, unlike the
    batch endpoint which strips them for finalized markets.
    """
    zero = {"yes_bid": 0.0, "no_bid": 0.0, "status": "unknown", "result": ""}
    try:
        resp = requests.get(
            f"https://api.elections.kalshi.com/trade-api/v2/markets/{ticker}",
            timeout=10,
        ).json()
        return _normalise_prices(resp.get("market", {}))
    except Exception as e:
        log.debug("sync: individual fetch failed %s: %s", ticker, e)
        return zero


def sync_from_kalshi(client: KalshiClient) -> list[dict]:
    """
    Fetch live positions directly from Kalshi and enrich with market data.
    Returns a list of dicts ready for display — this is the source of truth.
    """
    raw_positions = get_positions(client)

    temp_positions = [
        p for p in raw_positions
        if p.get("ticker", "").startswith("KX")
        and ("HIGH" in p.get("ticker", "") or "LOWT" in p.get("ticker", ""))
    ]

    enriched = []
    if not temp_positions:
        return enriched

    tickers = [p["ticker"] for p in temp_positions
               if float(p.get("position_fp") or 0) != 0]

    if not tickers:
        return enriched

    # ── Batch fetch current market prices ────────────────────────────────────
    prices = {}
    try:
        resp = requests.get(
            "https://api.elections.kalshi.com/trade-api/v2/markets",
            params={"tickers": ",".join(tickers)},
            timeout=15,
        ).json()
        for m in resp.get("markets", []):
            prices[m["ticker"]] = _normalise_prices(m)
    except Exception as e:
        log.error("sync: batch price fetch failed: %s", e)

    # ── Individual fallback for tickers with no price data ───────────────────
    # The batch endpoint strips result/last_price from finalized markets.
    # The individual endpoint always returns the full object.
    for ticker in tickers:
        p = prices.get(ticker, {})
        if not p or (p["yes_bid"] == 0 and p["no_bid"] == 0):
            prices[ticker] = _market_price(ticker)
            time.sleep(0.1)   # only fires for closed positions — gentle on the API

    # ── Fetch fills for avg_cost and opened-at timestamp ─────────────────────
    fills_by_ticker: dict[str, list] = {}
    try:
        resp = client.get("portfolio/fills", params={"limit": 200})
        for f in resp.get("fills", []):
            t = f.get("ticker", "")
            if t in tickers:
                fills_by_ticker.setdefault(t, []).append(f)
    except Exception as e:
        log.warning("sync: fills fetch failed: %s", e)

    # ── Enrich each position ──────────────────────────────────────────────────
    for pos in temp_positions:
        ticker      = pos["ticker"]
        position_fp = float(pos.get("position_fp") or 0)
        fees_paid   = float(pos.get("fees_paid_dollars") or 0)

        if position_fp == 0:
            continue

        side      = "yes" if position_fp > 0 else "no"
        contracts = int(abs(position_fp))

        # Opened-at timestamp: use earliest buy fill's created_time.
        buy_fills = [
            f for f in fills_by_ticker.get(ticker, [])
            if f.get("action") == "buy" and f.get("side") == side
        ]
        if buy_fills:
            earliest  = min(buy_fills, key=lambda f: f.get("created_time", ""))
            opened_at = _parse_kalshi_ts(earliest.get("created_time", ""))
        else:
            opened_at = _parse_kalshi_ts(pos.get("last_updated_ts", ""))

        # avg_cost from fills.
        # Kalshi fills return yes_price_dollars for ALL trades including NO buys.
        # For NO trades: no_price = 1.0 - yes_price_dollars.
        avg_cost = 0.0
        if buy_fills:
            if side == "yes":
                total_cost = sum(
                    float(f.get("yes_price_dollars") or 0) *
                    float(f.get("count_fp") or 0) for f in buy_fills
                )
            else:
                total_cost = sum(
                    (1.0 - float(f.get("yes_price_dollars") or 0)) *
                    float(f.get("count_fp") or 0) for f in buy_fills
                )
            total_contracts = sum(float(f.get("count_fp") or 0) for f in buy_fills)
            if total_contracts > 0:
                avg_cost = round(total_cost / total_contracts, 4)

        if avg_cost == 0 and contracts > 0:
            total_traded = float(pos.get("total_traded_dollars") or 0)
            avg_cost = round(total_traded / contracts, 4)

        # Current price and unrealised PnL
        p    = prices.get(ticker, {})
        live = False
        if p.get("yes_bid", 0) > 0 or p.get("no_bid", 0) > 0:
            current_price  = p["yes_bid"] if side == "yes" else p["no_bid"]
            unrealised_pnl = round((current_price - avg_cost) * contracts, 4)
            live           = p.get("status", "") in ("active", "initialized")
        else:
            current_price  = avg_cost
            unrealised_pnl = 0.0

        enriched.append({
            "ticker":         ticker,
            "side":           side,
            "contracts":      contracts,
            "avg_cost":       avg_cost,
            "current_price":  current_price,
            "unrealised_pnl": unrealised_pnl,
            "fees_paid":      fees_paid,
            "last_updated":   opened_at,
            "live":           live,
        })

    return enriched


# ---------------------------------------------------------------------------
# Order placement
# ---------------------------------------------------------------------------

def place_order(
    client:        KalshiClient,
    ticker:        str,
    side:          str,    # "yes" or "no"
    price_dollars: float,  # e.g. 0.35
    contracts:     int,
    paper:         bool = False,
    action:        str  = "buy",   # "buy" to open, "sell" to close
) -> dict:
    """
    Place a limit order on Kalshi.

    side:   "yes" or "no" -- which contracts to act on
    action: "buy"  to open a new position
            "sell" to close an existing position (exit trades must use this)
    price:  limit price in dollars ($0.01-$0.99)
    """
    price_cents = int(round(price_dollars * 100))

    order = {
        "ticker":          ticker,
        "action":          action,
        "side":            side,
        "type":            "limit",
        "count":           contracts,
        "yes_price":       price_cents if side == "yes" else (100 - price_cents),
        "client_order_id": f"kw-exit-{uuid.uuid4().hex[:8]}" if action == "sell" else f"kw-{uuid.uuid4().hex[:12]}",
    }

    if paper:
        log.info("[PAPER] %s %s %dx %s @ $%.2f", action.upper(), side.upper(), contracts, ticker, price_dollars)
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
    log.info("test: finding open market...")

    data = requests.get(
        "https://api.elections.kalshi.com/trade-api/v2/markets",
        params={"series_ticker": "KXHIGHNY", "status": "open"},
        timeout=10,
    ).json()
    markets = data.get("markets", [])

    if not markets:
        log.info("test: KXHIGHNY not found, trying KXHIGHMIA...")
        data    = requests.get(
            "https://api.elections.kalshi.com/trade-api/v2/markets",
            params={"series_ticker": "KXHIGHMIA", "status": "open"},
            timeout=10,
        ).json()
        markets = data.get("markets", [])

    if not markets:
        log.error("test: no open market found")
        return

    ticker = markets[len(markets) // 2]["ticker"]
    log.info("test market: %s", ticker)

    log.info("test step 1: placing YES limit @ $0.01...")
    order_body = {
        "ticker":          ticker,
        "action":          "buy",
        "side":            "yes",
        "type":            "limit",
        "count":           1,
        "yes_price":       1,
        "client_order_id": f"kw-test-{uuid.uuid4().hex[:8]}",
    }

    try:
        result   = client.post("portfolio/orders", order_body)
        order    = result.get("order", {})
        order_id = order.get("order_id") or order.get("id")

        if not order_id:
            log.error("test: unexpected response — no order_id")
            log.debug("test response: %s", json.dumps(result, indent=2))
            return

        log.info("test: order accepted  order_id=%s", order_id)
        log.info('test: status=%s', order.get('status','?'))

    except requests.exceptions.HTTPError as e:
        log.error("test: order placement failed: %s", e)
        try:
            log.error("test: response body: %s", e.response.json())
        except Exception:
            log.error("test: response text: %s", e.response.text)
        return
    except Exception as e:
        log.error("test: order placement failed: %s", e)
        return

    log.info("test step 2: cancelling order %s...", order_id)
    try:
        cancel_result = client.delete(f"portfolio/orders/{order_id}")
        cancelled     = cancel_result.get("order", {})
        log.info('test: cancelled  status=%s', cancelled.get('status','?'))
        log.info("test PASSED — full round trip OK")
    except Exception as e:
        log.error("test: cancel failed: %s", e)
        log.warning("WARNING: order %s may still be resting —" +  # 
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

# YES position exits
YES_STOP_LOSS = 0.30   # stop loss if YES falls 30% from entry

# NO position: time-weighted stop loss thresholds (fraction drop from entry NO price)
# Tightens as the day progresses — early illiquidity should not trigger an exit,
# but a late-day move against us is real information.
# Stop loss removed — data shows holding always beats exiting on price alone.
# All exits now routed through YES ceiling + forecast filter only.
NO_STOP_LOSS_MORNING   = 0.0    # disabled
NO_STOP_LOSS_PEAK      = 0.0    # disabled
NO_STOP_LOSS_AFTERNOON = 0.0    # disabled
                                # triggered by 21–23% intraday drops that fully recovered.

# NO position: probability ceiling
# Exit if YES crosses this regardless of our entry price — thesis is dead.
NO_YES_CEILING      = 0.60   # Yes must reach 0.60 before we consider exiting
NO_YES_CEILING_HOUR = 15     # Yes ceiling only fires at/after this local hour

# NO position: forecast anchor
# For HIGH: exit if observed high is within this many °F of the bracket floor.
# For LOWT: exit if observed low is within this many °F of the bracket ceiling.
FORECAST_ANCHOR_BUFFER  = 1.5   # °F — used by YES ceiling filter
FORECAST_FLOOR_GAP_MAX  = 2.0   # °F — if forecast > floor + this, Yes spike is noise   # °F — was 3.0. Tightened after audit: 10 WHW exits were
                                # triggered by the anchor firing 2–3°F below the bracket
                                # floor, but the high peaked below the floor or blasted
                                # above the ceiling (both NO wins).

ANCHOR_MIN_HOUR = 14           # local hour — anchor cannot fire before this.
                                # Before 14:00, the daily high hasn't been established
                                # and a small gap is normal morning temperature climbing.

ANCHOR_MIN_YES  = 0.30          # YES price must be at or above this for anchor to fire.
                                # If YES < 0.30, the market is already confident —
                                # temperature noise at that point adds no information.
                                # Only fire when BOTH temperature AND market show uncertainty.

# NO position: settlement hold override
# If the observed value is this many °F clear of the dangerous bracket boundary
# AND it's past SETTLEMENT_HOLD_HOUR local time, hold to settlement rather than
# taking an early exit — the position will resolve correctly.
SETTLEMENT_CLEAR_BUFFER = 5.0   # °F
SETTLEMENT_HOLD_HOUR    = 15    # local hour (3pm) after which we trust the observation


# ---------------------------------------------------------------------------
# Session-scoped ticker blacklist
# Tickers added here after any exit are never re-entered within the same
# app session. Resets naturally on restart (daily markets reset anyway).
# ---------------------------------------------------------------------------

_exited_this_session: set[str] = set()

# ---------------------------------------------------------------------------
# Daily capital snapshot
# Taken once at the first poll of each day. Used to compute a fixed main-
# engine budget and a hard cascade reserve that survive repeated polling.
# ---------------------------------------------------------------------------
from datetime import date as _date

_day_open_balance: float      = 0.0
_day_open_date:    _date|None = None
CASCADE_RESERVE:   float      = 30.00   # $ always kept for cascade engine
MAIN_BUDGET_PCT:   float      = 0.70    # fraction of day-open balance for main engine


def _update_day_snapshot(current_balance: float) -> tuple[float, float]:
    """
    Refresh the daily snapshot if the date has changed.
    Returns (main_deployable, cascade_reserve) based on day-open balance.

    main_deployable = how much of the main budget hasn't been deployed yet
                    = day_open * MAIN_BUDGET_PCT - already_deployed
    already_deployed = day_open_balance - current_balance
                     (crude proxy: assumes only exits return cash intraday)
    """
    global _day_open_balance, _day_open_date
    today = _date.today()
    if _day_open_date != today or _day_open_balance == 0.0:
        _day_open_balance = current_balance
        _day_open_date    = today
        log.info("day snapshot: $%.2f  (main_budget=$%.2f  cascade_reserve=$%.2f)",
                 current_balance,
                 round(current_balance * MAIN_BUDGET_PCT, 2),
                 CASCADE_RESERVE)

    main_budget      = round(_day_open_balance * MAIN_BUDGET_PCT, 2)
    already_deployed = max(0.0, _day_open_balance - current_balance)
    main_deployable  = max(0.0, round(main_budget - already_deployed, 2))
    return main_deployable, CASCADE_RESERVE


def _bracket_floor_ceiling(ticker: str) -> tuple[float | None, float | None]:
    """
    Extract (floor, ceiling) from a bracket ticker suffix.

    Kalshi bracket structure — the NUMBER in the ticker is the CAP:

      HIGH B brackets (e.g. B80.5 = "79 to 80°F"):
        cap   = 80.5  (the number)
        floor = 78.5  (cap - 2.0)
        Settles YES if 78.5 ≤ high < 80.5

      HIGH bottom T bracket (e.g. T79 = "78 or below"):
        floor = None  (no lower bound)
        cap   = 78.5  (val - 0.5)
        Settles YES if high < 78.5

      LOWT B brackets: same formula — cap = number, floor = cap - 2.0
      LOWT T brackets (e.g. T31 = "below 31°F low"):
        floor = None, cap = val

    Returns (None, None) if the suffix cannot be parsed.
    """
    try:
        bracket = ticker.split("-")[-1]
        is_lowt = "LOWT" in ticker

        if bracket.startswith("B"):
            cap = float(bracket[1:])
            return round(cap - 2.0, 1), cap

        elif bracket.startswith("T"):
            val = float(bracket[1:])
            if is_lowt:
                return None, val
            else:
                # HIGH T brackets are always bottom T ("below X°F") in our positions
                # since top T ("above X°F") NO trades are banned.
                return None, round(val - 0.5, 1)

    except (ValueError, IndexError):
        pass

    return None, None


def _no_stop_threshold(local_hour: int) -> float:
    """
    Return the NO stop loss fraction (drop from entry) based on city local hour.
    A higher value means we tolerate more of a drop before exiting.
    """
    if local_hour < 11:
        return NO_STOP_LOSS_MORNING
    elif local_hour < 13:
        return NO_STOP_LOSS_PEAK
    else:
        return NO_STOP_LOSS_AFTERNOON


def _city_local_hour(city: str) -> int:
    """Return the current local hour for a city. Defaults to 12 if unknown."""
    try:
        from cities import CITIES as _CITIES
        tz_name = _CITIES.get(city, {}).get("tz")
        if tz_name:
            return datetime.now(ZoneInfo(tz_name)).hour
    except Exception:
        pass
    return 12


def _post_exit_scan(
    client:         KalshiClient,
    exited_ticker:  str,
    live_positions: list,
    paper:          bool = False,
) -> None:
    """
    Called immediately after any No position is exited.

    Scans all remaining open brackets for the same market and enters No
    on any that are:
      - Still live (0.03 ≤ No ≤ NO_MAX_ENTRY_PRICE)
      - Not already at MAX_CONTRACTS
      - Not in _exited_this_session

    This covers two cases:
      1. Top-up: we already hold the bracket but have headroom for more contracts
      2. New entry: a bracket we don't yet hold that has become more certain
         now that a lower bracket has been exited (converging from below)

    The "exactly 2 live" restriction is removed — after any exit, all
    remaining eligible brackets in the market are candidates.
    """
    try:
        parts = exited_ticker.split("-")
        if len(parts) < 3:
            return
        series = parts[0]
        mdate  = parts[1]

        # Fetch all open brackets for this series
        resp = requests.get(
            "https://api.elections.kalshi.com/trade-api/v2/markets",
            params={"series_ticker": series, "status": "open"},
            timeout=10,
        ).json()
        markets = resp.get("markets", [])

        # Filter to today's date
        today_markets = [
            m for m in markets
            if mdate.upper() in m.get("ticker", "").upper()
        ]
        if not today_markets:
            log.debug("post_exit_scan: no open brackets for %s-%s", series, mdate)
            return

        # Current contracts held per ticker
        held = {p["ticker"]: p.get("contracts", 0) for p in live_positions}

        # Max contracts from decision engine
        try:
            import hight_decision_engine as _de
            max_c = _de.MAX_CONTRACTS
        except Exception:
            max_c = 6

        candidates = []
        for m in today_markets:
            t = m.get("ticker", "")
            if t == exited_ticker:
                continue   # skip the one we just exited
            if t in _exited_this_session:
                continue

            yes_p = float(m.get("yes_bid_dollars") or 0)
            no_p  = float(m.get("no_bid_dollars")  or 0)
            if yes_p == 0 and no_p > 0:
                yes_p = round(1.0 - no_p, 4)
            elif no_p == 0 and yes_p > 0:
                no_p = round(1.0 - yes_p, 4)

            # Must be live and in our entry range
            if not (0.75 <= no_p <= NO_MAX_ENTRY_PRICE):
                continue
            if yes_p > 0.25:
                continue

            # Check headroom
            currently_held = held.get(t, 0)
            headroom = max_c - currently_held
            if headroom <= 0:
                continue

            candidates.append({
                "ticker":   t,
                "no_price": no_p,
                "yes_price": yes_p,
                "headroom": headroom,
                "held":     currently_held,
            })

        if not candidates:
            log.debug("post_exit_scan: no eligible brackets after exit of %s", exited_ticker)
            return

        # Sort by No price descending — highest conviction first
        candidates.sort(key=lambda x: x["no_price"], reverse=True)

        log.info("post_exit_scan: %d eligible bracket(s) after exit of %s",
                 len(candidates), exited_ticker)

        for c in candidates:
            contracts = min(BASE_CONTRACTS, c["headroom"])
            action    = "top-up" if c["held"] > 0 else "new"
            log.info("post_exit_scan: %s No on %s @ $%.2f  (%s, held=%d headroom=%d)%s",
                     action, c["ticker"], c["no_price"],
                     action, c["held"], c["headroom"],
                     "  [PAPER]" if paper else "")

            if not paper:
                try:
                    place_order(
                        client        = client,
                        ticker        = c["ticker"],
                        side          = "no",
                        action        = "buy",
                        price_dollars = c["no_price"],
                        contracts     = contracts,
                        paper         = False,
                    )
                    _append_trade_log({
                        "ticker":       c["ticker"],
                        "city":         series,
                        "side":         "no",
                        "market_type":  "high",
                        "score":        0,
                        "score_detail": ["post_exit_scan"],
                        "entry_price":  c["no_price"],
                        "contracts":    contracts,
                        "placed_at":    datetime.now(timezone.utc).isoformat(),
                        "paper":        False,
                    })
                except Exception as e:
                    log.error("post_exit_scan order failed for %s: %s", c["ticker"], e)

    except Exception as e:
        log.error("post_exit_scan error for %s: %s", exited_ticker, e)


def check_exits(
    client:         KalshiClient,
    paper:          bool  = False,
    live_positions: list  = None,   # pass pre-fetched positions to skip redundant sync
    nws_snapshot:   dict  = None,   # pass pre-fetched NWS data to skip redundant fetch
) -> dict:
    """
    Check all open positions and trigger exits where appropriate.
    Sources positions directly from Kalshi — not from local positions.json.

    NO trades — three combined exit mechanisms:
      1. Time-weighted price stop: threshold tightens from 60% drop (morning)
         to 25% drop (afternoon) as observations firm up.
      2. Probability ceiling: exit if YES crosses 50¢ — thesis is dead
         regardless of our entry price.
      3. Forecast anchor: exit if the observed temperature comes within
         FORECAST_ANCHOR_BUFFER °F of the dangerous bracket boundary.
      Settlement hold override: if observed is SETTLEMENT_CLEAR_BUFFER °F
         clear of the bracket AND it's past SETTLEMENT_HOLD_HOUR, suppress
         all exits and hold to settlement.

    YES trades:
      - Stop loss when price falls YES_STOP_LOSS% from entry
      - No take profit — ride to resolution.

    Pass live_positions if already fetched this poll to avoid a redundant sync.
    Pass nws_snapshot if already fetched this poll to avoid a redundant NWS call.
    Returns dict of {ticker: reason} for each position that was exited.
    """
    exited = {}

    if live_positions is None:
        try:
            live_positions = sync_from_kalshi(client)
        except Exception as e:
            log.error("could not fetch live positions: %s", e)
            return exited

    if not live_positions:
        return exited

    # Fetch NWS snapshot once for all positions if not supplied by caller.
    if nws_snapshot is None:
        try:
            import nws_feed
            nws_snapshot = nws_feed.snapshot()
        except Exception as e:
            log.warning("NWS snapshot failed (anchor disabled): %s", e)
            nws_snapshot = {}

    tickers = [p["ticker"] for p in live_positions]
    prices  = {}
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
        log.error("batch price fetch failed: %s — skipping exit check", e)
        return exited

    from cities import SERIES_TO_CITY as _SERIES_TO_CITY

    for pos in live_positions:
        ticker    = pos["ticker"]

        # Safety guard — only manage temperature market positions
        if "HIGH" not in ticker and "LOWT" not in ticker:
            log.debug("skip %s — not a temperature market", ticker)
            continue

        side      = pos["side"]
        avg_cost  = pos["avg_cost"]
        contracts = pos["contracts"]

        if ticker not in prices:
            continue

        market    = prices[ticker]
        status    = market["status"]
        yes_price = market["yes_bid"]
        no_price  = market["no_bid"]

        if status not in ("active", "initialized"):
            continue

        exit_reason = None
        exit_price  = None

        if side == "no":
            # ── Resolve city and NWS data for this position ───────────────
            series      = ticker.split("-")[0]
            city        = _SERIES_TO_CITY.get(series)
            local_hour  = _city_local_hour(city) if city else 12
            is_lowt     = "LOWT" in ticker

            nws         = nws_snapshot.get(city, {}) if city else {}
            obs_high    = nws.get("observed_high_f")
            obs_low     = nws.get("observed_low_f")
            obs_val     = obs_low if is_lowt else obs_high

            floor, ceiling = _bracket_floor_ceiling(ticker)

            # ── Settlement hold override ───────────────────────────────────
            # If it's late AND the observed value is well clear of the
            # dangerous boundary, suppress all exits and hold to settlement.
            # This is a `continue` guard — if it fires, nothing below runs.
            # If it doesn't fire (obs not clear enough), all checks below
            # still run normally regardless of the hour.
            if local_hour >= SETTLEMENT_HOLD_HOUR and obs_val is not None:
                if not is_lowt and floor is not None:
                    # HIGH B bracket: safe if obs_high well below bracket floor
                    if (floor - obs_val) >= SETTLEMENT_CLEAR_BUFFER:
                        log.debug("HOLD_SETTLE  %s  " +
                              f"obs_high={obs_val}°F is "
                              f"{floor - obs_val:.1f}°F below bracket floor {floor}°F  "
                              f"(local={local_hour}h)")
                        continue
                elif not is_lowt and floor is None and ceiling is not None:
                    # HIGH bottom T: safe if obs_high well above ceiling
                    # (temp already exceeded the "below X" threshold → bracket dead)
                    if (obs_val - ceiling) >= SETTLEMENT_CLEAR_BUFFER:
                        log.debug("HOLD_SETTLE  %s  " +
                              f"obs_high={obs_val}°F is "
                              f"{obs_val - ceiling:.1f}°F above bracket ceiling {ceiling}°F  "
                              f"(local={local_hour}h)")
                        continue
                elif is_lowt and ceiling is not None:
                    # LOWT: safe if obs_low well above bracket ceiling
                    if (obs_val - ceiling) >= SETTLEMENT_CLEAR_BUFFER:
                        log.debug("HOLD_SETTLE  %s  " +
                              f"obs_low={obs_val}°F is "
                              f"{obs_val - ceiling:.1f}°F above bracket ceiling {ceiling}°F  "
                              f"(local={local_hour}h)")
                        continue

            # ── Probability ceiling (time-gated + forecast filter) ─────────
            # Only fires after NO_YES_CEILING_HOUR local time.
            # Additionally requires forecast to be within FORECAST_FLOOR_GAP_MAX
            # of the bracket floor — if forecast is well clear, the Yes spike
            # is almost certainly intraday noise that will revert by settlement.
            if local_hour >= NO_YES_CEILING_HOUR and yes_price >= NO_YES_CEILING:
                # Check forecast filter for B brackets
                ceiling_bypass = False
                if floor is not None:
                    corrected_fcst = nws.get("forecast_high_f")
                    if corrected_fcst is not None:
                        try:
                            from hight_decision_engine import _city_bias as _get_bias
                            corrected_fcst = corrected_fcst + _get_bias(city)
                        except Exception:
                            pass
                        fcst_floor_gap = corrected_fcst - floor
                        if fcst_floor_gap > FORECAST_FLOOR_GAP_MAX:
                            ceiling_bypass = True
                            log.debug("YES_CEILING_BYPASS  %s  "
                                      f"forecast {corrected_fcst:.1f}°F is {fcst_floor_gap:.1f}°F "
                                      f"above floor {floor}°F — Yes spike is noise, holding")

                if not ceiling_bypass:
                    exit_reason = "yes_ceiling"
                    exit_price  = no_price
                    log.info("YES_CEILING  %s  YES=%.2f >= %.2f  hour=%d  "
                             f"(entry NO=${avg_cost:.2f}  current NO=${no_price:.2f})",
                             ticker, yes_price, NO_YES_CEILING, local_hour)

            # ── Forecast anchor ────────────────────────────────────────────
            # Only fires at or after ANCHOR_MIN_HOUR local time.
            # Before the daily high has had a chance to establish (typically
            # peaks 14:00–16:00 local), a small gap is normal temperature
            # climbing and not a genuine danger signal.
            #
            # HIGH guard: only fire if obs_val < ceiling (bracket's upper edge).
            # If obs_high is already above the cap, the bracket is physically
            # eliminated — the day's high can't un-happen. Firing the anchor in
            # that case causes false exits on winning positions (Denver bug).
            # When obs >= cap, the settlement hold or normal expiry handles it.
            if exit_reason is None and obs_val is not None and local_hour >= ANCHOR_MIN_HOUR \
                    and yes_price >= ANCHOR_MIN_YES:
                anchor_triggered = False
                if not is_lowt and floor is not None:
                    # HIGH: dangerous if observed high approaching bracket floor,
                    # BUT only while obs hasn't yet cleared the bracket cap.
                    _cap = ceiling if ceiling is not None else round(floor + 2.0, 1)
                    if obs_val < _cap:
                        gap = floor - obs_val
                        if gap <= FORECAST_ANCHOR_BUFFER:
                            # Two-sided forecast bypass:
                            #
                            # Case A — forecast BELOW floor (bracket above forecast):
                            #   corrected_fcst < floor - buffer → temperature not expected
                            #   to reach the bracket at all → approaching floor is noise.
                            #
                            # Case B — forecast ABOVE cap (bracket below forecast):
                            #   corrected_fcst > cap + buffer → temperature expected to
                            #   blow THROUGH the bracket on its way up → approaching the
                            #   floor is the normal winning path, not a danger signal.
                            #   Only applies to B brackets (cap is not None).
                            #
                            # Only fire when forecast is ambiguous (inside bracket zone).
                            corrected_fcst = nws.get("forecast_high_f")
                            if corrected_fcst is not None:
                                from hight_decision_engine import _city_bias as _get_bias
                                try:
                                    corrected_fcst = corrected_fcst + _get_bias(city)
                                except Exception:
                                    pass

                                below_floor = corrected_fcst < floor - FORECAST_ANCHOR_BUFFER
                                above_cap   = (ceiling is not None and
                                               corrected_fcst > ceiling + FORECAST_ANCHOR_BUFFER)

                                if below_floor or above_cap:
                                    reason = "below floor" if below_floor else "above cap"
                                    log.debug("ANCHOR_BYPASS  %s  " +
                                          f"obs_high={obs_val}°F near floor {floor}°F BUT "
                                          f"corrected_fcst={corrected_fcst:.1f}°F is {reason} "
                                          f"— outcome already implied, holding")
                                else:
                                    anchor_triggered = True
                                    log.info("FORECAST_ANCHOR  %s  " +
                                          f"obs_high={obs_val}°F within {gap:.1f}°F of "
                                          f"bracket floor {floor}°F  "
                                          f"corrected_fcst={corrected_fcst:.1f}°F is ambiguous")
                            else:
                                # No forecast available — fire conservatively
                                anchor_triggered = True
                                log.info("FORECAST_ANCHOR  %s  " +
                                      f"obs_high={obs_val}°F within {gap:.1f}°F of "
                                      f"bracket floor {floor}°F (no forecast available)")
                elif is_lowt and ceiling is not None:
                    # LOWT: dangerous if observed low approaching (or below) bracket ceiling.
                    # Negative gap is correct here — obs already below cap means YES resolving.
                    gap = obs_val - ceiling
                    if gap <= FORECAST_ANCHOR_BUFFER:
                        anchor_triggered = True
                        log.info("FORECAST_ANCHOR  %s  " +
                              f"obs_low={obs_val}°F within {gap:.1f}°F of bracket ceiling {ceiling}°F")

                if anchor_triggered:
                    exit_reason = "forecast_anchor"
                    exit_price  = no_price

            # ── Price stop loss — DISABLED ─────────────────────────────────
            # Removed: data shows holding always beats exiting on price alone
            # at every streak length and threshold (analysis Apr 22 2026).
            # Only exit mechanism is YES ceiling + forecast filter above.

        else:
            # ── YES position: stop loss only, ride to resolution ───────────
            stop_loss = round(avg_cost * (1 - YES_STOP_LOSS), 2)

            if yes_price <= stop_loss:
                exit_reason = "stop_loss"
                exit_price  = yes_price
                log.warning("STOP LOSS  %s  YES $%.2f → $%.2f", ticker, avg_cost, yes_price)

        if exit_reason:
            if not paper:
                try:
                    place_order(
                        client        = client,
                        ticker        = ticker,
                        side          = side,
                        action        = "sell",
                        price_dollars = exit_price,
                        contracts     = contracts,
                        paper         = False,
                    )
                    exited[ticker] = "Stopped Out"
                    _exited_this_session.add(ticker)
                    log.info("EXIT  %s  SELL %s @ $%.2f  reason=%s", ticker, side.upper(), exit_price, exit_reason)

                    # Scan remaining brackets for new/top-up entries
                    if side == "no":
                        _post_exit_scan(client, ticker, live_positions, paper=False)
                except Exception as e:
                    log.error("exit order failed  %s: %s", ticker, e)
                    continue
            else:
                exited[ticker] = "Stopped Out"
                _exited_this_session.add(ticker)
                log.info("[PAPER] EXIT  %s  SELL %s @ $%.2f  reason=%s", ticker, side.upper(), exit_price, exit_reason)

                # Scan remaining brackets for new/top-up entries (paper)
                if side == "no":
                    _post_exit_scan(client, ticker, live_positions, paper=True)

        time.sleep(0.1)

    return exited


# ---------------------------------------------------------------------------
# Full pipeline: signals → execute
# ---------------------------------------------------------------------------

def _ticker_date(ticker: str):
    """
    Extract the settlement date from a Kalshi temperature ticker.
    Format: KXHIGHNY-26APR09-B70  ->  date(2026, 4, 9)
    Returns None if the date segment cannot be parsed.
    """
    try:
        parts = ticker.split("-")
        if len(parts) < 2:
            return None
        raw = parts[1]          # e.g. "26APR09"
        return datetime.strptime(raw, "%y%b%d").date()
    except (ValueError, IndexError):
        return None


def _city_local_date(city: str):
    """
    Return today's date in the city's local timezone.
    Falls back to UTC date if the city or timezone is not found.
    """
    from cities import CITIES as _CITIES
    tz_name = _CITIES.get(city, {}).get("tz")
    if tz_name:
        return datetime.now(ZoneInfo(tz_name)).date()
    return datetime.now(timezone.utc).date()


def run_pipeline(client: KalshiClient, city_filter: str = None, paper: bool = False):
    """Run HIGH and LOWT decision engines, then execute any actionable signals."""
    # ── HIGH markets ─────────────────────────────────────────────────────────
    # run() returns (evaluations, nws_snapshot) — snapshot reused by LOWT
    # to avoid a second full NWS sweep (60 API calls) each poll cycle.
    evaluations, nws_snapshot = decision_engine.run(city_filter=city_filter)
    decision_engine.display(evaluations)

    # ── LOWT markets ─────────────────────────────────────────────────────────
    try:
        lowt_evals = lowt_decision_engine.run(
            city_filter  = city_filter,
            paper        = paper,
            nws_snapshot = nws_snapshot,
        )
        lowt_decision_engine.display(lowt_evals)
        evaluations = evaluations + lowt_evals
    except Exception as e:
        log.warning("LOWT pipeline error (non-fatal): %s", e)

    try:
        balance                      = get_balance(client)
        deployable, cascade_reserve  = _update_day_snapshot(balance)
        log.info("balance: $%.2f  |  deployable: $%.2f  |  cascade_reserve: $%.2f",
                 balance, deployable, cascade_reserve)
    except Exception as e:
        log.warning("balance fetch failed: %s — using $0 cap", e)
        balance          = 0.0
        deployable       = 0.0
        cascade_reserve  = 0.0

    try:
        live_positions = sync_from_kalshi(client)
        # Track contracts held per ticker (for per-bracket headroom check)
        open_contracts = {p["ticker"]: p["contracts"] for p in live_positions}
        # Track positions held per city (for MAX_NO_PER_CITY gate)
        from cities import SERIES_TO_CITY as _SERIES_TO_CITY
        held_per_city: dict[str, int] = {}
        for ticker in open_contracts:
            city_name = _SERIES_TO_CITY.get(ticker.split("-")[0])
            if city_name:
                held_per_city[city_name] = held_per_city.get(city_name, 0) + 1
    except Exception:
        open_contracts = {}
        held_per_city  = {}

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

            # Same-ticker cooldown: never re-enter a ticker exited this session
            if ticker in _exited_this_session:
                log.debug("skip %s — cooldown", ticker)
                continue

            # Per-city cap: skip if already at MAX_NO_PER_CITY across all brackets.
            # Uses decision_engine.MAX_NO_PER_CITY (positions per city) — NOT
            # signal["max_contracts"] which is contracts per position.
            city_held = held_per_city.get(city, 0)
            if city_held >= decision_engine.MAX_NO_PER_CITY:
                log.debug("skip %s — %s already at MAX_NO_PER_CITY (%d/%d)",
                          ticker, city, city_held, decision_engine.MAX_NO_PER_CITY)
                continue
            contracts = contracts_for_signal(signal)
            side      = signal["trade_type"].lower()
            price     = signal["entry_price"]
            ticker    = signal["ticker"]

            held      = open_contracts.get(ticker, 0)
            max_contr = signal.get("max_contracts", 2)
            headroom  = max_contr - held
            if headroom <= 0:
                log.debug("skip %s — max contracts (%d/%d)", ticker, held, max_contr)
                continue
            contracts = min(contracts, headroom)

            # Only trade today's markets — compare against city local date, not UTC
            ticker_dt   = _ticker_date(ticker)
            today_local = _city_local_date(city)
            if ticker_dt is None:
                log.debug("skip %s — could not parse date", ticker)
                continue
            if ticker_dt != today_local:
                log.debug("skip %s — market date %s is not today (local: %s, city: %s)",
                          ticker, ticker_dt, today_local, city)
                continue

            cost = price * contracts

            # Cascade signals draw from the cascade reserve;
            # main engine signals draw from the main deployable budget
            is_cascade = signal.get("entry_tier", "").startswith("cascade")
            if is_cascade:
                if balance - cost < cascade_reserve:
                    log.debug("skip %s — cascade reserve would be breached "
                              "(balance=$%.2f  cost=$%.2f  reserve=$%.2f)",
                              ticker, balance, cost, cascade_reserve)
                    continue
            else:
                if deployed + cost > deployable:
                    log.debug("skip %s — would exceed deployable cap "
                              "(deployed=$%.2f + cost=$%.2f > $%.2f)",
                              ticker, deployed, cost, deployable)
                    continue

            log.info("SIGNAL  %s  %s", city, ticker)
            tier = signal.get("entry_tier", "")
            if tier.startswith("cascade"):
                detail = signal.get("trigger_info") or tier
            else:
                detail = ", ".join(signal.get("score_detail", [])) or "no_detail"

            log.info("  %s %dx @ $%.2f  score=%s/5  [%s]",
                     side.upper(), contracts, price,
                     signal.get("score", "?"),
                     detail)

            try:
                place_order(
                    client        = client,
                    ticker        = ticker,
                    side          = side,
                    price_dollars = price,
                    contracts     = contracts,
                    paper         = paper,
                )
                open_contracts[ticker] = open_contracts.get(ticker, 0) + contracts
                held_per_city[city]    = held_per_city.get(city, 0) + 1
                deployed += cost
                executed += 1
                _append_trade_log({
                    "ticker":       ticker,
                    "city":         city,
                    "side":         side,
                    "market_type":  signal.get("market_type", "high"),
                    "score":        signal.get("score", 0),
                    "score_detail": signal.get("score_detail", []),
                    "entry_price":  price,
                    "contracts":    contracts,
                    "placed_at":    datetime.now(timezone.utc).isoformat(),
                    "paper":        paper,
                })

            except Exception as e:
                log.error("order failed  %s: %s", ticker, e)

    log.info("%d order(s) placed", executed)
    return evaluations


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kalshi weather trader")
    parser.add_argument("--balance",    action="store_true", help="Show account balance")
    parser.add_argument("--positions",  action="store_true", help="Show open positions")
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

    if args.balance or args.positions or args.run or args.monitor or args.test_order:
        client = make_client()

        if args.balance:
            bal = get_balance(client)
            log.info("balance: $%.2f", bal)

        if args.positions:
            positions = sync_from_kalshi(client)
            if not positions:
                log.info("no open positions")
            else:
                print(f"\n  {'Ticker':<28} {'Side':>5} {'Qty':>4} "
                      f"{'AvgCost':>8} {'Current':>8} {'UnrealPnL':>10}")
                print(f"  {'-'*70}")
                for p in positions:
                    sign = "+" if p["unrealised_pnl"] >= 0 else ""
                    print(f"  {p['ticker']:<28} {p['side'].upper():>5} "
                          f"{p['contracts']:>4} ${p['avg_cost']:.2f}   "
                          f"${p['current_price']:.2f}   "
                          f"{sign}${p['unrealised_pnl']:.2f}")

        if args.test_order:
            test_order(client)

        if args.run:
            run_pipeline(client, city_filter=args.city, paper=args.paper)

        if args.monitor:
            log.info("exit monitor running — polling every %ds", MONITOR_INTERVAL)
            while True:
                log.info('checking exits...')
                check_exits(client, paper=args.paper)
                time.sleep(MONITOR_INTERVAL)

    else:
        parser.print_help()
