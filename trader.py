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
import threading
import cascade_engine
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

# Cross-platform advisory file lock. fcntl is POSIX-only (the Pi, where this
# actually runs live) — msvcrt covers Windows dev-environment usage (the
# stated Windows/PyCharm -> GitHub -> Pi workflow) so this module still
# imports cleanly there even though real trading never happens there.
try:
    import fcntl

    def _lock_file(f):
        fcntl.flock(f, fcntl.LOCK_EX)

    def _unlock_file(f):
        fcntl.flock(f, fcntl.LOCK_UN)
except ImportError:                                        # pragma: no cover
    import msvcrt

    def _lock_file(f):
        f.seek(0)
        msvcrt.locking(f.fileno(), msvcrt.LK_LOCK, 1)

    def _unlock_file(f):
        f.seek(0)
        try:
            msvcrt.locking(f.fileno(), msvcrt.LK_UNLCK, 1)
        except OSError:
            pass

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
import kalshi_scanner

from log_setup import get_logger
log = get_logger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DEMO_BASE_URL = "https://demo-api.kalshi.co/trade-api/v2"
PROD_BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"

# Max contracts per single order — hard safety cap
MAX_CONTRACTS_PER_ORDER = 10

# Global cap: maximum total contracts held across ALL engines on any single
# ticker, regardless of side (a 5-contract NO position and a 3-contract YES
# position on the same ticker count as 8 combined, not two separate pools).
#
# BUG FOUND (this session): this comment used to claim the cap "applies in
# run_pipeline before every order regardless of engine" — that was never
# true. run_pipeline (HIGH+LOWT combined pipeline) is the ONLY one of six
# concurrently-running engines (see scheduler.py's ThreadPoolExecutor:
# run_pipeline, sweep_engine, peak_scanner, evening_convergence,
# cascade_engine, hourly_nyc) that ever checked this constant. The other
# four call place_order() directly, each with their own small per-signal
# cap (3-4 contracts) but zero awareness of this constant, zero awareness
# of each other, and zero awareness of what run_pipeline is doing in a
# different thread at the same moment. Four engines each independently and
# correctly respecting their OWN 3-4 contract limit can still sum to well
# past this "global" cap if they all like the same bracket in the same poll
# cycle — confirmed directly against live fills: KXHIGHTSEA-26JUN08-B61.5
# (5 orders, 22 contracts, 68 seconds) and KXHIGHAUS-26JUN21-B92.5 (5
# orders, 20 contracts, ~2 minutes).
#
# FIX: place_order() itself now enforces this, under _order_lock, using
# _session_contracts_committed as the shared running total. Since all six
# engines are threads in one process (not separate processes), a plain
# threading.Lock is sufficient — no cross-process coordination needed.
# This protects every engine automatically, including the four that never
# checked this constant before, without requiring any change to their
# code. run_pipeline's own pre-existing live-position + resting-order check
# is left in place as a second, independent layer — it catches persisted
# resting orders across a scheduler restart, which this in-memory counter
# (reset to empty on every process start) has no way to see.
GLOBAL_MAX_CONTRACTS_PER_TICKER = 7

# Shared, cross-engine order-placement lock and running-total tracker.
# ALL writes to _session_contracts_committed must happen while holding
# _order_lock — see place_order()'s enforcement block below. Cleared only
# by process restart; harmless to accumulate dead tickers over a session's
# lifetime (each is one string key + one int, and temperature-market
# tickers are never reused across days).
#
# Two separate counters, real and paper, sharing the same lock and the same
# cap logic: paper orders must never affect real headroom or vice versa,
# but paper mode still needs to exercise the ACTUAL enforcement code path
# (not skip it) so it can be used to verify this fix against live capital
# risk before deploying to real trading.
_order_lock = threading.Lock()
_session_contracts_committed: dict[str, int] = {}
_paper_contracts_committed:   dict[str, int] = {}

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

TRADE_LOG_FILE     = Path("data/trade_log.json")
ENGINE_CAPITAL_FILE = Path("data/engine_capital_deployed.json")


def _locked_json_rmw(path: Path, mutate_fn, default):
    """
    Read-modify-write a JSON file while holding an exclusive OS-level lock
    for the WHOLE read+mutate+write cycle — not just the write.

    WHY THIS EXISTS: scheduler.py runs 6 engines concurrently as threads in
    one process via ThreadPoolExecutor(max_workers=6), every poll cycle —
    not an occasional edge case, the normal operating mode. Manual
    invocations (e.g. `python3 sweep_engine.py --paper`) add a fully
    separate OS process on top of that. Every engine's _append_trade_log
    call did an unsynchronized read-modify-write on the SAME file
    (data/trade_log.json): read the whole list, append, write the whole
    list back. If two callers' cycles overlap, the second writer's read
    predates the first writer's write, so the second write silently
    discards the first entry — a genuinely lost trade record, with no
    trace left behind to detect it after the fact. flock() serializes the
    whole cycle across threads AND processes (POSIX advisory locks are
    respected process-wide, not just within one interpreter), closing that
    window. See tools/audit_trade_log_today.py / audit_trade_log_vs_kalshi.py
    for how to check whether this already caused a loss historically.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        path.write_text(json.dumps(default))

    with open(path, "r+") as f:
        _lock_file(f)
        try:
            f.seek(0)
            raw = f.read()
            try:
                data = json.loads(raw) if raw.strip() else default
            except (json.JSONDecodeError, ValueError):
                data = default
            data = mutate_fn(data)
            f.seek(0)
            f.truncate()
            json.dump(data, f, indent=2, default=str)
            f.flush()
            os.fsync(f.fileno())
            return data
        finally:
            _unlock_file(f)


def _append_trade_log(entry: dict):
    """
    Append one trade entry to data/trade_log.json.

    Each entry captures the signal metadata at the moment of order placement
    so outcomes can later be joined against settlements by ticker.

    Fields saved:
      ticker, city, side, market_type, score, score_detail,
      entry_price, contracts, placed_at (UTC ISO), paper (bool)

    Locked end-to-end via _locked_json_rmw — see that function's docstring
    for why this matters (concurrent engines were previously able to lose
    each other's entries silently).
    """
    def _mutate(existing: list) -> list:
        existing.append(entry)
        return existing

    _locked_json_rmw(TRADE_LOG_FILE, _mutate, default=[])


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
                        log.warning("HTTP %d: retrying in %ds (attempt %d/%d)",
                                    resp.status_code, delay, attempt + 1, MAX_RETRIES)
                        time.sleep(delay)
                        continue
                resp.raise_for_status()
                return resp

            except requests.exceptions.Timeout:
                if attempt < MAX_RETRIES - 1:
                    delay = BASE_DELAY * (2 ** attempt)
                    log.warning("timeout: retrying in %ds (attempt %d/%d)",
                                delay, attempt + 1, MAX_RETRIES)
                    time.sleep(delay)
                else:
                    raise

            except requests.exceptions.ConnectionError:
                if attempt < MAX_RETRIES - 1:
                    delay = BASE_DELAY * (2 ** attempt)
                    log.warning("connection error: retrying in %ds (attempt %d/%d)",
                                delay, attempt + 1, MAX_RETRIES)
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


# ---------------------------------------------------------------------------
# Engine capital manager
# ---------------------------------------------------------------------------

class EngineCapital:
    """
    Proportional capital allocator across trading engines.

    Fetches the live Kalshi balance once per instance (or on demand),
    then tracks deployed capital per engine against each engine's share.

    Usage:
        cap = EngineCapital(client)
        if cap.can_deploy("cascade", cost=2.70):
            place_order(...)
            cap.record("cascade", 2.70)

    The instance is recreated each poll cycle so balances stay fresh.
    """

    def __init__(self, client=None, balance: float = None):
        if balance is not None:
            self._balance = balance
        elif client is not None:
            try:
                self._balance = get_balance(client)
            except Exception as e:
                log.warning("EngineCapital: balance fetch failed: %s — using 0", e)
                self._balance = 0.0
        else:
            self._balance = 0.0

        # Per-engine budget = balance * share
        self._budget: dict[str, float] = {
            engine: round(self._balance * share, 4)
            for engine, share in ENGINE_ALLOCATIONS.items()
        }

        # Per-engine deployed today — loaded from disk so restarts don't reset budget
        self._deployed: dict[str, float] = self._load_deployed()

        log.debug(
            "EngineCapital: balance=$%.2f  "
            "main=$%.2f  cascade=$%.2f  sweep=$%.2f  "
            "peak=$%.2f  topup=$%.2f  econv=$%.2f  lowt=$%.2f",
            self._balance,
            self._budget.get("main",    0),
            self._budget.get("cascade", 0),
            self._budget.get("sweep",   0),
            self._budget.get("peak",    0),
            self._budget.get("topup",   0),
            self._budget.get("econv",   0),
            self._budget.get("lowt",    0),
        )

    @property
    def balance(self) -> float:
        return self._balance

    def budget(self, engine: str) -> float:
        """Total budget allocated to this engine."""
        return self._budget.get(engine, 0.0)

    def remaining(self, engine: str) -> float:
        """Remaining undeployed budget for this engine."""
        return max(0.0, self._budget.get(engine, 0.0) - self._deployed.get(engine, 0.0))

    def can_deploy(self, engine: str, cost: float) -> bool:
        """Return True if engine has enough remaining budget to cover cost."""
        if cost <= 0:
            return True
        rem = self.remaining(engine)
        ok  = rem >= cost
        if not ok:
            log.debug(
                "EngineCapital: %s cannot deploy $%.2f "
                "(budget=$%.2f  deployed=$%.2f  remaining=$%.2f)",
                engine, cost,
                self._budget.get(engine, 0),
                self._deployed.get(engine, 0),
                rem,
            )
        return ok

    @staticmethod
    def _load_deployed() -> dict[str, float]:
        """Load today's deployed amounts from disk. Returns zeros if file absent or stale."""
        from datetime import date as _date
        try:
            ENGINE_CAPITAL_FILE.parent.mkdir(parents=True, exist_ok=True)
            if ENGINE_CAPITAL_FILE.exists():
                data = json.loads(ENGINE_CAPITAL_FILE.read_text())
                if data.get("date") == str(_date.today()):
                    deployed = data.get("deployed", {})
                    log.debug("EngineCapital: loaded deployed from disk: %s", deployed)
                    return {e: float(deployed.get(e, 0.0)) for e in ENGINE_ALLOCATIONS}
        except Exception as e:
            log.warning("EngineCapital: could not load deployed from disk: %s", e)
        return {e: 0.0 for e in ENGINE_ALLOCATIONS}

    def _save_deployed(self) -> None:
        """
        Persist today's deployed amounts to disk.

        Lock-protects the WRITE against torn/interleaved output if another
        process writes concurrently. This is a narrower fix than
        _append_trade_log's — within one process, all 6 scheduler threads
        share the SAME EngineCapital instance and _deployed dict (it's a
        module-level singleton via get_engine_capital()), so in-process
        increments are already correctly accumulated before this is called.
        The remaining gap is cross-PROCESS: two fully separate live (non-
        paper) processes each holding their own independently-loaded
        _deployed dict could still each write a version that doesn't
        include the other's increments — the lock prevents a corrupted/
        garbled file from concurrent writes, but doesn't merge two
        divergent in-memory totals. That would need a real transactional
        store (e.g. a SQLite row with an atomic UPDATE ... SET x = x + ?)
        rather than a flat JSON snapshot — not done here; flagging it as a
        known remaining limitation rather than claiming this fully closes
        the gap for that specific scenario.
        """
        from datetime import date as _date
        try:
            ENGINE_CAPITAL_FILE.parent.mkdir(parents=True, exist_ok=True)
            if not ENGINE_CAPITAL_FILE.exists():
                ENGINE_CAPITAL_FILE.write_text("{}")
            with open(ENGINE_CAPITAL_FILE, "r+") as f:
                _lock_file(f)
                try:
                    f.seek(0)
                    f.truncate()
                    json.dump({
                        "date":     str(_date.today()),
                        "deployed": self._deployed,
                    }, f)
                    f.flush()
                    os.fsync(f.fileno())
                finally:
                    _unlock_file(f)
        except Exception as e:
            log.warning("EngineCapital: could not save deployed to disk: %s", e)

    def record(self, engine: str, cost: float) -> None:
        """Record capital deployed by an engine and persist to disk."""
        if engine not in self._deployed:
            self._deployed[engine] = 0.0
        self._deployed[engine] = round(self._deployed[engine] + cost, 4)
        self._save_deployed()
        log.debug(
            "EngineCapital: %s deployed $%.2f  "
            "(total_deployed=$%.2f  remaining=$%.2f)",
            engine, cost,
            self._deployed[engine],
            self.remaining(engine),
        )

    def summary(self) -> str:
        parts = []
        for e in ENGINE_ALLOCATIONS:
            parts.append(
                f"{e}=${self.remaining(e):.2f}/{self._budget.get(e,0):.2f}"
            )
        return "  ".join(parts)


# Module-level singleton — persists across polls, resets at midnight.
# _deployed is preserved; only _balance and _budget are refreshed each poll.
_engine_capital: EngineCapital | None = None
_engine_capital_date: object = None   # tracks last reset date


def get_engine_capital(client=None) -> EngineCapital:
    """
    Return the persistent EngineCapital singleton.

    On each call:
      - If the date has changed (midnight), reset deployed tracking
      - If a client is provided, refresh balance and recompute budgets
        WITHOUT resetting the deployed amounts (so spending accumulates)
      - If no client and instance exists, return existing instance as-is
    """
    from datetime import date as _date
    global _engine_capital, _engine_capital_date

    today = _date.today()

    # Midnight reset — new day, fresh deployed counters
    if _engine_capital_date != today:
        _engine_capital = None
        _engine_capital_date = today

    if _engine_capital is None:
        # First call of the day — create fresh instance
        _engine_capital = EngineCapital(client=client)
        return _engine_capital

    if client is not None:
        # Refresh balance and budgets, but preserve deployed amounts
        try:
            new_balance = get_balance(client)
        except Exception as e:
            log.warning("EngineCapital: balance refresh failed: %s", e)
            return _engine_capital

        _engine_capital._balance = new_balance
        _engine_capital._budget = {
            engine: round(new_balance * share, 4)
            for engine, share in ENGINE_ALLOCATIONS.items()
        }
        # Note: _deployed is NOT reset — spending accumulates across polls

    return _engine_capital

def get_balance(client: KalshiClient) -> float:
    """Returns account balance in dollars."""
    data    = client.get("portfolio/balance")
    balance = data.get("balance", 0)
    return balance / 100   # Kalshi returns cents


# Module-level balance cache — updated by run_pipeline each poll,
# read by hight_decision_engine.kelly_contracts() without extra API calls.
_cached_balance: float | None = None

def get_balance_cached() -> float | None:
    """Return the most recently fetched balance, or None if not yet set."""
    return _cached_balance

def set_balance_cached(balance: float) -> None:
    """Called by run_pipeline after each balance fetch to keep the cache fresh."""
    global _cached_balance
    _cached_balance = balance


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

    Fallback priority for one-sided books:
      no_bid missing  → use (1 - yes_ask) if available, else (1 - yes_bid)
      yes_bid missing → use (1 - no_ask)  if available, else (1 - no_bid)
    This matches Kalshi's own portfolio mark-to-market more closely than
    the naive (1 - yes_bid) fallback, which overstates the bid-ask spread.
    """
    result  = (m.get("result") or "").lower()
    status  = m.get("status", "active")

    if result == "yes":
        return {"yes_bid": 0.99, "no_bid": 0.01, "status": status, "result": result}
    if result == "no":
        return {"yes_bid": 0.01, "no_bid": 0.99, "status": status, "result": result}

    yes_bid = float(m.get("yes_bid_dollars") or 0)
    no_bid  = float(m.get("no_bid_dollars")  or 0)
    yes_ask = float(m.get("yes_ask_dollars") or 0)
    no_ask  = float(m.get("no_ask_dollars")  or 0)

    if yes_bid == 0 and no_bid == 0:
        lp_cents = float(m.get("last_price") or 0)
        if 1 <= lp_cents <= 99:
            yes_bid = round(lp_cents / 100, 4)
            no_bid  = round(1.0 - yes_bid, 4)

    if yes_bid > 0 and no_bid == 0:
        # Prefer (1 - yes_ask) over (1 - yes_bid): yes_ask <= yes_bid so
        # this gives a slightly lower (more conservative) No mark, matching
        # how Kalshi values No positions in its own portfolio view.
        no_bid = round(1.0 - yes_ask, 4) if yes_ask > 0 else round(1.0 - yes_bid, 4)
    elif no_bid > 0 and yes_bid == 0:
        yes_bid = round(1.0 - no_ask, 4) if no_ask > 0 else round(1.0 - no_bid, 4)

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

        # Skip settled/finalized markets — Kalshi keeps positions indefinitely
        # in portfolio/positions even after resolution. Only show live markets.
        # Known live statuses: "active", "initialized", "open"
        # Known dead statuses: "finalized", "settled", "determined", "closed"
        market_status = (prices.get(ticker, {}).get("status") or "").lower()
        if market_status and market_status in ("finalized", "settled", "determined", "closed"):
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
            # market_exposure_dollars = actual filled cost (what we paid).
            # total_traded_dollars = total order volume including cancelled/repriced
            # orders that never filled — do NOT use it for avg_cost.
            exposure = float(pos.get("market_exposure_dollars") or 0)
            if exposure > 0:
                avg_cost = round(exposure / contracts, 4)

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

    CROSS-ENGINE CAP ENFORCEMENT (added this session — see
    GLOBAL_MAX_CONTRACTS_PER_TICKER's comment above for the full incident
    writeup): this is the ONE function every engine's order eventually
    passes through, so it's the one place a genuinely global cap can live.
    For "buy" actions, contracts are clamped to whatever headroom remains
    under GLOBAL_MAX_CONTRACTS_PER_TICKER — combined across BOTH sides of
    the ticker, tracked across ALL engines, reserved atomically under
    _order_lock before any network call is made. A successful "sell"
    releases headroom (reduces the committed total) since it shrinks real
    exposure.

    paper=True orders use a SEPARATE counter (_paper_contracts_committed,
    never _session_contracts_committed) — paper exposure must never
    consume or release real headroom, or vice versa. But paper orders DO
    go through the same cap logic as real ones, deliberately: this is what
    makes it possible to verify this exact enforcement mechanism, against
    the real code path, using paper mode — no code path is skipped for
    paper other than the final Kalshi API call itself. A rejected real
    order rolls back its reservation; a paper order can't be "rejected"
    (there's no real order to reject), so paper sells release headroom
    immediately and paper buys are never rolled back.
    """
    contracts      = int(contracts)
    reserved       = 0
    committed_dict = _paper_contracts_committed if paper else _session_contracts_committed

    if action == "buy" and contracts > 0:
        with _order_lock:
            committed = committed_dict.get(ticker, 0)
            headroom  = GLOBAL_MAX_CONTRACTS_PER_TICKER - committed
            if headroom <= 0:
                log.warning(
                    "place_order: BLOCKED %s%s — global cap reached "
                    "(%d/%d contracts already committed this session, "
                    "requested %d more)",
                    "[PAPER] " if paper else "", ticker, committed,
                    GLOBAL_MAX_CONTRACTS_PER_TICKER, contracts,
                )
                return {"blocked": True, "reason": "global_cap_reached",
                        "committed": committed, "cap": GLOBAL_MAX_CONTRACTS_PER_TICKER}
            if contracts > headroom:
                log.info(
                    "place_order: REDUCED %s%s from %d to %d contracts "
                    "(global cap: %d/%d already committed)",
                    "[PAPER] " if paper else "", ticker, contracts, headroom,
                    committed, GLOBAL_MAX_CONTRACTS_PER_TICKER,
                )
                contracts = headroom
            reserved = contracts
            committed_dict[ticker] = committed + reserved

    # ── Kalshi v2 order API ─────────────────────────────────────────────────
    # Everything is quoted from the YES side:
    #   bid = buy YES  (holding YES position)
    #   ask = sell YES (holding NO position — economically equiv to buying NO)
    # Price is YES price in dollars (string).
    yes_price = price_dollars if side == "yes" else (1.0 - price_dollars)

    if action == "buy":
        v2_side = "bid" if side == "yes" else "ask"
    else:  # sell / exit
        v2_side = "ask" if side == "yes" else "bid"

    order = {
        "ticker":                    ticker,
        "client_order_id":           f"kw-exit-{uuid.uuid4().hex[:8]}" if action == "sell"
                                     else f"kw-{uuid.uuid4().hex[:12]}",
        "side":                      v2_side,
        "count":                     f"{contracts:.2f}",
        "price":                     f"{yes_price:.4f}",
        "time_in_force":             "good_till_canceled",
        "self_trade_prevention_type": "taker_at_cross",
    }

    if paper:
        if action == "sell" and contracts > 0:
            # Paper mode has no real API call to succeed/fail against — a
            # paper sell always "succeeds", so release its headroom here,
            # not after a client.post() call that will never happen for
            # this branch. This MUST happen before the early return below.
            with _order_lock:
                _paper_contracts_committed[ticker] = max(
                    0, _paper_contracts_committed.get(ticker, 0) - contracts
                )
        log.info("[PAPER] %s %s %dx %s @ $%.2f", action.upper(), side.upper(), contracts, ticker, price_dollars)
        return {"paper": True, "order": order}

    try:
        raw = client.post("portfolio/events/orders", order)

        if action == "sell" and contracts > 0:
            # Confirmed success only — a rejected sell (caught below) must
            # NOT release headroom, since nothing was actually reduced.
            with _order_lock:
                _session_contracts_committed[ticker] = max(
                    0, _session_contracts_committed.get(ticker, 0) - contracts
                )

        # v2 response is flat {order_id, fill_count, remaining_count, ...}
        # Wrap in {"order": ...} for backwards compatibility with callers
        if "order_id" in raw and "order" not in raw:
            return {"order": raw}
        return raw
    except requests.exceptions.HTTPError as e:
        if reserved:
            # Buy was rejected at Kalshi — release the reservation so it
            # doesn't permanently eat into headroom it never actually used.
            with _order_lock:
                _session_contracts_committed[ticker] = max(
                    0, _session_contracts_committed.get(ticker, 0) - reserved
                )
            log.info("place_order: rolled back reservation of %d for %s "
                      "after order failure", reserved, ticker)
        try:
            detail = e.response.json()
        except Exception:
            detail = e.response.text
        raise RuntimeError(f"Order failed {e.response.status_code}: {detail}") from e
    except Exception:
        if reserved:
            with _order_lock:
                _session_contracts_committed[ticker] = max(
                    0, _session_contracts_committed.get(ticker, 0) - reserved
                )
            log.info("place_order: rolled back reservation of %d for %s "
                      "after unexpected failure", reserved, ticker)
        raise


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
        "ticker":                    ticker,
        "client_order_id":           f"kw-test-{uuid.uuid4().hex[:8]}",
        "side":                      "bid",
        "count":                     "1.00",
        "price":                     "0.0100",
        "time_in_force":             "good_till_canceled",
        "self_trade_prevention_type": "taker_at_cross",
    }

    try:
        result   = client.post("portfolio/events/orders", order_body)
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
# ── Top-up parameters ────────────────────────────────────────────────────────
TOPUP_HOUR_START    = 13     # local hour — top-up window opens
TOPUP_HOUR_END      = 15     # local hour — top-up window closes (inclusive)
YES_TOPUP_MAX       = 0.30   # max Yes price seen during window to qualify
                              # backtest: <=0.30 gives 98.6% WR, >0.40 turns negative EV
TOPUP_MAX_CONTRACTS = 3      # additional contracts per top-up signal
TOPUP_TOTAL_CAP     = 7      # max total contracts across all engines on a single ticker
                              # aligned with GLOBAL_MAX_CONTRACTS_PER_TICKER
NO_TOPUP_MAX_PRICE  = 0.91   # don't top up if No has drifted above this

NO_YES_CEILING      = 0.60   # retained for reference — ceiling exit DISABLED (see check_exits)
NO_YES_CEILING_HOUR = 15     # retained for reference

# If Yes is at or above this price the market has effectively closed — no
# No-side liquidity. Skip exit attempts to avoid spamming unfillable orders.
YES_MARKET_CLOSED   = 0.97

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
_topup_done:          set[str] = set()   # tickers already topped up this session
_yes_peaks:           dict     = {}      # ticker -> max Yes price seen today
_main_entered:        set[str] = set()   # tickers entered by main engine this session
                                         # prevents re-entry every poll due to fill latency

# ---------------------------------------------------------------------------
# Daily capital snapshot
# Taken once at the first poll of each day. Used to compute a fixed main-
# engine budget and a hard cascade reserve that survive repeated polling.
# ---------------------------------------------------------------------------
from datetime import date as _date

_day_open_balance: float      = 0.0
_day_open_date:    _date|None = None
_deployed_cascade: float      = 0.0   # cascade engine — persists across polls, resets midnight
_deployed_peak:    float      = 0.0   # peak scanner   — persists across polls, resets midnight
_deployed_tomorrow: float     = 0.0   # retained for legacy log compat — budget now in sweep
_deployed_sweep:    float      = 0.0   # unified sweep engine — persists across polls, resets midnight
_deployed_econv:   float      = 0.0   # evening convergence — persists across polls, resets midnight
_deployed_lowt:    float      = 0.0   # LOWT engine — persists across polls, resets midnight
# ---------------------------------------------------------------------------
# Engine capital allocation — proportional by proven EV/dollar
# ---------------------------------------------------------------------------
ENGINE_ALLOCATIONS: dict[str, float] = {
    "main":     0.10,   # HIGH main engine (post-dedup fix: ~$7/day actual need)
    "cascade":  0.25,   # HIGH cascade — highest proven EV
    "sweep":    0.20,   # unified sweep: directional + near-dead + dead bracket
    "peak":     0.08,   # intraday peak confirmation
    "topup":    0.03,   # augments existing positions (rarely fires)
    "econv":    0.03,   # evening convergence (rarely fires)
    "hourly":   0.03,   # NYC hourly temperature
    "lowt":     0.12,   # LOWT structural elimination + forecast distance
    # 16% unallocated buffer
}

# Legacy constant retained for backward compat
CASCADE_RESERVE:   float      = 30.00


def _update_day_snapshot(current_balance: float) -> tuple[float, float]:
    """
    Refresh the daily snapshot if the date has changed.
    Returns (main_deployable, cascade_reserve) based on day-open balance.

    All per-engine _deployed_* trackers reset at midnight so budgets are
    based on day-open balance — not disturbed by intraday winning settlements.
    """
    global _day_open_balance, _day_open_date, \
           _deployed_cascade, _deployed_peak, _deployed_tomorrow, _deployed_econv,\
           _deployed_lowt, _deployed_sweep
    today = _date.today()
    if _day_open_date != today or _day_open_balance == 0.0:
        _day_open_balance   = current_balance
        _day_open_date      = today
        _deployed_cascade   = 0.0
        _deployed_peak      = 0.0
        _deployed_tomorrow  = 0.0
        _deployed_econv     = 0.0
        _deployed_lowt      = 0.0
        _deployed_sweep     = 0.0
        log.info(
            "day snapshot: $%.2f  "
            "(main=$%.2f  cascade=$%.2f  peak=$%.2f  sweep=$%.2f  econv=$%.2f  hourly=$%.2f)",
            current_balance,
            round(current_balance * ENGINE_ALLOCATIONS["main"],    2),
            round(current_balance * ENGINE_ALLOCATIONS["cascade"], 2),
            round(current_balance * ENGINE_ALLOCATIONS["peak"],    2),
            round(current_balance * ENGINE_ALLOCATIONS["sweep"],   2),
            round(current_balance * ENGINE_ALLOCATIONS["econv"],   2),
            round(current_balance * ENGINE_ALLOCATIONS["hourly"],  2),
        )

    return 0.0, CASCADE_RESERVE   # main_deployable no longer used — EngineCapital governs


def get_cascade_deployable() -> float:
    """Remaining cascade budget for this session."""
    budget = round(_day_open_balance * ENGINE_ALLOCATIONS["cascade"], 2)
    return max(0.0, round(budget - _deployed_cascade, 2))


def record_cascade_deployed(cost: float) -> None:
    """Record capital deployed by the cascade engine."""
    global _deployed_cascade
    _deployed_cascade = round(_deployed_cascade + cost, 4)
    log.debug("cascade deployed: +$%.2f  (total=$%.2f  remaining=$%.2f)",
              cost, _deployed_cascade, get_cascade_deployable())


def get_peak_deployable() -> float:
    """Remaining peak scanner budget for this session."""
    budget = round(_day_open_balance * ENGINE_ALLOCATIONS["peak"], 2)
    return max(0.0, round(budget - _deployed_peak, 2))


def record_peak_deployed(cost: float) -> None:
    """Record capital deployed by the peak scanner."""
    global _deployed_peak
    _deployed_peak = round(_deployed_peak + cost, 4)
    log.debug("peak deployed: +$%.2f  (total=$%.2f  remaining=$%.2f)",
              cost, _deployed_peak, get_peak_deployable())


def get_tomorrow_deployable(ticker: str = None) -> float:
    """Remaining sweep budget for this session (legacy name retained for compat)."""
    budget = round(_day_open_balance * ENGINE_ALLOCATIONS["sweep"], 2)
    return max(0.0, round(budget - _deployed_sweep, 2))


def record_tomorrow_deployed(cost: float, ticker: str = None) -> None:
    """Record capital deployed by the tomorrow scanner."""
    global _deployed_tomorrow
    _deployed_tomorrow = round(_deployed_tomorrow + cost, 4)
    log.debug("tomorrow deployed: +$%.2f  (total=$%.2f  remaining=$%.2f)",
              cost, _deployed_tomorrow, get_tomorrow_deployable())


def get_econv_deployable() -> float:
    """Remaining evening convergence budget for this session."""
    budget = round(_day_open_balance * ENGINE_ALLOCATIONS["econv"], 2)
    return max(0.0, round(budget - _deployed_econv, 2))


def record_econv_deployed(cost: float) -> None:
    """Record capital deployed by the evening convergence engine."""
    global _deployed_econv
    _deployed_econv = round(_deployed_econv + cost, 4)
    log.debug("econv deployed: +$%.2f  (total=$%.2f  remaining=$%.2f)",
              cost, _deployed_econv, get_econv_deployable())


def get_lowt_deployable() -> float:
    """Remaining LOWT budget for today (based on day-open balance, not live balance)."""
    budget = round(_day_open_balance * ENGINE_ALLOCATIONS["lowt"], 2)
    return max(0.0, round(budget - _deployed_lowt, 2))


def record_lowt_deployed(cost: float) -> None:
    """Record capital deployed by the LOWT engine."""
    global _deployed_lowt
    _deployed_lowt = round(_deployed_lowt + cost, 4)
    log.debug("lowt deployed: +$%.2f  (total=$%.2f  remaining=$%.2f)",
              cost, _deployed_lowt, get_lowt_deployable())


def get_sweep_deployable() -> float:
    """Remaining sweep budget for today (based on day-open balance)."""
    budget = round(_day_open_balance * ENGINE_ALLOCATIONS["sweep"], 2)
    return max(0.0, round(budget - _deployed_sweep, 2))


def record_sweep_deployed(cost: float) -> None:
    """Record capital deployed by the unified sweep engine."""
    global _deployed_sweep
    _deployed_sweep = round(_deployed_sweep + cost, 4)
    log.debug("sweep deployed: +$%.2f  (total=$%.2f  remaining=$%.2f)",
              cost, _deployed_sweep, get_sweep_deployable())


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
    """Return the current local hour for a city. Falls back to UTC hour."""
    try:
        from cities import CITIES as _CITIES
        tz_name = _CITIES.get(city, {}).get("tz")
        if tz_name:
            return datetime.now(ZoneInfo(tz_name)).hour
    except Exception:
        pass
    return datetime.now(timezone.utc).hour


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
            if not (decision_engine.NO_MIN_ENTRY_PRICE <= no_p <= decision_engine.NO_MAX_ENTRY_PRICE):
                continue
            if yes_p > decision_engine.NO_MAX_YES_PRICE:
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
                        "entry_tier":   "post_exit_scan",
                    })
                except Exception as e:
                    log.error("post_exit_scan order failed for %s: %s", c["ticker"], e)

    except Exception as e:
        log.error("post_exit_scan error for %s: %s", exited_ticker, e)


# ---------------------------------------------------------------------------
# Open order management
# ---------------------------------------------------------------------------

# Backstop only — the price-based reprice/cancel logic below should handle
# almost everything within 1-2 poll cycles (~3-6 min at the observed
# cadence). This just bounds worst-case staleness for anything that defeats
# the price-based logic, known or not yet discovered.
STALE_ORDER_MAX_AGE_MIN = 20

def _no_ask_price(bracket: dict) -> float:
    """
    Ask-preferring price reference, used ONLY by manage_open_orders — NOT a
    replacement for market_utils.no_price(), which correctly prefers bid for
    every other caller (entry signal evaluation wants a conservative
    reference price, which bid provides).

    WHY THIS EXISTS: manage_open_orders uses its price reference to answer
    "has the market moved past our resting order, and what would it cost to
    chase it". For that specific decision, bid-preference has a dangerous
    failure mode: once a partial fill consumes available ask-side liquidity,
    OUR OWN remaining resting contracts can become the best (or only) bid on
    the book. no_price() would then read our own stale order back as "the
    current market price" — the reprice check compares our order's price
    against itself, always finds zero movement, and the order is left
    resting indefinitely. This is a plausible, code-confirmed mechanism for
    hung orders: this exact market produces one-sided books routinely (see
    hight_decision_engine's "One-sided book (no spread available)"
    rejections, observed across multiple cities in the same poll on
    2026-07-01). Reading the ASK side specifically — what it would actually
    cost to get filled by chasing the market — avoids reading our own bid
    back as the reference.

    NOT YET CONFIRMED against a live hung order — this is a strong,
    code-grounded hypothesis for the mechanism, not an observed fix. See
    tools/check_resting_orders.py to verify before treating this as settled.
    """
    return float(
        bracket.get("ob_no_ask") or
        bracket.get("no_ask") or
        bracket.get("no_ask_dollars") or
        0.0
    )


def _recover_fill_after_cancel_failure(
    client: "KalshiClient", order_id: str, ticker: str, order_no_price: float,
) -> bool:
    """
    Called whenever a cancel request inside manage_open_orders() fails. A
    failed cancel most often means the order no longer exists on Kalshi's
    side — which can mean it was already FILLED (a real, unbooked
    position) moments before our cancel request landed, or that it was
    already canceled/expired for some unrelated reason. The pre-existing
    code treated every cancel failure identically (log a warning, move on)
    without ever checking which of these it actually was.

    This checks Kalshi's own fill history for this order_id to tell the
    two apart. If a fill is found:
      - logs loudly at ERROR (this is real, previously-invisible exposure)
      - appends a trade_log.json entry tagged "manage_open_orders_orphan"
        so it's at least visible to every analytics/audit tool that reads
        trade_log.json going forward

    Deliberately does NOT touch open_contracts / held_city_map / any
    capital cap here — manage_open_orders() doesn't carry those shared
    structures in scope, and silently wiring a surprise fill into live
    capital-cap accounting inside an exception handler is a risk decision,
    not a logging decision. That wiring is a separate, explicit follow-up.

    Returns True if a fill was found and logged, False otherwise (nothing
    found — the original cancel-failure really was benign, or the
    verification lookup itself failed, in which case this fails loud
    rather than silently swallowing the ambiguity).
    """
    try:
        resp = client.get("portfolio/fills", params={"limit": 50, "ticker": ticker})
        matches = [f for f in resp.get("fills", []) if f.get("order_id") == order_id]
    except Exception as e:
        log.error(
            "manage_open_orders: cancel failed for %s (order %s) AND could "
            "not verify whether it actually filled: %s — check manually "
            "via: python3 tools/inspect_ticker_fills.py %s",
            ticker, order_id, e, ticker,
        )
        return False

    if not matches:
        return False

    total_contracts = sum(float(f.get("count_fp", 0) or 0) for f in matches)
    if total_contracts <= 0:
        return False
    avg_no_price = sum(
        float(f.get("no_price_dollars", 0) or 0) * float(f.get("count_fp", 0) or 0)
        for f in matches
    ) / total_contracts

    try:
        from cities import SERIES_TO_CITY as _SERIES_TO_CITY
        city_name = _SERIES_TO_CITY.get(ticker.split("-")[0], "")
    except Exception:
        city_name = ""

    log.error(
        "manage_open_orders: ORPHAN FILL — cancel for %s (order %s) failed "
        "because it had ALREADY FILLED (%d contract(s) @ $%.2f avg) before "
        "the cancel request landed. This position is NOT reflected in "
        "open_contracts, held_city_map, or any capital cap — only logged "
        "to trade_log.json here. Review capital exposure for this ticker "
        "manually.",
        ticker, order_id, int(total_contracts), avg_no_price,
    )

    try:
        _append_trade_log({
            "ticker":      ticker,
            "city":        city_name,
            "side":        "no",
            "market_type": "lowt" if "LOWT" in ticker.upper() else "high",
            "entry_price": round(avg_no_price, 4),
            "contracts":   int(total_contracts),
            "placed_at":   datetime.now(timezone.utc).isoformat(),
            "paper":       False,
            "entry_tier":  "manage_open_orders_orphan",
        })
    except Exception as e:
        log.error(
            "manage_open_orders: orphan fill detected for %s but the "
            "trade_log.json write ALSO failed: %s — this position is now "
            "invisible everywhere except Kalshi's own fill history.",
            ticker, e,
        )

    return True


def manage_open_orders(
    client:          KalshiClient,
    kalshi_snapshot: dict = None,
    no_min:          float = 0.85,   # HIGH floor — matches NO_MIN_ENTRY_PRICE in hight_decision_engine
    no_max:          float = 0.94,   # must match NO_MAX_ENTRY_PRICE in hight_decision_engine
    lowt_no_min:     float = 0.60,   # LOWT floor — LOWT-UP cascade enters down to 0.60 (backtested
                                     # +EV across [0.60,0.85); a 0.85 floor wrongly cancels its
                                     # highest-EV entries). Cancel only below 0.60 (thesis broken).
    paper:           bool  = False,
) -> None:
    """
    Manage resting (unfilled) orders each poll cycle.

    For each open temperature limit order:
      1. Fetch current NO ask price for that bracket
      2. If current NO ask is within [no_min, no_max): cancel + replace at
         current ask price (chase the market, always stay at the best price)
      3. If current NO ask is outside [no_min, no_max): cancel, do not replace

    Simple and aggressive — no age-based complexity. Every poll, resting orders
    are either repriced to the live market or cancelled. This eliminates hung
    orders caused by the market moving past our limit price.
    """

    try:
        resp = client.get("portfolio/orders", params={
            "status": "resting",
            "limit":  200,
        })
    except Exception as e:
        log.warning("manage_open_orders: failed to fetch open orders: %s", e)
        return

    orders = resp.get("orders", [])
    temp_orders = [
        o for o in orders
        if ("HIGH" in o.get("ticker", "").upper() or
            "LOWT" in o.get("ticker", "").upper())
    ]

    if not temp_orders:
        return

    log.info("manage_open_orders: %d resting temperature orders", len(temp_orders))

    replaced  = 0
    cancelled = 0

    for order in temp_orders:
        order_id = order.get("order_id", "")
        ticker   = order.get("ticker", "")

        # ── Contract count — v2 field is remaining_count_fp (a string like
        #    "3.00"); older names kept as fallback. Always float() first. ──
        resting = (order.get("remaining_count_fp")
                   or order.get("remaining_count")
                   or order.get("resting_contracts_count")
                   or order.get("count")
                   or 0)
        try:
            resting = int(float(str(resting)))
        except (TypeError, ValueError):
            resting = 0
        if resting <= 0:
            continue

        # ── Current NO ask price ─────────────────────────────────────────
        # Uses _no_ask_price (ask-preferring), NOT market_utils.no_price
        # (bid-preferring) — see _no_ask_price's docstring for why this
        # distinction matters specifically here.
        current_no = None
        if kalshi_snapshot:
            for city_data in kalshi_snapshot.values():
                for bracket in city_data.get("brackets", []):
                    if bracket.get("ticker") == ticker:
                        current_no = _no_ask_price(bracket)
                        break
                if current_no is not None:
                    break

        if current_no is None:
            try:
                mkt = client.get(f"markets/{ticker}")
                current_no = _no_ask_price(mkt.get("market", mkt))
            except Exception as e:
                log.debug("manage_open_orders: price fetch failed for %s: %s",
                          ticker, e)
                continue

        if not current_no or current_no <= 0:
            # No usable ASK price at all — either a genuinely one-sided book
            # with zero sell-side liquidity (nobody to trade with at any
            # price, not a stale-reference problem), or some other gap in
            # price data we haven't identified. Either way, the price-based
            # logic below has nothing to work with. Backstop: cancel purely
            # on age, regardless of price, so an order can't hang forever
            # just because we couldn't determine what to reprice it to.
            # created_time field name unconfirmed for this endpoint — same
            # defensive multi-fallback pattern as the resting-contracts
            # parsing above, since only fills' schema has been directly
            # verified against a live sample so far, not resting orders'.
            created_raw = (order.get("created_time") or order.get("created_at")
                           or order.get("ts") or "")
            age_min = None
            try:
                if isinstance(created_raw, (int, float)) or str(created_raw).isdigit():
                    created_dt = datetime.fromtimestamp(float(created_raw), tz=timezone.utc)
                else:
                    created_dt = datetime.fromisoformat(
                        str(created_raw).replace("Z", "+00:00"))
                age_min = (datetime.now(timezone.utc) - created_dt).total_seconds() / 60
            except (ValueError, TypeError, OSError):
                pass

            if age_min is not None and age_min >= STALE_ORDER_MAX_AGE_MIN:
                log.info(
                    "manage_open_orders: CANCEL %s — no usable ask price AND "
                    "resting %.0f min (>= %d min backstop)",
                    ticker, age_min, STALE_ORDER_MAX_AGE_MIN,
                )
                if not paper:
                    try:
                        client.delete(f"portfolio/orders/{order_id}")
                        cancelled += 1
                    except Exception as e:
                        log.warning("manage_open_orders: backstop cancel failed %s: %s",
                                    ticker, e)
                        _recover_fill_after_cancel_failure(client, order_id, ticker, current_no or 0.0)
            else:
                log.debug("manage_open_orders: %s — no usable price "
                          "(age=%s min), skipping this poll",
                          ticker, f"{age_min:.0f}" if age_min is not None else "unknown")
            continue

        # ── Order's placed NO price ──────────────────────────────────────
        # v2 stores the order's NO price as no_price_dollars (string "0.9200").
        # Older paths: price (YES, fraction) or no_price (cents or fraction).
        if order.get("no_price_dollars") is not None:
            order_no_price = round(float(order["no_price_dollars"]), 4)
        elif order.get("price") is not None:
            order_no_price = round(1.0 - float(order["price"]), 4)
        elif order.get("no_price") is not None:
            raw = float(order["no_price"])
            order_no_price = raw / 100 if raw > 1 else raw
        else:
            raw = float(order.get("yes_price_dollars",
                                  order.get("yes_price", 0)) or 0)
            yes_p = raw / 100 if raw > 1 else raw
            order_no_price = round(1.0 - yes_p, 4)

        # ── Decision ─────────────────────────────────────────────────────
        # LOWT orders use a wider floor: the LOWT-UP cascade legitimately enters
        # down to 0.60 (backtested +EV), so a 0.85 floor would wrongly cancel its
        # best trades. HIGH orders keep the 0.85 floor. Upper bound is shared.
        eff_no_min = lowt_no_min if "LOWT" in ticker.upper() else no_min
        out_of_range = (current_no < eff_no_min or current_no >= no_max)
        price_moved  = abs(current_no - order_no_price) >= 0.01

        if out_of_range:
            # Current price outside our entry criteria → cancel, do not replace
            log.info(
                "manage_open_orders: CANCEL %s  no=%.2f outside [%.2f,%.2f)",
                ticker, current_no, eff_no_min, no_max,
            )
            if not paper:
                try:
                    client.delete(f"portfolio/orders/{order_id}")
                    cancelled += 1
                except Exception as e:
                    log.warning("manage_open_orders: cancel failed %s: %s",
                                ticker, e)
                    _recover_fill_after_cancel_failure(client, order_id, ticker, order_no_price)

        elif price_moved:
            # Price has shifted → cancel and replace at current ask
            log.info(
                "manage_open_orders: REPRICE %s  %dc  %.2f→%.2f",
                ticker, resting, order_no_price, current_no,
            )
            if not paper:
                try:
                    client.delete(f"portfolio/orders/{order_id}")
                    place_order(
                        client        = client,
                        ticker        = ticker,
                        side          = "no",
                        price_dollars = current_no,
                        contracts     = resting,
                        paper         = False,
                    )
                    replaced += 1
                except Exception as e:
                    log.warning("manage_open_orders: reprice failed %s: %s",
                                ticker, e)
                    _recover_fill_after_cancel_failure(client, order_id, ticker, order_no_price)
        else:
            # Price-based logic found no reason to act. Universal backstop:
            # regardless of WHY the price check passed (genuinely stable
            # price, or some other undiscovered self-reference issue beyond
            # the specific bid-contamination mechanism _no_ask_price already
            # guards against), an order shouldn't be allowed to rest forever.
            created_raw = (order.get("created_time") or order.get("created_at")
                           or order.get("ts") or "")
            age_min = None
            try:
                if isinstance(created_raw, (int, float)) or str(created_raw).isdigit():
                    created_dt = datetime.fromtimestamp(float(created_raw), tz=timezone.utc)
                else:
                    created_dt = datetime.fromisoformat(
                        str(created_raw).replace("Z", "+00:00"))
                age_min = (datetime.now(timezone.utc) - created_dt).total_seconds() / 60
            except (ValueError, TypeError, OSError):
                pass

            if age_min is not None and age_min >= STALE_ORDER_MAX_AGE_MIN:
                log.info(
                    "manage_open_orders: CANCEL %s — price check found no "
                    "movement but resting %.0f min (>= %d min backstop)",
                    ticker, age_min, STALE_ORDER_MAX_AGE_MIN,
                )
                if not paper:
                    try:
                        client.delete(f"portfolio/orders/{order_id}")
                        cancelled += 1
                    except Exception as e:
                        log.warning("manage_open_orders: backstop cancel failed %s: %s",
                                    ticker, e)
                        _recover_fill_after_cancel_failure(client, order_id, ticker, order_no_price)
            # else: price unchanged, within range, and not yet stale enough
            # to force-cancel — leave the order resting.

    if replaced or cancelled:
        log.info("manage_open_orders: %d repriced  %d cancelled",
                 replaced, cancelled)


# ---------------------------------------------------------------------------


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

        # If Yes is at or near certainty the market has no No-side liquidity.
        # Stop firing sell orders that will never fill.
        if yes_price >= YES_MARKET_CLOSED:
            log.debug(
                "SKIP_EXIT  %s  yes=%.2f >= %.2f — market closed, no exit liquidity",
                ticker, yes_price, YES_MARKET_CLOSED,
            )
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
            if local_hour >= SETTLEMENT_HOLD_HOUR and obs_val is not None:
                if not is_lowt and floor is not None:
                    if (floor - obs_val) >= SETTLEMENT_CLEAR_BUFFER:
                        log.debug("HOLD_SETTLE  %s  obs_high=%.1f°F is %.1f°F below floor %.1f°F",
                                  ticker, obs_val, floor - obs_val, floor)
                        continue
                elif not is_lowt and floor is None and ceiling is not None:
                    if (obs_val - ceiling) >= SETTLEMENT_CLEAR_BUFFER:
                        log.debug("HOLD_SETTLE  %s  obs_high=%.1f°F is %.1f°F above ceiling %.1f°F",
                                  ticker, obs_val, obs_val - ceiling, ceiling)
                        continue
                elif is_lowt and ceiling is not None:
                    if (obs_val - ceiling) >= SETTLEMENT_CLEAR_BUFFER:
                        log.debug("HOLD_SETTLE  %s  obs_low=%.1f°F is %.1f°F above ceiling %.1f°F",
                                  ticker, obs_val, obs_val - ceiling, ceiling)
                        continue

            # ── Exit anchor DISABLED ───────────────────────────────────────
            # Analysis (May 2026, 583 positions): exit system was net -$93 vs
            # holding to settlement. Yes ceiling and forecast anchor both
            # destroyed value at every threshold tested. Hold everything to
            # settlement — the 95%+ WR makes exits counterproductive.
            # YES_MARKET_CLOSED guard above handles the only remaining edge
            # case (unfillable orders when market has resolved).

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


def check_topups(
    client:         KalshiClient,
    paper:          bool  = False,
    live_positions: list  = None,
    balance:        float = 0.0,
    deployable:     float = 0.0,
) -> int:
    """
    Top-up open No positions where the Yes price has stayed low through the day.

    Fires once per position per session during the 13:00-15:00 local window.
    Adds TOPUP_MAX_CONTRACTS extra contracts when the maximum Yes price seen
    during the window is <= YES_TOPUP_MAX (0.30).

    Backtest (Apr 6-23, 18 days):
      yes<=0.30 + 2c topup: 98.6% WR, +$67.91 additional PnL over base
      Improves every day the window is active. No bad days added.

    Returns number of top-up orders placed.
    """
    if live_positions is None:
        try:
            live_positions = sync_from_kalshi(client)
        except Exception as e:
            log.error("check_topups: could not fetch positions: %s", e)
            return 0

    if not live_positions:
        return 0

    # Filter to No positions only — top-up logic is No-specific
    no_positions = [p for p in live_positions if p.get("side", "").lower() == "no"]
    if not no_positions:
        return 0

    # Fetch live prices for all open tickers
    tickers = [p["ticker"] for p in no_positions]
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
            prices[m["ticker"]] = {"yes": yes_bid, "no": no_bid,
                                   "status": m.get("status", "active")}
    except Exception as e:
        log.warning("check_topups: price fetch failed: %s", e)
        return 0

    executed = 0
    deployed_topup = 0.0

    for pos in no_positions:
        ticker    = pos["ticker"]
        city      = pos.get("city") or _ticker_city(ticker)
        if not city:
            continue

        # Local hour gate
        local_hour = _city_local_hour(city)
        if not (TOPUP_HOUR_START <= local_hour <= TOPUP_HOUR_END):
            continue

        # Already topped up this session
        if ticker in _topup_done:
            continue

        # Market must still be active
        mkt = prices.get(ticker, {})
        if mkt.get("status", "active") != "active":
            continue

        curr_yes = mkt.get("yes", 1.0)
        curr_no  = mkt.get("no",  0.0)

        # Update Yes peak for this ticker
        prev_peak = _yes_peaks.get(ticker, 0.0)
        _yes_peaks[ticker] = max(prev_peak, curr_yes)

        # Yes must have stayed low throughout the window
        if _yes_peaks[ticker] > YES_TOPUP_MAX:
            continue

        # No price must still be in a sensible range
        if curr_no > NO_TOPUP_MAX_PRICE or curr_no <= 0.0:
            continue

        # Headroom check — don't exceed the cross-engine total cap.
        # TOPUP_TOTAL_CAP is aligned with GLOBAL_MAX_CONTRACTS_PER_TICKER (7)
        # so the topup engine respects the same ceiling as run_pipeline.
        held     = pos.get("contracts", 0)
        headroom = TOPUP_TOTAL_CAP - held
        if headroom <= 0:
            log.debug("TOPUP skip %s — no headroom (%d/%d)", ticker, held, TOPUP_TOTAL_CAP)
            continue

        contracts = min(TOPUP_MAX_CONTRACTS, headroom)
        cost      = round(curr_no * contracts, 4)

        # Capital gate — draw from topup allocation
        cap = get_engine_capital()
        if not cap.can_deploy("topup", cost):
            log.debug("TOPUP skip %s — topup budget exhausted (cost=$%.2f)",
                      ticker, cost)
            continue

        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        log.info("[%s] TOPUP  %s  +%dc @ $%.2f  "
                 "(max_yes_seen=%.2f  held=%d→%d)",
                 ts, ticker, contracts, curr_no,
                 _yes_peaks[ticker], held, held + contracts)

        if not paper:
            try:
                place_order(
                    client        = client,
                    ticker        = ticker,
                    side          = "no",
                    price_dollars = curr_no,
                    contracts     = contracts,
                    paper         = False,
                )
                _topup_done.add(ticker)
                cap.record("topup", cost)
                executed += 1
                _append_trade_log({
                    "ticker":       ticker,
                    "city":         city,
                    "side":         "no",
                    "market_type":  "high",
                    "score":        0,
                    "score_detail": ["topup_yes_low"],
                    "entry_price":  curr_no,
                    "contracts":    contracts,
                    "placed_at":    datetime.now(timezone.utc).isoformat(),
                    "paper":        False,
                    "entry_tier":   "topup",
                })
            except Exception as e:
                log.error("TOPUP order failed  %s: %s", ticker, e)
        else:
            log.info("  [PAPER] would top up %s +%dc @ $%.2f", ticker, contracts, curr_no)
            _topup_done.add(ticker)
            executed += 1

    if executed:
        log.info("TOPUP: %d top-up order(s) placed  ($%.2f deployed)", executed, deployed_topup)
    return deployed_topup


def _ticker_city(ticker: str) -> str | None:
    """Derive city name from ticker using SERIES_TO_CITY map."""
    try:
        from cities import SERIES_TO_CITY
        return SERIES_TO_CITY.get(ticker.split("-")[0])
    except Exception:
        return None


def run_pipeline(
    client:       KalshiClient,
    city_filter:  str  = None,
    paper:        bool = False,
    kalshi_high:  dict = None,
    kalshi_lowt:  dict = None,
    nws_snapshot: dict = None,
):
    """Run HIGH and LOWT decision engines, then execute any actionable signals."""
    global _deployed_cascade   # module-level tracker; += requires explicit global
    # ── HIGH markets ─────────────────────────────────────────────────────────
    evaluations, nws_snapshot, kalshi_results = decision_engine.run(
        city_filter     = city_filter,
        kalshi_snapshot = kalshi_high,
        nws_snapshot    = nws_snapshot,
    )
    decision_engine.display(evaluations)

    # ── LOWT markets ─────────────────────────────────────────────────────────
    try:
        lowt_kalshi = kalshi_lowt if kalshi_lowt is not None else \
                      kalshi_scanner.scan_all(city_filter=city_filter, market_type="lowt")
        lowt_evals = lowt_decision_engine.run(
            kalshi_results = lowt_kalshi,
            city_filter    = city_filter,
            paper          = paper,
            nws_results    = nws_snapshot,
        )
        lowt_decision_engine.display(lowt_evals)
        evaluations = evaluations + lowt_evals
        cascade_engine.display(evaluations)
    except Exception as e:
        log.warning("LOWT pipeline error (non-fatal): %s", e)

    try:
        balance                      = get_balance(client)
        _, cascade_reserve           = _update_day_snapshot(balance)
        set_balance_cached(balance)
        cap = get_engine_capital(client)
        log.info(
            "balance: $%.2f  |  main=$%.2f  cascade=$%.2f  "
            "sweep=$%.2f  econv=$%.2f  hourly=$%.2f",
            balance,
            cap.remaining("main"),
            cap.remaining("cascade"),
            cap.remaining("sweep"),
            cap.remaining("econv"),
            cap.remaining("hourly"),
        )
    except Exception as e:
        log.warning("balance fetch failed: %s — using $0 cap", e)
        balance          = 0.0
        cascade_reserve  = 0.0

    try:
        live_positions = sync_from_kalshi(client)
        # Track contracts held per ticker (for per-bracket headroom check)
        open_contracts = {p["ticker"]: p["contracts"] for p in live_positions}
        # Also include resting (unfilled) orders — these count against headroom
        # to prevent the accumulation bug where the engine re-enters the same
        # bracket every poll because the previous order hasn't filled yet.
        try:
            resting = client.get("portfolio/orders", params={
                "status": "resting", "limit": 200
            }).get("orders", [])
            for o in resting:
                t = o.get("ticker", "")
                if not t: continue
                n = int(float(o.get("remaining_count_fp") or
                              o.get("remaining_count") or
                              o.get("resting_contracts_count") or 0))
                if n > 0:
                    open_contracts[t] = open_contracts.get(t, 0) + n
        except Exception as e:
            log.debug("run_pipeline: resting orders fetch failed (non-fatal): %s", e)
        # Track positions held per city, split by market type so HIGH and LOWT
        # positions don't consume each other's per-city cap.
        from cities import SERIES_TO_CITY as _SERIES_TO_CITY
        held_high_per_city: dict[str, int] = {}
        held_lowt_per_city: dict[str, int] = {}
        for ticker in open_contracts:
            city_name = _SERIES_TO_CITY.get(ticker.split("-")[0])
            if city_name:
                if "LOWT" in ticker:
                    held_lowt_per_city[city_name] = held_lowt_per_city.get(city_name, 0) + 1
                else:
                    held_high_per_city[city_name] = held_high_per_city.get(city_name, 0) + 1
    except Exception:
        open_contracts     = {}
        held_high_per_city = {}
        held_lowt_per_city = {}

    executed = 0
    deployed = 0.0

    # Initialise engine capital with live balance
    cap = EngineCapital(balance=balance)
    log.info("EngineCapital: %s", cap.summary())

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

            # Main engine dedup: never re-enter a ticker already entered this session.
            tier_check = signal.get("entry_tier", "") or "main"
            if not tier_check.startswith("cascade") and ticker in _main_entered:
                log.debug("skip %s — already entered this session (main engine)", ticker)
                continue

            # Determine market type for this signal — drives city cap and counter
            is_lowt_signal = signal.get("market_type", "high") == "lowt"
            held_city_map  = held_lowt_per_city if is_lowt_signal else held_high_per_city
            max_per_city   = (lowt_decision_engine.MAX_NO_PER_CITY if is_lowt_signal
                              else decision_engine.MAX_NO_PER_CITY)

            # Per-city cap: separate limits for HIGH and LOWT so one type
            # doesn't consume the other's budget.
            city_held = held_city_map.get(city, 0)
            if city_held >= max_per_city:
                log.debug("skip %s — %s already at MAX_NO_PER_CITY (%d/%d)",
                          ticker, city, city_held, max_per_city)
                continue

            held      = open_contracts.get(ticker, 0)

            # Global per-ticker cap — enforced across all engines.
            if held >= GLOBAL_MAX_CONTRACTS_PER_TICKER:
                log.debug("skip %s — global cap reached (%d/%d contracts)",
                          ticker, held, GLOBAL_MAX_CONTRACTS_PER_TICKER)
                continue

            max_contr = signal.get("max_contracts", 2)
            headroom  = min(max_contr, GLOBAL_MAX_CONTRACTS_PER_TICKER) - held
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

            # Engine capital check — session-scoped per engine type
            tier = signal.get("entry_tier", "")
            if tier.startswith("cascade"):
                engine_key = "cascade"
            elif tier.startswith("lowt"):
                engine_key = "lowt"
            elif (tier.startswith("tomorrow") or tier == "dead_sweep"
                  or tier == "sweep"):
                engine_key = "sweep"
            else:
                engine_key = "main"
            if engine_key == "cascade":
                if get_cascade_deployable() < cost:
                    log.debug("skip %s — cascade session budget exhausted (cost=$%.2f  remaining=$%.2f)",
                              ticker, cost, get_cascade_deployable())
                    continue
            elif engine_key == "lowt":
                if get_lowt_deployable() < cost:
                    log.debug("skip %s — lowt budget exhausted "
                              "(cost=$%.2f  remaining=$%.2f)",
                              ticker, cost, get_lowt_deployable())
                    continue
            elif engine_key == "sweep":
                if get_sweep_deployable() < cost:
                    log.debug("skip %s — sweep budget exhausted "
                              "(cost=$%.2f  remaining=$%.2f)",
                              ticker, cost, get_sweep_deployable())
                    continue
            else:
                if not cap.can_deploy("main", cost):
                    log.debug("skip %s — main budget exhausted (cost=$%.2f)",
                              ticker, cost)
                    continue

            log.info("SIGNAL  %s  %s", city, ticker)
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
                held_city_map[city]    = held_city_map.get(city, 0) + 1
                cap.record(engine_key, cost)
                deployed += cost
                if engine_key == "cascade":
                    _deployed_cascade += cost
                elif engine_key == "lowt":
                    record_lowt_deployed(cost)
                elif engine_key == "sweep":
                    record_sweep_deployed(cost)
                else:
                    _main_entered.add(ticker)
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
                    "entry_tier":   signal.get("entry_tier", "") or "main",
                })

            except Exception as e:
                log.error("order failed  %s: %s", ticker, e)

    log.info("%d order(s) placed", executed)

    # ── Top-up pass ──────────────────────────────────────────────────────────
    # After placing new entries, re-fetch positions and run the top-up check.
    # Top-ups are gated to 13:00-15:00 local and only fire once per ticker.
    try:
        # Re-sync positions (new entries may have been added above)
        live_positions_post = sync_from_kalshi(client)
        topup_deployed = check_topups(
            client         = client,
            paper          = paper,
            live_positions = live_positions_post,
            balance        = balance,
        )
    except Exception as e:
        log.warning("top-up check error (non-fatal): %s", e)

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
