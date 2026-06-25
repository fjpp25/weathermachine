#!/usr/bin/env python3
"""
probe_resting.py — read-only diagnostic for manage_open_orders.

Calls the SAME endpoint manage_open_orders uses (portfolio/orders, status=resting)
and prints exactly what Kalshi returns, so we can see whether:
  (a) the fetch returns any orders at all,
  (b) the HIGH/LOWT ticker filter is dropping them,
  (c) the field names (status, ticker, remaining_count) match what the code expects.

READ-ONLY: only issues GET. Never places, cancels, or amends an order.

Run on the Pi from the repo dir:
    python3 probe_resting.py
"""
import json
from pathlib import Path
import os


def load_credentials():
    """Mirror scheduler.py: data/config.json first, then .env."""
    config_file = Path("data/config.json")
    if config_file.exists():
        config = json.loads(config_file.read_text())
        if config.get("key_id"):
            os.environ.setdefault("KALSHI_KEY_ID", config["key_id"])
        if config.get("key_file"):
            os.environ.setdefault("KALSHI_KEY_FILE", config["key_file"])
        os.environ["KALSHI_DEMO"] = "false" if config.get("live_mode") else "true"


def main():
    load_credentials()
    import trader
    client = trader.make_client(skip_confirmation=True)

    # Try a few status values — the live system uses "resting".
    for status in ("resting", "open", None):
        params = {"limit": 200}
        if status:
            params["status"] = status
        try:
            resp = client.get("portfolio/orders", params=params)
        except Exception as e:
            print(f"[status={status}] fetch FAILED: {e}")
            continue

        # What keys does the response have? (orders? data? something else?)
        top_keys = list(resp.keys()) if isinstance(resp, dict) else type(resp)
        orders = resp.get("orders", []) if isinstance(resp, dict) else []
        print(f"\n=== status={status!r} ===")
        print(f"  response top-level keys: {top_keys}")
        print(f"  orders returned: {len(orders)}")

        if orders:
            # Show the first order's full shape so we can see field names.
            print("  first order keys:", sorted(orders[0].keys()))
            for o in orders[:10]:
                tk = o.get("ticker", "")
                is_temp = ("HIGH" in tk.upper() or "LOWT" in tk.upper())
                print(f"    ticker={tk:28} status={o.get('status')!r} "
                      f"remaining={o.get('remaining_count')} "
                      f"resting_cc={o.get('resting_contracts_count')} "
                      f"count={o.get('count')}  passes_HIGH/LOWT_filter={is_temp}")

    print("\nDone. If 'orders returned: 0' for status='resting' but you see hung "
          "orders in the Kalshi UI, the status value or endpoint is the problem. "
          "If orders ARE returned but passes_HIGH/LOWT_filter=False, the ticker "
          "filter is the problem.")

    # ── Replicate manage_open_orders' PRICE RESOLUTION on each resting order ──
    # This is the suspected failure point: orders are SEEN but never repriced
    # or cancelled, which means current_no resolves to None/0 and the function
    # hits a silent `continue`. We reproduce that lookup exactly.
    print("\n=== PRICE RESOLUTION replay (the suspected silent-skip path) ===")
    try:
        resp = client.get("portfolio/orders", params={"status": "resting", "limit": 200})
        orders = resp.get("orders", [])
    except Exception as e:
        print(f"  fetch failed: {e}")
        orders = []

    temp_orders = [o for o in orders
                   if ("HIGH" in o.get("ticker", "").upper()
                       or "LOWT" in o.get("ticker", "").upper())]
    if not temp_orders:
        print("  no resting temperature orders right now — rerun when one is hung.")
        return

    # Build the HIGH snapshot exactly as the scheduler passes it.
    try:
        import kalshi_scanner
        snap = kalshi_scanner.scan_all(city_filter=None, market_type="high")
    except Exception as e:
        print(f"  could not build snapshot ({e}); will test per-ticker fetch only")
        snap = {}

    from market_utils import no_price as _no_price
    for o in temp_orders:
        ticker = o.get("ticker", "")
        # 1) snapshot lookup
        snap_price = None
        for city_data in (snap or {}).values():
            for bracket in city_data.get("brackets", []):
                if bracket.get("ticker") == ticker:
                    snap_price = _no_price(bracket)
                    break
            if snap_price is not None:
                break
        # 2) per-ticker fallback
        fetch_price = None
        try:
            mkt = client.get(f"markets/{ticker}")
            fetch_price = _no_price(mkt.get("market", mkt))
        except Exception as e:
            fetch_price = f"FETCH_ERR({e})"

        in_snap = "LOWT" not in ticker.upper()  # LOWT can't be in HIGH snapshot
        print(f"  {ticker:28} snapshot_no={snap_price!r:8} "
              f"fallback_no={fetch_price!r:8} "
              f"{'(LOWT: not in HIGH snapshot by design)' if 'LOWT' in ticker.upper() else ''}")

    print("\n  If snapshot_no=None AND fallback_no is None/0/err for an order, "
          "that's the silent `continue` — the order can never be repriced or "
          "cancelled, so it piles up. The non-None column tells us which lookup "
          "to rely on for the fix.")


if __name__ == "__main__":
    main()
