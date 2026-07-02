#!/usr/bin/env python3
"""
tools/check_resting_orders.py — verify (or refute) the self-referential bid
hypothesis for hanging orders before treating trader.py's fix as confirmed.

WHAT THIS CHECKS: for every currently-resting temperature order, fetches
the live order book for that ticker and reports:
  - the order's own resting price
  - the current best bid and best ask on that ticker
  - whether the order's price EXACTLY matches the current best bid (the
    tell-tale sign our own resting order IS the reported bid — i.e. the
    self-reference mechanism described in manage_open_orders' fix)
  - how long the order has been resting

This is diagnostic only — it does not cancel, reprice, or place anything.

USAGE (on the Pi, from repo root):
    python3 tools/check_resting_orders.py
"""
import json
from datetime import datetime, timezone
from pathlib import Path


def load_credentials():
    import os
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

    resp = client.get("portfolio/orders", params={"status": "resting", "limit": 200})
    orders = resp.get("orders", [])
    temp_orders = [
        o for o in orders
        if ("HIGH" in o.get("ticker", "").upper() or "LOWT" in o.get("ticker", "").upper())
    ]

    print(f"=== {len(temp_orders)} resting temperature order(s) ===\n")
    if not temp_orders:
        print("Nothing resting right now — re-run this during active trading hours "
              "when orders are more likely to be open.")
        return

    for order in temp_orders:
        ticker = order.get("ticker", "")
        order_id = order.get("order_id", "")

        order_no_price = None
        if order.get("no_price_dollars") is not None:
            order_no_price = round(float(order["no_price_dollars"]), 4)

        created_raw = order.get("created_time") or order.get("created_at") or ""
        age_min = None
        try:
            created_dt = datetime.fromisoformat(str(created_raw).replace("Z", "+00:00"))
            age_min = (datetime.now(timezone.utc) - created_dt).total_seconds() / 60
        except (ValueError, TypeError):
            pass

        try:
            mkt = client.get(f"markets/{ticker}")
            m = mkt.get("market", mkt)
        except Exception as e:
            print(f"{ticker}: fetch failed ({e})\n")
            continue

        current_bid = m.get("no_bid_dollars") or m.get("no_bid")
        current_ask = m.get("no_ask_dollars") or m.get("no_ask")

        print(f"{ticker}")
        print(f"  order_id:        {order_id}")
        print(f"  our resting price: {order_no_price}")
        print(f"  age:             {f'{age_min:.0f} min' if age_min is not None else 'unknown'}")
        print(f"  current No bid:  {current_bid}")
        print(f"  current No ask:  {current_ask}")

        if order_no_price is not None and current_bid is not None:
            try:
                is_self = abs(float(current_bid) - order_no_price) < 0.001
                if is_self:
                    print(f"  *** SELF-REFERENCE SUSPECTED: our resting order's price "
                          f"exactly matches the current best bid. If the old "
                          f"bid-preferring no_price() were still in use, "
                          f"manage_open_orders would have read this back as "
                          f"'the market' and found zero movement, explaining a hang. ***")
                elif current_ask is not None and abs(float(current_ask) - order_no_price) > 0.01:
                    print(f"  Ask has moved away from our resting price — this order "
                          f"SHOULD get repriced/cancelled by the fixed logic next poll.")
            except (ValueError, TypeError):
                pass
        print()


if __name__ == "__main__":
    main()
