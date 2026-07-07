#!/usr/bin/env python3
"""
tools/test_cancel_isolated.py — diagnostic only.

WHY THIS EXISTS
----------------
manage_open_orders() has been logging "410 Client Error: Gone" on cancel
attempts for resting orders that a separate, independent check (via
tools/inspect_ticker_fills.py) shows are STILL status=resting minutes
later. That's inconsistent: a 410 on cancel should mean the order no
longer exists, but it demonstrably still does. trader.py's KalshiClient
.delete() calls resp.raise_for_status() before ever reading resp.json(),
so the only thing we've seen so far is the generic requests.HTTPError
string — never Kalshi's actual JSON error body, which likely has a
specific error `code` explaining what's really going on.

This script:
  1. GETs the order directly right before attempting anything, to confirm
     its exact current status.
  2. Issues the raw DELETE itself (bypassing raise_for_status entirely)
     and prints status code + full response body, whatever it is.
  3. GETs the order again immediately after, to see whether the DELETE
     actually changed its state despite the error response.

This deliberately reuses trader.make_client() — the same signing/auth
code already used everywhere else — so a difference here vs. what
manage_open_orders() saw would point at Kalshi's side (e.g. an order
group, a queue-position race, a documented-but-undiscovered status code),
not at some request-construction bug unique to this script.

USAGE (repo root, on the Pi):
    python3 tools/test_cancel_isolated.py <order_id>
"""
import argparse
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def load_credentials():
    config_file = Path("data/config.json")
    if config_file.exists():
        config = json.loads(config_file.read_text())
        if config.get("key_id"):
            os.environ.setdefault("KALSHI_KEY_ID", config["key_id"])
        if config.get("key_file"):
            os.environ.setdefault("KALSHI_KEY_FILE", config["key_file"])
        os.environ["KALSHI_DEMO"] = "false" if config.get("live_mode") else "true"


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("order_id")
    args = ap.parse_args()

    load_credentials()
    import trader
    client = trader.make_client(skip_confirmation=True)

    print(f"\n{'='*80}\n  BEFORE — GET portfolio/orders (status=resting)\n{'='*80}")
    try:
        resp = client.get("portfolio/orders", params={"status": "resting", "limit": 200})
        match = [o for o in resp.get("orders", []) if o.get("order_id") == args.order_id]
        if match:
            print(json.dumps(match[0], indent=2))
        else:
            print(f"  order_id {args.order_id} NOT in the resting list right now.")
    except Exception as e:
        print(f"  GET failed: {e}")

    print(f"\n{'='*80}\n  DELETE — raw response, bypassing raise_for_status\n{'='*80}")
    import requests
    path = client._api_path(f"portfolio/orders/{args.order_id}")
    url = client.base_url + "/" + f"portfolio/orders/{args.order_id}"
    headers = client._headers("DELETE", path)
    try:
        raw = requests.request("DELETE", url, headers=headers, timeout=15)
        print(f"  HTTP {raw.status_code}")
        print(f"  Headers: {dict(raw.headers)}")
        print(f"  Body:")
        try:
            print(json.dumps(raw.json(), indent=2))
        except Exception:
            print(f"    (not JSON) {raw.text!r}")
    except Exception as e:
        print(f"  Request itself failed (connection/timeout, not an HTTP error): {e}")

    print(f"\n{'='*80}\n  AFTER — GET portfolio/orders (status=resting)\n{'='*80}")
    try:
        resp = client.get("portfolio/orders", params={"status": "resting", "limit": 200})
        match = [o for o in resp.get("orders", []) if o.get("order_id") == args.order_id]
        if match:
            print(json.dumps(match[0], indent=2))
            print("\n  Still resting after the DELETE attempt.")
        else:
            print(f"  order_id {args.order_id} no longer in the resting list — "
                  f"the DELETE evidently worked despite the error response, "
                  f"OR it filled/expired independently in between.")
    except Exception as e:
        print(f"  GET failed: {e}")


if __name__ == "__main__":
    main()
