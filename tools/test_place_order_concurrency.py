#!/usr/bin/env python3
"""
tools/test_place_order_concurrency.py — integration test, READ-ONLY in the
sense that it never talks to Kalshi (paper=True throughout).

WHY THIS EXISTS, separate from test_cap_concurrency.py
--------------------------------------------------------
test_cap_concurrency.py verifies the LOCKING ALGORITHM is sound — it
reproduces the reservation logic in isolation, not the real trader.py file.
That's useful (and it passed, 50/50 concurrent runs), but it can't catch a
mistake specific to the actual deployed code — a wrong variable name, a
misplaced line, a paper/real counter mix-up. This test imports the REAL
trader.py and calls the REAL place_order() function directly, concurrently,
with paper=True — so it's the real code path, verified, with zero risk to
real capital (paper orders never reach Kalshi's API).

This also verifies something test_cap_concurrency.py structurally can't:
that paper-mode orders are cap-enforced via their own SEPARATE counter
(_paper_contracts_committed) without ever touching or being affected by
_session_contracts_committed (real order accounting) — run this alongside
a real trading session and it should have zero effect on real headroom.

USAGE (repo root, on the Pi — needs no Kalshi credentials, since paper
mode never calls the API):
    python3 tools/test_place_order_concurrency.py
"""
from __future__ import annotations

import sys
import threading
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import trader


def run_concurrent_buys(ticker: str, n_engines: int, contracts_each: int) -> list[dict]:
    """Fires n_engines concurrent calls to the REAL trader.place_order(),
    all buying the same ticker, all in paper mode. Returns each call's
    actual response dict."""
    responses: list[dict] = []
    responses_lock = threading.Lock()

    def _one_call():
        # client=None is safe here: paper=True returns before place_order()
        # ever touches the client argument.
        resp = trader.place_order(
            client=None,
            ticker=ticker,
            side="no",
            price_dollars=0.95,
            contracts=contracts_each,
            paper=True,
            action="buy",
        )
        with responses_lock:
            responses.append(resp)

    threads = [threading.Thread(target=_one_call) for _ in range(n_engines)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return responses


def granted_contracts(resp: dict) -> int:
    """Extract how many contracts a place_order() response actually
    represents. Blocked -> 0. Otherwise, read back from the paper order body
    (count is stored as a formatted string, e.g. "3.00")."""
    if resp.get("blocked"):
        return 0
    order = resp.get("order", {})
    try:
        return int(float(order.get("count", 0)))
    except (TypeError, ValueError):
        return 0


def main():
    print(f"GLOBAL_MAX_CONTRACTS_PER_TICKER = {trader.GLOBAL_MAX_CONTRACTS_PER_TICKER}")

    # ── Test 1: concurrent buys on the real place_order(), paper mode ──────
    ticker = "KXHIGHTEST-INTEGRATION-1"
    trader._paper_contracts_committed.pop(ticker, None)  # clean slate

    print(f"\n{'='*70}")
    print(f"  Test 1: 5 concurrent engines x 5 contracts each, same ticker")
    print(f"{'='*70}")
    responses = run_concurrent_buys(ticker, n_engines=5, contracts_each=5)
    total = sum(granted_contracts(r) for r in responses)
    for i, r in enumerate(responses):
        g = granted_contracts(r)
        status = "BLOCKED" if r.get("blocked") else f"granted {g}"
        print(f"  call {i}: {status}")
    print(f"  TOTAL GRANTED (via real place_order()): {total}")
    assert total <= trader.GLOBAL_MAX_CONTRACTS_PER_TICKER, (
        f"FAILED: real place_order() allowed {total} contracts, "
        f"exceeding cap of {trader.GLOBAL_MAX_CONTRACTS_PER_TICKER}"
    )
    print(f"  PASS")

    # ── Test 2: paper and real counters are genuinely independent ──────────
    print(f"\n{'='*70}")
    print(f"  Test 2: paper orders must not touch real-order accounting")
    print(f"{'='*70}")
    ticker2 = "KXHIGHTEST-INTEGRATION-2"
    trader._session_contracts_committed.pop(ticker2, None)
    trader._paper_contracts_committed.pop(ticker2, None)

    run_concurrent_buys(ticker2, n_engines=3, contracts_each=5)
    real_committed = trader._session_contracts_committed.get(ticker2, 0)
    paper_committed = trader._paper_contracts_committed.get(ticker2, 0)
    print(f"  after 3 paper buys: real_committed={real_committed}, "
          f"paper_committed={paper_committed}")
    assert real_committed == 0, (
        f"FAILED: paper orders leaked into real accounting "
        f"(_session_contracts_committed[{ticker2}] = {real_committed}, expected 0)"
    )
    assert paper_committed > 0, "paper orders should have registered in the paper counter"
    print(f"  PASS — real accounting untouched by paper activity")

    # ── Test 3: sell releases headroom correctly in paper mode ─────────────
    print(f"\n{'='*70}")
    print(f"  Test 3: paper sell releases headroom for a subsequent paper buy")
    print(f"{'='*70}")
    ticker3 = "KXHIGHTEST-INTEGRATION-3"
    trader._paper_contracts_committed.pop(ticker3, None)

    buy_resp = trader.place_order(client=None, ticker=ticker3, side="no",
                                    price_dollars=0.95, contracts=7,
                                    paper=True, action="buy")
    assert granted_contracts(buy_resp) == 7, "expected full 7 contracts granted"
    print(f"  bought 7/7 (cap reached)")

    blocked_resp = trader.place_order(client=None, ticker=ticker3, side="no",
                                        price_dollars=0.95, contracts=2,
                                        paper=True, action="buy")
    assert blocked_resp.get("blocked"), "expected second buy to be blocked at full cap"
    print(f"  second buy correctly BLOCKED (at cap)")

    trader.place_order(client=None, ticker=ticker3, side="no",
                        price_dollars=0.95, contracts=3,
                        paper=True, action="sell")
    after_sell = trader._paper_contracts_committed.get(ticker3, 0)
    print(f"  sold 3 -> committed now {after_sell} (expected 4)")
    assert after_sell == 4, f"expected 4 after selling 3 of 7, got {after_sell}"

    retry_resp = trader.place_order(client=None, ticker=ticker3, side="no",
                                      price_dollars=0.95, contracts=3,
                                      paper=True, action="buy")
    assert granted_contracts(retry_resp) == 3, (
        f"expected 3 contracts of freed headroom to be grantable, "
        f"got {granted_contracts(retry_resp)}"
    )
    print(f"  re-buy of 3 correctly granted after sell freed headroom")
    print(f"  PASS")

    print(f"\n{'='*70}")
    print(f"  ALL INTEGRATION TESTS PASSED — verified against the REAL")
    print(f"  place_order() function, not a standalone reproduction.")
    print(f"{'='*70}")


if __name__ == "__main__":
    main()
