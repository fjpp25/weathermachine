#!/usr/bin/env python3
"""
Standalone concurrency test for place_order()'s new cap-enforcement logic.
Does NOT import trader.py (too many unrelated dependencies to resolve in
isolation) — instead reproduces the exact reservation algorithm added to
place_order() and hammers it with real concurrent threads, to verify the
lock actually prevents the race rather than just reading correctly.

This directly simulates the Seattle/Austin scenario: N "engines" (threads)
all independently decide, in the same instant, that they want to buy
contracts on the same ticker.
"""
import threading
import time
import random

GLOBAL_MAX_CONTRACTS_PER_TICKER = 7

_order_lock = threading.Lock()
_session_contracts_committed: dict[str, int] = {}

results = []
results_lock = threading.Lock()


def reserve(ticker: str, requested: int) -> int:
    """Mirrors place_order()'s reservation block exactly."""
    with _order_lock:
        committed = _session_contracts_committed.get(ticker, 0)
        headroom = GLOBAL_MAX_CONTRACTS_PER_TICKER - committed
        if headroom <= 0:
            return 0
        actual = min(requested, headroom)
        _session_contracts_committed[ticker] = committed + actual
        return actual


def simulate_engine(engine_name: str, ticker: str, requested: int):
    # Simulate the real-world timing: multiple engines' decision loops
    # landing within the same tiny window, not literally the same
    # nanosecond — this is the realistic version of the race, not an
    # artificially easier one.
    time.sleep(random.uniform(0, 0.01))
    granted = reserve(ticker, requested)
    with results_lock:
        results.append((engine_name, requested, granted))


def run_scenario(n_engines: int, requested_each: int, label: str):
    global _session_contracts_committed
    _session_contracts_committed = {}
    results.clear()
    ticker = "KXHIGHTEST-TEST"

    threads = [
        threading.Thread(target=simulate_engine, args=(f"engine_{i}", ticker, requested_each))
        for i in range(n_engines)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    total_granted = sum(g for _, _, g in results)
    print(f"\n{label}")
    print(f"  {n_engines} engines each requesting {requested_each} contracts "
          f"(naive worst case: {n_engines * requested_each})")
    for name, req, granted in sorted(results):
        print(f"    {name}: requested {req}, granted {granted}")
    print(f"  TOTAL GRANTED: {total_granted}  (cap: {GLOBAL_MAX_CONTRACTS_PER_TICKER})")
    assert total_granted <= GLOBAL_MAX_CONTRACTS_PER_TICKER, (
        f"FAILED: total granted {total_granted} exceeds cap "
        f"{GLOBAL_MAX_CONTRACTS_PER_TICKER}!"
    )
    print(f"  PASS — cap held under concurrent access")


if __name__ == "__main__":
    # Scenario 1: reproduces Seattle almost exactly — 5 engines, 3-7 contracts each
    run_scenario(5, 5, "Scenario 1: 5 engines x 5 contracts (Seattle-like)")

    # Scenario 2: reproduces Austin — 5 engines, smaller requests
    run_scenario(5, 4, "Scenario 2: 5 engines x 4 contracts (Austin-like)")

    # Scenario 3: stress test — many more engines than could realistically
    # exist, to confirm the lock holds even under extreme contention
    run_scenario(20, 3, "Scenario 3: 20 engines x 3 contracts (stress test)")

    # Scenario 4: run the whole thing 50 times to catch any timing-dependent
    # flakiness a single run might get lucky and not expose
    print(f"\n{'='*60}")
    print("Scenario 4: repeating 5-engine race 50 times to check for flakiness")
    failures = 0
    for i in range(50):
        _session_contracts_committed.clear()
        results.clear()
        ticker = "KXHIGHTEST-REPEAT"
        threads = [
            threading.Thread(target=simulate_engine, args=(f"e{j}", ticker, 4))
            for j in range(5)
        ]
        for t in threads: t.start()
        for t in threads: t.join()
        total = sum(g for _, _, g in results)
        if total > GLOBAL_MAX_CONTRACTS_PER_TICKER:
            failures += 1
            print(f"  run {i}: FAILED — total={total}")
    print(f"  {50 - failures}/50 runs held the cap correctly")
    assert failures == 0, f"{failures} runs exceeded the cap — lock is not working"
    print("  PASS — no flakiness across 50 repeated concurrent races")

    print(f"\n{'='*60}")
    print("ALL SCENARIOS PASSED")
