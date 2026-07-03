#!/usr/bin/env python3
"""
tools/verify_fills_pagination.py — diagnostic only. READ-ONLY.

WHY: scan_for_hedge_pattern.py (and compare_exit_accounting_fix.py, and
fetch_settlements.py) all cap fills/settlements pagination at 15 pages of
200 = 3000 records. scan_for_hedge_pattern.py's most recent run returned
EXACTLY 3000 fills — an exact round multiple of the page size, which is the
signature of the loop hitting its iteration cap rather than reaching a
natural end (a real fill count landing on a clean round number like that by
coincidence is the less likely explanation). If the cap was actually hit,
there's more data somewhere that wasn't fetched — and whether that missing
data is old (harmless for a "nothing recent" conclusion) or new (would
silently invalidate it) depends entirely on which direction the API paginates.

This checks both directly: whether the cursor was still non-null after 15
pages (cap actually hit, vs. genuinely reached the end), and the
created_time range of the FIRST page fetched (page 1, no cursor) vs the
LAST page fetched — which reveals the sort order without guessing.

USAGE (repo root, on the Pi):
    python3 tools/verify_fills_pagination.py
"""
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
        return
    env_file = Path(".env")
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            if "=" in line and not line.startswith("#"):
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())


def main():
    load_credentials()
    import trader
    client = trader.make_client(skip_confirmation=True)

    print("Fetching page 1 (no cursor)...")
    p1 = client.get("portfolio/fills", params={"limit": 200})
    page1_fills = p1.get("fills", [])
    if page1_fills:
        times = sorted(f.get("created_time", "") for f in page1_fills)
        print(f"  Page 1: {len(page1_fills)} fills, "
              f"created_time range {times[0]} .. {times[-1]}")
    cursor = p1.get("cursor")
    print(f"  cursor after page 1: {'present' if cursor else 'NONE (only 1 page total)'}")

    pages = 1
    last_page_fills = page1_fills
    all_fills = list(page1_fills)
    while cursor and pages < 15:
        d = client.get("portfolio/fills", params={"limit": 200, "cursor": cursor})
        b = d.get("fills", [])
        all_fills.extend(b)
        last_page_fills = b
        cursor = d.get("cursor")
        pages += 1
        if len(b) < 200:
            break

    print(f"\nFetched {pages} page(s), {len(all_fills)} total fills.")
    print(f"cursor after final page fetched: "
          f"{'STILL PRESENT — cap was hit, more data exists beyond this' if cursor else 'none — reached natural end, nothing missing'}")

    if last_page_fills:
        times = sorted(f.get("created_time", "") for f in last_page_fills)
        print(f"Last page fetched: created_time range {times[0]} .. {times[-1]}")

    if page1_fills and last_page_fills:
        p1_times = sorted(f.get("created_time", "") for f in page1_fills)
        pl_times = sorted(f.get("created_time", "") for f in last_page_fills)
        if p1_times[0] > pl_times[-1]:
            print("\n>>> ORDER: newest-first. Page 1 is the most recent data; "
                  "later pages go further back in time.")
            if cursor:
                print(">>> The cap being hit means OLD data is being cut off, "
                      "not recent data. 'Nothing since <date>' conclusions from "
                      "a capped fetch ARE trustworthy for recency questions.")
        elif pl_times[0] > p1_times[-1]:
            print("\n>>> ORDER: oldest-first. Page 1 is the OLDEST data; "
                  "later pages get more recent.")
            if cursor:
                print(">>> The cap being hit means RECENT data may be MISSING "
                      "entirely. Any 'nothing since <date>' conclusion drawn "
                      "from a capped fetch is NOT trustworthy — re-run with a "
                      "higher page cap or paginate from the other direction.")
        else:
            print("\n>>> Ranges overlap or are inconclusive — inspect manually.")


if __name__ == "__main__":
    main()
