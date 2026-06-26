"""
probe_settlements.py
--------------------
Diagnostic for the "position shows OPEN even though it settled days ago" bug.

Run from the project root on the Pi:
    python3 probe_settlements.py

It pulls the FULL settlements list from Kalshi (no 15-page cap), reports
the total record count, and checks whether the dashboard's current
15-page (3,000-record) cap is dropping older settlements.
"""

import sys
sys.path.insert(0, ".")

import dashboard   # reuse the dashboard's client + config loader

c = dashboard.get_client()
if not c:
    print("No client — check config.json")
    sys.exit(1)

all_s, cursor, pages = [], None, 0
while True:
    p = {"limit": 200}
    if cursor:
        p["cursor"] = cursor
    d = c.get("portfolio/settlements", params=p)
    b = d.get("settlements", [])
    all_s.extend(b)
    pages += 1
    cursor = d.get("cursor")
    if not cursor or len(b) < 200:
        break
    if pages > 200:                      # safety stop
        print("stopped at 200 pages")
        break

print(f"TOTAL settlement records on account: {len(all_s)}  (in {pages} pages)")

temp = [s for s in all_s if s.get("ticker", "").startswith("KX")
        and ("HIGH" in s.get("ticker", "") or "LOWT" in s.get("ticker", ""))
        and "TEMPNYCH" not in s.get("ticker", "")]
print(f"KX HIGH/LOWT temp settlements: {len(temp)}")

print("Dashboard currently caps at 15 pages = 3000 records.")
print(f"  -> hitting cap? "
      f"{'YES — older settlements DROPPED' if len(all_s) > 3000 else 'no'}")

dates = sorted(s.get("settled_time", "")[:10] for s in temp if s.get("settled_time"))
if dates:
    print(f"Temp settlement date range retrieved: {dates[0]} .. {dates[-1]}")

# The specific symptom: a Jun 20 Chicago position showing OPEN
chi = [s for s in temp if "CHI" in s.get("ticker", "")
       and s.get("settled_time", "")[:10].endswith(("-20", "-21"))]
print(f"\nChicago temp settlements dated ~Jun 20-21 found: {len(chi)}")
for s in sorted(chi, key=lambda x: x.get("settled_time", ""))[:15]:
    print("  ", s.get("ticker"), s.get("settled_time", "")[:10])
