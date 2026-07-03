"""
tools/check_econv_capital_at_signal.py

Correlates every "ECONV" signal-detection log line with the most recent
"EngineCapital:" budget-summary line before it, to test a specific
hypothesis: that evening_convergence.py's signals are being detected
correctly (confirmed: 64 ECONV lines in the last 30 days) but silently
rejected by the capital check (cap.can_deploy("econv", cost)) because the
econv budget share is smaller than the cost of a single entry on the
evenings in question.

IMPORTANT — read this before trusting the output:
EngineCapital.summary() formats each engine as REMAINING/BUDGET, not
DEPLOYED/BUDGET (verified against trader.py's EngineCapital.summary()
source directly — this script matches that, not the surface reading of
"econv=$X/$X" as "fully deployed", which was an earlier misread and IS
WRONG). remaining == budget just means $0 has been deployed on that engine
so far today, which is consistent with zero completed econv trades on
its own — it does NOT by itself prove or disprove the capital-blocking
hypothesis. What proves or disproves it is whether `remaining` was ever
LESS than the cost of the specific order evening_convergence attempted.

Cost is computed exactly as evening_convergence.py computes it:
    cost = round(no_p * contracts, 4)

Usage (on the Pi, from repo root):
    journalctl -u weathermachine --since "30 days ago" > /tmp/journal_dump.txt
    python3 tools/check_econv_capital_at_signal.py /tmp/journal_dump.txt

    # or pipe directly:
    journalctl -u weathermachine --since "30 days ago" | python3 tools/check_econv_capital_at_signal.py -
"""

import re
import sys
from datetime import datetime

# Matches the inner ISO-ish timestamp in the log message body, e.g.
# "2026-06-28 00:00:37 UTC" — more reliable than the syslog prefix since
# it's the timestamp the application itself recorded.
TS_RE = re.compile(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) UTC")

ECONV_MARKER = "ECONV  "
CAPITAL_MARKER = "EngineCapital:"
ECONV_BUDGET_RE = re.compile(r"econv=\$([\d.]+)/([\d.]+)")


def parse_timestamp(line: str):
    m = TS_RE.search(line)
    if not m:
        return None
    return datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S")


def parse_econv_line(line: str):
    """Extract (timestamp, city, ticker, no_price, contracts) from an ECONV line."""
    ts = parse_timestamp(line)
    if ts is None:
        return None
    idx = line.find(ECONV_MARKER)
    if idx == -1:
        return None
    # Fields are double-space delimited in the source format string.
    fields = [f for f in line[idx:].split("  ") if f]
    # fields[0] = "ECONV", fields[1] = city, fields[2] = ticker,
    # fields[3] = "No=0.92", ..., last field = "4c"
    if len(fields) < 4:
        return None
    city = fields[1]
    ticker = fields[2]
    no_field = fields[3]
    contracts_field = fields[-1].strip()
    try:
        no_price = float(no_field.split("=")[1])
    except (IndexError, ValueError):
        return None
    try:
        contracts = int(contracts_field.rstrip("c"))
    except ValueError:
        contracts = None
    return {
        "ts": ts, "city": city, "ticker": ticker,
        "no_price": no_price, "contracts": contracts,
    }


def parse_capital_line(line: str):
    ts = parse_timestamp(line)
    if ts is None or CAPITAL_MARKER not in line:
        return None
    m = ECONV_BUDGET_RE.search(line)
    if not m:
        return None
    remaining, budget = float(m.group(1)), float(m.group(2))
    return {"ts": ts, "remaining": remaining, "budget": budget}


def main():
    if len(sys.argv) < 2:
        print("Usage: check_econv_capital_at_signal.py <journal_dump.txt | ->")
        sys.exit(1)

    src = sys.stdin if sys.argv[1] == "-" else open(sys.argv[1], "r", errors="replace")

    econv_events = []
    capital_events = []
    for line in src:
        if ECONV_MARKER in line:
            ev = parse_econv_line(line)
            if ev:
                econv_events.append(ev)
        elif CAPITAL_MARKER in line:
            ev = parse_capital_line(line)
            if ev:
                capital_events.append(ev)

    capital_events.sort(key=lambda e: e["ts"])
    econv_events.sort(key=lambda e: e["ts"])

    print(f"Parsed {len(econv_events)} ECONV detection lines and "
          f"{len(capital_events)} EngineCapital budget lines.\n")

    if not econv_events:
        print("No ECONV lines parsed — check the input file/marker format.")
        return
    if not capital_events:
        print("No EngineCapital lines parsed — cannot correlate. Check that "
              "the journal dump includes trader.py's periodic budget logging.")
        return

    blocked = []
    ok = []
    no_prior_capital_line = []

    ci = 0  # pointer into capital_events, advanced as we scan econv_events in time order
    last_capital = None
    for ev in econv_events:
        while ci < len(capital_events) and capital_events[ci]["ts"] <= ev["ts"]:
            last_capital = capital_events[ci]
            ci += 1

        if last_capital is None:
            no_prior_capital_line.append(ev)
            continue

        cost = round(ev["no_price"] * (ev["contracts"] or 4), 4)
        gap_seconds = (ev["ts"] - last_capital["ts"]).total_seconds()

        record = {
            **ev,
            "cost": cost,
            "remaining_at_signal": last_capital["remaining"],
            "budget_at_signal": last_capital["budget"],
            "capital_line_age_sec": gap_seconds,
        }

        if last_capital["remaining"] < cost:
            blocked.append(record)
        else:
            ok.append(record)

    print(f"ECONV signals with a prior EngineCapital reading:     {len(blocked) + len(ok)}")
    print(f"  -> econv remaining < order cost (capital-blocked):  {len(blocked)}")
    print(f"  -> econv remaining >= order cost (should have gone through): {len(ok)}")
    print(f"ECONV signals with NO prior EngineCapital line at all: {len(no_prior_capital_line)}")

    if blocked:
        print("\n--- Capital-blocked examples (up to 15) ---")
        for r in blocked[:15]:
            print(f"  {r['ts']}  {r['city']:15s} {r['ticker']:30s} "
                  f"cost=${r['cost']:.2f}  remaining=${r['remaining_at_signal']:.2f}/"
                  f"{r['budget_at_signal']:.2f}  (capital line {r['capital_line_age_sec']:.0f}s earlier)")

    if ok:
        print("\n--- UNEXPLAINED: signals that should have gone through but didn't fire a trade (up to 15) ---")
        print("(these had enough remaining budget — if trade_log.json still shows 0 econv")
        print(" trades for these, the bug is NOT capital, it's somewhere else — order")
        print(" placement, an uncaught exception, or _fired dedup)")
        for r in ok[:15]:
            print(f"  {r['ts']}  {r['city']:15s} {r['ticker']:30s} "
                  f"cost=${r['cost']:.2f}  remaining=${r['remaining_at_signal']:.2f}/"
                  f"{r['budget_at_signal']:.2f}  (capital line {r['capital_line_age_sec']:.0f}s earlier)")


if __name__ == "__main__":
    main()
