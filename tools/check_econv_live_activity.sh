#!/usr/bin/env bash
# tools/check_econv_live_activity.sh
#
# Diagnostic only — read-only journalctl queries, no writes, no trading impact.
#
# Checks, in order:
#   1. How far back the systemd journal for `weathermachine` actually retains
#      logs. journalctl --since is a lie if the journal has already rotated
#      past that point — worth confirming before trusting any "0 matches"
#      result below as meaningful.
#   2. Whether evening_convergence.py's actual signal-detection line
#      (log.info("ECONV  %s  %s  No=%.2f ...") inside _check_city()) has
#      EVER printed. This is the exact moment a qualifying bracket is found,
#      before order placement is even attempted. Uses the literal "ECONV  "
#      (capital, two trailing spaces) so it can't collide with the routine
#      lowercase "econv=" budget-summary lines that print every poll cycle
#      regardless of whether evening_convergence found anything.
#   3. Whether the scheduler ever invoked evening_convergence.run_scan() at
#      all (scheduler.py's "[evening_convergence] starting" line). This is
#      log.debug level — if debug logging isn't enabled on this service,
#      an empty result here is INCONCLUSIVE, not damning, so it's reported
#      separately from the ECONV check above.
#
# NOTE: journal output is captured to a temp file once, then queried from
# there. Piping journalctl directly into head/tail under `set -o pipefail`
# causes journalctl to receive SIGPIPE when head/tail close the pipe early,
# which makes `set -e` abort the whole script silently — that was a real
# bug in the previous version of this script.
#
# Usage (on the Pi, from repo root):
#   bash tools/check_econv_live_activity.sh

set -euo pipefail

TMPFILE=$(mktemp)
trap 'rm -f "$TMPFILE"' EXIT

journalctl -u weathermachine --since "30 days ago" > "$TMPFILE"

echo "=== 1. Journal retention window for weathermachine.service ==="
LINE_COUNT=$(wc -l < "$TMPFILE")
echo "Total lines in window: $LINE_COUNT"
echo "First line: $(head -1 "$TMPFILE")"
echo "Last line:  $(tail -1 "$TMPFILE")"
echo ""

echo "=== 2. ECONV detection-line check (exact literal, full 30-day window) ==="
COUNT=$(grep -c "ECONV  " "$TMPFILE" || true)
echo "Matches: $COUNT"
if [ "$COUNT" -gt 0 ]; then
    echo ""
    echo "-- all matches --"
    grep "ECONV  " "$TMPFILE"
else
    echo "No matches — evening_convergence has never detected a qualifying setup"
    echo "in the retained journal window (see retention window above before"
    echo "treating this as conclusive)."
fi
echo ""

echo "=== 3. Scheduler invocation check (debug-level; empty result is inconclusive, not damning) ==="
DEBUG_COUNT=$(grep -c "evening_convergence\] starting" "$TMPFILE" || true)
echo "Matches: $DEBUG_COUNT"
if [ "$DEBUG_COUNT" -eq 0 ]; then
    echo "Zero matches likely just means debug logging isn't enabled for this"
    echo "service — check journalctl -u weathermachine | grep -i 'level' or"
    echo "the log_setup.py config if you want to confirm invocation directly."
fi
