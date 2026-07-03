#!/usr/bin/env bash
# tools/check_near_cap_journal_clues.sh
#
# Follow-up to tools/check_near_cap_unexplained_cases.py. That script found
# 1 of 5 unexplained near_cap cases explained by the already_traded dedup
# (New Orleans 2026-06-06). The remaining 4 split into two patterns:
#
#   - New Orleans 2026-06-04 and New York 2026-06-10: zero trades from ANY
#     engine that day (citywide silence — check for a pause/outage).
#   - Los Angeles 2026-06-19 and San Francisco 2026-07-02: other engines
#     traded that city that day, but zero hight_decision_engine-family
#     trades (main/near_cap/topup) — check whether hight_decision_engine.py
#     itself threw an exception before ever reaching the near_cap block.
#
# This script re-uses the EXISTING journal dump at /tmp/journal_dump.txt
# (from the earlier econv investigation) — no new `journalctl` pull needed.
# Read-only, no writes, no trading impact.
#
# Usage (on the Pi, from repo root):
#   bash tools/check_near_cap_journal_clues.sh
#   bash tools/check_near_cap_journal_clues.sh /path/to/other_dump.txt   # override

set -uo pipefail   # deliberately NOT using -e here — grep returning "no
                    # matches" (exit 1) is an expected, informative outcome
                    # in this script, not a failure. This is exactly the
                    # SIGPIPE/pipefail lesson from check_econv_live_activity.sh:
                    # don't let a command's "nothing found" exit code kill
                    # the whole script.

DUMP="${1:-/tmp/journal_dump.txt}"

if [ ! -f "$DUMP" ]; then
    echo "ERROR: $DUMP not found."
    echo "Either re-run the econv journal dump command, or pass a path:"
    echo "  bash tools/check_near_cap_journal_clues.sh /path/to/dump.txt"
    exit 1
fi

echo "Using journal dump: $DUMP"
echo ""

echo "=== 1. Citywide activity check: New Orleans, 2026-06-04 ==="
echo "(any hight_decision_engine log line mentioning this city on this date)"
MATCHES=$(grep "hight_decision_engine" "$DUMP" | grep "2026-06-04" | grep -i "new orleans\|nola")
COUNT=$(printf '%s\n' "$MATCHES" | grep -c . || true)
echo "Matches: $COUNT"
[ -n "$MATCHES" ] && printf '%s\n' "$MATCHES" | head -20
echo ""

echo "=== 2. Citywide activity check: New York, 2026-06-10 ==="
MATCHES=$(grep "hight_decision_engine" "$DUMP" | grep "2026-06-10" | grep -i "new york")
COUNT=$(printf '%s\n' "$MATCHES" | grep -c . || true)
echo "Matches: $COUNT"
[ -n "$MATCHES" ] && printf '%s\n' "$MATCHES" | head -20
echo ""

echo "=== 3. Exception/error check near Los Angeles, 2026-06-19 ==="
MATCHES=$(grep -i "traceback\|exception\|error" "$DUMP" | grep "2026-06-19")
COUNT=$(printf '%s\n' "$MATCHES" | grep -c . || true)
echo "Matches: $COUNT"
[ -n "$MATCHES" ] && printf '%s\n' "$MATCHES" | head -30
echo ""

echo "=== 4. Exception/error check near San Francisco, 2026-07-02 ==="
MATCHES=$(grep -i "traceback\|exception\|error" "$DUMP" | grep "2026-07-02")
COUNT=$(printf '%s\n' "$MATCHES" | grep -c . || true)
echo "Matches: $COUNT"
[ -n "$MATCHES" ] && printf '%s\n' "$MATCHES" | head -30
echo ""

echo "=== 5. Any ERROR-level line from hight_decision_engine at all, full 30-day window ==="
MATCHES=$(grep "hight_decision_engine" "$DUMP" | grep -i "error\|exception\|traceback")
COUNT=$(printf '%s\n' "$MATCHES" | grep -c . || true)
echo "Matches: $COUNT"
if [ -n "$MATCHES" ]; then
    printf '%s\n' "$MATCHES" | head -30
else
    echo "No error-level hight_decision_engine lines in the whole 30-day window —"
    echo "if sections 3/4 above also came back empty, an uncaught exception is"
    echo "unlikely to be the explanation for the LA/SF cases; something quieter"
    echo "is more likely (e.g. the main engine's own gates legitimately found"
    echo "nothing that day for unrelated reasons)."
fi
