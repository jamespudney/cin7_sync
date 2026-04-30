#!/usr/bin/env bash
# nearsync.sh — single near-real-time sync invocation.
# Pulls: stock + last-day stock adjustments + last-day sales (headers
# AND line items) + last-day purchases. Skips masters (handled by
# daily_sync). ~1-3 min, ~10 API calls. Designed to be safe to run
# every 15 minutes during the workday so the Ordering page sees
# accurate stock + same-day sales velocity.
set -uo pipefail

DATA_DIR="${DATA_DIR:-/data}"
LOG="${DATA_DIR}/output/nearsync.log"
mkdir -p "${DATA_DIR}/output"

stamp() { date -u +"%Y-%m-%dT%H:%M:%SZ"; }

echo "[$(stamp)] nearsync start" >> "$LOG"
python cin7_sync.py nearsync --days 1 >> "$LOG" 2>&1
RC=$?
if [ "$RC" -eq 0 ]; then
  echo "[$(stamp)] nearsync done (ok)" >> "$LOG"
else
  echo "[$(stamp)] nearsync exited rc=$RC" >> "$LOG"
fi
exit "$RC"
