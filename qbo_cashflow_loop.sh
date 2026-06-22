#!/usr/bin/env bash
# qbo_cashflow_loop.sh — refresh QuickBooks Online supplier bills for
# the Cashflow page every few hours. Started and supervised by start.sh.
#
# This runs in the web service, not a Render cron job, because the
# cashflow tables live on the same persistent disk/database context as
# the Streamlit app.
set -uo pipefail

DATA_DIR="${DATA_DIR:-/data}"
INTERVAL_HOURS="${QBO_CASHFLOW_INTERVAL_HOURS:-4}"
MONTHS_BACK="${QBO_CASHFLOW_MONTHS_BACK:-6}"
LOG="${DATA_DIR}/output/qbo_cashflow_loop.log"
mkdir -p "${DATA_DIR}/output"

stamp() { date -u +"%Y-%m-%dT%H:%M:%SZ"; }

if ! [[ "$INTERVAL_HOURS" =~ ^[0-9]+$ ]] \
        || [ "$INTERVAL_HOURS" -lt 1 ]; then
    echo "[$(stamp)] invalid QBO_CASHFLOW_INTERVAL_HOURS='${INTERVAL_HOURS}', using 4" \
      | tee -a "$LOG"
    INTERVAL_HOURS=4
fi

if ! [[ "$MONTHS_BACK" =~ ^[0-9]+$ ]] || [ "$MONTHS_BACK" -lt 1 ]; then
    echo "[$(stamp)] invalid QBO_CASHFLOW_MONTHS_BACK='${MONTHS_BACK}', using 6" \
      | tee -a "$LOG"
    MONTHS_BACK=6
fi

echo "[$(stamp)] qbo_cashflow_loop starting; interval = ${INTERVAL_HOURS}h; months_back = ${MONTHS_BACK}" \
  | tee -a "$LOG"

# Let Streamlit and the CIN7 near-sync loop start first, then refresh QBO.
sleep 90

while true; do
    echo "[$(stamp)] running QBO cashflow sync" | tee -a "$LOG"
    timeout 300 python cashflow_sync.py sync --months-back "$MONTHS_BACK" \
      >> "$LOG" 2>&1 || \
      echo "[$(stamp)] QBO cashflow sync failed/timed out" | tee -a "$LOG"

    echo "[$(stamp)] sleeping $((INTERVAL_HOURS * 3600))s until next QBO cashflow sync" \
      | tee -a "$LOG"
    sleep $((INTERVAL_HOURS * 3600))
done
