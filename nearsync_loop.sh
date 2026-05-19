#!/usr/bin/env bash
# nearsync_loop.sh — runs nearsync.sh every NEARSYNC_INTERVAL_MIN
# minutes (default 15) in an infinite loop. Started as a background
# process by start.sh.
#
# Why a loop and not a Render cron job: Render cron jobs can't share
# the persistent disk with the web service. We run inside the web
# service's container so we share /data.
set -uo pipefail

DATA_DIR="${DATA_DIR:-/data}"
INTERVAL_MIN="${NEARSYNC_INTERVAL_MIN:-15}"
LOG="${DATA_DIR}/output/nearsync_loop.log"
mkdir -p "${DATA_DIR}/output"

stamp() { date -u +"%Y-%m-%dT%H:%M:%SZ"; }

echo "[$(stamp)] nearsync_loop starting; interval = ${INTERVAL_MIN} min" \
  | tee -a "$LOG"

# Wait a few seconds before the first run so Streamlit has time to boot
# (avoids both processes hammering CIN7 + the disk at the same instant).
sleep 30

while true; do
    HOUR=$(date -u +%H)
    HOUR_NUM=$((10#$HOUR))
    DAILY_HOUR_NUM=$((10#${SYNC_HOUR_UTC:-2}))
    # Skip nearsync during the daily-sync window (02:00–02:59 UTC by
    # default). The daily sync is heavy and we don't want them
    # competing for CIN7 rate budget at the same time.
    if [ "$HOUR_NUM" -eq "$DAILY_HOUR_NUM" ]; then
        echo "[$(stamp)] in daily-sync hour, skipping nearsync" \
          | tee -a "$LOG"
    else
        ./nearsync.sh || \
          echo "[$(stamp)] nearsync.sh exited non-zero" >> "$LOG"
    fi

    # v2.67.230 — Monday bank-balance capture. Once per Monday at
    # ~13:00 UTC (08:00 EST / 09:00 EDT) sum the QBO bank-account
    # balances into the cashflow week's opening_balance cell.
    #
    # v2.67.237 — run it DETACHED with a hard timeout. The first
    # version ran `python capture_bank_balance.py` in the
    # FOREGROUND with no timeout; a hanging QBO call on Mon
    # 2026-05-18 wedged the entire 15-min nearsync loop for ~24h.
    # Now: write the sentinel up-front (so a failure can't retry-
    # spam), then launch the capture in a backgrounded subshell
    # with `timeout` — the loop proceeds to its sleep immediately
    # and can never be blocked by this step.
    DOW=$(date -u +%u)   # 1 = Monday
    CAP_SENTINEL="${DATA_DIR}/output/.bank_capture_$(date -u +%Y-%m-%d)"
    if [ "$DOW" = "1" ] && [ "$HOUR_NUM" -ge 13 ] \
            && [ ! -f "$CAP_SENTINEL" ]; then
        echo "[$(stamp)] Monday — bank-balance capture (detached)" \
          | tee -a "$LOG"
        touch "$CAP_SENTINEL"
        ( timeout 240 python capture_bank_balance.py \
            >> "$LOG" 2>&1 \
          || echo "[$(stamp)] capture_bank_balance.py failed/" \
                  "timed out" >> "$LOG" ) &
    fi

    sleep $((INTERVAL_MIN * 60))
done
