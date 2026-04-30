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
    sleep $((INTERVAL_MIN * 60))
done
