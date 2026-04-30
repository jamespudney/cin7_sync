#!/usr/bin/env bash
# sync_loop.sh — runs forever as a Render background worker.
# Wakes up once a day at $SYNC_HOUR_UTC (default 02:00 UTC), runs
# daily_sync.sh, then sleeps until the next target.
#
# Why a loop instead of Render's native cron service: cron jobs on
# Render don't support persistent disks, but our sync needs to write
# to /data. Background workers DO support disks, so we run a worker
# that schedules itself.
set -uo pipefail

DATA_DIR="${DATA_DIR:-/data}"
SYNC_HOUR_UTC="${SYNC_HOUR_UTC:-2}"   # 0-23
LOG="${DATA_DIR}/output/sync_loop.log"
mkdir -p "${DATA_DIR}/output"

stamp() { date -u +"%Y-%m-%dT%H:%M:%SZ"; }

echo "[$(stamp)] sync_loop starting; target hour UTC = $SYNC_HOUR_UTC" \
  | tee -a "$LOG"

while true; do
    # Compute seconds until next $SYNC_HOUR_UTC:00:00.
    now_epoch=$(date -u +%s)
    today_target_epoch=$(date -u -d "today ${SYNC_HOUR_UTC}:00:00" +%s)
    if [ "$now_epoch" -ge "$today_target_epoch" ]; then
        # Already past today's target — aim for tomorrow.
        next_target_epoch=$(date -u -d "tomorrow ${SYNC_HOUR_UTC}:00:00" +%s)
    else
        next_target_epoch=$today_target_epoch
    fi
    sleep_seconds=$(( next_target_epoch - now_epoch ))
    next_target_iso=$(date -u -d "@${next_target_epoch}" +"%Y-%m-%dT%H:%M:%SZ")

    echo "[$(stamp)] sleeping ${sleep_seconds}s until ${next_target_iso}" \
      | tee -a "$LOG"
    sleep "$sleep_seconds"

    echo "[$(stamp)] running daily_sync.sh" | tee -a "$LOG"
    ./daily_sync.sh || \
      echo "[$(stamp)] daily_sync.sh exited with non-zero status" \
        | tee -a "$LOG"
done
