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

# v2.67.209 — catch-up-on-boot. sync_loop only runs daily_sync.sh
# once a day at $SYNC_HOUR_UTC. But EVERY redeploy restarts this
# loop and recomputes "next target" — so a stretch of frequent
# deploys (e.g. the 20-commit weekend of the Postgres cutover)
# can starve daily_sync entirely: each restart after the daily
# hour pushes the run to "tomorrow", then the next deploy resets
# it again. Symptom James hit: sales_last_30d_*.csv never
# produced → Overview "Sales invoiced" tile stuck at $0.
#
# Fix: on startup, check whether the key daily-sync outputs are
# missing or stale (> CATCHUP_STALE_HOURS old, default 20h). If
# so, run daily_sync.sh ONCE immediately before entering the
# sleep loop. Mirrors slack_loop.sh's first-boot bootstrap.
CATCHUP_STALE_HOURS="${SYNC_CATCHUP_STALE_HOURS:-20}"
_catchup_needed=0
_freshest_sales=$(ls -t "${DATA_DIR}"/output/sales_last_30d_*.csv \
    2>/dev/null | head -1)
if [ -z "$_freshest_sales" ]; then
    echo "[$(stamp)] catch-up: no sales_last_30d_*.csv found" \
      | tee -a "$LOG"
    _catchup_needed=1
else
    _age_s=$(( $(date -u +%s) \
        - $(date -u -r "$_freshest_sales" +%s 2>/dev/null \
            || echo 0) ))
    _age_h=$(( _age_s / 3600 ))
    if [ "$_age_h" -ge "$CATCHUP_STALE_HOURS" ]; then
        echo "[$(stamp)] catch-up: sales CSV is ${_age_h}h old" \
          | tee -a "$LOG"
        _catchup_needed=1
    fi
fi
if [ "$_catchup_needed" = "1" ]; then
    echo "[$(stamp)] catch-up: running daily_sync.sh now" \
      | tee -a "$LOG"
    ./daily_sync.sh || \
      echo "[$(stamp)] catch-up daily_sync.sh non-zero status" \
        | tee -a "$LOG"
    python warm_engine.py 2>&1 | tee -a "$LOG" || true
else
    echo "[$(stamp)] catch-up: data fresh, no immediate sync" \
      | tee -a "$LOG"
fi

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

    # v2.67.36 — warm the ABC engine cache after every sync so the
    # first user post-sync gets a cache hit instead of waiting
    # 30-60s for the engine to recompute against fresh CSVs. Best-
    # effort; a failure here is logged but doesn't block the next
    # iteration.
    echo "[$(stamp)] warming engine cache" | tee -a "$LOG"
    python warm_engine.py 2>&1 | tee -a "$LOG" || \
      echo "[$(stamp)] warm_engine.py exited with non-zero status" \
        | tee -a "$LOG"

    # v2.67.38 — Friday weekly slow-mover digest email. Fires from
    # the same loop instead of a separate Render cron service.
    # Day-of-week check: %u returns 1=Mon ... 5=Fri ... 7=Sun.
    # The loop only fires once per day (after the SYNC_HOUR_UTC
    # daily_sync), so this gates exactly one send per Friday.
    # Silent no-op if SLOW_MOVERS_EMAIL_TO env var isn't set.
    if [ "$(date -u +%u)" = "5" ]; then
        echo "[$(stamp)] Friday — sending weekly slow-mover digest" \
          | tee -a "$LOG"
        python weekly_slow_movers_email.py 2>&1 | tee -a "$LOG" || \
          echo "[$(stamp)] weekly_slow_movers_email.py exited non-zero" \
            | tee -a "$LOG"
    fi
done
