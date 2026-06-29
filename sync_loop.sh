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
WARM_ENGINE_BOOT_DELAY_MIN="${WARM_ENGINE_BOOT_DELAY_MIN:-30}"
WARM_ENGINE_TIMEOUT_SECONDS="${WARM_ENGINE_TIMEOUT_SECONDS:-1200}"
WARM_ENGINE_MIN_AVAILABLE_MB="${WARM_ENGINE_MIN_AVAILABLE_MB:-2500}"
LOG="${DATA_DIR}/output/sync_loop.log"
mkdir -p "${DATA_DIR}/output"

stamp() { date -u +"%Y-%m-%dT%H:%M:%SZ"; }

echo "[$(stamp)] sync_loop starting; target hour UTC = $SYNC_HOUR_UTC" \
  | tee -a "$LOG"

_engine_refresh_running() {
    local lock="${DATA_DIR}/output/engine_refresh.lock"
    if [ ! -f "$lock" ]; then
        return 1
    fi
    local now_epoch lock_epoch age_s
    now_epoch=$(date -u +%s)
    lock_epoch=$(date -u -r "$lock" +%s 2>/dev/null || echo 0)
    age_s=$((now_epoch - lock_epoch))
    if [ "$age_s" -le 2700 ]; then
        return 0
    fi
    rm -f "$lock" 2>/dev/null || true
    return 1
}

_start_warm_engine() {
    local reason="$1"
    local delay_seconds="${2:-0}"
    local lock="${DATA_DIR}/output/engine_refresh.lock"
    local status="${DATA_DIR}/output/engine_refresh_status.json"
    local engine_log="${DATA_DIR}/output/engine_refresh.log"

    if [ "${WARM_ENGINE_ALLOW_STALE_INPUTS:-0}" != "1" ] && \
       ! _engine_inputs_ready; then
        echo "[$(stamp)] warm_engine skipped: core sync inputs missing/stale (${reason})" \
          | tee -a "$LOG"
        return 0
    fi

    if _engine_refresh_running; then
        echo "[$(stamp)] warm_engine already running; skipped (${reason})" \
          | tee -a "$LOG"
        return 0
    fi

    (
        if [ "$delay_seconds" -gt 0 ]; then
            echo "[$(stamp)] warm_engine scheduled in ${delay_seconds}s (${reason})" \
              >> "$LOG"
            sleep "$delay_seconds"
        fi
        if _engine_refresh_running; then
            echo "[$(stamp)] warm_engine already running after delay; skipped (${reason})" \
              >> "$LOG"
            exit 0
        fi

        printf '{"state":"running","reason":"%s","updated_at":"%s"}\n' \
            "$reason" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" > "$lock"
        cp "$lock" "$status" 2>/dev/null || true

        echo "[$(stamp)] warming engine cache (${reason})" \
          | tee -a "$LOG" >> "$engine_log"
        ENGINE_REFRESH_LOCK_PATH="$lock" \
        ENGINE_REFRESH_STATUS_PATH="$status" \
        ENGINE_REFRESH_REASON="$reason" \
        WARM_ENGINE_MIN_AVAILABLE_MB="$WARM_ENGINE_MIN_AVAILABLE_MB" \
        timeout "$WARM_ENGINE_TIMEOUT_SECONDS" python warm_engine.py \
            >> "$engine_log" 2>&1
        rc=$?
        if [ "$rc" -ne 0 ]; then
            echo "[$(stamp)] warm_engine failed/timed out (${reason}, rc=${rc})" \
              | tee -a "$LOG" >> "$engine_log"
            rm -f "$lock" 2>/dev/null || true
            printf '{"state":"failed","reason":"%s","exit_code":%s,"updated_at":"%s"}\n' \
                "$reason" "$rc" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
                > "$status"
        fi
    ) &
}

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

_check_daily_output_fresh() {
    local pattern="$1"
    local label="$2"
    local freshest
    freshest=$(ls -t "${DATA_DIR}"/output/${pattern} \
        2>/dev/null | head -1)
    if [ -z "$freshest" ]; then
        echo "[$(stamp)] catch-up: no ${label} found" \
          | tee -a "$LOG"
        _catchup_needed=1
        return
    fi
    local age_s
    local age_h
    age_s=$(( $(date -u +%s) \
        - $(date -u -r "$freshest" +%s 2>/dev/null || echo 0) ))
    age_h=$(( age_s / 3600 ))
    if [ "$age_h" -ge "$CATCHUP_STALE_HOURS" ]; then
        echo "[$(stamp)] catch-up: ${label} is ${age_h}h old" \
          | tee -a "$LOG"
        _catchup_needed=1
    fi
}

_check_daily_output_fresh "sales_last_30d_*.csv" "sales_last_30d CSV"
_check_daily_output_fresh \
    "sale_lines_last_30d_*.csv" "sale_lines_last_30d CSV"
_check_daily_output_fresh \
    "assemblies_last_30d_*.csv" "assemblies_last_30d CSV"

_engine_input_fresh() {
    local pattern="$1"
    local label="$2"
    local max_age_h="${3:-$CATCHUP_STALE_HOURS}"
    local freshest
    freshest=$(ls -t "${DATA_DIR}"/output/${pattern} \
        2>/dev/null | head -1)
    if [ -z "$freshest" ]; then
        echo "[$(stamp)] engine input missing: ${label}" \
          | tee -a "$LOG"
        return 1
    fi
    local age_s
    local age_h
    age_s=$(( $(date -u +%s) \
        - $(date -u -r "$freshest" +%s 2>/dev/null || echo 0) ))
    age_h=$(( age_s / 3600 ))
    if [ "$age_h" -ge "$max_age_h" ]; then
        echo "[$(stamp)] engine input stale: ${label} is ${age_h}h old" \
          | tee -a "$LOG"
        return 1
    fi
    return 0
}

_engine_inputs_ready() {
    _engine_input_fresh "sales_last_30d_*.csv" "sales_last_30d CSV" || return 1
    _engine_input_fresh "sale_lines_last_30d_*.csv" "sale_lines_last_30d CSV" || return 1
    _engine_input_fresh "assemblies_last_30d_*.csv" "assemblies_last_30d CSV" || return 1
    return 0
}

if [ "$_catchup_needed" = "1" ]; then
    echo "[$(stamp)] catch-up: running daily_sync.sh now" \
      | tee -a "$LOG"
    ./daily_sync.sh || \
      echo "[$(stamp)] catch-up daily_sync.sh non-zero status" \
        | tee -a "$LOG"
    _start_warm_engine "sync_loop catch-up" \
        $((WARM_ENGINE_BOOT_DELAY_MIN * 60))
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

    # Warm the ABC engine cache after every sync so the first user
    # post-sync gets a cache hit instead of waiting for the engine to
    # recompute. It runs detached with a lock, timeout and memory guard
    # so it cannot block the sync loop or pile onto Streamlit startup.
    _start_warm_engine "daily sync completed" 0

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
