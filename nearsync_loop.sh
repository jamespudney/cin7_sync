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
QBO_INTERVAL_HOURS="${QBO_CASHFLOW_INTERVAL_HOURS:-4}"
QBO_BOOT_DELAY_MIN="${QBO_CASHFLOW_BOOT_DELAY_MIN:-30}"
QBO_MONTHS_BACK="${QBO_CASHFLOW_MONTHS_BACK:-6}"
LOG="${DATA_DIR}/output/nearsync_loop.log"
QBO_LOG="${DATA_DIR}/output/qbo_cashflow_loop.log"
mkdir -p "${DATA_DIR}/output"

stamp() { date -u +"%Y-%m-%dT%H:%M:%SZ"; }

echo "[$(stamp)] nearsync_loop starting; interval = ${INTERVAL_MIN} min" \
  | tee -a "$LOG"
echo "[$(stamp)] QBO cashflow task: interval = ${QBO_INTERVAL_HOURS}h; boot delay = ${QBO_BOOT_DELAY_MIN} min; months_back = ${QBO_MONTHS_BACK}" \
  | tee -a "$QBO_LOG"

if ! [[ "$QBO_INTERVAL_HOURS" =~ ^[0-9]+$ ]] \
        || [ "$QBO_INTERVAL_HOURS" -lt 1 ]; then
    echo "[$(stamp)] invalid QBO_CASHFLOW_INTERVAL_HOURS='${QBO_INTERVAL_HOURS}', using 4" \
      | tee -a "$QBO_LOG"
    QBO_INTERVAL_HOURS=4
fi
if ! [[ "$QBO_BOOT_DELAY_MIN" =~ ^[0-9]+$ ]]; then
    echo "[$(stamp)] invalid QBO_CASHFLOW_BOOT_DELAY_MIN='${QBO_BOOT_DELAY_MIN}', using 30" \
      | tee -a "$QBO_LOG"
    QBO_BOOT_DELAY_MIN=30
fi
if ! [[ "$QBO_MONTHS_BACK" =~ ^[0-9]+$ ]] || [ "$QBO_MONTHS_BACK" -lt 1 ]; then
    echo "[$(stamp)] invalid QBO_CASHFLOW_MONTHS_BACK='${QBO_MONTHS_BACK}', using 6" \
      | tee -a "$QBO_LOG"
    QBO_MONTHS_BACK=6
fi

QBO_LOOP_STARTED_EPOCH=$(date -u +%s)

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

    # QBO supplier-payables refresh. This is deliberately hosted from
    # the existing near-sync loop rather than as a third supervised
    # process: QBO refreshes are useful a few times a day, but they
    # should not add startup memory pressure to the Streamlit container.
    QBO_INTERVAL_SECONDS=$((QBO_INTERVAL_HOURS * 3600))
    QBO_BOOT_DELAY_SECONDS=$((QBO_BOOT_DELAY_MIN * 60))
    QBO_NOW_EPOCH=$(date -u +%s)
    QBO_AGE_SECONDS=$((QBO_NOW_EPOCH - QBO_LOOP_STARTED_EPOCH))
    QBO_LAST_ATTEMPT="${DATA_DIR}/output/.qbo_cashflow_last_attempt"
    QBO_LOCK_DIR="${DATA_DIR}/output/.qbo_cashflow_sync.lock"
    QBO_RUN_NEEDED=0
    if [ "$QBO_AGE_SECONDS" -lt "$QBO_BOOT_DELAY_SECONDS" ]; then
        :
    elif [ ! -f "$QBO_LAST_ATTEMPT" ]; then
        QBO_RUN_NEEDED=1
    else
        QBO_LAST_EPOCH=$(date -u -r "$QBO_LAST_ATTEMPT" +%s 2>/dev/null \
            || echo 0)
        if [ $((QBO_NOW_EPOCH - QBO_LAST_EPOCH)) -ge "$QBO_INTERVAL_SECONDS" ]; then
            QBO_RUN_NEEDED=1
        fi
    fi

    if [ "$QBO_RUN_NEEDED" = "1" ]; then
        if [ -d "$QBO_LOCK_DIR" ]; then
            QBO_LOCK_EPOCH=$(date -u -r "$QBO_LOCK_DIR" +%s 2>/dev/null \
                || echo "$QBO_NOW_EPOCH")
            if [ $((QBO_NOW_EPOCH - QBO_LOCK_EPOCH)) -gt 1800 ]; then
                rmdir "$QBO_LOCK_DIR" 2>/dev/null || true
            fi
        fi
        if mkdir "$QBO_LOCK_DIR" 2>/dev/null; then
            touch "$QBO_LAST_ATTEMPT"
            echo "[$(stamp)] QBO cashflow sync starting (detached)" \
              | tee -a "$QBO_LOG"
            (
                trap 'rmdir "$QBO_LOCK_DIR" 2>/dev/null || true' EXIT
                timeout 300 python cashflow_sync.py sync \
                    --months-back "$QBO_MONTHS_BACK" >> "$QBO_LOG" 2>&1 \
                    || echo "[$(stamp)] QBO cashflow sync failed/timed out" \
                        >> "$QBO_LOG"
            ) &
        else
            echo "[$(stamp)] QBO cashflow sync already running; skipped" \
              | tee -a "$QBO_LOG"
        fi
    fi

    sleep $((INTERVAL_MIN * 60))
done
