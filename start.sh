#!/usr/bin/env bash
# start.sh — boot script used by Render's web service.
# Runs Streamlit + two sync loops inside one container.
#
# Why one process per concern is wrong here: Render disks can't be
# shared between services, so we'd lose data sharing if sync ran in
# a separate worker. Co-locating them is cheaper too.
#
# Process layout:
#   nearsync_loop.sh — runs every 15 min, pulls stock + last-day
#     sales/purchases. Critical for accurate Ordering page during
#     the workday. Logs to /data/output/nearsync_loop.log.
#   sync_loop.sh    — runs once at 02:00 UTC nightly, pulls full
#     masters + 3-day windows. Logs to /data/output/sync_loop.log.
#   streamlit       — foreground (exec). Must be foreground so
#     Render's health check on $PORT reaches it.
set -euo pipefail

# Make sure persistent-disk subdirectories exist on first boot.
mkdir -p "${DATA_DIR:-/data}/output"
mkdir -p "${DATA_DIR:-/data}/.streamlit"

# Stamp the web process with build metadata for the sidebar version chip.
# Render exposes RENDER_GIT_COMMIT on deploys; git fallback keeps local runs
# useful. APP_BUILD_DATE is the service start date, so it moves on every
# redeploy instead of relying on a manually bumped constant.
APP_BUILD_COMMIT="${APP_BUILD_COMMIT:-${RENDER_GIT_COMMIT:-}}"
if [ -z "$APP_BUILD_COMMIT" ] && command -v git >/dev/null 2>&1; then
    APP_BUILD_COMMIT="$(git rev-parse --short=7 HEAD 2>/dev/null || true)"
fi
export APP_BUILD_COMMIT="${APP_BUILD_COMMIT:0:7}"
export APP_BUILD_DATE="${APP_BUILD_DATE:-$(date +%Y-%m-%d)}"
echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] web build ${APP_BUILD_COMMIT:-unknown} deployed ${APP_BUILD_DATE}" \
    >> "${DATA_DIR:-/data}/output/build_info.log"

# v2.67.335 — one-time 365-day backfill of assemblies (FG-XXXX) the
# first time this boots after the v2.67.334 assembly-consumption
# pipeline ships. Without this, the engine only sees assemblies from
# the rolling daily/nearsync windows, so 12-month demand for
# assembly-heavy components (LED strips, profile parts) stays low
# until enough time has passed. James 2026-06-01 asked to backfill.
#
# Guarded by a marker file on the persistent disk so subsequent
# deploys / restarts skip it. Run in the background so Streamlit
# can start serving traffic immediately — the engine will pick up
# the new CSV on the next refresh (engine cache rebuild) once the
# backfill completes.
# v2.67.336 — marker bumped to _v2: the v2.67.335 run only fetched
# page 1 because the streak-break aborted on oldest-first sort.
# v2.67.336 fixes the pagination to walk every page.
_BF_MARKER="${DATA_DIR:-/data}/.assemblies_backfilled_v3"
_BF_LOG="${DATA_DIR:-/data}/output/assemblies_backfill.log"
if [ ! -f "$_BF_MARKER" ]; then
    (
        echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] starting 365d assembly backfill" >> "$_BF_LOG"
        if python cin7_sync.py assemblies --days 365 >> "$_BF_LOG" 2>&1; then
            touch "$_BF_MARKER"
            echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] backfill done; marker written" >> "$_BF_LOG"
        else
            echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] backfill FAILED (will retry next deploy)" >> "$_BF_LOG"
        fi
    ) &
fi

# v2.67.237 — supervise the sync loops. They are infinite while-
# loops and should never exit on their own, but if one ever does
# (crash, wedge cleared, OOM kill) it would otherwise stay dead
# until the next redeploy — which is exactly how nearsync
# silently stalled for ~24h. The supervisor restarts a loop 30s
# after any exit so the syncs are self-healing.
_supervise() {
    local name="$1"
    local script="$2"
    local log="${DATA_DIR:-/data}/output/${name}_loop.log"
    while true; do
        "$script" || true
        echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] [supervise] ${name}" \
             "loop exited — restarting in 30s" >> "$log"
        sleep 30
    done
}

# Start both sync loops under supervision, in the background.
# Trap SIGTERM so a clean Render restart kills all processes.
_supervise nearsync ./nearsync_loop.sh &
NEARSYNC_PID=$!
_supervise sync ./sync_loop.sh &
SYNC_PID=$!
trap "kill $NEARSYNC_PID $SYNC_PID 2>/dev/null || true" EXIT

# Streamlit in the foreground.
exec streamlit run app.py \
  --server.port "${PORT:-8501}" \
  --server.address 0.0.0.0 \
  --server.headless true \
  --browser.gatherUsageStats false
