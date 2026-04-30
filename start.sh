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

# Start both sync loops in the background. Trap SIGTERM so a clean
# Render restart kills all processes together.
./nearsync_loop.sh &
NEARSYNC_PID=$!
./sync_loop.sh &
SYNC_PID=$!
trap "kill $NEARSYNC_PID $SYNC_PID 2>/dev/null || true" EXIT

# Streamlit in the foreground.
exec streamlit run app.py \
  --server.port "${PORT:-8501}" \
  --server.address 0.0.0.0 \
  --server.headless true \
  --browser.gatherUsageStats false
