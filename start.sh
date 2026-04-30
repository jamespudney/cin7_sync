#!/usr/bin/env bash
# start.sh — boot script used by Render's web service.
# Runs both Streamlit AND the daily-sync loop inside one container.
#
# Why one process per concern is wrong here: Render disks can't be
# shared between services, so we'd lose data sharing if sync ran in
# a separate worker. Co-locating them is cheaper too.
#
# Process layout:
#   sync_loop.sh runs as a background process (&). It sleeps until
#     SYNC_HOUR_UTC, runs daily_sync.sh, then sleeps again. Logs go
#     to /data/output/sync_loop.log.
#   Streamlit runs in the foreground (exec). Must be foreground so
#     Render's health check on $PORT reaches it.
#
# Render sets $PORT to whatever port we should listen on. Streamlit
# binds to 0.0.0.0 (not localhost) so Render's load balancer can reach.
set -euo pipefail

# Make sure persistent-disk subdirectories exist on first boot.
mkdir -p "${DATA_DIR:-/data}/output"
mkdir -p "${DATA_DIR:-/data}/.streamlit"

# Start the sync loop in the background. trap SIGTERM so a clean
# Render restart kills both processes together.
./sync_loop.sh &
SYNC_PID=$!
trap "kill $SYNC_PID 2>/dev/null || true" EXIT

# Streamlit in the foreground.
exec streamlit run app.py \
  --server.port "${PORT:-8501}" \
  --server.address 0.0.0.0 \
  --server.headless true \
  --browser.gatherUsageStats false
