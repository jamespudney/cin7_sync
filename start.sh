#!/usr/bin/env bash
# start.sh — boot script used by Render's web service.
# Render sets $PORT to whatever port we should listen on. Streamlit must
# bind to 0.0.0.0 (not localhost) so Render's load balancer can reach it.
set -euo pipefail

# Make sure the persistent disk's subdirectories exist on first boot.
mkdir -p "${DATA_DIR:-/data}/output"
mkdir -p "${DATA_DIR:-/data}/.streamlit"

exec streamlit run app.py \
  --server.port "${PORT:-8501}" \
  --server.address 0.0.0.0 \
  --server.headless true \
  --server.enableCORS false \
  --server.enableXsrfProtection true \
  --browser.gatherUsageStats false
