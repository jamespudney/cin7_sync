#!/usr/bin/env bash
# nearsync.sh — single near-real-time sync invocation.
# Pulls: stock + last-day stock adjustments + last-day sales (headers
# AND line items) + last-day purchases. Skips masters (handled by
# daily_sync). ~1-3 min, ~10 API calls. Designed to be safe to run
# every 15 minutes during the workday so the Ordering page sees
# accurate stock + same-day sales velocity.
set -uo pipefail

DATA_DIR="${DATA_DIR:-/data}"
LOG="${DATA_DIR}/output/nearsync.log"
mkdir -p "${DATA_DIR}/output"

stamp() { date -u +"%Y-%m-%dT%H:%M:%SZ"; }

echo "[$(stamp)] nearsync start" >> "$LOG"
python cin7_sync.py nearsync --days 1 >> "$LOG" 2>&1
RC=$?
if [ "$RC" -eq 0 ]; then
  echo "[$(stamp)] nearsync done (ok)" >> "$LOG"
  if [ "${WARM_ENGINE_AFTER_NEARSYNC:-1}" = "1" ]; then
    echo "[$(stamp)] warm_engine after nearsync" >> "$LOG"
    python warm_engine.py >> "$LOG" 2>&1 || \
      echo "[$(stamp)] warm_engine after nearsync FAILED (continuing)" \
        >> "$LOG"
  fi
else
  echo "[$(stamp)] nearsync exited rc=$RC" >> "$LOG"
fi

# v2.67.54 — ShipStation 1-day catch-up. Picks up shipments
# created since the last run so the AI can answer "where's my
# shipment" questions within 15 minutes of the carrier label
# creation. No-ops when env vars aren't set; failure here doesn't
# affect the CIN7 nearsync exit code (we already captured RC).
echo "[$(stamp)] shipstation_sync recent --days 1" >> "$LOG"
python shipstation_sync.py recent --days 1 >> "$LOG" 2>&1 || \
  echo "[$(stamp)] shipstation_sync FAILED (continuing)" >> "$LOG"

# v2.67.55 — Shopify orders 1-day catch-up for conversion-
# attribution AI answers. Same gating pattern; same no-fail
# semantics (we already captured the CIN7 RC above).
if [ -n "${SHOPIFY_DOMAIN:-}" ] && [ -n "${SHOPIFY_ACCESS_TOKEN:-}" ]; then
    echo "[$(stamp)] shopify_sync --orders-recent 1" >> "$LOG"
    python shopify_sync.py --orders-recent 1 >> "$LOG" 2>&1 || \
      echo "[$(stamp)] shopify_orders FAILED (continuing)" >> "$LOG"
fi

exit "$RC"
