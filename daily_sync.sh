#!/usr/bin/env bash
# daily_sync.sh — Linux equivalent of daily_sync.bat.
# Run by Render's cron service every day at 02:00 UTC.
#
# Steps:
#   1. CIN7 masters + 3 days of sales/purchase headers (the same "quick"
#      sync the Windows .bat ran).
#   2. Drift catchers — propagate any SKU renames or supplier-name
#      changes that CIN7 made yesterday into our local DB.
#
# Logs go to /data/output/daily_sync.log (on the persistent disk so
# you can review past runs from inside the app or via the Render shell).
#
# Failure policy: every step uses `|| true` so a single failure doesn't
# block the rest of the chain. The next day's run will retry. Render
# will also email you the cron job's exit code.

set -uo pipefail

DATA_DIR="${DATA_DIR:-/data}"
LOG="${DATA_DIR}/output/daily_sync.log"
mkdir -p "${DATA_DIR}/output"

stamp() { date -u +"%Y-%m-%dT%H:%M:%SZ"; }

echo "" >> "$LOG"
echo "============================================================" >> "$LOG"
echo "  daily_sync.sh start at $(stamp)" >> "$LOG"
echo "============================================================" >> "$LOG"

echo "[$(stamp)] cin7_sync quick --days 3" >> "$LOG"
python cin7_sync.py quick --days 3 >> "$LOG" 2>&1 || \
  echo "[$(stamp)] cin7_sync quick FAILED (continuing)" >> "$LOG"

echo "[$(stamp)] sync_sku_renames" >> "$LOG"
python sync_sku_renames.py --apply >> "$LOG" 2>&1 || \
  echo "[$(stamp)] sync_sku_renames FAILED (continuing)" >> "$LOG"

echo "[$(stamp)] sync_supplier_names" >> "$LOG"
python sync_supplier_names.py --apply >> "$LOG" 2>&1 || \
  echo "[$(stamp)] sync_supplier_names FAILED (continuing)" >> "$LOG"

echo "[$(stamp)] daily_sync.sh done" >> "$LOG"
echo "" >> "$LOG"
