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

# Sale lines feed the ABC engine, customer rollups, velocity. The
# `quick` sync above pulls sale headers but NOT line items. Without
# this incremental pull, line-level data goes stale by ~1 day per day.
echo "[$(stamp)] cin7_sync salelines --days 3" >> "$LOG"
python cin7_sync.py salelines --days 3 >> "$LOG" 2>&1 || \
  echo "[$(stamp)] cin7_sync salelines FAILED (continuing)" >> "$LOG"

# Purchase lines feed FixedCost audit and supplier-pricing audits.
echo "[$(stamp)] cin7_sync purchaselines --days 7" >> "$LOG"
python cin7_sync.py purchaselines --days 7 >> "$LOG" 2>&1 || \
  echo "[$(stamp)] cin7_sync purchaselines FAILED (continuing)" >> "$LOG"

echo "[$(stamp)] sync_sku_renames" >> "$LOG"
python sync_sku_renames.py --apply >> "$LOG" 2>&1 || \
  echo "[$(stamp)] sync_sku_renames FAILED (continuing)" >> "$LOG"

echo "[$(stamp)] sync_supplier_names" >> "$LOG"
python sync_supplier_names.py --apply >> "$LOG" 2>&1 || \
  echo "[$(stamp)] sync_supplier_names FAILED (continuing)" >> "$LOG"

# Phase 3: auto-finalize submitted POs whose CIN7 status has flipped
# DRAFT → ORDERED / RECEIVING / etc. Reads the just-pulled purchase
# headers, transitions local po_drafts.status accordingly. Audited.
echo "[$(stamp)] auto_finalize_pos" >> "$LOG"
python auto_finalize_pos.py --apply >> "$LOG" 2>&1 || \
  echo "[$(stamp)] auto_finalize_pos FAILED (continuing)" >> "$LOG"

# Shopify content sync — feeds the AI Assistant's knowledge base
# with product descriptions, collections, pages, blog posts. Skipped
# automatically if SHOPIFY_DOMAIN / SHOPIFY_ACCESS_TOKEN aren't set.
if [ -n "${SHOPIFY_DOMAIN:-}" ] && [ -n "${SHOPIFY_ACCESS_TOKEN:-}" ]; then
    echo "[$(stamp)] shopify_sync" >> "$LOG"
    python shopify_sync.py >> "$LOG" 2>&1 || \
      echo "[$(stamp)] shopify_sync FAILED (continuing)" >> "$LOG"
else
    echo "[$(stamp)] shopify_sync skipped (env vars not set)" >> "$LOG"
fi

echo "[$(stamp)] daily_sync.sh done" >> "$LOG"
echo "" >> "$LOG"
