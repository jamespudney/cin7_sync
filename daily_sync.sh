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
# Failure policy: individual sync steps keep going so one failure does
# not block the rest of the chain. At the end, the script exits non-zero
# if a critical engine feed is still missing/stale, because warming ABC
# from partial sales or assembly data creates bad buying signals.

set -uo pipefail

DATA_DIR="${DATA_DIR:-/data}"
LOG="${DATA_DIR}/output/daily_sync.log"
mkdir -p "${DATA_DIR}/output"
CRITICAL_SYNC_FAILURE=0
DAILY_SYNC_CRITICAL_MAX_AGE_HOURS="${DAILY_SYNC_CRITICAL_MAX_AGE_HOURS:-30}"

stamp() { date -u +"%Y-%m-%dT%H:%M:%SZ"; }

verify_critical_csv() {
    local pattern="$1"
    local label="$2"
    local freshest
    freshest=$(ls -t "${DATA_DIR}"/output/${pattern} \
        2>/dev/null | head -1)
    if [ -z "$freshest" ]; then
        echo "[$(stamp)] CRITICAL missing ${label}; ABC cache must not be warmed" \
            >> "$LOG"
        CRITICAL_SYNC_FAILURE=1
        return
    fi

    local age_s
    local age_h
    age_s=$(( $(date -u +%s) \
        - $(date -u -r "$freshest" +%s 2>/dev/null || echo 0) ))
    age_h=$(( age_s / 3600 ))
    if [ "$age_h" -ge "$DAILY_SYNC_CRITICAL_MAX_AGE_HOURS" ]; then
        echo "[$(stamp)] CRITICAL stale ${label}: ${age_h}h old (${freshest})" \
            >> "$LOG"
        CRITICAL_SYNC_FAILURE=1
    fi
}

echo "" >> "$LOG"
echo "============================================================" >> "$LOG"
echo "  daily_sync.sh start at $(stamp)" >> "$LOG"
echo "============================================================" >> "$LOG"

echo "[$(stamp)] cin7_sync quick --days 3" >> "$LOG"
CIN7_QUICK_SKIP_ASSEMBLIES=1 python cin7_sync.py quick --days 3 \
    >> "$LOG" 2>&1 || \
  echo "[$(stamp)] cin7_sync quick FAILED (continuing)" >> "$LOG"

# Product thumbnails are attachment metadata, not buying-critical data.
# Refresh weekly during the overnight job so buyer pages get images without
# making live CIN7 calls during the workday. Set PRODUCT_IMAGE_SYNC_FORCE=1
# on Render to force a one-off refresh after deploy.
if [ "$(date -u +%u)" = "7" ] || [ "${PRODUCT_IMAGE_SYNC_FORCE:-0}" = "1" ]; then
    echo "[$(stamp)] cin7_sync product-images" >> "$LOG"
    python cin7_sync.py product-images >> "$LOG" 2>&1 || \
      echo "[$(stamp)] cin7_sync product-images FAILED (continuing)" >> "$LOG"
else
    echo "[$(stamp)] cin7_sync product-images skipped (weekly Sunday refresh)" \
      >> "$LOG"
fi

# v2.67.264 — BOM / parent-child structure. Previously refreshed
# only by the weekend deep sync, leaving the engine's bulk-to-cut
# rollup on week-stale (or absent) BOM data. boms is a per-product
# detail loop; placed after `quick` so it reuses the fresh
# products_*.json, and it has its own checkpoint so an interrupted
# run resumes on the next day's pass.
echo "[$(stamp)] cin7_sync boms" >> "$LOG"
python cin7_sync.py boms >> "$LOG" 2>&1 || \
  echo "[$(stamp)] cin7_sync boms FAILED (continuing)" >> "$LOG"

# v2.67.43 — refresh the 30-day sale-header window daily. The
# Overview tile "Sales invoiced (last 30d)" reads sales_last_30d_*
# directly. Without this refresh the file goes stale (we observed
# a 13-day-stale file producing $323K vs CIN7's $520K — a $200K
# gap that matters for sales-team commission visibility).
echo "[$(stamp)] cin7_sync sales --days 30" >> "$LOG"
python cin7_sync.py sales --days 30 >> "$LOG" 2>&1 || \
  echo "[$(stamp)] cin7_sync sales --days 30 FAILED (continuing)" >> "$LOG"

# Purchase headers feed open-PO visibility and auto-finalization. `quick`
# only pulls the 3-day window, but the dashboard expects the 30-day
# snapshot too.
echo "[$(stamp)] cin7_sync purchases --days 30" >> "$LOG"
python cin7_sync.py purchases --days 30 >> "$LOG" 2>&1 || \
  echo "[$(stamp)] cin7_sync purchases --days 30 FAILED (continuing)" >> "$LOG"

# Sale lines feed the ABC engine, customer rollups, velocity. The
# `quick` sync above pulls sale headers but NOT line items. Without
# this incremental pull, line-level data goes stale by ~1 day per day.
# v2.67.43 — bumped the salelines window from 3 to 30 days too so
# the line-level rollups behind the Monthly Metrics report stay
# current within the same window the headline tile reports.
echo "[$(stamp)] cin7_sync salelines --days 30" >> "$LOG"
python cin7_sync.py salelines --days 30 >> "$LOG" 2>&1 || \
  echo "[$(stamp)] cin7_sync salelines FAILED (continuing)" >> "$LOG"

# Finished-goods assemblies are ground-truth demand for components that
# are consumed into kits rather than sold directly. The quick sync above
# only pulls a 3-day assembly window, which is not enough for
# month-to-date demand on assembly-heavy SKUs such as
# LED-NEON-FLEX-NICHO-3000K-2.
echo "[$(stamp)] cin7_sync assemblies --days 30" >> "$LOG"
python cin7_sync.py assemblies --days 30 >> "$LOG" 2>&1 || \
  echo "[$(stamp)] cin7_sync assemblies FAILED (continuing)" >> "$LOG"

# Purchase lines feed FixedCost audit, supplier-pricing audits, AND
# the AI Assistant's get_incoming_stock + get_purchase_order tools.
# v2.67.51 — bumped from 7 to 30 days. With a 7-day window, any PO
# created more than a week ago that wasn't received yet would be
# invisible to the AI (reported as "no open PO matches") because the
# widest-window file we hold became stale between manual full syncs.
# Lead times from EU / Asia suppliers run 4-8 weeks, so 30d is the
# right floor. Same parallel as the sales-window bump in v2.67.43.
echo "[$(stamp)] cin7_sync purchaselines --days 30" >> "$LOG"
python cin7_sync.py purchaselines --days 30 >> "$LOG" 2>&1 || \
  echo "[$(stamp)] cin7_sync purchaselines FAILED (continuing)" >> "$LOG"

# Product Detail and AI stock-audit answers use the 30-day movement
# window. Nearsync keeps 1-day movement files hot; this keeps the wider
# movement context fresh once per day.
echo "[$(stamp)] cin7_sync stockadjustments --days 30" >> "$LOG"
python cin7_sync.py stockadjustments --days 30 >> "$LOG" 2>&1 || \
  echo "[$(stamp)] cin7_sync stockadjustments FAILED (continuing)" >> "$LOG"

echo "[$(stamp)] cin7_sync stocktransfers --days 30" >> "$LOG"
python cin7_sync.py stocktransfers --days 30 >> "$LOG" 2>&1 || \
  echo "[$(stamp)] cin7_sync stocktransfers FAILED (continuing)" >> "$LOG"

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
    # v2.67.55 — Shopify ORDER pull for conversion attribution.
    # Distinct from the content sync above (products / collections /
    # pages); writes to OUTPUT_DIR/shopify_orders_last_7d_*.csv. The
    # AI's get_shopify_order tool reads the merged file.
    echo "[$(stamp)] shopify_sync --orders-recent 7" >> "$LOG"
    python shopify_sync.py --orders-recent 7 >> "$LOG" 2>&1 || \
      echo "[$(stamp)] shopify_orders_recent FAILED (continuing)" >> "$LOG"
else
    echo "[$(stamp)] shopify_sync skipped (env vars not set)" >> "$LOG"
fi

# Inventory Planner buyer notes and alternates are curated team knowledge
# used by PO commentary and migration/compatibility workflows.
if [ -n "${IP_API_KEY:-}" ] && [ -n "${IP_ACCOUNT:-}" ]; then
    echo "[$(stamp)] ip_pull_alternates" >> "$LOG"
    python ip_pull_alternates.py >> "$LOG" 2>&1 || \
      echo "[$(stamp)] ip_pull_alternates FAILED (continuing)" >> "$LOG"
else
    echo "[$(stamp)] ip_pull_alternates skipped (IP env vars unset)" \
      >> "$LOG"
fi

# v2.67.54 — ShipStation sync. Recent catch-up keeps the rolling
# shipments_last_30d_*.csv up to date so the AI's
# get_shipping_details tool sees yesterday's labels and the Monthly
# Metrics shipping-cost row stays current. Note: first-time
# backfill (5y of history) needs to be run manually:
#   python shipstation_sync.py full --days 1825
#
# v2.67.81 — gate fixed. ShipStation v2 needs only API_KEY (no
# secret); v1 needs both. shipstation_sync.py auto-detects which
# version to use based on which credentials are set. Old gate
# required BOTH keys, silently skipping v2-only setups and leaving
# Monthly Metrics' Shipping Cost row empty for weeks. New gate:
# if API_KEY is set, run; the script handles version detection.
#
# v2.67.81 — bumped window from 7d to 30d so Monthly Metrics has
# the full current month visible without waiting for a manual
# backfill. Cost: trivial (per-shipment GET is cheap).
if [ -n "${SHIPSTATION_API_KEY:-}" ]; then
    echo "[$(stamp)] shipstation_sync recent --days 30" >> "$LOG"
    python shipstation_sync.py recent --days 30 >> "$LOG" 2>&1 || \
      echo "[$(stamp)] shipstation_sync FAILED (continuing)" >> "$LOG"
else
    echo "[$(stamp)] shipstation_sync skipped (SHIPSTATION_API_KEY unset)" \
      >> "$LOG"
fi

# v2.67.81 — housekeeping freshness audit. Catches silent staleness
# in any data feed (CSVs, DB tables) the app depends on. Always
# exits 0 — informational only. Output captured into the daily log
# AND a dedicated housekeeping log for quick scanning.
echo "[$(stamp)] housekeeping_audit" >> "$LOG"
python housekeeping_audit.py --verbose \
  --log "${DATA_DIR}/output/housekeeping.log" >> "$LOG" 2>&1 || \
  echo "[$(stamp)] housekeeping_audit FAILED (continuing)" >> "$LOG"

verify_critical_csv "sales_last_30d_*.csv" "sales_last_30d CSV"
verify_critical_csv "sale_lines_last_30d_*.csv" "sale_lines_last_30d CSV"
verify_critical_csv "assemblies_last_30d_*.csv" "assemblies_last_30d CSV"

if [ "$CRITICAL_SYNC_FAILURE" = "1" ]; then
    echo "[$(stamp)] daily_sync.sh finished with critical feed failures" >> "$LOG"
    echo "" >> "$LOG"
    exit 1
fi

echo "[$(stamp)] daily_sync.sh done" >> "$LOG"
echo "" >> "$LOG"
