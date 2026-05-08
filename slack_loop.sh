#!/usr/bin/env bash
# slack_loop.sh — Slack ingest + listener loop (v2.67.57, expanded
# in v2.67.58 with worker self-sufficiency).
#
# Designed to run as a Render Background Worker. The worker has its
# OWN persistent disk (Render disks are exclusive to one service).
# That means it needs its own copy of the CIN7/ShipStation/Shopify
# data — which this script bootstraps on first boot and keeps fresh
# via in-loop NearSync calls.
#
# Lifecycle:
#   1. First boot: if /data is empty, run a 30-day data bootstrap
#      (~30-60 min) so the listener has CSVs to read.
#   2. Steady state: loop forever, alternating between:
#        (a) Slack poll → ingest new messages → DB
#        (b) Slack listener → classify + respond
#        (c) Data refresh (NearSync style) every WORKER_DATA_SYNC_MINUTES
#
# Why a single combined loop rather than separate workers: simpler
# memory profile (only one Python process active at a time) and the
# user's Render plan has finite memory headroom.
#
# Required env vars:
#   SLACK_BOT_TOKEN       (bot polling + posting)
#   SLACK_AI_CHANNELS     (channel allowlist)
#   SLACK_AUDIT_CHANNEL   (#ai-audit destination)
#   ANTHROPIC_API_KEY     (response composition)
#
# Recommended env vars (for self-sufficient data):
#   CIN7_ACCOUNT_ID       (CIN7 product/sale/PO data)
#   CIN7_APPLICATION_KEY
#   SHIPSTATION_API_KEY   (shipment lookups)
#   SHOPIFY_DOMAIN        (conversion-attribution lookups)
#   SHOPIFY_ACCESS_TOKEN
#
# Optional env vars:
#   SLACK_LOOP_INTERVAL          poll cadence in seconds (default 60)
#   WORKER_DATA_SYNC_MINUTES     data refresh cadence (default 30)
#   DATA_DIR                     persistent disk root (default /data)

set -uo pipefail

DATA_DIR="${DATA_DIR:-/data}"
LOG="${DATA_DIR}/output/slack_loop.log"
mkdir -p "${DATA_DIR}/output"

INTERVAL="${SLACK_LOOP_INTERVAL:-60}"
DATA_SYNC_INTERVAL_MIN="${WORKER_DATA_SYNC_MINUTES:-30}"

stamp() { date -u +"%Y-%m-%dT%H:%M:%SZ"; }

echo "" >> "$LOG"
echo "============================================================" >> "$LOG"
echo "[$(stamp)] slack_loop starting" >> "$LOG"
echo "  poll interval         = ${INTERVAL}s" >> "$LOG"
echo "  data sync interval    = ${DATA_SYNC_INTERVAL_MIN}min" >> "$LOG"
echo "============================================================" >> "$LOG"

if [ -z "${SLACK_BOT_TOKEN:-}" ]; then
    echo "[$(stamp)] SLACK_BOT_TOKEN not set — exiting cleanly" >> "$LOG"
    exit 0
fi

# ----------------------------------------------------------------------
# v2.67.58 — Bootstrap: first-boot data sync
# ----------------------------------------------------------------------
# The worker's /data is empty on first deploy. The slack_listener
# relies on CSVs (products, stock, sales, purchases, shipments,
# shopify_orders) for its tool chain. If they're missing, the
# composer gracefully says "data not loadable" — bot stays silent
# on data-heavy questions until bootstrap finishes.
#
# Bootstrap window: 30 days of data. Captures recent transactions
# without the full 5-year backfill the web service's manual pull
# does. Sufficient for "where's INV-XXX" / "what's on PO-YYY" /
# "do we have warm white in stock" questions.
needs_bootstrap=0
if ! ls "${DATA_DIR}"/output/products_*.csv >/dev/null 2>&1; then
    needs_bootstrap=1
fi
if ! ls "${DATA_DIR}"/output/stock_on_hand_*.csv >/dev/null 2>&1; then
    needs_bootstrap=1
fi

if [ "$needs_bootstrap" = "1" ]; then
    echo "[$(stamp)] === FIRST-BOOT BOOTSTRAP (30-day data sync) ===" >> "$LOG"
    echo "[$(stamp)] This takes ~20-40 min. Bot will be silent on" >> "$LOG"
    echo "[$(stamp)] data-heavy questions until this completes." >> "$LOG"

    if [ -n "${CIN7_ACCOUNT_ID:-}" ] && [ -n "${CIN7_APPLICATION_KEY:-}" ]; then
        echo "[$(stamp)] cin7_sync quick --days 30" >> "$LOG"
        python cin7_sync.py quick --days 30 >> "$LOG" 2>&1 || \
            echo "[$(stamp)] cin7_sync.quick FAILED (continuing)" >> "$LOG"

        echo "[$(stamp)] cin7_sync salelines --days 30" >> "$LOG"
        python cin7_sync.py salelines --days 30 >> "$LOG" 2>&1 || \
            echo "[$(stamp)] cin7_sync.salelines FAILED" >> "$LOG"

        echo "[$(stamp)] cin7_sync purchaselines --days 30" >> "$LOG"
        python cin7_sync.py purchaselines --days 30 >> "$LOG" 2>&1 || \
            echo "[$(stamp)] cin7_sync.purchaselines FAILED" >> "$LOG"
    else
        echo "[$(stamp)] CIN7 env vars not set — skipping CIN7 bootstrap" >> "$LOG"
    fi

    if [ -n "${SHIPSTATION_API_KEY:-}" ]; then
        echo "[$(stamp)] shipstation_sync recent --days 30" >> "$LOG"
        python shipstation_sync.py recent --days 30 >> "$LOG" 2>&1 || \
            echo "[$(stamp)] shipstation_sync FAILED" >> "$LOG"
    fi

    if [ -n "${SHOPIFY_DOMAIN:-}" ] && [ -n "${SHOPIFY_ACCESS_TOKEN:-}" ]; then
        echo "[$(stamp)] shopify_sync --orders-recent 30" >> "$LOG"
        python shopify_sync.py --orders-recent 30 >> "$LOG" 2>&1 || \
            echo "[$(stamp)] shopify_sync FAILED" >> "$LOG"
    fi

    echo "[$(stamp)] === BOOTSTRAP COMPLETE — entering main loop ===" >> "$LOG"
fi

# ----------------------------------------------------------------------
# Main loop
# ----------------------------------------------------------------------
last_data_sync_epoch=$(date -u +%s)
# v2.67.66 — track when we last ran the lessons-learned summarizer.
# Runs at most once per day. Initial value 0 forces a run on first
# pass through the loop after boot (so a freshly-booted worker
# generates a summary if one doesn't exist for today).
last_lessons_epoch=0
# v2.67.80 — dimension-data maintenance cadence:
#   refresh-classifications: daily (no API spend)
#     re-pulls collections + metafields + applies title rules so
#     bot stays in sync if a buyer reorganises Shopify collections
#     or adds metafields
#   weekly-new-products: every 7 days
#     extracts vision dims for any new SKUs added since last run
last_dim_refresh_epoch=0
last_dim_weekly_epoch=0

while true; do
    now_epoch=$(date -u +%s)
    minutes_since_sync=$(( (now_epoch - last_data_sync_epoch) / 60 ))

    # Periodic data refresh (NearSync-style — last 1 day)
    if [ "$minutes_since_sync" -ge "$DATA_SYNC_INTERVAL_MIN" ]; then
        echo "[$(stamp)] data refresh (${minutes_since_sync}min since last)" >> "$LOG"
        if [ -n "${CIN7_ACCOUNT_ID:-}" ]; then
            python cin7_sync.py nearsync --days 1 >> "$LOG" 2>&1 || \
                echo "[$(stamp)] nearsync FAILED" >> "$LOG"
        fi
        if [ -n "${SHIPSTATION_API_KEY:-}" ]; then
            python shipstation_sync.py recent --days 1 >> "$LOG" 2>&1 || \
                echo "[$(stamp)] shipstation 1d FAILED" >> "$LOG"
        fi
        if [ -n "${SHOPIFY_DOMAIN:-}" ]; then
            python shopify_sync.py --orders-recent 1 >> "$LOG" 2>&1 || \
                echo "[$(stamp)] shopify 1d FAILED" >> "$LOG"
        fi
        last_data_sync_epoch=$(date -u +%s)
    fi

    # v2.67.66 — daily lessons-learned summarizer.
    # Once per ~24h, digest recent feedback into a 'lessons learned'
    # markdown that the listener prepends to the system prompt. Self-
    # healing: if the worker reboots, this runs again on first loop
    # pass so the summary is always fresh.
    seconds_since_lessons=$(( now_epoch - last_lessons_epoch ))
    if [ "$seconds_since_lessons" -ge 86400 ]; then
        echo "[$(stamp)] running bot_self_improvement summarizer" >> "$LOG"
        python bot_self_improvement.py daily --days 7 >> "$LOG" 2>&1 || \
            echo "[$(stamp)] summarizer FAILED" >> "$LOG"
        last_lessons_epoch=$(date -u +%s)
    fi

    # v2.67.80 — daily dimension-classifications refresh.
    # Catches collection / metafield / title changes since the last
    # extraction so bot answers stay in sync with Shopify reality.
    # No vision API spend.
    seconds_since_dim_refresh=$(( now_epoch - last_dim_refresh_epoch ))
    if [ "$seconds_since_dim_refresh" -ge 86400 ]; then
        if [ -n "${SHOPIFY_DOMAIN:-}" ] && [ -n "${SHOPIFY_ACCESS_TOKEN:-}" ]; then
            echo "[$(stamp)] refreshing product_dimensions classifications" >> "$LOG"
            python extract_dimensions.py refresh-classifications >> "$LOG" 2>&1 || \
                echo "[$(stamp)] dim refresh FAILED" >> "$LOG"
            last_dim_refresh_epoch=$(date -u +%s)
        fi
    fi

    # v2.67.80 — weekly new-product vision extraction.
    # Picks up any SKUs added in Shopify since the last run.
    seconds_since_dim_weekly=$(( now_epoch - last_dim_weekly_epoch ))
    if [ "$seconds_since_dim_weekly" -ge 604800 ]; then
        if [ -n "${SHOPIFY_DOMAIN:-}" ] \
                && [ -n "${SHOPIFY_ACCESS_TOKEN:-}" ] \
                && [ -n "${ANTHROPIC_API_KEY:-}" ]; then
            echo "[$(stamp)] weekly: extracting NEW products" >> "$LOG"
            python extract_dimensions.py weekly-new-products >> "$LOG" 2>&1 || \
                echo "[$(stamp)] weekly extract FAILED" >> "$LOG"
            last_dim_weekly_epoch=$(date -u +%s)
        fi
    fi

    # Slack ingest → DB
    python slack_sync.py poll >> "$LOG" 2>&1 || \
        echo "[$(stamp)] slack_sync.poll failed (continuing)" >> "$LOG"

    # Listener: classify + respond to unprocessed
    python slack_listener.py once >> "$LOG" 2>&1 || \
        echo "[$(stamp)] slack_listener.once failed (continuing)" >> "$LOG"

    sleep "$INTERVAL"
done
