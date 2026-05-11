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

# v2.67.111 — helper: launch a sync command in BACKGROUND with a
# PID-file lock so we don't double-run while a previous instance
# is still going. Each call returns immediately; the work happens
# in a backgrounded subshell. This keeps slack_listener.once
# reachable every loop iteration regardless of how long the
# underlying sync takes.
#
# Usage: _run_bg <name> <cmd...>
#   name: short identifier used for /tmp/<name>.pid lock file
#   cmd:  the command + args to run (quoted as one arg, eval'd)
_run_bg() {
    local name="$1"
    local cmd="$2"
    local pidfile="/tmp/${name}.pid"
    if [ -e "$pidfile" ] \
            && kill -0 "$(cat "$pidfile")" 2>/dev/null; then
        echo "[$(stamp)] [$name] still running (pid=$(cat "$pidfile")); skipping" >> "$LOG"
        return
    fi
    (
        echo "[$(stamp)] [bg-$name] start" >> "$LOG"
        eval "$cmd" >> "$LOG" 2>&1 || true
        echo "[$(stamp)] [bg-$name] done" >> "$LOG"
        rm -f "$pidfile"
    ) &
    echo $! > "$pidfile"
}

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
# v2.67.93 — marketing-data syncs:
#   klaviyo:    daily (campaigns from last 7 days + per-SKU clicks)
#   reviewsio:  daily (reviews modified in last 30 days)
#   semrush:    weekly (top 500 keyword positions, ~5k units)
last_klaviyo_epoch=0
last_reviewsio_epoch=0
last_semrush_epoch=0
# v2.67.97 — Google Ads + GA4 syncs (Phase 2 of Moby replacement):
#   google_ads:   daily (last 7 days of campaign daily metrics)
#   ga4:          daily (last 7 days, both campaign-totals + per-SKU)
# Both gated on Google OAuth env vars; silent skip if not provisioned.
last_googleads_epoch=0
last_ga4_epoch=0

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
    # v2.67.113 — backgrounded. The Anthropic call to summarise
    # feedback can take 10-30 sec which was blocking listener.
    seconds_since_lessons=$(( now_epoch - last_lessons_epoch ))
    if [ "$seconds_since_lessons" -ge 86400 ]; then
        last_lessons_epoch=$(date -u +%s)
        _run_bg "bot_self_improvement" \
            "python bot_self_improvement.py daily --days 7"
    fi

    # v2.67.110 — daily refresh chain runs in BACKGROUND so it
    # never blocks slack_listener.once. cin7_sync salelines takes
    # ~80 min due to CIN7's 2.5s rate limit on 1800+ sale-detail
    # calls. Pre-v2.67.110 this blocked the loop for the entire
    # duration, causing the bot to go silent for hours.
    #
    # New shape:
    #   - last_dim_refresh_epoch is set IMMEDIATELY so subsequent
    #     loop iterations skip the block until tomorrow.
    #   - The entire 30d refresh chain runs as a backgrounded
    #     subshell — the main loop continues to slack_listener
    #     within milliseconds.
    #   - A PID file at /tmp/dim_refresh.pid prevents double-runs
    #     in the unlikely case the timing check misfires.
    seconds_since_dim_refresh=$(( now_epoch - last_dim_refresh_epoch ))
    DIM_REFRESH_PID_FILE=/tmp/dim_refresh.pid
    if [ "$seconds_since_dim_refresh" -ge 86400 ]; then
        # Skip if a previous backgrounded refresh is still running
        if [ -e "$DIM_REFRESH_PID_FILE" ] \
                && kill -0 "$(cat "$DIM_REFRESH_PID_FILE")" \
                                2>/dev/null; then
            echo "[$(stamp)] daily refresh still running (pid=$(cat "$DIM_REFRESH_PID_FILE")); skipping" >> "$LOG"
        else
            echo "[$(stamp)] launching daily 30d refresh chain in BACKGROUND" >> "$LOG"
            last_dim_refresh_epoch=$(date -u +%s)
            (
                if [ -n "${CIN7_ACCOUNT_ID:-}" ]; then
                    echo "[$(stamp)] [bg] cin7 salelines 30d" >> "$LOG"
                    python cin7_sync.py salelines --days 30 \
                        >> "$LOG" 2>&1 || true
                    echo "[$(stamp)] [bg] cin7 sales 30d" >> "$LOG"
                    python cin7_sync.py sales --days 30 \
                        >> "$LOG" 2>&1 || true
                    echo "[$(stamp)] [bg] cin7 purchaselines 30d" >> "$LOG"
                    python cin7_sync.py purchaselines --days 30 \
                        >> "$LOG" 2>&1 || true
                fi
                if [ -n "${SHIPSTATION_API_KEY:-}" ]; then
                    echo "[$(stamp)] [bg] shipstation 30d" >> "$LOG"
                    python shipstation_sync.py recent --days 30 \
                        >> "$LOG" 2>&1 || true
                fi
                if [ -n "${SHOPIFY_DOMAIN:-}" ] \
                        && [ -n "${SHOPIFY_ACCESS_TOKEN:-}" ]; then
                    echo "[$(stamp)] [bg] dim refresh-classifications" >> "$LOG"
                    python extract_dimensions.py \
                        refresh-classifications \
                        >> "$LOG" 2>&1 || true
                fi
                rm -f "$DIM_REFRESH_PID_FILE"
                echo "[$(stamp)] [bg] daily refresh chain DONE" >> "$LOG"
            ) &
            echo $! > "$DIM_REFRESH_PID_FILE"
        fi
    fi

    # v2.67.80 — weekly new-product vision extraction.
    # v2.67.113 — backgrounded via _run_bg. Was missed by
    # v2.67.111 refactor and continued to block the main loop
    # for 5-10 min during its first run after worker restart,
    # delaying slack_listener.once.
    seconds_since_dim_weekly=$(( now_epoch - last_dim_weekly_epoch ))
    if [ "$seconds_since_dim_weekly" -ge 604800 ]; then
        if [ -n "${SHOPIFY_DOMAIN:-}" ] \
                && [ -n "${SHOPIFY_ACCESS_TOKEN:-}" ] \
                && [ -n "${ANTHROPIC_API_KEY:-}" ]; then
            last_dim_weekly_epoch=$(date -u +%s)
            _run_bg "dim_weekly" \
                "python extract_dimensions.py weekly-new-products"
        fi
    fi

    # v2.67.81 — housekeeping freshness audit, daily.
    # Catches silent staleness in any data feed the bot depends on.
    # Always exits 0 — informational only. Reuses last_lessons_epoch's
    # 24h cadence indirectly by gating on the dim_refresh window so
    # we always run audit RIGHT AFTER the daily refresh chain.
    if [ "$seconds_since_dim_refresh" -ge 86400 ] \
            && [ -e housekeeping_audit.py ]; then
        echo "[$(stamp)] housekeeping_audit" >> "$LOG"
        python housekeeping_audit.py --verbose \
            --log "${DATA_DIR}/output/housekeeping.log" >> "$LOG" 2>&1 || \
            echo "[$(stamp)] housekeeping_audit FAILED" >> "$LOG"
    fi

    # v2.67.111 — all daily/weekly cycles now run in BACKGROUND
    # via _run_bg helper. Each cycle's epoch is set IMMEDIATELY
    # (parent shell update), so subsequent loop iterations skip
    # the cycle for 24h regardless of how long the background
    # work takes. PID file under /tmp prevents double-runs.
    # Slack listener is reached on every 60s tick regardless of
    # sync activity.
    seconds_since_klaviyo=$(( now_epoch - last_klaviyo_epoch ))
    if [ "$seconds_since_klaviyo" -ge 86400 ] \
            && [ -n "${KLAVIYO_API_KEY:-}" ]; then
        last_klaviyo_epoch=$(date -u +%s)
        _run_bg "klaviyo_sync" \
            "python klaviyo_sync.py recent --days 7"
    fi

    seconds_since_reviewsio=$(( now_epoch - last_reviewsio_epoch ))
    if [ "$seconds_since_reviewsio" -ge 86400 ] \
            && [ -n "${REVIEWSIO_API_KEY:-}" ] \
            && [ -n "${REVIEWSIO_STORE_ID:-}" ]; then
        last_reviewsio_epoch=$(date -u +%s)
        _run_bg "reviewsio_sync" \
            "python reviewsio_sync.py recent --days 30"
    fi

    seconds_since_semrush=$(( now_epoch - last_semrush_epoch ))
    if [ "$seconds_since_semrush" -ge 604800 ] \
            && [ -n "${SEMRUSH_API_KEY:-}" ]; then
        last_semrush_epoch=$(date -u +%s)
        _run_bg "semrush_sync" \
            "python semrush_sync.py weekly --limit 500"
    fi

    seconds_since_googleads=$(( now_epoch - last_googleads_epoch ))
    if [ "$seconds_since_googleads" -ge 86400 ] \
            && [ -n "${GOOGLE_ADS_DEVELOPER_TOKEN:-}" ] \
            && [ -n "${GOOGLE_ADS_CLIENT_ID:-}" ] \
            && [ -n "${GOOGLE_ADS_CLIENT_SECRET:-}" ] \
            && [ -n "${GOOGLE_ADS_REFRESH_TOKEN:-}" ] \
            && [ -n "${GOOGLE_ADS_CUSTOMER_ID:-}" ]; then
        last_googleads_epoch=$(date -u +%s)
        # Both Google Ads syncs in one backgrounded subshell so
        # they run sequentially (sharing OAuth refresh) but the
        # main loop continues immediately.
        _run_bg "google_ads_sync" \
            "python google_ads_sync.py recent --days 7 && python google_ads_sync.py per-sku --days 7"
    fi

    seconds_since_ga4=$(( now_epoch - last_ga4_epoch ))
    if [ "$seconds_since_ga4" -ge 86400 ] \
            && [ -n "${GA4_PROPERTY_ID:-}" ] \
            && [ -n "${GOOGLE_ADS_CLIENT_ID:-}" ] \
            && [ -n "${GOOGLE_ADS_CLIENT_SECRET:-}" ] \
            && [ -n "${GOOGLE_ADS_REFRESH_TOKEN:-}" ]; then
        last_ga4_epoch=$(date -u +%s)
        _run_bg "ga4_sync" \
            "python ga4_sync.py recent --days 7"
    fi

    # Slack ingest → DB
    python slack_sync.py poll >> "$LOG" 2>&1 || \
        echo "[$(stamp)] slack_sync.poll failed (continuing)" >> "$LOG"

    # Listener: classify + respond to unprocessed
    python slack_listener.py once >> "$LOG" 2>&1 || \
        echo "[$(stamp)] slack_listener.once failed (continuing)" >> "$LOG"

    sleep "$INTERVAL"
done
