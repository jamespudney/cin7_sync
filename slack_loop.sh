#!/usr/bin/env bash
# slack_loop.sh — Slack ingest + listener loop (v2.67.57, expanded
# in v2.67.58 with worker self-sufficiency).
#
# Designed to run as a Render Background Worker. The worker has its
# OWN persistent disk (Render disks are exclusive to one service).
# Since 2026-09-03 the data on it is a MIRROR of the dashboard's
# (dataset_mirror.py pull from the shared Postgres), not a second
# CIN7/ShipStation/Shopify sync — see WORKER_DATA_FROM_DB below.
# The legacy own-sync path remains as a fallback.
#
# Lifecycle:
#   1. First boot: if /data is empty, run a data bootstrap
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
#   name: short identifier used for the PID lock file
#   cmd:  the command + args to run (quoted as one arg, eval'd)
#
# 2026-09-03 — admission control. Every `last_*_epoch` counter starts at 0,
# so on boot ~27 of these fire in the same loop iteration, each starting its
# own Python process that imports pandas before doing any work. On the 2 GB
# worker that reliably exceeded the memory limit; the OOM killed the
# container mid-run, it restarted, all 27 fired again — a permanent crash
# loop (6 kills in 8 minutes on 2026-09-03 after the fablab stock-alert job
# became the 28th). The jobs themselves were fine; launching them all at
# once was not.
#
# So _run_bg no longer launches unconditionally. A job starts only if fewer
# than BG_MAX_JOBS are already running AND MemAvailable is above
# BG_MIN_AVAILABLE_MB. Otherwise it goes on a queue and _bg_drain (called
# once per loop iteration) starts it as soon as there is room. Nothing is
# dropped and no call site changes — the herd is simply spread out.
BG_PID_DIR="${BG_PID_DIR:-/tmp/slack_loop_bg}"
BG_QUEUE="${BG_PID_DIR}/queue"
BG_MAX_JOBS="${BG_MAX_JOBS:-2}"
BG_MIN_AVAILABLE_MB="${BG_MIN_AVAILABLE_MB:-500}"
mkdir -p "$BG_PID_DIR"
: > "$BG_QUEUE"

# Linux MemAvailable in MB. Prints a large number if unreadable, so a
# missing /proc never blocks work.
_bg_available_mb() {
    awk '/^MemAvailable:/ {print int($2/1024); found=1}
         END {if (!found) print 999999}' /proc/meminfo 2>/dev/null \
        || echo 999999
}

# Count live background jobs, cleaning up stale PID files as we go.
_bg_running_count() {
    local n=0 pf pid
    for pf in "$BG_PID_DIR"/*.pid; do
        [ -e "$pf" ] || continue
        pid="$(cat "$pf" 2>/dev/null || true)"
        if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
            n=$((n + 1))
        else
            rm -f "$pf"
        fi
    done
    echo "$n"
}

_bg_launch() {
    local name="$1" cmd="$2"
    local pidfile="${BG_PID_DIR}/${name}.pid"
    (
        echo "[$(stamp)] [bg-$name] start" >> "$LOG"
        eval "$cmd" >> "$LOG" 2>&1 || true
        echo "[$(stamp)] [bg-$name] done" >> "$LOG"
        rm -f "$pidfile"
    ) &
    echo $! > "$pidfile"
}

# Queue a job, unless the same name is already waiting.
_bg_enqueue() {
    local name="$1" cmd="$2"
    if grep -q "^${name}"$'\t' "$BG_QUEUE" 2>/dev/null; then
        return
    fi
    printf '%s\t%s\n' "$name" "$cmd" >> "$BG_QUEUE"
    echo "[$(stamp)] [bg-$name] queued (running=$(_bg_running_count)," \
         "avail=$(_bg_available_mb)MB)" >> "$LOG"
}

# Start queued jobs while there is capacity. Called once per loop iteration.
_bg_drain() {
    local name cmd rest
    while [ -s "$BG_QUEUE" ]; do
        if [ "$(_bg_running_count)" -ge "$BG_MAX_JOBS" ]; then
            return
        fi
        if [ "$(_bg_available_mb)" -lt "$BG_MIN_AVAILABLE_MB" ]; then
            return
        fi
        IFS=$'\t' read -r name cmd < "$BG_QUEUE"
        rest="$(tail -n +2 "$BG_QUEUE")"
        printf '%s' "$rest" > "$BG_QUEUE"
        [ -n "$rest" ] && printf '\n' >> "$BG_QUEUE"
        [ -n "${name:-}" ] || continue
        echo "[$(stamp)] [bg-$name] dequeued" >> "$LOG"
        _bg_launch "$name" "$cmd"
    done
}

_run_bg() {
    local name="$1"
    local cmd="$2"
    local pidfile="${BG_PID_DIR}/${name}.pid"
    if [ -e "$pidfile" ] \
            && kill -0 "$(cat "$pidfile" 2>/dev/null)" 2>/dev/null; then
        echo "[$(stamp)] [$name] still running (pid=$(cat "$pidfile")); skipping" >> "$LOG"
        return
    fi
    rm -f "$pidfile"
    if [ "$(_bg_running_count)" -ge "$BG_MAX_JOBS" ] \
            || [ "$(_bg_available_mb)" -lt "$BG_MIN_AVAILABLE_MB" ]; then
        _bg_enqueue "$name" "$cmd"
        return
    fi
    _bg_launch "$name" "$cmd"
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
# Bootstrap windows: 365 days of sale headers for older backorder SO
# lookup, 30 days of line-level transaction detail for worker memory/API
# cost. Older SO detail can then be fetched live from CIN7 once the SO
# header resolves to a SaleID.
# ----------------------------------------------------------------------
# 2026-09-03 — ONE copy of the raw data. The dashboard service is the
# only one that syncs CIN7 / ShipStation / Shopify; it publishes every
# CSV to Postgres (dataset_mirror.py publish) and this worker PULLS
# them onto its disk with identical names/mtimes. All the glob-based
# loaders below keep working, they just read the dashboard's bytes —
# so the bot and the app can't disagree on on-hand, demand or "last
# sold". James: "the bot should match whatever the app's data is."
#
# WORKER_DATA_FROM_DB=1 (default) — pull from DB; the worker's own
#   CIN7/ShipStation/Shopify-orders syncs are disabled.
# WORKER_DATA_FROM_DB=0 — legacy: worker syncs its own copies.
# If the DB has no datasets yet (dashboard not deployed with the
# publisher), the legacy syncs run so nothing goes dark.
# ----------------------------------------------------------------------
WORKER_DATA_FROM_DB="${WORKER_DATA_FROM_DB:-1}"
MIRROR_OK=0
# Re-checked once per loop iteration (one short python start), so the
# many _mirror_available calls below are free.
_mirror_check() {
    if [ "$WORKER_DATA_FROM_DB" = "1" ] \
            && python dataset_mirror.py status >/dev/null 2>&1; then
        MIRROR_OK=1
    else
        MIRROR_OK=0
    fi
}
_mirror_available() { [ "$MIRROR_OK" = "1" ]; }
_mirror_pull() {
    echo "[$(stamp)] dataset_mirror pull" >> "$LOG"
    python dataset_mirror.py pull >> "$LOG" 2>&1 || \
        echo "[$(stamp)] dataset_mirror pull FAILED" >> "$LOG"
}

needs_bootstrap=0
_mirror_check
if _mirror_available; then
    echo "[$(stamp)] data source = shared DB (dataset_mirror)" >> "$LOG"
    _mirror_pull
else
    echo "[$(stamp)] data source = worker's own syncs (WORKER_DATA_FROM_DB=${WORKER_DATA_FROM_DB}, mirror empty or disabled)" >> "$LOG"
fi
if ! ls "${DATA_DIR}"/output/products_*.csv >/dev/null 2>&1; then
    needs_bootstrap=1
fi
if ! ls "${DATA_DIR}"/output/stock_on_hand_*.csv >/dev/null 2>&1; then
    needs_bootstrap=1
fi

if [ "$needs_bootstrap" = "1" ] && _mirror_available; then
    echo "[$(stamp)] products/stock still missing after mirror pull — dashboard has not published yet; waiting for next pull rather than syncing CIN7 here" >> "$LOG"
    needs_bootstrap=0
fi
if [ "$needs_bootstrap" = "1" ]; then
    echo "[$(stamp)] === FIRST-BOOT BOOTSTRAP (30-day data sync) ===" >> "$LOG"
    echo "[$(stamp)] This takes ~20-40 min. Bot will be silent on" >> "$LOG"
    echo "[$(stamp)] data-heavy questions until this completes." >> "$LOG"

    if [ -n "${CIN7_ACCOUNT_ID:-}" ] && [ -n "${CIN7_APPLICATION_KEY:-}" ]; then
        echo "[$(stamp)] cin7_sync quick --days 30" >> "$LOG"
        python cin7_sync.py quick --days 30 >> "$LOG" 2>&1 || \
            echo "[$(stamp)] cin7_sync.quick FAILED (continuing)" >> "$LOG"

        echo "[$(stamp)] cin7_sync sales --days 365" >> "$LOG"
        python cin7_sync.py sales --days 365 >> "$LOG" 2>&1 || \
            echo "[$(stamp)] cin7_sync.sales365 FAILED" >> "$LOG"

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
#   merchant:     daily (v2.67.118: feed status + free-listing perf)
# All gated on Google OAuth env vars; silent skip if not provisioned.
last_googleads_epoch=0
last_ga4_epoch=0
last_merchant_epoch=0      # v2.67.118 Google Merchant Center
last_fablab_autotag_epoch=0  # 2026-09-01 865FabLab corner auto-tag
last_fablab_stock_alert_epoch=0       # 2026-09-01 865FabLab stock-drop alerts
last_fablab_alert_replies_epoch=0     # 2026-09-02 865FabLab Slack-reply approval poll
last_fablab_assembly_po_epoch=0        # 2026-09-04 865FabLab labor-PO authorised watch
last_fablab_assembly_replies_epoch=0   # 2026-09-04 865FabLab assembly `done` replies
last_po_dispatch_epoch=0   # v2.67.130 PO dispatch reminders
last_dropship_epoch=0      # v2.67.138 dropship backorder warnings
last_bis_arrivals_epoch=0  # v2.67.140 back-in-stock arrival reminders
last_si_escalate_epoch=0   # v2.67.144 stock-issue DM escalation
last_si_replies_epoch=0    # v2.67.245 stock-issue thread-reply poll
last_notion_pull_epoch=0   # v2.67.254 Notion playbook pull
last_notion_push_epoch=0   # v2.67.254 Notion slow-movers push
last_notion_dims_epoch=0   # v2.67.281 Notion product-dimensions pull
last_ip_lead_times_epoch=0 # v2.67.285 IP observed lead-times pull
last_qbo_pl_epoch=0        # v2.67.292 QBO Profit & Loss pull
last_shopify_disc_epoch=0  # v2.67.303 Shopify monthly discounts
last_si_morning_epoch=0    # v2.67.144 stock-issue morning summary
last_si_morning_date=""    # one-summary-per-day idempotency
last_ship_margin_epoch=0   # v2.67.152 shipping margin monitor
last_bom_sync_epoch=0      # v2.67.195 BOM sync (weekly)
last_shopify_sync_epoch=0  # v2.67.274 Shopify content sync fallback

while true; do
    now_epoch=$(date -u +%s)
    _mirror_check
    # Start anything that was queued because memory or the concurrency
    # limit was tight last time round.
    _bg_drain
    minutes_since_sync=$(( (now_epoch - last_data_sync_epoch) / 60 ))

    # Periodic data refresh (NearSync-style — last 1 day)
    if [ "$minutes_since_sync" -ge "$DATA_SYNC_INTERVAL_MIN" ]; then
        echo "[$(stamp)] data refresh (${minutes_since_sync}min since last)" >> "$LOG"
        if _mirror_available; then
            # 2026-09-03 — read the dashboard's copies, don't re-sync.
            _mirror_pull
        else
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
    # 2026-09-03 — PID file now lives in BG_PID_DIR so this chain counts
    # towards _bg_running_count like every other background job. It is the
    # heaviest thing the worker runs (products + 365d sales + line items)
    # and it also starts at boot, so it must not run alongside the rest of
    # the boot catch-up herd.
    DIM_REFRESH_PID_FILE="${BG_PID_DIR}/dim_refresh.pid"
    if [ "$seconds_since_dim_refresh" -ge 86400 ]; then
        # Skip if a previous backgrounded refresh is still running
        if [ -e "$DIM_REFRESH_PID_FILE" ] \
                && kill -0 "$(cat "$DIM_REFRESH_PID_FILE" 2>/dev/null)" \
                                2>/dev/null; then
            echo "[$(stamp)] daily refresh still running (pid=$(cat "$DIM_REFRESH_PID_FILE")); skipping" >> "$LOG"
        elif [ "$(_bg_running_count)" -ge "$BG_MAX_JOBS" ] \
                || [ "$(_bg_available_mb)" -lt "$BG_MIN_AVAILABLE_MB" ]; then
            # Deferred, NOT stamped: last_dim_refresh_epoch stays where it
            # is so the next loop iteration retries once there is room.
            echo "[$(stamp)] daily refresh deferred (running=$(_bg_running_count)," \
                 "avail=$(_bg_available_mb)MB)" >> "$LOG"
        else
            echo "[$(stamp)] launching daily worker data refresh chain in BACKGROUND" >> "$LOG"
            last_dim_refresh_epoch=$(date -u +%s)
            (
                if _mirror_available; then
                    echo "[$(stamp)] [bg] cin7/shipstation refresh skipped — data comes from shared DB" >> "$LOG"
                elif [ -n "${CIN7_ACCOUNT_ID:-}" ]; then
                    # v2.67.370 — refresh products here because NearSync
                    # deliberately skips master data. This keeps Slack's
                    # local products_*.csv aligned with dashboard fixes for
                    # CIN7 fields such as Storage L x W x H In.
                    echo "[$(stamp)] [bg] cin7 products" >> "$LOG"
                    python cin7_sync.py products \
                        >> "$LOG" 2>&1 || true
                    echo "[$(stamp)] [bg] cin7 salelines 30d" >> "$LOG"
                    python cin7_sync.py salelines --days 30 \
                        >> "$LOG" 2>&1 || true
                    echo "[$(stamp)] [bg] cin7 sales 365d" >> "$LOG"
                    python cin7_sync.py sales --days 365 \
                        >> "$LOG" 2>&1 || true
                    echo "[$(stamp)] [bg] cin7 purchaselines 30d" >> "$LOG"
                    python cin7_sync.py purchaselines --days 30 \
                        >> "$LOG" 2>&1 || true
                fi
                if ! _mirror_available && [ -n "${SHIPSTATION_API_KEY:-}" ]; then
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
    # v2.67.114 — bumped Klaviyo window from 7 to 90 days.
    # Klaviyo daily-sync only catches campaigns sent in the
    # window; 7 days was too narrow for stores that send
    # weekly. 90 days catches the most recent ~13 weekly
    # newsletters and keeps the table populated even after
    # quiet periods.
    seconds_since_klaviyo=$(( now_epoch - last_klaviyo_epoch ))
    if [ "$seconds_since_klaviyo" -ge 86400 ] \
            && [ -n "${KLAVIYO_API_KEY:-}" ]; then
        last_klaviyo_epoch=$(date -u +%s)
        _run_bg "klaviyo_sync" \
            "python klaviyo_sync.py recent --days 90"
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

    # v2.67.195 BOM sync — weekly. Powers the stock-locator
    # audit + the runtime parent-SKU fallback in stock-issue
    # replies. The BOM endpoint requires one CIN7 detail call
    # per BOM-flagged product (~1k+ products), so at the 2.5s
    # rate limit it takes ~1 hour. Backgrounded via _run_bg so
    # the listener stays responsive throughout. Runs once every
    # 7 days — BOMs change rarely.
    seconds_since_bom=$(( now_epoch - last_bom_sync_epoch ))
    if [ "$seconds_since_bom" -ge 604800 ] \
            && ! _mirror_available \
            && [ -n "${CIN7_ACCOUNT_ID:-}" ] \
            && [ -n "${CIN7_APPLICATION_KEY:-}" ]; then
        last_bom_sync_epoch=$(date -u +%s)
        _run_bg "cin7_boms" \
            "python cin7_sync.py boms"
    fi

    # v2.67.xxx — monthly wide sale_lines re-backfill. The worker's
    # own CIN7 salelines sync (bootstrap block above + the periodic
    # refresh block below) has only ever requested a rolling --days
    # 30 window — nowhere near enough for worker_engine.py's 12-month
    # effective_units_12mo/90d calc, which every ABC/dormancy/PO-
    # commentary answer depends on. Confirmed live 2026-07-29: the
    # worker's widest available sale_lines file was 30 days old,
    # silently undercounting demand for every SKU, not just the one
    # a buyer happened to flag. Backfills 730 days once a month —
    # same restart-safe marker-file pattern sync_loop.sh uses for its
    # monthly report, so a redeploy on the 1st doesn't repeat the
    # run. Backgrounded via _run_bg (same as the BOM sync above) —
    # this is a much bigger pull than the usual 30-day sync and
    # shouldn't block the listener while it runs.
    day_of_month="$(date -u +%d)"
    day_of_month="${day_of_month#0}"
    this_month="$(date -u +%Y-%m)"
    worker_salelines_backfill_marker="/data/.last_worker_salelines_backfill_month"
    last_worker_salelines_backfill_month=""
    if [ -f "$worker_salelines_backfill_marker" ]; then
        last_worker_salelines_backfill_month="$(cat "$worker_salelines_backfill_marker" 2>/dev/null)"
    fi
    if [ "$day_of_month" -eq 1 ] \
            && [ "$this_month" != "$last_worker_salelines_backfill_month" ] \
            && ! _mirror_available \
            && [ -n "${CIN7_ACCOUNT_ID:-}" ] \
            && [ -n "${CIN7_APPLICATION_KEY:-}" ]; then
        echo "$this_month" > "$worker_salelines_backfill_marker"
        echo "[$(stamp)] day $day_of_month of $this_month — worker sale_lines full re-backfill (730d)" >> "$LOG"
        _run_bg "worker_salelines_backfill" \
            "python cin7_sync.py salelines --days 730"
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

    # v2.67.118 Google Merchant Center — feed status (every SKU
    # disapproved/warning state) + free-listing performance
    # (organic Shopping clicks, complementing google_ads_sync's
    # paid spend). Reuses the same GOOGLE_ADS_* OAuth creds; the
    # refresh token must carry the `content` scope.
    seconds_since_merchant=$(( now_epoch - last_merchant_epoch ))
    if [ "$seconds_since_merchant" -ge 86400 ] \
            && [ -n "${GOOGLE_MERCHANT_ID:-}" ] \
            && [ -n "${GOOGLE_ADS_CLIENT_ID:-}" ] \
            && [ -n "${GOOGLE_ADS_CLIENT_SECRET:-}" ] \
            && [ -n "${GOOGLE_ADS_REFRESH_TOKEN:-}" ]; then
        last_merchant_epoch=$(date -u +%s)
        _run_bg "merchant_sync" \
            "python merchant_sync.py daily --days 7"
    fi

    # 2026-09-01 — 865FabLab corner auto-tag: any new "Corner Connector"
    # SKU under Wired4Signs USA whose description mentions a diffuser
    # gets SR200 + Supplier=865FabLab in CIN7, and is added to the
    # app's own build list. Anything without "diffuser" in the
    # description is logged and left untouched for manual review (see
    # fablab_corner_autotag.py docstring — plain 3D-printed plastic
    # connectors don't fit any SR2xx rule).
    seconds_since_fablab_autotag=$(( now_epoch - last_fablab_autotag_epoch ))
    if [ "$seconds_since_fablab_autotag" -ge 86400 ] \
            && [ -n "${CIN7_ACCOUNT_ID:-}" ] \
            && [ -n "${CIN7_APPLICATION_KEY:-}" ]; then
        last_fablab_autotag_epoch=$(date -u +%s)
        _run_bg "fablab_corner_autotag" \
            "python fablab_corner_autotag.py run"
    fi

    # 2026-09-01/02 — 865FabLab stock-drop alerts. Daily: scan flagged
    # SKUs for a suggested batch and post a new alert (deduped — see
    # fablab_stock_alerts table). Every 5 min: poll each active alert's
    # thread for an "approve" reply and push a real CIN7 Draft PO if
    # found (same conversations.replies pattern as the stock-issue
    # reply poll below — Slack doesn't return plain in-thread replies
    # via conversations.history, so a dedicated poll is required).
    seconds_since_fablab_stock_alert=$(( now_epoch - last_fablab_stock_alert_epoch ))
    if [ "$seconds_since_fablab_stock_alert" -ge 86400 ]; then
        last_fablab_stock_alert_epoch=$(date -u +%s)
        _run_bg "fablab_stock_alert" \
            "python fablab_stock_alert.py run"
    fi

    seconds_since_fablab_alert_replies=$(( now_epoch - last_fablab_alert_replies_epoch ))
    if [ "$seconds_since_fablab_alert_replies" -ge 300 ]; then
        last_fablab_alert_replies_epoch=$(date -u +%s)
        _run_bg "fablab_alert_check_replies" \
            "python fablab_stock_alert.py check-replies"
    fi

    # 2026-09-04 865FabLab assembly flow (fablab_assemblies.py):
    # (a) once a labor PO is authorised in CIN7, post one Slack message
    # per assembly + Odoo lead/quote; (b) complete assemblies from
    # `done` replies in those threads. Both idempotent via DB tables.
    seconds_since_fablab_assembly_po=$(( now_epoch - last_fablab_assembly_po_epoch ))
    if [ "$seconds_since_fablab_assembly_po" -ge 300 ]; then
        last_fablab_assembly_po_epoch=$(date -u +%s)
        _run_bg "fablab_assembly_check_po" \
            "python fablab_assemblies.py check-po"
    fi
    seconds_since_fablab_assembly_replies=$(( now_epoch - last_fablab_assembly_replies_epoch ))
    if [ "$seconds_since_fablab_assembly_replies" -ge 180 ]; then
        last_fablab_assembly_replies_epoch=$(date -u +%s)
        _run_bg "fablab_assembly_check_replies" \
            "python fablab_assemblies.py check-replies"
    fi

    # v2.67.130 PO dispatch reminders — when a PO transitions to
    # RECEIVED and its line comments contain SO-numbers (backorders
    # the buyer flagged), post a reminder to #fulfillment so the
    # team picks those orders first. Idempotent: each PO is
    # notified exactly once via the po_dispatch_reminders table.
    # Gated on SLACK_FULFILLMENT_CHANNEL_ID — silent skip if not
    # provisioned.
    #
    # v2.67.135 — interval dropped from 24h to 5 min (300s).
    # James wants the post within minutes of CIN7 receipt; the real
    # bottleneck is NearSync's CSV-write cadence (~15 min), not the
    # reminder cycle. Running every 5 min adds minimal load thanks
    # to PRIMARY KEY idempotency in po_dispatch_reminders — runs
    # that find nothing new exit cheaply.
    seconds_since_po_dispatch=$(( now_epoch - last_po_dispatch_epoch ))
    if [ "$seconds_since_po_dispatch" -ge 300 ] \
            && [ -n "${SLACK_FULFILLMENT_CHANNEL_ID:-}" ]; then
        last_po_dispatch_epoch=$(date -u +%s)
        _run_bg "po_dispatch_reminder" \
            "python po_dispatch_reminder.py daily"
    fi

    # v2.67.138 Drop-ship backorder warnings — when a customer
    # orders a SKU flagged DropShipMode='Always Drop Ship' in
    # CIN7, post a warning to #purchase-backorder telling the team
    # to approve the auto-created draft PO. 5-min cadence; gated
    # on SLACK_PURCHASE_BACKORDER_CHANNEL_ID.
    seconds_since_dropship=$(( now_epoch - last_dropship_epoch ))
    if [ "$seconds_since_dropship" -ge 300 ] \
            && [ -n "${SLACK_PURCHASE_BACKORDER_CHANNEL_ID:-}" ]; then
        last_dropship_epoch=$(date -u +%s)
        _run_bg "dropship_backorder" \
            "python dropship_backorder.py daily"
    fi

    # v2.67.144 Stock-issue DM escalation. When a stock_issue
    # has been awaiting_response for 4+ hours, DM the configured
    # stockkeeper with the full intelligence block. Gated on
    # SLACK_STOCKKEEPER_DM_CHANNEL_ID.
    seconds_since_si_escalate=$(( now_epoch - last_si_escalate_epoch ))
    if [ "$seconds_since_si_escalate" -ge 600 ] \
            && [ -n "${SLACK_STOCKKEEPER_DM_CHANNEL_ID:-}" ]; then
        last_si_escalate_epoch=$(date -u +%s)
        _run_bg "stock_issues_escalate" \
            "python stock_issues_handler.py escalate"
    fi

    # v2.67.254 Notion playbook pull. Every 30 min, mirror the
    # team's Notion playbook pages (direct child pages + rows of
    # any database under the playbooks parent) into the local
    # notion_kb_articles table so the AI's search_knowledge_base
    # tool sees the freshest content. Idempotent — silent skip
    # if NOTION_INTEGRATION_SECRET isn't set.
    seconds_since_notion_pull=$(( now_epoch - last_notion_pull_epoch ))
    if [ "$seconds_since_notion_pull" -ge 1800 ] \
            && [ -n "${NOTION_INTEGRATION_SECRET:-}" ]; then
        last_notion_pull_epoch=$(date -u +%s)
        _run_bg "notion_pull" \
            "python notion_sync.py pull-playbooks"
    fi

    # v2.67.254 Notion slow-movers push. Once a day (86400s), push
    # the top-N dormant SKUs into the Slow Movers database under
    # the team parent. Cheaper than every-30-min and the data
    # itself only changes when the engine runs.
    seconds_since_notion_push=$(( now_epoch - last_notion_push_epoch ))
    if [ "$seconds_since_notion_push" -ge 86400 ] \
            && [ -n "${NOTION_INTEGRATION_SECRET:-}" ]; then
        last_notion_push_epoch=$(date -u +%s)
        _run_bg "notion_push_slow_movers" \
            "python notion_sync.py slow-movers"
    fi

    # v2.67.281 Notion product-dimensions pull. Once a day, read
    # the Notion 'Product Dimensions' page (the source of truth)
    # back into the local product_dimensions table, so the AI
    # assistant + Slack bot's get_product_dimensions tool answers
    # from the current data — including any manual corrections
    # made on the Notion page. Idempotent; silent skip when
    # NOTION_INTEGRATION_SECRET isn't set.
    seconds_since_notion_dims=$(( now_epoch - last_notion_dims_epoch ))
    if [ "$seconds_since_notion_dims" -ge 86400 ] \
            && [ -n "${NOTION_INTEGRATION_SECRET:-}" ]; then
        last_notion_dims_epoch=$(date -u +%s)
        _run_bg "notion_pull_dimensions" \
            "python notion_sync.py pull-product-dimensions"
    fi

    # v2.67.285 IP observed lead-times pull. Weekly. Walks every
    # IP variant and captures the observed (avg_lead_time) and
    # configured lead time per SKU into the local ip_lead_times
    # table. The reorder engine then prefers these over the
    # supplier_config defaults — the biggest single waste-removal
    # cashflow lever, because IP measures real PO-to-receipt time
    # whereas our default was 35-day sea on unconfigured suppliers.
    # Silent skip if IP creds aren't set.
    seconds_since_ip_lt=$(( now_epoch - last_ip_lead_times_epoch ))
    if [ "$seconds_since_ip_lt" -ge 604800 ] \
            && [ -n "${IP_API_KEY:-}" ] \
            && [ -n "${IP_ACCOUNT:-}" ]; then
        last_ip_lead_times_epoch=$(date -u +%s)
        _run_bg "ip_lead_times_sync" \
            "python ip_lead_times.py sync"
    fi

    # v2.67.292 QBO Profit & Loss by month. Daily. Pulls the last
    # 14 months of P&L from QuickBooks (the reconciled financial
    # ledger) and stores per-month / per-account amounts in
    # qbo_monthly_pl. The Monthly Metrics page then displays the
    # QB-canonical Sales / COGS / Shipping rows alongside the
    # CIN7-derived ones — Viktor's cross-system audit found
    # CIN7-derived figures drift 27-218% on shipping and up to
    # 27% on historical COGS. Idempotent; silent skip if QBO
    # isn't connected yet.
    seconds_since_qbo_pl=$(( now_epoch - last_qbo_pl_epoch ))
    if [ "$seconds_since_qbo_pl" -ge 86400 ]; then
        last_qbo_pl_epoch=$(date -u +%s)
        _run_bg "qbo_monthly_pl" \
            "python qbo_monthly_pl.py sync"
    fi

    # v2.67.303 Shopify monthly discounts. Daily. Pulls Shopify
    # Admin API orders for the trailing 14 months and aggregates
    # total_discounts by month — replaces the CIN7 line-discount
    # proxy in Monthly Metrics Section 6 (which undercount by
    # 60-70%). Silent skip if Shopify creds aren't set.
    seconds_since_shopify_disc=$(( now_epoch - last_shopify_disc_epoch ))
    if [ "$seconds_since_shopify_disc" -ge 86400 ] \
            && [ -n "${SHOPIFY_DOMAIN:-}" ] \
            && [ -n "${SHOPIFY_ACCESS_TOKEN:-}" ]; then
        last_shopify_disc_epoch=$(date -u +%s)
        _run_bg "shopify_discounts" \
            "python shopify_discounts.py sync"
    fi

    # v2.67.274 Shopify content-sync fallback. daily_sync.sh runs
    # shopify_sync.py at 02:00 UTC but errors are swallowed and the
    # AI assistant shows a stale-data banner if the sync falls behind.
    # This block re-runs the sync from the Slack worker once per 24h
    # so staleness can't exceed 24h even if daily_sync.sh misses it.
    # Silently skipped when SHOPIFY_DOMAIN / SHOPIFY_ACCESS_TOKEN
    # aren't set (same guard as daily_sync.sh).
    seconds_since_shopify_sync=$(( now_epoch - last_shopify_sync_epoch ))
    if [ "$seconds_since_shopify_sync" -ge 86400 ] \
            && [ -n "${SHOPIFY_DOMAIN:-}" ] \
            && [ -n "${SHOPIFY_ACCESS_TOKEN:-}" ]; then
        last_shopify_sync_epoch=$(date -u +%s)
        _run_bg "shopify_sync" \
            "python shopify_sync.py"
    fi

    # v2.67.245 Stock-issue thread-reply poll. Slack's
    # conversations.history doesn't return regular (non-broadcast)
    # in-thread replies, so a Jamie 'fixed' reply was being missed
    # and the issue stayed awaiting_response (Brandon flagged
    # SO-56536). Every 5 min, poll conversations.replies for each
    # open issue and apply the resolution-keyword check directly.
    seconds_since_si_replies=$(( now_epoch - last_si_replies_epoch ))
    if [ "$seconds_since_si_replies" -ge 300 ]; then
        last_si_replies_epoch=$(date -u +%s)
        _run_bg "stock_issues_check_replies" \
            "python stock_issues_handler.py check-replies"
    fi

    # v2.67.144 Stock-issue morning summary. Fires once per day
    # at the configured hour (default 8:30 ET).
    # v2.67.154 — date marker persisted to /data so worker
    # restarts (env var changes, deploys) don't cause repeat
    # posts. Earlier shell-variable approach reset on every
    # restart and re-fired in the same time window.
    si_morning_hour="${STOCK_ISSUE_MORNING_HOUR_ET:-8}"
    now_utc_hour=$(date -u +%H)
    now_utc_minute=$(date -u +%M)
    today_utc=$(date -u +%Y-%m-%d)
    si_morning_utc_hour=$(( si_morning_hour + 4 ))
    si_morning_marker="/data/.last_si_morning_date"
    if [ -f "$si_morning_marker" ]; then
        last_si_morning_date=$(cat "$si_morning_marker" 2>/dev/null)
    fi
    if [ "${now_utc_hour#0}" -ge "$si_morning_utc_hour" ] \
            && [ "${now_utc_minute#0}" -ge 30 ] \
            && [ "$today_utc" != "$last_si_morning_date" ] \
            && [ -n "${SLACK_STOCK_ISSUES_CHANNEL_ID:-}" ]; then
        echo "$today_utc" > "$si_morning_marker"
        last_si_morning_date="$today_utc"
        _run_bg "stock_issues_morning" \
            "python stock_issues_handler.py morning-summary"
    fi

    # v2.67.194 Stock-locator audit morning post. Once per day at
    # the configured hour (default 7 ET → 11 UTC), run the BOM
    # parent/child bin-mismatch audit and post the summary to
    # SLACK_STOCK_ISSUES_CHANNEL_ID (or override via
    # SLACK_LOCATOR_AUDIT_CHANNEL_ID). Read-only — no CIN7 writes
    # in this version. Idempotent via /data marker so worker
    # restarts in the same day don't repeat.
    locator_audit_hour="${LOCATOR_AUDIT_MORNING_HOUR_ET:-7}"
    locator_audit_utc_hour=$(( locator_audit_hour + 4 ))
    locator_audit_marker="/data/.last_locator_audit_date"
    last_locator_audit_date=""
    if [ -f "$locator_audit_marker" ]; then
        last_locator_audit_date=$(cat "$locator_audit_marker" 2>/dev/null)
    fi
    if [ "${now_utc_hour#0}" -ge "$locator_audit_utc_hour" ] \
            && [ "${now_utc_minute#0}" -ge 30 ] \
            && [ "$today_utc" != "$last_locator_audit_date" ] \
            && ( [ -n "${SLACK_LOCATOR_AUDIT_CHANNEL_ID:-}" ] \
                  || [ -n "${SLACK_STOCK_ISSUES_CHANNEL_ID:-}" ] ); then
        echo "$today_utc" > "$locator_audit_marker"
        # Prefer the dedicated channel env var; fall back to
        # stock-issues if not set. The Python script reads
        # SLACK_STOCK_ISSUES_CHANNEL_ID by default — override
        # via --channel-id when LOCATOR_AUDIT_CHANNEL_ID is set.
        if [ -n "${SLACK_LOCATOR_AUDIT_CHANNEL_ID:-}" ]; then
            _run_bg "stock_locator_audit" \
                "python stock_locator_audit.py post-summary --channel-id \"$SLACK_LOCATOR_AUDIT_CHANNEL_ID\""
        else
            _run_bg "stock_locator_audit" \
                "python stock_locator_audit.py post-summary"
        fi
    fi

    # v2.67.152 Shipping margin monitor. Every 30 min, scan
    # ShipStation shipments for margin events outside ±5% of cost
    # (with a $5 floor) and post to #shipping-issues. Gated on
    # SLACK_SHIPPING_ISSUES_CHANNEL_ID — silent skip if unset.
    seconds_since_ship_margin=$(( now_epoch - last_ship_margin_epoch ))
    if [ "$seconds_since_ship_margin" -ge 1800 ] \
            && [ -n "${SLACK_SHIPPING_ISSUES_CHANNEL_ID:-}" ]; then
        last_ship_margin_epoch=$(date -u +%s)
        _run_bg "shipping_margin_monitor" \
            "python shipping_margin_monitor.py daily"
    fi

    # v2.67.140 Back-in-stock arrival notifications — when a PO is
    # received, check demand_signals for pending notify_me rows
    # matching the PO's SKUs/families and post a reminder to the
    # original #back-in-stock channel. No env var gate: the channel
    # is derived from the demand_signal's source_ref. Idempotent
    # via the back_in_stock_arrival_notifications table.
    seconds_since_bis_arrivals=$(( now_epoch - last_bis_arrivals_epoch ))
    if [ "$seconds_since_bis_arrivals" -ge 300 ]; then
        last_bis_arrivals_epoch=$(date -u +%s)
        _run_bg "bis_arrivals" \
            "python back_in_stock_handler.py check-arrivals"
    fi

    # Slack ingest → DB
    python slack_sync.py poll >> "$LOG" 2>&1 || \
        echo "[$(stamp)] slack_sync.poll failed (continuing)" >> "$LOG"

    # Listener: classify + respond to unprocessed
    python slack_listener.py once >> "$LOG" 2>&1 || \
        echo "[$(stamp)] slack_listener.once failed (continuing)" >> "$LOG"

    sleep "$INTERVAL"
done
