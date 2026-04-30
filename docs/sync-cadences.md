# Data sync cadences

The app keeps its data fresh through two scheduled processes plus
manual triggers. Both run inside the live Render web service
container so they share the same persistent disk as Streamlit.

## Nearsync — every 15 minutes, all day

Runs `python cin7_sync.py nearsync --days 1`. Pulls:

- Stock on hand (full snapshot)
- Stock adjustments + transfers (last 1 day)
- Sales headers + sale lines (last 1 day)
- Purchase headers + purchase lines (last 1 day)

About 1-3 minutes per run. ~10-30 CIN7 API calls. Designed to keep
the Ordering page accurate while buyers are placing supplier POs
during business hours.

Skipped during the 02:00 UTC daily-sync hour to avoid contention.

Logs: `/data/output/nearsync.log` (latest run) and
`/data/output/nearsync_loop.log` (loop heartbeat).

## Daily full sync — once at 02:00 UTC

Runs `daily_sync.sh`. Calls:

1. `cin7_sync.py quick --days 3` — products, suppliers, customers,
   stock, sales/purchase headers (all masters refreshed).
2. `cin7_sync.py salelines --days 3` — line-item refresh for any
   sales updated in the last 3 days.
3. `cin7_sync.py purchaselines --days 7` — same for purchases.
4. `sync_sku_renames.py --apply` — propagates any SKU renames CIN7
   has made into our local DB.
5. `sync_supplier_names.py --apply` — same for supplier names.

Heavier than nearsync (~10-15 minutes total). Done overnight so
the workday runs unaffected.

Logs: `/data/output/daily_sync.log`.

## What is NOT in either sync

- **BOMs** — only refreshed on manual run (`cin7_sync.py boms`).
  ~2 hours full pull. Run when you've added/edited a BOM in CIN7.
- **Movements** — derived audit log; not used by the engine for
  daily decisions. Pulled manually if needed.

## Rate limiting

CIN7 caps the account at ~60 calls/minute, **shared** across all
integrations (us, Inventory Planner, Shopify, etc.). We throttle
ourselves at 2.5 seconds between calls (24/min target) so we stay
well under the cap and don't trigger 429s. A 429 costs 60 seconds of
sleep, so being conservative is faster end-to-end than being greedy.

## Why we use a worker loop, not Render's cron jobs

Render's cron services can't mount persistent disks. Our syncs need
to write to `/data/team_actions.db` and `/data/output/`. Solution:
the syncs run as background processes inside the web service's
container, sharing its disk.

`start.sh` launches three things:

1. `nearsync_loop.sh` — sleep-loop, runs every 15 min.
2. `sync_loop.sh` — sleep-loop, fires once per day at SYNC_HOUR_UTC.
3. Streamlit (foreground, so Render's health check reaches it).
