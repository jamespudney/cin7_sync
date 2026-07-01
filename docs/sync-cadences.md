# Data sync cadences

The app keeps its data fresh through two long-running scheduled
loops plus a few lightweight scheduled tasks and manual triggers.
They run inside the live Render web service container so they share
the same persistent disk as Streamlit.

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
6. `ip_sync_notes.py` — fast Inventory Planner replenishment-notes
   refresh into `/data/output/ip_notes_*.csv`. These are the buyer
   notes surfaced in Product Detail, Ordering drill-ins, PO commentary,
   and the Slack bot. Example: `LED-SMOKIES38-B-3` can carry a note
   like `BWF - MOQ 1000m/profile`.
7. `ip_pull_alternates.py` — heavier Inventory Planner knowledge export
   for combine-sales/stock links, settings, velocities, vendors,
   forecasts, and a second notes capture.

Heavier than nearsync (~10-15 minutes total). Done overnight so
the workday runs unaffected. After the sync completes, `sync_loop.sh`
starts `warm_engine.py` in the background to refresh the ABC engine
snapshot. On deploy catch-up it waits `WARM_ENGINE_BOOT_DELAY_MIN`
minutes first (default `30`) so the engine warmer does not compete
with Streamlit startup. The warmer also has a lock, timeout, and
`WARM_ENGINE_MIN_AVAILABLE_MB` memory guard.

Logs: `/data/output/daily_sync.log`.

## QBO cashflow sync — every 4 hours by default

Runs `python cashflow_sync.py sync --months-back 6`. Pulls:

- Recent QuickBooks Online supplier Bills for invoice detail.
- The full QBO open-bills list for authoritative open balances.

This is triggered by `nearsync_loop.sh` after its own near-sync work,
not by a third always-on process. It waits
`QBO_CASHFLOW_BOOT_DELAY_MIN` minutes after deploy (default `30`) so
QuickBooks work does not compete with Streamlit startup. This keeps
the Cashflow page from showing supplier invoices that were already
paid in QuickBooks. The dashboard's **Sync from QuickBooks** button
remains available for an immediate manual refresh.

Cadence is controlled by `QBO_CASHFLOW_INTERVAL_HOURS` on Render
(default `4`). The recent detail window is controlled by
`QBO_CASHFLOW_MONTHS_BACK` (default `6`); the sync still checks the
full open-bills list regardless of that window.

Logs: `/data/output/qbo_cashflow_loop.log`.

## What is NOT in these scheduled syncs

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

QBO supplier-bill refreshes are scheduled from inside
`nearsync_loop.sh` with a lock, timeout, and boot delay, so they do
not add another permanent process to the Streamlit container.
