# Wired4Signs Data & Operations Platform

This repo powers the Wired4Signs operations dashboard, Slack bot, sync jobs,
and buying intelligence around CIN7 Core data.

## Runtime Shape

The platform has two live services on Render:

- Dashboard: `app.py`, a Streamlit app for buying, stock, finance, and ops.
- Slack bot: `slack_listener.py`, the team-facing AI and action surface.

Both services read the same synced data snapshots and share team-written state
through the database layer in `db.py`.

## Data Flow

1. CIN7, Shopify, ShipStation, QuickBooks Online, Inventory Planner, and other
   sources are pulled by dedicated sync scripts.
2. CSV snapshots are written under the configured data directory, usually
   `/data/output/` on Render.
3. The dashboard and bot load those snapshots through `data_paths.py` and the
   helper loaders in `app.py`.
4. Buying intelligence is computed from the snapshots: ABC class, demand
   trend, stock cover, reorder quantity, slow-mover signals, and PO cost basis.
5. Team state such as notes, flags, approvals, Slack audit logs, and UI layout
   preferences is stored through `db.py`.

## Key Code Areas

- `cin7_sync.py`: CIN7 snapshot pulls.
- `app.py`: Streamlit entrypoint and remaining page orchestration.
- `app_config.py`: build/version display plus grouped dashboard navigation
  metadata.
- `app_pages/`: extracted dashboard page renderers and page-specific constants.
- `engine/`: Streamlit-free engine helpers that can be tested independently.
- `data_catalog.py`: snapshot discovery, freshness, and row-count reporting.
- `slack_listener.py`: Slack bot runtime.
- `ai_tools.py`, `ai_kb.py`, `intelligence_glossary.py`: AI tool definitions,
  knowledge, and shared terminology.
- `db.py`, `db_dialect.py`: SQLite/Postgres-compatible persistence.
- `worker_engine.py`, `demand_scoring.py`: background intelligence and demand
  scoring support.

## Persistent State

The platform has three different kinds of state:

- Source code: GitHub is the source of truth.
- Snapshot data: Render persistent disk under `/data/output/`.
- Team-written state: SQLite on Render persistent disk and the shared Postgres
  database during the ongoing migration.

Keep these separate. CSV snapshots can be regenerated from integrations, but
team-written database state should be treated as durable production data.

## Ordering Layout Preferences

Saved Ordering table layouts are keyed by the stable view value
`ordering_po_editor`. The value is centralized in
`app_pages/ordering_layout.py` as `ORDERING_PO_EDITOR_VIEW`.

Do not rename that value or reset the `ui_prefs` / `ui_presets` tables during
refactors. Existing buyer column order, visibility, widths, and presets depend
on that key.

## Deployment

Render watches the GitHub repo. A pushed commit triggers redeploys for the
dashboard and Slack bot, using `render.yaml` as the deployment blueprint.

The dashboard sidebar build chip is generated at runtime. `start.sh` stamps
the web service with `APP_BUILD_COMMIT` and `APP_BUILD_DATE`; `app_config.py`
falls back to Render/Git metadata, then the old static version only if metadata
is unavailable. Do not rely on manually editing a version/date string for
normal deploys.

The normal release flow is:

1. Make a scoped code change.
2. Run local compile/tests.
3. Commit and push to GitHub.
4. Render deploys the updated services.
5. Verify the live dashboard and bot behavior.

## Documentation Discipline

Behavior changes should update the knowledge sources at the same time as code.
The dashboard AI Assistant, Slack bot, and human users all depend on the docs
and glossary to understand how the platform works.

When a change affects workflow, source-of-truth rules, sync cadence, AI tool
behavior, engine logic, deployment behavior, or UI terminology, update the
relevant files in `docs/`, `ARCHITECTURE.md`, `README.md`, or
`intelligence_glossary.py` in the same PR.

## Structural Direction

The original dashboard grew as a large single-file Streamlit app. New work
should continue moving low-risk slices into focused modules:

- Shared constants and navigation in `app_config.py`.
- Page renderers in `app_pages/`.
- Testable business logic in `engine/`.
- Data discovery and freshness checks in `data_catalog.py`.

When extracting code, keep database keys, Streamlit session keys, and saved
user preference identifiers stable unless there is a deliberate migration plan.
