# Glossary — terms used throughout the app

Quick reference for terms that come up in conversations, the AI
Assistant, and the buyer dashboard.

## ABC class

A SKU's importance ranking, computed daily by the engine. **A** =
top 20% by combined revenue + qty rank, reviewed often, tightest
safety stock. **B** = middle 30%. **C** = bottom 50%, loosest
review cadence. SKUs with zero 12-month movement get "—" instead
of A/B/C; they're handled by the slow/dead pipeline.

## Active

Default classification for a SKU that's selling at a reasonable
clip relative to its on-hand stock. Reorder engine treats it as
in-scope.

## AdditionalAttribute1 (AA1) / AdditionalAttribute5 (AA5)

CIN7 product fields. AA1 is conventionally used for product family
code (e.g. "SIERRA38", "KP24"). AA5 is "Replaced By" / predecessor
mapping — written by `cin7_push_migrations.py`.

## Available stock

On-hand stock minus any reserved/allocated quantity (e.g., to a
sales order that hasn't shipped yet). What's actually free to sell
or transfer.

## BOM (Bill of Materials)

A parent assembly SKU that consumes one or more child component
SKUs when built. The engine rolls component demand up from parent
sales so child SKUs aren't mis-classified as dead.

## Classification

The slow/dead/active/watchlist label assigned per SKU. Drives
buyer warnings and reorder logic. See `inventory-rules.md`.

## Daily sync

The 02:00 UTC nightly run that refreshes CIN7 masters + the last
3 days of sales/purchases (headers + line items). See
`sync-cadences.md`.

## Dead stock

A SKU with positive on-hand stock and zero sales in the dormancy
window (default 365 days). Buyer should NOT reorder; sales should
consider pushing.

## Dormancy window

How far back the engine looks to decide if a SKU is dead. Default
365 days. Configurable per supplier in `db.supplier_config`.

## Effective demand

A SKU's demand AFTER rolling up migration predecessors and BOM
parents. The engine uses effective demand (not raw demand) for
reorder calculations. See `migrations.md`.

## Engine

Shorthand for `_abc_engine` — the function that computes per-SKU
ABC class, classification, target stock, and suggested reorder.
Cached on disk (`@st.cache_data(persist="disk")`); recomputes when
its input DataFrames change.

## Family

A group of related SKU variants. Conventionally captured in the
product's AdditionalAttribute1 field. Common families: SIERRA38,
CASCADE38, KP24, MP. See `migrations.md` and `inventory-rules.md`
for how the engine treats families.

## FixedCost

Per-SKU per-supplier fixed cost the buyer can override. Lives in
`db.sku_supplier_overrides`. Beats CIN7's AverageCost when both
exist. Used by the FixedCost Audit page and by PO push as the
preferred Price source.

## MOQ — Minimum Order Quantity

Smallest units-per-line a supplier will accept. Configured in
`db.supplier_config.moq_units`.

## MOV — Minimum Order Value

Smallest dollar value a supplier will accept on a PO. Configured
in `db.supplier_config.mov_amount`. Surfaced on the Ordering page
when a draft falls short.

## Nearsync

The 15-minute intra-day sync. Pulls stock + last-day sales/
purchases. Keeps the Ordering page accurate while buyers work.
See `sync-cadences.md`.

## On-hand

Physical inventory currently on the shelf, per CIN7. Does NOT
account for reservations.

## Predecessor / Successor

Two ends of a migration mapping. Predecessor is the retiring SKU;
successor is the new SKU that replaces it. The successor inherits
the predecessor's demand history for forecasting.

## Reorder Suggested

The qty the engine recommends ordering. Computed as `target_stock
− on_hand − on_order`, capped at zero. See `reorder-engine.md`.

## Slow-moving

A SKU that's selling, but at a low rate vs. on-hand cover (>~12
months of stock at current velocity). Buyer should reorder
cautiously; sales might offer it as a substitute.

## Watchlist

Borderline SKUs that don't clearly fit slow/dead/active. Buyer
reviews case-by-case.
