# Reorder engine — how it decides what to suggest

The Ordering page's "Suggested reorder" column is computed by the
ABC engine (`_abc_engine` in app.py) once per page load, then cached
on disk.

## What the engine considers

For each Stock-typed SKU, the engine looks at:

1. **12-month effective demand.** Sum of units sold in the last 365
   days, INCLUDING units rolled up from migration predecessors and
   BOM children. Migration-aware so a successor inherits its
   predecessor's velocity.
2. **On-hand stock.** Current physical inventory.
3. **Available stock.** Physical minus reserved/allocated.
4. **Open POs.** Quantity already on order with the supplier.
5. **Lead time.** From `db.supplier_config` (or the SKU's family
   default). Sea vs. air lead times are separate.
6. **Safety factor.** Per-class buffer — A-class items get the
   tightest safety stock (default 30% buffer), C-class the most
   generous (15%).
7. **Review window.** Per-class — A-class reviewed every 14 days,
   C-class every 45.

## ABC classification

A SKU's class is a hybrid of value rank and quantity rank:

- 60% weight: 12-month revenue percentile.
- 40% weight: 12-month qty percentile.

Then bucketed:

- **A** — top 20% by combined rank. Reviewed often; tightest safety.
- **B** — middle 30%. Standard review.
- **C** — bottom 50%. Loosest review; least urgent.

Items with zero 12-month movement are not assigned A/B/C — they get
"—" and are handled by the slow/dead stock pipeline instead.

## Suggested reorder formula

For an A/B/C-classified SKU not currently flagged dead/slow:

```
target_stock = (daily_velocity × (lead_time_days + review_days))
               × (1 + safety_pct/100)
suggested_reorder = max(0, target_stock - on_hand - on_order)
```

Where:

- `daily_velocity = effective_units_12mo / 365` (using rolled-up,
  migration-aware demand).
- `lead_time_days` is the supplier's stated lead time for the
  preferred freight mode.
- `review_days` is the class-specific window so we order enough to
  carry through to the next review.
- `safety_pct` is the class-specific buffer.

## Special cases

- **Recently migrated successor SKU** — its daily_velocity is the
  rolled-up family velocity, not just its own (which would be tiny
  since the migration is recent).
- **Dropship SKUs** — excluded from reorder suggestions; they ship
  direct from the supplier per-order.
- **Items in active PO drafts** — the suggested qty doesn't account
  for what's currently in a local PO draft (since drafts may not
  be submitted). The Ordering page shows the qty already in your
  draft as a separate column.
- **Override flags** — `db.sku_policy_overrides` rows take
  precedence over the engine's suggestion if present.

## Why the suggestion can differ from CIN7's "Reorder Quantity"

CIN7's product-level `MinimumBeforeReorder` and `ReorderQuantity`
are static values set per-SKU in CIN7. Our engine is dynamic — it
recomputes daily based on actual sales velocity, migration rollup,
and BOM dependency. They agree on simple cases but diverge for SKUs
with recent volatility, migrations, or BOM relationships.
