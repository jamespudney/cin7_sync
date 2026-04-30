# SKU migrations — predecessor / successor mappings

When a product line evolves, a new SKU often replaces an old one.
The team maintains a **migration mapping** so the reorder engine
knows that the new SKU's effective demand is the rolled-up
demand of the old SKU + new SKU.

## Why migrations matter

Without a migration mapping, two bad things happen:

1. **The successor looks like a slow seller.** Its raw 90-day
   demand is just its own ramp-up (a few units), so the engine
   under-orders.
2. **The predecessor looks like dead stock waiting to be cleared,**
   but if the team is using up the last of it deliberately, that's
   not the same thing as "no demand".

The migration mapping fixes both: the engine pretends the
predecessor's history "belongs" to the successor for forecasting
purposes.

## How a mapping is set

Every migration is a row in `db.sku_migrations`:

- `retiring_sku` — the old SKU that's being phased out.
- `successor_sku` — the new SKU that replaces it.
- `share_pct` — what % of retiring's demand should roll up to
  successor (usually 100; sometimes split if the line replaced
  multiple SKUs).
- `set_by` — who recorded it (a username, or `ip:imported` if
  imported from Inventory Planner's "merged" list).
- `set_at` — timestamp.

## How the engine uses migrations

When the engine computes 12-month demand for a successor:

```
effective_demand_12mo(successor)
  = own_units_12mo(successor)
  + sum(share_pct% × own_units_12mo(predecessor)
        for each predecessor)
```

This rolls up RECURSIVELY — if A→B→C, then C inherits both A and B's
history.

Same logic applies to:

- 90-day rollup (used for slow-stock dormancy)
- 45-day rollup (used for "rising demand" detection)
- Customer-concentration rollup (top customers are merged from
  predecessors so a customer's loyalty isn't lost on rebrand).

## How to view a chain

In the app, the **Migrations** page shows every recorded mapping.
You can also query via the AI Assistant (`get_migration_chain` tool).

## Pushing migrations to CIN7

When a migration is recorded, the script `cin7_push_migrations.py`
writes the successor SKU into the predecessor's `AdditionalAttribute5`
field in CIN7 (labelled "Replaced By" / "Predecessor or Replacement
Product"). This way buyers in CIN7 see the replacement context too.

**Important rule** discovered the hard way: the push only modifies
the AdditionalAttribute5 field. It NEVER renames or modifies the SKU
itself. SKUs are the join key between CIN7 and Shopify; renaming
them breaks the link. The push code has a hard whitelist of allowed
PUT body keys to enforce this.

## How to add a new migration

Three sources flow into `db.sku_migrations`:

1. **Inventory Planner import** — `ip_import_migrations.py` reads
   IP's `merged[]` array and writes one row per pair.
2. **Manual via the Migrations page** — buyer enters the retiring
   SKU + successor SKU + share %.
3. **CIN7 attribute import** — `cin7_ingest_attributes.py` reads
   the `AdditionalAttribute5` field across all products and adds
   any new mappings the team has typed directly in CIN7.

After any update, run `python cin7_push_migrations.py --apply` to
sync our DB back into CIN7 (so the AA5 field stays current there).
