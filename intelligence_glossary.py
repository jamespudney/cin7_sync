"""intelligence_glossary.py (v2.67.180)
=========================================

Single source of truth for the engine's intelligence rules.

This module exists so the Streamlit dashboard AND the Slack bot
worker reason from the **same rule book**. Both import the same
GLOSSARY_MARKDOWN string. When the dashboard's glossary is
updated, the bot's system prompt automatically reflects the
change — no copy-paste, no drift.

User principle (v2.67.70): "the slack bot must always match
answers that our ai assistant would give". This module is the
foundation of that promise.

Usage:
  from intelligence_glossary import GLOSSARY_MARKDOWN

  # In the dashboard:
  with st.expander("How to read this page"):
      st.markdown(GLOSSARY_MARKDOWN)

  # In the bot's system prompt:
  system = f"INTELLIGENCE MODEL CONTEXT:\\n{GLOSSARY_MARKDOWN}\\n..."

When updating: edit ONLY this file. Both services pick up the
change at next deploy / restart. Don't copy text into other
files.

====================================================================
KEEP-THIS-UPDATED RULE (v2.67.180)
====================================================================

This glossary is shown to:
  1. Every staff user on the Ordering / Slow Movers / Overview
     pages (via the in-page expander).
  2. Claude (the AI Assistant + the Slack bot) on EVERY query as
     part of the system prompt — it's how the AI knows what
     'A-class grace' or 'EffectiveUnitCost' actually mean.

So if you ship a commit that:
  • Adds, renames, or changes the meaning of an engine column
    (e.g. new `excess_units` definition, new `is_dormant` rule)
  • Adds a new engine-derived metric the user will see
    (e.g. v2.67.178's "Slow Stock Cleared")
  • Changes a threshold or grace rule (e.g. A-class grace days)
  • Adds a new fly-wheel signal (📦, 🪫, etc.)

…YOU MUST update GLOSSARY_MARKDOWN below in the same commit.

Don't worry about over-documenting. The cost of out-of-date
glossary is the bot gives wrong-but-confident answers to staff
and customers. The cost of an extra paragraph is nothing.
"""

GLOSSARY_MARKDOWN = """
#### ABC class
Every SKU is ranked A / B / C on a hybrid score (60% of 12-month value
rank + 40% of 12-month quantity rank):
- **A** — top cumulative 70% of annual value. High-impact items, watch closely.
- **B** — next 20%. Steady movers.
- **C** — last 10%. Low-impact, review less frequently.

#### Lead time (LT)
How long from placing the PO to receiving the goods. Set per supplier
in the Supplier configuration expander below. Air vs sea toggles use
different LTs; the engine picks the faster one when the supplier offers
air AND the item qualifies.

#### Safety %
A buffer added on top of lead-time demand to absorb variance (a big
order, a bad month). Defaults per class: A=30%, B=20%, C=15%.

#### Review days
How long between buying reviews for this supplier. The engine adds
`avg_daily × review_days` to target stock so you're covered between
reviews. Default: A=14d, B=30d, C=45d. Longer review = more stock
buffer, fewer POs. Shorter review = less capital tied up, more
frequent ordering.

#### Target stock — the reorder target
**`target = (LT × avg_daily × (1 + safety%)) + (avg_daily × review_days)`**
This is how much stock should be sitting on the shelf on a typical day
to cover the lead time and the review period without stocking out.

#### Suggested reorder (engine)
**`max(0, target − (Available + OnOrder − unfulfilled))`**
Only what you need to bring effective position back up to target.
Already accounts for open POs (ORDERED / ORDERING) and backorders.

#### OnHand / Allocated / Available
- **OnHand** — physical units in the warehouse.
- **Allocated** — reserved for existing customer orders.
- **Available** — OnHand − Allocated.

#### OnOrder
Units already placed on open POs (status ORDERED or ORDERING). The
engine subtracts these from what you need to reorder — you won't get
a suggestion to buy something that's already on its way.

#### Unfulfilled (backorders)
Customer orders with status BACKORDERED / ORDERED / ORDERING — units
customers are waiting on. Subtracted from effective position so the
engine prioritises SKUs that owe customers.

#### DoC (days of cover)
**`OnHand / avg_daily`** — how many days the current stock will last
at the 12-month average sales rate.

#### Effective units (12mo)
Direct sales + sales rolled up from child variants (MP variants, cuts,
kit components) + sales migrated from retiring SKUs. Used for the
reorder math, NOT the raw "units_12mo" figure.

#### FixedCost / AverageCost / PO cost
- **FixedCost** — the agreed supplier price on the SKU's supplier record
  in CIN7. What you'll actually pay on the PO.
- **AverageCost** — CIN7's weighted landed cost (drifts with every PO).
- **PO cost** — FixedCost if set, otherwise AverageCost fallback.
  Shown per row with a "Basis" column so you can see which one applied.

#### MOV (minimum order value)
Set per supplier (e.g. Blebox $250). The PO summary flags when the
current draft is below MOV so you can consolidate.

#### Freight mode
Air or Sea. The engine defaults to air when the supplier offers it
**and** the SKU's length fits in the supplier's air cutoff (e.g.
Topmet UPS caps at 2200mm). Override per row in the grid; the reorder
qty recalculates with the new lead time on next refresh.

#### Status badges
- 📦 **Dropship** — order-on-demand, we don't stock it.
- Active, Deprecated, Discontinued — from CIN7's product status.

#### Trend signal (📈 / 🎯 / 🔀 / 📉)
A secondary check the engine runs to detect when the last-45-day sales
pattern has diverged from the prior 45 days (days 45-90 ago). Uses
four signals combined to avoid false-positives:

- **📈 Trend** — ALL of these must be true: momentum >1.5, **4+ distinct
  customers**, top customer **under 40%**, and non-top customers averaging
  **at least 2 units each**. Real broad-based demand; engine switches to
  last-45d velocity to keep up.
- **🎯 Project** — ANY of these triggers: top customer **≥50%** of 45d
  volume, top **2 customers combined ≥70%**, or fewer than 3 distinct
  customers. Looks concentrated / one-off; engine subtracts top
  customer's 12mo contribution before forecasting to avoid over-ordering.
- **🔀 Mixed** — spike exists but fails both sets of rules. Watch
  signal, no velocity override.
- **📉 Decline** — units down 50%+ vs prior 45 days. Worth review.
- **Stable** — everything else.

**Why "top-2 combined" matters**: 8 customers with one buying 50% and
a second buying 20% is still concentrated (top-2 = 70%). The tighter
thresholds stop "many customers" from hiding real concentration.

**Why "non-top avg units"**: a SKU with 8 customers where the top buyer
took half leaves maybe 1-2 units each for the rest — that's not a trend,
that's noise. The ≥2 units average rule makes sure there's substance
beyond the big buyer.

Low-volume guard: SKUs selling fewer than 3 units in the last 45 days
skip classification entirely — the signal is too noisy at that scale.

The trend breakdown (who's buying, what %) shows in the transparency
panel at the bottom when you drill into any flagged SKU.

#### The 5 things driving reorder qty on any row
1. **12mo effective demand** (direct + rollups)
2. **Lead time** (longer = more stock)
3. **Safety + review days** (more buffer = more stock)
4. **What we already have** (OnHand, OnOrder, Available, Allocated)
5. **What we owe customers** (unfulfilled backorders bring it up)

For the full step-by-step math on any individual SKU, scroll to the
**transparency panel** below the PO table and pick the SKU — the
engine shows every input and how it got to the suggestion.

#### Slow movers / dormancy (v2.67.36+)
A SKU is **dormant** when its 90-day demand has dropped sharply
versus its 12-month baseline (≈80% drop), AND it still has stock
on hand. Computed by the engine on every recompute. Definitions:

- `is_dormant` (bool) — engine output column. True = currently slow.
- **Once-slow warning** — once a SKU has been flagged dormant, the
  fact persists in `sku_dormancy_log` even after the engine
  re-classifies it as active. The Ordering page renders ❗ in the
  Status column and a `⚠️ WAS slow-moving` auto-prefix in the
  Notes column. Auto-lifts after 90 days of sustained activity, or
  the buyer can dismiss manually from the Slow Movers page.
- **A-class grace (v2.67.48)** — A-class SKUs with positive 12mo
  demand are EXEMPT from dormancy flagging. Reasoning: A-class is
  by definition a steady-revenue mover; if the buyer over-bought
  to secure better pricing, recent sales naturally drop while
  stock is high — but the long-term pattern is unchanged. Flagging
  these would discourage reordering of steady movers. The grace
  applies in both `_is_dormant` (base rule) and
  `_refine_dormancy_by_class` (class-aware refinement). The
  Ordering page surfaces a 💼 note explaining the grace when an
  A-class item's 90d activity is below threshold.

#### Overstock / excess (v2.67.47+)
- `excess_units` — units held beyond expected near-term demand.
  Two implementations:
  - **Naive (in `_abc_engine`)** — `max(0, OnHand - effective_units_12mo)`.
    Always available, used by Slow Movers + Overview when Ordering
    hasn't run in this session.
  - **Precise (in Ordering page)** — `max(0, OnHand - target_stock)`
    where target_stock factors supplier lead time, safety stock,
    and review window. Overwrites the naive value on the cached
    engine_df once the Ordering page runs.
- `excess_value` — `excess_units × per-unit cost`. Cash that
  could be freed up by clearing the overstock down to target.

#### 🪫 REMNANT flag (v2.67.31)
Bulk-roll parent SKUs with `OnHand < 1.0` (less than one full
roll's worth). The engine's slow/dormant signals don't capture
"we have 0.4 of a 100m roll left" because per-foot child sales
roll up to the parent and keep its activity counter non-zero —
but practically, a partial roll is stock-to-clear. The flag
appears as a 🪫 prefix in the AI Assistant's stock-listing
answers and gets called out in product-discovery rows. Different
signal from slow-moving; both can apply to the same SKU.

#### Stock-reduction fly-wheel signals
Together these signals power the Slow Movers page, the Overview
slow-mover panel, the weekly digest email, and the AI Assistant's
stock-reduction answers:

- ⚠️ **SLOW** — `is_dormant=True` and OnHand>0
- 🔴 **DEAD** — OnHand>0 with zero 12mo effective demand
- 📦 **EXCESS** — `excess_units > 0` (over target)
- 🪫 **REMNANT** — bulk-roll parent with OnHand < 1.0
- 💼 **A-class grace** — would have flagged but A-class trumps
- ❗ **Once-slow warning** — was flagged in the past 90+ days,
  warning still active

#### Sales staff vs buyer signals
The Slow Movers page is buyer-facing: it shows what to clear and
lets you dismiss warnings. The AI Assistant is sales-facing: when
sales staff ask "what warm white strips do we have?" the answer
includes inline ⚠️/🔴/📦 flags so they know which items to
push.

#### parents_only filter (v2.67.22+)
The AI Assistant's `find_products` and `search_products_by_text`
tools default to `parents_only=true`, mirroring the Ordering
page's PO-suggestion logic. Hides per-foot cuts and BOM
derivatives in favor of the supplier-orderable parent
(LEDIRIS2700-120-100M) so the answer matches what the buyer
would actually order.

#### Bin location
Warehouse shelf location for each SKU, pulled from `stock_on_hand`
and surfaced through the AI Assistant's `get_sku_details`. Answers
"where do we keep X?".

#### PO Comments + Shipping notes (v2.67.44, expanded v2.67.52)
The AI now surfaces FIVE freeform text fields on every PO — each
typed by the buyer for a different purpose:
- **Comments** — top-level header free-text the buyer uses to
  record airfreight/seafreight or one-line ETA notes.
- **Shipping notes** — attribute under the "Vendor purchase"
  attribute set, used for richer progress detail like "departed
  Shenzhen 2026-04-12, in customs".
- **Memo** *(v2.67.52)* — the "Purchase Order Memo" big text box
  on the CIN7 PO form. The buyer's main instruction field for
  the entire order.
- **Note** *(v2.67.52)* — separate top-level note field. CIN7
  sometimes uses this for status / blame, e.g. "shipped in error
  by supplier — original PO-XXXX cancelled".
- **Terms** *(v2.67.52)* — payment terms (Net 30, Payment with
  Order, etc.).
All five flow through `get_incoming_stock` and `get_purchase_order`
so AI shipment-status answers report every signal the buyer
recorded.

#### Sale-side freeform fields (v2.67.52)
Sale orders have a parallel set of freeform fields the rep types
into. `get_sale_order` surfaces all of them:
- **Memo** — "Sale Order Memo" big text box. Rep's
  build/delivery instructions (e.g. "solder 5ft wire lead to
  each 5m roll").
- **Note** — top-level header note.
- **ShippingNotes** — delivery instructions (top-level on sales,
  unlike POs where it's an attribute).
- **Terms** — payment terms.
- **CustomerReference** — customer's own PO# referencing this
  sale.

#### Shopify order tracing (v2.67.55)
CIN7 records sales from the Shopify channel with
SourceChannel='Shopify' but doesn't carry the Shopify-side
conversion fields. shopify_sync.py now mirrors them locally so
the AI's `get_shopify_order` tool can answer "how did we get
this conversion" questions:

- **source_name** — Shopify's classification: web / pos /
  shopify_draft_order / mobile_app / etc.
- **landing_site** — URL of the FIRST page the customer hit on
  the storefront.
- **referring_site** — where the customer was BEFORE landing
  (google.com, instagram.com, t.co, etc.). The most useful
  field for marketing attribution.
- **note_attributes** — custom key=value pairs Shopify themes
  / apps stash on the order. UTM params often go here.
- **discount_codes** — coupons / promo codes redeemed.
- **customer_orders_count** + **customer_total_spent** — quick
  returning-customer flag.

When the AI sees `get_sale_order` return SourceChannel=Shopify,
it proactively follows up with `get_shopify_order` for the
joined view rather than "I have CIN7 data, check Shopify yourself".

#### ShipStation integration (v2.67.54)
ShipStation shipment data feeds two places:
- **AI Assistant** — `get_shipping_details(order_number /
  tracking_number / customer + date)` returns ship date, carrier,
  service, tracking number, ship-to address, shipment cost,
  weight, item summary, customer/internal notes. Voided shipments
  are flagged explicitly.
- **Monthly Metrics** — the "Shipping Cost" row in the Margins
  block aggregates `shipmentCost` per month (excluding voided
  shipments). Pre-ShipStation months show 0; post-integration
  months show real freight spend.

Setup:
1. Set `SHIPSTATION_API_KEY` + `SHIPSTATION_API_SECRET` in env.
2. One-time backfill: `python shipstation_sync.py full --days
   1825` (5 years; 30-60 minutes for a busy account).
3. NearSync (1-day) and Daily Sync (7-day) catch-ups run
   automatically once env vars are set. NearSync keeps
   shipments visible to the AI within 15 minutes of label
   creation.

#### Transaction lookup (v2.67.51)
The AI Assistant can pull up specific CIN7 documents on demand.
Three tools, picked by what kind of number the user mentions:
- **`get_purchase_order(po_number=PO-XXXX)`** — full PO lookup.
  Returns supplier, every line item (SKU / qty / price), Status,
  Required-By, Comments + Shipping notes. Includes received /
  closed POs (unlike `get_incoming_stock` which is open-only).
- **`get_sale_order(order_number / invoice_number / customer +
  date_from)`** — full sale lookup. Returns customer, every line
  item, line_total. Useful for "what did Acme buy on SO-12345"
  or "who ordered LED-V3060001-2 last week".
- **`get_stock_adjustment(stocktake_number / date_from)`** —
  adjustment header lookup. Returns EffectiveDate / Status /
  Reference. Per-SKU line detail is NOT in the local sync;
  the AI tells the user to view the line breakdown in CIN7.

#### Local sync windows (v2.67.51)
The AI's transaction tools read from local CSVs the daily sync
drops:
- **Purchase lines** — 30-day rolling window (bumped from 7d in
  v2.67.51 after PO-7109 was missed). The widest available window
  file is used as the base, with newer 1-day files merged on top.
- **Sale lines** — 30-day rolling window (bumped from 3d in
  v2.67.43). Plus the 1825-day longest-history file when present.
- **Stock adjustments** — 30-day window, headers only (no per-SKU
  line detail in the bulk endpoint).
- **Stock-on-hand `OnOrder` field** — the canonical PO total,
  refreshed every NearSync (15-min). When `get_incoming_stock`
  returns no PO lines but `OnOrder>0`, the tool flags this as a
  data gap rather than claiming "no PO exists".

#### Cost basis chain — how the engine values stock (v2.67.180)
The engine values inventory at `OnHand × EffectiveUnitCost`.
EffectiveUnitCost is resolved per SKU via a fall-back chain — the
first hit wins:

1. **Direct CIN7 cost** — CIN7's FIFO `AverageCost` if it has
   shipped enough product to publish one. Most accurate.
2. **Family-median fallback** — median AverageCost across all
   SKUs in the same product family. Used when direct is missing
   but siblings have cost data.
3. **Category-median fallback** — median across the broader
   category. Wider net, less precise.
4. **Unknown** — no cost basis available. Contributes $0 to
   Optimum and excess calculations (rather than blowing them up
   with a phantom valuation).

`CostBasisDetail` column on engine_df marks which path was used
per SKU. The Ordering page surfaces a "Cost basis coverage"
caption showing how many SKUs hit each tier — the more 'direct',
the more trustworthy the totals.

#### OnHandValue (engine column)
`OnHand × EffectiveUnitCost` per SKU. The engine's per-row
inventory valuation. Used by every page that sums
"slow-stock value", "excess value", etc., so the figures tie
out. Distinct from CIN7's headline `StockOnHand` value which
uses CIN7's own FIFO (the Overview headline tile reports both
and a delta caption when they diverge significantly).

#### TargetValue (engine column)
`target_stock × EffectiveUnitCost` per SKU. The dollar value of
the engine's recommended on-hand level. Summed across masters,
this is the **Optimum stock value** tile on the Ordering page
and the target the glide-path projects toward (v2.67.178 — no
more hardcoded $600k constant).

#### Optimum stock value (the glide-path target, v2.67.178)
**`sum(target_stock × EffectiveUnitCost across masters only)`**

What working capital SHOULD be tied up at, per the engine. The
Ordering page's "Optimum" tile + glide-path strip below the
tiles both source from here. If current stock > optimum, the
glide path shows the gap + an **ETA** based on the trailing-90d
slow-mover clearance rate (`gap / monthly_clearance`). If
under, it just shows % of optimum so the buyer knows to keep
ordering.

The optimum is masters-only because non-master variants
(per-foot cuts) roll their demand up to the master; counting
them would double-count.

#### Excess, Understock & how the Ordering tiles reconcile (v2.67.282)
The Ordering page headline has five tiles. They each measure a
different thing, so they do NOT form a simple subtraction. How
they relate:

- **Current stock value** — CIN7 FIFO value summed across ALL
  SKUs. No cost fallback. Ties to the Overview tile.
- **Optimum stock value** — `sum(TargetValue)` across masters
  only (cost fallbacks allowed).
- **Excess (cash to free up)** — `sum(max(0, OnHandValue −
  TargetValue))` across masters, PLUS dead non-master cuts at
  full value. A GROSS figure: it floors every SKU at zero, so it
  counts only SKUs OVER target and never nets the ones UNDER.
- **Understock (cash to redeploy)** — `sum(max(0, TargetValue −
  OnHandValue))` across masters. The half Excess omits: the
  spend needed to bring under-target SKUs up to target.
- **Dead stock** — a SUBSET of Excess (zero-demand SKUs still
  holding stock), surfaced separately. Not additive.

Exact reconciliation identity (master SKUs):
`master_overstock − understock = master_onhand − optimum`.

So **Excess is intentionally larger than Current − Optimum**:
Current − Optimum is the NET over-position, Excess is the GROSS
overstock before netting the under-stocked SKUs back in. Net
working capital actually freed ≈ `Excess − Understock`. Current
vs Optimum also differ in scope (all SKUs vs masters) and cost
basis (CIN7 FIFO vs cost-chain fallbacks), so the tiles never
tie to the exact dollar — the bridge caption under the tiles
states the live numbers.

#### Reorder engine: cadence + holiday cover (v2.67.283-284)
The reorder engine's target stock is built from four components,
in this order:

`target = lead_time_demand + safety_stock + review_period_demand
         + holiday_cover`

- **Lead-time demand** = `avg_daily × lead_time_days`. The stock
  needed to cover demand while the next order is in transit.
- **Safety stock** = `lead_time_demand × safety_pct`. ABC class
  drives this (A=30%, B=20%, C=15% by default). This is the only
  thing ABC class controls now — it no longer touches the review
  period (see below).
- **Review-period demand** = `avg_daily × review_days`, where
  **`review_days = supplier.order_cadence_days` when set** (the
  real reorder cadence), otherwise the legacy ABC-class default.
  This is the single biggest cashflow lever: if you reorder a
  supplier weekly, set their cadence to 7 — each order then
  carries only 7d of next-cycle stock instead of 30-45d. Set in
  Supplier settings → "Order cadence (days)".
- **Holiday cover** = `avg_daily × closure_days_in_window`,
  where `closure_days` is the count of days within the upcoming
  `lead_time + review` window that overlap any of the supplier's
  recorded closure periods (`supplier_holidays` table, edited
  per-supplier in Supplier settings). Multiple closure periods
  per supplier are supported. ISO week numbers ("Wk 32–34") are
  shown for buyer convenience.

Each per-SKU reorder explanation in the Ordering page names the
basis for each term — so when the engine suggests fewer units
than before, the buyer can see exactly why ("you reorder Topmet
every 7d", "Topmet closed Wk 32–34: +14d cover").

#### Slow Stock Cleared / Value (monthly metrics, v2.67.178)
Two new rows in Monthly Metrics → Inventory:

- **Slow Stock Cleared ($)** — sum of `Quantity × AverageCost`
  on sale_lines for SKUs in the current dormancy_warnings set,
  grouped by month. The **flow** metric: how much slow stock the
  team moved each month. Going UP = team is winning. Note: uses
  the current dormancy set (snapshot), not the
  was-dormant-at-that-time set — good enough for trend, slight
  imprecision for very old months.
- **Slow Stock Value (EOM)** — month-end value of slow stock on
  shelf, sourced from `slow_mover_value_snapshots` (engine
  writes daily). Current month uses the live
  `_compute_slow_stock_holding`. The **state** metric: how much
  slow stock is left to clear. Going DOWN = team is winning.
  Sparse for months before v2.67.36 (when the writer was added)
  — that's expected; the row fills out forward.

Both rows tie to the same dormancy set the Slow Movers page
uses, so the figures across pages stay aligned.

#### is_non_master_tube (engine column)
True for SKUs that are children of a bulk-roll master (per-foot
cuts, BOM derivatives). Their stock and demand roll up to the
master. Most calculations filter `~is_non_master_tube` so they
operate on parents + standalones only — avoids double-counting.
You'll see this filter on the Ordering page (parents_only
toggle), the Slow Movers page (auto-applied unless 'show all
flagged' is on), and the Optimum / Excess / Dead-stock math on
the headline tiles.

#### Engine-snapshot writers (provenance, v2.67.36+)
The engine writes two persistence tables during each recompute:
- **`sku_dormancy_log`** — one row per SKU that's ever been
  dormant. Tracks first_seen_dormant_at, last_seen_dormant_at,
  recovered_at, warning_lifted_at, warning_lift_reason. Powers
  the once-slow warning + auto-lift + manual dismiss flow.
- **`slow_mover_value_snapshots`** — one row per
  snapshot_date with (skus_count, units_on_hand, value_on_shelf)
  across the filtered slow-stock universe. Powers the
  "Slow Stock Value (EOM)" Monthly Metrics row and the MoM
  caption on the Overview slow-mover tile.

Both tables are write-through from the engine — the dashboard
runs the engine (typically on Ordering or Overview page load),
the engine writes these tables, and downstream pages read from
them. After the v2.67.163 Postgres cutover both live in the
shared Postgres DB so the worker (Slack bot) and web service
read the same provenance.
"""
