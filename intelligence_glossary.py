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
- **Available** — OnHand − Allocated. **Negative Available = oversold**
  (we owe customers more than we have on the shelf). The Status and
  reorder math both work off Available, not OnHand (v2.67.333).

#### OnOrder
Units already placed on open POs (status ORDERED or ORDERING). The
engine subtracts these from what you need to reorder — you won't get
a suggestion to buy something that's already on its way.

#### Backorder
The qty owed to customers we can't ship — equal to `max(0, Allocated −
OnHand)`. v2.67.329 removed it as a dedicated column because it's
mathematically just the negative side of Available (Available = −12 is
already saying "oversold by 12"). The engine still tracks it
internally; it just doesn't display as its own column to avoid
duplication with Available.

#### DoC (days of cover)
**`OnHand / avg_daily`** — how many days the current stock will last
at the 12-month average sales rate.

#### Effective units (12mo)
Direct sales of THIS SKU + **assembly consumption** (the SKU consumed
as a component in kit-builds, from CIN7's FG-XXXX tasks — see Assembly
consumption below) + sales rolled up from child variants (MP variants,
cuts) + sales migrated from retiring SKUs. Used for the reorder math,
NOT the raw "units_12mo" figure on its own.

#### Assembly consumption (FG-XXXX tasks) — v2.67.334-339
Many components — LED strips, profile parts, mounting clips — sell
mostly via kits, not as standalone items. When a kit ships, CIN7 fires
an **Assembly task** (FG-XXXX) that decrements each component listed
on the kit's pick list. The engine pulls those tasks
(/finishedGoods/pick) and folds the per-component consumption into the
demand math:

- Adds to **units_12mo / units_45d / units_90d / monthly buckets** so
  the SKU's velocity reflects ALL the ways it left the shelf — not
  just direct invoices.
- Suppresses the BOM-rollup-from-kit-sales path for those components
  (assembly consumption IS the kit-sale-times-ratio, just observed
  rather than derived — counting both would double).
- Surfaced in the Inspect-a-SKU panel under "🏗️ Assembly consumption"
  with every FG-XXXX task that pulled the SKU.
- Shown as a dedicated line in the calc trace: "Assembly consumption
  (FG- tasks): +N units (ground truth — kits built using this part)".

A component with active assembly consumption is therefore **not
slow / not dormant** even if direct invoices are sparse — the engine
sees the real demand, dormancy / Status / reorder all use the
augmented figure.

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
Air or Sea. Decision order:

1. **Category rule** (v2.67.340-341): SKUs in `Profiles - Channels`,
   `Accessories - Profiles - Inner profiles`, or `Diffusers` at **~3m
   length** (2950-3050mm) ship **sea** regardless of supplier
   air-eligibility. Long awkward items aren't economical on air.
2. **Supplier default**: otherwise air when the supplier offers it
   AND the SKU's length fits the supplier's `air_max_length_mm` (e.g.
   Topmet UPS caps at 2200mm). Sea is the fallback.
3. **IP lead time** (v2.67.343): when IP has an observed or configured
   lead time for the SKU, that DURATION wins (it's the real measured
   PO-to-receipt time). The freight METHOD (air/sea) is still set by
   rules 1-2 above.
4. **Per-row override**: the buyer can flip any row's mode via the
   Freight column dropdown ("air"/"sea"/"air (manual)"/"sea
   (manual)"). The reorder qty recalculates with the new lead time on
   next refresh.

The Inspect panel's calc trace shows the reason next to the lead-time
line, e.g. `Lead time: 35 days (sea (category rule: Profiles -
Channels at ~3m → sea))`.

#### Status badges (Ordering page)
Computed in `_status()` using **Available** (not OnHand) so a SKU that's
oversold (Allocated > OnHand) reads as urgent, not as Overstocked
(v2.67.333). Ladder:

- 🔴 **Reorder now** — Available ≤ 0 (oversold or no free stock), OR
  Available < lead-time demand. The engine wants stock immediately.
- 🟠 **Reorder soon** — engine's `reorder_qty > 0` AND Available <
  target, OR Available < target without an urgent shortfall.
- 🔵 **Overstocked** — Available > target × 1.5. **Uses Available, not
  OnHand** — a SKU with 100 on hand but 90 committed isn't overstocked.
- 🟢 **On target** — everything else (well-stocked, no reorder needed).
- 💀 **Dead stock** — eff_units_12mo = 0 AND OnHand > 0. Sitting
  inventory with no demand.
- ⚪ **No demand, no stock** — eff_units_12mo = 0 AND OnHand = 0.
- ❗ prefix — "once-slow" warning on a SKU that's currently recovering
  (the engine saw it as dormant in the past). Auto-lifts after 90 days
  of sustained recovery.

Other Status sources:
- 📦 **Dropship** — order-on-demand, we don't stock it.
- Active / Deprecated / Discontinued — from CIN7's product status.

#### Trend signal (📈 / 🎯 / 🔀 / 📉)
A secondary check the engine runs to detect when the last-45-day sales
pattern has diverged from the prior 45 days (days 45-90 ago). Uses
four signals combined to avoid false-positives:

**Customer diversity is the deciding signal** (v2.67.325). The old
top-share thresholds (top ≥ 50%, top-2 ≥ 70%) misfired on bulk rolls
where order sizes are naturally uneven — one buyer doing a big install
takes >50% of UNITS while 4 other customers buy fractions, and that's
diversified demand, not a project.

Rules now (when momentum > 1.5, i.e. a spike is present):

- **🎯 Project** — **only when ≤ 2 distinct customers** in the 45-day
  window. Genuine one-off concentration. Engine subtracts the top
  customer's 12mo contribution before forecasting to avoid over-
  ordering future stock against a one-time order.
- **📈 Trend** — **3+ distinct customers** AND top customer < 40% AND
  non-top customers averaging ≥ 2 units each. Real broad-based
  acceleration; engine switches to last-45d velocity to keep up.
- **🔀 Mixed** — **3+ distinct customers** but the spread isn't broad
  enough for Trend (one or two buyers leading but multiple
  participants). Engine uses normal 12mo velocity (NOT suppressed) —
  this is real demand, just uneven order sizes.
- **📉 Decline** — units down 50%+ vs prior 45 days. Worth review.
- **Stable** — momentum ≤ 1.5 or insufficient signal — no spike to
  decompose.

The promotion path (`_promote_dormant_flag`) uses the SAME diversity
rule: a 12mo customer count ≥ 3 means a SKU is never promoted to
Project regardless of low volume.

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

#### Reorder engine: cadence + holiday cover + IP lead times (v2.67.283-285)
The reorder engine's target stock is built from four components,
in this order:

`target = lead_time_demand + safety_stock + review_period_demand
         + holiday_cover`

- **Lead-time demand** = `avg_daily × lead_time_days`. The stock
  needed to cover demand while the next order is in transit.
  `lead_time_days` priority (v2.67.285):
    1. **IP observed actual** — `ip_lead_times.observed_lead_time_days`
       (IP's `avg_lead_time`, the real measured PO-to-receipt
       time). Sane-clamped to 3-120 days. Refreshed weekly by
       `ip_lead_times.py sync`. This is the canonical lead time.
    2. **IP configured** — `ip_lead_times.configured_lead_time_days`
       (IP's lead_time setting). Used if no observed value.
    3. **Supplier config** — `lead_time_air_days` if SKU air-
       eligible (within `air_max_length_mm`), else
       `lead_time_sea_days`. Fallback for SKUs not in IP.
    4. **Hard default** — 35 days. Only reached if nothing else.
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

#### Monthly Metrics — formulas & commission caveats (v2.67.290)
The Monthly Metrics page is the commission base, so every metric
needs to be auditable. The full methodology lives in an in-app
expander on that page; the headline definitions:

- **Sales $** = `sum(product line Total)` ex-tax, ex-shipping,
  excluding voided / credited / cancelled SaleIDs.
- **COGS** = `sum(Quantity × AverageCost)` on product lines.
  ⚠️ CIN7's `AverageCost` re-costs as later receipts change
  the moving average — historical months can drift over time.
  For commissions, run them promptly after month-close, or
  snapshot the figure at month-close (snapshot table is a
  pending project).
- **Gross Profit** = Sales − COGS. **GP %** = GP / Sales.
- **Average Order Value** = Sales $ / # of Monthly Orders.
  Numerator is product sales only; denominator counts distinct
  SaleIDs (excluding voided/credited). Higher than Easy
  Insight's AOV because we drop voided/credited.
- **Shipping Charged** = header-delta `InvoiceAmount − lines −
  Tax`, clipped ≥0. Captures parcel + LTL + handling +
  surcharges.
- **Shipping Cost (ShipStation parcel only)** = ShipStation
  `ShipmentCost` sum by month. ⚠️ **Parcel only** — LTL is
  not captured. So `Shipping Charged − Shipping Cost` over-
  states shipping margin for any month with LTL. Do not
  commission on shipping margin until reconciled.
- **Cumulative Customers (ever bought)** = monotonic count of
  customers whose first purchase is ≤ month m. Renamed from
  "Running Customer Count" (v2.67.290) because the previous
  label implied active. Easy Insight's similar-looking metric
  was "active in last N months" — that's a different scope.
- **Repeat Customer %** = of distinct customers buying in
  month m, share who had any prior-month purchase.
- **Average Inventory Value** = ⚠️ **modelled estimate**, not
  a measurement. The page walks COGS / purchases back from
  today's snapshot, with damping (raw walk-back drifts due to
  landed-cost mismatch between sale AverageCost and PO Total).
  For audit-grade history, we need month-end snapshots.
- **Stock Turn Rate (annualised)** = (COGS × 12) / Avg Inv.

For commissions: GP $ and GP % are canonical; the shipping
margin row and historical-COGS drift are the two unreconciled
items. Both are surfaced as warnings on the page.

#### Monthly Metrics — QB-canonical rows (v2.67.292)
QuickBooks Online is the reconciled financial source of truth.
The Viktor cross-system audit (May 2026) found CIN7-derived
figures drift materially from QB actuals — shipping charged
27-218% over QB every month, historical COGS up to 27% over,
Dec 2025 sales gap of -$45k (a journal entry CIN7 didn't surface).

The Monthly Metrics page now pulls QB Profit & Loss by month
(via `qbo_monthly_pl.py`) and shows QB-canonical rows alongside
the CIN7-derived ones:

- **QB Sales $** ← account `400` Sales
- **QB COGS** ← account `500` Cost of Goods Sold
- **QB Gross Profit / GP %** ← derived from above
- **QB Shipping Charged** ← account `405` Sales - Shipping
- **QB Shipping Cost** ← account `694` Shipping-Out
- **QB Shipping Margin** ← 405 − 694 (symmetric, no LTL gap)
- **Sales variance (CIN7 − QB)** — should trend to zero as
  reconciliation completes

The mapping lives in `qbo_account_mappings` (editable) so other
companies with different chart-of-accounts numbers can adapt
without code changes. The sync runs daily from `slack_loop.sh`.

For commissions: **the QB rows are the canonical figures.** The
CIN7-derived rows are the live operational view, useful for
spotting variance.

#### Monthly Metrics — canonical source-of-truth definitions (v2.67.301)
Following the May 2026 cross-system audit (App vs QB vs DEAR vs
Shopify vs ShipStation), the *role* of each metric is locked so
future debates don't re-open. The numbers themselves are
unchanged — this section names which row to use for which
purpose.

**Sales — three canonical roles.** Same data, three uses:
- **Operational Sales** = Section 1 "Sales $ [App]" (CIN7
  sale_lines). For KPIs, commissions, dashboards, MoM trend.
- **Accounting Revenue** = Section 6 "Net Sales (QB 400)"
  (QuickBooks acc 400). For P&L, accountant, tax, banking.
- **Gross Marketplace Revenue** = Section 6 "Gross Sales (est.)"
  (Net Sales + Discounts). For marketing / conversion / discount
  rate tracking.

**Discounts.** Sourced from `shopify_monthly_discounts` table
(populated by `python shopify_discounts.py sync` daily). The
Shopify Admin API's order.total_discounts is the single source
of truth — captures coupons, automatic promotions, compare-at
markdowns, shipping discounts, draft-order adjustments. The
page row auto-falls-back to the CIN7 line-discount proxy when
the Shopify table is empty (pre-sync state), so the row always
has a value. Cancelled Shopify orders are excluded; refunded
orders are kept (discount was applied at sale time).

**GP %.** Two views, both authoritative:
- **Operational GP %** = Section 1 (CIN7 product margin).
- **Accounting GP %** = Section 7 (QB Total Income − Total COGS).
Within 1-3% most months. July 2025 has a 7-pt gap (67% App vs 74%
QB) attributed to month-end COGS posting timing — the month-close
snapshot project will freeze the figure to remove this drift.

**Freight normalisation.** Section 8 (QB Shipping Detail, acc 405
vs acc 694) is canonical and symmetric. Section 2 (Margins &
Purchasing [App]) is operational: CIN7 header-delta charged +
ShipStation parcel cost — asymmetric (charged includes LTL,
cost is parcel-only), so it over-states margin by ~$3-34k/mo.
Mar 2026 had a duplicate UPS bill + double-counted ACH; the
adjusted figure (~$49,851) is used in the page vs Viktor's raw
$78,750.

**Credits / returns / voided sales.** Excluded upstream:
`Status IN ('VOIDED','CREDITED','CANCELLED','CANCELED')` filter
on CIN7 sale_lines before any aggregation. QB acc 400 is already
net of returns. Both sides agree on "booked-and-kept sales only".

**Marketplace fees / inventory adjustments.** QB classifies as
COGS (acc 502 Amazon Fees, acc 550 Inventory Adjustment).
Section 7 Total COGS includes them; Section 1 *COGS (App)* does
not (CIN7's per-line AverageCost only). The ~$5-15k/mo gap is
this design choice — narrow product COGS for buyer reporting,
broad Total COGS for accounting reconciliation.

**Confidence levels per the audit** (subject to the snapshot +
Shopify-discounts projects):
Revenue 90% · GP% 92% · Inventory 95% · Orders 95% ·
Freight 80% · Discounts 70% · Operating Profit 85%.

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
