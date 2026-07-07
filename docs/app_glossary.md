# Wired4Signs App Glossary

_Single source of truth for the ABC engine's intelligence rules and the signals the app surfaces. Generated from `intelligence_glossary.py`. To refresh: `python notion_sync.py dump-glossary`._

---
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
air AND the item qualifies. Inventory Planner can still provide an
observed lead-time duration, and a SKU-level **Sku LT** can be set in
Ordering or Product Detail. Ordering displays this as **Vendor LT**
(supplier/freight default), **Sku LT** (manual override), and **Used LT**
(the final engine value after IP and SKU overrides).

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
Direct sales + **assembly consumption** from CIN7 FG-XXXX component
pick-lines + sales rolled up from child variants (MP variants, cuts,
kit components) + sales migrated from retiring SKUs. Used for the
reorder math, NOT the raw "units_12mo" figure.

#### Lineage units / visible 12mo demand
`lineage_units_12mo` is the buyer-visible 12-month demand total from
the same buckets used for the sparkline and "Last 6 months" column.
The Ordering grid labels this as **12mo demand** so the number agrees
with the visible trend.

This is separate from `effective_units_12mo`, which still drives
target stock, suggested reorder, Status, and slow/dead/excess math.
If visible demand exists but effective reorder demand is zero, the SKU
is treated as **🎯 Project**: it moved historically/project-wise, but
the engine is not auto-reordering from that history.

MTD/current-month demand for assembly-heavy components depends on
`assemblies_last_30d_*.csv`, not just `sale_lines_last_30d_*.csv`.
If that assembly file is stale, components such as
`LED-NEON-FLEX-NICHO-3000K-2` can appear under-demanded and falsely
slow.

For "how many sold this month?" questions on components, use direct
invoice movement plus FG assembly consumption. Direct sale-lines alone
are not the total when assembly rows exist.

For exact-SKU month-to-date disputes, CIN7's product **Movements**
ledger is the tie-breaker. Count outbound `Sale` + `Finished Goods` /
`Assembly` rows as demand, show inbound `Advanced Purchase` / `Purchase`
rows separately, and do not net purchases against demand.

The Ordering and Product Detail demand drill-ins use the same basis:
direct component sales + FG component consumption. Kit sale-lines still
show in the activity feed for audit, but they are not added to the
monthly chart when FG rows exist because that would double-count the
same component movement.

#### FixedCost / AverageCost / PO cost
- **FixedCost** — the agreed supplier price on the SKU's supplier record
  in CIN7. What you'll actually pay on the PO.
- **AverageCost** — CIN7's weighted landed cost (drifts with every PO).
- **PO cost** — FixedCost if set, otherwise AverageCost fallback.
  Shown per row with a "Basis" column so you can see which one applied.

#### MOV (minimum order value)
Set per supplier (e.g. Blebox $250). The PO summary flags when the
current draft is below MOV so you can consolidate.

#### SKU buying settings (Sku LT / MOQ / EOQ)
These are per-SKU overrides stored in `sku_pack_settings` and editable
from both Ordering and Product Detail:

- **Vendor LT** — the supplier/freight default lead time before IP and
  SKU-specific overrides.
- **Sku LT** — manual SKU-level lead-time duration in days. Blank/0 means
  use Vendor LT. Existing buyer-entered Sku LT values are never overwritten
  by supplier defaults.
- **Used LT** — the final lead time the reorder engine uses after applying
  IP observed/configured lead time and then Sku LT when present.
- **SKU MOQ** — minimum order quantity for that SKU. It can lift the
  target stock and floor the suggested reorder when a positive reorder
  exists.
- **SKU EOQ / batch qty** — economic/order batch multiple. It rounds
  target stock and suggested reorder up to the next useful batch.

SKU MOQ/EOQ win over supplier MOQ. Project rows are not auto-inflated by
MOQ/EOQ; the buyer can still manually set an order qty for a known
project. Because these settings change target stock, they also affect
optimum stock, excess/slow-stock tied-up value, and reorder suggestions
after the next Ordering/ABC recalculation.

Ordering shows these settings in the main reorder table, optional
pull-forward table, and supplier catalogue table. Main reorder table edits
are committed with **Save edits**. Helper-grid edits in optional
pull-forward or supplier catalogue save immediately to the shared team
database and then recalculate the ordering tables, so buyer-entered SKU
policy values survive refreshes and deploys.

#### Freight mode
Air or Sea. Decision order:

1. **Category rule**: SKUs in `Profiles - Channels`,
   `Accessories - Profiles - Inner profiles`, or `Diffusers` at ~3m
   length ship sea regardless of supplier air-eligibility.
2. **Supplier default**: otherwise air when the supplier offers it and
   the SKU's length fits in the supplier's air cutoff. Sea is fallback.
3. **IP lead time**: when IP has an observed or configured lead time for
   the SKU, that duration wins over supplier default duration.
4. **SKU lead-time override**: when `Sku LT` is set for the SKU,
   that duration wins over Vendor LT. The freight METHOD is still from
   rules 1-2 or the manual override; this is a duration override.
5. **Per-row override**: the buyer can flip any row's mode via the
   Freight column dropdown. This changes the row's supplier/freight
   default shown as Vendor LT; Sku LT still wins if it is filled. The
   reorder qty recalculates with the new lead time on next refresh.

#### Ordering PO editor row focus
The Ordering page's main PO editor, optional pull-forward editor, and
all-supplier-SKUs add picker preserve the saved per-user column layout
and width settings. A
browser-side enhancer supports sideways movement across wide column
sets with horizontal wheel/trackpad input or modified left/right arrow
keys. Buyers can also click-hold inside the grid and drag left/right to
pan horizontally. The clicked-row cue stays visible while moving
sideways and clears when the buyer leaves the grid, clicks elsewhere,
or moves vertically. The cue is positioned against the full editor
frame so frozen/scrolling canvas layers do not make it jump to the
first visible row. This is a UI aid only: it does not change the
underlying reorder calculations, saved layout keys, draft qtys, or
CIN7 write logic.

#### Optional pull-forward
The section below the main PO editor is not a second reorder list. It
only shows SKUs where `Suggested reorder = 0` today, but the engine
expects the item to fall below target inside the selected pull-forward
window. Use it only to consolidate freight or hit MOV. The default
window follows supplier cadence where configured, falling back to a
short 21-day horizon instead of always starting at 45 days. Moving the
slider reruns the table and recomputes the optional qty
(`avg_daily × selected window`).

#### All supplier SKUs add picker
The bottom catalogue picker shows the selected supplier's SKU list with
search across SKU, name, category, status, trend, and ABC. It uses the
same saved column layout and widths as the main PO editor, but replaces
the main editor's action columns with a front **Add to PO** checkbox.
Ticked rows are appended to the main PO editor as extra lines. The
picker does not save or rewrite column preferences; final qty, freight,
notes, dropship/exclude, and SKU buying-policy edits are still made in
the main PO editor or Product Detail.

#### Finishing Work Orders queue
The Buying page **Finishing Work Orders** is driven by CIN7 BOM
structure. A finished SKU appears only when its BOM contains a service
component whose SKU/name looks like powder coating or anodizing, for
example `OSC-POWDERCOAT-BK-LRG-FT`. The page does not infer these
relationships from finished SKU names.

The suggested send quantity comes from the finished SKU's current
replenishment position: engine reorder qty first, otherwise target stock
minus available plus on-order. The page also lists the non-service raw
components in the same BOM, the raw quantity needed, raw available
stock, and a service-SKU summary so buyers can place the outside-service
order and warehouse can complete the CIN7 assembly/removal-assembly
workflow.

#### Status badges
Status is the buyer action label and uses **Available** (OnHand -
Allocated), not just OnHand:

- 🔴 **Reorder now** — Available < 0 (oversold), Available = 0 while
  target/reorder is positive, or Available < lead-time demand.
- 🟠 **Reorder soon** — below target but not urgent.
- 🔵 **Overstocked** — Available > target × 1.5.
- 🟢 **On target** — no buying action needed.
- 💀 **Dead stock** — no visible/effective 12mo demand and stock held.
- ⚪ **No demand, no stock** — no visible/effective 12mo demand and no
  stock.
- 📦 **Dropship** — order-on-demand, we don't stock it.
- Active, Deprecated, Discontinued — from CIN7's product status.

#### Sidebar build chip
The small footer in the dashboard sidebar shows the currently running
web build. It is no longer manually bumped per release. On Render,
`start.sh` exports `APP_BUILD_COMMIT` from the deployed Git commit and
`APP_BUILD_DATE` from the service start date; `app_config.py` turns
those into the displayed label, e.g. `build 84d9db4 · deployed
2026-06-17`. If Render/Git metadata is unavailable, the app falls back
to the old static version string.

#### Trend signal (📈 / 🎯 / 🔀 / 📉)
A secondary check the engine runs to detect when the last-45-day sales
pattern has diverged from the prior 45 days (days 45-90 ago). Uses
four signals combined to avoid false-positives:

- **📈 Trend** — 45d momentum >1.5 plus broad customer spread, OR a
  sustained lift in the 12-month sparkline where the latest 3 calendar
  buckets materially exceed the previous 3 with enough customers. Real
  broad-based demand; engine switches to recent velocity to keep up.
- **🎯 Project** — concentrated / one-off demand: a 45d spike with too
  few real buyers, visible historical lineage demand that should not
  drive auto-reorder, or a full-year pattern where only one or two
  customers account for the demand and recent activity is low. Engine
  subtracts the top customer's 12mo contribution before forecasting to
  avoid over-ordering.
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
skip classification unless there are 10+ distinct recent customers.
For bulk rolls and cut families, customer spread can be a stronger
signal than fractional roll-equivalent units.

The Trend column is recomputed after migration, BOM, strip-family, and
customer rollups. The final rolled values are the authority, so the
grid should never show rolled customer counts beside a stale direct-only
Trend label.

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

#### Last 6 months column
The Ordering page's **Last 6 months** column uses real calendar-month
buckets, oldest on the left and the current calendar month on the right.
It is based on synced CIN7 `sale_lines` by `InvoiceDate`, with credited,
voided, and cancelled lines excluded. The Inspect panel's **SKU sales
audit** shows the same calendar-month invoice totals beside `OrderDate`
totals, so buyers can spot open/current orders that are not yet counted
as invoiced demand.

#### Last 12 months column
The Ordering page and Product Detail also show **Last 12 months** from
the exact same monthly buckets as Last 6 months and the 12mo sparkline.
It is there so buyers can see the full-year shape while preserving the
existing saved column layouts.

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
  the buyer can dismiss manually from the Slow Movers page. The ❗
  prefix is treated as a warning badge only: Status filters still use
  the underlying base status such as 🔴 Reorder now.
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
Warehouse shelf location for each SKU. Source is CIN7's **Stock
locator** field only; never use Default location / warehouse Location
as the shelf code. Surfaced through stock-position answers and PO
commentary lines when known. Answers "where do we keep X?".

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
  Required-By, Comments + Shipping notes, plus per-line stock locator
  and storage dimension fields. Includes received / closed POs
  (unlike `get_incoming_stock` which is open-only).
- **`get_purchase_live(po_number=... / purchase_id=...)`** — live
  CIN7 fallback for fresh or draft POs that are not in the CSV sync yet.
  It returns the same per-line stock locator and storage dimension
  fields as the cached PO lookup.
  PurchaseAdvanced UI links carry the CIN7 UUID after `PurchaseAdvanced#`;
  those must be fetched through `/advanced-purchase` first, with legacy
  `/purchase` only as a fallback. If the live API still cannot see the PO,
  the bot should ask the user to save/refresh/retry the CIN7 PO link, not
  ask them to paste SKU lines as the normal workflow.
- **`get_sale_order(order_number / invoice_number / customer +
  date_from)`** — full sale lookup. Returns customer, every line
  item, line_total. Useful for "what did Acme buy on SO-12345"
  or "who ordered LED-V3060001-2 last week".
- **`get_stock_adjustment(stocktake_number / date_from)`** —
  adjustment header lookup. Returns EffectiveDate / Status /
  Reference. Per-SKU line detail is NOT in the local sync;
  the AI tells the user to view the line breakdown in CIN7.

#### PO dispatch reminders
When a received PO has SO references in its comments, the worker posts a
fulfillment reminder so customer backorders are picked first. Escalations
are **line-level**: the worker checks whether each specific SO/SKU from
the PO still needs dispatch. If an SO remains open only because it owes a
different item, but the PO-linked SKU has already shipped/invoiced, the PO
is stamped as handled and no "STILL hasn't shipped" alert is sent for that
line. If a referenced SO is older than the local sale-line CSV window, the
worker uses the sales-header SaleID to live-fetch CIN7 sale lines before
posting, so reminders name confirmed SO/SKU matches instead of telling the
warehouse to pick unconfirmed orders.

#### PO receipt wording
For PO commentary, `Available`, `OnHand`, `Allocated`, and `OnOrder` are
global stock-position fields across all sales and POs. They must not be
used as proof that a specific PO line was received. Only the PO receipt
fields (`quantity_received_on_po`, `quantity_outstanding_on_po`,
`receipt_status_on_po`) describe what CIN7 has recorded against that PO's
StockReceived tasks. If receipt status is `not_recorded`, say CIN7 has no
StockReceived lines visible for that PO and treat global Available only as
shortage context.

#### PO line stock locators
PO commentary should append the CIN7 Stock locator to each line when the
tool returns `stock_locator` (for example `📍 D29B`). If the locator is
blank/null, omit it. Do not fill the gap with Default location, warehouse
Location, or any other non-shelf field.

#### Local sync windows (v2.67.51)
The AI's transaction tools read from local CSVs the daily sync
drops:
- **Purchase lines** — 30-day rolling window (bumped from 7d in
  v2.67.51 after PO-7109 was missed). The widest available window
  file is used as the base, with newer 1-day files merged on top.
- **Sale lines** — 30-day rolling window (bumped from 3d in
  v2.67.43). Plus the 1825-day longest-history file when present.
- **Slack worker sale headers** — 365-day rolling window. Backorder
  SOs can be more than a month old, so the worker keeps a wider
  header index to resolve `SO-xxxxx` to a CIN7 SaleID, then uses live
  CIN7 lookup for line detail when the local sale-line window misses.
- **Stock adjustments** — 30-day window, headers only (no per-SKU
  line detail in the bulk endpoint).
- **Stock-on-hand `OnOrder` field** — the canonical PO total,
  refreshed every NearSync (15-min). When `get_incoming_stock`
  returns no PO lines but `OnOrder>0`, the tool flags this as a
  data gap rather than claiming "no PO exists".

#### Dashboard memory posture
The dashboard runs on Render with a hard memory ceiling, so large
historical CSVs must be loaded leanly. The app uses one merged
longest-window sale-lines DataFrame as the source of truth and no
longer pre-loads separate 3-day / 30-day sale-line fallback frames
beside it. The big merged sale, sales-header, and purchase-line
loaders read only the columns consumed by the dashboard and AI tools,
then downcast numeric columns where safe. This keeps cold ABC-engine
rebuilds and normal Streamlit reruns from holding multiple full-width
copies of the same CIN7 exports in memory. If Render still reports
OOM, inspect live memory metrics before increasing the instance size
or moving more computations into the background worker.

Large Streamlit caches must be bounded. Snapshot-keyed CSV readers use
small `max_entries` limits, and the biggest merged source/ABC caches
keep one current entry. NearSync creates new CSV filenames and mtimes
throughout the day; without those bounds, the web process can keep old
snapshots resident until Render kills the service.

Ordering-page reorder calculations are also split into two layers:
numeric fields (`target_stock`, `reorder_qty`, `lead_time_days`,
`excess_units`, etc.) are computed table-wide, but the long markdown
`calc_trace` is built lazily only for the SKU currently being inspected.
Do not store `calc_trace` on `engine_df`; thousands of per-SKU markdown
strings can push the Render web instance over memory.

Ordering also has a supplier snapshot serving cache. After
`warm_engine.py` writes `engine_output.csv`, it materializes one JSON row
per orderable supplier/SKU into `ordering_engine_snapshots` and
`ordering_supplier_rows`. The Ordering page can use that selected
supplier slice to avoid reshaping the full engine dataframe on every
widget rerun. It is not a calculation source: if the snapshot mtime does
not match the current `engine_output.csv`, or the DB read fails, the
page falls back to the normal engine dataframe.

Optional Ordering tools are lazy by design. Manual extra-line entry,
optional pull-forward, all-supplier catalogue search, sales-history
migration tools, and the per-SKU calculation inspector are hidden behind
toggles. This keeps the buyer screen quiet and prevents Streamlit from
building secondary data editors/charts on every normal PO edit.

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

#### Bulk-roll residue floor
For bulk-roll masters (50m/100m rolls), any stock, target, or PO
position below **5m worth of roll** is treated as operationally zero
for reorder, excess, out-of-stock, and Status calculations. Example:
0.0025 of a 100m roll is only 0.25m left. CIN7 may carry that decimal
because cuts/assemblies consumed a roll fraction, but the Ordering
page must not call it "Overstocked" or show dollars tied up. The Inspect
panel prints a note when it ignores this residue. Meaningful remnants
above 5m still count and can surface as cleanup stock.

For Neonica 100m master rolls, the Order qty is allowed to be a
decimal of the roll: 40m required becomes `0.40`, not a full `1.00`
roll. The engine skips MOQ/full-roll rounding on those fractional
bulk rows.

Project/manual rows also skip supplier MOQ auto-rounding. If the math
says a few-buyer Project needs 1-2 units, the engine must not inflate
that to a 10-unit MOQ automatically; the buyer can manually override
when a live project really needs more.

#### LED strip family rollup
LED strip cut variants roll up to the active buying-roll master by
shared SKU base. Known strip prefixes, including `LED-TSB`, are
recognised by SKU, so `LED-TSB2835-300-24-6000-0305` contributes demand
to `LED-TSB2835-300-24-6000-100M` without depending on the product name
containing the word "strip". If a larger historical family member is
discontinued/inactive, the app plans onto the largest active buying roll
instead. The name-based fallback is intentionally limited to real bulk
buying rolls (`25m+`, such as 25m/50m/100m). It must not turn a short
finished length such as 1m, 2m, or 2.35m into a parent simply because it
is the longest active SKU in a naming family. Direct PO history alone
does not create an alternate master; CIN7 BOM/sourcing structure is the
source of truth. If the engine still suggests zero after the rollup,
check concentration/project logic: one-customer demand may be shown for
manual review rather than converted into an automatic buy.

The Ordering Inspect panel includes a **Strip family movement audit**
for these rows. It reads the synced CIN7 `sale_lines`, excludes credited
/ voided / cancelled lines, and shows direct master sales plus child/cut
sales normalised into master-roll equivalents. For a 100m roll, 40m of
family movement appears as `0.40` roll equivalent. The audit also shows
the top customer's share so buyers can tell whether a zero reorder is
coming from missing movement or from project/concentration logic.

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

**Excluded customer sales.** Altar'd State sales are intentionally
removed from Wired4Signs operational analytics because they belong to
the separated manufacturing business, not Shopify / LED channel demand.
This exclusion is applied to CIN7 sale headers and sale lines before
ABC/reorder demand, dashboards, Monthly Metrics, customer metrics,
slow-stock cleared revenue, and AI sales tools aggregate anything.
Treat apostrophe variants (`Altar’d State`, `Altar'd State`, `ALTARD
STATE`) as the same excluded customer. Do not describe these rows as
missing or stale; they are deliberately out of scope.

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
  `_compute_slow_stock_holding`. That live holding value excludes
  stale active dormancy warnings when the current engine row shows
  positive 45d/90d movement. The **state** metric: how much slow
  stock is left to clear. Going DOWN = team is winning.
  Sparse for months before v2.67.36 (when the writer was added)
  — that's expected; the row fills out forward.

Both rows tie to the same dormancy set the Slow Movers page
uses, so the figures across pages stay aligned.

#### Staff sign-in
The app uses one combined sign-in screen in production: staff choose
their profile and enter the shared team password in the same form.
Successful sign-in creates a server-side `sid` session token and puts
it in the URL, so Render restarts can restore both the password gate
and the selected staff profile without asking the user to sign in
twice.

#### Cashflow actual revenue
The Overview and Cashflow pages use CIN7 sales headers as the
source of truth for weekly actual revenue. A sale counts in the
Monday-Sunday week of its `InvoiceDate`, using CIN7's Revenue
basis (`InvoiceAmount - Tax` where available), and VOIDED /
CREDITED / CANCELLED sales are excluded. This is intended to
match CIN7's General Dashboard `Revenue` tile for the same date
range.

In Cashflow, `Forecast sales` remains the editable planning row.
`Actual revenue (CIN7)` is a read-only comparison row that mirrors
the old Google cashflow sheet's `Actual` row. The "Use actual
sales" action can copy those CIN7 actuals into `Forecast sales`
for the previous/current week (or all shown weeks), stamped as
`auto:actual_sales`; manual forecast edits are preserved unless
the user explicitly chooses to overwrite them.

For Overview's month-to-date prior-year comparison, header revenue
is still preferred. If a historical period has sale-line orders but
sparse/missing matching sales headers, the app falls back for that
period only to sale-line `Total` so older years do not show tiny
missing revenue values.

#### Cashflow supplier payables
Supplier payables mirror QuickBooks Online Bills plus any manually
added non-QBO invoices. For QBO-sourced rows, QBO's open balance is
authoritative: bills with `qbo_balance <= 0` are treated as paid
even if the local workflow status was still `pending`.

The QBO sync imports recent bill detail and the full QBO open-bills
list. If an old local QBO mirror is no longer present in QBO's open
list, the sync marks it `paid` with `qbo_balance = 0`, so months-old
settled invoices no longer appear as overdue, due in 30 days, weekly
supplier payables, or daily cashflow calendar items. Manual rows are
controlled by the local `status` field.

The QBO bill sync runs automatically from `nearsync_loop.sh` every
`QBO_CASHFLOW_INTERVAL_HOURS` hours (default `4`) after a
`QBO_CASHFLOW_BOOT_DELAY_MIN` deploy delay (default `30` minutes).
It can still be run immediately from Cashflow via **Sync from
QuickBooks**.

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

ABC cache warming is opportunistic. `sync_loop.sh` starts
`warm_engine.py` in the background after syncs, using the same
`engine_refresh.lock` / `engine_refresh_status.json` files the app
shows in the sidebar. Deploy catch-up warms are delayed by
`WARM_ENGINE_BOOT_DELAY_MIN` (default 30 minutes), and the warmer
skips if available memory is below `WARM_ENGINE_MIN_AVAILABLE_MB`
(default 2500 MB on the shared 4 GB Render web instance).
