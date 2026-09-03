# CIN7 Analytics App — Rules & Decisions

**Purpose.** This file is the single source of truth for the business logic and design decisions baked into the app. Every rule below came from an explicit correction or choice made during development. Read this before changing any calculation — violating a rule will produce numbers the business owner has already rejected as wrong.

**Audience.** You (future you), any teammate who picks this up, an LLM summarising this app, or a consultant hired to extend it.

**Versioning.** When you add or change a rule, bump the top-of-file date and mark which page / function it affects. When a rule becomes obsolete, strike it through — don't delete — so the reasoning stays visible.

Last updated: 2026-09-03

---

## 1. Money & Cost Rules

**1.1 Inventory is valued on FIFO, not Average Cost.** The business works on FIFO. The only correct source for current inventory value is CIN7's `StockOnHand` field on the `productavailability` endpoint (synced into `stock_on_hand_*.csv`). `StockOnHand` is a dollar value per stock row — not a quantity — and represents CIN7's FIFO valuation. Never compute `OnHand × AverageCost` as the primary value for current inventory; that's an average-cost number that drifts with every PO and doesn't match CIN7's reports.
- *Fallback*: if `StockOnHand` is missing or 0 on a given row (rare; usually only on Service / Non-Inventory items), fall back to `OnHand × AverageCost`. Mark this fallback in code with a comment.
- *Per-unit FIFO cost* (when needed) = `StockOnHand / OnHand` for rows where `OnHand > 0`.
- *Applied at*: Overview stock card, Ordering page metrics, Product Master table, Product Detail cash-tied-up, Monthly Metrics inventory value.

**1.2 PO line value uses CIN7's FixedCost, not AverageCost.** `FixedCost` is the supplier's agreed price per unit (from the SKU's Suppliers record). `AverageCost` is landed cost and drifts. On the Ordering page, the PO editor uses `POCost` which = FixedCost first, AverageCost only as a silent fallback (flagged in the `POCostBasis` column so the buyer can spot-fix CIN7).

**1.3 Valuation vs. purchase cost mismatch is real.** CIN7's `AverageCost` on sold lines includes landed costs (freight, duties, customs) — but purchase `Total` is the ex-freight supplier invoice amount. Landed costs flow in via stock adjustments over time. This mismatch means any "walk-back" from current inventory using `COGS − Purchases` will drift by the uncaptured landed-cost delta. The Monthly Metrics page normalises this by capping historical values to ±15% of the current FIFO snapshot. The permanent fix is daily `inventory_value_history.csv` snapshots (not yet implemented).

---

## 2. Master / Child / Phantom Stock Rules

**2.1 A SKU is a "master" if any of these are true:**
- CIN7 has a Supplier assigned to it (we actually buy it).
- Its sourcing rule (`AdditionalAttribute1`) says "Purchased full length" or similar purchase-based phrasing.
- It has no BOM flag and no other evidence of being assembled.

Anything else is a child / phantom / cut / assembly.

**2.2 Only masters carry real stock value.** Non-master cuts / phantoms / assembled-on-demand variants do NOT carry their own physical stock in the business sense; their "stock" is just a projection of the master's stock cut up. When displaying capital figures:
- Current stock value: sum across all SKUs (matches what CIN7 StockOnHand returns — it distributes FIFO value across rows, so the total is right).
- Optimum / target stock value: filter to masters only. Summing target × cost across children would double-count.

**2.3 Sales demand must roll up from child to master.** A sale of a child SKU consumes master stock. The ABC engine computes `effective_units_12mo` as `direct + migrated_in + tube_rollup_in + kit_rollup_in`. That effective number — not the raw direct sales — is what drives Status (Dead Stock, Slow Mover, etc.), ABC classification, and reorder targets.

**2.3.1 Visible demand vs reorder demand.** `lineage_units_12mo` / `display_units_12mo` are buyer-visible history fields from the same monthly buckets shown in the Ordering trend columns. They explain what moved historically. `effective_units_12mo` remains the reorder-math field. If visible demand is >0 but effective demand is 0, label the row 🎯 Project/manual history, not Stable, and do not auto-reorder from that history.

**2.4 Rollup methods, in priority order:**
- **Method A — BOM components.** If a non-master SKU is an assembly (has components in CIN7's BOM table), distribute its sales to EACH component × BOM quantity. A single kit with three components rolls to all three, not just the first.
- **Method B — Tube master lookup.** For LED tube family SKUs, find the tube of the same family + color + length that's marked master.
- **Method C — SKU substitution.** Fallback when sourcing rule names a master SKU.
- **Method D — Family-prefix sibling.** If all else fails, find a master SKU sharing the same family prefix (e.g. `LED-01.018-*`) and use it.

**2.5 LED strip rollup — convert metres to active buying-roll units.** Strip cuts sold in metres must be converted: `consumption_master_rolls = consumption_metres / active_buying_roll_length`. Not `×100`. The earlier bug inflated target stock by 100×. If a larger historical family member is discontinued, retired, or inactive, it must not steal demand from the current active buying roll; e.g. a discontinued 50m roll should not stop a live 25m roll from receiving 5m/per-foot family demand.

**2.6 Multi-component kit rollup.** Kit sales (LEDKIT-*, LEDFIX-*) distribute demand to EVERY component in the BOM proportionally — not just the first component. Each component separately gets `kit_sales × its_BOM_quantity`.

**2.7 PO history is not a master rule.** Direct purchase history alone does not make an intermediate strip roll an alternate master. CIN7 BOM/sourcing structure is the source of truth. PO history can contain historical, emergency, or discontinued buys, so letting it block rollup can make best sellers look dormant in reorder math or Slack PO commentary. Demand drill-ins, 45d/90d columns, customer counts, Trend/Status, reorder math, slow stock, and bot answers must use the same child-to-active-master rollup.

**2.8 Exact purchase-pack SKU rollup.** A final `-X<number>` suffix means
the SKU is a supplier buying pack only when the unsuffixed base SKU also
exists, the base SKU is not supplier-assigned, and there is one clear
pack candidate. Example: `SNFX-L-CR-SCKT` sales/FG consumption feed
`SNFX-L-CR-SCKT-X100` as `base units ÷ 100`.

- Count base direct sales plus FG assembly consumption in the pack
  rollup. The pack is how that exact base item is replenished.
- Do not apply this when the base SKU is itself a bought/supplier SKU or
  when multiple pack candidates make the mapping ambiguous.
- The rollup affects `effective_units_12mo`, 45d/90d units, visible
  monthly buckets, customer metrics, Status/Trend, suggested reorder,
  optimum stock, and slow/excess stock. The base row becomes non-master
  for buying math so the app does not recommend both the base SKU and
  the pack SKU.

---

## 3. Sales Status & Demand Rules

**3.1 Exclude these sale statuses from demand / velocity calculations:** `VOIDED`, `CREDITED`, `CANCELLED`, `CANCELED`. They represent orders that didn't net out — including them double-counts the original sale.

**3.1.1 When to filter by InvoiceDate, not UpdatedSince.** CIN7's `saleList` API uses the `UpdatedSince` parameter — so the file we sync contains every sale UPDATED (status change, payment, etc.) in the window, not sales CREATED. Any metric that says "last 30 days" / "this month" / "this year" MUST filter client-side on `InvoiceDate` (or `OrderDate`) after loading. Sites that must do this: Overview "Sales invoiced", Overview "Today / MTD / YoY" tiles, FixedCost Audit window, Monthly Metrics (already done). Failure mode: overstated sales (e.g. $870k instead of $489k in a real test against CIN7's own dashboard).

**3.1.2 Matching CIN7's "Revenue" on the Overview dashboard.** CIN7's dashboard Revenue is **pre-tax**. Our `sales_headers.InvoiceAmount` includes tax + shipping. To match exactly, subtract the sum of `sale_lines.Tax` for SaleIDs in the same window. We show both numbers in the Overview metric (main figure = invoiced incl. tax; `pre-tax ≈` sub-number = CIN7 Revenue equivalent).

**3.1.3 Excluded sales customers.** Sales for **Altar'd State** are
excluded from Wired4Signs analytics because they belong to the separated
manufacturing side of the business, not Shopify / LED channel demand.
This exclusion applies before aggregation to both sale headers and sale
lines, including ABC/reorder demand, sales dashboards, monthly metrics,
customer metrics, slow-stock cleared revenue, AI sales tools, and Slack
bot sales/demand answers. The source CIN7 CSVs remain untouched; the app
and reporting loaders filter these rows at read time. Match apostrophe
variants (`Altar’d State`, `Altar'd State`, `ALTARD STATE`) as the same
excluded customer.

**3.1.4 QBO-side exclusion netting (Monthly Metrics Sections 6/7/8).**
The 3.1.3 exclusion above only filters CIN7 `sale_lines`/`sales` — it has
no effect on `qbo_monthly_pl.py`'s separate pull of QuickBooks Online
P&L data. That module nets excluded customers back out of the QBO
figures itself: `_find_excluded_customer_ids()` looks up every QBO
Customer record whose name matches an excluded key (catching numbered
duplicate/sub-customer records too), then `sync_exclusions_for_customers()`
issues **one combined** customer-scoped `ProfitAndLoss` report (QBO's
`customer` report filter accepts a comma-separated list of Customer
IDs) and stores the result in `qbo_monthly_pl_exclusions`.
`db.qbo_monthly_pl_summary_by_category()` subtracts those amounts from
the matching `(month, account_number, account_name)` row before
categorising, so Sections 6/7/8 agree with the CIN7-sourced sections.

⚠️ **Real incident (Aug 2026):** before the single-combined-query fix,
this exclusion was synced with one QBO report call **per matched
customer record**, summed client-side. Altar'd State existed in QBO as
two overlapping records ("Altar'd State" + an inactive "Altar'd
State 1"), so the same account/month got netted twice — Product COGS
collapsed from ~$220k/mo to $2.75k with GP% spiking to 99% before it
was traced back to this double-count. `qbo_monthly_pl_summary_by_category`
now also carries a tripwire: if an exclusion ever exceeds ~30% of the
pre-exclusion amount for a given `(month, account)`, it caps the
subtraction and logs a warning instead of silently zeroing out or
sign-flipping the category. Section 7 on the Monthly Metrics page
renders "—" (not a literal $0) for any month with no
`qbo_monthly_pl.py sync` data at all, so a sync gap is visually
distinct from a genuine $0 posted to an account.

The single-combined-query change alone wasn't sufficient the first
time it was tried: switching the write to a joined-label
`customer_name` (e.g. "Altar'd State, Altar'd State 1") without
deleting the two pre-existing per-customer-name rows first would have
had the netting SUM (which is blind to `customer_name`) add all three
rows together — turning the double-count into a triple-count on
deploy. `db.replace_qbo_monthly_pl_exclusions()` fixes this properly:
every sync deletes any existing row at each `(month, account_number,
account_name)` key — regardless of what `customer_name` it was
written under — before inserting the fresh total under one fixed
sentinel name (`qbo_monthly_pl.EXCLUSION_CUSTOMER_LABEL`). This is
self-healing: the first sync after this fix ships cleans up any stale
rows already on disk, with no separate migration step.

⚠️ **Follow-up incident (Aug 2026, same month, different root cause):**
after the double-count fix above shipped and was confirmed correctly
netting `$0` exclusions for Jun/Jul/Aug 2026, Section 7 was STILL
showing the same collapsed COGS ($110,353 / $15,429 / $3,187). Direct,
read-only inspection of QuickBooks' own `JournalEntry` records (via
`qbo_client.query_all()`, bypassing the P&L report and this app's
parsing entirely) found the real cause: **CIN7's own QuickBooks Online
integration** — which posts inventory-relief/COGS journal entries per
order (`DocNumber` prefix `FG-#####`, debiting/crediting "Inventory -
On Hand" and "Cost of Goods Sold") — had silently stopped posting
after 2026-06-17. Nothing in `cin7_sync`'s code was wrong;
`qbo_client.py` has no write path to QuickBooks at all (read-only
`report()`/`query()` only), so there was no sync bug to fix — the
entries genuinely never arrived in QuickBooks. This is an external
integration outage, not a code defect, and the fix is operational
(reconnect CIN7's QuickBooks integration; ask CIN7 support about
backfilling the missed Finished Goods entries).

To stop a future occurrence of this from looking like a silent
dashboard bug again, `db.qbo_monthly_pl_anomaly_months()` flags any
month whose QBO "Total Cost of Goods Sold" falls below 60% of the
median of the trailing clean (non-flagged) months, requiring at least
3 months of clean history before flagging starts, and excluding
flagged months from later baselines so a multi-month gap doesn't
"normalise" against itself. Both `app.py`'s Monthly Metrics page
(Section 7) and `monthly_metrics_report.py`'s Slack-posted PDF render
the same caveat from this same function — keeping the dashboard and
the Slack-facing report in agreement, per the standing rule that the
two must always show the same information.

**3.2 Unfulfilled sales reduce effective position.** Count `BACKORDERED + ORDERED + ORDERING` as unfulfilled units. Subtract from `OnHand + OnOrder − Allocated` before comparing against target to get the real reorder need.

**3.3 Migration: retiring SKU sales roll forward to successor.** Discontinued/phased-out lines (Smokies, Cascade) have their historical demand rolled into the successor (Sierra38, Sierra65) with a configurable share %. UI for managing these lives in the Ordering page's Migrations expander. Store in `sku_migrations` table.

**3.4 Trend vs. project detection.** The engine computes a secondary signal per SKU classifying recent demand patterns using a **45-day window** (shorter than 90 days so spikes get caught before the next PO cycle). The 45d / prior-45d / 90d windows are anchored to the newest sales or assembly date in the current snapshot, capped at today, so a stale last-good ABC snapshot does not turn recent-demand columns into zeroes.

**3.4.1 Assembly-heavy MTD demand.** Components consumed through CIN7 finished-goods assemblies (FG-XXXX tasks) must use `assemblies_last_30d_*.csv` for month-to-date demand. `sale_lines_last_30d_*.csv` alone is insufficient for SKUs that mostly leave stock via kit builds, e.g. `LED-NEON-FLEX-NICHO-3000K-2`. If the 30-day assembly file is stale or missing, the engine can understate current-month demand and falsely mark active components as slow/dormant.

The assembly sync must filter final rows by `finishedGoods` detail `CompletionDate`, not only by `finishedGoodsList.Date`. CIN7's list-level `Date` can be the task/start/list date, while the component consumption belongs to the completion month. AI velocity answers must use direct sale-lines + assembly consumption for MTD component movement.

⚠️ **Real incident (Aug 2026): the worker never had this data at all.** `worker_engine.py` (the Slack bot's engine) already had logic to prefer real per-task assembly consumption over a BOM-ratio proxy, built after Andrew flagged an ~11x-too-low reading for LED-SILICONE-PMT80050. But `slack_loop.sh` never ran `cin7_sync.py assemblies`, and `assemblies_last_*.csv` only ever lands on the dashboard's disk (Render disks aren't shared across services — the worker has none at all). So every BOM-heavy component with little direct `sale_lines` activity silently fell back to the same flawed proxy: LED-MC-20.009-S (a mounting clip, consumed mostly through kit builds) read as "181 effective 12mo" against a real ~180/month (~2,160/yr) rate — the exact same failure mode as PMT80050, just because the underlying real-data dependency was never actually wired up in production. Independently re-fetching a full assemblies history on the worker was rejected: `sync_assemblies()` needs one CIN7 detail API call per task, and a full window can take hours against the shared, rate-limited account — doubling the dashboard's own daily cost for the same data. Fixed by sharing it instead: `cin7_sync.py`'s `sync_assemblies()` now upserts into a shared `assembly_component_consumption` Postgres table alongside its CSV write, and the worker reads from `db.get_assembly_component_consumption()` — falling back to the local CSV glob only if the DB read is empty (e.g. local SQLite dev). See `assembly_component_consumption`'s schema comment in `db.py` for the full writeup.

Demand drill-ins and reorder math must follow the same rule. If FG component consumption exists for the inspected SKU, the demand view should use direct sales of that SKU plus FG component consumption as the ground-truth view. Kit sale-line × BOM-ratio rollup may be shown for audit, but must not be added to the monthly chart, 45d/90d windows, or 12mo `effective_units_12mo` at the same time because it double-counts the same component movement.

For an exact-SKU month-to-date dispute, CIN7's product **Movements** ledger (`/product?Sku=...&IncludeMovements=true`) is the reconciliation source. Count outbound `Sale` + `Finished Goods` / `Assembly` rows as demand, report inbound `Purchase` / `Advanced Purchase` rows separately, and do not net purchases against demand. If cached sale-lines/assemblies disagree with product Movements, the cache/sync is wrong and downstream slow-stock/reorder values must be refreshed from the corrected movement basis.

**Signals computed**:
- `units_45d` / `units_prior_45d` → `momentum` ratio (prior = days 45-90 ago).
- `customers_45d` — distinct customer count in last 45d.
- `top_cust_pct` — share taken by the single biggest 45d buyer.
- `top_2_cust_pct` — share taken by the top-2 combined.
- `non_top_avg_units` — avg units per customer excluding the top buyer.
- `top_cust_pct_12mo` / `top_cust_units_12mo` — full-year customer
  concentration. These must be computed from the whole 12mo window,
  not only from customers active in the last 45 days.

The buyer-facing `trend_flag` / Trend column must always be recomputed
after migration, BOM, strip-family, and customer rollups. The final
rolled values are the authority. A row must never show `customers_45d`
from rolled family demand while keeping an older direct-only Trend label.

**Classification** (tightened April 2026 after real-world feedback — original thresholds were too permissive):
- **📈 Trend** — momentum >1.5 plus broad customer spread, or sustained monthly lift in the existing 12-month sparkline. The 45d version is either **customers_45d ≥ 10** (very broad recent market signal, including fractional bulk-roll demand) OR **customers_45d ≥ 3**, **top_cust_pct < 40%**, and **non_top_avg_units ≥ 2**. The monthly version upgrades Stable to Trend when the most recent 3 calendar buckets materially exceed the previous 3 buckets with at least 3 recent customers. Real broad-based acceleration. Engine overrides `avg_daily` to use last-45d rate.
- **🎯 Project** — when the spike is concentrated to **1-2 distinct customers**, when last-12mo demand is concentrated into only **1-2 customers** with little recent activity, or when visible 12mo lineage demand exists but effective reorder demand is zero. Engine subtracts the top customer's 12mo contribution from effective demand before forecasting where applicable; visible-only project rows stay at zero auto-reorder unless the buyer manually overrides. Project rows do **not** auto-round up to supplier MOQ; the buyer can still override the order qty manually when a known project exists.
- **🔀 Mixed** — spike (momentum >1.5) with 3+ customers involved, but the spread is not broad enough for Trend. Watch signal; no velocity override.
- **📉 Decline** — momentum < 0.5. Manual review.
- **Stable** — everything else.

**Low-volume guard**: SKUs with <3 units in last 45d bypass classification unless there are **10+ distinct recent customers**. For bulk rolls and cut families, units may be fractional roll-equivalents while customer spread is the stronger signal.

**Why the refinement**: original rules allowed top_cust_pct up to 60% and called it a Trend. Real example: 8 customers, 50% to one — the other 7 averaged 1.6 units each. Not a trend, closer to a project. The top-2 share + non-top-avg checks catch this pattern explicitly.

The `calc_trace` transparency panel always shows the full breakdown when the flag is non-Stable: who bought, what %, top-2 %, non-top avg.

---

## 4. Excess / Dead Stock Rules

**4.1 Excess for masters.** `max(0, OnHand − Target)`. Over-target stock = excess.

**4.2 Excess for non-masters.** Only flag as excess if **zero direct sales**. A variant with sales is fulfilling real demand even if it's above "target" — the target doesn't apply to it the same way.

**4.3 Dead stock** = holding stock AND zero visible/effective demand. Must use effective units plus visible lineage/display demand, not direct-only sales — a master tube that only sells via its variants isn't dead. If `lineage_units_12mo` / `display_units_12mo` is >0 while `effective_units_12mo` is zero, report it as historical/project movement excluded from auto-reorder, not as steady demand. Oversold SKUs (`Available < 0`) always show Reorder now before any dead/no-demand label.

**4.4 Stock goal vs reorder level (2026-09-03, `engine/stock_goal.py`).**
The engine's `target_stock` is an *order-up-to reorder level* (the buying
trigger). It is NOT the stock budget: summed over the business it came to
~$165k / 24 days of cover, because anything selling <~10 units/yr rounded to
a target below one unit and the model assumes weekly fractional buys. The
app therefore carries three named figures, all from one function
(`stock_health_summary`) so the Command Centre, Stock Optimisation headline,
per-vendor tiles and daily snapshots agree by construction:

- **Reorder level (order-up-to)** — `target_stock × cost`. Was labelled
  "Optimum stock value"; do not call it Optimum again.
- **Stock goal** — per SKU the LARGEST of class days-of-cover × planning rate
  (`DEFAULT_COVER_DAYS`: A 50, B 75, C 150, D 0), the reorder level, and the
  range floor. Zero for dropship, do-not-reorder, discontinued and non-master
  rows. Planning rate = unadjusted 12mo rate (`avg_daily_base`), clamped to 0
  when nothing moved in 90 days. Valued at the Ordering `EffectiveUnitCost`
  chain (FIFO/unit → AverageCost → family/category median).
- **Excess** = `max(0, OnHand − goal) × cost` (gross); **Understock** =
  `max(0, goal − OnHand) × cost`. Dropship stock and non-master cuts with
  their own direct sales are never excess. Dead stock (D with stock) is
  inside Excess.
- **ABCD** — D = ABC "C" with zero visible 12mo demand. The cumulative-value
  ABC put ~5,300 no-demand SKUs into C and hid the active-C over-cover.
- **Range floor** — a live A/B/C SKU (planning rate > 0) targets at least
  one unit, or one pack/EOQ when set. Exempt: Project rows, bulk-roll
  masters. This DOES lift `target_stock`, so a dormant-but-still-selling
  C SKU at zero stock now suggests a reorder of one unit/pack.

The warm job writes `stock_goal_snapshots` daily (reorder level NULL —
needs supplier config); the Ordering/Stock Optimisation page overwrites the
same day's row with the full figures. Glide path runs to the goal.

---

## 5. Freight & Supplier Rules

**5.1 Air is the default when eligible.** If the supplier offers air (`lead_time_air_days` set) AND the SKU fits (`air_max_length_mm` not exceeded), default to air. Reasoning: shorter lead times = less capital tied up. Sea is the fallback.

**5.2 Freight mode is per-row overridable.** The buyer can switch any row in the PO editor to sea/air via a Selectbox column. Overrides persist in `st.session_state["freight_overrides"]` and recompute reorder qty with the new lead time on next rerun.

**5.3 Supplier configurations known as "air-default":**
- Neonica Polska Sp. z o.o. — 28d air (21d production + 7d transit from Poland)
- ARDITI GmbH (EUR) — 14d air (7d production + 7d transit from Germany)
- Blebox sp. z.o.o. — 14d air
- DIGIMAX SRL (Formerly DALCNET) (EUR) — 14d air
- EnoLED — 7d (US-local, ground or air doesn't matter)

Applied via `configure_air_suppliers.py`.

**5.4 Dropship items — four sources, merged with priority rules.** CIN7 Core has a native `DropShipMode` field on every product (`Always Drop Ship` / `No Drop Ship` / `Optional Drop Ship`) plus a `Tags` field that sometimes contains `Dropship`. Our sync captures both. The Ordering engine computes the effective dropship set as:

```
dropship_skus = CIN7_always_ds
              ∪ CIN7_tag_ds
              ∪ per_sku_app_flag
              ∪ (supplier_dropship_default − CIN7_no_ds)
```

- **CIN7 `Always Drop Ship`** is authoritative — those SKUs are always dropship no matter what.
- **Per-SKU app flag** (set by ticking the `📦 Dropship?` column in the PO editor) wins over everything. Use this for edge cases CIN7 doesn't know about yet.
- **Supplier-level `dropship_default`** (e.g. a new 100%-dropship supplier we haven't CIN7-tagged yet) applies to all that supplier's SKUs EXCEPT those explicitly marked "No Drop Ship" in CIN7 — CIN7's per-item intent wins over the supplier-wide default.
- **Write-back to CIN7**: WIRED (per-row explicit button). When a user unticks a CIN7-sourced dropship, an app-side `"Not dropship"` override is recorded and the row appears in the ⚠ Pending CIN7 dropship writes expander below the PO editor. Clicking "Write to CIN7" fires `PUT /product` updating both `DropShipMode` (to `No Drop Ship`) and `Tags` (removing the `Dropship` tag). Reverse direction also supported: ticking Dropship for a CIN7 "No Drop Ship" SKU queues a write to `Always Drop Ship` + add `Dropship` tag. On success the local override auto-clears. Requires the `.env` API key to have Products-Update permission.

**Engine behaviour for dropship SKUs:**
- `target_stock = 0`, `reorder_qty = 0`, `excess_units = 0`, `excess_value = 0`
- Status shows `📦 Dropship` badge
- Stays visible in the main reorder table (unlike "Do not reorder" which hides) so volume is trackable
- Excluded from Optimum Stock Value
- `📦 Dropship products` expander below the table shows all dropship SKUs with 12mo sales + est. annual spend; "Volume suggests promoting" hint fires at ≥40 units AND ≥$1,500/yr

**Baseline coverage (as of Apr 2026):** 130 SKUs with CIN7 `Always Drop Ship` + 132 with `Dropship` tag — primarily Gyford Décor (108 of 113 items) but also scattered across other suppliers. No supplier-level or per-SKU overrides needed to start — CIN7's data does the work.

**5.4 Minimum Order Value (MOV)** — configured per supplier in `supplier_config`. The PO editor flags when the current draft is below MOV so the buyer can consolidate lines.

**5.5 Optional pull-forward is not a reorder-now signal.** The
secondary supplier table below the main PO editor only shows SKUs where
`Suggested reorder = 0` today, but the item may fall below target
inside the selected pull-forward window. Use it only to hit MOV or
deliberately consolidate freight. The default window follows supplier
cadence where configured, otherwise 21 days. Moving the slider reruns
the table and recomputes the optional qty as
`avg_daily × selected window`; it must not change the main reorder
calculation unless the buyer ticks a row and adds it to the draft PO.

**5.6 Ordering add-to-PO helper grids reuse the saved PO editor
layout.** The main reorder table, optional pull-forward table, and
all-supplier-SKUs picker must use the same buyer-configured column
order and widths. Helper grids may replace the main action columns with
a front `Add to PO` checkbox, but must not overwrite saved column
layouts. Final qty, freight, notes, dropship/exclude, and SKU buying
policy edits happen in the main PO editor or Product Detail.

**5.7 SKU-level buying policy overrides supplier defaults.** The
`sku_pack_settings` table now stores per-SKU `lead_time_days`, `moq`,
and `eoq_qty` (plus legacy `pack_qty`). Product Detail and the Ordering
grid edit the same row, so buyer changes must stay in sync across both
places.

- **Sku Leadtime** is a duration override. Freight method is still chosen
  by category/supplier/manual freight rules, but if a SKU lead time is
  set, that duration overrides IP observed/configured and supplier
  default lead-time days.
- **SKU MOQ** lifts `target_stock` when the computed target is lower, and
  floors suggested reorder quantity when a positive reorder exists. It
  wins over supplier MOQ.
- **SKU EOQ / batch qty** rounds `target_stock` and suggested reorder up
  to a clean economic/order batch multiple. Legacy `pack_qty` is used as
  the batch multiple only when `eoq_qty` is empty.
- **Project rows are not auto-inflated** by MOQ/EOQ. They stay visible
  for buyer review, but a known project must be manually ordered.
- Because target stock changes, optimum stock value, excess/slow-stock
  tied-up value, and reorder suggestions must all reflect these SKU
  settings after the next Ordering/ABC recalculation.
- Ordering and Product Detail must show both `last_6mo_series` and
  `last_12mo_series` from the same monthly demand buckets, so buyers can
  see the full year without opening a CSV.

---

## 6. Customer & Retention Rules

**6.1 New customer** = first-ever purchase falls in the month in question.

**6.2 Lost customer (default definition)** = last purchase was 3 months before the reporting month. Switchable to 6 months if the user prefers a more conservative cut-off.

**6.3 Repeat customer %** = customers active in month M who had a prior purchase before M, as a percentage of total customers in M.

**6.4 Churn rate** = lost / running. Running count is cumulative unique customers through end of the reporting month.

---

## 7. Shipping & Charges Rules

**7.1 Shipping Charged comes from CIN7 invoice/order `AdditionalCharges`.**
- CIN7 Core's `/sale` detail endpoint returns an `AdditionalCharges` array at BOTH `Invoices[].AdditionalCharges` and `Order.AdditionalCharges`. Each entry is `{Description, Quantity, Price, Tax, Total, ...}`. Description usually starts with `"Shipping - "` (e.g. `"Shipping - UPS Ground"`, `"Shipping - Free shipping"`).
- Our extractor emits one synthetic sale_line per AdditionalCharges entry with `Total > 0`, copying Description as the SKU/Name. The invoice-level charges win when present (what was actually invoiced); the Order-level charges are used as a fallback when no invoice has been issued yet.
- Common mistake: earlier versions of our extractor assumed a single `ShippingTotal` field — that doesn't exist. If you see $0 or near-$0 shipping over a long window in Monthly Metrics, it means the sale_lines file was pulled BEFORE the AdditionalCharges fix landed. Re-run `python cin7_sync.py salelines --days 730` (or wait for the weekend sync) to backfill.
- The header-delta method (`Shipping ≈ InvoiceAmount − sum(line totals) − tax`) is still computed as a secondary signal; the synthetic-line method is preferred when both are available.

**7.2 Shipping Cost needs ShipStation.** We don't have carrier costs in CIN7. Placeholder row in the Monthly Metrics table says "— (ShipStation pending)". The ShipStation integration is Phase 2.

**7.3 Discounts sign convention.** In outputs, discounts are shown as negative (money taken off a sale). Internally `sale_lines.Discount` may be positive; the display layer multiplies by −1.

---

## 8. Sync Rules

**8.1 Near-sync MUST include line items.** The 15-minute Task Scheduler `nearsync` pulls: stock snapshot, stock adjustments (1d), stock transfers (1d), sale headers (1d), **sale lines (1d)**, purchase headers (1d), **purchase lines (1d)**. Dropping the line-item syncs made the "Today" tile go stale — confirmed bug, fixed. Keep them in.

**8.2 Nearsync runtime.** ~15-20 API calls at 2.5s rate, well under CIN7's 60/min cap. Rate is configurable via `CIN7_RATE_SECONDS` env var; 1.5s for overnight / deep syncs.

**8.3 Checkpoint files.** Every bulk sync writes a `.checkpoint_*.json` so interrupted runs resume without re-pulling. Never delete checkpoints manually.

**8.4 Weekend deep sync (Friday 6pm, auto-scheduled).** Phases:
1. Backup current output folder
2. Masters (products, customers, suppliers, boms) + 5-year sales/purchases headers
3. 3-year stock movements (adjustments + transfers + movements)
4. 5-year sale lines
5. 5-year purchase lines
6. DuckDB warehouse build (`load_warehouse.py`)
7. Summary to log

**8.5 Windows setup.**
- Laptop plugged in, set "sleep" to Never on AC
- Pause Windows Update on Friday evenings before the weekend sync
- Task Scheduler waking the PC is enabled in the registered task

**8.6 Data loader strategy.** Each loader (`_load_longest_sale_lines`, `_load_longest_purchase_lines`, `_load_longest_sales`) uses the same pattern: pick the largest-window CSV as the base, union any more-recently-written shorter-window files (captures intra-day data), dedupe on natural keys.

**8.7 Streamlit cache memory discipline.** Large CSV, merged-source, and ABC-engine caches must be bounded with `max_entries`. NearSync creates fresh filenames/mtimes throughout the day; unbounded `@st.cache_data` entries can keep old snapshots resident until the Render web process exceeds memory. The biggest merged source loaders and `_abc_engine` keep one current entry, while the generic CSV reader keeps a small rolling set. The background ABC warmer must also respect `WARM_ENGINE_MIN_AVAILABLE_MB` (2500 MB on the shared 4 GB Render web instance) before spawning a second Python process.

**8.8 Ordering supplier snapshots are an acceleration layer, not a
calculation source.** `warm_engine.py` writes `engine_output.csv` first,
then materializes one JSON row per orderable supplier/SKU into
`ordering_engine_snapshots` + `ordering_supplier_rows`. The Ordering
page may read a selected supplier from those tables to avoid reshaping
the full engine dataframe on every buyer action, but only if the
snapshot source mtime matches the current `engine_output.csv`. If the
DB snapshot is missing, stale, empty, or unreadable, the page must fall
back to `engine_output.csv` / `engine_df` without changing numbers.

---

## 9. UI / UX Rules

**9.1 Each user has their own saved layout.** Keyed by the sidebar "Your name" field. Empty name → saves under `default`. Different casing is normalised to lowercase.

**9.2 Required columns cannot be hidden.** `SKU`, `Include?`, `Order qty`, `POCost` are load-bearing for the PO editor. The column organizer shows them with a 🔒 prefix; hiding them silently re-adds them at save time.

**9.3 Column widths are persistent, 5-preset.** `tiny` (60px), `small`, `medium`, `large`, `huge` (400px). `tiny` and `huge` require Streamlit ≥1.40 (integer pixel widths). Fallback: silently maps to `small` / `large` on older Streamlit.

**9.4 User-named presets.** Save the current layout + widths under a custom name (📌 prefix). Appears in the Quick preset dropdown alongside built-in presets. Delete via the "My saved views" expander.

**9.5 Drag-and-drop requires `streamlit-sortables`.** Included in requirements.txt. Import is try/except'd — on failure, falls back to a table-based layout editor. Known caveat: Python caches imports at module load, so installing the package after Streamlit started requires a full Streamlit restart (not a browser refresh).

**9.6 Streamlit magic mode.** Bare expressions at module / page scope get auto-st.write()'d, which will dump a function's docstring into the UI. Always assign or use the result. In particular never write `_sort_items  # noqa` as a bare line.

**9.7 Streamlit emoji rules.**
- `icon=":shortcode:"` on `st.success/error/warning/info` DOES NOT work — Streamlit requires a literal emoji character.
- Inside `st.markdown` body text, only a subset of GitHub-style shortcodes work. Specifically `:white_check_mark:`, `:warning:`, `:x:`, `:gear:`, `:rocket:`, `:floppy_disk:`, `:pushpin:`, `:robot_face:` work. `:large_green_circle:` does NOT — use 🟢 directly.

**9.8 Pandas strict dtype.** Recent pandas versions refuse string assignment into numeric columns. Before overwriting a float64 column with formatted strings (e.g. `"$1,234"`), cast to `object` first.

**9.9 Data freshness indicator.** Sidebar shows 🟢 🟡 🔴 based on age of the latest `stock_on_hand_*.csv` (15-min nearsync heartbeat): green <20min, amber 20-60min, red >60min. Manual `🔄 Refresh data now` button clears Streamlit's 5-min cache.

**9.10 Ordering optional tools stay closed and lazy by default.** The
main PO editor is the primary buyer workspace. Secondary tools — manual
extra line, optional pull-forward, all supplier catalogue, sales-history
migrations, and calculation inspector — must be behind toggles or another
true lazy gate, not merely a closed `st.expander`. Streamlit expanders
still execute their body on every rerun, so heavy helper tables should
not be built until the buyer opens them. Explanatory copy belongs in
help text or a small closed notes expander inside the opened tool.

---

## 10. Data Backup & Recovery Rules

**10.1 team_actions.db stays local.** SQLite + cloud sync (GDrive / Dropbox / OneDrive) = database corruption. Never put this file in a live-synced folder. Use a nightly copy to GDrive backups as safety.

**10.2 Source code lives in Git.** Not GDrive. Private GitHub repo. `.gitignore` excludes `.env`, `.venv/`, `output/*.csv`, `team_actions.db`, `.checkpoints/`.

**10.3 Layout restore.** Every Save in the Column Layout expander writes a full snapshot to `audit_log`. `restore_layout.py` lists those saves and lets the user pick any to restore. Useful when a preset or teammate overwrites a good layout.

**10.4 Shared multi-user access.** Single-source-of-truth: Streamlit runs on James's PC, teammates access via LAN or Tailscale tunnel. Do NOT run two Streamlits against two copies of `team_actions.db` (conflicts + corruption).

---

## 11. Known Gaps / Future Work

**11.1 ShipStation integration.** Shipping Cost row is 0 until this lands. Unlocks true Line Contribution Margin.

**11.2 Assembly event sync.** Task #16 — gives us Assembled Output Quantity and Write-Off Quantity on the Monthly Metrics page.

**11.3 Detailed sale records.** The list endpoint skips per-sale shipping and some charge breakdowns. Full fidelity requires per-sale GETs (~100k API calls for 5yr). Not worth it now; the header-delta method is ~98% accurate.

**11.4 Daily inventory value snapshot.** Once rolling, replaces the walk-back reconstruction with true historical values. Ask when ready to wire.

**11.5 DuckDB warehouse.** Built by the weekend sync; Streamlit app not yet refactored to read from it. Next refactor: make large joins and multi-table queries hit DuckDB for sub-second performance.

**11.6 Commissions computation.** Waiting on rule definition from the user.

---

## 12. Verification Rules

When in doubt about a number:

- **Today tile disagreement**: check `output/stock_on_hand_*.csv` mtime (should be <15 min), hit 🔄 Refresh data now, cross-check with CIN7's dashboard.
- **Inventory value disagreement**: sum `StockOnHand` column in the latest stock CSV; should match CIN7's Product Availability screen exactly.
- **Historical monthly figures**: sale_lines should match Easy Insight within 1-3% (the 3% is status-filter nuance). GP% matches to ~1-2%. COGS differs because Easy Insight may use a different point-in-time AverageCost.
- **FixedCost Audit**: pick any row flagged "paying MORE", then hit Drill into SKU — the individual PO lines should show you the exact transactions driving the delta.
