# Roadmap — what's planned and what's done

This is the canonical backlog. Edit it directly any time — it's
indexed by the AI Assistant's knowledge base and read by future
Claude sessions to pick up where we left off.

**Convention:** when something ships, move it from "Active backlog"
to "Shipped" with a date. When something new comes up, add it to
"Active backlog" or "Future / wishlist".

Last updated: 2026-06-19

---

## How to use this file

1. **At the start of any session**, read this file + `git log -10 --oneline`
   — picks up the latest priorities without re-explaining.
2. **As work happens**, use TaskCreate/TaskUpdate for in-flight items.
3. **At the end of each session**, move completed items into "Shipped".
4. Long-form architecture decisions live in `docs/`.

---

## Strategic north star (2026-05-18)

**Viktor (3rd-party Slack AI) wins on Q&A.** Our system owns:
1. The ABC / reorder / dormancy **engine** — Wired4Signs' actual buying policy
2. **Automations that ACT** — write-backs to CIN7, scheduled jobs, reminders
3. **The dashboard** — Ordering page, Slow Movers, Cashflow, Monthly Metrics
4. **CIN7 write-backs** — closing loops, not just reporting

Stop polishing the bot as a generalist answerer. Spend the build
budget on **action, not answers**.

The core business goal hasn't changed: **shrink stock holding**.

---

## Active backlog (priority order)

### Immediate — verify & stabilise (this session)

1. **Verify dropship UPS tracking end-to-end** — just fixed today
   (v2.67.266: `ship["Status"] = "AUTHORISED"` bug + channel ID wired).
   A live UPS email landed — check Render logs for
   `"Posted dropship-UPS confirmation"` or check the Slack thread for
   a bot reply. If it worked, this is done. If not, debug.

2. **BOM engine parent-child fix** — PAUSED pending Nicolas Noakes'
   deep dive (started 2026-05-21). Multi-level BOMs (6" Sample ←
   609mm ← 2m ← bulk roll) currently strand demand at intermediate
   levels; the bulk roll is under-credited. Fix: resolve every BOM
   chain to root, roll all-level demand onto root, don't credit
   intermediate on-hand stock as coverage. DO NOT patch until
   Nicolas's model is confirmed — risk of over-correcting.

### Tier 1 — Automations that ACT (priority per Viktor strategy)

3. **Non-UPS carrier dropship tracking** — handler currently only
   parses UPS forwarded emails. If suppliers use FedEx, USPS, or DHL,
   those emails are ignored. Add carrier detection + separate parsers
   (or a generic tracking-number extractor). Medium effort ~1 day.

4. **Dropship: manual tracking fallback** — if email parse fails,
   today the bot posts a diagnostic. Add a Slack button or slash
   command so staff can trigger a manual write to CIN7 without opening
   the sale. Saves Cheran copy-paste. ~3 hours.

5. **Cashflow: QBO sync reliability** — QBO token refresh + bill
   sync are running but may have edge cases. Monitor for 401/expired-
   token errors in logs. If recurring, add an auto-reconnect flow.
   Ongoing.

6. **Stock locator audit** — scheduled daily (v2.67.194). Verify it's
   posting to Slack each morning and the output is useful. Tune if
   noisy. Low effort.

7. **Weekly slow-movers email** — `weekly_slow_movers_email.py`
   exists. Verify it's scheduled and Cheran/buyer is receiving it.
   Check output quality. Low effort.

### Tier 2 — Dashboard & quality

8. **Overview page KPI refresh** — replace current overview with
   CIN7-style KPI cards (Revenue, Net, Pending, Inflow, Outflow) +
   area chart + period selector. James flagged this look. ~3 hours.

9. **ModifiedSince for master data syncs** — products / customers /
   suppliers / stock are full pulls every night (~10 min). CIN7's
   `ModifiedSince` param would cut to ~2 min. ~2 hours.

10. **Adaptive CIN7 rate limiting** — replace fixed 2.5s with
    adaptive: speed up to 1.5s after clean stretch, back off on 429.
    ~2 hours.

11. **SQLite backup to cloud** — `team_actions.db` is source of truth
    for migrations / drafts / pricing. Rsync nightly to Backblaze B2
    (~$1/mo). ~2 hours + B2 account setup.

12. **Custom domain** — `analytics.w4susa.com` instead of
    `wired4signs-app.onrender.com`. ~30 min, needs DNS access.

13. **Shopify Dev Dashboard OAuth** — currently using borrowed
    `shpat_` token from Darryl's app. Proper `SHOPIFY_CLIENT_ID` +
    `SHOPIFY_CLIENT_SECRET` flow so we own the token. ~1 day.

### Tier 3 — Commercial Intelligence

14. **Gorgias integration** — pull customer support conversations;
    extract demand signals + product complaints + return requests.
    Foundation for seeing what customers are asking for that we don't
    stock. ~1 week.

15. **Cancellation + return intelligence** — extract from Slack /
    Gorgias; reduce demand-signal weight for cancelled orders; warn
    buyer before reordering returned products. ~1 week.

16. **SEO intelligence layer** — monitor Slack channel for SEO
    updates; map ranking changes to Shopify collections; classify
    demand as early/emerging/confirmed. ~1 week.

17. **Inventory Planner decommission** — IP is the legacy system we
    want to drop. Audit what IP still does that we don't cover, build
    replacements, set sunset date. Our engine now does ABC / reorder /
    dormancy; the gap is probably just the UI for buyers who use IP
    directly. ~2 weeks.

18. **Multimodal Slack analysis** — vision API on photos of damaged
    products, install pics, CIN7 screenshots. ~1 week.

### Tier 4 — Customer-facing AI (medium-term)

Not building yet — staff version must be solid first. Key constraint:
**never expose Classification, costs, dormancy dates, or supplier
names to the customer-facing surface**. Enforce at the data-access
layer (don't just instruct the AI to hide it).

19. **Customer-facing product assistant** — embedded on
    wired4signs.com. Subset of current tools, tighter system prompt,
    slow-mover preference (to shift stock) without exposing WHY it's
    preferred. Design the staff version with this future split in mind.

### Tier 5 — SaaS (only if/when multi-tenant)

See `SAAS_NOTES.md`. Don't touch until at least 1-2 paying customers.
- Postgres migration
- Per-tenant auth + isolation
- Strip Wired4Signs-specific hardcoding from core
- Stripe billing

---

## Shipped (since 2026-04-30)

### 2026-06-19

- **PO commentary stock locators** — cached PO lookup and live CIN7 PO
  lookup now attach `stock_locator` to every line when CIN7's Stock
  locator field is populated. Slack/app commentary guidance says to
  show it per line and to omit it when blank, never substituting Default
  location / warehouse Location.

### 2026-06-18

- **Dashboard memory hardening** — reduced Streamlit/Render OOM risk
  by removing eager duplicate sale-line fallback loads and making the
  merged sale, sales-header, and purchase-line CSV loaders read only
  the columns the dashboard/AI actually consume. Cold ABC-engine
  rebuilds now start from leaner DataFrames instead of full-width CSV
  snapshots.
- **Ordering calc-trace memory hardening** — Ordering still computes
  numeric target/reorder/excess fields table-wide, but now builds the
  long markdown `calc_trace` only for the SKU being inspected. This
  avoids storing thousands of large explanation strings on `engine_df`
  during cold Render page loads.
- **Bulk-roll residue floor** — bulk-roll masters now treat less than
  5m worth of residual stock/target/position as zero for reorder,
  excess, out-of-stock, and Status calculations. Fixes 100m rolls with
  tiny CIN7 decimal leftovers showing as "Overstocked" while the UI
  rounds OnHand to 0.
- **TSB strip-family rollup hardening** — `LED-TSB` is now an explicit
  strip SKU prefix, so sales of child cuts such as
  `LED-TSB2835-300-24-6000-0305` roll into the 100m master
  `LED-TSB2835-300-24-6000-100M` without relying on the product title
  containing "strip".
- **Neonica 100m fractional ordering guard** — Neonica 100m bulk rolls
  remain decimal-orderable (`0.40` for 40m) even if supplier config is
  later tightened for full-roll-only suppliers.
- **Strip family movement audit** — Ordering Inspect now shows synced
  CIN7 sale-line movement for strip families in master-roll equivalents,
  including direct master sales, child/cut rollup, recent family rows,
  and top-customer concentration.
- **Calendar-month sales audit** — Ordering's Last 6 months buckets now
  use real calendar months by CIN7 InvoiceDate, and Inspect includes a
  per-SKU sales audit comparing InvoiceDate demand with OrderDate rows
  so current-month zeroes can be reconciled quickly.
- **Ordering grid focus UX** — the PO editor keeps its existing saved
  column layouts, but now adds browser-side horizontal-scroll helpers
  so buyers can move across wide columns with trackpads, Shift+wheel,
  modified arrow keys, or click-hold-and-drag panning instead of
  hunting for the bottom scrollbar. The clicked-row cue stays visible
  while moving sideways and clears when the buyer leaves the grid or
  moves vertically.
- **PO dispatch + receipt correctness** — dispatch reminders are
  line-level by SO/SKU and PO commentary now uses CIN7 StockReceived
  fields for PO-specific receipt wording rather than global stock
  availability.

### 2026-05-22

- **v2.67.267** — Fix `get_slack_messages` correlated subquery alias
  (`slack_messages.channel_id` → `m.channel_id`; was breaking all
  channel-filtered Slack queries from the AI assistant)
- **v2.67.266** — Dropship tracking bug fixes: `ship["Status"] =
  "AUTHORISED"` (setdefault left DRAFT sales unprocessed by CIN7);
  `SLACK_DROPSHIP_TRACKING_CHANNEL_ID=C0B3KD6GBM3` wired into
  render.yaml so the listener gates on the right channel
- **Port from Max plan session** — merged sad-volhard branch (v2.67.20
  → v2.67.265) into main on the company plan, bringing ~245 commits
  across

### 2026-05-18 to 2026-05-21 (Max plan session, now merged)

- **Viktor strategy pivot** — decided 2026-05-18: stop competing on
  Q&A, specialise on engine/automations/write-backs/dashboard. Viktor
  bridge (v2.67.124-126) routes Q&A to Viktor, bot overlays engine
  signals Viktor can't compute.
- **v2.67.265** — ABC engine: strip parser defers to BOM data (fixes
  BROADWAY family mis-classification)
- **v2.67.264** — Daily BOM sync so BOM data isn't week-stale
- **v2.67.263-262** — Notion sync: customer-safe Priority Stock page;
  stop creating duplicate pages
- **v2.67.261** — Fix duplicate tool name breaking every AI call
- **v2.67.260** — Slack: poll bot DM conversations for 1:1 chat
- **v2.67.259** — Slack: auto-poll dedicated single-purpose channels
- **v2.67.258** — Dropship tracking: subject fallback + diagnostic
  reply when parse fails
- **v2.67.257** — Notion sync: store DB IDs locally; stop duplicates
- **v2.67.256** — Notion pull: include DB row properties
- **v2.67.255** — Shipping channel: auto-investigate margin on
  SO/INV mention
- **v2.67.254** — Notion pull walks databases + auto-schedule on worker
- **v2.67.253** — dump-glossary command + app_glossary.md snapshot
- **v2.67.252-249** — Notion sync phases 1-2: slow-movers register
  + playbook pull + AI search tool (`search_knowledge_base`)
- **v2.67.248-245** — Stock-issues tracker hardening: reply polling,
  acknowledgement, escalation
- **v2.67.244-241** — Cashflow: overdue separation, daily calendar,
  alert system
- **v2.67.240** — PO-dispatch reminder: accurate per-SO SKU breakdown
- **v2.67.237** — Fix nearsync loop wedge + supervise sync loops
- **v2.67.236** — Overview: sales tile survives missing 30-day file
- **v2.67.235** — Cashflow: loan & debt tracker (amortization engine)
- **v2.67.234** — Cashflow: scenario planning + custom rows
- **v2.67.233-219** — Cashflow dashboard built end-to-end: QBO
  OAuth + bills + payables + weekly forecast grid + projections +
  actual opening balances + credit-card payments + week-shift control
  + scenario planning + loan tracker + alert system
- **v2.67.213-207** — QBO/app hardening: fix PO-dispatch false
  positives, EULA + privacy pages, QBO token retry, super_admin tier
- **v2.67.206-204** — Stock/purchase analysis fixes; Recent Sales
  window filter
- **v2.67.203** — PO escalation: check CIN7 live status too; bot
  replies in own threads
- **v2.67.202** — PG post-cutover migrations: once-per-process gate
- **v2.67.199** — Fix Postgres case-sensitivity on supplier_config
- **v2.67.198** — Slow Movers: coerce datetime to str before slice
- **v2.67.197** — PO commentary: handle draft POs via UUID-from-URL
- **v2.67.196** — `get_purchase_live` tool: live CIN7 fallback for
  fresh POs not yet in the sync CSV
- **v2.67.195-194** — Schedule BOM sync weekly; schedule stock
  locator audit as daily morning Slack post
- **v2.67.193** — PO commentary: trigger on bare PO refs + CIN7 URLs
- **v2.67.192-189** — User Permissions: form-reset fix, Slack DM
  invites, resend button, 'Add new user' expander
- **v2.67.188** — PO commentary crosspost: read POs from one channel,
  post analysis to another
- **v2.67.185** — User Permissions portal
- **v2.67.160-153** — Dropship UPS tracking: full handler built —
  parse UPS forwarded email, match CIN7 sale by customer name, write
  `TrackingNumber` to `Fulfilments[0].Ship.Lines`, weight-mismatch
  check vs Shopify order, threaded Slack reply
- **v2.67.145-144** — Stock-issues tracker with context-provider
  design; tighten buyer-ping rule
- **v2.67.141-136** — Back-in-stock handler: walk Slack share-message
  attachment blocks; arrival matching; subscription handler
- **v2.67.130-124** — Viktor bridge: forwarding + overlay flow;
  channel-gated forwarding; overlay engine signals on Viktor replies
- **v2.67.111-57** — Slack listener + sync full build: channel
  polling, classification, autonomous response, audit DB, returns
  channel, orders channel, PO-review channel

### 2026-04-30 (from old NEXT_STEPS.md)

- AI Assistant Phase 0 (6 tools, multi-turn, audit log, feedback)
- Knowledge base layer (ai_kb.py + docs/)
- Render deploy live (single service, persistent disk, password gate)
- Shopify content sync (shopify_sync.py)
- CIN7 PO POST integration (multi-step, auto-rollback)
- Demand signals table + manual entry UI (v2.58-v2.61)
- Demand Signals review/edit page + auto-reconcile to CIN7 sales
- Buyer warning column on Ordering page
- Demand scoring doc (docs/demand-scoring.md)
- Auto-finalize submitted POs
- Master-1-per-draft safeguard
- Feedback review page + auto-alias learning
- Inline charts in AI answers
- Weekly slow-movers email (weekly_slow_movers_email.py)
