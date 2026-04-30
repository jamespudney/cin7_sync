# Roadmap — what's planned and what's done

This is the canonical backlog. Edit it directly any time — it's
indexed by the AI Assistant's knowledge base and read by future
Claude sessions to pick up where we left off.

**Convention:** when something ships, move it from "Active backlog"
to "Shipped" with a date. When something new comes up, add it to
"Active backlog" or "Future / wishlist".

Last updated: 2026-04-30

---

## How to use this file

1. **At the start of any session**, ask Claude to read `NEXT_STEPS.md`
   — it'll pick up the latest priorities without you re-explaining.
2. **As work happens**, the task tool tracks in-flight items. At the
   end of a session, I'll move completed ones into "Shipped" here.
3. **Long-form context** (architecture decisions, why we did things
   a certain way) lives in `docs/` — that's also indexed by the AI.

---

## Active backlog (priority order)

### Tier 1 — fix soon (next 1-2 sessions)

1. **Auto-finalize submitted POs (Phase 3)** — when CIN7 sync detects
   a submitted PO has flipped to `ORDERED`, auto-transition our
   local `po_drafts.status` from `submitted` → `finalized`. The DB
   function `db.mark_po_draft_finalized()` exists; the sync trigger
   isn't wired into `cin7_sync.py` yet. ~30 min.
2. **Master-1-per-draft session safeguard** — belt-and-braces in
   `cin7_post_po.py`: track in session state whether a master POST
   has been attempted for a given draft; refuse a second attempt
   even if local `cin7_po_id` was somehow cleared. Prevents the
   4-orphan-PO scenario. ~30 min.
3. **Feedback review page + auto-alias learning** — new "Review AI
   Q&A" page showing recent chats with feedback filter. For
   thumbs-down rows, allow buyer to enter a corrected SKU + the
   phrase that confused the AI. Writes to `product_aliases`. The
   AI Assistant on its next call checks `db.lookup_aliases()` first
   and uses the human-approved mapping. ~3 hours.
4. **Inline charts in AI answers** — extend `get_velocity` (and
   maybe `get_sales_totals`) with daily/weekly buckets; the
   Streamlit page detects chartable tool results and renders an
   inline `st.line_chart`. ~30 min.

### Tier 2 — quality & performance

5. **Refresh Overview to CIN7-style KPI dashboard** — KPI cards
   across the top (Revenue, Net, Pending, Inflow, Outflow), area
   chart below, period selector. User flagged this as the look they
   like in CIN7. ~3 hours.
6. **Use ModifiedSince for master data syncs** — products /
   customers / suppliers / stock are full pulls every night
   (~10 min). CIN7's `ModifiedSince` parameter would cut that to
   ~2 min by only fetching changed rows. ~2 hours.
7. **Adaptive CIN7 rate limiting** — replace fixed 2.5s rate with
   adaptive: speed up to 1.5s after several minutes without a 429,
   back off on first 429. Optimally uses whatever bandwidth is
   available regardless of what other integrations are doing.
   ~2 hours.
8. **Preemptive empty-data guards** — proactively scan the major
   pages (Overview, Monthly Metrics, FixedCost Audit, Ordering, LED
   Tubes) for `df["X"]` patterns where X might not exist on a fresh
   deploy. Replace with safe `.get()` patterns. ~2 hours.

### Tier 3 — bigger features

9. **Shopify integration via Dev Dashboard OAuth (Phase 1)** —
   refactor `shopify_sync.py` to use modern Dev Dashboard flow:
   `SHOPIFY_CLIENT_ID` + `SHOPIFY_CLIENT_SECRET`, request access
   tokens programmatically, cache + refresh, prefer GraphQL Admin
   API over REST. Don't depend on the legacy `shpat_` token
   long-term (currently borrowed from Darryl's app). ~1 day.
10. **CIN7 PO push: pre-submit validation enrichment** — verify
    Location exists in CIN7; compute CIN7's expected line Total in
    dry-run BEFORE master POST so mismatches are caught early; warn
    if a SKU has no per-supplier `Cost` (would fall back to
    `AverageCost`). ~3 hours.
11. **Nightly SQLite backup to cloud storage** — `team_actions.db`
    is the source of truth for migrations / drafts / pricing. We
    should rsync it to Backblaze B2 (~$1/mo) every night. ~2 hours
    + B2 account setup.
12. **Custom domain** — `analytics.w4susa.com` instead of
    `wired4signs-app.onrender.com`. ~30 min, requires DNS access.

### Tier 4 — Commercial Intelligence System (the big vision)

This is the multi-month roadmap. Each is its own project, queued in
priority order:

13. **Slack demand-signal capture** — bot with /stock, /askstock,
    /slowstock, /deadstock, /cancel, /return commands; LLM
    extraction of demand signals from messages; AI clarification
    loop in threads. Buyer warning column on Ordering page.
    ~2 weeks.
14. **Cancellation + return intelligence** — extract from Slack/
    Gorgias mentions; reduce demand-signal weight for cancelled
    orders; warn buyer before reordering returned products.
    ~1 week.
15. **Gorgias integration** — pull customer support conversations;
    extract demand signals + product complaints + return requests.
    ~1 week.
16. **SEO intelligence layer** — monitor a dedicated Slack channel
    for SEO updates; map ranking changes to Shopify collections;
    classify demand as early/emerging/confirmed. ~1 week.
17. **Weekly buyer summary email** — top demand signals, rising
    families, repeated out-of-stock inquiries, dead stock with new
    demand, return-affected reorders. ~3 days.
18. **Multimodal Slack attachment analysis** — vision API on photos
    of damaged products, screenshots of CIN7 issues, customer
    install pics. ~1 week.
19. **Inventory Planner decommission** — IP is the legacy system
    we want to drop. Audit what IP still does that we don't, build
    replacements, set sunset date. ~2 weeks.

### Tier 5 — SaaS readiness (only if/when we go multi-tenant)

See `SAAS_NOTES.md` for the full list. Headline items:

- Postgres migration (replace SQLite for multi-tenant queries)
- Per-tenant authentication + isolation
- Pull "Wired4Signs USA" hardcoded business logic out of core code
- Per-customer billing / Stripe integration

Don't do these until we have at least 1-2 paying customers asking
for it. Wasted effort otherwise.

---

## Shipped recently

### 2026-04-30 (today)

- **AI Assistant Phase 0** — natural-language Q&A page, 6 live tools
  (search_products, get_sku_details, get_velocity, get_dead_stock,
  get_migration_chain, get_sales_totals, search_knowledge_base),
  multi-turn conversation memory, audit log, thumbs up/down feedback.
- **Knowledge base layer** — `ai_kb.py` indexes `docs/` (8 starter
  docs: inventory-rules, reorder-engine, sync-cadences, migrations,
  po-workflow, glossary, data-sources, README) plus root-level
  RULES.md / DEPLOY.md / SAAS_NOTES.md.
- **Render deploy live** — single web service, persistent disk,
  password gate, 15-minute nearsync + nightly daily-sync, both
  inside the same container.
- **Today/MTD revenue fix** — switched to order-level
  `InvoiceAmount` so revenue matches CIN7's dashboard (includes
  shipping + tax).
- **Sidebar declutter** — consolidated 4 refresh-related buttons
  into 1.
- **Shopify content sync** — `shopify_sync.py` pulls products /
  collections / pages / blog articles via Admin API; AI knowledge
  base auto-indexes them. Uses borrowed token from Darryl's app
  for now (proper OAuth in Tier 3).
- **Source-of-truth rules** — docs/data-sources.md baked into
  system prompt: CIN7 for numbers, Shopify for words.
- **Path refactor for portability** — `data_paths.py` centralises
  `DATA_DIR` so the same code runs locally and on Render.
- **CIN7 PO POST integration** — multi-step flow with auto-rollback,
  strict supplier matching, per-supplier-Cost lookup, retry-lines
  recovery for partially-failed pushes.
- **`sync_supplier_names.py`** — drift detector + renamer across
  9 supplier-referencing tables; CIN7 is source of truth.

### Earlier (April 28-29)

- v2.22 — migration-aware demand rollup in engine
- v2.31-v2.33 — 45d/90d/365d customer rollups
- v2.34-v2.40 — CIN7 PO POST iteration cycle
- Multi-draft PO system with pessimistic locking
- IP merged[] migration import

See `git log` for the full history.

---

## Conventions for future Claude sessions

When starting a new session, please:

1. Read this file first.
2. Read `RULES.md` (business rules) and `docs/data-sources.md`
   (where to trust which data).
3. Check `git log -10 --oneline` to see what's changed recently.
4. Use TaskCreate/TaskUpdate liberally — even small tasks are worth
   tracking so they don't slip.
5. **At the end of each session**, update this file's "Shipped"
   section so the next session has accurate context.
