# SAAS_NOTES.md
## Future considerations for turning this into a multi-tenant product

This file captures decisions and gotchas relevant to making the app
sellable to other CIN7-using companies. None of these are needed for
v1 (single-tenant Wired4Signs). They're listed here so future-you (or
a developer hire) doesn't have to rediscover them.

---

## Tenancy model — pick before scaling past ~5 customers

| Model | Pros | Cons | When to choose |
|-------|------|------|----------------|
| **Silo** (one Render service per tenant) | Strongest isolation; SQLite still fine; no code changes needed | Render bill per tenant (~$26/mo each); no shared infra savings | 1–30 tenants, especially regulated/sensitive data |
| **Pool** (one service, Postgres with `tenant_id`) | Cheapest at scale; one deploy to manage | Schema refactor needed; `tenant_id` filter on every query; one bad migration affects everyone | 100+ tenants, low-touch onboarding |
| **Bridge** (one service, separate DB per tenant) | Balance of both; failure isolation; scaling sweet spot | Slightly more ops complexity; need a tenants index DB | 30–100 tenants |

Recommendation: silo until ~10 paying customers, then evaluate.

---

## Code-level changes needed for true multi-tenancy

These are the things to fix when going pool/bridge. None apply to silo.

1. **SQLite → Postgres.** Every table needs a `tenant_id` column and a
   composite index `(tenant_id, ...)`. Every query needs the
   `WHERE tenant_id = ?` filter. The `db.py` connection function needs
   to know which tenant the request belongs to (via session/request
   context).

2. **Auth needs to identify the tenant.** Right now `APP_PASSWORD` is
   a single shared password. For SaaS:
   - Each user has a unique account (email + password OR Google OAuth)
   - Each account belongs to exactly one tenant
   - Login flow sets `session.tenant_id` which every query reads from
   - Add a "tenants" table (id, name, created_at, plan) and a
     "users" table (id, email, tenant_id, role)

3. **Per-tenant CIN7 credentials.** Right now CIN7_ACCOUNT_ID and
   CIN7_APPLICATION_KEY are env vars. They become per-tenant config
   stored encrypted in the DB. Rotate via UI.

4. **Per-tenant data isolation in `output/`.** Currently the CSV pulls
   share a directory. Either move to per-tenant subdirectories
   (`/data/output/<tenant_id>/...`) or move CSV ingestion fully into
   the DB so the filesystem stops mattering.

5. **Sync scheduling.** Currently one cron job at 02:00 UTC. With many
   tenants you'd want either staggered cron (so they don't all hit
   CIN7's 60/min cap simultaneously) or queued background workers.

6. **Cost accounting.** CIN7 throttles per-account, not per-tenant of
   yours. So if multiple tenants share a CIN7 instance you have a
   contention problem. Realistically each tenant has their own CIN7
   account so this isn't an issue.

---

## Branding / company-specific text already extracted to env vars

These can be set per tenant without code changes:

- `COMPANY_NAME` — display name used throughout the UI.
- `APP_TITLE` — browser page title.
- `CIN7_DEFAULT_LOCATION` — default warehouse for PO POSTs.

Things that are STILL Wired4Signs-specific and would need extraction:

- LED-tube–specific BOM logic in `app.py` (lots of references to
  SIERRA38, SMOKIES, CASCADE, MP variants, "bare tubes" terminology).
- Family pricing rules seeded with Reeves-specific tiers.
- The "LED Tubes" page and its critical-components sidebar.

These are core to Wired4Signs's business model. For other tenants
they'd need to be either disabled or made fully data-driven (no
hardcoded family names — read from the DB).

---

## Pricing / packaging hypothesis (parked for future-you)

A possible go-to-market structure if this becomes a product:

| Tier | Price | What's included |
|------|-------|-----------------|
| Starter | $99/mo | 1 user, basic ABC + reorder, no PO push |
| Pro | $299/mo | 5 users, multi-draft POs, PO push, supplier configs |
| Enterprise | $999+/mo | Unlimited users, custom BOM logic, dedicated CSM |

CIN7 itself charges $349-799/mo per user, so a $99-299 add-on is
a sensible price point. Rough back-of-envelope: 100 customers at
$200 average = $20k MRR. Achievable in 2–3 years if you market well.

---

## Legal / compliance things to think about LATER

- **Data processing agreement (DPA)** with each customer — required if
  any are EU-based.
- **Terms of service + privacy policy** — generic SaaS templates work
  for v1 (Termly, iubenda, Vanta). Spend $50-200, not $5000 in legal
  fees.
- **CIN7 partnership** — they have a partner program. Worth applying
  to once you have 3+ paying customers; lets you list the app on
  their marketplace.
- **Terms-of-use for CIN7 API** — they require certain data handling
  practices for resellers/integrators. Read their docs.

---

## When to stop using SQLite

Hard limits:
- More than ~5 concurrent writers (SQLite serialises writes).
- More than ~50GB of data per tenant.
- Need for read replicas or multi-region.

For Wired4Signs specifically: SQLite is fine indefinitely. The DB is
~10MB after months of use. There are 5 users tops, mostly reading.

---

## Decision deferred to "future James"

- Whether to keep SQLite or move to Postgres. (Cost: 1-2 weeks dev.)
- Whether to do silo, pool, or bridge tenancy. (Cost: 2-4 weeks for
  pool/bridge; near-zero for silo.)
- Whether to build a customer-facing onboarding flow. (Cost: 1 week.)

None of these block v1.
