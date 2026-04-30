# Data sources — what's the source of truth for what

The app pulls from multiple systems. When numbers conflict, this doc
tells the AI Assistant (and you) which one wins.

## TL;DR

| Question | Source of truth |
|----------|-----------------|
| Stock on hand (units) | **CIN7** |
| Stock value ($) | **CIN7** (cost) × CIN7 (qty) |
| Sales / orders / revenue | **CIN7** |
| Purchase orders / supplier costs | **CIN7** |
| Product master data (SKU, name, type, BOM) | **CIN7** |
| Customer-facing product descriptions | **Shopify** |
| Product collections / categories | **Shopify** |
| FAQ / About / blog content | **Shopify** |
| SEO tags / metafields | **Shopify** |
| Customer support conversations | **Gorgias** (when integrated, Phase 2+) |
| Internal team chat | **Slack** (when integrated, Phase 2+) |

## CIN7 is the master for inventory

CIN7 Core (formerly DEAR) holds the physical inventory of record:

- Every SKU's stock-on-hand level
- Every sale that affects stock
- Every purchase order that adds stock
- Every stock adjustment, transfer, or write-off
- Every supplier and their costs

**Shopify mirrors stock from CIN7.** When a Shopify order is placed,
the integration deducts inventory in CIN7. When CIN7 stock changes,
the integration updates Shopify. There's a small lag (seconds to
minutes), and during that lag Shopify and CIN7 can disagree
temporarily — but **CIN7's number is correct** and Shopify catches
up.

Practical implications for the AI Assistant:

- "What's on hand for X?" → answer from CIN7 (via the engine /
  search_products / get_sku_details tools).
- "Why does Shopify say 5 but CIN7 says 4?" → tell the user the
  CIN7 number is correct; the gap is sync lag and will close on
  the next CIN7 → Shopify push.
- Never quote Shopify-API stock figures as authoritative. The
  shopify_sync.py script intentionally does NOT index Shopify's
  inventory_quantity field for this reason.

## Shopify is the master for customer-facing copy

What Shopify does have that CIN7 doesn't:

- Marketing-quality product descriptions (the long-form HTML body
  customers see on the product page)
- Collections (e.g. "LED Driveway Lights", "Warm White Strips") —
  these don't exist in CIN7
- Tags (SEO keywords, custom categories)
- Pages (FAQ, About, Returns Policy, Shipping Info)
- Blog posts (use cases, install guides, application stories)
- Metafields / metaobjects (custom attributes the SEO team curates)

When the AI is asked about how a product is positioned, what it's
used for, or what the FAQ says, it should consult the Shopify
content (`/data/shopify/...` paragraphs surfaced via
search_knowledge_base).

## When CIN7 and Shopify disagree

A few cases where you'll see disagreement:

1. **Stock during sync lag.** CIN7 changed; Shopify hasn't synced
   yet. → Use CIN7. Lag is usually <2 minutes.
2. **Product name / description.** CIN7 has terse SKU descriptions
   ("LED-2700K-24V-50M-WW"); Shopify has marketing copy ("Warm
   White 24V Bulk Roll, 50m, 3000K"). → Use both: CIN7 for SKU /
   technical specs, Shopify for the customer-facing language. The
   AI can include both if asked.
3. **Variants / options.** CIN7 SKUs may not map 1:1 to Shopify
   variants. Some Shopify variants share a CIN7 master SKU; some
   CIN7 SKUs aren't on Shopify (B2B only). → Trust CIN7's SKU as
   the technical key; Shopify's variant ID is for display only.
4. **Discontinued products.** A product can be archived in Shopify
   but still active in CIN7 (or vice versa). → CIN7 status is the
   commercial reality; Shopify visibility is the customer-facing
   reality. They can intentionally diverge.

## Future sources (Phase 2+)

When we add these, the source-of-truth rules extend:

- **Gorgias** — source of truth for customer support conversations,
  return requests, complaints. When asked "what are customers asking
  about?", answer from Gorgias.
- **Slack** — source of truth for internal team discussion. When
  asked "what is sales talking about?", answer from Slack.
- **Inventory Planner** — being decommissioned; treat as legacy.
  Anything in IP should also be in CIN7 or our local DB.

## Rule for the AI

When in doubt about a stock or financial number: **always answer
from CIN7-derived data**. If the user references a Shopify number
that disagrees, the AI should explain that CIN7 is the source of
truth and offer to look up the CIN7 figure.

When asked about customer-facing language, marketing positioning, or
how products are organised on the storefront: answer from Shopify
content (`/data/shopify/...`).

When in doubt about which to use: **prefer CIN7 for numbers,
Shopify for words**.
