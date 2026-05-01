# Demand scoring — the brain of the proactive intelligence layer

This doc describes how raw demand signals become a buyer-facing score
and warning. It's both a design spec for what we're building and a
reference for what `db.demand_signals` rows MEAN.

Right now we have:

- A `demand_signals` table with manual entry from the sidebar form
- AI tools that surface signals (`get_recent_signals`, `get_top_inquired_products`, `get_rising_demand`)
- A rule-based warning column on the Ordering page

What's still being designed (and what this doc is for):

- A 0-100 demand score per SKU, with confidence band
- Conversion rate (signal-to-sale) tracking
- Source-weighted scoring (a Slack inquiry from a salesperson who
  knows the customer is worth more than an anonymous web form)
- Time decay (a 6-month-old inquiry shouldn't carry the same weight
  as one from this morning)

---

## What is a demand signal?

A `demand_signals` row is **any moment where someone showed interest
in a product**. The interest may or may not translate to a sale; the
score is what tells us "how confident should the buyer be that this
interest is real, sustained, and conversion-likely?".

Sources of signals (open-ended; we add as integrations land):

| Source | Examples |
|--------|----------|
| `manual` | Sales rep types into the sidebar form: "customer asked for warm white driveway lights" |
| `slack` | Bot extracts from /stock or /askstock commands, or from free-text in a monitored channel |
| `gorgias` | Customer support chat or ticket mentions a product / asks for stock |
| `seo` | SEO team flags ranking improvement on a collection or keyword |
| `shopify_search` | Storefront search query (Shopify search analytics) |
| `shopify_abandoned` | Customer added to cart, didn't check out |
| `web_form` | Quote-request form, notify-me form |
| `phone` | Sales rep notes a phone inquiry |

Signal types (open-ended; what KIND of moment it was):

| Type | What it means |
|------|---------------|
| `inquiry` | "Do you stock this?" — earliest funnel signal |
| `quote` | Customer asked for a quote — stronger signal |
| `sold` | Inquiry converted to a sale — closes the loop |
| `lost` | Inquiry lost (out of stock / wrong product / no follow-up) |
| `substitute_offered` | Sales offered an alternative — relevant for substitution analytics |
| `cancelled` | Order was cancelled after placement — DOWN-WEIGHT signal |
| `returned` | Customer returned product — DOWN-WEIGHT signal |
| `complaint` | Quality / fit / function complaint — quality flag |
| `abandoned_cart` | Web cart abandoned — light signal |
| `notify_me` | "Email me when in stock" — strong intent signal |
| `seo_rank` | SEO ranking change — leading indirect signal |
| `search_query` | Storefront search — light signal but volume matters |

---

## The score (proposed — Phase 1 build)

For a given SKU, the **demand score** is a 0-100 number computed
from all `demand_signals` for that SKU within the last N days.

```
score(sku) = clamp_0_100(
    base_volume_score(sku, days=30)         # how many signals?
    × signal_quality_weight(sku, days=30)   # which signal types?
    × source_credibility_weight(sku, days=30)  # which sources?
    × recency_weight(sku, days=30)          # how recent?
    × conversion_factor(sku, days=90)       # any actually converted?
    − quality_penalty(sku, days=90)         # cancellations, returns, complaints
)
```

### `base_volume_score` — raw count, log-scaled

A SKU with 1 signal scores ~10. With 10 signals, ~50. With 50, ~80.
Diminishing returns past that. Formula:

```
base_volume = min(100, 20 × log2(1 + signal_count))
```

### `signal_quality_weight` — what kind of signal?

Weights per signal type:

| Type | Weight |
|------|--------|
| `quote` | 1.5 |
| `notify_me` | 1.4 |
| `inquiry` | 1.0 |
| `abandoned_cart` | 0.7 |
| `search_query` | 0.4 |
| `seo_rank` | 0.3 |
| `complaint` | 0.0 (counted but not as demand) |
| `cancelled` / `returned` | NEGATIVE (see penalty below) |

Apply by summing weighted counts then dividing by raw count to get
an average quality multiplier for the SKU.

### `source_credibility_weight` — which source?

Weights per source:

| Source | Weight | Why |
|--------|--------|-----|
| `manual` (sales rep) | 1.0 | Curated; sales filtered noise |
| `slack` (curated channel) | 1.0 | Same |
| `gorgias` | 0.9 | Customer-direct; high signal but mixed quality |
| `phone` / `web_form` | 0.9 | Same |
| `shopify_search` | 0.6 | Volume-heavy, low individual signal |
| `shopify_abandoned` | 0.5 | Many abandons aren't real demand |
| `seo` | 0.7 | Leading indicator but indirect |

### `recency_weight` — when?

Linear decay over the window:

```
weight_per_signal = max(0.1, 1.0 − (age_days / window_days))
```

A signal from this morning weights 1.0; a 30-day-old one weights ~0.1.

### `conversion_factor` — does interest → sale?

For each SKU, compute `sold_signals / total_signals` over the last 90
days. Multiplier:

```
conversion_factor = 1.0 + min(0.5, conversion_rate)
```

A SKU with 50% inquiry-to-sold conversion gets a 1.5× multiplier;
zero conversion = 1.0× (no penalty, but also no boost).

### `quality_penalty` — cancellation / return / complaint

Subtract:

```
quality_penalty = 5 × (cancelled + returned + complaint)  # cap at 30
```

A SKU with 3 returns this quarter loses 15 points off its raw score.

---

## Confidence band

Alongside the score, return a `confidence` value (0-1) that tells
the buyer how much weight to put on the score itself:

```
confidence = clamp_0_1(
    0.3 + 0.1 × min(7, distinct_signal_count)
        + 0.05 × distinct_source_count
        + 0.1 × distinct_customer_count
)
```

Logic: more signals + more sources + more customers = higher
confidence. A SKU with 1 signal from 1 customer gets ~0.4 confidence
("preliminary"); a SKU with 7+ signals from 3+ sources from 5+
customers gets ~0.95 ("strong").

---

## Buyer warning column — current rules vs scoring

Today (Phase 0) the Ordering page's warning column uses simple
boolean rules (see `app.py` `_warning_for_row`). Phase 1 should
replace those rules with score-based thresholds:

```
if score >= 70 and was_dead_or_slow:
    "🟡 Score 75/100: previously dead, recent surge — verify"
elif score >= 50 and confidence < 0.5:
    "👀 Score 55/100, low confidence — monitor"
elif quality_penalty >= 15:
    "⛔ Quality concerns: 3 returns this quarter"
```

The advantage: a single score + confidence beats a list of binary
flags for explainability and tunability.

---

## Decision rules (currently rules; will become score-driven)

| Buyer question | Today (rules) | Tomorrow (score) |
|----------------|---------------|------------------|
| Should I reorder X? | "AI Warning column" + classification | `score >= 50 AND confidence >= 0.6 AND quality_penalty < 10` |
| What's getting attention? | `get_top_inquired_products` | Top 15 by score over 30 days |
| What's rising? | `get_rising_demand` (count delta) | SKUs with score growth >= 30% week-over-week |

---

## Implementation order

1. ✅ `demand_signals` table — DONE
2. ✅ Manual entry form — DONE
3. ✅ AI query tools (recent / top / rising) — DONE
4. ✅ Rule-based warning column — DONE
5. **Next:** Score computation function (`db.compute_demand_score(sku)`)
6. **Next:** Replace rule-based warnings with score-based warnings
7. **Next:** Buyer dashboard with score leaderboard

---

## Open questions / decisions to make

- **Default score window:** 30 days? Could be configurable per SKU.
- **Should returned/cancelled units factor by quantity?** If a customer
  returns 50 units that's worse than returning 1.
- **Per-customer cap:** should we cap a single customer's contribution to
  prevent one big customer from dominating the score?
- **Family-level rollup:** does demand on a family's variants boost the
  master? (Probably yes — same rollup pattern as the engine's effective
  demand.)
- **SEO weighting:** SEO signals are leading-indirect. Maybe they should
  affect a separate "potential demand" score, not the actionable one.

These get answered as the system gets used and patterns emerge.

---

## Future sources to plug in (no schema change needed)

When these integrations land, just start writing rows with the new
`source` and `signal_type` values. The score formula auto-incorporates
them via the source-credibility table:

- **Slack `/stock` command** — extracts SKU + customer + qty, writes
  `source='slack', signal_type='inquiry'`
- **Slack `/cancel` command** — writes `signal_type='cancelled'`
- **Gorgias chat AI extraction** — pulls "do you have X?" from chats,
  writes `source='gorgias', signal_type='inquiry'`
- **SEO Slack channel** — writes `source='seo', signal_type='seo_rank'`
- **Shopify search analytics** — writes `source='shopify_search',
  signal_type='search_query'` for high-volume terms
- **Shopify abandoned carts** — writes `source='shopify_abandoned',
  signal_type='abandoned_cart'`

Each source pipeline is its own Phase 1+ task. The scoring engine
doesn't need to change.
