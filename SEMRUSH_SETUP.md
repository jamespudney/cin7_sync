# SEMrush — Setup & Verification Guide

This is for the cin7_sync `semrush_sync.py` integration. Quick tour of how SEMrush is structured, how the script uses it, and what to verify.

## How `semrush_sync.py` queries

Uses the `domain_organic` API endpoint, which queries by domain name. **No SEMrush project setup required** — it asks SEMrush directly: *"what keywords does wired4signsusa.com rank for?"*.

```python
SEMRUSH_DOMAIN = "wired4signsusa.com"  # default
```

If you have multiple companies in your SEMrush account, **none of them affect this query** — we hit the SEMrush organic search index directly for the wired4signsusa.com domain.

## Quick verification checklist

Run through these on semrush.com once — takes ~5 min.

### ✅ Verify your domain is being tracked organically

1. Log into [semrush.com](https://semrush.com)
2. Top search bar → enter `wired4signsusa.com` → press Enter
3. You should land on the **Domain Overview** page showing:
   - Organic search traffic (monthly visitors estimate)
   - Number of keywords ranking
   - Top countries
   - Top organic keywords table

If you see real numbers there → SEMrush has indexed your domain → our `domain_organic` calls will return data. ✓

### ✅ Verify your API plan tier

1. Top right → click your avatar → **Subscription Info**
2. Look for the row labeled **API access**
   - Must say **API Units / month: 30,000** or higher (Guru / Business / Enterprise)
   - **Pro** doesn't include API — would need to upgrade

If it says Guru: you're set. ✓

### ✅ Find your API key (if not already done)

1. Profile menu → **Subscription Info**
2. Left sidebar → **API Units**
3. Right side panel → **Your API key**
4. The key is a 32-character hex string

This goes into Render env var `SEMRUSH_API_KEY`. (You already set this in earlier session.)

## What the daily/weekly script does

### Weekly cycle (every 7 days, automatic via slack_loop.sh)

```bash
python semrush_sync.py weekly --limit 500
```

Pulls top **500 keywords** that wired4signsusa.com ranks for, sorted by traffic share. For each keyword we capture:

| Field | Example |
|---|---|
| `keyword` | "led channel under cabinet" |
| `position` | 2.0 |
| `previous_position` | 7.0 (for trend) |
| `search_volume` | 880 (monthly searches) |
| `url` | https://wired4signsusa.com/products/slim-led-channel-slim8... |
| `family` | "V3060001" (resolved via product_dimensions) |

Cost: ~5,000 units/week. Guru gives 30,000/mo → ~22% utilisation.

## Optional upgrade — Position Tracking projects

If you want to monitor a **specific curated keyword list with weekly auto-reports** (vs the auto-discovered top 500), SEMrush also has a "Position Tracking" feature. Different from what we're using now.

**Set up** (only if you want this):
1. SEMrush → top nav **Projects** → **+ Add Project**
2. Domain: `wired4signsusa.com`
3. Project name: "Wired4Signs USA"
4. Add to project → **Position Tracking**
5. Add up to 500 keywords you specifically care about (e.g. brand terms, top-volume product terms)
6. Note the project ID from the URL (e.g. `https://semrush.com/projects/1234567/...` → `1234567`)

If you'd like that data **in addition to** the domain_organic data we already pull, tell me the project ID and I'll build a `semrush_position_tracking_sync.py`.

For most use cases the current `domain_organic` is plenty — auto-discovers what you rank for, no manual list maintenance, broader coverage.

## Verification once data is flowing

After the first weekly sync runs (within 24h of the worker auto-deploy of v2.67.93+), spot-check from the worker shell:

```bash
sqlite3 -header -column /data/team_actions.db \
  "SELECT keyword, position, previous_position, search_volume,
          family, captured_at
   FROM seo_keyword_positions
   WHERE captured_at >= datetime('now', '-7 days')
   ORDER BY position ASC
   LIMIT 25;"
```

You should see real keywords with real positions. If empty → SEMRUSH_API_KEY may be wrong, or the slack_loop's weekly cycle hasn't fired yet.

## How the bot uses this

When a Slack user asks *"why are sales of LED-Slim8 up 40%?"*, the bot calls `get_marketing_intelligence(sku="LED-Slim8...")` which returns:

```json
{
  "sku": "LED-Slim8...",
  "family": "V3060001",
  "seo": {
    "observations": 12,
    "top_movements": [
      {"keyword": "led channel under cabinet",
       "position": 2.0, "previous_position": 7.0,
       "search_volume": 880}
    ]
  },
  "email": { ... },
  "reviews": { ... }
}
```

Bot composes: *"LED-Slim8 sales are up 40%. Likely drivers: SEO position jumped 7→2 for 'led channel under cabinet' (880 searches/mo) on 4/12, reinforced by newsletter 4/15 (32% open, 184 clicks on Slim8 link)."*

That's the Moby-replacement narrative. SEMrush is the foundation.
