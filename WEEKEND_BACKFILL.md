# Weekend Backfill Plan — Marketing Data

Goal: 3 years of historical Google Ads + GA4 data so the Ad-Umpire dashboard, the AI Assistant, and the Slack bot can do proper multi-year analysis.

## When to run

**Friday evening or Saturday morning.** Each backfill runs in the background via `nohup` so you can disconnect the shell. Total time: ~20–30 min walltime, ~$0 (all free APIs).

## Risk to watch

Render auto-deploys on every push to `main`. If I commit code Sat/Sun, the worker redeploys and **kills any in-flight backfills**. **Tell me Friday evening when you're about to start, and I'll code-freeze the worktree until you confirm done.**

## Step 1 — Open worker shell

Render → `cin7-sync-slack-bot` → Shell tab.

```bash
cd /opt/render/project/src
git checkout main 2>/dev/null
git status  # should show 'On branch main'
```

## Step 2 — Pin Google Ads API version (one-time)

Set in Render env permanently so the daily cycle uses v22:

```
GOOGLE_ADS_API_VERSION = v22
```

(env var; otherwise the daily cycle defaults to v19 which is deprecated)

## Step 3 — Kick off all backfills

Run these commands in sequence. Each is a separate `nohup` job so they all run in parallel — total walltime is bounded by the slowest one.

```bash
# Google Ads campaign-level (3 years, ~3 chunks @ 365 days each)
nohup python google_ads_sync.py full --days 1095 --verbose \
  > /data/output/backfill_google_ads.log 2>&1 &
echo "google_ads PID: $!"

# Google Ads PER-SKU shopping spend (v2.67.105) — same 3 years
nohup python google_ads_sync.py per-sku-backfill --days 1095 --verbose \
  > /data/output/backfill_google_ads_per_sku.log 2>&1 &
echo "google_ads per-sku PID: $!"

# GA4 (3 years, ~12 chunks @ 90 days each)
nohup python ga4_sync.py backfill --days 1095 --verbose \
  > /data/output/backfill_ga4.log 2>&1 &
echo "ga4 PID: $!"

# Reviews.io full backfill
nohup python reviewsio_sync.py full --verbose \
  > /data/output/backfill_reviewsio.log 2>&1 &
echo "reviewsio PID: $!"

# Klaviyo 1-year backfill (90 days was the daily; full is more useful)
nohup python klaviyo_sync.py recent --days 365 --verbose \
  > /data/output/backfill_klaviyo.log 2>&1 &
echo "klaviyo PID: $!"

jobs -l   # show all background jobs
```

## Step 4 — Monitor (anytime, can disconnect freely)

```bash
# Quick status — are jobs still running?
ps aux | grep -E "(google_ads|ga4|reviewsio|klaviyo)_sync" | grep -v grep

# Tail any single log
tail -f /data/output/backfill_google_ads.log

# Or quick "where are we" check across all
for f in /data/output/backfill_*.log; do
  echo "=== $f ==="
  tail -3 "$f"
done
```

## Step 5 — Verify when done (~30 min later)

```bash
sqlite3 -header -column /data/team_actions.db <<'SQL'
-- Google Ads campaign data (campaign-level)
SELECT 'ad_campaigns_daily' AS source,
       MIN(date) AS earliest, MAX(date) AS latest,
       COUNT(DISTINCT date) AS days,
       COUNT(*) AS rows,
       ROUND(SUM(spend), 2) AS total_spend,
       ROUND(SUM(revenue_ga4), 2) AS total_revenue
FROM ad_campaigns_daily WHERE platform = 'google_ads'
UNION ALL
-- Per-SKU spend + revenue
SELECT 'ad_campaign_skus' AS source,
       MIN(date), MAX(date),
       COUNT(DISTINCT date), COUNT(*),
       ROUND(SUM(spend), 2),
       ROUND(SUM(revenue), 2)
FROM ad_campaign_skus
UNION ALL
-- Reviews
SELECT 'product_reviews', MIN(review_date), MAX(review_date),
       NULL, COUNT(*), NULL, NULL
FROM product_reviews
UNION ALL
-- Email campaigns
SELECT 'email_campaigns', MIN(sent_at), MAX(sent_at),
       NULL, COUNT(*), NULL,
       ROUND(SUM(revenue), 2)
FROM email_campaigns;
SQL
```

Expected (rough):
- `ad_campaigns_daily` earliest = ~3 years ago, ~50k+ rows, total_spend in the hundreds of thousands
- `ad_campaign_skus` earliest = ~3 years ago, similar scale
- `product_reviews` count = however many reviews you've ever collected
- `email_campaigns` count = ~52 (1 per week for a year)

## Step 6 — Visit Ad-Umpire on the dashboard

Refresh the app → sidebar **Ad-Umpire** → date range = **Last 3 years**.

You should see:
- Three years of daily spend/revenue on the line chart
- All-time campaigns sorted by ROAS
- Per-SKU table showing money-losing SKUs (sort by "ROAS lowest")

## Step 7 — Test the AI Assistant

In the AI Assistant chat, ask:
- "What did we spend on advertising LED-Slim8 in the last 90 days?"
- "Which campaigns are below 2x ROAS year-to-date?"
- "Compare Q1 2026 vs Q1 2025 ad spend"
- "Which SKUs are losing money in shopping ads?"

The bot should call `get_sku_ad_spend`, `find_campaigns_to_cut`, `compare_ad_periods`, etc. and surface real numbers from 3 years of data.

## Daily cadence after backfill

`slack_loop.sh` already runs:
- google_ads_sync recent --days 7 (daily)
- google_ads_sync per-sku --days 7 (daily, v2.67.105)
- ga4_sync recent --days 7 (daily)
- reviewsio_sync recent --days 30 (daily)
- klaviyo_sync recent --days 7 (daily)
- semrush_sync weekly (weekly)

So once the backfill is complete, history stays current automatically. No action needed.
