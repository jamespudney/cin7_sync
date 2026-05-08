#!/usr/bin/env bash
# weekend_backfill.sh — sequential backfill of all marketing data
# (v2.67.108)
#
# Why: running 5 backfill jobs in parallel via nohup pushed the
# worker over its 2GB memory cap, causing Render to OOM-kill the
# instance mid-flight. This script runs them ONE AT A TIME so
# memory pressure stays low.
#
# Usage (run from worker shell):
#   chmod +x weekend_backfill.sh
#   nohup ./weekend_backfill.sh > /dev/null 2>&1 &
#   echo "PID: $!"
#
# Then disconnect, walk away, come back hours/days later.
#
# All output -> /data/output/backfill_weekend.log
#
# Idempotent — re-runnable. Each underlying sync UPSERTs on
# natural keys so duplicate runs just rewrite same data.

set -uo pipefail

LOG="${DATA_DIR:-/data}/output/backfill_weekend.log"
mkdir -p "$(dirname "$LOG")"

stamp() { date -u +"%Y-%m-%dT%H:%M:%SZ"; }

{
  echo ""
  echo "============================================================"
  echo "=== WEEKEND BACKFILL START: $(stamp) ==="
  echo "============================================================"
} >> "$LOG"

# Wait 60s at start so any pending Render deploy lands before
# we begin (otherwise the deploy will OOM-kill us on restart).
echo ">>> [settle] sleeping 60s for any pending deploy to land" >> "$LOG"
sleep 60

# ---------------------------------------------------------------------------
# 1. Klaviyo (~2 min, small)
# ---------------------------------------------------------------------------
echo "" >> "$LOG"
echo ">>> [1/5] klaviyo_sync recent --days 365 @ $(stamp)" >> "$LOG"
python klaviyo_sync.py recent --days 365 >> "$LOG" 2>&1 || \
  echo "<<< klaviyo errored (continuing)" >> "$LOG"
echo "<<< [1/5] klaviyo done @ $(stamp)" >> "$LOG"

# Klaviyo metrics dump — diagnostic for Monday
echo "" >> "$LOG"
echo ">>> [diag] dumping all Klaviyo metrics @ $(stamp)" >> "$LOG"
python <<'PYEOF' >> "$LOG" 2>&1
import os, requests
key = os.environ.get("KLAVIYO_API_KEY", "")
if not key:
    print("KLAVIYO_API_KEY not set; skip")
else:
    url = "https://a.klaviyo.com/api/metrics/"
    hdrs = {"Authorization": f"Klaviyo-API-Key {key}",
            "accept": "application/vnd.api+json",
            "revision": "2024-10-15"}
    out = []
    page = 0
    while url and page < 20:
        page += 1
        params = {"page[size]": 100} if page == 1 else None
        r = requests.get(url, headers=hdrs, params=params, timeout=30)
        if r.status_code != 200:
            print(f"page {page}: HTTP {r.status_code} {r.text[:300]}")
            break
        data = r.json()
        for m in data.get("data", []):
            out.append((m.get("id"),
                          (m.get("attributes") or {}).get("name")))
        url = (data.get("links") or {}).get("next")
    print(f"Total metrics found: {len(out)}")
    for mid, name in out:
        print(f"  {mid}: {name}")
PYEOF
echo "<<< [diag] klaviyo metrics dump done @ $(stamp)" >> "$LOG"

# ---------------------------------------------------------------------------
# 2. Reviews.io (~5 min)
# ---------------------------------------------------------------------------
echo "" >> "$LOG"
echo ">>> [2/5] reviewsio_sync full @ $(stamp)" >> "$LOG"
python reviewsio_sync.py full >> "$LOG" 2>&1 || \
  echo "<<< reviewsio errored (continuing)" >> "$LOG"
echo "<<< [2/5] reviewsio done @ $(stamp)" >> "$LOG"

# Reviews.io endpoint diagnostic — try multiple endpoint paths so
# we can see what each returns (helps Monday debugging).
echo "" >> "$LOG"
echo ">>> [diag] testing Reviews.io endpoint variants @ $(stamp)" >> "$LOG"
python <<'PYEOF' >> "$LOG" 2>&1
import os, requests
url = os.environ.get("REVIEWSIO_API_URL", "https://api.reviews.io")
store = os.environ.get("REVIEWSIO_STORE_ID", "")
key = os.environ.get("REVIEWSIO_API_KEY", "")
if not store or not key:
    print("REVIEWSIO_STORE_ID or REVIEWSIO_API_KEY not set; skip")
else:
    hdrs = {"Accept": "application/json",
            "store": store, "apikey": key}
    paths = ["/merchant/v3/reviews",
             "/merchant/v2.6/products/reviews",
             "/api/products/reviews",
             "/product/review",
             "/api/v1/product/review"]
    for p in paths:
        try:
            r = requests.get(f"{url}{p}", headers=hdrs,
                             params={"store": store,
                                     "page": 1, "per_page": 5},
                             timeout=15)
            print(f"{p}: HTTP {r.status_code} body={r.text[:300]!r}")
        except Exception as e:
            print(f"{p}: {type(e).__name__}: {e}")
PYEOF
echo "<<< [diag] reviewsio endpoint test done @ $(stamp)" >> "$LOG"

# ---------------------------------------------------------------------------
# 3. GA4 (~15 min, 12 chunks of 90 days)
# ---------------------------------------------------------------------------
echo "" >> "$LOG"
echo ">>> [3/5] ga4_sync backfill --days 1095 @ $(stamp)" >> "$LOG"
python ga4_sync.py backfill --days 1095 >> "$LOG" 2>&1 || \
  echo "<<< ga4 errored (continuing)" >> "$LOG"
echo "<<< [3/5] ga4 done @ $(stamp)" >> "$LOG"

# ---------------------------------------------------------------------------
# 4. Google Ads campaign-level (~5 min, 3 chunks of 365 days)
# ---------------------------------------------------------------------------
echo "" >> "$LOG"
echo ">>> [4/5] google_ads_sync full --days 1095 @ $(stamp)" >> "$LOG"
python google_ads_sync.py full --days 1095 >> "$LOG" 2>&1 || \
  echo "<<< google_ads errored (continuing)" >> "$LOG"
echo "<<< [4/5] google_ads done @ $(stamp)" >> "$LOG"

# ---------------------------------------------------------------------------
# 5. Google Ads per-SKU shopping spend (~10 min, 3 chunks)
# ---------------------------------------------------------------------------
echo "" >> "$LOG"
echo ">>> [5/5] google_ads_sync per-sku-backfill --days 1095 @ $(stamp)" >> "$LOG"
python google_ads_sync.py per-sku-backfill --days 1095 >> "$LOG" 2>&1 || \
  echo "<<< google_ads per-sku errored (continuing)" >> "$LOG"
echo "<<< [5/5] google_ads per-sku done @ $(stamp)" >> "$LOG"

# ---------------------------------------------------------------------------
# DONE — print final row counts
# ---------------------------------------------------------------------------
echo "" >> "$LOG"
echo ">>> [final] DB row counts @ $(stamp)" >> "$LOG"
sqlite3 "${DATA_DIR:-/data}/team_actions.db" >> "$LOG" 2>&1 <<'SQL'
SELECT 'ad_campaigns_daily ', COUNT(*),
       MIN(date), MAX(date) FROM ad_campaigns_daily;
SELECT 'ad_campaign_skus   ', COUNT(*),
       MIN(date), MAX(date) FROM ad_campaign_skus;
SELECT 'product_reviews    ', COUNT(*),
       MIN(review_date), MAX(review_date) FROM product_reviews;
SELECT 'email_campaigns    ', COUNT(*),
       MIN(sent_at), MAX(sent_at) FROM email_campaigns;
SQL

{
  echo ""
  echo "============================================================"
  echo "=== ALL DONE: $(stamp) ==="
  echo "============================================================"
} >> "$LOG"
