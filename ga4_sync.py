"""ga4_sync.py (v2.67.97)
==============================

Pull GA4 ecommerce events into local SQLite for ad-campaign
attribution.

Why this exists
---------------
Google Ads' self-reported conversion numbers are notoriously
inflated (view-through attribution, lifetime models). GA4 with
data-driven attribution is the conservative ground truth most
businesses use to make budget decisions. This script pulls per-
campaign + per-product GA4 metrics so the bot can reconcile:

  Google Ads says: "April PMax converted at 8.3x ROAS"
  GA4 says:       "April PMax actually 3.4x ROAS via DDA"
  Bot answer:     "Google Ads inflated by 2.4x. Treat the GA4
                   number as your decision baseline."

Two queries:
  1. Campaign-level daily totals (purchases, revenue) by source/
     campaign — merges INTO ad_campaigns_daily.conv_ga4 +
     revenue_ga4.
  2. Per-product daily ecommerce events (item_views, add_to_carts,
     purchases) by source/campaign — writes to ad_campaign_skus.

Cost model: free (read-only API; limits 200,000 tokens/day per
property).

CLI:
  python ga4_sync.py recent --days 7        # rolling daily refresh
  python ga4_sync.py recent --days 30       # backfill / catch-up
  python ga4_sync.py campaign-totals --days 7
  python ga4_sync.py per-sku --days 7

Env vars required:
  GA4_PROPERTY_ID            numeric, from GA4 admin
  GOOGLE_ADS_CLIENT_ID       same OAuth client as Google Ads
  GOOGLE_ADS_CLIENT_SECRET
  GOOGLE_ADS_REFRESH_TOKEN

(GA4 OAuth shares with Google Ads when both scopes were authorised
during the OAuth Playground step.)

Optional:
  GA4_THROTTLE_S             default 0.3

API reference:
  https://developers.google.com/analytics/devguides/reporting/data/v1
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))
import db  # noqa: E402

LOG_FORMAT = "%(asctime)s [%(levelname)s] %(message)s"
log = logging.getLogger("ga4_sync")


# ---------------------------------------------------------------------------
# OAuth — share with Google Ads
# ---------------------------------------------------------------------------
def _refresh_access_token(client_id: str, client_secret: str,
                              refresh_token: str
                              ) -> Optional[str]:
    try:
        r = requests.post(
            "https://oauth2.googleapis.com/token",
            data={
                "client_id": client_id,
                "client_secret": client_secret,
                "refresh_token": refresh_token,
                "grant_type": "refresh_token",
            },
            timeout=20,
        )
    except requests.RequestException as exc:
        log.error("OAuth token refresh network error: %s", exc)
        return None
    if r.status_code != 200:
        log.error("OAuth token refresh failed (%d): %s",
                    r.status_code, r.text[:300])
        return None
    return r.json().get("access_token")


# ---------------------------------------------------------------------------
# GA4 Data API client
# ---------------------------------------------------------------------------
class GA4Client:
    """Minimal GA4 Data API client. Uses :runReport endpoint."""

    BASE = "https://analyticsdata.googleapis.com/v1beta"

    def __init__(self, property_id: str, access_token: str,
                  throttle_s: float = 0.3):
        if not property_id:
            raise RuntimeError("GA4_PROPERTY_ID required")
        self.property_id = property_id.strip()
        self.access_token = access_token
        self.throttle_s = throttle_s
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        })
        self._last_call = 0.0

    def _throttle(self) -> None:
        elapsed = time.time() - self._last_call
        if elapsed < self.throttle_s:
            time.sleep(self.throttle_s - elapsed)
        self._last_call = time.time()

    def run_report(self, body: dict) -> Optional[dict]:
        url = (f"{self.BASE}/properties/{self.property_id}:runReport")
        for attempt in range(3):
            self._throttle()
            try:
                r = self.session.post(url, json=body, timeout=60)
            except requests.RequestException as exc:
                log.warning("network error: %s; retry %d",
                              exc, attempt)
                time.sleep(2 ** attempt)
                continue
            if r.status_code == 429:
                wait = int(r.headers.get("Retry-After", "5") or 5)
                log.warning("429 throttled; sleeping %ds", wait)
                time.sleep(wait)
                continue
            if r.status_code != 200:
                log.error("GA4 API HTTP %d: %s",
                            r.status_code, r.text[:500])
                return None
            try:
                return r.json()
            except Exception as exc:
                log.error("GA4 JSON parse failed: %s", exc)
                return None
        return None


# ---------------------------------------------------------------------------
# Report flows
# ---------------------------------------------------------------------------
def _ga4_rows(payload: dict) -> List[dict]:
    """Flatten a GA4 runReport response into a list of dicts.
    GA4 returns dimensions and metrics as parallel arrays per row."""
    if not payload:
        return []
    dim_headers = [h["name"] for h in payload.get(
        "dimensionHeaders") or []]
    met_headers = [h["name"] for h in payload.get(
        "metricHeaders") or []]
    rows = payload.get("rows") or []
    out: List[dict] = []
    for r in rows:
        d = {}
        for i, dh in enumerate(dim_headers):
            dvals = r.get("dimensionValues") or []
            d[dh] = dvals[i].get("value") if i < len(dvals) else None
        for i, mh in enumerate(met_headers):
            mvals = r.get("metricValues") or []
            d[mh] = mvals[i].get("value") if i < len(mvals) else None
        out.append(d)
    return out


def sync_campaign_totals(client: GA4Client, days: int) -> dict:
    """Per-campaign daily totals: conversions + revenue from GA4
    events, attributed to googleAds source."""
    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=days)
    body = {
        "dateRanges": [{
            "startDate": start.isoformat(),
            "endDate": end.isoformat(),
        }],
        "dimensions": [
            {"name": "date"},
            {"name": "sessionGoogleAdsCampaignId"},
            {"name": "sessionGoogleAdsCampaignName"},
            {"name": "sessionSource"},
            {"name": "sessionMedium"},
        ],
        "metrics": [
            {"name": "sessions"},
            {"name": "conversions"},
            {"name": "purchaseRevenue"},
            {"name": "transactions"},
        ],
        "dimensionFilter": {
            "filter": {
                "fieldName": "sessionMedium",
                "stringFilter": {
                    "matchType": "EXACT",
                    "value": "cpc",
                },
            },
        },
        "limit": 100000,
    }
    log.info("GA4 campaign-totals %s -> %s",
              start.isoformat(), end.isoformat())
    payload = client.run_report(body)
    rows = _ga4_rows(payload)
    log.info("Got %d GA4 rows", len(rows))

    # GA4 returns the campaign name + id we use to merge into
    # ad_campaigns_daily. Note: GA4's date is YYYYMMDD; need to
    # reformat.
    n_written = 0
    n_skipped = 0
    for r in rows:
        cid = r.get("sessionGoogleAdsCampaignId")
        if not cid or cid == "(not set)":
            n_skipped += 1
            continue
        date_raw = r.get("date") or ""
        if len(date_raw) == 8:
            iso_date = (f"{date_raw[:4]}-{date_raw[4:6]}-"
                          f"{date_raw[6:8]}")
        else:
            iso_date = date_raw
        # Detect platform from source — googleAds vs facebook etc.
        source = (r.get("sessionSource") or "").lower()
        platform = ("google_ads" if "google" in source
                      else "meta" if any(s in source for s in
                                            ("facebook", "instagram",
                                              "meta"))
                      else source or "google_ads")
        try:
            row = {
                "platform": platform,
                "campaign_id": str(cid),
                "campaign_name": r.get(
                    "sessionGoogleAdsCampaignName"),
                "campaign_type": None,
                "date": iso_date,
                "spend": 0.0,  # populated by google_ads_sync
                "impressions": None,
                "clicks": None,
                "conv_platform": None,
                "conv_ga4": float(r.get("conversions") or 0),
                "revenue_platform": None,
                "revenue_ga4": float(r.get("purchaseRevenue") or 0),
                "captured_at": datetime.now(
                    timezone.utc).isoformat(),
            }
            db.upsert_ad_campaign_daily(row)
            n_written += 1
        except Exception as exc:
            log.error("upsert failed: %s", exc)
            n_skipped += 1

    return {"written": n_written, "skipped": n_skipped,
              "from": start.isoformat(), "to": end.isoformat()}


def sync_per_sku(client: GA4Client, days: int) -> dict:
    """Per-product daily ecommerce metrics by campaign. Uses GA4's
    item-scoped dimensions (itemId is the Shopify variant id /
    SKU).

    v2.67.100 — split into two reports because GA4 doesn't allow
    combining event-scoped metrics (itemViewEvents, addToCarts)
    with item-scoped dimensions (itemId). Report A pulls per-SKU
    purchase quantity + revenue (item-scoped, OK with itemId).
    Report B pulls campaign-level view + add-to-cart counts
    (event-scoped, no itemId)."""
    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=days)

    # --- Report A: per-SKU purchases + revenue ---
    body_a = {
        "dateRanges": [{
            "startDate": start.isoformat(),
            "endDate": end.isoformat(),
        }],
        "dimensions": [
            {"name": "date"},
            {"name": "sessionGoogleAdsCampaignId"},
            {"name": "itemId"},
        ],
        "metrics": [
            {"name": "itemPurchaseQuantity"},
            {"name": "itemRevenue"},
        ],
        "dimensionFilter": {
            "filter": {
                "fieldName": "sessionMedium",
                "stringFilter": {
                    "matchType": "EXACT",
                    "value": "cpc",
                },
            },
        },
        "limit": 100000,
    }
    log.info("GA4 per-sku purchases %s -> %s",
              start.isoformat(), end.isoformat())
    payload = client.run_report(body_a)
    rows = _ga4_rows(payload)
    log.info("Got %d per-sku rows (purchases/revenue)", len(rows))

    # Look up family from product_dimensions for each SKU
    handle_lookup: Dict[str, dict] = {}
    for r in db.all_product_dimensions():
        h = (r.get("shopify_handle") or "").strip()
        if h:
            handle_lookup[h] = {"family": r.get("family") or ""}

    n_written = 0
    n_skipped = 0
    for r in rows:
        cid = r.get("sessionGoogleAdsCampaignId")
        sku = r.get("itemId")
        if not cid or cid == "(not set)" or not sku:
            n_skipped += 1
            continue
        date_raw = r.get("date") or ""
        if len(date_raw) == 8:
            iso_date = (f"{date_raw[:4]}-{date_raw[4:6]}-"
                          f"{date_raw[6:8]}")
        else:
            iso_date = date_raw
        # Family lookup — GA4 itemId might be a variant SKU.
        # Best-effort: if SKU starts with LED-FAMILY-, derive family.
        family = ""
        s = sku.upper()
        if s.startswith("LED-"):
            parts = s.split("-")
            if len(parts) >= 2:
                family = parts[1]
        elif s.startswith("LEDKIT-"):
            parts = s.split("-")
            if len(parts) >= 2:
                family = f"KIT-{parts[1]}"
        try:
            row = {
                "platform": "google_ads",
                "campaign_id": str(cid),
                "date": iso_date,
                "sku": sku,
                "family": family,
                # v2.67.100 — item_views + add_to_carts come from
                # a separate report (not item-scoped). Left as None
                # here; populated by sync_funnel_by_campaign if
                # added later.
                "item_views": None,
                "add_to_carts": None,
                "purchases": int(float(
                    r.get("itemPurchaseQuantity") or 0)),
                "revenue": float(r.get("itemRevenue") or 0),
                "captured_at": datetime.now(
                    timezone.utc).isoformat(),
            }
            db.upsert_ad_campaign_sku(row)
            n_written += 1
        except Exception as exc:
            log.error("upsert per-sku failed: %s", exc)
            n_skipped += 1

    return {"written": n_written, "skipped": n_skipped,
              "from": start.isoformat(), "to": end.isoformat()}


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def _setup_log(verbose: bool = False) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format=LOG_FORMAT,
        stream=sys.stdout,
        force=True,
    )


def _make_client() -> GA4Client:
    property_id = os.environ.get("GA4_PROPERTY_ID", "").strip()
    client_id = os.environ.get("GOOGLE_ADS_CLIENT_ID", "").strip()
    client_secret = os.environ.get(
        "GOOGLE_ADS_CLIENT_SECRET", "").strip()
    refresh_token = os.environ.get(
        "GOOGLE_ADS_REFRESH_TOKEN", "").strip()

    missing = []
    for name, val in [
        ("GA4_PROPERTY_ID", property_id),
        ("GOOGLE_ADS_CLIENT_ID", client_id),
        ("GOOGLE_ADS_CLIENT_SECRET", client_secret),
        ("GOOGLE_ADS_REFRESH_TOKEN", refresh_token),
    ]:
        if not val:
            missing.append(name)
    if missing:
        raise SystemExit(
            "Missing env vars: " + ", ".join(missing)
            + ". See GOOGLE_ADS_SETUP.md.")

    log.info("Refreshing OAuth access token...")
    access_token = _refresh_access_token(
        client_id, client_secret, refresh_token)
    if not access_token:
        raise SystemExit(
            "OAuth token refresh failed. The same OAuth client must "
            "have been authorised for the analytics.readonly scope. "
            "See GOOGLE_ADS_SETUP.md step 2.")

    return GA4Client(
        property_id=property_id,
        access_token=access_token,
        throttle_s=float(os.environ.get("GA4_THROTTLE_S", "0.3")))


def cmd_recent(args: argparse.Namespace) -> int:
    _setup_log(args.verbose)
    client = _make_client()
    log.info("=== campaign totals ===")
    r1 = sync_campaign_totals(client, args.days)
    log.info("DONE: %s", r1)
    log.info("=== per-sku ===")
    r2 = sync_per_sku(client, args.days)
    log.info("DONE: %s", r2)
    return 0


def cmd_campaign_totals(args: argparse.Namespace) -> int:
    _setup_log(args.verbose)
    client = _make_client()
    result = sync_campaign_totals(client, args.days)
    log.info("DONE: %s", result)
    return 0


def cmd_per_sku(args: argparse.Namespace) -> int:
    _setup_log(args.verbose)
    client = _make_client()
    result = sync_per_sku(client, args.days)
    log.info("DONE: %s", result)
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Sync GA4 ecommerce events into local DB")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_r = sub.add_parser("recent",
                            help="Pull both campaign totals + per-sku")
    p_r.add_argument("--days", type=int, default=7)
    p_r.add_argument("--verbose", action="store_true")
    p_r.set_defaults(func=cmd_recent)

    p_c = sub.add_parser("campaign-totals",
                            help="Just campaign daily totals")
    p_c.add_argument("--days", type=int, default=7)
    p_c.add_argument("--verbose", action="store_true")
    p_c.set_defaults(func=cmd_campaign_totals)

    p_s = sub.add_parser("per-sku",
                            help="Just per-SKU ecommerce events")
    p_s.add_argument("--days", type=int, default=7)
    p_s.add_argument("--verbose", action="store_true")
    p_s.set_defaults(func=cmd_per_sku)

    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
