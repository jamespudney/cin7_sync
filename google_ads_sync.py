"""google_ads_sync.py (v2.67.97)
=====================================

Pull Google Ads campaign + per-SKU spend data into local SQLite.

Why this exists
---------------
Triple Whale's Moby chat let you ask 'which campaigns to scale,
hold, or cut?' on top of Google Ads + GA4 data. We're cancelling
TW June 1; this script pulls the same campaign-level metrics
directly from Google Ads API, with GA4 attribution layered in via
ga4_sync.py.

Together with reviews.io / klaviyo / semrush data, the bot answers:
  - 'Compare April vs March campaign efficiency by type'
  - 'Which Google Ads campaigns are below 2.0x ROAS?'
  - 'Reconcile Google's reported conversions vs GA4 reality'
  - 'Sum up branded search spend across all campaigns'

Cost model: free (read-only API; included in Google Ads plan).

CLI:
  python google_ads_sync.py recent --days 30   # rolling daily refresh
  python google_ads_sync.py full --days 1825   # 5-year backfill
  python google_ads_sync.py one --campaign-id X  # debug one

Env vars required:
  GOOGLE_ADS_DEVELOPER_TOKEN
  GOOGLE_ADS_CLIENT_ID
  GOOGLE_ADS_CLIENT_SECRET
  GOOGLE_ADS_REFRESH_TOKEN
  GOOGLE_ADS_CUSTOMER_ID         (10-digit, no dashes)

Optional:
  GOOGLE_ADS_LOGIN_CUSTOMER_ID   (MCC ID if customer is sub-account)

Setup walkthrough lives in GOOGLE_ADS_SETUP.md (next to this file).

API reference:
  https://developers.google.com/google-ads/api/docs/start
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
log = logging.getLogger("google_ads_sync")


# ---------------------------------------------------------------------------
# OAuth token refresh
# ---------------------------------------------------------------------------
def _refresh_access_token(client_id: str, client_secret: str,
                              refresh_token: str
                              ) -> Optional[str]:
    """Exchange the long-lived refresh token for a short-lived
    access token (1-hour TTL). Called at the start of each run."""
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
# Google Ads API client
# ---------------------------------------------------------------------------
class GoogleAdsClient:
    """Minimal Google Ads API client that uses GAQL search via REST.

    Avoids the heavy google-ads-python SDK dependency. Sends GAQL
    queries to /googleAds:search and /googleAds:searchStream. Works
    for the read-only data we need (campaigns, spend, performance).

    v2.67.99 — version made configurable. Google Ads API releases
    a new version each quarter and deprecates old ones ~2 years
    later. Set GOOGLE_ADS_API_VERSION env var to bump without code
    change. Default tracked to a recent stable version."""

    DEFAULT_VERSION = "v19"
    _BASE_TEMPLATE = "https://googleads.googleapis.com/{ver}"

    @classmethod
    def _resolved_base(cls) -> str:
        ver = os.environ.get(
            "GOOGLE_ADS_API_VERSION", cls.DEFAULT_VERSION).strip()
        if not ver.startswith("v"):
            ver = "v" + ver
        return cls._BASE_TEMPLATE.format(ver=ver)

    @property
    def BASE(self) -> str:
        return self._resolved_base()

    def __init__(self, customer_id: str, access_token: str,
                  developer_token: str,
                  login_customer_id: Optional[str] = None):
        if not customer_id:
            raise RuntimeError("GOOGLE_ADS_CUSTOMER_ID required")
        # Strip dashes if user pasted '123-456-7890'
        self.customer_id = customer_id.replace("-", "").strip()
        self.access_token = access_token
        self.developer_token = developer_token
        self.login_customer_id = (login_customer_id or "").replace(
            "-", "").strip()
        self.session = requests.Session()
        headers = {
            "Authorization": f"Bearer {access_token}",
            "developer-token": developer_token,
            "Content-Type": "application/json",
        }
        if self.login_customer_id:
            headers["login-customer-id"] = self.login_customer_id
        self.session.headers.update(headers)
        self._last_call = 0.0

    def _throttle(self) -> None:
        elapsed = time.time() - self._last_call
        if elapsed < 0.2:
            time.sleep(0.2 - elapsed)
        self._last_call = time.time()

    def search_stream(self, gaql: str) -> List[dict]:
        """GAQL search via the streaming endpoint. Returns the
        accumulated results across all pages."""
        url = (f"{self.BASE}/customers/{self.customer_id}/"
                 f"googleAds:searchStream")
        self._throttle()
        try:
            r = self.session.post(url, json={"query": gaql},
                                       timeout=60)
        except requests.RequestException as exc:
            log.error("Google Ads API network error: %s", exc)
            return []
        if r.status_code != 200:
            log.error("Google Ads API HTTP %d: %s",
                        r.status_code, r.text[:500])
            return []
        # searchStream returns an array of GoogleAdsSearchResponse
        # objects. Concatenate all 'results' lists.
        try:
            payload = r.json()
        except Exception as exc:
            log.error("Google Ads API JSON parse failed: %s", exc)
            return []
        if not isinstance(payload, list):
            payload = [payload]
        out: List[dict] = []
        for chunk in payload:
            out.extend(chunk.get("results") or [])
        return out


# ---------------------------------------------------------------------------
# GAQL queries
# ---------------------------------------------------------------------------
_CAMPAIGN_DAILY_GAQL = """
SELECT
  campaign.id,
  campaign.name,
  campaign.advertising_channel_type,
  campaign.advertising_channel_sub_type,
  segments.date,
  metrics.cost_micros,
  metrics.impressions,
  metrics.clicks,
  metrics.conversions,
  metrics.conversions_value,
  metrics.all_conversions,
  metrics.all_conversions_value
FROM campaign
WHERE segments.date BETWEEN '{start}' AND '{end}'
  AND campaign.status != 'REMOVED'
"""


def _parse_campaign_daily_row(r: dict) -> dict:
    """GoogleAdsRow -> our DB schema."""
    camp = r.get("campaign") or {}
    seg = r.get("segments") or {}
    m = r.get("metrics") or {}
    cost_micros = int(m.get("costMicros", 0) or 0)
    return {
        "platform": "google_ads",
        "campaign_id": str(camp.get("id") or ""),
        "campaign_name": camp.get("name"),
        "campaign_type": (
            camp.get("advertisingChannelSubType")
            or camp.get("advertisingChannelType")
            or "").upper(),
        "date": seg.get("date"),
        "spend": cost_micros / 1e6,
        "impressions": int(m.get("impressions", 0) or 0),
        "clicks": int(m.get("clicks", 0) or 0),
        "conv_platform": float(m.get("conversions", 0) or 0),
        # GA4-attributed values come from ga4_sync.py and merge later
        "conv_ga4": None,
        "revenue_platform": float(m.get("conversionsValue", 0) or 0),
        "revenue_ga4": None,
        "captured_at": datetime.now(timezone.utc).isoformat(),
    }


def sync_range(client: GoogleAdsClient,
                  start_date,
                  end_date) -> dict:
    """Pull campaign daily metrics for an explicit date range.
    Both args are date objects. The Google Ads API caps date
    ranges at 365 days per query — caller chunks if needed."""
    gaql = _CAMPAIGN_DAILY_GAQL.format(
        start=start_date.isoformat(), end=end_date.isoformat())
    log.info("Pulling campaign daily metrics %s -> %s",
              start_date.isoformat(), end_date.isoformat())
    results = client.search_stream(gaql)
    log.info("Got %d row(s) from Google Ads API", len(results))

    n_written = 0
    n_skipped = 0
    for r in results:
        row = _parse_campaign_daily_row(r)
        if not row.get("campaign_id") or not row.get("date"):
            n_skipped += 1
            continue
        try:
            db.upsert_ad_campaign_daily(row)
            n_written += 1
        except Exception as exc:
            log.error("upsert failed: %s", exc)
            n_skipped += 1

    return {"written": n_written, "skipped": n_skipped,
              "from": start_date.isoformat(),
              "to": end_date.isoformat()}


def sync_recent(client: GoogleAdsClient, days: int) -> dict:
    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=days)
    return sync_range(client, start, end)


# ---------------------------------------------------------------------------
# v2.67.105 — Per-SKU spend from Shopping campaigns + PMax
# ---------------------------------------------------------------------------
# shopping_performance_view exposes per-product daily metrics for
# Shopping AND the shopping component of Performance Max campaigns.
# Without this we can only see CAMPAIGN-level spend; with this we
# get per-SKU spend so the buyer can see what they're paying to
# advertise each product, and compute per-SKU ROAS.

_PER_SKU_GAQL = """
SELECT
  segments.product_item_id,
  segments.product_title,
  segments.date,
  campaign.id,
  campaign.name,
  campaign.advertising_channel_type,
  metrics.cost_micros,
  metrics.impressions,
  metrics.clicks,
  metrics.conversions,
  metrics.conversions_value
FROM shopping_performance_view
WHERE segments.date BETWEEN '{start}' AND '{end}'
"""


def _derive_family_from_sku(sku: str) -> str:
    """Best-effort family extraction from variant SKU prefix."""
    if not sku:
        return ""
    s = sku.upper()
    if s.startswith("LED-"):
        parts = s.split("-")
        if len(parts) >= 2:
            return parts[1]
    if s.startswith("LEDKIT-"):
        parts = s.split("-")
        if len(parts) >= 2:
            return f"KIT-{parts[1]}"
    return ""


def _parse_per_sku_row(r: dict) -> dict:
    seg = r.get("segments") or {}
    camp = r.get("campaign") or {}
    m = r.get("metrics") or {}
    sku = (seg.get("productItemId") or "").strip()
    return {
        "platform": "google_ads",
        "campaign_id": str(camp.get("id") or ""),
        "date": seg.get("date"),
        "sku": sku,
        "family": _derive_family_from_sku(sku),
        # ga4_sync owns these — pass None so COALESCE preserves
        # whatever GA4 wrote (item_views, add_to_carts, purchases,
        # revenue).
        "item_views": None,
        "add_to_carts": None,
        "purchases": None,
        "revenue": None,
        # google_ads owns these
        "spend": int(m.get("costMicros", 0) or 0) / 1e6,
        "impressions": int(m.get("impressions", 0) or 0),
        "clicks": int(m.get("clicks", 0) or 0),
        "captured_at": datetime.now(timezone.utc).isoformat(),
    }


def sync_per_sku_range(client: GoogleAdsClient,
                          start_date,
                          end_date) -> dict:
    """Pull per-SKU spend / clicks / impressions for an explicit
    date range from shopping_performance_view."""
    gaql = _PER_SKU_GAQL.format(
        start=start_date.isoformat(), end=end_date.isoformat())
    log.info("Pulling per-SKU shopping metrics %s -> %s",
              start_date.isoformat(), end_date.isoformat())
    results = client.search_stream(gaql)
    log.info("Got %d shopping_performance_view rows", len(results))

    n_written = 0
    n_skipped = 0
    for r in results:
        row = _parse_per_sku_row(r)
        if not row.get("sku") or not row.get("date") \
                or not row.get("campaign_id"):
            n_skipped += 1
            continue
        try:
            db.upsert_ad_campaign_sku(row)
            n_written += 1
        except Exception as exc:
            log.error("upsert per-sku spend failed: %s", exc)
            n_skipped += 1

    return {"written": n_written, "skipped": n_skipped,
              "from": start_date.isoformat(),
              "to": end_date.isoformat()}


def sync_per_sku_recent(client: GoogleAdsClient,
                            days: int) -> dict:
    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=days)
    return sync_per_sku_range(client, start, end)


def sync_per_sku_backfill(client: GoogleAdsClient,
                              days: int,
                              chunk_days: int = 365
                              ) -> dict:
    """Multi-year backfill of per-SKU shopping spend, chunked."""
    today = datetime.now(timezone.utc).date()
    days_remaining = days
    cursor_end = today
    chunk_no = 0
    total_written = 0
    total_skipped = 0
    while days_remaining > 0:
        chunk_no += 1
        size = min(days_remaining, chunk_days)
        chunk_start = cursor_end - timedelta(days=size)
        log.info("=== per-sku chunk %d (%s -> %s) ===",
                  chunk_no, chunk_start.isoformat(),
                  cursor_end.isoformat())
        result = sync_per_sku_range(
            client, chunk_start, cursor_end)
        total_written += result["written"]
        total_skipped += result["skipped"]
        cursor_end = chunk_start - timedelta(days=1)
        days_remaining -= size
    return {
        "total_written": total_written,
        "total_skipped": total_skipped,
        "chunks": chunk_no,
        "earliest": cursor_end.isoformat(),
        "latest": today.isoformat(),
    }


def sync_backfill(client: GoogleAdsClient, days: int,
                     chunk_days: int = 365) -> dict:
    """Walk backwards through time in chunk_days windows. Lets us
    backfill multi-year history despite Google Ads API's 365-day
    per-query limit. v2.67.103."""
    today = datetime.now(timezone.utc).date()
    days_remaining = days
    cursor_end = today
    total_written = 0
    total_skipped = 0
    chunk_no = 0

    while days_remaining > 0:
        chunk_no += 1
        size = min(days_remaining, chunk_days)
        chunk_start = cursor_end - timedelta(days=size)
        log.info("=== chunk %d (%s -> %s, %d days) ===",
                  chunk_no, chunk_start.isoformat(),
                  cursor_end.isoformat(), size)
        result = sync_range(client, chunk_start, cursor_end)
        total_written += result["written"]
        total_skipped += result["skipped"]
        cursor_end = chunk_start - timedelta(days=1)
        days_remaining -= size

    return {
        "total_written": total_written,
        "total_skipped": total_skipped,
        "chunks": chunk_no,
        "earliest": cursor_end.isoformat(),
        "latest": today.isoformat(),
    }


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


def _make_client() -> GoogleAdsClient:
    customer_id = os.environ.get(
        "GOOGLE_ADS_CUSTOMER_ID", "").strip()
    developer_token = os.environ.get(
        "GOOGLE_ADS_DEVELOPER_TOKEN", "").strip()
    client_id = os.environ.get("GOOGLE_ADS_CLIENT_ID", "").strip()
    client_secret = os.environ.get(
        "GOOGLE_ADS_CLIENT_SECRET", "").strip()
    refresh_token = os.environ.get(
        "GOOGLE_ADS_REFRESH_TOKEN", "").strip()
    login_customer_id = os.environ.get(
        "GOOGLE_ADS_LOGIN_CUSTOMER_ID", "").strip()

    missing = []
    for name, val in [
        ("GOOGLE_ADS_CUSTOMER_ID", customer_id),
        ("GOOGLE_ADS_DEVELOPER_TOKEN", developer_token),
        ("GOOGLE_ADS_CLIENT_ID", client_id),
        ("GOOGLE_ADS_CLIENT_SECRET", client_secret),
        ("GOOGLE_ADS_REFRESH_TOKEN", refresh_token),
    ]:
        if not val:
            missing.append(name)
    if missing:
        raise SystemExit(
            "Missing env vars: " + ", ".join(missing)
            + ". See GOOGLE_ADS_SETUP.md for provisioning steps.")

    log.info("Refreshing Google Ads OAuth access token...")
    access_token = _refresh_access_token(
        client_id, client_secret, refresh_token)
    if not access_token:
        raise SystemExit(
            "OAuth token refresh failed. Verify CLIENT_ID, "
            "CLIENT_SECRET, and REFRESH_TOKEN are correct.")

    return GoogleAdsClient(
        customer_id=customer_id,
        access_token=access_token,
        developer_token=developer_token,
        login_customer_id=login_customer_id or None)


def cmd_recent(args: argparse.Namespace) -> int:
    _setup_log(args.verbose)
    client = _make_client()
    result = sync_recent(client, args.days)
    log.info("DONE: %s", result)
    return 0


def cmd_full(args: argparse.Namespace) -> int:
    """Backfill N days of history, chunking in 365-day windows
    backwards through time. v2.67.103 — fixed multi-year
    backfill (was previously a single chunk + break)."""
    _setup_log(args.verbose)
    client = _make_client()
    result = sync_backfill(client, args.days, chunk_days=365)
    log.info("=" * 60)
    log.info("BACKFILL DONE: %d written | %d skipped | %d chunks "
              "| range %s -> %s",
              result["total_written"], result["total_skipped"],
              result["chunks"], result["earliest"], result["latest"])
    return 0


def cmd_backfill(args: argparse.Namespace) -> int:
    """Alias for cmd_full with explicit chunk control."""
    _setup_log(args.verbose)
    client = _make_client()
    result = sync_backfill(client, args.days,
                              chunk_days=args.chunk_days)
    log.info("=" * 60)
    log.info("BACKFILL DONE: %d written | %d skipped | %d chunks "
              "| range %s -> %s",
              result["total_written"], result["total_skipped"],
              result["chunks"], result["earliest"], result["latest"])
    return 0


def cmd_per_sku(args: argparse.Namespace) -> int:
    """v2.67.105 — daily refresh of per-SKU shopping spend."""
    _setup_log(args.verbose)
    client = _make_client()
    result = sync_per_sku_recent(client, args.days)
    log.info("DONE: %s", result)
    return 0


def cmd_per_sku_backfill(args: argparse.Namespace) -> int:
    """v2.67.105 — multi-year per-SKU shopping spend backfill."""
    _setup_log(args.verbose)
    client = _make_client()
    result = sync_per_sku_backfill(client, args.days,
                                       chunk_days=args.chunk_days)
    log.info("=" * 60)
    log.info("PER-SKU BACKFILL DONE: %d written | %d skipped "
              "| %d chunks | range %s -> %s",
              result["total_written"], result["total_skipped"],
              result["chunks"], result["earliest"], result["latest"])
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Sync Google Ads campaign daily metrics into "
                      "local DB")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_r = sub.add_parser("recent",
                            help="Pull last N days of campaign metrics")
    p_r.add_argument("--days", type=int, default=30)
    p_r.add_argument("--verbose", action="store_true")
    p_r.set_defaults(func=cmd_recent)

    p_f = sub.add_parser("full",
                            help="Backfill N days, chunked in "
                                  "365-day windows")
    p_f.add_argument("--days", type=int, default=1095,
                       help="Days to backfill (default 1095 = 3 years)")
    p_f.add_argument("--verbose", action="store_true")
    p_f.set_defaults(func=cmd_full)

    p_b = sub.add_parser("backfill",
                            help="Same as full, with chunk-size knob")
    p_b.add_argument("--days", type=int, default=1095,
                       help="Days to backfill (default 1095 = 3 years)")
    p_b.add_argument("--chunk-days", type=int, default=365,
                       help="Days per API call (max 365)")
    p_b.add_argument("--verbose", action="store_true")
    p_b.set_defaults(func=cmd_backfill)

    # v2.67.105 — per-SKU shopping spend
    p_ps = sub.add_parser(
        "per-sku",
        help="Pull last N days of per-SKU shopping spend "
              "(shopping_performance_view)")
    p_ps.add_argument("--days", type=int, default=7)
    p_ps.add_argument("--verbose", action="store_true")
    p_ps.set_defaults(func=cmd_per_sku)

    p_pb = sub.add_parser(
        "per-sku-backfill",
        help="Multi-year per-SKU shopping spend backfill")
    p_pb.add_argument("--days", type=int, default=1095)
    p_pb.add_argument("--chunk-days", type=int, default=365)
    p_pb.add_argument("--verbose", action="store_true")
    p_pb.set_defaults(func=cmd_per_sku_backfill)

    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
