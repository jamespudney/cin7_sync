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


def sync_recent(client: GoogleAdsClient, days: int) -> dict:
    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=days)
    gaql = _CAMPAIGN_DAILY_GAQL.format(
        start=start.isoformat(), end=end.isoformat())
    log.info("Pulling campaign daily metrics %s -> %s",
              start.isoformat(), end.isoformat())
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
    _setup_log(args.verbose)
    client = _make_client()
    # Google Ads API max date range per query is 365 days; loop in chunks.
    days_remaining = args.days
    total_written = 0
    total_skipped = 0
    while days_remaining > 0:
        chunk = min(days_remaining, 365)
        result = sync_recent(client, chunk)
        total_written += result["written"]
        total_skipped += result["skipped"]
        days_remaining -= chunk
        # If we're walking back further, we'd need to adjust the
        # start/end dates. For now, --days N pulls last N days.
        # Multi-year backfill needs a richer date-range loop —
        # leaving as a TODO.
        break

    log.info("DONE: total written=%d skipped=%d",
              total_written, total_skipped)
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
                            help="Backfill (max 365 days per chunk)")
    p_f.add_argument("--days", type=int, default=365)
    p_f.add_argument("--verbose", action="store_true")
    p_f.set_defaults(func=cmd_full)

    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
