"""merchant_sync.py (v2.67.118)
=================================

Pull Google Merchant Center product feed status + free-listing
performance into local SQLite.

Why this exists
---------------
Google Ads already gives us per-SKU paid Shopping spend
(google_ads_sync.py). Merchant Center fills two complementary
gaps that Triple Whale could not:

  1. **Feed health** — which SKUs are silently disapproved or
     have warnings on Shopping ads / free listings? Every
     disapproved SKU is paying $0 in spend but COULD be earning
     impressions. High ROI surface.

  2. **Free-listing performance** — the organic side of Google
     Shopping. Clicks + impressions that aren't paid for. Tells
     us which SKUs are popular without ad support.

CLI:
  python merchant_sync.py status                # feed health (cheap)
  python merchant_sync.py performance --days 30 # free-listing perf
  python merchant_sync.py daily                 # both
  python merchant_sync.py backfill --days 1095  # 3-year history

Env vars required:
  GOOGLE_ADS_CLIENT_ID
  GOOGLE_ADS_CLIENT_SECRET
  GOOGLE_ADS_REFRESH_TOKEN     (must have `content` scope)
  GOOGLE_MERCHANT_ID           (your Merchant Center account id)

Optional:
  MERCHANT_THROTTLE_S          seconds between API calls (default 0.3)

API references
--------------
- Content API for Shopping v2.1 (product statuses):
  https://developers.google.com/shopping-content/reference/rest/v2.1
- Merchant Reporting API v1beta (performance reports):
  https://developers.google.com/merchant/api/reference/rest/reports_v1beta/accounts.reports/search
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
from typing import Any, Dict, Iterable, List, Optional

import requests

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))
import db  # noqa: E402

LOG_FORMAT = "%(asctime)s [%(levelname)s] %(message)s"
log = logging.getLogger("merchant_sync")


# ---------------------------------------------------------------------------
# OAuth — reuses google_ads_sync's token-refresh pattern
# ---------------------------------------------------------------------------
def _refresh_access_token(client_id: str, client_secret: str,
                              refresh_token: str
                              ) -> Optional[str]:
    """Exchange long-lived refresh token for a 1-hour access token.
    The refresh token must have been issued with the
    `https://www.googleapis.com/auth/content` scope — adwords-only
    tokens get 403 from the Content API."""
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
# Merchant API client
# ---------------------------------------------------------------------------
class MerchantClient:
    """Minimal Google Merchant Center client.

    Wraps two APIs:
      - Content API for Shopping v2.1 (productstatuses.list)
        endpoint: shoppingcontent.googleapis.com/content/v2.1
      - Merchant Reporting API v1beta (reports.search)
        endpoint: merchantapi.googleapis.com/reports/v1beta
    Both authenticate with the same OAuth `content` scope."""

    CONTENT_BASE = "https://shoppingcontent.googleapis.com/content/v2.1"
    REPORTING_BASE = "https://merchantapi.googleapis.com/reports/v1beta"

    def __init__(self, access_token: str, merchant_id: str,
                  throttle_s: float = 0.3):
        if not access_token:
            raise RuntimeError("Merchant access_token required")
        if not merchant_id:
            raise RuntimeError("GOOGLE_MERCHANT_ID required")
        self.access_token = access_token
        self.merchant_id = str(merchant_id).strip()
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

    def _get(self, url: str, params: Optional[dict] = None
              ) -> Optional[dict]:
        for attempt in range(3):
            self._throttle()
            try:
                r = self.session.get(url, params=params, timeout=30)
            except requests.RequestException as exc:
                log.warning("network error: %s; retry %d", exc, attempt)
                time.sleep(2 ** attempt)
                continue
            if r.status_code == 200:
                return r.json()
            if r.status_code in (429, 500, 502, 503, 504):
                log.warning("HTTP %d on %s; retry %d",
                              r.status_code, url, attempt)
                time.sleep(2 ** attempt)
                continue
            log.error("HTTP %d on %s: %s",
                        r.status_code, url, r.text[:300])
            return None
        return None

    def _post(self, url: str, body: dict
              ) -> Optional[dict]:
        for attempt in range(3):
            self._throttle()
            try:
                r = self.session.post(url, json=body, timeout=60)
            except requests.RequestException as exc:
                log.warning("network error: %s; retry %d", exc, attempt)
                time.sleep(2 ** attempt)
                continue
            if r.status_code == 200:
                return r.json()
            if r.status_code in (429, 500, 502, 503, 504):
                log.warning("HTTP %d on %s; retry %d",
                              r.status_code, url, attempt)
                time.sleep(2 ** attempt)
                continue
            log.error("HTTP %d on %s: %s",
                        r.status_code, url, r.text[:300])
            return None
        return None

    # ----- productstatuses (Content API v2.1) -----
    def iter_product_statuses(self,
                                  page_size: int = 250
                                  ) -> Iterable[dict]:
        """Stream every product's status across all destinations.
        Paginated via `nextPageToken`. Each row contains the offer
        plus its approval/disapproval state per destination."""
        url = (f"{self.CONTENT_BASE}/{self.merchant_id}"
                f"/productstatuses")
        params: Dict[str, Any] = {"maxResults": page_size}
        page = 0
        while True:
            payload = self._get(url, params=params)
            if not payload:
                return
            page += 1
            resources = payload.get("resources") or []
            log.info("productstatuses page %d: %d rows",
                      page, len(resources))
            for r in resources:
                yield r
            token = payload.get("nextPageToken")
            if not token:
                return
            params["pageToken"] = token

    # ----- reports.search (Merchant Reporting API v1beta) -----
    def search_report(self, query: str) -> Iterable[dict]:
        """Run a Reporting-API GAQL-style query. Returns all rows
        via pagination. The query language is documented at
        https://developers.google.com/merchant/api/guides/reports.

        Example: 'SELECT segments.offer_id, metrics.clicks,
                  metrics.impressions FROM ProductPerformanceView
                  WHERE segments.date BETWEEN "2024-01-01" AND
                  "2024-01-31"'."""
        url = (f"{self.REPORTING_BASE}/accounts/{self.merchant_id}"
                f"/reports:search")
        body: Dict[str, Any] = {"query": query}
        page = 0
        while True:
            payload = self._post(url, body)
            if not payload:
                return
            page += 1
            rows = payload.get("results") or []
            log.info("reports.search page %d: %d rows",
                      page, len(rows))
            for r in rows:
                yield r
            token = payload.get("nextPageToken")
            if not token:
                return
            body["pageToken"] = token


# ---------------------------------------------------------------------------
# Status mapping
# ---------------------------------------------------------------------------
def _roll_up_destination_status(
        statuses: List[dict], destination: str) -> str:
    """Find the entry for `destination` in productstatuses' nested
    `destinationStatuses` array and return its approval state.
    Possible values: approved, disapproved, pending, eligible,
    not_eligible. Empty string if destination not present."""
    for ds in statuses or []:
        if (ds.get("destination") or "").lower() == destination.lower():
            # The API uses `status` for the overall destination state.
            return (ds.get("status") or "").lower()
    return ""


def _normalise_issues(issues: List[dict]) -> dict:
    """Squash itemLevelIssues into a compact summary + raw JSON.
    Splits errors vs warnings vs info."""
    n_errors = 0
    n_warnings = 0
    compact = []
    for iss in issues or []:
        sev = (iss.get("servability") or iss.get("severity")
                or "").lower()
        # Content API v2.1 uses 'servability' (disapproved /
        # demoted / unaffected) AND 'resolution' (merchant_action
        # / pending_processing). Treat disapproved as error,
        # demoted as warning.
        if sev == "disapproved" or sev == "error":
            n_errors += 1
        elif sev == "demoted" or sev == "warning":
            n_warnings += 1
        compact.append({
            "code": iss.get("code"),
            "severity": sev,
            "destination": iss.get("destination"),
            "description": iss.get("description"),
            "detail": iss.get("detail"),
            "url": iss.get("documentation"),
        })
    return {
        "n_errors": n_errors,
        "n_warnings": n_warnings,
        "n_issues": len(compact),
        "issues_json": json.dumps(compact) if compact else None,
    }


# ---------------------------------------------------------------------------
# offer_id -> SKU / family resolution
# ---------------------------------------------------------------------------
_SKU_LOOKUP_CACHE: Optional[Dict[str, dict]] = None


def _build_sku_lookup() -> Dict[str, dict]:
    """Build {offer_id_normalised: {sku, family, shopify_handle,
    title}}. Merchant Center's offer_id is typically the Shopify
    SKU (when feeds come from the Google & YouTube Shopify app),
    so we key by SKU.

    Falls back to product_dimensions handle if SKU lookup misses
    (some feeds use shopify_product_id as offer_id)."""
    global _SKU_LOOKUP_CACHE
    if _SKU_LOOKUP_CACHE is not None:
        return _SKU_LOOKUP_CACHE
    out: Dict[str, dict] = {}
    try:
        rows = db.all_product_dimensions()
    except Exception:
        rows = []
    for r in rows:
        sku = (r.get("sku") or "").strip()
        family = r.get("family") or ""
        handle = (r.get("shopify_handle") or "").strip()
        title = r.get("title") or ""
        if sku:
            out[sku] = {
                "sku": sku, "family": family,
                "shopify_handle": handle, "title": title,
            }
            # Also key by lowercase for case-insensitive fallback
            out[sku.lower()] = out[sku]
        if handle:
            out[f"handle:{handle}"] = {
                "sku": sku, "family": family,
                "shopify_handle": handle, "title": title,
            }
    _SKU_LOOKUP_CACHE = out
    return out


def _resolve_offer(offer_id: str, title: str = "",
                       link: str = "") -> dict:
    """Try several key shapes to find the SKU for an offer_id.
    Returns {sku, family, shopify_handle, title}."""
    if not offer_id:
        return {}
    lookup = _build_sku_lookup()
    # Try direct match first
    info = lookup.get(offer_id) or lookup.get(offer_id.lower())
    if info:
        return info
    # Sometimes offer_id has shop:variant_id format
    if ":" in offer_id:
        tail = offer_id.split(":")[-1]
        info = lookup.get(tail) or lookup.get(tail.lower())
        if info:
            return info
    # Fallback: try to extract handle from the product link
    if link and "/products/" in link:
        try:
            handle = (link.split("/products/", 1)[1]
                          .split("/")[0].split("?")[0].strip())
            info = lookup.get(f"handle:{handle}")
            if info:
                return info
        except Exception:
            pass
    return {
        "sku": offer_id,  # Use offer_id as the SKU even if unmapped
        "family": "",
        "shopify_handle": "",
        "title": title or "",
    }


# ---------------------------------------------------------------------------
# Sync flows
# ---------------------------------------------------------------------------
def sync_product_statuses(client: MerchantClient) -> dict:
    """Pull every product's feed-health status into
    product_feed_status. Overwrites each row.

    Cheap operation: one paginated read across the whole catalog.
    ~10k products takes about 60 seconds."""
    log.info("Pulling product statuses from Merchant Center %s",
              client.merchant_id)
    n_written = 0
    n_skipped = 0
    for ps in client.iter_product_statuses(page_size=250):
        offer_id = ps.get("productId") or ps.get("offerId") or ""
        # productId is qualified (online:en:US:SKU123); pull tail
        if ":" in offer_id:
            offer_id = offer_id.split(":")[-1]
        title = ps.get("title") or ""
        link = ps.get("link") or ""
        info = _resolve_offer(offer_id, title=title, link=link)
        if not info.get("sku"):
            n_skipped += 1
            continue
        dest_statuses = ps.get("destinationStatuses") or []
        ads_status = _roll_up_destination_status(
            dest_statuses, "Shopping") or \
            _roll_up_destination_status(
                dest_statuses, "SHOPPING_ADS")
        fl_status = _roll_up_destination_status(
            dest_statuses, "SurfacesAcrossGoogle") or \
            _roll_up_destination_status(
                dest_statuses, "FREE_LISTINGS")
        issues = _normalise_issues(ps.get("itemLevelIssues") or [])
        row = {
            "sku": info["sku"],
            "offer_id": offer_id,
            "family": info.get("family") or "",
            "shopify_handle": info.get("shopify_handle") or "",
            "title": info.get("title") or title,
            "ads_status": ads_status,
            "free_listings_status": fl_status,
            "issues_json": issues["issues_json"],
            "n_issues": issues["n_issues"],
            "n_errors": issues["n_errors"],
            "n_warnings": issues["n_warnings"],
            "last_checked": datetime.now(timezone.utc).isoformat(),
        }
        try:
            db.upsert_product_feed_status(row)
            n_written += 1
        except Exception as exc:
            log.error("upsert feed status for %s failed: %s",
                        offer_id, exc)
            n_skipped += 1
    return {"written": n_written, "skipped": n_skipped}


def sync_free_listing_performance(client: MerchantClient,
                                          days: int = 30
                                          ) -> dict:
    """Pull per-SKU free-listing clicks + impressions for the last
    `days` days. Writes one row per SKU per day to
    ad_campaign_skus with platform='google_merchant' and
    campaign_id='free_listings'.

    Free listings are Google's organic Shopping surface — clicks
    from there are unattributed to any campaign, so the
    'campaign' is synthetic. The UNIQUE(platform, campaign_id,
    date, sku) constraint still works because no other writer
    uses that pair."""
    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=days - 1)
    log.info("Pulling free-listing performance %s -> %s",
              start, end)
    # NOTE: The Merchant Reporting API distinguishes traffic by
    # the `marketing_method` segment: ORGANIC = free listings,
    # ADS = paid Shopping ads. We only want ORGANIC here; paid is
    # already covered by google_ads_sync.
    query = (
        f"SELECT segments.offer_id, segments.date, "
        f"       metrics.clicks, metrics.impressions, "
        f"       metrics.click_through_rate "
        f"FROM ProductPerformanceView "
        f"WHERE segments.date BETWEEN '{start}' AND '{end}' "
        f"  AND segments.marketing_method = 'ORGANIC'")
    n_written = 0
    n_skipped = 0
    for r in client.search_report(query):
        segs = r.get("productPerformanceView") or {}
        # Reporting API returns nested {productPerformanceView:
        # {segments: {...}, metrics: {...}}} OR flat with
        # 'segments' and 'metrics' top-level depending on version.
        segments = (segs.get("segments") if segs
                      else r.get("segments")) or {}
        metrics = (segs.get("metrics") if segs
                      else r.get("metrics")) or {}
        offer_id = segments.get("offerId") or segments.get("offer_id")
        date = segments.get("date")
        if not offer_id or not date:
            n_skipped += 1
            continue
        if isinstance(date, dict):
            # API sometimes returns {year, month, day}
            try:
                date = (f"{int(date['year']):04d}-"
                          f"{int(date['month']):02d}-"
                          f"{int(date['day']):02d}")
            except Exception:
                n_skipped += 1
                continue
        info = _resolve_offer(offer_id)
        sku = info.get("sku") or offer_id
        clicks = metrics.get("clicks") or 0
        impressions = metrics.get("impressions") or 0
        try:
            clicks = int(clicks)
            impressions = int(impressions)
        except (TypeError, ValueError):
            n_skipped += 1
            continue
        if clicks == 0 and impressions == 0:
            continue  # skip empty rows
        row = {
            "platform": "google_merchant",
            "campaign_id": "free_listings",
            "date": date,
            "sku": sku,
            "family": info.get("family") or "",
            "free_listing_clicks": clicks,
            "free_listing_impressions": impressions,
            "captured_at": datetime.now(timezone.utc).isoformat(),
        }
        try:
            db.upsert_ad_campaign_sku(row)
            n_written += 1
        except Exception as exc:
            log.error("upsert free-listing row for %s/%s failed: %s",
                        sku, date, exc)
            n_skipped += 1
    return {"written": n_written, "skipped": n_skipped}


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


def _make_client() -> MerchantClient:
    client_id = os.environ.get("GOOGLE_ADS_CLIENT_ID", "").strip()
    client_secret = os.environ.get(
        "GOOGLE_ADS_CLIENT_SECRET", "").strip()
    refresh_token = os.environ.get(
        "GOOGLE_ADS_REFRESH_TOKEN", "").strip()
    merchant_id = os.environ.get(
        "GOOGLE_MERCHANT_ID", "").strip()
    if not all((client_id, client_secret, refresh_token,
                  merchant_id)):
        missing = [name for name, val in [
            ("GOOGLE_ADS_CLIENT_ID", client_id),
            ("GOOGLE_ADS_CLIENT_SECRET", client_secret),
            ("GOOGLE_ADS_REFRESH_TOKEN", refresh_token),
            ("GOOGLE_MERCHANT_ID", merchant_id),
        ] if not val]
        raise SystemExit(
            f"Missing env var(s): {', '.join(missing)}")
    access_token = _refresh_access_token(
        client_id, client_secret, refresh_token)
    if not access_token:
        raise SystemExit(
            "Failed to refresh access token — check that the "
            "refresh token was issued with both `adwords` AND "
            "`content` scopes (see GOOGLE_ADS_SETUP.md).")
    throttle = float(os.environ.get("MERCHANT_THROTTLE_S", "0.3"))
    return MerchantClient(
        access_token, merchant_id, throttle_s=throttle)


def cmd_status(args: argparse.Namespace) -> int:
    _setup_log(args.verbose)
    client = _make_client()
    result = sync_product_statuses(client)
    log.info("DONE status: %s", result)
    return 0


def cmd_performance(args: argparse.Namespace) -> int:
    _setup_log(args.verbose)
    client = _make_client()
    result = sync_free_listing_performance(client, days=args.days)
    log.info("DONE performance: %s", result)
    return 0


def cmd_daily(args: argparse.Namespace) -> int:
    _setup_log(args.verbose)
    client = _make_client()
    s_result = sync_product_statuses(client)
    log.info("DONE status: %s", s_result)
    p_result = sync_free_listing_performance(client, days=args.days)
    log.info("DONE performance: %s", p_result)
    return 0


def cmd_backfill(args: argparse.Namespace) -> int:
    _setup_log(args.verbose)
    client = _make_client()
    log.info("Backfilling %d days of free-listing performance",
              args.days)
    # Chunk the backfill into 90-day windows to keep responses
    # under the API's row-cap. Each window does its own
    # reports.search call.
    chunk = 90
    total = {"written": 0, "skipped": 0}
    remaining = args.days
    end = datetime.now(timezone.utc).date()
    while remaining > 0:
        window = min(chunk, remaining)
        window_end = end
        window_start = end - timedelta(days=window - 1)
        log.info("  window: %s -> %s (%d days)",
                  window_start, window_end, window)
        # sync_free_listing_performance is parameterised by days
        # from today; for backfill chunks we need to shift the
        # window. Inline a temp query here.
        query = (
            f"SELECT segments.offer_id, segments.date, "
            f"       metrics.clicks, metrics.impressions "
            f"FROM ProductPerformanceView "
            f"WHERE segments.date BETWEEN '{window_start}' "
            f"  AND '{window_end}' "
            f"  AND segments.marketing_method = 'ORGANIC'")
        n_written = 0
        n_skipped = 0
        for r in client.search_report(query):
            segs = r.get("productPerformanceView") or {}
            segments = (segs.get("segments") if segs
                          else r.get("segments")) or {}
            metrics = (segs.get("metrics") if segs
                          else r.get("metrics")) or {}
            offer_id = (segments.get("offerId")
                          or segments.get("offer_id"))
            date = segments.get("date")
            if not offer_id or not date:
                n_skipped += 1
                continue
            if isinstance(date, dict):
                try:
                    date = (f"{int(date['year']):04d}-"
                              f"{int(date['month']):02d}-"
                              f"{int(date['day']):02d}")
                except Exception:
                    n_skipped += 1
                    continue
            info = _resolve_offer(offer_id)
            sku = info.get("sku") or offer_id
            clicks = int(metrics.get("clicks") or 0)
            impressions = int(metrics.get("impressions") or 0)
            if clicks == 0 and impressions == 0:
                continue
            row = {
                "platform": "google_merchant",
                "campaign_id": "free_listings",
                "date": date,
                "sku": sku,
                "family": info.get("family") or "",
                "free_listing_clicks": clicks,
                "free_listing_impressions": impressions,
                "captured_at": datetime.now(
                    timezone.utc).isoformat(),
            }
            try:
                db.upsert_ad_campaign_sku(row)
                n_written += 1
            except Exception as exc:
                log.error("upsert failed %s/%s: %s",
                            sku, date, exc)
                n_skipped += 1
        log.info("  window done: written=%d skipped=%d",
                  n_written, n_skipped)
        total["written"] += n_written
        total["skipped"] += n_skipped
        end = window_start - timedelta(days=1)
        remaining -= window
    log.info("DONE backfill: %s", total)
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Sync Google Merchant Center into local DB")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_s = sub.add_parser("status",
                            help="Pull every product's feed-health "
                                  "status (Shopping ads + free "
                                  "listings approval state).")
    p_s.add_argument("--verbose", action="store_true")
    p_s.set_defaults(func=cmd_status)

    p_p = sub.add_parser("performance",
                            help="Pull per-SKU free-listing clicks "
                                  "+ impressions for the last N "
                                  "days.")
    p_p.add_argument("--days", type=int, default=30)
    p_p.add_argument("--verbose", action="store_true")
    p_p.set_defaults(func=cmd_performance)

    p_d = sub.add_parser("daily",
                            help="Both status + recent performance "
                                  "(default 7-day window).")
    p_d.add_argument("--days", type=int, default=7)
    p_d.add_argument("--verbose", action="store_true")
    p_d.set_defaults(func=cmd_daily)

    p_b = sub.add_parser("backfill",
                            help="Backfill performance for the "
                                  "last N days (chunked 90 at a "
                                  "time).")
    p_b.add_argument("--days", type=int, default=1095)
    p_b.add_argument("--verbose", action="store_true")
    p_b.set_defaults(func=cmd_backfill)

    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
