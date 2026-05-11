"""klaviyo_sync.py (v2.67.90)
=================================

Pull email-marketing data from Klaviyo into local SQLite.

Why this exists
---------------
Buyer needs to see, per SKU, "what marketing efforts have touched
this product?" so demand signals can be attributed to campaigns.
Klaviyo runs the newsletters; this script:

  1. Pulls every campaign from the last N days (default 90).
     Stores headline metrics — recipients, open rate, click rate,
     attributed revenue.
  2. For each campaign, pulls per-recipient "Clicked Email" events,
     resolves the destination URL to a Shopify handle, then
     resolves handle -> CIN7 SKU via product_dimensions.
  3. Aggregates click counts + attributed revenue per (campaign,
     SKU) into email_campaign_skus.

The AI bot's get_email_attribution(sku, days) tool then surfaces
this on demand: "Slim8 has had 184 newsletter clicks across 3
campaigns since 4/15".

Cost: included in your Klaviyo subscription (no API charges).
Time: ~5 min for 90 days of campaigns. Idempotent on
       (campaign_id, sku) so safe to re-run.

CLI:
  python klaviyo_sync.py recent --days 7      # rolling daily refresh
  python klaviyo_sync.py recent --days 90     # full backfill
  python klaviyo_sync.py one --campaign-id X  # debug a single one

Env vars required:
  KLAVIYO_API_KEY            (private key, starts with pk_)

Optional:
  KLAVIYO_API_VERSION        default '2024-10-15' (current as of
                              v2.67.90 build date)
  KLAVIYO_THROTTLE_S         seconds between API calls (default 0.5)
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
import urllib.parse as _urlparse
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import requests

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))
import db  # noqa: E402

LOG_FORMAT = "%(asctime)s [%(levelname)s] %(message)s"
log = logging.getLogger("klaviyo_sync")


# ---------------------------------------------------------------------------
# Klaviyo client
# ---------------------------------------------------------------------------
class KlaviyoClient:
    """Minimal Klaviyo REST client with pagination + throttling."""

    BASE = "https://a.klaviyo.com/api"

    def __init__(self, api_key: str,
                  api_version: str = "2024-10-15",
                  throttle_s: float = 0.5):
        if not api_key:
            raise RuntimeError("KLAVIYO_API_KEY required")
        self.api_key = api_key
        self.api_version = api_version
        self.throttle_s = throttle_s
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Klaviyo-API-Key {api_key}",
            "accept": "application/vnd.api+json",
            "revision": api_version,
        })
        self._last_call = 0.0

    def _throttle(self) -> None:
        elapsed = time.time() - self._last_call
        if elapsed < self.throttle_s:
            time.sleep(self.throttle_s - elapsed)
        self._last_call = time.time()

    def _get(self, url: str,
              params: Optional[dict] = None) -> Optional[dict]:
        for attempt in range(5):
            self._throttle()
            try:
                r = self.session.get(url, params=params, timeout=30)
            except requests.RequestException as exc:
                log.warning("network error: %s; retry %d", exc, attempt)
                time.sleep(2 ** attempt)
                continue
            if r.status_code == 429:
                wait = int(r.headers.get("Retry-After", "5") or 5)
                log.warning("429 throttled; sleeping %ds", wait)
                time.sleep(wait)
                continue
            if r.status_code == 401:
                raise RuntimeError(
                    "401 Unauthorized — check KLAVIYO_API_KEY")
            if r.status_code != 200:
                log.error("HTTP %d on %s: %s",
                            r.status_code, url, r.text[:300])
                return None
            return r.json()
        return None

    def list_campaigns(self, since: datetime
                          ) -> List[dict]:
        """Pull campaigns sent on/after `since`.

        v2.67.112 — Klaviyo's /campaigns/ endpoint rejected
        `send_time` filter ('not a filterable field'). The
        actual filterable fields are: archived, created_at,
        messages.channel, name, scheduled_at, status, updated_at.
        Switched to `scheduled_at` (most accurate for 'when was
        the campaign supposed to go out'). For sent_at on the
        response, we still read `send_time` from attributes —
        that field is returned, just not filterable."""
        filter_str = (f"and(equals(messages.channel,'email'),"
                          f"greater-or-equal(scheduled_at,"
                          f"{since.isoformat()}))")
        url = f"{self.BASE}/campaigns/"
        params = {
            "filter": filter_str,
            "page[size]": 100,
            "include": "campaign-messages",
            "sort": "-scheduled_at",
        }
        out: List[dict] = []
        while url:
            payload = self._get(url, params=params if not out else None)
            if not payload:
                break
            data = payload.get("data") or []
            out.extend(data)
            url = (payload.get("links") or {}).get("next")
            log.info("  Klaviyo campaigns page -> %d (running %d)",
                      len(data), len(out))
        return out

    def get_campaign_metrics(self, campaign_id: str
                                ) -> Optional[dict]:
        """Pull recipient/open/click totals for one campaign.
        Klaviyo's metrics endpoint returns aggregate stats."""
        # The 'campaign-recipient-estimations' endpoint gives us
        # the recipients count; 'campaign-message-statistics' gives
        # delivery + open + click + revenue totals.
        url = f"{self.BASE}/campaigns/{campaign_id}/"
        params = {"include": "campaign-messages"}
        return self._get(url, params=params)

    def query_campaign_values(self, campaign_id: str,
                                  metric_id: str
                                  ) -> Optional[dict]:
        """Klaviyo's reporting endpoint — gives metric totals for
        a campaign-as-attribution. Used to pull
        Placed Order / Clicked Email totals attributed."""
        url = f"{self.BASE}/campaign-values-reports/"
        body = {
            "data": {
                "type": "campaign-values-report",
                "attributes": {
                    "statistics": [
                        "recipients", "delivered", "delivery_rate",
                        "opens", "opens_unique", "open_rate",
                        "clicks", "clicks_unique", "click_rate",
                        "click_to_open_rate", "conversions",
                        "conversion_uniques", "conversion_rate",
                        "conversion_value", "average_order_value",
                        "revenue_per_recipient", "unsubscribes",
                        "unsubscribe_rate", "unsubscribe_uniques",
                        "spam_complaints", "spam_complaint_rate",
                        "bounce_rate", "bounced", "bounced_or_failed",
                        "bounced_or_failed_rate", "failed",
                        "failed_rate"],
                    "timeframe": {"key": "last_365_days"},
                    "conversion_metric_id": metric_id,
                    "filter": (f"equals(campaign_id,'{campaign_id}')"),
                },
            },
        }
        self._throttle()
        try:
            r = self.session.post(url, json=body, timeout=30)
        except requests.RequestException as exc:
            log.warning("campaign-values POST failed: %s", exc)
            return None
        if r.status_code != 200:
            log.warning("campaign-values HTTP %d: %s",
                          r.status_code, r.text[:200])
            return None
        return r.json()

    def _get_metric_id_by_name(self, name: str,
                                    fuzzy: bool = True
                                    ) -> Optional[str]:
        """v2.67.107 — paginate all metrics and find one by name.
        Klaviyo doesn't allow API-side filter on 'name'.

        v2.67.112 — wider matching. Klaviyo accounts may name
        their conversion metric differently:
          'Placed Order' (default Shopify integration)
          'Order Placed'
          'Purchase'
          'Order Completed'
          'Checkout Completed'
          Custom integration names
        Strategy:
          1. Exact match on `name` (case-sensitive)
          2. Case-insensitive exact match
          3. (if fuzzy) Substring match on lowercase
          4. (if fuzzy) Match anything with 'order' or 'placed'
             or 'purchase' in the name
        First match wins. Returns None only if nothing remotely
        plausible exists."""
        target = name.strip()
        target_lower = target.lower()
        seen: List[tuple] = []
        url = f"{self.BASE}/metrics/"
        params = {"page[size]": 100}
        first = True
        while url:
            payload = self._get(url, params=params if first else None)
            first = False
            if not payload:
                break
            for m in (payload.get("data") or []):
                attrs = m.get("attributes") or {}
                mname = (attrs.get("name") or "").strip()
                mid = m.get("id")
                seen.append((mid, mname))
                if mname == target:
                    return mid  # exact match wins immediately
            url = (payload.get("links") or {}).get("next")

        if not seen:
            return None

        # 2. Case-insensitive exact
        for mid, mname in seen:
            if mname.lower() == target_lower:
                log.info(
                    "Klaviyo: matched '%s' to '%s' "
                    "(case-insensitive)", target, mname)
                return mid

        if not fuzzy:
            return None

        # 3. Substring match either direction
        for mid, mname in seen:
            n = mname.lower()
            if target_lower in n or n in target_lower:
                log.info(
                    "Klaviyo: matched '%s' to '%s' "
                    "(substring)", target, mname)
                return mid

        # 4. Keyword-presence fallback (for 'Placed Order')
        if "placed" in target_lower or "order" in target_lower \
                or "purchase" in target_lower:
            for mid, mname in seen:
                n = mname.lower()
                if "order" in n or "purchase" in n \
                        or "placed" in n:
                    log.info(
                        "Klaviyo: keyword-matched '%s' to '%s' "
                        "(order/purchase fallback)",
                        target, mname)
                    return mid

        log.warning(
            "Klaviyo: no match for metric '%s'. Tried %d names: %s",
            target, len(seen),
            ", ".join(f"'{n}'" for _, n in seen[:10]))
        return None

    def get_placed_order_metric_id(self) -> Optional[str]:
        """Find the 'Placed Order' metric (Klaviyo's revenue-
        attribution conversion). Uses _get_metric_id_by_name
        with fuzzy fallback so accounts using 'Order Placed' /
        'Purchase' / etc. also resolve."""
        return self._get_metric_id_by_name(
            "Placed Order", fuzzy=True)

    def list_clicked_events_for_campaign(self, campaign_id: str,
                                              limit: int = 5000
                                              ) -> List[dict]:
        """Pull Clicked Email events tagged to this campaign.
        Each event has the URL clicked + customer.

        v2.67.107 — uses _get_metric_id_by_name (paginated client-
        side filter) instead of the broken filter-by-name."""
        metric_id = self._get_metric_id_by_name("Clicked Email")
        if not metric_id:
            log.warning("Clicked Email metric id not found")
            return []

        # Now query events for this campaign
        url = f"{self.BASE}/events/"
        params = {
            "filter": (f"and(equals(metric_id,'{metric_id}'),"
                          f"contains(properties,'{campaign_id}'))"),
            "page[size]": 200,
            "sort": "-datetime",
        }
        out: List[dict] = []
        while url and len(out) < limit:
            payload = self._get(url, params=params if not out else None)
            if not payload:
                break
            data = payload.get("data") or []
            out.extend(data)
            url = (payload.get("links") or {}).get("next")
            if len(out) >= limit:
                break
        return out


# ---------------------------------------------------------------------------
# URL -> Shopify handle resolution
# ---------------------------------------------------------------------------
_SHOPIFY_PRODUCT_PATH_RE = "/products/"


def _extract_handle_from_url(url: str) -> Optional[str]:
    """A Klaviyo click URL like
    'https://wired4signsusa.com/products/slim-led-channel-slim8-ac2-z?utm_...'
    -> 'slim-led-channel-slim8-ac2-z'."""
    if not url or _SHOPIFY_PRODUCT_PATH_RE not in url:
        return None
    try:
        parsed = _urlparse.urlparse(url)
        path = parsed.path
        if _SHOPIFY_PRODUCT_PATH_RE not in path:
            return None
        handle = path.split(_SHOPIFY_PRODUCT_PATH_RE, 1)[1]
        # Strip trailing slash and anything after it.
        handle = handle.split("/")[0].split("?")[0].strip()
        return handle or None
    except Exception:
        return None


_HANDLE_TO_SKU_CACHE: Optional[Dict[str, dict]] = None


def _build_handle_to_product_lookup() -> Dict[str, dict]:
    """Return {shopify_handle: {family, sku}} from product_dimensions
    table. Cached process-locally."""
    global _HANDLE_TO_SKU_CACHE
    if _HANDLE_TO_SKU_CACHE is not None:
        return _HANDLE_TO_SKU_CACHE
    rows = db.all_product_dimensions()
    lookup: Dict[str, dict] = {}
    for r in rows:
        h = (r.get("shopify_handle") or "").strip()
        if not h:
            continue
        # Family from product_dimensions; SKU is harder — that table
        # stores one row per Shopify product, not per variant. The
        # caller can find specific variant SKU via family lookup if
        # needed.
        lookup[h] = {
            "family": r.get("family") or "",
            "title": r.get("title") or "",
        }
    _HANDLE_TO_SKU_CACHE = lookup
    return lookup


# ---------------------------------------------------------------------------
# Sync flows
# ---------------------------------------------------------------------------
def sync_recent(client: KlaviyoClient, days: int) -> dict:
    """Pull campaigns from the last `days` days; write to DB."""
    since = datetime.now(timezone.utc) - timedelta(days=days)
    log.info("Fetching campaigns since %s", since.isoformat())
    campaigns = client.list_campaigns(since=since)
    log.info("Got %d campaigns", len(campaigns))

    metric_id = client.get_placed_order_metric_id()
    if not metric_id:
        log.warning("Placed Order metric id not found — revenue "
                      "attribution will be missing")

    handle_lookup = _build_handle_to_product_lookup()

    n_campaigns_written = 0
    n_clicks_written = 0
    n_skipped = 0
    for camp in campaigns:
        cid = camp.get("id")
        attrs = camp.get("attributes") or {}
        if not cid:
            continue
        # Pull metrics
        metrics_data = None
        if metric_id:
            metrics_data = client.query_campaign_values(cid, metric_id)
        stats = _extract_stats(metrics_data) if metrics_data else {}

        row = {
            "id": cid,
            "name": attrs.get("name"),
            "subject": (attrs.get("send_options") or {}).get(
                "subject_line") or attrs.get("name"),
            "sent_at": attrs.get("send_time"),
            "list_name": _extract_list_name(camp),
            "recipients": stats.get("recipients"),
            "delivered": stats.get("delivered"),
            "opens_unique": stats.get("opens_unique"),
            "clicks_unique": stats.get("clicks_unique"),
            "open_rate": stats.get("open_rate"),
            "click_rate": stats.get("click_rate"),
            "revenue": stats.get("conversion_value"),
            "orders": stats.get("conversions"),
            "raw_payload": json.dumps(metrics_data or camp)[:50000],
            "captured_at": datetime.now(timezone.utc).isoformat(),
        }
        try:
            db.upsert_email_campaign(row)
            n_campaigns_written += 1
        except Exception as exc:
            log.error("upsert campaign %s failed: %s", cid, exc)
            n_skipped += 1
            continue

        # Pull clicked events for per-SKU click counts
        events = client.list_clicked_events_for_campaign(cid)
        click_counts: Dict[str, dict] = {}
        for ev in events:
            ev_attrs = ev.get("attributes") or {}
            props = ev_attrs.get("event_properties") or {}
            url = props.get("URL") or props.get("url")
            handle = _extract_handle_from_url(url)
            if not handle:
                continue
            entry = click_counts.setdefault(handle, {
                "click_count": 0, "unique_emails": set()})
            entry["click_count"] += 1
            email = (ev_attrs.get("profile") or {}).get("email")
            if email:
                entry["unique_emails"].add(email)

        for handle, data in click_counts.items():
            info = handle_lookup.get(handle, {})
            sku_row = {
                "campaign_id": cid,
                "sku": handle,  # using handle as the join key for now
                "family": info.get("family") or "",
                "shopify_handle": handle,
                "click_count": data["click_count"],
                "unique_clicks": len(data["unique_emails"]),
                "attributed_revenue": None,
                "captured_at": datetime.now(timezone.utc).isoformat(),
            }
            try:
                db.upsert_email_campaign_sku(sku_row)
                n_clicks_written += 1
            except Exception as exc:
                log.error("upsert click row for %s/%s failed: %s",
                            cid, handle, exc)

        log.info("  [%s] %s — %d events, %d unique handles",
                  cid, (attrs.get("name") or "")[:50],
                  len(events), len(click_counts))

    return {
        "campaigns_written": n_campaigns_written,
        "click_rows_written": n_clicks_written,
        "skipped": n_skipped,
    }


def _extract_stats(metrics_data: dict) -> dict:
    """Pull the statistics dict from the campaign-values-reports
    response. Schema is nested under data.attributes.results."""
    try:
        results = (metrics_data.get("data") or {}).get(
            "attributes", {}).get("results", [])
        if not results:
            return {}
        stats = results[0].get("statistics") or {}
        # Coerce numerics
        return {k: (float(v) if isinstance(v, (int, float))
                      else v)
                  for k, v in stats.items()}
    except Exception:
        return {}


def _extract_list_name(camp: dict) -> Optional[str]:
    """Try to find a target list/segment name from the campaign
    payload. Klaviyo nests audiences in send_strategy or
    relationships."""
    attrs = camp.get("attributes") or {}
    audiences = attrs.get("audiences") or {}
    inc = audiences.get("included") or []
    if inc:
        return inc[0]
    return None


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


def _make_client() -> KlaviyoClient:
    api_key = os.environ.get("KLAVIYO_API_KEY", "").strip()
    if not api_key:
        raise SystemExit("KLAVIYO_API_KEY env var not set")
    api_version = os.environ.get(
        "KLAVIYO_API_VERSION", "2024-10-15").strip()
    throttle = float(os.environ.get("KLAVIYO_THROTTLE_S", "0.5"))
    return KlaviyoClient(api_key, api_version=api_version,
                            throttle_s=throttle)


def cmd_recent(args: argparse.Namespace) -> int:
    _setup_log(args.verbose)
    client = _make_client()
    result = sync_recent(client, args.days)
    log.info("DONE: %s", result)
    return 0


def cmd_one(args: argparse.Namespace) -> int:
    _setup_log(args.verbose)
    client = _make_client()
    log.info("Pulling campaign %s", args.campaign_id)
    metric_id = client.get_placed_order_metric_id()
    metrics = (client.query_campaign_values(args.campaign_id, metric_id)
                  if metric_id else None)
    print(json.dumps(metrics, indent=2, default=str))
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Sync Klaviyo email campaigns + per-product "
                      "click attribution into local DB")
    sub = parser.add_subparsers(dest="cmd", required=True)
    p_r = sub.add_parser("recent",
                            help="Pull campaigns sent in last N days")
    p_r.add_argument("--days", type=int, default=7,
                       help="Lookback in days (default 7)")
    p_r.add_argument("--verbose", action="store_true")
    p_r.set_defaults(func=cmd_recent)

    p_o = sub.add_parser("one", help="Debug a single campaign")
    p_o.add_argument("--campaign-id", required=True)
    p_o.add_argument("--verbose", action="store_true")
    p_o.set_defaults(func=cmd_one)

    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
