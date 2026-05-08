"""reviewsio_sync.py (v2.67.91)
====================================

Pull product-review data from Reviews.io into local SQLite.

Why this exists
---------------
Per-SKU review data is the most actionable signal in the buyer's
toolkit:
  - 4.9★ / 47 reviews → push it harder, hold more stock
  - 3.1★ / 12 reviews with "too dim" complaints → flag for product
    review, slow reorder

The AI bot's get_product_reviews(sku) tool then surfaces this on
demand, including 1-2★ recent reviews so buyers can see WHY a SKU
is rated low.

CLI:
  python reviewsio_sync.py recent --days 30   # rolling daily refresh
  python reviewsio_sync.py full               # full backfill
  python reviewsio_sync.py one --sku LED-X    # debug one SKU

Env vars required:
  REVIEWSIO_STORE_ID        e.g. 'www.wired4signsusa.com'
  REVIEWSIO_API_KEY         private API key (from Reviews.io
                              account settings)

Optional:
  REVIEWSIO_API_URL         override base URL
                              (default https://api.reviews.io)
  REVIEWSIO_THROTTLE_S      seconds between API calls (default 0.3)

Reviews.io API reference:
  https://developer.reviews.io/v2/reference/getreviews
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
log = logging.getLogger("reviewsio_sync")


class ReviewsIOClient:
    """Reviews.io REST client. Reviews.io has both a v1 and v2 API;
    v2 is current as of 2024+. Endpoints pulled from their docs."""

    def __init__(self, store_id: str, api_key: str,
                  api_url: str = "https://api.reviews.io",
                  throttle_s: float = 0.3):
        if not store_id:
            raise RuntimeError("REVIEWSIO_STORE_ID required")
        if not api_key:
            raise RuntimeError("REVIEWSIO_API_KEY required")
        self.store_id = store_id.strip()
        self.api_key = api_key.strip()
        self.base = api_url.rstrip("/")
        self.throttle_s = throttle_s
        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "application/json",
            "store": self.store_id,
            "apikey": self.api_key,
        })
        self._last_call = 0.0

    def _throttle(self) -> None:
        elapsed = time.time() - self._last_call
        if elapsed < self.throttle_s:
            time.sleep(self.throttle_s - elapsed)
        self._last_call = time.time()

    def _get(self, path: str,
              params: Optional[dict] = None) -> Optional[dict]:
        url = f"{self.base}{path}"
        for attempt in range(5):
            self._throttle()
            try:
                r = self.session.get(url, params=params, timeout=30)
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
            if r.status_code == 401:
                raise RuntimeError(
                    "401 Unauthorized — check REVIEWSIO_API_KEY "
                    "and REVIEWSIO_STORE_ID")
            if r.status_code != 200:
                log.error("HTTP %d on %s: %s",
                            r.status_code, url, r.text[:300])
                return None
            try:
                return r.json()
            except Exception as exc:
                log.error("JSON parse failed: %s", exc)
                return None
        return None

    def list_reviews(self, page: int = 1, per_page: int = 50,
                       since: Optional[datetime] = None,
                       sku: Optional[str] = None
                       ) -> Optional[dict]:
        """Pull a page of product reviews. Reviews.io has multiple
        endpoint generations:
          - /product/review                   (legacy)
          - /api/products/                    (v2)
          - /merchant/v2.6/products/...       (v2.6)
          - /merchant/v3/reviews              (v3 — current)
        v2.67.107 — try the merchant v3 endpoint first, fall back
        through older paths so we work regardless of account
        provisioning."""
        # v3 uses store_id in query, not header
        params_v3: Dict[str, Any] = {
            "store": self.store_id,
            "page": page,
            "per_page": per_page,
            "order": "desc",
        }
        if since:
            params_v3["minDate"] = since.strftime("%Y-%m-%d")
        if sku:
            params_v3["sku"] = sku

        # Try endpoints in order, return first that returns data
        # (or last attempted if all empty)
        endpoints_to_try = [
            "/merchant/v3/reviews",
            "/merchant/v2.6/products/reviews",
            "/api/products/reviews",
            "/product/review",
        ]
        last_payload = None
        for ep in endpoints_to_try:
            payload = self._get(ep, params=params_v3)
            if payload is None:
                continue
            last_payload = payload
            # Heuristic: a response with non-empty review list
            # means this endpoint works for this account.
            for k in ("reviews", "data", "review", "items"):
                v = payload.get(k)
                if isinstance(v, list) and v:
                    log.info(
                        "Using Reviews.io endpoint: %s", ep)
                    return payload
                if isinstance(v, list):
                    last_payload = payload
        return last_payload


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------
def _safe_int(v: Any) -> Optional[int]:
    try:
        return int(v) if v is not None else None
    except (TypeError, ValueError):
        return None


def _safe_float(v: Any) -> Optional[float]:
    try:
        return float(v) if v is not None else None
    except (TypeError, ValueError):
        return None


def _safe_iso_date(v: Any) -> Optional[str]:
    """Reviews.io returns dates as 'YYYY-MM-DD HH:MM:SS' or
    similar. Normalise to ISO 8601 with timezone."""
    if not v:
        return None
    s = str(v).strip()
    # Try several common formats.
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%SZ",
                  "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(s, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.isoformat()
        except Exception:
            continue
    return s


_SHOPIFY_HANDLE_BY_SKU: Optional[Dict[str, dict]] = None


def _build_sku_to_handle_lookup() -> Dict[str, dict]:
    """Reviews.io stores SKU as the per-variant Shopify SKU. We
    can join straight to CIN7 SKUs but we want family/handle for
    the AI tools too. Build {sku_prefix: handle/family} lookup
    from product_dimensions, where each dim row has a list of
    variants.
    Note: product_dimensions is keyed per-product not per-variant
    (one row per Shopify product, multiple SKUs share). We can
    only match family-prefix here. For variant-level joins we'd
    need a separate shopify_product_skus mapping (TODO v2.68)."""
    global _SHOPIFY_HANDLE_BY_SKU
    if _SHOPIFY_HANDLE_BY_SKU is not None:
        return _SHOPIFY_HANDLE_BY_SKU
    rows = db.all_product_dimensions()
    out: Dict[str, dict] = {}
    for r in rows:
        fam = (r.get("family") or "").strip().upper()
        if fam:
            out[fam] = {
                "shopify_handle": r.get("shopify_handle"),
                "title": r.get("title"),
            }
    _SHOPIFY_HANDLE_BY_SKU = out
    return out


def _resolve_handle_for_sku(sku: str) -> dict:
    """Best-effort: extract family from SKU and look up handle.
    Returns {family, shopify_handle, title} or empty if no match.
    """
    if not sku:
        return {}
    s = sku.strip().upper()
    family = ""
    if s.startswith("LED-"):
        parts = s.split("-")
        if len(parts) >= 2:
            family = parts[1]
    elif s.startswith("LEDKIT-"):
        parts = s.split("-")
        if len(parts) >= 2:
            family = f"KIT-{parts[1]}"
    if not family:
        return {}
    lookup = _build_sku_to_handle_lookup()
    info = lookup.get(family, {})
    if info:
        info = dict(info)
        info["family"] = family
    return info


# ---------------------------------------------------------------------------
# Sync
# ---------------------------------------------------------------------------
def _flatten_review(rev: dict) -> dict:
    """Reviews.io review payload -> our DB row shape."""
    sku = (rev.get("sku") or "").strip()
    info = _resolve_handle_for_sku(sku)
    images = rev.get("images") or []
    images_json = (json.dumps([img.get("url") for img in images
                                  if img.get("url")])
                     if images else None)
    return {
        "review_id": str(rev.get("review_id") or rev.get("id") or ""),
        "sku": sku,
        "family": info.get("family") or "",
        "shopify_handle": info.get("shopify_handle") or "",
        "shopify_product_id": rev.get("product_id"),
        "rating": _safe_float(rev.get("rating")),
        "title": rev.get("title"),
        "body": rev.get("review"),
        "reviewer_name": rev.get("reviewer", {}).get("first_name")
                            if isinstance(rev.get("reviewer"), dict)
                            else rev.get("reviewer"),
        "reviewer_email": rev.get("reviewer", {}).get("email")
                              if isinstance(rev.get("reviewer"), dict)
                              else None,
        "review_date": _safe_iso_date(
            rev.get("date_created") or rev.get("date")),
        "verified_buyer": 1 if (
            rev.get("verified_buyer") in (True, 1, "true", "1"))
                              else 0,
        "helpful_count": _safe_int(rev.get("helpful_votes")) or 0,
        "images_json": images_json,
        "captured_at": datetime.now(timezone.utc).isoformat(),
    }


def sync_reviews(client: ReviewsIOClient,
                    since: Optional[datetime] = None,
                    sku: Optional[str] = None
                    ) -> dict:
    """Walk Reviews.io paginated review feed, upsert each review."""
    page = 1
    per_page = 50
    n_written = 0
    n_skipped = 0
    while True:
        log.info("Fetching page %d (per_page=%d, since=%s, sku=%s)",
                  page, per_page,
                  since.isoformat() if since else "None", sku)
        payload = client.list_reviews(
            page=page, per_page=per_page, since=since, sku=sku)
        if not payload:
            break
        # Reviews.io returns reviews under various keys depending on
        # endpoint. Try common shapes.
        reviews = (payload.get("reviews")
                     or payload.get("data")
                     or payload.get("review", []))
        if not reviews and isinstance(payload, list):
            reviews = payload
        if not reviews:
            log.info("  empty page → stopping")
            break

        for rev in reviews:
            row = _flatten_review(rev)
            if not row.get("review_id"):
                n_skipped += 1
                continue
            try:
                db.upsert_product_review(row)
                n_written += 1
            except Exception as exc:
                log.error("upsert review %s failed: %s",
                            row.get("review_id"), exc)
                n_skipped += 1

        log.info("  page %d -> %d reviews (running %d written, "
                  "%d skipped)",
                  page, len(reviews), n_written, n_skipped)

        # Check for end of feed
        if len(reviews) < per_page:
            break
        page += 1
        if page > 1000:  # safety stop
            log.warning("page > 1000 — stopping safety guard")
            break

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


def _make_client() -> ReviewsIOClient:
    store_id = os.environ.get("REVIEWSIO_STORE_ID", "").strip()
    api_key = os.environ.get("REVIEWSIO_API_KEY", "").strip()
    api_url = os.environ.get("REVIEWSIO_API_URL",
                                "https://api.reviews.io").strip()
    throttle = float(os.environ.get("REVIEWSIO_THROTTLE_S", "0.3"))
    return ReviewsIOClient(store_id, api_key, api_url=api_url,
                              throttle_s=throttle)


def cmd_recent(args: argparse.Namespace) -> int:
    _setup_log(args.verbose)
    client = _make_client()
    since = (datetime.now(timezone.utc) - timedelta(days=args.days)
                if args.days > 0 else None)
    result = sync_reviews(client, since=since)
    log.info("DONE: %s", result)
    return 0


def cmd_full(args: argparse.Namespace) -> int:
    _setup_log(args.verbose)
    client = _make_client()
    result = sync_reviews(client, since=None)
    log.info("DONE: %s", result)
    return 0


def cmd_one(args: argparse.Namespace) -> int:
    _setup_log(args.verbose)
    client = _make_client()
    result = sync_reviews(client, sku=args.sku)
    log.info("DONE: %s", result)
    # Show what got stored
    summary = db.get_reviews_summary_for_sku(args.sku)
    print(json.dumps(summary, indent=2, default=str))
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Sync Reviews.io reviews into local DB")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_r = sub.add_parser("recent",
                            help="Pull reviews modified in last N days")
    p_r.add_argument("--days", type=int, default=30)
    p_r.add_argument("--verbose", action="store_true")
    p_r.set_defaults(func=cmd_recent)

    p_f = sub.add_parser("full",
                            help="Full backfill of all reviews")
    p_f.add_argument("--verbose", action="store_true")
    p_f.set_defaults(func=cmd_full)

    p_o = sub.add_parser("one", help="Pull and inspect for one SKU")
    p_o.add_argument("--sku", required=True)
    p_o.add_argument("--verbose", action="store_true")
    p_o.set_defaults(func=cmd_one)

    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
