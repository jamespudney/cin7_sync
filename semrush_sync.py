"""semrush_sync.py (v2.67.92)
=================================

Pull SEO ranking data from SEMrush into local SQLite.

Why this exists
---------------
SEMrush tracks keyword rankings for the wired4signsusa.com domain.
Each ranked URL maps to a Shopify product page (/products/<handle>)
or category page (/collections/<handle>). When a SKU's ranking
position improves significantly (e.g. position 7 -> 2), demand
typically follows 2-3 weeks later. The bot can flag this as a
buying signal: "rank just jumped — order more before the demand
spike."

Cost model — SEMrush Guru plan API:
  - 30,000 units/month included
  - Position Tracking endpoint: ~10 units/keyword/refresh
  - Domain organic positions: 50 units/call (10 results)
  - Strategy: weekly pull of all tracked keywords (cheap), plus
    daily pull of TOP 50 keywords (cheap also)

CLI:
  python semrush_sync.py weekly             # pull all tracked kws
  python semrush_sync.py top --limit 50     # pull top N
  python semrush_sync.py one --keyword X    # debug one keyword

Env vars required:
  SEMRUSH_API_KEY            32-char hex string

Optional:
  SEMRUSH_DOMAIN             default 'wired4signsusa.com'
  SEMRUSH_DATABASE           SEMrush database code (default 'us')
  SEMRUSH_THROTTLE_S         seconds between calls (default 0.5)

API reference:
  https://developer.semrush.com/api/v3/
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
import urllib.parse as _urlparse
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))
import db  # noqa: E402

LOG_FORMAT = "%(asctime)s [%(levelname)s] %(message)s"
log = logging.getLogger("semrush_sync")


# ---------------------------------------------------------------------------
# SEMrush client
# ---------------------------------------------------------------------------
class SEMrushClient:
    """SEMrush v3 REST client. Uses query-string API (not OAuth).
    Costs are 'units' deducted per call; Guru plan = 30k/mo."""

    BASE = "https://api.semrush.com/"

    def __init__(self, api_key: str,
                  domain: str = "wired4signsusa.com",
                  database: str = "us",
                  throttle_s: float = 0.5):
        if not api_key:
            raise RuntimeError("SEMRUSH_API_KEY required")
        self.api_key = api_key.strip()
        self.domain = domain.strip()
        self.database = database.strip()
        self.throttle_s = throttle_s
        self.session = requests.Session()
        self._last_call = 0.0

    def _throttle(self) -> None:
        elapsed = time.time() - self._last_call
        if elapsed < self.throttle_s:
            time.sleep(self.throttle_s - elapsed)
        self._last_call = time.time()

    def _get(self, params: dict) -> Optional[str]:
        merged = dict(params)
        merged["key"] = self.api_key
        for attempt in range(3):
            self._throttle()
            try:
                r = self.session.get(self.BASE, params=merged,
                                          timeout=30)
            except requests.RequestException as exc:
                log.warning("network error: %s; retry %d",
                              exc, attempt)
                time.sleep(2 ** attempt)
                continue
            if r.status_code != 200:
                log.error("HTTP %d on SEMrush call %s: %s",
                            r.status_code, params.get("type", "?"),
                            r.text[:200])
                return None
            text = r.text.strip()
            # SEMrush returns errors as plain text:
            #   ERROR 50 :: NOTHING FOUND
            if text.startswith("ERROR"):
                log.warning("SEMrush API error: %s", text)
                return None
            return text
        return None

    def domain_organic_positions(self, limit: int = 100,
                                       offset: int = 0
                                       ) -> Optional[List[dict]]:
        """Top organic positions for our domain. Costs 10 units per
        result row. Returns parsed list of dicts."""
        params = {
            "type": "domain_organic",
            "domain": self.domain,
            "database": self.database,
            "display_limit": limit,
            "display_offset": offset,
            "export_columns": "Ph,Po,Pp,Pd,Nq,Cp,Ur,Tr,Tc,Co,Nr,Td",
        }
        text = self._get(params)
        if not text:
            return None
        return _parse_csv(text)


# ---------------------------------------------------------------------------
# CSV parsing
# ---------------------------------------------------------------------------
def _parse_csv(text: str) -> List[dict]:
    """SEMrush returns semicolon-separated CSV with header row.
    Format: header1;header2;...\n value1;value2;..."""
    lines = text.strip().split("\n")
    if len(lines) < 2:
        return []
    header = lines[0].split(";")
    out: List[dict] = []
    for line in lines[1:]:
        if not line.strip():
            continue
        parts = line.split(";")
        row = {}
        for i, h in enumerate(header):
            row[h] = parts[i] if i < len(parts) else None
        out.append(row)
    return out


# Column name aliases — SEMrush uses short codes per export_columns.
# Reference: https://developer.semrush.com/api/v3/analytics/domain-reports/#domain-organic-search-keywords
_COL_ALIAS = {
    "Ph": "keyword",          # phrase
    "Po": "position",         # current position
    "Pp": "previous_position",
    "Pd": "position_difference",
    "Nq": "search_volume",    # search volume
    "Cp": "cpc",              # cost-per-click
    "Ur": "url",              # ranking URL
    "Tr": "traffic_share",    # estimated traffic share
    "Tc": "traffic_cost",
    "Co": "competition",
    "Nr": "results_count",
    "Td": "trend",
}


# ---------------------------------------------------------------------------
# URL -> SKU resolution
# ---------------------------------------------------------------------------
_HANDLE_LOOKUP_CACHE: Optional[Dict[str, dict]] = None


def _build_handle_lookup() -> Dict[str, dict]:
    """{shopify_handle: {family, title}} from product_dimensions."""
    global _HANDLE_LOOKUP_CACHE
    if _HANDLE_LOOKUP_CACHE is not None:
        return _HANDLE_LOOKUP_CACHE
    rows = db.all_product_dimensions()
    out: Dict[str, dict] = {}
    for r in rows:
        h = (r.get("shopify_handle") or "").strip()
        if h:
            out[h] = {
                "family": r.get("family") or "",
                "title": r.get("title") or "",
            }
    _HANDLE_LOOKUP_CACHE = out
    return out


def _extract_handle(url: str) -> Optional[str]:
    """A SEMrush ranking URL like
    'https://www.wired4signsusa.com/products/slim-led-channel-slim8-ac2-z'
    -> 'slim-led-channel-slim8-ac2-z'.
    Also handles /collections/, in which case we return the
    collection handle prefixed with 'col:' to distinguish."""
    if not url:
        return None
    try:
        parsed = _urlparse.urlparse(url)
        path = parsed.path
        if "/products/" in path:
            handle = (path.split("/products/", 1)[1]
                          .split("/")[0].split("?")[0].strip())
            return handle or None
        if "/collections/" in path:
            handle = (path.split("/collections/", 1)[1]
                          .split("/")[0].split("?")[0].strip())
            return f"col:{handle}" if handle else None
    except Exception:
        return None
    return None


def _resolve_url_to_product(url: str) -> dict:
    """Returns {family, sku, shopify_handle} for a URL.
    sku is None for collection pages."""
    handle = _extract_handle(url)
    if not handle:
        return {}
    if handle.startswith("col:"):
        # Collection page — no individual SKU
        return {
            "family": "",
            "sku": None,
            "shopify_handle": handle,
        }
    lookup = _build_handle_lookup()
    info = lookup.get(handle, {})
    return {
        "family": info.get("family") or "",
        "sku": None,    # SKU is per-variant; we have product-level
        "shopify_handle": handle,
    }


# ---------------------------------------------------------------------------
# Sync flows
# ---------------------------------------------------------------------------
def _safe_float(v: Any) -> Optional[float]:
    try:
        return float(v) if v not in (None, "") else None
    except (TypeError, ValueError):
        return None


def _safe_int(v: Any) -> Optional[int]:
    try:
        return int(v) if v not in (None, "") else None
    except (TypeError, ValueError):
        return None


def _flatten_position_row(r: dict) -> dict:
    """SEMrush domain_organic row -> our DB schema."""
    # Translate short column names to long
    expanded = {}
    for k, v in r.items():
        long_k = _COL_ALIAS.get(k, k)
        expanded[long_k] = v
    info = _resolve_url_to_product(expanded.get("url") or "")
    return {
        "keyword": expanded.get("keyword"),
        "url": expanded.get("url"),
        "sku": info.get("sku"),
        "family": info.get("family") or "",
        "position": _safe_float(expanded.get("position")),
        "previous_position": _safe_float(
            expanded.get("previous_position")),
        "search_volume": _safe_int(expanded.get("search_volume")),
        "serp_features": None,
        "source": "semrush",
        "captured_at": datetime.now(timezone.utc).isoformat(),
    }


def sync_domain_organic(client: SEMrushClient,
                            limit: int = 500
                            ) -> dict:
    """Pull top organic positions for our domain. limit=500 means
    we get the top 500 keywords by traffic — about 5,000 units."""
    log.info("Pulling top %d organic positions for %s",
              limit, client.domain)
    n_written = 0
    n_skipped = 0
    page_size = 100  # SEMrush max per call
    offset = 0
    while offset < limit:
        batch_size = min(page_size, limit - offset)
        rows = client.domain_organic_positions(
            limit=batch_size, offset=offset)
        if not rows:
            break
        log.info("  offset=%d -> %d rows", offset, len(rows))
        for r in rows:
            row = _flatten_position_row(r)
            if not row.get("keyword"):
                n_skipped += 1
                continue
            try:
                db.upsert_seo_keyword_position(row)
                n_written += 1
            except Exception as exc:
                log.error("upsert failed: %s", exc)
                n_skipped += 1
        offset += batch_size
        if len(rows) < batch_size:
            break  # end of results

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


def _make_client() -> SEMrushClient:
    api_key = os.environ.get("SEMRUSH_API_KEY", "").strip()
    if not api_key:
        raise SystemExit("SEMRUSH_API_KEY env var not set")
    domain = os.environ.get(
        "SEMRUSH_DOMAIN", "wired4signsusa.com").strip()
    database = os.environ.get("SEMRUSH_DATABASE", "us").strip()
    throttle = float(os.environ.get("SEMRUSH_THROTTLE_S", "0.5"))
    return SEMrushClient(api_key, domain=domain, database=database,
                            throttle_s=throttle)


def cmd_weekly(args: argparse.Namespace) -> int:
    """Top 500 organic keywords by traffic. ~5,000 units/run."""
    _setup_log(args.verbose)
    client = _make_client()
    result = sync_domain_organic(client, limit=args.limit)
    log.info("DONE: %s", result)
    return 0


def cmd_top(args: argparse.Namespace) -> int:
    _setup_log(args.verbose)
    client = _make_client()
    result = sync_domain_organic(client, limit=args.limit)
    log.info("DONE: %s", result)
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Sync SEMrush keyword positions into local DB")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_w = sub.add_parser("weekly",
                            help="Top organic keywords (~500 by "
                                  "default). Run weekly.")
    p_w.add_argument("--limit", type=int, default=500)
    p_w.add_argument("--verbose", action="store_true")
    p_w.set_defaults(func=cmd_weekly)

    p_t = sub.add_parser("top",
                            help="Top N keywords. Tunable.")
    p_t.add_argument("--limit", type=int, default=50)
    p_t.add_argument("--verbose", action="store_true")
    p_t.set_defaults(func=cmd_top)

    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
