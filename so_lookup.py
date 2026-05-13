"""so_lookup.py (v2.67.149)
=============================

CIN7 Sale Order ↔ Shopify Order cross-reference.

When anyone posts a message mentioning an SO-NNNNN in any channel
the bot listens to, post a short reply with the cross-reference
plus hyperlinks to both systems so staff can click-through fast.

Example reply:
  Sale [#SO-56168](https://inventory.dearsystems.com/Sale/Index/<uuid>)
  in Cin7 is Order [#42514](https://admin.shopify.com/store/wired4signs-usa/orders/<id>)
  in Shopify

Data sources:
  - sales_last_*d_*.csv (cin7_sync)  — has CIN7 ID, OrderNumber,
    Reference (the Shopify Order # — typically with a leading #)
  - shopify_orders_*.csv (shopify_sync) — has id (numeric Shopify
    internal), order_number (the customer-facing 42514 form)

Env vars:
  CIN7_SALE_URL_TEMPLATE   default https://inventory.dearsystems.com/Sale/Index/{id}
  SHOPIFY_STORE_SLUG       default wired4signs-usa
  SHOPIFY_ORDER_URL_TEMPLATE  default
    https://admin.shopify.com/store/{slug}/orders/{id}
"""

from __future__ import annotations

import glob
import logging
import os
import re
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd

try:
    from data_paths import OUTPUT_DIR
except ImportError:
    OUTPUT_DIR = Path(__file__).resolve().parent / "output"

log = logging.getLogger("so_lookup")

# Cache the cross-reference index for 5 min. Builds from
# sales_*.csv on first call; rebuilds on TTL expiry so newly
# created sales become resolvable shortly after they sync.
_CACHE_TTL_S = 300
_cache: dict = {
    "by_so": None,        # SO-XXXXX -> {cin7_id, shopify_order_num}
    "by_shop_num": None,  # 42514 -> {shopify_id, order_name}
    "loaded_at": 0.0,
}


_SO_PATTERNS = re.compile(
    r"\b(SO[-]?\d{4,})\b", re.IGNORECASE)
_INV_PATTERNS = re.compile(
    r"\b(INV[-]?\d{4,})\b", re.IGNORECASE)
_HASH_REF_PATTERNS = re.compile(
    r"#(\d{4,6})")  # for stripping leading # from CIN7 Reference


def _find_latest_csv(pattern: str) -> Optional[Path]:
    matches = glob.glob(str(OUTPUT_DIR / pattern))
    if not matches:
        return None
    return Path(max(matches, key=os.path.getmtime))


def _load_indexes() -> None:
    """Build SO→Shopify and Shopify-#→Shopify-id lookup tables.
    Cached for _CACHE_TTL_S. Re-runs on TTL expiry."""
    now = time.time()
    if (_cache["by_so"] is not None
            and now - _cache["loaded_at"] < _CACHE_TTL_S):
        return

    by_so: Dict[str, dict] = {}
    by_shop_num: Dict[str, dict] = {}

    # CIN7 sales side — OrderNumber (SO-) + Reference (Shopify #)
    sales_path = _find_latest_csv("sales_last_*d_*.csv")
    if sales_path:
        try:
            df = pd.read_csv(sales_path)
        except Exception as exc:
            log.error("Failed to load %s: %s", sales_path, exc)
            df = None
        if df is not None and not df.empty:
            so_col = next(
                (c for c in ("OrderNumber", "SaleNumber")
                  if c in df.columns), None)
            id_col = next(
                (c for c in ("ID", "Id", "SaleID")
                  if c in df.columns), None)
            ref_col = next(
                (c for c in ("Reference", "ExternalReference")
                  if c in df.columns), None)
            for _, row in df.iterrows():
                so = (str(row.get(so_col) or "").strip().upper()
                        if so_col else "")
                if not so:
                    continue
                cin7_id = (str(row.get(id_col) or "").strip()
                              if id_col else "")
                ref_raw = (str(row.get(ref_col) or "").strip()
                              if ref_col else "")
                # Reference may be "#42514" — strip to "42514"
                shop_num = ""
                if ref_raw:
                    m = _HASH_REF_PATTERNS.search(ref_raw)
                    if m:
                        shop_num = m.group(1)
                by_so[so] = {
                    "cin7_id": cin7_id,
                    "shopify_order_num": shop_num,
                    "reference_raw": ref_raw,
                }
    log.info(
        "Loaded SO index from %s (%d entries)",
        sales_path, len(by_so))

    # Shopify orders side — order_number (42514) + id (numeric)
    shop_path = _find_latest_csv("shopify_orders_*.csv")
    if not shop_path:
        # Some accounts split into shopify_orders.json or similar;
        # try shopify_orders.csv (no date suffix) too.
        shop_path = _find_latest_csv("shopify_orders.csv")
    if shop_path:
        try:
            df = pd.read_csv(shop_path)
        except Exception as exc:
            log.warning("Failed to load %s: %s", shop_path, exc)
            df = None
        if df is not None and not df.empty:
            num_col = next(
                (c for c in ("order_number", "OrderNumber",
                                "Order Number", "number")
                  if c in df.columns), None)
            id_col = next(
                (c for c in ("id", "ID", "OrderId", "Id",
                                "shopify_id")
                  if c in df.columns), None)
            name_col = next(
                (c for c in ("name", "order_name", "Name")
                  if c in df.columns), None)
            for _, row in df.iterrows():
                num = (str(row.get(num_col) or "").strip()
                        if num_col else "")
                # Normalise: strip leading # if present
                num = num.lstrip("#").strip()
                if not num:
                    continue
                shop_id = (str(row.get(id_col) or "").strip()
                              if id_col else "")
                name = (str(row.get(name_col) or "").strip()
                          if name_col else "")
                by_shop_num[num] = {
                    "shopify_id": shop_id,
                    "order_name": name,
                }
    log.info(
        "Loaded Shopify orders index from %s (%d entries)",
        shop_path, len(by_shop_num))

    _cache["by_so"] = by_so
    _cache["by_shop_num"] = by_shop_num
    _cache["loaded_at"] = now


def _cin7_sale_url(cin7_id: str) -> str:
    tpl = os.environ.get(
        "CIN7_SALE_URL_TEMPLATE",
        "https://inventory.dearsystems.com/Sale/Index/{id}")
    return tpl.format(id=cin7_id)


def _shopify_order_url(shopify_id: str) -> str:
    tpl = os.environ.get(
        "SHOPIFY_ORDER_URL_TEMPLATE",
        "https://admin.shopify.com/store/{slug}/orders/{id}")
    slug = os.environ.get(
        "SHOPIFY_STORE_SLUG", "wired4signs-usa")
    return tpl.format(slug=slug, id=shopify_id)


def find_so_references(text: str) -> List[str]:
    """Return distinct SO-XXXXX (normalised, uppercase) found in
    the message. Returns empty list if none — caller skips."""
    if not text:
        return []
    found: List[str] = []
    seen: set = set()
    for m in _SO_PATTERNS.finditer(text):
        raw = m.group(1).upper()
        # Normalise — ensure SO- prefix
        if not raw.startswith("SO-"):
            raw = "SO-" + raw[2:]
        if raw not in seen:
            seen.add(raw)
            found.append(raw)
    return found


def lookup_so(so_number: str) -> Optional[dict]:
    """Resolve an SO-NNNNN to its cross-reference + URLs.
    Returns dict with cin7_url, shopify_url (may be empty if no
    Shopify side match), shopify_order_num (display), shopify_id.

    Returns None if the SO can't be found at all (not in the
    sales CSV window).
    """
    if not so_number:
        return None
    _load_indexes()
    so_u = so_number.strip().upper()
    by_so = _cache["by_so"] or {}
    by_shop_num = _cache["by_shop_num"] or {}
    rec = by_so.get(so_u)
    if not rec:
        return None
    cin7_id = rec.get("cin7_id") or ""
    shop_num = rec.get("shopify_order_num") or ""
    cin7_url = _cin7_sale_url(cin7_id) if cin7_id else ""
    shopify_url = ""
    shopify_id = ""
    if shop_num:
        shop_rec = by_shop_num.get(shop_num) or {}
        shopify_id = shop_rec.get("shopify_id") or ""
        if shopify_id:
            shopify_url = _shopify_order_url(shopify_id)
    return {
        "so_number": so_u,
        "cin7_id": cin7_id,
        "cin7_url": cin7_url,
        "shopify_order_num": shop_num,
        "shopify_id": shopify_id,
        "shopify_url": shopify_url,
    }


def compose_reply(records: List[dict]) -> str:
    """Build the cross-reference Slack message. One line per SO;
    blank if records is empty."""
    if not records:
        return ""
    lines: List[str] = []
    for rec in records:
        so_disp = rec["so_number"]
        # CIN7 side — hyperlink if we have the UUID
        if rec.get("cin7_url"):
            so_part = f"<{rec['cin7_url']}|#{so_disp}>"
        else:
            so_part = f"#{so_disp}"
        # Shopify side
        shop_num = rec.get("shopify_order_num")
        if shop_num:
            if rec.get("shopify_url"):
                shop_part = (
                    f"Order <{rec['shopify_url']}|#{shop_num}> "
                    f"in Shopify")
            else:
                shop_part = f"Order #{shop_num} in Shopify"
            lines.append(
                f"Sale {so_part} in Cin7 is {shop_part}")
        else:
            # No Shopify cross-ref found (could be a non-Shopify
            # sale — manual phone order, B2B portal, etc.)
            lines.append(
                f"Sale {so_part} in Cin7 — no Shopify "
                f"cross-reference (likely non-Shopify channel)")
    return "\n".join(lines)


def handle_message(text: str) -> Optional[str]:
    """Top-level helper for the classifier. Returns the reply
    text, or None if there's nothing useful to say."""
    sos = find_so_references(text)
    if not sos:
        return None
    records: List[dict] = []
    for so in sos[:5]:  # cap to avoid mega-replies
        rec = lookup_so(so)
        if rec:
            records.append(rec)
    if not records:
        return None
    return compose_reply(records)
