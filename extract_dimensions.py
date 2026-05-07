"""extract_dimensions.py (v2.67.73)
=========================================

Vision-based dimension extractor for Shopify product images.

Why this exists
---------------
CIN7's Length / Width / Height / Weight fields are largely empty for
LED profiles (per dimension_describer.py smoke test, 5,000+ SKUs lack
any dimensional data). The actual numbers — outer width, channel
width, wing geometry, mounting type — live in cross-section
spec diagrams (PNGs) embedded in Shopify product image arrays.

Example: the Slim8 LED profile has a diagram showing
  outer 12.2mm × 7mm, channel 8mm wide, surface mount, U-shape.
None of those numbers are in CIN7 or in the Shopify description text.
They're in the image.

This script:
  1. Pulls every active Shopify product (handle, title, image URLs,
     variant SKUs).
  2. For each product, sends up to 5 image URLs to Claude Sonnet
     vision asking it to identify any cross-section spec diagram and
     extract the dimensions as structured JSON.
  3. Caches the result in db.product_dimensions, keyed on
     shopify_handle.
  4. dimension_describer.py + ai_tools.py + slack_listener.py all
     read from this table — instant, deterministic, single source.

Cost model (Sonnet 4.5 vision):
  ~$0.015–0.020 per product (3-5 images @ ~1600 tokens each).
  5,000 LED products ≈ $75–100 one-off.
  Weekly refresh of new/changed products ≈ $1–2/week.

CLI:
  # Dry-run on a single Shopify handle (test before full spend):
  python extract_dimensions.py one --handle slim8 --dry-run

  # Real extraction on a single product (caches result):
  python extract_dimensions.py one --handle slim8

  # Full catalog, skipping products already extracted:
  python extract_dimensions.py all

  # Full catalog, force re-extract everything:
  python extract_dimensions.py all --force

  # Limit to N products (useful for cost-bounded testing):
  python extract_dimensions.py all --limit 100

Env vars required:
  SHOPIFY_DOMAIN
  SHOPIFY_ACCESS_TOKEN
  ANTHROPIC_API_KEY

Optional:
  ANTHROPIC_MODEL_VISION  default 'claude-sonnet-4-5-20250929'
  EXTRACT_DIM_MAX_IMAGES  default 5  (per product)
  EXTRACT_DIM_REQ_DELAY_S default 0.4  (rate-limit cushion)
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

# These already exist in the repo and are battle-tested.
from shopify_sync import ShopifyClient  # noqa: E402

import db  # noqa: E402

try:
    import anthropic  # type: ignore
except Exception as exc:  # pragma: no cover
    anthropic = None  # type: ignore
    _ANTHROPIC_IMPORT_ERR = exc
else:
    _ANTHROPIC_IMPORT_ERR = None


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
DEFAULT_MODEL = "claude-sonnet-4-5-20250929"
MAX_IMAGES_PER_PRODUCT = int(os.environ.get(
    "EXTRACT_DIM_MAX_IMAGES", "5"))
REQ_DELAY_SECONDS = float(os.environ.get(
    "EXTRACT_DIM_REQ_DELAY_S", "0.4"))


SYSTEM_PROMPT = """You are a precise technical-spec extractor for an
LED-profile retailer (Wired4Signs USA). The user will send you 1–5
images from a Shopify product page. ONE OR MORE may be a
**technical cross-section diagram** showing the profile's dimensions
in millimetres (sometimes also in inches as a secondary annotation).
The rest are typically lifestyle photos, packaging shots, or
application examples.

Your job:
1. Identify whether ANY of the images is a technical cross-section
   diagram with numerical dimensions.
2. If yes, extract the dimensions and return them as JSON in the
   exact schema below. If multiple diagrams exist (e.g. one for the
   profile + one for the cover), use the PROFILE diagram.
3. If no diagram is present, return {"has_diagram": false}.

Return JSON only — no prose, no markdown fences, just the raw JSON
object. The schema is:

{
  "has_diagram": true | false,
  "source_image_position": 1-based index of the image you used,
  "outer_width_mm": number or null,
  "outer_height_mm": number or null,
  "channel_width_mm": number or null,    // LED-strip recess width
  "channel_depth_mm": number or null,    // LED-strip recess depth
  "wing_width_mm": number or null,       // each wing/flange (one side)
  "wing_count": 0|1|2|null,              // typically 0, or 2 for mud-in
  "mounting_type": "surface"|"recessed"|"mud-in"|"corner"|"pendant"|"unknown",
  "profile_shape": "U"|"square"|"angled"|"round"|"oval"|"wing"|"unknown",
  "has_clip_lips": true | false | null,  // top edges grip a cover?
  "max_strip_width_mm": number or null,  // strip that fits in channel
  "extra_notes": "short sentence on anything unusual",
  "confidence": "high" | "medium" | "low"
}

Rules:
- mm only. If the diagram shows inches, convert (1 inch = 25.4 mm).
- If a value isn't shown on the diagram, return null. Do NOT guess.
- "outer_width" = total external width of the profile body (the
  widest external dimension). "outer_height" = total external height.
- "channel_width" = width of the slot that holds the LED strip.
- For mud-in / drywall profiles, set wing_count to 2 and wing_width
  to one side's wing extension.
- Return ONLY the JSON object. No preamble.

MOUNTING TYPE CLASSIFICATION (Wired4Signs trade conventions):

DEFAULT: when the cross-section is a plain U-shape, square, or
rectangle with NO wide flanges extending beyond the body, classify
as "surface". Most LED channels are surface mount. Small internal
clip ribs or top-edge cover-retention lips do NOT count as flanges.

Use "mud-in" ONLY when wide flanges/wings clearly extend BEYOND the
profile body on one or both sides — the wings are designed to be
plastered into drywall, leaving only the diffuser visible. The
flanges are typically 8–25 mm wider than the body itself, and look
like horizontal "ears" sticking out from the sides of the body.
If you see ears, set wing_count and wing_width.

Use "recessed" ONLY when the diagram or product context makes clear
the profile is designed to sit FULLY FLUSH inside a routed groove
(no part of the body visible above the mounting surface, only the
diffuser shows). This is rare. If unsure between "surface" and
"recessed", choose "surface".

Use "corner" when the body cross-section is triangular or has
visible 45° angled mounting faces (designed for 90° corners).

Use "pendant" when there is an additional groove/channel on TOP
of the body for a suspension cable.

Use "unknown" only if the diagram is too unclear to classify.

MOUNTING-TYPE SYNONYMS (Wired4Signs trade language):

Treat these as ALL meaning the same mounting_type. Always emit
the canonical value on the right.

  Drywall            → "mud-in"
  Mud-in / Mud in    → "mud-in"
  Plaster-in         → "mud-in"
  Plaster mount      → "mud-in"
  Trimless           → "mud-in"
  Recessed-flange    → "mud-in"
  Flange-mount       → "mud-in"

  Surface            → "surface"
  Surface mount      → "surface"
  SMD mount          → "surface"
  Top mount          → "surface"

  Recessed           → "recessed"  (only when fully flush-fit, no
                                    flanges; rare)
  In-groove          → "recessed"
  Flush mount        → "recessed"

  Corner             → "corner"
  45-degree / 45 deg → "corner"
  Angle              → "corner"

  Pendant            → "pendant"
  Suspended          → "pendant"
  Hanging            → "pendant"

USING SHOPIFY COLLECTIONS AS CLASSIFICATION HINTS:

If the user-supplied product context lists Shopify collections this
product belongs to, treat them as MERCHANDISER-CURATED ground truth
for category questions. Match using the synonym table above.
Examples:
- A collection named "Mud-In Channels" or "Plaster-In Profiles" or
  "Drywall LED Channels" or "Trimless Profiles" → mounting_type =
  "mud-in" (regardless of what the diagram alone might suggest).
- "Surface Mount Channels" → mounting_type = "surface".
- "Recessed Channels" → mounting_type = "recessed".
- "Corner Profiles" or "45 Degree Channels" → "corner".
- "Pendant Profiles" or "Suspended Profiles" → "pendant".

When collections and diagram disagree, COLLECTIONS WIN.

USING SHOPIFY METAFIELDS:

Metafields are structured product data set by the merchandiser. They
override anything inferred from the diagram. If a metafield like
'mounting_type' or 'outer_width_mm' is present, use it as the value.
Apply the synonym table above to normalise mounting_type values.
The user-supplied prompt will list metafields explicitly with their
namespace.key and value."""


log = logging.getLogger("extract_dimensions")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _setup_log(verbose: bool = False) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        stream=sys.stdout,
    )


def _make_shopify_client() -> ShopifyClient:
    domain = os.environ.get("SHOPIFY_DOMAIN", "").strip()
    token = os.environ.get("SHOPIFY_ACCESS_TOKEN", "").strip()
    if not domain or not token:
        raise SystemExit("SHOPIFY_DOMAIN + SHOPIFY_ACCESS_TOKEN required")
    return ShopifyClient(domain, token)


def _fetch_product(client: ShopifyClient,
                     handle: str) -> Optional[dict]:
    """Pull a single product by handle. Shopify's products.json
    endpoint accepts ?handle=foo as a filter on 2024+ API versions."""
    try:
        url = f"{client.base}/products.json"
        r = client._get(url, params={"handle": handle, "limit": 1})
        if r.status_code != 200:
            log.warning("fetch_product(%s) -> HTTP %d: %s",
                          handle, r.status_code, r.text[:200])
            return None
        prods = (r.json() or {}).get("products", [])
        return prods[0] if prods else None
    except Exception as exc:
        log.warning("fetch_product(%s) failed: %s", handle, exc)
        return None


def _fetch_all_products(client: ShopifyClient) -> List[dict]:
    """All products from Shopify (paginated)."""
    return client.paginate("products.json", "products")


# ---------------------------------------------------------------------------
# v2.67.76 — Collections + metafields enrichment
# ---------------------------------------------------------------------------
def _build_collections_index(client: ShopifyClient
                                ) -> Dict[str, List[str]]:
    """Pull every collection + its products to build
    {product_id: [collection_title, ...]}.

    Why this matters: collections like 'Mud-In Channels' or
    'Surface Mount Profiles' encode mounting type far more reliably
    than image interpretation. We pass collection memberships to
    the vision model as context so it can prefer the merchandiser's
    classification over its own visual inference."""
    log.info("Building collections index...")
    out: Dict[str, List[str]] = {}

    try:
        customs = client.paginate(
            "custom_collections.json", "custom_collections")
    except Exception as exc:
        log.warning("custom_collections fetch failed: %s", exc)
        customs = []
    try:
        smarts = client.paginate(
            "smart_collections.json", "smart_collections")
    except Exception as exc:
        log.warning("smart_collections fetch failed: %s", exc)
        smarts = []

    log.info("  %d custom + %d smart collections",
              len(customs), len(smarts))

    for coll in customs + smarts:
        coll_id = coll.get("id")
        title = (coll.get("title") or "").strip()
        if not coll_id or not title:
            continue
        # Each collection has its own products endpoint.
        try:
            prods = client.paginate(
                f"collections/{coll_id}/products.json", "products")
        except Exception as exc:
            log.warning("collection %s products fetch failed: %s",
                          title, exc)
            continue
        for p in prods:
            pid = str(p.get("id") or "")
            if not pid:
                continue
            out.setdefault(pid, []).append(title)

    log.info("  index covers %d products", len(out))
    return out


def _fetch_metafields(client: ShopifyClient,
                        product_id: Any) -> List[dict]:
    """Fetch every metafield attached to one Shopify product.
    Returns list of {namespace, key, value, type} dicts. Empty
    list on any error — metafields are advisory, not required."""
    if not product_id:
        return []
    try:
        url = f"{client.base}/products/{product_id}/metafields.json"
        r = client._get(url)
        if r.status_code != 200:
            return []
        return (r.json() or {}).get("metafields", []) or []
    except Exception as exc:
        log.debug("metafields(%s) failed: %s", product_id, exc)
        return []


# Synonym table — normalise mounting_type values from metafields,
# collections, or vision so we always store the canonical value.
# Keys are lower-cased + space-stripped substrings to match against.
_MOUNTING_SYNONYMS = {
    # Mud-in family
    "drywall": "mud-in",
    "mud-in": "mud-in",
    "mud in": "mud-in",
    "mudin": "mud-in",
    "plaster-in": "mud-in",
    "plaster in": "mud-in",
    "plasterin": "mud-in",
    "plaster mount": "mud-in",
    "trimless": "mud-in",
    "recessed-flange": "mud-in",
    "recessed flange": "mud-in",
    "flange-mount": "mud-in",
    "flange mount": "mud-in",
    # Surface family
    "surface": "surface",
    "surface mount": "surface",
    "surface-mount": "surface",
    "smd mount": "surface",
    "top mount": "surface",
    # Recessed family
    "recessed": "recessed",
    "in-groove": "recessed",
    "in groove": "recessed",
    "flush mount": "recessed",
    "flush-mount": "recessed",
    # Corner family
    "corner": "corner",
    "45-degree": "corner",
    "45 degree": "corner",
    "45 deg": "corner",
    "angle": "corner",
    "angled": "corner",
    # Pendant family
    "pendant": "pendant",
    "suspended": "pendant",
    "hanging": "pendant",
}


def _normalise_mounting_type(raw: Optional[str]) -> Optional[str]:
    """Look up `raw` in the synonym table. Returns the canonical
    value, or the lowercased input if no match (so we don't lose
    data we don't recognise). None for falsy input."""
    if not raw:
        return None
    s = str(raw).lower().strip()
    if s in _MOUNTING_SYNONYMS:
        return _MOUNTING_SYNONYMS[s]
    # Try substring match — collection titles like
    # "Mud-In LED Channels for Drywall" should still resolve.
    for needle, canonical in _MOUNTING_SYNONYMS.items():
        if needle in s:
            return canonical
    return s


def _title_to_mounting_type(title: Optional[str]) -> Optional[str]:
    """Walk the synonym table against a product title. Returns the
    canonical mounting_type if any term matches, else None.

    Critically, this iterates synonyms in order — and the table
    starts with mud-in family terms — so a title like 'Recessed
    Drywall Channel' resolves to mud-in (drywall wins) rather than
    recessed. This matches W4S trade convention: drywall products
    are mud-in even if they also happen to sit flush.

    Differs from _normalise_mounting_type by returning None on no
    match (rather than the original string) — so callers can
    distinguish 'no signal' from 'unknown mount'."""
    if not title:
        return None
    s = str(title).lower()
    for needle, canonical in _MOUNTING_SYNONYMS.items():
        if needle in s:
            return canonical
    return None


# Metafield keys (any namespace) that explicitly carry dimension /
# classification data. If we find these we treat them as authoritative
# and overwrite vision's values.
_DIM_KEY_MAP = {
    # Outer dimensions
    "outer_width_mm": "outer_width_mm",
    "outer_width": "outer_width_mm",
    "width_mm": "outer_width_mm",
    "outer_height_mm": "outer_height_mm",
    "outer_height": "outer_height_mm",
    "height_mm": "outer_height_mm",
    # Channel
    "channel_width_mm": "channel_width_mm",
    "channel_width": "channel_width_mm",
    "channel_depth_mm": "channel_depth_mm",
    "channel_depth": "channel_depth_mm",
    # Strip fit
    "max_strip_width_mm": "max_strip_width_mm",
    "max_strip_width": "max_strip_width_mm",
    "compatible_strip_width": "max_strip_width_mm",
    # Wings
    "wing_width_mm": "wing_width_mm",
    "wing_count": "wing_count",
    # Classification
    "mounting_type": "mounting_type",
    "mount_type": "mounting_type",
    "profile_shape": "profile_shape",
}


def _metafields_to_dim_overrides(metafields: List[dict]) -> dict:
    """Walk metafields, return a dict of {dim_field: value} for any
    that match our known keys. Numeric fields are coerced to float."""
    out: dict = {}
    for m in metafields:
        key = (m.get("key") or "").lower().strip()
        target = _DIM_KEY_MAP.get(key)
        if not target:
            continue
        value = m.get("value")
        if value in (None, "", "null"):
            continue
        # Coerce numerics where appropriate.
        if target in ("outer_width_mm", "outer_height_mm",
                       "channel_width_mm", "channel_depth_mm",
                       "max_strip_width_mm", "wing_width_mm"):
            try:
                value = float(value)
            except (TypeError, ValueError):
                continue
        elif target == "wing_count":
            try:
                value = int(value)
            except (TypeError, ValueError):
                continue
        else:
            value = str(value).strip().lower()
            # Normalise mounting_type via synonym table.
            if target == "mounting_type":
                normed = _normalise_mounting_type(value)
                if normed:
                    value = normed
        out[target] = value
    return out


def _format_collections_for_prompt(titles: List[str]) -> str:
    if not titles:
        return ""
    return ", ".join(sorted(set(titles))[:12])


def _format_metafields_for_prompt(metafields: List[dict]) -> str:
    """Render a compact summary of metafields for the vision prompt.
    Caps at ~20 entries to keep token usage bounded."""
    if not metafields:
        return ""
    lines = []
    for m in metafields[:20]:
        ns = m.get("namespace") or ""
        k = m.get("key") or ""
        v = m.get("value")
        if v is None:
            continue
        v_str = str(v)
        if len(v_str) > 80:
            v_str = v_str[:77] + "..."
        lines.append(f"- {ns}.{k}: {v_str}")
    return "\n".join(lines)


def _is_likely_led_profile(prod: dict) -> bool:
    """Heuristic: only run vision on products that look like LED
    profiles / channels / kits. Skips lifestyle-only / accessory
    SKUs to avoid wasted API calls."""
    title = (prod.get("title") or "").lower()
    ptype = (prod.get("product_type") or "").lower()
    tags = (prod.get("tags") or "").lower()
    blob = f"{title} {ptype} {tags}"
    keywords = ("led", "profile", "channel", "extrusion", "strip",
                  "neon", "diffuser", "mud-in", "recessed",
                  "surface", "cove")
    return any(k in blob for k in keywords)


def _matches_filter(prod: dict, needle: str) -> bool:
    """Case-insensitive substring match against title / product_type
    / tags / handle. Used by --match to prioritise specific
    categories like 'channel'."""
    if not needle:
        return True
    n = needle.lower()
    return (n in (prod.get("title") or "").lower()
              or n in (prod.get("product_type") or "").lower()
              or n in (prod.get("tags") or "").lower()
              or n in (prod.get("handle") or "").lower())


def _pick_image_urls(prod: dict, cap: int) -> List[str]:
    """Sort product images by position and return up to `cap` URLs.
    The diagram is usually at position 2 or 3 — early images are
    typically the hero / lifestyle shots."""
    images = prod.get("images") or []
    images = sorted(images, key=lambda i: int(i.get("position", 99)))
    return [img["src"] for img in images[:cap] if img.get("src")]


def _variants_skus(prod: dict) -> List[str]:
    out = []
    for v in prod.get("variants") or []:
        sku = (v.get("sku") or "").strip()
        if sku:
            out.append(sku)
    return sorted(set(out))


def _family_from_skus(skus: List[str]) -> str:
    """Best-effort: pull the AdditionalAttribute1-style family
    prefix from the first SKU (e.g. LED-V3140020-... → V3140020).
    Falls back to empty string."""
    for sku in skus:
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


# ---------------------------------------------------------------------------
# Vision call
# ---------------------------------------------------------------------------
def _build_anthropic_client():
    if anthropic is None:  # pragma: no cover
        raise SystemExit(
            f"anthropic SDK not installed: {_ANTHROPIC_IMPORT_ERR}")
    api_key = os.environ.get("ANTHROPIC_API_KEY", "").strip()
    if not api_key:
        raise SystemExit("ANTHROPIC_API_KEY required")
    return anthropic.Anthropic(api_key=api_key)


def _call_vision(client: Any, image_urls: List[str],
                   product_title: str,
                   model: str,
                   collections_summary: str = "",
                   metafields_summary: str = ""
                   ) -> Dict[str, Any]:
    """One Anthropic vision call. Returns parsed JSON dict.
    Defensive: returns {'has_diagram': False, '_error': ...} on
    any failure."""
    if not image_urls:
        return {"has_diagram": False, "_error": "no_images"}

    content_blocks: List[dict] = []
    for i, url in enumerate(image_urls, start=1):
        content_blocks.append({
            "type": "image",
            "source": {"type": "url", "url": url},
        })

    context_lines = [
        f"Product title: {product_title}",
        f"Number of images attached: {len(image_urls)}",
        f"Image positions: 1..{len(image_urls)} in order.",
    ]
    if collections_summary:
        context_lines.append(
            f"\nShopify collections this product belongs to "
            f"(merchandiser-curated; PREFER these over visual "
            f"inference for mounting_type and profile category):\n"
            f"  {collections_summary}")
    if metafields_summary:
        context_lines.append(
            f"\nShopify metafields (structured product data; "
            f"AUTHORITATIVE — if a metafield contradicts the "
            f"diagram, the metafield is correct):\n"
            f"{metafields_summary}")
    context_lines.append("\nExtract dimensions per the schema.")

    content_blocks.append({
        "type": "text",
        "text": "\n".join(context_lines),
    })

    try:
        resp = client.messages.create(
            model=model,
            max_tokens=600,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": content_blocks}],
        )
    except Exception as exc:
        log.warning("vision call failed: %s", exc)
        return {"has_diagram": False, "_error": str(exc)}

    text = "".join(b.text for b in resp.content
                     if hasattr(b, "text")).strip()
    if not text:
        return {"has_diagram": False, "_error": "empty_response"}

    # Strip markdown fences if model added them despite instructions.
    if text.startswith("```"):
        lines = text.splitlines()
        if lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].startswith("```"):
            lines = lines[:-1]
        text = "\n".join(lines).strip()

    try:
        data = json.loads(text)
    except json.JSONDecodeError as exc:
        log.warning("vision returned non-JSON for product '%s': %s",
                      product_title, exc)
        return {"has_diagram": False, "_error": "bad_json",
                  "_raw": text[:500]}

    data["_raw"] = text
    return data


# ---------------------------------------------------------------------------
# Per-product extraction
# ---------------------------------------------------------------------------
def _extract_one(prod: dict, anthropic_client: Any,
                   model: str, dry_run: bool = False,
                   shopify_client: Optional[ShopifyClient] = None,
                   collections_index: Optional[Dict[str, List[str]]] = None,
                   ) -> Optional[dict]:
    """Run extraction for a single Shopify product. Returns the
    parsed result dict (also persists to DB unless dry_run=True).

    v2.67.76: when shopify_client + collections_index are supplied,
    we enrich the vision prompt with collection memberships +
    metafields, AND apply metafield overrides on the final result
    (metafield values are authoritative)."""
    handle = prod.get("handle") or ""
    title = prod.get("title") or handle
    pid = prod.get("id")
    skus = _variants_skus(prod)
    family = _family_from_skus(skus)
    image_urls = _pick_image_urls(prod, MAX_IMAGES_PER_PRODUCT)
    if not image_urls:
        log.info("[%s] no images — skipping", handle)
        return None

    # Pull metafields + collection memberships if we have a client.
    metafields: List[dict] = []
    collections: List[str] = []
    if shopify_client is not None:
        metafields = _fetch_metafields(shopify_client, pid)
        if collections_index is not None:
            collections = collections_index.get(str(pid), [])
        else:
            # one-product path: just fetch this product's collects
            collections = []

    coll_summary = _format_collections_for_prompt(collections)
    meta_summary = _format_metafields_for_prompt(metafields)

    log.info("[%s] %d images, family=%s, %d variants, "
              "%d collections, %d metafields",
              handle, len(image_urls), family or "?",
              len(skus), len(collections), len(metafields))

    result = _call_vision(anthropic_client, image_urls, title, model,
                            collections_summary=coll_summary,
                            metafields_summary=meta_summary)
    has_diagram = bool(result.get("has_diagram"))

    # Normalise mounting_type from vision output via synonym table.
    if result.get("mounting_type"):
        normed = _normalise_mounting_type(result["mounting_type"])
        if normed and normed != result["mounting_type"]:
            log.info("[%s] mounting_type normalised: %s -> %s",
                      handle, result["mounting_type"], normed)
            result["mounting_type"] = normed

    # Collection-driven mounting type: if any of the product's
    # collections match a synonym, it WINS over diagram inference.
    for coll_title in collections:
        canonical = _normalise_mounting_type(coll_title)
        if canonical in ("mud-in", "surface", "recessed",
                          "corner", "pendant"):
            if result.get("mounting_type") != canonical:
                log.info("[%s] mounting_type from collection '%s': "
                          "%s -> %s",
                          handle, coll_title,
                          result.get("mounting_type"), canonical)
                result["mounting_type"] = canonical
            break

    # v2.67.78 — title-driven mounting type override.
    # Title is the strongest signal because it's what merchandisers
    # actually wrote. If the title contains 'drywall', 'plaster-in',
    # 'trimless', or 'mud-in', force mud-in regardless of vision.
    # Same for 'surface mount', 'corner', 'pendant' etc.
    title_canonical = _title_to_mounting_type(title)
    if title_canonical:
        if result.get("mounting_type") != title_canonical:
            log.info("[%s] mounting_type from TITLE: %s -> %s "
                      "(title='%s')",
                      handle, result.get("mounting_type"),
                      title_canonical, title)
            result["mounting_type"] = title_canonical

    # v2.67.76 — apply metafield overrides. Merchandiser-curated
    # metafields are authoritative over visual inference.
    overrides = _metafields_to_dim_overrides(metafields)
    if overrides:
        for k, v in overrides.items():
            result[k] = v
        log.info("[%s] metafield overrides applied: %s",
                  handle, list(overrides.keys()))
        # If metafields supplied core dims, mark has_diagram=true
        # even if vision said no — we still have authoritative data.
        if any(k in overrides for k in
                  ("outer_width_mm", "outer_height_mm",
                   "channel_width_mm")):
            result["has_diagram"] = True
            has_diagram = True
            if not result.get("confidence"):
                result["confidence"] = "high"

    log.info("[%s] has_diagram=%s confidence=%s",
              handle, has_diagram, result.get("confidence"))
    if has_diagram:
        log.info("[%s]   %sx%s mm, channel %s mm, mount=%s, shape=%s",
                  handle,
                  result.get("outer_width_mm"),
                  result.get("outer_height_mm"),
                  result.get("channel_width_mm"),
                  result.get("mounting_type"),
                  result.get("profile_shape"))

    if dry_run:
        log.info("[%s] DRY RUN — not persisting", handle)
        return result

    src_pos = result.get("source_image_position")
    src_url = None
    if isinstance(src_pos, int) and 1 <= src_pos <= len(image_urls):
        src_url = image_urls[src_pos - 1]

    has_clip = result.get("has_clip_lips")
    has_clip_int = (1 if has_clip is True
                     else 0 if has_clip is False else None)

    row = {
        "shopify_product_id": str(prod.get("id") or ""),
        "shopify_handle": handle,
        "family": family,
        "title": title,
        "source_image_url": src_url,
        "source_image_position": src_pos,
        "outer_width_mm": result.get("outer_width_mm"),
        "outer_height_mm": result.get("outer_height_mm"),
        "channel_width_mm": result.get("channel_width_mm"),
        "channel_depth_mm": result.get("channel_depth_mm"),
        "wing_width_mm": result.get("wing_width_mm"),
        "wing_count": result.get("wing_count"),
        "mounting_type": result.get("mounting_type"),
        "profile_shape": result.get("profile_shape"),
        "has_clip_lips": has_clip_int,
        "max_strip_width_mm": result.get("max_strip_width_mm"),
        "extra_notes": result.get("extra_notes"),
        "raw_response": result.get("_raw") or json.dumps(result),
        "confidence": result.get("confidence"),
        "has_diagram": 1 if has_diagram else 0,
        "model_used": model,
        "extracted_at": datetime.now(timezone.utc).isoformat(),
    }

    db.upsert_product_dimensions(row)
    return result


# ---------------------------------------------------------------------------
# Subcommands
# ---------------------------------------------------------------------------
def _fetch_collections_for_one_product(client: ShopifyClient,
                                          product_id: Any
                                          ) -> List[str]:
    """For single-product runs, walk collects.json to find the
    collection IDs this product belongs to, then resolve titles.
    Cheaper than building the full index for one lookup."""
    if not product_id:
        return []
    titles: List[str] = []
    try:
        url = f"{client.base}/collects.json"
        r = client._get(url, params={"product_id": product_id})
        if r.status_code != 200:
            return []
        collects = (r.json() or {}).get("collects", []) or []
    except Exception:
        return []
    for c in collects:
        cid = c.get("collection_id")
        if not cid:
            continue
        # Try custom_collections then smart_collections.
        for endpoint in ("custom_collections", "smart_collections"):
            try:
                cu = f"{client.base}/{endpoint}/{cid}.json"
                rc = client._get(cu)
                if rc.status_code == 200:
                    blob = (rc.json() or {})
                    item = (blob.get("custom_collection")
                              or blob.get("smart_collection") or {})
                    t = (item.get("title") or "").strip()
                    if t:
                        titles.append(t)
                    break
            except Exception:
                continue
    return titles


def cmd_one(args: argparse.Namespace) -> int:
    _setup_log(args.verbose)
    client = _make_shopify_client()
    prod = _fetch_product(client, args.handle)
    if not prod:
        log.error("Product '%s' not found in Shopify", args.handle)
        return 2

    if args.dry_run:
        log.info("DRY RUN — calling Anthropic but not persisting")

    # v2.67.76: build a single-product collections index.
    pid = prod.get("id")
    titles = _fetch_collections_for_one_product(client, pid)
    coll_index = {str(pid): titles} if pid else {}

    anth_client = _build_anthropic_client()
    model = (args.model or os.environ.get(
        "ANTHROPIC_MODEL_VISION", DEFAULT_MODEL))
    result = _extract_one(prod, anth_client, model,
                            dry_run=args.dry_run,
                            shopify_client=client,
                            collections_index=coll_index)
    if result is None:
        return 1

    print(json.dumps(result, indent=2, default=str))
    return 0


def cmd_all(args: argparse.Namespace) -> int:
    _setup_log(args.verbose)
    client = _make_shopify_client()
    log.info("Fetching products from Shopify...")
    products = _fetch_all_products(client)
    log.info("Fetched %d products total", len(products))

    if not args.include_non_led:
        products = [p for p in products if _is_likely_led_profile(p)]
        log.info("Filtered to %d LED-profile-like products", len(products))

    if args.match:
        products = [p for p in products
                      if _matches_filter(p, args.match)]
        log.info("Filtered by --match='%s': %d remain",
                  args.match, len(products))

    already_done = set()
    if not args.force:
        already_done = db.product_dimensions_handles()
        log.info("Skip-cache: %d products already extracted",
                  len(already_done))

    work = [p for p in products
              if (p.get("handle") or "") not in already_done]
    if args.limit and args.limit > 0:
        work = work[:args.limit]
    log.info("Will process %d products", len(work))

    if args.dry_run:
        log.info("DRY RUN — calling Anthropic but not persisting")

    # v2.67.76: build collections index ONCE for all products.
    # This adds ~30s-2min depending on collection count but lets
    # every per-product call hit a local dict instead of paginating.
    coll_index: Dict[str, List[str]] = {}
    if not args.skip_collections:
        coll_index = _build_collections_index(client)

    anth_client = _build_anthropic_client()
    model = (args.model or os.environ.get(
        "ANTHROPIC_MODEL_VISION", DEFAULT_MODEL))

    n_diagrams = 0
    n_no_diagrams = 0
    n_errors = 0
    for i, prod in enumerate(work, start=1):
        try:
            res = _extract_one(prod, anth_client, model,
                                 dry_run=args.dry_run,
                                 shopify_client=client,
                                 collections_index=coll_index)
            if res is None:
                n_errors += 1
            elif res.get("has_diagram"):
                n_diagrams += 1
            else:
                n_no_diagrams += 1
        except Exception as exc:
            log.error("[%s] extraction failed: %s",
                        prod.get("handle"), exc)
            n_errors += 1

        if i % 25 == 0:
            log.info("Progress: %d / %d  (%d diagrams, %d none, %d errors)",
                      i, len(work), n_diagrams, n_no_diagrams, n_errors)
        if REQ_DELAY_SECONDS > 0:
            time.sleep(REQ_DELAY_SECONDS)

    log.info("=" * 60)
    log.info("DONE: %d processed | %d with diagram | %d none | %d errors",
              len(work), n_diagrams, n_no_diagrams, n_errors)
    return 0


def cmd_reclassify_from_titles(args: argparse.Namespace) -> int:
    """v2.67.78 repair: walk product_dimensions rows, apply the
    title-based mounting-type rule, fix any misclassifications.
    Free — no API calls. Run after upgrading the synonym table."""
    _setup_log(args.verbose)
    rows = db.all_product_dimensions()
    log.info("Loaded %d product_dimensions rows", len(rows))

    fixed = 0
    unchanged = 0
    no_signal = 0
    for r in rows:
        title = r.get("title") or ""
        current = r.get("mounting_type") or ""
        canonical = _title_to_mounting_type(title)
        if canonical is None:
            no_signal += 1
            continue
        if canonical == current:
            unchanged += 1
            continue
        if args.dry_run:
            log.info("[%s] WOULD fix: %s -> %s  (title='%s')",
                      r["shopify_handle"], current, canonical, title)
            fixed += 1
            continue
        # Build a row dict for upsert. Preserve all existing fields,
        # just swap mounting_type.
        new_row = dict(r)
        new_row["mounting_type"] = canonical
        # Drop autoincrement id so upsert keys on shopify_handle.
        new_row.pop("id", None)
        db.upsert_product_dimensions(new_row)
        log.info("[%s] fixed: %s -> %s  (title='%s')",
                  r["shopify_handle"], current, canonical, title)
        fixed += 1

    log.info("=" * 60)
    if args.dry_run:
        log.info("DRY RUN — would have fixed %d rows", fixed)
    else:
        log.info("Fixed %d rows | %d unchanged | %d no title signal",
                  fixed, unchanged, no_signal)
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Extract dimensions from Shopify product images "
                      "via Claude vision")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_one = sub.add_parser("one",
                              help="Extract a single product by handle")
    p_one.add_argument("--handle", required=True,
                          help="Shopify product handle, e.g. 'slim8'")
    p_one.add_argument("--dry-run", action="store_true",
                          help="Call API but don't persist to DB")
    p_one.add_argument("--model", default=None)
    p_one.add_argument("--verbose", action="store_true")
    p_one.set_defaults(func=cmd_one)

    p_all = sub.add_parser("all",
                              help="Extract every active product")
    p_all.add_argument("--force", action="store_true",
                          help="Re-extract products already cached")
    p_all.add_argument("--include-non-led", action="store_true",
                          help="Include non-LED-profile products too")
    p_all.add_argument(
        "--match", default=None,
        help="Only process products whose title/product_type/tags/"
              "handle contains this substring (case-insensitive). "
              "E.g. --match channel  for LED channels first.")
    p_all.add_argument("--limit", type=int, default=0,
                          help="Stop after N products (0 = no limit)")
    p_all.add_argument("--dry-run", action="store_true",
                          help="Call API but don't persist to DB")
    p_all.add_argument("--skip-collections", action="store_true",
                          help="Skip the (slow) collections index "
                                "build. Vision still gets metafields.")
    p_all.add_argument("--model", default=None)
    p_all.add_argument("--verbose", action="store_true")
    p_all.set_defaults(func=cmd_all)

    # v2.67.78 — title-based reclassification (no API spend)
    p_re = sub.add_parser(
        "reclassify-from-titles",
        help="Walk existing product_dimensions rows and fix "
              "mounting_type using the title-keyword rule. Free — "
              "no API calls. Run after synonym-table upgrades.")
    p_re.add_argument("--dry-run", action="store_true",
                         help="Print what WOULD change, don't write.")
    p_re.add_argument("--verbose", action="store_true")
    p_re.set_defaults(func=cmd_reclassify_from_titles)

    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
