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
- Return ONLY the JSON object. No preamble."""


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
                   model: str) -> Dict[str, Any]:
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
    content_blocks.append({
        "type": "text",
        "text": (
            f"Product title: {product_title}\n"
            f"Number of images attached: {len(image_urls)}\n"
            f"Image positions: 1..{len(image_urls)} in order.\n\n"
            f"Extract dimensions per the schema."
        ),
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
                   model: str, dry_run: bool = False
                   ) -> Optional[dict]:
    """Run extraction for a single Shopify product. Returns the
    parsed result dict (also persists to DB unless dry_run=True)."""
    handle = prod.get("handle") or ""
    title = prod.get("title") or handle
    skus = _variants_skus(prod)
    family = _family_from_skus(skus)
    image_urls = _pick_image_urls(prod, MAX_IMAGES_PER_PRODUCT)
    if not image_urls:
        log.info("[%s] no images — skipping", handle)
        return None

    log.info("[%s] %d images, family=%s, %d variants",
              handle, len(image_urls), family or "?", len(skus))

    result = _call_vision(anthropic_client, image_urls, title, model)
    has_diagram = bool(result.get("has_diagram"))

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
def cmd_one(args: argparse.Namespace) -> int:
    _setup_log(args.verbose)
    client = _make_shopify_client()
    prod = _fetch_product(client, args.handle)
    if not prod:
        log.error("Product '%s' not found in Shopify", args.handle)
        return 2

    if args.dry_run:
        log.info("DRY RUN — calling Anthropic but not persisting")

    anth_client = _build_anthropic_client()
    model = (args.model or os.environ.get(
        "ANTHROPIC_MODEL_VISION", DEFAULT_MODEL))
    result = _extract_one(prod, anth_client, model,
                            dry_run=args.dry_run)
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

    anth_client = _build_anthropic_client()
    model = (args.model or os.environ.get(
        "ANTHROPIC_MODEL_VISION", DEFAULT_MODEL))

    n_diagrams = 0
    n_no_diagrams = 0
    n_errors = 0
    for i, prod in enumerate(work, start=1):
        try:
            res = _extract_one(prod, anth_client, model,
                                 dry_run=args.dry_run)
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
    p_all.add_argument("--limit", type=int, default=0,
                          help="Stop after N products (0 = no limit)")
    p_all.add_argument("--dry-run", action="store_true",
                          help="Call API but don't persist to DB")
    p_all.add_argument("--model", default=None)
    p_all.add_argument("--verbose", action="store_true")
    p_all.set_defaults(func=cmd_all)

    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
