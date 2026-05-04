"""
product_search.py
=================
Unified product-discovery layer (v2.67) that searches BOTH the CIN7
inventory frame AND the Shopify product knowledge base, unions the
hits on SKU/handle, and tags each result with a source field
(``cin7``, ``shopify``, or ``both``).

Why this exists
---------------
``search_products_by_text`` only searches the CIN7 ``engine_df``. That
DataFrame is the truth for stock, sales, and costs — but customer-
facing series (e.g. White Lily, pure-white White Iris) live on
Shopify and have no CIN7 row at all. Asking the AI Assistant
"what warm white LED strips do we have" used to silently omit them
because the only tool it called was ``search_products_by_text``.

Source-of-truth contract (kept consistent with docs/data-sources.md)::

  CIN7    → STOCK numbers, sales, costs.
  Shopify → customer-facing TITLES, FAMILIES, COLLECTIONS, DESCRIPTIONS,
            TAGS. Storefront URLs.

The Shopify side is read from the .md files written by
``shopify_sync.py`` under ``DATA_DIR/shopify/products/``. Each call
walks those files; results are cached in-process keyed by the
directory's mtime fingerprint, so repeated tool calls in one
Streamlit session are fast but a fresh nightly sync invalidates the
cache automatically.

Family detection
----------------
``detect_family`` is a regex-based placeholder until the
``product_attributes`` table (Tier-A1 on the roadmap) ships. Once
families come from a structured column rather than a regex over
titles, this list goes away.

Public API
----------
- ``find_products(engine_df, sale_lines_df, args) -> dict`` — the AI
  tool entry point. Mirrors the (engine, sales, args) calling
  convention used by ai_tools.py.
- ``shopify_freshness_status() -> dict`` — for the AI Assistant page
  banner. Tells the user (and the assistant) whether the Shopify
  index is missing, stale, or fresh.
- ``detect_family(text) -> str | None`` — exposed in case other
  modules want to group by family.
"""
from __future__ import annotations

import re
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import pandas as pd

from data_paths import DATA_DIR


SHOPIFY_DIR = DATA_DIR / "shopify"
SHOPIFY_PRODUCTS_DIR = SHOPIFY_DIR / "products"

# Staleness threshold for the freshness banner. We picked 48h because
# storefront content doesn't change minute-to-minute and the nightly
# sync runs every ~24h — anything older than two cycles means the
# scheduled sync probably failed.
STALE_THRESHOLD_HOURS = 48.0


# ---------------------------------------------------------------------------
# Family detector — v2.67 placeholder.
# Order matters: more-specific patterns FIRST so e.g. "Slim8" wins
# over "Slim".
# ---------------------------------------------------------------------------
_FAMILIES: list[tuple[str, re.Pattern]] = [
    ("ELITE_GOLD",      re.compile(r"\belite\s+gold\b", re.I)),
    ("WHITE_IRIS",      re.compile(
        r"\b(white\s+iris|iris\s+series)\b", re.I)),
    ("WHITE_LILY",      re.compile(
        r"\b(white\s+lil?ly|lil?ly\s+series)\b", re.I)),
    ("CARDINAL_FLOWER", re.compile(r"\bcardinal\s+flower\b", re.I)),
    ("LIATRIS",         re.compile(r"\bliatris\b", re.I)),
    ("BALTIC_IVY",      re.compile(r"\bbaltic\s+ivy\b", re.I)),
    ("HONEY_SUCKLE",    re.compile(r"\bhoney[\s\-]?suckle\b", re.I)),
    ("SIERRA",          re.compile(r"\bsierra\b", re.I)),
    ("SMOKIES",         re.compile(r"\bsmokies\b", re.I)),
    ("OSLO",            re.compile(r"\boslo\b", re.I)),
    ("SLIM8",           re.compile(r"\bslim\s*8\b", re.I)),
    ("PLW80",           re.compile(r"\bplw\s*80\b", re.I)),
    ("PLW70",           re.compile(r"\bplw\s*70\b", re.I)),
    ("DISA",            re.compile(r"\bdisa\b", re.I)),
    # SLIM and DECOR are last so the more-specific Slim8 / something-
    # decor matches above don't get pre-empted.
    ("SLIM",            re.compile(r"\bslim\b", re.I)),
    ("DECOR",           re.compile(r"\bdecor\b", re.I)),
]


def detect_family(text: Optional[str]) -> Optional[str]:
    """Return a family code for the given product title/type, or None.

    Placeholder until ``product_attributes`` ships — when families
    become a structured column we should pull from there instead of
    pattern-matching titles. Track the migration in
    ``cin7_queued_next_work`` (Tier-A1).
    """
    if not text:
        return None
    for fam, pat in _FAMILIES:
        if pat.search(text):
            return fam
    return None


# ---------------------------------------------------------------------------
# Shopify .md parsing (matches the format written by shopify_sync.py).
# ---------------------------------------------------------------------------
@dataclass
class ShopifyProduct:
    """Parsed view of one Shopify product .md file."""
    handle: str
    title: str
    product_type: str = ""
    vendor: str = ""
    tags: str = ""
    storefront_url: str = ""
    skus: list[str] = field(default_factory=list)
    body: str = ""
    raw_text: str = ""
    file_mtime: float = 0.0

    @property
    def family(self) -> Optional[str]:
        return (detect_family(self.title)
                or detect_family(self.product_type))


# Matches the metadata bullet format written by shopify_sync.py:
#   - **Handle:** white-iris-series-...
_META_LINE = re.compile(r"^-\s+\*\*([^:]+):\*\*\s*(.*)$")


def _parse_shopify_product_md(path: Path) -> Optional[ShopifyProduct]:
    """Parse one product .md. Returns None on read errors so a single
    bad file never breaks indexing."""
    try:
        raw = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return None
    title = ""
    handle = path.stem
    product_type = ""
    vendor = ""
    tags = ""
    storefront_url = ""
    skus: list[str] = []
    body_lines: list[str] = []
    in_meta = False
    in_body = False
    for line in raw.splitlines():
        s = line.strip()
        if s.startswith("# ") and not title:
            title = s[2:].strip()
            continue
        if s.startswith("## "):
            heading = s[3:].strip().lower()
            in_meta = (heading == "metadata")
            in_body = (heading == "customer-facing description")
            continue
        if in_meta:
            m = _META_LINE.match(s)
            if not m:
                continue
            label = m.group(1).strip().lower()
            val = m.group(2).strip()
            if label == "handle":
                handle = val or handle
            elif label == "storefront url":
                storefront_url = val
            elif label == "vendor":
                vendor = val
            elif label == "product type":
                product_type = val
            elif label == "tags":
                tags = val
            elif label == "skus":
                skus = [t.strip() for t in val.split(",") if t.strip()]
        elif in_body:
            body_lines.append(line)
    body = " ".join(ln.strip() for ln in body_lines if ln.strip())
    try:
        mt = path.stat().st_mtime
    except OSError:
        mt = 0.0
    return ShopifyProduct(
        handle=handle,
        title=title or handle,
        product_type=product_type,
        vendor=vendor,
        tags=tags,
        storefront_url=storefront_url,
        skus=skus,
        body=body,
        raw_text=raw,
        file_mtime=mt,
    )


# In-process cache. Key is a tuple of (path, mtime) pairs so an edit
# (or a re-sync that overwrites files) invalidates automatically.
_PRODUCT_CACHE: dict[tuple, list[ShopifyProduct]] = {}


def _index_shopify_products() -> list[ShopifyProduct]:
    if not SHOPIFY_PRODUCTS_DIR.exists():
        return []
    paths = list(SHOPIFY_PRODUCTS_DIR.glob("*.md"))
    if not paths:
        return []
    fp = tuple(sorted(
        (str(p), p.stat().st_mtime) for p in paths if p.exists()))
    if fp in _PRODUCT_CACHE:
        return _PRODUCT_CACHE[fp]
    out: list[ShopifyProduct] = []
    for p in paths:
        sp = _parse_shopify_product_md(p)
        if sp is not None:
            out.append(sp)
    _PRODUCT_CACHE.clear()
    _PRODUCT_CACHE[fp] = out
    return out


# ---------------------------------------------------------------------------
# Freshness check (drives the AI Assistant page banner).
# ---------------------------------------------------------------------------
def shopify_freshness_status() -> dict:
    """Return a dict describing the state of the local Shopify index.

    Keys:
        state: "missing" | "stale" | "fresh"
        n_products: int
        oldest_age_hours / newest_age_hours: float | None
        message: human-readable single-line summary
    """
    if not SHOPIFY_PRODUCTS_DIR.exists():
        return {
            "state": "missing",
            "n_products": 0,
            "oldest_age_hours": None,
            "newest_age_hours": None,
            "message": (
                "Shopify product discovery data is missing. The AI "
                "Assistant cannot find Shopify-only products (White "
                "Lily, pure-white Iris, etc.) until the sync runs. "
                "Run `python shopify_sync.py` on the host."
            ),
        }
    paths = list(SHOPIFY_PRODUCTS_DIR.glob("*.md"))
    if not paths:
        return {
            "state": "missing",
            "n_products": 0,
            "oldest_age_hours": None,
            "newest_age_hours": None,
            "message": (
                "Shopify products directory exists but is empty. "
                "Run `python shopify_sync.py` to populate."
            ),
        }
    now = time.time()
    mtimes = [p.stat().st_mtime for p in paths]
    oldest_age_hours = (now - min(mtimes)) / 3600.0
    newest_age_hours = (now - max(mtimes)) / 3600.0
    state = "fresh" if oldest_age_hours <= STALE_THRESHOLD_HOURS else "stale"
    return {
        "state": state,
        "n_products": len(paths),
        "oldest_age_hours": round(oldest_age_hours, 1),
        "newest_age_hours": round(newest_age_hours, 1),
        "message": (
            f"{len(paths)} Shopify products indexed; oldest file "
            f"{oldest_age_hours:.1f}h old, newest "
            f"{newest_age_hours:.1f}h old."
        ),
    }


# ---------------------------------------------------------------------------
# find_products — the AI tool implementation.
# ---------------------------------------------------------------------------
_TOKEN_RE = re.compile(r"[a-zA-Z0-9]+")


def _tok(s: Optional[str]) -> set[str]:
    if not s:
        return set()
    return {t.lower() for t in _TOKEN_RE.findall(s) if len(t) >= 2}


def _shopify_score(sp: ShopifyProduct,
                    query_tokens: set[str],
                    any_of_terms: list[str]) -> tuple[float, list[str]]:
    """Score a Shopify product against the query.

    Returns (score, fields_hit). Score == 0 means no match.
    Required: every query token must appear in at least one field.
    OR-leg: at least one any_of_terms phrase must appear (substring).
    """
    fields = {
        "title": _tok(sp.title),
        "product_type": _tok(sp.product_type),
        "tags": _tok(sp.tags),
        "body": _tok(sp.body),
        "handle": _tok(sp.handle.replace("-", " ")),
    }
    fields_hit: list[str] = []
    score = 0.0

    if query_tokens:
        for tok in query_tokens:
            hit_field: Optional[str] = None
            for fname, ftoks in fields.items():
                if tok in ftoks:
                    hit_field = fname
                    break
            if hit_field is None:
                return (0.0, [])
            if hit_field not in fields_hit:
                fields_hit.append(hit_field)
            score += 2.5 if hit_field == "title" else 1.0

    if any_of_terms:
        haystack = " ".join([
            sp.title, sp.product_type, sp.tags, sp.body,
            sp.handle.replace("-", " "),
        ]).lower()
        any_hit = False
        for term in any_of_terms:
            t = (term or "").lower().strip()
            if not t:
                continue
            if t in haystack:
                any_hit = True
                if t in sp.title.lower():
                    score += 3.0
                else:
                    score += 1.0
                break
        if not any_hit:
            return (0.0, [])

    return (score, fields_hit)


# Default exclude list when the caller asks about strips. These are
# accessories that share search terms with strips but aren't strip
# products themselves; without this filter the answer fills up with
# dimmers/profiles and pushes real strips off the bottom.
_DEFAULT_EXCLUDES_FOR_STRIPS = (
    "dimmer", "controller", "power supply", "channel",
    "profile", "diffuser", "connector", "extension cable",
    "amplifier", "repeater", "splitter", "psu",
)


def find_products(engine_df: pd.DataFrame,
                   sale_lines_df: pd.DataFrame,
                   args: dict) -> dict:
    """Unified product discovery (CIN7 ⋃ Shopify).

    Args (only ``query`` is required):
        query: str — AND-matched tokens, e.g. "led strip".
        any_of_terms: list[str] — OR-matched alternatives. Multi-word
            phrases supported (substring on full text), e.g.
            ["warm white", "2200K", "2400K", "2700K", "2800K", "3000K"].
        exclude_types: list[str] — drop rows whose name/title contains
            any of these. If omitted and ``query`` includes "strip",
            a default accessories block list is applied.
        families: list[str] — restrict results to these family codes
            (matched via ``detect_family`` on the title / product type).
        in_stock_only: bool — default True. Only filters CIN7-side
            rows; Shopify-only rows are ALWAYS surfaced (with
            stock_status="unknown") so the answer doesn't silently
            omit families like White Lily that aren't in CIN7.
        limit: int — cap on returned rows (default 40, hard max 80).

    Result shape (relevant keys for the assistant + debug panel):
        matched: int
        results: [
          {
            sku, name, shopify_handle, shopify_title, shopify_url,
            family, source ∈ {"cin7","shopify","both"},
            stock, stock_status ∈ {"in_stock","out_of_stock","unknown"},
            matched_in: list[str],
            score: float,
            note: str | None  # set on shopify-only rows
          },
          ...
        ]
        shopify: { products_indexed, products_matched, freshness {...} }
        cin7:    { rows_matched_pre_dedupe, in_stock_only_applied,
                   searched_fields, excluded_count }
        filters_applied: { query, query_tokens, any_of_terms,
                           exclude_types, families, in_stock_only }
        warnings: list[str]
        note: str
    """
    query = (args.get("query") or "").strip()
    any_of_terms: list[str] = list(args.get("any_of_terms") or [])
    exclude_types: list[str] = list(args.get("exclude_types") or [])
    if not exclude_types and "strip" in query.lower():
        exclude_types = list(_DEFAULT_EXCLUDES_FOR_STRIPS)
    families: list[str] = [f.upper() for f in (args.get("families") or [])]
    in_stock_only = bool(args.get("in_stock_only", True))
    try:
        limit = int(args.get("limit", 40) or 40)
    except (TypeError, ValueError):
        limit = 40
    limit = max(1, min(limit, 80))

    query_tokens = _tok(query)
    excludes_lower = [e.lower() for e in exclude_types]

    # ---- Shopify leg ----
    shopify_products = _index_shopify_products()
    shopify_hits: list[tuple[float, list[str], ShopifyProduct]] = []
    for sp in shopify_products:
        if families:
            fam = sp.family
            if not fam or fam not in families:
                continue
        haystack = f"{sp.title} {sp.product_type}".lower()
        if any(e in haystack for e in excludes_lower):
            continue
        score, fields_hit = _shopify_score(sp, query_tokens, any_of_terms)
        if score > 0:
            shopify_hits.append((score, fields_hit, sp))
    shopify_hits.sort(key=lambda t: t[0], reverse=True)

    # ---- CIN7 leg (delegate to existing search_products_by_text
    # for consistent semantics; lazy import to avoid circular deps) ----
    cin7_rows: list[dict] = []
    cin7_searched_fields: list[str] = []
    cin7_excluded_count = 0
    if engine_df is not None and not engine_df.empty:
        try:
            from ai_tools import search_products_by_text  # type: ignore
            cin7_args = {
                "query": query,
                "any_of_terms": any_of_terms,
                "exclude_types": exclude_types,
                "fields": ["title", "name", "description", "tags",
                            "product_type", "category"],
                "in_stock_only": in_stock_only,
                "limit": 200,  # we dedupe + cap further down
            }
            if families and len(families) == 1:
                cin7_args["family"] = families[0]
            cin7_result = search_products_by_text(
                engine_df, sale_lines_df, cin7_args)
            cin7_rows = cin7_result.get("results") or []
            cin7_searched_fields = [
                s.get("actual_column", s.get("requested", ""))
                for s in (cin7_result.get("searched") or [])
            ]
            cin7_excluded_count = int(
                cin7_result.get("excluded_count", 0) or 0)
        except Exception as exc:  # noqa: BLE001
            cin7_rows = []
            cin7_searched_fields = [f"<error: {exc}>"]

    # Set of all SKUs present in engine_df, plus a lookup for stock
    # data on Shopify-driven rows.
    cin7_sku_set: set[str] = set()
    cin7_by_sku: dict[str, dict] = {}
    if (engine_df is not None and not engine_df.empty
            and "SKU" in engine_df.columns):
        cols = ["SKU", "Name"] + [
            c for c in ("OnHand", "Available")
            if c in engine_df.columns]
        for r in engine_df[cols].to_dict(orient="records"):
            sku = str(r.get("SKU") or "").strip()
            if not sku:
                continue
            cin7_sku_set.add(sku)
            cin7_by_sku[sku] = r

    # SKUs that PASSED the per-row CIN7 filter (text/any_of_terms/
    # in_stock_only). Used to gate Shopify-driven rows so a query
    # for "warm white" doesn't leak 6000K variants when the Shopify
    # product page mentions both warm and cool kelvin temps.
    cin7_matched_skus: set[str] = set()
    cin7_rows_by_sku: dict[str, dict] = {}
    for r in cin7_rows:
        sku = str(r.get("SKU") or "").strip()
        if sku:
            cin7_matched_skus.add(sku)
            cin7_rows_by_sku[sku] = r

    # ---- Union ----
    seen_skus: set[str] = set()
    out: list[dict] = []

    # 1) Shopify-driven rows first so Shopify-only families always show.
    #
    # Per-variant filter rule: when a Shopify product has SKUs that
    # exist in CIN7, emit ONLY the SKUs that ALSO passed the
    # per-row CIN7 filter (cin7_matched_skus). Otherwise a Shopify
    # product page that mentions both 2700K and 6000K variants would
    # leak the cool-white SKUs into a warm-white answer.
    #
    # Shopify-only fallback (e.g. White Lily): if NONE of the
    # product's SKUs exist in CIN7 at all, treat the whole product
    # as a shopify-only row so the family isn't silently omitted.
    for score, fields_hit, sp in shopify_hits:
        sp_skus = sp.skus or []
        sp_skus_in_cin7 = [s for s in sp_skus if s in cin7_sku_set]
        sp_skus_passing = [s for s in sp_skus if s in cin7_matched_skus]
        if sp_skus_in_cin7:
            # Family represented in CIN7. Emit only variants that
            # passed the per-row text/any_of_terms/in_stock filter.
            for sku in sp_skus_passing:
                if sku in seen_skus:
                    continue
                cin7_row = cin7_rows_by_sku.get(sku, {})
                # search_products_by_text already applied
                # in_stock_only; we re-derive stock for the response
                # payload (consumer needs to render stock numbers).
                onhand_raw = (cin7_row.get("OnHand")
                               if "OnHand" in cin7_row
                               else cin7_by_sku.get(sku, {}).get("OnHand"))
                try:
                    onhand = (float(onhand_raw)
                               if onhand_raw not in (None, "")
                                  and not pd.isna(onhand_raw)
                               else None)
                except (TypeError, ValueError):
                    onhand = None
                stock_status = ("in_stock" if (onhand or 0) > 0
                                else ("out_of_stock"
                                      if onhand is not None
                                      else "unknown"))
                seen_skus.add(sku)
                out.append({
                    "sku": sku,
                    "name": (cin7_row.get("Name")
                              or cin7_by_sku.get(sku, {}).get("Name")
                              or sp.title),
                    "shopify_handle": sp.handle,
                    "shopify_title": sp.title,
                    "shopify_url": sp.storefront_url,
                    "family": sp.family,
                    "source": "both",
                    "stock": onhand,
                    "stock_status": stock_status,
                    "matched_in": fields_hit,
                    "score": round(score, 2),
                    "note": None,
                })
                if len(out) >= limit:
                    break
            # Don't emit a shopify-only fallback row when sp_skus_in_cin7
            # is non-empty: the family IS in CIN7, just maybe not all
            # variants matched the current filter. Suppressing the
            # fallback avoids a confusing duplicate.
        else:
            # No SKU intersection with CIN7 — treat as Shopify-only
            # family (White Lily case). Surface with stock_status
            # 'unknown' so the user sees the family exists.
            out.append({
                "sku": None,
                "name": sp.title,
                "shopify_handle": sp.handle,
                "shopify_title": sp.title,
                "shopify_url": sp.storefront_url,
                "family": sp.family,
                "source": "shopify",
                "stock": None,
                "stock_status": "unknown",
                "matched_in": fields_hit,
                "score": round(score, 2),
                "note": (
                    "Found in Shopify; stock data not available "
                    "(no CIN7 SKU match)."
                ),
            })
        if len(out) >= limit:
            break

    # 2) CIN7-only rows: SKUs the Shopify index didn't claim.
    if len(out) < limit:
        shopify_sku_set: set[str] = set()
        for _, _, sp in shopify_hits:
            for s in sp.skus or []:
                shopify_sku_set.add(s)
        for r in cin7_rows:
            sku = str(r.get("SKU") or "").strip()
            if not sku or sku in seen_skus or sku in shopify_sku_set:
                continue
            name = str(r.get("Name") or "")
            family = detect_family(name)
            if families and family not in families:
                continue
            onhand_raw = r.get("OnHand")
            try:
                onhand = (float(onhand_raw)
                           if onhand_raw not in (None, "")
                              and not pd.isna(onhand_raw)
                           else None)
            except (TypeError, ValueError):
                onhand = None
            stock_status = ("in_stock" if (onhand or 0) > 0
                            else ("out_of_stock"
                                  if onhand is not None
                                  else "unknown"))
            if in_stock_only and stock_status != "in_stock":
                continue
            seen_skus.add(sku)
            out.append({
                "sku": sku,
                "name": name,
                "shopify_handle": None,
                "shopify_title": None,
                "shopify_url": None,
                "family": family,
                "source": "cin7",
                "stock": onhand,
                "stock_status": stock_status,
                "matched_in": ["cin7-text-search"],
                "score": float(r.get("score") or 0.0),
                "note": None,
            })
            if len(out) >= limit:
                break

    # ---- Warnings driven by freshness ----
    freshness = shopify_freshness_status()
    warnings: list[str] = []
    if freshness["state"] == "missing":
        warnings.append(
            "Shopify product discovery data is missing — only CIN7 "
            "inventory is being searched. Run shopify_sync.py."
        )
    elif freshness["state"] == "stale":
        warnings.append(
            f"Shopify product index is stale "
            f"({freshness['oldest_age_hours']}h old) — results may "
            f"miss recent storefront changes."
        )

    return {
        "matched": len(out),
        "results": out,
        "shopify": {
            "products_indexed": len(shopify_products),
            "products_matched": len(shopify_hits),
            "freshness": freshness,
        },
        "cin7": {
            "rows_matched_pre_dedupe": len(cin7_rows),
            "in_stock_only_applied": in_stock_only,
            "searched_fields": cin7_searched_fields,
            "excluded_count": cin7_excluded_count,
        },
        "filters_applied": {
            "query": query,
            "query_tokens": sorted(query_tokens),
            "any_of_terms": any_of_terms,
            "exclude_types": exclude_types,
            "families": families,
            "in_stock_only": in_stock_only,
        },
        "warnings": warnings,
        "note": (
            "Unified product discovery (v2.67): unions Shopify product "
            "KB with CIN7 inventory. Each result has a `source` field "
            "({cin7, shopify, both}). Shopify-only rows have "
            "stock_status='unknown' and a `note` explaining no CIN7 "
            "stock data is available — surface them, don't omit."
        ),
    }
