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

import logging
import re
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import pandas as pd

from data_paths import DATA_DIR


# v2.67.1 — log invocations so the Render log lets us correlate
# OOM events with our code path. Logger name 'product_search'
# means every line prefixes with that, easy to grep.
log = logging.getLogger("product_search")
if not log.handlers:
    # Match the format shopify_sync uses so the lines look the same
    # in Render's log viewer. INFO level is enough for one-per-call
    # signals; if we ever need byte-level accounting we can bump it.
    _h = logging.StreamHandler()
    _h.setFormatter(logging.Formatter(
        "%(asctime)s  %(levelname)-7s  %(name)s  %(message)s",
        datefmt="%H:%M:%S"))
    log.addHandler(_h)
    log.setLevel(logging.INFO)
    # Don't propagate to root — Streamlit sometimes installs a root
    # handler that double-prints.
    log.propagate = False


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
# v2.67.3 — body content is capped at this size to keep the in-process
# product cache bounded. Originally 4KB (v2.67.1) sized for just the
# customer-facing description, but v2.67.3 folded the ## Variants
# section into body content (see _parse_shopify_product_md) and a
# popular family page (e.g. White Iris) has 30+ variant lines averaging
# 80 chars each → 2.4KB just for variants, plus 1-2KB description.
# 12KB gives comfortable headroom while keeping per-product memory
# bounded. Counted in characters, not bytes — close enough for
# memory accounting.
_BODY_CAP_CHARS = 12288

# v2.67.7 — per-family overall cap on pass-1 emissions, paired with a
# per-shopify-hit cap of 1 (each Shopify .md page gets at most one
# pass-1 row). Set to 3 so a multi-page family like White Iris (which
# has separate Shopify .md pages for RGBW IP20, RGBW IP68, pure-white
# IP20 24V, pure-white IP68 24V, pure-white 12V, UL...) gets up to
# three of those .md pages represented in pass 1 -- typically one
# RGBW + one pure-white IP20 + one pure-white IP68 -- giving the
# user real variety. v2.67.6 had this set to 1 which meant whichever
# Iris .md scored highest (RGBW) ate the only slot and pure-white
# variants were starved. Pass 2 still drains deferred SKUs for
# additional depth; this cap only governs pass-1 breadth.
_PER_FAMILY_PASS_1 = 3


@dataclass
class ShopifyProduct:
    """Parsed view of one Shopify product .md file.

    v2.67.1: dropped ``raw_text`` (full markdown content) — the scorer
    never read it, so it was wasted memory in the cache. ``body`` is
    capped to ``_BODY_CAP_CHARS`` for the same reason.
    """
    handle: str
    title: str
    product_type: str = ""
    vendor: str = ""
    tags: str = ""
    storefront_url: str = ""
    skus: list[str] = field(default_factory=list)
    body: str = ""
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
            # v2.67.3 — any non-metadata ## section flows into body.
            # Previously only "customer-facing description" was kept;
            # that lost the ## Variants section, where the kelvin
            # temperatures and "Ultra Wm" abbreviations actually live
            # (each variant line written by shopify_sync reads e.g.
            # "- LEDIRIS2700-120-0305 — Ultra Wm (2700K) 120 LEDs/m
            # — $1.50"). Without those lines in the haystack the Iris
            # and Lily pages never scored against warm-white queries.
            # Folding everything-but-metadata into body restores the
            # signal and is forward-compatible with new ## sections.
            in_body = not in_meta
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
    if len(body) > _BODY_CAP_CHARS:
        # v2.67.1/v2.67.3 — bound per-product memory. Truncating here
        # means a single oversized product page can't bloat the index
        # cache. v2.67.3 raised the cap to fit the ## Variants block.
        body = body[:_BODY_CAP_CHARS]
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
        # v2.67.2 — bumped default 40 → 60 for more headroom after
        # the per-family cap was added; a "warm white led strips"
        # query now spans 12-15 families and 60 keeps each family's
        # pass-1 share intact while leaving room for pass-2 depth.
        limit = int(args.get("limit", 60) or 60)
    except (TypeError, ValueError):
        limit = 60
    limit = max(1, min(limit, 80))

    query_tokens = _tok(query)
    excludes_lower = [e.lower() for e in exclude_types]

    # v2.67.1 — entry log. If we OOM during this call, the line
    # before the SIGKILL in Render logs will be from here, telling
    # us which query triggered it.
    log.info(
        "find_products start: query=%r tokens=%d any_of=%d "
        "excludes=%d families=%d in_stock_only=%s limit=%d",
        query, len(query_tokens), len(any_of_terms),
        len(exclude_types), len(families), in_stock_only, limit,
    )
    _t_start = time.time()

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
    #
    # v2.67.2 fixes two bugs found by Render-log inspection:
    #
    # Bug #1 — Lily-explicit returned 0 despite shopify_hits=2.
    # Old code used ``if sp_skus_in_cin7:`` as the fork. White Lily's
    # Shopify pages list discontinued connector SKUs (LED-BCI8BB-2,
    # LED-BCI8XB-2W) that DO exist in CIN7 — so sp_skus_in_cin7 was
    # non-empty — but those connectors get dropped by the strip-
    # accessory exclude_types filter, so sp_skus_passing was empty,
    # the for-loop iterated zero times, and the shopify-only fallback
    # never fired. Now we fork on ``if sp_skus_passing:`` instead, so
    # whenever no variants pass the per-row filter we still surface
    # the family as a shopify-only row — with a slightly different
    # note depending on whether CIN7 has *any* SKUs for the family.
    #
    # Bug #2 — warm-white returned 40 with no Iris despite
    # shopify_hits=123. The first 5-6 hits (high-scoring Elite Gold
    # pages) each emitted 6-8 warm-white variants, exhausting the
    # limit budget before lower-ranked but legitimate hits (Iris,
    # Lily, etc.) got a chance. v2.67.2 splits emission into two
    # passes: pass 1 caps each family at ``_PER_FAMILY_PASS_1`` (4)
    # variants for breadth; pass 2 drains the deferred SKUs until
    # the limit is reached.
    seen_skus: set[str] = set()
    out: list[dict] = []
    emitted_per_family: dict[str, int] = {}
    deferred: list[tuple[float, list[str], "ShopifyProduct", str]] = []

    def _emit_both_row(sku: str, score: float,
                        fields_hit: list[str],
                        sp: "ShopifyProduct") -> None:
        """Append a source='both' row for a SKU that passed both the
        Shopify scorer and the per-row CIN7 filter. No-op if dedup
        already saw this SKU."""
        if sku in seen_skus:
            return
        cin7_row = cin7_rows_by_sku.get(sku, {})
        onhand_raw = (cin7_row.get("OnHand")
                       if "OnHand" in cin7_row
                       else cin7_by_sku.get(sku, {}).get("OnHand"))
        try:
            onhand_v = (float(onhand_raw)
                         if onhand_raw not in (None, "")
                            and not pd.isna(onhand_raw)
                         else None)
        except (TypeError, ValueError):
            onhand_v = None
        stock_status_v = ("in_stock" if (onhand_v or 0) > 0
                          else ("out_of_stock"
                                if onhand_v is not None
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
            "stock": onhand_v,
            "stock_status": stock_status_v,
            "matched_in": fields_hit,
            "score": round(score, 2),
            "note": None,
        })

    # v2.67.7 — per-shopify-hit emission with family-aware fallback
    # decision and a modest per-family cap.
    #
    # Why this structure:
    #
    # 1. Multi-page-per-family case (Lily): two Shopify pages share
    #    family WHITE_LILY (the older "continuous-led-lighting-strip-
    #    white-lilly-series" and the newer "continuous-cob-ip67-…-
    #    white-lily-series" with real CIN7-matching SKUs). v2.67.6
    #    processed the older one first, fell into the shopify-only
    #    branch with the misleading "stock data not available" note,
    #    and capped the family. The newer page's matching SKUs got
    #    deferred and lost. Fix: precompute family-level "any hit has
    #    passing SKUs?" lookahead — when this hit has no passing SKUs
    #    BUT another hit in the same family does, skip the shopify-
    #    only fallback for this hit and let the other hit emit.
    #
    # 2. Variety-per-family case (Iris): family WHITE_IRIS has 5+
    #    Shopify pages — RGBW IP20, RGBW IP68, pure-white IP20 24V,
    #    pure-white IP68 24V, pure-white 12V, UL — and the user
    #    expects to see BOTH the RGBW variants AND the pure-white
    #    variants. Cap=1 per family meant whichever Iris hit scored
    #    highest (RGBW) ate the family's only slot and pure-white was
    #    starved. Fix: emit one SKU per shopify_hit (not per family),
    #    then enforce a per-family cap of _PER_FAMILY_PASS_1=3 to
    #    keep one prolific family from dominating the answer. Iris
    #    gets up to 3 distinct .md files represented (likely RGBW +
    #    one pure-white IP20 + one pure-white IP68); pass 2 fills
    #    depth from deferred.

    # Pre-compute family-level lookahead: does ANY hit in this
    # family have at least one SKU passing the per-row CIN7 filter?
    # Used to decide whether a particular hit's "no passing" branch
    # should fire its shopify-only fallback or yield to a sibling
    # hit with real coverage.
    family_has_any_passing: dict[str, bool] = {}
    family_has_any_in_cin7: dict[str, bool] = {}
    for score, fields_hit, sp in shopify_hits:
        family_key = sp.family or f"_handle_{sp.handle}"
        if family_key not in family_has_any_passing:
            family_has_any_passing[family_key] = False
            family_has_any_in_cin7[family_key] = False
        for s in sp.skus or []:
            if s in cin7_matched_skus:
                family_has_any_passing[family_key] = True
            if s in cin7_sku_set:
                family_has_any_in_cin7[family_key] = True

    # Pass 1: one emission per shopify_hit (not per family) with a
    # per-family overall cap.
    for score, fields_hit, sp in shopify_hits:
        if len(out) >= limit:
            break
        sp_skus = sp.skus or []
        sp_skus_passing = [s for s in sp_skus if s in cin7_matched_skus]
        family_key = sp.family or f"_handle_{sp.handle}"

        if sp_skus_passing:
            # Emit ONE passing SKU from this hit, then defer the rest
            # of this hit's passing SKUs. Per-family cap caps how many
            # hits from the same family contribute pass-1 emissions.
            emitted_for_this_hit = False
            for sku in sp_skus_passing:
                if sku in seen_skus:
                    continue
                if (emitted_per_family.get(family_key, 0)
                        >= _PER_FAMILY_PASS_1) or emitted_for_this_hit:
                    deferred.append((score, fields_hit, sp, sku))
                    continue
                _emit_both_row(sku, score, fields_hit, sp)
                emitted_per_family[family_key] = (
                    emitted_per_family.get(family_key, 0) + 1)
                emitted_for_this_hit = True
                if len(out) >= limit:
                    break
        else:
            # No passing SKUs on THIS hit. Skip the shopify-only
            # fallback if a sibling hit in the same family DOES have
            # passing SKUs (it'll emit them) — otherwise we'd produce
            # the misleading "stock data not available" row when real
            # stock exists on a different .md page.
            if family_has_any_passing.get(family_key, False):
                continue
            # Also skip if the family already has any kind of emission
            # (avoid duplicate display rows for one family).
            if emitted_per_family.get(family_key, 0) > 0:
                continue
            if family_has_any_in_cin7.get(family_key, False):
                note = (
                    "Found in Shopify; CIN7 has variants for this "
                    "family but none passed the active filter (may "
                    "be discontinued, off-topic, or excluded by "
                    "accessory rules)."
                )
            else:
                note = (
                    "Found in Shopify; stock data not available "
                    "(no CIN7 SKU match)."
                )
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
                "note": note,
            })
            emitted_per_family[family_key] = (
                emitted_per_family.get(family_key, 0) + 1)

    # Pass 2: drain deferred variants up to limit. Pass 1 guarantees
    # every hit (and every family with at least one passing-SKU hit)
    # got pass-1 representation; pass 2 just backfills depth.
    if len(out) < limit:
        for score, fields_hit, sp, sku in deferred:
            if len(out) >= limit:
                break
            _emit_both_row(sku, score, fields_hit, sp)

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

    # v2.67.1 — exit log so we can see the success path complete.
    # If a call OOMs, this line is missing from the log and the
    # entry line above tells us how far we got.
    log.info(
        "find_products done: returned=%d shopify_indexed=%d "
        "shopify_hits=%d cin7_rows=%d elapsed=%.2fs",
        len(out), len(shopify_products), len(shopify_hits),
        len(cin7_rows), time.time() - _t_start,
    )

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
