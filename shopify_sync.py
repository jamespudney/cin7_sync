"""
shopify_sync.py
===============
Pull product, collection, page, and blog-article content from the
Shopify Admin API and write each item as a markdown file to
DATA_DIR/shopify/. The AI knowledge base (ai_kb.py) auto-indexes
that directory, so the AI Assistant can answer questions grounded
in the actual storefront copy — descriptions, FAQs, blog posts.

Why we don't crawl the public storefront
----------------------------------------
- The Admin API gives structured JSON (no HTML parsing).
- It includes data the public site doesn't surface (tags, draft
  pages, metafields).
- It doesn't generate bot traffic on your real customers' site.
- It's allowed by Shopify, no rate-limit surprises.

Auth
----
Two env vars:
  SHOPIFY_DOMAIN        e.g. 'wired4signs.myshopify.com'
  SHOPIFY_ACCESS_TOKEN  Admin API access token (shpat_...)

The token must have at least these scopes (least privilege):
  read_products, read_product_listings, read_inventory,
  read_content, read_themes, read_locales

Output layout
-------------
  /data/shopify/products/<handle>.md
  /data/shopify/collections/<handle>.md
  /data/shopify/pages/<handle>.md
  /data/shopify/blog-articles/<blog>--<handle>.md

Each .md file has frontmatter-style metadata at the top (handle,
SKUs, tags, etc.) followed by the body description. The KB indexer
scores hits on these and returns paragraphs.

Usage
-----
    .venv\\Scripts\\python shopify_sync.py             # full sync
    .venv\\Scripts\\python shopify_sync.py --dry-run   # log only

Recommended cadence: nightly via daily_sync.sh. Storefront content
doesn't change minute-to-minute; once a day is plenty.
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import sys
import time
from pathlib import Path
from typing import Optional

import requests
from dotenv import load_dotenv

from data_paths import DATA_DIR


SHOPIFY_API_VERSION = "2024-10"
# Polite throttle. Shopify allows 40 req/sec peak (2/sec sustained on
# Standard plans), but we don't need speed — sync runs nightly. 0.5s
# between requests = 2/sec which matches the sustained rate exactly,
# leaves plenty of headroom for other integrations.
RATE_LIMIT_SECONDS = 0.5

OUTPUT_DIR = DATA_DIR / "shopify"
PRODUCTS_DIR = OUTPUT_DIR / "products"
COLLECTIONS_DIR = OUTPUT_DIR / "collections"
PAGES_DIR = OUTPUT_DIR / "pages"
BLOG_ARTICLES_DIR = OUTPUT_DIR / "blog-articles"
POLICIES_DIR = OUTPUT_DIR / "policies"
MENUS_DIR = OUTPUT_DIR / "menus"


def _setup_log() -> logging.Logger:
    log = logging.getLogger("shopify_sync")
    log.setLevel(logging.INFO)
    if not log.handlers:
        sh = logging.StreamHandler()
        sh.setFormatter(logging.Formatter(
            "%(asctime)s  %(levelname)-7s  %(message)s",
            datefmt="%H:%M:%S"))
        log.addHandler(sh)
    return log


log = _setup_log()


# ---------------------------------------------------------------------------
# Cleaning utilities
# ---------------------------------------------------------------------------
_TAG_RE = re.compile(r"<[^>]+>")
_WS_RE = re.compile(r"\s+")
_FILENAME_BAD_RE = re.compile(r"[^a-zA-Z0-9_\-]+")


def html_to_text(html: Optional[str]) -> str:
    """Strip HTML tags and collapse whitespace. We deliberately use a
    simple regex instead of BeautifulSoup to avoid adding a dependency
    — Shopify HTML is well-formed enough that this works for >95% of
    our content. Tradeoff: very ugly tables or nested lists may lose
    structure, but body text is what we care about."""
    if not html:
        return ""
    text = _TAG_RE.sub(" ", html)
    text = (text.replace("&nbsp;", " ")
                .replace("&amp;", "&")
                .replace("&lt;", "<")
                .replace("&gt;", ">")
                .replace("&quot;", '"')
                .replace("&#39;", "'"))
    text = _WS_RE.sub(" ", text).strip()
    return text


def safe_filename(handle: str) -> str:
    """Turn a Shopify handle into a safe filename component."""
    return _FILENAME_BAD_RE.sub("-", (handle or "").strip("-")) or "untitled"


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------
class ShopifyClient:
    """Minimal Shopify Admin API client. Handles auth, pagination,
    rate-limit retries, and Link-header cursor parsing."""

    def __init__(self, domain: str, token: str):
        if not domain or not token:
            raise RuntimeError(
                "SHOPIFY_DOMAIN and SHOPIFY_ACCESS_TOKEN must be set "
                "in environment variables.")
        self.domain = domain.replace("https://", "").rstrip("/")
        self.token = token
        self.base = (f"https://{self.domain}/admin/api/"
                     f"{SHOPIFY_API_VERSION}")
        self.session = requests.Session()
        self.session.headers.update({
            "X-Shopify-Access-Token": token,
            "Accept": "application/json",
        })
        self._last_call = 0.0

    def _throttle(self) -> None:
        elapsed = time.time() - self._last_call
        if elapsed < RATE_LIMIT_SECONDS:
            time.sleep(RATE_LIMIT_SECONDS - elapsed)
        self._last_call = time.time()

    def _get(self, url: str,
              params: Optional[dict] = None) -> requests.Response:
        for attempt in range(5):
            self._throttle()
            r = self.session.get(url, params=params, timeout=30)
            if r.status_code == 429:
                # Shopify says "Retry-After" or rate-limit-budget header
                wait = int(r.headers.get("Retry-After", "2") or 2)
                log.warning("  429 throttled, sleeping %ds", wait)
                time.sleep(wait)
                continue
            return r
        return r

    def get_shop_info(self) -> dict:
        """Fetch /shop.json to learn the primary customer-facing
        domain (not the myshopify.com one). Used to build storefront
        URLs in each markdown file so the AI can cite them."""
        r = self._get(f"{self.base}/shop.json")
        if r.status_code != 200:
            return {}
        return (r.json() or {}).get("shop") or {}

    def paginate(self, endpoint: str,
                  resource_key: str,
                  params: Optional[dict] = None) -> list:
        """Walk Shopify's cursor-based pagination via Link header.
        Returns the merged list across all pages.

        endpoint     e.g. 'products.json'
        resource_key e.g. 'products' (what to extract from each page)"""
        url = f"{self.base}/{endpoint}"
        # Initial query — Shopify max 250 per page on most resources
        merged_params = dict(params or {})
        merged_params.setdefault("limit", 250)
        out = []
        page_idx = 0
        next_url: Optional[str] = url
        while next_url:
            page_idx += 1
            r = self._get(next_url,
                           params=merged_params if page_idx == 1 else None)
            if r.status_code != 200:
                log.error("  %s page %d -> %d %s",
                           endpoint, page_idx, r.status_code,
                           r.text[:200])
                break
            data = r.json()
            batch = data.get(resource_key) or []
            out.extend(batch)
            log.info("  %s page %d -> %d (running %d)",
                      endpoint, page_idx, len(batch), len(out))
            # Cursor: parse Link: <...>; rel="next"
            link = r.headers.get("Link") or r.headers.get("link") or ""
            next_url = None
            for part in link.split(","):
                segs = part.strip().split(";")
                if len(segs) < 2:
                    continue
                if 'rel="next"' in segs[1]:
                    cand = segs[0].strip()
                    if cand.startswith("<") and cand.endswith(">"):
                        next_url = cand[1:-1]
                        break
        return out


# ---------------------------------------------------------------------------
# Markdown writers
# ---------------------------------------------------------------------------
def _ensure_dirs() -> None:
    for d in (PRODUCTS_DIR, COLLECTIONS_DIR, PAGES_DIR,
              BLOG_ARTICLES_DIR, POLICIES_DIR, MENUS_DIR):
        d.mkdir(parents=True, exist_ok=True)


def write_product_md(prod: dict, storefront_url: str = "") -> Path:
    handle = prod.get("handle") or str(prod.get("id"))
    fname = PRODUCTS_DIR / f"{safe_filename(handle)}.md"
    title = prod.get("title") or handle
    body = html_to_text(prod.get("body_html", ""))
    vendor = prod.get("vendor", "")
    ptype = prod.get("product_type", "")
    tags = prod.get("tags", "")
    variants = prod.get("variants", []) or []
    skus = sorted({v.get("sku") for v in variants
                    if v.get("sku")})
    public_url = (f"{storefront_url}/products/{handle}"
                   if storefront_url and handle else "")

    lines = [
        f"# {title}",
        "",
        "## Metadata",
        "",
        f"- **Handle:** {handle}",
        f"- **Storefront URL:** {public_url}" if public_url else None,
        f"- **Vendor:** {vendor}" if vendor else None,
        f"- **Product type:** {ptype}" if ptype else None,
        f"- **Tags:** {tags}" if tags else None,
        f"- **SKUs:** {', '.join(skus)}" if skus else None,
        "",
        "## Customer-facing description",
        "",
        body or "*(No description on the product page.)*",
        "",
    ]
    if variants:
        lines.append("## Variants")
        lines.append("")
        # Note: we deliberately do NOT include inventory_quantity or
        # other stock fields here. CIN7 is the source of truth for
        # stock — Shopify mirrors it with a few-minute lag, so we
        # don't want the AI quoting potentially-stale numbers from
        # Shopify content. See docs/data-sources.md.
        for v in variants:
            sku = v.get("sku", "")
            vtitle = v.get("title", "")
            price = v.get("price", "")
            lines.append(f"- {sku or '(no SKU)'} — {vtitle} — "
                          f"${price}")
        lines.append("")
    fname.write_text(
        "\n".join(line for line in lines if line is not None),
        encoding="utf-8")
    return fname


def write_collection_md(coll: dict, products_in_coll: list,
                          storefront_url: str = "") -> Path:
    handle = coll.get("handle") or str(coll.get("id"))
    fname = COLLECTIONS_DIR / f"{safe_filename(handle)}.md"
    title = coll.get("title") or handle
    body = html_to_text(coll.get("body_html", ""))
    sort_order = coll.get("sort_order", "")
    public_url = (f"{storefront_url}/collections/{handle}"
                   if storefront_url and handle else "")
    lines = [
        f"# Collection: {title}",
        "",
        "## Metadata",
        "",
        f"- **Handle:** {handle}",
        f"- **Storefront URL:** {public_url}" if public_url else None,
        f"- **Sort order:** {sort_order}" if sort_order else None,
        f"- **Type:** {'smart' if 'rules' in coll else 'manual'}",
        "",
        "## Description",
        "",
        body or "*(No description on the collection page.)*",
        "",
    ]
    if products_in_coll:
        lines.append(f"## Products in this collection "
                      f"({len(products_in_coll)})")
        lines.append("")
        for p in products_in_coll[:200]:  # cap massive collections
            t = p.get("title", "")
            h = p.get("handle", "")
            lines.append(f"- [{t}](products/{safe_filename(h)}.md)")
        if len(products_in_coll) > 200:
            lines.append(f"- … and {len(products_in_coll) - 200} more")
        lines.append("")
    fname.write_text(
        "\n".join(line for line in lines if line is not None),
        encoding="utf-8")
    return fname


def write_page_md(page: dict, storefront_url: str = "") -> Path:
    handle = page.get("handle") or str(page.get("id"))
    fname = PAGES_DIR / f"{safe_filename(handle)}.md"
    title = page.get("title") or handle
    body = html_to_text(page.get("body_html", ""))
    public_url = (f"{storefront_url}/pages/{handle}"
                   if storefront_url and handle else "")
    fname.write_text(
        f"# Page: {title}\n\n## Metadata\n\n- **Handle:** {handle}\n"
        + (f"- **Storefront URL:** {public_url}\n" if public_url else "")
        + f"- **Published:** {page.get('published_at', '')}\n\n"
        f"## Body\n\n{body or '*(empty page body)*'}\n",
        encoding="utf-8")
    return fname


def write_policy_md(policy: dict, storefront_url: str = "") -> Path:
    """Policies: refund, privacy, terms-of-service, shipping,
    subscription. These are CUSTOMER-FACING — what shoppers see when
    they click 'Returns Policy' in the footer. Different endpoint
    from /pages so we used to miss them entirely. Shopify provides
    the policy.url field directly so we use that as the public URL."""
    handle = policy.get("handle") or "policy"
    fname = POLICIES_DIR / f"{safe_filename(handle)}.md"
    title = policy.get("title") or handle.replace("-", " ").title()
    body = html_to_text(policy.get("body", ""))
    # Shopify gives us policy.url directly — prefer it. Fall back to
    # building from the storefront URL.
    public_url = (policy.get("url")
                   or (f"{storefront_url}/policies/{handle}"
                       if storefront_url else ""))
    fname.write_text(
        f"# Policy: {title}\n\n## Metadata\n\n"
        f"- **Type:** {handle} (customer-facing storefront policy)\n"
        + (f"- **Storefront URL:** {public_url}\n" if public_url else "")
        + f"- **Last updated:** {policy.get('updated_at', '')}\n\n"
        f"## Body\n\n{body or '*(empty policy body)*'}\n",
        encoding="utf-8")
    return fname


def write_menu_md(menu: dict) -> Path:
    """Storefront navigation menu — what links/categories customers
    see in the header, footer, mobile nav, etc. The AI can use these
    to answer 'what categories do we have on the website?' or
    'where would a customer find driveway lights?'."""
    handle = menu.get("handle") or str(menu.get("id"))
    fname = MENUS_DIR / f"{safe_filename(handle)}.md"
    title = menu.get("title") or handle
    items = menu.get("items") or []
    lines = [
        f"# Menu: {title}",
        "",
        "## Metadata",
        "",
        f"- **Handle:** {handle}",
        f"- **Item count:** {len(items)}",
        "",
        "## Navigation items",
        "",
    ]

    def _render_items(items_list, depth: int = 0) -> None:
        indent = "  " * depth
        for it in items_list or []:
            label = it.get("title") or it.get("name") or "(unnamed)"
            url = it.get("url") or it.get("subject") or ""
            lines.append(f"{indent}- {label}{' — `' + url + '`' if url else ''}")
            children = it.get("items") or []
            if children:
                _render_items(children, depth + 1)
    _render_items(items)
    fname.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return fname


def write_article_md(article: dict, blog_handle: str,
                       storefront_url: str = "") -> Path:
    handle = article.get("handle") or str(article.get("id"))
    fname = BLOG_ARTICLES_DIR / (
        f"{safe_filename(blog_handle)}--{safe_filename(handle)}.md")
    title = article.get("title") or handle
    body = html_to_text(article.get("body_html", ""))
    summary = html_to_text(article.get("summary_html", ""))
    public_url = (f"{storefront_url}/blogs/{blog_handle}/{handle}"
                   if storefront_url and handle and blog_handle else "")
    fname.write_text(
        f"# Blog: {title}\n\n## Metadata\n\n"
        f"- **Blog:** {blog_handle}\n"
        f"- **Handle:** {handle}\n"
        + (f"- **Storefront URL:** {public_url}\n" if public_url else "")
        + f"- **Author:** {article.get('author', '')}\n"
        f"- **Tags:** {article.get('tags', '')}\n"
        f"- **Published:** {article.get('published_at', '')}\n\n"
        f"## Summary\n\n{summary or '*(no summary)*'}\n\n"
        f"## Body\n\n{body or '*(empty body)*'}\n",
        encoding="utf-8")
    return fname


# ---------------------------------------------------------------------------
# Main sync entry
# ---------------------------------------------------------------------------
def main() -> int:
    parser = argparse.ArgumentParser(
        description="Sync Shopify content to local AI knowledge base")
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Fetch and log counts but don't write any files.")
    parser.add_argument(
        "--skip-products", action="store_true")
    parser.add_argument(
        "--skip-collections", action="store_true")
    parser.add_argument(
        "--skip-pages", action="store_true")
    parser.add_argument(
        "--skip-blogs", action="store_true")
    parser.add_argument(
        "--skip-policies", action="store_true")
    parser.add_argument(
        "--skip-menus", action="store_true")
    args = parser.parse_args()

    load_dotenv()
    domain = os.environ.get("SHOPIFY_DOMAIN", "").strip()
    token = os.environ.get("SHOPIFY_ACCESS_TOKEN", "").strip()
    if not domain or not token:
        log.error("ERROR: SHOPIFY_DOMAIN / SHOPIFY_ACCESS_TOKEN env "
                   "vars are required.")
        return 1

    client = ShopifyClient(domain, token)
    _ensure_dirs()

    log.info("Connected to %s", domain)

    # Fetch the customer-facing primary domain so we can build
    # storefront URLs (different from the .myshopify.com one).
    shop_info = client.get_shop_info()
    primary_domain = (shop_info.get("primary_locale", "")
                       and shop_info.get("primary_domain", {}).get("url"))
    # Older API responses put it under different shapes; be defensive.
    if not primary_domain:
        primary_domain = shop_info.get("domain", "")
    if primary_domain and not primary_domain.startswith("http"):
        primary_domain = f"https://{primary_domain}"
    storefront_url = (primary_domain or "").rstrip("/")
    log.info("Storefront URL: %s",
              storefront_url or "(unknown — URLs in markdown will be blank)")

    n_products = n_collections = n_pages = n_articles = 0
    n_policies = n_menus = 0

    # ---- Products
    if not args.skip_products:
        log.info("Fetching products...")
        products = client.paginate("products.json", "products")
        log.info("Total products: %d", len(products))
        if not args.dry_run:
            for p in products:
                write_product_md(p, storefront_url)
                n_products += 1

    # ---- Collections (manual + smart)
    products_by_id = {}  # only populated if we need it for collections
    if not args.skip_collections:
        log.info("Fetching custom (manual) collections...")
        custom_colls = client.paginate(
            "custom_collections.json", "custom_collections")
        log.info("  custom: %d", len(custom_colls))
        log.info("Fetching smart collections...")
        smart_colls = client.paginate(
            "smart_collections.json", "smart_collections")
        log.info("  smart: %d", len(smart_colls))
        all_colls = custom_colls + smart_colls

        # For each collection, fetch products in it (so we can list
        # them in the markdown output for context).
        if all_colls and not args.dry_run:
            log.info("Fetching products per collection (linked listings)...")
            for coll in all_colls:
                cid = coll.get("id")
                products_in = client.paginate(
                    "products.json", "products",
                    params={"collection_id": cid, "fields":
                             "id,title,handle"})
                write_collection_md(coll, products_in, storefront_url)
                n_collections += 1

    # ---- Pages
    if not args.skip_pages:
        log.info("Fetching pages...")
        pages = client.paginate("pages.json", "pages")
        log.info("Total pages: %d", len(pages))
        if not args.dry_run:
            for p in pages:
                write_page_md(p, storefront_url)
                n_pages += 1

    # ---- Blog articles
    if not args.skip_blogs:
        log.info("Fetching blogs...")
        blogs = client.paginate("blogs.json", "blogs")
        for blog in blogs:
            blog_id = blog.get("id")
            blog_handle = blog.get("handle") or str(blog_id)
            log.info("  blog '%s'...", blog_handle)
            articles = client.paginate(
                f"blogs/{blog_id}/articles.json", "articles")
            log.info("    articles: %d", len(articles))
            if not args.dry_run:
                for a in articles:
                    write_article_md(a, blog_handle, storefront_url)
                    n_articles += 1

    # ---- Policies (returns, refund, shipping, privacy, terms)
    if not args.skip_policies:
        log.info("Fetching policies (returns/refund/shipping/etc)...")
        # Policies endpoint returns the full list directly, not paginated
        r = client._get(f"{client.base}/policies.json")
        if r.status_code == 200:
            policies = (r.json() or {}).get("policies") or []
            log.info("  policies: %d", len(policies))
            if not args.dry_run:
                for p in policies:
                    write_policy_md(p, storefront_url)
                    n_policies += 1
        else:
            log.warning("  /policies.json -> %d %s",
                         r.status_code, r.text[:200])

    # ---- Menus / navigation (storefront nav, footer links etc)
    if not args.skip_menus:
        log.info("Fetching storefront menus / navigation...")
        # Menus moved to GraphQL in newer API versions, but the REST
        # /admin/api/.../menus.json endpoint still works on most stores.
        r = client._get(f"{client.base}/menus.json")
        if r.status_code == 200:
            menus = (r.json() or {}).get("menus") or []
            log.info("  menus: %d", len(menus))
            if not args.dry_run:
                for m in menus:
                    write_menu_md(m)
                    n_menus += 1
        else:
            log.warning(
                "  /menus.json -> %d (some stores need GraphQL for "
                "menus — we'll add that path in Phase 1)",
                r.status_code)

    log.info("=" * 60)
    log.info("Wrote %d products, %d collections, %d pages, %d articles, "
              "%d policies, %d menus",
              n_products, n_collections, n_pages, n_articles,
              n_policies, n_menus)
    log.info("Output: %s", OUTPUT_DIR)
    return 0


if __name__ == "__main__":
    sys.exit(main())
