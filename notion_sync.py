"""notion_sync.py (v2.67.249)
================================

Push team-facing operational data into the team's Notion
workspace so multiple AI agents (and humans) share a single
source of truth — Phase 1: the slow-movers register.

Design
------
- One Notion DATABASE per data type (e.g. "Slow Movers"), nested
  under the configured parent page. Each row is a SKU, upserted
  by SKU on every sync. Schema is created on first run and never
  re-pushed.
- Fire-and-forget per row: API errors are logged and the loop
  carries on.
- `--dry-run` previews everything without touching Notion — safe
  to run locally before credentials are wired up.

Env vars
--------
NOTION_INTEGRATION_SECRET   the secret_xxx token from Notion
NOTION_TEAM_PARENT_PAGE_ID  page ID under which databases live

How to set up the Notion integration (one-time)
-----------------------------------------------
1. Notion -> Settings -> Connections -> Develop or manage
   integrations -> + New integration.
2. Type: Internal. Name: "Wired4Signs Sync". Workspace: your
   team workspace. Save.
3. Copy the *Internal Integration Secret* (secret_xxxx).
4. Open the Notion page that will be the parent
   (e.g. "Operations" or "Wired4Signs Knowledge Base").
5. Click `...` top-right -> Connections -> Add connections ->
   select "Wired4Signs Sync".
6. Copy the parent page ID from the URL: it's the 32-character
   hex string after the page title and a hyphen (NO hyphens
   when you copy it — Notion's URLs strip them).
"""

from __future__ import annotations

import argparse
import csv
import datetime
import glob
import json
import logging
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))
import db  # noqa: E402

try:
    from data_paths import OUTPUT_DIR
except ImportError:
    OUTPUT_DIR = SCRIPT_DIR / "output"

log = logging.getLogger("notion_sync")

NOTION_API = "https://api.notion.com/v1"
NOTION_VERSION = "2022-06-28"


# ---------------------------------------------------------------------------
# Auth + HTTP
# ---------------------------------------------------------------------------
def _config() -> Dict[str, str]:
    cfg = {
        "secret": os.environ.get(
            "NOTION_INTEGRATION_SECRET", "").strip(),
        "parent": os.environ.get(
            "NOTION_TEAM_PARENT_PAGE_ID", "").strip().replace(
                "-", ""),
    }
    missing = [k for k, v in cfg.items() if not v]
    if missing:
        raise RuntimeError(
            f"Notion env vars not set: {missing}. See the "
            f"notion_sync.py docstring for setup steps.")
    return cfg


def _request(method: str, path: str,
             json_body: Optional[Any] = None,
             cfg: Optional[Dict[str, str]] = None) -> Dict:
    cfg = cfg or _config()
    url = f"{NOTION_API}{path}"
    headers = {
        "Authorization": f"Bearer {cfg['secret']}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json",
    }
    for attempt in range(6):
        try:
            r = requests.request(method, url, headers=headers,
                                  json=json_body, timeout=30)
        except requests.RequestException as exc:
            raise RuntimeError(
                f"Notion API network error ({method} {path}): "
                f"{exc}") from exc
        # Notion limits to ~3 req/s — honour Retry-After on 429.
        if r.status_code == 429 and attempt < 5:
            try:
                wait = float(r.headers.get("Retry-After", "1"))
            except (TypeError, ValueError):
                wait = 1.0
            time.sleep(min(wait, 10.0) + 0.25)
            continue
        if r.status_code >= 400:
            raise RuntimeError(
                f"Notion API error ({method} {path}): "
                f"HTTP {r.status_code} — {r.text[:400]}")
        try:
            return r.json()
        except ValueError as exc:
            raise RuntimeError(
                f"Notion API returned non-JSON ({method} {path}): "
                f"{r.text[:300]}") from exc
    raise RuntimeError(
        f"Notion API rate-limited ({method} {path}) after retries")


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------
def _find_database_by_title(parent_page_id: str,
                             title: str,
                             cfg: Dict[str, str]) -> Optional[str]:
    """Walk the children of the parent page and return the ID of
    the child database whose title matches `title`, else None."""
    cursor = None
    while True:
        path = (f"/blocks/{parent_page_id}/children"
                f"?page_size=100"
                + (f"&start_cursor={cursor}" if cursor else ""))
        body = _request("GET", path, cfg=cfg)
        for block in body.get("results", []) or []:
            if block.get("type") != "child_database":
                continue
            child_title = (block.get("child_database") or {}
                           ).get("title") or ""
            if child_title.strip().lower() == title.strip().lower():
                return block.get("id")
        if not body.get("has_more"):
            return None
        cursor = body.get("next_cursor")


def _create_database(parent_page_id: str, title: str,
                     schema: Dict[str, Dict],
                     cfg: Dict[str, str]) -> str:
    body = {
        "parent": {"type": "page_id",
                   "page_id": parent_page_id},
        "title": [{"type": "text",
                   "text": {"content": title}}],
        "properties": schema,
    }
    res = _request("POST", "/databases", json_body=body, cfg=cfg)
    return res["id"]


def find_or_create_database(title: str, schema: Dict[str, Dict],
                             cfg: Optional[Dict[str, str]] = None,
                             registry_name: Optional[str] = None
                             ) -> str:
    """v2.67.257 — canonical-ID lookup first, then title fallback.

    Resolution order:
      1. Stored ID in notion_db_ids (registry_name; defaults to
         title.lower()). Verify it still exists via GET; if so,
         reuse — even if the DB was moved or renamed in Notion.
      2. Title search under the configured parent (legacy).
      3. Create a new database.
    Whichever path wins, store the resulting ID so subsequent
    runs hit step 1 — no more duplicate databases."""
    cfg = cfg or _config()
    key = (registry_name or title).strip().lower()
    # 1. Stored ID.
    try:
        stored = db.get_notion_db_id(key)
    except Exception:  # noqa: BLE001
        stored = None
    if stored:
        try:
            _request("GET", f"/databases/{stored}", cfg=cfg)
            return stored
        except Exception as exc:  # noqa: BLE001
            log.warning(
                "Stored Notion DB id %s for %r no longer "
                "resolves (%s) — falling back.", stored, key, exc)
            try:
                db.clear_notion_db_id(key)
            except Exception:  # noqa: BLE001
                pass
    # 2. Title search (handles a DB created before this fix).
    existing = _find_database_by_title(cfg["parent"], title, cfg)
    if existing:
        try:
            db.set_notion_db_id(key, existing)
        except Exception:  # noqa: BLE001
            pass
        return existing
    # 3. Create.
    log.info("Creating Notion database %r ...", title)
    new_id = _create_database(cfg["parent"], title, schema, cfg)
    try:
        db.set_notion_db_id(key, new_id)
    except Exception:  # noqa: BLE001
        pass
    return new_id


def query_database_by_title(database_id: str, title_prop: str,
                             title_value: str,
                             cfg: Optional[Dict[str, str]] = None
                             ) -> Optional[str]:
    """Return the page id of the row whose title-property matches,
    or None."""
    cfg = cfg or _config()
    body = {
        "filter": {
            "property": title_prop,
            "title": {"equals": title_value}},
        "page_size": 1,
    }
    res = _request("POST", f"/databases/{database_id}/query",
                    json_body=body, cfg=cfg)
    rows = res.get("results") or []
    return rows[0]["id"] if rows else None


def query_database_by_rich_text(database_id: str, prop: str,
                                 value: str,
                                 cfg: Optional[Dict[str, str]] = None
                                 ) -> Optional[str]:
    """Return the page id of the row whose rich_text property `prop`
    equals `value`, or None. Used to upsert keyed on a non-title
    natural key (e.g. a Shopify handle)."""
    cfg = cfg or _config()
    body = {
        "filter": {"property": prop,
                   "rich_text": {"equals": value}},
        "page_size": 1,
    }
    res = _request("POST", f"/databases/{database_id}/query",
                    json_body=body, cfg=cfg)
    rows = res.get("results") or []
    return rows[0]["id"] if rows else None


def upsert_row(database_id: str, title_prop: str,
               title_value: str, properties: Dict,
               cfg: Optional[Dict[str, str]] = None) -> str:
    """Insert or update a row in `database_id` keyed on its
    title property. Returns the row's page id."""
    cfg = cfg or _config()
    existing = query_database_by_title(
        database_id, title_prop, title_value, cfg)
    full_props = dict(properties)
    # Title property is the natural key.
    full_props[title_prop] = {
        "title": [{"type": "text",
                   "text": {"content": title_value}}]}
    if existing:
        _request("PATCH", f"/pages/{existing}",
                  json_body={"properties": full_props}, cfg=cfg)
        return existing
    res = _request("POST", "/pages", json_body={
        "parent": {"database_id": database_id},
        "properties": full_props,
    }, cfg=cfg)
    return res["id"]


# ---------------------------------------------------------------------------
# Helpers — Notion property builders
# ---------------------------------------------------------------------------
def _p_text(value: Optional[str]) -> Dict:
    s = (value or "")[:1900]
    return {"rich_text": (
        [{"type": "text", "text": {"content": s}}] if s else [])}


def _p_num(value) -> Dict:
    try:
        return {"number": (None if value is None
                            else float(value))}
    except (TypeError, ValueError):
        return {"number": None}


def _p_date(value: Optional[str]) -> Dict:
    if not value:
        return {"date": None}
    s = str(value)[:10]
    return {"date": {"start": s}}


def _p_select(value: Optional[str]) -> Dict:
    s = (str(value).strip() if value not in (None, "") else "")
    # Notion select option names can't contain commas; cap at 100.
    s = s.replace(",", " ")[:100]
    return {"select": ({"name": s} if s else None)}


def _p_checkbox(value) -> Dict:
    """0/1/'1'/True/None → Notion checkbox bool."""
    return {"checkbox": bool(value) and str(value) not in ("0", "")}


def _p_url(value: Optional[str]) -> Dict:
    s = (value or "").strip()
    return {"url": s or None}


# ---------------------------------------------------------------------------
# Notion page reading (Phase 2 — playbook pull)
# ---------------------------------------------------------------------------
def _rich_text_to_plain(rich: Optional[List[Dict]]) -> str:
    return "".join((rt.get("plain_text") or "")
                    for rt in (rich or []))


def _block_to_md(block: Dict, indent: str = "") -> str:
    """Render a single Notion block to markdown. Best-effort — a
    block whose type we don't recognise still surfaces its plain
    text so nothing is silently dropped from the AI's view."""
    t = block.get("type") or ""
    data = block.get(t) or {}
    rt = _rich_text_to_plain(data.get("rich_text"))
    if t == "paragraph":
        return indent + rt
    if t == "heading_1":
        return f"# {rt}"
    if t == "heading_2":
        return f"## {rt}"
    if t == "heading_3":
        return f"### {rt}"
    if t == "bulleted_list_item":
        return f"{indent}- {rt}"
    if t == "numbered_list_item":
        return f"{indent}1. {rt}"
    if t == "to_do":
        return (f"{indent}- "
                f"{'[x]' if data.get('checked') else '[ ]'} "
                f"{rt}")
    if t == "toggle":
        return f"{indent}▸ {rt}"
    if t == "quote":
        return f"{indent}> {rt}"
    if t == "code":
        lang = data.get("language") or ""
        return f"```{lang}\n{rt}\n```"
    if t == "callout":
        emoji = (data.get("icon") or {}).get("emoji") or "💡"
        return f"{emoji} {rt}"
    if t == "divider":
        return "---"
    if t == "child_page":
        return f"(sub-page: {data.get('title') or ''})"
    if t == "table_row":
        cells = [
            _rich_text_to_plain(c) for c in
            (data.get("cells") or [])]
        return "| " + " | ".join(cells) + " |"
    # Fallback — emit text if present.
    return indent + rt if rt else ""


def _fetch_block_children(page_id: str,
                          cfg: Dict[str, str]) -> List[Dict]:
    """Page through /blocks/{id}/children — Notion paginates at 100."""
    blocks: List[Dict] = []
    cursor = None
    while True:
        path = (f"/blocks/{page_id}/children?page_size=100"
                + (f"&start_cursor={cursor}" if cursor else ""))
        body = _request("GET", path, cfg=cfg)
        blocks.extend(body.get("results") or [])
        if not body.get("has_more"):
            break
        cursor = body.get("next_cursor")
    return blocks


def _render_blocks_md(blocks: List[Dict],
                      cfg: Dict[str, str],
                      indent: str = "",
                      depth: int = 0) -> str:
    if depth > 6:
        return ""  # depth guard
    parts: List[str] = []
    for b in blocks:
        rendered = _block_to_md(b, indent)
        if rendered:
            parts.append(rendered)
        if b.get("has_children") and b.get("id"):
            try:
                children = _fetch_block_children(b["id"], cfg)
            except Exception as exc:  # noqa: BLE001
                log.warning("Could not fetch sub-blocks of %s: %s",
                              b.get("id"), exc)
                children = []
            sub = _render_blocks_md(
                children, cfg, indent + "    ", depth + 1)
            if sub:
                parts.append(sub)
    return "\n".join(p for p in parts if p)


# ---------------------------------------------------------------------------
# Playbook pull — operational knowledge -> local mirror
# ---------------------------------------------------------------------------
def _row_title(properties: Dict) -> str:
    """Extract the title property value from a Notion database
    row (each DB has exactly one title-typed property)."""
    for _name, val in (properties or {}).items():
        if (val or {}).get("type") == "title":
            return _rich_text_to_plain(val.get("title")) \
                or "(untitled)"
    return "(untitled)"


def _format_property_value(prop_val: Dict) -> str:
    """Render a single Notion property value to a plain string.
    Handles the common property types used in playbook databases."""
    if not isinstance(prop_val, dict):
        return ""
    t = prop_val.get("type")
    if t == "rich_text":
        return _rich_text_to_plain(prop_val.get("rich_text"))
    if t == "title":
        return _rich_text_to_plain(prop_val.get("title"))
    if t == "number":
        n = prop_val.get("number")
        return "" if n is None else str(n)
    if t == "select":
        return ((prop_val.get("select") or {})
                .get("name") or "")
    if t == "status":
        return ((prop_val.get("status") or {})
                .get("name") or "")
    if t == "multi_select":
        return ", ".join(
            (o or {}).get("name") or ""
            for o in (prop_val.get("multi_select") or []))
    if t == "date":
        d = prop_val.get("date") or {}
        start = d.get("start") or ""
        end = d.get("end")
        return f"{start} → {end}" if end else start
    if t == "checkbox":
        return "✓" if prop_val.get("checkbox") else "✗"
    if t == "url":
        return prop_val.get("url") or ""
    if t == "email":
        return prop_val.get("email") or ""
    if t == "phone_number":
        return prop_val.get("phone_number") or ""
    if t == "people":
        names = []
        for p in (prop_val.get("people") or []):
            names.append(p.get("name") or p.get("id") or "")
        return ", ".join(n for n in names if n)
    if t == "files":
        return ", ".join(
            (f or {}).get("name") or ""
            for f in (prop_val.get("files") or []))
    if t == "formula":
        f = prop_val.get("formula") or {}
        ft = f.get("type")
        if ft == "string":
            return f.get("string") or ""
        if ft == "number":
            n = f.get("number")
            return "" if n is None else str(n)
        if ft == "boolean":
            return "✓" if f.get("boolean") else "✗"
        if ft == "date":
            d = f.get("date") or {}
            return d.get("start") or ""
    if t in ("created_time", "last_edited_time"):
        return prop_val.get(t) or ""
    if t == "created_by":
        return (prop_val.get("created_by") or {}).get("name") or ""
    if t == "last_edited_by":
        return ((prop_val.get("last_edited_by") or {})
                .get("name") or "")
    return ""


def _format_row_properties(properties: Dict) -> str:
    """Render every non-title Notion DB property as a markdown
    bullet ('**Column name:** value'). Empty values skipped."""
    lines: List[str] = []
    for name, val in (properties or {}).items():
        if (val or {}).get("type") == "title":
            continue
        rendered = _format_property_value(val)
        if not rendered:
            continue
        lines.append(f"- **{name}:** {rendered}")
    return "\n".join(lines)


def pull_playbooks(dry_run: bool = False) -> Dict:
    """Walk the children of NOTION_PLAYBOOKS_PARENT_ID (falls
    back to NOTION_TEAM_PARENT_PAGE_ID). For each child_page,
    mirror it. For each child_database, query the database and
    mirror every row (each row is itself a page in Notion).
    Renders block content to markdown and upserts to the
    notion_kb_articles local mirror — the AI Assistant searches
    against the mirror via the search_knowledge_base tool."""
    cfg = _config()
    parent = (os.environ.get(
        "NOTION_PLAYBOOKS_PARENT_ID", "").strip().replace("-", "")
              or cfg["parent"])
    log.info("Pulling playbook content under parent %s ...",
             parent)
    cursor = None
    pages: List[Dict] = []
    n_child_pages = 0
    n_databases = 0
    n_db_rows = 0
    while True:
        path = (f"/blocks/{parent}/children?page_size=100"
                + (f"&start_cursor={cursor}" if cursor else ""))
        body = _request("GET", path, cfg=cfg)
        for b in body.get("results") or []:
            btype = b.get("type")
            if btype == "child_page":
                n_child_pages += 1
                pages.append({
                    "id": (b.get("id") or "").replace("-", ""),
                    "title": ((b.get("child_page") or {})
                              .get("title") or "(untitled)"),
                    "last_edited_time": b.get("last_edited_time"),
                    "source": "child_page",
                })
            elif btype == "child_database":
                n_databases += 1
                db_id = b.get("id")
                db_title = ((b.get("child_database") or {})
                            .get("title") or "(database)")
                log.info("  Walking database %r (%s) ...",
                          db_title, db_id)
                # Query the database for ALL rows; each is a page.
                db_cursor = None
                while True:
                    qbody = {"page_size": 100}
                    if db_cursor:
                        qbody["start_cursor"] = db_cursor
                    qres = _request(
                        "POST", f"/databases/{db_id}/query",
                        json_body=qbody, cfg=cfg)
                    for row in qres.get("results") or []:
                        row_id = (row.get("id")
                                  or "").replace("-", "")
                        title = _row_title(
                            row.get("properties") or {})
                        pages.append({
                            "id": row_id,
                            "title": f"{db_title} — {title}",
                            "last_edited_time": row.get(
                                "last_edited_time"),
                            "source": "database_row",
                            # v2.67.256 — keep the row's
                            # properties so the mirrored
                            # content includes column values,
                            # not just the page body.
                            "properties": row.get(
                                "properties") or {},
                        })
                        n_db_rows += 1
                    if not qres.get("has_more"):
                        break
                    db_cursor = qres.get("next_cursor")
        if not body.get("has_more"):
            break
        cursor = body.get("next_cursor")
    log.info(
        "Found %d page(s) to mirror: %d direct child pages + "
        "%d row(s) across %d database(s)",
        len(pages), n_child_pages, n_db_rows, n_databases)
    n_ok = 0
    n_err = 0
    for p in pages:
        try:
            blocks = _fetch_block_children(p["id"], cfg)
            body_md = _render_blocks_md(blocks, cfg).strip()
        except Exception as exc:  # noqa: BLE001
            log.warning("Block fetch failed for %s (%s): %s",
                          p["title"], p["id"], exc)
            n_err += 1
            continue
        # v2.67.256 — for database rows, include the row's
        # properties (column values) above the body so the AI
        # sees the structured data even when the body is empty.
        props_md = _format_row_properties(
            p.get("properties") or {})
        if props_md and body_md:
            md = props_md + "\n\n---\n\n" + body_md
        elif props_md:
            md = props_md
        else:
            md = body_md
        url = f"https://www.notion.so/{p['id']}"
        if dry_run:
            log.info("[dry-run] %s — %d chars",
                      p["title"], len(md))
            continue
        try:
            db.upsert_kb_article(
                page_id=p["id"],
                title=p["title"],
                content_md=md,
                url=url,
                notion_edited_at=p.get("last_edited_time"),
                category="playbook",
            )
            n_ok += 1
            log.info("Mirrored %r (%d chars)",
                      p["title"], len(md))
        except Exception as exc:  # noqa: BLE001
            log.warning("DB upsert failed for %s: %s",
                          p["title"], exc)
            n_err += 1
    return {"found": len(pages), "synced": n_ok,
            "errors": n_err, "dry_run": dry_run}


# ---------------------------------------------------------------------------
# Slow Movers — Phase 1 sync
# ---------------------------------------------------------------------------
SLOW_MOVERS_TITLE = "Slow Movers"
SLOW_MOVERS_DB_KEY = "slow_movers"  # registry key for db ID
SLOW_MOVERS_SCHEMA = {
    # Title property — Notion requires exactly one of these.
    "SKU": {"title": {}},
    "Name": {"rich_text": {}},
    "OnHand": {"number": {"format": "number"}},
    "Days dormant": {"number": {"format": "number"}},
    "First dormant": {"date": {}},
    "Last engine run": {"date": {}},
    "Status": {"select": {"options": [
        {"name": "Active", "color": "red"},
        {"name": "Cleared", "color": "green"},
        {"name": "Dismissed", "color": "gray"},
    ]}},
    "Cost tied up ($)": {"number": {"format": "dollar"}},
}


def _latest_csv(pattern: str) -> Optional[Path]:
    files = glob.glob(str(OUTPUT_DIR / pattern))
    if not files:
        return None
    return Path(max(files, key=os.path.getmtime))


def _load_sku_lookup(csv_path: Path,
                     sku_col: str,
                     value_cols: List[str]) -> Dict[str, Dict]:
    """Return {sku: {col: value}} for the given columns from a CSV.
    Skips missing columns gracefully."""
    if not csv_path or not csv_path.exists():
        return {}
    out: Dict[str, Dict] = {}
    with csv_path.open(encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            sku = (row.get(sku_col) or "").strip()
            if not sku:
                continue
            out[sku.upper()] = {
                c: row.get(c) for c in value_cols
                if c in row}
    return out


def _build_slow_mover_rows() -> List[Dict]:
    """Combine dormancy warnings with the products + stock CSVs
    into one flat list ready for Notion."""
    try:
        warnings = db.get_dormancy_warnings()
    except Exception as exc:  # noqa: BLE001
        log.error("Could not load dormancy warnings: %s", exc)
        return []
    if not warnings:
        return []
    prods = _load_sku_lookup(
        _latest_csv("products_*.csv"),
        sku_col="SKU",
        value_cols=["Name", "AverageCost", "Category", "Status"])
    stock = _load_sku_lookup(
        _latest_csv("stock_on_hand_*.csv"),
        sku_col="SKU",
        value_cols=["OnHand", "Allocated", "Available"])
    today = datetime.date.today()
    rows = []
    for sku, w in warnings.items():
        sku_u = sku.upper()
        p = prods.get(sku_u, {})
        s = stock.get(sku_u, {})
        on_hand = None
        try:
            on_hand = float(s.get("OnHand") or 0)
        except (TypeError, ValueError):
            pass
        cost_each = None
        try:
            cost_each = (float(p.get("AverageCost") or 0)
                         if p.get("AverageCost") else None)
        except (TypeError, ValueError):
            pass
        cost_tied = (
            on_hand * cost_each
            if (on_hand is not None and cost_each is not None)
            else None)
        # v2.67.251 — Postgres returns these as datetime objects,
        # SQLite as ISO strings. Coerce defensively so the [:10]
        # slice and date parsing work either way.
        _fd_raw = w.get("first_seen_dormant_at")
        first_dormant = str(_fd_raw)[:10] if _fd_raw else ""
        _le_raw = w.get("last_engine_run_at")
        last_engine_run = str(_le_raw)[:10] if _le_raw else ""
        days_dormant = None
        if first_dormant:
            try:
                fd = datetime.date.fromisoformat(first_dormant)
                days_dormant = (today - fd).days
            except ValueError:
                pass
        rows.append({
            "sku": sku,
            "name": p.get("Name") or "",
            "on_hand": on_hand,
            "days_dormant": days_dormant,
            "first_dormant": first_dormant,
            "last_engine_run": last_engine_run,
            "status": "Active",
            "cost_tied_up": cost_tied,
        })
    rows.sort(key=lambda r: -(r.get("cost_tied_up") or 0))
    return rows


def sync_slow_movers(dry_run: bool = False,
                       limit: Optional[int] = None) -> Dict:
    """Push the current slow-movers register to Notion. `limit`
    caps the rows pushed (highest cost-tied-up first) — defaults
    to NOTION_SLOW_MOVERS_LIMIT env var or 200; Notion's 3-req/s
    rate limit makes full pushes of large registers slow."""
    rows = _build_slow_mover_rows()
    if not rows:
        log.info("No slow movers to push.")
        return {"pushed": 0, "rows": 0}
    if limit is None:
        try:
            limit = int(os.environ.get(
                "NOTION_SLOW_MOVERS_LIMIT", "") or "200")
        except ValueError:
            limit = 200
    if limit and len(rows) > limit:
        log.info("Capping push to top %d of %d slow movers "
                  "(highest cost-tied-up first). Raise/clear via "
                  "--limit or NOTION_SLOW_MOVERS_LIMIT.",
                  limit, len(rows))
        rows = rows[:limit]
    log.info("Built %d slow-mover row(s) "
             "(top cost-tied-up: %s · $%s)",
             len(rows), rows[0]["sku"],
             f"{rows[0].get('cost_tied_up') or 0:,.0f}")
    if dry_run:
        log.info("[dry-run] sample row:\n%s",
                 json.dumps(rows[0], indent=2, default=str))
        return {"pushed": 0, "rows": len(rows), "dry_run": True}
    cfg = _config()
    db_id = find_or_create_database(
        SLOW_MOVERS_TITLE, SLOW_MOVERS_SCHEMA, cfg,
        registry_name=SLOW_MOVERS_DB_KEY)
    n_ok = 0
    n_err = 0
    for r in rows:
        try:
            upsert_row(db_id, "SKU", r["sku"], {
                "Name": _p_text(r.get("name")),
                "OnHand": _p_num(r.get("on_hand")),
                "Days dormant": _p_num(r.get("days_dormant")),
                "First dormant": _p_date(r.get("first_dormant")),
                "Last engine run": _p_date(
                    r.get("last_engine_run")),
                "Status": _p_select(r.get("status") or "Active"),
                "Cost tied up ($)": _p_num(
                    r.get("cost_tied_up")),
            }, cfg=cfg)
            n_ok += 1
        except Exception as exc:  # noqa: BLE001
            log.warning("Notion upsert failed for %s: %s",
                          r["sku"], exc)
            n_err += 1
    log.info("Notion sync done: %d ok, %d errors", n_ok, n_err)
    return {"pushed": n_ok, "errors": n_err, "rows": len(rows)}


# ---------------------------------------------------------------------------
# Product dimensions -> the Product Info database
# ---------------------------------------------------------------------------
# v2.67.276 — extract_dimensions.py crawled every Shopify product
# image with Claude vision and cached the cross-section specs in the
# product_dimensions table (one row per Shopify product/handle).
# v2.67.278 — per James: Notion's "Product Info" database is the
# single source of truth for everything product-related. So the
# dimension data is written INTO that existing database as page
# rows — NOT a separate database. We add the dimension columns to
# Product Info on first run, then upsert each product as a row
# keyed on the database's "Name" (title) column.
PRODUCT_INFO_TITLE = "Product Info"
PRODUCT_INFO_DB_KEY = "product_info"     # registry key for db ID
PRODUCT_INFO_TITLE_PROP = "Name"         # the DB's title column

# The dimension columns we add to the Product Info database. The
# title column ("Name") already exists and is NOT in here — a
# database has exactly one title property.
PRODUCT_DIM_COLUMNS = {
    "Handle": {"rich_text": {}},
    "Family": {"rich_text": {}},
    "Outer width (mm)": {"number": {"format": "number"}},
    "Outer height (mm)": {"number": {"format": "number"}},
    "Channel width (mm)": {"number": {"format": "number"}},
    "Channel depth (mm)": {"number": {"format": "number"}},
    "Max strip width (mm)": {"number": {"format": "number"}},
    "Wing width (mm)": {"number": {"format": "number"}},
    "Wing count": {"number": {"format": "number"}},
    "Mounting type": {"select": {"options": [
        {"name": "surface", "color": "blue"},
        {"name": "recessed", "color": "purple"},
        {"name": "mud-in", "color": "orange"},
        {"name": "corner", "color": "green"},
        {"name": "pendant", "color": "pink"},
        {"name": "unknown", "color": "gray"},
    ]}},
    "Profile shape": {"select": {"options": [
        {"name": "U", "color": "blue"},
        {"name": "square", "color": "green"},
        {"name": "angled", "color": "orange"},
        {"name": "round", "color": "purple"},
        {"name": "oval", "color": "pink"},
        {"name": "wing", "color": "yellow"},
        {"name": "unknown", "color": "gray"},
    ]}},
    "Clip lips": {"checkbox": {}},
    "Confidence": {"select": {"options": [
        {"name": "high", "color": "green"},
        {"name": "medium", "color": "yellow"},
        {"name": "low", "color": "red"},
    ]}},
    "Has diagram": {"checkbox": {}},
    "Notes": {"rich_text": {}},
    "Source image": {"url": {}},
    "Extracted": {"date": {}},
}

# Used only if the Product Info database can't be found and has to
# be created from scratch (normally it already exists).
PRODUCT_INFO_CREATE_SCHEMA = dict(
    {PRODUCT_INFO_TITLE_PROP: {"title": {}}}, **PRODUCT_DIM_COLUMNS)

# Columns that count as 'real' dimensional data — a row is worth
# pushing if it has a diagram or at least one of these populated.
_PRODUCT_DIM_VALUE_COLS = (
    "outer_width_mm", "outer_height_mm", "channel_width_mm",
    "channel_depth_mm", "max_strip_width_mm", "wing_width_mm")


def _dim_row_has_data(r: Dict) -> bool:
    if r.get("has_diagram"):
        return True
    return any(r.get(c) not in (None, "")
               for c in _PRODUCT_DIM_VALUE_COLS)


def _ensure_database_properties(db_id: str,
                                 props: Dict[str, Dict],
                                 cfg: Dict[str, str]) -> List[str]:
    """Add any properties in `props` that don't already exist on
    the database. Notion's PATCH /databases merges properties —
    existing columns, their data, and rows are untouched. Returns
    the list of column names that were added."""
    meta = _request("GET", f"/databases/{db_id}", cfg=cfg)
    existing = set((meta.get("properties") or {}).keys())
    missing = {k: v for k, v in props.items() if k not in existing}
    if not missing:
        return []
    _request("PATCH", f"/databases/{db_id}",
              json_body={"properties": missing}, cfg=cfg)
    added = sorted(missing)
    log.info("Added %d new column(s) to %r: %s",
              len(added), PRODUCT_INFO_TITLE, ", ".join(added))
    return added


def _sample_row_titles(db_id: str, title_prop: str,
                        cfg: Dict[str, str],
                        n: int = 12) -> List[str]:
    """Return up to `n` existing row titles from a database — used
    in dry-run to confirm name-matching before any write."""
    try:
        res = _request("POST", f"/databases/{db_id}/query",
                         json_body={"page_size": n}, cfg=cfg)
    except Exception as exc:  # noqa: BLE001
        return [f"<query failed: {exc}>"]
    out: List[str] = []
    for row in res.get("results") or []:
        tp = (row.get("properties") or {}).get(title_prop) or {}
        out.append(_rich_text_to_plain(tp.get("title"))
                   or "(untitled)")
    return out


def sync_product_dimensions(dry_run: bool = False,
                             include_empty: bool = False,
                             limit: Optional[int] = None) -> Dict:
    """Write the product_dimensions table (vision-extracted Shopify
    product specs) into the Notion "Product Info" database. Adds
    the dimension columns to that database on first run, then
    upserts each product as a row keyed on the "Name" column — so
    re-runs update in place rather than duplicating.

    Rows with no diagram and no dimensions are skipped unless
    include_empty=True."""
    try:
        rows = db.all_product_dimensions()
    except Exception as exc:  # noqa: BLE001
        log.error("Could not load product_dimensions: %s", exc)
        return {"pushed": 0, "rows": 0, "error": str(exc)}
    if not rows:
        log.info("product_dimensions table is EMPTY — nothing to "
                  "push. Run extract_dimensions.py first.")
        return {"pushed": 0, "rows": 0}
    total = len(rows)
    rows = [dict(r) for r in rows]
    if not include_empty:
        rows = [r for r in rows if _dim_row_has_data(r)]
    log.info("product_dimensions: %d row(s) in table, %d with "
              "dimensional data%s",
              total, len(rows),
              "" if include_empty
              else " (empty rows skipped — --include-empty to "
                    "push all)")
    if not rows:
        log.info("No rows with dimensional data to push.")
        return {"pushed": 0, "rows": 0}
    # Most-complete rows first so a --limit'd test run is useful.
    rows.sort(key=lambda r: -sum(
        1 for c in _PRODUCT_DIM_VALUE_COLS
        if r.get(c) not in (None, "")))
    if limit and len(rows) > limit:
        log.info("Capping push to first %d of %d rows.",
                  limit, len(rows))
        rows = rows[:limit]
    cfg = _config()
    # Resolve the Product Info database (title search finds it
    # under the parent page; ID is then cached in the registry).
    db_id = find_or_create_database(
        PRODUCT_INFO_TITLE, PRODUCT_INFO_CREATE_SCHEMA, cfg,
        registry_name=PRODUCT_INFO_DB_KEY)
    log.info("Target: %r database (%s), upsert keyed on %r.",
              PRODUCT_INFO_TITLE, db_id, PRODUCT_INFO_TITLE_PROP)

    if dry_run:
        existing_names = _sample_row_titles(
            db_id, PRODUCT_INFO_TITLE_PROP, cfg, n=12)
        log.info("Would add columns: %s",
                  ", ".join(sorted(PRODUCT_DIM_COLUMNS)))
        log.info("--- Sample EXISTING %r rows in the database: ---",
                  PRODUCT_INFO_TITLE_PROP)
        for nm in existing_names:
            log.info("   • %s", nm)
        log.info("--- Sample dimension titles to match/insert: ---")
        for r in rows[:12]:
            log.info("   • %s", r.get("title") or "(no title)")
        log.info("[dry-run] would upsert %d row(s) keyed on %r. "
                  "If the two lists above use the SAME naming, "
                  "rows match in place; if not, new rows are "
                  "added.", len(rows), PRODUCT_INFO_TITLE_PROP)
        return {"pushed": 0, "rows": len(rows), "dry_run": True}

    # Ensure the dimension columns exist before writing to them.
    _ensure_database_properties(db_id, PRODUCT_DIM_COLUMNS, cfg)

    n_ok = 0
    n_err = 0
    for r in rows:
        handle = (r.get("shopify_handle") or "").strip()
        name = (r.get("title") or handle or "").strip()
        if not name:
            continue
        props = {
            "Handle": _p_text(handle),
            "Family": _p_text(r.get("family")),
            "Outer width (mm)": _p_num(r.get("outer_width_mm")),
            "Outer height (mm)": _p_num(r.get("outer_height_mm")),
            "Channel width (mm)": _p_num(
                r.get("channel_width_mm")),
            "Channel depth (mm)": _p_num(
                r.get("channel_depth_mm")),
            "Max strip width (mm)": _p_num(
                r.get("max_strip_width_mm")),
            "Wing width (mm)": _p_num(r.get("wing_width_mm")),
            "Wing count": _p_num(r.get("wing_count")),
            "Mounting type": _p_select(r.get("mounting_type")),
            "Profile shape": _p_select(r.get("profile_shape")),
            "Clip lips": _p_checkbox(r.get("has_clip_lips")),
            "Confidence": _p_select(r.get("confidence")),
            "Has diagram": _p_checkbox(r.get("has_diagram")),
            "Notes": _p_text(r.get("extra_notes")),
            "Source image": _p_url(r.get("source_image_url")),
            "Extracted": _p_date(
                str(r.get("extracted_at") or "")[:10]),
        }
        try:
            upsert_row(db_id, PRODUCT_INFO_TITLE_PROP, name,
                       props, cfg=cfg)
            n_ok += 1
        except Exception as exc:  # noqa: BLE001
            log.warning("Notion upsert failed for %s: %s",
                          name, exc)
            n_err += 1
    log.info("Product-dimensions sync done: %d ok, %d errors",
              n_ok, n_err)
    return {"pushed": n_ok, "errors": n_err, "rows": len(rows)}


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def _setup_log(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)-8s %(message)s",
        stream=sys.stdout, force=True)


def cmd_slow_movers(args) -> int:
    _setup_log(args.verbose)
    limit = getattr(args, "limit", None)
    result = sync_slow_movers(
        dry_run=bool(args.dry_run),
        limit=(int(limit) if limit else None))
    log.info("DONE: %s", result)
    return 0


def cmd_product_dimensions(args) -> int:
    _setup_log(args.verbose)
    limit = getattr(args, "limit", None)
    result = sync_product_dimensions(
        dry_run=bool(args.dry_run),
        include_empty=bool(args.include_empty),
        limit=(int(limit) if limit else None))
    log.info("DONE: %s", result)
    return 0


def cmd_pull_playbooks(args) -> int:
    _setup_log(args.verbose)
    result = pull_playbooks(dry_run=bool(args.dry_run))
    log.info("DONE: %s", result)
    return 0


def cmd_dump_glossary(args) -> int:
    """Write the current engine glossary to a markdown file —
    drop the file into Notion via Import -> Markdown for a clean
    page. Re-run after the engine evolves to refresh."""
    _setup_log(args.verbose)
    try:
        from intelligence_glossary import GLOSSARY_MARKDOWN
    except ImportError as exc:
        log.error("intelligence_glossary import failed: %s", exc)
        return 1
    output = args.output or "/tmp/app_glossary.md"
    header = (
        "# Wired4Signs App Glossary\n\n"
        "_Single source of truth for the ABC engine's "
        "intelligence rules and the signals the app surfaces. "
        "Generated from `intelligence_glossary.py`. To refresh: "
        "`python notion_sync.py dump-glossary`._\n\n---\n"
    )
    with open(output, "w", encoding="utf-8") as f:
        f.write(header)
        f.write(GLOSSARY_MARKDOWN.lstrip("\n"))
    log.info("Wrote %d chars -> %s",
             len(GLOSSARY_MARKDOWN), output)
    print(f"OK: {output}")
    return 0


def cmd_set_db_id(args) -> int:
    """v2.67.257 — manually register a Notion database ID under
    a logical name. Use after cleaning up duplicate databases:
    delete the dud in Notion, then bind the sync to the kept
    one so subsequent runs upsert into it."""
    _setup_log(args.verbose)
    name = (args.name or "").strip().lower()
    db_id = (args.db_id or "").strip().replace("-", "")
    if not name or not db_id:
        log.error("--name and --db-id are required")
        return 1
    db.set_notion_db_id(name, db_id)
    log.info("Set notion_db_ids[%s] = %s", name, db_id)
    return 0


def cmd_clear_db_id(args) -> int:
    """Forget a stored Notion database id — the next sync run
    will look it up by title or create a fresh one."""
    _setup_log(args.verbose)
    name = (args.name or "").strip().lower()
    if not name:
        log.error("--name is required")
        return 1
    db.clear_notion_db_id(name)
    log.info("Cleared notion_db_ids[%s]", name)
    return 0


def push_procedure_rule(title: str, content: str,
                         category: str = "procedures",
                         dry_run: bool = False) -> Dict:
    """v2.67.274 — Create or update a procedure/rule page under the
    Notion playbooks parent and immediately seed it into the local
    kb_articles table so the AI can search it right away (without
    waiting for the next 30-min pull-playbooks run).

    The page is created as a child of NOTION_PLAYBOOKS_PARENT_ID (falls
    back to NOTION_TEAM_PARENT_PAGE_ID if unset). Page content is a
    single paragraph block. Idempotent: the pull-playbooks sync will
    overwrite the kb_articles row on the next run with the same content,
    but the seed here means it's searchable immediately."""
    cfg = _config()
    playbooks_parent = os.environ.get(
        "NOTION_PLAYBOOKS_PARENT_ID", "").strip().replace("-", "")
    parent_id = playbooks_parent or cfg["parent"]

    now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()

    if dry_run:
        log.info("[DRY RUN] Would create Notion page '%s' under %s",
                  title, parent_id)
        log.info("[DRY RUN] Content:\n%s", content)
        return {"status": "dry_run", "title": title}

    # 1. Create or update the Notion page
    # First check if a page with this title already exists as a child.
    existing_id = None
    try:
        res = _request("POST",
                        f"/search",
                        json_body={"query": title,
                                    "filter": {"value": "page",
                                               "property": "object"}},
                        cfg=cfg)
        for item in (res.get("results") or []):
            if item.get("object") != "page":
                continue
            parent_block = item.get("parent", {})
            if parent_block.get("page_id", "").replace("-", "") != parent_id:
                continue
            # Check title matches
            props = item.get("properties", {})
            for _p in props.values():
                _t = _p.get("title") or []
                page_title = "".join(
                    t.get("plain_text", "") for t in _t)
                if page_title.strip().lower() == title.strip().lower():
                    existing_id = item["id"].replace("-", "")
                    break
            if existing_id:
                break
    except Exception as exc:
        log.warning("Notion page lookup failed: %s", exc)

    paragraph_block = {
        "object": "block",
        "type": "paragraph",
        "paragraph": {
            "rich_text": [{"type": "text",
                            "text": {"content": content[:2000]}}]
        },
    }
    if existing_id:
        # Update: append a new block (simplest approach — prepend the
        # updated content; full replacement would need to delete all
        # existing blocks first which is complex and risky).
        try:
            _request("PATCH",
                      f"/blocks/{existing_id}/children",
                      json_body={"children": [paragraph_block]},
                      cfg=cfg)
            notion_page_id = existing_id
            log.info("Updated existing Notion page '%s' (%s)",
                      title, notion_page_id)
        except Exception as exc:
            log.warning("Notion page update failed: %s", exc)
            notion_page_id = existing_id
    else:
        try:
            res = _request("POST", "/pages", json_body={
                "parent": {"page_id": parent_id},
                "properties": {
                    "title": {"title": [{"type": "text",
                                          "text": {"content": title}}]}
                },
                "children": [paragraph_block],
            }, cfg=cfg)
            notion_page_id = res.get("id", "").replace("-", "")
            log.info("Created Notion page '%s' (%s)",
                      title, notion_page_id)
        except Exception as exc:
            log.error("Notion page creation failed: %s", exc)
            notion_page_id = ""

    # 2. Seed into kb_articles immediately (don't wait 30-min pull).
    notion_url = (
        f"https://www.notion.so/{notion_page_id}"
        if notion_page_id else "")
    try:
        db.upsert_kb_article(
            page_id=notion_page_id or f"local_{title[:32]}",
            title=title,
            content_md=content,
            url=notion_url,
            notion_edited_at=now_iso,
            category=category,
        )
        log.info("Seeded kb_articles: '%s' (category=%s)",
                  title, category)
    except Exception as exc:
        log.warning("kb_articles seed failed: %s", exc)

    return {
        "status": "ok",
        "title": title,
        "notion_page_id": notion_page_id,
        "notion_url": notion_url,
        "category": category,
    }


def cmd_push_rule(args) -> int:
    """v2.67.274 — Push a named procedure/rule to Notion + seed
    it locally so the AI finds it immediately."""
    _setup_log(args.verbose)
    title = (args.title or "").strip()
    content = (args.content or "").strip()
    if not title or not content:
        log.error("--title and --content are required")
        return 1
    result = push_procedure_rule(
        title=title,
        content=content,
        category=(args.category or "procedures"),
        dry_run=bool(args.dry_run))
    log.info("DONE: %s", result)
    return 0


def cmd_inspect(args) -> int:
    """v2.67.277 — list every child database + sub-page directly
    under the parent page, with each database's column names and
    row count. Use this to find the exact database to write into
    before pointing a sync at it."""
    _setup_log(args.verbose)
    cfg = _config()
    parent = cfg["parent"]
    cursor = None
    n_db = 0
    n_pg = 0
    print(f"\nChildren of parent page {parent}:")
    print("=" * 64)
    while True:
        path = (f"/blocks/{parent}/children?page_size=100"
                + (f"&start_cursor={cursor}" if cursor else ""))
        body = _request("GET", path, cfg=cfg)
        for b in body.get("results") or []:
            t = b.get("type")
            if t == "child_database":
                n_db += 1
                db_id = (b.get("id") or "")
                title = ((b.get("child_database") or {})
                         .get("title") or "(untitled)")
                cols: List[str] = []
                n_rows = "?"
                try:
                    meta = _request("GET", f"/databases/{db_id}",
                                     cfg=cfg)
                    cols = sorted((meta.get("properties")
                                   or {}).keys())
                    q = _request(
                        "POST", f"/databases/{db_id}/query",
                        json_body={"page_size": 1}, cfg=cfg)
                    n_rows = ("1+" if q.get("results")
                              else "0")
                    if q.get("has_more"):
                        n_rows = "many"
                except Exception as exc:  # noqa: BLE001
                    cols = [f"<could not read: {exc}>"]
                print(f"\nDATABASE  {title!r}")
                print(f"  id      : {db_id.replace('-', '')}")
                print(f"  rows    : {n_rows}")
                print(f"  columns : {', '.join(cols)}")
            elif t == "child_page":
                n_pg += 1
                title = ((b.get("child_page") or {})
                         .get("title") or "(untitled)")
                print(f"\nSUB-PAGE  {title!r}")
                print("  id      : "
                      f"{(b.get('id') or '').replace('-', '')}")
        if not body.get("has_more"):
            break
        cursor = body.get("next_cursor")
    print("=" * 64)
    print(f"Found {n_db} database(s) and {n_pg} sub-page(s).\n")
    return 0


def cmd_check(args) -> int:
    """Smoke-test the Notion auth + parent-page access."""
    _setup_log(args.verbose)
    cfg = _config()
    me = _request("GET", "/users/me", cfg=cfg)
    parent = _request(
        "GET", f"/pages/{cfg['parent']}", cfg=cfg)
    title_parts = []
    for tt in ((parent.get("properties") or {})
               .get("title", {}).get("title") or []):
        title_parts.append(tt.get("plain_text") or "")
    log.info("Auth ok — bot: %s",
              (me.get("bot") or {}).get("owner", {}).get(
                  "type", "(unknown)"))
    log.info("Parent page reachable: %r",
              "".join(title_parts) or "(untitled)")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Sync operational data into Notion.")
    sub = parser.add_subparsers(dest="cmd", required=True)
    p_sm = sub.add_parser(
        "slow-movers",
        help="Push the current slow-movers register.")
    p_sm.add_argument("--dry-run", action="store_true")
    p_sm.add_argument("--verbose", action="store_true")
    p_sm.add_argument("--limit", type=int, default=None,
                        help="Cap pushed rows (default 200; set "
                              "to 0 for unlimited).")
    p_sm.set_defaults(func=cmd_slow_movers)
    p_pd = sub.add_parser(
        "product-dimensions",
        help="Push the vision-extracted product_dimensions table "
              "(Shopify product specs) to a Notion database.")
    p_pd.add_argument("--dry-run", action="store_true")
    p_pd.add_argument("--verbose", action="store_true")
    p_pd.add_argument(
        "--include-empty", action="store_true",
        help="Also push rows with no diagram and no dimensions.")
    p_pd.add_argument("--limit", type=int, default=None,
                        help="Cap pushed rows (most-complete first).")
    p_pd.set_defaults(func=cmd_product_dimensions)
    p_pb = sub.add_parser(
        "pull-playbooks",
        help="Mirror playbook child-pages from Notion into "
              "notion_kb_articles for the AI to search.")
    p_pb.add_argument("--dry-run", action="store_true")
    p_pb.add_argument("--verbose", action="store_true")
    p_pb.set_defaults(func=cmd_pull_playbooks)
    p_dg = sub.add_parser(
        "dump-glossary",
        help="Write the engine glossary to a markdown file you "
              "can import into Notion.")
    p_dg.add_argument("--output", default=None,
                       help="Output path (default /tmp/"
                             "app_glossary.md).")
    p_dg.add_argument("--verbose", action="store_true")
    p_dg.set_defaults(func=cmd_dump_glossary)
    p_sd = sub.add_parser(
        "set-db-id",
        help="Bind a logical sync name to an existing Notion "
              "database ID (e.g. after cleaning up duplicates).")
    p_sd.add_argument("--name", required=True,
                        help="Logical name, e.g. 'slow_movers'.")
    p_sd.add_argument("--db-id", required=True,
                        help="The Notion database ID (32-char "
                              "hex, hyphens optional).")
    p_sd.add_argument("--verbose", action="store_true")
    p_sd.set_defaults(func=cmd_set_db_id)
    p_cd = sub.add_parser(
        "clear-db-id",
        help="Forget a stored database id — next sync re-resolves.")
    p_cd.add_argument("--name", required=True)
    p_cd.add_argument("--verbose", action="store_true")
    p_cd.set_defaults(func=cmd_clear_db_id)
    p_pr = sub.add_parser(
        "push-rule",
        help="Create/update a procedure rule page in Notion and "
              "immediately seed it into the local AI knowledge base.")
    p_pr.add_argument("--title", required=True,
                       help="Page title, e.g. 'Rule: Always report "
                             "warehouse bin location in stock queries'.")
    p_pr.add_argument("--content", required=True,
                       help="Full rule text (plain prose).")
    p_pr.add_argument("--category", default="procedures",
                       help="kb_articles category (default: procedures).")
    p_pr.add_argument("--dry-run", action="store_true")
    p_pr.add_argument("--verbose", action="store_true")
    p_pr.set_defaults(func=cmd_push_rule)
    p_in = sub.add_parser(
        "inspect",
        help="List child databases + sub-pages under the parent "
              "page, with each database's columns and row count.")
    p_in.add_argument("--verbose", action="store_true")
    p_in.set_defaults(func=cmd_inspect)
    p_ck = sub.add_parser(
        "check", help="Verify auth and parent-page access.")
    p_ck.add_argument("--verbose", action="store_true")
    p_ck.set_defaults(func=cmd_check)
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
