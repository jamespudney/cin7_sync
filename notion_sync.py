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
    try:
        r = requests.request(method, url, headers=headers,
                              json=json_body, timeout=30)
    except requests.RequestException as exc:
        raise RuntimeError(
            f"Notion API network error ({method} {path}): "
            f"{exc}") from exc
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
                             cfg: Optional[Dict[str, str]] = None
                             ) -> str:
    cfg = cfg or _config()
    existing = _find_database_by_title(
        cfg["parent"], title, cfg)
    if existing:
        return existing
    log.info("Creating Notion database %r ...", title)
    return _create_database(cfg["parent"], title, schema, cfg)


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
    return {"select": ({"name": value} if value else None)}


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
def pull_playbooks(dry_run: bool = False) -> Dict:
    """List every child_page under NOTION_PLAYBOOKS_PARENT_ID
    (falls back to NOTION_TEAM_PARENT_PAGE_ID), fetch each page's
    content, render to markdown, and upsert into the local
    notion_kb_articles mirror. The AI Assistant then searches
    against the mirror via the search_knowledge_base tool."""
    cfg = _config()
    parent = (os.environ.get(
        "NOTION_PLAYBOOKS_PARENT_ID", "").strip().replace("-", "")
              or cfg["parent"])
    log.info("Pulling playbook child-pages under parent %s ...",
             parent)
    # List child_page blocks under the parent.
    cursor = None
    pages: List[Dict] = []
    while True:
        path = (f"/blocks/{parent}/children?page_size=100"
                + (f"&start_cursor={cursor}" if cursor else ""))
        body = _request("GET", path, cfg=cfg)
        for b in body.get("results") or []:
            if b.get("type") != "child_page":
                continue
            pages.append({
                "id": (b.get("id") or "").replace("-", ""),
                "title": ((b.get("child_page") or {})
                          .get("title") or "(untitled)"),
                "last_edited_time": b.get("last_edited_time"),
            })
        if not body.get("has_more"):
            break
        cursor = body.get("next_cursor")
    log.info("Found %d child page(s) under parent", len(pages))
    n_ok = 0
    n_err = 0
    for p in pages:
        try:
            blocks = _fetch_block_children(p["id"], cfg)
            md = _render_blocks_md(blocks, cfg).strip()
        except Exception as exc:  # noqa: BLE001
            log.warning("Block fetch failed for %s (%s): %s",
                          p["title"], p["id"], exc)
            n_err += 1
            continue
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


def sync_slow_movers(dry_run: bool = False) -> Dict:
    """Push the current slow-movers register to Notion."""
    rows = _build_slow_mover_rows()
    if not rows:
        log.info("No slow movers to push.")
        return {"pushed": 0, "rows": 0}
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
        SLOW_MOVERS_TITLE, SLOW_MOVERS_SCHEMA, cfg)
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
# CLI
# ---------------------------------------------------------------------------
def _setup_log(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)-8s %(message)s",
        stream=sys.stdout, force=True)


def cmd_slow_movers(args) -> int:
    _setup_log(args.verbose)
    result = sync_slow_movers(dry_run=bool(args.dry_run))
    log.info("DONE: %s", result)
    return 0


def cmd_pull_playbooks(args) -> int:
    _setup_log(args.verbose)
    result = pull_playbooks(dry_run=bool(args.dry_run))
    log.info("DONE: %s", result)
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
    p_sm.set_defaults(func=cmd_slow_movers)
    p_pb = sub.add_parser(
        "pull-playbooks",
        help="Mirror playbook child-pages from Notion into "
              "notion_kb_articles for the AI to search.")
    p_pb.add_argument("--dry-run", action="store_true")
    p_pb.add_argument("--verbose", action="store_true")
    p_pb.set_defaults(func=cmd_pull_playbooks)
    p_ck = sub.add_parser(
        "check", help="Verify auth and parent-page access.")
    p_ck.add_argument("--verbose", action="store_true")
    p_ck.set_defaults(func=cmd_check)
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
