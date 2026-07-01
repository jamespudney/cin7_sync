"""
ip_sync_notes.py
================
Pull current Inventory Planner replenishment notes into the shared data
folder used by the dashboard and Slack bot.

The full `ip_pull_alternates.py` export also captures notes, but it walks
many extra fields. This script is intentionally narrow and fast so buyer
notes can be refreshed without waiting on the heavier Inventory Planner
knowledge export.

Output
------
    DATA_DIR/output/ip_notes_<stamp>.csv
    DATA_DIR/output/ip-notes-probe_<sku>_<stamp>.csv for --sku smoke tests

Columns match the existing dashboard loader:
    SKU, VariantID, WarehouseID, Note, Tags

Env: IP_API_KEY, IP_ACCOUNT.
"""

from __future__ import annotations

import argparse
import csv
import logging
import os
import sys
import time
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional

import requests
from dotenv import load_dotenv

from data_paths import OUTPUT_DIR

BASE_URL = "https://app.inventory-planner.com/api/v1"
PAGE_SIZE = 1000
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 cin7-sync/1.0"
)

# Request the full warehouse block. Inventory Planner's nested field allowlist
# can return only the warehouse id when asked for `warehouse.replenishment_notes`
# directly, which makes every note look blank. The broader field mirrors the
# older ip_pull_alternates export that successfully surfaces buyer notes.
FIELDS = "id,connections,tags,warehouse"


def _setup_log(verbose: bool = False) -> logging.Logger:
    log = logging.getLogger("ip_notes")
    log.setLevel(logging.DEBUG if verbose else logging.INFO)
    if not log.handlers:
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        fh = logging.FileHandler(OUTPUT_DIR / "ip_notes_sync.log",
                                 encoding="utf-8")
        fh.setFormatter(logging.Formatter(
            "%(asctime)s  %(levelname)-8s %(message)s"))
        log.addHandler(fh)
        sh = logging.StreamHandler()
        sh.setFormatter(logging.Formatter("%(message)s"))
        log.addHandler(sh)
    return log


def _master_sku(variant: Dict[str, Any]) -> Optional[str]:
    conns = variant.get("connections") or []
    if conns and isinstance(conns[0], dict):
        return conns[0].get("sku")
    return None


def _tags_string(variant: Dict[str, Any]) -> str:
    tags = variant.get("tags") or []
    if isinstance(tags, list):
        return ",".join(str(t) for t in tags if str(t).strip())
    return ""


def _warehouse_id(warehouse: Dict[str, Any]) -> str:
    raw = (
        warehouse.get("warehouse")
        or warehouse.get("warehouse_id")
        or warehouse.get("id")
        or ""
    )
    if isinstance(raw, dict):
        return str(raw.get("id") or raw.get("name") or "")
    return str(raw or "")


def _collect_warehouse_blocks(variant: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Collect every warehouse-like block IP may return."""
    out: List[Dict[str, Any]] = []
    for key in ("warehouse", "warehouses"):
        value = variant.get(key)
        if isinstance(value, list):
            out.extend(w for w in value if isinstance(w, dict))
        elif isinstance(value, dict):
            out.append(value)
    for conn in variant.get("connections") or []:
        if not isinstance(conn, dict):
            continue
        for key in ("warehouse", "warehouses"):
            value = conn.get(key)
            if isinstance(value, list):
                out.extend(w for w in value if isinstance(w, dict))
            elif isinstance(value, dict):
                out.append(value)
    return out


def _note_text(warehouse: Dict[str, Any]) -> str:
    for key in (
        "replenishment_notes",
        "replenishment_note",
        "buyer_notes",
        "notes",
        "note",
    ):
        value = warehouse.get(key)
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return ""


def _safe_filename_token(value: str) -> str:
    token = "".join(
        ch if ch.isalnum() or ch in {"-", "_", "."} else "_"
        for ch in str(value or "")
    ).strip("._")
    return token or "all"


def _api_get(url: str,
             *,
             headers: Dict[str, str],
             params: Dict[str, Any],
             log: logging.Logger) -> Optional[Dict[str, Any]]:
    while True:
        try:
            resp = requests.get(url, headers=headers, params=params,
                                timeout=60)
        except requests.RequestException as exc:
            log.warning("network error: %s — retry in 5s", exc)
            time.sleep(5)
            continue
        if resp.status_code == 429:
            wait = int(resp.headers.get("Retry-After", "30"))
            log.warning("429 rate limit. Sleeping %ds", wait)
            time.sleep(wait)
            continue
        if resp.status_code != 200:
            log.error("request failed status=%d body=%s",
                      resp.status_code, resp.text[:500])
            return None
        try:
            return resp.json()
        except ValueError:
            log.error("request returned non-JSON: %s", resp.text[:500])
            return None


def fetch_variants(headers: Dict[str, str],
                   *,
                   rate: float,
                   sku: str = "",
                   limit_pages: Optional[int] = None,
                   log: logging.Logger) -> Iterable[Dict[str, Any]]:
    last_call = 0.0
    page = 0
    while True:
        elapsed = time.time() - last_call
        if elapsed < rate:
            time.sleep(rate - elapsed)

        params: Dict[str, Any] = {
            "limit": 5 if sku else PAGE_SIZE,
            "fields": FIELDS,
        }
        if sku:
            params["sku"] = sku
        else:
            params["page"] = page

        data = _api_get(f"{BASE_URL}/variants", headers=headers,
                        params=params, log=log)
        last_call = time.time()
        if data is None:
            return

        variants = data.get("variants") or []
        if sku:
            log.info("SKU %s -> %d record(s)", sku, len(variants))
        else:
            total = (data.get("meta") or {}).get("total")
            log.info("page %d -> %d records (total=%s)",
                     page, len(variants), total)
        for variant in variants:
            yield variant

        if sku or len(variants) < PAGE_SIZE:
            return
        page += 1
        if limit_pages is not None and page >= limit_pages:
            return


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Sync Inventory Planner replenishment notes")
    parser.add_argument(
        "--rate", type=float, default=None,
        help="Seconds between API calls (default: IP_RATE_SECONDS or 1.0)")
    parser.add_argument(
        "--sku", default="",
        help="Optional smoke-test SKU. Example: LED-SMOKIES38-B-3")
    parser.add_argument(
        "--limit-pages", type=int, default=None,
        help="Stop after N pages for a smoke test. Omit for full sync.")
    parser.add_argument(
        "--verbose", action="store_true",
        help="Print debug-level logs.")
    parser.add_argument(
        "--allow-empty", action="store_true",
        help="Publish an empty live ip_notes CSV. Normally blocked so a "
             "bad API field request cannot hide existing notes.")
    args = parser.parse_args()

    load_dotenv()
    key = os.environ.get("IP_API_KEY")
    account = os.environ.get("IP_ACCOUNT")
    if not key or not account:
        print("ERROR: IP_API_KEY / IP_ACCOUNT not set")
        return 1

    rate = (
        args.rate
        if args.rate is not None
        else float(os.environ.get("IP_RATE_SECONDS", "1.0"))
    )
    headers = {
        "Authorization": key,
        "Account": account,
        "Accept": "application/json",
        "User-Agent": USER_AGENT,
    }

    log = _setup_log(args.verbose)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    is_probe = bool(args.sku.strip() or args.limit_pages is not None)
    if is_probe:
        safe_sku = _safe_filename_token(args.sku.strip() or "page-test")
        notes_csv = OUTPUT_DIR / f"ip-notes-probe_{safe_sku}_{stamp}.csv"
    else:
        notes_csv = OUTPUT_DIR / f"ip_notes_{stamp}.csv"
    tmp_csv = OUTPUT_DIR / f".{notes_csv.name}.tmp"

    n_variants = 0
    n_note_rows = 0
    seen = set()
    with tmp_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["SKU", "VariantID", "WarehouseID", "Note", "Tags"])
        for variant in fetch_variants(
            headers,
            rate=rate,
            sku=args.sku.strip(),
            limit_pages=args.limit_pages,
            log=log,
        ):
            n_variants += 1
            sku = _master_sku(variant)
            if not sku:
                continue
            variant_id = variant.get("id") or ""
            tags = _tags_string(variant)
            for warehouse in _collect_warehouse_blocks(variant):
                note = _note_text(warehouse)
                if not note:
                    continue
                warehouse_id = _warehouse_id(warehouse)
                dedupe_key = (sku, variant_id, warehouse_id, note)
                if dedupe_key in seen:
                    continue
                seen.add(dedupe_key)
                writer.writerow([
                    sku,
                    variant_id,
                    warehouse_id,
                    note,
                    tags,
                ])
                n_note_rows += 1

    if n_note_rows == 0 and not (is_probe or args.allow_empty):
        empty_csv = OUTPUT_DIR / f"empty-ip-notes_{stamp}.csv"
        tmp_csv.replace(empty_csv)
        log.error(
            "Done, but 0 note rows were found across %d variants. "
            "Did not publish a live ip_notes CSV; wrote diagnostic file: %s",
            n_variants, empty_csv,
        )
        return 2

    tmp_csv.replace(notes_csv)
    log.info("Done. Variants scanned: %d", n_variants)
    log.info("Note rows written: %d", n_note_rows)
    log.info("%s CSV: %s", "Probe" if is_probe else "Notes", notes_csv)
    return 0


if __name__ == "__main__":
    sys.exit(main())
