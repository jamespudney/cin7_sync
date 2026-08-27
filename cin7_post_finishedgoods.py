"""
cin7_post_finishedgoods.py
============================
Push a received 865FabLab order (a po_drafts row scoped to supplier
"865FabLab") into CIN7 as real Finished Goods (Assembly) tasks -- one
per finished SKU line -- rather than a raw stock adjustment.

Why Finished Goods, not a stock adjustment
-------------------------------------------
A stock adjustment has no BOM/consumption semantics in CIN7's data
model -- it's just "quantity changed." CIN7's Finished Goods feature
(the same `/finishedGoods` endpoint family cin7_sync.sync_assemblies()
already reads to populate db.assembly_component_consumption) is the
purpose-built primitive for exactly this: consume a product's linked
BOM components and produce finished stock, as one atomic CIN7-native
operation. Pushing through this endpoint means these 865FabLab batches
automatically show up in the ABC engine's most trusted demand signal
for the raw materials (assemblies_df, "ground truth from FG- pick
lines" -- see _abc_engine()'s docstring in app.py) instead of relying
on the sales-based BOM-rollup proxy. James 2026-08-27 confirmed this
after asking "wouldn't an assembly be picked up as component sales,
as opposed to a stock adjustment?" -- correct, and better.

CIN7 API flow (per dearinventory.apib § Finished Goods)
-----------------------------------------------------------
    POST /finishedGoods
      Body: Status="COMPLETED", ProductID, Quantity, Location,
            WIPAccount, WIPDate, Account, CompletionDate, Notes
      -> CIN7 auto-populates OrderLines (from the product's own linked
         BOM) and PickLines (what was actually consumed), consumes the
         components, and adds the finished stock -- all in one call.
         One task per product; an order with multiple finished SKUs
         needs one POST per SKU.

James chose Status=COMPLETED (one click, no intermediate CIN7-side
review step) over AUTHORISED (create + populate BOM lines for review,
consume nothing yet) -- 2026-08-27. The safety checkpoint instead lives
entirely in our own UI: a read-only preview (estimated from our own
BOM data, since CIN7 doesn't expose a dry-run for this endpoint) before
an explicit acknowledgement gates the real push. A completed task can
still be voided afterward (DELETE /finishedGoods?ID=&Void=true) if
something turns out wrong.

Accounts (confirmed with James 2026-08-27, from his live chart of
accounts): WIPAccount="120" ("Inventory - Work In Progress"),
Account="115" ("Inventory - On Hand").

⚠ Hard rules
------------
* Status is always "COMPLETED" per James's explicit choice -- if this
  ever needs to change to a reviewable AUTHORISED/DRAFT flow, that's a
  parameter change here, not a rewrite.
* Even on HTTP 200, check the response's `Errors` array -- the API
  docs note a task can be CREATED but partially fail ("some errors
  occurred, but task was created"). Surface those, don't treat 200 as
  unconditional success.
* Idempotency is per (draft_id, sku) -- an order can have more than one
  finished SKU line, each becomes its own CIN7 task, and one can
  succeed while another fails.

Usage
-----
    from cin7_post_finishedgoods import push_finished_goods
    results = push_finished_goods(
        draft_id=42, bom_parents=BOM_PARENTS, actor="james", apply=True)
    for r in results:
        if r.ok:
            print(r.sku, r.cin7_assembly_number)
        else:
            print(r.sku, r.errors)
"""

from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

import requests
from dotenv import load_dotenv

import db
from data_paths import OUTPUT_DIR


BASE_URL = "https://inventory.dearsystems.com/ExternalApi/v2"
DEFAULT_RATE_S = 1.5
MAX_429_RETRIES = 3

DEFAULT_WIP_ACCOUNT = "120"  # Inventory - Work In Progress
DEFAULT_ACCOUNT = "115"      # Inventory - On Hand


@dataclass
class FinishedGoodsResult:
    """Result for ONE SKU's Finished Goods push. push_finished_goods
    returns a list of these (one order can have multiple finished
    SKU lines, each its own CIN7 task). stage: 'validated' / 'dry_run'
    / 'posted'."""
    sku: str
    ok: bool
    stage: str = "init"
    quantity: float = 0.0
    cin7_task_id: Optional[str] = None
    cin7_assembly_number: Optional[str] = None
    errors: list = field(default_factory=list)
    warnings: list = field(default_factory=list)
    estimated_consumption: list = field(default_factory=list)
    response: Optional[dict] = None


# ---------------------------------------------------------------------------
# HTTP helpers -- duplicated (not imported) from cin7_post_po.py on purpose.
# Both modules are small, sensitive, and independently reviewed; sharing a
# module here would mean any future edit to one risks the other's working,
# tested PO push path.
# ---------------------------------------------------------------------------

def _parse_retry_after(value, default: int = 30) -> int:
    if value is None:
        return default
    digits = "".join(ch for ch in str(value) if ch.isdigit())
    return int(digits) if digits else default


def _setup_log(stamp: str) -> logging.Logger:
    log = logging.getLogger("cin7_post_finishedgoods")
    log.setLevel(logging.INFO)
    if not log.handlers:
        fh = logging.FileHandler(
            OUTPUT_DIR / f"cin7_post_finishedgoods_{stamp}.log",
            encoding="utf-8")
        fh.setFormatter(logging.Formatter(
            "%(asctime)s  %(levelname)-8s %(message)s"))
        log.addHandler(fh)
        sh = logging.StreamHandler()
        sh.setFormatter(logging.Formatter("%(message)s"))
        log.addHandler(sh)
    return log


def _throttle(last_call_t: float, rate_s: float) -> float:
    elapsed = time.time() - last_call_t
    if elapsed < rate_s:
        time.sleep(rate_s - elapsed)
    return time.time()


def _http(method: str, url: str, headers: dict, *,
          json_body=None, params=None, log=None,
          rate_s: float = DEFAULT_RATE_S,
          last_call: float = 0.0) -> tuple[Optional[requests.Response], float]:
    resp = None
    for attempt in range(MAX_429_RETRIES + 1):
        last_call = _throttle(last_call, rate_s)
        try:
            resp = requests.request(
                method, url, headers=headers,
                json=json_body, params=params, timeout=60)
        except requests.RequestException as exc:
            if log:
                log.warning("  %s %s error: %s", method, url, exc)
            return None, last_call
        if resp.status_code != 429:
            break
        wait = _parse_retry_after(resp.headers.get("Retry-After"), 30)
        if log:
            log.info("  %s %s 429 — sleeping %ds (attempt %d)",
                      method, url, wait, attempt + 1)
        time.sleep(wait)
    return resp, last_call


def _credentials() -> tuple[Optional[str], Optional[str]]:
    load_dotenv()
    return (os.environ.get("CIN7_ACCOUNT_ID"),
             os.environ.get("CIN7_APPLICATION_KEY"))


def _resolve_products(skus: list, headers: dict, log=None,
                       rate_s: float = DEFAULT_RATE_S,
                       last_call: float = 0.0) -> tuple[dict, float]:
    """{sku: product_dict} for every resolvable SKU."""
    resolved: dict = {}
    for sku in skus:
        if not sku:
            continue
        resp, last_call = _http(
            "GET", f"{BASE_URL}/product", headers,
            params={"Sku": sku, "Limit": 1},
            log=log, rate_s=rate_s, last_call=last_call)
        if resp is None or resp.status_code != 200:
            continue
        products = (resp.json() or {}).get("Products") or []
        if products:
            resolved[sku] = products[0]
    return resolved, last_call


def estimate_consumption(sku: str, qty: float, bom_parents: dict) -> list:
    """Our own estimate of what CIN7 will consume, from the same BOM
    data the 865FabLab planner already uses. This is a preview only --
    CIN7 computes the actual consumption from its own linked BOM at
    push time, which is authoritative and may differ if our BOM sync
    is stale."""
    rows = []
    for comp in bom_parents.get(sku, []):
        comp_sku = str(comp.get("ComponentSKU") or "").strip()
        qty_per = comp.get("Quantity")
        try:
            qty_per = float(qty_per)
        except (TypeError, ValueError):
            qty_per = 0.0
        if not comp_sku or qty_per <= 0:
            continue
        rows.append({
            "Component": comp_sku,
            "Name": str(comp.get("ComponentName") or ""),
            "Qty per unit": qty_per,
            "Estimated consumption": round(qty * qty_per, 3),
        })
    return rows


# ---------------------------------------------------------------------------
# Push
# ---------------------------------------------------------------------------

def push_finished_goods(
        draft_id: int, bom_parents: dict, *,
        actor: str,
        apply: bool = False,
        location: str = "Main Warehouse",
        wip_account: str = DEFAULT_WIP_ACCOUNT,
        account: str = DEFAULT_ACCOUNT,
        rate_s: float = DEFAULT_RATE_S,
) -> list:
    """Push every finished-SKU line on a received 865FabLab order to
    CIN7 as a Finished Goods task (one per SKU). Returns a list of
    FinishedGoodsResult, one per line. Set apply=False for a dry-run:
    validates SKUs resolve in CIN7 and shows the estimated consumption,
    without POSTing (CIN7 has no dry-run for this endpoint, so this is
    OUR estimate, not a guarantee of what CIN7 will actually do)."""
    stamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    log = _setup_log(stamp)

    draft = db.get_po_draft(draft_id)
    if not draft:
        return [FinishedGoodsResult(
            sku="", ok=False, errors=[f"Order #{draft_id} not found"])]
    if draft["status"] not in ("submitted", "finalized"):
        return [FinishedGoodsResult(
            sku="", ok=False,
            errors=[f"Order is in status '{draft['status']}'; only a "
                    "submitted (placed) order can be pushed."])]

    finished_lines = db.get_po_draft_lines(draft_id)
    if not finished_lines:
        return [FinishedGoodsResult(
            sku="", ok=False, errors=[f"Order #{draft_id} has no lines."])]

    account_id, app_key = _credentials()
    if not account_id or not app_key:
        return [FinishedGoodsResult(
            sku=sku, ok=False,
            errors=["CIN7_ACCOUNT_ID / CIN7_APPLICATION_KEY missing in "
                    ".env"])
            for sku in finished_lines]
    headers = {
        "api-auth-accountid": account_id,
        "api-auth-applicationkey": app_key,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    skus = [s for s, q in finished_lines.items() if float(q or 0) > 0]
    resolved, last_call = _resolve_products(
        skus, headers, log=log, rate_s=rate_s)

    # Per-SKU idempotency: skip (as an error, so the UI surfaces it)
    # any SKU that already has a successful push recorded for this
    # order.
    already_pushed = {
        r["sku"] for r in db.list_fablab_finished_goods_pushes(draft_id)
        if r["status"] == "pushed"
    }

    results: list = []
    now_iso = datetime.now().isoformat()

    for sku in skus:
        qty = float(finished_lines[sku])
        result = FinishedGoodsResult(sku=sku, ok=False, quantity=qty)
        result.estimated_consumption = estimate_consumption(
            sku, qty, bom_parents)

        if sku in already_pushed:
            result.errors.append(
                f"{sku} already has a pushed Finished Goods task for "
                f"this order. Refusing to push again -- void in CIN7 "
                "directly if this was wrong.")
            results.append(result)
            continue

        prod = resolved.get(sku)
        if not prod:
            result.errors.append(f"{sku}: not found in CIN7.")
            results.append(result)
            continue

        body = {
            "Status": "COMPLETED",
            "ProductID": prod.get("ID"),
            "Location": location,
            "WIPAccount": wip_account,
            "WIPDate": now_iso,
            "Account": account,
            "Quantity": qty,
            "CompletionDate": now_iso,
            "Notes": f"865FabLab order #{draft_id} receiving",
        }

        if not apply:
            log.info("DRY-RUN — not posting %s to CIN7.", sku)
            result.ok = True
            result.stage = "dry_run"
            result.response = {"_dry_run": True, "body": body}
            results.append(result)
            continue

        log.info("POST /finishedGoods for %s x%g ...", sku, qty)
        resp, last_call = _http(
            "POST", f"{BASE_URL}/finishedGoods", headers,
            json_body=body, log=log, rate_s=rate_s, last_call=last_call)
        if resp is None:
            result.errors.append(f"{sku}: network error posting to CIN7")
            db.record_fablab_finished_goods_push(
                draft_id, sku, qty, status="failed", cin7_task_id=None,
                cin7_assembly_number=None, response={"body": body},
                actor=actor)
            results.append(result)
            continue
        if resp.status_code != 200:
            result.errors.append(
                f"{sku}: Finished Goods POST failed ({resp.status_code}): "
                f"{resp.text[:500]}")
            db.record_fablab_finished_goods_push(
                draft_id, sku, qty, status="failed", cin7_task_id=None,
                cin7_assembly_number=None,
                response={"body": body, "response_text": resp.text[:2000]},
                actor=actor)
            results.append(result)
            continue

        data = resp.json() or {}
        api_errors = data.get("Errors") or []
        result.response = data
        result.cin7_task_id = data.get("TaskID")
        result.cin7_assembly_number = data.get("AssemblyNumber")
        if api_errors:
            # Task was created but CIN7 reported partial issues --
            # still record it (with what CIN7 gave us) so it's not
            # silently lost, but don't mark it a clean success.
            result.warnings.extend(str(e) for e in api_errors)
            result.errors.append(
                f"{sku}: task {result.cin7_assembly_number} created but "
                f"CIN7 reported issues -- check it in CIN7: "
                f"{'; '.join(str(e) for e in api_errors)}")
            db.record_fablab_finished_goods_push(
                draft_id, sku, qty, status="failed",
                cin7_task_id=result.cin7_task_id,
                cin7_assembly_number=result.cin7_assembly_number,
                response=data, actor=actor)
            results.append(result)
            continue

        result.ok = True
        result.stage = "posted"
        db.record_fablab_finished_goods_push(
            draft_id, sku, qty, status="pushed",
            cin7_task_id=result.cin7_task_id,
            cin7_assembly_number=result.cin7_assembly_number,
            response=data, actor=actor)
        log.info("✓ %s posted. TaskID=%s AssemblyNumber=%s",
                 sku, result.cin7_task_id, result.cin7_assembly_number)
        results.append(result)

    return results
