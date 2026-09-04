"""fablab_assemblies.py — 865FabLab toll-manufacturing flow (2026-09-04)

James's design (thread #cin7-sync-improvements 2026-09-04):

  1. PLACE ORDER (app, "Plan & order" section)
     For every finished corner SKU on the 865FabLab order, create ONE
     CIN7 Finished Goods (assembly) task in AUTHORISED status. CIN7
     fills the component order lines from the product's BOM — that is
     the picker's pick list, and components move to WIP. Then raise ONE
     labor-only Draft PO to supplier 865FabLab: a single line
     `OSC-865FABLAB-LABOR` × total units (price = its 865FabLab Fixed
     Price), Memo = assembly numbers + end SKUs + pick list.
  2. APPROVAL = the buyer authorises that PO in CIN7. The worker polls
     (check-po) and, once the PO is no longer DRAFT, posts one Slack
     message PER ASSEMBLY to the 865 corner-manufacture channel and
     creates the Odoo CRM lead + quote (fablab_odoo.py, env-gated).
  3. RECEIVING = anyone replies in an assembly's Slack thread:
        done              → complete the whole assembly
        done 35           → partial: complete 35, new AUTHORISED task
                            for the remainder
        LED-G2000820-0609 = 0   (extra lines) → actual component
                            quantity used instead of the BOM default
     The worker (check-replies) completes the CIN7 task with the actual
     pick lines, stock updates, and BOM-vs-actual is logged in
     fablab_pick_variance so the BOM can be corrected over time.
     The app's receiving section does the same via a form.

CIN7 endpoints (dearinventory.apib § Finished Goods):
  POST /finishedGoods            Status=AUTHORISED → task + BOM order lines
  GET  /finishedGoods?TaskID=    task incl. OrderLines/PickLines
  PUT  /finishedGoods            Quantity change while AUTHORISED
  POST /finishedGoods/pick       Status=COMPLETED + explicit PickLines
  DELETE /finishedGoods?ID=&Void=true

CLI:
  python fablab_assemblies.py place --draft 12 [--apply]
  python fablab_assemblies.py check-po        (worker, every 5 min)
  python fablab_assemblies.py check-replies   (worker, every 5 min)
  python fablab_assemblies.py complete --assembly 3 [--qty 35] [--apply]
"""

from __future__ import annotations

import math
import argparse
import logging
import os
import re
import sys
from datetime import datetime
from typing import Optional

import db
import fablab_slack
from cin7_post_finishedgoods import (
    BASE_URL, DEFAULT_ACCOUNT, DEFAULT_RATE_S, DEFAULT_WIP_ACCOUNT,
    _credentials, _http,
)
# cin7_post_po's resolver passes IncludeSuppliers=true, which we need
# for the 865FabLab Fixed Price on the labor SKU.
from cin7_post_po import _resolve_products

log = logging.getLogger("fablab_assemblies")

FABLAB_SUPPLIER = "865FabLab"
LABOR_SKU = "OSC-865FABLAB-LABOR"
DEFAULT_LABOR_PRICE = 10.0
CORNER_CHANNEL_ID = os.environ.get(
    "SLACK_FABLAB_CORNER_CHANNEL_ID", fablab_slack.FABLAB_CHANNEL_ID)

_DONE_RE = re.compile(r"^\s*(?:done|received|complete[d]?)\b\s*(\d+(?:\.\d+)?)?",
                      re.IGNORECASE)
_OVERRIDE_RE = re.compile(
    r"^\s*([A-Za-z0-9][A-Za-z0-9._\-/]+)\s*(?:=|:|used|x)\s*(\d+(?:\.\d+)?)\s*$",
    re.IGNORECASE)


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _headers() -> Optional[dict]:
    account_id, app_key = _credentials()
    if not account_id or not app_key:
        return None
    return {
        "api-auth-accountid": account_id,
        "api-auth-applicationkey": app_key,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


def _now_iso() -> str:
    return datetime.now().strftime("%Y-%m-%dT%H:%M:%S")


def _num(v) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def _name_of(product_map: dict, sku: str) -> str:
    return str((product_map or {}).get(sku, {}).get("Name") or "")


def build_pick_list(lines: dict, bom_parents: dict,
                    product_map: Optional[dict] = None) -> tuple[list, dict]:
    """lines {sku: qty}. Returns (per_sku, totals):
       per_sku: [{sku, name, qty, components: [(comp_sku, comp_name, total)]}]
       totals:  {comp_sku: total}   (labor SKU excluded from both)"""
    per_sku, totals = [], {}
    for sku, qty in lines.items():
        qty = _num(qty)
        if qty <= 0:
            continue
        comps = []
        for comp in bom_parents.get(sku, []):
            csku = str(comp.get("ComponentSKU") or "").strip()
            per = _num(comp.get("Quantity"))
            if not csku or per <= 0 or csku.upper() == LABOR_SKU:
                continue
            total = round(qty * per, 3)
            comps.append((csku, str(comp.get("ComponentName") or
                                    _name_of(product_map, csku)), total))
            totals[csku] = round(totals.get(csku, 0.0) + total, 3)
        per_sku.append({"sku": sku, "name": _name_of(product_map, sku),
                        "qty": qty, "components": comps})
    return per_sku, totals


def _locator_of(product_map: dict, sku: str) -> str:
    row = (product_map or {}).get(sku, {}) or {}
    for key in ("StockLocator", "Stock Locator", "Stock locator",
                "stock_locator"):
        val = row.get(key)
        if val is not None and str(val).strip().lower() not in (
                "", "nan", "none", "null"):
            return str(val).strip()
    return ""


def component_notes(totals: dict, bom_parents: dict, product_map: dict,
                    stock_map: Optional[dict] = None) -> dict:
    """Per-component picker hints for the TOTALS block:
    stock locator + on-hand, and — when the component is itself an
    auto-assembly with too little on hand — the master item to cut it
    from (e.g. 609 mm channel cut 0.333 x from the 2 m length)."""
    notes: dict = {}
    for csku, total in (totals or {}).items():
        bits = []
        loc = _locator_of(product_map, csku)
        if loc:
            bits.append(f"loc {loc}")
        stk = (stock_map or {}).get(csku)
        on_hand = None
        if stk is not None:
            on_hand = _num(stk.get("OnHand", stk.get("Available", 0)))
            bits.append(f"on hand {on_hand:g}")
        prow = (product_map or {}).get(csku, {}) or {}
        auto = str(prow.get("AutoAssembly", "")).strip().lower() == "true"
        need = math.ceil(round(_num(total), 3))
        short = (on_hand is None and auto) or (
            on_hand is not None and on_hand < need)
        if auto and short:
            for comp in bom_parents.get(csku, []) or []:
                msku = str(comp.get("ComponentSKU") or "").strip()
                per = _num(comp.get("Quantity"))
                if not msku or per <= 0 or msku.upper() == LABOR_SKU:
                    continue
                gap = need if on_hand is None else max(need - on_hand, 0)
                mloc = _locator_of(product_map, msku)
                hint = (f"SHORT: cut {gap:g} from master {msku} "
                        f"({per:g} each = {math.ceil(gap * per - 1e-9):g} "
                        f"length(s))")
                if mloc:
                    hint += f" loc {mloc}"
                bits.append(hint)
        if bits:
            notes[csku] = " | ".join(bits)
    return notes


def format_pick_list(per_sku: list, totals: dict,
                     assembly_numbers: Optional[dict] = None,
                     header: str = "", notes: Optional[dict] = None) -> str:
    out = [header] if header else []
    for row in per_sku:
        tag = ""
        if assembly_numbers and assembly_numbers.get(row["sku"]):
            tag = f"[{assembly_numbers[row['sku']]}] "
        out.append(f"{tag}{row['sku']} x {row['qty']:g}"
                   + (f" - {row['name']}" if row["name"] else ""))
        for csku, cname, total in row["components"]:
            out.append(f"    - {csku} x {total:g}"
                       + (f" ({cname})" if cname else ""))
    if totals:
        out.append("")
        out.append("TOTALS TO PICK (rounded up to whole lengths):")
        for csku, total in sorted(totals.items()):
            whole = int(math.ceil(round(total, 3)))
            exact = f"  (BOM {total:g})" if abs(whole - total) > 1e-9 else ""
            note = f"  [{notes[csku]}]" if notes and notes.get(csku) else ""
            out.append(f"  {csku} x {whole}{exact}{note}")
    return "\n".join(out)


def _post_finished_goods(headers: dict, *, product_id: str, qty: float,
                         status: str, notes: str, location: str,
                         log_=None, last_call: float = 0.0):
    body = {
        "Status": status,
        "ProductID": product_id,
        "Location": location,
        "WIPAccount": DEFAULT_WIP_ACCOUNT,
        "WIPDate": _now_iso(),
        "Account": DEFAULT_ACCOUNT,
        "Quantity": float(qty),
        "Notes": notes,
    }
    if status == "COMPLETED":
        body["CompletionDate"] = _now_iso()
    resp, last_call = _http("POST", f"{BASE_URL}/finishedGoods", headers,
                            json_body=body, log=log_, rate_s=DEFAULT_RATE_S,
                            last_call=last_call)
    return body, resp, last_call


# ---------------------------------------------------------------------------
# 1. place order
# ---------------------------------------------------------------------------

def place_order(draft_id: int, bom_parents: dict, product_map: dict, *,
                actor: str, apply: bool = False,
                location: str = "Main Warehouse",
                stock_map: Optional[dict] = None) -> dict:
    """Create AUTHORISED assemblies for every order line, then the
    labor-only Draft PO. Idempotent per SKU (skips SKUs that already
    have a live assembly on this order) and per PO (push_po_draft's own
    guard). Returns a dict the UI renders."""
    import cin7_post_po

    res: dict = {"ok": False, "assemblies": [], "errors": [], "warnings": [],
                 "po_number": None, "memo": "", "dry_run": not apply}
    draft = db.get_po_draft(draft_id)
    if not draft:
        res["errors"].append(f"Order #{draft_id} not found.")
        return res
    if draft["status"] != "editing":
        res["errors"].append(
            f"Order is '{draft['status']}' — only an order still being "
            "edited can be placed.")
        return res
    lines = {s: _num(q) for s, q in db.get_po_draft_lines(draft_id).items()
             if _num(q) > 0}
    if not lines:
        res["errors"].append("Order has no quantities saved.")
        return res
    headers = _headers()
    if not headers:
        res["errors"].append("CIN7 credentials missing.")
        return res

    existing = {r["sku"]: r for r in db.list_fablab_assemblies(draft_id)
                if r["status"] in ("authorised", "completed")}
    resolved, last_call = _resolve_products(
        list(lines) + [LABOR_SKU], headers, log=log)
    missing = [s for s in lines if s not in resolved]
    if missing:
        res["errors"].append(f"Not found in CIN7: {', '.join(missing)}")
        return res
    labor = resolved.get(LABOR_SKU)
    if not labor:
        res["errors"].append(f"Labor SKU {LABOR_SKU} not found in CIN7.")
        return res

    # Labor price: 865FabLab Fixed Price on the labor SKU.
    price = None
    for s in labor.get("Suppliers") or []:
        if str(s.get("SupplierName") or "").lower() == FABLAB_SUPPLIER.lower():
            price = _num(s.get("FixedCost")) or _num(s.get("Cost")) or None
    if price is None:
        price = _num(db.fablab_setting_get("labor_unit_price")) or DEFAULT_LABOR_PRICE
        res["warnings"].append(
            f"No 865FabLab Fixed Price on {LABOR_SKU}; using ${price:.2f}.")

    # ---- assemblies
    assembly_numbers: dict = {}
    for sku, qty in lines.items():
        if sku in existing:
            row = existing[sku]
            assembly_numbers[sku] = row["assembly_number"]
            res["assemblies"].append({
                "sku": sku, "qty": row["quantity"], "status": row["status"],
                "assembly_number": row["assembly_number"], "reused": True})
            continue
        if not apply:
            res["assemblies"].append({"sku": sku, "qty": qty,
                                      "status": "dry_run",
                                      "assembly_number": None})
            continue
        body, resp, last_call = _post_finished_goods(
            headers, product_id=resolved[sku]["ID"], qty=qty,
            status="AUTHORISED",
            notes=f"865FabLab order #{draft_id} — {draft['name']}",
            location=location, log_=log, last_call=last_call)
        if resp is None or resp.status_code != 200:
            err = (f"{sku}: assembly POST failed "
                   f"({resp.status_code if resp is not None else 'network'}): "
                   f"{(resp.text[:300] if resp is not None else '')}")
            res["errors"].append(err)
            db.create_fablab_assembly(
                draft_id, sku, qty, status="failed", cin7_task_id=None,
                assembly_number=None, response={"body": body, "error": err},
                actor=actor)
            continue
        data = resp.json() or {}
        api_errors = data.get("Errors") or []
        if api_errors:
            res["warnings"].append(
                f"{sku}: {data.get('AssemblyNumber')} created with CIN7 "
                f"warnings: {'; '.join(map(str, api_errors))}")
        db.create_fablab_assembly(
            draft_id, sku, qty, status="authorised",
            cin7_task_id=data.get("TaskID"),
            assembly_number=data.get("AssemblyNumber"),
            response=data, actor=actor)
        assembly_numbers[sku] = data.get("AssemblyNumber")
        res["assemblies"].append({
            "sku": sku, "qty": qty, "status": "authorised",
            "assembly_number": data.get("AssemblyNumber")})

    if res["errors"]:
        res["warnings"].append(
            "Some assemblies failed — labor PO NOT raised. Fix and place "
            "again; existing assemblies are reused.")
        return res

    # ---- labor PO
    total_units = sum(lines.values())
    per_sku, totals = build_pick_list(lines, bom_parents, product_map)
    memo = format_pick_list(
        per_sku, totals, assembly_numbers,
        notes=component_notes(totals, bom_parents, product_map, stock_map),
        header=(f"865FabLab corner assembly — order #{draft_id} "
                f"{draft['name']} — {total_units:g} units. "
                f"Assemblies are AUTHORISED in CIN7 (pick lists there)."))
    res["memo"] = memo
    line = {
        "ProductID": labor.get("ID"),
        "SKU": labor.get("SKU"),
        "Name": labor.get("Name"),
        "Quantity": float(total_units),
        "Price": float(price),
        "Discount": 0,
        "Tax": 0,
        "Total": round(total_units * price, 2),
    }
    push = cin7_post_po.push_po_draft(
        draft_id, actor=actor, apply=apply, require_mov=False,
        default_location=location, lines_override=[line], memo=memo)
    res["po_lines"] = [line]
    res["labor_price"] = price
    res["warnings"].extend(push.warnings)
    if not push.ok:
        res["errors"].extend(push.errors or ["Labor PO push failed."])
        return res
    res["po_number"] = push.cin7_po_number
    res["po_id"] = push.cin7_po_id
    res["ok"] = True
    return res


# ---------------------------------------------------------------------------
# 2. PO authorised → Slack per assembly + Odoo
# ---------------------------------------------------------------------------

def _get_task(headers: dict, task_id: str, last_call: float = 0.0):
    resp, last_call = _http("GET", f"{BASE_URL}/finishedGoods", headers,
                            params={"TaskID": task_id}, log=log,
                            rate_s=DEFAULT_RATE_S, last_call=last_call)
    if resp is None or resp.status_code != 200:
        return None, last_call
    return resp.json() or {}, last_call


def _task_components(task: dict) -> list:
    """[(code, name, per_unit, total)] from a task's OrderLines, labor
    SKU excluded. per_unit = TotalQuantity / task Quantity."""
    qty = _num(task.get("Quantity")) or 1.0
    out = []
    for ol in task.get("OrderLines") or []:
        code = str(ol.get("ProductCode") or "")
        if code.upper() == LABOR_SKU:
            continue
        total = _num(ol.get("TotalQuantity")) or _num(ol.get("Quantity"))
        out.append((code, str(ol.get("Name") or ""), total / qty, total,
                    ol.get("ProductID")))
    return out


def _assembly_message(a, task: dict, po_number: str) -> str:
    comps = _task_components(task)
    lines = [
        f":hammer_and_wrench: *{a['assembly_number']}* · `{a['sku']}` × "
        f"*{_num(a['quantity']):g}* — {task.get('ProductName') or ''}",
        f"Order #{a['draft_id']} · labor PO {po_number} · status AUTHORISED",
        "*Pick list (BOM):*",
    ]
    for code, name, _per, total, _pid in comps:
        lines.append(f"• `{code}` × {total:g}" + (f" — {name}" if name else ""))
    lines.append(
        "_When the batch is back, reply here `done` (or `done 35` for a "
        "partial). Used less material than the BOM? Add lines like "
        "`LED-G2000820-0609 = 0`._")
    return "\n".join(lines)


def check_po_authorised(apply: bool = True) -> dict:
    """Worker poll: for placed 865FabLab orders whose labor PO has left
    DRAFT in CIN7, post per-assembly Slack messages + Odoo lead/quote
    once (fablab_po_notifications)."""
    stats = {"checked": 0, "notified": 0, "errors": []}
    headers = _headers()
    if not headers:
        return stats
    drafts = [d for d in db.list_po_drafts(supplier=FABLAB_SUPPLIER,
                                           include_archived=True)
              if d["status"] in ("submitted", "finalized") and d["cin7_po_id"]]
    last_call = 0.0
    for d in drafts:
        if db.fablab_po_notification_get(d["id"]):
            continue
        assemblies = db.list_fablab_assemblies(d["id"], status="authorised")
        if not assemblies:
            continue  # legacy order (no assemblies) — nothing to announce
        stats["checked"] += 1
        # NB: /purchase returns 400 for Advanced Purchase tasks — must use
        # /advanced-purchase (same Order.Status / top-level Status shape).
        resp, last_call = _http("GET", f"{BASE_URL}/advanced-purchase", headers,
                                params={"ID": d["cin7_po_id"]}, log=log,
                                rate_s=DEFAULT_RATE_S, last_call=last_call)
        if resp is None or resp.status_code != 200:
            stats["errors"].append(f"PO lookup failed for order #{d['id']}")
            continue
        po = resp.json() or {}
        order = po.get("Order") if isinstance(po.get("Order"), dict) else {}
        # Order.Status is the PO's own DRAFT/AUTHORISED; the top-level
        # Status is the overall task (DRAFT/ORDERED/RECEIVED/...).
        status = str(order.get("Status") or po.get("Status") or "").upper()
        if status == "DRAFT" and str(po.get("Status") or "").upper() == "VOIDED":
            status = "VOIDED"
        po_number = po.get("OrderNumber") or d["cin7_po_number"] or "?"
        if status in ("", "DRAFT"):
            continue
        if status == "VOIDED":
            db.fablab_po_notification_upsert(
                d["id"], cin7_po_number=po_number, odoo_error="PO voided")
            continue
        if not apply:
            log.info("[DRY] would notify for order #%s (%s %s)",
                     d["id"], po_number, status)
            stats["notified"] += 1
            continue

        # Header message, then one message per assembly (its own thread).
        total = sum(_num(a["quantity"]) for a in assemblies)
        header = (f":factory: *865FabLab corner order #{d['id']} — "
                  f"{d['name']}* · labor PO *{po_number}* authorised · "
                  f"{total:g} units across {len(assemblies)} assembl"
                  f"{'y' if len(assemblies) == 1 else 'ies'}. One message "
                  "per assembly follows — talk about each in its thread.")
        hts, err = fablab_slack.post(header, channel_id=CORNER_CHANNEL_ID)
        if err:
            stats["errors"].append(f"Slack header failed: {err}")
            continue
        desc_lines = []
        for a in assemblies:
            task, last_call = _get_task(headers, a["cin7_task_id"], last_call)
            task = task or {"ProductName": "", "OrderLines": [],
                            "Quantity": a["quantity"]}
            ts, err = fablab_slack.post(_assembly_message(a, task, po_number),
                                        channel_id=CORNER_CHANNEL_ID)
            if ts:
                db.set_fablab_assembly_slack(a["id"], CORNER_CHANNEL_ID, ts)
            else:
                stats["errors"].append(
                    f"Slack post failed for {a['assembly_number']}: {err}")
            desc_lines.append(
                f"<b>{a['assembly_number']}</b> {a['sku']} × "
                f"{_num(a['quantity']):g} — {task.get('ProductName') or ''}")
            for code, name, _per, tot, _pid in _task_components(task):
                desc_lines.append(f"&nbsp;&nbsp;• {code} × {tot:g} {name}")
        db.fablab_po_notification_upsert(
            d["id"], cin7_po_number=po_number,
            slack_channel=CORNER_CHANNEL_ID, slack_ts=hts)
        stats["notified"] += 1

        # Odoo (env-gated)
        try:
            import fablab_odoo
            if fablab_odoo.is_configured():
                client = fablab_odoo.OdooClient()
                labor_price = None
                for ln in order.get("Lines") or []:
                    if str(ln.get("SKU") or "").upper() == LABOR_SKU:
                        labor_price = _num(ln.get("Price")) or None
                info = client.create_lead_and_quote(
                    po_number=po_number, total_qty=total,
                    description_html="<br/>".join(desc_lines),
                    unit_price=labor_price)
                db.fablab_po_notification_upsert(
                    d["id"], odoo_lead_id=info["lead_id"],
                    odoo_quote_id=info["quote_id"],
                    odoo_quote_name=info["quote_name"])
                fablab_slack.post(
                    f":white_check_mark: Odoo: lead + quote *{info['quote_name']}* "
                    f"created for {po_number}.",
                    channel_id=CORNER_CHANNEL_ID, thread_ts=hts)
            else:
                db.fablab_po_notification_upsert(
                    d["id"], odoo_error="ODOO_API_KEY not configured")
        except Exception as exc:  # noqa: BLE001
            log.exception("Odoo step failed for order #%s", d["id"])
            db.fablab_po_notification_upsert(d["id"], odoo_error=str(exc)[:500])
            fablab_slack.post(
                f":warning: Odoo lead/quote for {po_number} failed: "
                f"{str(exc)[:300]}", channel_id=CORNER_CHANNEL_ID,
                thread_ts=hts)
    return stats


# ---------------------------------------------------------------------------
# 3. receiving → complete assembly with actual pick lines
# ---------------------------------------------------------------------------

def parse_reply(text: str) -> Optional[dict]:
    """'done', 'done 35', plus optional 'SKU = qty' lines.
    Returns {'qty': float|None, 'overrides': {SKU: qty}} or None."""
    lines = [ln.strip() for ln in (text or "").splitlines() if ln.strip()]
    if not lines:
        return None
    m = _DONE_RE.match(lines[0])
    if not m:
        return None
    qty = float(m.group(1)) if m.group(1) else None
    overrides = {}
    for ln in lines[1:]:
        om = _OVERRIDE_RE.match(ln)
        if om:
            overrides[om.group(1).upper()] = float(om.group(2))
    return {"qty": qty, "overrides": overrides}


def complete_assembly(assembly_id: int, *, qty_received: Optional[float] = None,
                      pick_overrides: Optional[dict] = None, actor: str,
                      apply: bool = True) -> dict:
    """Complete an AUTHORISED assembly in CIN7 with the ACTUAL component
    quantities (BOM default unless overridden). Partial receipt → the
    task quantity is reduced first and a new AUTHORISED task is created
    for the remainder. Records BOM-vs-actual in fablab_pick_variance."""
    res: dict = {"ok": False, "errors": [], "warnings": [], "picks": [],
                 "remainder": None}
    a = db.get_fablab_assembly(assembly_id)
    if not a:
        res["errors"].append(f"Assembly #{assembly_id} not found.")
        return res
    if a["status"] != "authorised":
        res["errors"].append(f"{a['assembly_number']} is '{a['status']}'.")
        return res
    headers = _headers()
    if not headers:
        res["errors"].append("CIN7 credentials missing.")
        return res
    task, last_call = _get_task(headers, a["cin7_task_id"])
    if not task:
        res["errors"].append("Could not load the task from CIN7.")
        return res
    cin7_status = str(task.get("Status") or "").upper()
    if cin7_status == "COMPLETED":
        db.mark_fablab_assembly_completed(assembly_id, _num(task.get("Quantity")),
                                          actor, response=task)
        res["warnings"].append("Already completed in CIN7 — synced locally.")
        res["ok"] = True
        return res
    if cin7_status == "VOIDED":
        db.set_fablab_assembly_status(assembly_id, "voided", actor)
        res["errors"].append("Task was voided in CIN7.")
        return res

    planned = _num(a["quantity"])
    qty = planned if qty_received is None else float(qty_received)
    if qty <= 0 or qty > planned + 1e-9:
        res["errors"].append(
            f"Quantity {qty:g} must be between 1 and {planned:g}.")
        return res
    partial = qty < planned - 1e-9
    overrides = {k.upper(): v for k, v in (pick_overrides or {}).items()}

    comps = _task_components(task)
    pick_lines, variance = [], []
    for code, _name, per, _tot, pid in comps:
        bom_q = round(per * qty, 4)
        actual = overrides.get(code.upper(), bom_q)
        variance.append((code, bom_q, actual))
        res["picks"].append({"component": code, "bom_qty": bom_q,
                             "actual_qty": actual})
        if actual > 0:
            pick_lines.append({"ProductID": pid, "ProductCode": code,
                               "Quantity": float(actual)})
    # Labor component (non-inventory) — keep CIN7's own line untouched
    for ol in task.get("OrderLines") or []:
        if str(ol.get("ProductCode") or "").upper() == LABOR_SKU:
            lq = (_num(ol.get("TotalQuantity")) or _num(ol.get("Quantity")))
            lq = lq / (_num(task.get("Quantity")) or 1.0) * qty
            pick_lines.append({"ProductID": ol.get("ProductID"),
                               "ProductCode": ol.get("ProductCode"),
                               "Quantity": float(round(lq, 4))})
    unknown = [k for k in overrides if k not in {c[0].upper() for c in comps}]
    if unknown:
        res["warnings"].append(
            f"Ignored overrides for SKUs not in this BOM: {', '.join(unknown)}")

    if not apply:
        res["ok"] = True
        res["dry_run"] = True
        res["remainder"] = planned - qty if partial else None
        return res

    if partial:
        put_body = {"ID": a["cin7_task_id"], "Quantity": float(qty),
                    "Notes": task.get("Notes") or ""}
        resp, last_call = _http("PUT", f"{BASE_URL}/finishedGoods", headers,
                                json_body=put_body, log=log,
                                rate_s=DEFAULT_RATE_S, last_call=last_call)
        if resp is None or resp.status_code != 200:
            res["errors"].append(
                "Could not reduce the task quantity for a partial receipt: "
                f"{resp.text[:300] if resp is not None else 'network'}")
            return res

    body = {
        "TaskID": a["cin7_task_id"],
        "Status": "COMPLETED",
        "WIPAccount": task.get("WIPAccount") or DEFAULT_WIP_ACCOUNT,
        "WIPDate": task.get("WIPDate") or _now_iso(),
        "Account": task.get("Account") or DEFAULT_ACCOUNT,
        "CompletionDate": _now_iso(),
        "PickLines": pick_lines,
    }
    resp, last_call = _http("POST", f"{BASE_URL}/finishedGoods/pick", headers,
                            json_body=body, log=log, rate_s=DEFAULT_RATE_S,
                            last_call=last_call)
    if resp is None or resp.status_code != 200:
        res["errors"].append(
            f"Complete failed: {resp.text[:400] if resp is not None else 'network'}")
        return res
    data = resp.json() or {}
    if data.get("Errors"):
        res["warnings"].append("CIN7 reported: " + "; ".join(map(str, data["Errors"])))
    db.mark_fablab_assembly_completed(assembly_id, qty, actor, response=data)
    db.record_fablab_pick_variance(assembly_id, a["draft_id"], a["sku"], qty,
                                   variance, actor)
    res["ok"] = True

    if partial:
        remainder = round(planned - qty, 4)
        rbody, rresp, last_call = _post_finished_goods(
            headers, product_id=task.get("ProductID"), qty=remainder,
            status="AUTHORISED",
            notes=f"865FabLab order #{a['draft_id']} — remainder of "
                  f"{a['assembly_number']}",
            location=task.get("Location") or "Main Warehouse", log_=log,
            last_call=last_call)
        if rresp is not None and rresp.status_code == 200:
            rdata = rresp.json() or {}
            new_id = db.create_fablab_assembly(
                a["draft_id"], a["sku"], remainder, status="authorised",
                cin7_task_id=rdata.get("TaskID"),
                assembly_number=rdata.get("AssemblyNumber"), response=rdata,
                actor=actor, parent_task_id=a["cin7_task_id"])
            res["remainder"] = {"id": new_id, "qty": remainder,
                                "assembly_number": rdata.get("AssemblyNumber")}
        else:
            res["warnings"].append(
                f"Completed {qty:g} but could not create the remainder task "
                f"for {remainder:g} — create it in CIN7 manually.")
    return res


def _announce_remainder(rem: dict, parent, headers, po_number: str) -> None:
    """Post the remainder assembly as its own Slack message."""
    a = db.get_fablab_assembly(rem["id"])
    task, _ = _get_task(headers, a["cin7_task_id"])
    task = task or {"ProductName": "", "OrderLines": [], "Quantity": a["quantity"]}
    ts, _err = fablab_slack.post(
        _assembly_message(a, task, po_number)
        + f"\n_(remainder of {parent['assembly_number']})_",
        channel_id=parent["slack_channel"] or CORNER_CHANNEL_ID)
    if ts:
        db.set_fablab_assembly_slack(a["id"], parent["slack_channel"]
                                     or CORNER_CHANNEL_ID, ts)


def check_replies(apply: bool = True) -> dict:
    """Worker poll: complete assemblies from `done` replies in their
    Slack threads."""
    stats = {"checked": 0, "completed": 0, "errors": []}
    token = os.environ.get("SLACK_BOT_TOKEN", "").strip()
    if not token:
        return stats
    open_rows = [a for a in db.list_fablab_assemblies(status="authorised")
                 if a["slack_ts"]]
    if not open_rows:
        return stats
    import slack_sync
    session = slack_sync._build_session(token)
    headers = _headers()
    for a in open_rows:
        stats["checked"] += 1
        try:
            body = slack_sync._slack_get(
                session, "conversations.replies",
                {"channel": a["slack_channel"], "ts": a["slack_ts"],
                 "oldest": a["last_reply_ts"] or a["slack_ts"],
                 "inclusive": "false", "limit": 50})
        except Exception as exc:  # noqa: BLE001
            stats["errors"].append(f"{a['assembly_number']}: {exc}")
            continue
        msgs = [m for m in (body.get("messages") or [])
                if m.get("ts") != a["slack_ts"]
                and float(m.get("ts", 0)) > float(a["last_reply_ts"] or 0)]
        if not msgs:
            continue
        newest = max(m["ts"] for m in msgs)
        for m in sorted(msgs, key=lambda x: float(x["ts"])):
            if m.get("bot_id") or m.get("subtype") == "bot_message":
                continue
            parsed = parse_reply(m.get("text") or "")
            if not parsed:
                continue
            user_id = m.get("user") or ""
            try:
                who = slack_sync._resolve_user(session, user_id) or user_id
            except Exception:  # noqa: BLE001
                who = user_id
            if not apply:
                log.info("[DRY] would complete %s: %s", a["assembly_number"], parsed)
                continue
            r = complete_assembly(a["id"], qty_received=parsed["qty"],
                                  pick_overrides=parsed["overrides"],
                                  actor=f"slack:{who}", apply=True)
            if r["ok"]:
                done_qty = parsed["qty"] if parsed["qty"] is not None else _num(a["quantity"])
                picks = ", ".join(
                    f"{p['component']} {p['actual_qty']:g}"
                    + (f" (BOM {p['bom_qty']:g})" if abs(p['actual_qty'] - p['bom_qty']) > 1e-9 else "")
                    for p in r["picks"])
                txt = (f":white_check_mark: *{a['assembly_number']}* completed by "
                       f"{who} — {done_qty:g} × `{a['sku']}` into stock. "
                       f"Components consumed: {picks or 'none'}.")
                if r.get("remainder"):
                    txt += (f"\nRemainder {r['remainder']['qty']:g} → new assembly "
                            f"*{r['remainder']['assembly_number']}* (own message below).")
                if r["warnings"]:
                    txt += "\n:warning: " + " ".join(r["warnings"])
                fablab_slack.post(txt, channel_id=a["slack_channel"],
                                  thread_ts=a["slack_ts"])
                stats["completed"] += 1
                if r.get("remainder") and headers:
                    notif = db.fablab_po_notification_get(a["draft_id"])
                    _announce_remainder(
                        r["remainder"], a, headers,
                        (notif["cin7_po_number"] if notif else "") or "")
            else:
                fablab_slack.post(
                    f":x: Could not complete *{a['assembly_number']}*: "
                    + "; ".join(r["errors"]),
                    channel_id=a["slack_channel"], thread_ts=a["slack_ts"])
                stats["errors"].append(
                    f"{a['assembly_number']}: {'; '.join(r['errors'])}")
            break  # one action per assembly per poll
        db.set_fablab_assembly_last_reply(a["id"], newest)
    return stats


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> int:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = p.add_subparsers(dest="cmd", required=True)
    sp = sub.add_parser("place")
    sp.add_argument("--draft", type=int, required=True)
    sp.add_argument("--apply", action="store_true")
    sub.add_parser("check-po").add_argument("--dry", action="store_true")
    sub.add_parser("check-replies").add_argument("--dry", action="store_true")
    sc = sub.add_parser("complete")
    sc.add_argument("--assembly", type=int, required=True)
    sc.add_argument("--qty", type=float)
    sc.add_argument("--apply", action="store_true")
    args = p.parse_args()

    if args.cmd == "place":
        from fablab_stock_alert import _load_data
        products, _stock, _engine, bom_parents = _load_data()
        product_map = {str(r["SKU"]).strip(): r for r in
                       products.to_dict("records")} if not products.empty else {}
        out = place_order(args.draft, bom_parents, product_map, actor="cli",
                          apply=args.apply)
    elif args.cmd == "check-po":
        out = check_po_authorised(apply=not args.dry)
    elif args.cmd == "check-replies":
        out = check_replies(apply=not args.dry)
    else:
        out = complete_assembly(args.assembly, qty_received=args.qty,
                                actor="cli", apply=args.apply)
    import json
    print(json.dumps(out, indent=2, default=str))
    return 0 if not out.get("errors") else 1


if __name__ == "__main__":
    sys.exit(main())
