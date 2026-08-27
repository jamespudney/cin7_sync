"""
cin7_post_stockadjustment.py
=============================
Push a received 865FabLab order (a po_drafts row scoped to supplier
"865FabLab") into CIN7 as a real DRAFT stock adjustment: +finished SKU
quantity, -consumed raw-material quantities (computed from the BOM).

Why this exists
----------------
The 865FabLab Production page's "Receiving checklist" used to just
*display* these numbers for a human to type into CIN7 by hand. This
module does the write instead — modeled directly on cin7_post_po.py,
the only other write path to CIN7 in this codebase, reusing its
validate-then-push shape and its "never auto-authorise" safety rule.

CIN7 API flow (per dearinventory.apib § Stock Adjustment)
-----------------------------------------------------------
    POST /stockadjustment
      Body: EffectiveDate, Status="DRAFT", Reference, UpdateOnHand=true,
            Lines: [NewStockLineModel, ...]
      -> One call (unlike the PO's two-step master+lines POST — a stock
         adjustment has no separate master/lines split).

⚠ Hard rules (never relax these)
---------------------------------
* Status is ALWAYS "DRAFT". We never send "COMPLETED" — a human reviews
  and authorises the adjustment inside CIN7 before it affects
  cost-of-goods/inventory-value accounts, exactly like the PO flow.
* NewStockLineModel.Quantity is an ABSOLUTE target ("New value for
  QuantityOnHand"), NOT a signed delta (dearinventory.apib:31005).
  Every line's Quantity is computed as `live_current_onhand + delta`
  (finished SKU) or `live_current_onhand - delta` (each raw material),
  using a LIVE GET /ref/productavailability call immediately before
  building the body -- never the periodically-synced stock CSV, which
  can be hours stale. Sending a raw delta as Quantity would silently
  overwrite on-hand stock to that number.
* Refuse to build a line where the computed new quantity would go
  negative -- a real sign that either the order or current CIN7 stock
  is wrong, not something to silently clamp to zero.
* Idempotency: if a push already exists for this draft_id (see
  db.list_fablab_stock_adjustments), refuse to push again.

Usage
-----
    from cin7_post_stockadjustment import push_stock_adjustment
    result = push_stock_adjustment(
        draft_id=42, bom_parents=BOM_PARENTS, actor="james", apply=True)
    if result.ok:
        print(result.cin7_stocktake_number)
    else:
        print(result.errors)
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

# Hard whitelist for each adjustment line body -- fail loudly on an
# unexpected key rather than silently send something CIN7 might
# interpret in an unintended way (same discipline as cin7_post_po.py).
ALLOWED_LINE_KEYS = {
    "ProductID", "SKU", "ProductName", "Quantity", "UnitCost",
    "Location", "LocationID", "ReceivedDate", "Comments",
}


@dataclass
class PushResult:
    """Return value from push_stock_adjustment. Always inspect .ok
    before .cin7_task_id. stage: 'validated' / 'dry_run' / 'posted'."""
    ok: bool
    stage: str = "init"
    cin7_task_id: Optional[str] = None
    cin7_stocktake_number: Optional[str] = None
    errors: list = field(default_factory=list)
    warnings: list = field(default_factory=list)
    preview: list = field(default_factory=list)
    response: Optional[dict] = None


# ---------------------------------------------------------------------------
# HTTP helpers -- duplicated (not imported) from cin7_post_po.py on purpose.
# Both modules are small, sensitive, and independently reviewed; sharing a
# module here would mean any future edit to one risks the other's working,
# tested PO push path. See cin7_post_po.py for the identical pattern.
# ---------------------------------------------------------------------------

def _parse_retry_after(value, default: int = 30) -> int:
    if value is None:
        return default
    digits = "".join(ch for ch in str(value) if ch.isdigit())
    return int(digits) if digits else default


def _setup_log(stamp: str) -> logging.Logger:
    log = logging.getLogger("cin7_post_stockadjustment")
    log.setLevel(logging.INFO)
    if not log.handlers:
        fh = logging.FileHandler(
            OUTPUT_DIR / f"cin7_post_stockadjustment_{stamp}.log",
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


# ---------------------------------------------------------------------------
# Live lookups
# ---------------------------------------------------------------------------

def _resolve_products(skus: list, headers: dict, log=None,
                       rate_s: float = DEFAULT_RATE_S,
                       last_call: float = 0.0) -> tuple[dict, float]:
    """{sku: product_dict} for every resolvable SKU. Same shape as
    cin7_post_po._resolve_products -- gives us ProductID + AverageCost."""
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


def _live_onhand(sku: str, location: str, headers: dict, log=None,
                  rate_s: float = DEFAULT_RATE_S,
                  last_call: float = 0.0) -> tuple[Optional[float], float]:
    """Live current OnHand for one SKU at one location, fetched fresh
    right before the push -- never the periodically-synced stock CSV.
    Returns (onhand_or_None, new_last_call). None means the SKU/location
    combination wasn't found (caller should treat as a blocking error,
    not silently default to 0 -- a genuinely-zero SKU still returns a
    row with OnHand=0, per dearinventory.apib:6182)."""
    resp, last_call = _http(
        "GET", f"{BASE_URL}/ref/productavailability", headers,
        params={"Sku": sku, "Location": location, "Limit": 10},
        log=log, rate_s=rate_s, last_call=last_call)
    if resp is None or resp.status_code != 200:
        return None, last_call
    rows = (resp.json() or {}).get("ProductAvailabilityList") or []
    for row in rows:
        if str(row.get("SKU") or "").strip().lower() == sku.strip().lower():
            onhand = row.get("OnHand")
            return (float(onhand) if onhand is not None else 0.0), last_call
    return None, last_call


# ---------------------------------------------------------------------------
# Build + validate
# ---------------------------------------------------------------------------

def build_stock_adjustment_lines(
        draft_id: int, bom_parents: dict, headers: dict, *,
        location: str = "Main Warehouse", log=None,
        rate_s: float = DEFAULT_RATE_S,
) -> tuple[list, list, list]:
    """Compute the CIN7 POST lines for a received 865FabLab order.

    Returns (lines, preview_rows, errors):
      lines: [{...NewStockLineModel-shaped dict...}] ready for the POST
             body once errors is empty.
      preview_rows: [{"SKU", "Name", "Current", "Delta", "New"}] for the
             confirmation UI -- rendered BEFORE anyone can click confirm.
      errors: human-readable blocking issues (missing product, unresolved
             SKU, or a computed negative on-hand).
    """
    errors: list = []
    preview_rows: list = []
    lines: list = []

    finished_lines = db.get_po_draft_lines(draft_id)  # {sku: qty}
    if not finished_lines:
        errors.append(f"Draft #{draft_id} has no lines.")
        return lines, preview_rows, errors

    # deltas[sku] = signed change to on-hand (+ for finished goods
    # received, - for raw materials consumed to build them)
    deltas: dict[str, float] = {}
    for sku, qty in finished_lines.items():
        qty = float(qty or 0)
        if qty <= 0:
            continue
        deltas[sku] = deltas.get(sku, 0.0) + qty
        for comp in bom_parents.get(sku, []):
            comp_sku = str(comp.get("ComponentSKU") or "").strip()
            qty_per = comp.get("Quantity")
            try:
                qty_per = float(qty_per)
            except (TypeError, ValueError):
                qty_per = 0.0
            if not comp_sku or qty_per <= 0:
                continue
            deltas[comp_sku] = deltas.get(comp_sku, 0.0) - (qty * qty_per)

    if not deltas:
        errors.append("No non-zero quantities on this draft.")
        return lines, preview_rows, errors

    skus = list(deltas.keys())
    resolved, last_call = _resolve_products(
        skus, headers, log=log, rate_s=rate_s)

    for sku in skus:
        prod = resolved.get(sku)
        if not prod:
            errors.append(f"{sku}: not found in CIN7 -- can't resolve "
                           "ProductID.")
            continue
        current, last_call = _live_onhand(
            sku, location, headers, log=log, rate_s=rate_s,
            last_call=last_call)
        if current is None:
            errors.append(
                f"{sku}: no stock record at location '{location}' -- "
                "can't compute a safe new quantity.")
            continue
        delta = deltas[sku]
        new_qty = current + delta
        preview_rows.append({
            "SKU": sku,
            "Name": prod.get("Name") or "",
            "Current": current,
            "Delta": delta,
            "New": new_qty,
        })
        if new_qty < 0:
            errors.append(
                f"{sku}: current {current:g} + delta {delta:g} = "
                f"{new_qty:g} -- would go negative. Refusing to build "
                "this line; check the order quantity or CIN7's current "
                "stock before retrying.")
            continue
        line = {
            "ProductID": prod.get("ID"),
            "SKU": sku,
            "ProductName": prod.get("Name") or "",
            "Quantity": new_qty,
            "UnitCost": float(prod.get("AverageCost") or 0),
            "Location": location,
            "ReceivedDate": datetime.now().isoformat(),
            "Comments": f"865FabLab order #{draft_id} receiving adjustment",
        }
        extra = set(line) - ALLOWED_LINE_KEYS
        if extra:
            raise RuntimeError(
                f"Unexpected keys in stock adjustment line: {extra}")
        lines.append(line)

    return lines, preview_rows, errors


def validate_stock_adjustment(
        draft_id: int, bom_parents: dict, headers: dict, *,
        location: str = "Main Warehouse", log=None,
        rate_s: float = DEFAULT_RATE_S,
) -> tuple[bool, list, dict]:
    """Pre-flight check. Returns (ok, errors, context) where context
    holds 'draft', 'lines' (CIN7-ready), and 'preview' (for the UI)."""
    errors: list = []
    ctx: dict = {}

    draft = db.get_po_draft(draft_id)
    if not draft:
        return False, [f"Order #{draft_id} not found"], ctx
    ctx["draft"] = dict(draft)

    if draft["status"] not in ("submitted", "finalized"):
        errors.append(
            f"Order is in status '{draft['status']}'; only a submitted "
            "(placed) order can be pushed as a receiving adjustment.")

    existing = db.list_fablab_stock_adjustments(draft_id)
    pushed = [r for r in existing if r["status"] == "pushed"]
    if pushed:
        errors.append(
            f"Order #{draft_id} already has a pushed adjustment "
            f"(CIN7 task {pushed[0]['cin7_task_id']}). Refusing to push "
            "again -- void/adjust in CIN7 directly if this was wrong.")
        ctx["existing"] = dict(pushed[0])

    if errors:
        return False, errors, ctx

    lines, preview, build_errors = build_stock_adjustment_lines(
        draft_id, bom_parents, headers, location=location, log=log,
        rate_s=rate_s)
    ctx["lines"] = lines
    ctx["preview"] = preview
    if build_errors:
        errors.extend(build_errors)
    if not lines:
        errors.append("No valid lines to push.")

    return (len(errors) == 0), errors, ctx


# ---------------------------------------------------------------------------
# Push
# ---------------------------------------------------------------------------

def push_stock_adjustment(
        draft_id: int, bom_parents: dict, *,
        actor: str,
        apply: bool = False,
        location: str = "Main Warehouse",
        reference: Optional[str] = None,
        rate_s: float = DEFAULT_RATE_S,
) -> PushResult:
    """End-to-end push of a received 865FabLab order to CIN7 as a DRAFT
    stock adjustment. Set apply=False for a dry-run that validates and
    returns the computed preview without POSTing."""
    stamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    log = _setup_log(stamp)
    result = PushResult(ok=False, stage="init")

    account_id, app_key = _credentials()
    if not account_id or not app_key:
        result.errors.append(
            "CIN7_ACCOUNT_ID / CIN7_APPLICATION_KEY missing in .env")
        return result
    headers = {
        "api-auth-accountid": account_id,
        "api-auth-applicationkey": app_key,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    log.info("Push stock adjustment for order #%d (apply=%s, actor=%s)",
             draft_id, apply, actor)
    ok, errs, ctx = validate_stock_adjustment(
        draft_id, bom_parents, headers, location=location, log=log,
        rate_s=rate_s)
    result.stage = "validated"
    result.preview = ctx.get("preview", [])
    if not ok:
        result.errors.extend(errs)
        return result

    lines = ctx["lines"]
    body = {
        "EffectiveDate": datetime.now().isoformat(),
        "Status": "DRAFT",  # never COMPLETED -- human authorises in CIN7
        "Reference": reference or f"865FabLab order #{draft_id}",
        "UpdateOnHand": True,
        "Lines": lines,
    }
    log.info("Stock adjustment body: %s", json.dumps(body, indent=2))

    if not apply:
        log.info("DRY-RUN — not posting to CIN7.")
        result.ok = True
        result.stage = "dry_run"
        result.response = {"_dry_run": True, "body": body}
        return result

    resp, _ = _http(
        "POST", f"{BASE_URL}/stockadjustment", headers,
        json_body=body, log=log, rate_s=rate_s)
    if resp is None:
        result.errors.append("Network error posting stock adjustment")
        db.record_fablab_stock_adjustment_push(
            draft_id, status="failed", cin7_task_id=None,
            cin7_stocktake_number=None, lines=lines, actor=actor)
        return result
    if resp.status_code != 200:
        result.errors.append(
            f"Stock adjustment POST failed ({resp.status_code}): "
            f"{resp.text[:500]}")
        db.record_fablab_stock_adjustment_push(
            draft_id, status="failed", cin7_task_id=None,
            cin7_stocktake_number=None, lines=lines, actor=actor)
        return result

    data = resp.json() or {}
    result.ok = True
    result.stage = "posted"
    result.cin7_task_id = data.get("TaskID")
    result.cin7_stocktake_number = data.get("StocktakeNumber")
    result.response = data
    db.record_fablab_stock_adjustment_push(
        draft_id, status="pushed",
        cin7_task_id=result.cin7_task_id,
        cin7_stocktake_number=result.cin7_stocktake_number,
        lines=lines, actor=actor)
    log.info("✓ Posted. TaskID=%s StocktakeNumber=%s",
             result.cin7_task_id, result.cin7_stocktake_number)
    return result
