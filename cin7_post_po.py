"""
cin7_post_po.py
===============
Push a local po_drafts row into CIN7 as a real Draft Advanced Purchase.

Why this exists
---------------
Until now, the "Create draft PO in CIN7" button on the Ordering page was a
placeholder. Buyers had to export CSV/PDF and re-key the lines into CIN7
by hand. This module wires the actual POST flow.

CIN7 API flow (per dearinventory.apib § Advanced Purchase)
----------------------------------------------------------
The endpoint is `/advanced-purchase` (HYPHENATED — earlier scratch probes
hit `/AdvancedPurchase` and silently 404'd with HTML, which is why every
prior attempt seemed to fail). Two calls are needed:

  1. POST /advanced-purchase
       Body: SupplierID + Approach=INVOICE + Location + PurchaseType=Advanced
             (+ optional Note/RequiredBy/TaxRule/Terms/AdditionalAttributes)
       → Returns the full Purchase object including the new ID and OrderNumber.
         Status starts as 'DRAFT'.

  2. POST /purchase/order
       Body: { TaskID: <ID-from-step-1>, Memo, Status: "DRAFT", Lines: [...] }
       → Adds the line items to the DRAFT we just created.
         A buyer logs into CIN7 to review, attach, and AUTHORISE.

We intentionally STOP at status=DRAFT. We never authorise or order from
the API — a human always reviews in CIN7 before the supplier sees it.

⚠ Hard rules (never relax these)
-------------------------------
• PRODUCT SKU MUTATION IS FORBIDDEN. Lines reference products by ProductID
  (the CIN7 GUID). We do NOT send any payload that could rename a product.
  This module touches /advanced-purchase and /purchase/order — neither
  should affect product master data, but we still enforce a whitelist on
  the order Lines body to be safe.
• Pre-flight: a draft must be valid (supplier resolves, every SKU resolves
  to a CIN7 ProductID, every qty > 0, MOV met or explicitly waived).
  Validation lives in `validate_draft()` and runs before any HTTP call.
• Idempotency: if the local draft already has a `cin7_po_id`, we refuse
  to POST again — the user must cancel the local draft or wipe the field.

Usage
-----
    # Library (called by app.py button):
        from cin7_post_po import push_po_draft
        result = push_po_draft(draft_id=42, actor="james", apply=True)
        if result.ok:
            print(result.cin7_po_number)
        else:
            print(result.errors)

    # CLI (manual / scripted runs):
        .venv\\Scripts\\python cin7_post_po.py --draft 42 --dry-run
        .venv\\Scripts\\python cin7_post_po.py --draft 42 --apply
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional

import requests
from dotenv import load_dotenv

import db


BASE_URL = "https://inventory.dearsystems.com/ExternalApi/v2"
# OUTPUT_DIR follows DATA_DIR env var (set to /data on Render).
from data_paths import OUTPUT_DIR  # noqa: E402

# Be friendlier than the migration push — POs are bigger, run rarely, and
# CIN7's 60/min cap is shared with other integrations.
DEFAULT_RATE_S = 1.5
MAX_429_RETRIES = 3

# Hard whitelist for the master POST body. Any extra keys must be added
# here intentionally — we'd rather fail loudly than silently send a body
# CIN7 might interpret in unexpected ways.
ALLOWED_MASTER_KEYS = {
    "SupplierID", "Supplier", "Contact", "Phone",
    "Approach", "BlindReceipt",
    "BillingAddress", "ShippingAddress",
    "TaxRule", "TaxCalculation", "Terms",
    "RequiredBy", "Location",
    "PurchaseType", "Note", "OrderNumber",
    "IsServiceOnly", "CurrencyRate",
    "AdditionalAttributes",
}
ALLOWED_ORDER_KEYS = {
    "TaskID", "CombineAdditionalCharges", "Memo", "Status",
    "Lines", "AdditionalCharges",
}
ALLOWED_LINE_KEYS = {
    "ProductID", "SKU", "Name", "Quantity", "Price",
    "Discount", "Tax", "TaxRule", "SupplierSKU",
    "Comment", "Total",
    # Total IS required — CIN7 validates the line.Total against its own
    # calculation (Quantity × Price × (1 − Discount/100) + Tax) and
    # rejects the line if they disagree. Discovered the hard way:
    # PO-7076 master was created but lines POST failed with
    # "attribute 'Total' doesn't match … Expected value is: 1436.50".
}


# ---------------------------------------------------------------------------
# Process-level master-POST tracker — belt-and-braces against
# accidentally creating duplicate CIN7 master POs for the same local
# draft within the same process. The local DB's cin7_po_id field is
# our PRIMARY guard, but if someone clears that field manually (or via
# the "Clear CIN7 link" button) AND clicks Confirm push twice in quick
# succession, the second call would see an empty cin7_po_id and POST
# again. This in-memory set catches that case — it remembers every
# draft_id we've already POST'd a master for during this Python
# process's lifetime. Reset on container restart, which is fine because
# by then the cin7_po_id should be persisted in the DB anyway.
_MASTER_POSTED_DRAFTS: set = set()


# ---------------------------------------------------------------------------
# Result type
# ---------------------------------------------------------------------------
@dataclass
class PushResult:
    """Return value from push_po_draft. Always inspect .ok before .cin7_po_number.
    `errors` is a list of human-readable messages; `warnings` is similar but
    non-fatal. Stage tells you how far we got: 'validated' / 'master_posted'
    / 'lines_posted' / 'finalised'."""
    ok: bool
    stage: str = "init"
    cin7_po_id: Optional[str] = None
    cin7_po_number: Optional[str] = None
    cin7_status: Optional[str] = None
    errors: list = field(default_factory=list)
    warnings: list = field(default_factory=list)
    master_response: Optional[dict] = None
    order_response: Optional[dict] = None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _parse_retry_after(value, default: int = 30) -> int:
    """CIN7 returns Retry-After as plain seconds ('30') or with units
    ('60 Seconds'). Strip non-digits."""
    if value is None:
        return default
    digits = "".join(ch for ch in str(value) if ch.isdigit())
    return int(digits) if digits else default


def _setup_log(stamp: str) -> logging.Logger:
    log = logging.getLogger("cin7_post_po")
    log.setLevel(logging.INFO)
    if not log.handlers:
        fh = logging.FileHandler(
            OUTPUT_DIR / f"cin7_post_po_{stamp}.log",
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
    """HTTP call with 429 retry + throttle. Returns (response, new_last_call)."""
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
# Resolve supplier & products
# ---------------------------------------------------------------------------
def _resolve_supplier(name: str, headers: dict, log=None,
                       rate_s: float = DEFAULT_RATE_S,
                       last_call: float = 0.0
                       ) -> tuple[Optional[dict], float, list]:
    """Look up SupplierID in CIN7 by exact name (case-insensitive).
    Returns (supplier_dict_or_None, new_last_call, candidates_list).

    Strict policy: we ONLY return a supplier on an EXACT case-insensitive
    name match. If CIN7's Search returns hits but none match exactly, we
    return None plus the list of candidates so the caller can show them
    to the user.

    Why so strict: silently falling back to 'first hit' caused us to
    POST a Reeves-labelled draft against the wrong CIN7 supplier
    (PO-7076 incident). Better to refuse than to mis-route a PO."""
    # Per dearinventory.apib § Supplier, the search param is `Name`
    # (returns suppliers whose name STARTS WITH the value). Earlier
    # versions used `Search` which CIN7 silently ignored, returning
    # the first 50 of ALL suppliers — that's how PO-7076 ended up
    # routed to the wrong vendor.
    resp, last_call = _http(
        "GET", f"{BASE_URL}/supplier",
        headers, params={"Name": name, "Page": 1, "Limit": 50},
        log=log, rate_s=rate_s, last_call=last_call)
    if resp is None or resp.status_code != 200:
        return None, last_call, []
    suppliers = (resp.json() or {}).get("SupplierList") or []
    if not suppliers:
        return None, last_call, []
    name_l = (name or "").strip().lower()
    for s in suppliers:
        if (s.get("Name") or "").strip().lower() == name_l:
            return s, last_call, suppliers
    # No exact match — return None so the caller can decide.
    return None, last_call, suppliers


def _resolve_products(skus: list, headers: dict, log=None,
                       rate_s: float = DEFAULT_RATE_S,
                       last_call: float = 0.0
                       ) -> tuple[dict, float]:
    """Resolve a list of SKUs to {sku: product_dict}. Missing SKUs are
    omitted from the dict. Each product_dict has at least ID and SKU; we
    rely on CIN7 to give us the canonical SKU back so we never send a
    different SKU back as a line item.

    We pass IncludeSuppliers=true so each product carries its
    Suppliers[] array. That's where CIN7 stores the supplier-specific
    `Cost` ("Latest purchase cost") that CIN7 itself uses when
    auto-filling a PO line. Without this we'd fall back to AverageCost,
    which is a global weighted average and does NOT match what CIN7
    expects on a per-supplier line."""
    resolved: dict = {}
    for sku in skus:
        if not sku:
            continue
        resp, last_call = _http(
            "GET", f"{BASE_URL}/product", headers,
            params={"Sku": sku, "Limit": 1, "IncludeSuppliers": "true"},
            log=log, rate_s=rate_s, last_call=last_call)
        if resp is None or resp.status_code != 200:
            continue
        products = (resp.json() or {}).get("Products") or []
        if products:
            resolved[sku] = products[0]
    return resolved, last_call


def _supplier_cost_for(prod: dict, supplier_id: Optional[str],
                        supplier_name: Optional[str]) -> Optional[float]:
    """Return the supplier-specific Cost from prod['Suppliers'] for the
    given supplier (matched by ID first, then name). None if no match.
    The matched value is what CIN7 uses internally when building a PO
    line — sending anything else risks the 'Total doesn't match'
    rejection we hit on PO-7076."""
    suppliers = prod.get("Suppliers") or []
    if not suppliers:
        return None
    sid_l = str(supplier_id or "").strip().lower()
    sname_l = str(supplier_name or "").strip().lower()
    for s in suppliers:
        if sid_l and str(s.get("SupplierID") or "").lower() == sid_l:
            cost = s.get("Cost")
            if cost is not None:
                return float(cost)
    # Fallback to name match
    for s in suppliers:
        if sname_l and str(
                s.get("SupplierName") or "").strip().lower() == sname_l:
            cost = s.get("Cost")
            if cost is not None:
                return float(cost)
    return None


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------
def validate_draft(draft_id: int, *,
                    require_mov: bool = True,
                    po_value_override: Optional[float] = None,
                    allow_existing_cin7_id: bool = False,
                    headers: Optional[dict] = None,
                    log=None) -> tuple[bool, list, dict]:
    """Pre-flight check. Returns (ok, errors, context).

    `po_value_override`: when the caller has a more accurate value for
    the PO than the proxy we'd compute (CIN7 AverageCost × qty), pass it
    here. The Streamlit UI passes the editor's `po_value` so MOV checks
    use the freshly-quoted price the buyer is actually seeing rather
    than a stale AverageCost.

    `context` is a dict containing items needed by the push step:
      - draft_row, lines_rows
      - supplier (CIN7 dict), supplier_cfg (db row)
      - resolved_products: {sku: cin7_product_dict}
      - po_value, mov_amount, mov_met

    The check is conservative: we'd rather fail noisily here than have
    CIN7 accept a partial PO that the buyer then has to clean up.
    """
    errors: list = []
    ctx: dict = {}

    draft = db.get_po_draft(draft_id)
    if not draft:
        return False, [f"Draft #{draft_id} not found"], ctx
    ctx["draft"] = dict(draft)

    if draft["status"] != "editing":
        errors.append(
            f"Draft is in status '{draft['status']}'; only 'editing' "
            "drafts can be pushed.")

    if draft["cin7_po_id"] and not allow_existing_cin7_id:
        errors.append(
            f"Draft already has CIN7 PO ID {draft['cin7_po_id']!r}. "
            "Refusing to POST again. Either cancel the local draft, "
            "or use retry_lines_only=True if the prior attempt failed "
            "before lines were posted (master exists but is empty).")

    lines = list(db.list_po_draft_lines(draft_id))
    if not lines:
        errors.append("Draft has no lines.")
    ctx["lines"] = [dict(line_row) for line_row in lines]

    # All quantities must be > 0
    bad_qty = [r for r in lines if (r["edited_qty"] or 0) <= 0]
    if bad_qty:
        errors.append(
            f"{len(bad_qty)} line(s) have qty <= 0: "
            f"{', '.join(r['sku'] for r in bad_qty[:5])}"
            + (" …" if len(bad_qty) > 5 else ""))

    # No duplicate SKUs (defensive — po_draft_lines.PK enforces this at
    # write time, but if someone has been poking the DB by hand this
    # is the place we catch it).
    seen_skus: dict = {}
    dups = []
    for r in lines:
        s = r["sku"]
        if s in seen_skus:
            dups.append(s)
        else:
            seen_skus[s] = True
    if dups:
        errors.append(
            f"Duplicate SKU(s) on this draft: {', '.join(dups[:5])}"
            + (" …" if len(dups) > 5 else ""))

    # Local supplier config (lead time, MOV, defaults)
    sup_cfgs = db.all_supplier_configs() if hasattr(
        db, "all_supplier_configs") else {}
    sup_cfg = sup_cfgs.get(draft["supplier"], {}) if isinstance(
        sup_cfgs, dict) else {}
    ctx["supplier_cfg"] = sup_cfg

    if not headers:
        return False, errors + [
            "No CIN7 headers provided — cannot resolve supplier/products. "
            "Pass headers= to validate_draft() or use push_po_draft()."], ctx

    # Resolve supplier (STRICT exact match — see _resolve_supplier docstring)
    sup, _, sup_candidates = _resolve_supplier(
        draft["supplier"], headers, log=log)
    if not sup:
        if sup_candidates:
            cand_names = [
                f"  • {(c.get('Name') or '').strip()}"
                for c in sup_candidates[:8]]
            errors.append(
                f"Supplier {draft['supplier']!r} did not exact-match any "
                f"CIN7 supplier. CIN7's Search returned "
                f"{len(sup_candidates)} candidate(s):\n"
                + "\n".join(cand_names)
                + (f"\n  … and {len(sup_candidates) - 8} more"
                   if len(sup_candidates) > 8 else "")
                + "\n\nFix: rename the local supplier to match CIN7 "
                "exactly (case-insensitive). Use rename_supplier_in_pricing.py "
                "or update db.supplier_config / family_supplier_assignments.")
        else:
            errors.append(
                f"Supplier {draft['supplier']!r} not found in CIN7 — "
                "the Search endpoint returned zero hits. Check the name "
                "in CIN7's Suppliers list.")
    ctx["cin7_supplier"] = sup
    ctx["cin7_supplier_candidates"] = sup_candidates

    # Resolve every line's SKU to a ProductID
    skus = [r["sku"] for r in lines if r["sku"]]
    resolved, _ = _resolve_products(skus, headers, log=log)
    ctx["resolved_products"] = resolved
    missing = [s for s in skus if s not in resolved]
    if missing:
        errors.append(
            f"{len(missing)} SKU(s) not found in CIN7: "
            f"{', '.join(missing[:8])}"
            + (" …" if len(missing) > 8 else ""))

    # MOV check.
    # Prefer the caller-supplied po_value (e.g., from the Streamlit
    # editor where the buyer may have entered freshly-quoted prices).
    # Fall back to qty × resolved product's AverageCost.
    if po_value_override is not None:
        po_value = float(po_value_override)
        ctx["po_value_source"] = "override (caller)"
    else:
        po_value = 0.0
        for r in lines:
            prod = resolved.get(r["sku"])
            if not prod:
                continue
            unit_cost = float(prod.get("AverageCost") or 0)
            po_value += float(r["edited_qty"] or 0) * unit_cost
        ctx["po_value_source"] = "estimate (CIN7 AverageCost × qty)"
    ctx["po_value_estimate"] = po_value
    mov_amount = float(sup_cfg.get("mov_amount") or 0)
    ctx["mov_amount"] = mov_amount
    ctx["mov_met"] = (mov_amount == 0) or (po_value >= mov_amount)
    if require_mov and mov_amount > 0 and po_value < mov_amount:
        errors.append(
            f"MOV not met: PO value ${po_value:,.0f} "
            f"({ctx['po_value_source']}) < required "
            f"${mov_amount:,.0f}. Add lines or pass "
            "require_mov=False to override.")

    return (len(errors) == 0), errors, ctx


# ---------------------------------------------------------------------------
# Body builders
# ---------------------------------------------------------------------------
def _build_master_body(*, supplier_id: str,
                        location: str,
                        approach: str = "INVOICE",
                        purchase_type: str = "Advanced",
                        tax_rule: Optional[str] = None,
                        terms: Optional[str] = None,
                        required_by: Optional[str] = None,
                        note: Optional[str] = None,
                        ) -> dict:
    """Build the POST /advanced-purchase body. SupplierID + Approach + Location
    + PurchaseType are all that's strictly required per the spec; the rest
    are optional but useful."""
    body: dict = {
        "SupplierID": supplier_id,
        "Approach": approach,             # 'INVOICE' or 'STOCK'
        "Location": location,
        "PurchaseType": purchase_type,    # 'Simple' or 'Advanced'
    }
    if tax_rule:
        body["TaxRule"] = tax_rule
    if terms:
        body["Terms"] = terms
    if required_by:
        body["RequiredBy"] = required_by
    if note:
        body["Note"] = note[:1024]
    bad = set(body.keys()) - ALLOWED_MASTER_KEYS
    if bad:
        raise RuntimeError(
            f"Master POST body has disallowed keys {bad}. "
            f"Add to ALLOWED_MASTER_KEYS only after auditing.")
    return body


def _build_lines(draft_lines: list, resolved: dict,
                  *,
                  supplier_id: Optional[str] = None,
                  supplier_name: Optional[str] = None,
                  freight_overrides: Optional[dict] = None,
                  unit_cost_override: Optional[dict] = None,
                  ) -> list:
    """Convert local draft lines + resolved CIN7 products into PO line dicts.
    supplier_id / supplier_name: identify which supplier on the product
        master we should pull the per-supplier `Cost` from. CIN7 uses
        that exact value when auto-filling a PO line; if we send a
        different Price it accepts our value, but if we mis-calculate
        Total it rejects with "Total doesn't match" (per the spec it
        validates Quantity × Price). So we ALWAYS source the price
        from CIN7 itself rather than the local app's POCost.
    freight_overrides: {sku: 'air'/'sea'} — appended to Comment so the
        buyer can see them in CIN7.
    unit_cost_override: {sku: float} — explicit override (rarely used —
        the user's policy is "don't override CIN7's cost").
    """
    out = []
    for r in draft_lines:
        sku = r["sku"]
        prod = resolved.get(sku)
        if not prod:
            # Skipped silently — validate_draft() should have caught this.
            continue
        # Cost resolution priority:
        #   1. Explicit override (rarely used)
        #   2. Supplier-specific Cost from prod.Suppliers[]
        #   3. AverageCost (last-ditch fallback; logs warning)
        if unit_cost_override and sku in unit_cost_override:
            unit_cost = float(unit_cost_override[sku])
        else:
            sup_cost = _supplier_cost_for(
                prod, supplier_id, supplier_name)
            if sup_cost is not None:
                unit_cost = sup_cost
            else:
                # No per-supplier Cost found. Use AverageCost so we
                # still send something — but be aware CIN7 may reject
                # the line if its expected Total differs.
                unit_cost = float(prod.get("AverageCost") or 0)
        comment = ""
        if freight_overrides and sku in freight_overrides:
            comment = f"Freight: {freight_overrides[sku]}"
        qty = float(r["edited_qty"] or 0)
        # CIN7 validates Total against its own calculation:
        #   Total = Quantity × Price × (1 − Discount/100) + Tax
        # Since we currently always send Discount=0 and Tax=0, this
        # simplifies to qty × unit_cost. Round to 2dp to match CIN7's
        # display precision and avoid floating-point mismatches.
        total = round(qty * unit_cost, 2)
        line = {
            "ProductID": prod.get("ID") or prod.get("ProductID"),
            # Echo back CIN7's SKU exactly. NEVER substitute a different
            # value — that's how the LED-18.046 rename happened in 2025.
            "SKU": prod.get("SKU"),
            "Name": prod.get("Name"),
            "Quantity": qty,
            "Price": unit_cost,
            "Discount": 0,
            "Tax": 0,
            "Total": total,
        }
        if comment:
            line["Comment"] = comment
        # Whitelist guard
        bad = set(line.keys()) - ALLOWED_LINE_KEYS
        if bad:
            raise RuntimeError(
                f"Order line has disallowed keys {bad}. "
                f"Add to ALLOWED_LINE_KEYS only after auditing.")
        out.append(line)
    return out


# ---------------------------------------------------------------------------
# Main push
# ---------------------------------------------------------------------------
def push_po_draft(draft_id: int, *,
                   actor: str,
                   apply: bool = False,
                   require_mov: bool = True,
                   po_value_override: Optional[float] = None,
                   approach: str = "INVOICE",
                   purchase_type: str = "Advanced",
                   default_location: str = "Main Warehouse",
                   tax_rule: Optional[str] = None,
                   terms: Optional[str] = None,
                   required_by: Optional[str] = None,
                   freight_overrides: Optional[dict] = None,
                   unit_cost_override: Optional[dict] = None,
                   rate_s: float = DEFAULT_RATE_S,
                   retry_lines_only: bool = False,
                   ) -> PushResult:
    """End-to-end push of a local po_drafts row to CIN7.
    Returns a PushResult — always check .ok before .cin7_po_number.
    Set apply=False for a dry-run that validates and prints the bodies
    without sending."""
    stamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    log = _setup_log(stamp)

    result = PushResult(ok=False, stage="init")

    # ---- Credentials
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

    # ---- Validate
    log.info("Push draft #%d (apply=%s, actor=%s, retry_lines_only=%s)",
              draft_id, apply, actor, retry_lines_only)
    ok, errs, ctx = validate_draft(
        draft_id,
        require_mov=require_mov,
        po_value_override=po_value_override,
        allow_existing_cin7_id=retry_lines_only,
        headers=headers, log=log)
    result.stage = "validated"
    if not ok:
        result.errors.extend(errs)
        return result
    draft = ctx["draft"]
    lines_rows = ctx["lines"]
    cin7_supplier = ctx["cin7_supplier"]
    resolved = ctx["resolved_products"]
    # In retry mode, refuse if the local draft has no recorded CIN7 ID.
    if retry_lines_only and not draft.get("cin7_po_id"):
        result.errors.append(
            "retry_lines_only=True but the draft has no cin7_po_id "
            "recorded. Run a normal push (without retry) instead.")
        return result

    # ---- Build master body
    note_parts = [
        f"Draft #{draft_id}: {draft.get('name') or '(unnamed)'}",
    ]
    if draft.get("freight_mode"):
        note_parts.append(f"Freight: {draft['freight_mode']}")
    if draft.get("note"):
        note_parts.append(draft["note"])
    note_parts.append(
        f"Generated by Wired4Signs analytics by {actor} at "
        f"{datetime.now().strftime('%Y-%m-%d %H:%M')}")
    master_body = _build_master_body(
        supplier_id=cin7_supplier["ID"],
        location=default_location,
        approach=approach,
        purchase_type=purchase_type,
        tax_rule=tax_rule,
        terms=terms,
        required_by=required_by,
        note=" | ".join(note_parts),
    )
    log.info("Master body: %s", json.dumps(master_body, indent=2))

    # ---- Build line items.
    # Pass supplier_id/name so each line uses CIN7's own per-supplier
    # Cost — that's the rule the user gave us: "don't overwrite the
    # unit cost CIN7 maintains when we input the SKU".
    line_items = _build_lines(
        lines_rows, resolved,
        supplier_id=cin7_supplier.get("ID") if cin7_supplier else None,
        supplier_name=(cin7_supplier.get("Name")
                        if cin7_supplier else None),
        freight_overrides=freight_overrides,
        unit_cost_override=unit_cost_override)
    if not line_items:
        result.errors.append("No resolvable line items.")
        return result
    log.info("Built %d line(s).", len(line_items))

    if not apply:
        log.info("DRY-RUN — not posting to CIN7.")
        result.ok = True
        result.stage = "dry_run"
        # Include the resolved supplier and the line preview so the
        # caller can show the user what would actually go out — that's
        # the last sanity-check before money moves.
        result.master_response = {
            "_dry_run": True,
            "body": master_body,
            "resolved_supplier": {
                "ID": cin7_supplier.get("ID") if cin7_supplier else None,
                "Name": (cin7_supplier.get("Name")
                          if cin7_supplier else None),
            } if cin7_supplier else None,
        }
        result.order_response = {
            "_dry_run": True, "lines": line_items}
        return result

    last_call = 0.0
    if retry_lines_only:
        # Use the existing master we created in a prior attempt.
        cin7_po_id = draft.get("cin7_po_id")
        cin7_po_number = draft.get("cin7_po_number")
        cin7_status = draft.get("cin7_po_status") or "DRAFT"
        result.cin7_po_id = cin7_po_id
        result.cin7_po_number = cin7_po_number
        result.cin7_status = cin7_status
        result.stage = "master_reused"
        log.info("  ↻ Re-using master ID=%s OrderNumber=%s",
                  cin7_po_id, cin7_po_number)
    else:
        # Belt-and-braces: refuse a second master POST for the same
        # draft within this process even if local cin7_po_id is empty.
        # Caused the PO-7073/74/75/76 orphan-PO scenario before. The
        # in-memory set is the second line of defence; the DB-level
        # idempotency check in validate_draft is the first.
        if draft_id in _MASTER_POSTED_DRAFTS:
            result.errors.append(
                f"Refusing master POST: draft #{draft_id} already had "
                "a master POSTed in this server process. If the prior "
                "POST succeeded, its cin7_po_id should already be on "
                "the draft (check db.po_drafts). If you really need to "
                "retry, restart the service or use retry_lines_only.")
            return result

        # ---- Step 1: POST master /advanced-purchase
        log.info("POST /advanced-purchase ...")
        resp, last_call = _http(
            "POST", f"{BASE_URL}/advanced-purchase", headers,
            json_body=master_body, log=log,
            rate_s=rate_s, last_call=last_call)
        if resp is None:
            result.errors.append("Network error posting master")
            return result
        if resp.status_code != 200:
            result.errors.append(
                f"Master POST failed ({resp.status_code}): "
                f"{resp.text[:600]}")
            (OUTPUT_DIR /
             f"cin7_post_po_master_fail_{draft_id}_{stamp}.txt"
             ).write_text(
                f"Status: {resp.status_code}\n"
                f"Body: {resp.text[:50000]}", encoding="utf-8")
            return result
        master = resp.json() or {}
        result.master_response = master
        cin7_po_id = master.get("ID")
        cin7_po_number = master.get("OrderNumber")
        cin7_status = master.get("Status")
        result.cin7_po_id = cin7_po_id
        result.cin7_po_number = cin7_po_number
        result.cin7_status = cin7_status
        result.stage = "master_posted"
        # Belt-and-braces: record this draft has had a master POST in
        # this process so a second call refuses regardless of DB state.
        _MASTER_POSTED_DRAFTS.add(draft_id)
        log.info("  ✓ Master created — ID=%s OrderNumber=%s Status=%s",
                  cin7_po_id, cin7_po_number, cin7_status)

        # Persist immediately so a subsequent failure of the lines POST
        # leaves a recoverable trail.
        try:
            db.set_po_draft_cin7_ids(
                draft_id,
                cin7_po_id=cin7_po_id,
                cin7_po_number=cin7_po_number,
                cin7_status=cin7_status,
                actor=actor)
        except Exception as exc:
            log.warning("  could not persist cin7_po_id locally: %s", exc)
            result.warnings.append(
                f"Master created in CIN7 ({cin7_po_id}) but local "
                f"cin7_po_id couldn't be saved: {exc}. The CIN7 PO is "
                "live — record it manually if needed.")

    # ---- Step 2: POST /purchase/order with TaskID + Lines
    order_body = {
        "TaskID": cin7_po_id,
        "CombineAdditionalCharges": False,
        "Memo": f"Wired4Signs draft #{draft_id}",
        "Status": "DRAFT",   # NEVER auto-AUTHORISE — buyer reviews in CIN7
        "Lines": line_items,
    }
    bad = set(order_body.keys()) - ALLOWED_ORDER_KEYS
    if bad:
        raise RuntimeError(
            f"Order POST body has disallowed keys {bad}. "
            f"Add to ALLOWED_ORDER_KEYS only after auditing.")
    log.info("POST /purchase/order  (TaskID=%s, %d line(s)) ...",
              cin7_po_id, len(line_items))
    resp2, last_call = _http(
        "POST", f"{BASE_URL}/purchase/order", headers,
        json_body=order_body, log=log,
        rate_s=rate_s, last_call=last_call)
    def _try_void_master(reason: str) -> bool:
        """Attempt to DELETE the master we just created so we don't
        leave an empty orphan PO in CIN7. Only runs in the master-just-
        created path (NOT in retry_lines_only mode — there the master
        already existed before this push, so we don't own it).
        Returns True if the master was successfully voided/deleted."""
        if retry_lines_only:
            log.info("Skipping auto-rollback — retry mode, master "
                      "predates this push.")
            return False
        log.info("Auto-rollback: DELETE /advanced-purchase?ID=%s&Void=true",
                  cin7_po_id)
        del_resp, _ = _http(
            "DELETE", f"{BASE_URL}/advanced-purchase", headers,
            params={"ID": cin7_po_id, "Void": "true"},
            log=log, rate_s=rate_s, last_call=last_call)
        if del_resp is not None and del_resp.status_code in (
                200, 204):
            log.info("  ✓ Master %s voided.", cin7_po_number)
            try:
                db.set_po_draft_cin7_ids(
                    draft_id, cin7_po_id="", cin7_po_number="",
                    cin7_status="", actor=actor)
            except Exception as exc:
                log.warning("  could not clear local cin7_po_id "
                             "after auto-rollback: %s", exc)
            return True
        log.warning(
            "  ⚠ Auto-rollback FAILED (%s, %s). Master left in CIN7.",
            "no response" if del_resp is None else del_resp.status_code,
            (reason or "")[:60])
        return False

    if resp2 is None:
        rolled = _try_void_master("network error on lines POST")
        if rolled:
            result.cin7_po_id = None
            result.cin7_po_number = None
            result.errors.append(
                "Network error posting lines. Master was auto-voided "
                "in CIN7 — no orphan left behind. Try again.")
        else:
            result.errors.append(
                f"Network error posting lines AND auto-rollback "
                f"failed. Master {cin7_po_number} ({cin7_po_id}) "
                "exists in CIN7 — please void it manually.")
        return result
    if resp2.status_code != 200:
        # Save the failure response for debugging BEFORE we attempt rollback
        (OUTPUT_DIR /
         f"cin7_post_po_lines_fail_{draft_id}_{stamp}.txt"
         ).write_text(
            f"Status: {resp2.status_code}\n"
            f"Body: {resp2.text[:50000]}\n\n"
            f"Lines we sent:\n{json.dumps(line_items, indent=2)}",
            encoding="utf-8")
        rolled = _try_void_master(
            f"lines POST {resp2.status_code}")
        if rolled:
            result.cin7_po_id = None
            result.cin7_po_number = None
            result.errors.append(
                f"Lines POST failed ({resp2.status_code}): "
                f"{resp2.text[:400]}\n\n"
                "Master was auto-voided in CIN7 — no orphan left behind. "
                "Fix the underlying issue and try again.")
        else:
            result.errors.append(
                f"Lines POST failed ({resp2.status_code}): "
                f"{resp2.text[:400]}\n\n"
                f"Auto-rollback also failed — master {cin7_po_number} "
                f"({cin7_po_id}) is now an empty orphan in CIN7. "
                "Please void it manually.")
        log.error(result.errors[-1])
        return result
    result.order_response = resp2.json() or {}
    result.stage = "lines_posted"
    log.info("  ✓ Lines applied.")

    # ---- Mark local draft submitted (and refresh cin7_po_status)
    try:
        db.mark_po_draft_submitted(
            draft_id, actor=actor,
            cin7_po_number=cin7_po_number or "",
            cin7_po_id=cin7_po_id or "")
        db.set_po_draft_cin7_ids(
            draft_id, cin7_status="DRAFT", actor=actor)
    except Exception as exc:
        log.warning(
            "Lines posted but local mark-submitted failed: %s", exc)
        result.warnings.append(
            f"Lines applied in CIN7 but local DB submit-mark failed: "
            f"{exc}. Reconcile manually.")

    result.ok = True
    result.stage = "finalised"
    log.info(
        "✓ Push complete — CIN7 PO %s (%s) is in DRAFT status. "
        "Buyer should review in CIN7 before authorising.",
        cin7_po_number, cin7_po_id)
    return result


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main() -> int:
    parser = argparse.ArgumentParser(
        description="Push a local po_drafts row to CIN7 as a Draft PO")
    parser.add_argument(
        "--draft", type=int, required=True,
        help="po_drafts.id to push")
    parser.add_argument(
        "--apply", action="store_true",
        help="Actually POST. Without this we dry-run.")
    parser.add_argument(
        "--no-mov", action="store_true",
        help="Skip MOV enforcement.")
    parser.add_argument(
        "--actor", default=None,
        help="Username for audit log. Default: $USER or 'cli'.")
    parser.add_argument(
        "--location", default=os.environ.get(
            "CIN7_DEFAULT_LOCATION", "Main Warehouse"),
        help="CIN7 location to receive into (default: $CIN7_DEFAULT_LOCATION "
             "or 'Main Warehouse').")
    parser.add_argument(
        "--approach", default="INVOICE",
        choices=["INVOICE", "STOCK"],
        help="INVOICE = invoice-first, STOCK = stock-first. Default: INVOICE.")
    parser.add_argument(
        "--type", default="Advanced", dest="ptype",
        choices=["Simple", "Advanced"],
        help="Purchase Type. Default: Advanced.")
    parser.add_argument(
        "--rate", type=float, default=DEFAULT_RATE_S,
        help=f"Seconds between API calls. Default {DEFAULT_RATE_S}.")
    parser.add_argument(
        "--retry-lines", action="store_true",
        help="Skip the master POST and only POST lines to the existing "
             "cin7_po_id stored on the draft. Useful when a previous "
             "attempt created the master but failed on lines.")
    args = parser.parse_args()

    actor = args.actor or os.environ.get("USER") or os.environ.get(
        "USERNAME") or "cli"

    print(f"\n{'='*72}")
    print(f"Pushing draft #{args.draft} (apply={args.apply}, actor={actor})")
    print('='*72)

    result = push_po_draft(
        args.draft, actor=actor, apply=args.apply,
        require_mov=not args.no_mov,
        approach=args.approach,
        purchase_type=args.ptype,
        default_location=args.location,
        rate_s=args.rate,
        retry_lines_only=args.retry_lines,
    )

    if result.warnings:
        print("\nWarnings:")
        for w in result.warnings:
            print(f"  ⚠  {w}")
    if result.errors:
        print("\nErrors:")
        for e in result.errors:
            print(f"  ✗  {e}")

    print(f"\nStage : {result.stage}")
    print(f"OK    : {result.ok}")
    if result.cin7_po_id:
        print(f"CIN7 PO ID     : {result.cin7_po_id}")
        print(f"CIN7 PO Number : {result.cin7_po_number}")
        print(f"CIN7 Status    : {result.cin7_status}")

    return 0 if result.ok else 1


if __name__ == "__main__":
    sys.exit(main())
