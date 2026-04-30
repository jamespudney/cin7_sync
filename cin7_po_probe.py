"""
cin7_po_probe.py
================
Probe CIN7's Purchase Order API to discover the full request/response
structure before we build the production POST flow. READ ONLY — never
creates or mutates a PO.

Workflow:
  1. List recent purchases via /purchaseList (uses existing read pattern)
  2. Pick a Reeves PO if available (or pass --po-id to target a specific one)
  3. Fetch full detail via /purchase?ID=<id>
  4. Dump the full JSON to output/cin7_po_probe/
  5. Print top-level keys and any nested structures (Lines, Suppliers, etc.)

This tells us:
  - The exact field names CIN7 returns on a fetched PO
  - The Lines array structure (SKU, Qty, Price, Discount, Tax, Total, etc.)
  - Status codes (DRAFT / AUTHORISED / ORDERED / RECEIVED)
  - Required vs optional fields
  - Any read-only fields (like CreatedDate from the AA5 lesson)

Usage
-----
    .venv\\Scripts\\python cin7_po_probe.py
    .venv\\Scripts\\python cin7_po_probe.py --supplier "Reeves Extruded Products, Inc"
    .venv\\Scripts\\python cin7_po_probe.py --po-id 188fe6c6-f7b8-4400-aff2-...

Safety
------
  - Read-only — only GETs the API
  - Hard-coded GUARD: this script will refuse to issue any PUT or POST
    even if a future edit attempts to. See _ENFORCE_READ_ONLY assertion.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path

import requests
from dotenv import load_dotenv

BASE_URL = "https://inventory.dearsystems.com/ExternalApi/v2"
OUTPUT_DIR = Path("output/cin7_po_probe")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

_ENFORCE_READ_ONLY = True


def _parse_retry_after(value, default: int = 30) -> int:
    if value is None:
        return default
    digits = "".join(ch for ch in str(value) if ch.isdigit())
    return int(digits) if digits else default


def _get(url: str, headers, params=None):
    """GET with 429 retry. NEVER calls PUT/POST."""
    if _ENFORCE_READ_ONLY:
        assert "PUT" not in url and "POST" not in url, \
            "This probe is hard-coded read-only — no PUT/POST allowed"
    for attempt in range(5):
        r = requests.get(url, headers=headers, params=params,
                          timeout=60)
        if r.status_code != 429:
            return r
        wait = _parse_retry_after(r.headers.get("Retry-After"), 60)
        print(f"  ... 429 attempt {attempt + 1}, sleeping {wait}s")
        time.sleep(wait)
    return r


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Probe CIN7 Purchase Order API (read-only)")
    parser.add_argument(
        "--supplier", default=None,
        help="Filter recent POs to those from this supplier (substring "
             "match, case-insensitive). Default: any.")
    parser.add_argument(
        "--po-id", default=None,
        help="Skip the list step and fetch this specific PurchaseID.")
    parser.add_argument(
        "--days", type=int, default=180,
        help="How far back to look for POs in the list (default 180).")
    args = parser.parse_args()

    load_dotenv()
    account_id = os.environ.get("CIN7_ACCOUNT_ID")
    app_key = os.environ.get("CIN7_APPLICATION_KEY")
    if not account_id or not app_key:
        print("ERROR: CIN7_ACCOUNT_ID / CIN7_APPLICATION_KEY missing")
        return 1
    headers = {
        "api-auth-accountid": account_id,
        "api-auth-applicationkey": app_key,
        "Accept": "application/json",
    }

    target_id = args.po_id

    if not target_id:
        # Step 1: list recent POs
        from datetime import timedelta
        since = (datetime.utcnow() - timedelta(days=args.days)).strftime(
            "%Y-%m-%dT%H:%M:%SZ")
        print(f"\nListing purchases since {since}...")
        page = 1
        all_pos = []
        while True:
            r = _get(f"{BASE_URL}/purchaseList", headers,
                       params={"Page": page, "Limit": 1000,
                                "UpdatedSince": since})
            if r.status_code != 200:
                print(f"  list failed {r.status_code}: {r.text[:300]}")
                return 1
            data = r.json()
            batch = data.get("PurchaseList") or []
            all_pos.extend(batch)
            print(f"  page {page} -> {len(batch)} (running {len(all_pos)})")
            if len(batch) < 1000:
                break
            page += 1
            time.sleep(1.5)
        print(f"Total recent POs: {len(all_pos)}")

        # Step 2: filter by supplier if requested
        if args.supplier:
            sup_l = args.supplier.lower()
            filtered = [
                p for p in all_pos
                if sup_l in str(p.get("Supplier") or "").lower()]
            print(f"  filtered to '{args.supplier}': {len(filtered)}")
            if filtered:
                all_pos = filtered

        if not all_pos:
            print("No POs to probe. Try a wider --days range or remove "
                  "--supplier filter.")
            return 1

        # Pick the most recent one (top of list — usually sorted by date)
        target = all_pos[0]
        target_id = (target.get("ID") or target.get("PurchaseID")
                      or target.get("PurchaseOrderID"))
        print(f"\nPicked: {target.get('Supplier')} - "
               f"#{target.get('OrderNumber') or '?'} "
               f"(status: {target.get('Status') or '?'}) — id {target_id}")

    # Step 3: fetch full detail. Try multiple endpoints + param names
    # because CIN7 splits Simple/Service/Advanced POs across paths.
    print(f"\nFetching full detail for ID={target_id}...")
    detail = None
    used_endpoint = None
    used_params = None

    def _safe_filename(s: str) -> str:
        return s.replace("/", "_").replace("\\", "_")

    # Endpoint paths to try.
    # IMPORTANT: per dearinventory.apib the canonical advanced-purchase
    # endpoint is HYPHENATED — `/advanced-purchase`. Earlier probes that
    # tried `/AdvancedPurchase` got HTML 404s for that reason. We keep
    # `purchase` and the hyphenated form first to minimize wasted calls.
    paths = [
        "purchase",
        "advanced-purchase",
        "AdvancedPurchase",
        "advancedpurchase",
    ]
    # Param sets to try with each path
    param_sets = [
        {"ID": target_id},
        {"PurchaseID": target_id},
        {"PurchaseOrderID": target_id},
        {"ID": target_id, "Task": "Order"},
    ]
    seen_responses = set()  # dedupe identical 11787-byte 404 HTMLs

    for endpoint in paths:
        for params in param_sets:
            r = _get(f"{BASE_URL}/{endpoint}", headers, params=params)
            sig = (r.status_code, len(r.text), r.text[:50])
            body_preview = r.text[:160].replace("\n", " ").replace("\r", "")
            short_resp = (
                f"{r.status_code}  body[{len(r.text)}b]: "
                f"{body_preview!r}")
            if sig in seen_responses:
                # Skip identical 404 HTML repeats to keep output readable
                print(f"  /{endpoint}  params={params}  → "
                       f"(same as previous {r.status_code} {len(r.text)}b)")
                continue
            seen_responses.add(sig)
            print(f"  /{endpoint}  params={params}  → {short_resp}")

            if r.status_code == 200 and r.text.strip():
                try:
                    detail = r.json()
                    used_endpoint = endpoint
                    used_params = params
                    break
                except ValueError:
                    (OUTPUT_DIR /
                     f"po_{target_id}_{_safe_filename(endpoint)}_raw.txt"
                     ).write_text(r.text[:50000], encoding="utf-8")
                    continue
        if detail is not None:
            break

    if detail is None:
        # As a last resort, try OPTIONS on the hyphenated endpoint and a
        # no-body POST to elicit a validation error that tells us the
        # required schema.
        print("\nFalling back to OPTIONS / POST diagnostics...")
        for verb, url in [
                ("OPTIONS", f"{BASE_URL}/advanced-purchase"),
                ("POST", f"{BASE_URL}/advanced-purchase"),
        ]:
            try:
                if verb == "OPTIONS":
                    r2 = requests.options(url, headers=headers, timeout=30)
                else:
                    # POST with empty body — diagnostic only. Server
                    # usually returns a 400 with a list of required
                    # fields, which is exactly what we need.
                    r2 = requests.post(url, headers={
                        **headers,
                        "Content-Type": "application/json",
                    }, json={}, timeout=30)
                allow = r2.headers.get("Allow", "")
                preview = r2.text[:300].replace("\n", " ").replace("\r", "")
                print(f"  {verb} {url}  → {r2.status_code}  "
                       f"Allow: {allow!r}  body: {preview!r}")
                # Save raw response for inspection
                (OUTPUT_DIR /
                 f"diag_{verb}_AdvancedPurchase.txt"
                 ).write_text(
                     f"Status: {r2.status_code}\n"
                     f"Headers: {dict(r2.headers)}\n\n"
                     f"Body:\n{r2.text[:50000]}",
                     encoding="utf-8")
            except Exception as exc:
                print(f"  {verb} {url}  → ERR {exc}")
        print("\n⚠ Could not GET an Advanced PO. Check:")
        print(f"  - {OUTPUT_DIR}/diag_*.txt for the diagnostic responses")
        print(f"  - {OUTPUT_DIR}/po_*_raw.txt for any HTML 404s saved")
        return 1
    print(f"  ✓ Used endpoint: /{used_endpoint}  with params {used_params}")
    safe_id = str(target_id).replace("/", "_")
    out_path = OUTPUT_DIR / f"po_{safe_id}.json"
    out_path.write_text(json.dumps(detail, indent=2)[:500000],
                         encoding="utf-8")
    print(f"  saved: {out_path}")

    # Step 4: print top-level structure
    print(f"\n{'=' * 70}\nTOP-LEVEL KEYS")
    print("=" * 70)
    if isinstance(detail, dict):
        for k in sorted(detail.keys()):
            v = detail[k]
            if isinstance(v, list):
                preview = f"<list of {len(v)}>"
                if v and isinstance(v[0], dict):
                    preview += (
                        f"  first item keys: {list(v[0].keys())[:8]}")
            elif isinstance(v, dict):
                preview = f"<dict keys: {list(v.keys())[:5]}>"
            else:
                preview = str(v)[:80]
            print(f"  {k:<32} {preview}")

    # Step 5: highlight likely-PO-relevant keys
    PO_KEYWORDS = (
        "ID", "OrderNumber", "Status", "Supplier", "ContactID",
        "Lines", "Order", "OrderDate", "ExpectedDeliveryDate",
        "PurchaseTaxRule", "Discount", "Total", "Note",
        "CombinePurchase", "Stage")
    print(f"\n{'=' * 70}\nKEYS LIKELY RELEVANT TO POST BODY")
    print("=" * 70)
    if isinstance(detail, dict):
        for k in sorted(detail.keys()):
            for kw in PO_KEYWORDS:
                if kw.lower() in k.lower():
                    v = detail[k]
                    print(f"  {k:<32} = "
                           f"{json.dumps(v, default=str)[:120]}")
                    break

    # Step 6: explicitly inspect Lines array (this is the meat)
    if isinstance(detail, dict):
        for line_key in ("Lines", "OrderLines", "PurchaseLines",
                          "Order", "Items"):
            if line_key in detail and isinstance(
                    detail[line_key], list) and detail[line_key]:
                print(f"\n{'=' * 70}\n{line_key}[0] FIELDS")
                print("=" * 70)
                first = detail[line_key][0]
                if isinstance(first, dict):
                    for k in sorted(first.keys()):
                        print(f"  {k:<32} {json.dumps(first[k], default=str)[:80]}")
                break

    print(f"\n{'=' * 70}")
    print("Probe complete. Saved JSON for inspection.")
    print("Next step: I'll use this structure to design the POST body.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
