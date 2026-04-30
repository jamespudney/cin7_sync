"""
cin7_alt_probe.py
=================
Fetch a single CIN7 product and find the API field name that holds
the "Alternative Products" data (the section in the CIN7 product detail
UI with columns: Interchangeable, Matching, Use in production, Quantity).

Usage
-----
    .venv\\Scripts\\python cin7_alt_probe.py LED-E60L24DC-KO

Output
------
    output/cin7_probe/product_<SKU>.json   full product payload
    Console: any keys / nested keys mentioning alt / related / replac /
             interchange / matching / production / quantity-link
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import requests
from dotenv import load_dotenv

BASE_URL = "https://inventory.dearsystems.com/ExternalApi/v2"

KEYWORDS = (
    "alternat", "related", "replac", "interchange", "matching",
    "subst", "swap", "equiv", "useinproduction",
)


def walk(obj, prefix=""):
    """Yield (path, value) for every leaf in a nested JSON structure."""
    if isinstance(obj, dict):
        for k, v in obj.items():
            path = f"{prefix}.{k}" if prefix else k
            yield path, v
            yield from walk(v, path)
    elif isinstance(obj, list):
        for i, item in enumerate(obj):
            path = f"{prefix}[{i}]"
            yield from walk(item, path)


def main() -> int:
    if len(sys.argv) < 2:
        print("Usage: cin7_alt_probe.py <SKU>")
        return 1
    sku = sys.argv[1]

    load_dotenv()
    account_id = os.environ.get("CIN7_ACCOUNT_ID")
    app_key = os.environ.get("CIN7_APPLICATION_KEY")
    if not account_id or not app_key:
        print("ERROR: CIN7_ACCOUNT_ID / CIN7_APPLICATION_KEY missing in .env")
        return 1

    headers = {
        "api-auth-accountid": account_id,
        "api-auth-applicationkey": app_key,
        "Accept": "application/json",
    }

    out_dir = Path("output/cin7_probe")
    out_dir.mkdir(parents=True, exist_ok=True)
    safe = sku.replace("/", "_")

    # Step 1: List call filtered by SKU. Some CIN7 endpoints return a
    # truncated record on /product list and need a /product?ID=... GET
    # for full detail. We try both.
    print(f"\n=== List endpoint /product?Sku={sku} ===")
    list_url = f"{BASE_URL}/product"
    try:
        r1 = requests.get(list_url, headers=headers,
                            params={"Sku": sku, "Limit": 1,
                                     "IncludeSuppliers": "true",
                                     "IncludeReorderLevels": "true"},
                            timeout=30)
        print(f"  status={r1.status_code} bytes={len(r1.text)}")
        if r1.status_code == 200:
            payload1 = r1.json()
            (out_dir / f"product_{safe}_list.json").write_text(
                json.dumps(payload1, indent=2)[:300000],
                encoding="utf-8")
            prods = payload1.get("Products") or []
            if prods:
                print(f"  Found {len(prods)} product(s) on list endpoint.")
                first = prods[0]
                product_id = first.get("ID") or first.get("ProductID")
                print(f"  ProductID = {product_id}")
            else:
                print("  No products found in list response.")
                product_id = None
        else:
            print(f"  Error body: {r1.text[:300]}")
            return 1
    except Exception as exc:
        print(f"  ERR {exc}")
        return 1

    # Step 2: Single product GET for richer detail (CIN7 often returns
    # nested arrays only on this endpoint, not on the list endpoint).
    # We also pass every plausible `Include*` parameter — some CIN7
    # endpoints gate nested arrays behind an explicit opt-in flag.
    if product_id:
        print(f"\n=== Single endpoint /product?ID={product_id} (with all Include flags) ===")
        try:
            r2 = requests.get(list_url, headers=headers,
                                params={
                                    "ID": product_id,
                                    "IncludeSuppliers": "true",
                                    "IncludeReorderLevels": "true",
                                    "IncludeAlternateProducts": "true",
                                    "IncludeAlternativeProducts": "true",
                                    "IncludeRelatedProducts": "true",
                                    "IncludeRelated": "true",
                                    "IncludeAlternates": "true",
                                    "IncludeAlternatives": "true",
                                    "IncludeReplacements": "true",
                                    "IncludeAll": "true",
                                },
                                timeout=30)
            print(f"  status={r2.status_code} bytes={len(r2.text)}")
            if r2.status_code == 200:
                payload2 = r2.json()
                (out_dir / f"product_{safe}_full.json").write_text(
                    json.dumps(payload2, indent=2)[:500000],
                    encoding="utf-8")
                # The single GET may return a wrapper or just the
                # product directly. Walk it.
                target = payload2
                # Common shapes
                if isinstance(payload2, dict):
                    for k in ("Products", "Product", "product"):
                        if k in payload2:
                            v = payload2[k]
                            target = (v[0] if isinstance(v, list) and v
                                      else v)
                            break
                # Walk and look for keyword hits
                print(f"\n  All TOP-LEVEL keys on the product:")
                if isinstance(target, dict):
                    for k in sorted(target.keys()):
                        val = target[k]
                        if isinstance(val, list):
                            preview = f"<list of {len(val)}>"
                        elif isinstance(val, dict):
                            preview = f"<dict, keys={list(val.keys())[:5]}>"
                        else:
                            preview = str(val)[:60]
                        print(f"    {k:<32} {preview}")

                print(f"\n  Keyword-matched paths (alt / related / replac / etc):")
                hits = []
                for path, value in walk(target):
                    if any(kw in path.lower() for kw in KEYWORDS):
                        hits.append((path, value))
                if not hits:
                    print("    (none — the field may not exist or "
                          "uses an unexpected name)")
                else:
                    for path, value in hits[:50]:
                        if isinstance(value, (str, int, float, bool)):
                            preview = str(value)[:120]
                        else:
                            preview = json.dumps(value)[:120]
                        print(f"    {path:<55} {preview}")
            else:
                print(f"  Error body: {r2.text[:300]}")
        except Exception as exc:
            print(f"  ERR {exc}")

    print(f"\nFiles saved to {out_dir}/")
    return 0


if __name__ == "__main__":
    sys.exit(main())
