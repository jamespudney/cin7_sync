"""Pull one known BOM product in detail and dump the
BillOfMaterialsProducts structure — so we can see the exact field name
CIN7 uses for the component (might be 'SKU', 'ComponentSKU', 'ProductID',
or nested).

Run:  .venv\\Scripts\\python debug_bom.py
"""
from __future__ import annotations
import json, os, sys
from dotenv import load_dotenv
from cin7_sync import Cin7Client

load_dotenv()


def main() -> None:
    client = Cin7Client(
        os.getenv("CIN7_ACCOUNT_ID", ""),
        os.getenv("CIN7_APPLICATION_KEY", ""),
        rate_seconds=1.0,
    )

    # Find the product ID for LED-TSB2835-300-24-6000-0305
    print("Looking up LED-TSB2835-300-24-6000-0305...")
    prods = client.get(
        "product",
        params={"Sku": "LED-TSB2835-300-24-6000-0305"},
    )
    pid = None
    plist = prods.get("Products") or []
    if plist:
        pid = plist[0].get("ID")
    if not pid:
        print("Could not find ID — exiting.")
        sys.exit(1)
    print(f"ID: {pid}")

    detail = client.get(
        "product",
        params={
            "ID": pid,
            "IncludeBOM": "true",
            "IncludeSuppliers": "true",
        },
    )
    rec = detail
    if isinstance(detail, dict) and "Products" in detail:
        ps = detail.get("Products") or []
        if ps:
            rec = ps[0]

    print()
    print("=== Top-level keys of product detail ===")
    for k in sorted(rec.keys()):
        print(f"  {k}")

    print()
    print("=== BillOfMaterialsProducts content (raw) ===")
    bom = rec.get("BillOfMaterialsProducts")
    print(json.dumps(bom, indent=2, default=str))


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"ERROR: {type(exc).__name__}: {exc}")
        sys.exit(1)
