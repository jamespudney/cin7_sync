"""
debug_sale_shipping.py — pull one real CIN7 sale in detail and dump
every shipping-related field. Use this to verify the extractor is
reading the correct CIN7 field name for customer-paid shipping.

Run:  .venv\\Scripts\\python debug_sale_shipping.py
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

from cin7_sync import Cin7Client

load_dotenv()
OUTPUT_DIR = Path(__file__).resolve().parent / "output"


def main() -> None:
    # Pick the most recent sale from the 30d CSV — guaranteed to still
    # exist in CIN7.
    files = sorted(OUTPUT_DIR.glob("sales_last_30d_*.csv"))
    if not files:
        print("No sales_last_30d CSV found. Run sync first.")
        sys.exit(1)
    df = pd.read_csv(files[-1], low_memory=False)
    # Pick one with a non-null InvoiceNumber (so an invoice was issued)
    df = df.dropna(subset=["InvoiceNumber"])
    if df.empty:
        print("No invoiced sales found in 30d file.")
        sys.exit(1)
    sid = df.iloc[0]["SaleID"]
    order_num = df.iloc[0]["OrderNumber"]
    print(f"Picking sale: {sid}  (order {order_num})")
    print()

    client = Cin7Client(
        os.getenv("CIN7_ACCOUNT_ID", ""),
        os.getenv("CIN7_APPLICATION_KEY", ""),
        rate_seconds=1.0,
    )
    detail = client.get("sale", {"ID": sid})

    # Print top-level keys
    print(f"=== Top-level keys of /sale response ===")
    for k in sorted(detail.keys()):
        print(f"  {k}")
    print()

    # Look for shipping at the top level
    ship_top = {k: v for k, v in detail.items()
                if any(w in k.lower() for w in
                         ("ship", "freight", "charge", "fee", "handling"))}
    print(f"=== Top-level shipping-ish fields ===")
    for k, v in ship_top.items():
        print(f"  {k}: {v}")
    print()

    # Drill into Invoices
    print(f"=== Invoices[].* keys ===")
    invoices = detail.get("Invoices") or []
    if not invoices:
        print("  (none — this sale has no invoice yet)")
    else:
        for i, inv in enumerate(invoices):
            if not isinstance(inv, dict):
                continue
            print(f"  Invoice #{i}:")
            for k in sorted(inv.keys()):
                v = inv.get(k)
                if k == "Lines":
                    print(f"    Lines: [array with {len(v or [])} entries]")
                elif k == "AdditionalCharges":
                    print(f"    AdditionalCharges: "
                          f"[array with {len(v or [])} entries]")
                    if v:
                        for j, c in enumerate(v[:3]):
                            print(f"      charge[{j}]: {c}")
                else:
                    # Truncate long values
                    s = str(v)
                    if len(s) > 80:
                        s = s[:80] + "..."
                    print(f"    {k}: {s}")
            print()
            ship_in_inv = {k: v for k, v in inv.items()
                             if any(w in k.lower() for w in
                                    ("ship", "freight", "charge",
                                     "fee", "handling"))}
            print(f"    >> Shipping-ish fields in this invoice:")
            for k, v in ship_in_inv.items():
                print(f"       {k}: {v}")
            print()

    # Also drill into Order
    order = detail.get("Order") or {}
    if isinstance(order, dict):
        print(f"=== Order.* keys ===")
        ship_in_order = {k: v for k, v in order.items()
                           if any(w in k.lower() for w in
                                  ("ship", "freight", "charge",
                                   "fee", "handling"))}
        for k, v in ship_in_order.items():
            print(f"  {k}: {v}")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"ERROR: {type(exc).__name__}: {exc}")
        sys.exit(1)
