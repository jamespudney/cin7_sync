"""
audit_migrations_to_engine.py
=============================
Diagnostic: trace whether predecessor SKUs' historical sales are
actually flowing through to their successors' reorder math.

Specifically focused on the SMOKIES38/CASCADE38 -> SIERRA38 and
SMOKIES65 -> SIERRA65 lineage where the buyer expects to see rolled-
up demand showing up in Reeves' PO.

Outputs a textual report covering:
  1. Migration records in db.sku_migrations (predecessor → successor)
  2. Each predecessor's 12mo units (from latest sale_lines CSV)
  3. Per-successor: how many predecessors point at it, what total
     12mo units are inherited
  4. Successor's OnHand and Supplier from products + stock data
  5. Whether the successor would be flagged for reorder (rough check
     based on OnHand vs cumulative inherited demand)

Run this AFTER products + stock have been synced and after the
ip_import_migrations.py / cin7_push_migrations.py flow.

Usage
-----
    .venv\\Scripts\\python audit_migrations_to_engine.py
    .venv\\Scripts\\python audit_migrations_to_engine.py --family SIERRA38
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

import db


OUTPUT_DIR = Path("output")


def _latest(prefix: str) -> Path | None:
    files = sorted(OUTPUT_DIR.glob(f"{prefix}_*.csv"))
    return files[-1] if files else None


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Audit migrations → engine flow")
    parser.add_argument(
        "--family", default=None,
        help="Filter to successors with this substring in SKU "
             "(e.g. SIERRA38 or SIERRA65)")
    args = parser.parse_args()

    # Load data
    products_path = _latest("products")
    sale_lines_path = (_latest("sale_lines_last_730d")
                        or _latest("sale_lines_last_365d")
                        or _latest("sale_lines"))
    stock_path = _latest("stock_on_hand")

    if not products_path:
        print("ERROR: no products CSV. Run cin7_sync.py products first.")
        return 1
    if not sale_lines_path:
        print("ERROR: no sale_lines CSV. Run cin7_sync.py salelines first.")
        return 1

    print(f"Products    : {products_path.name}")
    print(f"Sale lines  : {sale_lines_path.name}")
    print(f"Stock       : {stock_path.name if stock_path else '(none)'}")

    products = pd.read_csv(products_path, low_memory=False)
    sl = pd.read_csv(sale_lines_path, low_memory=False)
    stock = pd.read_csv(stock_path, low_memory=False) if stock_path else pd.DataFrame()

    sl["InvoiceDate"] = pd.to_datetime(sl["InvoiceDate"], errors="coerce")
    sl["Quantity"] = pd.to_numeric(sl["Quantity"], errors="coerce").fillna(0)
    cutoff = pd.Timestamp.now() - pd.Timedelta(days=365)
    units_12mo = (sl[sl["InvoiceDate"] >= cutoff]
                   .groupby("SKU")["Quantity"].sum().to_dict())

    onhand_by_sku = {}
    if not stock.empty and "SKU" in stock.columns:
        onhand_by_sku = (
            pd.to_numeric(stock["OnHand"], errors="coerce")
            .groupby(stock["SKU"]).sum().to_dict())
    products_supplier_col = (
        "Suppliers" if "Suppliers" in products.columns else None)

    # Load migrations
    migs = [dict(m) for m in db.all_migrations()]
    print(f"\nTotal migrations in DB: {len(migs)}")

    # Index: successor → list of predecessor records
    by_successor: dict = {}
    for m in migs:
        s = str(m.get("successor_sku") or "")
        by_successor.setdefault(s, []).append(m)

    # Filter successors if --family was given
    successors = sorted(by_successor.keys())
    if args.family:
        successors = [s for s in successors if args.family in s]
        print(f"Filtered to successors containing '{args.family}': "
               f"{len(successors)} found")

    if not successors:
        print("No matching successors. Exiting.")
        return 0

    # Per-successor report
    print("\n" + "=" * 90)
    print(f"{'Successor SKU':<35} {'Preds':>5} {'Inh 12mo':>10} "
           f"{'Own 12mo':>10} {'OnHand':>10} {'Total eff':>10}")
    print("=" * 90)

    rows = []
    for succ in successors:
        preds = by_successor[succ]
        inherited = 0.0
        for p in preds:
            ret = str(p.get("retiring_sku") or "")
            share = float(p.get("share_pct") or 100.0) / 100.0
            ret_units = float(units_12mo.get(ret, 0))
            inherited += ret_units * share
        own = float(units_12mo.get(succ, 0))
        oh = float(onhand_by_sku.get(succ, 0))
        eff = own + inherited
        rows.append({
            "successor": succ,
            "preds": len(preds),
            "inherited": inherited,
            "own": own,
            "onhand": oh,
            "effective": eff,
        })
        print(f"{succ:<35} {len(preds):>5} "
               f"{inherited:>10.0f} {own:>10.0f} "
               f"{oh:>10.0f} {eff:>10.0f}")

    # Sub-report: predecessors with > 0 12mo units (the active source)
    print("\n" + "=" * 90)
    print(f"Predecessors with non-zero 12mo units (the ones that "
           f"actually feed migration_in):")
    print("=" * 90)

    active_preds = []
    for succ in successors:
        for p in by_successor[succ]:
            ret = str(p.get("retiring_sku") or "")
            ret_units = float(units_12mo.get(ret, 0))
            if ret_units > 0:
                active_preds.append({
                    "predecessor": ret,
                    "successor": succ,
                    "share_pct": float(p.get("share_pct") or 100),
                    "12mo_units": ret_units,
                    "OnHand_residual": float(onhand_by_sku.get(ret, 0)),
                })
    if active_preds:
        active_preds.sort(key=lambda r: -r["12mo_units"])
        for p in active_preds[:50]:
            print(
                f"  {p['predecessor']:<35} -> {p['successor']:<35} "
                f"  {p['12mo_units']:>8.0f} units  "
                f"  share={p['share_pct']:.0f}%  "
                f"  residual OnHand={p['OnHand_residual']:.0f}")
        if len(active_preds) > 50:
            print(f"  ... and {len(active_preds) - 50} more")
    else:
        print("  ⚠ No predecessor has 12mo sales!")
        print("  This is the most likely reason the engine isn't")
        print("  inflating successor demand. Either:")
        print("    (a) the predecessor SKUs aren't in your sale_lines data")
        print("        (have they sold in the last 365 days?)")
        print("    (b) the SKU in db.sku_migrations doesn't exactly match")
        print("        the SKU in sale_lines (case, spacing, hyphens)")

    # Quick supplier check for SIERRA successors
    print("\n" + "=" * 90)
    print("Successor supplier mapping (from CIN7 product master):")
    print("=" * 90)
    for r in rows:
        succ = r["successor"]
        prod_match = products[products["SKU"].astype(str) == succ]
        if prod_match.empty:
            print(f"  {succ:<35} (NOT in product master)")
            continue
        sup_field = ""
        if products_supplier_col:
            raw = prod_match.iloc[0].get(products_supplier_col)
            sup_field = str(raw)[:80] if pd.notna(raw) else ""
        print(f"  {succ:<35} suppliers: {sup_field[:80]}")

    print("\nDone. If predecessors have non-zero 12mo units AND they map")
    print("to successors that are in product master, the engine SHOULD")
    print("be inflating successor demand. Reload the Streamlit app and")
    print("look at the migrated_from / migrated_in columns on those SKUs")
    print("in the Ordering page to verify.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
