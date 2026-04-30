"""
audit_sku_demand_chain.py
=========================
For one master SKU (e.g., LED-SIERRA38-W-3), walk the full demand
chain that should feed its reorder math, showing each contributing
piece:

  1. Direct migrations TO this SKU (predecessor → this)
  2. BOM children that consume this as a component → demand rollup
  3. For each BOM child, its OWN sales + its OWN migration_in
     (after the v2.22 fix, both should propagate up)
  4. The final cumulative effective_units_12mo expected

Use to verify the engine is seeing the full picture for a master SKU.

Usage
-----
    .venv\\Scripts\\python audit_sku_demand_chain.py LED-SIERRA38-W-3
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

import db


OUTPUT_DIR = Path("output")


def _latest(prefix: str):
    files = sorted(OUTPUT_DIR.glob(f"{prefix}_*.csv"))
    return files[-1] if files else None


def main() -> int:
    if len(sys.argv) < 2:
        print("Usage: audit_sku_demand_chain.py <SKU>")
        return 1
    target_sku = sys.argv[1]

    # Load data
    sl_path = (_latest("sale_lines_last_730d")
                or _latest("sale_lines_last_365d") or _latest("sale_lines"))
    boms_path = _latest("boms")
    products_path = _latest("products")
    stock_path = _latest("stock_on_hand")
    if not sl_path or not boms_path or not products_path:
        print("ERROR: missing CSVs (sale_lines / boms / products).")
        return 1

    sl = pd.read_csv(sl_path, low_memory=False)
    sl["InvoiceDate"] = pd.to_datetime(sl["InvoiceDate"], errors="coerce")
    sl["Quantity"] = pd.to_numeric(sl["Quantity"], errors="coerce").fillna(0)
    cutoff = pd.Timestamp.now() - pd.Timedelta(days=365)
    sl_12mo = sl[sl["InvoiceDate"] >= cutoff]

    units_12mo_by_sku = sl_12mo.groupby("SKU")["Quantity"].sum().to_dict()

    boms = pd.read_csv(boms_path, low_memory=False)
    products = pd.read_csv(products_path, low_memory=False)
    stock = pd.read_csv(stock_path, low_memory=False) if stock_path else pd.DataFrame()

    onhand_by_sku = {}
    if not stock.empty and "SKU" in stock.columns:
        onhand_by_sku = (
            pd.to_numeric(stock["OnHand"], errors="coerce")
            .groupby(stock["SKU"]).sum().to_dict())

    # Migration map
    migs = [dict(m) for m in db.all_migrations()]
    by_succ = {}
    for m in migs:
        by_succ.setdefault(str(m.get("successor_sku") or ""), []).append(m)

    # BOM: which SKUs consume target_sku as a component?
    # In our BOM CSV: AssemblySKU consumes ComponentSKU at Quantity ratio
    consumers = boms[boms["ComponentSKU"].astype(str) == target_sku].copy()
    if consumers.empty:
        print(f"\n{target_sku}: no BOM children found (no SKU lists this "
               f"as a component). All demand must be direct + migration.")
    else:
        consumers["Quantity"] = pd.to_numeric(consumers["Quantity"],
                                                errors="coerce").fillna(0)

    print(f"\n{'=' * 80}")
    print(f"DEMAND CHAIN AUDIT — {target_sku}")
    print(f"{'=' * 80}\n")

    # 1. Direct sales
    own_12mo = float(units_12mo_by_sku.get(target_sku, 0))
    own_oh = float(onhand_by_sku.get(target_sku, 0))
    print(f"[1] Direct 12mo sales       : {own_12mo:>10.0f} units")
    print(f"    Current OnHand          : {own_oh:>10.0f} units")

    # 2. Direct migrations TO this SKU (this SKU is the successor)
    direct_preds = by_succ.get(target_sku, [])
    direct_migrated = 0.0
    if direct_preds:
        print(f"\n[2] Direct predecessors ({len(direct_preds)}):")
        for m in direct_preds:
            ret = str(m.get("retiring_sku") or "")
            share = float(m.get("share_pct") or 100) / 100
            ret_12mo = float(units_12mo_by_sku.get(ret, 0))
            ret_oh = float(onhand_by_sku.get(ret, 0))
            inflow = ret_12mo * share
            direct_migrated += inflow
            print(f"    {ret:<35} 12mo={ret_12mo:>7.0f} × {share:.0%} = "
                   f"{inflow:>7.0f}   (residual OnHand={ret_oh:.0f})")
        print(f"    {'TOTAL direct migration_in':<35} "
               f"{direct_migrated:>20.0f}")
    else:
        print(f"\n[2] No direct predecessors (this SKU has no migration_in)")

    # 3. BOM children that consume this SKU
    if not consumers.empty:
        print(f"\n[3] BOM children consuming {target_sku} ({len(consumers)} found):")
        total_rollup = 0.0
        for _, c in consumers.iterrows():
            child_sku = str(c["AssemblySKU"])
            qty_per = float(c["Quantity"])
            child_12mo = float(units_12mo_by_sku.get(child_sku, 0))
            # Child's own migration_in
            child_preds = by_succ.get(child_sku, [])
            child_mig_in = 0.0
            for m in child_preds:
                ret = str(m.get("retiring_sku") or "")
                share = float(m.get("share_pct") or 100) / 100
                child_mig_in += float(units_12mo_by_sku.get(ret, 0)) * share
            child_total = child_12mo + child_mig_in  # post-v2.22 fix
            consumption = child_total * qty_per
            total_rollup += consumption
            print(f"    {child_sku:<35} qty/per={qty_per:.4f}")
            print(f"      own 12mo                : {child_12mo:>7.0f}")
            print(f"      + migration_in (preds)  : {child_mig_in:>7.0f}")
            if child_preds:
                for m in child_preds:
                    ret = str(m.get("retiring_sku") or "")
                    share = float(m.get("share_pct") or 100) / 100
                    units = float(units_12mo_by_sku.get(ret, 0)) * share
                    print(f"        ↳ from {ret:<28} "
                           f"{units:>6.0f} units")
            print(f"      = effective demand      : {child_total:>7.0f}")
            print(f"      × qty/per = rollup      : "
                   f"{consumption:>10.1f} (consumed from {target_sku})")
        print(f"    {'TOTAL tube_rollup_in':<35} "
               f"{total_rollup:>20.1f}")
    else:
        total_rollup = 0.0

    # 4. Expected effective_units_12mo for the master
    expected_eff = own_12mo + direct_migrated + total_rollup
    print(f"\n[4] Expected effective_units_12mo for {target_sku}:")
    print(f"    own_12mo                  : {own_12mo:>10.0f}")
    print(f"    + direct migrated_in      : {direct_migrated:>10.0f}")
    print(f"    + tube_rollup_in          : {total_rollup:>10.1f}")
    print(f"    {'─' * 50}")
    print(f"    {'Expected total':<26}: {expected_eff:>10.1f}")

    if expected_eff > 0:
        avg_daily = expected_eff / 365
        print(f"\n    Implied avg_daily         : {avg_daily:.3f}")
        print(f"    Implied 12mo at 35d LT    : "
               f"{avg_daily * (35 + 35 * 0.3 + 30):.0f} target_stock approx")

    print(f"\n{'=' * 80}")
    print(f"How to interpret:")
    print(f"  - The 'Expected total' should match what the engine shows in")
    print(f"    {target_sku}'s `effective_units_12mo` column on the Ordering")
    print(f"    page (or in the drill-down's debug section).")
    print(f"  - If the engine's number is LOWER than this, something in the")
    print(f"    rollup chain is broken. Compare row by row to find the gap.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
