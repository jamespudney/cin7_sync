"""
audit_all_bare_tubes.py
=======================
Walk every bare-tube master SKU we order from suppliers and verify
the complete demand chain feeding it: direct migrations + BOM rollup
from all child variants (each child includes its own migration_in).

The chain we expect:

    [Predecessor SKU]    (e.g. SMOKIES38-W-MP-2390)
            │
            │ migration (sku_migrations table)
            ▼
    [Successor child]    (e.g. SIERRA38-W-MP-2390)
            │
            │ child's own sales + child's migration_in
            ▼
    [BOM rollup × qty_per]
            ▼
    [Bare-tube master]   (e.g. SIERRA38-W-3 — what Reeves ships)

Reports per bare tube:
    - own 12mo sales
    - direct migration_in (predecessors of THIS bare tube)
    - tube_rollup_in (sum across all BOM children, each enriched with
      its own migration_in)
    - final expected effective_units_12mo
    - whether engine's view matches (a sanity flag)

Usage
-----
    .venv\\Scripts\\python audit_all_bare_tubes.py
    .venv\\Scripts\\python audit_all_bare_tubes.py --family SIERRA38
    .venv\\Scripts\\python audit_all_bare_tubes.py --supplier Reeves
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

import db


OUTPUT_DIR = Path("output")


def _latest(prefix: str):
    files = sorted(OUTPUT_DIR.glob(f"{prefix}_*.csv"))
    return files[-1] if files else None


def _is_bare_tube(sku: str) -> bool:
    """Heuristic: a bare-tube master is a SIERRA/SMOKIES/CASCADE-pattern
    SKU that does NOT have '-MP-' in it (meaning it's not the assembly
    with mounting plate). These are the SKUs ordered directly from
    Reeves / extruders."""
    s = sku.upper()
    if "MP-" in s:
        return False
    # Specifically interested in SIERRA today (active line); add others
    # if the user wants broader coverage.
    return "SIERRA" in s


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Audit all bare-tube demand chains")
    parser.add_argument(
        "--family", default=None,
        help="Filter to bare tubes containing this substring "
             "(e.g. SIERRA38)")
    parser.add_argument(
        "--supplier", default=None,
        help="Filter to bare tubes whose CIN7 product master lists "
             "this supplier name (substring match, case-insensitive)")
    parser.add_argument(
        "--min-eff", type=float, default=0.0,
        help="Only show rows with expected effective_units >= this. "
             "Default 0 = show all.")
    args = parser.parse_args()

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
    units_12mo = (sl[sl["InvoiceDate"] >= cutoff]
                   .groupby("SKU")["Quantity"].sum().to_dict())

    boms = pd.read_csv(boms_path, low_memory=False)
    boms["Quantity"] = pd.to_numeric(boms["Quantity"],
                                       errors="coerce").fillna(0)
    products = pd.read_csv(products_path, low_memory=False)
    stock = pd.read_csv(stock_path, low_memory=False) if stock_path else pd.DataFrame()

    onhand_by_sku = {}
    if not stock.empty and "SKU" in stock.columns:
        onhand_by_sku = (
            pd.to_numeric(stock["OnHand"], errors="coerce")
            .groupby(stock["SKU"]).sum().to_dict())

    # Migrations indexed by successor
    migs = [dict(m) for m in db.all_migrations()]
    by_succ: dict = {}
    for m in migs:
        by_succ.setdefault(str(m.get("successor_sku") or ""), []).append(m)

    # All bare-tube SKUs from product master
    all_skus = products["SKU"].astype(str).tolist()
    bare_tubes = [s for s in all_skus if _is_bare_tube(s)]
    if args.family:
        bare_tubes = [s for s in bare_tubes if args.family in s]

    # Optional supplier filter
    if args.supplier:
        sup_lower = args.supplier.lower()
        suppliers_col = (
            "Suppliers" if "Suppliers" in products.columns else None)
        if suppliers_col:
            ok = set()
            for _, row in products.iterrows():
                sk = str(row.get("SKU") or "")
                sup_raw = str(row.get(suppliers_col) or "").lower()
                if sup_lower in sup_raw:
                    ok.add(sk)
            bare_tubes = [s for s in bare_tubes if s in ok]

    if not bare_tubes:
        print("No bare tubes match filter. Exiting.")
        return 0

    # Build the rollup report
    print(f"\nAuditing {len(bare_tubes)} bare-tube SKUs...\n")
    print(f"{'Bare tube SKU':<32} {'Own':>5} {'DirMig':>7} "
           f"{'Children':>9} {'ChMig':>7} {'Rollup':>8} "
           f"{'Eff':>8} {'OnHand':>7}")
    print("=" * 92)

    rows = []
    for tube in sorted(bare_tubes):
        own_12mo = float(units_12mo.get(tube, 0))
        oh = float(onhand_by_sku.get(tube, 0))

        # Direct migrations TO this bare tube
        direct_preds = by_succ.get(tube, [])
        direct_mig = sum(
            float(units_12mo.get(str(p.get("retiring_sku") or ""), 0))
            * float(p.get("share_pct") or 100) / 100
            for p in direct_preds)

        # BOM children that consume this bare tube
        consumers = boms[boms["ComponentSKU"].astype(str) == tube]
        n_children = len(consumers)
        child_mig_total = 0.0
        rollup_total = 0.0
        for _, c in consumers.iterrows():
            child_sku = str(c["AssemblySKU"])
            qty_per = float(c["Quantity"])
            child_own = float(units_12mo.get(child_sku, 0))
            # Child's own migration_in
            child_mig_in = sum(
                float(units_12mo.get(str(p.get("retiring_sku") or ""), 0))
                * float(p.get("share_pct") or 100) / 100
                for p in by_succ.get(child_sku, []))
            child_mig_total += child_mig_in
            child_eff = child_own + child_mig_in
            rollup_total += child_eff * qty_per

        eff = own_12mo + direct_mig + rollup_total
        if eff < args.min_eff:
            continue

        print(f"{tube:<32} {own_12mo:>5.0f} {direct_mig:>7.0f} "
               f"{n_children:>9} {child_mig_total:>7.0f} "
               f"{rollup_total:>8.1f} {eff:>8.1f} {oh:>7.0f}")
        rows.append({
            "sku": tube,
            "own": own_12mo,
            "direct_mig": direct_mig,
            "n_children": n_children,
            "child_mig": child_mig_total,
            "rollup": rollup_total,
            "eff": eff,
            "onhand": oh,
        })

    print("=" * 92)
    print("\nLegend:")
    print("  Own       = bare tube's own 12mo sales (usually 0 — "
           "they're consumed via BOM, not sold direct)")
    print("  DirMig    = predecessors' 12mo × share, migrating directly "
           "to this bare tube")
    print("  Children  = number of BOM children consuming this tube")
    print("  ChMig     = sum of children's migration_in (post v2.22 fix, "
           "this bumps rollup)")
    print("  Rollup    = sum across children: (child_own + child_mig) "
           "× qty_per")
    print("  Eff       = Own + DirMig + Rollup → expected "
           "effective_units_12mo")
    print("  OnHand    = current stock")

    print(f"\nTotal expected effective demand across all bare tubes: "
           f"{sum(r['eff'] for r in rows):.0f} units")
    if rows:
        avg_eff = sum(r["eff"] for r in rows) / len(rows)
        print(f"Average per tube: {avg_eff:.1f}")
    print()

    # Highlight any tube where rollup includes migration but engine
    # might not be reflecting it (a quick sanity prompt)
    flagged = [r for r in rows if r["child_mig"] > 0 and r["rollup"] > 0]
    if flagged:
        print(f"⚠  {len(flagged)} tubes have BOM children with non-zero "
               f"migration_in. Verify these in the Ordering page show "
               f"the inflated effective demand. If `Suggest` looks low "
               f"vs the Eff column above, the v2.22 fix isn't being "
               f"applied (likely Streamlit cache — restart the app).")

    return 0


if __name__ == "__main__":
    sys.exit(main())
