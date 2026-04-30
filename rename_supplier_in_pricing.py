"""
rename_supplier_in_pricing.py
=============================
One-shot utility to rename a supplier across the three Supplier
Pricing tables. We seeded Reeves entries with supplier='Reeves' but
the actual CIN7 supplier name is the longer 'Reeves Extruded
Products, Inc - ...'. The Tier Opportunities expander filters on
exact match — so the seed needs to use the same string CIN7 uses.

Usage
-----
    .venv\\Scripts\\python rename_supplier_in_pricing.py \\
        --from "Reeves" --to "Reeves Extruded Products, Inc - ..."

Updates 3 tables:
  - family_color_pricing
  - family_setup_fees
  - family_pricing_rules
"""

from __future__ import annotations

import argparse
import sys

import db


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Rename supplier in pricing tables")
    parser.add_argument(
        "--from", dest="src", required=True,
        help="Current supplier value (e.g. 'Reeves')")
    parser.add_argument(
        "--to", dest="dst", required=True,
        help="New supplier value matching CIN7 exactly "
             "(e.g. 'Reeves Extruded Products, Inc - LLC')")
    parser.add_argument(
        "--apply", action="store_true",
        help="Actually run the UPDATEs. Without this we dry-run.")
    args = parser.parse_args()

    if args.src == args.dst:
        print("Source and destination are identical. Nothing to do.")
        return 0

    with db.connect() as c:
        # Count rows that would be affected
        n1 = c.execute(
            "SELECT COUNT(*) AS n FROM family_color_pricing "
            "WHERE supplier = ?", (args.src,)).fetchone()["n"]
        n2 = c.execute(
            "SELECT COUNT(*) AS n FROM family_setup_fees "
            "WHERE supplier = ?", (args.src,)).fetchone()["n"]
        n3 = c.execute(
            "SELECT COUNT(*) AS n FROM family_pricing_rules "
            "WHERE supplier = ?", (args.src,)).fetchone()["n"]

        print(f"Rows that match supplier='{args.src}':")
        print(f"  family_color_pricing : {n1}")
        print(f"  family_setup_fees    : {n2}")
        print(f"  family_pricing_rules : {n3}")

        if (n1 + n2 + n3) == 0:
            print(f"\nNo matching rows. Try running with the exact "
                   f"current supplier value (case-sensitive).")
            return 0

        if not args.apply:
            print(f"\n(Dry-run.) Would rename to: '{args.dst}'")
            print("Re-run with --apply to commit.")
            return 0

        c.execute(
            "UPDATE family_color_pricing SET supplier = ? "
            "WHERE supplier = ?", (args.dst, args.src))
        c.execute(
            "UPDATE family_setup_fees SET supplier = ? "
            "WHERE supplier = ?", (args.dst, args.src))
        c.execute(
            "UPDATE family_pricing_rules SET supplier = ? "
            "WHERE supplier = ?", (args.dst, args.src))
        c.execute(
            "INSERT INTO audit_log (event, actor, target, detail) "
            "VALUES (?, ?, ?, ?)",
            ("supplier_pricing.rename", "rename_script",
             args.src, f"-> {args.dst} "
             f"(family_color_pricing={n1}, "
             f"family_setup_fees={n2}, family_pricing_rules={n3})"))

    print(f"\nDone. Renamed '{args.src}' -> '{args.dst}' across "
           f"{n1 + n2 + n3} rows. Refresh the Streamlit app to "
           f"pick up the change.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
