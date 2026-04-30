"""
fix_db_retiring_sku.py
======================
Updates one entry in db.sku_migrations to use the correct retiring_sku
that matches CIN7's actual SKU. Use this when our DB had a wrong value
that didn't match CIN7 — typically a typo or shorter form.

Usage
-----
    .venv\\Scripts\\python fix_db_retiring_sku.py \\
        --from "LED-18.046" --to "LED-18.046-0609"
    # Dry-run by default. Add --apply to commit.
"""

from __future__ import annotations

import argparse
import sys

import db


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Rename a retiring_sku in db.sku_migrations")
    parser.add_argument("--from", dest="src", required=True,
                          help="Current (wrong) retiring_sku in DB")
    parser.add_argument("--to", dest="dst", required=True,
                          help="Correct retiring_sku that matches CIN7")
    parser.add_argument("--apply", action="store_true",
                          help="Commit the change")
    args = parser.parse_args()

    if args.src == args.dst:
        print("Source and destination identical. Nothing to do.")
        return 0

    with db.connect() as c:
        # Look up the existing row
        row = c.execute(
            "SELECT * FROM sku_migrations WHERE retiring_sku = ?",
            (args.src,)).fetchone()
        if not row:
            print(f"No row in sku_migrations with retiring_sku = "
                   f"'{args.src}'. Nothing to do.")
            return 0

        # Check no row already exists for the new key
        existing = c.execute(
            "SELECT * FROM sku_migrations WHERE retiring_sku = ?",
            (args.dst,)).fetchone()
        if existing:
            print(f"WARNING: row already exists with retiring_sku = "
                   f"'{args.dst}'. Manual reconciliation needed:")
            print(f"  current : {dict(existing)}")
            print(f"  pending : {dict(row)}")
            return 1

        print(f"Will rename retiring_sku '{args.src}' -> '{args.dst}'")
        print(f"  current row: successor={row['successor_sku']}, "
               f"share={row['share_pct']}%, set_by={row['set_by']}")

        if not args.apply:
            print("\n(Dry-run. Re-run with --apply to commit.)")
            return 0

        # SQLite: change primary key by delete + insert (avoid
        # ON CONFLICT issues with the existing row's pk).
        c.execute("DELETE FROM sku_migrations WHERE retiring_sku = ?",
                   (args.src,))
        c.execute(
            """
            INSERT INTO sku_migrations
                (retiring_sku, successor_sku, share_pct, set_by, note)
            VALUES (?, ?, ?, ?, ?)
            """,
            (args.dst, row["successor_sku"], row["share_pct"],
             row["set_by"], (row["note"] or "")
             + f"  [retiring_sku corrected from '{args.src}' to '{args.dst}']"),
        )
        c.execute(
            "INSERT INTO audit_log (event, actor, target, detail) "
            "VALUES (?, ?, ?, ?)",
            ("migration.rename_retiring", "fix_script", args.src,
             f"-> {args.dst}"),
        )
    print(f"Done. retiring_sku renamed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
