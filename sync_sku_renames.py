"""
sync_sku_renames.py
===================
CIN7 is the source of truth for SKUs. When the team renames a SKU in
CIN7 (e.g., adds a catalog number prefix or version suffix), our local
DB references can go stale. This script detects renames between two
product sync snapshots and propagates them through our DB tables.

Detection:
  Compare the two most recent output/products_*.csv files. Match rows
  by ProductID. Any ProductID where the SKU value differs is a rename.

Propagation (these tables get updated):
  - sku_migrations.retiring_sku
  - sku_migrations.successor_sku
  - sku_supplier_overrides.sku
  - family_critical_components.component_sku
  - sku_policy_overrides.sku
  - sku_pack_settings.sku
  - notes.sku
  - flags.sku
  - audit_log gets a record per rename

What's NOT updated (intentionally):
  - Historical CSVs in output/ — those get regenerated on next sync
  - sale_lines / purchase_lines — historical records keep their
    original SKU (CIN7 keeps it that way too; our reports follow)

Recommended cadence:
  Run after every `cin7_sync.py products`. Eventually we'll wire it
  into the sync itself.

Usage
-----
    .venv\\Scripts\\python sync_sku_renames.py              # dry-run
    .venv\\Scripts\\python sync_sku_renames.py --apply       # commit

Edge cases:
  - If the new SKU already exists in our DB tables (collision), we
    flag it and skip — manual review needed.
  - If a SKU appears in OLD csv but not NEW, that's a deletion (or the
    product was removed from CIN7). We flag but don't auto-clean.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

import db


# OUTPUT_DIR follows DATA_DIR env var (set to /data on Render).
from data_paths import OUTPUT_DIR  # noqa: E402


def _two_latest_products_csvs() -> tuple[Path | None, Path | None]:
    files = sorted(
        OUTPUT_DIR.glob("products_*.csv"),
        key=lambda p: p.stat().st_mtime)
    if len(files) < 2:
        return None, None
    return files[-2], files[-1]   # (older, newer)


def _detect_renames(old_csv: Path, new_csv: Path) -> list[dict]:
    """Returns list of {ProductID, OldSKU, NewSKU, Name} for renamed
    products."""
    old = pd.read_csv(old_csv, low_memory=False)
    new = pd.read_csv(new_csv, low_memory=False)
    old_idx = {str(r["ID"]): str(r["SKU"])
                for _, r in old.iterrows()
                if pd.notna(r.get("ID")) and pd.notna(r.get("SKU"))}
    renames = []
    for _, r in new.iterrows():
        pid = str(r.get("ID") or "")
        new_sku = str(r.get("SKU") or "")
        if not pid or not new_sku:
            continue
        old_sku = old_idx.get(pid)
        if old_sku and old_sku != new_sku:
            renames.append({
                "ProductID": pid,
                "OldSKU": old_sku,
                "NewSKU": new_sku,
                "Name": str(r.get("Name") or "")[:80],
            })
    return renames


def _propagate_one(c, old_sku: str, new_sku: str) -> dict:
    """Update DB tables that reference old_sku to use new_sku.
    Returns count of rows touched per table."""
    counts: dict = {}

    # Tables with single-column SKU references — straightforward UPDATE.
    # Each is wrapped in a check for collision (target row already
    # exists with new_sku) so we don't violate primary key constraints.
    SIMPLE = [
        # (table, sku_col, pk_col_or_uniq)
        ("sku_supplier_overrides", "sku", "sku"),
        ("sku_policy_overrides", "sku", "sku"),
        ("sku_pack_settings", "sku", "sku"),
        ("notes", "sku", None),  # no pk on sku
        ("flags", "sku", None),
    ]
    for table, col, pk in SIMPLE:
        if pk:
            existing = c.execute(
                f"SELECT COUNT(*) AS n FROM {table} WHERE {col} = ?",
                (new_sku,)).fetchone()
            if existing["n"] > 0:
                counts[f"{table}.{col} (collision)"] = -1
                continue
        n = c.execute(
            f"UPDATE {table} SET {col} = ? WHERE {col} = ?",
            (new_sku, old_sku)).rowcount
        if n:
            counts[f"{table}.{col}"] = n

    # sku_migrations: both retiring_sku and successor_sku may reference.
    # retiring_sku is the primary key — collision check.
    existing_ret = c.execute(
        "SELECT COUNT(*) AS n FROM sku_migrations WHERE retiring_sku = ?",
        (new_sku,)).fetchone()["n"]
    if existing_ret > 0:
        counts["sku_migrations.retiring_sku (collision)"] = -1
    else:
        n = c.execute(
            "UPDATE sku_migrations SET retiring_sku = ? WHERE retiring_sku = ?",
            (new_sku, old_sku)).rowcount
        if n:
            counts["sku_migrations.retiring_sku"] = n
    n = c.execute(
        "UPDATE sku_migrations SET successor_sku = ? WHERE successor_sku = ?",
        (new_sku, old_sku)).rowcount
    if n:
        counts["sku_migrations.successor_sku"] = n

    # family_critical_components.component_sku
    n = c.execute(
        "UPDATE family_critical_components "
        "SET component_sku = ? WHERE component_sku = ?",
        (new_sku, old_sku)).rowcount
    if n:
        counts["family_critical_components.component_sku"] = n

    return counts


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Sync CIN7 SKU renames into local DB")
    parser.add_argument(
        "--apply", action="store_true",
        help="Actually update DB. Without this we dry-run.")
    parser.add_argument(
        "--old", default=None,
        help="Older products CSV. Default: 2nd-most-recent.")
    parser.add_argument(
        "--new", default=None,
        help="Newer products CSV. Default: most recent.")
    args = parser.parse_args()

    if args.old and args.new:
        old_csv = Path(args.old)
        new_csv = Path(args.new)
    else:
        old_csv, new_csv = _two_latest_products_csvs()
    if not old_csv or not new_csv:
        print("ERROR: need at least 2 products_*.csv files in output/. "
              "Pass --old / --new explicitly or run "
              "cin7_sync.py products at least twice.")
        return 1

    print(f"Old: {old_csv.name}")
    print(f"New: {new_csv.name}")

    renames = _detect_renames(old_csv, new_csv)
    print(f"\nDetected {len(renames)} SKU rename(s):")
    if not renames:
        print("  (no renames)")
        return 0
    for r in renames:
        print(f"  {r['OldSKU']:<40} → {r['NewSKU']}   {r['Name'][:40]}")

    if not args.apply:
        print(f"\n(Dry-run.) Would propagate to DB tables. "
               f"Re-run with --apply to commit.")
        return 0

    print(f"\nPropagating to DB...")
    total_touched = 0
    collisions = []
    with db.connect() as c:
        for r in renames:
            old, new = r["OldSKU"], r["NewSKU"]
            counts = _propagate_one(c, old, new)
            if counts:
                summary = "; ".join(f"{k}={v}" for k, v in counts.items())
                print(f"  {old} → {new}: {summary}")
                total_touched += sum(v for v in counts.values() if v > 0)
                for k, v in counts.items():
                    if v < 0:
                        collisions.append((old, new, k))
            # Audit
            c.execute(
                "INSERT INTO audit_log (event, actor, target, detail) "
                "VALUES (?, ?, ?, ?)",
                ("sku_rename.propagated", "sync_sku_renames",
                 old, f"-> {new}; touched: {counts}"),
            )

    print(f"\nDone. Total rows touched: {total_touched}")
    if collisions:
        print(f"\n⚠  {len(collisions)} collision(s) — the new SKU "
               "already exists in the target table. Manual review needed:")
        for old, new, where in collisions:
            print(f"    {old} → {new}  ({where})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
