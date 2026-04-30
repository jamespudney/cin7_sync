"""
ip_import_migrations.py
=======================
One-shot import: take the IP "Combine sales/stock" relationships from
the latest ip_alternates_*.csv and write them into the local migration
DB (sku_migrations table) via db.set_migration().

After this runs, the existing migration machinery picks them up
automatically:
  - Section 7 redirect in render_demand_breakdown shows the successor
    when a predecessor SKU is opened.
  - Migration forecast page rolls predecessor demand into successors.
  - migrated_from columns in master rows show what each successor
    inherited.

Mapping between IP CSV columns and db.set_migration arguments:

    ip_alternates_*.csv          db.set_migration()
    -----------------------      -------------------------------------
    AlternativeSKU            ⇒  retiring_sku    (old / phased-out)
    MasterSKU                 ⇒  successor_sku   (new / active)
    Percent                   ⇒  share_pct       (% of demand merged)
    Source ("user")           ⇒  noted in the saved note field
    AlternativeTitle          ⇒  noted in the saved note field

Usage
-----
    .venv\\Scripts\\python ip_import_migrations.py            # DRY-RUN: shows what it would do
    .venv\\Scripts\\python ip_import_migrations.py --apply    # actually writes to DB

Safety
------
  - Dry-run by default. Pass --apply to commit.
  - Skips any retiring_sku that already has a migration set, UNLESS
    --overwrite is passed. This protects manual mappings the team
    may have set up via the Migration UI.
  - All writes are logged to db's audit_log via set_migration().
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

import db


OUTPUT_DIR = Path("output")


def _latest_alternates_csv() -> Path | None:
    files = sorted(OUTPUT_DIR.glob("ip_alternates_*.csv"))
    return files[-1] if files else None


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Import IP merged[] relationships into sku_migrations DB")
    parser.add_argument(
        "--apply", action="store_true",
        help="Actually write to DB. Without this flag we just print the diff.")
    parser.add_argument(
        "--overwrite", action="store_true",
        help="Overwrite existing migrations. Default behaviour is to "
             "skip retiring SKUs that already have a manual migration "
             "set up — protects team work.")
    parser.add_argument(
        "--actor", default="ip-import",
        help="Actor string recorded in audit log (default: 'ip-import')")
    args = parser.parse_args()

    csv_path = _latest_alternates_csv()
    if not csv_path:
        print("ERROR: no output/ip_alternates_*.csv found. "
              "Run ip_pull_alternates.py first.")
        return 1

    print(f"Reading {csv_path}")
    df = pd.read_csv(csv_path, low_memory=False)
    print(f"  {len(df)} rows")

    # Pre-load existing migrations so we can compute the diff
    existing = {row["retiring_sku"]: dict(row)
                for row in db.all_migrations()}
    print(f"  {len(existing)} existing migrations in DB")

    to_create = []
    to_update = []
    to_skip = []
    for _, row in df.iterrows():
        retiring = str(row.get("AlternativeSKU") or "").strip()
        successor = str(row.get("MasterSKU") or "").strip()
        if not retiring or not successor:
            continue
        try:
            share_pct = float(row.get("Percent", 100) or 100)
        except (TypeError, ValueError):
            share_pct = 100.0
        title = str(row.get("AlternativeTitle") or "").strip()[:80]
        source = str(row.get("Source") or "").strip()
        note = (f"IP merge[] import — source={source}; "
                f"old title was '{title}'")

        rec = {
            "retiring": retiring,
            "successor": successor,
            "share_pct": share_pct,
            "note": note,
        }
        if retiring in existing:
            old = existing[retiring]
            same = (old.get("successor_sku") == successor
                     and abs(float(old.get("share_pct") or 0)
                              - share_pct) < 0.01)
            if same:
                to_skip.append(("identical", rec, old))
            elif args.overwrite:
                to_update.append((rec, old))
            else:
                to_skip.append(("manual-mapping-protected", rec, old))
        else:
            to_create.append(rec)

    print()
    print(f"Diff against existing migrations:")
    print(f"  Would CREATE: {len(to_create)}")
    print(f"  Would UPDATE: {len(to_update)}")
    print(f"  Would SKIP  : {len(to_skip)}")

    if to_create[:5]:
        print("\n  Sample creates (first 5):")
        for r in to_create[:5]:
            print(f"    {r['retiring']:<40} -> {r['successor']:<40}  "
                   f"@ {r['share_pct']:.0f}%")

    if to_update[:5]:
        print("\n  Sample updates (first 5):")
        for r, old in to_update[:5]:
            print(f"    {r['retiring']:<40}  "
                   f"{old.get('successor_sku')} -> {r['successor']}  "
                   f"@ {old.get('share_pct'):.0f}% -> {r['share_pct']:.0f}%")

    if to_skip[:5]:
        print("\n  Sample skips (first 5):")
        for reason, r, old in to_skip[:5]:
            print(f"    [{reason}] {r['retiring']:<40} "
                   f"existing -> {old.get('successor_sku')} "
                   f"@ {old.get('share_pct'):.0f}%")

    if not args.apply:
        print("\n(Dry-run. Re-run with --apply to commit.)")
        return 0

    # Commit
    n_done = 0
    for rec in to_create:
        db.set_migration(
            retiring_sku=rec["retiring"],
            successor_sku=rec["successor"],
            actor=args.actor,
            share_pct=rec["share_pct"],
            note=rec["note"],
        )
        n_done += 1
    for rec, _old in to_update:
        db.set_migration(
            retiring_sku=rec["retiring"],
            successor_sku=rec["successor"],
            actor=args.actor,
            share_pct=rec["share_pct"],
            note=rec["note"],
        )
        n_done += 1

    print(f"\nDone. Wrote {n_done} migration record(s).")
    print("The Streamlit app will pick these up on next data refresh "
          "(sidebar → 🔄 Refresh data now).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
