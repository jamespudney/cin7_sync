"""
cin7_ingest_attributes.py
=========================
Read the latest output/products_*.csv and ingest two attributes:

  AdditionalAttribute5 ("Replaced By" / Predecessor or Replacement Product)
    → upserts into db.sku_migrations as
      (retiring=this SKU, successor=value, share=100, set_by=cin7-pull)

  AdditionalAttribute6 ("Alternative Product")
    → writes to output/cin7_alternatives_<stamp>.csv
      (alternatives don't carry sales rollup, so they don't go in the
       migrations table — the app loads this CSV separately for display
       in the drill-down's Family variants section)

The product CSV must already have been synced. If it's stale, run:

    .venv\\Scripts\\python cin7_sync.py products

Both attributes accept comma-separated values for multi-target entries.

Usage
-----
    .venv\\Scripts\\python cin7_ingest_attributes.py            # dry-run
    .venv\\Scripts\\python cin7_ingest_attributes.py --apply    # writes to DB
"""

from __future__ import annotations

import argparse
import csv
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

import db


OUTPUT_DIR = Path("output")


def _latest(prefix: str) -> Path | None:
    files = sorted(OUTPUT_DIR.glob(f"{prefix}_*.csv"))
    return files[-1] if files else None


def _split_skus(raw: str) -> list[str]:
    """Parse a multi-value attribute string into a list of SKUs.
    Accepts comma, semicolon, pipe, or newline separators."""
    if not raw or pd.isna(raw):
        return []
    out = []
    for chunk in str(raw).replace(";", ",").replace("|", ",").splitlines():
        for tok in chunk.split(","):
            t = tok.strip()
            if t:
                out.append(t)
    return out


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Ingest CIN7 AA5/AA6 product attributes into our DB / CSVs")
    parser.add_argument(
        "--apply", action="store_true",
        help="Actually write to DB. Without this flag we dry-run.")
    parser.add_argument(
        "--actor", default="cin7-pull",
        help="Actor recorded in audit log (default: 'cin7-pull')")
    args = parser.parse_args()

    products_csv = _latest("products")
    if not products_csv:
        print("ERROR: no output/products_*.csv found. "
              "Run `python cin7_sync.py products` first.")
        return 1

    print(f"Reading {products_csv}")
    df = pd.read_csv(products_csv, low_memory=False)
    print(f"  {len(df)} products")

    aa5_col = "AdditionalAttribute5"
    aa6_col = "AdditionalAttribute6"
    if aa5_col not in df.columns:
        print(f"WARN: '{aa5_col}' column not in CSV — re-sync products.")
    if aa6_col not in df.columns:
        print(f"WARN: '{aa6_col}' column not in CSV — re-sync products.")
    if aa5_col not in df.columns and aa6_col not in df.columns:
        return 1

    # -------------------------------------------------------------- AA5 (migrations)
    existing = {dict(m).get("retiring_sku"): dict(m)
                for m in db.all_migrations()}
    existing_set = set(existing.keys())

    aa5_rows = df[df[aa5_col].notna() & (df[aa5_col].astype(str).str.strip() != "")] \
        if aa5_col in df.columns else pd.DataFrame()
    print(f"\nAA5 ('Replaced By') ingest:")
    print(f"  {len(aa5_rows)} products have a non-empty AA5 value")

    to_create = []
    to_update = []
    to_skip_identical = []
    bad_skus = []  # values that don't match any known SKU
    known_skus = set(df["SKU"].astype(str)) if "SKU" in df.columns else set()
    for _, row in aa5_rows.iterrows():
        retiring = str(row["SKU"]).strip()
        targets = _split_skus(str(row[aa5_col]))
        if not targets:
            continue
        # If multiple targets, take the FIRST one. The DB schema is one-
        # successor-per-retiring; multi-target rare for true migrations
        # (a product is replaced by one thing). Log the rest for review.
        successor = targets[0]
        if successor not in known_skus:
            bad_skus.append((retiring, successor))
        if len(targets) > 1:
            # Surface to user — multi-value migrations are unusual
            print(
                f"  NOTE: {retiring} has multiple AA5 values "
                f"{targets} — using first ({successor}); review if "
                f"this should be split into separate retirees")

        if retiring in existing_set:
            old = existing[retiring]
            if str(old.get("successor_sku")) == successor:
                to_skip_identical.append(retiring)
            else:
                to_update.append((retiring, successor,
                                   old.get("successor_sku")))
        else:
            to_create.append((retiring, successor))

    print(f"  Would CREATE     : {len(to_create)}")
    print(f"  Would UPDATE     : {len(to_update)}")
    print(f"  Already identical: {len(to_skip_identical)}")
    if bad_skus:
        print(f"  Unknown target SKUs (warnings): {len(bad_skus)}")
        for r, s in bad_skus[:5]:
            print(f"    {r:<35} -> {s}  (target not in product master)")

    if to_create[:5]:
        print("  Sample CREATE (first 5):")
        for r, s in to_create[:5]:
            print(f"    {r:<35} -> {s}")
    if to_update[:5]:
        print("  Sample UPDATE (first 5):")
        for r, s, old in to_update[:5]:
            print(f"    {r:<35} {old} -> {s}")

    if args.apply:
        n_done = 0
        for r, s in to_create:
            db.set_migration(
                retiring_sku=r, successor_sku=s,
                actor=args.actor, share_pct=100.0,
                note="ingested from CIN7 AdditionalAttribute5")
            n_done += 1
        for r, s, _old in to_update:
            db.set_migration(
                retiring_sku=r, successor_sku=s,
                actor=args.actor, share_pct=100.0,
                note="updated from CIN7 AdditionalAttribute5")
            n_done += 1
        print(f"  AA5 written to DB: {n_done}")

    # -------------------------------------------------------------- AA6 (alternatives)
    aa6_rows = df[df[aa6_col].notna() & (df[aa6_col].astype(str).str.strip() != "")] \
        if aa6_col in df.columns else pd.DataFrame()
    print(f"\nAA6 ('Alternative Product') ingest:")
    print(f"  {len(aa6_rows)} products have a non-empty AA6 value")

    stamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    alt_csv = OUTPUT_DIR / f"cin7_alternatives_{stamp}.csv"
    n_alt_pairs = 0
    with alt_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["SKU", "AlternativeSKU", "AlternativeKnown"])
        for _, row in aa6_rows.iterrows():
            sku = str(row["SKU"]).strip()
            for alt in _split_skus(str(row[aa6_col])):
                w.writerow([sku, alt, "Y" if alt in known_skus else "N"])
                n_alt_pairs += 1

    print(f"  Wrote {n_alt_pairs} alternative-pair rows to {alt_csv}")
    print(f"  (AA6 is informational only — no DB write)")

    if not args.apply and (to_create or to_update):
        print("\n(Dry-run for AA5. Re-run with --apply to commit.)")

    return 0


if __name__ == "__main__":
    sys.exit(main())
