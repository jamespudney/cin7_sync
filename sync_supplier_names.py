"""
sync_supplier_names.py
======================
CIN7 is the single source of truth for supplier names. Whenever the
team edits a supplier name in CIN7 (or our local config drifts), this
script propagates the canonical CIN7 name through every local table
that references suppliers.

Why this matters: the CIN7 PO push (cin7_post_po.py) only accepts an
EXACT case-insensitive name match. A drift like 'Reeves' (local) vs
'Reeves Extruded Products, Inc' (CIN7) silently routed POs to the
wrong vendor before we tightened that — see the PO-7076 incident.

Detection strategy:
  1. Pull every supplier from CIN7 via /supplierList (paginated).
  2. Build a set of canonical names + IDs.
  3. For each local table, scan the supplier column and flag any value
     that doesn't appear (case-insensitively) in CIN7's set.
  4. For drift candidates with an obvious near-match (substring or
     fuzzy match), suggest a rename. For the rest, just report.

Tables we update:
  - supplier_config        (PK supplier_name)
  - supplier_pricing       (PK supplier_name)
  - family_supplier_assignments (supplier_name)
  - sku_supplier_overrides (supplier_name)
  - po_drafts              (supplier)
  - po_draft_edits         (supplier; legacy)
  - family_color_pricing   (supplier)
  - family_setup_fees      (supplier)
  - family_pricing_rules   (supplier)

What's NOT updated (intentionally):
  - audit_log entries — historical record stays as-typed
  - sale_lines / purchase_lines — historical, follow CIN7's own values

Usage
-----
    .venv\\Scripts\\python sync_supplier_names.py            # dry-run
    .venv\\Scripts\\python sync_supplier_names.py --apply    # commit
    .venv\\Scripts\\python sync_supplier_names.py --rename "Reeves" "Reeves Extruded Products, Inc" --apply
        ↑ targeted: rename just this one mapping (no auto-detect)

Recommended cadence: run after any CIN7 supplier-name edit, or weekly
as a drift catcher.
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import datetime
from pathlib import Path

import requests
from dotenv import load_dotenv

import db


BASE_URL = "https://inventory.dearsystems.com/ExternalApi/v2"
# OUTPUT_DIR follows DATA_DIR env var (set to /data on Render).
from data_paths import OUTPUT_DIR  # noqa: E402


# (table, supplier_column) — keep in sync with db.py schema.
SUPPLIER_TABLES = [
    ("supplier_config",            "supplier_name"),
    ("supplier_pricing",           "supplier_name"),
    ("family_supplier_assignments", "supplier_name"),
    ("sku_supplier_overrides",     "supplier_name"),
    ("po_drafts",                  "supplier"),
    ("po_draft_edits",             "supplier"),
    ("family_color_pricing",       "supplier"),
    ("family_setup_fees",          "supplier"),
    ("family_pricing_rules",       "supplier"),
]


def _parse_retry_after(value, default: int = 30) -> int:
    if value is None:
        return default
    digits = "".join(ch for ch in str(value) if ch.isdigit())
    return int(digits) if digits else default


def _get(url: str, headers, params=None, max_retries: int = 3):
    for attempt in range(max_retries + 1):
        r = requests.get(url, headers=headers, params=params, timeout=60)
        if r.status_code != 429:
            return r
        wait = _parse_retry_after(r.headers.get("Retry-After"), 30)
        print(f"  ... 429 attempt {attempt + 1}, sleeping {wait}s")
        time.sleep(wait)
    return r


def _fetch_cin7_suppliers(headers) -> list:
    """Pull every active supplier from CIN7. Returns list of dicts with
    at least 'ID' and 'Name'.

    Endpoint is /supplier (singular). Earlier draft used /supplierList
    which returns a 404 HTML page → JSONDecodeError on r.json()."""
    suppliers = []
    page = 1
    while True:
        r = _get(f"{BASE_URL}/supplier", headers,
                  params={"Page": page, "Limit": 1000})
        if r.status_code != 200:
            print(f"ERROR: /supplier page {page} -> {r.status_code} "
                   f"{r.text[:200]}")
            return suppliers
        # Defensive: even at 200, paranoid about non-JSON.
        try:
            data = r.json() or {}
        except ValueError:
            print(f"ERROR: /supplier page {page} returned non-JSON "
                   f"(first 200 chars): {r.text[:200]!r}")
            return suppliers
        batch = data.get("SupplierList") or []
        suppliers.extend(batch)
        print(f"  page {page} -> {len(batch)} (running {len(suppliers)})")
        if len(batch) < 1000:
            break
        page += 1
        time.sleep(1.5)
    return suppliers


def _local_supplier_names() -> dict:
    """Returns {table.col: {distinct_name: row_count}}."""
    out: dict = {}
    with db.connect() as c:
        for table, col in SUPPLIER_TABLES:
            try:
                rows = c.execute(
                    f"SELECT {col} AS sup, COUNT(*) AS n FROM {table} "
                    f"WHERE {col} IS NOT NULL AND {col} != '' "
                    f"GROUP BY {col}"
                ).fetchall()
            except Exception:
                # Table may not exist on older DBs
                continue
            out[f"{table}.{col}"] = {
                str(r["sup"]): int(r["n"]) for r in rows}
    return out


def _suggest_match(local_name: str, cin7_names: list) -> str | None:
    """Best-effort suggest-a-CIN7-name for a drifted local value.
    Returns the suggested CIN7 name or None.
    Strategy:
      1. Exact case-insensitive match → return.
      2. Local is a substring of one CIN7 name → return that.
      3. CIN7 name is a substring of local → return that.
      4. Otherwise None.
    """
    ll = local_name.strip().lower()
    for cn in cin7_names:
        if cn.strip().lower() == ll:
            return cn
    # Substring matches
    candidates = [
        cn for cn in cin7_names
        if ll in cn.strip().lower() or cn.strip().lower() in ll]
    if len(candidates) == 1:
        return candidates[0]
    return None


def _rename_one(c, old: str, new: str) -> dict:
    """Run UPDATE across every supplier-referencing table. Returns a
    {table.col: rows_touched, ...} report. Collisions (where the new
    name already exists in a PK column for the same row) are recorded
    as -1."""
    counts: dict = {}
    for table, col in SUPPLIER_TABLES:
        try:
            # If the column is a primary key, a rename to an existing
            # value would violate the constraint. Detect the conflict
            # rather than letting SQLite raise.
            pk_check = ""
            if table in ("supplier_config", "supplier_pricing"):
                # PK on supplier_name — check for collision
                existing = c.execute(
                    f"SELECT COUNT(*) AS n FROM {table} "
                    f"WHERE {col} = ?", (new,)).fetchone()
                if existing and existing["n"] > 0:
                    counts[f"{table}.{col} (collision)"] = -1
                    continue
            n = c.execute(
                f"UPDATE {table} SET {col} = ? WHERE {col} = ?",
                (new, old)).rowcount
            if n:
                counts[f"{table}.{col}"] = n
        except Exception as exc:
            counts[f"{table}.{col} (error)"] = str(exc)
    return counts


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Sync local supplier names to CIN7 (source of truth)")
    parser.add_argument(
        "--apply", action="store_true",
        help="Actually run the UPDATEs. Without this we dry-run.")
    parser.add_argument(
        "--rename", nargs=2, metavar=("OLD", "NEW"), default=None,
        help="Targeted rename: rename OLD -> NEW across all tables. "
             "Skips the CIN7 fetch + auto-detect path.")
    args = parser.parse_args()

    stamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")

    # --- Targeted rename short-circuit -----------------------------------
    if args.rename:
        old, new = args.rename
        print(f"Targeted rename: '{old}' -> '{new}'")
        if not args.apply:
            print("\n(Dry-run.) Re-run with --apply to commit.")
        with db.connect() as c:
            counts = _rename_one(c, old, new)
            if not args.apply:
                # Show what we'd do — preview UPDATE counts via SELECT
                print("Rows that would be touched:")
                for table, col in SUPPLIER_TABLES:
                    try:
                        n = c.execute(
                            f"SELECT COUNT(*) AS n FROM {table} "
                            f"WHERE {col} = ?", (old,)).fetchone()["n"]
                        if n:
                            print(f"  {table}.{col} : {n}")
                    except Exception:
                        pass
                return 0
            # Apply path
            print("Applied:")
            for k, v in counts.items():
                print(f"  {k} : {v}")
            c.execute(
                "INSERT INTO audit_log (event, actor, target, detail) "
                "VALUES (?, ?, ?, ?)",
                ("supplier.rename", "sync_supplier_names",
                 old, f"-> {new}; touched: {counts}"))
        return 0

    # --- Full sync path: fetch CIN7, compare, suggest --------------------
    load_dotenv()
    account_id = os.environ.get("CIN7_ACCOUNT_ID")
    app_key = os.environ.get("CIN7_APPLICATION_KEY")
    if not account_id or not app_key:
        print("ERROR: CIN7_ACCOUNT_ID / CIN7_APPLICATION_KEY missing")
        return 1
    headers = {
        "api-auth-accountid": account_id,
        "api-auth-applicationkey": app_key,
        "Accept": "application/json",
    }

    print("Fetching CIN7 supplier list...")
    cin7_suppliers = _fetch_cin7_suppliers(headers)
    if not cin7_suppliers:
        print("Got zero suppliers. Aborting.")
        return 1
    cin7_names = sorted({
        str(s.get("Name") or "").strip() for s in cin7_suppliers
        if s.get("Name")})
    cin7_lower = {n.lower() for n in cin7_names}
    print(f"CIN7 has {len(cin7_names)} unique supplier name(s).")

    print("\nScanning local tables for drift...")
    local = _local_supplier_names()
    drift: dict = {}  # local_name -> {tables: [...], suggested: name|None}
    for table_col, name_to_count in local.items():
        for nm, n in name_to_count.items():
            if nm.lower() in cin7_lower:
                continue  # exact match — fine
            entry = drift.setdefault(
                nm, {"tables": [], "rows": 0,
                      "suggested": _suggest_match(nm, cin7_names)})
            entry["tables"].append((table_col, n))
            entry["rows"] += n

    if not drift:
        print(f"\n✓ No drift detected. All local supplier names match "
              f"CIN7 exactly. Nice.")
        return 0

    print(f"\nDetected {len(drift)} drifted name(s):")
    for nm, info in sorted(drift.items()):
        sugg = info["suggested"] or "  (no obvious match — manual)"
        print(f"\n  ✗ '{nm}'")
        print(f"        appears in {info['rows']} row(s):")
        for t, n in info["tables"]:
            print(f"          - {t} ({n})")
        print(f"        suggested CIN7 name → {sugg!r}")

    if not args.apply:
        print(f"\n(Dry-run.) To rename one mapping run:")
        print(f"  .venv\\Scripts\\python sync_supplier_names.py "
              f"--rename \"OLD\" \"NEW\" --apply")
        print(f"To auto-apply ALL suggested renames at once: re-run with --apply.")
        return 0

    # --- Auto-apply path -----------------------------------------------
    print(f"\n--apply set: applying suggested renames...")
    n_applied = 0
    n_skipped = 0
    with db.connect() as c:
        for nm, info in sorted(drift.items()):
            if not info["suggested"]:
                print(f"  SKIP '{nm}' — no obvious CIN7 match.")
                n_skipped += 1
                continue
            target = info["suggested"]
            counts = _rename_one(c, nm, target)
            print(f"  ✓ '{nm}' → '{target}'")
            for k, v in counts.items():
                print(f"      {k} : {v}")
            c.execute(
                "INSERT INTO audit_log (event, actor, target, detail) "
                "VALUES (?, ?, ?, ?)",
                ("supplier.rename", "sync_supplier_names",
                 nm, f"-> {target}; touched: {counts}"))
            n_applied += 1
    print(f"\nDone. Applied {n_applied}, skipped {n_skipped} "
           f"(manual rename needed for those — use --rename).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
