"""
audit_renamed_skus.py
=====================
Identifies CIN7 products whose SKU was inadvertently changed by yesterday's
cin7_push_migrations.py run. The push script's PUT body contained
"SKU": retiring_sku from the DB, which CIN7 took as a rename instruction
when the DB value didn't exactly match CIN7's actual SKU.

Approach:
  1. Read the pre-push products CSV (from before the push ran)
  2. Pull current product master from CIN7 (paginated)
  3. Match by ProductID
  4. Report every product whose SKU differs

Output:
  output/renamed_skus_<stamp>.csv  with columns:
    ProductID, OriginalSKU, CurrentSKU, Name

This is READ-ONLY. No changes to CIN7. Use the output to drive
fix_renamed_skus.py.

Usage
-----
    .venv\\Scripts\\python audit_renamed_skus.py
    # Optionally specify the pre-push CSV explicitly:
    .venv\\Scripts\\python audit_renamed_skus.py --pre-push output/products_2026-04-27_090147.csv
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv

BASE_URL = "https://inventory.dearsystems.com/ExternalApi/v2"
OUTPUT_DIR = Path("output")


def _parse_retry_after(value, default: int = 30) -> int:
    if value is None:
        return default
    digits = "".join(ch for ch in str(value) if ch.isdigit())
    return int(digits) if digits else default


def _request_with_retry(method, url, headers, **kwargs):
    for attempt in range(5):
        r = requests.request(method, url, headers=headers,
                              timeout=60, **kwargs)
        if r.status_code != 429:
            return r
        wait = _parse_retry_after(r.headers.get("Retry-After"), 60)
        print(f"  ... 429 on attempt {attempt + 1}, sleeping {wait}s")
        time.sleep(wait)
    return r


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Audit CIN7 products renamed by yesterday's push")
    parser.add_argument(
        "--pre-push", default=None,
        help="Path to pre-push products CSV. Default: latest "
             "products_*.csv with mtime BEFORE 2026-04-28.")
    args = parser.parse_args()

    load_dotenv()
    account_id = os.environ.get("CIN7_ACCOUNT_ID")
    app_key = os.environ.get("CIN7_APPLICATION_KEY")
    if not account_id or not app_key:
        print("ERROR: CIN7 credentials missing in .env")
        return 1
    headers = {
        "api-auth-accountid": account_id,
        "api-auth-applicationkey": app_key,
        "Accept": "application/json",
    }

    # Find pre-push products CSV
    if args.pre_push:
        pre_path = Path(args.pre_push)
    else:
        # Pick the most recent products csv with mtime before April 28
        cutoff = datetime(2026, 4, 28).timestamp()
        candidates = sorted(OUTPUT_DIR.glob("products_*.csv"))
        pre_path = None
        for p in reversed(candidates):
            if p.stat().st_mtime < cutoff:
                pre_path = p
                break
        if not pre_path:
            print("ERROR: no pre-push products CSV found before "
                  "2026-04-28. Pass --pre-push explicitly.")
            return 1
    print(f"Pre-push CSV: {pre_path}")
    pre = pd.read_csv(pre_path, low_memory=False)
    pre_by_id = {str(r["ID"]): str(r["SKU"])
                  for _, r in pre.iterrows()
                  if pd.notna(r.get("ID")) and pd.notna(r.get("SKU"))}
    print(f"  {len(pre_by_id)} products in pre-push CSV")

    # Pull current CIN7 product master
    print("\nFetching current CIN7 product master...")
    page = 1
    page_size = 1000
    current = []
    while True:
        r = _request_with_retry(
            "GET", f"{BASE_URL}/product",
            headers,
            params={"Page": page, "Limit": page_size,
                     "IncludeSuppliers": "true",
                     "IncludeReorderLevels": "true"})
        if r.status_code != 200:
            print(f"ERROR fetching page {page}: {r.status_code} "
                  f"{r.text[:200]}")
            return 1
        data = r.json()
        prods = data.get("Products") or []
        current.extend(prods)
        print(f"  page {page}: {len(prods)} (running total {len(current)})")
        if len(prods) < page_size:
            break
        page += 1
        time.sleep(1.5)
    print(f"Pulled {len(current)} products from CIN7\n")

    current_by_id = {str(p["ID"]): {"SKU": str(p.get("SKU", "")),
                                      "Name": str(p.get("Name", ""))}
                      for p in current if p.get("ID")}

    # Find renamed
    renamed = []
    for pid, orig_sku in pre_by_id.items():
        cur = current_by_id.get(pid)
        if cur is None:
            # Product was deleted? Skip
            continue
        cur_sku = cur["SKU"]
        if cur_sku and cur_sku != orig_sku:
            renamed.append({
                "ProductID": pid,
                "OriginalSKU": orig_sku,
                "CurrentSKU": cur_sku,
                "Name": cur["Name"][:80],
            })

    print(f"Renamed products: {len(renamed)}")
    if renamed:
        print(f"\n{'OriginalSKU':<40} → {'CurrentSKU':<40}")
        print("-" * 90)
        for r in renamed:
            print(f"  {r['OriginalSKU']:<40} → {r['CurrentSKU']:<40}")

        stamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
        out = OUTPUT_DIR / f"renamed_skus_{stamp}.csv"
        with out.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=["ProductID", "OriginalSKU",
                                                "CurrentSKU", "Name"])
            w.writeheader()
            w.writerows(renamed)
        print(f"\nSaved: {out}")
        print("\nReview this CSV. If it matches your expectation, run:")
        print(f"  .\\.venv\\Scripts\\python fix_renamed_skus.py {out.name}")
    else:
        print("No renamed products found. Either the audit caught an "
              "edge case yesterday, or all PUT calls happened to send "
              "SKUs that already matched CIN7's records.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
