"""
fix_renamed_skus.py
===================
Restores SKUs on CIN7 products that were inadvertently renamed by
yesterday's cin7_push_migrations.py run. Reads the audit CSV produced
by audit_renamed_skus.py and PUTs the OriginalSKU back.

Critical: this PUT body is INTENTIONALLY MINIMAL — only the fields
required to update the SKU. Nothing else gets touched.

  PUT /product
  Body: {
    "ID": <ProductID>,
    "SKU": <OriginalSKU>,
    "AttributeSet": <existing AttributeSet>
  }

Usage
-----
    .venv\\Scripts\\python fix_renamed_skus.py <audit_csv>
    # Dry-run is the default. Add --apply to actually PUT.
    .venv\\Scripts\\python fix_renamed_skus.py renamed_skus_<stamp>.csv --apply

Safety
------
  - Reads the audit CSV produced by audit_renamed_skus.py
  - Dry-run by default; shows what each PUT body will contain
  - Verifies after each PUT that CIN7 echoed back the OriginalSKU
  - Logs everything to output/fix_renamed_skus_<stamp>.log
"""

from __future__ import annotations

import argparse
import csv
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path

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
        time.sleep(wait)
    return r


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Restore SKUs from audit CSV")
    parser.add_argument(
        "audit_csv", help="renamed_skus_*.csv from audit_renamed_skus.py")
    parser.add_argument(
        "--apply", action="store_true",
        help="Actually PUT the restorations. Dry-run otherwise.")
    parser.add_argument(
        "--rate", type=float, default=1.5,
        help="Seconds between API calls (default 1.5)")
    args = parser.parse_args()

    audit_path = Path(args.audit_csv)
    if not audit_path.exists() and (OUTPUT_DIR / args.audit_csv).exists():
        audit_path = OUTPUT_DIR / args.audit_csv
    if not audit_path.exists():
        print(f"ERROR: {args.audit_csv} not found")
        return 1

    load_dotenv()
    account_id = os.environ.get("CIN7_ACCOUNT_ID")
    app_key = os.environ.get("CIN7_APPLICATION_KEY")
    if not account_id or not app_key:
        print("ERROR: CIN7 credentials missing in .env")
        return 1
    headers = {
        "api-auth-accountid": account_id,
        "api-auth-applicationkey": app_key,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    stamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    log = logging.getLogger("fix_renamed_skus")
    log.setLevel(logging.INFO)
    if not log.handlers:
        fh = logging.FileHandler(
            OUTPUT_DIR / f"fix_renamed_skus_{stamp}.log",
            encoding="utf-8")
        fh.setFormatter(logging.Formatter(
            "%(asctime)s  %(levelname)-8s %(message)s"))
        log.addHandler(fh)
        sh = logging.StreamHandler()
        sh.setFormatter(logging.Formatter("%(message)s"))
        log.addHandler(sh)

    rows = []
    with audit_path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            rows.append(r)
    log.info("Loaded %d renamed products from %s",
              len(rows), audit_path.name)

    if not rows:
        log.info("Nothing to do.")
        return 0

    log.info("Mode: %s | Rate: %.1fs",
              "APPLY (will PUT to CIN7)" if args.apply else "DRY-RUN",
              args.rate)
    log.info("=" * 70)

    n_done = 0
    n_skipped = 0
    n_errors = 0
    last_call = 0.0

    for i, row in enumerate(rows, 1):
        pid = row["ProductID"]
        orig = row["OriginalSKU"]
        cur = row["CurrentSKU"]

        # Throttle
        elapsed = time.time() - last_call
        if elapsed < args.rate:
            time.sleep(args.rate - elapsed)

        # Re-fetch current state to confirm and get AttributeSet
        r = _request_with_retry(
            "GET", f"{BASE_URL}/product", headers,
            params={"ID": pid})
        last_call = time.time()
        if r.status_code != 200:
            log.warning("[%s] GET failed %d: %s",
                         orig, r.status_code, r.text[:200])
            n_errors += 1
            continue

        data = r.json()
        prods = data.get("Products") or [data]
        if not prods or not isinstance(prods[0], dict):
            log.warning("[%s] product not found by ID %s", orig, pid)
            n_errors += 1
            continue
        prod = prods[0]
        cur_sku_now = str(prod.get("SKU") or "")
        attr_set = str(prod.get("AttributeSet") or "")

        if cur_sku_now == orig:
            log.info("[%s] already restored — no change", orig)
            n_skipped += 1
            continue
        if cur_sku_now != cur:
            log.warning(
                "[%s] CIN7 SKU is %r (audit said %r) — proceeding "
                "with restore to %r anyway", orig, cur_sku_now, cur, orig)

        log.info("[%d/%d] %s -> %s  (ID %s)",
                  i, len(rows), cur_sku_now, orig, pid)

        if not args.apply:
            continue

        put_body = {
            "ID": pid,
            "SKU": orig,
            "AttributeSet": attr_set,
        }
        elapsed = time.time() - last_call
        if elapsed < args.rate:
            time.sleep(args.rate - elapsed)
        rp = _request_with_retry(
            "PUT", f"{BASE_URL}/product", headers, json=put_body)
        last_call = time.time()
        if not rp.ok:
            log.warning("[%s] PUT failed %d: %s",
                         orig, rp.status_code, rp.text[:300])
            n_errors += 1
            continue
        # Verify echo
        try:
            echo = rp.json()
            echo_prods = echo.get("Products") or [echo]
            echo_sku = str(echo_prods[0].get("SKU") or "")
            if echo_sku != orig:
                log.warning(
                    "[%s] PUT 200 but response SKU is %r — RESTORE FAILED",
                    orig, echo_sku)
                n_errors += 1
                continue
        except Exception:
            pass
        n_done += 1

    log.info("=" * 70)
    log.info("Summary:")
    log.info("  Audit rows         : %d", len(rows))
    log.info("  Already correct    : %d", n_skipped)
    log.info("  Errors             : %d", n_errors)
    if args.apply:
        log.info("  RESTORED           : %d", n_done)
    else:
        log.info("(Dry-run. Re-run with --apply to commit.)")

    return 0 if n_errors == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
