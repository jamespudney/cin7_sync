"""
cin7_push_migrations.py
=======================
Push every entry in db.sku_migrations to the corresponding CIN7
product's AdditionalAttribute5 ("Replaced By" / "Predecessor or
Replacement Product") field.

Migration semantics:
    db.sku_migrations.retiring_sku  → the SKU we EDIT in CIN7
    db.sku_migrations.successor_sku → the value we WRITE to AA5

So when our DB says "LED-E60L24DC-KO is retiring, replaced by
LED-XRD-60W-24 at 100%", we open LED-E60L24DC-KO's CIN7 record and set
AdditionalAttribute5 = "LED-XRD-60W-24". A buyer in CIN7 will then see
"Replaced By: LED-XRD-60W-24" on the product detail page.

Conflict policy
---------------
By default we SKIP any retiring SKU where CIN7 already has a non-empty
AdditionalAttribute5 value — this protects manual work the team has
done in CIN7. Pass --overwrite to force-update those.

Usage
-----
    .venv\\Scripts\\python cin7_push_migrations.py            # dry-run preview
    .venv\\Scripts\\python cin7_push_migrations.py --apply    # commit
    .venv\\Scripts\\python cin7_push_migrations.py --apply --overwrite

Output
------
    output/cin7_push_migrations_<stamp>.log
"""

from __future__ import annotations

import argparse
import logging
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

# CIN7 caps API at ~60 calls/min, SHARED across all integrations on the
# account (Inventory Planner, Shopify, near-sync, etc). Each push needs
# 1 GET + 1 PUT = 2 calls. At 1.5s rate we're at 40 calls/min, leaving
# 20/min headroom for other apps.
DEFAULT_RATE_S = 1.5
MAX_429_RETRIES = 3


def _parse_retry_after(value, default: int = 30) -> int:
    """CIN7 sometimes returns Retry-After as plain seconds ('30') and
    sometimes as a string with units ('60 Seconds'). Strip non-digits."""
    if value is None:
        return default
    digits = "".join(ch for ch in str(value) if ch.isdigit())
    return int(digits) if digits else default


def _setup_log(stamp: str) -> logging.Logger:
    log = logging.getLogger("cin7_push_migrations")
    log.setLevel(logging.INFO)
    if not log.handlers:
        fh = logging.FileHandler(
            OUTPUT_DIR / f"cin7_push_migrations_{stamp}.log",
            encoding="utf-8")
        fh.setFormatter(logging.Formatter(
            "%(asctime)s  %(levelname)-8s %(message)s"))
        log.addHandler(fh)
        sh = logging.StreamHandler()
        sh.setFormatter(logging.Formatter("%(message)s"))
        log.addHandler(sh)
    return log


def _throttle(last_call_t: float, rate_s: float) -> float:
    """Sleep so we don't exceed `rate_s` seconds between calls.
    Returns the new last-call timestamp."""
    elapsed = time.time() - last_call_t
    if elapsed < rate_s:
        time.sleep(rate_s - elapsed)
    return time.time()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Push db.sku_migrations to CIN7 AdditionalAttribute5")
    parser.add_argument(
        "--apply", action="store_true",
        help="Actually PUT to CIN7. Without this flag we dry-run.")
    parser.add_argument(
        "--overwrite", action="store_true",
        help="Overwrite existing CIN7 AA5 values. Default skips them "
             "to protect manual entries the team has made in CIN7.")
    parser.add_argument(
        "--rate", type=float, default=None,
        help="Seconds between API calls. Falls back to CIN7_RATE_SECONDS "
             f"in .env, or {DEFAULT_RATE_S}s if not set.")
    parser.add_argument(
        "--limit", type=int, default=None,
        help="Stop after N pushes (sanity test). Omit to process all.")
    parser.add_argument(
        "--filter", type=str, default=None,
        help="Only process retiring SKUs matching this substring "
             "(e.g. --filter LED-XRD for a pilot).")
    args = parser.parse_args()

    load_dotenv()
    account_id = os.environ.get("CIN7_ACCOUNT_ID")
    app_key = os.environ.get("CIN7_APPLICATION_KEY")
    if not account_id or not app_key:
        print("ERROR: CIN7_ACCOUNT_ID / CIN7_APPLICATION_KEY missing in .env")
        return 1

    # Resolve rate: CLI flag wins, then .env CIN7_RATE_SECONDS, then default
    if args.rate is not None:
        rate_s = float(args.rate)
    else:
        try:
            rate_s = float(
                os.environ.get("CIN7_RATE_SECONDS", DEFAULT_RATE_S))
        except (TypeError, ValueError):
            rate_s = DEFAULT_RATE_S
    args.rate = rate_s  # downstream code reads args.rate

    headers = {
        "api-auth-accountid": account_id,
        "api-auth-applicationkey": app_key,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    stamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    log = _setup_log(stamp)

    migrations = [dict(m) for m in db.all_migrations()]
    log.info("Loaded %d migrations from DB", len(migrations))

    if args.filter:
        migrations = [
            m for m in migrations
            if args.filter in str(m.get("retiring_sku") or "")
        ]
        log.info("After --filter '%s': %d remaining",
                  args.filter, len(migrations))

    log.info("Mode: %s  |  Overwrite: %s  |  Rate: %.1fs",
              "APPLY (will PUT to CIN7)" if args.apply else "DRY-RUN",
              args.overwrite, args.rate)
    log.info("=" * 60)

    n_processed = 0
    n_would_create = 0
    n_would_overwrite = 0
    n_skipped_existing = 0
    n_skipped_missing = 0
    n_done = 0
    n_errors = 0

    last_call = 0.0

    for mig in migrations:
        if args.limit and n_processed >= args.limit:
            break
        n_processed += 1

        retiring = str(mig.get("retiring_sku") or "").strip()
        successor = str(mig.get("successor_sku") or "").strip()
        if not retiring or not successor:
            continue

        # Step 1: Find the retiring SKU's product in CIN7. Retry on 429.
        r = None
        for _attempt in range(MAX_429_RETRIES + 1):
            last_call = _throttle(last_call, args.rate)
            try:
                r = requests.get(
                    f"{BASE_URL}/product",
                    headers=headers,
                    params={"Sku": retiring, "Limit": 1},
                    timeout=30,
                )
            except requests.RequestException as exc:
                log.warning("  [%s] GET error: %s", retiring, exc)
                n_errors += 1
                r = None
                break
            if r.status_code != 429:
                break
            wait = _parse_retry_after(r.headers.get("Retry-After"), 30)
            log.info("  [%s] 429 on GET — sleeping %ds (attempt %d)",
                      retiring, wait, _attempt + 1)
            time.sleep(wait)
        if r is None or r.status_code == 429:
            log.warning("  [%s] gave up after 429 retries", retiring)
            n_errors += 1
            continue

        if r.status_code != 200:
            log.warning("  [%s] GET %d: %s",
                         retiring, r.status_code, r.text[:200])
            n_errors += 1
            continue

        prods = (r.json() or {}).get("Products") or []
        if not prods:
            log.warning("  [%s] not found in CIN7 — skipped",
                         retiring)
            n_skipped_missing += 1
            continue

        prod = prods[0]
        prod_id = prod.get("ID") or prod.get("ProductID")
        existing_aa5 = (prod.get("AdditionalAttribute5") or "").strip()
        # CIN7 needs SKU + AttributeSet in the PUT body to actually write
        # AdditionalAttributeN fields. Without them the PUT returns 200
        # but silently drops the update. Discovered via cin7_put_test.py.
        prod_attribute_set = prod.get("AttributeSet") or ""

        # Diff
        if existing_aa5 == successor:
            log.info("  [%s] AA5 already set to '%s' — no change",
                      retiring, successor)
            continue

        if existing_aa5 and not args.overwrite:
            log.info(
                "  [%s] CIN7 has '%s' (manual?), DB says '%s' — "
                "SKIPPED (use --overwrite to force)",
                retiring, existing_aa5, successor)
            n_skipped_existing += 1
            continue

        if existing_aa5:
            n_would_overwrite += 1
            action = f"OVERWRITE: '{existing_aa5}' -> '{successor}'"
        else:
            n_would_create += 1
            action = f"CREATE: '' -> '{successor}'"

        log.info("  [%s]  %s", retiring, action)

        if not args.apply:
            continue

        # Step 2: PUT the update. Retry on 429.
        # SKU + AttributeSet are MANDATORY for CIN7 to accept the AA5
        # write — see cin7_put_test.py for evidence. Without them the
        # PUT is silently no-op'd despite returning 200.
        #
        # ⚠ HARD RULE: SKUs are the join key between CIN7 and Shopify.
        # We must NEVER send a SKU value that could rename the product.
        # The PUT body's SKU MUST be the exact value CIN7 returned in
        # the GET, byte-for-byte. We assert this before sending.
        cin7_actual_sku = str(prod.get("SKU") or "").strip()
        if not cin7_actual_sku:
            log.warning("  [%s] CIN7 returned product without a SKU "
                         "field — refusing to PUT to avoid blanking it",
                         retiring)
            n_errors += 1
            continue
        # Belt-and-braces: only allow these four keys in the body.
        # Any other field (incl. fat-body PUTs) raises before sending.
        ALLOWED_PUT_KEYS = {
            "ID", "SKU", "AttributeSet", "AdditionalAttribute5"}
        put_body = {
            "ID": prod_id,
            "SKU": cin7_actual_sku,  # echo CIN7's value, do NOT rename
            "AttributeSet": prod_attribute_set,
            "AdditionalAttribute5": successor,
        }
        # Defensive assert — fails loudly rather than silently sending
        # a body that could mutate other fields.
        bad_keys = set(put_body.keys()) - ALLOWED_PUT_KEYS
        if bad_keys:
            raise RuntimeError(
                f"PUT body contains disallowed keys {bad_keys}. "
                f"This script must NEVER send anything beyond "
                f"{ALLOWED_PUT_KEYS}. Aborting to prevent damage.")
        if put_body["SKU"] != cin7_actual_sku:
            raise RuntimeError(
                f"PUT body SKU {put_body['SKU']!r} does not match "
                f"CIN7's actual SKU {cin7_actual_sku!r}. Refusing to "
                f"send — this would rename the product.")
        put_resp = None
        for _attempt in range(MAX_429_RETRIES + 1):
            last_call = _throttle(last_call, args.rate)
            try:
                put_resp = requests.put(
                    f"{BASE_URL}/product",
                    headers=headers,
                    json=put_body,
                    timeout=30,
                )
            except requests.RequestException as exc:
                log.warning("  [%s] PUT error: %s", retiring, exc)
                n_errors += 1
                put_resp = None
                break
            if put_resp.status_code != 429:
                break
            wait = _parse_retry_after(put_resp.headers.get("Retry-After"), 30)
            log.info("  [%s] 429 on PUT — sleeping %ds (attempt %d)",
                      retiring, wait, _attempt + 1)
            time.sleep(wait)
        if put_resp is None or put_resp.status_code == 429:
            log.warning("  [%s] gave up after 429 retries", retiring)
            n_errors += 1
            continue

        if not put_resp.ok:
            log.warning("  [%s] PUT %d: %s",
                         retiring, put_resp.status_code,
                         put_resp.text[:200])
            n_errors += 1
            continue

        n_done += 1

    log.info("=" * 60)
    log.info("Summary:")
    log.info("  Processed         : %d", n_processed)
    log.info("  Would create      : %d", n_would_create)
    log.info("  Would overwrite   : %d", n_would_overwrite)
    log.info("  Skipped (existing): %d  (use --overwrite to force)",
              n_skipped_existing)
    log.info("  Skipped (missing) : %d", n_skipped_missing)
    log.info("  Errors            : %d", n_errors)
    if args.apply:
        log.info("  ACTUALLY WRITTEN  : %d", n_done)
    else:
        log.info("(Dry-run. Re-run with --apply to commit.)")

    return 0 if n_errors == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
