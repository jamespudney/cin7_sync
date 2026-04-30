"""
cin7_put_test.py
================
Diagnose why AdditionalAttribute5 PUTs aren't sticking. Tries 4
progressively-fatter PUT bodies on the same SKU and verifies after
each whether AA5 changed in CIN7. Prints the PUT response body for
each attempt so we can see if CIN7 returned a hidden error.

Test SKU is hard-coded to LED-M200L24DC (the user's failing example).
The test value rotates through TEST_A / TEST_B / TEST_C / TEST_D so
we can tell which attempt (if any) stuck.

USAGE
-----
    .venv\\Scripts\\python cin7_put_test.py
"""

from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path

import requests
from dotenv import load_dotenv

BASE_URL = "https://inventory.dearsystems.com/ExternalApi/v2"
TEST_SKU = "LED-M200L24DC"


def _parse_retry_after(value, default: int = 30) -> int:
    if value is None:
        return default
    digits = "".join(ch for ch in str(value) if ch.isdigit())
    return int(digits) if digits else default


def _request_with_retry(method, url, headers, **kwargs):
    """GET or PUT with 429 retry and Retry-After parsing."""
    for attempt in range(5):
        r = requests.request(method, url, headers=headers,
                              timeout=30, **kwargs)
        if r.status_code != 429:
            return r
        wait = _parse_retry_after(r.headers.get("Retry-After"), 60)
        print(f"   ... 429 — sleeping {wait}s (attempt {attempt + 1}/5)")
        time.sleep(wait)
    return r  # last response, even if 429


def get_full(sku: str, headers) -> dict:
    """Fetch the SKU's full product detail (with 429 retry)."""
    r = _request_with_retry(
        "GET", f"{BASE_URL}/product",
        headers, params={"Sku": sku, "Limit": 1})
    r.raise_for_status()
    prods = r.json().get("Products") or []
    if not prods:
        raise RuntimeError(f"SKU {sku} not found")
    p = prods[0]
    pid = p.get("ID") or p.get("ProductID")
    r2 = _request_with_retry(
        "GET", f"{BASE_URL}/product",
        headers, params={"ID": pid})
    r2.raise_for_status()
    out = r2.json()
    if "Products" in out and isinstance(out["Products"], list):
        return out["Products"][0]
    return out


def attempt_put(label: str, body: dict, headers) -> tuple:
    """Send the PUT, return (status, body_text, body_json_or_None).
    Also prints the AA5 value from the response body if present."""
    print(f"\n--- Attempt: {label} ---")
    print(f"PUT body keys: {list(body.keys())}")
    r = _request_with_retry(
        "PUT", f"{BASE_URL}/product", headers, json=body)
    print(f"Status: {r.status_code}")
    # Print AA5 from the response body if we can find it
    try:
        body_json = r.json()
        prods = body_json.get("Products") or [body_json]
        if prods and isinstance(prods[0], dict):
            aa5_in_resp = prods[0].get("AdditionalAttribute5")
            print(f"Response AA5: {aa5_in_resp!r}")
        return r.status_code, r.text, body_json
    except (ValueError, AttributeError):
        print(f"Response (first 800): {r.text[:800]}")
        return r.status_code, r.text, None


def main() -> int:
    load_dotenv()
    account_id = os.environ.get("CIN7_ACCOUNT_ID")
    app_key = os.environ.get("CIN7_APPLICATION_KEY")
    if not account_id or not app_key:
        print("ERROR: CIN7_ACCOUNT_ID / CIN7_APPLICATION_KEY missing")
        return 1
    headers = {
        "api-auth-accountid": account_id,
        "api-auth-applicationkey": app_key,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    print(f"\nFetching baseline for {TEST_SKU}...")
    p0 = get_full(TEST_SKU, headers)
    pid = p0.get("ID")
    aa5_before = p0.get("AdditionalAttribute5")
    aset = p0.get("AttributeSet")
    print(f"  ProductID    = {pid}")
    print(f"  AttributeSet = {aset}")
    print(f"  AA5 before   = {aa5_before!r}")

    # Attempt 1: minimal body (current production code) ----
    time.sleep(3.0)
    body1 = {"ID": pid, "AdditionalAttribute5": "TEST_A"}
    attempt_put("1. Minimal {ID, AA5}", body1, headers)

    time.sleep(3.0)
    p1 = get_full(TEST_SKU, headers)
    aa5_after_1 = p1.get("AdditionalAttribute5")
    print(f"  AA5 after attempt 1 = {aa5_after_1!r} "
           f"(expected TEST_A; {'STUCK' if aa5_after_1 == 'TEST_A' else 'DID NOT STICK'})")

    # Attempt 2: AttributeSet WITH double underscores ----
    # CIN7's internal canonical name appears to wrap the user-facing
    # name in '__...__'. The API GET strips them on read; PUT may need
    # them on write to recognize the attribute set context.
    time.sleep(3.0)
    aset_underscored = (
        f"__{aset}__" if aset and not (aset.startswith("__") and aset.endswith("__"))
        else aset)
    body2 = {
        "ID": pid,
        "SKU": TEST_SKU,
        "AttributeSet": aset_underscored,
        "AdditionalAttribute5": "TEST_B",
    }
    attempt_put(f"2. {{ID, SKU, AttributeSet={aset_underscored!r}, AA5}}",
                 body2, headers)

    time.sleep(3.0)
    p2 = get_full(TEST_SKU, headers)
    aa5_after_2 = p2.get("AdditionalAttribute5")
    print(f"  AA5 after attempt 2 = {aa5_after_2!r} "
           f"(expected TEST_B; {'STUCK' if aa5_after_2 == 'TEST_B' else 'DID NOT STICK'})")

    # Attempt 3: AttributeSet bare (no underscores) ----
    time.sleep(3.0)
    body3 = {
        "ID": pid,
        "SKU": TEST_SKU,
        "AttributeSet": aset,
        "AdditionalAttribute5": "TEST_C",
    }
    attempt_put(f"3. {{ID, SKU, AttributeSet={aset!r}, AA5}}",
                 body3, headers)

    time.sleep(3.0)
    p3 = get_full(TEST_SKU, headers)
    aa5_after_3 = p3.get("AdditionalAttribute5")
    print(f"  AA5 after attempt 3 = {aa5_after_3!r} "
           f"(expected TEST_C; {'STUCK' if aa5_after_3 == 'TEST_C' else 'DID NOT STICK'})")

    # Attempt 4: full product object echo + underscored AttributeSet ---
    time.sleep(3.0)
    body4 = dict(p3)
    body4["AttributeSet"] = aset_underscored
    body4["AdditionalAttribute5"] = "TEST_D"
    attempt_put("4. Full echo + underscored AttributeSet", body4, headers)

    time.sleep(3.0)
    p4 = get_full(TEST_SKU, headers)
    aa5_after_4 = p4.get("AdditionalAttribute5")
    print(f"  AA5 after attempt 4 = {aa5_after_4!r} "
           f"(expected TEST_D; {'STUCK' if aa5_after_4 == 'TEST_D' else 'DID NOT STICK'})")

    # Restore the intended value
    print("\nRestoring intended value LED-MX1-200W-24...")
    time.sleep(3.0)
    # Use whichever attempt worked; if none worked, restore via attempt 3 shape
    chosen_body = dict(p4)
    chosen_body["AdditionalAttribute5"] = "LED-MX1-200W-24"
    attempt_put("5. Restore", chosen_body, headers)

    time.sleep(3.0)
    p5 = get_full(TEST_SKU, headers)
    print(f"  Final AA5 = {p5.get('AdditionalAttribute5')!r}")

    print("\n=== Summary ===")
    print(f"  Attempt 1 (minimal):              {'✓' if aa5_after_1 == 'TEST_A' else '✗'}")
    print(f"  Attempt 2 (with AttributeSet):    {'✓' if aa5_after_2 == 'TEST_B' else '✗'}")
    print(f"  Attempt 3 (full echo):            {'✓' if aa5_after_3 == 'TEST_C' else '✗'}")
    print(f"  Attempt 4 (minimal + Name):       {'✓' if aa5_after_4 == 'TEST_D' else '✗'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
