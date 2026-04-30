"""
ip_fetch_one.py
===============
Fetch a single variant by SKU and dump its complete JSON, then grep for
any field that might be the "Combine sales/stock" / merge relationship.

Usage
-----
    .venv\\Scripts\\python ip_fetch_one.py LED-V3000938S-20

Output
------
    output/ip_probe/variant_<SKU>.json   full variant payload
    Console: any keys / nested keys mentioning merge/combine/related/etc.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import requests
from dotenv import load_dotenv

BASE_URL = "https://app.inventory-planner.com/api/v1"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 cin7-sync/1.0"
)

KEYWORDS = (
    "merge", "combin", "linked", "related", "alt",
    "replac", "subst", "inherit", "child", "parent",
    "sibling", "from_variant", "to_variant",
)


def walk(obj, prefix=""):
    """Yield (path, value) for every leaf in a nested JSON structure."""
    if isinstance(obj, dict):
        for k, v in obj.items():
            path = f"{prefix}.{k}" if prefix else k
            yield path, v
            yield from walk(v, path)
    elif isinstance(obj, list):
        for i, item in enumerate(obj):
            path = f"{prefix}[{i}]"
            yield from walk(item, path)


def main() -> int:
    if len(sys.argv) < 2:
        print("Usage: ip_fetch_one.py <SKU>")
        return 1
    sku = sys.argv[1]

    load_dotenv()
    key = os.environ.get("IP_API_KEY")
    account = os.environ.get("IP_ACCOUNT")
    if not key or not account:
        print("ERROR: IP_API_KEY / IP_ACCOUNT not set in .env")
        return 1

    headers = {
        "Authorization": key,
        "Account": account,
        "Accept": "application/json",
        "User-Agent": USER_AGENT,
    }

    # Filter by SKU. Per IP docs, sku=<value> retrieves the variant with
    # that SKU. limit=1 because SKU should be unique.
    url = f"{BASE_URL}/variants"
    print(f"GET {url}?sku={sku}")
    resp = requests.get(url, headers=headers,
                         params={"sku": sku, "limit": 5}, timeout=30)
    print(f"  status={resp.status_code} bytes={len(resp.text)}")
    if resp.status_code != 200:
        print(resp.text[:500])
        return 1

    data = resp.json()
    out_dir = Path("output/ip_probe")
    out_dir.mkdir(parents=True, exist_ok=True)
    safe = sku.replace("/", "_")
    out_file = out_dir / f"variant_{safe}.json"
    out_file.write_text(json.dumps(data, indent=2)[:500000], encoding="utf-8")
    print(f"  saved {out_file}")

    variants = data.get("variants") or []
    if not variants:
        print(f"\nNo variant found for SKU '{sku}'. Total in response: 0")
        return 0
    print(f"\nFound {len(variants)} matching variant(s).")

    # Grep every leaf path for keywords. This catches nested fields the
    # earlier flat scan missed.
    for v in variants:
        v_sku = v.get("connections", [{}])[0].get("sku", v.get("id"))
        print(f"\n=== {v_sku} ===")
        hits = []
        for path, value in walk(v):
            lower_path = path.lower()
            if any(kw in lower_path for kw in KEYWORDS):
                # Skip the noisy "linked_by" repetitions — only show first
                # occurrence per kind
                preview = json.dumps(value)[:120] if not isinstance(
                    value, (str, int, float, bool)) else str(value)[:120]
                hits.append((path, preview))
        if not hits:
            print("  (no matches for merge/combin/linked/alt/replac/etc.)")
        else:
            seen_kinds = set()
            for path, preview in hits:
                # Just summarise — same field appearing in 4 warehouses
                # only needs to print once.
                kind = path.split("[")[0]
                if kind in seen_kinds and not isinstance(preview, str):
                    continue
                seen_kinds.add(kind)
                print(f"  {path:<60} {preview}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
