"""
ip_probe.py
===========
One-shot probe of the Inventory Planner public API to discover:
  - whether our auth is working,
  - which endpoints are accessible,
  - what the JSON shapes look like (so we can build the proper puller).

Reads IP_API_KEY and IP_ACCOUNT from .env.

Usage
-----
    cd C:\\Tools\\cin7_sync
    .venv\\Scripts\\python ip_probe.py

Output
------
    output/ip_probe/<endpoint>.json    raw response (first page) per endpoint
    output/ip_probe/_summary.txt       human-readable status table
    Console output mirrors _summary.txt
"""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime
from pathlib import Path

import requests
from dotenv import load_dotenv

# IP's public API is at v1 (per docs:
# https://help.inventory-planner.com/en/articles/674662-inventory-planner-public-apis).
# Earlier 403s were because we were hitting /api/v3/, which Cloudflare blocks
# as an unrecognised path. v1 is the only version exposed.
CANDIDATE_BASES = [
    "https://app.inventory-planner.com/api/v1",
]

# A real-browser-ish User-Agent dodges Cloudflare's default-block of empty UAs.
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 cin7-sync/1.0"
)

# Endpoints we want to probe. IP's `fields` param is an allowlist (not an
# expand), and there's no `*` shortcut — so to discover what fields exist,
# we have to either (a) GET the single-variant resource (typically returns
# the full object) or (b) explicitly request candidate field names.
#
# The "fields_explicit" probe asks for every field name that might exist
# on a variant — IP returns just the ones that are real, ignoring the
# rest. Whatever comes back is the schema we have to work with.
CANDIDATE_FIELDS = ",".join([
    "id", "sku", "title", "name", "barcode", "vendor", "vendor_id",
    "vendor_name", "category", "categories", "tags", "store",
    "warehouse", "lead_time", "review_period", "replenishment",
    "stock", "available", "incoming", "demand", "min_replenishment",
    "max_replenishment", "abc", "abc_class",
    # The big questions — anything resembling alternates or notes:
    "notes", "note", "comment", "comments", "memo", "remark", "remarks",
    "alternates", "alternatives", "replacements", "substitutes", "subs",
    "alts", "related", "swap", "swap_with", "interchangeable",
    "custom_fields", "custom_field_1", "metadata", "internal_notes",
    "vendor_notes", "purchasing_notes", "buyer_notes",
])

PROBES = [
    ("variants", {"limit": 2}, "variants", "Default variant fields"),
    ("variants", {"limit": 1, "fields": CANDIDATE_FIELDS}, "variants_explicit",
        "Variant with every plausible alternates/notes field requested"),
    ("variant-vendors", {"limit": 5}, "variant_vendors", "Per-variant vendor mapping"),
    ("purchase-orders", {"limit": 2}, "purchase_orders", "PO list"),
]


def main() -> int:
    load_dotenv()
    key = os.environ.get("IP_API_KEY")
    account = os.environ.get("IP_ACCOUNT")
    if not key or not account:
        print("ERROR: IP_API_KEY and IP_ACCOUNT must be set in .env")
        return 1

    headers = {
        "Authorization": key,
        "Account": account,
        "Accept": "application/json",
        "User-Agent": USER_AGENT,
    }

    out_dir = Path("output/ip_probe")
    out_dir.mkdir(parents=True, exist_ok=True)

    # Step 1: find which base URL actually accepts our auth (look for 200 on
    # /variants — the most universally-present endpoint).
    base_url = None
    print(f"Account: {account}")
    print("\nFinding the live base URL...")
    for candidate in CANDIDATE_BASES:
        try:
            r = requests.get(
                f"{candidate}/variants",
                headers=headers,
                params={"limit": 1},
                timeout=15,
            )
            preview = r.text[:80].replace("\n", " ")
            print(f"  {r.status_code}  {candidate}   {preview}")
            if r.status_code == 200:
                base_url = candidate
                break
        except requests.RequestException as exc:
            print(f"  ERR  {candidate}   {exc}")

    if base_url is None:
        print(
            "\nNo candidate base URL returned 200. Either auth is being "
            "rejected or the API host is different from any of our guesses. "
            "Inspect output/ip_probe/<endpoint>.json — if it's HTML, IP's "
            "CDN is blocking; if it's a JSON error, auth/path is the issue."
        )
        # Fall through anyway — we'll still try the first candidate so the
        # raw output is captured for inspection.
        base_url = CANDIDATE_BASES[0]

    results = []
    print(f"\nProbing {base_url} as account={account}\n")
    print(f"{'endpoint':<22} {'status':<8} {'records':<10} notes")
    print("-" * 80)

    for path, params, label, why in PROBES:
        url = f"{base_url}/{path}"
        try:
            resp = requests.get(url, headers=headers, params=params, timeout=30)
            status = resp.status_code
            body_preview = resp.text[:300]
            try:
                payload = resp.json()
            except ValueError:
                payload = None

            # Try to count records under common keys
            count = "—"
            if isinstance(payload, dict):
                for key_name in (path, "items", "results", "data", label):
                    if key_name in payload and isinstance(payload[key_name], list):
                        count = str(len(payload[key_name]))
                        break
            elif isinstance(payload, list):
                count = str(len(payload))

            # Save raw response for inspection. JSON gets pretty-printed; HTML
            # / other text gets the FULL body so we can see Cloudflare error
            # codes, ray IDs, etc.
            out_file = out_dir / f"{label}.json"
            if payload is not None:
                out_file.write_text(
                    json.dumps(payload, indent=2)[:50000],
                    encoding="utf-8",
                )
            else:
                # Save with .html extension if it's clearly HTML
                ext = "html" if "<html" in resp.text[:200].lower() else "txt"
                out_file = out_dir / f"{label}.{ext}"
                out_file.write_text(resp.text[:200000], encoding="utf-8")

            note = "OK" if status == 200 else body_preview[:50]
            print(f"{path:<22} {status:<8} {count:<10} {note}")
            results.append((path, status, count, note, why))
        except requests.RequestException as exc:
            print(f"{path:<22} ERR      —          {exc}")
            results.append((path, "ERR", "—", str(exc), why))

    # Summary file
    summary = out_dir / "_summary.txt"
    with summary.open("w", encoding="utf-8") as fh:
        fh.write(f"IP probe @ {datetime.now().isoformat()}\n")
        fh.write(f"Base: {base_url}  Account: {account}\n\n")
        for path, status, count, note, why in results:
            fh.write(f"{path:<22} {str(status):<8} records={count:<6} | {why}\n")
            fh.write(f"   note: {note}\n\n")

    print(f"\nRaw responses saved to {out_dir}/")
    print(f"Summary:                {summary}")

    # Step 3: GET a single variant by ID. Single-resource GETs usually
    # return the whole object (no field filtering applied), so this is
    # our best shot at seeing the complete schema.
    sample_id = None
    try:
        v_data = json.loads(
            (out_dir / "variants.json").read_text(encoding="utf-8"))
        for k in ("variants", "items", "results", "data"):
            if isinstance(v_data, dict) and isinstance(v_data.get(k), list):
                if v_data[k]:
                    sample_id = v_data[k][0].get("id")
                    break
    except Exception:
        pass

    if sample_id:
        print(f"\nFetching single variant by ID ({sample_id})...")
        try:
            resp = requests.get(
                f"{base_url}/variants/{sample_id}",
                headers=headers, timeout=30)
            print(f"  status={resp.status_code} bytes={len(resp.text)}")
            try:
                payload = resp.json()
                (out_dir / "variant_single.json").write_text(
                    json.dumps(payload, indent=2)[:200000],
                    encoding="utf-8")
            except ValueError:
                (out_dir / "variant_single.txt").write_text(
                    resp.text[:50000], encoding="utf-8")
        except requests.RequestException as exc:
            print(f"  ERR {exc}")

    # Special peek: scan WHICHEVER variant response is richest, looking for
    # field names that might contain alternates/notes/custom data.
    var_file = None
    for candidate in ("variant_single.json",
                       "variants_explicit.json",
                       "variants.json"):
        f = out_dir / candidate
        if f.exists():
            try:
                d = json.loads(f.read_text(encoding="utf-8"))
                # Pick the file with the most keys on its sample
                size = 0
                if isinstance(d, dict):
                    for k in ("variant", "variants", "items"):
                        if k in d:
                            v = d[k]
                            if isinstance(v, list) and v:
                                size = len(v[0])
                                break
                            elif isinstance(v, dict):
                                size = len(v)
                                break
                if size > 0 and (var_file is None or size > 0):
                    var_file = f
                    if size >= 10:
                        break
            except Exception:
                continue
    if var_file is None:
        var_file = out_dir / "variants.json"
    print(f"\nScanning {var_file.name} for field schema...")
    if var_file.exists():
        try:
            data = json.loads(var_file.read_text(encoding="utf-8"))
            sample = None
            if isinstance(data, dict):
                for k in ("variants", "variant", "items", "results", "data"):
                    if k in data:
                        if isinstance(data[k], list) and data[k]:
                            sample = data[k][0]
                            break
                        elif isinstance(data[k], dict):
                            sample = data[k]
                            break
            elif isinstance(data, list) and data:
                sample = data[0]
            if isinstance(sample, dict):
                print("\nAll variant keys (alphabetical):")
                for k in sorted(sample.keys()):
                    v = sample[k]
                    preview = (
                        json.dumps(v)[:80] if not isinstance(v, str)
                        else v[:80]
                    )
                    print(f"  {k:<32} {preview}")

                # Highlight anything that smells like alternates / notes
                interesting = [
                    k for k in sample.keys()
                    if any(kw in k.lower() for kw in (
                        "note", "comment", "alt", "replac", "subst",
                        "custom", "tag", "memo", "remark"))
                ]
                print("\nFields likely to contain curated data:")
                if interesting:
                    for k in interesting:
                        v = sample[k]
                        preview = (
                            json.dumps(v)[:120] if not isinstance(v, str)
                            else v[:120]
                        )
                        print(f"  {k:<32} {preview}")
                else:
                    print("  (none found — alternates/notes likely NOT in API)")
        except Exception as exc:
            print(f"\n(couldn't parse variants response: {exc})")

    return 0


if __name__ == "__main__":
    sys.exit(main())
