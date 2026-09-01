"""fablab_corner_autotag.py

Auto-tags new 865FabLab premade-corner products in CIN7 and adds them
to the app's own "865FabLab build" flag list (app_pages/fablab_work_
orders.py), so a new corner variant shows up in the production
planner without anyone touching CIN7 by hand.

Criteria (James, 2026-09-01): Brand == "Wired4Signs USA" AND "corner
connector" appears anywhere in Name (case-insensitive). CIN7's own
Name filter is a PREFIX match, not substring -- a search for "Corner
Connector" silently misses "90 Degree Corner Connector for ..." --
so this pulls the full product catalog and filters locally instead.

Only ever auto-assigns SR200 ("Premade corner with diffuser"), and
only when both are true:
  - AdditionalAttribute2 is currently blank. Never overwrites an
    existing code -- including a human re-tag to SR201/SR202, or a
    previous run of this same script.
  - The product's Description mentions "diffuser". Some "Corner
    Connector"-named products are plain 3D-printed plastic joiners
    with no diffuser at all (confirmed on the Dubai/Alu-Flat SKUs,
    2026-09-01) -- SR200's build instructions start with "fit the
    diffuser", which doesn't apply to those, so they must NOT be
    auto-tagged. A match that fails this check is logged and left
    completely untouched (no rule, no supplier, no flag) for a human
    to classify manually.

Suppliers: only attaches the 865FabLab supplier when the product's
own Suppliers list is currently empty; never overwrites an existing
supplier assignment.

CLI:
  python fablab_corner_autotag.py run
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import sys
from typing import Any

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)

import db  # noqa: E402
from cin7_sync import Cin7Client  # noqa: E402
from engine.sku_rules import CORNER_BOM_RULES  # noqa: E402

log = logging.getLogger("fablab_corner_autotag")

TARGET_BRAND = "wired4signs usa"
NAME_SUBSTRING = "corner connector"
RULE_CODE = "SR200"
RULE_NAME = CORNER_BOM_RULES[RULE_CODE]["name"]
ATTRIBUTE_SET = "Product Additional Attributes"
FABLAB_SUPPLIER_NAME = "865FabLab"
FABLAB_FLAG_TYPE = "865FabLab build"
ACTOR = "fablab_corner_autotag"


def _strip_html(value: Any) -> str:
    text = re.sub(r"<[^<]+?>", " ", str(value or ""))
    return re.sub(r"\s+", " ", text).strip()


def _make_client() -> Cin7Client:
    account_id = os.environ.get("CIN7_ACCOUNT_ID", "").strip()
    application_key = os.environ.get("CIN7_APPLICATION_KEY", "").strip()
    if not account_id or not application_key:
        raise SystemExit(
            "Missing CIN7_ACCOUNT_ID / CIN7_APPLICATION_KEY env vars.")
    return Cin7Client(account_id, application_key, rate_seconds=1.2)


def _find_fablab_supplier_id(client: Cin7Client) -> str:
    resp = client.get("supplier", params={"Name": FABLAB_SUPPLIER_NAME,
                                            "Page": 1, "Limit": 5})
    for s in resp.get("SupplierList", []):
        if (s.get("Name") or "").strip().lower() == FABLAB_SUPPLIER_NAME.lower():
            return s["ID"]
    raise RuntimeError(
        f"No CIN7 supplier named {FABLAB_SUPPLIER_NAME!r} found -- "
        "create it in CIN7 first.")


def find_candidate_products(client: Cin7Client) -> list[dict]:
    """Full-catalog scan (see module docstring for why -- CIN7's Name
    filter is prefix-only and would silently under-match)."""
    matches = []
    for p in client.paginate("product", result_key="Products"):
        brand = (p.get("Brand") or "").strip().lower()
        name = (p.get("Name") or "").lower()
        if brand != TARGET_BRAND or NAME_SUBSTRING not in name:
            continue
        if (p.get("Status") or "").strip().lower() != "active":
            continue
        matches.append(p)
    return matches


def run(apply: bool = True) -> dict:
    client = _make_client()
    log.info("Scanning full CIN7 product catalog for candidates...")
    candidates = find_candidate_products(client)
    log.info("Found %d candidate product(s) (Brand=%s, name contains %r)",
              len(candidates), TARGET_BRAND, NAME_SUBSTRING)

    already_tagged = [p for p in candidates
                       if (p.get("AdditionalAttribute2") or "").strip()]
    untagged = [p for p in candidates
                 if not (p.get("AdditionalAttribute2") or "").strip()]

    to_tag = []
    skipped_no_diffuser = []
    for p in untagged:
        if "diffuser" in _strip_html(p.get("Description")).lower():
            to_tag.append(p)
        else:
            skipped_no_diffuser.append(p)

    if skipped_no_diffuser:
        log.info(
            "Skipping %d product(s) with no 'diffuser' in description "
            "-- not tagged, needs manual classification: %s",
            len(skipped_no_diffuser),
            ", ".join(p["SKU"] for p in skipped_no_diffuser))

    if not to_tag:
        log.info("Nothing new to tag.")
        return {"tagged": [], "skipped_no_diffuser": skipped_no_diffuser,
                 "already_tagged": len(already_tagged)}

    fablab_supplier_id = _find_fablab_supplier_id(client)
    log.info("Tagging %d new corner product(s) as %s...",
              len(to_tag), RULE_CODE)

    tagged = []
    for p in to_tag:
        sku = p["SKU"]
        fields = {
            "AttributeSet": ATTRIBUTE_SET,
            "AdditionalAttribute1": RULE_NAME,
            "AdditionalAttribute2": RULE_CODE,
        }
        if not p.get("Suppliers"):
            fields["Suppliers"] = [{"SupplierID": fablab_supplier_id}]
        if apply:
            client.update_product(p["ID"], fields)
            db.set_flag(sku, FABLAB_FLAG_TYPE, ACTOR,
                        notes=f"Auto-tagged {RULE_CODE} "
                              "(fablab_corner_autotag.py)")
        log.info("Tagged %s -> %s", sku, RULE_CODE)
        tagged.append(sku)

    return {"tagged": tagged, "skipped_no_diffuser": skipped_no_diffuser,
             "already_tagged": len(already_tagged)}


def _setup_log(verbose: bool = False) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        stream=sys.stdout, force=True)


def cmd_run(args: argparse.Namespace) -> int:
    _setup_log(args.verbose)
    result = run(apply=not args.dry_run)
    log.info("Done. Tagged %d, skipped (no diffuser) %d, already tagged %d.",
              len(result["tagged"]), len(result["skipped_no_diffuser"]),
              result["already_tagged"])
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--verbose", action="store_true")
    sub = parser.add_subparsers(dest="command", required=True)

    p_run = sub.add_parser("run", help="Scan and tag new corner products")
    p_run.add_argument("--dry-run", action="store_true",
                        help="Log what would be tagged without writing")
    p_run.set_defaults(func=cmd_run)

    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
