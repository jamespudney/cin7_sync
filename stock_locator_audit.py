"""stock_locator_audit.py (v2.67.184)
========================================

Audit the bin / stock-locator alignment between BOM parents and
their children.

Per James — for Topmet (and many other component-product
families), the master roll/profile and every BOM child should
share the SAME Stock Locator in CIN7. Warehouse search-by-bin
breaks when they drift: the picker walks to bin X for the
child, finds nothing because the master's stock is held at bin Y.

This module:
  1. Loads the freshest stock_on_hand_*.csv (SKU → Bin / Location)
  2. Loads the BOM map via bom_lookup
  3. For each AssemblyBOM child, compares the child's Bin
     against its parent's Bin
  4. Reports mismatches (child has Bin, parent has Bin, they
     differ) so the buyer / stock controller can fix them in
     CIN7

v2.67.184 is ALERT ONLY. A future iteration may add a
--push-to-cin7 mode that PUTs the parent's Bin onto each
mismatched child via the existing Cin7Client.put helper.

CLI:
    python stock_locator_audit.py audit               # print to stdout
    python stock_locator_audit.py audit --json        # JSON output
    python stock_locator_audit.py post-summary        # post to Slack
    python stock_locator_audit.py post-summary --dryrun

Env vars:
    SLACK_BOT_TOKEN
    SLACK_STOCK_ISSUES_CHANNEL_ID    where the summary lands
                                       (or override via flag)
    LOCATOR_AUDIT_MAX_LIST            cap on # of mismatches
                                       reported in Slack (default
                                       30; the full list goes to
                                       the log either way)
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

import pandas as pd  # noqa: E402

try:
    from data_paths import OUTPUT_DIR
except ImportError:
    OUTPUT_DIR = SCRIPT_DIR / "output"

log = logging.getLogger("stock_locator_audit")


# ---------------------------------------------------------------------------
# Data loaders
# ---------------------------------------------------------------------------
def _load_stock_bins() -> Optional[pd.DataFrame]:
    """Return the freshest stock_on_hand CSV with SKU/Bin/Location
    columns or None when no CSV exists."""
    candidates = sorted(
        OUTPUT_DIR.glob("stock_on_hand_*.csv"),
        key=lambda p: p.stat().st_mtime, reverse=True)
    if not candidates:
        log.error("No stock_on_hand_*.csv found in %s", OUTPUT_DIR)
        return None
    path = candidates[0]
    try:
        df = pd.read_csv(path, low_memory=False)
    except Exception as exc:
        log.error("Failed to read %s: %s", path, exc)
        return None
    keep = ["SKU"]
    for col in ("Bin", "Location", "OnHand", "Name"):
        if col in df.columns:
            keep.append(col)
    out = df[keep].copy()
    # Normalise SKU casing for join.
    out["SKU"] = out["SKU"].astype(str).str.strip().str.upper()
    if "Bin" in out.columns:
        out["Bin"] = (out["Bin"].fillna("").astype(str)
                          .str.strip())
    if "Location" in out.columns:
        out["Location"] = (out["Location"].fillna("").astype(str)
                              .str.strip())
    log.info("Loaded %s — %d SKU rows", path.name, len(out))
    return out


def _load_bom_pairs() -> List[Tuple[str, str]]:
    """Return (child_sku, parent_sku) pairs from the BOM. Uses the
    same bom_lookup that runtime parent-fallback code reads from,
    so the audit and the AI assistant see the same data."""
    try:
        from bom_lookup import _load, _cache  # noqa: WPS437
    except ImportError as exc:
        log.error("bom_lookup import failed: %s", exc)
        return []
    _load()
    children_of = _cache.get("children_of") or {}
    # children_of: master_sku → [{AssemblySKU: child_sku, ...}, ...]
    pairs: List[Tuple[str, str]] = []
    for master_sku, asm_rows in children_of.items():
        for row in asm_rows:
            child = (row or {}).get("AssemblySKU")
            if not child:
                continue
            pairs.append((str(child).strip().upper(),
                            str(master_sku).strip().upper()))
    # Deduplicate (same child can appear under multiple BOMTypes).
    pairs = list(set(pairs))
    log.info("BOM parent/child pairs: %d", len(pairs))
    return pairs


# ---------------------------------------------------------------------------
# Audit
# ---------------------------------------------------------------------------
def audit() -> List[dict]:
    """Return a list of mismatches:
        {child_sku, child_bin, parent_sku, parent_bin,
         child_on_hand, parent_on_hand}
    A mismatch fires when:
      • parent_bin is non-empty
      • child_bin is non-empty
      • the two strings differ (case-insensitive)
    Both-blank pairs are skipped — nothing to align. Child-blank
    pairs are skipped here (they're a separate issue: child has
    no Bin set at all). We focus on the cleanup-able mismatches."""
    stock = _load_stock_bins()
    pairs = _load_bom_pairs()
    if stock is None or not pairs:
        return []
    # Index for fast lookup
    by_sku = stock.set_index("SKU").to_dict("index")
    mismatches: List[dict] = []
    for child_sku, parent_sku in pairs:
        c = by_sku.get(child_sku) or {}
        p = by_sku.get(parent_sku) or {}
        c_bin = (c.get("Bin") or "").strip()
        p_bin = (p.get("Bin") or "").strip()
        if not p_bin or not c_bin:
            continue
        if c_bin.upper() == p_bin.upper():
            continue
        mismatches.append({
            "child_sku": child_sku,
            "child_name": c.get("Name") or "",
            "child_bin": c_bin,
            "child_on_hand": c.get("OnHand"),
            "parent_sku": parent_sku,
            "parent_name": p.get("Name") or "",
            "parent_bin": p_bin,
            "parent_on_hand": p.get("OnHand"),
        })
    log.info("Mismatches found: %d", len(mismatches))
    return mismatches


# ---------------------------------------------------------------------------
# Slack post
# ---------------------------------------------------------------------------
def _post_summary_to_slack(mismatches: List[dict],
                                channel_id: str,
                                dryrun: bool = False
                                ) -> Tuple[bool, str]:
    """Compose + post a summary. Returns (ok, detail)."""
    max_list = int(
        os.environ.get("LOCATOR_AUDIT_MAX_LIST", "30") or 30)
    n = len(mismatches)
    if n == 0:
        text = (
            "✅ *Stock-locator audit*: every BOM parent/child pair "
            "shares the same Bin. Nothing to fix.")
    else:
        # Group by parent for readability.
        by_parent: dict = {}
        for m in mismatches:
            by_parent.setdefault(
                (m["parent_sku"], m["parent_bin"]), []
            ).append(m)
        head_lines = [
            f"⚠️ *Stock-locator audit*: {n} child SKU"
            f"{'s' if n != 1 else ''} "
            f"{'have' if n != 1 else 'has'} a Bin that doesn't "
            f"match the master roll. Warehouse search-by-bin "
            f"won't find these consistently — please align them "
            f"in CIN7.",
            "",
        ]
        body_lines: List[str] = []
        listed = 0
        for (p_sku, p_bin), children in sorted(by_parent.items()):
            if listed >= max_list:
                body_lines.append(
                    f"… and {n - listed} more not shown. See "
                    "the audit log on the worker for full list.")
                break
            body_lines.append(f"*Master `{p_sku}`* — Bin *{p_bin}*")
            for c in children[:5]:  # cap children per parent
                listed += 1
                oh = c.get("child_on_hand")
                oh_str = (f"OnHand {int(oh)}"
                              if isinstance(oh, (int, float)) and oh > 0
                              else "OnHand 0")
                body_lines.append(
                    f"   ↳ `{c['child_sku']}` — Bin "
                    f"*{c['child_bin']}* · {oh_str}")
            if len(children) > 5:
                body_lines.append(
                    f"   _(+{len(children) - 5} more children "
                    f"under this master)_")
        text = "\n".join(head_lines + body_lines)
    if dryrun:
        print(text)
        return True, "dryrun"

    token = os.environ.get("SLACK_BOT_TOKEN", "").strip()
    if not token:
        return False, "SLACK_BOT_TOKEN not set"
    if not channel_id:
        return False, "no channel_id"
    try:
        import requests
        r = requests.post(
            "https://slack.com/api/chat.postMessage",
            headers={"Authorization": f"Bearer {token}"},
            json={
                "channel": channel_id,
                "text": text,
                "unfurl_links": False,
            },
            timeout=15)
        data = r.json()
        if not data.get("ok"):
            return False, f"Slack error: {data}"
        return True, str(data.get("ts"))
    except Exception as exc:
        return False, f"post failed: {exc}"


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main() -> int:
    parser = argparse.ArgumentParser(description=(
        "Audit parent/child stock-locator alignment in CIN7 BOMs."))
    sub = parser.add_subparsers(dest="cmd", required=True)
    p_aud = sub.add_parser("audit",
                              help="Run the audit and print results.")
    p_aud.add_argument("--json", action="store_true",
                          help="Output as JSON instead of text.")
    p_post = sub.add_parser("post-summary",
                                help="Post audit summary to Slack.")
    p_post.add_argument("--dryrun", action="store_true",
                            help="Print to stdout instead of Slack.")
    p_post.add_argument("--channel-id", default=None,
                            help="Override channel destination.")
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s")

    if args.cmd == "audit":
        results = audit()
        if args.json:
            print(json.dumps(results, indent=2))
        else:
            print(f"Mismatches: {len(results)}")
            for m in results[:50]:
                oh_c = m.get("child_on_hand")
                oh_p = m.get("parent_on_hand")
                print(f"  {m['child_sku']:<40s} "
                          f"bin={m['child_bin']!r:<8s} "
                          f"(OnHand={oh_c}) "
                          f"|| master {m['parent_sku']} "
                          f"bin={m['parent_bin']!r:<8s} "
                          f"(OnHand={oh_p})")
            if len(results) > 50:
                print(f"  … and {len(results) - 50} more")
        return 0

    if args.cmd == "post-summary":
        results = audit()
        channel = (args.channel_id
                      or os.environ.get(
                          "SLACK_STOCK_ISSUES_CHANNEL_ID",
                          "").strip())
        ok, detail = _post_summary_to_slack(
            results, channel, dryrun=args.dryrun)
        log.info("Slack post: ok=%s detail=%s", ok, detail)
        return 0 if ok else 1

    return 2


if __name__ == "__main__":
    sys.exit(main())
