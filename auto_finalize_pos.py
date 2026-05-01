"""
auto_finalize_pos.py
====================
Phase 3 of the PO push lifecycle: auto-detect when a submitted CIN7
PO has been authorised (DRAFT → ORDERED / RECEIVING / COMPLETED) and
transition our local po_drafts row to status='finalized'.

Local lifecycle:
    editing → submitted → finalized → (archived)
                ↑           ↑
                |           this script
                cin7_post_po.py

How it works:
1. Read db.po_drafts WHERE status='submitted' AND cin7_po_id IS NOT NULL.
2. For each, look up the corresponding CIN7 PO in the latest
   purchases_*.csv file (already on disk from the daily sync).
3. If the CIN7 status is ORDERED, RECEIVING, COMPLETED, or any other
   "past-DRAFT" status, transition the local draft to finalized via
   db.mark_po_draft_finalized().
4. If the CIN7 PO was VOIDED, transition local to cancelled instead.
5. Log everything to output/auto_finalize.log + audit_log table.

Recommended cadence: nightly via daily_sync.sh (runs after the
purchase header sync that gives us the fresh status data).

Usage
-----
    .venv\\Scripts\\python auto_finalize_pos.py             # apply
    .venv\\Scripts\\python auto_finalize_pos.py --dry-run   # log only
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

import db
from data_paths import OUTPUT_DIR


LOG_PATH = OUTPUT_DIR / "auto_finalize.log"

# CIN7 PO statuses that mean "past DRAFT" — any of these triggers
# the transition to finalized locally. Pulled from CIN7's docs:
# DRAFT/AUTHORISED/ORDERED/RECEIVING/RECEIVED/COMPLETED/VOIDED.
FINALIZED_STATUSES = {
    "AUTHORISED", "ORDERED", "RECEIVING",
    "RECEIVED", "COMPLETED",
    # Combined statuses CIN7 sometimes returns
    "PARTIALLY RECEIVED", "FULLY RECEIVED",
    "INVOICED", "INVOICED / CREDITED",
    "PAID", "PARTIALLY PAID",
}
CANCELLED_STATUSES = {
    "VOIDED", "CANCELLED", "CANCELED",
}


def _setup_log() -> logging.Logger:
    log = logging.getLogger("auto_finalize_pos")
    log.setLevel(logging.INFO)
    if not log.handlers:
        fh = logging.FileHandler(LOG_PATH, encoding="utf-8")
        fh.setFormatter(logging.Formatter(
            "%(asctime)s  %(levelname)-7s  %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%SZ"))
        log.addHandler(fh)
        sh = logging.StreamHandler()
        sh.setFormatter(logging.Formatter("%(message)s"))
        log.addHandler(sh)
    return log


def _latest_purchases_csv() -> Path | None:
    """Find the most recent purchases_*.csv on disk. The daily sync
    drops one of these per run (purchases_last_3d_<stamp>.csv etc).
    We pick the most recent regardless of window length."""
    candidates = sorted(
        OUTPUT_DIR.glob("purchases_last_*d_*.csv"),
        key=lambda p: p.stat().st_mtime,
        reverse=True)
    return candidates[0] if candidates else None


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Auto-finalize submitted CIN7 POs (Phase 3)")
    parser.add_argument(
        "--apply", action="store_true",
        help="Apply transitions. Without this we dry-run.")
    parser.add_argument(
        "--actor", default="auto_finalize",
        help="Audit actor name. Default: 'auto_finalize'.")
    args = parser.parse_args()

    log = _setup_log()
    log.info("=" * 60)
    log.info("auto_finalize_pos start (%s)",
              "APPLY" if args.apply else "DRY-RUN")

    # Load submitted drafts
    submitted = [
        dict(d) for d in db.list_po_drafts(status="submitted")
    ]
    submitted = [d for d in submitted if d.get("cin7_po_id")]
    log.info("Submitted drafts with CIN7 PO IDs: %d", len(submitted))
    if not submitted:
        log.info("Nothing to do.")
        return 0

    # Load latest purchases CSV — that's where CIN7 status lives.
    pcsv = _latest_purchases_csv()
    if not pcsv:
        log.error("No purchases_*.csv on disk. Run cin7_sync.py "
                   "purchases first.")
        return 1
    log.info("Using purchases CSV: %s", pcsv.name)
    purchases = pd.read_csv(pcsv, low_memory=False)
    if purchases.empty:
        log.error("Purchases CSV is empty.")
        return 1
    # Build lookup by CIN7 PO ID
    if "ID" not in purchases.columns:
        log.error("Purchases CSV missing ID column.")
        return 1
    purchases["ID"] = purchases["ID"].astype(str)
    by_id = {str(r["ID"]): dict(r)
             for _, r in purchases.iterrows()}

    # For each submitted draft, look up the current CIN7 status.
    n_finalized = 0
    n_cancelled = 0
    n_still_draft = 0
    n_not_found = 0
    for draft in submitted:
        draft_id = draft["id"]
        cin7_id = str(draft["cin7_po_id"])
        po = by_id.get(cin7_id)
        if not po:
            n_not_found += 1
            log.info("  draft #%s -> CIN7 PO %s NOT in latest "
                      "purchases CSV (status unknown — possibly older "
                      "than sync window)", draft_id, cin7_id)
            continue
        cin7_status = str(po.get("Status", "")).strip().upper()

        if cin7_status in FINALIZED_STATUSES:
            log.info("  draft #%s -> CIN7 PO %s status=%s (finalize)",
                      draft_id, cin7_id, cin7_status)
            if args.apply:
                db.mark_po_draft_finalized(
                    draft_id, actor=args.actor,
                    cin7_po_status=cin7_status)
            n_finalized += 1
        elif cin7_status in CANCELLED_STATUSES:
            log.info("  draft #%s -> CIN7 PO %s status=%s (cancel)",
                      draft_id, cin7_id, cin7_status)
            if args.apply:
                db.cancel_po_draft(
                    draft_id, actor=args.actor,
                    reason=f"CIN7 PO voided (status={cin7_status})")
            n_cancelled += 1
        else:
            n_still_draft += 1
            log.info("  draft #%s -> CIN7 PO %s status=%s (still draft)",
                      draft_id, cin7_id, cin7_status)

    log.info("Summary: %d finalized, %d cancelled, %d still draft, "
              "%d not in CSV (out of %d submitted)",
              n_finalized, n_cancelled, n_still_draft,
              n_not_found, len(submitted))
    if not args.apply and (n_finalized + n_cancelled) > 0:
        log.info("Re-run with --apply to commit %d transitions.",
                  n_finalized + n_cancelled)
    return 0


if __name__ == "__main__":
    sys.exit(main())
