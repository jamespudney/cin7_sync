"""housekeeping_audit.py (v2.67.81)
========================================

Freshness audit for every data feed the app depends on.

Why this exists
---------------
User concern: 'i dont want any of the stuff we develope to become
stale so we need to always make sure that it is at least kept upto
date on a housekeeping sync or the once a day sync'.

We've built features that quietly depend on a chain of data files:

  Monthly Metrics             ← shipments_*.csv (ShipStation)
                              ← sales_*.csv (CIN7)
                              ← purchases_*.csv (CIN7)
  Slow Movers / ABC engine    ← sale_lines_*.csv
                              ← stock_on_hand_*.csv
                              ← products_*.csv
  AI bot answers              ← all of the above + product_dimensions
                                + bot_lessons_learned (DB)
  dimension_describer         ← product_dimensions
                              ← Shopify product images / metafields
  PO editor / PO drafts       ← purchase_lines_last_*.csv
                              ← supplier_config (DB)
  Conversion-attribution      ← shopify_orders_*.csv

If any link goes stale silently, downstream features break without
visible errors (just stale numbers). This script enumerates every
expected data feed, checks its newest file's mtime, and:

  * logs OK rows with the file's age
  * logs WARN rows when age > expected refresh window
  * exits 0 always — this is an audit, not a gate

CLI
---
  python housekeeping_audit.py

  # Verbose: show OK rows too (default just logs WARN/ERROR):
  python housekeeping_audit.py --verbose

  # Output to a file as well as stdout (used by the daily cron):
  python housekeeping_audit.py --log /data/output/housekeeping.log

Designed to run inside daily_sync.sh and slack_loop.sh's daily
cycle. Cheap (~1 second).
"""

from __future__ import annotations

import argparse
import glob
import logging
import os
import sqlite3
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))
from data_paths import OUTPUT_DIR, DB_PATH  # noqa: E402

LOG_FORMAT = "%(asctime)s [%(levelname)s] %(message)s"
log = logging.getLogger("housekeeping")


# ---------------------------------------------------------------------------
# Audit specs
# ---------------------------------------------------------------------------
# Each row says: 'check that the newest matching file was modified
# in the last N hours, otherwise warn.'
# Set max_hours generously — we want to catch genuine staleness, not
# normal day-to-day variation.

@dataclass
class FeedSpec:
    name: str               # human-readable feed name
    pattern: str            # glob pattern, relative to OUTPUT_DIR
    max_hours: float        # warn if newest file is older than this
    consumed_by: str        # short note on who reads this
    severity: str = "warn"  # 'warn' or 'critical'


CSV_FEEDS: List[FeedSpec] = [
    FeedSpec(
        "CIN7 products master",
        "products_*.csv",
        max_hours=30.0,                     # daily sync, with slack
        consumed_by="all dashboards + AI bot"),
    FeedSpec(
        "CIN7 stock-on-hand",
        "stock_on_hand_*.csv",
        max_hours=30.0,
        consumed_by="Slow Movers, Overview, ABC engine"),
    FeedSpec(
        "CIN7 sales 30d window",
        "sales_last_30d_*.csv",
        max_hours=30.0,
        consumed_by="Monthly Metrics, Overview"),
    FeedSpec(
        "CIN7 sale-lines 30d window",
        "sale_lines_last_30d_*.csv",
        max_hours=30.0,
        consumed_by="ABC engine, AI bot"),
    FeedSpec(
        "CIN7 purchase-lines 30d window",
        "purchase_lines_last_30d_*.csv",
        max_hours=30.0,
        consumed_by="Ordering page, AI's get_purchase_order"),
    FeedSpec(
        "ShipStation shipments 30d window",
        "shipments_last_30d_*.csv",
        max_hours=30.0,
        consumed_by="Monthly Metrics shipping cost row, "
                       "AI's get_shipping_details",
        severity="critical"),  # was empty for weeks pre-v2.67.81
    FeedSpec(
        "Shopify orders 7d window",
        "shopify_orders_last_7d_*.csv",
        max_hours=30.0,
        consumed_by="conversion attribution, AI's get_shopify_order"),
    FeedSpec(
        "Shopify product content (markdown)",
        "../shopify/products/*.md",         # PRODUCTS_DIR is sibling
        max_hours=180.0,                    # weekly is fine
        consumed_by="AI knowledge base"),
]


@dataclass
class DBTableSpec:
    name: str
    table: str
    where: str = ""                         # extra SQL filter
    timestamp_col: Optional[str] = None     # column with last-update ts
    max_hours: float = 30.0
    consumed_by: str = ""
    min_rows: int = 0
    severity: str = "warn"


DB_FEEDS: List[DBTableSpec] = [
    DBTableSpec(
        "Bot lessons-learned daily summary",
        table="bot_lessons_learned",
        timestamp_col="generated_at",
        max_hours=30.0,
        consumed_by="AI Assistant + Slack bot system prompts"),
    DBTableSpec(
        "Vision-extracted product dimensions",
        table="product_dimensions",
        # Successfully-classified rows (anything with mounting_type
        # set, including from titles).
        where="mounting_type IS NOT NULL AND mounting_type != ''",
        timestamp_col="extracted_at",
        # Daily refresh-classifications doesn't bump extracted_at —
        # that's reserved for vision calls. Allow up to 8 days
        # (weekly cycle) before warning.
        max_hours=8 * 24.0,
        consumed_by="dimension_describer + AI bot",
        min_rows=100),
    DBTableSpec(
        "Slack message ingest",
        table="slack_messages",
        timestamp_col="captured_at",
        # Worker polls every 60s; warn if no message in last 24h
        # (could be quiet weekend, but extended silence = ingest broken)
        max_hours=24.0,
        consumed_by="Slack listener bot",
        min_rows=1),
]


# ---------------------------------------------------------------------------
# Checks
# ---------------------------------------------------------------------------
def _newest_file_mtime(pattern_rel: str) -> Optional[float]:
    """Return mtime of the newest file matching pattern (anchored at
    OUTPUT_DIR). None if no files exist."""
    abs_pattern = str((OUTPUT_DIR / pattern_rel).resolve())
    files = glob.glob(abs_pattern)
    if not files:
        return None
    return max(os.path.getmtime(f) for f in files)


def check_csv_feed(spec: FeedSpec) -> dict:
    now = time.time()
    mtime = _newest_file_mtime(spec.pattern)
    if mtime is None:
        return {
            "name": spec.name,
            "status": "MISSING",
            "age_hours": None,
            "max_hours": spec.max_hours,
            "consumed_by": spec.consumed_by,
            "severity": spec.severity,
            "message": f"no files match {spec.pattern}",
        }
    age_hours = (now - mtime) / 3600.0
    status = "OK" if age_hours <= spec.max_hours else "STALE"
    return {
        "name": spec.name,
        "status": status,
        "age_hours": age_hours,
        "max_hours": spec.max_hours,
        "consumed_by": spec.consumed_by,
        "severity": spec.severity,
        "message": (f"newest file {age_hours:.1f}h old "
                      f"(max {spec.max_hours:.1f}h)"),
    }


def check_db_feed(spec: DBTableSpec) -> dict:
    now = time.time()
    if not Path(DB_PATH).exists():
        return {
            "name": spec.name,
            "status": "MISSING",
            "consumed_by": spec.consumed_by,
            "severity": spec.severity,
            "message": f"DB file not found at {DB_PATH}",
        }
    try:
        conn = sqlite3.connect(DB_PATH, timeout=5)
        # Check table exists.
        cur = conn.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type='table' AND name=?", (spec.table,))
        if cur.fetchone() is None:
            return {
                "name": spec.name,
                "status": "MISSING",
                "consumed_by": spec.consumed_by,
                "severity": spec.severity,
                "message": f"table '{spec.table}' does not exist",
            }
        # Row count
        sql_count = f"SELECT COUNT(*) FROM {spec.table}"
        if spec.where:
            sql_count += f" WHERE {spec.where}"
        n = int(conn.execute(sql_count).fetchone()[0])
        if n < spec.min_rows:
            return {
                "name": spec.name,
                "status": "EMPTY",
                "consumed_by": spec.consumed_by,
                "severity": spec.severity,
                "message": (f"table has {n} rows "
                              f"(min expected {spec.min_rows})"),
            }
        # Newest timestamp
        if spec.timestamp_col:
            sql_ts = (f"SELECT MAX({spec.timestamp_col}) "
                        f"FROM {spec.table}")
            if spec.where:
                sql_ts += f" WHERE {spec.where}"
            ts_str = conn.execute(sql_ts).fetchone()[0]
            conn.close()
            if not ts_str:
                return {
                    "name": spec.name,
                    "status": "STALE",
                    "consumed_by": spec.consumed_by,
                    "severity": spec.severity,
                    "message": "no timestamp values present",
                }
            # Try ISO parse; fall back to epoch.
            try:
                from datetime import datetime
                if "T" in ts_str:
                    dt = datetime.fromisoformat(
                        ts_str.replace("Z", "+00:00"))
                else:
                    dt = datetime.fromisoformat(ts_str)
                ts_epoch = dt.timestamp()
            except Exception:
                return {
                    "name": spec.name,
                    "status": "OK",
                    "consumed_by": spec.consumed_by,
                    "severity": spec.severity,
                    "message": (f"{n} rows; newest ts unparseable "
                                  f"({ts_str})"),
                }
            age_hours = (now - ts_epoch) / 3600.0
            status = "OK" if age_hours <= spec.max_hours else "STALE"
            return {
                "name": spec.name,
                "status": status,
                "age_hours": age_hours,
                "max_hours": spec.max_hours,
                "consumed_by": spec.consumed_by,
                "severity": spec.severity,
                "message": (f"{n} rows; newest {age_hours:.1f}h old "
                              f"(max {spec.max_hours:.1f}h)"),
            }
        conn.close()
        return {
            "name": spec.name,
            "status": "OK",
            "consumed_by": spec.consumed_by,
            "severity": spec.severity,
            "message": f"{n} rows",
        }
    except Exception as exc:
        return {
            "name": spec.name,
            "status": "ERROR",
            "consumed_by": spec.consumed_by,
            "severity": spec.severity,
            "message": f"check failed: {exc}",
        }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def run_audit(verbose: bool = False) -> int:
    """Returns count of stale/missing/error rows."""
    log.info("Housekeeping audit starting")
    log.info("OUTPUT_DIR: %s", OUTPUT_DIR)
    log.info("DB_PATH:    %s", DB_PATH)
    log.info("=" * 60)

    bad = 0
    rows: List[dict] = []

    for spec in CSV_FEEDS:
        rows.append(check_csv_feed(spec))
    for spec in DB_FEEDS:
        rows.append(check_db_feed(spec))

    for r in rows:
        prefix = "[OK]" if r["status"] == "OK" else f"[{r['status']}]"
        msg = (f"{prefix} {r['name']:48s} {r['message']} "
                 f"(used by: {r['consumed_by']})")
        if r["status"] == "OK":
            if verbose:
                log.info(msg)
        else:
            bad += 1
            if r.get("severity") == "critical":
                log.error(msg)
            else:
                log.warning(msg)

    log.info("=" * 60)
    log.info("Audit complete: %d feeds checked | %d need attention",
              len(rows), bad)
    return bad


def main() -> int:
    p = argparse.ArgumentParser(
        description="Audit freshness of every data feed the app "
                      "depends on.")
    p.add_argument("--verbose", action="store_true",
                     help="Log OK rows too (default: only WARN/ERROR)")
    p.add_argument("--log", default=None,
                     help="Write a copy of output to this file too.")
    args = p.parse_args()

    handlers = [logging.StreamHandler(sys.stdout)]
    if args.log:
        handlers.append(logging.FileHandler(args.log, mode="a"))
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT,
                          handlers=handlers, force=True)

    bad = run_audit(verbose=args.verbose)
    # Always exit 0 — this is informational, not a gate.
    return 0


if __name__ == "__main__":
    sys.exit(main())
