"""shopify_discounts.py (v2.67.303)
====================================

Pull Shopify Admin API order discount totals by month into the
local `shopify_monthly_discounts` table. The Monthly Metrics page
Section 6 reads from this for the TRUE Gross Sales (est.) figure.

Why
---
Pre-v2.67.303, Section 6's discount row used CIN7 line-level
discounts as a proxy (~$10k/mo). Per Viktor's May 2026 audit that
undercounts Shopify's real activity by 60–70%: coupons, automatic
promotions, compare-at markdowns, shipping discounts and draft-
order adjustments all live in Shopify and only the simplest "line
discount" maps to a CIN7 column. Shopify's real number is closer
to $20-45k/mo.

What we capture
---------------
For each month in the window, two figures:
  • total_discounts — sum of order.total_discounts across all
    non-cancelled orders created that month
  • order_count     — non-cancelled orders for the month
                      (useful for sanity-checking)

Cancelled orders are excluded (cancelled_at IS NOT NULL); refunds
are KEPT (the discount was applied at sale time even if the order
was later refunded — matches how QB acc 400 nets out refunds).

CLI
---
    python shopify_discounts.py sync                  # last 14 months
    python shopify_discounts.py sync --months 24
    python shopify_discounts.py sync --dry-run --verbose
    python shopify_discounts.py show --month 2026-04

Env: SHOPIFY_DOMAIN, SHOPIFY_ACCESS_TOKEN.
"""

from __future__ import annotations

import argparse
import calendar
import logging
import os
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))
import db  # noqa: E402
from shopify_sync import ShopifyClient  # noqa: E402

log = logging.getLogger("shopify_discounts")


def _setup_log(verbose: bool = False) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(message)s",
        stream=sys.stdout, force=True)


def _make_client() -> ShopifyClient:
    domain = os.environ.get("SHOPIFY_DOMAIN", "").strip()
    token = os.environ.get("SHOPIFY_ACCESS_TOKEN", "").strip()
    if not domain or not token:
        raise SystemExit(
            "SHOPIFY_DOMAIN + SHOPIFY_ACCESS_TOKEN required in env.")
    return ShopifyClient(domain, token)


# ---------------------------------------------------------------------------
# Date helpers
# ---------------------------------------------------------------------------
def _months_back(n: int,
                  anchor: Optional[date] = None) -> Tuple[date, date]:
    """Return (start_date, end_date) covering the last n full
    months + the current (partial) month — matches the Monthly
    Metrics page's 14-month window."""
    anchor = anchor or date.today()
    last_day = calendar.monthrange(anchor.year, anchor.month)[1]
    end = date(anchor.year, anchor.month, last_day)
    start_year = anchor.year
    start_month = anchor.month - (n - 1)
    while start_month <= 0:
        start_month += 12
        start_year -= 1
    start = date(start_year, start_month, 1)
    return start, end


# ---------------------------------------------------------------------------
# Pull
# ---------------------------------------------------------------------------
def pull_orders(client: ShopifyClient,
                  start: date, end: date) -> List[Dict]:
    """Page through every Shopify order in [start, end] (created_at
    range). status=any so we get refunded/partially-refunded too;
    cancelled orders are filtered out later. fields= keeps the
    payload small."""
    params = {
        "status": "any",
        "created_at_min": f"{start.isoformat()}T00:00:00-00:00",
        "created_at_max": f"{end.isoformat()}T23:59:59-00:00",
        "limit": 250,
        "fields": ("id,name,created_at,total_discounts,"
                    "cancelled_at,financial_status"),
    }
    log.info("Pulling Shopify orders %s → %s ...",
              start.isoformat(), end.isoformat())
    rows = client.paginate("orders.json", "orders", params=params)
    log.info("Fetched %d orders.", len(rows))
    return rows


def aggregate_by_month(orders: List[Dict]) -> Dict[str, Dict]:
    """Sum total_discounts per YYYY-MM month. Skips cancelled."""
    by_month: Dict[str, Dict] = {}
    n_skipped_cancel = 0
    n_skipped_nodate = 0
    for o in orders:
        if o.get("cancelled_at"):
            n_skipped_cancel += 1
            continue
        created = o.get("created_at") or ""
        if not created:
            n_skipped_nodate += 1
            continue
        try:
            dt = datetime.fromisoformat(
                created.replace("Z", "+00:00"))
        except ValueError:
            n_skipped_nodate += 1
            continue
        month_key = dt.strftime("%Y-%m")
        try:
            disc = float(o.get("total_discounts") or 0)
        except (TypeError, ValueError):
            disc = 0.0
        entry = by_month.setdefault(
            month_key, {"total_discounts": 0.0, "order_count": 0})
        entry["total_discounts"] += disc
        entry["order_count"] += 1
    if n_skipped_cancel or n_skipped_nodate:
        log.info("Skipped %d cancelled + %d undated order(s).",
                  n_skipped_cancel, n_skipped_nodate)
    return by_month


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def cmd_sync(args) -> int:
    _setup_log(args.verbose)
    months_back = int(args.months or 14)
    start, end = _months_back(months_back)
    client = _make_client()

    orders = pull_orders(client, start, end)
    by_month = aggregate_by_month(orders)

    log.info("=" * 60)
    log.info("Aggregated %d month(s):", len(by_month))
    for m in sorted(by_month):
        d = by_month[m]
        log.info("  %s  $%9.2f   (%d orders)",
                  m, d["total_discounts"], d["order_count"])

    if args.dry_run:
        log.info("[dry-run] not writing to DB.")
        return 0

    n = 0
    for m, d in by_month.items():
        try:
            db.upsert_shopify_monthly_discounts(
                month=m,
                total_discounts=d["total_discounts"],
                order_count=d["order_count"])
            n += 1
        except Exception as exc:  # noqa: BLE001
            log.warning("upsert failed for %s: %s", m, exc)
    log.info("Wrote %d row(s) to shopify_monthly_discounts.", n)
    return 0


def cmd_show(args) -> int:
    _setup_log(args.verbose)
    row = db.get_shopify_monthly_discounts(args.month)
    if not row:
        log.info("No shopify_monthly_discounts row for %s. "
                  "Run 'sync' first.", args.month)
        return 0
    log.info("Shopify discounts for %s:", args.month)
    log.info("  total_discounts: $%.2f",
              float(row.get("total_discounts") or 0))
    log.info("  order_count    : %d",
              int(row.get("order_count") or 0))
    log.info("  synced_at      : %s",
              row.get("synced_at"))
    return 0


def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(
        description="Sync Shopify monthly discount totals.")
    sub = p.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser(
        "sync", help="Pull Shopify discounts for the last N months.")
    s.add_argument("--months", type=int, default=14,
                     help="Months back from today (default 14).")
    s.add_argument("--dry-run", action="store_true",
                     help="Fetch + aggregate but don't write.")
    s.add_argument("--verbose", action="store_true")
    s.set_defaults(func=cmd_sync)

    sh = sub.add_parser(
        "show", help="Show one month's stored row.")
    sh.add_argument("--month", required=True,
                      help="'YYYY-MM' month to inspect.")
    sh.add_argument("--verbose", action="store_true")
    sh.set_defaults(func=cmd_show)

    args = p.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
