"""
qbo_monthly_pl.py (v2.67.292)
=============================

Pull QuickBooks Online Profit & Loss data by month and store it in
the local `qbo_monthly_pl` table. The Monthly Metrics page reads
from this table to show **canonical** financial figures (QB-grounded)
alongside the CIN7-derived "operational" view.

Why
---
The Viktor cross-system audit (May 2026) compared the app's CIN7-
derived figures against actual QB account balances and found:
  • Shipping Charged inflated 27–218% vs QB acc 405 every month
  • Historical COGS drifting up to 27% vs QB acc 500 in older months
  • Dec 2025 sales gap of -$45k vs QB acc 400 (likely a credit note
    or journal entry that CIN7 doesn't surface)

QB is the reconciled financial source of truth — pulling it directly
removes the variance. Commissions still need a frozen month-end
snapshot (separate project) but at least the live page is honest.

What we capture
---------------
For each of the last N months (default 14, matching the Monthly
Metrics window):
  • account_id, account_name, account_number, account_type,
    parent_account_id
  • the per-month amount

The accounts of immediate interest (per W4S chart of accounts):
  400  Sales                  → Sales $ (income)
  405  Sales - Shipping       → Shipping Charged (income)
  500  Cost of Goods Sold     → COGS
  694  Shipping-Out           → Shipping Cost (expense)

Other accounts are also stored so the page can drill into them
without needing another QB pull.

CLI
---
    python qbo_monthly_pl.py sync                    # 14 months
    python qbo_monthly_pl.py sync --months 24        # custom window
    python qbo_monthly_pl.py sync --dry-run --verbose
    python qbo_monthly_pl.py show --month 2026-04    # inspect one month

Env: QBO must already be connected (see qbo_oauth.py).
"""

from __future__ import annotations

import argparse
import calendar
import logging
import re
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))
import db  # noqa: E402
import qbo_client  # noqa: E402
from sales_exclusions import (  # noqa: E402
    EXCLUDED_SALES_CUSTOMERS, _normalise_customer_name)

log = logging.getLogger("qbo_monthly_pl")


def _setup_log(verbose: bool = False) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(message)s",
        stream=sys.stdout, force=True)


# ---------------------------------------------------------------------------
# Date helpers
# ---------------------------------------------------------------------------
def _month_start(d: date) -> date:
    return date(d.year, d.month, 1)


def _month_end(d: date) -> date:
    last = calendar.monthrange(d.year, d.month)[1]
    return date(d.year, d.month, last)


def _months_back(n: int, anchor: Optional[date] = None) -> Tuple[date, date]:
    """Return (start_date, end_date) covering the last n full months
    plus the current (possibly partial) month — matching the Monthly
    Metrics page's 14-month window."""
    anchor = anchor or date.today()
    end = _month_end(anchor)
    start_year = anchor.year
    start_month = anchor.month - (n - 1)
    while start_month <= 0:
        start_month += 12
        start_year -= 1
    start = date(start_year, start_month, 1)
    return start, end


# ---------------------------------------------------------------------------
# QBO P&L report parsing
# ---------------------------------------------------------------------------
_MONTH_TITLE_RE = re.compile(
    r"^([A-Za-z]+)\s+(\d{4})$")
_MONTH_NAMES = {m.lower(): i for i, m in enumerate(
    calendar.month_name) if m}
_MONTH_NAMES.update({m.lower(): i for i, m in enumerate(
    calendar.month_abbr) if m})


def _parse_month_title(title: str) -> Optional[str]:
    """'Apr 2025' / 'April 2025' → '2025-04'. None on no match."""
    if not title:
        return None
    m = _MONTH_TITLE_RE.match(title.strip())
    if not m:
        return None
    mon_word, yr = m.group(1).lower(), int(m.group(2))
    mon = _MONTH_NAMES.get(mon_word)
    if not mon:
        return None
    return f"{yr:04d}-{mon:02d}"


def _money(val) -> float:
    if val in (None, ""):
        return 0.0
    try:
        return float(str(val).replace(",", "").replace("$", "").strip())
    except (TypeError, ValueError):
        return 0.0


def _extract_month_columns(report: Dict) -> List[Tuple[int, str]]:
    """Return [(column_index, 'YYYY-MM'), ...] for each Money column
    in the P&L Columns header. Skips the leading Account column and
    the trailing Total column."""
    cols = (report.get("Columns") or {}).get("Column") or []
    out: List[Tuple[int, str]] = []
    for i, c in enumerate(cols):
        col_type = (c.get("ColType") or "").strip()
        col_title = (c.get("ColTitle") or "").strip()
        if col_type != "Money":
            continue
        if not col_title or col_title.lower().startswith("total"):
            continue
        month = _parse_month_title(col_title)
        if month:
            out.append((i, month))
    return out


def _walk_rows(node: Any,
                month_columns: List[Tuple[int, str]],
                tuples: List[Tuple],
                section_type: Optional[str] = None,
                parent_id: Optional[str] = None) -> None:
    """Recursively walk the P&L Rows structure, collecting per-month
    amounts for every account leaf and section summary."""
    if isinstance(node, list):
        for item in node:
            _walk_rows(item, month_columns, tuples,
                        section_type, parent_id)
        return
    if not isinstance(node, dict):
        return

    # A "Section" row has a Header / sub-Rows / Summary trio. A
    # "Data" row is a single leaf with ColData. The 'type' field
    # distinguishes them.
    row_type = (node.get("type") or "").strip()
    group = (node.get("group") or "").strip()
    next_section = group or section_type

    # Leaf data row.
    coldata = node.get("ColData")
    if isinstance(coldata, list) and coldata:
        first = coldata[0] or {}
        name = (first.get("value") or "").strip()
        acct_id = (first.get("id") or "").strip()
        # Only emit if there's a real account name; section
        # placeholders have empty names.
        if name:
            for col_idx, month in month_columns:
                if col_idx < len(coldata):
                    val = (coldata[col_idx] or {}).get("value", "")
                    amount = _money(val)
                    tuples.append((
                        month, acct_id or None, name,
                        next_section, parent_id, amount))

    # Recurse into sub-rows of a section.
    for key in ("Rows", "Row"):
        sub = node.get(key)
        if sub:
            new_parent = acct_id or parent_id if 'acct_id' in dir() else parent_id
            _walk_rows(sub, month_columns, tuples,
                        next_section, new_parent)

    # Section summary (e.g. "Total Income", "Total COGS").
    summary = node.get("Summary")
    if summary:
        sc = summary.get("ColData")
        if isinstance(sc, list) and sc:
            first = sc[0] or {}
            name = (first.get("value") or "").strip()
            acct_id = (first.get("id") or "").strip()
            if name:
                for col_idx, month in month_columns:
                    if col_idx < len(sc):
                        val = (sc[col_idx] or {}).get("value", "")
                        amount = _money(val)
                        tuples.append((
                            month, acct_id or None, name,
                            next_section, parent_id, amount))


def parse_pnl(report: Dict) -> List[Tuple]:
    """Parse a QBO ProfitAndLoss report into a flat list of tuples:
        (month, account_id, account_name, section, parent_id, amount)
    section is the QB report group ('Income', 'CostOfGoodsSold',
    'Expenses', etc.) — useful for distinguishing 'Sales' (income)
    from 'Sales' (a sub-account inside Expenses)."""
    month_columns = _extract_month_columns(report)
    if not month_columns:
        log.warning("No month columns found in report — was "
                     "summarize_column_by=Month requested?")
        return []
    log.info("Report covers %d month(s): %s → %s",
              len(month_columns),
              month_columns[0][1], month_columns[-1][1])
    tuples: List[Tuple] = []
    _walk_rows(report.get("Rows") or {}, month_columns, tuples)
    return tuples


# ---------------------------------------------------------------------------
# Account-number enrichment
# ---------------------------------------------------------------------------
def _account_number_map() -> Dict[str, Dict[str, str]]:
    """{account_id: {'number': '400', 'type': 'Income', 'name': 'Sales'}}
    Pulled from QBO Account entity — the report itself doesn't
    include AcctNum, so we enrich after the fact."""
    try:
        accounts = qbo_client.query_all("SELECT * FROM Account")
    except Exception as exc:  # noqa: BLE001
        log.warning("Could not fetch QBO Accounts to enrich numbers: "
                      "%s", exc)
        return {}
    out: Dict[str, Dict[str, str]] = {}
    for a in accounts:
        aid = str(a.get("Id") or "").strip()
        if not aid:
            continue
        out[aid] = {
            "number": (a.get("AcctNum") or "").strip(),
            "type": (a.get("AccountType") or "").strip(),
            "name": (a.get("Name") or "").strip(),
        }
    return out


# ---------------------------------------------------------------------------
# Excluded-customer netting (Altar'd State)
# ---------------------------------------------------------------------------
# sales_exclusions.py already strips this customer out of CIN7-
# derived figures (sale_lines/sales headers), but this QBO pull is a
# separate data source with no customer-level filter of its own — a
# gap confirmed live 2026-07-23 (Altar'd State's own QBO customer
# record shows $185,193.61 of Income across Jan-Sep 2025, $117,470 of
# which falls inside the current 14-month reporting window). This
# section nets that back out so Sections 6/7/8 agree with the
# already-excluded CIN7 sections.
def _find_excluded_customer_ids() -> Dict[str, str]:
    """{qbo_display_name: qbo_customer_id} for every QBO customer
    whose normalised name starts with an excluded customer's
    normalised name — catches numbered duplicate/sub-customer QBO
    records too (confirmed live: QBO has both "Altar'd State" and a
    second, currently-inactive "Altar'd State 1" record), without
    catching unrelated similarly-named customers (confirmed live:
    "Altar Construction" does NOT match)."""
    excluded_keys = [_normalise_customer_name(n)
                      for n in EXCLUDED_SALES_CUSTOMERS]
    out: Dict[str, str] = {}
    for name in EXCLUDED_SALES_CUSTOMERS:
        # A short, apostrophe-free prefix for QBO's LIKE match —
        # the real filter is the normalised startswith check below,
        # so differences in how QBO's LIKE handles the apostrophe
        # don't matter here.
        prefix = re.split(r"[’'`\s]", name)[0].strip()
        if not prefix:
            continue
        try:
            matches = qbo_client.query(
                f"SELECT Id, DisplayName FROM Customer WHERE "
                f"DisplayName LIKE '{prefix}%'")
        except Exception as exc:  # noqa: BLE001
            log.warning("QBO customer lookup failed for %r: %s",
                         name, exc)
            continue
        for m in matches:
            disp = m.get("DisplayName") or ""
            key = _normalise_customer_name(disp)
            if any(key.startswith(ek) for ek in excluded_keys):
                out[disp] = m.get("Id")
    return out


def sync_exclusions_for_customer(customer_id: str, customer_name: str,
                                   start: date, end: date,
                                   acct_meta: Dict[str, Dict[str, str]]
                                   ) -> int:
    """Pull the SAME ProfitAndLoss report, scoped to one customer's
    own transactions via QBO's `customer` report filter, and store
    into qbo_monthly_pl_exclusions. Reuses parse_pnl() unchanged —
    QBO computes this customer's own Total Income/Total COGS/Net
    Operating Income subtotals the same way it does for the full-
    company report, so netting row-for-row (in
    qbo_monthly_pl_summary_by_category) stays consistent at every
    level, not just the leaf accounts."""
    report = qbo_client.report("ProfitAndLoss", params={
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "summarize_column_by": "Month",
        "accounting_method": "Accrual",
        "customer": customer_id,
    })
    tuples = parse_pnl(report)
    if not tuples:
        log.info("No P&L rows for excluded customer %s (Id=%s).",
                  customer_name, customer_id)
        return 0
    payload = []
    for (month, acct_id, name, section, parent, amount) in tuples:
        meta = acct_meta.get(acct_id or "") or {}
        payload.append({
            "month": month,
            "account_id": acct_id,
            "account_number": meta.get("number") or None,
            "account_name": name,
            "customer_name": customer_name,
            "amount": amount,
        })
    n_ok = db.batch_upsert_qbo_monthly_pl_exclusion(payload)
    log.info("Wrote %d exclusion row(s) for %s.", n_ok, customer_name)
    return n_ok


# ---------------------------------------------------------------------------
# Sync command
# ---------------------------------------------------------------------------
def cmd_sync(args) -> int:
    _setup_log(args.verbose)
    if not qbo_client.is_ready():
        log.error("QBO is not connected. Connect it from the "
                   "Cashflow Management page first.")
        return 1
    months_back = int(args.months or 14)
    start, end = _months_back(months_back)
    log.info("Pulling QBO ProfitAndLoss, %s → %s (%d months) "
              "summarised by month...",
              start.isoformat(), end.isoformat(), months_back)

    report = qbo_client.report("ProfitAndLoss", params={
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "summarize_column_by": "Month",
        "accounting_method": "Accrual",
    })

    tuples = parse_pnl(report)
    log.info("Parsed %d (month, account) tuples.", len(tuples))

    # Enrich with account number / type from the Account entity.
    acct_meta = _account_number_map()
    log.info("Loaded %d QBO accounts for number enrichment.",
              len(acct_meta))

    if args.dry_run:
        log.info("[dry-run] sample first 8 tuples:")
        for t in tuples[:8]:
            month, acct_id, name, section, parent, amount = t
            meta = acct_meta.get(acct_id or "") or {}
            log.info("   %s | acc#%-6s %-30s (%s) → $%.2f",
                      month, meta.get("number") or "-",
                      name[:30], section or "?", amount)
        log.info("Sample mapping verification "
                  "(W4S defaults):")
        seed_n = db.seed_default_qbo_account_mappings(actor="dry-run")
        log.info("  Would seed %d default mappings.", seed_n)
        return 0

    # Seed default mappings if none exist (idempotent — only
    # inserts categories the operator hasn't configured).
    seeded = db.seed_default_qbo_account_mappings(
        actor="qbo_monthly_pl.sync")
    if seeded:
        log.info("Seeded %d default account mappings.", seeded)

    # v2.67.293 — use the batch upsert. 868 rows over 868 separate
    # DB connections took 8 minutes on Render Postgres; batching
    # them under one connection drops that to seconds.
    payload = []
    for (month, acct_id, name, section, parent, amount) in tuples:
        meta = acct_meta.get(acct_id or "") or {}
        payload.append({
            "month": month,
            "account_id": acct_id,
            "account_number": meta.get("number") or None,
            "account_name": name,
            "amount": amount,
            "account_type": meta.get("type") or section,
            "parent_account_id": parent,
        })
    log.info("Bulk-writing %d row(s) to qbo_monthly_pl...",
              len(payload))
    n_ok = db.batch_upsert_qbo_monthly_pl(payload)

    log.info("=" * 60)
    log.info("Wrote %d / %d row(s) to qbo_monthly_pl.",
              n_ok, len(payload))

    # Net excluded-customer (Altar'd State) amounts back out — see
    # the "Excluded-customer netting" section above. Best-effort:
    # a failure here shouldn't fail the whole sync since the main
    # P&L data above is already written.
    try:
        excluded = _find_excluded_customer_ids()
    except Exception as exc:  # noqa: BLE001
        log.warning("Excluded-customer lookup failed: %s", exc)
        excluded = {}
    if excluded:
        log.info("Netting out %d excluded QBO customer record(s): %s",
                  len(excluded), ", ".join(excluded))
        for cust_name, cust_id in excluded.items():
            try:
                sync_exclusions_for_customer(
                    cust_id, cust_name, start, end, acct_meta)
            except Exception as exc:  # noqa: BLE001
                log.warning("Exclusion sync failed for %s: %s",
                             cust_name, exc)
    else:
        log.info("No excluded customers (%s) found in QBO — "
                  "nothing to net out.",
                  ", ".join(EXCLUDED_SALES_CUSTOMERS))
    return 0


# ---------------------------------------------------------------------------
# Inspection command
# ---------------------------------------------------------------------------
def cmd_show(args) -> int:
    _setup_log(args.verbose)
    rows = db.get_qbo_monthly_pl(
        start_month=args.month, end_month=args.month)
    if not rows:
        log.info("No qbo_monthly_pl rows for month %s. "
                  "Run 'sync' first.", args.month)
        return 0
    log.info("qbo_monthly_pl rows for %s:", args.month)
    for r in rows:
        log.info("  acc#%-6s %-40s ($%.2f)",
                  r.get("account_number") or "-",
                  (r.get("account_name") or "")[:40],
                  float(r.get("amount") or 0))
    # Summary by category — passes the FULL mapping dict so both
    # account_numbers and account_names matchers fire.
    mappings = db.get_qbo_account_mappings()
    if mappings:
        log.info("\nCategory summary (per mapping config):")
        by_cat = db.qbo_monthly_pl_summary_by_category(mappings)
        for cat, amt in sorted(
                (by_cat.get(args.month, {}) or {}).items()):
            log.info("  %-25s $%.2f", cat, amt)
    return 0


def cmd_seed_mappings(args) -> int:
    """Install / refresh the default qbo_account_mappings for any
    category not already configured. Safe to re-run — existing
    categories aren't overwritten."""
    _setup_log(args.verbose)
    n = db.seed_default_qbo_account_mappings(
        actor="qbo_monthly_pl.seed-mappings")
    log.info("Seeded %d new mapping(s).", n)
    log.info("Current mappings:")
    for cat, m in sorted(db.get_qbo_account_mappings().items()):
        nums = ", ".join(m.get("account_numbers") or []) or "—"
        names = ", ".join(m.get("account_names") or []) or "—"
        log.info("  %-30s nums=[%s]  names=[%s]",
                  cat, nums, names)
    return 0


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(
        description="Sync QBO Profit & Loss by month to the local DB.")
    sub = p.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser(
        "sync", help="Pull QBO P&L for the last N months.")
    s.add_argument("--months", type=int, default=14,
                     help="Months back from today (default 14).")
    s.add_argument("--dry-run", action="store_true",
                     help="Fetch + parse but don't write to DB.")
    s.add_argument("--verbose", action="store_true")
    s.set_defaults(func=cmd_sync)

    sh = sub.add_parser(
        "show", help="Inspect qbo_monthly_pl rows for one month.")
    sh.add_argument("--month", required=True,
                      help="'YYYY-MM' month to inspect.")
    sh.add_argument("--verbose", action="store_true")
    sh.set_defaults(func=cmd_show)

    sm = sub.add_parser(
        "seed-mappings",
        help="Insert default qbo_account_mappings for any category "
              "not yet configured. Idempotent — existing categories "
              "are left untouched.")
    sm.add_argument("--verbose", action="store_true")
    sm.set_defaults(func=cmd_seed_mappings)

    args = p.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
