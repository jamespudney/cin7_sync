"""import_cashflow_history.py (v2.67.226)
=============================================

One-time (re-runnable) importer: loads the historical weekly
figures from the team's Google cashflow Sheet 1 into the
cashflow_forecast table, so the Cashflow page's past weeks match
the spreadsheet.

What it imports
---------------
- Every Cash-Inflow and Cash-Outflow line item, for all 53/54
  weeks, mapped to the app's forecast row_keys.
- The week-1 opening cash balance.

What it deliberately SKIPS
--------------------------
- Chase Bank / Pinnacle Bank / Correction / Forcast Cash Balance
  / Actual opening balance — these are the spreadsheet's internal
  reconciliation rows. The app computes its own closing balance
  from the flows, so importing the manual "Correction" plug would
  just inject noise.
- "Supplier PUR Due / Paid" — the app derives the supplier-
  payables outflow from the live payables tracker.
- "Total ..." computed rows.

Notes
-----
- "UPS payments" and "UPS PAID" are summed into ups_payments.
- Imported cells are stamped updated_by='import:spreadsheet' so
  the sales-projection feature won't silently overwrite them.

Run (on the Render shell, where DB_BACKEND points at Postgres):
    python import_cashflow_history.py --dry-run   # preview
    python import_cashflow_history.py             # write
"""

from __future__ import annotations

import csv
import datetime
import io
import sys

import requests

import db

SHEET_CSV_URL = (
    "https://docs.google.com/spreadsheets/d/"
    "1iFDl_cc9ClrkjZOBqmZ9AUFIX0SJND21tw5kx0L2FWk/"
    "export?format=csv&gid=0")

# Spreadsheet row label (lower-cased, stripped) -> app row_key.
ROW_MAP = {
    "forecast sales": "forecast_sales",
    "prolux": "prolux",
    "other income incl rent": "other_income_rent",
    "865 wages": "wages_865",
    "google ads": "google_ads",
    "advertising bills": "advertising_bills",
    "chase credit card": "chase_credit_card",
    "shopify credit card": "shopify_credit_card",
    "payroll staff": "payroll_staff",
    "non-payroll staff": "non_payroll_staff",
    "bb loan": "bb_loan",
    "bi weekly staff": "bi_weekly_staff",
    "amex prime": "amex_prime",
    "amex gold": "amex_gold",
    "rent": "rent",
    "ben loan": "ben_loan",
    "loan repayments": "loan_repayments",
    "ups payments": "ups_payments",
    "ups paid": "ups_payments",   # summed into ups_payments
    "sales tax": "sales_tax",
    "income tax": "income_tax",
    "tarriffs": "tariffs",
    "cwo suppliers": "cwo_suppliers",
    "kub - utilities": "kub_utilities",
    "cellphones & internet": "cellphones_internet",
}


def _num(s):
    """Parse a spreadsheet money cell -> float or None."""
    if s is None:
        return None
    t = (str(s).strip().replace("$", "").replace(",", "")
         .replace(" ", ""))
    if not t:
        return None
    try:
        return float(t)
    except ValueError:
        return None


def _parse_week_start(cell):
    """'29 Dec 2025 - 04 Jan 2026' -> date(2025, 12, 29)."""
    if not cell:
        return None
    # The sheet uses an en-dash; tolerate a plain hyphen too.
    start = cell.replace("–", "-").split("-")[0].strip()
    for fmt in ("%d %b %Y", "%d %B %Y"):
        try:
            return datetime.datetime.strptime(start, fmt).date()
        except ValueError:
            continue
    return None


def main() -> int:
    dry = "--dry-run" in sys.argv
    print(f"Fetching cashflow Sheet 1 ...")
    resp = requests.get(SHEET_CSV_URL, timeout=30)
    resp.raise_for_status()
    # Force UTF-8 — requests' charset guess mangles the en-dash
    # used in the week-range headers.
    text = resp.content.decode("utf-8", errors="replace")
    rows = list(csv.reader(io.StringIO(text)))

    # Locate the week-range header row.
    header_idx = None
    for i, row in enumerate(rows):
        if any(("–" in (c or "") or " - " in (c or ""))
               and "20" in (c or "") for c in row):
            header_idx = i
            break
    if header_idx is None:
        print("ERROR: could not find the week-range header row.")
        return 1
    header = rows[header_idx]
    week_keys: dict = {}  # csv col index -> ISO week_start
    for col in range(1, len(header)):
        ws = _parse_week_start(header[col])
        if ws:
            week_keys[col] = ws.isoformat()
    if not week_keys:
        print("ERROR: no week columns parsed from the header.")
        return 1
    print(f"  {len(week_keys)} week columns: "
          f"{min(week_keys.values())} .. {max(week_keys.values())}")

    cells: dict = {}  # (week_key, row_key) -> summed amount
    matched_rows = []
    first_col = min(week_keys)
    opening_balance = None
    # Some labels appear twice (e.g. "Forecast Sales" — once in the
    # inflows block, again in the actual-vs-forecast block at the
    # bottom). Only the FIRST occurrence is imported. UPS payments
    # / UPS PAID are DIFFERENT labels so both still sum in.
    seen_labels: set = set()
    for row in rows[header_idx + 1:]:
        if not row or not (row[0] or "").strip():
            continue
        label = row[0].strip().lower()
        if label == "forcast cash balance":
            if opening_balance is None:
                opening_balance = (_num(row[first_col])
                                   if first_col < len(row)
                                   else None)
            continue
        row_key = ROW_MAP.get(label)
        if not row_key:
            continue
        if label in seen_labels:
            continue  # duplicate row lower in the sheet — skip
        seen_labels.add(label)
        matched_rows.append(row[0].strip())
        for col, wk in week_keys.items():
            if col >= len(row):
                continue
            val = _num(row[col])
            if val is None:
                continue
            key = (wk, row_key)
            cells[key] = cells.get(key, 0.0) + val

    if opening_balance is not None:
        cells[(week_keys[first_col], "opening_balance")] = \
            opening_balance

    print(f"  mapped {len(matched_rows)} line-item rows -> "
          f"{len(cells)} cells")
    print(f"  rows imported: {', '.join(sorted(set(matched_rows)))}")
    if opening_balance is not None:
        print(f"  opening balance (week {week_keys[first_col]}): "
              f"{opening_balance:,.2f}")

    if dry:
        print("DRY RUN — nothing written.")
        return 0

    n = 0
    for (wk, rk), amt in cells.items():
        db.set_forecast_cell(wk, rk, amt,
                             updated_by="import:spreadsheet")
        n += 1
    print(f"Imported {n} cells into cashflow_forecast. Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
