"""monthly_metrics_report.py
=============================
Generate a printable, multi-month PDF export of the Monthly Metrics
dashboard (the same per-month tables shown on that page, landscape
so every month column fits) and post it to a Slack channel via the
Wired4Signs bot.

Designed to run a few days into each new month (see the day-of-month
guard in sync_loop.sh), by which point QuickBooks bookkeeping has
mostly caught up on the prior month — reporting on the just-finished
month on day 1 would inherit the inflated-GP%/incomplete-COGS
distortion documented in app.py's Monthly Metrics methodology notes.

James (2026-07-23): the pie-per-section / single-month executive-
summary layout this used to have didn't show the actual month-by-
month figures the dashboard shows, which is what people actually
need to read off this report. Rewritten to mirror the dashboard's
own tables row-for-row (same labels, same order, same formulas) —
one wide table per section, months across the top, landscape so a
14-month run of columns actually fits and stays legible. Pie charts
were dropped in this redesign to keep the focus on the visible
figures; ask if you'd like them added back alongside the tables.

Section 4 ("Inventory") is the one deliberate exception — it's shown
as a CURRENT stock-value snapshot (slow-moving vs the rest), not a
per-month trend, because reproducing the dashboard's modelled
month-average walk-back here would mean re-deriving that whole model
a second time; a snapshot correctly labelled as such is preferable to
either skipping it or showing a misleading repeated number across
every month column.

Configuration via environment variables:
    SLACK_BOT_TOKEN                  Wired4Signs Slack bot token (xoxb-...)
    SLACK_MONTHLY_REPORT_CHANNEL_ID  Target Slack channel ID
If either is unset, the script builds the PDF, logs a warning, and
skips the Slack post (matches the "silent disable" convention used by
weekly_slow_movers_email.py / po_dispatch_reminder.py).

How it's wired:
  - Render: invoked from sync_loop.sh, guarded to fire once per month
    (day-of-month range + a persisted YYYY-MM marker on /data, same
    restart-safe pattern slack_loop.sh uses for its daily jobs).
"""

from __future__ import annotations

import io
import os
import sys
from datetime import datetime, date
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


def _emit(msg: str, level: str = "info") -> None:
    sys.stderr.write(f"[monthly_metrics_report] {level}: {msg}\n")
    sys.stderr.flush()


# ---------------------------------------------------------------------------
# Target month
# ---------------------------------------------------------------------------
def _target_month(today: Optional[date] = None) -> str:
    """The most recently completed calendar month, as 'YYYY-MM'. Used
    for the report's headline/filename — the table itself spans a
    wider month range (see `_report_months`)."""
    today = today or date.today()
    year, month = today.year, today.month
    if month == 1:
        return f"{year - 1}-12"
    return f"{year}-{month - 1:02d}"


def _current_partial_month(today: Optional[date] = None) -> str:
    """The in-progress calendar month, as 'YYYY-MM' — the rightmost
    (still-partial) column of the report table."""
    today = today or date.today()
    return f"{today.year}-{today.month:02d}"


def _report_months(current_month: str, lookback: int = 14) -> List[str]:
    """The month columns the report table shows — same default
    lookback as the dashboard's "Months to show" control, ending
    with the current in-progress month (matches the dashboard
    including the partial month as a normal rightmost column)."""
    import pandas as pd
    periods = pd.period_range(end=current_month, periods=lookback, freq="M")
    return [str(p) for p in periods]


# ---------------------------------------------------------------------------
# Data loading — CIN7 CSVs (same conventions as weekly_slow_movers_email.py)
# ---------------------------------------------------------------------------
def _load_cin7_data() -> Dict[str, Any]:
    import pandas as pd

    _here = Path(__file__).resolve().parent
    if str(_here) not in sys.path:
        sys.path.insert(0, str(_here))
    from data_paths import OUTPUT_DIR
    import db
    from sales_exclusions import filter_excluded_sales_customers

    def _latest(pattern: str) -> Optional[Path]:
        matches = sorted(
            OUTPUT_DIR.glob(pattern),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
        return matches[0] if matches else None

    products_csv = _latest("products_*.csv")
    stock_csv = _latest("stock_on_hand_*.csv")
    missing = [
        name for name, p in (
            ("products", products_csv), ("stock_on_hand", stock_csv))
        if p is None
    ]
    if missing:
        raise FileNotFoundError(
            f"Missing CSV(s) {missing} in {OUTPUT_DIR}. Has the sync run?")

    products = pd.read_csv(products_csv, low_memory=False)
    stock = pd.read_csv(stock_csv, low_memory=False)
    sale_lines = _load_longest_sale_lines(
        OUTPUT_DIR, pd, filter_excluded_sales_customers)
    if sale_lines.empty:
        raise FileNotFoundError(
            f"No sale_lines_last_*d_*.csv found in {OUTPUT_DIR}. "
            f"Has the sync run?")
    purchase_lines = _load_longest_purchase_lines(OUTPUT_DIR, pd)
    shopify_orders = _load_shopify_orders(OUTPUT_DIR, pd)

    return {
        "products": products, "stock": stock,
        "sale_lines": sale_lines, "purchase_lines": purchase_lines,
        "shopify_orders": shopify_orders, "db": db,
    }


def _load_longest_sale_lines(output_dir, pd, filter_excluded_sales_customers):
    """Union of the largest sale_lines_last_Nd_*.csv backfill window
    plus any more-recently-synced smaller windows — same pattern as
    app.py's _load_longest_sale_lines_cached (dedup logic copied
    verbatim). Picking just the single most-recently-MODIFIED file
    (the naive approach this used to use) almost always grabs a small
    rolling sync (last_1d/_7d) instead of the multi-year backfill file,
    silently truncating every month older than that window to zero —
    confirmed live 2026-07-23: the commentary showed "$0 last year"
    for a month (2025-06) the dashboard shows $385,681 of real sales
    for, because this loader was reading only the last few days."""
    import re as _re

    files = []
    for p in output_dir.glob("sale_lines_last_*d_*.csv"):
        m = _re.match(r"sale_lines_last_(\d+)d_", p.name)
        if m:
            files.append((int(m.group(1)), p.stat().st_mtime, p))
    if not files:
        return pd.DataFrame()

    files.sort(key=lambda x: (-x[0], -x[1]))
    base_file = files[0][2]
    base_mtime = files[0][1]
    try:
        base = pd.read_csv(base_file, low_memory=False)
    except Exception:  # noqa: BLE001
        return pd.DataFrame()

    for _days, mtime, p in files[1:]:
        if mtime <= base_mtime:
            continue
        try:
            more = pd.read_csv(p, low_memory=False)
        except Exception:  # noqa: BLE001
            continue
        if more.empty:
            continue
        base = pd.concat([base, more], ignore_index=True)

    dedupe_cols = [c for c in
                    ["SaleID", "SKU", "Quantity", "InvoiceNumber",
                     "OrderNumber"]
                    if c in base.columns]
    if dedupe_cols:
        base = base.drop_duplicates(subset=dedupe_cols, keep="last")

    second_key = [c for c in
                   ["SaleID", "SKU", "Quantity", "OrderNumber"]
                   if c in base.columns]
    if second_key and "InvoiceDate" in base.columns:
        base = base.sort_values(
            "InvoiceDate", na_position="first", kind="stable")
        base = base.drop_duplicates(subset=second_key, keep="last")
    base = filter_excluded_sales_customers(base)
    return base.reset_index(drop=True)


def _load_longest_purchase_lines(output_dir, pd):
    """Same union-of-windows pattern as _load_longest_sale_lines, for
    purchase_lines_last_Nd_*.csv (used by Section 2's Purchase $ /
    # of Purchases) — mirrors app.py's
    _load_longest_purchase_lines_cached."""
    import re as _re

    files = []
    for p in output_dir.glob("purchase_lines_last_*d_*.csv"):
        m = _re.match(r"purchase_lines_last_(\d+)d_", p.name)
        if m:
            files.append((int(m.group(1)), p.stat().st_mtime, p))
    if not files:
        return pd.DataFrame()

    files.sort(key=lambda x: (-x[0], -x[1]))
    base_file = files[0][2]
    base_mtime = files[0][1]
    try:
        base = pd.read_csv(base_file, low_memory=False)
    except Exception:  # noqa: BLE001
        return pd.DataFrame()

    for _days, mtime, p in files[1:]:
        if mtime <= base_mtime:
            continue
        try:
            more = pd.read_csv(p, low_memory=False)
        except Exception:  # noqa: BLE001
            continue
        if more.empty:
            continue
        if "PurchaseID" in base.columns and "PurchaseID" in more.columns:
            newer_pids = set(more["PurchaseID"].dropna().unique())
            base = base[~base["PurchaseID"].isin(newer_pids)]
        base = pd.concat([base, more], ignore_index=True)

    dedupe_cols = [c for c in
                    ["PurchaseID", "SKU", "Quantity", "OrderDate",
                     "OrderNumber", "Price"]
                    if c in base.columns]
    if dedupe_cols:
        base = base.drop_duplicates(subset=dedupe_cols, keep="last")
    return base.reset_index(drop=True)


def _load_shopify_orders(output_dir, pd):
    """Same union-of-full-plus-rolling-window + dedupe convention as
    app.py's _load_longest_shopify_orders_cached. Optional: if the
    sync hasn't run (older setups, or it's simply not been enabled),
    returns an empty frame and the Shopify channel split below falls
    back to "Other/Unclassified" for everything rather than erroring."""
    import re as _re

    files = []
    full_path = output_dir / "shopify_orders_full.csv"
    if full_path.exists():
        files.append(("full", full_path.stat().st_mtime, full_path))
    for p in output_dir.glob("shopify_orders_last_*d_*.csv"):
        m = _re.match(r"shopify_orders_last_(\d+)d_", p.name)
        if m:
            files.append((int(m.group(1)), p.stat().st_mtime, p))
    if not files:
        return pd.DataFrame(columns=["OrderNumber", "SourceName"])

    def _sort_key(item):
        days, mtime, _p = item
        return (-10**9, -mtime) if days == "full" else (-days, -mtime)

    files.sort(key=_sort_key)
    base = pd.DataFrame()
    base_mtime = 0.0
    for _days, mtime, p in files:
        try:
            chunk = pd.read_csv(p, low_memory=False)
        except Exception:  # noqa: BLE001
            continue
        if base.empty:
            base, base_mtime = chunk, mtime
            continue
        if mtime > base_mtime:
            base = pd.concat([base, chunk], ignore_index=True)
    if "ShopifyOrderID" in base.columns:
        base = base.drop_duplicates(subset=["ShopifyOrderID"], keep="last")
    return base.reset_index(drop=True)


_BAD_STATUSES = ("VOIDED", "CREDITED", "CANCELLED", "CANCELED")


def _month_lines(sale_lines, month: str, max_day: Optional[int] = None):
    """Sale lines with a valid InvoiceDate inside `month` ('YYYY-MM'),
    excluding voided/credited/cancelled — same net-demand convention
    the dashboard uses. `max_day`, if given, additionally requires
    day-of-month <= max_day — used for the year-over-year
    month-to-date commentary, so this year's partial month compares
    against the SAME number of days last year rather than the whole
    of last year's month."""
    import pandas as pd

    sl = sale_lines.copy()
    sl["InvoiceDate"] = pd.to_datetime(sl.get("InvoiceDate"), errors="coerce")
    sl = sl.dropna(subset=["InvoiceDate"])
    if "Status" in sl.columns:
        sl = sl[~sl["Status"].astype(str).str.upper().isin(_BAD_STATUSES)]
    period = sl["InvoiceDate"].dt.to_period("M").astype(str)
    sl = sl[period == month]
    if max_day is not None:
        sl = sl[sl["InvoiceDate"].dt.day <= max_day]
    return sl.copy()


_IS_SHIPPING_RE = r"^(shipping|freight|handling|delivery)"


def _split_product_shipping(sl):
    import pandas as pd
    name_col = sl.get("Name", pd.Series("", index=sl.index)).astype(str)
    is_ship = name_col.str.lower().str.match(_IS_SHIPPING_RE)
    return sl[~is_ship].copy(), sl[is_ship].copy()


def _channel_of_row(sc_val, sr_val) -> str:
    """Mirrors app.py's _channel_of_row exactly (Section 5/9 logic)."""
    sc = (str(sc_val) if sc_val is not None else "").strip().lower()
    sr = (str(sr_val) if sr_val is not None else "").strip().upper()
    if "shopify" in sc:
        return "Shopify"
    if "amazon" in sc or sr == "AMAZON":
        return "Amazon"
    if "ebay" in sc or sr == "EBAY":
        return "eBay"
    if sr == "SHOPIFY":
        return "Shopify"
    return "B2B / Direct"


def _shopify_rev_and_cnt_by_source(shopify_orders):
    """{(month_period, source_name): revenue} and {...: count}, from
    Shopify's own order data directly (TotalPrice, CreatedAt,
    SourceName) - NOT joined through CIN7.

    First attempt joined CIN7 sale_lines to shopify_orders by
    OrderNumber - wrong key. Confirmed via a live CIN7 API pull
    (2026-07-22): CIN7's own "OrderNumber" for a Shopify-channel sale
    is CIN7's OWN internal reference (e.g. "SO-56363"), unrelated to
    Shopify's order number. James's fix: don't round-trip through
    CIN7 at all - Shopify's own order data already has everything
    needed (TotalPrice + SourceName + CreatedAt per order)."""
    import pandas as pd

    if (shopify_orders is None or shopify_orders.empty
            or "CreatedAt" not in shopify_orders.columns
            or "SourceName" not in shopify_orders.columns):
        return pd.Series(dtype=float), pd.Series(dtype="int64")
    so = shopify_orders.copy()
    so["_dt"] = pd.to_datetime(
        so["CreatedAt"], errors="coerce", utc=True).dt.tz_localize(None)
    so = so.dropna(subset=["_dt"])
    so["MonthKey"] = so["_dt"].dt.to_period("M")
    so["TotalPrice"] = pd.to_numeric(
        so.get("TotalPrice"), errors="coerce").fillna(0)
    rev = so.groupby(["MonthKey", "SourceName"])["TotalPrice"].sum()
    cnt = so.groupby(["MonthKey", "SourceName"]).size()
    return rev, cnt


# ---------------------------------------------------------------------------
# Year-over-year commentary
# ---------------------------------------------------------------------------
def _yoy_month(month: str) -> str:
    """Same calendar month, one year earlier ('2026-06' -> '2025-06')."""
    year, mon = month.split("-")
    return f"{int(year) - 1}-{mon}"


def _num(sl, col):
    import pandas as pd
    return pd.to_numeric(sl.get(col), errors="coerce").fillna(0)


def _headline_cin7_metrics(sale_lines, month: str,
                            max_day: Optional[int] = None) -> Dict[str, float]:
    """A handful of top-line CIN7 figures for one month (optionally
    cut off at max_day, for a like-for-like partial-period YoY
    comparison) — enough for the commentary without recomputing all
    9 sections twice more for two more months."""
    sl = _month_lines(sale_lines, month, max_day=max_day)
    prod, _ = _split_product_shipping(sl)
    sales = float(_num(prod, "Total").sum())
    cogs = float((_num(prod, "Quantity") * _num(prod, "AverageCost")).sum())
    gp = sales - cogs
    orders = (int(prod["SaleID"].nunique())
              if "SaleID" in prod.columns else 0)
    return {
        "sales": sales, "cogs": cogs, "gp": gp,
        "gp_pct": (gp / sales * 100.0 if sales else 0.0),
        "orders": float(orders),
    }


def _headline_qb_metrics(db_module, month: str,
                          qb_by_month: Optional[dict] = None
                          ) -> Dict[str, float]:
    """QuickBooks is only ever available at whole-month granularity
    (qbo_monthly_pl has no daily breakdown), so there's no partial-
    period version of this — only used for the closed-month YoY
    comparison, never the month-to-date one."""
    if qb_by_month is None:
        mappings = db_module.get_qbo_account_mappings()
        qb_by_month = db_module.qbo_monthly_pl_summary_by_category(mappings)
    qb = qb_by_month.get(month) or {}
    return {
        "net_sales": qb.get("sales", 0.0),
        "total_revenue": qb.get("total_income", 0.0),
        "net_income": qb.get("qb_net_income", 0.0),
    }


def _pct_delta(cur: float, prior: float) -> Optional[float]:
    if not prior:
        return None
    return (cur - prior) / abs(prior) * 100.0


def _fmt_delta_pct(cur: float, prior: float) -> str:
    d = _pct_delta(cur, prior)
    if d is None:
        return "n/a (no prior-year baseline)"
    arrow = "▲" if d >= 0 else "▼"
    return f"{arrow} {abs(d):.0f}%"


def build_commentary(data: Dict[str, Any], month: str,
                      partial_month: str,
                      today: Optional[date] = None) -> Dict[str, str]:
    """James: month-on-month... compared to the previous year month —
    i.e. this report's closed month (e.g. June 2026) vs the SAME month
    last year (June 2025), PLUS this month-to-date (partial, however
    many days in) vs the SAME number of days in that same month last
    year — not a plain sequential month-over-month comparison.

    Returns {"html": ..., "slack": ...} — same underlying numbers,
    formatted for reportlab's Paragraph markup and Slack's mrkdwn
    respectively."""
    today = today or date.today()
    sale_lines = data["sale_lines"]
    db_module = data["db"]

    yoy_month = _yoy_month(month)
    yoy_partial_month = _yoy_month(partial_month)
    day_cutoff = today.day

    cur_closed = _headline_cin7_metrics(sale_lines, month)
    yoy_closed = _headline_cin7_metrics(sale_lines, yoy_month)
    cur_partial = _headline_cin7_metrics(
        sale_lines, partial_month, max_day=day_cutoff)
    yoy_partial = _headline_cin7_metrics(
        sale_lines, yoy_partial_month, max_day=day_cutoff)

    qb_cur_closed = _headline_qb_metrics(db_module, month)
    qb_yoy_closed = _headline_qb_metrics(db_module, yoy_month)

    def _row_html(label, cur, prior, fmt):
        return (f"{label}: {fmt(cur)} vs {fmt(prior)} last year "
                f"({_fmt_delta_pct(cur, prior)})")

    def _money(v):
        return f"${v:,.0f}"

    def _pct(v):
        return f"{v:.1f}%"

    def _num_fmt(v):
        return f"{v:,.0f}"

    closed_lines = [
        _row_html("Sales $", cur_closed["sales"], yoy_closed["sales"], _money),
        _row_html("Gross Profit", cur_closed["gp"], yoy_closed["gp"], _money),
        _row_html("GP %", cur_closed["gp_pct"], yoy_closed["gp_pct"], _pct),
        _row_html("Orders", cur_closed["orders"], yoy_closed["orders"], _num_fmt),
        _row_html("QB Net Income", qb_cur_closed["net_income"],
                   qb_yoy_closed["net_income"], _money),
    ]
    partial_lines = [
        _row_html("Sales $", cur_partial["sales"], yoy_partial["sales"], _money),
        _row_html("Gross Profit", cur_partial["gp"], yoy_partial["gp"], _money),
        _row_html("Orders", cur_partial["orders"], yoy_partial["orders"], _num_fmt),
    ]

    html = (
        f"<b>{month} vs {yoy_month} (same month last year):</b><br/>"
        + "<br/>".join(closed_lines)
        + f"<br/><br/><b>{partial_month} month-to-date (thru "
          f"{today:%b %d}) vs the same {day_cutoff} days in "
          f"{yoy_partial_month} last year:</b><br/>"
        + "<br/>".join(partial_lines)
        + "<br/><br/><i>QuickBooks figures (Net Income) are only "
          "available at whole-month granularity, so no month-to-date "
          "version is shown for that line.</i>"
    )
    slack = (
        f"*{month} vs {yoy_month} (same month last year):*\n"
        + "\n".join(f"• {l}" for l in closed_lines)
        + f"\n\n*{partial_month} month-to-date (thru {today:%b %d}) "
          f"vs the same {day_cutoff} days in {yoy_partial_month} last "
          f"year:*\n"
        + "\n".join(f"• {l}" for l in partial_lines)
    )
    return {"html": html, "slack": slack}


# ---------------------------------------------------------------------------
# Per-section, per-month row computation
# ---------------------------------------------------------------------------
# {section: [(metric label, format), ...]} — same labels/order as the
# dashboard's Monthly Metrics page. Section 4 isn't here — it's a
# current snapshot, not a per-month row (see compute_inventory_snapshot).
_SECTION_ROWS: Dict[str, List[Tuple[str, str]]] = {
    "1. Sales Overview [App]": [
        ("Sales $", "money"),
        ("Sales $ with Tax", "money"),
        ("# Orders", "int"),
        ("Quantity Sold", "int"),
        ("COGS", "money"),
        ("Discounts", "money"),
        ("Tax $", "money"),
        ("Gross Profit", "money"),
        ("GP %", "pct"),
    ],
    "2. Margins & Purchasing [App]": [
        ("Avg Order Value", "money"),
        ("# of Purchases", "int"),
        ("Purchase $", "money"),
    ],
    "3. Customer Metrics [App]": [
        ("New Customers", "int"),
        ("Running Customer Count", "int"),
        ("Lost Customers (3mo)", "int"),
        ("Repeat Customer %", "pct"),
    ],
    "5. Revenue by Channel [Cin7/DEAR]": [
        ("Shopify (Online Store)", "money"),
        ("Shopify (Draft Orders)", "money"),
        ("Shopify (Other/Unclassified)", "money"),
        ("Shopify Total", "money"),
        ("B2B / Direct", "money"),
        ("Amazon", "money"),
        ("eBay", "money"),
        ("Total (CIN7)", "money"),
        ("Net Sales (QB 400)", "money"),
    ],
    "6. Sales & Adjustments [QuickBooks]": [
        ("Gross Sales (est.)", "money"),
        ("Less: Discounts", "money"),
        ("Net Sales (QB 400)", "money"),
        ("Shipping Income (QB 405)", "money"),
        ("Total Revenue (QB Total Income)", "money"),
    ],
    "7. Cost & Profitability [QuickBooks]": [
        ("Product COGS (QB 500)", "money"),
        ("Amazon Fees (QB 502)", "money"),
        ("Inventory Adj (QB 550)", "money"),
        ("Total COGS", "money"),
        ("Gross Profit (QB)", "money"),
        ("GP %", "pct"),
        ("Total OpEx", "money"),
        ("Operating Profit", "money"),
        ("Op Margin %", "pct"),
        ("Net Income (QB)", "money"),
    ],
    "8. Shipping Detail [QuickBooks]": [
        ("Shipping Charged (QB 405)", "money"),
        ("Shipping-Out Cost (QB 694)", "money"),
        ("Shipping Margin", "money"),
        ("Margin %", "pct"),
    ],
    "9. Order Counts [Cin7/DEAR]": [
        ("Shopify (Online Store) Orders", "int"),
        ("Shopify (Draft Orders) Count", "int"),
        ("Shopify (Other/Unclassified) Orders", "int"),
        ("Shopify Total Orders", "int"),
        ("B2B / Direct Orders", "int"),
        ("Amazon Orders", "int"),
        ("eBay Orders", "int"),
        ("Total Orders", "int"),
    ],
}

_SECTION_ORDER = [
    "1. Sales Overview [App]",
    "2. Margins & Purchasing [App]",
    "3. Customer Metrics [App]",
    "5. Revenue by Channel [Cin7/DEAR]",
    "6. Sales & Adjustments [QuickBooks]",
    "7. Cost & Profitability [QuickBooks]",
    "8. Shipping Detail [QuickBooks]",
    "9. Order Counts [Cin7/DEAR]",
]


def _customer_first_last_seen(sale_lines):
    """{CustomerID: first/last purchase MonthKey} over ALL history —
    same basis app.py's Running Customer Count / Lost Customers (3mo)
    / Repeat Customer % use, computed once rather than per month."""
    import pandas as pd
    sl = sale_lines.copy()
    sl["InvoiceDate"] = pd.to_datetime(sl.get("InvoiceDate"), errors="coerce")
    if "Status" in sl.columns:
        sl = sl[~sl["Status"].astype(str).str.upper().isin(_BAD_STATUSES)]
    sl = sl.dropna(subset=["InvoiceDate", "CustomerID"])
    sl["MonthKey"] = sl["InvoiceDate"].dt.to_period("M")
    first_seen = sl.groupby("CustomerID")["MonthKey"].min()
    last_seen = sl.groupby("CustomerID")["MonthKey"].max()
    return first_seen, last_seen


def compute_month_values(data: Dict[str, Any], month: str,
                          first_seen, last_seen,
                          qb_by_month: dict,
                          shopify_disc_by_month: dict,
                          shop_rev_by_src, shop_cnt_by_src
                          ) -> Dict[str, Dict[str, float]]:
    """Raw (unformatted) values for every row in every per-month
    section, for ONE month. Mirrors app.py's Monthly Metrics row
    formulas exactly (same labels/order/sign conventions) so the PDF
    and dashboard always agree. The four precomputed arguments are
    all whole-history aggregates that don't need re-deriving per
    month — passed in so the caller only builds them once for the
    whole report instead of once per month."""
    import pandas as pd

    sale_lines = data["sale_lines"]
    purchase_lines = data["purchase_lines"]

    month_period = pd.Period(month, freq="M")
    sl_month = _month_lines(sale_lines, month)
    prod, _ship = _split_product_shipping(sl_month)

    sales = float(_num(prod, "Total").sum())
    tax = float(_num(prod, "Tax").sum())
    quantity = float(_num(prod, "Quantity").sum())
    cogs = float((_num(prod, "Quantity") * _num(prod, "AverageCost")).sum())
    orders = int(sl_month["SaleID"].nunique()) if "SaleID" in sl_month.columns else 0
    gp = sales - cogs

    # Discounts: prefer the Shopify Admin API's own total (same source
    # Section 1 + Section 6 both use on the dashboard) over CIN7's
    # line-level Discount proxy, which undercounts true Shopify
    # discounts by 60-70% (Viktor audit).
    _shopify_disc_month = shopify_disc_by_month.get(month)
    if _shopify_disc_month is not None and float(_shopify_disc_month) > 0:
        discounts = float(_shopify_disc_month)
    else:
        discounts = abs(float(_num(prod, "Discount").sum()))

    out: Dict[str, Dict[str, float]] = {}

    out["1. Sales Overview [App]"] = {
        "Sales $": sales,
        "Sales $ with Tax": sales + tax,
        "# Orders": orders,
        "Quantity Sold": quantity,
        "COGS": cogs,
        "Discounts": -discounts,
        "Tax $": tax,
        "Gross Profit": gp,
        "GP %": (gp / sales * 100 if sales else 0.0),
    }

    # ---- 2. Margins & Purchasing -------------------------------------
    avg_order_value = sales / orders if orders else 0.0
    po_count = 0
    po_spend = 0.0
    if not purchase_lines.empty:
        pl = purchase_lines.copy()
        pl["OrderDate"] = pd.to_datetime(pl.get("OrderDate"), errors="coerce")
        pl = pl.dropna(subset=["OrderDate"])
        pl["MonthKey"] = pl["OrderDate"].dt.to_period("M")
        pl_month = pl[pl["MonthKey"] == month_period]
        if "PurchaseID" in pl_month.columns:
            po_count = int(pl_month["PurchaseID"].nunique())
        po_spend = float(_num(pl_month, "Total").sum())
    out["2. Margins & Purchasing [App]"] = {
        "Avg Order Value": avg_order_value,
        "# of Purchases": po_count,
        "Purchase $": po_spend,
    }

    # ---- 3. Customer Metrics ------------------------------------------
    cust_in_month = (
        set(sl_month.dropna(subset=["CustomerID"])["CustomerID"].unique())
        if "CustomerID" in sl_month.columns else set())
    new_custs = {c for c in cust_in_month
                 if first_seen.get(c) == month_period}
    running_count = int((first_seen <= month_period).sum())
    lost_target = month_period - 3
    lost_count = int((last_seen == lost_target).sum())
    if cust_in_month:
        repeat = sum(
            1 for c in cust_in_month
            if first_seen.get(c) is not None and first_seen.get(c) < month_period)
        repeat_pct = repeat / len(cust_in_month) * 100
    else:
        repeat_pct = 0.0
    out["3. Customer Metrics [App]"] = {
        "New Customers": len(new_custs),
        "Running Customer Count": running_count,
        "Lost Customers (3mo)": lost_count,
        "Repeat Customer %": repeat_pct,
    }

    # ---- 5/9. Channel revenue + order counts ---------------------------
    if "SourceChannel" in sl_month.columns or "SalesRepresentative" in sl_month.columns:
        chan = prod.apply(
            lambda r: _channel_of_row(
                r.get("SourceChannel"), r.get("SalesRepresentative")),
            axis=1)
    else:
        chan = pd.Series("B2B / Direct", index=prod.index)
    _base_rev = prod.groupby(chan)["Total"].apply(
        lambda s: float(_num(prod.loc[s.index], "Total").sum()))
    _base_cnt = (prod.assign(_chan=chan).groupby("_chan")["SaleID"].nunique()
                 if "SaleID" in prod.columns else pd.Series(dtype=int))

    _cin7_shopify_rev = float(_base_rev.get("Shopify", 0.0))
    _cin7_shopify_cnt = int(_base_cnt.get("Shopify", 0))
    _online_rev = float(shop_rev_by_src.get((month_period, "web"), 0.0))
    _draft_rev = float(shop_rev_by_src.get(
        (month_period, "shopify_draft_order"), 0.0))
    _other_rev = max(_cin7_shopify_rev - _online_rev - _draft_rev, 0.0)
    _online_cnt = int(shop_cnt_by_src.get((month_period, "web"), 0))
    _draft_cnt = int(shop_cnt_by_src.get(
        (month_period, "shopify_draft_order"), 0))
    _other_cnt = max(_cin7_shopify_cnt - _online_cnt - _draft_cnt, 0)

    b2b_rev = float(_base_rev.get("B2B / Direct", 0.0))
    amazon_rev = float(_base_rev.get("Amazon", 0.0))
    ebay_rev = float(_base_rev.get("eBay", 0.0))
    b2b_cnt = int(_base_cnt.get("B2B / Direct", 0))
    amazon_cnt = int(_base_cnt.get("Amazon", 0))
    ebay_cnt = int(_base_cnt.get("eBay", 0))

    shopify_total_rev = _online_rev + _draft_rev + _other_rev
    shopify_total_cnt = _online_cnt + _draft_cnt + _other_cnt
    # "Total (CIN7)" ties to Section 1's Sales $ — built from the
    # ORIGINAL unsplit CIN7 Shopify figure, not the split sub-rows,
    # so it stays robust to the Online+Draft-exceeds-CIN7-total edge
    # case (where Other/Unclassified floors at 0).
    total_cin7_rev = _cin7_shopify_rev + b2b_rev + amazon_rev + ebay_rev
    total_cin7_cnt = _cin7_shopify_cnt + b2b_cnt + amazon_cnt + ebay_cnt

    qb = qb_by_month.get(month) or {}

    def _qb(cat: str) -> float:
        return float(qb.get(cat, 0.0) or 0.0)

    out["5. Revenue by Channel [Cin7/DEAR]"] = {
        "Shopify (Online Store)": _online_rev,
        "Shopify (Draft Orders)": _draft_rev,
        "Shopify (Other/Unclassified)": _other_rev,
        "Shopify Total": shopify_total_rev,
        "B2B / Direct": b2b_rev,
        "Amazon": amazon_rev,
        "eBay": ebay_rev,
        "Total (CIN7)": total_cin7_rev,
        "Net Sales (QB 400)": _qb("sales"),
    }
    out["9. Order Counts [Cin7/DEAR]"] = {
        "Shopify (Online Store) Orders": _online_cnt,
        "Shopify (Draft Orders) Count": _draft_cnt,
        "Shopify (Other/Unclassified) Orders": _other_cnt,
        "Shopify Total Orders": shopify_total_cnt,
        "B2B / Direct Orders": b2b_cnt,
        "Amazon Orders": amazon_cnt,
        "eBay Orders": ebay_cnt,
        "Total Orders": total_cin7_cnt,
    }

    # ---- 6/7/8. QuickBooks-sourced sections ----------------------------
    out["6. Sales & Adjustments [QuickBooks]"] = {
        "Gross Sales (est.)": _qb("sales") + discounts,
        "Less: Discounts": discounts,
        "Net Sales (QB 400)": _qb("sales"),
        "Shipping Income (QB 405)": _qb("shipping_charged"),
        "Total Revenue (QB Total Income)": _qb("total_income"),
    }
    total_income = _qb("total_income")
    out["7. Cost & Profitability [QuickBooks]"] = {
        "Product COGS (QB 500)": _qb("cogs"),
        "Amazon Fees (QB 502)": _qb("cogs_amazon_fees"),
        "Inventory Adj (QB 550)": _qb("inventory_adjustment"),
        "Total COGS": _qb("total_cogs"),
        "Gross Profit (QB)": _qb("qb_gross_profit"),
        "GP %": (_qb("qb_gross_profit") / total_income * 100
                 if total_income else 0.0),
        "Total OpEx": _qb("qb_total_expenses"),
        "Operating Profit": _qb("qb_net_operating_income"),
        "Op Margin %": (_qb("qb_net_operating_income") / total_income * 100
                        if total_income else 0.0),
        "Net Income (QB)": _qb("qb_net_income"),
    }
    ship_charged = _qb("shipping_charged")
    ship_cost = _qb("shipping_cost")
    ship_margin = ship_charged - ship_cost
    out["8. Shipping Detail [QuickBooks]"] = {
        "Shipping Charged (QB 405)": ship_charged,
        "Shipping-Out Cost (QB 694)": ship_cost,
        "Shipping Margin": ship_margin,
        "Margin %": (ship_margin / ship_charged * 100
                     if ship_charged else 0.0),
    }
    return out


def compute_monthly_tables(data: Dict[str, Any], months: List[str]
                            ) -> Dict[str, Dict[str, Dict[str, float]]]:
    """{section: {metric label: {month: raw value}}} across every
    month in `months`. Whole-history aggregates (customer first/last
    seen, QBO P&L, Shopify discounts, Shopify order split) are built
    ONCE here and threaded through per-month calls, rather than
    re-derived 14 times."""
    db = data["db"]
    sale_lines = data["sale_lines"]

    first_seen, last_seen = _customer_first_last_seen(sale_lines)
    try:
        qb_by_month = db.qbo_monthly_pl_summary_by_category(
            db.get_qbo_account_mappings())
    except Exception:  # noqa: BLE001
        qb_by_month = {}
    try:
        shopify_disc_by_month = db.all_shopify_monthly_discounts() or {}
    except Exception:  # noqa: BLE001
        shopify_disc_by_month = {}
    shop_rev_by_src, shop_cnt_by_src = _shopify_rev_and_cnt_by_source(
        data.get("shopify_orders"))

    raw: Dict[str, Dict[str, Dict[str, float]]] = {}
    for m in months:
        month_vals = compute_month_values(
            data, m, first_seen, last_seen, qb_by_month,
            shopify_disc_by_month, shop_rev_by_src, shop_cnt_by_src)
        for section, metrics in month_vals.items():
            raw.setdefault(section, {})
            for label, v in metrics.items():
                raw[section].setdefault(label, {})[m] = v
    return raw


def compute_inventory_snapshot(data: Dict[str, Any]) -> Dict[str, float]:
    """Section 4 — a CURRENT stock-value snapshot (not a per-month
    trend, see the module docstring for why)."""
    import pandas as pd
    stock = data["stock"]
    db = data["db"]
    stock_val_col = "StockOnHand" if "StockOnHand" in stock.columns else None
    total_stock_value = (
        float(pd.to_numeric(stock[stock_val_col], errors="coerce")
              .fillna(0).sum()) if stock_val_col else 0.0)
    try:
        warnings = db.get_dormancy_warnings()
        slow_skus = {str(w.get("SKU") or w.get("sku") or "")
                     for w in warnings} if warnings else set()
    except Exception:  # noqa: BLE001
        slow_skus = set()
    slow_value = 0.0
    if slow_skus and stock_val_col and "SKU" in stock.columns:
        mask = stock["SKU"].astype(str).isin(slow_skus)
        slow_value = float(
            pd.to_numeric(stock.loc[mask, stock_val_col], errors="coerce")
            .fillna(0).sum())
    return {
        "Total Stock Value (current)": total_stock_value,
        "Slow-Moving Stock Value (current)": slow_value,
    }


# ---------------------------------------------------------------------------
# PDF assembly
# ---------------------------------------------------------------------------
# Same muted, printer-friendly palette as po_pdf.py
_C_HEAD = "#1f2933"
_C_SUB = "#52606d"
_C_BORDER = "#c3ccd8"
_C_ZEBRA = "#f3f5f8"
_C_SUMMARY_BG = "#eef1f5"


def _fmt_value(v: Any, fmt: str) -> str:
    try:
        v = float(v)
    except (TypeError, ValueError):
        return ""
    if fmt == "money":
        return f"${v:,.0f}"
    if fmt == "int":
        return f"{v:,.0f}"
    if fmt == "pct":
        return f"{v:.1f}%"
    return f"{v}"


# {section: [(pie slice label, metric row to pull), ...]} — same
# category breakdown as the dashboard's per-section pie (James,
# 2026-07-23: bring the pies back, one per section, even if that
# means each section gets its own page). Values are summed across
# the CURRENT calendar year's months in the report range, matching
# the dashboard's own YTD-based pie.
_PIE_SECTION_METRICS: Dict[str, List[Tuple[str, str]]] = {
    "1. Sales Overview [App]": [
        ("COGS", "COGS"), ("Discounts", "Discounts"),
        ("Gross Profit", "Gross Profit")],
    "3. Customer Metrics [App]": [
        ("New Customers", "New Customers"),
        ("Lost Customers (3mo)", "Lost Customers (3mo)")],
    "5. Revenue by Channel [Cin7/DEAR]": [
        ("Shopify Online", "Shopify (Online Store)"),
        ("Shopify Draft Orders", "Shopify (Draft Orders)"),
        ("Shopify Other", "Shopify (Other/Unclassified)"),
        ("Amazon", "Amazon"), ("eBay", "eBay"),
        ("B2B / Direct", "B2B / Direct")],
    "7. Cost & Profitability [QuickBooks]": [
        ("Product COGS", "Product COGS (QB 500)"),
        ("Amazon Fees", "Amazon Fees (QB 502)"),
        ("Inventory Adj", "Inventory Adj (QB 550)")],
    "8. Shipping Detail [QuickBooks]": [
        ("Shipping Charged", "Shipping Charged (QB 405)"),
        ("Shipping-Out Cost", "Shipping-Out Cost (QB 694)")],
    "9. Order Counts [Cin7/DEAR]": [
        ("Shopify Online", "Shopify (Online Store) Orders"),
        ("Shopify Draft Orders", "Shopify (Draft Orders) Count"),
        ("Shopify Other", "Shopify (Other/Unclassified) Orders"),
        ("Amazon", "Amazon Orders"), ("eBay", "eBay Orders"),
        ("B2B / Direct", "B2B / Direct Orders")],
}
_PIE_COLORS = ["#2f6fed", "#e8833a", "#3aa76d", "#c94f4f", "#8e6fce",
               "#5aa9a3"]


def _render_pie(pie: Dict[str, float], title: str = "") -> Optional[bytes]:
    """Render one pie chart to PNG bytes. Returns None if there's
    nothing meaningful to plot (all-zero / empty).

    Uses a legend below the pie rather than labels on the wedges
    themselves: several sections here regularly have one or two very
    small slices next to much larger ones (e.g. "Amazon Fees" next to
    "Product COGS"), and on-wedge labels for adjacent thin slices
    overlap into an illegible smear regardless of font size. A legend
    is robust to any slice-size distribution."""
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    labels = [k for k, v in pie.items() if v and v > 0]
    values = [v for v in pie.values() if v and v > 0]
    if not values or sum(values) <= 0:
        return None

    fig, ax = plt.subplots(figsize=(3.6, 4.1), dpi=150)
    total = sum(values)
    wedges, _ = ax.pie(
        values, startangle=90, colors=_PIE_COLORS[:len(values)])
    legend_labels = [f"{lbl} ({v / total * 100:.0f}%)"
                      for lbl, v in zip(labels, values)]
    ax.legend(
        wedges, legend_labels, loc="upper center",
        bbox_to_anchor=(0.5, -0.02), ncol=1, frameon=False,
        fontsize=9, handlelength=1.0, handletextpad=0.5,
        labelspacing=0.4,
    )
    if title:
        ax.set_title(title, fontsize=11)
    ax.axis("equal")
    buf = io.BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight", transparent=True)
    plt.close(fig)
    buf.seek(0)
    return buf.getvalue()


def _pie_dict_for_section(section: str,
                           tables: Dict[str, Dict[str, Dict[str, float]]],
                           months: List[str],
                           ytd_year: int) -> Optional[dict]:
    """Sums each pie slice's metric over the current calendar year's
    months in the report range — same YTD basis as the dashboard's
    own per-section pie."""
    section_data = tables.get(section, {})

    def _ytd_sum(label: str) -> float:
        per_month = section_data.get(label, {})
        return sum(per_month.get(m, 0.0) for m in months
                   if int(m.split("-")[0]) == ytd_year)

    if section == "6. Sales & Adjustments [QuickBooks]":
        net_sales = _ytd_sum("Net Sales (QB 400)")
        ship_income = _ytd_sum("Shipping Income (QB 405)")
        total_rev = _ytd_sum("Total Revenue (QB Total Income)")
        sundry = max(total_rev - net_sales - ship_income, 0.0)
        return {"Net Sales": max(net_sales, 0.0),
                "Shipping Income": max(ship_income, 0.0),
                "Sundry Income": sundry}
    cfg = _PIE_SECTION_METRICS.get(section)
    if not cfg:
        return None
    return {label: abs(_ytd_sum(metric)) for label, metric in cfg}


def build_pdf(tables: Dict[str, Dict[str, Dict[str, float]]],
              months: List[str],
              inventory_snapshot: Dict[str, float],
              month: str,
              current_month: str,
              commentary: Optional[str] = None,
              company: str = "Wired4Signs USA") -> bytes:
    """Landscape, one wide table per section (Metric rows x month
    columns + YTD + Avg), matching the dashboard's own Monthly
    Metrics tables row-for-row. `months` is the full column range
    shown (oldest to newest); the last entry is `current_month`, the
    still-in-progress month — included as a normal column, same as
    the dashboard, with a note at the top rather than a separate
    callout box."""
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import letter, landscape
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.units import inch
    from reportlab.platypus import (
        SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
        KeepTogether, Image, PageBreak,
    )

    c_head = colors.HexColor(_C_HEAD)
    c_sub = colors.HexColor(_C_SUB)
    c_border = colors.HexColor(_C_BORDER)
    c_zebra = colors.HexColor(_C_ZEBRA)
    c_summary_bg = colors.HexColor(_C_SUMMARY_BG)

    page_size = landscape(letter)
    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf, pagesize=page_size,
        leftMargin=0.4 * inch, rightMargin=0.4 * inch,
        topMargin=0.4 * inch, bottomMargin=0.4 * inch,
        title=f"Monthly Metrics — {month}",
        author=company,
    )
    styles = getSampleStyleSheet()
    title_style = ParagraphStyle(
        "TitleW4S", parent=styles["Title"], fontSize=16, leading=19,
        textColor=c_head, spaceAfter=2)
    sub_style = ParagraphStyle(
        "SubW4S", parent=styles["Normal"], fontSize=8.5, leading=11,
        textColor=c_sub)
    section_style = ParagraphStyle(
        "SectionW4S", parent=styles["Heading3"], fontSize=10,
        leading=12, textColor=c_head, spaceBefore=6, spaceAfter=3)
    commentary_style = ParagraphStyle(
        "CommentaryW4S", parent=styles["Normal"], fontSize=8.5,
        leading=11.5, textColor=c_head)
    note_style = ParagraphStyle(
        "NoteW4S", parent=styles["Normal"], fontSize=7, leading=9,
        textColor=c_sub)

    story: List = []
    story.append(Paragraph(f"<b>{company}</b> — Monthly Metrics", title_style))
    story.append(Paragraph(
        f"<b>Months shown:</b> {months[0]} &ndash; {months[-1]} "
        f"(rightmost column, {current_month}, is still in progress — "
        f"month-to-date, not final) &nbsp;&middot;&nbsp; "
        f"<b>Generated:</b> {datetime.now():%Y-%m-%d %H:%M}",
        sub_style))
    story.append(Spacer(1, 6))
    if commentary:
        story.append(Paragraph(commentary, commentary_style))
        story.append(Spacer(1, 8))

    ytd_year = int(current_month.split("-")[0])
    n_data_cols = len(months) + 2  # + YTD + Avg
    page_w, _page_h = page_size
    usable_w = page_w - doc.leftMargin - doc.rightMargin
    metric_col_w = 1.3 * inch
    data_col_w = (usable_w - metric_col_w) / n_data_cols

    def _section_table(section: str) -> Table:
        rows_schema = _SECTION_ROWS[section]
        section_data = tables.get(section, {})
        header = ["Metric"] + months + ["YTD", "Avg"]
        data_rows: List[list] = [header]
        for label, fmt in rows_schema:
            per_month = section_data.get(label, {})
            vals_in_range = [per_month.get(m, 0.0) for m in months]
            ytd_vals = [per_month.get(m, 0.0) for m in months
                        if int(m.split("-")[0]) == ytd_year]
            if fmt == "pct":
                ytd = sum(ytd_vals) / len(ytd_vals) if ytd_vals else 0.0
            else:
                ytd = sum(ytd_vals)
            avg = (sum(vals_in_range) / len(vals_in_range)
                   if vals_in_range else 0.0)
            row = ([label] + [_fmt_value(v, fmt) for v in vals_in_range]
                   + [_fmt_value(ytd, fmt), _fmt_value(avg, fmt)])
            data_rows.append(row)

        col_widths = [metric_col_w] + [data_col_w] * n_data_cols
        t = Table(data_rows, colWidths=col_widths, repeatRows=1)
        style_cmds = [
            ("FONTSIZE", (0, 0), (-1, -1), 6.3),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("BACKGROUND", (0, 0), (-1, 0), c_head),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
            ("TEXTCOLOR", (0, 1), (-1, -1), c_head),
            ("ALIGN", (1, 0), (-1, -1), "RIGHT"),
            ("ALIGN", (0, 0), (0, -1), "LEFT"),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ("TOPPADDING", (0, 0), (-1, -1), 2.5),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 2.5),
            ("LEFTPADDING", (0, 0), (-1, -1), 3),
            ("RIGHTPADDING", (0, 0), (-1, -1), 3),
            ("BOX", (0, 0), (-1, -1), 0.4, c_border),
            ("INNERGRID", (0, 0), (-1, -1), 0.25, c_border),
            # YTD/Avg summary columns get their own shaded background.
            ("BACKGROUND", (-2, 1), (-1, -1), c_summary_bg),
        ]
        for r in range(1, len(data_rows), 2):
            style_cmds.append(
                ("BACKGROUND", (0, r), (-3, r), c_zebra))
        t.setStyle(TableStyle(style_cmds))
        return t

    # James, 2026-07-23: bring the pies back, placed with each
    # section, even if that means each section gets its own page —
    # the wide table already uses the full landscape width, so the
    # pie goes below it rather than beside it, using whatever
    # vertical room the page has left.
    def _pie_image(pie: Optional[dict]):
        if not pie:
            return None
        png = _render_pie(pie)
        if not png:
            return None
        # Matches _render_pie's figsize aspect ratio (3.6 x 4.1).
        return Image(io.BytesIO(png), width=2.6 * inch,
                     height=2.6 * inch * (4.1 / 3.6))

    sections_to_render = [s for s in _SECTION_ORDER if s in _SECTION_ROWS]
    for i, section in enumerate(sections_to_render):
        block = [Paragraph(section, section_style),
                 _section_table(section)]
        pie = _pie_dict_for_section(section, tables, months, ytd_year)
        img = _pie_image(pie)
        if img:
            block.append(Spacer(1, 10))
            block.append(img)
        story.append(KeepTogether(block))
        story.append(PageBreak())

    # Section 4 — current snapshot only (see module docstring for why
    # it isn't a per-month trend row like the others).
    inv_rows = [
        ["Metric", "Value"],
        ["Total Stock Value (current)",
         _fmt_value(inventory_snapshot.get(
             "Total Stock Value (current)", 0.0), "money")],
        ["Slow-Moving Stock Value (current)",
         _fmt_value(inventory_snapshot.get(
             "Slow-Moving Stock Value (current)", 0.0), "money")],
    ]
    inv_table = Table(inv_rows, colWidths=[2.4 * inch, 1.3 * inch])
    inv_table.setStyle(TableStyle([
        ("FONTSIZE", (0, 0), (-1, -1), 7.5),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("BACKGROUND", (0, 0), (-1, 0), c_head),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("ALIGN", (1, 0), (1, -1), "RIGHT"),
        ("BOX", (0, 0), (-1, -1), 0.4, c_border),
        ("INNERGRID", (0, 0), (-1, -1), 0.25, c_border),
        ("TOPPADDING", (0, 0), (-1, -1), 2.5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 2.5),
    ]))
    inv_pie = {"Slow-Moving Stock Value": max(
                   inventory_snapshot.get(
                       "Slow-Moving Stock Value (current)", 0.0), 0.0),
               "Other Stock Value": max(
                   inventory_snapshot.get(
                       "Total Stock Value (current)", 0.0)
                   - inventory_snapshot.get(
                       "Slow-Moving Stock Value (current)", 0.0), 0.0)}
    inv_block = [
        Paragraph("4. Inventory [App]", section_style),
        inv_table,
        Spacer(1, 2),
        Paragraph(
            "<i>Simplified: a CURRENT stock-value snapshot at report "
            "time (not a per-month figure) — the dashboard's modelled "
            "month-average walk-back isn't reproduced here.</i>",
            note_style),
    ]
    inv_img = _pie_image(inv_pie)
    if inv_img:
        inv_block.append(Spacer(1, 10))
        inv_block.append(inv_img)
    story.append(KeepTogether(inv_block))

    def _footer(canvas, doc_):
        canvas.saveState()
        canvas.setFont("Helvetica", 7)
        canvas.setFillColor(c_sub)
        pw, _ph = page_size
        canvas.drawString(
            0.4 * inch, 0.22 * inch,
            f"{company} · Monthly Metrics · "
            f"Generated {datetime.now():%Y-%m-%d %H:%M}")
        canvas.drawRightString(pw - 0.4 * inch, 0.22 * inch,
                                f"Page {doc_.page}")
        canvas.restoreState()

    doc.build(story, onFirstPage=_footer, onLaterPages=_footer)
    return buf.getvalue()


# ---------------------------------------------------------------------------
# Slack delivery
# ---------------------------------------------------------------------------
def post_pdf_to_slack(pdf_bytes: bytes, month: str,
                        commentary_slack: Optional[str] = None
                        ) -> Tuple[bool, str]:
    """Upload the PDF to Slack via files.getUploadURLExternal ->
    (presigned PUT) -> files.completeUploadExternal, matching the
    existing bot-token session pattern in slack_sync.py. Returns
    (ok, message)."""
    import requests

    token = os.environ.get("SLACK_BOT_TOKEN", "").strip()
    channel = os.environ.get("SLACK_MONTHLY_REPORT_CHANNEL_ID", "").strip()
    if not token:
        return False, "SLACK_BOT_TOKEN not set — skipping Slack post."
    if not channel:
        return False, ("SLACK_MONTHLY_REPORT_CHANNEL_ID not set — "
                         "skipping Slack post.")

    _here = Path(__file__).resolve().parent
    if str(_here) not in sys.path:
        sys.path.insert(0, str(_here))
    import slack_sync

    session = slack_sync._build_session(token)
    filename = f"monthly_metrics_{month}.pdf"

    # Step 1: get a presigned upload URL.
    resp = session.get(
        f"{slack_sync.SLACK_API}/files.getUploadURLExternal",
        params={"filename": filename, "length": len(pdf_bytes)},
        timeout=slack_sync.DEFAULT_TIMEOUT,
    )
    resp.raise_for_status()
    body = resp.json()
    if not body.get("ok"):
        return False, f"files.getUploadURLExternal failed: {body}"
    upload_url = body["upload_url"]
    file_id = body["file_id"]

    # Step 2: PUT the bytes to the presigned URL. NOT the bot-token
    # session — this is an unauthenticated presigned URL, a fresh
    # plain request is required.
    put_resp = requests.post(
        upload_url, files={"file": (filename, pdf_bytes,
                                     "application/pdf")},
        timeout=60)
    put_resp.raise_for_status()

    # Step 3: complete the upload, sharing it to the target channel
    # with a short message + the YoY commentary underneath.
    comment = (
        f"📊 Monthly Metrics report through *{month}* is ready "
        f"— see the attached PDF."
    )
    if commentary_slack:
        comment += f"\n\n{commentary_slack}"
    complete_body = slack_sync._slack_post(
        session, "files.completeUploadExternal", {
            "files": [{"id": file_id, "title": f"Monthly Metrics {month}"}],
            "channel_id": channel,
            "initial_comment": comment,
        })
    if not complete_body.get("ok"):
        return False, f"files.completeUploadExternal failed: {complete_body}"
    return True, f"posted to channel {channel}"


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main() -> int:
    month = _target_month()
    current_month = _current_partial_month()
    months = _report_months(current_month, lookback=14)
    _emit(f"building report for {months[0]}..{months[-1]} "
          f"(rightmost column partial)")

    try:
        cin7_data = _load_cin7_data()
    except Exception as exc:  # noqa: BLE001
        _emit(f"CIN7 data load failed: {exc!r}", level="error")
        return 2

    try:
        tables = compute_monthly_tables(cin7_data, months)
        inventory_snapshot = compute_inventory_snapshot(cin7_data)
    except Exception as exc:  # noqa: BLE001
        _emit(f"section computation failed: {exc!r}", level="error")
        return 3

    # Year-over-year commentary (closed month vs same month last year,
    # + month-to-date vs the same number of days last year). Best-
    # effort — a commentary failure shouldn't block the PDF/Slack
    # post going out.
    try:
        commentary = build_commentary(cin7_data, month, current_month)
    except Exception as exc:  # noqa: BLE001
        _emit(f"commentary build failed (continuing without it): "
              f"{exc!r}", level="warn")
        commentary = {"html": None, "slack": None}

    try:
        pdf_bytes = build_pdf(tables, months, inventory_snapshot, month,
                               current_month,
                               commentary=commentary.get("html"))
    except Exception as exc:  # noqa: BLE001
        _emit(f"PDF build failed: {exc!r}", level="error")
        return 4

    from data_paths import OUTPUT_DIR
    out_path = OUTPUT_DIR / f"monthly_metrics_{month}.pdf"
    out_path.write_bytes(pdf_bytes)
    _emit(f"wrote {out_path} ({len(pdf_bytes):,} bytes)")

    ok, msg = post_pdf_to_slack(pdf_bytes, month,
                                 commentary_slack=commentary.get("slack"))
    if ok:
        _emit(f"Slack: {msg}")
    else:
        _emit(f"Slack: {msg}", level="warn")
    return 0


if __name__ == "__main__":
    sys.exit(main())
