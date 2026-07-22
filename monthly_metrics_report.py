"""monthly_metrics_report.py
=============================
Generate a one-page-per-view, printable executive-summary PDF of the
Monthly Metrics dashboard (headline KPIs + a pie chart per section for
the most recently CLOSED calendar month) and post it to a Slack
channel via the Wired4Signs bot.

Designed to run a few days into each new month (see the day-of-month
guard in sync_loop.sh), by which point QuickBooks bookkeeping has
mostly caught up on the prior month — reporting on the just-finished
month on day 1 would inherit the inflated-GP%/incomplete-COGS
distortion documented in app.py's Monthly Metrics methodology notes.

This is deliberately a LIGHTER-WEIGHT companion to the full Monthly
Metrics page, not a byte-for-byte port of it — two figures are
simplified on purpose, and are labelled as such in the PDF:
  - "Shipping Charged" here is the sum of CIN7 sale lines matched by
    the simple is-shipping regex (no header-delta/reissue-dedup
    refinement, no LTL-freight recovery). Runs slightly lower than the
    dashboard's fuller figure — see app.py's Shipping Charged
    methodology notes.
  - "Inventory" here is a CURRENT stock-value snapshot (slow-moving vs
    the rest), not the dashboard's modelled month-average walk-back.
Everything else (Sales $, COGS, Discounts, channel/order breakdowns,
customer counts, all QuickBooks-sourced figures) uses the exact same
formulas and data sources as the dashboard.

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
    """The most recently completed calendar month, as 'YYYY-MM'."""
    today = today or date.today()
    year, month = today.year, today.month
    if month == 1:
        return f"{year - 1}-12"
    return f"{year}-{month - 1:02d}"


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
    sale_lines_csv = _latest("sale_lines_last_*d_*.csv")
    purchase_lines_csv = _latest("purchase_lines_last_*.csv")
    missing = [
        name for name, p in (
            ("products", products_csv), ("stock_on_hand", stock_csv),
            ("sale_lines", sale_lines_csv))
        if p is None
    ]
    if missing:
        raise FileNotFoundError(
            f"Missing CSV(s) {missing} in {OUTPUT_DIR}. Has the sync run?")

    products = pd.read_csv(products_csv, low_memory=False)
    stock = pd.read_csv(stock_csv, low_memory=False)
    sale_lines = filter_excluded_sales_customers(
        pd.read_csv(sale_lines_csv, low_memory=False))
    purchase_lines = (
        pd.read_csv(purchase_lines_csv, low_memory=False)
        if purchase_lines_csv is not None else pd.DataFrame())

    return {
        "products": products, "stock": stock,
        "sale_lines": sale_lines, "purchase_lines": purchase_lines,
        "db": db,
    }


_BAD_STATUSES = ("VOIDED", "CREDITED", "CANCELLED", "CANCELED")


def _month_lines(sale_lines, month: str):
    """Sale lines with a valid InvoiceDate inside `month` ('YYYY-MM'),
    excluding voided/credited/cancelled — same net-demand convention
    the dashboard uses."""
    import pandas as pd

    sl = sale_lines.copy()
    sl["InvoiceDate"] = pd.to_datetime(sl.get("InvoiceDate"), errors="coerce")
    sl = sl.dropna(subset=["InvoiceDate"])
    if "Status" in sl.columns:
        sl = sl[~sl["Status"].astype(str).str.upper().isin(_BAD_STATUSES)]
    period = sl["InvoiceDate"].dt.to_period("M").astype(str)
    return sl[period == month].copy()


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


# ---------------------------------------------------------------------------
# Per-section metric computation
# ---------------------------------------------------------------------------
def _num(sl, col):
    import pandas as pd
    return pd.to_numeric(sl.get(col), errors="coerce").fillna(0)


def compute_sections(data: Dict[str, Any], month: str) -> Dict[str, Any]:
    """Returns {section_name: {"pie": {label: value}, "table": [(label,
    value_str), ...], "note": optional str}} for all 9 sections."""
    import pandas as pd

    sale_lines = data["sale_lines"]
    stock = data["stock"]
    db = data["db"]

    sl_month = _month_lines(sale_lines, month)
    prod, ship = _split_product_shipping(sl_month)

    sales = float(_num(prod, "Total").sum())
    cogs = float((_num(prod, "Quantity") * _num(prod, "AverageCost")).sum())
    discounts = float(_num(prod, "Discount").sum())
    gp = sales - cogs
    shipping_charged = float(_num(ship, "Total").sum())

    out: Dict[str, Any] = {}

    # ---- 1. Sales Overview ------------------------------------------
    out["1. Sales Overview [App]"] = {
        "pie": {"COGS": max(cogs, 0), "Discounts": max(discounts, 0),
                "Gross Profit": max(gp, 0)},
        "table": [
            ("Sales $", f"${sales:,.0f}"),
            ("COGS", f"${cogs:,.0f}"),
            ("Discounts", f"-${discounts:,.0f}"),
            ("Gross Profit", f"${gp:,.0f}"),
            ("GP %", f"{(gp / sales * 100 if sales else 0):.1f}%"),
        ],
    }

    # ---- 2. Margins & Purchasing (simplified Shipping Charged) ------
    out["2. Margins & Purchasing [App]"] = {
        "pie": {"Shipping Cost": None, "Shipping Margin": None},
        "table": [("Shipping Charged (simplified)",
                    f"${shipping_charged:,.0f}")],
        "note": ("Simplified: sum of CIN7 lines matched by name "
                 "(shipping/freight/handling/delivery), not the "
                 "dashboard's full header-delta calc — runs lower, "
                 "especially where LTL freight isn't itemised."),
        "skip_pie": True,
    }

    # ---- 3. Customer Metrics ----------------------------------------
    sl_all = sale_lines.copy()
    sl_all["InvoiceDate"] = pd.to_datetime(
        sl_all.get("InvoiceDate"), errors="coerce")
    if "Status" in sl_all.columns:
        sl_all = sl_all[~sl_all["Status"].astype(str).str.upper()
                        .isin(_BAD_STATUSES)]
    sl_all = sl_all.dropna(subset=["InvoiceDate", "CustomerID"])
    first_purchase = sl_all.groupby("CustomerID")["InvoiceDate"].min()
    month_period = pd.Period(month, freq="M")
    cust_in_month = set(
        sl_month.dropna(subset=["CustomerID"])["CustomerID"].unique()
        if "CustomerID" in sl_month.columns else [])
    new_custs = {
        c for c in cust_in_month
        if c in first_purchase.index
        and first_purchase[c].to_period("M") == month_period
    }
    repeat_custs = cust_in_month - new_custs
    out["3. Customer Metrics [App]"] = {
        "pie": {"New Customers": len(new_custs),
                "Repeat Customers": len(repeat_custs)},
        "table": [
            ("New Customers", f"{len(new_custs):,}"),
            ("Repeat Customers", f"{len(repeat_custs):,}"),
            ("Total Customers This Month", f"{len(cust_in_month):,}"),
        ],
    }

    # ---- 4. Inventory (simplified: current snapshot, not walk-back) -
    stock_val_col = ("StockOnHand" if "StockOnHand" in stock.columns
                      else None)
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
    out["4. Inventory [App]"] = {
        "pie": {"Slow-Moving Stock Value": max(slow_value, 0),
                "Other Stock Value": max(total_stock_value - slow_value, 0)},
        "table": [
            ("Total Stock Value (current)", f"${total_stock_value:,.0f}"),
            ("Slow-Moving Stock Value (current)", f"${slow_value:,.0f}"),
        ],
        "note": ("Simplified: a CURRENT stock-value snapshot at report "
                 "time, not the dashboard's modelled month-average "
                 "walk-back figure."),
    }

    # ---- 5. Revenue by Channel + 9. Order Counts ---------------------
    if {"SourceChannel"}.issubset(sl_month.columns) or \
            "SalesRepresentative" in sl_month.columns:
        chan = prod.apply(
            lambda r: _channel_of_row(
                r.get("SourceChannel"), r.get("SalesRepresentative")),
            axis=1)
    else:
        chan = pd.Series("B2B / Direct", index=prod.index)
    chan_rev = prod.groupby(chan)["Total"].apply(
        lambda s: float(_num(prod.loc[s.index], "Total").sum()))
    chan_orders = prod.assign(_chan=chan).groupby("_chan")["SaleID"].nunique() \
        if "SaleID" in prod.columns else pd.Series(dtype=int)
    for c in ("Shopify", "Amazon", "eBay", "B2B / Direct"):
        chan_rev.setdefault(c, 0.0) if hasattr(chan_rev, "setdefault") else None
    chan_rev = {c: float(chan_rev.get(c, 0.0)) for c in
                ("Shopify", "Amazon", "eBay", "B2B / Direct")}
    chan_ord = {c: int(chan_orders.get(c, 0)) for c in
                ("Shopify", "Amazon", "eBay", "B2B / Direct")}
    out["5. Revenue by Channel [Cin7/DEAR]"] = {
        "pie": chan_rev,
        "table": [(k, f"${v:,.0f}") for k, v in chan_rev.items()],
    }
    out["9. Order Counts [Cin7/DEAR]"] = {
        "pie": chan_ord,
        "table": [(k, f"{v:,}") for k, v in chan_ord.items()],
    }

    # ---- 6/7/8. QuickBooks-sourced sections --------------------------
    mappings = db.get_qbo_account_mappings()
    qb_by_month = db.qbo_monthly_pl_summary_by_category(mappings)
    qb = (qb_by_month.get(month) or {})

    total_income = qb.get("total_income", 0.0)
    net_sales = qb.get("sales", 0.0)
    shipping_income = qb.get("shipping_charged", 0.0)
    sundry = max(total_income - net_sales - shipping_income, 0.0)
    out["6. Sales & Adjustments [QuickBooks]"] = {
        "pie": {"Net Sales": max(net_sales, 0), "Shipping Income":
                max(shipping_income, 0), "Sundry Income": sundry},
        "table": [
            ("Net Sales (QB 400)", f"${net_sales:,.0f}"),
            ("Shipping Income (QB 405)", f"${shipping_income:,.0f}"),
            ("Total Revenue (QB Total Income)", f"${total_income:,.0f}"),
        ],
    }

    prod_cogs = qb.get("cogs", 0.0)
    amz_fees = qb.get("cogs_amazon_fees", 0.0)
    inv_adj = qb.get("inventory_adjustment", 0.0)
    total_cogs = qb.get("total_cogs", 0.0)
    qb_gp = qb.get("qb_gross_profit", 0.0)
    out["7. Cost & Profitability [QuickBooks]"] = {
        "pie": {"Product COGS": max(prod_cogs, 0),
                "Amazon Fees": max(amz_fees, 0),
                "Inventory Adj": max(inv_adj, 0)},
        "table": [
            ("Product COGS (QB 500)", f"${prod_cogs:,.0f}"),
            ("Amazon Fees (QB 502)", f"${amz_fees:,.0f}"),
            ("Inventory Adj (QB 550)", f"${inv_adj:,.0f}"),
            ("Total COGS", f"${total_cogs:,.0f}"),
            ("Gross Profit (QB)", f"${qb_gp:,.0f}"),
        ],
    }

    ship_charged_qb = qb.get("shipping_charged", 0.0)
    ship_cost_qb = qb.get("shipping_cost", 0.0)
    ship_margin_qb = ship_charged_qb - ship_cost_qb
    out["8. Shipping Detail [QuickBooks]"] = {
        # A pie only makes sense when cost fits inside what was charged
        # (margin >= 0) -- clamping a negative margin to 0 would show
        # a misleading "100% cost" slice. This is common here: our own
        # audit found QB shipping margin negative in most months.
        "pie": ({"Shipping-Out Cost": ship_cost_qb,
                 "Shipping Margin": ship_margin_qb}
                if ship_margin_qb >= 0 else {}),
        "skip_pie": ship_margin_qb < 0,
        "table": [
            ("Shipping Charged (QB 405)", f"${ship_charged_qb:,.0f}"),
            ("Shipping-Out Cost (QB 694)", f"${ship_cost_qb:,.0f}"),
            ("Shipping Margin", f"${ship_margin_qb:,.0f}"),
        ],
        "note": (
            "Shipping cost exceeded what was charged this month, so a "
            "cost/margin split can't be shown as a pie — see the "
            "figures above."
            if ship_margin_qb < 0 else None),
    }

    return out


# ---------------------------------------------------------------------------
# Chart generation
# ---------------------------------------------------------------------------
_PIE_COLORS = ["#2f6fed", "#e8833a", "#3aa76d", "#c94f4f", "#8e6fce"]


def _render_pie(pie: Dict[str, float], title: str) -> Optional[bytes]:
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

    fig, ax = plt.subplots(figsize=(3.0, 3.4), dpi=150)
    total = sum(values)
    wedges, _ = ax.pie(
        values, startangle=90, colors=_PIE_COLORS[:len(values)])
    legend_labels = [f"{lbl} ({v / total * 100:.0f}%)"
                      for lbl, v in zip(labels, values)]
    ax.legend(
        wedges, legend_labels, loc="upper center",
        bbox_to_anchor=(0.5, -0.02), ncol=1, frameon=False,
        fontsize=7, handlelength=1.0, handletextpad=0.5,
        labelspacing=0.3,
    )
    ax.set_title(title, fontsize=9)
    ax.axis("equal")
    buf = io.BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight", transparent=True)
    plt.close(fig)
    buf.seek(0)
    return buf.getvalue()


# ---------------------------------------------------------------------------
# PDF assembly
# ---------------------------------------------------------------------------
_SECTION_ORDER = [
    "1. Sales Overview [App]",
    "2. Margins & Purchasing [App]",
    "3. Customer Metrics [App]",
    "4. Inventory [App]",
    "5. Revenue by Channel [Cin7/DEAR]",
    "6. Sales & Adjustments [QuickBooks]",
    "7. Cost & Profitability [QuickBooks]",
    "8. Shipping Detail [QuickBooks]",
    "9. Order Counts [Cin7/DEAR]",
]

# Same muted, printer-friendly palette as po_pdf.py
_C_HEAD = "#1f2933"
_C_SUB = "#52606d"
_C_BORDER = "#c3ccd8"
_C_ZEBRA = "#f3f5f8"


def build_pdf(sections: Dict[str, Any], month: str,
               company: str = "Wired4Signs USA") -> bytes:
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import letter
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.units import inch
    from reportlab.platypus import (
        SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, Image,
    )

    c_head = colors.HexColor(_C_HEAD)
    c_sub = colors.HexColor(_C_SUB)
    c_border = colors.HexColor(_C_BORDER)
    c_zebra = colors.HexColor(_C_ZEBRA)

    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf, pagesize=letter,
        leftMargin=0.55 * inch, rightMargin=0.55 * inch,
        topMargin=0.55 * inch, bottomMargin=0.5 * inch,
        title=f"Monthly Metrics — {month}",
        author=company,
    )
    styles = getSampleStyleSheet()
    title_style = ParagraphStyle(
        "TitleW4S", parent=styles["Title"], fontSize=18, leading=22,
        textColor=c_head, spaceAfter=2)
    sub_style = ParagraphStyle(
        "SubW4S", parent=styles["Normal"], fontSize=9.5, leading=12,
        textColor=c_sub)
    section_style = ParagraphStyle(
        "SectionW4S", parent=styles["Heading3"], fontSize=10.5,
        leading=13, textColor=c_head, spaceBefore=2, spaceAfter=3)
    note_style = ParagraphStyle(
        "NoteW4S", parent=styles["Normal"], fontSize=7, leading=9,
        textColor=c_sub)

    story: List = []
    story.append(Paragraph(
        f"<b>{company}</b> — Monthly Metrics Executive Summary",
        title_style))
    story.append(Paragraph(
        f"<b>Reporting month:</b> {month} &nbsp;·&nbsp; "
        f"<b>Generated:</b> {datetime.now():%Y-%m-%d %H:%M}",
        sub_style))
    story.append(Spacer(1, 10))

    def _block(section: str, payload: Dict[str, Any]):
        cell_story: List = []
        cell_story.append(Paragraph(section, section_style))
        png = None
        if not payload.get("skip_pie"):
            png = _render_pie(payload.get("pie") or {}, "")
        if png:
            # Matches _render_pie's figsize aspect ratio (3.0 x 3.4,
            # pie + legend below it) so the image isn't stretched.
            img = Image(io.BytesIO(png), width=1.6 * inch,
                        height=1.6 * inch * (3.4 / 3.0))
            cell_story.append(img)
        rows = payload.get("table") or []
        if rows:
            t = Table(rows, colWidths=[1.55 * inch, 0.95 * inch])
            t.setStyle(TableStyle([
                ("FONTSIZE", (0, 0), (-1, -1), 7.5),
                ("ALIGN", (1, 0), (1, -1), "RIGHT"),
                ("TEXTCOLOR", (0, 0), (-1, -1), c_head),
                ("LINEBELOW", (0, 0), (-1, -2), 0.25, c_zebra),
                ("TOPPADDING", (0, 0), (-1, -1), 1.5),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 1.5),
            ]))
            cell_story.append(t)
        if payload.get("note"):
            cell_story.append(Spacer(1, 2))
            cell_story.append(Paragraph(f"<i>{payload['note']}</i>",
                                          note_style))
        return cell_story

    # 3-column grid of section blocks. Each row is its own small Table
    # flowable (a handful of small blocks, well under a page), so rows
    # naturally don't split — no KeepTogether needed. (KeepTogether
    # nested inside a Table cell is a known-problematic reportlab
    # combination: it can report an unbounded/garbage height and blow
    # up doc.build with a LayoutError.)
    ordered = [(s, sections[s]) for s in _SECTION_ORDER if s in sections]
    ordered += [(s, v) for s, v in sections.items()
                if s not in _SECTION_ORDER]
    col_w = 2.35 * inch
    for i in range(0, len(ordered), 3):
        row_sections = ordered[i:i + 3]
        row_cells = [_block(s, p) for s, p in row_sections]
        while len(row_cells) < 3:
            row_cells.append("")
        grid = Table([row_cells], colWidths=[col_w] * 3)
        grid.setStyle(TableStyle([
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("LEFTPADDING", (0, 0), (-1, -1), 4),
            ("RIGHTPADDING", (0, 0), (-1, -1), 8),
        ]))
        story.append(grid)
        story.append(Spacer(1, 6))

    def _footer(canvas, doc_):
        canvas.saveState()
        canvas.setFont("Helvetica", 7.5)
        canvas.setFillColor(c_sub)
        page_w, _ = letter
        canvas.drawString(
            0.55 * inch, 0.3 * inch,
            f"{company} · Monthly Metrics — {month} · "
            f"Generated {datetime.now():%Y-%m-%d %H:%M}")
        canvas.drawRightString(page_w - 0.55 * inch, 0.3 * inch,
                                f"Page {doc_.page}")
        canvas.restoreState()

    doc.build(story, onFirstPage=_footer, onLaterPages=_footer)
    return buf.getvalue()


# ---------------------------------------------------------------------------
# Slack delivery
# ---------------------------------------------------------------------------
def post_pdf_to_slack(pdf_bytes: bytes, month: str) -> Tuple[bool, str]:
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
    # with a short message.
    complete_body = slack_sync._slack_post(
        session, "files.completeUploadExternal", {
            "files": [{"id": file_id, "title": f"Monthly Metrics {month}"}],
            "channel_id": channel,
            "initial_comment": (
                f"📊 Monthly Metrics executive summary for *{month}* is "
                f"ready — see the attached PDF."),
        })
    if not complete_body.get("ok"):
        return False, f"files.completeUploadExternal failed: {complete_body}"
    return True, f"posted to channel {channel}"


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main() -> int:
    month = _target_month()
    _emit(f"building report for {month}")

    try:
        cin7_data = _load_cin7_data()
    except Exception as exc:  # noqa: BLE001
        _emit(f"CIN7 data load failed: {exc!r}", level="error")
        return 2

    try:
        sections = compute_sections(cin7_data, month)
    except Exception as exc:  # noqa: BLE001
        _emit(f"section computation failed: {exc!r}", level="error")
        return 3

    try:
        pdf_bytes = build_pdf(sections, month)
    except Exception as exc:  # noqa: BLE001
        _emit(f"PDF build failed: {exc!r}", level="error")
        return 4

    from data_paths import OUTPUT_DIR
    out_path = OUTPUT_DIR / f"monthly_metrics_{month}.pdf"
    out_path.write_bytes(pdf_bytes)
    _emit(f"wrote {out_path} ({len(pdf_bytes):,} bytes)")

    ok, msg = post_pdf_to_slack(pdf_bytes, month)
    if ok:
        _emit(f"Slack: {msg}")
    else:
        _emit(f"Slack: {msg}", level="warn")
    return 0


if __name__ == "__main__":
    sys.exit(main())
