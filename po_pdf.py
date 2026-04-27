"""
po_pdf.py — build a buyer-friendly PDF of a draft purchase order.

Designed so the person opening the PDF doesn't need context: the cover
page explains what the numbers mean, the line table is colour-coded for
urgency, and the footer shows provenance. Pure Python (reportlab),
no system binaries.

Usage:
    from po_pdf import build_po_pdf
    pdf_bytes = build_po_pdf(supplier, po_lines_df, summary, meta)
    st.download_button("Download PO", pdf_bytes, "PO_<supplier>.pdf",
                       mime="application/pdf")
"""
from __future__ import annotations

import io
from datetime import datetime
from typing import Dict, Any, List

import pandas as pd

from reportlab.lib import colors
from reportlab.lib.pagesizes import letter, landscape
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
    PageBreak, KeepTogether,
)


# ---------------------------------------------------------------------------
# Colour palette — muted, printer-friendly, legible
# ---------------------------------------------------------------------------
C_HEAD = colors.HexColor("#1f2933")      # header dark
C_SUB  = colors.HexColor("#52606d")      # subheader gray
C_OK   = colors.HexColor("#0b7a3b")      # normal / positive
C_WARN = colors.HexColor("#b0570f")      # ageing / attention
C_BAD  = colors.HexColor("#b23838")      # backorder / stockout
C_DS   = colors.HexColor("#6b4fb5")      # dropship
C_ZEBRA = colors.HexColor("#f3f5f8")      # row alternation
C_BORDER = colors.HexColor("#c3ccd8")
C_LINE = colors.HexColor("#e4e9f0")


def _status_color(row: pd.Series) -> colors.Color:
    """Pick a color per row based on its status / position."""
    status = str(row.get("Status") or "").lower()
    if "dropship" in status:
        return C_DS
    unfulfilled = float(row.get("unfulfilled") or 0)
    onhand = float(row.get("OnHand") or 0)
    if unfulfilled > 0 and onhand == 0:
        return C_BAD
    if onhand == 0:
        return C_WARN
    return C_OK


def _fmt_money(v, zero_dash: bool = False) -> str:
    try:
        v = float(v)
    except (ValueError, TypeError):
        return "—"
    if zero_dash and v == 0:
        return "—"
    return f"${v:,.2f}"


def _fmt_qty(v) -> str:
    try:
        v = float(v)
    except (ValueError, TypeError):
        return "—"
    return f"{v:,.0f}"


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def build_po_pdf(
    supplier: str,
    po_lines: pd.DataFrame,
    summary: Dict[str, Any],
    meta: Dict[str, Any],
) -> bytes:
    """Render a draft PO to a PDF and return the bytes.

    Parameters
    ----------
    supplier : str
        Vendor name to print on the cover.
    po_lines : pd.DataFrame
        Rows to include. Expected columns (missing columns are tolerated):
          SKU, Name, ABC, Status, OnHand, OnOrder, Available, unfulfilled,
          units_12mo, target_stock, reorder_qty, Order qty, POCost,
          POCostBasis, Line value, freight_mode, lead_time_days, Note.
    summary : dict
        Aggregate metrics. Keys used:
          lines, units, value, mov_amount, mov_currency, mov_met,
          class_mix (dict e.g. {'A': 3, 'B': 2}).
    meta : dict
        Provenance + methodology metadata. Keys used:
          author, generated_at, freight_mode, lead_time,
          company_name, notes.
    """
    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf,
        pagesize=landscape(letter),
        leftMargin=0.5 * inch, rightMargin=0.5 * inch,
        topMargin=0.5 * inch, bottomMargin=0.5 * inch,
        title=f"Draft PO — {supplier}",
        author=meta.get("author", "Wired4Signs USA, LLC"),
    )
    styles = getSampleStyleSheet()

    # Custom styles
    title_style = ParagraphStyle(
        "TitleW4S", parent=styles["Title"],
        fontSize=20, leading=24, textColor=C_HEAD,
        spaceAfter=4,
    )
    sub_style = ParagraphStyle(
        "SubW4S", parent=styles["Normal"],
        fontSize=10, leading=13, textColor=C_SUB,
    )
    section_style = ParagraphStyle(
        "SectionW4S", parent=styles["Heading2"],
        fontSize=12, leading=15, textColor=C_HEAD,
        spaceBefore=8, spaceAfter=4,
    )
    body_style = ParagraphStyle(
        "BodyW4S", parent=styles["Normal"],
        fontSize=9, leading=12,
    )

    story: List = []

    # ---- Header block ------------------------------------------------
    company = meta.get("company_name", "Wired4Signs USA, LLC")
    now_str = meta.get("generated_at", datetime.now()).strftime(
        "%Y-%m-%d %H:%M"
    )
    story.append(Paragraph(f"<b>{company}</b> — Draft Purchase Order",
                             title_style))
    story.append(Paragraph(
        f"<b>Vendor:</b> {supplier} &nbsp; · &nbsp; "
        f"<b>Generated:</b> {now_str} &nbsp; · &nbsp; "
        f"<b>Prepared by:</b> {meta.get('author', '—')}",
        sub_style,
    ))
    if meta.get("freight_mode") or meta.get("lead_time"):
        fm = meta.get("freight_mode", "—")
        lt = meta.get("lead_time", "—")
        story.append(Paragraph(
            f"<b>Freight:</b> {fm} &nbsp; · &nbsp; "
            f"<b>Lead time:</b> {lt} &nbsp; · &nbsp; "
            f"<b>Currency:</b> {meta.get('currency', 'USD')}",
            sub_style,
        ))
    story.append(Spacer(1, 8))

    # ---- Summary band ------------------------------------------------
    mov_amt = summary.get("mov_amount") or 0
    mov_ccy = summary.get("mov_currency") or "USD"
    mov_met = summary.get("mov_met")
    mix = summary.get("class_mix") or {}
    mix_s = (f"A:{mix.get('A', 0)} &nbsp; B:{mix.get('B', 0)} "
             f"&nbsp; C:{mix.get('C', 0)}")
    summary_rows = [
        ["Lines", "Total units", "PO value",
         f"MOV ({mov_ccy} ${mov_amt:,.0f})", "Class mix"],
        [
            _fmt_qty(summary.get("lines")),
            _fmt_qty(summary.get("units")),
            _fmt_money(summary.get("value")),
            ("✓ above MOV" if mov_met else
             (f"✗ ${summary.get('value',0):,.0f} vs ${mov_amt:,.0f}"
              if mov_amt else "— not set")),
            mix_s.replace("&nbsp;", " "),
        ],
    ]
    summary_table = Table(
        summary_rows,
        colWidths=[1.2*inch]*5,
        hAlign="LEFT",
    )
    summary_table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), C_HEAD),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.whitesmoke),
        ("FONTSIZE", (0, 0), (-1, -1), 9),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("ALIGN", (0, 0), (-1, -1), "CENTER"),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("BOX", (0, 0), (-1, -1), 0.5, C_BORDER),
        ("INNERGRID", (0, 0), (-1, -1), 0.25, C_BORDER),
        ("TEXTCOLOR", (3, 1), (3, 1),
         C_OK if mov_met else (C_WARN if mov_amt else C_SUB)),
        ("FONTNAME", (3, 1), (3, 1), "Helvetica-Bold"),
    ]))
    story.append(summary_table)
    story.append(Spacer(1, 10))

    # ---- Methodology block ------------------------------------------
    method = (
        "<b>How to read this PO.</b> Each line is a suggested quantity "
        "computed from the SKU's last-12-months sales velocity, current "
        "on-hand, stock already on order (open POs with status ORDERED/"
        "ORDERING), and any unfulfilled customer backorders. Target stock "
        "covers the supplier's lead time plus a review-period buffer "
        "(sized by the SKU's ABC class: A=14d, B=30d, C=45d). VOIDED, "
        "CREDITED and CANCELLED sales are excluded from demand. The "
        "<b>legend below the table</b> explains the colour markers next "
        "to each SKU."
    )
    story.append(Paragraph(method, body_style))
    story.append(Spacer(1, 8))

    # ---- Line items table -------------------------------------------
    # Column set, tuned for buyer readability in landscape letter:
    #   # | ● | SKU | Name | Trend | 45d | Cust | Top% |
    #   OnHand | OnOrder | Last 6 months | Suggest | Order | Unit $ | Line $
    #
    # Dropped vs. older version:
    #   • Class (buyer rarely acts on it; trend flag is more actionable)
    #   • Status text (redundant with coloured bullet + Trend)
    #   • Unfilled (calc_trace shows when it matters)
    #   • Target (gap to Suggest is what the buyer acts on)
    #   • Freight (in header; rarely per-row relevant)
    #   • 12mo units (redundant with Last 6 months visual)
    # Added:
    #   • 45d units  — recent velocity at a glance
    #   • Cust        — distinct customers in last 45d
    #   • Top%        — top customer's share (concentration check)
    cols = [
        "#", "", "SKU", "Product name", "Trend",
        "45d", "Cust", "Top%",
        "OnHand", "OnOrder",
        "Last 6 months",
        "Suggest", "Order", "Unit $", "Line $",
    ]
    table_rows: List[List[Any]] = [cols]
    marker_rows: List[int] = []
    for idx, (_, r) in enumerate(po_lines.iterrows(), start=1):
        color = _status_color(r)
        order_qty = (int(r.get("Order qty") or 0)
                      if pd.notna(r.get("Order qty")) else 0)
        line_val = (float(r.get("Line value") or 0)
                      if pd.notna(r.get("Line value")) else 0.0)
        name_txt = str(r.get("Name") or "")[:38]
        # Trend column: prefer the flag; show just the emoji if set
        tf = str(r.get("trend_flag") or "Stable")
        trend_short = {
            "📈 Trend": "📈",
            "🎯 Project": "🎯",
            "🔀 Mixed": "🔀",
            "📉 Decline": "📉",
            "Stable": "—",
        }.get(tf, "—")
        # 45d / customers / top% — NaN-safe
        def _sn(v, default=0):
            try:
                v = float(v)
                return default if pd.isna(v) else v
            except (ValueError, TypeError):
                return default
        u45 = _sn(r.get("units_45d"))
        n_cust = int(_sn(r.get("customers_45d")))
        top_pct = _sn(r.get("top_cust_pct")) * 100
        # Last 6 months as an inline sequence
        last6 = str(r.get("last_6mo_series") or "")
        if not last6:
            t12 = r.get("trend_12m")
            if isinstance(t12, list) and t12:
                last6 = "  ".join(f"{int(round(v))}" for v in t12[-6:])
        table_rows.append([
            str(idx),
            "●",    # marker colored per-row via style below
            str(r.get("SKU") or "—"),
            name_txt,
            trend_short,
            f"{u45:,.0f}" if u45 else "—",
            str(n_cust) if n_cust else "—",
            f"{top_pct:.0f}%" if top_pct else "—",
            _fmt_qty(r.get("OnHand")),
            _fmt_qty(r.get("OnOrder")),
            last6 or "—",
            _fmt_qty(r.get("reorder_qty")),
            _fmt_qty(order_qty),
            _fmt_money(r.get("POCost")),
            _fmt_money(line_val, zero_dash=True),
        ])
        marker_rows.append((idx, color))

    col_widths = [
        0.3*inch, 0.15*inch, 1.1*inch, 2.2*inch,
        0.35*inch,                    # Trend
        0.4*inch, 0.35*inch, 0.4*inch,  # 45d, Cust, Top%
        0.5*inch, 0.5*inch,           # OnHand, OnOrder
        1.4*inch,                     # Last 6 months
        0.5*inch, 0.45*inch,          # Suggest, Order
        0.55*inch, 0.7*inch,          # Unit $, Line $
    ]
    tbl = Table(table_rows, colWidths=col_widths, repeatRows=1)
    ts = TableStyle([
        # Header
        ("BACKGROUND", (0, 0), (-1, 0), C_HEAD),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.whitesmoke),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 7.5),
        ("ALIGN", (0, 0), (-1, 0), "CENTER"),
        # Body
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("GRID", (0, 0), (-1, -1), 0.25, C_LINE),
        ("ALIGN", (2, 1), (2, -1), "LEFT"),       # SKU
        ("ALIGN", (3, 1), (3, -1), "LEFT"),       # Name
        ("ALIGN", (4, 1), (4, -1), "CENTER"),     # Trend emoji
        ("ALIGN", (5, 1), (7, -1), "RIGHT"),       # 45d, Cust, Top%
        ("ALIGN", (8, 1), (9, -1), "RIGHT"),       # OnHand, OnOrder
        ("ALIGN", (10, 1), (10, -1), "LEFT"),      # Last 6 months
        ("ALIGN", (11, 1), (14, -1), "RIGHT"),     # Suggest/Order/Unit$/Line$
        # Zebra stripes
    ])
    for i in range(1, len(table_rows)):
        if i % 2 == 0:
            ts.add("BACKGROUND", (0, i), (-1, i), C_ZEBRA)
    # Per-row marker colour
    for i, color in marker_rows:
        ts.add("TEXTCOLOR", (1, i), (1, i), color)
        ts.add("FONTNAME", (1, i), (1, i), "Helvetica-Bold")
        ts.add("FONTSIZE", (1, i), (1, i), 10)
    tbl.setStyle(ts)
    story.append(tbl)
    story.append(Spacer(1, 8))

    # ---- Legend -----------------------------------------------------
    legend_rows = [
        ["●", "Stocked — normal reorder", "●", "No stock — critical "
         "(re-order now)"],
        ["●", "No stock + backorders — highest priority", "●",
         "Dropship — review before including"],
    ]
    leg = Table(legend_rows, colWidths=[0.15*inch, 3.0*inch,
                                           0.15*inch, 3.5*inch])
    leg.setStyle(TableStyle([
        ("FONTSIZE", (0, 0), (-1, -1), 8),
        ("TEXTCOLOR", (0, 0), (0, 0), C_OK),
        ("TEXTCOLOR", (2, 0), (2, 0), C_WARN),
        ("TEXTCOLOR", (0, 1), (0, 1), C_BAD),
        ("TEXTCOLOR", (2, 1), (2, 1), C_DS),
        ("FONTNAME", (0, 0), (0, 0), "Helvetica-Bold"),
        ("FONTNAME", (2, 0), (2, 0), "Helvetica-Bold"),
        ("FONTNAME", (0, 1), (0, 1), "Helvetica-Bold"),
        ("FONTNAME", (2, 1), (2, 1), "Helvetica-Bold"),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("TEXTCOLOR", (1, 0), (1, 0), C_SUB),
        ("TEXTCOLOR", (3, 0), (3, 0), C_SUB),
        ("TEXTCOLOR", (1, 1), (1, 1), C_SUB),
        ("TEXTCOLOR", (3, 1), (3, 1), C_SUB),
    ]))
    story.append(leg)

    # ---- Optional notes --------------------------------------------
    if meta.get("notes"):
        story.append(Spacer(1, 10))
        story.append(Paragraph("<b>Buyer notes</b>", section_style))
        story.append(Paragraph(meta["notes"], body_style))

    # ---- Footer (on every page via onPage) -------------------------
    def _footer(canvas, doc_):
        canvas.saveState()
        canvas.setFont("Helvetica", 7.5)
        canvas.setFillColor(C_SUB)
        page_w, _ = landscape(letter)
        canvas.drawString(
            0.5 * inch, 0.3 * inch,
            f"Wired4Signs USA, LLC  ·  Draft PO — {supplier}  ·  "
            f"Generated {now_str} by {meta.get('author', '—')}"
        )
        canvas.drawRightString(
            page_w - 0.5 * inch, 0.3 * inch,
            f"Page {doc_.page}"
        )
        canvas.restoreState()

    doc.build(story, onFirstPage=_footer, onLaterPages=_footer)
    return buf.getvalue()
