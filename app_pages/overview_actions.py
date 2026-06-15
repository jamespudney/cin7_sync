"""Overview command-center helpers."""

from __future__ import annotations

from datetime import datetime

import streamlit as st


def render_attention_queue(
    *,
    freshness: tuple,
    purchase_lines,
    db_module,
    fmt_number,
    fmt_money,
    to_num,
) -> None:
    """Render a lightweight daily action strip for the Overview page.

    This intentionally avoids calling the ABC engine. The Overview should
    stay quick even when the engine cache is cold.
    """
    latest_mtime, minutes_ago, source_name = freshness

    open_pos = 0
    open_po_value = 0.0
    if not purchase_lines.empty and "Status" in purchase_lines.columns:
        open_mask = purchase_lines["Status"].astype(str).str.upper().isin(
            ("ORDERED", "ORDERING")
        )
        open_pos = purchase_lines.loc[open_mask, "PurchaseID"].nunique()
        open_po_value = float(to_num(purchase_lines.loc[open_mask, "Total"]).sum())

    try:
        stock_issues = db_module.list_open_stock_issues(limit=500)
        open_stock_issues = len(stock_issues)
    except Exception:  # noqa: BLE001
        open_stock_issues = None

    try:
        slow_warnings = db_module.get_dormancy_warnings()
        slow_count = len(slow_warnings)
    except Exception:  # noqa: BLE001
        slow_count = None

    st.subheader("Today needs attention")
    c1, c2, c3, c4 = st.columns(4)

    if latest_mtime is None:
        c1.metric("Data heartbeat", "missing")
        c1.caption("No stock_on_hand snapshot found.")
    else:
        if minutes_ago < 60:
            age = f"{minutes_ago:.0f}m ago"
        elif minutes_ago < 1440:
            age = f"{minutes_ago / 60:.1f}h ago"
        else:
            age = f"{minutes_ago / 1440:.1f}d ago"
        c1.metric("Data heartbeat", age)
        c1.caption(source_name or latest_mtime.strftime("%Y-%m-%d %H:%M"))

    c2.metric(
        "Open POs",
        fmt_number(open_pos),
        delta=fmt_money(open_po_value) if open_po_value else None,
        delta_color="off",
    )

    c3.metric(
        "Open stock issues",
        "—" if open_stock_issues is None else fmt_number(open_stock_issues),
    )

    c4.metric(
        "Slow-mover warnings",
        "—" if slow_count is None else fmt_number(slow_count),
    )

    st.caption(
        "Start here for the daily pulse: data freshness, open buying work, "
        "stock exceptions, and slow-moving inventory. The detailed pages "
        "remain the source of truth for each workflow."
    )

