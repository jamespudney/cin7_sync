"""Purchase Analysis page."""

from __future__ import annotations

from datetime import datetime

import pandas as pd
import streamlit as st


def render_purchase_analysis(
    *,
    purchase_lines,
    to_num,
    to_date,
    fmt_number,
    fmt_money,
    rows_selector,
) -> None:
    st.header(":truck: Purchase Analysis")

    if purchase_lines.empty:
        st.warning(
            "No purchase line data. Run `python cin7_sync.py "
            "purchaselines --days 90`."
        )
        return

    df = purchase_lines.copy()
    df["Total"] = to_num(df["Total"]).fillna(0)
    df["Quantity"] = to_num(df["Quantity"]).fillna(0)
    df["OrderDate"] = to_date(df["OrderDate"]).dt.tz_localize(None)

    col_w, _ = st.columns([1, 3])
    with col_w:
        window_days = st.selectbox(
            "Window",
            [90, 30, 7, 365, 1825],
            index=0,
            key="pa_window_days",
            format_func=lambda d: (
                f"Last {d} days" if d <= 365 else "Last 5 years (all)"
            ),
        )

    cutoff = pd.Timestamp(datetime.now().date()) - pd.Timedelta(
        days=int(window_days)
    )
    df = df[df["OrderDate"] >= cutoff]

    n_before_status = len(df)
    if "Status" in df.columns:
        bad_statuses = ("VOIDED", "CANCELLED", "CANCELED", "DRAFT")
        df = df[~df["Status"].astype(str).str.upper().isin(bad_statuses)]
    n_dropped_status = n_before_status - len(df)

    st.caption(
        f"Showing purchase lines for the last {window_days} days · "
        f"excludes voided/cancelled/draft ({n_dropped_status:,} dropped). "
        "Window selector above changes the period."
    )

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Line items", fmt_number(len(df)))
    c2.metric("Distinct POs", fmt_number(df["PurchaseID"].nunique()))
    c3.metric("Distinct SKUs", fmt_number(df["SKU"].nunique()))
    c4.metric("Total value", fmt_money(df["Total"].sum()))

    tab_sup, tab_sku, tab_po = st.tabs(
        ["By supplier", "Top SKUs", "Recent POs"]
    )

    with tab_sup:
        by_sup = (
            df.groupby("Supplier", dropna=False)
            .agg(
                POs=("PurchaseID", "nunique"),
                Lines=("SKU", "count"),
                SKUs=("SKU", "nunique"),
                Value=("Total", "sum"),
            )
            .sort_values("Value", ascending=False)
        )
        limit = rows_selector(key="pa_sup_rows")
        st.caption(
            f"Showing {min(limit, len(by_sup)):,} of "
            f"{len(by_sup):,} suppliers"
        )
        st.dataframe(by_sup.head(limit), width="stretch")

    with tab_sku:
        by_sku = (
            df.groupby(["SKU", "Name"], dropna=False)
            .agg(
                Qty=("Quantity", "sum"),
                Value=("Total", "sum"),
                POs=("PurchaseID", "nunique"),
            )
            .sort_values("Value", ascending=False)
        )
        limit = rows_selector(key="pa_sku_rows")
        st.caption(
            f"Showing {min(limit, len(by_sku)):,} of {len(by_sku):,} SKUs"
        )
        st.dataframe(by_sku.head(limit), width="stretch")

    with tab_po:
        po_summary = (
            df.groupby(
                ["PurchaseID", "OrderNumber", "OrderDate", "Supplier", "Status"],
                dropna=False,
            )
            .agg(Lines=("SKU", "count"), Value=("Total", "sum"))
            .reset_index()
            .sort_values("OrderDate", ascending=False)
        )
        limit = rows_selector(key="pa_po_rows")
        st.caption(
            f"Showing {min(limit, len(po_summary)):,} of "
            f"{len(po_summary):,} POs"
        )
        st.dataframe(po_summary.head(limit), width="stretch", height=560)

