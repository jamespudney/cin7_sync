"""Recent Sales page."""

from __future__ import annotations

from datetime import datetime

import pandas as pd
import streamlit as st


def render_sales_recent(
    *,
    sale_lines,
    to_num,
    to_date,
    fmt_number,
    fmt_money,
    rows_selector,
) -> None:
    st.header(":moneybag: Recent Sales")

    if sale_lines.empty:
        st.warning(
            "No sale line data. Run `python cin7_sync.py "
            "salelines --days 30`."
        )
        return

    df = sale_lines.copy()
    df["Total"] = to_num(df["Total"]).fillna(0)
    df["Quantity"] = to_num(df["Quantity"]).fillna(0)
    df["InvoiceDate"] = to_date(df["InvoiceDate"]).dt.tz_localize(None)

    col_w, _ = st.columns([1, 3])
    with col_w:
        window_days = st.selectbox(
            "Window",
            [30, 7, 90, 365, 1825],
            index=0,
            key="sr_window_days",
            format_func=lambda d: (
                f"Last {d} days" if d <= 365 else "Last 5 years (all)"
            ),
        )

    cutoff = pd.Timestamp(datetime.now().date()) - pd.Timedelta(
        days=int(window_days)
    )
    df = df[df["InvoiceDate"] >= cutoff]

    n_before_status = len(df)
    if "Status" in df.columns:
        bad_statuses = ("VOIDED", "CREDITED", "CANCELLED", "CANCELED")
        df = df[~df["Status"].astype(str).str.upper().isin(bad_statuses)]
    n_dropped_status = n_before_status - len(df)

    ship_sku_pats = (
        "SHIPPING",
        "FREIGHT",
        "DELIVERY",
        "POSTAGE",
        "COURIER",
        "HANDLING",
    )
    sku_str = df["SKU"].fillna("").astype(str).str.upper()
    name_str = df["Name"].fillna("").astype(str).str.upper()
    is_ship = pd.Series(False, index=df.index)
    for pat in ship_sku_pats:
        is_ship = is_ship | sku_str.str.startswith(pat)
        is_ship = is_ship | name_str.str.contains(pat, regex=False, na=False)
    n_ship = int(is_ship.sum())
    df = df[~is_ship]

    st.caption(
        f"Showing **product** sale lines for the last {window_days} days · "
        f"excludes voided/credited/cancelled ({n_dropped_status:,} dropped) · "
        f"excludes shipping/freight line items ({n_ship:,} dropped). "
        "These filters match the Monthly Metrics sales row so the figures correlate."
    )

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Line items", fmt_number(len(df)))
    c2.metric("Distinct sales", fmt_number(df["SaleID"].nunique()))
    c3.metric("Distinct SKUs", fmt_number(df["SKU"].nunique()))
    c4.metric("Product revenue", fmt_money(df["Total"].sum()))

    if "SaleType" in df.columns:
        types = sorted(df["SaleType"].dropna().unique().tolist())
        sel_types = st.multiselect("Sale type", types, default=types)
        if sel_types:
            df = df[df["SaleType"].isin(sel_types)]

    tab_sku, tab_cust, tab_lines = st.tabs(
        ["Top SKUs", "Top customers", "Recent lines"]
    )

    with tab_sku:
        by_sku = (
            df.groupby(["SKU", "Name"], dropna=False)
            .agg(
                Qty=("Quantity", "sum"),
                Revenue=("Total", "sum"),
                Orders=("SaleID", "nunique"),
            )
            .sort_values("Revenue", ascending=False)
        )
        limit = rows_selector(key="sr_sku_rows")
        st.caption(
            f"Showing {min(limit, len(by_sku)):,} of {len(by_sku):,} SKUs"
        )
        st.dataframe(by_sku.head(limit), width="stretch")

    with tab_cust:
        by_cust = (
            df.groupby(["CustomerID", "Customer"], dropna=False)
            .agg(
                Orders=("SaleID", "nunique"),
                Lines=("SKU", "count"),
                Revenue=("Total", "sum"),
            )
            .sort_values("Revenue", ascending=False)
        )
        limit = rows_selector(key="sr_cust_rows")
        st.caption(
            f"Showing {min(limit, len(by_cust)):,} of "
            f"{len(by_cust):,} customers"
        )
        st.dataframe(by_cust.head(limit), width="stretch")

    with tab_lines:
        recent = df.sort_values("InvoiceDate", ascending=False)[
            [
                "InvoiceDate",
                "OrderNumber",
                "Customer",
                "SKU",
                "Name",
                "Quantity",
                "Price",
                "Total",
                "Status",
            ]
        ]
        limit = rows_selector(key="sr_lines_rows")
        st.caption(
            f"Showing {min(limit, len(recent)):,} of {len(recent):,} lines"
        )
        st.dataframe(recent.head(limit), width="stretch", height=560)

