"""Product Master page."""

from __future__ import annotations

import streamlit as st


def render_product_master(*, products, rows_selector, parent_sku_for) -> None:
    st.header(":label: Product Master")

    if products.empty:
        st.warning("No products data. Run `python cin7_sync.py products`.")
        return

    c1, c2, c3, c4 = st.columns(4)
    cats = sorted(products["Category"].dropna().unique().tolist())
    brands = sorted(products["Brand"].dropna().unique().tolist())
    types = sorted(products["Type"].dropna().unique().tolist())
    statuses = sorted(products["Status"].dropna().unique().tolist())

    sel_cat = c1.multiselect("Category", cats, default=[])
    sel_brand = c2.multiselect("Brand", brands, default=[])
    sel_type = c3.multiselect("Type", types, default=[])
    sel_status = c4.multiselect(
        "Status",
        statuses,
        default=["Active"] if "Active" in statuses else [],
    )

    query = st.text_input("Search SKU or name", "")

    df = products.copy()
    if sel_cat:
        df = df[df["Category"].isin(sel_cat)]
    if sel_brand:
        df = df[df["Brand"].isin(sel_brand)]
    if sel_type:
        df = df[df["Type"].isin(sel_type)]
    if sel_status:
        df = df[df["Status"].isin(sel_status)]
    if query:
        mask = (
            df["SKU"].astype(str).str.contains(query, case=False, na=False)
            | df["Name"].astype(str).str.contains(query, case=False, na=False)
        )
        df = df[mask]

    df["Parent"] = df["SKU"].map(parent_sku_for)

    show_cols = [
        "SKU",
        "Name",
        "Parent",
        "Category",
        "Brand",
        "Type",
        "Status",
        "AverageCost",
        "MinimumBeforeReorder",
        "ReorderQuantity",
        "CreatedDate",
        "LastModifiedOn",
    ]
    show_cols = [c for c in show_cols if c in df.columns]
    limit = rows_selector(key="product_rows")
    st.caption(
        f"Showing {min(limit, len(df)):,} of {len(df):,} "
        f"matching (out of {len(products):,} total)"
    )
    st.dataframe(df[show_cols].head(limit), width="stretch", height=560)

