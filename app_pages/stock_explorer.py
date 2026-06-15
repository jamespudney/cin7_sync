"""Stock Explorer page."""

from __future__ import annotations

import streamlit as st


def render_stock_explorer(
    *,
    stock,
    products,
    to_num,
    fmt_number,
    fmt_money,
    rows_selector,
    parent_sku_for,
    family_sku_for,
) -> None:
    st.header(":package: Stock Explorer")

    if stock.empty:
        st.warning("No stock data. Run `python cin7_sync.py stock`.")
        return

    df = stock.copy()
    df["OnHand"] = to_num(df["OnHand"]).fillna(0)
    df["Available"] = to_num(df["Available"]).fillna(0)
    df["OnOrder"] = to_num(df["OnOrder"]).fillna(0)
    df["Allocated"] = to_num(df.get("Allocated", 0)).fillna(0)
    df["Phantom"] = (df["Available"] - df["OnHand"]).clip(lower=0)

    if not products.empty:
        bom_map = products.set_index("SKU")[
            [
                "BillOfMaterial",
                "BOMType",
                "AutoAssembly",
                "AutoDisassembly",
                "AverageCost",
            ]
        ].to_dict(orient="index")
        df["IsBOM"] = df["SKU"].map(
            lambda s: str(bom_map.get(s, {}).get("BillOfMaterial")) == "True"
        )
        df["BOMType"] = df["SKU"].map(
            lambda s: bom_map.get(s, {}).get("BOMType")
        )
        df["AvgCost"] = df["SKU"].map(
            lambda s: float(bom_map.get(s, {}).get("AverageCost") or 0)
        )
        if "StockOnHand" in df.columns:
            fifo = to_num(df["StockOnHand"]).fillna(0)
            onhand_avg = df["OnHand"] * df["AvgCost"]
            df["OnHandValue"] = fifo.where(fifo > 0, onhand_avg)
        else:
            df["OnHandValue"] = df["OnHand"] * df["AvgCost"]
    else:
        df["IsBOM"] = False
        df["BOMType"] = None
        df["AvgCost"] = 0.0
        df["OnHandValue"] = 0.0

    df["Parent"] = df["SKU"].map(parent_sku_for)
    df["Family"] = df["SKU"].map(family_sku_for)

    c1, c2, c3, c4 = st.columns(4)
    locs = sorted(stock["Location"].dropna().unique().tolist())
    sel_loc = c1.multiselect("Location", locs, default=[])
    query = c2.text_input("Search SKU or name", "")
    stock_filter = c3.selectbox(
        "Stock filter",
        [
            "All",
            "Zero physical (OnHand=0)",
            "Below 5 physical",
            "Positive physical only",
        ],
    )
    bom_filter = c4.selectbox(
        "BOM filter",
        [
            "All",
            "BOM products only",
            "Non-BOM only",
            "Phantom stock > 0 (derivable from masters)",
        ],
    )

    if sel_loc:
        df = df[df["Location"].isin(sel_loc)]
    if query:
        mask = (
            df["SKU"].astype(str).str.contains(query, case=False, na=False)
            | df["Name"].astype(str).str.contains(query, case=False, na=False)
        )
        df = df[mask]
    if stock_filter == "Zero physical (OnHand=0)":
        df = df[df["OnHand"] <= 0]
    elif stock_filter == "Below 5 physical":
        df = df[(df["OnHand"] > 0) & (df["OnHand"] < 5)]
    elif stock_filter == "Positive physical only":
        df = df[df["OnHand"] > 0]

    if bom_filter == "BOM products only":
        df = df[df["IsBOM"]]
    elif bom_filter == "Non-BOM only":
        df = df[~df["IsBOM"]]
    elif bom_filter == "Phantom stock > 0 (derivable from masters)":
        df = df[df["Phantom"] > 0]

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("SKU-locations shown", fmt_number(len(df)))
    c2.metric("Physical units (OnHand)", fmt_number(df["OnHand"].sum()))
    c3.metric("Phantom units (derivable)", fmt_number(df["Phantom"].sum()))
    c4.metric("Physical cash tied up", fmt_money(df["OnHandValue"].sum()))

    with st.expander("What's 'Phantom Stock'?"):
        st.markdown(
            "**`Available - OnHand`** for BOM products. These are units "
            "CIN7 *could* make by auto-assembly or auto-disassembly from "
            "master-length stock. They don't exist yet — no cash is tied "
            "up in them — but they're fulfillable if a customer orders.\n\n"
            "- **`OnHand`** = physical stock with actual cash invested.\n"
            "- **`Available`** = OnHand + Phantom = what we can actually "
            "ship to a customer.\n"
            "- Use **OnHand x AvgCost** for cash / working capital analysis.\n"
            "- Use **Available** for service-level / reorder decisions."
        )

    show_cols = [
        "SKU",
        "Name",
        "Parent",
        "Location",
        "OnHand",
        "Phantom",
        "Available",
        "Allocated",
        "OnOrder",
        "IsBOM",
        "BOMType",
        "AvgCost",
        "OnHandValue",
        "Bin",
        "NextDeliveryDate",
    ]
    show_cols = [c for c in show_cols if c in df.columns]
    limit = rows_selector(key="stock_rows")
    sorted_df = df[show_cols].sort_values("OnHandValue", ascending=False)
    st.caption(
        f"Showing {min(limit, len(sorted_df)):,} of "
        f"{len(sorted_df):,} matching rows"
    )
    st.dataframe(
        sorted_df.head(limit),
        width="stretch",
        height=520,
        column_config={
            "AvgCost": st.column_config.NumberColumn(format="$%.2f"),
            "OnHandValue": st.column_config.NumberColumn(format="$%.0f"),
            "IsBOM": st.column_config.CheckboxColumn("BOM?"),
        },
    )

