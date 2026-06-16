"""Data Health page."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from data_catalog import catalog_rows


def render_data_health(row_counts: dict[str, object]) -> None:
    st.header("Data Health")
    st.caption(
        "One place to see whether the snapshots powering the dashboard, "
        "bot, and buying engine are present and fresh."
    )

    rows = catalog_rows(row_counts=row_counts)
    df = pd.DataFrame(rows)
    status_order = {"missing": 0, "stale": 1, "aging": 2, "fresh": 3}
    if not df.empty:
        df["_status_sort"] = df["Status"].map(status_order).fillna(99)
        df = df.sort_values(["_status_sort", "Group", "Dataset"])
        df = df.drop(columns=["_status_sort"])

    st.dataframe(
        df,
        width="stretch",
        height=520,
        column_config={
            "Command": st.column_config.TextColumn(width="large"),
            "Latest file": st.column_config.TextColumn(width="large"),
        },
    )

    if not df.empty:
        missing = int((df["Status"] == "missing").sum())
        stale = int((df["Status"] == "stale").sum())
        aging = int((df["Status"] == "aging").sum())
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Fresh", int((df["Status"] == "fresh").sum()))
        c2.metric("Aging", aging)
        c3.metric("Stale", stale)
        c4.metric("Missing", missing)

    st.subheader("Expected sync commands")
    st.code(
        "# Near-real-time refresh\n"
        "python cin7_sync.py nearsync --days 1\n"
        "python warm_engine.py\n\n"
        "# Daily refresh\n"
        "python cin7_sync.py quick --days 3\n"
        "python cin7_sync.py sales --days 30\n"
        "python cin7_sync.py purchases --days 30\n"
        "python cin7_sync.py salelines --days 30\n"
        "python cin7_sync.py purchaselines --days 30\n"
        "python cin7_sync.py stockadjustments --days 30\n"
        "python cin7_sync.py stocktransfers --days 30\n"
        "python ip_pull_alternates.py\n"
        "python shopify_sync.py\n"
        "python warm_engine.py\n\n"
        "# Full 12-month bootstrap (manual, overnight)\n"
        "python cin7_sync.py salelines --days 365\n",
        language="powershell",
    )
