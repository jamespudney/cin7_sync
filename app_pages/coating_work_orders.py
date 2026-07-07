"""Anodizing and powder-coating buying/work-order page."""

from __future__ import annotations

import json
import re
from typing import Any, Optional

import pandas as pd
import streamlit as st


_POWDER_RE = re.compile(r"powder[\s_-]*coat|powdercoat", re.I)
_ANODIZE_RE = re.compile(r"anodi[sz]|anodize|anodise|anodizing|anodising", re.I)


def _num(value: Any, default: float = 0.0) -> float:
    try:
        if pd.isna(value):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _compact_num(value: float) -> str:
    value_f = _num(value)
    if abs(value_f - round(value_f)) < 0.0001:
        return f"{int(round(value_f))}"
    return f"{value_f:.2f}".rstrip("0").rstrip(".")


def _coating_type(component_sku: Any, component_name: Any) -> str:
    text = f"{component_sku or ''} {component_name or ''}"
    if _POWDER_RE.search(text):
        return "Powder coating"
    if _ANODIZE_RE.search(text):
        return "Anodizing"
    return ""


def _supplier_label(value: Any) -> str:
    """Return a readable first supplier name from CIN7's Suppliers field."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    parsed = value
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return ""
        try:
            parsed = json.loads(text)
        except Exception:
            return text
    if isinstance(parsed, dict):
        parsed = [parsed]
    if not isinstance(parsed, list):
        return str(value)
    names: list[str] = []
    for item in parsed:
        if not isinstance(item, dict):
            continue
        for key in (
            "SupplierName",
            "Supplier",
            "Name",
            "Company",
            "ContactName",
        ):
            name = str(item.get(key) or "").strip()
            if name:
                names.append(name)
                break
    return ", ".join(dict.fromkeys(names))


def _stock_by_sku(stock: pd.DataFrame) -> dict[str, dict[str, float]]:
    if stock is None or stock.empty or "SKU" not in stock.columns:
        return {}
    df = stock.copy()
    for col in ("OnHand", "Available", "OnOrder", "Allocated"):
        if col not in df.columns:
            df[col] = 0
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    grouped = (
        df.groupby("SKU", dropna=False)
        .agg(
            OnHand=("OnHand", "sum"),
            Available=("Available", "sum"),
            OnOrder=("OnOrder", "sum"),
            Allocated=("Allocated", "sum"),
        )
        .reset_index()
    )
    return grouped.set_index("SKU").to_dict(orient="index")


def _rows_by_sku(df: pd.DataFrame) -> dict[str, dict[str, Any]]:
    if df is None or df.empty or "SKU" not in df.columns:
        return {}
    return (
        df.drop_duplicates("SKU", keep="last")
        .set_index("SKU")
        .to_dict(orient="index")
    )


def _normalise_boms(boms: pd.DataFrame) -> pd.DataFrame:
    if boms is None or boms.empty:
        return pd.DataFrame()
    df = boms.copy()
    for col in (
        "AssemblySKU",
        "AssemblyName",
        "ComponentSKU",
        "ComponentName",
        "Quantity",
        "BOMType",
    ):
        if col not in df.columns:
            df[col] = ""
    df["AssemblySKU"] = df["AssemblySKU"].fillna("").astype(str).str.strip()
    df["ComponentSKU"] = df["ComponentSKU"].fillna("").astype(str).str.strip()
    df["AssemblyName"] = df["AssemblyName"].fillna("").astype(str)
    df["ComponentName"] = df["ComponentName"].fillna("").astype(str)
    df["Quantity"] = pd.to_numeric(df["Quantity"], errors="coerce").fillna(0)
    df = df[df["AssemblySKU"].str.len().gt(0)]
    df = df[df["ComponentSKU"].str.len().gt(0)]
    df = df[df["AssemblySKU"] != df["ComponentSKU"]]
    return df


def _summarise_service_lines(service_lines: pd.DataFrame) -> pd.DataFrame:
    if service_lines is None or service_lines.empty:
        return pd.DataFrame()
    return (
        service_lines.groupby(
            ["Coating type", "Service SKU", "Service name", "Vendor"],
            dropna=False,
        )
        .agg(
            Service_qty=("Service qty", "sum"),
            Finished_lines=("Finished SKU", "nunique"),
            Send_units=("Send qty", "sum"),
            Finished_SKUs=(
                "Finished SKU",
                lambda x: ", ".join(sorted(set(map(str, x)))),
            ),
        )
        .reset_index()
        .sort_values(["Coating type", "Service_qty"], ascending=[True, False])
    )


def build_coating_work_orders(
    *,
    boms: pd.DataFrame,
    products: pd.DataFrame,
    stock: pd.DataFrame,
    engine_df: pd.DataFrame,
    image_lookup: Optional[dict[str, str]] = None,
) -> dict[str, pd.DataFrame]:
    """Build coating replenishment rows from CIN7 BOM service components.

    CIN7 BOMs are the source of truth: an assembly is considered a coating
    variant only when its BOM includes a service component whose SKU or name
    looks like powder coating or anodizing.
    """
    bom_df = _normalise_boms(boms)
    if bom_df.empty:
        empty = pd.DataFrame()
        return {
            "lines": empty,
            "service_lines": empty,
            "service_summary": empty,
            "bom_rows": empty,
        }

    bom_df["Coating type"] = bom_df.apply(
        lambda r: _coating_type(r.get("ComponentSKU"), r.get("ComponentName")),
        axis=1,
    )
    service_rows = bom_df[bom_df["Coating type"].astype(str).str.len().gt(0)]
    if service_rows.empty:
        empty = pd.DataFrame()
        return {
            "lines": empty,
            "service_lines": empty,
            "service_summary": empty,
            "bom_rows": bom_df,
        }

    product_map = _rows_by_sku(products)
    stock_map = _stock_by_sku(stock)
    engine_map = _rows_by_sku(engine_df)
    image_lookup = image_lookup or {}
    rows: list[dict[str, Any]] = []
    service_summary_rows: list[dict[str, Any]] = []

    for assembly_sku, asm_service_rows in service_rows.groupby("AssemblySKU"):
        all_components = bom_df[bom_df["AssemblySKU"] == assembly_sku].copy()
        material_rows = all_components[
            ~all_components.index.isin(asm_service_rows.index)
        ].copy()

        prod = product_map.get(assembly_sku, {})
        eng = engine_map.get(assembly_sku, {})
        stk = stock_map.get(assembly_sku, {})
        assembly_name = (
            eng.get("Name")
            or prod.get("Name")
            or asm_service_rows.iloc[0].get("AssemblyName")
            or ""
        )

        onhand = _num(eng.get("OnHand", stk.get("OnHand", 0)))
        available = _num(eng.get("Available", stk.get("Available", 0)))
        on_order = _num(eng.get("OnOrder", stk.get("OnOrder", 0)))
        target = _num(eng.get("target_stock", 0))
        reorder_qty = _num(eng.get("reorder_qty", 0))
        effective_pos = available + on_order
        shortfall = max(0.0, target - effective_pos)
        action_qty = max(reorder_qty, shortfall)
        if action_qty > 0 and abs(action_qty - round(action_qty)) < 0.001:
            action_qty = float(int(round(action_qty)))

        material_bits: list[str] = []
        raw_required_bits: list[str] = []
        raw_available_bits: list[str] = []
        buildable_values: list[float] = []
        for _, comp in material_rows.iterrows():
            comp_sku = str(comp.get("ComponentSKU") or "").strip()
            if not comp_sku:
                continue
            qty_per = _num(comp.get("Quantity"), 0)
            if qty_per <= 0:
                continue
            comp_name = str(comp.get("ComponentName") or "").strip()
            comp_stk = stock_map.get(comp_sku, {})
            comp_available = _num(comp_stk.get("Available", comp_stk.get("OnHand", 0)))
            comp_required = action_qty * qty_per
            buildable_values.append(comp_available / qty_per)
            material_bits.append(
                f"{comp_sku} x {_compact_num(qty_per)}"
                + (f" - {comp_name}" if comp_name else "")
            )
            raw_required_bits.append(
                f"{comp_sku}: {_compact_num(comp_required)}"
            )
            raw_available_bits.append(
                f"{comp_sku}: {_compact_num(comp_available)}"
            )

        buildable = min(buildable_values) if buildable_values else 0.0
        if action_qty <= 0:
            raw_status = "No action"
        elif not buildable_values:
            raw_status = "Check BOM"
        elif buildable + 0.0001 >= action_qty:
            raw_status = "Raw available"
        else:
            raw_status = "Raw short"

        service_bits: list[str] = []
        service_vendor_bits: list[str] = []
        service_qty_total = 0.0
        coating_types = sorted(set(asm_service_rows["Coating type"].tolist()))
        for _, service in asm_service_rows.iterrows():
            service_sku = str(service.get("ComponentSKU") or "").strip()
            service_name = str(service.get("ComponentName") or "").strip()
            qty_per = _num(service.get("Quantity"), 0)
            service_qty = action_qty * qty_per
            service_qty_total += service_qty
            service_prod = product_map.get(service_sku, {})
            service_vendor = _supplier_label(service_prod.get("Suppliers"))
            if service_vendor:
                service_vendor_bits.append(service_vendor)
            service_bits.append(
                f"{service_sku} x {_compact_num(qty_per)}"
                + (f" - {service_name}" if service_name else "")
            )
            if service_qty > 0:
                service_summary_rows.append({
                    "Process": service.get("Coating type") or "",
                    "Service SKU": service_sku,
                    "Service name": service_name,
                    "Vendor": service_vendor,
                    "Service qty": service_qty,
                    "Finished lines": 1,
                    "Finished SKU": assembly_sku,
                    "Finished name": assembly_name,
                    "Send qty": action_qty,
                    "PO Comment": (
                        f"Finishing for: {assembly_sku} — "
                        f"{assembly_name} × {_compact_num(action_qty)}"
                    ),
                })

        # v2.67.370 — column order mirrors the Ordering page so buyers
        # have a consistent mental model across both pages.
        # PO comment context: finished SKU + name + qty for the vendor.
        _po_comment = (
            f"Finishing for: {assembly_sku} — {assembly_name} × "
            f"{_compact_num(action_qty)}"
        )
        rows.append({
            "Image": image_lookup.get(assembly_sku, ""),
            "Finished SKU": assembly_sku,
            "Finished name": assembly_name,
            "ABC": eng.get("ABC") or "",
            "Status": eng.get("Status") or "",
            "Process": " + ".join(coating_types),
            "Trend": eng.get("trend_flag") or "",
            "Last 6 months": eng.get("last_6mo_series") or "",
            "Avg/month": _num(eng.get("avg_month", 0)),
            "units_12mo": _num(
                eng.get("effective_units_12mo", eng.get("units_12mo", 0))),
            "units_45d": _num(eng.get("units_45d", 0)),
            "OnHand": onhand,
            "Available": available,
            "OnOrder": on_order,
            "Target stock": target,
            "Suggested send": reorder_qty,
            "Send qty": action_qty,
            "Stock ready?": raw_status,
            "Raw profile/part": "\n".join(material_bits),
            "Raw needed": "\n".join(raw_required_bits),
            "Raw on hand": "\n".join(raw_available_bits),
            "Buildable": buildable,
            "Coating service": "\n".join(service_bits),
            "Vendor": ", ".join(dict.fromkeys(service_vendor_bits)),
            "PO Comment": _po_comment,
            "Supplier": eng.get("Supplier") or _supplier_label(prod.get("Suppliers")),
        })

    lines = pd.DataFrame(rows)
    if not lines.empty:
        lines = lines.sort_values(
            ["Send qty", "units_45d", "units_12mo"],
            ascending=[False, False, False],
        )

    service_lines = pd.DataFrame(service_summary_rows)
    service_summary = _summarise_service_lines(service_lines)
    return {
        "lines": lines,
        "service_lines": service_lines,
        "service_summary": service_summary,
        "bom_rows": bom_df,
    }


def render_anodizing_powder_coating(
    *,
    boms: pd.DataFrame,
    products: pd.DataFrame,
    stock: pd.DataFrame,
    engine_df: pd.DataFrame,
    product_images: pd.DataFrame,
    product_image_lookup,
    fmt_number,
    fmt_money,
    rows_selector,
) -> None:
    st.header("🎨 Finishing Work Orders")
    st.info(
        "**Outsourced finishing queue — raw profiles + coating service = finished stock.**  \n"
        "This page shows which finished SKUs need raw material sent to a powder coating "
        "or anodizing vendor. Relationships are driven entirely by CIN7 BOMs — a SKU "
        "appears here only if its BOM contains a coating/anodizing service component "
        "(e.g. `OSC-POWDERCOAT-BK-LRG-FT`).  \n"
        "**Workflow:** decide qty to send → raise vendor PO (service SKU goes in PO with "
        "finished SKU context in the Comment field) → send raw stock → receive service → "
        "complete CIN7 assembly to produce the finished SKU.",
        icon="ℹ️",
    )

    if boms is None or boms.empty:
        st.warning(
            "No BOM data loaded. Run the CIN7 BOM sync before this page can "
            "identify powder-coated or anodized variants."
        )
        return

    image_lookup = product_image_lookup(products, product_images)
    built = build_coating_work_orders(
        boms=boms,
        products=products,
        stock=stock,
        engine_df=engine_df,
        image_lookup=image_lookup,
    )
    lines = built["lines"]
    service_lines = built["service_lines"]

    if lines.empty:
        st.info(
            "No coating/anodizing service components were found in the synced "
            "CIN7 BOMs. I look for component SKUs/names containing powder "
            "coat, powdercoat, anodize/anodise/anodizing/anodising."
        )
        return

    action_lines = lines[lines["Send qty"].fillna(0) > 0].copy()
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Coated/anodized finished SKUs", fmt_number(len(lines)))
    c2.metric("Need action now", fmt_number(len(action_lines)))
    c3.metric(
        "Finished units to send",
        fmt_number(action_lines["Send qty"].sum() if not action_lines.empty else 0),
    )
    c4.metric(
        "Raw-short lines",
        fmt_number(int((action_lines["Raw status"] == "Raw short").sum())),
    )

    filters = st.container(border=True)
    with filters:
        f1, f2, f3, f4 = st.columns([2, 1.3, 1.3, 1.3])
        query = f1.text_input(
            "Search finished SKU, raw SKU, service SKU, or name",
            key="coating_search",
        )
        coating_opts = sorted(lines["Process"].dropna().unique().tolist())
        coating_filter = f2.multiselect(
            "Process",
            coating_opts,
            default=coating_opts,
            key="coating_process_filter",
        )
        raw_filter = f3.multiselect(
            "Raw status",
            ["Raw available", "Raw short", "Check BOM", "No action"],
            default=["Raw available", "Raw short", "Check BOM"],
            key="coating_raw_filter",
        )
        action_only = f4.checkbox(
            "Only action-needed",
            value=True,
            key="coating_action_only",
        )

    view = lines.copy()
    if action_only:
        view = view[view["Send qty"].fillna(0) > 0]
    if coating_filter:
        view = view[view["Process"].isin(coating_filter)]
    if raw_filter:
        view = view[view["Raw status"].isin(raw_filter)]
    if query:
        q = query.strip()
        mask = pd.Series(False, index=view.index)
        for col in (
            "Finished SKU",
            "Finished name",
            "Coating service",
            "Raw profile/part",
            "Vendor",
            "Supplier",
            "Process",
        ):
            if col in view.columns:
                mask |= view[col].astype(str).str.contains(
                    q, case=False, na=False, regex=False)
        view = view[mask]
    if service_lines.empty or view.empty:
        service_summary = pd.DataFrame()
    else:
        filtered_service_lines = service_lines[
            service_lines["Finished SKU"].isin(view["Finished SKU"])
        ]
        service_summary = _summarise_service_lines(filtered_service_lines)

    tabs = st.tabs(["🔴 Action now", "📋 Send to vendor", "📦 All finishing SKUs"])

    _COL_CONFIG = {
        "Image": st.column_config.ImageColumn("Image", width="small"),
        "Finished SKU": st.column_config.TextColumn("Finished SKU", width="medium"),
        "Finished name": st.column_config.TextColumn("Name", width="large"),
        "ABC": st.column_config.TextColumn("ABC", width="small"),
        "Status": st.column_config.TextColumn("Status", width="medium"),
        "Process": st.column_config.TextColumn("Process", width="medium"),
        "Trend": st.column_config.TextColumn("Trend", width="medium"),
        "Last 6 months": st.column_config.TextColumn("Last 6 mo", width="medium"),
        "Avg/month": st.column_config.NumberColumn("Avg/mo", format="%.1f"),
        "units_12mo": st.column_config.NumberColumn("12mo units", format="%.0f"),
        "units_45d": st.column_config.NumberColumn("45d units", format="%.0f"),
        "OnHand": st.column_config.NumberColumn("On Hand", format="%.0f"),
        "Available": st.column_config.NumberColumn("Available", format="%.0f"),
        "OnOrder": st.column_config.NumberColumn("On Order", format="%.0f"),
        "Target stock": st.column_config.NumberColumn("Target", format="%.1f"),
        "Suggested send": st.column_config.NumberColumn("Suggested", format="%.0f"),
        "Send qty": st.column_config.NumberColumn("Send qty", format="%.0f"),
        "Stock ready?": st.column_config.TextColumn("Stock ready?", width="medium"),
        "Raw profile/part": st.column_config.TextColumn("Raw profile/part", width="large"),
        "Raw needed": st.column_config.TextColumn("Raw needed", width="medium"),
        "Raw on hand": st.column_config.TextColumn("Raw on hand", width="medium"),
        "Buildable": st.column_config.NumberColumn("Buildable", format="%.0f"),
        "Coating service": st.column_config.TextColumn("Coating service", width="large"),
        "Vendor": st.column_config.TextColumn("Vendor", width="medium"),
        "PO Comment": st.column_config.TextColumn(
            "PO Comment (copy to CIN7)", width="large",
            help="Paste this into the CIN7 PO line Comment field so the "
                 "vendor and warehouse know which finished SKU this batch is for."),
        "Supplier": st.column_config.TextColumn("Supplier", width="medium"),
    }

    # Column order mirrors the Ordering page: identity → demand → stock
    # position → action quantities → raw/service detail → PO context
    _COL_ORDER = [
        "Image", "Finished SKU", "Finished name", "ABC", "Status", "Process",
        "Trend", "Last 6 months", "Avg/month", "units_12mo", "units_45d",
        "OnHand", "Available", "OnOrder",
        "Target stock", "Suggested send", "Send qty",
        "Stock ready?", "Raw profile/part", "Raw needed", "Raw on hand",
        "Buildable", "Coating service", "Vendor", "PO Comment", "Supplier",
    ]

    def _ordered(df: pd.DataFrame) -> pd.DataFrame:
        cols = [c for c in _COL_ORDER if c in df.columns]
        extra = [c for c in df.columns if c not in cols]
        return df[cols + extra]

    with tabs[0]:
        st.caption(
            "Finishing SKUs at or below target stock — decide quantities, "
            "copy the **PO Comment** into your CIN7 service PO line so "
            "the vendor knows which finished SKU each batch produces."
        )
        limit = rows_selector(key="coating_work_queue_rows")
        show = _ordered(view.head(limit))
        st.dataframe(show, width="stretch", height=620, column_config=_COL_CONFIG)
        if not show.empty:
            st.download_button(
                "⬇ Download work-order CSV",
                data=show.drop(columns=["Image"], errors="ignore").to_csv(
                    index=False),
                file_name="finishing_work_orders.csv",
                mime="text/csv",
                use_container_width=True,
            )

    with tabs[1]:
        st.caption(
            "One row per coating/anodizing service SKU. Use this to raise "
            "the vendor PO — paste the **PO Comment** into each CIN7 line "
            "so the vendor knows which finished SKUs the batch produces."
        )
        if service_summary.empty:
            st.info("No positive service quantities in the current filters.")
        else:
            _SVC_COL_CONFIG = {
                "Process": st.column_config.TextColumn("Process", width="medium"),
                "Vendor": st.column_config.TextColumn("Vendor", width="medium"),
                "Service SKU": st.column_config.TextColumn(
                    "Service SKU", width="medium"),
                "Service name": st.column_config.TextColumn(
                    "Service name", width="large"),
                "Qty to order": st.column_config.NumberColumn(
                    "Qty to order", format="%.0f"),
                "Send qty total": st.column_config.NumberColumn(
                    "Raw send total", format="%.0f"),
                "Lines": st.column_config.NumberColumn(
                    "Finished lines", format="%.0f"),
                "Finished SKUs": st.column_config.TextColumn(
                    "Finished SKUs", width="large"),
                "Finished names": st.column_config.TextColumn(
                    "Finished names", width="large"),
                "PO Comment": st.column_config.TextColumn(
                    "PO Comment (copy to CIN7)", width="large",
                    help="Paste into the CIN7 PO line Comment field — "
                         "gives vendor and warehouse full context."),
            }
            st.dataframe(
                service_summary,
                width="stretch",
                height=420,
                column_config=_SVC_COL_CONFIG,
            )
            st.download_button(
                "⬇ Download vendor send CSV",
                data=service_summary.to_csv(index=False),
                file_name="finishing_vendor_send.csv",
                mime="text/csv",
                use_container_width=True,
            )

    with tabs[2]:
        all_limit = rows_selector(key="coating_all_rows")
        all_view = _ordered(lines.head(all_limit))
        st.caption(
            "All finished SKUs whose CIN7 BOM includes a powder-coating or "
            "anodizing service component, including rows with no action today."
        )
        st.dataframe(all_view, width="stretch", height=620,
                     column_config=_COL_CONFIG)
