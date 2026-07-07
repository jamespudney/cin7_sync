"""Finishing Work Orders — outsourced powder coating & anodizing.

Process-first interactive workflow:
  1. Pick a process (All / Powder coating / Anodizing)
  2. See ALL coated finished SKUs for that process, governed by the ABC engine
  3. Tick what to action, edit send quantities
  4. Export work order or push draft PO to CIN7
     (service SKU = the PO line; finished SKU context in Comment)
"""

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
        for key in ("SupplierName", "Supplier", "Name", "Company", "ContactName"):
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
    for col in ("AssemblySKU", "AssemblyName", "ComponentSKU",
                "ComponentName", "Quantity", "BOMType"):
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


def build_coating_work_orders(
    *,
    boms: pd.DataFrame,
    products: pd.DataFrame,
    stock: pd.DataFrame,
    engine_df: pd.DataFrame,
    image_lookup: Optional[dict[str, str]] = None,
) -> dict[str, pd.DataFrame]:
    """Build finishing work order rows from CIN7 BOM service components.
    Source of truth: CIN7 BOMs. A finished SKU appears only if its BOM
    contains a powder coating or anodizing service component.
    """
    bom_df = _normalise_boms(boms)
    if bom_df.empty:
        empty = pd.DataFrame()
        return {"lines": empty, "service_lines": empty, "bom_rows": empty}

    bom_df["Coating type"] = bom_df.apply(
        lambda r: _coating_type(r.get("ComponentSKU"), r.get("ComponentName")),
        axis=1,
    )
    service_rows = bom_df[bom_df["Coating type"].astype(str).str.len().gt(0)]
    if service_rows.empty:
        empty = pd.DataFrame()
        return {"lines": empty, "service_lines": empty, "bom_rows": bom_df}

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
            eng.get("Name") or prod.get("Name")
            or asm_service_rows.iloc[0].get("AssemblyName") or ""
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

        # Raw material components
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
            comp_available = _num(
                comp_stk.get("Available", comp_stk.get("OnHand", 0)))
            comp_required = action_qty * qty_per
            buildable_values.append(comp_available / qty_per)
            material_bits.append(
                f"{comp_sku} x {_compact_num(qty_per)}"
                + (f" - {comp_name}" if comp_name else ""))
            raw_required_bits.append(f"{comp_sku}: {_compact_num(comp_required)}")
            raw_available_bits.append(f"{comp_sku}: {_compact_num(comp_available)}")

        buildable = min(buildable_values) if buildable_values else 0.0
        if action_qty <= 0:
            raw_status = "No action"
        elif not buildable_values:
            raw_status = "Check BOM"
        elif buildable + 0.0001 >= action_qty:
            raw_status = "Raw available"
        else:
            raw_status = "Raw short"

        # Service components
        service_bits: list[str] = []
        service_vendor_bits: list[str] = []
        service_qty_total = 0.0
        coating_types = sorted(set(asm_service_rows["Coating type"].tolist()))
        primary_service_sku = ""
        primary_service_vendor = ""
        primary_service_cost = 0.0

        for _, service in asm_service_rows.iterrows():
            service_sku = str(service.get("ComponentSKU") or "").strip()
            service_name = str(service.get("ComponentName") or "").strip()
            qty_per = _num(service.get("Quantity"), 0)
            service_qty = action_qty * qty_per
            service_qty_total += service_qty
            service_prod = product_map.get(service_sku, {})
            service_vendor = _supplier_label(service_prod.get("Suppliers"))
            service_cost = _num(service_prod.get("AverageCost", 0))
            if not primary_service_sku:
                primary_service_sku = service_sku
                primary_service_vendor = service_vendor
                primary_service_cost = service_cost
            if service_vendor:
                service_vendor_bits.append(service_vendor)
            service_bits.append(
                f"{service_sku} x {_compact_num(qty_per)}"
                + (f" - {service_name}" if service_name else ""))
            if service_qty > 0:
                service_summary_rows.append({
                    "Process": service.get("Coating type") or "",
                    "Vendor": service_vendor,
                    "Service SKU": service_sku,
                    "Service name": service_name,
                    "Service qty": service_qty,
                    "Finished SKU": assembly_sku,
                    "Finished name": assembly_name,
                    "Send qty": action_qty,
                    "PO Comment": (
                        f"Finishing for: {assembly_sku} — "
                        f"{assembly_name} × {_compact_num(action_qty)}"
                    ),
                })

        rows.append({
            "Include?": action_qty > 0,
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
            "Service SKU": primary_service_sku,
            "Service vendor": primary_service_vendor,
            "Service cost": primary_service_cost,
            "PO Comment": (
                f"Finishing for: {assembly_sku} — "
                f"{assembly_name} × {_compact_num(action_qty)}"
            ),
            "Supplier": (
                eng.get("Supplier") or _supplier_label(prod.get("Suppliers"))),
        })

    lines = pd.DataFrame(rows)
    if not lines.empty:
        lines = lines.sort_values(
            ["Send qty", "units_45d", "units_12mo"],
            ascending=[False, False, False],
        )

    service_lines = pd.DataFrame(service_summary_rows)
    return {"lines": lines, "service_lines": service_lines, "bom_rows": bom_df}


def _summarise_service_lines(service_lines: pd.DataFrame) -> pd.DataFrame:
    """Summarise service lines by vendor/service SKU."""
    if service_lines is None or service_lines.empty:
        return pd.DataFrame()
    df = service_lines.copy()
    if "Coating type" in df.columns and "Process" not in df.columns:
        df = df.rename(columns={"Coating type": "Process"})
    elif "Process" not in df.columns:
        df["Process"] = ""
    grp = (
        df.groupby(
            ["Process", "Vendor", "Service SKU", "Service name"],
            dropna=False,
        )
        .agg(
            **{
                "Qty to order": ("Service qty", "sum"),
                "Lines": ("Finished SKU", "nunique"),
                "Send qty total": ("Send qty", "sum"),
                "Finished SKUs": (
                    "Finished SKU",
                    lambda x: ", ".join(sorted(set(map(str, x)))),
                ),
                "Finished names": (
                    "Finished name",
                    lambda x: ", ".join(
                        sorted(set(str(v) for v in x if v))),
                ),
                "PO Comment": (
                    "PO Comment",
                    lambda x: " | ".join(
                        sorted(set(str(v) for v in x if v))),
                ),
            }
        )
        .reset_index()
        .sort_values(["Process", "Qty to order"], ascending=[True, False])
    )
    col_order = [
        "Process", "Vendor", "Service SKU", "Service name",
        "Qty to order", "Send qty total", "Lines",
        "Finished SKUs", "Finished names", "PO Comment",
    ]
    return grp[[c for c in col_order if c in grp.columns]]


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
        "This page shows **all** finished SKUs that are assembled from a raw profile + "
        "a powder coating or anodizing service, as defined by their CIN7 BOMs.  \n"
        "**Workflow:** review engine-recommended SKUs → tick what to action → "
        "adjust quantities → export or push draft PO to CIN7 → send raw stock → "
        "receive service → complete CIN7 assembly.",
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
            "No coating/anodizing service components found in the synced CIN7 BOMs. "
            "Looking for component SKUs/names containing: "
            "powder coat, powdercoat, anodize/anodise/anodizing/anodising."
        )
        return

    # ── Process filter (the primary organiser — not vendor) ────────────────
    st.markdown("### ⚙️ Process")
    process_opts = ["All"] + sorted(
        lines["Process"].dropna().unique().tolist())
    pc1, pc2, pc3 = st.columns([2, 2, 3])
    sel_process = pc1.selectbox(
        "Finishing process",
        process_opts,
        key="finishing_process_select",
    )
    action_only = pc2.checkbox(
        "Action-needed only",
        value=True,
        key="finishing_action_only",
    )
    search = pc3.text_input(
        "Search SKU or name",
        key="finishing_search",
    )

    # ── Apply filters ──────────────────────────────────────────────────────
    view = lines.copy()
    if sel_process != "All":
        view = view[view["Process"] == sel_process]
    if action_only:
        view = view[view["Send qty"].fillna(0) > 0]
    if search:
        q = search.strip()
        mask = pd.Series(False, index=view.index)
        for col in ("Finished SKU", "Finished name", "Coating service",
                    "Raw profile/part", "Vendor", "Process"):
            if col in view.columns:
                mask |= view[col].astype(str).str.contains(
                    q, case=False, na=False, regex=False)
        view = view[mask]

    # ── Snapshot metrics ───────────────────────────────────────────────────
    action_view = view[view["Send qty"].fillna(0) > 0]
    mc1, mc2, mc3, mc4 = st.columns(4)
    mc1.metric("Finishing SKUs (filtered)", fmt_number(len(view)))
    mc2.metric("Need action now", fmt_number(len(action_view)))
    mc3.metric("Units to send", fmt_number(
        int(action_view["Send qty"].sum()) if not action_view.empty else 0))
    mc4.metric("Raw short", fmt_number(
        int((action_view["Stock ready?"] == "Raw short").sum())
        if not action_view.empty else 0))

    # ── Interactive work order grid ────────────────────────────────────────
    st.markdown("### 📋 Work order grid")
    st.caption(
        "Tick **✓** to include in the work order. "
        "Edit **✏ Send qty** to override the engine suggestion. "
        "Engine suggestion = max(reorder qty, shortfall vs target stock). "
        "Sorted by urgency: action-needed first, then by 45d demand."
    )

    EDITOR_COLS = [
        "Include?", "Image", "Finished SKU", "Finished name",
        "ABC", "Status", "Process", "Trend",
        "Last 6 months", "Avg/month", "units_12mo", "units_45d",
        "OnHand", "Available", "OnOrder",
        "Target stock", "Suggested send", "Send qty",
        "Stock ready?", "Raw profile/part", "Coating service", "Vendor",
        "PO Comment",
    ]
    DISABLED_COLS = [c for c in EDITOR_COLS if c not in ("Include?", "Send qty")]

    editor_df = view.copy().reset_index(drop=True)
    editor_df["Include?"] = editor_df["Send qty"].fillna(0) > 0
    show_cols = [c for c in EDITOR_COLS if c in editor_df.columns]

    _sess_key = f"finishing_editor_{sel_process}"
    edited = st.data_editor(
        editor_df[show_cols],
        key=_sess_key,
        use_container_width=True,
        height=580,
        disabled=DISABLED_COLS,
        column_config={
            "Include?": st.column_config.CheckboxColumn(
                "✓", help="Include in work order", width="small"),
            "Image": st.column_config.ImageColumn("Image", width="small"),
            "Finished SKU": st.column_config.TextColumn(
                "Finished SKU", width="medium"),
            "Finished name": st.column_config.TextColumn("Name", width="large"),
            "ABC": st.column_config.TextColumn("ABC", width="small"),
            "Status": st.column_config.TextColumn("Status", width="medium"),
            "Process": st.column_config.TextColumn("Process", width="medium"),
            "Trend": st.column_config.TextColumn("Trend", width="medium"),
            "Last 6 months": st.column_config.TextColumn(
                "Last 6 mo", width="medium"),
            "Avg/month": st.column_config.NumberColumn("Avg/mo", format="%.1f"),
            "units_12mo": st.column_config.NumberColumn(
                "12mo units", format="%.0f"),
            "units_45d": st.column_config.NumberColumn(
                "45d units", format="%.0f"),
            "OnHand": st.column_config.NumberColumn("On Hand", format="%.0f"),
            "Available": st.column_config.NumberColumn("Available", format="%.0f"),
            "OnOrder": st.column_config.NumberColumn("On Order", format="%.0f"),
            "Target stock": st.column_config.NumberColumn("Target", format="%.1f"),
            "Suggested send": st.column_config.NumberColumn(
                "Suggested", format="%.0f"),
            "Send qty": st.column_config.NumberColumn(
                "✏ Send qty", format="%.0f",
                help="Edit to override engine suggestion"),
            "Stock ready?": st.column_config.TextColumn(
                "Stock ready?", width="medium"),
            "Raw profile/part": st.column_config.TextColumn(
                "Raw profile/part", width="large"),
            "Coating service": st.column_config.TextColumn(
                "Coating service", width="large"),
            "Vendor": st.column_config.TextColumn("Vendor", width="medium"),
            "PO Comment": st.column_config.TextColumn(
                "PO Comment", width="large",
                help="Paste into CIN7 PO line Comment field"),
        },
    )

    # ── Work order summary + actions ───────────────────────────────────────
    selected = edited[edited["Include?"] == True].copy()  # noqa: E712
    n_lines = len(selected)
    total_units = int(selected["Send qty"].fillna(0).sum())

    st.divider()
    sa1, sa2, sa3 = st.columns([1, 1, 2])
    sa1.metric("Selected lines", n_lines)
    sa2.metric("Total units to send", total_units)

    # CSV export — always available
    if not selected.empty:
        sa3.download_button(
            "⬇ Download work order CSV",
            data=selected.drop(columns=["Image", "Include?"],
                               errors="ignore").to_csv(index=False),
            file_name="finishing_work_order.csv",
            mime="text/csv",
            use_container_width=True,
        )

    # ── Push to CIN7 ──────────────────────────────────────────────────────
    st.markdown("#### 🚀 Push to CIN7")
    st.caption(
        "Creates a Draft PO for the **coating/anodizing service SKUs**. "
        "The finished SKU + qty is written into each PO line Comment field "
        "so the vendor and warehouse know what each batch produces. "
        "The PO is grouped by service vendor — one PO per vendor."
    )

    with st.container(border=True):
        pb1, pb2 = st.columns([3, 1])
        actor = pb1.text_input(
            "Your name (recorded on the PO)",
            value=st.session_state.get("current_user", ""),
            key="finishing_actor",
        )
        dry_run = pb2.checkbox("Dry-run only", key="finishing_dry_run")
        ack = st.checkbox(
            f"I understand this creates a real Draft PO in CIN7 covering "
            f"{n_lines} service line(s). It needs human review in CIN7 "
            f"before the vendor sees it.",
            key="finishing_ack",
        )

        push_ok = n_lines > 0 and bool(actor.strip()) and ack
        if st.button(
            "✅ Confirm push" if not dry_run else "🔍 Validate (dry-run)",
            type="primary",
            disabled=not push_ok,
            key="finishing_push_btn",
        ):
            # Map finished SKU → service SKU using the full lines df
            svc_map = (
                lines.set_index("Finished SKU")[
                    ["Service SKU", "Service cost", "Service vendor", "PO Comment"]
                ].to_dict(orient="index")
            )
            # Group by service vendor so each gets its own PO
            vendor_groups: dict[str, list[dict]] = {}
            for _, row in selected.iterrows():
                fin_sku = str(row.get("Finished SKU") or "")
                svc_info = svc_map.get(fin_sku, {})
                svc_sku = svc_info.get("Service SKU", "")
                svc_vendor = svc_info.get("Service vendor", "") or "Unknown vendor"
                qty = _num(row.get("Send qty", 0))
                if not svc_sku or qty <= 0:
                    continue
                vendor_groups.setdefault(svc_vendor, []).append({
                    "sku": svc_sku,
                    "qty": qty,
                    "comment": str(svc_info.get("PO Comment", "")),
                    "finished_sku": fin_sku,
                    "finished_name": str(row.get("Finished name", "")),
                })

            if not vendor_groups:
                st.error("No valid service SKU lines. "
                         "Check service SKUs are in CIN7 BOMs.")
            else:
                for vendor, vlines in vendor_groups.items():
                    st.success(
                        f"{'🔍 Dry-run' if dry_run else '✅ Ready'}: "
                        f"**{vendor}** — {len(vlines)} line(s)"
                    )
                    for vl in vlines:
                        st.write(
                            f"• **{vl['sku']}** × {_compact_num(vl['qty'])} "
                            f"— _{vl['comment']}_"
                        )
                if not dry_run:
                    st.info(
                        "💡 Full CIN7 API push coming in next release. "
                        "For now copy the lines above into CIN7 manually — "
                        "the PO Comment text is pre-built for you."
                    )

    st.divider()

    # ── Secondary tabs ─────────────────────────────────────────────────────
    tabs = st.tabs(["📋 Send to vendor summary", "📦 All finishing SKUs"])

    # Service summary scoped to filtered view
    filtered_svc = (
        service_lines[
            service_lines["Finished SKU"].isin(view["Finished SKU"])
        ]
        if not service_lines.empty and "Finished SKU" in service_lines.columns
        else pd.DataFrame()
    )
    service_summary = _summarise_service_lines(filtered_svc)

    with tabs[0]:
        st.caption(
            "One row per coating/anodizing service SKU across all selected "
            "finished SKUs. Paste the **PO Comment** into each CIN7 PO line."
        )
        if service_summary.empty:
            st.info("No service lines for the current filter.")
        else:
            st.dataframe(
                service_summary,
                width="stretch",
                height=380,
                column_config={
                    "Process": st.column_config.TextColumn(
                        "Process", width="medium"),
                    "Vendor": st.column_config.TextColumn(
                        "Vendor", width="medium"),
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
                        help="Paste into CIN7 PO line Comment field."),
                },
            )
            st.download_button(
                "⬇ Download vendor send CSV",
                data=service_summary.to_csv(index=False),
                file_name="finishing_vendor_send.csv",
                mime="text/csv",
                use_container_width=True,
            )

    with tabs[1]:
        all_col_order = [
            "Include?", "Image", "Finished SKU", "Finished name",
            "ABC", "Status", "Process", "Trend",
            "Last 6 months", "Avg/month", "units_12mo", "units_45d",
            "OnHand", "Available", "OnOrder",
            "Target stock", "Suggested send", "Send qty",
            "Stock ready?", "Raw profile/part", "Raw needed", "Raw on hand",
            "Buildable", "Coating service", "Vendor", "PO Comment", "Supplier",
        ]
        all_view = lines[[c for c in all_col_order if c in lines.columns]].copy()
        st.caption(
            "All finished SKUs with coating/anodizing BOMs across all "
            "processes and vendors, including no-action rows."
        )
        st.dataframe(
            all_view,
            width="stretch",
            height=560,
            column_config={
                "Include?": st.column_config.CheckboxColumn("✓", width="small"),
                "Image": st.column_config.ImageColumn("Image", width="small"),
                "Send qty": st.column_config.NumberColumn(
                    "Send qty", format="%.0f"),
                "Suggested send": st.column_config.NumberColumn(
                    "Suggested", format="%.0f"),
                "Avg/month": st.column_config.NumberColumn(
                    "Avg/mo", format="%.1f"),
                "units_12mo": st.column_config.NumberColumn(
                    "12mo units", format="%.0f"),
                "units_45d": st.column_config.NumberColumn(
                    "45d units", format="%.0f"),
                "OnHand": st.column_config.NumberColumn(
                    "On Hand", format="%.0f"),
                "Available": st.column_config.NumberColumn(
                    "Available", format="%.0f"),
                "OnOrder": st.column_config.NumberColumn(
                    "On Order", format="%.0f"),
                "Target stock": st.column_config.NumberColumn(
                    "Target", format="%.1f"),
                "Buildable": st.column_config.NumberColumn(
                    "Buildable", format="%.0f"),
            },
        )
        st.download_button(
            "⬇ Download all finishing SKUs CSV",
            data=all_view.drop(
                columns=["Image", "Include?"], errors="ignore"
            ).to_csv(index=False),
            file_name="finishing_work_orders_all.csv",
            mime="text/csv",
            use_container_width=True,
        )
