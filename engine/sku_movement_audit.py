"""SKU movement audit helpers for bulk-roll demand rollups.

These helpers stay Streamlit-free so the dashboard, bot, and tests can
all inspect the same movement evidence without reaching into UI code.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

import pandas as pd

from engine.sku_rules import _is_strip_sku, _parse_strip_base


EXCLUDED_SALE_STATUSES = {"CREDITED", "VOIDED", "CANCELLED"}


def _empty_audit(reason: str) -> dict[str, Any]:
    return {
        "ok": False,
        "reason": reason,
        "base": "",
        "master_length_m": 0.0,
        "selected_length_m": 0.0,
        "family_rows": pd.DataFrame(),
        "recent_rows": pd.DataFrame(),
        "summary": {},
    }


def _clean_sale_lines(sale_lines_df: pd.DataFrame | None) -> pd.DataFrame:
    """Return sale lines with safe date/quantity columns and status filter."""
    sl = sale_lines_df.copy() if sale_lines_df is not None else pd.DataFrame()
    if sl.empty:
        sl = pd.DataFrame()
    for col in ("SKU", "InvoiceDate", "OrderDate", "Quantity", "Status"):
        if col not in sl.columns:
            sl[col] = pd.NA
    sl["InvoiceDate"] = pd.to_datetime(sl["InvoiceDate"], errors="coerce")
    sl["OrderDate"] = pd.to_datetime(sl["OrderDate"], errors="coerce")
    sl["Quantity"] = pd.to_numeric(sl["Quantity"], errors="coerce").fillna(0)
    sl["SKU"] = sl["SKU"].astype(str)
    if "Status" in sl.columns:
        sl = sl[~sl["Status"].astype(str).str.upper().isin(
            EXCLUDED_SALE_STATUSES)]
    return sl


def calendar_month_periods(today=None, periods: int = 12) -> list[pd.Period]:
    """Return oldest-to-newest calendar-month periods ending this month."""
    now = pd.Timestamp(today if today is not None else datetime.now().date())
    return list(pd.period_range(end=now.to_period("M"),
                                periods=int(periods),
                                freq="M"))


def build_sku_sales_audit(sku: str,
                          sale_lines_df: pd.DataFrame,
                          *,
                          today=None,
                          months: int = 6) -> dict[str, Any]:
    """Audit exact-SKU movement by calendar month.

    The ABC engine counts invoiced demand by `InvoiceDate`. Buyers often
    sanity-check against sales/orders they remember by `OrderDate`. This
    audit shows both so a current-month zero can be explained without
    exporting CSVs.
    """
    sku_s = str(sku or "").strip()
    if not sku_s:
        return {
            "ok": False,
            "reason": "No SKU supplied.",
            "monthly_rows": pd.DataFrame(),
            "recent_rows": pd.DataFrame(),
            "summary": {},
        }

    sl = _clean_sale_lines(sale_lines_df)
    sku_lines = sl[sl["SKU"].astype(str) == sku_s].copy()
    periods = calendar_month_periods(today=today, periods=months)
    labels = [str(p) for p in periods]
    current_period = periods[-1] if periods else pd.Timestamp(
        datetime.now().date()).to_period("M")

    rows = []
    for period in periods:
        inv_mask = sku_lines["InvoiceDate"].dt.to_period("M") == period
        order_mask = sku_lines["OrderDate"].dt.to_period("M") == period
        # Order-date rows that are not in the same invoice month are useful
        # for "I know we sold this" checks. They are not engine demand until
        # they have a counted InvoiceDate in the period.
        not_counted_mask = order_mask & ~inv_mask
        rows.append({
            "Month": str(period),
            "Invoice qty (engine)": float(
                sku_lines.loc[inv_mask, "Quantity"].sum()),
            "OrderDate qty": float(
                sku_lines.loc[order_mask, "Quantity"].sum()),
            "OrderDate not in invoice month": float(
                sku_lines.loc[not_counted_mask, "Quantity"].sum()),
            "Invoice lines": int(inv_mask.sum()),
            "OrderDate lines": int(order_mask.sum()),
        })
    monthly_rows = pd.DataFrame(rows)

    current_inv = float(monthly_rows.iloc[-1]["Invoice qty (engine)"]
                        if not monthly_rows.empty else 0)
    current_order = float(monthly_rows.iloc[-1]["OrderDate qty"]
                          if not monthly_rows.empty else 0)
    current_not_counted = float(
        monthly_rows.iloc[-1]["OrderDate not in invoice month"]
        if not monthly_rows.empty else 0)

    recent_cols = [c for c in [
        "InvoiceDate", "OrderDate", "SKU", "Customer", "Quantity", "SaleID",
        "OrderNumber", "InvoiceNumber", "Status",
    ] if c in sku_lines.columns]
    recent_rows = sku_lines.sort_values(
        ["InvoiceDate", "OrderDate"], ascending=False,
        na_position="last").head(30).copy()
    if not recent_rows.empty:
        recent_rows = recent_rows[recent_cols].copy()
        for date_col in ("InvoiceDate", "OrderDate"):
            if date_col in recent_rows.columns:
                recent_rows[date_col] = pd.to_datetime(
                    recent_rows[date_col], errors="coerce").dt.date.astype(str)

    return {
        "ok": True,
        "reason": "",
        "monthly_rows": monthly_rows,
        "recent_rows": recent_rows,
        "summary": {
            "sku": sku_s,
            "months": labels,
            "current_month": str(current_period),
            "current_invoice_qty": current_inv,
            "current_order_qty": current_order,
            "current_order_not_in_invoice_month_qty": current_not_counted,
            "total_invoice_qty": float(
                monthly_rows["Invoice qty (engine)"].sum()
                if not monthly_rows.empty else 0),
            "last_invoice_date": (
                sku_lines["InvoiceDate"].max().date().isoformat()
                if not sku_lines.empty
                and pd.notna(sku_lines["InvoiceDate"].max()) else "—"
            ),
            "last_order_date": (
                sku_lines["OrderDate"].max().date().isoformat()
                if not sku_lines.empty
                and pd.notna(sku_lines["OrderDate"].max()) else "—"
            ),
        },
    }


def build_sku_current_month_movement(
        sku: str,
        sale_lines_df: pd.DataFrame | None,
        assemblies_df: pd.DataFrame | None = None,
        *,
        today=None) -> dict[str, Any]:
    """Return current-calendar-month movement for one SKU.

    Direct sales are counted from sale_lines.InvoiceDate, matching the
    ABC engine. Component demand consumed through finished-goods builds is
    counted from assemblies.ComponentSKU using CompletionDate/Date.
    """
    sku_s = str(sku or "").strip()
    if not sku_s:
        return {
            "ok": False,
            "period": "",
            "direct_invoice_qty": 0.0,
            "assembly_qty": 0.0,
            "total_qty": 0.0,
        }

    period = calendar_month_periods(today=today, periods=1)[-1]

    sl = _clean_sale_lines(sale_lines_df)
    sku_lines = sl[sl["SKU"].astype(str) == sku_s].copy()
    direct_qty = 0.0
    if not sku_lines.empty:
        inv_period = sku_lines["InvoiceDate"].dt.to_period("M")
        direct_qty = float(
            sku_lines.loc[inv_period == period, "Quantity"].sum())

    assembly_qty = 0.0
    a = assemblies_df.copy() if assemblies_df is not None else pd.DataFrame()
    if not a.empty:
        for col in ("ComponentSKU", "CompletionDate", "Date",
                    "Quantity", "Status"):
            if col not in a.columns:
                a[col] = pd.NA
        comp = a[a["ComponentSKU"].astype(str) == sku_s].copy()
        if not comp.empty:
            status = comp["Status"].astype(str).str.upper()
            comp = comp[~status.isin(
                {"VOIDED", "CANCELLED", "CANCELED", "DRAFT"})]
            completed = pd.to_datetime(
                comp["CompletionDate"], errors="coerce")
            fallback = pd.to_datetime(comp["Date"], errors="coerce")
            comp["_movement_date"] = completed.fillna(fallback)
            comp["Quantity"] = pd.to_numeric(
                comp["Quantity"], errors="coerce").fillna(0)
            comp_period = comp["_movement_date"].dt.to_period("M")
            assembly_qty = float(
                comp.loc[comp_period == period, "Quantity"].sum())

    return {
        "ok": True,
        "period": str(period),
        "direct_invoice_qty": direct_qty,
        "assembly_qty": assembly_qty,
        "total_qty": direct_qty + assembly_qty,
    }


def build_strip_movement_audit(sku: str,
                               products_df: pd.DataFrame,
                               sale_lines_df: pd.DataFrame,
                               *,
                               today=None,
                               window_days: int = 365) -> dict[str, Any]:
    """Return family movement evidence for an LED strip bulk/cut SKU.

    Quantities are normalised to the selected/master roll length. For a
    100m master roll, 40m of child-cut demand is shown as 0.40 rolls.
    """
    sku_s = str(sku or "").strip()
    parsed = _parse_strip_base(sku_s)
    if not sku_s or not parsed:
        return _empty_audit("SKU is not a recognised LED strip length SKU.")
    base, selected_len = parsed
    if selected_len <= 0:
        return _empty_audit("SKU length could not be parsed.")

    product_names: dict[str, str] = {}
    family: dict[str, float] = {sku_s: float(selected_len)}

    if products_df is not None and not products_df.empty and "SKU" in products_df.columns:
        for _, row in products_df.iterrows():
            row_sku = str(row.get("SKU") or "").strip()
            if not row_sku:
                continue
            name = str(row.get("Name") or "")
            product_names[row_sku] = name
            if not _is_strip_sku(row_sku, name):
                continue
            row_parse = _parse_strip_base(row_sku)
            if not row_parse:
                continue
            row_base, row_len = row_parse
            if row_base == base and row_len > 0:
                family[row_sku] = float(row_len)

    if sale_lines_df is not None and not sale_lines_df.empty and "SKU" in sale_lines_df.columns:
        for row_sku in sale_lines_df["SKU"].dropna().astype(str).unique():
            row_parse = _parse_strip_base(row_sku)
            if row_parse and row_parse[0] == base and row_parse[1] > 0:
                family[str(row_sku)] = float(row_parse[1])

    if not family:
        return _empty_audit("No matching strip family SKUs found.")

    master_len = max(family.values()) if family else selected_len
    # For 50m/100m masters, display in master-roll equivalents. If a
    # smaller cut SKU is inspected directly, still normalise to the
    # largest family roll so the audit answers "what should we buy?"
    normalise_len = master_len if master_len >= selected_len else selected_len

    sl = _clean_sale_lines(sale_lines_df)
    if sl.empty or "SKU" not in sl.columns:
        sl = pd.DataFrame(columns=[
            "SKU", "InvoiceDate", "Quantity", "Customer", "CustomerID",
            "Status", "SaleID", "OrderNumber", "InvoiceNumber",
        ])

    now = pd.Timestamp(today if today is not None else datetime.now().date())
    cutoff_365 = now - pd.Timedelta(days=window_days)
    cutoff_90 = now - pd.Timedelta(days=90)
    cutoff_45 = now - pd.Timedelta(days=45)

    family_skus = set(family.keys())
    family_sl = sl[sl["SKU"].astype(str).isin(family_skus)].copy()
    family_sl = family_sl.dropna(subset=["InvoiceDate"])
    in_window = family_sl[family_sl["InvoiceDate"] >= cutoff_365].copy()

    rows = []
    for row_sku, length_m in sorted(
            family.items(), key=lambda item: (-item[1], item[0])):
        sku_lines = family_sl[family_sl["SKU"].astype(str) == row_sku]
        sku_12 = sku_lines[sku_lines["InvoiceDate"] >= cutoff_365]
        sku_90 = sku_lines[sku_lines["InvoiceDate"] >= cutoff_90]
        sku_45 = sku_lines[sku_lines["InvoiceDate"] >= cutoff_45]
        units_12 = float(sku_12["Quantity"].sum())
        units_90 = float(sku_90["Quantity"].sum())
        units_45 = float(sku_45["Quantity"].sum())
        metres_12 = units_12 * length_m
        rows.append({
            "SKU": row_sku,
            "Role": (
                "selected" if row_sku == sku_s
                else "family master" if length_m == master_len
                else "child/cut"
            ),
            "Name": product_names.get(row_sku, "")[:80],
            "Length m": length_m,
            "12mo qty": units_12,
            "90d qty": units_90,
            "45d qty": units_45,
            "12mo metres": metres_12,
            "Master roll equiv": (
                metres_12 / normalise_len if normalise_len else 0.0),
            "Last sale": (
                sku_lines["InvoiceDate"].max().date().isoformat()
                if not sku_lines.empty
                and pd.notna(sku_lines["InvoiceDate"].max()) else "—"
            ),
            "Customers 12mo": (
                sku_12["CustomerID"].nunique()
                if "CustomerID" in sku_12.columns
                else sku_12.get("Customer", pd.Series(dtype=object)).nunique()
            ),
        })

    family_rows = pd.DataFrame(rows)
    direct_row = family_rows[family_rows["SKU"] == sku_s]
    direct_rolls = (
        float(direct_row["Master roll equiv"].sum())
        if not direct_row.empty else 0.0)
    total_rolls = float(family_rows["Master roll equiv"].sum())
    child_rolls = max(0.0, total_rolls - direct_rolls)
    total_metres = float(family_rows["12mo metres"].sum())

    top_customer = "—"
    top_customer_rolls = 0.0
    top_customer_pct = 0.0
    if not in_window.empty:
        in_window["_length_m"] = in_window["SKU"].astype(str).map(family)
        in_window["_master_roll_equiv"] = (
            in_window["Quantity"] * in_window["_length_m"] / normalise_len
            if normalise_len else 0.0)
        cust_col = "Customer"
        if "Customer" not in in_window.columns:
            cust_col = "CustomerID" if "CustomerID" in in_window.columns else ""
        if cust_col:
            cust = (in_window.groupby(cust_col)["_master_roll_equiv"]
                    .sum().sort_values(ascending=False))
            if not cust.empty:
                top_customer = str(cust.index[0])
                top_customer_rolls = float(cust.iloc[0])
                top_customer_pct = (
                    top_customer_rolls / total_rolls * 100.0
                    if total_rolls else 0.0)

    recent_cols = [c for c in [
        "InvoiceDate", "SKU", "Customer", "Quantity", "SaleID",
        "OrderNumber", "InvoiceNumber", "Status",
    ] if c in family_sl.columns]
    recent_rows = family_sl.sort_values(
        "InvoiceDate", ascending=False).head(30).copy()
    if not recent_rows.empty:
        recent_rows["_length_m"] = recent_rows["SKU"].astype(str).map(family)
        recent_rows["Master roll equiv"] = (
            recent_rows["Quantity"] * recent_rows["_length_m"] / normalise_len
            if normalise_len else 0.0)
        recent_cols = [c for c in recent_cols if c in recent_rows.columns]
        recent_rows = recent_rows[
            recent_cols + ["Master roll equiv"]].copy()
        recent_rows["InvoiceDate"] = pd.to_datetime(
            recent_rows["InvoiceDate"], errors="coerce").dt.date.astype(str)

    return {
        "ok": True,
        "reason": "",
        "base": base,
        "master_length_m": float(normalise_len),
        "selected_length_m": float(selected_len),
        "family_rows": family_rows,
        "recent_rows": recent_rows,
        "summary": {
            "family_sku_count": len(family_rows),
            "direct_master_rolls_12mo": direct_rolls,
            "child_master_rolls_12mo": child_rolls,
            "total_master_rolls_12mo": total_rolls,
            "total_metres_12mo": total_metres,
            "top_customer": top_customer,
            "top_customer_rolls_12mo": top_customer_rolls,
            "top_customer_pct_12mo": top_customer_pct,
            "last_family_sale": (
                in_window["InvoiceDate"].max().date().isoformat()
                if not in_window.empty
                and pd.notna(in_window["InvoiceDate"].max()) else "—"
            ),
        },
    }
