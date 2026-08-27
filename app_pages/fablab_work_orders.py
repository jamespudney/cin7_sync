"""865FabLab Production — toll-manufactured accessory corners.

Wired4Signs supplies raw materials (BOM components already in stock) and
865FabLab supplies the labor to assemble finished accessory SKUs (e.g.
corner pieces). CIN7 has no supplier set for these SKUs yet, and they span
many unrelated OEM product families, so there's no reliable auto-detection
signal — the build list is a manually curated flag (see FABLAB_FLAG_TYPE),
same mechanism as "Confirmed kit" on Kits & Fixtures.

Workflow: flag SKUs -> forecast a monthly batch -> confirm raw materials
on hand cover it -> place the order (tracked in po_drafts, same table the
Ordering page uses) -> once the batch physically returns, use the
receiving checklist for the manual CIN7 stock adjustment.
"""

from __future__ import annotations

from typing import Any, Optional

import pandas as pd
import streamlit as st

FABLAB_SUPPLIER = "865FabLab"
FABLAB_FLAG_TYPE = "865FabLab build"


# ── Helpers ──────────────────────────────────────────────────────────────

def _num(value: Any, default: float = 0.0) -> float:
    try:
        if pd.isna(value):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _stock_by_sku(stock: pd.DataFrame) -> dict[str, dict[str, float]]:
    if stock is None or stock.empty or "SKU" not in stock.columns:
        return {}
    df = stock.copy()
    for col in ("OnHand", "Available", "OnOrder"):
        if col not in df.columns:
            df[col] = 0
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    grouped = (
        df.groupby("SKU", dropna=False)
        .agg(OnHand=("OnHand", "sum"), Available=("Available", "sum"),
             OnOrder=("OnOrder", "sum"))
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


# ── Materials math ──────────────────────────────────────────────────────

def _buildable_from_stock(
    sku: str, bom_parents: dict, stock_map: dict[str, dict[str, float]],
) -> tuple[float, list[str], list[dict]]:
    """Max units of `sku` buildable from current raw-material stock.

    Returns (buildable, material description lines, component rows)."""
    components = bom_parents.get(sku, [])
    buildable_values: list[float] = []
    bits: list[str] = []
    for comp in components:
        comp_sku = str(comp.get("ComponentSKU") or "").strip()
        qty_per = _num(comp.get("Quantity"))
        if not comp_sku or qty_per <= 0:
            continue
        comp_stk = stock_map.get(comp_sku, {})
        comp_avail = _num(comp_stk.get("Available", comp_stk.get("OnHand", 0)))
        buildable_values.append(comp_avail / qty_per)
        bits.append(f"{comp_sku} × {qty_per:g} (have {comp_avail:g})")
    buildable = min(buildable_values) if buildable_values else 0.0
    return buildable, bits, components


def build_planner_table(
    flagged_skus: list[str],
    products: pd.DataFrame,
    stock: pd.DataFrame,
    engine_df: pd.DataFrame,
    bom_parents: dict,
    weeks_cover: float,
) -> pd.DataFrame:
    """One row per flagged SKU: demand, on-hand, suggested batch, and
    whether current raw-material stock covers that batch."""
    stock_map = _stock_by_sku(stock)
    engine_map = _rows_by_sku(engine_df)
    product_map = _rows_by_sku(products)

    rows: list[dict[str, Any]] = []
    for sku in flagged_skus:
        eng = engine_map.get(sku, {})
        prod = product_map.get(sku, {})
        stk = stock_map.get(sku, {})
        name = eng.get("Name") or prod.get("Name") or ""

        # v2.67.394 — real physical stock, not the engine's phantom/
        # derivable "Available" figure. For a BOM/assembly SKU,
        # "Available" reflects how many COULD be built from raw
        # materials on hand, not units actually sitting on a shelf —
        # using it here suppressed the suggested batch even when zero
        # real finished units existed (confirmed on
        # LED-UNI-TILE12-180-FLAT270: Product Detail's real stock
        # position showed OnHand 0 vs "Available" 76).
        on_hand = _num(eng.get("OnHand", stk.get("OnHand", 0)))
        units_12mo = _num(eng.get(
            "effective_units_12mo", eng.get("units_12mo", 0)))
        monthly_demand = units_12mo / 12.0
        target_for_window = monthly_demand * (weeks_cover / 4.345)
        suggested = max(0.0, target_for_window - on_hand)

        buildable, material_bits, components = _buildable_from_stock(
            sku, bom_parents, stock_map)

        if suggested <= 0:
            status = "No action"
        elif not components:
            status = "Check BOM"
        elif buildable + 1e-6 >= suggested:
            status = "Raw available"
        else:
            status = "Raw short"

        rows.append({
            "SKU": sku,
            "Name": name,
            "ABC": eng.get("ABC") or "",
            "Status": eng.get("Status") or "",
            "On hand": round(on_hand, 1),
            "Monthly demand": round(monthly_demand, 2),
            "Suggested batch": round(suggested, 1),
            "Buildable from stock": round(buildable, 1),
            "Materials status": status,
            "Materials": "\n".join(material_bits),
        })

    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values("Suggested batch", ascending=False)
    return df


def build_materials_rollup(
    flagged_skus: list[str],
    batch_qtys: dict[str, float],
    bom_parents: dict,
    stock_map: dict[str, dict[str, float]],
) -> pd.DataFrame:
    """Aggregate raw-material demand across ALL flagged SKUs at their
    chosen batch quantities. Catches shortfalls the per-SKU buildable
    check misses when two flagged SKUs share a raw component."""
    needed: dict[str, float] = {}
    for sku in flagged_skus:
        qty = batch_qtys.get(sku, 0.0)
        if qty <= 0:
            continue
        for comp in bom_parents.get(sku, []):
            comp_sku = str(comp.get("ComponentSKU") or "").strip()
            qty_per = _num(comp.get("Quantity"))
            if not comp_sku or qty_per <= 0:
                continue
            needed[comp_sku] = needed.get(comp_sku, 0.0) + qty * qty_per

    rows = []
    for comp_sku, need in needed.items():
        comp_stk = stock_map.get(comp_sku, {})
        avail = _num(comp_stk.get("Available", comp_stk.get("OnHand", 0)))
        rows.append({
            "Component": comp_sku,
            "Needed": round(need, 1),
            "On hand": round(avail, 1),
            "Short by": round(max(0.0, need - avail), 1),
        })
    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values("Short by", ascending=False)
    return df


# ── Build-list manager (flags) ─────────────────────────────────────────

def _render_build_list_manager(products: pd.DataFrame, actor: str) -> list[str]:
    import db

    st.markdown("### \U0001f527 865FabLab build list")
    st.caption(
        "Flag which finished SKUs are toll-manufactured at 865FabLab. "
        "This is manually curated — CIN7 has no supplier set for these "
        "yet, and they span many unrelated OEM product families."
    )

    flag_rows = [f for f in db.list_flags(active_only=True)
                 if f["flag_type"] == FABLAB_FLAG_TYPE]
    flagged_skus = sorted({r["sku"] for r in flag_rows})

    c1, c2 = st.columns(2)
    with c1:
        search = st.text_input(
            "Search SKU or name to add", key="fablab_add_search")
        if search and products is not None and not products.empty:
            q = search.strip()
            mask = (
                products["SKU"].astype(str).str.contains(
                    q, case=False, na=False)
                | products["Name"].astype(str).str.contains(
                    q, case=False, na=False)
            )
            matches = products[mask].head(15)
            for _, row in matches.iterrows():
                sku = str(row["SKU"])
                mc1, mc2 = st.columns([4, 1])
                mc1.write(f"**{sku}** — {row.get('Name', '')}")
                if sku in flagged_skus:
                    mc2.write("✓ added")
                elif mc2.button("+ Add", key=f"fablab_add_{sku}"):
                    db.set_flag(sku, FABLAB_FLAG_TYPE, actor)
                    st.rerun()
    with c2:
        st.write(f"**Currently flagged ({len(flagged_skus)}):**")
        for r in flag_rows:
            rc1, rc2 = st.columns([4, 1])
            rc1.write(r["sku"])
            if rc2.button("Remove", key=f"fablab_remove_{r['id']}"):
                db.clear_flag(r["id"], actor)
                st.rerun()

    return flagged_skus


# ── Order (po_drafts) lifecycle ─────────────────────────────────────────

def _render_draft_lifecycle(actor: str) -> tuple[Optional[int], bool, bool]:
    """865FabLab-scoped version of the Ordering page's draft lifecycle UI
    (same po_drafts/po_draft_lines tables, fixed supplier).

    Returns (active_draft_id, can_edit, is_submitted)."""
    import db

    drafts = db.list_po_drafts(supplier=FABLAB_SUPPLIER, include_archived=False)
    archived = [
        d for d in db.list_po_drafts(supplier=FABLAB_SUPPLIER, include_archived=True)
        if d["status"] in ("finalized", "cancelled")
    ]

    state_key = "fablab_active_draft"
    active_id = st.session_state.get(state_key)

    opts = ["— No active order —"]
    opt_to_id: dict[str, Optional[int]] = {opts[0]: None}
    for d in drafts:
        lock_str = ""
        if d["locked_by"]:
            lock_str = "  \U0001f513 you" if d["locked_by"] == actor \
                else f"  \U0001f512 {d['locked_by']}"
        emoji = {"editing": "\U0001f4dd", "submitted": "\U0001f4e4"}.get(
            d["status"], "\U0001f4c4")
        ref = f" → ref {d['cin7_po_number']}" if d["cin7_po_number"] else ""
        label = f"{emoji} {d['name']} ({d['status']}){ref}{lock_str}"
        opts.append(label)
        opt_to_id[label] = d["id"]

    st.markdown(
        f"**\U0001f4cb 865FabLab orders** — {len(drafts)} active"
        + (f", {len(archived)} archived" if archived else ""))

    c1, c2 = st.columns([3, 2])
    with c1:
        default_idx = 0
        if active_id:
            for label, did in opt_to_id.items():
                if did == active_id:
                    default_idx = opts.index(label)
                    break
        picked = st.selectbox(
            "Active order", opts, index=default_idx,
            key="fablab_draft_picker")
        new_id = opt_to_id.get(picked)
        if new_id != active_id:
            st.session_state[state_key] = new_id
            st.rerun()
        active_id = new_id
    with c2:
        with st.popover("➕ New order", use_container_width=True):
            name = st.text_input(
                "Name", key="fablab_new_name",
                placeholder="e.g. August corner batch")
            note = st.text_input("Note (optional)", key="fablab_new_note")
            if st.button("Create", key="fablab_new_create", type="primary",
                         disabled=not name.strip()):
                new_draft_id = db.create_po_draft(
                    supplier=FABLAB_SUPPLIER, name=name.strip(),
                    actor=actor, note=note)
                st.session_state[state_key] = new_draft_id
                st.success(f"Created order #{new_draft_id}")
                st.rerun()

    if not active_id:
        return None, False, False

    active = db.get_po_draft(active_id)
    if active is None:
        st.warning("Selected order no longer exists.")
        st.session_state[state_key] = None
        return None, False, False

    is_submitted = active["status"] != "editing"
    lock_holder = active["locked_by"]
    i_hold_lock = (lock_holder == actor)

    info_cols = st.columns([3, 1, 1, 1])
    with info_cols[0]:
        meta = (f"**{active['name']}** · status `{active['status']}` "
                f"· created {active['created_at']}")
        if active["cin7_po_number"]:
            meta += f" · ref **{active['cin7_po_number']}**"
        if is_submitted:
            meta += " \U0001f512 (read-only — already placed)"
        elif lock_holder and not i_hold_lock:
            meta += f" · \U0001f512 locked by **{lock_holder}**"
        elif i_hold_lock:
            meta += " · \U0001f513 you have the lock"
        else:
            meta += " · ⚠ unlocked — click 'Take lock' to edit"
        st.markdown(meta)
    with info_cols[1]:
        if not is_submitted and not i_hold_lock:
            if st.button("\U0001f511 Take lock", key=f"fablab_lock_{active_id}",
                         use_container_width=True):
                if db.lock_po_draft(active_id, actor):
                    st.rerun()
                else:
                    st.error(f"\U0001f512 {lock_holder} holds the lock.")
        elif not is_submitted and i_hold_lock:
            if st.button("\U0001f513 Release lock",
                         key=f"fablab_release_{active_id}",
                         use_container_width=True):
                db.release_po_draft_lock(active_id, actor)
                st.rerun()
    with info_cols[2]:
        if not is_submitted and i_hold_lock:
            with st.popover("\U0001f4e4 Mark placed", use_container_width=True):
                st.markdown("**Record this order as placed with 865FabLab**")
                st.caption(
                    "865FabLab isn't set up as a CIN7 supplier yet, so "
                    "this just records your own reference (invoice #, "
                    "email confirmation, etc.) and locks the order as "
                    "placed.")
                ref = st.text_input(
                    "Your reference (optional)", key=f"fablab_ref_{active_id}")
                if st.button("Confirm", key=f"fablab_submit_{active_id}",
                             type="primary"):
                    db.mark_po_draft_submitted(
                        active_id, actor, cin7_po_number=ref.strip())
                    st.success("Marked as placed.")
                    st.rerun()
    with info_cols[3]:
        if not is_submitted and i_hold_lock:
            with st.popover("\U0001f5d1️ Cancel", use_container_width=True):
                st.markdown("**Cancel this order?** Cannot be undone.")
                reason = st.text_input(
                    "Reason (optional)",
                    key=f"fablab_cancel_reason_{active_id}")
                if st.button("Confirm cancel", key=f"fablab_cancel_{active_id}"):
                    db.cancel_po_draft(active_id, actor, reason=reason)
                    st.session_state[state_key] = None
                    st.success("Cancelled.")
                    st.rerun()

    can_edit = (not is_submitted) and i_hold_lock
    return active_id, can_edit, is_submitted


def _render_line_editor(
    draft_id: int, can_edit: bool, actor: str,
    planner_df: pd.DataFrame, product_map: dict,
) -> None:
    import db

    st.markdown("#### Order lines")
    if can_edit and not planner_df.empty:
        if st.button("⬇ Fill from planner batch quantities",
                     key=f"fablab_fill_{draft_id}"):
            for _, row in planner_df.iterrows():
                qty = _num(row.get("Batch qty", 0))
                if qty > 0:
                    db.upsert_po_draft_line(draft_id, row["SKU"], qty, actor)
            st.success("Filled order lines from planner.")
            st.rerun()

    saved = db.get_po_draft_lines(draft_id)
    if not saved:
        st.caption("No lines yet.")
        return

    lines_df = pd.DataFrame([
        {"SKU": sku, "Name": product_map.get(sku, {}).get("Name", ""),
         "Qty": qty}
        for sku, qty in saved.items()
    ])
    if can_edit:
        edited = st.data_editor(
            lines_df, key=f"fablab_lines_editor_{draft_id}",
            disabled=["SKU", "Name"], use_container_width=True,
            hide_index=True)
        if st.button("\U0001f4be Save line quantities",
                     key=f"fablab_save_lines_{draft_id}"):
            for _, row in edited.iterrows():
                qty = _num(row["Qty"])
                if qty > 0:
                    db.upsert_po_draft_line(draft_id, row["SKU"], qty, actor)
                else:
                    db.delete_po_draft_line(draft_id, row["SKU"])
            st.success("Saved.")
            st.rerun()
    else:
        st.dataframe(lines_df, use_container_width=True, hide_index=True)


def _render_stock_adjustment_push(
        draft_id: int, bom_parents: dict, actor: str) -> None:
    """Push a received 865FabLab order to CIN7 as a DRAFT stock
    adjustment. Mirrors the Ordering page's PO push UX: a button opens a
    confirmation expander (nothing happens on the first click), the
    computed current -> new quantities are always shown (a read-only
    dry-run -- no POST) before an explicit acknowledgement gates the one
    real "Confirm push" button. CIN7 always receives Status=DRAFT -- a
    human still reviews/authorises in CIN7 before it affects the books."""
    import db
    from cin7_post_stockadjustment import push_stock_adjustment

    st.markdown("#### \U0001f680 Push adjustment to CIN7")

    existing = db.list_fablab_stock_adjustments(draft_id)
    pushed = [r for r in existing if r["status"] == "pushed"]
    if pushed:
        p = pushed[0]
        st.success(
            f"Already pushed — CIN7 stocktake **{p['cin7_stocktake_number']}** "
            f"(task {p['cin7_task_id']}) by {p['pushed_by']} on "
            f"{str(p['pushed_at'])[:16]}. It's a DRAFT in CIN7 — "
            "complete/authorise it there once you've verified it.")
        return

    show_key = f"fablab_adj_show_{draft_id}"
    if st.button("\U0001f680 Push receiving adjustment to CIN7",
                 key=f"fablab_adj_btn_{draft_id}"):
        st.session_state[show_key] = True

    if not st.session_state.get(show_key):
        return

    with st.expander(f"Push order #{draft_id} to CIN7?", expanded=True):
        st.caption(
            "Computes each SKU's LIVE current CIN7 quantity and the new "
            "total after this order's +finished/−materials adjustment, "
            "then posts a DRAFT stock adjustment to CIN7 — you (or your "
            "team) still review and authorise it there before it "
            "affects the books.")
        # Read-only preview: apply=False never POSTs, only the live GETs
        # needed to compute current -> new quantities. Safe to run every
        # time this expander is open so the numbers stay fresh.
        with st.spinner("Checking live CIN7 quantities…"):
            preview_result = push_stock_adjustment(
                draft_id, bom_parents, actor=actor, apply=False)

        if preview_result.preview:
            st.markdown("**Computed lines (current → new on-hand):**")
            st.dataframe(
                pd.DataFrame(preview_result.preview),
                use_container_width=True, hide_index=True,
                column_config={
                    "Current": st.column_config.NumberColumn(format="%.2f"),
                    "Delta": st.column_config.NumberColumn(format="%.2f"),
                    "New": st.column_config.NumberColumn(format="%.2f"),
                })

        if preview_result.errors:
            for err in preview_result.errors:
                st.error(err)
            return

        st.info(
            "Review the current → new quantities above — an "
            "unexpectedly large jump usually means the order quantity "
            "is wrong. Tick the box below once they look right.")
        ack = st.checkbox(
            "I've checked the quantities above and understand this "
            "creates a real DRAFT stock adjustment in CIN7. It still "
            "needs review/authorisation in CIN7 before it affects the "
            "books.",
            key=f"fablab_adj_ack_{draft_id}")
        if st.button("Confirm push", type="primary", disabled=not ack,
                     key=f"fablab_adj_confirm_{draft_id}"):
            with st.spinner("Posting to CIN7…"):
                real = push_stock_adjustment(
                    draft_id, bom_parents, actor=actor, apply=True)
            if real.ok:
                st.success(
                    f"Pushed — CIN7 stocktake "
                    f"**{real.cin7_stocktake_number}**. It's a DRAFT — "
                    "complete/authorise it in CIN7 once you've "
                    "verified it.")
                st.session_state[show_key] = False
                st.rerun()
            else:
                for err in real.errors:
                    st.error(err)


def _render_receiving_checklist(
        bom_parents: dict, product_map: dict, actor: str) -> None:
    import db

    st.markdown("### \U0001f4e5 Receiving checklist")
    st.caption(
        "When a batch physically comes back from 865FabLab, pick the "
        "order below for the exact numbers to enter in CIN7's manual "
        "stock adjustment: + finished SKU, − raw materials consumed."
    )
    placed = [
        d for d in db.list_po_drafts(supplier=FABLAB_SUPPLIER, include_archived=True)
        if d["status"] in ("submitted", "finalized")
    ]
    if not placed:
        st.info("No placed orders yet.")
        return

    opt_to_id = {f"{d['name']} (#{d['id']}, {d['status']})": d["id"] for d in placed}
    picked = st.selectbox(
        "Order", list(opt_to_id.keys()), key="fablab_receiving_picker")
    draft_id = opt_to_id[picked]
    lines = db.get_po_draft_lines(draft_id)
    if not lines:
        st.warning("This order has no lines.")
        return

    finished_rows = []
    consumed: dict[str, float] = {}
    for sku, qty in lines.items():
        finished_rows.append({
            "Finished SKU": sku,
            "Name": product_map.get(sku, {}).get("Name", ""),
            "Qty received (+)": qty,
        })
        for comp in bom_parents.get(sku, []):
            comp_sku = str(comp.get("ComponentSKU") or "").strip()
            qty_per = _num(comp.get("Quantity"))
            if comp_sku and qty_per > 0:
                consumed[comp_sku] = consumed.get(comp_sku, 0.0) + qty * qty_per

    st.markdown("**Finished stock — add:**")
    st.dataframe(pd.DataFrame(finished_rows), use_container_width=True,
                 hide_index=True)
    st.markdown("**Raw materials consumed — subtract:**")
    consumed_rows = [
        {"Component": k, "Name": product_map.get(k, {}).get("Name", ""),
         "Qty consumed (−)": round(v, 2)}
        for k, v in consumed.items()
    ]
    st.dataframe(pd.DataFrame(consumed_rows), use_container_width=True,
                 hide_index=True)

    st.divider()
    _render_stock_adjustment_push(draft_id, bom_parents, actor)


# ── Main render ──────────────────────────────────────────────────────────

def render_fablab_work_orders(
    *,
    products: pd.DataFrame,
    stock: pd.DataFrame,
    engine_df: pd.DataFrame,
    bom_parents: dict,
    fmt_number,
    fmt_money,
) -> None:
    st.header("\U0001f3ed 865FabLab Production")
    st.info(
        "**Toll-manufactured accessory corners — you supply raw "
        "materials, 865FabLab supplies labor.** Forecast a monthly "
        "batch, confirm you have enough raw material on hand, place the "
        "order, then use the receiving checklist below for the manual "
        "CIN7 stock adjustment once the batch comes back.",
        icon="ℹ️",
    )

    current_user = st.session_state.get("current_user", "").strip() or "anonymous"

    flagged_skus = _render_build_list_manager(products, current_user)
    st.divider()

    if not flagged_skus:
        st.info(
            "No SKUs flagged yet — add one above (e.g. "
            "LED-UNI-TILE12-180-FLAT270) to start planning.")
        return

    st.markdown("### \U0001f4cb Production planner")
    weeks_cover = st.number_input(
        "Weeks of cover per batch", min_value=1.0, max_value=12.0,
        value=6.0, step=1.0, key="fablab_weeks_cover",
        help="Suggested batch qty tops up on-hand stock to this many "
             "weeks of forecast demand — default ~6 weeks.")

    planner_df = build_planner_table(
        flagged_skus, products, stock, engine_df, bom_parents, weeks_cover)
    if planner_df.empty:
        st.warning("No data for flagged SKUs.")
        return

    planner_df["Batch qty"] = planner_df["Suggested batch"]
    edited_planner = st.data_editor(
        planner_df,
        key="fablab_planner_editor",
        use_container_width=True,
        hide_index=True,
        disabled=[c for c in planner_df.columns if c != "Batch qty"],
        column_config={
            "On hand": st.column_config.NumberColumn(format="%.1f"),
            "Monthly demand": st.column_config.NumberColumn(format="%.2f"),
            "Suggested batch": st.column_config.NumberColumn(format="%.1f"),
            "Buildable from stock": st.column_config.NumberColumn(format="%.1f"),
            "Batch qty": st.column_config.NumberColumn(
                "✏ Batch qty", format="%.1f",
                help="Edit to override the suggested batch quantity."),
        },
    )

    n_short = int((edited_planner["Materials status"] == "Raw short").sum())
    mcol1, mcol2, mcol3 = st.columns(3)
    mcol1.metric("Flagged SKUs", fmt_number(len(edited_planner)))
    mcol2.metric(
        "Units in this batch",
        fmt_number(edited_planner["Batch qty"].fillna(0).sum()))
    mcol3.metric("Raw short", fmt_number(n_short))

    st.divider()
    st.markdown("### \U0001f9f1 Materials shortfall")
    batch_qtys = dict(
        zip(edited_planner["SKU"], edited_planner["Batch qty"].fillna(0)))
    stock_map = _stock_by_sku(stock)
    rollup_df = build_materials_rollup(
        flagged_skus, batch_qtys, bom_parents, stock_map)
    if rollup_df.empty:
        st.success("No raw materials needed for the current batch quantities.")
    else:
        short_rollup = rollup_df[rollup_df["Short by"] > 0]
        if short_rollup.empty:
            st.success("Enough raw material on hand for this batch.")
        else:
            st.warning(
                f"Short on {len(short_rollup)} component(s) — order "
                "these before placing the 865FabLab batch:")
        st.dataframe(rollup_df, use_container_width=True, hide_index=True)

    st.divider()
    st.markdown("### \U0001f4e6 Place order with 865FabLab")
    draft_id, can_edit, _is_submitted = _render_draft_lifecycle(current_user)
    product_map = _rows_by_sku(products)
    if draft_id:
        _render_line_editor(
            draft_id, can_edit, current_user, edited_planner, product_map)

    st.divider()
    _render_receiving_checklist(bom_parents, product_map, current_user)
