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

from engine.sku_rules import parse_corner_bom_rule

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

        rule = parse_corner_bom_rule(
            prod.get("AdditionalAttribute1"), prod.get("AdditionalAttribute2"))

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
            "BOM rule": rule["RuleCode"] if rule else "",
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


# ── Corner BOM rules (build instructions) ───────────────────────────────

def _render_bom_rule_instructions(
        flagged_skus: list[str], product_map: dict) -> None:
    """Build instructions grouped by rule code (SR200/SR201/SR202), not by
    SKU — the instructions text is identical for every SKU sharing a rule,
    so a per-SKU expander (one per flagged SKU) just repeats the same few
    paragraphs N times. Grouping collapses this to at most 3 expanders
    regardless of how many SKUs are flagged."""
    by_rule: dict[str, dict] = {}
    unrecognized: list[tuple[str, str]] = []
    for sku in flagged_skus:
        prod = product_map.get(sku, {})
        rule = parse_corner_bom_rule(
            prod.get("AdditionalAttribute1"), prod.get("AdditionalAttribute2"))
        if rule:
            entry = by_rule.setdefault(rule["RuleCode"], {
                "name": rule["Name"], "instructions": rule["Instructions"],
                "skus": []})
            entry["skus"].append((sku, prod.get("Name") or ""))
        else:
            unrecognized.append((sku, prod.get("Name") or ""))

    if not by_rule:
        st.caption(
            "No flagged SKU has a recognized SR2xx rule code yet — set "
            "AdditionalAttribute2 (rule code) and AdditionalAttribute1 "
            "(rule name) in CIN7 to see build instructions here.")
        return

    for rule_code in sorted(by_rule):
        entry = by_rule[rule_code]
        with st.expander(
                f"{rule_code}: {entry['name']} ({len(entry['skus'])} SKUs)"):
            for i, step in enumerate(entry["instructions"], start=1):
                st.markdown(f"{i}. {step}")
            st.dataframe(
                pd.DataFrame(entry["skus"], columns=["SKU", "Name"]),
                use_container_width=True, hide_index=True)

    if unrecognized:
        with st.expander(
                f"⚠ {len(unrecognized)} SKU(s) with no recognized rule"):
            st.dataframe(
                pd.DataFrame(unrecognized, columns=["SKU", "Name"]),
                use_container_width=True, hide_index=True)


# ── Build-list manager (flags) ─────────────────────────────────────────

def _get_flagged_skus() -> list[str]:
    import db
    flag_rows = [f for f in db.list_flags(active_only=True)
                 if f["flag_type"] == FABLAB_FLAG_TYPE]
    return sorted({r["sku"] for r in flag_rows})


def _render_build_list_manager(
        products: pd.DataFrame, actor: str, product_map: dict) -> None:
    """Search/add + a compact, filterable, bulk-removable grid — replaces
    the old one-row-plus-button-per-SKU list, which became unusable once
    the build list passed 100+ SKUs."""
    import db

    st.caption(
        "Flag which finished SKUs are toll-manufactured at 865FabLab. "
        "This is manually curated — CIN7 has no supplier set for these "
        "yet, and they span many unrelated OEM product families."
    )

    flag_rows = [f for f in db.list_flags(active_only=True)
                 if f["flag_type"] == FABLAB_FLAG_TYPE]
    flagged_skus = sorted({r["sku"] for r in flag_rows})
    id_by_sku = {r["sku"]: r["id"] for r in flag_rows}

    search = st.text_input("Search SKU or name to add", key="fablab_add_search")
    if search and products is not None and not products.empty:
        q = search.strip()
        mask = (
            products["SKU"].astype(str).str.contains(q, case=False, na=False)
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

    st.markdown(f"**Currently flagged ({len(flagged_skus)}):**")
    filter_q = st.text_input(
        "Filter flagged list", key="fablab_flagged_filter",
        placeholder="Filter by SKU, name, or rule code")
    grid_rows = []
    for sku in flagged_skus:
        prod = product_map.get(sku, {})
        rule = parse_corner_bom_rule(
            prod.get("AdditionalAttribute1"), prod.get("AdditionalAttribute2"))
        grid_rows.append({
            "SKU": sku,
            "Name": prod.get("Name") or "",
            "Rule": rule["RuleCode"] if rule else "",
            "Remove?": False,
        })
    grid_df = pd.DataFrame(grid_rows)
    if filter_q and not grid_df.empty:
        q = filter_q.strip()
        mask = (
            grid_df["SKU"].str.contains(q, case=False, na=False)
            | grid_df["Name"].str.contains(q, case=False, na=False)
            | grid_df["Rule"].str.contains(q, case=False, na=False)
        )
        grid_df = grid_df[mask]

    if grid_df.empty:
        st.caption("No flagged SKUs match." if filter_q else "None flagged yet.")
        return

    edited = st.data_editor(
        grid_df, key="fablab_build_list_editor", use_container_width=True,
        hide_index=True, disabled=["SKU", "Name", "Rule"],
        column_config={"Remove?": st.column_config.CheckboxColumn("Remove?")})
    to_remove = edited[edited["Remove?"] == True]  # noqa: E712
    if st.button(f"\U0001f5d1 Remove selected ({len(to_remove)})",
                 disabled=to_remove.empty, key="fablab_remove_selected"):
        for sku in to_remove["SKU"]:
            flag_id = id_by_sku.get(sku)
            if flag_id:
                db.clear_flag(flag_id, actor)
        st.success(f"Removed {len(to_remove)} SKU(s).")
        st.rerun()


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


def _render_push_po_to_cin7(draft_id: int, actor: str) -> None:
    """Push this draft to CIN7 as a real Draft Advanced Purchase, using
    the same validated flow as the main Ordering page
    (cin7_post_po.push_po_draft) -- resolves SKUs to CIN7 ProductIDs,
    stops at CIN7 status DRAFT (never auto-authorises; a human reviews
    in CIN7 first). Only usable now that 865FabLab is a real CIN7
    supplier (2026-09-01) -- before that, CIN7's supplier resolution
    would have failed with 'not found'."""
    import db

    saved = db.get_po_draft_lines(draft_id)
    if not saved:
        return

    st.markdown("#### \U0001f4e4 Push order to CIN7")
    st.caption(
        "Creates a real Draft Purchase in CIN7 for supplier 865FabLab "
        "-- stays in DRAFT status, never auto-authorised. Review "
        "pricing in CIN7 before authorising: these SKUs don't have a "
        "per-unit 865FabLab labor cost set on their Supplier link yet, "
        "so CIN7 will fall back to each product's AverageCost, which "
        "is likely NOT what 865FabLab actually charges for the build.")

    show_key = f"fablab_po_push_show_{draft_id}"
    if st.button("\U0001f4e4 Push to CIN7 as Draft PO",
                 key=f"fablab_po_push_btn_{draft_id}"):
        st.session_state[show_key] = True
    if not st.session_state.get(show_key):
        return

    with st.expander(f"Push order #{draft_id} to CIN7?", expanded=True):
        dry_run = st.checkbox(
            "Dry-run (validate + preview only, don't post)",
            value=True, key=f"fablab_po_push_dry_{draft_id}")
        ack = st.checkbox(
            "I understand this creates a real Draft PO in CIN7 and "
            "will review/fix pricing there before authorising.",
            key=f"fablab_po_push_ack_{draft_id}")
        if st.button("Confirm push", type="primary",
                     disabled=(not ack) and not dry_run,
                     key=f"fablab_po_push_confirm_{draft_id}"):
            from cin7_post_po import push_po_draft
            with st.spinner("Talking to CIN7 — this can take "
                             "10-60 seconds..."):
                result = push_po_draft(
                    draft_id, actor=actor, apply=not dry_run,
                    require_mov=False)
            if result.ok:
                if dry_run:
                    st.success(
                        f"Dry-run passed at stage `{result.stage}`. "
                        "Uncheck dry-run and confirm again to actually "
                        "push.")
                    with st.expander("Lines that would be sent"):
                        st.json(
                            (result.order_response or {}).get(
                                "lines", []))
                else:
                    st.success(
                        f"CIN7 PO **#{result.cin7_po_number}** created "
                        "in DRAFT status. Review and AUTHORISE in "
                        "CIN7 to send to 865FabLab.")
                    st.session_state[show_key] = False
                    st.rerun()
            else:
                st.error(f"Push did not complete. Stopped at stage "
                         f"`{result.stage}`.")
                for err in result.errors:
                    st.error(f"• {err}")
            for warning in result.warnings:
                st.warning(warning)


def _render_finished_goods_push(
        draft_id: int, bom_parents: dict, actor: str) -> None:
    """Push a received 865FabLab order to CIN7 as Finished Goods
    (Assembly) tasks -- one per finished SKU line. CIN7 auto-consumes
    the product's own linked BOM and adds the finished stock; this also
    means the batch shows up in the ABC engine's ground-truth assembly-
    consumption demand signal for the raw materials, not just a stock
    change. Status is always COMPLETED (James's explicit choice,
    2026-08-27) -- the safety checkpoint is entirely in this UI: a
    button opens a confirmation expander (nothing happens on the first
    click), an ESTIMATED consumption preview is shown (CIN7 has no
    dry-run for this endpoint, so this is our own BOM estimate, not a
    guarantee), then an explicit acknowledgement gates the one real
    "Confirm push" button."""
    import db
    from cin7_post_finishedgoods import push_finished_goods, estimate_consumption

    st.markdown("#### \U0001f680 Push to CIN7 as Finished Goods")

    lines = db.get_po_draft_lines(draft_id)
    pushed_skus = {
        r["sku"] for r in db.list_fablab_finished_goods_pushes(draft_id)
        if r["status"] == "pushed"
    }
    remaining = {s: q for s, q in lines.items() if s not in pushed_skus}

    if pushed_skus:
        done_rows = [
            r for r in db.list_fablab_finished_goods_pushes(draft_id)
            if r["status"] == "pushed"
        ]
        st.success(
            "Already pushed: " + ", ".join(
                f"**{r['sku']}** (CIN7 {r['cin7_assembly_number']})"
                for r in done_rows))
    if not remaining:
        return

    show_key = f"fablab_fg_show_{draft_id}"
    if st.button("\U0001f680 Push remaining line(s) to CIN7",
                 key=f"fablab_fg_btn_{draft_id}"):
        st.session_state[show_key] = True

    if not st.session_state.get(show_key):
        return

    with st.expander(f"Push order #{draft_id} to CIN7?", expanded=True):
        st.caption(
            "Creates a Finished Goods task in CIN7 for each SKU below — "
            "COMPLETED immediately: CIN7 consumes the product's own "
            "linked BOM and adds the finished stock in one step. The "
            "estimate below is OUR calculation from the same BOM data "
            "the planner uses — CIN7 computes the actual consumption "
            "from its own linked BOM at push time, which is "
            "authoritative and may differ slightly if a BOM sync is "
            "stale.")
        for sku, qty in remaining.items():
            est = estimate_consumption(sku, qty, bom_parents)
            st.markdown(f"**{sku}** × {qty:g}")
            if est:
                st.dataframe(pd.DataFrame(est), use_container_width=True,
                             hide_index=True)
            else:
                st.caption("No BOM components found for this SKU.")

        ack = st.checkbox(
            "I've checked the estimate above and understand this "
            "creates real, COMPLETED Finished Goods tasks in CIN7 — "
            "stock changes immediately, no further review step there.",
            key=f"fablab_fg_ack_{draft_id}")
        if st.button("Confirm push", type="primary", disabled=not ack,
                     key=f"fablab_fg_confirm_{draft_id}"):
            with st.spinner("Posting to CIN7…"):
                results = push_finished_goods(
                    draft_id, bom_parents, actor=actor, apply=True)
            any_ok = False
            for r in results:
                if r.ok:
                    any_ok = True
                    st.success(
                        f"{r.sku}: pushed — CIN7 assembly "
                        f"**{r.cin7_assembly_number}**.")
                else:
                    for err in r.errors:
                        st.error(err)
            if any_ok:
                st.session_state[show_key] = False
                st.rerun()


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
    _render_finished_goods_push(draft_id, bom_parents, actor)


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
    product_map = _rows_by_sku(products)
    flagged_skus = _get_flagged_skus()

    if not flagged_skus:
        st.info(
            "No SKUs flagged yet — open **⚙️ Manage build list** below to "
            "add one (e.g. LED-UNI-TILE12-180-FLAT270) to start planning.")
        with st.expander("⚙️ Manage build list (0 SKUs)"):
            _render_build_list_manager(products, current_user, product_map)
        return

    # ── Production planner (top — this is what a buyer opens the page for) ──
    st.markdown("### \U0001f4cb Production planner")
    pc1, pc2, pc3 = st.columns([2, 2, 3])
    with pc1:
        weeks_cover = st.number_input(
            "Weeks of cover per batch", min_value=1.0, max_value=12.0,
            value=6.0, step=1.0, key="fablab_weeks_cover",
            help="Suggested batch qty tops up on-hand stock to this many "
                 "weeks of forecast demand — default ~6 weeks.")
    with pc2:
        action_only = st.checkbox(
            "Action needed only", value=True, key="fablab_action_only",
            help="Hide SKUs with no suggested batch this cycle.")
    with pc3:
        search = st.text_input(
            "Search SKU, name, or rule", key="fablab_planner_search")

    planner_df = build_planner_table(
        flagged_skus, products, stock, engine_df, bom_parents, weeks_cover)
    if planner_df.empty:
        st.warning("No data for flagged SKUs.")
        return

    view = planner_df
    if action_only:
        view = view[view["Suggested batch"].fillna(0) > 0]
    if search:
        q = search.strip()
        mask = pd.Series(False, index=view.index)
        for col in ("SKU", "Name", "BOM rule"):
            mask |= view[col].astype(str).str.contains(
                q, case=False, na=False)
        view = view[mask]

    view = view.copy()
    view["Batch qty"] = view["Suggested batch"]
    edited_planner = st.data_editor(
        view,
        key="fablab_planner_editor",
        use_container_width=True,
        hide_index=True,
        disabled=[c for c in view.columns if c != "Batch qty"],
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
    mcol1.metric("Flagged SKUs", fmt_number(len(planner_df)))
    mcol2.metric(
        "Units in this batch",
        fmt_number(edited_planner["Batch qty"].fillna(0).sum()))
    mcol3.metric("Raw short", fmt_number(n_short))

    # ── Materials shortfall ──────────────────────────────────────────────
    st.divider()
    st.markdown("### \U0001f9f1 Materials shortfall")
    # Base qty for every flagged SKU (including any hidden by the
    # "Action needed only" / search filters above) is its suggested
    # batch; only override with an edit for rows currently visible in
    # the editor -- otherwise filtering the view would silently drop
    # hidden SKUs' raw-material needs from the rollup below.
    batch_qtys = dict(
        zip(planner_df["SKU"], planner_df["Suggested batch"].fillna(0)))
    batch_qtys.update(
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

    # ── Place order with 865FabLab ───────────────────────────────────────
    st.divider()
    st.markdown("### \U0001f4e6 Place order with 865FabLab")
    draft_id, can_edit, _is_submitted = _render_draft_lifecycle(current_user)
    if draft_id:
        _render_line_editor(
            draft_id, can_edit, current_user, edited_planner, product_map)
        if can_edit:
            _render_push_po_to_cin7(draft_id, current_user)

    # ── Receiving checklist ──────────────────────────────────────────────
    st.divider()
    _render_receiving_checklist(bom_parents, product_map, current_user)

    # ── Admin: build list + instructions (collapsed, rarely touched) ────
    st.divider()
    with st.expander(f"⚙️ Manage build list ({len(flagged_skus)} SKUs)"):
        _render_build_list_manager(products, current_user, product_map)
    st.markdown("### \U0001f4dd Build instructions")
    st.caption("Grouped by rule — click a rule to see its steps and SKUs.")
    _render_bom_rule_instructions(flagged_skus, product_map)
