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

import math
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
        # 2026-09-04 (James): whole units only — round UP so the batch
        # always covers the window (you can't build 0.3 of a part).
        suggested = float(math.ceil(max(0.0, target_for_window - on_hand) - 1e-9))

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
            "Suggested batch": int(suggested),
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

    default_idx = 0
    if active_id:
        for label, did in opt_to_id.items():
            if did == active_id:
                default_idx = opts.index(label)
                break
    picked = st.selectbox(
        "Active order", opts, index=default_idx,
        key="fablab_draft_picker",
        help="Pick an existing order, or leave on 'No active order' and "
             "create one from the ticked items below the table.")
    new_id = opt_to_id.get(picked)
    if new_id != active_id:
        st.session_state[state_key] = new_id
        st.rerun()
    active_id = new_id

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


def _render_place_order(draft_id: int, bom_parents: dict,
                        product_map: dict, actor: str) -> None:
    """Place the order: one AUTHORISED CIN7 assembly per line + one
    labor-only Draft PO to 865FabLab (fablab_assemblies.place_order).
    Preview first, then explicit confirm."""
    import db
    import fablab_assemblies as fa

    saved = db.get_po_draft_lines(draft_id)
    if not saved:
        return

    show_key = f"fablab_place_show_{draft_id}"
    if st.button("\U0001f680 Place order with 865FabLab",
                 key=f"fablab_place_btn_{draft_id}", type="primary"):
        st.session_state[show_key] = True
    if not st.session_state.get(show_key):
        return

    with st.expander(f"Place order #{draft_id}?", expanded=True):
        st.markdown(
            "**What happens:** one CIN7 assembly (Finished Goods, "
            "*authorised*) per SKU below — CIN7 builds each pick list from "
            "the BOM — plus one Draft PO to 865FabLab for "
            f"`{fa.LABOR_SKU}` × total units. When that PO is authorised "
            "in CIN7, each assembly is posted to the 865 corner channel "
            "and the Odoo lead + quote are created.")
        lines = {k: _num(v) for k, v in saved.items() if _num(v) > 0}
        per_sku, totals = fa.build_pick_list(lines, bom_parents, product_map)
        total_units = sum(lines.values())
        st.dataframe(pd.DataFrame([
            {"SKU": r["sku"], "Name": r["name"], "Qty": int(r["qty"]),
             "Components (BOM)": ", ".join(
                 f"{c} × {t:g}" for c, _n, t in r["components"])}
            for r in per_sku]), use_container_width=True, hide_index=True)
        st.caption(
            f"Labor PO line: {fa.LABOR_SKU} × {total_units:g} at the "
            "865FabLab fixed price in CIN7.")
        with st.expander("PO memo / pick list preview"):
            st.code(fa.format_pick_list(per_sku, totals), language=None)

        confirm = st.checkbox(
            "I've checked the quantities — create the assemblies and the "
            "labor PO now", key=f"fablab_place_confirm_{draft_id}")
        c1, c2 = st.columns([1, 1])
        with c1:
            go = st.button("Confirm — place order",
                           key=f"fablab_place_go_{draft_id}",
                           type="primary", disabled=not confirm)
        with c2:
            if st.button("Cancel", key=f"fablab_place_cancel_{draft_id}"):
                st.session_state[show_key] = False
                st.rerun()
        if go:
            with st.spinner("Creating assemblies and labor PO in CIN7…"):
                res = fa.place_order(
                    draft_id, bom_parents, product_map, actor=actor,
                    apply=True)
            for w in res.get("warnings", []):
                st.warning(w)
            if res.get("ok"):
                st.success(
                    f"Placed. Labor PO **{res['po_number']}** created in "
                    "CIN7 (DRAFT — authorise it there to notify 865FabLab). "
                    "Assemblies: "
                    + ", ".join(f"{a['assembly_number']} ({a['sku']} × "
                                f"{_num(a['qty']):g})"
                                for a in res["assemblies"]))
                st.session_state[show_key] = False
                st.rerun()
            else:
                for e in res.get("errors", []):
                    st.error(e)


@st.cache_data(ttl=60, show_spinner=False)
def _preview_completion(assembly_id: int, qty: float) -> dict:
    import fablab_assemblies as fa
    return fa.complete_assembly(assembly_id, qty_received=qty,
                                actor="preview", apply=False)


def _render_assembly_receiving(draft_id: int, actor: str) -> bool:
    """Per-assembly receiving for orders placed through the assembly
    flow. Returns True if the order has assemblies (legacy fallback
    otherwise)."""
    import db
    import fablab_assemblies as fa

    rows = db.list_fablab_assemblies(draft_id)
    if not rows:
        return False

    status_df = pd.DataFrame([{
        "Assembly": r["assembly_number"] or "—",
        "SKU": r["sku"],
        "Qty": _num(r["quantity"]),
        "Status": r["status"],
        "Received": _num(r["completed_qty"]),
        "Slack": "posted" if r["slack_ts"] else "—",
    } for r in rows])
    done = sum(1 for r in rows if r["status"] == "completed")
    st.markdown(f"**Assemblies — {done} of {len(rows)} received**")
    st.dataframe(status_df, use_container_width=True, hide_index=True)
    st.caption(
        "Normal path: reply `done` (or `done 35`) in the assembly's Slack "
        "thread. Use the form below only if Slack isn't handy.")

    open_rows = [r for r in rows if r["status"] == "authorised"]
    if not open_rows:
        return True
    labels = {f"{r['assembly_number']} · {r['sku']} × {_num(r['quantity']):g}": r
              for r in open_rows}
    pick = st.selectbox("Complete an assembly", list(labels),
                        key=f"fablab_recv_pick_{draft_id}")
    a = labels[pick]
    qty = st.number_input(
        "Quantity received", min_value=1, max_value=int(_num(a["quantity"])),
        value=int(_num(a["quantity"])), step=1,
        key=f"fablab_recv_qty_{a['id']}")
    preview = _preview_completion(a["id"], float(qty))
    if preview["errors"]:
        for e in preview["errors"]:
            st.error(e)
        return True
    pick_df = pd.DataFrame([
        {"Component": p["component"], "BOM qty": p["bom_qty"],
         "Actual used": p["actual_qty"]} for p in preview["picks"]])
    edited = st.data_editor(
        pick_df, key=f"fablab_recv_picks_{a['id']}_{qty}",
        disabled=["Component", "BOM qty"], hide_index=True,
        use_container_width=True,
        column_config={"Actual used": st.column_config.NumberColumn(
            min_value=0.0, help="Set to what was really consumed "
            "(offcut used → 0).")})
    if st.button(f"✅ Complete {a['assembly_number']}",
                 key=f"fablab_recv_go_{a['id']}", type="primary"):
        overrides = {row["Component"]: _num(row["Actual used"])
                     for _, row in edited.iterrows()}
        with st.spinner("Completing in CIN7…"):
            r = fa.complete_assembly(a["id"], qty_received=float(qty),
                                     pick_overrides=overrides,
                                     actor=actor, apply=True)
        for w in r.get("warnings", []):
            st.warning(w)
        if r["ok"]:
            msg = f"{a['assembly_number']} completed — {qty} into stock."
            if r.get("remainder"):
                msg += (f" Remainder {r['remainder']['qty']:g} → "
                        f"{r['remainder']['assembly_number']}.")
            if a["slack_ts"]:
                import fablab_slack
                fablab_slack.post(
                    f":white_check_mark: *{a['assembly_number']}* completed "
                    f"from the app by {actor} — {qty} × `{a['sku']}` into stock.",
                    channel_id=a["slack_channel"], thread_ts=a["slack_ts"])
            st.success(msg)
            st.rerun()
        else:
            for e in r["errors"]:
                st.error(e)
    return True


def _render_bom_reality_check() -> None:
    import db

    rows = db.fablab_pick_variance_summary(min_batches=1)
    if not rows:
        return
    with st.expander("\U0001f9ee BOM reality check (actual vs BOM usage)"):
        st.caption(
            "From completed assemblies. If 'Actual per unit' keeps landing "
            "below the BOM, adjust the BOM in CIN7 so stock stops drifting.")
        df = pd.DataFrame([{
            "SKU": r["sku"], "Component": r["component_sku"],
            "Batches": int(r["batches"]),
            "Units built": _num(r["finished_units"]),
            "BOM per unit": round(_num(r["bom_total"]) /
                                  max(_num(r["finished_units"]), 1e-9), 3),
            "Actual per unit": round(_num(r["actual_total"]) /
                                     max(_num(r["finished_units"]), 1e-9), 3),
            "Variance %": round(
                (_num(r["actual_total"]) - _num(r["bom_total"])) /
                max(_num(r["bom_total"]), 1e-9) * 100, 1),
        } for r in rows])
        st.dataframe(df, use_container_width=True, hide_index=True)


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
        "When a batch comes back from 865FabLab: orders placed through "
        "the assembly flow are completed per assembly (Slack `done` reply "
        "or the form here); older orders use the finished-goods push."
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
    if _render_assembly_receiving(draft_id, actor):
        _render_bom_reality_check()
        return
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
        "materials, 865FabLab supplies labor.** Pick or create an order, "
        "tick Include on the SKUs you want (Batch qty is pre-filled with "
        "the suggestion — edit if needed), save, check raw materials, "
        "place the order. Each SKU becomes a "
        "CIN7 assembly; when 865FabLab hands a batch back, reply `done` "
        "in its Slack thread and stock updates itself.",
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

    # ── Plan & order (one section, 2026-09-04 per James) ─────────────────
    # The planner table IS the order: pick/create an order, adjust the
    # "Batch qty" column (pre-filled with the suggested batch, or with
    # the quantities already saved on the order), then save. No separate
    # "order lines" editor to copy into.
    import db

    st.markdown("### \U0001f4cb Plan & order")
    draft_id, can_edit, is_submitted = _render_draft_lifecycle(current_user)
    saved_lines: dict = db.get_po_draft_lines(draft_id) if draft_id else {}

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
            help="Hide SKUs with no suggested batch and nothing on the order.")
        pretick_all = st.checkbox(
            "Pre-tick all suggested", value=False,
            key=f"fablab_pretick_{draft_id or 'none'}",
            help="Tick every SKU with a suggested batch. Otherwise tick "
                 "the ones you want by hand in the Include column.")
    with pc3:
        search = st.text_input(
            "Search SKU, name, or rule", key="fablab_planner_search")

    planner_df = build_planner_table(
        flagged_skus, products, stock, engine_df, bom_parents, weeks_cover)
    if planner_df.empty:
        st.warning("No data for flagged SKUs.")
        return

    planner_df = planner_df.copy()
    # Batch qty = what is on the order if saved, else the suggestion.
    # Ticks made before an order existed (or before saving) are remembered
    # in the session so creating/switching an order does not lose them.
    remembered: dict = st.session_state.get("fablab_ticked", {})
    planner_df["Batch qty"] = [
        float(saved_lines.get(
            sku, remembered.get(sku, sug if pd.notna(sug) else 0)))
        for sku, sug in zip(planner_df["SKU"], planner_df["Suggested batch"])
    ]
    # Include = on the order. Saved or remembered ticks are ticked;
    # everything else starts unticked unless "Pre-tick all suggested" is on.
    planner_df["Include"] = [
        (sku in saved_lines) or (sku in remembered)
        or (pretick_all and pd.notna(sug) and float(sug) > 0)
        for sku, sug in zip(planner_df["SKU"], planner_df["Suggested batch"])
    ]
    cols = ["Include"] + [c for c in planner_df.columns if c != "Include"]
    planner_df = planner_df[cols]

    view = planner_df
    if action_only:
        view = view[(view["Suggested batch"].fillna(0) > 0)
                    | (view["Batch qty"].fillna(0) > 0)]
    if search:
        q = search.strip()
        mask = pd.Series(False, index=view.index)
        for col in ("SKU", "Name", "BOM rule"):
            mask |= view[col].astype(str).str.contains(
                q, case=False, na=False)
        view = view[mask]
    view = view.copy()

    qty_editable = (draft_id is None) or can_edit
    if draft_id and is_submitted:
        st.caption("This order has already been placed — quantities are "
                   "read-only.")
    elif draft_id and not can_edit:
        st.caption("Take the lock above to edit quantities.")
    edited_planner = st.data_editor(
        view,
        key=f"fablab_planner_editor_{draft_id or 'none'}_{int(pretick_all)}",
        use_container_width=True,
        hide_index=True,
        disabled=[c for c in view.columns
                  if c not in ("Batch qty", "Include") or not qty_editable],
        column_config={
            "Include": st.column_config.CheckboxColumn(
                "✔ Include", help="Tick to put this SKU on the order."),
            "On hand": st.column_config.NumberColumn(format="%.1f"),
            "Monthly demand": st.column_config.NumberColumn(format="%.2f"),
            "Suggested batch": st.column_config.NumberColumn(
                format="%d", help="Rounded up to whole units."),
            "Buildable from stock": st.column_config.NumberColumn(format="%.1f"),
            "Batch qty": st.column_config.NumberColumn(
                "✏ Batch qty (order)", format="%d", step=1, min_value=0,
                help="What goes on the 865FabLab order. Pre-filled with the "
                     "suggested batch; edit, then save."),
        },
    )

    included = edited_planner[edited_planner["Include"].fillna(False)
                              .astype(bool)]
    if qty_editable:
        # Remember ticks for visible rows; keep remembered ticks for rows
        # hidden by the search/filter.
        visible = set(edited_planner["SKU"])
        remembered = {k: v for k, v in remembered.items() if k not in visible}
        remembered.update(
            {r["SKU"]: float(_num(r.get("Batch qty", 0)))
             for _, r in included.iterrows()})
        st.session_state["fablab_ticked"] = remembered
    n_short = int((included["Materials status"] == "Raw short").sum())
    mcol1, mcol2, mcol3 = st.columns(3)
    mcol1.metric("Flagged SKUs", fmt_number(len(planner_df)))
    mcol2.metric(
        "On this order",
        f"{fmt_number(included['Batch qty'].fillna(0).sum())} units · "
        f"{len(included)} SKUs")
    mcol3.metric("Raw short (included)", fmt_number(n_short))

    def _save_ticks(target_draft: int) -> int:
        n = 0
        for _, row in edited_planner.iterrows():
            qty = _num(row.get("Batch qty", 0))
            if bool(row.get("Include", False)) and qty > 0:
                db.upsert_po_draft_line(
                    target_draft, row["SKU"], qty, current_user)
                n += 1
            elif row["SKU"] in saved_lines:
                db.delete_po_draft_line(target_draft, row["SKU"])
        st.session_state["fablab_ticked"] = {}
        return n

    n_ticked = len(included)
    if draft_id is None:
        if n_ticked == 0:
            st.info("**Step 1** — tick the SKUs to build in the ✔ Include "
                    "column above, then name and create the order.",
                    icon="\U0001f4cb")
        else:
            st.info(f"**Step 2** — {n_ticked} SKU(s) ticked. Name the order "
                    "and create it; the ticked items go straight onto it.",
                    icon="\U0001f4e6")
        nc1, nc2 = st.columns([3, 2])
        with nc1:
            new_name = st.text_input(
                "Order name", key="fablab_quick_order_name",
                placeholder="e.g. September corner batch")
        with nc2:
            st.write("")
            label = ("\U0001f4e6 Create order with "
                     f"{n_ticked} ticked item(s)" if n_ticked
                     else "\U0001f4e6 Create empty order")
            if st.button(label, key="fablab_quick_create", type="primary",
                         disabled=not new_name.strip()):
                new_draft = db.create_po_draft(
                    supplier=FABLAB_SUPPLIER, name=new_name.strip(),
                    actor=current_user)
                n = _save_ticks(new_draft)
                st.session_state["fablab_active_draft"] = new_draft
                st.success(f"Order #{new_draft} created with {n} SKU(s).")
                st.rerun()
    elif can_edit:
        if not saved_lines:
            st.info("**Step 2** — tick items, then save them to this order.",
                    icon="\U0001f4e6")
        if st.button("\U0001f4be Save ticked items to this order",
                     key=f"fablab_save_{draft_id}", type="primary",
                     disabled=(n_ticked == 0 and not saved_lines)):
            n = _save_ticks(draft_id)
            st.success(f"Saved {n} SKU(s) to the order.")
            st.rerun()

    if draft_id and saved_lines:
        with st.expander(
                f"Lines saved on this order ({len(saved_lines)} SKUs)"):
            st.dataframe(pd.DataFrame([
                {"SKU": sku,
                 "Name": product_map.get(sku, {}).get("Name", ""),
                 "Qty": qty} for sku, qty in saved_lines.items()]),
                use_container_width=True, hide_index=True)
    if draft_id and can_edit and saved_lines:
        st.info("**Step 3** — review below and place the order with "
                "865FabLab.", icon="\U0001f680")
        _render_place_order(draft_id, bom_parents, product_map, current_user)

    # ── Materials shortfall ──────────────────────────────────────────────
    st.divider()
    st.markdown("### \U0001f9f1 Materials shortfall")
    # Base qty for every flagged SKU (including any hidden by the
    # "Action needed only" / search filters above) is its batch qty;
    # only override with an edit for rows currently visible in the
    # editor -- otherwise filtering the view would silently drop
    # hidden SKUs' raw-material needs from the rollup below.
    def _order_qtys(df: pd.DataFrame) -> dict:
        inc = df["Include"].fillna(False).astype(bool)
        return dict(zip(df["SKU"],
                        df["Batch qty"].fillna(0).where(inc, 0.0)))
    batch_qtys = _order_qtys(planner_df)
    batch_qtys.update(_order_qtys(edited_planner))
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
