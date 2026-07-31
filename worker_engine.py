"""worker_engine.py (v2.67.69)
=================================

Slim engine intelligence for the Slack bot worker.

Why this exists
---------------
The web service runs `_abc_engine()` in `app.py` to compute ABC class,
trend_flag, is_dormant, excess_units, and other derived signals. That
output drives the Streamlit dashboard. The Slack bot worker, running
on a separate Render service with its own /data disk, doesn't have
access to that engine output (Render disks are exclusive per service).

Result before v2.67.69: bot's answers about "slow movers", "overstock",
"ABC class" were based on a slim products+stock merge with NONE of the
engine signals. Inconsistent with what staff see in the dashboard.

This module replicates the **headline engine signals** so the worker's
listener can answer slow-mover / overstock / dormancy questions
correctly. Faithful to the dashboard for the most common questions;
some edge cases (A-class grace, multi-tier dormancy refinement) are
simplified.

Drift sources (will fix in v2.67.70 with shared Postgres):
- No A-class grace check (worker may flag A-class as dormant when
  dashboard wouldn't)
- No physical-unit Tier-1 dormancy floor for bulk rolls (dashboard
  compares meters of strip flow; worker is purely rate-based)
- Bulk/strip rollup is BOM-first with a naming fallback; still less rich
  than the dashboard engine but no longer direct-sales-only.
- No buyer manual corrections (those live in web service's DB)

Kept in sync with the dashboard (app.py's _abc_engine):
- Real FG assembly-task consumption (assemblies_last_*.csv) feeds
  effective_units_12mo/90d directly, same as app.py's v2.67.334 fix.
- Assembly-consumption dormancy grace — a SKU with any real assembly
  draw in the last 90 days is never flagged dormant, same as app.py's
  v2.67.374 fix.
- units_45d / units_prior_45d — own-SKU direct+assembly demand for the
  last 45 days / the 45-90-days-ago window, same windows as app.py's
  u45/uprior. PO commentary's ai_tools.get_purchase_order/get_purchase_live
  read this straight off engine_df; without it, every worker PO
  commentary silently read units_45d as 0 for every SKU (v2.67.377).

Still-open drift (units_12mo raw / lineage_units_12mo are not
distinguished from effective_units_12mo here — only affects the rarer
"moved historically but engine isn't reordering from it" PO-commentary
rule, not the primary units_45d recency signal above).

Output columns added to engine_df:
- ABC                       'A' | 'B' | 'C'
- effective_units_12mo      float — 12-month demand in SKU units
- effective_units_90d       float — 90-day demand
- units_45d                 float — own-SKU demand, last 45 days
- units_prior_45d           float — own-SKU demand, 45-90 days ago
- annual_value              effective_units_12mo × AverageCost
- is_dormant                bool — 12mo activity > 0 AND 90d ≤ 20% of 12mo rate
- excess_units              max(0, OnHand - effective_units_12mo)
- excess_value              excess_units × AverageCost
- OnHandValue               OnHand × AverageCost (fallback for stock value)
- trend_flag                'Stable' | '📈 Trend' | '📉 Decline' | '(unknown)'
- is_non_master_tube        bool — heuristic: SKU has per-foot suffix

Public API:
  compute_engine_signals(products, stock, sale_lines, boms=None)
  -> pd.DataFrame
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd

from engine.sku_rules import _is_strip_sku, _parse_strip_base
from engine.sku_rules import is_bulk_strip_roll_length
from engine.sku_rules import parse_sourcing_rule
from sales_exclusions import filter_excluded_sales_customers
from storage_dimensions import ensure_storage_dim_column

log = logging.getLogger("worker_engine")

_STOCK_LOCATOR_COLUMNS = (
    "StockLocator",
    "Stock Locator",
    "Stock locator",
    "stock_locator",
)


def _to_num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")


def _normalise_bin_aliases(df: pd.DataFrame) -> pd.DataFrame:
    """Populate Bin from CIN7's Stock locator field only."""
    bin_cols: list[str] = []
    for col in _STOCK_LOCATOR_COLUMNS:
        # Prefer unsuffixed values, then pandas merge suffixes. These
        # candidates are still strictly Stock locator fields; never use
        # Default location / warehouse Location as a shelf code.
        for candidate in (
            col,
            f"{col}_stock",
            f"{col}_y",
            f"{col}_product",
            f"{col}_x",
        ):
            if candidate in df.columns and candidate not in bin_cols:
                bin_cols.append(candidate)
    if not bin_cols:
        if "Bin" in df.columns:
            df["Bin"] = ""
        return df

    def _first_locator(row) -> str:
        for col in bin_cols:
            val = row.get(col)
            if val is None or pd.isna(val):
                continue
            text = str(val).strip()
            if text and text.lower() not in {"nan", "none", "null"}:
                return text
        return ""

    df["Bin"] = df.apply(_first_locator, axis=1)
    return df


def _is_discontinued(name: object, status: object) -> bool:
    text = f"{name or ''} {status or ''}".lower()
    status_s = str(status or "").strip().lower()
    return (
        "[discontinued]" in text
        or "discontinued" in text
        or status_s in {"discontinued", "inactive", "obsolete"}
    )


def compute_engine_signals(products: pd.DataFrame,
                              stock: pd.DataFrame,
                              sale_lines: pd.DataFrame,
                              boms: Optional[pd.DataFrame] = None,
                              assemblies: Optional[pd.DataFrame] = None
                              ) -> pd.DataFrame:
    """Compute the engine signals on the worker. Returns engine_df with
    one row per SKU and all derived columns."""
    if products is None or products.empty:
        return pd.DataFrame()

    # 1. Base merge: products + stock_on_hand on SKU.
    df = products.copy()
    ensure_storage_dim_column(df)
    df["SKU"] = df["SKU"].astype(str)

    if stock is not None and not stock.empty:
        stock_view = stock.copy()
        stock_view["SKU"] = stock_view["SKU"].astype(str)
        cols_to_pull = ["SKU"]
        # Pull only CIN7's Stock locator field for shelf position.
        # Do not use Location/Default location; that is the warehouse,
        # not the shelf locator staff are asking for.
        for c in ("OnHand", "OnOrder", "Available", "StockLocator",
                    "Stock Locator", "Stock locator", "stock_locator",
                    "StockOnHand"):
            if c in stock_view.columns:
                cols_to_pull.append(c)
        df = df.merge(stock_view[cols_to_pull].drop_duplicates(
            subset=["SKU"], keep="last"), on="SKU", how="left")
    df = _normalise_bin_aliases(df)

    # 2. Family resolution.
    if "AdditionalAttribute1" in df.columns:
        df["Family"] = df["AdditionalAttribute1"].fillna("")
    else:
        df["Family"] = ""

    # 3. is_non_master_tube — simple heuristic. Per-foot cuts and
    # short reels typically have SKU suffixes like -0305 (3.05ft),
    # -0610, -1015, etc. Master rolls are -100M / -50M / -5m. This
    # heuristic isn't perfect but catches ~90% of cases without
    # the BOM-based logic the web service uses.
    def _is_non_master(sku: str) -> bool:
        s = str(sku).upper()
        # Per-foot indicators
        if any(f"-{n}-" in s or s.endswith(f"-{n}")
                for n in ("0305", "0610", "0915", "1220", "1525",
                          "1830", "2135", "2440", "2745", "3050")):
            return True
        return False
    # 4. Compute 12mo + 90d demand from sale_lines.
    today_ts = pd.Timestamp(datetime.now().date())
    cutoff_12mo = today_ts - pd.Timedelta(days=365)
    cutoff_90d = today_ts - pd.Timedelta(days=90)
    cutoff_45d = today_ts - pd.Timedelta(days=45)

    eff_12mo_map: dict = {}
    eff_90d_map: dict = {}
    # v2.67.377 — units_45d / units_prior_45d, mirroring app.py's own
    # 45-day trend window (_abc_engine's u45/uprior). PO commentary's
    # "demand hierarchy" instructions (slack_listener.py) lead with
    # units_45d as the primary recency signal ("NEVER call dormant if
    # units_45d > 0") and get_purchase_order/get_purchase_live
    # (ai_tools.py) read it straight off engine_df — but this module
    # never populated the column, so it silently defaulted to 0 for
    # every SKU on every worker PO commentary. James 2026-07-30:
    # PO-7067 commentary read "Trend SKU with strong recent momentum"
    # (from trend_flag, which IS computed here) right next to "45-day
    # demand: 0 ft" — the contradiction was this missing field, not a
    # real absence of recent sales.
    units_45d_map: dict = {}
    units_prior_45d_map: dict = {}
    if sale_lines is not None and not sale_lines.empty:
        sl = filter_excluded_sales_customers(sale_lines).copy()
        if "SKU" in sl.columns and "Quantity" in sl.columns:
            sl["SKU"] = sl["SKU"].astype(str)
            sl["__qty"] = _to_num(sl["Quantity"]).fillna(0)
            # Date column — InvoiceDate or OrderDate.
            date_col = ("InvoiceDate" if "InvoiceDate" in sl.columns
                          else "OrderDate" if "OrderDate" in sl.columns
                          else None)
            if date_col:
                sl["__dt"] = pd.to_datetime(sl[date_col],
                                                errors="coerce", utc=True)
                # Drop rows we can't date.
                sl = sl.dropna(subset=["__dt"])
                # Convert to naive for cutoff comparison.
                sl["__dt"] = sl["__dt"].dt.tz_convert(None)

                eff_12mo_map = (sl[sl["__dt"] >= cutoff_12mo]
                                .groupby("SKU")["__qty"].sum().to_dict())
                eff_90d_map = (sl[sl["__dt"] >= cutoff_90d]
                                .groupby("SKU")["__qty"].sum().to_dict())
                units_45d_map = (sl[sl["__dt"] >= cutoff_45d]
                                  .groupby("SKU")["__qty"].sum().to_dict())
                units_prior_45d_map = (
                    sl[(sl["__dt"] >= cutoff_90d)
                       & (sl["__dt"] < cutoff_45d)]
                    .groupby("SKU")["__qty"].sum().to_dict())

    non_master_skus: set[str] = set()

    # 4a-2. REAL Finished-Goods assembly-task consumption
    # (assemblies_last_*d_*.csv) — actual per-task component pick
    # records, when available. Takes priority over the BOM-ratio
    # proxy below for whichever components it covers, since it's a
    # measurement of what was actually consumed, not an approximation
    # from "the assembly's own sales x BOM ratio".
    #
    # James 2026-07-30: Andrew flagged LED-SILICONE-PMT80050 (a BOM
    # component with zero direct sales) showing "12mo effective: 269
    # units" in PO commentary when real demand is ~250/month (~3000/
    # yr) -- a ~11x gap too large to explain by sale_lines staleness
    # alone. Root cause: the BOM-ratio proxy below assumes assembly
    # SOLD == assembly BUILT == component CONSUMED, which breaks down
    # whenever kits get built ahead of demand or components move
    # through other build paths. The real per-task consumption data
    # (already synced and already used elsewhere in this codebase,
    # e.g. the bot's current-month movement answers) is a direct
    # measurement and doesn't have that assumption. Mirrors app.py's
    # own CompletionDate-then-Date fallback and VOIDED/CANCELLED/
    # CANCELED/DRAFT exclusion for this exact data (see the per-SKU
    # demand-breakdown helper there).
    real_consumed_12mo: set[str] = set()
    assembly_units_90d_map: dict = {}
    if (assemblies is not None and not assemblies.empty
            and "ComponentSKU" in assemblies.columns
            and "Quantity" in assemblies.columns):
        asm_rows = assemblies.copy()
        asm_rows["ComponentSKU"] = asm_rows["ComponentSKU"].astype(str)
        asm_rows["__qty"] = _to_num(asm_rows["Quantity"]).fillna(0)
        _completed = (pd.to_datetime(asm_rows["CompletionDate"],
                                       errors="coerce", utc=True)
                       if "CompletionDate" in asm_rows.columns
                       else pd.Series(pd.NaT, index=asm_rows.index))
        _fallback = (pd.to_datetime(asm_rows["Date"],
                                      errors="coerce", utc=True)
                      if "Date" in asm_rows.columns
                      else pd.Series(pd.NaT, index=asm_rows.index))
        asm_rows["__dt"] = _completed.fillna(_fallback)
        asm_rows = asm_rows.dropna(subset=["__dt"])
        asm_rows["__dt"] = asm_rows["__dt"].dt.tz_convert(None)
        if "Status" in asm_rows.columns:
            _bad_asm_statuses = ("VOIDED", "CANCELLED", "CANCELED", "DRAFT")
            asm_rows = asm_rows[~asm_rows["Status"].astype(str).str.upper()
                                  .isin(_bad_asm_statuses)]
        real_12mo = (asm_rows[asm_rows["__dt"] >= cutoff_12mo]
                     .groupby("ComponentSKU")["__qty"].sum())
        real_90d = (asm_rows[asm_rows["__dt"] >= cutoff_90d]
                    .groupby("ComponentSKU")["__qty"].sum())
        real_45d = (asm_rows[asm_rows["__dt"] >= cutoff_45d]
                    .groupby("ComponentSKU")["__qty"].sum())
        real_prior_45d = (
            asm_rows[(asm_rows["__dt"] >= cutoff_90d)
                     & (asm_rows["__dt"] < cutoff_45d)]
            .groupby("ComponentSKU")["__qty"].sum())
        for comp, qty in real_12mo.items():
            eff_12mo_map[comp] = eff_12mo_map.get(comp, 0) + float(qty)
            real_consumed_12mo.add(comp)
        for comp, qty in real_90d.items():
            eff_90d_map[comp] = eff_90d_map.get(comp, 0) + float(qty)
        for comp, qty in real_45d.items():
            units_45d_map[comp] = units_45d_map.get(comp, 0) + float(qty)
        for comp, qty in real_prior_45d.items():
            units_prior_45d_map[comp] = (
                units_prior_45d_map.get(comp, 0) + float(qty))
        assembly_units_90d_map = real_90d.to_dict()

    # 4b. CIN7 BOM rollup. BOMs are the source of truth for assembly /
    # cut relationships: if SKU A is built from component B, demand on A
    # should plan B. Falls back to this ratio-based proxy only for
    # components NOT already covered by real assembly-task consumption
    # above (avoids double-counting whichever is more accurate).
    if boms is not None and not boms.empty:
        for _, row in boms.iterrows():
            asm = str(row.get("AssemblySKU") or "").strip()
            comp = str(row.get("ComponentSKU") or "").strip()
            if not asm or not comp or asm == comp:
                continue
            try:
                qty_per = float(row.get("Quantity") or 0)
            except (TypeError, ValueError):
                qty_per = 0.0
            if qty_per <= 0:
                continue
            u12 = float(eff_12mo_map.get(asm, 0))
            u90 = float(eff_90d_map.get(asm, 0))
            if not (u12 or u90):
                continue
            # The assembly's own demand has been (or would be)
            # explained by a component below — hide its own row
            # regardless of whether the proxy or real data ends up
            # being used for that component.
            non_master_skus.add(asm)
            if comp in real_consumed_12mo:
                continue
            eff_12mo_map[comp] = eff_12mo_map.get(comp, 0) + u12 * qty_per
            eff_90d_map[comp] = eff_90d_map.get(comp, 0) + u90 * qty_per

    # 4b-2. Exact purchase-pack rollup (base SKU -> final "-X<number>"
    # pack SKU, e.g. SNFX-M-WH-SET -> SNFX-M-WH-SET-X250). Mirrors
    # app.py's pack_rollup_rules exactly: only an unambiguous single
    # pack-SKU candidate per base, and only when the base SKU isn't
    # itself directly CIN7-supplier-assigned (which would mean it's
    # ordered independently, not through a pack).
    #
    # James 2026-07-23: Andrew (buyer) flagged SNFX-M-WH-SET-X250's
    # "effective 12mo demand" as wrong in the PO commentary bot (it
    # showed ~49.6; real annual demand is ~270 units, ~22/month).
    # Root cause: this rollup existed in app.py's dashboard engine
    # (v2.67.xxx "Exact purchase-pack demand rollup") but was never
    # ported here, so the bot was reading raw sale_lines quantity
    # against the literal pack SKU code (a purchasing-only SKU,
    # rarely if ever directly sold to a customer) instead of (base
    # SKU's real 12mo sales) ÷ pack size.
    #
    # Additive with the pack SKU's own raw units, same as app.py's
    # `tube_rollup_in` treatment of effective_units_12mo — if that
    # raw figure is itself noisy for a given pack SKU, that's a
    # pre-existing data-quality question shared with the dashboard,
    # not something this fix changes. Does NOT zero the base SKU's
    # own effective_units_12mo/90d (unlike the BOM/strip rollups
    # above) — app.py's own _effective_units() formula leaves the
    # base SKU's figure untouched by this rollup too; only a
    # separate Ordering-grid "is independently orderable" flag
    # (out of scope here — this module has no Ordering grid) is
    # affected there.
    try:
        from engine.sku_rules import parse_pack_purchase_sku
    except ImportError:  # pragma: no cover — defensive only
        parse_pack_purchase_sku = None

    if parse_pack_purchase_sku is not None and "SKU" in products.columns:
        product_skus = set(products["SKU"].astype(str))
        has_cin7_supplier: set[str] = set()
        if "Suppliers" in products.columns:
            for _, p in products.iterrows():
                sups_raw = p.get("Suppliers")
                if not sups_raw or sups_raw in ("[]", "None", None):
                    continue
                sups = sups_raw
                if isinstance(sups, str):
                    try:
                        import json as _json
                        sups = _json.loads(sups)
                    except (ValueError, TypeError):
                        continue
                if not isinstance(sups, list) or not sups:
                    continue
                if any(isinstance(s, dict) and s.get("SupplierName")
                        for s in sups):
                    has_cin7_supplier.add(str(p.get("SKU") or ""))

        pack_candidates_by_base: dict[str, list[tuple[str, int]]] = {}
        for _sku in product_skus:
            _pack = parse_pack_purchase_sku(_sku)
            if not _pack:
                continue
            _base_sku, _pack_size = _pack
            if _base_sku not in product_skus:
                continue
            pack_candidates_by_base.setdefault(_base_sku, []).append(
                (_sku, _pack_size))

        for _base_sku, _candidates in pack_candidates_by_base.items():
            if _base_sku in has_cin7_supplier:
                continue
            _supplier_candidates = [
                c for c in _candidates if c[0] in has_cin7_supplier]
            _usable = (_supplier_candidates if _supplier_candidates
                        else _candidates)
            if len(_usable) != 1:
                continue
            _pack_sku, _pack_size = _usable[0]
            if _pack_size <= 0:
                continue
            _own_u12 = float(eff_12mo_map.get(_base_sku, 0))
            _own_u90 = float(eff_90d_map.get(_base_sku, 0))
            if _own_u12 or _own_u90:
                eff_12mo_map[_pack_sku] = (
                    eff_12mo_map.get(_pack_sku, 0) + _own_u12 / _pack_size)
                eff_90d_map[_pack_sku] = (
                    eff_90d_map.get(_pack_sku, 0) + _own_u90 / _pack_size)

    # 4c. Naming fallback for BOM-sparse LED strip families. Pick the
    # largest active BULK buying roll as the master and convert child/cut
    # movement into master-roll equivalents. Short finished lengths must
    # not become masters; CIN7 BOMs remain the source of truth for those
    # relationships.
    bom_assemblies = set()
    if boms is not None and not boms.empty and "AssemblySKU" in boms.columns:
        bom_assemblies = set(boms["AssemblySKU"].dropna().astype(str))

    # v2.67.376 — require actual evidence of an assembly/cut relationship
    # before a same-family SKU can be hidden as a "non-master" cut. Mirrors
    # app.py's b0e8eb1 fix (v2.67.372): sharing a naming pattern with a
    # bigger sibling is NOT proof of a master/cut relationship — e.g.
    # LED-WLWW-30K-16-IP20-5 is an independently supplied 5m reel, not a
    # cut of the 25m roll, despite matching the naming pattern. Without
    # this guard the worker's PO commentary can zero out a real
    # best-seller's demand and flag it dormant.
    bom_flag_by_sku = {
        str(row.get("SKU") or ""): (
            str(row.get("BillOfMaterial")).lower() == "true")
        for _, row in products.iterrows()
    }
    rule_by_sku = {
        str(row.get("SKU") or ""): parse_sourcing_rule(
            row.get("AdditionalAttribute1"))
        for _, row in products.iterrows()
    }

    strip_families: dict[str, list[tuple[str, float, str, str]]] = {}
    for _, row in products.iterrows():
        sku_s = str(row.get("SKU") or "").strip()
        if not sku_s or sku_s in bom_assemblies:
            continue
        name = str(row.get("Name") or "")
        if not _is_strip_sku(sku_s, name):
            continue
        parsed = _parse_strip_base(sku_s)
        if not parsed:
            continue
        base, length_m = parsed
        if length_m <= 0:
            continue
        strip_families.setdefault(base, []).append(
            (sku_s, float(length_m), name, str(row.get("Status") or "")))

    for _, members in strip_families.items():
        if len(members) < 2:
            continue
        sorted_members = sorted(members, key=lambda item: -item[1])
        active_members = [
            m for m in sorted_members
            if not _is_discontinued(m[2], m[3])
        ]
        master_sku, master_len, _, _ = (active_members or sorted_members)[0]
        if not is_bulk_strip_roll_length(master_len):
            continue
        if master_len <= 0:
            continue
        for child_sku, child_len, _, _ in sorted_members:
            if child_sku == master_sku:
                continue
            # Only roll up (and hide) this SKU's own demand if there is
            # explicit evidence it's cut/assembled from the bulk master:
            # a CIN7 BOM, a BillOfMaterial=True flag, or a SourceFraction
            # sourcing rule. Otherwise it's a standalone purchased
            # product that only shares a naming pattern — leave its own
            # effective_units_12mo/90d alone.
            has_evidence = (
                child_sku in bom_assemblies
                or bom_flag_by_sku.get(child_sku, False)
                or rule_by_sku.get(child_sku, {}).get(
                    "SourceFraction") is not None
            )
            if not has_evidence:
                continue
            u12 = float(eff_12mo_map.get(child_sku, 0))
            u90 = float(eff_90d_map.get(child_sku, 0))
            if not (u12 or u90):
                continue
            qty_per = child_len / master_len
            eff_12mo_map[master_sku] = (
                eff_12mo_map.get(master_sku, 0) + u12 * qty_per)
            eff_90d_map[master_sku] = (
                eff_90d_map.get(master_sku, 0) + u90 * qty_per)
            non_master_skus.add(child_sku)

    df["is_non_master_tube"] = df["SKU"].apply(
        lambda s: _is_non_master(s) or s in non_master_skus)
    df["effective_units_12mo"] = df["SKU"].map(
        lambda s: 0.0 if s in non_master_skus
        else float(eff_12mo_map.get(s, 0)))
    df["effective_units_90d"] = df["SKU"].map(
        lambda s: 0.0 if s in non_master_skus
        else float(eff_90d_map.get(s, 0)))
    # v2.67.377 — own-SKU raw demand, NOT zeroed for non-master cuts
    # (mirrors app.py's plain units_45d/units_prior_45d columns): staff
    # asking about a specific per-foot cut SKU still want to see ITS
    # OWN recent sales, distinct from the master roll's rolled-up
    # effective figure.
    df["units_45d"] = df["SKU"].map(
        lambda s: float(units_45d_map.get(s, 0)))
    df["units_prior_45d"] = df["SKU"].map(
        lambda s: float(units_prior_45d_map.get(s, 0)))

    # 5. ABC classification — pragmatic 3-tier:
    # A = top 20% by annual_value AMONG SKUs with any 12mo sales
    # B = has sales but not in top 20%
    # C = no sales in last 12mo (the largest bucket usually)
    # This split matches the buyer's mental model better than a
    # rank-based split when many SKUs are zero-velocity.
    avg_cost = (_to_num(df.get("AverageCost", pd.Series(0)))
                  .fillna(0))
    df["AverageCost"] = avg_cost
    df["annual_value"] = df["effective_units_12mo"] * df["AverageCost"]
    sellers = df[df["effective_units_12mo"] > 0]
    if not sellers.empty:
        val_q80 = float(sellers["annual_value"].quantile(0.80))
    else:
        val_q80 = float("inf")  # no sellers → nobody is A
    df["ABC"] = df.apply(
        lambda r: "A" if (r["effective_units_12mo"] > 0
                            and r["annual_value"] >= val_q80)
        else "B" if r["effective_units_12mo"] > 0
        else "C", axis=1)

    # 6. is_dormant — three-tier rule:
    #   (a) Has stock but ZERO sales in 12mo → dormant (the
    #       'never sold / stale stock' case the buyer cares
    #       about most).
    #   (b) Has sales but recent 90d rate is <20% of expected
    #       → dormant ('demand fell off a cliff' case).
    #   (c) Otherwise → not dormant.
    # Note: web service's full engine has A-class grace + bulk-
    # master rollup. Drift acceptable for v2.67.69; v2.67.70
    # Postgres migration will make worker read the dashboard's
    # canonical is_dormant value.
    #
    # v2.67.374 (app.py) / mirrored here — assembly-consumption grace.
    # A bulk raw-material SKU actively drawn down by FG assembly tasks
    # in the last 90 days is manufacturing stock in use, not dormant,
    # even if its rate-based check below would otherwise flag it (real
    # assembly draws are lumpy). Keeps the bot's dormancy answers
    # consistent with the dashboard for the same underlying signal.
    def _dormant(row) -> bool:
        on_hand = float(row.get("OnHand") or 0)
        e12 = float(row["effective_units_12mo"] or 0)
        e90 = float(row["effective_units_90d"] or 0)
        if assembly_units_90d_map.get(str(row.get("SKU") or ""), 0) > 0:
            return False
        # Tier (a): stock-with-no-sales-history.
        if e12 == 0 and on_hand > 0:
            return True
        # Tier (b): demand collapsed.
        if e12 > 0:
            expected_90d = e12 * (90.0 / 365.0)
            if e90 < (0.20 * expected_90d):
                return True
        return False
    df["is_dormant"] = df.apply(_dormant, axis=1)

    # 7. OnHandValue — for excess + stock-value displays. Use
    # AverageCost × OnHand. Fallback to 0 when cost is missing.
    on_hand = _to_num(df.get("OnHand", pd.Series(0))).fillna(0)
    df["OnHand"] = on_hand
    df["OnHandValue"] = on_hand * avg_cost

    # 8. excess_units / excess_value.
    # excess = max(0, OnHand - effective_units_12mo)
    # i.e. units held beyond the past year's full demand.
    df["excess_units"] = (on_hand - df["effective_units_12mo"]
                            ).clip(lower=0)
    df["excess_value"] = df["excess_units"] * avg_cost

    # 9. trend_flag — simplified. Compares 90d rate vs 12mo rate.
    # Real engine has 5 categories and uses monthly buckets; this
    # version is binary 'rising' / 'falling' / 'stable'.
    def _trend(row) -> str:
        e12 = float(row["effective_units_12mo"] or 0)
        e90 = float(row["effective_units_90d"] or 0)
        if e12 <= 0:
            return "(no history)"
        rate_12mo_daily = e12 / 365.0
        rate_90d_daily = e90 / 90.0
        if rate_12mo_daily < 0.01:
            return "Stable"
        ratio = rate_90d_daily / rate_12mo_daily
        if ratio >= 1.50:
            return "📈 Trend"
        if ratio <= 0.50:
            return "📉 Decline"
        return "Stable"
    df["trend_flag"] = df.apply(_trend, axis=1)

    log.info(
        "Worker engine computed: %d SKUs, %d A-class, %d dormant, "
        "%d with excess",
        len(df),
        int((df["ABC"] == "A").sum()),
        int(df["is_dormant"].sum()),
        int((df["excess_units"] > 0).sum()))

    # Keep storage_dim present even when older products snapshots only
    # have CIN7's raw "Storage L x W x H In" additional attribute column.
    ensure_storage_dim_column(df)

    return df
