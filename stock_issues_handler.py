"""stock_issues_handler.py (v2.67.144)
=========================================

Stock-issues tracker for #stock-issues-queries.

Design philosophy (per James):
  - Bot is a CONTEXT PROVIDER, not an answerer.
  - When a stock query lands, the bot replies with structured
    intelligence (SKU + bin + qty + allocations + ETA) for the
    stock controller to verify/correct — NOT a dispatch decision.
  - Querier gets a brief acknowledgment with caveats (e.g. "PO ETA
    looks unconfirmed — verify with @AndrewTunley").
  - If no thread reply within 4h, bot DMs Jamie Webb with the
    intelligence so accountability sticks to a specific person.
  - Morning summary lists outstanding issues so the team sees the
    pile-up.

Issue classification:
  - supply_query  — pre-dispatch supply question ('can we supply
                    SO-NNNNN?', 'how many can we ship?', 'what
                    are we short?'). Resolution = SO ships.
  - count_wrong   — discrepancy claim ('should be N, found M').
                    Resolution = stock_adjustments entry. (v1
                    skips auto-resolution; relies on the stock
                    controller replying 'fixed'.)
  - mixed         — both signals in same message.

CLI:
  python stock_issues_handler.py escalate
  python stock_issues_handler.py morning-summary [--dryrun]
  python stock_issues_handler.py inspect --issue-id N

Env vars:
  SLACK_BOT_TOKEN
  SLACK_STOCK_ISSUES_CHANNEL_ID     where queries land (C08NEMCEHNF)
  SLACK_STOCKKEEPER_DM_CHANNEL_ID   D-channel for DM escalation
  SLACK_BUYER_DM_CHANNEL_ID         D-channel for buyer DM (PO ETA)
  STOCK_ISSUE_ESCALATION_HOURS      default 4
  STOCK_ISSUE_MORNING_HOUR_ET       default 8 (i.e. 8:30 — minutes
                                     fixed at 30)
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))
import db  # noqa: E402

try:
    from data_paths import OUTPUT_DIR
except ImportError:
    OUTPUT_DIR = SCRIPT_DIR / "output"

LOG_FORMAT = "%(asctime)s [%(levelname)s] %(message)s"
log = logging.getLogger("stock_issues_handler")


# ---------------------------------------------------------------------------
# Classification — when does a message in #stock-issues-queries
# count as a stock issue worth tracking?
# ---------------------------------------------------------------------------
_SO_RE = re.compile(r"\bSO[-]?(\d{4,})\b", re.IGNORECASE)
_INV_RE = re.compile(r"\bINV[-]?(\d{4,})\b", re.IGNORECASE)
_SKU_RE = re.compile(r"\b(LED(?:KIT)?-[A-Z0-9-]+)\b", re.IGNORECASE)

# Supply-query keywords: phrases asking 'can we ship?'
_SUPPLY_KEYWORDS = (
    "can we supply", "can we ship", "can we fulfil", "can we fulfill",
    "do we have", "are we short", "what are we short", "what we short",
    "how many can we", "how much can we", "please ship",
    "ship the", "supply the", "have stock", "in stock",
)

# Count-wrong keywords: discrepancy claims about CIN7 numbers.
_COUNT_WRONG_KEYWORDS = (
    "should be", "actual count", "actual qty", "actual quantity",
    "missing", "found extra", "found only", "i counted",
    "we counted", "stocktake", "stock take", "wrong qty",
    "wrong quantity", "wrong stock", "stock is wrong",
    "stock count", "system shows", "but system",
    "incorrect", "out by", "wrong by",
)


def classify_message(text: str) -> Optional[dict]:
    """Return a classification dict if `text` reads as a stock
    issue, None otherwise. Includes the detected pattern type +
    extracted entities (SO numbers, INV numbers, SKUs)."""
    if not text:
        return None
    lower = text.lower()
    sos = [f"SO-{m.group(1)}" for m in _SO_RE.finditer(text)]
    invs = [f"INV-{m.group(1)}" for m in _INV_RE.finditer(text)]
    skus = [m.group(1).upper() for m in _SKU_RE.finditer(text)]
    is_supply = any(k in lower for k in _SUPPLY_KEYWORDS)
    is_count = any(k in lower for k in _COUNT_WRONG_KEYWORDS)
    # Require AT LEAST one of:
    #   - SO number + supply keyword (Richard's pattern)
    #   - SKU + count-wrong keyword
    if sos and is_supply:
        issue_type = "supply_query"
    elif skus and is_count:
        issue_type = "count_wrong"
    elif sos and is_count:
        issue_type = "count_wrong"
    elif skus and is_supply:
        issue_type = "supply_query"
    else:
        return None
    return {
        "issue_type": issue_type,
        "so_numbers": sos,
        "inv_numbers": invs,
        "skus": skus,
    }


# ---------------------------------------------------------------------------
# Intelligence-block building — pulls live data for each item
# ---------------------------------------------------------------------------
def _load_sale_lines() -> Optional[pd.DataFrame]:
    """Cache-once helper; per-issue lookups against the same DF."""
    import glob
    matches = []
    for pat in ("sale_lines_last_1d_*.csv",
                 "sale_lines_last_7d_*.csv",
                 "sale_lines_last_*d_*.csv"):
        matches.extend(glob.glob(str(OUTPUT_DIR / pat)))
    if not matches:
        return None
    path = max(matches, key=os.path.getmtime)
    try:
        return pd.read_csv(path)
    except Exception as exc:
        log.warning("Failed to load sale_lines: %s", exc)
        return None


# v2.67.147 — pseudo-SKUs that appear in CIN7 sale_lines but
# aren't real products. Filter these out before any intelligence
# lookup so we don't treat 'SHIPPING - UPS® GROUND' as a stockable
# SKU. Match is case-insensitive substring on the SKU itself.
_PSEUDO_SKU_PATTERNS = (
    "SHIPPING", "FREIGHT", "DELIVERY", "POSTAGE", "COURIER",
    "TAX", "DISCOUNT", "ADJUSTMENT", "ROUNDING", "HANDLING",
    "INSURANCE",
)

# v2.67.148 — Dropship SKU index. Per-process cache built lazily
# from products_*.csv (the CIN7 product master). For dropship
# items, the fulfillment-shortage logic doesn't apply (we never
# warehouse them); the right intelligence is supplier + draft-PO
# status instead.
_DROPSHIP_SKU_SET_CACHE: Optional[set] = None


def _load_dropship_sku_set() -> set:
    """Return the set of SKUs flagged 'Always Drop Ship' in CIN7.
    Cached per-process. Defers to po_dispatch_reminder's helper
    for the actual CSV load so we don't duplicate path logic."""
    global _DROPSHIP_SKU_SET_CACHE
    if _DROPSHIP_SKU_SET_CACHE is not None:
        return _DROPSHIP_SKU_SET_CACHE
    out: set = set()
    try:
        from po_dispatch_reminder import _find_latest_csv
        path = _find_latest_csv("products_*.csv")
        if not path:
            _DROPSHIP_SKU_SET_CACHE = out
            return out
        products_df = pd.read_csv(path)
        mode_col = next(
            (c for c in ("DropShipMode", "DropshipMode",
                          "Drop Ship Mode")
              if c in products_df.columns), None)
        sku_col = next(
            (c for c in ("SKU", "Sku", "ProductCode")
              if c in products_df.columns), None)
        if mode_col and sku_col:
            mode_u = (products_df[mode_col].fillna("")
                          .astype(str).str.upper().str.strip())
            mask = mode_u == "ALWAYS DROP SHIP"
            for sku in products_df[mask][sku_col].dropna():
                s = str(sku).strip().upper()
                if s:
                    out.add(s)
        log.info("Loaded %d dropship SKUs into cache", len(out))
    except Exception as exc:
        log.warning("Failed to load dropship SKU set: %s", exc)
    _DROPSHIP_SKU_SET_CACHE = out
    return out


def _is_dropship_sku(sku: str) -> bool:
    if not sku:
        return False
    return sku.upper() in _load_dropship_sku_set()


# v2.67.190 — Direct stock_on_hand_*.csv lookup. The primary
# bin/OnHand source is the engine_df (via bot_engine_lookup),
# but the engine output can lag if the engine hasn't recomputed
# recently or if a SKU got added between recomputes. This module
# provides a per-process cached direct read of the freshest
# stock CSV — used as a SECOND-LINE fallback when engine_df
# returns nothing.
#
# CIN7's UI calls the bin field "Stock Locator". The API returns
# it under one of: Bin, BinLocation, StockLocator, Stock Locator
# (the spaced form is from CSV exports that preserve the UI
# header verbatim). We try every variant.
_STOCK_BIN_LOOKUP_CACHE: Optional[dict] = None


def _load_stock_bin_lookup() -> dict:
    """Return {sku_upper: {bin, on_hand, location, name}} from
    the freshest CSVs. v2.67.204 — reads TWO sources because
    CIN7 splits the data:

      products_*.csv         → StockLocator (the bin), Name
                               (canonical source — the bin is a
                               product-master attribute, not a
                               stock-on-hand attribute)
      stock_on_hand_*.csv    → OnHand, Location (warehouse)

    Indexed by SKU. Per-process cache. Empty dict if no CSVs
    available."""
    global _STOCK_BIN_LOOKUP_CACHE
    if _STOCK_BIN_LOOKUP_CACHE is not None:
        return _STOCK_BIN_LOOKUP_CACHE
    out: dict = {}
    try:
        import glob, os

        # ---- Source 1: products_*.csv — provides the BIN ----
        # The Stock Locator field lives on the product master
        # in CIN7 (one default bin per SKU). It does NOT come
        # from productavailability / stock_on_hand. My v2.67.190
        # missed this and only looked in stock_on_hand.
        prod_cands = sorted(
            glob.glob(str(OUTPUT_DIR / "products_*.csv")),
            key=os.path.getmtime, reverse=True)
        if prod_cands:
            ppath = prod_cands[0]
            pdf = pd.read_csv(ppath, low_memory=False)
            psku_col = next(
                (c for c in ("SKU", "Sku", "ProductCode")
                  if c in pdf.columns), None)
            # CIN7's API uses `StockLocator` (matching the UI
            # label "Stock locator"). Older exports may use
            # other variants — fall through.
            pbin_col = next(
                (c for c in ("StockLocator", "Stock Locator",
                                "stock_locator", "Bin", "bin",
                                "BinLocation")
                  if c in pdf.columns), None)
            pname_col = next(
                (c for c in ("Name", "ProductName", "name")
                  if c in pdf.columns), None)
            log.info(
                "stock_bin_lookup: products from %s — "
                "sku=%s bin=%s name=%s",
                ppath, psku_col, pbin_col, pname_col)
            if psku_col:
                for _, r in pdf.iterrows():
                    sku_u = str(r.get(psku_col) or "").strip().upper()
                    if not sku_u:
                        continue
                    entry = out.setdefault(sku_u, {
                        "bin": None, "on_hand": None,
                        "location": None, "name": None})
                    if pbin_col:
                        b = r.get(pbin_col)
                        if pd.notna(b) and str(b).strip():
                            entry["bin"] = str(b).strip()
                    if pname_col and not entry["name"]:
                        nm = r.get(pname_col)
                        if pd.notna(nm) and str(nm).strip():
                            entry["name"] = str(nm).strip()

        # ---- Source 2: stock_on_hand_*.csv — OnHand + Location ----
        soh_cands = sorted(
            glob.glob(str(OUTPUT_DIR / "stock_on_hand_*.csv")),
            key=os.path.getmtime, reverse=True)
        if soh_cands:
            spath = soh_cands[0]
            sdf = pd.read_csv(spath, low_memory=False)
            ssku_col = next(
                (c for c in ("SKU", "Sku", "ProductCode")
                  if c in sdf.columns), None)
            # Even on stock_on_hand, attempt a bin lookup
            # (some accounts include it here too) so the
            # mechanism is belt-and-braces.
            sbin_col = next(
                (c for c in ("Bin", "bin", "BinLocation",
                                "StockLocator", "Stock Locator")
                  if c in sdf.columns), None)
            soh_col = next(
                (c for c in ("OnHand", "on_hand", "Stock")
                  if c in sdf.columns), None)
            loc_col = next(
                (c for c in ("Location", "location",
                                "WarehouseName", "Warehouse")
                  if c in sdf.columns), None)
            log.info(
                "stock_bin_lookup: stock_on_hand from %s — "
                "sku=%s bin=%s onhand=%s loc=%s",
                spath, ssku_col, sbin_col, soh_col, loc_col)
            if ssku_col:
                for _, r in sdf.iterrows():
                    sku_u = str(r.get(ssku_col) or "").strip().upper()
                    if not sku_u:
                        continue
                    entry = out.setdefault(sku_u, {
                        "bin": None, "on_hand": None,
                        "location": None, "name": None})
                    # Only fill bin from stock_on_hand if
                    # products didn't already supply one.
                    if sbin_col and not entry["bin"]:
                        b = r.get(sbin_col)
                        if pd.notna(b) and str(b).strip():
                            entry["bin"] = str(b).strip()
                    if soh_col:
                        try:
                            v = float(r.get(soh_col) or 0)
                            entry["on_hand"] = (
                                (entry["on_hand"] or 0) + v)
                        except (TypeError, ValueError):
                            pass
                    if loc_col and not entry["location"]:
                        ln = r.get(loc_col)
                        if pd.notna(ln) and str(ln).strip():
                            entry["location"] = str(ln).strip()

        log.info("stock_bin_lookup: indexed %d SKUs total",
                  len(out))
    except Exception as exc:
        log.warning(
            "stock_bin_lookup load failed: %s", exc)
    _STOCK_BIN_LOOKUP_CACHE = out
    return out


def _direct_stock_lookup(sku: str) -> dict:
    """Return {bin, on_hand, location, name} for a SKU from
    stock_on_hand CSV directly. Skips the engine_df indirection.
    Used as a fallback when bot_engine_lookup returns nothing."""
    if not sku:
        return {}
    return _load_stock_bin_lookup().get(
        sku.strip().upper(), {})


# v2.67.173 — Stock-item SKU index. Per-process cache built lazily
# from products_*.csv. CIN7 products have a Type column with
# values 'Stock' or 'Service'. Non-stock items (services like
# 'INSTALL', 'CONSULTING', 'FREIGHT', shipping pseudo-SKUs etc.)
# should not trigger stock-issue intelligence in the
# #stock-issues-queries channel — those messages should be
# silently skipped.
_STOCK_ITEM_SKU_SET_CACHE: Optional[set] = None


def _load_stock_item_sku_set() -> set:
    """Return the set of SKUs flagged as Type='Stock' in CIN7.
    Anything not in this set is a service / non-stock product
    and shouldn't generate a stock-queries response."""
    global _STOCK_ITEM_SKU_SET_CACHE
    if _STOCK_ITEM_SKU_SET_CACHE is not None:
        return _STOCK_ITEM_SKU_SET_CACHE
    out: set = set()
    try:
        from po_dispatch_reminder import _find_latest_csv
        path = _find_latest_csv("products_*.csv")
        if not path:
            _STOCK_ITEM_SKU_SET_CACHE = out
            return out
        products_df = pd.read_csv(path)
        type_col = next(
            (c for c in ("Type", "ProductType")
              if c in products_df.columns), None)
        sku_col = next(
            (c for c in ("SKU", "Sku", "ProductCode")
              if c in products_df.columns), None)
        if type_col and sku_col:
            type_u = (products_df[type_col].fillna("")
                          .astype(str).str.upper().str.strip())
            mask = type_u == "STOCK"
            for sku in products_df[mask][sku_col].dropna():
                s = str(sku).strip().upper()
                if s:
                    out.add(s)
        log.info("Loaded %d stock-item SKUs into cache", len(out))
    except Exception as exc:
        log.warning("Failed to load stock-item SKU set: %s", exc)
    _STOCK_ITEM_SKU_SET_CACHE = out
    return out


def _is_stock_item(sku: str) -> bool:
    """True if SKU is a CIN7 stock-type product. If the cache is
    empty (products CSV missing, all loads failed), return True
    so we don't silently drop every message — better to over-
    respond than to disappear."""
    if not sku:
        return False
    cache = _load_stock_item_sku_set()
    if not cache:
        return True  # fail-open: empty cache means CSV unavailable
    return sku.upper() in cache


def _is_pseudo_sku(sku: str) -> bool:
    if not sku:
        return True
    su = sku.upper()
    return any(p in su for p in _PSEUDO_SKU_PATTERNS)


def _so_line_skus(sale_lines: pd.DataFrame,
                       so_number: str) -> List[dict]:
    """Return list of {sku, qty, customer} for one SO. Filters out
    pseudo-SKUs (shipping, freight, tax etc) that CIN7 stores as
    line items but aren't stockable products."""
    if sale_lines is None or sale_lines.empty:
        return []
    so_col = next(
        (c for c in ("OrderNumber", "SaleNumber", "InvoiceNumber")
          if c in sale_lines.columns), None)
    sku_col = next(
        (c for c in ("SKU", "ProductCode")
          if c in sale_lines.columns), None)
    qty_col = next(
        (c for c in ("Quantity", "Qty") if c in sale_lines.columns),
        None)
    cust_col = next(
        (c for c in ("Customer", "CustomerName", "BillingName")
          if c in sale_lines.columns), None)
    if not so_col or not sku_col:
        return []
    # Normalise SO number for matching (drop SO- prefix, match
    # numeric core like ai_tools.get_shipping_details does).
    norm = so_number.upper().replace("SO-", "").replace("SO", "")
    match = sale_lines[
        sale_lines[so_col].astype(str).str.upper()
        .str.replace("SO-", "", regex=False)
        .str.replace("INV-", "", regex=False) == norm]
    out = []
    for _, row in match.iterrows():
        sku = str(row.get(sku_col) or "").strip().upper()
        if _is_pseudo_sku(sku):
            continue
        out.append({
            "sku": sku,
            "qty": row.get(qty_col),
            "customer": (str(row.get(cust_col))
                          if cust_col else None),
        })
    return out


def _sku_intel(sku: str, sale_lines: pd.DataFrame) -> dict:
    """Aggregate everything we know about a SKU into the format
    the stock-controller intelligence block expects.

    v2.67.148 — also flags `is_dropship` based on CIN7's
    DropShipMode. Dropship items get a completely different
    intelligence template (no warehouse-OnHand check, focus on
    supplier + draft-PO status)."""
    out = {
        "sku": sku,
        "name": None,
        "bin": None,
        "abc": None,
        "trend": None,
        "on_hand": None,
        "allocated": None,
        "on_order": None,
        "open_sos": [],
        "next_po": None,
        "is_dropship": _is_dropship_sku(sku),
    }
    # bot_engine_lookup for ABC / trend / OnHand / bin
    try:
        from bot_engine_lookup import lookup_sku_signals
        sig = lookup_sku_signals(sku)
        if sig:
            out["on_hand"] = sig.get("stock")
            out["bin"] = sig.get("bin")
            out["abc"] = sig.get("abc")
            out["trend"] = sig.get("trend_flag")
    except Exception:
        pass

    # v2.67.182 — Parent-SKU fallback for child variants. Per-foot
    # cuts and other child SKUs (e.g. LED-Z1V-…-100, the 100ft cut
    # of a master roll) often have no bin / OnHand of their own
    # because warehouse stock is held on the master. When the
    # child lookup returns nothing useful, resolve the parent via
    # CIN7 BOM and inherit its signals. The parent_sku field is
    # surfaced to the composer so the reply can flag the
    # provenance ("via parent LED-…-100M").
    needs_parent_fallback = (
        out.get("bin") in (None, "", "?")
        or out.get("on_hand") is None)
    if needs_parent_fallback:
        try:
            from bom_lookup import parent_sku as _bom_parent
            parent = _bom_parent(sku)
            if parent and parent.upper() != sku:
                from bot_engine_lookup import lookup_sku_signals
                sig_p = lookup_sku_signals(parent)
                if sig_p:
                    out["parent_sku"] = parent
                    if out.get("on_hand") is None:
                        out["on_hand"] = sig_p.get("stock")
                    if out.get("bin") in (None, "", "?"):
                        out["bin"] = sig_p.get("bin")
                    if not out.get("abc"):
                        out["abc"] = sig_p.get("abc")
                    if not out.get("trend"):
                        out["trend"] = sig_p.get("trend_flag")
        except Exception:
            pass

    # v2.67.190 — Direct stock_on_hand CSV fallback when the
    # engine_df pipeline returns nothing for bin/OnHand. This
    # bypasses any engine-output staleness AND handles CSV
    # exports that use the "Stock Locator" column name (CIN7's
    # current UI label) instead of the legacy "Bin". Order of
    # preference:
    #   1. engine_df via bot_engine_lookup (above)
    #   2. parent SKU's engine_df via BOM (above)
    #   3. direct stock_on_hand CSV read for this SKU (here)
    #   4. direct stock_on_hand CSV read for parent SKU (also
    #      here, if step 1+2 gave us a parent)
    # Each step fills in only fields still missing — no
    # overwriting good data from earlier steps.
    if (out.get("bin") in (None, "", "?")
            or out.get("on_hand") is None):
        direct = _direct_stock_lookup(sku)
        if direct:
            if out.get("on_hand") is None and direct.get("on_hand") is not None:
                out["on_hand"] = direct["on_hand"]
            if out.get("bin") in (None, "", "?") and direct.get("bin"):
                out["bin"] = direct["bin"]
            if not out.get("name") and direct.get("name"):
                out["name"] = direct["name"]
    # If still no bin and we have a parent, try the parent in the
    # direct CSV too.
    if (out.get("bin") in (None, "", "?")
            and out.get("parent_sku")):
        direct_p = _direct_stock_lookup(out["parent_sku"])
        if direct_p:
            if out.get("on_hand") is None and direct_p.get("on_hand") is not None:
                out["on_hand"] = direct_p["on_hand"]
            if out.get("bin") in (None, "", "?") and direct_p.get("bin"):
                out["bin"] = direct_p["bin"]
    # Open SOs allocated against this SKU (count from sale_lines)
    if sale_lines is not None and not sale_lines.empty:
        sku_col = next(
            (c for c in ("SKU", "ProductCode")
              if c in sale_lines.columns), None)
        so_col = next(
            (c for c in ("OrderNumber", "SaleNumber")
              if c in sale_lines.columns), None)
        qty_col = next(
            (c for c in ("Quantity", "Qty")
              if c in sale_lines.columns), None)
        status_col = next(
            (c for c in ("Status", "InvoiceStatus")
              if c in sale_lines.columns), None)
        if sku_col:
            m = sale_lines[
                sale_lines[sku_col].astype(str).str.upper() == sku]
            # Filter out shipped/voided if status column present.
            if status_col is not None and not m.empty:
                status_u = (m[status_col].fillna("").astype(str)
                              .str.upper())
                m = m[~status_u.str.contains(
                    "RECEIVED|CANCELLED|VOIDED|CLOSED|COMPLETED",
                    regex=True, na=False)]
            if not m.empty and so_col:
                so_qty = {}
                for _, r in m.iterrows():
                    so = str(r.get(so_col) or "").strip()
                    q = r.get(qty_col) if qty_col else None
                    try:
                        q = float(q)
                    except (TypeError, ValueError):
                        q = 0.0
                    so_qty[so] = so_qty.get(so, 0.0) + q
                total_alloc = sum(so_qty.values())
                out["allocated"] = total_alloc
                out["open_sos"] = sorted(so_qty.items())[:5]
    # Next incoming PO via po_dispatch_reminder helpers.
    # v2.67.182 — Also check the parent SKU when the child has
    # nothing on order. Master rolls are what suppliers actually
    # ship; the cut SKU never has its own PO.
    skus_to_check = [sku]
    if out.get("parent_sku"):
        skus_to_check.append(str(out["parent_sku"]).upper())
    try:
        from po_dispatch_reminder import _load_purchases_and_lines
        purchases, lines = _load_purchases_and_lines()
        if lines is not None and not lines.empty:
            if "SKU" in lines.columns:
                m = lines[
                    lines["SKU"].astype(str).str.upper()
                    .isin(skus_to_check)]
                if "Status" in m.columns:
                    sc = (m["Status"].fillna("").astype(str)
                            .str.upper())
                    m = m[~sc.str.contains(
                        "RECEIVED|CLOSED|COMPLETED|CANCELLED|VOIDED|"
                        "DRAFT", regex=True, na=False)]
                if not m.empty:
                    date_col = next(
                        (c for c in ("RequiredBy", "ExpectedDate",
                                        "DeliveryDate")
                          if c in m.columns), None)
                    if date_col:
                        m = m.copy()
                        m["__d"] = pd.to_datetime(
                            m[date_col], errors="coerce")
                        m = m.sort_values(
                            "__d", na_position="last")
                    r = m.iloc[0]
                    out["next_po"] = {
                        "po_number": r.get("OrderNumber"),
                        "qty": r.get("Quantity"),
                        "supplier": r.get("Supplier"),
                        "eta": (str(r.get(date_col))[:10]
                                  if date_col
                                  and pd.notna(r.get(date_col))
                                  else None),
                    }
                    out["on_order"] = float(r.get("Quantity") or 0)
    except Exception as exc:
        log.warning("Next PO lookup for %s failed: %s", sku, exc)
    return out


def _compose_intelligence_block(items: List[dict],
                                       so_numbers: List[str],
                                       issue_type: str) -> str:
    """Build the stock-controller intelligence block. Per James's
    spec: factual data + 'please confirm tracking' ask. NO
    dispatch recommendation."""
    lines: List[str] = ["📋 *Stock-issue intelligence — please verify and confirm:*", ""]
    for item in items:
        sku = item.get("sku") or "?"
        name = item.get("name") or ""
        head = f"*`{sku}`*"
        if name:
            head += f" — _{name}_"
        # v2.67.148 — dropship badge so the stock controller
        # immediately sees this is a supplier-ship-direct item
        # and OnHand=0 is expected, not a counting error.
        if item.get("is_dropship"):
            head += "  📦 *DROPSHIP*"
        lines.append(head)
        # v2.67.182 — surface parent-SKU provenance when the child
        # has no warehouse data of its own and we resolved to the
        # master roll. Helps the stock controller know the figures
        # below are about the master, not the cut.
        parent_sku = item.get("parent_sku")
        if parent_sku:
            lines.append(
                f"  ↳ _stock/bin shown for master roll "
                f"`{parent_sku}` (child is cut from it)_")
        parts = []
        # v2.67.175 — always include bin (per James). For warehouse
        # items: show the bin, or `Bin: ?` if the engine lookup
        # didn't find one (so the controller sees we tried).
        # Dropship items skip bin since they're never warehoused.
        if not item.get("is_dropship"):
            bin_v = item.get("bin")
            parts.append(f"Bin {bin_v}" if bin_v else "Bin: *?*")
        if item.get("abc"):
            parts.append(f"{item['abc']}-class")
        if item.get("trend"):
            parts.append(item["trend"])
        if parts:
            lines.append(f"• {' · '.join(parts)}")
        # For dropship items, the OnHand line carries little
        # information (always 0/unknown); skip it.
        if not item.get("is_dropship"):
            on_hand_str = (f"{int(item['on_hand'])}"
                            if item.get("on_hand") is not None
                            else "?")
            alloc_str = (f"{int(item['allocated'])}"
                          if item.get("allocated") is not None
                          else "0")
            lines.append(
                f"• CIN7 OnHand: *{on_hand_str}* · "
                f"Allocated: *{alloc_str}*"
                + (f" (across {len(item['open_sos'])} open SOs)"
                    if item.get("open_sos") else ""))
        else:
            lines.append(
                "• _No warehouse stock (dropship) — "
                "supplier ships direct on PO approval_")
        if item.get("open_sos"):
            sos_str = ", ".join(
                f"{so}×{int(q)}" for so, q in item["open_sos"])
            lines.append(f"   _Open SOs:_ {sos_str}")
        po = item.get("next_po")
        if po:
            po_bits = []
            if po.get("po_number"):
                # v2.67.147 — po_number may or may not already
                # include the 'PO-' prefix depending on which CSV
                # field it came from. Don't double-prefix.
                pn = str(po["po_number"]).strip()
                if not pn.upper().startswith("PO-"):
                    pn = f"PO-{pn}"
                po_bits.append(pn)
            if po.get("qty") is not None:
                try:
                    po_bits.append(f"{int(po['qty'])} units")
                except Exception:
                    pass
            if po.get("supplier"):
                po_bits.append(str(po["supplier"]))
            if po.get("eta"):
                po_bits.append(f"ETA {po['eta']}")
            lines.append(f"• Next PO: {' · '.join(po_bits)}")
        else:
            lines.append("• Next PO: _none on order_")
        lines.append("")
    lines.append(
        "_Reply 'fixed' / 'adjusted' / 'no change' to close this "
        "issue. I'll DM Jamie if no response within 4h._")
    return "\n".join(lines)


def _fulfillment_status(item: dict) -> str:
    """Return 'yes' / 'no' / 'unknown' / 'dropship' for
    can-we-fulfill-this-line.

    v2.67.148 — Dropship items return 'dropship' regardless of
    OnHand. We never warehouse them, so the OnHand-vs-qty check
    is meaningless; the relevant question is whether the
    auto-created draft PO has been approved + supplier ETA.
    Both _needs_buyer_ping and _needs_reorder_flag treat
    'dropship' as a no-op (no fulfillment status to warn on)."""
    if item.get("is_dropship"):
        return "dropship"
    on_hand = item.get("on_hand")
    req = item.get("requested_qty")
    if on_hand is None or req is None:
        return "unknown"
    try:
        return "yes" if float(on_hand) >= float(req) else "no"
    except (TypeError, ValueError):
        return "unknown"


def _needs_buyer_ping(items: List[dict]) -> bool:
    """v2.67.147 — Only ping the buyer when we DEFINITELY can't
    fulfill (status == 'no', not 'unknown') AND there's an
    incoming PO with an ETA worth confirming. 'Unknown' status
    means we couldn't determine OnHand/qty — surfacing a
    speculative buyer ping in that case is noise, not signal."""
    for item in items:
        if _fulfillment_status(item) != "no":
            continue
        po = item.get("next_po") or {}
        if po.get("eta"):
            return True
    return False


def _needs_reorder_flag(items: List[dict]) -> bool:
    """v2.67.147 — Only fire the reorder flag on DEFINITE
    shortage with no PO. 'Unknown' status doesn't trigger; that's
    the stockkeeper's call once they verify the actual counts."""
    for item in items:
        if _fulfillment_status(item) != "no":
            continue
        po = item.get("next_po") or {}
        if not po.get("eta"):
            return True
    return False


def _compose_querier_reply(items: List[dict],
                                so_numbers: List[str],
                                buyer_dm_channel: Optional[str]
                                ) -> str:
    """Brief reply to the querier — high-level snapshot + the
    SPECIFIC follow-up the bot is recommending.

    Per James (v2.67.145 refinement): only ask the querier to
    confirm with the buyer when (a) we can't fulfill from on-hand
    and (b) there's an incoming PO with an ETA to verify. Don't
    fire the buyer ping when stock is fine OR when no PO exists."""
    buyer_text = ("@AndrewTunley"
                    if buyer_dm_channel else "the buyer")

    summary_bits = []
    any_unknown_qty = False
    for item in items:
        sku = item.get("sku") or "?"
        on_hand = item.get("on_hand")
        alloc = item.get("allocated") or 0
        req = item.get("requested_qty")
        next_eta = (item.get("next_po") or {}).get("eta")
        status = _fulfillment_status(item)

        # v2.67.148 — dropship items get their own row format.
        # They never carry warehouse stock; the relevant info is
        # "supplier ships direct + ETA from auto-PO".
        if status == "dropship":
            bit = f"📦 `{sku}` — *DROPSHIP*"
            if req is not None:
                try:
                    bit += f", ordered {int(req)}"
                except (TypeError, ValueError):
                    pass
            po = item.get("next_po") or {}
            if po.get("eta"):
                bit += f" · supplier ETA *{po['eta']}*"
            else:
                bit += " · _no draft PO visible — verify in CIN7_"
            summary_bits.append(bit)
            continue

        oh = (int(on_hand) if on_hand is not None else "?")
        bit_prefix = (
            "✅" if status == "yes"
            else "🟥" if status == "no"
            else "❔")
        # v2.67.175 — bin always present in the brief snapshot
        # so the warehouse can verify location at a glance.
        # `?` flags 'engine didn't find one — check CIN7'.
        # v2.67.182 — append parent-SKU note when the child's
        # bin/stock came from the master roll.
        bin_v = item.get("bin") or "?"
        parent_marker = ""
        if item.get("parent_sku"):
            parent_marker = (
                f" _(via master `{item['parent_sku']}`)_")
        bit = (f"{bit_prefix} `{sku}` — Bin *{bin_v}* · "
                f"OnHand {oh}{parent_marker}")
        if req is not None:
            try:
                bit += f", needs {int(req)}"
            except (TypeError, ValueError):
                pass
        else:
            any_unknown_qty = True
        if alloc and not req:
            bit += f", {int(alloc)} allocated total"
        if status == "no" and next_eta:
            bit += f" · next PO ETA *{next_eta}*"
        elif status == "no":
            bit += f" · *no incoming PO*"
        summary_bits.append(bit)
    snapshot = "\n".join(summary_bits[:5])

    body = snapshot
    needs_buyer = _needs_buyer_ping(items)
    needs_reorder = _needs_reorder_flag(items)
    has_dropship = any(
        _fulfillment_status(it) == "dropship" for it in items)
    dropship_without_po = any(
        _fulfillment_status(it) == "dropship"
        and not (it.get("next_po") or {}).get("eta")
        for it in items)

    # v2.67.148 — dropship-specific notes. These items don't go
    # through the normal stock-shortage logic; the action is
    # 'approve the draft PO' or 'check supplier ETA'.
    if dropship_without_po:
        body += (f"\n\n📦 Dropship item with no visible draft "
                  f"PO — verify in CIN7 that the auto-PO was "
                  f"created. May need {buyer_text} to action.")
    elif has_dropship:
        body += (f"\n\n📦 Dropship items ship direct from supplier. "
                  f"Confirm ETA accuracy with {buyer_text} if "
                  f"customer is waiting.")

    if needs_buyer:
        body += (f"\n\n⚠️ Stock is short on the items above with "
                  f"an incoming PO. Please confirm with "
                  f"{buyer_text} that the listed ETA is accurate.")
    if needs_reorder:
        body += (f"\n\n🔴 No incoming PO for items marked above. "
                  f"Stockkeeper / buyer to decide on reorder.")
    if not (needs_buyer or needs_reorder or has_dropship):
        all_yes = all(
            _fulfillment_status(it) == "yes" for it in items)
        if all_yes:
            body += "\n\n✅ All items appear to have stock on hand."
        elif any_unknown_qty:
            body += ("\n\n_Requested quantity wasn't extractable "
                      "from the message — verify the SO/SKU "
                      "details before quoting._")

    body += ("\n\n_Full detail for the stock controller posted "
              "in the next message._")
    return body


# ---------------------------------------------------------------------------
# Slack posting
# ---------------------------------------------------------------------------
def _post_to_slack(channel_id: str, text: str,
                       thread_ts: Optional[str] = None
                       ) -> Tuple[Optional[str], Optional[str]]:
    try:
        import slack_sync
    except ImportError as exc:
        return None, f"slack_sync import failed: {exc}"
    token = os.environ.get("SLACK_BOT_TOKEN", "").strip()
    if not token:
        return None, "SLACK_BOT_TOKEN not set"
    body = {
        "channel": channel_id,
        "text": text,
        "unfurl_links": False,
        "unfurl_media": False,
    }
    if thread_ts:
        body["thread_ts"] = thread_ts
    try:
        session = slack_sync._build_session(token)
        resp = slack_sync._slack_post(
            session, "chat.postMessage", body)
        if not resp.get("ok"):
            return None, f"slack returned ok=false: {resp}"
        return resp.get("ts"), None
    except Exception as exc:
        return None, f"post error: {exc}"


# ---------------------------------------------------------------------------
# Top-level handler (called from slack_listener.process_once)
# ---------------------------------------------------------------------------
def handle_stock_issue(msg: dict) -> Tuple[Optional[str], List[str]]:
    """Classify, persist, build intelligence, post the brief
    querier reply + stock-controller intelligence block. Returns
    (querier_reply_text, tools_used) so the listener can record
    its standard slack_bot_responses row for the QUERIER reply.
    The intelligence block + tracking row are persisted here."""
    text = msg.get("text") or ""
    cls = classify_message(text)
    if not cls:
        return None, []

    tools_used: List[str] = ["classify_message"]

    so_numbers = cls.get("so_numbers") or []
    inv_numbers = cls.get("inv_numbers") or []
    skus_from_text = cls.get("skus") or []

    # Build the list of items to surface intelligence for.
    sale_lines = _load_sale_lines()
    items: List[dict] = []
    seen_skus: set = set()
    families: List[str] = []

    # Expand SOs → SKUs from sale_lines. v2.67.145 — also capture
    # the line's requested quantity so the fulfillment-status
    # check (can we ship?) can reason about OnHand vs needs.
    for so in so_numbers + inv_numbers:
        for line in _so_line_skus(sale_lines, so):
            sku = line.get("sku") or ""
            if not sku or sku in seen_skus:
                continue
            seen_skus.add(sku)
            intel = _sku_intel(sku, sale_lines)
            try:
                intel["requested_qty"] = (
                    float(line.get("qty"))
                    if line.get("qty") is not None
                    else None)
            except (TypeError, ValueError):
                intel["requested_qty"] = None
            intel["from_so"] = so
            intel["customer"] = line.get("customer")
            items.append(intel)
        tools_used.append("so_line_skus")

    # Add any SKUs directly mentioned in the text.
    for sku in skus_from_text:
        if sku in seen_skus:
            continue
        seen_skus.add(sku)
        items.append(_sku_intel(sku, sale_lines))
        tools_used.append("sku_intel")

    if not items:
        # We classified it but couldn't expand to any SKU
        # intelligence — log + skip.
        return None, tools_used

    # v2.67.173 — Stock-items-only filter for the
    # #stock-issues-queries channel. Drop any SKU that isn't
    # Type='Stock' in CIN7 (services, freight, install fees,
    # consulting line items, etc.). If nothing stock-typed
    # remains after the filter, silently skip the message
    # entirely — the channel is for warehouse-stock queries.
    n_before = len(items)
    items = [i for i in items if _is_stock_item(i.get("sku") or "")]
    n_after = len(items)
    if n_after < n_before:
        tools_used.append(
            f"stock_item_filter:{n_before}->{n_after}")
    if not items:
        log.info("All %d SKUs in stock-query message were "
                  "non-stock items — skipping reply.", n_before)
        return None, tools_used

    # Persist the issue (idempotent on channel/ts).
    issue_id = db.upsert_stock_issue(
        raise_channel=msg["channel_id"],
        raise_ts=msg["ts"],
        raise_thread_ts=msg.get("thread_ts") or msg["ts"],
        raised_by=msg.get("user_name"),
        raised_text=text,
        issue_type=cls["issue_type"],
        so_numbers=so_numbers,
        skus=list(seen_skus),
        families=families,
    )
    tools_used.append(f"upsert_stock_issue:{issue_id}")

    # Compose both reply pieces.
    buyer_dm = os.environ.get(
        "SLACK_BUYER_DM_CHANNEL_ID", "").strip() or None
    querier_reply = _compose_querier_reply(
        items, so_numbers, buyer_dm)
    intel_block = _compose_intelligence_block(
        items, so_numbers, cls["issue_type"])

    # v2.67.147 — post BOTH messages in the right order from the
    # handler so Slack shows querier reply first, then intel
    # block. Previously the listener posted the querier reply
    # AFTER the handler posted the intel block, reversing the
    # intended order. We return ('', tools_used) so the listener
    # skips its own post (the return-empty-string contract is
    # already in place to handle parse failures).
    thread_ts = msg.get("thread_ts") or msg["ts"]
    q_posted_ts, q_err = _post_to_slack(
        msg["channel_id"], querier_reply, thread_ts=thread_ts)
    if q_err:
        log.error("Querier reply post failed for issue %d: %s",
                    issue_id, q_err)
    else:
        tools_used.append("querier_reply_posted")
        # Record the querier-reply in slack_bot_responses for
        # audit-mirror compatibility with the rest of the bot's
        # responses table.
        try:
            with db.connect() as c:
                c.execute(
                    "INSERT INTO slack_bot_responses "
                    "(in_channel, in_ts, in_thread_ts, "
                    " user_question, response_text, "
                    " response_ts, tools_used, classification) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (msg["channel_id"], msg["ts"], thread_ts,
                      (msg.get("text") or "")[:500],
                      querier_reply, q_posted_ts,
                      ",".join(tools_used),
                      "stock_issue_raise"))
        except Exception as exc:
            log.warning(
                "Failed to record querier reply in "
                "slack_bot_responses: %s", exc)

    posted_ts, err = _post_to_slack(
        msg["channel_id"], intel_block, thread_ts=thread_ts)
    if posted_ts:
        db.update_stock_issue_bot_reply(issue_id, posted_ts)
        tools_used.append("intel_block_posted")
    elif err:
        log.error("Intel block post failed for issue %d: %s",
                    issue_id, err)

    # Return empty string so listener doesn't try to post the
    # querier reply again (we already posted both).
    return "", tools_used


# ---------------------------------------------------------------------------
# Resolution detection — pick up confirmations from staff replies
# ---------------------------------------------------------------------------
_RESOLUTION_KEYWORDS = (
    "fixed", "adjusted", "corrected", "sorted", "done",
    "no change", "no adjustment", "all good", "confirmed",
    "resolved",
)


def maybe_resolve_from_thread_reply(msg: dict) -> bool:
    """If `msg` is a human reply in a thread we're tracking, and
    its text contains a resolution keyword, mark the corresponding
    stock_issue resolved. Returns True if resolution was applied."""
    text = (msg.get("text") or "").lower().strip()
    if not text:
        return False
    if msg.get("is_our_bot") or msg.get("is_bot"):
        return False
    thread_ts = msg.get("thread_ts")
    if not thread_ts:
        return False
    issue = db.find_stock_issue_by_thread(
        msg["channel_id"], thread_ts)
    if not issue:
        return False
    if not any(k in text for k in _RESOLUTION_KEYWORDS):
        return False
    db.resolve_stock_issue(
        int(issue["id"]),
        resolved_by=msg.get("user_name") or "unknown",
        resolution_text=(msg.get("text") or "")[:300])
    log.info("Resolved stock_issue %s via thread reply from %s",
              issue["id"], msg.get("user_name"))
    return True


# ---------------------------------------------------------------------------
# Escalation cycle — DM stock controller after Nh of no reply
# ---------------------------------------------------------------------------
def run_escalation_cycle(dryrun: bool = False,
                              min_age_hours: int = 4) -> dict:
    """For each awaiting_response stock_issue older than
    min_age_hours that hasn't been DM'd yet, DM Jamie Webb (or
    whoever SLACK_STOCKKEEPER_DM_CHANNEL_ID points at) with the
    intelligence block + a direct ask to confirm/correct."""
    dm_channel = os.environ.get(
        "SLACK_STOCKKEEPER_DM_CHANNEL_ID", "").strip()
    if not dryrun and not dm_channel:
        return {"escalated": 0, "skipped_no_channel": True}

    pending = db.list_stock_issues_pending_escalation(
        min_age_hours=min_age_hours)
    if not pending:
        return {"pending": 0, "escalated": 0}

    sale_lines = _load_sale_lines()
    n_escalated = 0
    n_errors = 0
    for issue in pending:
        so_numbers = (issue.get("so_numbers") or "").split(",")
        skus = (issue.get("skus") or "").split(",")
        so_numbers = [s for s in so_numbers if s]
        skus = [s for s in skus if s]
        items = []
        seen = set()
        for so in so_numbers:
            for line in _so_line_skus(sale_lines, so):
                sku = line.get("sku") or ""
                if not sku or sku in seen:
                    continue
                seen.add(sku)
                items.append(_sku_intel(sku, sale_lines))
        for sku in skus:
            if sku in seen:
                continue
            seen.add(sku)
            items.append(_sku_intel(sku, sale_lines))
        if not items:
            continue
        intel_text = _compose_intelligence_block(
            items, so_numbers, issue.get("issue_type"))
        # Wrap with a 'no reply, please action' header.
        age_hours = 0
        try:
            ts = pd.to_datetime(issue["created_at"])
            age_hours = int(
                (pd.Timestamp.now() - ts).total_seconds() / 3600)
        except Exception:
            pass
        dm_text = (
            f"🚨 *Stock issue waiting on you* "
            f"(raised ~{age_hours}h ago by "
            f"{issue.get('raised_by') or 'someone'}):\n\n"
            f"_Original message:_\n"
            f"> {(issue.get('raised_text') or '')[:300]}\n\n"
            + intel_text)
        log.info("Escalating issue %s to stockkeeper %s",
                  issue["id"], "[DRYRUN]" if dryrun else "")
        if dryrun:
            print(f"\n--- ESCALATION DM for issue "
                    f"{issue['id']} ---\n{dm_text}\n")
            continue
        posted_ts, err = _post_to_slack(dm_channel, dm_text)
        if err:
            log.error("DM escalation failed for issue %s: %s",
                        issue["id"], err)
            n_errors += 1
            continue
        db.update_stock_issue_dm(
            int(issue["id"]),
            dm_channel=dm_channel,
            dm_posted_ts=posted_ts,
            awaiting_user="stockkeeper")
        n_escalated += 1
    return {
        "pending": len(pending),
        "escalated": n_escalated,
        "errors": n_errors,
    }


# ---------------------------------------------------------------------------
# Auto-resolution from CIN7 evidence (v2.67.155)
# ---------------------------------------------------------------------------
# Two evidence paths to auto-close an open issue without a Slack
# 'fixed' reply:
#
#   1. count_wrong issue → scan stock_adjustments CSV for a
#      matching SKU adjustment dated AFTER the issue was raised.
#      If found, close with citation '<date> · <qty> · <ref>'.
#
#   2. supply_query issue → check the latest shipments CSV for a
#      shipment of any SO listed in the issue. If shipped after
#      raise date, close with tracking citation.
#
# Both run BEFORE the morning summary so resolved issues drop
# from the outstanding list. Audit trail preserved via the
# resolution_text field.
def _load_latest_adjustments() -> Optional[pd.DataFrame]:
    import glob
    matches = sorted(glob.glob(
        str(OUTPUT_DIR / "stock_adjustments_last_*d_*.csv")),
        key=os.path.getmtime, reverse=True)
    if not matches:
        return None
    try:
        return pd.read_csv(matches[0], low_memory=False)
    except Exception as exc:
        log.warning("Failed to load adjustments CSV: %s", exc)
        return None


def _load_latest_shipments() -> Optional[pd.DataFrame]:
    import glob
    matches = sorted(glob.glob(
        str(OUTPUT_DIR / "shipments_last_*d_*.csv")),
        key=os.path.getmtime, reverse=True)
    if not matches:
        # Fall back to full dump if rolling-window CSVs missing
        full = sorted(glob.glob(
            str(OUTPUT_DIR / "shipments_full.csv")))
        if not full:
            return None
        matches = full
    try:
        return pd.read_csv(matches[0], low_memory=False)
    except Exception as exc:
        log.warning("Failed to load shipments CSV: %s", exc)
        return None


def _normalise_order_id(s) -> str:
    if s is None or pd.isna(s):
        return ""
    raw = str(s).strip().upper().lstrip("#")
    for prefix in ("SO-", "INV-", "SO", "INV"):
        if raw.startswith(prefix):
            return raw[len(prefix):]
    return raw


def _find_adjustment_for_sku(adjustments: pd.DataFrame,
                                  sku: str,
                                  after_iso: str) -> Optional[dict]:
    """Find the FIRST stock-adjustment row matching `sku` dated
    after `after_iso`. Returns dict with date/qty/ref for the
    citation, or None if no match."""
    if adjustments is None or adjustments.empty or not sku:
        return None
    sku_col = next(
        (c for c in ("SKU", "ProductCode") if c in adjustments.columns),
        None)
    if not sku_col:
        return None
    date_col = next(
        (c for c in ("AdjustmentDate", "Date", "EffectiveDate",
                        "LastUpdatedDate")
          if c in adjustments.columns), None)
    qty_col = next(
        (c for c in ("Quantity", "Qty", "AdjustQty",
                        "QuantityAdjustment")
          if c in adjustments.columns), None)
    ref_col = next(
        (c for c in ("Reference", "AdjustmentNumber", "Note",
                        "Memo", "Reason")
          if c in adjustments.columns), None)
    df = adjustments[
        adjustments[sku_col].astype(str).str.upper().str.strip()
        == sku.upper()]
    if df.empty:
        return None
    # Filter by date
    if date_col and after_iso:
        try:
            cutoff = pd.to_datetime(after_iso, utc=True)
            dates = pd.to_datetime(
                df[date_col], errors="coerce", utc=True)
            df = df[dates >= cutoff]
        except Exception:
            pass
    if df.empty:
        return None
    if date_col:
        df = df.copy()
        df["__d"] = pd.to_datetime(
            df[date_col], errors="coerce", utc=True)
        df = df.sort_values("__d", na_position="last")
    row = df.iloc[0]
    return {
        "date": (str(row.get(date_col))[:10]
                  if date_col and pd.notna(row.get(date_col))
                  else None),
        "qty": row.get(qty_col) if qty_col else None,
        "reference": row.get(ref_col) if ref_col else None,
    }


def _find_shipment_for_so(shipments: pd.DataFrame,
                               so_number: str,
                               after_iso: str) -> Optional[dict]:
    """Find a shipment matching the SO number with a non-empty
    ShipDate after `after_iso`. Mirrors prefix-stripping pattern
    from ai_tools.get_shipping_details."""
    if shipments is None or shipments.empty or not so_number:
        return None
    order_col = next(
        (c for c in ("OrderNumber", "Order Number",
                        "SaleNumber", "InvoiceNumber")
          if c in shipments.columns), None)
    date_col = next(
        (c for c in ("ShipDate", "ShipmentDate")
          if c in shipments.columns), None)
    track_col = next(
        (c for c in ("TrackingNumber", "Tracking")
          if c in shipments.columns), None)
    voided_col = next(
        (c for c in ("Voided", "IsVoided")
          if c in shipments.columns), None)
    if not order_col or not date_col:
        return None
    norm = _normalise_order_id(so_number)
    if not norm:
        return None
    df = shipments[
        shipments[order_col].astype(str).apply(_normalise_order_id)
        == norm]
    if df.empty:
        return None
    # Has ship date + not voided
    df = df[df[date_col].notna()
              & (df[date_col].astype(str).str.strip() != "")]
    if voided_col and voided_col in df.columns:
        df = df[~df[voided_col].fillna(False).astype(bool)]
    if df.empty:
        return None
    if after_iso:
        try:
            cutoff = pd.to_datetime(after_iso, utc=True)
            dates = pd.to_datetime(
                df[date_col], errors="coerce", utc=True)
            df = df[dates >= cutoff]
        except Exception:
            pass
    if df.empty:
        return None
    row = df.iloc[0]
    return {
        "ship_date": str(row.get(date_col))[:10],
        "tracking": (str(row.get(track_col) or "").strip()
                      if track_col else None),
    }


def run_auto_resolution(dryrun: bool = False) -> dict:
    """For each open stock_issue, attempt to find CIN7 evidence
    that it's already been handled and auto-resolve. Posts a
    quiet confirmation in the original thread when it closes one
    so the team sees the citation. Returns summary dict.

    Logic:
      - count_wrong issues: scan stock_adjustments for matching
        SKU dated after issue creation
      - supply_query issues: scan shipments for matching SO with
        ShipDate after issue creation
    """
    open_issues = db.list_open_stock_issues(
        limit=200, max_age_days=60)
    if not open_issues:
        return {"resolved": 0, "pending": 0}

    adjustments = _load_latest_adjustments()
    shipments = _load_latest_shipments()

    n_resolved_adj = 0
    n_resolved_ship = 0
    n_unresolved = 0
    for iss in open_issues:
        skus_csv = iss.get("skus") or ""
        sos_csv = iss.get("so_numbers") or ""
        skus = [s for s in skus_csv.split(",") if s]
        sos = [s for s in sos_csv.split(",") if s]
        issue_type = (iss.get("issue_type") or "").lower()
        created_at = iss.get("created_at") or ""
        resolved = False
        citation = None

        # Try adjustment evidence for count_wrong (or mixed)
        if not resolved and issue_type in (
                "count_wrong", "mixed") and adjustments is not None:
            for sku in skus:
                ev = _find_adjustment_for_sku(
                    adjustments, sku, created_at)
                if ev:
                    qty = ev.get("qty")
                    qty_s = (f"{qty:+g}"
                              if qty is not None
                              and not pd.isna(qty) else "?")
                    citation = (
                        f"Auto-resolved: stock adjustment for "
                        f"{sku} on {ev.get('date') or '?'} "
                        f"(qty {qty_s})")
                    if ev.get("reference"):
                        citation += f" — ref {ev['reference']}"
                    resolved = True
                    n_resolved_adj += 1
                    break

        # Try shipment evidence for supply_query (or mixed)
        if not resolved and issue_type in (
                "supply_query", "mixed") and shipments is not None:
            for so in sos:
                ev = _find_shipment_for_so(
                    shipments, so, created_at)
                if ev:
                    citation = (
                        f"Auto-resolved: {so} shipped "
                        f"{ev.get('ship_date') or '?'}")
                    if ev.get("tracking"):
                        citation += (
                            f" — tracking {ev['tracking']}")
                    resolved = True
                    n_resolved_ship += 1
                    break

        if not resolved:
            n_unresolved += 1
            continue

        if dryrun:
            log.info("[DRYRUN] Would resolve issue %s — %s",
                      iss["id"], citation)
            continue

        try:
            db.resolve_stock_issue(
                int(iss["id"]),
                resolved_by="auto",
                resolution_text=citation)
            # Post citation as a thread reply for audit trail
            thread_ts = iss.get("raise_thread_ts")
            channel = iss.get("raise_channel")
            if thread_ts and channel:
                _post_to_slack(
                    channel,
                    f"✅ _{citation}_",
                    thread_ts=thread_ts)
        except Exception as exc:
            log.error("Failed to auto-resolve issue %s: %s",
                        iss["id"], exc)
    return {
        "open_scanned": len(open_issues),
        "resolved_via_adjustment": n_resolved_adj,
        "resolved_via_shipment": n_resolved_ship,
        "still_unresolved": n_unresolved,
    }


# ---------------------------------------------------------------------------
# Morning summary
# ---------------------------------------------------------------------------
def run_morning_summary(dryrun: bool = False) -> dict:
    """Post a daily summary of outstanding stock issues to the
    #stock-issues-queries channel.

    v2.67.155 — runs auto-resolution FIRST so issues that have
    evidence of being handled in CIN7 (stock adjustments / SO
    shipments) drop off the list automatically and only the
    genuinely-unresolved ones get reported."""
    channel = os.environ.get(
        "SLACK_STOCK_ISSUES_CHANNEL_ID", "").strip()
    if not dryrun and not channel:
        return {"posted": 0, "skipped_no_channel": True}
    # v2.67.155 — auto-resolve before listing
    auto_result = run_auto_resolution(dryrun=dryrun)
    log.info("Auto-resolution: %s", auto_result)
    issues = db.list_open_stock_issues(limit=50, max_age_days=30)
    if not issues:
        return {"posted": 0, "open_count": 0}
    lines = [f"📋 *Outstanding stock issues — "
              f"{len(issues)} open as of "
              f"{datetime.now().strftime('%-d %b %Y')}*", ""]
    for iss in issues:
        try:
            ts = pd.to_datetime(iss["created_at"])
            age_days = int(
                (pd.Timestamp.now() - ts).total_seconds() / 86400)
        except Exception:
            age_days = 0
        age_text = (f"{age_days} day{'s' if age_days != 1 else ''}"
                      if age_days > 0 else "<1 day")
        sos = iss.get("so_numbers") or ""
        skus = iss.get("skus") or ""
        primary = sos or skus or "(no identifier)"
        bullet = (f"• *{primary.split(',')[0]}* — raised "
                    f"{age_text} ago by "
                    f"{iss.get('raised_by') or '?'} "
                    f"· status _{iss.get('status')}_")
        lines.append(bullet)
    lines.append("")
    lines.append(
        "_Reply 'fixed' / 'adjusted' / 'no change' in each "
        "thread once handled._")
    text = "\n".join(lines)
    if dryrun:
        print(text)
        return {"posted": 0, "open_count": len(issues),
                  "dryrun": True}
    posted_ts, err = _post_to_slack(channel, text)
    if err:
        return {"posted": 0, "error": err,
                  "open_count": len(issues)}
    return {"posted": 1, "open_count": len(issues)}


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def _setup_log(verbose: bool = False) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format=LOG_FORMAT, stream=sys.stdout, force=True)


# ---------------------------------------------------------------------------
# Thread-reply poller (v2.67.245)
# ---------------------------------------------------------------------------
# Slack's conversations.history (the channel-level poll) returns
# top-level messages + broadcast replies, but NOT regular in-
# thread replies. So when Jamie replied 'fixed' in a stock-issue
# thread WITHOUT ticking "Also send to channel", the bot never saw
# the reply and the issue stayed awaiting_response — SO-56536 is
# the example Brandon flagged. Fix: for every open stock_issue,
# poll conversations.replies directly and run the same resolution-
# keyword detection on each in-thread reply.
def check_open_issues_for_replies(dryrun: bool = False) -> dict:
    """Poll conversations.replies for every open stock_issue and
    apply maybe_resolve_from_thread_reply to each human reply."""
    token = os.environ.get("SLACK_BOT_TOKEN", "").strip()
    if not token:
        return {"checked": 0, "skipped_no_token": True}
    issues = db.list_open_stock_issues(
        limit=200, max_age_days=14)
    if not issues:
        return {"checked": 0, "resolved": 0}
    try:
        import slack_sync
    except ImportError as exc:
        return {"error": f"slack_sync import: {exc}"}
    session = slack_sync._build_session(token)
    n_checked = 0
    n_resolved = 0
    for issue in issues:
        ch = issue.get("raise_channel")
        ts = (issue.get("raise_thread_ts")
              or issue.get("raise_ts"))
        if not ch or not ts:
            continue
        n_checked += 1
        try:
            body = slack_sync._slack_get(
                session, "conversations.replies",
                {"channel": ch, "ts": ts, "limit": 50})
        except Exception as exc:  # noqa: BLE001
            log.warning("conversations.replies %s/%s failed: %s",
                          ch, ts, exc)
            continue
        msgs = body.get("messages") or []
        if len(msgs) < 2:
            continue  # parent only — no replies
        # Skip the parent (msgs[0]) and scan thread replies.
        for m in msgs[1:]:
            user_id = m.get("user") or m.get("bot_id") or ""
            is_bot = bool(m.get("bot_id")
                          or m.get("subtype") == "bot_message")
            if is_bot:
                continue
            text = m.get("text") or ""
            if not any(k in text.lower()
                       for k in _RESOLUTION_KEYWORDS):
                continue
            user_name = ""
            if user_id:
                try:
                    user_name = slack_sync._resolve_user(
                        session, user_id)
                except Exception:  # noqa: BLE001
                    user_name = user_id
            reply_msg = {
                "text": text,
                "channel_id": ch,
                "thread_ts": ts,
                "user_id": user_id,
                "user_name": user_name,
                "is_bot": False,
                "is_our_bot": False,
                "ts": m.get("ts"),
            }
            if dryrun:
                log.info(
                    "[DRY] would resolve issue %s from %s: %r",
                    issue["id"], user_name, text[:80])
                break
            if maybe_resolve_from_thread_reply(reply_msg):
                n_resolved += 1
                break  # first matching reply resolves the issue
    return {"checked": n_checked, "resolved": n_resolved}


def cmd_check_replies(args: argparse.Namespace) -> int:
    _setup_log(args.verbose)
    result = check_open_issues_for_replies(
        dryrun=bool(args.dryrun))
    log.info("DONE: %s", result)
    return 0


def cmd_escalate(args: argparse.Namespace) -> int:
    _setup_log(args.verbose)
    hours = int(os.environ.get(
        "STOCK_ISSUE_ESCALATION_HOURS", "4") or 4)
    result = run_escalation_cycle(
        dryrun=bool(args.dryrun), min_age_hours=hours)
    log.info("DONE: %s", result)
    return 0


def cmd_morning(args: argparse.Namespace) -> int:
    _setup_log(args.verbose)
    result = run_morning_summary(dryrun=bool(args.dryrun))
    log.info("DONE: %s", result)
    return 0


def cmd_auto_resolve(args: argparse.Namespace) -> int:
    _setup_log(args.verbose)
    result = run_auto_resolution(dryrun=bool(args.dryrun))
    log.info("DONE: %s", result)
    return 0


def cmd_inspect(args: argparse.Namespace) -> int:
    _setup_log(args.verbose)
    with db.connect() as c:
        r = c.execute(
            "SELECT * FROM stock_issues WHERE id = ?",
            (args.issue_id,)).fetchone()
    if not r:
        log.error("No issue with id=%d", args.issue_id)
        return 1
    import json as _json
    print(_json.dumps(dict(r), indent=2, default=str))
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Stock-issues tracker for "
                      "#stock-issues-queries.")
    sub = parser.add_subparsers(dest="cmd", required=True)
    p_e = sub.add_parser("escalate")
    p_e.add_argument("--dryrun", action="store_true")
    p_e.add_argument("--verbose", action="store_true")
    p_e.set_defaults(func=cmd_escalate)
    p_m = sub.add_parser("morning-summary")
    p_m.add_argument("--dryrun", action="store_true")
    p_m.add_argument("--verbose", action="store_true")
    p_m.set_defaults(func=cmd_morning)
    p_a = sub.add_parser("auto-resolve",
        help="Run auto-resolution pass (debug). Standalone — "
              "scans open issues, checks CIN7 evidence, closes "
              "matched ones.")
    p_a.add_argument("--dryrun", action="store_true")
    p_a.add_argument("--verbose", action="store_true")
    p_a.set_defaults(func=cmd_auto_resolve)

    p_i = sub.add_parser("inspect")
    p_i.add_argument("--issue-id", type=int, required=True)
    p_i.add_argument("--verbose", action="store_true")
    p_i.set_defaults(func=cmd_inspect)

    p_cr = sub.add_parser("check-replies",
        help="Poll conversations.replies for each open issue and "
              "close any whose threads contain a 'fixed' / "
              "'adjusted' / 'no change' style reply that the "
              "channel-level history poll missed.")
    p_cr.add_argument("--dryrun", action="store_true")
    p_cr.add_argument("--verbose", action="store_true")
    p_cr.set_defaults(func=cmd_check_replies)

    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
