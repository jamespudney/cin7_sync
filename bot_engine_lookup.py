"""bot_engine_lookup.py (v2.67.124)
====================================

Engine-signal lookup helpers used by the Slack bot's Viktor-overlay
path. Returns the dashboard's intelligence model facts (ABC class,
trend_flag, is_dormant, stock, bin, supplier) for a single SKU or
a whole family, so the bot can overlay them on top of Viktor's
marketing-only answers.

Why a separate module
---------------------
slack_listener.py shouldn't import the heavy ABC engine on every
poll cycle. This file lazily loads engine_df from CSV (cached per
process) so the bot can answer 'what's the ABC class of LED-X'
without re-running the full engine each time.

This is the SLIM mirror of the dashboard's engine — same rules,
same columns, but no recomputation. When the engine_df CSV is
stale (older than `_STALE_HOURS`) we still serve from it but mark
the response so the bot can disclaim drift.

Public API
----------
- lookup_sku_signals(sku) -> dict | None
- lookup_family_signals(family) -> dict | None
"""

from __future__ import annotations

import logging
import os
import time
from pathlib import Path
from typing import Optional

import pandas as pd

from data_paths import DATA_DIR

log = logging.getLogger("bot_engine_lookup")

# Cache the engine frame for 5 min — the nearsync cycle runs every
# 15 min so we never serve more than ~20-min-stale data, which is
# good enough for "is this SKU A-class" questions.
_CACHE_TTL_S = 300
_STALE_HOURS = 24

_cache: dict = {"frame": None, "loaded_at": 0.0, "path": None}


def _engine_csv_candidates() -> list:
    """v2.67.124 — try several paths the engine output might land
    at depending on which service is running. Worker writes to
    DATA_DIR/engine_output.csv; the dashboard's full engine
    sometimes leaves a per-day snapshot."""
    cands = [
        DATA_DIR / "engine_output.csv",
        DATA_DIR / "ordering_engine_output.csv",
        DATA_DIR / "engine_df.csv",
    ]
    return [p for p in cands if p.exists()]


def _load_engine_df() -> Optional[pd.DataFrame]:
    """Return the cached engine_df, loading from CSV if cold or
    expired. None if no engine output is on disk yet."""
    now = time.time()
    if (_cache["frame"] is not None
            and now - _cache["loaded_at"] < _CACHE_TTL_S):
        return _cache["frame"]
    cands = _engine_csv_candidates()
    if not cands:
        log.info("No engine_output CSV found in %s", DATA_DIR)
        return None
    # Prefer the freshest file.
    path = max(cands, key=lambda p: p.stat().st_mtime)
    try:
        df = pd.read_csv(path)
    except Exception as exc:
        log.error("Failed to read engine CSV %s: %s", path, exc)
        return None
    _cache["frame"] = df
    _cache["loaded_at"] = now
    _cache["path"] = str(path)
    log.info("Loaded engine_df from %s (%d rows)", path, len(df))
    return df


def lookup_sku_signals(sku: str) -> Optional[dict]:
    """Return engine signals for one SKU, or None if not found.
    Output keys:
      abc, trend_flag, is_dormant, excess_units, stock, bin,
      family, supplier, last_sold, n_sold_12mo.
    Any field may be None/missing if the engine column wasn't
    computed for that row."""
    if not sku:
        return None
    df = _load_engine_df()
    if df is None or df.empty:
        return None
    # Engine output's SKU column is usually 'Sku' or 'sku'. Try both.
    sku_col = None
    for cand in ("Sku", "sku", "SKU"):
        if cand in df.columns:
            sku_col = cand
            break
    if not sku_col:
        return None
    matches = df[df[sku_col].astype(str).str.upper() == sku.upper()]
    if matches.empty:
        return None
    row = matches.iloc[0]
    return _row_to_signals(row)


def lookup_family_signals(family: str) -> Optional[dict]:
    """Return roll-up engine signals for one family. Used when
    Viktor mentions a family name (e.g. 'Slim8') rather than
    individual SKUs.
    Output keys:
      family, n_total, n_a_class, n_b_class, n_c_class,
      n_dormant, n_excess, family_stock, family_trending_up,
      family_trending_down."""
    if not family:
        return None
    df = _load_engine_df()
    if df is None or df.empty:
        return None
    fam_col = None
    for cand in ("Family", "family", "FAMILY"):
        if cand in df.columns:
            fam_col = cand
            break
    if not fam_col:
        return None
    fam_norm = family.replace(" ", "").upper()
    matches = df[df[fam_col].astype(str)
                  .str.replace(" ", "", regex=False)
                  .str.upper() == fam_norm]
    if matches.empty:
        return None

    def _count_eq(col_candidates, value):
        for c in col_candidates:
            if c in matches.columns:
                return int((matches[c] == value).sum())
        return None

    def _count_truthy(col_candidates):
        for c in col_candidates:
            if c in matches.columns:
                return int(matches[c].fillna(0).astype(bool).sum())
        return None

    def _sum(col_candidates):
        for c in col_candidates:
            if c in matches.columns:
                return float(matches[c].fillna(0).sum())
        return None

    return {
        "family": family,
        "n_total": len(matches),
        "n_a_class": _count_eq(("ABC", "abc", "Abc"), "A"),
        "n_b_class": _count_eq(("ABC", "abc", "Abc"), "B"),
        "n_c_class": _count_eq(("ABC", "abc", "Abc"), "C"),
        "n_dormant": _count_truthy(
            ("is_dormant", "IsDormant", "Is_Dormant")),
        "n_excess": _count_truthy(
            ("excess_units", "ExcessUnits")),
        "family_stock": _sum(
            ("OnHand", "on_hand", "Stock", "stock")),
        "n_trending_up": _count_eq(
            ("trend_flag", "TrendFlag"), "up"),
        "n_trending_down": _count_eq(
            ("trend_flag", "TrendFlag"), "down"),
    }


def _row_to_signals(row: pd.Series) -> dict:
    """Helper: pull the engine fields off a row, tolerant of column
    casing differences between the worker's slim engine and the
    dashboard's full engine."""
    def _get(*cands):
        for c in cands:
            if c in row.index:
                v = row[c]
                if pd.isna(v):
                    return None
                return v
        return None

    out = {
        "abc": _get("ABC", "abc", "Abc"),
        "trend_flag": _get("trend_flag", "TrendFlag"),
        "is_dormant": bool(_get("is_dormant", "IsDormant") or False),
        "excess_units": _get("excess_units", "ExcessUnits"),
        "stock": _get("OnHand", "on_hand", "Stock", "stock"),
        "bin": _get("Bin", "bin", "BinLocation"),
        "family": _get("Family", "family"),
        "supplier": _get("Supplier", "supplier", "SupplierName"),
        "last_sold": _get("LastSold", "last_sold"),
        "n_sold_12mo": _get("Sold12mo", "n_sold_12mo"),
    }
    # Coerce numerics to plain Python types
    for k in ("excess_units", "stock", "n_sold_12mo"):
        v = out.get(k)
        if v is not None:
            try:
                out[k] = float(v)
            except (TypeError, ValueError):
                pass
    return out


def freshness_status() -> dict:
    """Tell the caller how stale the cached engine output is. Used
    by the bot if it wants to disclaim potential drift in the
    overlay."""
    df = _load_engine_df()
    if df is None:
        return {"available": False}
    path = _cache.get("path")
    if not path:
        return {"available": True, "stale_hours": None}
    try:
        age_s = time.time() - Path(path).stat().st_mtime
    except OSError:
        return {"available": True, "stale_hours": None}
    return {
        "available": True,
        "stale_hours": round(age_s / 3600.0, 1),
        "is_stale": age_s > _STALE_HOURS * 3600,
        "path": path,
    }
