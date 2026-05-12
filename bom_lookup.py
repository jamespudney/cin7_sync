"""bom_lookup.py (v2.67.128)
==============================

BOM-based parent/child SKU lookup.

The canonical source of parent/child SKU relationships is the
CIN7 Bill of Materials (BOM). `cin7_sync.sync_boms()` writes a
nightly `boms_YYYY-MM-DD.csv` with columns:
  AssemblySKU, AssemblyName,
  ComponentSKU, ComponentName,
  Quantity, BOMType, AutoAssembly, AutoDisassembly

For LED tube products specifically, a per-foot cut (e.g.
LED-TSWP2835-...-0305) is the AssemblySKU and the master roll
(e.g. LED-TSWP2835-...-100M) is the ComponentSKU. So:

  parent_sku("LED-TSWP2835-...-0305")
    → "LED-TSWP2835-...-100M"

This is the single source of truth: don't use suffix heuristics.

Why a dedicated module
----------------------
- The Streamlit dashboard already builds in-memory BOM indexes
  (BOM_PARENTS, BOM_FAMILY in app.py), but ai_tools.py and the
  Slack bot had no equivalent — they were stuck using suffix
  heuristics from worker_engine.py's is_non_master_tube.
- This module gives both surfaces the same answer for the same
  SKU, with a 5-min cache so the lookup is cheap.

Public API
----------
- parent_sku(child_sku) -> Optional[str]
- children_of(master_sku) -> list[str]
- family_of(sku) -> Optional[str]   # canonical parent or self
- freshness_status() -> dict        # for diagnostics
"""

from __future__ import annotations

import glob
import logging
import os
import time
from pathlib import Path
from typing import List, Optional

import pandas as pd

from data_paths import DATA_DIR

log = logging.getLogger("bom_lookup")

_CACHE_TTL_S = 300
_cache: dict = {
    "parents_of": None,
    "children_of": None,
    "family_of": None,
    "loaded_at": 0.0,
    "path": None,
}


def _find_latest_bom_csv() -> Optional[Path]:
    """Locate the most-recent boms_YYYY-MM-DD.csv. cin7_sync writes
    one per sync into DATA_DIR; we always pick the freshest."""
    pattern = str(DATA_DIR / "boms_*.csv")
    candidates = glob.glob(pattern)
    if not candidates:
        return None
    return Path(max(candidates, key=os.path.getmtime))


def _load() -> None:
    """Refresh cached indexes from the freshest BOM CSV. Idempotent
    — uses TTL so frequent callers don't hit disk every time."""
    now = time.time()
    if (_cache["parents_of"] is not None
            and now - _cache["loaded_at"] < _CACHE_TTL_S):
        return
    path = _find_latest_bom_csv()
    if not path:
        # No BOM data yet — log once, leave the cache as empty maps.
        if _cache["parents_of"] is None:
            log.info("No BOM CSV in %s; parent lookups will return None.",
                      DATA_DIR)
            _cache["parents_of"] = {}
            _cache["children_of"] = {}
            _cache["family_of"] = {}
        _cache["loaded_at"] = now
        return
    try:
        df = pd.read_csv(path)
    except Exception as exc:
        log.error("Failed to read BOM CSV %s: %s", path, exc)
        return

    # v2.67.129 — Filter to AssemblyBOM rows. CIN7 stores multiple
    # BOM types in the same export: AssemblyBOM (build assembly
    # FROM components), DisassemblyBOM (split a parent INTO
    # children), and various Manufacture types. Only the Assembly
    # type correctly encodes "this per-foot cut is built from the
    # master roll" — the others have inverted or unrelated
    # semantics. We accept any BOMType containing 'Assembly'
    # (case-insensitive) to be tolerant of casing variations, and
    # fall back to ALL rows if the filtered set is empty (e.g.
    # the export was unusually shaped) — better than no lookup.
    if "BOMType" in df.columns:
        asm_mask = df["BOMType"].fillna("").astype(str).str.contains(
            "assembly", case=False, na=False)
        asm_only = df[asm_mask]
        if not asm_only.empty:
            log.info(
                "Filtering BOM rows: %d AssemblyBOM rows kept "
                "out of %d total", len(asm_only), len(df))
            df = asm_only
        else:
            log.warning(
                "No AssemblyBOM rows found in %s (BOMType values: "
                "%s) — using all rows as fallback",
                path,
                sorted(df["BOMType"].dropna().unique().tolist())[:10])

    parents_of: dict = {}
    children_of: dict = {}
    for _, row in df.iterrows():
        asm = row.get("AssemblySKU")
        comp = row.get("ComponentSKU")
        if not asm or not comp:
            continue
        asm = str(asm).strip()
        comp = str(comp).strip()
        if not asm or not comp:
            continue
        parents_of.setdefault(asm, []).append({
            "ComponentSKU": comp,
            "ComponentName": row.get("ComponentName"),
            "Quantity": row.get("Quantity"),
            "BOMType": row.get("BOMType"),
        })
        children_of.setdefault(comp, []).append({
            "AssemblySKU": asm,
            "AssemblyName": row.get("AssemblyName"),
            "Quantity": row.get("Quantity"),
            "BOMType": row.get("BOMType"),
        })
    # Family = primary parent (first listed component). Masters
    # represent themselves.
    family_of: dict = {}
    for asm, parents in parents_of.items():
        if parents:
            family_of[asm] = parents[0]["ComponentSKU"]
    for master in children_of.keys():
        family_of.setdefault(master, master)

    _cache["parents_of"] = parents_of
    _cache["children_of"] = children_of
    _cache["family_of"] = family_of
    _cache["loaded_at"] = now
    _cache["path"] = str(path)
    log.info("Loaded BOM index from %s — %d assemblies, %d masters",
              path, len(parents_of), len(children_of))


def parent_sku(child_sku: str) -> Optional[str]:
    """Return the primary parent (master roll) SKU for a child, or
    None if the child has no BOM entry. Uses the same 'first
    parent listed' convention as the dashboard's parent_sku_for."""
    if not child_sku:
        return None
    _load()
    parents = _cache["parents_of"].get(child_sku)
    if not parents:
        # Try case-insensitive match — CIN7 SKU casing is normally
        # consistent but defensive.
        cu = child_sku.upper()
        for k, v in _cache["parents_of"].items():
            if k.upper() == cu:
                parents = v
                break
    if not parents:
        return None
    return parents[0].get("ComponentSKU")


def children_of(master_sku: str) -> List[str]:
    """Return AssemblySKUs that consume `master_sku` as a component.
    Empty list if none / master not in BOM."""
    if not master_sku:
        return []
    _load()
    rows = _cache["children_of"].get(master_sku) or []
    if not rows:
        mu = master_sku.upper()
        for k, v in _cache["children_of"].items():
            if k.upper() == mu:
                rows = v
                break
    return [r["AssemblySKU"] for r in rows if r.get("AssemblySKU")]


def family_of(sku: str) -> Optional[str]:
    """Canonical parent for grouping; for a master, returns itself.
    For an assembly with multiple parents, returns the primary."""
    if not sku:
        return None
    _load()
    fam = _cache["family_of"].get(sku)
    if fam:
        return fam
    su = sku.upper()
    for k, v in _cache["family_of"].items():
        if k.upper() == su:
            return v
    return None


def is_child(sku: str) -> bool:
    """True if `sku` has at least one parent in the BOM (i.e. it's
    assembled from another SKU). False for masters and unknowns."""
    return parent_sku(sku) is not None


def freshness_status() -> dict:
    """For diagnostics — when the BOM index was last loaded and
    from which file."""
    _load()
    path = _cache.get("path")
    if not path:
        return {"available": False}
    try:
        age_s = time.time() - Path(path).stat().st_mtime
    except OSError:
        return {"available": True, "stale_hours": None}
    return {
        "available": True,
        "stale_hours": round(age_s / 3600.0, 1),
        "path": path,
        "n_assemblies": len(_cache.get("parents_of") or {}),
        "n_masters": len(_cache.get("children_of") or {}),
    }
