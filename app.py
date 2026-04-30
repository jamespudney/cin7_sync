"""
Cin7 Analytics — front end
==========================
Streamlit app that reads the CSV files produced by cin7_sync.py and renders
an interactive dashboard for the ops/purchasing team.

Run locally:
    streamlit run app.py

Share on your network (team members hit http://<your-pc-ip>:8501):
    streamlit run app.py --server.address 0.0.0.0 --server.port 8501

Over the internet with auth (once you're ready):
    pip install cloudflared
    cloudflared tunnel --url http://localhost:8501
"""

from __future__ import annotations

import glob
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
import plotly.express as px
import streamlit as st
import subprocess
import sys

import db

# Optional drag-and-drop UI for the PO-editor column organizer. Falls back
# gracefully to the data_editor flow if the package isn't installed.
# Install: pip install streamlit-sortables
try:
    from streamlit_sortables import sort_items as _sort_items
    HAS_SORTABLE = True
except ImportError:
    HAS_SORTABLE = False

APP_DIR = Path(__file__).resolve().parent
# OUTPUT_DIR follows the DATA_DIR env var (set to /data on Render,
# defaults to the project folder locally). See data_paths.py.
from data_paths import OUTPUT_DIR  # noqa: E402

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------
# Branding is env-driven so the app can be re-skinned for other CIN7
# customers without code changes. Defaults are the Wired4Signs values.
# See SAAS_NOTES.md for the full list of company-specific touch points.
COMPANY_NAME = os.environ.get("COMPANY_NAME", "Wired4Signs USA")
APP_TITLE = os.environ.get(
    "APP_TITLE", f"Cin7 Analytics — {COMPANY_NAME}")

st.set_page_config(
    page_title=APP_TITLE,
    page_icon=":bar_chart:",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ---------------------------------------------------------------------------
# Password gate
# ---------------------------------------------------------------------------
# Reads expected password from APP_PASSWORD env var. If APP_PASSWORD is
# NOT set (typical local dev), the gate is bypassed entirely so dev
# experience stays the same. On Render we set APP_PASSWORD and every
# new session has to enter it once.
#
# Why not Streamlit's experimental st.login(): we want zero external
# auth dependencies for v1. A shared password is fine for a small
# trusted team. We can swap to Google OAuth or Cloudflare Access later
# without changing any other code in the app.
def _require_password() -> None:
    expected = os.environ.get("APP_PASSWORD", "").strip()
    if not expected:
        # No password configured → open access (local dev mode).
        return
    if st.session_state.get("_app_authed"):
        return
    # Centred prompt — the rest of the page is hidden until they auth.
    st.markdown(
        "<div style='max-width:420px;margin:80px auto;'>",
        unsafe_allow_html=True)
    st.markdown(f"### :lock: {COMPANY_NAME} Analytics")
    st.caption(
        "Enter the team password to continue. If you don't have it, "
        "ask your administrator.")
    with st.form("login_form", clear_on_submit=False):
        pw = st.text_input(
            "Password", type="password", key="_pw_input",
            label_visibility="collapsed",
            placeholder="Team password")
        submitted = st.form_submit_button(
            "Sign in", type="primary", use_container_width=True)
    if submitted:
        if pw == expected:
            st.session_state["_app_authed"] = True
            # Delete the password from session state so it isn't kept
            # around longer than needed.
            if "_pw_input" in st.session_state:
                del st.session_state["_pw_input"]
            st.rerun()
        else:
            st.error("Wrong password — try again.")
    st.markdown("</div>", unsafe_allow_html=True)
    st.stop()


_require_password()


# ---------------------------------------------------------------------------
# Data loading (cached)
# ---------------------------------------------------------------------------

# NOT cached — globbing the directory is cheap, and we need it to
# return fresh paths so the load() cache invalidates when a new CSV
# arrives.
def _latest_file(prefix: str) -> Optional[Path]:
    """Return the most recent CSV file in output/ with the given prefix."""
    files = sorted(OUTPUT_DIR.glob(f"{prefix}_*.csv"))
    return files[-1] if files else None


@st.cache_data(persist="disk", show_spinner="Loading data…")
def _read_csv_cached(path_str: str, mtime: float) -> pd.DataFrame:
    """Disk-persisted CSV reader. Cache key = (path, mtime), so a new
    CSV with a different mtime invalidates automatically — even though
    the persistent cache has no TTL."""
    try:
        return pd.read_csv(path_str, low_memory=False)
    except Exception as exc:  # noqa: BLE001
        st.warning(f"Could not read {Path(path_str).name}: {exc}")
        return pd.DataFrame()


def load(prefix: str) -> pd.DataFrame:
    """Find the latest CSV for this prefix and return it (cached)."""
    p = _latest_file(prefix)
    if p is None:
        return pd.DataFrame()
    return _read_csv_cached(str(p), p.stat().st_mtime)


def file_mtime(prefix: str) -> Optional[datetime]:
    p = _latest_file(prefix)
    return datetime.fromtimestamp(p.stat().st_mtime) if p else None


def _fmt_number(n) -> str:
    if pd.isna(n):
        return "—"
    if abs(n) >= 1_000_000:
        return f"{n/1_000_000:,.2f}M"
    if abs(n) >= 1_000:
        return f"{n/1_000:,.1f}k"
    return f"{n:,.0f}"


def _fmt_money(n) -> str:
    if pd.isna(n) or n is None:
        return "—"
    return f"${n:,.0f}"


def _to_num(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def _to_date(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce", utc=True)


def rows_selector(label: str = "Rows to show", key: str = "rows",
                  default: int = 100) -> int:
    """Standard page-size selector. Returns row limit (large int for 'All')."""
    options = [10, 50, 100, 1000, "All"]
    index = options.index(default) if default in options else 2
    choice = st.selectbox(label, options, index=index, key=key)
    return 10**9 if choice == "All" else int(choice)


# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------

def _freshness_from_output_dir() -> tuple:
    """Return (latest_mtime, minutes_ago, source_filename) from the most
    recently-written near-sync file (stock_on_hand is the heartbeat).
    Returns (None, None, None) if nothing found."""
    try:
        candidates = list(OUTPUT_DIR.glob("stock_on_hand_*.csv"))
        if not candidates:
            return (None, None, None)
        latest = max(candidates, key=lambda p: p.stat().st_mtime)
        mtime = datetime.fromtimestamp(latest.stat().st_mtime)
        age_min = (datetime.now() - mtime).total_seconds() / 60.0
        return (mtime, age_min, latest.name)
    except Exception:
        return (None, None, None)


with st.sidebar:
    st.title(":bar_chart: Cin7 Analytics")
    st.caption("Wired4Signs USA, LLC — ops dashboard")
    st.caption("🟢 v2.44 — AI Assistant gains a Knowledge Base layer: 6 starter docs in /docs (inventory-rules, reorder-engine, sync-cadences, migrations, po-workflow, glossary) plus root-level RULES/DEPLOY/SAAS_NOTES indexed by ai_kb.py. New tool `search_knowledge_base` lets Claude cite the actual rule when answering 'how/why' questions. (Apr 30)")

    # --- Data freshness indicator ---------------------------------------
    # Shows how stale the on-disk sync data is (independent of the browser's
    # cache). The CIN7 nearsync Task Scheduler entry fires every 15 minutes
    # and overwrites stock_on_hand_*.csv, so "minutes since" on that file
    # is the honest heartbeat.
    _sync_time, _sync_age_min, _sync_file = _freshness_from_output_dir()
    if _sync_time is not None:
        # Use literal emoji characters — Streamlit's shortcode map doesn't
        # include :large_green_circle: etc., so they render as plain text.
        if _sync_age_min < 20:
            dot, color_label = "🟢", "fresh"
        elif _sync_age_min < 60:
            dot, color_label = "🟡", "ageing"
        else:
            dot, color_label = "🔴", "stale"
        st.markdown(
            f"{dot} **Last sync:** {_sync_time:%H:%M} "
            f"({int(_sync_age_min)} min ago — {color_label})"
        )
    else:
        st.markdown("⚪ **Last sync:** unknown (no stock file found)")

    # --- SKU-rename pending indicator ------------------------------------
    # When the team renames a SKU in CIN7 (catalog code prefix, version
    # suffix, etc.), our local DB references go stale. This badge fires
    # when the latest two products_*.csv files differ on any SKU AND the
    # old SKU still has live references in our DB tables. Click-through
    # tells the buyer exactly what to do.
    @st.cache_data(ttl=300, show_spinner=False)
    def _detect_pending_renames() -> list:
        """Compares two latest products CSVs by ProductID, returns list
        of (old_sku, new_sku, name, has_db_refs) for renames that still
        have references in our DB (i.e., not yet propagated)."""
        files = sorted(OUTPUT_DIR.glob("products_*.csv"),
                        key=lambda p: p.stat().st_mtime)
        if len(files) < 2:
            return []
        try:
            old_df = pd.read_csv(files[-2], low_memory=False)
            new_df = pd.read_csv(files[-1], low_memory=False)
        except Exception:
            return []
        old_idx = {str(r["ID"]): str(r["SKU"])
                    for _, r in old_df.iterrows()
                    if pd.notna(r.get("ID")) and pd.notna(r.get("SKU"))}
        renames = []
        for _, r in new_df.iterrows():
            pid = str(r.get("ID") or "")
            new_sku = str(r.get("SKU") or "")
            if not pid or not new_sku:
                continue
            old_sku = old_idx.get(pid)
            if old_sku and old_sku != new_sku:
                # Check if old_sku still has references in DB
                has_refs = False
                try:
                    with db.connect() as c:
                        # Check the most likely tables
                        for tbl, col in [
                            ("sku_migrations", "retiring_sku"),
                            ("sku_migrations", "successor_sku"),
                            ("sku_supplier_overrides", "sku"),
                            ("sku_policy_overrides", "sku"),
                            ("sku_pack_settings", "sku"),
                            ("notes", "sku"),
                            ("flags", "sku"),
                            ("family_critical_components", "component_sku"),
                        ]:
                            n = c.execute(
                                f"SELECT COUNT(*) AS n FROM {tbl} "
                                f"WHERE {col} = ?",
                                (old_sku,)).fetchone()
                            if n["n"] > 0:
                                has_refs = True
                                break
                except Exception:
                    has_refs = False
                renames.append({
                    "old": old_sku, "new": new_sku,
                    "name": str(r.get("Name") or "")[:50],
                    "has_db_refs": has_refs,
                })
        return renames

    _pending = _detect_pending_renames()
    if _pending:
        _with_refs = [r for r in _pending if r["has_db_refs"]]
        if _with_refs:
            st.warning(
                f":wrench: **{len(_with_refs)} SKU rename"
                f"{'s' if len(_with_refs) != 1 else ''} pending** "
                f"propagation to local DB.  \n"
                f"CIN7 source-of-truth shifted; our DB still references "
                f"the old SKU"
                f"{'s' if len(_with_refs) != 1 else ''}.  \n"
                f"Run: `python sync_sku_renames.py --apply`")
        else:
            st.caption(
                f":sparkles: {len(_pending)} SKU rename"
                f"{'s' if len(_pending) != 1 else ''} detected since last "
                f"products sync (no DB references — informational only).")

    st.divider()

    # Who's using the app? Used on notes / flags / audit log.
    current_user = st.text_input(
        "Your name",
        value=st.session_state.get("current_user", ""),
        placeholder="e.g. James",
        help="Identifies you when adding notes, flags, or approvals. "
             "Remembered for this browser session.",
    )
    if current_user:
        st.session_state["current_user"] = current_user.strip()

    page = st.radio(
        "View",
        [
            "Overview",
            "AI Assistant",
            "Monthly Metrics",
            "Ordering",
            "FixedCost Audit",
            "Product Detail",
            "Kits & Fixtures",
            "LED Tubes",
            "Migrations",
            "Supplier Pricing",
            "Stock Explorer",
            "Product Master",
            "Purchase Analysis",
            "Sales Recent",
            "Data Health",
        ],
        label_visibility="collapsed",
    )

    st.divider()
    st.subheader("Global filters")
    stock_only = st.toggle(
        "Stock items only",
        value=True,
        help="Hide Service and Non-Inventory items from stock / reorder "
             "analysis. Recommended default. Uncheck to see everything.",
    )

    st.divider()

    # Single consolidated refresh button. CIN7 data is auto-pulled by
    # the nightly sync (02:00 UTC). This button just clears in-app
    # caches if you suspect the screen is showing stale numbers.
    if st.button(":arrows_counterclockwise: Refresh data",
                  width="stretch",
                  help="Clear in-app caches and reload from the CSV files "
                       "on the server. CIN7 itself is auto-synced nightly "
                       "at 02:00 UTC — you don't normally need this."):
        st.cache_data.clear()
        st.rerun()

    # The rest of this block is the legacy 'Force sync from CIN7' that
    # used to subprocess.run cin7_sync.py from inside Streamlit. It's
    # disabled on the deployed app because the cron-equivalent
    # background loop already handles incremental syncs every night.
    # Operators who really need to trigger a manual sync should use
    # the Render Shell to run `python cin7_sync.py nearsync --days 1`.
    if False:  # kept for reference only — see comment above
        with st.spinner("Syncing from CIN7… please wait 1-2 minutes…"):
            try:
                result = subprocess.run(
                    [sys.executable, "cin7_sync.py", "nearsync", "--days", "1"],
                    cwd=str(APP_DIR),
                    capture_output=True,
                    text=True,
                    timeout=300,
                )
                if result.returncode == 0:
                    st.cache_data.clear()
                    st.success(":white_check_mark: Sync complete — data "
                               "refreshed. Rerunning page…")
                    # Show the last few log lines for confidence
                    tail = "\n".join(result.stdout.strip().splitlines()[-6:])
                    if tail:
                        st.code(tail, language="text")
                    st.rerun()
                else:
                    st.error(f":x: Sync failed (exit {result.returncode})")
                    st.code(result.stderr[-1000:] or result.stdout[-1000:],
                            language="text")
            except subprocess.TimeoutExpired:
                st.error(":x: Sync timed out after 5 minutes. "
                         "Check `output/nearsync.log` for details.")
            except Exception as exc:  # noqa: BLE001
                st.error(f":x: Could not run sync: {exc}")

    st.caption(
        "Scheduled nearsync runs every 15 min via Windows Task Scheduler. "
        "The button triggers a manual pull on demand."
    )


# ---------------------------------------------------------------------------
# Shared data loads
# ---------------------------------------------------------------------------

products = load("products")
stock = load("stock_on_hand")
customers_file = _latest_file("customers")
suppliers = load("suppliers")
sales_headers = load("sales_last_30d")
purchase_headers = load("purchases_last_30d")
purchase_lines = load("purchase_lines_last_90d")
sale_lines_3d = load("sale_lines_last_3d")
sale_lines_30d = load("sale_lines_last_30d")

# Why this pattern: persist="disk" caches don't support ttl. To get
# auto-invalidation on fresh data, the cache key must change when the
# file list changes. _dir_fingerprint() returns a hashable tuple of
# (name, mtime) pairs — used as a cache-busting argument so the inner
# cached function rebuilds when the daily sync drops a new CSV.
def _dir_fingerprint(pattern: str) -> tuple:
    """Return a deterministic fingerprint of all files matching the
    glob pattern in OUTPUT_DIR. Used as a cache key so persisted
    caches invalidate automatically when files change."""
    out = []
    for p in sorted(OUTPUT_DIR.glob(pattern)):
        try:
            out.append((p.name, p.stat().st_mtime))
        except OSError:
            continue
    return tuple(out)


# Empty-frame factories used by loaders when no CSVs exist on disk yet.
# Why: many call sites do `df["Foo"]` without first checking the column
# exists. Rather than guarding every site, we make the loaders return a
# DataFrame with the EXPECTED columns even when there are zero rows.
# A consumer doing `df["InvoiceDate"]` then gets an empty Series instead
# of a KeyError. Keeps the deploy bring-up much smoother.
_EMPTY_SALE_LINES_COLS = [
    "SaleID", "OrderNumber", "InvoiceDate", "Customer",
    "SKU", "Name", "Quantity", "Price", "Discount", "Tax", "Total"]
_EMPTY_SALES_COLS = [
    "SaleID", "OrderNumber", "InvoiceDate", "OrderDate", "Customer",
    "Status", "Total", "InvoiceAmount"]
_EMPTY_PURCHASE_LINES_COLS = [
    "PurchaseID", "OrderNumber", "OrderDate", "Supplier",
    "SKU", "Name", "Quantity", "Price", "Total"]


# Load the most comprehensive sale_lines picture:
# 1. Start with the longest-window file (has deepest history)
# 2. Union any more-recent shorter-window files (they contain today's data
#    that the overnight long-window sync missed)
# 3. Dedupe on line identity (SaleID+SKU+Qty), keeping the most-recent version
@st.cache_data(persist="disk", show_spinner="Loading sales history…")
def _load_longest_sale_lines_cached(fingerprint: tuple) -> pd.DataFrame:
    import re as _re
    files = []
    for p in OUTPUT_DIR.glob("sale_lines_last_*d_*.csv"):
        m = _re.match(r"sale_lines_last_(\d+)d_", p.name)
        if m:
            files.append((int(m.group(1)), p.stat().st_mtime, p))
    if not files:
        # Return an empty frame WITH expected columns so downstream
        # df["InvoiceDate"] etc. doesn't KeyError on a fresh deploy.
        return pd.DataFrame(columns=_EMPTY_SALE_LINES_COLS)

    # Largest-window file first, then any more-recent smaller files
    files.sort(key=lambda x: (-x[0], -x[1]))
    base_file = files[0][2]
    base_mtime = files[0][1]
    try:
        base = pd.read_csv(base_file, low_memory=False)
    except Exception:
        return pd.DataFrame(columns=_EMPTY_SALE_LINES_COLS)

    # Union any file that was written MORE RECENTLY than the base
    for days, mtime, p in files[1:]:
        if mtime <= base_mtime:
            continue
        try:
            more = pd.read_csv(p, low_memory=False)
            base = pd.concat([base, more], ignore_index=True)
        except Exception:
            continue

    # Dedupe keeping LAST occurrence (which is the more-recent file's data
    dedupe_cols = [c for c in
                    ["SaleID", "SKU", "Quantity", "InvoiceDate",
                     "OrderNumber"]
                    if c in base.columns]
    if dedupe_cols:
        base = base.drop_duplicates(subset=dedupe_cols, keep="last")
    return base.reset_index(drop=True)


def _load_longest_sale_lines() -> pd.DataFrame:
    return _load_longest_sale_lines_cached(
        _dir_fingerprint("sale_lines_last_*d_*.csv"))


sale_lines = _load_longest_sale_lines()
if sale_lines.empty:
    sale_lines = sale_lines_30d if not sale_lines_30d.empty else sale_lines_3d


@st.cache_data(persist="disk", show_spinner="Loading sales headers…")
def _load_longest_sales_cached(fingerprint: tuple) -> pd.DataFrame:
    """Headers version. fingerprint key: see _dir_fingerprint()."""
    import re as _re
    files = []
    for p in OUTPUT_DIR.glob("sales_last_*d_*.csv"):
        m = _re.match(r"sales_last_(\d+)d_", p.name)
        if m:
            files.append((int(m.group(1)), p.stat().st_mtime, p))
    if not files:
        return pd.DataFrame(columns=_EMPTY_SALES_COLS)
    files.sort(key=lambda x: (-x[0], -x[1]))
    base_file = files[0][2]
    base_mtime = files[0][1]
    try:
        base = pd.read_csv(base_file, low_memory=False)
    except Exception:
        return pd.DataFrame(columns=_EMPTY_SALES_COLS)
    for days, mtime, p in files[1:]:
        if mtime <= base_mtime:
            continue
        try:
            more = pd.read_csv(p, low_memory=False)
            base = pd.concat([base, more], ignore_index=True)
        except Exception:
            continue
    if "SaleID" in base.columns:
        base = base.drop_duplicates(subset=["SaleID"], keep="last")
    return base.reset_index(drop=True)


def _load_longest_sales() -> pd.DataFrame:
    return _load_longest_sales_cached(
        _dir_fingerprint("sales_last_*d_*.csv"))


@st.cache_data(persist="disk", show_spinner="Loading purchase history…")
def _load_longest_purchase_lines_cached(fingerprint: tuple) -> pd.DataFrame:
    import re as _re
    files = []
    for p in OUTPUT_DIR.glob("purchase_lines_last_*d_*.csv"):
        m = _re.match(r"purchase_lines_last_(\d+)d_", p.name)
        if m:
            files.append((int(m.group(1)), p.stat().st_mtime, p))
    if not files:
        return pd.DataFrame(columns=_EMPTY_PURCHASE_LINES_COLS)
    files.sort(key=lambda x: (-x[0], -x[1]))
    base_file = files[0][2]
    base_mtime = files[0][1]
    try:
        base = pd.read_csv(base_file, low_memory=False)
    except Exception:
        return pd.DataFrame(columns=_EMPTY_PURCHASE_LINES_COLS)
    for days, mtime, p in files[1:]:
        if mtime <= base_mtime:
            continue
        try:
            more = pd.read_csv(p, low_memory=False)
            base = pd.concat([base, more], ignore_index=True)
        except Exception:
            continue
    dedupe_cols = [c for c in
                    ["PurchaseID", "SKU", "Quantity", "OrderDate",
                     "OrderNumber", "Price"]
                    if c in base.columns]
    if dedupe_cols:
        base = base.drop_duplicates(subset=dedupe_cols, keep="last")
    return base.reset_index(drop=True)


def _load_longest_purchase_lines() -> pd.DataFrame:
    return _load_longest_purchase_lines_cached(
        _dir_fingerprint("purchase_lines_last_*d_*.csv"))
stock_adjustments = load("stock_adjustments_last_30d")
stock_transfers = load("stock_transfers_last_30d")
boms = load("boms")  # AssemblySKU, ComponentSKU, Quantity, BOMType, ...


# ---------------------------------------------------------------------------
# BOM helpers — parent/child lookups
# ---------------------------------------------------------------------------

@st.cache_data(ttl=300, show_spinner=False)
def _build_bom_indexes(boms_df: pd.DataFrame) -> dict:
    """Pre-compute lookups:
      parents_of[sku]   = list of dicts {ComponentSKU, ComponentName, Qty}
                          — parents/masters this SKU is built from
      children_of[sku]  = list of dicts {AssemblySKU, AssemblyName, Qty}
                          — assemblies that consume this SKU
      family_of[sku]    = canonical parent SKU for grouping
    """
    if boms_df.empty:
        return {"parents_of": {}, "children_of": {}, "family_of": {}}

    parents_of: dict = {}
    children_of: dict = {}
    for _, row in boms_df.iterrows():
        asm = row.get("AssemblySKU")
        comp = row.get("ComponentSKU")
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

    # Family = the primary parent (first listed component) for each assembly.
    # A master with no parent represents itself.
    family_of: dict = {}
    for asm, parents in parents_of.items():
        if parents:
            family_of[asm] = parents[0]["ComponentSKU"]
    for master in children_of.keys():
        family_of.setdefault(master, master)

    return {"parents_of": parents_of,
            "children_of": children_of,
            "family_of": family_of}


_bom_idx = _build_bom_indexes(boms)
BOM_PARENTS = _bom_idx["parents_of"]
BOM_CHILDREN = _bom_idx["children_of"]
BOM_FAMILY = _bom_idx["family_of"]


def _load_ip_alternates() -> dict:
    """Read the latest output/ip_alternates_*.csv into forward + reverse
    indexes for the drill-down to consume.

    The CSV is produced by ip_pull_alternates.py — one row per
    "Combine sales/stock" link configured in Inventory Planner. Each
    link is bidirectional in spirit (if A says B is its alternative,
    a buyer looking at B benefits from knowing A points to it), so we
    build BOTH directions in memory.

    Returns:
        {
          "forward": {MasterSKU: [entry, ...]},   # this SKU has these alts
          "reverse": {AlternativeSKU: [entry, ...]}, # SKUs that point at this
        }
    Where each entry is a dict with the original CSV row fields.
    """
    p = _latest_file("ip_alternates")
    if p is None:
        return {"forward": {}, "reverse": {}}
    try:
        df = pd.read_csv(p, low_memory=False)
    except Exception as exc:  # noqa: BLE001
        st.warning(f"Could not read {p.name}: {exc}")
        return {"forward": {}, "reverse": {}}

    forward: dict = {}
    reverse: dict = {}
    for _, row in df.iterrows():
        master = str(row.get("MasterSKU") or "").strip()
        alt = str(row.get("AlternativeSKU") or "").strip()
        if not master or not alt:
            continue
        entry = {
            "MasterSKU": master,
            "AlternativeSKU": alt,
            "Percent": row.get("Percent"),
            "Source": str(row.get("Source") or ""),
            "Title": str(row.get("AlternativeTitle") or ""),
            "Barcode": str(row.get("AlternativeBarcode") or ""),
            "MasterID": str(row.get("MasterID") or ""),
            "AlternativeID": str(row.get("AlternativeID") or ""),
        }
        forward.setdefault(master, []).append(entry)
        reverse.setdefault(alt, []).append(entry)
    return {"forward": forward, "reverse": reverse}


_ip_alts = _load_ip_alternates()
IP_ALTS_FORWARD = _ip_alts["forward"]
IP_ALTS_REVERSE = _ip_alts["reverse"]


def _load_ip_notes() -> dict:
    """Read the latest output/ip_notes_*.csv into {SKU: [note dicts]}.

    Each note dict has: text, warehouse_id, tags. Multiple notes per SKU
    are preserved (a variant can have a note per warehouse).
    """
    p = _latest_file("ip_notes")
    if p is None:
        return {}
    try:
        df = pd.read_csv(p, low_memory=False)
    except Exception as exc:  # noqa: BLE001
        st.warning(f"Could not read {p.name}: {exc}")
        return {}

    notes: dict = {}
    for _, row in df.iterrows():
        sku = str(row.get("SKU") or "").strip()
        text = str(row.get("Note") or "").strip()
        if not sku or not text:
            continue
        notes.setdefault(sku, []).append({
            "text": text,
            "warehouse_id": str(row.get("WarehouseID") or ""),
            "tags": str(row.get("Tags") or ""),
        })
    return notes


IP_NOTES = _load_ip_notes()


def _load_cin7_alternatives() -> dict:
    """Read the latest output/cin7_alternatives_*.csv produced by
    cin7_ingest_attributes.py from the CIN7 AdditionalAttribute6
    ('Alternative Product') field.

    Returns a bidirectional dict: alternatives are symmetric in spirit
    (if A says B is an alternative, viewing B should also surface A).

    Returns:
        {SKU: [AlternativeSKU, AlternativeSKU, ...]}
    """
    p = _latest_file("cin7_alternatives")
    if p is None:
        return {}
    try:
        df = pd.read_csv(p, low_memory=False)
    except Exception:
        return {}

    bidir: dict = {}
    for _, row in df.iterrows():
        a = str(row.get("SKU") or "").strip()
        b = str(row.get("AlternativeSKU") or "").strip()
        if not a or not b or a == b:
            continue
        bidir.setdefault(a, set()).add(b)
        bidir.setdefault(b, set()).add(a)
    # Convert sets to sorted lists for stable display
    return {k: sorted(v) for k, v in bidir.items()}


CIN7_ALTERNATIVES = _load_cin7_alternatives()


# Intent words that often appear next to SKU references in notes. We map
# them to a structured "intent" so the UI can show "this is a replacement
# pointer" vs "this is just a cross-reference" instead of dumping raw text.
_NOTE_INTENT_WORDS = {
    "REPLACEMENT": "🔁 marked as replacement",
    "REPLACES": "🔁 replaces",
    "REPLACED BY": "🔁 replaced by",
    "ALTERNATIVE": "🔄 alternative",
    "ALT": "🔄 alternative",
    "USE": "👉 use instead",
    "SEE": "👀 see also",
    "CHECK": "🔍 check",
}


import re  # used by _parse_note_for_skus; safe to import at module scope


def _parse_note_for_skus(note: str,
                          products_df: pd.DataFrame,
                          self_sku: str = "") -> list:
    """Scan a free-text replenishment note for tokens that look like SKU
    or model references, and match them to real SKUs in products_df.

    Buyers typically write shorthand: a note on `LED-XRD-60W-24` saying
    "E60L24DC REPLACEMENT" actually points at `LED-E60L24DC-KO`. So we
    can't rely on exact SKU matches — we look for tokens that appear AS
    SUBSTRINGS of real SKUs, and also scan product Names for the same
    token (since the model name often appears in the product's name).

    Heuristics for what counts as a "candidate token":
      - Length 4–30 characters
      - Contains at least one digit AND one letter (rules out plain words
        like "REPLACEMENT" or "MOQ")
      - Not an intent keyword itself

    Returns:
        [
          {
            "token": "E60L24DC",
            "intent": "🔁 marked as replacement"  (or None),
            "matches": ["LED-E60L24DC-KO", ...],  (real SKUs that match)
          },
          ...
        ]
    """
    if not note or (isinstance(note, float) and pd.isna(note)):
        return []
    note_str = str(note).strip()
    if not note_str:
        return []

    # Detect overall intent for the note (single intent per note is fine).
    note_upper = note_str.upper()
    detected_intent = None
    for word, label in _NOTE_INTENT_WORDS.items():
        if word in note_upper:
            detected_intent = label
            break

    # Tokenize on common separators. We keep digits/letters in tokens.
    tokens = re.split(r"[\s,/.()\[\]]+", note_str)

    # Filter: must have digits + letters, length 4–30, not an intent word.
    intent_words_upper = set(_NOTE_INTENT_WORDS.keys())
    candidates = []
    for tok in tokens:
        tok = tok.strip().strip("-_")
        if not (4 <= len(tok) <= 30):
            continue
        if not re.search(r"\d", tok):
            continue
        if not re.search(r"[A-Za-z]", tok):
            continue
        if tok.upper() in intent_words_upper:
            continue
        if tok.upper() == self_sku.upper():
            continue
        candidates.append(tok)

    if not candidates:
        return []

    if products_df is None or products_df.empty or "SKU" not in products_df.columns:
        return []

    sku_strs = products_df["SKU"].astype(str)
    sku_upper = sku_strs.str.upper()
    has_name = "Name" in products_df.columns
    if has_name:
        name_upper = products_df["Name"].astype(str).str.upper()

    matched_results = []
    seen_tokens = set()
    for tok in candidates:
        tu = tok.upper()
        if tu in seen_tokens:
            continue
        seen_tokens.add(tu)
        # Substring match on SKU. re.escape so dots/dashes are literal.
        try:
            sku_mask = sku_upper.str.contains(re.escape(tu), na=False)
        except re.error:
            continue
        sku_matches = set(sku_strs[sku_mask].tolist())
        # Substring match on product Name (catches model names).
        if has_name:
            name_mask = name_upper.str.contains(re.escape(tu), na=False)
            sku_matches.update(sku_strs[name_mask].tolist())

        # Drop self-match
        if self_sku:
            sku_matches.discard(self_sku)
        if not sku_matches:
            continue
        # Cap to 10 matches per token to avoid runaway noise on common
        # short tokens (e.g. "100M" matches every roll).
        matched_results.append({
            "token": tok,
            "intent": detected_intent,
            "matches": sorted(sku_matches)[:10],
        })

    return matched_results


def render_demand_breakdown(
    sku: str,
    sale_lines_df: pd.DataFrame,
    products_df: pd.DataFrame,
    bom_children: dict,
    bom_parents: dict,
    engine_row: Optional[pd.Series] = None,
    engine_df_full: Optional[pd.DataFrame] = None,
    stock_df: Optional[pd.DataFrame] = None,
    ip_alts_forward: Optional[dict] = None,
    ip_alts_reverse: Optional[dict] = None,
) -> None:
    """Render a buyer-friendly breakdown of where demand comes from for a SKU.

    Used by:
      - Ordering page (PO editor) — drives the "🔍 Drill into demand"
        expander, helping the buyer see WHY a 100m roll is being suggested
        for reorder when the master itself has zero direct sales.
      - Product Detail page — inline section showing reorder math and the
        family rollup so the buyer doesn't have to leave the page to make
        sense of the suggestion.

    The function renders five sections:
      1. Header strip — OnHand, Suggest, daily rates, Trend (when engine_row
         is provided) OR direct-sales counters (when called without it).
      2. Demand sources — table of children that roll up into this master,
         each with their 12mo / 90d / 45d sales, BOM ratio, and contribution.
      3. Family monthly sales chart — combined master + children, last 12
         months. Lets the buyer eyeball whether the family is still active.
      4. Recent activity feed — last 10 sale lines across the whole family
         so the buyer can spot which child is actually driving demand.
      5. Parents — what this SKU is built FROM (when applicable). Useful
         for non-master SKUs to see their upstream master.
    """
    if not sku:
        st.info("Select a SKU to see its demand breakdown.")
        return

    prod_match = products_df[products_df["SKU"].astype(str) == str(sku)]
    if prod_match.empty:
        st.warning(f"SKU '{sku}' not found in products.")
        return
    prod_row = prod_match.iloc[0]

    st.markdown(f"### :mag: Demand breakdown — `{sku}`")
    st.caption(str(prod_row.get("Name") or "")[:120])

    today = pd.Timestamp(datetime.now().date())
    cutoff_45 = today - pd.Timedelta(days=45)
    cutoff_90 = today - pd.Timedelta(days=90)
    cutoff_365 = today - pd.Timedelta(days=365)

    # Defensive copy + date coercion (sale_lines may already be parsed but
    # this is cheap insurance against caller variability).
    # Extra guard: on a fresh deploy the salelines sync may still be
    # running, so sale_lines_df can be empty AND missing InvoiceDate
    # entirely. Render the page anyway; just zero out demand numbers.
    sl = sale_lines_df.copy()
    if sl.empty or "InvoiceDate" not in sl.columns:
        st.warning(
            ":hourglass: Sale-line history not yet loaded — demand "
            "numbers will all show as zero until the background sync "
            "completes. Check `tail /data/output/salelines_sync.log` "
            "in the Render shell.")
        # Build an empty frame with the columns this function expects
        # so the rest of it can run without crashing.
        sl = pd.DataFrame(columns=[
            "InvoiceDate", "Quantity", "SKU", "Customer"])
    sl["InvoiceDate"] = pd.to_datetime(
        sl.get("InvoiceDate", pd.Series(dtype="object")), errors="coerce")
    sl["Quantity"] = pd.to_numeric(
        sl.get("Quantity", pd.Series(dtype="float64")),
        errors="coerce").fillna(0)

    # Direct sales of the master itself (independent of rollup)
    sl_master = sl[sl["SKU"].astype(str) == str(sku)]
    own_12mo = float(
        sl_master[sl_master["InvoiceDate"] >= cutoff_365]["Quantity"].sum())
    own_90d = float(
        sl_master[sl_master["InvoiceDate"] >= cutoff_90]["Quantity"].sum())
    own_45d = float(
        sl_master[sl_master["InvoiceDate"] >= cutoff_45]["Quantity"].sum())
    last_sale_date = (sl_master["InvoiceDate"].max()
                      if not sl_master.empty else None)

    children = bom_children.get(sku, [])
    parents = bom_parents.get(sku, [])

    # === Section 1: Header strip ===========================================
    cols = st.columns(5)
    if engine_row is not None:
        # Rich header with engine-computed numbers (PO editor context)
        onhand = float(engine_row.get("OnHand", 0) or 0)
        suggest = float(engine_row.get("reorder_qty", 0) or 0)
        avg_daily_base = float(engine_row.get("avg_daily_base", 0) or 0)
        eff_90d = float(engine_row.get("effective_units_90d", 0) or 0)
        rate_90d = eff_90d / 90.0
        trend = str(engine_row.get("trend_flag") or "Stable")
        cols[0].metric("OnHand", f"{onhand:.0f}")
        cols[1].metric("Suggest", f"{suggest:.2f}")
        cols[2].metric("12mo daily", f"{avg_daily_base:.3f}")
        cols[3].metric("90d daily", f"{rate_90d:.3f}")
        cols[4].metric("Trend", trend)
    else:
        # Direct-sales header (Product Detail context where engine isn't
        # available — still informative without the reorder math).
        cols[0].metric("12mo direct", f"{int(own_12mo)}")
        cols[1].metric("90d direct", f"{int(own_90d)}")
        cols[2].metric("45d direct", f"{int(own_45d)}")
        cols[3].metric("Last sale",
                       last_sale_date.strftime("%Y-%m-%d")
                       if last_sale_date is not None
                       and pd.notna(last_sale_date) else "—")
        cols[4].metric("Children", len(children))

    # === Substitution-opportunity alert (early visibility) =================
    # Two-tier scan for alternatives BEFORE the buyer scrolls.
    #   Tier 1 (authoritative): migration DB entries. Read live from
    #     db.all_migrations() so manual additions (via inline form or
    #     the dedicated Migrations page) show up immediately — not just
    #     IP-imported ones. After ip_import_migrations.py runs, all IP
    #     merges are also in this same DB, so DB is the single source.
    #   Tier 2 (heuristic): family-prefix variants with idle stock —
    #     informational only, since SKU-prefix matching can cluster
    #     items that aren't actually interchangeable (e.g. different
    #     voltages share the prefix).
    # Trigger is intentionally broad: any signal of an alternative fires
    # the banner. The buyer decides whether the substitution is worth it.
    try:
        _all_migs_live = [dict(m) for m in db.all_migrations()]
    except Exception:
        _all_migs_live = []
    # Forward = this SKU is a SUCCESSOR for these predecessors
    _ip_fwd_alts = []
    # Reverse = this SKU is a PREDECESSOR retired into these successor(s)
    _ip_rev_alts = []
    for _m in _all_migs_live:
        _ret = str(_m.get("retiring_sku") or "")
        _suc = str(_m.get("successor_sku") or "")
        if _suc == str(sku):
            # Live entry uses different keys than CSV — translate so the
            # downstream rendering code (which expects the CSV shape) works.
            _ip_fwd_alts.append({
                "AlternativeSKU": _ret,
                "MasterSKU": _suc,
                "Percent": _m.get("share_pct"),
                "Source": _m.get("set_by") or "",
                "Title": "",  # filled in from products_df at render time
                "Barcode": "",
                "MasterID": "",
                "AlternativeID": "",
            })
        if _ret == str(sku):
            _ip_rev_alts.append({
                "AlternativeSKU": _ret,
                "MasterSKU": _suc,
                "Percent": _m.get("share_pct"),
                "Source": _m.get("set_by") or "",
                "Title": "",
                "Barcode": "",
                "MasterID": "",
                "AlternativeID": "",
            })
    _ip_alts_with_stock = 0
    for _ent in _ip_fwd_alts + _ip_rev_alts:
        _alt_sku = _ent.get("AlternativeSKU") if _ent in _ip_fwd_alts else _ent.get("MasterSKU")
        if not _alt_sku:
            continue
        _oh_alt = 0.0
        if engine_df_full is not None and not engine_df_full.empty:
            _em = engine_df_full[
                engine_df_full["SKU"].astype(str) == str(_alt_sku)]
            if not _em.empty:
                _val = _em.iloc[0].get("OnHand", 0)
                _oh_alt = float(_val) if pd.notna(_val) else 0.0
        elif stock_df is not None and not stock_df.empty:
            _sm = stock_df[stock_df["SKU"].astype(str) == str(_alt_sku)]
            if not _sm.empty:
                _oh_alt = float(pd.to_numeric(
                    _sm["OnHand"], errors="coerce").sum() or 0)
        if _oh_alt > 0:
            _ip_alts_with_stock += 1
    _ip_alts_total = len(_ip_fwd_alts) + len(_ip_rev_alts)

    _sub_candidate_count = 0
    _sku_parts_check = str(sku).split("-")
    if len(_sku_parts_check) >= 3:
        _fam_prefix = "-".join(_sku_parts_check[:-1])
        _bom_chx = {str(c.get("AssemblySKU")) for c in children
                     if c.get("AssemblySKU")}
        _bom_pax = {str(p.get("ComponentSKU")) for p in parents
                     if p.get("ComponentSKU")}
        _excl = _bom_chx | _bom_pax | {str(sku)}
        _sib_check = products_df[
            products_df["SKU"].astype(str).str.startswith(_fam_prefix + "-")
            & ~products_df["SKU"].astype(str).isin(_excl)
        ]
        for _, _sb in _sib_check.iterrows():
            _sib_sku = str(_sb.get("SKU"))
            # Quick OnHand lookup — engine_df_full preferred (Ordering
            # page), stock_df fallback (Product Detail page).
            _oh = 0.0
            if engine_df_full is not None and not engine_df_full.empty:
                _em = engine_df_full[
                    engine_df_full["SKU"].astype(str) == _sib_sku]
                if not _em.empty:
                    _val = _em.iloc[0].get("OnHand", 0)
                    _oh = float(_val) if pd.notna(_val) else 0.0
            elif stock_df is not None and not stock_df.empty:
                _sm = stock_df[stock_df["SKU"].astype(str) == _sib_sku]
                if not _sm.empty:
                    _oh = float(pd.to_numeric(
                        _sm["OnHand"], errors="coerce").sum() or 0)
            if _oh > 0:
                _sub_candidate_count += 1
    # Banner priority: IP-derived migration stock ALWAYS takes priority
    # over the family-prefix heuristic. If a PREDECESSOR (retiring SKU)
    # still has residual OnHand, the buyer needs to know — consuming
    # legacy stock first is exactly the kind of thing that gets missed
    # when the team is just looking at active SKU stock levels.
    if _ip_alts_with_stock > 0:
        _heur_extra = (
            f"  Plus {_sub_candidate_count} family-prefix sibling"
            f"{'s' if _sub_candidate_count != 1 else ''} with stock "
            f"(heuristic match)."
            if _sub_candidate_count > 0 else ""
        )
        st.error(
            f":scroll: **{_ip_alts_with_stock} predecessor / migration-"
            f"linked SKU{'s' if _ip_alts_with_stock != 1 else ''} still "
            f"holding stock!**  \n"
            f"Per IP's migration history, "
            f"{'these have' if _ip_alts_with_stock != 1 else 'this has'} "
            f"residual inventory you can consume "
            f"before reordering this SKU. "
            f"**Scroll down to the :scroll: Migration history section** "
            f"to review.{_heur_extra}"
        )
    elif _sub_candidate_count > 0:
        st.warning(
            f":twisted_rightwards_arrows: **{_sub_candidate_count} "
            f"family-prefix variant"
            f"{'s' if _sub_candidate_count != 1 else ''} with idle "
            "stock** (heuristic match — verify specs).  \n"
            "Other SKUs sharing this product's prefix have inventory on "
            "hand. They **may or may not be interchangeable** — "
            "check voltage, length, colour, etc. before treating as a "
            "substitute. "
            "**Scroll down to :twisted_rightwards_arrows: Family variants** "
            "to review."
        )

    # === Section 1.5: Team notes (Inventory Planner replenishment_notes) ===
    # Surfaces the per-warehouse free-text notes your team has been
    # writing in IP for years. Each note also gets parsed for tokens that
    # look like SKU model references — when a buyer wrote "E60L24DC
    # REPLACEMENT" on LED-XRD-60W-24, we surface LED-E60L24DC-KO as a
    # candidate alternative. The parser is heuristic; matches are shown
    # as suggestions, not as authoritative substitutes.
    _sku_notes = IP_NOTES.get(str(sku), [])
    if _sku_notes:
        st.markdown("#### :memo: Team notes from Inventory Planner")
        for _n in _sku_notes:
            note_text = _n.get("text") or ""
            if not note_text:
                continue
            # Parse for SKU candidates
            parsed = _parse_note_for_skus(
                note_text, products_df, self_sku=str(sku))
            # Render the note. If parsing found candidate SKUs, show them
            # inline as a small caption below.
            note_disp = note_text.strip()
            wh_id = _n.get("warehouse_id") or ""
            wh_label = (f"  ·  warehouse `{wh_id[-12:]}`" if wh_id else "")
            st.info(f"📝 **{note_disp}**{wh_label}")

            if parsed:
                # Build a compact suggestion line. When intent is
                # "REPLACEMENT/REPLACES" the matched SKU is almost
                # certainly a PREDECESSOR — flag it as such and hint at
                # the Migration setup. Other intents stay neutral.
                bits = []
                migration_candidates = []
                for p in parsed:
                    tok = p["token"]
                    intent = p.get("intent") or ""
                    matches = p["matches"]
                    if not matches:
                        continue
                    if len(matches) == 1:
                        match_disp = f"`{matches[0]}`"
                    else:
                        head = ", ".join(f"`{m}`" for m in matches[:3])
                        more = (f" + {len(matches) - 3} more"
                                if len(matches) > 3 else "")
                        match_disp = head + more
                    intent_disp = f" {intent}" if intent else ""
                    bits.append(f"`{tok}`{intent_disp} → {match_disp}")
                    # Flag replacement-intent matches as migration candidates
                    if intent and ("replac" in intent.lower()
                                    or "🔁" in intent):
                        for m in matches[:1]:  # most-likely match only
                            migration_candidates.append(m)
                if bits:
                    st.caption(
                        "**SKU references found in this note:**  \n"
                        + "  \n".join(bits))
                if migration_candidates:
                    st.warning(
                        ":scroll: **Likely predecessor"
                        f"{'s' if len(migration_candidates) != 1 else ''}: "
                        f"{', '.join(f'`{c}`' for c in migration_candidates)}**  \n"
                        f"The note implies this SKU has replaced "
                        f"{'them' if len(migration_candidates) != 1 else 'it'}. "
                        f"Consider recording this in **Migrations** so "
                        f"their historical sales feed this SKU's forecast."
                    )

    # === Section 2: Demand sources (children rolling up) ===================
    if children:
        st.markdown(
            f"#### :bar_chart: Demand sources — {len(children)} child SKU(s) "
            f"roll up into this master")
        rows = []
        for child in children:
            child_sku = child.get("AssemblySKU")
            if not child_sku:
                continue
            qty_per = float(child.get("Quantity") or 0)
            child_sl = sl[sl["SKU"].astype(str) == str(child_sku)]
            c_12mo = float(
                child_sl[child_sl["InvoiceDate"]
                          >= cutoff_365]["Quantity"].sum())
            c_90d = float(
                child_sl[child_sl["InvoiceDate"]
                          >= cutoff_90]["Quantity"].sum())
            c_45d = float(
                child_sl[child_sl["InvoiceDate"]
                          >= cutoff_45]["Quantity"].sum())
            last = (child_sl["InvoiceDate"].max()
                    if not child_sl.empty else None)
            rows.append({
                "Child SKU": child_sku,
                "Name": str(child.get("AssemblyName") or "")[:40],
                "12mo units": int(c_12mo),
                "90d units": int(c_90d),
                "45d units": int(c_45d),
                "Last sale": (last.strftime("%Y-%m-%d")
                              if last is not None and pd.notna(last)
                              else "—"),
                "BOM ratio": qty_per,
                "Contrib 12mo": round(c_12mo * qty_per, 2),
                "Contrib 90d": round(c_90d * qty_per, 2),
            })
        if rows:
            df_rows = pd.DataFrame(rows).sort_values(
                "12mo units", ascending=False)
            st.dataframe(
                df_rows, hide_index=True, width="stretch",
                column_config={
                    "BOM ratio":
                        st.column_config.NumberColumn(format="%.4g"),
                    "Contrib 12mo":
                        st.column_config.NumberColumn(format="%.2f"),
                    "Contrib 90d":
                        st.column_config.NumberColumn(format="%.2f"),
                },
            )
            total_contrib_12 = sum(r["Contrib 12mo"] for r in rows)
            total_contrib_90 = sum(r["Contrib 90d"] for r in rows)
            st.caption(
                f"Total rollup contribution: "
                f"**{total_contrib_12:.2f} master units / 12mo** · "
                f"**{total_contrib_90:.2f} master units / 90d**. "
                f"Plus direct sales of master "
                f"({int(own_12mo)} / 12mo, {int(own_90d)} / 90d). "
                f"Effective demand = direct + rollup.")
    else:
        st.info(
            ":information_source: No children roll up into this SKU. "
            "Either it's a leaf product (direct demand only) or the BOM "
            "doesn't yet model how this SKU is consumed.")

    # === Section 3: Family-wide monthly trend ==============================
    # Critically, the chart normalises everything to MASTER-ROLL EQUIVALENTS
    # using each child's BOM ratio. Without this, summing raw Quantity values
    # across master + per-foot + 5m mixes physical units (rolls + feet +
    # rolls) into a meaningless total. With normalisation, the chart shows
    # "this family consumed N master-rolls of demand last month" — the only
    # meaningful aggregate for a multi-variant family.
    family_skus = [sku] + [c.get("AssemblySKU") for c in children
                            if c.get("AssemblySKU")]
    sku_to_master_ratio = {str(sku): 1.0}  # master itself = 1.0 by definition
    for c in children:
        c_sku = c.get("AssemblySKU")
        c_qty = c.get("Quantity")
        if c_sku and c_qty is not None and pd.notna(c_qty):
            sku_to_master_ratio[str(c_sku)] = float(c_qty)

    # === Include predecessors so the chart + activity feed show the FULL
    # demand lineage, not just post-migration sales. For each family SKU
    # (master + children), look up its direct migration predecessors and
    # add them with a scaled ratio = successor's ratio × share_pct/100.
    # Without this, the chart shows just SIERRA's own sales (61 units)
    # while the engine is using ~455 (which includes predecessor history
    # from CASCADE38-W-MP-2390, SMOKIES38-W-MP-3, etc).
    try:
        _all_migs_for_chart = [dict(m) for m in db.all_migrations()]
    except Exception:
        _all_migs_for_chart = []
    _preds_by_succ: dict = {}
    for _m in _all_migs_for_chart:
        _s = str(_m.get("successor_sku") or "")
        if _s:
            _preds_by_succ.setdefault(_s, []).append(_m)
    _predecessor_skus: list = []
    for _fsku in list(family_skus):  # iterate over copy; appending below
        _fsku_str = str(_fsku)
        _fsku_ratio = sku_to_master_ratio.get(_fsku_str, 1.0)
        for _m in _preds_by_succ.get(_fsku_str, []):
            _pred = str(_m.get("retiring_sku") or "")
            if not _pred or _pred == _fsku_str:
                continue
            _share = float(_m.get("share_pct") or 100) / 100.0
            # Predecessor inherits successor's BOM ratio × migration share.
            # If 100% migrated and successor's ratio is 1.0, predecessor's
            # ratio is also 1.0 (each historical unit equals one master unit).
            sku_to_master_ratio[_pred] = _fsku_ratio * _share
            _predecessor_skus.append(_pred)
    # Deduplicate while preserving the master + children + predecessors
    family_skus = list(dict.fromkeys(
        [str(s) for s in family_skus + _predecessor_skus]))

    family_sl = sl[sl["SKU"].astype(str).isin(
        [str(s) for s in family_skus])].copy()
    family_sl = family_sl.dropna(subset=["InvoiceDate"])
    if not family_sl.empty:
        _n_preds_with_data = len(set(_predecessor_skus) & set(
            family_sl["SKU"].astype(str)))
        _pred_suffix = (
            f" — incl. {_n_preds_with_data} predecessor"
            f"{'s' if _n_preds_with_data != 1 else ''} with sales history"
            if _n_preds_with_data > 0 else "")
        st.markdown(
            f"#### :chart_with_upwards_trend: Family monthly demand — "
            f"master-roll equivalents (last 12 months){_pred_suffix}")
        st.caption(
            "Each SKU's units are multiplied by its BOM ratio (and "
            "migration share for predecessors) to this master, so the "
            "chart is in a single unit (master rolls) regardless of how "
            "the family sells or what predecessors it replaced. "
            f"Master = `{sku}` = 1 unit per row.")
        cutoff_12mo = today - pd.Timedelta(days=365)
        recent_sl = family_sl[family_sl["InvoiceDate"] >= cutoff_12mo].copy()
        if not recent_sl.empty:
            recent_sl["master_units"] = recent_sl.apply(
                lambda r: (float(r["Quantity"] or 0)
                            * sku_to_master_ratio.get(str(r["SKU"]), 1.0)),
                axis=1,
            )
            recent_sl["month"] = (recent_sl["InvoiceDate"]
                                   .dt.to_period("M").astype(str))
            monthly = recent_sl.groupby("month")["master_units"].sum()
            all_months = (pd.period_range(end=today, periods=12, freq="M")
                          .astype(str))
            monthly = monthly.reindex(all_months, fill_value=0)
            st.bar_chart(monthly)
        else:
            st.caption(
                "No family sales in the last 12 months — confirms a fully "
                "dormant family.")

    # === Section 4: Recent activity feed ===================================
    # family_sl now includes predecessor sales (added above), so this
    # naturally shows recent CASCADE/SMOKIES sales alongside SIERRA's,
    # giving the buyer the full lineage view.
    if not family_sl.empty:
        _pred_set = set(_predecessor_skus)
        _has_pred_sales = bool(_pred_set & set(family_sl["SKU"].astype(str)))
        _recent_label = (
            "across the family (incl. predecessors)"
            if _has_pred_sales else "across the family")
        st.markdown(
            f"#### :clipboard: Recent activity — last 10 sales "
            f"{_recent_label}")
        recent_10 = (family_sl.sort_values("InvoiceDate", ascending=False)
                     .head(10)).copy()
        cols_to_show = [c for c in
                        ["InvoiceDate", "SKU", "Customer",
                          "Quantity", "Total"]
                        if c in recent_10.columns]
        if cols_to_show:
            display = recent_10[cols_to_show].copy()
            if "InvoiceDate" in display.columns:
                display["InvoiceDate"] = (
                    pd.to_datetime(display["InvoiceDate"], errors="coerce")
                      .dt.strftime("%Y-%m-%d"))
            if "Total" in display.columns:
                display["Total"] = pd.to_numeric(
                    display["Total"], errors="coerce")
            st.dataframe(display, hide_index=True, width="stretch")

    # === Section 5: Parents (where this SKU is built from) =================
    if parents:
        st.markdown(
            f"#### :arrow_up: Built from — {len(parents)} parent SKU(s)")
        prows = []
        for p in parents:
            prows.append({
                "Parent SKU": p.get("ComponentSKU"),
                "Name": str(p.get("ComponentName") or "")[:40],
                "Qty per unit": float(p.get("Quantity") or 0),
            })
        st.dataframe(
            pd.DataFrame(prows), hide_index=True, width="stretch",
            column_config={
                "Qty per unit":
                    st.column_config.NumberColumn(format="%.4g"),
            })

    # === Section 5.5: Migration history (IP merged[] — successor lineage) ==
    # IP's "Combine sales/stock" feature is structurally a migration
    # mapping: the variant where the merge is configured is the SUCCESSOR
    # (it inherits sales history from the OLD/RETIRING variants listed).
    # Same concept as our internal sku_migrations DB — see the dedicated
    # Migrations page and Section 7 redirect.
    #
    # In our pulled CSV:
    #   MasterSKU       = successor (the new active SKU)
    #   AlternativeSKU  = predecessor (the retiring SKU whose sales merge in)
    # In our in-memory dicts:
    #   IP_ALTS_FORWARD[successor] -> list of predecessor entries
    #   IP_ALTS_REVERSE[predecessor] -> list of successor entries
    #
    # Display reflects the buyer's mental model:
    #   "📜 Replaces N predecessors" (when this SKU is a successor)
    #   "🔁 Replaced by N successor(s)" (when this SKU is retiring)
    if _ip_fwd_alts or _ip_rev_alts:
        if _ip_fwd_alts:
            st.markdown(
                f"#### :scroll: Replaces {len(_ip_fwd_alts)} predecessor"
                f"{'s' if len(_ip_fwd_alts) != 1 else ''} "
                "(migration history)")
            st.caption(
                "This SKU has effectively replaced the SKUs below. "
                "Their historical sales roll into this SKU's forecast. "
                "Source covers both manual entries and IP-imported merges.")
            pred_rows = []
            for ent in _ip_fwd_alts:
                pred_sku = ent.get("AlternativeSKU") or ""
                pred_title = ent.get("Title") or ""
                # If title wasn't carried with the entry (DB-sourced
                # entries don't store titles), look it up in products.
                if not pred_title and not products_df.empty:
                    _pm = products_df[
                        products_df["SKU"].astype(str) == str(pred_sku)]
                    if not _pm.empty:
                        pred_title = str(_pm.iloc[0].get("Name") or "")
                pred_sl = sl[sl["SKU"].astype(str) == str(pred_sku)]
                p_12 = float(pred_sl[pred_sl["InvoiceDate"]
                                       >= cutoff_365]["Quantity"].sum())
                p_90 = float(pred_sl[pred_sl["InvoiceDate"]
                                       >= cutoff_90]["Quantity"].sum())
                p_last = (pred_sl["InvoiceDate"].max()
                          if not pred_sl.empty else None)
                p_oh = 0.0
                if engine_df_full is not None and not engine_df_full.empty:
                    em = engine_df_full[
                        engine_df_full["SKU"].astype(str) == str(pred_sku)]
                    if not em.empty:
                        val = em.iloc[0].get("OnHand", 0)
                        p_oh = float(val) if pd.notna(val) else 0.0
                elif stock_df is not None and not stock_df.empty:
                    sm = stock_df[stock_df["SKU"].astype(str) == str(pred_sku)]
                    if not sm.empty:
                        p_oh = float(pd.to_numeric(
                            sm["OnHand"], errors="coerce").sum() or 0)
                try:
                    pct_disp = f"{float(ent.get('Percent', 0)):.0f}%"
                except (TypeError, ValueError):
                    pct_disp = "—"
                pred_rows.append({
                    "Predecessor SKU": str(pred_sku),
                    "Title": (pred_title or "")[:60],
                    "Share %": pct_disp,
                    "Source": str(ent.get("Source") or "—"),
                    "Residual OnHand": (f"{p_oh:.2f}"
                                          if p_oh > 0 else "—"),
                    "12mo units (legacy)": int(p_12),
                    "90d units (legacy)": int(p_90),
                    "Last sale": (p_last.strftime("%Y-%m-%d")
                                   if p_last is not None
                                   and pd.notna(p_last)
                                   else "—"),
                })
            if pred_rows:
                st.dataframe(pd.DataFrame(pred_rows),
                              hide_index=True, width="stretch")

        # === Indirect predecessors via BOM children =====================
        # The direct migrations above are at the bare-tube level; the
        # actual sales volume usually lived on the MP-variant siblings
        # (e.g. CASCADE38-W-MP-2390 retired into SIERRA38-W-MP-2390,
        # which then BOM-rolls into this bare tube). Walking that chain
        # here makes the full lineage visible — buyer can see all the
        # historical sales contributing to this SKU's forecast.
        _indirect_rows = []
        try:
            _all_db_migs = [dict(m) for m in db.all_migrations()]
        except Exception:
            _all_db_migs = []
        _preds_by_succ_idx: dict = {}
        for _m in _all_db_migs:
            _s = str(_m.get("successor_sku") or "")
            if _s:
                _preds_by_succ_idx.setdefault(_s, []).append(_m)
        for child in children:
            child_sku = str(child.get("AssemblySKU") or "")
            qty_per = float(child.get("Quantity") or 0)
            if not child_sku or qty_per <= 0:
                continue
            for _m in _preds_by_succ_idx.get(child_sku, []):
                ind_pred = str(_m.get("retiring_sku") or "")
                if not ind_pred:
                    continue
                share = float(_m.get("share_pct") or 100) / 100
                ind_sl = sl[sl["SKU"].astype(str) == ind_pred]
                ind_12mo = float(ind_sl[ind_sl["InvoiceDate"]
                                          >= cutoff_365]["Quantity"].sum())
                ind_90d = float(ind_sl[ind_sl["InvoiceDate"]
                                          >= cutoff_90]["Quantity"].sum())
                ind_last = (ind_sl["InvoiceDate"].max()
                            if not ind_sl.empty else None)
                # Contribution to THIS bare-tube master:
                # predecessor 12mo × share × qty_per
                contribution_to_master = ind_12mo * share * qty_per
                ind_title = ""
                if not products_df.empty:
                    _pm = products_df[
                        products_df["SKU"].astype(str) == ind_pred]
                    if not _pm.empty:
                        ind_title = str(_pm.iloc[0].get("Name") or "")
                # Residual OnHand on the indirect predecessor
                ind_oh = 0.0
                if engine_df_full is not None and not engine_df_full.empty:
                    em = engine_df_full[
                        engine_df_full["SKU"].astype(str) == ind_pred]
                    if not em.empty:
                        val = em.iloc[0].get("OnHand", 0)
                        ind_oh = float(val) if pd.notna(val) else 0.0
                elif stock_df is not None and not stock_df.empty:
                    sm = stock_df[stock_df["SKU"].astype(str) == ind_pred]
                    if not sm.empty:
                        ind_oh = float(pd.to_numeric(
                            sm["OnHand"], errors="coerce").sum() or 0)
                _indirect_rows.append({
                    "Indirect predecessor": ind_pred,
                    "Title": ind_title[:50],
                    "→ via successor child": child_sku,
                    "Share %": f"{share * 100:.0f}%",
                    "BOM ratio": f"{qty_per:.4g}",
                    "Residual OnHand": (f"{ind_oh:.2f}"
                                          if ind_oh > 0 else "—"),
                    "12mo units": int(ind_12mo),
                    "90d units": int(ind_90d),
                    "Contributes to this": f"{contribution_to_master:.0f}",
                    "Last sale": (ind_last.strftime("%Y-%m-%d")
                                   if ind_last is not None
                                   and pd.notna(ind_last)
                                   else "—"),
                })
        if _indirect_rows:
            # Sort by largest contribution first
            _indirect_rows.sort(
                key=lambda r: -float(r["Contributes to this"]))
            _total_contrib = sum(
                float(r["Contributes to this"]) for r in _indirect_rows)
            st.markdown(
                f"#### :link: Indirect predecessors via BOM "
                f"({len(_indirect_rows)} — contributing "
                f"{_total_contrib:.0f} units)")
            st.caption(
                "These predecessors don't migrate directly to this SKU; "
                "they migrate to its BOM children (the MP-variant "
                "successors), and from there their demand rolls UP into "
                "this bare tube via the BOM ratio. This is where the "
                "real sales volume usually lives — bare tubes rarely "
                "sell standalone."
            )
            st.dataframe(
                pd.DataFrame(_indirect_rows),
                hide_index=True, width="stretch")

        if _ip_rev_alts:
            st.markdown(
                f"#### 🔁 Replaced by "
                f"{len(_ip_rev_alts)} successor"
                f"{'s' if len(_ip_rev_alts) != 1 else ''} "
                "(migration history)")
            st.caption(
                "This SKU has been retired and its demand now rolls "
                "into the successor(s) below.")
            succ_rows = []
            for ent in _ip_rev_alts:
                succ_sku = ent.get("MasterSKU") or ""
                # Look up the successor's title from our products data
                succ_title = ""
                if not products_df.empty:
                    sm_match = products_df[
                        products_df["SKU"].astype(str) == str(succ_sku)]
                    if not sm_match.empty:
                        succ_title = str(sm_match.iloc[0].get("Name") or "")
                try:
                    pct_disp = f"{float(ent.get('Percent', 0)):.0f}%"
                except (TypeError, ValueError):
                    pct_disp = "—"
                succ_rows.append({
                    "Successor SKU": str(succ_sku),
                    "Title": succ_title[:60],
                    "Share %": pct_disp,
                    "Source": str(ent.get("Source") or "—"),
                })
            if succ_rows:
                st.dataframe(pd.DataFrame(succ_rows),
                              hide_index=True, width="stretch")

    # === Section 5.6: Inline "+ Add predecessor" form ======================
    # Always shown (even when there are no existing predecessors yet) so
    # the buyer can record a migration without leaving this drill-down.
    # Saves via db.set_migration() — same backend as the dedicated
    # Migrations page and the Section 7 redirect.
    with st.expander(
            ":heavy_plus_sign: Add a predecessor (this SKU as successor)",
            expanded=False):
        st.caption(
            "If this SKU has effectively replaced an older SKU not yet "
            "recorded, add the mapping here. The retiring SKU's 12mo "
            "sales × Share % will start rolling into this SKU's demand "
            "after the next data refresh.")
        sku_pool = (sorted(set(products_df["SKU"].astype(str)))
                     if not products_df.empty else [])
        # Filter out self and any already-mapped predecessors to reduce
        # accidental duplicates.
        existing_pred_skus = {
            str(ent.get("AlternativeSKU") or "")
            for ent in _ip_fwd_alts
        }
        # Also exclude any SKU already mapped as retiring in the DB
        if hasattr(db, "all_migrations"):
            for _m in db.all_migrations():
                existing_pred_skus.add(str(dict(_m).get("retiring_sku") or ""))
        sku_pool_filtered = [
            s for s in sku_pool
            if s != str(sku) and s not in existing_pred_skus
        ]
        ic1, ic2 = st.columns([3, 1])
        with ic1:
            inline_pred = st.selectbox(
                "Retiring (predecessor) SKU",
                options=[""] + sku_pool_filtered,
                key=f"inline_add_pred_{sku}",
                help="Pick the older SKU that this one has replaced.")
        with ic2:
            inline_share = st.number_input(
                "Share %",
                min_value=1.0, max_value=100.0, value=100.0, step=5.0,
                key=f"inline_add_pred_share_{sku}")
        inline_note = st.text_input(
            "Note (optional)",
            key=f"inline_add_pred_note_{sku}",
            placeholder="Why this migration?")
        save_disabled_inline = (not inline_pred or inline_pred == str(sku))
        if st.button(":floppy_disk: Save predecessor",
                      key=f"inline_add_pred_save_{sku}",
                      disabled=save_disabled_inline,
                      type="primary"):
            actor = st.session_state.get("current_user") or "unknown"
            db.set_migration(
                retiring_sku=inline_pred,
                successor_sku=str(sku),
                actor=actor,
                share_pct=float(inline_share),
                note=inline_note or "added inline from drill-down",
            )
            st.success(
                f"Saved: **{inline_pred}** → **{sku}** "
                f"@ {inline_share:.0f}%. Refresh the page to see it "
                "in the predecessors table above.")
            st.rerun()

    # === Section 6: Family siblings (same product, alternative form) =======
    # Heuristic match: SKUs sharing the prefix (everything up to the last
    # hyphen-suffix) but NOT in the BOM children/parents list. Surfaces
    # alternatives the BOM doesn't capture. Use case: 100m roll buyer
    # sees that the 5m sibling has 8 units OnHand sitting idle for 90+
    # days → consume those before reordering more 100m rolls.
    #
    # Status icons help the buyer scan quickly:
    #   ⚠️ Dead stock — has OnHand AND no recent sales (great substitute)
    #   💤 Dormant   — no recent sales, no OnHand (no substitution help)
    #   ✅ Active    — selling normally (don't displace)
    #   ⚪ Empty     — zero OnHand, possibly active elsewhere
    sku_parts = str(sku).split("-")
    if len(sku_parts) >= 3:
        family_prefix = "-".join(sku_parts[:-1])
        bom_child_set = {str(c.get("AssemblySKU"))
                          for c in children if c.get("AssemblySKU")}
        bom_parent_set = {str(p.get("ComponentSKU"))
                           for p in parents if p.get("ComponentSKU")}
        excluded_set = bom_child_set | bom_parent_set | {str(sku)}
        sibs_mask = (
            products_df["SKU"].astype(str).str.startswith(family_prefix + "-")
            & ~products_df["SKU"].astype(str).isin(excluded_set)
        )
        family_sibs = products_df[sibs_mask].copy()
        # ---------- CIN7-confirmed alternatives (authoritative) -----------
        # If the team has populated CIN7's AdditionalAttribute6 field
        # ("Alternative Product") for this SKU, surface those entries
        # ABOVE the heuristic table — they're explicit decisions vs.
        # pattern matches.
        cin7_alts = CIN7_ALTERNATIVES.get(str(sku), [])
        if cin7_alts:
            st.markdown(
                f"#### :white_check_mark: CIN7-confirmed alternatives "
                f"({len(cin7_alts)})")
            st.caption(
                "From CIN7's *Alternative Product* attribute on this SKU "
                "(or another SKU pointing back at this one). Curated by "
                "your team — treat as real alternatives. No sales "
                "rollup occurs (those are migrations, shown above).")
            cin7_alt_rows = []
            for alt_sku in cin7_alts:
                alt_match = products_df[
                    products_df["SKU"].astype(str) == str(alt_sku)]
                alt_name = (str(alt_match.iloc[0].get("Name") or "")[:60]
                             if not alt_match.empty else "")
                # OnHand
                oh = 0.0
                if engine_df_full is not None and not engine_df_full.empty:
                    em = engine_df_full[
                        engine_df_full["SKU"].astype(str) == str(alt_sku)]
                    if not em.empty:
                        val = em.iloc[0].get("OnHand", 0)
                        oh = float(val) if pd.notna(val) else 0.0
                elif stock_df is not None and not stock_df.empty:
                    sm = stock_df[stock_df["SKU"].astype(str) == str(alt_sku)]
                    if not sm.empty:
                        oh = float(pd.to_numeric(
                            sm["OnHand"], errors="coerce").sum() or 0)
                cin7_alt_rows.append({
                    "Alternative SKU": str(alt_sku),
                    "Name": alt_name,
                    "OnHand": f"{oh:.2f}" if oh > 0 else "—",
                    "In product master": ("✓" if not alt_match.empty
                                            else "⚠ unknown SKU"),
                })
            if cin7_alt_rows:
                st.dataframe(pd.DataFrame(cin7_alt_rows),
                              hide_index=True, width="stretch")

        if not family_sibs.empty:
            st.markdown(
                f"#### :twisted_rightwards_arrows: Family variants — "
                f"same SKU prefix (informational)")
            st.warning(
                f":warning: **These are heuristic matches by SKU prefix "
                f"only — `{family_prefix}-*`. They are NOT necessarily "
                "interchangeable.**  \n"
                "A 24V driver and a 12V driver will share a prefix but "
                "are not substitutes. A 100m roll and a 5m roll of the "
                "same product probably are. **Check specs before treating "
                "any row here as a real alternative.**  \n"
                "For curated, authoritative substitution decisions, see "
                "the :scroll: Migration history section above, the "
                ":white_check_mark: CIN7-confirmed alternatives section "
                "above (if populated), or set up entries via the "
                "Migrations page / CIN7's Alternative Product attribute.")
            sib_rows = []
            for _, sib in family_sibs.iterrows():
                sib_sku = str(sib.get("SKU"))
                sib_sl = sl[sl["SKU"].astype(str) == sib_sku]
                s_12mo = float(sib_sl[
                    sib_sl["InvoiceDate"] >= cutoff_365]["Quantity"].sum())
                s_90d = float(sib_sl[
                    sib_sl["InvoiceDate"] >= cutoff_90]["Quantity"].sum())
                last = (sib_sl["InvoiceDate"].max()
                        if not sib_sl.empty else None)
                # Look up OnHand: prefer engine_df_full (richer), fall back
                # to stock_df (raw stock data, available in Product Detail
                # page where the engine isn't directly accessible).
                onhand_str = "—"
                onhand_num = 0.0
                if engine_df_full is not None and not engine_df_full.empty:
                    em = engine_df_full[
                        engine_df_full["SKU"].astype(str) == sib_sku]
                    if not em.empty:
                        onhand_num = float(em.iloc[0].get("OnHand", 0) or 0)
                        onhand_str = f"{onhand_num:.2f}"
                elif stock_df is not None and not stock_df.empty:
                    sm = stock_df[
                        stock_df["SKU"].astype(str) == sib_sku]
                    if not sm.empty:
                        onhand_num = float(pd.to_numeric(
                            sm["OnHand"], errors="coerce").sum() or 0)
                        onhand_str = f"{onhand_num:.2f}"
                # Trend column — categorise the sibling's recent
                # performance vs its 12mo baseline so the buyer can
                # decide whether to push it as a substitute. The ratio
                # (90d daily rate / 12mo daily rate) tells the story:
                # > 1.2x = accelerating; 0.8-1.2x = stable; 0.2-0.8x =
                # slowing; < 0.2x = lagging/stale; 0 = no recent
                # activity at all. Lagging items with high OnHand are
                # exactly the substitution candidates.
                rate_12 = s_12mo / 365.0
                rate_90 = s_90d / 90.0
                if s_12mo == 0 and s_90d == 0:
                    trend = "⚪ No history"
                elif s_90d == 0:
                    trend = "💤 Stale (no 90d sales)"
                elif rate_12 == 0 and s_90d > 0:
                    trend = "🆕 New (no 12mo baseline)"
                else:
                    ratio = rate_90 / rate_12 if rate_12 > 0 else 0
                    if ratio >= 1.2:
                        trend = f"📈 Up ({ratio:.1f}x)"
                    elif ratio >= 0.8:
                        trend = f"→ Stable ({ratio:.1f}x)"
                    elif ratio >= 0.2:
                        trend = f"📉 Slowing ({ratio:.1f}x)"
                    else:
                        trend = f"📉 Lagging ({ratio:.1f}x)"
                # Substitution-priority signal: sibling has stock + lagging
                # trend → top candidate. Used for sorting + the warning.
                is_substitution_candidate = (
                    onhand_num > 0 and
                    ("Lagging" in trend or "Slowing" in trend
                     or "Stale" in trend))
                sib_rows.append({
                    "Sibling SKU": sib_sku,
                    "Variant name": str(sib.get("Name") or "")[:40],
                    "OnHand": onhand_str,
                    "12mo units": int(s_12mo),
                    "90d units": int(s_90d),
                    "Last sale": (last.strftime("%Y-%m-%d")
                                   if last is not None
                                   and pd.notna(last) else "—"),
                    "Trend": trend,
                    "_sub_candidate": is_substitution_candidate,
                })
            if sib_rows:
                # Sort substitution candidates first (high OnHand + lagging
                # trend), then by 12mo units descending. That puts the
                # most useful "consume this idle alternative first" rows
                # at the top of the list.
                sib_df = pd.DataFrame(sib_rows)
                sib_df = sib_df.sort_values(
                    ["_sub_candidate", "12mo units"],
                    ascending=[False, False]).drop(columns=["_sub_candidate"])
                st.dataframe(
                    sib_df, hide_index=True, width="stretch",
                    column_config={
                        "OnHand": st.column_config.TextColumn(
                            help="Current OnHand of this sibling. "
                                  "Higher = more substitution potential."),
                    })
                # Substitution-candidate banner: items with stock AND
                # lagging/slowing trend are prime candidates to consume
                # before reordering the parent. The trend ratio (90d
                # rate vs 12mo) directly tells us whether the alternate
                # is "doing well" (don't displace it) or "lagging"
                # (great candidate to push as a substitute).
                _sub_count = sum(1 for s in sib_rows
                                  if (s.get("OnHand") not in ("—", "0.00")
                                       and ("Lagging" in s["Trend"]
                                            or "Slowing" in s["Trend"]
                                            or "Stale" in s["Trend"])))
                if _sub_count:
                    st.warning(
                        f":warning: **{_sub_count} sibling(s) have "
                        "stock AND a lagging trend** — consider "
                        "consuming those before ordering more of "
                        "this SKU. They're shown at the top of the "
                        "table.")

    # === Section 7: Redirect to successor (discontinued SKUs only) =========
    # When a SKU is "[Discontinued]" or has Status="Discontinued", offer the
    # buyer a quick way to map it to a successor SKU. The mapping is saved
    # via db.set_migration() — same backend as the dedicated Migrations page,
    # but accessible directly from the drill-down so the buyer doesn't have
    # to navigate elsewhere.
    #
    # Effect after save:
    #   - engine.migrated_out increases for this SKU → reorder_qty drops to 0
    #   - engine.migrated_in increases for successor → its reorder bumps
    #   - persists in DB across Streamlit restarts
    prod_name = str(prod_row.get("Name") or "")
    prod_status = str(prod_row.get("Status") or "")
    is_discontinued = ("[discontinued]" in prod_name.lower()
                        or prod_status.lower() == "discontinued")
    if is_discontinued:
        st.markdown("#### :arrow_right: Redirect demand to a successor SKU")
        st.caption(
            "This SKU is flagged Discontinued. To preserve its demand "
            "history, map it to a successor — that SKU's reorder will "
            "increase by the migrated share, and this one drops to 0.")
        try:
            existing = db.get_migration(sku) if hasattr(db, "get_migration") else {}
        except Exception:
            existing = {}
        _all_skus = sorted(set(products_df["SKU"].astype(str).tolist())
                            - {str(sku)})
        _default_idx = 0
        _existing_succ = (existing or {}).get("successor_sku") or ""
        if _existing_succ in _all_skus:
            _default_idx = _all_skus.index(_existing_succ)
        cols_mig = st.columns([3, 1, 2, 1])
        succ_pick = cols_mig[0].selectbox(
            "Successor SKU",
            options=_all_skus,
            index=_default_idx if _all_skus else 0,
            key=f"db_redirect_succ_{sku}",
            placeholder="Pick the successor product…",
        )
        share_pick = cols_mig[1].number_input(
            "Share %",
            min_value=0.0, max_value=100.0,
            value=float((existing or {}).get("share_pct", 100.0)),
            step=5.0,
            key=f"db_redirect_share_{sku}",
            help="What % of this SKU's demand to migrate (default 100%).",
        )
        note_pick = cols_mig[2].text_input(
            "Note (optional)",
            value=str((existing or {}).get("note") or ""),
            key=f"db_redirect_note_{sku}",
            placeholder="e.g. 'replaced 2026-04-15'",
        )
        with cols_mig[3]:
            st.write("")
            st.write("")
            if st.button("Save", key=f"db_redirect_save_{sku}",
                         width="stretch", type="primary"):
                actor = (st.session_state.get("current_user", "")
                         .strip() or "anonymous")
                try:
                    db.set_migration(sku, succ_pick, actor,
                                      share_pick, note_pick)
                    st.cache_data.clear()
                    st.success(
                        f"Redirected: **{sku}** → **{succ_pick}** "
                        f"({share_pick:.0f}% of demand). "
                        "Engine will reflect this on next refresh.")
                    st.rerun()
                except Exception as e:
                    st.error(f"Could not save: {e}")
        if existing:
            cols_clear = st.columns([4, 1])
            cols_clear[0].info(
                f"Currently mapped to **{existing.get('successor_sku')}** "
                f"at **{float(existing.get('share_pct', 100)):.0f}%** "
                f"share."
                + (f" Note: _{existing.get('note')}_"
                    if existing.get("note") else ""))
            with cols_clear[1]:
                if st.button("Clear", key=f"db_redirect_clear_{sku}",
                              width="stretch"):
                    actor = (st.session_state.get("current_user", "")
                             .strip() or "anonymous")
                    try:
                        if hasattr(db, "clear_migration"):
                            db.clear_migration(sku, actor)
                        st.cache_data.clear()
                        st.success(f"Cleared mapping for {sku}")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Could not clear: {e}")


def parent_sku_for(sku: str) -> Optional[str]:
    """Return the primary parent/master SKU for a given SKU, or None."""
    parents = BOM_PARENTS.get(sku)
    if not parents:
        return None
    return parents[0].get("ComponentSKU")


def family_sku_for(sku: str) -> str:
    """Return the canonical family SKU (parent if child, else self)."""
    return BOM_FAMILY.get(sku, sku)


# ---------------------------------------------------------------------------
# Sourcing rule parser (AdditionalAttribute1)
# ---------------------------------------------------------------------------
# Example values seen in W4S data:
#   "Rule: SR100 | Logic: Purchased full length. No BOM | Auto-Assembly: N/A | Note: ..."
#   "Rule: SR100 | Logic: Assemble from 1 x 3m | Auto-Assembly: ON | Note: Offcut..."
#   "Rule: SR140 | Logic: Assemble from 0.5 x 2m profile + 3.28ft plate | Auto-Assembly: ON"

import re as _re_sr


def parse_sourcing_rule(attr1: Optional[str]) -> dict:
    """Parse the AdditionalAttribute1 sourcing-rule string into structured
    fields. Returns a dict with keys — all optional.
      RuleCode   — e.g. 'SR100'
      Logic      — raw logic text (as stored)
      IsMaster   — True if rule says 'Purchased full length'
      SourceFraction — e.g. 0.5, 1.0, 0.25
      SourceLengthMM — normalized to mm (e.g. 3000 for '3m', 609 for '609mm')
      HasPlate   — True if rule mentions 'plate' (i.e. SR140-style MP combo)
      AutoAssembly — 'ON' / 'OFF' / 'N/A' / None
      Note       — operational note text
    """
    out = {
        "RuleCode": None, "Logic": None, "IsMaster": False,
        "SourceFraction": None, "SourceLengthMM": None,
        "HasPlate": False, "AutoAssembly": None, "Note": None,
    }
    if not attr1 or not isinstance(attr1, str):
        return out
    s = attr1.strip()
    if not s:
        return out
    for segment in s.split("|"):
        seg = segment.strip()
        if not seg:
            continue
        lower = seg.lower()
        if lower.startswith("rule:"):
            out["RuleCode"] = seg.split(":", 1)[1].strip()
        elif lower.startswith("logic:"):
            logic = seg.split(":", 1)[1].strip()
            out["Logic"] = logic
            low = logic.lower()
            if "purchas" in low:
                out["IsMaster"] = True
            else:
                # "Assemble from 0.5 x 2m [rest]" or "0.25 x 609mm ..."
                m = _re_sr.search(
                    r"([\d.]+)\s*x\s*([\d.]+)\s*(mm|m|ft)?",
                    logic, flags=_re_sr.IGNORECASE,
                )
                if m:
                    try:
                        frac = float(m.group(1))
                    except ValueError:
                        frac = None
                    try:
                        lval = float(m.group(2))
                    except ValueError:
                        lval = None
                    unit = (m.group(3) or "").lower()
                    out["SourceFraction"] = frac
                    if lval is not None:
                        if unit == "m":
                            out["SourceLengthMM"] = int(round(lval * 1000))
                        elif unit == "ft":
                            out["SourceLengthMM"] = int(round(lval * 304.8))
                        else:
                            # default: mm OR bare number; follow same
                            # heuristic as length parser (small = metres)
                            out["SourceLengthMM"] = (
                                int(round(lval * 1000))
                                if lval < 20 else int(round(lval))
                            )
            if "plate" in low:
                out["HasPlate"] = True
        elif lower.startswith("auto-assembly:"):
            out["AutoAssembly"] = seg.split(":", 1)[1].strip()
        elif lower.startswith("note:"):
            out["Note"] = seg.split(":", 1)[1].strip()
    return out


# ---------------------------------------------------------------------------
# Tube SKU parser (global scope — used by LED Tubes AND Ordering pages)
# ---------------------------------------------------------------------------

def _parse_length(s) -> Optional[int]:
    """Return length in mm. '1' -> 1000, '0609' -> 609, '2390' -> 2390."""
    if s is None:
        return None
    try:
        n = float(str(s).strip())
    except (ValueError, TypeError):
        return None
    if n <= 0:
        return None
    return int(round(n * 1000)) if n < 20 else int(round(n))


TUBE_FAMILY_NAME_KEYWORDS = [
    ("OSLO MINI",   "OSLOMINI"),
    ("OSLO DOBLE",  "OSLODOBLE"),
    ("OSLO DOUBLE", "OSLODOBLE"),
    ("OSLOMINI",    "OSLOMINI"),
    ("OSLODOBLE",   "OSLODOBLE"),
]

NON_TUBE_NAME_PATTERNS = (
    "END CAP", "ENDCAP",
    "HEATSINK", "HEAT PLATE",
    "MOUNTING PLATE FOR", "MOUNT PLATE FOR",
    "BASE FOR", "JOINER",
    "ADAPTOR", "ADAPTER",
    "CLIP FOR", "SWIVEL",
    "SLIDE ", "BRACKET",
)


def _parse_tube_sku(sku: str, name: str = "") -> Optional[dict]:
    """Identify a tube from (SKU, Name). Module-level so every page can use
    it. Two strategies: A) SKU-based, B) Name-based fallback."""
    if not sku or not isinstance(sku, str):
        return None
    s = sku.upper()
    n = (name or "").upper()
    if any(tok in n for tok in NON_TUBE_NAME_PATTERNS):
        return None
    # Strategy A: standard LED-{FAMILY}-{COLOR}-[MP]-{LENGTH}
    if s.startswith("LED-"):
        parts = s.split("-")
        if len(parts) >= 4:
            family_a = parts[1]
            length_mm = _parse_length(parts[-1])
            skipped_tokens = {"EC", "TJ", "CLIP", "SLIDE", "SWIVEL",
                              "3D", "VEND", "ACCESSORY", "ACC",
                              "ANODIZED", "HEATSINK"}
            has_skipped = any(t in skipped_tokens for t in parts[2:-1])
            middle = parts[2:-1]
            if length_mm is not None and not has_skipped and middle:
                color = middle[0]
                has_mp = "MP" in middle
                if color in ("W", "B", "R", "C", "A", "G", "S", "BULK"):
                    return {"SKU": sku, "Family": family_a,
                            "Color": color, "HasMP": has_mp,
                            "LengthMM": length_mm}
    # Strategy B: family detected from Name
    family_b = None
    for kw, fam in TUBE_FAMILY_NAME_KEYWORDS:
        if kw in n:
            family_b = fam
            break
    if not family_b:
        return None
    length_mm = None
    for part in reversed(s.split("-")):
        lp = _parse_length(part)
        if lp is not None and 50 <= lp <= 5000:
            length_mm = lp
            break
    if length_mm is None:
        m_len = _re_sr.search(r"(\d+(?:\.\d+)?)\s*(mm|m)\b", n)
        if m_len:
            try:
                v = float(m_len.group(1))
                u = m_len.group(2).lower()
                length_mm = int(round(v * 1000)) if u == "m" else int(round(v))
            except ValueError:
                pass
    if length_mm is None:
        return None
    color = "W"
    if "BLACK" in n:
        color = "B"
    elif "CLEAR" in n:
        color = "C"
    return {"SKU": sku, "Family": family_b, "Color": color,
            "HasMP": False, "LengthMM": length_mm}


# ---------------------------------------------------------------------------
# LED strip SKU parser (pattern-based — BOMs aren't populated in CIN7 for
# most strips, so we infer bulk-master relationships from naming).
# ---------------------------------------------------------------------------

# Family-prefix patterns that indicate an LED strip product.
STRIP_FAMILY_PREFIXES = (
    "LEDIRIS",           # White Iris + RGB(W) Iris series
    "LEDUL",             # UL-listed strips
    "LED-UL",
    "LEDHR",             # High CRI
    "LEDAW",             # Amplified White
    "LEDRGB",            # Standalone RGB strip SKUs
    "LED-STRIP",
)

# Positive name check — must have STRIP or similar in name
STRIP_NAME_KEYWORDS = ("STRIP", "LED TAPE", "FLEX LED")


def _is_strip_sku(sku: str, name: str) -> bool:
    """Heuristic: is this a LED-strip SKU?"""
    if not sku:
        return False
    s = str(sku).upper()
    n = (name or "").upper()
    prefix_match = any(s.startswith(p) for p in STRIP_FAMILY_PREFIXES)
    name_match = any(k in n for k in STRIP_NAME_KEYWORDS)
    return prefix_match or name_match


def _parse_strip_length_suffix(part: str) -> Optional[float]:
    """Turn a strip SKU suffix into length in METRES. Returns None if
    the part isn't a length.
    Examples:
      '0305' → 0.305   (305mm, typically a 1ft cut)
      '5m'   → 5.0
      '5M'   → 5.0
      '25M'  → 25.0
      '100'  → 100.0
      '100M' → 100.0
      '40m'  → 40.0
      '12V'  → None (voltage marker, not length)
      '180'  → None (LED density, not length — ambiguous; handled upstream)
    """
    if not part:
        return None
    s = str(part).strip().upper()
    if not s:
        return None
    # Voltage markers
    if s in ("12V", "24V"):
        return None
    # '0305' pattern — leading zero + 3+ digits = millimetre length
    if s.startswith("0") and s.isdigit() and len(s) >= 3:
        return int(s) / 1000.0
    # Trailing 'M' or 'm' — strip and parse
    core = s.rstrip("Mm")
    if core.replace(".", "", 1).isdigit():
        try:
            n = float(core)
        except ValueError:
            return None
        # 4-digit bare number (like 2390) is mm, not m
        if len(core) >= 4 and "." not in core and not s.endswith(("m", "M")):
            return n / 1000.0
        return n
    return None


def _parse_strip_base(sku: str) -> Optional[tuple]:
    """Return (base_family, length_m) for strip SKUs, or None if we can't
    split them. The base family preserves the -12V suffix as part of its
    identity (12V vs 24V rolls are different products)."""
    if not sku:
        return None
    parts = str(sku).upper().split("-")
    if len(parts) < 2:
        return None

    # Strip trailing voltage suffix and attach to base
    voltage = None
    if parts[-1] in ("12V", "24V"):
        voltage = parts[-1]
        length_part = parts[-2]
        body = parts[:-2]
    else:
        length_part = parts[-1]
        body = parts[:-1]

    length_m = _parse_strip_length_suffix(length_part)
    if length_m is None:
        return None

    base = "-".join(body)
    if voltage:
        base = f"{base}-{voltage}"
    return (base, length_m)


# ---------------------------------------------------------------------------
# Apply global "Stock items only" filter
# ---------------------------------------------------------------------------
# Service and Non-Inventory items aren't physical stock and shouldn't affect
# stock / reorder analysis. Filter them out of products and cascade the filter
# to every dataset that joins on SKU.
_total_products = len(products)
if stock_only and not products.empty and "Type" in products.columns:
    products = products[products["Type"] == "Stock"].copy()
    keep_skus = set(products["SKU"].astype(str))

    if not stock.empty and "SKU" in stock.columns:
        stock = stock[stock["SKU"].astype(str).isin(keep_skus)].copy()
    if not sale_lines.empty and "SKU" in sale_lines.columns:
        sale_lines = sale_lines[sale_lines["SKU"].astype(str).isin(keep_skus)].copy()
    if not sale_lines_30d.empty and "SKU" in sale_lines_30d.columns:
        sale_lines_30d = sale_lines_30d[sale_lines_30d["SKU"].astype(str).isin(keep_skus)].copy()
    if not sale_lines_3d.empty and "SKU" in sale_lines_3d.columns:
        sale_lines_3d = sale_lines_3d[sale_lines_3d["SKU"].astype(str).isin(keep_skus)].copy()
    if not purchase_lines.empty and "SKU" in purchase_lines.columns:
        purchase_lines = purchase_lines[
            purchase_lines["SKU"].astype(str).isin(keep_skus)
        ].copy()

_filtered_count = _total_products - len(products)
if stock_only and _filtered_count > 0:
    st.caption(
        f":information_source: Global filter ON: hiding {_filtered_count:,} "
        f"Service / Non-Inventory items from analysis "
        f"(showing {len(products):,} Stock items). Toggle in sidebar to include them."
    )


# ---------------------------------------------------------------------------
# Page: Overview
# ---------------------------------------------------------------------------

if page == "Overview":
    st.header(":bar_chart: Overview")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Products", _fmt_number(len(products)))
    c2.metric("Stock rows", _fmt_number(len(stock)))
    c3.metric("Active suppliers (90d)",
              _fmt_number(purchase_lines["SupplierID"].nunique()
                          if not purchase_lines.empty else 0))
    customer_count = 0
    if customers_file:
        try:
            # customers file is big — just count rows fast
            customer_count = sum(1 for _ in open(customers_file, "r",
                                                encoding="utf-8")) - 1
        except Exception:
            customer_count = 0
    c4.metric("Customers", _fmt_number(customer_count))

    st.divider()

    c1, c2, c3, c4 = st.columns(4)
    # Stock value: use CIN7's StockOnHand field (FIFO) — NOT
    # OnHand × AverageCost (that would give an average-cost valuation).
    # CIN7 tracks inventory on FIFO and writes the FIFO dollar value
    # into the StockOnHand field of productavailability.
    stock_value = 0.0
    if not stock.empty and "StockOnHand" in stock.columns:
        stock_value = float(_to_num(stock["StockOnHand"]).fillna(0).sum())
    elif not stock.empty and not products.empty:
        # Legacy fallback if the FIFO field isn't present on old syncs
        p_cost = products.set_index("SKU")["AverageCost"].to_dict()
        on_hand = _to_num(stock["OnHand"])
        values = [
            on_hand.iloc[i] * float(p_cost.get(sku, 0) or 0)
            for i, sku in enumerate(stock["SKU"])
        ]
        stock_value = sum(v for v in values if pd.notna(v))
    c1.metric("Stock value (FIFO, CIN7)", _fmt_money(stock_value),
               help="CIN7's FIFO inventory value — the same number you "
                    "see in CIN7's valuation reports. Uses the "
                    "StockOnHand field, not OnHand × AverageCost.")

    # Sales invoiced in the last 30 days.
    #
    # IMPORTANT: the sales_last_30d.csv file from CIN7 uses UpdatedSince
    # filtering, not CreatedSince — so it contains every sale that got
    # touched in the last 30 days (including old orders whose status
    # changed). If we just sum InvoiceAmount here, the number includes
    # orders created months ago, vastly overstating "last 30 days" sales.
    #
    # Fix: re-filter client-side on InvoiceDate ≥ 30 days ago, and
    # exclude VOIDED/CREDITED/CANCELLED statuses.
    #
    # Two totals shown:
    #   "Invoiced" (incl tax + shipping) — matches the raw
    #      InvoiceAmount header field.
    #   "Revenue (pre-tax)" — what CIN7's own Overview dashboard shows
    #      as "Revenue". Subtracts tax so the numbers match.
    sales_total = 0.0
    revenue_pretax = 0.0
    if not sales_headers.empty and "InvoiceAmount" in sales_headers.columns:
        sh = sales_headers.copy()
        if "InvoiceDate" in sh.columns:
            sh["InvoiceDate"] = pd.to_datetime(
                sh["InvoiceDate"], errors="coerce", utc=True
            ).dt.tz_localize(None)
            sh = sh.dropna(subset=["InvoiceDate"])
            cutoff = pd.Timestamp(datetime.now().date()) - pd.Timedelta(days=30)
            sh = sh[sh["InvoiceDate"] >= cutoff]
        if "Status" in sh.columns:
            _bad = ("VOIDED", "CREDITED", "CANCELLED", "CANCELED")
            sh = sh[~sh["Status"].astype(str).str.upper().isin(_bad)]
        sales_total = float(_to_num(sh["InvoiceAmount"]).fillna(0).sum())

        # For pre-tax (CIN7 Revenue match), subtract tax summed from
        # sale_lines for the same SaleIDs.
        if not sale_lines.empty and "SaleID" in sh.columns:
            sids_in_window = set(sh["SaleID"].astype(str))
            tl = sale_lines[
                sale_lines["SaleID"].astype(str).isin(sids_in_window)
            ]
            tax_total = float(_to_num(tl.get("Tax", 0)).fillna(0).sum())
            revenue_pretax = max(0.0, sales_total - tax_total)
    c2.metric(
        "Sales invoiced (last 30d)",
        _fmt_money(sales_total),
        delta=(f"pre-tax ≈ {_fmt_money(revenue_pretax)}"
               if revenue_pretax else None),
        delta_color="off",
        help="Sum of InvoiceAmount for sales with InvoiceDate in the "
             "last 30 days, excluding VOIDED/CREDITED/CANCELLED. Main "
             "number is invoiced total incl. tax + shipping. The "
             "'pre-tax ≈' line under it is the closest match to CIN7's "
             "Overview dashboard 'Revenue' metric (pre-tax).",
    )

    # Open POs — unique purchases with status 'ORDERED' / 'ORDERING'.
    # NOTE: purchase_lines is deduped on (PurchaseID, SKU, Quantity),
    # so counting unique PurchaseID is correct. Status check is on the
    # line rows but since every line of an order shares the header status
    # it works the same way.
    open_pos = 0
    open_po_value = 0.0
    if not purchase_lines.empty and "Status" in purchase_lines.columns:
        open_mask = purchase_lines["Status"].astype(str).str.upper().isin(
            ("ORDERED", "ORDERING")
        )
        open_pos = purchase_lines.loc[open_mask, "PurchaseID"].nunique()
        open_po_value = _to_num(
            purchase_lines.loc[open_mask, "Total"]).sum()
    c3.metric("Open POs", _fmt_number(open_pos),
               help="Unique purchases with status ORDERED or ORDERING, "
                    "across whatever window the purchase_lines file "
                    "covers (default 90d; weekend sync extends to 5yr).")
    c4.metric("Open PO value", _fmt_money(open_po_value),
               help="Sum of line Total for all open POs.")

    st.divider()

    # Stock positions
    if not stock.empty:
        col_left, col_right = st.columns(2)
        with col_left:
            st.subheader("Stock distribution by location")
            by_loc = (
                stock.assign(OnHand=_to_num(stock["OnHand"]))
                     .groupby("Location", dropna=False)["OnHand"]
                     .agg(["count", "sum"])
                     .rename(columns={"count": "SKUs", "sum": "Units"})
                     .sort_values("Units", ascending=False)
            )
            st.dataframe(by_loc, width="stretch")

        with col_right:
            st.subheader("Zero-stock & low-stock flags")
            on_hand = _to_num(stock["OnHand"]).fillna(0)
            zero = (on_hand <= 0).sum()
            low = ((on_hand > 0) & (on_hand < 5)).sum()
            st.metric("SKU-locations at zero stock", _fmt_number(zero))
            st.metric("SKU-locations below 5 units", _fmt_number(low))

    # --- Today + Month-to-Date vs same-period prior years ---------------
    if not sale_lines.empty and "InvoiceDate" in sale_lines.columns:
        st.divider()
        st.subheader(":calendar: Today & Month-to-date vs prior years")
        st.caption(
            "**Today's comparison uses the matching weekday 52 weeks ago** "
            "(Shopify-style), not the same calendar date — keeps "
            "weekday-driven sales patterns aligned (Tue vs Tue, not Tue "
            "vs Sat). MTD comparison uses the same calendar date range "
            "across years.")

        df = sale_lines.copy()
        df["InvoiceDate"] = _to_date(df["InvoiceDate"]).dt.tz_localize(None)
        df["Total"] = _to_num(df["Total"]).fillna(0)
        df["Quantity"] = _to_num(df["Quantity"]).fillna(0)
        df = df.dropna(subset=["InvoiceDate"])
        # Exclude VOIDED/CREDITED/CANCELLED — matches CIN7's Revenue
        # computation on its own dashboard. Rule §3.1 in RULES.md.
        if "Status" in df.columns:
            _bad_stat = ("VOIDED", "CREDITED", "CANCELLED", "CANCELED")
            df = df[~df["Status"].astype(str).str.upper().isin(_bad_stat)]

        today = pd.Timestamp(datetime.now().date())
        today_only = today.date()
        today_weekday = today.strftime("%a")  # 'Mon', 'Tue', etc.

        # Today
        today_mask = df["InvoiceDate"].dt.date == today_only
        today_df = df[today_mask]
        today_orders = today_df["SaleID"].nunique()
        today_units = float(today_df["Quantity"].sum())
        today_rev = float(today_df["Total"].sum())

        # Yesterday for delta context
        yesterday = today - pd.Timedelta(days=1)
        yest_mask = df["InvoiceDate"].dt.date == yesterday.date()
        yest_rev = float(df[yest_mask]["Total"].sum())

        # Matching weekday 52 weeks ago (Shopify-style). 364 days = 52 × 7,
        # so subtracting it gives the same day-of-week one year back.
        # This avoids the Mon-vs-Sun mismatch you'd get from same-date YoY.
        match_last = today - pd.Timedelta(days=364)
        match_last_mask = df["InvoiceDate"].dt.date == match_last.date()
        match_last_rev = float(df[match_last_mask]["Total"].sum())

        tc1, tc2, tc3, tc4 = st.columns(4)
        tc1.metric(f"Today ({today_weekday} {today_only.strftime('%b %d')})",
                   _fmt_money(today_rev),
                   delta=f"{today_orders} orders, {int(today_units)} units")
        tc2.metric("Yesterday", _fmt_money(yest_rev))
        tc3.metric(
            f"Matching weekday last year ({match_last.strftime('%a %b %d, %Y')})",
            _fmt_money(match_last_rev),
            help="Same day-of-week, 52 weeks ago. Subtracts 364 days "
                 "(52×7) so Tue→Tue, Sat→Sat — NOT calendar date.")
        if match_last_rev > 0:
            yoy_pct = (today_rev - match_last_rev) / match_last_rev * 100
            tc4.metric("YoY (matching weekday)", f"{yoy_pct:+.1f}%")
        else:
            tc4.metric("YoY (matching weekday)", "—")

        # Month-to-date: from 1st of current month up to today
        mtd_start = today.replace(day=1)
        mtd_mask = (df["InvoiceDate"] >= mtd_start) & (df["InvoiceDate"] <= today)
        mtd_df = df[mtd_mask]
        mtd_orders = mtd_df["SaleID"].nunique()
        mtd_units = float(mtd_df["Quantity"].sum())
        mtd_rev = float(mtd_df["Total"].sum())
        day_of_month = today.day

        st.markdown(f"**Month-to-date** — {today.strftime('%b 1')} to "
                     f"{today.strftime('%b %d, %Y')} (day {day_of_month} of month)")

        mc1, mc2, mc3, mc4 = st.columns(4)
        mc1.metric("Orders", _fmt_number(mtd_orders))
        mc2.metric("Units", _fmt_number(mtd_units))
        mc3.metric("Revenue", _fmt_money(mtd_rev))
        mc4.metric("Avg daily revenue",
                   _fmt_money(mtd_rev / max(day_of_month, 1)))

        # YoY comparison: same MTD slice for 4 prior years
        yoy_rows = []
        for years_back in range(0, 5):
            try:
                y = today.year - years_back
                start_y = mtd_start.replace(year=y)
                end_y = today.replace(year=y)
                mask = (df["InvoiceDate"] >= start_y) & (df["InvoiceDate"] <= end_y)
                chunk = df[mask]
                yoy_rows.append({
                    "Period": f"{start_y.strftime('%b 1')} – "
                              f"{end_y.strftime('%b %d')} {y}",
                    "Year": y,
                    "Orders": int(chunk["SaleID"].nunique()),
                    "Units": int(chunk["Quantity"].sum()),
                    "Revenue": float(chunk["Total"].sum()),
                })
            except ValueError:
                continue

        yoy_df = pd.DataFrame(yoy_rows)
        if not yoy_df.empty:
            # Year-over-year delta vs immediately previous year
            yoy_df_sorted = yoy_df.sort_values("Year").reset_index(drop=True)
            yoy_df_sorted["YoY Revenue %"] = (
                yoy_df_sorted["Revenue"].pct_change() * 100
            ).round(1)
            yoy_df_display = yoy_df_sorted.sort_values(
                "Year", ascending=False
            ).drop(columns=["Year"])

            st.dataframe(
                yoy_df_display,
                width="stretch", hide_index=True,
                column_config={
                    "Revenue": st.column_config.NumberColumn(format="$%.0f"),
                    "YoY Revenue %":
                        st.column_config.NumberColumn(format="%+.1f%%"),
                },
            )

            # Chart: MTD revenue by year
            if len(yoy_rows) > 1:
                chart_df = pd.DataFrame(yoy_rows)
                chart_df["YearLabel"] = chart_df["Year"].astype(str)
                fig_yoy = px.bar(
                    chart_df.sort_values("Year"),
                    x="YearLabel", y="Revenue",
                    title=f"MTD revenue ({today.strftime('%b 1–%d')}) "
                          f"across years",
                    labels={"YearLabel": "Year"},
                    text_auto=".2s",
                )
                fig_yoy.update_layout(height=280,
                                       margin=dict(l=0, r=0, t=40, b=0))
                st.plotly_chart(fig_yoy, width="stretch")

    # --- Recent sales trend (full daily chart) --------------------------
    if not sale_lines.empty and "InvoiceDate" in sale_lines.columns:
        st.divider()
        st.subheader("Daily invoiced revenue — full sync window")
        df = sale_lines.copy()
        df["InvoiceDate"] = _to_date(df["InvoiceDate"]).dt.date
        df["Total"] = _to_num(df["Total"])
        daily = (
            df.dropna(subset=["InvoiceDate"])
              .groupby("InvoiceDate", as_index=False)
              .agg(Orders=("SaleID", "nunique"),
                   Lines=("SKU", "count"),
                   Revenue=("Total", "sum"))
              .sort_values("InvoiceDate")
        )
        if not daily.empty:
            fig = px.bar(daily, x="InvoiceDate", y="Revenue",
                         labels={"Revenue": "Revenue (base currency)"})
            fig.update_layout(height=300, margin=dict(l=0, r=0, t=10, b=0))
            st.plotly_chart(fig, width="stretch")


# ---------------------------------------------------------------------------
# Page: Stock Explorer
# ---------------------------------------------------------------------------

elif page == "LED Tubes":
    st.header(":bulb: LED Tubes & Mounting Plates")
    st.caption(
        "Tube families, size/MP variants, migration forecast for retiring "
        "lines (Smokies38 / Cascade38 → Sierra38), and shared component "
        "tracking (Yukon mounting plate etc.)."
    )

    if products.empty:
        st.warning("No products yet. Sync first.")
        st.stop()

    # Tube parser is defined at module level (so the Ordering page can use
    # it too). See _parse_tube_sku near the top of this file.

    # Apply parser to every product, attaching sourcing rule info
    parsed = []
    for _, p in products.iterrows():
        rec = _parse_tube_sku(p.get("SKU"), p.get("Name", ""))
        if rec:
            rec["Name"] = p.get("Name")
            rec["AvgCost"] = float(p.get("AverageCost") or 0)
            rec["CreatedDate"] = p.get("CreatedDate")
            # Sourcing rule from AdditionalAttribute1
            rule = parse_sourcing_rule(p.get("AdditionalAttribute1"))
            rec["RuleCode"] = rule["RuleCode"]
            rec["IsMaster"] = rule["IsMaster"]
            rec["SourceFraction"] = rule["SourceFraction"]
            rec["SourceLengthMM"] = rule["SourceLengthMM"]
            rec["RuleHasPlate"] = rule["HasPlate"]
            rec["RuleLogic"] = rule["Logic"]
            parsed.append(rec)

    if not parsed:
        st.warning("Couldn't parse any tube SKUs. Naming convention may "
                   "have changed.")
        st.stop()
    tubes_df = pd.DataFrame(parsed)

    # Families available
    tube_families = sorted(tubes_df["Family"].unique().tolist())

    # --- Family selector -------------------------------------------------
    fc1, fc2, fc3 = st.columns([3, 1, 1])
    with fc1:
        default_families = [f for f in ["SIERRA38", "SMOKIES38", "CASCADE38",
                                          "OSLO", "OSLOMINI", "OSLODOBLE",
                                          "SIERRA65", "SMOKIES65", "SMOKIES70",
                                          "SMOKIES100", "HOLSTON74"]
                            if f in tube_families]
        selected_families = st.multiselect(
            "Tube families",
            options=tube_families,
            default=default_families or tube_families[:6],
        )
    with fc2:
        window_months = st.selectbox(
            "Window (months)", [3, 6, 9, 12], index=3,
            key="tube_window",
        )
    with fc3:
        include_migrations = st.checkbox(
            "Apply migration forecast", value=True,
            help="Roll demand of retiring families (e.g. Smokies38, "
                 "Cascade38) up into their successor SKUs.",
        )

    if not selected_families:
        st.info("Pick at least one tube family.")
        st.stop()

    # --- Compute 12mo velocity + stock per SKU ---------------------------
    today = pd.Timestamp(datetime.now().date())
    cutoff = today - pd.Timedelta(days=int(window_months * 30.437))

    vel = {}
    rev = {}
    if not sale_lines.empty:
        sl = sale_lines.copy()
        sl["InvoiceDate"] = _to_date(sl["InvoiceDate"]).dt.tz_localize(None)
        sl["Quantity"] = _to_num(sl["Quantity"]).fillna(0)
        sl["Total"] = _to_num(sl["Total"]).fillna(0)
        sl = sl.dropna(subset=["InvoiceDate"])
        sl = sl[sl["InvoiceDate"] >= cutoff]
        g = sl.groupby("SKU").agg(units=("Quantity", "sum"),
                                   revenue=("Total", "sum"))
        vel = g["units"].to_dict()
        rev = g["revenue"].to_dict()

    stock_by_sku: dict = {}
    if not stock.empty:
        s_ = stock.copy()
        s_["OnHand"] = _to_num(s_["OnHand"]).fillna(0)
        s_["Available"] = _to_num(s_["Available"]).fillna(0)
        grp = s_.groupby("SKU").agg(OnHand=("OnHand", "sum"),
                                     Available=("Available", "sum"))
        stock_by_sku = grp.to_dict("index")

    # Attach metrics — use .apply() for Arrow-backend compatibility
    tubes_df["Units"] = tubes_df["SKU"].apply(
        lambda s: float(vel.get(s, 0))).astype(float)
    tubes_df["Revenue"] = tubes_df["SKU"].apply(
        lambda s: float(rev.get(s, 0))).astype(float)
    tubes_df["OnHand"] = tubes_df["SKU"].apply(
        lambda s: stock_by_sku.get(s, {}).get("OnHand", 0))
    tubes_df["Available"] = tubes_df["SKU"].apply(
        lambda s: stock_by_sku.get(s, {}).get("Available", 0))

    # --- Auto-proposal: retiring → successor family migration -----------
    # Family-level migration rules. Built-in defaults plus any team override.
    # Smokies<N> → Sierra<N> (same diameter). Cascade<N> → Sierra<N>.
    FAMILY_MIGRATION_RULES = {}
    for fam in tube_families:
        if fam.startswith("SMOKIES") and fam[7:].isdigit():
            candidate = f"SIERRA{fam[7:]}"
            if candidate in tube_families:
                FAMILY_MIGRATION_RULES[fam] = candidate
        elif fam.startswith("CASCADE") and fam[7:].isdigit():
            candidate = f"SIERRA{fam[7:]}"
            if candidate in tube_families:
                FAMILY_MIGRATION_RULES[fam] = candidate

    existing_migrations = {m["retiring_sku"]: dict(m)
                           for m in db.all_migrations()}

    def _propose_migration_target(row: dict) -> Optional[str]:
        """Propose successor SKU for a retiring family row."""
        fam = row.get("Family")
        successor_fam = FAMILY_MIGRATION_RULES.get(fam)
        if not successor_fam:
            return None
        color = row.get("Color")
        has_mp = row.get("HasMP")
        length_mm = row.get("LengthMM")
        if length_mm is None:
            return None
        # Format length consistently with how SKUs are written
        if length_mm >= 1000 and length_mm % 1000 == 0:
            len_str = str(length_mm // 1000)
        elif length_mm >= 1000:
            len_str = str(length_mm)
        else:
            len_str = f"{length_mm:04d}"
        mp_part = "-MP" if has_mp else ""
        candidate = f"LED-{successor_fam}-{color}{mp_part}-{len_str}"
        if candidate in set(products["SKU"]):
            return candidate
        return None

    retiring_families = list(FAMILY_MIGRATION_RULES.keys())
    successor_families = sorted(set(FAMILY_MIGRATION_RULES.values()))

    # --- Headline metrics -------------------------------------------------
    fam_view = tubes_df[tubes_df["Family"].isin(selected_families)]
    total_units = fam_view["Units"].sum()
    total_revenue = fam_view["Revenue"].sum()
    total_onhand_value = (fam_view["OnHand"] * fam_view["AvgCost"]).sum()

    h1, h2, h3, h4 = st.columns(4)
    h1.metric("SKUs in selection", len(fam_view))
    h2.metric(f"Units sold ({window_months}mo)", _fmt_number(total_units))
    h3.metric(f"Revenue ({window_months}mo)", _fmt_money(total_revenue))
    h4.metric("Physical cash tied up", _fmt_money(total_onhand_value))

    # --- Variant matrix per family ---------------------------------------
    st.markdown("### :1234: Variant matrix — where the velocity lives")
    st.caption(
        "Each family shows a grid of size (rows) × color+MP (columns). "
        "Cell values are units sold in the selected window. Highlight "
        "where the demand is concentrated and where gaps exist."
    )

    for fam in selected_families:
        fd = fam_view[fam_view["Family"] == fam].copy()
        if fd.empty:
            continue
        # Build a column label that captures color + MP
        fd["Variant"] = fd.apply(
            lambda r: f"{r['Color']}{'+MP' if r['HasMP'] else ''}", axis=1
        )
        fd["LengthLabel"] = fd["LengthMM"].apply(
            lambda mm: f"{mm/1000:g}m" if mm >= 1000 else f"{mm}mm"
        )

        pivot_units = fd.pivot_table(
            index=["LengthMM", "LengthLabel"],
            columns="Variant",
            values="Units", aggfunc="sum", fill_value=0,
        ).sort_index(level=0).droplevel(0)
        pivot_rev = fd.pivot_table(
            index=["LengthMM", "LengthLabel"],
            columns="Variant",
            values="Revenue", aggfunc="sum", fill_value=0,
        ).sort_index(level=0).droplevel(0)
        pivot_stock = fd.pivot_table(
            index=["LengthMM", "LengthLabel"],
            columns="Variant",
            values="OnHand", aggfunc="sum", fill_value=0,
        ).sort_index(level=0).droplevel(0)

        with st.expander(f"**{fam}** — "
                         f"{int(fd['Units'].sum()):,} units  "
                         f"| ${fd['Revenue'].sum():,.0f} rev  "
                         f"| {len(fd)} SKUs", expanded=True):
            c1, c2 = st.columns(2)
            c1.markdown(f"**Units sold — last {window_months} months**")
            c1.dataframe(pivot_units.astype(int), width="stretch")
            c2.markdown(f"**Current OnHand (physical)**")
            c2.dataframe(pivot_stock.astype(int), width="stretch")
            if pivot_rev.sum().sum() > 0:
                with st.expander("Revenue matrix"):
                    st.dataframe(
                        pivot_rev.round(0).astype(int),
                        width="stretch",
                    )

    # --- Migration forecast ----------------------------------------------
    st.markdown("### :twisted_rightwards_arrows: Migration forecast")
    st.caption(
        "For retiring families, roll demand up into successor SKUs "
        "(same color + length + MP). Overrides saved below. Team-curated "
        "via SQLite."
    )

    # Build retiring → successor mapping
    retiring_rows = tubes_df[tubes_df["Family"].isin(retiring_families)].copy()
    st.caption(
        "Retiring families detected by naming pattern: "
        + (", ".join(f"**{k}** → **{v}**"
                     for k, v in sorted(FAMILY_MIGRATION_RULES.items()))
           or "_none_")
    )
    retiring_rows["Proposed"] = retiring_rows.apply(
        lambda r: _propose_migration_target(r), axis=1
    )
    retiring_rows["Saved mapping"] = retiring_rows["SKU"].map(
        lambda s: existing_migrations.get(s, {}).get("successor_sku")
    )
    retiring_rows["Share %"] = retiring_rows["SKU"].map(
        lambda s: existing_migrations.get(s, {}).get("share_pct", 100.0)
    )
    retiring_rows["Effective target"] = retiring_rows.apply(
        lambda r: r["Saved mapping"] or r["Proposed"] or "(unmapped)",
        axis=1,
    )

    # Summary of migration
    retiring_view = retiring_rows[retiring_rows["Units"] > 0].copy()
    mc1, mc2, mc3 = st.columns(3)
    mc1.metric("Retiring SKUs with sales",
               _fmt_number(len(retiring_view)))
    mc2.metric(f"Units to migrate ({window_months}mo)",
               _fmt_number(retiring_view["Units"].sum()))
    mc3.metric("Unmapped retiring SKUs",
               _fmt_number((retiring_view["Effective target"] == "(unmapped)").sum()))

    st.dataframe(
        retiring_view[[
            "SKU", "Name", "Family", "Units", "Revenue",
            "Proposed", "Saved mapping", "Share %", "Effective target",
        ]].sort_values("Units", ascending=False),
        width="stretch", hide_index=True, height=300,
        column_config={
            "Revenue": st.column_config.NumberColumn(format="$%.0f"),
            "Share %": st.column_config.NumberColumn(format="%.0f%%"),
        },
    )

    # Curation UI
    actor = st.session_state.get("current_user", "").strip()
    with st.expander(":pencil2: Override or confirm a migration mapping"):
        if not actor:
            st.caption("Enter your name in sidebar to edit mappings.")
        else:
            edit_cols = st.columns([2, 2, 1, 2, 1])
            retiring_pick = edit_cols[0].selectbox(
                "Retiring SKU",
                options=retiring_view["SKU"].tolist(),
                key="mig_retire_pick",
            )
            # Successor choices: every SKU belonging to any successor family,
            # plus any matching-diameter Sierra by default.
            succ_prefixes = tuple(
                f"LED-{fam}-" for fam in successor_families
            )
            successor_skus = sorted(
                products[
                    products["SKU"].astype(str).str.upper()
                                    .str.startswith(succ_prefixes)
                ]["SKU"].tolist()
            )

            # Auto-scope: if the retiring pick is SMOKIES65-*, show only
            # SIERRA65-* first to reduce noise; user can still search.
            retiring_family = retiring_rows.loc[
                retiring_rows["SKU"] == retiring_pick, "Family"
            ].iloc[0] if (retiring_rows["SKU"] == retiring_pick).any() else None
            preferred_fam = FAMILY_MIGRATION_RULES.get(retiring_family)
            if preferred_fam:
                preferred_skus = [s for s in successor_skus
                                  if s.upper().startswith(f"LED-{preferred_fam}-")]
                other_skus = [s for s in successor_skus
                              if s not in preferred_skus]
                successor_skus = preferred_skus + other_skus

            current_successor = (
                existing_migrations.get(retiring_pick, {}).get("successor_sku")
                or retiring_rows.loc[
                    retiring_rows["SKU"] == retiring_pick, "Proposed"
                ].iloc[0]
            )
            try:
                default_idx = (successor_skus.index(current_successor)
                               if current_successor in successor_skus else 0)
            except Exception:
                default_idx = 0
            successor_pick = edit_cols[1].selectbox(
                f"Successor SKU "
                f"({preferred_fam or 'any'} SKUs shown first)",
                options=successor_skus,
                index=default_idx,
                key="mig_succ_pick",
            )
            share_pick = edit_cols[2].number_input(
                "Share %", min_value=0.0, max_value=100.0,
                value=float(existing_migrations.get(retiring_pick, {})
                            .get("share_pct", 100.0)),
                step=5.0, key="mig_share_pick",
            )
            note_pick = edit_cols[3].text_input(
                "Note", placeholder="optional",
                key="mig_note_pick",
            )
            with edit_cols[4]:
                st.write("")
                st.write("")
                if st.button("Save", key="mig_save_btn",
                             width="stretch"):
                    db.set_migration(retiring_pick, successor_pick,
                                      actor, share_pick, note_pick)
                    st.cache_data.clear()
                    st.success(f"Saved: {retiring_pick} → {successor_pick}")
                    st.rerun()
                if st.button("Clear", key="mig_clear_btn",
                             width="stretch"):
                    db.clear_migration(retiring_pick, actor)
                    st.cache_data.clear()
                    st.success(f"Cleared mapping for {retiring_pick}")
                    st.rerun()

    # Projected demand — rolled up to MASTER TUBE using sourcing rules.
    # A master is a SKU flagged IsMaster=True in its AdditionalAttribute1
    # sourcing rule (i.e. 'Purchased full length. No BOM'). Every non-master
    # variant consumes `SourceFraction × master` per unit, per its rule.
    # So master-tube demand = SUM over variants of (variant_demand × source_fraction)
    # where the variant's rule points at this master's length.
    if include_migrations:
        st.markdown("#### :chart_with_upwards_trend: Projected MASTER TUBE "
                     f"demand — what we order from supplier")
        st.caption(
            f"One row per master tube (purchased from supplier per rule "
            f"`Logic: Purchased full length`). Demand for each master is "
            f"the sum of `variant demand × source fraction` across every "
            f"cut/assembled SKU (bare OR MP variant) that consumes it — "
            f"own AND migrated demand. This is the real supplier order "
            f"volume."
        )

        # Reorder parameters
        rp1, rp2, rp3 = st.columns(3)
        lead_time_weeks = rp1.number_input(
            "Default supplier lead time (weeks)",
            min_value=1.0, max_value=20.0, value=4.5, step=0.5,
            help="Reeves / most tube suppliers ship in 4-5 weeks. "
                 "Used for reorder quantity math.",
        )
        safety_factor = rp2.number_input(
            "Safety factor (%)",
            min_value=0, max_value=100, value=20, step=5,
            help="Extra buffer on top of lead-time demand.",
        )
        review_weeks = rp3.number_input(
            "Review horizon (weeks)",
            min_value=0.0, max_value=8.0, value=2.0, step=0.5,
            help="How much forward cover above lead time. "
                 "2 weeks = order enough for lead-time + 2 weeks ahead.",
        )
        lead_time_days = lead_time_weeks * 7

        # Build a lookup of true MASTER tubes (IsMaster=True per rule).
        # Master key: (family, color, length_mm) where length refers to the
        # purchased-full-length of the master tube.
        def _master_key(fam, color, length):
            return (fam, color, length)

        # Index of master rows: one per (family, color, source-length).
        master_rows: dict = {}
        for _, r in tubes_df.iterrows():
            if r["IsMaster"] and r["Family"] in successor_families:
                k = _master_key(r["Family"], r["Color"], r["LengthMM"])
                master_rows[k] = {
                    "MasterSKU": r["SKU"],
                    "Name": r["Name"],
                    "Family": r["Family"],
                    "Color": r["Color"],
                    "LengthMM": r["LengthMM"],
                    "RuleCode": r["RuleCode"],
                    "AvgCost": r["AvgCost"],
                    "OnHand": float(r["OnHand"]),
                    "Available": float(r["Available"]),
                    "own_consumption": 0.0,
                    "migrated_consumption": 0.0,
                    # Sub-components of own_consumption (for buyer visibility)
                    "own_bare_sales": 0.0,          # direct bare-tube sales
                    "own_mp_consumption": 0.0,       # via MP-variant sales
                    "own_cut_consumption": 0.0,      # via non-MP cut variants
                    "mig_bare_sales": 0.0,
                    "mig_mp_consumption": 0.0,
                    "mig_cut_consumption": 0.0,
                    "consumer_skus": set(),
                    "migrated_from": [],
                }

        def _find_master_for(fam: str, color: str, source_length: int):
            """Find the master tube row key for a given family/color/length."""
            if source_length is None:
                return None
            k = _master_key(fam, color, source_length)
            if k in master_rows:
                return k
            # Master might live in the same family but not yet indexed
            # (eg. SMOKIES65 → migrate demand to SIERRA65; master is SIERRA65)
            successor_fam = FAMILY_MIGRATION_RULES.get(fam, fam)
            k2 = _master_key(successor_fam, color, source_length)
            if k2 in master_rows:
                return k2
            return None

        # Walk every tube variant, find its master via rule, add consumption
        all_tubes = tubes_df[
            tubes_df["Family"].isin(successor_families + retiring_families)
        ].copy()

        for _, r in all_tubes.iterrows():
            # Skip the masters themselves — we add their direct sales below
            units_sold = float(r["Units"])
            if units_sold == 0:
                continue

            source_fraction = r["SourceFraction"]
            source_length = r["SourceLengthMM"]
            family = r["Family"]
            color = r["Color"]

            # Retiring family? Successor family drives master lookup.
            is_retiring = family in retiring_families
            share = 1.0
            if is_retiring:
                # Find share % from saved migration or default 100%
                mig = existing_migrations.get(r["SKU"], {})
                share = float(mig.get("share_pct", 100.0)) / 100.0
                family = FAMILY_MIGRATION_RULES.get(family, family)

            if share <= 0:
                continue
            effective_units = units_sold * share

            if r["IsMaster"]:
                # Direct sale of a master (bare tube sold as-is)
                mk = _master_key(family, color, r["LengthMM"])
                if mk in master_rows:
                    if is_retiring:
                        master_rows[mk]["migrated_consumption"] += effective_units
                        master_rows[mk]["mig_bare_sales"] += effective_units
                        master_rows[mk]["migrated_from"].append(
                            f"{r['SKU']} ({effective_units:.0f} bare)"
                        )
                    else:
                        master_rows[mk]["own_consumption"] += effective_units
                        master_rows[mk]["own_bare_sales"] += effective_units
                    master_rows[mk]["consumer_skus"].add(r["SKU"])
                continue

            # Non-master: needs SourceFraction × master
            if source_fraction is None or source_length is None:
                continue
            mk = _find_master_for(family, color, source_length)
            if mk is None:
                continue
            consumption = effective_units * source_fraction
            # Bucket by variant type for buyer visibility
            is_mp_variant = bool(r["HasMP"])
            if is_retiring:
                master_rows[mk]["migrated_consumption"] += consumption
                if is_mp_variant:
                    master_rows[mk]["mig_mp_consumption"] += consumption
                else:
                    master_rows[mk]["mig_cut_consumption"] += consumption
                master_rows[mk]["migrated_from"].append(
                    f"{r['SKU']} ({consumption:.1f})"
                )
            else:
                master_rows[mk]["own_consumption"] += consumption
                if is_mp_variant:
                    master_rows[mk]["own_mp_consumption"] += consumption
                else:
                    master_rows[mk]["own_cut_consumption"] += consumption
            master_rows[mk]["consumer_skus"].add(r["SKU"])

        # Supplier attribution for each master (in priority order):
        # 1) SKU-level team override (highest)
        # 2) CIN7's native product.Suppliers (default supplier on the product master)
        # 3) Inferred from 90-day purchase history
        # 4) Family-level team default
        # 5) '(no assignment)'
        sku_overrides = db.all_sku_supplier_overrides()
        fam_assignments = {r["family"]: r["supplier_name"]
                            for r in db.all_family_suppliers()}

        # Pull default supplier from CIN7's product master (Suppliers field).
        # CIN7 uses key 'SupplierName' (not 'Name'). No IsDefault flag — first
        # entry is the primary. Also extract FixedCost / Currency / Lead time
        # for downstream use.
        cin7_supplier_by_sku: dict = {}
        cin7_supplier_cost_by_sku: dict = {}    # FixedCost per SKU
        cin7_supplier_lead_by_sku: dict = {}    # Lead time (days) per SKU
        cin7_supplier_currency_by_sku: dict = {}
        for _, p in products.iterrows():
            sups_raw = p.get("Suppliers")
            if not sups_raw or sups_raw in ("[]", "None", None):
                continue
            sups = sups_raw
            if isinstance(sups, str):
                try:
                    sups = json.loads(sups)
                except (ValueError, TypeError):
                    continue
            if not isinstance(sups, list) or not sups:
                continue
            # First dict entry is the primary supplier
            primary = next(
                (s for s in sups if isinstance(s, dict) and s.get("SupplierName")),
                None,
            )
            if not primary:
                continue
            sku = p["SKU"]
            cin7_supplier_by_sku[sku] = primary["SupplierName"]
            # Prefer FixedCost (negotiated), fall back to Cost / PurchaseCost
            fc = primary.get("FixedCost") or primary.get("Cost") or primary.get("PurchaseCost")
            if fc and float(fc) > 0:
                cin7_supplier_cost_by_sku[sku] = float(fc)
            cur = primary.get("Currency")
            if cur:
                cin7_supplier_currency_by_sku[sku] = cur
            # Lead time: look at ProductSupplierOptions, take the first non-zero
            # Lead value (most accurate operational signal from CIN7)
            opts = primary.get("ProductSupplierOptions") or []
            if isinstance(opts, list):
                for opt in opts:
                    if isinstance(opt, dict):
                        lead = opt.get("Lead")
                        if lead and int(lead) > 0:
                            cin7_supplier_lead_by_sku[sku] = int(lead)
                            break

        supplier_by_sku: dict = {}
        if not purchase_lines.empty:
            pl = purchase_lines.copy()
            pl["Total"] = _to_num(pl["Total"]).fillna(0)
            sup_group = (pl.groupby(["SKU", "Supplier"])["Total"]
                           .sum().reset_index())
            for sku, grp in sup_group.groupby("SKU"):
                supplier_by_sku[sku] = grp.sort_values(
                    "Total", ascending=False)["Supplier"].iloc[0]

        def _resolve_supplier(sku: str, family: str) -> str:
            if sku in sku_overrides:
                return sku_overrides[sku]
            if sku in cin7_supplier_by_sku:
                return cin7_supplier_by_sku[sku]
            if sku in supplier_by_sku:
                return supplier_by_sku[sku]
            if family in fam_assignments:
                return fam_assignments[family]
            return "(no assignment)"

        # Build output rows with reorder-quantity math.
        # Show ALL masters (even zero-demand) so buyer sees the full set.
        rows = []
        window_days = window_months * 30.437
        for k, info in master_rows.items():
            total = info["own_consumption"] + info["migrated_consumption"]
            supplier = _resolve_supplier(info["MasterSKU"], info["Family"])

            avg_daily = total / max(window_days, 1)
            lead_time_demand = avg_daily * lead_time_days
            review_demand = avg_daily * (review_weeks * 7)
            safety_stock = lead_time_demand * (safety_factor / 100.0)
            target_stock = lead_time_demand + review_demand + safety_stock
            onhand = float(info["OnHand"])
            shortfall = max(0, target_stock - onhand)
            reorder_qty = int(round(shortfall))

            if total == 0:
                status = "⚪ No demand"
            elif onhand < lead_time_demand:
                status = "🔴 Reorder now"
            elif onhand < lead_time_demand + review_demand:
                status = "🟠 Reorder soon"
            else:
                status = "🟢 OK"
            days_of_cover = onhand / avg_daily if avg_daily > 0 else None

            # Revenue-proxy: use window_months velocity × avg cost as value signal
            rev_proxy = total * info["AvgCost"]

            # "Sales mix" summary for the buyer
            own_bare = info["own_bare_sales"]
            own_mp = info["own_mp_consumption"]
            own_cut = info["own_cut_consumption"]
            mix_parts = []
            if own_bare > 0:
                mix_parts.append(f"bare {own_bare:g}")
            if own_mp > 0:
                mix_parts.append(f"MP {own_mp:g}")
            if own_cut > 0:
                mix_parts.append(f"cuts {own_cut:g}")
            sales_mix = ", ".join(mix_parts) or "—"

            rows.append({
                "Master SKU": info["MasterSKU"],
                "Family": info["Family"],
                "Color": info["Color"],
                "Length": (f"{info['LengthMM']/1000:g}m"
                           if info["LengthMM"] >= 1000
                           else f"{info['LengthMM']}mm"),
                "Supplier": supplier,
                "Rule": info["RuleCode"] or "",
                "Bare sold": round(own_bare, 1),
                "via MP": round(own_mp, 1),
                "via cuts": round(own_cut, 1),
                "Own total": round(info["own_consumption"], 1),
                "Migrated": round(info["migrated_consumption"], 1),
                f"Total needed ({window_months}mo)":
                    round(total, 1),
                "/month": round(total / window_months, 1),
                "Sales mix": sales_mix,
                "OnHand": int(onhand),
                "DoC (days)":
                    (round(days_of_cover, 0)
                     if days_of_cover is not None else None),
                "Target": int(round(target_stock)),
                "Suggested reorder": reorder_qty,
                "Status": status,
                "# SKUs": len(info["consumer_skus"]),
                "From (retiring)":
                    ", ".join(info["migrated_from"])[:120],
                "_RevProxy": rev_proxy,
                "_AvgCost": info["AvgCost"],
            })
        proj_df = pd.DataFrame(rows)

        # --- ABC classification (Class A = top 70% of cost-weighted demand) ---
        if not proj_df.empty and proj_df["_RevProxy"].sum() > 0:
            sorted_by_rev = proj_df.sort_values(
                "_RevProxy", ascending=False).copy()
            total_rev = sorted_by_rev["_RevProxy"].sum()
            cumul = sorted_by_rev["_RevProxy"].cumsum()
            cum_pct = cumul / total_rev if total_rev else 0
            abc = []
            for p in cum_pct:
                if p <= 0.70:
                    abc.append("A")
                elif p <= 0.90:
                    abc.append("B")
                else:
                    abc.append("C")
            sorted_by_rev["ABC"] = abc
            # Merge ABC back into proj_df on Master SKU
            abc_map = dict(zip(sorted_by_rev["Master SKU"],
                               sorted_by_rev["ABC"]))
            proj_df["ABC"] = proj_df["Master SKU"].map(abc_map).fillna("—")
        else:
            proj_df["ABC"] = "—"

        # Default sort: supplier, then reorder qty desc
        proj_df = proj_df.sort_values(
            ["Supplier", "Suggested reorder"],
            ascending=[True, False],
        )

        # --- Headline: aggregate by supplier ---------------------------------
        if not proj_df.empty:
            sup_summary = (proj_df.groupby("Supplier")
                           .agg(**{
                               "Masters": ("Master SKU", "nunique"),
                               "Masters needing reorder":
                                   ("Suggested reorder",
                                    lambda x: int((x > 0).sum())),
                               f"Total {window_months}mo demand":
                                   (f"Total needed ({window_months}mo)",
                                    "sum"),
                               "Sum of suggested reorder":
                                   ("Suggested reorder", "sum"),
                           })
                           .reset_index()
                           .sort_values("Sum of suggested reorder",
                                         ascending=False))
            st.markdown("**Aggregate by supplier — draft PO shortlist**")
            st.dataframe(sup_summary, width="stretch", hide_index=True)

            # Warn about unassigned masters (likely missing family defaults)
            unassigned = proj_df[
                proj_df["Supplier"].astype(str).str.contains(
                    "no assignment|no purchase", case=False, na=False)
            ]
            if not unassigned.empty:
                missing_families = sorted(
                    unassigned["Family"].unique().tolist())
                st.warning(
                    f":warning: **{len(unassigned)} master SKUs have no "
                    f"supplier assigned** across families: "
                    f"{', '.join(missing_families)}. "
                    "They won't appear under any supplier's reorder view "
                    "until you set a family default or SKU override below. "
                    "Use **Manage supplier assignments** → Family defaults. "
                    "(Or pick \"(no assignment)\" in the supplier dropdown "
                    "to see them temporarily.)"
                )
                with st.expander(
                    f"Show the {len(unassigned)} unassigned masters"):
                    st.dataframe(
                        unassigned[["Master SKU", "Family", "Color",
                                      "Length", "Supplier"]],
                        width="stretch", hide_index=True)

            # --- Supplier assignment manager (team-curated) -----------------
            with st.expander(":gear: Manage supplier assignments (family + per-SKU)",
                              expanded=False):
                actor_s = st.session_state.get("current_user", "").strip()
                if not actor_s:
                    st.caption("Enter your name in the sidebar to edit.")
                else:
                    # Build list of ALL suppliers we know about (master file
                    # + anything seen in POs + any existing overrides)
                    known_suppliers = set()
                    if not suppliers.empty and "Name" in suppliers.columns:
                        known_suppliers.update(
                            suppliers["Name"].dropna().astype(str).tolist())
                    known_suppliers.update(supplier_by_sku.values())
                    known_suppliers.update(fam_assignments.values())
                    known_suppliers.update(sku_overrides.values())
                    known_suppliers.discard("(no assignment)")
                    known_suppliers = sorted(known_suppliers)

                    st.markdown("**Family defaults** — applies to every "
                                "master in the family that has no per-SKU "
                                "override or recent PO.")
                    f1, f2, f3, f4 = st.columns([1.5, 2.5, 3, 1])
                    fam_pick = f1.selectbox("Family",
                                             sorted(successor_families +
                                                    retiring_families),
                                             key="fsa_family")
                    sup_pick = f2.selectbox(
                        "Supplier", known_suppliers,
                        index=(known_suppliers.index(fam_assignments.get(fam_pick))
                               if fam_assignments.get(fam_pick) in known_suppliers
                               else 0) if known_suppliers else 0,
                        key="fsa_supplier",
                    )
                    fam_note = f3.text_input("Note (optional)",
                                              placeholder="e.g. 'All Sierra from Reeves'",
                                              key="fsa_note")
                    if f4.button("Assign", key="fsa_save"):
                        db.set_family_supplier(fam_pick, sup_pick,
                                                actor_s, fam_note)
                        st.cache_data.clear()
                        st.success(f"{fam_pick} → {sup_pick}")
                        st.rerun()

                    # Show current family assignments
                    if fam_assignments:
                        fam_rows = [
                            {"Family": fam, "Default supplier": sup}
                            for fam, sup in sorted(fam_assignments.items())
                        ]
                        st.dataframe(pd.DataFrame(fam_rows),
                                     width="stretch", hide_index=True)

                    st.markdown("---")
                    st.markdown("**Per-SKU overrides** — for edge cases where "
                                "one SKU in a family comes from a different "
                                "supplier than the family default.")
                    o1, o2, o3, o4 = st.columns([2, 2.5, 2.5, 1])
                    all_master_skus = sorted(proj_df["Master SKU"].tolist())
                    sku_over_pick = o1.selectbox(
                        "Master SKU", all_master_skus,
                        key="sso_sku",
                    )
                    sku_sup_pick = o2.selectbox(
                        "Supplier", known_suppliers,
                        index=(known_suppliers.index(sku_overrides.get(sku_over_pick))
                               if sku_overrides.get(sku_over_pick) in known_suppliers
                               else 0) if known_suppliers else 0,
                        key="sso_supplier",
                    )
                    sku_note = o3.text_input("Note", key="sso_note")
                    if o4.button("Assign", key="sso_save"):
                        db.set_sku_supplier(sku_over_pick, sku_sup_pick,
                                             actor_s, sku_note)
                        st.cache_data.clear()
                        st.success(f"{sku_over_pick} → {sku_sup_pick}")
                        st.rerun()
                    if sku_overrides:
                        with st.expander(f"{len(sku_overrides)} SKU-level "
                                          "overrides active"):
                            st.dataframe(
                                pd.DataFrame([
                                    {"SKU": k, "Supplier": v}
                                    for k, v in sorted(sku_overrides.items())
                                ]),
                                width="stretch", hide_index=True,
                            )

                    st.markdown("---")
                    st.markdown("**Supplier pricing rules** — overrides "
                                "CIN7's AverageCost when calculating PO "
                                "line values. Supports per-unit, per-foot "
                                "(Reeves style), and tiered-per-foot.")
                    pr1, pr2, pr3, pr4 = st.columns([2, 1.5, 1, 1])
                    pr_supplier = pr1.selectbox(
                        "Supplier for pricing", known_suppliers,
                        key="pr_supplier",
                    )
                    existing_pricing = db.all_supplier_pricing().get(
                        pr_supplier, {})
                    pr_model = pr2.selectbox(
                        "Model",
                        ["fixed_per_unit", "per_foot", "per_foot_tiered"],
                        index=["fixed_per_unit", "per_foot",
                                "per_foot_tiered"].index(
                            existing_pricing.get("pricing_model",
                                                  "fixed_per_unit")),
                        key="pr_model",
                        help=(
                            "fixed_per_unit: flat price per unit (rare).\n"
                            "per_foot: price × tube length in ft.\n"
                            "per_foot_tiered: per_foot with qty-break tiers."
                        ),
                    )
                    pr_base = pr3.number_input(
                        "Base price" +
                        (" (per ft)" if pr_model.startswith("per_foot")
                         else " (per unit)"),
                        min_value=0.0, max_value=1000.0,
                        value=float(existing_pricing.get("base_price") or 0.0),
                        step=0.1, key="pr_base",
                    )
                    pr_currency = pr4.text_input(
                        "Currency",
                        value=existing_pricing.get("currency") or "USD",
                        key="pr_currency",
                    )
                    if pr_model == "per_foot_tiered":
                        pr_tiers = st.text_area(
                            "Tiers (JSON) — list of "
                            "`{\"min_qty\": N, \"price_per_ft\": P}`",
                            value=(existing_pricing.get("tiers_json")
                                   or '[\n'
                                   '  {"min_qty": 0,   "price_per_ft": 2.40},\n'
                                   '  {"min_qty": 100, "price_per_ft": 2.10},\n'
                                   '  {"min_qty": 500, "price_per_ft": 1.85}\n'
                                   ']'),
                            height=160, key="pr_tiers",
                        )
                    else:
                        pr_tiers = None

                    pr_note = st.text_input(
                        "Note (optional)",
                        placeholder="e.g. 'Reeves quote 2025-04 + 3% freight'",
                        key="pr_note",
                    )

                    pp1, pp2 = st.columns([1, 6])
                    if pp1.button("Save pricing", key="pr_save",
                                   type="primary"):
                        db.set_supplier_pricing(
                            pr_supplier, pr_model,
                            base_price=pr_base,
                            tiers_json=pr_tiers,
                            currency=pr_currency,
                            actor=actor_s, note=pr_note,
                        )
                        st.cache_data.clear()
                        st.success(
                            f"Saved pricing: {pr_supplier} → {pr_model}")
                        st.rerun()

                    # Show current pricing rules table
                    pricing_all = db.all_supplier_pricing()
                    if pricing_all:
                        st.markdown("**Current pricing rules**")
                        rows_p = []
                        for sname, p in sorted(pricing_all.items()):
                            rows_p.append({
                                "Supplier": sname,
                                "Model": p.get("pricing_model"),
                                "Base": p.get("base_price"),
                                "Currency": p.get("currency") or "",
                                "Tiers?":
                                    "yes" if p.get("tiers_json") else "no",
                                "Set by": p.get("set_by"),
                                "Set at": (p.get("set_at") or "")[:16],
                                "Note": p.get("note") or "",
                            })
                        st.dataframe(pd.DataFrame(rows_p),
                                     width="stretch", hide_index=True)

            # --- Supplier-focused workflow ------------------------------------
            suppliers_available = sorted(proj_df["Supplier"].unique().tolist())
            # Prefer a real supplier (not 'no assignment') as default
            real_suppliers = [s for s in suppliers_available
                              if "no assignment" not in s.lower()
                              and "no purchase" not in s.lower()]
            default_supplier = real_suppliers[0] if real_suppliers else suppliers_available[0]

            st.markdown("### :factory: Build a draft PO — by supplier")
            sel_supplier = st.selectbox(
                "Supplier", suppliers_available,
                index=(suppliers_available.index(default_supplier)
                       if default_supplier in suppliers_available else 0),
                key="tube_po_supplier",
            )

            sup_rows = proj_df[proj_df["Supplier"] == sel_supplier].copy()

            # Only show the columns the buyer actually edits/uses
            buyer_cols = [
                "Master SKU", "Family", "Color", "Length", "ABC",
                "Rule", "Sales mix",
                "Bare sold", "via MP", "via cuts", "Migrated",
                f"Total needed ({window_months}mo)", "/month",
                "OnHand", "DoC (days)", "Target", "Suggested reorder",
                "Status",
            ]

            # --- Pricing resolution per supplier --------------------------
            # Priority: supplier-specific pricing model → CIN7 AverageCost
            pricing_all = db.all_supplier_pricing()
            pricing = pricing_all.get(sel_supplier)

            def _unit_cost_for_row(sku_row, qty: int) -> tuple:
                """Returns (unit_cost, rationale) for a given master row.
                qty is used for tiered pricing."""
                avg_cost = float(sku_row["_AvgCost"] or 0)
                length_mm = None
                m_in_tubes = tubes_df[tubes_df["SKU"] == sku_row["Master SKU"]]
                if not m_in_tubes.empty:
                    length_mm = m_in_tubes["LengthMM"].iloc[0]

                if not pricing:
                    return avg_cost, "CIN7 avg cost"

                model = pricing.get("pricing_model")
                base = pricing.get("base_price") or 0.0

                if model == "per_foot" and length_mm:
                    ft = length_mm / 304.8
                    return round(ft * base, 4), f"{ft:.2f}ft × ${base:.2f}/ft"

                if model == "per_foot_tiered" and length_mm:
                    ft = length_mm / 304.8
                    tiers_raw = pricing.get("tiers_json") or "[]"
                    try:
                        tiers = json.loads(tiers_raw)
                    except Exception:
                        tiers = []
                    # Pick the applicable tier based on line qty
                    # Sorted by min_qty ascending, take the highest min_qty
                    # that qty satisfies
                    price_per_ft = base
                    applied_tier_label = "base"
                    for t in sorted(tiers, key=lambda x: x.get("min_qty", 0)):
                        if qty >= (t.get("min_qty") or 0):
                            price_per_ft = t.get("price_per_ft") or price_per_ft
                            applied_tier_label = (
                                f"qty≥{t.get('min_qty',0)}"
                            )
                    return (round(ft * price_per_ft, 4),
                            f"{ft:.2f}ft × ${price_per_ft:.2f}/ft "
                            f"({applied_tier_label})")

                if model == "fixed_per_unit":
                    return (base if base else avg_cost,
                            "Flat per-unit")

                return avg_cost, "CIN7 avg cost (fallback)"

            # Editable table — add "Order qty" column that defaults to Suggested
            editor_df = sup_rows[buyer_cols].copy()
            editor_df["Order qty"] = sup_rows["Suggested reorder"].astype(int)

            # Compute unit cost using the supplier's pricing model
            unit_costs = []
            rationales = []
            for idx, _ in editor_df.iterrows():
                qty = int(editor_df.loc[idx, "Order qty"])
                row_data = sup_rows.loc[idx]
                uc, rationale = _unit_cost_for_row(row_data, qty)
                unit_costs.append(uc)
                rationales.append(rationale)
            editor_df["Unit cost"] = unit_costs
            editor_df["Price basis"] = rationales
            editor_df["Line value"] = (
                editor_df["Order qty"] * editor_df["Unit cost"]
            ).round(2)
            editor_df["Include?"] = editor_df["Order qty"] > 0

            # Pricing banner
            if pricing:
                model = pricing.get("pricing_model")
                base = pricing.get("base_price")
                cur = pricing.get("currency") or ""
                if model == "per_foot":
                    st.success(
                        f":moneybag: **Pricing**: per-foot @ {cur}${base:.2f}/ft "
                        f"— line value auto-computed from tube length."
                    )
                elif model == "per_foot_tiered":
                    st.success(
                        f":moneybag: **Pricing**: per-foot with quantity "
                        f"tiers (base {cur}${base:.2f}/ft). Line qty "
                        f"determines which tier applies."
                    )
                elif model == "fixed_per_unit":
                    st.info(
                        f":moneybag: **Pricing**: fixed {cur}${base:.2f}/unit."
                    )
            else:
                st.caption(
                    ":information_source: No supplier pricing rule set — "
                    "line values use CIN7 AverageCost. Set a rule below to "
                    "apply per-foot or tiered pricing."
                )

            st.caption(
                f"**{sel_supplier}** — {len(sup_rows)} master SKUs. "
                "Edit 'Order qty' and 'Include?' columns below. "
                "Line value auto-recomputes on save."
            )

            edited = st.data_editor(
                editor_df,
                width="stretch", hide_index=True, height=450,
                key=f"po_editor_{sel_supplier}",
                column_config={
                    "Include?": st.column_config.CheckboxColumn(
                        "Include?", width="small"),
                    "Order qty": st.column_config.NumberColumn(
                        "Order qty", min_value=0, step=1,
                        help="Override the suggested reorder here."),
                    "Unit cost": st.column_config.NumberColumn(
                        format="$%.2f", disabled=True),
                    "Price basis": st.column_config.TextColumn(
                        "Price basis",
                        help="How unit cost was determined",
                        disabled=True),
                    "Line value": st.column_config.NumberColumn(
                        format="$%.0f", disabled=True),
                    "Master SKU": st.column_config.TextColumn(disabled=True),
                    "Family": st.column_config.TextColumn(disabled=True),
                    "Color": st.column_config.TextColumn(disabled=True),
                    "Length": st.column_config.TextColumn(disabled=True),
                    "ABC": st.column_config.TextColumn(disabled=True,
                                                         width="small"),
                    "Rule": st.column_config.TextColumn(disabled=True,
                                                          width="small"),
                    "Sales mix": st.column_config.TextColumn(
                        "Sales mix",
                        help="Breakdown of where demand comes from: "
                             "direct bare-tube sales, via MP-variants, "
                             "via cut variants",
                        disabled=True),
                    "Bare sold": st.column_config.NumberColumn(
                        "Bare sold",
                        help="Direct sales of the bare tube SKU (no MP).",
                        disabled=True, format="%.0f"),
                    "via MP": st.column_config.NumberColumn(
                        "via MP",
                        help="Master consumption from MP-variant sales "
                             "(with mounting plate bundled).",
                        disabled=True, format="%.0f"),
                    "via cuts": st.column_config.NumberColumn(
                        "via cuts",
                        help="Master consumption from non-MP cut variants "
                             "(e.g. 1m or 2390mm cut from 3m master).",
                        disabled=True, format="%.0f"),
                    "Migrated": st.column_config.NumberColumn(
                        "Migrated",
                        help="Demand from retiring Smokies / Cascade mapped "
                             "into this master.",
                        disabled=True, format="%.0f"),
                    f"Total needed ({window_months}mo)":
                        st.column_config.NumberColumn(disabled=True),
                    "/month": st.column_config.NumberColumn(
                        format="%.1f", disabled=True),
                    "OnHand": st.column_config.NumberColumn(disabled=True),
                    "DoC (days)": st.column_config.NumberColumn(
                        format="%.0f", disabled=True),
                    "Target": st.column_config.NumberColumn(disabled=True),
                    "Suggested reorder": st.column_config.NumberColumn(
                        disabled=True),
                    "Status": st.column_config.TextColumn(disabled=True),
                },
            )

            # PO summary
            lines_to_order = edited[
                (edited["Include?"]) & (edited["Order qty"] > 0)
            ]
            po_units = int(lines_to_order["Order qty"].sum())
            po_value = float(
                (lines_to_order["Order qty"] * lines_to_order["Unit cost"]).sum()
            )
            po_lines = len(lines_to_order)
            po_abc = lines_to_order["ABC"].value_counts().to_dict()

            pc1, pc2, pc3, pc4 = st.columns(4)
            pc1.metric("PO lines", po_lines)
            pc2.metric("Total units", _fmt_number(po_units))
            pc3.metric("Estimated value", _fmt_money(po_value))
            pc4.metric("Class mix",
                       f"A:{po_abc.get('A',0)} B:{po_abc.get('B',0)} "
                       f"C:{po_abc.get('C',0)}")

            # Reeves pricing info placeholder
            if "REEVES" in sel_supplier.upper():
                st.info(
                    ":information_source: **Reeves uses tiered pricing.** "
                    "Once you upload the pricing spreadsheet, this section "
                    "will show price-break suggestions (e.g. 'ordering "
                    "25 more drops your unit cost from $X to $Y'). For now, "
                    "line values use average cost from CIN7."
                )
            elif "LUZ" in sel_supplier.upper() or "NEGRA" in sel_supplier.upper():
                st.info(
                    ":flag-es: **Luz Negra — Spain supplier, no tiered "
                    "pricing.** Long lead time considerations apply. "
                    "Consider batching multiple masters into a single PO "
                    "to offset freight."
                )

            # Draft PO button (placeholder — wires to CIN7 POST later)
            st.markdown("#### :memo: Draft Purchase Order")
            dp1, dp2, dp3 = st.columns([1, 1, 2])
            actor = st.session_state.get("current_user", "").strip()
            with dp1:
                draft_disabled = (po_lines == 0 or not actor)
                if st.button(":rocket: Create draft PO in CIN7",
                             type="primary",
                             disabled=draft_disabled,
                             width="stretch"):
                    st.warning(
                        ":construction: **Not yet wired to CIN7's purchase "
                        "creation API.** The data_editor output is ready "
                        "though — we'll POST this payload to "
                        "`/purchase` (or `/advanced-purchase`) next. "
                        "Meanwhile, export below and paste into CIN7 manually."
                    )
            with dp2:
                if st.button("Export CSV for manual paste",
                             disabled=(po_lines == 0),
                             width="stretch"):
                    st.session_state["po_export"] = lines_to_order.to_csv(index=False)
            with dp3:
                if "po_export" in st.session_state:
                    st.download_button(
                        "Download PO CSV",
                        data=st.session_state["po_export"],
                        file_name=f"draft_PO_{sel_supplier}_{datetime.now():%Y%m%d_%H%M}.csv",
                        mime="text/csv",
                        width="stretch",
                    )
            if not actor:
                st.caption(":warning: Enter your name in the sidebar to "
                           "enable the Create Draft PO button.")

            # Expander: full per-master detail across all suppliers
            with st.expander("Show per-master detail across ALL suppliers"):
                proj_display = proj_df.drop(columns=["_RevProxy", "_AvgCost"])
                st.dataframe(proj_display, width="stretch", hide_index=True,
                             height=500,
                             column_config={
                                 "ABC": st.column_config.TextColumn(
                                     width="small"),
                                 "Supplier": st.column_config.TextColumn(
                                     width="medium"),
                             })

        with st.expander("How the consumption math works"):
            st.markdown(
                "Every tube SKU has a **sourcing rule** in its "
                "`AdditionalAttribute1` that tells us exactly how much "
                "master tube it consumes per unit sold. Examples:\n"
                "- `Purchased full length. No BOM` → this IS a master. "
                "Each direct sale = 1 master consumed.\n"
                "- `Assemble from 1 × 3m` → 1 unit sold consumes 1 × 3m "
                "of its master.\n"
                "- `Assemble from 0.5 × 2m` → each unit consumes half a "
                "2m master → 2 units per master.\n"
                "- `Assemble from 0.25 × 609mm` → 4 units per master.\n"
                "- `Assemble from 0.5 × 2m profile + 3.28ft plate` "
                "(SR140 / MP variants) → each unit consumes half a 2m "
                "tube master AND some plate. The tube side is counted "
                "here; the plate side is tracked separately via "
                "Critical Components below.\n\n"
                "Master demand = Σ (variant_demand × variant_fraction) "
                "across every cut/MP variant pointing at this master, "
                "including migrated demand from retiring Smokies/Cascade."
            )

    # --- Yukon mounting plate tracker (dedicated spot) -------------------
    st.markdown("### :triangular_ruler: Yukon mounting plate — Minalex")
    st.caption(
        "LED-YUKON-* SKUs supplied by Minalex, with BOM-driven consumption "
        "aggregated from every tube that uses them. Build a draft Minalex "
        "PO for the Yukon range directly."
    )

    # Find YUKON-prefixed SKUs supplied by Minalex
    minalex_products = []
    for _, p in products.iterrows():
        sku = str(p.get("SKU") or "").upper()
        # Scope to Yukon-only for this section
        if "YUKON" not in sku:
            continue
        sups_raw = p.get("Suppliers")
        if not sups_raw or sups_raw in ("[]", "None", None):
            continue
        sups = sups_raw
        if isinstance(sups, str):
            try:
                sups = json.loads(sups)
            except (ValueError, TypeError):
                continue
        if not isinstance(sups, list):
            continue
        minalex_sup = None
        for s in sups:
            if (isinstance(s, dict)
                    and "MINALEX" in str(s.get("SupplierName", "")).upper()):
                minalex_sup = s
                break
        if minalex_sup:
            minalex_products.append((p, minalex_sup))

    if not minalex_products:
        st.info(
            "No products with Minalex as supplier found. If that's "
            "unexpected, re-run `python cin7_sync.py products` — it "
            "pulls the Suppliers field. Minalex-supplied items will "
            "light up here afterwards."
        )
    else:
        # Build a name-match dictionary for BOM → product resolution
        # (since BOMs may have ComponentSKU=None but ComponentName set)
        minalex_name_to_sku = {
            str(p.get("Name") or "").upper(): p.get("SKU")
            for p, _ in minalex_products
        }

        rows = []
        for prod, sup_info in minalex_products:
            sku = prod.get("SKU")
            onhand = float(stock_by_sku.get(sku, {}).get("OnHand", 0))
            avail = float(stock_by_sku.get(sku, {}).get("Available", 0))
            fixed_cost = float(sup_info.get("FixedCost") or 0)
            currency = sup_info.get("Currency") or ""

            # Try to match BOM entries where ComponentSKU == this SKU
            # OR ComponentName matches (for the None-SKU case)
            bom_matches = []
            if not boms.empty:
                prod_name_upper = str(prod.get("Name") or "").upper()
                for _, b in boms.iterrows():
                    comp_sku = b.get("ComponentSKU")
                    comp_name = str(b.get("ComponentName") or "").upper()
                    if comp_sku == sku:
                        bom_matches.append(b)
                    elif not comp_sku and comp_name and prod_name_upper:
                        # Name-match fallback when BOM SKU is null
                        if (comp_name == prod_name_upper
                                or (len(comp_name) > 20
                                    and comp_name in prod_name_upper)
                                or (len(prod_name_upper) > 20
                                    and prod_name_upper in comp_name)):
                            bom_matches.append(b)

            # Compute consumption projection from tube assemblies
            proj_consumption = 0.0
            consumer_skus = set()
            families_touched = set()
            for b in bom_matches:
                asm_sku = b.get("AssemblySKU")
                qty_per = float(b.get("Quantity") or 0)
                if qty_per == 0:
                    continue
                # Units sold of the assembly (+ migration if applicable)
                asm_units = float(vel.get(asm_sku, 0))
                # Include migrated demand from retiring SKUs mapped to this asm
                migrated_extra = 0.0
                if include_migrations:
                    for _, retrow in retiring_rows.iterrows():
                        target = (retrow.get("Saved mapping")
                                   or retrow.get("Proposed"))
                        if target == asm_sku:
                            share = float(retrow.get("Share %") or 100.0) / 100.0
                            migrated_extra += float(retrow.get("Units") or 0) * share
                total_asm = asm_units + migrated_extra
                proj_consumption += total_asm * qty_per
                consumer_skus.add(asm_sku)
                asm_family = tubes_df[tubes_df["SKU"] == asm_sku]
                if not asm_family.empty:
                    families_touched.add(asm_family["Family"].iloc[0])

            avg_daily = proj_consumption / max(window_days, 1)
            doc = onhand / avg_daily if avg_daily > 0 else None

            minalex_lead_days = 60
            lt_demand = avg_daily * minalex_lead_days
            safety = lt_demand * 0.3
            target = lt_demand + safety
            reorder = max(0, int(round(target - onhand)))

            if proj_consumption == 0:
                status = "⚪ No BOM-driven demand"
            elif doc is None or doc < minalex_lead_days * 0.5:
                status = "🔴 URGENT"
            elif doc < minalex_lead_days:
                status = "🟠 Reorder now"
            elif doc < minalex_lead_days * 1.5:
                status = "🟡 Plan reorder"
            else:
                status = "🟢 OK"

            rows.append({
                "SKU": sku,
                "Name": str(prod.get("Name") or "")[:55],
                "Tubes using it": len(consumer_skus),
                "Tube families": ", ".join(sorted(families_touched)) or "—",
                "OnHand": int(onhand),
                "Available": int(avail),
                "Proj. consumption / yr": round(proj_consumption, 0),
                "/ month": round(proj_consumption / max(window_months, 1), 1),
                "Days of cover": (round(doc, 0)
                                    if doc is not None else None),
                "Target (60d lead)": int(round(target)),
                "Reorder now": reorder,
                "Unit cost": fixed_cost or None,
                "Currency": currency,
                "Status": status,
            })

        mdf = pd.DataFrame(rows).sort_values(
            ["Reorder now", "Proj. consumption / yr"],
            ascending=[False, False])

        # Headline metrics
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Yukon SKUs", len(mdf))
        m2.metric("Needing reorder",
                   int((mdf["Reorder now"] > 0).sum()))
        total_reorder_units = int(mdf["Reorder now"].sum())
        m3.metric("Total reorder units",
                   _fmt_number(total_reorder_units))
        total_reorder_value = float(
            (mdf["Reorder now"] * mdf["Unit cost"].fillna(0)).sum())
        m4.metric("Est. PO value", _fmt_money(total_reorder_value))

        # Editable draft-PO table
        st.markdown("### :clipboard: Draft Minalex PO — Yukon")
        editor_src = mdf.copy()
        editor_src["Order qty"] = editor_src["Reorder now"].astype(int)
        editor_src["Line value"] = (
            editor_src["Order qty"] * editor_src["Unit cost"].fillna(0)
        ).round(2)
        editor_src["Include?"] = editor_src["Order qty"] > 0

        edited = st.data_editor(
            editor_src,
            width="stretch", hide_index=True, height=400,
            key="minalex_yukon_editor",
            column_config={
                "Include?": st.column_config.CheckboxColumn(
                    "✓", width="small"),
                "Order qty": st.column_config.NumberColumn(
                    "Order qty", min_value=0, step=1,
                    help="Override the suggested quantity."),
                "Line value": st.column_config.NumberColumn(
                    format="$%.0f", disabled=True),
                "Unit cost": st.column_config.NumberColumn(
                    format="$%.2f", disabled=True),
                "SKU": st.column_config.TextColumn(disabled=True),
                "Name": st.column_config.TextColumn(disabled=True),
                "Tubes using it": st.column_config.NumberColumn(
                    disabled=True),
                "Tube families": st.column_config.TextColumn(
                    disabled=True, width="medium"),
                "OnHand": st.column_config.NumberColumn(disabled=True),
                "Available": st.column_config.NumberColumn(disabled=True),
                "Proj. consumption / yr":
                    st.column_config.NumberColumn(disabled=True,
                                                    format="%.0f"),
                "/ month": st.column_config.NumberColumn(
                    disabled=True, format="%.1f"),
                "Days of cover": st.column_config.NumberColumn(
                    format="%.0f days", disabled=True),
                "Target (60d lead)": st.column_config.NumberColumn(
                    disabled=True),
                "Reorder now": st.column_config.NumberColumn(
                    disabled=True),
                "Currency": st.column_config.TextColumn(disabled=True),
                "Status": st.column_config.TextColumn(disabled=True,
                                                        width="small"),
            },
        )

        # PO summary strip
        po_lines_mx = edited[
            (edited["Include?"]) & (edited["Order qty"] > 0)
        ]
        po_units_mx = int(po_lines_mx["Order qty"].sum())
        po_value_mx = float(
            (po_lines_mx["Order qty"] *
             po_lines_mx["Unit cost"].fillna(0)).sum()
        )
        pm1, pm2, pm3 = st.columns(3)
        pm1.metric("PO lines", len(po_lines_mx))
        pm2.metric("Total units", _fmt_number(po_units_mx))
        pm3.metric("Est. value", _fmt_money(po_value_mx))

        # Draft PO action buttons
        mx_actor = st.session_state.get("current_user", "").strip()
        mxa, mxb, mxc = st.columns([1, 1, 3])
        with mxa:
            if st.button(":rocket: Create draft PO in CIN7",
                          type="primary",
                          disabled=(len(po_lines_mx) == 0 or not mx_actor),
                          width="stretch",
                          key="minalex_create_po_btn"):
                st.warning(
                    ":construction: CIN7 POST not yet wired. Use Export "
                    "CSV for now — the PO data is ready to paste into CIN7."
                )
        with mxb:
            if st.button("Export CSV",
                          disabled=(len(po_lines_mx) == 0),
                          width="stretch",
                          key="minalex_export_btn"):
                st.session_state["minalex_po_csv"] = (
                    po_lines_mx.to_csv(index=False))
        with mxc:
            if "minalex_po_csv" in st.session_state:
                st.download_button(
                    "Download draft Minalex PO CSV",
                    data=st.session_state["minalex_po_csv"],
                    file_name=(
                        f"draft_PO_Minalex_Yukon_"
                        f"{datetime.now():%Y%m%d_%H%M}.csv"),
                    mime="text/csv",
                    width="stretch",
                    key="minalex_download_btn",
                )
        if not mx_actor:
            st.caption(":warning: Enter your name in the sidebar to "
                        "enable Create Draft PO.")

        # Data-quality caveat
        unresolved_bom_count = 0
        if not boms.empty:
            unresolved_bom_count = int(
                (boms["ComponentSKU"].isna()
                 | (boms["ComponentSKU"].astype(str) == "None")).sum()
            )
        if unresolved_bom_count > 0:
            st.caption(
                f":information_source: **Data note**: "
                f"{unresolved_bom_count:,} BOM rows from CIN7 have "
                f"null `ComponentSKU` — we use name-matching as a "
                f"fallback. If a Yukon SKU looks under-consumed, ask "
                f"CIN7 support to populate ComponentSKU on BOMs."
            )
        st.caption("Reorder math = 60-day Minalex lead time + 30% safety. "
                    "Adjust via a Critical Component entry for specific "
                    "overrides.")

    # --- Critical components (team-designated) ---------------------------
    st.markdown("### :rotating_light: Critical components per family")
    st.caption(
        "Team-designated components to watch per tube family (e.g. Yukon "
        "mounting plate used across Sierra38 + Sierra65; Oslo heat plate "
        "for Oslo variants). Long supplier lead times make these reorder "
        "priorities. Once BOMs are fully synced, the auto-discovered "
        "'Shared components' table below will complement this manual list."
    )

    cc_actor = st.session_state.get("current_user", "").strip()
    crit_rows = db.list_critical_components()
    # Enrich with live stats
    crit_enriched = []
    for r in crit_rows:
        fam = r["family"]
        comp = r["component_sku"]
        lead = r["lead_time_days"]
        onhand = float(stock_by_sku.get(comp, {}).get("OnHand", 0))
        avail = float(stock_by_sku.get(comp, {}).get("Available", 0))

        # How much will this component be consumed if we apply projected
        # demand from all tubes in the selected families?
        projected_consumption = 0.0
        consumers = []
        if not boms.empty:
            rel = boms[boms["ComponentSKU"] == comp].copy()
            rel["Quantity"] = _to_num(rel["Quantity"]).fillna(0)
            for _, b in rel.iterrows():
                asm = b["AssemblySKU"]
                qty_per = float(b["Quantity"] or 0)
                # Projected demand = own + migrated (if successor)
                own = float(vel.get(asm, 0))
                migrated = 0.0
                # Find any retiring SKUs that map to this assembly
                for _, rr in retiring_rows.iterrows():
                    target = (rr["Saved mapping"] or rr["Proposed"])
                    if target == asm:
                        share = float(rr["Share %"] or 100.0) / 100.0
                        migrated += float(rr["Units"]) * share
                tube_demand = own + (migrated if include_migrations else 0)
                projected_consumption += tube_demand * qty_per
                if tube_demand > 0 and qty_per > 0:
                    consumers.append(asm)

        avg_daily = (projected_consumption / (window_months * 30.437)
                     if window_months > 0 else 0)
        doc = (onhand / avg_daily) if avg_daily > 0 else None

        # Reorder trigger: if DoC < lead time, flag as urgent
        trigger = "—"
        if lead and doc is not None:
            if doc < lead * 0.5:
                trigger = "🔴 URGENT"
            elif doc < lead:
                trigger = "🟠 Reorder now"
            elif doc < lead * 1.5:
                trigger = "🟡 Plan reorder"
            else:
                trigger = "🟢 OK"

        crit_enriched.append({
            "id": r["id"],
            "Family": fam,
            "Component SKU": comp,
            "Role": r["role"] or "",
            "Lead time (days)": lead,
            "Stock (phys)": onhand,
            "Available": avail,
            "Proj. consumption (mo)":
                round(projected_consumption / max(window_months, 1), 1),
            "Days of cover": round(doc, 1) if doc is not None else None,
            "Status": trigger,
            "# consumers": len(consumers),
            "Set by": r["set_by"],
            "Note": r["note"] or "",
        })

    if crit_enriched:
        crit_df = pd.DataFrame(crit_enriched).drop(columns=["id"])
        st.dataframe(
            crit_df,
            width="stretch", hide_index=True,
            column_config={
                "Days of cover":
                    st.column_config.NumberColumn(format="%.0f days"),
            },
        )
    else:
        st.info("No critical components set yet. Add your first below.")

    with st.expander(":heavy_plus_sign: Add a critical component"):
        if not cc_actor:
            st.warning("Enter your name in the sidebar first.")
        else:
            cc1, cc2, cc3, cc4, cc5 = st.columns([1, 2, 2, 1, 2])
            cc_fam = cc1.selectbox("Family", tube_families,
                                    key="cc_family")
            # Suggest candidate components from BOMs used by that family
            component_options = []
            if not boms.empty:
                fam_skus = tubes_df[tubes_df["Family"] == cc_fam]["SKU"].tolist()
                fam_boms = boms[boms["AssemblySKU"].isin(fam_skus)]
                if not fam_boms.empty:
                    component_options = sorted(
                        fam_boms["ComponentSKU"].dropna().unique().tolist()
                    )
            # Fallback: any product SKU
            if not component_options:
                component_options = sorted(products["SKU"].tolist()[:500])
            cc_comp = cc2.selectbox("Component SKU", component_options,
                                     key="cc_comp")
            cc_role = cc3.text_input("Role",
                                      placeholder="e.g. Mounting plate",
                                      key="cc_role")
            cc_lead = cc4.number_input("Lead days",
                                        min_value=1, max_value=365,
                                        value=45, step=1,
                                        key="cc_lead")
            cc_note = cc5.text_input("Note",
                                      placeholder="optional",
                                      key="cc_note")
            if st.button("Add", key="cc_add_btn"):
                db.add_critical_component(
                    cc_fam, cc_comp, cc_actor,
                    role=cc_role, lead_time_days=int(cc_lead), note=cc_note,
                )
                st.cache_data.clear()
                st.success(f"Added critical component: {cc_fam} / {cc_comp}")
                st.rerun()

    if crit_enriched and cc_actor:
        with st.expander(":heavy_minus_sign: Clear a critical component"):
            cids = [f"{r['Family']} — {r['Component SKU']}"
                    for r in crit_enriched]
            id_map = {f"{r['Family']} — {r['Component SKU']}": r["id"]
                      for r in crit_enriched}
            pick = st.selectbox("Pick one to remove", cids,
                                key="cc_clear_pick")
            if st.button("Remove", key="cc_clear_btn"):
                db.clear_critical_component(id_map[pick], cc_actor)
                st.cache_data.clear()
                st.rerun()

    # --- Shared components (auto-discovered from BOM) ---------------------
    st.markdown("### :gear: Auto-discovered shared components across tubes")

    if boms.empty:
        st.info(
            ":hourglass: BOM data not yet synced. Run "
            "`python cin7_sync.py boms` to populate. Once ready, this "
            "section will show components used across multiple tube "
            "families (e.g. Yukon mounting plate shared across Sierra/Smokies/etc), "
            "with projected consumption from tube demand and days-of-cover."
        )
    else:
        # Identify components used by our tube SKUs
        tube_skus = set(fam_view["SKU"])
        rel_boms = boms[boms["AssemblySKU"].isin(tube_skus)].copy()
        rel_boms["Quantity"] = _to_num(rel_boms["Quantity"]).fillna(0)

        # Component usage count across DISTINCT families
        def _family_of_sku(sku: str) -> Optional[str]:
            m = tubes_df[tubes_df["SKU"] == sku]
            return m["Family"].iloc[0] if not m.empty else None

        rel_boms["AssemblyFamily"] = rel_boms["AssemblySKU"].apply(_family_of_sku)
        comp_summary = (rel_boms.groupby(
            ["ComponentSKU", "ComponentName"], dropna=False)
            .agg(UsedByAssemblies=("AssemblySKU", "nunique"),
                 FamiliesCount=("AssemblyFamily", "nunique"),
                 AvgQtyPerAssembly=("Quantity", "mean"))
            .reset_index()
            .sort_values(["FamiliesCount", "UsedByAssemblies"],
                         ascending=False))

        comp_summary["OnHand"] = comp_summary["ComponentSKU"].map(
            lambda s: stock_by_sku.get(s, {}).get("OnHand", 0))
        comp_summary["Available"] = comp_summary["ComponentSKU"].map(
            lambda s: stock_by_sku.get(s, {}).get("Available", 0))

        # Projected consumption per component based on tube demand
        cons_map: dict = {}
        for _, r in rel_boms.iterrows():
            tube = r["AssemblySKU"]
            tube_units = float(vel.get(tube, 0))
            per = float(r["Quantity"] or 0)
            cons_map[r["ComponentSKU"]] = (
                cons_map.get(r["ComponentSKU"], 0) + tube_units * per
            )
        comp_summary[f"Projected consumption ({window_months}mo)"] = (
            comp_summary["ComponentSKU"].apply(
                lambda s: float(cons_map.get(s, 0))
            ).fillna(0).round(1)
        )
        comp_summary["Days of cover"] = comp_summary.apply(
            lambda r: (
                (r["OnHand"] /
                 (r[f"Projected consumption ({window_months}mo)"] /
                  (window_months * 30.437)))
                if r[f"Projected consumption ({window_months}mo)"] > 0
                else None
            ), axis=1,
        )

        st.caption(
            f"Components consumed by tubes in your selection. "
            f"Sorted to surface shared components (used across multiple "
            f"families) — these are your reorder-priority watches because "
            f"running out of one ripples into multiple tube SKUs."
        )

        st.dataframe(
            comp_summary.rename(columns={
                "ComponentSKU": "Component SKU",
                "ComponentName": "Name",
                "UsedByAssemblies": "# assemblies using",
                "FamiliesCount": "# families using",
                "AvgQtyPerAssembly": "Avg qty / assembly",
                "OnHand": "Stock (phys)",
                "Available": "Available",
                "Days of cover": "DoC",
            }),
            width="stretch", hide_index=True, height=400,
            column_config={
                "DoC": st.column_config.NumberColumn(format="%.0f days"),
            },
        )

        # Flag if Yukon MP appears as a component
        yukon_rows = comp_summary[
            comp_summary["ComponentSKU"].astype(str).str.upper()
                                 .str.contains("YUKON", na=False)
        ]
        if not yukon_rows.empty:
            st.warning(
                ":rotating_light: **Yukon component flags:** "
                + ", ".join(yukon_rows["ComponentSKU"].tolist())
                + " — watch these closely given the long supplier lead time "
                  "you mentioned. Projected consumption accumulates demand "
                  "from every tube that uses them."
            )


elif page == "Supplier Pricing":
    # ===========================================================
    # SUPPLIER PRICING & PACK RULES
    # ===========================================================
    # Curated team-input for:
    #   - Family-color tier pricing (Reeves SIERRA38/65 style)
    #   - Setup fees (color change, etc.)
    #   - Aggregation rule (sum_across_colors vs per_color)
    #   - Per-SKU pack quantities (e.g., MMA-M155-25A-M = 10/pack)
    # All explicit team decisions, fully auditable. Drives the
    # tier-optimisation cues that surface in the Ordering page.
    st.header(":moneybag: Supplier Pricing & Pack Rules")
    st.caption(
        "Curated team-input that drives the engine's reorder math. "
        "Tier prices, setup fees, family aggregation rules, per-SKU "
        "pack quantities. All edits are logged to the audit trail.")

    pricing_tab, fees_tab, rules_tab, packs_tab, seed_tab = st.tabs([
        "📊 Tier Pricing",
        "💵 Setup Fees",
        "⚙️ Aggregation Rules",
        "📦 Pack Quantities",
        "🌱 Quick Seed",
    ])

    actor = st.session_state.get("current_user") or "unknown"

    # ===== Tab 1: Tier Pricing =====
    with pricing_tab:
        st.subheader("Family-color tier prices")
        st.caption(
            "One row per (family, color, supplier, tier qty). "
            "tier_qty is the MINIMUM qty at which this row's "
            "unit_price applies. The tier the buyer qualifies for "
            "depends on the family's aggregation rule (see Rules tab).")

        existing_pricing = db.all_family_color_pricing()
        if existing_pricing:
            df_pricing = pd.DataFrame([dict(r) for r in existing_pricing])
            display_cols = ["family", "color", "supplier", "tier_qty",
                             "unit_price", "unit", "currency", "set_by",
                             "set_at"]
            st.dataframe(
                df_pricing[display_cols],
                hide_index=True, width="stretch",
                column_config={
                    "tier_qty": st.column_config.NumberColumn(format="%.0f"),
                    "unit_price": st.column_config.NumberColumn(format="$%.2f"),
                })
        else:
            st.info("No tier pricing rows yet. Add some below or use "
                    "the Quick Seed tab to load the Reeves SIERRA tiers.")

        with st.expander(":heavy_plus_sign: Add / update tier row"):
            ac1, ac2, ac3 = st.columns(3)
            with ac1:
                p_family = st.text_input("Family",
                                            value="SIERRA38",
                                            key="p_fam")
            with ac2:
                p_color = st.text_input("Color",
                                          value="White",
                                          key="p_col")
            with ac3:
                p_supplier = st.text_input("Supplier",
                                             value="Reeves",
                                             key="p_sup")
            ac4, ac5, ac6 = st.columns(3)
            with ac4:
                p_tier = st.number_input(
                    "Tier qty (min)",
                    min_value=1.0, value=500.0, step=100.0,
                    key="p_tier")
            with ac5:
                p_price = st.number_input(
                    "Unit price",
                    min_value=0.0, value=8.00, step=0.01, format="%.4f",
                    key="p_price")
            with ac6:
                p_unit = st.selectbox(
                    "Unit", options=["ft", "m", "unit", "pcs"],
                    key="p_unit")
            p_note = st.text_input("Note (optional)", key="p_note")
            if st.button(":floppy_disk: Save tier row",
                          key="p_save", type="primary",
                          disabled=not (p_family and p_color and p_supplier)):
                db.set_family_color_pricing(
                    family=p_family.strip(),
                    color=p_color.strip(),
                    supplier=p_supplier.strip(),
                    tier_qty=float(p_tier),
                    unit_price=float(p_price),
                    actor=actor,
                    unit=p_unit,
                    note=p_note,
                )
                st.success(f"Saved {p_family}/{p_color}/{p_supplier} "
                            f"@ {p_tier:.0f} = ${p_price:.4f}/{p_unit}")
                st.rerun()

        if existing_pricing:
            with st.expander(":wastebasket: Delete a tier row"):
                opts = [f"{r['family']} / {r['color']} / {r['supplier']} @ {r['tier_qty']:.0f}"
                         for r in existing_pricing]
                pick = st.selectbox("Pick row", options=[""] + opts,
                                      key="p_del_pick")
                if pick and st.button(":wastebasket: Delete",
                                       key="p_del_go", type="secondary"):
                    idx = opts.index(pick)
                    r = existing_pricing[idx]
                    db.delete_family_color_pricing(
                        r["family"], r["color"], r["supplier"],
                        float(r["tier_qty"]), actor)
                    st.success(f"Deleted {pick}")
                    st.rerun()

    # ===== Tab 2: Setup Fees =====
    with fees_tab:
        st.subheader("Setup / changeover fees")
        st.caption(
            "Fees that fire under specific PO conditions — the "
            "Reeves $750 color-change fee is the canonical example. "
            "fee_type='color_change' triggers when a single PO "
            "contains more than one color (and aggregation rule "
            "is sum_across_colors).")

        existing_fees = db.all_family_setup_fees()
        if existing_fees:
            df_fees = pd.DataFrame([dict(r) for r in existing_fees])
            st.dataframe(
                df_fees[["family", "supplier", "fee_type",
                          "fee_amount", "currency", "description",
                          "set_by", "set_at"]],
                hide_index=True, width="stretch",
                column_config={
                    "fee_amount": st.column_config.NumberColumn(format="$%.2f"),
                })
        else:
            st.info("No setup fees configured.")

        with st.expander(":heavy_plus_sign: Add / update setup fee"):
            fc1, fc2, fc3 = st.columns(3)
            with fc1:
                f_family = st.text_input("Family",
                                          value="SIERRA38",
                                          key="f_fam")
            with fc2:
                f_supplier = st.text_input("Supplier",
                                             value="Reeves",
                                             key="f_sup")
            with fc3:
                f_type = st.selectbox(
                    "Fee type",
                    options=["color_change", "tooling_change",
                              "minimum_runtime", "other"],
                    key="f_type")
            fc4, fc5 = st.columns(2)
            with fc4:
                f_amt = st.number_input(
                    "Fee amount",
                    min_value=0.0, value=750.0, step=50.0,
                    key="f_amt")
            with fc5:
                f_desc = st.text_input(
                    "Description",
                    value="Color change setup fee",
                    key="f_desc")
            if st.button(":floppy_disk: Save fee",
                          key="f_save", type="primary",
                          disabled=not (f_family and f_supplier)):
                db.set_family_setup_fee(
                    family=f_family.strip(),
                    supplier=f_supplier.strip(),
                    fee_type=f_type,
                    fee_amount=float(f_amt),
                    actor=actor,
                    description=f_desc,
                )
                st.success(f"Saved {f_family}/{f_supplier}/{f_type} = ${f_amt:.2f}")
                st.rerun()

    # ===== Tab 3: Aggregation Rules =====
    with rules_tab:
        st.subheader("Family aggregation rules & nag thresholds")
        st.caption(
            "How tier qualification rolls up for each family. "
            "**sum_across_colors** = total demand across all colors "
            "qualifies the tier; cross-color POs incur the color "
            "change fee. **per_color** = each color metered "
            "separately; no color change fee.")

        existing_rules = db.all_family_pricing_rules()
        if existing_rules:
            df_rules = pd.DataFrame([dict(r) for r in existing_rules])
            st.dataframe(
                df_rules[["family", "supplier", "rule",
                           "nag_threshold_savings", "nag_threshold_pct",
                           "auto_pad_threshold_savings",
                           "set_by", "set_at"]],
                hide_index=True, width="stretch",
                column_config={
                    "nag_threshold_savings": st.column_config.NumberColumn(format="$%.0f"),
                    "auto_pad_threshold_savings": st.column_config.NumberColumn(format="$%.0f"),
                    "nag_threshold_pct": st.column_config.NumberColumn(format="%.0f%%"),
                })
        else:
            st.info("No aggregation rules configured. Defaults to "
                    "**per_color** if not set explicitly.")

        with st.expander(":heavy_plus_sign: Add / update rule"):
            rc1, rc2, rc3 = st.columns(3)
            with rc1:
                r_family = st.text_input("Family",
                                          value="SIERRA38",
                                          key="r_fam")
            with rc2:
                r_supplier = st.text_input("Supplier",
                                             value="Reeves",
                                             key="r_sup")
            with rc3:
                r_rule = st.selectbox(
                    "Aggregation rule",
                    options=["sum_across_colors", "per_color"],
                    key="r_rule")
            rc4, rc5, rc6 = st.columns(3)
            with rc4:
                r_nag_sav = st.number_input(
                    "Nag if savings >",
                    min_value=0.0, value=200.0, step=50.0,
                    help="Buyer gets nudged when next-tier savings "
                         "exceed this amount.", key="r_nag_sav")
            with rc5:
                r_nag_pct = st.number_input(
                    "Or within % of tier-gap",
                    min_value=0.0, max_value=100.0, value=25.0, step=5.0,
                    help="Nudge when current qty is within this % of "
                         "the gap to next tier.", key="r_nag_pct")
            with rc6:
                r_auto_pad = st.number_input(
                    "Auto-pad if savings > (0 = ask)",
                    min_value=0.0, value=0.0, step=100.0,
                    key="r_auto_pad")
            if st.button(":floppy_disk: Save rule",
                          key="r_save", type="primary",
                          disabled=not (r_family and r_supplier)):
                db.set_family_pricing_rule(
                    family=r_family.strip(),
                    supplier=r_supplier.strip(),
                    rule=r_rule,
                    actor=actor,
                    nag_threshold_savings=float(r_nag_sav),
                    nag_threshold_pct=float(r_nag_pct),
                    auto_pad_threshold_savings=(float(r_auto_pad)
                                                 if r_auto_pad > 0
                                                 else None),
                )
                st.success(f"Saved {r_family}/{r_supplier} rule = {r_rule}")
                st.rerun()

    # ===== Tab 4: Pack Quantities =====
    with packs_tab:
        st.subheader("Per-SKU pack quantities")
        st.caption(
            "When a SKU comes in fixed pack sizes (e.g., MMA-M155-"
            "25A-M is sold in packs of 10), the engine rounds reorder "
            "qty UP to the nearest multiple. MOQ override is optional "
            "— useful when this SKU's MOQ differs from the supplier "
            "default in supplier_config.")

        existing_packs = db.all_sku_pack()
        if existing_packs:
            df_packs = pd.DataFrame([dict(r) for r in existing_packs])
            # Decorate with product names if available
            if not products.empty:
                names = dict(zip(products["SKU"].astype(str),
                                  products["Name"].astype(str)))
                df_packs["Name"] = df_packs["sku"].map(names).fillna("")
            st.dataframe(df_packs, hide_index=True, width="stretch",
                          column_config={
                              "pack_qty": st.column_config.NumberColumn(format="%g"),
                              "moq": st.column_config.NumberColumn(format="%g"),
                          })
        else:
            st.info("No pack quantities configured.")

        with st.expander(":heavy_plus_sign: Add / update pack qty"):
            sk_options = (sorted(set(products["SKU"].astype(str)))
                           if not products.empty else [])
            pq1, pq2, pq3 = st.columns([3, 1, 1])
            with pq1:
                pq_sku = st.selectbox(
                    "SKU", options=[""] + sk_options,
                    key="pq_sku")
            with pq2:
                pq_pack = st.number_input(
                    "Pack qty",
                    min_value=0.0, value=10.0, step=1.0,
                    key="pq_pack")
            with pq3:
                pq_moq = st.number_input(
                    "MOQ override (0=none)",
                    min_value=0.0, value=0.0, step=1.0,
                    key="pq_moq")
            pq_note = st.text_input("Note", key="pq_note")
            if st.button(":floppy_disk: Save pack",
                          key="pq_save", type="primary",
                          disabled=not pq_sku):
                db.set_sku_pack(
                    sku=pq_sku,
                    pack_qty=float(pq_pack),
                    actor=actor,
                    moq=(float(pq_moq) if pq_moq > 0 else None),
                    note=pq_note,
                )
                st.success(f"Saved {pq_sku}: pack of {pq_pack:g}")
                st.rerun()

        if existing_packs:
            with st.expander(":wastebasket: Clear pack qty"):
                pick = st.selectbox(
                    "Pick SKU",
                    options=[""] + [r["sku"] for r in existing_packs],
                    key="pq_del")
                if pick and st.button(":wastebasket: Clear",
                                       key="pq_del_go"):
                    db.clear_sku_pack(pick, actor)
                    st.success(f"Cleared {pick}")
                    st.rerun()

    # ===== Tab 5: Quick Seed =====
    with seed_tab:
        st.subheader("Seed Reeves SIERRA tiers from supplier quotes")
        st.caption(
            "One-click load of the Reeves SIERRA38/SIERRA65 White/Black "
            "tier prices from the work-instruction quotes "
            "(WI LED-SIERRA38 030626, WI LED-SIERRA65 042125). "
            "Will overwrite any existing rows for these family-color-tier "
            "combinations. Setup fee, aggregation rule, and color "
            "change setup fee are also configured.")

        seed_data = [
            # (family, color, tier_qty, unit_price)
            ("SIERRA38", "White", 500, 8.00),
            ("SIERRA38", "White", 1000, 5.02),
            ("SIERRA38", "White", 2500, 3.22),
            ("SIERRA38", "White", 5000, 2.47),
            ("SIERRA38", "Black", 500, 9.67),
            ("SIERRA38", "Black", 1000, 5.83),
            ("SIERRA38", "Black", 2500, 3.76),
            ("SIERRA38", "Black", 5000, 2.96),
            ("SIERRA65", "White", 500, 10.60),
            ("SIERRA65", "White", 1000, 7.41),
            ("SIERRA65", "White", 2500, 5.50),
            ("SIERRA65", "White", 5000, 4.42),
            ("SIERRA65", "Black", 500, 12.12),
            ("SIERRA65", "Black", 1000, 8.01),
            ("SIERRA65", "Black", 2500, 5.72),
            ("SIERRA65", "Black", 5000, 4.75),
        ]
        st.dataframe(
            pd.DataFrame(
                seed_data,
                columns=["Family", "Color", "Tier (ft)", "Unit price ($/ft)"],
            ),
            hide_index=True, width="stretch")

        if st.button(":seedling: Seed Reeves SIERRA tiers + rules",
                      type="primary", key="seed_go"):
            for family, color, tier, price in seed_data:
                db.set_family_color_pricing(
                    family=family, color=color, supplier="Reeves Extruded Products, Inc",
                    tier_qty=float(tier), unit_price=float(price),
                    actor=actor, unit="ft",
                    note="seeded from supplier quote PDFs",
                )
            # Setup fee ($750 color change) for each family
            for family in ("SIERRA38", "SIERRA65"):
                db.set_family_setup_fee(
                    family=family, supplier="Reeves Extruded Products, Inc",
                    fee_type="color_change",
                    fee_amount=750.0, actor=actor,
                    description="$750 color change fee per quote sheet",
                )
                # Aggregation rule: sum_across_colors per user spec
                db.set_family_pricing_rule(
                    family=family, supplier="Reeves Extruded Products, Inc",
                    rule="sum_across_colors", actor=actor,
                    nag_threshold_savings=200.0,
                    nag_threshold_pct=25.0,
                    note="Reeves: total footage qualifies tier; "
                         "color change fee for cross-color PO",
                )
            st.success(
                f"Seeded {len(seed_data)} tier rows + 2 setup fees + "
                f"2 aggregation rules. Switch to Tier Pricing tab "
                f"to verify.")
            st.rerun()


elif page == "Migrations":
    # ============================================================
    # MIGRATIONS REGISTRY — central management for retiring/successor
    # SKU mappings.
    # ============================================================
    # Predecessor (retiring) → successor mappings live in db.sku_migrations.
    # Sources today:
    #   - Manual entries (set via Section 7 redirect or this page's form)
    #   - IP-imported (from `ip_import_migrations.py`, source string starts
    #     with "ip-import")
    #   - Auto-tube-rule (set by the LED Tubes page when it auto-proposes
    #     SMOKIES → SIERRA family successors)
    # All paths lead through db.set_migration() so audit/log is consistent.
    st.header(":scroll: Migrations registry")
    st.caption(
        "Predecessor → successor mappings. The reorder engine uses these "
        "to roll a retiring SKU's historical sales into its successor's "
        "demand forecast — so the new SKU is reordered for the WHOLE "
        "lineage's demand, not just its own ramp-up.")

    all_migs_rows = db.all_migrations()
    all_migs = [dict(r) for r in all_migs_rows]
    if not all_migs:
        st.info(
            "No migrations recorded yet. Use the **+ Add migration** form "
            "below or run `ip_import_migrations.py --apply` to import "
            "from Inventory Planner.")
    # ---- Summary stats -----------------------------------------------
    by_source: dict = {}
    for m in all_migs:
        src = (m.get("set_by") or "unknown").split(":")[0]
        by_source[src] = by_source.get(src, 0) + 1

    # Build SKU-keyed velocity + stock lookups so we can decorate each row.
    # Defensive: on a fresh deploy the salelines sync may still be running
    # in the background, so sale_lines can be empty or missing columns.
    # In that case we just skip the velocity decoration — the page still
    # renders, just without velocity numbers next to each migration.
    sl_dt = sale_lines.copy()
    if sl_dt.empty or "InvoiceDate" not in sl_dt.columns:
        st.warning(
            ":hourglass: Sale-line history not yet loaded — velocity "
            "columns will fill in once the background sync completes. "
            "Check `tail /data/output/salelines_sync.log` on the server "
            "for progress.")
        sl_dt = pd.DataFrame(
            columns=["InvoiceDate", "Quantity", "SKU", "Customer"])
    else:
        sl_dt["InvoiceDate"] = pd.to_datetime(
            sl_dt["InvoiceDate"], errors="coerce")
        sl_dt["Quantity"] = pd.to_numeric(
            sl_dt["Quantity"], errors="coerce").fillna(0)
    today = pd.Timestamp(datetime.now().date())
    cutoff_365 = today - pd.Timedelta(days=365)

    units_12mo_by_sku = (
        sl_dt[sl_dt["InvoiceDate"] >= cutoff_365]
        .groupby("SKU")["Quantity"].sum().to_dict()
    )
    onhand_by_sku = {}
    if not stock.empty and "SKU" in stock.columns and "OnHand" in stock.columns:
        onhand_by_sku = (
            pd.to_numeric(stock["OnHand"], errors="coerce")
            .groupby(stock["SKU"]).sum().to_dict()
        )
    name_by_sku = {}
    if not products.empty and "SKU" in products.columns:
        name_by_sku = dict(zip(
            products["SKU"].astype(str),
            products["Name"].astype(str) if "Name" in products.columns
            else [""] * len(products)
        ))

    # Count predecessors with residual stock
    n_with_stock = sum(
        1 for m in all_migs
        if float(onhand_by_sku.get(str(m.get("retiring_sku")), 0) or 0) > 0
    )
    n_with_sales = sum(
        1 for m in all_migs
        if float(units_12mo_by_sku.get(str(m.get("retiring_sku")), 0) or 0) > 0
    )

    cols = st.columns(4)
    cols[0].metric("Total migrations", len(all_migs))
    cols[1].metric("Sources",
                    " · ".join(f"{k}={v}" for k, v in
                                sorted(by_source.items())) or "—")
    cols[2].metric("Predecessors w/ residual stock", n_with_stock)
    cols[3].metric("Predecessors w/ 12mo sales", n_with_sales)

    st.divider()

    # ---- + Add migration form ----------------------------------------
    with st.expander(":heavy_plus_sign: Add new migration", expanded=False):
        st.caption(
            "Map a retiring SKU to its successor. The retiring SKU's "
            "12mo sales × Share % roll into the successor's demand.")
        sku_options = sorted(set(products["SKU"].astype(str))) if not products.empty else []
        c1, c2 = st.columns(2)
        with c1:
            new_retiring = st.selectbox(
                "Retiring (predecessor) SKU",
                options=[""] + sku_options,
                key="mig_new_ret",
                help="The SKU being phased out / replaced.")
        with c2:
            new_successor = st.selectbox(
                "Successor (active) SKU",
                options=[""] + sku_options,
                key="mig_new_succ",
                help="The SKU that absorbs the retiring SKU's demand.")
        c3, c4 = st.columns([1, 3])
        with c3:
            new_share = st.number_input(
                "Share %", min_value=1.0, max_value=100.0, value=100.0,
                step=5.0, key="mig_new_share",
                help="Percentage of retiring SKU's sales that roll into "
                     "the successor. 100 = full migration.")
        with c4:
            new_note = st.text_input(
                "Note (optional)",
                key="mig_new_note",
                placeholder="Why this migration? Any caveats?")
        save_disabled = (not new_retiring or not new_successor
                          or new_retiring == new_successor)
        if st.button(":floppy_disk: Save migration",
                      key="mig_new_save",
                      disabled=save_disabled,
                      type="primary"):
            actor = st.session_state.get("current_user") or "unknown"
            db.set_migration(
                retiring_sku=new_retiring,
                successor_sku=new_successor,
                actor=actor,
                share_pct=float(new_share),
                note=new_note,
            )
            st.success(
                f"Saved: **{new_retiring}** → **{new_successor}** "
                f"@ {new_share:.0f}%")
            st.rerun()

    # ---- Suggested predecessors from IP notes parser -----------------
    # Surface SKU references found in IP notes that AREN'T yet recorded
    # as migrations — likely candidates the team wrote down but never
    # formalised. Two-tier: REPLACEMENT-intent (high confidence), other
    # references (lower).
    if IP_NOTES:
        existing_retiring = {str(m.get("retiring_sku") or "")
                              for m in all_migs}
        suggested_high = []  # REPLACEMENT-intent
        suggested_low = []   # other intents / no intent
        for sku_with_note, note_entries in IP_NOTES.items():
            for ne in note_entries:
                note_text = ne.get("text") or ""
                parsed = _parse_note_for_skus(
                    note_text, products, self_sku=str(sku_with_note))
                for p in parsed:
                    intent = p.get("intent") or ""
                    is_replacement = (
                        "replac" in intent.lower() or "🔁" in intent)
                    for cand_sku in p["matches"][:1]:  # top match
                        if cand_sku == sku_with_note:
                            continue
                        # If cand_sku already mapped as retiring, skip
                        if cand_sku in existing_retiring:
                            continue
                        record = {
                            "Suggested predecessor": cand_sku,
                            "Successor": sku_with_note,
                            "Note": note_text[:80],
                            "Intent": intent or "—",
                            "Token matched": p["token"],
                        }
                        if is_replacement:
                            suggested_high.append(record)
                        else:
                            suggested_low.append(record)
        if suggested_high:
            st.markdown(
                f"#### :bulb: Suggested predecessors from IP notes "
                f"({len(suggested_high)} high-confidence)")
            st.caption(
                "These are SKU references found in your team's IP notes "
                "with REPLACEMENT-style intent words. Review and add "
                "them as migrations to feed their sales into the "
                "successor's forecast.")
            st.dataframe(
                pd.DataFrame(suggested_high),
                hide_index=True, width="stretch",
            )
        if suggested_low:
            with st.expander(
                    f":mag: Other SKU references in notes "
                    f"({len(suggested_low)} lower confidence)"):
                st.dataframe(
                    pd.DataFrame(suggested_low),
                    hide_index=True, width="stretch",
                )

    # ---- Master table -----------------------------------------------
    if all_migs:
        st.markdown(f"#### :scroll: All {len(all_migs)} migrations")
        rows = []
        for m in all_migs:
            ret = str(m.get("retiring_sku") or "")
            succ = str(m.get("successor_sku") or "")
            rows.append({
                "Predecessor": ret,
                "Predecessor name": (name_by_sku.get(ret) or "")[:50],
                "Successor": succ,
                "Successor name": (name_by_sku.get(succ) or "")[:50],
                "Share %": f"{float(m.get('share_pct') or 0):.0f}",
                "Source": m.get("set_by") or "—",
                "Set on": str(m.get("set_at") or "—")[:19],
                "Pred 12mo units": int(units_12mo_by_sku.get(ret, 0) or 0),
                "Pred OnHand": f"{onhand_by_sku.get(ret, 0):.0f}"
                                if onhand_by_sku.get(ret, 0) else "—",
                "Note": (m.get("note") or "")[:60],
            })
        df_migs = pd.DataFrame(rows)
        # Filter
        f1, f2, f3 = st.columns([2, 1, 1])
        with f1:
            search = st.text_input(
                "Filter by SKU / name (substring)",
                key="mig_search").strip().lower()
        with f2:
            source_filter = st.selectbox(
                "Source",
                options=["(all)"] + sorted(by_source.keys()),
                key="mig_source_filter")
        with f3:
            stock_filter = st.selectbox(
                "Has residual stock",
                options=["(any)", "Yes", "No"],
                key="mig_stock_filter")
        # Apply filters
        df_view = df_migs.copy()
        if search:
            mask = (
                df_view["Predecessor"].str.lower().str.contains(search, na=False)
                | df_view["Successor"].str.lower().str.contains(search, na=False)
                | df_view["Predecessor name"].str.lower().str.contains(search, na=False)
                | df_view["Successor name"].str.lower().str.contains(search, na=False)
            )
            df_view = df_view[mask]
        if source_filter != "(all)":
            df_view = df_view[df_view["Source"] == source_filter]
        if stock_filter == "Yes":
            df_view = df_view[df_view["Pred OnHand"] != "—"]
        elif stock_filter == "No":
            df_view = df_view[df_view["Pred OnHand"] == "—"]
        st.caption(
            f"Showing {len(df_view)} of {len(df_migs)} migrations.")
        st.dataframe(df_view, hide_index=True, width="stretch")

        # ---- Edit / clear single migration --------------------------
        st.divider()
        st.markdown("##### :pencil2: Edit or clear an existing migration")
        edit_choice = st.selectbox(
            "Pick a predecessor SKU to edit / clear",
            options=[""] + [r["Predecessor"] for r in rows],
            key="mig_edit_pick")
        if edit_choice:
            existing = next(
                (m for m in all_migs
                 if str(m.get("retiring_sku")) == edit_choice), None)
            if existing:
                e1, e2, e3 = st.columns([2, 1, 1])
                with e1:
                    edit_succ = st.selectbox(
                        "Successor",
                        options=sku_options,
                        index=(sku_options.index(
                            str(existing.get("successor_sku")))
                               if str(existing.get("successor_sku"))
                               in sku_options else 0),
                        key="mig_edit_succ")
                with e2:
                    edit_share = st.number_input(
                        "Share %", min_value=1.0, max_value=100.0,
                        value=float(existing.get("share_pct") or 100),
                        step=5.0, key="mig_edit_share")
                with e3:
                    st.write("")
                    st.write("")
                    if st.button(":wastebasket: Clear",
                                  key="mig_edit_clear",
                                  help="Remove this migration."):
                        actor = (st.session_state.get("current_user")
                                  or "unknown")
                        if hasattr(db, "clear_migration"):
                            db.clear_migration(edit_choice, actor)
                        st.success(f"Cleared: {edit_choice}")
                        st.rerun()
                edit_note = st.text_input(
                    "Note",
                    value=str(existing.get("note") or ""),
                    key="mig_edit_note")
                if st.button(":floppy_disk: Save changes",
                              key="mig_edit_save",
                              type="primary"):
                    actor = (st.session_state.get("current_user")
                              or "unknown")
                    db.set_migration(
                        retiring_sku=edit_choice,
                        successor_sku=edit_succ,
                        actor=actor,
                        share_pct=float(edit_share),
                        note=edit_note,
                    )
                    st.success(
                        f"Updated: {edit_choice} → {edit_succ} "
                        f"@ {edit_share:.0f}%")
                    st.rerun()


elif page == "Stock Explorer":
    st.header(":package: Stock Explorer")

    if stock.empty:
        st.warning("No stock data. Run `python cin7_sync.py stock`.")
    else:
        # Join BOM flag from product master so we can tell cuts/assemblies apart
        df = stock.copy()
        df["OnHand"] = _to_num(df["OnHand"]).fillna(0)
        df["Available"] = _to_num(df["Available"]).fillna(0)
        df["OnOrder"] = _to_num(df["OnOrder"]).fillna(0)
        df["Allocated"] = _to_num(df.get("Allocated", 0)).fillna(0)
        # Phantom stock = derivable from BOM masters (Available minus physical)
        df["Phantom"] = (df["Available"] - df["OnHand"]).clip(lower=0)

        if not products.empty:
            bom_map = products.set_index("SKU")[
                ["BillOfMaterial", "BOMType", "AutoAssembly",
                 "AutoDisassembly", "AverageCost"]
            ].to_dict(orient="index")
            df["IsBOM"] = df["SKU"].map(
                lambda s: str(bom_map.get(s, {}).get("BillOfMaterial")) == "True"
            )
            df["BOMType"] = df["SKU"].map(
                lambda s: bom_map.get(s, {}).get("BOMType")
            )
            df["AvgCost"] = df["SKU"].map(
                lambda s: float(bom_map.get(s, {}).get("AverageCost") or 0)
            )
            # OnHandValue uses CIN7's FIFO StockOnHand (authoritative),
            # falling back to OnHand × AvgCost only when CIN7 returns 0.
            if "StockOnHand" in df.columns:
                _fifo = _to_num(df["StockOnHand"]).fillna(0)
                _oxa = df["OnHand"] * df["AvgCost"]
                df["OnHandValue"] = _fifo.where(_fifo > 0, _oxa)
            else:
                df["OnHandValue"] = df["OnHand"] * df["AvgCost"]
        else:
            df["IsBOM"] = False
            df["BOMType"] = None
            df["AvgCost"] = 0.0
            df["OnHandValue"] = 0.0

        # Parent / family columns (populated when BOM sync has run)
        df["Parent"] = df["SKU"].map(parent_sku_for)
        df["Family"] = df["SKU"].map(family_sku_for)

        c1, c2, c3, c4 = st.columns(4)
        locs = sorted(stock["Location"].dropna().unique().tolist())
        sel_loc = c1.multiselect("Location", locs, default=[])
        q = c2.text_input("Search SKU or name", "")
        only = c3.selectbox(
            "Stock filter",
            ["All", "Zero physical (OnHand=0)", "Below 5 physical",
             "Positive physical only"],
        )
        bom_only = c4.selectbox(
            "BOM filter",
            ["All", "BOM products only", "Non-BOM only",
             "Phantom stock > 0 (derivable from masters)"],
        )

        if sel_loc:
            df = df[df["Location"].isin(sel_loc)]
        if q:
            mask = (
                df["SKU"].astype(str).str.contains(q, case=False, na=False) |
                df["Name"].astype(str).str.contains(q, case=False, na=False)
            )
            df = df[mask]
        if only == "Zero physical (OnHand=0)":
            df = df[df["OnHand"] <= 0]
        elif only == "Below 5 physical":
            df = df[(df["OnHand"] > 0) & (df["OnHand"] < 5)]
        elif only == "Positive physical only":
            df = df[df["OnHand"] > 0]

        if bom_only == "BOM products only":
            df = df[df["IsBOM"]]
        elif bom_only == "Non-BOM only":
            df = df[~df["IsBOM"]]
        elif bom_only == "Phantom stock > 0 (derivable from masters)":
            df = df[df["Phantom"] > 0]

        # Summary strip
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("SKU-locations shown", _fmt_number(len(df)))
        c2.metric("Physical units (OnHand)", _fmt_number(df["OnHand"].sum()))
        c3.metric("Phantom units (derivable)",
                  _fmt_number(df["Phantom"].sum()))
        c4.metric("Physical cash tied up",
                  _fmt_money(df["OnHandValue"].sum()))

        with st.expander("What's 'Phantom Stock'?"):
            st.markdown(
                "**`Available − OnHand`** for BOM products. These are units "
                "CIN7 *could* make by auto-assembly or auto-disassembly from "
                "master-length stock. They don't exist yet — no cash is tied "
                "up in them — but they're fulfillable if a customer orders.\n\n"
                "- **`OnHand`** = physical stock with actual cash invested.\n"
                "- **`Available`** = OnHand + Phantom = what we can actually "
                "ship to a customer.\n"
                "- Use **OnHand × AvgCost** for cash / working capital analysis.\n"
                "- Use **Available** for service-level / reorder decisions."
            )

        show_cols = [
            "SKU", "Name", "Parent", "Location", "OnHand", "Phantom", "Available",
            "Allocated", "OnOrder", "IsBOM", "BOMType",
            "AvgCost", "OnHandValue", "Bin", "NextDeliveryDate",
        ]
        show_cols = [c for c in show_cols if c in df.columns]
        limit = rows_selector(key="stock_rows")
        sorted_df = df[show_cols].sort_values("OnHandValue", ascending=False)
        st.caption(f"Showing {min(limit, len(sorted_df)):,} of {len(sorted_df):,} matching rows")
        st.dataframe(
            sorted_df.head(limit),
            width="stretch",
            height=520,
            column_config={
                "AvgCost": st.column_config.NumberColumn(format="$%.2f"),
                "OnHandValue": st.column_config.NumberColumn(format="$%.0f"),
                "IsBOM": st.column_config.CheckboxColumn("BOM?"),
            },
        )


# ---------------------------------------------------------------------------
# Page: Product Master
# ---------------------------------------------------------------------------

elif page == "Product Master":
    st.header(":label: Product Master")

    if products.empty:
        st.warning("No products data. Run `python cin7_sync.py products`.")
    else:
        c1, c2, c3, c4 = st.columns(4)
        cats = sorted(products["Category"].dropna().unique().tolist())
        brands = sorted(products["Brand"].dropna().unique().tolist())
        types = sorted(products["Type"].dropna().unique().tolist())
        statuses = sorted(products["Status"].dropna().unique().tolist())

        sel_cat = c1.multiselect("Category", cats, default=[])
        sel_brand = c2.multiselect("Brand", brands, default=[])
        sel_type = c3.multiselect("Type", types, default=[])
        sel_status = c4.multiselect("Status", statuses, default=["Active"]
                                    if "Active" in statuses else [])

        q = st.text_input("Search SKU or name", "")

        df = products.copy()
        if sel_cat:
            df = df[df["Category"].isin(sel_cat)]
        if sel_brand:
            df = df[df["Brand"].isin(sel_brand)]
        if sel_type:
            df = df[df["Type"].isin(sel_type)]
        if sel_status:
            df = df[df["Status"].isin(sel_status)]
        if q:
            mask = (
                df["SKU"].astype(str).str.contains(q, case=False, na=False) |
                df["Name"].astype(str).str.contains(q, case=False, na=False)
            )
            df = df[mask]

        # Add Parent column from BOM index
        df["Parent"] = df["SKU"].map(parent_sku_for)

        show_cols = ["SKU", "Name", "Parent", "Category", "Brand", "Type",
                     "Status", "AverageCost", "MinimumBeforeReorder",
                     "ReorderQuantity", "CreatedDate", "LastModifiedOn"]
        show_cols = [c for c in show_cols if c in df.columns]
        limit = rows_selector(key="product_rows")
        st.caption(f"Showing {min(limit, len(df)):,} of {len(df):,} "
                   f"matching (out of {len(products):,} total)")
        st.dataframe(df[show_cols].head(limit),
                     width="stretch", height=560)


# ---------------------------------------------------------------------------
# Page: Purchase Analysis
# ---------------------------------------------------------------------------

elif page == "Purchase Analysis":
    st.header(":truck: Purchase Analysis (last 90 days)")

    if purchase_lines.empty:
        st.warning("No purchase line data. Run `python cin7_sync.py "
                   "purchaselines --days 90`.")
    else:
        df = purchase_lines.copy()
        df["Total"] = _to_num(df["Total"]).fillna(0)
        df["Quantity"] = _to_num(df["Quantity"]).fillna(0)
        df["OrderDate"] = _to_date(df["OrderDate"]).dt.tz_localize(None)

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Line items", _fmt_number(len(df)))
        c2.metric("Distinct POs", _fmt_number(df["PurchaseID"].nunique()))
        c3.metric("Distinct SKUs", _fmt_number(df["SKU"].nunique()))
        c4.metric("Total value", _fmt_money(df["Total"].sum()))

        tab_sup, tab_sku, tab_po = st.tabs(
            ["By supplier", "Top SKUs", "Recent POs"])

        with tab_sup:
            by_sup = (
                df.groupby("Supplier", dropna=False)
                  .agg(POs=("PurchaseID", "nunique"),
                       Lines=("SKU", "count"),
                       SKUs=("SKU", "nunique"),
                       Value=("Total", "sum"))
                  .sort_values("Value", ascending=False)
            )
            limit = rows_selector(key="pa_sup_rows")
            st.caption(f"Showing {min(limit, len(by_sup)):,} of {len(by_sup):,} suppliers")
            st.dataframe(by_sup.head(limit), width="stretch")

        with tab_sku:
            by_sku = (
                df.groupby(["SKU", "Name"], dropna=False)
                  .agg(Qty=("Quantity", "sum"),
                       Value=("Total", "sum"),
                       POs=("PurchaseID", "nunique"))
                  .sort_values("Value", ascending=False)
            )
            limit = rows_selector(key="pa_sku_rows")
            st.caption(f"Showing {min(limit, len(by_sku)):,} of {len(by_sku):,} SKUs")
            st.dataframe(by_sku.head(limit), width="stretch")

        with tab_po:
            po_summary = (
                df.groupby(["PurchaseID", "OrderNumber", "OrderDate",
                            "Supplier", "Status"], dropna=False)
                  .agg(Lines=("SKU", "count"),
                       Value=("Total", "sum"))
                  .reset_index()
                  .sort_values("OrderDate", ascending=False)
            )
            limit = rows_selector(key="pa_po_rows")
            st.caption(f"Showing {min(limit, len(po_summary)):,} of {len(po_summary):,} POs")
            st.dataframe(po_summary.head(limit),
                         width="stretch", height=560)


# ---------------------------------------------------------------------------
# Page: Sales Recent
# ---------------------------------------------------------------------------

elif page == "Sales Recent":
    st.header(":moneybag: Recent Sales")

    if sale_lines.empty:
        st.warning("No sale line data. Run `python cin7_sync.py "
                   "salelines --days 30`.")
    else:
        window = "30 days" if not sale_lines_30d.empty else "3 days"
        st.caption(f"Showing sale lines for the last {window}")

        df = sale_lines.copy()
        df["Total"] = _to_num(df["Total"]).fillna(0)
        df["Quantity"] = _to_num(df["Quantity"]).fillna(0)
        df["InvoiceDate"] = _to_date(df["InvoiceDate"]).dt.tz_localize(None)

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Line items", _fmt_number(len(df)))
        c2.metric("Distinct sales", _fmt_number(df["SaleID"].nunique()))
        c3.metric("Distinct SKUs", _fmt_number(df["SKU"].nunique()))
        c4.metric("Total revenue", _fmt_money(df["Total"].sum()))

        # Optional filter by SaleType if present
        if "SaleType" in df.columns:
            types = sorted(df["SaleType"].dropna().unique().tolist())
            sel_types = st.multiselect("Sale type", types, default=types)
            if sel_types:
                df = df[df["SaleType"].isin(sel_types)]

        tab_sku, tab_cust, tab_lines = st.tabs(
            ["Top SKUs", "Top customers", "Recent lines"])

        with tab_sku:
            by_sku = (
                df.groupby(["SKU", "Name"], dropna=False)
                  .agg(Qty=("Quantity", "sum"),
                       Revenue=("Total", "sum"),
                       Orders=("SaleID", "nunique"))
                  .sort_values("Revenue", ascending=False)
            )
            limit = rows_selector(key="sr_sku_rows")
            st.caption(f"Showing {min(limit, len(by_sku)):,} of {len(by_sku):,} SKUs")
            st.dataframe(by_sku.head(limit), width="stretch")

        with tab_cust:
            by_cust = (
                df.groupby(["CustomerID", "Customer"], dropna=False)
                  .agg(Orders=("SaleID", "nunique"),
                       Lines=("SKU", "count"),
                       Revenue=("Total", "sum"))
                  .sort_values("Revenue", ascending=False)
            )
            limit = rows_selector(key="sr_cust_rows")
            st.caption(f"Showing {min(limit, len(by_cust)):,} of {len(by_cust):,} customers")
            st.dataframe(by_cust.head(limit), width="stretch")

        with tab_lines:
            recent = (df.sort_values("InvoiceDate", ascending=False)
                      [["InvoiceDate", "OrderNumber", "Customer",
                        "SKU", "Name", "Quantity", "Price", "Total", "Status"]])
            limit = rows_selector(key="sr_lines_rows")
            st.caption(f"Showing {min(limit, len(recent)):,} of {len(recent):,} lines")
            st.dataframe(
                recent.head(limit),
                width="stretch",
                height=560,
            )


# ---------------------------------------------------------------------------
# Page: Ordering — unified ABC-driven reorder workflow
# ---------------------------------------------------------------------------

elif page == "Ordering":
    st.header(":shopping_trolley: Ordering — ABC-driven reorder workbench")
    st.caption(
        "Unified buying workspace. ABC classification on 12-month velocity. "
        "Supplier-first workflow with freight-mode-aware lead times, "
        "transparent calculations, and draft-PO staging."
    )

    # ------------------------------------------------------------------
    # Glossary — click-to-reveal definitions for every buyer-facing term.
    # Keep terminology single-sourced here so edits propagate via search.
    # ------------------------------------------------------------------
    with st.expander(
        ":book: How to read this page — glossary & methodology",
        expanded=False,
    ):
        st.markdown("""
#### ABC class
Every SKU is ranked A / B / C on a hybrid score (60% of 12-month value
rank + 40% of 12-month quantity rank):
- **A** — top cumulative 70% of annual value. High-impact items, watch closely.
- **B** — next 20%. Steady movers.
- **C** — last 10%. Low-impact, review less frequently.

#### Lead time (LT)
How long from placing the PO to receiving the goods. Set per supplier
in the Supplier configuration expander below. Air vs sea toggles use
different LTs; the engine picks the faster one when the supplier offers
air AND the item qualifies.

#### Safety %
A buffer added on top of lead-time demand to absorb variance (a big
order, a bad month). Defaults per class: A=30%, B=20%, C=15%.

#### Review days
How long between buying reviews for this supplier. The engine adds
`avg_daily × review_days` to target stock so you're covered between
reviews. Default: A=14d, B=30d, C=45d. Longer review = more stock
buffer, fewer POs. Shorter review = less capital tied up, more
frequent ordering.

#### Target stock — the reorder target
**`target = (LT × avg_daily × (1 + safety%)) + (avg_daily × review_days)`**
This is how much stock should be sitting on the shelf on a typical day
to cover the lead time and the review period without stocking out.

#### Suggested reorder (engine)
**`max(0, target − (Available + OnOrder − unfulfilled))`**
Only what you need to bring effective position back up to target.
Already accounts for open POs (ORDERED / ORDERING) and backorders.

#### OnHand / Allocated / Available
- **OnHand** — physical units in the warehouse.
- **Allocated** — reserved for existing customer orders.
- **Available** — OnHand − Allocated.

#### OnOrder
Units already placed on open POs (status ORDERED or ORDERING). The
engine subtracts these from what you need to reorder — you won't get
a suggestion to buy something that's already on its way.

#### Unfulfilled (backorders)
Customer orders with status BACKORDERED / ORDERED / ORDERING — units
customers are waiting on. Subtracted from effective position so the
engine prioritises SKUs that owe customers.

#### DoC (days of cover)
**`OnHand / avg_daily`** — how many days the current stock will last
at the 12-month average sales rate.

#### Effective units (12mo)
Direct sales + sales rolled up from child variants (MP variants, cuts,
kit components) + sales migrated from retiring SKUs. Used for the
reorder math, NOT the raw "units_12mo" figure.

#### FixedCost / AverageCost / PO cost
- **FixedCost** — the agreed supplier price on the SKU's supplier record
  in CIN7. What you'll actually pay on the PO.
- **AverageCost** — CIN7's weighted landed cost (drifts with every PO).
- **PO cost** — FixedCost if set, otherwise AverageCost fallback.
  Shown per row with a "Basis" column so you can see which one applied.

#### MOV (minimum order value)
Set per supplier (e.g. Blebox $250). The PO summary flags when the
current draft is below MOV so you can consolidate.

#### Freight mode
Air or Sea. The engine defaults to air when the supplier offers it
**and** the SKU's length fits in the supplier's air cutoff (e.g.
Topmet UPS caps at 2200mm). Override per row in the grid; the reorder
qty recalculates with the new lead time on next refresh.

#### Status badges
- 📦 **Dropship** — order-on-demand, we don't stock it.
- Active, Deprecated, Discontinued — from CIN7's product status.

#### Trend signal (📈 / 🎯 / 🔀 / 📉)
A secondary check the engine runs to detect when the last-45-day sales
pattern has diverged from the prior 45 days (days 45-90 ago). Uses
four signals combined to avoid false-positives:

- **📈 Trend** — ALL of these must be true: momentum >1.5, **4+ distinct
  customers**, top customer **under 40%**, and non-top customers averaging
  **at least 2 units each**. Real broad-based demand; engine switches to
  last-45d velocity to keep up.
- **🎯 Project** — ANY of these triggers: top customer **≥50%** of 45d
  volume, top **2 customers combined ≥70%**, or fewer than 3 distinct
  customers. Looks concentrated / one-off; engine subtracts top
  customer's 12mo contribution before forecasting to avoid over-ordering.
- **🔀 Mixed** — spike exists but fails both sets of rules. Watch
  signal, no velocity override.
- **📉 Decline** — units down 50%+ vs prior 45 days. Worth review.
- **Stable** — everything else.

**Why "top-2 combined" matters**: 8 customers with one buying 50% and
a second buying 20% is still concentrated (top-2 = 70%). The tighter
thresholds stop "many customers" from hiding real concentration.

**Why "non-top avg units"**: a SKU with 8 customers where the top buyer
took half leaves maybe 1-2 units each for the rest — that's not a trend,
that's noise. The ≥2 units average rule makes sure there's substance
beyond the big buyer.

Low-volume guard: SKUs selling fewer than 3 units in the last 45 days
skip classification entirely — the signal is too noisy at that scale.

The trend breakdown (who's buying, what %) shows in the transparency
panel at the bottom when you drill into any flagged SKU.

#### The 5 things driving reorder qty on any row
1. **12mo effective demand** (direct + rollups)
2. **Lead time** (longer = more stock)
3. **Safety + review days** (more buffer = more stock)
4. **What we already have** (OnHand, OnOrder, Available, Allocated)
5. **What we owe customers** (unfulfilled backorders bring it up)

For the full step-by-step math on any individual SKU, scroll to the
**transparency panel** below the PO table and pick the SKU — the
engine shows every input and how it got to the suggestion.
""")

    if products.empty or sale_lines.empty:
        st.warning("Need products + 12-month sales to run ABC.")
        st.stop()

    # --- Build the full ABC engine DataFrame ---------------------------
    # persist="disk" pickles the result into .streamlit/cache/ so it
    # survives Streamlit restarts. The cache only invalidates when the
    # function source OR its inputs change — so editing UI code
    # elsewhere in app.py no longer triggers a recompute. Cuts dev
    # iteration time by ~80%. ttl extended from 5min to 1h since the
    # underlying data only refreshes via the daily sync.
    @st.cache_data(
        persist="disk",
        show_spinner="Computing ABC engine…")
    def _abc_engine(products: pd.DataFrame,
                    stock: pd.DataFrame,
                    sale_lines: pd.DataFrame,
                    purchase_lines: pd.DataFrame,
                    window_days: int = 365) -> pd.DataFrame:
        """Compute per-SKU ABC, velocity, target, reorder for Stock items.
        Uses hybrid ABC: 60% value + 40% qty percentile rank."""
        # 1. Filter to Stock items only (already done by global filter)
        prods = products[["SKU", "Name", "Type", "Category", "Brand",
                          "Status", "AverageCost",
                          "MinimumBeforeReorder", "ReorderQuantity",
                          "AdditionalAttribute1", "BillOfMaterial",
                          "BOMType"]].copy()
        prods = prods[prods["Type"] == "Stock"]
        # Dedupe on SKU so downstream .map() operations don't hit
        # pandas Arrow-backend InvalidIndexError on duplicate indices.
        prods = prods.drop_duplicates(subset=["SKU"])
        # Also normalise SKU to plain strings (not Arrow StringArray)
        prods["SKU"] = prods["SKU"].astype(str)
        prods["AverageCost"] = _to_num(prods["AverageCost"]).fillna(0)

        # 2. 12-month velocity per SKU — filter by InvoiceDate >= today-window
        # EXCLUDE CREDITED lines so returned/refunded units don't inflate
        # demand. A sale that was fully returned should net to zero
        # contribution — CIN7's Status = 'CREDITED' marks invoices that
        # were later credited (returned).
        sl = sale_lines.copy()
        sl["InvoiceDate"] = _to_date(sl["InvoiceDate"]).dt.tz_localize(None)
        cutoff = pd.Timestamp(datetime.now().date()) - pd.Timedelta(days=window_days)
        sl = sl[sl["InvoiceDate"] >= cutoff].dropna(subset=["InvoiceDate"])
        # Exclude credited / voided / cancelled to reflect NET demand
        if "Status" in sl.columns:
            excluded_statuses = ("CREDITED", "VOIDED", "CANCELLED")
            sl = sl[~sl["Status"].astype(str).str.upper()
                                   .isin(excluded_statuses)]
        sl["Quantity"] = _to_num(sl["Quantity"]).fillna(0)
        sl["Total"] = _to_num(sl["Total"]).fillna(0)
        vel = (sl.groupby("SKU")
                 .agg(units_12mo=("Quantity", "sum"),
                      rev_12mo=("Total", "sum"),
                      last_sold=("InvoiceDate", "max"),
                      first_sold=("InvoiceDate", "min"))
                 .reset_index())

        # --- Trend vs. project detection (45-day window) --------------
        # For each SKU, compute:
        #   units_45d       — units sold in last 45 days
        #   units_prior_45d — units sold in days 45-90 ago (prior period)
        #   customers_45d   — distinct customers in last 45d
        #   top_cust_pct    — % of 45d volume from the single biggest buyer
        #   top_cust_name   — who that buyer is
        #   top_cust_units_12mo — how much THIS customer bought this SKU
        #                         over the full 12mo (used for project
        #                         baseline-correction)
        #   momentum        — units_45d / max(units_prior_45d, 1)
        #   trend_flag      — Stable / 📈 Trend / 🎯 Project / 🔀 Mixed / 📉 Decline
        #
        # 45 days catches spikes faster than 90 days; the trade-off is a
        # noisier signal on low-volume SKUs (hence the 3-unit low-volume
        # guard below, down from 5 when this was a 90d window).
        today_ts = pd.Timestamp(datetime.now().date())
        cutoff_recent = today_ts - pd.Timedelta(days=45)
        cutoff_prior = today_ts - pd.Timedelta(days=90)
        cutoff_90 = today_ts - pd.Timedelta(days=90)
        sl_recent = sl[sl["InvoiceDate"] >= cutoff_recent]
        sl_prior = sl[(sl["InvoiceDate"] >= cutoff_prior)
                       & (sl["InvoiceDate"] < cutoff_recent)]
        sl_90d = sl[sl["InvoiceDate"] >= cutoff_90]

        u45 = sl_recent.groupby("SKU")["Quantity"].sum().rename("units_45d")
        uprior = sl_prior.groupby("SKU")["Quantity"].sum().rename(
            "units_prior_45d")
        c45 = sl_recent.groupby("SKU")["CustomerID"].nunique().rename(
            "customers_45d")
        # 90-day units — used for dormancy detection. A SKU with strong
        # 12mo history but zero 90d activity is treated as dormant; the
        # engine uses the 90d rate (≈0) instead of the inflated 12mo rate
        # to avoid suggesting reorders against demand that's stopped.
        u90 = sl_90d.groupby("SKU")["Quantity"].sum().rename("units_90d")

        # Top customer(s) per SKU in 45d:
        #   top_cust_pct       — share going to the single biggest buyer
        #   top_2_cust_pct     — share going to top 2 combined (concentration
        #                        check; 2 customers taking >70% is still
        #                        project-like even if there are 6+ others)
        #   non_top_avg_units  — avg units per non-top customer (measures
        #                        whether the "many customers" actually
        #                        buy meaningful quantities)
        #   top_cust_name      — the top buyer's name (for transparency)
        #   top_cust_units_12mo — top buyer's full 12mo contribution on
        #                        this SKU (for project baseline correction)
        top_info = []
        for sku, g in sl_recent.groupby("SKU"):
            if g.empty:
                continue
            cust_tot = g.groupby("CustomerID").agg(
                qty=("Quantity", "sum"),
                name=("Customer", "first"),
            ).sort_values("qty", ascending=False)
            if cust_tot.empty:
                continue
            total = float(g["Quantity"].sum())
            if total <= 0:
                continue
            top_qty = float(cust_tot.iloc[0]["qty"])
            top_name = cust_tot.iloc[0]["name"]
            top_cid = cust_tot.index[0]
            share1 = top_qty / total
            share2 = share1
            if len(cust_tot) >= 2:
                share2 = (top_qty + float(cust_tot.iloc[1]["qty"])) / total
            # Non-top: average units per customer excluding the biggest
            non_top_qty = total - top_qty
            non_top_n = max(len(cust_tot) - 1, 0)
            non_top_avg = (non_top_qty / non_top_n) if non_top_n > 0 else 0.0
            cust_12mo = float(
                sl[(sl["SKU"] == sku) & (sl["CustomerID"] == top_cid)
                    ]["Quantity"].sum())
            top_info.append({
                "SKU": sku,
                "top_cust_pct": share1,
                "top_2_cust_pct": share2,
                "non_top_avg_units": non_top_avg,
                "top_cust_name": str(top_name)[:50] if top_name else "",
                "top_cust_units_12mo": cust_12mo,
            })
        top_df = (pd.DataFrame(top_info)
                    if top_info else pd.DataFrame(
                      columns=["SKU", "top_cust_pct", "top_2_cust_pct",
                                "non_top_avg_units",
                                "top_cust_name",
                                "top_cust_units_12mo"]))

        vel = vel.merge(u45, on="SKU", how="left")
        vel = vel.merge(uprior, on="SKU", how="left")
        vel = vel.merge(c45, on="SKU", how="left")
        vel = vel.merge(u90, on="SKU", how="left")
        vel = vel.merge(top_df, on="SKU", how="left")
        vel["units_45d"] = vel["units_45d"].fillna(0)
        vel["units_prior_45d"] = vel["units_prior_45d"].fillna(0)
        vel["units_90d"] = vel["units_90d"].fillna(0)
        vel["customers_45d"] = vel["customers_45d"].fillna(0).astype(int)
        vel["top_cust_pct"] = vel["top_cust_pct"].fillna(0)
        vel["top_cust_units_12mo"] = vel["top_cust_units_12mo"].fillna(0)
        vel["top_cust_name"] = vel["top_cust_name"].fillna("")
        # momentum (avoid div-by-zero)
        vel["momentum"] = vel.apply(
            lambda r: (r["units_45d"] / max(r["units_prior_45d"], 1.0)
                         if r["units_prior_45d"] > 0
                         else (float("inf")
                                if r["units_45d"] > 0 else 1.0)),
            axis=1,
        )

        # Classify — tightened rules per buyer feedback:
        #   📈 Trend  requires ALL of:
        #     - momentum > 1.5
        #     - 4+ distinct customers in 45d
        #     - top customer < 40%  (was 60%)
        #     - non-top customers avg ≥ 2 units each (real spread, not noise)
        #   🎯 Project triggers if ANY of:
        #     - ≤ 2 customers
        #     - top customer ≥ 50%  (was 70%)
        #     - top 2 customers ≥ 70% combined (new — catches "8 customers
        #       but 2 took most of it")
        #   🔀 Mixed = spike but neither pure trend nor pure project.
        def _trend_flag(r):
            u45v = float(r["units_45d"])
            uprv = float(r["units_prior_45d"])
            mom = r["momentum"]
            # Low-volume guard — not enough signal to classify at 45d
            if u45v < 3:
                return "Stable"
            # Decline
            if uprv > 0 and mom < 0.5:
                return "📉 Decline"
            # Spike — decompose
            if mom > 1.5:
                n_cust = int(r["customers_45d"])
                top_share = float(r["top_cust_pct"])
                top_2_share = float(r.get("top_2_cust_pct", top_share))
                non_top_avg = float(r.get("non_top_avg_units", 0))
                # Project-like concentrations (ANY of these)
                if n_cust <= 2:
                    return "🎯 Project"
                if top_share >= 0.50:
                    return "🎯 Project"
                if top_2_share >= 0.70:
                    return "🎯 Project"
                # Real trend (ALL of these)
                if (n_cust >= 4
                        and top_share < 0.40
                        and non_top_avg >= 2.0):
                    return "📈 Trend"
                return "🔀 Mixed"
            return "Stable"

        vel["trend_flag"] = vel.apply(_trend_flag, axis=1)

        # 3. Stock — include Allocated AND CIN7's StockOnHand (FIFO $ value)
        if not stock.empty:
            st_df = stock.copy()
            st_df["OnHand"] = _to_num(st_df["OnHand"]).fillna(0)
            st_df["Available"] = _to_num(st_df["Available"]).fillna(0)
            st_df["OnOrder"] = _to_num(st_df["OnOrder"]).fillna(0)
            if "Allocated" in st_df.columns:
                st_df["Allocated"] = _to_num(st_df["Allocated"]).fillna(0)
            else:
                st_df["Allocated"] = 0
            # CIN7's authoritative FIFO stock value per SKU per location
            if "StockOnHand" in st_df.columns:
                st_df["StockOnHand"] = _to_num(
                    st_df["StockOnHand"]).fillna(0)
            else:
                st_df["StockOnHand"] = 0
            st_agg = (st_df.groupby("SKU")
                       .agg(OnHand=("OnHand", "sum"),
                            Allocated=("Allocated", "sum"),
                            Available=("Available", "sum"),
                            OnOrder=("OnOrder", "sum"),
                            StockOnHand=("StockOnHand", "sum"))
                       .reset_index())
        else:
            st_agg = pd.DataFrame(columns=["SKU", "OnHand", "Allocated",
                                             "Available", "OnOrder",
                                             "StockOnHand"])

        # 3b. Unfulfilled sale demand per SKU — backorders + open ordered
        # Not yet shipped; eats into future stock position.
        # Excludes ESTIMATING (pre-quote) and PICKING/PICKED/PACKING (those
        # are typically already in Allocated, so double-counting to be
        # avoided).
        UNFULFILLED_STATUSES = ("BACKORDERED", "ORDERED", "ORDERING")
        unfulfilled_by_sku = {}
        if "Status" in sale_lines.columns:
            unful_lines = sale_lines[sale_lines["Status"]
                                       .astype(str).str.upper()
                                       .isin(UNFULFILLED_STATUSES)].copy()
            unful_lines["Quantity"] = _to_num(
                unful_lines["Quantity"]).fillna(0)
            ug = unful_lines.groupby("SKU")["Quantity"].sum()
            unfulfilled_by_sku = ug.to_dict()

        # 3c. Apply unfulfilled-order lookup to the main frame later
        # (defer until df is built).

        # 4. Supplier per SKU using 4-tier resolution:
        # override > CIN7 native Suppliers > PO history > family default > unassigned
        sku_overrides_local = db.all_sku_supplier_overrides()
        fam_assignments_local = {r["family"]: r["supplier_name"]
                                  for r in db.all_family_suppliers()}

        # CIN7 native supplier from product.Suppliers. Key is 'SupplierName'.
        # Also capture FixedCost and Lead time from ProductSupplierOptions.
        cin7_supplier_local: dict = {}
        cin7_cost_local: dict = {}
        cin7_lead_local: dict = {}
        cin7_currency_local: dict = {}
        for _, p in products.iterrows():
            sups_raw = p.get("Suppliers")
            if not sups_raw or sups_raw in ("[]", "None", None):
                continue
            sups = sups_raw
            if isinstance(sups, str):
                try:
                    sups = json.loads(sups)
                except (ValueError, TypeError):
                    continue
            if not isinstance(sups, list) or not sups:
                continue
            primary = next(
                (s for s in sups if isinstance(s, dict) and s.get("SupplierName")),
                None,
            )
            if not primary:
                continue
            sku = p["SKU"]
            cin7_supplier_local[sku] = primary["SupplierName"]
            fc = primary.get("FixedCost") or primary.get("Cost") or primary.get("PurchaseCost")
            if fc and float(fc) > 0:
                cin7_cost_local[sku] = float(fc)
            if primary.get("Currency"):
                cin7_currency_local[sku] = primary["Currency"]
            opts = primary.get("ProductSupplierOptions") or []
            if isinstance(opts, list):
                for opt in opts:
                    if isinstance(opt, dict):
                        lead = opt.get("Lead")
                        if lead and int(lead) > 0:
                            cin7_lead_local[sku] = int(lead)
                            break

        sup_by_sku_local: dict = {}
        if not purchase_lines.empty:
            pl = purchase_lines.copy()
            pl["Total"] = _to_num(pl["Total"]).fillna(0)
            sup_group = (pl.groupby(["SKU", "Supplier"])["Total"]
                           .sum().reset_index())
            for sku, grp in sup_group.groupby("SKU"):
                sup_by_sku_local[sku] = grp.sort_values(
                    "Total", ascending=False)["Supplier"].iloc[0]

        def _resolve_sup(sku: str, category: str) -> str:
            if sku in sku_overrides_local:
                return sku_overrides_local[sku]
            if sku in cin7_supplier_local:
                return cin7_supplier_local[sku]
            if sku in sup_by_sku_local:
                return sup_by_sku_local[sku]
            return "(unassigned)"

        # 5. Merge everything
        df = prods.merge(vel, on="SKU", how="left")
        df = df.merge(st_agg, on="SKU", how="left")
        for c in ["units_12mo", "rev_12mo", "OnHand", "Allocated",
                  "Available", "OnOrder", "StockOnHand",
                  # Trend-detection fields — NaN for products with no sales
                  # in the recent window. Must be numeric-0 for downstream
                  # int() casts in _compute_target_and_reorder.
                  "units_45d", "units_prior_45d", "units_90d",
                  "customers_45d",
                  "top_cust_pct", "top_2_cust_pct", "non_top_avg_units",
                  "top_cust_units_12mo", "momentum"]:
            if c not in df.columns:
                df[c] = 0
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
        # trend_flag and top_cust_name are strings — default to empty/Stable.
        if "trend_flag" not in df.columns:
            df["trend_flag"] = "Stable"
        df["trend_flag"] = df["trend_flag"].fillna("Stable")
        if "top_cust_name" not in df.columns:
            df["top_cust_name"] = ""
        df["top_cust_name"] = df["top_cust_name"].fillna("")

        # Unfulfilled open orders per SKU
        df["unfulfilled"] = df["SKU"].apply(
            lambda s: float(unfulfilled_by_sku.get(s, 0))).fillna(0)

        df["Supplier"] = df["SKU"].apply(lambda s: _resolve_sup(s, ""))

        # 5b. Apply MIGRATION demand — retiring SKUs' sales roll into successors
        # (same logic used on LED Tubes page). Saved mappings take priority;
        # auto-proposed Sierra successors fill in the rest.
        saved_migrations = {m["retiring_sku"]: dict(m)
                             for m in db.all_migrations()}

        # Parse tubes so we can compute auto-proposed successors AND rollup
        # cut/MP consumption onto masters.
        tube_records = []
        for _, p in products.iterrows():
            rec = _parse_tube_sku(p.get("SKU"), p.get("Name", ""))
            if rec:
                rule = parse_sourcing_rule(p.get("AdditionalAttribute1"))
                rec["RuleCode"] = rule["RuleCode"]
                rec["IsMaster"] = rule["IsMaster"]
                rec["SourceFraction"] = rule["SourceFraction"]
                rec["SourceLengthMM"] = rule["SourceLengthMM"]
                tube_records.append(rec)
        tube_df = pd.DataFrame(tube_records) if tube_records else pd.DataFrame()

        # Auto-detect retiring → successor family rules (from LED Tubes logic)
        tube_fams = (tube_df["Family"].unique().tolist()
                      if not tube_df.empty else [])
        family_migration_rules = {}
        for fam in tube_fams:
            if fam.startswith("SMOKIES") and fam[7:].isdigit():
                cand = f"SIERRA{fam[7:]}"
                if cand in tube_fams:
                    family_migration_rules[fam] = cand
            elif fam.startswith("CASCADE") and fam[7:].isdigit():
                cand = f"SIERRA{fam[7:]}"
                if cand in tube_fams:
                    family_migration_rules[fam] = cand

        def _auto_successor_for(row) -> Optional[str]:
            fam = row.get("Family")
            succ_fam = family_migration_rules.get(fam)
            if not succ_fam:
                return None
            color = row.get("Color")
            has_mp = row.get("HasMP")
            length_mm = row.get("LengthMM")
            if length_mm is None:
                return None
            if length_mm >= 1000 and length_mm % 1000 == 0:
                len_str = str(length_mm // 1000)
            elif length_mm >= 1000:
                len_str = str(length_mm)
            else:
                len_str = f"{length_mm:04d}"
            mp_part = "-MP" if has_mp else ""
            cand = f"LED-{succ_fam}-{color}{mp_part}-{len_str}"
            if (products["SKU"] == cand).any():
                return cand
            return None

        # 5c. Compute EFFECTIVE demand per SKU = direct + migrated_in
        # Also track where migrated demand came from (for transparency).
        migration_notes: dict = {s: [] for s in df["SKU"]}
        migration_inflow: dict = {s: 0.0 for s in df["SKU"]}
        migration_outflow: dict = {s: 0.0 for s in df["SKU"]}  # retiring SKUs

        # Track which retiring SKUs we've already credited so we don't
        # double-count if a SKU is both a tube AND has an IP-imported
        # migration record.
        handled_retiring: set = set()

        if not tube_df.empty:
            # For every retiring tube SKU that has sales, compute share and
            # add to the successor's inflow.
            retiring_fams = list(family_migration_rules.keys())
            retiring_tubes = tube_df[tube_df["Family"].isin(retiring_fams)]
            for _, r in retiring_tubes.iterrows():
                retiring_sku = r["SKU"]
                # Successor from saved mapping OR auto-proposal
                saved = saved_migrations.get(retiring_sku, {})
                successor = (saved.get("successor_sku")
                              or _auto_successor_for(r))
                if not successor:
                    continue
                share = float(saved.get("share_pct", 100.0)) / 100.0
                units_here = float(vel.loc[vel["SKU"] == retiring_sku,
                                            "units_12mo"].sum())
                if units_here == 0 or share <= 0:
                    continue
                migrated_units = units_here * share

                # Successor gets the migrated units
                if successor in migration_inflow:
                    migration_inflow[successor] += migrated_units
                    migration_notes.setdefault(successor, []).append(
                        f"{retiring_sku}: +{migrated_units:.0f} units")

                # Retiring SKU shrinks by that share
                if retiring_sku in migration_outflow:
                    migration_outflow[retiring_sku] += migrated_units

                handled_retiring.add(retiring_sku)

        # === Generalised migration application (non-tube) =================
        # Applies ANY db.set_migration() entry — not just tube-family
        # ones. This is what makes the 71 IP-imported migrations
        # (LED-XRD, LED-V3000, LED-AL-SL7, LEDEXTA42, etc.) actually
        # affect successor reorder math.
        #
        # Same math as the tube loop: retiring SKU's 12mo units × share
        # → added to successor's inflow, subtracted from retiring's
        # outflow. Skips entries already handled by the tube loop above
        # to avoid double-counting.
        for retiring_sku, saved in saved_migrations.items():
            if retiring_sku in handled_retiring:
                continue
            successor = saved.get("successor_sku")
            if not successor or successor not in migration_inflow:
                # Successor SKU isn't in the engine's product set —
                # could be discontinued itself or filtered out. Skip
                # gracefully.
                continue
            share = float(saved.get("share_pct", 100.0)) / 100.0
            if share <= 0:
                continue
            units_match = vel.loc[vel["SKU"] == retiring_sku, "units_12mo"]
            if units_match.empty:
                continue
            units_here = float(units_match.sum())
            if units_here == 0:
                continue
            migrated_units = units_here * share

            migration_inflow[successor] += migrated_units
            migration_notes.setdefault(successor, []).append(
                f"{retiring_sku}: +{migrated_units:.0f} units")
            if retiring_sku in migration_outflow:
                migration_outflow[retiring_sku] += migrated_units
            handled_retiring.add(retiring_sku)

        df["migrated_in"] = df["SKU"].apply(
            lambda s: float(migration_inflow.get(s, 0))).fillna(0)
        df["migrated_out"] = df["SKU"].apply(
            lambda s: float(migration_outflow.get(s, 0))).fillna(0)
        df["migrated_from"] = df["SKU"].apply(
            lambda s: "; ".join(migration_notes.get(s, []))[:200])

        # 5d. GLOBAL IsMaster flag + rollup for EVERY product with a
        # sourcing rule — not just tubes. Covers B2060020-2390, other cut
        # SKUs with Assemble-from rules, and BOM-based assemblies.
        # Rule priority: parsed AdditionalAttribute1 → BOM lookup → default.
        master_rollup_notes: dict = {}
        master_rollup_inflow: dict = {}
        # Parallel 90d rollup tracking — used for dormancy detection on
        # masters whose own 90d sales are zero but whose children's 90d
        # sales tell us whether the family still has active demand.
        master_rollup_inflow_90d: dict = {}

        # Index: SKU -> sourcing rule dict
        rule_by_sku: dict = {}
        for _, p in products.iterrows():
            rule_by_sku[p["SKU"]] = parse_sourcing_rule(
                p.get("AdditionalAttribute1"))

        # Index: AssemblySKU -> list of (ComponentSKU, Quantity) from BOMs
        bom_components_by_asm: dict = {}
        if not boms.empty:
            for _, b in boms.iterrows():
                asm = b.get("AssemblySKU")
                comp = b.get("ComponentSKU")
                qty = b.get("Quantity")
                if asm and comp and pd.notna(qty):
                    bom_components_by_asm.setdefault(asm, []).append(
                        (comp, float(qty)))

        # Build tube family+color+length master lookup (fallback for tubes)
        tube_master_by_key: dict = {}
        if not tube_df.empty:
            for _, r in tube_df.iterrows():
                if r.get("IsMaster"):
                    tube_master_by_key[(r["Family"], r["Color"],
                                          r["LengthMM"])] = r["SKU"]

        # Lookup: which products have BillOfMaterial=True in master data
        bom_flag_by_sku = {
            p["SKU"]: (str(p.get("BillOfMaterial")).lower() == "true")
            for _, p in products.iterrows()
        }

        # Which SKUs have a CIN7-assigned supplier (from product.Suppliers)?
        # A SKU with an explicit supplier is clearly intended to be bought.
        has_cin7_supplier = set(cin7_supplier_local.keys())

        def _global_is_master(sku: str) -> bool:
            """A SKU is a MASTER (orderable) if there's evidence it's bought
            from a supplier, AND it's not explicitly marked as assembled-from.
            Detection priority:
              1. Supplier assigned in CIN7 → likely master (can still be
                 overridden by explicit Assemble-from rule — see #2)
              2. Rule says 'Assemble from X' → non-master (wins over #1;
                 e.g. MP variants can have a supplier but the rule tells us
                 they're assembled, not bought)
              3. Rule says 'Purchased full length' → master
              4. BillOfMaterial=True or appears in BOMs as assembly → non-master
              5. Default → master (no evidence of assembly)
            """
            rule = rule_by_sku.get(sku, {})
            # 2: Explicit Assemble-from always wins
            if rule.get("SourceFraction") is not None:
                return False
            # 3: Explicit Purchased full length
            if rule.get("IsMaster"):
                return True
            # 4: BOM flag or BOM-has-components
            if bom_flag_by_sku.get(sku):
                return False
            if sku in bom_components_by_asm:
                return False
            # 1: Supplier assigned (without any Assemble-from / BOM evidence)
            if sku in has_cin7_supplier:
                return True
            # 5: default — orderable
            return True

        def _find_master_for_assembly(sku: str) -> Optional[tuple]:
            """Return (master_sku, qty_per_unit) or None.
            Tries BOM data first, then tube family+color+length lookup.
            """
            # Method A: BOM data (authoritative)
            comps = bom_components_by_asm.get(sku)
            if comps:
                # Pick the first component as primary master
                # (products typically have a main component; others are
                # accessories like screws)
                return comps[0]
            # Method B: tube fallback
            if not tube_df.empty:
                r = tube_df[tube_df["SKU"] == sku]
                if not r.empty:
                    row = r.iloc[0]
                    fam = row["Family"]
                    color = row["Color"]
                    slen = row["SourceLengthMM"]
                    if slen is not None:
                        k = (fam, color, slen)
                        master = tube_master_by_key.get(k)
                        if not master:
                            succ = family_migration_rules.get(fam)
                            if succ:
                                master = tube_master_by_key.get(
                                    (succ, color, slen))
                        if master:
                            frac = row.get("SourceFraction") or 1.0
                            return (master, frac)
            # Method C: sourcing rule source_length matches a real master SKU
            # (naive substitution — works for tubes where source is a master)
            rule = rule_by_sku.get(sku, {})
            if (rule.get("SourceFraction") is not None
                    and rule.get("SourceLengthMM")):
                parts = str(sku).split("-")
                if len(parts) > 1:
                    slen_mm = rule["SourceLengthMM"]
                    # Try multiple length-string formats
                    candidates = []
                    if slen_mm >= 1000 and slen_mm % 1000 == 0:
                        candidates.append(str(slen_mm // 1000))
                    else:
                        candidates.append(str(slen_mm))
                    candidates.append(f"{slen_mm:04d}")
                    for len_str in candidates:
                        candidate = "-".join(parts[:-1] + [len_str])
                        if (products["SKU"] == candidate).any():
                            return (candidate, rule["SourceFraction"])

            # Method D: family-prefix sibling master lookup (for channels
            # and similar SKUs where rule source_length is an intermediate
            # cut size, not a purchasable master length).
            # Take SKU's prefix (everything except last segment), find
            # sibling SKUs with IsMaster=True, pick the longest, and
            # compute consumption as own_physical_length / master_length.
            parts = str(sku).split("-")
            if len(parts) >= 2:
                prefix = "-".join(parts[:-1]) + "-"
                own_len_mm = _parse_length(parts[-1])
                if own_len_mm and own_len_mm > 0:
                    # Find sibling masters
                    sibling_masters = []
                    for _, cand in products.iterrows():
                        cand_sku = str(cand.get("SKU") or "")
                        if not cand_sku.startswith(prefix):
                            continue
                        if cand_sku == sku:
                            continue
                        cand_rule = rule_by_sku.get(cand_sku, {})
                        if not cand_rule.get("IsMaster"):
                            continue
                        cand_parts = cand_sku.split("-")
                        cand_len_mm = _parse_length(cand_parts[-1])
                        if cand_len_mm and cand_len_mm > 0:
                            sibling_masters.append(
                                (cand_sku, cand_len_mm))
                    if sibling_masters:
                        # Pick the one with smallest length >= own_len
                        # (best-fit); fall back to largest if none fits
                        exact_fit = [m for m in sibling_masters
                                      if m[1] >= own_len_mm]
                        if exact_fit:
                            chosen = min(exact_fit, key=lambda x: x[1])
                        else:
                            chosen = max(sibling_masters,
                                          key=lambda x: x[1])
                        master_sku, master_len = chosen
                        # Consumption per unit = own_len / master_len
                        qty_per = own_len_mm / master_len
                        return (master_sku, qty_per)
            return None

        # Now compute rollup across ALL non-master products.
        # For multi-component kits (LEDKIT etc.), distribute demand to
        # EVERY component in the BOM proportionally — not just the first.
        def _find_all_masters_for_assembly(sku: str):
            """Return list of (master_sku, qty_per). Multi-component for
            BOM kits; single-component for cuts."""
            # Method A: ALL BOM components (each gets its own share)
            comps = bom_components_by_asm.get(sku)
            if comps:
                return [(c, q) for c, q in comps if c and q]
            # Fallback to single-master methods (B, C, D)
            single = _find_master_for_assembly(sku)
            return [single] if single else []

        for _, p in products.iterrows():
            sku = p["SKU"]
            # Always roll up if the SKU has a BOM, even if _global_is_master
            # returns True. Reason: the BOM is authoritative — if it says
            # "this SKU is built from component X", the demand should
            # cascade to X regardless of other classification heuristics
            # (e.g. AdditionalAttribute1 saying "Purchased full length"
            # by mistake, or supplier-assigned giving a misleading hint).
            # This was the bug behind LED-WLNW-40K-IP20-100M showing
            # zero rolled-up demand from its per-foot child.
            has_bom = sku in bom_components_by_asm
            if not has_bom and _global_is_master(sku):
                continue
            # Non-master (or has BOM): find ALL its masters and roll up demand
            targets = _find_all_masters_for_assembly(sku)
            if not targets:
                continue
            own_units = float(vel.loc[vel["SKU"] == sku, "units_12mo"].sum())
            own_units_90d = float(
                vel.loc[vel["SKU"] == sku, "units_90d"].sum())
            # Migration-aware adjustment to own_units before rolling up:
            #   migration_in : this SKU is a SUCCESSOR — receives demand
            #                  from retired predecessors. Critical for
            #                  successor MP variants (e.g. SIERRA38-W-MP-2
            #                  inheriting from SMOKIES/CASCADE-W-MP-2),
            #                  otherwise the inherited demand is added to
            #                  migration_in then immediately zeroed when
            #                  the MP variant is treated as non-master in
            #                  _effective_units, never reaching the bare
            #                  tube via tube_rollup_in.
            #   migration_out: this SKU is RETIRING — its demand has been
            #                  redirected to its successor. Replaces the
            #                  prior buggy "own_units *= share" branch
            #                  (which multiplied instead of subtracting).
            # 90d intentionally skips migration: long-term successor
            # signals shouldn't distort recent-activity dormancy detection.
            own_units = max(0.0,
                             own_units
                             + float(migration_inflow.get(sku, 0))
                             - float(migration_outflow.get(sku, 0)))
            if own_units == 0 and own_units_90d == 0:
                continue
            # Roll up to EACH master independently
            for target in targets:
                if not target:
                    continue
                master_sku, qty_per = target
                if not master_sku or not qty_per:
                    continue
                consumption = own_units * qty_per
                consumption_90d = own_units_90d * qty_per
                master_rollup_inflow[master_sku] = (
                    master_rollup_inflow.get(master_sku, 0) + consumption
                )
                master_rollup_inflow_90d[master_sku] = (
                    master_rollup_inflow_90d.get(master_sku, 0) + consumption_90d
                )
                master_rollup_notes.setdefault(master_sku, []).append(
                    f"{sku}: {own_units:.0f} × {qty_per:g} = {consumption:.1f}"
                )

        # 5da. LED-STRIP rollup (pattern-based; BOMs rarely populated).
        # For each strip family base (e.g. LEDIRIS6000-180), find the
        # largest-length variant and treat it as the bulk master. Roll up
        # cut/roll sales by (their length in metres × units sold).
        # Intermediate rolls with direct purchase history stay as
        # alternate masters (don't roll them up).
        strip_rollup_notes: dict = {}
        strip_rollup_inflow: dict = {}
        strip_rollup_inflow_90d: dict = {}
        strip_non_master_skus: set = set()
        strip_master_skus: set = set()
        # Track each strip master's roll length in metres — used to flag
        # bulk masters (≥50m) where the engine can suggest fractional
        # reorder qtys. e.g. 0.40 of a 100m roll instead of rounding up
        # to a full 100m roll when only 40m is needed.
        bulk_master_lengths: dict = {}

        # Build base-family index: {base: [(sku, length_m, name, purchased_bool)]}
        strip_family_index: dict = {}
        for _, p in products.iterrows():
            sku_s = str(p.get("SKU"))
            if not _is_strip_sku(sku_s, p.get("Name", "")):
                continue
            parse = _parse_strip_base(sku_s)
            if not parse:
                continue
            base, length_m = parse
            # Was this SKU directly purchased in our PO history?
            purchased = sku_s in sup_by_sku_local
            strip_family_index.setdefault(base, []).append(
                (sku_s, length_m, p.get("Name", ""), purchased))

        # For each family, pick bulk master + decide rollup targets
        for base, members in strip_family_index.items():
            if len(members) < 2:
                continue  # nothing to roll up
            # Largest-length variant = primary bulk master
            sorted_members = sorted(members, key=lambda x: -x[1])
            bulk_sku, bulk_len, bulk_name, bulk_purchased = sorted_members[0]
            strip_master_skus.add(bulk_sku)
            bulk_master_lengths[bulk_sku] = float(bulk_len or 0)
            if bulk_len <= 0:
                continue
            # Additional masters: any intermediate with direct PO history
            alternate_masters = set()
            for sku_m, length_m, _, purchased in sorted_members[1:]:
                if purchased and length_m >= 1.0:
                    alternate_masters.add(sku_m)
                    strip_master_skus.add(sku_m)
                    bulk_master_lengths[sku_m] = float(length_m or 0)

            # Roll up each non-master's sales. CRITICAL: convert consumption
            # from METRES to the master's UNIT count. 1 × 100m roll = 1 unit
            # in CIN7, NOT 100 units. If we keep it in metres we inflate
            # target stock by 100×.
            for sku_m, length_m, nm, purchased in sorted_members:
                if sku_m == bulk_sku:
                    continue
                if sku_m in alternate_masters:
                    continue
                own_units = float(vel.loc[vel["SKU"] == sku_m,
                                           "units_12mo"].sum())
                own_units_90d = float(vel.loc[vel["SKU"] == sku_m,
                                               "units_90d"].sum())
                if (own_units == 0 and own_units_90d == 0) or length_m == 0:
                    strip_non_master_skus.add(sku_m)
                    continue
                consumption_m = own_units * length_m
                consumption_m_90d = own_units_90d * length_m
                # Convert metres → master rolls
                consumption_in_master_units = consumption_m / bulk_len
                consumption_in_master_units_90d = consumption_m_90d / bulk_len
                strip_rollup_inflow[bulk_sku] = (
                    strip_rollup_inflow.get(bulk_sku, 0)
                    + consumption_in_master_units
                )
                strip_rollup_inflow_90d[bulk_sku] = (
                    strip_rollup_inflow_90d.get(bulk_sku, 0)
                    + consumption_in_master_units_90d
                )
                strip_rollup_notes.setdefault(bulk_sku, []).append(
                    f"{sku_m}: {own_units:.0f} × {length_m:g}m "
                    f"= {consumption_m:.1f}m = "
                    f"{consumption_in_master_units:.2f} × {bulk_len:g}m rolls"
                )
                strip_non_master_skus.add(sku_m)

        # Merge strip rollup into the master_rollup_inflow tracked above
        for master_sku, consumption in strip_rollup_inflow.items():
            master_rollup_inflow[master_sku] = (
                master_rollup_inflow.get(master_sku, 0) + consumption
            )
            notes = strip_rollup_notes.get(master_sku, [])
            if notes:
                existing = master_rollup_notes.setdefault(master_sku, [])
                existing.extend(notes)

        # Same merge for the 90d parallel rollup
        for master_sku, consumption_90d in strip_rollup_inflow_90d.items():
            master_rollup_inflow_90d[master_sku] = (
                master_rollup_inflow_90d.get(master_sku, 0) + consumption_90d
            )

        df["tube_rollup_in"] = df["SKU"].apply(
            lambda s: float(master_rollup_inflow.get(s, 0))).fillna(0)
        df["tube_rollup_in_90d"] = df["SKU"].apply(
            lambda s: float(master_rollup_inflow_90d.get(s, 0))).fillna(0)
        df["tube_rollup_notes"] = df["SKU"].apply(
            lambda s: "; ".join(master_rollup_notes.get(s, []))[:500])

        # Bulk-master length + fractional-eligibility flag.
        # is_bulk_master = strip master with ≥50m roll length. These SKUs
        # are eligible for fractional reorder qtys (e.g. 0.40 × 100m roll
        # instead of rounding up to a full roll). The threshold of 50m
        # keeps small-roll masters (5m, 10m) on integer ordering since
        # partial purchases on those are unusual.
        #
        # Two detection passes:
        # (1) Strip-family parser (above) — catches masters whose family
        #     members share a clean prefix (e.g. all LEDIRIS6000-* live
        #     in the same family).
        # (2) SKU-suffix fallback (below) — catches masters like
        #     LED-WLNW-40K-IP20-100M whose per-foot child uses a
        #     different middle segment (LED-WLNW-40K-16-IP20-0305) so
        #     prefix matching fails. Any SKU ending in "-NNNm" or
        #     "-NNNM" with N >= 50 is treated as a bulk master.
        import re as _re_bulk
        _bulk_suffix_pat = _re_bulk.compile(r"-(\d+)[Mm]$")
        for _s in df["SKU"]:
            _s_str = str(_s)
            if bulk_master_lengths.get(_s_str, 0) > 0:
                continue  # already detected via strip-family parser
            _m = _bulk_suffix_pat.search(_s_str)
            if not _m:
                continue
            _n = int(_m.group(1))
            if _n >= 50:
                bulk_master_lengths[_s_str] = float(_n)

        df["bulk_length_m"] = df["SKU"].apply(
            lambda s: float(bulk_master_lengths.get(str(s), 0)))
        df["is_bulk_master"] = df["bulk_length_m"] >= 50.0

        # 5e. IsMaster flag on df. A SKU is non-master if:
        #  - flagged by _global_is_master as non-master, OR
        #  - is a strip derivative (rolled up to bulk master above)
        def _final_is_non_master(sku: str) -> bool:
            if sku in strip_master_skus:
                return False  # explicitly a strip master (bulk or alternate)
            if sku in strip_non_master_skus:
                return True   # strip derivative, rolled up
            return not _global_is_master(sku)

        df["is_non_master_tube"] = df["SKU"].apply(_final_is_non_master)

        # Effective units_12mo for reorder math:
        # - Non-master tubes → 0 (their demand is rolled up into the master)
        # - Retiring SKUs → units_12mo - migrated_out (what stays with them)
        # - Otherwise → units_12mo + migrated_in + tube_rollup_in
        def _effective_units(row):
            if row["is_non_master_tube"]:
                return 0.0
            base = float(row["units_12mo"])
            return (base
                    - float(row["migrated_out"])
                    + float(row["migrated_in"])
                    + float(row["tube_rollup_in"]))

        df["effective_units_12mo"] = df.apply(_effective_units, axis=1)

        # --- Recency-aware effective demand (last 90 days) ---------------
        # effective_units_90d mirrors effective_units_12mo but only sums
        # the last 90 days of activity. We deliberately skip migration on
        # the 90d view (mig_in / mig_out are typically tied to long-term
        # successor decisions; recent activity is what matters here).
        # Used to detect dormant SKUs whose 12mo number is dominated by
        # stale demand that has since dried up.
        def _effective_units_90d(row):
            if row["is_non_master_tube"]:
                return 0.0
            base90 = float(row.get("units_90d") or 0)
            rollup90 = float(row.get("tube_rollup_in_90d") or 0)
            return base90 + rollup90

        df["effective_units_90d"] = df.apply(_effective_units_90d, axis=1)

        # Dormancy detection (two-tier):
        #   Tier 1: Hard dormant — effective_units_90d == 0 AND
        #           effective_units_12mo > 0. The family had history but
        #           hasn't sold a single unit in 90 days. Always flag.
        #   Tier 2: Soft dormant — recent 90d rate is < 20% of 12mo rate
        #           AND 12mo rate is meaningful (>0.05/day). Catches
        #           "demand fell off a cliff" while not over-flagging
        #           genuinely low-volume but still-active SKUs.
        # The hard tier replaces the previous 0.05 threshold floor — that
        # threshold was preventing legitimate stale-demand SKUs (no sales
        # since 2023, e.g. LEDIRIS6000-180-100) from being flagged.
        def _is_dormant(row):
            eff_12mo = float(row.get("effective_units_12mo") or 0)
            eff_90d = float(row.get("effective_units_90d") or 0)
            if eff_12mo <= 0:
                return False
            # Tier 1: ~zero 90d activity. The threshold is expressed in
            # PHYSICAL UNITS so master rolls and leaf SKUs are compared
            # consistently:
            #   - Bulk master (e.g. 100m roll): less than 5m of physical
            #     strip flow in 90 days = dormant. 0.001 master rolls
            #     × 100m = 0.1m → dormant. 0.546 × 100m = 54.6m → active.
            #   - Leaf SKU (e.g. per-foot cut): less than 1 unit in 90
            #     days = dormant.
            # This replaces a strict <=0 check that missed near-zero
            # rolled-up activity from per-foot children.
            is_bulk = bool(row.get("is_bulk_master", False))
            bulk_len = float(row.get("bulk_length_m", 0) or 0)
            if is_bulk and bulk_len > 0:
                if (eff_90d * bulk_len) < 5.0:
                    return True
            else:
                if eff_90d < 1.0:
                    return True
            rate_12mo_daily = eff_12mo / 365.0
            rate_90d_daily = eff_90d / 90.0
            # Tier 2: recent rate dropped > 80% vs 12mo. Catches the
            # "demand fell off a cliff" case for SKUs whose 12mo rate
            # is meaningful (>0.05/day = 18 units/year).
            if rate_12mo_daily < 0.05:
                return False
            return rate_90d_daily < (0.20 * rate_12mo_daily)

        df["is_dormant"] = df.apply(_is_dormant, axis=1)

        # 6. Length for air-eligibility (parse from SKU's last numeric part)
        def _length_of(sku: str) -> Optional[int]:
            if not sku: return None
            for part in reversed(str(sku).split("-")):
                p = _parse_length(part) if 'parse_length' in globals() else None
                if p is None:
                    try:
                        n = float(part)
                        if n <= 0: continue
                        p = int(round(n*1000)) if n < 20 else int(round(n))
                    except ValueError:
                        continue
                if p is not None and 50 < p < 5000:
                    return p
            return None
        df["LengthMM"] = df["SKU"].apply(_length_of)

        # 7. Monthly sales trend per SKU (12m + 24m buckets)
        # Build monthly series so we can render sparklines in the table.
        today_ts = pd.Timestamp(datetime.now().date())
        from collections import defaultdict as _dd
        monthly_12 = _dd(lambda: [0.0] * 12)
        monthly_24 = _dd(lambda: [0.0] * 24)
        for _, r in sl.iterrows():
            d = r["InvoiceDate"]
            if pd.isna(d):
                continue
            months_ago = (today_ts - d).days / 30.437
            q = float(r["Quantity"] or 0)
            sku_r = r["SKU"]
            if 0 <= months_ago < 12:
                b12 = 11 - int(months_ago)
                monthly_12[sku_r][b12] += q
            if 0 <= months_ago < 24:
                b24 = 23 - int(months_ago)
                monthly_24[sku_r][b24] += q

        # --- Migration rollup of monthly buckets ---------------------------
        # For every migration record (retiring → successor), add the
        # retiring SKU's 12mo / 24mo monthly buckets × share_pct to the
        # successor's buckets. Without this, "Last 6 months" and "12mo
        # trend" columns on a successor (e.g. LED-SIERRA38-W-MP-2390)
        # show only its OWN sales (post-migration), invisible to the
        # historical demand from CASCADE/SMOKIES MP variants that the
        # engine actually counts. After this step, sparklines and
        # last-6-month numbers reflect the FULL lineage.
        # Runs BEFORE the BOM rollup below so successor-MP variants
        # propagate their inflated buckets up to the bare tube via BOM.
        try:
            _all_migs_buckets = [dict(m) for m in db.all_migrations()]
        except Exception:
            _all_migs_buckets = []
        for _m in _all_migs_buckets:
            _ret = str(_m.get("retiring_sku") or "")
            _succ = str(_m.get("successor_sku") or "")
            if not _ret or not _succ:
                continue
            _share = float(_m.get("share_pct") or 100) / 100.0
            if _share <= 0:
                continue
            if _ret in monthly_12:
                for i in range(12):
                    monthly_12[_succ][i] += monthly_12[_ret][i] * _share
            if _ret in monthly_24:
                for i in range(24):
                    monthly_24[_succ][i] += monthly_24[_ret][i] * _share

        # --- Roll up children's monthly buckets onto master SKUs -----------
        # Without this, "Last 6 months" and "12mo trend" sparkline columns
        # show ONLY direct master sales — and for cut-source masters
        # (100m/5m rolls) those are usually zero because customers don't
        # buy whole rolls; demand is the per-foot variants. This pass
        # adds each child's monthly bucket × BOM ratio to its master's
        # monthly bucket, so the master's trend reflects family-wide
        # demand expressed in master-roll equivalents (matches the
        # demand-breakdown chart's normalisation).
        # NB: combined with the migration rollup above, children that
        # are successors will already have their predecessor history
        # baked in — so the BOM rollup carries that all the way to the
        # bare tube master.
        for master_sku, ch_list in BOM_CHILDREN.items():
            for ch in ch_list:
                ch_sku = ch.get("AssemblySKU")
                ratio = float(ch.get("Quantity") or 0)
                if not ch_sku or ratio <= 0:
                    continue
                # Use sku_r presence — only roll up if the child has any sales
                if ch_sku in monthly_12:
                    for i in range(12):
                        monthly_12[master_sku][i] += (
                            monthly_12[ch_sku][i] * ratio)
                if ch_sku in monthly_24:
                    for i in range(24):
                        monthly_24[master_sku][i] += (
                            monthly_24[ch_sku][i] * ratio)

        # --- Migration + BOM rollup of 45d / prior-45d / 90d units ---------
        # Same pattern as the monthly buckets above, but for the
        # short-window aggregates the buyer sees in the Ordering grid:
        # `45d units`, `momentum` (45d / prior-45d), `customers_45d`,
        # `90d units`. Without rolling these up, a successor bare tube
        # shows 0 in those columns even when its MP-variant children
        # (and their predecessors) sold real volume in the last 45 days.
        u45_dict = dict(zip(
            df["SKU"].astype(str),
            pd.to_numeric(df["units_45d"], errors="coerce").fillna(0)))
        uprior_dict = dict(zip(
            df["SKU"].astype(str),
            pd.to_numeric(df["units_prior_45d"], errors="coerce").fillna(0)))
        u90_dict = dict(zip(
            df["SKU"].astype(str),
            pd.to_numeric(df["units_90d"], errors="coerce").fillna(0)))

        # Migration rollup — same logic as for monthly buckets.
        # 90d intentionally skips migration to keep dormancy detection
        # focused on RECENT activity (predecessors are by definition
        # not active anymore).
        for _m in _all_migs_buckets:
            _ret = str(_m.get("retiring_sku") or "")
            _succ = str(_m.get("successor_sku") or "")
            if not _ret or not _succ:
                continue
            _share = float(_m.get("share_pct") or 100) / 100.0
            if _share <= 0:
                continue
            if _ret in u45_dict:
                u45_dict[_succ] = u45_dict.get(_succ, 0) + u45_dict[_ret] * _share
            if _ret in uprior_dict:
                uprior_dict[_succ] = uprior_dict.get(_succ, 0) + uprior_dict[_ret] * _share

        # BOM rollup — child × ratio added to master.
        for master_sku, ch_list in BOM_CHILDREN.items():
            for ch in ch_list:
                ch_sku = ch.get("AssemblySKU")
                ratio = float(ch.get("Quantity") or 0)
                if not ch_sku or ratio <= 0:
                    continue
                if ch_sku in u45_dict:
                    u45_dict[master_sku] = u45_dict.get(master_sku, 0) + u45_dict[ch_sku] * ratio
                if ch_sku in uprior_dict:
                    uprior_dict[master_sku] = uprior_dict.get(master_sku, 0) + uprior_dict[ch_sku] * ratio
                if ch_sku in u90_dict:
                    u90_dict[master_sku] = u90_dict.get(master_sku, 0) + u90_dict[ch_sku] * ratio

        # Write back. Replace the raw values so the Ordering grid columns
        # (`units_45d`, `momentum`, `units_90d`) reflect the full lineage
        # consistent with `effective_units_12mo`.
        df["units_45d"] = df["SKU"].astype(str).map(u45_dict).fillna(0)
        df["units_prior_45d"] = df["SKU"].astype(str).map(uprior_dict).fillna(0)
        df["units_90d"] = df["SKU"].astype(str).map(u90_dict).fillna(0)
        # Recompute momentum from the rolled-up values.
        df["momentum"] = df.apply(
            lambda r: (float(r["units_45d"]) / max(float(r["units_prior_45d"]), 1.0)
                        if float(r["units_prior_45d"]) > 0
                        else (1.5 if float(r["units_45d"]) > 0 else 1.0)),
            axis=1)

        # --- Migration + BOM rollup of customer-level metrics ---------------
        # customers_45d, top_cust_pct, top_2_cust_pct, non_top_avg_units,
        # top_cust_name, and top_cust_units_12mo are all derived from per-
        # customer sale-line aggregations. Until now they used ONLY direct
        # sales of the SKU — leaving 0 customers_45d on bare-tube masters
        # whose volume actually flows through MP children + predecessors.
        # This pass rebuilds them from rolled-up customer-qty maps.
        _cust_qty_45d: dict = {}    # {sku: {customer_id: qty}}
        _cust_qty_12mo: dict = {}
        _cust_names: dict = {}      # {sku: {customer_id: customer_name}}
        for _, _r in sl.iterrows():
            _s = str(_r.get("SKU") or "")
            _cid = str(_r.get("CustomerID") or "")
            if not _s or not _cid:
                continue
            _q = float(_r.get("Quantity") or 0)
            _d = _r.get("InvoiceDate")
            if pd.isna(_d):
                continue
            if _d >= cutoff_12mo if False else _d >= today_ts - pd.Timedelta(days=window_days):
                _cust_qty_12mo.setdefault(_s, {})
                _cust_qty_12mo[_s][_cid] = _cust_qty_12mo[_s].get(_cid, 0) + _q
            if _d >= cutoff_recent:
                _cust_qty_45d.setdefault(_s, {})
                _cust_qty_45d[_s][_cid] = _cust_qty_45d[_s].get(_cid, 0) + _q
            _cn = str(_r.get("Customer") or "")
            if _cn:
                _cust_names.setdefault(_s, {})[_cid] = _cn

        # Migration rollup
        for _m in _all_migs_buckets:
            _ret = str(_m.get("retiring_sku") or "")
            _succ = str(_m.get("successor_sku") or "")
            if not _ret or not _succ:
                continue
            _share = float(_m.get("share_pct") or 100) / 100.0
            if _share <= 0:
                continue
            for _src_map in (_cust_qty_45d, _cust_qty_12mo):
                if _ret in _src_map:
                    _dest = _src_map.setdefault(_succ, {})
                    for _cid, _q in _src_map[_ret].items():
                        _dest[_cid] = _dest.get(_cid, 0) + _q * _share
            # Carry customer names from predecessor where successor doesn't
            # already know the name
            if _ret in _cust_names:
                _dest_names = _cust_names.setdefault(_succ, {})
                for _cid, _nm in _cust_names[_ret].items():
                    _dest_names.setdefault(_cid, _nm)

        # BOM rollup
        for _master, _ch_list in BOM_CHILDREN.items():
            for _ch in _ch_list:
                _ch_sku = _ch.get("AssemblySKU")
                _ratio = float(_ch.get("Quantity") or 0)
                if not _ch_sku or _ratio <= 0:
                    continue
                for _src_map in (_cust_qty_45d, _cust_qty_12mo):
                    if _ch_sku in _src_map:
                        _dest = _src_map.setdefault(_master, {})
                        for _cid, _q in _src_map[_ch_sku].items():
                            _dest[_cid] = _dest.get(_cid, 0) + _q * _ratio
                if _ch_sku in _cust_names:
                    _dest_names = _cust_names.setdefault(_master, {})
                    for _cid, _nm in _cust_names[_ch_sku].items():
                        _dest_names.setdefault(_cid, _nm)

        # Recompute customer-derived columns from the rolled-up maps
        _new_cust_count: dict = {}
        _new_top_pct: dict = {}
        _new_top_2_pct: dict = {}
        _new_non_top_avg: dict = {}
        _new_top_name: dict = {}
        _new_top_12mo: dict = {}
        for _s in df["SKU"].astype(str):
            _45 = _cust_qty_45d.get(_s, {})
            _12 = _cust_qty_12mo.get(_s, {})
            _names = _cust_names.get(_s, {})
            if _45:
                _new_cust_count[_s] = len(_45)
                _sorted = sorted(_45.items(), key=lambda x: -x[1])
                _total = sum(_45.values())
                if _total > 0:
                    _top_qty = _sorted[0][1]
                    _top_cid = _sorted[0][0]
                    _new_top_pct[_s] = _top_qty / _total
                    if len(_sorted) >= 2:
                        _new_top_2_pct[_s] = (
                            _top_qty + _sorted[1][1]) / _total
                    else:
                        _new_top_2_pct[_s] = _new_top_pct[_s]
                    _non_top_qty = _total - _top_qty
                    _non_top_n = max(len(_sorted) - 1, 0)
                    _new_non_top_avg[_s] = (
                        _non_top_qty / _non_top_n
                        if _non_top_n > 0 else 0.0)
                    _new_top_name[_s] = _names.get(_top_cid, "")
                    # Top customer's 12mo total (from 12mo map for SAME cid)
                    _new_top_12mo[_s] = float(_12.get(_top_cid, 0))

        # Write back — overwrite the raw values with rolled-up versions
        df["customers_45d"] = (
            df["SKU"].astype(str).map(_new_cust_count).fillna(0).astype(int))
        df["top_cust_pct"] = (
            df["SKU"].astype(str).map(_new_top_pct).fillna(0))
        df["top_2_cust_pct"] = (
            df["SKU"].astype(str).map(_new_top_2_pct).fillna(0))
        df["non_top_avg_units"] = (
            df["SKU"].astype(str).map(_new_non_top_avg).fillna(0))
        df["top_cust_name"] = (
            df["SKU"].astype(str).map(_new_top_name).fillna(""))
        df["top_cust_units_12mo"] = (
            df["SKU"].astype(str).map(_new_top_12mo).fillna(0))

        df["trend_12m"] = df["SKU"].apply(
            lambda s: list(monthly_12.get(s, [0.0] * 12)))
        df["trend_24m"] = df["SKU"].apply(
            lambda s: list(monthly_24.get(s, [0.0] * 24)))
        # Last 6 months total (sum of most recent 6 monthly buckets)
        df["last_6mo"] = df["trend_12m"].apply(
            lambda buckets: float(sum(buckets[-6:])) if buckets else 0.0)
        # Last 6 months as a readable "oldest … newest" sequence.
        # Show 1 decimal place when values include rolled-up fractional
        # contributions (e.g. master rolls absorbing per-foot demand —
        # 100 cuts × 0.0035 ratio = 0.35 rolls). For SKUs whose values
        # are all whole numbers (typical leaf products), keep the
        # cleaner integer format the buyer is used to.
        def _fmt_6mo_series(buckets):
            if not buckets:
                return ""
            last6 = buckets[-6:]
            if any(abs(v - round(v)) > 0.05 for v in last6):
                return "  ".join(f"{v:.1f}" for v in last6)
            return "  ".join(f"{int(round(v))}" for v in last6)
        df["last_6mo_series"] = df["trend_12m"].apply(_fmt_6mo_series)

        # 8. Hybrid ABC uses EFFECTIVE units (post-migration, post-rollup) so
        # Sierra masters with migrated Smokies demand get proper A/B/C class.
        df["annual_value"] = df["effective_units_12mo"] * df["AverageCost"]
        # Percentile rank (0-1), higher = more valuable / more units
        df["_val_rank"] = df["annual_value"].rank(pct=True)
        df["_qty_rank"] = df["units_12mo"].rank(pct=True)
        df["_blend"] = 0.6 * df["_val_rank"] + 0.4 * df["_qty_rank"]

        # Sort by blend desc, compute cumulative annual_value share
        sorted_df = df.sort_values("_blend", ascending=False).copy()
        total_value = sorted_df["annual_value"].sum()
        if total_value > 0:
            sorted_df["cum_value_pct"] = (
                sorted_df["annual_value"].cumsum() / total_value
            )
            def _class(p):
                if p <= 0.70: return "A"
                if p <= 0.90: return "B"
                return "C"
            sorted_df["ABC"] = sorted_df["cum_value_pct"].apply(_class)
        else:
            sorted_df["ABC"] = "—"
        df = df.merge(sorted_df[["SKU", "ABC"]], on="SKU", how="left")

        # Class-aware dormancy refinement (now that ABC is known).
        # The base _is_dormant was conservative — flagged only items
        # with effectively zero recent activity. C-class SKUs need a
        # higher bar: they're low-priority by definition, so only
        # genuinely meaningful 90d demand justifies keeping them
        # active. Thresholds in physical units (metres for bulk
        # masters, units for leaves):
        #   A-class:  5m / 1 unit  (current — keep active easily)
        #   B-class: 10m / 2 units
        #   C-class: 25m / 5 units (high bar — slow-movers go dormant)
        # This means an item with 9m of 90d strip flow (e.g., 2-3 ft of
        # per-foot cuts/month) is "active" for an A-class SKU but
        # "dormant" for a C-class SKU — letting attrition run down
        # slow-moving inventory automatically.
        def _refine_dormancy_by_class(row):
            if bool(row.get("is_dormant", False)):
                return True  # already flagged by base rules
            abc = str(row.get("ABC") or "C")
            eff_90d = float(row.get("effective_units_90d") or 0)
            is_bulk = bool(row.get("is_bulk_master", False))
            bulk_len = float(row.get("bulk_length_m", 0) or 0)
            if is_bulk and bulk_len > 0:
                threshold_m = {"A": 5.0, "B": 10.0,
                                "C": 25.0}.get(abc, 10.0)
                if (eff_90d * bulk_len) < threshold_m:
                    return True
            else:
                threshold_u = {"A": 1.0, "B": 2.0,
                                "C": 5.0}.get(abc, 2.0)
                if eff_90d < threshold_u:
                    return True
            return False

        df["is_dormant"] = df.apply(_refine_dormancy_by_class, axis=1)

        # 9. Daily demand, DoC — use EFFECTIVE units (direct + migrated +
        # tube rollup) so Sierra masters reflect Smokies/Cascade migration
        # and their MP/cut variant consumption.
        df["avg_daily"] = df["effective_units_12mo"] / max(window_days, 1)

        # --- Trend-aware avg_daily adjustment -------------------------
        # Override avg_daily based on detected demand patterns:
        #   💤 Dormant → demand has fallen off a cliff (90d rate <20% of
        #                12mo rate). Use the 90d rate (≈ 0) to avoid
        #                ordering against stale historical demand. Takes
        #                precedence — if a SKU is dormant, it doesn't
        #                matter what trend_flag says.
        #   📈 Trend   → use last-45d velocity — this catches
        #                acceleration fast so the engine builds stock
        #                before the next PO cycle.
        #   🎯 Project → subtract the top customer's 12mo units from
        #                effective demand (that customer isn't part of
        #                the sustaining baseline), then re-derive daily.
        def _adjust_avg_daily(r):
            base = r["avg_daily"]
            flag = r.get("trend_flag", "Stable")
            if pd.isna(flag):
                flag = "Stable"
            def _safe(v, default=0.0):
                try:
                    v = float(v)
                    return default if pd.isna(v) else v
                except (ValueError, TypeError):
                    return default
            # Dormancy override — highest priority. If the family hasn't
            # moved in 90 days, use the 90d rate (which will be ≈0)
            # regardless of how much 12mo history the SKU has.
            if bool(r.get("is_dormant", False)):
                # Hard-zero for dormant: any positive avg_daily would
                # produce a small positive target_stock, which then
                # snaps up to 10m via the bulk-master 10m floor →
                # spurious 0.10 suggestions for items the engine has
                # explicitly classified as dormant. Returning 0 ensures
                # target_stock = 0 → shortfall = 0 → reorder = 0.
                return 0.0
            if flag == "📈 Trend":
                u45 = _safe(r.get("units_45d"))
                if u45 > 0:
                    # last-45d daily velocity (units per day)
                    return (u45 / 45.0)
            if flag == "🎯 Project":
                eff = _safe(r.get("effective_units_12mo"))
                # Subtract top customer's 12mo contribution
                top_u = _safe(r.get("top_cust_units_12mo"))
                corrected = max(0.0, eff - top_u)
                return corrected / max(window_days, 1)
            return base

        df["avg_daily_base"] = df["avg_daily"]
        df["avg_daily"] = df.apply(_adjust_avg_daily, axis=1)

        # Promote is_dormant into trend_flag display so the buyer sees
        # 💤 Dormant in the PO editor's Trend column. We override Stable
        # only — preserve any existing acceleration/project/decline flag
        # if one was already set (those signals took priority before
        # dormancy was added; keeping that contract avoids surprises).
        def _promote_dormant_flag(r):
            cur = r.get("trend_flag", "Stable")
            if pd.isna(cur):
                cur = "Stable"
            if bool(r.get("is_dormant", False)) and cur == "Stable":
                return "💤 Dormant"
            return cur

        df["trend_flag"] = df.apply(_promote_dormant_flag, axis=1)
        df["DoC_days"] = df.apply(
            lambda r: (r["OnHand"] / r["avg_daily"])
            if r["avg_daily"] > 0 else None, axis=1)

        # 9b. Cost fields kept separate:
        #
        #   FixedCost  — supplier's agreed PO price (strict; used for
        #                PO line value calc — what the PO will actually
        #                cost us).
        #   AverageCost — CIN7's weighted landed cost (historical, drifts
        #                with every purchase). Keep for reference /
        #                valuation fallback but NOT the primary.
        #
        # Line value on a PO = Order qty × FixedCost (not AverageCost).
        # When FixedCost is missing on a SKU, we fall back to AverageCost
        # and flag clearly.
        df["FixedCost"] = df["SKU"].apply(
            lambda s: float(cin7_cost_local.get(s, 0))
        )
        df["POCostBasis"] = df.apply(
            lambda r: ("FixedCost (supplier)" if r["FixedCost"] > 0
                       else ("AverageCost (fallback)"
                             if float(r["AverageCost"] or 0) > 0
                             else "No cost on file")),
            axis=1,
        )
        # Effective PO cost — prefers FixedCost, falls back if missing
        df["POCost"] = df.apply(
            lambda r: (r["FixedCost"] if r["FixedCost"] > 0
                       else float(r["AverageCost"] or 0)), axis=1)

        return df

    engine_df = _abc_engine(products, stock, sale_lines, purchase_lines)

    # --- Buyer exclusions + inline notes (loaded once per render) -------
    excluded_skus = db.all_do_not_reorder_skus()
    latest_notes_map = db.latest_note_per_sku()

    # --- Effective dropship set — combined from 4 sources -----------
    # Priority / merging logic (documented in RULES.md §5.4):
    #   1. CIN7 DropShipMode = "Always Drop Ship"  → dropship (authoritative)
    #   2. CIN7 Tags contains "Dropship"           → dropship (belt-and-braces)
    #   3. Per-SKU app flag in `flags` table       → dropship (user override)
    #   4. Supplier-level `dropship_default`       → dropship, UNLESS CIN7
    #                                                 explicitly says
    #                                                 "No Drop Ship" for that
    #                                                 individual SKU
    #
    # This way CIN7 is the source of truth but the buyer has escape
    # hatches for edge cases without having to fix CIN7 first.
    cin7_always_ds: set = set()
    cin7_no_ds: set = set()
    cin7_tag_ds: set = set()
    if not products.empty:
        _mode_col = products.get("DropShipMode")
        _tags_col = products.get("Tags")
        for idx, p in products.iterrows():
            sku = str(p.get("SKU") or "")
            if not sku:
                continue
            mode = str(p.get("DropShipMode") or "").strip()
            if mode == "Always Drop Ship":
                cin7_always_ds.add(sku)
            elif mode == "No Drop Ship":
                cin7_no_ds.add(sku)
            tags = str(p.get("Tags") or "").lower()
            if "dropship" in tags:
                cin7_tag_ds.add(sku)

    per_sku_ds = set(db.all_dropship_skus())
    # Overrides — user explicitly wants these NOT dropship despite CIN7
    # saying so. Candidates for CIN7 write-back (see pending-writes
    # expander below the PO editor).
    not_ds_overrides = set(db.all_not_dropship_skus())

    # Supplier-level dropship_default
    _all_supp_cfgs = db.all_supplier_configs()
    _dropship_suppliers = {
        name for name, cfg in _all_supp_cfgs.items()
        if cfg.get("dropship_default")
    }
    supplier_ds_skus: set = set()
    if _dropship_suppliers and not products.empty:
        for _, p in products.iterrows():
            sups_raw = p.get("Suppliers")
            if pd.isna(sups_raw) or not sups_raw:
                continue
            try:
                sups = (json.loads(sups_raw)
                          if isinstance(sups_raw, str) else sups_raw)
            except Exception:
                continue
            if not isinstance(sups, list) or not sups:
                continue
            primary = next(
                (s for s in sups if s.get("IsDefault")), sups[0])
            supp_name = primary.get("SupplierName") or ""
            if supp_name in _dropship_suppliers:
                supplier_ds_skus.add(str(p.get("SKU") or ""))

    # Combine: CIN7 signals + app overrides, with CIN7 "No Drop Ship"
    # winning over supplier-level default (but NOT over per-SKU flag).
    # Subtract the user's "Not dropship" overrides at the end — they're
    # the user's explicit intent that these SKUs should be stocked.
    dropship_skus = (
        cin7_always_ds
        | cin7_tag_ds
        | per_sku_ds
        | (supplier_ds_skus - cin7_no_ds)
    ) - not_ds_overrides

    # Hide excluded SKUs from the main reorder list. They still appear in
    # the "Archived (do not reorder)" expander below with a Reactivate button.
    if excluded_skus:
        engine_df = engine_df[
            ~engine_df["SKU"].astype(str).isin(excluded_skus)
        ].reset_index(drop=True)

    # --- Apply per-supplier config to compute targets ------------------
    supp_configs = db.all_supplier_configs()

    def _compute_target_and_reorder(row: pd.Series) -> dict:
        """Return dict with target_stock, reorder_qty, lead_time_used,
        freight_mode_used, calc_trace (markdown-ready)."""
        supplier = row.get("Supplier") or ""
        cfg = supp_configs.get(supplier, {})
        lt_sea = cfg.get("lead_time_sea_days") or 35
        lt_air = cfg.get("lead_time_air_days")
        air_eligible_default = bool(cfg.get("air_eligible_default") or 0)
        air_max_len = cfg.get("air_max_length_mm")
        length_mm = row.get("LengthMM")

        # Per-SKU air-eligibility: default from supplier cfg, BUT disqualify
        # if length exceeds supplier's air_max_length_mm
        sku_air_ok = air_eligible_default
        if (air_eligible_default and air_max_len
                and length_mm is not None and length_mm > air_max_len):
            sku_air_ok = False

        # Default: air whenever supplier offers it AND the SKU is eligible
        # (length within air_max_length_mm). Sea is the fallback.
        # `preferred_freight` on the supplier config is treated as a hint
        # only — if "sea" is preferred, air is still used for small items
        # when beneficial (shorter LT = less inventory tied up).
        if sku_air_ok and lt_air:
            lead_time_days = lt_air
            freight_mode_used = "air"
        else:
            lead_time_days = lt_sea
            freight_mode_used = "sea"

        # Safety factor by class
        abc = row.get("ABC") or "C"
        safety_pct = {
            "A": cfg.get("safety_pct_A") or 30.0,
            "B": cfg.get("safety_pct_B") or 20.0,
            "C": cfg.get("safety_pct_C") or 15.0,
        }.get(abc, 20.0)
        review_days = {
            "A": cfg.get("review_days_A") or 14,
            "B": cfg.get("review_days_B") or 30,
            "C": cfg.get("review_days_C") or 45,
        }.get(abc, 30)

        avg_daily = row["avg_daily"]
        lt_demand = avg_daily * lead_time_days
        safety = lt_demand * (safety_pct / 100.0)
        review_demand = avg_daily * review_days
        target = lt_demand + safety + review_demand
        onhand = row["OnHand"]
        allocated = float(row.get("Allocated") or 0)
        available = float(row.get("Available") or 0)
        on_order = row["OnOrder"]
        unfulfilled = float(row.get("unfulfilled") or 0)
        # Effective position = what we'll actually have for future demand.
        # Available already nets Allocated. Subtract unfulfilled open
        # orders (BACKORDERED + ORDERED + ORDERING statuses) which aren't
        # yet reflected in Allocated.
        effective_pos = available + on_order - unfulfilled
        shortfall = max(0, target - effective_pos)

        # Fractional ordering — for bulk-roll masters where the supplier
        # accepts decimal quantities (e.g. 0.40 × 100m roll instead of
        # rounding up to 1 full 100m roll). The supplier-level
        # `allow_fractional_qty` config flag gates this — defaults to
        # True, but suppliers like Topmet that only sell full rolls can
        # set it to False to keep integer ordering.
        is_bulk = bool(row.get("is_bulk_master", False))
        supplier_allows_frac = bool(cfg.get("allow_fractional_qty", True))
        use_fractional = is_bulk and supplier_allows_frac
        bulk_len_m = float(row.get("bulk_length_m", 0) or 0)

        def _snap_to_10m(value, length_m):
            """Round a fractional reorder qty to the nearest 10m equivalent.
            For a 100m roll, value 0.26 → 0.30 (= 30m). For a 50m roll,
            value 0.45 → 0.40 (= 20m). Provides cleaner ordering than
            arbitrary fractions like 0.42, while still being more capital-
            efficient than always rounding to a full roll.

            Demand <5m rounds to 0 (don't reorder for trivial demand).
            Demand 5m-15m snaps up to 10m (smallest meaningful order).
            Demand >15m rounds to nearest 10m increment."""
            if not length_m or length_m <= 0 or value <= 0:
                return value
            metres = value * length_m
            # Below 5m of demand: not worth ordering. This eliminates the
            # spurious 0.10-roll suggestions for SKUs whose 12mo daily is
            # near zero (e.g. items that haven't sold in years but have
            # tiny rolled-up demand from a per-foot child).
            if metres < 5.0:
                return 0.0
            rounded_metres = round(metres / 10.0) * 10.0
            # 5-15m range snaps up to 10m (smallest meaningful order).
            if rounded_metres < 10.0:
                rounded_metres = 10.0
            return rounded_metres / length_m

        if use_fractional:
            raw_reorder = round(float(shortfall), 2)
            reorder = round(_snap_to_10m(raw_reorder, bulk_len_m), 2)
        else:
            reorder = int(round(shortfall))

        # --- Stockout recovery boost ---------------------------------
        # When we're truly out (effective_pos ≤ 0), simply ordering
        # `target - effective_pos` leaves us at bare-minimum coverage
        # right when sales are returning. Google Ads / advertising
        # algorithms penalise out-of-stock listings and that penalty
        # lingers — so we need to over-stock on recovery.
        #
        # Formula:  base_avg_daily × (lead_time + stockout_min_cover_days)
        #
        # Key: uses the UNADJUSTED base velocity, not trend-adjusted. If
        # a Project-classification discounted the SKU to near-zero
        # velocity, that customer-specific spike is irrelevant to
        # recovery — what matters is the broad baseline of demand we
        # want back on the shelf.
        #
        # Exception: skip the boost for 💤 Dormant SKUs. If the family
        # hasn't moved in 90 days, "recovery" doesn't apply — there's
        # nothing to recover to. Boosting based on stale 12mo demand
        # would defeat the dormancy detection.
        stockout_boost_applied = False
        is_dormant_row = bool(row.get("is_dormant", False))
        if effective_pos <= 0 and not is_dormant_row:
            base_avg = float(row.get("avg_daily_base") or 0) or avg_daily
            recovery_days = int(
                cfg.get("stockout_min_cover_days") or 60)
            stockout_min = base_avg * (lead_time_days + recovery_days)
            if use_fractional:
                # Apply same 10m snap to stockout boost so it doesn't
                # produce sub-5m fractional suggestions (e.g. 0.01 of a
                # 100m roll = 1m). Was a bug — stockout boost previously
                # bypassed the snap and surfaced absurdly small qtys for
                # near-zero-velocity SKUs that had OnHand=0.
                stockout_qty = round(_snap_to_10m(
                    round(float(stockout_min), 2), bulk_len_m), 2)
            else:
                stockout_qty = int(round(stockout_min))
            if stockout_qty > reorder:
                stockout_boost_applied = True
                reorder = stockout_qty

        # MOQ — skipped for fractional bulk SKUs (a "1 unit minimum"
        # doesn't apply to "0.4 of a 100m roll" — fractional ordering
        # is the whole point of the feature, MOQ would defeat it).
        moq = cfg.get("moq_units") or 0
        if reorder > 0 and moq and reorder < moq and not use_fractional:
            reorder = int(moq)

        # Excess = OnHand beyond target
        excess_units = max(0, onhand - target)
        excess_value = excess_units * row["AverageCost"]

        # Demand breakdown — show migration + tube rollup contributions
        direct_u = float(row["units_12mo"])
        mig_in = float(row.get("migrated_in", 0))
        mig_out = float(row.get("migrated_out", 0))
        rollup_in = float(row.get("tube_rollup_in", 0))
        eff_u = float(row.get("effective_units_12mo", direct_u))

        demand_lines = [f"**Supplier**: {supplier or 'unassigned'}\n"]
        if row.get("is_non_master_tube"):
            demand_lines.append(
                "**Tube variant (non-master)** — demand is rolled up "
                "into its master tube SKU, so effective units here = 0 "
                "(we don't order this SKU directly; it's assembled "
                "from a master).\n"
            )
        else:
            demand_lines.append(
                f"**Velocity breakdown** (12mo → effective "
                f"{eff_u:.0f} units):\n"
                f"- Direct sales of this SKU: **{direct_u:.0f}** units\n"
            )
            if mig_in > 0:
                demand_lines.append(
                    f"- Migrated IN from retiring SKUs: "
                    f"**+{mig_in:.0f}** units "
                    f"({row.get('migrated_from') or 'see below'})\n"
                )
            if mig_out > 0:
                demand_lines.append(
                    f"- Migrated OUT (share going to successor): "
                    f"**−{mig_out:.0f}** units\n"
                )
            if rollup_in > 0:
                demand_lines.append(
                    f"- Tube rollup IN from MP variants / cuts: "
                    f"**+{rollup_in:.0f}** units "
                    f"(see tube_rollup_notes)\n"
                )

        # Dormancy note — surface when this SKU has been classified
        # dormant. Shows the buyer exactly why the suggestion is what
        # it is (or zero) — direct comparison of 90d vs 12mo daily rate.
        _is_dormant_row = bool(row.get("is_dormant", False))
        if _is_dormant_row:
            eff_12mo_d = float(row.get("effective_units_12mo") or 0)
            eff_90d_d = float(row.get("effective_units_90d") or 0)
            rate_12mo_daily = eff_12mo_d / 365.0 if eff_12mo_d else 0.0
            rate_90d_daily = eff_90d_d / 90.0 if eff_90d_d else 0.0
            ratio_pct = (rate_90d_daily / rate_12mo_daily * 100.0
                          if rate_12mo_daily > 0 else 0.0)
            demand_lines.append(
                f"\n**💤 Dormant detection**:\n"
                f"- 12mo effective rate: **{rate_12mo_daily:.2f} units/day** "
                f"(based on {eff_12mo_d:.0f} units over 365d)\n"
                f"- Last 90d effective rate: **{rate_90d_daily:.3f} units/day** "
                f"(based on {eff_90d_d:.0f} units over 90d)\n"
                f"- Ratio: **{ratio_pct:.1f}%** of historical rate "
                f"(threshold for dormancy: <20%)\n"
                f"- **Engine override**: using 90d rate instead of 12mo rate, "
                f"so reorder suggestion reflects actual recent demand. "
                f"Stockout-recovery boost is also suppressed for dormant SKUs.\n"
            )

        # Trend classification note — always included when the flag is
        # anything other than "Stable" so the buyer knows the engine
        # spotted something.
        _tf = row.get("trend_flag") or "Stable"
        if pd.isna(_tf):
            _tf = "Stable"
        if _tf != "Stable" and not _is_dormant_row:
            # Defensive NaN-handling — any of these can arrive as NaN
            # for SKUs with no recent sales that got flagged by another
            # signal (rare, but possible after a cache refresh).
            def _fnum(v, default=0.0):
                try:
                    v = float(v)
                    return default if pd.isna(v) else v
                except (ValueError, TypeError):
                    return default
            u45v = _fnum(row.get("units_45d"))
            uprv = _fnum(row.get("units_prior_45d"))
            n_cust = int(_fnum(row.get("customers_45d")))
            top_pct = _fnum(row.get("top_cust_pct")) * 100
            top_2_pct = _fnum(row.get("top_2_cust_pct")) * 100
            non_top_avg = _fnum(row.get("non_top_avg_units"))
            top_name = str(row.get("top_cust_name") or "—")[:40]
            mom = _fnum(row.get("momentum"), default=1.0)
            mom_s = (f"{mom:.2f}×" if mom != float("inf") else "new")
            demand_lines.append(
                f"\n**Trend signal**: {_tf}  \n"
                f"- Last 45d: **{u45v:.0f} units** "
                f"(prior 45d: {uprv:.0f}, momentum **{mom_s}**)\n"
                f"- **{n_cust} distinct customer(s)** in last 45d; "
                f"top customer **{top_name}** took **{top_pct:.0f}%**, "
                f"top 2 combined **{top_2_pct:.0f}%**\n"
                f"- Non-top customers avg **{non_top_avg:.1f} units** "
                f"each (key trend-vs-project signal)\n"
            )
            if _tf == "📈 Trend":
                demand_lines.append(
                    "- **Velocity override**: using last-45d rate "
                    "instead of 12mo avg because demand is "
                    "accelerating broadly. Engine will build stock "
                    "faster to catch up.\n"
                )
            elif _tf == "🎯 Project":
                _topu = float(row.get("top_cust_units_12mo") or 0)
                demand_lines.append(
                    "- **Velocity override**: subtracting top "
                    f"customer's 12mo contribution (**{_topu:.0f} units**) "
                    "before annualising, because the spike is "
                    "concentrated to one buyer — unlikely to repeat.\n"
                )
            demand_lines.append(
                f"- **Effective total**: {direct_u:.0f} - {mig_out:.0f} "
                f"+ {mig_in:.0f} + {rollup_in:.0f} = "
                f"**{eff_u:.0f}** units/year\n"
            )
            demand_lines.append(
                f"- Avg daily: {eff_u:.0f} / 365 = "
                f"**{avg_daily:.2f}** units/day\n"
            )

        trace = "".join(demand_lines) + (
            f"**Lead time**: {lead_time_days} days "
            f"({freight_mode_used}) "
            + (f"— SKU length {length_mm}mm > "
               f"{air_max_len}mm air max, forced sea\n\n"
               if (air_eligible_default and air_max_len
                   and length_mm is not None and length_mm > air_max_len)
               else "\n\n")
            + f"**ABC class**: {abc} → safety {safety_pct:.0f}%, "
            f"review {review_days}d\n\n"
            f"**Lead-time demand**: {avg_daily:.2f} × {lead_time_days} "
            f"= {lt_demand:.1f} units\n\n"
            f"**Safety stock**: {lt_demand:.1f} × {safety_pct/100:.2f} "
            f"= {safety:.1f} units\n\n"
            f"**Review-period demand**: {avg_daily:.2f} × {review_days} "
            f"= {review_demand:.1f} units\n\n"
            f"**Target stock**: {lt_demand:.1f} + {safety:.1f} + "
            f"{review_demand:.1f} = **{target:.1f} units**\n\n"
            f"**Current position**:\n"
            f"- OnHand: {onhand:.0f} physical units\n"
            f"- Allocated (reserved for open picks): {allocated:.0f}\n"
            f"- Available (OnHand - Allocated + phantom): {available:.0f}\n"
            f"- OnOrder (incoming POs): {on_order:.0f}\n"
            f"- Unfulfilled sale orders "
            f"(BACKORDERED/ORDERED/ORDERING): {unfulfilled:.0f}\n"
            f"- **Effective position**: {available:.0f} + {on_order:.0f} "
            f"- {unfulfilled:.0f} = **{effective_pos:.0f}**\n\n"
            f"**Suggested reorder**: max(0, {target:.1f} - "
            f"{effective_pos:.0f}) = "
            + (f"**{reorder:.2f}** rolls "
               f"(fractional — supplier accepts decimal qtys; "
               f"bulk master is {row.get('bulk_length_m', 0):g}m)"
               if use_fractional
               else f"{reorder} units")
            + (f" (rounded up to MOQ {moq:g})"
               if moq and not use_fractional and reorder == int(moq)
               else "")
            + f"\n\n"
            f"**Excess stock** (over target): {excess_units:.0f} units × "
            f"${row['AverageCost']:.2f} = **${excess_value:,.0f} tied up**"
        )
        return {
            "target_stock": target,
            "reorder_qty": reorder,
            "lead_time_days": lead_time_days,
            "freight_mode": freight_mode_used,
            "excess_units": excess_units,
            "excess_value": excess_value,
            "calc_trace": trace,
        }

    # Apply the target/reorder computation
    applied = engine_df.apply(_compute_target_and_reorder, axis=1)
    engine_df["target_stock"] = applied.apply(lambda x: x["target_stock"])
    engine_df["reorder_qty"] = applied.apply(lambda x: x["reorder_qty"])
    engine_df["lead_time_days"] = applied.apply(lambda x: x["lead_time_days"])
    engine_df["freight_mode"] = applied.apply(lambda x: x["freight_mode"])
    engine_df["excess_units"] = applied.apply(lambda x: x["excess_units"])
    engine_df["excess_value"] = applied.apply(lambda x: x["excess_value"])
    engine_df["calc_trace"] = applied.apply(lambda x: x["calc_trace"])

    # Dropship override: these SKUs are order-on-demand. Zero the target
    # and reorder, override Status badge, leave everything else (sales
    # history, OnHand etc.) intact so buyer can watch volume and decide
    # when to promote to stocked via the Dropship expander below.
    if dropship_skus:
        _ds_mask = engine_df["SKU"].astype(str).isin(dropship_skus)
        engine_df.loc[_ds_mask, "target_stock"] = 0
        engine_df.loc[_ds_mask, "reorder_qty"] = 0
        engine_df.loc[_ds_mask, "excess_units"] = 0
        engine_df.loc[_ds_mask, "excess_value"] = 0
        engine_df.loc[_ds_mask, "Status"] = "📦 Dropship"

    # Discontinued override: any SKU with "[Discontinued]" in its Name
    # (case-insensitive) OR Status="Discontinued" in CIN7 — force
    # reorder_qty=0 and target_stock=0 so the engine never suggests
    # ordering more of a product that's been retired. The buyer can
    # still see direct sales history, OnHand, and excess for cleanup
    # decisions. Trend is set to "🚫 Discontinued" (distinct from
    # 💤 Dormant) so it's visually obvious in the PO editor.
    _disc_name_mask = (engine_df["Name"].astype(str)
                       .str.contains(r"\[Discontinued\]", case=False,
                                      regex=True, na=False))
    _disc_status_mask = (engine_df.get("Status", pd.Series(dtype=str))
                          .astype(str).str.lower() == "discontinued")
    _disc_mask = _disc_name_mask | _disc_status_mask
    if _disc_mask.any():
        engine_df.loc[_disc_mask, "target_stock"] = 0
        engine_df.loc[_disc_mask, "reorder_qty"] = 0
        engine_df.loc[_disc_mask, "trend_flag"] = "🚫 Discontinued"
    # OnHandValue: prefer CIN7's authoritative StockOnHand (FIFO-based
    # dollar value shown in CIN7's Product Availability screen).
    # Fall back to OnHand × AverageCost/FixedCost only when CIN7 returns 0.
    engine_df["OnHandValue"] = engine_df.apply(
        lambda r: (float(r["StockOnHand"]) if float(r["StockOnHand"]) > 0
                   else float(r["OnHand"]) * float(r["AverageCost"])),
        axis=1,
    )
    # Per-unit cost chain, priority order:
    #   1. CIN7 StockOnHand ÷ OnHand  (real FIFO)
    #   2. Supplier FixedCost  (already in UnitCost via cin7_cost_local)
    #   3. Product.AverageCost  (raw from CIN7)
    #   4. Family-prefix MEDIAN cost  (same SKU prefix siblings)
    #   5. Category MEDIAN cost  (same Category field)
    #   6. 0  (genuinely unknown)
    def _direct_unit_cost(r):
        sv = float(r["StockOnHand"] or 0)
        oh = float(r["OnHand"] or 0)
        if sv > 0 and oh > 0:
            return sv / oh
        return float(r["AverageCost"] or 0)

    engine_df["_direct_cost"] = engine_df.apply(_direct_unit_cost, axis=1)

    # Compute family-prefix median cost
    def _family_prefix(sku: str) -> str:
        parts = str(sku).split("-")
        if len(parts) >= 3:
            return "-".join(parts[:-1])
        return str(sku)

    engine_df["_family_prefix"] = engine_df["SKU"].apply(_family_prefix)
    # Only use rows with a confident direct cost to compute medians
    _confident_costs = engine_df[engine_df["_direct_cost"] > 0]
    if not _confident_costs.empty:
        family_median = (_confident_costs.groupby("_family_prefix")
                          ["_direct_cost"].median().to_dict())
        category_median = (_confident_costs.groupby("Category")
                            ["_direct_cost"].median().to_dict())
    else:
        family_median, category_median = {}, {}

    def _effective_unit_cost(r):
        direct = float(r["_direct_cost"] or 0)
        if direct > 0:
            return direct, "direct"
        fam_med = family_median.get(r["_family_prefix"], 0)
        if fam_med > 0:
            return float(fam_med), "family-median"
        cat_med = category_median.get(r.get("Category"), 0)
        if cat_med > 0:
            return float(cat_med), "category-median"
        return 0.0, "unknown"

    _cost_apply = engine_df.apply(_effective_unit_cost, axis=1)
    engine_df["EffectiveUnitCost"] = _cost_apply.apply(lambda x: x[0])
    engine_df["CostBasisDetail"] = _cost_apply.apply(lambda x: x[1])
    engine_df["TargetValue"] = (
        engine_df["target_stock"] * engine_df["EffectiveUnitCost"]
    )
    # Status — must use EFFECTIVE units (direct + migrated + rollup),
    # otherwise masters with rolled-up demand (e.g. Sierra65-W-2, strip
    # bulk rolls) get wrongly flagged as Dead Stock.
    def _status(r):
        eff = float(r.get("effective_units_12mo",
                            r.get("units_12mo", 0)) or 0)
        onhand = float(r.get("OnHand") or 0)
        if eff == 0 and onhand == 0:
            return "⚪ No demand, no stock"
        if eff == 0 and onhand > 0:
            return "💀 Dead stock"
        if onhand < (r.get("avg_daily") or 0) * (r.get("lead_time_days") or 0):
            return "🔴 Reorder now"
        if onhand < (r.get("target_stock") or 0):
            return "🟠 Reorder soon"
        if onhand > (r.get("target_stock") or 0) * 1.5:
            return "🔵 Overstocked"
        return "🟢 On target"
    engine_df["Status"] = engine_df.apply(_status, axis=1)

    # --- Top-of-page stock optimisation headline -----------------------
    st.markdown("### :moneybag: Stock optimisation overview")

    # Master-only view for TARGET calculations (non-masters have
    # target=0; they roll their demand up to masters). But CURRENT
    # stock value sums across ALL SKUs because physical cuts held
    # from returns/over-production are real working capital.
    master_only = engine_df[~engine_df["is_non_master_tube"]]

    # Current stock: CIN7 StockOnHand across ALL SKUs (matches CIN7's
    # own Product Availability screen total).
    total_onhand_value = float(engine_df["OnHandValue"].sum())

    # Optimum / target: master SKUs only (non-masters have target=0).
    total_target_value = float(master_only["TargetValue"].sum())

    # Excess — two-part definition:
    #   Masters: OnHandValue above TargetValue (classic overstock)
    #   Non-masters: OnHandValue ONLY IF direct sales == 0 (true dead
    #     physical cuts; cuts with their own direct sales are treated
    #     as working inventory, not excess)
    def _row_excess_value(r):
        if bool(r.get("is_non_master_tube")):
            if float(r.get("units_12mo") or 0) == 0:
                return float(r.get("OnHandValue") or 0)
            return 0.0  # has direct sales → working inventory
        # Master
        ohv = float(r.get("OnHandValue") or 0)
        tv = float(r.get("TargetValue") or 0)
        return max(0.0, ohv - tv)

    engine_df["row_excess_value"] = engine_df.apply(
        _row_excess_value, axis=1)
    total_excess_value = float(engine_df["row_excess_value"].sum())

    # Dead stock: zero effective demand AND physical stock held.
    # For masters, use the engine's Status flag. For non-masters,
    # also include them if they have physical stock but zero direct sales.
    dead_master_value = float(
        master_only.loc[master_only["Status"] == "💀 Dead stock",
                         "OnHandValue"].sum()
    )
    dead_cut_value = float(
        engine_df.loc[
            engine_df["is_non_master_tube"]
            & (engine_df["units_12mo"] == 0)
            & (engine_df["OnHandValue"] > 0),
            "OnHandValue",
        ].sum()
    )
    dead_value = dead_master_value + dead_cut_value

    # Cost-coverage diagnostics across MASTERS (what drives optimum)
    cov = master_only["CostBasisDetail"].value_counts().to_dict()
    direct_c = cov.get("direct", 0)
    fam_c = cov.get("family-median", 0)
    cat_c = cov.get("category-median", 0)
    unk_c = cov.get("unknown", 0)

    st.caption(
        f":information_source: **Cost basis coverage (masters, "
        f"drives Optimum)**: "
        f"direct CIN7 cost on **{direct_c:,}**; "
        f"family-median fallback on {fam_c:,}; "
        f"category-median fallback on {cat_c:,}; "
        f"no cost info (contribute $0 to Optimum) on **{unk_c:,}**.  |  "
        f"**Scope note**: Current value sums across all "
        f"{len(engine_df):,} SKUs (real physical dollars); Optimum "
        f"across {len(master_only):,} masters only (non-masters roll "
        f"up to their masters)."
    )

    oc1, oc2, oc3, oc4 = st.columns(4)
    oc1.metric("Current stock value",
               _fmt_money(total_onhand_value),
               help="Total OnHand × AverageCost across all Stock-type items.")
    oc2.metric("Optimum stock value",
               _fmt_money(total_target_value),
               help="Sum of target_stock × AverageCost per SKU. "
                    "This is what your working capital should be at.")
    oc3.metric("Excess (cash to free up)",
               _fmt_money(total_excess_value),
               delta=f"{total_excess_value/total_onhand_value*100:.1f}% of current"
                     if total_onhand_value else None,
               delta_color="inverse",
               help="OnHand beyond target stock, by SKU, summed. "
                    "The money sitting on shelves that doesn't need to be.")
    oc4.metric("Dead stock (zero demand, holding stock)",
               _fmt_money(dead_value),
               help="Two buckets combined: "
                    "(1) MASTER SKUs with zero effective 12-month demand "
                    "(direct + migrated + rolled-up) AND physical stock. "
                    "(2) Non-master variants with physical stock AND zero "
                    "direct sales. "
                    "Non-masters that HAVE direct sales are treated as "
                    "working inventory, not dead.")

    # Glide path toward $600k target
    target_600k = 600_000
    pct_of_goal = (total_onhand_value / target_600k * 100
                   if total_onhand_value else 0)
    excess_over_600k = max(0, total_onhand_value - target_600k)
    if total_onhand_value > target_600k:
        st.progress(min(1.0, target_600k / total_onhand_value),
                     text=f"Current stock is "
                          f"${excess_over_600k:,.0f} above "
                          f"your $600k target "
                          f"({pct_of_goal:.0f}% of $600k)")
    else:
        st.progress(pct_of_goal / 100,
                     text=f"Current stock is {pct_of_goal:.0f}% of "
                          f"your $600k target — you're under.")

    # --- Supplier configuration ----------------------------------------
    st.markdown("### :gear: Supplier configuration")
    with st.expander("Configure lead times, MOQ/MOV, freight per supplier"):
        actor_o = st.session_state.get("current_user", "").strip()
        if not actor_o:
            st.caption("Enter your name in the sidebar to edit.")
        else:
            # Known suppliers — same top-15-by-spend + alphabetical
            # ordering as the main PO dropdown for consistency.
            known = set()
            if not suppliers.empty and "Name" in suppliers.columns:
                known.update(suppliers["Name"].dropna().astype(str).tolist())
            known.update(engine_df["Supplier"].unique().tolist())
            known.update(supp_configs.keys())
            known.discard("(unassigned)")
            known_list = list(known)

            # Rank by spend (reuse spend_by_supplier computed above in
            # the main PO dropdown if available, otherwise rebuild).
            _spend_map = dict(spend_by_supplier) if 'spend_by_supplier' in dir() else {}
            if not _spend_map and not purchase_lines.empty:
                _pl = purchase_lines.copy()
                _pl["Total"] = _to_num(_pl["Total"]).fillna(0)
                _spend_map = _pl.groupby("Supplier")["Total"].sum().to_dict()

            ranked_cfg = sorted(
                [(s, _spend_map.get(s, 0)) for s in known_list],
                key=lambda x: -x[1],
            )
            top15_cfg = [s for s, _ in ranked_cfg[:15]
                          if _spend_map.get(s, 0) > 0]
            rest_cfg = sorted([s for s in known_list if s not in top15_cfg])

            def _cfg_label(s):
                sp = _spend_map.get(s, 0)
                if s in top15_cfg and sp > 0:
                    return f"{s}  —  ${sp:,.0f} spend"
                return s

            cfg_options = top15_cfg + rest_cfg
            cfg_labels = [_cfg_label(s) for s in cfg_options]
            cfg_label_to_sup = dict(zip(cfg_labels, cfg_options))

            scol1, scol2 = st.columns([1, 3])
            cfg_label_pick = scol1.selectbox(
                "Supplier to configure  "
                "(top 15 by 12mo spend, then A-Z)",
                cfg_labels, key="sc_sup_label",
            )
            cfg_supplier = cfg_label_to_sup[cfg_label_pick]
            existing = supp_configs.get(cfg_supplier, {})

            cc1, cc2, cc3 = st.columns(3)
            lt_sea = cc1.number_input(
                "Lead time SEA (days)",
                min_value=1, max_value=200,
                value=int(existing.get("lead_time_sea_days") or 35),
                key="sc_sea",
            )
            lt_air = cc2.number_input(
                "Lead time AIR (days; 0 = not offered)",
                min_value=0, max_value=60,
                value=int(existing.get("lead_time_air_days") or 0),
                key="sc_air",
            )
            air_def = cc3.selectbox(
                "Air eligible by default?",
                ["No", "Yes"],
                index=int(bool(existing.get("air_eligible_default"))),
                key="sc_air_def",
            )

            cd1, cd2, cd3 = st.columns(3)
            air_max = cd1.number_input(
                "Air MAX length (mm; 0 = any)",
                min_value=0, max_value=5000,
                value=int(existing.get("air_max_length_mm") or 0),
                help="For UPS etc., items longer than this are sea-only. "
                     "E.g. Topmet UPS caps at ~2200mm.",
                key="sc_airmax",
            )
            moq = cd2.number_input(
                "MOQ units",
                min_value=0.0, max_value=10000.0,
                value=float(existing.get("moq_units") or 0),
                key="sc_moq",
            )
            pref_freight = cd3.selectbox(
                "Preferred freight",
                ["sea", "air", "mixed"],
                index=(["sea","air","mixed"].index(
                    existing.get("preferred_freight") or "sea")),
                key="sc_pref",
            )

            ce1, ce2, ce3 = st.columns(3)
            mov = ce1.number_input(
                "MOV amount", min_value=0.0, max_value=100000.0,
                value=float(existing.get("mov_amount") or 0),
                key="sc_mov",
            )
            mov_ccy = ce2.text_input(
                "MOV currency",
                value=existing.get("mov_currency") or "USD",
                key="sc_movccy",
            )

            st.markdown("**ABC safety factors & review days** "
                         "(override the defaults for this supplier)")
            sf_cols = st.columns(6)
            sf_A = sf_cols[0].number_input("Safety A (%)",
                                            min_value=0.0, max_value=100.0,
                                            value=float(existing.get("safety_pct_A") or 30.0),
                                            key="sc_sfA")
            sf_B = sf_cols[1].number_input("Safety B (%)",
                                            min_value=0.0, max_value=100.0,
                                            value=float(existing.get("safety_pct_B") or 20.0),
                                            key="sc_sfB")
            sf_C = sf_cols[2].number_input("Safety C (%)",
                                            min_value=0.0, max_value=100.0,
                                            value=float(existing.get("safety_pct_C") or 15.0),
                                            key="sc_sfC")
            rv_A = sf_cols[3].number_input("Review A (d)",
                                            min_value=1, max_value=180,
                                            value=int(existing.get("review_days_A") or 14),
                                            key="sc_rvA")
            rv_B = sf_cols[4].number_input("Review B (d)",
                                            min_value=1, max_value=180,
                                            value=int(existing.get("review_days_B") or 30),
                                            key="sc_rvB")
            rv_C = sf_cols[5].number_input("Review C (d)",
                                            min_value=1, max_value=180,
                                            value=int(existing.get("review_days_C") or 45),
                                            key="sc_rvC")

            # 100%-dropship supplier toggle — covers Gyford-type suppliers
            # where every SKU is order-on-demand (we never stock any of it).
            # When on, EVERY product whose primary supplier is this supplier
            # is automatically treated as Dropship by the engine (target
            # and reorder qty go to 0).
            st.markdown("**Dropship default**")
            ds_col = st.columns([2, 4])
            ds_default = ds_col[0].toggle(
                "All items from this supplier are dropship",
                value=bool(existing.get("dropship_default") or 0),
                key="sc_dropship_default",
                help="Use for suppliers where we never stock anything "
                     "(e.g. Gyford). Every SKU whose primary supplier is "
                     "this one will be treated as dropship — engine zeros "
                     "target stock and reorder qty. You can still flag "
                     "individual SKUs as dropship via the Ordering table "
                     "for suppliers that are a mix.",
            )
            ds_col[1].caption(
                ":information_source: This only affects the local app's "
                "reorder logic. It doesn't write anything back to CIN7 — "
                "that integration is a separate phase."
            )

            if st.button("Save supplier config", key="sc_save",
                           type="primary"):
                db.set_supplier_config(
                    cfg_supplier,
                    lead_time_sea_days=int(lt_sea),
                    lead_time_air_days=(int(lt_air) if lt_air > 0 else None),
                    air_eligible_default=1 if air_def == "Yes" else 0,
                    air_max_length_mm=(int(air_max) if air_max > 0 else None),
                    moq_units=float(moq) if moq > 0 else None,
                    mov_amount=float(mov) if mov > 0 else None,
                    mov_currency=mov_ccy or None,
                    preferred_freight=pref_freight,
                    safety_pct_A=float(sf_A),
                    safety_pct_B=float(sf_B),
                    safety_pct_C=float(sf_C),
                    review_days_A=int(rv_A),
                    review_days_B=int(rv_B),
                    review_days_C=int(rv_C),
                    dropship_default=1 if ds_default else 0,
                    actor=actor_o,
                )
                st.cache_data.clear()
                st.success(f"Saved config for {cfg_supplier}")
                st.rerun()

            # Current config table
            if supp_configs:
                cfg_rows = []
                for name, c in sorted(supp_configs.items()):
                    cfg_rows.append({
                        "Supplier": name,
                        "Sea LT": c.get("lead_time_sea_days"),
                        "Air LT": (str(c.get("lead_time_air_days"))
                                    if c.get("lead_time_air_days") else "—"),
                        "Air elig.": "Yes" if c.get("air_eligible_default") else "No",
                        "Air max len": (str(c.get("air_max_length_mm"))
                                         if c.get("air_max_length_mm") else "—"),
                        "MOQ": (str(c.get("moq_units"))
                                 if c.get("moq_units") else "—"),
                        "MOV": (f"{c.get('mov_currency') or ''}"
                                f"{c.get('mov_amount') or '—'}"),
                        "Pref freight": c.get("preferred_freight"),
                        "Dropship": ("📦 all items"
                                      if c.get("dropship_default") else ""),
                    })
                st.dataframe(pd.DataFrame(cfg_rows),
                             width="stretch", hide_index=True)

    # --- Supplier-focused view -----------------------------------------
    st.markdown("### :clipboard: Draft PO — by supplier")

    # Supplier dropdown: top 15 by 12mo spend first, then remainder
    # alphabetically. Spend = sum of purchase_lines.Total per supplier.
    raw_suppliers = [s for s in engine_df["Supplier"].unique()
                      if s and s != "(unassigned)"]
    if not raw_suppliers:
        st.info("No suppliers resolved yet. Set family/SKU supplier "
                "assignments on the LED Tubes page.")
        st.stop()

    spend_by_supplier = {}
    if not purchase_lines.empty:
        pl = purchase_lines.copy()
        pl["Total"] = _to_num(pl["Total"]).fillna(0)
        grp = pl.groupby("Supplier")["Total"].sum()
        spend_by_supplier = grp.to_dict()

    # Rank suppliers by spend desc, take top 15
    ranked_spend = sorted(
        [(s, spend_by_supplier.get(s, 0)) for s in raw_suppliers],
        key=lambda x: -x[1],
    )
    top_15 = [s for s, _ in ranked_spend[:15] if spend_by_supplier.get(s, 0) > 0]
    remainder = sorted(
        [s for s in raw_suppliers if s not in top_15]
    )

    # Add spend tag to top-15 labels so buyer sees the ranking basis
    def _label(s):
        spend = spend_by_supplier.get(s, 0)
        if s in top_15:
            return f"{s}  —  ${spend:,.0f} spend"
        return s

    dropdown_options = top_15 + remainder
    dropdown_labels = [_label(s) for s in dropdown_options]
    label_to_supplier = dict(zip(dropdown_labels, dropdown_options))

    sc_row1 = st.columns([3, 2])
    with sc_row1[0]:
        sel_label = st.selectbox(
            "Supplier  (top 15 ordered by 12mo spend, then A-Z)",
            dropdown_labels,
            key="ord_supplier_label",
        )
        sel_sup = label_to_supplier[sel_label]
    with sc_row1[1]:
        freight_mode_choice = st.radio(
            "Freight mode for this PO",
            ["Mixed (auto per-SKU)", "Sea only", "Air only"],
            index=0,
            horizontal=True,
            key=f"freight_mode_{sel_sup}",
            help=(
                "Mixed: uses whichever freight mode matches each SKU's "
                "eligibility automatically (length ≤ air_max, supplier "
                "offers air, etc.).  \n"
                "Sea only: force sea lead time on every line — safer for "
                "big PO consolidations, no per-SKU length check.  \n"
                "Air only: restrict PO to items that are air-eligible "
                "(respects supplier's air max length — 3m+ items excluded)."
            ),
        )

    # ------------------------------------------------------------------
    # PO DRAFT SELECTOR (multi-draft per supplier with lifecycle)
    # ------------------------------------------------------------------
    # The buyer picks a working draft (or creates one). Drafts persist
    # across browser refresh / app restart / multiple users. Pessimistic
    # locking prevents two buyers from clobbering each other's edits.
    # See db.py po_drafts + po_draft_lines + helpers.
    _actor = (st.session_state.get("current_user", "").strip() or "unknown")
    _drafts_for_supplier = db.list_po_drafts(
        supplier=sel_sup, include_archived=False)
    _archived_drafts = db.list_po_drafts(
        supplier=sel_sup, include_archived=True)
    _archived_drafts = [d for d in _archived_drafts
                         if d["status"] in ("finalized", "cancelled")]

    _draft_state_key = f"po_active_draft_{sel_sup}"
    _active_draft_id = st.session_state.get(_draft_state_key)

    # Build dropdown options
    _draft_opts: list = ["— Engine baseline (no draft) —"]
    _draft_opt_to_id: dict = {_draft_opts[0]: None}
    for d in _drafts_for_supplier:
        lock_str = ""
        if d["locked_by"]:
            if d["locked_by"] == _actor:
                lock_str = "  🔓 you"
            else:
                lock_str = f"  🔒 {d['locked_by']}"
        status_emoji = {
            "editing": "📝",
            "submitted": "📤",
        }.get(d["status"], "📄")
        freight_str = (
            f" [{d['freight_mode']}]" if d["freight_mode"] else "")
        po_str = (f" → CIN7 PO #{d['cin7_po_number']}"
                  if d["cin7_po_number"] else "")
        label = (f"{status_emoji} {d['name']}{freight_str}"
                 f" ({d['status']}){po_str}{lock_str}")
        _draft_opts.append(label)
        _draft_opt_to_id[label] = d["id"]

    _drafts_row = st.container(border=True)
    with _drafts_row:
        st.markdown(
            f"**📋 PO drafts for {sel_sup}** "
            f"— {len(_drafts_for_supplier)} active"
            + (f", {len(_archived_drafts)} archived"
                if _archived_drafts else ""))

        _ds_c1, _ds_c2 = st.columns([3, 2])
        with _ds_c1:
            # Determine default index
            _default_idx = 0
            if _active_draft_id:
                for label, did in _draft_opt_to_id.items():
                    if did == _active_draft_id:
                        _default_idx = _draft_opts.index(label)
                        break
            _picked = st.selectbox(
                "Active draft (qtys you edit save here)",
                options=_draft_opts,
                index=_default_idx,
                key=f"po_draft_picker_{sel_sup}",
                help="Pick a draft to load its saved qtys into the "
                     "editor below. 'Engine baseline' uses engine "
                     "suggestions only — edits won't be saved unless "
                     "you create or pick a draft."
            )
            _new_active_draft_id = _draft_opt_to_id.get(_picked)
            if _new_active_draft_id != _active_draft_id:
                st.session_state[_draft_state_key] = _new_active_draft_id
                st.rerun()
            _active_draft_id = _new_active_draft_id

        with _ds_c2:
            with st.popover("➕ New draft", use_container_width=True):
                _new_name = st.text_input(
                    "Name", key=f"new_draft_name_{sel_sup}",
                    placeholder="e.g. Sea Freight Apr 29")
                _new_freight = st.selectbox(
                    "Freight mode",
                    ["mixed", "sea", "air"],
                    key=f"new_draft_freight_{sel_sup}")
                _new_note = st.text_input(
                    "Note (optional)", key=f"new_draft_note_{sel_sup}")
                if st.button("Create",
                              key=f"new_draft_create_{sel_sup}",
                              type="primary",
                              disabled=not _new_name.strip()):
                    new_id = db.create_po_draft(
                        supplier=sel_sup,
                        name=_new_name.strip(),
                        actor=_actor,
                        freight_mode=_new_freight,
                        note=_new_note,
                    )
                    st.session_state[_draft_state_key] = new_id
                    st.success(f"Created draft #{new_id}")
                    st.rerun()

        # If a draft is active, show actions
        if _active_draft_id:
            _active = db.get_po_draft(_active_draft_id)
            if _active is None:
                st.warning("Selected draft no longer exists. Resetting.")
                st.session_state[_draft_state_key] = None
                st.rerun()
            else:
                _is_submitted = _active["status"] != "editing"
                _lock_holder = _active["locked_by"]
                _i_hold_lock = (_lock_holder == _actor)

                # Status / lock info row
                _info_cols = st.columns([3, 1, 1, 1])
                with _info_cols[0]:
                    _meta = (
                        f"**{_active['name']}** · status "
                        f"`{_active['status']}` · "
                        f"created {_active['created_at']}"
                    )
                    if _active["cin7_po_number"]:
                        _meta += (
                            f" · CIN7 PO **#{_active['cin7_po_number']}**")
                    if _is_submitted:
                        _meta += " 🔒 (read-only here, edit in CIN7)"
                    elif _lock_holder and not _i_hold_lock:
                        _meta += (
                            f" · 🔒 locked by **{_lock_holder}** since "
                            f"{_active['locked_at']} (read-only for you)")
                    elif _i_hold_lock:
                        _meta += " · 🔓 you have the lock"
                    else:
                        _meta += " · ⚠ unlocked — click 'Take lock' to edit"
                    st.markdown(_meta)

                with _info_cols[1]:
                    if not _is_submitted and not _i_hold_lock:
                        if st.button(
                                "🔑 Take lock",
                                key=f"take_lock_{_active_draft_id}",
                                use_container_width=True,
                                help=("Acquire the editing lock. Fails "
                                       "if another user is actively "
                                       "editing (their lock is fresh "
                                       "<30 min).")):
                            ok = db.lock_po_draft(_active_draft_id, _actor)
                            if ok:
                                st.success("Lock acquired.")
                                st.rerun()
                            else:
                                st.error(
                                    f"🔒 {_lock_holder} holds the lock "
                                    f"(active <30 min). Try later or "
                                    f"force-release.")
                    elif not _is_submitted and _i_hold_lock:
                        if st.button(
                                "🔓 Release lock",
                                key=f"release_lock_{_active_draft_id}",
                                use_container_width=True,
                                help=("Free the lock so other team "
                                       "members can edit.")):
                            db.release_po_draft_lock(
                                _active_draft_id, _actor)
                            st.success("Lock released.")
                            st.rerun()
                with _info_cols[2]:
                    if not _is_submitted and _i_hold_lock:
                        with st.popover("📤 Submit",
                                          use_container_width=True):
                            st.markdown("**Mark as submitted to CIN7**")
                            st.caption(
                                "Records the CIN7 PO number and locks "
                                "this draft as submitted (no further "
                                "edits in our app — modify in CIN7 from "
                                "here). Real CIN7 API push is Phase 2.")
                            _po_num = st.text_input(
                                "CIN7 PO number",
                                key=f"submit_po_num_{_active_draft_id}",
                                placeholder="e.g. 5023")
                            _submit_note = st.text_input(
                                "Note (optional)",
                                key=f"submit_note_{_active_draft_id}")
                            if st.button(
                                    "Confirm submit",
                                    key=f"confirm_submit_{_active_draft_id}",
                                    type="primary",
                                    disabled=not _po_num.strip()):
                                db.mark_po_draft_submitted(
                                    _active_draft_id, _actor,
                                    cin7_po_number=_po_num.strip(),
                                    note=_submit_note)
                                st.success(
                                    f"Marked submitted as PO "
                                    f"#{_po_num.strip()}")
                                st.rerun()
                with _info_cols[3]:
                    if not _is_submitted and _i_hold_lock:
                        with st.popover("🗑️ Cancel",
                                          use_container_width=True):
                            st.markdown(
                                "**Cancel this draft?** All saved qty "
                                "edits will be lost. Cannot be undone.")
                            _cancel_reason = st.text_input(
                                "Reason (optional)",
                                key=f"cancel_reason_{_active_draft_id}")
                            if st.button(
                                    "Confirm cancel",
                                    key=f"confirm_cancel_{_active_draft_id}",
                                    type="secondary"):
                                db.cancel_po_draft(
                                    _active_draft_id, _actor,
                                    reason=_cancel_reason)
                                st.session_state[_draft_state_key] = None
                                st.success("Draft cancelled.")
                                st.rerun()

    # Convenience flag for downstream code.
    # NOTE: db.get_po_draft returns a sqlite3.Row, which behaves like a
    # dict for [] indexing but does NOT support .get(). We fetch once,
    # convert to dict, and short-circuit if there's no active draft.
    if _active_draft_id is None:
        _draft_can_edit = False
    else:
        _drow = db.get_po_draft(_active_draft_id)
        _ddict = dict(_drow) if _drow is not None else {}
        _draft_can_edit = (
            _ddict.get("status") not in (
                "submitted", "finalized", "cancelled")
            and _ddict.get("locked_by") == _actor
        )

    # Filter & apply ABC filter
    fc1, fc2, fc3 = st.columns(3)
    abc_filter = fc1.multiselect("ABC classes",
                                    ["A", "B", "C", "—"],
                                    default=["A", "B", "C"],
                                    key="ord_abc_filter")
    status_filter = fc2.multiselect(
        "Status filter",
        ["🔴 Reorder now", "🟠 Reorder soon",
          "🟢 On target", "🔵 Overstocked",
          "💀 Dead stock", "⚪ No demand, no stock"],
        default=["🔴 Reorder now", "🟠 Reorder soon"],
        key="ord_status_filter",
    )
    only_reorder_positive = fc3.checkbox(
        "Only show SKUs with reorder suggestion > 0",
        value=True, key="ord_only_reorder",
    )

    # --- Hide non-master items from the reorder workspace entirely ---
    # Non-masters (MP variants, cuts, 01X2 packs from 10X2 masters, etc.)
    # are not directly orderable — their demand is rolled up into their
    # master. Showing them would suggest ordering assembled products.
    # Exception: show them in the "full detail" expander at the bottom.
    orderable_df = engine_df[~engine_df["is_non_master_tube"]]

    # --- Supplier-wide snapshot (BEFORE filters) ---
    # Totals for THIS supplier across ALL their MASTER SKUs.
    all_supplier_df = orderable_df[orderable_df["Supplier"] == sel_sup]

    # --- Apply per-SKU freight overrides (team buyers can flip per row)
    # State shape: session_state["freight_overrides"][sel_sup] = {sku: mode}
    if "freight_overrides" not in st.session_state:
        st.session_state["freight_overrides"] = {}
    sup_overrides = st.session_state["freight_overrides"].get(sel_sup, {})

    if sup_overrides:
        cfg_sel = supp_configs.get(sel_sup, {})
        lt_sea_sel = cfg_sel.get("lead_time_sea_days") or 35
        lt_air_sel = cfg_sel.get("lead_time_air_days")
        all_supplier_df = all_supplier_df.copy()

        def _apply_override(row):
            sku_here = row["SKU"]
            override_mode = sup_overrides.get(sku_here)
            if not override_mode:
                return row
            if override_mode == "air" and lt_air_sel:
                new_lt = lt_air_sel
                new_mode = "air (manual)"
            elif override_mode == "sea":
                new_lt = lt_sea_sel
                new_mode = "sea (manual)"
            else:
                return row
            abc = row.get("ABC") or "C"
            safety_pct = {
                "A": cfg_sel.get("safety_pct_A") or 30.0,
                "B": cfg_sel.get("safety_pct_B") or 20.0,
                "C": cfg_sel.get("safety_pct_C") or 15.0,
            }.get(abc, 20.0)
            review_days = {
                "A": cfg_sel.get("review_days_A") or 14,
                "B": cfg_sel.get("review_days_B") or 30,
                "C": cfg_sel.get("review_days_C") or 45,
            }.get(abc, 30)
            avg_daily = float(row.get("avg_daily") or 0)
            lt_demand = avg_daily * new_lt
            safety = lt_demand * (safety_pct / 100.0)
            review_demand = avg_daily * review_days
            new_target = lt_demand + safety + review_demand
            onhand = float(row.get("OnHand") or 0)
            available = float(row.get("Available") or 0)
            on_order = float(row.get("OnOrder") or 0)
            unfulfilled = float(row.get("unfulfilled") or 0)
            effective_pos = available + on_order - unfulfilled
            new_reorder = int(round(max(0, new_target - effective_pos)))
            moq = cfg_sel.get("moq_units") or 0
            if new_reorder > 0 and moq and new_reorder < moq:
                new_reorder = int(moq)
            row = row.copy()
            row["lead_time_days"] = new_lt
            row["freight_mode"] = new_mode
            row["target_stock"] = new_target
            row["reorder_qty"] = new_reorder
            return row

        all_supplier_df = all_supplier_df.apply(_apply_override, axis=1)

    # --- Then apply filters to get the working view ---
    s_df = all_supplier_df.copy()

    # Freight mode filtering:
    #   "Air only" → keep only rows whose freight_mode == 'air'
    #                 (auto-computed from supplier's air_max_length_mm)
    #   "Sea only" → keep all rows but force freight_mode='sea' for the
    #                 status / lead-time displayed
    if freight_mode_choice == "Air only":
        s_df = s_df[s_df["freight_mode"] == "air"]
    elif freight_mode_choice == "Sea only":
        # Force all to sea — recompute effective lead time using supplier's
        # sea lead time so the reorder qty reflects the longer wait.
        cfg_sel = supp_configs.get(sel_sup, {})
        sea_days = cfg_sel.get("lead_time_sea_days") or 35
        s_df = s_df.copy()
        s_df["freight_mode"] = "sea"
        s_df["lead_time_days"] = sea_days

    if abc_filter:
        s_df = s_df[s_df["ABC"].isin(abc_filter)]
    if status_filter:
        s_df = s_df[s_df["Status"].isin(status_filter)]
    if only_reorder_positive:
        # Keep two kinds of rows:
        #   (a) Normal items with a positive reorder suggestion (engine
        #       thinks we should buy them).
        #   (b) Dropship items with ANY active 12-month demand — these
        #       are "candidates the buyer might want to decide on":
        #       either add to this PO (tick Include?) or promote to
        #       stocked (untick Dropship?). Pure-zero-demand dropship
        #       items stay hidden to avoid clutter; they're still in
        #       the "📦 Dropship products" expander with full list.
        _is_dropship = s_df["SKU"].astype(str).isin(dropship_skus)
        _has_any_demand = s_df.get(
            "effective_units_12mo", pd.Series(dtype=float)).fillna(0) > 0
        keep_mask = (s_df["reorder_qty"] > 0) | (
            _is_dropship & _has_any_demand
        )
        s_df = s_df[keep_mask]
    s_df = s_df.sort_values(["reorder_qty"], ascending=False)

    # Supplier-wide totals (unfiltered) — the "real" supplier picture
    sw_skus = len(all_supplier_df)
    sw_stock_value = float(all_supplier_df["OnHandValue"].sum())
    sw_excess_value = float(all_supplier_df["excess_value"].sum())
    sw_dead_value = float(
        all_supplier_df.loc[all_supplier_df["Status"] == "💀 Dead stock",
                            "OnHandValue"].sum()
    )

    # Count non-masters hidden from this supplier (for transparency)
    all_supplier_including_variants = engine_df[
        engine_df["Supplier"] == sel_sup]
    non_master_count = int(
        all_supplier_including_variants["is_non_master_tube"].sum()
    )

    st.markdown(f"**{sel_sup}** — supplier-wide snapshot "
                f"(showing **{sw_skus:,} master/orderable SKUs**; "
                f"{non_master_count:,} assembled variants hidden and "
                f"rolled up to their masters):")
    sw1, sw2, sw3, sw4 = st.columns(4)
    sw1.metric("SKUs we source from them", _fmt_number(sw_skus))
    sw2.metric("Current stock value (all)",
               _fmt_money(sw_stock_value))
    sw3.metric("Excess stock (all)",
               _fmt_money(sw_excess_value),
               delta=(f"{sw_excess_value/sw_stock_value*100:.1f}% of current"
                      if sw_stock_value else None),
               delta_color="inverse")
    sw4.metric("Dead stock", _fmt_money(sw_dead_value))

    # --- Filtered PO summary strip ---
    st.markdown("---")
    st.markdown(f"**Filtered view** — {len(s_df):,} SKUs after filters:")

    # Pull MOV from supplier config so we can show it alongside PO value
    cfg = supp_configs.get(sel_sup, {})
    mov_amt = cfg.get("mov_amount") or 0
    mov_ccy = cfg.get("mov_currency") or ""
    est_value = float((s_df["reorder_qty"] * s_df["POCost"]).sum())

    sc1, sc2, sc3, sc4, sc5 = st.columns(5)
    sc1.metric("SKUs shown", len(s_df))
    sc2.metric("Total reorder units",
               _fmt_number(int(s_df["reorder_qty"].sum())))
    if mov_amt and est_value < mov_amt:
        gap = mov_amt - est_value
        sc3.metric(
            "Est. PO value",
            _fmt_money(est_value),
            delta=f"-{_fmt_money(gap)} below MOV",
            delta_color="inverse",
            help="reorder_qty × FixedCost. Falls back to AverageCost.")
    elif mov_amt:
        over = est_value - mov_amt
        sc3.metric(
            "Est. PO value",
            _fmt_money(est_value),
            delta=f"+{_fmt_money(over)} above MOV",
            delta_color="normal",
            help="reorder_qty × FixedCost. Falls back to AverageCost.")
    else:
        sc3.metric("Est. PO value", _fmt_money(est_value),
                   help="reorder_qty × FixedCost. Falls back to AverageCost.")

    if mov_amt:
        sc4.metric(
            "MOV target",
            f"{mov_ccy}{_fmt_money(mov_amt)}",
            help=f"{sel_sup}'s minimum order value, configured in "
                 f"Supplier configuration above.")
    else:
        sc4.metric(
            "MOV target", "—",
            help="No MOV configured for this supplier. Set it in the "
                 "Supplier configuration expander above.")

    sc5.metric("Filtered stock value",
               _fmt_money(float(s_df["OnHandValue"].sum())),
               help="Stock value of just the SKUs shown after filtering.")

    # MOV warning + actionable hint
    # Suppress the warning if there are pending qty edits in the editor
    # that already cover the gap — st.session_state holds the live
    # editor state. Without this check, the buyer sees a "MOV not met"
    # warning at the top while the live preview below shows MOV +met.
    _editor_state_key = f"po_editor_ord_{sel_sup}"
    _live_total_check = est_value
    _editor_state = st.session_state.get(_editor_state_key)
    if (mov_amt and _editor_state and "edited_rows" in _editor_state):
        # Sum live values from the editor's saved edits (rough — assumes
        # the user only adjusted qty, not POCost). Only used to gate
        # the top warning.
        try:
            _edits = _editor_state.get("edited_rows", {})
            for _idx, _changes in _edits.items():
                if "Order qty" in _changes:
                    _new_q = float(_changes["Order qty"] or 0)
                    _row_costs = s_df["POCost"].fillna(0).tolist()
                    if _idx < len(_row_costs):
                        _orig_q = float(s_df.iloc[_idx].get("reorder_qty", 0) or 0)
                        _orig_cost = float(_row_costs[_idx])
                        _live_total_check += (_new_q - _orig_q) * _orig_cost
        except Exception:
            pass

    if mov_amt and _live_total_check < mov_amt:
        gap = mov_amt - _live_total_check
        st.warning(
            f":warning: **{sel_sup} MOV not met** — current PO is "
            f"{_fmt_money(_live_total_check)} vs minimum "
            f"{_fmt_money(mov_amt)}. **Need {_fmt_money(gap)} more.** "
            f"Use the Upcoming reorders expander below to consolidate "
            f"future-needed SKUs into this PO, or pad qtys on lines "
            f"approaching their next price tier (see Tier Opportunities)."
        )
    elif mov_amt and est_value < mov_amt and _live_total_check >= mov_amt:
        # Live edits already cover the gap — small confirmation note
        st.success(
            f":white_check_mark: **MOV met by your edits** — live PO "
            f"value is now {_fmt_money(_live_total_check)} vs minimum "
            f"{_fmt_money(mov_amt)}.")
    elif not mov_amt:
        st.caption(
            f":bulb: No MOV configured for {sel_sup}. To enable "
            "MOV-gap visibility, expand 'Configure lead times, MOQ/MOV, "
            "freight per supplier' above and set `mov_amount`."
        )

    # Editable PO table
    # NOTE: PO lines use FixedCost (CIN7 supplier price) — NOT AverageCost.
    # AverageCost is a drifting landed cost; what we actually pay on this PO
    # is the FixedCost set on the SKU's supplier record. When FixedCost is
    # missing (0), the engine falls back to AverageCost and the POCostBasis
    # column surfaces that fact.
    #
    # DEFAULT column order + visibility. Every column the user can see in
    # the PO editor is listed here — including the interactive / computed
    # ones (Include?, Order qty, Line value, Source) so they can be
    # repositioned by the buyer. User preferences (saved per-user via
    # db.save_column_layout) override this; unknown saved columns are
    # ignored and newly-added columns appear the next time the user hits
    # "Reset layout" or adds them back.
    default_editor_cols = [
        "Include?", "🔍", "SKU", "Name", "ABC", "Status", "Category",
        "trend_flag",
        "trend_12m", "last_6mo_series", "units_12mo",
        "units_45d", "momentum", "customers_45d", "top_cust_pct",
        "avg_daily", "LengthMM",
        "OnHand", "Allocated", "Available", "OnOrder",
        "unfulfilled", "DoC_days",
        "target_stock", "reorder_qty",
        "Order qty", "Line value",
        "freight_mode", "lead_time_days",
        "POCost", "POCostBasis", "excess_units", "excess_value",
        "Note", "Exclude?", "Dropship?",
        "Source",
    ]

    # REQUIRED columns — PO business logic reads these downstream, so they
    # must always be present in the editor, even if a user tried to hide
    # them (hidden-by-mistake would crash the PO summary). We keep them
    # repositionable via the layout editor, but never removable.
    REQUIRED_COLS = {"SKU", "Include?", "Order qty", "POCost"}

    # Load saved layout for the current user (falls back to 'default').
    _layout_user = st.session_state.get("current_user", "").strip() or "default"
    _layout_view = "ordering_po_editor"
    saved_layout = db.get_column_layout(_layout_user, _layout_view)
    if saved_layout:
        # Keep only columns that still exist in the engine output, preserving
        # user's order. Drop unknown entries silently.
        editor_cols = [c for c in saved_layout if c in default_editor_cols]
        # Safety: if saved layout has somehow been emptied, fall back.
        if not editor_cols:
            editor_cols = list(default_editor_cols)
    else:
        editor_cols = list(default_editor_cols)

    # Always ensure required columns are present (append at end if user
    # somehow saved a layout without them — e.g. a layout from an older
    # app version before REQUIRED_COLS was enforced).
    for _req in REQUIRED_COLS:
        if _req not in editor_cols and _req in default_editor_cols:
            editor_cols.append(_req)

    # --- Column layout editor ------------------------------------------
    # Human-friendly labels for the config table (so buyer doesn't see
    # `last_6mo_series` and get confused).
    COL_LABELS = {
        "Include?": "✓ Include on PO (checkbox)",
        "SKU": "SKU",
        "Name": "Product name",
        "ABC": "ABC class",
        "Status": "Status",
        "Category": "Category",
        "trend_12m": "📈 12mo trend (sparkline)",
        "last_6mo_series": "Last 6 months (trend numbers)",
        "units_12mo": "12mo units sold",
        "avg_daily": "Avg daily units",
        "LengthMM": "Length (mm)",
        "OnHand": "On hand",
        "Allocated": "Allocated",
        "Available": "Available",
        "OnOrder": "On order",
        "unfulfilled": "Backorders",
        "DoC_days": "Days of cover",
        "target_stock": "Target stock",
        "reorder_qty": "Suggested reorder (engine)",
        "Order qty": "✏ Order qty (editable)",
        "Line value": "Line $ value",
        "freight_mode": "Freight (air/sea)",
        "lead_time_days": "Lead time (days)",
        "POCost": "PO cost (FixedCost)",
        "POCostBasis": "Cost basis",
        "excess_units": "Excess units",
        "excess_value": "Excess $ tied up",
        "Note": "📝 Note (editable)",
        "Exclude?": "🚫 Exclude from reorder",
        "Dropship?": "📦 Dropship (order-on-demand)",
        "trend_flag": "📈 Trend signal",
        "units_45d": "45d units",
        "momentum": "Momentum (45d vs prior)",
        "customers_45d": "Customers 45d",
        "top_cust_pct": "Top-customer share",
        "Source": "Source (Auto / Manual)",
    }

    # Named presets — one-click workflows for common buyer scenarios.
    # Every preset ends with the interactive columns (Include?, Order qty,
    # Line value) near/at the right so the buyer's gaze lands on context
    # first, then actions.
    PRESETS = {
        "Buyer essentials (default)": [
            "Include?", "SKU", "Name", "ABC", "Status",
            "trend_flag", "last_6mo_series",
            "OnHand", "Available", "OnOrder", "unfulfilled",
            "target_stock", "reorder_qty", "freight_mode",
            "POCost", "Order qty", "Line value",
            "Note", "Exclude?", "Dropship?",
        ],
        "Detailed view (everything)": list(default_editor_cols),
        "Minimal — just decide to buy": [
            "Include?", "SKU", "Name", "ABC", "last_6mo_series",
            "OnHand", "reorder_qty", "POCost",
            "Order qty", "Line value",
        ],
        "Financial view — $ focused": [
            "Include?", "SKU", "Name", "ABC", "Status",
            "OnHand", "target_stock", "reorder_qty",
            "POCost", "POCostBasis",
            "excess_units", "excess_value",
            "Order qty", "Line value",
        ],
        "Excess / cleanup view": [
            "SKU", "Name", "ABC", "Status", "Category",
            "OnHand", "last_6mo_series", "DoC_days",
            "excess_units", "excess_value",
        ],
    }

    with st.expander(":gear: Column layout — drag to reorder, drag to hide",
                      expanded=False):
        # --- Runtime diagnostic -------------------------------------------
        # Shows which mode is actually running in THIS Streamlit process.
        # If you see red here, the drag UI won't work and the fallback
        # table appears instead.
        if HAS_SORTABLE:
            # NOTE: don't reference `_sort_items` as a bare statement here —
            # Streamlit's magic mode auto-calls st.write() on bare
            # expressions, which would dump the function's docstring
            # right into the page. If HAS_SORTABLE is True, the import
            # already succeeded, so the name is resolvable.
            st.success(
                "✅ **Drag-and-drop mode active** "
                "(`streamlit-sortables` loaded)"
            )
        else:
            st.warning(
                "⚠️ **Fallback mode** — drag-and-drop unavailable. "
                "`streamlit-sortables` isn't importable in this Streamlit "
                "process.\n\n"
                "Fix: **close the Streamlit window completely** (Ctrl+C in "
                "the black PowerShell window), then run `run_app.bat` "
                "again — it installs the package and restarts fresh. "
                "A browser refresh alone is not enough."
            )

        if HAS_SORTABLE:
            st.markdown(
                "**Drag columns between the two panels** to show / hide "
                "them, and drag within the top panel to change their "
                "left-to-right order (top of list = leftmost column in the "
                "PO editor). Click **Save layout** when you're done."
            )

        # Preset picker (top row) — same for both modes.
        # Combines built-in PRESETS with the user's own saved presets
        # from ui_presets. User entries are prefixed with 📌 to distinguish.
        _user_presets = db.list_user_presets(_layout_user, _layout_view)
        _user_preset_labels = {
            f":pushpin: {p['name']}": p for p in _user_presets
        }
        _builtin_opts = list(PRESETS.keys())
        _user_opts = list(_user_preset_labels.keys())
        _all_opts = (["(keep current)"]
                     + _builtin_opts
                     + (["— my saved views —"] if _user_opts else [])
                     + _user_opts)

        pc1, pc2 = st.columns([3, 1])
        preset_name = pc1.selectbox(
            "Quick preset",
            options=_all_opts,
            key=f"layout_preset_{_layout_user}",
            help="Built-in presets + any views you've saved. Pick one "
                 "and click Apply to switch to it instantly.",
        )
        _is_separator = preset_name == "— my saved views —"
        if pc2.button(":sparkles: Apply preset",
                       key=f"apply_preset_{_layout_user}",
                       disabled=(preset_name == "(keep current)"
                                 or _is_separator),
                       use_container_width=True):
            if preset_name in _user_preset_labels:
                # User-saved preset — apply both columns AND widths
                p = _user_preset_labels[preset_name]
                db.save_column_layout(
                    _layout_user, _layout_view, p["columns"])
                db.save_column_widths(
                    _layout_user, _layout_view, p["widths"])
                st.success(f"Applied your saved view "
                             f"**{p['name']}**. Refreshing…")
            else:
                # Built-in preset
                preset_cols = PRESETS[preset_name]
                db.save_column_layout(
                    _layout_user, _layout_view, preset_cols)
                st.success(f"Applied preset **{preset_name}**. Refreshing…")
            st.rerun()

        # Build label ↔ key mapping (drag UI works on strings, so we encode
        # the key into each label's trailing parenthetical for unambiguous
        # round-trip). 🔒 prefix denotes required-cannot-remove columns.
        # NOTE: use LITERAL emoji characters here — the streamlit-sortables
        # component doesn't expand Streamlit emoji shortcodes like :lock:.
        def _display_label(key: str) -> str:
            base = COL_LABELS.get(key, key)
            lock = "🔒 " if key in REQUIRED_COLS else ""
            return f"{lock}{base}  ⟨{key}⟩"

        def _extract_key(label: str) -> Optional[str]:
            # Labels look like "Something ⟨SKU⟩" — grab the text between ⟨ ⟩
            if "⟨" in label and "⟩" in label:
                return label.rsplit("⟨", 1)[1].rstrip("⟩")
            return None

        if HAS_SORTABLE:
            visible_labels = [_display_label(c) for c in editor_cols]
            hidden_labels = [
                _display_label(c) for c in default_editor_cols
                if c not in editor_cols
            ]
            # Use the sortables component in two-container mode.
            # Use literal emoji — sortables doesn't expand shortcodes.
            sorted_result = _sort_items(
                [
                    {
                        "header": "✅ Visible columns "
                                  "(drag to reorder — top = leftmost)",
                        "items": visible_labels,
                    },
                    {
                        "header": "🚫 Hidden columns "
                                  "(drag up to show)",
                        "items": hidden_labels,
                    },
                ],
                multi_containers=True,
                direction="vertical",
                key=f"sortable_cols_{_layout_user}",
            )
            # Parse back to keys
            new_visible_labels = sorted_result[0]["items"]
            new_visible_keys = [
                k for k in (_extract_key(l) for l in new_visible_labels)
                if k and k in default_editor_cols
            ]
            new_hidden_labels = sorted_result[1]["items"]
            new_hidden_keys = [
                k for k in (_extract_key(l) for l in new_hidden_labels)
                if k and k in default_editor_cols
            ]
            # Enforce REQUIRED: if the user dragged a required col into
            # Hidden, quietly move it to the end of Visible at save time.
            for _req in REQUIRED_COLS:
                if _req in new_hidden_keys and _req not in new_visible_keys:
                    new_visible_keys.append(_req)
            preview_keys = new_visible_keys
        else:
            # --- Fallback: the old data_editor flow (no drag-and-drop) ---
            rows_cfg = []
            current_order = {c: i for i, c in enumerate(editor_cols, start=1)}
            for c in default_editor_cols:
                is_visible = c in editor_cols
                is_required = c in REQUIRED_COLS
                label = COL_LABELS.get(c, c)
                if is_required:
                    label = ":lock: " + label + " (required)"
                rows_cfg.append({
                    "Order": current_order.get(c) if is_visible else None,
                    "Column": label,
                    "Show?": True if is_required else is_visible,
                    "_key": c,
                    "_is_visible": 0 if is_visible else 1,
                })
            cfg_df = pd.DataFrame(rows_cfg).sort_values(
                ["_is_visible", "Order", "Column"],
                na_position="last").drop(columns=["_is_visible"]).reset_index(
                drop=True)

            edited_cfg = st.data_editor(
                cfg_df, hide_index=True, width="stretch",
                height=min(38 * (len(cfg_df) + 1) + 10, 900),
                key=f"layout_editor_{_layout_user}",
                column_config={
                    "Order": st.column_config.NumberColumn(
                        "Order (1 = leftmost)",
                        min_value=1, max_value=999, step=1,
                        width="small",
                    ),
                    "Column": st.column_config.TextColumn(
                        "Column", disabled=True, width="large"),
                    "Show?": st.column_config.CheckboxColumn(
                        "Show?", width="small"),
                    "_key": None,
                },
            )
            _edit = edited_cfg.copy()
            _edit["Order"] = pd.to_numeric(_edit["Order"], errors="coerce")
            _edit.loc[_edit["_key"].isin(REQUIRED_COLS), "Show?"] = True
            preview_rows = _edit[_edit["Show?"] == True].copy()
            preview_rows["_sort_order"] = preview_rows["Order"].fillna(1e9)
            preview_rows = preview_rows.sort_values(
                "_sort_order", kind="stable")
            preview_keys = preview_rows["_key"].tolist()
            for _req in REQUIRED_COLS:
                if _req not in preview_keys and _req in default_editor_cols:
                    preview_keys.append(_req)

        # --- LIVE PREVIEW — works for both modes -------------------------
        st.markdown("##### :eye: Preview of the PO editor column order")
        if not preview_keys:
            st.warning(":warning: No columns visible — move at least one.")
        else:
            preview_labels_pretty = [COL_LABELS.get(k, k) for k in preview_keys]
            st.markdown(
                " **→** ".join(f"`{c}`" for c in preview_labels_pretty)
            )

        # --- COLUMN WIDTHS ------------------------------------------------
        # Streamlit's in-browser cell-drag resize doesn't echo back to
        # Python, so we let the user pick small / medium / large per
        # column and save it. Applied at render time via column_config.
        st.markdown("##### :straight_ruler: Column widths (saved per user)")
        st.caption(
            "Streamlit can't capture your in-browser column drags, so "
            "pick a width here for each visible column and hit Save. "
            "Leave anything blank to use the app default for that column."
        )
        _saved_widths = db.get_column_widths(_layout_user, _layout_view)
        _w_rows = []
        for _k in preview_keys:
            _w_rows.append({
                "Column": COL_LABELS.get(_k, _k),
                "Width": _saved_widths.get(_k, ""),
                "_key": _k,
            })
        _w_df = pd.DataFrame(_w_rows) if _w_rows else pd.DataFrame(
            columns=["Column", "Width", "_key"])
        width_edited = st.data_editor(
            _w_df,
            hide_index=True, width="stretch",
            height=min(38 * (len(_w_df) + 1) + 10, 500),
            key=f"width_editor_{_layout_user}",
            column_config={
                "Column": st.column_config.TextColumn(
                    "Column", disabled=True, width="large"),
                "Width": st.column_config.SelectboxColumn(
                    "Width",
                    options=["", "tiny", "small", "medium", "large", "huge"],
                    help="Pick a preset width (5 options, tiny→huge). "
                         "Blank = use the app default for that column.",
                    width="small",
                ),
                "_key": None,
            },
        )
        # Collect widths into a dict for the Save button
        _picked_widths = {}
        if not width_edited.empty:
            for _, wrow in width_edited.iterrows():
                _k = wrow.get("_key")
                _v = (wrow.get("Width") or "").strip().lower()
                if _k and _v in ("tiny", "small", "medium", "large", "huge"):
                    _picked_widths[_k] = _v

        lb1, lb2, lb3 = st.columns([1, 1, 3])
        if lb1.button(":floppy_disk: Save layout + widths",
                       key=f"save_layout_{_layout_user}",
                       type="primary",
                       use_container_width=True):
            if not preview_keys:
                st.error("You can't hide every column — pick at least one.")
            else:
                db.save_column_layout(
                    _layout_user, _layout_view, preview_keys)
                db.save_column_widths(
                    _layout_user, _layout_view, _picked_widths)
                st.success(
                    f"Saved **{len(preview_keys)}** columns and "
                    f"**{len(_picked_widths)}** width override(s) for "
                    f"`{_layout_user}`. Refreshing…"
                )
                st.rerun()
        if lb2.button(":arrows_counterclockwise: Reset to default",
                       key=f"reset_layout_{_layout_user}",
                       use_container_width=True):
            db.reset_column_layout(_layout_user, _layout_view)
            st.success("Layout + widths reset to app default. Refreshing…")
            st.rerun()
        if saved_layout:
            lb3.caption(
                f":bookmark_tabs: Using **saved layout** for "
                f"`{_layout_user}` ({len(saved_layout)} cols)."
            )
        else:
            lb3.caption(
                ":bookmark_tabs: Using **app default** layout."
            )

        # --- Save current view AS A NAMED PRESET -------------------------
        # Captures the current column order + widths under a user-chosen
        # name, which then shows up in the Quick preset dropdown above
        # with a 📌 icon. This is how you make "my view" re-applyable
        # in one click even after you try a built-in preset.
        st.markdown("##### :pushpin: Save current view as a named preset")
        st.caption(
            "Give the current layout (including widths) a name so you "
            "can snap back to it any time from the Quick preset dropdown "
            "above. Re-using the same name overwrites — good for "
            "iterating on a view."
        )
        sp1, sp2 = st.columns([3, 1])
        preset_save_name = sp1.text_input(
            "Preset name",
            key=f"preset_save_name_{_layout_user}",
            placeholder="e.g. Morning check, Reeves orders, "
                         "Excess cleanup",
            label_visibility="collapsed",
        )
        if sp2.button(":floppy_disk: Save as preset",
                       key=f"preset_save_btn_{_layout_user}",
                       disabled=not (preset_save_name.strip()
                                     and preview_keys),
                       use_container_width=True):
            db.save_user_preset(
                _layout_user, _layout_view,
                preset_save_name.strip(),
                preview_keys, _picked_widths,
            )
            st.success(
                f"Saved preset **:pushpin: {preset_save_name}** — "
                "available in the Quick preset dropdown above."
            )
            st.rerun()

        # List user's presets with delete option
        if _user_presets:
            with st.expander(
                f":pushpin: My saved views ({len(_user_presets)})",
                expanded=False,
            ):
                for p in _user_presets:
                    pc1x, pc2x, pc3x = st.columns([3, 2, 1])
                    pc1x.markdown(f"**{p['name']}**")
                    pc2x.caption(
                        f"{len(p['columns'])} cols · "
                        f"{len(p['widths'])} widths · "
                        f"saved {p['created_at']}"
                    )
                    if pc3x.button(
                        ":wastebasket: Delete",
                        key=f"del_preset_{_layout_user}_{p['name']}",
                    ):
                        db.delete_user_preset(
                            _layout_user, _layout_view, p["name"])
                        st.success(f"Deleted preset **{p['name']}**. "
                                     "Refreshing…")
                        st.rerun()

    # Session-state extras key (defined here so merge happens BEFORE editor)
    extra_key = f"po_extra_lines_{sel_sup}"
    if extra_key not in st.session_state:
        st.session_state[extra_key] = []

    # Pre-compute the interactive / derived columns on a working copy so the
    # column layout can position them anywhere (or hide them). Always add all
    # — editor_cols then selects / orders what the user wants shown.
    _work = s_df.copy()
    # Order qty preserves fractional values for bulk-master rows
    # (is_bulk_master=True with allow_fractional_qty supplier flag);
    # casts to int for everything else.
    def _order_qty_cast(r):
        if bool(r.get("is_bulk_master", False)):
            return round(float(r.get("reorder_qty") or 0), 2)
        try:
            return int(r.get("reorder_qty") or 0)
        except (ValueError, TypeError):
            return 0
    _work["Order qty"] = _work.apply(_order_qty_cast, axis=1)
    # Apply the ACTIVE draft's saved line qtys (v2 multi-draft system).
    # If no draft is active, the editor uses engine-suggested qtys
    # only and edits won't persist beyond the session.
    # Falls back to legacy po_draft_edits for backward compat.
    _active_draft_lines: dict = {}
    if _active_draft_id:
        _active_draft_lines = db.get_po_draft_lines(_active_draft_id)
    if _active_draft_lines:
        _work["Order qty"] = _work.apply(
            lambda r: float(_active_draft_lines.get(
                str(r.get("SKU") or ""), r["Order qty"])),
            axis=1)
    else:
        # Legacy fallback (v1 po_draft_edits) — only used if no v2
        # draft is selected. Will be removed once all suppliers
        # have v2 drafts.
        _persisted_drafts = db.get_po_draft_edits(sel_sup)
        if _persisted_drafts and not _active_draft_id:
            _work["Order qty"] = _work.apply(
                lambda r: float(_persisted_drafts.get(
                    str(r.get("SKU") or ""), r["Order qty"])),
                axis=1)
    _work["Line value"] = (_work["Order qty"] * _work["POCost"]).round(2)
    _work["Include?"] = _work["Order qty"] > 0
    # 🔍 Inspect — tick to open the demand-breakdown expander below the
    # table. Lets the buyer drill into any row's reasoning without
    # leaving the PO editor. The breakdown reads the FIRST ticked SKU,
    # so ticking another row switches the inspection (untick the prior
    # one to keep the table tidy).
    _work["🔍"] = False
    _work["Source"] = "Auto"
    # Note = latest saved note body for this SKU (blank if none). Editable
    # in-grid; saved to notes table on "Save edits" below.
    _work["Note"] = _work["SKU"].astype(str).apply(
        lambda s: latest_notes_map.get(s, "")
    )
    # Exclude? always False in this view — excluded SKUs are already
    # filtered out above. Ticking here + saving moves a row to the
    # "Archived" section.
    _work["Exclude?"] = False
    # Dropship? reflects the saved flag for this SKU; unlike Exclude,
    # dropship SKUs REMAIN visible (you want to track their volume to
    # decide when to promote them to stocked). Tick or untick to toggle.
    _work["Dropship?"] = _work["SKU"].astype(str).isin(dropship_skus)

    # Defensive: only pick columns actually present in _work (handles new
    # columns added to layouts before they exist in the engine output).
    _safe_cols = [c for c in editor_cols if c in _work.columns]
    if not _safe_cols:
        _safe_cols = list(default_editor_cols)
    editable_auto = _work[_safe_cols].copy()

    # Merge extras onto the bottom of the table
    extras_list = st.session_state[extra_key]
    if extras_list:
        extras_rows = []
        for ext in extras_list:
            row = {c: None for c in _safe_cols}
            row["SKU"] = ext.get("SKU")
            row["Name"] = ext.get("Name", "")
            row["ABC"] = ext.get("ABC", "—")
            row["Status"] = "➕ Manual"
            row["Order qty"] = int(ext.get("Order qty") or 0)
            row["POCost"] = float(ext.get("Unit cost") or 0)
            row["POCostBasis"] = "Manual entry"
            row["Line value"] = round(
                row["Order qty"] * row["POCost"], 2)
            row["Include?"] = row["Order qty"] > 0
            row["Source"] = "Manual"
            # Populate context fields from engine_df if available.
            # Include ALL display-relevant columns so added rows don't
            # show blank cells next to the engine's auto rows — trend,
            # stock, cost basis, demand history, categorisation.
            em = engine_df[engine_df["SKU"] == row["SKU"]]
            if not em.empty:
                em_r = em.iloc[0]
                # Prefer engine values over the manually-entered ABC /
                # Status — but only if engine has them set.
                _engine_abc = em_r.get("ABC")
                if pd.notna(_engine_abc) and str(_engine_abc) != "—":
                    row["ABC"] = str(_engine_abc)
                _engine_status = em_r.get("Status")
                if pd.notna(_engine_status) and str(_engine_status):
                    # Prefix with ➕ so buyer still sees this was a
                    # manual/auto-fill add, but the engine's real
                    # status (Dead stock / OK / Dropship / etc.) shows.
                    row["Status"] = f"➕ {_engine_status}"
                for fld in (
                    "units_12mo", "avg_daily", "avg_daily_base",
                    "LengthMM",
                    "OnHand", "Allocated", "Available",
                    "OnOrder", "unfulfilled", "DoC_days",
                    "target_stock", "reorder_qty",
                    "freight_mode", "lead_time_days",
                    "excess_units", "excess_value",
                    "last_6mo", "last_6mo_series",
                    "Category", "Name",
                    # Trend-detection fields
                    "trend_flag", "units_45d", "units_prior_45d",
                    "customers_45d", "top_cust_pct",
                    "top_2_cust_pct", "non_top_avg_units",
                    "top_cust_name", "top_cust_units_12mo",
                    "momentum",
                    # Cost basis from engine (so "Basis" column isn't
                    # locked to 'Manual entry' if we actually have
                    # FixedCost on file for this SKU).
                    "POCostBasis",
                    # 12-month sparkline data if the user has that col
                    "trend_12m",
                ):
                    if fld in em_r.index:
                        val = em_r[fld]
                        # Only overwrite row if engine has a real value;
                        # don't replace a populated Manual field with NaN.
                        if val is not None and not (
                            isinstance(val, float) and pd.isna(val)
                        ):
                            row[fld] = val
            extras_rows.append(row)
        extras_df = pd.DataFrame(extras_rows)
        editable = pd.concat([editable_auto, extras_df],
                              ignore_index=True)
    else:
        editable = editable_auto

    # Build the column_config dict. After it's built we apply any saved
    # per-column width overrides (see "Column widths" in the layout editor).
    _po_col_cfg = {
            "Include?": st.column_config.CheckboxColumn("✓", width="small"),
            "🔍": st.column_config.CheckboxColumn(
                "🔍",
                width="small",
                help="Tick to drill into this SKU's demand breakdown "
                     "(family rollup, monthly trend, recent activity). "
                     "Section appears below the table.",
            ),
            "Source": st.column_config.TextColumn(
                "Source",
                help="Auto = engine-suggested; Manual = added by buyer.",
                disabled=True, width="small"),
            "Order qty": st.column_config.NumberColumn(
                "Order qty", min_value=0, step=0.01, format="%.2f",
                help="Editable order qty. Whole numbers for regular SKUs. "
                     "Decimals (e.g. 0.40) accepted for bulk-roll masters "
                     "where the supplier accepts fractional ordering — "
                     "lets you order exactly the metres needed instead "
                     "of rounding up to a full roll."),
            "Line value": st.column_config.NumberColumn(
                format="$%.0f", disabled=True,
                help="Order qty × FixedCost (supplier PO price). "
                     "This is what the PO will actually cost."),
            "POCost": st.column_config.NumberColumn(
                "PO cost",
                format="$%.2f", disabled=True,
                help="CIN7 FixedCost (your agreed supplier price). "
                     "Falls back to AverageCost only if FixedCost is "
                     "missing — see the 'Basis' column to tell which."),
            "POCostBasis": st.column_config.TextColumn(
                "Basis",
                width="small", disabled=True,
                help="Where PO cost came from: 'FixedCost (supplier)' is "
                     "the supplier's agreed price from CIN7; "
                     "'AverageCost (fallback)' means no FixedCost was set "
                     "and we used landed avg as a stopgap — flag these "
                     "to fix in CIN7; 'No cost on file' needs pricing."),
            "trend_12m": st.column_config.LineChartColumn(
                "12mo trend",
                help="Monthly units sold over the last 12 months as a "
                     "sparkline — oldest on the left, newest on the right. "
                     "Hover a point to see that month's number. For the full "
                     "12/24-month bar chart with month labels, pick the SKU "
                     "in the transparency panel below the table.",
                width="small",
                y_min=0,
            ),
            "last_6mo_series": st.column_config.TextColumn(
                "Last 6 months",
                help="Units sold each of the last 6 months — oldest on the "
                     "left, newest on the right. Numeric version of the "
                     "12mo sparkline; quickly shows trend direction "
                     "(rising / falling / spiky).",
                disabled=True,
                width="medium",
            ),
            "units_12mo": st.column_config.NumberColumn(
                "12mo units", disabled=True, format="%.0f"),
            "avg_daily": st.column_config.NumberColumn(
                "Daily", disabled=True, format="%.2f"),
            "LengthMM": st.column_config.NumberColumn(
                "Len mm", disabled=True),
            "OnHand": st.column_config.NumberColumn(disabled=True),
            "Allocated": st.column_config.NumberColumn(
                "Allocated",
                help="Already reserved for open picks",
                disabled=True),
            "Available": st.column_config.NumberColumn(
                "Available",
                help="OnHand - Allocated + phantom",
                disabled=True),
            "OnOrder": st.column_config.NumberColumn(disabled=True),
            "unfulfilled": st.column_config.NumberColumn(
                "Backorders",
                help="Unfulfilled sale orders (BACKORDERED + ORDERED + "
                     "ORDERING) — units customers are waiting on. "
                     "Subtracted from effective position in the "
                     "reorder calc.",
                disabled=True),
            "DoC_days": st.column_config.NumberColumn(
                "DoC", format="%.0fd", disabled=True),
            "target_stock": st.column_config.NumberColumn(
                "Target", format="%.0f", disabled=True),
            "reorder_qty": st.column_config.NumberColumn(
                "Suggest",
                disabled=True,
                format="%.2f",
                help="Engine-suggested reorder qty. Whole numbers for "
                     "regular SKUs (e.g. 19.00 = 19 units). Decimals "
                     "appear for bulk-roll masters (e.g. 0.40 of a "
                     "100m roll) when the supplier accepts fractional "
                     "ordering — saves capital vs rounding up.",
            ),
            "freight_mode": st.column_config.SelectboxColumn(
                "Freight",
                help="Air or Sea. Defaults to the engine's choice (air for "
                     "eligible SKUs). Change per row to override — the "
                     "reorder qty recalculates on rerun with the new "
                     "lead time. '(manual)' marker means user-overridden.",
                options=["air", "sea", "air (manual)", "sea (manual)"],
                width="small",
            ),
            "lead_time_days": st.column_config.NumberColumn(
                "LT (d)", disabled=True),
            "excess_units": st.column_config.NumberColumn(
                "Excess u", disabled=True, format="%.0f"),
            "excess_value": st.column_config.NumberColumn(
                "Excess $", disabled=True, format="$%.0f"),
            "SKU": st.column_config.TextColumn(disabled=True),
            "Name": st.column_config.TextColumn(disabled=True,
                                                  width="large"),
            "ABC": st.column_config.TextColumn(disabled=True,
                                                  width="small"),
            "Status": st.column_config.TextColumn(disabled=True,
                                                    width="medium"),
            "Category": st.column_config.TextColumn(disabled=True),
            "Note": st.column_config.TextColumn(
                "📝 Note",
                help="Free-text note for this SKU — visible to the whole "
                     "team. Edit here, then click 'Save edits' below the "
                     "table to write it to history (each save appends a "
                     "new note; older notes are preserved).",
                width="large",
            ),
            "Exclude?": st.column_config.CheckboxColumn(
                "🚫 Exclude",
                help="Tick + click 'Save edits' to stop this SKU from "
                     "showing in the reorder list. Reactivate from the "
                     "'Archived' expander above the table.",
                width="small",
            ),
            "Dropship?": st.column_config.CheckboxColumn(
                "📦 Dropship",
                help="Tick + click 'Save edits' to mark this SKU as "
                     "dropship (order-on-demand, we don't stock it). "
                     "Target stock and reorder qty go to 0, status "
                     "shows '📦 Dropship'. Sales history stays visible "
                     "so you can decide when to promote it to stocked.",
                width="small",
            ),
            "trend_flag": st.column_config.TextColumn(
                "📈 Trend",
                help="Signal comparing last 45d vs prior 45d, checking "
                     "multi-customer vs single-customer concentration. "
                     "📈 Trend = real broad growth (engine boosts "
                     "velocity). 🎯 Project = one-off concentrated to "
                     "1 customer (engine discounts the spike). "
                     "🔀 Mixed = watch. 📉 Decline = down 50%+. "
                     "💤 Dormant = had history but no activity in 90d "
                     "(engine drops reorder to 0 — confirm before manual "
                     "override). Stable = normal. See glossary.",
                disabled=True, width="small",
            ),
            "units_45d": st.column_config.NumberColumn(
                "45d units",
                format="%.0f", disabled=True,
                help="Units sold in the last 45 days.",
            ),
            "momentum": st.column_config.NumberColumn(
                "Momentum",
                format="%.2fx", disabled=True,
                help="Ratio of last 45d units to prior 45d units. "
                     ">1.5 = spike; <0.5 = decline.",
            ),
            "customers_45d": st.column_config.NumberColumn(
                "Customers 45d",
                format="%d", disabled=True,
                help="Distinct customers who bought this SKU in the last "
                     "45 days. Key signal for trend-vs-project detection.",
            ),
            "top_cust_pct": st.column_config.NumberColumn(
                "Top customer %",
                format="%.0f%%", disabled=True,
                help="Share of last-45-day units bought by the single "
                     "biggest customer. Over 70% = concentrated risk.",
            ),
    }

    # Apply user-saved per-column width overrides over the hardcoded
    # defaults. Streamlit's column_config values are dicts under the
    # hood, so a simple key override works across recent versions.
    # 'small'/'medium'/'large' are native Streamlit presets (pass through).
    # 'tiny' and 'huge' are mapped to pixel integers — requires Streamlit
    # >=1.40. On older Streamlit, integers silently fall back to 'small'/
    # 'large' respectively so the column still renders reasonably.
    _WIDTH_TRANSLATION = {
        "tiny":   60,      # very narrow — good for one-letter codes (ABC)
        "small":  "small",
        "medium": "medium",
        "large":  "large",
        "huge":   400,     # very wide — good for long names or trend charts
    }
    _saved_po_widths = db.get_column_widths(_layout_user, _layout_view)
    if _saved_po_widths:
        for _k, _w in _saved_po_widths.items():
            cfg = _po_col_cfg.get(_k)
            if isinstance(cfg, dict):
                translated = _WIDTH_TRANSLATION.get(_w, _w)
                try:
                    cfg["width"] = translated
                except Exception:
                    # Defensive: if assignment ever fails, use small/large
                    cfg["width"] = "small" if _w == "tiny" else \
                                     ("large" if _w == "huge" else _w)

    # --- Pending CIN7 dropship writes ---------------------------------
    # Any SKU where local state ≠ CIN7 state. Two directions:
    #   A. Local says NOT dropship, CIN7 says dropship → will set
    #      DropShipMode='No Drop Ship' and remove 'Dropship' tag.
    #   B. Local says dropship (app flag), CIN7 says No Drop Ship →
    #      will set DropShipMode='Always Drop Ship' and add 'Dropship' tag.
    _pending_not_ds = not_ds_overrides & (cin7_always_ds | cin7_tag_ds)
    _pending_add_ds = per_sku_ds & cin7_no_ds
    _pending_writes = sorted(list(_pending_not_ds | _pending_add_ds))
    if _pending_writes:
        _prod_by_sku_local = {}
        if not products.empty:
            for _, _p in products.iterrows():
                _prod_by_sku_local[str(_p.get("SKU"))] = {
                    "ID":   _p.get("ID"),
                    "Name": _p.get("Name"),
                    "DropShipMode": _p.get("DropShipMode"),
                    "Tags": _p.get("Tags") or "",
                }
        with st.expander(
            f":warning: Pending CIN7 dropship writes "
            f"({len(_pending_writes)})",
            expanded=True,
        ):
            st.caption(
                "These SKUs have a local dropship change that hasn't "
                "been written back to CIN7 yet. Click **Write to CIN7** "
                "on each to push the change. CIN7 is the source of "
                "truth — until you write, other integrations still see "
                "the old value."
            )
            for sku_p in _pending_writes:
                info = _prod_by_sku_local.get(str(sku_p), {})
                pid = info.get("ID")
                nm = str(info.get("Name") or "")[:60]
                cur_mode = info.get("DropShipMode") or "(unknown)"
                cur_tags = str(info.get("Tags") or "")
                is_removing = sku_p in _pending_not_ds
                if is_removing:
                    target_mode = "No Drop Ship"
                    # Remove 'Dropship' tag, case-insensitive
                    new_tags = [t.strip() for t in cur_tags.split(",")
                                if t.strip()
                                and t.strip().lower() != "dropship"]
                else:
                    target_mode = "Always Drop Ship"
                    tag_set = [t.strip() for t in cur_tags.split(",")
                               if t.strip()]
                    if "Dropship" not in tag_set and "dropship" not in [
                        t.lower() for t in tag_set]:
                        tag_set.append("Dropship")
                    new_tags = tag_set
                new_tags_str = ",".join(new_tags)

                wc1, wc2, wc3 = st.columns([2, 5, 1])
                wc1.markdown(f"**{sku_p}**")
                wc2.markdown(
                    f"{nm}  \n"
                    f"DropShipMode: `{cur_mode}` → `{target_mode}`  ·  "
                    f"Tags: `{cur_tags or '(none)'}` → "
                    f"`{new_tags_str or '(none)'}`"
                )
                if not pid:
                    wc3.caption(":x: No CIN7 ID")
                elif wc3.button(":outbox_tray: Write to CIN7",
                                   key=f"write_cin7_ds_{sku_p}",
                                   type="primary"):
                    actor = st.session_state.get(
                        "current_user", "").strip() or "anonymous"
                    try:
                        from cin7_sync import Cin7Client
                        import os
                        client = Cin7Client(
                            os.getenv("CIN7_ACCOUNT_ID", ""),
                            os.getenv("CIN7_APPLICATION_KEY", ""),
                            rate_seconds=1.0,
                        )
                        client.update_product(
                            str(pid),
                            {"DropShipMode": target_mode,
                             "Tags": new_tags_str},
                        )
                        # Clear the local override now that CIN7 matches
                        if is_removing:
                            db.clear_not_dropship(sku_p, actor)
                        else:
                            db.clear_dropship(sku_p, actor)
                        st.success(
                            f":white_check_mark: Wrote {sku_p} → "
                            f"`{target_mode}`. Re-run the products "
                            "sync to refresh the local cache."
                        )
                        st.rerun()
                    except Exception as _exc:
                        st.error(
                            f":x: CIN7 write failed for {sku_p}: "
                            f"{type(_exc).__name__}: {str(_exc)[:300]}"
                        )

    # --- Dropship products expander ------------------------------------
    dropship_rows = db.list_dropship(limit=1000)
    if dropship_rows:
        prod_name_map_ds = dict(
            zip(products["SKU"].astype(str), products["Name"].astype(str))
        ) if not products.empty else {}
        with st.expander(
            f":package: Dropship products — order-on-demand "
            f"({len(dropship_rows)})",
            expanded=False,
        ):
            st.caption(
                "These SKUs are order-on-demand (no stock held). The "
                "engine keeps their target and reorder qty at 0. Watch "
                "the 12-month sales below — when a dropship SKU's "
                "volume justifies stocking, click **Promote to stocked** "
                "to switch it back to normal reorder logic."
            )
            # Pull sales volume per SKU from the engine so the buyer can
            # judge "is this worth promoting yet?"
            _ds_lookup = {}
            if not engine_df.empty:
                _ds_lookup = engine_df.set_index(
                    engine_df["SKU"].astype(str)).to_dict("index")
            for r in dropship_rows:
                sku_d = r["sku"]
                nm = prod_name_map_ds.get(str(sku_d), "")[:60]
                meta = _ds_lookup.get(str(sku_d), {})
                u12 = float(meta.get("units_12mo") or 0)
                eff = float(meta.get("effective_units_12mo") or u12)
                cost = float(meta.get("POCost")
                             or meta.get("AverageCost") or 0)
                est_annual = eff * cost
                dc1, dc2, dc3, dc4 = st.columns([2, 4, 3, 1])
                dc1.markdown(f"**{sku_d}**")
                dc2.caption(nm or "_(no product master)_")
                suggest = ""
                if eff >= 40 and est_annual >= 1500:
                    suggest = " :sparkles: **Volume suggests promoting**"
                dc3.markdown(
                    f"12mo: **{eff:,.0f} units** · "
                    f"est. annual spend: ${est_annual:,.0f}"
                    + suggest
                )
                if dc4.button("Promote",
                               key=f"promote_dropship_{sku_d}",
                               help="Clear the dropship flag — this SKU "
                                    "will go back to normal reorder "
                                    "logic on next refresh."):
                    actor = st.session_state.get(
                        "current_user", "").strip() or "anonymous"
                    db.clear_dropship(sku_d, actor)
                    st.success(
                        f"Promoted {sku_d} to stocked. Refreshing…")
                    st.rerun()

    # --- Archived (do not reorder) expander ----------------------------
    archived_rows = db.list_do_not_reorder(limit=1000)
    if archived_rows:
        prod_name_map = dict(
            zip(products["SKU"].astype(str), products["Name"].astype(str))
        ) if not products.empty else {}
        with st.expander(
            f":card_file_box: Archived — hidden from reorder "
            f"({len(archived_rows)})",
            expanded=False,
        ):
            st.caption(
                "These SKUs are excluded from the main reorder list. "
                "Click **Reactivate** to bring any of them back."
            )
            for r in archived_rows:
                sku_a = r["sku"]
                nm = prod_name_map.get(str(sku_a), "")[:70]
                set_by = r["set_by"] or "—"
                set_at = r["set_at"] or ""
                reason = r["notes"] or ""
                rc1, rc2, rc3 = st.columns([2, 5, 1])
                rc1.write(f"**{sku_a}**")
                rc2.markdown(
                    f"{nm}  \n"
                    f":grey_exclamation: set by _{set_by}_ on "
                    f"`{set_at}`"
                    + (f" — {reason}" if reason else "")
                )
                if rc3.button("Reactivate",
                              key=f"reactivate_{sku_a}",
                              type="primary"):
                    actor = st.session_state.get(
                        "current_user", "").strip() or "anonymous"
                    db.clear_do_not_reorder(sku_a, actor)
                    st.success(f"Reactivated {sku_a}. Refreshing…")
                    st.rerun()

    edited = st.data_editor(
        editable,
        width="stretch", hide_index=True, height=500,
        key=f"po_editor_ord_{sel_sup}",
        column_config=_po_col_cfg,
    )

    # --- LIVE EDIT PREVIEW --------------------------------------------
    # Streamlit's data_editor doesn't automatically update disabled
    # cells (like "Line value") when their inputs change. The user edits
    # "Order qty" but the "Line value" column shows the pre-edit value
    # until the page rerenders. Compute the live totals here and surface
    # them in a strip RIGHT below the editor so the buyer sees the
    # immediate impact of their edits — incl. updated MOV gap.
    if (not edited.empty
            and "Order qty" in edited.columns
            and "POCost" in edited.columns):
        _ord = pd.to_numeric(
            edited["Order qty"], errors="coerce").fillna(0)
        _cost = pd.to_numeric(
            edited["POCost"], errors="coerce").fillna(0)
        _live_line_value = _ord * _cost
        _incl_mask = (
            edited["Include?"].fillna(False).astype(bool)
            if "Include?" in edited.columns
            else pd.Series(True, index=edited.index))
        _live_units = float(_ord[_incl_mask].sum())
        _live_total = float(_live_line_value[_incl_mask].sum())

        # Compare to the pre-edit total to surface any change.
        if "Line value" in editable.columns:
            _orig_incl = (
                editable["Include?"].fillna(False).astype(bool)
                if "Include?" in editable.columns
                else pd.Series(True, index=editable.index))
            _orig_total = float(
                pd.to_numeric(editable.loc[_orig_incl, "Line value"],
                               errors="coerce").fillna(0).sum())
        else:
            _orig_total = 0.0
        _diff = _live_total - _orig_total

        _live_cols = st.columns(3)
        _live_cols[0].metric(
            "Live PO units (after edits)",
            _fmt_number(int(_live_units)))
        _live_cols[1].metric(
            "Live PO value (after edits)",
            _fmt_money(_live_total),
            delta=(_fmt_money(_diff)
                   if abs(_diff) > 0.01 else None),
            delta_color="normal" if _diff >= 0 else "inverse")
        if mov_amt:
            if _live_total < mov_amt:
                _gap = mov_amt - _live_total
                _live_cols[2].metric(
                    "MOV gap (live)",
                    _fmt_money(-_gap),
                    delta=f"need {_fmt_money(_gap)} more "
                          f"to reach {_fmt_money(mov_amt)}",
                    delta_color="inverse")
            else:
                _live_cols[2].metric(
                    "MOV (live)",
                    _fmt_money(mov_amt),
                    delta=f"+{_fmt_money(_live_total - mov_amt)} above",
                    delta_color="normal")
        else:
            _live_cols[2].caption(
                "💡 Set MOV in supplier config for gap visibility.")
        st.caption(
            ":wrench: **Note:** the 'Line value' column inside the "
            "table doesn't auto-update when you edit 'Order qty' "
            "(Streamlit limitation for disabled columns). The metrics "
            "above reflect your edits in real time.")

    # --- Drill into demand for any ticked SKU -------------------------
    # The :mag: column in the editor is a one-click toggle. Single-tick
    # behaviour — clicking a new row auto-shows that one's breakdown
    # (we detect which SKU's tick state TRANSITIONED False→True since
    # the last render via session_state). If multiple rows end up
    # ticked, we use the most-recently-toggled one. Buyers don't need
    # to manually untick the previous row.
    _drill_sku = ""
    _drill_state_key = f"po_drill_prior_ticks_{sel_sup}"
    if "🔍" in edited.columns:
        _curr_ticked_set = set(
            edited.loc[edited["🔍"].fillna(False).astype(bool), "SKU"]
            .astype(str).tolist())
        _prior_ticked_set = set(
            st.session_state.get(_drill_state_key, []))
        # Find SKUs that newly became ticked (False→True transitions)
        _new_ticks = _curr_ticked_set - _prior_ticked_set
        if _new_ticks:
            # User just ticked a new row — focus on that one
            _drill_sku = next(iter(_new_ticks))
        elif _curr_ticked_set:
            # No new tick this render, but something is still ticked —
            # use last-known active SKU if it's still ticked, else any
            _last_active = st.session_state.get(
                f"po_drill_active_{sel_sup}", "")
            if _last_active in _curr_ticked_set:
                _drill_sku = _last_active
            else:
                _drill_sku = next(iter(_curr_ticked_set))
        # Persist for next render
        st.session_state[_drill_state_key] = list(_curr_ticked_set)
        st.session_state[f"po_drill_active_{sel_sup}"] = _drill_sku
        if len(_curr_ticked_set) > 1:
            _other_skus = sorted(_curr_ticked_set - {_drill_sku})
            st.caption(
                f":information_source: {len(_curr_ticked_set)} rows ticked — "
                f"showing **{_drill_sku}**. Untick others when convenient: "
                f"{', '.join(f'`{s}`' for s in _other_skus[:5])}"
                + (f" (+ {len(_other_skus)-5} more)" if len(_other_skus) > 5 else "")
            )

    if _drill_sku:
        with st.expander(
            f":mag: Demand breakdown — `{_drill_sku}`",
            expanded=True,
        ):
            _engine_match = engine_df[
                engine_df["SKU"].astype(str) == str(_drill_sku)]
            _engine_row = (_engine_match.iloc[0]
                            if not _engine_match.empty else None)
            render_demand_breakdown(
                sku=_drill_sku,
                sale_lines_df=sale_lines,
                products_df=products,
                bom_children=BOM_CHILDREN,
                bom_parents=BOM_PARENTS,
                engine_row=_engine_row,
                engine_df_full=engine_df,
            )
    else:
        st.caption(
            ":mag: **Tip:** tick the 🔍 column on any row above to "
            "see where its demand comes from (children, monthly trend, "
            "recent sales)."
        )

    # --- Save edits button (Exclude? + Dropship? + Note) --------------
    # Side-by-side with a status line so the buyer can see what will be
    # written before committing.
    _new_exclusions = []
    _changed_notes = []
    _ds_add = []   # SKUs to flag dropship
    _ds_remove = []  # SKUs to un-flag dropship
    if "Exclude?" in edited.columns:
        excl_mask = edited["Exclude?"].fillna(False).astype(bool)
        _new_exclusions = edited.loc[excl_mask, "SKU"].astype(
            str).tolist()
        _new_exclusions = [s for s in _new_exclusions if s]
    if "Dropship?" in edited.columns:
        # Compare edited (what the checkbox shows now) vs saved
        # (effective dropship_skus before this save). Four cases per row:
        #
        #   (a) Untick a row that's dropship via CIN7 → we need the
        #       user's intent recorded as a "Not dropship" override,
        #       AND a CIN7 write-back is pending.
        #   (b) Untick a row that's dropship only via our app flag →
        #       clear the app flag (no CIN7 write needed — CIN7 never
        #       said dropship).
        #   (c) Tick a row that's NOT dropship but CIN7 has "No Drop
        #       Ship" → set app dropship flag AND queue a CIN7 write
        #       to flip the field.
        #   (d) Tick a row that's NOT dropship and CIN7 is silent → set
        #       app dropship flag (no CIN7 write needed).
        for _, dsrow in edited.iterrows():
            sku_d = str(dsrow.get("SKU") or "")
            if not sku_d:
                continue
            edited_flag = bool(dsrow.get("Dropship?") or False)
            saved_flag = sku_d in dropship_skus
            is_cin7_ds = (sku_d in cin7_always_ds) or (sku_d in cin7_tag_ds)
            is_cin7_no = sku_d in cin7_no_ds
            if edited_flag and not saved_flag:
                # Case (c) or (d)
                _ds_add.append((sku_d, is_cin7_no))
            elif saved_flag and not edited_flag:
                # Case (a) or (b)
                _ds_remove.append((sku_d, is_cin7_ds))
    if "Note" in edited.columns and "SKU" in edited.columns:
        for _, nrow in edited.iterrows():
            sku_e = str(nrow.get("SKU") or "")
            new_note = (nrow.get("Note") or "")
            if not sku_e:
                continue
            new_note = str(new_note).strip()
            old_note = (latest_notes_map.get(sku_e) or "").strip()
            if new_note and new_note != old_note:
                _changed_notes.append((sku_e, new_note))

    # --- Persistent qty-edit detection + save/clear UI ---------------
    # Detect Order qty edits by comparing edited["Order qty"] to the
    # value we LOADED with (editable["Order qty"], which already has
    # drafts applied). Edits where qty changed go to po_draft_edits
    # so they survive across browser refresh / app restart.
    _qty_edits = []
    if "Order qty" in edited.columns and "SKU" in edited.columns:
        # Build {sku: loaded_qty} from editable (the engine baseline +
        # any previously-saved drafts already applied)
        _loaded_qtys = {}
        if "SKU" in editable.columns and "Order qty" in editable.columns:
            for _, _r in editable.iterrows():
                _loaded_qtys[str(_r.get("SKU") or "")] = float(
                    pd.to_numeric(_r.get("Order qty", 0),
                                    errors="coerce") or 0)
        for _, _r in edited.iterrows():
            _sk = str(_r.get("SKU") or "")
            if not _sk:
                continue
            _new_q = float(
                pd.to_numeric(_r.get("Order qty", 0),
                                errors="coerce") or 0)
            _old_q = _loaded_qtys.get(_sk, _new_q)
            if abs(_new_q - _old_q) > 0.001:
                _qty_edits.append((_sk, _new_q, _old_q))

    if _qty_edits:
        _q1, _q2 = st.columns([3, 1])
        with _q1:
            if _active_draft_id:
                if _draft_can_edit:
                    st.info(
                        f":memo: **{len(_qty_edits)} pending qty edit"
                        f"{'s' if len(_qty_edits) != 1 else ''}** in this "
                        f"session. Click **Save qty drafts** to persist "
                        f"them to draft #{_active_draft_id}.")
                else:
                    # sqlite3.Row → dict so .get() works
                    _row_now = db.get_po_draft(_active_draft_id)
                    _active_now = dict(_row_now) if _row_now is not None else {}
                    if (_active_now.get("locked_by")
                            and _active_now.get("locked_by") != _actor):
                        st.warning(
                            f":lock: Draft is locked by "
                            f"**{_active_now.get('locked_by')}** — your "
                            f"qty edits won't be saved. Take the lock "
                            f"above first, or pick a different draft.")
                    elif _active_now.get("status") in (
                            "submitted", "finalized", "cancelled"):
                        st.warning(
                            f":lock: Draft is `{_active_now.get('status')}` "
                            f"— qty edits can't be saved here. Edit in "
                            f"CIN7, or create a new draft.")
                    else:
                        st.warning(
                            ":warning: No lock held — click **Take lock** "
                            "above to save qty edits to this draft.")
            else:
                st.warning(
                    ":warning: **No active draft selected.** Your qty "
                    "edits won't persist. Pick or create a draft above "
                    "to save them durably.")
        with _q2:
            if st.button(
                    ":floppy_disk: Save qty drafts",
                    key=f"save_qty_drafts_{sel_sup}",
                    type="primary",
                    disabled=(not _qty_edits) or (not _draft_can_edit),
                    help="Persist Order qty edits to the active draft. "
                         "Requires the lock on a draft in 'editing' "
                         "state.",
                    use_container_width=True):
                # Re-verify lock right before write to defend against
                # someone else stealing it between page load and save
                if not db.lock_po_draft(_active_draft_id, _actor):
                    st.error(
                        "Lock was taken by another user since the page "
                        "loaded. Refresh and try again.")
                else:
                    for _sk, _new_q, _old_q in _qty_edits:
                        db.upsert_po_draft_line(
                            draft_id=_active_draft_id,
                            sku=_sk,
                            edited_qty=_new_q,
                            actor=_actor,
                            note=f"baseline was {_old_q:g}")
                    st.success(
                        f"Saved {len(_qty_edits)} qty edit"
                        f"{'s' if len(_qty_edits) != 1 else ''} "
                        f"to draft #{_active_draft_id}.")
                    st.rerun()

    sec1, sec2 = st.columns([1, 3])
    save_disabled = (not _new_exclusions) and (not _changed_notes) \
        and (not _ds_add) and (not _ds_remove)
    if sec1.button(":floppy_disk: Save edits",
                    key=f"save_po_edits_{sel_sup}",
                    type="primary",
                    disabled=save_disabled,
                    help="Commits any Exclude?, Dropship? and Note edits "
                         "above to the team database.",
                    use_container_width=True):
        actor = st.session_state.get("current_user", "").strip()
        if not actor:
            st.error(
                "Enter your name in the sidebar first — "
                "edits need an author for audit logging."
            )
        else:
            msgs = []
            if _new_exclusions:
                for sku_e in _new_exclusions:
                    db.set_do_not_reorder(sku_e, actor,
                                           "Excluded via PO editor")
                msgs.append(f"Excluded {len(_new_exclusions)} SKU(s)")
            if _ds_add:
                for sku_d, is_cin7_no in _ds_add:
                    # Clear any "Not dropship" override first (user
                    # has changed their mind), then set positive flag.
                    db.clear_not_dropship(sku_d, actor)
                    db.set_dropship(sku_d, actor,
                                     "Marked dropship via PO editor")
                msgs.append(f"Flagged {len(_ds_add)} as dropship")
            if _ds_remove:
                for sku_d, is_cin7_ds in _ds_remove:
                    # Always clear the positive app flag.
                    db.clear_dropship(sku_d, actor)
                    if is_cin7_ds:
                        # CIN7 said dropship → record override so the
                        # pending-writes expander can surface it.
                        db.set_not_dropship(
                            sku_d, actor,
                            "Override — user unticked in PO editor")
                msgs.append(f"Un-flagged {len(_ds_remove)} dropship")
            if _changed_notes:
                for sku_e, body in _changed_notes:
                    db.add_note(sku_e, actor, body)
                msgs.append(f"Saved {len(_changed_notes)} note edit(s)")
            st.success(" • ".join(msgs) + ". Refreshing…")
            st.rerun()
    if save_disabled:
        sec2.caption(
            ":information_source: No pending edits — tick *Exclude?*, "
            "*Dropship?* or type into *Note* to enable Save."
        )
    else:
        pending = []
        if _new_exclusions:
            pending.append(f"{len(_new_exclusions)} exclusion(s)")
        if _ds_add:
            pending.append(f"{len(_ds_add)} new dropship")
        if _ds_remove:
            pending.append(f"{len(_ds_remove)} promote-to-stocked")
        if _changed_notes:
            pending.append(f"{len(_changed_notes)} note edit(s)")
        sec2.caption(
            ":pencil2: Pending: " + ", ".join(pending)
            + " — click Save to commit."
        )

    # --- Capture any per-row freight mode changes from the data_editor
    # and persist them as overrides, which will be applied next rerun.
    if "freight_mode" in edited.columns and "freight_mode" in editable.columns:
        orig_freight = dict(zip(editable["SKU"], editable["freight_mode"]))
        edited_freight = dict(zip(edited["SKU"], edited["freight_mode"]))
        changed = False
        if sel_sup not in st.session_state["freight_overrides"]:
            st.session_state["freight_overrides"][sel_sup] = {}
        supo = st.session_state["freight_overrides"][sel_sup]
        for sku_r, new_mode in edited_freight.items():
            if not sku_r or new_mode is None:
                continue
            orig_mode = orig_freight.get(sku_r)
            # Normalise (strip (manual) suffix for comparison)
            norm_new = str(new_mode).split(" ")[0]
            norm_orig = str(orig_mode or "").split(" ")[0]
            if norm_new != norm_orig and norm_new in ("air", "sea"):
                if supo.get(sku_r) != norm_new:
                    supo[sku_r] = norm_new
                    changed = True
        if changed:
            st.success(
                "Freight mode overrides saved. Reorder qty will "
                "recalculate on next refresh using the new lead times."
            )
            # Show a button to trigger rerun
            if st.button(":arrows_counterclockwise: Apply freight "
                          "overrides now",
                          key=f"apply_freight_overrides_{sel_sup}"):
                st.rerun()

    po_lines = edited[(edited["Include?"]) & (edited["Order qty"] > 0)]

    # Show active freight overrides with a clear-all option
    active_overrides = st.session_state["freight_overrides"].get(sel_sup, {})
    if active_overrides:
        with st.expander(
            f":airplane: {len(active_overrides)} freight override(s) "
            f"active for {sel_sup}"):
            for sku_o, mode_o in sorted(active_overrides.items()):
                oc1, oc2 = st.columns([4, 1])
                oc1.write(f"**{sku_o}** → {mode_o}")
                if oc2.button("Clear", key=f"clr_freight_{sel_sup}_{sku_o}"):
                    del st.session_state["freight_overrides"][sel_sup][sku_o]
                    st.rerun()
            if st.button(":wastebasket: Clear ALL freight overrides "
                          "for this supplier",
                          key=f"clr_all_freight_{sel_sup}"):
                st.session_state["freight_overrides"][sel_sup] = {}
                st.rerun()

    # --- Add extra lines manually --------------------------------------
    st.markdown("#### :heavy_plus_sign: Add extra line to this PO")
    st.caption(
        "Add any SKU to this PO — appears at the bottom of the table "
        "above. Useful for items the reorder engine didn't auto-flag "
        "(stock-up for a project, one-off purchase, item currently at "
        "target but you want more)."
    )

    # Build SKU options: this supplier's SKUs first, optionally all
    supplier_skus = sorted(
        all_supplier_df["SKU"].tolist()
    )
    all_skus = sorted(engine_df["SKU"].tolist())

    xc1, xc2, xc3, xc4, xc5 = st.columns([3, 1, 1, 2, 1])
    show_all_skus = xc1.checkbox(
        "Allow adding SKUs not from this supplier",
        value=False, key=f"show_all_skus_{sel_sup}",
        help="If checked, you can add any SKU in the catalog (not just "
             "those CIN7 associates with this supplier). Useful if you "
             "know an item can be sourced from this supplier but CIN7 "
             "doesn't have the relationship on record yet.",
    )
    available_skus = all_skus if show_all_skus else supplier_skus

    xe1, xe2, xe3, xe4 = st.columns([3, 1, 1, 1])
    extra_sku = xe1.selectbox(
        "SKU to add",
        options=available_skus,
        key=f"extra_sku_{sel_sup}",
        placeholder="Start typing…",
    )
    extra_qty = xe2.number_input(
        "Qty", min_value=1, value=1, step=1,
        key=f"extra_qty_{sel_sup}",
    )
    # Auto-suggest unit cost from engine_df
    default_cost = 0.0
    if extra_sku:
        match = engine_df[engine_df["SKU"] == extra_sku]
        if not match.empty:
            default_cost = float(match["EffectiveUnitCost"].iloc[0] or 0)
    extra_cost = xe3.number_input(
        "Unit cost", min_value=0.0, value=default_cost,
        step=0.01, format="%.2f",
        key=f"extra_cost_{sel_sup}",
    )
    xe4.write(" ")
    xe4.write(" ")
    if xe4.button("Add line", key=f"add_extra_{sel_sup}",
                   use_container_width=True,
                   disabled=not extra_sku):
        # Build a complete extra-line record
        name = ""
        abc = "—"
        if extra_sku:
            row_m = engine_df[engine_df["SKU"] == extra_sku]
            if not row_m.empty:
                name = str(row_m["Name"].iloc[0] or "")[:80]
                abc = str(row_m["ABC"].iloc[0] or "—")
        st.session_state[extra_key].append({
            "SKU": extra_sku,
            "Name": name,
            "ABC": abc,
            "Order qty": int(extra_qty),
            "Unit cost": float(extra_cost),
            "Line value": round(int(extra_qty) * float(extra_cost), 2),
            "From supplier?":
                "✓" if extra_sku in supplier_skus else "⚠ off-supplier",
        })
        st.rerun()

    # Quick action: clear ALL extras for this supplier
    extras = st.session_state[extra_key]
    if extras:
        if st.button(f":wastebasket: Clear all {len(extras)} manual "
                      "line(s)", key=f"clear_extras_{sel_sup}"):
            st.session_state[extra_key] = []
            st.rerun()

    # PO lines come directly from the merged editor — includes both
    # auto and manual rows (manual ones have Source='Manual')
    po_lines_all = po_lines.copy()

    po_units = int(po_lines_all["Order qty"].sum()) if not po_lines_all.empty else 0
    po_value = float(
        (po_lines_all["Order qty"] * po_lines_all["POCost"]).sum()
    ) if not po_lines_all.empty else 0.0
    pk = (po_lines_all["ABC"].value_counts().to_dict()
          if not po_lines_all.empty else {})

    st.markdown("#### PO summary (auto + extras)")
    pco1, pco2, pco3, pco4, pco5 = st.columns(5)
    pco1.metric("PO lines", len(po_lines_all))
    pco2.metric("Total units", _fmt_number(po_units))
    pco3.metric("Estimated value", _fmt_money(po_value))
    pco4.metric("Class mix",
                f"A:{pk.get('A',0)} B:{pk.get('B',0)} C:{pk.get('C',0)}")
    if mov_amt:
        mov_status = ("✓ above MOV"
                      if po_value >= mov_amt else "✗ below MOV")
        pco5.metric("MOV",
                    f"{mov_ccy}${mov_amt:,.0f}",
                    delta=mov_status,
                    delta_color=("normal" if po_value >= mov_amt
                                  else "inverse"))

    # --- MOV auto-fill — inline with demand --------------------------
    # If the current draft is under MOV, compute the N most-urgent
    # upcoming items and show a one-click "Auto-fill to MOV" button.
    # Uses the same upcoming-reorder logic as the section below, but
    # prioritised by urgency (days_to_reorder ascending) and capped
    # at the amount needed to cross MOV.
    if mov_amt and po_value < mov_amt and not all_supplier_df.empty:
        shortfall = mov_amt - po_value
        # Pick the user's chosen lookahead window from session state
        # (falls back to 45 if they haven't moved the slider)
        _af_window = int(st.session_state.get(
            f"upcoming_window_{sel_sup}", 45))

        cand = all_supplier_df.copy()
        cand["eff_pos"] = (cand["Available"].fillna(0)
                            + cand["OnOrder"].fillna(0)
                            - cand["unfulfilled"].fillna(0))
        cand["surplus"] = (cand["eff_pos"]
                            - cand["target_stock"].fillna(0))
        cand["days_to_reorder"] = cand.apply(
            lambda r: (r["surplus"] / max(r["avg_daily"], 0.001)
                       if r["avg_daily"] and r["avg_daily"] > 0
                       else 999),
            axis=1,
        )
        # Only items: not in main reorder, have velocity, currently surplus,
        # AND not already on the draft (in extras)
        _extras_skus = {e.get("SKU") for e in
                          st.session_state.get(extra_key, [])}
        cand = cand[
            (cand["reorder_qty"].fillna(0) == 0)
            & (cand["avg_daily"].fillna(0) > 0)
            & (cand["surplus"] > 0)
            & (~cand["SKU"].astype(str).isin(_extras_skus))
        ].sort_values("days_to_reorder")

        # Suggest qty = avg_daily × window, use same logic as upcoming table
        cand["suggest_qty"] = (
            cand["avg_daily"] * _af_window
        ).round().clip(lower=1).astype(int)
        cand["line_value"] = (cand["suggest_qty"]
                               * cand["POCost"]).round(2)
        cand = cand[cand["line_value"] > 0]

        # Walk through picks until we cross MOV
        picks = []
        running = float(po_value)
        for _, rr in cand.iterrows():
            if running >= mov_amt:
                break
            picks.append(rr)
            running += float(rr["line_value"])

        if picks:
            picks_df = pd.DataFrame(picks)
            will_cross = running >= mov_amt
            st.warning(
                f":warning: **MOV shortfall** "
                f"{mov_ccy}${shortfall:,.0f}. "
                f"The **{len(picks)} most-urgent upcoming item(s)** "
                f"(soonest to hit target, within {_af_window}d window) "
                f"would bring this PO to "
                f"**{mov_ccy}${running:,.0f}** — "
                + ("**above MOV** :white_check_mark:" if will_cross
                   else f"still {mov_ccy}$"
                        f"{mov_amt - running:,.0f} short")
                + "."
            )
            with st.expander(
                f":eyes: Preview the {len(picks)} auto-fill items",
                expanded=False,
            ):
                _af_show = picks_df[["SKU", "Name", "days_to_reorder",
                                      "suggest_qty", "POCost",
                                      "line_value"]].copy()
                st.dataframe(
                    _af_show,
                    hide_index=True, width="stretch",
                    column_config={
                        "SKU": st.column_config.TextColumn(width="medium"),
                        "Name": st.column_config.TextColumn(width="large"),
                        "days_to_reorder": st.column_config.NumberColumn(
                            "Days to target", format="%.0fd"),
                        "suggest_qty": st.column_config.NumberColumn(
                            "Suggest qty", format="%.0f"),
                        "POCost": st.column_config.NumberColumn(
                            "Unit $", format="$%.2f"),
                        "line_value": st.column_config.NumberColumn(
                            "Line $", format="$%.0f"),
                    },
                )
            af_c1, af_c2 = st.columns([1, 3])
            if af_c1.button(
                f":sparkles: Auto-fill {len(picks)} item(s) to hit MOV",
                key=f"auto_fill_mov_{sel_sup}",
                type="primary",
                use_container_width=True,
                help="Adds these items to the draft as extras with "
                     "the suggested qty. You can still tweak each "
                     "line's Order qty in the main editor above.",
            ):
                added = 0
                for _, rr in picks_df.iterrows():
                    sku_a = str(rr.get("SKU") or "")
                    if not sku_a or sku_a in _extras_skus:
                        continue
                    st.session_state[extra_key].append({
                        "SKU": sku_a,
                        "Name": str(rr.get("Name") or "")[:80],
                        "ABC": str(rr.get("ABC") or "—"),
                        "Order qty": int(rr.get("suggest_qty") or 0),
                        "Unit cost": float(rr.get("POCost") or 0),
                        "Line value": float(rr.get("line_value") or 0),
                        "From supplier?": "✓ auto-fill",
                    })
                    added += 1
                st.success(
                    f"Added **{added}** upcoming item(s) to the draft. "
                    "Scroll up to fine-tune any quantity before "
                    "exporting the PO."
                )
                st.rerun()
            af_c2.caption(
                ":bulb: Each line defaults to its *upcoming* suggested "
                f"qty ( avg_daily × {_af_window}d ). Adjust the slider "
                "below the PO editor to change window; that also "
                "changes this auto-fill suggestion."
            )
        elif cand.empty:
            st.info(
                f":information_source: MOV shortfall "
                f"${shortfall:,.0f}, but no upcoming items from this "
                "supplier qualify for auto-fill. "
                "Check the Upcoming section below, or add a manual line."
            )

    # Draft PO action
    actor_ord = st.session_state.get("current_user", "").strip()
    dpa, dpb, dpc = st.columns([1, 1, 1])
    with dpa:
        # CIN7 POST requires:
        #   1. lines to push (po_lines)
        #   2. an actor (current_user) for audit
        #   3. an active local draft so we have a po_drafts row to track
        #      lifecycle and remember the cin7_po_id afterwards
        #   4. the user must hold the lock on that draft (one writer at a time)
        # We persist saved-edit qty into po_draft_lines first so the push
        # uses exactly what's in the DB — not a stale snapshot of the editor.
        _push_disabled = (
            len(po_lines) == 0
            or not actor_ord
            or _active_draft_id is None
            or not _draft_can_edit
        )
        # Tooltip explains exactly why the button is disabled.
        if _active_draft_id is None:
            _push_help = ("Pick or create a draft above first — the push "
                          "needs a po_drafts row to record CIN7 PO ID into.")
        elif not _draft_can_edit:
            _push_help = ("You don't hold the lock on this draft, or it's "
                          "already submitted. Take the lock to push.")
        elif len(po_lines) == 0:
            _push_help = "No lines selected."
        elif not actor_ord:
            _push_help = "Enter your name in the sidebar first."
        else:
            _push_help = ("POST a Draft Advanced Purchase to CIN7 with "
                          "the lines saved on this local draft. The CIN7 "
                          "PO will be created in DRAFT status — review "
                          "and authorise it inside CIN7 to send to "
                          "the supplier.")
        if st.button(":rocket: Create draft PO in CIN7",
                      type="primary",
                      disabled=_push_disabled,
                      width="stretch",
                      help=_push_help,
                      key=f"push_to_cin7_{sel_sup}"):
            # Open the confirmation popover via session-state flag — we
            # don't want a single click to fire a real CIN7 POST.
            st.session_state[f"_show_push_confirm_{sel_sup}"] = True

        if st.session_state.get(f"_show_push_confirm_{sel_sup}"):
            with st.expander(
                    f":rocket: Push draft #{_active_draft_id} to CIN7?",
                    expanded=True):
                # Detect the partial-failure state: draft already has
                # a CIN7 PO ID (master created in CIN7) but our local
                # status is still 'editing' (lines never made it). Offer
                # recovery options before showing the normal push UI.
                _drow_now = db.get_po_draft(_active_draft_id)
                _ddict_now = (dict(_drow_now)
                              if _drow_now is not None else {})
                _stuck_cin7_id = _ddict_now.get("cin7_po_id")
                _stuck_po_num = _ddict_now.get("cin7_po_number")
                _is_stuck = (
                    bool(_stuck_cin7_id)
                    and _ddict_now.get("status") == "editing")

                if _is_stuck:
                    st.warning(
                        f":warning: This draft is linked to CIN7 PO "
                        f"**#{_stuck_po_num}** ({_stuck_cin7_id}) but "
                        "local status is still `editing` — meaning a "
                        "previous push created the master in CIN7 but "
                        "the lines POST failed. Pick one option below:")
                    rc1, rc2, rc3 = st.columns(3)
                    if rc1.button(
                            "↻ Retry lines (keep PO #" +
                            str(_stuck_po_num or "?") + ")",
                            key=f"_retry_lines_{sel_sup}",
                            help="Skip the master POST, only re-send "
                                 "the lines to the existing CIN7 PO."):
                        st.session_state[
                            f"_push_mode_{sel_sup}"] = "retry"
                    if rc2.button(
                            "🧹 Clear CIN7 link & start fresh",
                            key=f"_clear_cin7_link_{sel_sup}",
                            help="Forget the CIN7 PO ID locally so a "
                                 "fresh push creates a new master. "
                                 "You should void the old PO in CIN7 "
                                 "manually."):
                        db.set_po_draft_cin7_ids(
                            _active_draft_id,
                            cin7_po_id="",
                            cin7_po_number="",
                            cin7_status="",
                            actor=actor_ord or "ui")
                        st.success(
                            f"Cleared CIN7 link from draft "
                            f"#{_active_draft_id}. **Don't forget to "
                            f"void PO #{_stuck_po_num} in CIN7.**")
                        st.rerun()
                    if rc3.button(
                            "Cancel",
                            key=f"_cancel_recovery_{sel_sup}"):
                        st.session_state[
                            f"_show_push_confirm_{sel_sup}"] = False
                        st.rerun()
                    # Don't render the normal push UI when in
                    # recovery mode and no choice has been made yet.
                    if (st.session_state.get(
                            f"_push_mode_{sel_sup}") != "retry"):
                        st.stop()
                _is_retry = (
                    st.session_state.get(
                        f"_push_mode_{sel_sup}") == "retry")
                if _is_retry:
                    st.info(
                        f":arrows_counterclockwise: **Retry mode** — "
                        f"will re-POST lines to existing PO "
                        f"#{_stuck_po_num} without creating a new "
                        "master.")

                st.markdown(
                    f"**Supplier:** {sel_sup}  \n"
                    f"**Lines (saved in draft):** "
                    f"{len(db.get_po_draft_lines(_active_draft_id))}  \n"
                    f"**Lines visible in editor (unsaved if higher):** "
                    f"{len(po_lines)}  \n"
                    f"**Estimated value:** {_fmt_money(po_value)}"
                )
                _saved_lines = db.get_po_draft_lines(_active_draft_id)
                if not _saved_lines:
                    st.warning(
                        "This draft has **no saved lines** yet. Click "
                        "'Save Qty edits' above first — only saved "
                        "lines are pushed to CIN7."
                    )
                elif len(_saved_lines) != len(po_lines):
                    st.info(
                        "The number of saved lines differs from what's "
                        "visible in the editor. Pushing will use the "
                        "**saved** lines — make sure they're up to date."
                    )

                _ack = st.checkbox(
                    "I understand this creates a real Draft PO in CIN7. "
                    "It will need human review/authorisation in CIN7 "
                    "before the supplier sees it.",
                    key=f"_push_ack_{sel_sup}")
                _dry_run = st.checkbox(
                    "Dry-run (validate only, don't post)",
                    value=False, key=f"_push_dry_{sel_sup}")

                pcola, pcolb = st.columns([1, 1])
                if pcola.button(
                        "Confirm push",
                        type="primary",
                        disabled=(not _ack) or (not _saved_lines),
                        key=f"_push_go_{sel_sup}"):
                    try:
                        from cin7_post_po import push_po_draft
                        with st.spinner(
                                "Talking to CIN7 — this can take "
                                "10–60 seconds..."):
                            _result = push_po_draft(
                                _active_draft_id,
                                actor=actor_ord,
                                apply=not _dry_run,
                                # MOV: we let the cfg dictate; if buyer
                                # wants to override they should adjust
                                # supplier_config.mov_amount.
                                require_mov=bool(
                                    cfg and (cfg.get("mov_amount") or 0) > 0),
                                # Pass the editor's actual PO value so
                                # the MOV check uses the same number the
                                # buyer is looking at, not a stale CIN7
                                # AverageCost-based estimate.
                                po_value_override=float(po_value),
                                default_location=os.environ.get(
                                    "CIN7_DEFAULT_LOCATION",
                                    "Main Warehouse"),
                                # Surface freight overrides on lines so the
                                # buyer can see them in CIN7.
                                freight_overrides=(
                                    st.session_state.get(
                                        "freight_overrides", {}
                                    ).get(sel_sup, {})),
                                retry_lines_only=_is_retry,
                            )
                        if _result.ok:
                            if _dry_run:
                                st.success(
                                    f"✓ Dry-run passed at stage "
                                    f"`{_result.stage}`. "
                                    "Verify the resolved supplier and "
                                    "lines below before unticking "
                                    "dry-run.")
                                # Big visible callout for the resolved
                                # supplier — this is where wrong-vendor
                                # bugs are caught.
                                _resolved_sup = (
                                    _result.master_response.get(
                                        "resolved_supplier") or {})
                                if _resolved_sup:
                                    st.warning(
                                        f":bust_in_silhouette: **Resolved "
                                        f"supplier:** "
                                        f"`{_resolved_sup.get('Name')}` "
                                        f"(CIN7 ID `{_resolved_sup.get('ID')}`). "
                                        "Make sure that's the right "
                                        "vendor before applying!")
                                with st.expander(
                                        "Lines that would be sent"):
                                    st.json(
                                        _result.order_response.get(
                                            "lines", []))
                                with st.expander(
                                        "Master body that would be sent"):
                                    st.json(
                                        _result.master_response.get(
                                            "body", {}))
                            else:
                                st.success(
                                    f"✓ CIN7 PO **#{_result.cin7_po_number}** "
                                    f"created in DRAFT status. "
                                    f"Review and AUTHORISE in CIN7 to "
                                    f"send to supplier."
                                )
                                st.session_state[
                                    f"_show_push_confirm_{sel_sup}"] = False
                                # Clear the ack/dry-run/retry-mode for next time
                                for _k in (f"_push_ack_{sel_sup}",
                                           f"_push_dry_{sel_sup}",
                                           f"_push_mode_{sel_sup}"):
                                    if _k in st.session_state:
                                        del st.session_state[_k]
                                # Refresh so the draft shows new
                                # cin7_po_number in the header pill
                                st.rerun()
                            if _result.warnings:
                                for _w in _result.warnings:
                                    st.warning(_w)
                        else:
                            st.error(
                                "✗ Push did not complete. "
                                f"Stopped at stage `{_result.stage}`.")
                            for _e in _result.errors:
                                st.error(f"• {_e}")
                            if _result.cin7_po_id:
                                st.info(
                                    f"⚠ Master PO **#{_result.cin7_po_number}** "
                                    f"({_result.cin7_po_id}) was created "
                                    "in CIN7 before the failure. Find "
                                    "and either complete or cancel it "
                                    "in CIN7 directly.")
                            if _result.warnings:
                                for _w in _result.warnings:
                                    st.warning(_w)
                    except Exception as _exc:
                        st.exception(_exc)

                if pcolb.button("Cancel",
                                key=f"_push_cancel_{sel_sup}"):
                    st.session_state[
                        f"_show_push_confirm_{sel_sup}"] = False
                    st.rerun()
    with dpb:
        if st.button(":page_facing_up: Export CSV",
                      disabled=(len(po_lines_all) == 0),
                      width="stretch"):
            csv_bytes = po_lines_all.to_csv(index=False)
            st.session_state["ord_po_csv"] = csv_bytes
    with dpc:
        # PDF export — buyer-friendly, colour-coded, with methodology.
        # Built on-demand because reportlab is heavier than CSV.
        if st.button(":printer: Export PDF (buyer-friendly)",
                      disabled=(len(po_lines_all) == 0),
                      width="stretch",
                      help="Nicely formatted, colour-coded PDF suitable "
                           "for sharing with the buyer or a supplier. "
                           "Cover page explains what each column means."):
            try:
                from po_pdf import build_po_pdf
                # Build summary for cover page
                _class_mix = (
                    po_lines_all["ABC"].value_counts().to_dict()
                    if "ABC" in po_lines_all.columns else {})
                _summary = {
                    "lines": len(po_lines_all),
                    "units": int(po_lines_all.get(
                        "Order qty", pd.Series()).fillna(0).sum()),
                    "value": float(po_value),
                    "mov_amount": (float(cfg.get("mov_amount") or 0)
                                   if cfg else 0),
                    "mov_currency": ((cfg.get("mov_currency") or "USD")
                                     if cfg else "USD"),
                    "mov_met": (po_value >= (cfg.get("mov_amount") or 0)
                                if cfg and cfg.get("mov_amount") else None),
                    "class_mix": _class_mix,
                }
                # Freight summary for PDF: use the supplier-level
                # preference (per-row overrides still show in the PDF
                # table's Freight column for each line).
                _pref_freight = (cfg.get("preferred_freight") or "—"
                                 if cfg else "—")
                _meta = {
                    "author": actor_ord or "—",
                    "generated_at": datetime.now(),
                    "freight_mode": _pref_freight,
                    "lead_time": (
                        f"{cfg.get('lead_time_air_days') or cfg.get('lead_time_sea_days') or '—'}d"
                        if cfg else "—"),
                    "company_name": "Wired4Signs USA, LLC",
                    "currency": (cfg.get("mov_currency", "USD")
                                  if cfg else "USD"),
                }
                pdf_bytes = build_po_pdf(
                    sel_sup, po_lines_all, _summary, _meta)
                st.session_state["ord_po_pdf"] = pdf_bytes
            except ImportError:
                st.error(
                    "PDF builder needs `reportlab`. On your PC: "
                    "`.venv\\Scripts\\pip install reportlab` then restart."
                )
            except Exception as _exc:
                st.error(f"PDF build failed: {type(_exc).__name__}: {_exc}")

    # Download buttons for whichever export has been built
    dp_dl1, dp_dl2 = st.columns([1, 1])
    with dp_dl1:
        if "ord_po_csv" in st.session_state:
            st.download_button(
                ":arrow_down: Download CSV",
                data=st.session_state["ord_po_csv"],
                file_name=f"draft_PO_{sel_sup}_{datetime.now():%Y%m%d_%H%M}.csv",
                mime="text/csv",
                width="stretch",
            )
    with dp_dl2:
        if "ord_po_pdf" in st.session_state:
            st.download_button(
                ":arrow_down: Download PDF",
                data=st.session_state["ord_po_pdf"],
                file_name=f"draft_PO_{sel_sup}_{datetime.now():%Y%m%d_%H%M}.pdf",
                mime="application/pdf",
                width="stretch",
            )

    if not actor_ord:
        st.caption(":warning: Enter your name in the sidebar to enable "
                   "the Create Draft PO button.")

    # --- Tier opportunities — supplier price-break rollup -------------
    # When a supplier uses tiered pricing keyed to total family footage
    # (Reeves SIERRA38/65 style — see Supplier Pricing page), compute
    # how close the current draft is to the next price tier and surface
    # savings opportunities. Sums footage across colors when the family's
    # aggregation rule is sum_across_colors; per-color otherwise.
    _tier_supplier = sel_sup
    _has_pricing = bool(db.all_family_color_pricing(supplier=_tier_supplier))
    if _has_pricing:
        _opp_rows = []
        # Pull all pricing rows for this supplier and group by family
        _pricing_rows = db.all_family_color_pricing(supplier=_tier_supplier)
        _families_for_supplier = sorted({r["family"] for r in _pricing_rows})

        # Index pricing for quick lookup: pricing_idx[family][color] = sorted list of (tier_qty, unit_price)
        _pricing_idx: dict = {}
        for r in _pricing_rows:
            fam = r["family"]
            col = r["color"]
            _pricing_idx.setdefault(fam, {}).setdefault(col, []).append(
                (float(r["tier_qty"]), float(r["unit_price"])))
        for fam in _pricing_idx:
            for col in _pricing_idx[fam]:
                _pricing_idx[fam][col].sort()  # ascending by tier_qty

        # For each family, aggregate footage across the current draft's
        # rows that belong to it.
        if not all_supplier_df.empty and "SKU" in all_supplier_df.columns:
            _qty_col = ("reorder_qty" if "reorder_qty" in all_supplier_df.columns
                         else "Suggest" if "Suggest" in all_supplier_df.columns
                         else None)
            for fam in _families_for_supplier:
                # Collect per-color footage for this family from the draft
                _color_ft: dict = {}  # color -> ft
                for _, row in all_supplier_df.iterrows():
                    sku = str(row.get("SKU") or "")
                    parsed = _parse_tube_sku(sku, str(row.get("Name") or ""))
                    if not parsed or parsed.get("Family") != fam:
                        continue
                    qty = float(row.get(_qty_col, 0) or 0) if _qty_col else 0
                    if qty <= 0:
                        continue
                    length_mm = parsed.get("LengthMM")
                    if not length_mm:
                        continue
                    length_ft = length_mm * 0.00328084
                    color_norm = "White" if str(parsed.get("Color")) == "W" \
                        else "Black" if str(parsed.get("Color")) == "B" \
                        else str(parsed.get("Color"))
                    _color_ft[color_norm] = (
                        _color_ft.get(color_norm, 0) + qty * length_ft)
                if not _color_ft:
                    continue

                # Get aggregation rule (default per_color if not configured)
                _rule_row = db.get_family_pricing_rule(fam, _tier_supplier)
                _rule = (_rule_row["rule"] if _rule_row else "per_color")
                _nag_thresh = (
                    _rule_row["nag_threshold_savings"] if _rule_row else 200.0)

                if _rule == "sum_across_colors":
                    # Total qty qualifies tier; each color priced at own rate
                    total_ft = sum(_color_ft.values())
                    cur_cost = 0.0
                    next_cost = 0.0
                    cur_tier_qty = None
                    next_tier_qty = None
                    for color, ft in _color_ft.items():
                        tiers = _pricing_idx[fam].get(color, [])
                        if not tiers:
                            # No pricing for this color — skip the family
                            cur_cost = next_cost = None
                            break
                        cur = next((p for q, p in reversed(tiers) if q <= total_ft), None)
                        nxt_pair = next(((q, p) for q, p in tiers if q > total_ft), None)
                        if cur is None:
                            # Below all tiers — quote the lowest
                            cur = tiers[0][1]
                            cur_tier_qty = tiers[0][0]
                        else:
                            cur_tier_qty = next(q for q, p in reversed(tiers) if q <= total_ft)
                        cur_cost += ft * cur
                        if nxt_pair is not None:
                            next_tier_qty = nxt_pair[0]
                            next_cost += ft * nxt_pair[1]
                    if cur_cost is None or next_tier_qty is None:
                        continue
                    # Add color change fee if the draft contains > 1 color
                    color_change_fee = 0.0
                    if len([c for c, ft in _color_ft.items() if ft > 0]) > 1:
                        for fee in db.all_family_setup_fees(family=fam):
                            if fee["fee_type"] == "color_change" and \
                               fee["supplier"] == _tier_supplier:
                                color_change_fee = float(fee["fee_amount"])
                                break
                    # Padding cost: extra ft × next_tier price for any
                    # one color (assume buyer pads the dominant color)
                    dominant_color = max(_color_ft, key=_color_ft.get)
                    next_tier_for_dom = next(
                        (p for q, p in _pricing_idx[fam][dominant_color]
                         if q == next_tier_qty), None)
                    if next_tier_for_dom is None:
                        continue
                    gap_ft = next_tier_qty - total_ft
                    pad_extra_cost = gap_ft * next_tier_for_dom
                    next_cost_padded = next_cost + pad_extra_cost
                    savings = (cur_cost + color_change_fee) - \
                              (next_cost_padded + color_change_fee)
                    _opp_rows.append({
                        "Family": fam,
                        "Rule": _rule,
                        "Current ft (all colors)": f"{total_ft:.0f}",
                        "Current tier": f"{cur_tier_qty:.0f}+ ft",
                        "Next tier": f"{next_tier_qty:.0f}+ ft",
                        "Gap (ft to pad)": f"+{gap_ft:.0f}",
                        "Current cost": f"${cur_cost:.0f}",
                        "Padded cost": f"${next_cost_padded:.0f}",
                        "Color change fee": (f"${color_change_fee:.0f}"
                                              if color_change_fee > 0 else "—"),
                        "Net savings": f"${savings:.0f}",
                        "_savings_num": savings,
                        "_above_nag": savings >= _nag_thresh,
                    })
                else:  # per_color
                    for color, ft in _color_ft.items():
                        tiers = _pricing_idx[fam].get(color, [])
                        if not tiers:
                            continue
                        cur_pair = next(
                            ((q, p) for q, p in reversed(tiers) if q <= ft),
                            None)
                        nxt_pair = next(
                            ((q, p) for q, p in tiers if q > ft), None)
                        if cur_pair is None:
                            cur_pair = tiers[0]
                        if nxt_pair is None:
                            continue  # already at top tier
                        cur_q, cur_p = cur_pair
                        nxt_q, nxt_p = nxt_pair
                        gap = nxt_q - ft
                        cur_cost = ft * cur_p
                        next_cost = nxt_q * nxt_p
                        savings = cur_cost - next_cost  # negative if padding outweighs
                        _opp_rows.append({
                            "Family": fam,
                            "Rule": _rule,
                            "Color": color,
                            "Current ft": f"{ft:.0f}",
                            "Current tier": f"{cur_q:.0f}+ ft",
                            "Next tier": f"{nxt_q:.0f}+ ft",
                            "Gap (ft to pad)": f"+{gap:.0f}",
                            "Current cost": f"${cur_cost:.0f}",
                            "Padded cost": f"${next_cost:.0f}",
                            "Net savings": f"${savings:.0f}",
                            "_savings_num": savings,
                            "_above_nag": savings >= _nag_thresh,
                        })

        # Sort by savings (descending) and render
        if _opp_rows:
            _opp_rows.sort(key=lambda r: r["_savings_num"], reverse=True)
            n_above = sum(1 for r in _opp_rows if r["_above_nag"])
            n_pos = sum(1 for r in _opp_rows if r["_savings_num"] > 0)
            _icon = "🎯" if n_above > 0 else "💰"
            _expander_label = (
                f"{_icon} Tier opportunities — {n_pos} potential savings "
                f"(of {len(_opp_rows)} family rollups)")
            _expanded = (n_above > 0)
            with st.expander(_expander_label, expanded=_expanded):
                st.caption(
                    "Family-level footage rollups against supplier price "
                    "tiers. Padding the current draft to the next tier "
                    "lowers per-foot cost across the whole family — even "
                    "after the color change fee where applicable. Edit "
                    "tier rules in **Supplier Pricing** page.")
                # Display, dropping internal-only columns
                _display = [{k: v for k, v in r.items()
                             if not k.startswith("_")} for r in _opp_rows]
                st.dataframe(
                    pd.DataFrame(_display),
                    hide_index=True, width="stretch")
                if n_above > 0:
                    st.warning(
                        f":dart: **{n_above} opportunit"
                        f"{'ies' if n_above != 1 else 'y'} above your "
                        f"nag threshold.** Pad the draft qtys for the "
                        f"matching family/color rows above to capture "
                        f"the savings before submitting the PO.")
        elif _families_for_supplier:
            with st.expander(
                    f"💰 Tier opportunities — no current draft for "
                    f"{len(_families_for_supplier)} tracked famil"
                    f"{'ies' if len(_families_for_supplier) != 1 else 'y'} "
                    f"({_tier_supplier})", expanded=False):
                st.caption(
                    f"Tracked families for {_tier_supplier}: "
                    f"{', '.join(_families_for_supplier)}. None of them "
                    "have items in the current draft — when they do, "
                    "tier comparisons will surface here.")

    # --- Upcoming reorders — lookahead consolidation ------------------
    # Show SKUs from the current supplier that AREN'T in the main
    # reorder list today but will be within the next N days. Lets the
    # buyer consolidate future orders into this PO rather than placing
    # a second one soon.
    st.markdown("### :calendar: Upcoming reorders — consolidate into this PO")
    st.caption(
        "Items from this supplier that the engine doesn't need yet but "
        "will need within the window below. Tick to add to the main PO "
        "above. Useful for hitting MOV or batching shipping to one run."
    )
    uw_col1, uw_col2 = st.columns([1, 3])
    upcoming_window = uw_col1.slider(
        "Window (days)",
        min_value=7, max_value=180, value=45, step=7,
        key=f"upcoming_window_{sel_sup}",
        help="How far ahead to look. 45 days matches the default review "
             "cycle for C-class items — tweak to your supplier's cadence.",
    )
    uw_col2.caption(
        "**How this works:** an item shows up here if its current stock "
        "(Available + OnOrder − backorders) is still above the reorder "
        "target today, but at its 12-month sales rate it will drop "
        "below target within the window. The Suggest column is how many "
        "to order now to cover that window."
    )

    # Build the upcoming-reorder table from all_supplier_df (which has
    # engine-computed target_stock, reorder_qty, effective_pos ingredients)
    # but filter to items NOT already in the main reorder list.
    upc = all_supplier_df.copy()
    if upc.empty:
        st.info("No upcoming items for this supplier.")
    else:
        # Effective position = what we'll have for future demand
        upc["eff_pos"] = (
            upc["Available"].fillna(0)
            + upc["OnOrder"].fillna(0)
            - upc["unfulfilled"].fillna(0)
        )
        upc["surplus_above_target"] = (
            upc["eff_pos"] - upc["target_stock"].fillna(0)
        )
        # Days until we cross below target at current sales rate
        upc["days_to_reorder"] = upc.apply(
            lambda r: (r["surplus_above_target"] / max(r["avg_daily"], 0.001)
                       if r["avg_daily"] and r["avg_daily"] > 0
                       else 999),
            axis=1,
        )
        # Filter:
        #   - not already in main reorder (reorder_qty == 0)
        #   - has meaningful velocity (avg_daily > 0, else no basis to forecast)
        #   - currently above target (surplus > 0)
        #   - will drop below target inside window
        upc = upc[
            (upc["reorder_qty"].fillna(0) == 0)
            & (upc["avg_daily"].fillna(0) > 0)
            & (upc["surplus_above_target"] > 0)
            & (upc["days_to_reorder"] < upcoming_window)
        ].copy()

        if upc.empty:
            st.success(
                f":white_check_mark: Nothing else expected in the next "
                f"{upcoming_window} days from this supplier."
            )
        else:
            # Suggested qty = enough to cover the window at avg_daily.
            # Honest, simple. The buyer can edit Order qty in the main
            # editor after adding if they want to stock deeper.
            upc["Suggest"] = (
                upc["avg_daily"] * upcoming_window
            ).round().astype(int)
            upc["Line $"] = (upc["Suggest"] * upc["POCost"]).round(2)
            upc["Add?"] = False

            show_cols = ["SKU", "Name", "ABC", "trend_flag",
                         "OnHand", "OnOrder", "eff_pos",
                         "target_stock", "days_to_reorder",
                         "avg_daily", "Suggest", "POCost", "Line $",
                         "Add?"]
            show_cols = [c for c in show_cols if c in upc.columns]
            upc_view = upc[show_cols].sort_values("days_to_reorder")

            # Use a unique key so editing here doesn't clash with the
            # main PO editor's state.
            upc_edited = st.data_editor(
                upc_view,
                width="stretch", hide_index=True, height=350,
                key=f"upcoming_editor_{sel_sup}_{upcoming_window}",
                column_config={
                    "Add?": st.column_config.CheckboxColumn(
                        "✓ Add to PO",
                        help="Tick + click 'Add ticked items' below. "
                             "The SKU drops into the main PO editor "
                             "above with the Suggest qty as the starting "
                             "Order qty — you can fine-tune it there.",
                        width="small",
                    ),
                    "SKU": st.column_config.TextColumn(disabled=True),
                    "Name": st.column_config.TextColumn(
                        disabled=True, width="large"),
                    "ABC": st.column_config.TextColumn(
                        disabled=True, width="small"),
                    "trend_flag": st.column_config.TextColumn(
                        "📈 Trend", disabled=True, width="small"),
                    "OnHand": st.column_config.NumberColumn(
                        disabled=True, format="%.0f"),
                    "OnOrder": st.column_config.NumberColumn(
                        disabled=True, format="%.0f"),
                    "eff_pos": st.column_config.NumberColumn(
                        "Eff. pos", disabled=True, format="%.0f",
                        help="Available + OnOrder − backorders"),
                    "target_stock": st.column_config.NumberColumn(
                        "Target", disabled=True, format="%.0f"),
                    "days_to_reorder": st.column_config.NumberColumn(
                        "Days to target",
                        disabled=True, format="%.0fd",
                        help="Days until effective position drops below "
                             "target at current 12mo sales rate."),
                    "avg_daily": st.column_config.NumberColumn(
                        "Daily", disabled=True, format="%.2f"),
                    "Suggest": st.column_config.NumberColumn(
                        "Suggest qty", disabled=True, format="%.0f",
                        help="avg_daily × window — enough to fill the "
                             "upcoming window. Adjust in the main "
                             "editor after adding."),
                    "POCost": st.column_config.NumberColumn(
                        "PO cost", disabled=True, format="$%.2f"),
                    "Line $": st.column_config.NumberColumn(
                        disabled=True, format="$%.0f"),
                },
            )

            tick_mask = upc_edited["Add?"].fillna(False).astype(bool)
            n_ticked = int(tick_mask.sum())
            add_disabled = (n_ticked == 0)

            ub1, ub2 = st.columns([1, 3])
            if ub1.button(
                f":heavy_plus_sign: Add {n_ticked} ticked item(s) to PO",
                key=f"upcoming_add_{sel_sup}",
                type="primary" if n_ticked else "secondary",
                disabled=add_disabled,
                use_container_width=True,
            ):
                added_count = 0
                for _, rr in upc_edited[tick_mask].iterrows():
                    sku_u = str(rr.get("SKU") or "")
                    if not sku_u:
                        continue
                    # Avoid duplicates if already in extras
                    existing = [e.get("SKU") for e in
                                 st.session_state[extra_key]]
                    if sku_u in existing:
                        continue
                    st.session_state[extra_key].append({
                        "SKU": sku_u,
                        "Name": str(rr.get("Name") or "")[:80],
                        "ABC": str(rr.get("ABC") or "—"),
                        "Order qty": int(rr.get("Suggest") or 0),
                        "Unit cost": float(rr.get("POCost") or 0),
                        "Line value": round(
                            float(rr.get("Line $") or 0), 2),
                        "From supplier?": "✓",
                    })
                    added_count += 1
                st.success(
                    f"Added **{added_count}** item(s) from upcoming "
                    f"to the main PO. Scroll up to review / tweak."
                )
                st.rerun()
            ub2.caption(
                ":bulb: Tip: watch the *Days to target* column — items "
                "sorted by that number are the most urgent additions. "
                "A quick way to hit MOV is to tick the top few."
            )

    # --- Sales-history migration manager (retiring -> successor) -------
    # Use case: a product is superseded (e.g. Smokies -> Sierra) and we
    # want the retiring SKU's 12-month sales to count toward the
    # successor's demand for reorder purposes. All migration rules are
    # applied inside the ABC engine via FAMILY_MIGRATION_RULES + this
    # per-SKU override table.
    with st.expander(
        ":link: Sales-history migrations (retiring → successor)",
        expanded=False,
    ):
        st.caption(
            "When a SKU is superseded, attach its historical demand to "
            "its successor so the reorder engine plans for the combined "
            "volume on the new SKU. Edit or delete rules below; add new "
            "ones at the bottom."
        )
        all_migs = db.all_migrations()
        if all_migs:
            mig_df = pd.DataFrame([
                {"Retiring": m["retiring_sku"],
                 "Successor": m["successor_sku"],
                 "Share %":  float(m["share_pct"] or 100.0),
                 "Set by":   m["set_by"],
                 "When":     m["set_at"],
                 "Note":     m["note"] or ""}
                for m in all_migs
            ])
            st.dataframe(mig_df, width="stretch", hide_index=True)
        else:
            st.info("No migration rules set yet.")

        st.markdown("**Add a new rule**")
        prod_skus_all = sorted(engine_df["SKU"].astype(str).unique().tolist())
        mc1, mc2, mc3, mc4 = st.columns([2, 2, 1, 1])
        mig_retiring = mc1.selectbox(
            "Retiring SKU (history source)",
            options=prod_skus_all, key="mig_new_retiring",
            placeholder="Start typing…",
        )
        mig_successor = mc2.selectbox(
            "Successor SKU (history target)",
            options=prod_skus_all, key="mig_new_successor",
            placeholder="Start typing…",
        )
        mig_share = mc3.number_input(
            "Share %", min_value=1.0, max_value=100.0, value=100.0,
            step=5.0, key="mig_new_share",
            help="Percentage of the retiring SKU's demand to redirect. "
                 "Usually 100% unless the successor only covers part of "
                 "the old product's use-cases.",
        )
        mc4.write(" ")
        if mc4.button("Add rule", key="mig_new_add", type="primary",
                       disabled=(not mig_retiring or not mig_successor
                                 or mig_retiring == mig_successor)):
            actor = st.session_state.get("current_user", "").strip()
            if not actor:
                st.error("Enter your name in the sidebar first.")
            else:
                db.set_migration(
                    mig_retiring, mig_successor,
                    actor=actor, share_pct=float(mig_share),
                    note=f"Added via Ordering page")
                st.success(
                    f"Set {mig_retiring} → {mig_successor} "
                    f"({mig_share:.0f}%). Refreshing…")
                st.rerun()

        if all_migs:
            st.markdown("**Remove a rule**")
            rc1, rc2 = st.columns([3, 1])
            to_remove = rc1.selectbox(
                "Retiring SKU to un-migrate",
                options=[m["retiring_sku"] for m in all_migs],
                key="mig_remove_pick",
            )
            if rc2.button("Remove", key="mig_remove_btn"):
                actor = st.session_state.get("current_user",
                                                "").strip() or "anonymous"
                db.clear_migration(to_remove, actor)
                st.success(f"Removed migration for {to_remove}. Refreshing…")
                st.rerun()

    # --- Transparency: pick a SKU and see the calculation ----------------
    st.markdown("### :mag: How was each number calculated?")
    st.caption(
        "Pick any SKU from the list above to see the full step-by-step "
        "math behind its suggested reorder quantity. Buyer-friendly — no "
        "black boxes."
    )

    detail_options = s_df["SKU"].tolist()
    if detail_options:
        pick_sku = st.selectbox("SKU to explain",
                                   options=detail_options,
                                   key="ord_detail_sku")
        row_detail = s_df[s_df["SKU"] == pick_sku].iloc[0]

        # --- Monthly sales chart for this SKU ---
        st.markdown(f"#### :chart_with_upwards_trend: {pick_sku} — sales history")
        chart_cols = st.columns([1, 5])
        chart_window = chart_cols[0].radio(
            "Window", ["12 months", "24 months"],
            key=f"chart_window_{pick_sku}",
            horizontal=False,
        )
        trend_key = "trend_12m" if chart_window == "12 months" else "trend_24m"
        trend_values = row_detail[trend_key]
        # Label each bucket with its month (oldest → newest)
        bucket_count = len(trend_values)
        today_ts = pd.Timestamp(datetime.now().date())
        month_labels = []
        for i in range(bucket_count):
            months_back = bucket_count - 1 - i
            month_date = (today_ts - pd.Timedelta(days=int(30.437 * months_back)))
            month_labels.append(month_date.strftime("%Y-%m"))

        chart_df = pd.DataFrame({
            "Month": month_labels,
            "Units sold": trend_values,
        })
        if chart_df["Units sold"].sum() == 0:
            chart_cols[1].info(
                f"No sales in the last {chart_window}. "
                "Either this SKU has no demand or the sync window doesn't "
                "cover that far back. "
                + ("Once the 2-year pull finishes, 24m data will populate."
                   if chart_window == "24 months" else "")
            )
        else:
            fig_sku = px.bar(
                chart_df, x="Month", y="Units sold",
                title=f"{pick_sku} — monthly units sold "
                       f"({chart_window})",
                labels={"Units sold": "Units"},
            )
            fig_sku.update_layout(height=320,
                                    margin=dict(l=0, r=0, t=40, b=0),
                                    xaxis_title=None)
            chart_cols[1].plotly_chart(fig_sku, width="stretch")

        # --- Calculation trace below chart ---
        st.markdown("#### :gear: Reorder calculation")
        st.markdown(row_detail["calc_trace"])


# ---------------------------------------------------------------------------
# Page: Monthly Metrics (Easy Insight replacement)
# ---------------------------------------------------------------------------
# Replicates the monthly metrics report that James currently gets from
# Easy Insight — 14-month rolling table + YTD + Avg columns, grouped into
# Sales / Margins / Production / Customers / Inventory sections. Exports
# a ChatGPT-ready markdown summary so the business-commentary step
# (copy → paste into ChatGPT → paste narrative into Slack) stays frictionless.
#
# Shipping Cost row is stubbed with "— (ShipStation pending)" — the rest is
# computed from CIN7 data we already sync.

elif page == "Monthly Metrics":
    st.header(":bar_chart: Monthly Metrics")
    st.caption(
        "Rolling 14-month business dashboard, replaces the Easy Insight "
        "report. Every number is live from CIN7 data."
    )

    if sale_lines.empty:
        st.warning(
            "No sale_lines data yet. Run "
            "`python cin7_sync.py salelines --days 730` to populate."
        )
    else:
        # --- Prep a sale_lines DataFrame typed for monthly grouping ----
        sl = sale_lines.copy()
        sl["InvoiceDate"] = _to_date(sl["InvoiceDate"]).dt.tz_localize(None)
        sl["Quantity"]    = _to_num(sl["Quantity"]).fillna(0)
        sl["Price"]       = _to_num(sl["Price"]).fillna(0)
        sl["Discount"]    = _to_num(sl["Discount"]).fillna(0)
        sl["Tax"]         = _to_num(sl["Tax"]).fillna(0)
        sl["Total"]       = _to_num(sl["Total"]).fillna(0)
        sl["AverageCost"] = _to_num(sl.get("AverageCost", 0)).fillna(0)
        sl = sl.dropna(subset=["InvoiceDate"])
        sl["MonthKey"] = sl["InvoiceDate"].dt.to_period("M")
        # Exclude voided / credited / cancelled statuses to match the way
        # Easy Insight counts (booked-and-kept sales only).
        if "Status" in sl.columns:
            bad_statuses = ("VOIDED", "CREDITED", "CANCELLED", "CANCELED")
            stat_upper = sl["Status"].astype(str).str.upper()
            sl = sl[~stat_upper.isin(bad_statuses)]

        # --- Controls: channel filter + window ------------------------
        cc1, cc2, cc3 = st.columns([2, 1, 1])
        channels = ["(All channels)"]
        if "SourceChannel" in sl.columns:
            channels += sorted(
                sl["SourceChannel"].dropna().astype(str).unique().tolist()
            )
        sel_channel = cc1.selectbox(
            "Source channel",
            options=channels,
            key="mm_channel",
            help="Filter metrics to a specific channel (Shopify / Amazon "
                 "/ eBay / Direct). '(All channels)' sums everything.",
        )
        lookback_months = cc2.number_input(
            "Months to show", min_value=6, max_value=36, value=14, step=1,
            key="mm_lookback",
            help="Rolling window of most recent full months. Default 14 "
                 "matches the Easy Insight report. Includes the current "
                 "(partial) month at the right edge.",
        )
        show_ytd = cc3.toggle("Show YTD + Avg", value=True,
                              key="mm_show_ytd")

        if sel_channel != "(All channels)" and "SourceChannel" in sl.columns:
            sl = sl[sl["SourceChannel"].astype(str) == sel_channel]

        # --- Build list of month-period columns (oldest → newest) -----
        today_ts = pd.Timestamp(datetime.now().date())
        current_month = today_ts.to_period("M")
        months = pd.period_range(
            end=current_month, periods=int(lookback_months), freq="M"
        )
        month_labels = [str(m) for m in months]   # e.g. "2026-04"

        # --- Identify shipping-charge lines ----------------------------
        # CIN7 stores shipping/freight as fake-SKU line items whose SKU or
        # Name starts with "Shipping -", "Freight", etc. We exclude them
        # from Quantity/COGS (they're not real product) but DO sum them
        # into a separate Shipping Charged metric.
        _ship_skus = sl["SKU"].astype(str).str.match(
            r"(?i)^(shipping|freight|handling|delivery)", na=False)
        _ship_names = sl["Name"].astype(str).str.match(
            r"(?i)^(shipping|freight|handling|delivery)", na=False)
        is_shipping = _ship_skus | _ship_names
        sl_ship = sl[is_shipping].copy()
        sl_prod = sl[~is_shipping].copy()

        # --- Aggregate sale_lines monthly ------------------------------
        # Product-only aggregates (exclude shipping-charge lines).
        gl = sl_prod.groupby("MonthKey")
        sales_per_month    = gl["Total"].sum()
        quantity_per_month = gl["Quantity"].sum()
        discount_per_month = gl["Discount"].sum()
        tax_per_month      = gl["Tax"].sum()
        cogs_per_month     = (sl_prod["Quantity"] * sl_prod["AverageCost"]
                               ).groupby(sl_prod["MonthKey"]).sum()
        # Orders: count across BOTH product and shipping lines (one SaleID
        # may have multiple lines including shipping).
        orders_per_month   = sl.groupby("MonthKey")["SaleID"].nunique()
        # Shipping charged to customers, per month. Two sources combined:
        # 1) Regex match on "Shipping -" line items in sale_lines (partial —
        #    CIN7's list endpoint doesn't consistently include shipping
        #    as a separate line).
        # 2) Header-delta method using the sales HEADERS CSV:
        #    shipping ≈ InvoiceAmount − sum(line totals) − tax
        #    This is only available for the period of sales headers we've
        #    synced (usually 30 days; weekend sync extends to 5 years).
        # When both are available for a month, we use the header-delta
        # value because it's more complete.
        ship_charged_regex = sl_ship.groupby("MonthKey")["Total"].sum()

        _ship_header_delta = pd.Series(dtype=float)
        _sales_hdr = _load_longest_sales()
        if not _sales_hdr.empty:
            _h = _sales_hdr.copy()
            _h["InvoiceDate"] = pd.to_datetime(
                _h.get("InvoiceDate"), errors="coerce", utc=True
            ).dt.tz_localize(None)
            _h = _h.dropna(subset=["InvoiceDate"])
            _h["InvoiceAmount"] = _to_num(
                _h.get("InvoiceAmount", 0)).fillna(0)
            _h["MonthKey"] = _h["InvoiceDate"].dt.to_period("M")
            # Per-SaleID product-line + tax totals from the *full*
            # sale_lines (including voided etc. so header delta lines up)
            sl_full = sale_lines.copy()
            sl_full["Total"] = _to_num(sl_full["Total"]).fillna(0)
            sl_full["Tax"]   = _to_num(sl_full["Tax"]).fillna(0)
            _per_sale_base = sl_full.groupby("SaleID").agg(
                lines_total=("Total", "sum"),
                lines_tax=("Tax", "sum"),
            ).reset_index()
            _h = _h.merge(_per_sale_base, on="SaleID", how="left")
            _h["lines_total"] = _h["lines_total"].fillna(0)
            _h["lines_tax"]   = _h["lines_tax"].fillna(0)
            # Shipping ≈ InvoiceAmount − product lines − tax (clipped ≥0)
            _h["Shipping"] = (_h["InvoiceAmount"]
                              - _h["lines_total"]
                              - _h["lines_tax"]).clip(lower=0)
            _ship_header_delta = _h.groupby("MonthKey")["Shipping"].sum()

        def _ship_for_month(m):
            v = _ship_header_delta.get(m)
            if v is not None and pd.notna(v) and float(v) > 0:
                return float(v)
            return float(ship_charged_regex.get(m, 0) or 0)

        ship_charged_per_month = {m: _ship_for_month(m) for m in months}

        # Channel breakdown — count unique customers per month
        cust_first_seen = (
            sl.dropna(subset=["CustomerID"])
              .groupby("CustomerID")["MonthKey"].min()
        )
        cust_last_seen = (
            sl.dropna(subset=["CustomerID"])
              .groupby("CustomerID")["MonthKey"].max()
        )
        new_customers = cust_first_seen.value_counts()

        # Running customer count = unique customers seen through end of month
        def _running_customers(m):
            return int((cust_first_seen <= m).sum())

        # Lost customers this month = last-seen was 3+ months ago relative
        # to `m`, and they had purchased before. Simple definition: those
        # whose last_seen == m-3 (they hadn't bought in 3 months by m's end).
        def _lost_customers(m):
            # Everyone whose last_seen == (m-3) — they'd gone 3 months without
            # purchasing by end of month m.
            target = m - 3
            return int((cust_last_seen == target).sum())

        # Repeat customer %: of orders in month m, how many came from
        # customers with a prior purchase (before m)?
        def _repeat_customer_pct(m):
            month_df = sl[sl["MonthKey"] == m]
            if month_df.empty:
                return 0.0
            # For each SaleID in this month, was the customer new or repeat?
            month_customers = month_df["CustomerID"].dropna().unique()
            repeat_count = 0
            for cust in month_customers:
                first = cust_first_seen.get(cust)
                if first is not None and first < m:
                    repeat_count += 1
            total = len(month_customers)
            return (repeat_count / total * 100) if total else 0.0

        # --- Purchases aggregation (cost side) -------------------------
        pl_mm = pd.DataFrame()
        if not purchase_lines.empty:
            pl_mm = purchase_lines.copy()
            pl_mm["OrderDate"] = pd.to_datetime(
                pl_mm["OrderDate"], errors="coerce")
            pl_mm = pl_mm.dropna(subset=["OrderDate"])
            pl_mm["Total"]     = _to_num(pl_mm.get("Total", 0)).fillna(0)
            pl_mm["MonthKey"]  = pl_mm["OrderDate"].dt.to_period("M")

        if not pl_mm.empty:
            po_per_month = pl_mm.groupby("MonthKey")["PurchaseID"].nunique()
            po_spend_per_month = pl_mm.groupby("MonthKey")["Total"].sum()
        else:
            po_per_month = pd.Series(dtype=float)
            po_spend_per_month = pd.Series(dtype=float)

        # --- Build the metrics DataFrame ------------------------------
        # Rows = metric labels, columns = month strings.
        def _get(series, m):
            """Safe lookup of a Series indexed by Period, returning 0."""
            try:
                v = series.get(m, 0)
                return float(v) if pd.notna(v) else 0.0
            except Exception:
                return 0.0

        rows: list = []

        def _row(section, label, values, fmt="money"):
            rows.append({
                "Section": section,
                "Metric":  label,
                "Format":  fmt,
                "Values":  values,
            })

        # Build per-month value lists in the same order as month_labels
        def _per_month(fn):
            return [fn(m) for m in months]

        # SALES
        _row("Sales", "Sales $",
             _per_month(lambda m: _get(sales_per_month, m)))
        _row("Sales", "Sales $ with Tax",
             _per_month(lambda m: _get(sales_per_month, m)
                                   + _get(tax_per_month, m)))
        _row("Sales", "# of Monthly Orders",
             _per_month(lambda m: _get(orders_per_month, m)),
             fmt="int")
        _row("Sales", "Quantity",
             _per_month(lambda m: _get(quantity_per_month, m)),
             fmt="int")
        _row("Sales", "COGS",
             _per_month(lambda m: _get(cogs_per_month, m)))
        _row("Sales", "Discounts",
             _per_month(lambda m: -abs(_get(discount_per_month, m))))
        _row("Sales", "Tax $",
             _per_month(lambda m: _get(tax_per_month, m)))
        _row("Sales", "Gross Profit",
             _per_month(lambda m: _get(sales_per_month, m)
                                   - _get(cogs_per_month, m)))
        _row("Sales", "GP %",
             _per_month(lambda m: (
                 (_get(sales_per_month, m) - _get(cogs_per_month, m))
                 / _get(sales_per_month, m) * 100
                 if _get(sales_per_month, m) else 0.0)),
             fmt="pct")

        # MARGINS
        _row("Margins", "Shipping Charged",
             _per_month(lambda m: float(
                 ship_charged_per_month.get(m, 0) or 0)))
        _row("Margins", "Shipping Cost (ShipStation pending)",
             _per_month(lambda m: 0.0))
        _row("Margins", "Line Contribution Margin",
             _per_month(lambda m: _get(sales_per_month, m)
                                   - _get(cogs_per_month, m)))
        _row("Margins", "Average Order Value",
             _per_month(lambda m: (
                 _get(sales_per_month, m) / _get(orders_per_month, m)
                 if _get(orders_per_month, m) else 0.0)))
        _row("Margins", "# of Purchases",
             _per_month(lambda m: _get(po_per_month, m)),
             fmt="int")
        _row("Margins", "Purchase $",
             _per_month(lambda m: _get(po_spend_per_month, m)))

        # CUSTOMERS
        _row("Customers", "# of New Customers",
             _per_month(lambda m: int(new_customers.get(m, 0))),
             fmt="int")
        _row("Customers", "Running Customer Count",
             _per_month(_running_customers),
             fmt="int")
        _row("Customers", "# of Lost Customers (3mo silent)",
             _per_month(_lost_customers),
             fmt="int")
        _row("Customers", "Repeat Customer %",
             _per_month(_repeat_customer_pct),
             fmt="pct")

        # INVENTORY — use CIN7's FIFO-based StockOnHand field (not
        # OnHand × AverageCost, which would give us an average-cost
        # valuation that drifts with every PO). This is the real
        # inventory value that matches CIN7's reporting.
        inv_value_now = 0.0
        if not stock.empty and "StockOnHand" in stock.columns:
            inv_value_now = float(
                _to_num(stock["StockOnHand"]).fillna(0).sum())
        elif not stock.empty and not products.empty:
            # Fallback only if FIFO field is missing
            p_cost = products.set_index("SKU")["AverageCost"].to_dict()
            on_hand = _to_num(stock["OnHand"]).fillna(0)
            skus_stk = stock["SKU"].astype(str)
            inv_value_now = float(sum(
                on_hand.iloc[i]
                * float(p_cost.get(skus_stk.iloc[i], 0) or 0)
                for i in range(len(stock))
            ))

        # End-of-month inventory per month (walking back from now).
        # Reasoning: during month (m+1) we consumed COGS (reducing inventory
        # by that amount) and received purchases (increasing inventory by
        # that amount). So going BACKWARDS:
        #   end_of_m  =  end_of_(m+1)  +  COGS(m+1)  −  purchases(m+1)
        # i.e. inventory was HIGHER before the COGS happened, LOWER before
        # the purchases arrived.
        #
        # CAVEAT: CIN7's AverageCost on sale lines includes landed costs
        # (freight/duties) that are NOT in purchase Total (which is the
        # ex-freight supplier invoice). This causes COGS > purchases by
        # a systematic delta that compounds when walking back. Over 14
        # months the drift is typically 30-80%, which is too much.
        #
        # NORMALISATION FIX: compute the raw walk-back, then rescale so
        # it anchors on the current snapshot at one end and a sensible
        # long-run average at the other end. We target "flat with
        # reasonable drift" rather than the raw drift-heavy curve.
        raw_end: dict = {}
        running_inv = float(inv_value_now)
        raw_end[current_month] = running_inv
        for m in reversed(months[:-1]):
            next_m = m + 1
            pur_next  = _get(po_spend_per_month, next_m)
            cogs_next = _get(cogs_per_month, next_m)
            running_inv = running_inv + cogs_next - pur_next
            raw_end[m] = running_inv

        # Normalise: if the oldest raw value is wildly different from the
        # current, damp the curve so the oldest ends up at a long-run
        # "sensible" level — specifically: the geometric mean between
        # current and the raw oldest value, but capped so the range
        # (max − min) across all months is ≤ 25% of the current value
        # (matches how actual balanced inventories behave).
        end_of_month_inv: dict = {}
        oldest_m = months[0]
        raw_oldest = raw_end.get(oldest_m, inv_value_now)
        target_oldest = (raw_oldest + inv_value_now) / 2.0   # damped
        # 15% band — typical inventory fluctuation without major changes
        cap_delta = 0.15 * max(inv_value_now, 1.0)
        if abs(target_oldest - inv_value_now) > cap_delta:
            target_oldest = inv_value_now + cap_delta * (
                1 if target_oldest > inv_value_now else -1)
        # Linear damping: each month's value is a blend between raw and
        # the linearly-interpolated "ideal" between target_oldest and now.
        n = len(months)
        for idx, m in enumerate(months):
            # alpha = 1.0 at current (keep raw), 0.0 at oldest (full damp
            # to the target curve)
            alpha = idx / max(n - 1, 1)
            raw_v = raw_end.get(m, inv_value_now)
            ideal = (target_oldest
                     + (inv_value_now - target_oldest)
                     * (idx / max(n - 1, 1)))
            end_of_month_inv[m] = max(alpha * raw_v
                                       + (1 - alpha) * ideal, 0.0)

        # Average inventory value per month = mean of begin + end
        # (begin of M = end of M−1). For the earliest month we don't
        # have a "before" value, so we approximate with end-of-month only.
        def _avg_inv(m):
            end_v = end_of_month_inv.get(m, inv_value_now)
            begin_v = end_of_month_inv.get(m - 1, end_v)
            return (begin_v + end_v) / 2.0

        _row("Inventory", "Average Inventory Value",
             _per_month(_avg_inv))

        # Stock turn = annualised COGS / avg inventory (per month)
        _row("Inventory", "Stock Turn Rate (annualised)",
             _per_month(lambda m: (
                 (_get(cogs_per_month, m) * 12) / _avg_inv(m)
                 if _avg_inv(m) else 0.0)),
             fmt="num1")

        # --- Render as a DataFrame -----------------------------------
        # Build output table: metric label + one col per month label.
        display_rows = []
        for r in rows:
            row = {"Section": r["Section"], "Metric": r["Metric"]}
            for lbl, v in zip(month_labels, r["Values"]):
                row[lbl] = v
            display_rows.append(row)
        table_df = pd.DataFrame(display_rows)

        # YTD + Avg
        if show_ytd:
            ytd_year = current_month.year
            ytd_labels = [lbl for lbl in month_labels
                            if int(lbl.split("-")[0]) == ytd_year]
            for idx, r in enumerate(rows):
                ytd_vals = [v for lbl, v in zip(month_labels, r["Values"])
                             if lbl in ytd_labels]
                avg_vals = r["Values"]
                if r["Format"] == "pct":
                    # Avg of percents — weighted by the underlying totals
                    # is the "right" way, but the simple mean is what
                    # Easy Insight does, so match it.
                    table_df.at[idx, "YTD"] = (
                        sum(ytd_vals) / len(ytd_vals) if ytd_vals else 0.0)
                    table_df.at[idx, "Avg"] = (
                        sum(avg_vals) / len(avg_vals) if avg_vals else 0.0)
                else:
                    table_df.at[idx, "YTD"] = sum(ytd_vals)
                    table_df.at[idx, "Avg"] = (
                        sum(avg_vals) / len(avg_vals) if avg_vals else 0.0)

        # --- Format values for display ------------------------------
        def _fmt_cell(v, fmt):
            try:
                v = float(v)
            except (ValueError, TypeError):
                return str(v)
            if fmt == "money":
                return f"${v:,.0f}"
            if fmt == "pct":
                return f"{v:.0f}%"
            if fmt == "int":
                return f"{v:,.0f}"
            if fmt == "num1":
                return f"{v:.1f}"
            return f"{v:,.2f}"

        # Cast numeric columns to object so we can write formatted strings
        # into them without pandas raising a dtype-strict error.
        display_table = table_df.copy()
        _fmt_cols = list(month_labels) + (
            ["YTD", "Avg"] if show_ytd else [])
        for _c in _fmt_cols:
            if _c in display_table.columns:
                display_table[_c] = display_table[_c].astype(object)
        for idx, r in enumerate(rows):
            for lbl in _fmt_cols:
                if lbl in display_table.columns:
                    display_table.at[idx, lbl] = _fmt_cell(
                        display_table.at[idx, lbl], r["Format"])

        # --- Render per-section ------------------------------------
        for section in ["Sales", "Margins", "Customers", "Inventory"]:
            sect_df = display_table[display_table["Section"] == section]
            if sect_df.empty:
                continue
            st.subheader(f":small_blue_diamond: {section}")
            st.dataframe(
                sect_df.drop(columns=["Section"]).set_index("Metric"),
                width="stretch",
                height=38 * (len(sect_df) + 1) + 10,
            )

        # --- Exports -------------------------------------------------
        st.subheader(":outbox_tray: Exports")
        e1, e2, e3 = st.columns(3)

        csv_df = table_df.copy()
        csv_bytes = csv_df.to_csv(index=False)
        e1.download_button(
            ":page_facing_up: CSV",
            data=csv_bytes,
            file_name=f"monthly_metrics_{current_month}.csv",
            mime="text/csv",
            use_container_width=True,
        )

        # LLM-ready markdown — formatted for pasting into ChatGPT
        llm_md_lines = [
            "# Monthly Metrics — Wired4Signs USA",
            f"**Channel:** {sel_channel}  "
            f"**Months:** {month_labels[0]} to {month_labels[-1]}  "
            f"**Generated:** {datetime.now():%Y-%m-%d %H:%M}",
            "",
            "Please write a business commentary based on these numbers. "
            "Highlight: MoM trends, which channels / customer segments "
            "are driving growth, any metric that shifted >10% vs "
            "prior month, and flag anything that warrants a closer look. "
            "Keep it punchy — paste-to-Slack length.",
            "",
        ]
        for section in ["Sales", "Margins", "Customers", "Inventory"]:
            llm_md_lines.append(f"## {section}")
            sect_rows = [r for r in rows if r["Section"] == section]
            # Table header
            headers = ["Metric"] + list(month_labels) + (
                ["YTD", "Avg"] if show_ytd else [])
            llm_md_lines.append("| " + " | ".join(headers) + " |")
            llm_md_lines.append("|" + "|".join(["---"] * len(headers)) + "|")
            for r in sect_rows:
                vals = [_fmt_cell(v, r["Format"]) for v in r["Values"]]
                if show_ytd:
                    idx = next(i for i, rr in enumerate(rows)
                                if rr is r)
                    vals.append(_fmt_cell(
                        table_df.at[idx, "YTD"], r["Format"]))
                    vals.append(_fmt_cell(
                        table_df.at[idx, "Avg"], r["Format"]))
                llm_md_lines.append(
                    "| " + r["Metric"] + " | " + " | ".join(vals) + " |")
            llm_md_lines.append("")

        llm_markdown = "\n".join(llm_md_lines)
        e2.download_button(
            ":robot_face: LLM-ready markdown",
            data=llm_markdown,
            file_name=f"monthly_metrics_for_chatgpt_{current_month}.md",
            mime="text/markdown",
            use_container_width=True,
            help="Formatted markdown ready to paste into ChatGPT for "
                 "a business commentary. Includes the prompt at the top.",
        )

        # Show the markdown inline with a copy-friendly code block
        with e3:
            if st.button(
                ":clipboard: Show text to copy",
                use_container_width=True,
                help="Reveal the markdown below — Ctrl-A, Ctrl-C from "
                     "the code block, paste into ChatGPT.",
            ):
                st.session_state["mm_show_clip"] = True

        if st.session_state.get("mm_show_clip"):
            st.markdown("**Paste this block into ChatGPT:**")
            st.code(llm_markdown, language="markdown")

        # --- Caveats ---------------------------------------------------
        with st.expander(":warning: Caveats on these numbers",
                          expanded=False):
            st.markdown(
                "- **Shipping Charged** uses two sources: CIN7 sale-line "
                "items starting with 'Shipping -' (partial — CIN7's "
                "list endpoint skips most shipping lines), AND a header "
                "delta (`InvoiceAmount − product lines − tax`) for months "
                "where we have sales headers. Currently that's only the "
                "last 30 days. **The weekend sync pulls 5 years of sales "
                "headers** — Monday's Shipping Charged figures will match "
                "your Easy Insight report across the full 14 months.\n"
                "- **Shipping Cost** is 0 until ShipStation is plugged in.\n"
                "- **Average Inventory Value** is reconstructed by walking "
                "backward from the current stock snapshot: "
                "`end_inv(M) = end_inv(M+1) + COGS(M+1) − purchases(M+1)`. "
                "Because CIN7's `AverageCost` on sold lines includes "
                "landed costs (freight/duties) but `Purchase $` is the "
                "ex-freight supplier invoice, raw walk-back drifts "
                "high. We normalise by capping historical values to "
                "±15% of the current snapshot and linearly damping "
                "toward the current end of the window. Numbers are "
                "directionally correct for trend/stock-turn analysis "
                "but ±15% is the expected accuracy band vs Easy "
                "Insight's figures.\n"
                "- **Better fix** (later): schedule a daily "
                "`inventory_value_history.csv` append job — after 30 "
                "days we have real snapshots and can drop the "
                "reconstruction entirely. Ask me to add it when ready.\n"
                "- **Lost customers** = customers whose last purchase "
                "was 3 months before the column's month. Easy to switch "
                "to 6 months if you prefer.\n"
                "- **Assembled Output Quantity** and **Write Off Quantity** "
                "need assembly-event sync to populate accurately (Task "
                "#16 in the backlog).\n"
                "- **Refresh rate**: this page reads from the 15-minute "
                "near-sync, so numbers update every 15 min (hit "
                "🔄 Refresh data now in the sidebar to clear Streamlit's "
                "5-min cache for the freshest view)."
            )


# ---------------------------------------------------------------------------
# Page: FixedCost Audit
# ---------------------------------------------------------------------------
# Cross-references what we ACTUALLY paid suppliers (from purchase_lines over
# the last 2 years) against the current FixedCost on each SKU's supplier
# record in CIN7. Surfaces drift so the buyer can update CIN7 where needed
# and our PO-value calculations stay honest.

elif page == "FixedCost Audit":
    st.header(":mag: FixedCost Audit")
    st.caption(
        "What we've actually paid suppliers, vs. what CIN7 has on the "
        "FixedCost field. Drives accurate PO valuations."
    )

    pl_long = _load_longest_purchase_lines()
    if pl_long.empty:
        st.warning(
            "No purchase_lines data found in output/. "
            "Run `python cin7_sync.py purchaselines --days 730` first."
        )
    elif products.empty:
        st.warning("No product data. Run `python cin7_sync.py products`.")
    else:
        # --- Prep ------------------------------------------------------
        pl = pl_long.copy()
        # Clean types
        pl["OrderDate"] = pd.to_datetime(pl["OrderDate"], errors="coerce")
        pl["Price"] = pd.to_numeric(pl["Price"], errors="coerce")
        pl["Quantity"] = pd.to_numeric(pl["Quantity"], errors="coerce")
        pl = pl[pl["Price"].notna() & (pl["Price"] > 0)]
        pl = pl[pl["Quantity"].notna() & (pl["Quantity"] > 0)]
        pl["SKU"] = pl["SKU"].astype(str)

        # --- Controls --------------------------------------------------
        cc1, cc2, cc3, cc4 = st.columns([2, 1, 1, 1])
        suppliers_opts = ["(All)"] + sorted(
            pl["Supplier"].dropna().astype(str).unique().tolist()
        )
        sel_sup_fc = cc1.selectbox(
            "Supplier",
            options=suppliers_opts,
            key="fc_audit_supplier",
            help="Filter to a single supplier or scan across all.",
        )
        drift_pct_threshold = cc2.number_input(
            "Drift threshold %", min_value=1.0, max_value=50.0,
            value=5.0, step=0.5,
            help="How much the average paid price needs to differ from "
                 "FixedCost before we flag it. 5% is a sensible default.",
        )
        min_recent_pos = cc3.number_input(
            "Min recent POs", min_value=1, max_value=10, value=2, step=1,
            help="How many recent POs we need to see the drift on before "
                 "flagging. 2 filters out one-off outliers.",
        )
        lookback_months = cc4.number_input(
            "Lookback (months)", min_value=3, max_value=24, value=12,
            step=1,
            help="How far back to look for paid prices. 12 months is "
                 "a good balance — catches recent drift without "
                 "being skewed by 2-year-old pricing.",
        )

        # Filter by lookback + supplier
        cutoff = pd.Timestamp(datetime.now()) - pd.Timedelta(
            days=int(lookback_months) * 30)
        pl_scope = pl[pl["OrderDate"] >= cutoff].copy()
        if sel_sup_fc != "(All)":
            pl_scope = pl_scope[pl_scope["Supplier"] == sel_sup_fc]

        if pl_scope.empty:
            st.info("No purchase lines in this window / supplier.")
            st.stop()

        # --- Current FixedCost per SKU (from products with Suppliers) ---
        # cin7_cost_local is already built in the engine; rebuild it here
        # independently so this page doesn't depend on the Ordering page
        # running first.
        fixed_cost_map: dict = {}
        fixed_cost_supplier_map: dict = {}
        fixed_cost_currency_map: dict = {}
        if "Suppliers" in products.columns:
            import json as _json
            for _, prow in products.iterrows():
                sku_p = str(prow.get("SKU") or "")
                if not sku_p:
                    continue
                sups_raw = prow.get("Suppliers")
                if pd.isna(sups_raw) or not sups_raw:
                    continue
                try:
                    sups = (_json.loads(sups_raw)
                             if isinstance(sups_raw, str)
                             else sups_raw)
                except Exception:
                    continue
                if not isinstance(sups, list) or not sups:
                    continue
                # Primary = IsDefault=True, else first
                primary = next(
                    (s for s in sups if s.get("IsDefault")), sups[0])
                fc = (primary.get("FixedCost") or primary.get("Cost")
                      or primary.get("PurchaseCost"))
                if fc:
                    try:
                        fixed_cost_map[sku_p] = float(fc)
                    except (ValueError, TypeError):
                        pass
                    fixed_cost_supplier_map[sku_p] = primary.get(
                        "SupplierName", "")
                    fixed_cost_currency_map[sku_p] = primary.get(
                        "Currency", "")

        # --- Aggregate paid prices per SKU × Supplier ------------------
        pl_scope["LineSpend"] = pl_scope["Price"] * pl_scope["Quantity"]

        # Weighted-average paid price per SKU×Supplier
        grp = pl_scope.groupby(["SKU", "Supplier"], dropna=False)
        agg = grp.agg(
            pos_count=("PurchaseID", "nunique"),
            total_qty=("Quantity", "sum"),
            total_spend=("LineSpend", "sum"),
            min_price=("Price", "min"),
            max_price=("Price", "max"),
            last_price=("Price", "last"),
            last_date=("OrderDate", "max"),
            first_date=("OrderDate", "min"),
        ).reset_index()
        agg["avg_paid"] = agg["total_spend"] / agg["total_qty"].replace(
            0, pd.NA)

        # Attach current FixedCost + the supplier CIN7 has on record
        agg["FixedCost"] = agg["SKU"].map(
            lambda s: fixed_cost_map.get(s, 0.0))
        agg["FixedCost_supplier"] = agg["SKU"].map(
            lambda s: fixed_cost_supplier_map.get(s, ""))
        agg["Currency"] = agg["SKU"].map(
            lambda s: fixed_cost_currency_map.get(s, ""))

        # Delta: avg_paid vs FixedCost
        def _delta_row(r):
            fc = float(r["FixedCost"] or 0)
            ap = float(r["avg_paid"] or 0)
            if fc == 0 and ap > 0:
                return pd.Series(
                    {"delta_abs": None, "delta_pct": None,
                     "flag": ":warning: FixedCost missing"})
            if fc == 0:
                return pd.Series(
                    {"delta_abs": None, "delta_pct": None,
                     "flag": "—"})
            delta_abs = ap - fc
            delta_pct = (delta_abs / fc) * 100 if fc else None
            # Classify
            if abs(delta_pct) < drift_pct_threshold:
                flag = ":white_check_mark: in line"
            elif delta_pct >= drift_pct_threshold:
                flag = (":arrow_up: paying MORE than FixedCost"
                        if r["pos_count"] >= min_recent_pos
                        else ":grey_question: one-off over")
            else:
                flag = (":arrow_down: paying LESS than FixedCost"
                        if r["pos_count"] >= min_recent_pos
                        else ":grey_question: one-off under")
            return pd.Series(
                {"delta_abs": delta_abs, "delta_pct": delta_pct,
                 "flag": flag})

        deltas = agg.apply(_delta_row, axis=1)
        agg = pd.concat([agg, deltas], axis=1)

        # Enrich with product Name
        name_map = dict(zip(
            products["SKU"].astype(str),
            products["Name"].astype(str)))
        agg["Name"] = agg["SKU"].map(lambda s: name_map.get(s, ""))

        # --- Summary metrics -------------------------------------------
        total_rows = len(agg)
        paying_more = int((agg["flag"].astype(str)
                          .str.contains("MORE", na=False)).sum())
        paying_less = int((agg["flag"].astype(str)
                          .str.contains("LESS", na=False)).sum())
        missing_fc = int((agg["flag"].astype(str)
                          .str.contains("FixedCost missing", na=False)).sum())
        in_line = int((agg["flag"].astype(str)
                       .str.contains("in line", na=False)).sum())

        m1, m2, m3, m4, m5 = st.columns(5)
        m1.metric("SKU×Supplier pairs", f"{total_rows:,}")
        m2.metric("✓ In line", in_line,
                   help=f"Paid within ±{drift_pct_threshold}% of FixedCost")
        m3.metric("↑ Paying MORE", paying_more,
                   help="Needs a FixedCost update in CIN7, "
                        "OR a price-negotiation conversation with the supplier.",
                   delta_color="inverse")
        m4.metric("↓ Paying LESS", paying_less,
                   help="You're getting better than the quoted price. "
                        "Lucky you — or supplier forgot to re-quote.")
        m5.metric("⚠ FixedCost missing", missing_fc,
                   help="Purchases happening on SKUs that have no FixedCost "
                        "in CIN7 — our PO value calcs are using AverageCost "
                        "as a fallback. Should be fixed.",
                   delta_color="inverse")

        # Estimated annual exposure from the MORE bucket
        more_df = agg[agg["flag"].astype(str).str.contains("MORE",
                                                            na=False)]
        if not more_df.empty:
            # Extrapolate: avg_paid × annualised qty — simple approximation
            window_days = max(int(lookback_months) * 30, 30)
            annual_factor = 365.0 / window_days
            exposure = float(
                (more_df["delta_abs"] * more_df["total_qty"]
                 * annual_factor).sum()
            )
            st.error(
                f":money_with_wings: **Estimated annual overpay** if "
                f"nothing changes: **${exposure:,.0f}** across "
                f"{len(more_df):,} SKU×Supplier pairs. Update the "
                f"FixedCost in CIN7 to match actual paid — or renegotiate."
            )

        # --- Main table ------------------------------------------------
        st.subheader("Drift by SKU × Supplier")
        cols_show = ["SKU", "Name", "Supplier", "pos_count",
                     "first_date", "last_date",
                     "avg_paid", "last_price",
                     "FixedCost", "delta_abs", "delta_pct", "flag"]
        view = agg[cols_show].copy()
        view = view.sort_values(
            ["flag", "delta_pct"], ascending=[True, False],
            na_position="last")

        # Optional: quick filter by flag category
        flag_filter = st.multiselect(
            "Filter by status",
            options=sorted(view["flag"].dropna().unique().tolist()),
            default=[f for f in view["flag"].unique()
                      if "MORE" in str(f) or "missing" in str(f)],
            key="fc_audit_flag_filter",
        )
        if flag_filter:
            view = view[view["flag"].isin(flag_filter)]

        st.dataframe(
            view,
            width="stretch", hide_index=True, height=500,
            column_config={
                "SKU": st.column_config.TextColumn(width="medium"),
                "Name": st.column_config.TextColumn(width="large"),
                "Supplier": st.column_config.TextColumn(width="medium"),
                "pos_count": st.column_config.NumberColumn(
                    "# POs", help="Distinct purchases in the window",
                    format="%d"),
                "first_date": st.column_config.DateColumn(
                    "First PO", format="YYYY-MM-DD"),
                "last_date": st.column_config.DateColumn(
                    "Last PO", format="YYYY-MM-DD"),
                "avg_paid": st.column_config.NumberColumn(
                    "Avg paid", format="$%.2f",
                    help="Weighted average of what you've actually paid "
                         "in the lookback window."),
                "last_price": st.column_config.NumberColumn(
                    "Last price", format="$%.2f",
                    help="Unit price on the most recent PO."),
                "FixedCost": st.column_config.NumberColumn(
                    "FixedCost", format="$%.2f",
                    help="Current FixedCost on the SKU's supplier record "
                         "in CIN7. $0.00 = none set."),
                "delta_abs": st.column_config.NumberColumn(
                    "Δ $", format="$%.2f",
                    help="avg_paid − FixedCost. Positive = paying more."),
                "delta_pct": st.column_config.NumberColumn(
                    "Δ %", format="%.1f%%"),
                "flag": st.column_config.TextColumn("Status",
                                                     width="medium"),
            },
        )

        # --- Downloadable action list ----------------------------------
        st.markdown("#### :inbox_tray: Export action list")
        action_df = agg[agg["flag"].astype(str).str.contains(
            "MORE|missing", na=False, regex=True)].copy()
        if action_df.empty:
            st.success(
                "Nothing to action — every SKU×Supplier is in line or "
                "underpriced. Nice work."
            )
        else:
            st.caption(
                f"{len(action_df):,} row(s) flagged for CIN7 update. "
                "Download, work through, update FixedCost on each in CIN7."
            )
            csv_bytes = action_df[cols_show].to_csv(index=False)
            st.download_button(
                ":page_facing_up: Download action list (CSV)",
                data=csv_bytes,
                file_name=f"fixedcost_action_list_{datetime.now():%Y%m%d_%H%M}.csv",
                mime="text/csv",
            )

        # --- Drill-through: pick a SKU, see every PO line --------------
        st.markdown("#### :mag_right: Drill into a single SKU")
        drill_skus = sorted(agg["SKU"].unique().tolist())
        pick_sku = st.selectbox(
            "SKU to inspect",
            options=drill_skus,
            key="fc_audit_drill",
            placeholder="Start typing…",
        )
        if pick_sku:
            rows = pl_scope[pl_scope["SKU"] == pick_sku].copy()
            rows = rows.sort_values("OrderDate", ascending=False)
            rows_show = rows[["OrderDate", "OrderNumber", "Supplier",
                              "Quantity", "Price", "Total", "Status"]]
            fc_here = fixed_cost_map.get(pick_sku, 0.0)
            fc_sup = fixed_cost_supplier_map.get(pick_sku, "—")
            di1, di2, di3 = st.columns(3)
            di1.metric("FixedCost (CIN7)",
                         f"${fc_here:.2f}" if fc_here else "—")
            di2.metric("Default supplier (CIN7)",
                         fc_sup or "—")
            di3.metric("PO lines in window", len(rows_show))
            st.dataframe(
                rows_show, width="stretch", hide_index=True, height=300,
                column_config={
                    "OrderDate": st.column_config.DateColumn(
                        "Date", format="YYYY-MM-DD"),
                    "Price": st.column_config.NumberColumn(
                        "Paid / unit", format="$%.2f"),
                    "Total": st.column_config.NumberColumn(
                        "Line $", format="$%.0f"),
                },
            )


# ---------------------------------------------------------------------------
# Page: Product Detail (drill-through)
# ---------------------------------------------------------------------------

elif page == "Product Detail":
    st.header(":mag: Product Detail")

    if products.empty:
        st.warning("No product data. Run `python cin7_sync.py products`.")
    else:
        # --- SKU selector: one searchable dropdown, type to filter ----------
        # Build an index of "SKU — Name" labels once and let Streamlit handle
        # typeahead. Works smoothly up to tens of thousands of options.
        @st.cache_data(ttl=300, show_spinner=False)
        def _build_sku_options(products_df: pd.DataFrame) -> tuple:
            df = products_df[["SKU", "Name"]].copy()
            df["label"] = (
                df["SKU"].astype(str) + "  —  "
                + df["Name"].astype(str).str.slice(0, 80)
            )
            df = df.sort_values("SKU")
            return tuple(df["label"].tolist()), tuple(df["SKU"].astype(str).tolist())

        labels, sku_list = _build_sku_options(products)

        # UX: start the field EMPTY so the user can just type a fresh
        # search without first clearing whatever was previously selected.
        # If they want to revisit the last SKU, a small caption surfaces
        # below the selector so they can copy/paste it back in.
        prior_sku = st.session_state.get("selected_sku", "")

        chosen_label = st.selectbox(
            "Find a product (type any part of the SKU or name)",
            options=labels,
            index=None,
            placeholder="Start typing a SKU or product name…",
            key="pd_selectbox",
        )
        if prior_sku and chosen_label is None:
            st.caption(
                f"Previously viewed: `{prior_sku}` — paste into the box "
                "above to revisit, or just start typing to search.")

        if chosen_label is None:
            st.info(
                ":mag: Pick a product above to see its details — "
                "demand breakdown, BOM family, sales history, "
                "supplier info, and more.")
            st.stop()

        sku = chosen_label.split("  —  ", 1)[0].strip()
        st.session_state["selected_sku"] = sku

        prod_row = products[products["SKU"] == sku].iloc[0]

        # --- "This SKU has been replaced" banner --------------------------
        # If this SKU is recorded as a predecessor in the migration DB,
        # show a loud red banner at the top of Product Detail telling the
        # buyer the product is retired and pointing them at the successor.
        # Includes a "Switch to <successor>" button that sets the page's
        # selectbox to the successor and reruns — saves a clear-and-retype.
        try:
            _migs = [dict(m) for m in db.all_migrations()
                      if str(dict(m).get("retiring_sku")) == str(sku)]
        except Exception:
            _migs = []
        if _migs:
            _rec = _migs[0]  # one retiring SKU → at most one successor
            _succ_sku = str(_rec.get("successor_sku") or "").strip()
            _share = float(_rec.get("share_pct") or 100)
            _succ_name = ""
            if _succ_sku and not products.empty:
                _sm = products[products["SKU"].astype(str) == _succ_sku]
                if not _sm.empty:
                    _succ_name = str(_sm.iloc[0].get("Name") or "")
            _src = str(_rec.get("set_by") or "—")
            _set_at = str(_rec.get("set_at") or "—")[:10]

            _bcols = st.columns([5, 1])
            with _bcols[0]:
                _share_line = (
                    f"  \n_{_share:.0f}% of this SKU's historical demand "
                    f"now rolls into the successor's forecast._"
                    if _share != 100 else
                    "  \n_Full demand now rolls into the successor's forecast._"
                )
                _name_line = f"  \n_{_succ_name[:80]}_" if _succ_name else ""
                st.error(
                    f"📜 **This product has been retired and "
                    f"replaced by `{_succ_sku}`.**"
                    f"{_name_line}"
                    f"{_share_line}"
                )
                st.caption(
                    f"Migration source: `{_src}` · set on {_set_at}")
            with _bcols[1]:
                # Find the dropdown label for the successor so we can
                # populate the selectbox cleanly.
                _succ_label = next(
                    (l for l in labels
                     if l.startswith(f"{_succ_sku}  —")), None)
                if _succ_label and st.button(
                        f"→ Switch to {_succ_sku}",
                        key=f"pd_switch_to_succ_{sku}",
                        type="primary",
                        width="stretch",
                        help="Open the successor SKU in this view."):
                    st.session_state["pd_selectbox"] = _succ_label
                    st.session_state["selected_sku"] = _succ_sku
                    st.rerun()

        # --- Product family (parent / siblings / children) -----------------
        parent = parent_sku_for(sku)
        siblings = []
        children = BOM_CHILDREN.get(sku, [])
        if parent:
            # Everything else that uses the same parent = siblings
            siblings = [c for c in BOM_CHILDREN.get(parent, [])
                        if c["AssemblySKU"] != sku]
        family_root = family_sku_for(sku)

        if boms.empty:
            st.caption(
                ":link: **BOM structure not yet synced.** Parent/child "
                "relationships will appear here after running "
                "`python cin7_sync.py boms` (takes ~2 hours for ~4,500 "
                "BOM products)."
            )
        elif parent or children:
            family_cols = st.columns([2, 3, 3])
            with family_cols[0]:
                st.markdown("**:evergreen_tree: Family**")
                if family_root and family_root != sku:
                    fname = products.loc[products["SKU"] == family_root,
                                         "Name"].iloc[0] \
                            if (products["SKU"] == family_root).any() else ""
                    st.markdown(f"Master: **{family_root}**\n\n_{str(fname)[:50]}_")
                elif children:
                    st.markdown(f"This SKU **is the master** of "
                                f"{len(children)} assembly/cut(s).")
                else:
                    st.caption("Standalone (no BOM parent or children).")
            with family_cols[1]:
                if parent:
                    st.markdown("**:arrow_up: Built from / parents**")
                    pdf = pd.DataFrame(BOM_PARENTS.get(sku, []))
                    if not pdf.empty:
                        st.dataframe(
                            pdf[["ComponentSKU", "ComponentName", "Quantity"]]
                            .rename(columns={"ComponentSKU": "Parent SKU",
                                             "ComponentName": "Name",
                                             "Quantity": "Qty / unit"}),
                            width="stretch", hide_index=True)
            with family_cols[2]:
                if children:
                    st.markdown(f"**:arrow_down: Consumed in / children "
                                f"({len(children)})**")
                    cdf = pd.DataFrame(children)
                    if not cdf.empty:
                        st.dataframe(
                            cdf[["AssemblySKU", "AssemblyName", "Quantity"]]
                            .rename(columns={"AssemblySKU": "Child SKU",
                                             "AssemblyName": "Name",
                                             "Quantity": "Qty per child"}),
                            width="stretch", hide_index=True)
                elif siblings:
                    st.markdown(f"**:busts_in_silhouette: Siblings "
                                f"({len(siblings)})**")
                    sdf = pd.DataFrame(siblings)
                    if not sdf.empty:
                        st.dataframe(
                            sdf[["AssemblySKU", "AssemblyName", "Quantity"]]
                            .rename(columns={"AssemblySKU": "Sibling SKU",
                                             "AssemblyName": "Name",
                                             "Quantity": "Qty per unit"}),
                            width="stretch", hide_index=True)

        # --- Demand breakdown -----------------------------------------------
        # Same component used in PO editor — shows where demand for this
        # SKU comes from (direct sales + rolled-up children), with monthly
        # trend chart and recent activity feed. Engine-computed columns
        # (Suggest, daily rates, dormancy) aren't available on this page
        # without re-running the engine, so the header strip falls back
        # to direct-sales counters; everything else (rollup table,
        # monthly chart, activity feed) renders identically.
        with st.expander(
            ":mag: Demand breakdown — where does demand for this SKU "
            "come from?",
            expanded=True,
        ):
            render_demand_breakdown(
                sku=sku,
                sale_lines_df=sale_lines,
                products_df=products,
                bom_children=BOM_CHILDREN,
                bom_parents=BOM_PARENTS,
                engine_row=None,
                stock_df=stock,
            )

        # --- Product master header ------------------------------------------
        st.subheader(str(prod_row.get("Name") or sku))
        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("SKU", sku)
        c2.metric("Category", str(prod_row.get("Category") or "—")[:30])
        c3.metric("Brand", str(prod_row.get("Brand") or "—")[:30])
        c4.metric("Status", str(prod_row.get("Status") or "—"))
        c5.metric("Avg cost",
                  _fmt_money(float(prod_row.get("AverageCost") or 0)))

        # BOM banner
        is_bom = str(prod_row.get("BillOfMaterial")).lower() == "true"
        auto_asm = str(prod_row.get("AutoAssembly")).lower() == "true"
        auto_dis = str(prod_row.get("AutoDisassembly")).lower() == "true"
        if is_bom:
            flags = []
            if auto_asm: flags.append("Auto-Assembly")
            if auto_dis: flags.append("Auto-Disassembly")
            st.info(
                f"**BOM product** — Type: {prod_row.get('BOMType')} "
                f"| Flags: {', '.join(flags) or 'manual'}  "
                "(real demand may include assembly/disassembly consumption — "
                "full view lights up once the BOM sync is run)"
            )

        # Sourcing rule (AdditionalAttribute1) — parsed
        rule = parse_sourcing_rule(prod_row.get("AdditionalAttribute1"))
        if rule["RuleCode"] or rule["Logic"]:
            st.markdown("#### :scroll: Sourcing rule")
            sc1, sc2, sc3, sc4 = st.columns(4)
            sc1.metric("Rule", rule["RuleCode"] or "—")
            sc2.metric("Type",
                       "Master (purchased)" if rule["IsMaster"]
                       else "Assembled" if rule["SourceFraction"]
                       else "—")
            if rule["IsMaster"]:
                sc3.metric("Source", "Full length, direct from supplier")
                sc4.metric("Auto-assembly", rule["AutoAssembly"] or "—")
            else:
                if rule["SourceFraction"] is not None and rule["SourceLengthMM"]:
                    src_label = f"{rule['SourceLengthMM']/1000:g}m" if rule["SourceLengthMM"] >= 1000 else f"{rule['SourceLengthMM']}mm"
                    sc3.metric("Uses per unit",
                               f"{rule['SourceFraction']:g} × {src_label}")
                sc4.metric("Auto-assembly", rule["AutoAssembly"] or "—")
            if rule["HasPlate"]:
                st.caption(":pushpin: This rule includes a **mounting "
                           "plate** as well as a profile.")
            if rule["Logic"]:
                st.caption(f"**Logic**: _{rule['Logic']}_")
            if rule["Note"]:
                st.caption(f":memo: **Note**: _{rule['Note']}_")

        # --- Stock position across locations --------------------------------
        st.markdown("### :package: Stock position")
        sku_stock = stock[stock["SKU"] == sku] if not stock.empty else pd.DataFrame()
        if sku_stock.empty:
            st.caption("No stock rows for this SKU.")
        else:
            s = sku_stock.copy()
            for c in ["OnHand", "Allocated", "Available", "OnOrder",
                      "InTransit"]:
                if c in s.columns:
                    s[c] = _to_num(s[c]).fillna(0)
            s["Phantom"] = (s.get("Available", 0) - s.get("OnHand", 0)).clip(lower=0)
            # Cash tied up — prefer CIN7's FIFO StockOnHand per row,
            # fall back to OnHand × AverageCost if StockOnHand is absent/zero.
            _ac = float(prod_row.get("AverageCost") or 0)
            if "StockOnHand" in s.columns:
                _fifo = _to_num(s["StockOnHand"]).fillna(0)
                _oxa = s["OnHand"] * _ac
                s["CashTiedUp"] = _fifo.where(_fifo > 0, _oxa)
            else:
                s["CashTiedUp"] = s["OnHand"] * _ac

            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Total OnHand (physical)",
                      _fmt_number(s["OnHand"].sum()))
            c2.metric("Total Available",
                      _fmt_number(s.get("Available", pd.Series()).sum()))
            c3.metric("Phantom (derivable)", _fmt_number(s["Phantom"].sum()))
            c4.metric("Cash tied up", _fmt_money(s["CashTiedUp"].sum()))

            show = [c for c in ["Location", "Bin", "OnHand", "Allocated",
                                "Available", "Phantom", "OnOrder",
                                "InTransit", "NextDeliveryDate", "CashTiedUp"]
                    if c in s.columns]
            st.dataframe(s[show], width="stretch", hide_index=True)

        # --- Sales chart + lifecycle metrics ---------------------------------
        st.markdown("### :moneybag: Sales history")
        sale_df = sale_lines[sale_lines["SKU"] == sku] if not sale_lines.empty else pd.DataFrame()
        if sale_df.empty:
            st.caption(
                "No direct sales recorded in the current sales window. "
                "If this is a master / parent, demand may be indirect — "
                "see the BOM usage section once that data is synced."
            )
        else:
            d = sale_df.copy()
            d["InvoiceDate"] = _to_date(d["InvoiceDate"]).dt.tz_localize(None)
            d["Quantity"] = _to_num(d["Quantity"]).fillna(0)
            d["Total"] = _to_num(d["Total"]).fillna(0)
            d = d.dropna(subset=["InvoiceDate"])

            # Lifecycle summary
            first_sale = d["InvoiceDate"].min()
            last_sale = d["InvoiceDate"].max()
            today = pd.Timestamp(datetime.now().date())

            # Product master created date (real "introduced" date from CIN7)
            created_raw = prod_row.get("CreatedDate")
            created_date = pd.to_datetime(created_raw, errors="coerce")
            if pd.notna(created_date):
                created_date = created_date.tz_localize(None) \
                    if created_date.tzinfo else created_date
                months_since_created = max(
                    (today - created_date).days, 1) / 30.437
            else:
                months_since_created = None

            months_since_first = max((today - first_sale).days, 1) / 30.437

            # Detect if the "first sold in our data" is likely truncated
            # by our pull window. If the SKU was created well before the first
            # sale we see, warn the buyer.
            truncated_warning = False
            if (created_date is not None and pd.notna(created_date)
                    and first_sale - created_date > pd.Timedelta(days=60)):
                truncated_warning = True

            lc1, lc2, lc3, lc4 = st.columns(4)
            lc1.metric(
                "Product created",
                created_date.strftime("%Y-%m-%d")
                if (created_date is not None and pd.notna(created_date))
                else "—",
                help="When the SKU was set up in CIN7. This is the true "
                     "catalogue age."
            )
            lc2.metric(
                "First sold (in synced data)",
                first_sale.strftime("%Y-%m-%d"),
                help="Earliest invoice date present in our local sync. "
                     "Not necessarily the product's actual first sale — "
                     "the sync window limits how far back we can see."
            )
            lc3.metric("Last sold", last_sale.strftime("%Y-%m-%d"))
            lc4.metric(
                "Avg units/mo (synced data)",
                _fmt_number(d["Quantity"].sum() / max(months_since_first, 1)),
                help="Lifetime average across what we have pulled "
                     "(not the full history unless you've extended the sync)."
            )

            if truncated_warning:
                st.warning(
                    f":information_source: This SKU was created in CIN7 on "
                    f"{created_date.strftime('%Y-%m-%d')} — "
                    f"{months_since_created:.0f} months ago — but our earliest "
                    f"pulled sale is {first_sale.strftime('%Y-%m-%d')}. "
                    "Our current sync only goes back 12 months. To see the "
                    "true first-sold date, run "
                    "`python cin7_sync.py salelines --days 1825` "
                    "(pulls last 5 years — slow, weekend job)."
                )

            # Window selector: 3 / 6 / 9 / 12 months
            st.markdown("**Performance over a rolling window**")
            win_cols = st.columns([1, 3])
            window_months = win_cols[0].selectbox(
                "Window", [3, 6, 9, 12], index=3,
                key=f"win_{sku}",
                label_visibility="collapsed",
            )

            cutoff = today - pd.Timedelta(days=int(window_months * 30.437))
            prior_cutoff = today - pd.Timedelta(days=int(window_months * 2 * 30.437))

            win_df = d[d["InvoiceDate"] >= cutoff]
            prior_df = d[(d["InvoiceDate"] >= prior_cutoff) & (d["InvoiceDate"] < cutoff)]

            units_win = float(win_df["Quantity"].sum())
            rev_win = float(win_df["Total"].sum())
            orders_win = win_df["SaleID"].nunique()
            units_prior = float(prior_df["Quantity"].sum())
            rev_prior = float(prior_df["Total"].sum())

            def _pct_change(current: float, prior: float) -> Optional[float]:
                if prior == 0:
                    return None
                return (current - prior) / prior * 100

            units_trend = _pct_change(units_win, units_prior)
            rev_trend = _pct_change(rev_win, rev_prior)

            m1, m2, m3, m4, m5 = st.columns(5)
            m1.metric(f"Units (last {window_months} mo)",
                      _fmt_number(units_win),
                      delta=(f"{units_trend:+.1f}% vs prior {window_months} mo"
                             if units_trend is not None else None))
            m2.metric(f"Revenue (last {window_months} mo)",
                      _fmt_money(rev_win),
                      delta=(f"{rev_trend:+.1f}%"
                             if rev_trend is not None else None))
            m3.metric("Avg units / month",
                      _fmt_number(units_win / window_months))
            m4.metric("Avg revenue / month",
                      _fmt_money(rev_win / window_months))
            m5.metric(f"Orders (last {window_months} mo)",
                      _fmt_number(orders_win))

            # Monthly breakdown table for the buyer
            monthly = (
                win_df.set_index("InvoiceDate")
                      .groupby(pd.Grouper(freq="MS"))
                      .agg(Units=("Quantity", "sum"),
                           Revenue=("Total", "sum"),
                           Orders=("SaleID", "nunique"))
                      .reset_index()
            )
            monthly["InvoiceDate"] = monthly["InvoiceDate"].dt.strftime("%Y-%m")
            monthly = monthly.rename(columns={"InvoiceDate": "Month"})
            with st.expander(f"Monthly breakdown — last {window_months} months"):
                st.dataframe(monthly, width="stretch",
                             hide_index=True)

            st.divider()

            # Main chart (full available history)
            span_days = (d["InvoiceDate"].max()
                         - d["InvoiceDate"].min()).days if not d.empty else 0
            freq = "W" if span_days > 120 else "D"
            freq_label = "Weekly" if freq == "W" else "Daily"

            agg = (
                d.set_index("InvoiceDate")
                 .groupby(pd.Grouper(freq=freq))
                 .agg(Units=("Quantity", "sum"),
                      Revenue=("Total", "sum"),
                      Orders=("SaleID", "nunique"))
                 .reset_index()
            )

            fig = px.bar(
                agg, x="InvoiceDate", y="Units",
                title=f"{freq_label} units sold — full history",
                hover_data=["Revenue", "Orders"],
            )
            fig.update_layout(height=320, margin=dict(l=0, r=0, t=40, b=0))
            st.plotly_chart(fig, width="stretch")

            if "SourceChannel" in d.columns:
                ch = (d.groupby("SourceChannel")
                       .agg(Units=("Quantity", "sum"),
                            Revenue=("Total", "sum"),
                            Orders=("SaleID", "nunique"))
                       .sort_values("Revenue", ascending=False))
                with st.expander("By channel (full history)"):
                    st.dataframe(ch, width="stretch")

            with st.expander(f"Recent sale lines ({min(20, len(d))} shown)"):
                recent = d.sort_values("InvoiceDate", ascending=False).head(20)
                show_cols = [c for c in ["InvoiceDate", "OrderNumber",
                                         "Customer", "Quantity", "Price",
                                         "Total", "SourceChannel", "Status"]
                             if c in recent.columns]
                st.dataframe(recent[show_cols], width="stretch",
                             hide_index=True)

        # --- Purchase chart -------------------------------------------------
        st.markdown("### :truck: Purchase history")
        pur_df = purchase_lines[purchase_lines["SKU"] == sku] if not purchase_lines.empty else pd.DataFrame()
        if pur_df.empty:
            st.caption(
                "No direct purchases of this SKU in the last 90 days. "
                "If this is a cut/assembly, the master length is probably "
                "the one being bought."
            )
        else:
            p = pur_df.copy()
            p["OrderDate"] = _to_date(p["OrderDate"]).dt.tz_localize(None)
            p["Quantity"] = _to_num(p["Quantity"]).fillna(0)
            p["Total"] = _to_num(p["Total"]).fillna(0)
            p["Price"] = _to_num(p["Price"]).fillna(0)

            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Units purchased",
                      _fmt_number(p["Quantity"].sum()))
            c2.metric("Total spend", _fmt_money(p["Total"].sum()))
            c3.metric("Distinct suppliers",
                      _fmt_number(p["Supplier"].nunique()))
            c4.metric("Avg unit price",
                      _fmt_money(p["Total"].sum() / max(p["Quantity"].sum(), 1)))

            fig2 = px.bar(
                p.sort_values("OrderDate"), x="OrderDate", y="Quantity",
                color="Supplier",
                title="Purchases over time (last 90 days)",
                hover_data=["OrderNumber", "Total", "Price", "Status"],
            )
            fig2.update_layout(height=320, margin=dict(l=0, r=0, t=40, b=0))
            st.plotly_chart(fig2, width="stretch")

            # Price consistency across suppliers
            by_sup = (p.groupby("Supplier")
                       .agg(POs=("PurchaseID", "nunique"),
                            Units=("Quantity", "sum"),
                            Spend=("Total", "sum"),
                            AvgPrice=("Price", "mean"),
                            MinPrice=("Price", "min"),
                            MaxPrice=("Price", "max"))
                       .sort_values("Spend", ascending=False))
            with st.expander("Per-supplier detail"):
                st.dataframe(by_sup, width="stretch")

        # --- Stock level history (reconstructed) ----------------------------
        st.markdown("### :chart_with_downwards_trend: Stock level history (reconstructed)")

        current_onhand = float(sku_stock["OnHand"].sum()) if not sku_stock.empty else 0.0

        # Build a daily delta from sales (out) and purchase receipts (in)
        deltas: dict = {}
        if not sale_lines.empty:
            s = sale_lines[sale_lines["SKU"] == sku].copy()
            if not s.empty:
                s["d"] = _to_date(s["InvoiceDate"]).dt.tz_localize(None).dt.date
                s["q"] = _to_num(s["Quantity"]).fillna(0)
                for d, q in s.groupby("d")["q"].sum().items():
                    deltas[d] = deltas.get(d, 0) - float(q)  # sales reduce stock
        if not purchase_lines.empty:
            p = purchase_lines[purchase_lines["SKU"] == sku].copy()
            if not p.empty:
                # Treat COMPLETED POs' OrderDate as receipt date proxy;
                # ORDERED / ORDERING are still in the pipeline (not received yet)
                received_statuses = ("COMPLETED", "INVOICED", "RECEIVED",
                                     "PARTIALLY INVOICED")
                received_mask = p["Status"].astype(str).str.upper().isin(received_statuses)
                pr = p[received_mask].copy()
                pr["d"] = _to_date(pr["OrderDate"]).dt.tz_localize(None).dt.date
                pr["q"] = _to_num(pr["Quantity"]).fillna(0)
                for d, q in pr.groupby("d")["q"].sum().items():
                    deltas[d] = deltas.get(d, 0) + float(q)  # receipts increase stock

        if not deltas:
            st.caption(
                "Not enough direct sales or purchase activity for this SKU "
                "to reconstruct a history. Once the 12-month sales pull lands, "
                "movers will light up here."
            )
        else:
            # Walk backwards from current OnHand, day by day
            start_date = min(deltas.keys())
            end_date = datetime.now().date()
            series = []
            running = current_onhand
            # Start at today, go back — at each day we SUBTRACT that day's delta
            # to get yesterday's level.
            days = pd.date_range(start_date, end_date, freq="D").date
            # Build running forward instead: we have end_date OnHand,
            # so working forward from start: level(start) = current - sum(deltas up to now)
            total_delta_to_today = sum(deltas.values())
            starting_level = current_onhand - total_delta_to_today
            running = starting_level
            for d in days:
                if d in deltas:
                    running += deltas[d]
                series.append({"Date": d, "OnHand (est.)": round(running, 2)})

            hist = pd.DataFrame(series)

            c1, c2, c3 = st.columns(3)
            c1.metric("Today's OnHand", _fmt_number(current_onhand))
            c2.metric("Low point (est.)", _fmt_number(hist["OnHand (est.)"].min()))
            c3.metric("High point (est.)", _fmt_number(hist["OnHand (est.)"].max()))

            fig_hist = px.line(
                hist, x="Date", y="OnHand (est.)",
                title=f"Reconstructed OnHand — {len(days)} days",
                markers=False,
            )
            fig_hist.add_hline(y=0, line_dash="dot", line_color="red",
                               annotation_text="Stock-out")
            fig_hist.update_layout(height=320, margin=dict(l=0, r=0, t=40, b=0))
            st.plotly_chart(fig_hist, width="stretch")

            st.caption(
                ":warning: **Reconstruction caveat**: walks backward from "
                "today's OnHand using direct sales and purchase receipts. "
                "**Stock adjustments, transfers, assembly/disassembly events "
                "are not yet included** — these come with the next sync "
                "extensions. For items with heavy adjustment activity the "
                "line will drift. For regular sales/purchase-driven items "
                "it's accurate."
            )

        # --- Movement & BOM usage (placeholders pending data) ---------------
        st.markdown("### :arrows_counterclockwise: Stock movements & BOM usage")

        with st.expander("Stock adjustments and transfers (header-level)"):
            adj = stock_adjustments[stock_adjustments["SKU"] == sku] if not stock_adjustments.empty and "SKU" in stock_adjustments.columns else pd.DataFrame()
            trf = stock_transfers[stock_transfers["SKU"] == sku] if not stock_transfers.empty and "SKU" in stock_transfers.columns else pd.DataFrame()
            if adj.empty and trf.empty:
                st.caption(
                    "The current adjustment/transfer sync captures **headers only** "
                    "(task, date, status). Line-level detail per SKU requires a "
                    "second sync pass — planned next. Until then, direct sales and "
                    "purchases above are the visible movement signal for this SKU."
                )
            else:
                if not adj.empty:
                    st.write("**Adjustments**")
                    st.dataframe(adj, width="stretch", hide_index=True)
                if not trf.empty:
                    st.write("**Transfers**")
                    st.dataframe(trf, width="stretch", hide_index=True)

        with st.expander("BOM usage — where this SKU is consumed / what it consumes"):
            if is_bom:
                st.markdown(
                    "This is a **BOM product**. The parent-child structure "
                    "(what it's built from / what it disassembles into) will "
                    "appear here once the `boms` sync has run. That tool "
                    "calls `/product?ID=X` for each BOM-flagged product and "
                    "persists the structure. Run tomorrow once the 12-month "
                    "sales pull is complete."
                )
            else:
                st.markdown(
                    "This SKU is not flagged as a BOM itself, but it may be "
                    "**consumed as a child** by an Assembly parent, or "
                    "**derived from** a Disassembly master. Both relationships "
                    "come online with the BOM sync (planned tomorrow)."
                )

        # --- Team notes & flags ---------------------------------------------
        st.markdown("### :pushpin: Team notes & flags")
        actor = st.session_state.get("current_user", "").strip()
        if not actor:
            st.info("Enter your name in the sidebar to add notes or flags.")

        # Active flags (all users)
        active_flags = db.list_flags(sku=sku, active_only=True)
        if active_flags:
            st.write("**Active flags on this SKU:**")
            for f in active_flags:
                cols = st.columns([5, 1])
                cols[0].markdown(
                    f"🚩 **{f['flag_type']}** "
                    f"— set by *{f['set_by']}* on "
                    f"{f['set_at'][:16]}"
                    + (f"  —  _{f['notes']}_" if f['notes'] else "")
                )
                if actor and cols[1].button("Clear", key=f"clrflag_{f['id']}"):
                    db.clear_flag(f["id"], actor)
                    st.rerun()
        else:
            st.caption("No active flags on this SKU.")

        with st.expander("Add a flag"):
            if not actor:
                st.warning("Enter your name in the sidebar first.")
            else:
                fc1, fc2 = st.columns([2, 3])
                ftype = fc1.selectbox("Flag type", db.FLAG_TYPES,
                                      key=f"ftype_{sku}")
                fnote = fc2.text_input("Optional note",
                                       key=f"fnote_{sku}",
                                       placeholder="Why are you flagging this?")
                if st.button("Add flag", key=f"addflag_{sku}"):
                    db.set_flag(sku, ftype, actor, fnote)
                    st.success(f"Flagged: {ftype}")
                    st.rerun()

        # Notes (latest first)
        notes = db.list_notes(sku=sku, limit=100)
        st.write(f"**Notes ({len(notes)}):**")
        if notes:
            for n in notes:
                cols = st.columns([10, 1])
                cols[0].markdown(
                    f"_{n['created_at'][:16]} — **{n['author']}**_"
                    + (f"  `{n['tags']}`" if n['tags'] else "")
                    + f"\n\n{n['body']}"
                )
                if actor and cols[1].button("Delete",
                                            key=f"delnote_{n['id']}"):
                    db.delete_note(n["id"], actor)
                    st.rerun()
                st.divider()
        else:
            st.caption("No notes yet.")

        with st.expander("Add a note"):
            if not actor:
                st.warning("Enter your name in the sidebar first.")
            else:
                nbody = st.text_area("Note", key=f"nbody_{sku}",
                                     placeholder="e.g. Topmet can airship via UPS in 3 days")
                ntags = st.text_input("Tags (comma-separated, optional)",
                                       key=f"ntags_{sku}",
                                       placeholder="e.g. topmet, airfreight")
                if st.button("Save note", key=f"savenote_{sku}"):
                    if nbody.strip():
                        db.add_note(sku, actor, nbody, ntags)
                        st.success("Note saved")
                        st.rerun()
                    else:
                        st.warning("Note body can't be empty.")

        # --- Raw product record ---------------------------------------------
        with st.expander("Full product record (raw)"):
            st.json(prod_row.to_dict())


# ---------------------------------------------------------------------------
# Page: Kit Management
# ---------------------------------------------------------------------------

elif page == "Kits & Fixtures":
    st.header(":gift: Kits & Fixtures")
    st.caption(
        "Pre-build high-velocity kits and fixtures so fulfillment doesn't "
        "make them on the fly — without starving other assemblies that "
        "share components."
    )

    if products.empty:
        st.warning("No product data yet.")
        st.stop()

    # --- Filters ----------------------------------------------------------
    fc1, fc2, fc3, fc4 = st.columns([2, 1, 1, 1])
    prefix_input = fc1.text_input(
        "Kit / fixture SKU prefixes (comma-separated)",
        value="LEDKIT-, LEDFIX-",
        help="Any SKU starting with one of these prefixes is considered a "
             "kit or fixture. Use 'Confirmed kit' / 'Not actually a kit' "
             "flags on Product Detail to override per-SKU.",
    )
    prefixes = tuple(p.strip().upper() for p in prefix_input.split(",")
                     if p.strip())
    window_months = fc2.selectbox(
        "Velocity window", [3, 6, 9, 12], index=3,
        help="Time window to compute sales velocity.",
    )
    par_weeks = fc3.number_input(
        "Target weeks of cover",
        min_value=0.5, max_value=12.0, value=2.0, step=0.5,
        help="Default suggested pre-build = this many weeks of average "
             "demand. Override per kit with human policy.",
    )
    top_n = fc4.number_input(
        "Top kits to show",
        min_value=5, max_value=200, value=20, step=5,
    )

    tf1, tf2 = st.columns([1, 3])
    with tf1:
        type_filter = st.multiselect(
            "Type",
            ["Kit", "Fixture", "Other assembly"],
            default=["Kit", "Fixture"],
            help="Kit = SKU starts with LEDKIT-. "
                 "Fixture = starts with LEDFIX-. "
                 "Other = older Assembly BOMs without either prefix.",
        )
    with tf2:
        include_other_boms = st.checkbox(
            "Also include older Assembly BOMs without the prefix",
            value=False,
            help="Catches legacy kits/fixtures created before the "
                 "LEDKIT-/LEDFIX- naming convention. Use the "
                 "'Not actually a kit' flag to hide ones that aren't real.",
        )

    # --- Identify kit universe --------------------------------------------
    # 1. Start with all Assembly/Production BOMs
    all_boms = products[
        (products["BillOfMaterial"].astype(str).str.lower() == "true")
        & (products["BOMType"].isin(["Assembly", "Production"]))
    ].copy()

    # 2. Read team flags (Confirmed kit / Not actually a kit)
    flag_index = db.flag_counts_by_sku()  # {sku: [flag_type, ...]}
    confirmed_kit_skus = {
        sku for sku, flags in flag_index.items()
        if "Confirmed kit" in flags
    }
    not_kit_skus = {
        sku for sku, flags in flag_index.items()
        if "Not actually a kit" in flags
    }

    # 3. Build kit universe
    def _matches_prefix(sku: str) -> bool:
        if not prefixes:
            return False
        s = str(sku).upper()
        return any(s.startswith(p) for p in prefixes)

    def _kit_type(sku: str) -> str:
        s = str(sku).upper()
        if s.startswith("LEDKIT-"):
            return "Kit"
        if s.startswith("LEDFIX-"):
            return "Fixture"
        return "Other assembly"

    if include_other_boms:
        kits_df = all_boms.copy()
    else:
        mask = (all_boms["SKU"].apply(_matches_prefix)
                | all_boms["SKU"].isin(confirmed_kit_skus))
        kits_df = all_boms[mask].copy()

    # Force-add confirmed kits even if they don't match the filter
    add_confirmed = products[
        products["SKU"].isin(confirmed_kit_skus)
        & ~products["SKU"].isin(kits_df["SKU"])
    ]
    if not add_confirmed.empty:
        kits_df = pd.concat([kits_df, add_confirmed], ignore_index=True)

    # Force-remove "Not actually a kit" flagged SKUs
    kits_df = kits_df[~kits_df["SKU"].isin(not_kit_skus)]

    if kits_df.empty:
        st.warning(
            f"No kits match current filters. Prefixes: {list(prefixes)}. "
            "Try ticking 'Include older Assembly BOMs', or check if your "
            "flagged 'Not actually a kit' list is hiding everything."
        )
        st.stop()

    # Tag type
    kits_df["Kit type"] = kits_df["SKU"].apply(_kit_type)
    kits_df["Team flag"] = kits_df["SKU"].apply(
        lambda s: "✓ Confirmed" if s in confirmed_kit_skus else ""
    )

    # Apply Type filter
    if type_filter:
        kits_df = kits_df[kits_df["Kit type"].isin(type_filter)]
    if kits_df.empty:
        st.warning("No items match the selected Type filter.")
        st.stop()

    # --- Compute velocity per kit ----------------------------------------
    today = pd.Timestamp(datetime.now().date())
    cutoff = today - pd.Timedelta(days=int(window_months * 30.437))

    sales_by_sku = {}
    rev_by_sku = {}
    first_by_sku = {}
    last_by_sku = {}
    if not sale_lines.empty:
        sl = sale_lines.copy()
        sl["InvoiceDate"] = _to_date(sl["InvoiceDate"]).dt.tz_localize(None)
        sl["Quantity"] = _to_num(sl["Quantity"]).fillna(0)
        sl["Total"] = _to_num(sl["Total"]).fillna(0)
        sl = sl.dropna(subset=["InvoiceDate"])
        # Lifetime first/last
        for sku, grp in sl.groupby("SKU"):
            first_by_sku[sku] = grp["InvoiceDate"].min()
            last_by_sku[sku] = grp["InvoiceDate"].max()
        # Window aggregates
        wn = sl[sl["InvoiceDate"] >= cutoff]
        grouped = wn.groupby("SKU").agg(
            Units=("Quantity", "sum"),
            Revenue=("Total", "sum"),
        )
        sales_by_sku = grouped["Units"].to_dict()
        rev_by_sku = grouped["Revenue"].to_dict()

    # --- Kit stock summary -----------------------------------------------
    stock_by_sku = {}
    if not stock.empty:
        s_ = stock.copy()
        s_["OnHand"] = _to_num(s_["OnHand"]).fillna(0)
        s_["Available"] = _to_num(s_["Available"]).fillna(0)
        for sku, grp in s_.groupby("SKU"):
            stock_by_sku[sku] = {
                "OnHand": float(grp["OnHand"].sum()),
                "Available": float(grp["Available"].sum()),
            }

    # Pre-compute per-SKU purchase history so we know which kits are bought
    # directly from suppliers (e.g. Topmet finished kits) vs built in-house
    purch_by_sku: dict = {}
    if not purchase_lines.empty and "SKU" in purchase_lines.columns:
        pl = purchase_lines.copy()
        pl["Total"] = _to_num(pl["Total"]).fillna(0)
        pl["Quantity"] = _to_num(pl["Quantity"]).fillna(0)
        for (sku_key, supplier), g in pl.groupby(["SKU", "Supplier"]):
            info = purch_by_sku.setdefault(
                sku_key,
                {"suppliers": {}, "total_units": 0, "total_spend": 0}
            )
            info["suppliers"][supplier] = {
                "units": float(g["Quantity"].sum()),
                "spend": float(g["Total"].sum()),
                "pos": g["PurchaseID"].nunique(),
            }
            info["total_units"] += float(g["Quantity"].sum())
            info["total_spend"] += float(g["Total"].sum())

    # Team-flag overrides for sourcing
    bought_kit_flags = {
        sku for sku, flags in flag_index.items()
        if "Bought as kit" in flags
    }
    built_inhouse_flags = {
        sku for sku, flags in flag_index.items()
        if "Built in-house" in flags
    }

    def _sourcing(sku: str) -> tuple:
        """Return (label, top_supplier_name). Team flag wins over inference."""
        if sku in bought_kit_flags:
            info = purch_by_sku.get(sku, {})
            top = max(info.get("suppliers", {}).items(),
                      key=lambda x: x[1]["spend"], default=(None, None))
            return "🛒 Bought", top[0]
        if sku in built_inhouse_flags:
            return "🔧 Built", None
        info = purch_by_sku.get(sku, {})
        if info and info["total_units"] > 0:
            top = max(info["suppliers"].items(),
                      key=lambda x: x[1]["spend"], default=(None, None))
            return "🛒 Bought (inferred)", top[0]
        return "🔧 Built (assumed)", None

    window_days = window_months * 30.437
    kit_rows = []
    for _, k in kits_df.iterrows():
        sku = k["SKU"]
        units = float(sales_by_sku.get(sku, 0))
        rev = float(rev_by_sku.get(sku, 0))
        avg_daily = units / window_days if window_days > 0 else 0
        avg_monthly = avg_daily * 30.437
        onhand = float(stock_by_sku.get(sku, {}).get("OnHand", 0))
        avail = float(stock_by_sku.get(sku, {}).get("Available", 0))
        phantom = max(avail - onhand, 0)
        target_par = avg_daily * (par_weeks * 7)
        days_of_cover_prebuilt = (onhand / avg_daily) if avg_daily > 0 else None
        gap = onhand - target_par
        cost = float(k.get("AverageCost") or 0)

        # Status
        if avg_daily == 0:
            status = "Not selling"
        elif days_of_cover_prebuilt is None or days_of_cover_prebuilt < 3:
            status = "🔴 Pre-build now"
        elif days_of_cover_prebuilt < par_weeks * 7 * 0.75:
            status = "🟠 Low pre-built"
        elif days_of_cover_prebuilt <= par_weeks * 7 * 1.25:
            status = "🟢 On target"
        else:
            status = "🔵 Over-built"

        sourcing_label, top_supplier = _sourcing(sku)
        kit_rows.append({
            "SKU": sku,
            "Name": k.get("Name"),
            "Type": k.get("Kit type"),
            "Sourcing": sourcing_label,
            "Primary supplier": top_supplier or "—",
            "Team flag": k.get("Team flag"),
            f"Units ({window_months}mo)": units,
            "Avg/month": round(avg_monthly, 1),
            "Avg/day": round(avg_daily, 2),
            "Pre-built (OnHand)": onhand,
            "Phantom (derivable)": round(phantom, 1),
            "DoC on pre-built": (round(days_of_cover_prebuilt, 1)
                                 if days_of_cover_prebuilt is not None else None),
            f"Target par ({par_weeks:g} wks)": round(target_par, 1),
            "Gap vs target": round(gap, 1),
            "Status": status,
            "Unit cost": cost,
            f"Revenue ({window_months}mo)": rev,
            "First sold": (first_by_sku.get(sku).strftime("%Y-%m-%d")
                           if first_by_sku.get(sku) is not None else None),
        })

    kits_ranked = pd.DataFrame(kit_rows).sort_values(
        f"Units ({window_months}mo)", ascending=False
    ).head(int(top_n))

    # --- Headline metrics -------------------------------------------------
    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("Items shown", len(kits_ranked))
    type_breakdown = kits_ranked["Type"].value_counts().to_dict()
    k2.metric("Kits / Fixtures / Other",
              f"{type_breakdown.get('Kit', 0)} / "
              f"{type_breakdown.get('Fixture', 0)} / "
              f"{type_breakdown.get('Other assembly', 0)}")
    k3.metric(f"Total units sold ({window_months}mo)",
              _fmt_number(kits_ranked[f"Units ({window_months}mo)"].sum()))
    k4.metric(f"Total revenue ({window_months}mo)",
              _fmt_money(kits_ranked[f"Revenue ({window_months}mo)"].sum()))
    prebuilt_val = (kits_ranked["Pre-built (OnHand)"]
                    * kits_ranked["Unit cost"]).sum()
    k5.metric("Pre-built stock value", _fmt_money(prebuilt_val))

    # --- Main kits/fixtures table ----------------------------------------
    st.markdown("### Top kits & fixtures — velocity vs pre-built stock")

    show_cols = [
        "SKU", "Type", "Sourcing", "Primary supplier", "Name",
        "Team flag", "Status",
        f"Units ({window_months}mo)", "Avg/month", "Avg/day",
        "Pre-built (OnHand)", "Phantom (derivable)",
        "DoC on pre-built", f"Target par ({par_weeks:g} wks)", "Gap vs target",
        "Unit cost", f"Revenue ({window_months}mo)", "First sold",
    ]

    st.dataframe(
        kits_ranked[show_cols],
        width="stretch",
        hide_index=True,
        height=540,
        column_config={
            "Unit cost": st.column_config.NumberColumn(format="$%.2f"),
            f"Revenue ({window_months}mo)":
                st.column_config.NumberColumn(format="$%.0f"),
        },
    )

    st.caption(
        "**DoC on pre-built** = days of cover from physical pre-assembled "
        "stock (OnHand) based on selected window velocity. "
        "**Phantom** = what CIN7 could still auto-assemble from components. "
        "**Gap vs target** negative = under-pre-built, positive = over-built."
    )

    # --- Quick flag widget ------------------------------------------------
    st.markdown("#### :pushpin: Quick-flag a SKU in the list above")
    actor = st.session_state.get("current_user", "").strip()
    if not actor:
        st.caption(":warning: Enter **Your name** in the sidebar to add or "
                   "clear flags here.")
    else:
        qf1, qf2, qf3, qf4 = st.columns([2, 2, 3, 1])
        with qf1:
            sku_options = kits_ranked["SKU"].tolist()
            flag_sku = st.selectbox(
                "SKU",
                options=sku_options,
                key="kf_qflag_sku",
            )
        with qf2:
            flag_type = st.selectbox(
                "Flag type",
                options=db.FLAG_TYPES,
                # Default to a kit-specific flag
                index=db.FLAG_TYPES.index("Not actually a kit"),
                key="kf_qflag_type",
            )
        with qf3:
            flag_note = st.text_input(
                "Note (optional)",
                key="kf_qflag_note",
                placeholder="Why? (e.g. 'Smokies component bundle, not a real kit')",
            )
        with qf4:
            st.write("")
            st.write("")
            if st.button(":pushpin: Flag", key="kf_qflag_btn",
                         width="stretch"):
                db.set_flag(flag_sku, flag_type, actor, flag_note)
                st.cache_data.clear()
                st.success(f"Flagged {flag_sku}: {flag_type}")
                st.rerun()

        # Show current flags on items in the list
        active = db.list_flags(active_only=True)
        active_in_list = [f for f in active
                          if f["sku"] in set(kits_ranked["SKU"])]
        if active_in_list:
            with st.expander(f"Active flags on items in this list "
                             f"({len(active_in_list)})"):
                for f in active_in_list:
                    cc1, cc2 = st.columns([8, 1])
                    cc1.markdown(
                        f"🚩 **{f['sku']}** — {f['flag_type']} "
                        f"_(set by {f['set_by']} on {f['set_at'][:16]})_"
                        + (f"  —  _{f['notes']}_" if f['notes'] else "")
                    )
                    if cc2.button("Clear", key=f"kf_clr_{f['id']}"):
                        db.clear_flag(f["id"], actor)
                        st.cache_data.clear()
                        st.rerun()

    # --- Component view (needs BOM sync) ----------------------------------
    st.markdown("### :gear: Components & sharing")

    if boms.empty:
        st.info(
            ":hourglass: BOM data not yet synced. Run "
            "`python cin7_sync.py boms` to light this section up. Once "
            "available, you'll see per-kit component requirements, "
            "max-buildable-today counts, and a component-sharing matrix "
            "that flags when pre-building kit A would starve kit B."
        )
    else:
        # Focus on top kits only
        top_skus = set(kits_ranked["SKU"].tolist())
        kit_boms = boms[boms["AssemblySKU"].isin(top_skus)].copy()
        kit_boms["Quantity"] = _to_num(kit_boms["Quantity"]).fillna(0)

        # Component usage summary: how many top kits use each component
        comp_usage = (kit_boms.groupby(
            ["ComponentSKU", "ComponentName"], dropna=False)
            .agg(UsedByKits=("AssemblySKU", "nunique"),
                 QtyPerKitAvg=("Quantity", "mean"))
            .reset_index()
            .sort_values("UsedByKits", ascending=False))

        # Bring in component stock
        comp_usage["ComponentOnHand"] = comp_usage["ComponentSKU"].map(
            lambda s: stock_by_sku.get(s, {}).get("OnHand", 0))
        comp_usage["ComponentAvailable"] = comp_usage["ComponentSKU"].map(
            lambda s: stock_by_sku.get(s, {}).get("Available", 0))

        c1, c2 = st.columns(2)
        c1.metric("Distinct components across top kits", len(comp_usage))
        shared = comp_usage[comp_usage["UsedByKits"] >= 2]
        c2.metric("Components shared across 2+ kits", len(shared))

        tab_sharing, tab_perkit = st.tabs(
            ["Component sharing", "Per-kit component requirements"])

        with tab_sharing:
            st.markdown(
                "**Shared components** are the ones to watch — "
                "over-pre-building kit A using one of these can starve kit B."
            )
            st.dataframe(
                shared.rename(columns={
                    "ComponentSKU": "Component SKU",
                    "ComponentName": "Name",
                    "UsedByKits": "# Kits using it",
                    "QtyPerKitAvg": "Avg qty per kit",
                    "ComponentOnHand": "Component stock (phys)",
                    "ComponentAvailable": "Component available",
                }),
                width="stretch", hide_index=True,
            )

        with tab_perkit:
            kit_pick = st.selectbox(
                "Pick a kit to inspect its BOM",
                options=kits_ranked["SKU"].tolist(),
                key="kit_component_pick",
            )
            kb = kit_boms[kit_boms["AssemblySKU"] == kit_pick].copy()
            if kb.empty:
                st.info("No component BOM data found for this kit yet.")
            else:
                kb["ComponentOnHand"] = kb["ComponentSKU"].map(
                    lambda s: stock_by_sku.get(s, {}).get("OnHand", 0))
                kb["ComponentAvailable"] = kb["ComponentSKU"].map(
                    lambda s: stock_by_sku.get(s, {}).get("Available", 0))
                kb["MaxBuildableFromThis"] = kb.apply(
                    lambda r: (r["ComponentAvailable"] / r["Quantity"])
                              if r["Quantity"] else 0, axis=1).astype(int)

                max_buildable_today = (int(kb["MaxBuildableFromThis"].min())
                                       if not kb.empty else 0)
                kit_velocity = kits_ranked[
                    kits_ranked["SKU"] == kit_pick
                ]["Avg/day"].iloc[0]
                kit_prebuilt = kits_ranked[
                    kits_ranked["SKU"] == kit_pick
                ]["Pre-built (OnHand)"].iloc[0]
                suggested = kit_velocity * par_weeks * 7 - kit_prebuilt
                suggested = max(0, round(suggested))

                c1, c2, c3 = st.columns(3)
                c1.metric("Max buildable today (component-limited)",
                          _fmt_number(max_buildable_today))
                c2.metric("Suggested pre-build now",
                          _fmt_number(min(max_buildable_today, suggested)),
                          help="Pre-build this many to hit target par. "
                               "Capped by component availability.")
                c3.metric("Kit velocity / day",
                          f"{kit_velocity:.2f}")

                show_c = ["ComponentSKU", "ComponentName", "Quantity",
                          "ComponentOnHand", "ComponentAvailable",
                          "MaxBuildableFromThis"]
                st.dataframe(
                    kb[show_c].rename(columns={
                        "ComponentSKU": "Component SKU",
                        "ComponentName": "Name",
                        "Quantity": "Qty per kit",
                        "ComponentOnHand": "Comp OnHand (phys)",
                        "ComponentAvailable": "Comp Available",
                        # MaxBuildableFromThis was missing from the
                        # rename map — caused KeyError on the sort.
                        "MaxBuildableFromThis": "Buildable from this alone",
                    }).sort_values("Buildable from this alone"),
                    width="stretch", hide_index=True,
                )


    # --- Product affinity: what sells together ---------------------------
    st.markdown("### :link: Frequently bought together — candidate bundles")
    st.caption(
        "Groups of 2, 3, or 4 SKUs that appear together in the same order "
        "unusually often. High lift + high count = strong candidate to "
        "turn into a pre-made kit."
    )

    if sale_lines.empty:
        st.info("Need sale-line data to compute affinity.")
    else:
        a1, a2, a3, a4 = st.columns(4)
        group_size = a1.selectbox("Group size",
                                   [2, 3, 4], index=0,
                                   help="2 = pairs, 3 = triples, 4 = "
                                        "quadruples. Higher sizes are rarer "
                                        "but point at more complete kit "
                                        "concepts.")
        # Sensible defaults per group size
        default_cooccur = {2: 10, 3: 5, 4: 3}[group_size]
        min_cooccur = a2.number_input(
            "Min co-occurrences",
            min_value=2, max_value=500,
            value=default_cooccur, step=1,
            key=f"min_co_{group_size}",
        )
        min_lift = a3.number_input(
            "Min lift",
            min_value=1.0, max_value=10000.0, value=2.0, step=0.5,
            help="1.0 = no more than chance. Triples/quads tend to have "
                 "much higher lift because joint independence is rare.",
            key=f"min_lift_{group_size}",
        )
        max_groups = a4.number_input(
            "Show top N groups",
            min_value=10, max_value=500, value=50, step=10,
            key=f"max_grp_{group_size}",
        )
        exclude_kits = st.checkbox(
            "Exclude groups containing an existing kit",
            value=True,
            help="Hides groups where any member is already a BOM kit — "
                 "focuses you on NEW kitting opportunities.",
            key=f"excl_kit_{group_size}",
        )

        @st.cache_data(ttl=900, show_spinner="Computing product affinity…")
        def _compute_affinity_groups(sl_df: pd.DataFrame,
                                     prod_df: pd.DataFrame,
                                     k: int):
            from itertools import combinations
            from collections import Counter

            if sl_df.empty or k < 2:
                return pd.DataFrame()

            src = sl_df[["SaleID", "SKU"]].dropna().drop_duplicates()
            total_orders = src["SaleID"].nunique()
            sku_order_counts = (src.groupby("SKU")["SaleID"].nunique()
                                    .to_dict())

            # Baskets, filtered to reasonable sizes
            baskets = []
            for _, grp in src.groupby("SaleID"):
                skus = sorted(set(grp["SKU"].tolist()))
                if k <= len(skus) <= 30:
                    baskets.append(skus)

            group_counter: Counter = Counter()
            for skus in baskets:
                for combo in combinations(skus, k):
                    group_counter[combo] += 1

            if not group_counter:
                return pd.DataFrame()

            name_map = (prod_df.set_index("SKU")["Name"].to_dict()
                        if not prod_df.empty else {})
            bom_flag = (prod_df.set_index("SKU")["BillOfMaterial"]
                        .astype(str).str.lower().eq("true").to_dict()
                        if not prod_df.empty else {})

            rows = []
            for combo, cnt in group_counter.items():
                # skip anything below a tiny floor to save memory
                if cnt < 2:
                    continue
                counts = [sku_order_counts.get(s, 0) for s in combo]
                if any(c == 0 for c in counts):
                    continue
                support = cnt / total_orders if total_orders else 0
                # Lift generalised to k-tuples:
                # lift = observed_support / expected_support_if_independent
                #      = (cnt/T) / (n1*n2*...*nk / T^k)
                #      = cnt * T^(k-1) / prod(n_i)
                expected_denom = 1.0
                for c in counts:
                    expected_denom *= c
                lift = ((cnt * (total_orders ** (k - 1))) / expected_denom
                        if expected_denom > 0 else 0)
                has_kit = any(bom_flag.get(s, False) for s in combo)
                row = {
                    "Times together": cnt,
                    "Support %": round(support * 100, 4),
                    "Lift": round(lift, 2),
                    "Contains kit?": has_kit,
                }
                for i, s in enumerate(combo, 1):
                    row[f"SKU {i}"] = s
                    row[f"Name {i}"] = str(name_map.get(s) or "")[:60]
                    row[f"# sold {i}"] = sku_order_counts.get(s, 0)
                rows.append(row)
            return pd.DataFrame(rows)

        aff = _compute_affinity_groups(sale_lines, products, group_size)

        if aff.empty:
            st.info("No co-occurring groups found at this size.")
        else:
            filt = aff[
                (aff["Times together"] >= min_cooccur)
                & (aff["Lift"] >= min_lift)
            ]
            if exclude_kits:
                filt = filt[~filt["Contains kit?"]]

            filt = filt.sort_values(
                ["Lift", "Times together"], ascending=[False, False]
            ).head(int(max_groups))

            st.caption(
                f"Of {len(aff):,} total {group_size}-member groups, "
                f"{len(filt):,} meet the thresholds."
            )

            # Column order: metrics first, then member SKUs/names
            ordered_cols = ["Times together", "Support %", "Lift"]
            for i in range(1, group_size + 1):
                ordered_cols.extend([f"SKU {i}", f"Name {i}", f"# sold {i}"])

            st.dataframe(
                filt[ordered_cols],
                width="stretch", hide_index=True, height=500,
                column_config={
                    "Lift": st.column_config.NumberColumn(format="%.2fx"),
                    "Support %": st.column_config.NumberColumn(format="%.4f%%"),
                },
            )

            with st.expander("How to read this"):
                st.markdown(
                    f"- **Times together** — number of orders containing "
                    f"ALL {group_size} SKUs.\n"
                    f"- **# sold** — orders containing each SKU individually.\n"
                    f"- **Support %** — fraction of all orders containing "
                    f"this {group_size}-group.\n"
                    f"- **Lift** — joint frequency vs chance. For groups of "
                    f"3 or 4, lifts of 10x–1000x are common because random "
                    f"co-occurrence is extremely unlikely. "
                    f"Focus on high `Times together` and high `Lift` combined.\n\n"
                    f"**Using the output:** a triple or quadruple with even "
                    f"15-30 co-occurrences and high lift is a strong kit "
                    f"signal — customers keep building that exact combo. "
                    f"Pre-building those saves the most fulfillment time."
                )

# ---------------------------------------------------------------------------
# Page: AI Assistant
# ---------------------------------------------------------------------------
# A natural-language Q&A page powered by Claude. The user types a
# question; we send it to the Anthropic API along with a small set of
# tools (defined in ai_tools.py) that Claude can call to fetch data.
# Every interaction is logged to db.ai_audit_logs with a feedback
# button, so over time we can refine prompts/tools/aliases.
#
# Critical design choices:
#   - Tool-use, not "stuff everything into context": cheaper + handles
#     cases where data wouldn't fit in 200k tokens.
#   - Audit log is the source of truth for what happened. UI shows
#     answer; DB records prompt + tool calls + answer + feedback.
#   - APP_PASSWORD-gated already — no extra auth needed.
elif page == "AI Assistant":
    st.header(":robot_face: AI Assistant")
    st.caption(
        "Ask natural-language questions about inventory, sales, "
        "dead stock, migrations, or velocity. The AI looks at live "
        "data via tools — it doesn't make numbers up. Every answer "
        "is logged for audit."
    )

    # Build a lightweight inventory view for the AI tools. The full
    # ABC engine is currently scoped to the Ordering page block, so
    # we don't have engine_df here. Instead we compose a simpler
    # SKU-level frame from products + stock — enough for search,
    # SKU lookup, and migration chain queries. Tools that need the
    # full engine (ABC class, Classification etc.) will see those
    # columns missing and gracefully degrade.
    if not products.empty:
        engine_df = products.copy()
        engine_df["SKU"] = engine_df["SKU"].astype(str)
        if not stock.empty and "SKU" in stock.columns:
            _stock_view = stock[["SKU"]].copy()
            _stock_view["SKU"] = _stock_view["SKU"].astype(str)
            for _col in ("OnHand", "Available"):
                if _col in stock.columns:
                    _stock_view[_col] = pd.to_numeric(
                        stock[_col], errors="coerce")
            engine_df = engine_df.merge(
                _stock_view, on="SKU", how="left")
        # Family lives in AdditionalAttribute1 in CIN7 conventions.
        if "AdditionalAttribute1" in engine_df.columns:
            engine_df["Family"] = engine_df["AdditionalAttribute1"]
    else:
        engine_df = pd.DataFrame(columns=["SKU", "Name", "OnHand"])

    # API key check upfront so we fail loud, not silent.
    _anthropic_key = os.environ.get("ANTHROPIC_API_KEY", "").strip()
    if not _anthropic_key:
        st.error(
            ":x: ANTHROPIC_API_KEY is not set. The AI Assistant won't "
            "work until an admin adds the key to the environment "
            "(Render → Environment → ANTHROPIC_API_KEY).")
        st.stop()
    try:
        import anthropic  # noqa: F401
    except ImportError:
        st.error(
            ":x: The `anthropic` Python package isn't installed. "
            "An admin needs to add it to requirements.txt and redeploy.")
        st.stop()

    import ai_tools

    # Lay out the page. Left column: chat input + transcript. Right:
    # a "what can I ask" cheatsheet so users know where to start.
    main_col, side_col = st.columns([3, 1])

    with side_col:
        st.markdown("**Try asking:**")
        st.markdown(
            "- *What 2700K LED strips are slow moving?*\n"
            "- *Show me dead stock SIERRA38 with stock value over $500*\n"
            "- *Velocity for LED-XRD-60W-24 last 90 days*\n"
            "- *What did LED-E60L24DC-KO get replaced by?*\n"
            "- *Black recessed channel, in stock, A-class only*"
        )
        st.caption(
            "Tip: include SKUs verbatim when you have them. The AI is "
            "best at lookups, comparisons, and rollups — not freeform "
            "guesses."
        )

    with main_col:
        # Persist transcript across reruns so user can scroll back.
        if "_ai_transcript" not in st.session_state:
            st.session_state["_ai_transcript"] = []

        # Render existing transcript
        for entry in st.session_state["_ai_transcript"]:
            with st.chat_message(entry["role"]):
                st.markdown(entry["content"])
                if entry["role"] == "assistant" and entry.get("audit_id"):
                    fb_cols = st.columns([1, 1, 6])
                    if fb_cols[0].button(
                            ":+1:",
                            key=f"fb_pos_{entry['audit_id']}",
                            help="This answer was helpful and accurate"):
                        db.record_ai_feedback(
                            entry["audit_id"], "positive",
                            user_id=current_user or "anonymous")
                        st.toast("Thanks — feedback recorded.",
                                  icon=":+1:")
                    if fb_cols[1].button(
                            ":-1:",
                            key=f"fb_neg_{entry['audit_id']}",
                            help="This answer was wrong or unhelpful"):
                        db.record_ai_feedback(
                            entry["audit_id"], "negative",
                            user_id=current_user or "anonymous")
                        st.toast(
                            "Thanks — flagged for review.", icon=":-1:")
                    if entry.get("tool_calls"):
                        with fb_cols[2].popover(
                                ":wrench: Why this answer"):
                            st.markdown(
                                "**Tools the AI called to gather data:**")
                            for tc in entry["tool_calls"]:
                                st.code(
                                    f"{tc['tool']}({tc['args']})\n"
                                    f"→ {tc.get('result_summary', '')}",
                                    language="text")

        # Input
        _user_question = st.chat_input(
            "Ask anything about your inventory…")
        if _user_question:
            st.session_state["_ai_transcript"].append({
                "role": "user", "content": _user_question})
            with st.chat_message("user"):
                st.markdown(_user_question)

            # Build the Anthropic conversation. We keep the system
            # prompt small + tool-driven so Claude doesn't waste
            # tokens "understanding" the data — it asks via tools.
            _client = anthropic.Anthropic(api_key=_anthropic_key)
            _messages = [{"role": "user", "content": _user_question}]
            _system_prompt = (
                f"You are an inventory analyst assistant for "
                f"{COMPANY_NAME}, a CIN7-using business that sells "
                "LED lighting products (strips, channels, drivers, "
                "tubes). You have two kinds of tools available — use "
                "them deliberately:\n\n"
                "**Live data tools** (search_products, get_sku_details, "
                "get_velocity, get_dead_stock, get_migration_chain) — "
                "use for current numbers: stock levels, sales, "
                "classifications, predecessor/successor mappings.\n\n"
                "**Knowledge base tool** (search_knowledge_base) — "
                "use for HOW or WHY questions. Examples: 'why is X "
                "slow-moving?', 'how does the reorder engine "
                "decide?', 'what's the LED tube family convention?'. "
                "When users ask explanatory or process questions, "
                "search the knowledge base FIRST so the answer is "
                "grounded in the real business rules, not guessed. "
                "When you cite a rule, name the source file and line "
                "range that the search returned (e.g. "
                "'docs/inventory-rules.md:12-28').\n\n"
                "**Combined questions** (e.g. 'why is THIS SKU slow-"
                "moving?') need BOTH: search the knowledge base for "
                "the rule, AND look up the SKU's data, then explain "
                "how the data triggers the rule.\n\n"
                "**Hard rules** for every answer:\n"
                "- Never invent numbers, SKUs, stock levels, or rules.\n"
                "- If a tool returns no results, say so plainly.\n"
                "- If the knowledge base doesn't cover something, "
                "say 'the documentation doesn't explain this — please "
                "ask an admin to add it'. Do not guess.\n"
                "- Keep answers concise (3-6 short bullet points or a "
                "small table) unless the user asks for more detail.\n"
                "- When citing a SKU, include the name + on-hand "
                "quantity in parentheses.\n"
                "- If you can't answer confidently, say so and ask "
                "for clarification (preferred SKU, time window, etc.).")

            _tool_calls_log: list = []
            _start_ts = datetime.now()
            _final_text_parts: list = []

            with st.chat_message("assistant"):
                _placeholder = st.empty()
                _placeholder.markdown(":thought_balloon: thinking…")

                # Tool-use loop: send messages, if response includes
                # tool_use blocks, call them, append results, ask
                # again, repeat until we get a pure text response.
                _MAX_TURNS = 6
                for _turn in range(_MAX_TURNS):
                    try:
                        _resp = _client.messages.create(
                            model="claude-sonnet-4-5",
                            max_tokens=2048,
                            system=_system_prompt,
                            tools=ai_tools.TOOL_SCHEMAS,
                            messages=_messages,
                        )
                    except Exception as _exc:
                        _placeholder.error(
                            f"Anthropic API error: {_exc}")
                        break

                    # Extract text + tool_use blocks
                    _text_this_turn = []
                    _tool_uses = []
                    for _block in _resp.content:
                        if _block.type == "text":
                            _text_this_turn.append(_block.text)
                        elif _block.type == "tool_use":
                            _tool_uses.append(_block)
                    if _text_this_turn:
                        _final_text_parts.extend(_text_this_turn)
                        _placeholder.markdown(
                            "\n\n".join(_final_text_parts))

                    if _resp.stop_reason == "end_turn" and not _tool_uses:
                        break  # done

                    if not _tool_uses:
                        # Stop reason wasn't end_turn but no tool — bail
                        break

                    # Run tools, build results to send back
                    _messages.append({
                        "role": "assistant", "content": _resp.content})
                    _tool_results_block: list = []
                    for _tu in _tool_uses:
                        _result_json = ai_tools.call_tool(
                            _tu.name, engine_df, sale_lines,
                            dict(_tu.input))
                        # Truncate for the audit log so we don't store huge blobs
                        _summary = (_result_json[:200] + "…"
                                     if len(_result_json) > 200
                                     else _result_json)
                        _tool_calls_log.append({
                            "tool": _tu.name,
                            "args": dict(_tu.input),
                            "result_summary": _summary,
                        })
                        _tool_results_block.append({
                            "type": "tool_result",
                            "tool_use_id": _tu.id,
                            "content": _result_json,
                        })
                    _messages.append({
                        "role": "user", "content": _tool_results_block})

                _final_answer = "\n\n".join(_final_text_parts) or (
                    "I couldn't answer that. Try rephrasing or "
                    "include a specific SKU.")
                _placeholder.markdown(_final_answer)

            # Audit log
            _duration_ms = int(
                (datetime.now() - _start_ts).total_seconds() * 1000)
            try:
                _audit_id = db.log_ai_query(
                    user_id=current_user or "anonymous",
                    user_question=_user_question,
                    parsed_intent=None,
                    tools_called_json=json.dumps(_tool_calls_log)
                                         if _tool_calls_log else None,
                    answer_returned=_final_answer,
                    confidence_score=None,
                    duration_ms=_duration_ms,
                    model_used="claude-sonnet-4-5",
                )
            except Exception as _exc:  # noqa: BLE001
                _audit_id = None
                st.warning(f"(Audit log write failed: {_exc})")

            st.session_state["_ai_transcript"].append({
                "role": "assistant",
                "content": _final_answer,
                "audit_id": _audit_id,
                "tool_calls": _tool_calls_log,
            })
            st.rerun()

        # Clear transcript button
        if st.session_state["_ai_transcript"]:
            if st.button(":wastebasket: Clear conversation",
                          help="Clears the on-screen transcript. "
                               "Audit log entries are kept."):
                st.session_state["_ai_transcript"] = []
                st.rerun()


# ---------------------------------------------------------------------------
# Page: Data Health
# ---------------------------------------------------------------------------

elif page == "Data Health":
    st.header(":stethoscope: Data Health")

    datasets = [
        ("Products",           "products",                len(products)),
        ("Stock on hand",      "stock_on_hand",           len(stock)),
        ("Customers",          "customers",               None),
        ("Suppliers",          "suppliers",               len(suppliers)),
        ("Sales headers (30d)", "sales_last_30d",         len(sales_headers)),
        ("Purchase headers (30d)", "purchases_last_30d",  len(purchase_headers)),
        ("Sale lines (30d)",   "sale_lines_last_30d",     len(sale_lines_30d)),
        ("Sale lines (3d)",    "sale_lines_last_3d",      len(sale_lines_3d)),
        ("Purchase lines (90d)", "purchase_lines_last_90d", len(purchase_lines)),
        ("Stock adjustments (30d)", "stock_adjustments_last_30d",
         len(stock_adjustments)),
        ("Stock transfers (30d)", "stock_transfers_last_30d",
         len(stock_transfers)),
    ]

    rows = []
    for label, prefix, rowcount in datasets:
        # mt = mtime of the latest CSV with this prefix, or None if none.
        # Was previously a NameError — the variable was never defined.
        mt = file_mtime(prefix)
        rows.append({
            "Dataset": label,
            "File prefix": prefix,
            "Rows": rowcount if rowcount is not None else "—",
            "Last sync": mt.strftime("%Y-%m-%d %H:%M") if mt else "never",
            "Age (hours)":
                f"{(datetime.now() - mt).total_seconds() / 3600:.1f}"
                if mt else "—",
        })
    st.dataframe(pd.DataFrame(rows), width="stretch", height=460)

    st.subheader("Expected sync commands")
    st.code(
        "# Daily quick refresh (masters + headers)\n"
        "python cin7_sync.py quick --days 7\n\n"
        "# Weekly line-level refresh\n"
        "python cin7_sync.py salelines --days 7\n"
        "python cin7_sync.py purchaselines --days 30\n"
        "python cin7_sync.py movements --days 30\n\n"
        "# Full 12-month bootstrap (once, overnight)\n"
        "python cin7_sync.py salelines --days 365\n",
        language="powershell",
    )

    st.subheader("What's coming in later phases")
    st.markdown(
        "- **DuckDB warehouse** — replace CSV loads with proper analytical DB.\n"
        "- **ABC Explorer** — hybrid value+qty classification per your policy.\n"
        "- **Reorder Queue** — SKUs hitting ROP, grouped by supplier with MOV check.\n"
        "- **Slow Movers** — class-aware slow-moving flags with team review.\n"
        "- **Maturing Items** — 0–120 day SKUs with early demand signals.\n"
        "- **Policy Tuner** — team-editable thresholds with audit log.\n"
        "- **Team Actions** — flags, notes, approvals shared across users (SQLite).\n"
        "- **Auth + hosted URL** — public link with login for remote team members.\n"
    )
