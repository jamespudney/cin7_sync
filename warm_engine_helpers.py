"""warm_engine_helpers.py
==========================
Helper module that imports the dashboard's `_abc_engine` and the same
already-loaded DataFrames from app.py, populating `engine_output.csv`
so the next user gets a hot cache.

Why a separate file
-------------------
Importing `app.py` from a script triggers all its module-level
Streamlit calls (st.set_page_config, st.sidebar UI, etc.). To call
`_abc_engine` without lighting up the whole UI, we'd need to either
(a) refactor app.py to put its UI inside an `if __name__ == "__app__"`
guard or (b) accept that import is fine outside a real Streamlit
runtime — Streamlit's UI calls become no-ops when there's no script
context.

Option (b) works in practice. Streamlit's recent versions print
warnings about "missing ScriptRunContext" but functions like
`st.cache_data` still execute correctly because they fall back to a
local in-process cache plus disk persistence. The disk persistence
is what we need.

If a future Streamlit version makes (b) infeasible, the alternative
is to spin up a real Streamlit headless run pointing at a tiny
warmup script that calls `_abc_engine` once and exits.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any

# Quiet Streamlit's "missing ScriptRunContext" warnings during the
# warm — they're harmless here.
os.environ.setdefault("STREAMLIT_LOGGER_LEVEL", "error")


def _latest_csv(output_dir: Path, pattern: str) -> Path | None:
    matches = sorted(
        output_dir.glob(pattern),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    return matches[0] if matches else None


def _source_paths(output_dir: Path) -> dict[str, str]:
    """Best-effort source paths for warm logs/status output."""
    sale_line_paths = sorted(
        output_dir.glob("sale_lines_last_*d_*.csv"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    paths = {
        "products": _latest_csv(output_dir, "products_*.csv"),
        "stock": _latest_csv(output_dir, "stock_on_hand_*.csv"),
        "purchase_lines": _latest_csv(output_dir, "purchase_lines_last_*.csv"),
        "assemblies": _latest_csv(output_dir, "assemblies_last_*.csv"),
    }
    return {
        **{k: str(v) for k, v in paths.items() if v is not None},
        "sale_lines": ", ".join(str(p) for p in sale_line_paths),
    }


def _dataframes_from_app(app_module: Any) -> dict[str, Any]:
    """Use the dashboard's loaded frames instead of re-reading CSVs.

    Importing app.py already runs the same lean/union loaders the user
    sees in Streamlit. Reusing those objects prevents a second full CSV
    copy in memory and keeps engine_output.csv aligned with the grid's
    freshest sale-line union.
    """
    from data_paths import OUTPUT_DIR

    missing = [
        name for name in ("products", "stock", "sale_lines",
                          "purchase_lines")
        if getattr(app_module, name, None) is None
    ]
    if missing:
        raise RuntimeError(f"app.py did not expose frames: {missing}")

    return {
        "products": app_module.products,
        "stock": app_module.stock,
        "sale_lines": app_module.sale_lines,
        "purchase_lines": app_module.purchase_lines,
        "assemblies": getattr(app_module, "assemblies", None),
        "_source_paths": _source_paths(OUTPUT_DIR),
    }


def warm() -> dict[str, Any]:
    """Load fresh data, call _abc_engine, write cache. Returns a
    summary dict for the caller's log output."""
    import datetime as _dt
    import pandas as pd  # noqa: F401
    from data_paths import OUTPUT_DIR

    # Import _abc_engine lazily so any Streamlit import noise during
    # app.py module-load is contained. We wrap in a try-except to
    # report import errors cleanly to the calling script.
    try:
        # app.py is at the project root; sys.path needs to include
        # this directory so the import resolves.
        _here = Path(__file__).resolve().parent
        if str(_here) not in sys.path:
            sys.path.insert(0, str(_here))
        import app as _app
        _abc_engine = _app._abc_engine
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"could not import _abc_engine: {exc!r}") from exc

    bundle = _dataframes_from_app(_app)
    products = bundle["products"]
    stock = bundle["stock"]
    sale_lines = bundle["sale_lines"]
    purchase_lines = bundle["purchase_lines"]
    assemblies = bundle.get("assemblies")

    # Coerce the columns the engine expects. The Streamlit app does
    # this via _to_date / _to_num; we replicate the minimum subset
    # the engine relies on. If the engine expects more, the call
    # will raise and we'll see it in the log.
    if "InvoiceDate" in sale_lines.columns:
        sale_lines["InvoiceDate"] = pd.to_datetime(
            sale_lines["InvoiceDate"], errors="coerce")
    if "OrderDate" in sale_lines.columns:
        sale_lines["OrderDate"] = pd.to_datetime(
            sale_lines["OrderDate"], errors="coerce")
    if "Quantity" in sale_lines.columns:
        sale_lines["Quantity"] = pd.to_numeric(
            sale_lines["Quantity"], errors="coerce").fillna(0)
    if "Total" in sale_lines.columns:
        sale_lines["Total"] = pd.to_numeric(
            sale_lines["Total"], errors="coerce").fillna(0)

    if "OnHand" in stock.columns:
        stock["OnHand"] = pd.to_numeric(
            stock["OnHand"], errors="coerce").fillna(0)
    if "Available" in stock.columns:
        stock["Available"] = pd.to_numeric(
            stock["Available"], errors="coerce").fillna(0)

    # The actual warm. _abc_engine is @st.cache_data; calling it
    # populates the disk cache and we also write the portable CSV
    # snapshot the web app prefers.
    result_df = _abc_engine(
        products, stock, sale_lines, purchase_lines,
        assemblies_df=assemblies)
    out_path = OUTPUT_DIR / "engine_output.csv"
    tmp_path = OUTPUT_DIR / "engine_output.tmp.csv"
    result_df.to_csv(tmp_path, index=False)
    tmp_path.replace(out_path)

    return {
        "rows": int(len(result_df)) if hasattr(result_df, "__len__") else None,
        "cached_at": _dt.datetime.utcnow().isoformat() + "Z",
        "sources": bundle["_source_paths"],
    }


if __name__ == "__main__":
    info = warm()
    print(info)
