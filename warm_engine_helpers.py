"""warm_engine_helpers.py
==========================
Helper module that imports `_abc_engine` from app.py and calls it
with the latest CSV-derived DataFrames, populating Streamlit's
on-disk cache so the next user gets a hot cache.

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


def _load_dataframes() -> dict[str, Any]:
    """Read the freshest products, stock, sale_lines, purchase_lines
    CSVs from the OUTPUT_DIR location. Returns a dict ready to pass
    as kwargs to _abc_engine."""
    import pandas as pd
    from data_paths import OUTPUT_DIR

    products_csv = _latest_csv(OUTPUT_DIR, "products_*.csv")
    stock_csv = _latest_csv(OUTPUT_DIR, "stock_on_hand_*.csv")
    purchase_lines_csv = _latest_csv(
        OUTPUT_DIR, "purchase_lines_last_*.csv")

    # sale_lines: pick the longest-window file, mirroring the app's
    # _load_longest_sale_lines() preference.
    sale_lines_files = sorted(
        OUTPUT_DIR.glob("sale_lines_last_*d_*.csv"),
        key=lambda p: (
            -int(p.name.split("_last_")[1].split("d_")[0])
            if "_last_" in p.name and "d_" in p.name else 0,
            -p.stat().st_mtime,
        ),
    )
    sale_lines_csv = sale_lines_files[0] if sale_lines_files else None

    missing = [
        name for name, path in (
            ("products", products_csv),
            ("stock_on_hand", stock_csv),
            ("sale_lines", sale_lines_csv),
            ("purchase_lines", purchase_lines_csv),
        ) if path is None
    ]
    if missing:
        raise FileNotFoundError(
            f"Missing CSVs in {OUTPUT_DIR}: {missing}. "
            "Has the sync run yet?"
        )

    return {
        "products": pd.read_csv(products_csv, low_memory=False),
        "stock": pd.read_csv(stock_csv, low_memory=False),
        "sale_lines": pd.read_csv(sale_lines_csv, low_memory=False),
        "purchase_lines": pd.read_csv(
            purchase_lines_csv, low_memory=False),
        "_source_paths": {
            "products": str(products_csv),
            "stock": str(stock_csv),
            "sale_lines": str(sale_lines_csv),
            "purchase_lines": str(purchase_lines_csv),
        },
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
        from app import _abc_engine
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"could not import _abc_engine: {exc!r}") from exc

    bundle = _load_dataframes()
    products = bundle["products"]
    stock = bundle["stock"]
    sale_lines = bundle["sale_lines"]
    purchase_lines = bundle["purchase_lines"]

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

    # The actual warm. _abc_engine is @st.cache_data — calling it
    # populates the disk cache. Discard the return value; the act of
    # calling is what matters.
    result_df = _abc_engine(
        products, stock, sale_lines, purchase_lines)
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
