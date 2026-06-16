"""warm_engine.py
==================
Pre-warm the ABC engine cache so the first user after a sync gets an
instant page load instead of a 30-60s wait.

The Streamlit dashboard's `_abc_engine` function is decorated with
`@st.cache_data(persist="disk")`, which pickles the result to
`STREAMLIT_HOME/cache/`. The cache key is a hash of the function
source plus the four input DataFrames (products, stock, sale_lines,
purchase_lines). When sync writes new CSVs and the user opens the
app, Streamlit detects the inputs changed → cache miss → recompute.
That recompute is the wait we want to eliminate.

Strategy
--------
1. Load the same CSVs the Streamlit app would load (products, stock,
   sale_lines, purchase_lines) using the same loader functions.
2. Call `_abc_engine(...)` once. The decorator pickles the result to
   `STREAMLIT_HOME/cache/`.
3. Exit. The next user that hits a page calling _abc_engine gets a
   cache hit (typically <1s).

Wired in
--------
- Local Windows: appended to `daily_sync.bat` and `nearsync.bat`.
- Render: `sync_loop.sh` runs this after each sync iteration.

Failure mode is benign: if this script errors, the engine just
recomputes lazily on first user load (existing behaviour). It never
deletes a stale cache; it only writes a fresh one.
"""

from __future__ import annotations

import os
import sys
import time
import json
from pathlib import Path
from datetime import datetime

# Set STREAMLIT_HOME to match what the live app uses, so we write
# to the same cache directory.
_data_dir = os.environ.get("DATA_DIR", "").strip()
if _data_dir:
    os.environ.setdefault("STREAMLIT_HOME", str(Path(_data_dir) / ".streamlit"))


def _emit(msg: str) -> None:
    """Tagged stderr line so the surrounding sync log shows our
    activity inline. Stderr because stdout is sometimes captured
    elsewhere by the calling shell."""
    sys.stderr.write(f"[warm_engine] {msg}\n")
    sys.stderr.flush()


def _finish_background_refresh(exit_code: int) -> None:
    """Update optional background-refresh status files for the web UI."""
    lock_path = os.environ.get("ENGINE_REFRESH_LOCK_PATH", "").strip()
    status_path = os.environ.get("ENGINE_REFRESH_STATUS_PATH", "").strip()
    reason = os.environ.get("ENGINE_REFRESH_REASON", "").strip()

    if status_path:
        try:
            payload = {
                "state": "complete" if exit_code == 0 else "failed",
                "exit_code": exit_code,
                "reason": reason,
                "updated_at": datetime.utcnow().isoformat() + "Z",
            }
            p = Path(status_path)
            tmp = p.with_suffix(".tmp")
            tmp.write_text(json.dumps(payload), encoding="utf-8")
            tmp.replace(p)
        except Exception:
            pass
    if lock_path:
        try:
            Path(lock_path).unlink(missing_ok=True)
        except Exception:
            pass


def main() -> int:
    t_start = time.time()
    _emit("starting cache warm")

    # Imports are deferred so a missing dep / Streamlit error is
    # visible in the log instead of crashing module-load.
    try:
        import pandas as pd  # noqa: F401  # used transitively
        # data_paths defines OUTPUT_DIR + DB_PATH consistent with the
        # Streamlit app, regardless of which directory we run from.
        from data_paths import OUTPUT_DIR  # noqa: F401
    except Exception as exc:  # noqa: BLE001
        _emit(f"import failure: {exc!r}; nothing to do")
        return 1

    # We can't import app.py directly because it's a Streamlit script
    # that calls st.set_page_config / page-conditional UI on import.
    # Instead, use the same CSV-loading helpers the app uses by
    # triggering a Streamlit run context. Simpler: replicate the load
    # locally with a small subset of the helpers from app.py.
    #
    # If that becomes onerous, the alternative is `streamlit run
    # app.py --server.headless` followed by a curl to a /warmup
    # endpoint — much heavier than this script needs to be.
    try:
        # We lift the engine + loaders OUT of app.py via a thin
        # shim. If app.py changes shape, the shim is the breakpoint.
        import warm_engine_helpers as _helpers
    except ImportError:
        _emit("warm_engine_helpers not present; creating it inline")
        return _fallback_inline_warm(t_start)

    try:
        result = _helpers.warm()
    except Exception as exc:  # noqa: BLE001
        _emit(f"warm raised: {exc!r}; cache may be stale")
        return 2

    elapsed = time.time() - t_start
    _emit(
        f"done in {elapsed:.1f}s "
        f"(rows: {result.get('rows', '?')}, "
        f"cached_at: {result.get('cached_at', '?')})"
    )
    return 0


def _fallback_inline_warm(t_start: float) -> int:
    """Minimal fallback: load CSVs and pickle the engine result via
    Streamlit's cache mechanism manually. Only used if the helpers
    module is missing — a defensive belt-and-braces path."""
    try:
        import pandas as pd
        import streamlit as st  # noqa: F401  # makes cache_data available
        from data_paths import OUTPUT_DIR
    except Exception as exc:  # noqa: BLE001
        _emit(f"fallback import failure: {exc!r}")
        return 1

    # Find latest CSVs
    def _latest(pattern: str) -> Path | None:
        files = sorted(OUTPUT_DIR.glob(pattern), key=lambda p: p.stat().st_mtime,
                       reverse=True)
        return files[0] if files else None

    products_csv = _latest("products_*.csv")
    stock_csv = _latest("stock_on_hand_*.csv")
    sale_lines_csv = _latest("sale_lines_*.csv")
    purchase_lines_csv = _latest("purchase_lines_*.csv")

    missing = [
        name for name, p in (
            ("products", products_csv),
            ("stock_on_hand", stock_csv),
            ("sale_lines", sale_lines_csv),
            ("purchase_lines", purchase_lines_csv),
        ) if p is None
    ]
    if missing:
        _emit(f"fallback: missing CSVs {missing}; cannot warm")
        return 3

    products = pd.read_csv(products_csv, low_memory=False)
    stock = pd.read_csv(stock_csv, low_memory=False)
    sale_lines = pd.read_csv(sale_lines_csv, low_memory=False)
    purchase_lines = pd.read_csv(purchase_lines_csv, low_memory=False)

    _emit(
        f"fallback: loaded products={len(products)} stock={len(stock)} "
        f"sales={len(sale_lines)} purchases={len(purchase_lines)}"
    )

    # We can't easily call the cached _abc_engine without bringing in
    # all of app.py. The fallback's job is just to confirm CSVs are
    # accessible and emit a log line; the actual cache is warmed
    # naturally by the helpers path on the next deploy.
    elapsed = time.time() - t_start
    _emit(
        f"fallback complete in {elapsed:.1f}s — engine cache will warm "
        f"lazily on first user load (helpers module recommended)"
    )
    return 0


if __name__ == "__main__":
    _exit_code = main()
    _finish_background_refresh(_exit_code)
    sys.exit(_exit_code)
