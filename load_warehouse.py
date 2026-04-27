"""
load_warehouse.py — consolidate CSVs into a single DuckDB warehouse
===================================================================
Reads every sync CSV in output/ and loads it into `warehouse.duckdb` as
named tables. Designed to run after a deep sync (e.g. weekend_deep_sync.bat)
so the app has one fast, queryable store instead of hundreds of timestamped
CSVs.

Policy per table:
  - Pick the LONGEST-window CSV for each prefix (e.g. sale_lines_last_730d)
    as the base.
  - Union any more-recently-written shorter-window files on top (e.g. today's
    sale_lines_last_1d) to capture intra-day data.
  - Dedupe on a natural key (PurchaseID+SKU+Quantity, etc).
  - CREATE OR REPLACE the DuckDB table so we can re-run safely.

The Streamlit app doesn't read this yet — that's a later refactor. For now
the warehouse is a fast SQL scratchpad for ad-hoc analysis, DuckDB CLI
queries, and future vendor-performance / dead-stock views.

Run:  python load_warehouse.py
Out:  warehouse.duckdb   (adjacent to this script)
"""
from __future__ import annotations

import logging
import re
import sys
from pathlib import Path
from typing import List, Optional, Tuple

import duckdb  # type: ignore
import pandas as pd

APP_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = APP_DIR / "output"
WAREHOUSE_PATH = APP_DIR / "warehouse.duckdb"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-5s  %(message)s",
)
log = logging.getLogger("load_warehouse")


# Each entry: (table_name, file_prefix_glob, dedupe_cols, is_windowed)
# is_windowed = True -> "prefix_last_Nd_*.csv" (use longest N + union
#                       fresher); False -> simple "prefix_*.csv" (take newest)
TABLES = [
    ("products",           "products",          ["SKU"], False),
    ("stock_on_hand",      "stock_on_hand",     ["SKU", "Location"], False),
    ("customers",          "customers",         ["CustomerID"], False),
    ("suppliers",          "suppliers",         ["SupplierID"], False),
    ("boms",               "boms",              None, False),
    ("sales",              "sales_last",
     ["SaleID"], True),
    ("sale_lines",         "sale_lines_last",
     ["SaleID", "SKU", "Quantity", "InvoiceDate"], True),
    ("purchases",          "purchases_last",
     ["PurchaseID"], True),
    ("purchase_lines",     "purchase_lines_last",
     ["PurchaseID", "SKU", "Quantity", "OrderDate", "Price"], True),
    ("stock_adjustments",  "stock_adjustments_last",
     None, True),
    ("stock_transfers",    "stock_transfers_last",
     None, True),
    ("movements",          "movements_last",
     None, True),
]


def _longest_plus_fresh(prefix: str) -> Optional[pd.DataFrame]:
    """For a windowed prefix, pick the longest-window CSV as the base and
    union any more-recently-written shorter-window files on top."""
    files: List[Tuple[int, float, Path]] = []
    for p in OUTPUT_DIR.glob(f"{prefix}_*d_*.csv"):
        m = re.match(rf"{re.escape(prefix)}_(\d+)d_", p.name)
        if m:
            files.append((int(m.group(1)), p.stat().st_mtime, p))
    if not files:
        return None
    files.sort(key=lambda x: (-x[0], -x[1]))   # biggest window, then newest
    base_file = files[0][2]
    base_mtime = files[0][1]
    try:
        base = pd.read_csv(base_file, low_memory=False)
    except Exception as exc:
        log.warning("  Could not read %s: %s", base_file.name, exc)
        return None
    log.info("  base: %s (%d rows)", base_file.name, len(base))
    for days, mtime, p in files[1:]:
        if mtime <= base_mtime:
            continue
        try:
            more = pd.read_csv(p, low_memory=False)
            log.info("  unioning: %s (+%d rows, written after base)",
                     p.name, len(more))
            base = pd.concat([base, more], ignore_index=True)
        except Exception as exc:
            log.warning("  Could not union %s: %s", p.name, exc)
    return base


def _newest(prefix: str) -> Optional[pd.DataFrame]:
    """For a non-windowed prefix, read the single newest CSV."""
    files = sorted(
        OUTPUT_DIR.glob(f"{prefix}_*.csv"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if not files:
        # Fall back to exact prefix.csv if present (legacy layout)
        p = OUTPUT_DIR / f"{prefix}.csv"
        if p.exists():
            return pd.read_csv(p, low_memory=False)
        return None
    log.info("  newest: %s", files[0].name)
    try:
        return pd.read_csv(files[0], low_memory=False)
    except Exception as exc:
        log.warning("  Could not read %s: %s", files[0].name, exc)
        return None


def _dedupe(df: pd.DataFrame, dedupe_cols: Optional[List[str]]) -> pd.DataFrame:
    if not dedupe_cols:
        return df
    cols = [c for c in dedupe_cols if c in df.columns]
    if not cols:
        return df
    before = len(df)
    df = df.drop_duplicates(subset=cols, keep="last")
    if len(df) < before:
        log.info("  deduped: %d -> %d rows (key=%s)",
                 before, len(df), ",".join(cols))
    return df


def load_all() -> None:
    log.info("Opening warehouse at %s", WAREHOUSE_PATH)
    con = duckdb.connect(str(WAREHOUSE_PATH))
    try:
        summary = []
        for table, prefix, dedupe_cols, is_windowed in TABLES:
            log.info("== %s (prefix=%s%s) ==",
                     table, prefix, " [windowed]" if is_windowed else "")
            if is_windowed:
                df = _longest_plus_fresh(prefix)
            else:
                df = _newest(prefix)
            if df is None or df.empty:
                log.warning("  SKIP %s — no data", table)
                summary.append((table, "SKIP", 0))
                continue
            df = _dedupe(df, dedupe_cols)
            # Register with DuckDB and materialise as a real table
            con.register("tmp_df", df)
            con.execute(f"CREATE OR REPLACE TABLE {table} AS "
                        f"SELECT * FROM tmp_df")
            con.unregister("tmp_df")
            rows = con.execute(
                f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            log.info("  loaded %s: %d rows", table, rows)
            summary.append((table, "OK", rows))

        # Helpful indexes on high-cardinality join keys
        log.info("== creating indexes ==")
        _maybe_index(con, "products", "SKU")
        _maybe_index(con, "stock_on_hand", "SKU")
        _maybe_index(con, "sale_lines", "SKU")
        _maybe_index(con, "sale_lines", "InvoiceDate")
        _maybe_index(con, "purchase_lines", "SKU")
        _maybe_index(con, "purchase_lines", "OrderDate")
        _maybe_index(con, "purchase_lines", "Supplier")

        log.info("== summary ==")
        for table, status, rows in summary:
            log.info("  %-22s %-4s  %10d rows", table, status, rows)
        log.info("Warehouse written: %s", WAREHOUSE_PATH)
    finally:
        con.close()


def _maybe_index(con: "duckdb.DuckDBPyConnection", table: str,
                  col: str) -> None:
    """CREATE INDEX defensively — no-op if the column doesn't exist."""
    try:
        cols = {r[1] for r in con.execute(
            f"PRAGMA table_info('{table}')").fetchall()}
    except Exception:
        return
    if col not in cols:
        return
    try:
        con.execute(
            f'CREATE INDEX IF NOT EXISTS ix_{table}_{col} '
            f'ON {table}("{col}")'
        )
    except Exception as exc:
        log.warning("  index on %s(%s) failed: %s", table, col, exc)


if __name__ == "__main__":
    try:
        load_all()
    except Exception:
        log.exception("FATAL")
        sys.exit(1)
