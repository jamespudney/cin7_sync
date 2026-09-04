"""Daily value snapshots derived from the engine frame.

Writes one row per calendar day into ``slow_mover_value_snapshots`` and
``dead_stock_value_snapshots`` (last write wins). These feed the
Monthly Metrics "Slow Stock Value (EOM)" row and the stock-optimisation
progress chart, and the Overview month-over-month captions.

History: the same writes used to live inline at the tail of
``_abc_engine`` inside a bare ``except: pass``. They silently stopped
on 2026-06-23 (last slow-mover row) and the dead-stock table never
received a row at all, so Monthly Metrics showed $0 for Jul–Sep 2026.
This module makes the write explicit, tolerant of column dtype drift,
and reports errors to the caller instead of swallowing them. It is
called from the warm job (every nearsync) and from the engine tail.
"""
from __future__ import annotations

from typing import Any

import pandas as pd


def _bool_col(df: pd.DataFrame, name: str) -> pd.Series:
    if name not in df.columns:
        return pd.Series(False, index=df.index)
    col = df[name]
    if col.dtype != bool:
        col = col.map(lambda v: bool(v) if pd.notna(v) else False)
    return col.astype(bool)


def _num_col(df: pd.DataFrame, name: str) -> pd.Series:
    if name not in df.columns:
        return pd.Series(0.0, index=df.index)
    return pd.to_numeric(df[name], errors="coerce").fillna(0.0)


def slow_stock_totals(df: pd.DataFrame) -> dict[str, float]:
    """Slow (dormant) stock actually on shelf: parents/standalones with
    OnHand > 0 — same definition as ``_compute_slow_stock_holding``."""
    mask = (_bool_col(df, "is_dormant")
            & (_num_col(df, "OnHand") > 0)
            & ~_bool_col(df, "is_non_master_tube"))
    sub = df.loc[mask]
    return {
        "skus": int(len(sub)),
        "units": float(_num_col(sub, "OnHand").sum()),
        "value": float(_num_col(sub, "OnHandValue").sum()),
    }


def dead_stock_totals(df: pd.DataFrame) -> dict[str, float]:
    sub = df.loc[_bool_col(df, "is_dead")]
    return {
        "skus": int(len(sub)),
        "units": float(_num_col(sub, "OnHand").sum()),
        "value": float(_num_col(sub, "OnHandValue").sum()),
    }


def record_value_snapshots(df: pd.DataFrame, db_module: Any) -> dict:
    """Write today's slow-mover and dead-stock value rows. Never raises;
    returns {"slow": {...}|{"error"}, "dead": {...}|{"error"}} so the
    caller can log it."""
    out: dict = {}
    if df is None or getattr(df, "empty", True):
        return {"error": "empty engine frame"}
    try:
        slow = slow_stock_totals(df)
        db_module.record_slow_mover_value_snapshot(
            slow["skus"], slow["units"], slow["value"])
        out["slow"] = slow
    except Exception as exc:  # noqa: BLE001
        out["slow"] = {"error": repr(exc)}
    try:
        dead = dead_stock_totals(df)
        db_module.record_dead_stock_value_snapshot(
            dead["skus"], dead["units"], dead["value"])
        out["dead"] = dead
    except Exception as exc:  # noqa: BLE001
        out["dead"] = {"error": repr(exc)}
    return out
