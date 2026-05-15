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
some edge cases (bulk-master rollup, A-class grace, multi-tier
dormancy refinement) are simplified.

Drift sources (will fix in v2.67.70 with shared Postgres):
- No A-class grace check (worker may flag A-class as dormant when
  dashboard wouldn't)
- No bulk-master rollup (per-foot children counted separately)
- No buyer manual corrections (those live in web service's DB)

Output columns added to engine_df:
- ABC                       'A' | 'B' | 'C'
- effective_units_12mo      float — 12-month demand in SKU units
- effective_units_90d       float — 90-day demand
- annual_value              effective_units_12mo × AverageCost
- is_dormant                bool — 12mo activity > 0 AND 90d ≤ 20% of 12mo rate
- excess_units              max(0, OnHand - effective_units_12mo)
- excess_value              excess_units × AverageCost
- OnHandValue               OnHand × AverageCost (fallback for stock value)
- trend_flag                'Stable' | '📈 Trend' | '📉 Decline' | '(unknown)'
- is_non_master_tube        bool — heuristic: SKU has per-foot suffix

Public API:
  compute_engine_signals(products, stock, sale_lines) -> pd.DataFrame
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd

log = logging.getLogger("worker_engine")


def _to_num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")


def compute_engine_signals(products: pd.DataFrame,
                              stock: pd.DataFrame,
                              sale_lines: pd.DataFrame
                              ) -> pd.DataFrame:
    """Compute the engine signals on the worker. Returns engine_df with
    one row per SKU and all derived columns."""
    if products is None or products.empty:
        return pd.DataFrame()

    # 1. Base merge: products + stock_on_hand on SKU.
    df = products.copy()
    df["SKU"] = df["SKU"].astype(str)

    if stock is not None and not stock.empty:
        stock_view = stock.copy()
        stock_view["SKU"] = stock_view["SKU"].astype(str)
        cols_to_pull = ["SKU"]
        # v2.67.190 — include CIN7's newer "Stock Locator" /
        # "StockLocator" column names too. Some CSV exports use
        # the spaced UI label verbatim.
        for c in ("OnHand", "OnOrder", "Available", "Bin",
                    "BinLocation", "StockLocator", "Stock Locator",
                    "Location", "StockOnHand"):
            if c in stock_view.columns:
                cols_to_pull.append(c)
        df = df.merge(stock_view[cols_to_pull].drop_duplicates(
            subset=["SKU"], keep="last"), on="SKU", how="left")

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
    df["is_non_master_tube"] = df["SKU"].apply(_is_non_master)

    # 4. Compute 12mo + 90d demand from sale_lines.
    today_ts = pd.Timestamp(datetime.now().date())
    cutoff_12mo = today_ts - pd.Timedelta(days=365)
    cutoff_90d = today_ts - pd.Timedelta(days=90)

    eff_12mo_map: dict = {}
    eff_90d_map: dict = {}
    if sale_lines is not None and not sale_lines.empty:
        sl = sale_lines.copy()
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

    df["effective_units_12mo"] = df["SKU"].map(
        lambda s: float(eff_12mo_map.get(s, 0)))
    df["effective_units_90d"] = df["SKU"].map(
        lambda s: float(eff_90d_map.get(s, 0)))

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
    def _dormant(row) -> bool:
        on_hand = float(row.get("OnHand") or 0)
        e12 = float(row["effective_units_12mo"] or 0)
        e90 = float(row["effective_units_90d"] or 0)
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

    return df
