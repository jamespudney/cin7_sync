"""Stock goal model — the ONE place that defines the inventory figures
shown across the app (Command Centre, Ordering headline, per-vendor
tiles, Slow Movers, snapshots).

Background (James, 2026-09-03). The engine's ``target_stock`` is an
*order-up-to reorder level*: ``avg_daily x (LT x (1 + safety%) +
review_days)``. Summed over every SKU it came to ~$165k against ~$700k
on the shelf, i.e. 24 days of cover / ~15 turns — not a figure a
distributor on 21–35 day European lead times with ~2,450 active SKUs
can run at. It is the right number for *when and how much to order*,
but the wrong number for *how much stock the business should carry*,
because it structurally omits:

* range stock — anything selling fewer than ~10 units/yr rounds to a
  target below one unit, yet you must hold at least one sellable
  unit/pack to have a catalogue (1,706 of 2,451 active SKUs, $295k);
* cycle stock from pack sizes / MOQ / MOV (only 6 SKUs carry a MOQ or
  batch qty);
* it only refreshed when someone opened the Ordering page.

So the app now carries THREE clearly named figures, all defined here:

``reorder_level``  — the engine's ``target_stock`` (unchanged formula,
                     plus the range floor below). Drives POs.
``goal``           — the stock the business *should* carry:
                     ``max(avg_daily x cover_days[class], reorder_level,
                     range_floor)``; zero for D-class (no demand),
                     dropship, discontinued and do-not-reorder SKUs.
``excess`` / ``understock`` — always measured against ``goal``.

Cover days per class are the policy knobs (``DEFAULT_COVER_DAYS``).

Everything here is Streamlit-free so it can be unit-tested and run
from ``warm_engine`` as well as the dashboard.
"""

from __future__ import annotations

from typing import Iterable, Mapping

import pandas as pd

# Days of cover the business wants to carry per class. A is kept
# tight (it turns ~7x already and the reorder level usually exceeds
# this anyway), B and C carry more because they are bought less often
# and hold the range together. D (no demand in 12 months) is 0.
DEFAULT_COVER_DAYS: dict[str, float] = {"A": 50.0, "B": 75.0, "C": 150.0}

# Classes that get a range floor of one unit / one pack when they have
# live demand. D never does.
_RANGE_FLOOR_CLASSES = ("A", "B", "C")

GOAL_COLUMNS = ("ABCD", "planning_avg_daily", "range_floor_units",
                "goal_units", "goal_value", "unit_cost_for_goal",
                "excess_exempt")


# --------------------------------------------------------------------
# Small scalar helpers (unit-tested directly)
# --------------------------------------------------------------------

def _num(value, default: float = 0.0) -> float:
    try:
        f = float(value)
    except (TypeError, ValueError):
        return default
    if f != f:  # NaN
        return default
    return f


def abcd_class(abc, visible_units_12mo) -> str:
    """Split the engine's cumulative-value ABC into A / B / C / D.

    The engine's ABC is a cumulative *value* rank (A = first 70% of
    annual value, B = next 20%, C = the rest), so every SKU with zero
    12-month demand lands in C. That mixes 1,560 active-but-small SKUs
    with ~5,300 that have not moved at all and hides the active-C
    over-cover. D = ABC "C" with no visible 12-month demand.
    """
    abc_s = str(abc or "").strip()
    if abc_s in ("A", "B"):
        return abc_s
    if _num(visible_units_12mo) <= 0:
        return "D"
    return "C" if abc_s == "C" else (abc_s or "D")


def planning_avg_daily(avg_daily, effective_units_12mo,
                       effective_units_90d, avg_daily_base=None) -> float:
    """Daily rate used for goal / range-floor math.

    The engine's ``avg_daily`` is hard-zeroed for *dormant* SKUs
    (C-class needs >=5 units in 90 days to count as active) so the
    reorder engine lets slow movers run down. That is right for the
    buying trigger but wrong for "what should we carry": a catalogue
    SKU selling 4 units a year is still live stock. So:

    * start from the unadjusted 12-month rate (``avg_daily_base``) when
      the engine has one, otherwise ``avg_daily``;
    * clamp to 0 when the SKU had 12-month demand but NOTHING in the
      last 90 days (ended project / genuinely stopped) — the same
      clamp the Ordering page applies.
    """
    base = _num(avg_daily_base) if avg_daily_base is not None else 0.0
    ad = base if base > 0 else _num(avg_daily)
    if ad <= 0:
        return 0.0
    if _num(effective_units_90d) <= 0 and _num(effective_units_12mo) > 0:
        return 0.0
    return ad


def range_floor_units(planning_daily: float, abcd: str, *,
                      pack_qty: float = 0.0,
                      is_project: bool = False,
                      is_bulk_master: bool = False) -> float:
    """Minimum units to keep a live SKU in the range.

    * one unit, or one pack/batch when the SKU has a pack/EOQ set;
    * only when the SKU has a live planning rate (so dormant, D-class
      and ended-project rows stay at 0);
    * not for Project rows (buyer orders those by hand — RULES 3.4);
    * not for bulk-roll masters: their demand is rolled-up metres and
      the residue floor in ``engine.reorder_math`` already governs
      what counts as "stock" for a roll.
    """
    if abcd not in _RANGE_FLOOR_CLASSES:
        return 0.0
    if planning_daily <= 0 or is_project or is_bulk_master:
        return 0.0
    pack = _num(pack_qty)
    return pack if pack >= 1 else 1.0


def goal_units(planning_daily: float, abcd: str, *,
               reorder_level: float = 0.0,
               floor: float = 0.0,
               cover_days: Mapping[str, float] | None = None) -> float:
    """Units the business should carry for this SKU."""
    if abcd == "D":
        return 0.0
    days = (cover_days or DEFAULT_COVER_DAYS).get(abcd)
    if days is None:
        days = DEFAULT_COVER_DAYS["C"]
    cover = max(0.0, planning_daily) * float(days)
    return max(cover, max(0.0, _num(reorder_level)), max(0.0, floor))


def unit_cost_for_goal(onhand, onhand_value, average_cost, fixed_cost) -> float:
    """Cost basis for valuing goal/excess — the same chain the engine's
    excess baseline uses: FIFO per-unit (OnHandValue / OnHand) when the
    SKU is on the shelf, else AverageCost, else FixedCost."""
    oh = _num(onhand)
    ohv = _num(onhand_value)
    if oh > 0 and ohv > 0:
        return ohv / oh
    ac = _num(average_cost)
    if ac > 0:
        return ac
    return _num(fixed_cost)


# --------------------------------------------------------------------
# DataFrame level
# --------------------------------------------------------------------

def add_abcd_column(df: pd.DataFrame) -> pd.DataFrame:
    """Add/refresh ``ABCD`` from ``ABC`` + visible 12-month demand."""
    if df.empty:
        df["ABCD"] = pd.Series(dtype="object")
        return df
    if "_visible_units_12mo" in df.columns:
        visible = pd.to_numeric(df["_visible_units_12mo"], errors="coerce")
    else:
        cols = [c for c in ("effective_units_12mo", "lineage_units_12mo",
                            "units_12mo") if c in df.columns]
        visible = (df[cols].apply(pd.to_numeric, errors="coerce")
                   .fillna(0).max(axis=1) if cols
                   else pd.Series(0.0, index=df.index))
    visible = visible.fillna(0)
    abc = df["ABC"] if "ABC" in df.columns else pd.Series("", index=df.index)
    df["ABCD"] = [abcd_class(a, v) for a, v in zip(abc, visible)]
    return df


def compute_stock_goal(df: pd.DataFrame, *,
                       cover_days: Mapping[str, float] | None = None,
                       pack_qty_by_sku: Mapping[str, float] | None = None,
                       zero_goal_skus: Iterable[str] | None = None,
                       no_excess_skus: Iterable[str] | None = None,
                       ) -> pd.DataFrame:
    """Add the goal columns (see ``GOAL_COLUMNS``) to an engine frame.

    Works both before the Ordering page has computed ``target_stock``
    (warm job / Command Centre) and after (goal then also covers the
    reorder level). ``zero_goal_skus`` = dropship + do-not-reorder +
    discontinued: they carry no goal, so any stock is excess.
    Non-master rows (cuts/variants) get goal 0 — their demand rolls up
    to the master, exactly as target does (RULES 2.2).
    ``no_excess_skus`` (dropship, RULES 5.4) additionally never count as
    excess: their stock is in transit to a customer, not sitting.
    """
    if df.empty:
        for c in GOAL_COLUMNS:
            df[c] = pd.Series(dtype="float64" if c != "ABCD" else "object")
        return df
    add_abcd_column(df)
    pack_map = {str(k): _num(v) for k, v in (pack_qty_by_sku or {}).items()}
    zero_set = {str(s) for s in (zero_goal_skus or ())}
    no_excess_set = {str(s) for s in (no_excess_skus or ())}

    def _col(name, default=0.0):
        if name in df.columns:
            return pd.to_numeric(df[name], errors="coerce").fillna(default)
        return pd.Series(default, index=df.index)

    avg_daily = _col("avg_daily")
    avg_daily_base = (_col("avg_daily_base") if "avg_daily_base" in df.columns
                      else None)
    eff12 = _col("effective_units_12mo")
    eff90 = _col("effective_units_90d")
    onhand = _col("OnHand")
    ohv = _col("OnHandValue")
    avg_cost = _col("AverageCost")
    fixed_cost = _col("FixedCost")
    target = _col("target_stock") if "target_stock" in df.columns else None
    non_master = (df["is_non_master_tube"].fillna(False).astype(bool)
                  if "is_non_master_tube" in df.columns
                  else pd.Series(False, index=df.index))
    bulk = (df["is_bulk_master"].fillna(False).astype(bool)
            if "is_bulk_master" in df.columns
            else pd.Series(False, index=df.index))
    trend = (df["trend_flag"].astype(str)
             if "trend_flag" in df.columns
             else pd.Series("", index=df.index))
    skus = df["SKU"].astype(str) if "SKU" in df.columns else pd.Series("", index=df.index)

    plan, floors, goals, values, costs = [], [], [], [], []
    for i in range(len(df)):
        abcd = df["ABCD"].iat[i]
        pd_ = planning_avg_daily(
            avg_daily.iat[i], eff12.iat[i], eff90.iat[i],
            avg_daily_base=(avg_daily_base.iat[i]
                            if avg_daily_base is not None else None))
        sku = skus.iat[i]
        if non_master.iat[i] or sku in zero_set:
            fl = 0.0
            gu = 0.0
        else:
            fl = range_floor_units(
                pd_, abcd,
                pack_qty=pack_map.get(sku, 0.0),
                is_project=("Project" in trend.iat[i]),
                is_bulk_master=bool(bulk.iat[i]))
            gu = goal_units(
                pd_, abcd,
                reorder_level=(target.iat[i] if target is not None else 0.0),
                floor=fl, cover_days=cover_days)
        cost = unit_cost_for_goal(onhand.iat[i], ohv.iat[i],
                                  avg_cost.iat[i], fixed_cost.iat[i])
        plan.append(pd_); floors.append(fl); goals.append(gu)
        costs.append(cost); values.append(gu * cost)

    df["planning_avg_daily"] = plan
    df["range_floor_units"] = floors
    df["goal_units"] = goals
    df["unit_cost_for_goal"] = costs
    df["goal_value"] = values
    df["excess_exempt"] = [sku in no_excess_set for sku in skus]
    return df


def excess_and_understock(df: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
    """Per-row excess / understock VALUE against goal.

    Excess is stock above goal at the on-shelf cost basis; understock is
    the goal value not yet on the shelf. Non-masters with their own
    direct sales are working inventory (RULES 4.2) → never excess.
    """
    onhand = pd.to_numeric(df.get("OnHand", 0), errors="coerce").fillna(0)
    goal = pd.to_numeric(df.get("goal_units", 0), errors="coerce").fillna(0)
    cost = pd.to_numeric(df.get("unit_cost_for_goal", 0), errors="coerce").fillna(0)
    excess_units = (onhand - goal).clip(lower=0)
    under_units = (goal - onhand).clip(lower=0)
    if "is_non_master_tube" in df.columns:
        non_master = df["is_non_master_tube"].fillna(False).astype(bool)
        direct = pd.to_numeric(df.get("units_12mo", 0), errors="coerce").fillna(0)
        working_cut = non_master & (direct > 0)
        excess_units = excess_units.where(~working_cut, 0.0)
    if "excess_exempt" in df.columns:
        exempt = df["excess_exempt"].fillna(False).astype(bool)
        excess_units = excess_units.where(~exempt, 0.0)
    return excess_units * cost, under_units * cost


def stock_health_summary(df: pd.DataFrame, *, scope: str = "all") -> dict:
    """The figures every page shows, from one calculation.

    Requires the goal columns (call ``compute_stock_goal`` first).
    ``reorder_level_value`` is None when ``target_stock`` is absent
    (warm job / Command Centre before Ordering has run today).
    """
    if df.empty or "goal_units" not in df.columns:
        return {"sku_count": 0, "current_value": 0.0, "goal_value": 0.0,
                "reorder_level_value": None, "dead_value": 0.0,
                "dead_sku_count": 0, "excess_value": 0.0,
                "understock_value": 0.0, "by_class": [], "scope": scope}
    if "ABCD" not in df.columns:
        add_abcd_column(df)
    if "unit_cost_for_goal" not in df.columns:
        df["unit_cost_for_goal"] = [
            unit_cost_for_goal(a, b, c, d) for a, b, c, d in zip(
                df.get("OnHand", pd.Series(0, index=df.index)),
                df.get("OnHandValue", pd.Series(0, index=df.index)),
                df.get("AverageCost", pd.Series(0, index=df.index)),
                df.get("FixedCost", pd.Series(0, index=df.index)))]
    if "goal_value" not in df.columns:
        df["goal_value"] = (
            pd.to_numeric(df["goal_units"], errors="coerce").fillna(0)
            * pd.to_numeric(df["unit_cost_for_goal"], errors="coerce").fillna(0))
    onhand_value = pd.to_numeric(df.get("OnHandValue", 0), errors="coerce").fillna(0)
    excess, under = excess_and_understock(df)
    cost = pd.to_numeric(df["unit_cost_for_goal"], errors="coerce").fillna(0)
    if "target_stock" in df.columns:
        target = pd.to_numeric(df["target_stock"], errors="coerce").fillna(0)
        reorder_level_value = float((target * cost).sum())
    else:
        reorder_level_value = None
    if "is_dead" in df.columns:
        dead_mask = df["is_dead"].fillna(False).astype(bool)
    else:
        onh = pd.to_numeric(df.get("OnHand", 0), errors="coerce").fillna(0)
        dead_mask = (df["ABCD"] == "D") & (onh > 0)
    annual = pd.to_numeric(df.get("annual_value", 0), errors="coerce").fillna(0)

    by_class = []
    for cls in ("A", "B", "C", "D"):
        m = df["ABCD"] == cls
        cur = float(onhand_value[m].sum())
        cogs = float(annual[m].sum())
        by_class.append({
            "class": cls,
            "sku_count": int(m.sum()),
            "current_value": cur,
            "goal_value": float(df.loc[m, "goal_value"].sum()),
            "excess_value": float(excess[m].sum()),
            "understock_value": float(under[m].sum()),
            "annual_cogs": cogs,
            "days_cover": (365.0 * cur / cogs) if cogs > 0 else None,
        })
    return {
        "scope": scope,
        "sku_count": int(len(df)),
        "current_value": float(onhand_value.sum()),
        "goal_value": float(df["goal_value"].sum()),
        "reorder_level_value": reorder_level_value,
        "dead_value": float(onhand_value[dead_mask].sum()),
        "dead_sku_count": int(dead_mask.sum()),
        "excess_value": float(excess.sum()),
        "understock_value": float(under.sum()),
        "annual_cogs": float(annual.sum()),
        "by_class": by_class,
    }
