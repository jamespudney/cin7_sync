import pandas as pd

from engine import stock_goal as sg


def test_abcd_splits_zero_demand_c_into_d():
    assert sg.abcd_class("A", 0) == "A"
    assert sg.abcd_class("B", 0) == "B"
    assert sg.abcd_class("C", 3) == "C"
    assert sg.abcd_class("C", 0) == "D"
    assert sg.abcd_class("—", 0) == "D"


def test_planning_rate_drops_ended_projects():
    assert sg.planning_avg_daily(1.5, 500, 0) == 0.0
    assert sg.planning_avg_daily(1.5, 500, 20) == 1.5
    assert sg.planning_avg_daily(0, 0, 0) == 0.0


def test_range_floor_rules():
    # live C-class SKU -> one unit
    assert sg.range_floor_units(0.01, "C") == 1.0
    # pack size wins over one unit
    assert sg.range_floor_units(0.01, "C", pack_qty=25) == 25.0
    # no live rate -> nothing
    assert sg.range_floor_units(0.0, "C") == 0.0
    # D / project / bulk masters are exempt
    assert sg.range_floor_units(0.5, "D") == 0.0
    assert sg.range_floor_units(0.5, "A", is_project=True) == 0.0
    assert sg.range_floor_units(0.5, "A", is_bulk_master=True) == 0.0


def test_goal_units_is_max_of_cover_reorder_level_and_floor():
    # A-class: 2/day x 50 days = 100, reorder level 120 wins
    assert sg.goal_units(2.0, "A", reorder_level=120, floor=1) == 120
    # cover wins when reorder level is smaller
    assert sg.goal_units(2.0, "A", reorder_level=60, floor=1) == 100
    # floor wins for tiny movers
    assert sg.goal_units(0.005, "C", reorder_level=0.4, floor=1) == 1
    # D class never carries a goal
    assert sg.goal_units(0.5, "D", reorder_level=50, floor=1) == 0
    # custom cover days
    assert sg.goal_units(1.0, "B", cover_days={"B": 10}) == 10


def test_unit_cost_chain():
    assert sg.unit_cost_for_goal(10, 50, 7, 9) == 5.0   # FIFO per unit
    assert sg.unit_cost_for_goal(0, 0, 7, 9) == 7.0    # AverageCost
    assert sg.unit_cost_for_goal(0, 0, 0, 9) == 9.0    # FixedCost


def _frame():
    return pd.DataFrame([
        # A-class, moving, below goal (understock)
        dict(SKU="A1", ABC="A", avg_daily=2.0, effective_units_12mo=730,
             effective_units_90d=180, units_12mo=730, OnHand=40,
             OnHandValue=400, AverageCost=10, FixedCost=9,
             is_non_master_tube=False, is_bulk_master=False,
             trend_flag="Stable", annual_value=7300, is_dead=False),
        # C-class tiny mover, engine target rounds to ~0, holds 6 units
        dict(SKU="C1", ABC="C", avg_daily=0.01, effective_units_12mo=4,
             effective_units_90d=1, units_12mo=4, OnHand=6,
             OnHandValue=60, AverageCost=10, FixedCost=9,
             is_non_master_tube=False, is_bulk_master=False,
             trend_flag="Stable", annual_value=40, is_dead=False),
        # D-class dead stock
        dict(SKU="D1", ABC="C", avg_daily=0.0, effective_units_12mo=0,
             effective_units_90d=0, units_12mo=0, OnHand=20,
             OnHandValue=200, AverageCost=10, FixedCost=9,
             is_non_master_tube=False, is_bulk_master=False,
             trend_flag="💤 Dormant", annual_value=0, is_dead=True),
        # non-master cut with its own sales: working inventory
        dict(SKU="CUT1", ABC="C", avg_daily=0.0, effective_units_12mo=0,
             effective_units_90d=0, units_12mo=12, OnHand=3,
             OnHandValue=30, AverageCost=10, FixedCost=9,
             is_non_master_tube=True, is_bulk_master=False,
             trend_flag="Stable", annual_value=0, is_dead=False),
        # dropship SKU: zero goal, all stock is excess
        dict(SKU="DS1", ABC="B", avg_daily=1.0, effective_units_12mo=365,
             effective_units_90d=90, units_12mo=365, OnHand=5,
             OnHandValue=50, AverageCost=10, FixedCost=9,
             is_non_master_tube=False, is_bulk_master=False,
             trend_flag="Stable", annual_value=3650, is_dead=False),
    ])


def test_compute_stock_goal_and_summary():
    df = sg.compute_stock_goal(_frame(), zero_goal_skus={"DS1"},
                               no_excess_skus={"DS1"})
    g = df.set_index("SKU")
    assert g.loc["A1", "ABCD"] == "A"
    assert g.loc["A1", "goal_units"] == 100          # 2/day x 50d
    assert g.loc["C1", "ABCD"] == "C"
    assert g.loc["C1", "range_floor_units"] == 1
    assert g.loc["C1", "goal_units"] == 1.5          # 0.01 x 150d
    assert g.loc["D1", "ABCD"] == "D"
    assert g.loc["D1", "goal_units"] == 0
    assert g.loc["CUT1", "goal_units"] == 0
    assert g.loc["DS1", "goal_units"] == 0

    s = sg.stock_health_summary(df)
    assert s["sku_count"] == 5
    assert s["current_value"] == 740
    assert s["reorder_level_value"] is None
    assert round(s["goal_value"], 2) == 1015.0       # 1000 + 15
    assert s["dead_value"] == 200 and s["dead_sku_count"] == 1
    # excess: C1 4.5 units x $10 = 45, D1 200; DS1 (dropship) and the
    # working cut CUT1 are excluded
    assert round(s["excess_value"], 2) == 245.0
    # understock: A1 60 units x $10
    assert round(s["understock_value"], 2) == 600.0
    classes = {c["class"]: c for c in s["by_class"]}
    assert classes["A"]["understock_value"] == 600
    assert classes["D"]["current_value"] == 200
    assert classes["A"]["days_cover"] == 365.0 * 400 / 7300


def test_goal_covers_reorder_level_once_target_present():
    df = _frame()
    df["target_stock"] = [150, 0.3, 0, 0, 0]
    df = sg.compute_stock_goal(df, zero_goal_skus={"DS1"})
    g = df.set_index("SKU")
    assert g.loc["A1", "goal_units"] == 150
    s = sg.stock_health_summary(df)
    assert s["reorder_level_value"] == 150 * 10 + 0.3 * 10
