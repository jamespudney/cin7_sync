from __future__ import annotations

import os
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch

import pandas as pd

import ai_tools
import po_dispatch_reminder
import slack_listener
import so_lookup
import worker_engine
from app_config import (
    PAGE_CAPTIONS,
    PAGE_DESCRIPTIONS,
    PAGE_GROUP_BY_NAME,
    PAGE_GROUPS,
    PAGE_OPTIONS,
    _app_version_label,
    _build_date,
)
from app_pages.my_profile import (
    SLACK_OAUTH_ENV_VARS,
    missing_slack_oauth_env_vars,
)
from app_pages.ordering_layout import ORDERING_PO_EDITOR_VIEW
from cin7_sync import Cin7Client
from data_catalog import DatasetSpec, catalog_rows, latest_file
from engine.sku_rules import (
    _is_strip_sku,
    _parse_length,
    _parse_strip_base,
    _parse_tube_sku,
    parse_sourcing_rule,
)
from engine.sku_movement_audit import (
    build_sku_current_month_movement,
    build_sku_sales_audit,
    build_strip_movement_audit,
    calendar_month_periods,
)
from engine.reorder_math import (
    bulk_residue_floor_units,
    excess_units_over_target,
    fractional_bulk_order_allowed,
    normalise_planning_quantity,
)
from storage_dimensions import extract_storage_dim


class PageConfigTests(unittest.TestCase):
    def test_ordering_column_preferences_keep_stable_view_key(self) -> None:
        self.assertEqual(ORDERING_PO_EDITOR_VIEW, "ordering_po_editor")

    def test_missing_slack_oauth_env_vars_reports_optional_setup(self) -> None:
        with patch.dict("os.environ", {}, clear=True):
            self.assertEqual(
                missing_slack_oauth_env_vars(),
                list(SLACK_OAUTH_ENV_VARS),
            )

        configured = {name: "set" for name in SLACK_OAUTH_ENV_VARS}
        with patch.dict("os.environ", configured, clear=True):
            self.assertEqual(missing_slack_oauth_env_vars(), [])

    def test_app_version_label_uses_build_metadata(self) -> None:
        with patch.dict(
            "os.environ",
            {
                "APP_BUILD_COMMIT": "84d9db4e6fd7976fda609de0f8b53e31fd5e6def",
                "APP_BUILD_DATE": "2026-06-17",
            },
            clear=True,
        ), patch("app_config._git_output", return_value=""):
            self.assertEqual(_app_version_label(), "build 84d9db4")
            self.assertEqual(_build_date(), "2026-06-17")

    def test_page_metadata_is_consistent(self) -> None:
        self.assertEqual(len(PAGE_OPTIONS), len(PAGE_CAPTIONS))
        self.assertEqual(len(PAGE_OPTIONS), len(set(PAGE_OPTIONS)))

        grouped_pages = [
            page
            for pages in PAGE_GROUPS.values()
            for page in pages
        ]
        self.assertEqual(PAGE_OPTIONS, grouped_pages)

        for page in PAGE_OPTIONS:
            self.assertIn(page, PAGE_DESCRIPTIONS)
            self.assertIn(page, PAGE_GROUP_BY_NAME)
            self.assertTrue(PAGE_DESCRIPTIONS[page])


class AppMemoryStructureTests(unittest.TestCase):
    def test_dashboard_avoids_duplicate_sale_line_loads(self) -> None:
        script = (
            Path(__file__).resolve().parents[1] / "app.py"
        ).read_text(encoding="utf-8")

        self.assertNotIn('sale_lines_3d = load("sale_lines_last_3d")',
                         script)
        self.assertNotIn('sale_lines_30d = load("sale_lines_last_30d")',
                         script)
        self.assertNotIn('purchase_lines = load("purchase_lines_last_90d")',
                         script)
        self.assertIn("sale_lines_3d = pd.DataFrame()", script)
        self.assertIn("sale_lines_30d = pd.DataFrame()", script)

    def test_big_dashboard_loaders_use_lean_csv_columns(self) -> None:
        script = (
            Path(__file__).resolve().parents[1] / "app.py"
        ).read_text(encoding="utf-8")

        self.assertIn("def _read_csv_lean", script)
        self.assertIn("_SALE_LINES_USECOLS", script)
        self.assertIn("_SALES_HEADERS_USECOLS", script)
        self.assertIn("_PURCHASE_LINES_USECOLS", script)
        self.assertIn("_read_csv_lean(base_file, _SALE_LINES_USECOLS",
                      script)
        self.assertIn("_read_csv_lean(base_file, _SALES_HEADERS_USECOLS",
                      script)
        self.assertIn("_read_csv_lean(base_file, _PURCHASE_LINES_USECOLS",
                      script)

    def test_ordering_reorder_trace_is_built_lazily(self) -> None:
        script = (
            Path(__file__).resolve().parents[1] / "app.py"
        ).read_text(encoding="utf-8")

        reorder_cols_start = script.index("_reorder_cols = (")
        reorder_cols_end = script.index(")", reorder_cols_start)
        reorder_cols_block = script[reorder_cols_start:reorder_cols_end]

        self.assertNotIn("calc_trace", reorder_cols_block)
        self.assertIn("include_trace: bool = False", script)
        self.assertIn('engine_df = engine_df.drop(columns=["calc_trace"])',
                      script)
        self.assertIn("include_trace=True).get(\"calc_trace\")", script)

    def test_abc_engine_does_not_foreground_rebuild_by_default(self) -> None:
        script = (
            Path(__file__).resolve().parents[1] / "app.py"
        ).read_text(encoding="utf-8")

        accessor_start = script.index("def _get_engine_df_cached")
        accessor_end = script.index("\ndef _get_engine_df()", accessor_start)
        accessor_block = script[accessor_start:accessor_end]

        self.assertIn(
            '_start_background_engine_refresh("engine snapshot requested '
            'but missing")',
            accessor_block,
        )
        self.assertIn(
            'os.environ.get("ABC_ALLOW_FOREGROUND_COMPUTE") != "1"',
            accessor_block,
        )
        self.assertIn("return pd.DataFrame()", accessor_block)
        self.assertIn("_abc_engine(", accessor_block)

    def test_ordering_editor_has_focus_scroll_enhancer(self) -> None:
        script = (
            Path(__file__).resolve().parents[1] / "app.py"
        ).read_text(encoding="utf-8")

        self.assertIn("def _render_ordering_editor_enhancer", script)
        self.assertIn("w4s-ordering-active-row", script)
        self.assertIn("function hideGuide()", script)
        self.assertIn("ENHANCER_VERSION = \"persistent-row-pan-v2\"",
                      script)
        self.assertIn("const host = frame;", script)
        self.assertIn("function handlePointerMove", script)
        self.assertIn("\"ArrowUp\", \"ArrowDown\", \"PageUp\", \"PageDown\", \"Escape\"",
                      script)
        self.assertIn("w4s-ordering-dragging", script)
        self.assertIn("w4s-ordering-editor-", script)
        self.assertIn("_render_ordering_editor_enhancer(_ordering_grid_anchor)",
                      script)

    def test_ordering_recent_demand_anchors_to_snapshot_date(self) -> None:
        script = (
            Path(__file__).resolve().parents[1] / "app.py"
        ).read_text(encoding="utf-8")

        self.assertIn("def _analysis_today_from_dates", script)
        self.assertIn(
            "today_ts = _analysis_today_from_dates(*_analysis_date_sources)",
            script,
        )
        self.assertIn("cutoff_recent = today_ts - pd.Timedelta(days=45)",
                      script)
        self.assertIn("if _d >= cutoff:", script)
        self.assertNotIn(
            "cutoff = pd.Timestamp(datetime.now().date()) - "
            "pd.Timedelta(days=window_days)",
            script,
        )
        self.assertNotIn(
            "today_ts = pd.Timestamp(datetime.now().date())\n"
            "    cutoff_recent",
            script,
        )

    def test_ordering_sku_detail_surfaces_current_month_audit(self) -> None:
        script = (
            Path(__file__).resolve().parents[1] / "app.py"
        ).read_text(encoding="utf-8")

        self.assertIn("def _render_ordering_engine_input_freshness", script)
        self.assertIn("Engine inputs ·", script)
        self.assertIn("Sale lines 30d", script)
        self.assertIn("FG assemblies 30d", script)
        self.assertIn("Ordering, slow-stock, and AI demand answers", script)
        self.assertIn("_d2[0].metric(", script)
        self.assertIn('"Current month"', script)
        self.assertIn("_current_month_live_units", script)
        self.assertIn('_d2[1].metric("90d units"', script)
        self.assertIn('_d2[2].metric("Customers 45d"', script)
        self.assertIn('_d2[3].metric("Momentum"', script)
        self.assertIn(
            "Exact synced sale lines show",
            script,
        )
        self.assertIn("snapshot is likely stale or missing recent", script)
        self.assertIn("sale-line files; run/await", script)

    def test_slow_stock_overview_explains_value_changes(self) -> None:
        script = (
            Path(__file__).resolve().parents[1] / "app.py"
        ).read_text(encoding="utf-8")

        self.assertIn('"detail_df": detail_df', script)
        self.assertIn("Why did slow-stock value move?", script)
        self.assertIn("Top 20 slow-stock value", script)
        self.assertIn("Flagged by latest run", script)
        self.assertIn("Largest SKUs touched by the latest run", script)

    def test_assembly_sync_is_part_of_daily_freshness(self) -> None:
        root = Path(__file__).resolve().parents[1]
        daily = (root / "daily_sync.sh").read_text(encoding="utf-8")
        loop = (root / "sync_loop.sh").read_text(encoding="utf-8")
        cin7_sync = (root / "cin7_sync.py").read_text(encoding="utf-8")
        catalog = (root / "data_catalog.py").read_text(encoding="utf-8")
        housekeeping = (root / "housekeeping_audit.py").read_text(
            encoding="utf-8"
        )

        self.assertIn("cin7_sync assemblies --days 30", daily)
        self.assertIn("CIN7_QUICK_SKIP_ASSEMBLIES=1", daily)
        self.assertIn("CRITICAL_SYNC_FAILURE", daily)
        self.assertIn("CIN7_QUICK_SKIP_ASSEMBLIES", cin7_sync)
        self.assertIn("assemblies_last_30d_*.csv", loop)
        self.assertIn("_engine_inputs_ready", loop)
        self.assertIn("Assemblies (30d)", catalog)
        self.assertIn("assemblies_last_30d", catalog)
        self.assertIn("CIN7 assemblies 30d window", housekeeping)
        self.assertIn("assemblies_last_30d_*.csv", housekeeping)

    def test_sku_detail_uses_live_current_month_movement(self) -> None:
        script = (
            Path(__file__).resolve().parents[1] / "app.py"
        ).read_text(encoding="utf-8")

        self.assertIn("build_sku_current_month_movement", script)
        self.assertIn("_current_month_engine_units", script)
        self.assertIn("_cached_live_product_movements_for_sku", script)
        self.assertIn("_current_month_product_units", script)
        self.assertIn("Live CIN7 product Movements show", script)
        self.assertIn("FG assembly consumption", script)
        self.assertIn("then to CIN7 product Movements", script)
        self.assertIn("ai_tools.set_assemblies(assemblies)", script)

    def test_demand_breakdown_uses_finished_goods_consumption(self) -> None:
        script = (
            Path(__file__).resolve().parents[1] / "app.py"
        ).read_text(encoding="utf-8")

        self.assertIn("assemblies_df: Optional[pd.DataFrame] = None",
                      script)
        self.assertGreaterEqual(script.count("assemblies_df=assemblies"), 2)
        self.assertIn("FG assembly consumption found", script)
        self.assertIn("Kit-sale BOM estimate", script)
        self.assertIn("Recent FG assembly consumption", script)
        self.assertIn("NON_MOVEMENT_COMPONENT_SALE_STATUSES", script)
        self.assertIn("Suppressed", script)
        self.assertIn("assembly rows are the actual component movement",
                      script)

    def test_assembly_sync_filters_by_detail_completion_date(self) -> None:
        script = (
            Path(__file__).resolve().parents[1] / "cin7_sync.py"
        ).read_text(encoding="utf-8")

        self.assertIn("CIN7_ASSEMBLY_LIST_BUFFER_DAYS", script)
        self.assertIn("candidate_cutoff", script)
        self.assertIn("completion_dt is not None and completion_dt < cutoff",
                      script)
        self.assertIn("assemblies_{days}d_v2_completion", script)

    def test_product_movement_audit_supports_batch_engine_scan(self) -> None:
        script = (
            Path(__file__).resolve().parents[1]
            / "audit_live_cin7_demand.py"
        ).read_text(encoding="utf-8")

        self.assertIn("--batch-engine", script)
        self.assertIn("--all-engine", script)
        self.assertIn("--assembly-heavy", script)
        self.assertIn("_engine_row_assembly_score", script)
        self.assertIn("assembly_units_45d", script)
        self.assertIn("cin7_product_movement_audit_", script)
        self.assertIn("Delta live minus local", script)
        self.assertIn("Live CIN7 Movement demand MTD", script)
        self.assertIn("Live FG/Assembly demand", script)
        self.assertIn("def _read_app_window_csv", script)
        self.assertIn("1d/3d nearsync files", script)

    def test_warm_engine_reuses_app_sale_line_union(self) -> None:
        helper_script = (
            Path(__file__).resolve().parents[1] / "warm_engine_helpers.py"
        ).read_text(encoding="utf-8")

        self.assertIn("def _dataframes_from_app", helper_script)
        self.assertIn('"sale_lines": app_module.sale_lines', helper_script)
        self.assertIn("assemblies_df=assemblies", helper_script)
        self.assertIn(
            "keeps engine_output.csv aligned with the grid's",
            helper_script,
        )
        self.assertNotIn("pd.read_csv(sale_lines_csv", helper_script)

    def test_cashflow_actual_revenue_matches_cin7_basis(self) -> None:
        script = (
            Path(__file__).resolve().parents[1] / "app.py"
        ).read_text(encoding="utf-8")

        self.assertIn("def _sales_actuals_frame", script)
        self.assertIn("Cashflow actual sales are grouped by InvoiceDate",
                      script)
        self.assertIn("InvoiceAmount minus tax", script)
        self.assertIn("General Dashboard Revenue tile", script)
        self.assertIn("Actual revenue (CIN7)", script)
        self.assertIn("Difference (actual - forecast)", script)
        self.assertIn("Revenue last week (Mon-Sun)", script)
        self.assertIn("auto:actual_sales", script)
        self.assertIn("_wk_actual = dict(_cf_actual_sales_by_week)",
                      script)

    def test_cashflow_payables_use_qbo_open_balance(self) -> None:
        root = Path(__file__).resolve().parents[1]
        app_script = (root / "app.py").read_text(encoding="utf-8")
        db_script = (root / "db.py").read_text(encoding="utf-8")
        sync_script = (
            root / "cashflow_sync.py").read_text(encoding="utf-8")

        self.assertIn("def _cf_payable_is_open", app_script)
        self.assertIn("qbo_balance", app_script)
        self.assertIn("if not _cf_payable_is_open", app_script)
        self.assertIn("def mark_qbo_payables_closed_except", db_script)
        self.assertIn("status = 'paid'", db_script)
        self.assertIn("qbo_balance = 0", db_script)
        self.assertIn("only_unpaid=True", sync_script)
        self.assertIn("mark_qbo_payables_closed_except", sync_script)

    def test_qbo_cashflow_sync_runs_from_nearsync_task(self) -> None:
        root = Path(__file__).resolve().parents[1]
        start_script = (root / "start.sh").read_text(encoding="utf-8")
        render_config = (root / "render.yaml").read_text(encoding="utf-8")
        nearsync_loop = (
            root / "nearsync_loop.sh").read_text(encoding="utf-8")
        sync_script = (
            root / "cashflow_sync.py").read_text(encoding="utf-8")

        self.assertNotIn("_supervise qbo_cashflow", start_script)
        self.assertNotIn("QBO_CASHFLOW_PID", start_script)
        self.assertIn("QBO_CASHFLOW_INTERVAL_HOURS", render_config)
        self.assertIn("QBO_CASHFLOW_BOOT_DELAY_MIN", render_config)
        self.assertIn("QBO_CASHFLOW_MONTHS_BACK", render_config)
        self.assertIn("python cashflow_sync.py sync", nearsync_loop)
        self.assertIn("--months-back", nearsync_loop)
        self.assertIn("QBO_CASHFLOW_BOOT_DELAY_MIN", nearsync_loop)
        self.assertIn(".qbo_cashflow_sync.lock", nearsync_loop)
        self.assertIn("timeout 300", nearsync_loop)
        self.assertIn("qbo_client.is_ready()", sync_script)
        self.assertIn("def cmd_sync", sync_script)

    def test_sync_catchup_checks_sale_line_window(self) -> None:
        script = (
            Path(__file__).resolve().parents[1] / "sync_loop.sh"
        ).read_text(encoding="utf-8")

        self.assertIn("_check_daily_output_fresh", script)
        self.assertIn("sales_last_30d_*.csv", script)
        self.assertIn("sale_lines_last_30d_*.csv", script)
        self.assertIn("sale_lines_last_30d CSV", script)

    def test_signin_combines_password_and_profile(self) -> None:
        script = (
            Path(__file__).resolve().parents[1] / "app.py"
        ).read_text(encoding="utf-8")

        self.assertIn("Password gate + profile sign-in", script)
        self.assertIn("def _complete_user_signin", script)
        self.assertIn("def _restore_user_session_from_url", script)
        self.assertIn("st.session_state[\"_app_authed\"] = True", script)
        self.assertIn("st.query_params[\"sid\"] = tok", script)
        self.assertIn("Sign in once with the team password and your staff profile",
                      script)
        self.assertIn("st.selectbox(\n                \"Your name\"", script)
        self.assertNotIn("Enter the team password to continue", script)

    def test_warm_engine_runs_detached_with_memory_guard(self) -> None:
        root = Path(__file__).resolve().parents[1]
        sync_loop = (root / "sync_loop.sh").read_text(encoding="utf-8")
        warm_engine = (root / "warm_engine.py").read_text(encoding="utf-8")
        render_config = (root / "render.yaml").read_text(encoding="utf-8")

        self.assertIn("_start_warm_engine", sync_loop)
        self.assertIn("WARM_ENGINE_BOOT_DELAY_MIN", sync_loop)
        self.assertIn("ENGINE_REFRESH_LOCK_PATH", sync_loop)
        self.assertIn("timeout \"$WARM_ENGINE_TIMEOUT_SECONDS\"",
                      sync_loop)
        self.assertIn("engine_refresh_status.json", sync_loop)
        self.assertNotIn("python warm_engine.py 2>&1 | tee -a \"$LOG\"",
                         sync_loop)
        self.assertIn("WARM_ENGINE_MIN_AVAILABLE_MB", warm_engine)
        self.assertIn("MemAvailable:", warm_engine)
        self.assertIn("skipping cache warm", warm_engine)
        self.assertIn("WARM_ENGINE_MIN_AVAILABLE_MB", render_config)

    def test_app_deploys_are_staged_not_auto_deployed(self) -> None:
        render_config = (
            Path(__file__).resolve().parents[1] / "render.yaml"
        ).read_text(encoding="utf-8")

        self.assertIn("autoDeploy: false", render_config)
        self.assertIn("deploy manually/off-hours", render_config)

    def test_mtd_yoy_table_uses_shared_revenue_helper(self) -> None:
        script = (
            Path(__file__).resolve().parents[1] / "app.py"
        ).read_text(encoding="utf-8")

        self.assertIn("_mtd_rev_src, _mtd_rev_col", script)
        self.assertIn("hdf[\"__rev\"] = hdf[\"__sales_amount\"]",
                      script)
        self.assertIn("def _period_order_count", script)
        self.assertIn("coverage = header_orders / max(line_orders, 1)",
                      script)
        self.assertIn("return line_rev", script)
        self.assertIn("\"Revenue\": _rev_for_dates(", script)
        self.assertNotIn("\"Revenue\": float(chunk[\"Total\"].sum())",
                         script)


class ReorderMathTests(unittest.TestCase):
    def test_bulk_roll_residue_is_ignored_for_planning(self) -> None:
        # LED-KP24-6000K-IP20-100M showed ~0.0025 of a 100m roll
        # left: visually 0 rolls, but enough for the old status rule to
        # say "Overstocked" when target was 0.
        self.assertEqual(
            bulk_residue_floor_units(True, 100),
            0.05,
        )
        self.assertEqual(
            normalise_planning_quantity(
                0.0025, is_bulk_master=True, bulk_length_m=100),
            0.0,
        )
        self.assertEqual(
            excess_units_over_target(
                0.0025, 0, is_bulk_master=True, bulk_length_m=100),
            0.0,
        )

    def test_meaningful_bulk_stock_still_counts(self) -> None:
        self.assertEqual(
            normalise_planning_quantity(
                0.08, is_bulk_master=True, bulk_length_m=100),
            0.08,
        )
        self.assertEqual(
            excess_units_over_target(
                0.08, 0, is_bulk_master=True, bulk_length_m=100),
            0.08,
        )

    def test_neonica_100m_bulk_rolls_allow_decimal_order_qtys(self) -> None:
        self.assertTrue(
            fractional_bulk_order_allowed(
                "Neonica Polska Sp. z o.o.",
                True,
                100,
                {"allow_fractional_qty": False},
            )
        )
        self.assertFalse(
            fractional_bulk_order_allowed(
                "Topmet Light (EUR)",
                True,
                100,
                {"allow_fractional_qty": False},
            )
        )
        self.assertFalse(
            fractional_bulk_order_allowed(
                "Neonica Polska Sp. z o.o.",
                False,
                100,
                {"allow_fractional_qty": True},
            )
        )


class StripRollupParsingTests(unittest.TestCase):
    def test_tsb_0305_child_links_to_100m_master_base(self) -> None:
        master = "LED-TSB2835-300-24-6000-100M"
        child = "LED-TSB2835-300-24-6000-0305"

        self.assertTrue(_is_strip_sku(master, ""))
        self.assertTrue(_is_strip_sku(child, ""))
        self.assertEqual(
            _parse_strip_base(master),
            ("LED-TSB2835-300-24-6000", 100.0),
        )
        self.assertEqual(
            _parse_strip_base(child),
            ("LED-TSB2835-300-24-6000", 0.305),
        )

    def test_strip_movement_audit_rolls_child_sales_to_100m_master(self) -> None:
        products = pd.DataFrame([
            {
                "SKU": "LEDIRISRGBCW-11.8-IP20-100M",
                "Name": "RGB CW Iris 100m",
            },
            {
                "SKU": "LEDIRISRGBCW-11.8-IP20-0305",
                "Name": "RGB CW Iris 305mm cut",
            },
            {
                "SKU": "LEDIRISRGBCW-11.8-IP20-5M",
                "Name": "RGB CW Iris 5m",
            },
        ])
        sale_lines = pd.DataFrame([
            {
                "SKU": "LEDIRISRGBCW-11.8-IP20-0305",
                "InvoiceDate": "2026-06-01",
                "Quantity": 10,
                "Customer": "Customer A",
                "CustomerID": "A",
                "Status": "AUTHORISED",
            },
            {
                "SKU": "LEDIRISRGBCW-11.8-IP20-5M",
                "InvoiceDate": "2026-05-01",
                "Quantity": 2,
                "Customer": "Customer B",
                "CustomerID": "B",
                "Status": "AUTHORISED",
            },
            {
                "SKU": "LEDIRISRGBCW-11.8-IP20-100M",
                "InvoiceDate": "2026-04-01",
                "Quantity": 0.4,
                "Customer": "Customer A",
                "CustomerID": "A",
                "Status": "AUTHORISED",
            },
        ])

        audit = build_strip_movement_audit(
            "LEDIRISRGBCW-11.8-IP20-100M",
            products,
            sale_lines,
            today=pd.Timestamp("2026-06-18"),
        )

        self.assertTrue(audit["ok"])
        self.assertEqual(audit["base"], "LEDIRISRGBCW-11.8-IP20")
        self.assertAlmostEqual(
            audit["summary"]["direct_master_rolls_12mo"],
            0.4,
        )
        # 10 x 0.305m + 2 x 5m = 13.05m = 0.1305 of a 100m roll.
        self.assertAlmostEqual(
            audit["summary"]["child_master_rolls_12mo"],
            0.1305,
        )
        self.assertAlmostEqual(
            audit["summary"]["total_master_rolls_12mo"],
            0.5305,
        )

    def test_sku_sales_audit_separates_invoice_month_from_order_month(self) -> None:
        sale_lines = pd.DataFrame([
            {
                "SKU": "LED-V3000938S-20",
                "InvoiceDate": "2026-05-31",
                "OrderDate": "2026-06-02",
                "Quantity": 7,
                "Customer": "Customer A",
                "Status": "AUTHORISED",
            },
            {
                "SKU": "LED-V3000938S-20",
                "InvoiceDate": None,
                "OrderDate": "2026-06-10",
                "Quantity": 3,
                "Customer": "Customer B",
                "Status": "AUTHORISED",
            },
            {
                "SKU": "LED-V3000938S-20",
                "InvoiceDate": "2026-06-12",
                "OrderDate": "2026-06-11",
                "Quantity": 2,
                "Customer": "Customer C",
                "Status": "CREDITED",
            },
        ])

        audit = build_sku_sales_audit(
            "LED-V3000938S-20",
            sale_lines,
            today=pd.Timestamp("2026-06-18"),
            months=2,
        )

        self.assertTrue(audit["ok"])
        self.assertEqual(audit["summary"]["current_month"], "2026-06")
        # Credited invoice is excluded; open/current OrderDate lines are
        # visible but not counted by the engine's InvoiceDate bucket.
        self.assertEqual(audit["summary"]["current_invoice_qty"], 0)
        self.assertEqual(audit["summary"]["current_order_qty"], 10)
        self.assertEqual(
            audit["summary"]["current_order_not_in_invoice_month_qty"],
            10,
        )

    def test_current_month_movement_includes_assembly_consumption(self) -> None:
        sale_lines = pd.DataFrame([
            {
                "SKU": "LED-NEON-FLEX-NICHO-3000K-2",
                "InvoiceDate": "2026-06-12",
                "Quantity": 5,
                "Status": "AUTHORISED",
            },
            {
                "SKU": "LED-NEON-FLEX-NICHO-3000K-2",
                "InvoiceDate": "2026-06-14",
                "Quantity": 2,
                "Status": "CREDITED",
            },
            {
                "SKU": "LED-NEON-FLEX-NICHO-3000K-2",
                "InvoiceDate": "2026-05-30",
                "Quantity": 7,
                "Status": "AUTHORISED",
            },
        ])
        assemblies = pd.DataFrame([
            {
                "ComponentSKU": "LED-NEON-FLEX-NICHO-3000K-2",
                "CompletionDate": "2026-06-11",
                "Date": "2026-06-10",
                "Quantity": 30,
                "Status": "COMPLETED",
            },
            {
                "ComponentSKU": "LED-NEON-FLEX-NICHO-3000K-2",
                "CompletionDate": "2026-06-15",
                "Date": "2026-06-15",
                "Quantity": 4,
                "Status": "DRAFT",
            },
            {
                "ComponentSKU": "LED-OTHER",
                "CompletionDate": "2026-06-15",
                "Date": "2026-06-15",
                "Quantity": 99,
                "Status": "COMPLETED",
            },
        ])

        movement = build_sku_current_month_movement(
            "LED-NEON-FLEX-NICHO-3000K-2",
            sale_lines,
            assemblies,
            today=pd.Timestamp("2026-06-23"),
        )

        self.assertTrue(movement["ok"])
        self.assertEqual(movement["period"], "2026-06")
        self.assertEqual(movement["direct_invoice_qty"], 5)
        self.assertEqual(movement["assembly_qty"], 30)
        self.assertEqual(movement["total_qty"], 35)

    def test_current_month_movement_suppresses_nonfinal_component_sale_lines(
            self) -> None:
        sku = "LED-AB-SL-M3"
        sale_lines = pd.DataFrame([
            {
                "SKU": sku,
                "InvoiceDate": "2026-06-19",
                "OrderDate": "2026-06-19",
                "Quantity": 33,
                "Total": 100,
                "Status": "PICKING",
                "SaleID": "SO-57961",
            },
        ])
        assemblies = pd.DataFrame([
            {
                "ComponentSKU": sku,
                "CompletionDate": "2026-06-19",
                "Date": "2026-06-19",
                "Quantity": 31,
                "Status": "COMPLETED",
                "TaskID": "FG-49275",
            },
            {
                "ComponentSKU": sku,
                "CompletionDate": "2026-06-18",
                "Date": "2026-06-18",
                "Quantity": 4,
                "Status": "COMPLETED",
                "TaskID": "FG-49231",
            },
        ])

        movement = build_sku_current_month_movement(
            sku,
            sale_lines,
            assemblies,
            today=pd.Timestamp("2026-06-24"),
        )

        self.assertEqual(movement["direct_invoice_qty"], 0)
        self.assertEqual(movement["assembly_qty"], 35)
        self.assertEqual(movement["total_qty"], 35)
        self.assertEqual(movement["ignored_nonmovement_direct_qty"], 33)

    def test_ai_velocity_reports_current_month_assembly_movement(self) -> None:
        today = pd.Timestamp(datetime.now().date())
        sku = "LED-NEON-FLEX-NICHO-3000K-2"
        sale_lines = pd.DataFrame([
            {
                "SKU": sku,
                "InvoiceDate": today.strftime("%Y-%m-%d"),
                "Quantity": 5,
                "Total": 100,
                "Status": "AUTHORISED",
                "SaleID": "S1",
            }
        ])
        assemblies = pd.DataFrame([
            {
                "ComponentSKU": sku,
                "CompletionDate": today.strftime("%Y-%m-%d"),
                "Date": today.strftime("%Y-%m-%d"),
                "Quantity": 44,
                "Status": "COMPLETED",
            }
        ])

        try:
            ai_tools.set_assemblies(assemblies)
            with patch.object(
                ai_tools,
                "_get_live_product_movements",
                return_value={"ok": False, "reason": "test"},
            ):
                result = ai_tools.get_velocity(
                    pd.DataFrame(),
                    sale_lines,
                    {"sku": sku, "days": 30},
                )
        finally:
            ai_tools.set_assemblies(pd.DataFrame())

        movement = result["current_month_movement"]
        self.assertEqual(movement["direct_invoice_qty"], 5)
        self.assertEqual(movement["assembly_qty"], 44)
        self.assertEqual(movement["total_qty"], 49)
        demand = result["current_month_demand"]
        self.assertEqual(demand["source"], "synced_sale_lines_plus_fg_assemblies")
        self.assertEqual(demand["direct_invoice_qty"], 5)
        self.assertEqual(demand["fg_assembly_qty"], 44)
        self.assertEqual(demand["total_qty"], 49)
        self.assertIn("current_month_demand.total_qty",
                      result["assistant_guidance"])
        self.assertIn("units_sold_note", result)
        self.assertIn("Finished Goods assembly consumption",
                      result["assistant_guidance"])
        self.assertIn("current_month_live_product_movements", result)

    def test_live_product_movements_count_sale_and_assembly_demand(self
                                                                   ) -> None:
        movements = [
            {
                "Type": "Finished Goods",
                "Date": "2026-06-23T00:00:00",
                "Number": "FG-49408",
                "Quantity": -50,
                "Amount": -2451,
                "Location": "Main Warehouse",
            },
            {
                "Type": "Sale",
                "Date": "2026-06-18T00:00:00",
                "Number": "SO-57914",
                "Quantity": -2,
                "Amount": -98.04,
                "Location": "Main Warehouse",
            },
            {
                "Type": "Advanced Purchase",
                "Date": "2026-06-12T00:00:00",
                "Number": "PO-7214",
                "Quantity": 60,
                "Amount": 2941.2,
                "Location": "Main Warehouse",
            },
            {
                "Type": "Finished Goods",
                "Date": "2026-05-31T00:00:00",
                "Number": "FG-OLD",
                "Quantity": -99,
                "Amount": -1,
                "Location": "Main Warehouse",
            },
        ]

        summary = ai_tools._summarise_product_movements(
            movements, period="2026-06")

        self.assertTrue(summary["ok"])
        self.assertEqual(summary["demand_qty"], 52)
        self.assertEqual(summary["outbound_qty_all_types"], 52)
        self.assertEqual(summary["purchase_qty"], 60)
        self.assertEqual(
            summary["by_type"]["Finished Goods"]["outbound_qty"], 50)
        self.assertEqual(summary["by_type"]["Sale"]["outbound_qty"], 2)
        self.assertEqual(
            summary["by_type"]["Advanced Purchase"]["signed_qty"], 60)

    def test_calendar_month_periods_end_on_current_calendar_month(self) -> None:
        self.assertEqual(
            [str(p) for p in calendar_month_periods(
                today=pd.Timestamp("2026-06-18"), periods=3)],
            ["2026-04", "2026-05", "2026-06"],
        )


class DataCatalogTests(unittest.TestCase):
    def test_latest_file_prefers_newest_timestamp_or_stable_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            older = root / "products_2026-01-01.csv"
            newer = root / "products_2026-01-02.csv"
            older.write_text("SKU\nA\n", encoding="utf-8")
            newer.write_text("SKU\nB\n", encoding="utf-8")

            self.assertEqual(latest_file("products", root), newer)

            stable = root / "products.csv"
            stable.write_text("SKU\nC\n", encoding="utf-8")
            self.assertIn(latest_file("products", root), {newer, stable})

    def test_catalog_rows_flags_missing_and_stale(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            stale_file = root / "stock_on_hand_2026-01-01.csv"
            stale_file.write_text("SKU\nA\n", encoding="utf-8")

            spec = DatasetSpec(
                "Stock",
                "stock_on_hand",
                "CIN7",
                expected_cadence_hours=1,
            )
            rows = catalog_rows(
                row_counts={"stock_on_hand": 1},
                specs=(spec,),
                now=datetime.now() + timedelta(hours=3),
                output_dir=root,
            )

            self.assertEqual(rows[0]["Rows"], "1")
            self.assertEqual(rows[0]["Status"], "stale")

            missing = catalog_rows(
                specs=(DatasetSpec("Missing", "missing", "CIN7", 1),),
                output_dir=root,
            )
            self.assertEqual(missing[0]["Status"], "missing")

    def test_ai_data_freshness_reports_stale_and_missing_feeds(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            now = datetime(2026, 6, 16, 12, 0)
            products = root / "products_2026-06-16.csv"
            stock = root / "stock_on_hand_2026-06-16.csv"
            products.write_text("SKU\nA\n", encoding="utf-8")
            stock.write_text("SKU\nA\n", encoding="utf-8")
            os.utime(products, (now.timestamp(), now.timestamp()))
            stale_ts = (now - timedelta(hours=2)).timestamp()
            os.utime(stock, (stale_ts, stale_ts))

            report = ai_tools._data_freshness_report(
                "stock_position", output_dir=root, now=now)

        by_prefix = {
            row["prefix"]: row["status"]
            for row in report["datasets"]
        }
        self.assertEqual(by_prefix["products"], "fresh")
        self.assertEqual(by_prefix["stock_on_hand"], "stale")
        self.assertEqual(by_prefix["purchase_lines_last"], "missing")
        self.assertFalse(report["all_fresh"])

    def test_data_freshness_tool_is_registered(self) -> None:
        schema_names = {schema["name"] for schema in ai_tools.TOOL_SCHEMAS}

        self.assertIn("get_data_freshness", schema_names)
        self.assertIn("get_data_freshness", ai_tools.TOOL_HANDLERS)


class WorkerLoopTests(unittest.TestCase):
    def test_slack_worker_refreshes_product_master_for_storage_dims(self) -> None:
        script = (
            Path(__file__).resolve().parents[1] / "slack_loop.sh"
        ).read_text(encoding="utf-8")

        daily_refresh = script.index("launching daily worker data refresh chain")
        products_refresh = script.index("python cin7_sync.py products",
                                        daily_refresh)
        sales_refresh = script.index("python cin7_sync.py salelines --days 30",
                                    daily_refresh)

        self.assertLess(products_refresh, sales_refresh)
        self.assertIn("NearSync", script)
        self.assertIn("Storage L x W x H In", script)
        self.assertIn("python cin7_sync.py sales --days 365", script)

    def test_worker_uses_widest_sale_line_window_not_lexicographic_latest(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            three_day = root / "sale_lines_last_3d_2026-06-17.csv"
            thirty_day = root / "sale_lines_last_30d_2026-06-17.csv"
            three_day.write_text("OrderNumber\nSO-NEW\n", encoding="utf-8")
            thirty_day.write_text("OrderNumber\nSO-57284\n", encoding="utf-8")

            picked = slack_listener._widest_window_file(
                [str(three_day), str(thirty_day)], "sale_lines_last")

        self.assertEqual(Path(picked).name, thirty_day.name)

    def test_so_lookup_uses_widest_sales_header_window(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            three_day = root / "sales_last_3d_2026-06-17.csv"
            thirty_day = root / "sales_last_30d_2026-06-17.csv"
            three_day.write_text(
                "SaleID,OrderNumber,CustomerReference\n"
                "new-id,SO-58000,#43000\n",
                encoding="utf-8",
            )
            thirty_day.write_text(
                "SaleID,OrderNumber,CustomerReference\n"
                "sale-57284,SO-57284,#42555\n",
                encoding="utf-8",
            )
            os.utime(thirty_day, (1, 1))
            os.utime(three_day, (2, 2))

            old_cache = dict(so_lookup._cache)
            so_lookup._cache.update({
                "by_so": None,
                "by_shop_num": None,
                "loaded_at": 0.0,
            })
            try:
                with patch.object(so_lookup, "OUTPUT_DIR", root):
                    result = so_lookup.lookup_so("SO-57284")
            finally:
                so_lookup._cache.update(old_cache)

        self.assertIsNotNone(result)
        self.assertEqual(result["cin7_id"], "sale-57284")

    def test_purchase_receipts_by_sku_sums_stock_received_lines(self) -> None:
        purchase = {
            "StockReceived": [{
                "Date": "2026-06-17",
                "Lines": [
                    {"SKU": "LED-A", "ReceivedQuantity": 2},
                    {"SKU": "LED-A", "Quantity": 1},
                    {"SKU": "LED-B", "ReceivedQuantity": 4},
                ],
            }],
        }

        receipts = ai_tools._purchase_receipts_by_sku(purchase)

        self.assertEqual(receipts["LED-A"]["quantity"], 3)
        self.assertEqual(receipts["LED-B"]["quantity"], 4)
        self.assertEqual(receipts["LED-A"]["dates"], ["2026-06-17", "2026-06-17"])


class PoDispatchReminderTests(unittest.TestCase):
    def test_sale_sku_state_is_line_level_not_header_level(self) -> None:
        sale = {
            "Order": {"Lines": [
                {"SKU": "LED-MX2-192W-24", "Quantity": 3},
                {"SKU": "LED-STRIP-OWED", "Quantity": 1},
            ]},
            "Invoices": [{
                "Status": "AUTHORISED",
                "Lines": [
                    {"SKU": "LED-MX2-192W-24", "Quantity": 3},
                ],
            }],
        }

        self.assertEqual(
            po_dispatch_reminder._sale_sku_fulfilment_state(
                sale, "LED-MX2-192W-24"),
            "fulfilled",
        )
        self.assertEqual(
            po_dispatch_reminder._sale_sku_fulfilment_state(
                sale, "LED-STRIP-OWED"),
            "pending",
        )

    def test_dispatch_escalation_suppresses_shipped_po_sku(self) -> None:
        class FakeClient:
            def get_sale(self, sale_id):
                self.sale_id = sale_id
                return {
                    "Order": {"Lines": [
                        {"SKU": "LED-MX2-192W-24", "Quantity": 3},
                        {"SKU": "LED-STRIP-OWED", "Quantity": 1},
                    ]},
                    "Invoices": [{
                        "Status": "AUTHORISED",
                        "Lines": [
                            {"SKU": "LED-MX2-192W-24", "Quantity": 3},
                        ],
                    }],
                }

        kept, suppressed = (
            po_dispatch_reminder._filter_line_sos_needing_dispatch(
                line_sos=["SO-57569"],
                sku="LED-MX2-192W-24",
                unshipped_sos={"SO-57569"},
                sale_ids={"SO-57569": "sale-uuid"},
                client=FakeClient(),
                sale_cache={},
                local_invoiced_qtys={},
            ))

        self.assertEqual(kept, [])
        self.assertEqual(suppressed, ["SO-57569"])

    def test_dispatch_escalation_suppresses_shipped_pack_line(self) -> None:
        class FakeClient:
            def get_sale(self, sale_id):
                return {
                    "Order": {"Lines": [
                        {"SKU": "LED-MX2-192W-24", "Quantity": 3},
                        {"SKU": "LED-STRIP-OWED", "Quantity": 1},
                    ]},
                    "Fulfilments": [{
                        "FulFilmentStatus": "PARTIALLY FULFILLED",
                        "Pack": {"Lines": [
                            {
                                "SKU": "LED-MX2-192W-24",
                                "Quantity": 3,
                                "Box": "Box 1",
                            },
                        ]},
                        "Ship": {"Lines": [
                            {"Boxes": "Box 1", "IsShipped": True},
                        ]},
                    }],
                    "Invoices": [],
                }

        kept, suppressed = (
            po_dispatch_reminder._filter_line_sos_needing_dispatch(
                line_sos=["SO-57569"],
                sku="LED-MX2-192W-24",
                unshipped_sos={"SO-57569"},
                sale_ids={"SO-57569": "sale-uuid"},
                client=FakeClient(),
                sale_cache={},
                local_invoiced_qtys={},
            ))

        self.assertEqual(kept, [])
        self.assertEqual(suppressed, ["SO-57569"])

    def test_dispatch_escalation_keeps_other_pending_sku(self) -> None:
        class FakeClient:
            def get_sale(self, sale_id):
                return {
                    "Order": {"Lines": [
                        {"SKU": "LED-MX2-192W-24", "Quantity": 3},
                        {"SKU": "LED-STRIP-OWED", "Quantity": 1},
                    ]},
                    "Invoices": [{
                        "Status": "AUTHORISED",
                        "Lines": [
                            {"SKU": "LED-MX2-192W-24", "Quantity": 3},
                        ],
                    }],
                }

        kept, suppressed = (
            po_dispatch_reminder._filter_line_sos_needing_dispatch(
                line_sos=["SO-57569"],
                sku="LED-STRIP-OWED",
                unshipped_sos={"SO-57569"},
                sale_ids={"SO-57569": "sale-uuid"},
                client=FakeClient(),
                sale_cache={},
                local_invoiced_qtys={},
            ))

        self.assertEqual(kept, ["SO-57569"])
        self.assertEqual(suppressed, [])

    def test_dispatch_escalation_uses_local_invoice_fallback(self) -> None:
        kept, suppressed = (
            po_dispatch_reminder._filter_line_sos_needing_dispatch(
                line_sos=["SO-57569"],
                sku="LED-MX2-192W-24",
                unshipped_sos={"SO-57569"},
                sale_ids={},
                client=None,
                sale_cache={},
                local_invoiced_qtys={
                    ("SO-57569", "LED-MX2-192W-24"): 3,
                },
            ))

        self.assertEqual(kept, [])
        self.assertEqual(suppressed, ["SO-57569"])

    def test_build_so_needs_uses_live_sale_lines_when_csv_misses_so(self) -> None:
        class FakeClient:
            def get_sale(self, sale_id):
                self.sale_id = sale_id
                return {
                    "Order": {"Lines": [
                        {"SKU": "LED-22.109", "Quantity": 7},
                        {"SKU": "LED-OTHER", "Quantity": 1},
                    ]},
                }

        sale_cache = {}
        needs = po_dispatch_reminder._build_so_needs(
            ["SO-55871"],
            {"LED-12.019-5", "LED-22.109"},
            {},
            {"SO-55871": "sale-uuid"},
            FakeClient(),
            sale_cache,
        )

        self.assertEqual(needs, [("SO-55871", ["LED-22.109"])])
        self.assertIn("SO-55871", sale_cache)

    def test_build_so_needs_drops_resolved_so_with_no_po_sku_match(self) -> None:
        class FakeClient:
            def get_sale(self, sale_id):
                return {
                    "Order": {"Lines": [
                        {"SKU": "LED-NOT-ON-PO", "Quantity": 1},
                    ]},
                }

        needs = po_dispatch_reminder._build_so_needs(
            ["SO-56149"],
            {"LED-01.014-2", "LED-13.025"},
            {},
            {"SO-56149": "sale-uuid"},
            FakeClient(),
            {},
        )

        self.assertEqual(needs, [])

    def test_build_so_needs_keeps_unknown_so_as_unconfirmed(self) -> None:
        needs = po_dispatch_reminder._build_so_needs(
            ["SO-55871"],
            {"LED-12.019-5"},
            {},
            {},
            None,
            {},
        )

        self.assertEqual(needs, [("SO-55871", None)])

    def test_po_dispatch_message_uses_received_wording(self) -> None:
        msg = po_dispatch_reminder._compose_reminder(
            "PO-7114",
            "Luz Negra (EUR)",
            "FULLY RECEIVED",
            "2026-06-19",
            [("LED-22.109", 7)],
            [("SO-55871", ["LED-22.109"])],
        )

        self.assertIn("SO-55871 — needs `LED-22.109`", msg)
        self.assertIn("now that this PO has arrived", msg)
        self.assertNotIn("when this PO arrives", msg)


class Cin7ClientTests(unittest.TestCase):
    def test_purchase_uuid_uses_advanced_purchase_endpoint_first(self) -> None:
        client = Cin7Client("acct", "key")
        calls = []

        def fake_get(path, params=None):
            calls.append((path, params))
            if path == "advanced-purchase":
                return {
                    "ID": params["ID"],
                    "OrderNumber": "PO-7303",
                    "Order": {"Lines": [{"SKU": "LED-TEST"}]},
                }
            raise AssertionError(f"unexpected endpoint {path}")

        client.get = fake_get
        result = client.get_purchase("ac1e4559-c63a-4028-abec-5a4ad00d22fb")

        self.assertEqual(calls[0][0], "advanced-purchase")
        self.assertEqual(result["_cin7_detail_endpoint"], "advanced-purchase")
        self.assertEqual(result["Order"]["Lines"][0]["SKU"], "LED-TEST")

    def test_purchase_number_uses_advanced_detail_after_list_match(self) -> None:
        client = Cin7Client("acct", "key")
        calls = []

        def fake_get(path, params=None):
            calls.append((path, params))
            if path == "purchaseList":
                return {
                    "PurchaseList": [{
                        "ID": "ac1e4559-c63a-4028-abec-5a4ad00d22fb",
                        "OrderNumber": "PO-7303",
                        "Type": "Advanced Purchase",
                    }],
                }
            if path == "advanced-purchase":
                return {
                    "ID": params["ID"],
                    "OrderNumber": "PO-7303",
                    "Order": {"Lines": []},
                }
            raise AssertionError(f"unexpected endpoint {path}")

        client.get = fake_get
        result = client.get_purchase("PO-7303")

        self.assertEqual([call[0] for call in calls],
                         ["purchaseList", "advanced-purchase"])
        self.assertEqual(result["_cin7_detail_endpoint"], "advanced-purchase")


class IncomingStockTests(unittest.TestCase):
    def tearDown(self) -> None:
        ai_tools.set_purchase_lines(pd.DataFrame())
        ai_tools.set_products(pd.DataFrame())

    def test_purchase_order_lines_include_stock_locator_from_engine(self) -> None:
        sku = "LED-22.109"
        engine_df = pd.DataFrame([{
            "SKU": sku,
            "Name": "Wall Support Bracket",
            "StockLocator": "D29B",
            "Location": "Main Warehouse",
            "storage_dim": '___ x 1.669" x 1.457"',
        }])
        purchase_lines = pd.DataFrame([{
            "PurchaseID": "7114",
            "OrderNumber": "PO-7114",
            "RequiredBy": "2026-06-22",
            "Status": "INVOICED",
            "Supplier": "Luz Negra (EUR)",
            "SKU": sku,
            "Name": "Wall Support Bracket for Comenza Profile",
            "Quantity": 7,
        }])

        ai_tools.set_purchase_lines(purchase_lines)
        result = ai_tools.get_purchase_order(
            engine_df, pd.DataFrame(), {"po_number": "PO-7114"})

        line = result["purchase_orders"][0]["lines"][0]
        self.assertEqual(line["stock_locator"], "D29B")

    def test_purchase_order_stock_locator_falls_back_to_product_master(self) -> None:
        sku = "LED-12.019-5"
        purchase_lines = pd.DataFrame([{
            "PurchaseID": "7114",
            "OrderNumber": "PO-7114",
            "Status": "INVOICED",
            "Supplier": "Luz Negra (EUR)",
            "SKU": sku,
            "Name": "Round Stainless Steel LED Handrail Profile",
            "Quantity": 3,
        }])
        products = pd.DataFrame([{
            "SKU": sku,
            "Name": "Round Stainless Steel LED Handrail Profile",
            "StockLocator": "A12C",
            "Location": "Main Warehouse",
        }])

        ai_tools.set_purchase_lines(purchase_lines)
        ai_tools.set_products(products)
        result = ai_tools.get_purchase_order(
            pd.DataFrame(), pd.DataFrame(), {"po_number": "PO-7114"})

        line = result["purchase_orders"][0]["lines"][0]
        self.assertEqual(line["stock_locator"], "A12C")

    def test_purchase_order_stock_locator_ignores_default_location(self) -> None:
        sku = "LED-22.109"
        engine_df = pd.DataFrame([{
            "SKU": sku,
            "Location": "Main Warehouse",
        }])
        purchase_lines = pd.DataFrame([{
            "PurchaseID": "7114",
            "OrderNumber": "PO-7114",
            "Status": "INVOICED",
            "Supplier": "Luz Negra (EUR)",
            "SKU": sku,
            "Name": "Wall Support Bracket",
            "Quantity": 7,
        }])

        ai_tools.set_purchase_lines(purchase_lines)
        result = ai_tools.get_purchase_order(
            engine_df, pd.DataFrame(), {"po_number": "PO-7114"})

        line = result["purchase_orders"][0]["lines"][0]
        self.assertIsNone(line["stock_locator"])

    def test_purchase_live_lines_include_stock_locator_from_engine(self) -> None:
        sku = "LED-22.109"
        engine_df = pd.DataFrame([{
            "SKU": sku,
            "StockLocator": "D29B",
            "Location": "Main Warehouse",
            "storage_dim": '___ x 1.669" x 1.457"',
        }])

        class FakeClient:
            def __init__(self, account_id, app_key):
                self.account_id = account_id
                self.app_key = app_key

            def get_purchase(self, po_ref):
                self.po_ref = po_ref
                return {
                    "ID": "purchase-id",
                    "OrderNumber": "PO-7114",
                    "Supplier": "Luz Negra (EUR)",
                    "Status": "INVOICED",
                    "Order": {
                        "Lines": [{
                            "SKU": sku,
                            "Name": "Wall Support Bracket",
                            "Quantity": 7,
                        }],
                    },
                }

        with patch.dict(
            "os.environ",
            {
                "CIN7_ACCOUNT_ID": "acct",
                "CIN7_APPLICATION_KEY": "key",
            },
        ), patch("cin7_sync.Cin7Client", FakeClient):
            result = ai_tools.get_purchase_live(
                engine_df, pd.DataFrame(), {"po_number": "PO-7114"})

        line = result["lines"][0]
        self.assertEqual(line["stock_locator"], "D29B")

    def test_incoming_stock_excludes_fully_received_po_balance(self) -> None:
        sku = "LED-89030021-2"
        engine_df = pd.DataFrame([{
            "SKU": sku,
            "OnOrder": 160,
            "OnHand": 133.75,
        }])
        purchase_lines = pd.DataFrame([
            {
                "PurchaseID": "6816",
                "OrderNumber": "PO-6816",
                "RequiredBy": "2026-05-03",
                "Status": "INVOICED",
                "Supplier": "Topmet",
                "SKU": sku,
                "Name": "Slim8 Black 2m",
                "Quantity": 120,
                "Price": 6.5,
                "Total": 780,
            },
            {
                "PurchaseID": "6816",
                "OrderNumber": "PO-6816",
                "RequiredBy": "2026-05-03",
                "Status": "INVOICED-Received",
                "ReceivedDate": "2026-05-06",
                "Supplier": "Topmet",
                "SKU": sku,
                "Name": "Slim8 Black 2m",
                "Quantity": 120,
            },
            {
                "PurchaseID": "7071",
                "OrderNumber": "PO-7071",
                "RequiredBy": "2026-06-18",
                "Status": "INVOICED",
                "Supplier": "Topmet",
                "SKU": sku,
                "Name": "Slim8 Black 2m",
                "Quantity": 80,
            },
            {
                "PurchaseID": "7210",
                "OrderNumber": "PO-7210",
                "RequiredBy": "2026-07-16",
                "Status": "PARTIALLY INVOICED",
                "Supplier": "Topmet",
                "SKU": sku,
                "Name": "Slim8 Black 2m",
                "Quantity": 80,
            },
        ])

        ai_tools.set_purchase_lines(purchase_lines)
        result = ai_tools.get_incoming_stock(
            engine_df, pd.DataFrame(), {"sku": sku})

        po_numbers = {line["po_number"] for line in result["lines"]}
        self.assertNotIn("PO-6816", po_numbers)
        self.assertEqual(po_numbers, {"PO-7071", "PO-7210"})
        self.assertEqual(result["matched"], 2)
        self.assertEqual(result["open_po_quantity_total"], 160)
        self.assertEqual(result["cin7_stock_on_order"], 160)
        self.assertIsNone(result["reconciliation_note"])

    def test_incoming_stock_keeps_remaining_partial_receipt_balance(self) -> None:
        sku = "LED-PARTIAL"
        engine_df = pd.DataFrame([{"SKU": sku, "OnOrder": 60}])
        purchase_lines = pd.DataFrame([
            {
                "PurchaseID": "8000",
                "OrderNumber": "PO-8000",
                "RequiredBy": "2026-06-20",
                "Status": "PARTIALLY RECEIVED",
                "Supplier": "Topmet",
                "SKU": sku,
                "Name": "Partial test",
                "Quantity": 100,
                "Total": 1000,
            },
            {
                "PurchaseID": "8000",
                "OrderNumber": "PO-8000",
                "Status": "PARTIALLY RECEIVED-Received",
                "ReceivedDate": "2026-06-12",
                "Supplier": "Topmet",
                "SKU": sku,
                "Name": "Partial test",
                "Quantity": 40,
            },
        ])

        ai_tools.set_purchase_lines(purchase_lines)
        result = ai_tools.get_incoming_stock(
            engine_df, pd.DataFrame(), {"sku": sku})

        self.assertEqual(result["matched"], 1)
        line = result["lines"][0]
        self.assertEqual(line["quantity_on_order"], 60)
        self.assertEqual(line["original_order_quantity"], 100)
        self.assertEqual(line["quantity_received_against_order"], 40)
        self.assertEqual(line["line_total_value"], 600)
        self.assertEqual(result["open_po_quantity_total"], 60)

    def test_incoming_stock_suppresses_oldest_excess_line_by_on_order(self) -> None:
        sku = "LED-89030021-2"
        engine_df = pd.DataFrame([{"SKU": sku, "OnOrder": 160}])
        purchase_lines = pd.DataFrame([
            {
                "PurchaseID": "6816",
                "OrderNumber": "PO-6816",
                "RequiredBy": "2026-05-03",
                "Status": "INVOICED",
                "Supplier": "Topmet",
                "SKU": sku,
                "Name": "Slim8 Black 2m",
                "Quantity": 120,
            },
            {
                "PurchaseID": "7071",
                "OrderNumber": "PO-7071",
                "RequiredBy": "2026-06-18",
                "Status": "INVOICED",
                "Supplier": "Topmet",
                "SKU": sku,
                "Name": "Slim8 Black 2m",
                "Quantity": 80,
            },
            {
                "PurchaseID": "7210",
                "OrderNumber": "PO-7210",
                "RequiredBy": "2026-07-16",
                "Status": "PARTIALLY INVOICED",
                "Supplier": "Topmet",
                "SKU": sku,
                "Name": "Slim8 Black 2m",
                "Quantity": 80,
            },
        ])

        ai_tools.set_purchase_lines(purchase_lines)
        result = ai_tools.get_incoming_stock(
            engine_df, pd.DataFrame(), {"sku": sku})

        po_numbers = {line["po_number"] for line in result["lines"]}
        self.assertEqual(po_numbers, {"PO-7071", "PO-7210"})
        self.assertEqual(result["open_po_quantity_total"], 160)
        self.assertEqual(
            result["stock_on_order_suppressed_lines"][0]["po_number"],
            "PO-6816")
        self.assertIn("suppressed", result["reconciliation_note"])

    def test_stock_position_tool_is_registered_for_assistant(self) -> None:
        schema_names = {schema["name"] for schema in ai_tools.TOOL_SCHEMAS}
        self.assertIn("get_stock_position", schema_names)
        self.assertIn("get_stock_position", ai_tools.TOOL_HANDLERS)

    def test_stock_position_includes_stock_and_incoming_po_summary(self) -> None:
        sku = "LED-89030021-2"
        engine_df = pd.DataFrame([{
            "SKU": sku,
            "Name": "Slim8 Black 2m",
            "OnHand": 133.75,
            "Allocated": 29,
            "Available": 104.75,
            "OnOrder": 160,
            "unfulfilled": 0,
            "StockLocator": "D29B",
            "storage_dim": "2m profile",
            "ABC": "A",
            "trend_flag": "Trend",
            "is_dormant": False,
            "effective_units_12mo": 69,
            "DoC_days": 700,
            "target_stock": 69,
            "reorder_qty": 0,
            "excess_units": 64.75,
            "excess_value": 423,
        }])
        purchase_lines = pd.DataFrame([
            {
                "PurchaseID": "7071",
                "OrderNumber": "PO-7071",
                "RequiredBy": "2026-06-18",
                "Status": "INVOICED",
                "Supplier": "Topmet",
                "SKU": sku,
                "Name": "Slim8 Black 2m",
                "Quantity": 80,
                "Comments": "Sea Freight",
                "ShippingNotes": "ETA CHS 6/11",
            },
            {
                "PurchaseID": "7210",
                "OrderNumber": "PO-7210",
                "RequiredBy": "2026-07-16",
                "Status": "PARTIALLY INVOICED",
                "Supplier": "Topmet",
                "SKU": sku,
                "Name": "Slim8 Black 2m",
                "Quantity": 80,
                "Comments": "Sea Freight",
                "ShippingNotes": "Ship 6/11 ETA CHS 7/9",
            },
        ])

        ai_tools.set_purchase_lines(purchase_lines)
        result = ai_tools.get_stock_position(
            engine_df, pd.DataFrame(), {"sku": sku})

        self.assertEqual(result["matched"], 1)
        self.assertEqual(result["stock"]["on_hand"], 133.75)
        self.assertEqual(result["stock"]["available"], 104.75)
        self.assertEqual(result["stock"]["allocated"], 29)
        self.assertEqual(result["stock"]["on_order"], 160)
        self.assertEqual(result["stock"]["bin"], "D29B")
        self.assertEqual(result["stock"]["storage_dim"], "2m profile")
        self.assertEqual(result["engine_signals"]["ABC"], "A")
        self.assertEqual(result["engine_signals"]["trend_flag"], "Trend")
        self.assertEqual(result["incoming_stock"]["matched"], 2)
        self.assertEqual(
            result["incoming_stock"]["open_po_quantity_total"], 160)
        self.assertEqual(
            {line["po_number"] for line in result["incoming_stock"]["lines"]},
            {"PO-7071", "PO-7210"})
        self.assertEqual(result["data_freshness"]["scope"], "stock_position")
        self.assertIn("OnOrder", result["formatting_guidance"])
        self.assertIn("data-freshness", result["formatting_guidance"])

    def test_stock_position_uses_stock_locator_as_bin_alias(self) -> None:
        sku = "LED-89030021-2"
        engine_df = pd.DataFrame([{
            "SKU": sku,
            "Name": "Slim8 Black 2m",
            "OnHand": 133.75,
            "Allocated": 29,
            "Available": 104.75,
            "OnOrder": 160,
            "StockLocator": "D29B",
            "ABC": "A",
            "trend_flag": "Trend",
            "is_dormant": False,
        }])

        ai_tools.set_purchase_lines(pd.DataFrame())
        result = ai_tools.get_stock_position(
            engine_df, pd.DataFrame(), {"sku": sku})

        self.assertEqual(result["stock"]["bin"], "D29B")

    def test_stock_position_uses_raw_storage_dimension_field(self) -> None:
        sku = "LED-89030021-2"
        engine_df = pd.DataFrame([{
            "SKU": sku,
            "Name": "Slim8 Black 2m",
            "OnHand": 133.75,
            "OnOrder": 160,
            " storage l x w x h in ": '78" x 0.906" x 0.354"',
        }])

        ai_tools.set_purchase_lines(pd.DataFrame())
        result = ai_tools.get_stock_position(
            engine_df, pd.DataFrame(), {"sku": sku})

        self.assertEqual(
            result["stock"]["storage_dim"], '78" x 0.906" x 0.354"')

    def test_stock_position_falls_back_to_product_master_storage_dim(self) -> None:
        sku = "LED-89030021-2"
        engine_df = pd.DataFrame([{
            "SKU": sku,
            "Name": "Slim8 Black 2m",
            "OnHand": 133.75,
            "OnOrder": 160,
            "storage_dim": "",
        }])
        products = pd.DataFrame([{
            "SKU": sku,
            "Name": "Slim8 Black 2m",
            "AdditionalAttribute6": '78" x 0.906" x 0.354"',
        }])

        ai_tools.set_products(products)
        ai_tools.set_purchase_lines(pd.DataFrame())
        result = ai_tools.get_stock_position(
            engine_df, pd.DataFrame(), {"sku": sku})

        self.assertEqual(
            result["stock"]["storage_dim"], '78" x 0.906" x 0.354"')

    def test_product_dimensions_returns_cin7_storage_dimension(self) -> None:
        sku = "LED-89030021-2"
        products = pd.DataFrame([{
            "SKU": sku,
            "Name": "Slim LED Channel ~ Model Slim8 (Black, 2m)",
            "AdditionalAttribute6": '78" x 0.906" x 0.354"',
        }])
        ai_tools.set_products(products)

        with patch("ai_tools.db.search_product_dimensions", return_value=[]):
            result = ai_tools.get_product_dimensions(
                pd.DataFrame(), pd.DataFrame(), {"query": "Slim8 Black 2m"})

        self.assertEqual(
            result["cin7_storage_dimension"]["storage_dim"],
            '78" x 0.906" x 0.354"')
        self.assertFalse(
            result["cin7_storage_dimension"]["storage_dim_missing"])

    def test_product_dimensions_rows_include_cin7_storage_dimension(self) -> None:
        sku = "LED-89030021-2"
        products = pd.DataFrame([{
            "SKU": sku,
            "Name": "Slim LED Channel ~ Model Slim8 (Black, 2m)",
            "AdditionalAttribute6": '78" x 0.906" x 0.354"',
        }])
        ai_tools.set_products(products)
        dimension_rows = [{
            "title": "Slim8 Black 2m",
            "shopify_handle": "slim8-black-2m",
            "family": "Slim8",
            "outer_width_mm": 12.2,
            "outer_height_mm": 7,
            "channel_width_mm": None,
            "channel_depth_mm": None,
            "max_strip_width_mm": None,
            "wing_count": None,
            "wing_width_mm": None,
            "mounting_type": "surface",
            "profile_shape": "channel",
            "extra_notes": "",
        }]

        with patch(
            "ai_tools.db.search_product_dimensions",
            return_value=dimension_rows,
        ):
            result = ai_tools.get_product_dimensions(
                pd.DataFrame(), pd.DataFrame(), {"query": "Slim8 Black 2m"})

        self.assertEqual(
            result["results"][0]["cin7_storage_dimension_in"],
            '78" x 0.906" x 0.354"')

    def test_stock_position_skips_blank_bin_for_stock_locator(self) -> None:
        sku = "LED-89030021-2"
        engine_df = pd.DataFrame([{
            "SKU": sku,
            "Name": "Slim8 Black 2m",
            "OnHand": 133.75,
            "Available": 104.75,
            "OnOrder": 160,
            "Bin": "",
            "Location": "Main Warehouse",
            "StockLocator": "D29B",
        }])

        ai_tools.set_purchase_lines(pd.DataFrame())
        result = ai_tools.get_stock_position(
            engine_df, pd.DataFrame(), {"sku": sku})

        self.assertEqual(result["stock"]["bin"], "D29B")

    def test_stock_position_ignores_default_location(self) -> None:
        sku = "LED-89030021-2"
        engine_df = pd.DataFrame([{
            "SKU": sku,
            "Name": "Slim8 Black 2m",
            "OnHand": 133.75,
            "Available": 104.75,
            "Location": "Main Warehouse",
        }])

        ai_tools.set_purchase_lines(pd.DataFrame())
        result = ai_tools.get_stock_position(
            engine_df, pd.DataFrame(), {"sku": sku})

        self.assertIsNone(result["stock"]["bin"])

    def test_worker_engine_normalises_stock_locator_to_bin(self) -> None:
        sku = "LED-89030021-2"
        products = pd.DataFrame([{
            "SKU": sku,
            "Name": "Slim8 Black 2m",
            "AverageCost": 1.0,
        }])
        stock = pd.DataFrame([{
            "SKU": sku,
            "OnHand": 133.75,
            "Bin": "",
            "Location": "Main Warehouse",
            "StockLocator": "D29B",
        }])

        result = worker_engine.compute_engine_signals(
            products, stock, pd.DataFrame())

        self.assertEqual(result.iloc[0]["Bin"], "D29B")

    def test_worker_engine_ignores_default_location(self) -> None:
        sku = "LED-89030021-2"
        products = pd.DataFrame([{
            "SKU": sku,
            "Name": "Slim8 Black 2m",
            "AverageCost": 1.0,
        }])
        stock = pd.DataFrame([{
            "SKU": sku,
            "OnHand": 133.75,
            "Location": "Main Warehouse",
        }])

        result = worker_engine.compute_engine_signals(
            products, stock, pd.DataFrame())

        self.assertEqual(result.iloc[0].get("Bin", ""), "")

    def test_storage_dimension_extracts_named_cin7_field(self) -> None:
        row = {
            "SKU": "LED-NEON-FLEX-SUPER-SLIM-ST",
            " storage l x w x h in ": '78" x 0.906" x 0.354"',
        }

        self.assertEqual(extract_storage_dim(row), '78" x 0.906" x 0.354"')

    def test_storage_dimension_extracts_inches_from_positional_attribute(self) -> None:
        row = {
            "SKU": "LED-89030021-2",
            "AdditionalAttribute6": '78" x 0.906" x 0.354"',
        }

        self.assertEqual(extract_storage_dim(row), '78" x 0.906" x 0.354"')

    def test_worker_engine_promotes_raw_storage_dimension_field(self) -> None:
        sku = "LED-NEON-FLEX-SUPER-SLIM-ST"
        products = pd.DataFrame([{
            "SKU": sku,
            "Name": "Super Slim ST",
            "AverageCost": 1.0,
            " storage l x w x h in ": "10 x 0.5 x 0.5",
        }])
        stock = pd.DataFrame([{
            "SKU": sku,
            "OnHand": 20,
        }])

        result = worker_engine.compute_engine_signals(
            products, stock, pd.DataFrame())

        self.assertEqual(result.iloc[0]["storage_dim"], "10 x 0.5 x 0.5")


class SkuRuleTests(unittest.TestCase):
    def test_sourcing_rule_parses_purchase_and_assembly_logic(self) -> None:
        purchased = parse_sourcing_rule(
            "Rule: SR100 | Logic: Purchased full length. No BOM | "
            "Auto-Assembly: N/A | Note: stock as master"
        )
        self.assertEqual(purchased["RuleCode"], "SR100")
        self.assertTrue(purchased["IsMaster"])
        self.assertEqual(purchased["AutoAssembly"], "N/A")

        assembled = parse_sourcing_rule(
            "Rule: SR140 | Logic: Assemble from 0.5 x 2m profile + plate | "
            "Auto-Assembly: ON"
        )
        self.assertFalse(assembled["IsMaster"])
        self.assertEqual(assembled["SourceFraction"], 0.5)
        self.assertEqual(assembled["SourceLengthMM"], 2000)
        self.assertTrue(assembled["HasPlate"])

    def test_tube_and_strip_sku_parsers(self) -> None:
        self.assertEqual(_parse_length("0609"), 609)
        self.assertEqual(_parse_length("3"), 3000)

        tube = _parse_tube_sku("LED-SIERRA38-W-MP-3000", "Sierra 38 white")
        self.assertIsNotNone(tube)
        self.assertEqual(tube["Family"], "SIERRA38")
        self.assertTrue(tube["HasMP"])
        self.assertEqual(tube["LengthMM"], 3000)

        self.assertTrue(_is_strip_sku("LEDIRIS-WW-24V-5M", "LED tape"))
        self.assertEqual(_parse_strip_base("LEDIRIS-WW-24V-5M"), ("LEDIRIS-WW-24V", 5.0))


if __name__ == "__main__":
    unittest.main()
