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
from app_pages.coating_work_orders import build_coating_work_orders
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
    is_bulk_strip_roll_length,
    parse_pack_purchase_sku,
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
from sales_exclusions import (
    excluded_sales_customer_mask,
    filter_excluded_sales_customers,
)
from storage_dimensions import extract_storage_dim


class SalesExclusionTests(unittest.TestCase):
    def test_altard_state_customer_is_excluded_across_apostrophes(self) -> None:
        sales = pd.DataFrame([
            {"Customer": "Altar’d State", "Total": 100, "Quantity": 2},
            {"Customer": "Altar'd State", "Total": 50, "Quantity": 1},
            {"Customer": "ALTARD STATE", "Total": 25, "Quantity": 1},
            {"Customer": "Regular LED Customer", "Total": 200, "Quantity": 4},
        ])

        mask = excluded_sales_customer_mask(sales)
        self.assertEqual(mask.tolist(), [True, True, True, False])

        filtered = filter_excluded_sales_customers(sales)
        self.assertEqual(len(filtered), 1)
        self.assertEqual(filtered.iloc[0]["Customer"], "Regular LED Customer")

    def test_sales_audit_uses_excluded_customer_filter(self) -> None:
        sale_lines = pd.DataFrame([
            {
                "SKU": "LED-TEST",
                "InvoiceDate": "2026-06-01",
                "OrderDate": "2026-06-01",
                "Quantity": 10,
                "Customer": "Altar’d State",
                "Status": "AUTHORISED",
            },
            {
                "SKU": "LED-TEST",
                "InvoiceDate": "2026-06-02",
                "OrderDate": "2026-06-02",
                "Quantity": 3,
                "Customer": "Regular LED Customer",
                "Status": "AUTHORISED",
            },
        ])

        audit = build_sku_sales_audit(
            "LED-TEST",
            sale_lines,
            today=pd.Timestamp("2026-06-18"),
            months=1,
        )

        self.assertTrue(audit["ok"])
        self.assertEqual(
            audit["summary"]["current_invoice_qty"],
            3.0,
        )


class DemandRollupTests(unittest.TestCase):
    def test_strip_audit_uses_active_buying_roll_not_discontinued_larger_roll(
            self) -> None:
        products = pd.DataFrame([
            {
                "SKU": "LED-WLWW-30K-IP67-50",
                "Name": "[Discontinued] White Lily LED Strip 3000K 50m",
                "Status": "Discontinued",
            },
            {
                "SKU": "LED-WLWW-30K-IP67-25",
                "Name": "White Lily LED Strip 3000K 25m",
                "Status": "Active",
            },
            {
                "SKU": "LED-WLWW-30K-IP67-5",
                "Name": "White Lily LED Strip 3000K 5m",
                "Status": "Active",
            },
        ])
        sale_lines = pd.DataFrame([
            {
                "SKU": "LED-WLWW-30K-IP67-25",
                "InvoiceDate": "2026-07-01",
                "Quantity": 1,
                "Customer": "Customer A",
                "Status": "AUTHORISED",
            },
            {
                "SKU": "LED-WLWW-30K-IP67-5",
                "InvoiceDate": "2026-07-01",
                "Quantity": 5,
                "Customer": "Customer B",
                "Status": "AUTHORISED",
            },
        ])

        audit = build_strip_movement_audit(
            "LED-WLWW-30K-IP67-25",
            products,
            sale_lines,
            today=pd.Timestamp("2026-07-02"),
        )

        self.assertTrue(audit["ok"])
        self.assertEqual(audit["master_length_m"], 25.0)
        self.assertEqual(
            audit["summary"]["direct_master_rolls_12mo"],
            1.0,
        )
        self.assertEqual(
            audit["summary"]["child_master_rolls_12mo"],
            1.0,
        )
        self.assertEqual(
            audit["summary"]["total_master_rolls_12mo"],
            2.0,
        )
        roles = dict(zip(
            audit["family_rows"]["SKU"],
            audit["family_rows"]["Role"],
        ))
        self.assertEqual(
            roles["LED-WLWW-30K-IP67-50"],
            "retired family master",
        )

    def test_strip_audit_does_not_crown_short_finished_length_as_master(
            self) -> None:
        products = pd.DataFrame([
            {
                "SKU": "LED-NEON-FLEX-NICHO-3000K-2350",
                "Name": "Nicho 3000K 2.35m finished length",
                "Status": "Active",
            },
            {
                "SKU": "LED-NEON-FLEX-NICHO-3000K-2",
                "Name": "Nicho 3000K 2m finished length",
                "Status": "Active",
            },
            {
                "SKU": "LED-NEON-FLEX-NICHO-3000K-1",
                "Name": "Nicho 3000K 1m finished length",
                "Status": "Active",
            },
            {
                "SKU": "LED-NEON-FLEX-NICHO-3000K-0600",
                "Name": "Nicho 3000K 600mm finished length",
                "Status": "Active",
            },
            {
                "SKU": "LED-NEON-FLEX-NICHO-3000K-0300",
                "Name": "Nicho 3000K 300mm finished length",
                "Status": "Active",
            },
        ])
        sale_lines = pd.DataFrame([{
            "SKU": "LED-NEON-FLEX-NICHO-3000K-2",
            "InvoiceDate": "2026-07-02",
            "Quantity": 5,
            "Customer": "Regular LED Customer",
            "Status": "AUTHORISED",
        }])

        audit = build_strip_movement_audit(
            "LED-NEON-FLEX-NICHO-3000K-2350",
            products,
            sale_lines,
            today=pd.Timestamp("2026-07-03"),
        )

        self.assertFalse(audit["ok"])
        self.assertIn("No active bulk buying roll", audit["reason"])


    def test_independently_supplied_strip_sku_not_hidden_by_bulk_sibling(
            self) -> None:
        """LED-WLWW-30K-16-IP20-5 regression test.

        A 5m fixed-length reel that shares a naming family with a ≥25m bulk
        roll and a per-foot variant. The 5m reel is independently ordered from
        a supplier (has CIN7 supplier assigned). It must NOT be added to
        strip_non_master_skus and must:
          - remain visible in orderable_df (Ordering page)
          - retain its own effective_units_12mo (is_non_master_tube = False)
          - still have its sales rolled up to the bulk master (family rollup)

        The fix guards both paths in app.py that add to strip_non_master_skus:
          1. zero-demand path  (own_units == 0 and own_units_90d == 0)
          2. has-demand path   (sales rolled up + non-master flagged)
        """
        from engine.sku_rules import _is_strip_sku, _parse_strip_base

        # Confirm the SKU enters strip parsing via name keyword match
        self.assertTrue(
            _is_strip_sku("LED-WLWW-30K-16-IP20-5",
                          "Wide Lily LED Strip 3000K 16mm IP20 5m"))
        # Confirm it parses correctly as length 5m
        parsed = _parse_strip_base("LED-WLWW-30K-16-IP20-5")
        self.assertIsNotNone(parsed)
        self.assertAlmostEqual(parsed[1], 5.0)
        # Bulk sibling parses as ≥25m (would trigger master election)
        parsed_bulk = _parse_strip_base("LED-WLWW-30K-16-IP20-25")
        self.assertIsNotNone(parsed_bulk)
        self.assertAlmostEqual(parsed_bulk[1], 25.0)
        # Fix is in the strip family loop — BOM required to classify as
        # non-master:
        # - No BOM → NOT added to strip_non_master_skus
        # - is_non_master_tube = False → full demand calculation applies
        # - effective_units_12mo uses real sales data
        # - Demand is NOT rolled up to bulk master (no BOM = no relationship)
        # - SKU appears in orderable_df with its own reorder suggestion
        # Full engine path tested in the Ordering integration tests.


class CoatingWorkOrderTests(unittest.TestCase):
    def test_powder_coating_queue_uses_cin7_bom_service_component(self) -> None:
        boms = pd.DataFrame([
            {
                "AssemblySKU": "LED-AL-PL55B-FL-1",
                "AssemblyName": "PL55 black finished channel",
                "ComponentSKU": "LED-AL-PL55-FL-1",
                "ComponentName": "PL55 raw channel",
                "Quantity": 1,
                "BOMType": "Assembly",
            },
            {
                "AssemblySKU": "LED-AL-PL55B-FL-1",
                "AssemblyName": "PL55 black finished channel",
                "ComponentSKU": "OSC-POWDERCOAT-BK-LRG-FT",
                "ComponentName": "Powder coat black large per foot",
                "Quantity": 3,
                "BOMType": "Assembly",
            },
        ])
        products = pd.DataFrame([
            {
                "SKU": "LED-AL-PL55B-FL-1",
                "Name": "PL55 black finished channel",
                "Suppliers": "Topmet",
            },
            {
                "SKU": "LED-AL-PL55-FL-1",
                "Name": "PL55 raw channel",
            },
            {
                "SKU": "OSC-POWDERCOAT-BK-LRG-FT",
                "Name": "Powder coat black large per foot",
                "Suppliers": '[{"SupplierName": "Powder Coating Vendor"}]',
            },
        ])
        stock = pd.DataFrame([
            {
                "SKU": "LED-AL-PL55B-FL-1",
                "OnHand": 2,
                "Available": 2,
                "OnOrder": 0,
                "Allocated": 0,
            },
            {
                "SKU": "LED-AL-PL55-FL-1",
                "OnHand": 10,
                "Available": 10,
                "OnOrder": 0,
                "Allocated": 0,
            },
        ])
        engine_df = pd.DataFrame([{
            "SKU": "LED-AL-PL55B-FL-1",
            "Name": "PL55 black finished channel",
            "OnHand": 2,
            "Available": 2,
            "OnOrder": 0,
            "target_stock": 7,
            "reorder_qty": 5,
            "ABC": "A",
            "trend_flag": "Stable",
            "Status": "🔴 Reorder now",
            "effective_units_12mo": 40,
            "units_45d": 8,
            "avg_month": 3,
            "Supplier": "Topmet",
        }])

        result = build_coating_work_orders(
            boms=boms,
            products=products,
            stock=stock,
            engine_df=engine_df,
            image_lookup={},
        )
        lines = result["lines"]
        service_summary = result["service_summary"]

        self.assertEqual(len(lines), 1)
        row = lines.iloc[0]
        self.assertEqual(row["Finished SKU"], "LED-AL-PL55B-FL-1")
        self.assertEqual(row["Coating type"], "Powder coating")
        self.assertEqual(row["Send qty"], 5)
        self.assertEqual(row["Service qty"], 15)
        self.assertIn("LED-AL-PL55-FL-1", row["Raw component(s)"])
        self.assertEqual(row["Raw status"], "Raw available")
        self.assertIn("Powder Coating Vendor", row["Coating vendor"])

        self.assertEqual(len(service_summary), 1)
        self.assertEqual(
            service_summary.iloc[0]["Service SKU"],
            "OSC-POWDERCOAT-BK-LRG-FT",
        )
        self.assertEqual(service_summary.iloc[0]["Service_qty"], 15)

    def test_anodizing_service_component_is_detected(self) -> None:
        boms = pd.DataFrame([{
            "AssemblySKU": "LED-AL-ANOD-1",
            "AssemblyName": "Anodized profile",
            "ComponentSKU": "OSC-ANODIZING-CLEAR-FT",
            "ComponentName": "Clear anodizing per foot",
            "Quantity": 2,
        }])

        result = build_coating_work_orders(
            boms=boms,
            products=pd.DataFrame(),
            stock=pd.DataFrame(),
            engine_df=pd.DataFrame([{
                "SKU": "LED-AL-ANOD-1",
                "reorder_qty": 4,
                "target_stock": 4,
                "Available": 0,
                "OnOrder": 0,
            }]),
            image_lookup={},
        )

        self.assertEqual(result["lines"].iloc[0]["Coating type"], "Anodizing")
        self.assertEqual(result["lines"].iloc[0]["Service qty"], 8)


class PageConfigTests(unittest.TestCase):
    def test_ordering_column_preferences_keep_stable_view_key(self) -> None:
        self.assertEqual(ORDERING_PO_EDITOR_VIEW, "ordering_po_editor")

    def test_ordering_grid_does_not_force_pinned_columns(self) -> None:
        script = (
            Path(__file__).resolve().parents[1] / "app.py"
        ).read_text(encoding="utf-8")

        self.assertNotIn("pinned=True", script)
        self.assertIn("Do not auto-pin any columns", script)

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

    def test_anodizing_powder_coating_is_buying_page(self) -> None:
        self.assertIn(
            "Finishing Work Orders",
            PAGE_GROUPS["Buying"],
        )
        self.assertIn(
            "Finishing Work Orders",
            PAGE_DESCRIPTIONS,
        )


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

    def test_ordering_has_materialized_supplier_snapshot_fallback(self) -> None:
        root = Path(__file__).resolve().parents[1]
        app_script = (root / "app.py").read_text(encoding="utf-8")
        warm_script = (
            root / "warm_engine_helpers.py"
        ).read_text(encoding="utf-8")
        db_script = (root / "db.py").read_text(encoding="utf-8")

        self.assertIn("ordering_engine_snapshots", db_script)
        self.assertIn("ordering_supplier_rows", db_script)
        self.assertIn("def replace_ordering_supplier_snapshot", db_script)
        self.assertIn("def get_ordering_supplier_snapshot_rows", db_script)
        self.assertIn("replace_ordering_supplier_snapshot(", warm_script)
        self.assertIn("def _ordering_snapshot_matches_engine", app_script)
        self.assertIn("def _load_ordering_supplier_snapshot", app_script)
        self.assertIn("db.get_latest_ordering_snapshot_meta()", app_script)
        self.assertIn("db.list_ordering_snapshot_suppliers", app_script)
        self.assertIn("ordering_supplier_snapshot_used = False", app_script)
        self.assertIn(
            'orderable_df[orderable_df["Supplier"] == sel_sup]',
            app_script,
        )

    def test_strip_family_rollup_feeds_visible_demand_windows(self) -> None:
        script = (
            Path(__file__).resolve().parents[1] / "app.py"
        ).read_text(encoding="utf-8")

        self.assertIn("strip_rollup_rules: list[tuple[str, str, float]]",
                      script)
        self.assertIn("direct PO history alone is NOT enough", script)
        self.assertIn("is_bulk_strip_roll_length(bulk_len)", script)
        self.assertIn("short finished length", script)
        self.assertIn("_strip_member_discontinued", script)
        self.assertNotIn("alternate_masters", script)
        self.assertGreaterEqual(
            script.count(
                "for child_sku, master_sku, qty_per in strip_rollup_rules"
            ),
            2,
        )
        self.assertIn(
            "for _child_sku, _master_sku, _qty_per in strip_rollup_rules",
            script,
        )

    def test_ordering_editor_has_focus_scroll_enhancer(self) -> None:
        script = (
            Path(__file__).resolve().parents[1] / "app.py"
        ).read_text(encoding="utf-8")

        self.assertIn("def _render_ordering_editor_enhancer", script)
        self.assertIn("w4s-ordering-active-row", script)
        self.assertIn("function hideGuide()", script)
        self.assertIn("ENHANCER_VERSION = \"persistent-row-pan-v4\"",
                      script)
        self.assertIn(".w4s-ordering-editor-frame img:hover", script)
        self.assertIn("const host = frame;", script)
        self.assertIn("function handlePointerMove", script)
        self.assertIn("function installSoon()", script)
        self.assertIn("new MutationObserver", script)
        self.assertIn("\"ArrowUp\", \"ArrowDown\", \"PageUp\", \"PageDown\", \"Escape\"",
                      script)
        self.assertIn("w4s-ordering-dragging", script)
        self.assertIn("w4s-ordering-editor-", script)
        self.assertIn("_render_ordering_editor_enhancer(_ordering_grid_anchor)",
                      script)
        self.assertIn("w4s-pull-forward-editor-", script)
        self.assertIn("_render_ordering_editor_enhancer(_pull_forward_anchor)",
                      script)
        self.assertIn("w4s-all-supplier-editor-", script)
        self.assertIn("_render_ordering_editor_enhancer(_all_supplier_anchor)",
                      script)

    def test_ordering_add_to_po_sections_reuse_saved_column_layout(self) -> None:
        script = (
            Path(__file__).resolve().parents[1] / "app.py"
        ).read_text(encoding="utf-8")

        helper_start = script.index("def _ordering_add_to_po_cols")
        helper_end = script.index("# --- MOV auto-fill", helper_start)
        helper_block = script[helper_start:helper_end]

        self.assertIn("for col in editor_cols:", helper_block)
        self.assertIn('if col in {"Include?", "🔍"}:', helper_block)
        self.assertIn('return ["Add?"] + cols', helper_block)
        self.assertIn("def _ordering_add_to_po_column_config", helper_block)
        self.assertIn("def _ordering_add_to_po_view", helper_block)
        self.assertIn("def _ordering_add_selected_rows_to_po", helper_block)
        self.assertNotIn("save_column_layout", helper_block)
        self.assertIn("All supplier SKUs — search and add to PO", script)
        self.assertIn("Supplier catalogue", script)
        self.assertIn(
            "column_config=_ordering_add_to_po_column_config()",
            script,
        )

    def test_ordering_uses_team_default_layout_without_overwriting_users(self) -> None:
        root = Path(__file__).resolve().parents[1]
        script = (root / "app.py").read_text(encoding="utf-8")
        db_script = (root / "db.py").read_text(encoding="utf-8")

        self.assertIn("TEAM_DEFAULT_UI_USER", db_script)
        self.assertIn("JAMES_LAYOUT_FALLBACK_USERS", db_script)
        self.assertIn("def get_column_layout_with_default", db_script)
        self.assertIn("def get_column_widths_with_default", db_script)
        self.assertIn("def publish_team_default_column_layout", db_script)
        self.assertIn("personal_layout = db.get_column_layout", script)
        self.assertIn("db.get_column_layout_with_default", script)
        self.assertIn("db.get_column_widths_with_default", script)
        self.assertIn("Team default", script)
        self.assertIn("Existing personal saved views were not changed", script)
        self.assertIn("Column layout mode is open", script)
        self.assertIn("to return to the PO table", script)
        self.assertIn("st.stop()", script)

    def test_product_images_feed_and_ordering_thumbnail_column(self) -> None:
        root = Path(__file__).resolve().parents[1]
        script = (root / "app.py").read_text(encoding="utf-8")
        cin7_sync = (root / "cin7_sync.py").read_text(encoding="utf-8")
        daily = (root / "daily_sync.sh").read_text(encoding="utf-8")

        self.assertIn("def sync_product_images", cin7_sync)
        self.assertIn(
            '"product-images": ("no-days", sync_product_images)',
            cin7_sync,
        )
        self.assertIn('"IncludeAttachments": "true"', cin7_sync)
        self.assertIn('write_outputs("product_images", rows)', cin7_sync)
        self.assertIn("PRODUCT_IMAGE_SYNC_FORCE", daily)
        self.assertIn("cin7_sync product-images", daily)
        self.assertIn('product_images = load("product_images")', script)
        self.assertIn("def _product_image_lookup", script)
        self.assertIn('"Image"', script)
        self.assertIn("st.column_config.ImageColumn", script)
        self.assertIn("Hover over the thumbnail", script)

    def test_inventory_planner_notes_sync_writes_to_shared_output(self) -> None:
        root = Path(__file__).resolve().parents[1]
        script = (root / "app.py").read_text(encoding="utf-8")
        notes_sync = (root / "ip_sync_notes.py").read_text(encoding="utf-8")
        ip_pull = (root / "ip_pull_alternates.py").read_text(encoding="utf-8")
        daily = (root / "daily_sync.sh").read_text(encoding="utf-8")
        catalog = (root / "data_catalog.py").read_text(encoding="utf-8")
        docs = (root / "docs" / "sync-cadences.md").read_text(
            encoding="utf-8"
        )

        self.assertIn("from data_paths import OUTPUT_DIR", notes_sync)
        self.assertIn(
            'FIELDS = "id,connections,tags,replenishment_notes,warehouse"',
            notes_sync,
        )
        self.assertIn("replenishment_notes", notes_sync)
        self.assertIn("top-level (`replenishment_notes`)", notes_sync)
        self.assertIn("def _note_entries_for_variant", notes_sync)
        self.assertIn("ip-notes-probe_", notes_sync)
        self.assertIn("empty-ip-notes_", notes_sync)
        self.assertIn("Did not publish a live ip_notes CSV", notes_sync)
        self.assertIn('"SKU", "VariantID", "WarehouseID", "Note", "Tags"',
                      notes_sync)
        self.assertIn("from data_paths import OUTPUT_DIR", ip_pull)
        self.assertIn(
            'FIELDS = "id,connections,merged,replenishment_notes,warehouse,tags"',
            ip_pull,
        )
        self.assertIn("top_note = _note_text(v)", ip_pull)
        self.assertNotIn('OUTPUT_DIR = Path("output")', ip_pull)
        self.assertIn("python ip_sync_notes.py", daily)
        self.assertIn("python ip_sync_notes.py", catalog)
        self.assertIn("LED-SMOKIES38-B-3", docs)
        self.assertIn("def _ip_notes_candidate_files", script)
        self.assertIn("def _read_ip_notes_file", script)
        self.assertIn("if notes:\n            return notes", script)
        self.assertIn("IP_NOTES_BY_SKU_KEY", script)
        self.assertIn("_ip_notes_for_sku", script)
        self.assertIn("_clean_note_text", script)

    def test_overview_surfaces_inventory_cost_vs_retail_value(self) -> None:
        script = (
            Path(__file__).resolve().parents[1] / "app.py"
        ).read_text(encoding="utf-8")

        self.assertIn("def _stock_retail_bridge", script)
        self.assertIn("Inventory value vs retail value", script)
        self.assertIn("Stock cost", script)
        self.assertIn("Stock retail", script)
        self.assertIn("Stock retail by supplier", script)

    def test_ordering_optional_tools_are_lazy_toggled(self) -> None:
        script = (
            Path(__file__).resolve().parents[1] / "app.py"
        ).read_text(encoding="utf-8")

        self.assertIn("show_manual_add = st.toggle", script)
        self.assertIn("if show_manual_add:", script)
        self.assertIn("show_pull_forward = st.toggle", script)
        self.assertIn("if show_pull_forward:", script)
        self.assertIn("show_supplier_catalog = st.toggle", script)
        self.assertIn("if show_supplier_catalog:", script)
        self.assertIn("show_migration_tools = st.toggle", script)
        self.assertIn("if show_migration_tools:", script)
        self.assertIn("show_calc_inspector = st.toggle", script)
        self.assertIn("if show_calc_inspector:", script)
        self.assertNotIn("PO actions — save your edits", script)
        self.assertNotIn("No MOV configured for {sel_sup}. To enable",
                         script)

    def test_supplier_config_follows_draft_po_supplier_by_default(self) -> None:
        script = (
            Path(__file__).resolve().parents[1] / "app.py"
        ).read_text(encoding="utf-8")

        self.assertIn('st.session_state["ordering_active_supplier"] = sel_sup',
                      script)
        self.assertIn("Keep Supplier configuration aligned with Draft PO supplier",
                      script)
        self.assertIn("sc_follow_draft_supplier", script)
        self.assertIn("_config_supplier_mismatch", script)
        self.assertIn("Draft PO supplier is **{_active_po_supplier}**",
                      script)
        self.assertIn("I understand this will change {cfg_supplier}",
                      script)
        self.assertIn("Not saved: Supplier configuration is editing",
                      script)

    def test_pull_forward_window_is_not_hardcoded_to_45_days(self) -> None:
        script = (
            Path(__file__).resolve().parents[1] / "app.py"
        ).read_text(encoding="utf-8")

        self.assertIn("def _pull_forward_window_default_days", script)
        self.assertIn("_pull_forward_window_key", script)
        self.assertIn("Pull-forward window (days)", script)
        self.assertIn("would become a suggested reorder inside", script)
        self.assertIn("the slider recomputes the optional qty", script)
        self.assertIn("pull_forward_reorder_qty", script)
        self.assertIn("_main_po_skus", script)
        self.assertIn("window_target_stock", script)
        self.assertNotIn('f"upcoming_window_{sel_sup}", 45', script)

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

    def test_ordering_has_sku_buying_policy_columns(self) -> None:
        root = Path(__file__).resolve().parents[1]
        script = (root / "app.py").read_text(encoding="utf-8")
        db_script = (root / "db.py").read_text(encoding="utf-8")

        self.assertIn("lead_time_days INTEGER", db_script)
        self.assertIn("eoq_qty REAL", db_script)
        self.assertIn("def set_sku_buying_settings", db_script)
        self.assertIn("_migrate_sku_pack_buying_settings", db_script)
        self.assertIn("sku_buying_settings = {", script)
        self.assertIn("sku_buying_settings_db = {", script)
        self.assertIn("_ordering_sku_buying_preview", script)
        self.assertIn("_policy_preview_updates", script)
        self.assertIn("_policy_preview_clears", script)
        self.assertIn("def _save_sku_buying_policy_edit", script)
        self.assertIn("def _supplier_has_sku_buying_policy", script)
        self.assertIn("def _overlay_sku_buying_policy_columns", script)
        self.assertIn("Saved SKU buying settings for {persisted}", script)
        self.assertIn("def _sync_helper_sku_buying_preview", script)
        self.assertIn(
            "_sync_helper_sku_buying_preview(upc_edited, upc_view)",
            script,
        )
        self.assertIn(
            "_sync_helper_sku_buying_preview(\n"
            "                    catalog_edited, catalog_view)",
            script,
        )
        self.assertIn('st.session_state.pop("_reorder_apply_sig", None)',
                      script)
        self.assertIn('"sku_buying": sku_buying_settings', script)
        self.assertIn('lead_time_basis = "sku"', script)
        self.assertIn('"vendor_lead_time_days"', script)
        self.assertIn('"sku_lead_time_days"', script)
        self.assertIn('"vendor_lead_time_days": "Vendor LT"', script)
        self.assertIn('"lead_time_days": "Used LT"', script)
        self.assertIn('"sku_lead_time_days": "Sku LT"', script)
        self.assertIn("Buyer-facing Vendor LT must mean", script)
        self.assertIn("def _vendor_default_lead_time_for_row", script)
        self.assertIn("Protected existing ", script)
        self.assertIn("_stale_zero_preview_skus", script)
        self.assertIn("Never let that temporary", script)
        self.assertIn("deliberate clear action", script)
        self.assertIn("and not existing_has_policy", script)
        self.assertIn("and not _pd_existing_has_policy", script)
        self.assertIn(
            "SELECT moq_units, mov_amount, mov_currency", db_script)
        self.assertIn(
            "SELECT pack_qty, moq, lead_time_days, eoq_qty, note",
            db_script,
        )
        self.assertIn(
            "SELECT * FROM supplier_config ORDER BY set_at ASC",
            db_script,
        )
        self.assertIn('"sku_moq"', script)
        self.assertIn('"sku_eoq_qty"', script)
        self.assertIn("SKU buying policy", script)
        self.assertIn("db.set_sku_buying_settings", script)
        self.assertIn("_sku_buying_edits", script)
        self.assertIn("sku_buying_settings_db.get(sku_e", script)
        self.assertIn("Ordering and Product Detail use this", script)

    def test_ordering_supplier_snapshot_columns_are_defensive(self) -> None:
        script = (
            Path(__file__).resolve().parents[1] / "app.py"
        ).read_text(encoding="utf-8")

        self.assertIn("def _normalise_ordering_supplier_df", script)
        self.assertIn('"reorder_qty": 0.0', script)
        self.assertIn("_supplier_reorder_qty = pd.to_numeric", script)
        self.assertIn('out.get("reorder_qty"', script)
        self.assertIn("_product_names_by_sku", script)
        self.assertIn("missing_name", script)
        self.assertIn("def _apply_ordering_view_filters", script)
        self.assertIn("relax_status_if_empty=True", script)
        self.assertIn("live_supplier_df = _prepared_live_supplier_df()", script)
        self.assertIn("def _positive_reorder_count", script)
        self.assertIn("_snapshot_has_reorder", script)
        self.assertIn("Main PO grid should mean", script)
        self.assertIn("keep_mask = _supplier_reorder_qty > 0", script)
        self.assertNotIn('(s_df["reorder_qty"] > 0)', script)

    def test_warning_prefixed_statuses_still_match_base_filter(self) -> None:
        script = (
            Path(__file__).resolve().parents[1] / "app.py"
        ).read_text(encoding="utf-8")

        filter_pos = script.index("def _apply_ordering_view_filters")
        status_pos = script.index("if status_filter:", filter_pos)
        reorder_pos = script.index("if only_reorder_positive:", status_pos)
        block = script[status_pos:reorder_pos]
        self.assertIn("status_base = status_series.str.replace", block)
        self.assertIn('r"^❗\\s*"', block)
        self.assertIn("status_series.isin(status_filter)", block)
        self.assertIn("status_base.isin(status_filter)", block)
        self.assertIn("warning prefix is a badge", block)

    def test_ordering_supplier_catalog_search_is_committed_and_bounded(self) -> None:
        script = (
            Path(__file__).resolve().parents[1] / "app.py"
        ).read_text(encoding="utf-8")

        self.assertIn("def _filter_supplier_catalog", script)
        self.assertIn("Searches SKU and product name", script)
        self.assertIn("roma endcap", script)
        self.assertIn("catalog_clear_", script)
        self.assertNotIn("catalog_search_form_", script)
        self.assertIn("max_catalog_rows = 80 if", script)
        self.assertIn("Edits here save immediately", script)

    def test_product_detail_buying_settings_are_above_demand_breakdown(self) -> None:
        script = (
            Path(__file__).resolve().parents[1] / "app.py"
        ).read_text(encoding="utf-8")

        self.assertLess(
            script.index('st.markdown("### Buying settings")'),
            script.index(":mag: Demand breakdown — where does demand"),
        )
        self.assertIn("high-use SKU-level controls stay near the top", script)

    def test_ordering_and_product_detail_show_12mo_series(self) -> None:
        script = (
            Path(__file__).resolve().parents[1] / "app.py"
        ).read_text(encoding="utf-8")

        self.assertIn('df["last_12mo_series"]', script)
        self.assertIn('"last_12mo_series"', script)
        self.assertIn("Last 12 months", script)
        self.assertIn("_pd_engine_row = _pd_hit.iloc[0]", script)
        self.assertIn("engine_row=_pd_engine_row", script)

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
        self.assertIn('"WARM_ENGINE_MIN_AVAILABLE_MB", "2500"', warm_engine)
        self.assertIn(
            'WARM_ENGINE_MIN_AVAILABLE_MB="${WARM_ENGINE_MIN_AVAILABLE_MB:-2500}"',
            sync_loop,
        )
        self.assertIn("WARM_ENGINE_MIN_AVAILABLE_MB", render_config)
        self.assertIn('value: "2500"', render_config)

    def test_large_streamlit_caches_are_bounded(self) -> None:
        script = (
            Path(__file__).resolve().parents[1] / "app.py"
        ).read_text(encoding="utf-8")

        self.assertIn(
            '@st.cache_data(persist="disk", show_spinner="Loading data…",\n'
            '               max_entries=32)',
            script,
        )
        self.assertIn(
            '@st.cache_data(persist="disk", show_spinner="Loading sales history…",\n'
            '               max_entries=1)',
            script,
        )
        self.assertIn('show_spinner="Computing ABC engine…",', script)
        self.assertIn("max_entries=1)\ndef _abc_engine", script)
        self.assertIn(
            "@st.cache_resource(show_spinner=False, max_entries=1)\n"
            "def _get_engine_df_cached",
            script,
        )
        self.assertIn("def _mem_available_mb", script)
        self.assertIn("available < threshold", script)
        self.assertIn('"state": "skipped"', script)
        self.assertIn('"skip_reason"', script)

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

    def test_project_reorder_uses_final_label_and_skips_moq(self) -> None:
        script = (
            Path(__file__).resolve().parents[1] / "app.py"
        ).read_text(encoding="utf-8")

        self.assertIn("top_cust_pct_12mo", script)
        self.assertIn(
            "if visible_12mo > 0 and u45 < 3 and 0 < cust_12mo <= 2",
            script,
        )
        project_reason_pos = script.index(
            'df["project_reason"] = df.apply(_project_reason, axis=1)'
        )
        velocity_pos = script.index(
            'df["avg_daily"] = df.apply(_adjust_avg_daily, axis=1)',
            project_reason_pos,
        )
        doc_pos = script.index('df["DoC_days"] = df.apply(', velocity_pos)
        self.assertLess(velocity_pos, doc_pos)
        self.assertIn(
            "and not use_fractional and not is_project_row",
            script,
        )
        self.assertIn(
            "MOQ {moq:g} not auto-applied to Project rows",
            script,
        )

    def test_trend_column_uses_final_rolled_metrics(self) -> None:
        script = (
            Path(__file__).resolve().parents[1] / "app.py"
        ).read_text(encoding="utf-8")

        customer_rollup_pos = script.index('df["customers_45d"] = (')
        full_year_customer_pos = script.index(
            'df["customers_12mo"] = (',
            customer_rollup_pos,
        )
        final_trend_pos = script.index(
            'df["trend_flag"] = df.apply(_trend_flag, axis=1)',
            customer_rollup_pos,
        )
        monthly_trend_pos = script.index(
            'df["trend_flag"] = df.apply(_upgrade_trend_from_monthly, axis=1)',
            final_trend_pos,
        )
        promote_pos = script.index(
            'df["trend_flag"] = df.apply(_promote_dormant_flag, axis=1)',
            monthly_trend_pos,
        )
        self.assertLess(customer_rollup_pos, final_trend_pos)
        self.assertLess(full_year_customer_pos, final_trend_pos)
        self.assertLess(final_trend_pos, monthly_trend_pos)
        self.assertLess(monthly_trend_pos, promote_pos)
        self.assertIn("if u45v < 3 and n_cust < 10:", script)
        self.assertIn("if n_cust >= 10:", script)
        self.assertIn("Sustained monthly lift catches products", script)
        self.assertIn(
            "final rolled metrics are the source of truth",
            script,
        )

    def test_status_uses_visible_demand_and_shortage_wins(self) -> None:
        script = (
            Path(__file__).resolve().parents[1] / "app.py"
        ).read_text(encoding="utf-8")

        status_pos = script.index("def _status(r):")
        visible_pos = script.index("visible_12mo = max(", status_pos)
        shortage_pos = script.index("if available < 0:", visible_pos)
        no_demand_pos = script.index(
            "elif visible_12mo <= 0 and onhand == 0:",
            shortage_pos,
        )
        dead_pos = script.index(
            "elif visible_12mo <= 0 and onhand > 0:",
            no_demand_pos,
        )
        status_apply_pos = script.index(
            'engine_df["Status"] = engine_df.apply(_status, axis=1)',
            dead_pos,
        )
        dropship_final_pos = script.index(
            'engine_df.loc[_ds_mask, "Status"] = "📦 Dropship"',
            status_apply_pos,
        )
        self.assertLess(shortage_pos, no_demand_pos)
        self.assertLess(no_demand_pos, dead_pos)
        self.assertLess(status_apply_pos, dropship_final_pos)
        self.assertIn("display_units_12mo", script[status_pos:dead_pos])

    def test_assembly_components_do_not_add_bom_estimate_to_12mo_rollup(self) -> None:
        script = (
            Path(__file__).resolve().parents[1] / "app.py"
        ).read_text(encoding="utf-8")

        effective_rollup_pos = script.index(
            "# Now compute rollup across ALL non-master products."
        )
        monthly_rollup_pos = script.index(
            "# --- Roll up children's monthly buckets onto master SKUs",
            effective_rollup_pos,
        )
        effective_rollup_block = script[
            effective_rollup_pos:monthly_rollup_pos
        ]

        self.assertIn(
            "if has_bom and master_sku in assembly_components:",
            effective_rollup_block,
        )
        self.assertIn(
            "skipped BOM sale estimate; FG assembly",
            effective_rollup_block,
        )
        self.assertIn(
            "master_rollup_inflow[master_sku]",
            effective_rollup_block,
        )


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
    def test_bulk_strip_roll_length_guard_only_allows_buying_rolls(self) -> None:
        self.assertFalse(is_bulk_strip_roll_length(0.3))
        self.assertFalse(is_bulk_strip_roll_length(2.35))
        self.assertFalse(is_bulk_strip_roll_length(5))
        self.assertTrue(is_bulk_strip_roll_length(25))
        self.assertTrue(is_bulk_strip_roll_length(100))

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

    def test_worker_engine_does_not_hide_standalone_sibling_without_bom(
            self) -> None:
        """v2.67.376 — mirrors app.py's b0e8eb1 fix. LED-WLWW-30K-IP67-5
        is an independently supplied 5m reel, not a cut of the 25m roll.
        Sharing a naming pattern with a bigger sibling is NOT proof of a
        master/cut relationship — without a BOM/BillOfMaterial/
        SourceFraction, its own real sales must stay on its own row."""
        products = pd.DataFrame([
            {
                "SKU": "LED-WLWW-30K-IP67-50",
                "Name": "[Discontinued] White Lily LED Strip 3000K 50m",
                "Status": "Discontinued",
                "AverageCost": 10.0,
            },
            {
                "SKU": "LED-WLWW-30K-IP67-25",
                "Name": "White Lily LED Strip 3000K 25m",
                "Status": "Active",
                "AverageCost": 10.0,
            },
            {
                "SKU": "LED-WLWW-30K-IP67-5",
                "Name": "White Lily LED Strip 3000K 5m",
                "Status": "Active",
                "AverageCost": 10.0,
            },
        ])
        stock = pd.DataFrame([
            {"SKU": "LED-WLWW-30K-IP67-25", "OnHand": 0},
            {"SKU": "LED-WLWW-30K-IP67-5", "OnHand": 0},
        ])
        sale_lines = pd.DataFrame([{
            "SKU": "LED-WLWW-30K-IP67-5",
            "InvoiceDate": "2026-07-01",
            "Quantity": 5,
            "Customer": "Regular LED Customer",
        }])

        result = worker_engine.compute_engine_signals(
            products, stock, sale_lines)
        by_sku = result.set_index("SKU")

        self.assertFalse(
            bool(by_sku.loc["LED-WLWW-30K-IP67-5", "is_non_master_tube"])
        )
        self.assertEqual(
            by_sku.loc["LED-WLWW-30K-IP67-5",
                       "effective_units_12mo"],
            5.0,
        )
        self.assertEqual(
            by_sku.loc["LED-WLWW-30K-IP67-25",
                       "effective_units_12mo"],
            0.0,
        )

    def test_worker_engine_rolls_strip_child_demand_when_bom_confirms_cut(
            self) -> None:
        """Same naming family as above, but this time a real CIN7 BOM
        says the 5m SKU is assembled/cut from the 25m roll — so the
        rollup (and hiding the child's own demand) is correct."""
        products = pd.DataFrame([
            {
                "SKU": "LED-WLWW-30K-IP67-50",
                "Name": "[Discontinued] White Lily LED Strip 3000K 50m",
                "Status": "Discontinued",
                "AverageCost": 10.0,
            },
            {
                "SKU": "LED-WLWW-30K-IP67-25",
                "Name": "White Lily LED Strip 3000K 25m",
                "Status": "Active",
                "AverageCost": 10.0,
            },
            {
                "SKU": "LED-WLWW-30K-IP67-5",
                "Name": "White Lily LED Strip 3000K 5m",
                "Status": "Active",
                "AverageCost": 10.0,
                "BillOfMaterial": "True",
            },
        ])
        stock = pd.DataFrame([
            {"SKU": "LED-WLWW-30K-IP67-25", "OnHand": 0},
            {"SKU": "LED-WLWW-30K-IP67-5", "OnHand": 0},
        ])
        sale_lines = pd.DataFrame([{
            "SKU": "LED-WLWW-30K-IP67-5",
            "InvoiceDate": "2026-07-01",
            "Quantity": 5,
            "Customer": "Regular LED Customer",
        }])

        result = worker_engine.compute_engine_signals(
            products, stock, sale_lines)
        by_sku = result.set_index("SKU")

        self.assertEqual(
            by_sku.loc["LED-WLWW-30K-IP67-25",
                       "effective_units_12mo"],
            1.0,
        )
        self.assertTrue(
            bool(by_sku.loc["LED-WLWW-30K-IP67-5", "is_non_master_tube"])
        )
        self.assertEqual(
            by_sku.loc["LED-WLWW-30K-IP67-5",
                       "effective_units_12mo"],
            0.0,
        )

    def test_worker_engine_does_not_roll_short_finished_lengths_by_name(
            self) -> None:
        products = pd.DataFrame([
            {
                "SKU": "LED-NEON-FLEX-NICHO-3000K-2350",
                "Name": "Nicho 3000K 2.35m finished length",
                "Status": "Active",
                "AverageCost": 10.0,
            },
            {
                "SKU": "LED-NEON-FLEX-NICHO-3000K-2",
                "Name": "Nicho 3000K 2m finished length",
                "Status": "Active",
                "AverageCost": 10.0,
            },
            {
                "SKU": "LED-NEON-FLEX-NICHO-3000K-0300",
                "Name": "Nicho 3000K 300mm finished length",
                "Status": "Active",
                "AverageCost": 10.0,
            },
        ])
        stock = pd.DataFrame([
            {"SKU": "LED-NEON-FLEX-NICHO-3000K-2350", "OnHand": 0},
            {"SKU": "LED-NEON-FLEX-NICHO-3000K-2", "OnHand": 0},
            {"SKU": "LED-NEON-FLEX-NICHO-3000K-0300", "OnHand": 0},
        ])
        sale_lines = pd.DataFrame([{
            "SKU": "LED-NEON-FLEX-NICHO-3000K-2",
            "InvoiceDate": "2026-07-01",
            "Quantity": 5,
            "Customer": "Regular LED Customer",
        }])

        result = worker_engine.compute_engine_signals(
            products, stock, sale_lines)
        by_sku = result.set_index("SKU")

        self.assertEqual(
            by_sku.loc["LED-NEON-FLEX-NICHO-3000K-2350",
                       "effective_units_12mo"],
            0.0,
        )
        self.assertFalse(
            bool(by_sku.loc["LED-NEON-FLEX-NICHO-3000K-2",
                            "is_non_master_tube"])
        )
        self.assertEqual(
            by_sku.loc["LED-NEON-FLEX-NICHO-3000K-2",
                       "effective_units_12mo"],
            5.0,
        )

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

    def test_purchase_pack_sku_parser_is_strict(self) -> None:
        self.assertEqual(
            parse_pack_purchase_sku("SNFX-L-CR-SCKT-X100"),
            ("SNFX-L-CR-SCKT", 100),
        )
        self.assertEqual(
            parse_pack_purchase_sku("ABC-X2"),
            ("ABC", 2),
        )
        self.assertIsNone(parse_pack_purchase_sku("SNFX-L-CR-SCKT"))
        self.assertIsNone(parse_pack_purchase_sku("SNFX-L-CR-SCKT-X1"))
        self.assertIsNone(parse_pack_purchase_sku("SNFX-L-CR-SCKT-01X2"))


if __name__ == "__main__":
    unittest.main()
