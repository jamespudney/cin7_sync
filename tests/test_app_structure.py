from __future__ import annotations

import os
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch

import pandas as pd

import ai_tools
import worker_engine
from app_config import (
    PAGE_CAPTIONS,
    PAGE_DESCRIPTIONS,
    PAGE_GROUP_BY_NAME,
    PAGE_GROUPS,
    PAGE_OPTIONS,
)
from app_pages.my_profile import (
    SLACK_OAUTH_ENV_VARS,
    missing_slack_oauth_env_vars,
)
from app_pages.ordering_layout import ORDERING_PO_EDITOR_VIEW
from data_catalog import DatasetSpec, catalog_rows, latest_file
from engine.sku_rules import (
    _is_strip_sku,
    _parse_length,
    _parse_strip_base,
    _parse_tube_sku,
    parse_sourcing_rule,
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

        daily_refresh = script.index("launching daily 30d refresh chain")
        products_refresh = script.index("python cin7_sync.py products",
                                        daily_refresh)
        sales_refresh = script.index("python cin7_sync.py salelines --days 30",
                                    daily_refresh)

        self.assertLess(products_refresh, sales_refresh)
        self.assertIn("NearSync", script)
        self.assertIn("Storage L x W x H In", script)


class IncomingStockTests(unittest.TestCase):
    def tearDown(self) -> None:
        ai_tools.set_purchase_lines(pd.DataFrame())

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
