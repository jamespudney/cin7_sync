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
