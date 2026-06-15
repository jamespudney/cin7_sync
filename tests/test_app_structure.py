from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path

from app_config import (
    PAGE_CAPTIONS,
    PAGE_DESCRIPTIONS,
    PAGE_GROUP_BY_NAME,
    PAGE_GROUPS,
    PAGE_OPTIONS,
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


class PageConfigTests(unittest.TestCase):
    def test_ordering_column_preferences_keep_stable_view_key(self) -> None:
        self.assertEqual(ORDERING_PO_EDITOR_VIEW, "ordering_po_editor")

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
