"""Standalone verification tests for ai_tools._demand_snapshot_caveat
and its wiring into get_purchase_order / get_purchase_live.

Real incident this covers: PO-7558 commentary called LED-20.080 "no
demand history (0 units 12mo, 0 effective)" two days after a real sale
(SO-60355) had already landed in CIN7. The engine's effective_units_12mo
hadn't caught up yet because the widest sale_lines snapshot available
at commentary time was a monthly-refreshed 730-day backfill that
simply predated the order — a benign gap that self-corrected within
the next sync cycle, but the confidently-worded "no demand history"
needlessly alarmed the buyer in the meantime.

This file proves:
  1. The exact LED-20.080 scenario (effective_units_12mo == 0, a real
     sale 2 days ago) produces a caveat mentioning the order and date.
  2. A genuinely dormant SKU (0 effective demand, no recent sale at
     all) gets no caveat — this only fires on an actual contradiction.
  3. A sale outside the lookback window doesn't trigger it.
  4. A SKU with real nonzero effective demand never gets a caveat,
     even with a very recent sale (no contradiction to flag).
  5. The field is wired through end-to-end on both get_purchase_order
     and get_purchase_live.

Run directly:
    python3 tests/test_demand_snapshot_caveat.py
or:
    python3 -m unittest tests.test_demand_snapshot_caveat -v
"""

from __future__ import annotations

import sys
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch

import pandas as pd

SCRIPT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPT_DIR))

import ai_tools  # noqa: E402


def _sale_lines(sku: str, days_ago: int, order_number: str = "SO-60355"
                 ) -> pd.DataFrame:
    order_date = (datetime.now() - timedelta(days=days_ago)
                  ).strftime("%Y-%m-%d")
    return pd.DataFrame([{
        "SaleID": "test-sale-id",
        "OrderNumber": order_number,
        "OrderDate": order_date,
        "InvoiceDate": order_date,
        "SKU": sku,
        "Quantity": 1.0,
    }])


class DemandSnapshotCaveatUnitTests(unittest.TestCase):
    """Direct tests of the helper function."""

    def test_led_20080_scenario_produces_caveat(self) -> None:
        sl = _sale_lines("LED-20.080", days_ago=2)
        note = ai_tools._demand_snapshot_caveat("LED-20.080", sl, 0.0)
        self.assertIsNotNone(note)
        self.assertIn("SO-60355", note)
        self.assertIn("0", note)

    def test_genuinely_dormant_no_recent_sale_no_caveat(self) -> None:
        sl = _sale_lines("LED-99.999", days_ago=2)  # different SKU
        note = ai_tools._demand_snapshot_caveat("LED-20.080", sl, 0.0)
        self.assertIsNone(note)

    def test_empty_sale_lines_no_caveat(self) -> None:
        note = ai_tools._demand_snapshot_caveat(
            "LED-20.080", pd.DataFrame(), 0.0)
        self.assertIsNone(note)

    def test_sale_beyond_lookback_window_no_caveat(self) -> None:
        sl = _sale_lines("LED-20.080", days_ago=20)
        note = ai_tools._demand_snapshot_caveat(
            "LED-20.080", sl, 0.0, lookback_days=7)
        self.assertIsNone(note)

    def test_sale_just_inside_lookback_window_triggers(self) -> None:
        sl = _sale_lines("LED-20.080", days_ago=7)
        note = ai_tools._demand_snapshot_caveat(
            "LED-20.080", sl, 0.0, lookback_days=7)
        self.assertIsNotNone(note)

    def test_nonzero_effective_demand_never_flagged(self) -> None:
        # Even with a sale yesterday, a real nonzero demand read means
        # there's no contradiction to surface.
        sl = _sale_lines("LED-MC-20.009-S", days_ago=1)
        note = ai_tools._demand_snapshot_caveat(
            "LED-MC-20.009-S", sl, 181.0)
        self.assertIsNone(note)

    def test_custom_lookback_days_respected(self) -> None:
        sl = _sale_lines("LED-20.080", days_ago=10)
        self.assertIsNone(ai_tools._demand_snapshot_caveat(
            "LED-20.080", sl, 0.0, lookback_days=7))
        self.assertIsNotNone(ai_tools._demand_snapshot_caveat(
            "LED-20.080", sl, 0.0, lookback_days=14))


class GetPurchaseOrderWiringTests(unittest.TestCase):
    def tearDown(self) -> None:
        ai_tools.set_purchase_lines(pd.DataFrame())

    def test_caveat_field_present_on_purchase_order_line(self) -> None:
        sku = "LED-20.080"
        engine_df = pd.DataFrame([{
            "SKU": sku,
            "effective_units_12mo": 0,
            "units_12mo": 0,
        }])
        purchase_lines = pd.DataFrame([{
            "PurchaseID": "7558",
            "OrderNumber": "PO-7558",
            "Status": "ORDERED",
            "Supplier": "Luz Negra (EUR)",
            "SKU": sku,
            "Name": "Test accessory",
            "Quantity": 10,
        }])
        sale_lines = _sale_lines(sku, days_ago=2)

        ai_tools.set_purchase_lines(purchase_lines)
        result = ai_tools.get_purchase_order(
            engine_df, sale_lines, {"po_number": "PO-7558"})

        line = result["purchase_orders"][0]["lines"][0]
        self.assertIsNotNone(line["demand_snapshot_caveat"])
        self.assertIn("SO-60355", line["demand_snapshot_caveat"])

    def test_no_caveat_field_when_no_contradiction(self) -> None:
        sku = "LED-20.080"
        engine_df = pd.DataFrame([{
            "SKU": sku,
            "effective_units_12mo": 0,
            "units_12mo": 0,
        }])
        purchase_lines = pd.DataFrame([{
            "PurchaseID": "7558",
            "OrderNumber": "PO-7558",
            "Status": "ORDERED",
            "Supplier": "Luz Negra (EUR)",
            "SKU": sku,
            "Name": "Test accessory",
            "Quantity": 10,
        }])

        ai_tools.set_purchase_lines(purchase_lines)
        result = ai_tools.get_purchase_order(
            engine_df, pd.DataFrame(), {"po_number": "PO-7558"})

        line = result["purchase_orders"][0]["lines"][0]
        self.assertIsNone(line["demand_snapshot_caveat"])


class GetPurchaseLiveWiringTests(unittest.TestCase):
    def test_caveat_field_present_on_purchase_live_line(self) -> None:
        sku = "LED-20.080"
        engine_df = pd.DataFrame([{
            "SKU": sku,
            "effective_units_12mo": 0,
            "units_12mo": 0,
        }])
        sale_lines = _sale_lines(sku, days_ago=2)

        class FakeClient:
            def __init__(self, account_id, app_key):
                pass

            def get_purchase(self, po_ref):
                return {
                    "ID": "purchase-id",
                    "OrderNumber": "PO-7558",
                    "Supplier": "Luz Negra (EUR)",
                    "Status": "ORDERED",
                    "Order": {
                        "Lines": [{
                            "SKU": sku,
                            "Name": "Test accessory",
                            "Quantity": 10,
                        }],
                    },
                }

        with patch.dict(
            "os.environ",
            {"CIN7_ACCOUNT_ID": "acct", "CIN7_APPLICATION_KEY": "key"},
        ), patch("cin7_sync.Cin7Client", FakeClient):
            result = ai_tools.get_purchase_live(
                engine_df, sale_lines, {"po_number": "PO-7558"})

        line = result["lines"][0]
        self.assertIsNotNone(line["demand_snapshot_caveat"])
        self.assertIn("SO-60355", line["demand_snapshot_caveat"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
