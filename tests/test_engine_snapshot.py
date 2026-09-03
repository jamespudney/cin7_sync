"""Full engine snapshot in the shared DB (2026-09-03).

The Slack bot worker has no disk, so it never saw engine_output.csv and
recomputed a simplified engine of its own. The warm job now writes every
engine row to `engine_snapshot_rows`; the listener reads that so the bot
quotes exactly the dashboard's figures. These tests cover the round trip
and the Bin (stock locator) re-attachment the canonical frame needs.
"""
from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import db  # noqa: E402
import worker_engine  # noqa: E402


class _TempDb(unittest.TestCase):
    def setUp(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory()
        self._patch = patch.object(
            db, "DB_PATH", str(Path(self._tmpdir.name) / "t.db"))
        self._patch.start()
        with db.connect():
            pass

    def tearDown(self) -> None:
        self._patch.stop()
        self._tmpdir.cleanup()


class EngineSnapshotRoundTrip(_TempDb):
    def test_write_read_keeps_every_row_and_column(self) -> None:
        df = pd.DataFrame({
            "SKU": ["A1", "CUT-0305", "N1"],
            "Supplier": ["Topmet", "", "Neonica"],
            "is_non_master_tube": [False, True, False],
            "ABC": ["A", "C", "B"],
            "OnHand": [10, 3, 0],
            "excess_value": [12.5, 0.0, float("nan")],
            "goal_units": [8.0, 0.0, 2.0],
        })
        info = db.replace_engine_snapshot(df, source_mtime=1000.0)
        self.assertTrue(info["written"])
        self.assertEqual(info["rows"], 3)  # cuts + unsupplied kept
        rows = db.load_engine_snapshot_rows()
        back = pd.DataFrame(rows)
        self.assertEqual(sorted(back["SKU"]), ["A1", "CUT-0305", "N1"])
        self.assertIn("goal_units", back.columns)
        self.assertEqual(
            float(back.set_index("SKU").loc["A1", "excess_value"]), 12.5)
        # nan → null → None
        self.assertTrue(
            pd.isna(back.set_index("SKU").loc["N1", "excess_value"]))

    def test_only_newest_snapshot_is_kept(self) -> None:
        db.replace_engine_snapshot(
            pd.DataFrame({"SKU": ["OLD"]}), source_mtime=1.0)
        db.replace_engine_snapshot(
            pd.DataFrame({"SKU": ["NEW"]}), source_mtime=2.0)
        meta = db.get_latest_engine_snapshot_meta()
        self.assertEqual(meta["snapshot_key"], "engine:2")
        self.assertEqual([r["SKU"] for r in db.load_engine_snapshot_rows()],
                         ["NEW"])

    def test_empty_when_nothing_written(self) -> None:
        self.assertEqual(db.load_engine_snapshot_rows(), [])
        self.assertEqual(db.get_latest_engine_snapshot_meta(), {})


class AttachStockLocator(unittest.TestCase):
    def test_bin_comes_from_stock_locator_not_location(self) -> None:
        engine = pd.DataFrame({"SKU": ["A1", "B1"], "OnHand": [1, 2]})
        stock = pd.DataFrame({
            "SKU": ["A1", "B1"],
            "StockLocator": ["R3-S2", None],
            "Location": ["Main Warehouse", "Main Warehouse"],
        })
        out = worker_engine.attach_stock_locator(engine, stock)
        self.assertEqual(list(out["Bin"]), ["R3-S2", ""])
        self.assertEqual(len(out), 2)

    def test_no_stock_frame_is_harmless(self) -> None:
        engine = pd.DataFrame({"SKU": ["A1"], "OnHand": [1]})
        out = worker_engine.attach_stock_locator(engine, None)
        self.assertEqual(len(out), 1)


if __name__ == "__main__":
    unittest.main()
