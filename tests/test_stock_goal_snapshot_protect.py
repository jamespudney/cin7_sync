"""A complete stock_goal_snapshots row (with reorder level, written by the
Ordering page) must never be downgraded by the warm job's provisional
write (no reorder level) later the same day. Seen live 2026-09-04: the
Command Centre showed $358K against the page's $426K."""
from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import db  # noqa: E402


def _summary(goal, rl):
    return {"sku_count": 3, "current_value": 700.0, "goal_value": goal,
            "reorder_level_value": rl, "excess_value": 500.0,
            "understock_value": 200.0, "dead_value": 100.0,
            "dead_sku_count": 1, "annual_cogs": 2000.0, "by_class": []}


class ProtectCompleteRow(unittest.TestCase):
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

    def test_partial_then_complete_then_partial(self) -> None:
        db.record_stock_goal_snapshot(_summary(358.0, None), source="warm_engine")
        self.assertEqual(db.get_latest_stock_goal_snapshot()["goal_value"], 358.0)
        db.record_stock_goal_snapshot(_summary(426.0, 165.0), source="ordering_page")
        snap = db.get_latest_stock_goal_snapshot()
        self.assertEqual(snap["goal_value"], 426.0)
        self.assertEqual(snap["reorder_level_value"], 165.0)
        # provisional write later the same day is ignored
        db.record_stock_goal_snapshot(_summary(358.0, None), source="warm_engine")
        snap = db.get_latest_stock_goal_snapshot()
        self.assertEqual(snap["goal_value"], 426.0)
        self.assertEqual(snap["source"], "ordering_page")
        # a fresh complete write still wins
        db.record_stock_goal_snapshot(_summary(430.0, 166.0), source="ordering_page")
        self.assertEqual(db.get_latest_stock_goal_snapshot()["goal_value"], 430.0)


if __name__ == "__main__":
    unittest.main()
