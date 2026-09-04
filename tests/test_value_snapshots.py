"""engine/value_snapshots.py — slow/dead value snapshot writer must
tolerate object-dtype flag columns and report (not swallow) errors."""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from engine.value_snapshots import (  # noqa: E402
    dead_stock_totals, record_value_snapshots, slow_stock_totals)


def _frame():
    return pd.DataFrame({
        "SKU": ["a", "b", "c", "d"],
        "is_dormant": [True, True, None, False],       # object dtype
        "is_non_master_tube": [False, True, None, False],
        "is_dead": [False, None, True, False],
        "OnHand": [2, 5, 1, 0],
        "OnHandValue": [10.0, 50.0, 7.5, 0.0],
    })


class _FakeDb:
    def __init__(self, fail=False):
        self.calls = []
        self.fail = fail

    def record_slow_mover_value_snapshot(self, *a):
        if self.fail:
            raise RuntimeError("boom")
        self.calls.append(("slow",) + a)

    def record_dead_stock_value_snapshot(self, *a):
        self.calls.append(("dead",) + a)


class ValueSnapshots(unittest.TestCase):
    def test_totals(self):
        slow = slow_stock_totals(_frame())
        # a: dormant, on hand, master -> counted; b: non-master tube -> no
        self.assertEqual(slow["skus"], 1)
        self.assertAlmostEqual(slow["value"], 10.0)
        dead = dead_stock_totals(_frame())
        self.assertEqual(dead["skus"], 1)
        self.assertAlmostEqual(dead["value"], 7.5)

    def test_writes_both(self):
        db = _FakeDb()
        out = record_value_snapshots(_frame(), db)
        self.assertEqual([c[0] for c in db.calls], ["slow", "dead"])
        self.assertNotIn("error", out["slow"])

    def test_error_is_reported_not_raised(self):
        db = _FakeDb(fail=True)
        out = record_value_snapshots(_frame(), db)
        self.assertIn("error", out["slow"])
        self.assertEqual([c[0] for c in db.calls], ["dead"])

    def test_empty(self):
        self.assertIn("error", record_value_snapshots(pd.DataFrame(), _FakeDb()))


if __name__ == "__main__":
    unittest.main()
