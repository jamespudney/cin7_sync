"""dataset_mirror.py — dashboard publishes sync CSVs to the shared DB,
worker pulls identical bytes onto its own disk (2026-09-03)."""
from __future__ import annotations

import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import db  # noqa: E402
import dataset_mirror as dm  # noqa: E402


def _write(dirpath: Path, name: str, text: str, mtime: float) -> Path:
    p = dirpath / name
    p.write_text(text, encoding="utf-8")
    os.utime(p, (mtime, mtime))
    return p


class MirrorTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        root = Path(self._tmp.name)
        self.app_dir = root / "app_output"
        self.worker_dir = root / "worker_output"
        self.app_dir.mkdir()
        self.worker_dir.mkdir()
        self._patch = patch.object(db, "DB_PATH", str(root / "t.db"))
        self._patch.start()
        with db.connect():
            pass

    def tearDown(self) -> None:
        self._patch.stop()
        self._tmp.cleanup()

    def test_logical_key_strips_sync_stamp_only(self) -> None:
        self.assertEqual(dm.logical_key("products_2026-09-03_020112.csv"),
                         "products.csv")
        self.assertEqual(
            dm.logical_key("sale_lines_last_730d_2026-08-01_031500.csv"),
            "sale_lines_last_730d.csv")
        self.assertEqual(dm.logical_key("shipments_full.csv"),
                         "shipments_full.csv")
        self.assertEqual(dm.logical_key("engine_output.csv"),
                         "engine_output.csv")

    def test_publish_then_pull_gives_identical_files_and_mtimes(self) -> None:
        _write(self.app_dir, "products_2026-09-03_020000.csv",
               "SKU,Name\nA1,a\n", 1_700_000_000)
        _write(self.app_dir, "products_2026-09-02_020000.csv",
               "SKU,Name\nOLD,o\n", 1_699_900_000)  # older, not published
        _write(self.app_dir, "sale_lines_last_730d_2026-08-01_030000.csv",
               "SKU,Quantity\nA1,5\n", 1_690_000_000)
        _write(self.app_dir, "sale_lines_last_30d_2026-09-03_030000.csv",
               "SKU,Quantity\nA1,1\n", 1_700_000_100)
        _write(self.app_dir, "shipments_full.csv", "id\n1\n", 1_700_000_200)
        _write(self.app_dir, "products_2026-09-03_020000.json", "[]", 1)
        _write(self.app_dir, "cin7_sync.log", "noise", 1)

        info = dm.publish(self.app_dir, publisher="test")
        self.assertEqual(sorted(k for k, *_ in info["pushed"]),
                         ["products.csv", "sale_lines_last_30d.csv",
                          "sale_lines_last_730d.csv", "shipments_full.csv"])

        # Worker had its own stale copies from its own sync.
        _write(self.worker_dir, "products_2026-09-01_120000.csv",
               "SKU,Name\nSTALE,s\n", 1_699_000_000)
        _write(self.worker_dir, "sale_lines_last_30d_2026-09-01_120000.csv",
               "SKU,Quantity\nSTALE,9\n", 1_699_000_000)

        out = dm.pull(self.worker_dir)
        self.assertTrue(out["available"])
        names = sorted(p.name for p in self.worker_dir.iterdir()
                       if not p.name.startswith("."))
        self.assertEqual(names, [
            "products_2026-09-03_020000.csv",
            "sale_lines_last_30d_2026-09-03_030000.csv",
            "sale_lines_last_730d_2026-08-01_030000.csv",
            "shipments_full.csv",
        ])
        # Stale worker-sync copies were removed so globs can't pick them.
        self.assertIn("products_2026-09-01_120000.csv", out["removed"])
        for name in names:
            a = self.app_dir / name
            w = self.worker_dir / name
            self.assertEqual(a.read_bytes(), w.read_bytes(), name)
            self.assertAlmostEqual(a.stat().st_mtime, w.stat().st_mtime,
                                   delta=1, msg=name)

    def test_republish_same_bytes_new_stamp_renames_without_download(self) -> None:
        _write(self.app_dir, "stock_on_hand_2026-09-03_100000.csv",
               "SKU,OnHand\nA1,3\n", 1_700_000_000)
        dm.publish(self.app_dir, publisher="test")
        dm.pull(self.worker_dir)
        # Dashboard re-syncs: identical content, new stamp.
        (self.app_dir / "stock_on_hand_2026-09-03_100000.csv").unlink()
        _write(self.app_dir, "stock_on_hand_2026-09-03_103000.csv",
               "SKU,OnHand\nA1,3\n", 1_700_001_800)
        info = dm.publish(self.app_dir, publisher="test")
        self.assertEqual(info["pushed"], [])  # touch only
        out = dm.pull(self.worker_dir)
        self.assertEqual(out["written"], [])
        self.assertEqual(
            sorted(p.name for p in self.worker_dir.glob("stock_on_hand_*.csv")),
            ["stock_on_hand_2026-09-03_103000.csv"])

    def test_pull_reports_unavailable_when_db_empty(self) -> None:
        out = dm.pull(self.worker_dir)
        self.assertFalse(out["available"])
        self.assertFalse(dm.status()["available"])

    def test_changed_content_is_redownloaded(self) -> None:
        _write(self.app_dir, "products_2026-09-03_020000.csv",
               "SKU\nA1\n", 1_700_000_000)
        dm.publish(self.app_dir, publisher="test")
        dm.pull(self.worker_dir)
        _write(self.app_dir, "products_2026-09-03_020000.csv",
               "SKU\nA1\nB2\n", 1_700_000_500)
        dm.publish(self.app_dir, publisher="test")
        out = dm.pull(self.worker_dir)
        self.assertEqual(out["written"], ["products_2026-09-03_020000.csv"])
        self.assertEqual(
            (self.worker_dir / "products_2026-09-03_020000.csv").read_text(),
            "SKU\nA1\nB2\n")


if __name__ == "__main__":
    unittest.main()
