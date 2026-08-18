"""Standalone verification tests for sharing real CIN7 assembly-
consumption data with the worker via Postgres instead of a local CSV.

Real incident (Aug 2026): worker_engine.py already had logic to
prefer real per-task assembly consumption over a BOM-ratio proxy
(built after Andrew flagged an ~11x-too-low reading for LED-SILICONE-
PMT80050), but the worker never actually received any assemblies
data — slack_loop.sh never runs `cin7_sync.py assemblies`, and
assemblies_last_*.csv only ever lands on the app service's disk
(Render disks aren't shared across services). So every BOM-heavy
component with little direct sale_lines activity (e.g.
LED-MC-20.009-S, a mounting clip) silently fell back to the same
flawed proxy that caused the original incident.

Re-fetching a full assemblies history independently on the worker was
rejected: sync_assemblies() requires one detail API call PER TASK and
a full window can take hours against the shared, rate-limited CIN7
account. Instead, cin7_sync.py's sync_assemblies() now also upserts
into a shared `assembly_component_consumption` Postgres table, and
the worker reads from there instead of (or falling back to) the local
CSV glob it could never actually find data in.

This file proves:
  1. db.py's upsert/read/prune functions behave correctly in
     isolation (bulk upsert is idempotent, `since` filtering works,
     pruning only removes rows past the retention window).
  2. slack_listener.py's data loader prefers the shared DB data over
     the local CSV glob, and the resulting engine_df shows correct
     nonzero effective_units_12mo for an assembly-only-demand SKU
     (LED-MC-20.009-S) — reproducing the fix for the real incident.
  3. It still falls back to a local CSV if the DB read is empty
     (regression safety net for local dev against SQLite).

Run directly:
    python3 tests/test_assembly_component_consumption.py
or:
    python3 -m unittest tests.test_assembly_component_consumption -v
"""

from __future__ import annotations

import sys
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch

SCRIPT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPT_DIR))

import db  # noqa: E402


def _row(task_id, sku, qty, days_ago, assembly_number="FG-1",
         status="COMPLETED"):
    completion = (datetime.now() - timedelta(days=days_ago)
                  ).strftime("%Y-%m-%d")
    return {
        "TaskID": task_id,
        "ComponentSKU": sku,
        "AssemblyNumber": assembly_number,
        "ParentSKU": "AERC-FRAME-COMPLETE",
        "ParentName": "Frame",
        "Quantity": qty,
        "CompletionDate": completion,
        "Status": status,
    }


class _TempDbTestCase(unittest.TestCase):
    """Points db.py at a fresh, isolated SQLite file for the duration
    of each test."""

    def setUp(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory()
        tmp_path = str(Path(self._tmpdir.name) / "test_team_actions.db")
        self._db_path_patcher = patch.object(db, "DB_PATH", tmp_path)
        self._db_path_patcher.start()
        with db.connect():
            pass  # applies schema

    def tearDown(self) -> None:
        self._db_path_patcher.stop()
        self._tmpdir.cleanup()


class UpsertReadPruneTests(_TempDbTestCase):
    def test_upsert_skips_rows_missing_task_id_or_sku(self) -> None:
        rows = [
            _row("1", "LED-MC-20.009-S", 8, days_ago=1),
            {"TaskID": "", "ComponentSKU": "X", "Quantity": 1},
            {"TaskID": "2", "ComponentSKU": "", "Quantity": 1},
        ]
        n = db.upsert_assembly_component_consumption(rows)
        self.assertEqual(n, 1)

    def test_upsert_is_idempotent_on_same_task_and_sku(self) -> None:
        row = _row("1", "LED-MC-20.009-S", 8, days_ago=1)
        db.upsert_assembly_component_consumption([row])
        row["Quantity"] = 99  # same key, different quantity
        db.upsert_assembly_component_consumption([row])
        all_rows = db.get_assembly_component_consumption()
        self.assertEqual(len(all_rows), 1)
        self.assertEqual(all_rows[0]["Quantity"], 99.0)

    def test_multi_bin_pick_lines_sum_instead_of_overwrite(self) -> None:
        # Real scenario flagged in review: one assembly task picks the
        # SAME component from two different bins (split stock), which
        # cin7_sync.py writes as two separate PickLine rows sharing
        # the same (TaskID, ComponentSKU) but different BinID/
        # Quantity. Before the fix, ON CONFLICT DO UPDATE overwrote
        # rather than summed, silently losing whichever bin's
        # quantity lost the race — the exact class of bug this whole
        # feature exists to fix, reintroduced via a different path.
        bin_a = _row("1", "LED-MC-20.009-S", 5, days_ago=1)
        bin_a["BinID"] = "BIN-A"
        bin_b = _row("1", "LED-MC-20.009-S", 3, days_ago=1)
        bin_b["BinID"] = "BIN-B"

        n = db.upsert_assembly_component_consumption([bin_a, bin_b])
        self.assertEqual(n, 1)  # one merged row, not two

        rows = db.get_assembly_component_consumption()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["Quantity"], 8.0)  # 5 + 3, not 3 or 5

    def test_repeated_upsert_of_same_batch_replaces_not_adds(self) -> None:
        # Deliberately REPLACE, not ADD, on conflict — additive
        # semantics would break idempotency: re-running the same sync
        # twice (a legitimate operational scenario — manual retries,
        # a scheduled run overlapping a manual one) would otherwise
        # double the stored quantity every re-run. This is why the
        # multi-bin fix merges within ONE call (summing before
        # upserting) rather than relying on the DB to accumulate
        # across calls — safe only because cin7_sync.py's row-
        # building loop always appends a task's FULL pick-line set
        # before that task is eligible for a flush, so one task's
        # pick lines are never split across two separate upsert calls
        # in practice (see the multi-bin test above for the case that
        # matters).
        row = _row("1", "LED-MC-20.009-S", 8, days_ago=1)
        db.upsert_assembly_component_consumption([row])
        db.upsert_assembly_component_consumption([row])  # same sync, re-run
        rows = db.get_assembly_component_consumption()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["Quantity"], 8.0)  # not 16.0

    def test_since_filter_excludes_older_rows(self) -> None:
        db.upsert_assembly_component_consumption([
            _row("1", "SKU-A", 1, days_ago=1),
            _row("2", "SKU-A", 1, days_ago=200),
        ])
        recent_cutoff = (datetime.now() - timedelta(days=30)
                          ).strftime("%Y-%m-%d")
        recent = db.get_assembly_component_consumption(since=recent_cutoff)
        self.assertEqual(len(recent), 1)
        self.assertEqual(recent[0]["TaskID"], "1")

    def test_prune_removes_only_rows_past_retention_window(self) -> None:
        db.upsert_assembly_component_consumption([
            _row("1", "SKU-A", 1, days_ago=1),
            _row("2", "SKU-A", 1, days_ago=500),
        ])
        n_pruned = db.prune_assembly_component_consumption(
            older_than_days=400)
        self.assertEqual(n_pruned, 1)
        remaining = db.get_assembly_component_consumption()
        self.assertEqual(len(remaining), 1)
        self.assertEqual(remaining[0]["TaskID"], "1")

    def test_prune_also_removes_null_completion_date_rows(self) -> None:
        # A CIN7 detail response missing BOTH CompletionDate and Date
        # (task.get("Date") also empty) reaches cin7_sync.py with
        # completion=None — such rows are already invisible to every
        # actual demand calculation (worker_engine.py drops them via
        # dropna()), so there's nothing to preserve by keeping them.
        # Under plain SQL semantics `NULL < cutoff` is never true, so
        # without an explicit `OR completion_date IS NULL` these rows
        # would accumulate in the table forever.
        dated = _row("1", "SKU-A", 1, days_ago=1)
        undated = _row("2", "SKU-A", 1, days_ago=1)
        undated["CompletionDate"] = None
        db.upsert_assembly_component_consumption([dated, undated])
        self.assertEqual(len(db.get_assembly_component_consumption()), 2)

        n_pruned = db.prune_assembly_component_consumption(
            older_than_days=400)
        self.assertEqual(n_pruned, 1)
        remaining = db.get_assembly_component_consumption()
        self.assertEqual(len(remaining), 1)
        self.assertEqual(remaining[0]["TaskID"], "1")

    def test_get_excludes_null_completion_date_rows_when_since_set(self) -> None:
        dated = _row("1", "SKU-A", 1, days_ago=1)
        undated = _row("2", "SKU-A", 1, days_ago=1)
        undated["CompletionDate"] = None
        db.upsert_assembly_component_consumption([dated, undated])

        cutoff = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
        rows = db.get_assembly_component_consumption(since=cutoff)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["TaskID"], "1")

        # Without a `since` bound, the undated row is still returned —
        # only the since-filtered path silently excludes NULLs.
        all_rows = db.get_assembly_component_consumption()
        self.assertEqual(len(all_rows), 2)

    def test_empty_rows_returns_zero(self) -> None:
        self.assertEqual(db.upsert_assembly_component_consumption([]), 0)


class ListenerPrefersSharedDbTests(unittest.TestCase):
    """End-to-end: confirm the worker's engine_df picks up real
    assembly-driven demand for a component sourced from the shared
    DB, with no local assemblies CSV present at all — the exact
    production scenario (no disk, no local sync)."""

    def setUp(self) -> None:
        import pandas as pd
        self.pd = pd
        self._tmpdir = tempfile.TemporaryDirectory()
        self.output_dir = Path(self._tmpdir.name)

        sku = "LED-MC-20.009-S"
        pd.DataFrame([{"SKU": sku, "Name": "Mounting Clip",
                       "Status": "Active"}]).to_csv(
            self.output_dir / "products_2026-01-01_000000.csv",
            index=False)
        pd.DataFrame([{"SKU": sku, "OnHand": 120, "OnOrder": 200,
                       "Available": 100}]).to_csv(
            self.output_dir / "stock_on_hand_2026-01-01_000000.csv",
            index=False)

        import slack_listener
        self.slack_listener = slack_listener
        slack_listener._LISTENER_DATA_CACHE["engine_df"] = None
        slack_listener._LISTENER_DATA_CACHE["sale_lines_df"] = None
        slack_listener._LISTENER_DATA_CACHE["loaded_at"] = 0

        self._data_dir_patcher = patch(
            "data_paths.OUTPUT_DIR", self.output_dir)
        self._data_dir_patcher.start()

    def tearDown(self) -> None:
        self._data_dir_patcher.stop()
        self._tmpdir.cleanup()
        self.slack_listener._LISTENER_DATA_CACHE["engine_df"] = None
        self.slack_listener._LISTENER_DATA_CACHE["sale_lines_df"] = None
        self.slack_listener._LISTENER_DATA_CACHE["loaded_at"] = 0

    def test_engine_df_shows_real_demand_from_shared_db(self) -> None:
        sku = "LED-MC-20.009-S"
        db_rows = [
            _row(str(i), sku, 8, days_ago=d)
            for i, d in enumerate([2, 10, 20, 40, 60], start=1)
        ]
        with patch("db.get_assembly_component_consumption",
                    return_value=db_rows):
            engine_df, _sale_lines = (
                self.slack_listener._get_data_for_listener())

        self.assertIsNotNone(engine_df)
        row = engine_df[engine_df["SKU"] == sku]
        self.assertFalse(row.empty)
        # 5 tasks x 8 units = 40 units of real assembly-driven demand,
        # none of it visible in sale_lines (there is none loaded) —
        # this can ONLY be nonzero if the shared-DB assemblies path
        # actually fed into worker_engine.compute_engine_signals().
        self.assertGreater(float(row.iloc[0]["effective_units_12mo"]), 0)

    def test_falls_back_to_local_csv_when_db_empty(self) -> None:
        sku = "LED-MC-20.009-S"
        self.pd.DataFrame([_row("1", sku, 8, days_ago=2)]).to_csv(
            self.output_dir / "assemblies_last_30d_2026-01-01_000000.csv",
            index=False)

        with patch("db.get_assembly_component_consumption",
                    return_value=[]):
            engine_df, _sale_lines = (
                self.slack_listener._get_data_for_listener())

        self.assertIsNotNone(engine_df)
        row = engine_df[engine_df["SKU"] == sku]
        self.assertFalse(row.empty)
        self.assertGreater(float(row.iloc[0]["effective_units_12mo"]), 0)

    def test_db_error_falls_back_to_local_csv(self) -> None:
        sku = "LED-MC-20.009-S"
        self.pd.DataFrame([_row("1", sku, 8, days_ago=2)]).to_csv(
            self.output_dir / "assemblies_last_30d_2026-01-01_000000.csv",
            index=False)

        with patch("db.get_assembly_component_consumption",
                    side_effect=RuntimeError("connection refused")):
            engine_df, _sale_lines = (
                self.slack_listener._get_data_for_listener())

        self.assertIsNotNone(engine_df)
        row = engine_df[engine_df["SKU"] == sku]
        self.assertFalse(row.empty)
        self.assertGreater(float(row.iloc[0]["effective_units_12mo"]), 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
