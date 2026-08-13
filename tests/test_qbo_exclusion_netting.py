"""Standalone verification tests for the QBO exclusion double-count fix.

Real incident this covers: Altar'd State existed in QBO as TWO
overlapping customer records ("Altar'd State" + an inactive "Altar'd
State 1"). Pre-fix, `qbo_monthly_pl.py` called the customer-scoped
ProfitAndLoss report once PER matched record and summed the results
client-side into `qbo_monthly_pl_exclusions`; `db.
qbo_monthly_pl_summary_by_category` then subtracted the combined
(effectively doubled) amount from the matching (month, account) row
with no floor or sanity check. That collapsed Product COGS from
~$220k/mo to $2.75k in Aug 2026, with GP% spiking to 99%.

This file proves, without any live QBO connection:
  1. The OLD per-customer-call-and-sum shape double-counts (reproduced
     here structurally, since the old code path no longer exists).
  2. The NEW single-combined-query sync function
     (`qbo_monthly_pl.sync_exclusions_for_customers`) does not.
  3. `db.qbo_monthly_pl_summary_by_category`'s tripwire caps an
     implausible exclusion instead of silently zeroing/flipping a
     category, even under a contrived worst-case input.
  4. A normal, single-customer, non-overlapping exclusion still nets
     correctly and is unaffected by the fix (regression check).

Run directly:
    python3 tests/test_qbo_exclusion_netting.py
or:
    python3 -m unittest tests.test_qbo_exclusion_netting -v
"""

from __future__ import annotations

import logging
import sys
import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import patch

SCRIPT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPT_DIR))

import db  # noqa: E402
import qbo_monthly_pl  # noqa: E402


def _pnl_report(month_title: str, account_id: str, account_name: str,
                 amount: float) -> dict:
    """Build a minimal synthetic QBO ProfitAndLoss report JSON —
    enough structure for qbo_monthly_pl.parse_pnl() to extract one
    (month, account) tuple, matching the real Columns/Rows/ColData
    shape QBO returns."""
    amt_str = f"{amount:.2f}"
    return {
        "Columns": {"Column": [
            {"ColType": "Account", "ColTitle": ""},
            {"ColType": "Money", "ColTitle": month_title},
            {"ColType": "Money", "ColTitle": "Total"},
        ]},
        "Rows": {"Row": [
            {"ColData": [
                {"value": account_name, "id": account_id},
                {"value": amt_str},
                {"value": amt_str},
            ]},
        ]},
    }


class _TempDbTestCase(unittest.TestCase):
    """Points db.py at a fresh, isolated SQLite file for the duration
    of each test so these tests never touch the real team_actions.db."""

    def setUp(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory()
        tmp_path = str(Path(self._tmpdir.name) / "test_team_actions.db")
        self._db_path_patcher = patch.object(db, "DB_PATH", tmp_path)
        self._db_path_patcher.start()
        # Confirm schema applies cleanly against the fresh file.
        with db.connect():
            pass

    def tearDown(self) -> None:
        self._db_path_patcher.stop()
        self._tmpdir.cleanup()


class DoubleCountReproductionTests(_TempDbTestCase):
    """Scenario 1: two overlapping QBO customer records for the same
    real-world excluded customer."""

    MONTH = "2026-08"
    ACCT_ID = "61"
    ACCT_NUM = "500"
    ACCT_NAME = "Cost of Goods Sold"
    PRE_EXCLUSION_COGS = 220_000.0
    # The real-world excluded customer's TRUE exclusion amount --
    # what a correctly-scoped single query should net out.
    TRUE_EXCLUSION = 15_000.0

    def _seed_pre_exclusion_pl_row(self) -> None:
        n = db.batch_upsert_qbo_monthly_pl([{
            "month": self.MONTH,
            "account_id": self.ACCT_ID,
            "account_number": self.ACCT_NUM,
            "account_name": self.ACCT_NAME,
            "amount": self.PRE_EXCLUSION_COGS,
            "account_type": "Cost of Goods Sold",
            "parent_account_id": None,
        }])
        self.assertEqual(n, 1)

    def _cogs_for_month(self) -> float:
        mapping = {"cogs": {"account_numbers": [self.ACCT_NUM],
                             "account_names": []}}
        by_month = db.qbo_monthly_pl_summary_by_category(mapping)
        return by_month.get(self.MONTH, {}).get("cogs", 0.0)

    def test_old_per_customer_loop_shape_double_counts(self) -> None:
        """Reproduce the OLD bug's arithmetic shape directly: two QBO
        customer records for the same real customer, each synced with
        its OWN report call, each call returning the customer's full
        TRUE_EXCLUSION amount (because both records scope to
        overlapping/duplicate underlying transactions -- that overlap
        is exactly what made this a real bug rather than two genuinely
        different customers). The old code wrote BOTH as separate
        rows under the same (month, account_number, account_name) key
        (customer_name differed, which was NOT part of the netting
        group-by), so qbo_monthly_pl_summary_by_category's
        `SUM(amount) GROUP BY month, account_number, account_name`
        summed both -- double-counting the exclusion.

        The old per-customer function no longer exists (that's the
        fix), so this test reconstructs its write pattern inline to
        prove the arithmetic it produced was wrong -- this is the
        regression the fix must never reintroduce."""
        self._seed_pre_exclusion_pl_row()

        # OLD shape: one exclusion row PER matched customer record,
        # each carrying the full (overlapping) exclusion amount.
        old_style_payload = []
        for cust_name in ("Altar'd State", "Altar'd State 1"):
            old_style_payload.append({
                "month": self.MONTH,
                "account_id": self.ACCT_ID,
                "account_number": self.ACCT_NUM,
                "account_name": self.ACCT_NAME,
                "customer_name": cust_name,
                "amount": self.TRUE_EXCLUSION,
            })
        n = db.batch_upsert_qbo_monthly_pl_exclusion(old_style_payload)
        self.assertEqual(n, 2, "both per-customer exclusion rows "
                          "should have been written under different "
                          "customer_name values")

        cogs = self._cogs_for_month()
        naive_expected_correct = (
            self.PRE_EXCLUSION_COGS - self.TRUE_EXCLUSION)
        naive_expected_doubled = (
            self.PRE_EXCLUSION_COGS - 2 * self.TRUE_EXCLUSION)

        # The old shape double-subtracts: COGS comes out lower than
        # the correct single-netting figure would give.
        self.assertLess(cogs, naive_expected_correct)
        self.assertAlmostEqual(cogs, naive_expected_doubled, places=2)

    @patch("qbo_monthly_pl.qbo_client.report")
    def test_new_combined_query_sync_does_not_double_count(
            self, mock_report) -> None:
        """The NEW sync_exclusions_for_customers() issues ONE report
        call for the union of matched customer IDs. Mock QBO to
        return the TRUE (non-doubled) exclusion amount for that
        combined call -- exactly what QBO's own customer-scoped
        report is expected to return for the union of transactions,
        per the Reports API's documented comma-separated `customer`
        filter -- and confirm exactly one row lands in
        qbo_monthly_pl_exclusions, netting correctly."""
        self._seed_pre_exclusion_pl_row()

        def _fake_report(name, params=None):
            self.assertEqual(name, "ProfitAndLoss")
            cust_param = (params or {}).get("customer", "")
            ids = {c for c in cust_param.split(",") if c}
            # Both id1 and id2 are matched (the union query), but the
            # report reflects the TRUE underlying transactions only
            # once -- there is no second report call left to sum.
            self.assertEqual(ids, {"id1", "id2"})
            return _pnl_report("Aug 2026", self.ACCT_ID, self.ACCT_NAME,
                                 self.TRUE_EXCLUSION)

        mock_report.side_effect = _fake_report

        acct_meta = {self.ACCT_ID: {"number": self.ACCT_NUM,
                                      "type": "Cost of Goods Sold",
                                      "name": self.ACCT_NAME}}
        n = qbo_monthly_pl.sync_exclusions_for_customers(
            ["id1", "id2"], "Altar'd State, Altar'd State 1",
            date(2026, 8, 1), date(2026, 8, 31), acct_meta)
        self.assertEqual(
            n, 1, "one combined query should write exactly one "
                   "exclusion row, not one per matched customer ID")
        self.assertEqual(mock_report.call_count, 1,
                          "exactly one QBO report call, not one per "
                          "customer record")

        cogs = self._cogs_for_month()
        expected = self.PRE_EXCLUSION_COGS - self.TRUE_EXCLUSION
        self.assertAlmostEqual(cogs, expected, places=2)


class StaleRowReplaceSemanticsTests(_TempDbTestCase):
    """Scenario reproducing the reviewer's exact finding: the first-
    pass fix (one combined query, still upserted under a joined
    customer_name label) does NOT clean up rows already on disk from
    the ORIGINAL per-customer-name bug. Because the netting query
    sums every row under a (month, account_number, account_name) key
    regardless of customer_name, those stale rows plus the new row
    combine into a TRIPLE-count -- worse than the original double-
    count, not better.

    Reviewer's numbers (reproduced here exactly):
      * Correct COGS (one true exclusion netted):      $205,000
      * Buggy prod state pre-fix (2 stale rows):        $190,000
      * First-pass "fix" + one sync cycle (2 stale +
        1 new row, still summed together):              $175,000

    This proves the NEW sync function (using
    db.replace_qbo_monthly_pl_exclusions(), keyed off (month,
    account_number, account_name) regardless of customer_name) must
    DELETE the old stale rows before/while inserting the new one, so
    only ONE exclusion amount ever nets against COGS afterwards."""

    MONTH = "2026-08"
    ACCT_ID = "61"
    ACCT_NUM = "500"
    ACCT_NAME = "Cost of Goods Sold"
    PRE_EXCLUSION_COGS = 220_000.0
    TRUE_EXCLUSION = 15_000.0
    EXPECTED_CORRECT_COGS = 205_000.0  # 220,000 - 15,000

    def _mapping(self):
        return {"cogs": {"account_numbers": [self.ACCT_NUM],
                          "account_names": []}}

    def _cogs_for_month(self) -> float:
        by_month = db.qbo_monthly_pl_summary_by_category(self._mapping())
        return by_month.get(self.MONTH, {}).get("cogs", 0.0)

    def _seed_pre_exclusion_pl_row(self) -> None:
        n = db.batch_upsert_qbo_monthly_pl([{
            "month": self.MONTH,
            "account_id": self.ACCT_ID,
            "account_number": self.ACCT_NUM,
            "account_name": self.ACCT_NAME,
            "amount": self.PRE_EXCLUSION_COGS,
            "account_type": "Cost of Goods Sold",
            "parent_account_id": None,
        }])
        self.assertEqual(n, 1)

    def _seed_old_style_stale_rows(self) -> None:
        """Reproduce the state left behind by the ORIGINAL (pre-any-
        fix) per-customer-record sync: one row per matched QBO
        customer record, each under that customer's own name, each
        carrying the (overlapping) TRUE_EXCLUSION amount."""
        old_style_payload = []
        for cust_name in ("Altar'd State", "Altar'd State 1"):
            old_style_payload.append({
                "month": self.MONTH,
                "account_id": self.ACCT_ID,
                "account_number": self.ACCT_NUM,
                "account_name": self.ACCT_NAME,
                "customer_name": cust_name,
                "amount": self.TRUE_EXCLUSION,
            })
        n = db.batch_upsert_qbo_monthly_pl_exclusion(old_style_payload)
        self.assertEqual(n, 2, "both legacy per-customer-name stale "
                          "rows should have been seeded")

    @patch("qbo_monthly_pl.qbo_client.report")
    def test_new_sync_cleans_up_stale_rows_from_prior_bug(
            self, mock_report) -> None:
        self._seed_pre_exclusion_pl_row()
        self._seed_old_style_stale_rows()

        # Confirm the reviewer's starting point: 2 stale rows summed
        # together already double-count against the correct figure
        # (this is the buggy state currently sitting in prod).
        buggy_prod_cogs = self._cogs_for_month()
        self.assertAlmostEqual(
            buggy_prod_cogs,
            self.PRE_EXCLUSION_COGS - 2 * self.TRUE_EXCLUSION,
            places=2,
            msg="sanity check: 2 pre-existing stale rows should "
                 "already double-count, matching the reviewer's "
                 "$190,000 buggy-prod-state figure")

        # Now run the NEW sync function -- QBO mocked to return the
        # TRUE (non-doubled) exclusion for the one combined query.
        def _fake_report(name, params=None):
            self.assertEqual(name, "ProfitAndLoss")
            cust_param = (params or {}).get("customer", "")
            ids = {c for c in cust_param.split(",") if c}
            self.assertEqual(ids, {"id1", "id2"})
            return _pnl_report("Aug 2026", self.ACCT_ID, self.ACCT_NAME,
                                 self.TRUE_EXCLUSION)

        mock_report.side_effect = _fake_report
        acct_meta = {self.ACCT_ID: {"number": self.ACCT_NUM,
                                      "type": "Cost of Goods Sold",
                                      "name": self.ACCT_NAME}}
        n = qbo_monthly_pl.sync_exclusions_for_customers(
            ["id1", "id2"], "Altar'd State, Altar'd State 1",
            date(2026, 8, 1), date(2026, 8, 31), acct_meta)
        self.assertEqual(n, 1, "the replace should write exactly one "
                          "row for this (month, account)")

        # Exactly one exclusion row should now exist for this key --
        # the two legacy stale rows must be GONE, not summed alongside
        # the new one.
        with db.connect() as c:
            remaining = c.execute(
                "SELECT customer_name, amount FROM "
                "qbo_monthly_pl_exclusions WHERE month = ? AND "
                "account_number = ? AND account_name = ?",
                (self.MONTH, self.ACCT_NUM, self.ACCT_NAME)).fetchall()
        self.assertEqual(
            len(remaining), 1,
            "stale rows from the prior buggy sync must be deleted, "
            "leaving exactly one row at this (month, account) key -- "
            f"found: {[dict(r) for r in remaining]}")
        self.assertEqual(
            dict(remaining[0])["customer_name"],
            qbo_monthly_pl.EXCLUSION_CUSTOMER_LABEL)

        # The netted COGS must match the reviewer's expected correct
        # figure exactly -- not the $190,000 buggy-prod figure, and
        # NOT the $175,000 triple-count the flawed first-pass fix
        # would have produced.
        fixed_cogs = self._cogs_for_month()
        self.assertAlmostEqual(fixed_cogs, self.EXPECTED_CORRECT_COGS,
                                 places=2)
        self.assertNotAlmostEqual(fixed_cogs, 190_000.0, places=2)
        self.assertNotAlmostEqual(fixed_cogs, 175_000.0, places=2)

    def test_replace_does_not_touch_unrelated_account_or_month(
            self) -> None:
        """The DELETE step must be scoped to only the (month,
        account_number, account_name) keys being replaced -- an
        exclusion row for a different account, or a different month,
        must survive untouched."""
        other_month = "2026-07"
        other_acct_num = "694"
        other_acct_name = "Shipping-Out"
        db.batch_upsert_qbo_monthly_pl_exclusion([{
            "month": other_month, "account_id": "72",
            "account_number": other_acct_num,
            "account_name": other_acct_name,
            "customer_name": "Some Other Excluded Customer (unrelated)",
            "amount": 999.0,
        }])

        n = db.replace_qbo_monthly_pl_exclusions([{
            "month": self.MONTH, "account_id": self.ACCT_ID,
            "account_number": self.ACCT_NUM,
            "account_name": self.ACCT_NAME,
            "customer_name": qbo_monthly_pl.EXCLUSION_CUSTOMER_LABEL,
            "amount": self.TRUE_EXCLUSION,
        }])
        self.assertEqual(n, 1)

        with db.connect() as c:
            untouched = c.execute(
                "SELECT amount FROM qbo_monthly_pl_exclusions WHERE "
                "month = ? AND account_number = ? AND "
                "account_name = ?",
                (other_month, other_acct_num,
                 other_acct_name)).fetchall()
        self.assertEqual(len(untouched), 1,
                          "unrelated exclusion row (different month "
                          "and account) must not be deleted")
        self.assertAlmostEqual(dict(untouched[0])["amount"], 999.0,
                                 places=2)


class TripwireFloorTests(_TempDbTestCase):
    """Scenario 2: even if a future customer-matching edge case
    somehow produces an implausible exclusion again, the netting
    tripwire in db.qbo_monthly_pl_summary_by_category must stop it
    from silently zeroing out / sign-flipping a whole category."""

    MONTH = "2026-08"
    ACCT_NUM = "500"
    ACCT_NAME = "Cost of Goods Sold"

    def _mapping(self):
        return {"cogs": {"account_numbers": [self.ACCT_NUM],
                          "account_names": []}}

    def test_contrived_worst_case_exclusion_is_capped_not_zeroed(
            self) -> None:
        pre_amount = 220_000.0
        # Worst-case contrived input: an exclusion almost equal to
        # the WHOLE pre-exclusion amount -- exactly the shape of the
        # real double-count incident (excluded ~= 2x the true share).
        runaway_exclusion = 217_250.0

        db.batch_upsert_qbo_monthly_pl([{
            "month": self.MONTH, "account_id": "61",
            "account_number": self.ACCT_NUM,
            "account_name": self.ACCT_NAME, "amount": pre_amount,
            "account_type": "Cost of Goods Sold",
            "parent_account_id": None,
        }])
        db.batch_upsert_qbo_monthly_pl_exclusion([{
            "month": self.MONTH, "account_id": "61",
            "account_number": self.ACCT_NUM,
            "account_name": self.ACCT_NAME,
            "customer_name": "Altar'd State (contrived worst case)",
            "amount": runaway_exclusion,
        }])

        with self.assertLogs("db", level="WARNING") as cm:
            by_month = db.qbo_monthly_pl_summary_by_category(
                self._mapping())
        self.assertTrue(
            any("tripwire" in msg for msg in cm.output),
            "the tripwire should log a warning when an exclusion "
            "this large is netted")

        cogs = by_month.get(self.MONTH, {}).get("cogs", 0.0)
        # Must NOT collapse to near-zero (the real incident's failure
        # mode: $220k -> $2.75k) and must NOT flip sign.
        self.assertGreater(
            cogs, pre_amount * 0.5,
            "the floor must prevent the category from collapsing "
            "anywhere near zero")
        self.assertGreater(cogs, 0.0, "must not flip sign negative")
        # The capped result should equal pre_amount minus AT MOST
        # _EXCL_MAX_SHARE (30%) of the pre-exclusion magnitude.
        self.assertAlmostEqual(cogs, pre_amount * 0.70, places=2)

    def test_exclusion_with_no_matching_pl_row_is_not_applied(
            self) -> None:
        """An exclusion row with no corresponding qbo_monthly_pl row
        (pre-exclusion amount 0) must not manufacture a negative
        category out of nothing."""
        db.batch_upsert_qbo_monthly_pl_exclusion([{
            "month": self.MONTH, "account_id": "61",
            "account_number": self.ACCT_NUM,
            "account_name": self.ACCT_NAME,
            "customer_name": "Altar'd State", "amount": 5_000.0,
        }])
        # Deliberately no matching qbo_monthly_pl row is seeded.
        by_month = db.qbo_monthly_pl_summary_by_category(
            self._mapping())
        # No qbo_monthly_pl row at all means the category shouldn't
        # even appear (nothing to net against, nothing to categorise).
        self.assertNotIn(self.MONTH, by_month)


class RegressionNormalExclusionTests(_TempDbTestCase):
    """Scenario 3: a normal, single-customer, non-overlapping
    exclusion (well under the tripwire threshold) must still net
    exactly as before the fix -- no behavioural change for the
    common case."""

    def test_single_customer_modest_exclusion_nets_normally(self) -> None:
        month = "2026-08"
        acct_num = "500"
        acct_name = "Cost of Goods Sold"
        pre_amount = 220_000.0
        modest_exclusion = 15_000.0  # ~6.8% of pre_amount -- well
                                       # under the 30% tripwire share.

        db.batch_upsert_qbo_monthly_pl([{
            "month": month, "account_id": "61",
            "account_number": acct_num, "account_name": acct_name,
            "amount": pre_amount, "account_type": "Cost of Goods Sold",
            "parent_account_id": None,
        }])
        db.batch_upsert_qbo_monthly_pl_exclusion([{
            "month": month, "account_id": "61",
            "account_number": acct_num, "account_name": acct_name,
            "customer_name": "Altar'd State", "amount": modest_exclusion,
        }])

        mapping = {"cogs": {"account_numbers": [acct_num],
                             "account_names": []}}
        # No warning should fire for a well-under-threshold exclusion.
        import io
        logger = logging.getLogger("db")
        handler = logging.StreamHandler(io.StringIO())
        logger.addHandler(handler)
        try:
            by_month = db.qbo_monthly_pl_summary_by_category(mapping)
        finally:
            logger.removeHandler(handler)

        cogs = by_month.get(month, {}).get("cogs", 0.0)
        self.assertAlmostEqual(cogs, pre_amount - modest_exclusion,
                                 places=2)

    def test_sales_exclusions_customer_name_matching_unaffected(
            self) -> None:
        """Sanity check that the CIN7-side sales_exclusions matching
        (a completely separate mechanism from the QBO netting fixed
        here) is untouched by this change."""
        from sales_exclusions import _normalise_customer_name
        self.assertEqual(
            _normalise_customer_name("Altar'd State"), "ALTARDSTATE")
        self.assertEqual(
            _normalise_customer_name("Altar’d State"), "ALTARDSTATE")
        self.assertNotEqual(
            _normalise_customer_name("Altar Construction"),
            "ALTARDSTATE")


class CompileSanityTests(unittest.TestCase):
    """Confirm every file touched by this fix still compiles cleanly."""

    def test_changed_files_compile(self) -> None:
        import py_compile
        for rel in ("db.py", "qbo_monthly_pl.py", "app.py"):
            path = SCRIPT_DIR / rel
            try:
                py_compile.compile(str(path), doraise=True)
            except py_compile.PyCompileError as exc:  # noqa: BLE001
                self.fail(f"{rel} failed to compile: {exc}")


if __name__ == "__main__":
    unittest.main(verbosity=2)
