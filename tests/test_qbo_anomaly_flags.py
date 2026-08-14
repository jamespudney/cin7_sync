"""Standalone verification tests for db.qbo_monthly_pl_anomaly_months.

Real incident this covers: CIN7's own QuickBooks Online integration
(which posts inventory-relief/COGS journal entries per order) silently
stopped posting after 2026-06-17, with no error surfaced anywhere in
this app. qbo_monthly_pl.py faithfully pulled whatever QuickBooks' own
P&L report said — the sync and dashboard code were both correct — but
because the upstream entries were simply never posted, Section 7
[QuickBooks] Product COGS collapsed from a normal ~$180-220k/month down
to $110,353 (Jun 2026), $15,429 (Jul 2026), and $3,187 (Aug 2026), with
GP% climbing to 77%/97%/99%. It read as a dashboard bug until traced
back to the upstream integration outage directly against QuickBooks'
own JournalEntry records.

This file proves, without any live QBO connection:
  1. That exact real-world sequence (12 normal months, then the three
     collapsed months) gets flagged, using QB's own historical
     Total COGS figures.
  2. Ordinary month-to-month seasonal variation (no real outage) is
     NOT flagged — the threshold has real headroom.
  3. Too little history (cold start) never flags anything.
  4. A flagged month doesn't poison the baseline for the next one
     (the multi-month gap doesn't get "normalised" against itself).

Run directly:
    python3 tests/test_qbo_anomaly_flags.py
or:
    python3 -m unittest tests.test_qbo_anomaly_flags -v
"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPT_DIR))

import db  # noqa: E402


def _qb_by_month(total_cogs_by_month: dict) -> dict:
    return {m: {"total_cogs": v} for m, v in total_cogs_by_month.items()}


class RealIncidentReproductionTests(unittest.TestCase):
    """The exact Jun/Jul/Aug 2026 collapse, from the real qbo_monthly_pl
    Total Cost of Goods Sold figures pulled during the incident."""

    def setUp(self) -> None:
        self.qb_by_month = _qb_by_month({
            "2025-07": 186811.0, "2025-08": 213682.0, "2025-09": 209832.0,
            "2025-10": 218077.0, "2025-11": 167168.0, "2025-12": 190540.0,
            "2026-01": 193626.0, "2026-02": 207910.0, "2026-03": 227156.0,
            "2026-04": 209126.0, "2026-05": 236524.0,
            "2026-06": 112679.76, "2026-07": 15429.47, "2026-08": 3187.38,
        })

    def test_jul_and_aug_flagged(self) -> None:
        flagged = db.qbo_monthly_pl_anomaly_months(self.qb_by_month)
        self.assertIn("2026-07", flagged)
        self.assertIn("2026-08", flagged)

    def test_flagged_months_carry_correct_value_and_baseline(self) -> None:
        flagged = db.qbo_monthly_pl_anomaly_months(self.qb_by_month)
        aug = flagged["2026-08"]
        self.assertAlmostEqual(aug["value"], 3187.38, places=2)
        # Baseline should sit near the normal ~$180-220k range, not be
        # dragged down by June (excluded as its own flagged month).
        self.assertGreater(aug["baseline"], 150000.0)
        self.assertLess(aug["ratio"], 0.1)

    def test_clean_prior_months_never_flagged(self) -> None:
        flagged = db.qbo_monthly_pl_anomaly_months(self.qb_by_month)
        for m in ("2025-10", "2025-11", "2026-03", "2026-05"):
            self.assertNotIn(m, flagged)

    def test_flagged_month_does_not_poison_next_baseline(self) -> None:
        # If June's collapsed value leaked into the baseline used for
        # July, July's baseline would be dragged down toward June's
        # already-low figure instead of the real ~$200k normal range —
        # weakening (or hiding) the July flag entirely.
        flagged = db.qbo_monthly_pl_anomaly_months(self.qb_by_month)
        jul_baseline = flagged["2026-07"]["baseline"]
        self.assertGreater(jul_baseline, 150000.0)


class NoFalsePositiveTests(unittest.TestCase):
    """Ordinary seasonal swings, with no real outage, must not flag."""

    def test_normal_seasonal_variation_not_flagged(self) -> None:
        # +/-30% month-to-month swings, no sustained collapse.
        qb_by_month = _qb_by_month({
            "2025-07": 180000.0, "2025-08": 220000.0, "2025-09": 150000.0,
            "2025-10": 210000.0, "2025-11": 170000.0, "2025-12": 230000.0,
            "2026-01": 160000.0, "2026-02": 200000.0, "2026-03": 190000.0,
        })
        flagged = db.qbo_monthly_pl_anomaly_months(qb_by_month)
        self.assertEqual(flagged, {})

    def test_genuinely_slow_month_within_threshold_not_flagged(self) -> None:
        # A real, non-outage slow month (e.g. a January lull) at 65%
        # of trailing baseline should clear the 60% floor.
        qb_by_month = _qb_by_month({
            "2025-07": 200000.0, "2025-08": 200000.0, "2025-09": 200000.0,
            "2025-10": 200000.0, "2026-01": 130000.0,
        })
        flagged = db.qbo_monthly_pl_anomaly_months(qb_by_month)
        self.assertEqual(flagged, {})

    def test_cold_start_never_flags(self) -> None:
        # Fewer than min_baseline_months of history — nothing to judge
        # against, so even a huge apparent drop should not be flagged.
        qb_by_month = _qb_by_month({
            "2026-06": 200000.0, "2026-07": 2000.0,
        })
        flagged = db.qbo_monthly_pl_anomaly_months(qb_by_month)
        self.assertEqual(flagged, {})

    def test_empty_input_returns_empty(self) -> None:
        self.assertEqual(db.qbo_monthly_pl_anomaly_months({}), {})


class CustomThresholdTests(unittest.TestCase):
    """Sanity-check the tunable parameters behave as documented."""

    def test_custom_category(self) -> None:
        qb_by_month = {
            "2025-07": {"cogs": 100000.0}, "2025-08": {"cogs": 100000.0},
            "2025-09": {"cogs": 100000.0}, "2025-10": {"cogs": 5000.0},
        }
        flagged = db.qbo_monthly_pl_anomaly_months(
            qb_by_month, category="cogs")
        self.assertIn("2025-10", flagged)

    def test_stricter_threshold_catches_more(self) -> None:
        qb_by_month = _qb_by_month({
            "2025-07": 200000.0, "2025-08": 200000.0, "2025-09": 200000.0,
            "2025-10": 140000.0,  # 70% of baseline
        })
        # Default 0.6 threshold: 140k / 200k = 0.70 > 0.6, not flagged.
        self.assertEqual(
            db.qbo_monthly_pl_anomaly_months(qb_by_month), {})
        # An 80% threshold should catch the same month.
        flagged = db.qbo_monthly_pl_anomaly_months(
            qb_by_month, threshold_fraction=0.8)
        self.assertIn("2025-10", flagged)


if __name__ == "__main__":
    unittest.main(verbosity=2)
