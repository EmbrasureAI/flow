"""Offline checks for the crash-loop ACK evidence, failure evidence and the soak growth rule."""

import argparse
import random
import unittest

from crash_loop import CrashLoop, TransactionLog, row_difference
from soak import RESOURCES, sustained_growth


class TransactionLogTests(unittest.TestCase):
    def test_only_certainly_acknowledged_and_unpublished_transactions_violate(self):
        log = TransactionLog()
        log.add(100, 120, "published")
        log.add(200, 220, "acknowledged-unpublished")
        log.add(300, 320, "straddles-ack")
        log.add(400, 420, "unacknowledged")
        # Published through 150: the first transaction's commit is covered.
        self.assertEqual([entry[2] for entry in log.violations(confirmed=320, published=150)],
                         ["acknowledged-unpublished", "straddles-ack"])
        self.assertEqual([entry[2] for entry in log.violations(confirmed=310, published=150)],
                         ["acknowledged-unpublished"])
        # lo < published: the commit record may be inside the published range.
        self.assertEqual(log.violations(confirmed=420, published=401), [])
        self.assertEqual(log.prune(published=300), 2)
        self.assertEqual([entry[2] for entry in log.entries], ["straddles-ack", "unacknowledged"])


class GrowthTests(unittest.TestCase):
    @staticmethod
    def samples(values):
        return [{resource: value for resource in RESOURCES} for value in values]

    def test_steady_or_noisy_resources_pass(self):
        tolerance = dict.fromkeys(RESOURCES, 0.25)
        self.assertEqual(sustained_growth(self.samples([1, 50] + [100, 104, 99, 101, 103, 98] * 3), 2, tolerance), {})
        # Growth that stops after the first window is a warm-up plateau.
        self.assertEqual(sustained_growth(self.samples([100, 100, 200, 200, 200, 200]), 0, tolerance), {})

    def test_sustained_growth_beyond_tolerance_fails(self):
        tolerance = dict.fromkeys(RESOURCES, 0.25)
        growing = sustained_growth(self.samples([100, 110, 120, 130, 140, 150]), 0, tolerance)
        self.assertEqual(set(growing), set(RESOURCES))
        self.assertEqual(growing["rss_bytes"]["window_medians"], [105, 125, 145])
        # Monotonic but within tolerance.
        self.assertEqual(sustained_growth(self.samples([100, 101, 102, 103, 104, 105]), 0, tolerance), {})

    def test_too_few_samples_is_an_error(self):
        with self.assertRaises(AssertionError):
            sustained_growth(self.samples([1, 2, 3, 4, 5]), 0, dict.fromkeys(RESOURCES, 0.25))


class FailedCycleTests(unittest.TestCase):
    def test_row_difference_reports_key_ranges_and_bounded_rows(self):
        expected = [(key, "new" * 40) for key in range(11_000_000, 11_000_020)]
        actual = [row for row in expected if row[0] >= 11_000_005] + [(42, "extra")]
        actual[0] = (actual[0][0], "changed")
        difference = row_difference(actual, expected)
        self.assertEqual((difference["missing"], difference["extra"], difference["changed"]), (5, 1, 1))
        self.assertEqual(difference["missing_ranges"], [[11_000_000, 11_000_004, 5]])
        self.assertEqual(difference["extra_ranges"], [[42, 42, 1]])
        self.assertEqual(difference["changed_keys"], [11_000_005])
        self.assertEqual(len(difference["rows"]), 7)
        self.assertTrue(difference["rows"][0]["postgres"][1].endswith("..."))
        self.assertIsNone(difference["rows"][0]["iceberg"])

    def test_a_failed_cycle_is_kept_with_its_boundaries(self):
        loop = CrashLoop.__new__(CrashLoop)
        loop.args = argparse.Namespace(min_kill=0.5, max_kill=10)
        loop.rng = random.Random(1)
        loop.report = {}
        loop.arm = lambda fault, window: {"fault": fault}

        def run_cycle(number, window, fault, details):
            details["at_ms"]["killed"] = details["at_ms"]["armed"] + 1
            raise AssertionError(f"cycle-{number}: orders differs")

        loop.run_cycle = run_cycle
        with self.assertRaisesRegex(AssertionError, "cycle-61: orders differs"):
            loop.cycle(61)
        failed = loop.report["failed_cycle"]
        self.assertEqual(failed["cycle"], 61)
        self.assertEqual(set(failed["at_ms"]), {"armed", "killed"})
        self.assertIn("orders differs", failed["traceback"])


if __name__ == "__main__":
    unittest.main()
