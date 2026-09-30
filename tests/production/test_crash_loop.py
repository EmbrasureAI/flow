"""Offline checks for the crash-loop ACK evidence and the soak growth rule."""

import unittest

from crash_loop import TransactionLog
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


if __name__ == "__main__":
    unittest.main()
