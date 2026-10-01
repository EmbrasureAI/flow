"""Offline checks that a soak's growth failure never skips or hides the final
row differential, and that a differential failure keeps the growth evidence."""

import argparse
import contextlib
import io
import json
from pathlib import Path
import tempfile
import unittest

from soak import Soak

GROWTH = {"rss_bytes": {"window_medians": [100.0, 130.0, 160.0], "limit": 125.0}}


class Closed:
    def close(self):
        pass


class SoakExecutionTests(unittest.TestCase):
    def soak(self, growth, final_error=None):
        directory = Path(self.enterContext(tempfile.TemporaryDirectory()))
        soak = Soak.__new__(Soak)  # no services: only the phase sequence runs
        soak.args = argparse.Namespace(seed=7)
        soak.directory = directory
        soak.report = {"phases": [], "passed": False}
        soak.workload = soak.proxy = None
        soak.pg = soak.duck = Closed()
        soak.calls = []
        for name in ("seed", "initialize", "handoff"):
            setattr(soak, name, lambda name=name: soak.calls.append(name) or {})
        soak.start = soak.stop = lambda: None
        soak.check_worker_panics = lambda: soak.calls.append("panics")

        def run_soak():
            soak.calls.append("soak")
            soak.report["growth"] = growth
            return {"growing": sorted(growth)}

        def final_check():
            soak.calls.append("final")
            if final_error:
                raise AssertionError(final_error)
            return {"orders": {"rows": 1}}

        soak.soak, soak.final_check = run_soak, final_check
        return soak

    def written(self, soak):
        return json.loads((soak.directory / "report.json").read_text())

    def test_growth_is_raised_after_a_passing_final_differential(self):
        soak = self.soak(GROWTH)
        with self.assertRaisesRegex(AssertionError, "sustained resource growth"):
            soak.execute()
        self.assertEqual(soak.calls[-2:], ["final", "panics"])
        report = self.written(soak)
        self.assertIn("final-differential", [phase["name"] for phase in report["phases"]])
        self.assertFalse(report["passed"])
        self.assertEqual(report["growth"], GROWTH)
        self.assertIn("sustained resource growth", report["failure"])

    def test_final_differential_failure_keeps_growth_evidence(self):
        soak = self.soak(GROWTH, final_error="final: orders differs")
        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr), self.assertRaisesRegex(AssertionError, "orders differs"):
            soak.execute()
        report = self.written(soak)
        self.assertFalse(report["passed"])
        self.assertIn("orders differs", report["failure"])
        self.assertEqual(report["growth"], GROWTH)
        self.assertIn("also: sustained resource growth", stderr.getvalue())

    def test_steady_soak_passes_after_the_final_differential(self):
        soak = self.soak({})
        soak.execute()
        self.assertEqual(soak.calls, ["seed", "initialize", "handoff", "soak", "final", "panics"])
        self.assertTrue(self.written(soak)["passed"])


if __name__ == "__main__":
    unittest.main()
