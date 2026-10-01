"""Offline checks that a soak's growth failure never skips or hides the final
row differential, and that a differential failure keeps the growth evidence."""

import argparse
import contextlib
import io
import json
from pathlib import Path
import tempfile
import unittest

from soak import Soak, process_memory, reproduction

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


class FinalCheckTests(unittest.TestCase):
    def test_final_metrics_follow_the_drain_and_survive_a_failed_comparison(self):
        soak = Soak.__new__(Soak)
        soak.report, events = {}, []
        soak.process = argparse.Namespace(pid=0)
        soak.wait_materialized = lambda barrier: events.append("drained")
        soak.metrics = lambda: events.append("metrics") or {"flow_materialized_lsn": 9}

        def compare(phase):
            raise AssertionError(f"{phase}: orders differs")

        soak.compare = compare
        with self.assertRaisesRegex(AssertionError, "final: orders differs"):
            soak.final_check()
        self.assertEqual(events, ["drained", "metrics"])
        self.assertEqual(soak.report["final_metrics"], {"flow_materialized_lsn": 9})


class ReproductionTests(unittest.TestCase):
    def test_command_replays_the_soak_with_its_detector_settings(self):
        args = argparse.Namespace(seed=2245799672, duration=1800, writers=3, large_rows=12000, large_interval=15,
                                  format_version=2, binary=Path("target/debug/embrasure-flow"), timeout=600,
                                  sample_seconds=30, verify_every=600, retention_secs=120,
                                  warmup_fraction=0.5, rss_tolerance=0.4, state_tolerance=0.5,
                                  metadata_tolerance=0.25)
        command = reproduction(args)
        self.assertTrue(command.startswith("uv run tests/production/soak.py --seed 2245799672 "))
        for option in ("--duration 1800", "--warmup-fraction 0.5", "--rss-tolerance 0.4",
                       "--state-tolerance 0.5", "--metadata-tolerance 0.25", "--retention-secs 120",
                       "--binary target/debug/embrasure-flow", "--timeout 600"):
            self.assertIn(option, command)


class ProcessMemoryTests(unittest.TestCase):
    def test_linux_rss_components_are_parsed(self):
        proc = Path(self.enterContext(tempfile.TemporaryDirectory()))
        (proc / "42").mkdir()
        (proc / "42" / "status").write_text("Name:\tembrasure-flow\nVmRSS:\t   3072 kB\nRssAnon:\t   2048 kB\n"
                                            "RssFile:\t    1000 kB\nRssShmem:\t      24 kB\nThreads:\t37\n")
        (proc / "42" / "smaps_rollup").write_text("Rss:                3072 kB\nLazyFree:             8 kB\n"
                                                  "AnonHugePages:     2048 kB\n")
        self.assertEqual(process_memory(42, proc), {"rss_anon_bytes": 2048 * 1024, "rss_file_bytes": 1000 * 1024,
                                                    "rss_shmem_bytes": 24 * 1024, "threads": 37,
                                                    "lazy_free_bytes": 8 * 1024,
                                                    "anon_huge_page_bytes": 2048 * 1024})
        self.assertEqual(process_memory(43, proc), {})


if __name__ == "__main__":
    unittest.main()
