"""Service-free subprocess checks of actual runner lifetime methods."""

import ast
import json
import os
from pathlib import Path
import socket
import signal
import sqlite3
from contextlib import closing
import subprocess
import sys
import tempfile
import threading
import time
import tomllib
import traceback
from types import SimpleNamespace
import unittest
import math

from benchmark_store import (BenchmarkStore, DenseKeys, PAGE_ROWS, QUEUE_ITEMS,
                             mutation_counts, clock_assessment, catalog_slo_met)
from process_lifecycle import arm_workload, disarm_workload, run_supervised, write_report
from provenance import load_build_manifest


DIRECTORY = Path(__file__).resolve().parent


def runner_class(filename, name, base, methods):
    # Compile real lifecycle/workload code without importing the optional database
    # and reader libraries. Only service operations are replaced by inert doubles.
    tree = ast.parse((DIRECTORY / filename).read_text())
    node = next(node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == name)
    node.bases = [ast.Name(id="FixtureBase", ctx=ast.Load())]
    node.body = [method for method in node.body if isinstance(method, ast.FunctionDef) and method.name in methods]
    module = ast.fix_missing_locations(ast.Module(body=[node], type_ignores=[]))
    scope = globals() | {"FixtureBase": base, "__file__": str(DIRECTORY / filename)}
    exec(compile(module, str(DIRECTORY / filename), "exec"), scope)
    return scope[name]


def fixture(mode, directory):
    class Base:
        def __init__(self, args):
            self.args, self.directory = args, args.artifacts
            directory.mkdir()
            self.report = {"passed": False, "phases": []}
            self.store = BenchmarkStore(directory / "evidence.sqlite3")
            self.store.sample("retained", {"value": 42})
            self.store.flush()
            self.process = object() if mode == "cleanup" else None
            self.pg = self.duck = self.compactor = None
            if mode == "constructor":
                raise RuntimeError("constructor rejected after recorder acquisition")

        def stop(self):
            raise RuntimeError("cleanup stop rejected")

    runner = runner_class("benchmark.py", "Benchmark", Base,
                          {"__init__", "close_run", "execute", "execute_phases", "workload"})
    args = SimpleNamespace(artifacts=directory, build_manifest=directory / "invalid-manifest.json",
                           duration=.05, warmup=0, timeout=.2, rate=200, transaction_rows=10,
                           writers=1, initial_rows=100, reader_poll_seconds=.01)
    if mode == "cleanup":
        runner.execute_phases = lambda self: None
    instance = None
    try:
        instance = runner(args)
        if mode in ("benchmark-stall", "source-stall"):
            instance.names = ["events_0"]
            instance.key_pools = [{}]
            instance.threads = []
            instance.writer_connections = [None]
            instance.writer_lock = threading.Lock()
            instance.stop_writers = threading.Event()
            instance.errors = []
            instance.calibrate_clock = lambda: {"postgres_minus_client_us": 0, "roundtrip_us": 10}
            instance.alive = lambda: None
            instance.observe = lambda cursors: None
            instance.sample_resources = lambda: None
            instance.report["failure"] = "prior diagnostic retained"
            receiver, peer = socket.socketpair()
            instance.writer = lambda *unused: receiver.recv(1)
            write_report(directory / "worker.json", {"pid": os.getpid()})
            if mode == "source-stall":
                source = runner_class("source_capacity.py", "SourceCapacity", runner,
                                      {"source_workload", "join_writers"})
                instance.__class__ = source
                instance.source_workload()
            else:
                instance.workload()
            raise AssertionError("stalled request returned")
        instance.execute()
    finally:
        write_report(directory / "threads.json", {
            "recorder_alive": any(t.name == "benchmark-evidence" for t in threading.enumerate())})


# Expose the same clock used by the compiled workload, without service imports.
def micros():
    return time.time_ns() // 1000


class BenchmarkLifecycleTest(unittest.TestCase):
    def test_fixture_compaction_overrides_keep_one_valid_section_and_table_contracts(self):
        renderer = runner_class("../local/run.py", "Run", object, {"configure", "set_compaction_policy"})
        with tempfile.TemporaryDirectory() as directory:
            run = renderer()
            run.directory, run.name = Path(directory), "fixture"
            run.args = SimpleNamespace(catalog_uri="http://localhost:8181", warehouse="s3://warehouse/",
                                       s3_endpoint="http://localhost:9000", postgres_url="postgres://localhost/test")
            run.configure()
            original = tomllib.loads(run.config.read_text())
            run.set_compaction_policy({"oldest_l0_soft_ms": 1000, "delete_files_soft": 128})
            updated = tomllib.loads(run.config.read_text())
            self.assertEqual(updated.pop("compaction"), original.pop("compaction") |
                             {"oldest_l0_soft_ms": 1000, "delete_files_soft": 128})
            self.assertEqual(updated, original)

    def test_failures_close_real_recorder_and_exit(self):
        for mode, expected in (("manifest", "invalid-manifest"),
                               ("cleanup", "cleanup stop rejected"),
                               ("constructor", "constructor rejected")):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as temporary:
                directory = Path(temporary) / "run"
                result = subprocess.run([sys.executable, __file__, "--fixture", mode, str(directory)],
                                        capture_output=True, text=True, timeout=5)
                self.assertNotEqual(result.returncode, 0, result.stdout)
                report = json.loads((directory / "report.json").read_text())
                self.assertIn(expected, report["failure"])
                self.assertFalse(report["passed"])
                self.assertFalse(json.loads((directory / "threads.json").read_text())["recorder_alive"])
                with closing(sqlite3.connect(directory / "evidence.sqlite3")) as db:
                    self.assertEqual(db.execute("SELECT count(*) FROM samples").fetchone()[0], 1)

    def test_stalled_network_workloads_exit_and_retain_evidence(self):
        for mode in ("benchmark-stall", "source-stall"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as temporary:
                directory = Path(temporary) / "run"
                result = subprocess.run([sys.executable, __file__, "--supervisor", mode, str(directory)],
                                        capture_output=True, text=True, timeout=5)
                self.assertEqual(result.returncode, 124, result.stderr)
                report = json.loads((directory / "report.json").read_text())
                self.assertIn("workload hard deadline", report["failure"])
                self.assertEqual(report["worker_failure"], "prior diagnostic retained")
                self.assertTrue(report["evidence_incomplete"])
                self.assertFalse(report["passed"])
                pid = json.loads((directory / "worker.json").read_text())["pid"]
                with self.assertRaises(ProcessLookupError):
                    os.kill(pid, 0)
                with closing(sqlite3.connect(directory / "evidence.sqlite3")) as db:
                    self.assertEqual(db.execute("SELECT count(*) FROM samples").fetchone()[0], 1)

    def test_sigterm_reaps_the_isolated_worker_and_records_failure(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary) / "run"
            process = subprocess.Popen([sys.executable, __file__, "--supervisor", "benchmark-stall", str(directory)],
                                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            try:
                deadline = time.monotonic() + 3
                while not (directory / "worker.json").exists():
                    self.assertIsNone(process.poll())
                    if time.monotonic() >= deadline:
                        self.fail("supervised worker did not start")
                    time.sleep(.005)
                pid = json.loads((directory / "worker.json").read_text())["pid"]
                process.send_signal(signal.SIGTERM)
                self.assertEqual(process.wait(timeout=5), 143)
                with self.assertRaises(ProcessLookupError):
                    os.kill(pid, 0)
                report = json.loads((directory / "report.json").read_text())
                self.assertIn("SIGTERM", report["failure"])
                self.assertFalse(report["passed"])
            finally:
                if process.poll() is None:
                    process.kill()
                    process.wait(timeout=5)

    def test_outer_deadline_covers_setup_without_report(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary) / "run"
            result = run_supervised([sys.executable, "-c", "import time; time.sleep(60)"], directory, .1)
            self.assertEqual(result.returncode, 124)
            report = json.loads((directory / "report.json").read_text())
            self.assertIn("run hard deadline", report["failure"])
            self.assertFalse(report["passed"])

    def test_exact_mutation_mix_includes_probe(self):
        for profile, rows, expected in (("mixed", 90, (63, 9, 18)),
                                        ("mixed", 100, (70, 10, 20)),
                                        ("update90", 90, (81, 0, 9))):
            with self.subTest(profile=profile, rows=rows):
                counts = mutation_counts(profile, rows)
                self.assertEqual(counts, expected)
                self.assertEqual(sum(counts), rows)
                self.assertGreaterEqual(counts[2], 1)  # the reserved immutable insertion

    def test_v3_inventory_counts_shared_puffin_once(self):
        runner = runner_class("benchmark.py", "Benchmark", object, {"inventory"})
        instance = runner()
        instance.args = SimpleNamespace(format_version=3)
        instance.names = ["events"]
        instance.table = lambda name: {"metadata": {
            "format-version": 3, "current-snapshot-id": 1,
            "snapshots": [{"snapshot-id": 1, "manifest-list": "list", "summary": {}}]}}
        records = [{"status": 1, "data_file": {
            "content": 1, "file_path": "shared.puffin", "file_format": "PUFFIN",
            "file_size_in_bytes": 1024, "record_count": 3,
            "referenced_data_file": target, "content_offset": offset,
            "content_size_in_bytes": 100}} for target, offset in (("a.parquet", 4), ("b.parquet", 104))]
        instance.avro = lambda path: [{"manifest_path": "manifest"}] if path == "list" else records
        result = instance.inventory()["events"]
        self.assertEqual((result["deletion_vectors"], result["delete_objects"], result["delete_bytes"],
                          result["deletion_vector_bytes"]), (2, 1, 1024, 200))
        records[1]["data_file"]["referenced_data_file"] = "a.parquet"
        with self.assertRaisesRegex(AssertionError, "multiple DVs"):
            instance.inventory()

    def test_clock_calibration_brackets_tail_observations(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            runner = runner_class("benchmark.py", "Benchmark", object, {"workload"})
            instance = runner()
            instance.args = SimpleNamespace(duration=.01, warmup=0, timeout=1, rate=100,
                                            transaction_rows=1, writers=0, initial_rows=0,
                                            reader_poll_seconds=.001)
            instance.directory, instance.names, instance.report = directory, [], {}
            instance.stop_writers = threading.Event()
            instance.writer_lock = threading.Lock()
            instance.writer_connections = []
            events = []
            instance.calibrate_clock = lambda: events.append("calibrate") or {}
            instance.alive = instance.sample_resources = lambda: None
            instance.observe = lambda cursors: events.append("observe")
            instance.latencies = lambda: None
            instance.store = BenchmarkStore(directory / "evidence.sqlite3")
            instance.store.all_committed_observed = lambda: events.count("observe") == 2
            try:
                instance.workload()
                self.assertEqual(events, ["calibrate", "observe", "observe", "calibrate"])
            finally:
                instance.store.close()

    def test_clock_drift_and_rtt_cannot_hide_latency_failure(self):
        before = {"postgres_minus_client_us": 0, "roundtrip_us": 200}
        for after, valid in ((before, True),
                             ({"postgres_minus_client_us": 2_000_000, "roundtrip_us": 200}, False),
                             ({"postgres_minus_client_us": 0, "roundtrip_us": 200_000}, False)):
            with self.subTest(after=after):
                clock = clock_assessment(before, after, 50_000)
                self.assertEqual(clock["latency_qualification_available"], valid)
                # Under the drift case a true 1500ms latency becomes 500ms with
                # average correction, reproducing the original false pass.
                reported_ms = (1_500_000 - after["postgres_minus_client_us"] + clock["correction_us"]) / 1000
                if after["postgres_minus_client_us"]:
                    self.assertEqual(reported_ms, 500)
                percentiles = {"p95_ms": 500, "p99_ms": 700}
                self.assertEqual(catalog_slo_met(percentiles, clock, 1000, 2000), valid)
        clock = clock_assessment(before, before, 50_000)
        self.assertFalse(catalog_slo_met({"p95_ms": 999.95, "p99_ms": 1000}, clock, 1000, 2000))


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] in ("--fixture", "--supervisor"):
        action, mode, directory = sys.argv[1:]
        directory = Path(directory)
        if action == "--supervisor":
            result = run_supervised([sys.executable, __file__, "--fixture", mode, str(directory)], directory, 3)
            raise SystemExit(result.returncode)
        fixture(mode, directory)
    else:
        unittest.main()
