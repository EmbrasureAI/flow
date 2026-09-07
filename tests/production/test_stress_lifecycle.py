# /// script
# requires-python = ">=3.11"
# dependencies = ["duckdb==1.5.5", "psycopg[binary]==3.3.5", "boto3==1.43.88", "fastavro==1.12.2"]
# ///
"""Service-free checks of stress writer cancellation and process ownership."""

from contextlib import nullcontext
import json
import os
from pathlib import Path
import sys
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "local"))
import stress
from process_lifecycle import run_supervised


class Connection:
    def __init__(self, error=None):
        self.entered = threading.Event()
        self.canceled = threading.Event()
        self.closed = False
        self.error = error or stress.psycopg.errors.QueryCanceled("fixture canceled")
        self.cancel_timeouts = []
        self.statements = 0

    def __enter__(self):
        return self

    def __exit__(self, *unused):
        self.closed = True

    def transaction(self):
        return nullcontext()

    def execute(self, query, parameters=None):
        self.statements += 1
        if isinstance(query, str) and query.startswith("UPDATE"):
            self.entered.set()
            if not self.canceled.wait(timeout=5):
                raise RuntimeError("test did not release blocked query")
            raise self.error
        return SimpleNamespace(rowcount=1)

    def cancel_safe(self, timeout):
        self.cancel_timeouts.append(timeout)
        self.canceled.set()


def run_fixture(directory, workers=1):
    return SimpleNamespace(directory=directory, name="fixture", report={},
                           args=SimpleNamespace(workers=workers, seed=1, postgres_url="unused"))


class StressLifecycleTests(unittest.TestCase):
    def test_stop_cancels_all_connections_joins_and_closes(self):
        with tempfile.TemporaryDirectory() as directory:
            run = run_fixture(Path(directory), workers=2)
            connections = [Connection(), Connection()]
            with patch.object(stress.psycopg, "connect", side_effect=connections) as connect:
                writers = stress.Writers(run)
                try:
                    self.assertTrue(all(connection.entered.wait(timeout=2) for connection in connections))
                    writers.stop()
                    self.assertEqual(writers.errors, [])
                    self.assertEqual(writers.connections, {})
                    self.assertTrue(run.report["writer_shutdown"]["joined"])
                    self.assertFalse((run.directory / ".workload-deadline.json").exists())
                    for connection in connections:
                        self.assertEqual(connection.cancel_timeouts, [3])
                        self.assertTrue(connection.closed)
                    for call in connect.call_args_list:
                        self.assertEqual(call.kwargs["connect_timeout"], 5)
                finally:
                    for connection in connections:
                        connection.canceled.set()
                    for thread in writers.threads:
                        thread.join(timeout=2)

    def test_shutdown_preserves_real_error_and_unsolicited_query_cancel(self):
        cases = [(stress.psycopg.OperationalError("connection lost"), False),
                 (stress.psycopg.errors.QueryCanceled("statement timeout"), True)]
        for error, unsolicited in cases:
            with self.subTest(error=error), tempfile.TemporaryDirectory() as directory:
                connection = Connection(error)
                run = run_fixture(Path(directory))
                with patch.object(stress.psycopg, "connect", return_value=connection):
                    writers = stress.Writers(run)
                    try:
                        self.assertTrue(connection.entered.wait(timeout=2))
                        if unsolicited:
                            connection.canceled.set()
                            writers.threads[0].join(timeout=2)
                            self.assertFalse(writers.threads[0].is_alive())
                        with self.assertRaisesRegex(RuntimeError, str(error)):
                            writers.stop()
                        self.assertTrue(connection.closed)
                    finally:
                        connection.canceled.set()
                        writers.threads[0].join(timeout=2)

    def test_connection_finishing_after_stop_does_not_start_work(self):
        with tempfile.TemporaryDirectory() as directory:
            opening, release = threading.Event(), threading.Event()
            connection = Connection()

            def connect(*args, **kwargs):
                opening.set()
                if not release.wait(timeout=3):
                    raise RuntimeError("test did not release connection")
                return connection

            with patch.object(stress.psycopg, "connect", side_effect=connect):
                writers = stress.Writers(run_fixture(Path(directory)))
                self.assertTrue(opening.wait(timeout=2))
                # Release connect only after the real stop method sets its flag.
                def release_on_stop():
                    with writers.condition:
                        writers.condition.wait_for(lambda: writers.stopping, timeout=2)
                    release.set()
                releaser = threading.Thread(target=release_on_stop)
                releaser.start()
                try:
                    writers.stop()
                    self.assertEqual(connection.statements, 0)
                    self.assertTrue(connection.closed)
                finally:
                    release.set()
                    releaser.join(timeout=2)
                    writers.threads[0].join(timeout=2)

    def test_supervisor_reaps_stuck_native_cancellation_and_preserves_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "run"
            result = run_supervised([sys.executable, __file__, "--stalled-cancel", str(output)], output, 10)
            self.assertEqual(result.returncode, 124)
            report = json.loads((output / "report.json").read_text())
            self.assertFalse(report["passed"])
            self.assertIn("workload hard deadline", report["failure"])
            self.assertEqual(report["worker_failure"], "original workload failure")
            pid = int((output / "pid").read_text())
            with self.assertRaises(ProcessLookupError):
                os.kill(pid, 0)

    def test_writer_cleanup_error_keeps_original_failure_and_closes_other_owners(self):
        with tempfile.TemporaryDirectory() as directory:
            run = stress.StressRun.__new__(stress.StressRun)
            run.directory = Path(directory)
            run.report = {"passed": False}
            run.phase = Mock(side_effect=ValueError("original phase failure"))
            run.writers = SimpleNamespace(stop=Mock(side_effect=RuntimeError("writer cleanup failed")), threads=[])
            run.compactor = None
            run.stop = Mock()
            run.pg, run.duck = Mock(), Mock()
            with self.assertRaisesRegex(ValueError, "original phase failure"):
                run.execute()
            run.stop.assert_called_once()
            run.pg.close.assert_called_once()
            run.duck.close.assert_called_once()
            report = json.loads((run.directory / "report.json").read_text())
            self.assertEqual(report["failure"], "original phase failure")
            self.assertEqual(report["cleanup_errors"], ["writer cleanup failed"])


def stalled_cancel(directory):
    directory.mkdir()
    (directory / "pid").write_text(str(os.getpid()))
    (directory / "report.json").write_text(json.dumps({"passed": False, "failure": "original workload failure"}))
    connection = Connection()
    # Model a native cancellation call that never returns despite its timeout.
    connection.cancel_safe = lambda timeout: threading.Event().wait()
    arm = stress.arm_workload
    with patch.object(stress.psycopg, "connect", return_value=connection), patch.object(
            stress, "arm_workload", side_effect=lambda path, deadline: arm(path, time.monotonic() + .2)):
        writers = stress.Writers(run_fixture(directory))
        assert connection.entered.wait(timeout=2)
        writers.stop()


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--stalled-cancel":
        stalled_cancel(Path(sys.argv[2]))
    else:
        unittest.main()
