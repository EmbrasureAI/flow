"""Failure controls for owned container cleanup and WAL process deadlines."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import Mock, patch

from container_cleanup import cleanup_container
import source_versions
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "local"))
from run import Run


class ContainerLifecycleTests(unittest.TestCase):
    def test_source_start_timeout_still_removes_named_container(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            binary = root / "flow"
            binary.write_bytes(b"test binary")
            artifacts = root / "artifacts"
            commands = []

            def command(args, **kwargs):
                commands.append(args)
                if args[:2] == ["docker", "run"]:
                    raise subprocess.TimeoutExpired(args, kwargs["timeout"])
                if args[:2] == ["docker", "logs"]:
                    return subprocess.CompletedProcess(args, 0, b"startup logs", b"")
                if args[:2] == ["docker", "rm"]:
                    return subprocess.CompletedProcess(args, 0, "", "")
                self.fail(f"unexpected command: {args}")

            argv = ["source_versions.py", "--versions", "14.24", "--catalog-uri", "http://catalog",
                    "--s3-endpoint", "http://s3", "--binary", str(binary), "--artifacts", str(artifacts)]
            with patch.object(sys, "argv", argv), patch("source_versions.subprocess.run", side_effect=command):
                with self.assertRaises(SystemExit) as exited:
                    source_versions.main()
            self.assertEqual(exited.exception.code, 1)
            name = commands[0][commands[0].index("--name") + 1]
            self.assertEqual(commands[1:], [["docker", "logs", name],
                                             ["docker", "rm", "--force", "--volumes", name]])
            report = json.loads((artifacts / "report.json").read_text())
            self.assertFalse(report["passed"])
            record = report["versions"][0]
            self.assertFalse(record["passed"])
            self.assertIn("TimeoutExpired", record["error"])
            self.assertTrue(record["cleanup"]["passed"])
            self.assertEqual(record["cleanup"]["container"], name)
            self.assertEqual((artifacts / "pg-14.24.log").read_bytes(), b"startup logs")

    def test_collection_failures_still_remove_every_owned_container(self):
        for failure in (subprocess.TimeoutExpired("docker logs", 30), OSError("artifact disk full")):
            commands = []
            def command(args, **kwargs):
                commands.append(args)
                self.assertGreater(kwargs["timeout"], 0)
                if args[1] == "logs":
                    if isinstance(failure, subprocess.TimeoutExpired):
                        raise failure
                    return subprocess.CompletedProcess(args, 0, b"log", b"")
                return subprocess.CompletedProcess(args, 0, "", "")
            log = Mock()
            log.write_bytes.side_effect = failure
            with patch("container_cleanup.subprocess.run", side_effect=command):
                results = [cleanup_container(name, log) for name in ("owned-one", "owned-two")]
            self.assertEqual([args[-1] for args in commands if args[1] == "rm"], ["owned-one", "owned-two"])
            self.assertTrue(all(not result["passed"] and "collection_error" in result for result in results))
        with patch("container_cleanup.subprocess.run", return_value=subprocess.CompletedProcess([], 1, "", "daemon unavailable")):
            self.assertFalse(cleanup_container("owned")["passed"])

    def test_joined_successful_process_with_worker_panic_fails_fixture(self):
        with tempfile.TemporaryDirectory() as temporary:
            run = Run.__new__(Run)
            run.directory = Path(temporary)
            run.report = {"passed": True}
            run.log = (run.directory / "daemon-1.log").open("wb")
            run.process = subprocess.Popen([sys.executable, "-c", "print(\"thread 'worker' panicked at synthetic failure\")"],
                                           stdout=run.log, stderr=subprocess.STDOUT)
            self.assertEqual(run.process.wait(timeout=5), 0)
            with self.assertRaisesRegex(AssertionError, "worker panic"):
                run.stop()
            self.assertIsNone(run.process)
            self.assertTrue(run.log.closed)
            report = json.loads((run.directory / "report.json").read_text())
            self.assertFalse(report["passed"])
            self.assertEqual(len(report["worker_panics"]), 1)

    def test_wal_deadline_owner_removes_container_after_stalled_docker_call(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            docker = root / "docker"
            docker.write_text(f'''#!{sys.executable}
import json, os, sys, time
from pathlib import Path
root = Path({str(root)!r})
if sys.argv[1] == 'create':
    (root / 'create-pid').write_text(str(os.getpid()))
    time.sleep(60)
elif sys.argv[1] == 'rm':
    (root / 'removed').write_text(sys.argv[-1])
else:
    raise SystemExit(2)
''')
            docker.chmod(0o755)
            directory = root / "run"
            result = subprocess.run([sys.executable, str(Path(__file__).with_name("wal_overhead.py")),
                                     "--artifacts", str(directory), "--timeout", "1"],
                                    env=os.environ | {"PATH": str(root) + os.pathsep + os.environ["PATH"]},
                                    capture_output=True, text=True, timeout=8)
            self.assertEqual(result.returncode, 124, result.stderr)
            pid = int((root / "create-pid").read_text())
            # The process group kill also removes the worker's blocked Docker child.
            deadline = time.monotonic() + 2
            while True:
                try:
                    os.kill(pid, 0)
                except ProcessLookupError:
                    break
                if time.monotonic() >= deadline:
                    self.fail("stalled Docker subprocess survived the WAL supervisor")
                time.sleep(.01)
            report = json.loads((directory / "report.json").read_text())
            self.assertFalse(report["passed"])
            self.assertTrue(report["cleanup"]["passed"])
            self.assertEqual(report["cleanup"]["container"], (root / "removed").read_text())


if __name__ == "__main__":
    unittest.main()
