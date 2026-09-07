"""Cleanup must survive failures in the evidence it is collecting."""
import importlib.util
import json
from pathlib import Path
import subprocess
import tempfile
from types import SimpleNamespace
import unittest


SPEC = importlib.util.spec_from_file_location("demo_validation", Path(__file__).resolve().parents[2] / "demo/validate.py")
DEMO = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(DEMO)


class DemoCleanupTest(unittest.TestCase):
    def test_log_timeout_still_removes_owned_project_and_preserves_initial_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            run = DEMO.Validation(SimpleNamespace(artifacts=Path(directory) / "run", project="owned"))
            run.created = True
            commands = []

            def command(args, **kwargs):
                commands.append(args)
                if "logs" in args:
                    raise subprocess.TimeoutExpired(args, 1)
                return subprocess.CompletedProcess(args, 0, "removed", "")

            def fail_phase(*args):
                raise ValueError("initial validation failure")

            run.run = command
            run.phase = fail_phase
            with self.assertRaisesRegex(ValueError, "initial validation failure"):
                run.execute()
            self.assertTrue(any("down" in args for args in commands))
            report = json.loads((run.directory / "report.json").read_text())
            self.assertEqual(report["failure"], "initial validation failure")
            self.assertIn("collection_error", report)
            self.assertEqual(report["cleanup_exit_code"], 0)


if __name__ == "__main__":
    unittest.main()
