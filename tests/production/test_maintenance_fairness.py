"""Build admission can precede the next throttled metrics export."""
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from maintenance_fairness import FairnessRun


class FairnessObservationTests(unittest.TestCase):
    def fixture(self, directory, samples):
        run = FairnessRun.__new__(FairnessRun)
        run.args = SimpleNamespace(timeout=1)
        run.directory = Path(directory)
        run.inputs = ["held-input"]
        run.barrier = 100
        run.object_proxy = Mock()
        run.object_proxy.held.is_set.return_value = True
        run.object_proxy.release_reads.is_set.return_value = False
        run.start = Mock()
        run.alive = Mock()
        run.table = Mock(return_value={"metadata": {
            "current-snapshot-id": 1,
            "snapshots": [{"snapshot-id": 1, "summary": {
                "streaming.last-lsn": "0/A", "streaming.build-snapshot-id": "1",
            }}],
        }})
        run.log_fields = Mock(return_value=[{
            "event": "compaction_build_started", "base_snapshot_id": 1,
        }])
        run.status = Mock(side_effect=[
            {"watermarks": {"materialized_lsn": 10}, "pending_transactions": 20},
            {"watermarks": {"materialized_lsn": 20}, "pending_transactions": 10},
        ])
        run.metrics = Mock(side_effect=samples)
        run.confirmed = Mock(return_value=100)
        run.wait_materialized = Mock()
        run.compare = Mock(return_value={})
        run.rows = Mock(return_value=[])
        run.initial_metadata = run.before_metadata = {}
        run.initial_rows = run.before_rows = []
        return run

    def test_waits_for_pressure_export_after_build_admission(self):
        with tempfile.TemporaryDirectory() as directory, patch("time.sleep"):
            run = self.fixture(directory, [
                {"flow_materialized_lsn": 10},
                {'flow_table_publication_pressure{table_id="1"}': 1},
            ])
            result = run.admission_and_progress()
            self.assertEqual(result["pressure_at_admission"], [1])
            self.assertEqual(run.metrics.call_count, 2)
            run.wait_materialized.assert_called_once_with(run.barrier)

    def test_hard_pressure_fails_without_waiting_for_it_to_clear(self):
        with tempfile.TemporaryDirectory() as directory, patch("time.sleep"):
            run = self.fixture(directory, [
                {},
                {'flow_table_publication_pressure{table_id="1"}': 2},
                {'flow_table_publication_pressure{table_id="1"}': 1},
            ])
            with self.assertRaisesRegex(AssertionError, "hard debt"):
                run.admission_and_progress()
            self.assertEqual(run.metrics.call_count, 2)
            run.wait_materialized.assert_not_called()


if __name__ == "__main__":
    unittest.main()
