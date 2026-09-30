"""A concurrent heartbeat must not satisfy a row transaction's visibility wait."""
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

import psycopg

from faults import FaultRun
from run import Run


class MaterializationBarrierTests(unittest.TestCase):
    def test_waits_ignore_progress_between_precommit_sample_and_row_commit(self):
        for recovery in (False, True):
            with self.subTest(recovery=recovery):
                run = FaultRun.__new__(FaultRun)
                run.args = SimpleNamespace(timeout=1)
                run.pg = Mock()
                run.pg.info.transaction_status = psycopg.pq.TransactionStatus.IDLE
                # The row transaction sampled 100, a concurrent heartbeat ended
                # at 120, the row transaction committed at 140, and our marker
                # was written at 160. Only the last sample proves visibility.
                run.pg.execute.return_value.fetchone.return_value = ("0/A0",)
                progress = iter((120, 180))
                observed = []

                def metrics():
                    value = next(progress)
                    observed.append(value)
                    return {"flow_materialized_lsn": value,
                            "flow_journal_durable_lsn": 180,
                            "flow_source_received_lsn": 180}

                run.metrics = metrics
                run.alive = Mock()
                run.process = Mock()
                run.process.poll.return_value = None
                run.compactor = None
                run.recovery_times = []
                run.compare = Mock(side_effect=lambda _: {"observed": observed[-1]})
                with patch("time.sleep"):
                    result = run.recover(100, "post-index-rebuild") if recovery else run.wait_materialized(100)
                self.assertEqual(observed, [120, 180])
                if recovery:
                    self.assertEqual(result["observed"], 180)
                    run.compare.assert_called_once_with("post-index-rebuild")
                else:
                    self.assertEqual(result["flow_materialized_lsn"], 180)

    def test_wait_cannot_emit_marker_inside_uncommitted_transaction(self):
        run = Run.__new__(Run)
        run.pg = Mock()
        run.pg.info.transaction_status = psycopg.pq.TransactionStatus.INTRANS
        with self.assertRaises(AssertionError):
            run.materialization_barrier(100)
        run.pg.execute.assert_not_called()

    def test_marker_does_not_lower_requested_watermark(self):
        run = Run.__new__(Run)
        run.pg = Mock()
        run.pg.info.transaction_status = psycopg.pq.TransactionStatus.IDLE
        run.pg.execute.return_value.fetchone.return_value = ("0/A0",)
        self.assertEqual(run.materialization_barrier(200), 200)


if __name__ == "__main__":
    unittest.main()
