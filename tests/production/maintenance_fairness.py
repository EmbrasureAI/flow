#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["duckdb==1.5.5", "psycopg[binary]==3.3.5", "boto3==1.43.88", "fastavro==1.12.2", "pytz==2026.3.post1"]
# ///
"""Require soft compaction admission while a preloaded CDC backlog stays ready."""

import argparse
import json
import os
from pathlib import Path
import signal
import time
import traceback

from concurrent_compaction import ConcurrentRun, Run, dump, lsn


class FairnessRun(ConcurrentRun):
    def __init__(self, args):
        super().__init__(args)
        # A finite diagnostic isolates soft file debt. Its 65 source pages fit
        # below every hard file limit, even if maintenance is entirely starved.
        self.policy = {
            "l0_soft_files": 2, "l0_hard_files": 128,
            "stable_small_soft_files": 128, "stable_small_hard_files": 128,
            "delete_files_soft": 128, "delete_files_hard": 128,
            "oldest_l0_soft_ms": 120000, "oldest_l0_hard_ms": 240000,
            "min_file_age_ms": 0,
        }
        text = self.config.read_text().replace("table_workers = 4", "table_workers = 2")
        text = text.replace("pending_transactions = 32", "pending_transactions = 16")
        # A low token rate introduces permit gaps in which even a starving
        # scheduler can run maintenance. Keep permits faster than real commits.
        text = text.replace("commits_per_second = 100", "commits_per_second = 10000")
        self.config.write_text(text)
        self.set_compaction_policy(self.policy)
        self.cold_table = getattr(args, "cold_table", False)
        self.report.update(compaction_policy=self.policy, table_workers=2,
                           pending_transactions=16, commits_per_second=10000,
                           queued_transactions=1025,
                           hot_tables=(["accounts"] if self.cold_table else
                                       ["orders", "accounts"] if args.two_hot_tables else ["orders"]),
                           planned_phases=5)

    def seed(self):
        return Run.seed(self)

    def status(self):
        return json.loads((self.directory / "state/status.json").read_text())

    def stop_clean(self):
        process = self.process
        process.send_signal(signal.SIGTERM)
        code = process.wait(timeout=10)
        self.process = None
        self.log.close()
        assert code == 0, f"daemon did not exit cleanly: {code}"
        return code

    def queued_backlog(self):
        self.start()
        self.handoff()
        seed = self.transaction([
            "INSERT INTO orders (id, tenant, payload) VALUES (9000, 1, 'fairness-input')",
        ])
        self.wait_materialized(seed)
        self.stop_clean()
        self.before_metadata = self.table("orders")
        self.before_rows = self.rows("orders", self.before_metadata)
        _, files = self.files(self.before_metadata)
        self.inputs = [path for path, file in files.items()
                       if file["content"] == 0 and "flow-l0-" in path]
        assert len(self.inputs) >= 2, "fixture needs two eligible pre-backlog L0 inputs"

        # All source commits precede native restart; publication cannot rely on
        # gaps between a live producer's arrivals to run optional maintenance.
        for index in range(1024):
            statements = ["UPDATE accounts SET amount=amount+0.0001 WHERE id=1"] if self.cold_table else [
                f"INSERT INTO orders (id, tenant, payload) VALUES ({100000 + index}, 2, 'queued-{index}')",
            ]
            if self.args.two_hot_tables:
                statements.append("UPDATE accounts SET amount=amount+0.0001 WHERE id=1")
            self.transaction(statements)
        self.barrier = self.transaction(["UPDATE accounts SET amount=amount+0.25 WHERE id=1"] if self.cold_table else [
            "DELETE FROM orders WHERE id BETWEEN 101020 AND 101023",
            "UPDATE orders SET id=id+100000 WHERE id BETWEEN 100000 AND 100003",
            "UPDATE accounts SET amount=amount+0.25 WHERE id=1",
        ])
        committed_at = time.monotonic()

        # Prime the durable queue without native compaction. A held POST keeps
        # the first source page from draining while capture registers the rest.
        self.catalog_proxy.hold_commits("ingest", table="accounts" if self.cold_table else "orders")
        self.start()
        self.until("backlog did not reach the held publication", self.catalog_proxy.commit_held.is_set)
        def registered_backlog():
            status = self.status()
            return status if (status["watermarks"]["journal_durable_lsn"] >= self.barrier
                              and status["pending_transactions"] > 16) else None
        registered = self.until("source backlog was not durably registered", registered_backlog)
        self.stop_clean()
        self.catalog_proxy.release_commits.set()
        self.until("held publication did not finish after shutdown", lambda:
            any(event.get("held_before_upstream") and event.get("upstream_status") == 200
                and "ingest" in event.get("operations", []) for event in self.catalog_proxy.events))
        assert self.confirmed() < self.barrier
        # The scheduler uses the first transaction's source age. Age even the
        # newest queued commit beyond the one-second optional-maintenance delay,
        # so subsequent 16-transaction pages remain immediately ready.
        time.sleep(max(0, 1.1 - (time.monotonic() - committed_at)))
        return {"source_barrier": self.barrier, "registered_status": registered,
                "held_input_files": self.inputs, "confirmed_lsn": self.confirmed(),
                "newest_queued_commit_age_seconds": round(time.monotonic() - committed_at, 3),
                "optional_maintenance_delay_seconds": 1}

    def admission_and_progress(self):
        self.object_proxy.hold_reads(self.inputs)
        self.native = True
        started = time.monotonic()
        self.start()
        self.until("soft build was starved by queued CDC", self.object_proxy.held.is_set, timeout=15)
        admitted_after = time.monotonic() - started
        orders = self.table("orders")["metadata"]
        head = next(snapshot for snapshot in orders["snapshots"]
                    if snapshot["snapshot-id"] == orders["current-snapshot-id"])
        orders_lsn = lsn(head["summary"]["streaming.last-lsn"])
        assert orders_lsn < self.barrier, "orders build started only after its own backlog drained"
        order_snapshots = {snapshot["snapshot-id"] for snapshot in orders["snapshots"]}
        event = self.until("held input read lacks build-start evidence", lambda:
            next((event for event in self.log_fields()
                  if event.get("event") == "compaction_build_started"
                  and event.get("base_snapshot_id") in order_snapshots), None))
        admitted = self.status()
        materialized = admitted["watermarks"]["materialized_lsn"]
        assert materialized < self.barrier, "build started only after the source backlog drained"
        assert admitted["pending_transactions"] > 0, "build admission did not overlap queued work"
        pressure = [value for key, value in self.metrics().items()
                    if key.startswith("flow_table_publication_pressure{")]
        assert pressure and max(pressure) < 2, "hard debt, not fairness, admitted the build"
        def progressed():
            status = self.status()
            return status if status["watermarks"]["materialized_lsn"] > materialized else None
        advanced = self.until("CDC stopped while an optional build read was held", progressed, timeout=5)
        assert not self.object_proxy.release_reads.is_set()
        self.until("source ACK stopped behind optional build", lambda: self.confirmed() > materialized, timeout=5)
        self.object_proxy.release_reads.set()
        self.wait_materialized(self.barrier)
        self.until("source ACK did not reach the final transaction", lambda: self.confirmed() >= self.barrier)
        self.until("admitted background build did not publish", lambda:
            any(snapshot["summary"].get("streaming.build-snapshot-id") == str(event["base_snapshot_id"])
                for snapshot in self.table("orders")["metadata"]["snapshots"]))
        result = self.compare("queued-cdc-and-soft-build")
        assert self.rows("orders", self.initial_metadata) == self.initial_rows
        assert self.rows("orders", self.before_metadata) == self.before_rows
        dump(self.directory / "retained-pre-backlog-metadata.json", self.before_metadata)
        return result | {"build": event, "admission_status": admitted,
                         "orders_head_lsn_at_admission": orders_lsn,
                         "probe_admissions": [event for event in self.log_fields()
                                              if event.get("event") == "compaction_build_probe_admitted"],
                         "progress_while_held": advanced, "pressure_at_admission": pressure,
                         "admission_after_seconds": round(admitted_after, 3),
                         "completed_after_seconds": round(time.monotonic() - started, 3),
                         "confirmed_lsn": self.confirmed(),
                         "retained_initial_rows": len(self.initial_rows),
                         "retained_pre_backlog_rows": len(self.before_rows)}

    def execute(self):
        try:
            self.phase("seed", self.seed)
            self.phase("initial-copy", self.initialize)
            self.phase("queue-source-and-prime-durable-backlog", self.queued_backlog)
            self.phase("soft-build-admission-with-continuously-ready-cdc", self.admission_and_progress)
            self.phase("clean-shutdown", lambda: {"exit_code": self.stop_clean()})
            self.check_worker_panics()
            self.report["passed"] = True
        except BaseException as error:
            self.report.update(error=str(error), traceback=traceback.format_exc())
            raise
        finally:
            self.object_proxy.release_reads.set()
            self.catalog_proxy.release_commits.set()
            self.stop()
            self.object_proxy.close()
            self.catalog_proxy.close()
            dump(self.directory / "report.json", self.report)
            self.pg.close()
            self.duck.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--postgres-url", default=os.environ.get("FLOW_POSTGRES_URL"))
    parser.add_argument("--catalog-uri", required=True)
    parser.add_argument("--s3-endpoint", required=True)
    parser.add_argument("--warehouse", default="s3://warehouse/")
    parser.add_argument("--binary", type=Path, default=Path("target/debug/embrasure-flow"))
    parser.add_argument("--artifacts", type=Path, required=True)
    parser.add_argument("--two-hot-tables", action="store_true",
                        help="also update accounts in every transaction to cover two-worker contention")
    parser.add_argument("--cold-table", action="store_true",
                        help="leave orders unchanged after restart while accounts has a durable CDC backlog")
    parser.add_argument("--timeout", type=float, default=180)
    args = parser.parse_args()
    if args.cold_table and args.two_hot_tables:
        parser.error("--cold-table and --two-hot-tables are mutually exclusive")
    FairnessRun(args).execute()
    print(f"PASS: {args.artifacts.resolve() / 'report.json'}", flush=True)


if __name__ == "__main__":
    main()
