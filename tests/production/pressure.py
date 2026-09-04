#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["duckdb==1.5.5", "psycopg[binary]==3.3.5", "boto3==1.43.88", "fastavro==1.12.2"]
# ///
"""Pause publication at hard reader debt, then resume through an external rewrite."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
import traceback

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "local"))
from run import Run, dump, lsn


class PressureRun(Run):
    def __init__(self, args):
        super().__init__(args)
        self.compactor = None
        self.compactor_log = None
        self.environment["RUST_LOG"] = "info"
        self.config.write_text(self.config.read_text().replace(
            "\n[[tables]]", "\n[compaction]\noldest_l0_soft_ms = 1000\noldest_l0_hard_ms = 2000\n\n[[tables]]", 1))
        with args.compactor_binary.open("rb") as executable:
            digest = hashlib.file_digest(executable, "sha256").hexdigest()
        self.report["compactor_binary"] = {"path": str(args.compactor_binary.resolve()), "sha256": digest}
        self.report["policy"] = {"oldest_l0_soft_ms": 1000, "oldest_l0_hard_ms": 2000,
                                 "roles": ["ingest", "coordinator"]}

    def command(self, command):
        result = super().command(command)
        return result + ["--roles", "ingest,coordinator"] if command == "run" else result

    def alive(self):
        super().alive()
        if self.compactor is not None and self.compactor.poll() is not None:
            raise RuntimeError(f"external compactor exited {self.compactor.returncode}; see compactor.log")

    def paused(self):
        metrics = self.metrics()
        assert metrics.get("flow_materialized_lsn", 0) < self.barrier, "publication crossed hard reader debt"
        confirmed = self.pg.execute("SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name=%s",
                                    (self.name,)).fetchone()[0]
        assert lsn(confirmed) < self.barrier, "PostgreSQL ACK crossed unpublished data"
        assert self.process.pid == self.daemon_pid
        self.alive()
        return {"journal_lsn": metrics.get("flow_journal_durable_lsn", 0),
                "materialized_lsn": metrics.get("flow_materialized_lsn", 0), "confirmed_flush_lsn": confirmed}

    def build_pressure(self):
        self.daemon_pid = self.process.pid
        before = {name: self.table(name)["metadata"]["current-snapshot-id"] for name in ("orders", "accounts")}
        time.sleep(2.2)
        self.barrier = self.transaction([
            "UPDATE orders SET payload='held-by-reader-debt', amount=amount+0.5 WHERE id <= 12",
            "UPDATE accounts SET amount=amount+0.5 WHERE id <= 4",
        ])
        self.until("capture did not journal while publication was paused", lambda:
                   self.metrics().get("flow_journal_durable_lsn", 0) >= self.barrier)
        self.until("both tables did not reach hard reader debt", lambda:
                   sum(value == 2 for key, value in self.metrics().items()
                       if key.startswith("flow_table_publication_pressure{")) == 2)
        for _ in range(5):
            self.paused()
            time.sleep(0.2)
        for name, snapshot in before.items():
            assert self.table(name)["metadata"]["current-snapshot-id"] == snapshot
        return self.paused() | {"source_transaction_barrier": self.barrier, "daemon_pid": self.daemon_pid}

    def compactor_events(self):
        return [json.loads(line) for line in (self.directory / "compactor.log").read_text().splitlines()
                if line.startswith("{")]

    def relieve_pressure(self):
        self.compactor_log = (self.directory / "compactor.log").open("wb")
        self.compactor = subprocess.Popen(
            [str(self.args.compactor_binary.resolve()), "--config", str(self.config), "--interval-ms", "1000"],
            env=self.environment, stdout=self.compactor_log, stderr=subprocess.STDOUT)
        self.wait_materialized(self.barrier)
        committed = {event["table"] for event in self.compactor_events()
                     if event.get("event") == "compaction_committed"}
        assert committed == {"orders", "accounts"}, "publication resumed without external rewrites of both tables"
        assert self.process.pid == self.daemon_pid and self.generation == 1
        result = self.compare("external-rewrite-unpaused-publication")
        return result | {"daemon_pid": self.process.pid, "rewritten_tables": sorted(committed)}

    def after_recovery(self):
        barrier = self.transaction([
            "UPDATE orders SET payload='after-external-reconcile', data=decode('00ff', 'hex') WHERE id <= 8",
            "DELETE FROM orders WHERE id BETWEEN 20 AND 27",
            "UPDATE orders SET id=id+100000 WHERE id BETWEEN 30 AND 37",
            "UPDATE accounts SET amount=amount-0.25 WHERE id <= 4",
            "DELETE FROM accounts WHERE id=10",
        ])
        self.wait_materialized(barrier)
        result = self.compare("updates-deletes-key-moves-after-unpause")
        assert self.process.pid == self.daemon_pid and self.generation == 1
        assert self.rows("orders", self.initial_metadata) == self.initial_rows
        return result | {"daemon_pid": self.process.pid, "retained_initial_rows": len(self.initial_rows)}

    def execute(self):
        try:
            self.phase("seed", self.seed)
            self.phase("initial-copy", self.initialize)
            self.start()
            self.phase("snapshot-wal-handoff", self.handoff)
            self.phase("hard-debt-holds-publication-and-ack", self.build_pressure)
            self.phase("external-compactor-reconciles-and-unpauses", self.relieve_pressure)
            self.phase("mutations-and-history-after-recovery", self.after_recovery)
            self.report["passed"] = True
        except BaseException as error:
            self.report.update(error=str(error), traceback=traceback.format_exc())
            raise
        finally:
            self.stop()
            if self.compactor is not None and self.compactor.poll() is None:
                self.compactor.send_signal(signal.SIGINT)
                try:
                    self.compactor.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    self.compactor.kill()
                    self.compactor.wait(timeout=10)
            if self.compactor_log is not None:
                self.compactor_log.close()
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
    parser.add_argument("--compactor-binary", type=Path, default=Path("target/release/examples/local_compactor"))
    parser.add_argument("--artifacts", type=Path, required=True)
    parser.add_argument("--timeout", type=float, default=180)
    args = parser.parse_args()
    PressureRun(args).execute()
    print(f"PASS: {args.artifacts.resolve() / 'report.json'}", flush=True)


if __name__ == "__main__":
    main()
