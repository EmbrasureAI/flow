#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["duckdb==1.5.5", "psycopg[binary]==3.3.5", "boto3==1.43.88", "fastavro==1.12.2", "pytz==2026.3.post1"]
# ///
"""Hold real compaction commits at soft and hard debt across a daemon restart."""

import argparse
import json
import os
from pathlib import Path
import signal
import sys
import threading
import time
import traceback

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "local"))
from run import Run, dump, lsn
from proxy import CatalogProxy
import psycopg
from psycopg import sql


class MaintenanceRun(Run):
    def __init__(self, args):
        super().__init__(args)
        self.native = False
        self.producer = None
        self.producer_stop = threading.Event()
        self.producer_barriers = []
        self.producer_error = None
        self.proxy = CatalogProxy(args.catalog_uri, self.directory / "catalog-proxy.jsonl")
        self.environment["RUST_LOG"] = "info"
        text = self.config.read_text().replace(json.dumps(args.catalog_uri), json.dumps(self.proxy.url))
        text = text.replace("[limits]\n", "[limits]\ntable_workers = 2\n")
        text = text.replace("\n[[tables]]", "\n[compaction]\noldest_l0_soft_ms = 1000\noldest_l0_hard_ms = 2000\n\n[[tables]]", 1)
        self.config.write_text(text)

    def command(self, command):
        command_line = super().command(command)
        if command == "run" and not self.native:
            command_line += ["--roles", "ingest,coordinator"]
        return command_line

    def hard_limit(self, milliseconds):
        lines = self.config.read_text().splitlines()
        self.config.write_text("\n".join(
            f"oldest_l0_hard_ms = {milliseconds}" if line.startswith("oldest_l0_hard_ms") else line
            for line in lines) + "\n")

    def heads(self):
        return {name: self.table(name)["metadata"]["current-snapshot-id"] for name in ("orders", "accounts")}

    def confirmed(self):
        return lsn(self.pg.execute("SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name=%s",
                                   (self.name,)).fetchone()[0])

    def paused_backlog(self, label):
        # Capture the transaction durably with native compaction disabled. This
        # creates an existing due CDC queue before either restart, avoiding a
        # race between an idle maintenance tick and fresh source capture.
        time.sleep(2.2)
        before = self.heads()
        barrier = self.transaction([
            f"UPDATE orders SET payload='{label}', amount=amount+0.5 WHERE id BETWEEN 1 AND 12",
            "DELETE FROM orders WHERE id BETWEEN 20 AND 22",
            "UPDATE orders SET id=id+100000 WHERE id BETWEEN 30 AND 32",
            "UPDATE accounts SET amount=amount+0.5 WHERE id <= 4",
        ])
        self.until("source transaction was not journaled at hard pressure", lambda:
                   self.metrics().get("flow_journal_durable_lsn", 0) >= barrier)
        self.until("both tables did not reach hard pressure", lambda:
                   sum(value == 2 for key, value in self.metrics().items()
                       if key.startswith("flow_table_publication_pressure{")) == 2)
        assert self.metrics()["flow_materialized_lsn"] < barrier
        assert self.confirmed() < barrier
        assert self.heads() == before
        self.stop()
        return barrier, before

    def produce(self):
        try:
            with psycopg.connect(self.args.postgres_url, autocommit=True) as connection:
                connection.execute(sql.SQL("SET search_path TO {}, public").format(sql.Identifier(self.name)))
                while not self.producer_stop.is_set():
                    with connection.transaction():
                        connection.execute("UPDATE orders SET amount=amount+0.0001 WHERE id=2048")
                        barrier = lsn(connection.execute("SELECT pg_current_wal_insert_lsn()::text").fetchone()[0])
                    self.producer_barriers.append(barrier)
                    self.producer_stop.wait(0.01)
        except BaseException as error:
            self.producer_error = error

    def stop_producer(self):
        self.producer_stop.set()
        if self.producer is not None:
            self.producer.join(timeout=5)
            assert not self.producer.is_alive(), "source producer failed to stop"
        if self.producer_error is not None:
            raise self.producer_error

    def soft_debt(self):
        self.barrier, _ = self.paused_backlog("soft-debt-published")
        self.hard_limit(120000)
        self.native = True
        self.proxy.hold_commits("compact", table="orders")
        self.producer = threading.Thread(target=self.produce, name="maintenance-cdc", daemon=True)
        self.producer.start()
        started = time.monotonic()
        self.start()
        self.until("soft maintenance did not get an opportunity during active CDC",
                   self.proxy.commit_held.is_set, timeout=15)
        held_after = time.monotonic() - started
        self.stop_producer()
        self.wait_materialized(self.barrier)
        self.until("source ACK was held behind optional compaction", lambda: self.confirmed() >= self.barrier)
        metrics = self.metrics()
        # The deliberately held compact POST cannot have published these rows.
        selected = [row for row in self.rows("orders") if 1 <= row[0] <= 12]
        assert len(selected) == 12 and all(row[2] == "soft-debt-published" for row in selected)
        assert metrics["flow_pending_transactions"] > 0, "fixture did not keep CDC queued during maintenance admission"
        assert self.producer_barriers and held_after < 15
        self.held_heads = self.heads()
        return {"source_barrier": self.barrier, "materialized_lsn": metrics["flow_materialized_lsn"],
                "confirmed_lsn": self.confirmed(), "pending_transactions": metrics["flow_pending_transactions"],
                "source_transactions_during_restart": len(self.producer_barriers),
                "maintenance_held_after_seconds": round(held_after, 3), "snapshots": self.held_heads}

    def unresolved_restart(self):
        # The proxy already owns real prepared POSTs. Let them reach the catalog
        # only after SIGTERM, so their successful outcomes are unknown locally.
        process = self.process
        started = time.monotonic()
        process.send_signal(signal.SIGTERM)
        code = process.wait(timeout=5)
        shutdown = time.monotonic() - started
        assert code == 0
        self.process = None
        self.log.close()
        self.proxy.release_commits.set()
        self.until("held compact did not commit after daemon exit", lambda:
                   any(event.get("held_before_upstream") and event.get("upstream_status") == 200
                       and "compact" in event.get("operations", []) for event in self.proxy.events))
        self.start()
        self.wait_materialized(max(self.producer_barriers + [self.barrier]))
        result = self.compare("prepared-compaction-restart")
        assert self.rows("orders", self.initial_metadata) == self.initial_rows
        result.update(sigterm_seconds=round(shutdown, 3), exit_code=code,
                      retained_initial_rows=len(self.initial_rows))
        return result

    def no_l0(self):
        for name in ("orders", "accounts"):
            metadata = self.table(name)["metadata"]
            head = next(snapshot for snapshot in metadata["snapshots"]
                        if snapshot["snapshot-id"] == metadata["current-snapshot-id"])
            for manifest in self.avro(head["manifest-list"]):
                for entry in self.avro(manifest["manifest_path"]):
                    file = entry["data_file"]
                    if entry["status"] != 2 and file["content"] == 0 and "flow-l0-" in file["file_path"]:
                        return False
        return True

    def hard_debt(self):
        # Finish the producer's final L0 tail before lowering the hard age. The
        # preceding recovery proof permits that tail and does not wait for an
        # optional maintenance pass to finish.
        self.until("soft follow-up did not drain the recovered producer tail", self.no_l0)
        self.stop()
        self.native = False
        self.hard_limit(2000)
        self.start()
        # The recovered rewrite is L1. A new publication creates the aged L0
        # input required for the mandatory-pressure half of the same gate.
        barrier = self.transaction([
            "UPDATE orders SET payload='hard-debt-seed' WHERE id=100",
            "UPDATE accounts SET amount=amount+0.25 WHERE id=8",
        ])
        self.wait_materialized(barrier)
        barrier, before = self.paused_backlog("hard-debt-held")
        self.native = True
        self.proxy.hold_commits("compact")
        self.start()
        self.until("hard pressure did not start mandatory compaction", self.proxy.commit_held.is_set)
        self.until("restart did not publish a current status observation", lambda:
                   self.metrics().get("flow_journal_durable_lsn", 0) >= barrier)
        for _ in range(5):
            assert self.metrics()["flow_materialized_lsn"] < barrier
            assert self.confirmed() < barrier
            assert self.heads() == before, "due CDC crossed hard reader debt before remediation"
            self.alive()
            time.sleep(0.2)
        self.proxy.release_commits.set()
        self.wait_materialized(barrier)
        result = self.compare("hard-debt-relieved")
        return result | {"source_barrier": barrier, "confirmed_lsn": self.confirmed()}

    def execute(self):
        try:
            self.phase("seed", self.seed)
            self.phase("initial-copy", self.initialize)
            self.start()
            self.phase("snapshot-wal-handoff", self.handoff)
            self.phase("soft-debt-publishes-and-acks-before-maintenance", self.soft_debt)
            self.phase("sigterm-and-unknown-compaction-recovery", self.unresolved_restart)
            self.phase("hard-debt-still-blocks-publication", self.hard_debt)
            self.report["passed"] = True
        except BaseException as error:
            self.report.update(error=str(error), traceback=traceback.format_exc())
            raise
        finally:
            self.producer_stop.set()
            self.stop()
            if self.producer is not None:
                self.producer.join(timeout=5)
            self.proxy.close()
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
    parser.add_argument("--timeout", type=float, default=180)
    args = parser.parse_args()
    MaintenanceRun(args).execute()
    print(f"PASS: {args.artifacts.resolve() / 'report.json'}", flush=True)


if __name__ == "__main__":
    main()
