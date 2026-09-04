#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["duckdb==1.5.5", "psycopg[binary]==3.3.5", "boto3==1.43.88", "fastavro==1.12.2", "pytz==2026.3.post1"]
# ///
"""Source schema, checkpoint and process lifecycle contracts across real services."""

import argparse
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
from proxy import CatalogProxy


class ContractRun(Run):
    def __init__(self, args):
        super().__init__(args)
        self.proxy = CatalogProxy(args.catalog_uri, self.directory / "catalog-proxy.jsonl")
        args.catalog_uri = self.proxy.url
        self.configure()
        self.config.write_text(self.config.read_text().replace(
            "snapshot_retention_secs = 3600", "snapshot_retention_secs = 3600\ncheckpoint_interval_secs = 2\nretained_checkpoints = 2"))
        self.environment["RUST_LOG"] = "info"

    def fields(self, table):
        metadata = self.table(table)["metadata"]
        return next(schema["fields"] for schema in metadata["schemas"]
                    if schema["schema-id"] == metadata["current-schema-id"])

    def idle_add(self):
        before = self.fields("orders")
        original = self.table("orders")
        snapshot = original["metadata"]["current-snapshot-id"]
        original_rows = self.rows("orders", original)
        original_ids = {item["snapshot-id"] for item in original["metadata"]["snapshots"]}
        original_snapshot = next(item for item in original["metadata"]["snapshots"] if item["snapshot-id"] == snapshot)
        event_start = len(self.proxy.events)
        watermark = self.metrics()["flow_materialized_lsn"]
        self.proxy.arm_drop("schema")
        with self.pg.transaction():
            ddl_xid = self.pg.execute("SELECT txid_current()").fetchone()[0] % (1 << 32)
            self.pg.execute("ALTER TABLE orders ADD COLUMN occurred_at timestamptz")
        self.until("idle committed ADD did not reach Iceberg", lambda: len(self.fields("orders")) == len(before) + 1)
        assert self.proxy.dropped.wait(timeout=10), "successful schema commit response was not dropped"
        after = self.fields("orders")
        assert after[:-1] == before, "existing Iceberg field identities changed"
        assert after[-1]["type"] == "timestamptz" and not after[-1]["required"]
        after_watermark = self.metrics()["flow_materialized_lsn"]
        assert after_watermark >= watermark
        if after_watermark != watermark:
            # Some PostgreSQL majors emit the DDL's empty transaction. Its real
            # terminal can advance ACK; the metadata update cannot invent one.
            events = [json.loads(line).get("fields", {}) for line in
                      (self.directory / f"daemon-{self.generation}.log").read_text().splitlines()
                      if line.startswith("{")]
            assert any(event.get("event") == "transaction_journaled" and event.get("xid") == ddl_xid
                       and lsn(event["end_lsn"]) == after_watermark for event in events), \
                "metadata-only evolution invented source progress"
        schema_requests = [event for event in self.proxy.events[event_start:]
                           if event.get("schema_update") and 200 <= event.get("upstream_status", 0) < 300]
        assert schema_requests, "schema publication request was not recorded"
        # The proxy emits the operations key for every nonempty add-snapshot
        # list, even when the snapshot has no streaming operation annotation.
        assert all("operations" not in event for event in schema_requests), "schema POST added a snapshot"
        current = self.table("orders")
        intervening = [item for item in current["metadata"]["snapshots"] if item["snapshot-id"] not in original_ids]
        for item in intervening:
            summary = item["summary"]
            assert (summary.get("streaming.writer-id") == "embrasure-flow"
                    and summary.get("streaming.operation") == "compact"
                    and summary["operation"] == "replace"
                    and summary.get("streaming.last-lsn") == original_snapshot["summary"]["streaming.last-lsn"]
                    and int(summary.get("deleted-data-files", 0)) > 0), "idle DDL created an unproven snapshot"
        expected_operations = {item["summary"]["flow.operation-id"] for item in intervening}
        def compactions_completed():
            events = [json.loads(line).get("fields", {}) for line in
                      (self.directory / f"daemon-{self.generation}.log").read_text().splitlines()
                      if line.startswith("{")]
            completed = {event.get("operation_id") for event in events if event.get("event") == "compaction_completed"}
            return expected_operations <= completed
        self.until("intervening native compaction did not complete", compactions_completed)
        assert self.rows("orders", original) == original_rows, "idle DDL or compaction changed retained history"
        dump(self.directory / "idle-ddl-before-metadata.json", original)
        dump(self.directory / "idle-ddl-after-metadata.json", current)
        return self.compare("idle-add-after-response-loss") | {
            "schema_requests": schema_requests, "intervening_compactions": intervening,
            "historical_snapshot_rows": len(original_rows)}

    def queued_versions(self):
        self.proxy.hold_commits()
        try:
            self.transaction(["UPDATE orders SET payload='old-wire-shape' WHERE id <= 8"])
            assert self.proxy.commit_held.wait(timeout=30)
            self.pg.execute("ALTER TABLE orders ADD COLUMN comment text DEFAULT NULL")
            self.transaction(["UPDATE orders SET comment='first-add', occurred_at='2024-11-03 01:30:00.123456-07' WHERE id <= 16"])
            self.pg.execute("ALTER TABLE orders ADD COLUMN measure numeric(18,4)")
            barrier = self.transaction([
                "UPDATE orders SET measure=-123.4567, occurred_at='2024-11-03 01:30:00.654321-08' WHERE id BETWEEN 9 AND 24",
                "DELETE FROM orders WHERE id BETWEEN 40 AND 47",
                "UPDATE orders SET id=id+100000 WHERE id BETWEEN 48 AND 55",
                "UPDATE accounts SET amount=amount+0.25 WHERE id <= 4",
            ])
            self.until("new schema transactions were not durable behind blocked catalog", lambda:
                       self.metrics().get("flow_journal_durable_lsn", 0) >= barrier)
            self.stop(crash=True)
        finally:
            self.proxy.release_commits.set()
        self.start()
        self.wait_materialized(barrier)
        result = self.compare("queued-schema-versions-after-sigkill")
        assert [field["id"] for field in self.fields("orders")] == list(range(1, 14))
        assert self.rows("orders", self.initial_metadata) == self.initial_rows
        return result

    def same_transaction_schema(self):
        before = len(self.fields("orders"))
        barrier = self.transaction([
            "UPDATE orders SET comment='before-add-in-tx' WHERE id=1",
            "SAVEPOINT transient_schema",
            "ALTER TABLE orders ADD COLUMN discarded_in_savepoint text",
            "UPDATE orders SET discarded_in_savepoint='discarded' WHERE id=2",
            "ROLLBACK TO SAVEPOINT transient_schema",
            "ALTER TABLE orders ADD COLUMN in_transaction boolean",
            "UPDATE orders SET in_transaction=true, comment='after-add-in-tx' WHERE id=3",
        ])
        self.wait_materialized(barrier)
        assert len(self.fields("orders")) == before + 1
        assert self.fields("orders")[-1]["name"] == "in_transaction"
        return self.compare("same-transaction-add-and-savepoint-rollback")

    def stopped_schema_history(self):
        self.stop(crash=True)
        self.pg.execute("ALTER TABLE orders ADD COLUMN during_stop_a text")
        self.transaction(["UPDATE orders SET during_stop_a='first-stopped-version' WHERE id <= 4"])
        self.pg.execute("ALTER TABLE orders ADD COLUMN during_stop_b bigint")
        barrier = self.transaction(["UPDATE orders SET during_stop_b=42 WHERE id BETWEEN 3 AND 6"])
        self.start()
        self.wait_materialized(barrier)
        return self.compare("historical-schema-widths-after-restart")

    def unsafe_schema(self):
        before = self.table("orders")
        case = self.args.unsafe_case
        if case == "column-incarnation":
            self.pg.execute("ALTER TABLE orders DROP COLUMN ratio, ADD COLUMN ratio double precision")
            expected = "column was dropped or replaced"
        elif case == "non-null-default":
            self.pg.execute("ALTER TABLE orders ADD COLUMN unsafe integer DEFAULT 7")
            self.pg.execute("ALTER TABLE orders ALTER COLUMN unsafe DROP DEFAULT")
            expected = "new columns must be nullable"
        elif case == "volatile-default":
            self.pg.execute("ALTER TABLE orders ADD COLUMN unsafe double precision DEFAULT random()")
            self.pg.execute("ALTER TABLE orders ALTER COLUMN unsafe DROP DEFAULT")
            expected = "heap was rewritten"
        else:
            raise ValueError(case)
        code = self.process.wait(timeout=25)
        assert code != 0, f"unsafe {case} was accepted"
        log = (self.directory / f"daemon-{self.generation}.log").read_text()
        assert expected in log, f"wrong rejection for {case}: {log[-2000:]}"
        after = self.table("orders")
        assert after["metadata"]["current-schema-id"] == before["metadata"]["current-schema-id"]
        assert self.rows("orders", before) == self.rows("orders", after), "unsafe DDL changed public rows"
        return {"case": case, "rejection": expected, "exit_code": code}

    def aborted_ddl(self):
        before = self.fields("orders")
        self.pg.execute("BEGIN")
        try:
            self.pg.execute("ALTER TABLE orders ADD COLUMN rolled_back text")
            # Exceeds logical_decoding_work_mem in the production fixture and
            # crosses the streamed abort path with a transient relation shape.
            self.pg.execute("UPDATE orders SET payload=repeat(md5(id::text),128), rolled_back='never-visible'")
        finally:
            self.pg.execute("ROLLBACK")
        barrier = self.transaction(["UPDATE orders SET comment='after-aborted-ddl' WHERE id <= 8"])
        self.wait_materialized(barrier)
        assert self.fields("orders") == before
        return self.compare("aborted-streamed-ddl")

    def checkpoint_restore(self):
        def retained():
            paths = list((self.directory / "state" / "checkpoints").glob("*/CURRENT"))
            return paths if len(paths) == 2 else None
        self.until("retained index checkpoints were not created", retained, timeout=40)
        # Two further health cycles exercise retirement, not just creation.
        time.sleep(11)
        assert len(retained()) == 2
        self.stop(crash=True)
        lost = self.directory / "lost-index"
        lost.mkdir()
        for name in ("index", "index-generations"):
            path = self.directory / "state" / name
            if path.exists():
                path.rename(lost / name)
        self.start()
        def restored():
            events = [json.loads(line).get("fields", {}) for line in
                      (self.directory / f"daemon-{self.generation}.log").read_text().splitlines() if line.startswith("{")]
            return next((event for event in events if event.get("event") == "index_generation_activated"), None)
        event = self.until("missing index was not recovered", restored)
        assert event["restored_checkpoint"], "matching retained checkpoint was not used"
        barrier = self.transaction(["UPDATE orders SET comment='after-checkpoint' WHERE id <= 8",
                                    "DELETE FROM orders WHERE id BETWEEN 60 AND 63"])
        self.wait_materialized(barrier)
        return self.compare("checkpoint-restore-after-ddl")

    def terminate(self):
        status = json.loads(subprocess.check_output(self.command("status"), env=self.environment))
        assert status["ready"]
        self.process.send_signal(signal.SIGTERM)
        assert self.process.wait(timeout=15) == 0, "SIGTERM did not shut down gracefully"
        status = json.loads(subprocess.check_output(self.command("status"), env=self.environment))
        assert not status["ready"]
        self.stop()
        self.start()
        barrier = self.transaction(["UPDATE accounts SET amount=amount+0.5 WHERE id=1"])
        self.wait_materialized(barrier)
        return self.compare("sigterm-restart")

    def key_drift(self):
        self.pg.execute("ALTER TABLE orders DROP CONSTRAINT orders_pkey, ADD PRIMARY KEY (tenant,id)")
        code = self.process.wait(timeout=20)
        assert code != 0, "primary-key contract drift was accepted"
        log = (self.directory / f"daemon-{self.generation}.log").read_text()
        assert "primary key" in log.lower() or "primary-key" in log.lower()
        self.pg.execute("ALTER TABLE orders DROP CONSTRAINT orders_pkey, ADD PRIMARY KEY (id)")
        self.stop()
        self.start()
        barrier = self.transaction(["UPDATE orders SET comment='restored-key-contract' WHERE id <= 8"])
        self.wait_materialized(barrier)
        return self.compare("source-key-contract-restored")

    def execute(self):
        try:
            self.phase("seed", self.seed)
            self.phase("initial-copy", self.initialize)
            self.start()
            self.phase("snapshot-wal-handoff", self.handoff)
            if self.args.unsafe_case:
                self.phase(self.args.unsafe_case, self.unsafe_schema)
                self.stop()
                self.check_worker_panics()
                self.report["passed"] = True
                return
            self.phase("idle-nullable-add-with-lost-response", self.idle_add)
            self.phase("queued-schema-versions-and-crash", self.queued_versions)
            self.phase("streamed-aborted-ddl", self.aborted_ddl)
            self.phase("same-transaction-schema-and-savepoint", self.same_transaction_schema)
            self.phase("historical-schema-widths-after-stop", self.stopped_schema_history)
            self.phase("checkpoint-retention-and-restore", self.checkpoint_restore)
            self.phase("graceful-sigterm-and-status", self.terminate)
            self.phase("primary-key-drift", self.key_drift)
            self.stop()
            self.check_worker_panics()
            self.report["passed"] = True
        except BaseException as error:
            self.report.update(error=str(error), traceback=traceback.format_exc())
            raise
        finally:
            self.stop()
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
    parser.add_argument("--unsafe-case", choices=("column-incarnation", "non-null-default", "volatile-default"))
    args = parser.parse_args()
    ContractRun(args).execute()
    print(f"PASS: {args.artifacts.resolve() / 'report.json'}", flush=True)


if __name__ == "__main__":
    main()
