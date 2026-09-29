#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["duckdb==1.5.5", "psycopg[binary]==3.3.5", "boto3==1.43.88", "fastavro==1.12.2"]
# ///
"""Composite-key recovery and capture-quota ACK fencing against persistent local services."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import sys
import time
import traceback

from psycopg import sql

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "local"))
from run import Run, dump, lsn
from proxy import CatalogProxy


class BoundaryRun(Run):
    QUOTA_BYTES = 64 << 20  # The existing journal segment is 64 MiB.
    RECOVERY_BYTES = 256 << 20
    QUOTA_BATCHES = 10
    QUOTA_ROWS = 512
    PAYLOAD_BYTES = 16384

    def __init__(self, args):
        super().__init__(args)
        self.proxy = CatalogProxy(args.catalog_uri, self.directory / "catalog-proxy.jsonl")
        self.config.write_text(self.config.read_text().replace(
            json.dumps(args.catalog_uri), json.dumps(self.proxy.url)))
        self.environment["RUST_LOG"] = "info"
        self.histories = {}
        self.report.update(primary_key=["id", "tenant"], journal_quota_bytes=self.QUOTA_BYTES,
                           quota_source_rows_max=self.QUOTA_BATCHES * self.QUOTA_ROWS,
                           quota_payload_bytes=self.PAYLOAD_BYTES)

    def configure(self):
        super().configure()
        self.config.write_text(self.config.read_text()
                               .replace("primary_key = [0]", "primary_key = [0, 1]", 1)
                               .replace("journal_bytes = 268435456", f"journal_bytes = {self.QUOTA_BYTES}"))

    def phase(self, name, action):
        self.report["active_phase"] = name
        dump(self.directory / "report.json", self.report)
        super().phase(name, action)
        self.report.pop("active_phase", None)

    def seed(self):
        result = super().seed()
        self.pg.execute("ALTER TABLE orders DROP CONSTRAINT orders_pkey, ADD PRIMARY KEY (id, tenant)")
        return result | {"orders_primary_key": ["id", "tenant"]}

    def rows(self, table, metadata=None):
        metadata = metadata or self.table(table)
        order = "id, tenant" if table == "orders" else "id"
        return self.duck.execute(f"SELECT * FROM iceberg_scan(?) ORDER BY {order}",
                                 [metadata["metadata-location"]]).fetchall()

    def compare(self, label):
        result = {}
        for table in ("orders", "accounts"):
            order = sql.SQL("id, tenant" if table == "orders" else "id")
            expected = self.pg.execute(sql.SQL("SELECT * FROM {} ORDER BY {}")
                                       .format(sql.Identifier(table), order)).fetchall()
            metadata = self.table(table)
            actual = self.rows(table, metadata)
            assert actual == expected, f"{label}: {table} source/DuckDB rows differ ({len(expected)} / {len(actual)})"
            dump(self.directory / f"{label}-{table}-metadata.json", metadata)
            result[table] = {"rows": len(actual), "snapshot": metadata["metadata"]["current-snapshot-id"],
                             "sha256": hashlib.sha256(json.dumps(actual, default=str).encode()).hexdigest()}
        return result

    def events(self):
        for line in (self.directory / f"daemon-{self.generation}.log").read_text(errors="replace").splitlines():
            try:
                yield json.loads(line).get("fields", {})
            except ValueError:
                pass  # A running process can leave an incomplete final log line.

    def start_ready(self):
        started = time.time_ns() // 1_000_000
        self.start()
        def fresh():
            path = self.directory / "state/status.json"
            if not path.exists():
                return None
            status = json.loads(path.read_text())
            return status if (status.get("process_id") == self.process.pid and status.get("ready")
                              and status.get("updated_at_ms", 0) >= started) else None
        return self.until("new daemon did not publish fresh ready status", fresh)

    def slot(self):
        row = self.pg.execute("SELECT slot_name, plugin, database, confirmed_flush_lsn::text "
                              "FROM pg_replication_slots WHERE slot_name=%s", (self.name,)).fetchone()
        assert row is not None, "original source slot disappeared"
        return dict(zip(("name", "plugin", "database", "confirmed_flush_lsn"), row))

    def identity(self):
        state = self.directory / "state"
        return {name: hashlib.sha256((state / name).read_bytes()).hexdigest()
                for name in ("source-identity.json", "control/IDENTITY")}

    def remember(self, label):
        saved = {table: self.table(table) for table in ("orders", "accounts")}
        self.histories[label] = {table: (metadata, self.rows(table, metadata)) for table, metadata in saved.items()}
        for table, metadata in saved.items():
            dump(self.directory / f"{label}-history-{table}.json", metadata)
        return {table: len(rows) for table, (_, rows) in self.histories[label].items()}

    def history(self):
        for label, tables in self.histories.items():
            for table, (metadata, expected) in tables.items():
                assert self.rows(table, metadata) == expected, f"{label}: historical {table} rows changed"
        return {label: {table: len(rows) for table, (_, rows) in tables.items()}
                for label, tables in self.histories.items()}

    def ack_reached(self, barrier):
        self.until("source slot ACK did not resume", lambda: lsn(self.slot()["confirmed_flush_lsn"]) >= barrier)
        confirmed = self.slot()["confirmed_flush_lsn"]
        # Metrics export once per second. ACK can already include a later
        # no-row transaction while the file still reflects the original
        # barrier. Catch the sampled ACK in the exported observation instead
        # of comparing a newer database read with an older metrics snapshot.
        metrics = self.wait_materialized(lsn(confirmed))
        assert lsn(confirmed) <= metrics["flow_materialized_lsn"]
        return {"confirmed_flush_lsn": confirmed, "materialized_lsn": metrics["flow_materialized_lsn"]}

    def ack_held(self, barrier):
        slot = self.slot()
        metrics = self.metrics()
        assert lsn(slot["confirmed_flush_lsn"]) < barrier, "slot ACK crossed held catalog publication"
        assert metrics["flow_materialized_lsn"] < barrier
        assert lsn(slot["confirmed_flush_lsn"]) <= metrics["flow_materialized_lsn"] <= metrics["flow_journal_durable_lsn"]
        return {"slot": slot, "metrics": metrics, "held_transaction_barrier": barrier}

    def release_held(self, event_start):
        self.proxy.release_commits.set()
        return self.until("held catalog request did not finish after release", lambda:
                          [event for event in self.proxy.events[event_start:] if event.get("held_before_upstream")],
                          timeout=35)

    def composite_changes(self):
        self.transaction([
            "UPDATE orders SET tenant=tenant+100, payload='tenant-component-move' WHERE id BETWEEN 1 AND 8",
            "UPDATE orders SET id=id+100000 WHERE id BETWEEN 9 AND 12",
            "INSERT INTO orders(id,tenant,payload) VALUES (1,501,'same-id-a'),(1,502,'same-id-b'),(-9223372036854775807,-2147483648,'signed-min')",
            "UPDATE accounts SET amount=amount+1 WHERE id=1",
        ])
        self.transaction(["DELETE FROM orders WHERE id=1 AND tenant=501"])
        barrier = self.transaction([
            "INSERT INTO orders(id,tenant,payload) VALUES (1,501,'reborn-before-crash')",
            "UPDATE orders SET payload=NULL WHERE id=1 AND tenant=502",
        ])
        self.wait_materialized(barrier)
        assert self.pg.execute("SELECT count(*) FROM orders WHERE id=1").fetchone()[0] == 3
        self.source_identity = self.identity()
        self.slot_identity = {key: value for key, value in self.slot().items() if key != "confirmed_flush_lsn"}
        return self.compare("composite-components-and-reinsert") | {"history": self.remember("before-crash")}

    def composite_recovery(self):
        event_start = len(self.proxy.events)
        self.proxy.hold_commits()
        try:
            first = self.transaction([
                "UPDATE orders SET tenant=601,payload='pending-key-move' WHERE id=1 AND tenant=501",
                "DELETE FROM orders WHERE id=1 AND tenant=502",
                "UPDATE accounts SET amount=amount+2 WHERE id=2",
            ])
            barrier = self.transaction([
                "INSERT INTO orders(id,tenant,payload) VALUES (1,502,'reborn-pending'),(9223372036854775800,2147483647,'signed-max')",
                "UPDATE orders SET id=-4 WHERE id=4 AND tenant=104",
            ])
            assert self.proxy.commit_held.wait(timeout=20), "no real ingestion POST reached the hold"
            self.until("composite transactions were not journaled", lambda:
                       self.metrics().get("flow_journal_durable_lsn", 0) >= barrier, timeout=20)
            evidence = self.ack_held(first)
            self.stop(crash=True)
            missing = self.directory / "lost-row-index"
            missing.mkdir()
            moved = []
            for name in ("index", "index-generations"):
                path = self.directory / "state" / name
                if path.exists():
                    path.rename(missing / name)
                    moved.append(name)
            assert moved and (self.directory / "state/control/CURRENT").exists()
        finally:
            self.proxy.release_commits.set()
        released = self.release_held(event_start)
        self.start_ready()
        self.wait_materialized(barrier)
        activated = [event for event in self.events() if event.get("event") == "index_generation_activated"]
        assert len(activated) == 1 and not activated[0]["restored_checkpoint"], "missing row index was not reconstructed"
        assert self.identity() == self.source_identity
        assert {key: value for key, value in self.slot().items() if key != "confirmed_flush_lsn"} == self.slot_identity
        recovered = self.compare("composite-pending-index-rebuild")
        snapshots = [snapshot for table in ("orders", "accounts") for snapshot in self.table(table)["metadata"]["snapshots"]]
        for operation in {event.get("operation_id") for event in released} - {None}:
            assert sum(snapshot["summary"].get("flow.operation-id") == operation for snapshot in snapshots) == 1
        barrier = self.transaction([
            "UPDATE orders SET tenant=-7,payload='after-rebuild' WHERE id=1 AND tenant=601",
            "DELETE FROM orders WHERE id=1 AND tenant=502",
            "UPDATE orders SET tenant=2147483646 WHERE id=9223372036854775800",
            "UPDATE accounts SET amount=amount-0.5 WHERE id=2",
        ])
        self.wait_materialized(barrier)
        return {"during_fault": evidence, "removed_index_directories": moved, "activation": activated[0],
                "released_requests": released, "recovered": recovered,
                "post_rebuild": self.compare("composite-post-rebuild-crud"),
                "ack": self.ack_reached(barrier), "history": self.history()}

    def quota_recovery(self):
        before = self.remember("before-quota")
        event_start = len(self.proxy.events)
        self.proxy.hold_commits()
        cohort = []
        self.report["quota_committed_transactions"] = cohort
        try:
            first = self.transaction([
                "UPDATE orders SET payload='quota-held' WHERE id=2",
                "UPDATE accounts SET amount=amount+3 WHERE id=3",
            ])
            assert self.proxy.commit_held.wait(timeout=20), "quota case never held a real ingestion POST"
            self.until("held quota transaction was not journaled", lambda:
                       self.metrics().get("flow_journal_durable_lsn", 0) >= first, timeout=20)
            # At most 80 MiB of committed row payloads; actual encoded journal
            # accounting, not this nominal payload size, must trigger the error.
            for batch in range(self.QUOTA_BATCHES):
                start = 2000000 + batch * self.QUOTA_ROWS
                barrier = self.transaction([
                    f"INSERT INTO orders(id,tenant,payload) SELECT i,i%11,repeat(md5(i::text),512) "
                    f"FROM generate_series({start},{start + self.QUOTA_ROWS - 1}) i",
                ])
                cohort.append({"first_id": start, "rows": self.QUOTA_ROWS, "barrier": barrier})
                dump(self.directory / "report.json", self.report)
                if self.process.poll() is not None:
                    break
            code = self.process.wait(timeout=20)
            assert code != 0, "capture quota exhaustion did not fail the daemon"
            log = (self.directory / f"daemon-{self.generation}.log").read_text()
            quota = re.search(r"journal quota exhausted: (\d+) bytes in use, (\d+) requested, (\d+) quota", log)
            assert quota is not None, "daemon did not report the configured journal quota failure"
            used, requested, capacity = map(int, quota.groups())
            assert used <= capacity == self.QUOTA_BYTES and used + requested > capacity
            durable = max(lsn(event["end_lsn"]) for event in self.events() if event.get("event") == "transaction_journaled")
            assert first <= durable < cohort[-1]["barrier"], "quota case did not leave committed source work beyond the durable journal"
            evidence = self.ack_held(first)
            evidence.update(exit_code=code, used_bytes=used, requested_bytes=requested,
                            capacity_bytes=capacity, last_logged_durable_lsn=durable)
            for table, (metadata, expected) in self.histories["before-quota"].items():
                assert self.rows(table) == expected, "held quota work changed public rows"
            self.stop(crash=True)  # Already exited: close the owned log/process handle.
        finally:
            self.proxy.release_commits.set()
        released = self.release_held(event_start)
        (self.directory / "flow-quota-limited.toml").write_text(self.config.read_text())
        self.config.write_text(self.config.read_text().replace(
            f"journal_bytes = {self.QUOTA_BYTES}", f"journal_bytes = {self.RECOVERY_BYTES}"))
        dump(self.directory / "quota-failure-evidence.json", evidence)
        self.start_ready()
        final_barrier = cohort[-1]["barrier"]
        self.wait_materialized(final_barrier)
        assert self.identity() == self.source_identity
        assert {key: value for key, value in self.slot().items() if key != "confirmed_flush_lsn"} == self.slot_identity
        return {"before_rows": before, "during_fault": evidence, "released_requests": released,
                "recovery_capacity_bytes": self.RECOVERY_BYTES,
                "rows": self.compare("same-slot-quota-recovery"),
                "ack": self.ack_reached(final_barrier), "history": self.history()}

    def final_restart(self):
        process = self.process
        self.stop()
        assert process.returncode == 0, "recovered daemon did not stop cleanly"
        self.start_ready()
        barrier = self.transaction([
            "UPDATE orders SET tenant=-8 WHERE id=1 AND tenant=-7",
            "DELETE FROM orders WHERE id=-9223372036854775807 AND tenant=-2147483648",
            "UPDATE accounts SET amount=amount+0.25 WHERE id=4",
        ])
        self.wait_materialized(barrier)
        return {"rows": self.compare("clean-restart-after-both-boundaries"),
                "ack": self.ack_reached(barrier), "history": self.history()}

    def execute(self):
        try:
            self.phase("seed-composite-primary-key", self.seed)
            self.phase("initial-copy", self.initialize)
            self.start_ready()
            self.phase("snapshot-wal-handoff", self.handoff)
            self.phase("composite-components-and-delete-reinsert", self.composite_changes)
            self.phase("pending-composite-work-crash-and-index-rebuild", self.composite_recovery)
            self.phase("journal-quota-ack-fence-and-same-slot-recovery", self.quota_recovery)
            self.phase("clean-restart-and-final-key-changes", self.final_restart)
            process = self.process
            self.stop()
            assert process.returncode == 0
            self.check_worker_panics()
            self.report["passed"] = True
        except BaseException as error:
            self.report.update(failure=str(error), traceback=traceback.format_exc())
            raise
        finally:
            self.stop()
            self.proxy.close()
            dump(self.directory / "report.json", self.report)
            self.pg.close()
            self.duck.close()
            self.s3.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--postgres-url", default=os.environ.get("FLOW_POSTGRES_URL"))
    parser.add_argument("--catalog-uri", required=True)
    parser.add_argument("--s3-endpoint", required=True)
    parser.add_argument("--warehouse", default="s3://warehouse/")
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--artifacts", type=Path, required=True)
    parser.add_argument("--timeout", type=float, default=180)
    args = parser.parse_args()
    if not args.postgres_url:
        parser.error("provide --postgres-url or FLOW_POSTGRES_URL")
    BoundaryRun(args).execute()
    print(f"PASS: {args.artifacts.resolve() / 'report.json'}", flush=True)


if __name__ == "__main__":
    main()
