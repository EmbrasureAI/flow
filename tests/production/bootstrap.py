#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["duckdb==1.5.5", "psycopg[binary]==3.3.5", "boto3==1.43.88", "fastavro==1.12.2"]
# ///
"""Crash COPY at durable boundaries while CDC changes both tables; recover exact public rows."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import threading
import time
import traceback

import psycopg
from psycopg import sql

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "local"))
from run import Run, dump, lsn
from health import HealthProxy
from proxy import CatalogProxy
from process_lifecycle import run_supervised, arm_workload, disarm_workload


class BootstrapRun(Run):
    def __init__(self, args):
        super().__init__(args)
        self.writer_lock = threading.Lock()
        self.writer_connection = None
        self.copy_proxy = HealthProxy(args.postgres_url, self.directory / "copy-proxy.json",
            query_fragments=(b"COPY (SELECT ", f'FROM ONLY "{self.name}"."orders"'.encode()))
        self.catalog_proxy = CatalogProxy(args.catalog_uri, self.directory / "catalog-proxy.jsonl")
        args.catalog_uri = self.catalog_proxy.url
        self.configure()
        self.environment.update(FLOW_LOCAL_POSTGRES_URL=self.copy_proxy.connection, RUST_LOG="info,flow_events=debug")

    def seed(self):
        super().seed()
        self.pg.execute("INSERT INTO orders SELECT id + 4096, tenant, payload, amount, active, day, stamp, token, data, ratio FROM orders")
        self.pg.execute("UPDATE orders SET payload = repeat(md5(id::text), 512)")
        self.order_oid = self.pg.execute("SELECT 'orders'::regclass::oid").fetchone()[0]
        self.account_oid = self.pg.execute("SELECT 'accounts'::regclass::oid").fetchone()[0]
        return {"orders": 8192, "accounts": 16, "payload_bytes": 16384}

    def start_init(self):
        self.generation += 1
        self.log_path = self.directory / f"init-{self.generation}.log"
        self.log = self.log_path.open("wb")
        self.process = subprocess.Popen(self.command("init"), env=self.environment | {"RUST_LOG": "info,flow_events=debug"}, stdout=self.log, stderr=subprocess.STDOUT)

    def events(self):
        events = []
        for line in self.log_path.read_text().splitlines():
            try:
                events.append(json.loads(line).get("fields", {}))
            except json.JSONDecodeError:
                pass
        return events

    def wait_event(self, predicate, description):
        deadline = time.monotonic() + self.args.timeout
        while time.monotonic() < deadline:
            self.alive()
            events = self.events()
            if predicate(events):
                return events
            time.sleep(0.005)
        raise TimeoutError(description)

    def writer(self):
        try:
            with psycopg.connect(self.args.postgres_url, autocommit=True, connect_timeout=5,
                                  options="-c statement_timeout=5000") as pg:
                with self.writer_lock:
                    self.writer_connection = pg
                pg.execute(sql.SQL("SET search_path TO {}, public").format(sql.Identifier(self.name)))
                while not self.writer_stop.is_set():
                    commit = self.commits + 1
                    with pg.transaction():
                        pg.execute("UPDATE orders SET amount = %s WHERE id = 1", (commit,))
                        pg.execute("UPDATE accounts SET amount = %s WHERE id = 1", (commit,))
                        if commit == 1:
                            pg.execute("DELETE FROM orders WHERE id = 2")
                            pg.execute("INSERT INTO orders (id, tenant, payload) VALUES (9000, 2, 'concurrent-insert')")
                        if commit == 3:
                            pg.execute("UPDATE orders SET id = 9001 WHERE id = 3")
                        barrier = lsn(pg.execute("SELECT pg_current_wal_insert_lsn()::text").fetchone()[0])
                    self.commits = commit
                    self.barrier = barrier
                    self.writer_stop.wait(0.05)
        except BaseException as error:
            if not (self.writer_stop.is_set() and isinstance(error, psycopg.errors.QueryCanceled)):
                self.writer_error = error
        finally:
            with self.writer_lock:
                self.writer_connection = None

    def stop_writer(self):
        if not hasattr(self, "writer_stop"):
            return
        self.writer_stop.set()
        # An OS/network stall can outlive server statement_timeout. The external
        # supervisor bounds this entire cancellation/join path as a final guard.
        arm_workload(self.directory, time.monotonic() + 20)
        with self.writer_lock:
            pg = self.writer_connection
        cancel_error = None
        if pg is not None:
            try:
                pg.cancel_safe(timeout=3)
            except psycopg.Error as error:
                cancel_error = str(error)
        self.writer_thread.join(timeout=10)
        assert not self.writer_thread.is_alive(), "bootstrap writer did not stop after cancellation"
        disarm_workload(self.directory)
        self.report["writer_shutdown"] = {"joined": True, "connection_closed": pg is None or pg.closed,
                                          "cancel_error": cancel_error}
        if self.writer_error:
            raise self.writer_error

    def kill_during_copy(self):
        self.start_init()
        self.until("permanent slot was not created", lambda: self.pg.execute(
            "SELECT 1 FROM pg_replication_slots WHERE slot_name = %s", (self.name,)).fetchone())
        self.writer_stop = threading.Event()
        self.writer_error = None
        self.commits = 0
        self.barrier = 0
        self.writer_thread = threading.Thread(target=self.writer)
        self.writer_thread.start()
        self.until("orders COPY was not held unsent", lambda: self.copy_proxy.active == 1)
        events = self.wait_event(lambda events: any(event.get("event") == "bootstrap_table_published" and event.get("table_id") == self.account_oid for event in events)
                                and any(event.get("event") == "transaction_journaled" for event in events), "small base or concurrent CDC was not published")
        self.stop(crash=True)
        events = self.events()
        assert not any(event.get("event") in ("bootstrap_table_staged", "bootstrap_table_published")
                       and event.get("table_id") == self.order_oid for event in events), "orders crossed held COPY boundary"
        self.account_snapshot = self.table("accounts")["metadata"]["current-snapshot-id"]
        self.copy_proxy.stall_health.clear()
        return {"commits_during_copy": self.commits, "published_account_snapshot": self.account_snapshot,
                "orders_staging_bytes": sum(path.stat().st_size for path in (self.directory / "state" / "bootstrap").rglob("*.segment"))}

    def kill_after_terminal(self):
        if self.args.lose_index_during_bootstrap:
            index = self.directory / "state" / "index"
            index.rename(index.with_name("index-lost-during-bootstrap"))
        if self.args.add_column_during_recovery:
            self.pg.execute("ALTER TABLE orders ADD COLUMN added text")
            self.pg.execute("UPDATE orders SET added = 'new-column-before-recopy' WHERE id % 17 = 0")
        self.catalog_proxy.hold_commits(kind="ingest", table="orders")
        self.start_init()
        self.until("staged orders publication was not held", self.catalog_proxy.commit_held.is_set)
        self.stop(crash=True)
        events = self.events()
        assert not any(event.get("event") == "bootstrap_table_published" and event.get("table_id") == self.order_oid for event in events), "publication finished before staging crash boundary"
        staged = next(event for event in events if event.get("event") == "bootstrap_table_staged" and event.get("table_id") == self.order_oid)
        self.staged_cut = staged["lsn"]
        prepared = next(event for event in events if event.get("event") == "ingest_prepared"
                        and event.get("table_id") == self.order_oid)
        self.staged_operation = prepared["operation_id"]
        self.catalog_proxy.release_commits.set()
        assert self.table("accounts")["metadata"]["current-snapshot-id"] == self.account_snapshot, "already published table was copied again"
        return {"orders_staged_cut": self.staged_cut, "orders_prepared_operation": self.staged_operation,
                "rows": staged["rows"], "commits": self.commits}

    def finish_bootstrap(self):
        self.start_init()
        code = self.process.wait(timeout=self.args.timeout)
        if code != 0:
            raise RuntimeError(f"resumed initialization exited {code}; see {self.log_path}")
        events = self.events()
        assert not any(event.get("event") == "bootstrap_table_staged" for event in events), "durable staged data was unnecessarily recopied"
        # A Prepared catalog operation can finish in startup recovery before the
        # COPY planner runs, so no new bootstrap_table_published event is required.
        snapshots = [snapshot for snapshot in self.table("orders")["metadata"]["snapshots"]
                     if snapshot["summary"].get("flow.operation-id") == self.staged_operation]
        assert len(snapshots) == 1, "staged orders operation was replaced or published twice"
        assert snapshots[0]["summary"]["streaming.last-lsn"] == self.staged_cut
        self.stop()
        self.stop_writer()
        self.start()
        self.wait_materialized(self.barrier)
        self.verify("bootstrap")
        return {"source_commits": self.commits, "orders_cut_preserved": self.staged_cut}

    def verify(self, phase):
        for table in ("orders", "accounts"):
            columns = "id, tenant, md5(payload), amount, active, day, stamp, token, data, ratio" if table == "orders" else "id, amount"
            if table == "orders" and self.args.add_column_during_recovery:
                columns += ", added"
            expected = self.pg.execute(sql.SQL(f"SELECT {columns} FROM {{}} ORDER BY id").format(sql.Identifier(table))).fetchall()
            metadata = self.table(table)
            actual = self.duck.execute(f"SELECT {columns} FROM iceberg_scan(?) ORDER BY id", [metadata["metadata-location"]]).fetchall()
            assert actual == expected, f"{phase}: {table} public rows differ; actual={len(actual)} expected={len(expected)}"
            dump(self.directory / f"{phase}-{table}-metadata.json", metadata)

    def index_loss(self):
        self.stop(crash=True)
        # Earlier bootstrap recovery may already have activated a generation.
        # Removing only the legacy directory would leave the active index intact.
        missing = self.directory / "lost-row-index"
        missing.mkdir()
        moved = []
        for name in ("index", "index-generations"):
            index = self.directory / "state" / name
            if index.exists():
                index.rename(missing / name)
                moved.append(name)
        assert moved, "no replaceable row index was removed"
        assert (self.directory / "state" / "control" / "CURRENT").exists()
        self.start()
        self.until("index reconstruction did not finish", lambda: '"index_generation_activated"' in (self.directory / f"daemon-{self.generation}.log").read_text())
        barrier = self.transaction(["UPDATE orders SET amount = amount + 3 WHERE id IN (1, 9001)", "DELETE FROM orders WHERE id = 9000", "UPDATE accounts SET amount = amount + 4 WHERE id = 1"])
        self.wait_materialized(barrier)
        self.verify("index-loss")
        return {"post_rebuild_updates_and_delete": True, "removed_index_directories": moved}

    def execute(self):
        try:
            self.phase("seed", self.seed)
            self.phase("kill-during-copy-after-other-base-and-cdc", self.kill_during_copy)
            self.phase("kill-after-durable-staging", self.kill_after_terminal)
            self.phase("resume-staged-copy-and-cdc", self.finish_bootstrap)
            self.phase("total-index-loss-and-rebuild", self.index_loss)
            self.report["passed"] = True
        except BaseException as error:
            self.report["error"] = str(error)
            self.report["traceback"] = traceback.format_exc()
            raise
        finally:
            original_failure = sys.exc_info()[1]
            cleanup_error = None
            dump(self.directory / "report.json", self.report)
            for cleanup in (self.stop_writer, self.stop, self.copy_proxy.close,
                            self.catalog_proxy.close, self.pg.close, self.duck.close):
                try:
                    cleanup()
                except BaseException as error:
                    cleanup_error = cleanup_error or error
                    self.report.setdefault("cleanup_errors", []).append(str(error))
                    self.report["passed"] = False
            dump(self.directory / "report.json", self.report)
            if cleanup_error is not None and original_failure is None:
                raise cleanup_error


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--postgres-url", default=os.environ.get("FLOW_POSTGRES_URL"))
    parser.add_argument("--catalog-uri", required=True)
    parser.add_argument("--s3-endpoint", required=True)
    parser.add_argument("--warehouse", default="s3://warehouse/")
    parser.add_argument("--binary", type=Path, default=Path("target/debug/embrasure-flow"))
    parser.add_argument("--artifacts", type=Path, required=True)
    parser.add_argument("--timeout", type=float, default=180)
    parser.add_argument("--lose-index-during-bootstrap", action="store_true",
                        help="require index reconstruction while one table is copied and another is unfinished")
    parser.add_argument("--add-column-during-recovery", action="store_true", help="add and populate a nullable column between the interrupted COPY and its newer snapshot")
    parser.add_argument("--supervised-worker", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()
    if not args.supervised_worker:
        result = run_supervised([sys.executable, str(Path(__file__).resolve()), *sys.argv[1:], "--supervised-worker"],
                                args.artifacts, timeout=12 * args.timeout + 120)
        raise SystemExit(result.returncode)
    os.environ["RUST_LOG"] = "info,flow_events=debug"
    BootstrapRun(args).execute()
    print(f"PASS: {args.artifacts.resolve() / 'report.json'}", flush=True)


if __name__ == "__main__":
    main()
