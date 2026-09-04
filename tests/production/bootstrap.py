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


class BootstrapRun(Run):
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
        self.process = subprocess.Popen(self.command("init"), env=self.environment | {"RUST_LOG": "info"}, stdout=self.log, stderr=subprocess.STDOUT)

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
            with psycopg.connect(self.args.postgres_url, autocommit=True) as pg:
                pg.execute(sql.SQL("SET search_path TO {}, public").format(sql.Identifier(self.name)))
                while not self.writer_stop.is_set():
                    self.commits += 1
                    with pg.transaction():
                        pg.execute("UPDATE orders SET amount = %s WHERE id = 1", (self.commits,))
                        pg.execute("UPDATE accounts SET amount = %s WHERE id = 1", (self.commits,))
                        if self.commits == 1:
                            pg.execute("DELETE FROM orders WHERE id = 2")
                            pg.execute("INSERT INTO orders (id, tenant, payload) VALUES (9000, 2, 'concurrent-insert')")
                        if self.commits == 3:
                            pg.execute("UPDATE orders SET id = 9001 WHERE id = 3")
                        self.barrier = lsn(pg.execute("SELECT pg_current_wal_insert_lsn()::text").fetchone()[0])
                    self.writer_stop.wait(0.05)
        except BaseException as error:
            self.writer_error = error

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
        events = self.wait_event(lambda events: any(event.get("event") == "bootstrap_table_published" and event.get("table_id") == self.account_oid for event in events)
                                and any(event.get("event") == "transaction_journaled" for event in events), "small base or concurrent CDC was not published")
        assert not any(event.get("event") == "bootstrap_table_staged" and event.get("table_id") == self.order_oid for event in events), "large COPY finished before crash boundary"
        self.account_snapshot = self.table("accounts")["metadata"]["current-snapshot-id"]
        self.stop(crash=True)
        return {"commits_during_copy": self.commits, "published_account_snapshot": self.account_snapshot,
                "orders_staging_bytes": sum(path.stat().st_size for path in (self.directory / "state" / "bootstrap").rglob("*.segment"))}

    def kill_after_terminal(self):
        if self.args.add_column_during_recovery:
            self.pg.execute("ALTER TABLE orders ADD COLUMN added text")
            self.pg.execute("UPDATE orders SET added = 'new-column-before-recopy' WHERE id % 17 = 0")
        self.start_init()
        events = self.wait_event(lambda events: any(event.get("event") == "bootstrap_table_staged" and event.get("table_id") == self.order_oid for event in events), "resumed COPY did not reach durable staging")
        assert not any(event.get("event") == "bootstrap_table_published" and event.get("table_id") == self.order_oid for event in events), "publication finished before staging crash boundary"
        staged = next(event for event in events if event.get("event") == "bootstrap_table_staged" and event.get("table_id") == self.order_oid)
        self.staged_cut = staged["lsn"]
        self.stop(crash=True)
        assert self.table("accounts")["metadata"]["current-snapshot-id"] == self.account_snapshot, "already published table was copied again"
        return {"orders_staged_cut": self.staged_cut, "rows": staged["rows"], "commits": self.commits}

    def finish_bootstrap(self):
        self.start_init()
        code = self.process.wait(timeout=self.args.timeout)
        if code != 0:
            raise RuntimeError(f"resumed initialization exited {code}; see {self.log_path}")
        events = self.events()
        assert not any(event.get("event") == "bootstrap_table_staged" for event in events), "durable staged data was unnecessarily recopied"
        published = next(event for event in events if event.get("event") == "bootstrap_table_published" and event.get("table_id") == self.order_oid)
        assert published["lsn"] == self.staged_cut
        self.stop()
        self.writer_stop.set()
        self.writer_thread.join(timeout=15)
        assert not self.writer_thread.is_alive()
        if self.writer_error:
            raise self.writer_error
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
        index = self.directory / "state" / "index"
        index.rename(index.with_name("index-lost-for-test"))
        self.start()
        self.until("index reconstruction did not finish", lambda: '"index_generation_activated"' in (self.directory / f"daemon-{self.generation}.log").read_text())
        barrier = self.transaction(["UPDATE orders SET amount = amount + 3 WHERE id IN (1, 9001)", "DELETE FROM orders WHERE id = 9000", "UPDATE accounts SET amount = amount + 4 WHERE id = 1"])
        self.wait_materialized(barrier)
        self.verify("index-loss")
        return {"post_rebuild_updates_and_delete": True}

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
            if hasattr(self, "writer_stop"):
                self.writer_stop.set()
                self.writer_thread.join(timeout=15)
            self.stop()
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
    parser.add_argument("--add-column-during-recovery", action="store_true", help="add and populate a nullable column between the interrupted COPY and its newer snapshot")
    args = parser.parse_args()
    os.environ["RUST_LOG"] = "info"
    BootstrapRun(args).execute()
    print(f"PASS: {args.artifacts.resolve() / 'report.json'}", flush=True)


if __name__ == "__main__":
    main()
