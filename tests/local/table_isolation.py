#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["duckdb==1.5.5", "psycopg[binary]==3.3.5", "boto3==1.43.88", "fastavro==1.12.2"]
# ///
"""Exercise table publication isolation with real PostgreSQL and Iceberg readers."""

import argparse
import json
import os
from pathlib import Path
import signal
import sys
import time
import traceback

from run import Run, dump, lsn

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "production"))
from proxy import CatalogProxy


class IsolationRun(Run):
    def __init__(self, args):
        self.proxy = None
        super().__init__(args)
        self.proxy = CatalogProxy(args.catalog_uri, self.directory / "catalog-proxy.jsonl")
        # Keep independent reader requests on the actual catalog, including
        # while the daemon's table loads are denied during recovery.
        self.config.write_text(self.config.read_text().replace(
            f"uri = {json.dumps(args.catalog_uri)}", f"uri = {json.dumps(self.proxy.url)}"))

    def configure(self):
        super().configure()
        self.environment["RUST_LOG"] = "info"
        self.config.write_text(self.config.read_text().replace(
            "pending_transactions = 32", "pending_transactions = 4\ntable_workers = 1\ncollapse_memory_bytes = 0"))
        # Publication isolation is tested without competing maintenance. Keep
        # file/deletion budgets above this bounded workload, which makes no
        # reader-debt or long-term throughput claim.
        self.set_compaction_policy({"oldest_l0_soft_ms": 3600000, "oldest_l0_hard_ms": 7200000,
                                    "l0_soft_files": 1024, "l0_hard_files": 2048,
                                    "stable_small_soft_files": 1024, "stable_small_hard_files": 2048,
                                    "delete_files_soft": 1024, "delete_files_hard": 2048})
        if self.args.quota:
            self.config.write_text(self.config.read_text().replace(
                "journal_bytes = 268435456", "journal_bytes = 67108864"))
        self.report["profile"] = {"table_workers": 1, "pending_transactions": 4,
                                  "collapse_memory_bytes": 0,
                                  "roles": ["ingest", "coordinator"], "quota_case": self.args.quota}

    def command(self, command):
        result = super().command(command)
        return result + ["--roles", "ingest,coordinator"] if command == "run" else result

    def status(self):
        path = self.directory / "state" / "status.json"
        if not path.exists():
            return {}
        status = json.loads(path.read_text())
        return status if self.process and status.get("process_id") == self.process.pid else {}

    def blocked(self, table="orders"):
        return next((entry for entry in self.status().get("blocked_tables", [])
                     if entry["table_id"] == self.table_ids[table]), None)

    def table_progress(self, table):
        return next((entry["materialized_lsn"] for entry in self.status().get("table_progress", [])
                     if entry["table_id"] == self.table_ids[table]), 0)

    def wait_blocked(self):
        state = self.until("orders did not enter blocked state", self.blocked)
        assert state["attempts"] >= 1
        assert state["error_code"]
        assert state["blocked_at_ms"] <= state["last_failed_at_ms"] <= state["retry_at_ms"]
        return state

    def check_ack(self, barrier):
        confirmed = self.pg.execute(
            "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = %s",
            (self.name,)).fetchone()[0]
        status = self.status()
        assert lsn(confirmed) < barrier, "source ACK passed an unpublished table transaction"
        assert status["watermarks"]["materialized_lsn"] < barrier
        return {"confirmed_flush_lsn": confirmed, "barrier": barrier, "status": status}

    def healthy(self, barrier):
        self.until("healthy table progress stopped behind orders", lambda:
                   self.table_progress("accounts") >= barrier)
        expected = self.pg.execute("SELECT * FROM accounts ORDER BY id").fetchall()
        self.until("healthy Iceberg table differs from PostgreSQL", lambda:
                   self.rows("accounts") == expected)
        self.alive()
        return {"rows": len(expected), "materialized_lsn": self.table_progress("accounts")}

    def burst(self, label, count=24):
        self.transaction(["UPDATE orders SET id=9001 WHERE id=4",
                          "UPDATE accounts SET amount=amount+0.125 WHERE id=1"])
        for index in range(count):
            statements = ["UPDATE accounts SET amount=amount+0.125 WHERE id=1"]
            if index % 2 == 0:
                statements.append(f"UPDATE orders SET payload='{label}-{index}' WHERE id=1")
            if index % 6 == 0:
                statements.append("UPDATE orders SET amount=COALESCE(amount,0)+1 WHERE id=2")
            elif index % 6 == 2:
                statements.append("DELETE FROM orders WHERE id=2")
            elif index % 6 == 4:
                statements.append(f"INSERT INTO orders (id,tenant,payload) VALUES (2,1,'{label}-reinserted')")
            barrier = self.transaction(statements)
        self.transaction(["UPDATE orders SET id=4 WHERE id=9001",
                          "UPDATE accounts SET amount=amount+0.125 WHERE id=1"])
        # Changes after more than a full admission window must still be seen.
        barrier = self.transaction(["UPDATE accounts SET id=300 WHERE id=200",
                                    "DELETE FROM accounts WHERE id=16",
                                    "INSERT INTO accounts VALUES (16,88.25)",
                                    "UPDATE accounts SET id=200 WHERE id=300"])
        self.pg.execute("BEGIN")
        self.pg.execute("UPDATE accounts SET amount=-999 WHERE id=1")
        self.pg.execute("ROLLBACK")
        return barrier

    def catch_up(self, barrier, label):
        self.proxy.allow_table("orders")
        self.wait_materialized(barrier)
        self.until("recovered table stayed blocked", lambda: not self.blocked())
        self.until("source ACK did not resume after all tables recovered", lambda: lsn(self.pg.execute(
            "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name=%s",
            (self.name,)).fetchone()[0]) >= barrier)
        return self.compare(label)

    def rejection(self, status, restart=False):
        pid = self.process.pid
        before = self.table("orders")
        before_rows = self.rows("orders", before)
        before_progress = self.table_progress("orders")
        offset = len(self.proxy.events)
        self.proxy.reject_table("orders", status, methods=("POST",))
        blocked_barrier = self.transaction([
            f"UPDATE orders SET payload='blocked-{status}' WHERE id=1",
            "UPDATE accounts SET amount=amount+1 WHERE id=1",
        ])
        blocked = self.wait_blocked()
        assert blocked["pending_operation"], "publication failure lost its unfinished operation"
        tail = self.burst(f"blocked-{status}")
        evidence = {"blocked": blocked, "healthy": self.healthy(tail), "ack": self.check_ack(blocked_barrier)}
        assert self.process.pid == pid
        assert self.rows("orders") == before_rows, "rejected orders publication changed visible rows"
        assert self.table_progress("orders") == before_progress
        if restart:
            # Deny catalog loads as well. A persisted table failure must not
            # become a startup-wide failure when reopening the same operation.
            self.proxy.reject_table("orders", status)
            self.stop(crash=True)
            tail = self.burst("while-stopped")
            self.start()
            restarted_pid = self.process.pid
            restored = self.wait_blocked()
            assert restored["blocked_at_ms"] == blocked["blocked_at_ms"]
            assert restored["attempts"] >= blocked["attempts"]
            # A retry may prove a rejected operation did not commit and prepare
            # a replacement before the crash. The unresolved operation must
            # still be present; the ambiguous-commit phase checks exact identity.
            assert restored["pending_operation"]
            evidence["after_restart"] = {"blocked": restored, "healthy": self.healthy(tail),
                                         "ack": self.check_ack(blocked_barrier)}
            assert self.process.pid == restarted_pid
            assert self.rows("orders") == before_rows
            assert self.table_progress("orders") == before_progress
            self.until("restarted table load was never retried under denial", lambda: any(
                event.get("fault") == "reject-table-before-upstream" and event["method"] == "GET"
                for event in self.proxy.events[offset:]))
        evidence["recovered"] = self.catch_up(tail, f"recovered-{status}")
        rejected = [event for event in self.proxy.events[offset:]
                    if event.get("fault") == "reject-table-before-upstream"]
        assert rejected and all(event["table"] == "orders" for event in rejected)
        if restart:
            assert any(event["method"] == "GET" for event in rejected), "restart never retried the denied table load"
        return evidence | {"rejected_requests": len(rejected)}

    def ambiguous_commit(self):
        pid = self.process.pid
        offset = len(self.proxy.events)
        self.proxy.arm_drop(table="orders", reject_after=503)
        blocked_barrier = self.transaction(["UPDATE orders SET payload='lost-response' WHERE id=2"])
        assert self.proxy.dropped.wait(timeout=self.args.timeout), "no successful orders commit response was lost"
        blocked = self.wait_blocked()
        assert blocked["pending_operation"]
        tail = self.burst("ambiguous")
        evidence = {"blocked": blocked, "healthy": self.healthy(tail), "ack": self.check_ack(blocked_barrier)}
        assert self.process.pid == pid
        dropped = [event for event in self.proxy.events[offset:]
                   if event.get("fault") == "drop-after-successful-commit"]
        assert len(dropped) == 1 and dropped[0].get("operation_id")
        self.stop(crash=True)
        self.start()
        restored = self.wait_blocked()
        assert restored["pending_operation"] == blocked["pending_operation"]
        tail = self.burst("ambiguous-restart")
        evidence["after_restart"] = {"healthy": self.healthy(tail), "ack": self.check_ack(blocked_barrier)}
        evidence["recovered"] = self.catch_up(tail, "ambiguous-recovered")
        snapshots = [snapshot for snapshot in self.table("orders")["metadata"]["snapshots"]
                     if snapshot["summary"].get("flow.operation-id") == dropped[0]["operation_id"]]
        assert len(snapshots) == 1, "ambiguous operation was lost or published twice"
        assert snapshots[0]["snapshot-id"] == dropped[0]["snapshot_id"]
        return evidence | {"confirmed_upstream_commit": dropped[0]}

    def repair_one_of_two_blocked_tables(self):
        pid = self.process.pid
        orders_before = self.rows("orders")
        self.proxy.reject_table("orders", 503, methods=("POST",))
        self.proxy.reject_table("accounts", 403, methods=("POST",))
        barrier = self.transaction(["UPDATE orders SET payload='both-blocked' WHERE id=1",
                                    "UPDATE accounts SET amount=amount+1 WHERE id=1"])
        self.until("both tables did not independently enter blocked state", lambda:
                   self.blocked("orders") and self.blocked("accounts"))
        both = self.status()
        assert all(entry["pending_operation"] for entry in both["blocked_tables"])
        self.proxy.allow_table("accounts")
        tail = self.burst("one-table-repaired")
        healthy = self.healthy(tail)
        self.until("repaired accounts remained blocked", lambda: not self.blocked("accounts"))
        assert self.blocked("orders"), "orders cleared before its fault was repaired"
        assert self.rows("orders") == orders_before
        assert self.process.pid == pid
        ack = self.check_ack(barrier)
        result = self.catch_up(tail, "both-blocked-recovered")
        assert self.process.pid == pid
        return {"both_blocked": both, "accounts_repaired": healthy, "ack": ack, "recovered": result}

    def commit_after_crash(self):
        offset = len(self.proxy.events)
        self.proxy.hold_commits(table="orders")
        try:
            barrier = self.transaction(["UPDATE orders SET payload='caller-will-die' WHERE id=3"])
            assert self.proxy.commit_held.wait(timeout=self.args.timeout), "no prepared orders commit was held"
            self.check_ack(barrier)
            old = self.process
            self.stop(crash=True)
            assert old.returncode == -signal.SIGKILL
        finally:
            self.proxy.release_commits.set()
        completed = self.until("held commit did not succeed after caller died", lambda: [
            event for event in self.proxy.events[offset:] if event.get("held_before_upstream")
            and 200 <= event.get("upstream_status", 0) < 300])
        assert len(completed) == 1 and completed[0].get("operation_id")
        self.start()
        self.wait_materialized(barrier)
        result = self.compare("commit-after-caller-crash")
        snapshots = [snapshot for snapshot in self.table("orders")["metadata"]["snapshots"]
                     if snapshot["summary"].get("flow.operation-id") == completed[0]["operation_id"]]
        assert len(snapshots) == 1, "restarted Prepared operation was duplicated"
        return result | {"upstream_commit_after_sigkill": completed[0]}

    def quota(self):
        self.proxy.reject_table("orders", 503, methods=("POST",))
        blocked_barrier = self.transaction(["UPDATE orders SET payload='quota-blocked' WHERE id=1"])
        self.wait_blocked()
        # About 80 MiB of explicit row payload against a 64 MiB journal. Each
        # transaction and row fits normal framing; host disk exhaustion is never used.
        for index in range(80):
            barrier = self.transaction([
                "INSERT INTO orders (id,tenant,payload) SELECT i,1,repeat(md5(i::text),512) "
                f"FROM generate_series({10000 + index * 64},{10063 + index * 64}) i"
            ])
        code = self.process.wait(timeout=self.args.timeout)
        assert code != 0, "journal quota must stop the shared capture runtime"
        log = (self.directory / f"daemon-{self.generation}.log").read_text()
        assert "journal quota exhausted" in log
        evidence = self.check_ack(blocked_barrier)
        self.stop()
        self.config.write_text(self.config.read_text().replace(
            "journal_bytes = 67108864", "journal_bytes = 268435456"))
        self.proxy.allow_table("orders")
        self.start()
        self.wait_materialized(barrier)
        return {"exit_code": code, "at_quota": evidence, "recovered": self.catch_up(barrier, "quota-recovered")}

    def execute(self):
        try:
            self.phase("seed", self.seed)
            self.table_ids = dict(self.pg.execute(
                "SELECT relname,oid FROM pg_class WHERE relnamespace=%s::regnamespace AND relname IN ('orders','accounts')",
                (self.name,)).fetchall())
            self.phase("initial-copy", self.initialize)
            self.start()
            self.phase("snapshot-wal-handoff", self.handoff)
            self.until("initial per-table progress was not observed", lambda: all(
                self.table_progress(table) >= self.handoff_barrier for table in ("orders", "accounts")))
            if self.args.quota:
                self.phase("shared-journal-quota-and-replay", self.quota)
            else:
                self.phase("503-single-worker-backlog-and-blocked-restart", lambda: self.rejection(503, restart=True))
                self.phase("403-table-denial-and-repair", lambda: self.rejection(403))
                self.phase("both-tables-blocked-and-one-repaired", self.repair_one_of_two_blocked_tables)
                self.phase("ambiguous-commit-and-blocked-restart", self.ambiguous_commit)
                self.phase("prepared-commit-after-caller-crash", self.commit_after_crash)
                self.phase("source-reconnect", self.reconnect)
            self.phase("native-manifest-and-operation-audit", lambda:
                       {table: self.audit(table) for table in ("orders", "accounts")})
            self.phase("source-truncate-isolates-table", self.truncate)
            self.stop()
            self.report["passed"] = True
        except BaseException as error:
            self.report.update(failure=str(error), traceback=traceback.format_exc())
            raise
        finally:
            self.stop()
            if self.proxy is not None:
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
    parser.add_argument("--quota", action="store_true", help="run the separate 64 MiB journal exhaustion case")
    args = parser.parse_args()
    if not args.postgres_url:
        parser.error("provide --postgres-url or FLOW_POSTGRES_URL")
    IsolationRun(args).execute()
    print(f"PASS: {args.artifacts.resolve() / 'report.json'}", flush=True)


if __name__ == "__main__":
    main()
