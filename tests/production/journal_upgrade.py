#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["duckdb==1.5.5", "psycopg[binary]==3.3.5", "boto3==1.43.88", "fastavro==1.12.2"]
# ///
"""Recover an old Prepared epoch and uncounted backlog using counted descriptors."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import signal
import shutil
import subprocess
import traceback

from proxy import CatalogProxy
from upgrade import UpgradeRun, dump, lsn
from legacy_evidence import legacy_journal_evidence


TABLES = ("orders", "accounts")


class JournalUpgradeRun(UpgradeRun):
    def __init__(self, args):
        self.release_on_start = False
        super().__init__(args)
        self.report.pop("index_formats")
        self.report["expected_formats"] = {"journal_terminal": [4, 5], "ledger": ["FLLEDG02", "FLLEDG03"],
                                           "index": "unchanged format 3"}
        self.report["profile"] = {"roles": ["ingest", "coordinator"], "priority": "balanced",
                                  "oldest_l0_soft_ms": 3600000, "oldest_l0_hard_ms": 7200000}
        self.proxy = CatalogProxy(args.catalog_uri, self.directory / "catalog-proxy.jsonl")
        args.catalog_uri = self.proxy.url
        self.configure()
        self.history = {}

    def configure(self):
        super().configure()
        self.environment["RUST_LOG"] = "info"
        # Isolate format recovery from age-triggered maintenance while compaction
        # is disabled. This functional gate makes no latency or reader-debt claim.
        self.config.write_text(self.config.read_text().replace(
            "\n[[tables]]", "\n[compaction]\noldest_l0_soft_ms = 3600000\noldest_l0_hard_ms = 7200000\n\n[[tables]]", 1
        ).replace("primary_key = [0]", 'primary_key = [0]\npriority = "balanced"'))

    def command(self, command):
        result = super().command(command)
        return result + ["--roles", "ingest,coordinator"] if command == "run" else result

    def start(self):
        super().start()
        if self.release_on_start:
            # The new process exists before the outstanding old HTTP request is
            # released; its Prepared recovery must resolve either catalog outcome.
            self.release_on_start = False
            self.proxy.release_commits.set()

    def identity(self):
        state = self.directory / "state"
        assert (state / "index" / "CURRENT").is_file()
        return {"source": json.loads((state / "source-identity.json").read_text()),
                "control": (state / "control" / "IDENTITY").read_text(),
                "index": (state / "index" / "IDENTITY").read_text(),
                "generations": sorted(str(path.relative_to(state))
                                      for path in (state / "index-generations").glob("*/CURRENT")),
                "config_sha256": hashlib.sha256(self.config.read_bytes()).hexdigest()}

    def unchanged_identity(self):
        assert self.identity() == self.original_identity, "upgrade replaced source, control, config or index"
        assert not any(event.get("event") == "index_generation_activated"
                       or event.get("event", "").startswith("bootstrap_table_") for event in self.events()), \
            "upgrade rebuilt the index or copied the source again"
        assert all(self.slot()[key] == self.original_slot[key] for key in ("slot", "plugin", "database"))

    def remember_history(self, label):
        snapshots = {}
        for table in TABLES:
            metadata = self.table(table)
            rows = self.rows(table, metadata)
            self.history[(label, table)] = (metadata, rows)
            snapshots[table] = {"metadata_location": metadata["metadata-location"], "rows": len(rows),
                                "snapshot": metadata["metadata"]["current-snapshot-id"]}
            dump(self.directory / f"{label}-{table}-metadata.json", metadata)
        return snapshots

    def verify_history(self):
        for (label, table), (metadata, rows) in self.history.items():
            assert self.rows(table, metadata) == rows, f"{label}: retained {table} snapshot changed"
        assert self.rows("orders", self.initial_metadata) == self.initial_rows
        return {"retained_table_snapshots": len(self.history), "initial_orders_rows": len(self.initial_rows)}

    def burst(self, count, label):
        transactions = []
        for index in range(count):
            with self.pg.transaction():
                xid = int(self.pg.execute("SELECT pg_current_xact_id()::text").fetchone()[0])
                assert self.pg.execute("UPDATE orders SET amount=amount+0.125, payload=%s WHERE id=%s",
                                       (f"{label}-{index}", index + 1)).rowcount == 1
                assert self.pg.execute("UPDATE accounts SET amount=amount+0.25 WHERE id=%s",
                                       (index + 1,)).rowcount == 1
                barrier = lsn(self.pg.execute("SELECT pg_current_wal_insert_lsn()::text").fetchone()[0])
            transactions.append({"xid": xid % (1 << 32), "precommit_barrier": barrier})
        return transactions

    def upgrade_backlog(self):
        self.original_identity = self.identity()
        self.original_slot = self.slot()
        before_history = self.remember_history("before-upgrade")
        self.proxy.hold_commits()
        first = self.burst(6, "old-prepared")
        self.until("old catalog POST was not held", self.proxy.commit_held.is_set, timeout=20)
        collapsed = {event["operation_id"]: event for event in self.events()
                     if event.get("event") == "epoch_collapsed" and event["transactions"] > 1}
        prepared = [event for event in self.events() if event.get("event") == "ingest_prepared"
                    and event["operation_id"] in collapsed]
        assert prepared, "old Prepared operation did not contain multiple transactions"
        tail = self.burst(4, "old-backlog")
        transactions = first + tail
        self.until("old journal and source ledger did not retain the complete held backlog", lambda:
                   self.metrics().get("flow_journal_durable_lsn", 0) >= tail[-1]["precommit_barrier"]
                   and self.metrics().get("flow_pending_transactions", 0) >= len(transactions), timeout=15)
        journaled = {event["xid"]: event for event in self.events()
                     if event.get("event") == "transaction_journaled"}
        for transaction in transactions:
            event = journaled[transaction["xid"]]
            transaction["end_lsn"] = event["end_lsn"]
            assert lsn(event["end_lsn"]) > transaction["precommit_barrier"]
        barrier = lsn(transactions[-1]["end_lsn"])
        held_metrics, held_slot = self.metrics(), self.slot()
        assert held_metrics["flow_journal_durable_lsn"] >= barrier
        assert held_metrics["flow_materialized_lsn"] < first[0]["precommit_barrier"]
        assert lsn(held_slot["confirmed_flush_lsn"]) < first[0]["precommit_barrier"]
        evidence = {"original_identity": self.original_identity, "original_slot": self.original_slot,
                    "before_history": before_history, "transactions": transactions,
                    "prepared": prepared, "collapsed": list(collapsed.values()),
                    "held_metrics": held_metrics, "held_slot": held_slot, "durable_barrier": barrier,
                    "old_pid": self.process.pid}
        self.report["upgrade_checkpoint"] = evidence
        dump(self.directory / "report.json", self.report)
        old_process = self.process
        self.stop(crash=True)
        assert old_process.returncode == -signal.SIGKILL
        assert not self.proxy.release_commits.is_set(), "held request escaped before the crash"
        expected = {item["xid"]: lsn(item["end_lsn"]) for item in transactions}
        journal = legacy_journal_evidence(self.directory / "state" / "journal", self.name, expected)
        # Opening RocksDB may recover its WAL. Inspect an offline copy so format
        # admission cannot alter the actual state handed to the new binary.
        copied_control = self.directory / "legacy-control-evidence"
        shutil.copytree(self.directory / "state" / "control", copied_control)
        inspection = subprocess.run([str(self.args.ledger_inspector.resolve()), str(copied_control), self.name,
                                     *map(str, expected.values())], capture_output=True, text=True, timeout=30)
        (self.directory / "legacy-ledger-inspector.log").write_text(inspection.stdout + inspection.stderr)
        inspection.check_returncode()
        ledger = json.loads(inspection.stdout)
        assert {(item["xid"], item["end_lsn"]) for item in ledger} == set(expected.items()), "ledger backlog identities differ"
        assert all(item["format"] == "FLLEDG02" for item in ledger), \
            "legacy-format precondition: expected FLLEDG02 backlog ledger entries"
        evidence["observed_legacy_formats"] = {"journal": journal, "ledger": ledger}
        self.report["formats"] = evidence["observed_legacy_formats"]
        dump(self.directory / "report.json", self.report)
        self.args.binary, self.binary_digest = self.next_binary, self.next_digest
        self.report["binary"] = self.report["upgrade_binary"].copy()
        self.release_on_start = True
        ready = self.start_ready()
        self.wait_materialized(barrier)
        self.until("slot ACK did not reach the recovered prefix", lambda:
                   lsn(self.slot()["confirmed_flush_lsn"]) >= barrier)
        self.unchanged_identity()
        result = self.compare("old-prepared-and-backlog-recovered")
        operation_ids = {event["operation_id"] for event in prepared}
        recovered = []
        for table in TABLES:
            snapshots = self.table(table)["metadata"]["snapshots"]
            operations = [snapshot["summary"].get("flow.operation-id") for snapshot in snapshots]
            assert len(operations) == len(set(operations)), "logical operation committed more than once"
            recovered.extend(snapshot for snapshot in snapshots
                             if snapshot["summary"].get("flow.operation-id") in operation_ids)
        assert {snapshot["summary"]["flow.operation-id"] for snapshot in recovered} == operation_ids, \
            "old Prepared epoch was replaced instead of recovered"
        assert all(int(snapshot["summary"]["streaming.transaction-count"]) > 1 for snapshot in recovered)
        return result | evidence | {"new_pid": self.process.pid, "ready": ready,
                                    "old_exit_code": old_process.returncode, "recovered_prepared": recovered,
                                    "history": self.verify_history()}

    def counted_noop(self):
        before = self.remember_history("after-upgrade")
        barrier = self.transaction([
            "INSERT INTO orders (id, tenant, payload) VALUES (6000, 1, 'counted-then-deleted')",
            "UPDATE orders SET payload='updated-before-delete' WHERE id=6000",
            "DELETE FROM orders WHERE id=6000",
            "INSERT INTO accounts VALUES (6000, 1.25)",
            "DELETE FROM accounts WHERE id=6000",
        ])
        self.wait_materialized(barrier)
        self.until("no-op transaction did not release the ACK prefix", lambda:
                   lsn(self.slot()["confirmed_flush_lsn"]) >= barrier)
        result = self.compare("counted-noop")
        assert all(result[table]["snapshot"] == before[table]["snapshot"] for table in TABLES), \
            "fully collapsed transaction created an empty snapshot"
        return result | {"source_mutations": {"orders": 3, "accounts": 2}, "barrier": barrier}

    def counted_stream(self):
        before = {table: {snapshot["snapshot-id"] for snapshot in self.table(table)["metadata"]["snapshots"]}
                  for table in TABLES}
        result = self.stream()
        publications = {}
        for table in TABLES:
            added = [snapshot for snapshot in self.table(table)["metadata"]["snapshots"]
                     if snapshot["snapshot-id"] not in before[table]]
            assert len(added) == 1, f"{table}: one oversized source transaction was split across snapshots"
            assert added[0]["summary"]["streaming.transaction-count"] == "1"
            publications[table] = added[0]
        return result | {"publications": publications, "history": self.verify_history()}

    def restart_queued(self):
        retained = self.remember_history("before-counted-restart")
        process = self.process
        self.stop()
        assert process.returncode == 0
        slot = self.slot()
        assert slot["active_pid"] is None
        barrier = self.transaction([
            "UPDATE orders SET id=9000, payload='moved-across-counted-restart' WHERE id=5001",
            "DELETE FROM orders WHERE id=5000",
            "INSERT INTO orders (id, tenant, payload) VALUES (5000, 2, 'reinserted-across-counted-restart')",
            "UPDATE accounts SET id=300 WHERE id=100",
            "UPDATE accounts SET amount=amount-0.75 WHERE id<=4",
        ])
        assert self.slot()["confirmed_flush_lsn"] == slot["confirmed_flush_lsn"]
        self.start_ready()
        self.wait_materialized(barrier)
        self.until("ACK did not resume after counted restart", lambda:
                   lsn(self.slot()["confirmed_flush_lsn"]) >= barrier)
        self.unchanged_identity()
        return self.compare("counted-restart-queued-wal") | {"barrier": barrier, "slot": self.slot(),
                                                           "retained": retained, "history": self.verify_history()}

    def stop_checked(self):
        process = self.process
        self.stop()
        assert process.returncode == 0
        self.check_worker_panics()
        assert self.slot()["active_pid"] is None
        return {"pid": process.pid, "exit_code": process.returncode, "worker_panics": []}

    def execute(self):
        try:
            self.phase("seed", self.seed)
            self.phase("old-binary-initial-copy", self.initialize)
            self.start_ready()
            self.phase("old-binary-handoff", self.handoff)
            self.phase("old-binary-crud", self.mutations)
            self.phase("old-prepared-backlog-upgrade", self.upgrade_backlog)
            self.phase("counted-noop-without-empty-snapshot", self.counted_noop)
            self.phase("counted-large-whole-transaction", self.counted_stream)
            self.phase("counted-restart-and-queued-wal", self.restart_queued)
            self.phase("stopped-without-worker-panics", self.stop_checked)
            self.report["passed"] = True
        except BaseException as error:
            self.report.update(error=str(error), traceback=traceback.format_exc())
            raise
        finally:
            self.proxy.release_commits.set()
            self.stop()
            self.proxy.close()
            try:
                self.check_worker_panics()
            finally:
                dump(self.directory / "report.json", self.report)
                self.pg.close()
                self.duck.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--postgres-url", default=os.environ.get("FLOW_POSTGRES_URL"))
    parser.add_argument("--catalog-uri", required=True)
    parser.add_argument("--s3-endpoint", required=True)
    parser.add_argument("--warehouse", default="s3://warehouse/")
    parser.add_argument("--previous-binary", type=Path, required=True)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--ledger-inspector", type=Path, default=Path("target/debug/examples/inspect_legacy_ledger"),
                        help="offline testkit example built with cargo build -p flow-testkit --example inspect_legacy_ledger")
    parser.add_argument("--artifacts", type=Path, required=True)
    parser.add_argument("--timeout", type=float, default=180)
    args = parser.parse_args()
    if not args.postgres_url:
        parser.error("provide --postgres-url or FLOW_POSTGRES_URL")
    JournalUpgradeRun(args).execute()
    print(f"PASS: {args.artifacts.resolve() / 'report.json'}", flush=True)


if __name__ == "__main__":
    main()
