#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["duckdb==1.5.5", "psycopg[binary]==3.3.5", "boto3==1.43.88", "fastavro==1.12.2"]
# ///
"""Upgrade a populated format-2 index to format 3 without replacing source/control authority."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import sys
import time
import traceback

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "local"))
from run import Run, dump, lsn


class UpgradeRun(Run):
    def __init__(self, args):
        self.next_binary = args.binary.resolve()
        with self.next_binary.open("rb") as executable:
            self.next_digest = hashlib.file_digest(executable, "sha256").hexdigest()
        args.binary = args.previous_binary.resolve()
        super().__init__(args)
        assert self.next_digest != self.binary_digest, "upgrade requires two different immutable binaries"
        self.report["previous_binary"] = self.report["binary"].copy()
        self.report["upgrade_binary"] = {"path": str(self.next_binary), "sha256": self.next_digest}
        self.report["index_formats"] = {"previous": 2, "upgraded": 3}
        self.environment["RUST_LOG"] = "info"

    def events(self):
        events = []
        for line in (self.directory / f"daemon-{self.generation}.log").read_text().splitlines():
            try:
                events.append(json.loads(line).get("fields", {}))
            except json.JSONDecodeError:
                pass
        return events

    def start_ready(self):
        started_ns = time.time_ns()
        self.start()

        def fresh_ready():
            status_path = self.directory / "state" / "status.json"
            metrics_path = self.directory / "state" / "metrics.prom"
            if not status_path.exists() or not metrics_path.exists():
                return None
            status = json.loads(status_path.read_text())
            if (status.get("process_id") == self.process.pid and status.get("ready")
                    and status["updated_at_ms"] >= started_ns // 1_000_000
                    and metrics_path.stat().st_mtime_ns >= started_ns):
                assert status["source_id"] == self.name
                return status
            return None

        return self.until("new daemon PID did not publish fresh ready status and metrics", fresh_ready)

    def slot(self):
        row = self.pg.execute(
            "SELECT slot_name, plugin, database, confirmed_flush_lsn::text, active_pid "
            "FROM pg_replication_slots WHERE slot_name = %s", (self.name,)).fetchone()
        assert row is not None, "original logical replication slot disappeared"
        return dict(zip(("slot", "plugin", "database", "confirmed_flush_lsn", "active_pid"), row))

    def upgrade(self):
        state = self.directory / "state"
        previous_pid = self.process.pid
        old_process = self.process
        self.stop()
        assert old_process.returncode == 0, "previous binary did not stop gracefully"
        self.compare("before-upgrade")
        self.previous_metadata = {name: self.table(name) for name in ("orders", "accounts")}
        self.previous_rows = {name: self.rows(name, metadata) for name, metadata in self.previous_metadata.items()}
        self.source_identity = (state / "source-identity.json").read_bytes()
        self.control_identity = (state / "control" / "IDENTITY").read_bytes()
        self.old_index = state / "index"
        assert (self.old_index / "CURRENT").is_file(), "previous binary did not retain its populated index"
        assert not list((state / "index-generations").glob("*/CURRENT")), "old run unexpectedly rebuilt its fresh index"
        before = self.metrics()
        slot = self.slot()
        assert slot["active_pid"] is None
        assert lsn(slot["confirmed_flush_lsn"]) <= before["flow_materialized_lsn"]
        # Changes made while capture is stopped prove slot continuity. Creating
        # a new slot or silently starting at the current WAL would lose them.
        barrier = self.transaction([
            "UPDATE orders SET payload='queued-during-upgrade', amount=0.125 WHERE id BETWEEN 1 AND 8",
            "DELETE FROM orders WHERE id BETWEEN 86 AND 90",
            "UPDATE orders SET id=id+200000 WHERE id BETWEEN 100076 AND 100078",
            "INSERT INTO orders (id, tenant, payload) VALUES (8000, 3, 'upgrade-backlog')",
            "UPDATE accounts SET amount=amount+0.75 WHERE id <= 3",
            "DELETE FROM accounts WHERE id=16",
            "INSERT INTO accounts VALUES (300, -12.3456)",
        ])
        assert barrier > before["flow_materialized_lsn"]
        assert self.slot()["confirmed_flush_lsn"] == slot["confirmed_flush_lsn"]
        self.args.binary = self.next_binary
        self.binary_digest = self.next_digest
        self.report["binary"] = self.report["upgrade_binary"].copy()
        dump(self.directory / "report.json", self.report)
        ready = self.start_ready()
        assert self.process.pid != previous_pid
        events = self.events()
        rejection = next((event for event in events if "per-file live-row counts changed" in event.get("error", "")), None)
        assert rejection is not None, "old index was not rejected for the format-3 live-row-count migration"
        activated = [event for event in events if event.get("event") == "index_generation_activated"]
        assert len(activated) == 1, "upgrade must activate exactly one rebuilt generation"
        activated = activated[0]
        assert not activated["restored_checkpoint"], "old-format checkpoint bypassed index reconstruction"
        self.new_index = Path(activated["path"]).resolve()
        assert self.new_index.parent == (state / "index-generations").resolve()
        assert self.new_index != self.old_index.resolve() and (self.new_index / "CURRENT").is_file()
        assert (self.old_index / "CURRENT").is_file(), "upgrade removed old index instead of rebuilding alongside it"
        assert not any(event.get("event", "").startswith("bootstrap_table_") for event in events)
        assert (state / "source-identity.json").read_bytes() == self.source_identity
        assert (state / "control" / "IDENTITY").read_bytes() == self.control_identity
        self.wait_materialized(barrier)
        result = self.compare("queued-wal-after-format-upgrade")
        assert self.metrics()["flow_materialized_lsn"] >= before["flow_materialized_lsn"]
        after_slot = self.slot()
        assert all(after_slot[key] == slot[key] for key in ("slot", "plugin", "database"))
        self.until("slot acknowledgement did not resume after upgrade", lambda:
                   lsn(self.slot()["confirmed_flush_lsn"]) >= barrier)
        return result | {"previous_pid": previous_pid, "upgraded_pid": self.process.pid,
                         "ready": ready, "previous_index": str(self.old_index), "activated": activated,
                         "format_rejection": rejection, "control_identity_preserved": True,
                         "source_identity_preserved": True, "slot": self.slot(), "backlog_barrier": barrier}

    def after_upgrade(self):
        barrier = self.transaction([
            "UPDATE orders SET payload=repeat('upgraded-世界-',1024), data=decode('00ff00', 'hex') WHERE id BETWEEN 1 AND 4",
            "DELETE FROM orders WHERE id=5000",
            "INSERT INTO orders (id, tenant, payload, amount) VALUES (5000, 4, 'replacement-same-key', -99.125)",
            "UPDATE orders SET id=9000, payload='moved-after-upgrade' WHERE id=5001",
            "UPDATE orders SET id=id+100000 WHERE id BETWEEN 300076 AND 300078",
            "DELETE FROM orders WHERE id=8000",
            "UPDATE accounts SET amount=amount-0.5 WHERE id <= 3",
            "UPDATE accounts SET id=301 WHERE id=300",
        ])
        self.wait_materialized(barrier)
        result = self.compare("crud-and-key-moves-after-upgrade")
        for name, metadata in self.previous_metadata.items():
            assert self.rows(name, metadata) == self.previous_rows[name], f"{name}: pre-upgrade snapshot changed"
        assert self.rows("orders", self.initial_metadata) == self.initial_rows, "initial snapshot changed"
        result["retained_history"] = {name: len(rows) for name, rows in self.previous_rows.items()}
        return result

    def restart_current_format(self):
        generations = set((self.directory / "state" / "index-generations").glob("*/CURRENT"))
        self.stop()
        self.start_ready()
        assert not any(event.get("event") == "index_generation_activated" for event in self.events()), \
            "current-format restart rebuilt the index again"
        assert set((self.directory / "state" / "index-generations").glob("*/CURRENT")) == generations
        barrier = self.transaction(["UPDATE orders SET amount=7.125 WHERE id=400076",
                                    "UPDATE accounts SET amount=3.75 WHERE id=301"])
        self.wait_materialized(barrier)
        state = self.directory / "state"
        assert (state / "source-identity.json").read_bytes() == self.source_identity
        assert (state / "control" / "IDENTITY").read_bytes() == self.control_identity
        return self.compare("current-format-restart") | {"index": str(self.new_index)}

    def execute(self):
        try:
            self.phase("seed", self.seed)
            self.phase("previous-binary-initial-copy", self.initialize)
            self.start_ready()
            self.phase("previous-binary-snapshot-wal-handoff", self.handoff)
            self.phase("previous-binary-crud-and-key-moves", self.mutations)
            self.phase("format-2-to-3-upgrade-and-queued-wal", self.upgrade)
            self.phase("new-binary-crud-and-retained-history", self.after_upgrade)
            self.phase("current-format-restart-reuses-index", self.restart_current_format)
            self.report["passed"] = True
        except BaseException as error:
            self.report.update(error=str(error), traceback=traceback.format_exc())
            raise
        finally:
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
    parser.add_argument("--previous-binary", type=Path, required=True)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--artifacts", type=Path, required=True)
    parser.add_argument("--timeout", type=float, default=180)
    args = parser.parse_args()
    if not args.postgres_url:
        parser.error("provide --postgres-url or FLOW_POSTGRES_URL")
    UpgradeRun(args).execute()
    print(f"PASS: {args.artifacts.resolve() / 'report.json'}", flush=True)


if __name__ == "__main__":
    main()
