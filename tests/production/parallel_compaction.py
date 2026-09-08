#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["duckdb==1.5.5", "psycopg[binary]==3.3.5", "boto3==1.43.88", "fastavro==1.12.2", "pytz==2026.3.post1"]
# ///
"""Hold two real table builds while CDC, cancellation and restart remain correct."""

import argparse
import os
from pathlib import Path
import signal
import time
import traceback
from urllib.parse import unquote, urlsplit
from uuid import UUID

from concurrent_compaction import ConcurrentRun, dump


class ParallelRun(ConcurrentRun):
    def __init__(self, args):
        super().__init__(args)
        # Keep late mutations below hard debt so admission must be speculative.
        self.policy = {
            "l0_soft_files": 2, "l0_hard_files": 128,
            "stable_small_soft_files": 128, "stable_small_hard_files": 128,
            "delete_files_soft": 128, "delete_files_hard": 128,
            "oldest_l0_soft_ms": 1000, "oldest_l0_hard_ms": 240000,
            "min_file_age_ms": 0,
        }
        policy = "\n[compaction]\n" + "".join(f"{key} = {value}\n" for key, value in self.policy.items())
        self.config.write_text(self.config.read_text().replace("\n[[tables]]", policy + "\n[[tables]]", 1))
        self.report.update(compaction_policy=self.policy, table_workers=4, required_parallel_builds=2,
                           planned_phases=6)
        self.paired_histories = {}

    def seed(self):
        result = super().seed()
        self.pg.execute("INSERT INTO accounts SELECT i, i * 1.125 FROM generate_series(10000,18191) i")
        result["accounts"] += 8192
        self.table_ids = {name: self.pg.execute(f"SELECT '{name}'::regclass::oid").fetchone()[0]
                          for name in ("orders", "accounts")}
        return result

    def initialize(self):
        result = super().initialize()
        self.initial_histories = {}
        for name in self.table_ids:
            metadata = self.table(name)
            self.initial_histories[name] = metadata, self.rows(name, metadata)
        return result

    @staticmethod
    def object_path(path):
        uri = urlsplit(path)
        return "/" + uri.netloc + uri.path

    def held_requests(self, state="held"):
        with self.object_proxy.lock:
            return [event.copy() for event in self.object_proxy.events[self.hold_event_offset:]
                    if event["state"] == state and (state == "held" or event.get("held"))]

    def build_events(self, event):
        return {item["operation_id"]: item for item in self.log_fields()
                if item.get("event") == event}

    def assert_pair_held(self, pair):
        assert not self.object_proxy.release_reads.is_set()
        held = {unquote(urlsplit(event["path"]).path) for event in self.held_requests()}
        completed = {unquote(urlsplit(event["path"]).path) for event in self.held_requests("complete")}
        for build in pair.values():
            assert self.object_path(build["held_file"]) in held
            assert self.object_path(build["held_file"]) not in completed, "a held worker request completed"
        started = self.build_events("compaction_build_started")
        assert set(started) == {build["operation_id"] for build in pair.values()}, \
            "a table admitted a duplicate build while its first worker was still held"
        assert all(path.exists() for path in self.pair_scratch), "scratch disappeared before worker join"
        self.alive()

    def held_pair(self, label, first):
        self.until("preceding maintenance did not drain both tables", self.no_l0)
        self.stop()
        self.native = False
        self.start()
        barrier = self.transaction([
            f"UPDATE orders SET payload='{label}-input',amount=amount+1 WHERE id BETWEEN {first} AND {first+511}",
            f"UPDATE accounts SET amount=amount+1 WHERE id BETWEEN {10000+first} AND {10511+first}",
        ])
        self.wait_materialized(barrier)
        self.until("input transaction did not ACK", lambda: self.confirmed() >= barrier)
        self.stop()
        inputs = {}
        histories = {}
        for name in self.table_ids:
            metadata = self.table(name)
            snapshot, files = self.files(metadata)
            paths = [path for path, file in files.items() if file["content"] == 0 and "flow-l0-" in path]
            assert paths, f"{name}: no eligible L0 input"
            inputs[name] = {}
            for path in paths:
                keys = [row[0] for row in self.duck.execute("SELECT id FROM read_parquet(?) ORDER BY id LIMIT 8", [path]).fetchall()]
                assert len(keys) == 8, f"{name}: input cannot exercise eight translated positions"
                inputs[name][path] = keys
            histories[name] = metadata, self.rows(name, metadata)
            dump(self.directory / f"{label}-{name}-base.json", metadata)
        self.paired_histories[label] = histories
        with self.object_proxy.lock:
            self.hold_event_offset = len(self.object_proxy.events)
        self.object_proxy.hold_reads([path for files in inputs.values() for path in files])
        self.native = True
        self.start()
        began = time.monotonic()

        def both_reads_held():
            paths = {unquote(urlsplit(event["path"]).path) for event in self.held_requests()}
            return all(any(self.object_path(path) in paths for path in files) for files in inputs.values())

        # Serial compaction cannot admit the second build while the first read is held.
        self.until("second table build was not admitted while the first was held", both_reads_held, timeout=15)
        held = {unquote(urlsplit(event["path"]).path) for event in self.held_requests()}
        started = self.build_events("compaction_build_started")
        admitted = self.build_events("compaction_input_admitted")
        pair = {}
        for name, table_id in self.table_ids.items():
            matching = [operation for operation, event in admitted.items()
                        if event["table_id"] == table_id and operation in started]
            assert len(matching) == 1, f"{name}: expected one captured build, got {matching}"
            operation = matching[0]
            metadata, _ = histories[name]
            snapshot, _ = self.files(metadata)
            assert started[operation]["base_snapshot_id"] == snapshot["snapshot-id"]
            path = next(path for path in inputs[name] if self.object_path(path) in held)
            pair[name] = {"label": label, "operation_id": operation, "table_id": table_id,
                          "table_uuid": metadata["metadata"]["table-uuid"],
                          "base_snapshot": snapshot["snapshot-id"], "base_sequence": snapshot["sequence-number"],
                          "held_file": path, "keys": inputs[name][path]}
        assert len({build["operation_id"] for build in pair.values()}) == 2
        assert len({build["table_uuid"] for build in pair.values()}) == 2
        self.pair_scratch = sorted(path for path in (self.directory / "state/compaction").iterdir() if path.is_dir())
        assert len(self.pair_scratch) == 2, "both workers must own separate scratch directories"
        assert all(str(UUID(path.name)) == path.name for path in self.pair_scratch)
        self.assert_pair_held(pair)
        self.report.setdefault("held_pairs", []).append({"label": label, "builds": pair,
            "scratch": [str(path.relative_to(self.directory)) for path in self.pair_scratch],
            "both_held_after_seconds": round(time.monotonic()-began, 3)})
        return pair

    def late_pair_mutations(self, pair):
        first, second = [], []
        for name, build in pair.items():
            a, b, c, d, e, f, g, h = build["keys"]
            first.extend([
                f"UPDATE {name} SET amount=amount+2 WHERE id IN ({a},{b})",
                f"DELETE FROM {name} WHERE id IN ({c},{d},{g})",
                f"UPDATE {name} SET id=id+500000 WHERE id IN ({e},{f})",
            ])
            if name == "orders":
                second.append(f"INSERT INTO orders (id,tenant,payload,amount) VALUES ({g},9,'{build['label']}-reborn',42.5)")
            else:
                second.append(f"INSERT INTO accounts (id,amount) VALUES ({g},42.5)")
            second.append(f"UPDATE {name} SET amount=amount+3 WHERE id={h}")
        self.transaction(first)
        return self.transaction(second)

    def cdc_while_held(self, pair):
        barrier = self.late_pair_mutations(pair)
        self.wait_materialized(barrier)
        self.until("source ACK stalled behind two held builds", lambda: self.confirmed() >= barrier)
        self.assert_pair_held(pair)
        for name, build in pair.items():
            assert self.table(name)["metadata"]["current-snapshot-id"] != build["base_snapshot"]
        return barrier, self.compare("two-builds-held-cdc")

    def check_histories(self, label):
        for histories in (self.initial_histories, self.paired_histories[label]):
            for name, (metadata, rows) in histories.items():
                assert self.rows(name, metadata) == rows, f"{name}: retained history changed"

    def parallel_catchup(self):
        pair = self.held_pair("catchup", 1)
        barrier, before = self.cdc_while_held(pair)
        scratch = list(self.pair_scratch)
        self.object_proxy.release_reads.set()
        translated = {}
        for name, build in pair.items():
            def committed():
                metadata = self.table(name)
                snapshots = [snapshot for snapshot in metadata["metadata"]["snapshots"]
                             if snapshot["summary"].get("streaming.build-snapshot-id") == str(build["base_snapshot"])]
                return (metadata, snapshots[0]) if snapshots else None
            metadata, compact = self.until(f"{name}: held build did not activate", committed)
            translated[name] = self.translated_deletes(metadata, compact, build)
        self.until("activated workers retained scratch", lambda: all(not path.exists() for path in scratch))
        after = self.compare("two-builds-activated")
        self.check_histories("catchup")
        return {"builds": pair, "source_barrier": barrier, "confirmed_lsn": self.confirmed(),
                "rows_while_held": before, "rows_after_activation": after, "translated_deletes": translated}

    def parallel_deadline(self):
        pair = self.held_pair("deadline", 1025)
        barrier, before = self.cdc_while_held(pair)
        operations = {build["operation_id"] for build in pair.values()}
        self.until("both held workers did not request deadline cancellation", lambda:
                   operations <= self.build_events("compaction_build_cancellation_requested").keys(), timeout=40)
        requested = self.build_events("compaction_build_cancellation_requested")
        for operation in operations:
            assert requested[operation]["durable_ownership_retained"] is True
            assert int(requested[operation]["elapsed_ms"]) >= 30000
        self.assert_pair_held(pair)
        assert not operations & self.build_events("compaction_build_retired").keys()
        scratch = list(self.pair_scratch)
        self.object_proxy.release_reads.set()
        self.until("both canceled workers did not join and retire their BUILD", lambda:
                   operations <= self.build_events("compaction_build_retired").keys(), timeout=20)
        self.until("joined workers retained scratch", lambda: all(not path.exists() for path in scratch))
        self.wait_materialized(barrier)
        self.until("deadline recovery did not ACK", lambda: self.confirmed() >= barrier)
        self.until("deadline recovery did not drain both tables", self.no_l0)
        for name, build in pair.items():
            assert all(snapshot["summary"].get("flow.operation-id") != build["operation_id"]
                       for snapshot in self.table(name)["metadata"]["snapshots"]), "canceled BUILD was published"
        self.check_histories("deadline")
        return {"builds": pair, "source_barrier": barrier, "rows_while_held": before,
                "cancellation": {operation: requested[operation] for operation in operations},
                "retired": {operation: self.build_events("compaction_build_retired")[operation] for operation in operations},
                "rows_after_join": self.compare("two-build-deadline-recovery")}

    def parallel_restart(self):
        pair = self.held_pair("restart", 2049)
        barrier, before = self.cdc_while_held(pair)
        scratch = list(self.pair_scratch)
        began = time.monotonic()
        self.process.send_signal(signal.SIGTERM)
        code = self.process.wait(timeout=5)
        assert code == 0, f"SIGTERM exit: {code}"
        self.process = None
        self.log.close()
        assert not self.object_proxy.release_reads.is_set()
        assert all(path.exists() for path in scratch), "shutdown discarded unjoined worker scratch"
        self.object_proxy.release_reads.set()
        self.start()
        self.until("restart did not discard both abandoned BUILD records", lambda:
                   any(event.get("message") == "discarded compaction builds from the previous process"
                       and event.get("abandoned") == 2 for event in self.log_fields()))
        self.until("restart retained either abandoned UUID scratch", lambda: all(not path.exists() for path in scratch))
        self.wait_materialized(barrier)
        self.until("restart did not ACK", lambda: self.confirmed() >= barrier)
        self.until("restart did not drain both tables", self.no_l0)
        self.check_histories("restart")
        return {"builds": pair, "source_barrier": barrier, "rows_before_shutdown": before,
                "exit_code": code, "shutdown_and_recovery_seconds": round(time.monotonic()-began, 3),
                "discarded_builds": 2, "reclaimed_scratch": [str(path.relative_to(self.directory)) for path in scratch],
                "rows_after_restart": self.compare("two-abandoned-builds-recovered")}

    def execute(self):
        try:
            self.phase("seed", self.seed)
            self.phase("initial-copy", self.initialize)
            self.native = True
            self.start()
            self.phase("snapshot-wal-handoff", self.handoff)
            self.phase("parallel-builds-cdc-and-exact-activation", self.parallel_catchup)
            self.phase("parallel-deadlines-retain-ownership-until-join", self.parallel_deadline)
            self.phase("parallel-sigterm-and-abandoned-build-recovery", self.parallel_restart)
            self.stop()
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
            self.report["completed_phases"] = len(self.report["phases"])
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
    ParallelRun(parser.parse_args()).execute()


if __name__ == "__main__":
    main()
