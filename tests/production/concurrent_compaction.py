#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["duckdb==1.5.5", "psycopg[binary]==3.3.5", "boto3==1.43.88", "fastavro==1.12.2", "pytz==2026.3.post1"]
# ///
"""Hold actual compactor Parquet reads while CDC, hard debt and restart proceed."""

import argparse
import json
import os
from pathlib import Path
import signal
import sys
import time
import traceback
from urllib.parse import unquote, urlsplit

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "local"))
from run import Run, dump, lsn
from proxy import CatalogProxy
from s3_proxy import S3ReadProxy


class ConcurrentRun(Run):
    def __init__(self, args):
        super().__init__(args)
        self.native = False
        self.histories = {}
        self.catalog_proxy = CatalogProxy(args.catalog_uri, self.directory / "catalog-proxy.jsonl")
        self.object_proxy = S3ReadProxy(args.s3_endpoint, self.directory / "s3-proxy.jsonl")
        self.environment["RUST_LOG"] = "info"
        text = self.config.read_text().replace(json.dumps(args.catalog_uri), json.dumps(self.catalog_proxy.url))
        text = text.replace("[limits]\n", "[limits]\ntable_workers = 4\n")
        self.config.write_text(text.replace(json.dumps(args.s3_endpoint), json.dumps(self.object_proxy.url)))
        # The generic smoke fixture forces aggressive delete retirement. These
        # phases test default soft/hard debt while holding an L0 rewrite; that
        # override would prioritize unrelated L2 reclamation instead.
        self.set_compaction_policy({"deleted_rows_percent": 30, "delete_files_soft": 8, "delete_files_hard": 16})
        # Keep default worker/read budgets and the default 10s/30s debt policy.
        self.report["compaction_policy"] = {"oldest_l0_soft_ms": 10000, "oldest_l0_hard_ms": 30000}
        self.report.update(table_workers=4, planned_phases=7)

    def phase(self, name, action):
        try:
            return super().phase(name, action)
        except BaseException:
            self.report["failed_phase"] = name
            raise

    def command(self, command):
        command_line = super().command(command)
        if command == "run" and not self.native:
            command_line += ["--roles", "ingest,coordinator"]
        return command_line

    def seed(self):
        result = super().seed()
        self.pg.execute("""INSERT INTO orders (id, tenant, payload, amount)
            SELECT i, i % 11, repeat(md5(i::text), 4), i * 1.0123
            FROM generate_series(10000, 41999) i""")
        result["orders"] += 32000
        self.table_id = self.pg.execute("SELECT 'orders'::regclass::oid").fetchone()[0]
        return result

    def confirmed(self):
        return lsn(self.pg.execute("SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name=%s",
                                   (self.name,)).fetchone()[0])

    def head(self):
        return self.table("orders")["metadata"]["current-snapshot-id"]

    def files(self, metadata, snapshot_id=None):
        metadata = metadata["metadata"]
        snapshot_id = snapshot_id or metadata["current-snapshot-id"]
        snapshot = next(item for item in metadata["snapshots"] if item["snapshot-id"] == snapshot_id)
        files = {}
        for manifest in self.avro(snapshot["manifest-list"]):
            for entry in self.avro(manifest["manifest_path"]):
                if entry["status"] == 2:
                    continue
                file = entry["data_file"]
                sequence = entry.get("sequence_number")
                file_sequence = entry.get("file_sequence_number")
                files[file["file_path"]] = file | {
                    "data_sequence": manifest["sequence_number"] if sequence is None else sequence,
                    "file_sequence": manifest["sequence_number"] if file_sequence is None else file_sequence,
                    "added_snapshot": entry.get("snapshot_id") or manifest["added_snapshot_id"],
                }
        return snapshot, files

    def no_l0(self):
        for name in ("orders", "accounts"):
            _, files = self.files(self.table(name))
            if any(file["content"] == 0 and "flow-l0-" in path for path, file in files.items()):
                return False
        return True

    def log_fields(self):
        for line in (self.directory / f"daemon-{self.generation}.log").read_text(errors="replace").splitlines():
            try:
                yield json.loads(line).get("fields", {})
            except ValueError:
                pass  # A concurrent log write may leave its final line incomplete.

    def held_build(self, label, first):
        if self.native:
            self.until("preceding maintenance did not drain L0", self.no_l0)
            self.stop()
            self.native = False
            self.start()
        barrier = self.transaction([
            f"UPDATE orders SET payload='{label}-input', amount=amount+1 WHERE id BETWEEN {first} AND {first + 511}",
        ])
        self.wait_materialized(barrier)
        self.until("seed ACK did not advance", lambda: self.confirmed() >= barrier)
        self.stop()
        metadata = self.table("orders")
        base, files = self.files(metadata)
        selected = None
        for path, file in files.items():
            if file["content"] == 0 and "flow-l0-" in path:
                ids = [row[0] for row in self.duck.execute(
                    "SELECT id FROM read_parquet(?) WHERE id BETWEEN ? AND ? ORDER BY id",
                    [path, first, first + 511]).fetchall()]
                if len(ids) >= 8:
                    selected = path, ids[:8]
                    break
        assert selected is not None, "fixture did not publish a selectable L0 data file"
        input_snapshot = next(snapshot for snapshot in metadata["metadata"]["snapshots"]
                              if snapshot["snapshot-id"] == files[selected[0]]["added_snapshot"])
        if label == "hard":
            # Enter the late soft-age window before starting the worker. Waiting
            # for all 30s of file age with a live build can consume its separate
            # 30s lifetime before the deliberately slow preparation finishes.
            age_ms = time.time_ns() // 1_000_000 - input_snapshot["timestamp-ms"]
            time.sleep(max(0, (20000 - age_ms) / 1000))
            assert time.time_ns() // 1_000_000 - input_snapshot["timestamp-ms"] < 30000, "fixture missed the soft-age admission window"
        self.object_proxy.hold_reads([selected[0]])
        self.native = True
        self.start()
        started = time.monotonic()
        self.until("compactor did not request the selected Parquet input", self.object_proxy.held.is_set, timeout=25)
        # A smaller preceding group may have completed before this exact input
        # was selected. Admit the actual captured head, not the pre-start head.
        metadata = self.table("orders")
        base, files = self.files(metadata)
        assert selected[0] in files
        admitted = self.until("held GET lacks snapshot-capture acknowledgement", lambda:
                   next((event for event in self.log_fields() if event.get("event") == "compaction_build_started"
                         and event.get("base_snapshot_id") == base["snapshot-id"]), None))
        assert self.head() == base["snapshot-id"], "catalog advanced before the held build's source mutations"
        self.histories[label] = metadata, self.rows("orders", metadata)
        result = {"label": label, "operation_id": admitted["operation_id"],
                  "base_snapshot": base["snapshot-id"], "base_sequence": base["sequence-number"],
                  "base_timestamp_ms": base["timestamp-ms"], "input_timestamp_ms": input_snapshot["timestamp-ms"],
                  "held_file": selected[0], "keys": selected[1],
                  "held_after_start_seconds": round(time.monotonic() - started, 3)}
        dump(self.directory / f"{label}-build-base.json", metadata)
        return result

    def late_mutations(self, build):
        a, b, c, d, e, f, g, h = build["keys"]
        label = build["label"]
        first = self.transaction([
            f"UPDATE orders SET payload='{label}-late-update', amount=amount+2 WHERE id IN ({a},{b})",
            f"DELETE FROM orders WHERE id IN ({c},{d},{g})",
            f"UPDATE orders SET id=id+500000 WHERE id IN ({e},{f})",
            "UPDATE accounts SET amount=amount+0.25 WHERE id=1",
        ])
        # A separate commit forces delete/reinsert through the durable CDC path.
        final = self.transaction([
            f"INSERT INTO orders (id, tenant, payload, amount) VALUES ({g}, 9, '{label}-reborn', 42.5)",
            f"UPDATE orders SET payload='{label}-second-update' WHERE id={h}",
        ])
        return {"first_barrier": first, "source_barrier": final}

    def wait_compaction(self, build):
        def committed():
            metadata = self.table("orders")
            matching = [snapshot for snapshot in metadata["metadata"]["snapshots"]
                        if snapshot["summary"].get("streaming.operation") == "compact"
                        and snapshot["summary"].get("streaming.build-snapshot-id") == str(build["base_snapshot"])]
            return (metadata, matching[0]) if matching else None
        return self.until("held speculative build did not commit its catch-up result", committed)

    def translated_deletes(self, metadata, compact, build):
        _, files = self.files(metadata, compact["snapshot-id"])
        rewritten = {path: file for path, file in files.items()
                     if file["content"] == 0 and file["added_snapshot"] == compact["snapshot-id"]}
        assert rewritten, "speculative compact added no data files"
        assert all(file["data_sequence"] == build["base_sequence"] for file in rewritten.values())
        masked_ids = []
        delete_sequences = set()
        physical_ids = {}
        for path, file in files.items():
            if file["content"] != 1:
                continue
            positions = self.duck.execute("SELECT file_path, pos FROM read_parquet(?)", [path]).fetchall()
            assert positions == sorted(set(positions))
            for target, position in positions:
                if target in rewritten:
                    data = rewritten[target]
                    assert data["data_sequence"] <= file["data_sequence"] <= compact["sequence-number"]
                    if target not in physical_ids:
                        physical_ids[target] = self.duck.execute("SELECT id FROM read_parquet(?)", [target]).fetchall()
                    masked_ids.append(physical_ids[target][position][0])
                    delete_sequences.add(file["data_sequence"])
        assert sorted(masked_ids) == sorted(build["keys"]), (masked_ids, build["keys"])
        return {"rewritten_files": len(rewritten), "translated_delete_rows": len(masked_ids),
                "delete_sequences": sorted(delete_sequences), "data_sequence": build["base_sequence"],
                "compact_sequence": compact["sequence-number"]}

    def soft_catchup(self):
        build = self.held_build("soft", 1)
        barriers = self.late_mutations(build)
        self.wait_materialized(barriers["source_barrier"])
        self.until("source ACK stalled behind soft compaction", lambda: self.confirmed() >= barriers["source_barrier"])
        assert not self.object_proxy.release_reads.is_set()
        assert self.head() != build["base_snapshot"]
        before_release = self.compare("cdc-while-build-read-held")
        confirmed_while_held = self.confirmed()
        # An unchanged candidate must survive the optional 750ms stall budget.
        # Resume CDC while its original worker completes; do not upload the same
        # data repeatedly just because preparation is slower than that budget.
        self.catalog_proxy.delay_seconds = 1.25
        try:
            self.object_proxy.release_reads.set()
            self.until("soft preparation was not deferred past its stall budget", lambda:
                next((event for event in self.log_fields()
                      if event.get("event") == "compaction_preparation_deferred"
                      and event.get("operation_id") == build["operation_id"]), None), timeout=15)
        finally:
            self.catalog_proxy.delay_seconds = 0
        metadata, compact = self.wait_compaction(build)
        assert compact['summary']['flow.operation-id'] == build['operation_id']
        translated = self.translated_deletes(metadata, compact, build)
        result = self.compare("speculative-catchup-complete")
        assert self.rows("orders", self.initial_metadata) == self.initial_rows
        base_metadata, base_rows = self.histories[build["label"]]
        assert self.rows("orders", base_metadata) == base_rows, "build base changed after catch-up"
        assert any(snapshot["snapshot-id"] == build["base_snapshot"]
                   for snapshot in self.table("orders")["metadata"]["snapshots"]), "build base was expired"
        return build | barriers | {"confirmed_while_held": confirmed_while_held,
            "rows_while_held": before_release, "rows_after_release": result,
            "translated_deletes": translated, "retained_initial_rows": len(self.initial_rows),
            "retained_build_base_rows": len(base_rows)}

    def hard_pressure(self):
        build = self.held_build("hard", 1025)
        # Started after file age 20s. Cross hard age 30s with room for slow
        # preparation within the worker's independent 30s lifetime limit.
        self.until("L0 did not reach the default hard age", lambda:
                   time.time_ns() // 1_000_000 - build["input_timestamp_ms"] >= 31000, timeout=25)
        before = self.head()
        barriers = self.late_mutations(build)
        barrier = barriers["source_barrier"]
        self.until("hard-pressure source transaction was not journaled", lambda:
                   self.metrics().get("flow_journal_durable_lsn", 0) >= barrier)
        pressure = f'flow_table_publication_pressure{{table_id="{self.table_id}"}}'
        self.until("table did not expose hard publication pressure", lambda: self.metrics().get(pressure) == 2)
        for _ in range(4):
            assert self.metrics()["flow_materialized_lsn"] < barrier
            assert self.confirmed() < barrier
            assert self.head() == before, "CDC crossed hard debt while its remedy was held"
            self.alive()
            time.sleep(0.25)
        # Preparation loads the catalog head after the data worker joins. Hard
        # debt cannot resume CDC, so retain that work past the optional 750ms
        # budget instead of throwing it away and rebuilding the same inputs.
        self.catalog_proxy.delay_seconds = 1.25
        try:
            self.object_proxy.release_reads.set()
            prepared = self.until("hard-pressure preparation was discarded instead of completed", lambda:
                next((event for event in self.log_fields()
                      if event.get("event") == "compaction_preparation_ready"
                      and event.get("operation_id") == build["operation_id"]), None), timeout=8)
            assert prepared["elapsed_ms"] >= 1000, "fixture did not cross the optional preparation budget"
            assert not any(event.get("event") == "compaction_preparation_deadline"
                           and event.get("operation_id") == build["operation_id"] for event in self.log_fields())
        finally:
            self.catalog_proxy.delay_seconds = 0
        self.wait_materialized(barrier)
        self.until("hard debt recovery did not ACK", lambda: self.confirmed() >= barrier)
        self.wait_compaction(build)
        return build | barriers | {"preparation_elapsed_ms": prepared["elapsed_ms"],
                                   "rows_after_release": self.compare("hard-pressure-relieved"),
                                   "confirmed_lsn": self.confirmed()}

    def shutdown_build(self):
        build = self.held_build("shutdown", 2049)
        barriers = self.late_mutations(build)
        self.wait_materialized(barriers["source_barrier"])
        self.until("pre-shutdown CDC did not ACK", lambda: self.confirmed() >= barriers["source_barrier"])
        scratch = [path for path in (self.directory / "state/compaction").iterdir() if path.is_dir()]
        assert scratch, "held build has no scratch directory to recover"
        started = time.monotonic()
        self.process.send_signal(signal.SIGTERM)
        code = self.process.wait(timeout=5)
        elapsed = time.monotonic() - started
        assert code == 0
        assert not self.object_proxy.release_reads.is_set(), "test released worker before daemon exit"
        self.process = None
        self.log.close()
        # Accounts may also start maintenance before SIGTERM. Snapshot all
        # remaining scratch after exit, when no worker can create or retire it.
        scratch = [path for path in (self.directory / "state/compaction").iterdir() if path.is_dir()]
        assert scratch, "shutdown removed scratch while the selected worker was unjoined"
        self.object_proxy.release_reads.set()
        self.start()
        recovered = self.until("restart did not discard durable abandoned BUILD", lambda:
                   next((event for event in self.log_fields()
                         if event.get("message") == "discarded compaction builds from the previous process"), None))
        assert 1 <= recovered["abandoned"] <= min(2, len(scratch)), "recovery exceeds bounded BUILD ownership"
        self.until("restart retained abandoned build scratch", lambda:
                   all(not path.exists() for path in scratch), timeout=10)
        self.wait_materialized(barriers["source_barrier"])
        self.until("restart did not rebuild remaining L0", self.no_l0)
        result = self.compare("abandoned-build-restart")
        assert self.rows("orders", self.initial_metadata) == self.initial_rows
        return build | barriers | {"sigterm_seconds": round(elapsed, 3), "exit_code": code,
                                   "durable_build_discarded": True,
                                   "discarded_builds": recovered["abandoned"],
                                   "reclaimed_scratch": [str(path.relative_to(self.directory)) for path in scratch],
                                   "rows_after_restart": result}

    def deadline_cancellation(self):
        build = self.held_build("deadline", 3073)
        pid = self.process.pid
        held_since = time.monotonic()
        scratch = [path for path in (self.directory / "state/compaction").iterdir() if path.is_dir()]
        assert scratch, "held build has no scratch directory"
        selected = urlsplit(build["held_file"])
        selected_path = "/" + selected.netloc + selected.path

        def event(name):
            return next((item for item in self.log_fields()
                         if item.get("event") == name and item.get("operation_id") == build["operation_id"]), None)

        def held_completions():
            with self.object_proxy.lock:
                return [item for item in self.object_proxy.events
                        if item["state"] == "complete" and item.get("held")
                        and unquote(urlsplit(item["path"]).path) == selected_path]

        requested = self.until("build never requested cancellation at its real deadline", lambda:
                               event("compaction_build_cancellation_requested"), timeout=40)
        assert int(requested["elapsed_ms"]) >= 30000, requested
        assert requested["durable_ownership_retained"] is True, requested
        # Start this clock after observing the hold, so the selected GET itself
        # stays held for more than 30s, independently of earlier build setup.
        self.until("selected GET did not remain held beyond 30 seconds", lambda:
                   time.monotonic() - held_since >= 31, timeout=35)
        barriers = self.late_mutations(build)
        barrier = barriers["source_barrier"]
        self.until("deadline-phase source transaction was not journaled", lambda:
                   self.metrics().get("flow_journal_durable_lsn", 0) >= barrier, timeout=10)
        pressure = f'flow_table_publication_pressure{{table_id="{self.table_id}"}}'
        self.until("held deadline build did not reach hard reader pressure", lambda:
                   self.metrics().get(pressure) == 2, timeout=10)
        for _ in range(4):
            assert not self.object_proxy.release_reads.is_set()
            assert not held_completions(), "selected worker request completed before release"
            assert event("compaction_build_retired") is None, "BUILD retired before actual worker join"
            assert all(path.exists() for path in scratch), "worker scratch removed before join"
            assert self.metrics()["flow_materialized_lsn"] < barrier
            assert self.confirmed() < barrier
            self.alive()
            assert self.process.pid == pid
            time.sleep(0.25)
        held_seconds = time.monotonic() - held_since
        self.object_proxy.release_reads.set()
        retired = self.until("released worker did not join and retire BUILD", lambda:
                             event("compaction_build_retired"), timeout=20)
        assert int(retired["elapsed_ms"]) >= int(requested["elapsed_ms"])
        self.until("joined worker retained its scratch directory", lambda:
                   all(not path.exists() for path in scratch), timeout=10)
        self.until("held request did not finish after release", held_completions, timeout=10)
        self.wait_materialized(barrier)
        self.until("CDC did not ACK after deadline cancellation", lambda: self.confirmed() >= barrier)
        self.until("deadline fallback did not drain L0", self.no_l0)
        assert self.process.pid == pid, "deadline cancellation restarted the daemon"
        metadata = self.table("orders")
        assert all(snapshot["summary"].get("flow.operation-id") != build["operation_id"]
                   for snapshot in metadata["metadata"]["snapshots"]), "cancelled build was published"
        rows = self.compare("deadline-cancellation-recovered")
        assert self.rows("orders", self.initial_metadata) == self.initial_rows
        base_metadata, base_rows = self.histories[build["label"]]
        assert self.rows("orders", base_metadata) == base_rows, "deadline cancellation changed build history"
        return build | barriers | {"same_process_id": pid, "held_seconds_after_observation": round(held_seconds, 3),
            "cancellation_requested": requested, "joined_and_retired": retired,
            "held_request_completions": held_completions(), "durable_ownership_retained_while_held": True,
            "scratch_removed_after_join": [str(path.relative_to(self.directory)) for path in scratch],
            "confirmed_lsn": self.confirmed(), "rows_after_release": rows,
            "retained_initial_rows": len(self.initial_rows), "retained_build_base_rows": len(base_rows)}

    def execute(self):
        try:
            self.phase("seed", self.seed)
            self.phase("initial-copy", self.initialize)
            self.start()
            self.phase("snapshot-wal-handoff", self.handoff)
            self.phase("soft-build-cdc-and-translated-deletes", self.soft_catchup)
            self.phase("hard-debt-pauses-ack-during-build", self.hard_pressure)
            self.phase("deadline-cancellation-retains-build-until-join", self.deadline_cancellation)
            self.phase("sigterm-retains-build-and-restart-recovers", self.shutdown_build)
            self.stop()
            self.check_worker_panics()
            self.report["passed"] = True
        except BaseException as error:
            self.report.update(error=str(error), traceback=traceback.format_exc())
            raise
        finally:
            self.stop()
            self.object_proxy.close()
            self.catalog_proxy.close()
            self.report["completed_phases"] = len(self.report["phases"])
            self.report["proxy"] = {
                "s3_completed_requests": sum(event["state"] == "complete" for event in self.object_proxy.events),
                "s3_held_requests": sum(event["state"] == "held" for event in self.object_proxy.events),
                "s3_absence_probes": sum(event["method"] == "HEAD" and event.get("status") == 404
                                         for event in self.object_proxy.events),
                "s3_errors": [event for event in self.object_proxy.events
                              if "error" in event or event.get("status", 0) >= 400
                              and not (event["method"] == "HEAD" and event["status"] == 404)],
                "catalog_absence_probes": sum(event["method"] == "HEAD" and event.get("upstream_status") == 404
                                              for event in self.catalog_proxy.events),
                "catalog_errors": [event for event in self.catalog_proxy.events
                                   if "transport_error" in event or event.get("upstream_status", 0) >= 400
                                   and not (event["method"] == "HEAD" and event["upstream_status"] == 404)],
            }
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
    ConcurrentRun(parser.parse_args()).execute()


if __name__ == "__main__":
    main()
