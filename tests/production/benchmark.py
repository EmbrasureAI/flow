#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["duckdb==1.5.5", "psycopg[binary]==3.3.5", "boto3==1.43.88", "fastavro==1.12.2"]
# ///
"""Open-loop CDC load, commit-to-reader latency, and independent full-row verification."""

import argparse
import csv
from itertools import islice
import hashlib
import json
import math
import os
from pathlib import Path
import random
import signal
import subprocess
import sys
import threading
import time
import traceback
import tempfile
from urllib.parse import urlparse

import fastavro

import duckdb
import psycopg
from psycopg import sql

from reader_compare import compare_snapshots
from benchmark_store import (BenchmarkStore, DenseKeys, LATENCIES, PAGE_ROWS, QUEUE_ITEMS,
                             bounded_lines, zipf_sample, mutation_counts, clock_assessment, catalog_slo_met)
from process_lifecycle import arm_workload, disarm_workload, run_supervised, write_report
from provenance import load_build_manifest, machine_identity, repository_identity, sha256_file, verified_binary
from reconciliation import scan_reconciliation_events

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "local"))
from run import Run, lsn  # noqa: E402


def micros():
    return time.time_ns() // 1000


class Benchmark(Run):
    def __init__(self, args):
        try:
            super().__init__(args)
        except BaseException as error:
            # Run may fail before configure(), or configure may have already
            # acquired a recorder. Only touch a directory this run created.
            if hasattr(self, "report"):
                self.close_run(error)
            raise

    def phase(self, name, action):
        self.report["active_phase"] = name
        write_report(self.directory / "report.json", self.report)
        started = time.monotonic()
        print(f"[{name}]", flush=True)
        details = action()
        self.report["phases"].append({"name": name, "seconds": round(time.monotonic() - started, 3), "details": details})
        self.report.pop("active_phase")
        write_report(self.directory / "report.json", self.report)

    def close_run(self, failure=None):
        if failure is not None:
            self.report.update(passed=False, failure=str(failure))
            self.report.setdefault("traceback", traceback.format_exc())
        actions = []
        if getattr(self, "compactor", None) is not None:
            actions.append(("compactor", self.stop_compactor))
        if getattr(self, "process", None) is not None:
            actions.append(("daemon", self.stop))
        for name in ("store", "pg", "duck"):
            resource = getattr(self, name, None)
            if resource is not None:
                actions.append((name, resource.close))
        for name, action in actions:
            try:
                action()
            except BaseException as error:
                self.report.setdefault("cleanup_errors", []).append({"resource": name, "error": str(error)})
                self.report.update(passed=False)
                if failure is None:
                    failure = error
                    self.report["failure"] = str(error)
        try:
            self.report["state_bytes"] = {
                name: sum(path.stat().st_size for path in (self.directory / "state" / name).rglob("*") if path.is_file())
                for name in ("index", "journal", "spool")}
        except BaseException as error:
            self.report.setdefault("cleanup_errors", []).append({"resource": "state inventory", "error": str(error)})
            self.report.update(passed=False)
            if failure is None:
                failure = error
                self.report["failure"] = str(error)
        finally:
            write_report(self.directory / "report.json", self.report)
        if failure is not None:
            raise failure

    def payload_expression(self, key):
        # Expressions are internal SQL fragments; workload inputs remain bind parameters.
        if self.args.payload_entropy == "high":
            return sql.SQL("left((SELECT string_agg(md5(" + key + " || ':' || blocks.n::text), '' ORDER BY blocks.n) "
                           "FROM generate_series(1,%s) AS blocks(n)),%s)")
        return sql.SQL("left(repeat(md5(" + key + "),%s),%s)")

    def configure(self):
        self.names = [f"events_{i}" for i in range(self.args.tables)]
        self.errors = []
        self.duck.execute("SET memory_limit = '1GiB'")
        self.duck.execute("SET temp_directory = ?", [str(self.directory / "duckdb-temp")])
        self.stop_writers = threading.Event()
        self.writer_connections = [None] * self.args.writers
        self.writer_lock = threading.Lock()
        self.compactor = self.compactor_log = None
        self.compactor_binary = self.compactor_log_path = None
        self.compactor_generation = 0
        self.daemon_stops = []
        text = f'''state_dir = {json.dumps(str(self.directory / 'state'))}
[source]
connection_env = "FLOW_LOCAL_POSTGRES_URL"
id = "{self.name}"
slot = "{self.name}"
publication = "{self.name}"
ack_mode = "materialized"
journal_durability = "local-disk"
[catalog]
uri = {json.dumps(self.args.catalog_uri)}
warehouse = {json.dumps(self.args.warehouse)}
"s3.endpoint" = {json.dumps(self.args.s3_endpoint)}
"s3.region" = {json.dumps(os.environ.get('AWS_REGION', 'us-east-1'))}
"s3.path-style-access" = "true"
'''
        text += f'''[compaction]
data_rewrite_scope = {json.dumps({'current': 'all', 'l0-only': 'l0-only', 'delete-only': 'disabled', 'disabled': 'disabled'}[self.args.maintenance_mode])}
'''
        if self.args.maintenance_mode in ("disabled", "delete-only"):
            text += '''l0_soft_files = 1000000
l0_hard_files = 2000000
l0_soft_bytes = 1152921504606846976
l0_hard_bytes = 2305843009213693952
oldest_l0_soft_ms = 2305843009213693952
oldest_l0_hard_ms = 4611686018427387904
'''
        if self.args.maintenance_mode in ("disabled", "delete-only", "l0-only"):
            text += '''stable_small_soft_files = 1000000
stable_small_hard_files = 2000000
'''
        elif self.args.stable_small_soft_files is not None:
            text += f'''stable_small_soft_files = {self.args.stable_small_soft_files}
stable_small_hard_files = {self.args.stable_small_hard_files}
'''
        if self.args.maintenance_mode == "disabled":
            text += '''delete_files_soft = 1000000
delete_files_hard = 2000000
deleted_rows_percent = 100
'''
        text += f'''[limits]
table_workers = {self.args.table_workers}
manifest_max_count = {self.args.manifest_max_count}
batch_rows = {self.args.batch_rows}
chunk_bytes = 1048576
batch_bytes = 8388608
pending_transactions = {self.args.pending_transactions}
commits_per_second = 100
journal_bytes = 8589934592
spool_bytes = 4294967296
wal_soft_bytes = 8589934592
wal_hard_bytes = 17179869184
snapshot_retention_secs = 3600
'''
        for name in self.names:
            text += f'''\n[[tables]]
source_namespace = "{self.name}"
source_table = "{name}"
target_namespace = ["{self.name}"]
target_table = "{name}"
format_version = {self.args.format_version}
primary_key = [0]
append_only = {str(self.args.profile == 'append').lower()}
columns = [
  {{ field_id = 1, name = "id", data_type = "Int64", nullable = false }},
  {{ field_id = 2, name = "version", data_type = "Int64", nullable = false }},
  {{ field_id = 3, name = "payload", data_type = "String", nullable = false }},
]
'''
        self.config = self.directory / "flow.toml"
        self.config.write_text(text)
        # Timing events are required evidence, independent of the caller's logging preference.
        self.environment = os.environ | {"FLOW_LOCAL_POSTGRES_URL": self.args.postgres_url, "RUST_LOG": "info,flow_events=debug"}
        self.store = BenchmarkStore(self.directory / "evidence.sqlite3")

    def verify_runtime_inputs(self):
        daemon = verified_binary(self.build_manifest, "daemon", self.args.binary)
        config_sha256 = sha256_file(self.config)
        expected = self.report["artifact_identity"]["config"]["sha256"]
        if config_sha256 != expected:
            raise RuntimeError(
                f"runtime config changed during the run: expected {expected}, got {config_sha256}"
            )
        return {"daemon": daemon, "config_sha256": config_sha256}

    def command(self, command):
        result = super().command(command)
        if command == "run" and self.args.maintenance_mode == "disabled":
            result += ["--roles", "ingest,coordinator"]
        return result

    def start(self):
        self.verify_runtime_inputs()
        super().start()

    def stop(self, crash=False):
        if self.process is None:
            return None
        process = self.process
        forced_kill = False
        if process.poll() is None:
            process.send_signal(signal.SIGKILL if crash else signal.SIGINT)
            try:
                process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                forced_kill = True
                process.kill()
                process.wait(timeout=10)
        self.process = None
        self.log.close()
        record = {
            "pid": process.pid,
            "returncode": process.returncode,
            "crash_requested": crash,
            "forced_kill": forced_kill,
            "generation": self.generation,
        }
        self.daemon_stops.append(record)
        self.report["daemon_stops"] = self.daemon_stops
        self.verify_runtime_inputs()
        return record

    def seed(self):
        self.pg.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(self.name)))
        for name in self.names:
            table = sql.Identifier(name)
            self.pg.execute(sql.SQL("CREATE TABLE {} (id bigint PRIMARY KEY, version bigint NOT NULL, payload text NOT NULL)").format(table))
            self.pg.execute(sql.SQL("ALTER TABLE {} REPLICA IDENTITY FULL").format(table))
            # Each row has a distinct deterministic payload. Its entropy is recorded below.
            self.pg.execute(sql.SQL("INSERT INTO {} SELECT i,0,{} FROM generate_series(1,%s) i").format(table, self.payload_expression("i::text")),
                            (math.ceil(self.args.row_bytes / 32), self.args.row_bytes, self.args.initial_rows))
        self.pg.execute(sql.SQL("CREATE PUBLICATION {} FOR TABLE {}").format(
            sql.Identifier(self.name), sql.SQL(",").join(map(sql.Identifier, self.names))))
        self.oids = {name: self.pg.execute("SELECT %s::regclass::oid", (name,)).fetchone()[0] for name in self.names}
        return {"rows_per_table": self.args.initial_rows, "tables": self.args.tables}

    def initialize(self):
        self.verify_runtime_inputs()
        with (self.directory / "init.log").open("wb") as log:
            subprocess.run(self.command("init"), env=self.environment, stdout=log,
                           stderr=subprocess.STDOUT, check=True, timeout=self.args.timeout)
        self.verify_runtime_inputs()
        metrics = self.directory / "state" / "metrics.prom"
        if metrics.exists():
            (self.directory / "init-metrics.prom").write_bytes(metrics.read_bytes())
        return self.verify()

    def verify(self, pinned_tables=None):
        result = {}
        for name in self.names:
            query = sql.SQL("SELECT id,version,md5(payload) FROM {} ORDER BY id").format(sql.Identifier(name))
            metadata = self.table(name) if pinned_tables is None else pinned_tables[name]
            actual_version = metadata["metadata"]["format-version"]
            assert actual_version == self.args.format_version, (
                f"{name}: expected Iceberg v{self.args.format_version}, got v{actual_version}")
            # COPY is a disk sink; fetchmany on an ordinary result does not bound
            # libpq's result buffer or DuckDB's materialized Python result.
            snapshot_id = metadata["metadata"]["current-snapshot-id"]
            evidence = self.directory / f"verification-{name}-{snapshot_id}"
            evidence.with_suffix(".metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
            path = evidence.with_suffix(".csv")
            destination = "'" + str(path).replace("'", "''") + "'"
            self.duck.execute("COPY (SELECT id,version,md5(payload) FROM iceberg_scan(?) ORDER BY id) TO " +
                              destination + " (FORMAT CSV, HEADER false)", [metadata["metadata-location"]])
            count = 0
            digest = hashlib.sha256()
            verified = False
            try:
                with path.open(newline="") as rows, self.pg.transaction():
                    self.pg.execute("SET TRANSACTION READ ONLY")
                    with self.pg.cursor(name="benchmark_full_rows") as source:
                        source.itersize = 4096
                        source.execute(query)
                        target = csv.reader(rows)
                        while True:
                            expected = source.fetchmany(4096)
                            actual = [(int(key), int(version), payload_hash)
                                      for key, version, payload_hash in islice(target, 4096)]
                            if expected != actual:
                                mismatch = next(((a, b) for a, b in zip(expected, actual) if a != b), None)
                                raise AssertionError(f"{name}: source/target rows differ at offset {count}, first={mismatch}")
                            if not expected:
                                break
                            digest.update(repr(expected).encode())
                            count += len(expected)
                verified = True
            finally:
                if verified:
                    path.unlink(missing_ok=True)
            result[name] = {"rows": count, "sha256": digest.hexdigest(), "snapshot": metadata["metadata"]["current-snapshot-id"]}
        return result

    def check_worker_panics(self):
        # Keep the raw logs, but never load an entire long-running daemon log.
        self.report["worker_panics"] = []
        paths = sorted(self.directory.glob("daemon-*.log")) + sorted(self.directory.glob("compactor*.log"))
        for path in paths:
            for number, line in enumerate(bounded_lines(path), 1):
                if line.startswith("thread '") and " panicked at " in line:
                    self.report["worker_panics"] = [{"log": path.name, "line": number, "message": line.rstrip()}]
                    raise AssertionError(f"worker panic logged in {path.name}:{number}")

    def start_compactor(self, binary=None, *, max_input_files=None, max_input_bytes=None):
        binary = binary or self.args.compactor_binary
        if binary is None:
            return
        binary = binary.resolve()
        if self.compactor is not None:
            if binary != self.compactor_binary:
                raise RuntimeError(f"compactor already running from {self.compactor_binary}")
            return
        verified_binary(self.build_manifest, "external_compactor", binary)
        self.verify_runtime_inputs()
        self.compactor_generation += 1
        self.compactor_log_path = self.directory / f"compactor-{self.compactor_generation}.log"
        self.compactor_log = self.compactor_log_path.open("wb")
        try:
            command = [str(binary), "--config", str(self.config),
                       "--interval-ms", str(self.args.compactor_interval_ms)]
            if max_input_files is not None:
                command += ["--max-input-files", str(max_input_files)]
            if max_input_bytes is not None:
                command += ["--max-input-bytes", str(max_input_bytes)]
            self.compactor = subprocess.Popen(command,
                                              env=self.environment, stdout=self.compactor_log,
                                              stderr=subprocess.STDOUT)
            self.compactor_binary = binary
        except BaseException:
            self.compactor_log.close()
            self.compactor_log = self.compactor_log_path = None
            raise

    def stop_compactor(self):
        if self.compactor is None:
            return None
        process = self.compactor
        binary = self.compactor_binary
        log_path = self.compactor_log_path
        forced_kill = False
        if process.poll() is None:
            process.send_signal(signal.SIGINT)
            try:
                process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                forced_kill = True
                process.kill()
                process.wait(timeout=10)
        self.compactor = None
        self.compactor_log.close()
        self.compactor_log = self.compactor_binary = self.compactor_log_path = None
        return {"pid": process.pid, "returncode": process.returncode,
                "binary": str(binary), "log": log_path.name, "forced_kill": forced_kill}

    def alive(self):
        super().alive()
        if self.compactor and self.compactor.poll() is not None:
            raise RuntimeError(f"compactor exited {self.compactor.returncode}")
        self.store.check()
        if self.errors:
            raise RuntimeError("workload failed: " + self.errors[0])

    def writer(self, worker, start_mono, start_us, scheduled):
        args = self.args
        rng = random.Random(args.seed + worker)
        # Writers own disjoint positive keys; negative keys are immutable reader probes.
        keys = self.key_pools[worker]
        record = None
        pending_postcommit = None
        interval = args.transaction_rows / args.rate
        try:
            with psycopg.connect(args.postgres_url, autocommit=True, connect_timeout=10) as pg:
                with self.writer_lock:
                    self.writer_connections[worker] = pg
                pg.execute(sql.SQL("SET search_path TO {}, public").format(sql.Identifier(self.name)))
                pg.execute("SET statement_timeout = '30s'")
                for sequence in range(worker, scheduled, args.writers):
                    if self.stop_writers.is_set():
                        return
                    due = start_mono + sequence * interval
                    if self.stop_writers.wait(max(0, due - time.monotonic())):
                        return
                    admitted_ns = time.monotonic_ns()
                    record = {"sequence": sequence, "scheduled_at_micros": start_us + int(sequence * interval * 1e6),
                              "measured": sequence * interval >= args.warmup, "table": self.names[(sequence // args.writers) % args.tables],
                              "admission_lag_micros": (admitted_ns - int(due * 1e9)) / 1000}
                    if pending_postcommit is not None:
                        record["previous_postcommit_keys_micros"] = pending_postcommit["postcommit_keys_micros"]
                    if record["admission_lag_micros"] > args.max_start_delay_ms * 1000:
                        record["missed_arrival"] = True
                        record["intent_enqueue_wait_micros"] = self.store.record(
                            record, enqueue_field="intent_enqueue_wait_micros")
                        pending_postcommit = None
                        continue
                    key_prepare_started_ns = time.monotonic_ns()
                    name, marker = record["table"], -(sequence + 1)
                    active = keys[name]
                    updates, deletes, inserts = mutation_counts(args.profile, args.transaction_rows)
                    # One ordinary insertion per tx is reserved as an immutable visibility probe.
                    if args.key_distribution == "zipf":
                        selected = zipf_sample(rng, range(len(active)), updates + deletes, args.zipf_exponent)
                    else:
                        population = max(updates + deletes, len(active) // 100) if args.key_distribution == "hot" else len(active)
                        selected = rng.sample(range(population), updates + deletes)
                    chosen = [active[index] for index in selected]
                    update_keys, delete_keys = chosen[:updates], chosen[updates:]
                    new_keys = [args.initial_rows + 1 + sequence * args.transaction_rows + i for i in range(inserts - 1)]
                    record["key_prepare_micros"] = (time.monotonic_ns() - key_prepare_started_ns) / 1000
                    record["started_at_micros"] = micros()
                    record["intent_enqueue_wait_micros"] = self.store.record(
                        record, marker, enqueue_field="intent_enqueue_wait_micros")
                    pending_postcommit = None
                    postgres_started_ns = time.monotonic_ns()
                    try:
                        with pg.transaction():
                            record["xid"] = pg.execute("SELECT pg_current_xact_id()::text").fetchone()[0]
                            if updates:
                                affected = pg.execute(sql.SQL("UPDATE {} SET version=%s,payload={} WHERE id=ANY(%s)").format(sql.Identifier(name), self.payload_expression("id::text || ':' || %s::text")),
                                                      (sequence + 1, sequence + 1, math.ceil(args.row_bytes / 32), args.row_bytes, update_keys)).rowcount
                                assert affected == updates, "workload update missed its owned keys"
                            if deletes:
                                affected = pg.execute(sql.SQL("DELETE FROM {} WHERE id=ANY(%s)").format(sql.Identifier(name)), (delete_keys,)).rowcount
                                assert affected == deletes, "workload delete missed its owned keys"
                            affected = pg.execute(sql.SQL("INSERT INTO {} SELECT i,%s,{} FROM unnest(%s::bigint[]) i").format(sql.Identifier(name), self.payload_expression("i::text || ':' || %s::text")),
                                                  (sequence + 1, sequence + 1, math.ceil(args.row_bytes / 32), args.row_bytes, new_keys + [marker])).rowcount
                            assert affected == inserts, "workload inserted an unexpected number of rows"
                    finally:
                        record["postgres_transaction_micros"] = (time.monotonic_ns() - postgres_started_ns) / 1000
                    record.update(commit_response_at_micros=micros(), mutations=args.transaction_rows,
                                  update_rows=updates, delete_rows=deletes, insert_rows=inserts, marker=marker)
                    record["commit_enqueue_wait_micros"] = self.store.record(
                        record, marker, enqueue_field="commit_enqueue_wait_micros")
                    postcommit_started_ns = time.monotonic_ns()
                    try:
                        active.remove_selected(selected[updates:])
                        active.extend(new_keys)
                    finally:
                        pending_postcommit = {
                            "sequence": sequence,
                            "measured": record["measured"],
                            "postcommit_keys_micros": (time.monotonic_ns() - postcommit_started_ns) / 1000,
                        }
        except BaseException:
            self.errors.append(traceback.format_exc())
            if record is not None:
                try:
                    self.store.record(record, -(record["sequence"] + 1) if not record.get("missed_arrival") else None)
                except BaseException:
                    self.errors.append(traceback.format_exc())
        finally:
            if pending_postcommit is not None:
                try:
                    self.store.sample("writer_terminal", {"worker": worker, **pending_postcommit})
                except BaseException:
                    self.errors.append(traceback.format_exc())
            with self.writer_lock:
                self.writer_connections[worker] = None

    def observe(self, cursors):
        self.store.flush()
        for name in self.names:
            pending, cursors[name] = self.store.pending(name, cursors.get(name))
            if not pending:
                continue
            began = time.monotonic()
            metadata = self.table(name)
            catalog_ms = (time.monotonic() - began) * 1000
            location = metadata["metadata-location"]
            began = time.monotonic()
            started_at = micros()
            try:
                markers = self.duck.execute("SELECT id FROM iceberg_scan(?) WHERE id IN (SELECT unnest(?::BIGINT[])) LIMIT " + str(PAGE_ROWS + 1),
                                            [location, [marker for _, marker in pending]]).fetchall()
            except duckdb.Error as error:
                self.report.setdefault("reader_failures", []).append({
                    "table": name, "metadata_location": location,
                    "started_at_micros": started_at, "elapsed_ms": (time.monotonic() - began) * 1000,
                    "error": str(error),
                })
                raise
            observed = micros()
            expected = {marker: sequence for sequence, marker in pending}
            returned = [marker for (marker,) in markers]
            if len(returned) != len(set(returned)) or any(marker not in expected for marker in returned):
                raise AssertionError("reader returned duplicate or unrequested immutable markers")
            self.store.observed([expected[marker] for marker in returned], observed)
            self.store.sample("reader", {"table": name, "observed_at_micros": observed, "catalog_get_ms": catalog_ms,
                                         "scan_ms": (time.monotonic() - began) * 1000, "marker_rows": len(markers),
                                         "requested_markers": len(pending), "metadata_location": location})

    def sample_resources(self):
        metrics = self.metrics()
        row = self.pg.execute("SELECT pg_wal_lsn_diff(pg_current_wal_lsn(),restart_lsn),pg_wal_lsn_diff(pg_current_wal_lsn(),confirmed_flush_lsn) FROM pg_replication_slots WHERE slot_name=%s", (self.name,)).fetchone()
        pids = [self.process.pid] + ([self.compactor.pid] if self.compactor else [])
        process = subprocess.check_output(["ps", "-o", "pid=,rss=,%cpu=", "-p", ",".join(map(str, pids))], text=True)
        self.store.sample("resource", {"at_micros": micros(), "metrics": metrics, "retained_wal_bytes": int(row[0]),
                               "unacknowledged_wal_bytes": int(row[1]), "processes": [
                                   {"pid": int(pid), "rss_bytes": int(rss) * 1024, "cpu_percent": float(cpu)}
                                   for pid, rss, cpu in map(str.split, process.splitlines())]})

    def workload(self):
        args = self.args
        scheduled = math.floor((args.duration + args.warmup) * args.rate / args.transaction_rows)
        interval = args.transaction_rows / args.rate
        warmup_scheduled = min(scheduled, math.ceil(args.warmup / interval))
        while warmup_scheduled and (warmup_scheduled - 1) * interval >= args.warmup:
            warmup_scheduled -= 1
        while warmup_scheduled < scheduled and warmup_scheduled * interval < args.warmup:
            warmup_scheduled += 1
        self.planned_arrivals = {
            "warmup": warmup_scheduled,
            "measured": scheduled - warmup_scheduled,
            "all": scheduled,
        }
        self.key_pools = [{} for _ in range(args.writers)]
        threads = []
        finished = False
        try:
            # Initial disk setup is outside the arrival clock. Each rank maps to
            # the same key as the original per-writer list, including deletions.
            for worker, pools in enumerate(self.key_pools):
                for name in self.names:
                    pools[name] = DenseKeys(self.directory / f"keys-{worker}-{name}.bin",
                                            range(worker + 1, args.initial_rows + 1, args.writers))
            self.report["clock"] = {
                "before": self.calibrate_clock(),
                "method": "minimum RTT of five queries before writer workload and after final observation drain; average offset; raw signed values retained",
                "assumption": "offset stays within endpoint offsets and RTT uncertainty during the workload and observation drain",
            }
            start_mono, start_us = time.monotonic() + .5, micros() + 500000
            writer_deadline = start_mono + args.warmup + args.duration + args.timeout
            write_report(self.directory / "report.json", self.report)
            arm_workload(self.directory, writer_deadline)
            self.measured_start_us = start_us + int(args.warmup * 1e6)
            self.measured_end_us = self.measured_start_us + int(args.duration * 1e6)
            threads = [threading.Thread(target=self.writer, args=(worker, start_mono, start_us, scheduled)) for worker in range(args.writers)]
            for thread in threads:
                thread.start()
            cursors = {}
            next_sample = time.monotonic()
            while any(thread.is_alive() for thread in threads):
                if time.monotonic() >= writer_deadline:
                    raise TimeoutError("writers exceeded the workload deadline")
                self.alive()
                self.observe(cursors)
                if time.monotonic() >= next_sample:
                    self.sample_resources()
                    next_sample = time.monotonic() + 1
                time.sleep(args.reader_poll_seconds)
            disarm_workload(self.directory)
            self.alive()
            deadline = time.monotonic() + args.timeout
            while time.monotonic() < deadline:
                self.alive()
                self.observe(cursors)
                self.sample_resources()
                if self.store.all_committed_observed():
                    break
                time.sleep(args.reader_poll_seconds)
            self.report["clock"]["after"] = self.calibrate_clock()
            finished = True
        finally:
            self.stop_writers.set()
            with self.writer_lock:
                connections = [connection for connection in self.writer_connections if connection is not None]
            for connection in connections:
                try:
                    connection.cancel_safe(timeout=5)
                except psycopg.Error as error:
                    self.report.setdefault("writer_cancellation_errors", []).append(str(error))
            # Keep the deadline armed until joins complete. If a native call or
            # cancellation stalls, the supervising process terminates this group.
            for thread in threads:
                thread.join()
            disarm_workload(self.directory)
            for pools in self.key_pools:
                for keys in pools.values():
                    keys.close()
            if not finished:
                self.store.export(self.directory)
        try:
            return self.latencies()
        finally:
            enriched = self.store.db.execute("SELECT 1 FROM sqlite_master WHERE name='analysis'").fetchone() is not None
            self.store.export(self.directory, enriched=enriched)

    def calibrate_clock(self):
        samples = []
        for _ in range(5):
            before = micros()
            server = self.pg.execute("SELECT (extract(epoch FROM clock_timestamp()) * 1000000)::bigint").fetchone()[0]
            after = micros()
            samples.append({"at_micros": (before + after) // 2, "postgres_minus_client_us": server - (before + after) / 2,
                            "roundtrip_us": after - before})
        return min(samples, key=lambda sample: sample["roundtrip_us"])

    def latencies(self):
        self.store.import_logs(sorted(self.directory.glob("daemon-*.log")), self.name)
        clock = self.report["clock"]
        clock.update(clock_assessment(clock["before"], clock["after"], self.args.max_clock_uncertainty_ms * 1000))
        self.store.analyze(self.oids, clock["correction_us"])
        db = self.store.db
        summary = db.execute("""
            SELECT count(*) AS scheduled, count(t.commit_response_at_micros) AS completed,
                coalesce(sum(t.missed_arrival=1),0) AS missed,
                coalesce(sum(t.missed_arrival IS NULL AND t.commit_response_at_micros IS NULL),0) AS source_commit,
                coalesce(sum(t.commit_response_at_micros IS NOT NULL AND a.end_lsn IS NULL),0) AS journal,
                coalesce(sum(t.commit_response_at_micros IS NOT NULL AND a.end_lsn IS NOT NULL
                    AND a.catalog_committed_at_micros IS NULL),0) AS catalog,
                coalesce(sum(t.commit_response_at_micros IS NOT NULL AND a.catalog_committed_at_micros IS NOT NULL
                    AND t.reader_observed_at_micros IS NULL),0) AS reader,
                max(a.catalog_committed_at_micros) AS catalog_end,
                coalesce(sum(t.commit_response_at_micros>=?),0) AS late_commits,
                coalesce(sum(t.mutations),0) AS mutations,coalesce(sum(t.insert_rows),0) AS insert_rows,
                coalesce(sum(t.update_rows),0) AS update_rows,coalesce(sum(t.delete_rows),0) AS delete_rows
            FROM transactions t JOIN analysis a USING(sequence) WHERE t.measured=1
        """, (self.measured_end_us,)).fetchone()
        missing = {name: summary[name] for name in ("source_commit", "journal", "catalog", "reader") if summary[name]}
        totals = {name: summary[name] for name in ("mutations", "insert_rows", "update_rows", "delete_rows")}
        cohorts = db.execute("""
            SELECT count(*) AS scheduled, count(commit_response_at_micros) AS committed,
                coalesce(sum(missed_arrival=1),0) AS missed,
                coalesce(sum(measured=0),0) AS warmup_scheduled,
                coalesce(sum(measured=0 AND commit_response_at_micros IS NOT NULL),0) AS warmup_committed,
                coalesce(sum(measured=0 AND missed_arrival=1),0) AS warmup_missed
            FROM transactions
        """).fetchone()
        window_rows = db.execute("SELECT coalesce(sum(mutations),0) FROM transactions "
                                 "WHERE commit_response_at_micros>=? AND commit_response_at_micros<?",
                                 (self.measured_start_us, self.measured_end_us)).fetchone()[0]
        percentiles = {name: self.store.distribution(name) for name in LATENCIES}
        requested = self.store.distribution("scheduled_to_reader", self.planned_arrivals["measured"])
        unobserved = db.execute("SELECT count(*) FROM transactions WHERE marker IS NOT NULL "
                                "AND reader_observed_at_micros IS NULL").fetchone()[0]
        observations_complete = (bool(summary["completed"]) and not missing and not unobserved and
                                 summary["completed"] == percentiles["commit_to_reader"]["count"])
        catalog_delay = catalog_rate = None
        if observations_complete:
            catalog_end = max(self.measured_end_us, summary["catalog_end"])
            catalog_delay = (catalog_end - self.measured_end_us) / 1e6
            catalog_rate = totals["mutations"] / ((catalog_end - self.measured_start_us) / 1e6)
        def cohort(name, recorded, committed, missed):
            planned = self.planned_arrivals[name]
            return {"planned": planned, "recorded": recorded, "committed": committed,
                    "missed": missed, "unrecorded": planned - recorded}

        return {"scheduled_transactions": summary["scheduled"], "committed_transactions": summary["completed"],
                "missed_scheduled_arrivals": summary["missed"], "missing_observations": missing, "affected_rows": totals,
                "arrival_cohorts": {
                    "measured": cohort("measured", summary["scheduled"], summary["completed"], summary["missed"]),
                    "warmup": cohort("warmup", cohorts["warmup_scheduled"], cohorts["warmup_committed"],
                                     cohorts["warmup_missed"]),
                    "all": cohort("all", cohorts["scheduled"], cohorts["committed"], cohorts["missed"]),
                },
                "unobserved_committed_transactions_including_warmup": unobserved,
                "requested_mutations_per_second": self.args.rate,
                "source_commit_window_mutations_per_second": window_rows / self.args.duration,
                "catalog_completion_delay_after_measurement_seconds": catalog_delay,
                "measured_cohort_catalog_mutations_per_second_including_drain": catalog_rate,
                "scheduled_cohort_mutations_per_second": totals["mutations"] / self.args.duration,
                "commits_after_measurement_window": summary["late_commits"],
                "latency": percentiles,
                "catalog_latency_slo_met": catalog_slo_met(percentiles["commit_to_catalog"], clock,
                                                          self.args.catalog_p95_ms, self.args.catalog_p99_ms),
                "catalog_latency_qualification_available": clock["latency_qualification_available"],
                "catalog_latency_slo_uncertainty_ms": clock["uncertainty_us"] / 1000,
                "catalog_latency_slo_ms": {"p95": self.args.catalog_p95_ms, "p99": self.args.catalog_p99_ms},
                "scheduled_to_reader_including_failures": requested,
                "writer_timing": self.store.writer_timing_summary(),
                "all_committed_transactions_observed": observations_complete,
                "target_arrival_rate_sustained": (
                    summary["scheduled"] == self.planned_arrivals["measured"]
                    and summary["missed"] == 0
                    and window_rows / self.args.duration >= self.args.rate * .95
                )}

    def avro(self, path):
        # Inventory polling must not retain all historical manifests in Run's cache.
        uri = urlparse(path)
        if uri.scheme != "s3":
            raise AssertionError(f"expected S3 artifact, got {path}")
        with tempfile.SpooledTemporaryFile(max_size=1 << 20) as spool:
            body = self.s3.get_object(Bucket=uri.netloc, Key=uri.path.lstrip("/"))["Body"]
            try:
                while chunk := body.read(65536):
                    spool.write(chunk)
            finally:
                body.close()
            spool.seek(0)
            yield from fastavro.reader(spool)

    def inventory(self, pinned_tables=None):
        result = {}
        for name in self.names:
            table = self.table(name) if pinned_tables is None else pinned_tables[name]
            metadata = table["metadata"]
            assert metadata["format-version"] == self.args.format_version, (
                f"{name}: expected Iceberg v{self.args.format_version}, got v{metadata['format-version']}")
            current = next(snap for snap in metadata["snapshots"] if snap["snapshot-id"] == metadata["current-snapshot-id"])
            counts = {"manifests": 0, "data_files": 0, "delete_files": 0,
                      "data_bytes": 0, "delete_bytes": 0, "data_rows": 0, "delete_rows": 0,
                      "deletion_vectors": 0, "deletion_vector_bytes": 0}
            delete_objects = {}
            vector_targets = set()
            for manifest in self.avro(current["manifest-list"]):
                counts["manifests"] += 1
                for entry in self.avro(manifest["manifest_path"]):
                    if entry["status"] == 2:
                        continue
                    file = entry["data_file"]
                    if file["content"] == 0:
                        counts["data_files"] += 1
                        counts["data_bytes"] += file["file_size_in_bytes"]
                        counts["data_rows"] += file["record_count"]
                    elif file["content"] == 1:
                        counts["delete_files"] += 1
                        delete_objects[file["file_path"]] = file["file_size_in_bytes"]
                        counts["delete_rows"] += file["record_count"]
                        if file["file_format"].upper() == "PUFFIN":
                            assert metadata["format-version"] == 3
                            target = file["referenced_data_file"]
                            assert target and target not in vector_targets, "multiple DVs for one data file"
                            vector_targets.add(target)
                            assert file["content_offset"] >= 4 and file["content_size_in_bytes"] > 0
                            assert file["content_offset"] + file["content_size_in_bytes"] <= file["file_size_in_bytes"]
                            counts["deletion_vectors"] += 1
                            counts["deletion_vector_bytes"] += file["content_size_in_bytes"]
            counts["delete_bytes"] = sum(delete_objects.values())
            counts["delete_objects"] = len(delete_objects)
            result[name] = {
                            **counts,
                            "format_version": metadata["format-version"],
                            "average_data_file_bytes": counts["data_bytes"] / counts["data_files"] if counts["data_files"] else None,
                            "delete_files_per_data_file": counts["delete_files"] / counts["data_files"] if counts["data_files"] else None,
                            "delete_rows_per_data_row": counts["delete_rows"] / counts["data_rows"] if counts["data_rows"] else None,
                            "current_snapshot_id": metadata["current-snapshot-id"],
                            "snapshots": len(metadata["snapshots"]),
                            "current_external_compaction": "local-test.compaction-id" in current["summary"],
                            "compaction_commits": sum(snap["summary"].get("streaming.operation") == "compact" or
                                                      "local-test.compaction-id" in snap["summary"] for snap in metadata["snapshots"])}
        return result

    def scans(self):
        result = {}
        for name in self.names:
            began = time.monotonic()
            metadata = self.table(name)
            metadata_ms = (time.monotonic() - began) * 1000
            timings = []
            for _ in range(3):
                began = time.monotonic()
                self.duck.execute("SELECT count(*),sum(version),sum(length(payload)) FROM iceberg_scan(?)", [metadata["metadata-location"]]).fetchone()
                timings.append((time.monotonic() - began) * 1000)
            result[name] = {"catalog_get_ms": metadata_ms, "scan_ms": timings, "metadata_location": metadata["metadata-location"]}
        return result

    def resource_summary(self):
        """Sampled resource bounds and process counters, not billing estimates."""
        gauges = ("flow_table_", "flow_source_retained_wal_bytes", "flow_journal_bytes")
        peaks = {}
        samples, peak_rss = 0, 0
        for sample in self.store.samples("resource"):
            samples += 1
            peak_rss = max(peak_rss, max((p["rss_bytes"] for p in sample["processes"]), default=0))
            for name, value in sample["metrics"].items():
                if name.startswith(gauges) and not name.split("{", 1)[0].endswith("_total"):
                    peaks[name] = max(peaks.get(name, value), value)
        final = self.metrics()
        counters = {name: value for name, value in final.items()
                    if name.split("{", 1)[0].endswith(("_total", "_sum", "_count"))}
        return {"samples": samples, "sampled_peak_gauges": peaks,
                "peak_sampled_process_rss_bytes": peak_rss,
                "process_counters_through_drain": counters,
                "method": "One-second observations can miss peaks. Counters include warmup, measured work and drain; "
                          "FileIO calls/accepted bytes exclude hidden SDK requests and are not provider billing. "
                          "Initial COPY metrics are preserved separately in init-metrics.prom."}

    def source_acknowledgement(self):
        """Prove PostgreSQL has acknowledged the exact materialized prefix."""
        started_at_ms = micros() // 1000
        status_path = self.directory / "state" / "status.json"

        def aligned():
            if not status_path.exists():
                return None
            status = json.loads(status_path.read_text())
            if status["source_id"] != self.name or status["process_id"] != self.process.pid:
                raise AssertionError("status observation does not belong to the current daemon")
            if not status["ready"] or status["updated_at_ms"] <= started_at_ms:
                return None
            watermarks = status["watermarks"]
            materialized = watermarks["materialized_lsn"]
            locally_drained = (
                status["pending_transactions"] == 0
                and status["captured_durable_lsn"] == materialized
                and watermarks["journal_durable_lsn"] == materialized
                and watermarks["received_lsn"] == materialized
            )
            if not locally_drained:
                return None
            row = self.pg.execute(
                "SELECT confirmed_flush_lsn::text FROM pg_catalog.pg_replication_slots WHERE slot_name=%s",
                (self.name,),
            ).fetchone()
            if row is None or row[0] is None:
                raise AssertionError("replication slot or confirmed flush LSN is missing")
            confirmed = lsn(row[0])
            if confirmed > materialized:
                raise AssertionError("PostgreSQL acknowledged beyond the materialized watermark")
            if confirmed < materialized:
                return None
            return {"confirmed_flush_lsn": row[0], "confirmed_flush_lsn_u64": confirmed,
                    "materialized_lsn": materialized, "pending_transactions": 0,
                    "matched_materialized": True}

        return self.until("PostgreSQL did not acknowledge the exact materialized prefix", aligned)

    def compacted_reader_baseline(self, previous):
        workload_compactor = self.stop_compactor()
        assert workload_compactor is None or (
            not workload_compactor["forced_kill"] and workload_compactor["returncode"] == 0
        ), "workload compactor did not stop gracefully at the reader-baseline boundary"
        initial_inventory = self.inventory()
        ready_tables = {
            name for name, item in initial_inventory.items()
            if item["delete_files"] == 0 and item["data_files"] == 1
        }
        checked_snapshots = {
            name: item["current_snapshot_id"] for name, item in initial_inventory.items()
        }
        self.start_compactor(
            self.args.reader_baseline_compactor,
            max_input_files=self.args.reader_baseline_max_input_files,
            max_input_bytes=self.args.reader_baseline_max_input_bytes,
        )

        def ready():
            changed_without_external_marker = False
            for name in self.names:
                if name in ready_tables:
                    continue
                table = self.table(name)
                metadata = table["metadata"]
                snapshot_id = metadata["current-snapshot-id"]
                snapshot = next(item for item in metadata["snapshots"]
                                if item["snapshot-id"] == snapshot_id)
                if "local-test.compaction-id" in snapshot["summary"]:
                    ready_tables.add(name)
                elif checked_snapshots.get(name) != snapshot_id:
                    checked_snapshots[name] = snapshot_id
                    changed_without_external_marker = True
            # A daemon rewrite can win the race and leave an already compact
            # layout without the fixture marker. Inspect manifests only when a
            # catalog head changes, rather than on every readiness poll.
            if changed_without_external_marker:
                for name, item in self.inventory().items():
                    if item["delete_files"] == 0 and item["data_files"] == 1:
                        ready_tables.add(name)
            return True if len(ready_tables) == len(self.names) else None

        self.until(
            "final-state compactor did not produce a compacted external snapshot", ready,
            timeout=self.args.reader_baseline_timeout)
        stopped_compactor = self.stop_compactor()
        assert stopped_compactor is not None and not stopped_compactor["forced_kill"] \
            and stopped_compactor["returncode"] == 0, \
            "reader-baseline compactor did not stop gracefully"
        compacted = {name: self.table(name) for name in self.names}
        inventory = self.inventory(compacted)
        assert all(
            item["delete_files"] == 0
            and (item["data_files"] == 1 or item["current_external_compaction"])
            for item in inventory.values()
        ), "final reader baseline is not compacted"

        expected = {}
        not_required = {}
        for name in self.names:
            metadata = compacted[name]["metadata"]
            snapshot_id = int(metadata["current-snapshot-id"])
            snapshot = next(item for item in metadata["snapshots"]
                            if item["snapshot-id"] == snapshot_id)
            identity = (int(self.oids[name]), snapshot_id)
            if "local-test.compaction-id" in snapshot["summary"]:
                expected[identity] = name
            else:
                not_required[name] = {"table_id": identity[0], "snapshot_id": identity[1],
                                      "reason": "already_compact"}
        acknowledged = {}
        cursor = 0
        daemon_log = self.directory / f"daemon-{self.generation}.log"

        def reconciled():
            nonlocal cursor
            cursor, observed = scan_reconciliation_events(daemon_log, cursor, expected)
            acknowledged.update(observed)
            if acknowledged.keys() != expected.keys():
                return None
            return {name: acknowledged[identity] for identity, name in expected.items()}

        reconciliation = (self.until(
            "daemon did not durably reconcile every compacted external snapshot", reconciled,
            timeout=self.args.reader_baseline_timeout)
            if expected else {})
        verified = self.verify(compacted)
        self.alive()
        daemon_stop = self.stop()
        assert daemon_stop is not None and not daemon_stop["forced_kill"] \
            and daemon_stop["returncode"] == 0, "daemon did not stop gracefully"
        baseline = {"inventory": inventory, "same_final_rows": verified,
                    "metadata_locations": {name: compacted[name]["metadata-location"] for name in self.names},
                    "external_reconciliation": {
                        "event": "external_snapshot_reconciled",
                        "acknowledged_after_durable_index_apply": reconciliation,
                        "reconciliation_not_required": not_required,
                    },
                    "quiescence": {"daemon": daemon_stop,
                                   "workload_compactor": workload_compactor,
                                   "compactor": stopped_compactor}}
        self.report["verified_reader_baseline"] = baseline
        self.check_worker_panics()
        write_report(self.directory / "report.json", self.report)
        comparison = compare_snapshots(
            self.duck,
            {name: previous[name]["metadata_location"] for name in self.names},
            baseline["metadata_locations"],
        )
        return {"inventory": inventory, "same_final_rows": verified,
                "external_reconciliation": baseline["external_reconciliation"], **comparison,
                "quiescence": baseline["quiescence"]}

    def execute(self):
        failure = None
        try:
            self.execute_phases()
        except BaseException as error:
            failure = error
            self.report["traceback"] = traceback.format_exc()
        finally:
            self.close_run(failure)

    def execute_phases(self):
        args = self.args
        self.report["measurement_protocol"] = {
            "version": "disk-evidence-paged-probes-v3",
            "probe_query": "one page of outstanding marker IDs per table/poll; ascending sequence with wrap; holes retained",
            "probe_page_rows": PAGE_ROWS, "recorder_queue_items": QUEUE_ITEMS,
            "sqlite_cache_mib_per_connection": 16, "sqlite_temp_store": "FILE", "sqlite_mmap_bytes": 0,
            "duckdb_memory_limit": "1GiB", "verification": "named PostgreSQL cursor plus sorted DuckDB CSV disk sink; 4096-row comparison",
            "key_storage": "8-byte disk slots per writer/table; same rank sampling and swap-last order",
            "writer_timing": "monotonic admission, key preparation, recorder enqueue, PostgreSQL transaction and causal predecessor postcommit key-file timings; microseconds",
            "postcommit_attribution": "previous_postcommit_keys_micros is work before the recorded arrival; a final value per writer is reported separately and is not part of that arrival cohort",
            "evidence": "evidence.sqlite3; existing JSON arrays exported as streams",
            "scope": "bounded Python collections and engine-managed spill budgets, not an RSS guarantee for native libraries or catalog metadata",
            "comparison": "writer timing instrumentation changes producer and recorder overhead and requires a fresh v3 matched control; previous reports are unchanged",
        }
        root = Path(__file__).resolve().parents[2]
        self.build_manifest = load_build_manifest(args.build_manifest)
        daemon_binary = verified_binary(self.build_manifest, "daemon", args.binary)
        workload_compactor = (verified_binary(self.build_manifest, "external_compactor", args.compactor_binary)
                              if args.compactor_binary else None)
        baseline_compactor = (verified_binary(self.build_manifest, "external_compactor", args.reader_baseline_compactor)
                              if args.reader_baseline_compactor else None)
        repository = repository_identity()
        self.report["artifact_identity"] = {
            "schema": 2,
            "repository": repository,
            "build_manifest": self.build_manifest,
            "harness_sha256": {
                str(path.relative_to(root)): sha256_file(path)
                for path in (Path(__file__).resolve(), root / "tests/production/benchmark_store.py",
                             root / "tests/production/process_lifecycle.py",
                             root / "tests/production/reader_compare.py",
                             root / "tests/production/provenance.py",
                             root / "tests/production/reconciliation.py",
                             root / "tests/local/run.py", root / "tests/local/duckdb_extensions.py")
            },
            "binaries": {
                "daemon": daemon_binary,
                "external_during_workload": workload_compactor,
                "same_final_state_external": baseline_compactor,
            },
            "rustc_observed_at_run": subprocess.check_output(["rustc", "-Vv"], text=True).strip(),
            "config": {"path": str(self.config), "sha256": sha256_file(self.config)},
            "machine": machine_identity(),
            "compaction": {
                "daemon_during_workload": args.maintenance_mode != "disabled",
                "external_during_workload": bool(args.compactor_binary),
                "same_final_state_external": bool(args.reader_baseline_compactor),
                "interval_ms": args.compactor_interval_ms,
                "same_final_state_budget": {
                    "max_input_files": args.reader_baseline_max_input_files,
                    "max_input_bytes": args.reader_baseline_max_input_bytes,
                    "timeout_seconds": args.reader_baseline_timeout,
                } if args.reader_baseline_compactor else None,
            },
        }
        self.report.update(format_version=args.format_version, profile=args.profile, seed=args.seed, initial_rows_per_table=args.initial_rows,
                           tables=args.tables, payload_bytes=args.row_bytes, warmup_seconds=args.warmup,
                           measured_seconds=args.duration, transaction_rows=args.transaction_rows, writers=args.writers,
                           key_distribution=args.key_distribution, reader_poll_seconds=args.reader_poll_seconds,
                           batch_rows=args.batch_rows, pending_transactions=args.pending_transactions,
                           maintenance_mode=args.maintenance_mode,
                           manifest_max_count=args.manifest_max_count,
                           payload=("distinct MD5 blocks per row and generation; hex text (4 bits per byte), PostgreSQL generation CPU included"
                                    if args.payload_entropy == "high" else "distinct-per-row repeated MD5 text; compressible, not random-byte throughput"),
                           compactor=("external background process" if args.compactor_binary else
                                      "daemon background role" if args.maintenance_mode != "disabled" else "disabled"),
                           build=str(args.binary.resolve()), platform=subprocess.check_output(["uname", "-a"], text=True).strip())
        self.report["maintenance_policy_contract"] = {
            "mode": args.maintenance_mode,
            "daemon_compactor_enabled": args.maintenance_mode != "disabled",
            "data_rewrite_scope": {
                "current": "all", "l0-only": "l0-only",
                "delete-only": "disabled", "disabled": "disabled",
            }[args.maintenance_mode],
            "delete_rewrites_enabled": args.maintenance_mode != "disabled",
            "data_debt_bounds_relaxed_for_finite_diagnostic": args.maintenance_mode in ("disabled", "delete-only"),
            "stable_small_file_bounds_relaxed_for_finite_diagnostic": args.maintenance_mode != "current",
            "stable_small_file_bounds": {
                "soft": args.stable_small_soft_files,
                "hard": args.stable_small_hard_files,
                "source": "explicit" if args.stable_small_soft_files is not None else "binary_default",
            },
            "delete_debt_bounds_relaxed_for_finite_diagnostic": args.maintenance_mode == "disabled",
            "manifest_max_count": args.manifest_max_count,
        }
        if args.key_distribution == "zipf":
            self.report["key_distribution_contract"] = {
                "sampler": "finite-zipf-bucket-rejection-v1", "exponent": args.zipf_exponent,
                "weights": "rank**(-exponent), successive draws without replacement within each transaction",
                "rank_population": "1-based current active-key vector for this writer and table; delete swaps in the last key, inserts append",
                "reproducibility": "Random(seed + worker), conditional on the same admitted transaction sequence",
                "scope": "independent writer/table populations; no shared-key writer contention",
                "proposal_limit": "max(1024, 128 * (updates + deletes)); exhaustion fails the run without a fallback distribution",
            }
        self.phase("seed", self.seed)
        self.phase("initial-copy-and-full-row-check", self.initialize)
        self.phase("baseline-reader", self.scans)
        self.start()
        self.start_compactor()
        self.phase("open-loop-workload", self.workload)
        self.phase("post-drain-source-acknowledgement", self.source_acknowledgement)
        self.phase("full-source-target-verification", self.verify)
        self.phase("post-workload-inventory", self.inventory)
        self.phase("post-workload-reader", self.scans)
        self.phase("resource-and-cost-observations", self.resource_summary)
        if args.reader_baseline_compactor:
            previous = next(phase["details"] for phase in self.report["phases"]
                            if phase["name"] == "post-workload-reader")
            self.phase("same-final-state-compacted-reader", lambda: self.compacted_reader_baseline(previous))
        performance = next(phase["details"] for phase in self.report["phases"] if phase["name"] == "open-loop-workload")
        self.report["correctness_passed"] = performance["all_committed_transactions_observed"]
        self.report["performance_qualified"] = performance["target_arrival_rate_sustained"] and performance["catalog_latency_slo_met"]
        self.report["reader_scan_qualified"] = None
        if args.reader_baseline_compactor:
            comparison = next(phase["details"] for phase in self.report["phases"]
                              if phase["name"] == "same-final-state-compacted-reader")
            self.report["reader_scan_qualified"] = comparison["reader_scan_qualified"]
            self.report["performance_qualified"] &= self.report["reader_scan_qualified"]
        workload_compactor_stop = self.stop_compactor()
        if workload_compactor_stop is not None:
            assert not workload_compactor_stop["forced_kill"] \
                and workload_compactor_stop["returncode"] == 0, \
                "workload compactor did not stop gracefully"
            self.report["workload_compactor_shutdown"] = workload_compactor_stop
        daemon_stop = self.stop()
        if daemon_stop is not None:
            assert not daemon_stop["forced_kill"] and daemon_stop["returncode"] == 0, \
                "daemon did not stop gracefully"
        self.check_worker_panics()
        self.report["passed"] = self.report["correctness_passed"] and self.report["performance_qualified"]
        if not self.report["passed"]:
            raise AssertionError("correctness or performance qualification failed; see report")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--postgres-url", default=os.environ.get("FLOW_POSTGRES_URL"))
    parser.add_argument("--catalog-uri", required=True)
    parser.add_argument("--s3-endpoint", required=True)
    parser.add_argument("--warehouse", default="s3://warehouse/")
    parser.add_argument("--binary", type=Path, default=Path("target/release/embrasure-flow"))
    parser.add_argument("--format-version", type=int, choices=(2, 3), default=2)
    parser.add_argument("--build-manifest", type=Path, required=True)
    parser.add_argument("--compactor-binary", type=Path)
    parser.add_argument("--reader-baseline-compactor", type=Path)
    parser.add_argument("--reader-baseline-max-input-files", type=int, default=256)
    parser.add_argument("--reader-baseline-max-input-bytes", type=int, default=512 << 20)
    parser.add_argument("--reader-baseline-timeout", type=float, default=180)
    parser.add_argument("--compactor-interval-ms", type=int, default=3000)
    parser.add_argument("--maintenance-mode", choices=("disabled", "delete-only", "l0-only", "current"),
                        default="current")
    parser.add_argument("--stable-small-soft-files", type=int)
    parser.add_argument("--stable-small-hard-files", type=int)
    parser.add_argument("--manifest-max-count", type=int, default=64)
    parser.add_argument("--artifacts", type=Path, required=True)
    parser.add_argument("--timeout", type=float, default=180)
    parser.add_argument("--setup-timeout", type=float, default=600,
                        help="finite setup/verification allowance, in addition to workload and drain budgets")
    parser.add_argument("--supervised-worker", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--duration", type=float, default=60)
    parser.add_argument("--warmup", type=float, default=10)
    parser.add_argument("--rate", type=int, default=1000)
    parser.add_argument("--transaction-rows", type=int, default=100)
    parser.add_argument("--initial-rows", type=int, default=100000)
    parser.add_argument("--row-bytes", type=int, choices=(256, 1024, 16384), default=256)
    parser.add_argument("--payload-entropy", choices=("repeated", "high"), default="repeated")
    parser.add_argument("--tables", type=int, default=1)
    parser.add_argument("--writers", type=int, default=4)
    parser.add_argument("--table-workers", type=int, default=4,
                        help="daemon table actors; independent of source writer connections")
    parser.add_argument("--seed", type=int, default=20260905)
    parser.add_argument("--profile", choices=("mixed", "append", "update90"), default="mixed")
    parser.add_argument("--key-distribution", choices=("uniform", "hot", "zipf"), default="uniform")
    parser.add_argument("--zipf-exponent", type=float, default=.99,
                        help="finite Zipf rank exponent (0 < s <= 2); used only with --key-distribution zipf")
    parser.add_argument("--max-start-delay-ms", type=float, default=250)
    parser.add_argument("--reader-poll-seconds", type=float, default=1)
    parser.add_argument("--batch-rows", type=int, default=1024)
    parser.add_argument("--pending-transactions", type=int, default=256)
    parser.add_argument("--max-clock-uncertainty-ms", type=float, default=50)
    parser.add_argument("--catalog-p95-ms", type=float, default=1000)
    parser.add_argument("--catalog-p99-ms", type=float, default=2000)
    args = parser.parse_args()
    if not args.postgres_url:
        parser.error("provide --postgres-url or FLOW_POSTGRES_URL")
    if min(args.duration, args.rate, args.tables, args.writers, args.timeout, args.reader_poll_seconds) <= 0 or args.warmup < 0:
        parser.error("duration, rate, tables, writers, timeout and polling must be positive; warmup must be nonnegative")
    if args.transaction_rows < 1 or (args.profile != "append" and args.transaction_rows % 10):
        parser.error("transaction rows must be positive; mixed and update90 require a multiple of ten")
    if args.initial_rows // args.writers < args.transaction_rows:
        parser.error("initial rows per writer must exceed transaction rows")
    if args.duration * args.rate < args.transaction_rows:
        parser.error("measurement window must schedule at least one complete transaction")
    if (args.pending_transactions <= 0 or args.table_workers <= 0 or args.manifest_max_count < 2
            or args.reader_baseline_max_input_files <= 0
            or args.reader_baseline_max_input_bytes <= 0
            or args.reader_baseline_timeout <= 0):
        parser.error("pending transactions, table workers, reader-baseline budgets and timeouts must be positive; "
                     "manifest max count must be at least two")
    if (args.stable_small_soft_files is None) != (args.stable_small_hard_files is None):
        parser.error("stable-small soft and hard limits must be supplied together")
    if args.stable_small_soft_files is not None:
        if args.maintenance_mode != "current":
            parser.error("stable-small overrides require --maintenance-mode current")
        if args.stable_small_soft_files <= 0 or args.stable_small_soft_files > args.stable_small_hard_files:
            parser.error("stable-small limits must be positive with soft at most hard")
    if args.key_distribution == "zipf" and (not math.isfinite(args.zipf_exponent) or not 0 < args.zipf_exponent <= 2):
        parser.error("Zipf exponent must be finite and greater than zero, at most two")
    if not all(math.isfinite(value) and value > 0 for value in (
            args.setup_timeout, args.timeout, args.duration, args.reader_baseline_timeout,
            args.max_clock_uncertainty_ms, args.catalog_p95_ms, args.catalog_p99_ms)) or not math.isfinite(args.warmup):
        parser.error("time and clock uncertainty budgets must be finite and positive")
    if not args.supervised_worker:
        timeout = args.setup_timeout + args.warmup + args.duration + 2 * args.timeout
        if args.reader_baseline_compactor:
            timeout += 2 * args.reader_baseline_timeout
        completed = run_supervised([sys.executable, str(Path(__file__).resolve()), *sys.argv[1:],
                                    "--supervised-worker"], args.artifacts, timeout)
        raise SystemExit(completed.returncode)
    Benchmark(args).execute()
    print(f"PASS: {args.artifacts.resolve() / 'report.json'}", flush=True)


if __name__ == "__main__":
    main()
