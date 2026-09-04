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
from benchmark_store import BenchmarkStore, DenseKeys, LATENCIES, PAGE_ROWS, QUEUE_ITEMS, bounded_lines, zipf_sample

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "local"))
from run import Run, dump  # noqa: E402


def micros():
    return time.time_ns() // 1000




class Benchmark(Run):
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
[limits]
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
        self.environment = os.environ | {"FLOW_LOCAL_POSTGRES_URL": self.args.postgres_url, "RUST_LOG": "info"}
        self.store = BenchmarkStore(self.directory / "evidence.sqlite3")

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
        with (self.directory / "init.log").open("wb") as log:
            subprocess.run(self.command("init"), env=self.environment, stdout=log,
                           stderr=subprocess.STDOUT, check=True, timeout=self.args.timeout)
        metrics = self.directory / "state" / "metrics.prom"
        if metrics.exists():
            (self.directory / "init-metrics.prom").write_bytes(metrics.read_bytes())
        return self.verify()

    def verify(self, pinned_tables=None):
        result = {}
        for name in self.names:
            query = sql.SQL("SELECT id,version,md5(payload) FROM {} ORDER BY id").format(sql.Identifier(name))
            metadata = self.table(name) if pinned_tables is None else pinned_tables[name]
            # COPY is a disk sink; fetchmany on an ordinary result does not bound
            # libpq's result buffer or DuckDB's materialized Python result.
            path = self.directory / "verification-rows.csv"
            destination = "'" + str(path).replace("'", "''") + "'"
            self.duck.execute("COPY (SELECT id,version,md5(payload) FROM iceberg_scan(?) ORDER BY id) TO " +
                              destination + " (FORMAT CSV, HEADER false)", [metadata["metadata-location"]])
            count = 0
            digest = hashlib.sha256()
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
            finally:
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

    def start_compactor(self, binary=None):
        binary = binary or self.args.compactor_binary
        if binary and self.compactor is None:
            self.compactor_log = (self.directory / "compactor.log").open("wb")
            self.compactor = subprocess.Popen([str(binary.resolve()), "--config", str(self.config),
                                              "--interval-ms", str(self.args.compactor_interval_ms)], env=self.environment,
                                             stdout=self.compactor_log, stderr=subprocess.STDOUT)

    def stop_compactor(self):
        if self.compactor is None:
            return None
        process = self.compactor
        if process.poll() is None:
            process.send_signal(signal.SIGINT)
            try:
                process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=10)
        self.compactor = None
        self.compactor_log.close()
        return {"pid": process.pid, "returncode": process.returncode}

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
                    record = {"sequence": sequence, "scheduled_at_micros": start_us + int(sequence * interval * 1e6),
                              "measured": sequence * interval >= args.warmup, "table": self.names[(sequence // args.writers) % args.tables]}
                    if time.monotonic() - due > args.max_start_delay_ms / 1000:
                        record["missed_arrival"] = True
                        self.store.record(record)
                        continue
                    name, marker = record["table"], -(sequence + 1)
                    active = keys[name]
                    updates = int(args.transaction_rows * {"mixed": .7, "update90": .9, "append": 0}[args.profile])
                    deletes = int(args.transaction_rows * .1) if args.profile == "mixed" else 0
                    inserts = args.transaction_rows - updates - deletes
                    # One ordinary insertion per tx is reserved as an immutable visibility probe.
                    if args.key_distribution == "zipf":
                        selected = zipf_sample(rng, range(len(active)), updates + deletes, args.zipf_exponent)
                    else:
                        population = max(updates + deletes, len(active) // 100) if args.key_distribution == "hot" else len(active)
                        selected = rng.sample(range(population), updates + deletes)
                    chosen = [active[index] for index in selected]
                    update_keys, delete_keys = chosen[:updates], chosen[updates:]
                    new_keys = [args.initial_rows + 1 + sequence * args.transaction_rows + i for i in range(inserts - 1)]
                    record["started_at_micros"] = micros()
                    self.store.record(record, marker)
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
                    record.update(commit_response_at_micros=micros(), mutations=args.transaction_rows,
                                  update_rows=updates, delete_rows=deletes, insert_rows=inserts, marker=marker)
                    self.store.record(record, marker)
                    active.remove_selected(selected[updates:])
                    active.extend(new_keys)
        except BaseException:
            self.errors.append(traceback.format_exc())
            if record is not None:
                try:
                    self.store.record(record, -(record["sequence"] + 1) if not record.get("missed_arrival") else None)
                except BaseException:
                    self.errors.append(traceback.format_exc())
        finally:
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
            start_mono, start_us = time.monotonic() + .5, micros() + 500000
            self.measured_start_us = start_us + int(args.warmup * 1e6)
            self.measured_end_us = self.measured_start_us + int(args.duration * 1e6)
            threads = [threading.Thread(target=self.writer, args=(worker, start_mono, start_us, scheduled)) for worker in range(args.writers)]
            for thread in threads:
                thread.start()
            cursors = {}
            next_sample = time.monotonic()
            while any(thread.is_alive() for thread in threads):
                self.alive()
                self.observe(cursors)
                if time.monotonic() >= next_sample:
                    self.sample_resources()
                    next_sample = time.monotonic() + 1
                time.sleep(args.reader_poll_seconds)
            self.alive()
            deadline = time.monotonic() + args.timeout
            while time.monotonic() < deadline:
                self.alive()
                self.observe(cursors)
                self.sample_resources()
                if self.store.all_committed_observed():
                    break
                time.sleep(args.reader_poll_seconds)
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
            # Cancellation requests do not release a worker's files/recorder.
            # The owned connection must finish or fail, and every thread joins.
            for thread in threads:
                thread.join()
            for pools in self.key_pools:
                for keys in pools.values():
                    keys.close()
            if not finished:
                self.store.export(self.directory)
        try:
            self.report["clock"]["after"] = self.calibrate_clock()
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
        offset = (clock["before"]["postgres_minus_client_us"] + clock["after"]["postgres_minus_client_us"]) / 2
        clock["correction_us"] = offset
        clock["offset_change_us"] = clock["after"]["postgres_minus_client_us"] - clock["before"]["postgres_minus_client_us"]
        self.store.analyze(self.oids, offset)
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
        window_rows = db.execute("SELECT coalesce(sum(mutations),0) FROM transactions "
                                 "WHERE commit_response_at_micros>=? AND commit_response_at_micros<?",
                                 (self.measured_start_us, self.measured_end_us)).fetchone()[0]
        percentiles = {name: self.store.distribution(name) for name in LATENCIES}
        requested = self.store.distribution("scheduled_to_reader", summary["scheduled"])
        unobserved = db.execute("SELECT count(*) FROM transactions WHERE marker IS NOT NULL "
                                "AND reader_observed_at_micros IS NULL").fetchone()[0]
        observations_complete = (bool(summary["completed"]) and not missing and not unobserved and
                                 summary["completed"] == percentiles["commit_to_reader"]["count"])
        catalog_delay = catalog_rate = None
        if observations_complete:
            catalog_end = max(self.measured_end_us, summary["catalog_end"])
            catalog_delay = (catalog_end - self.measured_end_us) / 1e6
            catalog_rate = totals["mutations"] / ((catalog_end - self.measured_start_us) / 1e6)
        return {"scheduled_transactions": summary["scheduled"], "committed_transactions": summary["completed"],
                "missed_scheduled_arrivals": summary["missed"], "missing_observations": missing, "affected_rows": totals,
                "unobserved_committed_transactions_including_warmup": unobserved,
                "requested_mutations_per_second": self.args.rate,
                "source_commit_window_mutations_per_second": window_rows / self.args.duration,
                "catalog_completion_delay_after_measurement_seconds": catalog_delay,
                "measured_cohort_catalog_mutations_per_second_including_drain": catalog_rate,
                "scheduled_cohort_mutations_per_second": totals["mutations"] / self.args.duration,
                "commits_after_measurement_window": summary["late_commits"],
                "latency": percentiles,
                "catalog_latency_slo_met": (percentiles["commit_to_catalog"].get("p95_ms", float("inf")) < self.args.catalog_p95_ms and
                                            percentiles["commit_to_catalog"].get("p99_ms", float("inf")) < self.args.catalog_p99_ms),
                "catalog_latency_slo_ms": {"p95": self.args.catalog_p95_ms, "p99": self.args.catalog_p99_ms},
                "scheduled_to_reader_including_failures": requested,
                "all_committed_transactions_observed": observations_complete,
                "target_arrival_rate_sustained": summary["missed"] == 0 and window_rows / self.args.duration >= self.args.rate * .95}

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

    def inventory(self):
        result = {}
        for name in self.names:
            metadata = self.table(name)["metadata"]
            current = next(snap for snap in metadata["snapshots"] if snap["snapshot-id"] == metadata["current-snapshot-id"])
            counts = {"manifests": 0, "data_files": 0, "delete_files": 0, "data_bytes": 0}
            for manifest in self.avro(current["manifest-list"]):
                counts["manifests"] += 1
                for entry in self.avro(manifest["manifest_path"]):
                    if entry["status"] == 2:
                        continue
                    file = entry["data_file"]
                    if file["content"] == 0:
                        counts["data_files"] += 1
                        counts["data_bytes"] += file["file_size_in_bytes"]
                    elif file["content"] == 1:
                        counts["delete_files"] += 1
            result[name] = {**counts, "snapshots": len(metadata["snapshots"]),
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

    def compacted_reader_baseline(self, previous):
        self.start_compactor(self.args.reader_baseline_compactor)
        def ready():
            inventory = self.inventory()
            return inventory if all(item["delete_files"] == 0 and (item["data_files"] == 1 or item["current_external_compaction"])
                                    for item in inventory.values()) else None
        inventory = self.until("final-state compactor did not produce a compacted baseline", ready)
        compacted = {name: self.table(name) for name in self.names}
        verified = self.verify(compacted)
        self.alive()
        stopped_compactor = self.stop_compactor()
        daemon = self.process
        self.stop()
        baseline = {"inventory": inventory, "same_final_rows": verified,
                    "metadata_locations": {name: compacted[name]["metadata-location"] for name in self.names},
                    "quiescence": {"daemon": {"pid": daemon.pid, "returncode": daemon.returncode},
                                   "compactor": stopped_compactor}}
        self.report["verified_reader_baseline"] = baseline
        self.check_worker_panics()
        dump(self.directory / "report.json", self.report)
        assert stopped_compactor["returncode"] == 0, "reader-baseline compactor did not stop gracefully"
        comparison = compare_snapshots(
            self.duck,
            {name: previous[name]["metadata_location"] for name in self.names},
            baseline["metadata_locations"],
        )
        return {"inventory": inventory, "same_final_rows": verified, **comparison,
                "quiescence": baseline["quiescence"]}

    def execute(self):
        args = self.args
        self.report["measurement_protocol"] = {
            "version": "disk-evidence-paged-probes-v2",
            "probe_query": "one page of outstanding marker IDs per table/poll; ascending sequence with wrap; holes retained",
            "probe_page_rows": PAGE_ROWS, "recorder_queue_items": QUEUE_ITEMS,
            "sqlite_cache_mib_per_connection": 16, "sqlite_temp_store": "FILE", "sqlite_mmap_bytes": 0,
            "duckdb_memory_limit": "1GiB", "verification": "named PostgreSQL cursor plus sorted DuckDB CSV disk sink; 4096-row comparison",
            "key_storage": "8-byte disk slots per writer/table; same rank sampling and swap-last order",
            "evidence": "evidence.sqlite3; existing JSON arrays exported as streams",
            "scope": "bounded Python collections and engine-managed spill budgets, not an RSS guarantee for native libraries or catalog metadata",
            "comparison": "changed reader query and storage overhead require a fresh matched control; previous reports are unchanged",
        }
        self.report.update(profile=args.profile, seed=args.seed, initial_rows_per_table=args.initial_rows,
                           tables=args.tables, payload_bytes=args.row_bytes, warmup_seconds=args.warmup,
                           measured_seconds=args.duration, transaction_rows=args.transaction_rows, writers=args.writers,
                           key_distribution=args.key_distribution, reader_poll_seconds=args.reader_poll_seconds,
                           batch_rows=args.batch_rows, pending_transactions=args.pending_transactions,
                           payload=("distinct MD5 blocks per row and generation; hex text (4 bits per byte), PostgreSQL generation CPU included"
                                    if args.payload_entropy == "high" else "distinct-per-row repeated MD5 text; compressible, not random-byte throughput"),
                           compactor="external background process" if args.compactor_binary else "daemon background role",
                           build=str(args.binary.resolve()), platform=subprocess.check_output(["uname", "-a"], text=True).strip())
        if args.key_distribution == "zipf":
            self.report["key_distribution_contract"] = {
                "sampler": "finite-zipf-bucket-rejection-v1", "exponent": args.zipf_exponent,
                "weights": "rank**(-exponent), successive draws without replacement within each transaction",
                "rank_population": "1-based current active-key vector for this writer and table; delete swaps in the last key, inserts append",
                "reproducibility": "Random(seed + worker), conditional on the same admitted transaction sequence",
                "scope": "independent writer/table populations; no shared-key writer contention",
                "proposal_limit": "max(1024, 128 * (updates + deletes)); exhaustion fails the run without a fallback distribution",
            }
        try:
            self.report["clock"] = {"before": self.calibrate_clock(),
                                    "method": "minimum RTT of five server clock queries before and after; average offset correction; raw negative values retained"}
            self.phase("seed", self.seed)
            self.phase("initial-copy-and-full-row-check", self.initialize)
            self.phase("baseline-reader", self.scans)
            self.start()
            self.start_compactor()
            self.phase("open-loop-workload", self.workload)
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
            self.stop()
            self.stop_compactor()
            self.check_worker_panics()
            self.report["passed"] = self.report["correctness_passed"] and self.report["performance_qualified"]
            if not self.report["passed"]:
                raise AssertionError("correctness or performance qualification failed; see report")
        except BaseException as error:
            self.report.update(failure=str(error), traceback=traceback.format_exc())
            raise
        finally:
            self.stop()
            self.stop_compactor()
            self.report["state_bytes"] = {name: sum(path.stat().st_size for path in (self.directory / "state" / name).rglob("*") if path.is_file())
                                          for name in ("index", "journal", "spool")}
            try:
                self.store.close()
            except BaseException as error:
                self.report.update(passed=False, storage_close_failure=str(error))
                raise
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
    parser.add_argument("--binary", type=Path, default=Path("target/release/embrasure-flow"))
    parser.add_argument("--compactor-binary", type=Path)
    parser.add_argument("--reader-baseline-compactor", type=Path)
    parser.add_argument("--compactor-interval-ms", type=int, default=3000)
    parser.add_argument("--artifacts", type=Path, required=True)
    parser.add_argument("--timeout", type=float, default=180)
    parser.add_argument("--duration", type=float, default=60)
    parser.add_argument("--warmup", type=float, default=10)
    parser.add_argument("--rate", type=int, default=1000)
    parser.add_argument("--transaction-rows", type=int, default=100)
    parser.add_argument("--initial-rows", type=int, default=100000)
    parser.add_argument("--row-bytes", type=int, choices=(256, 1024, 16384), default=256)
    parser.add_argument("--payload-entropy", choices=("repeated", "high"), default="repeated")
    parser.add_argument("--tables", type=int, default=1)
    parser.add_argument("--writers", type=int, default=4)
    parser.add_argument("--seed", type=int, default=20260905)
    parser.add_argument("--profile", choices=("mixed", "append", "update90"), default="mixed")
    parser.add_argument("--key-distribution", choices=("uniform", "hot", "zipf"), default="uniform")
    parser.add_argument("--zipf-exponent", type=float, default=.99,
                        help="finite Zipf rank exponent (0 < s <= 2); used only with --key-distribution zipf")
    parser.add_argument("--max-start-delay-ms", type=float, default=250)
    parser.add_argument("--reader-poll-seconds", type=float, default=1)
    parser.add_argument("--batch-rows", type=int, default=1024)
    parser.add_argument("--pending-transactions", type=int, default=256)
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
    if args.pending_transactions <= 0:
        parser.error("pending transactions must be positive")
    if args.key_distribution == "zipf" and (not math.isfinite(args.zipf_exponent) or not 0 < args.zipf_exponent <= 2):
        parser.error("Zipf exponent must be finite and greater than zero, at most two")
    Benchmark(args).execute()
    print(f"PASS: {args.artifacts.resolve() / 'report.json'}", flush=True)


if __name__ == "__main__":
    main()
