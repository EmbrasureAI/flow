#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["duckdb==1.5.5", "psycopg[binary]==3.3.5", "boto3==1.43.88", "fastavro==1.12.2"]
# ///
"""Source-generator capacity only: the benchmark's exact mixed PostgreSQL writer."""

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import sys
import threading
import time
import traceback
import uuid

import psycopg
from psycopg import sql

from benchmark import Benchmark, micros
from benchmark_store import BenchmarkStore, DenseKeys, PAGE_ROWS, write_array, mutation_counts
from process_lifecycle import arm_workload, disarm_workload, run_supervised, write_report
from provenance import machine_identity, repository_identity, sha256_file


class SourceCapacity(Benchmark):
    def __init__(self, args):
        try:
            self.initialize_source(args)
        except BaseException as error:
            if hasattr(self, "report"):
                self.close_run(error)
            raise

    def initialize_source(self, args):
        # Deliberately bypass Run's daemon, object store and DuckDB initialization.
        self.args = args
        self.directory = args.artifacts.resolve()
        self.directory.mkdir(parents=True, exist_ok=False)
        self.report = {"phases": [], "passed": False}
        self.name = "flow_source_" + uuid.uuid4().hex[:12]
        self.names = ["events_0"]
        self.errors = []
        self.stop_writers = threading.Event()
        self.writer_connections = [None] * args.writers
        self.writer_lock = threading.Lock()
        self.key_pools = [{} for _ in range(args.writers)]
        self.threads = []
        self.pg = psycopg.connect(args.postgres_url, autocommit=True, connect_timeout=10)
        self.pg.execute("SET timezone = 'UTC'")
        self.pg.execute(sql.SQL("SET search_path TO {}, public").format(sql.Identifier(self.name)))
        self.pg.execute("SET statement_timeout = '60s'")
        self.report = {
            "scope": "source-generator capacity only; no ingestion qualification",
            "run": self.name, "phases": [], "passed": False,
            "source_correctness_passed": False, "source_capacity_met": False,
            "workload": {k: v for k, v in vars(args).items() if k not in ("postgres_url", "artifacts")},
            "machine": machine_identity(), "source_code": repository_identity(),
            "harness_sha256": self.harness_hashes(),
            "postgres": self.postgres_identity(),
        }

        self.store = BenchmarkStore(self.directory / "evidence.sqlite3")

    def postgres_identity(self):
        with self.pg.cursor(row_factory=psycopg.rows.dict_row) as cursor:
            cursor.execute("""
                SELECT current_database() AS database, version() AS version,
                    (pg_control_system()).system_identifier::text AS system_identifier,
                    pg_postmaster_start_time()::text AS postmaster_started_at,
                    current_setting('fsync') AS fsync,
                    current_setting('full_page_writes') AS full_page_writes,
                    current_setting('synchronous_commit') AS synchronous_commit,
                    current_setting('wal_level') AS wal_level,
                    current_setting('max_connections') AS max_connections
            """)
            return dict(cursor.fetchone())

    @staticmethod
    def harness_hashes():
        directory = Path(__file__).resolve().parent
        return {name: sha256_file(directory / name) for name in (
            "source_capacity.py", "benchmark.py", "benchmark_store.py", "process_lifecycle.py", "provenance.py", "../local/run.py")}

    def source_workload(self):
        args = self.args
        interval = args.transaction_rows / args.rate
        scheduled = math.floor((args.duration + args.warmup) * args.rate / args.transaction_rows)
        warmup = min(scheduled, math.ceil(args.warmup / interval))
        while warmup and (warmup - 1) * interval >= args.warmup:
            warmup -= 1
        while warmup < scheduled and warmup * interval < args.warmup:
            warmup += 1
        self.planned_arrivals = {"warmup": warmup, "measured": scheduled - warmup, "all": scheduled}
        for worker, pools in enumerate(self.key_pools):
            pools[self.names[0]] = DenseKeys(self.directory / f"keys-{worker}.bin",
                                             range(worker + 1, args.initial_rows + 1, args.writers))
        start_mono, start_us = time.monotonic() + .5, micros() + 500000
        self.measured_start_us = start_us + int(args.warmup * 1e6)
        self.measured_end_us = self.measured_start_us + int(args.duration * 1e6)
        self.threads = [threading.Thread(target=self.writer, args=(worker, start_mono, start_us, scheduled))
                        for worker in range(args.writers)]
        deadline = start_mono + args.warmup + args.duration + args.timeout
        write_report(self.directory / "report.json", self.report)
        arm_workload(self.directory, deadline)
        failure = None
        try:
            for thread in self.threads:
                thread.start()
            while any(thread.is_alive() for thread in self.threads):
                self.store.check()
                if self.errors:
                    raise RuntimeError(self.errors[0])
                if time.monotonic() >= deadline:
                    raise TimeoutError("source writers exceeded the workload deadline")
                time.sleep(.1)
        except BaseException as error:
            failure = error
            raise
        finally:
            try:
                self.join_writers()
                disarm_workload(self.directory)
            except BaseException as error:
                if failure is None:
                    raise
                self.report.setdefault("cleanup_errors", []).append(str(error))
        self.store.flush()
        summary = self.source_summary()
        self.report["source_workload"] = summary
        if self.errors:
            raise RuntimeError(self.errors[0])
        for cohort in summary["arrival_cohorts"].values():
            if cohort["unrecorded"] or cohort["incomplete_commits"]:
                raise AssertionError(f"source evidence is incomplete: {cohort}")
        return summary

    def join_writers(self):
        self.stop_writers.set()
        with self.writer_lock:
            connections = [c for c in self.writer_connections if c is not None]
        for connection in connections:
            try:
                connection.cancel_safe(timeout=5)
            except psycopg.Error as error:
                self.report.setdefault("cleanup_errors", []).append(str(error))
        # The CLI supervisor enforces the armed workload deadline even if a
        # native socket call or cancellation never returns. Keep owned key files
        # and the recorder open until workers finish or that process is killed.
        for thread in self.threads:
            if thread.ident is not None:
                thread.join()

    def source_summary(self):
        self.store.flush()
        db = self.store.db
        cohorts = {}
        for name, predicate in (("measured", "measured=1"), ("warmup", "measured=0"), ("all", "1")):
            row = db.execute(f"""SELECT count(*) AS recorded,
                count(commit_response_at_micros) AS committed,
                coalesce(sum(missed_arrival=1),0) AS missed,
                coalesce(sum(commit_response_at_micros IS NULL AND missed_arrival IS NULL),0) AS incomplete_commits
                FROM transactions WHERE {predicate}""").fetchone()
            cohorts[name] = {"planned": self.planned_arrivals[name], **dict(row),
                             "unrecorded": self.planned_arrivals[name] - row["recorded"]}
        window_rows = db.execute("SELECT coalesce(sum(mutations),0) FROM transactions "
            "WHERE commit_response_at_micros>=? AND commit_response_at_micros<?",
            (self.measured_start_us, self.measured_end_us)).fetchone()[0]
        late = db.execute("SELECT count(*) FROM transactions WHERE measured=1 AND commit_response_at_micros>=?",
                          (self.measured_end_us,)).fetchone()[0]
        return {"arrival_cohorts": cohorts,
                "requested_mutations_per_second": self.args.rate,
                "source_commit_response_window_mutations_per_second": window_rows / self.args.duration,
                "measured_commits_after_window": late,
                "writer_timing": self.store.writer_timing_summary(),
                "clock": "client wall timestamps for commit responses; monotonic source admission and writer phase timings"}

    def verify_source(self):
        final_identity = self.postgres_identity()
        self.report["postgres_after"] = final_identity
        if final_identity != self.report["postgres"]:
            raise AssertionError("PostgreSQL incarnation or durability settings changed during the run")
        db = self.store.db
        committed, inserts, deletes = db.execute("""SELECT count(*),coalesce(sum(insert_rows),0),
            coalesce(sum(delete_rows),0) FROM transactions WHERE commit_response_at_micros IS NOT NULL""").fetchone()
        expected_rows = self.args.initial_rows + inserts - deletes
        actual_rows, markers = self.pg.execute("SELECT count(*),count(*) FILTER (WHERE id<0) FROM events_0").fetchone()
        if (actual_rows, markers) != (expected_rows, committed):
            raise AssertionError(f"source row/marker count differs: {(actual_rows, markers)} != {(expected_rows, committed)}")
        # Every committed probe must exist, including commits outside the measured window.
        cursor = db.execute("SELECT marker,sequence+1 FROM transactions WHERE commit_response_at_micros IS NOT NULL ORDER BY sequence")
        while batch := cursor.fetchmany(PAGE_ROWS):
            expected = sorted(tuple(row) for row in batch)
            actual = self.pg.execute("SELECT id,version FROM events_0 WHERE id=ANY(%s) ORDER BY id",
                                     ([row[0] for row in expected],)).fetchall()
            if actual != expected:
                raise AssertionError("a committed immutable source marker is absent or changed")
        digest = hashlib.sha256()
        count = 0
        payload = self.payload_expression("CASE WHEN version=0 THEN id::text ELSE id::text || ':' || version::text END")
        query = sql.SQL("SELECT id,version,md5(payload),payload={} FROM events_0 ORDER BY id").format(payload)
        with self.pg.transaction():
            self.pg.execute("SET TRANSACTION READ ONLY")
            self.pg.execute("SET LOCAL work_mem = '16MB'")
            with self.pg.cursor(name="source_capacity_rows") as source:
                source.execute(query, (math.ceil(self.args.row_bytes / 32), self.args.row_bytes))
                while batch := source.fetchmany(PAGE_ROWS):
                    sequences = set()
                    for key, version, _, valid_payload in batch:
                        if key == 0 or version < 0 or not valid_payload:
                            raise AssertionError(f"invalid source row semantics for key {key}")
                        if version:
                            sequences.add(version - 1)
                        if key < 0:
                            if version != -key:
                                raise AssertionError("immutable marker version changed")
                        else:
                            owner = (key - 1) % self.args.writers
                            if key > self.args.initial_rows:
                                origin, offset = divmod(key - self.args.initial_rows - 1, self.args.transaction_rows)
                                if offset >= mutation_counts(self.args.profile, self.args.transaction_rows)[2] - 1 or version <= origin:
                                    raise AssertionError("inserted key has invalid origin or version")
                                sequences.add(origin)
                                owner = origin % self.args.writers
                            if version and (version - 1) % self.args.writers != owner:
                                raise AssertionError("row version violates writer key ownership")
                    if sequences:
                        placeholders = ','.join('?' for _ in sequences)
                        known = {row[0] for row in db.execute(
                            f"SELECT sequence FROM transactions WHERE commit_response_at_micros IS NOT NULL AND sequence IN ({placeholders})",
                            tuple(sequences))}
                        if known != sequences:
                            raise AssertionError("source row references an uncommitted transaction")
                    for key, version, payload_hash, _ in batch:
                        digest.update(f"{key},{version},{payload_hash}\n".encode())
                    count += len(batch)
        if count != expected_rows:
            raise AssertionError("source changed during final verification")
        return {"rows": count, "committed_markers": committed, "sha256": digest.hexdigest(),
                "verification": "all rows: id/version ownership, committed origins, exact deterministic payload; all immutable markers"}

    def execute(self):
        failure = None

        def cleanup_failed(error):
            nonlocal failure
            self.report.setdefault("cleanup_errors", []).append(str(error))
            self.report["passed"] = False
            if failure is None:
                failure = error
                self.report["failure"] = str(error)

        try:
            self.phase("seed", self.seed)
            self.phase("source-only-workload", self.source_workload)
            self.phase("full-source-verification", self.verify_source)
            self.report["source_correctness_passed"] = True
            workload = self.report["source_workload"]
            self.report["source_capacity_met"] = (
                workload["arrival_cohorts"]["all"]["missed"] == 0
                and workload["source_commit_response_window_mutations_per_second"] >= self.args.rate * .95)
            self.report["passed"] = self.report["source_capacity_met"]
        except BaseException as error:
            failure = error
            self.report.update(failure=str(error), traceback=traceback.format_exc())
        finally:
            try:
                try:
                    self.join_writers()
                except BaseException as error:
                    cleanup_failed(error)
                for pools in self.key_pools:
                    for keys in pools.values():
                        try:
                            keys.close()
                        except BaseException as error:
                            cleanup_failed(error)
                try:
                    self.store.flush()
                    write_array(self.directory / "transactions.json", self.store.transactions())
                except BaseException as error:
                    cleanup_failed(error)
                finally:
                    # Export failure must not leave the recorder waiting forever.
                    try:
                        self.store.close()
                    except BaseException as error:
                        cleanup_failed(error)
                self.report["evidence_sha256"] = {path.name: sha256_file(path) for path in
                    [self.directory / "evidence.sqlite3", self.directory / "transactions.json", *self.directory.glob("keys-*.bin")]}
                if self.harness_hashes() != self.report["harness_sha256"]:
                    raise RuntimeError("source harness changed during the run")
            except BaseException as error:
                cleanup_failed(error)
            finally:
                try:
                    self.store.close()
                except BaseException as error:
                    cleanup_failed(error)
                try:
                    self.pg.close()
                except BaseException as error:
                    cleanup_failed(error)
                try:
                    write_report(self.directory / "report.json", self.report)
                except BaseException as error:
                    cleanup_failed(error)

        if failure is not None:
            raise failure
        return self.report["passed"]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--postgres-url", default=os.environ.get("FLOW_POSTGRES_URL"))
    parser.add_argument("--artifacts", type=Path, required=True)
    parser.add_argument("--writers", type=int, choices=(4, 8), default=4)
    parser.add_argument("--rate", type=int, default=50000)
    parser.add_argument("--duration", type=float, default=60)
    parser.add_argument("--warmup", type=float, default=10)
    parser.add_argument("--initial-rows", type=int, default=100000)
    parser.add_argument("--row-bytes", type=int, choices=(256, 1024, 16384), default=256)
    parser.add_argument("--max-start-delay-ms", type=float, default=250)
    parser.add_argument("--timeout", type=float, default=45)
    parser.add_argument("--setup-timeout", type=float, default=600,
                        help="finite setup/verification allowance in addition to workload budget")
    parser.add_argument("--supervised-worker", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--seed", type=int, default=20260905)
    parser.set_defaults(tables=1, transaction_rows=100, profile="mixed", key_distribution="uniform", payload_entropy="repeated")
    args = parser.parse_args()
    if not args.postgres_url:
        parser.error("provide --postgres-url or FLOW_POSTGRES_URL")
    if (args.rate <= 0 or args.duration <= 0 or args.warmup < 0 or args.timeout <= 0
            or args.max_start_delay_ms <= 0
            or args.initial_rows // args.writers < args.transaction_rows * .8
            or not all(math.isfinite(v) for v in (args.duration, args.warmup, args.timeout, args.setup_timeout, args.max_start_delay_ms))):
        parser.error("positive finite workload limits and enough owned keys are required")
    if args.setup_timeout <= 0:
        parser.error("setup timeout must be positive")
    if not args.supervised_worker:
        completed = run_supervised([sys.executable, str(Path(__file__).resolve()), *sys.argv[1:],
                                    "--supervised-worker"], args.artifacts,
                                   args.setup_timeout + args.warmup + args.duration + args.timeout)
        raise SystemExit(completed.returncode)
    passed = SourceCapacity(args).execute()
    print(f"{'PASS' if passed else 'SOURCE CAPACITY NOT MET'}: {args.artifacts.resolve() / 'report.json'}", flush=True)
    raise SystemExit(0 if passed else 1)


if __name__ == "__main__":
    main()
