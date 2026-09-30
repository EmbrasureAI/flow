#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "duckdb==1.5.5",
#   "psycopg[binary]==3.3.5",
#   "boto3==1.43.88",
#   "fastavro==1.12.2",
# ]
# ///
"""Sustained concurrent CDC, background compaction, and crash-recovery checks."""

import argparse
from collections import Counter
import hashlib
import math
from pathlib import Path
import os
import random
import subprocess
import sys
import threading
import time
import traceback

import psycopg
from psycopg import sql

from run import Run, dump

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "production"))
from process_lifecycle import arm_workload, disarm_workload, run_supervised


COHORT = 8
HOT_ROWS = 128


def base(worker):
    return (worker + 1) * 1_000_000


def digest(rows):
    return hashlib.sha256(repr(rows).encode()).hexdigest()


class Writers:
    """Pause only between transactions so source/target comparisons share a boundary."""

    def __init__(self, run):
        self.run = run
        self.condition = threading.Condition()
        self.paused = False
        self.stopping = False
        self.active = 0
        self.errors = []
        self.connections = {}
        self.cancel_requested = set()
        self.counts = [Counter() for _ in range(run.args.workers)]
        self.threads = [threading.Thread(target=self.write, args=(worker,), name=f"writer-{worker}")
                        for worker in range(run.args.workers)]
        for thread in self.threads:
            thread.start()

    def check(self):
        with self.condition:
            if self.errors:
                raise RuntimeError("PostgreSQL workload writer failed:\n" + self.errors[0])

    def pause(self):
        with self.condition:
            self.paused = True
            if not self.condition.wait_for(lambda: self.active == 0, timeout=30):
                raise TimeoutError("source writers did not finish their transactions")
        self.check()

    def resume(self):
        with self.condition:
            self.paused = False
            self.condition.notify_all()

    def stop(self):
        with self.condition:
            self.stopping = True
            self.condition.notify_all()
            connections = dict(self.connections)
            self.cancel_requested.update(connections)
        # Native socket calls can outlive their Python timeout. Keep a process
        # deadline armed until every non-daemon writer has actually joined.
        arm_workload(self.run.directory, time.monotonic() + 3 * len(connections) + 15)
        cancellation_errors = []
        for worker, connection in connections.items():
            try:
                connection.cancel_safe(timeout=3)
            except psycopg.Error as error:
                cancellation_errors.append({"worker": worker, "error": str(error)})
        deadline = time.monotonic() + 10
        for thread in self.threads:
            thread.join(timeout=max(0, deadline - time.monotonic()))
        unjoined = [thread.name for thread in self.threads if thread.is_alive()]
        shutdown = self.run.report.setdefault("writer_shutdown", {"cancellation_errors": []})
        shutdown["cancellation_errors"].extend(cancellation_errors)
        shutdown.update(joined=not unjoined, unjoined=unjoined)
        if not unjoined:
            disarm_workload(self.run.directory)
        self.check()
        if unjoined:
            raise TimeoutError(f"source writers did not stop: {unjoined}")

    def totals(self):
        with self.condition:
            return dict(sum(self.counts, Counter()))

    def write(self, worker):
        rng = random.Random(self.run.args.seed + worker)
        counts = self.counts[worker]
        first = base(worker)
        generation = 0
        try:
            with psycopg.connect(self.run.args.postgres_url, autocommit=True, connect_timeout=5,
                                  options="-c statement_timeout=20000") as pg:
                with self.condition:
                    # stop() may have run while this connection was opening.
                    if self.stopping:
                        return
                    self.connections[worker] = pg
                pg.execute(sql.SQL("SET search_path TO {}, public").format(sql.Identifier(self.run.name)))
                while True:
                    with self.condition:
                        self.condition.wait_for(lambda: self.stopping or not self.paused)
                        if self.stopping:
                            return
                        self.active += 1
                    try:
                        generation += 1
                        key = first + 100 + rng.randrange(HOT_ROWS)
                        operation = rng.randrange(6)
                        effects = Counter()
                        def mutate(kind, query, parameters):
                            affected = pg.execute(query, parameters).rowcount
                            effects[kind + "_rows"] += affected
                            return affected

                        with pg.transaction():
                            # Every committed table snapshot must preserve all eight rows of each generation.
                            mutate("update", "UPDATE orders SET amount = %s, payload = %s WHERE id BETWEEN %s AND %s",
                                   (generation, f"generation-{generation}", first, first + COHORT - 1))
                            mutate("update", "UPDATE accounts SET amount = %s WHERE id BETWEEN %s AND %s",
                                   (generation, first, first + COHORT - 1))
                            if operation == 0:
                                inserted = mutate("insert", "INSERT INTO orders (id, tenant, payload, amount) VALUES (%s,%s,%s,%s) "
                                                  "ON CONFLICT (id) DO NOTHING", (key, worker, f"upsert-世界-{generation}", generation))
                                if not inserted:
                                    mutate("update", "UPDATE orders SET payload = %s, amount = %s WHERE id = %s",
                                           (f"upsert-世界-{generation}", generation, key))
                            elif operation == 1:
                                mutate("update", "UPDATE orders SET payload = %s, amount = %s, active = %s, data = %s WHERE id = %s",
                                       (None if generation % 3 == 0 else "updated-λ", -generation, generation % 2 == 0,
                                        bytes([worker % 256, generation % 256, 0, 255]), key))
                            elif operation == 2:
                                mutate("delete", "DELETE FROM orders WHERE id = %s", (key,))
                            elif operation == 3:
                                mutate("delete", "DELETE FROM orders WHERE id = %s", (key,))
                                mutate("insert", "INSERT INTO orders (id, tenant, payload) VALUES (%s,%s,'reinserted')", (key, worker))
                            elif operation == 4:
                                moved = key + 10_000
                                mutate("delete", "DELETE FROM orders WHERE id = %s", (moved,))
                                mutate("update", "UPDATE orders SET id = %s, payload = 'moved' WHERE id = %s", (moved, key))
                                mutate("insert", "INSERT INTO orders (id, tenant, payload) VALUES (%s,%s,'replacement')", (key, worker))
                            else:
                                temporary = first + 100_000 + generation
                                mutate("insert", "INSERT INTO orders (id, tenant) VALUES (%s,%s)", (temporary, worker))
                                mutate("update", "UPDATE orders SET payload = 'never-visible' WHERE id = %s", (temporary,))
                                mutate("delete", "DELETE FROM orders WHERE id = %s", (temporary,))
                            pg.execute("SAVEPOINT undo")
                            pg.execute("DELETE FROM orders WHERE id BETWEEN %s AND %s", (first, first + COHORT - 1))
                            pg.execute("UPDATE accounts SET amount = -999999 WHERE id BETWEEN %s AND %s", (first, first + COHORT - 1))
                            pg.execute("ROLLBACK TO SAVEPOINT undo")
                            pg.execute("RELEASE SAVEPOINT undo")
                            if generation % 11 == 0:
                                raise psycopg.Rollback()
                        with self.condition:
                            counts["savepoint_rollback"] += 1
                            counts["savepoint_rolled_back_rows"] += COHORT * 2
                            if generation % 11 == 0:
                                counts["rollback"] += 1
                                counts.update({"rolled_back_" + kind: value for kind, value in effects.items()})
                            else:
                                counts["commit"] += 1
                                counts[("upsert", "update", "delete", "delete_reinsert", "pk_move", "insert_update_delete")[operation]] += 1
                                counts.update(effects)
                    finally:
                        with self.condition:
                            self.active -= 1
                            self.condition.notify_all()
                    time.sleep(rng.uniform(0.25, 0.55))
        except BaseException as error:
            with self.condition:
                if not (worker in self.cancel_requested and isinstance(error, psycopg.errors.QueryCanceled)):
                    self.errors.append(traceback.format_exc())
                self.condition.notify_all()
        finally:
            with self.condition:
                self.connections.pop(worker, None)
                self.condition.notify_all()


class StressRun(Run):
    def __init__(self, args):
        self.writers = None
        self.compactor = None
        self.compactor_log = None
        self.samples = 0
        self.peak_lag_bytes = 0
        self.peak_pending = 0
        self.retained = []
        self.active_intervals = []
        super().__init__(args)
        self.report.update(seed=args.seed, workers=args.workers, requested_active_seconds=args.duration)

    def seed(self):
        result = super().seed()
        for worker in range(self.args.workers):
            first = base(worker)
            self.pg.execute("INSERT INTO orders (id,tenant,payload,amount) SELECT i,%s,'generation-0',0 "
                            "FROM generate_series(%s::bigint,%s::bigint) i", (worker, first, first + COHORT - 1))
            self.pg.execute("INSERT INTO accounts SELECT i,0 FROM generate_series(%s::bigint,%s::bigint) i",
                            (first, first + COHORT - 1))
            self.pg.execute("INSERT INTO orders (id,tenant,payload) SELECT i,%s,'hot-key' "
                            "FROM generate_series(%s::bigint,%s::bigint) i", (worker, first + 100, first + 100 + HOT_ROWS - 1))
        result["orders"] += self.args.workers * (COHORT + HOT_ROWS)
        result["accounts"] += self.args.workers * COHORT
        return result

    def start_compactor(self):
        if self.args.compactor_binary is not None:
            self.compactor_log = (self.directory / "compactor.log").open("wb")
            self.compactor = subprocess.Popen(
                [str(self.args.compactor_binary.resolve()), "--config", str(self.config)],
                env=self.environment, stdout=self.compactor_log, stderr=subprocess.STDOUT)
        self.report["compactor"] = "external process" if self.compactor else "daemon background compactor role"

    def alive(self):
        super().alive()
        if self.compactor is not None and self.compactor.poll() is not None:
            raise RuntimeError(f"background compactor exited {self.compactor.returncode}; see compactor.log")
        if self.writers is not None:
            self.writers.check()

    def streamed_abort(self):
        self.pg.execute("SELECT pg_stat_clear_snapshot()")
        before = self.pg.execute("SELECT stream_count FROM pg_stat_replication_slots WHERE slot_name = %s", (self.name,)).fetchone()[0]
        barrier = self.transaction([
            """INSERT INTO orders (id,tenant,payload) SELECT i,7,string_agg(md5((i * 100 + j)::text),'')
               FROM generate_series(200000,203999) i CROSS JOIN generate_series(1,20) j GROUP BY i""",
            "SAVEPOINT streamed_child",
            "UPDATE orders SET payload = reverse(payload), amount = -999999 WHERE id BETWEEN 200000 AND 203999",
            "DELETE FROM orders WHERE id BETWEEN 200000 AND 200511",
            "ROLLBACK TO SAVEPOINT streamed_child",
            "RELEASE SAVEPOINT streamed_child",
            "UPDATE orders SET amount = 123.4567 WHERE id BETWEEN 200000 AND 200511",
            "DELETE FROM orders WHERE id BETWEEN 203744 AND 203999",
            "UPDATE accounts SET amount = amount + 1 WHERE id <= 8",
        ])
        self.wait_materialized(barrier)
        result = self.compare("streamed-subtransaction-abort")
        def streamed():
            self.pg.execute("SELECT pg_stat_clear_snapshot()")
            count = self.pg.execute("SELECT stream_count FROM pg_stat_replication_slots WHERE slot_name = %s", (self.name,)).fetchone()[0]
            return count - before if count > before else None
        result["stream_count_delta"] = self.until("large transaction did not use pgoutput streaming", streamed)
        result["committed_rows"] = {"insert": 4000, "update": 520, "delete": 256}
        result["aborted_subtransaction_rows"] = {"update": 4000, "delete": 512}
        return result

    def sample(self):
        self.alive()
        for table in ("orders", "accounts"):
            metadata = self.table(table)
            rows = self.rows(table, metadata)
            assert len(rows) == len({row[0] for row in rows}), f"{table}: duplicate visible primary key"
            by_id = {row[0]: row for row in rows}
            amount_column = 3 if table == "orders" else 1
            for worker in range(self.args.workers):
                cohort = [by_id.get(key) for key in range(base(worker), base(worker) + COHORT)]
                assert all(row is not None for row in cohort), f"{table}: partially visible cohort"
                generations = {row[amount_column] for row in cohort}
                assert len(generations) == 1, f"{table}: torn transaction generation {generations}"
                assert next(iter(generations)) >= 0, f"{table}: aborted subtransaction became visible"
            dump(self.directory / f"latest-observed-{table}.json", metadata)
        # Old metadata locations pin immutable snapshots even while compaction rewrites current files.
        table, metadata, expected = self.retained[self.samples % len(self.retained)]
        assert digest(self.rows(table, metadata)) == expected, "retained snapshot changed during writes/compaction"
        metrics = self.metrics()
        assert metrics.get("flow_materialized_lsn", 0) <= metrics.get("flow_journal_durable_lsn", 0) <= metrics.get("flow_source_received_lsn", 0)
        self.peak_lag_bytes = max(self.peak_lag_bytes, metrics.get("flow_source_received_lsn", 0) - metrics.get("flow_materialized_lsn", 0))
        self.peak_pending = max(self.peak_pending, metrics.get("flow_pending_transactions", 0))
        self.samples += 1

    def drive(self, seconds):
        start = time.time_ns() // 1_000_000
        deadline = time.monotonic() + seconds
        try:
            while time.monotonic() < deadline:
                self.sample()
                time.sleep(min(1.5, max(0, deadline - time.monotonic())))
        finally:
            self.active_intervals.append((start, time.time_ns() // 1_000_000))
        return {"writer_operations": self.writers.totals(), "live_snapshot_checks": self.samples}

    def checkpoint(self, name, resume=True):
        self.writers.pause()
        try:
            barrier = self.transaction([
                "UPDATE orders SET ratio = COALESCE(ratio,0) + 0.125 WHERE id = 1",
                "UPDATE accounts SET amount = amount + 0.125 WHERE id = 1",
            ])
            metrics = self.wait_materialized(barrier)
            result = self.compare(name)
            for table in ("orders", "accounts"):
                metadata = self.table(table)
                self.retained.append((table, metadata, digest(self.rows(table, metadata))))
            result["source_materialized_lsn"] = metrics["flow_materialized_lsn"]
            result["writer_operations"] = self.writers.totals()
            return result
        finally:
            if resume:
                self.writers.resume()

    def crash(self):
        before = self.metrics()
        self.stop(crash=True)
        commits_before = self.writers.totals().get("commit", 0)
        time.sleep(3)
        self.writers.check()
        commits_after = self.writers.totals().get("commit", 0)
        assert commits_after > commits_before, "no WAL backlog was committed while daemon was down"
        self.start()
        return {"committed_while_down": commits_after - commits_before,
                "materialized_before_kill": before.get("flow_materialized_lsn", 0)}

    def verify_compaction(self):
        result = self.compaction()
        for table in ("orders", "accounts"):
            snapshots = sorted(self.table(table)["metadata"]["snapshots"], key=lambda row: row["sequence-number"])
            active = [snapshot for snapshot in snapshots if any(start <= snapshot["timestamp-ms"] <= end
                      for start, end in self.active_intervals)]
            labels = [snapshot["summary"].get("streaming.operation") for snapshot in active]
            interleaved = any(label == "compact" and "ingest" in labels[:index] and "ingest" in labels[index + 1:]
                              for index, label in enumerate(labels))
            assert interleaved, f"{table}: no ingest/compact/ingest commits while source writers were active: {labels}"
            result[table]["active_compaction_commits"] = labels.count("compact")
            result[table]["active_ingestion_commits"] = labels.count("ingest")
            external = sum("local-test.compaction-id" in snapshot["summary"] for snapshot in active)
            result[table]["active_external_compaction_commits"] = external
            if self.compactor is not None:
                assert external > 0, f"{table}: external background service did not compact during writes"
            result[table]["ingest_compact_ingest_interleaved"] = interleaved
        for table, metadata, expected in self.retained:
            assert digest(self.rows(table, metadata)) == expected, "retained snapshot changed after compaction"
        result["retained_snapshots_verified"] = len(self.retained)
        result["live_snapshot_checks"] = self.samples
        result["peak_observed_source_lag_bytes"] = self.peak_lag_bytes
        result["peak_observed_pending_transactions"] = self.peak_pending
        result["writer_operations"] = self.writers.totals()
        self.report["active_intervals_unix_ms"] = self.active_intervals
        return result

    def execute(self):
        try:
            self.phase("seed", self.seed)
            self.phase("initial-copy", self.initialize)
            self.start()
            self.phase("snapshot-wal-handoff", self.handoff)
            self.phase("streamed-subtransaction-abort", self.streamed_abort)
            self.start_compactor()
            self.retained = [("orders", self.initial_metadata, digest(self.initial_rows))]
            self.writers = Writers(self)
            for quarter in range(4):
                self.phase(f"concurrent-writers-{quarter + 1}", lambda: self.drive(self.args.duration / 4))
                if quarter == 1:
                    self.phase("sigkill-with-active-writers", self.crash)
                self.phase(f"checkpoint-{quarter + 1}", lambda q=quarter: self.checkpoint(f"checkpoint-{q + 1}", resume=q != 3))
            self.writers.stop()
            self.phase("compaction-interleaving-and-history", self.verify_compaction)
            self.report["passed"] = True
        except BaseException as error:
            self.report["failure"] = str(error)
            self.report["traceback"] = traceback.format_exc()
            raise
        finally:
            original_failure = sys.exc_info()[1]
            writer_cleanup_error = None
            dump(self.directory / "report.json", self.report)
            try:
                if self.writers is not None:
                    self.writers.stop()
            except BaseException as error:
                writer_cleanup_error = error
                self.report["passed"] = False
                self.report.setdefault("cleanup_errors", []).append(str(error))
                self.report.setdefault("failure", str(error))
            finally:
                # Bound compactor/daemon shutdown too, after successful writer
                # join has disarmed its more specific deadline.
                if self.writers is None or not any(thread.is_alive() for thread in self.writers.threads):
                    arm_workload(self.directory, time.monotonic() + 60)
                if self.compactor is not None:
                    self.compactor.terminate()
                    try:
                        self.compactor.wait(timeout=15)
                    except subprocess.TimeoutExpired:
                        self.compactor.kill()
                        self.compactor.wait(timeout=10)
                    self.compactor_log.close()
                self.stop()
                dump(self.directory / "report.json", self.report)
                self.pg.close()
                self.duck.close()
                if writer_cleanup_error is None:
                    disarm_workload(self.directory)
            if writer_cleanup_error is not None and original_failure is None:
                raise writer_cleanup_error


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--postgres-url", default=os.environ.get("FLOW_POSTGRES_URL"))
    parser.add_argument("--catalog-uri", required=True)
    parser.add_argument("--s3-endpoint", required=True)
    parser.add_argument("--warehouse", default="s3://warehouse/")
    parser.add_argument("--binary", type=Path, default=Path("target/debug/embrasure-flow"))
    parser.add_argument("--compactor-binary", type=Path, help="optional external compactor, accepting --config PATH")
    parser.add_argument("--artifacts", type=Path, required=True)
    parser.add_argument("--timeout", type=float, default=180)
    parser.add_argument("--duration", type=float, default=120, help="active concurrent-writer seconds, excluding checkpoints")
    parser.add_argument("--seed", type=int, default=20260904)
    parser.add_argument("--workers", type=int, default=3)
    parser.add_argument("--supervised-worker", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()
    if not args.postgres_url:
        parser.error("provide --postgres-url or FLOW_POSTGRES_URL")
    if (args.workers < 1 or not math.isfinite(args.duration) or args.duration < 20
            or not math.isfinite(args.timeout) or args.timeout <= 0):
        parser.error("--workers and --timeout must be positive, --duration at least 20 seconds, and times finite")
    if not args.supervised_worker:
        result = run_supervised([sys.executable, str(Path(__file__).resolve()), *sys.argv[1:], "--supervised-worker"],
                                args.artifacts, timeout=args.duration + 16 * args.timeout + 120)
        raise SystemExit(result.returncode)
    StressRun(args).execute()
    print(f"PASS: {args.artifacts.resolve() / 'report.json'}", flush=True)


if __name__ == "__main__":
    main()
