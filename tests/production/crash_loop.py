#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["duckdb==1.5.5", "psycopg[binary]==3.3.5", "boto3==1.43.88", "fastavro==1.12.2"]
# ///
"""Seeded crash-loop differential test.

Concurrent writers with overlapping keys, periodic large streamed transactions,
savepoint rollbacks and nullable column additions run while the daemon is
SIGKILLed at random points under random catalog faults. After every recovery,
each table must match PostgreSQL exactly, every Flow operation must be committed
at most once, and the slot must never have acknowledged a transaction that the
catalog did not contain when the daemon died.
"""

import argparse
from collections import Counter
import json
from decimal import Decimal
import os
from pathlib import Path
import random
import shlex
import sys
import threading
import time
import traceback

import psycopg
from psycopg import sql

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "local"))
from run import Run, dump, lsn
from proxy import CatalogProxy

TABLES = ("orders", "accounts", "events", "ticks")
# Writers share these key ranges, so their changes contend and interleave.
KEYS = {"orders": 6000, "accounts": 64, "events": 2000}
WEIGHTS = {"orders": 5, "accounts": 2, "events": 3}
OPERATIONS = ("update", "update", "upsert", "delete", "move")
BULK_BASE = 10_000_000
FAULTS = ("none", "hold", "reject", "drop", "unknown")
EXPECTED_ABORTS = (psycopg.errors.DeadlockDetected, psycopg.errors.UniqueViolation,
                   psycopg.errors.LockNotAvailable, psycopg.errors.QueryCanceled)


class TransactionLog:
    """Committed transactions with WAL positions bracketing their commit record.

    `lo` is read inside the transaction before COMMIT and `hi` after it returns,
    so lo < commit LSN <= end LSN <= hi. Every transaction advances its writer's
    `ticks` row, which no later change can cancel, so an acknowledged
    transaction must be inside the ticks table's published LSN range.
    """

    def __init__(self):
        self.lock = threading.Lock()
        self.entries = []

    def add(self, lo, hi, label):
        with self.lock:
            self.entries.append((lo, hi, label))

    def violations(self, confirmed, published):
        """Transactions certainly acknowledged (hi <= ACK) yet certainly unpublished (lo >= last LSN)."""
        with self.lock:
            return [entry for entry in self.entries if entry[1] <= confirmed and entry[0] >= published]

    def prune(self, published):
        # Published LSNs never regress; entries already covered can never violate.
        with self.lock:
            before = len(self.entries)
            self.entries = [entry for entry in self.entries if entry[0] >= published]
            return before - len(self.entries)


class Workload:
    """Writers pause only between transactions so comparisons share a boundary."""

    def __init__(self, run):
        self.run = run
        self.args = run.args
        self.log = TransactionLog()
        self.condition = threading.Condition()
        self.paused = False
        self.stopping = False
        self.active = 0
        self.errors = []
        self.counts = Counter()
        self.columns = {"orders": [], "events": []}
        # Daemon threads: a failed run must not hang in interpreter shutdown.
        self.threads = [threading.Thread(target=self.guard, args=(self.writer, worker), name=f"writer-{worker}",
                                         daemon=True) for worker in range(self.args.writers)]
        self.threads.append(threading.Thread(target=self.guard, args=(self.bulk, self.args.writers), name="bulk",
                                             daemon=True))
        for thread in self.threads:
            thread.start()

    def check(self):
        with self.condition:
            if self.errors:
                raise RuntimeError("PostgreSQL workload failed:\n" + self.errors[0])

    def pause(self):
        with self.condition:
            self.paused = True
            if not self.condition.wait_for(lambda: self.active == 0, timeout=90):
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
        for thread in self.threads:
            thread.join(timeout=90)
        unjoined = [thread.name for thread in self.threads if thread.is_alive()]
        if unjoined:
            raise TimeoutError(f"source writers did not stop: {unjoined}")
        self.check()

    def add_column(self, table, name):
        with self.condition:
            self.columns[table].append(name)

    def enter(self):
        with self.condition:
            self.condition.wait_for(lambda: self.stopping or not self.paused)
            if self.stopping:
                return False
            self.active += 1
            return True

    def leave(self):
        with self.condition:
            self.active -= 1
            self.condition.notify_all()

    def count(self, **values):
        with self.condition:
            self.counts.update(values)

    def guard(self, target, worker):
        try:
            options = "-c statement_timeout=30000 -c lock_timeout=10000"
            with psycopg.connect(self.args.postgres_url, autocommit=True, connect_timeout=10, options=options) as pg:
                pg.execute(sql.SQL("SET search_path TO {}, public").format(sql.Identifier(self.run.name)))
                target(pg, worker, random.Random(f"{self.args.seed}:{worker}"))
        except BaseException:
            with self.condition:
                self.errors.append(traceback.format_exc())
                # Stop the other writers; the main loop reports this error.
                self.stopping = True
                self.condition.notify_all()

    def commit(self, pg, worker, sequence, rng, body):
        """Run one transaction; log it only if it committed."""
        rollback = rng.random() < 0.05
        try:
            with pg.transaction() as transaction:
                effects = body()
                # The per-writer ticks row changes in every transaction.
                pg.execute("UPDATE ticks SET sequence = %s WHERE id = %s", (sequence, worker))
                if rollback:
                    raise psycopg.Rollback(transaction)
                lo = lsn(pg.execute("SELECT pg_current_wal_insert_lsn()::text").fetchone()[0])
        except EXPECTED_ABORTS as error:
            self.count(**{"aborted_" + type(error).__name__: 1})
            return
        if rollback:
            self.count(rolled_back=1)
            return
        hi = lsn(pg.execute("SELECT pg_current_wal_insert_lsn()::text").fetchone()[0])
        self.log.add(lo, hi, f"{worker}:{sequence}")
        self.count(committed=1, **effects)

    def writer(self, pg, worker, rng):
        sequence = 0
        while self.enter():
            try:
                sequence += 1
                self.commit(pg, worker, sequence, rng, lambda: self.mutations(pg, worker, sequence, rng))
            finally:
                self.leave()
            time.sleep(rng.uniform(0, 0.15))

    def mutations(self, pg, worker, sequence, rng):
        effects = Counter()
        created = set()
        version = worker * 1_000_000 + sequence
        for _ in range(rng.randint(1, 6)):
            self.mutate(pg, rng, version, created, effects)
        if rng.random() < 0.3:
            # Changes inside a rolled-back savepoint must never be published.
            pg.execute("SAVEPOINT undo")
            for _ in range(rng.randint(1, 3)):
                self.mutate(pg, rng, -version, set(created), Counter())
            pg.execute("ROLLBACK TO SAVEPOINT undo")
            pg.execute("RELEASE SAVEPOINT undo")
            effects["savepoint_rollbacks"] += 1
        return effects

    def mutate(self, pg, rng, version, created, effects):
        table = rng.choices(list(WEIGHTS), weights=list(WEIGHTS.values()))[0]
        operation = rng.choice(OPERATIONS)
        key = rng.randint(1, KEYS[table])
        if operation in ("delete", "move") and (table, key) in created:
            # Deleting a row inserted by this transaction could net to no change.
            operation = "update"
        values = self.values(table, rng, version)
        identifier = sql.Identifier(table)
        if operation == "update":
            assignments = sql.SQL(", ").join(sql.SQL("{} = %s").format(sql.Identifier(name)) for name in values)
            query = sql.SQL("UPDATE {} SET {} WHERE id = %s").format(identifier, assignments)
            affected = pg.execute(query, [*values.values(), key]).rowcount
        elif operation == "upsert":
            names = sql.SQL(", ").join(sql.Identifier(name) for name in ("id", *values))
            updates = sql.SQL(", ").join(sql.SQL("{0} = excluded.{0}").format(sql.Identifier(name)) for name in values)
            query = sql.SQL("INSERT INTO {} ({}) VALUES ({}) ON CONFLICT (id) DO UPDATE SET {} RETURNING xmax = 0").format(
                identifier, names, sql.SQL(", ").join(sql.Placeholder() * (len(values) + 1)), updates)
            if pg.execute(query, [key, *values.values()]).fetchone()[0]:
                created.add((table, key))
            affected = 1
        elif operation == "delete":
            affected = pg.execute(sql.SQL("DELETE FROM {} WHERE id = %s").format(identifier), (key,)).rowcount
        else:
            target = rng.randint(1, KEYS[table])
            first = next(iter(values))
            query = sql.SQL("UPDATE {0} SET id = %s, {1} = %s WHERE id = %s "
                            "AND NOT EXISTS (SELECT 1 FROM {0} WHERE id = %s)").format(identifier, sql.Identifier(first))
            affected = pg.execute(query, (target, values[first], key, target)).rowcount
            if affected:
                created.add((table, target))
        effects[f"{table}_{operation}_rows"] += affected

    def values(self, table, rng, version):
        with self.condition:
            added = list(self.columns.get(table, ()))
        if table == "orders":
            values = {"tenant": version % 11, "payload": f"v{version}-{rng.getrandbits(32):08x}-λ",
                      "amount": Decimal(version) / 10000,
                      "active": version % 2 == 0, "ratio": version / 7}
        elif table == "accounts":
            values = {"amount": Decimal(version) / 1000 + Decimal(rng.randrange(1000)) / 10000}
        else:
            values = {"writer": version // 1_000_000, "version": version, "flag": rng.random() < 0.5,
                      "ratio": rng.random()}
        for name in added:
            if rng.random() < 0.5:
                values[name] = version if name.endswith("_int") else f"added-{version}"
        return values

    def bulk(self, pg, worker, rng):
        """Periodic transactions larger than logical_decoding_work_mem are streamed."""
        sequence = 0
        next_at = time.monotonic() + rng.uniform(1, self.args.large_interval)
        while True:
            with self.condition:
                if self.condition.wait_for(lambda: self.stopping, timeout=max(0, next_at - time.monotonic())):
                    return
            if not self.enter():
                return
            try:
                sequence += 1
                start = BULK_BASE + (sequence % 2) * 1_000_000
                rows = self.args.large_rows
                version = worker * 1_000_000 + sequence

                def body():
                    # Replace this range's rows from two batches ago.
                    deleted = pg.execute("DELETE FROM orders WHERE id >= %s AND id < %s", (start, start + 1_000_000)).rowcount
                    pg.execute("""INSERT INTO orders (id, tenant, payload)
                        SELECT i, i %% 11, string_agg(md5((i * 100 + j)::text || %s), '')
                        FROM generate_series(%s::bigint, %s::bigint) i CROSS JOIN generate_series(1, 20) j GROUP BY i""",
                               (str(version), start, start + rows - 1))
                    pg.execute("UPDATE orders SET amount = %s WHERE id BETWEEN %s AND %s",
                               (Decimal(version) / 10000, start, start + rows // 10))
                    pg.execute("SAVEPOINT bulk_undo")
                    pg.execute("DELETE FROM orders WHERE id BETWEEN %s AND %s", (start, start + rows // 2))
                    pg.execute("ROLLBACK TO SAVEPOINT bulk_undo")
                    pg.execute("UPDATE accounts SET amount = %s WHERE id = %s",
                               (Decimal(version) / 1000, rng.randint(1, KEYS["accounts"])))
                    return {"large_transactions": 1, "large_rows": rows, "large_deleted_rows": deleted}

                self.commit(pg, worker, sequence, rng, body)
            finally:
                self.leave()
            next_at = time.monotonic() + rng.uniform(0.5, 1.5) * self.args.large_interval


class CrashLoop(Run):
    def __init__(self, args):
        self.proxy = None
        self.workload = None
        super().__init__(args)
        self.rng = random.Random(args.seed)
        self.proxy = CatalogProxy(args.catalog_uri, self.directory / "catalog-proxy.jsonl")
        # Only the daemon sees faults; row and metadata checks read the real catalog.
        self.config.write_text(self.config.read_text().replace(
            f"uri = {json.dumps(args.catalog_uri)}", f"uri = {json.dumps(self.proxy.url)}"))
        assert self.proxy.url in self.config.read_text()
        self.operations = {}
        self.cycles = []
        self.report.update(seed=args.seed, reproduce=reproduction(args), cycles=self.cycles,
                           format_version=args.format_version)

    def configure(self):
        super().configure()
        self.environment["RUST_LOG"] = "info"
        # The base fixture forces a rewrite after almost every epoch. Keep
        # compaction running beside the crashes without it dominating recovery.
        self.set_compaction_policy({"deleted_rows_percent": 10, "delete_files_soft": 16, "delete_files_hard": 64})
        text = self.config.read_text()
        for name, columns in (
            ("events", [("id", "Int64", False), ("writer", "Int32", False), ("version", "Int64", True),
                        ("flag", "Bool", True), ("ratio", "Float64", True)]),
            ("ticks", [("id", "Int32", False), ("sequence", "Int64", False)]),
        ):
            text += (f'\n[[tables]]\nsource_namespace = "{self.name}"\nsource_table = "{name}"\n'
                     f'target_namespace = ["{self.name}"]\ntarget_table = "{name}"\nprimary_key = [0]\n'
                     f'{self.format_line()}columns = [\n')
            for index, (column, kind, nullable) in enumerate(columns, 1):
                text += (f'  {{ field_id = {index}, name = "{column}", data_type = "{kind}", '
                         f'nullable = {str(nullable).lower()} }},\n')
            text += "]\n"
        self.config.write_text(text)

    def seed(self):
        result = super().seed()
        # DEFAULT identity: fixed-width columns only, and added columns stay fixed-width.
        self.pg.execute("""CREATE TABLE events (id bigint PRIMARY KEY, writer integer NOT NULL,
            version bigint, flag boolean, ratio double precision)""")
        self.pg.execute("INSERT INTO events SELECT i, 0, 0, i % 2 = 0, i * 0.5 FROM generate_series(1, 1000) i")
        self.pg.execute("CREATE TABLE ticks (id integer PRIMARY KEY, sequence bigint NOT NULL)")
        self.pg.execute("ALTER TABLE ticks REPLICA IDENTITY FULL")
        self.pg.execute("INSERT INTO ticks SELECT i, 0 FROM generate_series(0, %s) i", (self.args.writers,))
        self.pg.execute(sql.SQL("ALTER PUBLICATION {} ADD TABLE events, ticks").format(sql.Identifier(self.name)))
        result.update(events=1000, ticks=self.args.writers + 1)
        return result

    def compare(self, phase):
        """Compare full rows over the target's current schema; PostgreSQL columns
        not yet in the target must be entirely NULL (no row carried a value yet)."""
        result = {}
        for name in TABLES:
            metadata = self.table(name)
            schema = next(schema for schema in metadata["metadata"]["schemas"]
                          if schema["schema-id"] == metadata["metadata"]["current-schema-id"])
            columns = [field["name"] for field in schema["fields"]]
            source = [row[0] for row in self.pg.execute(
                "SELECT attname FROM pg_attribute WHERE attrelid = %s::regclass AND attnum > 0 "
                "AND NOT attisdropped ORDER BY attnum", (name,)).fetchall()]
            assert source[:len(columns)] == columns, f"{phase}: {name} columns differ: {columns} vs {source}"
            for missing in source[len(columns):]:
                present = self.pg.execute(sql.SQL("SELECT count(*) FROM {} WHERE {} IS NOT NULL").format(
                    sql.Identifier(name), sql.Identifier(missing))).fetchone()[0]
                assert present == 0, f"{phase}: {name}.{missing} has {present} values but is not in the target"
            expected = self.pg.execute(sql.SQL("SELECT {} FROM {} ORDER BY id").format(
                sql.SQL(", ").join(map(sql.Identifier, columns)), sql.Identifier(name))).fetchall()
            actual = self.rows(name, metadata)
            if actual != expected:
                mismatch = next(((i, a, b) for i, (a, b) in enumerate(zip(actual, expected)) if a != b), None)
                dump(self.directory / f"{phase}-{name}-metadata.json", metadata)
                raise AssertionError(f"{phase}: {name} differs: Iceberg={len(actual)} PostgreSQL={len(expected)} "
                                     f"first={mismatch}; reproduce with --seed {self.args.seed}")
            result[name] = {"rows": len(actual), "columns": len(columns),
                            "snapshot": metadata["metadata"]["current-snapshot-id"]}
        return result

    def published(self):
        """Per table: every operation ID, and the highest source LSN in the catalog."""
        last = {}
        for name in TABLES:
            snapshots = self.table(name)["metadata"]["snapshots"]
            counts = Counter(snapshot["summary"].get("flow.operation-id") for snapshot in snapshots)
            counts.pop(None, None)
            duplicated = [operation for operation, count in counts.items() if count > 1]
            assert not duplicated, f"{name}: operation committed more than once: {duplicated[:3]}"
            for snapshot in snapshots:
                operation = snapshot["summary"].get("flow.operation-id")
                if operation is None:
                    continue
                identity = (name, snapshot["snapshot-id"])
                assert self.operations.setdefault(operation, identity) == identity, (
                    f"operation {operation} committed as {self.operations[operation]} and {identity}")
            last[name] = max((lsn(snapshot["summary"]["streaming.last-lsn"]) for snapshot in snapshots
                              if "streaming.last-lsn" in snapshot["summary"]), default=0)
        return last

    def slot(self):
        return lsn(self.pg.execute("SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = %s",
                                   (self.name,)).fetchone()[0])

    def check_ack(self):
        # The process is dead: neither the slot nor Flow's catalog writes can move,
        # except a held or in-flight request that commits after its caller died.
        confirmed = self.slot()
        last = self.published()
        violations = self.workload.log.violations(confirmed, last["ticks"])
        assert not violations, (
            f"slot confirmed {confirmed} past unpublished transactions {violations[:3]} "
            f"(ticks published through {last['ticks']}); reproduce with --seed {self.args.seed}")
        return {"confirmed_flush_lsn": confirmed, "published_last_lsn": last,
                "pruned_log_entries": self.workload.log.prune(last["ticks"])}

    def arm(self, fault, window):
        details = {"fault": fault}
        if fault == "hold":
            self.proxy.hold_commits()
            details["release_after"] = round(self.rng.uniform(0.2, 1.5) * window, 3)
        elif fault == "reject":
            self.proxy.reject = True
            details["reject_for"] = round(self.rng.uniform(0.3, 1.0) * window, 3)
        elif fault == "drop":
            self.proxy.arm_drop()
        elif fault == "unknown":
            details["status"] = self.rng.choice((500, 502, 504))
            self.proxy.arm_drop(status=details["status"])
        return details

    def disarm(self):
        self.proxy.reject = False
        with self.proxy.lock:
            self.proxy.drop_remaining = 0
        # A held commit proceeds upstream after its caller died.
        self.proxy.release_commits.set()

    def add_column(self):
        table = self.rng.choice(("orders", "events"))
        self.added_columns += 1
        # DEFAULT identity requires fixed-width columns; FULL accepts any type.
        kind = "integer" if table == "events" or self.rng.random() < 0.5 else "text"
        name = f"added_{self.added_columns}_{'int' if kind == 'integer' else 'text'}"
        self.pg.execute("SET lock_timeout = '5s'")
        try:
            self.pg.execute(sql.SQL("ALTER TABLE {} ADD COLUMN {} " + kind).format(
                sql.Identifier(table), sql.Identifier(name)))
        except psycopg.errors.LockNotAvailable:
            return {"table": table, "column": name, "skipped": "lock timeout"}
        finally:
            self.pg.execute("RESET lock_timeout")
        self.workload.add_column(table, name)
        return {"table": table, "column": name, "type": kind}

    def cycle(self, number):
        window = self.rng.uniform(self.args.min_kill, self.args.max_kill)
        fault = self.rng.choice(FAULTS)
        details = {"cycle": number, "window_seconds": round(window, 3), **self.arm(fault, window)}
        ddl_at = (self.rng.uniform(0, window)
                  if self.added_columns < self.args.max_added_columns and self.rng.random() < self.args.ddl_probability
                  else None)
        with self.proxy.lock:
            offset = len(self.proxy.events)
        started = time.monotonic()
        while (elapsed := time.monotonic() - started) < window:
            self.alive()
            self.workload.check()
            if ddl_at is not None and elapsed >= ddl_at:
                details["added_column"] = self.add_column()
                ddl_at = None
            if fault == "reject" and elapsed >= details["reject_for"]:
                self.proxy.reject = False
            if fault == "hold" and elapsed >= details["release_after"]:
                self.proxy.release_commits.set()
            time.sleep(0.05)
        process = self.process
        self.stop(crash=True)
        assert process.returncode == -9, f"daemon exited {process.returncode} before SIGKILL"
        details["ack"] = self.check_ack()
        self.disarm()
        with self.proxy.lock:
            details["proxy_faults"] = Counter(event.get("fault") for event in self.proxy.events[offset:]
                                              if event.get("fault"))
        recovered = time.monotonic()
        self.start()
        self.workload.pause()
        try:
            details["materialized"] = self.wait_materialized(0)["flow_materialized_lsn"]
            details["recovery_seconds"] = round(time.monotonic() - recovered, 3)
            details["rows"] = self.compare(f"cycle-{number}")
            self.published()
        finally:
            self.workload.resume()
        self.cycles.append(details)
        print(f"[cycle {number}] {fault} after {window:.2f}s: recovered in {details['recovery_seconds']}s", flush=True)
        dump(self.directory / "report.json", self.report)
        return details

    def crash_loop(self):
        self.added_columns = 0
        self.workload = Workload(self)
        deadline = time.monotonic() + self.args.duration
        number = 0
        while time.monotonic() < deadline and (self.args.cycles is None or number < self.args.cycles):
            number += 1
            self.cycle(number)
        self.workload.stop()
        faults = Counter(cycle["fault"] for cycle in self.cycles)
        assert number >= self.args.min_cycles, f"only {number} crash cycles fit in the time budget"
        return {"cycles": number, "faults": faults, "workload": dict(self.workload.counts),
                "added_columns": self.added_columns}

    def final(self):
        barrier = self.wait_materialized(0)
        result = {"rows": self.compare("final"), "materialized": barrier["flow_materialized_lsn"]}
        self.published()
        result["operations"] = len(self.operations)
        if not self.args.skip_manifest_audit:
            result["manifests"] = {name: self.audit(name, last=self.args.audit_snapshots) for name in TABLES}
        initial = self.initial_metadata["metadata"]["current-snapshot-id"]
        if any(snapshot["snapshot-id"] == initial for snapshot in self.table("orders")["metadata"]["snapshots"]):
            assert self.rows("orders", self.initial_metadata) == self.initial_rows, "retained initial snapshot changed"
        # PostgreSQL must have streamed at least one large transaction.
        self.pg.execute("SELECT pg_stat_clear_snapshot()")
        result["stream_count"] = self.pg.execute(
            "SELECT stream_count FROM pg_stat_replication_slots WHERE slot_name = %s", (self.name,)).fetchone()[0]
        if self.workload.counts["large_transactions"]:
            assert result["stream_count"] > 0, "no large transaction was streamed; raise --large-rows"
        return result

    def execute(self):
        print(f"SEED={self.args.seed}\nreproduce: {reproduction(self.args)}", flush=True)
        try:
            self.phase("seed", self.seed)
            self.phase("initial-copy", self.initialize)
            self.start()
            self.phase("snapshot-wal-handoff", self.handoff)
            self.phase("randomized-crash-loop", self.crash_loop)
            self.phase("final-differential-and-manifest-audit", self.final)
            self.stop()
            self.check_worker_panics()
            self.report["passed"] = True
        except BaseException as error:
            self.report["failure"] = str(error)
            self.report["traceback"] = traceback.format_exc()
            raise
        finally:
            if self.workload is not None and not self.workload.stopping:
                try:
                    self.workload.stop()
                except Exception as error:
                    self.report["writer_shutdown_error"] = str(error)
            self.stop()
            if self.proxy is not None:
                self.proxy.close()
            dump(self.directory / "report.json", self.report)
            self.pg.close()
            self.duck.close()


def reproduction(args):
    arguments = ["uv", "run", "tests/production/crash_loop.py", "--seed", str(args.seed),
                 "--duration", f"{args.duration:g}", "--writers", str(args.writers),
                 "--min-kill", f"{args.min_kill:g}", "--max-kill", f"{args.max_kill:g}",
                 "--large-rows", str(args.large_rows), "--large-interval", f"{args.large_interval:g}",
                 "--format-version", str(args.format_version), "--binary", str(args.binary),
                 "--timeout", f"{args.timeout:g}"]
    if args.cycles is not None:
        arguments += ["--cycles", str(args.cycles)]
    return shlex.join(arguments) + " --catalog-uri ... --s3-endpoint ... --artifacts NEW_DIRECTORY"


def parser():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--postgres-url", default=os.environ.get("FLOW_POSTGRES_URL"))
    parser.add_argument("--catalog-uri", required=True)
    parser.add_argument("--s3-endpoint", required=True)
    parser.add_argument("--warehouse", default="s3://warehouse/")
    parser.add_argument("--binary", type=Path, default=Path("target/debug/embrasure-flow"))
    parser.add_argument("--artifacts", type=Path, required=True)
    parser.add_argument("--timeout", type=float, default=240, help="maximum seconds per recovery wait")
    parser.add_argument("--seed", type=int, default=None, help="random seed (default: new, printed and recorded)")
    parser.add_argument("--duration", type=float, default=240, help="seconds of crash cycles")
    parser.add_argument("--cycles", type=int, help="stop after this many cycles, even with time left")
    parser.add_argument("--min-cycles", type=int, default=5, help="fail if fewer cycles fit in the duration")
    parser.add_argument("--writers", type=int, default=3)
    parser.add_argument("--min-kill", type=float, default=0.5, help="shortest run before SIGKILL, seconds")
    parser.add_argument("--max-kill", type=float, default=10, help="longest run before SIGKILL, seconds")
    parser.add_argument("--large-rows", type=int, default=12000,
                        help="rows per large transaction; the default exceeds a 4MB logical_decoding_work_mem")
    parser.add_argument("--large-interval", type=float, default=15, help="mean seconds between large transactions")
    parser.add_argument("--ddl-probability", type=float, default=0.25, help="chance of a column addition per cycle")
    parser.add_argument("--max-added-columns", type=int, default=8)
    parser.add_argument("--format-version", type=int, choices=(2, 3), default=2)
    parser.add_argument("--skip-manifest-audit", action="store_true", help="skip the final manifest audit")
    parser.add_argument("--audit-snapshots", type=int, default=64,
                        help="audit the manifests of each table's most recent snapshots")
    return parser


def main():
    arguments = parser()
    args = arguments.parse_args()
    if not args.postgres_url:
        arguments.error("provide --postgres-url or FLOW_POSTGRES_URL")
    if not 0 < args.min_kill <= args.max_kill or args.writers < 1 or args.duration <= 0:
        arguments.error("need 0 < --min-kill <= --max-kill, at least one writer and a positive duration")
    if args.seed is None:
        args.seed = int.from_bytes(os.urandom(4), "big")
    CrashLoop(args).execute()
    print(f"PASS (seed {args.seed}): {args.artifacts.resolve() / 'report.json'}", flush=True)


if __name__ == "__main__":
    main()
