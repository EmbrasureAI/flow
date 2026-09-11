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
"""Exercise the real daemon against disposable PostgreSQL, REST, and S3 services."""

import argparse
import io
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import time
import tomllib
import traceback
from urllib.parse import quote, urlparse
from urllib.request import urlopen
import uuid

import boto3
from botocore.config import Config as S3Config
import duckdb
import fastavro
import psycopg
from psycopg import sql

from duckdb_extensions import load_extensions


def lsn(value):
    high, low = value.split("/")
    return (int(high, 16) << 32) | int(low, 16)


def literal(value):
    return "'" + str(value).replace("'", "''") + "'"


def dump(path, value):
    path.write_text(json.dumps(value, indent=2, default=str) + "\n")


class Run:
    def __init__(self, args):
        self.args = args
        self.directory = args.artifacts.resolve()
        self.directory.mkdir(parents=True, exist_ok=False)
        self.name = "flow_test_" + uuid.uuid4().hex[:12]
        self.process = None
        self.log = None
        self.generation = 0
        self.report = {"run": self.name, "phases": [], "passed": False}
        with args.binary.open("rb") as executable:
            self.binary_digest = hashlib.file_digest(executable, "sha256").hexdigest()
        self.report["binary"] = {"path": str(args.binary.resolve()), "sha256": self.binary_digest}
        self.pg = psycopg.connect(args.postgres_url, autocommit=True)
        self.pg.execute("SET timezone = 'UTC'")
        self.pg.execute(sql.SQL("SET search_path TO {}, public").format(sql.Identifier(self.name)))
        endpoint = urlparse(args.s3_endpoint)
        if endpoint.scheme not in ("http", "https") or not endpoint.netloc:
            raise ValueError("S3 endpoint must be an http(s) URL")
        self.s3 = boto3.client(
            "s3", endpoint_url=args.s3_endpoint,
            region_name=os.environ.get("AWS_REGION", "us-east-1"),
            config=S3Config(s3={"addressing_style": "path"}),
        )
        self.avro_cache = {}
        self.duck = duckdb.connect()
        load_extensions(self.duck)
        self.duck.execute("SET httpfs_connection_caching = true")
        self.report["reader_settings"] = dict(self.duck.execute(
            "SELECT name, value FROM duckdb_settings() WHERE name IN "
            "('httpfs_connection_caching', 'httpfs_client_implementation', 'http_keep_alive', "
            "'http_retries', 'http_retry_backoff', 'http_retry_wait_ms', 'http_timeout', "
            "'enable_http_metadata_cache', 'enable_external_file_cache') ORDER BY name"
        ).fetchall())
        self.duck.execute("SET timezone = 'UTC'")
        self.duck.execute(
            "CREATE SECRET local_s3 (TYPE s3, KEY_ID {}, SECRET {}, REGION {}, "
            "ENDPOINT {}, URL_STYLE 'path', USE_SSL {})".format(
                literal(os.environ["AWS_ACCESS_KEY_ID"]),
                literal(os.environ["AWS_SECRET_ACCESS_KEY"]),
                literal(os.environ.get("AWS_REGION", "us-east-1")),
                literal(endpoint.netloc), str(endpoint.scheme == "https").lower(),
            )
        )
        self.report["versions"] = {
            "duckdb": duckdb.__version__, "psycopg": psycopg.__version__,
            "boto3": boto3.__version__, "fastavro": fastavro.__version__,
            "postgres": self.pg.execute("SHOW server_version").fetchone()[0],
            "logical_decoding_work_mem": self.pg.execute("SHOW logical_decoding_work_mem").fetchone()[0],
            "extensions": self.duck.execute(
                "SELECT extension_name, extension_version FROM duckdb_extensions() WHERE loaded"
            ).fetchall(),
        }
        self.configure()

    def configure(self):
        columns = [
            ("id", "Int64", False), ("tenant", "Int32", False),
            ("payload", "String", True), ("amount", {"Decimal": {"precision": 18, "scale": 4}}, True),
            ("active", "Bool", True), ("day", "Date", True),
            ("stamp", "TimestampMicros", True), ("token", "Uuid", True),
            ("data", "Binary", True), ("ratio", "Float64", True),
        ]
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
batch_rows = 128
chunk_bytes = 65536
batch_bytes = 262144
pending_transactions = 32
commits_per_second = 100
journal_bytes = 268435456
spool_bytes = 134217728
wal_soft_bytes = 1073741824
wal_hard_bytes = 2147483648
snapshot_retention_secs = 3600
[compaction]
# This fixture asserts delete retirement even for small changes. Production
# defaults deliberately leave low-debt files alone instead of rewriting them.
deleted_rows_percent = 1
delete_files_soft = 1
delete_files_hard = 2
'''
        for table, fields in (("orders", columns), ("accounts", [columns[0], columns[3]])):
            text += f'''\n[[tables]]
source_namespace = "{self.name}"
source_table = "{table}"
target_namespace = ["{self.name}"]
target_table = "{table}"
primary_key = [0]
columns = [\n'''
            for index, (name, kind, nullable) in enumerate(fields, 1):
                data_type = ('{ Decimal = { precision = 18, scale = 4 } }'
                             if isinstance(kind, dict) else json.dumps(kind))
                text += (f'  {{ field_id = {index}, name = "{name}", data_type = {data_type}, '
                         f'nullable = {str(nullable).lower()} }},\n')
            text += "]\n"
        self.config = self.directory / "flow.toml"
        self.config.write_text(text)
        self.environment = os.environ | {"FLOW_LOCAL_POSTGRES_URL": self.args.postgres_url}

    def phase(self, name, action):
        start = time.monotonic()
        print(f"[{name}]", flush=True)
        details = action()
        self.report["phases"].append({"name": name, "seconds": round(time.monotonic() - start, 3), "details": details})
        dump(self.directory / "report.json", self.report)

    def set_compaction_policy(self, values):
        before, section, after = self.config.read_text().partition("[compaction]\n")
        assert section, "base fixture must define compaction policy"
        policy, boundary, rest = after.partition("\n[")
        merged = tomllib.loads(policy) | values
        replacement = "".join(f"{key} = {json.dumps(value)}\n" for key, value in merged.items())
        self.config.write_text(before + section + replacement + ("\n[" + rest if boundary else ""))

    def command(self, command):
        return [str(self.args.binary.resolve()), "--config", str(self.config), command]

    def start(self):
        with self.args.binary.open("rb") as executable:
            if hashlib.file_digest(executable, "sha256").hexdigest() != self.binary_digest:
                raise RuntimeError("test binary changed during the run; use a stable build for reproducible recovery checks")
        self.generation += 1
        self.log = (self.directory / f"daemon-{self.generation}.log").open("wb")
        self.process = subprocess.Popen(self.command("run"), env=self.environment, stdout=self.log, stderr=subprocess.STDOUT)

    def alive(self):
        if self.process is not None and self.process.poll() is not None:
            raise RuntimeError(f"daemon exited {self.process.returncode}; see daemon-{self.generation}.log")

    def stop(self, crash=False):
        if self.process is None:
            return
        if self.process.poll() is None:
            self.process.send_signal(signal.SIGKILL if crash else signal.SIGINT)
            try:
                self.process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=10)
        self.process = None
        self.log.close()
        try:
            self.check_worker_panics()
        except Exception:
            self.report["passed"] = False
            dump(self.directory / "report.json", self.report)
            raise

    def check_worker_panics(self):
        """Check joined processes: a blocking-worker panic can leave exit code zero."""
        paths = sorted(self.directory.glob("daemon-*.log")) + sorted(self.directory.glob("compactor*.log"))
        panics = [{"log": path.name, "line": number, "message": line}
                  for path in paths
                  for number, line in enumerate(path.read_text(errors="replace").splitlines(), 1)
                  if line.startswith("thread '") and " panicked at " in line]
        self.report["worker_panics"] = panics
        if panics:
            raise AssertionError(f"worker panic logged in {panics[0]['log']}:{panics[0]['line']}")

    def until(self, description, condition, timeout=None):
        deadline = time.monotonic() + (timeout or self.args.timeout)
        while time.monotonic() < deadline:
            self.alive()
            result = condition()
            if result:
                return result
            time.sleep(0.25)
        raise TimeoutError(description)

    def metrics(self):
        path = self.directory / "state" / "metrics.prom"
        if not path.exists():
            return {}
        values = {}
        for line in path.read_text().splitlines():
            if not line or line.startswith("#"):
                continue
            name, value = line.rsplit(maxsplit=1)
            # LSNs must not round through IEEE-754 at large WAL positions.
            values[name] = int(value) if value.isdecimal() else float(value)
        return values

    def materialization_barrier(self, barrier):
        # transaction() returns a pre-COMMIT lower bound for negative ACK checks.
        # Another transaction can commit past it before our changes commit.
        # Emit a marker after the caller's COMMIT so source-wide progress past
        # this point proves those changes are visible, even on an idle source.
        assert self.pg.info.transaction_status == psycopg.pq.TransactionStatus.IDLE
        marker = self.pg.execute(
            "SELECT pg_logical_emit_message(true, 'flow-test-barrier', '')::text"
        ).fetchone()[0]
        return max(barrier, lsn(marker))

    def wait_materialized(self, barrier):
        barrier = self.materialization_barrier(barrier)
        def reached():
            metrics = self.metrics()
            return metrics if metrics.get("flow_materialized_lsn", 0) >= barrier else None
        metrics = self.until("source materialization did not reach transaction", reached)
        assert metrics["flow_materialized_lsn"] <= metrics["flow_journal_durable_lsn"] <= metrics["flow_source_received_lsn"]
        return metrics

    def transaction(self, statements):
        with self.pg.transaction():
            for statement in statements:
                self.pg.execute(statement)
            # Lower bound for negative ACK checks, not proof of materialization.
            # Source-wide waits add a marker after COMMIT before checking progress.
            barrier = lsn(self.pg.execute("SELECT pg_current_wal_insert_lsn()::text").fetchone()[0])
        return barrier

    def table(self, name):
        url = f"{self.args.catalog_uri.rstrip('/')}/v1/namespaces/{quote(self.name)}/tables/{quote(name)}"
        with urlopen(url, timeout=15) as response:
            return json.load(response)

    def rows(self, table, metadata=None):
        metadata = metadata or self.table(table)
        return self.duck.execute("SELECT * FROM iceberg_scan(?) ORDER BY id", [metadata["metadata-location"]]).fetchall()

    def compare(self, phase):
        result = {}
        for name in ("orders", "accounts"):
            expected = self.pg.execute(sql.SQL("SELECT * FROM {} ORDER BY id").format(sql.Identifier(name))).fetchall()
            metadata = self.table(name)
            actual = self.rows(name, metadata)
            if actual != expected:
                mismatch = next(((i, a, b) for i, (a, b) in enumerate(zip(actual, expected)) if a != b), None)
                raise AssertionError(f"{phase}: {name} differs: DuckDB={len(actual)}, Postgres={len(expected)}, first={mismatch}")
            dump(self.directory / f"{phase}-{name}-metadata.json", metadata)
            result[name] = {"rows": len(actual), "snapshot": metadata["metadata"]["current-snapshot-id"]}
        return result

    def avro(self, path):
        if path not in self.avro_cache:
            uri = urlparse(path)
            if uri.scheme != "s3":
                raise AssertionError(f"expected S3 artifact, got {path}")
            body = self.s3.get_object(Bucket=uri.netloc, Key=uri.path.lstrip("/"))["Body"].read()
            self.avro_cache[path] = list(fastavro.reader(io.BytesIO(body)))
        return self.avro_cache[path]

    def audit(self, table):
        metadata = self.table(table)["metadata"]
        assert metadata["format-version"] == 2
        operations = set()
        delete_snapshots = 0
        compactions = 0
        current = None
        for snapshot in metadata["snapshots"]:
            summary = snapshot["summary"]
            operation = summary.get("flow.operation-id") or summary["local-test.compaction-id"]
            assert operation not in operations, "same logical operation committed twice"
            operations.add(operation)
            compact = summary.get("streaming.operation") == "compact"
            delete_rewrite = summary.get("streaming.operation") in ("compact-deletes", "repair-delete-dependencies")
            compactions += int(compact)
            assert not (compact or delete_rewrite) or summary["operation"] == "replace"
            removed_delete_sequences, added_delete_sequences = [], []
            live = {}
            for manifest in self.avro(snapshot["manifest-list"]):
                for entry in self.avro(manifest["manifest_path"]):
                    if entry["status"] == 2:
                        if delete_rewrite and entry.get("snapshot_id") == snapshot["snapshot-id"]:
                            assert entry["data_file"]["content"] == 1, "delete rewrite removed a data file"
                            removed_delete_sequences.append(entry["sequence_number"] if entry.get("sequence_number") is not None else manifest["sequence_number"])
                        continue
                    file = entry["data_file"]
                    path = file["file_path"]
                    assert path not in live, "duplicate live file reference"
                    data_sequence = entry.get("sequence_number")
                    if data_sequence is None:
                        data_sequence = manifest["sequence_number"]
                    file_sequence = entry.get("file_sequence_number")
                    if file_sequence is None:
                        file_sequence = manifest["sequence_number"]
                    assert 0 <= data_sequence <= file_sequence <= snapshot["sequence-number"]
                    added_snapshot = entry.get("snapshot_id") or manifest["added_snapshot_id"]
                    if entry["status"] == 1 and added_snapshot == snapshot["snapshot-id"]:
                        assert file_sequence == snapshot["sequence-number"]
                        if delete_rewrite:
                            assert file["content"] == 1, "delete rewrite added a data file"
                            added_delete_sequences.append(data_sequence)
                        elif not compact:
                            assert data_sequence == snapshot["sequence-number"], "RowDelta must share commit sequence"
                    live[path] = file
            if delete_rewrite:
                # Physical delete rewrites preserve the maximum input data
                # sequence; assigning the new commit sequence changes scope.
                assert removed_delete_sequences and added_delete_sequences
                assert set(added_delete_sequences) == {max(removed_delete_sequences)}, "delete rewrite changed its input sequence scope"
            deletes = [file for file in live.values() if file["content"] == 1]
            assert all(file["content"] in (0, 1) for file in live.values())
            delete_snapshots += bool(deletes)
            for file in deletes:
                positions = self.duck.execute("SELECT file_path, pos FROM read_parquet(?)", [file["file_path"]]).fetchall()
                assert positions == sorted(set(positions)), "positions must be sorted and unique"
                assert len(positions) == file["record_count"]
                for path, position in positions:
                    assert path in live and live[path]["content"] == 0
                    assert 0 <= position < live[path]["record_count"]
            if snapshot["snapshot-id"] == metadata["current-snapshot-id"]:
                current = {"data_files": sum(file["content"] == 0 for file in live.values()), "delete_files": len(deletes)}
        return {"snapshots": len(metadata["snapshots"]), "delete_snapshots": delete_snapshots, "compactions": compactions, "current": current}

    def seed(self):
        self.pg.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(self.name)))
        self.pg.execute("""CREATE TABLE orders (
            id bigint PRIMARY KEY, tenant integer NOT NULL, payload text, amount numeric(18,4),
            active boolean, day date, stamp timestamp, token uuid, data bytea, ratio double precision
        )""")
        self.pg.execute("CREATE TABLE accounts (id bigint PRIMARY KEY, amount numeric(18,4))")
        self.pg.execute("ALTER TABLE orders REPLICA IDENTITY FULL")
        self.pg.execute("ALTER TABLE accounts REPLICA IDENTITY FULL")
        self.pg.execute("""INSERT INTO orders SELECT i, i % 11,
            CASE WHEN i % 13 = 0 THEN NULL ELSE 'héllo-世界-' || i END,
            (i - 2000) * 1.0123, i % 2 = 0, date '1960-01-01' + i,
            timestamp '1960-01-01' + i * interval '1.000001 seconds',
            md5(i::text)::uuid, decode(md5(i::text), 'hex'), i * 0.125
            FROM generate_series(1, 4096) i""")
        self.pg.execute("INSERT INTO accounts SELECT i, i * 10.125 FROM generate_series(1, 16) i")
        self.pg.execute(sql.SQL("CREATE PUBLICATION {} FOR TABLE orders, accounts").format(sql.Identifier(self.name)))
        return {"orders": 4096, "accounts": 16}

    def initialize(self):
        baseline = {name: self.pg.execute(sql.SQL("SELECT * FROM {} ORDER BY id").format(sql.Identifier(name))).fetchall()
                    for name in ("orders", "accounts")}
        with (self.directory / "init.log").open("wb") as log:
            process = subprocess.Popen(self.command("init"), env=self.environment, stdout=log, stderr=subprocess.STDOUT)
            try:
                def slot_created():
                    if process.poll() not in (None, 0):
                        raise RuntimeError("initialization failed; see init.log")
                    return self.pg.execute("SELECT 1 FROM pg_replication_slots WHERE slot_name = %s AND confirmed_flush_lsn IS NOT NULL", (self.name,)).fetchone()
                self.until("initial logical slot was not created", slot_created)
                self.handoff_barrier = self.transaction([
                    "INSERT INTO orders (id, tenant, payload) VALUES (7000, 2, 'snapshot-wal-boundary')",
                    "INSERT INTO accounts VALUES (200, 72.5)",
                ])
                if process.wait(timeout=self.args.timeout) != 0:
                    raise RuntimeError("initialization failed; see init.log")
            finally:
                if process.poll() is None:
                    process.kill()
                    process.wait(timeout=10)
        self.initial_metadata = self.table("orders")
        self.initial_rows = self.rows("orders", self.initial_metadata)
        result = {}
        for table, expected in baseline.items():
            metadata = self.table(table)
            actual = self.rows(table, metadata)
            assert actual == expected, f"{table}: initial exported snapshot differs from pre-slot rows"
            dump(self.directory / f"initial-{table}-metadata.json", metadata)
            result[table] = {"rows": len(actual), "snapshot": metadata["metadata"]["current-snapshot-id"]}
        return result

    def handoff(self):
        self.wait_materialized(self.handoff_barrier)
        return self.compare("snapshot-wal-handoff")

    def mutations(self):
        barrier = self.transaction([
            "UPDATE orders SET payload = 'updated-世界', amount = -123.4567, data = decode('00ff', 'hex') WHERE id BETWEEN 1 AND 50",
            "DELETE FROM orders WHERE id BETWEEN 51 AND 75",
            "UPDATE orders SET id = id + 100000 WHERE id BETWEEN 76 AND 85",
            "INSERT INTO orders (id, tenant, payload) VALUES (5000, 7, 'inserted'), (5001, 0, NULL)",
            "UPDATE accounts SET amount = amount + 7.25 WHERE id <= 4",
            "INSERT INTO accounts VALUES (100, 42.125)",
        ])
        self.wait_materialized(barrier)
        result = self.compare("mutations")
        assert self.rows("orders", self.initial_metadata) == self.initial_rows, "retained snapshot changed"
        result["historical_snapshot_rows"] = len(self.initial_rows)
        return result

    def rollback(self):
        self.pg.execute("BEGIN")
        self.pg.execute("DELETE FROM orders")
        self.pg.execute("UPDATE accounts SET amount = 0")
        self.pg.execute("ROLLBACK")
        barrier = self.transaction([
            "INSERT INTO orders (id, tenant, payload) VALUES (6000, 1, 'temporary')",
            "UPDATE orders SET payload = 'collapsed' WHERE id = 6000",
            "DELETE FROM orders WHERE id = 6000",
            "UPDATE accounts SET amount = amount + 1 WHERE id = 1",
        ])
        self.wait_materialized(barrier)
        return self.compare("rollback-collapse")

    def stream(self):
        self.pg.execute("SELECT pg_stat_clear_snapshot()")
        before_count, before_bytes = self.pg.execute(
            "SELECT stream_count, stream_bytes FROM pg_stat_replication_slots WHERE slot_name = %s", (self.name,)
        ).fetchone()
        barrier = self.transaction([
            """INSERT INTO orders (id, tenant, payload) SELECT i, i % 11,
                string_agg(md5((i * 100 + j)::text), '') FROM generate_series(200000, 211999) i
                CROSS JOIN generate_series(1, 20) j GROUP BY i""",
            "UPDATE orders SET amount = 123.4567 WHERE id BETWEEN 200000 AND 200999",
            "DELETE FROM orders WHERE id BETWEEN 211000 AND 211999",
            "UPDATE accounts SET amount = amount - 12.5 WHERE id <= 8",
        ])
        self.wait_materialized(barrier)
        result = self.compare("streamed-transaction")
        result["mutations"] = 14008
        def streamed():
            self.pg.execute("SELECT pg_stat_clear_snapshot()")
            row = self.pg.execute(
                "SELECT stream_count, stream_bytes FROM pg_stat_replication_slots WHERE slot_name = %s", (self.name,)
            ).fetchone()
            return row if row and row[0] > before_count and row[1] - before_bytes >= 12000 * 640 else None
        count, size = self.until("PostgreSQL did not stream the large transaction", streamed)
        result["postgres_stream_count"] = count
        result["postgres_stream_bytes"] = size
        result["postgres_stream_count_delta"] = count - before_count
        result["postgres_stream_bytes_delta"] = size - before_bytes
        return result

    def reconnect(self):
        original = self.pg.execute("SELECT active_pid FROM pg_replication_slots WHERE slot_name = %s", (self.name,)).fetchone()[0]
        assert original is not None
        assert self.pg.execute("SELECT pg_terminate_backend(%s)", (original,)).fetchone()[0]
        barrier = self.transaction([
            "UPDATE orders SET ratio = -0.125 WHERE id = 1",
            "UPDATE accounts SET amount = 11.1111 WHERE id = 1",
        ])
        self.wait_materialized(barrier)
        replacement = self.pg.execute("SELECT active_pid FROM pg_replication_slots WHERE slot_name = %s", (self.name,)).fetchone()[0]
        assert replacement is not None and replacement != original
        result = self.compare("replication-reconnect")
        result["backend_changed"] = True
        return result

    def restart(self):
        before = self.metrics()
        self.stop(crash=True)
        barrier = self.transaction([
            "UPDATE orders SET payload = 'after-crash' WHERE id BETWEEN 200000 AND 200255",
            "DELETE FROM orders WHERE id BETWEEN 200256 AND 200511",
            "INSERT INTO orders (id, tenant, payload) SELECT i, 3, 'backlog' FROM generate_series(300000, 300511) i",
            "UPDATE accounts SET amount = -99.125 WHERE id <= 2",
        ])
        self.start()
        after = self.wait_materialized(barrier)
        assert after["flow_materialized_lsn"] >= before["flow_materialized_lsn"]
        result = self.compare("crash-restart")
        confirmed = self.until("slot ACK did not follow materialization", lambda: (
            value if (value := self.pg.execute(
                "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = %s", (self.name,)
            ).fetchone()[0]) and lsn(value) >= barrier else None
        ))
        # Read the latest source-wide gauge after reading PostgreSQL's ACK.
        assert lsn(confirmed) <= self.metrics()["flow_materialized_lsn"]
        result["confirmed_flush_lsn"] = confirmed
        return result

    def compaction(self):
        def compacted():
            audits = {table: self.audit(table) for table in ("orders", "accounts")}
            return audits if all(audit["compactions"] > 0 and audit["current"]["delete_files"] == 0
                                 for audit in audits.values()) else None
        audits = self.until("idle compactor did not retire positional deletes", compacted)
        result = self.compare("compacted")
        assert audits["orders"]["delete_snapshots"] > 0, "no positional-delete snapshot was exercised"
        assert audits["orders"]["compactions"] > 0
        assert self.rows("orders", self.initial_metadata) == self.initial_rows
        result["metadata_audit"] = audits
        return result

    def truncate(self):
        before = {table: self.table(table)["metadata"]["current-snapshot-id"] for table in ("orders", "accounts")}
        barrier = self.transaction(["TRUNCATE orders", "UPDATE accounts SET amount = 0 WHERE id = 1"])
        try:
            code = self.process.wait(timeout=self.args.timeout)
        except subprocess.TimeoutExpired as error:
            raise AssertionError("unsupported TRUNCATE did not stop capture") from error
        assert code != 0
        message = (self.directory / f"daemon-{self.generation}.log").read_text()
        assert "TRUNCATE requires coordinated table replacement" in message
        confirmed = self.pg.execute("SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = %s", (self.name,)).fetchone()[0]
        assert lsn(confirmed) < barrier, "unsupported source operation was acknowledged"
        after = {table: self.table(table)["metadata"]["current-snapshot-id"] for table in ("orders", "accounts")}
        assert before == after, "unsupported transaction changed a target snapshot"
        return {"exit_code": code, "confirmed_flush_lsn": confirmed, "snapshot_ids_unchanged": True}

    def execute(self):
        try:
            self.phase("seed", self.seed)
            self.phase("initial-copy", self.initialize)
            self.start()
            self.phase("exported-snapshot-to-wal-handoff", self.handoff)
            self.phase("insert-update-delete-pk-move-multitable", self.mutations)
            self.phase("rollback-and-mutation-collapse", self.rollback)
            self.phase("streamed-large-transaction", self.stream)
            self.phase("replication-connection-recovery", self.reconnect)
            self.phase("sigkill-and-wal-backlog-recovery", self.restart)
            self.phase("compaction-and-native-metadata", self.compaction)
            self.phase("unsupported-truncate-fails-before-ack", self.truncate)
            self.report["passed"] = True
        except BaseException as error:
            self.report["failure"] = str(error)
            self.report["traceback"] = traceback.format_exc()
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
    parser.add_argument("--binary", type=Path, default=Path("target/debug/embrasure-flow"))
    parser.add_argument("--artifacts", type=Path, required=True, help="new directory; logs/state retained even on failure")
    parser.add_argument("--timeout", type=float, default=180, help="maximum seconds per wait/init")
    args = parser.parse_args()
    if not args.postgres_url:
        parser.error("provide --postgres-url or FLOW_POSTGRES_URL")
    Run(args).execute()
    print(f"PASS: {args.artifacts.resolve() / 'report.json'}", flush=True)


if __name__ == "__main__":
    main()
