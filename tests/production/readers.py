#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["duckdb==1.5.5", "psycopg[binary]==3.3.5", "boto3==1.43.88", "fastavro==1.12.2"]
# ///
"""Read native CDC tables with Spark and Trino, rewrite them with Spark, then continue CDC."""
import argparse
import datetime
import decimal
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time
import traceback
from urllib.request import urlopen
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "local"))
from run import Run, dump
from container_cleanup import cleanup_container

SPARK_IMAGE = "apache/spark:3.5.6@sha256:599722640c8a42ff5ecca329a3faa5371b3bd5c752b320776d5cdd1eed3441ac"
TRINO_IMAGE = "trinodb/trino:470@sha256:a591436ddc20cef15a4424704423326828591727b0e7c40c6ea4165d4564df67"
ICEBERG_VERSION = "1.10.1"
JARS = {
    "iceberg-spark-runtime-3.5_2.12": "2ff0b9637b671a5a28224c57e8393a1e6a65796ca1fe859b32359f9fe766932534c69d35e4ebcc166b4498242402d9669cea0d6508593c992691c5598089f709",
    "iceberg-aws-bundle": "a649f50fd8508b3e179002ecbc28b3ae3de374c6851ce2ed203fe29d3bbf7794780075bd8ad8f41655d4f8684ff064f58d32218d329b0937bce034199afb900a",
}


def canonical(value):
    if isinstance(value, decimal.Decimal):
        return str(value)
    if isinstance(value, datetime.datetime):
        return value.isoformat(timespec="microseconds")
    if isinstance(value, datetime.date):
        return value.isoformat()
    if isinstance(value, (bytes, bytearray)):
        return value.hex()
    if isinstance(value, uuid.UUID):
        return str(value)
    return value


def canonical_rows(table, rows):
    output = []
    for row in rows:
        values = [canonical(value) for value in row]
        if table == "orders":
            if values[6] is not None:
                values[6] = datetime.datetime.fromisoformat(values[6]).isoformat(timespec="microseconds")
            if values[8] is not None:
                values[8] = values[8].lower()
        output.append(values)
    return output


def order_by(table):
    # Trino displays decimal values as varchar; sort their numeric value in all
    # engines. Both columns define the complete keyless row, including NULLs.
    return "id NULLS FIRST, CAST(amount AS DECIMAL(18,4)) NULLS FIRST" if table == "duplicates" else "id"


class ReaderRun(Run):
    def configure(self):
        super().configure()
        self.reader_tables = ("orders", "accounts") + (("duplicates",) if self.args.keyless_duplicates else ())
        self.set_compaction_policy({"oldest_l0_soft_ms": 60000, "oldest_l0_hard_ms": 300000})
        text = self.config.read_text()
        if self.args.keyless_duplicates:
            text += f'''\n[[tables]]
source_namespace = "{self.name}"
source_table = "duplicates"
target_namespace = ["{self.name}"]
target_table = "duplicates"
primary_key = []
append_only = true
columns = [
  {{ field_id = 1, name = "id", data_type = "Int64", nullable = false }},
  {{ field_id = 2, name = "amount", data_type = {{ Decimal = {{ precision = 18, scale = 4 }} }}, nullable = true }},
]
'''
        self.config.write_text(text)
        self.environment["RUST_LOG"] = "info"
        self.trino_container = None
        self.spark_container = None
        self.last_external = None
        self.report.update(native_compaction=self.args.native_compaction,
                           keyless_duplicates=self.args.keyless_duplicates,
                           compaction_age_ms={"soft": 60000, "hard": 300000})

    def command(self, command):
        result = super().command(command)
        if command == "run" and (not self.args.native_compaction or getattr(self, "protect_rewrite_inputs", False)):
            result += ["--roles", "ingest,coordinator"]
        return result

    def seed(self):
        result = super().seed()
        if self.args.keyless_duplicates:
            self.pg.execute("CREATE TABLE duplicates (id bigint NOT NULL, amount numeric(18,4))")
            self.pg.execute("""INSERT INTO duplicates SELECT i % 7,
                CASE WHEN i % 5 = 0 THEN NULL ELSE (i % 11) * 1.25 END
                FROM generate_series(1, 513) i""")
            self.pg.execute(f'ALTER PUBLICATION "{self.name}" ADD TABLE duplicates')
            result["duplicates"] = 513
        return result

    def initialize(self):
        self.baseline = {table: self.pg.execute(f"SELECT * FROM {table} ORDER BY {order_by(table)}").fetchall()
                         for table in self.reader_tables}
        result = super().initialize()
        if self.args.keyless_duplicates:
            self.initial_duplicates_metadata = self.table("duplicates")
            self.initial_duplicates_rows = self.rows("duplicates", self.initial_duplicates_metadata)
            assert self.initial_duplicates_rows == self.baseline["duplicates"], "COPY changed duplicate multiplicities"
            dump(self.directory / "initial-duplicates-metadata.json", self.initial_duplicates_metadata)
            result["duplicates"] = {"rows": len(self.initial_duplicates_rows),
                                    "snapshot": self.initial_duplicates_metadata["metadata"]["current-snapshot-id"]}
        return result

    def rows(self, table, metadata=None):
        metadata = metadata or self.table(table)
        return self.duck.execute(f"SELECT * FROM iceberg_scan(?) ORDER BY {order_by(table)}",
                                 [metadata["metadata-location"]]).fetchall()

    def prepare_readers(self):
        self.jars = self.args.jar_cache.resolve()
        self.jars.mkdir(parents=True, exist_ok=True)
        for artifact, expected in JARS.items():
            path = self.jars / f"{artifact}-{ICEBERG_VERSION}.jar"
            if not path.exists():
                url = f"https://repo.maven.apache.org/maven2/org/apache/iceberg/{artifact}/{ICEBERG_VERSION}/{path.name}"
                temporary = path.with_suffix(".download")
                with urlopen(url, timeout=60) as source, temporary.open("wb") as target:
                    shutil.copyfileobj(source, target)
                temporary.replace(path)
            with path.open("rb") as source:
                actual = hashlib.file_digest(source, "sha512").hexdigest()
            assert actual == expected, f"Maven artifact checksum mismatch: {path}"
        trino = self.directory / "trino"
        (trino / "catalog").mkdir(parents=True)
        (trino / "config.properties").write_text("""coordinator=true
node-scheduler.include-coordinator=true
http-server.http.port=8080
discovery.uri=http://localhost:8080
query.max-memory=512MB
query.max-memory-per-node=256MB
memory.heap-headroom-per-node=128MB
""")
        (trino / "jvm.config").write_text("""-server
-Xmx1G
-XX:+ExitOnOutOfMemoryError
-XX:ReservedCodeCacheSize=256M
-Dfile.encoding=UTF-8
-Djava.security.manager=allow
""")
        (trino / "node.properties").write_text(f"node.environment=integration\nnode.id={self.name}\nnode.data-dir=/tmp/trino\n")
        (trino / "log.properties").write_text("io.trino=WARN\n")
        (trino / "catalog" / "flow.properties").write_text(f"""connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri={self.args.container_catalog_uri}
iceberg.rest-catalog.warehouse={self.args.warehouse}
fs.native-s3.enabled=true
s3.endpoint={self.args.container_s3_endpoint}
s3.region={os.environ.get('AWS_REGION', 'us-east-1')}
s3.path-style-access=true
s3.aws-access-key={os.environ['AWS_ACCESS_KEY_ID']}
s3.aws-secret-key={os.environ['AWS_SECRET_ACCESS_KEY']}
""")
        self.trino_container = self.name + "-trino"
        subprocess.run(["docker", "run", "-d", "--name", self.trino_container,
                        "--network", self.args.network, "--cpus=2", "--memory=2g",
                        "-v", f"{trino}:/etc/trino:ro", TRINO_IMAGE], check=True, capture_output=True, text=True)
        deadline = time.monotonic() + self.args.timeout
        while time.monotonic() < deadline:
            result = self.trino_sql("SELECT 1 AS ready", check=False)
            if result.returncode == 0:
                break
            time.sleep(1)
        else:
            raise TimeoutError("Trino did not accept a SQL query")
        self.report["versions"].update({"spark_image": SPARK_IMAGE, "trino_image": TRINO_IMAGE,
                                         "iceberg_java": ICEBERG_VERSION, "jar_sha512": JARS})
        return {"trino": "SELECT 1 succeeded", "jars": list(JARS)}

    def trino_sql(self, query, check=True):
        result = subprocess.run(["docker", "exec", self.trino_container, "trino", "--server", "http://localhost:8080",
                                 "--user", "integration", "--output-format", "JSON", "--execute", query],
                                capture_output=True, text=True, timeout=self.args.timeout)
        if check and result.returncode:
            raise RuntimeError(f"Trino query failed: {result.stderr}\n{query}")
        return result

    def spark_sql(self, phase, queries):
        plan = self.directory / f"{phase}-spark-plan.json"
        dump(plan, queries)
        fixtures = Path(__file__).resolve().parent / "reader_fixtures"
        self.spark_container = self.name + "-spark"
        command = ["docker", "run", "--rm", "--name", self.spark_container, "--network", self.args.network,
                   "--cpus=2", "--memory=2g", "-e", "AWS_ACCESS_KEY_ID", "-e", "AWS_SECRET_ACCESS_KEY",
                   "-e", "AWS_REGION", "-e", "TZ=UTC", "-e", "SPARK_LOCAL_IP=127.0.0.1", "-v", f"{fixtures}:/fixtures:ro",
                   "-v", f"{self.jars}:/iceberg:ro", "-v", f"{self.directory}:/artifacts:ro", SPARK_IMAGE,
                   "/opt/spark/bin/spark-submit", "--master", "local[2]", "--driver-memory", "1g",
                   "--jars", ",".join(f"/iceberg/{artifact}-{ICEBERG_VERSION}.jar" for artifact in JARS)]
        config = {
            "spark.ui.enabled": "false", "spark.sql.shuffle.partitions": "2", "spark.sql.session.timeZone": "UTC",
            "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            "spark.sql.catalog.flow": "org.apache.iceberg.spark.SparkCatalog", "spark.sql.catalog.flow.type": "rest",
            "spark.sql.catalog.flow.uri": self.args.container_catalog_uri,
            "spark.sql.catalog.flow.warehouse": self.args.warehouse,
            "spark.sql.catalog.flow.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
            "spark.sql.catalog.flow.s3.endpoint": self.args.container_s3_endpoint,
            "spark.sql.catalog.flow.s3.path-style-access": "true",
        }
        for key, value in config.items():
            command += ["--conf", f"{key}={value}"]
        command += ["/fixtures/spark.py", f"/artifacts/{plan.name}"]
        log_path = self.directory / f"{phase}-spark.log"
        with log_path.open("w") as log:
            result = subprocess.run(command, stdout=log, stderr=subprocess.STDOUT, timeout=self.args.timeout)
        self.spark_container = None
        if result.returncode:
            raise RuntimeError(f"Spark exited {result.returncode}; see {log_path}")
        output = {}
        for line in log_path.read_text().splitlines():
            if line.startswith("FLOW_READER_RESULT="):
                row = json.loads(line.removeprefix("FLOW_READER_RESULT="))
                output[row["name"]] = row
        assert set(output) == {query["name"] for query in queries}, "Spark did not return every requested result"
        return output

    def select(self, engine, table, snapshot=None):
        reference = f"flow.{self.name}.{table}"
        if snapshot is not None:
            reference += f" {'VERSION AS OF' if engine == 'spark' else 'FOR VERSION AS OF'} {snapshot}"
        projection = "*"
        if engine == "trino":
            projection = ("id, tenant, payload, CAST(amount AS varchar) AS amount, active, CAST(day AS varchar) AS day, "
                          "CAST(stamp AS varchar) AS stamp, CAST(token AS varchar) AS token, to_hex(data) AS data, ratio"
                          if table == "orders" else "id, CAST(amount AS varchar) AS amount")
        return f"SELECT {projection} FROM {reference} ORDER BY {order_by(table)}"

    def verify(self, phase):
        comparisons = [(table, table, None) for table in self.reader_tables]
        comparisons.append(("history", "orders", self.initial_metadata))
        if self.args.keyless_duplicates:
            comparisons.append(("duplicates_history", "duplicates", self.initial_duplicates_metadata))
        queries = [{"name": name, "sql": self.select("spark", table,
                    metadata["metadata"]["current-snapshot-id"] if metadata else None)}
                   for name, table, metadata in comparisons]
        spark = self.spark_sql(phase, queries)
        result = {}
        for table, source, metadata in comparisons:
            snapshot = metadata["metadata"]["current-snapshot-id"] if metadata else None
            expected_rows = ((self.initial_rows if source == "orders" else self.initial_duplicates_rows)
                             if metadata else self.baseline[source] if phase == "initial"
                             else self.pg.execute(f"SELECT * FROM {source} ORDER BY {order_by(source)}").fetchall())
            expected = canonical_rows(source, expected_rows)
            actual_spark = canonical_rows(source, spark[table]["rows"])
            trino = self.trino_sql(self.select("trino", source, snapshot))
            (self.directory / f"{phase}-{table}-trino.jsonl").write_text(trino.stdout)
            actual_trino = canonical_rows(source, [list(json.loads(line).values()) for line in trino.stdout.splitlines() if line])
            actual_duck = canonical_rows(source, self.rows(source, metadata))
            for engine, actual in (("Spark", actual_spark), ("Trino", actual_trino), ("DuckDB", actual_duck)):
                if actual != expected:
                    mismatch = next(((i, a, e) for i, (a, e) in enumerate(zip(actual, expected)) if a != e), None)
                    raise AssertionError(f"{phase} {table} {engine}: rows={len(actual)} expected={len(expected)} first={mismatch}")
            result[table] = {"rows": len(expected), "readers": ["Spark", "Trino", "DuckDB"],
                             "snapshot": snapshot if metadata else self.table(source)["metadata"]["current-snapshot-id"]}
        return result

    def wait_native_compaction(self, table, after_sequence):
        def completed():
            metadata = self.table(table)
            snapshots = [snapshot for snapshot in metadata["metadata"]["snapshots"]
                         if snapshot["sequence-number"] > after_sequence
                         and snapshot["summary"].get("streaming.writer-id") == "embrasure-flow"
                         and snapshot["summary"].get("streaming.operation") == "compact"
                         and int(snapshot["summary"].get("deleted-data-files", 0)) > 0]
            if snapshots:
                dump(self.directory / f"native-{table}-after-{after_sequence}-metadata.json", metadata)
                return {"table": table, "after_sequence": after_sequence,
                        "snapshots": snapshots}
            return None
        return self.until(f"native compaction did not rewrite {table} after sequence {after_sequence}", completed)

    def prepare_delete_rewrite(self):
        # Joining the native owner fences its in-flight work before new deletes
        # are produced. Keep this role set until Spark has consumed the inputs.
        if self.args.native_compaction:
            self.stop()
            self.protect_rewrite_inputs = True
            self.start()
        barrier = self.transaction(["UPDATE orders SET payload='fresh-spark-delete-input' WHERE id BETWEEN 200 AND 207"])
        self.wait_materialized(barrier)
        metadata = self.table("orders")["metadata"]
        snapshot = next(item for item in metadata["snapshots"]
                        if item["snapshot-id"] == metadata["current-snapshot-id"])
        deletes = [entry["data_file"] for manifest in self.avro(snapshot["manifest-list"])
                   for entry in self.avro(manifest["manifest_path"])
                   if entry["status"] != 2 and entry["data_file"]["content"] == 1]
        assert sum(file["record_count"] for file in deletes) > 0, "fresh cohort produced no live position deletes"
        return {"barrier": barrier, "snapshot": snapshot["snapshot-id"], "protected_delete_files": deletes}

    def restore_native(self):
        if self.args.native_compaction and getattr(self, "protect_rewrite_inputs", False):
            self.stop()
            self.protect_rewrite_inputs = False
            self.start()

    def rewrite(self, procedure, table="orders"):
        # Give Spark stable rewrite inputs. A concurrent native rewrite can
        # legitimately invalidate Spark's optimistic plan. Native maintenance
        # resumes before the following CDC and independent-reader assertions.
        if self.args.native_compaction and not getattr(self, "protect_rewrite_inputs", False):
            self.stop()
            self.protect_rewrite_inputs = True
            self.start()
        before = self.table(table)
        before_ids = {snapshot["snapshot-id"] for snapshot in before["metadata"]["snapshots"]}
        arguments = f"table => '{self.name}.{table}'"
        if procedure != "rewrite_manifests":
            arguments += ", options => map('rewrite-all', 'true')"
        else:
            arguments += ", use_caching => false"
        phase = procedure if table == "orders" else f"{procedure}-{table}"
        dump(self.directory / f"{phase}-before-metadata.json", before)
        output = self.spark_sql(phase, [{"name": "rewrite", "sql": f"CALL flow.system.{procedure}({arguments})"}])["rewrite"]
        assert len(output["rows"]) == 1, f"unexpected Spark procedure result: {output}"
        result = dict(zip(output["columns"], output["rows"][0]))
        input_count = {"rewrite_position_delete_files": "rewritten_delete_files_count",
                       "rewrite_data_files": "rewritten_data_files_count",
                       "rewrite_manifests": "rewritten_manifests_count"}[procedure]
        assert result[input_count] > 0, f"{procedure} rewrote no input files: {output}"
        assert result.get("failed_data_files_count", 0) == 0, f"Spark left failed rewrite groups: {output}"
        after = self.table(table)
        dump(self.directory / f"{phase}-after-metadata.json", after)
        assert before["metadata"]["current-snapshot-id"] != after["metadata"]["current-snapshot-id"], \
            f"{procedure} did not advance the catalog head: {result}"
        new_snapshots = [snapshot for snapshot in after["metadata"]["snapshots"]
                         if snapshot["snapshot-id"] not in before_ids]
        # This fixture owns an isolated namespace; Spark is its only external
        # writer. Native snapshots cannot satisfy the external rewrite proof.
        external = [snapshot for snapshot in new_snapshots
                    if snapshot["summary"].get("streaming.writer-id") != "embrasure-flow"
                    and snapshot["summary"]["operation"] == "replace"]
        assert external, f"{procedure} produced no attributable external replace snapshot: {result}"
        self.last_external = {"table": table, "sequence": max(snapshot["sequence-number"] for snapshot in external)}
        return {"before": before["metadata"]["current-snapshot-id"],
                "after": after["metadata"]["current-snapshot-id"], "table": table,
                "result": result, "external_snapshots": external,
                "concurrent_native_snapshots": [snapshot for snapshot in new_snapshots
                    if snapshot["summary"].get("streaming.writer-id") == "embrasure-flow"]}

    def followup(self, number, restart=False):
        if restart:
            self.stop(crash=True)
        statements = [
            f"UPDATE orders SET payload = 'after-external-{number}', amount = {number}.125 WHERE id BETWEEN 100 AND 110",
            f"DELETE FROM orders WHERE id = {110 + number}",
            f"UPDATE orders SET id = {8000 + number} WHERE id = {120 + number}",
            f"INSERT INTO orders (id, tenant, payload) VALUES ({9000 + number}, 4, 'new-after-rewrite-{number}')",
            f"UPDATE accounts SET amount = amount + {number} WHERE id = 1",
        ]
        if self.args.keyless_duplicates:
            statements.append("INSERT INTO duplicates VALUES (2, 2.5), (2, 2.5), (2, NULL), (0, NULL), (9, 11.25)")
        barrier = self.transaction(statements)
        if restart:
            self.start()
        self.wait_materialized(barrier)
        native = (self.wait_native_compaction(self.last_external["table"], self.last_external["sequence"])
                  if self.args.native_compaction else None)
        result = self.verify(f"after-rewrite-{number}")
        if native:
            result["native_compaction"] = native
        return result

    def execute(self):
        try:
            self.phase("reader-runtimes", self.prepare_readers)
            self.phase("seed", self.seed)
            self.phase("initial-copy", self.initialize)
            self.phase("initial-independent-readers-and-history", lambda: self.verify("initial"))
            self.start()
            self.phase("snapshot-wal-handoff", self.handoff)
            self.phase("crud", self.mutations)
            self.phase("crud-independent-readers-and-history", lambda: self.verify("crud"))
            if self.args.native_compaction:
                initial = next(snapshot for snapshot in self.initial_metadata["metadata"]["snapshots"]
                               if snapshot["snapshot-id"] == self.initial_metadata["metadata"]["current-snapshot-id"])
                self.phase("native-compaction-before-external", lambda: self.wait_native_compaction("orders", initial["sequence-number"]))
            for number, procedure in enumerate(("rewrite_position_delete_files", "rewrite_data_files", "rewrite_manifests"), 1):
                if procedure == "rewrite_position_delete_files":
                    self.phase("fresh-protected-delete-inputs", self.prepare_delete_rewrite)
                self.phase(procedure, lambda procedure=procedure: self.rewrite(procedure))
                self.restore_native()
                self.phase(f"cdc-after-{procedure}", lambda number=number: self.followup(number, restart=number == 3))
            if self.args.keyless_duplicates:
                self.phase("keyless-external-data-rewrite", lambda: self.rewrite("rewrite_data_files", "duplicates"))
                self.restore_native()
                self.phase("keyless-append-and-restart", lambda: self.followup(4, restart=True))
            self.stop()
            self.check_worker_panics()
            self.report["passed"] = True
        except BaseException as error:
            self.report["failure"] = str(error)
            self.report["traceback"] = traceback.format_exc()
            raise
        finally:
            original_failure = sys.exc_info()[0] is not None
            try:
                self.stop()
            except Exception as error:
                self.report.update(passed=False, cleanup_error=str(error))
            self.report["container_cleanup"] = [
                cleanup_container(container, self.directory / f"{container}.log")
                for container in (self.spark_container, self.trino_container) if container]
            failed = self.report.get("cleanup_error") or any(not item["passed"] for item in self.report["container_cleanup"])
            if failed:
                self.report["passed"] = False
            self.pg.close()
            self.duck.close()
            dump(self.directory / "report.json", self.report)
            if failed and not original_failure:
                raise RuntimeError("reader fixture cleanup failed; see report.json")



def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--postgres-url", default=os.environ.get("FLOW_POSTGRES_URL"))
    parser.add_argument("--catalog-uri", required=True)
    parser.add_argument("--s3-endpoint", required=True)
    parser.add_argument("--warehouse", default="s3://warehouse/")
    parser.add_argument("--binary", type=Path, default=Path("target/debug/embrasure-flow"))
    parser.add_argument("--artifacts", type=Path, required=True)
    parser.add_argument("--timeout", type=float, default=240)
    parser.add_argument("--network", default="flow-production-local_default")
    parser.add_argument("--container-catalog-uri", default="http://rest:8181")
    parser.add_argument("--container-s3-endpoint", default="http://minio:9000")
    parser.add_argument("--jar-cache", type=Path, default=Path("target/reader-jars"))
    parser.add_argument("--native-compaction", action="store_true",
                        help="require native compaction before and after independently attributed Spark rewrites")
    parser.add_argument("--keyless-duplicates", action="store_true",
                        help="include an append-only table with duplicate complete rows and retained history")
    args = parser.parse_args()
    if not args.postgres_url:
        parser.error("provide --postgres-url or FLOW_POSTGRES_URL")
    ReaderRun(args).execute()
    print(f"PASS: {args.artifacts.resolve() / 'report.json'}", flush=True)


if __name__ == "__main__":
    main()
