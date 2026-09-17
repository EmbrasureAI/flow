#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["duckdb==1.5.5", "psycopg[binary]==3.3.5", "boto3==1.43.88", "fastavro==1.12.2"]
# ///
"""Many-column COPY, wide CDC transactions, null changes and same-state recovery."""
import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import subprocess
import sys
import time
import traceback

from psycopg import sql

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "local"))
from run import Run, dump, lsn
from process_lifecycle import run_supervised


class WideRun(Run):
    def configure(self):
        super().configure()
        # Use the normal production byte budgets, not the smoke fixture's 64 KiB
        # chunks. Keep its small total disk quotas to exercise rolling cleanup.
        prefix = self.config.read_text().split("[[tables]]")[0]
        prefix = prefix.replace("chunk_bytes = 65536", "chunk_bytes = 4194304")
        prefix = prefix.replace("batch_bytes = 262144", "batch_bytes = 8388608")
        text = prefix + f'''[[tables]]
source_namespace = "{self.name}"
source_table = "orders"
target_namespace = ["{self.name}"]
target_table = "orders"
primary_key = [0]
columns = [
{{ field_id = 1, name = "id", data_type = "Int64", nullable = false }},
{{ field_id = 2, name = "version", data_type = "Int64", nullable = false }},
'''
        self.fields = [f"c{i:04}" for i in range(self.args.columns)]
        for i, name in enumerate(self.fields, 3):
            text += f'{{ field_id = {i}, name = "{name}", data_type = "String", nullable = true }},\n'
        self.config.write_text(text + "]\n")
        self.environment["RUST_LOG"] = "info"

    def seed(self):
        self.pg.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(self.name)))
        self.pg.execute("CREATE TABLE orders (id bigint PRIMARY KEY, version bigint NOT NULL, " +
                        ",".join(f"{name} text" for name in self.fields) + ")")
        self.pg.execute("ALTER TABLE orders REPLICA IDENTITY FULL")
        # Each value varies by column and row. MD5 blocks are deterministic hex,
        # not incompressible bytes. NULLs exercise nullable Arrow arrays.
        expressions = [f"CASE WHEN (i+{j}) % 17=0 THEN NULL ELSE "
                       f"left(repeat(md5(i::text || ':{j}'),{(self.args.value_bytes+31)//32}),{self.args.value_bytes}) END"
                       for j in range(self.args.columns)]
        self.pg.execute("INSERT INTO orders SELECT i,0," + ",".join(expressions) +
                        f" FROM generate_series(1,{self.args.rows}) i")
        self.pg.execute(sql.SQL("CREATE PUBLICATION {} FOR TABLE orders").format(sql.Identifier(self.name)))
        return {"rows": self.args.rows, "columns": self.args.columns + 2,
                "maximum_text_bytes_per_row": self.args.columns * self.args.value_bytes,
                "value_bytes": self.args.value_bytes}

    def compare(self, label):
        metadata = self.table("orders")
        # Stream both ordered results so the reader does not hold two copies of
        # the entire wide table in Python memory. Equality includes every cell.
        count = 0
        digest = hashlib.sha256()
        with self.pg.transaction(), self.pg.cursor(name="wide_verify") as source:
            source.execute("SELECT * FROM orders ORDER BY id")
            target = self.duck.execute("SELECT * FROM iceberg_scan(?) ORDER BY id",
                                       [metadata["metadata-location"]])
            while True:
                expected, actual = source.fetchmany(32), target.fetchmany(32)
                assert actual == expected, f"{label}: rows differ at batch starting {count}"
                if not expected:
                    break
                for row in expected:
                    digest.update(json.dumps(row, separators=(",", ":"), ensure_ascii=False).encode() + b"\n")
                count += len(expected)
        dump(self.directory / f"{label}-metadata.json", metadata)
        return {"rows": count, "sha256": digest.hexdigest(), "all_columns_equal": True}

    def initialize(self):
        with (self.directory / "init.log").open("wb") as log:
            subprocess.run(self.command("init"), env=self.environment, stdout=log,
                           stderr=subprocess.STDOUT, timeout=self.args.timeout, check=True)
        return self.compare("initial-copy")

    def churn(self):
        started = time.monotonic()
        for n in range(self.args.transactions):
            column = self.fields[n % len(self.fields)]
            lo = (n * self.args.transaction_rows) % self.args.rows + 1
            hi = min(self.args.rows, lo + self.args.transaction_rows - 1)
            # Most wide values stay unchanged; some transition NULL -> text and
            # text -> NULL. The complete source transaction stays atomic.
            value = "NULL" if n % 3 == 0 else f"'generation-{n}-世界'"
            self.barrier = self.transaction([
                f"UPDATE orders SET version=version+1,{column}={value} WHERE id BETWEEN {lo} AND {hi}"
            ])
        admitted = time.monotonic() - started
        self.wait_materialized(self.barrier)
        result = self.compare("wide-cdc")
        result.update(transactions=self.args.transactions, source_seconds=admitted,
                      source_and_drain_seconds=time.monotonic()-started)
        return result

    def restart(self):
        identity = (self.directory / "state/source-identity.json").read_bytes()
        self.stop(crash=True)
        self.barrier = self.transaction([
            "UPDATE orders SET version=version+1 WHERE id % 7=0",
            f"UPDATE orders SET id=id+{self.args.rows} WHERE id % 11=0",
            "DELETE FROM orders WHERE id % 13=0",
            f"INSERT INTO orders (id,version,{self.fields[-1]}) VALUES (-1,1,'after-crash-世界')",
        ])
        self.start()
        self.wait_materialized(self.barrier)
        assert (self.directory / "state/source-identity.json").read_bytes() == identity
        result = self.compare("same-state-restart")
        def acknowledged():
            confirmed = self.pg.execute("SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name=%s",
                                        (self.name,)).fetchone()[0]
            return confirmed if lsn(confirmed) >= self.barrier else None
        result["confirmed_flush_lsn"] = self.until("source acknowledgement did not catch up", acknowledged)
        assert lsn(result["confirmed_flush_lsn"]) <= self.metrics()["flow_materialized_lsn"]
        return result

    def execute(self):
        try:
            self.phase("seed", self.seed)
            self.phase("many-column-initial-copy", self.initialize)
            self.start()
            self.phase("wide-transactions-null-changes", self.churn)
            self.phase("sigkill-key-moves-deletes-and-restart", self.restart)
            self.stop()
            self.report["passed"] = True
        except BaseException as error:
            self.report.update(failure=str(error), traceback=traceback.format_exc())
            raise
        finally:
            try:
                self.stop()
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
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--artifacts", type=Path, required=True)
    parser.add_argument("--timeout", type=float, default=300, help="hard deadline for the complete suite in seconds")
    parser.add_argument("--supervised-worker", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--columns", type=int, default=256)
    parser.add_argument("--rows", type=int, default=1000)
    parser.add_argument("--value-bytes", type=int, default=256)
    parser.add_argument("--transactions", type=int, default=12)
    parser.add_argument("--transaction-rows", type=int, default=200)
    args = parser.parse_args()
    if not args.postgres_url or not 1 <= args.columns <= 1500:
        parser.error("PostgreSQL URL and 1–1500 text columns are required")
    if (min(args.rows, args.value_bytes, args.transactions, args.transaction_rows) <= 0
            or not math.isfinite(args.timeout) or args.timeout <= 0):
        parser.error("row, value, transaction and timeout bounds must be positive")
    if not args.supervised_worker:
        return run_supervised([sys.executable, __file__, *sys.argv[1:], "--supervised-worker"],
                              args.artifacts, args.timeout).returncode
    WideRun(args).execute()
    print(f"PASS: {args.artifacts.resolve() / 'report.json'}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
