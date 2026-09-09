#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["duckdb==1.5.5", "psycopg[binary]==3.3.5", "boto3==1.43.88", "fastavro==1.12.2"]
# ///
"""Explicit projection across COPY/WAL, excluded TOAST/DDL and durable restarts."""
import argparse
import os
from pathlib import Path
import subprocess
import sys
import traceback

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "local"))
from run import Run, dump
from psycopg import sql


class ProjectionRun(Run):
    def configure(self):
        super().configure()
        prefix = self.config.read_text().split("[[tables]]")[0]
        self.config.write_text(prefix + f'''[[tables]]
source_namespace = "{self.name}"
source_table = "orders"
target_namespace = ["{self.name}"]
target_table = "orders"
column_selection = "explicit"
primary_key = [0]
columns = [
{{ field_id = 1, name = "id", data_type = "Int32", nullable = false }},
{{ field_id = 2, name = "value", data_type = "String", nullable = true }},
]
''')

    def seed(self):
        self.pg.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(self.name)))
        self.pg.execute("CREATE TYPE opaque AS (x integer)")
        self.pg.execute("CREATE TABLE orders (ignored opaque, id integer PRIMARY KEY, payload text, generated integer GENERATED ALWAYS AS (id * 2) STORED, value text)")
        self.pg.execute("ALTER TABLE orders REPLICA IDENTITY FULL")
        self.pg.execute("INSERT INTO orders (ignored,id,payload,value) SELECT ROW(i)::opaque, i, (SELECT string_agg(md5((i*1000+j)::text), '') FROM generate_series(1,100) j), 'value-'||i FROM generate_series(1,128) i")
        options = " WITH (publish_generated_columns=stored)" if int(self.pg.execute("SHOW server_version_num").fetchone()[0]) >= 180000 else ""
        self.pg.execute(sql.SQL("CREATE PUBLICATION {} FOR TABLE orders" + options).format(sql.Identifier(self.name)))
        return {"rows": 128, "server_version": self.pg.execute("SHOW server_version_num").fetchone()[0]}

    def initialize(self):
        # Missing PK must fail before allocating a replication slot or copying rows.
        original = self.config.read_text()
        self.config.write_text(original.replace('name = "id"', 'name = "payload"').replace('data_type = "Int32"', 'data_type = "String"'))
        result = subprocess.run(self.command("init"), env=self.environment, capture_output=True, timeout=self.args.timeout)
        assert result.returncode != 0 and b"complete primary key" in result.stderr, result.stderr.decode()
        assert not self.pg.execute("SELECT 1 FROM pg_replication_slots WHERE slot_name=%s", (self.name,)).fetchone()
        self.config.write_text(original)
        if int(self.pg.execute("SHOW server_version_num").fetchone()[0]) >= 150000:
            # Catalog generated-column normalization must not hide omitted
            # ordinary columns, even when those columns are excluded in Flow.
            self.pg.execute(sql.SQL("ALTER PUBLICATION {} SET TABLE orders (id, value)").format(sql.Identifier(self.name)))
            result = subprocess.run(self.command("init"), env=self.environment, capture_output=True, timeout=self.args.timeout)
            assert result.returncode != 0 and b"publication must include every current source column" in result.stderr, result.stderr.decode()
            assert not self.pg.execute("SELECT 1 FROM pg_replication_slots WHERE slot_name=%s", (self.name,)).fetchone()
            self.pg.execute(sql.SQL("ALTER PUBLICATION {} SET TABLE orders").format(sql.Identifier(self.name)))
        if int(self.pg.execute("SHOW server_version_num").fetchone()[0]) >= 180000:
            self.pg.execute(sql.SQL("ALTER PUBLICATION {} SET (publish_generated_columns=none)").format(sql.Identifier(self.name)))
            result = subprocess.run(self.command("init"), env=self.environment, capture_output=True, timeout=self.args.timeout)
            assert result.returncode != 0 and b"requires publish_generated_columns=stored" in result.stderr, result.stderr.decode()
            assert not self.pg.execute("SELECT 1 FROM pg_replication_slots WHERE slot_name=%s", (self.name,)).fetchone()
            self.pg.execute(sql.SQL("ALTER PUBLICATION {} SET (publish_generated_columns=stored)").format(sql.Identifier(self.name)))
        with (self.directory / "init.log").open("wb") as log:
            subprocess.run(self.command("init"), env=self.environment, stdout=log, stderr=subprocess.STDOUT, check=True, timeout=self.args.timeout)
        self.initial_metadata = self.table("orders")
        self.initial_rows = self.rows("orders", self.initial_metadata)
        return self.compare("snapshot")

    def compare(self, phase):
        expected = self.pg.execute("SELECT id,value FROM orders ORDER BY id").fetchall()
        metadata = self.table("orders")
        actual = self.rows("orders", metadata)
        assert actual == expected, f"{phase}: full selected rows differ"
        fields = next(s["fields"] for s in metadata["metadata"]["schemas"] if s["schema-id"] == metadata["metadata"]["current-schema-id"])
        assert [f["name"] for f in fields] == ["id", "value"]
        return {"rows": len(actual), "snapshot": metadata["metadata"]["current-snapshot-id"]}

    def changes(self):
        barrier = self.transaction([
            "UPDATE orders SET ignored=ROW(999)::opaque WHERE id<=16",
            "UPDATE orders SET value='changed', id=id+1000 WHERE id<=8",
            "DELETE FROM orders WHERE id BETWEEN 90 AND 100",
            "INSERT INTO orders (id,value) VALUES (2000,NULL)",
        ])
        self.wait_materialized(barrier)
        return self.compare("cdc")

    def excluded_ddl(self):
        # Dropping an earlier field changes wire offsets, never source attnums.
        self.pg.execute("ALTER TABLE orders DROP COLUMN ignored")
        self.pg.execute("ALTER TABLE orders ADD COLUMN extra opaque")
        barrier = self.transaction(["UPDATE orders SET extra=ROW(1)::opaque, value='after-ddl' WHERE id<30"])
        self.wait_materialized(barrier)
        result = self.compare("excluded-ddl")
        self.stop(crash=True)
        barrier = self.transaction(["UPDATE orders SET value='offline-change' WHERE id<30"])
        self.start()
        self.wait_materialized(barrier)
        return result | {"restart": self.compare("restart")}

    def compact(self):
        self.until("native compaction did not publish", lambda: any(s.get("summary", {}).get("streaming.operation") == "compact" for s in self.table("orders")["metadata"]["snapshots"]))
        assert self.rows("orders", self.initial_metadata) == self.initial_rows, "retained snapshot changed"
        return self.compare("compaction")

    def selected_ddl(self):
        self.stop()
        previous = self.rows("orders", self.table("orders"))
        self.pg.execute("ALTER TABLE orders DROP COLUMN value")
        self.pg.execute("ALTER TABLE orders ADD COLUMN value text")
        self.pg.execute("UPDATE orders SET value='replacement'")
        result = subprocess.run(self.command("run"), env=self.environment, capture_output=True, timeout=self.args.timeout)
        assert result.returncode != 0 and b"dropped or replaced" in result.stderr, result.stderr.decode()
        assert self.rows("orders", self.table("orders")) == previous
        return {"selected_column_replacement_rejected": True, "published_rows_unchanged": True}

    def frozen_mode(self):
        self.stop()
        original = self.config.read_text()
        self.config.write_text(original.replace('column_selection = "explicit"', 'column_selection = "all_current"'))
        result = subprocess.run(self.command("run"), env=self.environment, capture_output=True, timeout=self.args.timeout)
        self.config.write_text(original)
        assert result.returncode != 0 and b"selection mode differs" in result.stderr, result.stderr.decode()
        self.start()
        return {"mode_mutation_rejected": True}

    def replacement_generation(self):
        # Recover the rejected selected-column replacement using isolated state,
        # a new slot and target. The old public snapshot remains readable.
        old = self.table("orders")
        old_rows = self.rows("orders", old)
        text = self.config.read_text()
        text = text.replace(str(self.directory / "state"), str(self.directory / "replacement-state"))
        text = text.replace(f'id = "{self.name}"', f'id = "{self.name}_replacement"')
        text = text.replace(f'slot = "{self.name}"', f'slot = "{self.name}_replacement"')
        text = text.replace('target_table = "orders"', 'target_table = "orders_replacement"')
        self.config.write_text(text)
        with (self.directory / "replacement-init.log").open("wb") as log:
            subprocess.run(self.command("init"), env=self.environment, stdout=log, stderr=subprocess.STDOUT, check=True, timeout=self.args.timeout)
        new = self.table("orders_replacement")
        assert new["metadata"]["table-uuid"] != old["metadata"]["table-uuid"]
        expected = self.pg.execute("SELECT id,value FROM orders ORDER BY id").fetchall()
        assert self.rows("orders_replacement", new) == expected
        self.start()
        self.transaction(["UPDATE orders SET value='replacement-cdc' WHERE id<30"])
        expected = self.pg.execute("SELECT id,value FROM orders ORDER BY id").fetchall()
        self.until("replacement generation CDC did not catch up", lambda: self.rows("orders_replacement") == expected)
        self.stop(crash=True)
        self.transaction(["UPDATE orders SET value='replacement-offline' WHERE id<30"])
        self.start()
        expected = self.pg.execute("SELECT id,value FROM orders ORDER BY id").fetchall()
        self.until("replacement generation restart did not catch up", lambda: self.rows("orders_replacement") == expected)
        assert self.rows("orders", self.table("orders")) == old_rows
        return {"rows": len(expected), "fresh_target_uuid": True, "snapshot_cdc_restart": True, "prior_target_unchanged": True}

    def execute(self):
        try:
            for name, method in [("seed",self.seed),("snapshot",self.initialize)]: self.phase(name,method)
            self.start()
            for name, method in [("cdc",self.changes),("compaction",self.compact),("excluded-ddl-restart",self.excluded_ddl),("frozen-mode",self.frozen_mode),("selected-ddl",self.selected_ddl),("replacement-generation",self.replacement_generation)]: self.phase(name,method)
            self.report["passed"] = True
        except Exception:
            self.report["error"] = traceback.format_exc()
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
    parser.add_argument("--artifacts", type=Path, required=True)
    parser.add_argument("--timeout", type=float, default=180)
    args = parser.parse_args()
    if not args.postgres_url: parser.error("provide --postgres-url or FLOW_POSTGRES_URL")
    ProjectionRun(args).execute()
    print(f"PASS: {args.artifacts / 'report.json'}", flush=True)

if __name__ == "__main__": main()
