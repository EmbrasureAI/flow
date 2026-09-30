#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["duckdb==1.5.5", "psycopg[binary]==3.3.5", "boto3==1.43.88", "fastavro==1.12.2"]
# ///
"""Fivetran-style PG mappings through COPY, CDC, restart, compaction and Iceberg reads."""
import argparse
from decimal import Decimal
import json
import os
from pathlib import Path
import subprocess
import sys
import traceback

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "local"))
from run import Run, dump
from psycopg import sql


class TypeRun(Run):
    def configure(self):
        super().configure()
        self.generated = int(self.pg.execute("SHOW server_version_num").fetchone()[0]) >= 180000
        self.columns = [
            ("id", "String"), ("doc", "String"), ("json_raw", "String"),
            ("tags", "String"), ("matrix", "String"), ("state", "String"),
            ("states", "String"), ("quantity", "Int32"), ("amount", "String"),
            ("amounts", "String"), ("domain_doc", "String"), ("domain_ids", "String"),
            ("payload", "String"), ("domain_arrays", "String"),
        ]
        if self.generated:
            self.columns.append(("computed", "Int32"))
        prefix = self.config.read_text().split("[[tables]]")[0]
        text = prefix + f'''[[tables]]
source_namespace = "{self.name}"
source_table = "orders"
target_namespace = ["{self.name}"]
target_table = "orders"
primary_key = [0]
columns = [
'''
        for index, (name, kind) in enumerate(self.columns, 1):
            text += f'{{ field_id = {index}, name = "{name}", data_type = "{kind}", nullable = {str(index != 1).lower()} }},\n'
        self.config.write_text(text + "]\n")
        self.environment["RUST_LOG"] = "info"

    def seed(self):
        self.pg.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(self.name)))
        self.pg.execute("CREATE TYPE status AS ENUM ('ready', 'waiting')")
        self.pg.execute("CREATE DOMAIN positive_int AS integer CHECK (VALUE > 0)")
        self.pg.execute("CREATE DOMAIN nested_int AS positive_int")
        self.pg.execute("CREATE DOMAIN json_doc AS jsonb")
        self.pg.execute("CREATE DOMAIN uuid_value AS uuid")
        self.pg.execute("CREATE DOMAIN int_list AS integer[]")
        generated = ", computed integer GENERATED ALWAYS AS (quantity * 2) STORED" if self.generated else ""
        self.pg.execute("""CREATE TABLE orders (
            id uuid PRIMARY KEY, doc jsonb, json_raw json, tags text[], matrix integer[][],
            state status, states status[], quantity nested_int, amount numeric,
            amounts numeric[], domain_doc json_doc, domain_ids uuid_value[], payload text, domain_arrays int_list[]
        """ + generated + ")")
        self.pg.execute("ALTER TABLE orders REPLICA IDENTITY FULL")
        self.pg.execute("""INSERT INTO orders (id, doc, json_raw, tags, matrix, state, states, quantity, amount, amounts, domain_doc, domain_ids, payload)
            SELECT md5(i::text)::uuid,
                '{"nested":{"value":123456789012345678901234567890.123456789},"null":null}'::jsonb,
                '{"duplicate":1,"duplicate":2,"unicode":"世界"}'::json,
                ARRAY['NULL', NULL, 'quote"slash\\', '世界'],
                '[0:1][3:4]={{1,2},{3,4}}'::integer[], 'ready', ARRAY['ready','waiting',NULL]::status[],
                i, 12345678901234567890123456789012345678901234567890.000001,
                ARRAY[12345678901234567890123456789012345678901234567890.000001, NULL, -0.0001]::numeric[],
                '{"domain":true}'::jsonb, ARRAY[md5(i::text)::uuid,NULL]::uuid_value[],
                (SELECT string_agg(md5((i * 1000 + j)::text), '') FROM generate_series(1,100) j)
            FROM generate_series(1,128) i""")
        self.pg.execute("UPDATE orders SET doc=doc || jsonb_build_object('large', payload)")
        # Keep SQL NULL, JSON null and the JSON string "null" distinct through
        # COPY, CDC, restart and compaction, including top-level scalar values.
        self.pg.execute("""UPDATE orders SET doc = CASE quantity % 6
            WHEN 0 THEN NULL WHEN 1 THEN 'null'::jsonb WHEN 2 THEN '"null"'::jsonb
            WHEN 3 THEN 'false'::jsonb WHEN 4 THEN '[]'::jsonb
            ELSE '123456789012345678901234567890.123456789'::jsonb END
            WHERE quantity <= 12""")
        self.pg.execute("UPDATE orders SET json_raw=doc::json WHERE quantity <= 12")
        self.pg.execute("UPDATE orders SET domain_arrays=ARRAY[ARRAY[42,NULL]::int_list,NULL::int_list,ARRAY[]::int_list]")
        options = " WITH (publish_generated_columns = stored)" if self.generated else ""
        self.pg.execute(sql.SQL("CREATE PUBLICATION {} FOR TABLE orders" + options).format(sql.Identifier(self.name)))
        return {"rows": 128, "stored_generated": self.generated}

    def initialize(self):
        if ("computed", "Int32") in self.columns:
            # Append-only tables need no FULL identity, but COPY must still use
            # only columns that the publication will send through CDC.
            original = self.config.read_text()
            self.config.write_text(original.replace("primary_key = [0]", "append_only = true\nprimary_key = [0]"))
            self.pg.execute("ALTER TABLE orders REPLICA IDENTITY DEFAULT")
            self.pg.execute(sql.SQL("ALTER PUBLICATION {} SET (publish_generated_columns=none)").format(sql.Identifier(self.name)))
            try:
                result = subprocess.run(self.command("init"), env=self.environment, capture_output=True, timeout=self.args.timeout)
                assert result.returncode != 0 and b"publication omits a configured source column" in result.stdout + result.stderr, (result.stdout + result.stderr).decode()
                assert not self.pg.execute("SELECT 1 FROM pg_replication_slots WHERE slot_name=%s", (self.name,)).fetchone()
            finally:
                self.config.write_text(original)
                self.pg.execute("ALTER TABLE orders REPLICA IDENTITY FULL")
                self.pg.execute(sql.SQL("ALTER PUBLICATION {} SET (publish_generated_columns=stored)").format(sql.Identifier(self.name)))
        with (self.directory / "init.log").open("wb") as log:
            subprocess.run(self.command("init"), env=self.environment, stdout=log, stderr=subprocess.STDOUT,
                           timeout=self.args.timeout, check=True)
        self.initial_metadata = self.table("orders")
        self.initial_rows = self.rows("orders", self.initial_metadata)
        return self.compare("snapshot")

    def compare(self, phase):
        json_columns = {"doc", "json_raw", "tags", "matrix", "states", "amounts", "domain_doc", "domain_ids", "domain_arrays", "extra_domain"}
        expressions = []
        for name, _ in self.columns:
            if name in {"tags", "matrix", "states", "amounts", "domain_ids", "domain_arrays"}:
                expressions.append(f"to_json({name})::text")
            elif name in json_columns or name in {"id", "state", "amount", "extra_uuid"}:
                expressions.append(f"{name}::text")
            else:
                expressions.append(name)
        expected = self.pg.execute("SELECT " + ",".join(expressions) + " FROM orders ORDER BY id").fetchall()
        metadata = self.table("orders")
        actual = self.rows("orders", metadata)
        def normalized(rows):
            result = []
            for row in rows:
                values = list(row)
                for i, (name, _) in enumerate(self.columns):
                    if values[i] is not None and name in json_columns:
                        values[i] = ("json", json.loads(values[i], parse_float=Decimal, parse_int=Decimal))
                    elif values[i] is not None and name == "amount":
                        values[i] = str(Decimal(values[i])) if values[i] in {"NaN", "Infinity", "-Infinity"} else Decimal(values[i])
                result.append(values)
            return result
        assert normalized(actual) == normalized(expected), f"{phase}: complete source/target rows differ"
        fields = next(s["fields"] for s in metadata["metadata"]["schemas"] if s["schema-id"] == metadata["metadata"]["current-schema-id"])
        assert [f["type"] for f in fields] == ["int" if t == "Int32" else "string" for _, t in self.columns]
        dump(self.directory / f"{phase}-metadata.json", metadata)
        return {"rows": len(actual), "snapshot": metadata["metadata"]["current-snapshot-id"]}

    def changes(self):
        barrier = self.transaction([
            "UPDATE orders SET domain_arrays=ARRAY[ARRAY[1,2,3]::int_list] WHERE quantity <= 16",
            "UPDATE orders SET quantity=quantity+1, state='waiting', amount=-0.000000000000000000000000000001 WHERE quantity <= 16",
            "UPDATE orders SET id='aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa', tags='{}', matrix=NULL, doc='null' WHERE id=md5('1')::uuid",
            "UPDATE orders SET doc=NULL, json_raw='null', states='{}', domain_ids='{}' WHERE id=md5('2')::uuid",
            "DELETE FROM orders WHERE quantity BETWEEN 90 AND 100",
            "INSERT INTO orders (id, state, quantity, amount, amounts) VALUES ('bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb', 'ready', 500, 'Infinity', ARRAY['NaN','Infinity','-Infinity']::numeric[])",
        ])
        self.wait_materialized(barrier)
        return self.compare("cdc")

    def restart(self):
        self.stop(crash=True)
        barrier = self.transaction(["UPDATE orders SET amount=999999999999999999999999999999999999999999999.000001, quantity=quantity+1 WHERE state='waiting'"])
        self.start()
        self.wait_materialized(barrier)
        return self.compare("restart")

    def additive_types(self):
        # Exercise the durable schema barrier as well as configured bootstrap types.
        self.pg.execute("ALTER TABLE orders ADD COLUMN extra_uuid uuid")
        self.pg.execute("ALTER TABLE orders ADD COLUMN extra_domain json_doc")
        self.columns.extend([("extra_uuid", "String"), ("extra_domain", "String")])
        barrier = self.transaction([
            "UPDATE orders SET extra_uuid=id, extra_domain=jsonb_build_object('added', quantity) WHERE quantity < 40",
        ])
        self.wait_materialized(barrier)
        result = self.compare("additive-types")
        self.stop(crash=True)
        self.start()
        barrier = self.transaction(["UPDATE orders SET extra_uuid=id WHERE quantity < 40"])
        self.wait_materialized(barrier)
        return result | {"restart": self.compare("additive-types-restart")}

    def compact(self):
        def compacted():
            return any(s.get("summary", {}).get("streaming.operation") == "compact" for s in self.table("orders")["metadata"]["snapshots"])
        self.until("native compaction did not publish", compacted)
        result = self.compare("compaction")
        assert self.rows("orders", self.initial_metadata) == self.initial_rows, "retained snapshot changed"
        return result

    def execute(self):
        try:
            self.phase("seed", self.seed)
            self.phase("snapshot", self.initialize)
            self.start()
            self.phase("cdc", self.changes)
            self.phase("restart", self.restart)
            self.phase("compaction", self.compact)
            self.phase("additive-types", self.additive_types)
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
    if not args.postgres_url:
        parser.error("provide --postgres-url or FLOW_POSTGRES_URL")
    TypeRun(args).execute()
    print(f"PASS: {args.artifacts / 'report.json'}", flush=True)


if __name__ == "__main__":
    main()
