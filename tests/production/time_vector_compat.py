#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["duckdb==1.5.5", "psycopg[binary]==3.3.5", "boto3==1.43.88", "fastavro==1.12.2"]
# ///
"""Real COPY/CDC/restart/compaction tests for time and optional pgvector support."""
import argparse
import json
import os
from pathlib import Path

from psycopg import sql
from type_compat import TypeRun
from run import dump


class TimeVectorRun(TypeRun):
    def configure(self):
        super().configure()
        self.columns = [("id", "String"), ("clock", "String"), ("clocks", "String"), ("payload", "String")]
        if self.args.vector:
            self.columns += [("embedding", "String"), ("embeddings", "String"), ("flexible", "String")]
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

    def seed(self):
        self.pg.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(self.name)))
        self.pg.execute("CREATE DOMAIN clock_value AS time(6)")
        extra = ""
        if self.args.vector:
            # Non-public extension schema ensures OID/extension discovery, not search_path assumptions.
            self.pg.execute("CREATE SCHEMA IF NOT EXISTS flow_vector_extension")
            self.pg.execute("CREATE EXTENSION IF NOT EXISTS vector WITH SCHEMA flow_vector_extension")
            extension_schema = self.pg.execute("SELECT n.nspname FROM pg_extension e JOIN pg_namespace n ON n.oid=e.extnamespace WHERE e.extname='vector'").fetchone()[0]
            self.vector_type = sql.Identifier(extension_schema, "vector").as_string(self.pg)
            self.pg.execute(f"CREATE DOMAIN embedding_value AS {self.vector_type}(1536)")
            extra = f", embedding embedding_value, embeddings embedding_value[], flexible {self.vector_type}"
        self.pg.execute("CREATE TABLE orders (id time(6) PRIMARY KEY, clock clock_value, clocks clock_value[], payload text" + extra + ")")
        self.pg.execute("ALTER TABLE orders REPLICA IDENTITY FULL")
        self.pg.execute("""INSERT INTO orders SELECT
            '00:00:00'::time + i * interval '1 second',
            CASE i % 4 WHEN 0 THEN '24:00:00'::time WHEN 1 THEN '00:00:00'::time
                WHEN 2 THEN '23:59:59.999999'::time ELSE NULL END,
            ARRAY['00:00:00'::time, NULL, '24:00:00'::time, '12:34:56.123456'::time]::clock_value[],
            (SELECT string_agg(md5((i * 1000 + j)::text), '') FROM generate_series(1,100) j)
            """ + (", NULL, NULL, NULL" if self.args.vector else "") + " FROM generate_series(1,128) i")
        if self.args.vector:
            self.pg.execute(f"""UPDATE orders SET embedding=(SELECT array_agg((j::real/7919)::real)::{self.vector_type} FROM generate_series(1,1536) j),
                flexible='[0.1,-0,3.4028235e38,1.1754944e-38,1e-45]'::{self.vector_type}""")
            self.pg.execute("UPDATE orders SET embeddings=ARRAY[embedding,NULL]::embedding_value[]")
            self.pg.execute("UPDATE orders SET embedding=NULL, embeddings='{}' WHERE id='00:00:01'")
        self.pg.execute(sql.SQL("CREATE PUBLICATION {} FOR TABLE orders").format(sql.Identifier(self.name)))
        return {"rows": 128, "vector": self.args.vector, "time_primary_key": True}

    def compare(self, phase):
        expressions = []
        json_columns = {"clocks", "embedding", "embeddings", "flexible"}
        for name, _ in self.columns:
            if name in {"id", "clock", "added_clock"}:
                # Cast interval explicitly: to_char(time) rounds or wraps 24:00:00 in some clients.
                expressions.append(f"CASE WHEN {name} IS NULL THEN NULL ELSE lpad(extract(hour FROM {name})::int::text,2,'0') || ':' || lpad(extract(minute FROM {name})::int::text,2,'0') || ':' || to_char(extract(second FROM {name}), 'FM00.000000') END")
            elif name == "clocks":
                expressions.append("to_json(clocks)::text")
            elif name in {"embedding", "flexible"}:
                expressions.append(f"to_json({name}::real[]::double precision[])::text")
            elif name == "embeddings":
                expressions.append("CASE WHEN embeddings IS NULL THEN NULL ELSE (SELECT coalesce(json_agg(e::real[]::double precision[]), '[]'::json)::text FROM unnest(embeddings) e) END")
            else:
                expressions.append(name)
        expected = self.pg.execute("SELECT " + ",".join(expressions) + " FROM orders ORDER BY id").fetchall()
        metadata = self.table("orders")
        actual = self.rows("orders", metadata)
        def normalize(rows):
            result = []
            for row in rows:
                values = list(row)
                for index, (name, _) in enumerate(self.columns):
                    if values[index] is not None and name in json_columns:
                        values[index] = json.loads(values[index])
                        if name == "clocks":
                            values[index] = [None if v is None else v.split('.')[0] + '.' + (v.split('.')[1] if '.' in v else '').ljust(6, '0') for v in values[index]]
                result.append(values)
            return result
        assert normalize(actual) == normalize(expected), f"{phase}: complete source/target rows differ"
        fields = next(s["fields"] for s in metadata["metadata"]["schemas"] if s["schema-id"] == metadata["metadata"]["current-schema-id"])
        assert all(f["type"] == "string" for f in fields)
        dump(self.directory / f"{phase}-metadata.json", metadata)
        return {"rows": len(actual), "snapshot": metadata["metadata"]["current-snapshot-id"]}

    def changes(self):
        changes = [
            "UPDATE orders SET clock='12:34:56.000001', payload='changed' WHERE id < '00:00:20'", # unchanged toasted vectors
            "UPDATE orders SET id='24:00:00', clocks='{}' WHERE id='00:00:02'", # time PK move, end of day
            "UPDATE orders SET clock=NULL, clocks=NULL WHERE id='00:00:03'",
            "DELETE FROM orders WHERE id BETWEEN '00:01:30' AND '00:01:40'",
            "INSERT INTO orders (id,clock,clocks,payload) VALUES ('23:59:59.999999','24:00:00',ARRAY['00:00:00'::time,NULL],'inserted')",
        ]
        if self.args.vector:
            changes += [f"UPDATE orders SET flexible='[1,2,3]'::{self.vector_type}, embedding=NULL WHERE id='00:00:04'", "UPDATE orders SET embeddings=NULL WHERE id='00:00:05'"]
        self.wait_materialized(self.transaction(changes))
        return self.compare("cdc")

    def restart(self):
        self.stop(crash=True)
        changes = ["UPDATE orders SET clock='00:00:00.000001' WHERE id='24:00:00'", "DELETE FROM orders WHERE id='00:00:06'"]
        if self.args.vector:
            changes.append(f"UPDATE orders SET flexible='[0.2,0.3]'::{self.vector_type} WHERE id='00:00:07'")
        barrier = self.transaction(changes)
        self.start()
        self.wait_materialized(barrier)
        return self.compare("restart")

    def additive_types(self):
        self.pg.execute("ALTER TABLE orders ADD COLUMN added_clock clock_value")
        self.columns.append(("added_clock", "String"))
        self.wait_materialized(self.transaction(["UPDATE orders SET added_clock='24:00:00' WHERE id='24:00:00'"]))
        result = self.compare("additive-types")
        self.stop(crash=True)
        self.start()
        self.wait_materialized(self.transaction(["UPDATE orders SET added_clock='12:34:56.123456' WHERE id='24:00:00'"]))
        return result | {"restart": self.compare("additive-types-restart")}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--postgres-url", default=os.environ.get("FLOW_POSTGRES_URL"))
    parser.add_argument("--catalog-uri", required=True)
    parser.add_argument("--s3-endpoint", required=True)
    parser.add_argument("--warehouse", default="s3://warehouse/")
    parser.add_argument("--binary", type=Path, default=Path("target/debug/embrasure-flow"))
    parser.add_argument("--artifacts", type=Path, required=True)
    parser.add_argument("--timeout", type=float, default=180)
    parser.add_argument("--vector", action="store_true", help="require a PostgreSQL server with pgvector available")
    args = parser.parse_args()
    if not args.postgres_url:
        parser.error("provide --postgres-url or FLOW_POSTGRES_URL")
    TimeVectorRun(args).execute()
    print(f"PASS: {args.artifacts / 'report.json'}", flush=True)


if __name__ == "__main__":
    main()
