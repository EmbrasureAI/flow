#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["duckdb==1.5.5", "psycopg[binary]==3.3.5", "boto3==1.43.88", "fastavro==1.12.2"]
# ///
"""Nullable evolution and durable source-table isolation using real services."""
import argparse
import json
import os
from pathlib import Path
import traceback

from run import dump
from table_isolation import IsolationRun


class SchemaRun(IsolationRun):
    def configure(self):
        super().configure()
        if self.args.explicit:
            self.config.write_text(self.config.read_text().replace(
                "[[tables]]", '[[tables]]\ncolumn_selection = "explicit"'))

    def fields(self):
        metadata = self.table("orders")["metadata"]
        return next(s["fields"] for s in metadata["schemas"]
                    if s["schema-id"] == metadata["current-schema-id"])

    def relax(self):
        previous = self.table("orders")
        fields = self.fields()
        self.proxy.arm_drop("schema")
        barrier = self.transaction(["ALTER TABLE orders ALTER COLUMN tenant DROP NOT NULL",
                                    "UPDATE orders SET tenant=NULL WHERE id <= 1000",
                                    "SELECT pg_sleep(1)",
                                    "UPDATE accounts SET amount=1 WHERE id=1"])
        self.wait_materialized(barrier)
        assert self.proxy.dropped.wait(timeout=10), "schema commit response was not dropped"
        relaxed = self.fields()
        assert relaxed[1] == fields[1] | {"required": False}
        assert relaxed[:1] + relaxed[2:] == fields[:1] + fields[2:]
        assert previous["metadata"]["table-uuid"] == self.table("orders")["metadata"]["table-uuid"]
        result = self.compare("nullable-row")
        self.stop(crash=True)
        self.start()
        barrier = self.transaction(["UPDATE orders SET tenant=NULL WHERE id=2"])
        self.wait_materialized(barrier)
        self.compare("nullable-restart")
        if not self.args.explicit:
            self.pg.execute("ALTER TABLE orders ADD COLUMN after_relax text")
            barrier = self.transaction(["UPDATE orders SET after_relax='new' WHERE id=1"])
            self.wait_materialized(barrier)
            self.compare("addition-after-relaxation")
        return result

    def isolate(self):
        previous = self.rows("orders")
        # An explicit selection must never copy excluded values into quarantine.
        secret = "excluded-schema-isolation-fixture-secret-782ab19d"
        if self.args.explicit:
            self.pg.execute("ALTER TABLE orders ADD COLUMN excluded_secret text")
            barrier = self.transaction([f"UPDATE orders SET excluded_secret='{secret}' WHERE id=1"])
            self.wait_materialized(barrier)
        self.pg.execute("ALTER TABLE orders RENAME COLUMN payload TO renamed_payload")
        barrier = self.transaction(["UPDATE orders SET renamed_payload='blocked-change' WHERE id=1",
                                    "UPDATE accounts SET amount=2 WHERE id=1"])
        self.wait_blocked()
        assert self.blocked()["error_code"] == "source_schema_incompatible"
        self.healthy(barrier)
        self.check_ack(barrier)
        assert self.rows("orders") == previous
        self.pg.execute("BEGIN")
        self.pg.execute("UPDATE orders SET renamed_payload='rolled-back-evidence' WHERE id=2")
        self.pg.execute("ROLLBACK")
        for i in range(12):
            barrier = self.transaction([f"UPDATE accounts SET amount={i} WHERE id=1",
                                        f"UPDATE orders SET renamed_payload='retained-{i}' WHERE id=1"])
        self.healthy(barrier)
        self.check_ack(barrier)
        self.stop(crash=True)
        self.start()
        barrier = self.transaction(["UPDATE accounts SET amount=123 WHERE id=1"])
        self.wait_blocked()
        self.healthy(barrier)
        self.check_ack(barrier)
        assert self.rows("orders") == previous
        for segment in (self.directory / "state" / "journal").glob("*.segment"):
            assert secret.encode() not in segment.read_bytes(), "excluded value entered the journal"
        return {"blocked": self.blocked(), "healthy_barrier": barrier,
                "restart_preserved_block": True, "excluded_values_retained": False}

    def execute(self):
        try:
            self.phase("seed", self.seed)
            self.table_ids = dict(self.pg.execute(
                "SELECT relname,oid FROM pg_class WHERE relnamespace=%s::regnamespace AND relname IN ('orders','accounts')",
                (self.name,)).fetchall())
            self.phase("initial-copy", self.initialize)
            self.start()
            self.phase("handoff", self.handoff)
            self.phase("relax-nullability-and-restart", self.relax)
            self.phase("schema-block-mixed-transactions-and-restart", self.isolate)
            self.report["passed"] = True
        except BaseException as error:
            self.report.update(failure=str(error), traceback=traceback.format_exc())
            raise
        finally:
            self.stop()
            self.proxy.close()
            dump(self.directory / "report.json", self.report)
            self.pg.close()
            self.duck.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--postgres-url", default=os.environ.get("FLOW_POSTGRES_URL"))
    parser.add_argument("--catalog-uri", required=True)
    parser.add_argument("--s3-endpoint", required=True)
    parser.add_argument("--warehouse", default="s3://warehouse/")
    parser.add_argument("--binary", type=Path, default=Path("target/debug/embrasure-flow"))
    parser.add_argument("--artifacts", type=Path, required=True)
    parser.add_argument("--timeout", type=float, default=180)
    parser.add_argument("--explicit", action="store_true")
    parser.set_defaults(quota=False)
    args = parser.parse_args()
    SchemaRun(args).execute()
    print(f"PASS: {args.artifacts / 'report.json'}", flush=True)
