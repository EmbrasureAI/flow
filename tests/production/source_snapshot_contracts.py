#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["duckdb==1.5.5", "psycopg[binary]==3.3.5", "boto3==1.43.88", "fastavro==1.12.2"]
# ///
"""Prove physical-table snapshot scope and fail-closed RLS with the real daemon."""
import argparse
import copy
import json
import os
from pathlib import Path
import subprocess
import sys
import traceback
import time
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "local"))
from run import Run, dump, lsn


class SnapshotRun(Run):
    def init_once(self, expected_success):
        log_path = self.directory / "init.log"
        with log_path.open("wb") as log:
            result = subprocess.run(self.command("init"), env=self.environment,
                                    stdout=log, stderr=subprocess.STDOUT, timeout=self.args.timeout)
        assert (result.returncode == 0) == expected_success, log_path.read_text()
        return log_path.read_text()

    def verify_physical_rows(self):
        expected = self.pg.execute("SELECT * FROM ONLY orders ORDER BY id").fetchall()
        actual = self.rows("orders")
        assert actual == expected, f"physical parent snapshot differs: {len(actual)} != {len(expected)}"
        return {"parent_rows": len(actual), "excluded_child_rows": self.pg.execute(
            "SELECT count(*) FROM ONLY orders_child").fetchone()[0]}

    def inheritance(self):
        self.seed()
        self.pg.execute("CREATE TABLE orders_child () INHERITS (orders)")
        self.pg.execute("INSERT INTO orders_child(id, tenant, payload) VALUES (90000, 1, 'child-only')")
        self.pg.execute(sql.SQL("ALTER PUBLICATION {} SET TABLE ONLY orders, ONLY accounts")
                        .format(sql.Identifier(self.name)))
        self.init_once(True)
        initial = self.verify_physical_rows()
        self.start()
        barrier = self.transaction([
            "UPDATE orders_child SET payload = 'child-updated' WHERE id = 90000",
            "INSERT INTO orders_child(id, tenant, payload) VALUES (90001, 1, 'new-child')",
            "UPDATE ONLY orders SET payload = 'parent-updated' WHERE id = 1",
        ])
        self.wait_materialized(barrier)
        return {"initial": initial, "after_cdc": self.verify_physical_rows()}

    def incarnation(self):
        self.seed()
        self.pg.execute("DELETE FROM orders")
        self.init_once(True)
        original = self.table("orders")["metadata"]
        assert original.get("current-snapshot-id", -1) in (-1, None)
        self.start()
        self.until("capture did not start", lambda: self.metrics().get("flow_source_received_lsn", 0))
        tables_uri = f"{self.args.catalog_uri.rstrip('/')}/v1/namespaces/{self.name}/tables"
        with urlopen(Request(tables_uri + "/orders", method="DELETE"), timeout=15):
            pass
        schema = next(s for s in original["schemas"] if s["schema-id"] == original["current-schema-id"])
        body = json.dumps({"name": "orders", "schema": schema,
                           "properties": {"format-version": "2"}}).encode()
        with urlopen(Request(tables_uri, data=body, headers={"Content-Type": "application/json"}), timeout=15):
            pass
        replacement = self.table("orders")["metadata"]
        assert replacement["table-uuid"] != original["table-uuid"]
        barrier = self.transaction(["INSERT INTO orders(id, tenant, payload) VALUES (1, 1, 'must-not-publish')"])
        deadline = time.monotonic() + self.args.timeout
        log = self.directory / f"daemon-{self.generation}.log"
        while time.monotonic() < deadline:
            if "Iceberg target UUID changed" in log.read_text():
                break
            self.alive()
            time.sleep(.1)
        else:
            raise TimeoutError("running actor did not reject replacement table UUID")
        self.stop()
        assert self.metrics().get("flow_materialized_lsn", 0) < barrier
        confirmed = self.pg.execute("SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name=%s", (self.name,)).fetchone()[0]
        assert lsn(confirmed) < barrier, "replacement-table transaction was acknowledged to PostgreSQL"
        after = self.table("orders")["metadata"]
        assert after.get("current-snapshot-id", -1) in (-1, None)
        assert after["schemas"] == replacement["schemas"]
        return {"original_uuid": original["table-uuid"], "replacement_uuid": after["table-uuid"],
                "rejected_before_publication": True, "source_barrier_not_acknowledged": barrier, "confirmed_flush_lsn": confirmed}

    def rls(self):
        self.seed()
        role = self.name + "_reader"
        self.pg.execute(sql.SQL("CREATE ROLE {} LOGIN REPLICATION NOSUPERUSER NOBYPASSRLS PASSWORD {}")
                        .format(sql.Identifier(role), sql.Literal("local-snapshot-test")))
        try:
            self.pg.execute(sql.SQL("GRANT USAGE ON SCHEMA {} TO {}")
                            .format(sql.Identifier(self.name), sql.Identifier(role)))
            self.pg.execute(sql.SQL("GRANT SELECT ON orders, accounts TO {}")
                            .format(sql.Identifier(role)))
            self.pg.execute("ALTER TABLE orders ENABLE ROW LEVEL SECURITY")
            self.pg.execute("ALTER TABLE orders FORCE ROW LEVEL SECURITY")
            self.pg.execute("CREATE POLICY visible_rows ON orders FOR SELECT USING (id <= 2)")
            restricted_url = make_conninfo(self.args.postgres_url, user=role, password="local-snapshot-test")
            with psycopg.connect(restricted_url) as restricted:
                visible = restricted.execute(sql.SQL("SELECT count(*) FROM {}.orders")
                                             .format(sql.Identifier(self.name))).fetchone()[0]
            assert visible == 2, "fixture role must actually be subject to RLS"
            self.environment["FLOW_LOCAL_POSTGRES_URL"] = restricted_url
            log = self.init_once(False)
            assert "row-level security" in log.lower(), log
            try:
                metadata = self.table("orders")["metadata"]
            except HTTPError as error:
                if error.code != 404:
                    raise
            else:
                assert metadata.get("current-snapshot-id", -1) in (-1, None), \
                    "RLS-filtered initial rows were published"
            return {"policy_visible_rows": visible, "initial_publication_rejected": True}
        finally:
            self.pg.execute(sql.SQL("DROP OWNED BY {}").format(sql.Identifier(role)))
            self.pg.execute(sql.SQL("DROP ROLE {}").format(sql.Identifier(role)))

    def execute_contract(self, name):
        try:
            self.phase(name, getattr(self, name))
            self.report["passed"] = True
        except BaseException as error:
            self.report["error"] = str(error)
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
    parser.add_argument("--artifacts", type=Path, required=True)
    parser.add_argument("--timeout", type=float, default=180)
    args = parser.parse_args()
    args.artifacts.mkdir(parents=True, exist_ok=False)
    for name in ("inheritance", "rls", "incarnation"):
        case = copy.copy(args)
        case.artifacts = args.artifacts / name
        SnapshotRun(case).execute_contract(name)
    print(json.dumps({"passed": True, "artifacts": str(args.artifacts.resolve())}))


if __name__ == "__main__":
    main()
