#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["duckdb==1.5.5", "psycopg[binary]==3.3.5", "boto3==1.43.88", "fastavro==1.12.2"]
# ///
"""Lose the read-only keeper after COPY; preserve completion and real COPY failures."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import traceback

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "local"))
from run import Run, dump
from proxy import CatalogProxy
from process_lifecycle import run_supervised


class KeeperRun(Run):
    def __init__(self, args):
        super().__init__(args)
        self.proxy = CatalogProxy(args.catalog_uri, self.directory / "catalog-proxy.jsonl")
        args.catalog_uri = self.proxy.url
        self.configure()
        self.environment["RUST_LOG"] = "info"

    def start_init(self):
        self.generation += 1
        self.log_path = self.directory / f"init-{self.generation}.log"
        self.log = self.log_path.open("wb")
        self.process = subprocess.Popen(self.command("init"), env=self.environment,
                                        stdout=self.log, stderr=subprocess.STDOUT)

    def events(self):
        events = []
        for line in self.log_path.read_text().splitlines():
            try:
                events.append(json.loads(line).get("fields", {}))
            except json.JSONDecodeError:
                pass
        return events

    def finish_init(self):
        code = self.process.wait(timeout=self.args.timeout)
        self.stop()
        return code

    def keeper_loss(self):
        self.proxy.hold_commits()
        self.start_init()
        self.until("both COPY workers did not finish before publication", lambda:
            len({e["table_id"] for e in self.events()
                 if e.get("event") == "bootstrap_table_staged"}) == 2)
        self.until("catalog publication was not held", self.proxy.commit_held.is_set)
        # The test owns these source tables. Worker transactions have committed;
        # only their exporting keeper still holds this read-only snapshot/lock.
        keepers = self.pg.execute("""SELECT a.pid FROM pg_stat_activity a
            WHERE a.application_name = 'embrasure-flow' AND a.state = 'idle in transaction'
              AND a.query = 'SELECT pg_catalog.pg_export_snapshot()'
              AND EXISTS (SELECT 1 FROM pg_locks l WHERE l.pid = a.pid
                  AND l.relation = 'orders'::regclass AND l.mode = 'AccessShareLock')""").fetchall()
        assert len(keepers) == 1, f"expected exactly one snapshot keeper, found {keepers}"
        assert self.pg.execute("SELECT pg_terminate_backend(%s)", keepers[0]).fetchone()[0]
        self.proxy.release_commits.set()
        assert self.finish_init() == 0, f"late keeper loss failed initialization; see {self.log_path}"
        events = self.events()
        assert any(e.get("event") == "bootstrap_snapshot_cleanup_failed" for e in events)
        assert sum(e.get("event") == "bootstrap_completed" for e in events) == 1
        assert len({e["table_id"] for e in events if e.get("event") == "bootstrap_table_published"}) == 2
        snapshots = {name: self.table(name)["metadata"]["current-snapshot-id"]
                     for name in ("orders", "accounts")}
        self.compare("initial-bases")
        self.start_init()
        assert self.finish_init() == 0
        # The durable copied flag takes the early return; an unpublished flag
        # would re-enter bootstrap and emit bootstrap_completed again.
        assert not any(e.get("event", "").startswith("bootstrap_") for e in self.events())
        assert snapshots == {name: self.table(name)["metadata"]["current-snapshot-id"] for name in snapshots}
        self.start()
        barrier = self.transaction(["UPDATE orders SET amount = amount + 1 WHERE id = 1",
                                    "DELETE FROM accounts WHERE id = 2"])
        self.wait_materialized(barrier)
        self.compare("cdc-after-keeper-loss")
        return {"terminated_keeper": keepers[0][0], "durable_completion": True,
                "repeated_init_did_not_recopy": True, "cdc_rows_verified": True}

    def copy_failure(self):
        # This source row exceeds the configured COPY chunk admission limit.
        self.pg.execute("UPDATE orders SET payload = repeat('x', 70000) WHERE id = 1")
        for _ in range(2):
            self.start_init()
            assert self.finish_init() != 0, "real COPY failure was incorrectly treated as cleanup"
            assert "snapshot row exceeds configured chunk limit" in self.log_path.read_text()
            assert not any(e.get("event") == "bootstrap_completed" for e in self.events())
            assert self.table("orders")["metadata"].get("current-snapshot-id") in (None, -1)
        return {"copy_failure_preserved": True, "repeated_init_still_requires_copy": True}

    def execute(self, fail_copy):
        try:
            self.phase("seed", self.seed)
            self.phase("copy-failure" if fail_copy else "late-keeper-loss",
                       self.copy_failure if fail_copy else self.keeper_loss)
            self.report["passed"] = True
        except BaseException as error:
            self.report.update(error=str(error), traceback=traceback.format_exc())
            raise
        finally:
            dump(self.directory / "report.json", self.report)
            self.proxy.release_commits.set()
            try:
                self.stop()
            finally:
                self.proxy.close()
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
    parser.add_argument("--timeout", type=float, default=120)
    parser.add_argument("--supervised-worker", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()
    if not args.supervised_worker:
        result = run_supervised([sys.executable, str(Path(__file__).resolve()), *sys.argv[1:],
                                 "--supervised-worker"], args.artifacts, timeout=6 * args.timeout + 120)
        raise SystemExit(result.returncode)
    for fail_copy, name in ((False, "late-keeper-loss"), (True, "copy-failure")):
        case = argparse.Namespace(**(vars(args) | {"artifacts": args.artifacts / name}))
        KeeperRun(case).execute(fail_copy)
    print(f"PASS: {args.artifacts.resolve()}", flush=True)


if __name__ == "__main__":
    main()
