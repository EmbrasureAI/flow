#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["duckdb==1.5.5", "psycopg[binary]==3.3.5", "boto3==1.43.88", "fastavro==1.12.2", "pytz==2026.3.post1"]
# ///
"""Require metadata and object reclamation before a table's own CDC backlog drains."""

import argparse
import json
import os
from pathlib import Path
import time
import traceback
from urllib.parse import urlsplit
from urllib.request import Request, urlopen

from botocore.exceptions import ClientError
from maintenance_fairness import FairnessRun, dump, lsn


class PeriodicRun(FairnessRun):
    def __init__(self, args):
        super().__init__(args)
        text = self.config.read_text().replace("pending_transactions = 16", "pending_transactions = 4")
        text = text.replace("table_workers = 2", f"table_workers = {args.table_workers}")
        text = text.replace("snapshot_retention_secs = 3600", "snapshot_retention_secs = 2\nsnapshot_expiration = true\n"
                            "manifest_max_count = 4\ngarbage_interval_secs = 1\norphan_grace_secs = 1\n"
                            "checkpoint_interval_secs = 2\nretained_checkpoints = 2")
        # The finite backlog exceeds the 128-snapshot history trigger. Native
        # rewrites remain enabled: expired L0 birth snapshots imply hard age.
        for key, value in self.policy.items():
            if "_files" in key:
                replacement = 2 if key == "l0_soft_files" else 512
                text = text.replace(f"{key} = {value}\n", f"{key} = {replacement}\n")
                self.policy[key] = replacement
        self.config.write_text(text)
        self.report.update(pending_transactions=4, table_workers=args.table_workers, compaction_policy=self.policy,
                           native_compaction=True, manifest_max_count=4,
                           snapshot_retention_secs=2, garbage_interval_secs=1,
                           orphan_grace_secs=1, checkpoint_interval_secs=2,
                           catalog_request_delay_seconds=.01, timeout_seconds=args.timeout,
                           drain_timeout_seconds=args.drain_timeout or args.timeout)

    def queued_backlog(self):
        result = super().queued_backlog()
        # A standard Iceberg tag retains a real historical reader while other
        # old snapshots and their owned manifest lists become reclaimable.
        snapshot = self.initial_metadata["metadata"]["current-snapshot-id"]
        body = {"requirements": [{"type": "assert-table-uuid", "uuid": self.before_metadata["metadata"]["table-uuid"]}],
                "updates": [{"action": "set-snapshot-ref", "ref-name": "retained-initial",
                             "type": "tag", "snapshot-id": snapshot}]}
        uri = f"{self.args.catalog_uri.rstrip('/')}/v1/namespaces/{self.name}/tables/orders"
        with urlopen(Request(uri, data=json.dumps(body).encode(), headers={"Content-Type": "application/json"}), timeout=10) as response:
            assert response.status == 200
        self.expirable = self.before_metadata["metadata"]["current-snapshot-id"]
        self.expirable_manifest = next(s["manifest-list"] for s in self.before_metadata["metadata"]["snapshots"]
                                      if s["snapshot-id"] == self.expirable)
        assert self.expirable != snapshot
        self.catalog_proxy.delay_seconds = .01
        return result | {"retained_tag_snapshot": snapshot,
                         "expirable_snapshot": self.expirable,
                         "expirable_manifest_list": self.expirable_manifest}

    def missing(self, path):
        uri = urlsplit(path)
        try:
            self.s3.head_object(Bucket=uri.netloc, Key=uri.path.lstrip('/'))
        except ClientError as error:
            if error.response["ResponseMetadata"]["HTTPStatusCode"] == 404:
                return True
            raise
        return False

    @staticmethod
    def source_lsn(metadata):
        return max((lsn(s["summary"]["streaming.last-lsn"]) for s in metadata["snapshots"]
                    if "streaming.last-lsn" in s.get("summary", {})), default=0)

    def admission_and_progress(self):
        self.native = True
        started = time.monotonic()
        self.start()
        table_id = self.pg.execute("SELECT 'orders'::regclass::oid").fetchone()[0]
        observations = []
        self.report["reclamation_observations"] = observations
        saw_rewrite = False
        saw_expiration = False
        saw_reclamation = False
        while time.monotonic() - started < self.args.timeout:
            assert self.process.poll() is None, "daemon exited during periodic maintenance"
            metadata = self.table("orders")["metadata"]
            source_lsn = self.source_lsn(metadata)
            assert source_lsn < self.barrier, "periodic work finished only after orders' own backlog drained"
            status = self.status()
            events = list(self.log_fields())
            rewrite = any(event.get("event") == "manifest_rewrite_completed" and event.get("table_id") == table_id
                          for event in events)
            expired = all(s["snapshot-id"] != self.expirable for s in metadata["snapshots"])
            reclaimed = expired and self.missing(self.expirable_manifest)
            if (rewrite and not saw_rewrite) or (expired and not saw_expiration) or (reclaimed and not saw_reclamation):
                observations.append({"seconds": round(time.monotonic() - started, 3),
                                     "orders_lsn": source_lsn, "status": status,
                                     "manifest_rewrite": rewrite, "snapshot_expired": expired,
                                     "manifest_list_deleted": reclaimed})
            saw_rewrite |= rewrite
            saw_expiration |= expired
            saw_reclamation |= reclaimed
            checkpoints = [event for event in events if event.get("event") == "index_checkpoint_completed"]
            builds = [event for event in events if event.get("event") == "compaction_build_started"]
            data_work = builds if self.args.table_workers > 1 else [
                event for event in events if event.get("event") == "compaction_completed"
                and event.get("candidate_kind") == "data_rewrite"]
            if (saw_rewrite and saw_expiration and saw_reclamation and len(checkpoints) >= 3
                    and data_work):
                # Catalog reads and S3 HEAD are separate observations. Prove
                # the table still has work after all reclamation checks finish.
                proof = self.table("orders")["metadata"]
                assert self.source_lsn(proof) < self.barrier, "reclamation overlapped only the final drain"
                boundary = {"orders_lsn": self.source_lsn(proof), "status": self.status(),
                            "checkpoints": checkpoints, "data_work": data_work}
                self.report["reclamation_boundary"] = boundary
                break
            time.sleep(.1)
        assert saw_rewrite and saw_expiration and saw_reclamation, "periodic maintenance failed to reclaim old metadata"
        assert len(checkpoints) >= 3, "checkpoint rotation did not overlap the backlog"
        assert data_work, "native data maintenance did not compete with periodic work"
        before = self.status()["watermarks"]["materialized_lsn"]
        self.until("CDC did not resume after periodic reclamation", lambda:
                   self.status()["watermarks"]["materialized_lsn"] > before)
        # Fairness/reclamation must meet the original deadline above. Draining
        # 1,025 queued transactions under deliberately frequent maintenance is
        # a separate correctness check, not a throughput qualification.
        self.wait_materialized(self.barrier, timeout=self.args.drain_timeout)
        self.until("source ACK did not reach the final transaction", lambda: self.confirmed() >= self.barrier)
        result = self.compare("periodic-maintenance-and-queued-cdc")
        metadata = self.table("orders")
        tagged = self.initial_metadata["metadata"]["current-snapshot-id"]
        assert metadata["metadata"]["refs"]["retained-initial"]["snapshot-id"] == tagged
        assert any(s["snapshot-id"] == tagged for s in metadata["metadata"]["snapshots"])
        assert self.rows("orders", self.initial_metadata) == self.initial_rows
        self.until("obsolete checkpoint directories were not retired", lambda:
                   len(list((self.directory / "state/checkpoints").glob("*/CURRENT"))) == 2)
        active = set()
        for event in self.log_fields():
            if event.get("event") == "periodic_maintenance_admitted":
                assert not active, "multiple periodic jobs occupied the workers together"
                active.add(event["table_id"])
            elif event.get("event") == "periodic_maintenance_completed":
                active.remove(event["table_id"])
        return result | {"reclamation_observations": observations,
                         "periodic_admissions": [event for event in self.log_fields()
                             if event.get("event") == "periodic_maintenance_admitted"],
                         "reclamation_boundary": boundary, "retained_initial_rows": len(self.initial_rows),
                         "confirmed_lsn": self.confirmed()}

    def execute(self):
        try:
            self.phase("seed", self.seed)
            self.phase("initial-copy", self.initialize)
            self.phase("queue-source-and-prime-durable-backlog", self.queued_backlog)
            self.phase("periodic-reclamation-with-continuously-ready-cdc", self.admission_and_progress)
            self.phase("clean-shutdown", lambda: {"exit_code": self.stop_clean()})
            self.check_worker_panics()
            self.report["passed"] = True
        except BaseException as error:
            self.report.update(error=str(error), traceback=traceback.format_exc())
            raise
        finally:
            self.object_proxy.release_reads.set()
            self.catalog_proxy.release_commits.set()
            self.stop()
            self.object_proxy.close()
            self.catalog_proxy.close()
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
    parser.add_argument("--table-workers", type=int, choices=(1, 2), default=2)
    parser.add_argument("--two-hot-tables", action="store_true")
    parser.add_argument("--timeout", type=float, default=180)
    parser.add_argument("--drain-timeout", type=float, help="final backlog drain limit; defaults to --timeout")
    args = parser.parse_args()
    PeriodicRun(args).execute()
    print(f"PASS: {args.artifacts.resolve() / 'report.json'}", flush=True)


if __name__ == "__main__":
    main()
