#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["duckdb==1.5.5", "psycopg[binary]==3.3.5", "boto3==1.43.88", "fastavro==1.12.2"]
# ///
"""Real-service CDC failure/recovery checks, restricted to one disposable Compose project."""

import argparse
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
import traceback
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import urlopen

import psycopg
from psycopg import sql

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "local"))
from run import Run, dump, lsn
from proxy import CatalogProxy

CATALOG_READ_RECOVERY_SECONDS = 5


class FaultRun(Run):
    def configure(self):
        super().configure()
        # Recovery assertions require structured activation events regardless
        # of the caller's logging preference.
        self.environment["RUST_LOG"] = "info,flow_events=debug"
        # FULL old/new text tuples can exceed twice the decoded row size because
        # bytea is hex encoded. Keep the fixture bounded without rejecting its
        # intentional wide-row workload at the ordinary smoke test's 64 KiB cap.
        self.config.write_text(self.config.read_text().replace("chunk_bytes = 65536", "chunk_bytes = 262144"))
        if self.args.ack_mode == "journaled":
            # Journaled ACK lets PostgreSQL release WAL once a transaction is in
            # the local journal; the fixture declares that journal independent.
            self.config.write_text(self.config.read_text()
                                   .replace('ack_mode = "materialized"', 'ack_mode = "journaled"')
                                   .replace('journal_durability = "local-disk"',
                                            'journal_durability = "independent-storage"'))

    def __init__(self, args):
        self.upstream = args.catalog_uri
        self.proxy = None
        self.compactor = None
        self.compactor_log = None
        self.compactor_generation = 0
        self.incidents = []
        self.recovery_times = []
        self.catalog_database_restarted = False
        self.check_compose(args)
        super().__init__(args)
        self.proxy = CatalogProxy(self.upstream, self.directory / "catalog-proxy.jsonl")
        args.catalog_uri = self.proxy.url
        self.configure()
        self.report.update(compose_project=args.compose_project, availability_incidents=self.incidents,
                           recovery_times=self.recovery_times, fault_seconds=args.fault_seconds,
                           format_version=args.format_version, ack_mode=args.ack_mode)

    @staticmethod
    def compose_command(args):
        command = ["docker", "compose", "-p", args.compose_project, "-f", str(args.compose_file)]
        if args.compose_env_file is not None:
            command.extend(["--env-file", str(args.compose_env_file)])
        return command

    @staticmethod
    def check_compose(args):
        if not args.compose_project.startswith("flow-"):
            raise ValueError("use a disposable --compose-project starting with flow-")
        for service in ("postgres", "minio", "rest"):
            result = subprocess.run(FaultRun.compose_command(args) + ["ps", "-q", service],
                                    check=True, text=True, capture_output=True)
            if not result.stdout.strip():
                raise RuntimeError(f"{service} is not running in requested project {args.compose_project}")

    def phase(self, name, action):
        started = time.monotonic()
        self.current_phase = name
        print(f"[{name}]", flush=True)
        phase = {"name": name, "passed": False}
        self.report["phases"].append(phase)
        try:
            phase["details"] = action()
            phase["passed"] = True
        except BaseException as error:
            phase["error"] = str(error)
            raise
        finally:
            phase["seconds"] = round(time.monotonic() - started, 3)
            dump(self.directory / "report.json", self.report)

    def table(self, name):
        # Independent inspection bypasses only injected catalog transport faults.
        url = f"{self.upstream.rstrip('/')}/v1/namespaces/{quote(self.name)}/tables/{quote(name)}"
        deadline = time.monotonic() + CATALOG_READ_RECOVERY_SECONDS
        while True:
            try:
                timeout = max(0.01, deadline - time.monotonic()) if self.catalog_database_restarted else 15
                with urlopen(url, timeout=timeout) as response:
                    return json.load(response)
            except HTTPError as error:
                # A successful readiness read does not visit every JDBC pooled
                # connection. A later read can still encounter one terminated
                # by our database restart. Retry only this bounded read failure;
                # missing tables and incorrect data remain immediate failures.
                if (not self.catalog_database_restarted or error.code not in {500, 502, 503, 504}
                        or time.monotonic() >= deadline):
                    raise
                self.incidents.append({"phase": self.current_phase, "service": "catalog-reader",
                                       "http_status": error.code, "table": name, "retried": True})
                error.close()
                time.sleep(min(0.25, max(0, deadline - time.monotonic())))

    def compose(self, *arguments):
        with (self.directory / "compose-actions.log").open("a") as output:
            output.write(json.dumps({"phase": self.current_phase, "arguments": arguments}) + "\n")
            output.flush()
            subprocess.run(self.compose_command(self.args) + list(arguments), check=True,
                           stdout=output, stderr=subprocess.STDOUT, timeout=40)

    def start_compactor(self):
        if self.args.compactor_binary is None:
            return
        self.compactor_generation += 1
        self.compactor_log = (self.directory / f"compactor-{self.compactor_generation}.log").open("wb")
        self.compactor = subprocess.Popen([str(self.args.compactor_binary.resolve()), "--config", str(self.config)],
                                          env=self.environment, stdout=self.compactor_log, stderr=subprocess.STDOUT)

    def stop_compactor(self, require_clean=False):
        if self.compactor is not None:
            process = self.compactor
            shutdown = {"pid": process.pid, "log": f"compactor-{self.compactor_generation}.log",
                        "requested_at_ms": time.time_ns() // 1_000_000, "forced_kill": False}
            if process.poll() is None:
                # The fixture compactor handles SIGINT and finishes its current
                # catalog pass before exiting. SIGTERM can orphan a proxy POST.
                process.send_signal(signal.SIGINT)
                try:
                    process.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    shutdown["forced_kill"] = True
                    process.kill()
                    process.wait(timeout=10)
            shutdown.update(exit_code=process.returncode, joined_at_ms=time.time_ns() // 1_000_000)
            self.compactor = None
            self.compactor_log.close()
            self.report.setdefault("compactor_shutdowns", []).append(shutdown)
            if require_clean:
                assert not shutdown["forced_kill"] and process.returncode == 0, (
                    "external compactor did not finish its catalog pass cleanly before the crash boundary")
            return shutdown

    def recover(self, barrier, name):
        """An explicit test supervisor, with every application exit reported as an incident."""
        started = time.monotonic()
        barrier = self.materialization_barrier(barrier)
        restarted = False
        deadline = started + self.args.timeout
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                self.incidents.append({"phase": name, "service": "daemon", "exit_code": self.process.returncode,
                                       "log": f"daemon-{self.generation}.log", "supervisor_restart": not restarted})
                if restarted:
                    raise RuntimeError(f"daemon exited again after service recovery; see daemon-{self.generation}.log")
                self.stop()
                self.start()
                restarted = True
            if self.compactor is not None and self.compactor.poll() is not None:
                self.incidents.append({"phase": name, "service": "external-compactor", "exit_code": self.compactor.returncode,
                                       "log": f"compactor-{self.compactor_generation}.log", "supervisor_restart": True})
                self.stop_compactor()
                self.start_compactor()
            metrics = self.metrics()
            if metrics.get("flow_materialized_lsn", 0) >= barrier:
                result = self.compare(name)
                recovery = {"phase": name, "seconds": round(time.monotonic() - started, 3),
                            "daemon_restarted": restarted, "materialized_lsn": metrics["flow_materialized_lsn"]}
                self.recovery_times.append(recovery)
                result["recovery"] = recovery
                return result
            time.sleep(0.25)
        raise TimeoutError(f"{name}: source did not recover after fault removal")

    def check_ack(self, barrier):
        confirmed = self.pg.execute("SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = %s",
                                    (self.name,)).fetchone()[0]
        metrics = self.metrics()
        if self.args.ack_mode == "journaled":
            # The metrics file is periodic; a later export can only be higher.
            durable = self.until("journal durability did not cover the source ACK", lambda: (
                value if (value := self.metrics().get("flow_journal_durable_lsn", 0)) >= lsn(confirmed) else None))
            assert lsn(confirmed) <= durable, "PostgreSQL acknowledged a transaction missing from the journal"
        else:
            assert lsn(confirmed) < barrier, "PostgreSQL acknowledged a source transaction before publication"
        assert metrics.get("flow_materialized_lsn", 0) < barrier
        assert metrics.get("flow_materialized_lsn", 0) <= metrics.get("flow_journal_durable_lsn", 0)
        return {"confirmed_flush_lsn": confirmed, "barrier": barrier, "metrics": metrics}

    def fault_writes(self, name):
        return self.transaction([
            "UPDATE orders SET amount = COALESCE(amount,0) + 0.125, payload = "
            f"'{name}' WHERE id BETWEEN 1 AND 384",
            "DELETE FROM orders WHERE id BETWEEN 500 AND 515",
            "INSERT INTO orders (id,tenant,payload) SELECT i,3,'reinserted' FROM generate_series(500,515) i",
            "UPDATE accounts SET amount = COALESCE(amount,0) + 1.25 WHERE id <= 8",
        ])

    def lost_response(self, name="lost-commit-response", fault="drop-after-successful-commit", **outcome):
        """Upstream commits, but its caller learns nothing, an error, or too late."""
        with self.proxy.lock:
            offset = len(self.proxy.events)
        self.proxy.arm_drop(**outcome)
        barrier = self.fault_writes(name)
        # Fail fast, with its log, if the daemon exits before the fault fires.
        self.until(f"proxy never hid a successful ingestion commit ({fault})", self.proxy.dropped.is_set)
        result = self.recover(barrier, name)
        # A late response is recorded only after its delayed write.
        dropped = self.until(f"{fault} was not recorded", lambda: [
            event for event in self.proxy.events[offset:] if event.get("fault") == fault])
        assert len(dropped) == 1
        assert 200 <= dropped[0]["upstream_status"] < 300
        operation = dropped[0]["operation_id"]
        assert operation
        snapshots = [snapshot for table in ("orders", "accounts") for snapshot in self.table(table)["metadata"]["snapshots"]
                     if snapshot["summary"].get("flow.operation-id") == operation]
        assert len(snapshots) == 1, "ambiguous commit was duplicated or disappeared"
        assert snapshots[0]["snapshot-id"] == dropped[0]["snapshot_id"]
        result["confirmed_upstream_commit"] = dropped[0]
        result["matching_logical_commits"] = len(snapshots)
        return result

    def unknown_status(self, status):
        return self.lost_response(f"commit-state-unknown-{status}", "unknown-status-after-successful-commit",
                                  status=status)

    def late_response(self):
        return self.lost_response("late-commit-response", "late-after-successful-commit",
                                  delay_seconds=self.args.late_response_seconds)

    def catalog_outage(self):
        self.proxy.reject = True
        try:
            barrier = self.fault_writes("catalog-unavailable")
            time.sleep(self.args.fault_seconds)
            evidence = self.check_ack(barrier)
        finally:
            self.proxy.reject = False
        result = self.recover(barrier, "catalog-unavailable")
        result["during_fault"] = evidence
        result["rejected_requests"] = sum(event.get("fault") == "reject-before-upstream" for event in self.proxy.events)
        assert result["rejected_requests"] > 0
        return result

    def wait_http(self, url):
        deadline = time.monotonic() + 45
        while time.monotonic() < deadline:
            try:
                with urlopen(url, timeout=2) as response:
                    if response.status == 200:
                        return
            except (OSError, URLError):
                pass
            time.sleep(0.25)
        raise TimeoutError(f"service did not recover at {url}")

    def service_outage(self, service, health):
        before = {name: self.table(name)["metadata"]["table-uuid"] for name in ("orders", "accounts")}
        self.compose("stop", "-t", "1", service)
        try:
            barrier = self.fault_writes(service + "-restart")
            time.sleep(self.args.fault_seconds)
            evidence = self.check_ack(barrier)
        finally:
            self.compose("start", service)
            self.wait_http(health)
        result = self.recover(barrier, service + "-restart")
        after = {name: self.table(name)["metadata"]["table-uuid"] for name in ("orders", "accounts")}
        assert before == after, "table identity changed across service restart"
        result.update(during_fault=evidence, persistent_table_uuids=after)
        return result

    def postgres_restart(self):
        before = self.pg.execute("SELECT system_identifier::text FROM pg_control_system()").fetchone()[0]
        self.pg.close()
        self.compose("restart", "-t", "1", "postgres")
        self.catalog_database_restarted = True
        deadline = time.monotonic() + 45
        while True:
            try:
                self.pg = psycopg.connect(self.args.postgres_url, autocommit=True, connect_timeout=2)
                break
            except psycopg.OperationalError:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(0.25)
        self.pg.execute("SET timezone = 'UTC'")
        self.pg.execute(sql.SQL("SET search_path TO {}, public").format(sql.Identifier(self.name)))
        after = self.pg.execute("SELECT system_identifier::text FROM pg_control_system()").fetchone()[0]
        assert before == after
        barrier = self.fault_writes("postgres-restart")
        # This fixture stores its catalog in the same PostgreSQL server. Its
        # JDBC pool can retain terminated connections even though /v1/config is
        # healthy, so readiness must exercise actual table reads.
        catalog_ready = False
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            try:
                for name in ("orders", "accounts"):
                    self.table(name)
                catalog_ready = True
                break
            except (OSError, URLError):
                time.sleep(0.5)
        if not catalog_ready:
            self.incidents.append({"phase": "postgres-restart", "service": "catalog",
                                   "reason": "JDBC catalog table reads failed after database restart",
                                   "supervisor_restart": True})
            self.compose("restart", "-t", "1", "rest")
            self.wait_http(self.upstream + "/v1/config")
            for name in ("orders", "accounts"):
                self.table(name)
        result = self.recover(barrier, "postgres-restart")
        result["system_identifier_preserved"] = after
        result["catalog_pool_recovered_without_restart"] = catalog_ready
        return result

    def durable_backlog_crash(self, lose_index=False):
        shutdown = self.stop_compactor(require_clean=True)
        # Publication of a fresh mutation on both tables must first reconcile
        # each table's index with the compactor's final catalog head. Take the
        # crash-specific log and request offsets only after this checkpoint.
        heads = {name: self.table(name)["metadata"]["current-snapshot-id"]
                 for name in ("orders", "accounts")}
        barrier = self.transaction([
            "UPDATE orders SET amount = COALESCE(amount,0) + 0.0001 WHERE id = 1",
            "UPDATE accounts SET amount = COALESCE(amount,0) + 0.0001 WHERE id = 1",
        ])
        checkpoint = {"phase": self.current_phase, "compactor_shutdown": shutdown,
                      "catalog_heads_after_join": heads, "barrier": barrier,
                      "materialized_metrics": self.wait_materialized(barrier),
                      "rows": self.compare(f"{self.current_phase}-compactor-reconciled"),
                      "completed_at_ms": time.time_ns() // 1_000_000}
        self.report.setdefault("forced_success_checkpoints", []).append(checkpoint)
        result = self._durable_backlog_crash(lose_index)
        result["external_compactor_checkpoint"] = checkpoint
        return result

    def _durable_backlog_crash(self, lose_index):
        old_log = self.directory / f"daemon-{self.generation}.log"
        log_offset = old_log.stat().st_size
        with self.proxy.lock:
            event_offset = len(self.proxy.events)
        self.proxy.hold_commits()
        try:
            barrier = self.transaction([
                "INSERT INTO orders (id,tenant,payload) SELECT i,4,repeat(md5(i::text),20) "
                f"FROM generate_series({410000 if lose_index else 400000},{413999 if lose_index else 403999}) i",
                "UPDATE orders SET amount = -111.125 WHERE id BETWEEN 1 AND 256",
                "UPDATE accounts SET amount = -222.25 WHERE id <= 8",
            ])
            assert self.proxy.commit_held.wait(timeout=30), "no catalog ingestion commit was held"
            self.until("transaction was not journaled while catalog commit was held", lambda:
                       self.metrics().get("flow_journal_durable_lsn", 0) >= barrier, timeout=30)
            evidence = self.check_ack(barrier)
            old_process = self.process
            self.stop(crash=True)
            assert old_process.returncode == -signal.SIGKILL
            prepared = set()
            with old_log.open("rb") as log:
                log.seek(log_offset)
                for line in log:
                    try:
                        event = json.loads(line).get("fields", {})
                    except ValueError:
                        continue
                    if event.get("event") == "ingest_prepared":
                        prepared.add(event["operation_id"])
            assert prepared, "killed daemon did not record a Prepared operation in this crash phase"
            evidence.update(old_pid=old_process.pid, old_exit_code=old_process.returncode,
                            old_stopped_at_ms=time.time_ns() // 1_000_000,
                            prepared_operation_ids=sorted(prepared), proxy_event_offset=event_offset)
        finally:
            self.proxy.release_commits.set()
        # Require a current Prepared request to commit after its caller dies.
        # Other requests may still race with recovery, but earlier crash phases
        # and unsuccessful requests cannot satisfy this boundary.
        completed = []
        deadline = time.monotonic() + 35
        while time.monotonic() < deadline:
            with self.proxy.lock:
                completed = [event for event in self.proxy.events[event_offset:]
                             if event.get("held_before_upstream") and event.get("operation_id") in prepared
                             and 200 <= event.get("upstream_status", 0) < 300]
            if completed:
                break
            time.sleep(0.1)
        assert completed, "this crash phase's held Prepared request did not succeed upstream"
        assert old_process.returncode == -signal.SIGKILL and self.process is None
        evidence["upstream_success_confirmed_at_ms"] = time.time_ns() // 1_000_000
        moved = []
        if lose_index:
            missing = self.directory / "lost-row-index"
            missing.mkdir()
            for name in ("index", "index-generations"):
                path = self.directory / "state" / name
                if path.exists():
                    path.rename(missing / name)
                    moved.append(name)
            assert moved, "no replaceable row index was removed"
            assert (self.directory / "state" / "control" / "CURRENT").exists()
        evidence["replacement_start_requested_at_ms"] = time.time_ns() // 1_000_000
        self.start()
        result = self.recover(barrier, "durable-backlog-sigkill")
        result.update(during_fault=evidence, completed_held_requests=completed, inserted_rows=4000)
        if lose_index:
            log = (self.directory / f"daemon-{self.generation}.log").read_text()
            assert "index_generation_activated" in log, "startup did not rebuild the missing row index"
            result["removed_index_directories"] = moved
            barrier = self.transaction(["UPDATE orders SET payload='after-index-rebuild' WHERE id BETWEEN 410000 AND 410031",
                                        "DELETE FROM orders WHERE id BETWEEN 410032 AND 410063",
                                        "UPDATE orders SET id=id+1000000 WHERE id BETWEEN 410064 AND 410079"])
            self.wait_materialized(barrier)
            result["post_rebuild_updates_deletes_key_moves"] = self.compare("post-index-rebuild")
        return result

    def wide_rows(self):
        barrier = self.transaction([
            "INSERT INTO orders (id,tenant,payload,data) SELECT 900000 + i,8,"
            "string_agg(md5((i * 1000 + j)::text), ''),decode(string_agg(md5((i * 2000 + j)::text), ''),'hex') "
            "FROM generate_series(1,20) i CROSS JOIN generate_series(1,512) j GROUP BY i",
        ])
        self.wait_materialized(barrier)
        inserted = self.compare("wide-row-insert")
        lengths = self.pg.execute("SELECT min(octet_length(payload)), min(octet_length(data)) FROM orders WHERE id BETWEEN 900001 AND 900020").fetchone()
        assert lengths == (16384, 8192)
        barrier = self.transaction([
            "UPDATE orders SET tenant = tenant + 1, amount = 987.6543 WHERE id BETWEEN 900001 AND 900020",
            "UPDATE accounts SET amount = amount + 0.5 WHERE id = 1",
        ])
        # This must continue to ingest. A safe-but-unsupported TOAST rejection is
        # still a functional failure and must not be disguised as a passing test.
        self.wait_materialized(barrier)
        result = self.compare("unchanged-toast-update")
        barrier = self.transaction([
            "UPDATE orders SET payload = reverse(payload), data = substring(data FROM 2) WHERE id BETWEEN 900001 AND 900010",
            "UPDATE orders SET id = id + 1000 WHERE id BETWEEN 900011 AND 900015",
            "DELETE FROM orders WHERE id BETWEEN 900016 AND 900020",
        ])
        self.wait_materialized(barrier)
        result["subsequent_changes"] = self.compare("toast-change-key-move-delete")
        result.update(inserted=inserted, text_bytes=lengths[0], binary_bytes=lengths[1])
        return result

    def final_audit(self):
        if self.compactor is not None and self.compactor.poll() is not None:
            raise RuntimeError(f"external compactor exited unexpectedly; see compactor-{self.compactor_generation}.log")
        result = self.compare("final")
        result["manifests"] = {name: self.audit(name) for name in ("orders", "accounts")}
        if self.compactor is not None:
            result["external_compaction_commits"] = sum(
                "local-test.compaction-id" in snapshot["summary"]
                for name in ("orders", "accounts") for snapshot in self.table(name)["metadata"]["snapshots"])
            assert result["external_compaction_commits"] > 0, "external service never committed a compaction"
        assert self.rows("orders", self.initial_metadata) == self.initial_rows
        result["initial_snapshot_rows_unchanged"] = len(self.initial_rows)
        self.report["availability_without_supervisor_restart"] = not self.incidents
        self.report["correctness_and_recovery_passed"] = True
        return result

    def execute(self):
        try:
            self.phase("seed", self.seed)
            self.phase("initial-copy", self.initialize)
            self.start()
            self.phase("snapshot-wal-handoff", self.handoff)
            self.start_compactor()
            self.phase("actual-committed-response-loss", self.lost_response)
            for status in (500, 502, 504):
                self.phase(f"committed-then-reported-{status}", lambda status=status: self.unknown_status(status))
            self.phase("committed-response-after-client-timeout", self.late_response)
            self.phase("catalog-http-outage", self.catalog_outage)
            self.phase("durable-catalog-service-restart", lambda: self.service_outage("rest", self.upstream + "/v1/config"))
            self.phase("durable-object-store-service-restart", lambda: self.service_outage("minio", self.args.s3_endpoint + "/minio/health/live"))
            self.phase("postgres-service-restart", self.postgres_restart)
            self.phase("sigkill-with-journaled-unacknowledged-work", self.durable_backlog_crash)
            self.phase("catalog-commit-and-physical-index-loss", lambda: self.durable_backlog_crash(lose_index=True))
            # Keep one graceful pause across the adjacent forced-success checks;
            # do not signal a newly started compactor before its handler is ready.
            self.start_compactor()
            self.phase("wide-rows-and-unchanged-toast", self.wide_rows)
            self.phase("independent-reader-and-manifest-audit", self.final_audit)
            self.stop_compactor()
            self.stop()
            self.check_worker_panics()
            self.report["passed"] = True
        except BaseException as error:
            self.report["failure"] = str(error)
            self.report["traceback"] = traceback.format_exc()
            raise
        finally:
            self.stop_compactor()
            self.stop()
            if self.proxy is not None:
                self.proxy.close()
            dump(self.directory / "report.json", self.report)
            self.pg.close()
            self.duck.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--postgres-url", default=os.environ.get("FLOW_POSTGRES_URL"))
    parser.add_argument("--catalog-uri", required=True)
    parser.add_argument("--s3-endpoint", required=True)
    parser.add_argument("--warehouse", default="s3://warehouse/")
    parser.add_argument("--binary", type=Path, default=Path("target/release/embrasure-flow"))
    parser.add_argument("--compactor-binary", type=Path)
    parser.add_argument("--compose-project", required=True)
    parser.add_argument("--compose-file", type=Path, default=Path("tests/production/compose.yaml"))
    parser.add_argument("--compose-env-file", type=Path)
    parser.add_argument("--artifacts", type=Path, required=True)
    parser.add_argument("--timeout", type=float, default=180)
    parser.add_argument("--fault-seconds", type=float, default=5)
    parser.add_argument("--late-response-seconds", type=float, default=65,
                        help="hold one committed response beyond the daemon's 60-second catalog request timeout")
    parser.add_argument("--format-version", type=int, choices=(2, 3), default=2)
    parser.add_argument("--ack-mode", choices=("materialized", "journaled"), default="materialized",
                        help="journaled declares independent journal storage and checks ACK <= journal durability")
    args = parser.parse_args()
    if not args.postgres_url:
        parser.error("provide --postgres-url or FLOW_POSTGRES_URL")
    if args.fault_seconds < 1 or args.timeout <= 0:
        parser.error("fault duration must be at least one second and timeout must be positive")
    FaultRun(args).execute()
    print(f"PASS: {args.artifacts.resolve() / 'report.json'}", flush=True)


if __name__ == "__main__":
    main()
