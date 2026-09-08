#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["duckdb==1.5.5", "psycopg[binary]==3.3.5", "boto3==1.43.88", "fastavro==1.12.2", "pytz==2026.3.post1"]
# ///
"""Stall WAL-health queries; verify readiness recovery, CDC and shutdown."""

import argparse
import json
import os
from pathlib import Path
import select
import signal
import socket
import socketserver
import struct
import sys
import threading
import time
import traceback

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "local"))
from run import Run, dump
from proxy import CatalogProxy
from psycopg.conninfo import conninfo_to_dict, make_conninfo


def receive(sock, size):
    chunks = bytearray()
    while len(chunks) < size:
        part = sock.recv(size - len(chunks))
        if not part:
            raise EOFError
        chunks.extend(part)
    return bytes(chunks)


class HealthProxy:
    """Plaintext PostgreSQL relay, restricted to this disposable test connection."""

    def __init__(self, connection, trace, query_fragments=None):
        settings = conninfo_to_dict(connection)
        upstream = (settings.get("host", "127.0.0.1"), int(settings.get("port", 5432)))
        self.trace = trace
        self.query_fragments = tuple(query_fragments) if query_fragments is not None else (
            b"pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)",)
        if not self.query_fragments or any(not fragment for fragment in self.query_fragments):
            raise ValueError("query hold requires nonempty SQL fragments")
        self.lock = threading.Lock()
        self.active = self.peak = self.started = 0
        self.completed = []
        self.query_held = threading.Event()
        self.stopped = threading.Event()
        self.stall_health = threading.Event()
        self.stall_health.set()
        proxy = self

        class Server(socketserver.ThreadingTCPServer):
            daemon_threads = True

        class Handler(socketserver.BaseRequestHandler):
            def handle(self):
                held = None
                with socket.create_connection(upstream, timeout=5) as remote:
                    remote.settimeout(None)

                    def backend():
                        try:
                            while data := remote.recv(65536):
                                self.request.sendall(data)
                        except OSError:
                            pass

                    reader = threading.Thread(target=backend, daemon=True)
                    reader.start()
                    try:
                        header = receive(self.request, 4)
                        remote.sendall(header + receive(self.request, struct.unpack("!I", header)[0] - 4))
                        while not proxy.stopped.is_set():
                            kind = receive(self.request, 1)
                            header = receive(self.request, 4)
                            body = receive(self.request, struct.unpack("!I", header)[0] - 4)
                            if (proxy.stall_health.is_set() and kind in (b"P", b"Q")
                                    and all(fragment in body for fragment in proxy.query_fragments)):
                                held = time.monotonic()
                                with proxy.lock:
                                    proxy.started += 1
                                    proxy.active += 1
                                    proxy.peak = max(proxy.peak, proxy.active)
                                proxy.query_held.set()
                                # Keep this query unsent until its client cancels. Other
                                # connections, including replication, remain untouched.
                                while not proxy.stopped.is_set():
                                    ready, _, _ = select.select([self.request], [], [], 0.1)
                                    if ready:
                                        data = self.request.recv(65536)
                                        if not data or data[:1] == b"X":
                                            break
                                break
                            remote.sendall(kind + header + body)
                    except (EOFError, OSError):
                        pass
                    finally:
                        if held is not None:
                            with proxy.lock:
                                proxy.active -= 1
                                proxy.completed.append(time.monotonic() - held)
                                dump(proxy.trace, {"started": proxy.started, "peak_inflight": proxy.peak,
                                                   "completed_seconds": proxy.completed})
                        for sock in (remote, self.request):
                            try:
                                sock.shutdown(socket.SHUT_RDWR)
                            except OSError:
                                pass
                        reader.join(timeout=2)

        self.server = Server(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.connection = make_conninfo(**(settings | {"host": "127.0.0.1",
            "port": str(self.server.server_address[1]), "sslmode": "disable"}))

    def close(self):
        self.stopped.set()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=3)


class CountedCatalog(CatalogProxy):
    def __init__(self, upstream, trace):
        self.active_posts = self.peak_posts = 0
        super().__init__(upstream, trace)

    def forward(self, handler):
        commit = handler.command == "POST" and "/tables/" in handler.path
        if commit:
            with self.lock:
                self.active_posts += 1
                self.peak_posts = max(self.peak_posts, self.active_posts)
        try:
            super().forward(handler)
        finally:
            if commit:
                with self.lock:
                    self.active_posts -= 1


class HealthRun(Run):
    def __init__(self, args):
        super().__init__(args)
        self.health = HealthProxy(args.postgres_url, self.directory / "health-proxy.json")
        self.catalog_proxy = CountedCatalog(args.catalog_uri, self.directory / "catalog-proxy.jsonl")
        args.catalog_uri = self.catalog_proxy.url
        self.configure()
        self.config.write_text(self.config.read_text().replace("pending_transactions = 32",
            "pending_transactions = 32\ntable_workers = 1\ncheckpoint_interval_secs = 1\nretained_checkpoints = 2"))
        self.environment.update(FLOW_LOCAL_POSTGRES_URL=self.health.connection, RUST_LOG="info")

    def blocked_health_cdc(self):
        self.catalog_proxy.peak_posts = 0  # Initial COPY has its own worker budget.
        self.catalog_proxy.hold_commits()
        self.start()
        try:
            self.until("WAL health query was not intercepted", lambda: self.health.active == 1, timeout=10)
            barrier = self.transaction([
                "UPDATE orders SET payload='health-query-is-blocked' WHERE id <= 32",
                "DELETE FROM orders WHERE id BETWEEN 40 AND 47",
                "UPDATE orders SET id=id+100000 WHERE id BETWEEN 48 AND 55",
                "INSERT INTO orders (id,tenant,payload) VALUES (9000,3,'new')",
                "UPDATE accounts SET amount=amount+1 WHERE id <= 8",
            ])
            assert self.catalog_proxy.commit_held.wait(timeout=2), "no table publication reached the catalog"
            time.sleep(1)
            assert self.catalog_proxy.peak_posts == 1, "table_workers=1 admitted concurrent table publications"
        finally:
            self.catalog_proxy.release_commits.set()
        # Status updates on every completion. Metrics can wait for the five-second
        # idle tick, which coincides with the injected query timeout.
        self.until("CDC did not finish before the stalled health query timed out",
            lambda: json.loads((self.directory / "state/status.json").read_text())
                ["watermarks"]["materialized_lsn"] >= barrier, timeout=3)
        assert self.health.active == 1 and not self.health.completed, \
            "CDC waited for the stalled health query instead of progressing independently"
        result = self.compare("cdc-during-stalled-health")
        result["peak_concurrent_table_posts"] = self.catalog_proxy.peak_posts
        return result

    def timeout_checkpoint_shutdown(self):
        self.until("health query did not time out", lambda: self.health.completed, timeout=8)
        assert 4 <= self.health.completed[0] < 7, self.health.completed

        def source_status(health, ready):
            status = json.loads((self.directory / "state/status.json").read_text())
            return status if status["source_health"] == health and status["ready"] == ready else None

        unavailable = self.until("timed-out health query did not clear readiness",
            lambda: source_status("unavailable", False), timeout=3)
        self.until("health retry was not started", lambda: self.health.started >= 2 and self.health.active == 1, timeout=12)
        barrier = self.transaction([
            "UPDATE orders SET payload='health-unavailable' WHERE id <= 16",
            "DELETE FROM orders WHERE id BETWEEN 20 AND 23",
            "INSERT INTO orders (id,tenant,payload) VALUES (9001,3,'after-health-timeout')",
        ])
        self.wait_materialized(barrier)
        cdc = self.compare("cdc-with-unavailable-source-health")
        assert source_status("unavailable", False), "CDC publication incorrectly restored readiness"
        self.until("checkpoint did not complete beside stalled health", lambda:
            (self.directory / "state/checkpoints").exists()
            and any((self.directory / "state/checkpoints").iterdir()), timeout=3)

        # A held query remains blocked until cancellation; subsequent connections
        # pass through normally once the current timeout has closed its socket.
        self.health.stall_health.clear()
        recovered = self.until("successful health query did not restore readiness",
            lambda: source_status("healthy", True), timeout=15)
        self.health.stall_health.set()
        self.until("health query was not stalled again before shutdown",
            lambda: self.health.active == 1, timeout=10)
        started = time.monotonic()
        self.process.send_signal(signal.SIGTERM)
        code = self.process.wait(timeout=3)
        elapsed = time.monotonic() - started
        assert code == 0, f"SIGTERM failed with exit code {code}"
        deadline = time.monotonic() + 2
        while self.health.active and time.monotonic() < deadline:
            time.sleep(0.05)
        assert self.health.active == 0, "shutdown did not cancel health query"
        status = json.loads((self.directory / "state/status.json").read_text())
        assert not status["ready"], status
        assert self.health.peak == 1, "health checks overlapped"
        assert self.catalog_proxy.peak_posts == 1, "worker limit was exceeded"
        return {"health_timeout_seconds": self.health.completed[0], "health_queries": self.health.started,
                "health_peak_inflight": self.health.peak, "sigterm_seconds": elapsed,
                "unavailable_status": unavailable, "recovered_status": recovered,
                "cdc_while_unavailable": cdc, "exit_code": code, "ready": status["ready"]}

    def execute(self):
        try:
            self.phase("seed", self.seed)
            self.phase("initial-copy", self.initialize)
            self.phase("cdc-and-worker-admission-during-stalled-health", self.blocked_health_cdc)
            self.phase("health-timeout-checkpoint-and-sigterm", self.timeout_checkpoint_shutdown)
            self.report["passed"] = True
        except BaseException as error:
            self.report.update(error=str(error), traceback=traceback.format_exc())
            raise
        finally:
            self.stop()
            self.health.close()
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
    parser.add_argument("--timeout", type=float, default=90)
    args = parser.parse_args()
    if not args.postgres_url:
        parser.error("provide --postgres-url or FLOW_POSTGRES_URL")
    HealthRun(args).execute()
    print(f"PASS: {args.artifacts.resolve() / 'report.json'}", flush=True)


if __name__ == "__main__":
    main()
