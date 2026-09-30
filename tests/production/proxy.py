#!/usr/bin/env python3
"""A loopback REST proxy for deterministic catalog failure injection."""

import argparse
import http.client
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import socket
import threading
import time
from urllib.parse import quote, urlsplit


class CatalogProxy:
    """Control faults directly from the test process; never expose a control API."""

    def __init__(self, upstream, trace, port=0):
        self.upstream = urlsplit(upstream)
        if self.upstream.scheme not in ("http", "https") or not self.upstream.netloc:
            raise ValueError("upstream must be an HTTP(S) URL")
        self.trace = Path(trace)
        self.lock = threading.Lock()
        self.reject = False
        self.table_rejections = {}
        self.delay_seconds = 0
        self.drop_remaining = 0
        self.drop_kind = "ingest"
        self.drop_table = None
        self.reject_after_drop = None
        self.drop_status = None
        self.drop_delay = 0
        self.hold_kind = "ingest"
        self.hold_table = None
        self.dropped = threading.Event()
        self.commit_held = threading.Event()
        self.release_commits = threading.Event()
        self.release_commits.set()
        self.events = []
        proxy = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"
            # Send small response bodies without a delayed-ACK wait after headers.
            disable_nagle_algorithm = True

            def setup(self):
                super().setup()
                self.connection.settimeout(30)
                factory = http.client.HTTPSConnection if proxy.upstream.scheme == "https" else http.client.HTTPConnection
                self.upstream_connection = factory(proxy.upstream.hostname, proxy.upstream.port, timeout=30)

            def finish(self):
                try:
                    super().finish()
                finally:
                    self.upstream_connection.close()

            def handle_one_request(self):
                self.raw_requestline = None
                try:
                    super().handle_one_request()
                except ConnectionResetError:
                    if self.raw_requestline is not None:
                        raise
                    # An idle keepalive peer can reset before its next request.
                    self.close_connection = True

            def log_message(self, *_args):
                pass

            def do_GET(self):
                proxy.forward(self)

            do_POST = do_GET
            do_HEAD = do_GET
            do_DELETE = do_GET

        self.server = ThreadingHTTPServer(("127.0.0.1", port), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, name="catalog-proxy", daemon=True)
        self.thread.start()
        self.url = f"http://127.0.0.1:{self.server.server_port}"

    def close(self):
        self.release_commits.set()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)

    def reject_table(self, table, status=503, methods=None):
        """Reject this table before upstream; other tables and config stay live."""
        if status not in (403, 503):
            raise ValueError("table fault status must be 403 or 503")
        with self.lock:
            self.table_rejections[table] = (status, frozenset(methods) if methods is not None else None)

    def allow_table(self, table):
        with self.lock:
            self.table_rejections.pop(table, None)

    def arm_drop(self, kind="ingest", table=None, reject_after=None, status=None, delay_seconds=0):
        """Hide the outcome of the next successful matching commit from its caller.

        By default the connection closes without a response. `status` instead
        replaces the upstream success with a commit-state-unknown 500, 502 or
        504; `delay_seconds` holds the real response past the client's timeout.
        """
        if reject_after is not None and (table is None or reject_after not in (403, 503)):
            raise ValueError("post-commit rejection requires a table and status 403 or 503")
        if status is not None and status not in (500, 502, 504):
            raise ValueError("an unknown commit outcome is reported as 500, 502 or 504")
        if status is not None and delay_seconds:
            raise ValueError("choose a rewritten status or a late response, not both")
        with self.lock:
            self.drop_remaining = 1
            self.drop_kind = kind
            self.drop_table = table
            self.reject_after_drop = reject_after
            self.drop_status = status
            self.drop_delay = delay_seconds
            self.dropped.clear()

    def hold_commits(self, kind="ingest", table=None):
        self.hold_kind = kind
        self.hold_table = table
        self.commit_held.clear()
        self.release_commits.clear()

    def record(self, event):
        with self.lock:
            self.events.append(event)
            with self.trace.open("a") as output:
                output.write(json.dumps(event) + "\n")

    def forward(self, handler):
        started = time.monotonic()
        event = {"unix_ms": time.time_ns() // 1_000_000, "method": handler.command, "path": handler.path}
        body = handler.rfile.read(int(handler.headers.get("Content-Length", "0")))
        try:
            request = json.loads(body) if body else {}
        except (ValueError, UnicodeDecodeError):
            request = {}
        snapshots = [update.get("snapshot", {}) for update in request.get("updates", [])
                     if update.get("action") == "add-snapshot"]
        ingestion = next((snapshot for snapshot in snapshots
                          if snapshot.get("summary", {}).get("streaming.operation") == "ingest"), None)
        operations = {snapshot.get("summary", {}).get("streaming.operation") for snapshot in snapshots}
        if operations:
            event["operations"] = sorted(operation for operation in operations if operation is not None)
        schema_update = any(update.get("action") == "add-schema" for update in request.get("updates", []))
        if schema_update:
            event["schema_update"] = True
        if ingestion:
            event["operation_id"] = ingestion.get("summary", {}).get("flow.operation-id")
            event["snapshot_id"] = ingestion.get("snapshot-id")
        connection = handler.upstream_connection
        try:
            if self.reject:
                event["fault"] = "reject-before-upstream"
                self.respond(handler, 503, b'{"error":{"message":"injected catalog outage","type":"ServiceUnavailableException","code":503}}')
                return
            with self.lock:
                rejection = next(((table, status) for table, (status, methods) in self.table_rejections.items()
                                  if self.matches_table(handler.path, table)
                                  and (methods is None or handler.command in methods)), None)
            if rejection is not None:
                table, status = rejection
                event.update(fault="reject-table-before-upstream", table=table, status=status)
                error_type = "ForbiddenException" if status == 403 else "ServiceUnavailableException"
                self.respond(handler, status, json.dumps({"error": {
                    "message": "injected table catalog fault", "type": error_type, "code": status,
                }}).encode())
                return
            if self.delay_seconds:
                time.sleep(self.delay_seconds)
            if (self.hold_kind in operations and not self.release_commits.is_set()
                    and (self.hold_table is None or self.matches_table(handler.path, self.hold_table))):
                event["held_before_upstream"] = True
                self.commit_held.set()
                if not self.release_commits.wait(timeout=45):
                    raise TimeoutError("test did not release held catalog commit within 45 seconds")
            headers = {key: value for key, value in handler.headers.items()
                       if key.lower() not in ("host", "connection", "content-length", "transfer-encoding")}
            connection.request(handler.command, self.upstream.path.rstrip("/") + handler.path, body, headers)
            response = connection.getresponse()
            data = response.read()
            event["upstream_status"] = response.status
            should_drop = False
            if (self.drop_kind in operations or schema_update and self.drop_kind == "schema") and 200 <= response.status < 300:
                with self.lock:
                    if self.drop_remaining and (self.drop_table is None or self.matches_table(handler.path, self.drop_table)):
                        self.drop_remaining -= 1
                        should_drop = True
                        if self.reject_after_drop is not None:
                            # Install before disconnecting: recovery reads cannot race
                            # ahead of the fixture's post-commit failure boundary.
                            self.table_rejections[self.drop_table] = (self.reject_after_drop, None)
            if should_drop and self.drop_status is not None:
                # Upstream committed, but the client is told the outcome is unknown,
                # as a gateway or catalog may after an interrupted commit.
                status = self.drop_status
                event.update(fault="unknown-status-after-successful-commit", rewritten_status=status)
                self.dropped.set()
                handler.close_connection = True
                self.respond(handler, status, json.dumps({"error": {
                    "message": "injected unknown commit outcome", "type": "CommitStateUnknownException",
                    "code": status,
                }}).encode())
            elif should_drop and self.drop_delay:
                # The response arrives only after the client's own deadline.
                event.update(fault="late-after-successful-commit", delay_seconds=self.drop_delay)
                self.dropped.set()
                handler.close_connection = True
                time.sleep(self.drop_delay)
                try:
                    self.respond(handler, response.status, data, response.getheader("Content-Type", "application/json"))
                except OSError as error:
                    event["late_response_error"] = str(error)
            elif should_drop:
                # Upstream has durably accepted the actual add-snapshot request. Closing
                # without headers prevents the client from learning the outcome.
                event["fault"] = "drop-after-successful-commit"
                self.dropped.set()
                handler.close_connection = True
                handler.connection.shutdown(socket.SHUT_RDWR)
                handler.connection.close()
            else:
                self.respond(handler, response.status, data, response.getheader("Content-Type", "application/json"),
                             response.getheader("Content-Length"))
        except (OSError, http.client.HTTPException, TimeoutError) as error:
            event["transport_error"] = str(error)
            # A write may already have committed. Close both legs without replay.
            handler.close_connection = True
            connection.close()
            try:
                self.respond(handler, 502, b'{"error":{"message":"catalog proxy transport failure","type":"ServiceUnavailableException","code":502}}')
            except OSError:
                pass
        finally:
            event["elapsed_ms"] = round((time.monotonic() - started) * 1000, 3)
            self.record(event)

    @staticmethod
    def matches_table(path, table):
        return urlsplit(path).path.endswith("/tables/" + quote(table, safe=""))

    @staticmethod
    def respond(handler, status, body, content_type="application/json", content_length=None):
        handler.send_response(status)
        handler.send_header("Content-Type", content_type)
        bodyless = handler.command == "HEAD" or status in (204, 304) or 100 <= status < 200
        if status != 204 and not 100 <= status < 200:
            if bodyless:
                if content_length is not None:
                    handler.send_header("Content-Length", content_length)
            else:
                handler.send_header("Content-Length", str(len(body)))
        if handler.close_connection:
            handler.send_header("Connection", "close")
        handler.end_headers()
        if not bodyless:
            handler.wfile.write(body)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--upstream", required=True)
    parser.add_argument("--trace", type=Path, required=True)
    parser.add_argument("--port", type=int, default=0)
    args = parser.parse_args()
    proxy = CatalogProxy(args.upstream, args.trace, args.port)
    print(proxy.url, flush=True)
    try:
        threading.Event().wait()
    except KeyboardInterrupt:
        pass
    finally:
        proxy.close()


if __name__ == "__main__":
    main()
