"""Catalog read recovery must not turn real failures into passing fault tests."""
import contextlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import threading
import time
import unittest
from unittest.mock import patch
from urllib.error import HTTPError

from faults import FaultRun


@contextlib.contextmanager
def catalog(statuses):
    requests = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            code = statuses[min(len(requests), len(statuses) - 1)]
            requests.append(self.path)
            body = b'{"metadata":{"current-snapshot-id":42}}'
            self.send_response(code)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    run = FaultRun.__new__(FaultRun)
    run.upstream = f"http://127.0.0.1:{server.server_port}"
    run.name = "fixture"
    run.current_phase = "after-postgres-restart"
    run.catalog_database_restarted = True
    run.incidents = []
    try:
        yield run, requests
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()


class CatalogReadRecoveryTests(unittest.TestCase):
    def test_closed_pooled_connection_retries_and_records_incident(self):
        with catalog([500, 200]) as (run, requests):
            self.assertEqual(run.table("orders")["metadata"]["current-snapshot-id"], 42)
            self.assertEqual(len(requests), 2)
            self.assertEqual(run.incidents[0]["http_status"], 500)

    def test_missing_or_forbidden_table_is_not_retried(self):
        for code in (403, 404):
            with self.subTest(code=code), catalog([code, 200]) as (run, requests):
                with self.assertRaises(HTTPError) as caught:
                    run.table("orders")
                self.assertEqual(caught.exception.code, code)
                self.assertEqual(len(requests), 1)
                self.assertEqual(run.incidents, [])

    def test_server_error_before_injected_restart_is_not_retried(self):
        with catalog([500, 200]) as (run, requests):
            run.catalog_database_restarted = False
            with self.assertRaises(HTTPError):
                run.table("orders")
            self.assertEqual(len(requests), 1)

    def test_persistent_server_error_exhausts_and_preserves_http_error(self):
        with catalog([500]) as (run, requests), patch("faults.CATALOG_READ_RECOVERY_SECONDS", 0.05):
            started = time.monotonic()
            with self.assertRaises(HTTPError) as caught:
                run.table("orders")
            self.assertEqual(caught.exception.code, 500)
            self.assertGreaterEqual(len(requests), 2)
            self.assertLess(time.monotonic() - started, 1)


if __name__ == "__main__":
    unittest.main()
