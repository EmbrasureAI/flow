"""Local HTTP checks for connection reuse without changing fault semantics."""

from contextlib import contextmanager
import http.client
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import socket
import struct
import tempfile
import threading
import time
import unittest
from urllib.parse import urlsplit

from proxy import CatalogProxy
from s3_proxy import S3ReadProxy


@contextmanager
def fixture(proxy_type):
    requests = []
    accepted = []

    class Server(ThreadingHTTPServer):
        daemon_threads = True

        def get_request(self):
            connection, address = super().get_request()
            accepted.append(address)
            return connection, address

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *_args):
            pass

        def do_GET(self):
            body = b"".join(S3ReadProxy.request_body(self))
            requests.append((self.command, self.path, dict(self.headers), body))
            if self.path == "/lost":
                self.close_connection = True
                self.connection.shutdown(socket.SHUT_RDWR)
                self.connection.close()
                return
            if self.path == "/chunks":
                self.send_response(200)
                self.send_header("Transfer-Encoding", "chunked")
                self.end_headers()
                self.wfile.write(b"3\r\nabc\r\n2\r\nde\r\n0\r\n\r\n")
                return
            status = 204 if self.command == "DELETE" else 200
            self.send_response(status)
            if status != 204:
                self.send_header("Content-Length", "5")
            self.end_headers()
            if self.command != "HEAD" and status != 204:
                self.wfile.write(b"ab" if self.path == "/short" else b"abcde")
            if self.path == "/short":
                self.close_connection = True

        do_HEAD = do_GET
        do_DELETE = do_GET
        do_POST = do_GET
        do_PUT = do_GET

    upstream = Server(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=upstream.serve_forever, daemon=True)
    thread.start()
    with tempfile.TemporaryDirectory() as directory:
        proxy = proxy_type(f"http://127.0.0.1:{upstream.server_port}", Path(directory) / "trace.jsonl")
        endpoint = urlsplit(proxy.url)
        client = http.client.HTTPConnection(endpoint.hostname, endpoint.port, timeout=3)
        try:
            yield proxy, client, requests, accepted
        finally:
            client.close()
            proxy.close()
            upstream.shutdown()
            upstream.server_close()
            thread.join(timeout=3)


class ProxyTests(unittest.TestCase):
    def test_idle_peer_reset_finishes_without_server_error(self):
        for proxy_type in (CatalogProxy, S3ReadProxy):
            with self.subTest(proxy=proxy_type.__name__), fixture(proxy_type) as (proxy, client, _, _):
                errors = []
                finished = threading.Event()
                original_shutdown = proxy.server.shutdown_request

                def shutdown(request):
                    try:
                        original_shutdown(request)
                    finally:
                        finished.set()

                proxy.server.shutdown_request = shutdown
                proxy.server.handle_error = lambda *_args: errors.append("unexpected server error")
                client.request("GET", "/object")
                client.getresponse().read()
                client.sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
                client.close()
                self.assertTrue(finished.wait(timeout=3))
                self.assertEqual(errors, [])

    def test_reuses_both_connections_across_get_head_and_bodyless_delete(self):
        for proxy_type in (CatalogProxy, S3ReadProxy):
            with self.subTest(proxy=proxy_type.__name__), fixture(proxy_type) as (_, client, requests, accepted):
                first_socket = None
                for method in ("GET", "HEAD", "DELETE", "GET"):
                    client.request(method, "/object")
                    response = client.getresponse()
                    self.assertEqual(response.read(), b"abcde" if method == "GET" else b"")
                    self.assertFalse(response.will_close)
                    if method == "HEAD":
                        self.assertEqual(response.getheader("Content-Length"), "5")
                    if first_socket is None:
                        first_socket = client.sock
                    self.assertIs(client.sock, first_socket)
                self.assertEqual(len(requests), 4)
                self.assertEqual(len(accepted), 1)

    def test_s3_preserves_signed_request_chunks(self):
        with fixture(S3ReadProxy) as (_, client, requests, accepted):
            chunks = b"3;chunk-signature=abc\r\nxyz\r\n0;chunk-signature=def\r\nx-amz-checksum-crc32: test\r\n\r\n"
            client.putrequest("PUT", "/object", skip_host=True)
            client.putheader("Host", "signed.example")
            client.putheader("Transfer-Encoding", "chunked")
            client.endheaders(chunks)
            self.assertEqual(client.getresponse().read(), b"abcde")
            client.request("GET", "/object")
            self.assertEqual(client.getresponse().read(), b"abcde")
            self.assertEqual(requests[0][2]["Host"], "signed.example")
            self.assertEqual(requests[0][3], chunks)
            self.assertEqual(len(accepted), 1)

    def test_s3_chunked_response_closes_after_decoded_body(self):
        with fixture(S3ReadProxy) as (_, client, _, _):
            client.request("GET", "/chunks")
            response = client.getresponse()
            self.assertIsNone(response.getheader("Transfer-Encoding"))
            self.assertTrue(response.will_close)
            self.assertEqual(response.read(), b"abcde")

    def test_s3_truncated_response_closes_instead_of_reusing(self):
        with fixture(S3ReadProxy) as (_, client, _, _):
            client.request("GET", "/short")
            response = client.getresponse()
            with self.assertRaises(http.client.IncompleteRead):
                response.read()
            self.assertEqual(client.sock.recv(1), b"")

    def test_upstream_lost_response_does_not_replay_write(self):
        for proxy_type in (CatalogProxy, S3ReadProxy):
            with self.subTest(proxy=proxy_type.__name__), fixture(proxy_type) as (_, client, requests, accepted):
                client.request("POST", "/lost", b"{}")
                response = client.getresponse()
                self.assertEqual(response.status, 502)
                self.assertTrue(response.will_close)
                response.read()
                self.assertEqual(len(requests), 1)
                self.assertEqual(len(accepted), 1)

    def test_catalog_injected_lost_commit_still_disconnects(self):
        with fixture(CatalogProxy) as (proxy, client, requests, accepted):
            client.request("GET", "/object")
            client.getresponse().read()
            proxy.arm_drop("ingest")
            body = json.dumps({"updates": [{"action": "add-snapshot", "snapshot": {
                "summary": {"streaming.operation": "ingest"}}}]})
            client.request("POST", "/commit", body)
            with self.assertRaises(http.client.RemoteDisconnected):
                client.getresponse()
            self.assertTrue(proxy.dropped.is_set())
            self.assertEqual([request[1] for request in requests], ["/object", "/commit"])
            self.assertEqual(len(accepted), 1)

    def test_catalog_unknown_commit_status_follows_upstream_success(self):
        body = json.dumps({"updates": [{"action": "add-snapshot", "snapshot": {
            "snapshot-id": 7, "summary": {"streaming.operation": "ingest", "flow.operation-id": "op-1"}}}]})
        for status in (500, 502, 504):
            with self.subTest(status=status), fixture(CatalogProxy) as (proxy, client, requests, _):
                proxy.arm_drop(status=status)
                client.request("POST", "/v1/namespaces/test/tables/orders", body)
                response = client.getresponse()
                self.assertEqual(response.status, status)
                self.assertTrue(response.will_close)
                self.assertEqual(json.loads(response.read())["error"]["code"], status)
                self.assertTrue(proxy.dropped.is_set())
                self.assertEqual(len(requests), 1, "the rewritten commit must reach upstream exactly once")
                # The proxy records its event after responding; wait for it.
                deadline = time.monotonic() + 5
                while not [event for event in proxy.events if event.get("fault")] and time.monotonic() < deadline:
                    time.sleep(.01)
                event, = [event for event in proxy.events if event.get("fault")]
                self.assertEqual(event["fault"], "unknown-status-after-successful-commit")
                self.assertEqual((event["operation_id"], event["snapshot_id"], event["upstream_status"]),
                                 ("op-1", 7, 200))
                # Only the armed commit is affected.
                client.close()
                client.request("POST", "/v1/namespaces/test/tables/orders", body)
                self.assertEqual(client.getresponse().read(), b"abcde")
        with self.assertRaises(ValueError):
            CatalogProxy.arm_drop(None, status=503)

    def test_catalog_late_commit_response_outlives_client_timeout(self):
        with fixture(CatalogProxy) as (proxy, client, requests, _):
            proxy.arm_drop(delay_seconds=1.5)
            client.timeout = 0.3
            body = json.dumps({"updates": [{"action": "add-snapshot", "snapshot": {
                "summary": {"streaming.operation": "ingest", "flow.operation-id": "op-2"}}}]})
            client.request("POST", "/v1/namespaces/test/tables/orders", body)
            with self.assertRaises(TimeoutError):
                client.getresponse()
            self.assertTrue(proxy.dropped.is_set())
            self.assertEqual(len(requests), 1)
            client.close()
            deadline = time.monotonic() + 5
            while not any(event.get("fault") == "late-after-successful-commit" for event in proxy.events):
                self.assertLess(time.monotonic(), deadline)
                time.sleep(0.05)
            event, = [event for event in proxy.events if event.get("fault")]
            self.assertEqual((event["operation_id"], event["upstream_status"]), ("op-2", 200))

    def test_table_rejection_matches_exact_table_and_selected_methods(self):
        with fixture(CatalogProxy) as (proxy, client, requests, _):
            proxy.reject_table("orders", 403, methods=("POST",))
            for method, path, expected in (
                ("GET", "/v1/config", 200),
                ("GET", "/v1/namespaces/test/tables/orders", 200),
                ("POST", "/v1/namespaces/test/tables/accounts", 200),
                ("POST", "/v1/namespaces/test/tables/orders_backup", 200),
                ("POST", "/v1/namespaces/test/tables/orders?test=1", 403),
            ):
                client.request(method, path, b"{}")
                response = client.getresponse()
                self.assertEqual(response.status, expected)
                response.read()
            self.assertEqual(len(requests), 4, "a rejected write reached upstream")
            proxy.reject_table("orders", 503)
            client.request("GET", "/v1/namespaces/test/tables/orders")
            response = client.getresponse()
            self.assertEqual(response.status, 503)
            response.read()
            proxy.allow_table("orders")
            client.request("GET", "/v1/namespaces/test/tables/orders")
            self.assertEqual(client.getresponse().read(), b"abcde")
            self.assertEqual(len(requests), 5)

    def test_table_drop_waits_for_selected_commit_and_blocks_recovery_reads(self):
        with fixture(CatalogProxy) as (proxy, client, requests, _):
            proxy.arm_drop(table="orders", reject_after=403)
            body = json.dumps({"updates": [{"action": "add-snapshot", "snapshot": {
                "summary": {"streaming.operation": "ingest"}}}]})
            client.request("POST", "/v1/namespaces/test/tables/accounts", body)
            self.assertEqual(client.getresponse().read(), b"abcde")
            self.assertFalse(proxy.dropped.is_set())
            client.request("POST", "/v1/namespaces/test/tables/orders", body)
            with self.assertRaises(http.client.RemoteDisconnected):
                client.getresponse()
            self.assertTrue(proxy.dropped.is_set())
            client.close()
            client.request("GET", "/v1/namespaces/test/tables/orders")
            response = client.getresponse()
            self.assertEqual(response.status, 403)
            response.read()
            client.request("GET", "/v1/namespaces/test/tables/accounts")
            self.assertEqual(client.getresponse().read(), b"abcde")
            self.assertEqual([request[1].rsplit("/", 1)[1] for request in requests],
                             ["accounts", "orders", "accounts"])


if __name__ == "__main__":
    unittest.main()
