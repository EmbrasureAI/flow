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


if __name__ == "__main__":
    unittest.main()
