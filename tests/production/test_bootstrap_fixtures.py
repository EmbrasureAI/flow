# /// script
# requires-python = ">=3.11"
# dependencies = ["duckdb==1.5.5", "psycopg[binary]==3.3.5", "boto3==1.43.88", "fastavro==1.12.2"]
# ///
"""Service-free writer cancellation and selective COPY hold controls."""

from pathlib import Path
import socket
import socketserver
import struct
import tempfile
import threading
import unittest
from unittest.mock import Mock

from bootstrap import BootstrapRun
from health import HealthProxy, receive
from psycopg.conninfo import conninfo_to_dict


class BootstrapFixtureTests(unittest.TestCase):
    def test_shutdown_cancels_and_joins_before_returning(self):
        with tempfile.TemporaryDirectory() as directory:
            run = BootstrapRun.__new__(BootstrapRun)
            run.directory = Path(directory)
            run.writer_stop = threading.Event()
            run.writer_lock = threading.Lock()
            run.writer_error = None
            run.report = {}
            canceled = threading.Event()
            connection = Mock(closed=False)
            connection.cancel_safe.side_effect = lambda timeout: canceled.set()
            run.writer_connection = connection

            def blocked_writer():
                canceled.wait(3)
                connection.closed = True

            run.writer_thread = threading.Thread(target=blocked_writer)
            run.writer_thread.start()
            run.stop_writer()
            connection.cancel_safe.assert_called_once_with(timeout=3)
            self.assertTrue(canceled.is_set())
            self.assertFalse(run.writer_thread.is_alive())
            self.assertTrue(run.report["writer_shutdown"]["connection_closed"])

    def test_copy_hold_leaves_other_table_and_cdc_queries_unblocked(self):
        received = []

        class Server(socketserver.ThreadingTCPServer):
            daemon_threads = True

        class Handler(socketserver.BaseRequestHandler):
            def handle(self):
                try:
                    header = receive(self.request, 4)
                    receive(self.request, struct.unpack("!I", header)[0] - 4)
                    while True:
                        kind = receive(self.request, 1)
                        header = receive(self.request, 4)
                        body = receive(self.request, struct.unpack("!I", header)[0] - 4)
                        received.append(body)
                        self.request.sendall(b"ack")
                except (EOFError, OSError):
                    pass

        with tempfile.TemporaryDirectory() as directory, Server(("127.0.0.1", 0), Handler) as upstream:
            server_thread = threading.Thread(target=upstream.serve_forever, daemon=True)
            server_thread.start()
            proxy = HealthProxy(f"host=127.0.0.1 port={upstream.server_address[1]}",
                                Path(directory) / "proxy.json", (b"COPY ", b'"orders"'))
            clients = []
            try:
                port = int(conninfo_to_dict(proxy.connection)["port"])

                def send(query):
                    client = socket.create_connection(("127.0.0.1", port), timeout=2)
                    clients.append(client)
                    client.sendall(struct.pack("!II", 8, 196608))
                    body = query + b"\0"
                    client.sendall(b"Q" + struct.pack("!I", len(body) + 4) + body)
                    return client

                held = send(b'COPY "orders" TO STDOUT')
                self.assertTrue(proxy.query_held.wait(timeout=2))
                self.assertEqual(proxy.active, 1)
                self.assertEqual(receive(send(b'COPY "accounts" TO STDOUT'), 3), b"ack")
                self.assertEqual(receive(send(b"START_REPLICATION SLOT fixture LOGICAL 0/0"), 3), b"ack")
                self.assertFalse(any(b'"orders"' in body for body in received))
                self.assertEqual(proxy.active, 1)
                held.close()
            finally:
                for client in clients:
                    client.close()
                proxy.close()
                upstream.shutdown()
                server_thread.join(timeout=3)


if __name__ == "__main__":
    unittest.main()
