"""Bounded HTTP relay that can hold selected S3 reads without changing signatures."""

import http.client
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import threading
import time
from urllib.parse import unquote, urlsplit


class S3ReadProxy:
    def __init__(self, upstream, trace):
        self.upstream = urlsplit(upstream)
        if self.upstream.scheme not in ("http", "https") or self.upstream.path not in ("", "/"):
            raise ValueError("S3 upstream must be an HTTP(S) origin")
        self.trace = Path(trace)
        self.lock = threading.Lock()
        self.selected = set()
        self.held = threading.Event()
        self.release_reads = threading.Event()
        self.release_reads.set()
        self.events = []
        proxy = self

        class Server(ThreadingHTTPServer):
            # The fixture stops its daemon before close. Join request handlers
            # too, so canceled GETs finish recording before the report is saved.
            daemon_threads = False

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

            do_HEAD = do_GET
            do_PUT = do_GET
            do_POST = do_GET
            do_DELETE = do_GET

        self.server = Server(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, name="s3-read-proxy", daemon=True)
        self.thread.start()
        self.url = f"http://127.0.0.1:{self.server.server_port}"

    def hold_reads(self, paths):
        selected = set()
        for path in paths:
            uri = urlsplit(path)
            if uri.scheme != "s3":
                raise ValueError(f"expected S3 object: {path}")
            selected.add("/" + uri.netloc + uri.path)
        if not selected:
            raise ValueError("at least one exact object is required")
        with self.lock:
            self.selected = selected
            self.held.clear()
            self.release_reads.clear()

    def record(self, event):
        with self.lock:
            self.events.append(event)
            with self.trace.open("a") as output:
                output.write(json.dumps(event) + "\n")

    def close(self):
        self.release_reads.set()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)

    @staticmethod
    def request_body(handler):
        """Relay HTTP chunks verbatim, including AWS signed chunk extensions."""
        if handler.headers.get("Transfer-Encoding", "").lower() == "chunked":
            while True:
                line = handler.rfile.readline(65537)
                if not line or len(line) > 65536 or not line.endswith(b"\r\n"):
                    raise ValueError("invalid request chunk header")
                size = int(line.split(b";", 1)[0], 16)
                yield line
                if size == 0:
                    while True:
                        trailer = handler.rfile.readline(65537)
                        if not trailer or len(trailer) > 65536:
                            raise ValueError("invalid request trailer")
                        yield trailer
                        if trailer == b"\r\n":
                            return
                remaining = size
                while remaining:
                    data = handler.rfile.read(min(remaining, 65536))
                    if not data:
                        raise EOFError("truncated request chunk")
                    yield data
                    remaining -= len(data)
                ending = handler.rfile.read(2)
                if ending != b"\r\n":
                    raise ValueError("invalid request chunk ending")
                yield ending
        else:
            remaining = int(handler.headers.get("Content-Length", "0"))
            while remaining:
                data = handler.rfile.read(min(remaining, 65536))
                if not data:
                    raise EOFError("truncated request body")
                yield data
                remaining -= len(data)

    def forward(self, handler):
        started = time.monotonic()
        event = {"unix_ms": time.time_ns() // 1_000_000, "method": handler.command,
                 "path": handler.path, "range": handler.headers.get("Range")}
        connection = handler.upstream_connection
        response_started = False
        hold = False
        try:
            with self.lock:
                hold = (handler.command == "GET" and not self.release_reads.is_set()
                        and unquote(urlsplit(handler.path).path) in self.selected)
            if hold:
                self.record(event | {"state": "held"})
                self.held.set()
                if not self.release_reads.wait(timeout=60):
                    raise TimeoutError("test did not release selected S3 read within 60 seconds")
            connection.putrequest(handler.command, handler.path, skip_host=True, skip_accept_encoding=True)
            # Host is signed against this proxy's endpoint. MinIO must receive
            # that exact Host, plus the original path, headers and encoded body.
            for name, value in handler.headers.items():
                connection.putheader(name, value)
            connection.endheaders()
            for data in self.request_body(handler):
                connection.send(data)
            response = connection.getresponse()
            event["status"] = response.status
            bodyless = handler.command == "HEAD" or response.status in (204, 304) or 100 <= response.status < 200
            # http.client decodes response chunks. Keep close-delimited framing
            # for those responses and reuse connections only with a known length.
            expected_length = response.length
            if not bodyless and (response.chunked or expected_length is None):
                handler.close_connection = True
            handler.send_response_only(response.status, response.reason)
            for name, value in response.getheaders():
                if (name.lower() not in ("connection", "transfer-encoding")
                        and not (response.chunked and name.lower() == "content-length")):
                    handler.send_header(name, value)
            if handler.close_connection:
                handler.send_header("Connection", "close")
            handler.end_headers()
            response_started = True
            transferred = 0
            while data := response.read(65536):
                handler.wfile.write(data)
                transferred += len(data)
            if not bodyless and expected_length is not None and transferred != expected_length:
                raise EOFError("truncated upstream response body")
        except (OSError, EOFError, ValueError, http.client.HTTPException) as error:
            event["error"] = str(error)
            # Never replay a request after a transport error: writes may commit.
            handler.close_connection = True
            connection.close()
            if not response_started:
                try:
                    handler.send_error(502, "S3 test proxy transport failure")
                except OSError:
                    pass
        finally:
            event.update(state="complete", held=hold,
                         elapsed_ms=round((time.monotonic() - started) * 1000, 3))
            self.record(event)
