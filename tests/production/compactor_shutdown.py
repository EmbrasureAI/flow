#!/usr/bin/env python3
"""Check SIGINT retention while the fixture compactor awaits a real catalog read."""

import argparse
import hashlib
import json
from pathlib import Path
import signal
import subprocess
import threading
import time
import tomllib

from proxy import CatalogProxy


class HeldRead(CatalogProxy):
    def __init__(self, upstream, trace):
        self.reads = 0
        self.held = threading.Event()
        self.release = threading.Event()
        super().__init__(upstream, trace)

    def forward(self, handler):
        if handler.command == "GET" and "/tables/" in handler.path:
            self.reads += 1
            # A complete first pass and an interval wait ensure that even the
            # old process has installed its global SIGINT handler.
            if self.reads == 2:
                self.held.set()
                if not self.release.wait(timeout=15):
                    self.respond(handler, 503, b'{}')
                    return
        super().forward(handler)


def check(binary, config_text, upstream, directory, expect_exit):
    directory.mkdir()
    proxy = HeldRead(upstream, directory / "proxy.jsonl")
    original_uri = tomllib.loads(config_text)["catalog"]["uri"]
    config = directory / "flow.toml"
    config.write_text(config_text.replace(f"uri = {json.dumps(original_uri)}",
                                          f"uri = {json.dumps(proxy.url)}", 1))
    assert tomllib.loads(config.read_text())["catalog"]["uri"] == proxy.url
    evidence = {"binary": str(binary.resolve()), "sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
                "expected_exit": expect_exit}
    process = None
    try:
        with (directory / "compactor.log").open("w") as log:
            process = subprocess.Popen([str(binary.resolve()), "--config", str(config), "--interval-ms", "500"],
                                       stdout=log, stderr=subprocess.STDOUT)
            assert proxy.held.wait(timeout=10), "compactor did not reach its second catalog read"
            assert process.poll() is None
            process.send_signal(signal.SIGINT)
            time.sleep(.15)
            assert process.poll() is None, "SIGINT interrupted the in-flight pass"
            released = time.monotonic()
            proxy.release.set()
            try:
                evidence["returncode"] = process.wait(timeout=2)
                evidence["exit_after_release_seconds"] = time.monotonic() - released
                assert expect_exit and process.returncode == 0, "unexpected compactor exit"
            except subprocess.TimeoutExpired:
                evidence["signal_lost"] = True
                assert not expect_exit, "compactor lost SIGINT during its pass"
                assert proxy.reads >= 3, "negative control did not continue another pass"
            assert all(event["method"] == "GET" for event in proxy.events), "fixture requires an already compacted table"
            assert all(event.get("upstream_status") == 200 for event in proxy.events)
            evidence["passed"] = True
    finally:
        proxy.release.set()
        if process is not None and process.poll() is None:
            process.kill()
            process.wait(timeout=5)
            evidence["cleanup_returncode"] = process.returncode
        proxy.close()
        evidence["catalog_table_reads"] = proxy.reads
        (directory / "result.json").write_text(json.dumps(evidence, indent=2) + "\n")
    return evidence


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True, help="retained benchmark configuration with a compacted first table")
    parser.add_argument("--catalog-uri", required=True)
    parser.add_argument("--previous-binary", type=Path, required=True)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--artifacts", type=Path, required=True)
    args = parser.parse_args()
    args.artifacts.mkdir(parents=True, exist_ok=False)
    # The fixture only reads one retained table; source and local state are unused.
    parts = args.config.read_text().split("[[tables]]")
    assert len(parts) >= 2
    config_text = parts[0] + "[[tables]]" + parts[1]
    results = [check(binary, config_text, args.catalog_uri, args.artifacts / label, expect_exit)
               for label, binary, expect_exit in [("negative", args.previous_binary, False), ("fixed", args.binary, True)]]
    (args.artifacts / "report.json").write_text(json.dumps({"passed": True, "results": results}, indent=2) + "\n")
    print(f"PASS: {args.artifacts / 'report.json'}")


if __name__ == "__main__":
    main()
