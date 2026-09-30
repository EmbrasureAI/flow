#!/usr/bin/env python3
"""Run source contracts on isolated PostgreSQL majors using an existing REST/S3 fixture."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import time
import uuid

from container_cleanup import cleanup_container


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--versions", nargs="+", choices=["14.24", "15.19", "16.15", "17.11", "18.6"],
                        default=["14.24", "15.19", "16.15", "17.11"])
    parser.add_argument("--catalog-uri", required=True)
    parser.add_argument("--s3-endpoint", required=True)
    parser.add_argument("--binary", type=Path, default=Path("target/release/embrasure-flow"))
    parser.add_argument("--artifacts", type=Path, required=True)
    args = parser.parse_args()
    args.artifacts.mkdir(parents=True, exist_ok=False)
    with args.binary.open("rb") as binary:
        binary_sha256 = hashlib.file_digest(binary, "sha256").hexdigest()
    report = {"passed": False, "versions": [], "binary": str(args.binary.resolve()),
              "binary_sha256": binary_sha256}
    try:
        for version in args.versions:
            name = "flow-source-" + uuid.uuid4().hex[:12]
            record = {"postgres": version, "passed": False, "suites": []}
            report["versions"].append(record)
            try:
                subprocess.run(["docker", "run", "--detach", "--name", name, "--cpus=2", "--memory=1g",
                                "--publish", "127.0.0.1:0:5432", "--env", "POSTGRES_USER=flow",
                                "--env", "POSTGRES_PASSWORD=local-test-password", "--env", "POSTGRES_DB=flow",
                                f"postgres:{version}-bookworm", "postgres", "-c", "wal_level=logical",
                                "-c", "max_replication_slots=8", "-c", "max_wal_senders=8",
                                "-c", "logical_decoding_work_mem=4MB", "-c", "track_commit_timestamp=on"], check=True, timeout=120)
                inspection = json.loads(subprocess.check_output(["docker", "inspect", name], timeout=30))[0]
                record["image"] = inspection["Image"]
                port = inspection["NetworkSettings"]["Ports"]["5432/tcp"][0]["HostPort"]
                record["host_port"] = int(port)
                deadline = time.monotonic() + 60
                while subprocess.run(["docker", "exec", name, "pg_isready", "-U", "flow", "-d", "flow"],
                                     stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=10).returncode:
                    if time.monotonic() >= deadline:
                        raise TimeoutError(f"PostgreSQL {version} did not become ready")
                    time.sleep(0.25)
                environment = os.environ | {
                    "FLOW_POSTGRES_URL": f"postgres://flow:local-test-password@127.0.0.1:{port}/flow?sslmode=disable",
                    "RUST_LOG": "info",
                }
                for suite in ("contracts", "bootstrap"):
                    output = args.artifacts / f"pg-{version}-{suite}"
                    command = ["uv", "run", f"tests/production/{suite}.py", "--catalog-uri", args.catalog_uri,
                               "--s3-endpoint", args.s3_endpoint, "--binary", str(args.binary.resolve()),
                               "--artifacts", str(output.resolve())]
                    if suite == "bootstrap":
                        command.append("--add-column-during-recovery")
                    started = time.monotonic()
                    subprocess.run(command, env=environment, check=True)
                    record["suites"].append({"suite": suite, "seconds": round(time.monotonic() - started, 3),
                                            "report": str(output.resolve() / "report.json")})
                record["passed"] = True
            except Exception as error:
                record["error"] = f"{type(error).__name__}: {error}"
            finally:
                # Docker may create the named container before its client times out.
                record["cleanup"] = cleanup_container(name, args.artifacts / f"pg-{version}.log")
                if not record["cleanup"]["passed"]:
                    record["passed"] = False
                (args.artifacts / "report.json").write_text(json.dumps(report, indent=2) + "\n")
        report["passed"] = all(record["passed"] for record in report["versions"])
    finally:
        (args.artifacts / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    if not report["passed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
