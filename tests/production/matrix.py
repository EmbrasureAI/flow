#!/usr/bin/env python3
"""Run bounded production workloads serially and preserve failed qualifications."""

import argparse
import json
import math
import os
from pathlib import Path
import subprocess
import sys

from process_lifecycle import run_supervised


CASES = {
    "mixed-1k": ["--rate", "1000"],
    "mixed-10k": ["--rate", "10000"],
    "mixed-50k": ["--rate", "50000"],
    "append-10k": ["--rate", "10000", "--profile", "append"],
    "ten-tables-10k": ["--rate", "10000", "--tables", "10"],
    "hot-updates-10k": ["--rate", "10000", "--profile", "update90", "--key-distribution", "hot"],
    "wide-1k": ["--rate", "1000", "--row-bytes", "16384", "--payload-entropy", "high", "--initial-rows", "10000"],
    "single-row-1k": ["--rate", "1000", "--transaction-rows", "1", "--profile", "append"],
    "external-compactor-1k": ["--rate", "1000", "--compactor-interval-ms", "10000"],
    "one-kib-10k": ["--rate", "10000", "--row-bytes", "1024"],
    "hundred-tables-10k": ["--rate", "10000", "--tables", "100", "--initial-rows", "10000"],
    "large-tx-10k": ["--rate", "10000", "--transaction-rows", "10000"],
    "zipf-updates-10k": ["--rate", "10000", "--profile", "update90", "--key-distribution", "zipf"],
}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--postgres-url", default=os.environ.get("FLOW_POSTGRES_URL"))
    parser.add_argument("--catalog-uri", required=True)
    parser.add_argument("--s3-endpoint", required=True)
    parser.add_argument("--binary", type=Path, default=Path("target/release/embrasure-flow"))
    parser.add_argument("--build-manifest", type=Path, required=True)
    parser.add_argument("--compactor-binary", type=Path)
    parser.add_argument("--reader-baseline-compactor", type=Path)
    parser.add_argument("--artifacts", type=Path, required=True)
    parser.add_argument("--duration", type=float, default=60)
    parser.add_argument("--setup-timeout", type=float, default=600)
    parser.add_argument("--warmup", type=float, default=10)
    parser.add_argument("--case", choices=CASES, action="append")
    args = parser.parse_args()
    selected = args.case or list(CASES)
    if not args.postgres_url or args.duration <= 0 or args.warmup < 0:
        parser.error("provide PostgreSQL URL, positive duration and nonnegative warmup")
    if "external-compactor-1k" in selected and args.compactor_binary is None:
        parser.error("--compactor-binary is required for external-compactor-1k")
    if not all(math.isfinite(value) for value in (args.setup_timeout, args.duration, args.warmup)) or args.setup_timeout <= 0:
        parser.error("setup timeout and workload durations must be finite, with positive setup timeout")
    args.artifacts.mkdir(parents=True, exist_ok=False)
    results = []
    for name in selected:
        directory = args.artifacts / name
        command = ["uv", "run", str(Path(__file__).with_name("benchmark.py")),
                   "--postgres-url", args.postgres_url, "--catalog-uri", args.catalog_uri,
                   "--s3-endpoint", args.s3_endpoint, "--binary", str(args.binary),
                   "--build-manifest", str(args.build_manifest),
                   "--artifacts", str(directory), "--duration", str(args.duration),
                   "--warmup", str(args.warmup), "--setup-timeout", str(args.setup_timeout),
                   "--supervised-worker", *CASES[name]]
        if name == "external-compactor-1k":
            command += ["--compactor-binary", str(args.compactor_binary)]
        if args.reader_baseline_compactor:
            command += ["--reader-baseline-compactor", str(args.reader_baseline_compactor)]
        print(f"[{name}]", flush=True)
        with (args.artifacts / f"{name}.log").open("w") as output:
            completed = run_supervised(command, directory,
                                       args.setup_timeout + args.warmup + args.duration + 360
                                       + (360 if args.reader_baseline_compactor else 0),
                                       stdout=output, stderr=subprocess.STDOUT)
        path = directory / "report.json"
        report = json.loads(path.read_text()) if path.exists() else {}
        results.append({"case": name, "exit_code": completed.returncode,
                        "passed": completed.returncode == 0 and report.get("passed", False),
                        "command": command,
                        "report": str(path.resolve()), "failure": report.get("failure")})
        (args.artifacts / "matrix.json").write_text(json.dumps(results, indent=2) + "\n")
        print(f"[{name}] {'PASS' if results[-1]['passed'] else 'FAIL'}", flush=True)
        if not report.get("correctness_passed", False):
            print("Stopped: resolve incomplete correctness or recovery before another workload.", flush=True)
            break
    return 0 if all(result["passed"] for result in results) else 1


if __name__ == "__main__":
    sys.exit(main())
