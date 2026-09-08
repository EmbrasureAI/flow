#!/usr/bin/env python3
"""Run the controlled maintenance-policy matrix at 1k and 10k mutations/s."""

import argparse
import json
import math
import os
from pathlib import Path
import subprocess
import sys

from provenance import sha256_file
from process_lifecycle import run_supervised


MODES = (
    ("a-disabled", "disabled"),
    ("b-delete-only", "delete-only"),
    ("c-l0-only", "l0-only"),
    ("d-current", "current"),
)


def phase(report, name):
    return next((item.get("details") for item in report.get("phases", []) if item.get("name") == name), None)


def summary(report):
    workload = phase(report, "open-loop-workload") or {}
    inventory = phase(report, "post-workload-inventory") or {}
    latency = workload.get("latency", {}).get("commit_to_catalog", {})
    return {
        "correctness_passed": report.get("correctness_passed"),
        "performance_qualified": report.get("performance_qualified"),
        "reader_scan_qualified": report.get("reader_scan_qualified"),
        "measured_arrivals": workload.get("arrival_cohorts", {}).get("measured"),
        "source_mutations_per_second": workload.get("source_commit_window_mutations_per_second"),
        "catalog_mutations_per_second_including_drain": workload.get(
            "measured_cohort_catalog_mutations_per_second_including_drain"
        ),
        "commit_to_catalog_p95_ms": latency.get("p95_ms"),
        "commit_to_catalog_p99_ms": latency.get("p99_ms"),
        "inventory": inventory,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--postgres-url", default=os.environ.get("FLOW_POSTGRES_URL"))
    parser.add_argument("--catalog-uri", required=True)
    parser.add_argument("--s3-endpoint", required=True)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--build-manifest", type=Path, required=True)
    parser.add_argument("--reader-baseline-compactor", type=Path, required=True)
    parser.add_argument("--artifacts", type=Path, required=True)
    parser.add_argument("--duration", type=float, default=60)
    parser.add_argument("--setup-timeout", type=float, default=600)
    parser.add_argument("--warmup", type=float, default=10)
    parser.add_argument("--baseline-max-input-files", type=int, default=16384)
    parser.add_argument("--baseline-max-input-bytes", type=int, default=512 << 20)
    parser.add_argument("--baseline-timeout", type=float, default=900)
    args = parser.parse_args()
    worst_case_transactions = math.floor((args.duration + args.warmup) * 10000 / 100)
    conservative_file_bound = 2 * worst_case_transactions + 16
    if (not args.postgres_url or args.duration <= 0 or args.warmup < 0
            or args.baseline_max_input_bytes <= 0 or args.baseline_timeout <= 0
            or args.baseline_max_input_files < conservative_file_bound):
        parser.error(
            "provide PostgreSQL URL, positive duration/baseline budgets, nonnegative warmup, "
            f"and at least {conservative_file_bound} baseline input files for this workload"
        )

    if not all(math.isfinite(value) for value in (args.setup_timeout, args.duration, args.warmup)) or args.setup_timeout <= 0:
        parser.error("setup timeout and workload durations must be finite, with positive setup timeout")
    args.artifacts.mkdir(parents=True, exist_ok=False)
    results = []
    benchmark = Path(__file__).with_name("benchmark.py")
    matrix_identity = {
        "path": str(Path(__file__).resolve()),
        "sha256": sha256_file(Path(__file__).resolve()),
        "benchmark_sha256": sha256_file(benchmark),
    }
    for rate in (1000, 10000):
        for label, mode in MODES:
            name = f"{rate}-{label}"
            output = args.artifacts / name
            command = [
                "uv", "run", str(benchmark),
                "--postgres-url", args.postgres_url,
                "--catalog-uri", args.catalog_uri,
                "--s3-endpoint", args.s3_endpoint,
                "--binary", str(args.binary),
                "--build-manifest", str(args.build_manifest),
                "--reader-baseline-compactor", str(args.reader_baseline_compactor),
                "--reader-baseline-max-input-files", str(args.baseline_max_input_files),
                "--reader-baseline-max-input-bytes", str(args.baseline_max_input_bytes),
                "--reader-baseline-timeout", str(args.baseline_timeout),
                "--artifacts", str(output),
                "--duration", str(args.duration),
                "--warmup", str(args.warmup),
                "--setup-timeout", str(args.setup_timeout), "--supervised-worker",
                "--rate", str(rate),
                "--maintenance-mode", mode,
                # Hold manifest rewrites constant to isolate data/delete maintenance.
                "--manifest-max-count", "1000000",
            ]
            print(f"[{name}]", flush=True)
            with (args.artifacts / f"{name}.log").open("w") as log:
                completed = run_supervised(command, output,
                                           args.setup_timeout + args.warmup + args.duration + 360
                                           + 2 * args.baseline_timeout,
                                           stdout=log, stderr=subprocess.STDOUT)
            report_path = output / "report.json"
            report = json.loads(report_path.read_text()) if report_path.exists() else {}
            result = {
                "case": name,
                "rate": rate,
                "maintenance_mode": mode,
                "exit_code": completed.returncode,
                "command": command,
                "report": str(report_path.resolve()),
                "failure": report.get("failure"),
                "matrix_harness": matrix_identity,
                **summary(report),
            }
            results.append(result)
            (args.artifacts / "matrix.json").write_text(json.dumps(results, indent=2) + "\n")
            print(f"[{name}] {'PASS' if completed.returncode == 0 else 'FAIL'}", flush=True)
            if report.get("correctness_passed") is not True:
                print("Stopped: resolve incomplete correctness before another workload.", flush=True)
                return 1
    return 0 if all(item["exit_code"] == 0 for item in results) else 1


if __name__ == "__main__":
    sys.exit(main())
