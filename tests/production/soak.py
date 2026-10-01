#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["duckdb==1.5.5", "psycopg[binary]==3.3.5", "boto3==1.43.88", "fastavro==1.12.2"]
# ///
"""Steady-state soak: run the crash-loop workload without faults and fail on
sustained growth of daemon memory, local state, journal or table metadata.

After a warm-up, the samples are split into three equal windows. A resource
fails when its window medians strictly increase and the last window exceeds
the first by more than the tolerance. Retention and garbage collection are
shortened so that metadata and owned-artifact bookkeeping reach steady state
within the run; this checks for leaks, not for the default retention cost.

Each sample also keeps the daemon's allocator and memory-budget gauges, so a
growth failure shows whether live allocations, allocator retention or bounded
caches account for the RSS. A growth failure is raised only after the final
row differential, so both are checked on every run.
"""

import os
from pathlib import Path
import shlex
from statistics import median
import subprocess
import sys
import time
import traceback
from urllib.parse import urlparse

sys.path.insert(0, str(Path(__file__).resolve().parent))
from crash_loop import CrashLoop, TABLES, Workload, parser
from run import dump

RESOURCES = ("rss_bytes", "state_bytes", "journal_bytes", "metadata_bytes")
# Diagnostic gauges kept with each sample; absent without the jemalloc feature.
MEMORY_GAUGES = ("flow_allocator_", "flow_memory_")


def directory_bytes(path):
    total = 0
    for item in path.rglob("*"):
        try:
            if item.is_file():
                total += item.stat().st_size
        except FileNotFoundError:
            pass  # replaced by the daemon during the walk
    return total


def sustained_growth(samples, warmup, tolerance):
    """Return {resource: evidence} for resources growing across the whole run."""
    steady = samples[warmup:]
    third = len(steady) // 3
    if third < 2:
        raise AssertionError(f"only {len(steady)} post-warm-up samples; lengthen the soak")
    growing = {}
    for resource in RESOURCES:
        windows = [median(sample[resource] for sample in steady[index * third:(index + 1) * third])
                   for index in range(3)]
        limit = windows[0] * (1 + tolerance[resource])
        if windows[0] < windows[1] < windows[2] and windows[2] > limit:
            growing[resource] = {"window_medians": windows, "limit": limit}
    return growing


def reproduction(args):
    arguments = ["uv", "run", "tests/production/soak.py", "--seed", str(args.seed),
                 "--duration", f"{args.duration:g}", "--writers", str(args.writers),
                 "--large-rows", str(args.large_rows), "--large-interval", f"{args.large_interval:g}",
                 "--format-version", str(args.format_version), "--sample-seconds", f"{args.sample_seconds:g}",
                 "--verify-every", f"{args.verify_every:g}", "--retention-secs", str(args.retention_secs),
                 "--warmup-fraction", f"{args.warmup_fraction:g}", "--rss-tolerance", f"{args.rss_tolerance:g}",
                 "--state-tolerance", f"{args.state_tolerance:g}",
                 "--metadata-tolerance", f"{args.metadata_tolerance:g}"]
    return shlex.join(arguments) + " --catalog-uri ... --s3-endpoint ... --artifacts NEW_DIRECTORY"


class Soak(CrashLoop):
    def __init__(self, args):
        super().__init__(args)
        # The inherited command would replay a crash loop, not this soak.
        self.report["reproduce"] = reproduction(args)

    def configure(self):
        super().configure()
        text = self.config.read_text().replace(
            "snapshot_retention_secs = 3600",
            f"snapshot_retention_secs = {self.args.retention_secs}\n"
            f"garbage_interval_secs = 30\norphan_grace_secs = {self.args.retention_secs}")
        self.config.write_text(text)

    def sample(self):
        rss = subprocess.run(["ps", "-o", "rss=", "-p", str(self.process.pid)],
                             capture_output=True, text=True, check=True).stdout.strip()
        metadata = 0
        snapshots = {}
        for name in TABLES:
            table = self.table(name)
            location = urlparse(table["metadata-location"])
            metadata += self.s3.head_object(Bucket=location.netloc, Key=location.path.lstrip("/"))["ContentLength"]
            snapshots[name] = len(table["metadata"]["snapshots"])
        state = self.directory / "state"
        metrics = self.metrics()
        return {"elapsed_seconds": round(time.monotonic() - self.started, 1), "rss_bytes": int(rss) * 1024,
                "state_bytes": directory_bytes(state), "journal_bytes": directory_bytes(state / "journal"),
                "metadata_bytes": metadata, "snapshots": snapshots,
                "source_lag_bytes": metrics.get("flow_source_received_lsn", 0) - metrics.get("flow_materialized_lsn", 0),
                "memory": {name: value for name, value in metrics.items() if name.startswith(MEMORY_GAUGES)}}

    def soak(self):
        self.workload = Workload(self)
        self.started = time.monotonic()
        samples = self.report.setdefault("samples", [])
        checks = self.report.setdefault("row_checks", [])
        next_check = self.started + self.args.verify_every
        while time.monotonic() - self.started < self.args.duration:
            deadline = time.monotonic() + self.args.sample_seconds
            while time.monotonic() < deadline:
                self.alive()
                self.workload.check()
                time.sleep(0.5)
            samples.append(self.sample())
            print(f"[sample {len(samples)}] {samples[-1]}", flush=True)
            if time.monotonic() >= next_check:
                self.workload.pause()
                try:
                    self.wait_materialized(0)
                    checks.append(self.compare(f"soak-{len(checks) + 1}"))
                    self.published()
                finally:
                    self.workload.resume()
                next_check = time.monotonic() + self.args.verify_every
            dump(self.directory / "report.json", self.report)
        self.workload.stop()
        warmup = int(len(samples) * self.args.warmup_fraction)
        tolerance = {"rss_bytes": self.args.rss_tolerance, "state_bytes": self.args.state_tolerance,
                     "journal_bytes": self.args.state_tolerance, "metadata_bytes": self.args.metadata_tolerance}
        # Recorded here and raised after the final differential.
        self.report["growth"] = sustained_growth(samples, warmup, tolerance)
        # The nightly artifact keeps report.json but not the state directory.
        self.report["final_metrics"] = self.metrics()
        return {"samples": len(samples), "warmup_samples": warmup, "row_checks": len(checks),
                "growing": sorted(self.report["growth"]), "workload": dict(self.workload.counts)}

    def final_check(self):
        self.wait_materialized(0)
        result = self.compare("final")
        self.published()
        return result

    def execute(self):
        print(f"SEED={self.args.seed}", flush=True)
        try:
            self.phase("seed", self.seed)
            self.phase("initial-copy", self.initialize)
            self.start()
            self.phase("snapshot-wal-handoff", self.handoff)
            self.phase("steady-state-soak", self.soak)
            self.phase("final-differential", self.final_check)
            self.stop()
            self.check_worker_panics()
            growing = self.report["growth"]
            assert not growing, f"sustained resource growth: {growing}"
            self.report["passed"] = True
        except BaseException as error:
            self.report["failure"] = str(error)
            self.report["traceback"] = traceback.format_exc()
            # A correctness failure must not hide recorded growth, or the reverse.
            if self.report.get("growth") and "sustained resource growth" not in str(error):
                print(f"also: sustained resource growth: {self.report['growth']}", file=sys.stderr, flush=True)
            raise
        finally:
            if self.workload is not None and not self.workload.stopping:
                try:
                    self.workload.stop()
                except Exception as error:
                    self.report["writer_shutdown_error"] = str(error)
            self.stop()
            if self.proxy is not None:
                self.proxy.close()
            dump(self.directory / "report.json", self.report)
            self.pg.close()
            self.duck.close()


def main():
    arguments = parser()
    arguments.description = __doc__
    arguments.set_defaults(duration=1800)
    arguments.add_argument("--sample-seconds", type=float, default=30)
    arguments.add_argument("--verify-every", type=float, default=600, help="seconds between paused row checks")
    arguments.add_argument("--warmup-fraction", type=float, default=0.3)
    arguments.add_argument("--retention-secs", type=int, default=120,
                           help="snapshot retention and orphan grace, short enough to reach steady state")
    arguments.add_argument("--rss-tolerance", type=float, default=0.25)
    arguments.add_argument("--state-tolerance", type=float, default=0.5)
    arguments.add_argument("--metadata-tolerance", type=float, default=0.25)
    args = arguments.parse_args()
    if not args.postgres_url:
        arguments.error("provide --postgres-url or FLOW_POSTGRES_URL")
    if args.seed is None:
        args.seed = int.from_bytes(os.urandom(4), "big")
    Soak(args).execute()
    print(f"PASS (seed {args.seed}): {args.artifacts.resolve() / 'report.json'}", flush=True)


if __name__ == "__main__":
    main()
