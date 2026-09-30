"""Finite process ownership and durable failure reports for benchmark CLIs."""

import json
import math
import os
from pathlib import Path
import signal
import subprocess
import time


WORKLOAD_DEADLINE = ".workload-deadline.json"


def write_report(path, report):
    """Replace atomically so the supervisor can recover the last complete report."""
    path = Path(path)
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w") as output:
        json.dump(report, output, indent=2, default=str)
        output.write("\n")
        output.flush()
        os.fsync(output.fileno())
    temporary.replace(path)
    directory_fd = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


def arm_workload(directory, deadline):
    write_report(Path(directory) / WORKLOAD_DEADLINE, {"deadline": deadline})


def disarm_workload(directory):
    (Path(directory) / WORKLOAD_DEADLINE).unlink(missing_ok=True)


def run_supervised(command, directory, timeout, **popen_options):
    """Exec a fresh process group; no worker thread or descendant survives return.

    The workload sidecar bounds blocked native calls and cleanup independently of
    the larger initialization/verification budget. SQLite already committed to
    disk survives forced termination; in-flight/queued evidence may be incomplete
    and can never qualify. The supervisor has no database or recorder threads.
    """
    if not math.isfinite(timeout) or timeout <= 0:
        raise ValueError("run timeout must be positive and finite")
    directory = Path(directory)
    # Refuse to amend evidence from a previous run, even if its child would reject it.
    if directory.exists():
        raise FileExistsError(directory)
    deadline = time.monotonic() + timeout
    process = None
    failure = None
    terminated = False
    def request_termination(signum, frame):
        nonlocal terminated
        terminated = True
    previous_handler = signal.signal(signal.SIGTERM, request_termination)
    try:
        process = subprocess.Popen(command, start_new_session=True, **popen_options)
        while process.poll() is None:
            if terminated:
                failure = "supervisor received SIGTERM; owned process group terminated"
                break
            now = time.monotonic()
            workload_path = directory / WORKLOAD_DEADLINE
            if workload_path.exists():
                try:
                    workload_deadline = json.loads(workload_path.read_text())["deadline"]
                except FileNotFoundError:  # normal disarm raced this poll
                    workload_deadline = math.inf
                if now >= workload_deadline:
                    failure = "workload hard deadline exceeded; owned process group terminated"
                    break
            if now >= deadline:
                failure = f"run hard deadline exceeded ({timeout:g}s); owned process group terminated"
                break
            time.sleep(.05)
    except BaseException as error:
        failure = f"supervisor interrupted: {error}"
        raise
    finally:
        try:
            if terminated:
                failure = "supervisor received SIGTERM; owned process group terminated"
            if process is not None:
                # Kill the whole group even if its leader already exited: it may have
                # abandoned a daemon/compactor. SIGKILL also handles stuck native threads.
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                process.wait(timeout=10)
                if failure or process.returncode != 0:
                    directory.mkdir(parents=True, exist_ok=True)
                    path = directory / "report.json"
                    try:
                        report = json.loads(path.read_text())
                    except (FileNotFoundError, json.JSONDecodeError):
                        report = {}
                    if failure:
                        if report.get("failure"):
                            report["worker_failure"] = report["failure"]
                        report.update(failure=failure, correctness_passed=False,
                                      performance_qualified=False, source_correctness_passed=False,
                                      source_capacity_met=False, evidence_incomplete=True)
                    else:
                        report.setdefault("failure", f"benchmark worker exited with code {process.returncode}")
                    report.update(passed=False, worker_returncode=process.returncode)
                    write_report(path, report)
        finally:
            signal.signal(signal.SIGTERM, previous_handler)
    return subprocess.CompletedProcess(command, 143 if terminated else (124 if failure else process.returncode))
