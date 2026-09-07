#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["duckdb==1.5.5"]
# ///
"""Compare retained Iceberg layouts without starting ingestion or changing reports."""

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import statistics
import sys
import time
from urllib.parse import urlparse

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "local"))
from duckdb_extensions import load_extensions


SCAN_SQL = "SELECT count(*),sum(version),sum(length(payload)) FROM iceberg_scan(?)"
MAX_SCAN_RATIO = 1.25
WARMUP_PAIRS = 2
MEASURED_PAIRS = 20
READER_SETTINGS_SQL = """
    SELECT name, value FROM duckdb_settings() WHERE name IN (
        'httpfs_connection_caching', 'httpfs_client_implementation', 'http_keep_alive',
        'http_retries', 'http_retry_backoff', 'http_retry_wait_ms', 'http_timeout',
        'enable_http_metadata_cache', 'enable_external_file_cache'
    ) ORDER BY name
"""


def compare_snapshots(duck, original, compacted):
    """Measure pinned locations after callers stop their owned writer processes."""
    if not original or original.keys() != compacted.keys():
        raise ValueError("comparison requires matching nonempty table sets")
    result = {
        "original_scans": {}, "scans": {}, "scan_slowdown_vs_compacted": {},
        "verified_aggregates": {}, "max_scan_ratio": MAX_SCAN_RATIO,
        "reader_settings": dict(duck.execute(READER_SETTINGS_SQL).fetchall()),
        "protocol": {
            "warmup_scans_per_layout": WARMUP_PAIRS,
            "measured_scans_per_layout": MEASURED_PAIRS,
            "pair_order": "original/compacted, then compacted/original, repeated",
            "query": SCAN_SQL,
        },
        "method": "Explicit warmup followed by twenty alternating pairs against pinned metadata; ratio of median planning-plus-scan times, excluding REST GET",
    }
    for name in original:
        layouts = {
            "original": {"metadata_location": original[name], "warmup_ms": [], "scan_ms": []},
            "compacted": {"metadata_location": compacted[name], "warmup_ms": [], "scan_ms": []},
        }
        expected = None
        for phase, count in (("warmup_ms", WARMUP_PAIRS), ("scan_ms", MEASURED_PAIRS)):
            for pair in range(count):
                order = ("original", "compacted") if pair % 2 == 0 else ("compacted", "original")
                for label in order:
                    layout = layouts[label]
                    started = time.perf_counter_ns()
                    actual = duck.execute(SCAN_SQL, [layout["metadata_location"]]).fetchone()
                    layout[phase].append((time.perf_counter_ns() - started) / 1e6)
                    if expected is None:
                        expected = actual
                    elif actual != expected:
                        raise AssertionError(f"{name}: pinned layout scan aggregates differ")
        for layout in layouts.values():
            layout["median_scan_ms"] = statistics.median(layout["scan_ms"])
        result["original_scans"][name] = layouts["original"]
        result["scans"][name] = layouts["compacted"]
        result["verified_aggregates"][name] = expected
        result["scan_slowdown_vs_compacted"][name] = (
            layouts["original"]["median_scan_ms"] / layouts["compacted"]["median_scan_ms"]
        )
    result["reader_scan_qualified"] = all(
        ratio <= MAX_SCAN_RATIO for ratio in result["scan_slowdown_vs_compacted"].values()
    )
    return result


def retained_locations(report):
    phases = {phase["name"]: phase["details"] for phase in report["phases"]}
    original = phases["post-workload-reader"]
    baseline = phases.get("same-final-state-compacted-reader")
    if baseline is None:
        baseline = report["verified_reader_baseline"]
        compacted = baseline["metadata_locations"]
    else:
        compacted = {name: item["metadata_location"] for name, item in baseline["scans"].items()}
    verified = phases["full-source-target-verification"]
    if (not original or original.keys() != compacted.keys() or original.keys() != verified.keys()
            or original.keys() != baseline["same_final_rows"].keys()):
        raise ValueError("retained report requires matching nonempty verified table sets")
    for name in original:
        before, after = verified[name], baseline["same_final_rows"][name]
        if (before["rows"], before["sha256"]) != (after["rows"], after["sha256"]):
            raise ValueError(f"{name}: retained report lacks matching final-row verification")
    return ({name: item["metadata_location"] for name, item in original.items()},
            compacted)


def profile_summary(profile):
    """DuckDB 1.5.5 has three disjoint plan-creation parent phases."""
    parents = {}
    for name in ("planner", "all_optimizers", "physical_planner"):
        value = profile.get(name)
        if (isinstance(value, bool) or not isinstance(value, (int, float))
                or not math.isfinite(value) or value < 0):
            raise ValueError(f"profile has no valid {name} timing")
        parents[name + "_ms"] = value * 1000
    result = {
        "planning_parent_ms": parents, "planning_ms": sum(parents.values()),
        "profiled_query_latency_ms": profile["latency"] * 1000,
        "nested_phase_ms": {name + "_ms": profile[name] * 1000 for name in (
            "planner_binding", "optimizer_extension", "physical_planner_column_binding",
            "physical_planner_create_plan", "physical_planner_resolve_types",
        ) if name in profile},
        "operators": [],
    }
    def visit(node):
        if "operator_name" in node:
            result["operators"].append({
                "name": node["operator_name"],
                "accumulated_operator_ms": node["operator_timing"] * 1000,
                "returned_cardinality": node.get("operator_cardinality"),
                "extra_info": node.get("extra_info", {}),
            })
        for child in node.get("children", []):
            visit(child)
    visit(profile)
    return result


def diagnose_snapshots(duck, locations, directory, id_range, result):
    """Fill a caller-owned result so failed/partial attempts remain reportable."""
    queries = {
        "full": (SCAN_SQL, []),
        "selective": (SCAN_SQL + " WHERE id BETWEEN ? AND ?", list(id_range)),
    }
    result.update(
        completed=False, planning_qualified=False, metadata_locations=locations,
        protocol={
            "name": "duckdb-pinned-planning-selective-v1",
            "warmup_pairs_per_query_and_mode": WARMUP_PAIRS,
            "unprofiled_pairs_per_query": MEASURED_PAIRS,
            "profiled_pairs_per_query": MEASURED_PAIRS,
            "pair_order": "original/compacted, then compacted/original, repeated",
            "mode_order": "all unprofiled timings, then detailed profiles",
            "queries": {name: {"sql": query, "predicate_parameters": parameters}
                        for name, (query, parameters) in queries.items()},
            "planning_definition": "planner + all_optimizers + physical_planner; excludes parsing/client overhead",
            "planning_ratio_limit": 2.0,
            "selective_wall_ratio_limit": None,
        },
        interpretation=[
            "Profiled timings include instrumentation overhead and are separate from unprofiled query wall samples.",
            "Binding and individual optimizer timings are nested in their parents; never add them again.",
            "Operator timings accumulate across worker threads and are not pure CPU, physical scan wall time or wire measurements.",
            "Queries use immutable metadata pins without REST lookup; metadata and manifest I/O inside DuckDB is included.",
            "This gate covers only the stated warmed planning-phase definition, not first observation, other readers or overall production readiness.",
        ],
        samples=[], summaries={},
    )
    expected = {}
    duck.execute("PRAGMA disable_profiling")
    for mode in ("wall", "profile"):
        for name, (query, predicate) in queries.items():
            for phase, pairs in (("warmup", WARMUP_PAIRS), ("measured", MEASURED_PAIRS)):
                for pair in range(pairs):
                    order = ("original", "compacted") if pair % 2 == 0 else ("compacted", "original")
                    for label in order:
                        sample = {"mode": mode, "query": name, "phase": phase,
                                  "pair": pair, "layout": label}
                        result["samples"].append(sample)
                        profile = None
                        try:
                            if mode == "profile" and phase == "measured":
                                profile = directory / f"{name}-{pair:02}-{label}.json"
                                if profile.exists():
                                    raise ValueError("profile output must be a new file")
                                sample["profile"] = str(profile.resolve())
                                duck.execute("PRAGMA enable_profiling = 'json'")
                                duck.execute("SET profiling_mode = 'detailed'")
                                duck.execute("SET profiling_output = ?", [str(profile.resolve())])
                            started = time.perf_counter_ns()
                            actual = duck.execute(query, [locations[label], *predicate]).fetchall()
                            sample["client_elapsed_ms"] = (time.perf_counter_ns() - started) / 1e6
                            sample["aggregates"] = actual
                            if profile is not None:
                                duck.execute("PRAGMA disable_profiling")
                                raw = profile.read_bytes()
                                sample["profile_sha256"] = hashlib.sha256(raw).hexdigest()
                                sample["diagnostic"] = profile_summary(json.loads(raw))
                            if name not in expected:
                                expected[name] = actual
                            if actual != expected[name]:
                                raise AssertionError(f"{name}: pinned layout aggregates differ")
                            if name == "selective" and not 0 < actual[0][0] < expected["full"][0][0]:
                                raise AssertionError("ID range must select some, but not all, verified rows")
                        except BaseException as error:
                            sample["failure"] = f"{type(error).__name__}: {error}"
                            raise
    for name in queries:
        summary = {"verified_aggregates": expected[name], "layouts": {}}
        for label in locations:
            samples = [sample for sample in result["samples"]
                       if sample["phase"] == "measured" and sample["query"] == name
                       and sample["layout"] == label]
            summary["layouts"][label] = {
                "median_unprofiled_wall_ms": statistics.median(
                    sample["client_elapsed_ms"] for sample in samples if sample["mode"] == "wall"),
                "median_planning_ms": statistics.median(
                    sample["diagnostic"]["planning_ms"] for sample in samples if sample["mode"] == "profile"),
            }
        original, compacted = (summary["layouts"][label] for label in ("original", "compacted"))
        if compacted["median_unprofiled_wall_ms"] <= 0 or compacted["median_planning_ms"] <= 0:
            raise ValueError("baseline timing must be positive to form a ratio")
        summary["unprofiled_wall_ratio"] = original["median_unprofiled_wall_ms"] / compacted["median_unprofiled_wall_ms"]
        summary["planning_ratio"] = original["median_planning_ms"] / compacted["median_planning_ms"]
        summary["planning_qualified"] = summary["planning_ratio"] <= 2.0
        result["summaries"][name] = summary
    result["selectivity"] = expected["selective"][0][0] / expected["full"][0][0]
    result["planning_qualified"] = all(summary["planning_qualified"] for summary in result["summaries"].values())
    result["completed"] = True


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", type=Path, action="append", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--s3-endpoint", required=True)
    parser.add_argument("--diagnostics-dir", type=Path,
                        help="Opt in to bounded planning/selective diagnostics; requires one report/table")
    parser.add_argument("--table")
    parser.add_argument("--id-range", type=int, nargs=2, metavar=("LOW", "HIGH"))
    parser.add_argument("--quiescence-note")
    args = parser.parse_args()
    if args.output.exists():
        parser.error("output must be a new file; retained reports are immutable")
    endpoint = urlparse(args.s3_endpoint)
    if endpoint.scheme not in ("http", "https") or not endpoint.netloc:
        parser.error("S3 endpoint must be an http(s) URL")
    if args.diagnostics_dir is not None:
        if len(args.report) != 1 or not args.table or args.id_range is None or not args.quiescence_note:
            parser.error("diagnostics require one report, --table, --id-range and --quiescence-note")
        if not -(1 << 63) <= args.id_range[0] <= args.id_range[1] < (1 << 63):
            parser.error("ID range must be ordered signed 64-bit integers")
        if args.diagnostics_dir.exists():
            parser.error("diagnostics directory must be new")
        args.diagnostics_dir.mkdir(parents=True)
    elif args.table or args.id_range is not None or args.quiescence_note:
        parser.error("diagnostic options require --diagnostics-dir")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    result = {
        "runs": [], "passed": False, "duckdb": duckdb.__version__,
        "quiescence": "Caller-managed; this command starts or stops no writer processes",
    }
    with args.output.open("x") as output, duckdb.connect() as duck:
        try:
            if args.diagnostics_dir is not None:
                if duckdb.__version__ != "1.5.5":
                    raise ValueError("diagnostic planning definition requires DuckDB 1.5.5")
                duck.execute("SET memory_limit = '1GiB'")
                duck.execute("SET temp_directory = ?", [str((args.diagnostics_dir / "spill").resolve())])
                duck.execute("SET max_temp_directory_size = '4GiB'")
                result["quiescence"] = args.quiescence_note
                result["script_sha256"] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
                result["resource_settings"] = dict(duck.execute(
                    "SELECT name,value FROM duckdb_settings() WHERE name IN "
                    "('memory_limit','temp_directory','max_temp_directory_size','threads') ORDER BY name"
                ).fetchall())
            load_extensions(duck)
            duck.execute("SET httpfs_connection_caching = true")
            result["reader_settings"] = dict(duck.execute(READER_SETTINGS_SQL).fetchall())
            quote = lambda value: "'" + str(value).replace("'", "''") + "'"
            duck.execute(
                "CREATE SECRET reader_s3 (TYPE s3, KEY_ID {}, SECRET {}, REGION {}, "
                "ENDPOINT {}, URL_STYLE 'path', USE_SSL {})".format(
                    quote(os.environ["AWS_ACCESS_KEY_ID"]), quote(os.environ["AWS_SECRET_ACCESS_KEY"]),
                    quote(os.environ.get("AWS_REGION", "us-east-1")), quote(endpoint.netloc),
                    str(endpoint.scheme == "https").lower(),
                )
            )
            result["extensions"] = duck.execute(
                "SELECT extension_name, extension_version FROM duckdb_extensions() WHERE loaded"
            ).fetchall()
            for path in args.report:
                raw = path.read_bytes()
                report = json.loads(raw)
                entry = {"source_report": str(path.resolve()), "source_report_sha256": hashlib.sha256(raw).hexdigest(),
                         "source_binary": report.get("binary"), "passed": False}
                result["runs"].append(entry)
                try:
                    original, compacted = retained_locations(report)
                    if args.diagnostics_dir is None:
                        entry["comparison"] = compare_snapshots(duck, original, compacted)
                        entry["passed"] = entry["comparison"]["reader_scan_qualified"]
                    else:
                        if args.table not in original:
                            raise ValueError("table is absent from the verified retained pair")
                        entry["source_reader_settings"] = report.get("reader_settings")
                        entry["table"] = args.table
                        entry["diagnostics"] = {}
                        diagnose_snapshots(duck, {"original": original[args.table], "compacted": compacted[args.table]},
                                           args.diagnostics_dir, args.id_range, entry["diagnostics"])
                        entry["passed"] = entry["diagnostics"]["planning_qualified"]
                except Exception as error:
                    entry["failure"] = str(error)
            result["passed"] = all(entry["passed"] for entry in result["runs"])
        except Exception as error:
            result["failure"] = str(error)
        finally:
            json.dump(result, output, indent=2)
            output.write("\n")
    print(f"{'PASS' if result['passed'] else 'FAIL'}: {args.output.resolve()}")
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
