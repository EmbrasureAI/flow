#!/usr/bin/env python3
"""Summarize where a benchmark run spent its time, from its daemon events and metrics.

Usage: epoch_report.py ARTIFACT_DIR [ARTIFACT_DIR ...] [--json]

Each directory is a `benchmark.py --artifacts` result. The report needs the
daemon's `flow_events` debug events (benchmark.py enables them) and the exported
`state/metrics.prom`.

CDC service time is the sum of each epoch's time from collapse start to source
ledger completion. Unlike drain throughput it excludes queueing, so it varies
much less between identical runs; compare it per mutation across run lengths.
"""

import argparse
from datetime import datetime
import json
from pathlib import Path
import re
import statistics
import sys

STAGES = ("collapse", "prepare", "commit", "apply")


def timestamp(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()


def metric_sum(directory, name, labels=""):
    path = Path(directory) / "state" / "metrics.prom"
    if not path.exists():
        return None
    total = 0.0
    for line in path.read_text().splitlines():
        if line.startswith(name + "_sum") and labels in line:
            total += float(line.rsplit(None, 1)[-1])
    return total


def events(directory):
    found = []
    for path in sorted(Path(directory).glob("daemon-*.log")):
        with path.open(errors="replace") as lines:
            for line in lines:
                if not line.startswith("{") or '"event"' not in line or "transaction_journaled" in line:
                    continue
                record = json.loads(line)
                fields = record.get("fields", {})
                if "event" in fields:
                    found.append((timestamp(record["timestamp"]), fields["event"], fields))
    found.sort(key=lambda item: item[0])
    return found


def summarize(directory):
    directory = Path(directory)
    report = json.loads((directory / "report.json").read_text())
    phases = {phase["name"]: phase.get("details", {}) for phase in report.get("phases", [])}
    workload = phases.get("open-loop-workload", {})
    latency = workload.get("latency", {}).get("commit_to_catalog", {})
    stages = dict.fromkeys(STAGES, 0.0)
    modes = {}
    epochs = []
    transactions = 0
    collapse_start = prepared = published = None
    for at, event, fields in events(directory):
        if event == "epoch_collapsed":
            elapsed = fields["elapsed_ms"] / 1000
            collapse_start = at - elapsed
            stages["collapse"] += elapsed
            modes[fields.get("collapse_mode", "?")] = modes.get(fields.get("collapse_mode", "?"), 0) + 1
            prepared = published = None
        elif event == "ingest_prepared" and collapse_start is not None:
            stages["prepare"] += fields["precommit_prepare_ms"] / 1000
            prepared = at
        elif event == "table_published" and prepared is not None:
            stages["commit"] += at - prepared
            published = at
        elif event == "epoch_completed" and published is not None:
            stages["apply"] += at - published
            epochs.append((collapse_start, at))
            transactions += fields.get("transactions", 0)
            collapse_start = prepared = published = None
    service = sum(end - start for start, end in epochs)
    span = epochs[-1][1] - epochs[0][0] if epochs else 0.0
    mutations = transactions * report.get("transaction_rows", 0)
    resources = phases.get("resource-and-cost-observations", {})
    return {
        "run": directory.name,
        "correct": report.get("correctness_passed"),
        "failure": report.get("failure"),
        "source_mutations_per_second": workload.get("source_commit_window_mutations_per_second"),
        "missed_transactions": workload.get("missed_scheduled_arrivals"),
        "drain_mutations_per_second": workload.get("measured_cohort_catalog_mutations_per_second_including_drain"),
        "commit_to_catalog_ms": {key: latency.get(key) for key in ("p50_ms", "p95_ms", "p99_ms")},
        "epochs": len(epochs),
        "mutations": mutations,
        "cdc_service_seconds": service,
        "service_us_per_mutation": service / mutations * 1e6 if mutations else None,
        "stage_seconds": stages,
        "between_epochs_seconds": span - service,
        "compaction_stall_seconds": metric_sum(directory, "flow_compaction_publication_stall_seconds"),
        "index_apply_seconds": metric_sum(directory, "flow_index_apply_seconds"),
        "collapse_modes": modes,
        "peak_rss_mib": (resources.get("peak_sampled_process_rss_bytes") or 0) / 2**20,
    }


def fmt(value, spec):
    return "-" if value is None else format(value, spec)


def table(rows):
    header = ("run", "ok", "drain/s", "p50 s", "p95 s", "svc s", "µs/mut", "collapse",
              "prepare", "apply", "between", "stall", "disk ep", "RSS MiB")
    out = ["| " + " | ".join(header) + " |", "|" + "---|" * len(header)]
    for row in rows:
        latency = row["commit_to_catalog_ms"]
        stages = row["stage_seconds"]
        disk = sum(count for mode, count in row["collapse_modes"].items() if mode != "memory")
        out.append("| " + " | ".join((
            row["run"], "yes" if row["correct"] else "NO",
            fmt(row["drain_mutations_per_second"], ",.0f"),
            fmt(latency["p50_ms"] and latency["p50_ms"] / 1000, ".1f"),
            fmt(latency["p95_ms"] and latency["p95_ms"] / 1000, ".1f"),
            fmt(row["cdc_service_seconds"], ".1f"), fmt(row["service_us_per_mutation"], ".1f"),
            fmt(stages["collapse"], ".1f"), fmt(stages["prepare"], ".1f"), fmt(stages["apply"], ".1f"),
            fmt(row["between_epochs_seconds"], ".1f"), fmt(row["compaction_stall_seconds"], ".1f"),
            str(disk), fmt(row["peak_rss_mib"], ".0f"))) + " |")
    return "\n".join(out)


def median(rows, pick):
    values = [value for value in map(pick, rows) if value is not None]
    return statistics.median(values) if values else None


def compare(base, candidate):
    """Median-of-runs comparison; lower is better except throughput."""
    measures = (
        ("CDC service µs/mutation", lambda row: row["service_us_per_mutation"], False),
        ("drain mutations/s", lambda row: row["drain_mutations_per_second"], True),
        ("commit→catalog p95 ms", lambda row: row["commit_to_catalog_ms"]["p95_ms"], False),
        ("collapse s", lambda row: row["stage_seconds"]["collapse"], False),
        ("prepare s", lambda row: row["stage_seconds"]["prepare"], False),
        ("apply + ledger s", lambda row: row["stage_seconds"]["apply"], False),
        ("compaction stall s", lambda row: row["compaction_stall_seconds"], False),
        ("peak RSS MiB", lambda row: row["peak_rss_mib"], False),
    )
    out = ["| measure (median) | base | candidate | change |", "|---|---:|---:|---:|"]
    for name, pick, higher_better in measures:
        b, c = median(base, pick), median(candidate, pick)
        change = "-" if not b or c is None else f"{(c - b) / b * 100:+.1f}%"
        out.append(f"| {name} | {fmt(b, ',.1f')} | {fmt(c, ',.1f')} | {change} |")
    spread = measures[0][1]
    for label, rows in (("base", base), ("candidate", candidate)):
        values = [value for value in map(spread, rows) if value is not None]
        if len(values) > 1:
            out.append(f"\n{label} service µs/mutation range: {min(values):.1f}–{max(values):.1f}")
    return "\n".join(out)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("artifacts", nargs="+", type=Path)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    rows = []
    for directory in args.artifacts:
        try:
            rows.append(summarize(directory))
        except (OSError, KeyError, ValueError) as error:
            print(f"{directory}: cannot summarize ({error})", file=sys.stderr)
    if args.json:
        print(json.dumps(rows, indent=2))
    else:
        print(table(rows))


if __name__ == "__main__":
    main()
