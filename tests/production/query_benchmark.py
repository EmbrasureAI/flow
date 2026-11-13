#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["duckdb==2.0.0", "pyiceberg[pyarrow]>=0.9", "numpy"]
# ///
"""Measure selective reads of a retained benchmark table; never changes the table.

Run serially with ingestion and other benchmarks. PyIceberg supplies independent
expected rows; DuckDB reads the same immutable metadata location for every query.
"""

import argparse
import json
import os
import random
import statistics
import sys
import time
from pathlib import Path
from urllib.parse import urlparse

import duckdb
import numpy as np
import pyarrow.compute as pc
from pyiceberg.catalog import load_catalog

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "local"))
from duckdb_extensions import load_extensions


def measure(connection, queries, repeats):
    samples = []
    for _ in range(repeats):
        for sql, parameters, expected in queries:
            started = time.perf_counter_ns()
            actual = connection.execute(sql, parameters).fetchall()
            samples.append((time.perf_counter_ns() - started) / 1e6)
            if sorted(actual) != sorted(expected):
                raise AssertionError(f"reader result differs from PyIceberg: {parameters}")
    ordered = sorted(samples)
    return {
        "queries": len(queries), "repeats": repeats,
        "median_ms": statistics.median(samples),
        "p90_ms": ordered[int(0.9 * (len(ordered) - 1))],
        "samples_ms": samples,
    }


def benchmark(args):
    if args.repeats < 3:
        raise ValueError("use at least three repeats")
    key = os.environ.get("AWS_ACCESS_KEY_ID", "demo-access")
    secret = os.environ.get("AWS_SECRET_ACCESS_KEY", "demo-secret-key")
    region = os.environ.get("AWS_REGION", "us-east-1")
    catalog = load_catalog("query-benchmark", type="rest", uri=args.catalog_uri, **{
        "s3.endpoint": args.s3_endpoint, "s3.access-key-id": key,
        "s3.secret-access-key": secret, "s3.region": region,
    })
    table = catalog.load_table(args.table)
    location = table.metadata_location
    # A full scan independently applies deletes before choosing queries. Sampling
    # follows key order, so physical file order cannot change the selected keys.
    reference = table.scan(selected_fields=("id", "version", "payload")).to_arrow()
    ids = reference.column("id").to_numpy()
    versions = reference.column("version").to_numpy()
    positive = np.sort(ids[ids > 0])
    negative = np.sort(ids[ids < 0]).tolist()
    if len(positive) == 0:
        raise ValueError("expected a benchmark table with positive IDs")
    rng = random.Random(args.seed)
    points = [int(positive[rng.randrange(len(positive))]) for _ in range(40)]
    starts = [rng.randrange(int(positive.min()), max(int(positive.min()) + 1,
                                                   int(positive.max()) - 1000))
              for _ in range(20)]
    markers = [rng.sample(negative, min(50, len(negative))) for _ in range(10)] if negative else []
    queries = {"point_lookup": [], "range_1000": [], "full_aggregate": []}
    for key_id in points:
        positions = np.flatnonzero(ids == key_id)
        if len(positions) != 1:
            raise AssertionError("reference contains duplicate positive IDs")
        index = int(positions[0])
        expected = [(key_id, int(versions[index]), reference.column("payload")[index].as_py())]
        queries["point_lookup"].append((
            "SELECT id,version,payload FROM iceberg_scan(?) WHERE id = ?",
            [location, key_id], expected))
    for start in starts:
        selected = (ids >= start) & (ids <= start + 999)
        count = int(np.count_nonzero(selected))
        expected = [(count, int(versions[selected].sum()) if count else None)]
        queries["range_1000"].append((
            "SELECT count(*),sum(version) FROM iceberg_scan(?) WHERE id BETWEEN ? AND ?",
            [location, start, start + 999], expected))
    queries["full_aggregate"].append((
        "SELECT count(*),sum(version),max(length(payload)) FROM iceberg_scan(?)",
        [location], [(len(ids), int(versions.sum()),
                      pc.max(pc.utf8_length(reference.column("payload"))).as_py())]))
    if markers:
        queries["marker_lookup_50"] = [(
            "SELECT id FROM iceberg_scan(?) WHERE id IN (SELECT unnest(?::BIGINT[]))",
            [location, values], [(value,) for value in values]) for values in markers]
    rows = len(ids)
    del reference, ids, versions, positive, negative
    with duckdb.connect() as connection:
        load_extensions(connection)
        connection.execute("SET httpfs_connection_caching = true")
        endpoint = urlparse(args.s3_endpoint)
        # SQL string literals, including credentials, must be escaped separately.
        literal = lambda value: "'" + value.replace("'", "''") + "'"
        connection.execute(
            f"CREATE SECRET s (TYPE s3, KEY_ID {literal(key)}, SECRET {literal(secret)}, "
            f"REGION {literal(region)}, ENDPOINT {literal(endpoint.netloc)}, "
            f"URL_STYLE 'path', USE_SSL {str(endpoint.scheme == 'https').lower()})")
        connection.execute("SELECT count(*) FROM iceberg_scan(?)", [location]).fetchone()
        report = {
            "table": args.table, "metadata_location": location, "rows": rows,
            "duckdb": duckdb.__version__, "seed": args.seed,
            "extensions": dict(connection.execute(
                "SELECT extension_name,extension_version FROM duckdb_extensions() WHERE loaded"
            ).fetchall()),
            "external_file_cache": connection.execute(
                "SELECT current_setting('enable_external_file_cache')").fetchone()[0],
            "protocol": "Warm metadata/connections; 40 points, 20 ranges, full aggregate, "
                        "10 marker sets; exact results checked against a full PyIceberg scan. "
                        "Times include planning and fetching results, exclude reference checks.",
            "point_ids": points, "range_starts": starts,
        }
        for name, workload in queries.items():
            report[name] = measure(connection, workload, args.repeats)
        return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog-uri", required=True)
    parser.add_argument("--s3-endpoint", required=True)
    parser.add_argument("--table", required=True)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--seed", type=int, default=11)
    args = parser.parse_args()
    print(json.dumps(benchmark(args), indent=2))


if __name__ == "__main__":
    main()
