#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["duckdb==1.5.5"]
# ///
"""Check exported v3 pipeline fixtures with a stock, independent Iceberg reader."""
import argparse
import json
from pathlib import Path

import duckdb

from duckdb_extensions import load_extensions


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("fixtures", type=Path, help="FLOW_V3_EXPORT directory from v3_pipeline")
    args = parser.parse_args()
    args.fixtures.mkdir(parents=True, exist_ok=True)
    report = {"passed": False, "duckdb": duckdb.__version__, "fixtures": []}
    try:
        with duckdb.connect() as connection:
            load_extensions(connection)
            report["extensions"] = dict(connection.execute(
                "SELECT extension_name, extension_version FROM duckdb_extensions() WHERE loaded"
            ).fetchall())
            report["external_file_cache"] = connection.execute(
                "SELECT current_setting('enable_external_file_cache')"
            ).fetchone()[0]
            # Require every case: an empty or partial export must never pass.
            for name in ("cumulative", "compacted", "upgraded"):
                metadata = args.fixtures / f"{name}.metadata.json"
                assert json.loads(metadata.read_text())["format-version"] == 3
                expected = json.loads((args.fixtures / f"{name}.expected.json").read_text())
                actual = connection.execute(
                    "SELECT id, value FROM iceberg_scan(?) ORDER BY id", [str(metadata.resolve())]
                ).fetchall()
                # Spark validates reserved lineage fields separately. This gate
                # checks complete user rows and multiplicities through v3 deletes.
                assert [list(row) for row in actual] == [row[:2] for row in expected], name
                report["fixtures"].append({"name": name, "rows": len(actual)})
        report["passed"] = True
    except Exception as error:
        report["error"] = str(error)
        raise
    finally:
        (args.fixtures / "reader-report.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
