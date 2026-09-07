#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["duckdb==1.5.5"]
# ///
"""Load stock reader extensions and verify cold-cache installation."""

import json
import tempfile

import duckdb


def load_extensions(connection):
    # The signed core repository supports bootstrap without httpfs. Forcing an
    # HTTPS repository first makes httpfs installation depend on httpfs itself.
    connection.execute("INSTALL httpfs FROM core; LOAD httpfs; INSTALL iceberg FROM core; LOAD iceberg")
    # Iceberg 45163a28 on DuckDB 1.5.5 uses the wrong buffer offset for cached
    # Puffin ranges. Keep the cache disabled for correct shared-vector reads.
    connection.execute("SET enable_external_file_cache = false")


if __name__ == "__main__":
    with tempfile.TemporaryDirectory(prefix="flow-duckdb-extensions-") as directory:
        for cache in ("cold", "warm"):
            with duckdb.connect(config={"extension_directory": directory}) as connection:
                load_extensions(connection)
                assert not connection.execute("SELECT current_setting('allow_unsigned_extensions')").fetchone()[0]
                assert not connection.execute("SELECT current_setting('enable_external_file_cache')").fetchone()[0]
                loaded = dict(connection.execute(
                    "SELECT extension_name, extension_version FROM duckdb_extensions() "
                    "WHERE loaded AND extension_name IN ('httpfs', 'iceberg', 'avro')"
                ).fetchall())
                assert loaded.keys() == {"httpfs", "iceberg", "avro"}, loaded
                print(json.dumps({"cache": cache, "duckdb": duckdb.__version__, "extensions": loaded}))
