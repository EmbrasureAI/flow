# Independent reader and rewrite interoperability

`../readers.py` runs the daemon against the production fixture and starts its own Spark and Trino containers on the existing Docker network. It exposes no host ports and removes only its own reader containers. PostgreSQL, REST, MinIO, table metadata, journals and reports remain available after the run.

```sh
uv run tests/production/readers.py \
  --postgres-url 'postgres://flow:local-test-password@127.0.0.1:55432/flow?sslmode=disable' \
  --catalog-uri http://127.0.0.1:58181 \
  --s3-endpoint http://127.0.0.1:59000 \
  --binary target/release/embrasure-flow \
  --artifacts target/production-readers-01
```

Provide `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` and `AWS_REGION` for the disposable local object store. The default Docker network is `flow-production-local_default`, with internal endpoints `http://rest:8181` and `http://minio:9000`; flags allow other fixture names. Do not rebuild the selected binary or restart shared services during a run.

The harness compares every ordered field with PostgreSQL after initial COPY, inserts, updates, deletes, primary-key moves and each external rewrite. Spark, Trino and DuckDB also read a retained initial snapshot. Values cover nullable fields, Unicode, decimal, negative-epoch dates and microsecond timestamps, UUIDs and binary data. Engine-specific display forms are normalized without dropping columns or rounding timestamps.

Only Spark performs external maintenance. Each procedure must report rewritten inputs and commit a new snapshot; an empty invocation does not pass. CDC then modifies the rewritten data and must reach its source-wide materialization barrier before all readers are compared again. The final phase also kills and restarts the daemon with queued changes. The default invocation disables native compaction. The fixture allows a 60-second soft and 300-second hard L0 age because independent reader jobs take time; it does not qualify the default reader-debt latency budget.

To qualify coexistence with native compaction and keyless duplicate rows, add both options to the same invocation:

```sh
uv run tests/production/readers.py \
  --postgres-url 'postgres://flow:local-test-password@127.0.0.1:55432/flow?sslmode=disable' \
  --catalog-uri http://127.0.0.1:58181 \
  --s3-endpoint http://127.0.0.1:59000 \
  --binary target/qualification-binaries/current/embrasure-flow \
  --artifacts target/production-readers-native \
  --native-compaction --keyless-duplicates
```

`--native-compaction` enables the daemon's normal compactor role and requires a native data rewrite before the first external procedure, then another native data rewrite after each Spark rewrite and its subsequent CDC. It retains the same age settings and waits for committed snapshots with positive removed-data-file counts. Those waits can take a soft-age interval. Each Spark call must independently return a positive rewritten-input count, no failed data-file groups, and new retained external `replace` snapshots. Before/after metadata, external snapshots, and any native commits during the call are saved separately. Spark is the only external writer in the fixture's unique namespace, so a native commit cannot satisfy this attribution check. The gate proves both maintainers operate on the same tables across CDC and restart; it does not require their file reads to overlap.

`--keyless-duplicates` adds an append-only table without a primary key. Its 513 initial rows include repeated complete rows, different decimal amounts for the same `id`, and NULL amounts. Every engine compares the full row list using the same total order over both columns, with explicit `NULLS FIRST` and numeric decimal ordering. Duplicate multiplicities are preserved. Follow-up transactions append more duplicates, and a separate Spark data rewrite is followed by a daemon restart and another append. Spark, Trino and DuckDB also recheck the retained initial duplicate snapshot after every phase. The option works with either native-compaction mode.

The default invocation keeps native compaction disabled and omits the additional table. Every successful run checks the joined daemon logs for worker panics, including panics that leave process exit status zero.

Versions are pinned to Spark 3.5.6 and Trino 470 Docker image digests. Iceberg Java 1.10.1's Spark 3.5 / Scala 2.12 runtime and AWS bundle are downloaded from Maven Central and checked against fixed SHA-512 values. The harness records these pins in its report; it does not use the Rust implementation to read data inside either independent engine.

The procedure syntax follows [Iceberg 1.10.1's Spark procedure reference](https://iceberg.apache.org/docs/1.10.1/spark-procedures/). Trino's configuration follows the [470 REST catalog documentation](https://github.com/trinodb/trino/blob/470/docs/src/main/sphinx/object-storage/metastores.md#rest-catalog) and [470 native S3 documentation](https://github.com/trinodb/trino/blob/470/docs/src/main/sphinx/object-storage/file-system-s3.md). This is a correctness and interoperability suite, not a performance benchmark.
