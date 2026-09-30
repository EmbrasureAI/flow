# Local service integration tests

This suite runs the actual Rust daemon against PostgreSQL, an Apache Iceberg REST catalog, and MinIO. Stock DuckDB independently reads the committed Iceberg tables. The driver compares complete, ordered rows with PostgreSQL; it does not substitute direct Parquet reads for an Iceberg query.

It creates a unique source schema, publication, replication slot, and target namespace per run. Only disposable test services should be supplied. The final test deliberately truncates its source table and confirms its table is blocked without acknowledging that transaction while healthy tables continue. Artifacts and service data remain for inspection.

## Run

Requirements: Docker with Compose, the built native daemon, and [uv](https://docs.astral.sh/uv/). DuckDB, psycopg, boto3, and fastavro are pinned in the driver's script metadata. First use downloads DuckDB's signed `httpfs` and `iceberg` extensions; their actual versions are recorded in `report.json` because extension releases are independent of the Python package.

From the repository root:

```sh
cargo build --locked -p flow-daemon
export COMPOSE_PROJECT_NAME=flow-local-test
docker compose -f tests/local/compose.yaml up -d --wait
export AWS_ACCESS_KEY_ID=demo-access
export AWS_SECRET_ACCESS_KEY=demo-secret-key
export AWS_REGION=us-east-1
export FLOW_POSTGRES_URL="postgres://flow:local-test-password@$(docker compose -f tests/local/compose.yaml port postgres 5432)/flow?sslmode=disable"
uv run tests/local/run.py \
  --catalog-uri "http://$(docker compose -f tests/local/compose.yaml port rest 8181)" \
  --s3-endpoint "http://$(docker compose -f tests/local/compose.yaml port minio 9000)" \
  --artifacts /tmp/flow-local-test-001
```

The artifact directory must be new. `--binary` selects another daemon build; `--timeout` sets the maximum seconds per bounded wait or initialization (default 180). The driver also accepts `--postgres-url` and `--warehouse` for other disposable service deployments.

The Compose fixture uses dynamically assigned loopback ports, so it leaves existing local services alone. Query the ports again if recreating or restarting containers changes their assignments. The test terminates only the replication connection for its own uniquely named slot; it does not restart shared services.

Remove this fixture's containers when finished:

```sh
docker compose -f tests/local/compose.yaml down
```

This removes container writable layers. Add `--volumes` to also remove this fixture's Docker volumes. Local test runs leave logical slots behind in the disposable database; recreate the fixture between long-running development sessions rather than accumulating abandoned slots.

## Coverage

1. Binary COPY of 4,096 initial orders and 16 accounts, including nullable text, Unicode, decimals, booleans, dates and timestamps before the Unix epoch, UUIDs, binary values, integers, and floating point.
2. A transaction committed after the logical slot's consistent point and before streaming starts, proving the exported-snapshot/WAL handoff.
3. Inserts, updates, deletes, primary-key moves, and one source transaction spanning both target tables.
4. A rolled-back transaction and insert/update/delete collapse within a committed transaction.
5. A transaction containing 14,008 mutations, exceeding both the source's configured logical decoding buffer and the daemon's journal chunk size. PostgreSQL slot statistics must prove it was streamed.
6. Forced termination of the active replication backend, automatic reconnect, and subsequent materialization.
7. `SIGKILL` of the daemon, committed WAL backlog while it is down, and restart using the same journal, RocksDB state, and logical slot.
8. Real compaction commits, unchanged live rows, retained historical snapshot reads, and retirement of positional delete files.
9. Native Avro manifest checks across retained snapshots: unique operation IDs, unique live file references, correct data/file sequence numbers, and sorted unique positional deletes pointing within live data files.
10. Fail-closed handling of unsupported `TRUNCATE`: nonzero daemon exit, unchanged target snapshots, and no source acknowledgment past the unsupported transaction.

Every phase records its duration and evidence. `report.json`, catalog metadata snapshots, generated configuration, initial-copy logs, daemon logs, and the durable local state are retained. The generated configuration references PostgreSQL credentials through an environment variable.

This is correctness and reader-interoperability coverage. It is not a performance benchmark or a complete failure matrix. It does not deterministically interrupt every commit boundary, exercise full server or S3 outages, validate multiple Iceberg readers, or cover unsupported schema changes and unchanged TOAST values.

## Table publication isolation

`table_isolation.py` uses the same disposable services and connection arguments.
It injects table-specific catalog failures with one worker, a four-descriptor
admission budget, and disk-backed mutation collapse. It checks healthy-table rows
and progress independently while the source acknowledgement remains pinned.

```sh
uv run tests/local/table_isolation.py \
  --catalog-uri "http://$(docker compose -f tests/local/compose.yaml port rest 8181)" \
  --s3-endpoint "http://$(docker compose -f tests/local/compose.yaml port minio 9000)" \
  --artifacts /tmp/flow-table-isolation-001
```

The suite covers mixed transactions beyond the admission window, primary-key
moves and repeated delete/reinsert, 403/503 repair, both tables blocked followed by
repairing just one, restart while target loads are denied, lost successful commit
responses, and commits completed after the caller is killed. Ordinary table
failures must preserve the daemon PID. Native manifest audits verify unique
operation identities and exact reader results. Source connection/slot/identity failure remains global. Classified table schema/row errors and TRUNCATE are isolated.

CI runs both cases on PostgreSQL 14 and 18 (`isolation` shard), together with
[`default_identity.py`](DEFAULT_IDENTITY.md#engine-only-run).

Run it again with `--quota` and a new artifact directory for the separate 64 MiB
journal-limit scenario. That run verifies a safe stop with unpublished changes
retained, then explicitly increases the fixture limit and checks exact replay.
This is a journal quota; it does not bound all index, control, or object storage.

## Sustained concurrency and background compaction

`stress.py` reuses the same disposable services and connection arguments. It runs three independently seeded PostgreSQL writers for 120 seconds of active work by default. Writers use separate key ranges, with repeated changes to a small hot-key set within each range. Four checkpoints pause writers between transactions, wait for a source-wide materialization barrier, and compare every row and column in both tables with DuckDB.

```sh
uv run tests/local/stress.py \
  --catalog-uri "http://$(docker compose -f tests/local/compose.yaml port rest 8181)" \
  --s3-endpoint "http://$(docker compose -f tests/local/compose.yaml port minio 9000)" \
  --duration 120 --workers 3 --seed 20260904 \
  --artifacts /tmp/flow-local-stress-001
```

The daemon's background compactor role is enabled throughout. To also exercise an independent process racing with ingestion, build the local service and add its path to the same command:

```sh
cargo build --locked -p flow-testkit --example local_compactor
# Add to the stress.py arguments:
# --compactor-binary target/debug/examples/local_compactor
```

The external service scans standard Iceberg tables, writes replacement Parquet files, and commits validated rewrites using its own scratch storage. It never opens the ingestion daemon's RocksDB directory. Its output is retained in `compactor.log`, and it remains running while the ingestion daemon is killed and restarted. The final audit requires compaction snapshots between ingestion snapshots during the recorded active-writing intervals for both tables. With an external process configured, each table must also have an external compaction commit during those intervals.

The workload exercises inserts, updates, null transitions, binary changes, deletes, deletion followed by reinsertion, primary-key movement, insert/update/delete collapse, full transaction rollback, and savepoint rollback. A separate large transaction streams thousands of changes, rolls back a substantial child transaction, then commits additional updates and deletes. Halfway through the concurrent phase, the daemon receives `SIGKILL`; producers continue committing for three seconds before it restarts against the same durable state.

While producers run, DuckDB checks current snapshots for duplicate keys and partially published transaction generations. Each writer updates an eight-row cohort together in each table; a visible cohort must contain all eight rows at one generation. This checks atomicity within each Iceberg table, without assuming cross-table snapshot atomicity. Retained metadata locations are reread throughout and after compaction to verify historical rows remain unchanged. Source and target are compared in full only at aligned, paused checkpoints.

The report records committed transaction counts, actual affected insert/update/delete row counts, aborted changes separately, compaction and ingestion commit counts, retained snapshot checks, and peak observed source lag and pending transactions. The seed reproduces each writer's operation sequence; thread scheduling and total transaction counts still depend on machine speed. Duration excludes checkpoint waits, and the suite is correctness coverage rather than a throughput benchmark.

## References

- [DuckDB Iceberg extension](https://duckdb.org/docs/current/core_extensions/iceberg/overview) and [S3 Iceberg reads](https://duckdb.org/docs/current/guides/network_cloud_storage/s3_iceberg_import): scan the exact metadata location returned by the catalog, with standard S3 credentials and an explicit local endpoint.
- [PostgreSQL replication slot statistics](https://www.postgresql.org/docs/current/monitoring-stats.html#MONITORING-PG-STAT-REPLICATION-SLOTS-VIEW): independently verify logical streaming occurred.
- [Iceberg format specification](https://iceberg.apache.org/spec/): validate manifest status, sequence inheritance, and v2 positional deletes independently of the writer.
- Package versions were resolved from each maintainer's PyPI metadata: [DuckDB](https://pypi.org/project/duckdb/), [psycopg](https://pypi.org/project/psycopg/), [boto3](https://pypi.org/project/boto3/), and [fastavro](https://pypi.org/project/fastavro/).

## Source schema isolation

Run `uv run tests/local/schema_isolation.py` with the same service/binary/artifact
arguments, then repeat with `--explicit` and a new artifact directory. It verifies
required-to-nullable evolution through a lost catalog response, restart and a
later nullable addition. Renaming a selected column must block only that table,
retain its changes, preserve healthy-table progress and hold the source ACK.
The explicit-selection case checks that excluded values never enter the journal.
Source-table blocks require explicit resynchronization; ordinary publication blocks still retry.

`schema_isolation.py --heap-rewrite` covers excluded-column migration safety:
adding without a default and backfilling with ordinary updates preserves capture;
adding a volatile default rewrites the heap and durably fences only that table.
It checks healthy-table progress, restart retention, and the safe diagnostic reason.
