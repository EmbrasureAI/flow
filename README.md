# Embrasure Flow

A Rust streaming materializer from PostgreSQL into ordinary Apache Iceberg v2 tables. Updates publish replacement Parquet rows and position deletes in one standard snapshot. Readers never need this service, its journal, or its RocksDB index.

**Status:** implemented and tested against PostgreSQL 14–18, an Apache Iceberg REST catalog, MinIO, and stock DuckDB, Spark and Trino. Recovery, schema changes, external maintenance and Linux deployment have passing local integration gates. The [qualification report](docs/production-qualification.md) records the evidence and outstanding performance targets. This is not yet a qualified production release.

## Build and inspect

Install the toolchain in `rust-toolchain.toml`, a C++ compiler, libclang, CMake, pkg-config and OpenSSL development headers.

```sh
cargo build --locked --release -p flow-daemon
cargo test --locked --workspace
cargo run --locked -p flow-daemon -- --config examples/flow.toml check
```

The standalone binary is `target/release/embrasure-flow`. An opt-in [container demo](demo/README.md) supplies PostgreSQL, MinIO, an Apache REST catalog fixture and Trino. The Linux demo has passed initial load, CRUD, key changes, restart, graceful shutdown and non-root volume checks.

The separate [local service integration suite](tests/local/README.md) runs the native binary against disposable Docker services and checks complete Iceberg row results with DuckDB.

The [production-oriented local suite](tests/production/README.md) adds durable PostgreSQL and object volumes, a PostgreSQL-backed catalog, service resource limits, real committed-response loss, container restarts, and an open-loop workload matrix with separate journal, catalog, and reader timings.

## Data path

```text
pgoutput protocol 2 → bounded transaction spool → synced source journal
                                                        │
                          complete transaction envelopes│
                                                        ▼
bounded mutation collapse → Parquet + position deletes → prepared plan
                                                        │
                                      validated catalog CAS
                                                        │
                                      ordinary Iceberg snapshot
                                                        │
                                   durable index update → source ACK
```

One actor captures a logical slot. Per-table work can run concurrently, with a source-wide acknowledgement ledger waiting for every affected table in the contiguous transaction prefix. A table never exposes part of a source transaction. Cross-table query atomicity is not promised.

The same table coordinator handles ingestion and physical rewrites. Local compaction workers build files against a frozen index view while CDC continues. At publication, the coordinator validates the inputs and translates intervening deletes to the new physical rows. Replaced inputs cause safe replanning. Workers cannot commit to a catalog; the [local compaction protocol](docs/local-compaction.md) preserves ordinary recovery and external reconciliation.

## Implemented

- Pinned Postgres transport, strict pgoutput v2 decoding, streamed transaction and nested-abort handling, reconnect feedback, complete-row validation, binary initial COPY helpers and a consistent-snapshot initializer.
- Versioned, checksummed journal segments, transaction-preserving group commit, disk quotas, exclusive writer locking, recovery truncation, independent readers and reclamation after materialization.
- Persistent forward/reverse row indices, bounded memory collapse with disk replay on overflow, stable composite keys, row fingerprints, staged commit plans, conditional index updates, checkpoints and full index rebuild APIs.
- Arrow batches and standard Parquet files with stable field IDs and Iceberg metrics; globally sorted position deletes; immutable physical attempts and deterministic logical operation IDs.
- Standard v2 RowDelta and RewriteFiles actions, catalog compare-and-swap, ancestry and delete-conflict validation, retained-history recovery and native Avro prepared plans.
- Debt-based compaction planning, delete-file consolidation, exact per-file live-row counts, residual deletes that preserve unselected data files, bounded concurrent Parquet builds and equivalent external rewrite reconciliation.
- Protected snapshot expiration, conservative collection of registered service-owned artifacts, retained checkpoints and crash-safe index-generation replacement.
- Event-driven batching, adaptive publication deadlines, a global commit-rate budget, bounded capture queues, per-table fencing, WAL pressure checks and Prometheus latency, reader-debt and storage/REST diagnostics.
- Nullable additive DDL propagation, including historical schema replay and recovery across schema changes; incompatible changes stop capture before acknowledgement.
- A CLI with `check`, `init`, `run`, and `status`; TLS-capable PostgreSQL connections and REST/S3 through Iceberg's storage abstractions.

See [architecture decisions](docs/architecture.md), [implementation checkpoints](docs/implementation-plan.md), and the [supplied specification](docs/spec.md).

## Configure and run

Copy `examples/flow.toml` and provide the PostgreSQL URL through its named environment variable. Storage and catalog credentials follow their standard credential providers. The configured column list defines the initial source schema. New nullable columns without a non-null backfill are discovered automatically; keep the original prefix in the configuration across restarts. Field IDs are stable destination identities, while PostgreSQL attribute identity is checked separately to detect drop-and-recreate changes.

Mutable tables require a primary key and `REPLICA IDENTITY FULL`. Publications must include inserts, updates, deletes and truncates, with exactly the configured tables and no row filters. TRUNCATE causes capture to stop explicitly. Keyless tables are supported only in append-only mode.

An unchanged TOAST value is recovered from the complete old tuple included in that replication event. Missing or unresolved old values stop capture rather than publishing an incomplete row.

```sh
export FLOW_POSTGRES_URL='postgres://user:password@host/database?sslmode=require'
./target/release/embrasure-flow --config flow.toml init
./target/release/embrasure-flow --config flow.toml run
```

`init` creates a replication slot and copies up to four tables concurrently while capturing CDC into the durable journal. Each table has its own staging journal and publishes reader-sized initial files. Source acknowledgement stays at the initial cut until every table has a published base. Restart reuses completed staging and recopies only unfinished tables from a new temporary snapshot; the original replication slot and source identity stay intact. The service never drops or resets permanent slots automatically.

The default combined roles are `ingest,coordinator,compactor`. `--roles=ingest,coordinator` disables built-in compaction. At a hard reader-debt limit the affected table pauses publication and retries, while capture continues within its disk budget. External maintenance can clear that debt and resume publication without restarting the daemon. An optional `[compaction]` section configures the thresholds. Workers and rebuild facilities are also available as Rust libraries.

`status` reads an atomic status file without locking the index. It reports exact source watermarks, readiness, process identity and freshness while running or stopped. `state_dir/metrics.prom` supports a Prometheus textfile collector. See [observability](docs/observability.md) for latency definitions, reader debt and the distinction between SDK operations and billed requests.

## Recovery and operational limits

`materialized` acknowledgement is the default. `journaled` mode requires an explicit declaration of independently durable storage. Selecting that mode does not replicate a local disk. Disk loss and a process crash are different failure models.

Keep snapshot history covering unfinished prepared operations, reader retention windows and useful checkpoints. The garbage collector protects retained snapshots, checkpoints and in-flight operations, and deletes only registered service-owned artifacts after a grace period. Index loss does not impair reads; startup restores a matching checkpoint or scans standard data and deletes, then atomically activates the rebuilt generation before restoring writes. Source lineage or unexplained external logical changes stop publication.

Current support boundaries:

- Unpartitioned Iceberg v2 with position deletes. Low-fan-out partitions and v3 follow the original specification's v2 correctness and performance gates.
- Mutable tables require a stable primary key and FULL replica identity. Keyless tables support append-only ingestion and equivalent external physical rewrites, including duplicate rows.
- Automatic DDL supports nullable column additions without a non-null backfill. Unsupported type, key, identity or source-lineage changes fail closed. TRUNCATE is rejected.
- Initial COPY uses up to four workers. Recovery needs capacity for one additional temporary replication slot. Transaction metadata and row payloads spill to disk; configured journal, spool, message and row-size budgets still apply.
- Catalog and object-store transient failures retry with durable prepared-operation recovery. Run the daemon under a supervisor for process failures and startup failures. Local fault tests do not establish independent-host durability.
- Compaction supports unsorted layouts. Z-order-aware compaction, distributed compaction protocols, HA and global autocompaction are outside the early release scope. External compactor reconciliation is included and tested with actual Spark maintenance.

## Verification and performance

Integration tests exercise real RocksDB/segment files, actual Parquet/Avro artifacts, the stock Apache Iceberg Rust reader, a scripted wire peer and child-process crashes. Tests include a catalog response lost after commit, multi-table ACK ordering, repeated updates and key changes, large transaction collapse, shared deletes, stale compaction, external rewrite equivalence and index reconstruction.

The local service suite additionally exercises initial COPY and WAL handoff, all supported value types, large streamed transactions, source reconnect, daemon crash recovery, compaction, historical reads, and unsupported-operation acknowledgement safety. A sustained variant runs three concurrent CRUD writers with both native compaction and an independent background maintenance process, including streamed savepoint rollback and a daemon crash while writes continue. Both suites read actual MinIO objects through DuckDB and audit native Avro manifests independently. No 24-hour benchmark has run. [Validation gates and local results](docs/validation.md) track the remaining conformance, performance and operational checks.

Performance choices include bounded journal group commit, concurrent compaction builds, bounded Arrow batches, disk-backed transaction maps and delete sorting, bulk RocksDB reads, batched atomic source-ledger updates, deferred intermediate index sync with a durable completion barrier, immutable manifest caching, bounded object I/O, size-tiered compaction and publication backpressure. Measured journal sync and compaction occupancy drove the latest changes. The qualification report records their evidence alongside unresolved catalog-latency and reader-overhead targets.

## License and provenance

Apache-2.0. Moonlink is a design reference only; its BSL implementation was not copied. The pinned Apache Iceberg source is vendored with documented transaction-extension, position-delete cache isolation, retry-classification and opt-in observability patches. Its released transaction extension boundary is private. Source and Iceberg capability audits are in [source references](docs/references-source.md), [Iceberg references](docs/references-iceberg.md), and [compaction references](docs/references-compaction.md). No upstream pull requests have been opened from this repository.
