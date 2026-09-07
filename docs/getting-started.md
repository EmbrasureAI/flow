# Getting started

For a complete local stack, follow the [Docker demo](../demo/README.md).
The steps below run the native binary against an existing PostgreSQL database,
Iceberg REST catalog and S3-compatible object store.

## Build

Use the pinned Rust toolchain and install a C++ compiler, libclang, CMake,
pkg-config and OpenSSL development headers. On macOS, Xcode Command Line Tools
supply the compiler and libclang.

```sh
cargo build --locked --release -p flow-daemon
cargo run --locked -p flow-daemon -- --config examples/flow.toml check
```

## Configuration

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

`status` reads an atomic status file without locking the index. It reports exact source watermarks, readiness, process identity and freshness while running or stopped. `state_dir/metrics.prom` supports a Prometheus textfile collector. See [observability](observability.md) for latency definitions, reader debt and the distinction between SDK operations and billed requests.

## Recovery and operational limits

`materialized` acknowledgement is the default. `journaled` mode requires an explicit declaration of independently durable storage. Selecting that mode does not replicate a local disk. Disk loss and a process crash are different failure models.

Keep snapshot history covering unfinished prepared operations, reader retention windows and useful checkpoints. The garbage collector protects retained snapshots, checkpoints and in-flight operations, and deletes only registered service-owned artifacts after a grace period. Index loss does not impair reads; startup restores a matching checkpoint or scans standard data and deletes, then atomically activates the rebuilt generation before restoring writes. Source lineage or unexplained external logical changes stop publication.

Automatic snapshot expiration defaults to disabled. Set `limits.snapshot_expiration = true` only when external branch/tag creation and retention-policy changes are excluded or coordinated with this service's expiration. Coordinate catalog retention-policy changes with ingestion and maintenance commits too: a policy-only update does not move a snapshot head. Standard Iceberg REST cannot atomically assert the complete reference set and its policies; assertions on existing reference heads alone do not cover newly created tags. With expiration disabled, retained history and its files accumulate; arrange catalog-side coordinated expiration if automatic cleanup is required. Data compaction, manifest rewriting and collection of unreferenced owned artifacts still run.

Current support boundaries:

- Unpartitioned Iceberg v2 position deletes and v3 deletion vectors. Set `format_version = 3` on a table to create a v3 target; see [v3 configuration and compatibility](iceberg-v3.md). Partitioning remains planned work.
- Mutable tables require a stable primary key and FULL replica identity. Keyless tables support append-only ingestion and equivalent external physical rewrites, including duplicate rows.
- Automatic DDL supports nullable column additions without a non-null backfill. Unsupported type, key, identity or source-lineage changes fail closed. TRUNCATE is rejected.
- Initial COPY uses up to four workers. Recovery needs capacity for one additional temporary replication slot. Transaction metadata and row payloads spill to disk; configured journal, spool, message and row-size budgets still apply.
- Catalog and object-store transient failures retry with durable prepared-operation recovery. Run the daemon under a supervisor for process failures and startup failures. Local fault tests do not establish independent-host durability.
- Compaction supports unsorted layouts. Z-order-aware compaction, distributed compaction protocols, HA and global autocompaction are outside the early release scope. External compactor reconciliation is included and tested with actual Spark maintenance.

Keep the source publication's table membership, operation flags, column lists and row filters fixed from initialization through streaming. Coordinate changes by stopping capture and resynchronizing the affected source before resuming. PostgreSQL may silently omit changes under an altered publication, so restoring its settings or restarting the service cannot prove that no rows were missed.
