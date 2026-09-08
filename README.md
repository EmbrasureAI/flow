# Embrasure Flow

Stream PostgreSQL changes into standard Apache Iceberg tables, using Rust.
Inserts, updates and deletes publish as ordinary Parquet data with v2 position
deletes or v3 deletion vectors. Compatible Iceberg readers query the tables directly.

**Development status:** correctness and recovery have passing local integration
checks. Throughput, read overhead and endurance qualification are still in
progress. This is not yet a qualified production release. See the
[measured results and remaining targets](docs/performance.md).

## Get started

The [Docker demo](demo/README.md) includes PostgreSQL, MinIO, an Iceberg REST
catalog, Trino and the ingestion service:

```sh
docker compose -f demo/compose.yaml up --build -d
```

To build the native binary, install the pinned Rust toolchain, a C++ compiler,
libclang, CMake, pkg-config and OpenSSL development headers:

```sh
cargo build --locked --release -p flow-daemon
cargo run --locked -p flow-daemon -- --config examples/flow.toml check
```

Follow [Getting started](docs/getting-started.md) to configure a source and run
`init`, `run` and `status` against your own services.

## How it works

```text
PostgreSQL WAL → durable transaction journal → per-table materialization
                                             ↓
                              Parquet data + deletes
                                             ↓
                                   Iceberg catalog commit
                                             ↓
                              durable row index → source ACK
```

A source transaction becomes visible atomically within each destination table.
A source-wide ledger advances acknowledgement only through completed transactions.
The journal and RocksDB index support writes and recovery; readers need only the
Iceberg catalog and object storage.

Local compaction runs alongside ingestion. Before publishing a rewrite, the
coordinator validates its inputs and translates intervening deletes. External
physical rewrites are reconciled before ingestion reuses row locations.

## Current support

- PostgreSQL initial COPY and logical replication, with transaction replay,
  reconnects and interrupted-bootstrap recovery.
- Unpartitioned Iceberg v2 position deletes or v3 deletion vectors, an Iceberg REST catalog and S3-compatible storage.
- Inserts, updates, deletes and primary-key changes; append-only keyless tables.
- Nullable column additions, durable publication recovery, checkpoints and index rebuilds.
- Local data/delete compaction, external compactor reconciliation and protected cleanup.

Mutable tables require a stable primary key and `REPLICA IDENTITY FULL`.
TRUNCATE and incompatible schema changes stop capture. Cross-table query
atomicity, HA, distributed compaction and Z-order compaction are not supported.
Partitioning remains planned work. See [v3 configuration](docs/iceberg-v3.md) and the full
[operating limits](docs/getting-started.md#recovery-and-operational-limits).

## Documentation and contributions

- [Documentation index](docs/README.md)
- [Architecture](docs/architecture.md) and [compaction protocol](docs/local-compaction.md)
- [Configuration example](examples/flow.toml) and [observability](docs/observability.md)
- [Integration tests](tests/production/README.md) and [performance results](docs/performance.md)
- [Contributing](CONTRIBUTING.md) and [code guide](docs/code-guide.md)

## License

Embrasure Flow is licensed under [Apache-2.0](LICENSE). Vendored dependencies
retain their upstream licenses and [attribution notices](NOTICE).
[Distribution notices](licenses/README.md) explains the generated binary license
bundle and the few upstream texts missing from published dependency packages.
