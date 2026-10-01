<p align="center">
  <a href="https://embrasure.ai">
    <img src="docs/assets/embrasure-banner.svg" alt="Embrasure Flow" width="100%">
  </a>
</p>

<h1 align="center">Embrasure Flow</h1>

<p align="center">
  Stream PostgreSQL changes into Apache Iceberg. Written in Rust.
</p>

<p align="center"><strong>Status: beta.</strong> See <a href="#support-and-status">support and status</a>.</p>

<p align="center">
  <a href="https://github.com/EmbrasureAI/flow/actions/workflows/ci.yml"><img src="https://github.com/EmbrasureAI/flow/actions/workflows/ci.yml/badge.svg?branch=main" alt="Rust CI"></a>
  <a href="https://github.com/EmbrasureAI/flow/actions/workflows/services.yml"><img src="https://github.com/EmbrasureAI/flow/actions/workflows/services.yml/badge.svg?branch=main" alt="Service integration"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-Apache--2.0-2C2721" alt="License: Apache-2.0"></a>
</p>

<p align="center">
  <a href="docs/README.md">Documentation</a> ·
  <a href="demo/README.md">Quickstart</a> ·
  <a href="docs/performance.md">Benchmarks</a> ·
  <a href="CONTRIBUTING.md">Contributing</a> ·
  <a href="https://github.com/EmbrasureAI/flow/issues">Issues</a>
</p>

Flow copies existing PostgreSQL rows, then continuously replicates inserts,
updates and deletes into standard Iceberg tables. Query the tables directly
through a compatible Iceberg reader, using your catalog and object storage.

## Highlights

- **Postgres to Iceberg in one service.** Initial COPY and ongoing logical
  replication, with background compaction in the same Rust process.
- **Standard tables.** Parquet data with Iceberg v2 position deletes or v3 deletion
  vectors, published through an Iceberg REST catalog to S3-compatible storage.
- **Mutable data.** Inserts, updates, deletes and primary-key changes, plus
  append-only ingestion for tables without a primary key.
- **Durable recovery.** A transaction journal, persistent row index and publication
  ledger support replay, reconnects and interrupted-bootstrap recovery.
- **Maintenance alongside ingestion.** Local data/delete compaction and
  reconciliation of external compactor rewrites.

## Quickstart

Run the local demo with Git, Docker and Docker Compose. It includes PostgreSQL,
MinIO, an Iceberg REST catalog, Trino and Flow:

```sh
git clone https://github.com/EmbrasureAI/flow.git
cd flow
docker compose -f demo/compose.yaml up --build -d
```

Once `initialize` has completed and `flow` has started, query the replicated table:

```sh
docker compose -f demo/compose.yaml exec trino trino \
  --execute 'SELECT * FROM lake.replicated.orders'
```

The [demo guide](demo/README.md) walks through changing source rows, checking
service status and cleaning up. The stack uses named volumes and does not
publish ports on the host.

### Build from source

Install the [pinned Rust toolchain](rust-toolchain.toml), a C++ compiler, libclang,
CMake, pkg-config and OpenSSL development headers, then build and validate the
example configuration:

```sh
cargo build --locked --release -p flow-daemon
./target/release/embrasure-flow --config examples/flow.toml check
```

On GNU/Linux, add `--features jemalloc` to enable process-wide allocation and
background reclamation of unused pages, including RocksDB's C++ allocations.
For a container image, pass `--build-arg FLOW_FEATURES=jemalloc` to `docker build`.
The default build uses the system allocator; the feature has no effect on other
targets. See [allocator metrics](docs/observability.md#allocator-memory) for
measurement details and limits.

### Build a container image

The [Dockerfile](Dockerfile) builds a minimal image from this checkout:

```sh
docker build -t embrasure-flow .
docker run --rm --env-file flow.env \
  -v "$PWD/flow.toml:/etc/flow.toml:ro" -v flow-state:/data \
  embrasure-flow --config /etc/flow.toml init
docker run --env-file flow.env \
  -v "$PWD/flow.toml:/etc/flow.toml:ro" -v flow-state:/data \
  embrasure-flow
```

The image runs `embrasure-flow --config /etc/flow.toml run` by default as an
unprivileged user. Mount the configuration at `/etc/flow.toml`, set
`state_dir = "/data"` in it and mount a persistent volume there, and pass the
environment variables your configuration names for credentials (here through
`flow.env`).

### Connect your own database

Follow [Getting started](docs/getting-started.md). In short:

```sh
embrasure-flow --config flow.toml check --source   # read-only PostgreSQL preflight
embrasure-flow --config flow.toml discover >> flow.toml   # generate [[tables]]
embrasure-flow --config flow.toml init             # slot + initial copy
embrasure-flow --config flow.toml run              # stream changes
```

Flow needs persistent disk for its transaction journal and RocksDB row index.
Readers access Iceberg independently of the running service.

## How it works

```mermaid
flowchart LR
    Postgres[PostgreSQL] -->|COPY + logical replication| Flow
    Flow -->|Parquet data + deletes| Iceberg[Apache Iceberg]
    Iceberg --> Readers[Compatible query engines]
```

Flow journals source transactions, materializes data and deletes, and commits
them to the Iceberg catalog. A transaction becomes visible atomically within
each destination table. A source-wide ledger advances acknowledgement only
through completed transactions.

Compaction runs alongside ingestion. Before publishing a rewrite, the
coordinator validates its inputs and translates intervening deletes. External
physical rewrites are reconciled before ingestion reuses row locations.
See the [architecture](docs/architecture.md) and
[compaction protocol](docs/local-compaction.md) for the durable state transitions.

## Support and status

**Flow is beta software.** Every change runs service tests against real
PostgreSQL, MinIO and an Iceberg REST catalog, reading results with stock DuckDB.
On each of PostgreSQL 14–18 they cover snapshot/CDC type compatibility, schema
changes, SIGKILL and catalog/object-store/PostgreSQL outage recovery, and the
fail-closed checks for a lost or rewound slot or journal. Table isolation,
REPLICA IDENTITY DEFAULT, lifecycle and compaction scenarios run on PostgreSQL
14 and 18; Iceberg v3, a randomized crash loop and the Trino and Spark
external-maintenance checks run on PostgreSQL 18, with a longer crash loop and
a resource soak nightly. Power loss (unsynced page cache) is not simulated.
See [the fault suite](tests/production/FAULTS.md). Performance qualification is ongoing; the
[benchmark report](docs/performance.md) records measured throughput, latency
and the targets still open. Configuration and on-disk state may change between
minor releases, with the upgrade path stated in the [changelog](CHANGELOG.md);
see [upgrading](docs/upgrading.md).

Supported today:

- PostgreSQL 14–18 sources, with inserts, updates, deletes and primary-key
  changes. Mutable tables need a primary key and either `REPLICA IDENTITY FULL`
  or, when every replicated column is fixed-width, `DEFAULT`; see
  [replica identity](docs/getting-started.md#replica-identity). Tables without
  a primary key replicate append-only.
- Unpartitioned Iceberg v2 (position deletes) and v3 (deletion vectors) tables
  through an Iceberg REST catalog on S3-compatible storage.
- The [type mappings](docs/postgres-types.md); JSON and JSONB replicate as
  normalized JSON text.
- Automatic schema evolution for nullable column additions and compatible
  required-to-nullable changes. `TRUNCATE` and other incompatible changes block
  the affected table while healthy tables continue; see
  [table isolation](docs/table-publication-isolation.md) and
  [operations](docs/operations.md).

Not yet supported: partitioned source tables or Iceberg partition specs,
per-table re-snapshots without resynchronizing the source, cross-table query
atomicity, high availability, distributed compaction and Z-order compaction.
Check [v3 reader compatibility](docs/iceberg-v3.md) and the
[operating limits](docs/getting-started.md#recovery-and-operational-limits)
before deploying.

## Documentation

| Guide | What you will find |
| --- | --- |
| [Getting started](docs/getting-started.md) | Build, configure, initialize and run Flow |
| [Configuration](examples/flow.toml) | Source, storage, catalog and compaction settings |
| [PostgreSQL type mappings](docs/postgres-types.md) | Supported application types across snapshot and CDC |
| [Operations](docs/operations.md) | Preflight, resynchronization, adding tables and planned maintenance |
| [Observability](docs/observability.md) | Status, HTTP probes, watermarks, metrics and diagnosis |
| [Upgrading](docs/upgrading.md) | Release compatibility and the upgrade procedure |
| [Iceberg v3](docs/iceberg-v3.md) | Deletion vectors, upgrades and reader compatibility |
| [Code guide](docs/code-guide.md) | Crate responsibilities and module layout |
| [Integration tests](tests/production/README.md) | Service fixtures, reader checks and recovery scenarios |

Browse the [documentation index](docs/README.md) for the full set of guides.

## Contributing

Bug reports, documentation improvements and code contributions are welcome.
Read [Contributing](CONTRIBUTING.md) for development setup, checks and review
expectations. Open an [issue](https://github.com/EmbrasureAI/flow/issues) to discuss
substantial changes or report a bug with reproduction steps. Participation is
governed by the [code of conduct](CODE_OF_CONDUCT.md). Report security issues
privately as described in [SECURITY.md](SECURITY.md), not in public issues.

## License

Flow is licensed under [Apache-2.0](LICENSE). Vendored dependencies retain their
upstream licenses and [attribution notices](NOTICE). See
[distribution notices](licenses/README.md) for the binary license bundle.
