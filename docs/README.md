# Documentation

## Using Embrasure Flow

- [Getting started](getting-started.md): build, configuration, initialization, recovery and support boundaries.
- [PostgreSQL type mappings](postgres-types.md): supported types, JSON semantics and schema changes.
- [Docker demo](../demo/README.md): a complete local stack with standard Trino reads.
- [Observability](observability.md): status, watermarks, metrics and operational diagnosis.
- [Operations](operations.md): source preflight, resynchronization, adding tables, planned maintenance and capacity.
- [Iceberg v3](iceberg-v3.md): deletion vectors, upgrades and compatibility checks.
- [Performance](performance.md): recorded throughput, publication latency and reader overhead.

## Development

- [Contributing](../CONTRIBUTING.md): development setup, tests and dependency changes.
- [Code guide](code-guide.md): crate ownership, module layout and test locations.
- [Architecture](architecture.md): transaction identity, durable state and publication.
- [Table publication isolation](table-publication-isolation.md): independent admission, retry, and durable recovery.
- [Compaction protocol](local-compaction.md): background builds, catch-up and activation.
- [Local tests](../tests/local/README.md) and [service integration](../tests/production/README.md).
- [V2/v3 benchmark](benchmarks/v3-deletion-vectors.md): workload, results, limitations and reproduction.

## Dependencies and references

- [Licenses and distribution notices](../licenses/README.md).
- Implementation contracts: [PostgreSQL and source capture](references-source.md),
  [Iceberg](references-iceberg.md), and [compaction](references-compaction.md).
- [Prepared upstream patches](patches/upstream/README.md).
