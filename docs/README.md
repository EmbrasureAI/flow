# Documentation

## Using Embrasure Flow

- [Getting started](getting-started.md): build, configuration, initialization, recovery and support boundaries.
- [Docker demo](../demo/README.md): a complete local stack with standard Trino reads.
- [Observability](observability.md): status, watermarks, metrics and operational diagnosis.
- [Performance](performance.md): recorded throughput, publication latency and reader overhead.

## Development

- [Contributing](../CONTRIBUTING.md): development setup, tests and dependency changes.
- [Architecture](architecture.md): transaction identity, durable state and publication.
- [Compaction protocol](local-compaction.md): background builds, catch-up and activation.
- [Local tests](../tests/local/README.md) and [service integration](../tests/production/README.md).
- [Design and qualification records](development/README.md): acceptance requirements, test evidence and remaining work.

## Dependencies and references

- [Licenses and distribution notices](../licenses/README.md).
- Reference reviews: [PostgreSQL and source capture](references-source.md),
  [Iceberg](references-iceberg.md), and [compaction](references-compaction.md).
- [Prepared upstream patches](patches/upstream/README.md).
