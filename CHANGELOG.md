# Changelog

Notable changes to Embrasure Flow. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow
[Semantic Versioning](https://semver.org/) as described in
[upgrading](docs/upgrading.md): while Flow is 0.x, a minor release may change
configuration or on-disk state, and its entry states the upgrade path.

Each release entry lists **Upgrade** notes first when an upgrade needs action.
No release has been tagged yet; the first will be `0.1.0`.

## [Unreleased]

### Upgrade

Notes for deployments built from earlier commits of `main`:

- `limits.snapshot_expiration` now defaults to `true`. A configuration that
  omitted it previously kept every snapshot; it now expires snapshots older
  than `limits.snapshot_retention_secs` (one hour) once a table has at least
  128, and garbage collection later deletes files no retained snapshot uses.
  Set `snapshot_expiration = false` to keep the previous behavior.
- Existing state directories are used as is; no resynchronization is needed.

### Added

- Initial COPY and PostgreSQL 14–18 logical replication into unpartitioned
  Iceberg v2 and v3 tables through an Iceberg REST catalog, with a durable
  journal, persistent row index, background compaction and reconciliation of
  external maintenance.
- `REPLICA IDENTITY DEFAULT` for primary-key tables whose replicated columns
  are all fixed-width.
- Administrator-managed publications, including `FOR ALL TABLES` and
  `FOR TABLES IN SCHEMA`; Flow ignores tables it does not capture. A contract
  change to one configured table blocks only that table.
- PostgreSQL TLS modes and configured CA bundles.
- PostgreSQL type mappings, explicit column selection and pgvector.
- Independent per-table publication, so a failing table does not stop the others.
- Durable heartbeats that advance idle sources' replication slots.
- Snapshot expiration, manifest rewriting and collection of Flow-owned files,
  including catalog metadata JSON.
- `embrasure-flow discover` prints `[[tables]]` configuration for the
  publication's tables, named tables or a schema, using the same type mapping
  and validation as `init`.
- `embrasure-flow check --source` runs read-only PostgreSQL readiness checks.
- Optional `[http]` listener serving `/healthz`, `/readyz` and `/metrics`.
- A full ingress journal pauses capture until publication drains
  (`flow_capture_journal_full`) instead of stopping the daemon.
- Optional jemalloc allocator on GNU/Linux (`--features jemalloc`).
- Release workflow publishing Linux x86-64 and arm64 binaries and a
  multi-architecture image at `ghcr.io/embrasureai/flow`.

### Security

- Credentials are kept out of catalog and configuration errors.

[Unreleased]: https://github.com/EmbrasureAI/flow/commits/main
