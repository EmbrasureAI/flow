# Changelog

Notable changes to Embrasure Flow. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow
[Semantic Versioning](https://semver.org/) as described in
[upgrading](docs/upgrading.md): while Flow is 0.x, a minor release may change
configuration or on-disk state, and its entry states the upgrade path.

Each release entry lists **Upgrade** notes first when an upgrade needs action.
No release has been tagged yet; the first will be `0.1.0`.

## [Unreleased]

This will be the initial public release, `0.1.0`.

### Added

**Capture and publication**

- Initial COPY and PostgreSQL 14–18 logical replication into unpartitioned
  Iceberg v2 and v3 tables through an Iceberg REST catalog, with a durable
  journal, persistent row index, background compaction and reconciliation of
  external maintenance.
- Independent per-table publication, so a failing table does not stop the
  others.
- `REPLICA IDENTITY DEFAULT` for primary-key tables whose replicated columns
  are all fixed-width.
- Administrator-managed publications, including `FOR ALL TABLES` and
  `FOR TABLES IN SCHEMA`; Flow ignores tables it does not capture. A contract
  change to one configured table blocks only that table.
- New source columns are accepted unless their `ADD COLUMN` default fills
  existing rows or the table is rewritten.
- PostgreSQL type mappings, explicit column selection and pgvector.
- Durable heartbeats that advance idle sources' replication slots.
- A full ingress journal pauses capture until publication drains
  (`flow_capture_journal_full`) instead of stopping the daemon.
- Publication epochs are sized by payload and descriptor memory; the optional
  `limits.epoch_max_transactions` also caps source transactions per epoch.
- Configurable spool limits `limits.spool_transactions` and
  `limits.spool_subtransactions`.

**Iceberg v3**

- `format_version = 3` in a `[[tables]]` block creates a v3 target (v2 remains
  the default). Position deletes are written as Puffin deletion vectors,
  compaction preserves row lineage (`_row_id`,
  `_last_updated_sequence_number`), and tables upgraded to v3 through the
  catalog are handled. See [Iceberg v3](docs/iceberg-v3.md).

**Catalogs**

- HTTPS REST catalogs verified against the system trust store
  (`SSL_CERT_FILE`/`SSL_CERT_DIR`), plus `catalog.tls_ca_file` for private CAs.
- OAuth client-credential tokens are renewed before they expire, and a 401 or
  419 response is re-authenticated once.
- Rejected catalog credentials or permissions block the affected tables with
  the `catalog_auth` code, distinct from `catalog_unavailable`.

**Table maintenance**

- Snapshot expiration is on by default (`limits.snapshot_expiration`). It keeps
  at least `limits.snapshot_retain_last` (128) snapshots of `main`, honors an
  explicit table `history.expire.max-snapshot-age-ms`, and caps Flow-managed
  history at `limits.snapshot_max_count` (1,000). Set
  `snapshot_expiration = false` to keep every snapshot.
- Manifest rewriting, and collection of Flow-owned files including catalog
  metadata JSON. Unreferenced objects wait `limits.orphan_grace_secs` (24
  hours) from when collection first sees them unreferenced; superseded
  metadata JSON waits `limits.metadata_json_grace_secs` (one hour by default)
  after leaving the catalog's metadata log. Catalog metadata JSON written
  outside Flow's paths (`write.metadata.path`) is never collected.
- `embrasure-flow metadata-import` adopts existing catalog metadata JSON into
  grace-delayed collection.
- Maintenance failures back off per table instead of stopping the daemon.

**Commands and configuration**

- `embrasure-flow discover` prints `[[tables]]` configuration for the
  publication's tables, named tables or a schema, using the same type mapping
  and validation as `init`.
- `embrasure-flow check --source` runs read-only PostgreSQL readiness checks;
  `check --storage` verifies every index checksum offline. The full checksum
  scan also runs at startup after an unclean exit or with
  `storage.verify_index_on_start = true`.
- A relative `state_dir` or `catalog.tls_ca_file` resolves against the
  configuration file's directory, not the working directory.
- `[storage]` settings: capture pauses when free space on the state volume
  falls below `min_free_bytes` (4 GiB) and resumes automatically; `check`,
  `init` and `run` report the free-space budget.
- Startup retries transient catalog and PostgreSQL failures for up to ten
  minutes; invalid connection settings fail immediately.
- PostgreSQL TLS modes and configured CA bundles; `sslmode=verify-ca` requires
  `sslrootcert`, as in libpq.

**Operations and observability**

- Exit codes for supervisors: 78 = configuration invalid or incompatible with
  the state directory, 79 = resynchronization required, 75 = a source or
  catalog dependency stayed unavailable. `status` exits 3 when not ready. Use
  `RestartPreventExitStatus=78 79` under systemd.
- Fatal errors from `init` and `run` are logged as a JSON `event:"fatal"`
  line. Per-transaction and per-epoch events are debug-level under the
  `flow_events` target (`RUST_LOG=info,flow_events=debug`).
- Optional `[http]` listener serving `/healthz`, `/readyz` and `/metrics`.
  `/healthz` returns 503 when the running main loop stalls for
  `liveness_timeout_secs` (300 by default; 0 disables).
- Per-table stall and lag metrics (`flow_table_blocked`,
  `flow_table_lag_seconds`, …), capture, disk and maintenance signals
  (`flow_capture_connected`, `flow_capture_disk_low`,
  `flow_table_maintenance_failing`, …), process gauges and example alert
  rules; `status.json` reports `state` and `last_error`.
- Optional jemalloc allocator on GNU/Linux (`--features jemalloc`).
- Memory-budget gauges (`flow_memory_index_block_cache_bytes`,
  `flow_memory_index_memtable_bytes`, `flow_memory_manifest_cache_bytes`,
  `flow_memory_retained_index_bytes`, …) that attribute resident memory to the
  row index, manifest caches and garbage reachability indexes.
- A Dockerfile that builds a minimal image running as an unprivileged user.
- Operations guidance for systemd and Kubernetes, and disk sizing.

**Known limitations**

- On PostgreSQL 16 and later, capture connection and reconnection wait while
  another transaction holds an `ACCESS EXCLUSIVE` lock on a published table
  (for example a long `TRUNCATE`, rewriting `ALTER`, `VACUUM FULL` or
  `CLUSTER`). Capture that is already streaming continues.

### Security

- Credentials are kept out of catalog and configuration errors, and catalog
  response bodies are omitted from diagnostics.
- Secret values are redacted from debug output of catalog, storage and HTTP
  client configuration, and credential-provider logs are capped at INFO.
- Startup and `check --source` warn about unauthenticated PostgreSQL TLS;
  Flow warns when catalog credentials would travel over plain `http://`.
- Flow runs with umask 0077 and keeps `state_dir` owner-only (0700), tightening
  an existing one it owns, and refuses a group- or world-writable `state_dir`
  owned by another non-root user. Read status from another user through
  `[http]`; on shared volumes, put `state_dir` in a subdirectory.
- A scheduled dependency audit and Dependabot run in CI, and workflow actions
  are pinned by commit.

[Unreleased]: https://github.com/EmbrasureAI/flow/commits/main
