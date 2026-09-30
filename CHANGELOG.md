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

Notes for deployments built from earlier commits of `main`. Existing state
directories and configurations keep working; no resynchronization is needed.

- **Snapshot history.** `limits.snapshot_expiration` now defaults to `true`.
  Expiration keeps at least `limits.snapshot_retain_last` (128) snapshots of
  `main`, honors an explicit table `history.expire.max-snapshot-age-ms`, and
  caps Flow-managed history at `limits.snapshot_max_count` (1,000). Set
  `snapshot_expiration = false` to keep every snapshot.
- **Garbage collection.** Unreferenced Flow-owned objects wait one
  `limits.orphan_grace_secs` from when collection first sees them unreferenced
  (previously a second full grace from creation). Superseded catalog metadata
  JSON waits `limits.metadata_json_grace_secs` (one hour by default) after it
  leaves the catalog's metadata log; raise it if readers keep metadata file
  locations. Registry records migrate lazily to a due queue.
- **Exit codes.** 78 = configuration invalid or incompatible with the state
  directory, 79 = resynchronization required, 75 = a source or catalog
  dependency stayed unavailable. `status` exits 3 when not ready. Use
  `RestartPreventExitStatus=78 79` under systemd.
- **Logging.** Fatal errors from `init`/`run` are a JSON `event:"fatal"` line.
  Per-transaction and per-epoch events are debug-level under the `flow_events`
  target; parsers need `RUST_LOG=info,flow_events=debug`.
- **Paths.** A relative `state_dir` or `catalog.tls_ca_file` resolves against
  the configuration file's directory, not the working directory.
- **Permissions.** Flow runs with umask 0077 and creates `state_dir` as 0700,
  tightening an existing one it owns. A textfile collector or `status` run as
  another user can no longer read it; use `[http]` instead. On shared volumes,
  put `state_dir` in a subdirectory.
- **PostgreSQL TLS.** `sslmode=verify-ca` now requires `sslrootcert`, as in
  libpq.
- **Catalog errors.** Rejected catalog credentials or permissions block tables
  with `catalog_auth` instead of `catalog_unavailable`; alerts should match
  both. Older binaries cannot read a durable `catalog_auth` block.
- **Journal recovery.** Corruption in a closed journal segment now stops
  startup instead of silently deleting later segments.
- **Index checks.** The full index checksum scan runs after an unclean exit,
  with `storage.verify_index_on_start = true`, or via `check --storage`, not on
  every start. The first start after upgrading scans once.
- **Probes.** `/healthz` returns 503 when the running main loop stalls for
  `[http] liveness_timeout_secs` (300 by default; 0 disables).

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
- HTTPS REST catalogs verified against the system trust store
  (`SSL_CERT_FILE`/`SSL_CERT_DIR`), plus `catalog.tls_ca_file` for private CAs.
- Per-table stall and lag metrics (`flow_table_blocked`,
  `flow_table_lag_seconds`, …), process gauges and example alert rules;
  `status.json` reports `state` and `last_error`.
- Capture reconnect, low-disk and maintenance-failure signals
  (`flow_capture_connected`, `flow_capture_disk_low`,
  `flow_table_maintenance_failing`, …).
- `[storage]` settings: capture pauses when free space on the state volume
  falls below `min_free_bytes` (4 GiB) and resumes automatically; `check`,
  `init` and `run` report the free-space budget. `check --storage` verifies
  every index checksum offline.
- Configurable spool limits `limits.spool_transactions` and
  `limits.spool_subtransactions` (default raised to 1,048,576).
- Operations guidance for systemd and Kubernetes, and disk sizing.

### Changed

- OAuth client-credential tokens are renewed before they expire and a 401/419
  is re-authenticated once, so tables no longer stay blocked after expiry.
- Startup retries transient catalog and PostgreSQL failures for up to ten
  minutes; invalid connection settings fail immediately.
- Publication epochs are sized by payload (32 MiB) and descriptor memory rather
  than `limits.pending_transactions` divided across busy tables, which bounds
  only queued lookahead now.
- Expiration and manifest rewriting run after publishing when a table exceeds
  its history or manifest limits, even under WAL pressure; maintenance failures
  back off per table instead of stopping the daemon.
- Garbage collection indexes retained manifests once instead of re-reading
  them on every page and bounds each page's deletions.
- New source columns are accepted unless their `ADD COLUMN` default fills
  existing rows or the table is rewritten; later `SET DEFAULT`, backfills and
  `SET NOT NULL` no longer block the table.
- Index revisions are synced before the control store records them, so a host
  crash no longer forces a full index rebuild in the common case.
- RocksDB open files are bounded and the soft `RLIMIT_NOFILE` is raised.
- The HTTP listener allows 64 connections and one second to send a request.

### Fixed

- A rolled-back streamed transaction containing `TRUNCATE`, an incompatible
  column change or an undecodable row no longer blocks its table.
- UPDATE/DELETE on an append-only table, a row larger than
  `limits.chunk_bytes`, a slot still held by a previous walsender (55006), too
  many connections (53300), `gc.enabled=false` targets and externally expired
  snapshots no longer crash-loop the whole daemon; the affected table is
  blocked or the operation retried.
- Catalog metadata JSON outside Flow's paths (`write.metadata.path`) is no
  longer claimed or deleted and no longer fails publication.
- A full disk no longer stops the service when writing `status.json` or
  `metrics.prom`.
- Compaction catch-up replans instead of stopping the daemon when an external
  rewrite removes a v3 deletion vector's target data file.
- Iceberg v3 row-lineage rewrites of binary columns are fixed.
- Snapshot keepers survive `idle_in_transaction_session_timeout` and
  `transaction_timeout`; heartbeats commit with `synchronous_commit = local`.

### Security

- Credentials are kept out of catalog and configuration errors.
- Secret values are redacted from debug output of catalog, storage and HTTP
  client configuration, and credential-provider logs are capped at INFO.
- Startup and `check --source` warn about unauthenticated PostgreSQL TLS;
  Flow warns when catalog credentials would travel over plain `http://`.
- `state_dir` is owner-only; Flow refuses a group- or world-writable
  `state_dir` owned by another non-root user.
- rustls updated for RUSTSEC-2026-0285; a scheduled dependency audit and
  Dependabot run in CI; workflow actions are pinned by commit.
- Release re-runs no longer overwrite published images or assets unless
  `replace_assets` is set.

[Unreleased]: https://github.com/EmbrasureAI/flow/commits/main
