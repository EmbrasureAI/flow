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
- `REPLICA IDENTITY DEFAULT` for primary-key tables. Out-of-line (TOAST)
  values an update leaves unchanged are carried forward from the key's earlier
  row image in the epoch or its indexed Iceberg row, so variable-width tables no
  longer need `FULL`. A primary-key change that leaves such a value unchanged
  blocks the table; keep `FULL` for tables whose keys change.
  A live table can switch between `DEFAULT` and `FULL` without blocking.
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

**Remote durability**

- Optional `[durability]` makes object storage (`s3://`, S3-compatible
  endpoints or `file://`) the authority for the journal, control state and
  source identity, so a worker and its whole state volume can be replaced
  without losing changes acknowledged to the source. Appends are written behind;
  catalog changes, object deletions, source acknowledgements and slot creation
  wait until the state they depend on is replicated and the owner's lease is
  valid. Row-index checkpoints are replicated too. See
  [remote durability](docs/remote-durability.md).
- A stale row-index checkpoint is advanced from Iceberg file changes instead of
  rebuilding every table, with or without `[durability]`.
- Opt-in `storage.medium = "memory"` for a `state_dir` on tmpfs or ramfs
  (Linux, with `[durability]`): startup verifies the filesystem, and free space
  is also bounded by the memory left under the process's cgroup v2 limits, so
  capture pauses before memory runs out. See
  [memory-backed state](docs/operations.md#memory-backed-state).
- Maintenance scans fetch small inputs whole, a bounded number ahead
  (`parquet_read.prefetch_files`, `parquet_read.prefetch_file_bytes`), and
  fresh publications reuse the catalog head and plan they already validated,
  removing multi-second maintenance stalls from publication latency.

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
  row index, the manifest cache and garbage reachability indexes.
- The parsed-manifest cache shared by publication and maintenance releases
  manifests unread for ten minutes and reloads them if read again, so rarely
  read manifests superseded by commits, compaction and manifest rewrites no
  longer accumulate until the cache reaches its 128 MiB budget.
- A Dockerfile that builds a minimal image running as an unprivileged user.
- Operations guidance for systemd and Kubernetes, and disk sizing.

**Known limitations**

- On PostgreSQL 16 and later, capture connection and reconnection wait while
  another transaction holds an `ACCESS EXCLUSIVE` lock on a published table
  (for example a long `TRUNCATE`, rewriting `ALTER`, `VACUUM FULL` or
  `CLUSTER`). Capture that is already streaming continues.

### Changed

**Performance**

- Capture keeps small PostgreSQL source transactions in a bounded memory buffer
  (256 KiB each, 32 MiB in total, so capture may use up to about 32 MiB more
  memory) instead of creating a spool directory and file per transaction.
  Larger transactions still spill to disk under `limits.spool_bytes`.
- Up to 256 small source transactions share one journal sync, still within the
  5 ms group window, and each journal frame is written with one call.
- UPDATE/DELETE keys are encoded without copying the table schema, and CDC and
  initial COPY rows no longer revalidate the relation's types row by row.
- The four concurrent initial COPY workers run on separate runtime threads.
- Periodic row-index checkpoints flush memtables before taking the index
  locks, and collapse folds source chunks outside the row lock, shortening
  stalls of concurrent publication.
- An epoch makes two REST catalog `loadTable` calls instead of three unless
  recovery just committed through the catalog, and build probes skip a
  redundant reload.
- Publication and maintenance share one 128 MiB manifest cache instead of two
  64 MiB caches, and snapshot manifest lists are cached too, removing duplicate
  manifest and manifest-list fetches per epoch. The
  `flow_memory_manifest_cache_bytes` and `flow_memory_manifest_cache_entries`
  gauges report one `cache="shared"` series instead of `publication` and
  `maintenance`.
- v3 epochs merge each touched data file's previous deletion vector in memory
  instead of restaging every prior position through the row index. Legacy
  position deletes and unusually large priors keep the disk-backed path.
- Deletion-vector reads parse each Puffin object's header and footer once and
  fetch vectors concurrently within the `[parquet_read]` prefetch bounds. Small
  objects go from four requests per vector to about one per object, with
  unchanged validation.
- Data builds reuse manifests already loaded for planning, and v3 delete
  consolidation no longer reads inputs or uploads a Puffin object for a rewrite
  that cannot reduce the object count.
- Manifest rewrites check and write their outputs concurrently, garbage
  collection history walks no longer evict current manifests from the cache,
  and data rewrites commit without reading back the plan they just wrote.
- Remote durability restores journal segments concurrently and streams journal
  parts and row-index files to disk, and the startup scan of retained journal
  bytes no longer blocks replication.
- The scheduler no longer rescans the operations column family on every loop
  iteration, and `status.json` updates are coalesced and written off the
  scheduler task.

**Remote durability metrics**

- `flow_durability_put_seconds` gains `kind="checkpoint"`, and
  `flow_durability_retries_total` also counts control snapshot, journal part and
  checkpoint writes; filter by `kind` for log-batch retries only.

### Fixed

- Reuse observed Puffin object sizes within a delete scan, avoiding repeated
  metadata requests for shared v3 deletion vectors while preserving validation.
- PostgreSQL date and timestamp arrays accept finite values beyond Chrono's
  calendar range whenever scalar decoding supports them, preserving the existing
  JSON string representation and rejection of infinite dates and timestamps.
- A streamed transaction could publish the changes of a savepoint it had
  rolled back. When PostgreSQL spills a transaction and later streams a
  savepoint with more than 4096 changes from the spill, it sends no abort for
  that savepoint's rollback. Capture now checks the status of each
  subtransaction with surviving changes in PostgreSQL's commit log at the
  commit and drops those that rolled back.
- jemalloc builds keep transparent huge pages off the allocator's memory
  (built-in `thp:never`). With the kernel set to `always`, huge pages made RSS
  keep growing past the memory jemalloc held; `MALLOC_CONF` overrides the
  default.
- A subtransaction abort for a transaction not streamed on the current
  replication connection no longer stops capture; it has nothing to discard,
  as in PostgreSQL's apply worker. Other unexpected stream aborts still stop
  capture, now naming the check that failed and the transaction IDs.
- Remote durability: checkpoint uploads (control snapshots, journal parts and
  manifests) retry transient object-storage errors. The startup checkpoint
  waits like a barrier and fails only once replication stops, so a brief S3
  error no longer stops the daemon at startup. A local error fails it after
  three attempts instead of waiting forever.
- Remote durability: a checkpoint that finished after a torn journal tail was
  repaired could reuse stale journal parts, corrupting later restores.
- Remote durability: S3 `409 ConditionalRequestConflict` responses and
  transient errors during startup writes are retried.
- Remote durability: `file://` prefixes create objects atomically through a
  `.staging/` directory under the prefix, so a failed write no longer leaves a
  partial object that stops replication. The filesystem must support hard
  links.
- Remote durability: shutdown waits about one lease at most while object
  storage is unavailable instead of hanging; the next start then waits one
  lease before taking over. Error exits first try, within that time, to
  replicate records written just before them, such as a resync marker.
- Remote durability: `queue_memory_bytes` also bounds queued journal extents,
  so an outage no longer grows replication memory without bound.
- Remote durability: row-index uploads require a fresh owner lease, and index
  replica deletions are counted in `flow_durability_deleted_objects_total`.
- Catching up a stale row-index checkpoint detects a delete that was removed or
  narrowed for a surviving data file, for example by an external writer, and
  falls back to a full rebuild instead of leaving the restored rows out of the
  index.
- A periodic maintenance visit that only recovered an unfinished ingest
  operation no longer disables periodic metadata maintenance, snapshot
  expiration and garbage collection for the rest of the process.
- Background build catch-up reads and charges each intermediate deletion vector
  once, avoiding spurious delete-budget rejections on chains of replaced
  vectors.
- Failing to delete a local compaction scratch directory is logged instead of
  stopping the daemon; the next start removes leftovers.
- [Remote durability](docs/remote-durability.md) lists the
  `AbortMultipartUpload` permission and the upload bandwidth that
  `request_timeout_secs` implies, and `examples/flow.toml` documents every
  `[compaction]` key with its default.

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
