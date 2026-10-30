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

### Upgrade

From earlier development builds of `main`:

- The row index moves to format 4, which names data files by ordinal. The
  first start rebuilds it from the control store and Iceberg, skipping
  row-index checkpoints of the older format; the journal, control store,
  source slot and Iceberg tables are kept. Allow for the rebuild's time and its
  copy of the index (see [disk sizing](docs/operations.md#index-verification-and-rebuild-space)).
- Earlier builds refuse to open the new index, which also has new
  `file_remaps` and `file_ordinals` column families (RocksDB reports
  `Column families not opened`), without changing it. To downgrade, restore
  the `state_dir` copied before the upgrade, or delete the row index so the
  earlier build rebuilds it from Iceberg.
- Data rewrites of keyed tables commit lazy index remaps, and delete-file count
  debt is paid by delete-only consolidation, by default. Set
  `[compaction] lazy_index_remap = false` or `delete_debt = "current"` for the
  earlier behavior. Small-file compaction also starts earlier
  (`l0_soft_files` and `stable_small_soft_files` default to 6 instead of 24).
- Remote durability writes log batches by size and age
  (`[durability] batch_interval_ms`, 250 ms) and at once when an external
  effect waits, instead of whenever `max_writes_per_second` allows; `0`
  restores the earlier writes. Source acknowledgements are released up to the
  interval later. `flow_durability_barrier_seconds` gains an `effect` label.

### Added

**Capture and publication**

- Initial COPY and PostgreSQL 14–18 logical replication into unpartitioned
  Iceberg v2 and v3 tables through an Iceberg REST catalog, with a durable
  journal, persistent row index, background compaction and reconciliation of
  external maintenance.
- Independent per-table publication, so a failing table does not stop the
  others. A table blocked until resync (`source_schema_incompatible` or
  `publication_changed`) has its changes settled without publication, so it
  stops holding the source acknowledgement and WAL. Another writer's row changes
  to a target block only that table (`external_mutation`); rolling them back
  within `limits.external_mutation_grace_secs` resumes it, and afterwards it
  requires a resync.
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
- Data rewrites of keyed tables, background and strict, commit one index
  remap per input file instead of rewriting every moved row's index entries,
  and reads translate through the remaps (`[compaction] lazy_index_remap`,
  default `true`; `false` restores per-row deltas). In service benchmarks the
  table's compaction stall fell by 70–96%, and commit-to-catalog p95 at 30,000
  mutations/s fell from 3.2 s to 1.2 s for the default mixed workload and from
  41.7 s to 5.8 s for Zipf-skewed updates. Periodic maintenance drops remaps
  nothing needs, and while a table keeps more than `remap_max_files` (512)
  remaps or a chain longer than `remap_max_depth` (8) hops, materializes the
  longest chain, at most `remap_cleanup_rows` (32,768) entries per visit,
  without the table fence. `flow_index_remap_files`,
  `flow_index_remap_max_depth`, `flow_index_remaps_dropped_total` and
  `flow_index_remap_rows_materialized_total` report it. See
  [local compaction](docs/local-compaction.md#lazy-index-remap).
- Delete-file count debt, soft and hard, is paid by the bounded delete-only
  consolidation instead of a data rewrite of a mostly-live base, and
  delete-driven data rewrites select only files past `deleted_rows_percent`
  (`[compaction] delete_debt`, default `"consolidate"`; `"current"` restores
  count-driven data rewrites). With 2M rows and Zipf-skewed updates at 30,000
  mutations/s, commit-to-catalog p95 fell from 6.8 s to 2.0 s and reads stayed
  within 1.25× of a compacted copy; the default mixed workload was neutral.
  Format v3 tables keep `"current"`: a data file has at most one deletion
  vector, so consolidation cannot lower its count. A
  `maintenance_action_chosen` debug event and a `selection_reason` field on
  build and rewrite events record why each action ran.
- Opt-in `[compaction] delete_layout = "per_target"` (default `"shared"`;
  format v3 tables always use `"shared"`) makes v2 delete-only consolidation
  and data-rewrite residuals write one position-delete file per target data
  file. Consolidation merges a target's files by size tier, so it reads about
  the new deletes instead of every accumulated one, and data rewrites of inputs
  with only single-target deletes write no residual. On the default mixed
  workload at 30,000 mutations/s for five minutes, commit-to-catalog p95 fell
  from 7.6 s to 2.5 s and compaction stall by two thirds. CDC epochs still
  write one shared delete file each. `delete_rewrite_completed`,
  `compaction_completed` and `compaction_catch_up_completed` report the
  layout, input kinds and residual rows.
- With `delete_layout = "per_target"`, the delete files readers open stay
  bounded under skew. Only full-size data files (outside L0, at least
  `[compaction] target_delete_min_file_bytes`, 128 MiB) keep one delete file
  each; positions on L0 and smaller files share range files. Once single-target
  files on full-size files reach `target_delete_files_max` (16) while one has
  more than one, consolidation merges the smallest until at most half remain,
  and smaller data files carrying single-target files are rewritten at
  `small_file_target_deletes` (8). A build probe or idle visit that
  consolidates also starts a background data rewrite while rewrite debt
  remains, so continuous CDC no longer starves builds. In a daemon test of
  Zipf CDC the mean delete-file count fell from 85 to 13 and the peak from 150
  to 23. New `flow_table_target_delete_files` and
  `flow_table_small_target_delete_files` gauges report both counts.

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
  capture pauses before memory runs out. On EBS gp3 at 30,000 to 60,000
  mutations/s it lowered CDC service time 2.6–6.0% and p95 commit-to-catalog
  latency 17–35% against unreplicated state on a dedicated volume. See
  [memory-backed state](docs/operations.md#memory-backed-state) and the
  [benchmark](docs/benchmarks/remote-durability.md#memory-backed-state).
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

- With remote durability, a publication's catalog commit waits only for the
  replicated state it was built from, up to its artifact registration, instead
  of the newest log record, and log batches are written by size and age or as
  soon as an external effect waits. On real S3 at 60,000 mutations/s,
  replication had cost 3.8% CDC service time, 3.6% drain and 63% p95
  commit-to-catalog latency, about 80 ms per commit; afterwards CDC service
  time and drain were unchanged within run-to-run noise at 30,000 and 60,000
  mutations/s, and log PUTs fell by about three quarters to about 4.5 a second
  ([report](docs/benchmarks/remote-durability.md#throughput-on-real-s3)).
- The row index names data files by per-table ordinal instead of path in
  primary-key values, reverse keys and staged deltas (index format 4), so
  lookups, staging, apply and RocksDB compaction no longer scale with path
  length. In the ingest microbenchmark one table rose from about 113,000 to
  158,000 mutations/s with default paths and from 83,000 to 157,000 with
  paths 155 characters longer
  ([report](docs/benchmarks/index-file-ordinals.md)).
- Index apply looks up and checks each chunk's rows on up to
  `min(available_parallelism, 8)` threads, and collapse binds folded keys
  against the index on up to four threads, decoding locations without
  allocating their paths. In the service benchmark CDC service time per
  mutation fell 6–8% from parallel apply and collapse time 52–58% from
  parallel bind.
- Data-rewrite residuals stream staged delete positions without decoding each
  record; the residual merge took about half as long in a local benchmark.
- `[compaction] l0_soft_files` and `stable_small_soft_files` default to 6
  instead of 24, so background compaction starts earlier; the hard limits stay
  at 32, so publication pauses at the same debt. Merge-on-read scan cost tracks
  file count, and at 30,000 mutations/s the median read slowdown against a
  compacted copy fell from 2.7× to 1.6× and commit-to-catalog p95 by 43%, for
  about 8% more CDC work per mutation.
- Capture keeps small PostgreSQL source transactions in a bounded memory buffer
  (256 KiB each, 32 MiB in total, so capture may use up to about 32 MiB more
  memory) instead of creating a spool directory and file per transaction.
  Larger transactions still spill to disk under `limits.spool_bytes`.
- Up to 256 small source transactions share one journal sync, still within the
  5 ms group window, and each journal frame is written with one call.
- UPDATE/DELETE keys are encoded without copying the table schema, and CDC and
  initial COPY rows no longer revalidate the relation's types row by row.
- The four concurrent initial COPY workers run on separate runtime threads.
- A busy table's next epoch is folded from the journal while the current
  epoch commits and applies, so its collapse only binds the fold against the
  index. The fold stays within `limits.collapse_memory_bytes`, is used only if
  the epoch begins with exactly the folded transactions, and is checked like a
  cold collapse. `limits.collapse_prefold = false` turns it off;
  `flow_collapse_prefolds_total` counts used and discarded folds.
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
- Delete-only consolidation merges its inputs' positions in memory (16 bytes
  each, within its one-million-position budget) and writes them from borrowed
  paths, instead of round-tripping every position through the scratch store.
  Merging eight files of about 800,000 positions with 120-byte paths fell from
  948 ms to 123 ms (shared layout) and from 1,251 ms to 154 ms (`per_target`)
  in a local release benchmark, with byte-identical outputs. Delete scans for
  data rewrites and catch-up resolve each run of positions on one path once.
- Data builds reuse manifests already loaded for planning, and v3 delete
  consolidation no longer reads inputs or uploads a Puffin object for a rewrite
  that cannot reduce the object count.
- Manifest rewrites check and write their outputs concurrently, garbage
  collection history walks no longer evict current manifests from the cache,
  and data rewrites commit without reading back the plan they just wrote.
- Remote durability restores journal segments concurrently and streams journal
  parts and row-index files to disk, and the startup scan of retained journal
  bytes no longer blocks replication.
- Remote durability checkpoints upload journal parts of at most 8 MiB instead
  of up to a whole 64 MiB segment, so the default `request_timeout_secs` needs
  about 2 MiB/s of upload bandwidth instead of 16 MiB/s. Consolidating a
  segment's small parts keeps its full-size parts instead of uploading the
  whole segment again.
- The scheduler no longer rescans the operations column family on every loop
  iteration, and `status.json` updates are coalesced and written off the
  scheduler task.
- Prepared data, delete and deletion-vector files, prepared plans and
  manifest-rewrite artifacts are created with a conditional create
  (`If-None-Match: *` on S3) instead of a HEAD before each write, which also
  closes the race with a concurrent writer. With 20 ms of added object-store
  latency, CDC service time per mutation fell 8% in the service benchmark.
- A row-index checkpoint syncs its files after releasing the row and control
  locks, so ingest and control transitions no longer wait for the sync. A
  pending registration protects its snapshots meanwhile.
- Remote durability keeps index checkpoint hashes in
  `checkpoints/replicated-hashes.json`, so the first checkpoint after a restart
  no longer hashes the whole row index again.
- Compacted files and the initial copy use Parquet row groups of up to
  `limits.parquet_row_group_rows` (default 122,880) instead of 8,192 rows. In
  the service benchmark full scans took 15–17% less time, while CDC service
  time per mutation rose about 3%, from compaction encoding larger groups
  alongside it. CDC publication files keep 8,192 rows, compaction keeps
  8,192-row batches, and position reads use the offset index to skip pages.
  Set the limit to 8192 for the previous layout.

**Remote durability metrics**

- `flow_durability_put_bytes_total{kind}` counts bytes written to object
  storage per object kind.
- `flow_durability_barrier_seconds` has an `effect` label (`catalog_commit`,
  `catalog`, `delete`, `replication_slot`, `acknowledgement`);
  acknowledgement samples measure from computing source progress to releasing
  it.
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
