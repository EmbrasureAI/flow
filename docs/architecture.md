# Architecture decisions

## Public read contract

The current Iceberg snapshot is the entire public read plane. A standard reader requires only the catalog and the user's objects. L0, L1 and L2 are maintenance classifications of ordinary Parquet files. The write-side index is neither a reader dependency nor an alternative hot-table format.

## Transaction identity and acknowledgement

A source ID names a database/slot incarnation, not merely a hostname. Runtime preflight verifies source lineage. Transaction identity includes the source, xid and commit/end LSN. PostgreSQL's end LSN is used for acknowledgement; the server's reported WAL end is never a received or durable watermark.

Complete source transactions are journaled before dispatch. The source ledger persists each table's publication and advances only through its complete ordered prefix. A table's progress is durable only after its index transition is complete. Restart repairs the gap between that index transition and source-ledger completion.

Materialized ACK is the default for local storage. Journaled ACK is an explicit operational contract with independently durable storage. No configuration flag can make a single local volume replicated.

## Initial snapshot and concurrent capture

The first table copies share the permanent slot's exported snapshot. A SQL transaction imports and re-exports that snapshot before the replication connection closes, allowing ordinary CDC capture to start while up to four COPY workers use the shared view. The keeper holds table locks that permit DML and prevent concurrent schema changes during COPY. PostgreSQL documents the lifetime of [replication snapshots](https://www.postgresql.org/docs/18/protocol-replication.html) and [SQL-exported snapshots](https://www.postgresql.org/docs/18/functions-admin.html#FUNCTIONS-SNAPSHOT-SYNCHRONIZATION).

Each table's cut, schema version, staging path and phase are durable control records. A completed staging journal ends with a synced terminal record before the table becomes eligible for publication. An empty source-wide bootstrap transaction fences acknowledgement until every table's base is committed and indexed; CDC capture and durable ledger registration continue behind that fence.

After an interrupted COPY, completed staging and catalog publications are recovered first. Only unfinished tables are recopied using a new temporary slot's snapshot. Those tables record their newer cuts before COPY and suppress CDC already covered by their bases; completed tables retain their original cuts. The permanent slot and source incarnation never change. A temporary slot belongs to its snapshot attempt and disappears when that connection closes. Target UUID, source lineage and schema checks still apply on every resume.

## Durable file and index boundary

A logical epoch ID is deterministic. A physical attempt has a fresh suffix, so retrying a failed build never overwrites an object that may still be referenced. A prepared plan is an ordinary Iceberg manifest list and Avro manifests, plus small internal validation context.

The commit protocol is build → seal prepared state → validate and catalog CAS → persist catalog proof → apply index batches → complete source ledger. Every index batch updates both directions, per-file live-row counts and the applied cursor atomically. Until its last batch, the table fence stays set. Catalog failure is ambiguous: recovery searches the operation marker on retained current ancestry before replaying anything.

Coordinator commits require the exact snapshot represented by their index. Even an insert-only publication must not advance past an external rewrite without reconciling unchanged rows' physical locations. A definitively rejected attempt can be discarded only after complete retained ancestry proves its base was superseded and its operation never committed. The table then reconciles and collapses the original journal transactions again, with fresh physical output paths. Ambiguous responses retain their durable prepared state. Retries are bounded and keep the table fenced from other local work.

The source ledger has one owner. Its complete SOURCE-only write batches use the control commit mutex without waiting for the row/spool mutation mutex. Row, table and operation read-modify-write paths retain that mutex. The control commit lock serializes both database revisions and fences a failed durable transition before another write can enter; checkpoints acquire the row lock followed by the control lock.

The durable control store owns bootstrap and source-ledger records, table watermarks, and pending operation headers, artifacts and prepared payloads. Row locations, inverse mappings, per-file live-row counts, mutation spools and apply cursors belong to a replaceable index generation. Each durable transition writes an atomic index batch with its revision, then synchronizes the control batch with the same revision. Intermediate index apply batches retain atomic cursors and WAL protection without a second fsync. A crash can leave either database ahead, but reopening requires equal revisions before ordinary publication or source acknowledgement. Any mismatch fences the index and requires recovery from control authority and the catalog.

Recovery resolves pending catalog operations before scanning current data and position deletes into a fresh generation. It preserves published LSNs and schema versions, checks target UUIDs, and rechecks catalog heads before activation. Physical checkpoint files and their directory entries are synced before registration; activation syncs the replacement generation before atomically switching the control pointer. A checkpoint is usable only when its watermarks and catalog snapshots still match. Stale, missing or unusable checkpoints fall back to a full scan. Index loss never substitutes zero watermarks for missing authority; loss of the control store requires restoring that authority. Index format 3 adds derived live-row counts. Older formats trigger reconstruction from the existing control authority and Iceberg; upgrading the index does not recreate the source slot or control store.

## Row identity

Primary keys use a versioned typed encoding with explicit variable-length framing. Keys cannot contain nulls or floating-point values in the initial profile. Float zero and NaN representations are canonicalized for row fingerprints. Trailing nulls do not change a fingerprint, allowing an additive nullable field to preserve old row identity.

Unknown mutable keys are errors. An unchanged TOAST marker can be resolved from the same update's complete FULL old tuple; unavailable or incomplete old values are errors. Neither path emits equality deletes or placeholders. Key changes collapse as delete-old plus insert-new, including when both keys are touched repeatedly within one transaction.

Current-epoch collapse uses a bounded ordered memory map for ordinary source
prefixes. It shares the disk path's mutation transitions and initial-presence
checks. Insert-then-delete entries remain until the original key's absence has
been proved. Binding captures the complete table state and exact physical head;
publication rechecks both before writing artifacts or completing a no-op. If
retained state exceeds `limits.collapse_memory_bytes`, the entire memory attempt
is dropped and the same durable journal prefix is replayed through the disk path.
Initial copies, oversized transactions and legacy descriptors use disk directly.
Crash recovery always comes from the journal or the existing Prepared operation.

Replaying durable input avoids a second spill format. Memory and spill budgets
are explicit per worker, so total resource use depends on worker concurrency.

## Compaction and external maintenance

Workers receive a base snapshot and exact data/delete inputs, scan in bounded batches, apply position deletes and produce conditional old-to-new row mappings. Final serialization alone does not prevent resurrection; validation must prove the same inputs and applicable delete state survived since the base snapshot. Any conflicting delete forces replan.

Data compaction selects a bounded data-file group without pulling every shared-delete target into it. The worker reads the applicable delete files once, filters each record using its original sequence and target identity, and stages a sorted, deduplicated set on disk. Selected data rows consume their positions; residual delete files preserve effective positions for unselected survivors. Data and residual replacements publish in one exact-base `replace` snapshot. Only selected data rows receive new index locations, so a minor L0 rewrite can leave an existing L1/L2 base physically unchanged.

Optional data builds use a frozen RocksDB read view and durable BUILD ownership
without holding the table's publication fence. After build completion, a detached
task captures exact catalog/index head H, validates ancestry and physical inputs,
and translates late position deletes to output positions. The daemon briefly
gates the next same-table epoch, then activates only if catalog identity/location
and the complete `TableState` still equal H. Activation atomically installs the
ordinary Building fence before staging and sealing Prepared. The preparation
gate expires after 750 ms; its worker keeps BUILD and scratch while retiring in
the background, without continuing to pause CDC. BUILD protects files and
required history until the worker joins or ownership moves to Prepared. See
[local compaction](local-compaction.md) for the proof, bounds and tests.

Deletion density comes from live-row counts at the exact indexed snapshot. Low-density delete fanout triggers a separate bounded delete-only rewrite with no row-index deltas. Total delete-file count also contributes to debt, including dangling files; a fully dangling singleton can be removed with no output. The data-rewrite planner separately bounds delete inputs to 32 files, 32 MiB and one million positions by default. Exceeding a work budget preserves backpressure. File grouping respects the current unpartitioned spec. Z-order-aware compaction, distributed compaction protocols, HA and global autocompaction remain outside early scope; external compactor reconciliation remains required.

Unknown snapshots are classified from physical changes, not trusted solely because their operation says `replace`. Reconciliation proves the same PK set and fingerprints before moving index entries, and verifies effective deletes on surviving files. Keyless append-only tables use a disk-backed multiset comparison that preserves duplicate multiplicities and synthetic identities. Logical mutations stop publication. A full index rebuild uses standard file/deletion state and can occur after the service has been removed and later restored.

Independent maintenance uses its own idempotency-marker namespace. The local compactor fixture exercises this boundary in a separate process with private scratch storage; it never reads the source journal or ingestion index. Generic Iceberg actions may rebase validated files, while coordinator actions additionally require their indexed base snapshot.

## Latency and reader health

Only tables with pending CDC have publication deadlines. The scheduler subtracts estimated commit time from the publication budget, becomes ready at 10,000 source mutations, 32 MiB of serialized transaction payload or its deadline, and applies a global commit-rate budget. The row count is a readiness trigger, not an admitted-epoch ceiling. When a worker becomes available, it takes the already-queued ordered prefix of ordinary transactions within the 32 MiB payload limit and the existing loaded descriptor window; it never waits to fill that batch. A transaction with more than 10,000 mutations for the table or more than 32 MiB of payload is admitted alone. It may produce many files but still publishes in one snapshot per table. Zero-mutation work still receives a deadline and advances its local watermark without creating an empty snapshot.

New terminals carry exact per-table mutation counts computed from surviving spool chunks, before collapse. Retained older terminals have explicitly unknown counts: they become ready immediately and publish one whole transaction at a time. This preserves compatibility without rereading payloads, at a temporary batching cost while old history drains.

Compaction debt can delay or stop publication. The source can continue into its bounded durable journal until its own capacity is reached. This preserves row correctness and current-table health at the cost of a visible latency violation.

Metadata and garbage work has a separate due signal from soft data compaction.
One periodic table actor may run at a time, and ready CDC receives a dispatch
between periodic visits. Each visit checks manifest/history debt and scans one
bounded garbage-registry page; continuation can resume after CDC gets its turn.
The collector's scan budget is cooperative: retained-artifact checks and in-flight
storage requests are not a hard wall-clock deadline. Builds, prepared publication
and checkpoint protection retain their existing ownership rules.

Checkpoint rotation does not wait for all tables to become idle. A checkpoint
records pending operations consistently with its index and keeps their base and
successor snapshots protected. Direct restore requires a matching checkpoint
without pending operations; other checkpoints use the existing catalog-scan
recovery path. Rotation advances the protected history floor during active writes.

The scheduler uses observed per-table service times, including preparation and maintenance, to adjust publication deadlines. Periodic visits and build probes do not enter CDC service-time estimates. These estimates do not establish an SLO: the measured runs still fail latency qualification. Longer workloads, reader amplification and provider-level cost qualification remain open; FileIO and REST diagnostics record API operations rather than billed requests.
