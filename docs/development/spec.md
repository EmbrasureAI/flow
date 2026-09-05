# Implementation brief for the coding agent

## 1. Mission

Implement a permissively licensed, cloud-neutral, Rust-based streaming materializer that continuously replicates PostgreSQL tables into **ordinary Apache Iceberg v2 or v3 tables**.

The defining requirement is:

> The freshest externally published state must be queryable through an unmodified, standards-compliant Iceberg reader. No custom reader, proprietary hot-table format, union-read adapter, or vendor-specific query engine may be required.

The architecture is a native-Iceberg streaming LSM:

```text
PostgreSQL WAL
       ↓
source-wide durable transaction journal
       ↓
per-table materializers
       ↓
small, immediately visible Iceberg L0 files
+ position deletes or deletion vectors
       ↓
standard Iceberg snapshot
       ↓
Spark / Trino / DuckDB / other compatible readers

                 asynchronously

Iceberg L0
       ↓
continuous minor compaction
       ↓
Iceberg L1 / L2
       ↓
large, reader-efficient Parquet files
```

The target is **p95 PostgreSQL commit-to-Iceberg-publication below one second** for ordinary transactions in a same-region reference environment, while ensuring that small-file and delete-file amplification remain bounded.

Do not optimize for theoretical universality. Optimize first for a credible, measurable implementation covering:

* PostgreSQL.
* S3-compatible storage.
* Iceberg REST Catalog.
* Iceberg v2 position deletes.
* Unpartitioned or low-fan-out tables.
* Unmodified Spark, Trino, and DuckDB readers.
* One authoritative logical writer per replicated table.

## 2. Context and relationship to Moonlink

Moonlink is the most relevant prior implementation. It has:

* PostgreSQL CDC.
* An Arrow/memory/NVMe write layer.
* A primary-key and row-position index.
* Size-tuned Parquet persistence.
* Deletion vectors.
* Data and index compaction.
* Iceberg v3 output.

However, Moonlink separates its freshest state from its ordinary Iceberg snapshot. Its subsecond DuckDB path uses a custom storage extension that connects to Moonlink over a Unix socket, asks for a Moonlink-specific read state, scans returned Parquet files, and applies custom delete filtering.

That read state may include temporary files representing committed-but-not-yet-persisted rows in addition to persisted object-store files, position deletes, and DVs.

Moonlink’s README distinguishes standard Iceberg v3 output from its special real-time DuckDB, Postgres, and DataFusion integrations.

This project’s differentiation is therefore:

```text
Moonlink freshest read:
Moonlink state + special adapter

This project’s freshest published read:
standard Iceberg snapshot + normal Iceberg reader
```

Study Moonlink’s architecture for concepts, tests, failure modes, index design, and compaction policy. Do **not** copy Moonlink implementation code. Moonlink is currently under the Business Source License rather than an open-source license and restricts managed-service use until its future license change. This implementation must be clean-room and should use Apache-2.0-compatible dependencies.

Useful Moonlink design references include:

* A 500 ms internal snapshot timer.
* A production Parquet flush target of 128 MiB.
* Compaction groups of roughly 16–32 files and a final-file target around 512 MiB.
* A table architecture containing memory state, disk files, deletion vectors, indices, a WAL, Iceberg persistence, and compaction.

Use those as informed starting points, not immutable defaults.

---

## 3. Non-negotiable invariants

### 3.1 Standard Iceberg is always the public read plane

Every publicly visible table state must be a valid Iceberg snapshot consisting only of standard:

* Parquet data files.
* Iceberg manifests and metadata.
* Iceberg v2 position-delete files, or
* Iceberg v3 deletion vectors stored in Puffin files.

A normal reader must not need the service’s RocksDB index, source journal, control plane, or compactor.

### 3.2 Iceberg remains readable after the service is removed

The user owns:

* Object storage.
* Catalog.
* Namespace.
* Data files.
* Delete files.
* Metadata.

Stopping or permanently deleting the service must not impair reads of the latest committed Iceberg snapshot.

### 3.3 The service is the authoritative logical writer

Support arbitrary readers, but not arbitrary uncoordinated logical writers.

The contract is:

> Open read plane; coordinated write plane.

External `INSERT`, `UPDATE`, `DELETE`, and `MERGE` operations on replicated tables are unsupported initially because PostgreSQL is the logical source of truth.

### 3.4 Reader amplification must remain bounded

Low publication latency is not success if:

* The current snapshot accumulates unlimited small files.
* Delete files per data file increase indefinitely.
* Manifest planning grows indefinitely.
* Compaction debt grows continuously.
* Reader performance degrades over time.

When the system cannot preserve both latency and reader health, it must raise publication latency or backpressure ingestion rather than silently degrading the table.

### 3.5 Do not use equality deletes as the normal persisted representation

Use:

* Iceberg v2 position deletes first.
* Iceberg v3 deletion vectors later.

Equality deletes may simplify ingestion but push substantial matching work onto readers and have inconsistent support across engines. Do not use them as an emergency fallback when the row-position index is unavailable.

### 3.6 Correctness before subsecond claims

A transaction must never be lost, duplicated, partially applied within one table, or resurrected by compaction.

Large transactions, schema changes, external rewrites, and recovery may temporarily violate the latency SLO. They may not violate correctness.

---

## 4. Initial supported profile

Implement this profile first:

```text
Language:            Rust
Source:              PostgreSQL
Capture:             pgoutput logical replication
Destination format:  Iceberg v2
Delete representation:
                     position-delete files
Storage:             S3 and MinIO
Catalog:             Iceberg REST Catalog
Table layout:        unpartitioned initially
Readers tested:      Spark, Trino, DuckDB
Deployment:          one binary/container
Writer model:        one coordinator per table
Compaction:          built in, with separable worker role
```

After this profile passes correctness and 24-hour performance tests, add:

1. Low-fan-out partitioned tables.
2. Iceberg v3 deletion vectors.
3. Additional catalogs.
4. GCS and Azure through storage abstractions.
5. Distributed compaction workers.
6. Additional CDC sources.

Do not start with multiple databases, MySQL, arbitrary transformations, joins, or a full stream-processing runtime.

---

## 5. Rust implementation foundation

Create a Rust workspace resembling:

```text
crates/
  model/                 shared transaction and row types
  pg-source/             PostgreSQL snapshot and CDC capture
  ingress-journal/       source-wide durable transaction log
  state-store/           RocksDB state and schemas
  iceberg-ext/           missing Iceberg transaction/validation primitives
  materializer/          per-table L0 writers
  coordinator/           table commits and source acknowledgement
  compactor/             planner, worker, validator
  catalog-watch/         external snapshot classification/reconciliation
  daemon/                CLI, role management, config, metrics
  testkit/               Docker fixtures, chaos tests, reader conformance
```

Use:

* Tokio for asynchronous execution.
* Arrow and Parquet for in-memory batches and file writing.
* RocksDB for indices, transaction state, and prepared-operation metadata.
* Apache `iceberg-rust` as the Iceberg implementation.
* Iceberg’s storage and catalog abstractions instead of AWS-specific calls.
* Prometheus/OpenTelemetry-compatible metrics.
* `tracing` for structured logs.
* Property-based tests for transaction collapse and recovery state machines.

### 5.1 PostgreSQL source implementation

Prefer starting from **Supabase ETL** as the PostgreSQL capture subsystem. It is an Apache-2.0 Rust replication framework with initial-copy handling, ongoing CDC, persistent checkpoints, and custom destination interfaces. Its existing Iceberg destination is irrelevant and should not be reused; use only the source and initial-sync machinery. ([GitHub][5])

Pin a specific commit and place it behind an internal `PostgresSource` trait.

Before relying on it, add capability tests for:

* Transaction boundaries.
* Multi-table transactions.
* Primary-key changes.
* Large streamed transactions.
* TOAST markers.
* Network interruption and reconnect.
* Initial snapshot plus CDC catch-up.
* PostgreSQL 14–18.

If the library cannot expose the required transaction semantics, replace only the low-level transport with a native pgoutput implementation such as a current Rust logical-replication client. Do not redesign the rest of the system around a source-library limitation.

### 5.2 Iceberg Rust gaps are part of the project

As of the current Iceberg Rust implementation, the general writer layer can construct data, equality-delete, and position-delete writers, but several transaction-level operations required here are still incomplete. ([Iceberg Rust][6])

In particular:

* `RewriteFiles` support and its conflict validation remain active work.
* A reusable `SnapshotValidator` remains active work.
* Native v3 DV writing plus `RowDelta` remains active work. ([GitHub][7])

Therefore:

1. Fork or pin `iceberg-rust`.
2. Implement missing `RowDeltaAction`, `RewriteFilesAction`, and snapshot validation as isolated upstream-compatible patches.
3. Add tests equivalent in behavior to Iceberg Java’s validation tests.
4. Submit generally useful portions upstream.
5. Never bypass validation by hand-writing metadata JSON.

Shipping these missing Rust primitives upstream is itself a valuable part of the OSS contribution.

---

## 6. Top-level architecture

```text
                         PostgreSQL
                             │
                     one logical slot
                             │
                 source transaction decoder
                             │
                  durable ingress journal
                             │
                 source acknowledgement actor
                             │
                  transaction dispatcher
                 /           │            \
                /            │             \
       table actor A   table actor B   table actor C
             │               │              │
       mutable epoch   mutable epoch   mutable epoch
             │               │              │
       L0 file writer  L0 file writer  L0 file writer
             \               │              /
              \              │             /
             per-table commit coordinators
                             │
                   Iceberg REST Catalog
                             │
                      object storage
                             │
                 standard Iceberg readers

                compaction planner per table
                             │
                 stateless compaction workers
                             │
                  same table coordinator
                             │
                      validated rewrite
```

Use one source actor per logical replication slot and one commit coordinator per destination table.

Compaction execution may run concurrently or in another process. All final table commits still pass through the table coordinator.

---

## 7. Source-wide transaction journal and WAL acknowledgement

### 7.1 Journal model

Create a source-wide ordered journal containing complete PostgreSQL transaction envelopes:

```rust
struct SourceTransaction {
    source_id: SourceId,
    xid: u32,
    begin_lsn: PgLsn,
    commit_lsn: PgLsn,
    commit_timestamp: Timestamp,
    schema_versions: Vec<TableSchemaVersion>,
    affected_tables: Vec<TableId>,
    mutation_chunks: Vec<JournalChunkRef>,
}
```

Large transactions may be represented by multiple chunk records followed by one durable terminal commit record.

Journal files should be append-only segments with:

* Versioned framing.
* CRC/checksum.
* Monotonic sequence numbers.
* Transaction identifiers.
* Commit markers.
* `fdatasync` or equivalent durability.
* Recovery truncation at the first corrupt or incomplete frame.

Do not store arbitrarily large raw transaction payloads as individual RocksDB values. Use sequential segment files, while RocksDB stores offsets and transaction state.

### 7.2 Acknowledgement is source-wide

Maintain three distinct watermarks:

```text
received_lsn
journal_durable_lsn
materialized_lsn
```

`materialized_lsn` means every table affected by every transaction in the contiguous prefix has committed those mutations to Iceberg.

Implement two acknowledgement modes:

#### `ack_mode = materialized`

```text
Postgres ACK =
highest contiguous transaction whose affected tables
have all committed to Iceberg
```

This is the safest default when the journal is not independently durable against node or disk loss.

#### `ack_mode = journaled`

```text
Postgres ACK =
highest contiguous transaction whose complete transaction
and commit marker are durably stored in the ingress journal
```

This decouples PostgreSQL WAL retention from temporary Iceberg lag. Enable it only when the configured journal backend satisfies an explicitly documented durability class.

Journal segments may be deleted only after all affected tables have materialized the corresponding transactions.

A PostgreSQL logical slot is one ordered stream for a database and may resend recent messages after a crash, so replay and idempotence are required. ([PostgreSQL][8])

### 7.3 WAL retention protection

Continuously monitor:

* `restart_lsn`.
* `confirmed_flush_lsn`.
* Current WAL LSN.
* Retained WAL bytes.
* `wal_status`.
* `safe_wal_size`, where available.
* Ingress-journal bytes.
* Oldest unmaterialized source transaction.

PostgreSQL explicitly warns that replication slots can retain enough WAL to fill `pg_wal`. ([PostgreSQL][2])

Implement:

* Configurable soft and hard retained-WAL thresholds.
* Health warnings before the hard threshold.
* A journal disk quota.
* A `source_at_risk` state.
* Metrics and actionable error messages.

Never automatically:

* Advance an ACK over undurable data.
* Drop the replication slot.
* Discard source transactions.
* Pretend a lost slot is recoverable.

If both the journal and PostgreSQL WAL approach their limits, stop advancing acknowledgements, surface a critical condition, and preserve correctness.

### 7.4 Multi-table transactions

A PostgreSQL transaction may affect multiple tables.

Each table may publish independently, so cross-table atomic query visibility is not guaranteed. However, source acknowledgement in `materialized` mode must wait until all affected tables have committed.

Maintain a transaction ledger:

```text
source transaction T
  table A → snapshot 101 committed
  table B → snapshot 305 pending
  table C → snapshot 992 committed

T is not materialized yet
```

Do not allow a fast table to move the source ACK beyond an earlier transaction that remains pending on a slow table.

---

## 8. Large transactions and complete row images

### 8.1 Large transaction spill

Never assume a transaction fits in memory.

PostgreSQL can stream portions of a large in-progress transaction using Stream Start/Stop followed by Stream Commit or Abort. ([PostgreSQL][9])

Implement a transaction spool:

```text
txn/<xid>/
  segment-0001
  segment-0002
  ...
  state
```

Maintain a transaction-local disk-backed mutation map keyed by:

```text
(table_id, primary_key)
```

This map should collapse repeated mutations:

```text
INSERT → UPDATE         = one final INSERT
UPDATE → UPDATE         = delete original + final INSERT
INSERT → DELETE         = no output
UPDATE → DELETE         = delete original only
DELETE → INSERT         = replacement according to source semantics
PK change               = delete old PK + insert new PK
```

For large transactions:

* Keep event payloads on disk.
* Keep only bounded working sets in memory.
* Clean up aborted transaction state.
* Write multiple Parquet files if required.
* Publish all changes for a given table from that source transaction in one table snapshot.

Do not split one source transaction across multiple visible snapshots for the same table merely to satisfy the latency target.

Define the subsecond SLO only for ordinary transactions, for example:

```text
<= 10,000 row mutations
<= 16 MiB decoded payload
```

Transactions above the threshold must remain correct but are reported separately.

### 8.2 PostgreSQL TOAST and replica identity

A replacement-row Iceberg model requires a complete new row.

Under normal replica identity, unchanged TOASTed column values may be absent from an update event. Debezium documents that it cannot safely fetch these values out-of-band because of race conditions; it emits a placeholder instead. With `REPLICA IDENTITY FULL`, TOAST values are included in the before and after images. ([Debezium][3])

For V1:

* Require `REPLICA IDENTITY FULL` for mutable tables.
* Append-only tables do not require it.
* Check this during table setup.
* Refuse to start mutable replication when the requirement is unmet.
* If an unchanged-TOAST marker appears at runtime, pause the table rather than writing a placeholder.
* Benchmark and report the resulting PostgreSQL WAL overhead.

A later experimental mode may reconstruct missing values from the indexed previous Iceberg row, but do not make random object-store row reconstruction part of V1.

---

## 9. Persistent row-location index

For each current logical row, persist:

```rust
struct RowLocation {
    data_file_id: FileId,
    row_position: u64,
    data_sequence_number: i64,
    spec_id: i32,
    partition: EncodedPartition,
    source_commit_lsn: PgLsn,
    row_version: u64,
    row_fingerprint: [u8; 16],
}
```

Use a stable canonical primary-key encoding supporting composite keys.

Maintain RocksDB column families equivalent to:

```text
pk_location:
  table_id + encoded_pk → RowLocation

file_rows:
  table_id + file_id + row_position → encoded_pk

file_metadata:
  file_id → path, row_count, size, level, creation snapshot

prepared_operations:
  operation_id → state, files, index delta, source LSNs

source_transactions:
  source transaction → per-table publication state

table_state:
  last observed snapshot, last materialized LSN, schema version
```

The reverse `file_rows` mapping is needed for compaction and external-rewrite reconciliation.

The index is:

* Required for writes.
* Never required for reads.
* Rebuildable from the current Iceberg table.
* Periodically checkpointed for faster recovery.

During initial loads and compaction, construct index entries while writing output rows so row positions are known without rescanning.

---

## 10. L0 publication path

L0 is an internal classification, not a new Iceberg file type.

### 10.1 Epoch formation

Maintain one mutable epoch per table.

Close an epoch when the first of these occurs:

* Publication deadline reached.
* Buffered payload reaches 16–32 MiB.
* Row-count threshold reached.
* A forced consistency operation requires publication.
* A large source transaction completes.

Starting publication timing:

```text
target maximum buffering: 300–500 ms
hard maximum under normal load: 750 ms
```

Estimate recent object-store and catalog commit latency and close the epoch early enough to meet the one-second publication budget.

Never create an empty snapshot.

### 10.2 Mutation resolution

For every final mutation in the epoch:

#### INSERT

* Append the complete row to a new Parquet file.
* Record the resulting file and row position.

#### UPDATE

* Look up the old physical row location.
* Emit a position delete for the old location.
* Append the complete replacement row.
* Update the index only after the Iceberg commit succeeds.

#### DELETE

* Look up the old physical row location.
* Emit a position delete.
* Remove the index mapping after the commit succeeds.

#### Unknown key

Do not silently fall back to equality deletes.

Pause and:

* Reconcile the index.
* Determine whether this is replay, drift, or a source inconsistency.
* Retry only after the physical location is known.

### 10.3 Parquet output

For V1:

* Support unpartitioned tables first.
* Write one or a small number of data files per table epoch.
* Preserve deterministic row ordering within each output file.
* Capture row positions as rows are written.
* Produce complete Iceberg column metrics.
* Use configurable Parquet compression.
* Keep schema field IDs stable.

Under subsecond publication, files may be much smaller than the final target. That is expected.

### 10.4 Position-delete output

Write Iceberg v2 position-delete files sorted by:

```text
data file path
row position
```

This ordering is required and helps readers apply deletes efficiently. ([Apache Iceberg][10])

Aggregate deletes across multiple target data files into a bounded number of delete files per epoch rather than blindly creating one file per target row or target file.

Track which data files each delete file references.

If a later compaction rewrites only some data files referenced by a shared delete file, either:

* Expand the compaction input to the full safe closure, or
* Rewrite the residual delete entries for unaffected files.

### 10.5 Prepared commit protocol

Each table publication follows:

```text
1. Build data and delete files.
2. Persist PreparedTableCommit in RocksDB.
3. Commit one Iceberg RowDelta snapshot.
4. Persist the committed snapshot ID.
5. Atomically apply the index delta.
6. Mark included source transactions materialized for this table.
7. Permit source-wide acknowledgement to advance.
8. Garbage-collect prepared state later.
```

Include snapshot summaries such as:

```text
streaming.writer-id
streaming.source-id
streaming.operation = ingest
streaming.epoch-id
streaming.first-lsn
streaming.last-lsn
streaming.transaction-count
streaming.schema-version
```

Use deterministic epoch and file identifiers so retries are idempotent.

On recovery, inspect snapshot summaries to determine whether a prepared operation committed before the crash.

---

## 11. Compactor deployment model

Ship one binary with roles:

```text
--roles=ingest,coordinator,compactor
--roles=ingest,coordinator
--roles=compactor
```

Start with all roles in one process.

Architecturally separate:

* Compaction planning.
* Compaction execution.
* Table commit coordination.

A stateless compaction worker may:

1. Receive a plan tied to base snapshot `S`.
2. Read selected data files and all applicable delete state.
3. Produce new data files.
4. Produce a complete old-location-to-new-location index delta.
5. Return the prepared result.

The worker must never independently commit the rewrite.

Only the table coordinator may:

* Refresh current table metadata.
* Validate the plan.
* Commit the rewrite.
* Apply the index transition.

This allows workers to scale independently without creating multiple competing authorities for the table.

---

## 12. Compaction policy

Use LSM-inspired, debt-driven maintenance rather than an hourly cron.

### 12.1 Levels

```text
L0:
recent files produced by subsecond epochs

L1:
64–128 MiB files produced by minor compaction

L2:
256–512 MiB reader-optimized files
```

These are logical service classifications. Every level is ordinary Iceberg data.

### 12.2 L0 triggers

Trigger L0 compaction when any threshold is crossed:

* L0 file count greater than 24–32.
* Oldest L0 file older than 10–30 seconds.
* L0 bytes greater than 128–256 MiB.
* Position-delete overlap exceeds its budget.
* Deleted-row density exceeds its budget.
* Query-planning or file-open metrics exceed limits.

### 12.3 L0 to L1

Use size-tiered compaction:

* Prefer similarly sized recent files.
* Prioritize the oldest L0 files.
* Limit a compaction group to roughly 16–32 files.
* Target 64–128 MiB outputs.
* Apply all applicable deletes while rewriting.
* Avoid repeatedly rewriting very new files likely to receive additional updates.

### 12.4 L1 to L2

Use partition-local or leveled compaction:

* Target 256–512 MiB.
* Respect the Iceberg table sort order.
* If no sort order exists and update workloads dominate, optionally sort by primary key.
* Avoid blind full-table compaction.

### 12.5 Delete-aware compaction

Prioritize files when:

* Position-delete density exceeds roughly 15–25%.
* More than approximately four delete files overlap one data file.
* A delete file is tiny or references many now-dead files.
* Reader delete-processing time is increasing.

These values are initial tuning points, not specification requirements.

### 12.6 Compaction debt and write stalls

Compute debt from:

```text
L0 file count
L0 bytes
oldest L0 age
delete files per data file
deleted-row density
manifest count
estimated rewrite bytes
compaction queue age
```

Implement soft and hard debt limits.

At the soft limit:

* Increase compaction concurrency.
* Reduce nonessential maintenance.
* Lengthen publication intervals slightly for low-priority tables.

At the hard limit:

* Apply LSM-style write stalls or substantial publication backpressure.
* Keep source capture in the durable journal if capacity permits.
* Surface the latency violation explicitly.

Never let reader amplification grow without bound merely to preserve a latency metric.

---

## 13. Compaction correctness protocol

This is a central correctness requirement.

### 13.1 The stale-compactor race

The unsafe sequence is:

```text
Compactor reads row R from file A at snapshot S
Ingestion deletes R at snapshot S+1
Compactor writes R into file B
Compactor replaces A with B
R is accidentally resurrected
```

Serializing only the final commits does not prevent this.

### 13.2 Required state machine

For each compaction:

```text
PLANNED
  base snapshot S
  selected input files I
  applicable delete state D
  input-file generations

BUILT
  replacement files O
  old→new row-location delta

VALIDATING
  refresh current head H

COMMITTED
  validated RewriteFiles commit

INDEX_APPLIED
  CAS updates complete
```

### 13.3 Final validation

Immediately before commit:

1. Confirm that `S` is an ancestor of current head `H`.
2. Confirm every input data file is still live.
3. Confirm no input file was rewritten or removed.
4. Confirm no new position delete or DV since `S` applies to any input file.
5. Confirm no incompatible schema or partition-spec change occurred.
6. Confirm the exact delete state used by the worker remains current.
7. Confirm any shared position-delete file is either fully consumed or has a correctly generated residual replacement.

Use the equivalent of Iceberg:

* `validateFromSnapshot`.
* `validateDataFilesExist`.
* `validateNoConflictingDeleteFiles`.
* Rewrite input-existence validation.

Iceberg’s conflict-validation APIs exist specifically to detect concurrent deletes and rewrites that could invalidate a read-derived operation. ([Apache Iceberg][1])

If any validation fails:

```text
do not rebase stale output
do not commit
mark output as orphan candidate
replan from the current snapshot
```

### 13.4 Index transition

After the rewrite commit succeeds, apply index mappings using compare-and-swap:

```text
replace:
  pk → old_file, old_position

with:
  pk → new_file, new_position

only if:
  current pk mapping still equals old_file, old_position, old_version
```

Never overwrite a newer mapping created by ingestion.

Remove old reverse mappings only after the corresponding CAS operation succeeds or is proven obsolete.

### 13.5 Ingestion commits need validation too

An ingestion update may have looked up a row in file `A` while an external compactor replaced `A`.

Before committing its position delete:

* Validate that the referenced data file still exists.
* If not, refresh and reconcile the key’s new location.
* Regenerate the delete artifact.
* Retry the table commit.

---

## 14. External Iceberg maintenance interoperability

Users must **not** be required to disable all table maintenance.

### 14.1 Allowed without special handling

Allow ordinary reads and metadata maintenance such as:

* Manifest rewrites.
* Statistics generation.
* Snapshot expiration respecting retention constraints.
* Orphan cleanup using a sufficiently conservative age threshold.

Manifest rewriting and snapshot expiration do not inherently change physical row positions in current live data files.

### 14.2 Recognized delete-file maintenance

Allow standard position-delete-file rewrites when they preserve the logical delete set.

Refresh delete metadata before the next ingestion or compaction commit.

### 14.3 External data-file compaction

External `rewrite_data_files` changes physical locations and invalidates part of the index.

Detect every unknown snapshot before committing new ingestion. Classify snapshots using:

* Snapshot operation.
* Added and removed data files.
* Added and removed delete files.
* Service snapshot-summary markers.
* Schema/spec changes.

For a likely physical rewrite:

1. Pause publication for the affected table.
2. Continue journaling source WAL.
3. Identify removed and added files.
4. Scan added files.
5. Rebuild PK, row position, and row fingerprint entries.
6. Compare the added rows against the expected live rows from removed files.
7. Update mappings with compare-and-swap.
8. Resume publication.

Store a canonical row fingerprint in the index so a supposed external compaction can be distinguished from a rewrite that changed row values.

If the PK set or row fingerprints do not match expected logical state, classify it as an external logical mutation and pause for operator intervention.

### 14.4 Product modes

Expose:

#### `managed-maintenance`

* Built-in compactor is the normal data rewriter.
* Strongest publication SLO.
* External rewrites are reconciled as exceptional events.

#### `external-maintenance`

* External compaction is expected.
* Automatic reconciliation remains enabled.
* Publication SLO is suspended while affected files are reindexed.

Do not require users to turn off existing maintenance before initial adoption. Clearly explain that running two frequent data compactors is redundant and may increase lag and cost.

---

## 15. Iceberg v3 mode

Implement v3 only after the v2 position-delete implementation is correct and benchmarked.

Iceberg v3 requires:

* New deletes represented as DVs rather than new position-delete files.
* At most one current DV per data file.
* A newly written DV to include prior applicable position deletes and the prior DV.
* DV metadata to reference the exact data file.
* DVs to be stored in Puffin blobs. ([Apache Iceberg][10])

For each epoch:

1. Group touched base files.
2. Load and cache the current DV bitmap for each.
3. Union newly deleted positions.
4. Write replacement DV blobs.
5. Pack multiple DVs into one Puffin file when practical.
6. Commit replacement DV metadata atomically with new rows.
7. Remove superseded DV references.
8. Rewrite the base file when deletion density crosses its threshold.

Because Iceberg Rust’s v3 DV writer and RowDelta work remain incomplete, implement these as upstream-oriented contributions rather than proprietary metadata code. ([GitHub][11])

Keep v2 as the broad-compatibility mode even after v3 exists.

---

## 16. Adaptive latency and cost control

Do not implement a fixed unconditional 500 ms commit timer for every table.

Use an event-driven earliest-deadline scheduler.

Each table has:

```text
first_pending_event_time
target_publication_deadline
buffered_bytes
buffered_rows
estimated_commit_p95
compaction_debt
priority_class
```

Publication occurs at the earliest of:

* Deadline minus estimated commit duration.
* Size threshold.
* Row threshold.
* Required transaction boundary.
* Explicit flush.

Offer profiles:

```text
realtime:
  target p95 <1 second

balanced:
  target 2–5 seconds

efficient:
  size-first batching with a larger maximum delay
```

Use a global commit-rate budget so hundreds of tables do not simultaneously overwhelm the catalog.

When deadlines cannot all be satisfied:

1. Prioritize real-time tables.
2. Preserve source order.
3. Preserve reader-health limits.
4. Report missed SLOs.
5. Never create empty commits.

There is no architecture that avoids the cost of a small standard Iceberg artifact when only a tiny amount of data arrives and must become visible to a normal reader immediately. Treat this as a documented trade-off, not an implementation bug.

---

## 17. Metadata and snapshot maintenance

Subsecond snapshots require explicit history management.

Implement:

* Fast-append or RowDelta behavior on the hot path.
* No eager manifest merging during every ingestion commit.
* Manifest rewrites when planning debt crosses thresholds.
* Snapshot expiration with a configurable safe-reader window.
* Preservation of selected checkpoint snapshots for useful time travel.
* Delayed orphan-file deletion.
* Cleanup of failed prepared-operation output after a grace period.

Track:

```text
snapshots created per second
retained snapshots
metadata bytes written
manifest count
manifest entries
catalog commit latency
orphan candidates
object-store PUT/GET/DELETE operations
```

Snapshot expiration reduces retained metadata but does not erase the cost of having created snapshots. Cost metrics must therefore account for creation rate as well as retained table size.

---

## 18. Initial snapshot

Implement PostgreSQL initial synchronization as:

```text
1. Create or attach to logical replication slot.
2. Establish a consistent snapshot and start LSN.
3. Begin journaling changes after that LSN.
4. Bulk-copy existing rows in parallel.
5. Write L1-sized Parquet files.
6. Build both index directions while writing.
7. Commit the initial Iceberg snapshot.
8. Replay journaled transactions through the ordinary L0 path.
9. Mark the table live after catch-up.
```

Do not invent a polling-based initial snapshot if the chosen PostgreSQL source library already has a correct snapshot-plus-CDC handoff.

Initial copy output should be reader optimized; do not deliberately generate L0 files for the static bulk copy.

---

## 19. Schema and source constraints

For the first production-ready profile:

* Require a primary key for mutable tables.
* Allow tables without a key only in append-only mode.
* Support composite primary keys.
* Support nullable non-key columns.
* Support additive nullable columns.
* Pause on unsupported type or incompatible schema changes.
* Preserve stable Iceberg field IDs.
* Treat primary-key changes as delete-old-key plus insert-new-key.
* Reject or explicitly defer `TRUNCATE`.
* Defer generated-column and arbitrary custom-type support unless the source library already handles them safely.
* Defer PostgreSQL prepared transactions/two-phase commit.

Schema changes must be serialized with table publication and reflected in Iceberg before rows using the new schema are published.

---

## 20. Recovery model

### 20.1 Source transaction recovery

On restart:

1. Recover journal segments.
2. Discard incomplete transaction tails.
3. Load transaction ledger.
4. Resume PostgreSQL from the latest safely acknowledged LSN.
5. Deduplicate resent transactions by source ID, commit LSN, and transaction identity.
6. Redispatch journaled but unmaterialized transactions.

### 20.2 Table commit recovery

For each prepared operation:

* Find an Iceberg snapshot carrying its operation or epoch ID.
* If found, finish applying the index delta.
* If not found, retry the commit or mark its files as orphan candidates.
* Do not resume new writes until index state matches the current table snapshot.

### 20.3 Index recovery

Periodically checkpoint the index with the corresponding Iceberg snapshot ID.

Recovery order:

```text
restore newest usable checkpoint
replay service-authored snapshot deltas
reconcile external snapshots
resume publication
```

If no checkpoint is available, rebuild by scanning the current Iceberg table and applying all current position deletes or DVs.

The ability to rebuild the index is mandatory. The expected speed of that rebuild is not part of the read contract.

---

## 21. Correctness testing

Implement deterministic and randomized tests for:

* Insert, update, delete.
* Composite keys.
* Primary-key changes.
* Multiple updates to one key in a transaction.
* Insert then delete in one transaction.
* Multi-table transactions.
* Empty transactions.
* Transaction abort.
* Large streamed transactions.
* TOASTed columns.
* Network reconnect.
* Source message replay.
* Crash before data-file write.
* Crash after data-file write.
* Crash after prepared-state persistence.
* Crash after Iceberg commit but before index update.
* Crash after index update but before source acknowledgement.
* Compaction versus concurrent update.
* Compaction versus concurrent delete.
* External compaction versus ingestion.
* Catalog conflict and retry.
* Snapshot expiration.
* Index checkpoint loss.
* Full index rebuild.
* Schema addition.
* Reader correctness through stock Spark, Trino, and DuckDB.

The critical regression test is:

```text
1. Compactor reads row R from input file.
2. Ingestion commits a delete of R.
3. Compactor attempts to commit stale output containing R.
4. Validation must reject the compaction.
5. R must remain deleted.
```

Use chaos injection at every durable state transition.

---

## 22. Performance and cost benchmark

Run each principal workload for at least 24 hours.

### 22.1 Dimensions

Test:

```text
active tables:
1, 10, 100

row sizes:
256 B, 1 KiB, 16 KiB

mutation rates:
1K, 10K, 50K, 100K mutations/s

operation mixes:
100% insert
70% update / 20% insert / 10% delete
90% update

update locality:
localized hot set
Zipfian
uniform random

transaction sizes:
1 row
100 rows
10K rows
very large streamed transaction

partition fan-out:
1 initially
then 10 and 100 as later stress tests
```

### 22.2 Latency measurements

Measure independently:

```text
Postgres commit → journal durable

Postgres commit → Iceberg catalog commit

Iceberg catalog commit → Trino observes change

Iceberg catalog commit → Spark observes change

Iceberg catalog commit → DuckDB observes change
```

Do not report catalog publication as though it automatically equals every reader’s visibility. Readers may cache table metadata.

### 22.3 Throughput and health measurements

Record:

* Mutations per second.
* Source bytes per second.
* L0 files and bytes.
* Oldest L0 age.
* Position-delete files per data file.
* Deleted-row density.
* Current-snapshot file count.
* Manifest count.
* Query planning latency.
* Full-scan latency.
* Selective-filter latency.
* Compaction input/output bytes.
* Compaction write amplification.
* Compaction queue age.
* RocksDB bytes per indexed row.
* Index IOPS.
* Catalog operations.
* Object-store requests.
* Retained PostgreSQL WAL.
* Ingress-journal size.

### 22.4 Cost measurements

Produce:

```text
dollars per million mutations
dollars per GiB ingested
dollars per active-table-hour
object-store operations per million mutations
catalog commits per million mutations
compaction bytes per source byte
```

Make the price model configurable rather than hardcoding one cloud’s rates.

### 22.5 Acceptance targets

For ordinary transactions in the declared same-region reference environment:

```text
Postgres commit → Iceberg catalog commit:
p95 < 1 second
p99 < 2 seconds

Sustained baseline:
50K mixed mutations/s

Stretch:
100K+ mixed mutations/s

L0:
no unbounded growth
oldest file normally <30 seconds
current tail normally <=32 files per table

Reader health:
planning latency <=2x fully compacted baseline
full-scan slowdown target <=25%
delete amplification remains within configured limits

Correctness:
zero missing, duplicated, or resurrected rows
under crash and concurrency tests
```

For 100 continuously active low-volume tables, do not assume the same efficiency. Publish the measured snapshot-rate and cost curve honestly.

---

## 23. Implementation sequence

### Phase 0: capability audit and repository setup

* Create the Rust workspace.
* Pin Supabase ETL and Iceberg Rust revisions.
* Verify source transaction semantics.
* Verify current v2 write and read capabilities.
* Write ADRs for acknowledgement, durability, row identity, and external maintenance.
* Establish Apache-2.0 licensing and clean-room rules.

### Phase 1: append-only vertical slice

Deliver:

```text
Postgres
→ source-wide journal
→ one unpartitioned Iceberg v2 table
→ <1s append publication
→ stock Trino/Spark/DuckDB read
```

Include crash recovery and source-wide ACK semantics.

### Phase 2: position-delete MOR

Add:

* PK index.
* Reverse file index.
* Updates and deletes.
* Transaction-local mutation collapse.
* V2 position-delete writer.
* RowDelta commit action.
* TOAST preflight and `REPLICA IDENTITY FULL`.
* Multi-table transaction ledger.

### Phase 3: native compaction

Add:

* L0 classification.
* Debt-based planner.
* Worker interface.
* `RewriteFiles`.
* Snapshot validation.
* CAS index transition.
* Stale-compactor regression tests.
* Reader-health controls.

### Phase 4: operational interoperability

Add:

* Snapshot watcher.
* External metadata-maintenance classification.
* External data-rewrite reconciliation.
* Row fingerprints.
* Snapshot retention and orphan cleanup.
* Split compactor-worker deployment.

### Phase 5: scale and benchmark

Run:

* 24-hour workloads.
* 1/10/100 active-table tests.
* Random versus localized updates.
* Cost model.
* Reader visibility tests.
* Large transaction and WAL pressure tests.

Do not begin v3 until these pass.

### Phase 6: Iceberg v3

Add:

* Puffin writer.
* Deletion-vector encoding.
* DV merge semantics.
* V3 RowDelta.
* Stock-reader conformance.
* Upstream Iceberg Rust contributions.

---

## 24. Explicit non-goals

Do not implement in the first version:

* A custom query engine.
* A Moonlink-style union-read extension.
* Kafka or Flink as mandatory infrastructure.
* Multiple independent logical writers.
* Cross-table atomic Iceberg visibility.
* Arbitrary partition fan-out with a subsecond guarantee.
* Generic streaming SQL.
* MySQL or Oracle CDC.
* Equality-delete-based steady state.
* Automatic source-slot deletion.
* Silent placeholder values for missing TOAST data.
* Hand-authored Iceberg metadata outside the Iceberg library.
* V4-only features.
* Cloud-vendor-specific control-plane dependencies.

---

## 25. Required OSS deliverables

The repository is not complete until it includes:

1. A one-command local demo with:

   * PostgreSQL.
   * MinIO.
   * An open REST catalog.
   * The Rust service.
   * Trino or DuckDB querying the table.

2. An end-to-end demonstration where:

   * A PostgreSQL row is inserted.
   * The plain Iceberg reader sees it.
   * The row is updated.
   * The plain reader sees the replacement through v2 MOR.
   * The L0 files are compacted.
   * The plain reader still returns the same logical result.

3. A conformance suite against Spark, Trino, and DuckDB.

4. A stale-compactor correctness test.

5. A large-transaction and TOAST test.

6. A source-wide WAL acknowledgement test involving one transaction touching multiple tables.

7. A benchmark report covering latency, reader degradation, compaction amplification, and cost.

8. Documentation that clearly distinguishes:

   * Journal durability.
   * Iceberg publication.
   * Actual reader observation.
   * Physical compaction.

9. Upstream pull requests for generally useful Iceberg Rust primitives.

---

## 26. Final prioritization

Focus engineering effort in this order:

```text
1. Source-wide durability and replay correctness
2. Valid v2 RowDelta publication
3. PK→physical-position index correctness
4. Stale-compactor conflict detection
5. Bounded L0 and delete amplification
6. External maintenance reconciliation
7. Latency and cost optimization
8. Iceberg v3 deletion vectors
```

The project succeeds when it demonstrates:

> Moonlink-like CDC, indexing, and compaction, but with the standard Iceberg snapshot—not a specialized adapter—as the freshest public read plane.

The hardest and most valuable parts are not PostgreSQL decoding or Parquet writing. They are source-wide transaction durability, physical-row indexing, concurrency-safe compaction, adaptive commit economics, and keeping the current Iceberg snapshot healthy under a sustained subsecond publication workload.

[1]: https://iceberg.apache.org/javadoc/1.4.3/org/apache/iceberg/RowDelta.html?utm_source=chatgpt.com "RowDelta"
[2]: https://www.postgresql.org/docs/current/view-pg-replication-slots.html?utm_source=chatgpt.com "Documentation: 18: 53.20. pg_replication_slots"
[3]: https://debezium.io/documentation/reference/stable/connectors/postgresql.html?utm_source=chatgpt.com "Debezium connector for PostgreSQL"
[4]: https://iceberg.apache.org/docs/latest/spark-structured-streaming/?utm_source=chatgpt.com "Spark Structured Streaming - Apache Iceberg"
[5]: https://github.com/supabase/etl?utm_source=chatgpt.com "supabase/etl: A high-performance Postgres replication ..."
[6]: https://rust.iceberg.apache.org/api/iceberg/writer/index.html?utm_source=chatgpt.com "iceberg::writer"
[7]: https://github.com/apache/iceberg-rust/issues/1606/linked_closing_reference?reference_location=REPO_ISSUES_INDEX&utm_source=chatgpt.com "Add `RewriteFiles` support to iceberg-rust · Issue #1607"
[8]: https://www.postgresql.org/docs/current/logicaldecoding-explanation.html?utm_source=chatgpt.com "Documentation: 18: 47.2. Logical Decoding Concepts"
[9]: https://www.postgresql.org/docs/current/protocol-logical-replication.html?utm_source=chatgpt.com "18: 54.5. Logical Streaming Replication Protocol"
[10]: https://iceberg.apache.org/spec/?utm_source=chatgpt.com "Spec - Apache Iceberg™"
[11]: https://github.com/apache/iceberg-rust/issues/2580?utm_source=chatgpt.com "Deletion-vector writer + RowDelta action · Issue #2580"
