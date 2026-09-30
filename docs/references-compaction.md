# Index and maintenance contracts

## Storage contracts

| Dependency contract | Implementation requirement |
| --- | --- |
| [Iceberg specification](https://iceberg.apache.org/spec/) | Physical identities are path plus zero-based row ordinal. Position deletes have sequence applicability rules and must remain valid after rewrites. |
| [Iceberg Java RewriteFiles contract](https://iceberg.apache.org/javadoc/latest/org/apache/iceberg/RewriteFiles.html) | Rewrites preserve the live row set. Validate reads against their base snapshot; surviving input files alone do not prove stale output safe. |
| [RocksDB basic operations](https://github.com/facebook/rocksdb/wiki/Basic-Operations) | One WAL-synced batch publishes forward/reverse index changes and operation progress together. Reads followed by a write batch require serialization to implement CAS. |
| [RocksDB tuning guide](https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide) | Shared bounded block cache, Bloom filters, bounded memtables, batched writes, and scan cache bypass are initial choices. Hardware-dependent tuning requires measurements. |
| [RocksDB checkpoints](https://github.com/facebook/rocksdb/wiki/Checkpoints) | Native checkpoints capture all column families consistently and can hard-link SSTs on the same filesystem. |

## Index activation constraints

The pinned RocksDB 10.4.2 C++ API documents multi-column-family external-file
ingestion as all-or-none for manifest recovery, while warning that concurrent
reads can observe a mixture across column families unless they use a snapshot
([`DB::IngestExternalFiles`](https://github.com/facebook/rocksdb/blob/410c5623195ecbe4699b9b5a5f622c7325cec6fe/include/rocksdb/db.h#L1990-L2006)).
The pinned `rust-rocksdb` 0.24.0 API exposes only
[single-column-family ingestion calls](https://github.com/rust-rocksdb/rust-rocksdb/blob/bb7d2168eab1bc7849f23adbcb825e3aba1bd2f4/src/db.rs#L2282-L2357).
Sequentially ingesting SSTs into the live forward, reverse and count column
families can therefore expose or recover only part of one logical transition;
it cannot replace their atomic write batch. Current RocksDB main has a separate
[prepare/commit ingestion API](https://github.com/facebook/rocksdb/blob/37234200b57d8d0a6a5c41f2d9811bbd2e293544/include/rocksdb/db.h#L2396-L2445),
but that API is not present in the pinned engine or Rust binding.

The default index apply batch and lookup chunk size is 1,024 rows. Each apply
batch retains the atomic transition across index directions, counts and progress.

## Decisions

The state store has one process owner. Its row mutex covers both CAS reads and
batch writes. RocksDB prevents opening the same database for writing twice.
Complete SOURCE-ledger batches on a controlled store use the separate control
commit mutex, which orders both database revisions and fences failures before
unlocking. They do not wait for unrelated row/spool processing. Checkpoints hold
row then control locks; readers use consistent index snapshots. Standalone stores
retain the row lock for ledger writes.

Prepared deltas live as separate bounded records, not a single transaction-sized
value. Applying a committed delta is resumable in bounded batches; the table stays
blocked until its final completion marker is durable. Each batch atomically changes
both index directions, per-file live-row counts and the apply cursor. Reopening cannot expose a table as ready
with a partially applied index. Large transaction collapse also uses per-key disk
entries and bounded scans.

Compaction planning is deterministic: debt determines urgency and bounded
size-tiered groups prioritize older files. Data selection does not expand through
shared-delete targets. The worker stages every applicable delete once, applies
positions to selected data, and writes residual positions for surviving files.
This implements the specification's residual-rewrite alternative to full closure;
an L0 pass need not read or rewrite its unchanged L1/L2 base.

Delete dependencies include encoded bytes and row counts. The default data-rewrite
budget admits at most 32 delete inputs, 32 MiB and one million positions, in
addition to the data-file budget. Actual object sizes and Parquet counts are
checked during scanning. A budget failure keeps publication backpressured. The
residual writer streams the disk-sorted set without consulting surviving rows in
the live index or rereading their data files. It preserves original applicability
before assigning residual output the maximum input delete data sequence.

Data and residual files share one exact-base Rewrite operation and prepared
artifact plan. Only selected data rows produce conditional index deltas. The
existing ownership reservations, lost-response resolution, index-loss recovery
and retained-snapshot GC protections cover both output kinds.

Delete-file fanout first triggers bounded delete-only consolidation when data
deletion density is low. The worker reads reserved path/position columns,
preserves original sequence applicability, removes dangling paths and spills
sorting/deduplication to scratch storage. The exact-head replacement uses the
existing prepared operation, artifact ownership and recovery path with zero row
index deltas. A fully dangling singleton can be removed without writing output;
a group that cannot reduce file count creates no replacement snapshot.

Deletion density uses exact live-row counters updated atomically with forward
and reverse index entries. Shared delete-file record counts are not charged to
every possible target. Counter reads require the same unfenced table snapshot;
older index formats rebuild from catalog authority before using these counters.
Both applicable fanout and total delete-file count contribute to reader debt,
so dangling files cannot accumulate outside the maintenance policy. Rebuilding
derived counts preserves control authority and the permanent source slot.

External data rewrites are accepted only after comparing every live key and its
canonical fingerprint. A snapshot's `replace` operation label is a hint, not proof
of logical equivalence. Unknown history, unverified schema changes, unexpected inserts,
missing rows, and changed values pause publication.

Keyless append-only tables use a disk-backed multiset join ordered by canonical
fingerprint and physical location. Equal rows are matched deterministically to
their original synthetic identities; duplicates are neither collapsed nor
invented. This reuses the same prepared index transition and recovery fence as
keyed reconciliation.

New service data filenames carry `flow-l0-`, `flow-l1-`, or `flow-l2-` hints.
These survive snapshot expiration and index rebuilding; file size alone does
not demote a compacted idle file back to L0. Legacy service layouts remain
recognized. Unknown external outputs start at L1, and undersized files outside
L0 still contribute to bounded file-count maintenance regardless of their level
hint. A compacted singleton does not repeatedly rewrite just because it is small.

## Durability barriers and staging cost

Derived transaction-collapse entries, sorted delete entries, and Building-phase
index deltas keep the RocksDB WAL enabled but use `sync=false`. The durable source
journal can recreate unfinished work. On a standalone store, `seal_transaction`
and `seal_prepare` synchronize the index WAL under the serialized writer lock.
The pinned RocksDB 10.4.2
[`WriteToWAL` implementation](https://github.com/facebook/rocksdb/blob/v10.4.2/db/db_impl/db_impl_write.cc)
synchronizes all live log files in order for a synchronous write, including log
rotation during a large staged transaction.

Controlled stores instead write the index batch without a sync, then synchronize
the separate control database with the same revision and authoritative operation
record. This does not synchronize the index WAL. Reopening checks revision
equality; missing derived index writes require reconstruction from control and
catalog authority before publication or ACK. The index retains its WAL and atomic
flush configuration for consistency across column families, as described in
[RocksDB's atomic-flush contract](https://github.com/facebook/rocksdb/wiki/Atomic-flush).
Both modes avoid a disk barrier for every small staging chunk while preserving
their distinct recovery boundaries.

Table fences, completed preparations, catalog commit proofs, final index apply,
source ledger updates and cleanup boundaries retain these durable transitions.
Intermediate index apply batches keep their row mappings, live counts and cursor
atomic in the WAL without a separate sync. The final batch advances the table
watermark and clears its fence using the standalone WAL barrier or the controlled
revision protocol above. A process crash also
preserves ordinary non-sync WAL appends on supported POSIX filesystems; a machine
crash may lose unfinished derived work, which is discarded and replayed.
See [RocksDB synchronous and non-sync writes](https://github.com/facebook/rocksdb/wiki/Basic-Operations#synchronous-writes).

## Catalog JSON ownership

Catalog-generated JSON is independent of snapshot expiration. Before publication
replaces a catalog pointer, register that immutable JSON with the manifest attempt.
Schema and expiration-only commits register the same pointer durably before commit.
Only flat children of `{location}/metadata/` are registered. JSON a catalog
writes elsewhere (such as under `write.metadata.path`) stays catalog-owned and
is skipped, never collected.
A durable per-object clock requires a full grace after first observing an
unreferenced file, independently of upload/fence age. Retained references reset
that clock. JSON uses `limits.metadata_json_grace_secs`, other objects
`limits.orphan_grace_secs`. Check absence before deleting obsolete siblings of a
partially live owner, so repeat sweeps do not create endless S3 delete markers.

Current metadata and all catalog metadata-log entries are protected by GC; empty
metadata logs (including Glue REST) do not disable cleanup. JSON-only registry pages
avoid scanning manifests. `gc.enabled=false` disables all physical collection.

New ownership records use `owned-artifacts/v2/`. The collector reads v1 and v2
within the same bounded page and cursor. Old engines cannot safely classify JSON,
so rollback must leave v2 records unread rather than delete their objects. After
its first examination a record moves to the due queue
`owned-artifacts/v2/{table}/~/{due_ms}-{id}` with an unchanged value, so a
release that predates the queue still reads and conservatively collects it after
a rollback. The sweep cursor moved to `artifact-gc/v2/`; each new sweep deletes
the obsolete `artifact-gc/v1/` cursor. The record format and
durable control/index ownership protocol are otherwise unchanged.

Pre-registry JSON can be adopted with `metadata-import --inventory <ndjson>` while
the daemon is stopped and supervisor desired state is paused. Entries contain
`table_uuid` and `path`. The command checks frozen source/table identities, flat
metadata paths, each object's UUID/location/timestamp and a 16 MiB read limit.
Without `--apply`, it only validates. Apply records ownership; collection gives
adopted JSON the full `limits.orphan_grace_secs`, not the shorter metadata JSON
grace, because secondary catalogs or readers may still use it by location. The
grace starts when collection first observes the JSON unreferenced. Import never
deletes objects or commits catalog changes. Import is
idempotent and refuses a live daemon's state lock. Resume the normal source after
bounded batches, keeping upstream WAL within its retention headroom.

`flow_garbage_metadata_json_delete_requests_total` reports successful JSON delete
requests. Versioned object stores can retain noncurrent versions after deletion;
physical storage reclamation requires a separately scoped version-retention policy.
