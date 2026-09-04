# Index and maintenance references

Reviewed 2026-09-03. Implementations in this repository were written independently.
Moonlink source code was not copied, translated, or included as a dependency.

| Primary reference | Design consequence |
| --- | --- |
| [Moonlink README](https://github.com/Mooncake-Labs/moonlink/tree/401c1d1ad5354f77bf64cbc4f008c4e62a16781d) | A physical row index and asynchronous file compaction address the cost of updates. The freshest public state here is always a standard Iceberg snapshot. |
| [Moonlink chaos-test design](https://github.com/Mooncake-Labs/moonlink/blob/401c1d1ad5354f77bf64cbc4f008c4e62a16781d/src/moonlink/src/table_handler/README.md) | Exercise transaction state, aborts, reopening durable state, and maintenance interleavings. Our concurrent-delete regression deliberately extends beyond quiescent maintenance tests. |
| [Moonlink license](https://github.com/Mooncake-Labs/moonlink/blob/401c1d1ad5354f77bf64cbc4f008c4e62a16781d/LICENSE) | BSL 1.1; architecture documentation only. No Moonlink implementation is used. |
| [Iceberg specification](https://iceberg.apache.org/spec/) | Physical identities are path plus zero-based row ordinal. Position deletes have sequence applicability rules and must remain valid after rewrites. |
| [Iceberg Java RewriteFiles contract](https://iceberg.apache.org/javadoc/latest/org/apache/iceberg/RewriteFiles.html) | Rewrites preserve the live row set. Validate reads against their base snapshot; surviving input files alone do not prove stale output safe. |
| [Apache Hudi indexes](https://hudi.apache.org/docs/indexes/) | Resolve changed keys through a durable location index instead of scanning all data. Unlike Hudi file-group identities, Iceberg physical positions change on every rewrite. |
| [Apache Paimon primary-key tables](https://paimon.apache.org/docs/master/primary-key-table/) and [Iceberg interoperability](https://paimon.apache.org/docs/master/iceberg/primary-key-table/) | Bounded compaction debt is useful, but reader-side key merging is outside our read contract. Every L0 publication already resolves mutations using standard position deletes. |
| [RocksDB basic operations](https://github.com/facebook/rocksdb/wiki/Basic-Operations) | One WAL-synced batch publishes forward/reverse index changes and operation progress together. Reads followed by a write batch require serialization to implement CAS. |
| [RocksDB tuning guide](https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide) | Shared bounded block cache, Bloom filters, bounded memtables, batched writes, and scan cache bypass are initial choices. Hardware-dependent tuning requires measurements. |
| [RocksDB checkpoints](https://github.com/facebook/rocksdb/wiki/Checkpoints) | Native checkpoints capture all column families consistently and can hard-link SSTs on the same filesystem. |

## Prepared-state activation references

These sources support the boundary between background preparation and exact
activation; they do not establish benchmark equivalence, and no source code from
these projects is copied or translated here.

[Moonlink compaction](https://github.com/Mooncake-Labs/moonlink/blob/401c1d1ad5354f77bf64cbc4f008c4e62a16781d/src/moonlink/src/storage/compaction/compactor.rs#L240-L381)
builds the surviving-row location map and replacement global index as compaction
output. Its snapshot owner then
[installs that prepared result and reconciles deletion logs](https://github.com/Mooncake-Labs/moonlink/blob/401c1d1ad5354f77bf64cbc4f008c4e62a16781d/src/moonlink/src/storage/mooncake_table/snapshot_maintenance.rs#L315-L427)
that arrived after the build. This supports doing physical remapping and index
construction off the table lane, followed by one owner-controlled catch-up and
activation boundary. It does not support moving a row-by-row live-index rewrite
into the activation path.

RisingWave Hummock compaction creates immutable SST outputs. The meta service
represents activation as a
[small version delta that removes input SSTs and adds outputs](https://github.com/risingwavelabs/risingwave/blob/c351d73d4a33dacb824287a7d5b839e04dccf838/src/meta/src/hummock/manager/compaction/mod.rs#L147-L194),
then validates the assignment and compaction-group version and
[commits the affected metadata together](https://github.com/risingwavelabs/risingwave/blob/c351d73d4a33dacb824287a7d5b839e04dccf838/src/meta/src/hummock/manager/compaction/mod.rs#L821-L1007).
Readers retain
[pinned immutable versions](https://github.com/risingwavelabs/risingwave/blob/c351d73d4a33dacb824287a7d5b839e04dccf838/src/storage/src/hummock/local_version/pinned_version.rs#L40-L155)
while newer state is published. The relevant property for this state store is
that activation scales with the number of prepared artifacts, rather than the
number of remapped rows.

Materialize Persist gives each background merge result its
[exact input identity and output](https://github.com/MaterializeInc/materialize/blob/6cef38c018b3b235e1fd937f493aa498c58c9542/src/persist-client/src/internal/trace.rs#L76-L87).
Application checks the expected frontier and input range and can classify an
[exact, subset, stale or invalid result](https://github.com/MaterializeInc/materialize/blob/6cef38c018b3b235e1fd937f493aa498c58c9542/src/persist-client/src/internal/trace.rs#L1184-L1283)
before a small
[compare-and-set state transition](https://github.com/MaterializeInc/materialize/blob/6cef38c018b3b235e1fd937f493aa498c58c9542/src/persist-client/src/internal/apply.rs#L357-L499).
An indeterminate commit preserves an output that may have become referenced
[until later reconciliation](https://github.com/MaterializeInc/materialize/blob/6cef38c018b3b235e1fd937f493aa498c58c9542/src/persist-client/src/internal/machine.rs#L1055-L1108).
This is the reference for self-identifying external-compactor artifacts, exact
base validation, idempotent activation outcomes and conservative orphan cleanup.

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

The [measured apply comparisons](../target/apply-batch-decoupled-profile-33/assessment.md)
reject larger live-index `WriteBatch` commits. The implementation can now hold
PK and reverse lookup chunks at 1,024 rows while building a larger atomic commit,
and it records exact serialized bytes, operations, owner reads and phase timing.
At 4,096 and 8,192 commit rows, owner reads stay fixed and resource limits pass,
but the 100,000 and one-million-row gains remain below their declared gates.
The production default therefore remains 1,024.

The next candidate is immutable, generation-prefixed index state prepared under
an unreachable prefix, followed by exact catch-up and one small durable
active-generation switch. Old generations must remain pinned for readers and be
collected only after reconciliation. That larger change requires its own recovery
and service measurements; results from Hummock, Persist or Moonlink do not qualify
its performance here.

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
so dangling files cannot accumulate outside the maintenance policy. The real
[format-2-to-3 upgrade gate](../target/production-upgrade-01/report.json) rebuilt
those derived counts without replacing control authority or the permanent slot.

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

## Measured follow-up

[Matrix04](production-qualification.md#delete-consolidation-follow-up) measured
delete-only consolidation and exact density on release06. Its 1k run used six
data rewrites and 19 delete passes, with 30.209 MB returned by storage reads and
21.643 MB accepted by streaming writes, down from 101.246 MB and 78.526 MB in
matrix03. Both measured rates still failed latency targets. Residual data/delete
rewriting was implemented after that binary; its focused integration checks do
not constitute a new throughput or latency result.

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
