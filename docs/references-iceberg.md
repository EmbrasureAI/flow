# Iceberg transaction foundation

The implementation is pinned to Apache `iceberg` **0.10.1**, release commit
`04ae06bdb15a6fd7c7927d29d4e0f6a33de0f1f9`. It uses Arrow and Parquet 58 and
requires Rust 1.94 or newer. The release crate is vendored under `vendor/iceberg`
with its Apache license, NOTICE, provenance, and tests intact.

The [0.10.1 transaction API](https://rust.iceberg.apache.org/api/iceberg/transaction/index.html)
has fast append but lacks RowDelta and RewriteFiles. Its action trait and
`TableCommit` construction are private. The local
[upstream patch](patches/iceberg-table-commit.patch) exposes the existing typed
commit builder, adds snapshot-reference accessors, and allows expiration to
protect explicit snapshots and a minimum reader window. Publication preserves
the user's main-branch retention policy. This is a small extension seam, not
a replacement Iceberg metadata implementation. No upstream submission has
been made.

The separate [delete-cache patch](patches/iceberg-delete-cache.patch) fixes a
reader sequence-isolation defect found by the integration suite. A shared
delete file can contain positions for a data file newer than that delete's
sequence. The planner correctly excludes that delete from the newer file's
task, but the upstream cache previously merged positions globally by data path.
The cache now retains input-delete identity and combines only the task's
applicable inputs. It also registers positional-delete completion waiters before
releasing the state lock. The complete stock-reader regression remains in the
delete-rewrite integration suite.

The [sequence-isolation patch series](patches/upstream/README.md) targets
upstream commit `6fa8e03834dca17481441ec758afbbe12efc27f6` and includes a native
Parquet regression. It adapts to upstream's newer V3 reader and excludes the
already merged lost-wakeup fix. The series documents its base and reproduction
commands. It does not alter the vendored runtime; no upstream submission has
been made.

## Standards and reference behavior

The [Iceberg format specification](https://iceberg.apache.org/spec/#sequence-numbers)
defines inherited data/file sequence numbers. `RowDeltaAction` adds data and
position deletes in a single snapshot. Both inherit the new commit sequence;
existing and removed entries preserve their original sequence numbers.
`RewriteFilesAction` assigns replacement data the read snapshot's data sequence while
its file sequence records the actual commit. Position deletes use the
inclusive `data_sequence <= delete_sequence` rule, matching partition and
file path. Path bounds only exclude impossible matches; absent bounds are
treated conservatively.

Delete-only rewrites filter each input at its original sequence, deduplicate
effective positions on disk, and assign output the maximum selected input delete
data sequence. The output file sequence inherits the new commit. Exact-head
validation is mandatory for this action. This follows Java's
[position-delete commit manager](https://github.com/apache/iceberg/blob/main/core/src/main/java/org/apache/iceberg/actions/RewritePositionDeletesCommitManager.java).

Mixed data/residual rewrites use `with_residual_deletes` and also require the
exact base. The worker reads all deletes applicable to selected data, preserves
their effective positions for unselected surviving files, and publishes those
residuals alongside replacement data. Residual delete data sequences use the
same maximum-input rule; both output kinds inherit the new file sequence.
Unselected data files and unrelated delete files retain their original metadata.
This avoids expanding a minor rewrite into every shared-delete target.

Java's [RowDelta](https://iceberg.apache.org/javadoc/1.11.0/org/apache/iceberg/RowDelta.html),
[RewriteFiles](https://iceberg.apache.org/javadoc/1.11.0/org/apache/iceberg/RewriteFiles.html),
and [rewrite validation tests](https://github.com/apache/iceberg/blob/main/core/src/test/java/org/apache/iceberg/TestRewriteFiles.java)
informed the validation boundary. We validate ancestry, input existence and
identity, schema/spec stability and exact applicable delete state. A data-only
action can remove a shared delete only when all potential live targets are
consumed. The explicit mixed action instead requires caller-produced residuals
and fences publication to the exact head. Generic data-only rewrites check
intermediate snapshots to catch changes that
appeared and disappeared after a worker's read. A stale rewrite is rejected;
the caller must replan its output.

The final catalog request includes table UUID, main snapshot, current schema,
and default spec requirements. A race after validation fails the catalog
compare-and-swap. `commit` does not silently rebase stale worker output or
blindly retry ambiguous failures.

## Recovery and ownership

Every publication has a `flow.operation-id` snapshot-summary marker. Recovery
searches retained ancestors, including snapshots older than the current head.
A marker on a detached branch does not count as successful publication. If
history no longer proves the prepared operation's ancestry, publication stops
for recovery rather than risking duplicate mutations. Retention must preserve
unresolved prepared operations and compaction bases.

Prepared artifact metadata uses the library's standard Avro manifests and
manifest list. `write_artifact_plan` writes the list last; the journal/state
store persists that path together with the serializable `CommitBase` and
operation identity. `read_artifact_plan` reconstructs real `DataFile` values
without maintaining a parallel serialization schema. Paths must be immutable
and unique; partial plans and failed commit output become orphan candidates
after the configured retention grace period.

The action validates metadata, not Parquet payloads. Its caller must supply
complete durable files, include every referenced delete target in validation,
and apply all worker-visible deletes before preparing rewrite output. Shared
delete files may remain live; removing one requires consuming every target or
using the explicit residual mode to preserve all effective survivor positions.
The coordinator stages both data and delete outputs in the same native artifact
plan. Its existing optional delete sequence distinguishes these prepared mixed
rewrites from ordinary data-only plans; no new operation kind or recovery
protocol is needed.

## Performance decisions and current limits

Append publication reuses unchanged manifests and writes separate data/delete
manifests without merging old ones. Rewrites touch only manifests containing
removed files. Manifest loading has bounded concurrency (16). A coordinator
should reuse one `ManifestCache` across actions; its default estimated decoded
budget is 64 MiB. Immutable entries use `Arc` so views and caches share file
metrics instead of deep-copying them. The cache key includes table UUID and
the manifest's inherited sequence/snapshot context.
Exact delete targets are indexed once per view, avoiding an all-files scan per
data file during health planning. Shared deletes retain conservative matching.

Validation still traverses the live metadata view; it is not constant-time
in table file count. Prepared plans currently write metadata separately from
the committed manifests. Reusing those manifests and incrementally maintaining
the validated view are future optimizations, contingent on profiling. No
latency or throughput claim is inferred from local tests.

The service supports unpartitioned v2 and v3 tables. It rejects v1 writes,
partitioned or mixed-spec tables, encrypted writes, and equality-delete state.
See [Iceberg v3](iceberg-v3.md) for deletion vectors, row lineage and upgrades.
The integration tests use
the real in-memory catalog and Avro readers/writers, including the stale-worker
regression, recovery markers, sequence inheritance, shared-delete safety,
expired bases, schema changes, and branch-retention preservation. They do not
replace the [Spark/Trino/DuckDB interoperability suite](../tests/production/reader_fixtures/README.md)
or [live-service tests](../tests/production/README.md). Performance results apply
only to the binary and environment recorded in each [benchmark](performance.md).

## Connected maintenance

`TableMaintenance` connects the planner and worker to durable prepared rewrites,
validated catalog publication, and index compare-and-swap. External snapshots
are processed in ancestry order. Delete-file rewrites compare effective
position sets; data rewrites additionally compare live keys, row fingerprints,
and cardinality. Survivors' delete sets must remain unchanged. Unsupported
logical changes leave publication paused.

Full index rebuild scans ordinary Parquet and applies standard position deletes
into a fresh RocksDB store, then verifies the catalog head before making the
replacement ready. The caller supplies the source watermark from its durable
ledger and installs the replacement before resuming writes. Keyless append-only
rows receive unique physical identities; external data-rewrite reconciliation
matches their row multisets on disk. Duplicate values retain their multiplicity,
and each matched row keeps its original synthetic identity, source LSN, and
logical version. A changed duplicate count is a logical mutation even when the
total row count is unchanged.

`RewriteManifestsAction` follows the standard
[manifest-entry rules](https://iceberg.apache.org/spec/#manifests) and Java's
[RewriteManifests contract](https://iceberg.apache.org/javadoc/latest/org/apache/iceberg/RewriteManifests.html).
It merges bounded groups of small manifests separately for data and deletes,
discards dead entries, and preserves each live file's original snapshot ID,
data sequence, file sequence, and `DataFile` metadata. The defaults trigger at
64 manifests and bound a pass to 64 inputs, 32 MiB of encoded input, and 100,000
entries; the target output size is estimated from input Avro lengths. It keeps
untouched manifests and commits an exact-base `replace` snapshot. The coordinator
owns every deterministic output path in a durable Building record before any
upload, seals a zero-delta prepared operation after writing, and retains the
table's source watermark. Recovery after catalog success applies that metadata
transition without adding another snapshot.

History expiration uses upstream's action and reference rules, protects the
reader window, preserves existing references, and retains the descendant
history of pending operations and supplied worker/checkpoint bases. An unresolved
initial operation suppresses expiration. It removes only metadata references;
physical orphan cleanup remains a separate delayed operation.

## Physical artifact collection

Every service data/delete upload, prepared-plan manifest, and catalog action's
manifest/list is registered before its first PUT. Data/delete names use finite
ordinal reservations of 64 files; the initial reservation shares the durable
Building barrier and the final counts share the Prepared barrier. Metadata
attempts register one compact range before writing. Recovery after complete
index loss registers new metadata directly in the independent control store,
so a replay cannot bypass ownership. Catalog-created metadata JSON belongs to
the catalog and is outside this registry.

The collector follows Iceberg's
[retention and orphan-file guidance](https://iceberg.apache.org/docs/latest/maintenance/#delete-orphan-files):
retain files needed by snapshots and allow a grace period for readers and
interrupted work. Our candidate set consists only of registered exact paths;
external files are never discovered or deleted by a warehouse-wide sweep.
All retained snapshots, including branches and tags, protect their manifest
lists, manifests, and live data/delete files. Pending operation namespaces and
registered checkpoint/worker snapshots add protection. A missing protected
snapshot stops collection. Tombstones in a newer manifest do not keep expired
physical inputs alive once every snapshot that actually used them is gone.

Registration keeps writing each record under an unordered key. A sweep
examines every unordered record once and then moves it into a due queue whose
key begins with the next time any of its objects can change state: when a
running grace ends, or a recheck of a still-referenced object (half the
record's age, between one minute and one hour, never longer than the grace).
The queue is read only up to the time the sweep started, so a sweep stops at
the first record that is not yet due. Its cost follows new registrations and
due work, not the size of the registry. Queue keys stay in the `v2/` namespace;
records from older releases, including random UUID keys, are read and migrated
by the same sweep.

Reachability comes from an in-memory index of the retained snapshots: each
manifest list, manifest and live data/delete path, reference-counted by
manifest-list path. Manifest lists and manifests are immutable, so each is
read once while it is retained, not once per page; a page reads only lists
committed since the previous page and releases expired ones. The index stores
64-bit path hashes; a collision can only retain an orphan. It is rebuilt after
a restart. All indexes share an estimated 128 MiB budget; a table whose index
alone exceeds it intersects each page's candidates with a full manifest walk,
as earlier releases did.

Each object has one grace clock. It starts when a sweep first observes the
object unreferenced by retained metadata and by protected operations, builds
and checkpoints, and resets whenever the object is observed referenced again.
Observation happens no earlier than the object actually became unreferenced,
so a reader that planned an expired snapshot, or an interrupted upload whose
fence was released, always has the full grace. Creation age does not count:
a data file live for a month still gets the full grace after its snapshot
expires. `limits.orphan_grace_secs` (24 hours by default) must cover the
longest planned reader and interrupted-upload lifetime. Catalog metadata JSON
has its own `limits.metadata_json_grace_secs`, 600 seconds by default (never
more than the orphan grace unless set). The JSON is protected while it is the
current pointer or in the catalog metadata log, and readers load the current
pointer, so the risk of the shorter grace is limited to a client that resolved
a pointer and fetched it only after the JSON left the log, or that stores
metadata file locations for later use. Iceberg's own
`write.metadata.delete-after-commit.enabled` deletes such files without any
grace. Raise the setting if your readers retain metadata locations. A lowered
grace applies to already-queued records when they next become due.

Each page is bounded by registry records, candidate objects, deletions (64 by
default, each preceded by an existence check, 16 concurrently) and a 250 ms
budget for selection and index reads. It refreshes the complete catalog
metadata before starting clocks or issuing deletions and writes all registry
changes in one durable batch. Per-record cursors and the sweep cursor survive
restart; a record remains until all of its objects are deleted or absent.
Collection runs on the serialized table actor and yields to CDC between pages.
External writers must not resurrect expired or unpublished service paths.
There is no distributed lease or cross-service garbage protocol in this
profile.

The local integration suite uses real Parquet and Avro, updates/deletes,
compaction, checkpoint readers, failed partial uploads, complete index loss,
small cursor budgets, and reopening durable state. A registration-failure gate
also verifies that data, deletes, and metadata remain absent when ownership
cannot be persisted.
