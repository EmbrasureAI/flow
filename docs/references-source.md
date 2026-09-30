# PostgreSQL capture and journal contracts

## Transport dependency

The PostgreSQL transport is the MIT/Apache-2.0 rust-postgres fork at
[`c4b8de06aaa99f71800126bedcca6e623d368357`](https://github.com/iambriccardo/rust-postgres/tree/c4b8de06aaa99f71800126bedcca6e623d368357),
pinned in `crates/pg-source/Cargo.toml` with a [local frame-admission patch](../vendor/tokio-postgres/LOCAL_CHANGES.md). It supplies authentication, TLS extension
points, PostgreSQL framing, binary COPY framing, and `CopyBothDuplex`. Our
`PostgresSource` boundary owns protocol-2 transaction semantics; changing the
transport need not change the journal, coordinator, or materializer.

## Idle publications and upgrades

Capture emits a transactional `pg_logical_emit_message` heartbeat immediately on
connect and every 30 seconds. The source role needs EXECUTE permission on that
PostgreSQL function. The heartbeat writes WAL metadata without changing source
rows or requiring a heartbeat table. Its enclosing commit is captured, journaled
and materialized through the ordinary ledger path before feedback advances.
Nontransactional messages and server keepalive WAL ends never advance progress.
This prevents an idle publication from retaining unrelated database WAL forever;
long source transactions or blocked materialization can still retain WAL.

The internal engine reads verified format-2 source schema records from the
previous Postgres-only engine, retaining their full column-incarnation proof.
The generic format-3 envelope is an in-memory compatibility view; existing records
are not rewritten. Unknown formats, truncated records and invalid incarnation
proofs fail closed. Preserve state, source slots and destination during upgrades.

## Standards that determine behavior

* [Logical replication parameters and flow](https://www.postgresql.org/docs/18/protocol-logical-replication.html):
  protocol 2 is available from PostgreSQL 14; `streaming='on'` permits interleaved
  transaction segments. We do not negotiate parallel apply or two-phase output.
* [Message formats](https://www.postgresql.org/docs/18/protocol-logicalrep-message-formats.html):
  ordinary Begin carries the **final** LSN, while Commit carries distinct commit
  and end LSNs. ACKs use end LSN. Streamed row messages include a subtransaction
  XID in addition to their current top-level stream transaction. Tuple `u` is an
  unchanged-TOAST marker, not NULL. All timestamps are converted from PostgreSQL's
  2000 epoch to the shared model's Unix epoch.
* [PostgreSQL's streamed apply behavior](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/replication/logical/worker.c):
  a subtransaction abort truncates the spool at its first change, removing
  descendant changes too. Filtering only matching XIDs would be incorrect.
  Our implementation uses bounded savepoint metadata and sequential segment
  truncation; no PostgreSQL implementation text is copied.
* [Logical decoding and exported snapshots](https://www.postgresql.org/docs/18/logicaldecoding-explanation.html):
  a slot may resend transactions after server recovery. A new slot's exported
  snapshot and consistent point establish one source-wide initial-copy boundary.
* [Streaming replication feedback](https://www.postgresql.org/docs/18/protocol-replication.html):
  the server's WAL end is not proof of local receipt, durability, or publication.
  A requested replay LSN is not an ACK proof either. Automatic keepalive replies
  remain at zero until the coordinator supplies verified watermarks.
* TOAST reconstruction: fetching missing values through a separate query races
  with concurrent source writes. Mutable capture requires FULL identity. pgoutput
  can still mark new tuple fields unchanged; capture reconstructs them only from
  the validated complete old tuple carried by the same update. Missing, malformed, key-only,
  or incomplete old tuples stop capture. No current-row query or placeholder is
  used.

## Implemented paths and verification

| Capability | Implementation and evidence | Remaining validation |
| --- | --- | --- |
| Source transaction boundaries | Protocol fixtures plus real PostgreSQL 14.24–18.6 contract suites | Longer production-shaped qualification |
| Multiple tables in one transaction | One envelope, sorted affected-table set, complete chunk references | Reader/catalog publication tests are separate |
| Primary-key changes | FULL before row supplies old canonical key; new row supplies replacement; real update/delete/key-move workloads across PostgreSQL 14.24–18.6 | Longer production-shaped qualification |
| Large streamed transactions | One bounded active buffer, disk spool, bounded XID/savepoint counts, terminal-only journal commit | Sustained throughput and source memory measurements |
| Nested stream abort | Real disk suffix truncation removes aborted parent and child changes | PostgreSQL nested-savepoint workloads |
| TOAST | Validated FULL old tuples resolve unchanged fields; unresolved values stop before a terminal record. Real 18.6 updates, key moves and deletes preserve 16 KiB text and 8 KiB binary values | PostgreSQL version matrix and larger configured row limits |
| Reconnect | Real pinned transport against a local scripted wire peer; interrupted uncommitted stream reconnects; journal deduplicates committed LSNs | Real failover and PostgreSQL slot rollback |
| Initial snapshot | Permanent slot export, imported snapshot with parallel binary COPY and concurrent CDC; real crashes before/after staging, nullable DDL between copy attempts, retained completed tables and index-loss recovery across PostgreSQL 14.24–18.6 | Larger copy and multi-hour recovery workloads |
| Nullable additive DDL | Durable schema registry, exact source catalog proof, stable Iceberg field IDs, historical projection; mixed-version/savepoint/abort/restart contracts on PostgreSQL 14.24–18.6 | Broader incompatible-DDL matrix |
| PostgreSQL 14–18 | Protocol 2, actual source contracts and bootstrap recovery on 14.24, 15.19, 16.15, 17.11 and 18.6 | Ongoing release matrix |
| Crash recovery | Real segment files with torn/CRC-damaged suffixes, XID reuse, reclamation checkpoint, exclusive writer and quota tests | Power-loss fault injection on deployment storage |

The Rust protocol and journal fixtures run without live services. The separate
[local service suite](../tests/local/README.md) and
[production fault suite](../tests/production/FAULTS.md) exercise PostgreSQL 18.6,
an Iceberg REST catalog and MinIO, with independent DuckDB comparisons. The
`source_versions.py` runner repeats the schema and bootstrap contracts on
isolated PostgreSQL 14–17 containers, recording image digests and the daemon
binary hash. This is functional conformance, not failover certification. Latency
and throughput qualification belongs to the measured benchmark reports.

## Journal durability and recovery contract

One journal directory belongs to one source/slot lineage. The `writer.lock`
exclusive lock prevents concurrent local owners. Payload chunks are opaque to the
journal; the source currently serializes `Vec<Mutation>` with bincode 1.3. The
journal framing is versioned separately so future payload migrations can fail
closed rather than silently reinterpret data.

Every frame has a 28-byte little-endian header:

| Bytes | Meaning |
| --- | --- |
| 0–3 | `FLJ1` magic |
| 4–5 | Format version, currently 1 |
| 6 | Chunk (1), legacy terminal (2), abort (3), range terminal (4), or counted terminal (5) |
| 7 | Reserved zero |
| 8–11 | Payload byte length |
| 12–19 | Monotonic sequence number |
| 20–23 | PostgreSQL transaction ID |
| 24–27 | CRC32 over the preceding 24 bytes and payload |

Segment rotation syncs the previous segment and directory; terminal commits sync
the current segment before advancing `durable_lsn`. Recovery checks bounds,
sequence, checksum, terminal references and commit order. Only the final segment
can hold an unsynchronized suffix, so recovery truncates a torn or corrupt frame
only there. Damage in any earlier segment is storage corruption: open fails with
`SegmentCorrupt` and modifies no segment file. `Journal::open_with_floor` also
refuses (`DurableTail`) to truncate a final-segment tail if that would lose a
transaction at or below a durable position the caller recorded independently;
the daemon passes the source ledger's journal durable LSN. No recovery path
deletes a segment. Unknown versions and record kinds stop recovery without
truncating the journal. Orphan transactions acquire abort markers before new
capture can reuse their XIDs. The returned `Recovery::truncated_bytes` must be
surfaced to the operator. In journaled ACK mode, damaged already-acknowledged
storage is a critical durability failure, not evidence that PostgreSQL can
resend the lost records.

Range terminals contain first/last chunk locations, count, payload bytes and an
ordered-reference checksum. This descriptor stays constant in size for a large
transaction. Capture copies each surviving spool transaction consecutively, so
normal replay scans sequentially without a per-chunk sidecar index. Interleaved
journal writers remain supported: replay validates skipped frames and filters by
XID, rejecting an intervening terminal or abort for that same XID.

Retained terminals use a rebuildable 32-byte index entry per commit, stored in
one file per journal segment. The index requires no separate sync. Its extent
and the durable LSN become visible together after the journal sync. Recovery
reconstructs missing or torn indexes from validated frames; cursor iteration
holds one transaction at a time. Segment pin counts and cursor-based reclamation
avoid both a backlog-sized transaction vector and rescanning row payloads for
acknowledgement. Index bytes are included in the disk quota. Legacy terminal
vectors remain readable within the configured frame limit; source-ledger entries
migrate on update to a marked descriptor format.

The source ledger registers a bounded journal page or completes a table's
publication page with one durable ControlStore write. That write atomically
includes changed descriptors, source watermarks and deletion of the completed
prefix. A failed validation changes none of them. Each page is capped by the
configured state batch size; later complete transactions never release an ACK
past an earlier incomplete transaction.

The manual real-RocksDB microbenchmark runs with:

```sh
cargo test --locked -p flow-coordinator --test ledger_batches \
  measure_durable_ledger_batches -- --ignored --nocapture
```

Reclamation persists a checksum-protected all-tables materialization watermark
with file sync, atomic rename and directory sync before removing whole segments.
Unmaterialized terminal records, referenced chunks and active transaction chunks
pin their segments. `JournalReader` permits independent immutable reads; the
coordinator must not reclaim a transaction while its readers still need it.

`sync_data` establishes the operating system/filesystem durability contract. It
does **not** establish independent durability against node or disk loss. Use
materialized ACK by default. Journaled ACK requires separately provisioned and
documented independent storage durability, plus recovery procedures for storage
corruption. Neither capture nor the journal automatically drops slots, advances
over missing transactions, or removes an existing source snapshot.

## Deliberate limits and performance choices

* Scalar mappings include bool, integers, floats, strings, bytea, finite dates
  and timestamps, and bounded decimals. [PostgreSQL type mappings](postgres-types.md)
  adds UUID strings, JSON/JSONB strings, JSON-encoded arrays, enum labels, domain
  base values, exact numeric strings, and published PostgreSQL 18 stored generated
  columns. Native `Uuid` remains available for existing non-Athena targets. No
  decimal-to-float conversion or silent numeric rounding is used.
* Snapshot copy uses the verified current publication projection. The publication
  must contain every configured table and may contain others, such as an
  administrator-owned `FOR ALL TABLES` or `FOR TABLES IN SCHEMA` publication.
  Capture drops other tables' relation metadata, rows and TRUNCATE entries before
  decoding; a transaction that only touched them commits as empty source progress.
  Configured tables must publish every current column without a row filter. Keep published
  operations and the configured tables' membership and column/filter settings
  fixed from slot creation through capture; pgoutput can omit changes after
  publication DDL without notifying the consumer. Setup, every capture reconnect
  and a running check about once a minute on capture's session verify this
  contract; each violation must be confirmed by a second read in a fresh
  repeatable-read snapshot. A violation that affects one configured table
  (missing from the publication, a row filter or column list on it, or the
  PostgreSQL 18 generated-column requirement) blocks only that table with
  `publication_changed`: capture drops its changes like an unconfigured table's,
  including rows an open transaction spooled before the latch, and its journaled
  transactions complete at its unchanged snapshot, so other tables keep
  publishing and acknowledging. A missing publication or an unpublished
  operation affects every table: capture stops and
  `state_dir/publication-resync-required.json` records the slot and reason, and every later
  start for that slot refuses until a resync replaces it. Neither kind clears
  when the settings are restored; only a resync does. During bootstrap every
  violation stays connection-wide. Source identity is proven on the replication
  connection and the SQL session before any verdict, so a connection that
  reaches a different system or database fails as an identity error, blocks no
  table and writes no marker. The check is best effort. It catches lasting
  changes, not a configured table removed and re-added between checks: pgoutput
  decides membership as of each change, so writes made while the table was out
  are never sent, and checks on either side of the gap both pass. Coordinate
  every publication change that affects
  configured tables with a resync. Incompatible table DDL and received TRUNCATE stop capture.
  A failed initial-copy slot is retained for operator diagnosis.
  Publication must include TRUNCATE messages so such a source operation pauses
  capture rather than disappearing silently.
* The daemon persists an `IDENTIFY_SYSTEM` proof before initial copy and verifies
  the system identifier, database, timeline, configured source identity and slot
  on every reconnect. Timeline changes currently require coordinated operator
  recovery. The slot must still use `pgoutput`, belong to the expected database,
  retain usable WAL, and have no ACK beyond the local durable journal.
* The source's one active mutation buffer batches rows into bounded, single-table
  chunks. Spool-only table/schema headers allow terminal journal assembly to
  copy surviving serialized bytes without decoding all transaction rows again.
  The existing fixed-width vector length supplies checked per-table mutation
  counts during that same replay. Counting after spool rollback excludes aborted
  savepoints; INSERT, UPDATE and DELETE each count once before collapse.
* Incomplete streamed transaction payloads live on disk. Savepoint lookup is
  constant-time; abort truncation touches only the abandoned suffix. Quotas and
  explicit open-transaction and savepoint limits turn overload into backpressure.
  Transaction chunk counts and retained terminal metadata are storage-backed;
  they do not impose an aggregate in-memory vector or COMMIT frame limit.
* Spool and journal writes are sequential and distinct. This deliberately spends
  additional local write bandwidth to make aborted-subtransaction filtering and
  durable terminal ownership simple. Revisit bypassing spool for ordinary small
  transactions only after profiling shows this path is material.
* All source connections reject an oversized advertised PostgreSQL frame after
  reading its header, before buffering the complete payload. This applies to COPY
  and replication traffic; the transport allowance includes protocol overhead.
  Decoded logical messages and retained chunks have separate admission checks.
  These limits bound individual frames and batches, not total process memory.

Journal terminal kind 5 and source-ledger envelope `FLLEDG03` persist these
counts. Readers explicitly decode older terminal kinds 2 and 4 and ledger
envelopes without a prefix or with `FLLEDG02`; no existing files are rewritten
as a migration. Older descriptors retain unknown counts and publish alone rather
than inventing a row estimate. Chunk payloads, segment indexes, source LSNs,
durability barriers and index-generation formats are unchanged. Older binaries
reject new terminal/envelope versions, so downgrade requires a compatible binary.

## Nullable additive schema transitions

Source configuration remains the initial schema contract. A durable source
registry records verified successors, PostgreSQL column numbers and physical
table identity before a transaction terminal can reference a new version.
The decoder can revisit older Relation messages after restart; mixed versions
within one transaction project their historical rows through the verified
nullable suffix before a single Iceberg row delta. Source schema versions are
independent of Iceberg schema IDs, and existing Iceberg field IDs never change.

Only committed source metadata authorizes schema publication. The source checks
the SQL catalog on changed Relation messages at commit and on its five-second
health tick, so idle additions also reach Iceberg without inventing a source
watermark. PostgreSQL versions that emit an empty DDL transaction can advance
through its actual durable journal terminal. Savepoint rollback and streamed
transaction abort discard provisional
schemas with their abandoned rows. The table actor serializes the Iceberg schema
update before publishing affected data, and reloads metadata to resolve a lost
catalog response.

The SQL proof checks actual primary-key columns, nullability, generated columns,
defaults and missing-column values. PostgreSQL may store a constant default in
`attmissingval` without rewriting old tuples; dropping that default later does
not remove the backfill semantics. Such additions require resynchronization.
See the official [column catalog](https://www.postgresql.org/docs/18/catalog-pg-attribute.html)
and [ALTER TABLE behavior](https://www.postgresql.org/docs/18/sql-altertable.html).
Only nullable additions with no default or a literal NULL default are accepted.

Stored column numbers catch a dropped and re-added column even when its name and
type are unchanged. Physical table rewrites also stop capture: a volatile
default can rewrite existing rows without equivalent pgoutput row events, and
the current catalog alone cannot distinguish that history from a harmless
rewrite. This deliberately includes `VACUUM FULL`, `CLUSTER` and similar source
storage rewrites; operators must resynchronize before capture resumes. Schema
record format 2 requires these proofs and explicitly rejects earlier development
records that lack them.
