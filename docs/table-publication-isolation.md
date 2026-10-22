# Table publication isolation

Flow schedules CDC publication independently for each table while retaining one
source transaction ledger and one acknowledgement frontier. A classified catalog
or object-storage publication failure blocks that table, releases its worker, and
leaves its changes available for replay. Other tables remain eligible for work.

## Durable authority

The journal remains the source of row payloads. The source ledger stores each
transaction descriptor once, including its affected tables and their completed
publications. A compact reference index maps `(source, table, transaction end)` to
that existing descriptor. Registration inserts references atomically with ledger
entries; table completion removes its references atomically with completion proof.

This separates table admission from the source's oldest unfinished transaction.
A healthy table can seek its next reference directly without scanning another
table's retained history. These SOURCE records are mirrored through the
authoritative control store and survive index-generation recovery. They do not
introduce another payload journal.

Queued descriptors share the configured `pending_transactions` lookahead.
Admission rotates across eligible tables, including when there are more tables
than available descriptors. Queued work only makes a table schedulable: a
dispatched epoch leaves the lookahead and continues in order through that table's
durable references, up to the 32 MiB payload limit, a fixed per-epoch
descriptor-memory limit and the optional `limits.epoch_max_transactions` cap on
source transactions per epoch, so a busy table does not hold lookahead another
table needs. A failed or deferred worker must return before its queued and running
reservations are evicted; its epoch is reloaded from the ledger. Eviction removes only volatile admission:
restarting the cursor at zero finds exactly the references that still need work.
It neither completes a transaction nor advances an acknowledgement.

## Failure and recovery

A durable blocked record contains the table, a bounded error code, timestamps,
attempt count, retry deadline, and any unfinished operation identity. Raw remote
error text is not copied into status records. Retry delays hold no worker or
descriptor reservation. Due retries alternate with ready CDC so a group of
unavailable tables cannot monopolize publication lanes. The existing per-table
maintenance and operation fences remain in force.

An ambiguous catalog commit retains its prepared operation, artifact identities,
and table fence. Recovery resolves that original operation before admitting new
publication. The runtime reconciles an applied operation's table references
through its original `last_lsn` in bounded pages, then retires the operation.
It must not infer that a failed request means the catalog rejected the commit.
Incomplete Building operations can be discarded because they were never eligible
for publication. Workers holding independent build resources must join before
those resources are released.

Durable no-op table progress also recovers without an Applied operation. Startup
reconciliation seeks each pending table prefix and compares references with its
materialized position, while retaining the conservative fence on unfinished
operations. It does not walk healthy tables' already-completed retained history.

Known source-table schema/row errors also isolate an established table identity.
Compatible required-to-nullable transitions evolve the existing Iceberg schema,
preserving field IDs, key bytes, and old files. New nullable source versions are
proved at commit before publication; streamed NULL rows may select a provisional
decoder while the committed catalog proof is still pending.

Incompatible names/types, selected-column loss, primary-key/replica-identity drift,
row decoding failures, UPDATE or DELETE on an `append_only` table, a single row
change larger than `limits.chunk_bytes`, and TRUNCATE latch a source-table block
(`source_schema_incompatible`) in the authoritative control store; the logged
reason names the cause. From then on capture drops the table's changes, see
[blocks that require a resync](#blocks-that-require-a-resync). Commit proof
failures block the table the same way. Inside a streamed, still-uncommitted
transaction these decisions are provisional: the change is quarantined in that
transaction's spool, other transactions keep publishing the table, and the
block is latched only if the transaction commits with it; a rollback or
savepoint rollback discards the decision with the change. Quarantined changes
stay in that transaction's spool and never reach the journal. Row limits
reserve room to wrap any admitted decoded row as quarantine. Only raw wire
evidence larger than one chunk (text-encoded row images, or the row that
exceeded `limits.chunk_bytes` itself) is replaced by a bounded marker recording
its table and size. Incompatible DDL that commits after all of its streamed
rows were rolled back to a savepoint is blocked by the table's next Relation
message or the five-second catalog refresh rather than at its own commit,
still before any row of the new shape is published. `ADD COLUMN ... DEFAULT`
blocks even on an empty table, because PostgreSQL still records a missing
value for it; add the column without a default to avoid that. Explicitly
excluded cells are projected out before quarantine, including when a selected
column disappears.

A source-table block persists across restart and source repair. Use an explicit
resync/replacement to establish new authoritative state; never clear the block or
discard journal records manually. A [resync](operations.md#resynchronize-a-source)
replaces the whole connection.

Another writer's commit to a target is reconciled when it only rewrites files
(`replace` snapshots that preserve every live key, row fingerprint and effective
delete). A commit that adds or removes rows, or adds equality deletes, means the
target no longer mirrors the source: the index cannot adopt it, and replaying
later changes over it would publish wrong rows. That table is blocked with
`external_mutation` and the other tables keep publishing. A `replace` snapshot
whose rows or deletes differ from those it replaced is found only partway
through reconciliation, so it stays connection-wide, as do missing snapshot
history, an unknown Flow operation (both can mean restored or foreign local
state) and a foreign schema or partition-spec change.

Journal corruption, source connection/slot/identity failures, state-store failures,
spool limits, a replication message larger than `limits.source_message_bytes`
(rejected before its table can be read), invalid shared invariants, and
unclassified errors remain connection-wide. A replication slot still held by a
previous session (SQLSTATE 55006) and a server out of connection slots (53300)
are retried with backoff like a disconnect; PostgreSQL releases a stale
walsender's slot within `wal_sender_timeout`. Retries never stop, so once
failures have lasted five minutes or twice `wal_sender_timeout`, whichever is
longer, capture logs an error with the slot holder's `active_pid` and sets `flow_capture_reconnect_stalled`. Completed bootstrap with intact local authority
can load and recover targets independently. Initial snapshot/bootstrap and
whole-index reconstruction still require their coordinated recovery path.
Unknown/replaced source identities also retain coordinated recovery.

### Blocks that require a resync

`source_schema_incompatible`, `publication_changed` and `external_mutation`
blocks never clear without a resync, and a resync starts from a new slot. Source
WAL retained for the old slot cannot recover the table, so a blocked table must
not hold the shared acknowledgement frontier: a table the operator cannot
repair in place would otherwise fill the source database's disk.

Capture drops a blocked table's changes. It discards their relation metadata,
row images and TRUNCATE entries before spool and journal, exactly like an
unconfigured table, and skips rows that an open transaction spooled before the
latch. An `external_mutation` block is detected by publication rather than
capture, so the table's later changes are still journaled and are settled as
they arrive. Transactions journaled before the latch are completed in the
ledger at the table's unchanged snapshot, one bounded page per loop
iteration and only while no run owns the table, so an admitted run's own
completion is never contradicted. Its finished compaction candidates are
retired and its queued work stops counting against admission. An operation
the table left unfinished first gets a recovery-only attempt: a building
operation is discarded, and a prepared or committed one is resolved through
the ordinary recovery path without publishing new changes. If the table's
target is gone or was replaced, that operation is abandoned locally instead,
since whatever it may have committed belongs to a table this pipeline no
longer owns and a resync rebuilds it. Once its journaled transactions are
settled, the blocked table no longer holds the acknowledgement frontier or
source WAL; an unfinished operation whose recovery keeps failing still does.
Quarantined schema-block changes already in the journal are settled the same
way.

### Publication changes

A publication change that affects one configured table blocks only that table
with `publication_changed`; the other tables keep capturing, publishing and
acknowledging. This covers a configured table removed from the publication, a
row filter or a column list added to it, and PostgreSQL 18's generated-column
requirement failing for it. Startup, every capture reconnect and a running
check about once a minute on capture's identity-proven session detect these,
and each needs a confirming second read. A change that affects every table (the
publication is missing, or INSERT, UPDATE, DELETE or TRUNCATE is unpublished)
stays connection-wide: `publication_changed` source health, a slot resync
marker and exit. During bootstrap every publication change stays
connection-wide.

The publication may already have skipped some of the table's changes, so only
a resync can recover it; capture drops its changes as described in
[blocks that require a resync](#blocks-that-require-a-resync). The block
persists across restart even after the publication is restored; completing a
publication or recovery never clears it, and only a resync does. A publication
block supersedes an earlier schema block on the same table in status, since
changes may be missing rather than merely undecodable. The publication check
runs before schema refresh at startup and reconnect, so a removed table is
reported as `publication_changed` rather than as a schema block.

The check is best effort. It cannot detect a configured table removed and
re-added between checks, whose interim writes pgoutput never sends; coordinate
such publication changes with a resync. Other tables may join or leave the
publication.

## Acknowledgements, limits, and observations

Independent table progress does not imply atomic visibility across tables. A
source transaction that touches two tables can become visible in one before the
other. The source acknowledgement still advances only through transactions whose
affected tables have all completed materialization, or were settled because the
table requires a resync. Local journal storage is not
sufficient to enable journaled acknowledgements.

Consequently, publication isolation provides bounded tolerance for an outage.
While a retryable block (catalog or storage unavailability, rejected
credentials, maintenance pressure) lasts, the journal retains the unfinished
transaction and the subsequent source prefix, and the source may continue
retaining WAL. Blocks that require a resync settle instead. Existing journal and spool quotas still
apply, and reaching them does not permit dropping changes. Journal usage is not
total disk usage: the row index, control database, reference index, and retained
operation artifacts consume additional storage. Independent remote capture
durability and total-storage budgeting are separate contracts.

Table observations distinguish last successful materialized progress from a
current publication block. A connection-wide checkpoint must not be presented as
each table's own freshness. Status should preserve the table's last success while
publication is blocked or catching up.

## Prior art

- [Kafka consumer partition flow control](https://kafka.apache.org/41/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html)
  separates fetching from processing, supports pausing selected partitions, and
  requires careful ordering and committed-position coordination. Flow applies
  that separation after its shared capture journal; a table is not an independent
  source acknowledgement domain.
- Moonlink's [table event handling](https://github.com/Mooncake-Labs/moonlink/blob/401c1d1ad5354f77bf64cbc4f008c4e62a16781d/src/moonlink/src/table_handler.rs#L611)
  and [WAL persistence](https://github.com/Mooncake-Labs/moonlink/blob/401c1d1ad5354f77bf64cbc4f008c4e62a16781d/src/moonlink/src/storage/wal.rs#L917)
  illustrate separate table publication and replay state. Its
  [shared dispatcher](https://github.com/Mooncake-Labs/moonlink/blob/401c1d1ad5354f77bf64cbc4f008c4e62a16781d/src/moonlink_connectors/src/pg_replicate/moonlink_sink.rs#L55)
  can still wait for space in a full table queue. Flow therefore also isolates
  admission; per-table error handling alone is insufficient.
- Iceberg's [unknown commit-state contract](https://github.com/apache/iceberg/blob/main/api/src/main/java/org/apache/iceberg/exceptions/CommitStateUnknownException.java)
  requires resolving the prior outcome before retrying or removing files. Flow's
  durable operation identity, retained artifacts, and recovery-before-admission
  rule preserve this contract.

These are design precedents, not claims that the projects expose identical
failure guarantees. The Moonlink references identify a fixed public revision.


## Qualification

`tests/local/schema_isolation.py` covers real PostgreSQL/Iceberg nullable updates,
lost schema commit responses, streamed DDL, subsequent additions, incompatible
schema blocks, mixed transactions beyond admission capacity, release of the
source acknowledgement past a blocked table, process restart, and exclusion of
unselected values. Run it with and
without `--explicit`, and with `--heap-rewrite`, which blocks a table on a
volatile-default heap rewrite and checks that the block survives a crash
restart while the other table keeps publishing. The PostgreSQL 14–18 CI matrix
runs all three.
