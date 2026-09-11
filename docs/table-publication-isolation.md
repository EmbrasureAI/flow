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
table's retained history. On upgrade, a resumable migration builds references in
bounded pages, advancing its progress marker in the same durable write. These
SOURCE records are mirrored through the authoritative control store and survive
index-generation recovery. They do not introduce another payload journal.

Queued and running descriptors share the configured `pending_transactions`
budget. Admission rotates across eligible tables, including when there are more
tables than available descriptors. A failed worker must return before its queued
and running reservations are evicted. Eviction removes only volatile admission:
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

Only known table publication errors receive this treatment. Journal corruption,
source failures, state-store failures, invalid shared invariants, and unclassified
errors remain connection-wide. Completed bootstrap with intact local authority
can load and recover targets independently. Initial snapshot/bootstrap, legacy
target-identity adoption, and whole-index reconstruction still require their
existing coordinated recovery path. Source schema validation remains shared.

## Acknowledgements, limits, and observations

Independent table progress does not imply atomic visibility across tables. A
source transaction that touches two tables can become visible in one before the
other. The source acknowledgement still advances only through transactions whose
affected tables have all completed materialization. Local journal storage is not
sufficient to enable journaled acknowledgements.

Consequently, publication isolation provides bounded tolerance for an outage.
The journal retains the unfinished transaction and the subsequent source prefix;
the source may continue retaining WAL. Existing journal and spool quotas still
apply, and reaching them does not permit dropping changes. Journal usage is not
total disk usage: the row index, control database, reference index, and retained
operation artifacts consume additional storage. Independent remote capture
durability and total-storage budgeting are separate contracts.

Once recovery releases a large completed source prefix, acknowledgement and
cleanup advance in bounded pages so they yield to other work.

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
