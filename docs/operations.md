# Operations

Flow stops rather than guess when a source change could have hidden rows from
it. This guide describes the supported way through the routine changes that
trigger those stops. It follows from one rule: **one state directory captures
one fixed set of tables from one slot**. A change to that set, to the slot's
history or to a table's physical identity is handled by starting a new
incarnation, not by editing the old one.

## Before initialization

Run the connected preflight with the same configuration and environment as
the daemon:

```sh
embrasure-flow --config flow.toml check --source
```

It is read-only. It checks the server version and `wal_level`, free replication
slots and WAL senders (including room for the temporary recovery slot), the
replication login, heartbeat permission, each configured table (primary key,
replica identity, types, `SELECT`, row-level security and inheritance), the
publication contract and, when present, the slot's WAL retention, invalidation
and `catalog_xmin` age. It also lists other inactive slots retaining WAL. The
command exits nonzero when any check fails. Rerun it when diagnosing a stopped
daemon.

`discover` generates the `[[tables]]` configuration for the publication's
tables from the same catalog reads; see
[configuration](getting-started.md#configuration).

### Bound source WAL retention

PostgreSQL's default `max_slot_wal_keep_size = -1` lets a stalled or abandoned
slot retain WAL until the source disk fills. Set a cap the source volume can
hold alongside its normal WAL. Reaching the cap invalidates the slot, and that
requires resynchronization, so size it well above the backlog you expect to
drain (hours of peak WAL, not minutes). Flow reports `warning` and `at_risk`
from PostgreSQL's remaining `safe_wal_size`, independently of
`limits.wal_soft_bytes` and `limits.wal_hard_bytes`. Alert on those states.

On PostgreSQL 18, `idle_replication_slot_timeout` invalidates a slot whose
consumer stays disconnected too long. Leave it disabled or set it longer than
any planned Flow downtime.

## Resynchronize a source

Several changes require resynchronization: a changed table set, a publication
violation (`publication-resync-required.json` or a `publication_changed` block), a lost or invalidated slot, a
changed source identity (failover, major upgrade, restore), a table blocked for
a heap rewrite, and incompatible schema changes. Resynchronization starts a new
incarnation beside the old one:

1. Stop the daemon. Keep the old state directory for diagnosis; do not delete
   or edit files inside it.
2. Choose a new `state_dir` and a new `source.slot`.
3. Point each `[[tables]]` entry at a new, nonexistent target table, or drop
   the old target through the catalog. `init` requires an empty target whose
   schema matches the configuration.
4. Run `check --source`, then `init`, then `run`.
5. Move readers to the new targets once they reach the source.
6. Drop the old slot on the source:
   `SELECT pg_drop_replication_slot('old_slot');`. An abandoned slot retains
   WAL indefinitely unless `max_slot_wal_keep_size` caps it.
7. Drop the old targets when readers no longer need them.

Writing to new target names keeps the old tables readable until the new
incarnation catches up.

## Add or remove tables

The configured table set is fixed for a state directory. To add tables without
recopying existing ones, run another Flow instance with its own state
directory, slot and publication (or the same administrator-managed publication)
for only the new tables. Each instance holds one more slot and WAL sender.

To remove a table, or to merge instances, resynchronize with the new table set.
Until then, keep the removed table in the publication: removing a configured
table from the publication blocks that table (`publication_changed`).

## Planned source maintenance

| Operation | Effect on Flow | Action |
| --- | --- | --- |
| Restart, minor upgrade, network interruption | Capture reconnects from the durable journal | None |
| Add a column without a default, then set a default, backfill or `SET NOT NULL` | Captured automatically as an optional Iceberg field | None |
| Add a column with a non-NULL default (`ADD COLUMN ... DEFAULT x`) | The table is blocked: existing rows gain the value without row changes | Add it without a default, then `SET DEFAULT` and backfill; otherwise resynchronize |
| `VACUUM FULL`, `CLUSTER`, `pg_repack`, rewriting `ALTER TABLE` | The rewritten table is blocked; other tables continue | Resynchronize the source. Plain `VACUUM` is safe |
| `TRUNCATE` of a configured table | That table is blocked once the transaction commits; a rolled-back TRUNCATE has no effect | Resynchronize the source |
| A configured table leaves the publication or gains a row filter or column list | That table is blocked with `publication_changed` | Avoid it; otherwise resynchronize |
| The publication is dropped or stops publishing an operation | Capture stops with a resynchronization requirement | Avoid it; otherwise resynchronize |
| Physical standby promotion (failover) | Source timeline changes; capture stops | Resynchronize from the promoted primary |
| Major upgrade (`pg_upgrade`, dump/restore, logical migration) | Slot history does not carry over; slots migrated by `pg_upgrade` are unsupported | Resynchronize after the upgrade |

A table blocked by a schema change, heap rewrite or TRUNCATE still holds the
shared source acknowledgement, because an
incomplete transaction cannot be skipped. Healthy tables keep publishing, but
source WAL and the local journal grow until you resynchronize. Treat a block as
urgent. `status` reports `blocked_tables` with the reason; see
[table isolation and recovery](table-publication-isolation.md).

Failover slots (PostgreSQL 17 `failover = true`) are not yet used. After a
promotion, Flow cannot prove that the new primary's slot history matches its
journal, so it requires resynchronization.

## Journal capacity

The local journal holds captured transactions until every affected table has
published them. When `limits.journal_bytes` is full, capture pauses and
`flow_capture_journal_full` is 1. Publication keeps draining the journal, and
the source slot retains the unjournaled WAL. Capture resumes automatically
after the drain; nothing is acknowledged or dropped in between. A long pause
usually means publication is stalled (catalog or object-store outage, blocked
table), so diagnose that first. If a single source transaction cannot fit in a
drained journal, the daemon stops and asks for a larger `journal_bytes`. Keep
the journal much larger than the largest source transaction and at least four
64 MiB segments.

## Iceberg table maintenance

Flow compacts data and deletes, rewrites manifests, expires snapshots after
`limits.snapshot_retention_secs` and removes files it wrote once nothing
retains them. It does not delete files it did not register: data written by
other engines, their failed writes and catalog files outside Flow's records are
not Flow's to remove. If other writers commit to the same tables, give their
orphan-file cleanup to that engine's maintenance, using a conservative age
threshold (Iceberg's default is three days). See
[getting started](getting-started.md#recovery-and-operational-limits) for
snapshot retention and reference coordination.
