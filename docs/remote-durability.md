# Remote durability

With a `[durability]` section, Flow makes object storage the authority for its
captured journal and control state. A worker and its entire state volume can be
lost: the next owner restores the latest checkpoint plus a short log tail and
resumes, without losing any change acknowledged to the source. The local volume
remains the working copy and holds the replaceable row index. It may be
ephemeral, or [memory-backed](operations.md#memory-backed-state), but it needs
the same [capacity](operations.md#disk-capacity) as without remote durability.

Remote durability is opt-in. Without the section, local disk remains the
authority exactly as before, and journaled acknowledgement still requires
`journal_durability = "independent-storage"`.

```toml
[durability]
# A dedicated prefix for this source incarnation. s3://, file:// (an absolute
# path on independently durable storage) and memory:// (tests) are supported.
uri = "s3://bucket/flow/orders-primary-v1"
# region = "us-east-1"          # defaults to AWS_REGION
# endpoint = "http://minio:9000" # S3-compatible services, path-style
# max_batch_bytes = 8388608
# max_inflight_writes = 4
# max_writes_per_second = 20     # 0 removes the request budget
# queue_memory_bytes = 67108864
# checkpoint_interval_secs = 30
# checkpoint_batches = 1000
# lease_secs = 10
# request_timeout_secs = 4
# stall_warning_secs = 30
```

S3 credentials come from the standard chain (environment, profile, ECS task
role or instance metadata). The prefix needs `GetObject`, `PutObject` with
conditional writes, `DeleteObject`, `AbortMultipartUpload` (row-index files
larger than 8 MiB are uploaded in parts) and `ListBucket` on the bucket, so a
missing object reports 404 rather than 403. With `[durability]` configured, the
journal counts as independently durable, so `ack_mode = "journaled"` is allowed.

## Contract

Appends never wait for object storage. Every locally durable journal extent and
control-store batch is queued in its local commit order and written behind as
immutable, conditionally created log batches. Several batches are written
concurrently; the durable frontier advances only over a contiguous prefix, so
recovery always sees a prefix of local history.

Nothing outside Flow may depend on state until it is replicated. Every effect
visible outside the process first waits on a barrier, which returns once all
earlier changes are durable and this process still holds its lease:

| Effect | Where the barrier is |
| --- | --- |
| Iceberg catalog changes (commits, table and namespace creation, schema evolution, expiration) | Fenced catalog wrapper around every mutation |
| Object deletions (garbage collection and any FileIO delete) | Storage observer, before the request |
| PostgreSQL acknowledgement | Capture sends only progress whose state is replicated; later progress waits without blocking capture |
| Replication slot creation during `init` | Before `CREATE_REPLICATION_SLOT`, after its intent is recorded |

A barrier therefore costs about one object write when it must wait. In steady
state the record it depends on is usually already durable.

If object storage is unavailable, local capture continues within its journal
quota and queue budget, but nothing is acknowledged, published or deleted:
barriers wait, logging a warning every `stall_warning_secs`, and fail only once
replication has stopped, for example because a newer owner claimed the state.
Log batches and checkpoint objects retry object-storage errors, so the startup
checkpoint waits the same way; a local error, such as an unreadable journal
segment, fails it after three attempts. The source retains WAL meanwhile. Queued records
count against `queue_memory_bytes`; journal extents count only their small
descriptors, since their bytes are read from the local segment when batched, and
contiguous extents share one. Appends block once the budget is used. A clean
shutdown waits about one lease at most for its final checkpoint and release
record, then stops without a release, so the next start waits one lease. A
shutdown after an error likewise waits about one lease at most for queued
records, such as a resync marker, to replicate.

## Ownership and fencing

Each process start claims the next owner epoch with a conditionally created
`owners/<epoch>.json`. The owner renews a lease every quarter of `lease_secs` by
checking that no successor claim exists; barriers require a lease with more
than two seconds left. After startup, no object write starts unless the lease
outlasts `request_timeout_secs`: log batches and checkpoint objects wait for
one, and row-index uploads are skipped without one. The format identity, owner
claim and epoch start written during startup precede the lease and are
conditional creates. A replaced owner fails its next barrier and stops
writing, and the daemon exits.

A successor waits one lease after its claim before reading the final log tail,
unless the previous owner wrote a release record on clean shutdown, or it
finished recovering, ran under the same kernel and held the same lock file this
process now holds, which proves it exited. A crash on the same volume therefore restarts immediately; a lost or
partitioned host costs one lease. Catalog commits that were in flight when the
old owner lost its lease are still resolved safely: every commit is preceded by a
durable prepared operation, and Iceberg compare-and-swap plus operation markers
make the successor either find the commit or commit the same operation itself.
`request_timeout_secs` bounds each object request; the lease must exceed twice
that plus two seconds.

## Recovery

On start, Flow compares the local state directory with the replicated history:

| Mode | When | Result |
| --- | --- | --- |
| `fresh` | Neither exists | Initialize normally |
| `migrated` | Local state, empty prefix | Replicate the existing local state, then continue |
| `local` | Local state written by the most recent replicating owner | Keep it; upload a checkpoint before any external effect |
| `restored` | No usable local state, or local state superseded by a newer owner | Move superseded files to `replaced-<epoch>-<time>/`, restore the control store, journal and authority files, then rebuild the row index from Iceberg |

`remote-durability.json` binds the state directory to its prefix. A directory
replicated to one URI refuses another, and refuses to start without
`[durability]`; remove the marker deliberately to migrate. The prefix's
`format.json` binds it to one source ID; use a new prefix for a new source
incarnation, as with a resynchronization.

Every process takes a baseline checkpoint after opening its control store and
before any barrier passes, so local writes made by an earlier process but not
yet replicated (for example after a crash) are covered before anything depends
on them. Unchanged journal bytes already in the previous checkpoint are reused,
not uploaded again.

Restoring the journal and control store is bounded by the checkpoint interval
and the unpublished backlog.

## Row index recovery

The row index is derived state. Each registered local index checkpoint (every
`limits.checkpoint_interval_secs`) is also uploaded, content addressed, so
successive checkpoints send only new SST files; replicas follow local rotation.
When a replacement owner has no index, it downloads the newest replica and
registers it locally before index recovery runs.

Recovery advances a checkpoint that no longer matches the catalog heads instead
of rescanning every table. For each table it compares the checkpoint snapshot's
files with the head's: entries in removed data files and at newly deleted
positions of shared files are removed, then the live rows of added files are
inserted, as two compare-and-swap checked operations. An operation the
checkpoint caught mid-publication is first finished if its commit was recorded,
or discarded. The work is proportional to files changed since the checkpoint.
Any disagreement, or a checkpoint snapshot that is no longer retained, discards
the candidate and falls back to the full rebuild from current Iceberg data and
position deletes. This also applies without `[durability]`, whenever a local
checkpoint is stale.

## Layout, checkpoints and retention

```text
<prefix>/format.json                         source identity and format version
<prefix>/owners/<epoch>.json                 one claim per process start
<prefix>/log/<seq>.batch                     ordered records, contiguous from the last checkpoint
<prefix>/checkpoints/<replay>-<ordinal>-<epoch>-<count>.json state as of a log position
<prefix>/control/<ordinal>-<epoch>-<crc>.snapshot compressed control-store image
<prefix>/journals/journal/<segment>-<start>-<end>-<crc>.part
<prefix>/index/checkpoints/<name>.json        files of one row-index checkpoint
<prefix>/index/files/<blake3>-<size>          content-addressed index checkpoint files
```

Log batches carry checksummed, versioned records: control write batches,
journal extents and truncations, journal reclamation, small authority files
(`source-identity.json`, `publication-resync-required.json`), epoch starts and releases.
An owner treats a batch as durable only once every earlier one is, so recovery
reads each owner's batches up to its first missing sequence number or ordinal
gap and ignores the rest, along with any batch of an older epoch that appears
after a newer epoch began. If a batch was missing before the lease wait,
recovery reads the tail again afterwards, since it may have landed while the
previous lease was valid. A new owner starts its epoch after every listed batch.

A checkpoint is written every `checkpoint_interval_secs` while the state
changes, or after `checkpoint_batches` log batches. It names a control snapshot
and journal parts that cover every retained segment, and is published only after
every record it reflects is durable. Snapshot and part names include their
checksum, so no owner can replace an object another checkpoint names; journal
parts are uploaded concurrently. After a checkpoint, the owner deletes log
batches and objects that neither it nor its predecessor needs; a predecessor
checkpoint is always kept as a fallback. Row-index replicas no longer
registered locally, and files only they used, are deleted too. Only an owner
with a lease valid for at least half of `lease_secs` deletes. Journal
reclamation is replicated in order, so restored journals keep exactly the
retained segments. A segment whose bytes are not durable yet is
kept locally until a later reclamation instead of blocking the journal. Index
checkpoint files are hashed and uploaded in 8 MiB pieces, and hashes of
hard-linked SST files carry over between checkpoints.

## Cost and tuning

Each log write is one conditional PUT. Up to `max_inflight_writes` start at
once; beyond `max_writes_per_second` records wait and form larger batches, which
bounds request cost at the price of latency. At low rates each change is its own
batch. Lease renewal is one HEAD per quarter lease. Checkpoints add a control
snapshot, new journal parts of at most 8 MiB and a manifest per interval;
retention deletes in batches. Journal bytes are written twice, once in the log
and once as checkpoint parts.

## Observability

`status.json` includes a `durability` object: URI, mode, epoch, appended and
durable positions, remaining lease, any failure, recovery time and restore
statistics, and cumulative batches, records, bytes, retries, checkpoints and
deleted objects.

| Metric | Meaning |
| --- | --- |
| `flow_durability_pending_records` | Appended records not yet durable |
| `flow_durability_put_seconds{kind}` | Object writes for `log` batches, `control` snapshots, journal `part`s and `checkpoint` manifests |
| `flow_durability_batch_bytes`, `flow_durability_batch_records` | Size of each log batch |
| `flow_durability_barrier_seconds` | Time external effects waited for replication |
| `flow_durability_retries_total{kind}` | Retried object writes, by the same kinds |
| `flow_durability_checkpoint_seconds` | Checkpoint duration |
| `flow_durability_control_snapshot_bytes` | Latest control snapshot size |
| `flow_durability_deleted_objects_total` | Objects removed by retention, including row-index replicas |
| `flow_durability_lease_seconds` | Configured lease, set at each renewal |
| `flow_durability_failed` | 1 once replication stopped; the process exits |
| `flow_durability_index_checkpoint_seconds`, `flow_durability_index_checkpoint_bytes_total` | Index checkpoint replication time and newly uploaded bytes |

`durability_recovered` and `durability_checkpoint` log events report each start
and checkpoint; `index_checkpoint_replicated`, `index_checkpoint_downloaded` and
`index_checkpoint_caught_up` report index checkpoint replication and recovery.

## Limits

- One process owns a prefix. Fencing relies on bounded clock rates and on
  object requests finishing within `request_timeout_secs`.
- Each log batch, control snapshot and journal part is written with a single
  request that must finish within `request_timeout_secs`. Journal parts are at
  most 8 MiB and a batch can reach `max_batch_bytes` (8 MiB by default), so the
  default 4 seconds needs about 2 MiB/s of upload bandwidth per request; a
  larger `max_batch_bytes` raises that proportionally. Slower links retry with
  warnings that name the object size; raise `request_timeout_secs` together
  with `lease_secs`, or lower `max_batch_bytes`.
- Downloading the index checkpoint scales with index size, and catch-up with
  the files changed since that checkpoint. Without a usable replica, the full
  rebuild scales with table size.
- Initial-copy staging journals stay local. Losing them during `init` recopies
  the unfinished tables from a new snapshot.
- Upgrades must keep the record and checkpoint format readable. Version 1 is
  the only format.
