# Service observations

`embrasure-flow status` reads an atomic local observation without locking the state database. It reports the source identity, process, readiness, observation timestamp, captured durable LSN, ledger watermarks and registered pending transactions. Readiness expires after fifteen seconds without an update. SIGINT and SIGTERM clear it before shutdown. This file is not recovery authority.
`status` exits 0 when the observation reports ready, 3 when it does not (the
JSON is still printed), 78 when the configuration is invalid, and 1 when there
is no readable observation.

`state` is `starting` from process start through recovery, index rebuild and
startup source validation, `running` once the main loop is serving, and
`stopped` after exit. When `init` or `run` fails, `last_error` records the
exit class (`config`, `resync_required`, `unavailable` or `failure`), the
[exit code](operations.md#exit-codes), the time (`at_ms`) and, for the
operator-action classes, Flow's own message. Other error chains can contain
URLs or row values, so they appear only in the `fatal` log event. The next
process keeps `last_error` while `starting` and clears it once `running`; a
successful `init` also clears it. A process that never obtained the state
directory's lock (because another process holds it) records nothing.

`source_health` reports `unknown` before the first WAL check, then `healthy`,
`warning`, `at_risk`, `unavailable`, `slot_lost`, or `publication_changed`. Hard
WAL/journal pressure and failed monitoring clear readiness while capture and
publication can continue. A successful later check restores readiness; a lost
slot is recorded before the process exits for resynchronization. A restart
keeps reporting `slot_lost`, with readiness withheld, until a WAL check proves
otherwise; a slot that is really lost then stops the process again.
`source_health: publication_changed` means the publication no longer covers the
capture contract for every table after slot creation: it is missing, or an
operation flag was unpublished. The violation writes the durable
`publication-resync-required.json` marker, records `publication_changed` with `ready: false`,
then exits with an error naming the publication and the change. Later starts for
the same slot record it again and exit until a resync.

A change that affects one configured table (removed from the publication, given
a row filter or column list, or failing PostgreSQL 18's generated-column
requirement) does not change `source_health` or stop the process. That table
appears in `blocked_tables` with `error_code: "publication_changed"`, capture
drops its changes, and the other tables keep publishing and acknowledging. Match
a record to its table through `table_progress`, which keeps every configured
table, including blocked ones, with the same `table_id` and its
`source_namespace` and `source_table`. The block survives restart and only a
resync clears it.

Startup, `init` resume, every capture reconnect and a check about once a minute
on capture's session verify the contract. A session that reaches a different
system or database is an identity error, never `publication_changed`. An
interrupted check is transient and retried. The check is best effort: it misses a
configured table removed and re-added between checks, so `healthy` and an empty
`blocked_tables` do not prove that no change was skipped. Initial `unknown` permits startup readiness
and is distinguished from a failed check by the health-availability metric.

## Readiness and liveness

Readiness means the service is running and its source is safe: the main loop
has started and the latest WAL check did not report hard pressure, a lost slot,
a publication contract violation or a failed check. It does not mean every
table is publishing. A blocked table, a catalog or object-store outage, or a
growing publication lag leave readiness unchanged, because capture and the
other tables continue and a restart would not help. Watch those through
`blocked_tables` and the [stall signals](#stall-and-lag-signals) below.

With `[http] listen = "host:port"` configured, `init` and `run` also serve these
observations over HTTP: `/healthz` (or `/livez`) for liveness, `/readyz`
answers 200 only while this process reports ready (503 otherwise, with the
status JSON), and `/metrics` returns the current `metrics.prom`. Do not restart
on a failed readiness check: WAL pressure clears readiness while capture
continues. The endpoints are unauthenticated and read-only.

`/healthz` answers 503 only when the running service's main loop has not
advanced for `[http] liveness_timeout_secs` (default 300; 0 disables the
check). The loop wakes at least every five seconds, and each step is bounded,
so a stall means the process is wedged and a restart is appropriate. Startup
recovery, index rebuild, startup source validation and `init`'s initial COPY
are never judged, however long they take; they report `state: starting`.

## Metrics and diagnostic events

`state_dir/metrics.prom` uses Prometheus text format for a textfile collector. Like the rest of `state_dir`, it is readable only by the user running Flow, so a collector must run as that user; otherwise scrape `/metrics`. Its integer LSN text remains exact; Prometheus stores floating-point samples, so use `status` for exact comparisons beyond its integer precision. Metrics reset on process restart. `init` installs its own recorder and flushes bootstrap duration, outcome and I/O diagnostics on success or failure. The next `init` or `run` replaces that process snapshot; archive `metrics.prom` after initialization to retain COPY cost diagnostics. Labels contain configured table IDs, not row keys or object paths.

Source ledger completion and ACK feedback precede observation writes. Status
updates immediately on completion; full Prometheus rendering and atomic export
run at most once per second during activity. The existing five-second health
tick refreshes idle or stalled tables, so the textfile is not a one-second
freshness guarantee. The first write and explicit graceful-shutdown readiness
update force an export. Error cleanup marks status stopped without forcing a
final metrics export; initialization retains its separate success/error flush.
Failed writes propagate without advancing the successful export deadline.
No separate export task is created.

| Observation | Meaning |
| --- | --- |
| `flow_up`, `flow_ready` | 1 in every export; 1 while the exporting process reports ready |
| `flow_process_start_time_seconds`, `flow_last_update_timestamp_seconds` | Wall-clock process start and export time. A textfile collector keeps serving the last file after the process exits, so alert on the export's age |
| `flow_blocked_tables` | Number of tables in `blocked_tables` |
| `flow_table_blocked{table_id,code}` | 1 for each blocked table, labelled with its stable error code; the series disappears when the block clears |
| `flow_table_blocked_since_timestamp_seconds{table_id,code}` | When the table's current block began |
| `flow_table_materialized_lsn{table_id}` | The table's last successfully published source position |
| `flow_table_lag_bytes{table_id}` | Captured WAL from the start of the table's oldest registered, unpublished transaction to the captured durable LSN; 0 when the table has none |
| `flow_table_lag_seconds{table_id}` | Age of that transaction's source commit (source clock); 0 when the table has none |
| `flow_source_received_lsn` | Highest source end LSN Flow has received and recorded; at least `flow_journal_durable_lsn` |
| `flow_journal_durable_lsn` | Actual capture frontier after journal sync |
| `flow_ledger_registered_lsn` | Prefix registered with source-wide completion tracking |
| `flow_materialized_lsn` | Contiguous source prefix published across every affected table |
| `flow_pending_transactions` | Registered, incomplete source transactions; compare the captured and registered LSNs to detect additional disk backlog |
| `flow_source_health_check_available` | Whether the latest WAL check returned a result; zero before the first result or after a monitoring failure |
| `flow_source_at_risk` | Latest check found hard WAL/journal pressure or a lost slot; inspect availability before interpreting zero as healthy |
| `flow_source_retained_wal_bytes` | WAL the slot retains on the source (current WAL LSN minus the slot's `restart_lsn`) at the latest WAL check |
| `flow_source_safe_wal_bytes` | PostgreSQL's `safe_wal_size` for the slot: WAL that can still be written before the slot is at risk of invalidation. Present only when `max_slot_wal_keep_size` caps retention |
| `flow_journal_bytes` | Total size of the local journal's segment files at the latest WAL check; excludes segment indexes |
| `flow_capture_journal_full` | 1 while capture is paused because the journal quota is full; publication keeps draining and capture resumes automatically |
| `flow_capture_connected` | 1 while replication is streaming, 0 while capture is retrying its connection or replication start |
| `flow_capture_reconnect_failures_total{reason}` | Failed capture (re)connect attempts: `slot_in_use` (SQLSTATE 55006), `too_many_connections` (53300) or `unavailable` |
| `flow_capture_reconnect_stalled` | 1 once reconnects have failed for 5 minutes, or twice `wal_sender_timeout` if longer; an error log names the slot's `active_pid`. Retries continue and it resets on reconnect. Alert on it: no changes are captured meanwhile |
| `flow_capture_disk_low` | 1 while capture is paused because the state volume has less than `storage.min_free_bytes` free; see [disk capacity](operations.md#disk-capacity) |
| `flow_state_volume_available_bytes` | Free bytes on the state volume at the last startup or capture check (about every five seconds) |
| `flow_observation_write_skipped_total` | `status.json`/`metrics.prom` updates skipped because the state volume was full |
| `flow_commit_to_journal_seconds` | Source commit timestamp to successful journal durability |
| `flow_journal_transactions_total` | Source transactions with a nonzero XID durably written to the journal |
| `flow_journal_committed_payload_bytes_total` | Serialized mutation-chunk payload bytes in successfully synced source transactions; excludes aborted subtransactions, journal framing and terminal records |
| `flow_source_registration_seconds` | Successful dispatcher page read and durable source-ledger registration, before feedback; includes blocking database work |
| `flow_source_registration_transactions` | Newly registered transactions per successful dispatcher page; histogram count measures registration pages |
| `flow_capture_phase_seconds` | Schema validation and spool begin/replay/disposal per transaction; journal sync per durability group, labelled by bounded phase and success/error |
| `flow_journal_commit_group_transactions` | Number of complete source transactions sharing a successful journal durability barrier |
| `flow_catalog_commit_seconds` | Catalog publication path, including preparing and committing metadata |
| `flow_catalog_commit_attempts_total{table_id}` | CDC catalog commit attempts, including retries and recovery of a prepared operation |
| `flow_catalog_commits_total{table_id}` | CDC epochs this process committed to the catalog |
| `flow_catalog_recovered_commits_total{table_id}` | Prepared CDC operations found already committed during recovery, for example after a lost commit response; excluded from latency samples |
| `flow_epoch_oldest_commit_to_catalog_seconds` | Oldest source commit in a published epoch to catalog success; this is an epoch histogram, not a transaction percentile |
| `flow_index_apply_seconds` | Local index application after catalog visibility |
| `flow_ingest_prepare_seconds` | Successful ingest artifact and delta preparation through durable Prepared, before catalog resolution |
| `flow_table_local_phase_seconds` | Successful epoch collapse, or dispatcher completion through ledger updates, index cleanup, ACK feedback and observation export |
| `flow_maintenance_phase_seconds` | Fixed preparation phases (`preflight`, `catch_up`, `plan_seal`) and recovery phases (`catalog_resolve`, `mark_apply`) |
| `flow_state_apply_batch_seconds` | State-store aggregate lock wait, preparation, commit and lock hold, plus decode, PK lookup, reverse lookup, mutation-build and file-count components; fixed operation-kind and controlled/standalone labels |
| `flow_state_apply_batch_size` | Apply cursor rows, serialized index `WriteBatch` bytes, submitted update-record count and reverse-owner reads; fixed measurement, operation-kind and controlled/standalone labels |
| `flow_state_stage_batch_seconds` | Delta-batch construction, duplicate lookup and staged write; fixed operation-kind and controlled/standalone labels |
| `flow_state_collapse_batch_seconds` | Disk-collapse batch setup, fold, prior spool reads, index lookup, binding and staged write; fixed phase and controlled/standalone labels |
| `flow_table_l0_files`, `flow_table_l0_bytes`, `flow_table_oldest_l0_seconds`, `flow_table_small_files`, `flow_table_*delete_files`, `flow_table_reclaimable_bytes`, `flow_table_manifest*`, `flow_table_publication_pressure` | L0 count, bytes and oldest-file age, small files, total delete files and per-file fanout, reclaimable bytes, manifest count/entries, and pressure (`0` healthy, `1` delay, `2` pause) |
| `flow_table_retries_total` | Interrupted table attempts that enter recovery and retry |
| `flow_compaction_seconds`, `flow_compactions_total` | Successful built-in compaction passes, labelled `kind="data"` or `kind="delete"` |
| `flow_compaction_finalization_actor_lane_hold_seconds` | Completed data-build finalization from actual table-lane reservation through cleanup and lane release |
| `flow_compaction_preparation_handoff_actor_lane_hold_seconds` | Actor time used to validate and hand a completed build to detached preparation; separate from finalization |
| `flow_compaction_publication_stall_seconds` | Same-table publication gate from preparation handoff through activation, invalidation or preparation deadline expiry |
| `flow_compaction_preparations_total`, `flow_compaction_activations_total` | Detached preparation and exact activation outcomes, with fixed result labels |
| `flow_compaction_candidate_invalidations_total` | Candidates retired before publication, labelled by preparation/activation stage and bounded reason |
| `flow_external_reconciliations_total` | External snapshots successfully verified and durably applied to the row index, labelled by table and bounded reconciliation kind |
| `flow_artifact_{files,bytes}_written_total` | Successfully finished data/delete artifacts, including attempts later invalidated |
| `flow_garbage_delete_requests_total` | Conservative deletion requests against registered obsolete artifacts |
| `flow_garbage_metadata_json_delete_requests_total` | Successful delete requests for superseded catalog metadata JSON |
| `flow_garbage_collection_seconds` | Duration of each successful garbage-collection pass for a table |
| `flow_metadata_maintenance_failures_total` | Table-scoped manifest rewrite, snapshot expiration or garbage collection failures, labelled by `task`; each is retried with backoff |
| `flow_table_maintenance_failing` | 1 after five consecutive failures of one metadata task (`table_id`, `task`) until it succeeds; CDC continues, the task needs attention |
| `flow_expiration_missing_protections_total` | Checkpoint or operation snapshots another process expired before Flow; their protection is dropped |
| `flow_snapshots_over_cap` | Snapshots above `limits.snapshot_max_count` that protection or table policy retain. Checkpoints protect about `retained_checkpoints` × `checkpoint_interval_secs` of commits, so tables committing faster than about `snapshot_max_count` / 600 per second (1.7 at defaults) stay above the cap |
| `flow_snapshot_cap_exceeded_by_table_policy` | 1 when an explicit table `history.expire.max-snapshot-age-ms` keeps the table above the snapshot cap |
| `flow_snapshot_expiration_disabled` | 1 for targets with `gc.enabled=false`; Flow never expires their history |
| `flow_bootstrap_runs_total{outcome}`, `flow_bootstrap_seconds{outcome}` | `init` runs and their duration, labelled `success` or `error` |

Recovered catalog markers are counted separately and excluded from latency samples. Source-to-service times require synchronized clocks; the production benchmark records calibration uncertainty and complete per-transaction observations. Artifact counters do not include internal SDK retry traffic or catalog-owned metadata writes and must not be presented as cloud billing measurements.

`external_snapshot_reconciled` carries the exact table ID, snapshot ID, sequence
number and reconciliation kind. It is emitted only after the verified snapshot
has been durably applied to the row index and the applied operation is cleared.

`transaction_journaled` is a debug event (see [logging](#logging)). It includes `journal_payload_bytes` from the durable chunk
descriptor. This counts bincode mutation vectors after rollback and before
collapse: vector headers, table/schema identifiers, insert/update row images,
update old keys and delete keys. Complete PostgreSQL old tuples used to resolve
TOAST are not retained in this encoding.
It supplies a declared serialized-ingress byte basis for scoped throughput and
cost reports. It is not PostgreSQL wire traffic, WAL volume, unique final-table
bytes or total journal disk usage. Match source identity and end LSN when joining
events; do not sum duplicate replay evidence. The process counter covers the same
nonzero-XID transactions as `flow_journal_transactions_total` and resets on restart.

Source registration is separate from journal sync. Coalesced capture notifications
can register several journal groups in one bounded page. Registration time runs
on the dispatcher; journal-sync time runs on the capture thread. The two totals
overlap and must not be added as serial publication time. Failed pages do not
contribute successful-page observations.

Preparation and application timings are nested diagnostics, not independent
latency components to sum indiscriminately. `compaction_completed` retains its
aggregate worker/preparation/publication fields. For concurrent builds, catch-up
runs in detached preparation and is reported alongside activation diagnostics;
it is not a serial component of the later coordinator interval. Accepted mappings
remain in scratch during catch-up and enter the main store only after exact
activation. Strict rewrites report zero catch-up time.
`compaction_catch_up_completed` reports one successful inner catch-up pass with
its operation, table, build snapshot and fenced head. Its exclusive fields sum
to `inner_elapsed_ms`: `validation_setup_ms`, `late_delete_scan_stage_ms`,
`mapping_elapsed_ms` and `residual_write_ms`. The mapping interval includes a
single staging thread overlapping the next batch's lookup and checks. Its nested
fields obey `mapping_elapsed = mapping_lookup + residual_spool + mapping_verify
+ mapping_stage - mapping_overlap + mapping_other` (all in milliseconds).
`mapping_stage_ms` measures execution in that thread; `mapping_overlap_ms` is
the actual intersection with the next caller batch's interval. Queue, dispatch,
join and other mapping bookkeeping belong to `mapping_other_ms`. These fields
measure elapsed intervals, not CPU time or counterfactual time saved.

The mapping caller and staging thread each retain at most one row-capped batch;
owned key/path/partition payload also uses the existing writer batch/row-group
byte limit. One decoded lookahead, temporary key clones and lookup results,
vector spare capacity, and RocksDB/encoding buffers are additional. This is
logical batch admission, not an absolute decoding or process-RSS bound.

Residual spooling writes only translated late masks; original and late residuals
are merged directly during residual writing, which includes scratch reads,
live-file filtering, deduplication and Parquet output. The mapping caller's
verification remainder includes scratch reads, key preparation and ordering;
final coverage checks remain inside the whole mapping interval. The outer coordinator
catch-up duration includes blocking-task dispatch and runtime setup. A successful
catch-up can still fail during sealing or publication, so this event alone does
not prove a commit.
`compaction_input_admitted` records the S input data/delete file counts, physical
rows and metadata byte sizes before file reads. These are admitted work, not
bytes transferred. `compaction_data_built` records the actual S mappings, scanned
delete rows and produced data files/rows/bytes. Both describe worker attempts,
which can later be cancelled or rejected without publishing. No planner reason
or input level is inferred from the output level.

The catch-up event additionally records the H delete dependency totals, newly
scanned delete rows, mappings visited, accepted and masked, and final physical
output totals. H dependency totals include original S dependencies and must not
be added to S totals as new reads. Mappings partition into accepted and masked
rows; produced data still includes masked physical rows. Output delete rows
include both translated masks and retained residual positions, after deduplication.
`compaction_completed` repeats final artifact totals with the catalog snapshot
only after catalog resolution and index application succeed. Correlate the
operation ID; worker, catch-up and completion outputs are successive observations
of the same artifacts, not separate writes to add together.

`maintenance_recovery_terminal` is emitted exactly once for every recovery call,
including success, replan and error outcomes. It reports the operation delta
count, starting apply cursor, and applied/obsolete rows from this invocation
only. A retry after partial application reports the remaining work, not lifetime
totals. The event also identifies the phase at entry; it can describe ordinary
publication or recovery of an already committed snapshot. Its exclusive
diagnostics split the durable operation lookup, catalog-resolution phase, the
actual `Catalog::update_table` await, durable commit marking, state application,
discard-before-replan and unassigned recovery overhead.
`catalog_resolution_path` states whether the operation committed, was already
found, was already marked committed, or needs replanning. `maintenance_recovered`
is also emitted after state application completes successfully; its
`catalog_resolve_ms` and `mark_apply_ms` fields are coarser aggregates. `compaction_completed.coordinator_prepare_publish_ms` covers
the coordinator interval from preflight preparation through durable application;
`coordinator_prepare_publish_other_ms` excludes the reported preparation and
publish/application intervals. Join the operation ID to
`compaction_finalization_actor_lane_released`, whose `lane_hold_ms` begins at the
daemon's busy-lane reservation and ends immediately after release. It includes
task dispatch, retries, coordinator work, candidate cleanup and scratch removal.
Its `publication_stall_ms` begins at the earlier preparation handoff and includes
detached catch-up plus finalization while publication remains gated. When the
750 ms optional gate expires, `compaction_preparation_deferred` reports its stall
and the same candidate continues without holding CDC. Later activation starts
a new stall sample. Deadline and failed-preparation stalls are
reported by `compaction_preparation_deadline` or
`compaction_preparation_discarded`; a timed-out worker can continue retiring after
same-table CDC resumes. All preparation remains bounded by the original
30-second build age. Hard debt keeps its publication gate through that budget.
These are preparation deadlines, not bounds on
activation or worker joining. The handoff lane has its own event and histogram so it
does not dilute finalization samples. Candidate invalidation events include stage,
bounded reason and the complete error chain.
`delete_rewrite_completed` similarly
records admitted rows, scanned rows and produced file/row/byte totals with its
operation and catalog snapshot. These file sizes exclude metadata artifacts and
SDK traffic; process-wide storage counters remain a separate observation. `ingest_prepared`, `epoch_collapsed` and `epoch_completed`
carry operation IDs in logs for correlation; operation IDs are never metric
labels. ACK feedback timing ends when the coordinator sends feedback, before
PostgreSQL necessarily receives it.
`epoch_completed` retains total `elapsed_ms` and partitions it into
`durable_completion_ms` (ledger, pending/index cleanup and ACK feedback) and
`observation_ms` (immediate status and any due Prometheus export). Its
`metrics_exported` field distinguishes full exports from lightweight status
updates.

`ingest_prepared` partitions `precommit_prepare_ms` into
`data_stage_pair_wall_ms`, `delete_write_ms` (delete-writer calls and close),
and `other_prepare_ms`. Each pair starts before staging the previous batch on a
blocking worker and ends after both that stage and the next data write, final
close, or input receive finish. Only one stage is outstanding. The first write
has no preceding stage. The outer remainder includes table loading, writer
setup, mutation-to-delta assembly, artifact-plan writing and durable sealing.

Within those pairs, `data_write_ms` measures data-writer calls and close;
`index_delete_stage_ms` measures index-delta and position-delete spool staging
inside the worker, including lock waits. `data_stage_overlap_ms` is the summed
intersection of their actual start/end intervals. `data_stage_other_ms` covers
dispatch, input-channel waits and join overhead outside those intervals.
Thus pair wall equals data plus staging minus overlap plus pair overhead.
These are elapsed times, not CPU or storage-only timings. Add the three outer
fields to obtain preparation time; do not add the nested concurrent fields
again.

Index-application samples cover normal committed batches, excluding invalid and
already-applied calls. Preparation includes operation/delta reads, batched key
lookups, conditional row checks, live-file counts and write-batch construction.
Commit encloses the existing staged or durable write path, including control
locking and revision work; it is not a measurement of the fsync syscall alone.
Commit errors are timed before propagation. Intermediate apply batches already
use unsynced WAL writes; the final boundary preserves existing durability and
source-acknowledgement rules.

Delta-staging samples separate bounded iterator consumption, key checks,
serialization and batch construction (`build`), sorted cross-batch duplicate
checks (`duplicate_lookup`), and the existing staged write (`write`). The
duplicate phase includes bounded reverse seeks: a marker range strictly beyond
the persisted maximum is proven fresh; overlapping ranges retain full lookups.
Initial operation lookup and lock setup are outside these intervals. Invalid or
incomplete batches are excluded; write failures are timed before propagation.

Source-collapse batch samples partition row-lock wait and the sealed-spool check
(`setup`), local fold/key construction (`fold`), prior-spool reads/decode and
missing-key construction (`prior_spool`), authoritative PK reads (`index_lookup`),
original binding, presence validation and encoding (`bind`), and the staged write
(`write`). Only calls reaching the write attempt contribute samples, including
write failures before propagation; earlier validation/read failures are excluded.
Final destructors and metric emission are outside the intervals. These phases
are nested within epoch collapse and must not be added to that outer duration.

Each table accepts `priority = "realtime"`, `"balanced"`, or `"efficient"` (default `realtime`). The scheduler uses the recent 32 completed table attempts to estimate the 95th-percentile service duration. It subtracts that estimate and existing source backlog age from the profile deadline, subject to a global commit-rate limit and file/byte thresholds. Maintenance occupies the same table lane and is included in the estimate. CDC completes its source-ledger and ACK boundary before optional maintenance. After one second of deferral, an eligible soft data-build probe or periodic maintenance visit may precede another ready CDC epoch; later CDC completions do not extend that deadline. Soft delete consolidation may still wait for a CDC gap or hard pressure. Hard reader debt requires maintenance before publication. The one-second deferral bounds admission priority, not maintenance duration or publication latency.

Checkpoint and index-generation events include their revisions, paths, durations and restore/rebuild result in structured logs. Catalog publication events expose the first/last LSN and actual catalog completion timestamp independently of index completion. The local test runner uses those events rather than treating acknowledgement or a recent status timestamp as proof of catalog visibility.

Capture seals a group when it reaches 32 complete transactions, crosses 4 MiB of transaction payload, or reaches a five-millisecond scheduling deadline. The payload threshold can be exceeded by the final complete transaction; its payload stays disk-backed and the transaction remains atomic. Source/schema interruptions may seal smaller groups. Terminal records and their chunks become discoverable by concurrent journal readers together, only after sync; source notifications and ACK eligibility follow that boundary. Slow disk or source I/O can exceed the scheduling deadline.

Capture phase timings isolate work within the capture actor, not time waiting for PostgreSQL or queued WAL. Schema validation includes the final buffered chunk flush; ordinary verified schemas do not require a SQL query per transaction. Spool replay includes journal chunk appends but excludes terminal sync. Journal sync measures the commit `sync_data` call; it excludes terminal serialization and index bookkeeping. Spool begin and disposal measure filesystem lifecycle calls. These phases do not cover all capture CPU time and must not be treated as a complete latency decomposition.

`status` also reports `blocked_tables` with stable error codes, retry timestamps,
attempt counts, and retained operation IDs. `table_progress` reports each table's
last successful materialized LSN. These positions may advance independently of the
connection's contiguous materialized watermark. A blocked table retains its last
successful position while other tables continue. See
[table publication isolation](table-publication-isolation.md) for recovery and
storage limits. Code `catalog_auth` means the catalog or its OAuth endpoint
rejected Flow's credentials or permissions (after one token renewal when OAuth
credentials are configured) and needs operator action; `catalog_unavailable`
covers catalog outages and other unexpected responses.

## Stall and lag signals

The table gauges above are rendered from the same observation as `status`:
blocked-table series exist only while the block does, and every label is a
configured table ID or a fixed error code, so series count is bounded by the
table count. Per-table lag refreshes with each table's completion and with the
five-second health observation, so newly captured work appears within one
interval. Registration of captured work is bounded; compare
`flow_journal_durable_lsn` with `flow_ledger_registered_lsn` for the backlog not
yet attributed to tables. Lag seconds use the source commit timestamp, so they
require synchronized clocks.

Example Prometheus alerting rules; tune thresholds to your latency targets:

```yaml
groups:
  - name: embrasure-flow
    rules:
      - alert: FlowTableBlocked
        expr: flow_table_blocked == 1
        for: 10m
        annotations:
          summary: "Table {{ $labels.table_id }} blocked ({{ $labels.code }})"
          description: "Healthy tables continue, but source WAL and the journal grow until it is resolved. See status blocked_tables."
      - alert: FlowTableLagging
        expr: flow_table_lag_seconds > 900
        for: 10m
      - alert: FlowNotReady
        expr: flow_ready == 0
        for: 5m
      - alert: FlowObservationStale
        expr: time() - flow_last_update_timestamp_seconds > 120
      - alert: FlowJournalFull
        expr: flow_capture_journal_full == 1
        for: 15m
      - alert: FlowSourceAtRisk
        expr: flow_source_at_risk == 1 or flow_source_health_check_available == 0
        for: 5m
      - alert: FlowSourceWalRetention
        # Only where PostgreSQL caps retention (max_slot_wal_keep_size).
        expr: flow_source_safe_wal_bytes < 4 * 1024 * 1024 * 1024
        for: 5m
```

With `/metrics` scraped directly, Prometheus' own `up` covers a stopped process;
`FlowObservationStale` covers the textfile collector and a wedged exporter.

## Logging

`init` and `run` log one JSON object per line on standard output. `RUST_LOG`
selects levels with [`tracing` filter directives](https://docs.rs/tracing-subscriber/latest/tracing_subscriber/filter/struct.EnvFilter.html);
the default is `info`. Per-transaction and per-epoch evidence
(`transaction_journaled`, `ingest_prepared`, `epoch_collapsed`,
`table_published`, `epoch_completed`) is logged at debug level under the
`flow_events` target. Enable only those with `RUST_LOG=info,flow_events=debug`;
the benchmark and service suites do. `RUST_LOG=debug` for every target is
verbose, and third-party libraries can then log request details, including
credentials; do not use it in production.

A failure that stops the process is logged once as an `ERROR` event with
`event: "fatal"`, the complete error chain in `error`, its `class` and the
`exit_code`; `init` and `run` do not print it separately to standard error
unless it is a terminal or `RUST_LOG` filters the event out. Other commands also print a plain `error:` line on
standard error. Source storage pressure is logged when its state changes and
repeated at most every five minutes while unchanged (`warning` at `WARN`,
`at_risk` at `ERROR`); the metrics carry every five-second check. A retried
startup dependency logs `event: "startup_retry"` with the step, attempt and
delay.

## Storage and REST cost diagnostics

The daemon decorates the configured Iceberg storage factory without replacing
its OpenDAL configuration or credential handling. Storage metrics reuse handles
with a fixed set of operation labels; no bucket, path, key, or table name becomes
a label.

| Metric | Meaning |
| --- | --- |
| `flow_storage_operations_started_total` | FileIO calls started, including calls later cancelled |
| `flow_storage_operations_total` | Completed calls, labelled `success` or `error` |
| `flow_storage_operation_seconds` | Completed call duration; stream open, write chunks, and close are separate operations |
| `flow_storage_bytes_total` | Bytes returned by successful `read`/`read_range`, or accepted by successful `write`/`write_chunk` |
| `flow_catalog_http_requests_total` | Calls submitted to reqwest, including OAuth, configuration reads, and catalog operations |
| `flow_catalog_http_results_total` | Completed execute calls by success, HTTP client/server error, other status, or transport error |
| `flow_catalog_http_seconds` | Execute duration through response headers; body reading is measured separately |
| `flow_catalog_request_body_bytes_submitted_total` | Known serialized request-body bytes submitted to reqwest, including failed calls |
| `flow_catalog_response_body_bytes_total` | Complete response bodies delivered to the catalog decoder |
| `flow_catalog_body_reads_total` / `flow_catalog_body_read_seconds` | Success/error and duration of response-body reads |

Streaming write bytes are accepted bytes, not a durability claim: inspect the
`writer_close` result as well. A failed call may have transferred partial bytes
that the API cannot report. Stream deletion is one logical API operation, even
when the backend issues several requests. Storage retries, multipart parts,
HTTP redirects, internal transport retries, headers, and TLS overhead are not
counted separately. These observations support cost diagnosis and regression
comparisons; provider request logs and billing remain the authority for billed
request counts and wire traffic. Catalog metric labels use bounded endpoint
classes and methods, never actual URLs or credentials.

The REST hook is an opt-in `metrics` feature in the vendored catalog crate. The
storage decorator forwards configuration and preserves typetag serialization.
A real filesystem test covers full/range reads, streaming writes, stream deletion,
errors, and serialized factory reconstruction. A loopback HTTP test covers OAuth,
normal responses, HTTP errors, connection failure, and label redaction.

The standard-library [offline cost CLI](../tests/production/cost_report.py)
accepts user-supplied USD prices and explicitly measured quantities with a common
UTC interval, local evidence references and declared accounting scope. It hashes
all inputs and evidence, refuses to overwrite reports, and produces six
normalized ratios, such as dollars per million mutations and catalog commits
per million mutations. The [usage and partial-input example](../tests/production/README.md#offline-cost-report)
describes the small input contract. Unknown source bytes, provider requests,
compaction bytes or prices remain null with reasons. A known subtotal is separate
from the complete total within the declared scope; omitted billing categories
are never implicitly free or covered by that total.

The CLI does not infer aligned measurements from the daemon's lifetime counters.
Use actual committed mutations and distinct successful catalog operations in
the same interval, a declared measured source-byte basis, integrated active-table
time, and scoped compaction input/output evidence. Compute/storage/network dollar
terms require measured allocated resource-hours, retained byte-hours or provider
traffic records with matching price units. FileIO call normalization is a separate
diagnostic, not provider request billing. Local cost arithmetic does not qualify
cloud costs, latency, throughput or a 100-table cost curve; those still require
complete measurements under the declared deployment conditions.

## Collapse retention

`limits.collapse_memory_bytes` defaults to 32 MiB per active table epoch; zero
selects disk-only collapse. Four workers can retain up to 128 MiB of additional
accounted memory. Charges include key and row-vector capacities, string/binary
capacities, original physical paths and partitions, and conservative map storage.
The limit applies during both folding and physical-location binding. Capacity
exhaustion discards the attempt and replays its journal prefix on disk; source
validation and storage errors remain errors.

This is separate from decoded journal chunks, index lookup batches, RocksDB
cache/memtables and writer workspace. It is an accounted retention limit, not a
process RSS limit. Oversized and initial-copy transactions take the disk path
directly. Raising worker concurrency also raises the possible retained memory.

The `epoch_collapsed` event records `collapse_mode`, the configured limit,
and the largest accepted retention when known. Modes distinguish memory fit,
disabled buffering, initial COPY, oversized or uncounted prefixes and capacity fallback.
Binding overflow reports an unknown peak because the partial map is consumed and
dropped; `collapse_memory_peak_known` distinguishes that from zero. The outer
collapse duration covers every mode. StateStore's six batch-phase histograms
cover only disk collapse and must not be treated as a complete decomposition of
buffered epochs.

## Parquet admission

`limits.batch_bytes` also bounds logical rows passed to each writer Arrow batch;
`limits.parquet_row_group_bytes` defaults to 32 MiB and forces a rowgroup flush
before the next batch would exceed it. Charges include fixed slots, actual
string/binary lengths, offsets, and validity allowance. A row exceeding the
batch limit fails before output registration. These limits apply during initial
COPY, CDC publication, and maintenance.

Compaction, external reconciliation, and index rebuild share `[parquet_read]`:

| Limit | Default | Admission check |
| --- | --- | --- |
| `footer_bytes` | 8 MiB | Footer range checked before fetching metadata |
| `row_group_fetch_bytes` | 128 MiB | Span of selected compressed columns, including coalescing gaps |
| `row_group_uncompressed_bytes` | 256 MiB | Sum of selected uncompressed encoded column pages |
| `batch_bytes` | 128 MiB | Conservative expanded values plus fixed slots, offsets, and validity |

The scanner reads one rowgroup at a time and reduces batch row counts when the
expanded payload requires it. It does not estimate variable values from average
row width: dictionary repetition can expand far beyond encoded page size.
Position-delete scans project only the reserved path and position fields.
An external rowgroup outside the configured limits needs a smaller rewrite or
an explicitly larger read budget before reconciliation can resume.

These are valid-file payload limits, not a process RSS guarantee. Codec buffers,
metadata objects, allocator capacity, the row index, and concurrent table workers
consume additional memory. Resource sizing must include that concurrency.
`limits.table_workers` defaults to four and bounds concurrent table jobs across
CDC publication and maintenance. Waiting tables retain their source backlog;
reaching the worker limit does not relax reader-debt or acknowledgement rules.

## Allocator memory

GNU/Linux builds with `--features jemalloc` enable process-wide jemalloc and its
background reclamation threads during `init` and `run`. The daemon samples the
allocator at most once every 15 seconds and exports these gauges in `metrics.prom`
and `/metrics`:

| Gauge | Meaning |
| --- | --- |
| `flow_allocator_allocated_bytes` | Bytes currently allocated to application objects |
| `flow_allocator_active_bytes` | Active allocation pages, including unused space within them |
| `flow_allocator_resident_bytes` | Allocator resident pages, including allocator metadata and dirty pages |

The values overlap and must not be added together. Allocator resident memory is
not process RSS: mapped files, thread stacks and other mappings are measured
separately by the operating system. Compare allocator gauges with process RSS
when distinguishing live objects from retained pages. Failed samples omit the
gauges until a later sample succeeds; absence does not mean zero memory.
Background reclamation does not cap live allocations or replace worker, batch,
cache and container limits. Default and non-GNU/Linux builds omit these gauges.

Every build also exports the current usage of the process-wide memory budgets,
refreshed with the periodic health observation:

| Gauge | Meaning |
| --- | --- |
| `flow_memory_index_block_cache_bytes` | Row-index RocksDB block cache usage, including index and filter blocks |
| `flow_memory_index_block_cache_pinned_bytes` | Block cache entries pinned by open readers |
| `flow_memory_index_memtable_bytes` | Active, unflushed and pinned memtables across the row index's column families |
| `flow_memory_index_table_reader_bytes` | Table-reader memory outside the block cache |
| `flow_memory_manifest_cache_bytes{cache}` | Estimated bytes of parsed manifests in the shared `publication` or `maintenance` cache |
| `flow_memory_manifest_cache_entries{cache}` | Parsed manifests in that cache |
| `flow_memory_retained_index_bytes` | Estimated bytes of the garbage-collection reachability indexes |

Cache bytes are the estimates each cache evicts against, not allocator
measurements. Temporary row indexes opened by compaction and reconciliation
jobs are not included. These reads never stop ingestion: a failed read keeps
the gauge's previous value and logs `memory_observation_failed` at most every
ten minutes.
