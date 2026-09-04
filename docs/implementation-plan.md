# Implementation checkpoints

The [supplied specification](spec.md) and [OSS readiness contract](oss-readiness.md) govern the work. The current supported layout is unpartitioned Iceberg v2 with standard position deletes and one authoritative logical writer per table. External compactor reconciliation is required. The early scope excludes Z-order-aware compaction, distributed compaction protocols, HA and global autocompaction. Low-fan-out partitioning and v3 retain their original later-phase validation gates.

## End-to-end contract

1. Verify the PostgreSQL source incarnation, publication and schema contract. Capture complete transactions, including streamed aborts, into bounded disk-backed journal records before advancing durability.
2. Establish initial table bases through parallel COPY while CDC capture continues. Persist per-table cuts, selected schemas and staging terminals outside the replaceable index; hold the source-wide ACK fence until all bases publish.
3. Collapse mutations against the physical-row index in bounded batches. Serialize supported nullable schema additions with the journal and Iceberg schema so field IDs and historical row meaning remain stable.
4. Write ordinary Parquet data, sorted position deletes and native Iceberg manifests. Register artifact ownership before upload, persist the exact prepared operation, validate the indexed base and commit one transaction per table through catalog compare-and-swap.
5. Resolve ambiguous catalog outcomes from retained operation markers and ancestry. Apply row-index batches atomically with their cursor, preserve authority in the durable control store, and complete source-ledger entries only after table materialization.
6. Advance source acknowledgement through the contiguous completed source prefix. Reader visibility remains a separate catalog-and-object contract that does not depend on the journal or index.
7. Reconcile external physical rewrites before using old row locations. Reject logical drift. Plan bounded data maintenance with residual deletes for surviving files, and separate delete-only consolidation when fanout dominates; pause table publication at hard reader debt and resume after independent maintenance restores capacity.
8. Recover missing or incompatible index generations from durable control, valid checkpoints and current standard Iceberg data/deletes. Resolve pending catalog work before activation and revalidate target UUIDs and heads before switching generations.

Review every change against this chain. A performance improvement must preserve its durable and public boundaries; an isolated helper or passing unit test is not completion.

## Implemented and integrated

| Area | Current integration |
| --- | --- |
| Capture and bounded state | Strict pgoutput v2, source-contract checks, streamed subtransactions, same-event FULL old-tuple TOAST recovery, disk journal descriptors, paged source ledger and bounded mutation/delete spools and transaction-preserving journal group commit. |
| Bootstrap | Up to four COPY workers, concurrent CDC, per-table staging, L1 initial files and durable ACK fence. Interrupted copies retain the permanent slot and recopy only unfinished tables from a temporary snapshot; staged and published tables retain their own cuts. |
| Schemas | Durable source schema registry, versioned journal records and supported nullable additive publication. Unsupported defaults, key/type changes and physical column/source incarnation changes fail safely rather than inventing values. |
| Publication and recovery | Exact indexed-base CAS, operation-marker resolution, durable Prepared payloads, conditional index apply, separate control authority, automatic generation rebuild and checkpoint registration/activation. |
| Maintenance | Bounded data compaction with residual deletes and locally concurrent builds, standalone delete consolidation, exact per-file deletion density, keyed and keyless append-only external reconciliation, manifest rewrite, protected artifact cleanup and snapshot retention, and reader-debt pause/resume. |
| Scheduling and operations | Table priorities, observed publication-service estimates, bounded backlog accounting, status and graceful termination, retained checkpoints, containerized non-root runtime and reproducible service gates. FileIO/REST diagnostics distinguish API activity from provider requests; isolated WAL and pinned reader-planning checks add independent measurements. |
| Standard readers | Actual Spark, Trino, DuckDB and stock Iceberg Rust reads. Java Spark position-delete, data-file and manifest rewrites followed by CDC and restart have passed exact-row checks. |

## Evidence checkpoints

[Validation](validation.md) maps each capability to its local report and tested binary. The latest focused evidence includes:

- [Bootstrap recovery](../target/production-bootstrap-02/report.json) and [DDL between COPY attempts](../target/production-bootstrap-ddl-02/report.json): mid-COPY crash, durable staging reuse, concurrent CDC, index deletion and automatic rebuild.
- [Schema/lifecycle contracts](../target/production-contracts-08/report.json): eleven passing phases on release24 covering idle nullable additions during native compaction, lost response, queued versions, abort/savepoint boundaries, historical schema widths, checkpoint restore and SIGTERM/status. Separate contract reports verify unsupported defaults and column incarnation fencing.
- [Durable faults](../target/production-faults-15/report.json): twelve passing phases on release28 across actual service outages/restarts, successful-commit response loss, durable unpublished work and physical index loss. Both crash phases require successful current-phase Prepared requests after SIGKILL and before replacement. The independent compactor pauses across those two forced-success phases and runs through the other faults.
- [Reader-debt recovery](../target/production-pressure-01/report.json): publication and ACK pause, continued capture, external compaction and automatic resumption.
- [Independent readers](../target/production-readers-06/report.json): sixteen passing phases on release23, native compaction before and after independently attributed Spark rewrites, exact keyed and keyless duplicate rows across Spark/Trino/DuckDB, retained histories and source changes across restart.
- [Index-format upgrade](../target/production-upgrade-01/report.json): release05 → release06 preserved the control store, source identity and permanent slot; rebuilt format-2 state into format 3; consumed queued WAL; preserved exact CRUD/key moves and history; and reused the new generation on the next restart.
- [Delete/residual integration](../crates/testkit/tests/delete_rewrite.rs): actual Parquet/Avro and stock Rust reads verify sequence filtering, shared targets, duplicate positions, exact deletion density, dangling-file removal, unchanged base files, lost-response recovery and full index-loss reconstruction.
- [Linux demo](../target/production-linux-demo-01/report.json): a freshly built Linux image, fresh isolated volumes, standard Trino CRUD, non-root ownership, SIGTERM exit 0, resumed source backlog and idempotent initialization.

These reports span successive builds. Older service/stress and workload reports remain evidence for their recorded binaries; newer targeted checks do not replace a complete final-release run. Failed workload reports remain visible in the [production qualification report](production-qualification.md).

## Latest performance checkpoint

Immutable [release29](../target/qualification-binaries/source-release29.json)
passes 128 active Rust tests, strict Clippy, formatting and Rust 1.94 compatibility.
The current-epoch buffer shares disk mutation rules and validates its captured
schema, complete table state and physical head before publication. Its explicit
32 MiB per-worker allowance can add 128 MiB at four workers; decoding, lookup and
writer workspace remain separate. Persisted formats and durability barriers are
unchanged.

All fourteen [finite comparison cases](../target/collapse-buffer-profile-29/assessment.md)
pass exact current/history/index checks, including forced disk fallback. Lifecycle
medians improve 29.88% for repeated keys and 10.71% for distinct keys. The
[interrupted bootstrap/DDL gate](../target/production-bootstrap-ddl-03/report.json)
passes five phases, and [faults16](../target/production-faults-16/report.json) passes
twelve phases including successful-commit response loss, index loss and external
reconciliation. Three independent-compactor supervisor restarts remain recorded
as availability incidents.

The [bounded service control](../target/production-bounded-protocol-matched-29/report.json)
requests 50k mixed mutations/s with native concurrent compaction:

| Check | Measured | Required |
| --- | --- | --- |
| Scheduled measured arrivals | 29,250 committed; 750 missed | All 30,000 scheduled transactions |
| Committed transactions | 34,250 including warmup; all observed | No missing committed transactions |
| Exact final rows | 442,500 before and after reader-baseline compaction | Exact source agreement |
| Catalog latency p95 / p99 | 6.750 / 7.222 seconds | Below 1 / 2 seconds |
| Final drain | 5.906 seconds | No growing steady-state backlog |
| Reader slowdown | 2.271× | At most 1.25× |

Committed-row correctness passed; source admission, catalog latency and reader
health failed. Every completed runtime epoch fit the buffer, and total logged
collapse time was 8.032 seconds. The previous release28 control admitted all 35,000
transactions and logged 15.429 seconds of collapse, with catalog p95/p99 of
5.069/6.299 seconds and 1.844× scan slowdown. Different admitted cohorts and
compaction work prevent treating either overall result as an isolated buffer
comparison. Retained arrival and phase records guide the next change; thresholds
and the bounded-v2 measurement protocol stay fixed.

The first focused index-apply change removes duplicate owned decoding of current
file paths and partitions without changing the durable representation. Its
[twelve-sample actual-RocksDB comparison](../target/apply-borrow-profile-29/assessment.md)
passes exact forward/reverse/count/checkpoint/reopen checks. Median apply time
improves 1.92% with one file and 3.64% with 256 files; obsolete apply improves
4.35% and 4.79%. The full release gate passes 129 active tests. This is retained
as a bounded improvement and does not qualify the failed service control.

No sustained 50k, 100-table or 24-hour qualification is claimed. The separate
[WAL check](../target/production-wal-overhead-01/assessment.md) and
[pinned reader diagnostic](../target/production-reader-diagnostics-27-01/assessment.md)
close specific measurement gaps without qualifying provider costs or the remaining
workload dimensions. Final server-side PostgreSQL acknowledgement was not captured
by bounded-v2; final local watermarks must not be relabeled as that measurement.

## Remaining sequence

1. Resolve the measured service critical path using release29's retained arrival, publication and compaction records. The buffer is exercised in every runtime epoch and reduces collapse work, but the service still misses the source, catalog and reader gates. The borrowed index view removes one measured allocation source but its isolated 1.92–3.64% apply improvement is too small to assume service qualification. Continue separating source-admission stalls, table-lane occupancy and physical maintenance amplification before the next focused change. The rejected [atomic-flush experiment](../target/atomic-flush-profile-28/assessment.md) remains rejected. Preserve physical-head validation, journal replay, exact Prepared ownership and existing workload thresholds; speculative next-epoch reservation remains deferred.
2. Repeat the measured workloads after a supported optimization, preserving the local build/catch-up protocol, measurement protocol and resource budgets. Once the 50k and 100-table cases meet their gates, extend the same artifact to the remaining workload dimensions.
3. Use the validated disk-backed harness for subsequent workloads and retain its measurement protocol. The seven-phase composite-key and capture-quota service gate passes on release23, including crash/rebuild, acknowledgement fencing and same-slot recovery. Preserve existing format compatibility; the rejected dictionary experiment does not require a new format-upgrade campaign.
4. Extend source-version, reader-version and storage/catalog coverage beyond the recorded gates. Real-source tests cover PostgreSQL 14.24, 15.19, 16.15, 17.11 and 18.6; reader results apply to the exact versions in validation. Extend failure and retention checks to deployment conditions.
5. Extend reader-observation and resource accounting, then run the original workload matrix for its required 24-hour durations. Qualify steady-state compaction, resource growth, index I/O, provider costs, planning and scan amplification, and publication latency together. The passing finite WAL/planning checks and short saturation probes do not satisfy this gate.
6. Publish accurate OSS support and dependency/license policies, maintain Linux CI/demo checks, and prepare reusable upstream Iceberg patches.
7. Add low-fan-out partitioning, then v3 behavior only after the specification's preceding qualification gates, with separate interoperable delete/rewrite/recovery validation.

Local integration, independent readers, real service fault injection and performance work are authorized by the follow-up requests. Their remaining gaps are engineering and qualification work, not deferred user requests.
