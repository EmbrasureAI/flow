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
| Maintenance | Bounded data compaction with residual deletes and locally concurrent builds, background catch-up preparation followed by exact activation, deadline retirement that preserves BUILD protection, standalone delete consolidation, exact per-file deletion density, keyed and keyless append-only external reconciliation, manifest rewrite, protected artifact cleanup and snapshot retention, and reader-debt pause/resume. |
| Scheduling and operations | Table priorities, observed publication-service estimates, bounded backlog accounting, status and graceful termination, retained checkpoints, containerized non-root runtime and reproducible service gates. FileIO/REST diagnostics distinguish API activity from provider requests; isolated WAL and pinned reader-planning checks add independent measurements. |
| Standard readers | Actual Spark, Trino, DuckDB and stock Iceberg Rust reads. Java Spark position-delete, data-file and manifest rewrites followed by CDC and restart have passed exact-row checks. |

## Evidence checkpoints

The frozen [release34 gate](qualification/release34.md) closes durable cleanup, generation corruption recovery and WAL-health status gaps. The release34 workload matrix exposes starvation of soft maintenance under continuous CDC. [Release35](qualification/release35.md) adds a bounded capture opportunity and passes a service regression that fails on release34, all 154 active Rust tests, nine Python checks and 29 service phases. Its six performance comparisons preserve accepted data but do not qualify throughput or reader health.

[Validation](validation.md) maps each capability to its local report and tested binary. The latest focused evidence includes:

- [Bootstrap recovery](../target/production-bootstrap-02/report.json) and [DDL between COPY attempts](../target/production-bootstrap-ddl-02/report.json): mid-COPY crash, durable staging reuse, concurrent CDC, index deletion and automatic rebuild.
- [Schema/lifecycle contracts](../target/production-contracts-08/report.json): eleven passing phases on release24 covering idle nullable additions during native compaction, lost response, queued versions, abort/savepoint boundaries, historical schema widths, checkpoint restore and SIGTERM/status. Separate contract reports verify unsupported defaults and column incarnation fencing.
- [Durable faults](../target/production-faults-15/report.json): twelve passing phases on release28 across actual service outages/restarts, successful-commit response loss, durable unpublished work and physical index loss. Both crash phases require successful current-phase Prepared requests after SIGKILL and before replacement. The independent compactor pauses across those two forced-success phases and runs through the other faults.
- [Release32 durable faults](../target/production-faults-release32-01/report.json): twelve passing correctness/recovery phases cover committed-response loss, catalog outage, durable REST/MinIO/PostgreSQL restarts, SIGKILL with journaled work, a successful catalog commit followed by physical index loss, wide/unchanged TOAST and an independent reader/manifest audit. Three external-compactor supervisor restarts remain availability incidents.
- [Reader-debt recovery](../target/production-pressure-01/report.json): publication and ACK pause, continued capture, external compaction and automatic resumption.
- [Independent readers](../target/production-readers-06/report.json): sixteen passing phases on release23, native compaction before and after independently attributed Spark rewrites, exact keyed and keyless duplicate rows across Spark/Trino/DuckDB, retained histories and source changes across restart.
- [Index-format upgrade](../target/production-upgrade-01/report.json): release05 → release06 preserved the control store, source identity and permanent slot; rebuilt format-2 state into format 3; consumed queued WAL; preserved exact CRUD/key moves and history; and reused the new generation on the next restart.
- [Delete/residual integration](../crates/testkit/tests/delete_rewrite.rs): actual Parquet/Avro and stock Rust reads verify sequence filtering, shared targets, duplicate positions, exact deletion density, dangling-file removal, unchanged base files, lost-response recovery and full index-loss reconstruction.
- [Release32 concurrent compaction](../target/production-concurrent-compaction-release32-01/report.json): seven passing real-service phases cover CRUD, key moves and delete/reinsert while compaction I/O is held, hard-debt acknowledgement pause, deadline retirement with BUILD protection, scratch cleanup, SIGTERM during held I/O and restart recovery with retained history.
- [RocksDB staging profile](../target/sst-staging-profile-32/assessment.md): eighteen alternating fresh-database samples at 100,000 and 1,000,000 mappings compare current staging, a prevalidated ordinal WAL lower bound and external-SST staging. Exact forward/reverse owners, file counts, cursors and reopen checks pass, including cleanup after closing between SST ingestion and sealing.
- Index apply profiles: the [72-sample coupled screen](../target/apply-batch-row-profile-33/assessment.md) rejects 4,096, 8,192 and 16,384 rows. A [second 36-sample screen](../target/apply-batch-decoupled-profile-33/assessment.md) fixes lookup chunks at 1,024 while comparing 1,024, 4,096 and 8,192-row atomic commits. Every sample passes exact index and reopen checks; neither larger commit group meets the declared workload gains.
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

Release31 establishes the controlled maintenance baseline. Release32 moves
catalog-head validation, one immutable RocksDB index view and catch-up preparation
out of the table actor, then activates only if the prepared catalog and complete
table state still match exactly. The identical [A/B/C/D comparison](../target/production-maintenance-matrix-release32-01/comparison.md)
runs disabled, delete-only, L0-only and current maintenance at 1,000 and 10,000
mutations/s. Every release31 and release32 case has complete measured arrivals,
exact source acknowledgement and final rows, durable external reconciliation,
clean daemon and compactor exits, and catalog p95/p99 below 1/2 seconds.

Release32 records 20/20, 9/9, 20/20 and 31/31 started/ready preparations in the
1k-C, 1k-D, 10k-C and 10k-D cases; all activated and none reached a deadline,
invalidated or used strict fallback. Foreground actor occupancy falls from
2,339.368/1,786.497/6,970.142/10,433.008 ms to
1,522.175/1,591.487/4,492.401/9,131.178 ms respectively. Direct publication
stall is 2,118.186/2,033.862/7,846.988/12,596.863 ms; release31 has only the old
actor-hold proxy. The release32 reader ratios are 1.284/1.317/1.538/1.438 at 1k
and 1.801/2.058/3.278/1.397 at 10k, so every case still fails the 1.25 bound.
These finite runs qualify their correctness and catalog-latency gates, not the
full production target or sustained reader health.

The [release32 RocksDB staging profile](../target/sst-staging-profile-32/assessment.md)
does not support adopting external SSTs as a general staging optimization. Against
the comparable prevalidated ordinal-WAL path, external SST reduces median staging
activation from 277.925 ms to 1.543 ms at one million mappings, after 258.6 ms of
background construction, but total staging plus apply rises 4.948% to 4,051.255 ms.
Apply consumes 93.579% of that path. This narrows the next experiment to bounded
atomic index-apply batches; live forward, reverse and count column families must
remain one atomic transition.

The [coupled apply screen](../target/apply-batch-row-profile-33/assessment.md)
passes 72 exact samples but rejects every larger row ceiling. The
[decoupled screen](../target/apply-batch-decoupled-profile-33/assessment.md)
then holds lookup chunks at 1,024 rows. Reverse-owner reads remain identical for
every commit size, and 4,096/8,192-row commits improve the heavy one-million-row,
256-file compaction by paired medians of 12.54%/12.38%. They reach only
3.42%/4.46% at 100,000 rows and 0.49%/0.07% for the one-million-row update/delete
case; 4,096 regresses 0.19% in the eight-file case. Both candidates miss the
10% and 20% compaction gates. The default stays at 1,024 rows, so this experiment
does not justify a serialized 8 MiB cap or a service-matrix rerun for a larger
row ceiling. The retained implementation still requires final service validation.

No sustained 50k, 100-table or 24-hour qualification is claimed. The separate
[WAL check](../target/production-wal-overhead-01/assessment.md) and
[pinned reader diagnostic](../target/production-reader-diagnostics-27-01/assessment.md)
close specific measurement gaps without qualifying provider costs or the remaining
workload dimensions. Final server-side PostgreSQL acknowledgement was not captured
by bounded-v2; final local watermarks must not be relabeled as that measurement.

## Remaining sequence

1. Establish source capacity before repeating 50k daemon comparisons. The [source-only prerequisite](qualification/source-capacity.md) misses arrivals in three serial four/eight/four-writer trials; adding writers does not resolve it. Release35, release34 and the maintenance-disabled 50k diagnostic also miss source arrivals. Release35 admits soft builds during backlog, but its unequal accepted cohort prevents a performance conclusion. Its 1k/10k catalog latency passes while reader health fails; ten-table latency also fails. Preserve exact table/head validation, Prepared ownership, deadline retirement, journal replay and the same measurement thresholds.
2. Retain release32's current staging path. The external-SST comparison passes its correctness and reopen boundaries but does not improve total work at one million mappings; its tiny install only moves construction out of the activation barrier while the unchanged index apply dominates.
3. Retain the 1,024-row apply default. The coupled and fixed-lookup comparisons reject larger atomic commits; the external-SST comparison also fails to improve total work. Use the release34 end-to-end phase evidence to select any further index change. An unreachable generation is a possible design, not an accepted implementation plan or a reason to repeat component tuning before measuring the current pipeline.
4. Close the measured 50k, multi-table and reader gaps before endurance qualification. Release34 completes all thirteen workload cases with exact accepted data and ACK, but only wide-1k and append-10k meet every performance gate; 100-table catalog p95/p99 are 39.6/43.5 seconds. Verify periodic metadata/garbage work and bounded resource growth under continuous backlog, then run the required 24-hour dimensions. Qualify index I/O, provider costs, planning/scan amplification and publication latency together.
5. Extend source-version, reader-version and storage/catalog coverage beyond the recorded gates. Real-source tests cover PostgreSQL 14.24, 15.19, 16.15, 17.11 and 18.6; reader results apply to the exact versions in validation. Extend failure and retention checks to deployment conditions.
6. Publish accurate OSS support and dependency/license policies, maintain Linux CI/demo checks, and prepare reusable upstream Iceberg patches.
7. Add low-fan-out partitioning, then v3 behavior only after the specification's preceding qualification gates, with separate interoperable delete/rewrite/recovery validation.

Local integration, independent readers, real service fault injection and performance work are authorized by the follow-up requests. Their remaining gaps are engineering and qualification work, not deferred user requests.
