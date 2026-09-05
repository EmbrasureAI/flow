# Release34 local correctness and recovery evidence

Release34 passed the local service correctness and recovery campaign. This is
not a fully production-qualified release: performance qualification is pending,
and neither 50,000 mutations/s nor a 24-hour endurance run is qualified.

## Artifact and checks

The frozen optimized build uses clean source commit
`b0ca5f0b0dc929edf93faf4effaa1ce266054dcd`, tree
`82a1ebe3e64f452958687e6dc25bd4819e055ea6`, Rust 1.97.1, and target
`aarch64-apple-darwin`. The service campaign uses harness commit
`80f2fa556c38f02c8287753095a1c5cf158cc59a`; its health-test observation fix does
not change either frozen binary.

| Artifact | SHA-256 |
| --- | --- |
| Daemon | `e9a70ebd97f1a6091371aecf6af7108a27670033d7d4b2e459d9989dc81a9d7f` |
| External compactor | `930c928c398fb145a0e3c0c06f034ee50b9c88f68b768f7b73c1b847fbd02595` |
| Cargo.lock | `2d84ac4f5caa3d832aa41c8020efcb083513c12e936309bd9d9259f8021aa8af` |

Workspace formatting, Clippy with warnings denied, and the Rust test suite
passed: 154 active tests across 40 test binaries, with four explicitly ignored
manual profiles. Nine Python checks passed. The build manifest records the
complete command and toolchain in
`target/qualification-binaries/release34/build-manifest.json`.

The build includes checksum-based index recovery and generation retirement,
durable bootstrap staging cleanup, source-ledger reconciliation before retiring
Applied operations, atomic ingest spool retirement, and bounded garbage
collection that promptly continues eligible backlog. PostgreSQL decimal typmods
are checked exactly; unsupported partitioned roots fail preflight. WAL health
failures and source retention risk are exposed in status and readiness.

## Real service campaign

`target/release34-campaign-03/summary.json` records five passing commands and
35 passing phases, including setup phases. The environment used PostgreSQL
18.6, a durable REST catalog, MinIO, and independent DuckDB 1.5.5 reads of ordinary
Iceberg. Reported durations below are whole command durations, including setup
and deliberate stalls, rather than ingestion latency measurements.

| Suite | Phases | Seconds | Coverage |
| --- | ---: | ---: | --- |
| Health | 4 | 26.704 | CDC during a stalled monitor query, one-worker admission, timeout, readiness recovery, checkpoint and SIGTERM |
| Boundaries | 7 | 32.787 | Composite key changes, delete/reinsert, pending-work crash and index rebuild, journal quota ACK fence, same-slot recovery and clean restart |
| Bootstrap | 5 | 11.958 | Kills during COPY and after durable staging, concurrent CDC, additive schema change during recovery, total index loss |
| Concurrent compaction | 7 | 181.566 | CDC and translated deletes during builds, hard-debt ACK pause, cancellation ownership until worker join, SIGTERM and restart |
| Faults | 12 | 88.859 | Lost successful commit response, catalog outage, persistent service restarts, SIGKILL, index loss, wide rows and unchanged TOAST, external compactor reconciliation |

The health query timed out after 5.001 seconds. Status changed to
`unavailable`/`ready=false`, CDC continued, and successful monitoring restored
`healthy`/`ready=true`. SIGTERM completed in 0.019 seconds. During a held
compaction build, SIGTERM completed in 1.028 seconds; restart discarded the
durable abandoned build and reclaimed its scratch directory.

The independent reader verified exact current rows after mutations and recovery,
and retained historical snapshots remained readable. Boundary tests preserved
both pre-crash and pre-quota snapshots with 4,100 orders and 17 accounts. The
compaction test verified eight translated delete rows, retained data sequence
4 below delete sequence 5, and compaction sequence 6. Its retained initial
snapshot still contained 36,096 rows. The fault campaign observed 15 external
compaction commits; the final manifest audit found one data file and no delete
files per table, while the initial 4,096-order snapshot remained unchanged.
These are correctness observations, not reader latency qualification.

Fault recovery measurements were 4.631 seconds for a lost successful commit
response, 2.340 seconds after catalog HTTP recovery, 10.723 seconds for REST
service restart, 0.576 seconds for MinIO restart, and 5.138 seconds for PostgreSQL
restart. The daemon stayed running through those five recoveries. The background
test compactor exited during three faults and required supervisor restarts;
the JDBC catalog also required a restart to recover its connection pool after
PostgreSQL restarted. Accordingly, the fault report marks correctness and
recovery passed but `availability_without_supervisor_restart=false`. No worker
panics were recorded in the boundary, concurrent-compaction, or fault reports.

Two unsuccessful campaign attempts remain in `target/release34-campaign` and
`target/release34-campaign-02`. The first failed initialization on catalog HTTP
500. The second exposed a health-test measurement error: it waited for metrics
export, whose idle cadence coincided with the injected five-second timeout.
Harness commit `80f2fa5` observes the immediately updated status watermark with
a three-second bound instead. The third campaign passed using the same frozen
daemon. These earlier attempts are retained evidence, not counted as passes.

Raw reports, metadata, proxy logs and process logs remain under the ignored
`target/` directories named above. This tracked summary preserves the artifact
identities and result limits; a fresh clone must rerun the harness to obtain
those raw artifacts.

## Small-file policy comparison

Four serial runs on the same binary compare the default stable-small-file
soft/hard bounds of 24/32 against 4/8. Each uses one table, 100,000 initial rows,
256-byte compressible payloads, four writers, 100-row transactions, ten seconds
warmup and 60 measured seconds. The mix is 70% updates, 20% inserts and 10%
deletes. Manifest maintenance is held above the finite run envelope in both
cases; this comparison isolates one policy setting.

| Rate | Bounds | Catalog p95 / p99 ms | Scan ratio | Data / delete rewrites |
| --- | --- | ---: | ---: | ---: |
| 1,000 | 24/32 | 516.603 / 555.886 | 1.490 | 9 / 13 |
| 1,000 | 4/8 | 505.641 / 626.240 | 1.509 | 9 / 14 |
| 10,000 | 24/32 | 879.863 / 1,230.213 | 1.392 | 29 / 1 |
| 10,000 | 4/8 | 1,006.152 / 1,453.277 | 1.605 | 23 / 3 |

The candidate is rejected; defaults stay 24/32. All four runs pass exact rows,
complete arrivals and observations, PostgreSQL ACK, external reconciliation,
and clean shutdown. All fail the 1.25 scan-ratio target; the 10k candidate also
fails p95 latency. No hard-pressure or failed maintenance events occurred.

Earlier small-file consolidation increased metadata-based rewrite input/output
ratios from 0.306/0.237 to 0.384/0.317 at 1k and from 0.214/0.145 to 0.231/0.163
at 10k. These compare completed maintenance file bytes through the original
pinned snapshot against encoded journal payload, excluding the external baseline;
they are not provider traffic or billing measurements.

Original scans take 7.2–10.0 ms, versus 4.8–6.3 ms after independent compaction
of the same exact rows. The comparison changes manifests, data files and deletes
together, so it does not isolate the cause of the remaining reader overhead.
Manifest peaks stay below the default trigger of 64. Full samples, snapshot
cutoffs, report hashes and accounting are retained in
`target/release34-policy/assessment.{md,json}`. One pair per rate is enough to
reject this change, not to establish repeat-run performance distributions.

## Default-policy workload matrix

All thirteen cases of `target/release34-matrix` use the default compaction
policy and manifest trigger of 64, with the benchmark's recorded worker, commit
and disk budgets. Each measured case lasts 60 seconds after ten seconds warmup.
All thirteen verify exact final source/target rows, final source ACK, every
committed observation, independent compactor reconciliation and clean shutdown.

| Workload | Missed arrivals | Catalog p95 / p99 ms | Worst scan ratio | All bounded gates |
| --- | ---: | ---: | ---: | --- |
| Mixed 1k | 0 | 522 / 581 | 1.337 | Fail |
| Mixed 10k | 0 | 865 / 1,105 | 1.396 | Fail |
| Mixed 50k | 3,874 | 22,013 / 22,363 | 2.885 | Fail |
| Wide 16 KiB rows, 1k | 0 | 665 / 783 | 1.003 | Pass |
| Ten tables, 10k total | 390 | 1,831 / 2,523 | 2.359 | Fail |
| External compactor, 1k | 0 | 523 / 1,366 | 1.320 | Fail |
| Append 10k | 0 | 544 / 937 | 1.160 | Pass |
| Hot updates 10k | 0 | 520 / 1,674 | 1.450 | Fail |
| 1 KiB rows, 10k | 29 | 789 / 1,078 | 1.229 | Fail |
| Large 10,000-row transactions, 10k | 0 | 618 / 701 | 1.735 | Fail |
| Single-row transactions, 1k | 8,847 | 856 / 951 | 1.085 | Fail |
| Zipf updates, 10k | 0 | 793 / 1,129 | 1.555 | Fail |
| 100 tables, 10k total | 0 | 39,622 / 43,519 | 1.388 | Fail |

Percentiles describe committed transactions only. Missed arrivals still fail
qualification, even when the observed latency and reader gates pass. The 50k
case admits 43,507 mutations/s during the window and drains in 20.311 seconds.
All 409,650 final rows match, including after independent compaction. There is
no evidence of data loss in this failed performance run.

The `50k-assessment.md` beside those reports attributes 22.968 seconds of the
measured minute to synchronous data rewriting, preparation and publication/apply.
Ingestion occupies another 34.512 seconds. These are wall-time phases, not CPU
samples. A continuously ready CDC queue precedes overdue optional maintenance in
the scheduler, consistent with the observed transition from early background
builds to nine synchronous rewrites across warmup and drain. Three later
preparation timeouts occur after final ingestion and cannot explain its latency.
The next experiment admits overdue background capture under backlog while
preserving hard debt bounds, exact activation and cheaper delete consolidation.

The 100-table case admits all 6,000 measured transactions but fails latency.
Per-table retained snapshot counts range from 22 to 25 (median 23), manifests
from 6 to 13 (median 8), and original scan medians from 3.543 to 6.439 ms
(median 4.409 ms). Snapshot counts include initialization and maintenance.
The 100-table fixture starts with 10,000 rows per table; the one- and ten-table
fixtures start with 100,000. These observations do not isolate table-count cost.
Full distributions, report hashes and actual correctness evidence are in
`target/release34-matrix/assessment.{md,json}`. Only two of the thirteen cases
meet every bounded performance gate; none establishes 24-hour qualification.
