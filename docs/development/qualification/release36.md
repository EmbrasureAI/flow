# Release36 periodic maintenance evidence

This change allows manifest consolidation, snapshot expiry, object reclamation
and checkpoint rotation to progress during a continuously queued CDC workload.
The evidence below includes debug regressions and 24 passing service phases on
one frozen optimized artifact. Performance qualification remains separate; this
is not a production-qualified release.

## Implementation and repository checks

Periodic metadata work has its own due state, independent of soft data builds.
The scheduler permits one periodic visit at a time and requires a ready CDC
dispatch between visits. The same eligibility rule filters timer deadlines, so
blocked periodic entries cannot cause an expired-timer loop. A garbage sweep
visits a bounded page and, when more work remains, continues after yielding
to ready CDC instead of waiting for the ordinary collection interval. Transient
unfenced failures retry after one second. Existing table
ownership, catalog conflict recovery and data-build limits remain in force.

Periodic checkpoints no longer require every table to have no pending operation.
The checkpoint takes the row/control locks and retains pending operation and
snapshot protection. Such checkpoints are still excluded from direct index
restore; the existing catalog-scan rebuild is the safe fallback. A real
RocksDB/Iceberg integration test covers two durable boundaries: a successful
catalog commit whose response was lost while the operation remained Prepared,
and a known commit with two of three index deltas applied. After physical index
loss, both recover exact rows and source ACK, while snapshot expiry retains the
checkpoint's oldest base and the committed operation's successor marker.

The full Rust gate passed 155 active tests across 40 test binaries, with four
manual profiles ignored and no failures. Strict Clippy and the daemon build
passed. The Python harness gate passed all 16 tests, and formatting and Python
syntax checks passed. Logs are `target/periodic-final-workspace-tests.log`,
`target/periodic-final-clippy.log`, `target/periodic-final-build.log`, and
`target/release36-python-tests.log`.
The debug service binary has SHA-256
`45b59e195e03f6d7740c0179071c0ea0f50ffb4c2db6e1c0fba22afb07b73f51`.

## Service evidence

The fixture queues 1,025 transactions before native restart and uses
four-transaction pages. It requires a real manifest rewrite, expiration of an
old snapshot, and deletion of that snapshot's owned manifest-list object from
MinIO while orders' own snapshot LSN remains below the final source barrier.
An Iceberg tag protects the initial reader snapshot. A fresh LSN check at the
reclamation boundary prevents a stale observation from counting as progress
before drain. Checkpoint rotation is also observed before drain.

The matched native-enabled release35 negative control in
`target/release36-debug-campaign-02/old-negative` failed because periodic work
completed only after orders' own backlog drained. The corrected debug binary
passed these cases against PostgreSQL, the REST catalog and MinIO:

| Case and report directory under `target/` | Phases | Object deleted at | Pending then | Full reclamation/drain phase |
| --- | ---: | ---: | ---: | ---: |
| One hot table, two workers: `release36-debug-campaign-02/periodic` | 5 | 46.977 s | 620 | 154.464 s |
| Two hot tables, one worker: `release36-debug-campaign-03/periodic-single-worker` | 5 | 148.735 s | 625 | 395.538 s |
| Two hot tables, two workers, persistent proxy connections: `release36-debug-campaign-05/periodic-with-builds` | 5 | 40.500 s | 633 | 114.875 s |

All three cases completed with 5,118 orders and 17 accounts matching PostgreSQL,
source ACK reaching the final transaction, and all 4,096 tagged initial orders
unchanged. They passed clean shutdown and worker-panic checks. Actual native
data work occurred before reclamation: background builds in the two-worker
case and synchronous rewrites in the single-worker case.

The fixture deliberately uses a four-manifest threshold, two-second snapshot
retention, one-second garbage interval/grace, two-second checkpoint interval,
two retained checkpoints, and a 10 ms delay per proxied catalog request in the
passing cases. These are finite stress-test settings, not default policy or
ingestion latency measurements.

## Frozen optimized artifact

The optimized artifact was built from clean commit
`a9cfadb24f872e4ef40c3658b2d96fe99f9989f0`, tree
`27c1ab3af356da9c7ee373df426028d6820bb8ff`, with Rust 1.97.1 for
`aarch64-apple-darwin`, the release profile and thin LTO. Its build manifest is
`target/qualification-binaries/release36/build-manifest.json`.

- Daemon SHA-256: `c646a1bb39fdcc92b09dd349088ed8387130dcaa352d4005063a7fcbd64677a7`.
- External compactor SHA-256: `930c928c398fb145a0e3c0c06f034ee50b9c88f68b768f7b73c1b847fbd02595`.
- Cargo.lock SHA-256: `2d84ac4f5caa3d832aa41c8020efcb083513c12e936309bd9d9259f8021aa8af`.

The serial optimized campaign in `target/release36-campaign/summary.json` passed
all 24 phases against the durable PostgreSQL 18.6, REST catalog and MinIO fixture:

| Case | Phases | Elapsed | Report SHA-256 |
| --- | ---: | ---: | --- |
| Periodic cleanup with two hot tables | 5 | 99.670 s | `3729428eb19abf5ea6cab0101c37208e7752a268590de67bae516901f590246d` |
| Concurrent compaction and recovery | 7 | 180.986 s | `51074bf9c91a7f4db2ce5f4e2767670570e96d0d0cffdc26ee6b84853a28f118` |
| Durable service faults | 12 | 74.120 s | `5ac326cf4d7db359f310dfd81a59c8580350d3704697706c4c667f1669467abd` |

The optimized periodic case deleted the old manifest list at 30.945 seconds
with 592 transactions pending, six checkpoint rotations and 32 background builds.
It then verified all 5,118 orders, 17 accounts, source ACK and the 4,096 tagged
initial orders. The concurrent case covers held-read CDC, translated deletes,
hard-debt ACK pause, deadline cancellation/join and interrupted-build restart.
Fault coverage includes lost successful commit responses, actual durable service
restarts, crash/journal replay, physical index loss, wide/unchanged TOAST and an
independent reader/manifest audit. The external compactor required three supervisor
restarts across outages; this is recovery evidence, not uninterrupted availability.

## Hundred-table performance control

The four-worker control in `target/release36-performance` failed source admission,
so its strict campaign stopped before running the eight-worker comparison.
All 6,610 accepted transactions reached the journal, catalog and reader, and the
final 1,066,100 rows across 100 tables matched PostgreSQL with exact final ACK.
However, 390 of 6,000 measured arrivals were missed. Catalog p95/p99 was
23,591.508/25,107.711 ms, drain was 26.970 seconds, and the maximum reader ratio
was 1.546. This is an overall failure, not evidence of a worker-count improvement.
The report SHA-256 is
`2b66f7c3e1692f1e4a7e010e638493b55f46fd24b43c57ba13bac65cd0f1d38f`.

The retained source-arrival assessment finds bursty client response delays:
28 PostgreSQL transaction intervals exceed 250 ms, with approximately 91% of
their cumulative duration after the PostgreSQL commit timestamp and before the
client receives success. That timestamp precedes the durable commit response;
it does not distinguish WAL flush, transport or Python scheduling delay.
The recorder queue peaks at 45/256 items and its longest enqueue is 100 microseconds.
WAL-triggered checkpoints overlap 227 missed arrivals, while the largest
163-arrival cluster falls between them. Checkpoints alone are not an established
cause. Producer thread stacks and backend wait events are the next diagnostic;
PostgreSQL settings, workload thresholds and runtime defaults remain unchanged.

The first instrumented attempt, `target/release36-source-diagnostic`, terminated
with SIGSEGV in CPython 3.13's asynchronous `faulthandler` stack dump before
completing the workload. The macOS crash report resolves through
`_PyFrame_SafeGetCode`, `dump_frame` and `faulthandler_user`; the retained
`crash-assessment.json` records its hash and cleanup of the owned daemon. This
is a failed diagnostic, even though its sampler summary's `diagnostic_complete`
field is true. It establishes no throughput or correctness result. A follow-up
uses ordinary Python thread sampling instead of asynchronous signal dumps.


## Instrumented concurrency comparison

The revised Python sampler uses `sys._current_frames()` from an ordinary thread,
with PostgreSQL waits sampled independently in the parent process. Both runs
completed their diagnostic, provenance, full-row and ACK checks. The same frozen
binaries, workload, thresholds and sampling were used, with four versus eight
table workers. Each admitted all 1,000 warmup and 6,000 measured transactions and
verified 1,070,000 rows across 100 tables.

| Table workers | Catalog p95 / p99 | Drain | Maximum reader ratio |
| ---: | ---: | ---: | ---: |
| 4 | 16,050.124 / 16,205.766 ms | 16.020 s | 1.477 |
| 8 | 1,580.898 / 1,899.653 ms | 1.250 s | 1.392 |

Evidence is in `target/release36-source-diagnostic-02` and `-03`. Their report
SHA-256 values are respectively
`421686dd07a637fa2a6a6e920c5cadab30ce2fc955471447b47e41bf81aebf5f` and
`df4d4b465c977b944abc2168293b76c1be003a2bf3e931840a95a1fdf778652a`.

The manifests, binary hashes, six harness hashes, wrapper hash and all 100 final
row digests match. After normalizing fixture namespaces and artifact paths,
configuration differs only in `table_workers`. Both execute 1,750 ingestion epochs
of four transactions. The retained `-03/comparison.md` and identity JSON record
these checks.

The approximately 90% lower p95 with eight workers supports the worker-capacity
explanation for this workload. Both still fail the one-second p95 and 1.25 reader
ratio targets. The instrumentation changes overhead, and a serial pair is not
endurance evidence. Runtime defaults remain unchanged; the failed uninstrumented
control is not replaced by these diagnostic results.

The four-worker diagnostic captured 13 source transactions over 100 ms, peaking
at 220.584 ms. Backend samples show COMMIT waiting on `WalSync`/`WALWrite` while
Python writer stacks wait in psycopg transaction exit. These smaller
tails have direct backend-wait evidence. No transaction exceeded 250 ms, and
Python sampling gaps peaked at 175.462 ms, so the original near-second missed-arrival
bursts did not recur and their cause remains unresolved. No source SQL, WAL policy,
durability setting or arrival threshold was changed.

The remaining eight-worker tail is predominantly before collapse: the slowest
5% of measured transactions average 1,698.3 ms from journal to collapse and
104.6 ms from collapse to catalog. During those earlier intervals, reconstructed
known occupancy averages 6.91 of eight workers: 3.64 in strict rewrite phases,
2.44 in ingestion and 0.84 in background phases. This points to transient worker
congestion involving synchronous maintenance, not background builds occupying
most slots. The exact queue/refresh/control split is not instrumented, so this
result does not establish a more specific serialization bottleneck.


## Retained failures and limits

The initial `target/release36-debug-campaign` used disabled data rewrites with
two-second retention. It exhausted the recoverable age history of unreclaimed
L0 files and reached the conservative hard pause for unknown age. Its failed
reports remain retained; this fixture did not establish the intended periodic
fairness regression. Native data maintenance was enabled for the matched cases.

The first single-worker run in `release36-debug-campaign-02` timed out during
drain at 180 seconds with 321 transactions pending. Repeating the full fixture
with a 600-second diagnostic deadline passed; that increase is not a production
latency SLO change. Fixture corrections did not change the tested production
code. The additional `release36-debug-campaign-03/periodic-with-builds` attempt
failed with a local `Can't assign requested address` transport error and is not
counted as a pass. The retry in `release36-debug-campaign-04` reclaimed the
manifest list at 88.970 seconds with 661 transactions pending, but failed its
final DuckDB read with an empty S3 HTTP response. It also remains a failed run.
Socket samples during that retry peaked at 17,216 `TIME_WAIT` connections,
including 7,470 to MinIO and 7,374 to the S3 proxy. Both test proxies were
forcing connection closure for every request. This establishes avoidable test
transport churn; it does not independently prove the cause of either failure.

The proxy repair gives each downstream handler one reusable upstream connection,
closes it on handler exit or transport error, and never replays an uncertain
write. S3 chunk signatures are forwarded unchanged; decoded response chunks keep
close-delimited framing. Seven local HTTP tests cover reuse, HEAD/bodyless framing,
chunked uploads/responses, truncation, idle disconnects and lost-response semantics. The passing
`release36-debug-campaign-05` repeat used the same Rust binary and retained all
functional assertions. Its sampled `TIME_WAIT` peak was 298; seven checkpoints
and 37 native builds preceded the reclamation boundary. These counts describe
a corrected diagnostic fixture, not production throughput improvement.

Early scope remains unpartitioned Iceberg v2 with position deletes and external
compactor reconciliation. Z-order, distributed compaction, HA and global
autocompaction remain excluded. These short local tests do not qualify
50,000 mutations/s or 24-hour endurance. Raw reports and logs remain in ignored
`target/` directories; this tracked record preserves their identities and limits.
