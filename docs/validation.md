# Validation gates

Validation follows the complete path from PostgreSQL commit through durable capture, catalog publication, independent reader visibility, index recovery and source acknowledgement. Passing a component test does not qualify sustained throughput or a deployment. The accepted scope is in [OSS readiness](oss-readiness.md); external compactor reconciliation is included. Z-order, a distributed compaction protocol, HA and global autocompaction are excluded from early functionality.

## Reproducible gates

Run `cargo test --locked --workspace --all-targets`, `cargo fmt --all --check`, and `cargo clippy --locked --workspace --all-targets --no-deps -- -D warnings`. Project integration tests use real RocksDB databases, journal segments, Parquet and Avro files, the stock Iceberg Rust reader, scripted wire peers and child-process exits. Vendored Apache source may emit existing upstream warnings. A test count is not an acceptance contract; use the complete gate against the exact revision being released.

Release29 adds bounded current-epoch memory collapse with complete disk replay on
capacity exhaustion. Its [full Rust gate](../target/release29-validation-02/report.json)
passes 128 active tests, strict Clippy, formatting, the release build and Rust 1.94;
one pre-existing test remains ignored. Rust source hashes are stable throughout.
The daemon hash is
`5a498e7adeaffd07f4cd2ed0c7090693815bee1c80ef36ac87c42063522fc306`.
The actual-file checks cover memory/disk/fallback equivalence, retained-capacity
accounting, stale full-state/head rejection before artifacts, keyless replay,
updates/deletes/key moves, lost responses and historical schemas. A
[finite comparison](../target/collapse-buffer-profile-29/runs-01/report.json)
passes all fourteen cases; service qualification remains separate.

The release28 source passed formatting, all 124 active workspace tests, strict
Clippy and Rust 1.94 checks, with one pre-existing ignored test. Its immutable daemon is
`eae6d39db4ab2f1ea214965065af88d488eeb2edfa2e7a4eece72bcce70aa204`;
[binary and source provenance](../target/qualification-binaries/source-release28.json)
are retained separately from later documentation edits. Live reports identify
their own binary and scope below.

Release28 reuses one journal cursor per epoch, overlaps one bounded compaction
mapping stage with the next batch's checks, caps mapping payload bytes, and adds
coarse StateStore collapse timings. Actual-file tests cover cursor errors and
rollover, wide-key admission and staging join before failure/recovery. The
[full gate record](../target/release28-validation/report.json) verifies unchanged
Rust source throughout the checks. These component results do not qualify
service throughput or latency.

Release27 adds the measured single-file reverse-owner range proof and overlaps
one batch's index staging with the next data write. All existing ownership,
expected-value, cursor and durability checks remain. The expanded actual-database
recovery test verifies unordered positions, partial apply/reopen, gaps and
collisions. A real file/database join test holds staging while a writer fails;
the result cannot return until staging finishes, and reopening retains Building
for safe discard. All 120 workspace tests passed, with one existing ignored
test. The new timing fields distinguish pair wall time, overlap and dispatch
overhead. These checks do not establish an end-to-end performance gain.

Release26 preallocates private index/spool keys without changing their bytes or
format. It also measures successful source-registration page sizes and elapsed
time, separately from overlapping journal sync. The [bounded database comparison](../target/key-capacity-profile-25/assessment.md)
records twelve correct samples and modest, workload-specific improvements.

Release25 introduced journal payload-byte and maintenance work observations.
Its full live workload verifies all completed rewrite counts. The first release26
gate exposed an intermittent scoped tracing assertion in the partial-staging
fixture. A [deterministic library probe](../target/telemetry-capture-26/diagnosis.json)
confirmed a cached disabled callsite. Removing the fragile capture kept every
physical failure, restart and row assertion; the complete gate then passed.
Persisted formats, scheduling policy and resource budgets are unchanged. The
three standard-library harness tests last passed on release25.

Release24's only Rust changes reconcile an idle nullable schema addition before
maintenance. The helper verifies the indexed head and pending fence, the exact
catalog schema and durable source registry, and the supported nullable suffix.
It changes only the table's schema version, retaining the materialized LSN and all
physical row mappings. The extended actual-file schema fixture checks those
invariants, stale-build rejection, new-schema compaction and replay of mixed
historical/current schema mutations. Index format 3 is unchanged. The rejected
dictionary candidate was removed before the complete release24 checks and build.

The Rust integration coverage includes streamed transactions and nested aborts, unchanged TOAST handling, bounded journal descriptors and ledger batches, torn-tail recovery, quotas, single-writer ownership, typed values, key changes, transaction collapse, source-wide ACK ordering, and shared-delete handling. Recovery tests cross Prepared, catalog-proof and partially applied index boundaries. Separate control-store tests remove the complete index, preserve source and table authority, fence revision mismatches, and exercise checkpoint activation around process exits. Artifact registration tests cover ownership before PUT, unresolved operations and recovery-only control writes.

The controlled-ledger lock regression holds an actual row-collapse operation
open while persisting source progress, then performs concurrent table/ledger
updates, creates a checkpoint and reopens matching authority. The original lock
path fails this gate; the narrow ledger path passes. A real read-only control
DB forces failure after the writable index has advanced: subsequent source/table
commits are rejected, ledger authority remains unchanged and reopening fences the
revision mismatch. No production fault hook or additional lock was added.
The [call-path assessment](../target/production-cpu-profile-22/callpath-assessment.md)
records the motivating profile and the separate correctness evidence.

The bounded group-commit change passed all 21 tests in
`cargo test --locked -p flow-ingress-journal -p flow-pg-source`, plus strict Clippy
for both crates and all targets. The actual-file gates cover atomic reader
frontiers across multiple journal segments, stable concurrent cursors, staged
chunk retention during reclamation, ordering and descriptor limits, and recovery
of complete transaction prefixes from an unflushed or torn group. One failure
test writes real terminal and index files, then uses a `/dev/null` file handle to
make the OS sync syscall fail. It verifies that the reader frontier stays
unchanged and the writer remains poisoned until reopening resolves durability.

Capture defers publication and acknowledgement while terminals are staged.
Source spools remain retained until the shared journal barrier succeeds; the
buffered integration fixture verifies this alongside mixed schema versions,
savepoint rollback, unchanged TOAST, key moves and deletes. These component gates
do not establish throughput. The release10 service recovery result is recorded
below as `faults-08`, alongside the completed `contracts-03` schema and lifecycle
gate. The completed group-commit probe and later failed workloads are recorded
in [production qualification](production-qualification.md#group-commit-and-batched-index-reads).

Mutation-count compatibility gates reopen real journal files containing terminal
kinds 2, 4 and 5 and real control-store ledger entries from unprefixed, `FLLEDG02`
and `FLLEDG03` formats. Surviving streamed parent/child rollback produces exact
per-table counts; malformed coverage and overflowing sums cannot stage a new
terminal. Runtime admission tests restore an in-flight prefix ahead of later
work, preserve oversized and unknown-count transactions whole, and keep ACK
behind every affected table. Actual-Parquet publication tests distinguish source
mutation counts from collapsed rows and advance a zero-row transaction without
creating an empty snapshot. These focused gates passed; release20 and earlier
service/performance reports do not qualify this later row-trigger change.

The direct residual-stream merge passed the actual-file concurrent-rewrite and
delete-rewrite suites. A shared delete introduced during BUILD overlaps the
original residuals while late CDC creates a replacement mask; the resulting two
cohorts contain exactly four unique positions, preserve sequence applicability,
leave the base data unchanged and match current and historical stock-reader rows.
The same suites cover lost responses, partial staging, stale builds and index
loss. These checks establish correctness, not a measured performance gain.

Parquet admission tests exercise actual compressed files, dictionary expansion
with and without decoded-size statistics, decimal widths, field projection,
rowgroup/file rotation, and rejected I/O ranges. Residual-delete tests keep a
large base file physically unchanged while compacting L0, including a lost
catalog response and complete index loss. They verify historical rows, delete
sequence applicability, deduplication, exact live-row counts, and subsequent CDC.

Concurrent data-build integration captures a real immutable RocksDB index view,
then publishes intervening updates, deletes, key moves and delete/reinsert before
finalizing. Actual-file tests verify translated position masks, original data
sequences, residual deletes, exact current and historical rows, and index density.
Lost-response cases recover the corrected plan after reopening and after physical
index loss. Independent data rewrites and delete-only rewrites both invalidate
selected inputs safely. Durable BUILD tests verify GC/history protection, control
copying across generations, startup abandonment and normal Prepared promotion.

The lost-response pipeline tests keep recovery reads unavailable after the catalog commits, close and reopen the index at durable Prepared, then reconnect. They verify reader visibility, no duplicate logical publication, and recovery through a subsequent external rewrite. A response loss that can be resolved inline is not substituted for that crash boundary.

## Current local service evidence

The following reports were produced on 2026-09-04 UTC. Paths are retained local artifacts, not source-controlled release assets. Services use PostgreSQL 18.6, durable PostgreSQL-backed Iceberg REST 1.10.1, MinIO and actual S3 objects. Each report identifies its individual phases and preserves logs and failure evidence.

| Boundary | Passing report | What crossed the boundary |
| --- | --- | --- |
| Parallel bootstrap and concurrent CDC | [bootstrap-02](../target/production-bootstrap-02/report.json) | Killed a large COPY after another table published and CDC reached the journal; recopied only the unfinished table; killed after its durable terminal; resumed staging; removed the active index; rebuilt and continued updates/deletes with exact DuckDB rows. |
| Schema change during partial bootstrap | [bootstrap-ddl-03](../target/production-bootstrap-ddl-03/report.json) | Release29 passes all five phases: interrupted COPY after another base and CDC became durable, nullable-column addition before recopy, crash after the staging terminal, publication from retained staging with 27 concurrent source commits, and total index loss followed by exact DuckDB updates/deletes. |
| Schema and lifecycle contracts | [contracts-08](../target/production-contracts-08/report.json) | All eleven phases passed on release24: idle nullable additions with lost catalog response and native compaction, queued schema versions across crash, streamed DDL abort, savepoints, historical schema widths, checkpoint retention/restore, SIGTERM/status and primary-key drift. No worker panics after stopping. |
| Scheduler liveness and health cancellation | [health-04](../target/production-health-04/report.json) | Stalled only the WAL-health query while two-table CRUD/key moves published; held catalog commits to verify `table_workers = 1`; observed checkpoint creation, a five-second query timeout without overlapping connections, and prompt SIGTERM with readiness false. |
| CDC priority and maintenance recovery | [maintenance-priority-02](../target/production-maintenance-priority-02/report.json) | All six phases passed: initial COPY/handoff, source publication and ACK before a held soft-pressure compaction with CDC still queued, SIGTERM before the compaction's catalog outcome, restart recovery with exact current/history rows, and hard-pressure publication/ACK blocking until compaction completed. |
| CDC during local compaction builds | [concurrent-compaction-07](../target/production-concurrent-compaction-07/report.json) | All seven phases passed on release28: concurrent CRUD/ACK, translated masks, history, hard-debt blocking, real cancellation deadline and join, and SIGTERM/restart with BUILD and scratch cleanup. Logs contain no worker panics. |
| Unsupported source changes | [column incarnation](../target/production-contracts-column-incarnation-01/report.json), [non-null default](../target/production-contracts-non-null-default-01/report.json), [volatile default](../target/production-contracts-volatile-default-01/report.json) | Rejected changes whose physical or historical meaning could not be safely inferred; tested acknowledgement fencing. |
| Durable service failures with local compaction | [faults-15](../target/production-faults-15/report.json) | All twelve correctness and recovery phases passed on release28: successful commit with lost response, HTTP outage, REST/MinIO/PostgreSQL restarts, SIGKILL with unacknowledged durable work, successful catalog commit followed by physical index loss, wide TOAST updates and manifest audit. Both crash phases prove current Prepared requests succeeded after the old process stopped and before replacement. Exact 12,080 orders/17 accounts and 4,096 historical rows; no unplanned daemon exits or worker panics. The external compactor required three supervisor restarts. |
| External compactor backpressure | [pressure-01](../target/production-pressure-01/report.json) | Hard reader debt paused publication and ACK while capture continued; independent compaction reconciled and released the table; subsequent mutations and history remained correct. |
| Retained delete-budget backlog | [budget-recovery-03](../target/production-budget-recovery-03/report.json) | Release13 resumed the failed matrix09 state and permanent slot with unchanged budgets; cleared 13,794 pending transactions; matched all 447,100 rows; preserved the 309,160-row historical snapshot, identities and original report; reached the journal ACK barrier and passed restart. |
| Independent Java readers and maintenance | [readers-06](../target/production-readers-06/report.json) | Sixteen phases on release23 with native compaction enabled; independently attributed Spark data/delete/manifest rewrites, exact keyed and keyless duplicate rows in Spark/Trino/DuckDB, retained history, subsequent CDC and restart. |
| Linux container packaging | [linux-demo-01](../target/production-linux-demo-01/report.json) | Fresh isolated Compose volumes, Linux-built daemon, Trino CRUD/key moves, UID 10001 volume ownership, SIGTERM exit 0, stopped status, queued changes across restart and idempotent initialization preserving the slot. Cleanup removed only the validation project. |
| Index format upgrade | [upgrade-01](../target/production-upgrade-01/report.json) | Actual release05 format2 to release06 format3 upgrade with queued WAL, rebuilt generation, preserved source/control identity and history, exact CRUD/key moves, and generation reuse after another restart. |
| Fixture compactor SIGINT | [compactor-shutdown-01](../target/production-compactor-shutdown-01/report.json) | A held catalog read reproduced the lost SIGINT on release21; release22 retained the signal, completed the pass and exited 0 in 18.4 ms after release. The retained first table was already compacted, and the proxy saw reads only. |
| Composite keys and capture quota | [boundaries-02](../target/production-boundaries-02/report.json) | Seven phases passed on release23: shared first key components, component changes, delete/reinsert, pending-work SIGKILL and complete index rebuild, actual 64 MiB journal exhaustion with ACK fenced, same-slot recovery and final restart. Final 9,219 orders/17 accounts and both retained histories match; no worker panics. |
| Counted journal and ledger upgrade | [journal-upgrade-01](../target/production-journal-upgrade-01/report.json) | All nine phases passed from release20 to release21: SIGKILL with two six-transaction Prepared epochs and ten durable pending transactions, recovery of the original operations without rebuilding the index or another COPY, counted no-op/large-transaction publication, queued WAL across restart, exact current/history rows and no stopped worker panics. |

The release23 schema refresh [contracts-06](../target/production-contracts-06/report.json)
failed after COPY and WAL handoff. A supported idle nullable addition advanced the
catalog schema without advancing the local table schema watermark; a subsequent
compaction build rejected the mismatch and stopped the daemon. The actual schema
commit response had been lost and reloaded. This retained failure is a correctness
regression, not a passing release23 schema gate or proof of index corruption.
Release24 fixes it. The first rerun, `contracts-07`, then failed a fixture
assumption that the head snapshot could not change while background compaction
was active. The corrected fixture proves the schema POST itself adds no snapshot
and attributes every intervening physical rewrite to a completed native operation
at the exact previous table LSN. Full [contracts-08](../target/production-contracts-08/report.json)
passed all eleven phases with 4,085 final orders and 17 accounts, exact retained
history and no worker panics. Its execution provenance records the updated fixture
separately from the unchanged release24 binary.

The [faults-11 evidence summary](../target/production-faults-11/summary.json)
correlates both index-loss operations with their current-phase Prepared records
and successful held catalog requests. The original report's nested request list
incorrectly reused earlier-phase events; preserved raw proxy/daemon logs supply
the correct proof. The corrected fixture passed all twelve phases in
[faults-12](../target/production-faults-12/report.json), requiring matching
current-phase success while the old process is stopped and before removing the
index or starting a replacement. It audited 41 snapshots and 17 external
compactions and stopped with no pending transactions. The original faults-11
report remains unchanged.

Release27's first [fault run](../target/production-faults-13/failure-summary.json)
passed nine phases, then failed to obtain the required successful commit after
SIGKILL: the independent compactor advanced both catalog heads while the current
Prepared POSTs were held, so both returned HTTP 409. Index loss and replacement
recovery were not attempted. This is a failed test setup, not demonstrated row
corruption. The report and raw requests remain unchanged.

The corrected [faults14](../target/production-faults-14/report.json) passed all
12 phases with the same frozen release27 binaries. The fixture gracefully joined
the independent compactor before two consecutive forced-success crash phases,
then established a fresh both-table materialization checkpoint before each hold.
Both phases independently proved current Prepared requests succeeded after
SIGKILL and before replacement or index removal. External compaction resumed
after both recoveries. The final audit matched 12,080 orders, 17 accounts and the
4,096-row historical snapshot, with 50 retained snapshots and 16 external
compaction commits. There were no unexpected daemon exits or worker panics;
two external-compactor supervisor restarts remain availability incidents.
The [execution manifest](../target/production-faults-14/command.json) records the
updated fixture separately from the immutable production source archive.

Release28 repeated the unchanged corrected fixture in
[faults15](../target/production-faults-15/report.json): all twelve phases passed.
Both forced-success crashes recorded HTTP 200 for the current orders and accounts
Prepared operations after SIGKILL; the second then removed the physical index
and verified updates, deletes and key moves after reconstruction. Final rows
matched 12,080 orders and 17 accounts, with the 4,096-row history unchanged.
The audit covered 48 snapshots and 14 external compaction commits. The independent
compactor needed three supervisor restarts during service faults; these remain
availability incidents. Final pending transactions were zero and all source
frontiers reached 87,713,688,560. The [campaign manifest](../target/release28-campaign/execution/summary.json)
records binary/fixture hashes and empty owned process groups between gates.

The counted-descriptor upgrade used old daemon SHA-256
`9534b8c93807d48d524e981fb9cbdc10d958321b6a8077561b586d60ceecf8f6`
and new daemon SHA-256
`a014a32b00470c03e389638347876beb12cace2434416b8e1120dfb13bc06348`.
Before SIGKILL, the actual catalog POSTs were held, journal durability reached
`D/B88BCC58`, and both materialization and PostgreSQL ACK remained at
`D/B88B9680`. Release21 preserved source, control, index and configuration
identities and resolved both original six-transaction operations. A subsequent
five-mutation insert/delete no-op advanced ACK without a snapshot; the streamed
14,008-mutation transaction created exactly one snapshot per table. Final queued
WAL recovery matched 15,074 orders and 18 accounts, retained six pinned table
snapshots and the initial 4,096-row orders snapshot, and stopped with zero pending
transactions and all frontiers at `D/B941ECC0`. The new daemon exited 0 and no
worker panics were recorded. Balanced priority and disabled compaction with
one-hour/two-hour L0 age limits isolate compatibility; this gate does not qualify
latency, maintenance performance or earlier service suites on release21.

The earlier bootstrap and contract reports precede binary-hash recording in the shared harness; their logs remain useful boundary evidence but do not identify a release binary by digest. [Faults-06](../target/production-faults-06/report.json) used the release binary SHA-256 `907c9e69f1cb69289d6c732de7dd531f8ecf1c764deb3407e5e6688852b90de7`. Reader and pressure checks used the later debug binary `66228c024035fab55beefa2713f4ab62754b4a1977c93685281d4916ba3a51a7`, including configurable compaction policy and table pause/resume. The Linux demo recorded its separately compiled container binary `81b02536889bf58193878a2614da8eaa98c16e91cb342df124e37abae69920d2`. These are distinct tested builds, not a claim that every suite ran on one final artifact. Repeat the release gates after subsequent changes.

The earlier [faults-10](../target/production-faults-10/report.json) and
[contracts-05](../target/production-contracts-05/report.json) used immutable
[release19](../target/qualification-binaries/release19.json), daemon SHA-256
`b80f08320260afc2a12b0d262d937a4759214cb5b05ad8e9b97def17ae21f2e8`.
Fault recovery matched 12,080 orders and 17 accounts, preserved the initial
4,096-row snapshot, and audited 39 retained snapshots and eleven external
compaction commits. Its independent compactor required three explicit supervisor
restarts during injected outages; uninterrupted availability is not claimed.
The schema/lifecycle suite finished with exactly 4,085 orders and 17 accounts.
Both suites checked stopped-process logs and found no worker panics.

The latest [concurrent-compaction-07](../target/production-concurrent-compaction-07/report.json)
passed all seven phases on immutable [release28](../target/qualification-binaries/release28.json),
including bounded mapping-stage overlap and reusable journal replay.
Cancellation requested at 30.002 seconds retained BUILD; actual join and
retirement occurred at build age 37.039 seconds after the held read was released.
SIGTERM exited 0 in 1.022 seconds with I/O held. Restart reclaimed BUILD/scratch
and matched 36,089 orders and 17 accounts, retaining exact historical rows.
Final received, journal, materialized and captured frontiers were all
87,702,360,864 with no pending transactions. Two S3 broken-pipe errors came
from deliberately held GETs; there were no catalog errors or worker panics.
Full Rust, formatting, strict Clippy and Rust 1.94 gates also passed on this
artifact. The prior [release22 gate](../target/production-concurrent-compaction-06/report.json)
remains retained as evidence for its own build.

[Concurrent-compaction-04](../target/production-concurrent-compaction-04/report.json)
passed all seven phases on release16 SHA-256
`e949003057d57cd8ef588901bfa2b5c0d11b265df7e7cf6d1b9a1d4a847ceea6`,
after the one-pass catch-up staging, batched row processing and owned worker
runtime changes. Cancellation retained BUILD at 30.003 seconds; actual join and
retirement occurred at build age 33.021 seconds, after the held read was released.
SIGTERM exited 0 in 1.030 seconds while I/O remained held. Restart reclaimed
abandoned BUILD/scratch and matched 36,089 orders and 17 accounts. The initial
36,096-row snapshot and 36,093-row deadline base stayed exact. Final stopped
status had no pending work and all watermarks at `9/6E6E8CF0` (40507444464).
Both recorded S3 errors came from deliberately held GETs; no catalog errors or
worker panics occurred. The fixture now rejects logged worker panics even when
the process exit code is zero.

The historical `concurrent-compaction-03` passed all seven phases on immutable release14,
SHA-256 `b8f430122b6b02d3a2e6c4fedd107bf1a28557790be3793537f6237869d5f2bf`.
The new deadline phase requested cancellation at 30.003 seconds and verified the
durable BUILD record. The selected read stayed held for 32.991 seconds after
observation; neither BUILD nor scratch was retired while held. Actual worker join
and durable retirement occurred at build age 37.038 seconds, after release. The same daemon
resumed CDC and source ACK, preserved the 36,093-row build base and 36,096-row
initial history, and never published the cancelled operation. The subsequent
SIGTERM exited successfully in 0.011 seconds; restart removed the abandoned BUILD
and scratch and matched 36,089 orders and 17 accounts. Final stopped status had
zero pending transactions and received, journal and materialized watermarks all
at `7/F4478BE8` (34163100648). Five S3 transport errors were deliberately held GETs
whose clients timed out; no catalog errors occurred. These are correctness and
liveness results, not workload qualification or a rerun of other service gates.

The historical `concurrent-compaction-02`, `faults-09` and `contracts-04` ran
sequentially on immutable release12, SHA-256
`0ebc7471c1edd8f792fcec846e37bcac17b6ba2158b8984161964059290cb6f5`.
The concurrency gate kept the default four table workers and 10s/30s L0 debt
limits. While a selected Parquet GET was held, two real transactions containing
updates, deletes, key moves and delete/reinsert reached the catalog and source
ACK. Catch-up retained data sequence 4, emitted eight translated position masks
at sequence 5 and committed at sequence 6. Initial and build-base history stayed
exact. The hard-debt phase held source ACK until the active build completed.
SIGTERM exited successfully in 0.011 seconds with the read still held; restart
retired one durable BUILD and removed its abandoned scratch directory. Final
rows matched 36,091 orders and 17 accounts. The sole S3 transport error was the
deliberately canceled held GET. The [repeatable command](../tests/production/README.md#cdc-during-a-compaction-build)
and full traces retain the exact process boundaries. The preceding debug
[concurrent-compaction-01](../target/production-concurrent-compaction-01/report.json)
passed its six phases before scratch reclamation was added; that report remains
unchanged and does not establish the later cleanup boundary.

`faults-09` matched all 12,080 orders and 17 accounts, audited 35 retained
snapshots and nine actual external compaction commits, and preserved the initial
4,096 orders. The independent compactor binary was SHA-256
`2e2233faa75a8346520b2eb51f4e15e94fc0b14f3684b85c0c3e0934ef2db359`.
It exited during the catalog HTTP outage, REST restart and PostgreSQL restart,
requiring three explicit supervisor restarts. No unplanned daemon exit or
additional catalog-pool restart occurred. The report records
`availability_without_supervisor_restart: false`; correctness recovery does not
imply uninterrupted availability of that independent test service.

`contracts-04` passed all eleven schema and lifecycle phases on the same release12
binary, ending with exactly 4,085 orders and 17 accounts after restored
primary-key fencing. These three suites qualify their tested correctness and
liveness boundaries, not sustained throughput or ingestion latency.

`faults-08` and `contracts-03` used immutable release10, SHA-256
`66e10f7b0cb08f97254414aaef48a28d95a94dc450c631be3ff4deb41d8cac4a`.
The fault run's final comparison matched all 12,080 orders and 17 accounts. Its audit covered
39 snapshots across the two tables and 12 external compaction commits; the
initial 4,096-row orders snapshot remained unchanged. No unplanned daemon exit
occurred. The independent compactor exited during the catalog outage and REST
restart and required two supervisor restarts, so the report explicitly records
`availability_without_supervisor_restart: false`. This run had no additional
catalog-pool restart incident after PostgreSQL restarted.

`contracts-03` completed all eleven phases on that same binary, including source
schema changes and recovery while group commit was enabled. Its final comparison
matched 4,085 orders and 17 accounts after checkpoint restore, restart and
primary-key drift fencing. These service gates qualify their correctness and
recovery boundaries; they do not establish sustained throughput.

[Faults-07](../target/production-faults-07/report.json) remains a failed fixture
run on the same release10 binary. Its row comparison completed after physical
index loss, but an inherited log filter hid the INFO-level
`index_generation_activated` event required by the next assertion. The fixture
now sets `RUST_LOG=info`; `faults-08` observed activation and completed the
remaining phases. The earlier report also preserves its separate catalog-pool
and compactor restart incidents. Neither the missing log event nor the later
passing run turns `faults-07` into a completed gate.

`health-04` used debug binary SHA-256
`cebe8ec88b290c1b97dd223fb19f2a733a11d7612cfa43721529ca60ffcc31eb`.
Its blocked health query closed after 5.002 seconds, only one health connection
existed at a time, and SIGTERM exited with code 0 in 0.011 seconds. The earlier
[health-03](../target/production-health-03/report.json) exposed the unclosed
PostgreSQL connection task after client timeout; the passing run includes explicit
driver ownership and cancellation. Health-01 and health-02 retain separate catalog
setup failures from stale JDBC connections after the fixture PostgreSQL restart;
REST was restarted before health-04. The [health gate command](../tests/production/README.md#health-and-scheduler-liveness)
repeats these process boundaries without service restarts. This is liveness
evidence, not throughput or latency qualification.

`maintenance-priority-02` used the debug binary at
`target/maintenance-priority-fixture/embrasure-flow`, SHA-256
`8c52195f0e8168c3662d7b3feb618a6d402560a2ef3b7bfa83ce2aad579c3865`.
The producer committed 121 transactions during restart; 14 remained pending when
the soft-pressure compaction reached the held POST, 1.859 seconds after restart.
The preceding source transaction was already visible and acknowledged. SIGTERM
exited with code 0 in 0.009 seconds; the held compaction then committed while the
daemon was stopped. Recovery and the subsequent hard-pressure gate matched all
4,094 orders and 17 accounts, preserving the 4,096-row initial snapshot. This
qualifies the scheduling and recovery boundaries, not ingestion latency. The
[repeatable command](../tests/production/README.md#cdc-priority-and-maintenance-recovery)
uses an unchanged binary and separate artifact directory.

[Maintenance-priority-01](../target/production-maintenance-priority-01/report.json)
retains its successful soft-pressure and unknown-outcome recovery phases and its
interrupted hard-phase setup. That fixture lowered the hard age before draining
the producer's final L0 tail, so the daemon correctly blocked the next seed
transaction. The run was stopped; the second attempt explicitly drained that
tail before changing the threshold. The first report is not a passing full gate.

## Standard readers and external maintenance

The [reader harness](../tests/production/reader_fixtures/README.md) pins multi-platform Docker digests for Spark 3.5.6 and Trino 470. Spark uses the official Iceberg 1.10.1 runtime for Spark 3.5 / Scala 2.12 and AWS bundle with fixed SHA-512 checksums. DuckDB is 1.5.5. Neither independent Java engine loads the Rust implementation.

All sixteen phases in [readers-06](../target/production-readers-06/report.json)
passed on immutable release23 (`da62b6bb44c9fe242742c3b647141f887a9cba58a31c4afaddef7a6d684c35b8`)
with native compaction enabled. Spark performed independently attributed
position-delete (1 → 1), keyed data (3 → 1), manifest (3 → 1) and keyless data
(4 → 1) rewrites, each with positive work and a retained external replacement
snapshot. Native data rewrites occurred before the first external rewrite and
after each subsequent rewrite plus CDC, including restart. This proves
coexistence across source progress and recovery; it does not require simultaneous
file reads by both compactors.

All three engines compared complete ordered fields, including nulls, Unicode,
decimal, negative-epoch dates, microsecond timestamps, UUIDs and binary data.
Final rows matched 4,074 orders, 18 accounts and 533 keyless rows. The keyless
comparison preserves full-row multiplicities, repeated IDs with different
amounts and null amounts. Retained histories remained exactly 4,096 orders and
513 keyless rows. No worker panics were found after the daemon stopped.

The earlier [readers-04](../target/production-readers-04/report.json) passed
thirteen phases with native compaction disabled, independently attributing all
three keyed rewrite procedures to Spark.

Both reader fixtures use explicit 60-second soft and 300-second hard L0 age
limits to accommodate Java reader-job duration; they do not qualify the default
reader-debt latency budget. The separate pressure test checks the pause/resume
contract with its own small thresholds. The [Linux demo validator](../demo/README.md)
creates its own project with no host ports, retains reports and removes only its
fresh containers and volumes.

## Earlier concurrency and performance evidence

The earlier [local service and stress suites](../tests/local/README.md) remain useful regression evidence. A 120-second stress run used three concurrent CRUD writers and an independent background compactor, observed fifty external compactions during writing, committed source transactions while the daemon was stopped, and compared complete rows and retained historical snapshots after recovery. That run predates the separate control store, parallel bootstrap and later schema work; it is not evidence that every newer path has sustained-load qualification.

[Matrix-01](../target/production-matrix-01/matrix.json) and [matrix-02](../target/production-matrix-02/matrix.json) contain failed requested-load qualifications, daemon failures and undrained source/target comparisons. They remain failures. An incomplete drain neither qualifies correctness at the requested load nor independently proves permanent data loss. Later correctness fixes and passing small suites do not retroactively turn these workload results into capacity claims.

The [production qualification report](production-qualification.md) owns measured rates, latency distributions, bottlenecks and later reruns. Keep source commit, synced journal, catalog response and independent reader observation separate. Polling bounds are not exact publication latency, and test-orchestration duration is not throughput. No 24-hour workload qualification has run. Request/byte counters or a local cost model alone cannot establish cloud costs or the specification's steady-state reader amplification targets.

## Remaining release work

- Repeat source conformance on the final release artifact. Contracts and bootstrap/rebuild passed on PostgreSQL 15.19, 16.15 and 17.11 in [source-versions-01](../target/production-source-versions-01/report.json), and 14.24 in [source-versions-02](../target/production-source-versions-02/report.json), alongside the PostgreSQL 18.6 suites above. The first report retains its failed PostgreSQL 14 test assumption about an empty DDL transaction; the second verifies the actual DDL transaction and journal boundary.
- Repeat the complete gates on an immutable release artifact in CI. Broaden reader versions and storage/catalog providers; local MinIO and one REST implementation do not cover every cloud failure mode.
- Complete the specification's workload matrix and 24-hour principal workloads, with sufficient drain, bounded compaction debt, complete observations and separate planning/delete-application measurements. Record failed targets alongside successes.
- Qualify FULL-identity WAL overhead, journal/index footprint, maintenance bytes, storage/catalog requests and configurable-price accounting under declared deployment conditions.
- Maintain dependency/license and image gates; prepare the reusable Iceberg API changes for upstream review.
- Add low-fan-out partitioning and v3 only with their own source, standard-reader, delete, maintenance and recovery gates after the original v2 qualification prerequisites. They are subsequent implementation work, not early-scope exclusions.
