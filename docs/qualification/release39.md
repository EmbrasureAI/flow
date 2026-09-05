# Finish preparation under hard publication pressure

The [release38 50k follow-up](release38.md#full-50k-follow-up) exposes redundant
synchronous rebuilding. Two concurrent L1 candidates finish their data writes,
then exceed the optional 750 ms preparation stall budget while the table is
already paused by hard reader debt. Their completed work is discarded and the
same accumulated L0 debt is rebuilt synchronously.

In the retained log, `flow-l1-rewrite-f74906cf` has 26 inputs and 249,317 output
rows. Its valid catch-up takes 1.673 seconds, but preparation expires first.
The subsequent 32-file synchronous rewrite holds the table for about 8.20 seconds.
An earlier discarded candidate similarly precedes a 4.37-second replacement.
For both candidates the table was already waiting for its build before
preparation handoff. The detailed operation IDs and timestamps remain in
`target/release38-performance/serial-50k/daemon-1.log`.

The scheduler now distinguishes this existing hard-pressure state at handoff.
Hard-paused tables wait for preparation within the remaining original 30-second
build age. Optional preparations retain their 750 ms budget. No later slowness
promotes optional work to the hard path. Timeout ownership, worker joining,
scratch cleanup, and exact-head validation before activation are unchanged.

The existing real-service hard-debt test now delays catalog requests by 1.25
seconds. It requires the same operation to report completed preparation after
one second, without deadline retirement, then verifies its compaction snapshot,
exact current rows and source ACK. The other phases retain late-delete
translation, history, build cancellation, SIGTERM and restart checks.

The frozen release38 negative control fails at the intended hard-preparation
assertion and logs deadline retirement after a 1,262.85 ms publication stall.
Its report is `target/release39-negative-control/report.json`, SHA-256
`bfbd9f92490cac91b4f08dd32743de718219bd29005a82853cc5da58efe3089f`.

The default workspace passes 154 active tests across 40 binaries, with one
manual profile ignored (`target/release39-workspace-tests.log`). Formatting and
strict Clippy (`target/release39-clippy.log`) pass. The frozen optimized service
gate below passes; performance qualification remains incomplete.

The optimized artifact is frozen from clean commit
`2856bd82c613e92dccfa2e9b6965c51d812ee752`, tree
`8d4a5bbd4189d51dbee6892048d07c0768c932d4`. Daemon SHA-256 is
`720b6b1e7e9fcc4838bce9a2f13d8848c5a4bd72c9db38de2869117fa9c62f37`;
the independent compactor is unchanged from release38 at
`776710a6edb2cdee3f5fff225cb90cb21594266fad817b8af7167706d633cbce`.

The first optimized service run passes the new hard-preparation check, then fails
an existing restart assertion that assumed exactly one abandoned BUILD. Logs
show that the accounts table also started maintenance before SIGTERM, and
restart correctly discarded two builds. `target/release39-campaign` retains this
failed run (report SHA-256
`56eb2e824576f31c632fc8cdf1ba4473dbec03d4faeb860cfd10a04fc7df7219`).
The corrected fixture permits the scheduler's bounded one or two
abandoned builds, snapshots every remaining scratch directory after process
exit, and requires all of them to be removed on restart. The dedicated parallel
fixture separately requires exactly two held owners and two recovered BUILDs.
The corrected service campaign uses the same frozen binary.

## Corrected optimized service gate

All 30 phases pass in `target/release39-campaign-02/summary.json`. The serial
campaign verifies frozen binary, manifest and harness hashes before every case,
without competing builds or workloads. The artifact uses Rust 1.97.1 with the
release profile for `aarch64-apple-darwin`; its manifest is
`target/qualification-binaries/release39/build-manifest.json`.

| Case | Phases | Elapsed | Report SHA-256 |
| --- | ---: | ---: | --- |
| Concurrent compaction and recovery | 7 | 179.750 s | `397b7edb14c6856873a71fc8ebd6fa83c32773c9458898168d95b7986e77d7d3` |
| Two held table builds | 6 | 71.930 s | `a44f914029b673519983c1e6a875d34cc2480289bbb48e5f11950d606cb066fa` |
| Periodic cleanup with two hot tables | 5 | 97.940 s | `cf9527e77bc08f50660a85dea546fab5d3b6567b0fe52cf0e2ab41f0c3258c80` |
| Durable service faults | 12 | 74.250 s | `1050322e8d2920a3be989391c909fc92e1939340a65909984e64d39a86015ecd` |

The hard-pressure candidate completes preparation in 1,258.817 ms, survives the
old 750 ms bound and publishes without retirement. Exact current rows and source
ACK pass. The remaining concurrent phases verify translated deletes, retained
history, the unchanged 30-second build cancellation fence, scratch reclamation
after joining, graceful SIGTERM and restart recovery. Parallel, periodic and
fault cases preserve their separate coverage, including external reconciliation.

The independent compactor requires three supervisor restarts during injected
outages. These are passing recovery checks, not uninterrupted availability.
The original fixture failure remains retained. These service results do not
qualify the 50k, reader-health or 24-hour targets.

## Independent readers and external rewrites

All 16 phases pass in `target/release39-followup/readers/report.json` in
368.472 seconds, on the same frozen daemon. Report SHA-256 is
`69fb5981d40f85795f7aef9d1c212654c088bab402d6d25f40326e508a69ba24`.
Spark, Trino and DuckDB verify exact current and historical rows across CRUD,
independently attributed Java position-delete, data-file and manifest rewrites,
continued native compaction, subsequent CDC and restart. Keyless duplicate rows
survive external rewriting and later appends. Final rows match 4,074 orders,
18 accounts and 533 duplicate rows; retained bases remain 4,096 orders and
513 duplicates. No worker panics are recorded.

Reader versions are DuckDB 1.5.5 (Iceberg extension `45163a28`), Spark 3.5.6
with Iceberg Java 1.10.1, and Trino 470. Spark image SHA-256 is
`599722640c8a42ff5ecca329a3faa5371b3bd5c752b320776d5cdd1eed3441ac`;
Trino image SHA-256 is
`a591436ddc20cef15a4424704423326828591727b0e7c40c6ea4165d4564df67`.
The report retains both Java JAR SHA-512 values and complete reader settings.

The fixture uses 60-second soft and 300-second hard L0 age limits to accommodate
Java maintenance. This qualifies interoperability under those settings, not
the default 10/30-second reader-debt behavior or scan latency.

## Full 50k follow-up

The full workload completes with exact accepted data, but fails qualification.
It uses the unchanged serial generator, one table, four writers, 100-row mixed
transactions, 256-byte payloads, 100,000 initial rows, ten warmup seconds and
60 measured seconds. Native maintenance runs during ingestion; the unchanged
independent compactor produces the final-state reader reference afterward.

| Measure | Release38 | Release39 | Required |
| --- | ---: | ---: | --- |
| Measured committed / missed transactions | 24,596 / 5,404 | 28,576 / 1,424 | All 30,000 admitted |
| Warmup committed / missed transactions | 4,254 / 746 | 5,000 / 0 | All 5,000 admitted |
| Source response-window mutations/s | 41,120 | 47,448.33 | At least 47,500 |
| Catalog p95 / p99 | 15.964 / 17.164 s | 9.507 / 9.980 s | Below 1 / 2 s |
| Final drain | 11.731 s | 9.011 s | No growing steady-state backlog |
| Reader ratio | 2.710 | 2.434 | At most 1.25 |
| Exact accepted final rows | 388,500 | 435,760 | Exact source agreement |
| Background / synchronous rewrites through ingestion | 13 / 4 | 21 / 1 | Diagnostic |

Every one of release39's 33,576 committed transactions is observed. Full source
and Iceberg rows match, including after reference compaction; PostgreSQL ACK
matches materialized LSN `23/3EBC3B40` with zero pending transactions.
`target/release39-performance/comparison-identity.json` verifies equal harnesses,
normalized configuration/commands, build settings and reference compactor.
PostgreSQL may have a different postmaster incarnation after the intervening
fault campaign; no resource or durability settings were changed. The unequal
accepted cohorts prevent interpreting the lower latencies as a controlled
speedup. Missed arrivals remain part of the failed result.

The slowest 5% of admitted transactions spend a mean 9.582 seconds between journal
and collapse, versus 0.198 seconds from collapse to catalog publication. The
largest completed preparation/activation publication stall is 2.724 seconds.
These log-derived intervals include I/O waits. Remaining catch-up and activation
cost needs investigation while preserving exact activation fences; extending
deadlines again would not remove that work.

The report is `target/release39-performance/serial-50k/report.json`, SHA-256
`627436a59d4242ef94975d0732233e0e627dae31e516683052193e130a81e8fb`.
The retained release38 baseline is
`71fc5b3536095956b1d1f0e712abe29a43263ee9910aa89a99d9c90740ac0ae2`.
The serial follow-up runner records exit status 1 and complete report identity
in `target/release39-followup/summary.json`. This is a finite local workload;
the 50k, reader-health and 24-hour production gates remain unmet.

## Prepared-delta allocation follow-up

The next source change decodes prepared mappings directly from the RocksDB raw
cursor, avoiding temporary owned copies of both the key and encoded value.
Decoded rows remain owned; prefix bounds, checksum defaults, block-cache fill policy and
terminal error propagation remain unchanged. No storage format or activation
fence changes. This change is not part of the frozen release39 binary above.

An isolated optimized measurement stages and seals 218,743 deterministic rewrite
mappings through the normal state-store API, closes the database and opens it
read-only. All old/new decoded records agree, with length-framed bincode SHA-256
`42dbfcb0f5da754685f42b10bb51519ebf0564c9f4fcfb483da91ebe1b2cb9e0`.
Twelve alternating warm scan pairs give median times of 62.317 ms for the generic
iterator and 54.493 ms for the raw cursor; every pair improves, with a median
paired reduction of 12.174%. This is synthetic real-RocksDB scan evidence, not
retained workload replay or an end-to-end latency result. The initial attempt
to replay the completed service operation failed because it had been retired.

The local report is
`target/prepared-delta-cursor-review/synthetic-report.json`, SHA-256
`41934eb1c571199500ebe18c8e82a6d8c8850324484e4dfbad7b0749aa8db4e4`;
measurement helper SHA-256 is
`de53267b08fbcfbce56115d486a70172c1564d6577e98801bafec23dd2f587cd`.
The temporary test-only measurement module was removed before the workspace
gate. The permanent regression checks sealed/unsealed and empty operations,
operation isolation, staging order, repeated exhaustion and owned results after
closing a reopened database. The roughly eight-millisecond scan saving does not
resolve the multi-second qualification gap.

The follow-up passes all 155 active Rust tests (one ignored), strict workspace
Clippy and formatting. Logs are retained under
`target/prepared-delta-cursor-review/`.
