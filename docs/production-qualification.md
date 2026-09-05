# Local production qualification

The implementation has passing local recovery and interoperability gates, but has not met the specification's performance qualification. The target is 50,000 mixed mutations/s with PostgreSQL commit-to-catalog p95 below one second and p99 below two seconds for ordinary transactions. Correct final rows alone do not qualify that target.

The [release37 candidate](qualification/release37.md) passes 30 frozen optimized service phases. A matched 100-table, eight-worker diagnostic admits every transaction and verifies all 1,070,000 rows. Two background builds reduce catalog p95/p99 from 1,581/1,900 to 708/823 ms, but maximum reader amplification rises from 1.392 to 1.684 and remains unqualified. The [release36](qualification/release36.md) uninstrumented control missed 390 source arrivals; that failure is not replaced by these sampled diagnostics. Earlier results below retain their original binary and measurement scope.

## Environment and measurement

The persistent fixture runs PostgreSQL 18.6, MinIO, and Apache Iceberg REST 1.10.1 with PostgreSQL JDBC 42.7.13. The catalog has its own PostgreSQL database and role on the source server. Named volumes preserve source, catalog and object data across restarts. Source fsync, synchronous commits, full-page writes, logical decoding and commit timestamps are enabled. Additional source conformance runs use PostgreSQL 14.24, 15.19, 16.15 and 17.11.

The host is an Apple M5 Pro with 18 logical CPUs and 48 GiB RAM, running macOS 26.4. The Docker VM has 8 GiB. PostgreSQL is limited to four CPUs and 2 GiB, MinIO to two CPUs and 2 GiB, and REST to two CPUs and 1.5 GiB with a 768 MiB JVM heap. The native daemon, workload generator and DuckDB reader share the host. Performance runs use Rust 1.97.1 release builds with thin LTO; correctness reports identify their binaries separately. Linux deployment has a separate container gate.

After readers05, all 64 retained replication slots were inactive and the fixture
had reached its configured capacity. With all fixture clients stopped, capacity
was raised to 128 and PostgreSQL and REST restarted. Volumes, retained slots and
resource/durability settings were preserved. Later reports use this capacity;
earlier reports retain their original environment. Before release34, 125 of 128 slots
were retained and inactive; capacity was raised to 256 with the same preservation
and durability settings. REST was restarted after PostgreSQL to clear stale JDBC
connections.

The [benchmark](../tests/production/README.md) schedules arrivals independently of completion, counts missed arrivals, and compares sorted final source and Iceberg rows. Publication latency uses source commit timestamps, durable journal events and actual successful catalog response events. Before/after clock calibration preserves raw values and offset uncertainty. Reader visibility is a polling upper bound, reported separately. Read timings include planning and execution and cannot qualify a planning-only target.

Qualification requires no missed arrivals, at least 95% of the requested mutation rate committed inside the measurement window, complete observations, exact final rows and both latency targets. Failed admissions and missing observations stay visible. SDK operation/byte diagnostics support comparisons but do not claim billed request counts; see [observability](observability.md).

These tests cross real logical replication, durable files, S3, REST compare-and-swap and independent readers. They do not qualify remote-region latency, TLS, managed storage or independent-host failure domains. Restarting PostgreSQL also interrupts the catalog database. HA, distributed compaction protocols, Z-order compaction and global autocompaction are excluded from early functionality by the user's scope clarification.

## Correctness and recovery

| Gate | Result and evidence |
| --- | --- |
| Service faults | All 12 phases passed in `target/production-faults-06/report.json`: actual successful-response loss, HTTP503, REST/MinIO/PostgreSQL restarts, SIGKILL with durable unpublished work, catalog commit racing with physical index loss, wide TOAST values and independent manifest/history checks. |
| Source schemas | `target/production-contracts-02/report.json` passed idle nullable ADD, response loss, mixed historical schemas, savepoint/stream aborts, restart, checkpoint restoration and primary-key drift. Separate `production-contracts-{column-incarnation,non-null-default,volatile-default}-01` reports passed rejection of unsafe DDL. |
| Interrupted bootstrap | `target/production-bootstrap-ddl-02/report.json` passed interruption during parallel COPY, concurrent CDC, nullable ADD between attempts, completed-stage reuse, index loss and exact final rows. |
| PostgreSQL versions | Contracts and interrupted-bootstrap/DDL passed on 14.24 in `target/production-source-versions-02/report.json` and 15.19/16.15/17.11 in `target/production-source-versions-01/report.json`. The first 14 probe's empty-DDL-transaction assertion failure is retained; the corrected assertion verifies the actual DDL XID and journal end-LSN. |
| Independent maintenance/readers | All 16 phases in `target/production-readers-05/report.json` passed on release16 using DuckDB 1.5.5, Spark 3.5.6 with Iceberg 1.10.1, and Trino 470. Native compaction continued before and after independently attributed Spark position-delete, data-file and manifest rewrites. Exact keyed/keyless duplicate rows and histories survived CDC and restart. |
| Hard reader debt | `target/production-pressure-01/report.json` passed with native compaction disabled: capture continued while table publication and ACK paused, then an independent compactor reconciled the tables and resumed the same daemon PID. Follow-up CRUD, key moves, retained history and a real commit conflict passed. |
| Index format upgrade | All seven phases in `target/production-upgrade-01/report.json` passed on real PostgreSQL/REST/MinIO/DuckDB using release05 → release06. The new binary rejected the old index format, rebuilt both tables into one new generation, preserved control/source identity and the permanent slot, consumed queued WAL, and passed CRUD/key moves and historical reads. A second release06 restart reused that generation. |
| Linux deployment | All six phases in `target/production-linux-demo-01/report.json` passed on a fresh stack: non-root UID 10001 and volume ownership, Trino exact CRUD/key moves, clean SIGTERM and status, restart with queued source changes, and idempotent initialization. |

The final fault run had **zero unplanned daemon exits**. Its independent test compactor required three supervisor restarts during HTTP503, REST and PostgreSQL faults; the deliberate daemon SIGKILL also required its expected restart. Do not infer uninterrupted availability for every process from the correctness result. Final DuckDB rows matched 12,080 orders and 17 accounts, 34 snapshots were audited, ten external compaction commits occurred, and the initial 4,096-row snapshot remained unchanged. Outage recovery checkpoints ranged from 0.30 to 6.91 seconds after restoration; they exclude the injected outage.

Evidence is versioned by binary, not just by a passing label. Fault06 used release SHA256 `907c9e69…de7`. Reader04 and pressure01 used debug SHA256 `66228c02…a51a7`, which includes later batched-ledger and pressure behavior. Reports without a recorded hash are older local evidence. Subsequent changes also have focused Rust integration checks and must be requalified before release.

The upgrade gate records both binary hashes (abbreviated here): release05 `d98c4236…92fe8` and release06 `b3e9e38e…8cfd8`. It requires ready status from the new process ID and freshly written metrics before checking progress, so retained observations cannot hide a failed restart. Final rows matched 4,069 orders and 18 accounts, with initial and pre-upgrade history unchanged. This is an upgrade correctness gate, not a rebuild-capacity benchmark.

The reader gates allow 60s/300s L0 age limits during slow Java maintenance;
readers04 disables native compaction and readers05 enables it. They prove
reconciliation under those limits. The pressure gate separately uses explicit
1s/2s limits; neither reader fixture qualifies the default reader-debt SLO.

## Measured performance baseline

The prior release baseline in `target/production-matrix-02` used one table, 100,000 initial rows, 100 mutations per transaction, four writers, 256-byte compressible payloads, ten seconds of warmup and 60 measured seconds. The mix was 70% updates, 20% inserts and 10% deletes, with native compaction active.

| Requested mutations/s | Source mutations/s | Catalog mutations/s including drain | Catalog p95 / p99 | Result |
| --- | ---: | ---: | --- | --- |
|1,000 |1,000 |993 |1,847 /2,068ms | Exact rows; latency failed |
|10,000 |9,998 |9,813 |3,068 /3,432ms | Exact rows; latency failed |
|50,000 |49,468 | Incomplete | Unqualified |319 missed arrivals;952 journal and513 catalog observations missing after 180s drain |

The incomplete 50k run is a failed workload, not proof of data loss. Its observed latency subset is not a full-cohort percentile. Earlier probes remain in `target/production-matrix-01`.

Measured fixes since that baseline include batched source-ledger durability and adaptive publication deadlines. In a real local control-store experiment with 2,048 two-table transactions, batching 256 transactions reduced median completion time from 1,839 ms to 32 ms. This is a component result and cannot substitute for an end-to-end latency result.

The follow-up `target/production-matrix-03` used the same workload dimensions and release SHA256 `d98c4236…92fe8`, including batching, adaptive deadlines and I/O diagnostics:

| Requested mutations/s | Completed transactions | Catalog p95 / p99 | Native compaction time | Result |
| --- | --- | --- | --- | --- |
| 1,000 | 600/600, exact rows | 1,607 / 1,688 ms | 29 rewrites, 45.12 s | p95 failed |
| 10,000 | 6,000/6,000, exact rows | 3,041 / 3,415 ms | 20 rewrites, 48.85 s | Both latency targets failed |

Both runs admitted the complete requested source load. The 10k cohort required 2.76 seconds of catalog drain. Sampled daemon RSS peaked at 307 MiB and 359 MiB respectively. Compaction counters include the ten-second warmup, sixty-second measured interval and drain. In the 1k run, storage returned 101.25 MB through full/range reads and accepted 78.53 MB of streaming writes; these are decimal SDK byte counts, not billed wire traffic. These measurements identified repeated full-base rewrites driven by delete-file fanout as an optimization target.

## Delete-consolidation follow-up

`target/production-matrix-04` used release06 SHA256 `b3e9e38e…8cfd8` with the same workload dimensions. This binary adds bounded delete-only consolidation and exact indexed deletion density. Both runs sustained their requested source arrival rate, observed every committed transaction and matched final source rows. Both failed the catalog latency targets.

| Requested mutations/s | Completed measured transactions | Catalog p95 / p99 | Native data rewrites | Native delete passes | Result |
| --- | --- | --- | --- | --- | --- |
| 1,000 | 600/600 | 2,184.91 / 2,976.83 ms | 6, totaling 12.37 s | 19, totaling 2.50 s | Both latency targets failed |
| 10,000 | 6,000/6,000 | 3,775.01 / 5,061.94 ms | 11, totaling 32.78 s | 16, totaling 2.62 s | Both latency targets failed |

For the 1k workload, storage reads returned **30.209 MB** and streaming writes accepted **21.643 MB**, compared with **101.246 MB** and **78.526 MB** in matrix03. These decimal SDK byte counts cover the daemon's warmup, measured work and drain, excluding initial COPY. They do not include hidden SDK traffic or represent provider billing. The 10k run returned 82.045 MB and accepted 58.606 MB.

Lower I/O did not qualify publication latency: measured p95 and p99 increased at both rates. The remaining data passes still expanded their inputs through shared-delete closure, pulling the base into minor L0 work. Residual data/delete rewriting was implemented after release06 to remove that requirement. Focused Rust integration tests verify an unchanged base file, exact survivor deletes and density, retained history, lost responses and index-loss recovery. **Matrix04 does not measure that later implementation**; the release08 follow-up below measures the subsequent combined changes.

## Residual-rewrite follow-up

[Matrix05](../target/production-matrix-05/matrix.json) used immutable release08 SHA256 `fc9357b1ee50b2aa51a85a253186db0ab4fefcd660e66c97248099cb0a5a4f37`. It includes residual data/delete rewriting, bounded Parquet reads and writer admission, and the corrected size-tier guard. It retained the one-table, 100,000-initial-row, 256-byte compressible-payload workload, 100 mutations per transaction, four writers, ten seconds of warmup and sixty measured seconds. These results measure the combined implementation, not the isolated contribution of each change.

| Requested mutations/s | Completed measured transactions | Source mutations/s in window | Catalog p95 / p99 | Reader scan ratio | Result |
| --- | --- | ---: | --- | ---: | --- |
| [1,000](../target/production-matrix-05/mixed-1k/report.json) | 600/600 | 1,000.00 | 638.31 / 1,050.78 ms | 0.900 | Passed this bounded workload |
| [10,000](../target/production-matrix-05/mixed-10k/report.json) | 6,000/6,000 | 10,001.67 | 1,320.39 / 1,727.90 ms | 0.962 | p95 failed; p99 passed |

Both runs had zero missed arrivals, no missing journal/catalog/reader observations, and exact final source and Iceberg rows. Catalog drain after the measurement window was 0.546 s and 0.527 s respectively. The reader ratio compares the median of three warm scans before and after independently compacting the **same final logical rows**, using exact retained metadata locations. Both reader checks passed. These short measurements combine planning and scan time; a ratio below one is not evidence that fragmentation improves readers, and does not qualify a planning-only or sustained reader target.

| Requested mutations/s | Native data rewrites | Native delete passes | Returned read bytes | Accepted streaming-write bytes | Sampled peak daemon RSS |
| --- | --- | --- | ---: | ---: | ---: |
| 1,000 | 9, totaling 3.67 s | 18, totaling 2.06 s | 15.573 MB | 12.064 MB | 281.9 MiB |
| 10,000 | 74, totaling 34.90 s | 11, totaling 1.48 s | 77.890 MB | 67.795 MB | 333.1 MiB |

These counters cover warmup, measured work and drain, excluding initial COPY. Reads combine full and range reads; byte totals use decimal MB and report FileIO activity, not billed traffic. One-second resource samples can miss peaks. Relative to matrix04, the 1k case reduced data-compaction time and I/O and passed latency. At 10k, the much larger number of smaller data rewrites still consumed 34.90 s and p95 remained above the target.

The next measured scheduling change is to publish already-due CDC before soft maintenance, while preserving recovery/reconciliation barriers and publication pauses at hard reader debt. It must also retain bounded opportunities for maintenance. Concurrent data building with late-update repair remains a design option on hold until measurements after that narrower change show a need. Matrix05 qualifies neither 50k throughput nor a 24-hour workload.

## Broader release08 workloads

[Matrix06](../target/production-matrix-06/matrix.json) used the same immutable release08 SHA256 `fc9357b1ee50b2aa51a85a253186db0ab4fefcd660e66c97248099cb0a5a4f37`. All three cases used ten seconds of warmup, sixty measured seconds, four source writers and 100 mutations per transaction. Every committed transaction was eventually observed at the journal, catalog and reader, and full source/target row checks passed. **All three performance qualifications failed.**

| Case | Committed / scheduled transactions | Source mutations/s in window | Catalog p95 / p99 | Catalog drain | Reader scan ratio | Failed checks |
| --- | --- | ---: | --- | --- | --- | --- |
| [Mixed 50k](../target/production-matrix-06/mixed-50k/report.json) | 30,000/30,000 | 49,998.33 | 161.763 / 167.283 s | 167.100 s | 1.427 | Both catalog latency targets; reader scan |
| [Wide 1k](../target/production-matrix-06/wide-1k/report.json) | 600/600 | 1,000.00 | 1.098 / 1.379 s | 0.139 s | 0.831 | Catalog p95; p99 and reader passed |
| [Ten tables, aggregate 10k](../target/production-matrix-06/ten-tables-10k/report.json) | 5,994/6,000 | 9,990.00 | 6.579 / 8.075 s | 0.269 s | 1.076–1.332 | Six missed arrivals; both catalog latency targets; three reader checks |

Mixed 50k used one table with 100,000 initial rows and 256-byte compressible payloads. It admitted every scheduled transaction, but catalog throughput including drain was only 13,210 mutations/s. Commit-to-journal p95 was **114.139 s** and p99 118.209 s, so substantial delay accumulated before journal durability as well as before publication. Data maintenance completed 109 rewrites totaling 150.35 s across warmup, measurement and drain. The final 450,000 rows matched exactly after the backlog drained; this does not qualify sustained 50k ingestion.

Wide 1k used one table with 10,000 initial rows and 16 KiB payloads formed from distinct MD5 blocks per row and generation. These are hex text with four bits of information per byte; PostgreSQL payload-generation CPU is included. All arrivals were admitted. Commit-to-journal p95 was 43.57 ms; the final 17,000 rows matched. Ten tables started with 100,000 rows each and used 256-byte compressible payloads. Its six missed arrivals remain failures even though all 5,994 committed transactions were observed and all ten final tables matched. Commit-to-journal p95 was 76.46 ms. Three tables exceeded the reader ratio limit of 1.25 (`events_1`, `events_2` and `events_6`); the worst ratio was 1.332.

| Case | Sampled peak daemon RSS | Sampled peak retained source WAL | Sampled peak journal bytes | Returned read bytes | Accepted streaming-write bytes |
| --- | ---: | ---: | ---: | ---: | ---: |
| Mixed 50k | 346.8 MiB | 2,150.724 MB | 307.050 MB | 256.607 MB | 258.241 MB |
| Wide 1k | 666.9 MiB | 880.791 MB | 71.108 MB | 1,733.460 MB | 1,804.952 MB |
| Ten tables, aggregate 10k | 563.7 MiB | 977.185 MB | 66.528 MB | 109.207 MB | 91.459 MB |

Resource values are one-second sampled maxima, not enforced limits or memory bounds. I/O counters cover warmup, measured work and drain, excluding initial COPY; decimal MB count FileIO returned/accepted bytes. The reader check uses the same-final-state comparison described above and combines planning with execution. These cases ran on the shared local host and service limits already documented; they do not establish independent-host capacity or 24-hour stability.

The long commit-to-journal interval motivated per-transaction capture timing to distinguish processing and durability costs. Release09's measurements below follow that diagnostic step and the separate due-CDC-before-soft-maintenance scheduling change. The completed release08 reports remain unchanged.

## Repeated reader comparison

[Reader-repeat01](../target/production-reader-repeat-01/report.json) re-read the exact original and compacted metadata locations recorded by matrix06, using DuckDB 1.5.5 and the same recorded extension versions. It ran separately from ingestion and started no fixture processes. The retained-report command records caller-managed quiescence; it does not stop shared services. This check did not rerun the source workload or change its previous qualification result.

The shared [comparison helper](../tests/production/reader_compare.py) explicitly warms each layout twice, then records twenty samples per layout in alternating A/B and B/A pairs. All warmup and measured samples are retained. The ratio remains original median divided by compacted median, and every table must remain at or below **1.25**. Each query checks equal aggregates; the retained reports also contain the matching full-row verification. New benchmark runs capture and verify the compacted metadata, stop their owned daemon and compactor, and use this same comparison protocol.

| Retained case | Original median | Compacted median | Repeated ratio | Result |
| --- | ---: | ---: | ---: | --- |
| Mixed 50k, `events_0` | 13.353 ms | 8.571 ms | 1.558 | Reader gate failed |
| Ten tables, `events_1` | 5.352 ms | 4.641 ms | 1.153 | Passed; matrix06 ratio was 1.332 |
| Ten tables, `events_2` | 5.843 ms | 4.937 ms | 1.184 | Passed; matrix06 ratio was 1.254 |
| Ten tables, `events_6` | 5.692 ms | 4.758 ms | 1.196 | Passed; matrix06 ratio was 1.282 |

All ten tables passed the repeated reader check, with ratios from 1.129 to 1.197. Their original three-sample failures were not reproduced under the balanced protocol. The 50k failure persisted: its 12 data files and three delete files scanned slower than the compacted one-data-file, zero-delete layout. This supports a remaining reader-overhead issue, but does not isolate deletion density, file opens or layout effects. The repeat report fails overall because the 50k ratio exceeds the unchanged limit. It does not erase matrix06's source-admission or catalog-latency failures.

## Release09 scheduling and capture measurements

[Matrix07](../target/production-matrix-07/matrix.json) used release09 SHA256 `6827d39f51abdfa978b8b5c3d4b7966620fdf06c8479b92164ecb88d682cf071`, recorded in the [binary manifest](../target/qualification-binaries/release09.json). This binary includes due-CDC-before-soft-maintenance ordering and capture phase timing. The same one-table, 100,000-initial-row mixed workload ran for ten warmup seconds and sixty measured seconds. Every scheduled transaction committed, every committed transaction was observed, and exact final rows matched.

| Requested mutations/s | Completed transactions | Catalog median | Catalog p95 / p99 | Result |
| --- | --- | ---: | --- | --- |
| [1,000](../target/production-matrix-07/mixed-1k/report.json) | 600/600 | 289.50 ms | 495.82 / 661.94 ms | Source load and catalog latency passed |
| [10,000](../target/production-matrix-07/mixed-10k/report.json) | 6,000/6,000 | 266.04 ms | 1,313.15 / 1,832.03 ms | Catalog p95 failed; p99 passed |

Both sustained their requested source arrival rate. Catalog drain was 0.014 s and 0.208 s. At 10k, the median fell from matrix05's 529.04 ms to 266.04 ms, but the p95 target remains unmet. These runs omitted the compacted reader comparison: `reader_scan_qualified` is null, so the 1k pass covers source load and catalog latency only. The separate repeat above measures retained release08 layouts, not release09 output.

[Capture-profile09](../target/production-capture-profile-09/report.json) used the same release09 binary for a separate fifteen-second, no-warmup 50k probe with 100 mutations per transaction. All 7,500 scheduled transactions committed, were observed and matched final rows. It failed latency: commit-to-journal p95 was 22.087 s, catalog p95/p99 were 22.671/24.007 s, and catalog drain was 23.771 s.

The 7,500 measured journal-sync phases totaled **30.623 s**, a mean of **4.083 ms per transaction**. Sync accounted for 90.5% of the five instrumented capture phases' aggregate duration. The other means were 0.157 ms for spool creation, 0.157 ms for disposal, 0.065 ms for schema validation and 0.050 ms for spool replay/append. These are phase wall-time counters, not total capture CPU time. At the offered 500 transactions/s, individual sync barriers alone consumed more time than the fifteen-second source window. This evidence motivates bounded journal group commit while preserving durable transaction visibility and acknowledgement boundaries. Its implementation and qualification follow these reports; no group-commit performance result is claimed here.

## Remaining qualification

The complete workload dimensions, sustained 50k target, 24-hour principal workloads, 100-table capacity, reader scan/planning amplification and provider request/byte costs remain open. Low-fan-out partitioning and v3 follow the original specification's qualified v2 baseline. No local bounded run is a production release certificate.

## Group commit and batched index reads

Immutable release10 (`66e10f7b0cb08f97254414aaef48a28d95a94dc450c631be3ff4deb41d8cac4a`)
adds bounded journal group commit. The otherwise matching fifteen-second
[capture probe](../target/production-capture-profile-10/report.json) committed
and observed all 7,500 transactions with exact final rows and no missed arrivals.
Journal p95 fell from 22,087.16 ms in profile09 to 24.56 ms; 1,077 barriers sealed
7,500 transactions and spent 5.724 s syncing, versus 30.623 s for 7,500 barriers.
Catalog p95 was still 7,427.47 ms with 8.307 s of drain, so the probe failed
performance qualification. Eight data rewrites occupied 8.781 s of the 23.268 s
first-journal-to-final-catalog interval. This attributes the remaining delay to
compaction occupying the table lane, without claiming every foreground cost is
profiled.

[Matrix08](../target/production-matrix-08/matrix.json) uses immutable release11
(`97c92a74ce2c16e4829b0389b9a3dae01791d054c1feee130cc7efebcdb52dc4`), adding
bounded batched index reads. Every committed transaction was observed and final
rows matched, but every case failed performance qualification. Reader ratios
use the strengthened two-warmup, twenty-alternating-pair protocol.

| Workload | Committed / scheduled TX | Missed arrivals | Journal p95 | Catalog p95 / p99 | Catalog drain | Reader ratio |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| Mixed 10k | 6,000 / 6,000 | 0 | 21.95 ms | 1,584.49 / 2,013.78 ms | 1.955 s | 1.174, pass |
| Mixed 50k | 25,767 / 30,000 | 4,233 | 76.96 ms | 63,655.77 / 66,470.81 ms | 66.486 s | 1.676, fail |
| Wide 1k | 600 / 600 | 0 | 41.10 ms | 1,043.43 / 1,443.27 ms | 1.400 s | 1.024, pass |
| Ten tables, aggregate 10k | 5,996 / 6,000 | 4 | 28.02 ms | 3,073.02 / 5,186.03 ms | 0.364 s | One failure at 1.420; remaining nine 1.153–1.191 |

The 50k source achieved 43,095 mutations/s within the measurement window;
committed-only latency percentiles exclude its 4,233 failed arrivals. These
results establish capture progress and preserved correctness, not sustained
50k capacity or a latency improvement attributable to the batched index reads.
The next implementation step is bounded local concurrent data compaction with
explicit late-delete reconciliation; the strict existing rewrite path remains
available for mandatory fallback. The required long-duration, reader planning,
provider-cost and broader workload gates remain open.

## Local concurrent compaction: release12

Immutable release12 (`0ebc7471c1edd8f792fcec846e37bcac17b6ba2158b8984161964059290cb6f5`)
adds the local build/catch-up protocol. The six-phase held-S3-read gate, twelve
service-failure phases, and eleven schema/lifecycle phases passed on this binary;
[validation](validation.md) records exact provenance and supervisor incidents.

[Matrix09](../target/production-matrix-09/matrix.json) preserves two failed overall
qualifications. Mixed 10k committed and observed all 6,000 transactions, with
exact final rows and no missed arrivals. Catalog p95/p99 **803.77/1,027.55 ms**
passed, compared with 1,584.49/2,013.78 ms in matrix08. Journal p95 was 19.24 ms and
catalog drain was 0.132 s. Its same-state reader comparison still failed: 21 data
files and 1 delete file versus 1 compacted data file, with medians 9.170 ms and
5.939 ms (1.544×). This is a catalog-latency pass, not an overall workload pass.

Mixed 50k committed 29,710 of 30,000 scheduled transactions, missing 290 arrivals.
Journal p95/p99 were 36.85/58.30 ms, but 13,794 committed transactions remained
without catalog observation after the 180-second drain deadline. Publication
was stuck retrying its unchanged delete-input budget. The incomplete 15,916-TX
catalog sample had p95/p99 28,271.15/28,606.14 ms; those percentiles exclude the
unobserved tail and cannot qualify latency. Final source/target rows did not
match because the journal backlog had not drained; this does not itself prove
permanent data loss. The stopped fixture and its durable backlog are retained
for recovery verification after the fix.

A [pinned dependency census](../target/matrix09-dependency-inventory-01/report.json)
and [exact position census](../target/matrix09-dependency-inventory-01/positions.json)
found 7 delete files containing 1,043,408 unique, effective positions and 2.92 MB.
The 1M-row limit was exceeded; file and byte limits were not. One 925,296-row
shared file covered 21 data targets, and delete metadata lacked path bounds.
Every data file had at most 100,000 physical rows. Merging or deduplicating those
files cannot reduce the position total. The next fix preserves input budgets,
adds exact path bounds and bounded dependency splitting, and admits useful data
subsets instead of rejecting an oversized greedy group. It must drain the saved
backlog before new performance claims.

## Saved backlog recovery: release13

[Recovery03](../target/production-budget-recovery-03/report.json) resumed matrix09's
unchanged source, permanent slot and state directory using immutable release13
(`e5824095f71aabb5e9492a992bcb4d9e0959965c179c7f661b3977de16b00a03`).
The release passed workspace tests, strict project Clippy, formatting and Rust
1.94 compatibility checks. Its planner admits feasible subsets under the existing
delete budgets, and its position-delete output carries exact path bounds with
at most eight residual ranges per data rewrite.

All **13,794 pending transactions** drained. The complete ordered source and
DuckDB streams matched at **447,100 rows**, and both materialization and source
acknowledgement reached the retained journal barrier `7/F2D683E8`. The recovery
phase took 44.566 seconds including row/history verification. The original
309,160-row snapshot retained its exact hash. Source and control identities were
unchanged; graceful stop and restart preserved the recovered result. The original
matrix report's bytes remain unchanged.

Fourteen data rewrites completed during recovery. Feasible subsets and bounded
residuals sufficed; this run did **not** invoke the new standalone dependency-repair
path. That path has separate actual-file recovery tests. Recovery01 and recovery02
stopped on missing fields in the temporary Python harness before daemon progress;
their failed reports remain retained. No fresh source load ran during recovery,
so this result closes the saved-backlog correctness failure without qualifying
50k throughput or latency.

The [matrix09 10k layout census](../target/reader-layout-census-09-10k/summary.json)
also narrows its reader failure. The original layout decoded 207,824 physical
rows for 170,000 live rows, versus 170,000 after full compaction. It had 42 versus
22 row groups and 4.36 versus 3.57 MB of projected compressed column data. Codecs
and encodings matched; bytes per physical row were nearly identical. This
establishes 22% extra physical data and almost twice the groups, but does not
apportion the measured 1.544× time among decoding, file opens, deletes and planning.

## Fresh mixed workloads: release14

[Matrix10](../target/production-matrix-10/matrix.json) uses immutable release14
(`b8f430122b6b02d3a2e6c4fedd107bf1a28557790be3793537f6237869d5f2bf`),
with bounded repair retries and a guard against delete consolidation undoing
admissible data work. The full Rust gates and seven-phase live concurrency test
passed before these serial workloads. Both workloads committed every scheduled
transaction, observed every commit and matched the complete final row stream.
Both still failed overall performance qualification.

| Workload | Committed / scheduled TX | Journal p95 | Catalog p95 / p99 | Catalog drain | Final rows | Reader ratio |
| --- | ---: | ---: | --- | ---: | ---: | ---: |
| Mixed 10k | 6,000 / 6,000 | 19.19 ms | 871.16 / 1,138.51 ms | 0.286 s | 170,000 | 1.501, fail |
| Mixed 50k | 30,000 / 30,000 | 22.90 ms | 42,764.14 / 43,602.95 ms | 43.416 s | 450,000 | 2.702, fail |

There were no missed arrivals or missing observations. Source commit-window
rates were 9,998.33 and 49,995 mutations/s. Including catalog drain, the 50k
measured cohort completed at **29,009 mutations/s**. This closes the prior
deadlock and incomplete-observation failure, but does not qualify sustained
50k publication. Reader medians were 8.571 versus 5.712 ms at 10k and 24.990
versus 9.248 ms at 50k, using the unchanged balanced twenty-pair protocol.

The [physical census](../target/reader-layout-census-10/report.json) found
199,088 versus 170,000 physical rows at 10k, and 721,675 versus 450,000 at 50k.
The latter retained **271,675 effective deleted rows**, 94 versus 58 row groups,
and 15.18 versus 9.52 MB of projected compressed data. All delete files now have
path bounds. Compression and encodings match the fully compacted layouts.
The [warmed profiles](../target/reader-layout-census-10/profiles-02/summary.json)
show similar 50k planning times and a larger difference inside `ICEBERG_SCAN`;
they do not separate deletion, decoding and I/O. Operator times accumulate
across eighteen threads and are not additive wall-time components.

Through the last ingest catalog event, 39 compactions occupied **49.418 seconds
of the 113.369-second interval** including warmup and drain. Concurrent finalizers
used 25.123 seconds of the table lane; fourteen strict rewrites used 24.295
seconds. Concurrent worker time overlaps CDC and is excluded from that sum.
Ingest catalog resolution totaled 3.999 seconds and index application 15.261
seconds across 139 publications. The unassigned remainder includes collapse,
artifact preparation, durable completion, maintenance setup and scheduling.
New phase timings will distinguish those costs before optimization. Snapshot
history also shows L0 choices increasing delete-file count under hard pressure;
the next policy review uses exact per-file deletion counts to assess that choice.

## Deletion-reclamation priority and phase attribution: release15

[Matrix11](../target/production-matrix-11/matrix.json) uses immutable release15
(`93fe88b5c60cc9369d5fd0b90f0a105bfc069840e7125620eeca7ac4ee8e5ae1`).
Its planner prioritizes larger absolute deletion reclamation when delete-file
pressure rises, preserving hard L0 urgency and all input budgets. Formatting,
workspace tests, strict project Clippy and Rust 1.94 checks passed. The workloads
retain the preceding runs' 60-second measurement, 10-second warmup, arrival
schedule, mutation mix, admission limits and reader comparison thresholds.

| Workload | Committed / scheduled TX | Journal p95 | Catalog p95 / p99 | Catalog drain | Final rows | Reader ratio |
| --- | ---: | ---: | --- | ---: | ---: | ---: |
| Mixed 10k | 6,000 / 6,000 | 19.43 ms | 990.05 / 1,583.13 ms | 0.421 s | 170,000 | 1.445, fail |
| Mixed 50k | 29,995 / 30,000 | 26.31 ms | 34,560.51 / 35,067.76 ms | 35.475 s | 449,950 | 1.970, fail |

Every committed transaction was observed and both complete final row streams
matched. The five missed 50k source arrivals remain failures. Including drain,
that measured cohort published 31,417 mutations/s. The 10k catalog latency gate
passed narrowly; both overall qualifications failed. The 50k final layout had
19 data files and 7 delete files, versus one data file and no deletes after
independent full compaction. Native data rewrites decreased from 39 to 27.
These short-run improvements do not qualify the requested capacity.

The [layout census](../target/reader-layout-census-11/report.json) found 623,310
physical rows, 173,360 effective deleted positions and 82 row groups at 50k.
Twenty-four of 27 rewrites reduced global delete-file count, compared with 21 of
39 previously. Every rewrite starting at the hard delete-file limit brought it
below that limit. Remaining deletion density is mostly in L0; its next build
was already selected at the final snapshot. No final dependency exceeded the
worker's file, row or byte limits. The final reader-baseline shutdown also logged
a worker runtime panic despite process exit 0; it is retained as a lifecycle
failure to fix separately from the measured ingestion interval.

[Phase attribution](../target/production-matrix-11/mixed-50k/phase-attribution.json)
accounts for 102.248 of the 105.475 seconds through the final ingest catalog event.
Serial compaction used 37.760 seconds: 33.109 seconds finalizing 26 concurrent
builds, plus 4.651 seconds for one strict rewrite. Background workers used 26.296
overlapping seconds, excluded from that serial total. Ingestion used 19.556
seconds collapsing mutations, 24.463 preparing output, 4.421 on the observed
catalog path, 15.795 applying/cleaning up after publication and 0.253 completing
the source ledger. Only 3.227 seconds remain unattributed.

Within compaction finalization, catch-up used 12.509 seconds and plan/seal work
9.925 seconds. Across ingest and rewrite index application, batch preparation
used 18.859 seconds and writes used 7.716 seconds; controlled lock waits totaled
about 2 ms. Those histogram phases are nested within the event totals and must
not be added again. Timings identify row processing and repeated staging as the
next optimization boundaries, rather than catalog latency or lock contention.
The parser separates exact event windows from sampled histogram deltas and
retains counter-export offsets and unsuccessful/malformed log lines.

## Row-processing and worker-lifecycle changes: release16

[Matrix12](../target/production-matrix-12/matrix.json) uses immutable release16
(`e949003057d57cd8ef588901bfa2b5c0d11b265df7e7cf6d1b9a1d4a847ceea6`).
It adds bounded batched collapse lookups, fewer row/key copies, one-pass catch-up
staging and independent worker I/O runtimes. Full Rust/MSRV checks and the
[seven-phase live concurrency gate](../target/production-concurrent-compaction-04/report.json)
passed. Both workloads observed every scheduled transaction, matched the complete
final row streams and logged no worker panics. Both still failed qualification.

| Workload | Committed / scheduled TX | Journal p95 | Catalog p95 / p99 | Catalog drain | Final rows | Reader ratio |
| --- | ---: | ---: | --- | ---: | ---: | ---: |
| Mixed 10k | 6,000 / 6,000 | 19.45 ms | 1,021.26 / 1,757.04 ms | 1.042 s | 170,000 | 1.267, fail |
| Mixed 50k | 30,000 / 30,000 | 23.38 ms | 27,163.03 / 27,641.28 ms | 27.184 s | 450,000 | 2.425, fail |

The 50k cohort completed at 34,410 mutations/s including drain. Its final layout
had 26 data files and 13 delete files, with 25 native compaction commits. Reader
slowdown worsened from release15 even as publication delay improved; neither
metric can substitute for the other. The 10k p95 and reader ratio narrowly missed
the unchanged limits and remain failures.

[Compared phase totals](../target/production-matrix-12/mixed-50k/phase-comparison.json)
show serial compaction falling from 37.760 to 31.869 seconds and aggregate
compaction preparation from 22.553 to 18.628 seconds. Ingest collapse fell from
19.556 to 15.765 seconds; ingest preparation remained nearly unchanged at 24.386
seconds. Its index application used 15.035 seconds, including 10.201 seconds of
batch preparation and 4.804 seconds of writes. Controlled lock waits remain
negligible. Catch-up now includes staging that previously appeared under
plan/seal; the subphase shift alone is not an optimization gain.

The [isolated StateStore comparison](../target/state-row-profile/report.json)
used three alternating samples per immutable executable and fresh controlled
RocksDB directories, without simultaneous builds or service workloads. Median
unique-key collapse improved 17%, repeated-key collapse 13%, and update apply 4%.
That sequential-key microbenchmark did not time delta staging and cannot qualify
service throughput.

The subsequent [delta-staging comparison](../target/state-row-profile-staging/tuning/runs-01/report.json)
used six alternating runs of immutable executables, fresh controlled RocksDB,
113-byte file paths and 524,288 updates in 512 bounded staging calls. Inputs were
constructed outside the timers. Reusing buffers, column-family handles and
uniqueness-marker storage reduced median staging from 1.300470 to 0.869887 seconds
(33.1%); the three-sample ranges did not overlap. Apply and collapse medians rose
about 2–3%, so this is evidence for the staging change only. Storage formats,
duplicate checks, batch limits and durability barriers are unchanged. Release17
includes that change and per-phase ingest preparation observations; its service
result is measured separately.

## Staging follow-up

[Matrix13](../target/production-matrix-13/matrix.json) uses immutable release17
(`e3ff13ef73cf87bddb45c8eaeb5c1614ef46200c87ed058f521fc3e0589045c2`), including
the staging allocation change, preparation phase observations and structural
schema comparison for external reconciliation after multiple nullable additions.
Full Rust tests, formatting, strict Clippy and the Rust 1.94 checks passed.
The explicit mixed-50k case retains the preceding workload and reader protocol.

All 30,000 scheduled transactions committed and were observed, with no missed
arrivals and exactly 450,000 final rows. Journal p95/p99 were 23.07/31.26 ms.
Catalog p95/p99 improved to 22,252.85/22,546.14 ms, with 21.701 seconds of drain
and 36,719 cohort mutations/s including drain. The source committed at
50,003 mutations/s within the window; three cohort transactions committed just
after it. No worker panics were found. The run still failed both latency targets
and its reader ratio worsened to 2.875 (28.264/9.831 ms). Its observed final
inventory held 31 data files and 16 delete files with 23 native compaction commits.

The [phase attribution](../target/production-matrix-13/mixed-50k/phase-attribution.json)
shows ingest preparation falling from 24.386 to 19.879 seconds. Of that total,
index/delete staging consumed 12.035 seconds, data writing 3.646, delete writing
0.985 and other preparation 3.213. These are whole-event accumulated durations,
not contiguous intervals. Serial maintenance fell from 31.869 to 29.471 seconds;
collapse rose from 15.765 to 16.732, catalog path from 5.175 to 6.429, and
post-catalog work from 15.090 to 15.699. Writer/staging overlap cannot save more
than the 3.646 seconds of data writing before contention and boundary costs,
so it remains deferred while duplicate-lookup work is examined.

The [ordered-marker comparison](../target/state-row-profile-staging/monotone/runs-01/report.json)
then measured a bounded reverse-seek proof ahead of duplicate lookup. Six
alternating runs reduced monotone staging median from 0.817822 to 0.306169 seconds
(62.6%), with non-overlapping ranges. Other phase medians changed by about 0–2%.
This fixture proves the ordered path's local benefit only. Actual-database tests
also cover fresh out-of-order ranges, duplicate keys/locations below earlier
maxima, reopening and interrupted staging. Those ranges retain full duplicate
checks. No cached maximum, caller ordering assumption or storage-format change
was introduced.

## Wider release18 workloads

[Matrix14](../target/production-matrix-14/matrix.json) used immutable release18
(`5edaf98ff094031f5ccab06e229b2bf83d8b009d06af8724bb54cb9407c2289b`), including
the marker-range proof. Full Rust tests, formatting, strict Clippy and Rust 1.94
checks passed. Every case committed and observed all scheduled transactions,
missed no arrivals and passed complete source/target row comparison. Every
overall qualification failed.

| Case | Committed TX | Catalog p95 / p99 | Drain | Final rows | Reader ratio |
| --- | ---: | --- | ---: | ---: | --- |
| Uniform mixed 50k | 30,000 | 17,946.92 / 18,599.48 ms | 18.563 s | 450,000 | 2.143, fail |
| Zipf updates 10k | 6,000 | 1,178.87 / 1,763.28 ms | 0.410 s | 170,000 | 1.268, fail |
| 1 KiB mixed 10k | 6,000 | 848.45 / 1,659.42 ms | 0.254 s | 170,000 | 1.361, fail |
| 10,000-row transactions at 10k | 60 | 611.96 / 671.60 ms | 0 s | 170,000 | 1.516, fail |
| 100 tables, aggregate mixed 10k | 6,000 | 38,319.20 / 38,535.71 ms | 38.831 s | 1,070,000 | Incomplete: reader I/O error |

The 1 KiB and large-transaction cases passed source-rate and catalog-latency
checks, but failed reader health. The latter has only sixty measured transactions;
its p99 is the maximum sample. These remain short workloads, not sustained
qualification. Zipf uses exponent 0.99 and finite rank weights within each
writer/table's current active-key vector, with distinct keys per transaction,
and a 90% update / 10% insert mix. The other cases use 70% updates, 20% inserts
and 10% deletes.
The hundred-table case starts with 10,000 rows per table; other cases start with
100,000 rows per table. All retain the existing seed, warmup,
measurement interval and reader protocol except their declared changed dimension.

At 50k, controlled duplicate lookup fell from 4.076 to 0.445 seconds (89.1%)
over the same 3,364 staging batches. Combined index/delete staging fell from
12.035 to 8.311 seconds and total ingest preparation from 19.879 to 17.368.
Serial maintenance still consumed 29.389 seconds, collapse 16.838 and
post-catalog work 16.222 through final catalog publication. The cohort completed
at 38,186 mutations/s including drain. Duplicate lookup is no longer the main
cost; application preparation, compaction catch-up and collapse remain material.
The first four cases recorded no worker panics after stopping their processes.

The hundred-table source/target comparison completed for every table before
the final reader experiment. That experiment then failed on a DuckDB HTTP HEAD
request for `events_62` metadata with no response headers or data. The original
report and captured service logs retain this failure. The later transport
diagnosis below establishes Docker forwarding address exhaustion. The missing
reader comparison cannot be counted as a pass or a measured slowdown. Any
subsequent reader repeat uses separately recorded pinned inputs and leaves the
original run unchanged.

## Hundred-table retained reader recovery

[Recovery01](../target/production-reader-recovery-01/recovery.json) fetched the
current catalog metadata without changing it, preserved raw REST and S3 metadata,
and pinned one data file with no delete files for each of the 100 tables. All
1,070,000 ordered rows matched the counts and SHA-256 hashes from the original
complete source/target verification. Before/after process censuses found no
fixture clients or sampled daemon PID, and retained daemon/compactor logs had
no worker panics. Original process exit codes were not retained and are not
reconstructed by this check.

The [recovered input report](../target/production-reader-recovery-01/input-report.json)
records these pins and the original report's hash. The separate
[reader repeat](../target/production-reader-recovery-01/reader-repeat.json)
completed all 100 tables without a reader I/O error, using the unchanged two
warmup scans and twenty alternating pairs per layout. Every aggregate matched.
Only 68 tables met the 1.25 ratio limit: the median table ratio was **1.0996**,
the minimum **0.9901**, and the maximum **1.6556** at `events_25`. The repeat
therefore failed reader qualification. It neither replaces the original reader
I/O failure nor changes that workload's failed catalog-latency result; the
original report remains byte-identical with SHA-256
`4342bdbb1fda659199cdeabe7fee5e2528f3bbc93ceca62b4300e6b713db36df`.

## Release20 and interrupted reader observation

[Release20](../target/qualification-binaries/release20.json) adds shorter ingest
paths, catch-up phase attribution, a shared maintenance I/O driver and one-second
metrics export cadence. Full Rust/MSRV gates and all seven live concurrency
phases passed. Its [matrix15 mixed-50k run](../target/production-matrix-15/mixed-50k/report.json)
committed 29,985 of 30,000 scheduled transactions and observed every commit.
All 449,850 final rows matched. Catalog p95/p99 were **13,659.38/14,458.08 ms**,
drain was **12.771 seconds**, and measured-cohort throughput including drain was
**41,205 mutations/s**. Reader slowdown was **2.205×**. The run failed arrival,
catalog-latency and reader gates; its fifteen missed arrivals also make the
admitted cohort different from release18's. No worker panics were logged.

The [hundred-table case](../target/production-matrix-15/hundred-tables-10k/report.json)
failed during live observation on another DuckDB HTTP HEAD with no response,
this time for `events_90`. It stopped after 5,875 committed transactions and 113
missed arrivals across warmup and measurement, leaving 1,102 durable pending
transactions. No complete workload latency or final reader qualification is
claimed for that interrupted run.

The separate [retained-backlog recovery](../target/production-reader-failure-recovery-01/report.json)
used the same immutable binary, state, configuration and permanent slot. It
cleared all 1,102 pending transactions, matched **1,058,750 rows across 100 tables**,
preserved the exact 1,047,830 rows in the pinned pre-recovery snapshots, and
advanced source ACK through `D/A4DF5458`. Recovery plus verification took
21.154 seconds; the subsequent clean restart and verification took 3.582 seconds.
There were no worker panics. The original failure report remains unchanged.
This proves recovery, not performance qualification. The separate transport
diagnosis follows.

## Reader transport diagnosis

The [retained diagnostic](../target/reader-transport-15/assessment.md) establishes
the cause of both hundred-table reader failures. Docker's VM port forwarder
logged `connect: cannot assign requested address` for all four retries at
10:32:46 UTC in matrix14 and 11:02:10 UTC in matrix15. A read-only diagnostic
against 1,647 historical metadata pins reproduced the same failure after 1,574
successful queries and 12,105 successful HTTP requests. Each of its four failed
GETs matches the VM forwarding error within one millisecond. Linux identifies
this `connect()` error as ephemeral-port exhaustion; the forwarder accepted the
host connection but could not connect to MinIO, producing an empty HTTP response.
[Linux connect(2)](https://man7.org/linux/man-pages/man2/connect.2.html),
[Docker Desktop networking](https://docs.docker.com/desktop/features/networking/).

The same stock DuckDB 1.5.5/httpfs 827222f reader, with only
`httpfs_connection_caching=true` changed, completed 5,000 queries and 16,153
HTTP requests against the same retained pins with no errors. It retained eleven
TCP connections, while the failed default run accumulated 10,534 host TIME_WAIT
sockets. The [pinned HTTPFS setting](https://github.com/duckdb/duckdb-httpfs/blob/827222f/src/httpfs_extension.cpp#L158-L169)
enables bounded connection reuse; it does not enable metadata caching or change
Iceberg reads. The diagnostic performed no ingestion, catalog writes, object
mutations or service restarts. Sampled process records contain no concurrent
compiler or daemon. These are transport diagnostics, not ingestion or reader
performance results.

New local qualification runs and standalone reader comparisons enable this stock
setting and retain actual transport settings in `reader_settings`. Retry,
backoff, timeout, keepalive and metadata-cache defaults remain unchanged; errors
still fail the run and the 1.25 scan-ratio limit is unchanged. Matrix14 and
matrix15 reports remain immutable failures. Fresh workload qualification under
this explicitly recorded transport configuration is still required.

## Counted batching and matched pending windows: release21

[Release21](../target/qualification-binaries/release21.json) passed formatting,
workspace tests, strict Clippy, Rust 1.94 checks and the nine-phase live
[journal/ledger upgrade](../target/production-journal-upgrade-01/report.json).
Its daemon SHA-256 is
`a014a32b00470c03e389638347876beb12cace2434416b8e1120dfb13bc06348`;
the exact source archive and build-tool versions are retained in
[source-release21.json](../target/qualification-binaries/source-release21.json).
The build adds durable per-table source mutation counts and removes transient
prepared-delta iterator copies. Its initial admission implementation also uses
10,000 mutations as a hard epoch limit, which the measurements below exposed as
a costly restriction.

All three [matrix16 cases](../target/production-matrix-16/matrix.json) used stock
DuckDB connection caching, unchanged retry/metadata-cache settings, sixty measured
seconds after ten seconds of warmup, four source writers, native concurrent
compaction and the same compacted-reader protocol. Every committed transaction
was observed, full source/target rows matched, and all owned workers stopped
without panics. The pending-1,024 reader-baseline compactor needed SIGKILL after
its fifteen-second SIGINT grace period; that run does not prove graceful
compactor shutdown. Both 100-table paired-reader comparisons completed without the
previous transport failure. Every case still failed performance qualification.

| Case | Committed / scheduled transactions | Catalog p95 / p99 | Final drain | Verified rows | Reader result |
| --- | ---: | ---: | ---: | ---: | --- |
| 100 tables, pending 256 | 6,000 / 6,000 | 8,967.59 / 9,073.62 ms | 8.313 s | 1,070,000 | 84/100 tables pass; maximum 1.537× |
| 100 tables, pending 1,024 | 5,901 / 6,000 | 9,222.68 / 13,203.27 ms | 6.956 s | 1,069,010 | 93/100 tables pass; maximum 1.394× |
| Mixed 50k, pending 256 | 28,432 / 30,000 | 61,619.67 / 61,827.68 ms | 61.420 s | 434,320 | 2.193×, fail |

The pending-window variant missed 99 arrivals and did not improve tail latency.
It does not justify changing the default. Different admitted cohorts prevent
attributing every metric difference solely to the window size. The 50k run
missed 1,568 arrivals and delivered only 23,416 measured-cohort mutations/s when
drain is included. Its exact row digest is
`0ced59403863ab6c3a859dc9aef0b8d394850d4287ed86d5ed62e8c0feb0b28b`.

[Phase attribution](../target/production-matrix-16/mixed-50k/phase-attribution.json)
shows 335 ingestion epochs, with 334 containing exactly 100 source transactions.
Release20 had 142 epochs, mostly 256 transactions. The pre-final table-lane
interval increased from 82.771 to 131.420 seconds: serial compaction from 27.319
to 52.388 seconds, catalog paths from 4.882 to 12.170, and ingestion preparation
from 15.981 to 22.229. Post-catalog application fell slightly, from 14.546 to
13.970 seconds. These builds also differ in source cohort and reader transport,
so the comparison is diagnostic, not an isolated A/B estimate.

The specification defines row thresholds as publication triggers and classifies
individual transactions up to 10,000 mutations as ordinary; it does not require
that value as a hard epoch ceiling. The corrective path retains accurate row
readiness, whole transactions, the existing 32 MiB admission bound and bounded
pending descriptors, while admitting already-queued ordinary transactions
without waiting to fill a batch. Unknown legacy and individually large
transactions remain isolated. This requires new validation and measurement;
release21's failed reports remain unchanged.


## Corrected admission and residual merge: release22

[Release22](../target/qualification-binaries/release22.json), daemon SHA-256
`217946a5d004b7d00e87216cae62071c04159a86f1565d33325c0183bf75ddf6`, passed
formatting, workspace all-target tests, strict Clippy and Rust 1.94 checks.
The [seven-phase concurrent-compaction gate](../target/production-concurrent-compaction-06/report.json)
passed with exact current and historical rows, deadline cancellation and actual
join, held-I/O shutdown and restart, and no worker panics. The
[fixture SIGINT regression](../target/production-compactor-shutdown-01/report.json)
reproduced release21's lost signal; release22 completed the held pass and exited
0 in 18.4 ms after release. Source and toolchain provenance are retained.

The correction retains the 10,000-mutation readiness trigger and admits queued
ordinary transactions within the unchanged 32 MiB and descriptor limits. It also
merges original residual deletes, late deletes and replacement masks directly
into the bounded writer. Sequence rules, H validation and Prepared/CAS recovery
are unchanged. Stream traversal now appears in residual-write timing, so a drop
in residual-spool time alone is not a speedup measurement.

[Matrix17 mixed-50k](../target/production-matrix-17/mixed-50k/report.json) used the
same declared connection-cache protocol, sixty measured seconds after ten
seconds of warmup, four source writers, pending window 256 and native concurrent
compaction. All **30,000/30,000 transactions** committed and were observed, with
zero missed arrivals. Exact source/target verification matched **450,000 rows**,
digest `6029dd6903ebdb7ad7cf65b6b400da97bdbead0020a7d9fe02c9a5f41ecfb47a`.

| Measurement | Result | Contract |
| --- | ---: | --- |
| Commit-to-journal p95 / p99 | 26.530 / 36.007 ms | Diagnostic |
| Commit-to-catalog p95 / p99 | 10,609.114 / 11,507.611 ms | Fail: 1,000 / 2,000 ms |
| Final catalog drain | 10.044 s | Backlog remains after source window |
| Measured cohort rate including drain | 42,829.998 mutations/s | Below 50,000 |
| Paired reader slowdown | 2.169× | Fail: at most 1.25× |

Original versus compacted pins contained 20/1 data files, 8/0 delete files and
15,317,551/11,081,575 data bytes. Paired median scans were 18.709/8.625 ms.
Both the daemon and baseline compactor exited 0, with no worker panics. The row-cap
regression recovered, but latency and reader performance still fail.

[Phase attribution](../target/production-matrix-17/mixed-50k/phase-attribution.json)
records an 80.044-second pre-final interval: 26.150 seconds of serial compaction,
15.866 of collapse, 16.046 of preparation, 13.939 of post-catalog work, 4.786 of
ingestion catalog work and 0.216 of completion. The seven catch-up phases total
13.840 seconds across all logged catch-ups. These are path measurements, not
isolated CPU costs or a controlled estimate of either individual source change.

Separate [reader profiles](../target/reader-path-profile-22-02/assessment.md)
retain the same verified pins: median planning was 1.990/1.883 ms, and accumulated
parallel scan-operator time was 38.545/23.496 ms. These instrumented measurements
show a larger scan difference than planning difference; they neither replace the
quiet paired ratio nor isolate network, decoding and CPU. A separate
[instrumented daemon sample](../target/production-cpu-profile-22/profile.json)
also preserved exact rows and all 30,000 observations. Its profiler overhead
precludes treating it as a quiet qualification run.

The [exact pinned-layout census](../target/reader-layout-census-22/assessment.md)
found 625,705 physical data rows and 175,705 unique, applicable deleted positions,
leaving the verified 450,000 live rows. There were no duplicate, dangling or
sequence-inapplicable delete records. Data row groups were 84 versus 58, with
similar median occupancy (7,916.5 versus 7,875 rows). Ten L0 files already exceeded
the existing 30% density threshold and held 102,252 deleted positions. Their
dependencies fit current budgets. This supports improving maintenance execution;
it does not justify wider budgets, smaller row groups or another deduplication
pass. This census reads exact delete rows and data footers, not timed data scans.


## Source-ledger lock isolation: release23

The [wall-stack assessment](../target/production-cpu-profile-22/callpath-assessment.md)
found the main coordinator waiting for the row/spool mutex during source-ledger
registration in 1,381/4,308 observations (32.06%). This is sampled wall time,
not CPU utilization or a speedup forecast. The change bypasses that mutex only
for complete SOURCE-ledger batches on a controlled store. The existing control
mutex orders index/control revisions and poisons failures before unlocking;
row/table/operation read-modify-write paths keep their lock. Checkpoints hold both
locks and recheck failure. Actual-database liveness, failed control-write and
reopen tests passed, as did all Rust/MSRV gates.

The immutable [release23](../target/qualification-binaries/source-release23.json)
daemon is `da62b6bb44c9fe242742c3b647141f887a9cba58a31c4afaddef7a6d684c35b8`.
[Matrix18](../target/production-matrix-18/matrix.json) used an immutable
[copy of the previous harness](../target/qualification-harnesses/release23/manifest.json),
with the same sixty-second window, ten-second warmup, stock connection cache,
default row/descriptor budgets and native concurrent compaction. This separates
the measurement from subsequent bounded-recorder development.

| Case | Committed / scheduled | Catalog p95 / p99 | Final drain | Exact rows | Reader result |
| --- | ---: | ---: | ---: | ---: | --- |
| Mixed 50k | 30,000 / 30,000 | 9,169.489 / 9,570.989 ms | 8.225 s | 450,000 | 2.064×, fail |
| 100 tables, 10k aggregate | 6,000 / 6,000 | 7,297.410 / 7,580.441 ms | 6.290 s | 1,070,000 | 86/100 pass; maximum 1.499× |

Both cases observed every committed transaction, missed no scheduled arrivals,
and stopped the daemon and reader-baseline compactor with exit 0. There were no
worker panics. Measured-cohort rates including drain were 43,972.218 and
9,051.160 mutations/s. Every performance case remains a failure.

The 50k digest remains
`6029dd6903ebdb7ad7cf65b6b400da97bdbead0020a7d9fe02c9a5f41ecfb47a`.
Its original pin contains 17 data files, seven delete files and 15,221,949 data
bytes; the compacted pin contains one data file, no deletes and 11,096,051 bytes.
The [phase attribution](../target/production-matrix-18/mixed-50k/phase-attribution.json)
shows an interval of 78.225 seconds through the last catalog completion. Collapse
remained 15.861 seconds, preparation 15.739 and post-catalog work 14.236. Serial
compaction totaled 23.826 seconds, with no strict fallback in this run. The
comparison therefore establishes improved measured tails alongside the proven
liveness fix, not a 32% reduction in row-processing cost.


## Processing-batch sensitivity on release23

A single [same-binary variant](../target/production-batch-sensitivity-23/report.json)
changed `batch_rows` from 1,024 to 4,096, retaining the immutable previous harness,
8 MiB decoded writer batches, 32 MiB row groups, source chunk bytes, pending
window, compaction policy, workload and SLOs. It was motivated by repeated
unsynced WAL-write paths in the retained sample, not an assumption of a per-batch
fsync. No default was changed.

All 30,000 scheduled transactions committed and were observed with zero misses,
and exact final rows remained 450,000 with the same digest. Catalog p95/p99 were
**4,864.388/5,498.310 ms**, final drain **4.045 seconds**, and measured-cohort rate
including drain **46,841.989 mutations/s**. The paired reader ratio was **2.338×**.
Latency, throughput and reader amplification still fail the contract. Both owned
processes exited 0; no worker panics occurred.

Compared with matrix18's 1,024-row control, catalog p95 fell from 9,169.489 ms
and drain from 8.225 seconds. Sampled daemon RSS increased from 462,929,920 to
532,660,224 bytes; one-second sampling is not a true peak bound.
[Phase attribution](../target/production-batch-sensitivity-23/phase-attribution.json)
records 12.317 seconds of collapse (previously 15.861), 12.251 of post-catalog
work (14.236) and 15.612 of preparation (15.739). Total catch-up work fell from
13.810 to 10.821 seconds. The variant also had 163 rather than 153 epochs and
one strict maintenance fallback, so this is an end-to-end sensitivity, not an
isolated per-batch speedup. Further work must improve steady-state maintenance
and complete the acceptance gates, rather than relabel this as 50k qualification.

## Bounded benchmark evidence and matched control

The benchmark now stores active keys and transaction/probe evidence on disk,
uses indexed SQL attribution and exact external-sort percentiles, and verifies
full rows with a server-side PostgreSQL cursor and DuckDB CSV spill. Actual-file
tests preserve uniform/hot/Zipf rank selection, delete swap order, missing-probe
holes, unsigned LSN ordering, failure tails and recorder shutdown. Offline replay
of 7,000 retained transactions reproduced all 17 existing workload fields.
[Validation evidence](../target/benchmark-storage-validation-01/report.json)
records the exact sources and inputs.

The [live smoke](../target/production-bounded-protocol-smoke-01/report.json)
passed with 600 total transactions across three tables, native compaction and
36,000 exact final rows. A fresh [50k control](../target/production-bounded-protocol-matched-01/report.json)
then used immutable release23, default 1,024-row batches and the same 60-second
measurement plus ten-second warmup. Protocol `disk-evidence-paged-probes-v2`
changes the observation query and explicitly sets DuckDB's memory limit to 1 GiB;
the earlier harness and its results remain immutable.

All 30,000 measured transactions committed and were observed, with zero missed
arrivals or missing stages. Final 450,000 rows retained the same digest. Catalog
p95/p99 were **14,122.378/14,871.889 ms**, drain **14.534 seconds**, and reader
slowdown **1.856×**. Both owned processes exited 0 without worker panics. These
are failures under the unchanged performance thresholds.

The old control's 75 probe scans totaled 2.929 seconds; the bounded control's 77
scans totaled 7.555 seconds. The new query returned exactly 35,000 probes across
the run, compared with repeated historical probes in the old observer. Those
elapsed totals indicate observer overhead worth isolating; they do not establish
how much of the catalog-latency difference it caused. A bounded fixed-pin query
comparison found no benefit from an explicit marker range: current `IN` and
`IN` plus `BETWEEN` had medians of 50.598 and 51.049 ms over four alternating
pairs, returning the same 1,024 IDs from four data files. DuckDB already supplied
dynamic range filters. [Profiles](../target/probe-sql-profile-v2-02/operator-summary.json)
retain this rejected explanation; the bounded query and protocol were unchanged.

## Rejected compact physical-index encoding

A private file-token dictionary was implemented and tested against actual
RocksDB databases, preserving public paths and durable plans. Snapshot retention,
atomic last-reference retirement, obsolete-row handling and corruption checks
passed. The [24-sample comparison](../target/compact-index-profile/runs-01/report.json)
used 32,768 rows, three update rounds, identical 111-byte paths, eight or 256
files, 1,024/4,096-row batches and three alternating repetitions per variant.
It measured actual apply batches and separate read, collapse and storage costs.

With eight files, median apply improved 7.85%/11.15%. With 256 files, apply gains
were only 1.39%/1.42%, while batched reads regressed 50.74%/13.39%, collapse
15.05%/5.06%, and scalar reads about 4×. Encoded apply bytes fell 60–69.5%, but
closed SST bytes grew 1.7–2.1% in the many-file case. Fewer encoded write bytes alone
did not establish a useful overall improvement.

The candidate was rejected and its production changes restored. Index format 3
and existing recovery compatibility remain in use. Source, raw results and the
rejected candidate are retained under `target/compact-index-profile`; no further
dictionary tuning or service qualification is claimed.

## Measured maintenance work on release25

The [unchanged bounded-protocol control](../target/production-bounded-protocol-matched-25/report.json)
uses [release25](../target/qualification-binaries/source-release25.json), the same
frozen harness, default 1,024-row batches and compaction budgets. Its only runtime
changes since release24 add bounded observations at existing durability and
maintenance boundaries. All 30,000 measured transactions committed and were
observed, with no missed arrivals or missing stages. Exact final rows remained
450,000 with the same digest. Both owned processes exited 0 without worker panics.

Catalog p95/p99 were **12,052.976/12,987.638 ms**, drain **11.162 seconds**, and
paired reader slowdown **1.856×**. These remain performance failures. This run
provides work attribution; its timing difference from the earlier control is not
attributed to the observation changes.

Before final ingestion publication, 24 completed compactions admitted 4,683,094
physical data rows, built 2,235,749 rows and applied 1,900,019 mappings. The
difference is exactly 335,730 catch-up masks. Each concurrent pass satisfies
built mappings = accepted + masked, and accepted = applied, with no obsolete
rows or partial apply cursor. These are physical-work counts, not unique source
rows. [Phase attribution](../target/production-bounded-protocol-matched-25/phase-attribution.json)
records **27.018 seconds** of serial maintenance, including **11.059 seconds**
of inner catch-up and **8.998 seconds** of commit marking and index application.
Residual writing consumed 1.669 seconds inside catch-up. These nested intervals
must not be added again to total maintenance time.

Across all 25 completed passes, maintenance produced 64.744 MB of committed
Parquet. Unique journal observations counted 1,037,680,016 serialized payload
bytes across 35,000 transactions including warmup. Their 6.239% ratio excludes
metadata, failed/unpublished outputs and storage-provider traffic, and includes
rewrites of initial-copy rows. One later build produced 252,223 rows without a
completion event and remains separate from committed output. This establishes
counting boundaries for further optimization, not production cost qualification.

## Release26 matched control

The [same bounded workload](../target/production-bounded-protocol-matched-26/report.json)
uses immutable release26, the frozen v2 harness and unchanged batching and
maintenance budgets. All 30,000 measured transactions committed and were observed;
all 35,000 transactions including warmup joined exactly across journal, catalog
and reader evidence. Full verification matched the same 450,000 rows before and
after independent compaction. Both owned clients exited 0 without worker panics.

Catalog p95/p99 were **9,378.663/9,713.754 ms**, drain **8.516 seconds**, and reader
slowdown **2.314×**. Source commits admitted 49,915 mutations/s within the measurement
window, with 63 transactions committing after its edge and zero missed arrivals.
These results remain performance failures; the difference from release25 is not
an isolated estimate of the small allocation change's benefit.

New registration histograms record 4,993 successful pages covering all 35,000
transactions in 1.310 seconds of dispatcher time. This interval includes reading
and validating descriptors and registering their durable ledger entries. It is
separate from capture-thread journal sync, and the two totals must not be added
as serial publication work. The [retained audit](../target/bounded-protocol-validation-26/summary.json)
verifies all joins, metric counts, exact rows, hashes and stopped clients.

The [maintenance comparison](../target/compaction-work-amplification-26/assessment.md)
finds slightly more physical work before final ingestion: 4,747,664 admitted
rows, 2,323,160 built rows and 467,438 late masks. All completed passes preserve
the mapping and artifact count invariants. Serial maintenance falls from 27.018
to 23.797 seconds, largely because this run has no strict worker occupancy on
the table lane. Per-mapping costs improve modestly, but that does not attribute
the whole latency difference to allocation savings. Controlled rewrite apply
preparation still accounts for 6.026 seconds across the full run; its enclosing
metric does not distinguish decoding, reads and write-batch construction.

A subsequent [spool-serialization comparison](../target/state-encoding-profile-26/assessment.md)
reused one encoding buffer per state batch. All twelve actual-database samples
and the failure/reopen checks passed, but collapse results were inconsistent:
1.79% slower with eight files and 3.20% faster with 256, with overlapping ranges.
Unchanged read paths moved by similar amounts. The candidate was rejected and
all state-store sources restored exactly to release26. Its code and results are
retained; no live-workload gain is claimed.

The subsequent [reverse-owner comparison](../target/reverse-owner-profile-26/assessment.md)
supports one narrower index change. A bounded seek can prove that every target
in a single-file batch lies beyond its existing indexed positions; otherwise
the original ownership reads remain. One-file apply median fell from 233.061
to 202.893 ms across three update rounds, with non-overlapping sample ranges.
The 256-file fallback showed no clear change. All twelve samples preserved
exact rows and closed SST sizes; the expanded recovery test covers partial apply,
reopen, unordered positions, gaps and collisions. This is an apply result,
not an end-to-end latency claim.

## Release27 bounded control and crash recovery

Immutable [release27](../target/qualification-binaries/release27.json) passed all
120 active Rust tests, strict Clippy, the Rust 1.94 check and the corrected
[12-phase service-fault suite](../target/production-faults-14/report.json).
The [first fault attempt](../target/production-faults-13/failure-summary.json)
retains a legitimate catalog conflict that prevented the fixture's forced-success
crash boundary; the subsequent fixture correction did not change the binaries.

The [unchanged bounded-v2 control](../target/production-bounded-protocol-matched-27/report.json)
verified all **34,881 committed transactions** and **448,810 exact final rows**,
before and after independent baseline compaction. It missed **119 of 30,000
scheduled measured arrivals**; 29,881 measured transactions committed, and every
committed transaction joined journal, catalog and reader evidence. No committed
rows were missing, but the requested arrival workload was not sustained.

Catalog p95/p99 were **6,300.651/6,721.036 ms**, final drain **5.635 seconds**, and
full-scan slowdown **2.092×**. All three remain performance failures. The admitted
cohort differs from earlier controls, so this run does not establish a matched
full-rate improvement. Both owned clients exited 0; no worker panics occurred.

The [retained audit](../target/bounded-protocol-validation-27/readout.md) verifies
all outcome records and identifies two missed-arrival clusters across all four
source writers. The retained client intervals include recorder and source work;
they cannot isolate PostgreSQL execution or recorder backpressure as the cause.

All 152 preparation events satisfy the new overlap accounting: **10.520 seconds**
of paired wall time equals 4.076 seconds of data writing plus 8.477 seconds of
staging, minus **2.138 seconds of observed overlap**, plus 0.104 seconds of pair
overhead. Total preparation was 15.250 seconds. The overlap is a measured
intersection, not a counterfactual speedup to subtract again. Before final catalog
publication, serial maintenance occupied 23.103 seconds, collapse 16.328 seconds,
catalog work 6.062 seconds and post-catalog work 11.805 seconds. All 23 completed
native rewrites preserve their mapping, delete and output-count invariants; one
additional uncompleted build remains separate from committed output.

## Isolated PostgreSQL WAL overhead

The [replica-identity comparison](../target/production-wal-overhead-01/assessment.md)
passed 12 cases and 24 measured phases on an isolated PostgreSQL 18.6 cluster:
48,000 mutations with exact rows, identical ordered changes between FULL and
DEFAULT, and three alternating pairs for each payload. No catalog or daemon
shared this source. Explicit checkpoints, full-page writes and checksums stayed
enabled; WAL compression and autovacuum were disabled for this finite check.

Median FULL/DEFAULT generated-WAL ratios were **1.415× after a checkpoint** and
**1.643× without another checkpoint** for 256-byte payloads. For 16 KiB payloads
left unchanged by updates, the ratios were **3.318×** and **3.675×**. The values
come from `pg_stat_wal.wal_bytes`; raw FPI counts, LSN distances, settled statistics
and checkpoint evidence remain separate. They quantify this declared workload,
not production traffic or the historical streaming benchmarks. The created
container and volume were confirmed removed.

## Pinned reader planning and selective queries

The [release27 reader diagnostic](../target/production-reader-diagnostics-27-01/assessment.md)
uses the exact verified 448,810-row pre/post-compaction pins after owned writers
stopped. All 176 query attempts returned matching aggregates, including 2,057
rows selected by `id BETWEEN 1 AND 10000`; all 80 detailed profiles are retained.
DuckDB 1.5.5 used a 1 GiB managed-memory limit and 4 GiB spill limit.

The ratio of median plan-creation time was **1.169× for full scans** and **1.213×
for selective queries**, passing the scoped 2× gate. This sums DuckDB's three
sequential parent phases: planner, all optimizers and physical planner. Nested
binding/optimizer timings and parallel operator times are reported separately.
It excludes parsing/client overhead and does not measure other readers or their
first observation of a commit.

Separate unprofiled wall-time ratios were **2.462× full** and **2.614× selective**.
No selective-query threshold was invented. This additional instrumented protocol
does not replace control27's original 2.092× scan result, missed arrivals or
failed performance qualification. The bounded-v2 comparison functions and its
frozen source remain unchanged.

## Journal replay and whole-collapse diagnostics

The [journal comparison](../target/journal-replay-profile/assessment.md) supports
retaining one reusable reader per epoch. Three alternating pairs replayed the
same checksummed files with unchanged payload checks. At 29,600 bytes per
transaction, median warm replay fell from 0.777 to 0.220 seconds per 65,536
transactions, saving **8.506 microseconds per transaction**. Opening/recovery,
mutation decoding, index work and publication were outside that timer. All input
file hashes stayed unchanged; actual-file tests cover errors, partial iterators,
transaction-ID reuse, independent readers and a growing tail that later rolls.

A separate [whole-collapse diagnostic](../target/whole-collapse-profile-27/assessment.md)
uses the unchanged release27 reader and real journal, controlled index and
Parquet publication. Three fixed runs each processed 256 transactions containing
25,600 mutations, with repeated keys, updates, deletes and primary-key moves.
All passed exact original-location checks and standard-reader verification of
6,656 current rows and 4,096 historical rows.

Median whole collapse was **58.065 ms** (41.078–60.222 ms). StateStore calls took
48.497 ms and about 82% of the interval; replay, decoding and source processing
together took 8.917 ms and about 17%. These diagnostics have different workloads
and timing scopes, so their percentages and savings cannot be combined or scaled
onto the production collapse total. They support the small replay change and
direct batch-phase measurement in the next service run. Speculative epoch
reservation remains deferred.

The [compaction mapping comparison](../target/catchup-overlap-profile-27/assessment.md)
supports overlapping one accepted batch's staging with the next batch's checks
at the same fenced head. Median whole catch-up fell from **249.173 to 226.342 ms**,
or **9.163%**, with non-overlapping observed ranges across three alternating
pairs. Each run used the same deterministic 100,000-row workload and budgets
with fresh physical artifacts. All preserved 53,333 accepted/applied mappings,
46,667 masks, 80,000 exact current rows and 100,000 historical rows. Publication
and index application were outside the timer. The change also admits mappings
by logical bytes as well as rows, and a held-write failure test verifies the
stage joins before recovery can discard partial work. This result does not
qualify service latency or reader performance.


## Release28 correctness and bounded control

Immutable [release28](../target/qualification-binaries/source-release28.json)
passed all 124 active Rust tests, strict Clippy and Rust 1.94 checks. The
[campaign](../target/release28-campaign/execution/summary.json) then passed all
seven concurrent-compaction phases and twelve service-fault phases before running
one unchanged bounded-v2 control. Every owned process group was empty before the
next gate. The fault suite retained three independent-compactor supervisor
restarts as availability incidents.

The [control](../target/production-bounded-protocol-matched-28/report.json)
committed and observed all **35,000 transactions**, including all 30,000 measured
arrivals. No arrivals were missed. Exact **450,000 final rows** and their checksum
matched before and after independent baseline compaction.

| Measurement | Release28 | Gate |
| --- | ---: | ---: |
| Commit-to-journal p95 / p99 | 25.568 / 34.845 ms | Separate durability measurement |
| Commit-to-catalog p95 / p99 | 5,068.537 / 6,298.527 ms | Below 1,000 / 2,000 ms |
| Catalog drain | 3.532 s | No growing steady-state backlog |
| Commit-to-reader p95 / p99 | 5,790.587 / 6,981.825 ms | Separate polling observation |
| Scan slowdown | 1.844× | At most 1.25× |

**Performance remains unqualified.** Release27 missed 119 arrivals, so its smaller
committed cohort is not a matched full-rate baseline. The combined changes and
shared host also prevent attributing the difference to one optimization.

The [retained audit](../target/bounded-protocol-validation-28/readout.md) checks
SQLite integrity, every source/journal/catalog/reader join and recalculated
percentiles. All local source frontiers ended at 90,270,299,304 with zero pending
transactions. The frozen harness did not retain a final PostgreSQL
`confirmed_flush_lsn`, so those local frontiers do not prove the server's final
received ACK. Daemon and baseline compactor exited zero, with no worker panics.

Direct [StateStore timing](../target/collapse-production-audit-28/assessment.md)
records 3,439 write attempts with matching counts across all six phases. Lifetime
collapse batch time totals 14.550 seconds: primary-key lookup 6.164 seconds,
staged writes 4.298 seconds, prior-spool reads 2.381 seconds, binding 1.227 seconds,
folding 0.464 seconds and setup 0.016 seconds. These intervals include warmup and
are nested within whole collapse; they exclude early rejected batches, destructors
and metric emission. They differ from the isolated all-miss fixture and do not
measure CPU time.


The [compaction audit](../target/compaction-work-amplification-28/assessment.md)
verifies all 25 completed operations against their logged mappings, masks, deletes
and applies; no incomplete builds remain in this run. Before final ingest,
maintenance occupied 19.391 seconds and the measured table-lane union occupied
67.317 of 73.532 seconds. Mapping-stage overlap accounts for a measured 4.143-second
intersection. Nested staging became more expensive per accepted row, while whole
mapping elapsed fell; the intersection cannot also be counted as CPU savings.
Seven finalizers still exceeded one second. The next investigation therefore
focuses on the measured database work, preserving the existing compaction protocol.


## Rejected independent-flush experiment

The [fixed StateStore comparison](../target/atomic-flush-profile-28/assessment.md)
tested removing `atomic_flush=true` while keeping WAL enabled and every durability
barrier unchanged. Six actual-database samples, in alternating order, passed exact
rows, reverse mappings, counts and checkpoint/reopen checks. A separate abrupt-exit
test forced a selective column-family flush before recovering a partial apply.

Median lifecycle time rose from **1.476 to 1.618 seconds**; observed ranges overlapped
(1.291–1.587 versus 1.371–2.000 seconds). Pre-checkpoint retained index WAL grew
from about **20.96 MB to 455.64 MB**. Fewer flushes did not establish a performance
benefit. The candidate was rejected, and the
[restoration proof](../target/atomic-flush-profile-28/restoration-proof.json)
verifies that production Rust source again matches release28. The candidate and
its targeted test remain in the diagnostic archive. No release29 service campaign
ran, and no new throughput or latency result is claimed.

## Current-epoch buffer comparison

Release29 shares mutation folding and physical binding between an explicitly
bounded memory map and the existing disk path. Capacity overflow drops the map
and replays the same complete journal prefix. The configured default adds up to
32 MiB of accounted retention per active worker, or 128 MiB at four workers;
initial copies, oversized transactions and legacy descriptors use disk directly.
This leaves transaction scheduling, persisted formats and durability barriers
unchanged.

The [finite actual-file comparison](../target/collapse-buffer-profile-29/runs-01/report.json)
uses one frozen candidate executable with disk-only and 32 MiB settings. Three
alternating pairs per workload each replay 256 transactions of 100 mutations:
60 ordinary updates, 10 key moves, 20 inserts and 10 deletes, with three-column
rows and 256-byte text. Timed work includes collapse, publication, index apply
and spool cleanup. Source generation, journal sync/reopen and exact current,
historical, forward/reverse/count/fingerprint checks are outside measurement.

| Workload | Disk lifecycle median (range), ms | Buffer lifecycle median (range), ms | Change |
| --- | --- | --- | --- |
| Repeated keys | 111.698 (89.212–113.025) | 78.325 (63.973–82.353) | −29.88% |
| Distinct keys | 173.730 (168.128–174.016) | 155.127 (144.363–155.390) | −10.71% |

All twelve paired cases pass exact rows and history. The accepted buffer peaks
are 9,558,100 and 26,966,016 bytes; these exclude separately bounded decoding,
lookup and writer workspace. Whole-process peak RSS includes untimed setup and
verification and does not isolate buffer memory. Two additional 1 MiB-cap cases
force complete disk replay and pass the same checks; one sample each does not
estimate fallback performance. Raw outliers and failed build attempts are retained.

The improvement supports a service candidate. It is not a same-memory-budget
comparison, CPU saving, sustained throughput result or catalog/reader SLO proof.
The full source gates pass 128 active tests, formatting, strict Clippy, release
build and Rust 1.94. Release28 remains the latest completed service control until
the candidate's separate service reports finish.

## Release29 service control

Release29's service gates use the frozen daemon
`5a498e7adeaffd07f4cd2ed0c7090693815bee1c80ef36ac87c42063522fc306`.
The interrupted bootstrap/DDL fixture passes all five phases, and faults16 passes
all twelve phases with empty owned process groups. Both successful-commit response
loss boundaries, physical index loss, external rewrite reconciliation, wide rows,
unchanged TOAST and independent-reader checks pass. The independent compactor
needs three supervisor restarts during catalog and PostgreSQL outages; the report
does not classify uninterrupted compactor availability as passing.

The unchanged bounded-v2 control is correctness-complete and performance-unqualified.
All 34,250 committed transactions are journaled, published and observed, and the
source and both final reader states agree on 442,500 rows. The measured interval
commits 29,250 of 30,000 scheduled transactions. Its 750 misses form nine retained
clusters; neither client-response intervals nor the saved timestamps identify a
cause. Catalog p95/p99 are 6.750/7.222 seconds, final drain is 5.906 seconds and
reader slowdown is 2.271×. These fail the original gates. Final server-side
PostgreSQL acknowledgement remains unmeasured.

All 156 runtime epochs use the memory path. Accepted retention is 1.945–24.339 MiB,
with a 23.876 MiB median and no fallback. Measured-window collapse occupancy falls
from 12.537 seconds in release28 to 6.369 seconds in release29. This confirms the
buffer is active and reducing its intended work, but does not explain or erase
the end-to-end failure.

[Phase attribution](../target/phase-attribution-29/README.md) shows the prefinal
table lane grew from 67.317 to 68.893 seconds despite less committed source work.
Release29 spends 22.285 seconds in maintenance, including 11.857 seconds of catch-up
and 7.774 seconds of mark/apply. Preparation plus commit after ingestion catalog
success also grows. Mapping staging is 96.36% overlapped, so another stage-overlap
change lacks support. The shared controlled `StateStore::apply_batch` preparation
path is the next focused target: its process-lifetime nested time increases for
both ingestion and rewrites while lock wait remains below 1 ms. This is a measured
location for further work, not proof that CPU, RocksDB I/O or allocation is the
cause. One postfinal build remains incomplete and excluded from committed totals.

## Borrowed index-row comparison

The first release30 candidate changes only the current-row decode inside
`StateStore::apply_batch`. A private serde view borrows the file path and
partition from the pinned RocksDB value, compares every `RowLocation` field and
then leaves reverse ownership, file counts, cursor updates and the durable write
batch on the existing owned delta. Codec tests require byte-for-byte
reserialization and matching trailing, truncated and invalid UTF-8 behavior.

The [finite comparison](../target/apply-borrow-profile-29/assessment.md) runs
twelve serial actual-RocksDB samples across one-file and 256-file layouts. Each
sample applies 98,304 accepted replacements and 24,576 obsolete rewrite rows in
1,024-row batches, then verifies every forward row, reverse owner and file count
through checkpoint restore and controlled reopen. Median total apply time changes
from 204.644 to 200.717 ms (-1.92%) with one file and 381.530 to 367.624 ms
(-3.64%) with 256 files. Obsolete apply improves 4.35% and 4.79%. SST byte counts
and control revisions are identical within each layout.

The candidate passes formatting, 129 active workspace tests across 40 binaries,
strict Clippy, the release build and Rust 1.94. The change is retained as a small
allocation reduction. It does not explain release29's full service regression or
qualify source admission, catalog latency, reader slowdown or final PostgreSQL
acknowledgement.

## Release31 maintenance baseline and release32 phase split

The controlled matrix holds the manifest-rewrite threshold at 1,000,000 and runs
four policies: A disables native maintenance, B allows delete consolidation only, C adds bounded
L0 data rewriting, and D uses the current L0/L1/L2 policy. Each case seeds one
100,000-row table, warms for ten seconds and schedules 100-row mixed transactions
for sixty seconds at 1,000 or 10,000 mutations/s. Release31 is frozen at commit
`2d8905383dee369704a56faf5acc1123d6bce59c` with daemon SHA-256
`2c871a39cc1df7b30e7bee366c2de7cbdcea56e6d7089f520f5719b88359d7a9` and external
compactor SHA-256 `bba2b925b13e373bb524e5208b904aaee37d78926c6258bce40ad5de878bce24`.
Release32 is frozen at commit `5efbf5b89005d1b59d6bc88bfbcce2259774e891`
with daemon SHA-256 `9519e6c30e8964c8147b726381ca665ca128949aae004eb31b9cb76880f4882b`
and external compactor SHA-256 `b92922700e757652c2433244d0c5adcf540138d23276e103a6586fe1822fbe8d`.
The [comparison](../target/production-maintenance-matrix-release32-01/comparison.md)
records both build manifests, source trees and unchanged harness hashes.

The same release32 daemon passes all seven phases in the
[concurrent-compaction service gate](../target/production-concurrent-compaction-release32-01/report.json).
It publishes real updates, deletes, key moves and delete/reinsert while compaction
I/O is held, pauses ACK at hard debt, requests cancellation at 30.003 seconds and
retains BUILD until join/retire at 37.042 seconds, then removes scratch. SIGTERM
exits 0 in 1.032 seconds with I/O held, and restart recovery preserves current and
retained historical rows without a worker panic. This exercises the split's
successful service path plus build cancellation and restart boundaries; it is not
a sustained workload qualification.

It also passes all twelve correctness and recovery phases in the
[release32 fault gate](../target/production-faults-release32-01/report.json): an
actual committed-response loss, catalog HTTP outage, durable REST/MinIO/PostgreSQL
restarts, SIGKILL with journaled unacknowledged work, successful catalog commit
before physical index loss, wide/unchanged TOAST and independent reader/manifest
audit. Final rows match 12,080 orders and 17 accounts, the initial 4,096-row
history is unchanged, 14 external compaction commits are audited and no worker
panic occurs. The external compactor needs supervisor restarts during the catalog
outage, REST restart and PostgreSQL restart; two later shutdowns exit 0 without
forced kill. Correctness/recovery passes, while uninterrupted compactor
availability remains unqualified.

| Case | Catalog mutations/s, release31 → 32 | Catalog p95/p99 ms, release31 → 32 | Data/delete files, release31 → 32 | Reader ratio, release31 → 32 |
| --- | ---: | ---: | ---: | ---: |
| 1k A, disabled | 998.0 → 997.0 | 515.7/535.3 → 504.5/522.5 | 153/152 → 153/152 | 1.281 → 1.284 |
| 1k B, delete-only | 997.9 → 994.8 | 515.5/531.8 → 500.5/525.4 | 152/7 → 153/3 | 1.395 → 1.317 |
| 1k C, L0-only | 997.5 → 993.3 | 520.7/551.1 → 517.0/528.7 | 20/7 → 21/7 | 1.525 → 1.538 |
| 1k D, current | 993.0 → 994.5 | 512.5/581.8 → 519.7/527.5 | 21/1 → 4/2 | 1.315 → 1.438 |
| 10k A, disabled | 9,920.5 → 9,977.2 | 498.4/528.7 → 494.2/516.9 | 173/172 → 171/170 | 1.733 → 1.801 |
| 10k B, delete-only | 9,952.0 → 9,976.3 | 737.1/950.5 → 668.3/869.4 | 186/2 → 174/2 | 2.002 → 2.058 |
| 10k C, L0-only | 9,953.9 → 9,947.9 | 682.0/844.4 → 687.6/870.4 | 29/1 → 27/1 | 3.097 → 3.278 |
| 10k D, current | 9,965.8 → 9,947.6 | 620.4/859.3 → 604.0/799.1 | 20/4 → 8/3 | 1.445 → 1.397 |

All 600 measured 1k arrivals and all 6,000 measured 10k arrivals commit in every
case, with no missed or unrecorded arrivals; the source schedule sustains exactly
1,000 and 10,000 mutations/s. All sixteen release31/release32 cases meet the
catalog p95 below one second and p99 below two seconds. They also pass exact source
acknowledgement, complete source/target rows, durable reconciliation after the
independent same-final-state compactor, daemon and compactor exit 0 without forced
kill, and the zero-worker-panic check. Every reader ratio exceeds 1.25, so all
case runs return nonzero and neither release is performance-qualified.

Release32 starts preparation in the background after a build, captures one
immutable RocksDB view, and permits foreground activation only when the catalog head
and complete table state still equal the prepared state. The primary maintenance
scope ends at the report's resource snapshot after warmup, workload and drain.

| Active case | Data rewrites through drain/raw log, release31 → 32 | Preparation started/ready/deadline/invalidated | Activation committed/no-change/invalidated | Foreground actor ms, release31 → release32 handoff + finalization | Publication stall ms, release31 proxy → release32 |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1k C | 19/19 → 20/20 | 20/20/0/0 | 20/0/0 | 2,339.368 → 78.216 + 1,443.959 = 1,522.175 | 2,339.368 → 2,118.186 |
| 1k D | 9/9 → 9/10 | 9/9/0/0 | 9/0/0 | 1,786.497 → 31.782 + 1,559.704 = 1,591.487 | 1,786.497 → 2,033.862 |
| 10k C | 20/21 → 20/21 | 20/20/0/0 | 20/0/0 | 6,970.142 → 65.288 + 4,427.113 = 4,492.401 | 6,970.142 → 7,846.988 |
| 10k D | 29/30 → 31/31 | 31/31/0/0 | 31/0/0 | 10,433.008 → 104.673 + 9,026.505 = 9,131.178 | 10,433.008 → 12,596.863 |

A and B perform no data preparations. No release32 case has an in-flight
preparation at observation or uses strict fallback. Release31 has no direct
publication-stall metric; its old finalization actor hold is the closest proxy
because catch-up and activation both occupied that lane. The split lowers summed
actor occupancy in every active case, but direct publication stall improves only
in 1k-C relative to that proxy. It therefore establishes the concurrency boundary
and preserves finite correctness/latency, but does not establish lower total stall.

Raw daemon logs can include maintenance that starts after the resource snapshot
during the final external reader baseline. The raw counts in the table retain that
work only for disclosure; catch-up, state-apply, catalog and actor phase totals use
the through-drain count and exclude later rewrites. Release32's reader ratios are
1.284/1.317/1.538/1.438 at 1k and 1.801/2.058/3.278/1.397 at 10k. Reader
amplification remains the failed gate.

## Release32 RocksDB staging profile

The [actual-RocksDB profile](../target/sst-staging-profile-32/assessment.md) runs
three rotated fresh-database samples for each of 100,000 and 1,000,000 mappings.
It compares current staging with two prevalidated compaction lower bounds: ordinal
WAL records and an LZ4 external SST installed into the isolated delta column
family. Every sample applies a 90% replacement and 10% delete rewrite over 256
old and 256 new files. Reopen validation checks every staged delta, all primary
keys, reverse owners, per-file counts, the operation cursor and final table state.

At one million mappings, median external-SST construction takes 258.6 ms and its
foreground install plus seal takes 1.543 ms, versus 277.925 ms for ordinal WAL
staging. The unchanged live-index apply then takes 3,791.127 ms, or 93.579% of
the external-SST total. Its 4,051.255 ms median total is 4.948% slower than the
ordinal-WAL lower bound. At 100,000 mappings, the SST total is 3.996% faster, but
that smaller result does not offset the one-million-row regression or justify a
new storage path.

The profile also closes and reopens after ingesting 4,096 SST ordinals while the
operation remains Building, discards that unsealed operation and reuses its ID
through a normal exact apply. This passes the intended reopen cleanup boundary;
it is not exhaustive machine-crash injection. The result rejects external SST as
the next general optimization and focuses the next comparison on larger bounded
atomic apply batches, with independent row and serialized-byte ceilings.

## State-store apply-batch profiles

The [first comparison](../target/apply-batch-row-profile-33/assessment.md) runs 72
rotated actual-RocksDB samples. Its compaction cases cover 100,000 and 1,000,000
rows with eight and 256 files; its 90/10 update-delete cases use 100,000 and
1,000,000 rows with 256 files. It verifies exact forward and reverse mappings,
file counts, operation cursors, table state and clean reopen.
None of the coupled 4,096, 8,192 or 16,384-row settings improves every case;
all increase call tails and peak RSS. The result retains 1,024.

The [decoupled comparison](../target/apply-batch-decoupled-profile-33/assessment.md)
uses source commit `8be299ac751510fc5d61ed6aab4e8c1cac49939e` and frozen binary
`6e96635fc3087f9ea83231a3883680c367793d900a049cdadd57488e28590abc`.
It fixes PK and reverse lookup chunks at 1,024 rows, then runs 27 compaction and
nine update-delete samples with 1,024, 4,096 and 8,192-row atomic commits. The
runner retains commands, logs, timing/RSS output, exact serialized `WriteBatch`
bytes and operation counts, reverse-owner reads and phase distributions. All 36
samples pass exact apply and reopen validation.

Reverse-owner reads are identical across commit sizes in every workload, so the
experiment removes the identified lookup-width confound. Paired-median improvements
for 4,096/8,192 are 3.42%/4.46% at 100,000 rows and 256 files,
12.54%/12.38% at one million rows and 256 files, and 0.49%/0.07% for the
one-million-row update-delete case. The eight-file results are -0.19%/+3.71%.
The largest observed call is 54.20 ms, the largest narrow-row batch is 3.60 MiB,
and the maximum median RSS increase is 12.47 MiB. Both candidates fail the
declared 10% and 20% compaction gains. No production ceiling changes, serialized
8 MiB cap or release33 service claim follow from this result.

The initial decoupled run stopped after five samples because a live directory
census raced a RocksDB background file removal. It published no report. The final
profile measures directory and WAL footprints only after a clean database close;
the passing reports use a rebuilt binary and fresh artifact directories. This is
a harness correction, not an apply-path failure.
