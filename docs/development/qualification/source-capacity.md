# Source-generator capacity prerequisite

The current four-writer source-only control admits all 35,000 scheduled
transactions at 50,000 offered mutations/s. Earlier four/eight/four-writer runs
miss arrivals, demonstrating variable capacity rather than a fixed ceiling.
These runs use the exact `Benchmark.writer`, seed and payload
expression, with the existing bounded SQLite recorder and disk key pools. They
create no ingestion daemon, replication slot, catalog, object-store or reader
connection. They therefore measure source generation and PostgreSQL response
capacity, not ingestion throughput or catalog latency.

The original serial order was four writers, eight writers, then four writers again. Each
run seeds 100,000 rows and offers 100-row mixed transactions with 256-byte
payloads: 70 updates, 20 inserts and 10 deletes. Ten seconds of warmup precede
60 measured seconds; the random seed is 20260905 and the arrival tolerance is
250 ms. Each run plans 5,000 warmup and 30,000 measured transactions.

| Run | Measured committed / missed | Warmup committed / missed | Source response-window mutations/s | Final rows / immutable markers |
| --- | ---: | ---: | ---: | ---: |
| Four writers, initial | 28,286 / 1,714 | 3,211 / 1,789 | 47,140 | 414,970 / 31,497 |
| Eight writers | 22,654 / 7,346 | 4,572 / 428 | 37,810 | 372,260 / 27,226 |
| Four writers, repeated | 27,345 / 2,655 | 2,537 / 2,463 | 45,528 | 398,820 / 29,882 |

All three runs have zero unrecorded arrivals and zero incomplete commits. Every
committed immutable marker exists with its original version. All surviving rows
pass deterministic payload, ID origin, committed-version and writer-ownership
checks, and final row counts match the committed insertion/deletion counts.
Each writer also asserts the actual affected rows inside every source transaction.
Source correctness passes; source capacity fails, so all three exit with status 1.

PostgreSQL 18.6 reports the same system identifier and postmaster incarnation
before and after every run. `fsync`, `full_page_writes` and `synchronous_commit`
remain enabled, with logical WAL. No source restart or durability relaxation is
hidden in the comparison.

## Recorded phase evidence

These are client elapsed measurements across the measured cohort. PostgreSQL
transaction time includes the client's SQL/response interval; it is not server
CPU time. The key phases include disk-backed rank selection and key-pool updates.

| Run | PostgreSQL transaction p50 / p95 ms | Key preparation p95 ms | Prior key-pool update p95 ms | Recorder peak items / capacity |
| --- | ---: | ---: | ---: | ---: |
| Four writers, initial | 3.91 / 10.92 | 1.57 | 1.13 | 15 / 256 |
| Eight writers | 12.25 / 34.17 | 8.26 | 5.68 | 37 / 256 |
| Four writers, repeated | 4.52 / 10.62 | 1.80 | 1.47 | 85 / 256 |

Recorder enqueue p95 remains below 9 microseconds in all three runs. The evidence
queue is not observed saturating. Eight writers increase the measured SQL and
key-processing intervals and miss more measured arrivals than either four-writer
run; these samples do not justify increasing daemon benchmark concurrency.

The initial four-writer run admits every scheduled transaction in nine successive
five-second buckets from scheduled second 20 through 65. Its misses cluster earlier
and in the final five seconds, alongside longer SQL/response intervals. This
shows variable source performance rather than establishing a fixed four-writer
capacity ceiling. No retained measurement isolates the cause as PostgreSQL CPU,
client scheduling, filesystem work or another shared resource.

These failed source-only cohorts cannot be used to attribute a 50k end-to-end
failure entirely to the daemon. They also do not excuse the independently
measured publication backlog and reader overhead. A 50k ingestion claim requires
a source configuration that admits the complete offered workload, followed by
the same full ingestion and reader gates.

## Current serial control and rejected pipeline

The release38 follow-up keeps the four-writer workload and durability settings
above. The serial control admits all 35,000 transactions and verifies 450,000
final rows and every immutable marker. A candidate pipelines SQL within each
source transaction while retaining affected-row checks before commit. It passes
source correctness but misses one measured arrival, so it is reverted.

| Generator | Measured committed / missed | Warmup committed / missed | Response-window mutations/s | SQL/response p95 | Final rows / markers |
| --- | ---: | ---: | ---: | ---: | ---: |
| Current serial | 30,000 / 0 | 5,000 / 0 | 49,998.33 | 5.613 ms | 450,000 / 35,000 |
| Pipelined candidate | 29,999 / 1 | 5,000 / 0 | 50,001.67 | 4.638 ms | 449,990 / 34,999 |

Both have zero unrecorded arrivals and incomplete commits. Response-window rates
count responses within the wall window and do not replace the complete scheduled
cohort check. PostgreSQL's system identifier and postmaster incarnation match
before and after both runs; fsync, full-page writes, synchronous commits and
logical WAL remain enabled. The lower candidate SQL/response p95 is a client
elapsed measurement, not server CPU time or a complete-arrival capacity win.

Eight actual-writer cases separately verify serial/pipelined equivalence for
mixed, append and update-heavy profiles, plus pipeline rollback on affected-row
mismatch and SQL error. Exact rows and key-pool hashes match each successful
pair; failures preserve the original rows and keys without a committed record.
This is writer-semantics coverage, not ingestion qualification.

The serial control uses `benchmark.py` SHA-256
`1d0d0f69b132599c1981a110efac0cc9552b6a42aed587219b544a3d5b2ab184`;
the candidate uses `20f20d7388b7898e96d817bfe9ef91ffdd694b63b645aa3ced528000ffc0da54`.
The source-capacity wrapper and recorder hashes match the later original runs.
`target/release38-source-pipeline/comparison.json` records the decision and
identities. The rejected source and patch remain ignored local evidence as
`candidate-benchmark.py` and `candidate.patch`; the patch SHA-256 is
`a4ce4f68060b222798e020e3efa1dca8197b948b38cf40f9bbbed56c1a31301f`.

| Report | SHA-256 |
| --- | --- |
| Current serial control (`target/release38-source-pipeline/serial/report.json`, local artifact) | `fce60d1663b6b94c9b3a95cfc292d94a7a2d46dc7f644ddb0d71edc3111d8f93` |
| Rejected pipeline (`target/release38-source-pipeline/pipelined/report.json`, local artifact) | `729c9c125bf9e5c511d957c68fb41583fcc1705fc862b51061d760c077ca4f58` |
| Writer semantics (`target/source-pipeline-validation/results/report.json`, local artifact) | `b15475aa63c82c72a624662e5306e94f2bced9354acc7cccdb0c27054796f1e1` |

This complete serial source-only gate permits a new full 50k ingestion test;
it does not establish capacity while the daemon, compaction and readers compete
for resources. The earlier failures remain valid for their recorded runs.

## Retained evidence

Raw directories are ignored local artifacts. Report hashes pin the results; each
report also includes SQLite, transaction-export and key-file hashes, source-code
identity, machine information and the PostgreSQL observations.

| Report | SHA-256 |
| --- | --- |
| Initial four writers (`target/release35-source-capacity/writers4/report.json`, local artifact) | `1b4efb8f898a6fbcec04475924a95f6af9dd0a5b202a10c25c6b98b92f957ce7` |
| Eight writers (`target/release35-source-capacity/writers8/report.json`, local artifact) | `8bd3ad34a2aa7342521da16fe4976e10fb73c6e09cc82705d98eb8f7ac896c88` |
| Repeated four writers (`target/release35-source-capacity/writers4-paired/report.json`, local artifact) | `702e394908df84cdf765ec973f72d3e9d3070e46093dc287fe80300acd9f93fe` |

All three use producer `benchmark.py` SHA-256
`28d9e804b8969bab98e3547d4e215e9eace36256cc4b83c91e207d783706b41c`
and recorder/key implementation `benchmark_store.py` SHA-256
`2f9b2084a58b3428cb48ebc98930f09cb235c43bbbdd8c9605ad1a5db71d22ad`.
The initial wrapper hash is
`0e73a7b8ffaf39bd7f64f31c466ed35ddf711d779b4e88f3ab8c96f661c02405`.
An export-failure cleanup correction then produces wrapper hash
`87248d8260068c9dd5a64a770372ed5d1beb859d2c8c827171d8b8c6c2caaa3a`,
used unchanged for the eight-writer and repeated four-writer runs. The correction
does not change source scheduling, SQL, random selection or verification.

## Rejected private-key file resize change

The release39 pending-window diagnostic exposes significant source-worker
transaction and postcommit key-file time; recorder backpressure does not account
for its missed arrivals. SQL round-trip wall time includes client scheduling and
network time, so these records do not establish a PostgreSQL fsync or GIL cause.

The causal comparison follows each writer's committed transaction immediately
preceding its first missed arrival. Across 1,071 baseline and 3,009 candidate
transitions, mean source-worker cycles are 18.420 and 23.548 ms against an 8 ms
schedule. PostgreSQL round-trip wall accounts for 81.1% and 74.9%; client key-file
maintenance accounts for 12.7% and 20.8%. Recorder queue peaks are 17/256 and
28/256 items. The reproducible offline analysis and all transitions are retained
in `target/release39-pending320/source-diagnosis/`; report SHA-256 is
`3a677aab11dc5f21d7ee93c87d31edb6bcf20299783a4c1765086c8b1912428b`.

One isolated candidate removes the per-transaction file shrink in
`DenseKeys.remove_selected`, retaining logical read bounds and trimming the tail
at close. All 16 Python checks pass, including exact key order and final file
bytes. Four private concurrent writers then execute 2,000 mixed key operations
each, comparing eight alternating baseline/candidate samples with identical
final files. Candidate median wall time regresses 14.849%, from 2.139 to
2.457 seconds. The change and its additional test assertions are reverted.
This file-only comparison contains no PostgreSQL or ingestion workload.

The retained report is `target/dense-key-resize-review/report.json`, SHA-256
`f999601345391d6e96bd391278e86345180c791f4cd4246914cc04cd8a639462`.
That directory retains the executed helper, both source variants, rejected
patch and a replay helper pointing at the saved candidate. This result does
not justify another SQL or durability change, and does not alter the failed
source-arrival or daemon-latency qualification results.
