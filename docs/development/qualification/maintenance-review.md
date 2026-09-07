# Maintenance review

The soft data-build starvation fix remains justified. A fresh old-binary
negative control reproduces starvation, and both current-build regression cases
pass. Three varied workloads preserve exact rows and acknowledgements, but none
passes every performance gate. No scheduler thresholds, batch sizes, staging
formats or runtime APIs were changed in this review.

## Artifact and method

The optimized binary was built from clean commit
`2a087c634d394eb2f6b3ef253a19935100e9c4e0` with the existing release profile.
Its manifest is
`target/qualification-binaries/maintenance-review-current/build-manifest.json`.
Daemon SHA-256 is
`9e4fc366e130fcf83258cb98d5d3235a7ee1b8bf8886592eb7ace8eb90e3c1f2`;
reference compactor SHA-256 is
`db6843a727350664a67b68cf74354b819a4f4575a646c4e7f70f24e3f019e9f6`.

Runs used the existing PostgreSQL, MinIO and REST fixture on a shared 48 GiB ARM
host. Service CPU limits remained 4/2/2, respectively, with PostgreSQL durability
enabled. Tests and builds from this review ran serially; unrelated host activity
was not controlled. These results are not matched before/after speedup evidence.

Each measured workload used 10 seconds of warmup and 120 measured seconds,
10,000 offered mutations/s, four writers, four table workers, 100-row
transactions, 256-byte payloads and 100,000 initial rows per table. All used
the same maintenance policy, 256 pending transactions and 1,024-row batches.
The hot case used 90% updates and hot-key selection; the other cases used
70% updates, 20% inserts and 10% deletes. Native compaction ran during ingestion.
The independent compactor produced the final reader reference afterward.

## Starvation regression

The release34 negative control fails at the intended assertion: orders' build
starts only after its own backlog drains. Both current five-phase cases pass:

| Queued workload | Build admission after restart | Transactions still pending |
| --- | ---: | ---: |
| One hot table | 1.543 s | 641 |
| Two hot tables | 1.532 s | 673 |

Publication and PostgreSQL ACK advance while actual compaction input reads are
held. The admitted builds publish, current rows match, both retained historical
snapshots remain unchanged, and shutdown succeeds. Admission timings include
fixture coordination and are not publication-latency measurements.

The guarantee is specifically soft data-build admission. Probes yield when the
planner prefers synchronous delete consolidation; that work can still wait for
a CDC gap or hard pressure. These workloads do not isolate that limitation as
the cause of a performance failure.

## Workload results

| Workload | Mutations/s including drain | Publication p95 / p99 | Missed transactions / 12,000 | Read ratio |
| --- | ---: | ---: | ---: | ---: |
| Mixed, one table | 9,911 | 1.809 / 2.546 s | 55 | 1.631 |
| Hot updates, one table | 9,956 | 0.628 / 1.562 s | 5 | 1.209 |
| Mixed, ten tables | 9,358 | 2.739 / 3.904 s | 515 | 1.444 worst table |

Every committed transaction was observed. Full source/target rows, final
materialized ACK, reference-compaction row equivalence and external
reconciliation passed. All warmup transactions committed. Missing scheduled
arrivals remain failures even where latency and read thresholds pass.

Read ratios use 20 alternating warmed DuckDB scan pairs against exact compacted
copies of the final rows. The mixed single-table median was 13.40 ms versus
8.21 ms compacted. These are post-ingestion planning/scan measurements, excluding
catalog lookup, not concurrent-query latency or steady-state endurance.

## Profile and next decision

The mixed single-table run completed 39 background and seven synchronous data
rewrites through the final ingestion publication. Its slowest 5% of transactions
spent a mean 2.014 seconds between journal durability and collapse start, versus
0.212 seconds from collapse start to catalog publication. The first interval
includes scheduling, refresh and mandatory maintenance; it is not pure queue time.

Maintenance state application took 12.76 seconds across operations. Its batch
counters attribute 4.96 seconds to primary-key lookups, 5.24 seconds to writes,
1.04 seconds to delta decoding and 1.09 seconds to mutation construction.
Plan/seal work took 9.48 seconds, and detached catch-up took 9.20 seconds, including
4.25 seconds of current-row lookup. These are nested work totals, not additive
wall-clock latency or separate throughput estimates.

A separate 50k/s diagnostic sampled thread stacks for 20 seconds. It preserved
exact rows and ACK but failed performance qualification; sampler overhead and
missed arrivals make its timings unsuitable for comparison with unprofiled runs.
Of 1,960 observations within the shared row-index lookup function, 1,635 were inside
RocksDB `MultiGet`. Index application had 3,285 observations, including 1,113
inside `MultiGet`. These inclusive stack counts include waits, not just CPU work.

The next focused target is primary-key lookup cost during index activation.
Separate native lookup work from Rust decoding before proposing an API change.
The present evidence does not justify a new borrowing API solely to remove
allocations, larger apply batches, external-SST staging or new scheduling domains.
Any candidate needs paired actual-RocksDB measurements and the same varied
workloads, preserving exact-state checks, durability and recovery semantics.

## Interrupted run and retained evidence

The first mixed attempt exhausted host disk space and stopped with a source
spool I/O error; Docker also stopped. It is invalid performance evidence. After
reclaiming disposable compiler cache and restoring the existing services, the
same daemon state recovered all 143,260 final rows and exact materialized ACK
`23/D1C860E0`, then shut down cleanly. The workload was repeated in a fresh
directory. This was an observed recovery incident, not a controlled disk-fault test.

Raw artifacts remain under `target/maintenance-review/`. `summary.json` and
`profile-result.json` retain commands and outcomes, including failures;
`analysis.json` contains phase attribution. Each benchmark report embeds binary,
configuration, harness and environment identity. The serial command scripts are
`run.py` and `resume.py`; the separate sampler is `profile.py`.

| Report | SHA-256 |
| --- | --- |
| `fairness-negative/report.json` | `ffdd290e737c638baa492a29c0b460565e67f1d3617b953ba742edb89b4de309` |
| `fairness-one-table/report.json` | `e5152fb7a8696b2e20d94e9227612f0c7529f980f748ff73eb052838c61cf79e` |
| `fairness-two-tables/report.json` | `6baf3d70f642643dc00775e79c579579a0e18f6721b30feb7c4c1a4cff987044` |
| `mixed-10k-repeat/report.json` | `a12fc878197c0afd20561d7ed87e3dffb0465a3a7d07ca7c9ba89952c42bf5a8` |
| `hot-updates-10k/report.json` | `9db9cd95d5ce741db56a759a7053980c00f7ecc9ab46b343f94e3fce8178ec4f` |
| `ten-tables-10k/report.json` | `85170bfdd1e32f648451fd12a4321a5f5a1670ec2b227efd3968f3e294b82e96` |
| `profile-50k/report.json` | `682b0c385499cde65242ecb358654a2904a88105fb2b6fc3f59babbc9c2f29d5` |
| `disk-full-recovery/report.json` | `8aa420cb768d60c7015559b1b353406f988bac1893da5fbe1ee21f5af54d3d68` |

The raw stack sample SHA-256 is
`7367a2a57640b5d8ebb53f35c0a8d795fe62bdff4e58482bb9d0e786d0235236`.
The [service test guide](../../../tests/production/README.md) documents the
workload flags and independent reader checks.
