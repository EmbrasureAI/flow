# Source-generator capacity prerequisite

The source-only prerequisite fails at 50,000 offered mutations/s with both four
and eight writers. These runs use the exact `Benchmark.writer`, seed and payload
expression, with the existing bounded SQLite recorder and disk key pools. They
create no ingestion daemon, replication slot, catalog, object-store or reader
connection. They therefore measure source generation and PostgreSQL response
capacity, not ingestion throughput or catalog latency.

The serial order was four writers, eight writers, then four writers again. Each
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

## Retained evidence

Raw directories are ignored local artifacts. Report hashes pin the results; each
report also includes SQLite, transaction-export and key-file hashes, source-code
identity, machine information and the PostgreSQL observations.

| Report | SHA-256 |
| --- | --- |
| [Initial four writers](../../target/release35-source-capacity/writers4/report.json) | `1b4efb8f898a6fbcec04475924a95f6af9dd0a5b202a10c25c6b98b92f957ce7` |
| [Eight writers](../../target/release35-source-capacity/writers8/report.json) | `8bd3ad34a2aa7342521da16fe4976e10fb73c6e09cc82705d98eb8f7ac896c88` |
| [Repeated four writers](../../target/release35-source-capacity/writers4-paired/report.json) | `702e394908df84cdf765ec973f72d3e9d3070e46093dc287fe80300acd9f93fe` |

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
