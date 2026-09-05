# Performance

The service has passing local correctness and recovery checks, but has not met
its full performance targets. The current targets are 50,000 mixed mutations/s,
PostgreSQL-commit-to-Iceberg-publication latency below 1 second at p95 and
2 seconds at p99, and read time at most 1.25 times a compacted equivalent.
No 24-hour endurance qualification has completed.

## Recorded local results

Each case below has 10 seconds of warmup and 60 measured seconds, with native
compaction enabled. Mutations include inserts, updates and deletes. Publication
latency ends at the catalog commit; it is distinct from reader observation time.

| Workload | Throughput including drain | Publication p50 | p95 | p99 | Read-time ratio |
| --- | ---: | ---: | ---: | ---: | ---: |
| 100 tables, 10k/s offered, 8 table workers | 9,836/s | 550 ms | 838 ms | 1.05 s | 1.10 median table; 1.76 worst |
| One table, 50k/s offered, default settings | 41,408/s | 3.35 s | 9.51 s | 9.98 s | 2.43 |
| One table, 50k/s offered, 320 pending transactions | 38,211/s | 2.37 s | 6.44 s | 6.99 s | 1.10 |

The 100-table case admitted all 6,000 measured transactions. The default and
320-window high-load cases missed 1,424 and 5,743 of 30,000 scheduled measured
transactions, respectively. Every committed transaction was observed; full
source/target rows and materialized source acknowledgement matched. Unequal
accepted workloads prevent attributing the lower latency or read ratio to the
larger window. The default remains 256 pending transactions.

Read ratios compare warmed DuckDB aggregate scans against fully compacted copies
of the same final rows, using 20 alternating pairs and pinned snapshots. They
measure planning and scan time after ingestion stops, excluding REST lookup.
They do not measure contention during active ingestion or all query patterns.
The default high-load case took 23.54 ms versus 9.67 ms after compaction.

## Environment and reproducibility

These are finite single-host tests on an Apple M5 Pro with 48 GiB RAM. Docker had
8 GiB; PostgreSQL had a 4-CPU/2-GiB limit, with fsync, synchronous commit and full
page writes enabled. MinIO and the REST catalog each had 2-CPU limits. The
high-load cases use four source writers, 100-mutation transactions, 256-byte
payloads and 100,000 initial rows. Hosted CI uses a smaller fixture and does not
establish these performance figures.

The [100-table record](development/qualification/release38.md) and
[high-load records](development/qualification/release39.md) identify frozen
binaries, configurations and report hashes. Later source changes do not inherit
a qualification from those builds. The [service test guide](../tests/production/README.md)
describes how to run the workload and retain new artifacts. Historical failures
and rejected optimizations remain in the [qualification history](development/production-qualification.md).
