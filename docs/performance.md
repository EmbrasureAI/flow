# Performance

The service has passing local correctness and recovery checks, but has not met
its full performance targets. The current targets are 50,000 mixed mutations/s,
PostgreSQL-commit-to-Iceberg-publication latency below 1 second at p95 and
2 seconds at p99, and read time at most 1.25 times a compacted equivalent.
No 24-hour endurance qualification has completed.

## V3 deletion-vector results

The latest [v2/v3 comparison](benchmarks/v3-deletion-vectors.md)
used six one-table runs with the same binary and policy, native background
compaction, 10 seconds of warmup and 60 measured seconds. All passed exact-row,
source-acknowledgement and external-reconciliation checks.

| V3 workload | Throughput including drain | Publication p95 | p99 |
| --- | ---: | ---: | ---: |
| Hot updates, 10k/s offered | 9,928/s | 523 ms | 1.54 s |
| Mixed, 10k/s offered | 9,895/s | 844 ms | 1.24 s |
| Mixed, 50k/s offered | 26,939/s | 44.46 s | 45.05 s |

Direct comparisons of identical final rows in the retained streaming layouts
measured v3 full scans at 1.07× v2 for mixed load and 2.20× for hot updates.
The hot-update layouts contained 25 data files for v3 versus three for v2;
these observations do not isolate delete-encoding cost. At 50k/s, accepted
source cohorts differed, so no same-row v2/v3 read comparison was made.

Both formats used DuckDB 1.5.5 with its external file cache disabled to work
around a confirmed shared-Puffin reader bug. The record explains the upstream
fix, retained failure reproduction, exact reader settings and hashes. These
results do not inherit the reader policy or qualification of older runs.
V3 preparation read amplification remains the measured high-load bottleneck.

## Reproducibility

The [benchmark report](benchmarks/v3-deletion-vectors.md) gives the environment,
workload, reader protocol, build identity and commands. These are finite local
observations; missed arrivals, latency failures and layout differences remain
part of the results. The [service test guide](../tests/production/README.md)
describes how to collect a new report against current source.
