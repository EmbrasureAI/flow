# Performance

The service has passing local correctness and recovery checks, but has not met
its full performance targets. The current targets are 50,000 mixed mutations/s,
PostgreSQL-commit-to-Iceberg-publication latency below 1 second at p95 and
2 seconds at p99, and read time at most 1.25 times a compacted equivalent.
No 24-hour endurance qualification has completed.

## Expanded load and schema coverage

The [expanded load matrix](benchmarks/load-matrix.md) passed exact data checks
for all seven cases, including 100 tables, 16 KiB payloads, one-row transactions
and 10,000-row transactions. The 50k offered case delivered 46,102 mutations/s
including drain, with publication p99 of 6.02 seconds. Low-rate streaming layouts
also missed the compacted-reader target. Separate 258-column, 1,502-column and
approximately MiB-row recovery checks passed. Read the report's workload and
shared-host limits before using these observations for sizing.

## Publication batching across many tables

Before this change, each table's publication epoch held at most its share of
the global `pending_transactions` window (256 descriptors by default, counting
queued and in-flight work, one per affected table). With N busy tables, an
epoch carried about 256/N transactions, so small transactions were limited to
roughly 256/N × `table_workers` per catalog commit latency: on the order of
10–20 transactions/s across 100 tables at 0.5–1 s commits, and about 256 per
commit for a single table. Workloads with many rows per transaction hid this.

The window now bounds only queued lookahead. A dispatched epoch continues
through its table's durable ledger references, up to 32 MiB of payload and
about 4 MiB of in-memory transaction descriptors (roughly 16,000 single-table
transactions), so epoch size no longer depends on how many other tables are
busy. Descriptor memory is bounded by the lookahead plus that per-epoch limit
for each running worker. Catalog commit latency, the commit-rate budget and
`table_workers` still bound throughput. Set `limits.epoch_max_transactions` to
cap the source transactions in one epoch when per-epoch commit work or latency
matters more than batching. The results on this page predate the change and
have not been rerun.

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
