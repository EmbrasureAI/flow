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
host limits before using these observations for sizing.

## Collapse memory

The [collapse memory comparison](benchmarks/collapse-memory.md) raised the
default `limits.collapse_memory_bytes` from 32 MiB to 128 MiB. With the old limit,
most backlogged one-table epochs overflowed into disk collapse. On an 8-vCPU VM at
50k offered mixed mutations/s, the daemon drained 21,684–22,316 mutations/s
against 18,254–19,888/s, with commit-to-catalog p95 of 18–20 s against 29–30 s.
Both still miss the latency target.

## Publication batching across many tables

The shared `limits.pending_transactions` window (256 descriptors by default)
bounds only queued lookahead, which makes tables schedulable. A dispatched epoch
continues in order through its table's durable ledger references, up to 32 MiB
of payload and about 4 MiB of in-memory transaction descriptors (roughly 16,000
single-table transactions), so epoch size does not depend on how many other
tables are busy. Descriptor memory is bounded by the lookahead plus that
per-epoch limit for each running worker. Catalog commit latency, the
commit-rate budget (`limits.commits_per_second`) and `limits.table_workers`
bound throughput. The optional `limits.epoch_max_transactions` (unset by
default) caps the source transactions in one epoch when per-epoch commit work
or latency matters more than batching.

The measurements on this page were taken on pre-release development builds,
some with different epoch sizing, and have not been repeated on the current
release. Treat them as indicative, not as current results.

## V3 deletion-vector results

The [v2/v3 comparison](benchmarks/v3-deletion-vectors.md)
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
around a confirmed shared-Puffin reader bug. The record links the upstream
fix and gives the exact reader settings. V3 preparation read amplification
remains the measured high-load bottleneck.

## Compaction worker

A compaction worker no longer converts scanned Arrow batches into rows and back
([report](benchmarks/arrow-compaction-rewrite.md)), and it now stages row
mappings on a separate thread into scratch stores with larger memtables
([report](benchmarks/compaction-staging.md)). On the focused worker benchmark,
16 files of 32,768 ten-column rows, the staging change cut median worker latency
a further 30–32% on two vCPUs and 14–24% on one, with 14–24% less CPU per
compaction, for up to about 90 MiB more peak memory per running build. These
are worker timings on local disk, not publication latency or service
throughput; the numbers above were not rerun.

Those worker benchmarks linked the system allocator. Built with jemalloc, as
the daemon is, the same worker benchmark ran 9–17% faster on two vCPUs in
paired runs on a noisy host, with 8–16% less CPU
([report](benchmarks/jemalloc-and-reconcile.md)). The same report times
reconciliation of an external rewrite at about 46,000 rows/s on two vCPUs,
mostly in unbatched row-index lookups and apply; larger scratch memtables gave
no consistent gain there and were not adopted.

## Reproducibility

The [benchmark report](benchmarks/v3-deletion-vectors.md) gives the environment,
workload, reader protocol and commands. These are finite local
observations; missed arrivals, latency failures and layout differences remain
part of the results. The [service test guide](../tests/production/README.md)
describes how to collect a new report against current source.
