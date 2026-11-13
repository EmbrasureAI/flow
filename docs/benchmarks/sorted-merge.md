# Sorted merge compaction

Measured on 2026-11-13. Preserving completed builds across delete consolidation
and admitting the large sorted merge improve selective reads while keeping CDC
service cost below the baseline in the 2M-row comparison. The 2M-row update workload
still misses the absolute publication-latency target. These are single-host
fixture results, not a cloud-latency or ingest-capacity qualification.

## Change and cause

Normal position-delete consolidation used to invalidate concurrent data builds
because their delete files disappeared, even when the replacement preserved the
effective deleted positions. Catch-up now proves that every base delete on its
unchanged selected inputs remains deleted at the head. It rejects resurrection,
including equal-count swaps, and retains only current head deletes for residuals.
Compact replacement sets fitting the read budget skip restaging original input
holes; larger sets retain a disk-backed proof. Existing input and time bounds
remain enforced.

The conservative merge charge was about 740 MiB on the large rewrite, exceeding
both the former 256 MiB default and a 512 MiB experiment. The default is now
1 GiB per worker, with merge enabled. The admission calculation is unchanged;
it counts logical fetched/decoded payload, not a process RSS cap. Writer buffers,
remaps and allocator overhead remain additional.

Profiling the former DuckDB 1.5.5 reader found position-delete hash-set work
masking data pruning gains. DuckDB 2.0 uses a different delete representation.
Both sides of each comparison below use the same reader version. Updating the
reader alone did not make the old writer outputs consistently faster; the writer
corrections improve both tested readers. The tables are unpartitioned: the
pruning measured here is Parquet row-group pruning.

## Environment and protocol

Amazon Linux 2023 x86_64, Linux 6.18.56, 8 vCPU, 16 GiB RAM, Rust 1.97.1;
PostgreSQL 18.6, the repository REST catalog fixture and MinIO on the same host.
Release ThinLTO with jemalloc. Four writers, 100 mutations/transaction, 256-byte
compressible payload, uniform keys, ordinary Iceberg v2. Each side has three
alternating runs at 30,000 offered mutations/s, 60 seconds measured after
10 seconds warmup. `update90` starts with 2,000,000 rows; `mixed` with 100,000.
Every run performs full row verification against PostgreSQL. CDC stage totals use the
A/B harness's complete epoch accounting, including warmup and drain.

The base is the previous revision, with sorted merge available but off by
default. The candidate adds this change: merge on by default, 1 GiB admission
and the replacement proof. No candidate configuration overrides were used.
This report was added after measurement; measured implementation files are unchanged.

```sh
tests/production/ab.py --base origin/main --candidate . --profile release --runs 3 \
  --args "--profile update90 --initial-rows 2000000 --rate 30000 --duration 60 --warmup 10"
tests/production/ab.py --base origin/main --candidate . --profile release --runs 3 \
  --args "--profile mixed --initial-rows 100000 --rate 30000 --duration 60 --warmup 10"
```

No compilation or query benchmark ran concurrently with timed ingestion.
Earlier disk-constrained experiments are excluded from these final tables;
completed source tables were removed between experiments to retain headroom,
while Iceberg snapshots and measurement reports were kept.

## Ingestion

All twelve final correctness checks pass. Publication gates require p95 <= 1 s
and p99 <= 2 s, including clock uncertainty. The arrival gate requires no missed
measured transactions and at least 95% of the offered rate committed in the
measurement window. One missed transaction represents 100 offered mutations.

### 2M-row update90

| measure (median) | base | candidate | change |
|---|---:|---:|---:|
| CDC service µs/mutation | 19.8 | 18.9 | -4.2% |
| drain mutations/s | 25,263.8 | 27,188.0 | +7.6% |
| commit→catalog p95 ms | 9,091.8 | 8,497.1 | -6.5% |
| collapse s | 6.6 | 6.3 | -4.0% |
| prepare s | 13.4 | 12.6 | -6.0% |
| apply + ledger s | 17.8 | 16.9 | -5.4% |
| compaction stall s | 4.3 | 6.1 | +42.1% |
| peak RSS MiB | 607.4 | 616.3 | +1.5% |

base service µs/mutation range: 19.5–20.5

candidate service µs/mutation range: 18.5–19.9

Base runs 1/2/3: admitted source commits [28783, 29965, 30000] mutations/s; missed measured arrivals [699, 21, 0] transactions; missed warmup arrivals [0, 0, 0]. Arrival gates passed: 1/3; publication latency gates passed: 0/3.

Candidate runs 1/2/3: admitted source commits [29995, 29997, 28228] mutations/s; missed measured arrivals [0, 0, 1124] transactions; missed warmup arrivals [0, 0, 261]. Arrival gates passed: 2/3; publication latency gates passed: 0/3.

The three first large sorted merges published successfully. Two later candidate
builds exceeded the existing delete-input budget and replanned; that bound was
not relaxed. The third candidate also spent 2.86 s writing residual deletes on
its first large catch-up, versus about 1.35 s for its proof/staging/mapping work.
Stall time remains higher despite lower median CDC service and publication p95.
The per-run variation and missed arrivals limit claims about precise effect size.

### Mixed

| measure (median) | base | candidate | change |
|---|---:|---:|---:|
| CDC service µs/mutation | 16.8 | 17.1 | +1.8% |
| drain mutations/s | 29,864.0 | 29,856.9 | -0.0% |
| commit→catalog p95 ms | 533.2 | 531.4 | -0.3% |
| collapse s | 4.6 | 4.5 | -1.5% |
| prepare s | 12.1 | 12.4 | +2.7% |
| apply + ledger s | 13.9 | 14.0 | +0.7% |
| compaction stall s | 8.7 | 8.7 | +0.0% |
| peak RSS MiB | 454.1 | 503.8 | +10.9% |

base service µs/mutation range: 16.7–17.0

candidate service µs/mutation range: 16.8–17.3

Base runs 1/2/3: admitted source commits [30000, 30000, 30000] mutations/s; missed measured arrivals [0, 0, 0] transactions; missed warmup arrivals [0, 0, 0]. Arrival gates passed: 3/3; publication latency gates passed: 3/3.

Candidate runs 1/2/3: admitted source commits [30000, 29998, 29998] mutations/s; missed measured arrivals [0, 0, 0] transactions; missed warmup arrivals [0, 0, 0]. Arrival gates passed: 3/3; publication latency gates passed: 3/3.

## Reader results

Read the six immutable final update90 snapshots serially after ingestion stopped.
External file caching is disabled; metadata/connections are warmed. The main
protocol uses 40 seeded positive point lookups, 20 ranges of 1,000 IDs, one full
aggregate, and 10 sets of 50 marker IDs, each repeated three times. Every result
is checked against a full independent PyIceberg scan. The complete final
snapshots also match the retained PostgreSQL row digests, including maintenance
completed after the ingestion verification phase. Times include planning and
fetching results, excluding reference checks. Tables show medians of the three
per-table medians. This measures the whole writer outcome, including delete and
file counts; it does not isolate sorting from maintenance scheduling.

DuckDB 2.0.0, Iceberg `4c0c0874c4`:

| Query (median ms) | Base | Candidate | Change |
|---|---:|---:|---:|
| Point lookup | 116.57 | 51.48 | -55.8% |
| 1,000-key range | 99.13 | 43.75 | -55.9% |
| Full aggregate | 213.45 | 129.99 | -39.1% |
| 50-marker lookup | 91.43 | 43.06 | -52.9% |

Compatibility diagnostic on the same six snapshots with DuckDB 1.5.5,
Iceberg `45163a28`: five points, five ranges, one full aggregate and three marker
sets, each repeated three times, with the same independent checks. Its smaller
query set makes absolute cross-version comparisons approximate; within-version
base/candidate comparisons use the same protocol.

| Query (median ms) | Base | Candidate | Change |
|---|---:|---:|---:|
| Point lookup | 461.96 | 67.60 | -85.4% |
| 1,000-key range | 442.12 | 59.27 | -86.6% |
| Full aggregate | 625.75 | 178.41 | -71.5% |
| 50-marker lookup | 530.25 | 68.88 | -87.0% |

## Layout and actual reads

Median physical rows in verified sorted files increased from 60.1% to
100.0%. Every stamped candidate file was checked for actual key ordering.
Median retained delete rows fell from 1,222,541 to 126,588. Files can still overlap
in key range; this is not a claim of global nonoverlapping partitions.

A transparent local HTTP proxy counted response body bytes for three identical
positive point keys on final base-1/candidate-1, with external file cache off.
Proxy timings are excluded from the latency table; headers and TLS overhead are
not counted. This is observed I/O, not a row-group-bound simulation.

| HTTP response body MiB/query | Base | Candidate | Change |
|---|---:|---:|---:|
| Data | 28.319 | 11.100 | -60.8% |
| Deletes | 4.229 | 1.047 | -75.2% |
| Metadata/other | 0.263 | 0.226 | -14.2% |

The full query protocol is reproducible with
[`query_benchmark.py`](../../tests/production/query_benchmark.py).

## Validation

664 Rust tests passed, 0 failed, 39 ignored, including 150 randomized merge seeds.
Workspace formatting and clippy passed, as did 76 production Python tests.
Failure-path coverage includes resurrection with unchanged counts, both compact
and disk-backed retention proofs, exclusion of stale residual deletes, late
changes during merge, v2/v3 lineage, lost catalog responses and index reopen.
The broader PostgreSQL service-version matrix remains a CI check; the fixture
measurements here use PostgreSQL 18.6.
