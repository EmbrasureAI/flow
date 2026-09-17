# Expanded load matrix

All seven workloads passed complete source/target row equality, transaction
observation, exact PostgreSQL acknowledgement and external-compactor
reconciliation. None passed every performance gate. These results qualify
finite local correctness, not the full performance targets or endurance.

The measured release daemon was built from internal Flow revision
`36ff0b7210accb924993db73ba2eb9ab76cd663a`, not a commit in this public repository;
its SHA-256 is `22d8e99bab2a03a7206324b415caab85f43a09a3ca8247d5a7a906bb30417b1e`.
PostgreSQL 18.6, Iceberg REST 1.10.1 and MinIO ran in Docker on a shared Apple
M5 Pro / 48 GiB / 18-logical-CPU host. Docker had approximately 8 GiB memory;
PostgreSQL used 4 CPUs / 2 GiB, REST 2 CPUs / 1.5 GiB and MinIO 2 CPUs / 2 GiB.
The native macOS daemon and DuckDB 1.5.5 reader shared that host. Runs were serial,
with 10 seconds of warmup, 60 measured seconds, four source writers, four table
workers, Iceberg v2 and the default maintenance policy. Other tasks were active.

The test harness is ported here for rerunning against public Flow. These historical
measurements do not establish performance or correctness of the current public
binary. CI runs the wide-schema checks against the public source.

## Results

Throughput includes the final catalog drain. Misses are scheduled source
transactions, whose row counts differ by case. The reader ratio is the worst
per-table ratio of median scan times from twenty alternating pairs against the
same final data after compaction. The gate is 1.25×. RSS is the largest native
process observed at one-second intervals during workload/drain; it can miss
peaks and does not qualify the Linux allocator or production container budget.

| Case | Mutations/s including drain | Missed transactions | Publication p95 / p99 (ms) | Reader ratio | Sampled RSS (MiB) |
| --- | ---: | ---: | ---: | ---: | ---: |
| mixed-1k | 994 | 0 | 552 / 731 | 2.14× | 236 |
| wide-1k | 988 | 4 | 930 / 1167 | 1.04× | 492 |
| hot-updates-10k | 9,773 | 91 | 1038 / 2222 | 1.33× | 202 |
| large-tx-10k | 10,000 | 0 | 483 / 915 | 1.50× | 345 |
| single-row-1k | 929 | 3,315 | 866 / 1019 | 1.10× | 145 |
| hundred-tables-10k | 8,889 | 71 | 6504 / 7678 | 3.55× | 429 |
| mixed-50k | 46,102 | 0 | 5513 / 6019 | 1.10× | 455 |

`wide-1k` uses 16 KiB high-entropy hex-text payloads. `large-tx-10k` uses
10,000-row transactions. `single-row-1k` is append-only with one row per
transaction. `hundred-tables-10k` starts with one million rows across 100 tables;
its offered rate is aggregate. Other cases use one table. Consult the
[service guide](../../tests/production/README.md) for the exact workload contracts.

The 50k case accepted all measured source arrivals, then needed 5.07 seconds of
final drain. Its 6.02-second publication p99 fails the two-second target. A
separate source-only precheck achieved 48,452 mutations/s with 855 missed
transactions, so these shared-host runs cannot establish a stable 50k capacity
bound. At 1k mixed load and with 10,000-row transactions, ingestion rate and
publication latency passed; the remaining failure was reading the streaming
layout versus its compacted equivalent.

The mixed 1k streaming layout contained 11 data files and seven delete files
across 18 manifests, versus one data file and no deletes after the independent
rewrite. Query-layout costs should be evaluated alongside ingestion throughput.
Do not replace streaming measurements with measurements taken only after an
external rewrite, or silently remove missed arrivals from qualification.

## Wide-schema correctness

The new `wide_rows.py` checks passed on the same binary:

- 258 columns, 1,000 initial rows, up to 64 KiB text per row.
- 1,502 columns, 1,000 initial rows, small nullable values.
- Ten columns, 128 initial rows, eight 128 KiB text fields per row.

Each includes twelve complete update transactions, unchanged values, transitions
to and from NULL, SIGKILL, writes while stopped, key moves, deletes, and restart
with the original durable source identity. Every cell is compared with PostgreSQL
through the independent Iceberg reader and acknowledgement is fenced by
materialization. These wide-schema fixtures use compressible deterministic text;
they are correctness checks, separate from the high-entropy payload benchmark.
The PostgreSQL 18 service CI job includes all three cases.

## V3 and a matched configuration check

A separate v3 hot-update run at 10k offered mutations/s passed full correctness,
acknowledgement and external reconciliation. It delivered 9,907/s including
drain, missed ten source transactions, and measured publication p95/p99 of
1,990/3,629 ms. Its reader ratio was 1.07× and sampled native RSS was 239 MiB.
It fails the arrival and publication targets. The accepted source cohort differs
from the v2 run, so this is not a matched format-performance comparison.

Two further 1k mixed runs compared the default policy with the combined settings
`manifest_max_count = 8`, `stable_small_soft_files = 8`, and
`stable_small_hard_files = 12`. Both passed every gate and produced the identical
107,000-row SHA-256
`44475b126840e383c5ebf459130dc39296d0b4674933e821b0b81a28a770cf3d`.

| Measurement | Fresh default | Combined settings |
| --- | ---: | ---: |
| Publication p95 / p99 | 623 / 942 ms | 737 / 1,118 ms |
| Streaming scan median | 41.84 ms | 37.18 ms |
| Scan ratio against own compacted layout | 1.24× | 1.20× |
| Catalog table POST calls through drain | 165 | 183 |
| Sampled native RSS | 236 MiB | 260 MiB |

The modest scan gain costs publication latency, catalog calls and sampled memory.
This single pair does not justify changing defaults. The original default run's
2.14× reader ratio and the repeat's 1.24× also show that one final streaming
layout cannot establish a continuous scan guarantee. To reproduce the combined
settings, pass those three corresponding hyphenated flags to `benchmark.py`;
keep the binary, source workload, seed, duration and reader protocol matched.

## Recovery coverage

Journal quota, composite keys, physical index reconstruction, hard reader debt
and external-rewrite unpausing passed. The 120-second stress run committed 795
writer transactions and passed four full data checkpoints, 70 live snapshot
checks, nine retained-snapshot checks and a 943-snapshot metadata audit. It
included full and savepoint rollbacks and SIGKILL with active writers.

The fault suite passed lost successful commit responses, catalog/object-store
outages and restarts, PostgreSQL restart, SIGKILL with journaled unacknowledged
work, successful catalog commit followed by index loss, and unchanged TOAST.
The external test compactor required three supervised restarts during outages;
these remain availability incidents rather than uninterrupted-service claims.

## Reproduction and limits

Freeze a release build and start the disposable services as described in the
[service test guide](../../tests/production/README.md), then run:

```sh
python3 tests/production/matrix.py \
  --catalog-uri "$FLOW_REST_URL" --s3-endpoint "$FLOW_S3_URL" \
  --binary target/qualification-binaries/current/embrasure-flow \
  --build-manifest target/qualification-binaries/current/build-manifest.json \
  --reader-baseline-compactor target/qualification-binaries/current/local_compactor \
  --duration 60 --warmup 10 --artifacts target/expanded-matrix \
  --case mixed-1k --case wide-1k --case hot-updates-10k --case large-tx-10k \
  --case single-row-1k --case hundred-tables-10k --case mixed-50k
```

Use fresh artifact directories. Performance-only failures are retained and the
matrix continues; incomplete correctness stops it. The completed matrix above
ran after recovery from an earlier Docker resource outage. The interrupted
source-capacity and matrix attempts are not included in its measurements.
A separate interrupted wide-schema run recovered all 924 expected final rows
after the object-store service was restored; its original report remains failed.

No 24-hour endurance run, independent-host loss, Glue/Athena measurement or
production Linux memory-limit qualification is established by this matrix.
