# Iceberg v3 deletion-vector benchmark

All six local runs passed full-row verification, transaction-marker observation, exact PostgreSQL WAL acknowledgement, and external compactor reconciliation. V3 sustained 10k mutations/s with publication p95 below one second and p99 below two seconds in both workloads. At 50k/s offered, v3 fell behind: 26,939 mutations/s including drain, with publication p95/p99 of 44.46/45.05 seconds. This is not a production performance qualification.

## Protocol

Runs used the same frozen release binary, default maintenance policy and 256 pending transactions. Each had one table, 100,000 initial rows, four writers, 100 mutations per transaction, 256-byte compressible payloads, 10 seconds of warmup and 60 measured seconds. Mixed means 70% updates, 10% deletes and 20% inserts; hot-update means 90% updates and 10% inserts with hot-key sampling. Every v3 inventory verified actual format version 3 and live Puffin deletion vectors.

PostgreSQL 18.6, a Java Iceberg 1.10.1 REST catalog backed by PostgreSQL, and MinIO ran in Docker on the shared Apple M5 Pro / 48 GiB host. Native compaction ran throughout each workload. An independent compactor then rewrote the final table, and the daemon reconciled it before another full-row check. Source durability settings remained enabled. No benchmark-owned builds or profiling ran during timed ingestion. This host was not reserved. Treat these finite runs as local observations, not isolated hardware capacity or a competitive product comparison.

The host had 18 logical CPUs and Docker had 8 GiB of memory. PostgreSQL used a 4-CPU/2-GiB limit, MinIO 2 CPUs/2 GiB, and REST 2 CPUs/1.5 GiB. The daemon and reader shared the host with the workload generator.

DuckDB 1.5.5 used signed core extensions: Iceberg `45163a28`, Avro `f9d5902`, HTTPFS `827222f`. All v2 and v3 runs disabled `enable_external_file_cache` because of the confirmed reader bug below. HTTP connection caching remained enabled.

## Ingestion

“Source” counts PostgreSQL commit responses inside the measured window. “Catalog including drain” divides the measured cohort by the window plus final publication delay. They are different quantities. Percentiles measure PostgreSQL commit to catalog acknowledgement, excluding warmup. Clock calibration brackets the complete observation drain; all six final runs passed its uncertainty check.

| Workload | Format | Source mutations/s | Catalog including drain/s | Publication p95 | p99 | Measured arrivals missed | Final drain |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| update90-hot-10k | v3 | 10,000 | 9,928 | 0.523 s | 1.537 s | 0/6,000 | 0.437 s |
| update90-hot-10k | v2 | 10,000 | 9,961 | 0.518 s | 1.513 s | 0/6,000 | 0.236 s |
| mixed-10k | v2 | 10,002 | 9,932 | 0.754 s | 0.977 s | 0/6,000 | 0.409 s |
| mixed-10k | v3 | 10,000 | 9,895 | 0.844 s | 1.238 s | 0/6,000 | 0.639 s |
| mixed-50k | v3 | 47,312 | 26,939 | 44.455 s | 45.053 s | 1,611/30,000 | 45.382 s |
| mixed-50k | v2 | 44,820 | 38,894 | 11.509 s | 12.518 s | 2,999/30,000 | 9.421 s |

All 10k cases admitted every warmup and measured arrival and finished with 170,000 rows. Final rows matched between formats for each workload. At 50k, v3 also missed 214 warmup arrivals; v2 missed none. Final row counts were 431,750 for v3 and 420,010 for v2. These unequal accepted cohorts prevent a same-row high-load read comparison or a controlled saturation speedup claim.

V3 PostgreSQL-commit-to-reader p95/p99 was 1.49/2.39 seconds for hot updates, 1.67/1.98 seconds for mixed 10k, and 47.53/49.07 seconds for mixed 50k. Reader observation includes the one-second polling interval and query duration.

## Direct streaming-layout read comparison

For the two 10k pairs, the complete sorted source/target row hashes matched across formats. The retained streaming snapshots were compared directly after all owned writers stopped, with two warmup pairs and twenty alternating v2/v3 pairs per query. Both use the cache workaround. The aggregate query reads count, sum(version), and sum(payload length); the selective query adds `id BETWEEN 1 AND 1000`. Timings include planning and scan execution, excluding REST lookup.

| Workload | Query | v2 median | v3 median | v3/v2 |
| --- | --- | ---: | ---: | ---: |
| update90-hot-10k | full | 25.95 ms | 56.99 ms | 2.20× |
| update90-hot-10k | selective | 11.27 ms | 42.70 ms | 3.79× |
| mixed-10k | full | 32.80 ms | 35.24 ms | 1.07× |
| mixed-10k | selective | 35.38 ms | 37.02 ms | 1.05× |

The hot-update v3 snapshot had 25 data files and 12 DVs, versus three data files and one position-delete file for v2. A separate detailed profile recorded full-query planning at 16.16 ms versus 5.43 ms. This diagnoses the observed layouts; it does not isolate bitmap decoding cost or establish a universal format penalty. The single detailed profiles are explanatory samples, separate from the twenty unprofiled timing pairs.

Each run also retained its original same-final-row comparison against external compaction. Those v3 layout ratios were 2.26× (hot updates), 1.47× (mixed 10k), and 1.08× (mixed 50k). They are fragmentation diagnostics against fully compacted copies, not alternate streaming-ingestion baselines. The legacy 1.25× gate remains in reports; only hot-update v2 passed every existing gate. No 24-hour run was performed.

## Measured bottleneck

At 50k, ingestion preparation took 61.32 seconds in v3 versus 16.07 seconds in v2 across warmup, measurement and drain. The `other_prepare` region took 51.97 versus 4.02 seconds, 12.47× per accepted mutation. This region contains prior cumulative-DV merging, making it the leading candidate for the next focused optimization; it is not separately timed yet. Normalized range-read and metadata request counts increased 7.19× and 9.10×. Data/index staging, catalog commit and index application did not explain the regression. Timers overlap, so their sums must not be presented as end-to-end wall time. No thresholds were tuned against these results.

## Reader compatibility

DuckDB 1.5.5 with Iceberg extension `45163a28` can apply the wrong blob when its external file cache reads shared Puffin objects. All six results use `SET enable_external_file_cache = false` for both formats. Independent Spark 3.5.6 / Java Iceberg 1.10.1 verification confirmed the affected snapshot's complete rows and row lineage. See the [upstream multi-blob Puffin fix](https://github.com/duckdb/duckdb-iceberg/commit/8b2e25f4c883fe0e22aaf27510115a6159c8f6bc). Signed extension `6561bfca` passed the same history replay with caching enabled, but was not used for these timings.

## Reproducing the workload

Start disposable services and set the connection environment using the [service test guide](../../tests/production/README.md). Freeze a build and run each case serially in a fresh artifact directory. For example, the v3 mixed 10k case uses:

```sh
python3 tests/production/freeze_build.py --output target/benchmark-build
uv run tests/production/benchmark.py \
  --catalog-uri http://127.0.0.1:58181 \
  --s3-endpoint http://127.0.0.1:59000 \
  --binary target/benchmark-build/embrasure-flow \
  --build-manifest target/benchmark-build/build-manifest.json \
  --reader-baseline-compactor target/benchmark-build/local_compactor \
  --reader-baseline-max-input-files 16384 --reader-baseline-timeout 900 \
  --tables 1 --initial-rows 100000 --writers 4 --transaction-rows 100 \
  --row-bytes 256 --table-workers 4 --batch-rows 1024 \
  --pending-transactions 256 --seed 20260905 --reader-poll-seconds 1 \
  --duration 60 --warmup 10 --rate 10000 --profile mixed \
  --key-distribution uniform --format-version 3 \
  --artifacts target/benchmarks/mixed-10k-v3
```

Run formats 2 and 3 with `mixed`/`uniform` at 10,000 and 50,000 mutations/s, and `update90`/`hot` at 10,000. Keep the other settings fixed. Reports include actual table format, live deletion-vector inventory, source arrivals, clock uncertainty, full-row verification, reader settings and binary/configuration identities. A performance-target miss produces a nonzero exit even when correctness passes; inspect both outcomes.

The direct reader comparison uses the `post-workload-reader` metadata locations from each format's report. Require matching full-row counts and SHA-256 values from `full-source-target-verification` before comparing. Stop owned writers, use the reader settings above, run two warmup pairs followed by twenty alternating pairs, and report the ratio of median elapsed times. The full query is `SELECT count(*), sum(version), sum(length(payload)) FROM iceberg_scan(?)`; the selective query adds `WHERE id BETWEEN 1 AND 1000`. Compare streaming snapshots before the external rewrite. Do not substitute a compacted snapshot or compare unequal accepted datasets.

## Measurement identity

The frozen build used base commit `fcd7f15e8cb90a6261b48458e3d0151e118f1a8e` plus the v3 working changes, with dirty-source SHA-256 `494f4e50c09fe2b7b719a0208f7f0a9fcf6a3cbf65eb50f5bb20f94ccc73f5cd`. Daemon SHA-256 is `351a8761c2403bcc02bb2469f8c389d838a5fe999deec4953b820591c2647e4f`; external compactor SHA-256 is `8386d7eda8ee0155bb85185364be0ba9a03b90cd4bb8c3036c4fdc8ceeb06ca8`.

The subsequent shared-Puffin consolidation acceptance fix is not included in this measured binary. Rebuilding current source produces new evidence, not these exact timings. Raw reports and binaries were retained locally and are not distributed with the repository; the hashes below identify the reports used here.

| Report | SHA-256 |
| --- | --- |
| `update90-hot-10k-v3/report.json` | `743d27f0a0da8979353a6dfe81bd1105299ca42bc86ea0324c9325164c7a0be2` |
| `update90-hot-10k-v2/report.json` | `36c84ced5a6473a7fe61867e90ce6051d5edda34fcee4cca51c025dcfd45935c` |
| `mixed-10k-v2/report.json` | `2760ab9a7b8fa7b6545411eee52c2706a97e3a5dfd0dca1c05ef84fe8e964118` |
| `mixed-10k-v3/report.json` | `c97b0a663b20d3f178ed24bdbe387250abef18f46b6b37ef7e98ee8da47095fd` |
| `mixed-50k-v3/report.json` | `89615d925c861ddfef7643cf711a0bb40daf5e2fae3e2cfe0c6edf856b01a5e9` |
| `mixed-50k-v2/report.json` | `3313aa6b3d29fd8660d37e01a4cb370533be848957f7b249e6a3b39d31ebbd24` |
