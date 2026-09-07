# Iceberg v3 deletion-vector benchmark

All six local runs passed full-row verification, transaction-marker observation, exact PostgreSQL WAL acknowledgement, and external compactor reconciliation. V3 sustained 10k mutations/s with publication p95 below one second and p99 below two seconds in both workloads. At 50k/s offered, v3 fell behind: 26,939 mutations/s including drain, with publication p95/p99 of 44.46/45.05 seconds. This is not a production performance qualification.

## Protocol

Runs used the same frozen release binary, default maintenance policy and 256 pending transactions. Each had one table, 100,000 initial rows, four writers, 100 mutations per transaction, 256-byte compressible payloads, 10 seconds of warmup and 60 measured seconds. Mixed means 70% updates, 10% deletes and 20% inserts; hot-update means 90% updates and 10% inserts with hot-key sampling. Every v3 inventory verified actual format version 3 and live Puffin deletion vectors.

PostgreSQL 18.6, a Java Iceberg 1.10.1 REST catalog backed by PostgreSQL, and MinIO ran in Docker on the shared Apple M5 Pro / 48 GiB host. Native compaction ran throughout each workload. An independent compactor then rewrote the final table, and the daemon reconciled it before another full-row check. Source durability settings remained enabled. No owned builds or profiling ran during timed ingestion. This host was not reserved; unrelated work was observed during earlier attempts. Treat these finite runs as local observations, not isolated hardware capacity or a competitive product comparison.

DuckDB 1.5.5 used signed core extensions: Iceberg `45163a28`, Avro `f9d5902`, HTTPFS `827222f`. All v2 and v3 runs disabled `enable_external_file_cache` because of the confirmed reader bug below. HTTP connection caching remained enabled. This reader policy differs from older benchmark records.

## Ingestion

“Source” counts PostgreSQL commit responses inside the measured window. “Catalog including drain” divides the measured cohort by the window plus final publication delay. They are different quantities. Percentiles measure PostgreSQL commit to catalog acknowledgement, excluding warmup. Clock calibration now brackets the complete observation drain; all six final runs passed its uncertainty check.

| Workload | Format | Source mutations/s | Catalog including drain/s | Publication p95 | p99 | Measured arrivals missed | Final drain |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| update90-hot-10k | v3 | 10,000 | 9,928 | 0.523 s | 1.537 s | 0/6,000 | 0.437 s |
| update90-hot-10k | v2 | 10,000 | 9,961 | 0.518 s | 1.513 s | 0/6,000 | 0.236 s |
| mixed-10k | v2 | 10,002 | 9,932 | 0.754 s | 0.977 s | 0/6,000 | 0.409 s |
| mixed-10k | v3 | 10,000 | 9,895 | 0.844 s | 1.238 s | 0/6,000 | 0.639 s |
| mixed-50k | v3 | 47,312 | 26,939 | 44.455 s | 45.053 s | 1,611/30,000 | 45.382 s |
| mixed-50k | v2 | 44,820 | 38,894 | 11.509 s | 12.518 s | 2,999/30,000 | 9.421 s |

All 10k cases admitted every warmup and measured arrival and finished with 170,000 identical rows. At 50k, v3 also missed 214 warmup arrivals; v2 missed none. Final row counts were 431,750 for v3 and 420,010 for v2. These unequal accepted cohorts prevent a same-row high-load read comparison or a controlled saturation speedup claim.

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

## Fixes and reader compatibility

- REST table creation previously dropped the typed format version. It now sends the reserved `format-version` property; bootstrap rejects a wrong format returned for a newly created table. The harness checks actual metadata before timed work.
- The external compactor fixture now preserves v3 row lineage and budgets shared Puffin objects once.
- Delete-consolidation locality checks now remove logical DV IDs correctly rather than comparing them to physical object paths.
- Clock calibration includes tail drain, timed-out source-fixture containers are cleaned up, and failed verification retains its exact metadata and CSV.

Two earlier hot-update attempts returned incorrect rows through the long-lived DuckDB connection. The captured second failure had 165,401 rows / 165,289 distinct keys, while fresh DuckDB and independent Spark/Java Iceberg reads of the exact snapshot matched all 170,000 source rows. Java also verified unique, non-null row IDs and non-null update sequences. The retained-object audit found no overwrite evidence across 224 snapshots.

Replaying the 68 observed snapshot reads reproduced the same bad result deterministically. The installed Iceberg extension used the base buffer pointer instead of the adjusted pointer for a cached Puffin range, allowing one blob’s deletion vector to be applied to another target. This is the [upstream multi-blob Puffin fix](https://github.com/duckdb/duckdb-iceberg/commit/8b2e25f4c883fe0e22aaf27510115a6159c8f6bc). Disabling `enable_external_file_cache` made the full replay and five repeated exports correct. Disabling Parquet metadata caching or forcing one reader thread did not. Signed nightly extension `6561bfca`, which contains the fix, also passed with the cache enabled; it was only a diagnostic control, not the benchmark dependency.

The shared test reader loader therefore disables that cache on the pinned stable reader. Applications using the affected extension should do the same or use an extension containing the upstream fix. The six final runs all passed with this workaround; earlier failed reports remain retained and are not mixed into the results.

Validation included 40 Python harness tests, six REST create-table tests, the targeted locality regression, the v3 pipeline and delete-rewrite integration suites, formatting, and zero-fuzz REST patch replay. The preceding full workspace run is recorded separately in `target/v3-validation.json`.

## Final review follow-up

A fresh independent review found that delete consolidation could count a shared Puffin object as removed while unselected blobs still referenced it. The final acceptance check counts only fully retired objects. A real-file integration test reproduces the old two-to-three object regression, rejects partial selection after the fix, and verifies full consolidation preserves rows, lineage and the source watermark. The reviewer rechecked the fix with no further findings. This change follows the frozen benchmark; the measurements above describe the recorded binary, not a performance rerun of the final source.

The final source passes the full workspace test gate, strict workspace Clippy, formatting, 40 Python harness tests, four license-policy tests and Linux x86/ARM notice packaging. Review logs are retained under `target/v3-final-review-*`.

## Artifacts and reproduction

All raw paths below are local, ignored artifacts, not repository downloads. The benchmark option `--format-version 3` creates v3 targets; its default is 2. Run the existing `tests/production/freeze_build.py` tool, then `tests/production/benchmark.py` with the recorded workload arguments. Exact commands for all six runs are in `target/v3-dv-results/matrix.json`. The shared loader supplies the reader workaround.

- Build HEAD: `fcd7f15e8cb90a6261b48458e3d0151e118f1a8e`; dirty source SHA-256: `494f4e50c09fe2b7b719a0208f7f0a9fcf6a3cbf65eb50f5bb20f94ccc73f5cd`.
- Daemon SHA-256: `351a8761c2403bcc02bb2469f8c389d838a5fe999deec4953b820591c2647e4f`.
- External compactor SHA-256: `8386d7eda8ee0155bb85185364be0ba9a03b90cd4bb8c3036c4fdc8ceeb06ca8`.
- Build manifest: `target/qualification-binaries/v3-dv-final/build-manifest.json`.
- Exact Rust source patch/untracked archive: `target/v3-dv-benchmark/source.patch` and `untracked-source.tar.gz`. The subsequent reader-only harness changes and actual settings are recorded in each final report and `target/v3-dv-results/harness.patch`.
- Final matrix and summaries: `target/v3-dv-results/{matrix,summary,paired-readers,mixed-50k-bottleneck}.json`.
- Reproduction, immutable-object audit and Java comparison: `target/v3-dv-benchmark/update90-hot-10k-v3/`; reader root cause: `reader-cache-root-cause.json`.
- Earlier failed attempts: `target/v3-dv-performance/`, `target/v3-dv-performance-final/` and `target/v3-dv-benchmark/`.

| Final run report under `target/v3-dv-results/` | SHA-256 |
| --- | --- |
| `update90-hot-10k-v3/report.json` | `743d27f0a0da8979353a6dfe81bd1105299ca42bc86ea0324c9325164c7a0be2` |
| `update90-hot-10k-v2/report.json` | `36c84ced5a6473a7fe61867e90ce6051d5edda34fcee4cca51c025dcfd45935c` |
| `mixed-10k-v2/report.json` | `2760ab9a7b8fa7b6545411eee52c2706a97e3a5dfd0dca1c05ef84fe8e964118` |
| `mixed-10k-v3/report.json` | `c97b0a663b20d3f178ed24bdbe387250abef18f46b6b37ef7e98ee8da47095fd` |
| `mixed-50k-v3/report.json` | `89615d925c861ddfef7643cf711a0bb40daf5e2fae3e2cfe0c6edf856b01a5e9` |
| `mixed-50k-v2/report.json` | `3313aa6b3d29fd8660d37e01a4cb370533be848957f7b249e6a3b39d31ebbd24` |
