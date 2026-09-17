# Production-oriented local tests

Use the [fault suite](FAULTS.md) for recovery and outage checks, and `benchmark.py` for measured load and latency. These use real PostgreSQL, S3-compatible storage, an Iceberg REST catalog, and stock DuckDB. The native ingestion daemon and optional independent compactor run as separate processes. The benchmark creates its own uniquely named schema, publication, slot, and target namespace; supply disposable services.

This is a reproducible local qualification harness, not a claim that one machine reproduces a cloud region. Docker services, the daemon, the producer, and DuckDB share the host's CPU, disk, network stack, and memory. Run the suites serially, record the host and service resource limits, and repeat representative results before comparing changes.

## Start persistent services

Choose three free loopback ports. If a port is occupied, select another; do not stop the existing process. Save the selected values in an ignored local environment file:

```sh
mkdir -p target/production-local
cat > target/production-local/ports.env <<'EOF'
FLOW_PRODUCTION_PG_PORT=55432
FLOW_PRODUCTION_S3_PORT=59000
FLOW_PRODUCTION_REST_PORT=58181
EOF
docker compose --env-file target/production-local/ports.env \
  -p flow-production-local -f tests/production/compose.yaml up --build -d --wait
```

The fixture keeps PostgreSQL and MinIO data in named volumes, enables PostgreSQL durability, and sets explicit CPU/memory budgets. The REST catalog uses a separate database on the same PostgreSQL server. Its image adds a checksum-pinned PostgreSQL JDBC driver to the Apache fixture. Consequently, a PostgreSQL restart also interrupts catalog storage; this is one machine's recovery test, not independent database failover.

`FLOW_PRODUCTION_PGDATA` defaults to `/var/lib/postgresql/18/docker`, preserving existing PostgreSQL 18 volumes. Older-version fixtures can set it to `/var/lib/postgresql/flow`; keep it outside `/var/lib/postgresql/data`, which PostgreSQL 14–17 images mount as a separate anonymous volume. Use a separate project and volume for each major version; changing PGDATA is not a database upgrade.

Each retained run keeps a replication slot. The default capacity is 32; set
`FLOW_PRODUCTION_REPLICATION_SLOTS=128` in the local environment file when keeping
more runs for inspection, then apply Compose again with all fixture clients
stopped. Restart REST afterward to replace connections to the restarted catalog
database. This preserves volumes and retained slots.

After collecting reports, remove only this disposable project with the same Compose arguments followed by `down --volumes`. This destroys its fixture data; retained files under `target/production-*` remain. The harness itself leaves containers and volumes running for inspection. Measured results and limitations are in [performance results](../../docs/performance.md).

## Check source-generator capacity

Run `source_capacity.py` before interpreting a source-limited ingestion benchmark:

```sh
uv run tests/production/source_capacity.py --writers 4 \
  --artifacts target/source-capacity-4-writers
uv run tests/production/source_capacity.py --writers 8 \
  --artifacts target/source-capacity-8-writers
```

Set `FLOW_POSTGRES_URL` to the disposable PostgreSQL service. Run these serially
with other workloads stopped. Both reuse the exact benchmark seed, mixed writer,
deterministic payloads, bounded SQLite recorder and disk key pools: 100,000 initial
rows, 256-byte payloads, 100 mutations per transaction, ten seconds of warmup and
60 measured seconds at 50,000 offered mutations/s. No daemon, replication slot,
Iceberg catalog, object store or reader connection is created; the source schema
and seed publication remain for inspection.

Reports separate planned, committed, missed and incomplete source arrivals,
retain writer phase timings and evidence hashes, and verify every committed
negative marker plus all surviving row IDs, versions and deterministic payloads.
PostgreSQL identity and durability settings are recorded before and after the
run. Exit status is nonzero for an error, incomplete evidence or a source-capacity
miss. Passing this prerequisite establishes source-generator capacity only; it
makes no ingestion or catalog-latency claim.

## Run a measured workload

Freeze optimized binaries and their build manifest before collecting results:

```sh
python3 tests/production/freeze_build.py \
  --output target/qualification-binaries/current
export AWS_ACCESS_KEY_ID=demo-access
export AWS_SECRET_ACCESS_KEY=demo-secret-key
export AWS_REGION=us-east-1
export FLOW_POSTGRES_URL='postgres://flow:local-test-password@127.0.0.1:55432/flow?sslmode=disable'
uv run tests/production/benchmark.py \
  --catalog-uri http://127.0.0.1:58181 \
  --s3-endpoint http://127.0.0.1:59000 \
  --binary target/qualification-binaries/current/embrasure-flow \
  --build-manifest target/qualification-binaries/current/build-manifest.json \
  --compactor-binary target/qualification-binaries/current/local_compactor \
  --artifacts target/production-1000-001 \
  --rate 1000 --duration 60 --warmup 10
```

The endpoints are examples; configure the actual disposable deployment. Leave PostgreSQL durability enabled: `fsync`, `full_page_writes`, and synchronous commit. The [local Compose fixture](../local/README.md) also retains PostgreSQL durability defaults; use this production fixture for explicit resource budgets and persistent service state. Do not compare release and profiling builds as equivalent results. Every artifact directory must be new. Directory names are labels, not run identity. Each report embeds the build manifest and records its hash, source commit/tree/dirty-diff digest, exact binary hashes by process role, build-time Rust toolchain/profile/target/features/environment, harness/config hashes, run-time toolchain, machine capacity, and compactor topology. The harness rejects a binary that does not match the manifest and rechecks each executable immediately before spawning it. The harness retains logs and durable state and stops only processes it started; it does not delete service data or replication slots.

The default workload seeds 100,000 rows in one table, then uses four connections and 100 mutations per transaction: 70 updates, 20 inserts, and 10 deletes. Each transaction includes one immutable negative-key visibility probe among its 20 inserts, so probes are counted in the requested mutation rate. Writers have disjoint key ownership to avoid artificial row-lock contention. Updates and deletes assert the actual affected row counts. The complete final `(id, version, md5(payload))` stream must match PostgreSQL in sorted order, including probes.

Parameters expose these workload dimensions:

| Dimension | Options |
| --- | --- |
| Total offered mutation rate | `--rate 1000`, `10000`, `50000` |
| Table fan-out | `--tables 1`, `10`, `100`; rate is aggregate across all tables |
| Initial size | `--initial-rows 100000` per table |
| Payload width | `--row-bytes 256`, `1024`, `16384` |
| Payload entropy | `--payload-entropy repeated` or `high` |
| Mutation mix | `--profile mixed`, `update90` (90% updates, 10% inserts), `append` |
| Key selection | `--key-distribution uniform`, `hot` (first 1% of each writer's active keys), or `zipf --zipf-exponent 0.99` |
| Transaction size | `--transaction-rows 10`, `100`, `1000`, `10000`; append-only also permits one-row transactions |
| Writer concurrency | `--writers 4` |
| Daemon table concurrency | `--table-workers 4`; recorded independently from source writers |
| Runtime batch and queue budgets | `--batch-rows 1024 --pending-transactions 256`; both are recorded in each report |
| Timing | `--warmup 10 --duration 60 --timeout 180` |
| Reader polling | `--reader-poll-seconds 1` |

Start at 1,000 mutations/s, then increase to 10,000 and 50,000. The baseline target is **50,000 mixed mutations/s with catalog p95 below 1 second and p99 below 2 seconds**. A lower-rate pass does not satisfy that throughput target. Repeat the rate sweep for ten tables, the 90% update workload, append-only ingestion, and wider rows. Keep the seed and all other parameters fixed when comparing builds. Longer runs are needed to establish steady-state compaction and metadata growth.

`matrix.py` runs thirteen cases serially: the three mixed rates, append-only, ten tables, hot updates, 16 KiB values, one-row transactions, an independent compactor, and the four additional dimensions below. Adding a case makes it available for qualification; it does not establish a performance result. It retains every failed qualification and continues after performance-only failures. Incomplete correctness stops the matrix so recovery can be resolved before another workload. Run it only when no fault injection or build is using the fixture:

| Added case | Parameters |
| --- | --- |
| `one-kib-10k` | 1 KiB payloads, 10,000 mutations/s |
| `hundred-tables-10k` | 100 tables, 10,000 initial rows each, 10,000 aggregate mutations/s |
| `large-tx-10k` | 10,000 mutations per transaction, 10,000 mutations/s |
| `zipf-updates-10k` | 90% updates, finite Zipf exponent 0.99, 10,000 mutations/s |

```sh
python3 tests/production/matrix.py \
  --catalog-uri http://127.0.0.1:58181 \
  --s3-endpoint http://127.0.0.1:59000 \
  --binary target/qualification-binaries/current/embrasure-flow \
  --build-manifest target/qualification-binaries/current/build-manifest.json \
  --compactor-binary target/qualification-binaries/current/local_compactor \
  --artifacts target/production-matrix
```

Use repeated `--case` options to select a subset and `--duration`/`--warmup` to change timing. Each case has its own report; `matrix.json` records every exit code and failure. The 16 KiB and hundred-table cases start with 10,000 rows per table to bound the fixture's data volume; other cases start with 100,000 rows per table. The separate compactor runs every ten seconds in its case. This bounded matrix is a regression and capacity probe, not 24-hour endurance qualification.

Before changing maintenance architecture, run the controlled A/B/C/D comparison at 1,000 and 10,000 mutations/s:

```sh
python3 tests/production/maintenance_matrix.py \
  --catalog-uri http://127.0.0.1:58181 \
  --s3-endpoint http://127.0.0.1:59000 \
  --binary target/qualification-binaries/current/embrasure-flow \
  --build-manifest target/qualification-binaries/current/build-manifest.json \
  --reader-baseline-compactor target/qualification-binaries/current/local_compactor \
  --artifacts target/production-maintenance-matrix-01
```

The four modes are a finite diagnostic control, delete-file consolidation without data rewrites, bounded L0-to-L1 binpacking, and the current all-level rewrite policy. The wrapper holds manifest rewriting off equally across the cases. The first two modes explicitly raise L0 debt limits above the bounded run envelope; the first three raise only the independent stable-small-file limits, leaving C's real L0 limits active. The disabled mode also raises delete-debt limits and removes the daemon compactor role. These settings isolate maintenance costs and must not be used as production defaults. Each result retains catalog latency, drain throughput, planned versus recorded arrivals, file and delete counts/bytes/rows, average data-file size, delete ratios, and the same-final-state reader result.

The default matrix admits at most 16,384 files and 512 MiB to its final external rewrite, with a separate 900-second deadline. The wrapper rejects a custom duration whose conservative file bound exceeds that cap; increase `--baseline-max-input-files` explicitly when extending the run. These larger limits apply only after measured ingestion has stopped and are recorded in every report. Baseline readiness polls catalog heads and reads manifests initially, after a non-fixture head change, and once after the compactor stops, avoiding repeated full inventory scans while thousands of small files are present.

For CPU and allocation capture, freeze a separate profiling build. It inherits release optimization and adds debug information plus forced frame pointers:

```sh
python3 tests/production/freeze_build.py \
  --profile profiling \
  --output target/qualification-binaries/profiling-001
```

Zipf selection follows the [finite rank probability law](https://commons.apache.org/proper/commons-statistics/commons-statistics-distribution/apidocs/org/apache/commons/statistics/distribution/ZipfDistribution.html), with weights `r**(-s)` for ranks `1..N`. The default exponent 0.99 matches [YCSB's default skew](https://github.com/brianfrankcooper/YCSB/blob/master/core/src/main/java/site/ycsb/generator/ZipfianGenerator.java). Draws are successive and without replacement within a transaction, so updates and deletes use distinct keys. Ranks refer to each writer/table's current active-key vector: deletes swap the last key into the removed position, and new keys append. Popularity therefore follows those changing ranks. Writers retain their existing disjoint ownership, and probes remain immutable.

The sampler uses power-of-two rejection buckets rather than a weight per active key, requiring `O(log N + k)` extra memory for `k` selected keys. Exponents are limited to `(0, 2]`; bucket correction accepts at least one quarter of proposals before duplicate rejection. Selecting many distinct keys under strong skew can still require many retries. Each transaction permits at most `max(1024, 128*k)` proposals; exhausting that limit fails the run without substituting uniform draws. Sampler CPU contributes to open-loop admission delay. Zipf reports include the exponent, rank definition, bound and reproducibility contract; the same seed and admitted transaction sequence reproduce the same choices.

Default payloads are distinct per row and generation, generated from repeated MD5 text. They are deliberately reproducible and **compressible**. `--payload-entropy high` generates a different MD5 block for every 32 characters, removing the repeated-block compression shortcut. This remains hexadecimal text, with at most four bits of entropy per byte; PostgreSQL's payload-generation CPU cost is included in achieved source throughput. A 16 KiB payload is not equivalent to 16 KiB of incompressible object-store traffic. These results cannot qualify random-byte throughput, very large transactions, partition fan-out, or shared-key writer contention. Seeding 100,000 rows per table on ten tables means a million initial rows.

Use `--format-version 3` to create v3 benchmark tables with deletion vectors;
the default is 2. The harness checks the catalog's actual format before starting
timed work and reports logical vectors, physical Puffin objects and blob bytes.

## Measurement contract

Transactions arrive on a predetermined open-loop schedule. Each writer owns every fourth arrival by default and does not shift future arrivals when PostgreSQL is slow. An arrival more than 250 ms late is rejected and counted in `missed_scheduled_arrivals`; no silent catch-up loop turns a saturation test into a lower-rate success. The legacy top-level `scheduled_transactions`, `committed_transactions`, and `missed_scheduled_arrivals` fields describe recorded measured-window arrivals only. `arrival_cohorts` distinguishes nominally planned, recorded, committed, missed, and unrecorded counts for warmup, the measured window, and the complete run. Qualification uses the measured cohort; warmup misses remain visible. `source_commit_window_mutations_per_second` counts actual PostgreSQL commit responses inside the measurement window, independently of the scheduled cohort; it is a source input rate, not proof of Iceberg's capacity. `catalog_completion_delay_after_measurement_seconds` reports the tail after the measurement window, and `measured_cohort_catalog_mutations_per_second_including_drain` divides the measured cohort's mutations by the window plus that tail. The latter two fields are null if timing observations are incomplete. Late source completions and the scheduled-cohort rate are reported separately. Warmup transactions are excluded from latency percentiles.

The daemon emits structured events at durable journal append and after the catalog acknowledges a table publication. XID joins the source commit to its journal record; table OID and the closed interval of source end LSNs join it to the containing Iceberg publication. Each benchmark transaction affects exactly one target table. Negative-key probes survive subsequent mutations, so every committed transaction can be observed independently by DuckDB. The harness polls fresh catalog metadata and queries the exact returned Iceberg metadata location, rather than substituting direct Parquet reads.

`report.json` includes p50, p95, p99, maximum, and observation counts for:

- PostgreSQL commit to journal durability, both raw and with clock correction.
- Journal durability to catalog response, and PostgreSQL commit to catalog response.
- Catalog response to DuckDB query completion, and PostgreSQL commit to DuckDB query completion.
- Scheduled arrival to DuckDB completion, including producer admission delay.
- Client transaction start to commit response.
- Writer admission lag, key preparation, evidence-queue insertion, PostgreSQL transaction, and active-key file maintenance.

Five PostgreSQL clock queries before the writers start and after the observation drain use the minimum-roundtrip midpoint to estimate host/container clock offset. The average offset corrects durations starting on the PostgreSQL clock. Both samples, roundtrip uncertainty, and drift are retained. Negative durations are not silently clamped. A negative catalog-response-to-reader value can occur because a reader sees a committed snapshot before the writer receives its REST response; response time is an acknowledgement bound, not the catalog's internal storage timestamp.

Reader visibility is an **observed upper bound**, quantized by polling cadence, pending-probe paging and query time. At the default one-second polling interval, it is not appropriate to interpret subsecond differences as exact reader visibility latency. The `disk-evidence-paged-probes-v3` protocol queries at most 1,024 outstanding immutable marker IDs per table and poll, using the exact returned catalog metadata location. Each paging round freezes its highest sequence, so continued arrivals cannot starve older holes. Only returned markers receive a first-observation timestamp, captured after query completion; an unchanged snapshot can be queried again for pending probes. Query time includes Iceberg planning and scan execution; the REST metadata request is measured separately, not mislabeled as complete query planning time.

Missing journal, catalog, or reader observations remain explicit failures. Requested-arrival percentiles include failed or unobserved requests as a failure tail rather than replacing them with invented timeout measurements. After drain, the harness waits for a fresh exact status observation with no pending work and equal received, journal-durable, captured-durable, and materialized LSNs. It then requires PostgreSQL's server-side `confirmed_flush_lsn` to equal that materialized watermark and retains both text and unsigned integer forms in `post-drain-source-acknowledgement`. Qualification requires at least one measured commit, all commits observed, no missed scheduled arrivals, at least 95% of the requested commit-window rate, and the stated catalog latency SLO. A correctness pass and a performance qualification are separate report fields; the process exits unsuccessfully when either fails. This makes saturation visible rather than reporting only the fast completed subset.

Long-run evidence uses the standard-library [disk store](benchmark_store.py), with a 256-item recorder queue, batched SQLite writes, two 16 MiB SQLite page caches, disabled SQLite mmap and file-backed temporary sorts. A full queue blocks producers and recorder errors fail the workload; observations are never dropped. The recorder reports exact insertion wait under the queue mutex, queue pressure and each writer phase. `previous_postcommit_keys_micros` is the preceding active-key file update that delayed the recorded arrival, so the first measured arrival can include warmup work. The final update from each writer is reported separately and must not be combined with that arrival cohort as a per-transaction population. Performance comparisons require the same recorder protocol and instrumentation because both contribute overhead. Active positive keys occupy eight-byte disk slots per writer/table, preserving the same sampled ranks, RNG calls and swap-last deletion order. Initial key-file creation happens before the arrival clock starts. Shutdown interrupts arrival waits, requests cancellation on owned PostgreSQL connections and joins the actual writers before closing their files. A cancellation request does not imply an in-flight connection has finished.

After the workload, daemon logs are streamed once into indexed SQLite tables; source end LSNs use unsigned big-endian keys. Identical repeated events are idempotent, while conflicting events, ambiguous wire-XID reuse and overlapping publication intervals fail attribution. Exact nearest-rank percentiles use disk sorts, including signed values and the failed-request tail. The same `transactions.json`, `resources.json` and `reader-samples.json` arrays are exported incrementally; `evidence.sqlite3` also permits cursor-based consumers without loading those arrays. Missing warmup probe observations remain a correctness failure, although warmup latencies remain outside the measured cohort.

Full-row verification retains the exact metadata pin and preserves the CSV on failure. It uses a named PostgreSQL cursor in a read-only transaction and a sorted DuckDB CSV disk sink, comparing 4,096 rows at a time with the existing row-hash contract. DuckDB has an explicit 1 GiB managed-memory limit and a per-run spill directory; inventory manifests are streamed through bounded spools. These are bounds on harness collections and managed spill budgets, **not** a strict RSS bound on native extensions or catalog metadata. Disk usage grows with active keys, transactions, logs and exact-sort scratch space; a long run needs sufficient local disk. This implementation does not itself qualify a 24-hour workload.

The focused storage gate uses actual files and SQLite, without services:

```sh
python3 -m unittest discover -s tests/production -p test_benchmark_store.py -v
```

## Retained evidence

Each run saves `report.json`, transaction timing records, reader samples, periodic source watermark and retained-WAL samples, the exact post-drain PostgreSQL acknowledgement, per-process RSS/CPU readings, initial-copy logs and metrics, daemon logs, compactor logs, and local state. A resource summary records sampled reader-debt peaks and process counters through drain. Counters include warmup and drain; one-second sampling can miss brief peaks. Initial COPY metrics are archived before the daemon replaces its process metrics. Final inventory includes live data/delete files, manifests, retained snapshots, and compaction counts. Source/target hashes are for diagnosis; matching ordered row streams is the correctness assertion. `ps` CPU values are OS-reported process utilization samples, not a precise per-phase CPU accounting model.

The initial reader baseline and final reader timings use different data populations. To compare the **same final dataset**, pass the frozen `local_compactor` beside the required `build-manifest.json`, for example `--reader-baseline-compactor target/qualification-binaries/current/local_compactor`. After the workload drains and its scan is recorded, the harness cleanly stops any workload compactor, starts the selected baseline compactor, and waits for an external rewrite without positional deletes or an already compact one-file table. It pins that snapshot and stops the compactor, then keeps the daemon alive until an exact table-and-snapshot event confirms durable row-index reconciliation for each newly external head. Already compact heads are reported separately. Full source/target verification follows before the daemon stops and the pinned reader layouts are measured. Shared services remain running.

The shared [reader comparison](reader_compare.py) explicitly warms both pinned layouts twice, then measures twenty scans per layout in alternating original/compacted and compacted/original pairs. Before those scans, the report saves a `verified_reader_baseline` checkpoint with exact compacted metadata locations, full-row counts and hashes, stopped-process IDs and exit codes, and the joined-worker panic check. A reader failure therefore preserves verified inputs for a separate repeat. Completed comparisons also retain every warmup and measured sample and the ratio of the two median scan times. Every query checks matching aggregates. The 1.25 ratio limit is unchanged. The fixture defaults to explicit 512 MiB compressed-input and 256-file maintenance limits and fails when the selected recorded budgets are exceeded.

The shared reader loader disables `enable_external_file_cache` on the pinned
DuckDB 1.5.5 reader. Iceberg extension `45163a28` uses the wrong buffer offset
for cached shared-Puffin ranges and can return incorrect rows. The
[v3 benchmark record](../../docs/benchmarks/v3-deletion-vectors.md)
describes the reader workaround and upstream fix. All v2/v3 comparisons use the same workaround and record the
actual setting. Re-enable it only with a validated fixed reader.

Local runs and standalone comparisons explicitly enable stock DuckDB
`httpfs_connection_caching=true` to reuse object-store connections. The
`reader_settings` field records its actual value, backend selection, keepalive,
retries, backoff, retry wait, timeout and metadata-cache setting. Reader errors
fail the run. Account for the recorded transport settings when comparing reports.

To repeat only the reader comparison on retained layouts, use the current AWS credentials and S3 endpoint:

```sh
uv run tests/production/reader_compare.py \
  --s3-endpoint http://127.0.0.1:59000 \
  --report target/production-matrix/mixed-50k/report.json \
  --report target/production-matrix/ten-tables-10k/report.json \
  --output target/production-reader-repeat-new/report.json
```

Run this command while workload activity is stopped. It starts or stops no writer processes and requires no PostgreSQL or catalog connection. Each input must retain both metadata locations and matching final-row verification, either in a completed comparison or its saved baseline checkpoint. The separate output records the input report's hash, its original binary path/hash, the current DuckDB/extension versions, actual reader settings and all scan samples. The output file must be new; original reports and their failures remain unchanged. A missing retained object or a ratio above 1.25 fails the repeat.

Without that option, the initial and final reader timings **do not establish the same-state full-scan slowdown target**. Even with it, combined planning-plus-scan timings do not independently qualify the planning-only target. The live harness does not assign cloud prices or infer dollars per million mutations from local CPU time. The separate offline cost report below requires explicitly scoped measurements and supplied prices.

The option also works with `matrix.py`. When supplied, `reader_scan_qualified`
requires every table's measured slowdown to be at most 25%; failure makes the
case fail even when catalog latency passes. Without it, this field is null and
the performance result covers source load and catalog latency only.

### Optional planning and selective-filter diagnostics

The same CLI has a separate diagnostic mode for one verified table and an
explicit signed-integer `id` range. Run it only after all writers and other
timing jobs have stopped. It does not modify the retained report, metadata,
default comparison, or workload protocol.

```sh
uv run tests/production/reader_compare.py \
  --s3-endpoint http://127.0.0.1:59000 \
  --report target/production-matrix/mixed-50k/report.json \
  --output target/reader-diagnostics-new/report.json \
  --diagnostics-dir target/reader-diagnostics-new/profiles \
  --table events_0 --id-range 0 1023 \
  --quiescence-note 'Owned daemon and compactor joined; no other timing jobs active'
```

The output file and profile directory must be new. The diagnostic uses a 1 GiB
DuckDB managed-memory limit and at most 4 GiB of spill in its own directory.
It records effective limits, threads, engine/extension versions, transport
settings, input/script hashes and the exact pre/post-compaction metadata pins.
These limits do not bound native extension RSS. Credentials come from the same
AWS environment variables as the default comparison.

Both queries aggregate `count(*)`, `sum(version)` and `sum(length(payload))`;
the selective query adds `WHERE id BETWEEN ? AND ?`. The range must return
some, but not all, rows. All observations must match between the verified
layouts. First, each query runs two warmup pairs and twenty alternating
unprofiled pairs. Only after those wall-time measurements finish does each
query run two further warmup pairs and twenty detailed-profile pairs. Raw JSON,
every sample, failed attempts, aggregates and actual selectivity are retained.
Profiled timings include instrumentation overhead and are not substituted for
unprofiled query wall time.

For DuckDB 1.5.5, **planning** here means the sum of the disjoint `planner`,
`all_optimizers` and `physical_planner` phases for each profiled query. The gate
requires the original layout's median to be at most twice the compacted
layout's median for both queries. It excludes parsing and other client overhead;
the [pinned execution source](https://github.com/duckdb/duckdb/blob/v1.5.5/src/main/client_context.cpp#L370-L421)
defines these boundaries. Binding and individual optimizer fields remain
separate nested observations because the
[profiler accumulates their time into parent phases](https://github.com/duckdb/duckdb/blob/v1.5.5/src/main/query_profiler.cpp#L273-L302).

Diagnostic success covers aggregate equivalence and this planning gate only.
Full and selective wall-time ratios are reported without affecting that gate;
the default mode's 1.25 full-scan limit remains unchanged. There is no invented
selective-filter threshold. Parallel operator timings can exceed query wall
time, as the [DuckDB profiling guide](https://duckdb.org/docs/current/guides/meta/explain_analyze)
explains. They are not pure physical-scan, CPU or network measurements, and
profile byte/row counters do not replace an actual file census. This mode does
not qualify first-observation latency, other readers, or overall readiness.

## PostgreSQL replica-identity WAL overhead

`wal_overhead.py` measures generated PostgreSQL WAL for `REPLICA IDENTITY FULL`
versus `DEFAULT`, using a primary key in both cases. It creates its own disposable
PostgreSQL 18.6 container with a random free loopback port and fresh volume. It
does not connect to the shared source/catalog cluster or start the daemon.
Run it alone after other measured workloads stop:

```sh
uv run tests/production/wal_overhead.py \
  --artifacts target/production-wal-overhead-01
```

The finite workload has three alternating FULL/DEFAULT pairs for each of two
payload widths: 256 bytes and 16 KiB. Each case starts from the same 1,024 rows
and runs two ordered phases of twenty 100-mutation transactions. Transactions
contain 70 updates (ten move primary keys), 20 inserts and ten deletes. Ordinary
updates replace the payload; wide updates leave it untouched. Deterministic
SHAKE-generated hexadecimal payloads and observed external TOAST chunks make the
wide case an unchanged-TOAST workload, not an estimate based on configured width.
Every statement checks its affected-row count; each committed phase compares
all actual row values with an independent model. Pairing also checks operation
digests, counts and final-row digests.

Both identities use `wal_level=logical`, synchronous commits, `fsync` and
`full_page_writes`; WAL compression is off. A checkpoint precedes the first
phase; the second phase has no new checkpoint. Actual FPI counts are reported in
both, because the second phase can still allocate or first-touch pages. Autovacuum
is disabled only in this isolated finite fixture, and unexpected checkpoint
activity invalidates a phase. This does not measure long-run vacuum cost.

The primary byte measurement is the delta of `pg_stat_wal.wal_bytes`, alongside
record and FPI counts. Separate insertion/write/flush LSN distances are retained;
they are WAL address-space distances, not interchangeable with record-byte
counters, retained WAL or journal payload bytes. Writers close before boundary
sampling. Exact table mutation counters and stable WAL/checkpoint snapshots over
more than one second fence delayed statistics; timeout, counter reset, an extra
client, mismatched rows or an intervening checkpoint fails the run.

Each new artifact directory retains raw boundary samples, ordered operation
plans, script/image identities, PostgreSQL settings, per-pair byte ratios and
ranges, server logs and failures. A failed run also attempts to dump its current
source table before cleanup. Only the container/volume created by the run
is removed. `passed` means the finite measurement and row checks completed; there
is no overhead threshold. This measures neither end-to-end ingestion performance
nor the WAL overhead of another workload.

Measurement semantics follow PostgreSQL's documentation for
[WAL and full-page writes](https://www.postgresql.org/docs/18/runtime-config-wal.html),
[cluster statistics and their delayed publication](https://www.postgresql.org/docs/18/monitoring-stats.html),
[WAL positions](https://www.postgresql.org/docs/18/functions-admin.html#FUNCTIONS-ADMIN-BACKUP)
and [checkpoint control data](https://www.postgresql.org/docs/18/functions-info.html#FUNCTIONS-CONTROLDATA).

## Offline cost report

`cost_report.py` uses only the Python standard library and reads explicit local
measurements, prices and evidence files. It does not query services, extract
unmeasured quantities from benchmark reports, or change performance results.

```sh
python3 tests/production/cost_report.py \
  --measurements target/cost-input/measurements.json \
  --prices target/cost-input/prices.json \
  --output target/cost-input/report.json
python3 -m unittest discover -s tests/production -p test_cost_report.py -v
```

The output must be new; parent directories must already exist. All quantities
must cover the same timezone-aware UTC window. Counts and bytes are nonnegative
JSON integers; durations, usage and prices are finite nonnegative numbers. Every
measured quantity includes its unit, window, measurement basis and a local
evidence path, resolved relative to the measurements file. Inputs and evidence
are hashed and the declared measurements and prices are retained in the output.
Evidence files are not interpreted or certified by this tool.

The six outputs are USD per million mutations, per source GiB and per active-table
hour; provider object-store requests and catalog commits per million mutations;
and compaction bytes per source byte. Unknown quantities or zero denominators
produce `null` with a reason. `cost_items` must list **every item in the declared
accounting scope**, including unknown items as `null`. Missing usage or prices
leaves `total_usd_within_declared_scope` and its normalized dollar outputs null;
`known_subtotal_usd` remains explicitly partial. Even a complete declared scope
does not imply a complete provider bill.

Price keys match cost-item names, with `usd_per_unit` and the exact usage unit.
Supported cost units are `requests`, `hours`, `vCPU-hours`, `GiB-hours`, `GiB`,
`bytes` and `seconds`; supplied prices have no built-in cloud rates. Supply
provider request logs for `object_store_requests`. Optional `fileio_calls` uses
unit `calls`, appears only as a separate diagnostic, and cannot be priced as a
provider request. `source_bytes` must name its measured logical or transport byte
basis, and `compaction_bytes` must specify physical input/output coverage and
whether failed attempts are included. Active-table time is the sum of measured
active intervals across tables, not automatically table count times the requested
benchmark duration.

This is an **illustrative partial input, not a measurement or price quote**.
For a dry run, save it as `measurements.json` and place a text file stating that
the values are illustrative at `evidence.txt` beside it:

```json
{
  "accounting_scope": "ILLUSTRATIVE ONLY: daemon compute and provider requests. Source database, catalog, retained storage, network and readers excluded.",
  "window": ["2026-09-04T00:00:00Z", "2026-09-04T01:00:00Z"],
  "quantities": {
    "committed_mutations": {
      "value": 2000000, "unit": "mutations",
      "window": ["2026-09-04T00:00:00Z", "2026-09-04T01:00:00Z"],
      "evidence": "evidence.txt", "basis": "illustrative committed mutation count"
    },
    "source_bytes": null,
    "active_table_seconds": null,
    "object_store_requests": null,
    "catalog_commits": null,
    "compaction_bytes": null
  },
  "cost_items": {
    "daemon": {
      "value": 1, "unit": "hours",
      "window": ["2026-09-04T00:00:00Z", "2026-09-04T01:00:00Z"],
      "evidence": "evidence.txt", "basis": "illustrative allocated instance-hour"
    },
    "provider_requests": null
  }
}
```

An illustrative `prices.json` is
`{"daemon":{"usd_per_unit":1,"unit":"hours"}}`. Its known subtotal is USD 1;
its total remains unknown because provider usage and pricing are missing. Real
reports require actual evidence. Existing benchmark counters cover startup,
warmup and drain and may precede quiescence: do not divide them by measured-only
mutations. Retained WAL, LSN distance, configured payload width and sampled CPU
percent are not substitutes for source bytes, billed requests or allocated
resource-hours.

## Fixture compactor shutdown

The fixture compactor must retain SIGINT while it finishes an in-flight table
pass. `compactor_shutdown.py` holds the second real catalog read after a completed
pass and interval wait, sends SIGINT, then releases the read. The previous binary
must demonstrate the lost signal by continuing another pass; the fixed binary
must finish and exit 0. Both process results, binary hashes and proxy requests are
retained. The negative-control process is killed only during owned cleanup.

```sh
python3 tests/production/compactor_shutdown.py \
  --config target/production-matrix/hundred-tables-10k/flow.toml \
  --catalog-uri http://127.0.0.1:58181 \
  --previous-binary target/qualification-binaries/baseline/local_compactor \
  --binary target/qualification-binaries/candidate/local_compactor \
  --artifacts target/production-compactor-shutdown-new
```

Use the AWS environment variables above and a retained benchmark configuration
whose first table already has one data file and no deletes. The command restricts
the copied configuration to that table and requires catalog reads only. It uses a
free proxy port and does not start ingestion or replace local state.

## Counted journal and ledger upgrade

`journal_upgrade.py` runs COPY and CRUD with a legacy-format baseline binary, holds an actual catalog POST
after a multi-transaction epoch reaches Prepared, and retains additional
two-table transactions in the journal and source ledger before SIGKILL. It
starts the candidate on the same configuration, slot, control database and row index,
then checks recovery of the original Prepared operation and exact current and
retained historical rows without an index rebuild or another COPY. Subsequent
checks cover a counted insert/delete no-op without an empty snapshot, a streamed
14,008-mutation transaction with exactly one snapshot per table, WAL queued across
restart, ACK progress and stopped worker panic checks.

```sh
cargo build -p flow-testkit --example inspect_legacy_ledger
uv run tests/production/journal_upgrade.py \
  --catalog-uri http://127.0.0.1:58181 \
  --s3-endpoint http://127.0.0.1:59000 \
  --previous-binary target/qualification-binaries/baseline/embrasure-flow \
  --binary target/qualification-binaries/candidate/embrasure-flow \
  --ledger-inspector target/debug/examples/inspect_legacy_ledger \
  --artifacts target/production-journal-upgrade-new
```

Use the PostgreSQL and AWS environment variables above. The new artifact directory
retains both binary hashes, the original state identities, exact journal end LSNs,
held ACK/frontier evidence and proxy requests. This functional test records balanced
priority, ingest/coordinator roles and one-hour/two-hour L0 age limits to isolate
format recovery while compaction is disabled; it makes no latency claim. It uses a
free loopback proxy port and stops only its own processes, leaving service data
and the original replication slot intact. After joining the old binary, the gate
checks journal frame headers and CRCs for each retained backlog transaction and
requires terminal kind 4. Supply a baseline built before counted journal and
ledger formats were introduced; a current build cannot serve as that baseline.
The offline inspector opens a copy of the control store,
decodes the matching ledger identities, and requires FLLEDG02 entries. Observed
formats are retained in the report; two distinct current-format binaries fail this
legacy precondition. The original recovery state is never rewritten by inspection.

## Health and scheduler liveness

`health.py` uses the same services and credentials. Its private loopback proxies
withhold only the PostgreSQL WAL-health query and hold catalog commits long enough
to check `table_workers = 1`. It verifies two-table CRUD and primary-key moves
reach Iceberg while health is stalled, checks the five-second query timeout,
checkpoint creation, absence of overlapping health connections, and SIGTERM
cancellation with stopped readiness. The PostgreSQL relay uses plaintext only for
this disposable local fixture. Run it separately from performance measurements:

```sh
uv run tests/production/health.py \
  --catalog-uri http://127.0.0.1:58181 \
  --s3-endpoint http://127.0.0.1:59000 \
  --binary target/release/embrasure-flow \
  --artifacts target/production-health-new
```

Set `FLOW_POSTGRES_URL` and the AWS environment variables as above, use a new
artifact directory, and keep the binary unchanged during the run. The harness
uses free proxy ports and stops only its own processes; it never restarts services.

## CDC priority and maintenance recovery

`maintenance_priority.py` uses the same local services and credentials. It first
journals due CDC with compaction disabled, then restarts with soft debt and an
active producer. A private proxy holds an actual orders compaction POST while the
test checks that the preceding source transaction is visible and acknowledged,
and that optional maintenance receives an opportunity while CDC remains queued.
The daemon exits on SIGTERM before the proxy forwards that POST. Its successful
catalog outcome is therefore unknown locally; restart must recover it and retain
exact current and historical rows. A final hard-debt phase holds compaction again
and verifies that both publication and source ACK remain blocked until release.

```sh
uv run tests/production/maintenance_priority.py \
  --catalog-uri http://127.0.0.1:58181 \
  --s3-endpoint http://127.0.0.1:59000 \
  --binary target/release/embrasure-flow \
  --artifacts target/production-maintenance-priority-new
```

Use an immutable binary and fresh artifact directory. Run separately from
performance measurements; the harness uses free proxy ports and stops its own
daemon, proxy and producer without restarting services.

## CDC during a compaction build

`concurrent_compaction.py` holds an exact selected Parquet GET after the worker
captures its index snapshot. With the default four table workers and 10s/30s
soft/hard debt limits, actual updates, deletes, key moves and a delete/reinsert
must reach the catalog and source ACK while the soft-pressure read remains held.
After release, the test checks exact rows through DuckDB, retained history, eight
translated position deletes, and their sequence relationship to rewritten data.
A separate phase reaches hard debt and verifies publication and ACK pause until
the active build completes. It delays real catalog requests by 1.25 seconds and
requires the same preparation to survive the optional 750 ms budget, finish
within the original build-age limit, and commit without rebuilding its inputs.
The deadline phase holds a selected GET beyond the
unchanged 30-second build limit. It requires a cancellation-request event with
verified durable BUILD ownership, keeps the request and scratch held without a
retirement event, then releases the GET and requires actual join, durable BUILD
retirement and scratch removal. The same daemon must resume CDC and ACK, preserve
exact current and historical rows, and never publish the cancelled operation.
This phase requires a binary containing the cancellation and retirement events.
SIGTERM must exit with the read still held; restart
must retire the durable abandoned build, remove its scratch directory and recover
exact rows. The private S3 proxy preserves signed Host, path, headers and streamed
request bodies, and selects its own free loopback port.

```sh
uv run tests/production/concurrent_compaction.py \
  --catalog-uri http://127.0.0.1:58181 \
  --s3-endpoint http://127.0.0.1:59000 \
  --binary target/release/embrasure-flow \
  --artifacts target/production-concurrent-compaction-new
```

Use the AWS and PostgreSQL environment variables above and a stable binary. Run
serially with other fault and performance suites. The report keeps every phase,
binary digest and proxy error; the JSONL traces also retain ordinary HEAD 404
checks for not-yet-created objects.

## Composite keys and capture capacity

`boundaries.py` covers two service boundaries without restarting shared services:

```sh
uv run tests/production/boundaries.py \
  --postgres-url "$FLOW_POSTGRES_URL" \
  --catalog-uri http://127.0.0.1:58181 \
  --s3-endpoint http://127.0.0.1:59000 \
  --binary target/qualification-binaries/current/embrasure-flow \
  --artifacts target/production-boundaries-new
```

It starts from a composite `(id, tenant)` primary key, inserts rows sharing `id`,
changes each key component, and exercises delete/reinsert plus negative and large
signed components. A real catalog POST is held while transactions become durable;
the owned daemon is killed and only its replaceable row index is moved aside.
Recovery must preserve the source slot/control identity, reconstruct the index,
resolve the pending publication exactly once, and support subsequent key changes.
Stock DuckDB checks full current rows and retained historical snapshots.

The capacity phase uses a 64 MiB journal quota, the minimum compatible with the
existing segment size. It holds ingestion POSTs and commits at most ten batches
of 512 rows with 16 KiB payloads. The test requires the actual encoded-byte quota
error, an unacknowledged source suffix, and unchanged public rows while held.
It then releases publication, restores this fixture's ordinary 256 MiB quota,
and recovers the same slot/state, including committed rows that were not yet
journaled. This is configured-capacity exhaustion, not a disk-full simulation or
a performance test. The old quota config, daemon/proxy logs, committed cohort,
ACK evidence and failures are retained. Only owned processes are stopped; ports
are allocated through the existing proxy helper.

## Maintenance fairness under queued CDC

`maintenance_fairness.py` queues 1,025 source transactions while the daemon is
stopped, then primes a durable backlog behind a held ingest commit. With native
compaction enabled on restart, every 16-transaction publication page is already
old enough to be immediately scheduled. The default workload keeps orders hot;
`--two-hot-tables` also updates accounts on every commit to cover worker contention.
A 10,000 commits/s diagnostic limit avoids artificial permit gaps. A background build must start before
the backlog drains, and publication and source ACK must advance while an actual
input Parquet read is held. DuckDB verifies current and retained historical rows;
the owned daemon must exit cleanly.

```sh
uv run tests/production/maintenance_fairness.py \
  --postgres-url "$FLOW_POSTGRES_URL" \
  --catalog-uri http://127.0.0.1:58181 \
  --s3-endpoint http://127.0.0.1:59000 \
  --binary target/qualification-binaries/current/embrasure-flow \
  --artifacts target/production-maintenance-fairness-new
```

This finite scheduling regression records its diagnostic policy: soft L0 count
2, hard count 128, raised delete/stable-file limits and two workers.
The bounded workload fits below those hard limits. Reaching hard pressure or
starting a build only after draining the backlog fails the test; neither is
accepted as evidence of fair admission. These settings are not proposed defaults
or a throughput qualification.

## Two-table background compaction

`parallel_compaction.py` holds actual Parquet reads from two distinct table builds
at once. It verifies CDC and ACK progress, translated position deletes and exact
current/history rows, ownership through the real 30-second cancellation deadline,
and recovery of both abandoned builds after SIGTERM. It also checks that a table
cannot admit another build while its worker remains held.

```sh
uv run tests/production/parallel_compaction.py \
  --catalog-uri "$FLOW_REST_URL" --s3-endpoint "$FLOW_S3_URL" \
  --binary target/debug/embrasure-flow --artifacts target/service-parallel-compaction
```

The fixture uses four table workers and soft-only input debt. A binary limited
to one background build cannot satisfy the second held-read assertion within
15 seconds.

## Periodic maintenance during queued CDC

`periodic_maintenance.py` reuses the durable-backlog fixture to require a manifest
rewrite, snapshot expiration, and deletion of an owned obsolete manifest list
before the table's own source backlog drains. A standard Iceberg tag preserves
an independent historical reader. The test also checks checkpoint rotation,
exact final updates/deletes/key moves, ACK, and clean shutdown.

```sh
uv run tests/production/periodic_maintenance.py \
  --catalog-uri "$FLOW_REST_URL" --s3-endpoint "$FLOW_S3_URL" \
  --binary target/debug/embrasure-flow --artifacts target/service-periodic
```

Use `--two-hot-tables --table-workers 1 --timeout 600 --drain-timeout 900` for a shared single worker, or
`--two-hot-tables --timeout 600` to include competing tables and native background builds.
Native data rewrites remain enabled so expired file-birth snapshots cannot leave
unclearable hard L0 age debt. The four-transaction pages, short retention/cleanup
intervals and 10 ms catalog request delay isolate scheduling and reclamation. These are diagnostic settings,
not production defaults or a throughput benchmark. `--timeout` still bounds the
fairness and reclamation checks. `--drain-timeout` independently bounds the final
backlog drain, which must still reach exact source/target equality and ACK.

The fault proxies reuse HTTP connections for ordinary requests and close them
on transport errors or deliberately lost responses. They never replay writes.
S3 response bodies without safe length framing remain close-delimited.

## Many-column and MiB-row recovery

`wide_rows.py` complements the single-payload benchmark with independently
variable column count and cell size. It compares every cell through DuckDB after
initial COPY, complete CDC transactions with unchanged values and NULL changes,
and a same-state SIGKILL/restart with key moves, deletes and writes while down.
It also requires the original source identity and safe PostgreSQL acknowledgement.
Comparison streams 32 rows at a time rather than retaining two complete wide
tables in Python. These are finite correctness probes, not latency or endurance
qualification; values are deterministic, compressible MD5 text.

```sh
uv run tests/production/wide_rows.py \
  --catalog-uri http://127.0.0.1:58181 --s3-endpoint http://127.0.0.1:59000 \
  --binary target/qualification-binaries/current/embrasure-flow \
  --columns 256 --value-bytes 256 --rows 1000 \
  --transactions 12 --transaction-rows 200 \
  --artifacts target/wide-columns-01
```

Use a fresh artifact directory for each case. To probe the column-count boundary,
use `--columns 1500 --value-bytes 1 --rows 1000`. To exercise approximately MiB
rows, use `--columns 8 --value-bytes 131072 --rows 128 --transaction-rows 4`.
Each table also has an ID and a version column. Nullable values make actual row
width slightly smaller than `columns * value-bytes`. PostgreSQL's physical tuple
limit also constrains combinations of column count and value size; a source-side
insert rejection is not an ingestion result. The fixture uses the ordinary
4 MiB chunk / 8 MiB batch budgets and the smoke fixture's smaller disk quotas.
Run the production matrix separately for high-entropy 16 KiB payloads and measured
throughput, and the fault suite for SIGKILL with proven unfinished journal work.
