# Remote durability: integration results

October 13, 2026. These trials measure [remote durability](../remote-durability.md)
against local-disk authority on emulated and real object storage. Remote
durability is opt-in.

## Summary

- At 30,000 and 60,000 mutations/s on real S3, replication now costs no
  measurable CDC service time or drain, after two changes from October 29: a
  commit waits only for the state it was built from, and log batches are
  written by size and age. Before them it cost up to 3.8% service time, 3.6%
  drain and 63% p95 latency at 60,000 mutations/s. See
  [Throughput on real S3](#throughput-on-real-s3).
- Remote durability costs about as much as local-disk authority. With emulated
  S3 latency, one table at 20 transactions/s published at p50/p99 1.26/2.68 s
  against 1.18/2.44 s locally. Against real S3 Standard and an AWS Glue
  catalog, one table at 5 transactions/s measured 1.78/3.85 s against
  1.83/4.09 s locally.
- Every trial passed exact verification of every row, including updates, deletes,
  rollbacks and writes during the initial copy. Fault trials covered process
  crashes, loss of the whole state directory, loss of 33 transactions that
  PostgreSQL had acknowledged but Iceberg did not yet contain, a stale owner
  running beside its replacement, and a lost catalog commit response.
- Publication and maintenance changes made alongside remote durability cut the
  emulated p99 from 16.8 s to 2.5 s in both modes; they are not specific to
  remote durability.
- Replicated row-index checkpoints with catch-up recover a 1,000,340-row table's
  index in 2.2 s instead of 12.4 s for a full rebuild.
- The ten-table case is still far from a two-second target (p50/p99 about
  2.8/9.7 s with four table workers, 1.9/5.2 s with ten), in both modes.

## Environments

| Environment | Source | Catalog | Object storage |
| --- | --- | --- | --- |
| Emulated | PostgreSQL 18.4, local | Iceberg REST fixture 1.10.1 | MinIO behind a latency proxy |
| Real services | PostgreSQL 18.4, local | AWS Glue through an Iceberg REST adapter | S3 Standard |

Both ran on an 8-vCPU, 16 GiB x86-64 host in us-east-1, with PostgreSQL `fsync`,
`full_page_writes` and `synchronous_commit` on. The proxy delays each of Flow's
object requests by a sample drawn from S3 conditional PUTs measured from the
same host: p50 32 ms, p90 43 ms, p99 about 105 ms for 4 KiB objects, 70 ms p50
for 4 MiB, and 21 ms for HEAD. The REST fixture is faster than Glue, whose table
GETs measured p50 109 ms and commits p50 230 ms. The source shares the host,
which is more favorable than a remote customer database. These are bounded
trials, not capacity qualification. Release builds used `lto = "off"` and 16
codegen units to iterate quickly.

## Workload and verification

Each transaction updates ten random keys, deletes and reinserts one key, and
inserts an immutable marker row in every table. `--adversarial` adds permanent
deletes and fully rolled-back transactions. A trial passes only when every
committed marker is visible in every table through DuckDB reading ordinary
Iceberg metadata, and a sorted comparison of every `(id, version, md5(payload))`
row equals PostgreSQL. Catalog latency joins each source commit timestamp to
Flow's catalog commit event for the slowest table; reader latency adds polling.
Percentiles use the sample at `floor(p × (n − 1))`.

## Steady state, emulated S3 latency

One table, 10,000 seed rows, 5 transactions/s for 45 s. Catalog visibility in
seconds:

| Build | Authority | p50 | p90 | p99 |
| --- | --- | ---: | ---: | ---: |
| Durability only | Local disk | 1.85 | 2.59 | 4.39 |
| Durability only | Remote | 1.87 | 7.51 | 8.91 |
| + publication round trips | Local disk | 1.58 | 13.41 | 16.78 |
| + publication round trips | Remote | 1.53 | 13.24 | 16.78 |
| + maintenance prefetch | Local disk | 1.18 | 1.85 | 2.52 |
| + maintenance prefetch | Remote | 1.17 | 1.85 | 2.61 |

The first two rows used 2,000 seed rows for 30 s. Their tails, and the second
pair's, were maintenance holding the table actor: a delete rewrite and an L1
rewrite reading inputs one file and one range at a time, and background builds
whose catch-up could not finish inside the 750 ms activation gate while new
epochs kept arriving. A normal epoch also made about sixteen sequential object
requests. The publication change reuses the head and plan a fresh publication
already validated and writes manifests concurrently; the maintenance change
fetches small inputs whole, a bounded number ahead of the scan.

## Load and table fan-out, emulated S3 latency

Adversarial workload, 10,000 seed rows, 45 s. Catalog p50/p90/p99 in seconds:

| Case | Local disk | Remote |
| --- | ---: | ---: |
| 1 table, 20 tx/s | 1.18 / 1.87 / 2.44 | 1.26 / 1.97 / 2.68 |
| 10 tables, 2 tx/s | 2.89 / 8.09 / 10.59 | 2.79 / 7.22 / 9.72 |

With `table_workers = 10` instead of the default 4, the ten-table remote case
measured 1.86/3.40/5.19 s: concurrency, not replication, bounds it. Before the
publication and maintenance changes, the 20 transactions/s case measured
1.95/4.34/7.54 s remote and 1.90/6.11/9.07 s local, so remote durability itself
was already not the limiting factor. At 20 transactions/s, remote barriers
averaged 21 ms, log writes 36 ms, and batches 2.5 records; capture latency
stayed under 50 ms after the first second.

## Real S3 and Glue

One table, 10,000 seed rows, adversarial, 5 transactions/s for 60 s:

| Authority | Catalog p50 / p90 / p99 | Reader p50 / p99 |
| --- | ---: | ---: |
| Local disk | 1.83 / 3.08 / 4.09 s | 2.34 / 4.62 s |
| Remote | 1.78 / 2.72 / 3.85 s | 2.33 / 4.39 s |

Remote log writes averaged 30 ms and barriers 29 ms, with about one record per
batch at this rate: roughly 15 conditional PUTs per second, plus one lease check
every 2.5 s. After the run the prefix held 4 log batches, 2 checkpoints, 2
control snapshots and 3 journal parts (1.1 MB).

## Faults

Two tables, 10,000 seed rows, 4 transactions/s for 40 s, adversarial. Every
trial passed exact rows. Recovery is the time from the fault to the first later
transaction visible to the reader.

| Fault | Ack | Environment | Result |
| --- | --- | --- | --- |
| SIGKILL, same volume | materialized | emulated | Recovered in 4.2 s; the same lock under the same kernel proves the old owner exited, so no lease wait (27.3 s before that change) |
| SIGKILL, state directory removed | materialized | emulated | Restored from object storage; 16.3 s (49.0 s before maintenance and restart changes) |
| 15 s catalog outage, then SIGKILL and state directory removed | journaled | emulated | 33 transactions acknowledged to PostgreSQL but absent from Iceberg at the loss were restored and published |
| Same | journaled | real S3, Glue | 33 such transactions restored and published |
| Replacement started on a new volume beside a running owner | materialized | emulated, real | Old owner fenced and exited 1; replacement restored and completed |
| Catalog commit succeeded, response replaced by HTTP 502 | journaled | emulated, real | Operation found by its marker; no duplicate |
| SIGTERM and restart | journaled | emulated | Release record; restart did not wait for the lease |

Acknowledged-but-unpublished transactions are counted independently: the
harness reads the slot's `confirmed_flush_lsn`, maps it to journaled
transactions, and confirms their markers are missing from current Iceberg
snapshots before removing the state directory. Unclean host replacement waits
one 10 s lease plus 1 s; a crash on the same volume does not.

## Large table recovery

One table with 1,000,000 seed rows and 100 transactions committed during the
initial copy, then 4 transactions/s with the state directory removed halfway.
Both trials passed exact rows (1,000,340).

| Index recovery | Restore control and journal | Row index | First visible after loss |
| --- | ---: | ---: | ---: |
| Full rebuild from Iceberg, emulated | 13.4 s | 12.4 s | 29.3 s |
| Replicated checkpoint and catch-up, emulated | 13.3 s | 2.2 s | 19.7 s |
| Replicated checkpoint and catch-up, real S3 and Glue | 12.4 s | 1.6 s | 19.3 s |

In the emulated trial the replicated checkpoint was 190 MB; downloading it took
0.5 s from the local MinIO, and catch-up processed 16 removed and 17 added files.
Against real S3 the 223 MB checkpoint downloaded in 0.8 s and catch-up took
1.2 s. The first checkpoint after each restart uploaded 8 MB and 0.4 MB. Restore
time is dominated by the lease wait for an unclean predecessor. Full rebuild
time grows with table size; catch-up grows with files changed since the last
checkpoint (every 15 s in this trial, 300 s by default).

## Final build

The final build gates every object write on the lease, makes recovery stop each
owner's history at its first missing batch (rereading after the lease wait),
names control snapshots by checksum and writer, uploads journal parts
concurrently, streams index checkpoint files and stops journal reclamation from
blocking on replication. It was rerun on the emulated setup, with exact rows in
every trial:

| Trial | Result |
| --- | --- |
| 1 table, 20 tx/s, local disk | Catalog p50/p90/p99 1.17/1.84/2.44 s |
| 1 table, 20 tx/s, remote | 1.26/1.98/2.63 s; barriers 15 ms mean, log writes 36 ms |
| SIGKILL, same volume | No lease wait; first change visible 5.5 s after the kill |
| 15 s catalog outage, then SIGKILL and state directory removed (journaled) | 34 acknowledged, unpublished transactions restored and published |
| Replacement beside a running owner | Old owner fenced and exited 1 |
| SIGTERM and restart | Release record; no lease wait; first change visible 3.1 s later |
| SIGKILL, state directory removed | Restored; first change visible 18.2 s later |

The fault trials shared the host with a workspace build, so their recovery
times are a little slower than in the table above.

## Throughput on real S3

October 29, 2026. These runs measure what replication costs at sustained load
and the two changes it led to: a publication's commit waits only for the state
it was built from, and log batches are written by size and age, or at once
when an external effect waits. Each comparison runs the same binary with and
without `[durability]` using `tests/production/ab.py`, alternating sides, three
runs per side, every row verified.

- Host: one c7i.4xlarge (16 vCPU, 32 GiB) in us-east-1 per comparison. One
  150 GiB gp3 volume (6,000 IOPS, 400 MiB/s) holds the state directory and the
  fixture's PostgreSQL 18 and Iceberg REST catalog. The Iceberg warehouse and
  the durability prefix are in the same S3 Standard bucket, reached with the
  host's instance role.
- Workloads: the default mixed 100-row transactions at 30,000 and 60,000
  mutations/s for 60 s after 10 s of warmup; the Zipf update90 workload on
  2,000,000 rows at 30,000 mutations/s for 45 s; and 400 s at 30,000
  mutations/s, two runs per side, which crosses a 300 s row-index checkpoint.
- Builds: `profiling` for the final 30k and 60k comparisons, `bench-fast` for
  the rest.

Cost of replication, median change from local-only to `[durability]`:

| Workload | Before: CDC service / drain / p95 | After: CDC service / drain / p95 |
| --- | --- | --- |
| 30k mutations/s | −0.1% / +0.8% / +18% | −0.3% / +0.3% / +1.1% |
| 60k mutations/s | +3.8% / −3.6% / +63% | −0.7% / −1.3% / +11.5% |
| Zipf, 30k mutations/s | +6.9% / −1.6% / +13% | −0.2% / −2.1% / +9.4% |
| 400 s at 30k mutations/s | not measured | +0.2% / −0.2% / +7.1% |

CDC service time is per mutation; p95 is commit-to-catalog latency. The
service time changes after are within each side's run-to-run range, and so are
the p95 changes except at 60k mutations/s, where both sides are at the
single-table lane's limit and their p95 ranges overlap (3.5–4.5 s and 3.6–4.7
s).

- Before, every catalog commit waited for the newest log record. Under
  continuous capture that is journal bytes captured while the manifests were
  written, so the commit stage took 75–83 ms longer per epoch (about 185 to
  265 ms), epochs grew, and near saturation drain fell. Waiting only for the
  publication's registration brought the extra wait to 6–20 ms on average
  (`flow_durability_barrier_seconds{effect="catalog_commit"}`): 9–16 ms more
  per epoch than local-only.
- Log PUTs per million mutations fell from about 700 to 170 at 30k mutations/s
  and from about 390 to 92 at 60k, about 4.5 a second instead of the
  `max_writes_per_second` cap of 20; batches average 1.8 and 3.5 MiB. Control
  checkpoints every 30 s took 0.1–0.6 s, and the 151 MiB row-index checkpoint
  uploaded in 1.8–2.4 s, neither visibly delaying publication. No request was
  retried.
- Source acknowledgements were released 180–220 ms after their progress was
  computed, against about 80 ms before; in `materialized` mode they follow
  publication anyway. Peak sampled RSS was 7–11% higher with replication.
- Splitting the prefix a commit waits for over the free write slots did not
  shorten the wait, which is mostly earlier full batches already in flight,
  and added 22% more PUTs, so it was dropped. Removing the request budget
  (`max_writes_per_second = 0`) on the old writer saved about 25 ms per commit
  for 4.4 times the PUTs.

Every run passed full verification. At 60,000 mutations/s the single-host
PostgreSQL source missed 900–3,700 of about 36,000 scheduled transactions per
run on both sides, and the 2.5 s p95 commit-to-catalog gate failed on both
sides; the Zipf and 400 s runs missed 240–1,600 transactions per run. One
local-only Zipf run's independent reader-baseline compactor did not finish
within its 180 s timeout on real S3 after verification had passed.

## Limits of this evidence

- Short single-host trials; not a capacity, endurance or multi-tenant result.
- The emulated proxy reproduces request latency, not S3 throughput limits,
  throttling or failure modes. The real-service runs are few, and their catalog
  adapter is not part of this repository.
- Ten-table freshness misses the working two-second target in both modes.

## Reproduction

Build with `cargo build --release -p flow-daemon`, start the
[production fixture stack](../../tests/production/README.md) services (or any
PostgreSQL with `wal_level=logical`, MinIO and the Iceberg REST fixture), then:

```sh
uv run tests/production/durability_bench.py --label remote-01 \
  --root runs --binary target/release/embrasure-flow \
  --durability remote --s3-latency-ms 30 --seconds 45 --tps 5 --seed-rows 10000
uv run tests/production/durability_bench.py --label backlog-loss \
  --root runs --binary target/release/embrasure-flow --durability remote \
  --ack-mode journaled --fault disk-loss-backlog --tables 2 --tps 4 --adversarial
```

Throughput comparisons use the [A/B harness](../../tests/production/README.md)
on one binary, with the fixture's warehouse in S3
(`FLOW_PRODUCTION_WAREHOUSE=s3://bucket/prefix/`):

```sh
tests/production/ab.py --profile profiling --base HEAD --candidate HEAD --runs 3 \
  --args "--rate 60000 --duration 60 --warmup 10 \
    --s3-endpoint https://s3.us-east-1.amazonaws.com --warehouse s3://bucket/prefix/" \
  --candidate-args "--durability-uri s3://bucket/prefix/durability"
```
