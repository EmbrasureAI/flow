# Production allocator and external-rewrite reconcile

## Summary

- The focused benchmarks linked the system allocator, but the published daemon
  is built with `--features jemalloc`. With jemalloc, the
  [compaction worker benchmark](../../crates/compactor/tests/compaction_benchmark.rs)
  was faster in 14 of 15 paired comparisons: by 9–17% at the median on two
  vCPUs, with 8–16% less CPU. The host was noisy during
  these runs, so the size of the gain is approximate. Earlier compaction reports
  therefore understate production speed. Their before-and-after comparisons are
  unaffected, because both sides used the same allocator.
- A new [reconcile benchmark](../../crates/coordinator/tests/reconcile_benchmark.rs)
  times verification of an external rewrite of all 16 files into one file.
  In the quietest round it reconciled about 46,000 rows/s on two vCPUs with
  jemalloc, well below the compaction worker's rate on the same rows.
- Reconcile scratch stores were not given compaction's 64 MiB memtables. In
  rounds that compared both sizes within one process, the larger memtables
  ranged from 12% faster to 9% slower, with no consistent gain, for up to about
  96 MiB more memory per reconcile.

## Running with jemalloc

`flow-state-store/jemalloc` enables RocksDB's jemalloc. On GNU/Linux this
replaces `malloc` for the whole process, which is how `flow-daemon/jemalloc`
works. Builds inside the repository pick up the built-in `thp:never` from
`.cargo/config.toml`, as the daemon does. The daemon also enables jemalloc's
background purge threads at startup; `MALLOC_CONF=background_thread:true` does
the same for a benchmark.

```sh
CARGO_TARGET_DIR=target-jemalloc cargo test --locked --release \
  -p flow-compactor -p flow-coordinator --features flow-state-store/jemalloc \
  --test compaction_benchmark --test reconcile_benchmark --no-run
MALLOC_CONF=background_thread:true taskset -c 0,1 \
  target-jemalloc/release/deps/compaction_benchmark-<hash> --ignored --nocapture
MALLOC_CONF=background_thread:true FLOW_BENCH_SCRATCH_MIB=16,64 taskset -c 0,1 \
  target-jemalloc/release/deps/reconcile_benchmark-<hash> --ignored --nocapture
```

A separate target directory keeps the system-allocator build intact, because the
feature changes how RocksDB is built.

## Compaction worker: system allocator and jemalloc

The workload is the [Arrow-only rewrite report](arrow-compaction-rewrite.md)'s:
16 files of 32,768 ten-column rows (524,288 rows, 35.9 MiB of Parquet), a real
RocksDB row index, local files, one warmup and seven iterations per case. The
code is `main` at `155cdbe` in both builds.

Sequential rounds, one allocator at a time, varied by up to 2× from round to
round on this host. The comparison below therefore ran the two builds at the
same time on separate physical cores (`taskset -c 0,1` and `-c 2,3`), swapping
cores between rounds, so host noise reached both alike. Sharing the last-level
cache slows both builds compared with running alone.

Two vCPUs, medians of three paired rounds:

| Case | p50 ms | Rows/s | CPU s | Peak RSS growth MiB |
| --- | --- | --- | --- | --- |
| v2, no deletes | 4,089 → 3,387 (−17%) | 128,200 → 154,800 (1.21×) | 6.85 → 5.74 (−16%) | 96–103 → 29–51 |
| v2, 10% position deletes | 4,057 → 3,490 (−14%) | 116,300 → 135,200 (1.16×) | 6.41 → 5.48 (−15%) | 26–87 → 53–66 |
| v3, 10% deletion vectors | 3,780 → 3,456 (−9%) | 124,800 → 136,500 (1.09×) | 6.09 → 5.60 (−8%) | 33–90 → 60–65 |

jemalloc was faster in all nine two-vCPU pairs. On one vCPU (cores 0 and 2,
two paired rounds) it was faster in five of six pairs, but single runs varied
by up to 2× between rounds, so those figures are not reported.

## Reconcile of an external data rewrite

`crates/coordinator/tests/reconcile_benchmark.rs` (ignored by default) ingests
the same 16 files through the service and indexes them. It then commits an
external `replace` snapshot, labelled with a foreign operation key the way
Spark's `rewrite_data_files` would be, that rewrites every row into one file.
Each iteration copies the pre-rewrite index and times one
`TableMaintenance::reconcile` call with a fresh scratch store. The call
classifies the snapshot as a data rewrite, scans the new file into scratch,
checks every key, fingerprint and reverse mapping against the index, and
applies the new locations. It asserts that the head snapshot was applied and
that sampled keys moved off the rewritten files. The scratch options are the
daemon's. `FLOW_BENCH_SCRATCH_MIB` (default `16,64`) lists memtable sizes to
compare, and each iteration rotates the order of the sizes.

The same benchmark in Embrasure's internal build, timed phase by phase, spent
about 18% of reconcile in scanning and staging into scratch. The rest was
row-index work: a reverse-index check with one point lookup per removed row,
a second lookup per row while staging into the index, and the apply. Both
lookups are unbatched `lookup` calls, while compaction batches its lookups with
`lookup_many`.

Results with the daemon's 16 MiB scratch memtables, per round:

| | p50 ms | Rows/s | CPU s |
| --- | --- | --- | --- |
| Two vCPUs, jemalloc | 11,400 and 21,164 | 46,000 and 24,800 | 18.70 and 31.00 |
| Two vCPUs, system allocator | 13,444 and 13,775 | 39,000 and 38,100 | 21.44 and 21.80 |
| One vCPU, jemalloc | 21,118 and 20,155 | 24,800 and 26,000 | 19.91 and 19.11 |
| One vCPU, system allocator | 22,892 and 21,647 | 22,900 and 24,200 | 21.33 and 20.45 |

The second two-vCPU jemalloc round coincided with host interference: its
process CPU rose by two thirds for the same work.

### 64 MiB scratch memtables: measured and not adopted

p50 change from 16 to 64 MiB, measured within one process in each round:

| | Round 1 | Round 2 |
| --- | --- | --- |
| Two vCPUs, jemalloc | +3% | −12% |
| Two vCPUs, system allocator | +9% | −1% |
| One vCPU, jemalloc | +3% | −11% |
| One vCPU, system allocator | −8% | −5% |

The internal build, measured on a quieter host, gained 0.3–3.7% from the same
change. Up to two 64 MiB memtables would add as much as 96 MiB to each
reconcile, against the 128 MiB per build that the
[memory guide](../operations.md) allows only for compaction. The index lookups
and apply are the place to speed up reconcile, not scratch.

## Method

The host is a KVM guest with an Intel Xeon @ 2.50 GHz, 8 vCPUs (two hyperthreads
per core), 16 GiB of memory, a virtio disk, Linux 6.18 with transparent huge
pages set to `madvise`, and Rust 1.97.1. "Two vCPUs" is the two hyperthreads of
one core; "one vCPU" is one hyperthread. Reconcile rounds ran one after another
on an otherwise idle host, alternating which allocator went first. The jemalloc
runs set `MALLOC_CONF=background_thread:true`. A jemalloc statistics dump
confirmed `opt.thp: "never"` and background threads, and the system-allocator
binaries were checked for the absence of jemalloc's `malloc`.

## Limits

These are focused timings against local files, not service throughput.
Object-store latency and catalog commits are outside both benchmarks' timed
regions, apart from the reconcile benchmark's table reload. The reconcile
fixture rewrites one unpartitioned, delete-free table in one snapshot.
Delete-rewrite verification and keyless tables are not measured. The host
differs from the one in earlier compaction reports, and was noisy during these
runs, so compare absolute numbers only within one table.
