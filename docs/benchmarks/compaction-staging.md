# Compaction staging overlap and scratch memtables

## Where the time went

After the [Arrow-only rewrite](arrow-compaction-rewrite.md), the
[worker benchmark](../../crates/compactor/tests/compaction_benchmark.rs) spent
about 3.0–3.4 seconds per compaction on two vCPUs. Temporary timers around each
phase of `build_data`, for the v2 case without deletes, attributed it as
follows. The other two cases were similar.

| Phase | ms | Share |
| --- | ---: | ---: |
| Staging row mappings into scratch RocksDB (`stage_deltas`) | ~1,340 | 44% |
| Row-index lookups (`lookup_many`) | ~730 | 24% |
| Parquet encode (`write_arrow`) | ~595 | 19% |
| Keys and fingerprints | ~210 | 7% |
| Parquet decode, schema conformance and delete filtering | ~190 | 6% |

Every phase ran in sequence on the worker thread. Separately, RocksDB's
background threads flushed and recompacted the scratch store's 16 MiB
memtables. Those threads took about a third of the CPU samples in a profile of
the benchmark: on two vCPUs they ran beside the worker, and on one vCPU they
competed with it.

## What RisingWave's compactor does

[RisingWave](https://github.com/risingwavelabs/risingwave) compacts Iceberg
with [iceberg-compaction](https://github.com/nimtable/iceberg-compaction)
(Apache-2.0), a Rust engine on DataFusion. Its source shows that its speed comes
from running many file streams at once on a multi-thread runtime, with scan and
decode overlapping encode and write as separate tasks. Its Parquet path is not
special: 1,024-row batches like ours, full decode and re-encode of every file,
parquet-rs defaults with ZSTD and 128 MiB row groups. It also keeps no row index
during the rewrite; a later resolver reads the output's key columns and updates
its index. Flow validates and maps every row while it rewrites, so that design
does not transfer directly.

The transferable lesson for one compaction on a small host was to stop
running independent stages in sequence.

## Changes

- **Staging overlap.** The worker hands each batch's mappings to a dedicated
  staging thread through a bounded queue of two batches. Decode, lookups and
  encode of the next batch proceed while the previous batch is staged. Order and
  delta ordinals are unchanged. A staging error is returned at the next send or
  when the worker waits for the queue before sealing, and the stager waits for
  queued batches when dropped, so no scratch write outlives the build.
- **Scratch memtables.** Compaction scratch stores use
  `StateStoreOptions::scratch()`, with 64 MiB memtables instead of 16 MiB.
  They flush a quarter as often, and RocksDB rewrites the staged mappings far
  fewer times before the build ends.

## Variants rejected

Settings were compared within one process on one fixture, rotating variants
every iteration so host noise affected them equally. Host noise between runs
was up to about 30%, so separate runs of single variants were not trusted.

| Variant | Result |
| --- | --- |
| Scratch without LZ4 compression | Up to 14% less CPU alone, and 1–5% faster on top of 64 MiB memtables (none on top of 32 MiB), but scratch files 3–4× larger (about 54 → 173–220 MiB here). On a memory-backed `state_dir` that is memory. |
| L0 compaction trigger of 16 files | 4–17% slower: staging's duplicate checks read across more L0 files. |
| 4,096-row batches | No consistent change. |
| 32 MiB memtables | About 60% of the gain of 64 MiB, for about a third of the extra memory. |

Pipelining alone, without larger memtables, cut two-vCPU latency 13–18%. On one
vCPU it neither helped nor hurt beyond noise; the larger memtables gave the
one-vCPU gain.

## Method

The workload and host are those of the
[Arrow-only rewrite report](arrow-compaction-rewrite.md): 16 files of 32,768
ten-column rows (524,288 rows, 35.9 MiB of Parquet), a real RocksDB row index,
local files, release profile, and the same KVM guest (Intel Xeon @ 2.90 GHz,
16 GiB, virtio disk, Linux 6.18, Rust 1.97.1) pinned with `taskset`. The
benchmark opens its scratch store with the options production uses, so the
baseline uses the default options and the candidate uses
`StateStoreOptions::scratch()`.

Baseline is the Arrow-only rewrite on `main` at `318bc85`. Each
checkout used its own Cargo target directory, the binaries were checked for the
presence and absence of the stager, and they ran alternately on an otherwise
idle host: four rounds on two vCPUs and two on one vCPU, seven iterations each
after a warmup. Figures are medians across rounds.

## Results

Two vCPUs:

| Case | p50 ms | p95 ms | Rows/s | CPU s | Peak RSS growth MiB |
| --- | --- | --- | --- | --- | --- |
| v2, no deletes | 3,412 → 2,399 (−30%) | 3,770 → 2,681 | 153,700 → 220,000 (1.43×) | 5.00 → 4.05 (−19%) | 16–25 → 101–111 |
| v2, 10% position deletes | 3,322 → 2,341 (−30%) | 3,664 → 2,456 | 142,100 → 201,600 (1.42×) | 4.83 → 3.74 (−23%) | 39–45 → 87–102 |
| v3, 10% deletion vectors | 3,499 → 2,370 (−32%) | 3,800 → 2,486 | 134,900 → 199,100 (1.48×) | 4.94 → 3.77 (−24%) | 37–45 → 91–98 |

One vCPU:

| Case | p50 ms | p95 ms | Rows/s | CPU s | Peak RSS growth MiB |
| --- | --- | --- | --- | --- | --- |
| v2, no deletes | 5,182 → 3,941 (−24%) | 6,030 → 4,310 | 101,200 → 133,100 (1.32×) | 5.18 → 3.94 (−24%) | 20–24 → 94–96 |
| v2, 10% position deletes | 4,978 → 3,970 (−20%) | 5,217 → 4,422 | 94,800 → 120,000 (1.27×) | 4.97 → 3.96 (−20%) | 35–36 → 82–103 |
| v3, 10% deletion vectors | 4,932 → 4,255 (−14%) | 5,277 → 6,170 | 95,700 → 113,800 (1.19×) | 4.93 → 4.23 (−14%) | 35–44 → 85–103 |

On two vCPUs median worker latency fell 30%, 30% and 32%, throughput rose 1.43×,
1.42× and 1.48×, and CPU time per compaction fell 19–24%. On one vCPU latency fell
24%, 20% and 14%.

The candidate was faster in every round except one two-vCPU and one one-vCPU
round of the v3 case, both during host noise; the one-vCPU v3 figure rests on
two rounds and is the least certain. The larger memtables cost up to about
90 MiB more peak memory per running build, released when the build ends; the
memory guide in [Operations](../operations.md) accounts for up to 128 MiB of
scratch memtables per build. Scratch files grew slightly,
from about 40–60 to 55–75 MiB in this workload.

## Limits

These are worker timings against local files, not service throughput or
publication latency. Catalog commits, object-store latency and index
application are outside the timed region. The fixture's table is unpartitioned
and unsorted. Reconcile and index-rebuild scratch stores keep the default
options; their staging pattern is similar but was not measured. The bigger
remaining lever is RisingWave's: splitting one compaction's inputs across
several workers, which needs one consistent index snapshot and a deterministic
merge of staged mappings.
