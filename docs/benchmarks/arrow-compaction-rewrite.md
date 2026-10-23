# Arrow-only compaction rewrite

## Change

A compaction worker read each Parquet batch as Arrow, converted it into
`Vec<Value>` rows to compute keys and fingerprints, and converted the rows back
into Arrow for the Parquet writer. Each row was validated three times, and
every string and binary value was copied twice.

The worker now keeps scanned batches columnar:

- `crates/compactor/src/columnar.rs` selects scan columns by field ID into the
  writer's Arrow schema, accepting what `rows_from_batch` accepts. Arrays are
  shared, not copied, unless their Arrow type differs. Deleted rows are removed
  with one Arrow filter, and only when a batch contains deletes.
- Keys and fingerprints are computed from the arrays. They reproduce
  `flow_model`'s canonical encoding, and unit tests compare them byte for byte
  with `TableSchema::encode_key` and `fingerprint` for every column type, NaN
  and signed zero, interior and trailing nulls, and older files that lack a
  later nullable column.
- `DataWriter::write_arrow` writes a batch already in `writer_arrow_schema`. It
  applies the row writer's admission limits, batch splitting, file rotation,
  lineage columns and decimal-precision validation. A test checks that it writes
  the same Parquet bytes and row locations as `DataWriter::write`.

Data files, delete files, index mappings and the scan used by index rebuild and
external reconciliation are unchanged. That scan now converts filtered Arrow
batches into rows.

## Method

`crates/compactor/tests/compaction_benchmark.rs` (ignored by default) commits
16 data files of 32,768 rows each (524,288 rows, 35.9 MiB of Parquet) to a
memory catalog over local files. Rows have ten columns: keyed `Int64`, `Int64`,
two strings (one 64–160 bytes), `Decimal(18,2)`, `timestamptz`, nullable
`Float64`, `Bool`, a half-null string and a UUID. Each case loads a real
RocksDB row index and times one strict `compact` call over all 16 files with a
fresh scratch store, after one warmup, for seven iterations:

- v2 with no deletes;
- v2 with 10% of rows removed by one position-delete file;
- v3 with 10% removed by deletion vectors, preserving row lineage.

The worker runs on a one-thread Tokio runtime, as in the daemon. CPU time comes
from `/proc/self/stat` and includes RocksDB and Tokio background threads. Peak
RSS growth is the high-water mark during the call, above the RSS before it.
Output row and mapping counts are checked after every iteration.

Host: KVM guest, Intel Xeon @ 2.90 GHz, 16 GiB RAM, virtio disk, Amazon Linux
2023, Linux 6.18, Rust 1.97.1, release profile (thin LTO). The process was
pinned with `taskset` to two vCPUs and then one, the small end of the hardware
this service runs on. Baseline is `main` at `318bc85` with only the benchmark
file added. Each checkout used its own Cargo target directory, the two binaries
were confirmed to contain and omit the new code paths, and they ran alternately
on an otherwise idle host.

```sh
cargo test --locked --release -p flow-compactor --test compaction_benchmark --no-run
taskset -c 0,1 target/release/deps/compaction_benchmark-<hash> --ignored --nocapture
```

`FLOW_BENCH_FILES`, `FLOW_BENCH_ROWS_PER_FILE`, `FLOW_BENCH_WARMUPS` and
`FLOW_BENCH_ITERATIONS` change the workload.

## Results

Latency is per compaction call; throughput is live rows written per second.
Each figure is the median across alternating rounds of seven iterations: four
rounds on two vCPUs and two on one vCPU.

Two vCPUs:

| Case | p50 ms | p95 ms | Rows/s | CPU s | Peak RSS growth MiB |
| --- | --- | --- | --- | --- | --- |
| v2, no deletes | 4,804 → 3,412 (−29%) | 5,104 → 3,770 | 109,200 → 153,700 (1.41×) | 6.58 → 5.00 (−24%) | 14–18 → 16–25 |
| v2, 10% position deletes | 4,454 → 3,322 (−25%) | 5,108 → 3,664 | 106,100 → 142,100 (1.34×) | 5.94 → 4.83 (−19%) | 35–43 → 39–45 |
| v3, 10% deletion vectors | 4,458 → 3,499 (−22%) | 5,889 → 3,800 | 105,900 → 134,900 (1.27×) | 5.94 → 4.94 (−17%) | 35–43 → 37–45 |

One vCPU:

| Case | p50 ms | p95 ms | Rows/s | CPU s | Peak RSS growth MiB |
| --- | --- | --- | --- | --- | --- |
| v2, no deletes | 6,918 → 5,182 (−25%) | 7,302 → 6,030 | 75,800 → 101,200 (1.33×) | 6.92 → 5.18 (−25%) | 17–19 → 20–24 |
| v2, 10% position deletes | 6,434 → 4,978 (−23%) | 6,664 → 5,217 | 73,600 → 94,800 (1.29×) | 6.43 → 4.97 (−23%) | 35–37 → 35–36 |
| v3, 10% deletion vectors | 6,606 → 4,932 (−25%) | 7,150 → 5,277 | 71,700 → 95,700 (1.33×) | 6.60 → 4.93 (−25%) | 35–36 → 35–44 |

On two vCPUs median worker latency fell 29%, 25% and 22% for the three cases,
throughput rose 1.41×, 1.34× and 1.27×, and CPU time per compaction fell 24%,
19% and 17%. On one vCPU latency fell 25%, 23% and 25%. Peak RSS growth did not
differ consistently between builds; the spread between cases tracks the staged
delete positions. One of the Arrow build's four two-vCPU rounds was slowed by
host noise (about 5.2–5.6 seconds in two cases); the medians absorb it.

## Where the remaining time goes

A sampled profile of the Arrow build (`perf record -g`, three iterations of all
three cases, same two vCPUs) is flat: no symbol exceeds 7%. Grouping symbols by
owner, with the caveat that shared routines such as `memcmp` and `memmove` are
attributed to their most likely caller:

| Group | Share of samples |
| --- | ---: |
| RocksDB row-index lookups and scratch staging | ~51% |
| Allocator and bulk memory moves | ~26% |
| Parquet encode, including ZSTD | ~9% |
| Unresolved or miscellaneous | ~10% |
| BLAKE3 row fingerprints | ~2% |
| Arrow filtering and key encoding | <1% |

The row index and scratch store now dominate, and this change does not touch
them. Further compaction work should target the index lookups and staged
mappings rather than the row conversion.

## Limits

These are worker timings against local files, not service throughput or
publication latency. Catalog commits, object-store latency, index application
and concurrent maintenance are outside the timed region. The fixture's table is
unpartitioned and unsorted, as the worker requires. Seven iterations make p95
the second-slowest sample, so p95 is sensitive to a single interrupted run, and
one vCPU has two rounds rather than four. Row widths, column types and the
share of deleted rows all change the ratio, because they change how much work
the removed row conversion represented. The baseline already builds writer
batches directly in the writer's Arrow schema, so the gain here is the removed
row materialization, not that earlier conversion.
