# Data-file ordinals in the row index

Row-index format 4 names data files by a per-table ordinal instead of their
path in primary-key values, reverse keys and staged deltas. Before, every
indexed row repeated its data file's full path, about 110 bytes for the
default warehouse layout, so bind lookups, delta staging, apply writes and
RocksDB compaction all scaled with path length.

## Summary

- One busy table rose from 111,000–116,000 to 158,000–159,000 mutations/s
  with default paths, and from 83,000 to 157,000 with paths 155 characters
  longer.
- Median apply fell from 403 to 277 ms per epoch and collapse from 158 to
  130 ms (533–592 and 171 ms with long paths). Path length no longer affects
  either.
- These are local stage timings without PostgreSQL, object storage or a REST
  catalog, not publication latency or service throughput.

## Environment and workload

- One 16-vCPU x86-64 host (AWS `c7i.4xlarge`).
- Release builds of the two revisions, two runs per side.
- The base already included parallel apply lookups, streamed residual
  positions and parallel bind.
- The PostgreSQL-free ingest microbenchmark `ingest_benchmark`,
  `ingest_epochs_one_table`: 1M initial keys, 10 epochs of 113,000 mutations
  from 100-row source transactions (70% updates, 20% inserts and 10% deletes),
  256-byte rows. The journal, row index, publisher and Iceberg files are real;
  the catalog is in memory. The final Iceberg rows and every indexed key are
  checked against the generator.

```sh
INGEST_BENCH_KEYS=1000000 INGEST_BENCH_EPOCHS=10 \
  cargo test --release -p flow-testkit --test ingest_benchmark \
  ingest_epochs_one_table -- --ignored --nocapture
```

## Design limits

- Ordinals are allocated under the row lock in the batch that first names a
  file and are never reused. A failed write that touched the dictionary fences
  the store.
- Live-row counts, remaps and the transaction spool stay keyed by path.
- An ordinal is released once no row, reverse entry or remap names it and no
  remaining operation's staged deltas pin it.
- Format 3 and older indexes are rebuilt from the control store and Iceberg on
  first start; older builds refuse a format 4 index. See the
  [changelog](../../CHANGELOG.md) upgrade notes.
