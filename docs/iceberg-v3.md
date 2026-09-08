# Iceberg v3

Set `format_version = 3` in a `[[tables]]` configuration block to create a v3
target. The default remains v2 for reader compatibility. Existing targets use
their catalog format; changing configuration does not upgrade an existing table.
Both modes currently require unpartitioned, unsorted tables.

V3 stores position deletes as compressed bitmaps in standard Puffin deletion
vectors. Each data file has at most one live vector. An ingest epoch unions its
new positions with prior effective deletes and atomically replaces the old vector
alongside new data. Several vectors can share a Puffin object. Manifest identity
includes the blob offset; retention and garbage collection protect the physical
object while any retained snapshot references it.

Compaction preserves `_row_id` and `_last_updated_sequence_number`. Updates use
Iceberg's permitted delete-and-insert representation, assigning a new ID to the
replacement row. A background build can catch up through cumulative vector
replacements after proving that each replacement retains every earlier deleted
position. The existing ancestry and delete-read budgets bound that work; exceeding
a budget causes replanning. External physical rewrites still require reconciliation
before the row-location index is reused.

## Upgrading

Upgrade through your catalog or an Iceberg engine that supports v3. Coordinate
the upgrade with the service and other writers. Prepared work captures its table
format and is replanned if that format changes before publication. Already
committed work is recovered from its operation marker.

Legacy v2 position-delete files remain readable after an upgrade. A new vector
contains their effective positions for its target and supersedes them for that
target. Shared legacy files remain live where surviving files still need them.
The first v3 snapshot assigns row IDs to existing files; reconciliation accepts
that inherited metadata without treating it as a physical-file replacement.

## Limits and validation

Vectors use the writer's row-group byte budget, capped at 64 MiB per blob, with a
100-million-position cardinality limit. Puffin objects rotate between targets to
bound both payload and footer size. Readers validate framing, CRC, cardinality,
target, offsets and row bounds before applying deletes. Compressed LZ4 footers
are supported with a separate decompression limit. Delete-input budgets charge
DV blob ranges; metadata has its own read limit.

Local integration tests use real journal frames, RocksDB, Parquet and Puffin with
an in-process catalog. They cover repeated updates/deletes, lost commit responses,
restarts, shared objects, upgrades, compaction catch-up, external reconciliation,
row lineage and sparse-vector publication pressure:

```sh
cargo test --locked --workspace --all-targets
```

Independent checks with DuckDB 1.5.5 and Spark 3.5.6 / Iceberg Java 1.10.1 read the
same cumulative-delete, post-compaction and upgraded fixtures successfully.
Spark also verified row IDs and update sequences. Fixtures can be retained for
other readers with `FLOW_V3_EXPORT=/absolute/output/path` when running the
`flow-testkit` `v3_pipeline` test target.

The [v3 service benchmark](benchmarks/v3-deletion-vectors.md)
adds six PostgreSQL/REST/MinIO runs with native background compaction, exact-row
checks and external reconciliation. V3 sustained 10k mutations/s with subsecond
publication p95; at 50k/s offered, it accumulated substantial lag. See the record
for throughput including drain, direct v2/v3 reader comparisons and limitations.

DuckDB 1.5.5 with Iceberg extension `45163a28` requires
`SET enable_external_file_cache = false` when reading shared Puffin vectors.
Its cached range reader can otherwise apply the wrong blob to a target. Our test
loader applies this workaround. An extension containing the
[upstream fix](https://github.com/duckdb/duckdb-iceberg/commit/8b2e25f4c883fe0e22aaf27510115a6159c8f6bc)
can retain the cache; signed extension `6561bfca` passed the captured history replay.

The format contracts are the [Iceberg table specification](https://iceberg.apache.org/spec/)
and [Puffin specification](https://iceberg.apache.org/puffin-spec/).
