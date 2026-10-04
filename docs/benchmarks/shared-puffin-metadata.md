# Shared-Puffin metadata investigation

## Source baseline and path

Baseline: upstream `main` at `2ed96d7bddd2294c9d3afd501ab217be1d1a528a`, fetched
2026-10-04. The investigation branch is local. The existing snapshot-cleanup
branch was left intact in its original checkout.

1. `crates/materializer/src/deletion_vector.rs`: `VectorWriter::finish_target`
   appends one bitmap blob per target. `finish_object` closes the Puffin writer,
   observes its size, and builds multiple `DataFile` descriptors with the same
   physical path/size and different target, offset, length and cardinality.
   Rotation occurs between targets or when the footer budget is reached.
2. `crates/coordinator/src/publication.rs`: `TablePublisher::merge_previous_deletes`
   obtains the current `SnapshotView`, identifies superseded vectors, and calls
   `stage_position_deletes`. ManifestCache caches parsed manifests, not Puffin
   object sizes. Shared legacy position files remain subject to the existing
   applicability rules.
3. `crates/compactor/src/worker.rs`: staging reaches `load_deletes`. Its ordered
   descriptor loop opens an input, calls `metadata`, and compares its size to
   **each** descriptor's manifest size. Logical byte/row budgets are per blob.
4. `vendor/iceberg/src/puffin/deletion_vector.rs`: `read_deletion_vector` opens
   another input and calls `metadata` again. It makes four bounded reads: header,
   last 12 bytes, footer including magic, and requested blob. It validates bounds,
   magic, footer flags/length, exact blob identity/target/cardinality, framing,
   CRC and portable bitmap data. It does not use the generic PuffinReader cache.
5. `InputFile::metadata` directly delegates to `Storage::metadata`. The resolving
   OpenDAL storage selects a cached operator; OpenDAL storage calls `op.stat`.
   The pinned OpenDAL S3 backend (`opendal-service-s3` 0.57.0) issues HEAD for stat.
   FileIO's cached Storage and OpenDAL's cached operators, connection pools and
   rotating credentials do not cache object metadata. Timeout/retry layers may
   issue extra requests on failure; successful reads have no metadata cache.
6. Metadata contains only `size`. The loader needs it for manifest validation;
   the DV reader also needs it to find the footer and bound ranges. Merely
   deduplicating the outer call would leave one inner HEAD per descriptor.

The writer refuses to overwrite a prepared artifact, closes each object before
creating descriptors, and uses attempt-specific names. Cumulative replacement
writes new objects. Published objects are immutable under Iceberg's file model;
retention protects the physical object while any retained descriptor references
it. This does not promise detection of arbitrary concurrent out-of-band rewrites.
See the [Iceberg specification](https://iceberg.apache.org/spec/) and
[Puffin specification](https://iceberg.apache.org/puffin-spec/).

## Measurement method

`crates/compactor/tests/puffin_metadata.rs` writes real multi-blob Puffin objects
using Flow's v3 `DeleteWriter`. An in-process HTTP server serves the resulting
bytes to the production FileIO/resolving OpenDAL/S3 adapter. The server counts
actual HEAD and bounded GET requests and returns real range bytes. HTTP
connections persist. These are protocol fixtures, not AWS or MinIO measurements.
The public `stage_delete_files` entry point runs the real loader and RocksDB
scratch writes; assertions compare every resulting RowLocation to the input.
There are no data Parquet reads or catalog requests in the timed region.

The focused benchmark uses 64 vectors in two objects (32 each), two warmups and
nine measured iterations per case. Target cardinalities are `base + target_index`,
with every second row deleted. The cases are:

- base 16, no artificial delay: 3,040 positions;
- base 16, 2 ms server delay per HEAD/GET: the same 3,040 positions;
- base 4,096, no artificial delay: 264,160 positions.

Timers separately cover delete loading/staging, copying staged positions to an
epoch in RocksDB, and reading that epoch/rebuilding DVs into memory. The latter
two reproduce the cumulative conversion work without catalog publication or
object upload. Neither the sum nor any one timer is full `other_prepare` or
end-to-end publication throughput. Added server delay is a sensitivity experiment;
Tokio timer granularity means it is not an exact simulated network RTT.

Commands and results follow after the baseline and candidate runs.

## Follow-up candidates (outside this patch)

`load_deletes` expands `DeleteVector::iter()` into full `RowLocation` values and
calls `StateStore::put_position_deletes` in batches. That method builds keys with
the target path and position and serializes each location. Publication reads those
values from its `prior-deletes` scan, writes them to the epoch, and discards the
scan. `delete_batches` reads/deserializes the epoch again; `VectorWriter::write`
reconstructs a bitmap before encoding it. The state store provides sorted union,
bounded batching and recovery behavior, so bypassing it is an architectural
change requiring independent correctness work.

The four GETs per descriptor also reread and parse the same header/footer.
For 64 vectors that means 256 ranges, of which 192 fetch framing/metadata.
Reusing validated Puffin framing/footer per physical object within one scan is
an obvious candidate. Blob reads could then be coalesced when adjacent, but must
retain memory/read budgets, cancellation and per-blob identity/CRC checks. The
generic `PuffinReader` has a per-instance footer cache, but this specialized
bounded DV reader does not use it; substituting the generic reader without its
limits is not a safe trivial change. Neither redesign belongs in this patch.

## Baseline observations, before implementation

Six HTTP-backed regression tests passed on unchanged production code. Three
vectors sharing one object issued **6 HEADs and 12 range GETs**. Five vectors
sharing three objects issued **10 HEADs and 20 range GETs**. Later descriptors
with conflicting manifest sizes and a corrupt second blob were rejected.
Repeated scans and a failed-HEAD-then-retry case passed as well.

The baseline focused benchmark completed successfully. Medians in milliseconds:

| Base cardinality | Server delay/request | HEADs | Ranges | Range bytes | Load/stage | Scratch copy | Rebuild | Memory-only read/decode |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 16 | 0 ms | 128 | 256 | 421,024 | 425.474 | 52.992 | 26.527 | 13.842 |
| 16 | 2 ms | 128 | 256 | 421,024 | 2,227.967 | 59.112 | 26.155 | 30.361 |
| 4,096 | 0 ms | 128 | 256 | 949,568 | 2,442.796 | 3,224.982 | 1,415.203 | 16.115 |

An independent probe of the same 128 serial HEADs (two warmups, nine samples,
no artificial delay) measured **129.457 ms median**, range 105.104–170.000 ms.
This is a separate measurement, not a directly attributed fraction of the loader.
It establishes nontrivial request cost before changing the implementation.

Host: x86_64 Linux, Intel i5-1135G7 (4 cores/8 threads), 7.4 GiB RAM; Rust 1.97.1.
These are the repository's **unoptimized test profile** measurements on an
unreserved host, not release-build production capacity numbers. Dependencies
were built offline before timing. Full PostgreSQL/REST/MinIO ingestion,
`other_prepare`, mutations/s and cloud performance were not measured. The
published v3 benchmark is historical context, not this baseline.

Raw local logs are retained under `target/puffin-baseline-tests.log`,
`target/puffin-baseline-benchmark.log` and `target/puffin-baseline-head-cost.log`.

Decision: implement a scan-local size reuse and feed that observed size to the
bounded Puffin reader. Both metadata calls must be addressed; then repeat the
same workload and retain all validation. Do not change cumulative representation
or range reads in this patch.
