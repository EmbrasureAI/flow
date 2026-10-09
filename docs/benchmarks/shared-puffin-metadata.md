# Shared-Puffin metadata investigation

## Source baseline and path

Baseline: upstream `main` at `2ed96d7bddd2294c9d3afd501ab217be1d1a528a`, fetched
2026-10-04. The path below describes the baseline before metadata reuse.

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

Commands and fresh baseline/candidate results are recorded below.

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

## Implementation and safety

`load_deletes` now holds a `HashMap<&str, u64>` for Puffin paths for the duration
of one call. Only a successful storage metadata observation enters it. Every
logical descriptor still compares its manifest size with the observed value,
including descriptors encountered after a cache hit. Parquet handling is unchanged.

The bounded reader now exposes `read_deletion_vector_with_size(&InputFile, ...)`.
The existing `read_deletion_vector` entry point still observes metadata and keeps
its pre-I/O validation order, then delegates to the same reader. The compactor
passes the already observed size, not a trusted manifest size. This removes the
second HEAD without duplicating or bypassing validation. The vendor provenance
and replay patch are included.

- Correctness: applicability, sequence filtering, row/byte budgets, cancellation,
  range bounds, footer matching, checksum, cardinality and row-position checks
  remain in their existing order in the loader/reader.
- Freshness: the map is destroyed when the scan returns or fails. A later scan
  observes storage again; there is no process-wide or publication-attempt cache.
- Retries: OpenDAL's request retry behavior is unchanged. Failed observations
  return their original error. A new scan starts fresh and discards prior scratch
  state. HEAD counts in this report are for successful requests without retries.
- Immutability: within-call reuse assumes published objects are immutable, as
  the writer and Iceberg model require. Out-of-band replacement during the scan
  is outside that contract; neither the old HEAD/GET sequence nor this patch
  provides transactional reads of mutable objects. Same-size corruption remains
  subject to the unchanged framing/CRC/identity checks.
- Memory: O(unique Puffin paths), with borrowed path strings and one u64 size per
  entry plus HashMap allocation overhead. No blobs or footer payloads are cached.
- Error handling: the manifest-size mismatch text remains exactly
  `delete file size differs from manifest`, even for a later shared descriptor.
  The failed-HEAD and corrupt-later-blob tests exercise error propagation.

New tests in `crates/compactor/tests/puffin_metadata.rs`:

| Test | Evidence |
| --- | --- |
| `shared_puffin_counts_backing_requests_and_decodes_every_blob` | Three distinct offsets and lengths in one writer-produced object; one HEAD, twelve GETs, exact positions. |
| `multiple_puffin_files_count_backing_requests` | Five vectors distributed 2/2/1; three HEADs, twenty GETs, exact positions. |
| `every_descriptor_must_match_the_observed_object_size` | Wrong manifest sizes on the first and a later descriptor preserve the existing error. |
| `a_new_scan_rechecks_metadata_and_replaces_scratch_positions` | Repeated calls each issue their own HEAD and retain the same exact staged positions. |
| `metadata_failure_can_be_retried_by_a_new_scan` | HTTP 404 propagates before any GET; the next successful call observes metadata and loads all positions. |
| `corrupt_blob_is_still_rejected` | A checksum error in the second blob is rejected after metadata has already been reused. |
| `shared_puffin_preparation_benchmark` (ignored) | Repeatable before/after staging, copy, rebuild and memory-only decoding measurements. |
| `baseline_metadata_request_cost` (ignored) | Independent cost of the original 128 HEAD requests through the actual S3 adapter. |

## Review fixes and reproduction

The original observed-size patch used repository-relative paths, which failed
with the documented `patch -p1` invocation from an extracted crate. Its paths
now match the other vendor patches (`a/src/...`, `b/src/...`). Applying it with
`--fuzz=0` to the pre-optimization sources reproduces both current vendored
files byte-for-byte.

The HTTP-backed regression suite also checks an actual physical-size change
between scans, recovery after a later blob fails with partial scratch state,
and validation parity between the original and observed-size reader APIs.
The original API still performs a HEAD on every call; the observed-size API
performs none. Both reject invalid ranges, target/cardinality mismatches and
resource-limit violations, with argument validation preceding all I/O. These tests live in the workspace so normal CI runs
them; vendored-crate unit tests are excluded from `cargo test --workspace`.

Run correctness checks and timings separately; concurrent builds and test runs
can distort the benchmark. Use a separate Cargo target directory per checkout.
If sharing one, run `cargo clean -p iceberg -p flow-compactor` before switching
between baseline and candidate to prevent reuse of the other checkout's build
artifacts. The candidate benchmark asserts its HEAD/GET counts before reporting
timings.


```sh
cargo test --locked -j 2 -p flow-compactor -p flow-materializer -p flow-coordinator
cargo test --locked -j 2 -p flow-testkit --test vendor_deletion_vectors --test v3_pipeline
cargo clippy --locked -j 2 -p flow-compactor -p flow-materializer -p flow-coordinator --all-targets --no-deps -- -D warnings
cargo fmt --all --check
cargo test --locked -j 2 -p flow-compactor --test puffin_metadata shared_puffin_preparation_benchmark -- --ignored --exact --nocapture --test-threads=1
cargo test --locked -j 2 -p flow-compactor --test puffin_metadata baseline_metadata_request_cost -- --ignored --exact --nocapture --test-threads=1
```

The HTTP fixture requires permission to bind a loopback socket. No external
object store or credentials are needed.

## Fresh comparison, 2026-10-09

Rebuilt baseline production sources from `c4d5c73` and candidate sources from
`ff50fd6` with the review tests. Both used the same HTTP fixture and timing
workload, including the fixture's object-replacement lock. The baseline omitted
the new-API-only regression test because that symbol does not exist there.
Changed-package artifacts were cleared between builds. A trial candidate run
that reused baseline artifacts failed the request-count regressions; its timings
were discarded and both versions were rebuilt. The candidate benchmark now
asserts request counts before reporting timings.

Two warmups and nine measured iterations per case, run serially on the same
host/toolchain/test profile described above. Median load/stage time in ms:

| Base cardinality | Delay/request (ms) | Baseline | Candidate |
| ---: | ---: | ---: | ---: |
| 16 | 0 | 341.330 | 239.069 |
| 16 | 2 | 1593.597 | 1108.206 |
| 4096 | 0 | 2205.077 | 2027.408 |

All three workloads went from **128 HEADs to 2**. Both versions issued 256 range
GETs and read identical bytes: 421,024 for the small workloads and 949,568 for
the large workload. Every iteration verified the exact staged positions.
The independent 128-HEAD probe measured **80.533 ms median** in this run.
The PR description's 16.1 ms isolated-HEAD claim is not supported by the recorded
baseline or this rerun and should be replaced with these measured results.

These are local observations, not production capacity estimates. Large-workload
sample ranges overlap; the runs do not establish a robust speedup for that case.
RocksDB scratch copying and bitmap reconstruction remain substantial costs.
No AWS/MinIO, release-profile, ingestion-throughput, or full-publication benchmark
was run. The artificial delay case demonstrates sensitivity to metadata latency.

Validation: 67 affected-crate tests and 10 v3 pipeline/vendor-reader integration
tests passed. All nine HTTP regressions and both normally ignored measurement
tests then passed in the final serial run. Formatting and affected-crate Clippy
passed. The separate standalone vendored-crate unit suite was not run; workspace
integration tests exercised both public reader APIs. One unrelated ignored
benchmark in the affected-crate suite was not run.

Raw local evidence: `target/puffin-review-baseline-benchmark.log`,
`target/puffin-review-candidate-benchmark.log`, `target/puffin-review-tests.log`,
`target/puffin-review-v3.log`, and `target/puffin-review-clippy.log`.
