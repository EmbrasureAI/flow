# Same-commit manifest packing

This candidate reduces metadata fanout when a rewrite consumes every live entry
in several manifests. It preserves the current snapshot's deletion records and
packs them alongside additions in the new manifest for each content type. The
supported view already requires the current unpartitioned spec. No extra catalog
commit, data rewrite, retention change or configuration option is introduced.

## Evidence and standard semantics

The [release37 exact reader pins](release37.md#exact-pinned-reader-follow-up)
identify nine current-snapshot deletion-only manifests among 18 manifests for
`events_25`. The other files include six L0s that ordinary maintenance consumes
about nine seconds later. The immediate metadata fanout is a distinct cost from
that pending data work.

Current deletion entries must remain available to snapshot change APIs.
[Java's manifest merger](https://github.com/apache/iceberg/blob/3a68b70bfb1e4ca6e9386271e262f48596cd6c44/core/src/main/java/org/apache/iceberg/ManifestMergeManager.java)
combines current-snapshot ADDED and DELETED entries while preserving their status.
[BaseSnapshot](https://github.com/apache/iceberg/blob/3a68b70bfb1e4ca6e9386271e262f48596cd6c44/core/src/main/java/org/apache/iceberg/BaseSnapshot.java)
enumerates removals from manifests attributed to that snapshot. Merely omitting
the nine manifests would lose this information.

The implementation first classifies affected manifests without writing files.
Untouched manifests retain their identity. A manifest with both removed and
surviving entries follows the existing rewrite path. Fully removed manifests
contribute references to a per-content group, bounded by the operation's removed
file set. Each group shares the existing new output manifest, including when it
contains only deletions. Original data/file sequences and complete file payloads
are retained; only deletion status and snapshot attribution change as before.
The exact number of output manifests and the manifest list are registered before
any writer is opened. Existing catalog CAS, snapshot summaries and durable
recovery boundaries are unchanged.

## Regression gates

The isolated pre-change control at commit `c2622a1` fails the extended transaction
test at the intended output bound: seven manifests instead of four. Its log is
`target/release38-negative-control-02.log`. The initial attempt failed to compile
because the test needed to collect an iterator; it is retained separately in
`target/release38-negative-control.log` and is not behavioral evidence.

The changed default-feature workspace passes 154 tests across 40 binaries, with
one manual profile ignored (`target/release38-workspace-tests.log`). Strict Clippy
(`target/release38-clippy.log`), formatting and diff checks pass. Two existing
integration tests were extended:

- The transaction test combines wholly removed data and delete manifests with a
  mixed survivor/removal manifest and a disjoint append. It verifies exact removed
  entries, current snapshot attribution, original sequences, content/spec integrity,
  unchanged manifest reuse, one-commit history and idempotent recovery. A deletion
  group without replacement files still produces its required manifest.
- The real Parquet/shared-delete test reduces six manifests to three while
  preserving every removed delete entry. Existing exact stock-reader rows,
  historical reads, lost-response recovery and index reconstruction checks pass.

## Frozen optimized service gate

The artifact was built from clean commit
`3ef7335a0b31af679d8a29d67946825e7f186aad`, tree
`88cee298bdb64e66664c12691960eeeddcbe3604`, with Rust 1.97.1, thin LTO and
the release profile for `aarch64-apple-darwin`. Its manifest is
`target/qualification-binaries/release38/build-manifest.json`.

- Daemon SHA-256: `8d2c7a51ea9fd60f6848cf8c68af9873c196622f42bf479cbc6c713b0ef00a2a`.
- External compactor SHA-256: `776710a6edb2cdee3f5fff225cb90cb21594266fad817b8af7167706d633cbce`.
- Cargo.lock SHA-256: `2d84ac4f5caa3d832aa41c8020efcb083513c12e936309bd9d9259f8021aa8af` (unchanged).

All 30 phases pass in `target/release38-campaign/summary.json`. The serial runner
verifies frozen binary, manifest and service harness hashes before each case.
No builds or competing workloads ran during the campaign.

| Case | Phases | Elapsed | Report SHA-256 |
| --- | ---: | ---: | --- |
| Two held table builds | 6 | 71.910 s | `2cb266aa5b6427af1111d1b5a37a353d4156800256b77b0a4370813adcf49d9c` |
| Periodic cleanup with two hot tables | 5 | 99.075 s | `98f3bfa9c2a38f7e5f2eae1f68022309043eec3c5e1ade6dcd9266166a0e5826` |
| Concurrent compaction and recovery | 7 | 174.417 s | `86b11f47ff32ede98634443952133b3cbe3534689cb6ee221ed32019b5c31faf` |
| Durable service faults | 12 | 74.190 s | `d6b18e67dbcaaf636b907ae7fe4c519efa6bd43f028e3a5234b38bbb63fd3cf8` |

These cases cover CRUD and key moves during held compaction I/O, cancellation,
restart, exact current/history reads, periodic maintenance, lost commit responses,
service outages, physical index loss and external compactor reconciliation. The
fault compactor requires three supervisor restarts across injected outages;
passing recovery does not establish uninterrupted availability.

## Direct Java removal APIs

After the service campaign, a separate read-only check executes Iceberg Java
1.10.1 `Snapshot.removedDataFiles` and `removedDeleteFiles` through pinned Spark
3.5.6. It verifies three native compaction snapshots from the parallel fixture:
`8431542721667219483`, `2400729280389008832` and `2193834532588046375`.
Each returns exactly one removed data file and one removed position-delete file.
Paths, row counts, sizes and original data/file sequence numbers match raw Avro
entries and snapshot summaries. Both combined addition/deletion manifests and
deletion-only groups are exercised.

The report is `target/release38-java-removals/results/report.json`, SHA-256
`540fa509ba28de5a90367eee90f32088e76c54c4aa8d2ae6aac22df4391fb617`.
It records the container digest, JAR hashes, scripts and immutable input objects;
the table metadata SHA-256 is
`7db9b2f7d4c55f2f4227d5dd887f9556db4436b07c21dd151632365bb8d052f2`.
The container exits before the performance workload starts. This verifies the
snapshot change APIs directly; it is not a new full Spark reader qualification.

## Matched eight-worker diagnostic

`target/release38-source-diagnostic` repeats release37's instrumented 100-table,
10,000-mutations/s case with 10 seconds of warmup and 60 measured seconds.
Both admit all 1,000 warmup and 6,000 measured transactions, publish 1,750
four-transaction epochs, and verify 1,070,000 final rows with exact source ACK.
`comparison-identity.json` verifies all 100 final row digests, normalized
configuration and commands, harness/wrapper hashes, clean frozen sources and
matching build settings. `compare.py` reproduces the comparison.

Both the daemon and independent reference compactor use the changed publication
library. The compacted reference is regenerated with the new compactor, so this
is not a comparison against unchanged reference metadata. Native maintenance
runs during ingestion; the independent compactor runs afterward for the reference.

| Measure | Release37 | Release38 |
| --- | ---: | ---: |
| Catalog p95 / p99 | 708.200 / 822.698 ms | 837.772 / 1,051.664 ms |
| Drain after measured interval | 0.474 s | 1.001 s |
| Sampled process RSS peak | 517,996,544 bytes | 463,470,592 bytes |
| Maximum reader ratio | 1.684 | 1.763 |
| Background / synchronous rewrites through ingestion | 315 / 73 | 359 / 61 |

Catalog latency meets the 1/2-second targets in this finite diagnostic, but the
reader ratio still exceeds 1.25. Both runs exit nonzero for failed qualification
while correctness and diagnostic completion pass. Packing preserves removal
semantics and bounds metadata output; this result does not establish an overall
reader performance improvement. Sampled RSS can miss peaks, and the sampler
changes workload overhead.

The source report SHA-256 is
`bf72c3501a2b2621da54e38bbf7e8bee55816b849b1e621dceedeb9a733709f6`;
the release37 baseline is
`74dfd216d1220f86f68897d191e409c27ac97d0bb739282b2eca8c333bd15f30`.
The 50k throughput, reader-health and 24-hour production gates remain unmet.
Earlier missed-arrival failures retain their original scope and are not replaced
by this sampled diagnostic.

## Exact pinned reader follow-up

`target/release38-reader-assessment/results/assessment.json` verifies immutable
metadata hashes and snapshot IDs for all 100 original/reference table pairs in
both releases. Original manifests fall from 1,026 to 619 and reference manifests
from 658 to 195. Original live data files increase from 423 to 447 while delete
files decrease from 190 to 167. Both references have 100 data files and no deletes.
Packing therefore reduces metadata fanout on both sides; the worse ratio alone
does not show that this metadata reduction has no benefit.

With all writers and builds stopped, DuckDB 1.5.5 repeats full and selective
queries on `events_41`, release38's worst benchmark table, against its exact pins
and the corresponding release37 pins. Release38 has 15 manifests, nine data files
and six delete files versus two manifests, one data file and no deletes in its
reference. Twenty alternating pairs follow two warmup pairs, with unprofiled
wall measurements separated from detailed planning profiles.

| Measure on `events_41` | Release37 | Release38 |
| --- | ---: | ---: |
| Full-query native / reference wall median | 5.077 / 4.815 ms | 5.723 / 3.306 ms |
| Full-query wall ratio | 1.054 | 1.731 |
| Selective wall ratio | 1.088 | 1.789 |
| Full-query planning ratio | 1.032 | 1.580 |
| Selective planning ratio | 0.990 | 1.709 |

All aggregates match: 10,720 full rows and 91 selected rows for IDs 1–100.
Both releases pass the separate 2.0 planning bound; release38's full-query wall
ratio still fails 1.25. Reference wall time also improves, consistent with its
smaller metadata layout. These are warmed reads of small tables, not sufficient
evidence for changing general maintenance defaults. Planning excludes parsing
and client overhead; operator timings are neither physical I/O wall time nor CPU
measurements. The retained reports include the exact scripts, settings and pins.
