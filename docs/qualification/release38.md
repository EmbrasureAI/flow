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

These raw-manifest assertions verify the metadata consumed by Java's snapshot
APIs; they do not claim that this gate directly executed those Java methods.
Frozen optimized service and performance gates are next. The independent local
compactor uses the same publication code, so its binary and compacted reference
metadata also change; the next comparison must disclose that shared change.
