# Local changes to iceberg 0.10.1

This package is modified by Embrasure Flow. It is not an unmodified ASF release.
The original Apache LICENSE, NOTICE, source headers, and `.cargo_vcs_info.json`
are retained.

- Original archive: https://static.crates.io/crates/iceberg/iceberg-0.10.1.crate
- Archive SHA-256: `6e50fb53c7480f414911ab76b6e14c0042d54ad5c30f6ac2bf2f52a487a25716`
- Upstream commit: `04ae06bdb15a6fd7c7927d29d4e0f6a33de0f1f9`
- Upstream tree: `crates/iceberg`

Exposes the typed table-commit builder, snapshot-reference accessors, and explicit
snapshot-retention protection. Protection takes precedence over explicit head
validation. Expiration asserts every observed reference to detect concurrent head
moves/removals and replan. These REST requirements are not metadata CAS: callers
must exclude concurrent reference creation and retention-policy mutations through
catalog permissions or coordination. Changed files: `src/catalog/mod.rs`,
`src/spec/table_metadata.rs`, `src/transaction/expire_snapshots.rs`.

The separate reader correctness patch scopes cached delete vectors to each input
delete file and unions only the deletes selected for the current scan task. It
checks cached membership, unions borrowed bitmaps without cloning each vector,
publishes loaded position vectors atomically, and registers completion notification
while holding the loader state lock. This prevents cross-file delete leakage and
a missed wakeup when a concurrent load completes before the waiter is polled.
Claimed positional loads own a completion guard that publishes terminal failure
on I/O/parse errors or cancellation; waiters recheck that result before proceeding.
The returned receiver also bounds the spawned loader's lifetime, cancelling its
buffered work when the scan drops it. Equality predicate publication records a
terminal error when its sender is dropped, preserves the claimed load's notifier,
and wakes registered waiters; current and later readers receive that error.
Changed files: `src/arrow/delete_filter.rs`,
`src/arrow/caching_delete_file_loader.rs`, `src/delete_vector.rs`.

The separate Parquet writer patch exposes `flush_row_group()` without closing the
file. Callers can bound logical input bytes before appending another batch,
independently of the writer's encoded-size estimate. File statistics omit a bound
when any non-null row group has missing or inexact statistics; short exact groups
cannot mask longer values in truncated groups. Existing files must be rewritten
to correct previously published bounds. Changed file:
`src/writer/file_writer/parquet_writer.rs`.

The separate UUID schema patch preserves Arrow UUID extension metadata and
recognizes it when converting Arrow schemas back to Iceberg, including list
elements, map keys/values and struct children. Changed file:
`src/arrow/schema.rs`. See `docs/patches/iceberg-uuid.patch`.

The v3 patch adds bounded Puffin deletion-vector encoding and reading (including
LZ4 footers), carries blob descriptors through scan tasks, and scopes shared
reader state to logical blob identity. Scans apply one cumulative DV per target,
suppress legacy position deletes for that target, and validate row bounds.
Manifest loading resolves inherited row IDs before entries are copied into new
manifests. The patch adds `crc32fast` and `lz4_flex` as direct dependencies;
both were already dependencies in the workspace lockfile. See
`docs/patches/iceberg-v3.patch` for the complete file list and changes.

From an extracted crate, apply these repository patches (use absolute patch paths):

```sh
patch -p1 < docs/patches/iceberg-table-commit.patch
patch -p1 < docs/patches/iceberg-delete-cache.patch
patch -p1 < docs/patches/iceberg-parquet-row-group.patch
patch -p1 < docs/patches/iceberg-uuid.patch
patch -p1 < docs/patches/iceberg-v3.patch
```

The patches include prominent local-modification notices. This provenance file is
added separately. The crate's original Cargo.lock is omitted because the repository
uses its root Cargo.lock. No upstream submission is claimed.

The source-nullability patch adds a root-field `make_column_optional(field_id)`
schema action. It preserves IDs/types/defaults and rejects identifier fields,
missing fields and nested paths. Existing current-schema requirements still
fence concurrent catalog schema edits. Changed file:
`src/transaction/update_schema.rs`.
