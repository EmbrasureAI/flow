# Local changes to iceberg 0.10.1

This package is modified by Embrasure Flow. It is not an unmodified ASF release.
The original Apache LICENSE, NOTICE, source headers, and `.cargo_vcs_info.json`
are retained.

- Original archive: https://static.crates.io/crates/iceberg/iceberg-0.10.1.crate
- Archive SHA-256: `6e50fb53c7480f414911ab76b6e14c0042d54ad5c30f6ac2bf2f52a487a25716`
- Upstream commit: `04ae06bdb15a6fd7c7927d29d4e0f6a33de0f1f9`
- Upstream tree: `crates/iceberg`

Exposes the typed table-commit builder, snapshot-reference accessors, and explicit
snapshot-retention protection. Changed files: `src/catalog/mod.rs`,
`src/spec/table_metadata.rs`, `src/transaction/expire_snapshots.rs`.

The separate reader correctness patch scopes cached delete vectors to each input
delete file and unions only the deletes selected for the current scan task. It
checks cached membership, unions borrowed bitmaps without cloning each vector,
publishes loaded position vectors atomically, and registers completion notification
while holding the loader state lock. This prevents cross-file delete leakage and
a missed wakeup when a concurrent load completes before the waiter is polled.
Changed files: `src/arrow/delete_filter.rs`,
`src/arrow/caching_delete_file_loader.rs`, `src/delete_vector.rs`.

The separate Parquet writer patch exposes `flush_row_group()` without closing the
file. Callers can bound logical input bytes before appending another batch,
independently of the writer's encoded-size estimate. Changed file:
`src/writer/file_writer/parquet_writer.rs`.

From an extracted crate, apply these repository patches (use absolute patch paths):

```sh
patch -p1 < docs/patches/iceberg-table-commit.patch
patch -p1 < docs/patches/iceberg-delete-cache.patch
patch -p1 < docs/patches/iceberg-parquet-row-group.patch
```

The patches include prominent local-modification notices. This provenance file is
added separately. The crate's original Cargo.lock is omitted because the repository
uses its root Cargo.lock. No upstream submission is claimed.
