# Local changes to iceberg-catalog-rest 0.10.1

This package is modified by Embrasure Flow. It is not an unmodified ASF release.
The original Apache LICENSE, NOTICE, source headers, and `.cargo_vcs_info.json`
are retained.

- Original archive: https://static.crates.io/crates/iceberg-catalog-rest/iceberg-catalog-rest-0.10.1.crate
- Archive SHA-256: `d1500a26a9b18f286a319e914ce7290dddb7e2eef810edf535afc4ce0c8a6f45`
- Upstream commit: `04ae06bdb15a6fd7c7927d29d4e0f6a33de0f1f9`
- Upstream tree: `crates/catalog/rest`

Marks HTTP 408, 429, and server failures as retryable, while retaining ambiguous
commit-state errors. Adds opt-in bounded-label REST metrics. Changed files:
`Cargo.toml`, `Cargo.toml.orig`, `README.md`, `src/catalog.rs`, `src/client.rs`,
`src/lib.rs`; `src/observation.rs` is a local Apache-2.0 addition.

From an extracted crate, apply these repository patches in order (use absolute
patch paths):

```sh
patch -p1 < docs/patches/iceberg-rest-retryable-status.patch
patch -p3 < docs/patches/iceberg-rest-observability.patch
```

The patches include prominent local-modification notices. This provenance file is
added separately. The crate's original Cargo.lock is omitted because the repository
uses its root Cargo.lock. No upstream submission is claimed.
