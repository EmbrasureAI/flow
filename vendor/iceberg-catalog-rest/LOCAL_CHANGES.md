# Local changes to iceberg-catalog-rest 0.10.1

This package is modified by Embrasure Flow. It is not an unmodified ASF release.
The original Apache LICENSE, NOTICE, source headers, and `.cargo_vcs_info.json`
are retained.

- Original archive: https://static.crates.io/crates/iceberg-catalog-rest/iceberg-catalog-rest-0.10.1.crate
- Archive SHA-256: `d1500a26a9b18f286a319e914ce7290dddb7e2eef810edf535afc4ce0c8a6f45`
- Upstream commit: `04ae06bdb15a6fd7c7927d29d4e0f6a33de0f1f9`
- Upstream tree: `crates/catalog/rest`

Marks HTTP 408, 429, and server failures as retryable, while retaining ambiguous
commit-state errors. OAuth errors retain this classification for both catalog JSON
and empty/proxy responses; bad-credential responses remain permanent. Adds opt-in
bounded-label REST metrics with structural route matching so identifier values
such as `config` or `namespaces` do not change endpoint attribution. Changed files:
`Cargo.toml`, `Cargo.toml.orig`, `README.md`, `src/catalog.rs`, `src/client.rs`,
`src/lib.rs`; `src/observation.rs` is a local Apache-2.0 addition.

Table creation forwards the typed `TableCreation.format_version` as the reserved
REST `format-version` property. The typed field wins over a conflicting raw
property, and unrelated properties are preserved. HTTP request regression cases
cover default v2, explicit v3, and a conflicting property. This correction changes
`src/catalog.rs` and `README.md`.

Response diagnostics omit OAuth and catalog response bodies, which can contain
tokens or vended storage credentials. JSON decoding errors retain only category,
line and column, since serde's original message can also quote secret values.
OAuth failure status continues to determine retryability without logging the
server's free-form error message. Workspace daemon HTTP regressions cover these
paths through the public catalog API. This correction changes `src/client.rs`.

From an extracted crate, apply these repository patches in order (use absolute
patch paths):

```sh
patch -p1 < docs/patches/iceberg-rest-retryable-status.patch
patch -p3 < docs/patches/iceberg-rest-observability.patch
patch -p3 < docs/patches/iceberg-rest-format-version.patch
patch -p3 < docs/patches/iceberg-rest-response-redaction.patch
```

The patches include prominent local-modification notices. This provenance file is
added separately. The crate's original Cargo.lock is omitted because the repository
uses its root Cargo.lock. No upstream submission is claimed.
