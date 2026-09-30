# Local changes to iceberg-storage-opendal 0.10.1

This package is modified by Embrasure Flow. It is not an unmodified ASF release.
The original Apache LICENSE, NOTICE, source headers, and `.cargo_vcs_info.json`
are retained.

- Original archive: https://static.crates.io/crates/iceberg-storage-opendal/iceberg-storage-opendal-0.10.1.crate
- Archive SHA-256: `983efbae430ed40c074af69a8b0aff99bee73e54686cf26ecde97f84127812a2`
- Upstream commit: `04ae06bdb15a6fd7c7927d29d4e0f6a33de0f1f9`
- Upstream tree: `crates/storage/opendal`

The local changes are in `src/s3_operator_cache.rs`, `src/lib.rs`, and
`src/resolving.rs`. End-to-end credential reuse and rotation tests live in
`crates/testkit/tests/s3_credentials.rs`.

The upstream S3 storage creates a new OpenDAL operator for each file operation.
Each operator starts with an empty AWS credential cache, turning object-store
traffic into ECS credential endpoint traffic. The resolving factory owns a
bounded operator cache shared by table reloads and file clones. Entries match
the full parsed S3 configuration and bucket. Different catalog factories or
custom credential providers never share entries. The existing reqsign signer
retains ownership of expiration checks and concurrent credential refresh.

The cache retains at most 128 operators and evicts entries idle for 15 minutes
or older than one hour on access. The absolute age also refreshes providers that
do not report credential expiration. It is neither serialized nor included in debug output. In-flight
operations retain their own operator clones and survive eviction.

Timeout and retry layers are applied once when an S3 operator enters the cache.
OpenDAL's timeout layer updates a shared accessor executor, so layering cached
clones on every file operation would build a recursive executor chain and could
overflow the stack during execution or shutdown. S3 cache hits return the fully
layered operator; other backends keep their existing layer path. A bounded-stack
regression repeatedly constructs S3 writers and drops the final cache owner.
Changed files: `src/lib.rs`, `src/resolving.rs`; `src/s3_operator_cache.rs` is
new Embrasure Flow code and carries its own Apache-2.0 header. See
`docs/patches/iceberg-storage-opendal-operator-cache.patch`.

`OpenDalResolvingStorage` formats its shared properties in `Debug` output as key
names with redacted values, since they carry object-store credentials. Changed
file: `src/resolving.rs`. See
`docs/patches/iceberg-storage-opendal-debug-redaction.patch`, which applies on
top of the operator cache change.

From an extracted crate, apply these repository patches in order (use absolute
patch paths):

```sh
patch -p3 < docs/patches/iceberg-storage-opendal-operator-cache.patch
patch -p3 < docs/patches/iceberg-storage-opendal-debug-redaction.patch
```

The patches include prominent local-modification notices. This provenance file is
added separately. The crate's original Cargo.lock is omitted because the repository
uses its root Cargo.lock. No upstream submission is claimed.
