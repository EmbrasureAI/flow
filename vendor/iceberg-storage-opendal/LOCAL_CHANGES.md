# Embrasure storage patch

Baseline: crates.io `iceberg-storage-opendal` 0.10.1, Apache Iceberg Rust commit
`04ae06bdb15a6fd7c7927d29d4e0f6a33de0f1f9`, `crates/storage/opendal`.
Upstream source and Apache notices are retained. The local changes are in
`src/s3_operator_cache.rs`, `src/lib.rs`, and `src/resolving.rs`. End-to-end
credential reuse and rotation tests live in `crates/testkit/tests/s3_credentials.rs`.

The upstream S3 storage creates a new OpenDAL operator for each file operation.
Each operator starts with an empty AWS credential cache, turning object-store
traffic into ECS credential endpoint traffic. The resolving factory now owns a
bounded operator cache shared by table reloads and file clones. Entries match
the full parsed S3 configuration and bucket. Different catalog factories or
custom credential providers never share entries. The existing reqsign signer
retains ownership of expiration checks and concurrent credential refresh.

The cache retains at most 128 operators and evicts entries idle for 15 minutes
or older than one hour on access. The absolute age also refreshes providers that
do not report credential expiration. It is neither serialized nor included in debug output. In-flight
operations retain their own operator clones and survive eviction.
