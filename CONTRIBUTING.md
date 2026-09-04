# Contributing

Use the pinned Rust toolchain, a C++ compiler, libclang, CMake, pkg-config and OpenSSL development headers. The Rust RocksDB binding compiles its pinned native library. On macOS, Xcode Command Line Tools supply libclang; set `LIBCLANG_PATH` if your compiler is installed elsewhere.

```sh
cargo fmt --all --check
cargo clippy --locked --workspace --all-targets --no-deps -- -D warnings
cargo test --locked --workspace
```

Tests use local files, the Apache Iceberg memory catalog, and a scripted PostgreSQL wire peer. They do not require Docker or access to a database. Prefer integration tests at a durable state transition over tests that repeat implementation details. Add a failure test whenever a change alters source acknowledgement, catalog commit recovery, index application or delete handling.

Keep modules organized around ownership of state. Workers produce immutable artifacts; coordinators alone publish. Do not introduce equality-delete fallbacks, custom readers, unbounded per-transaction buffers, or automatic slot resets.

Reference Moonlink documentation for design concepts only. Do not copy its BSL implementation. New contributions must be your own work or carry an Apache-2.0-compatible license. Preserve the Apache headers and provenance in `vendor/iceberg`; the intentional changes are recorded as a small patch in the reference documentation. Changes suitable for Iceberg itself should remain separable from the service.

There is no performance guarantee yet. A performance claim needs its environment, workload, elapsed duration, source commit time, catalog publication time, reader observation time, reader amplification and storage cost. Do not present local memory-catalog timings as cloud publication latency.
