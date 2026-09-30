# Contributing

Open an issue to report a bug or discuss a substantial change. Include the
PostgreSQL, catalog and reader versions, relevant configuration, reproduction
steps and expected behavior. Remove credentials and source data from logs.
Report security vulnerabilities privately as described in [SECURITY.md](SECURITY.md).
Participation is governed by the [code of conduct](CODE_OF_CONDUCT.md).

## Development setup

Use the pinned Rust toolchain, a C++ compiler, libclang, CMake, pkg-config and
OpenSSL development headers. On macOS, Xcode Command Line Tools supply the
compiler and libclang. RocksDB builds its pinned native library from source.

```sh
cargo fmt --all --check
cargo clippy --locked --workspace --all-targets --no-deps -- -D warnings
cargo test --locked --workspace
```

Workspace tests use actual local files, the Iceberg memory catalog and a scripted
PostgreSQL wire peer. They do not need Docker or a live database. The
[local service suite](tests/local/README.md) and
[production fixture](tests/production/README.md) cover PostgreSQL, object storage,
standard readers and external maintenance. The [Docker demo](demo/README.md)
checks the packaged service.

## Changes and review

The [code guide](docs/code-guide.md) maps crate responsibilities, internal
modules and test locations. Start there when deciding where a change belongs.

Keep changes focused and explain the problem, resulting behavior and relevant
validation. Add user-visible changes to the `Unreleased` section of the
[changelog](CHANGELOG.md), with an **Upgrade** note for anything that changes
configuration defaults, on-disk state or required operator action; see the
[compatibility policy](docs/upgrading.md#compatibility-policy). Prefer integration checks at durable state transitions to tests that
repeat implementation details. Changes to source acknowledgement, catalog
recovery, index application or delete handling need failure-path coverage.
Documentation-only changes do not require a service benchmark.

Keep documentation focused on current behavior, design contracts and reproducible
instructions. Keep only the latest benchmark results with their limits and build
identity; development journals, agent reviews and internal build labels belong
in Git history, not the public guides.

Workers produce artifacts; coordinators publish them. Preserve complete source
transactions, bounded memory, ordinary Iceberg reads and explicit source-lineage
checks. See the [architecture](docs/architecture.md) and
[compaction protocol](docs/local-compaction.md) before changing those boundaries.

Performance claims need the environment, offered and admitted workload,
duration, publication and reader latency, read amplification and resource use.
Record missed arrivals and failed gates. Local memory-catalog timings are not
cloud publication latency. The current results are in [Performance](docs/performance.md).

## Dependencies and licensing

Contributions use the project's Apache-2.0 license. Preserve upstream notices and
modification records in `vendor/`; keep reusable Iceberg changes separable from
the service. Review new dependency terms and the
[distribution notice policy](licenses/README.md) when updating `Cargo.lock`.

Workspace crates are currently unpublished implementation components. Their
manifests disable accidental crates.io publication while the public API and
release process are being established.

## Releases

Maintainers release from `main`:

1. Set `version` under `[workspace.package]` in `Cargo.toml` and update `Cargo.lock`.
2. Rename the changelog's `Unreleased` section to `## [X.Y.Z] - YYYY-MM-DD`
   and start a new empty `Unreleased` section.
3. Merge, then push the tag `vX.Y.Z` from that commit.

The [release workflow](.github/workflows/release.yml) checks that the tag,
workspace version and changelog agree, then publishes Linux x86-64 and arm64
archives with checksums and license notices, a multi-architecture image at
`ghcr.io/embrasureai/flow`, and a GitHub release with the changelog entry.
Tags with a hyphen (for example `v0.2.0-rc.1`) are prereleases and do not move
the image's `latest` tag.
