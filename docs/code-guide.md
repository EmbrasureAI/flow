# Code guide

Start at [`crates/daemon/src/main.rs`](../crates/daemon/src/main.rs) for the CLI,
then [`runtime.rs`](../crates/daemon/src/runtime.rs) for startup, recovery and
the scheduling loop. The workspace libraries are implementation components;
their package names share the `flow-` prefix and expose their public API through
`lib.rs`.

## Crates and ownership

| Directory | Responsibility |
| --- | --- |
| [`model`](../crates/model/src/lib.rs) | Storage-independent identities, schemas, rows and source transactions. |
| [`ingress-journal`](../crates/ingress-journal/src/lib.rs) | Durable transaction frames, segmented storage and bounded replay. |
| [`pg-source`](../crates/pg-source/src/lib.rs) | PostgreSQL snapshot, replication protocol and transaction assembly. |
| [`state-store`](../crates/state-store/src/lib.rs) | Durable control records, replaceable row index and atomic delta application. |
| [`iceberg-ext`](../crates/iceberg-ext/src/lib.rs) | Iceberg commit actions, manifests and artifact tracking used by Flow. |
| [`materializer`](../crates/materializer/src/lib.rs) | Schema conversion and bounded Parquet data/delete writers. |
| [`catalog-watch`](../crates/catalog-watch/src/lib.rs) | Classification and row-identity checks for external snapshot changes. Catalog polling belongs to the caller. |
| [`compactor`](../crates/compactor/src/lib.rs) | Rewrite planning, bounded workers and late-delete translation. |
| [`coordinator`](../crates/coordinator/src/lib.rs) | Publication, maintenance, external reconciliation and source acknowledgement. |
| [`daemon`](../crates/daemon/src/main.rs) | Configuration, process lifecycle and scheduling the table/source work. |
| [`testkit`](../crates/testkit/src/lib.rs) | Shared local fixtures and integration tests spanning the libraries. |

Source capture writes complete transactions to the journal. The coordinator
collapses them, asks materializers or compactors to build artifacts, publishes
through Iceberg, and applies the resulting index changes before completing the
source ledger. Workers build artifacts; the coordinator owns publication.
See [Architecture](architecture.md) for the durable transition rules.

## Finding implementation details

- The daemon's `runtime/pending.rs` owns transaction admission,
  `runtime/table.rs` executes table work, and `runtime/health.rs` handles source
  health. The parent module owns task scheduling and completion.
- The coordinator's `publication/collapse.rs` prepares an epoch from journal
  input; `publication.rs` publishes and recovers it. `artifacts.rs` tracks object
  ownership shared by publication and maintenance. The `maintenance/` modules
  cover background builds, delete rewrites, manifests, garbage and recovery.
- In the state store, `control.rs` owns durable control state, `reader.rs` owns
  read views, `spool.rs` and `buffer.rs` collapse mutations, and `apply.rs` applies
  committed row-location changes. The root retains store setup and durable
  transition sequencing.
- The journal's `frame.rs` defines its disk format. The root owns writing,
  syncing and recovery; `chunks.rs` and `transactions.rs` provide replay readers.

Keep modules named for the responsibility they own. Use private modules and
narrow visibility for implementation details; moving a function does not make
it a new public API. Add a crate when it establishes a useful dependency or
ownership boundary, rather than to shorten a file.

## Tests and supporting files

Crate-local `tests/` directories exercise that library's public API. Cross-crate
integration tests live in `crates/testkit/tests/`; private implementation tests
stay beside their owner. The [local smoke suite](../tests/local/README.md) and
[service suites](../tests/production/README.md) use PostgreSQL, object storage and
independent readers. Performance fixtures share that service setup.

[`examples/flow.toml`](../examples/flow.toml) is the configuration example;
[`demo/`](../demo/README.md) is the packaged deployment example. `scripts/`
contains distribution tooling. [`vendor/`](../vendor/README.md) contains pinned
upstream code and its modification records. Current benchmark results and
reproduction instructions are in [Performance](performance.md).

Use the commands in [Contributing](../CONTRIBUTING.md) to build and test. For
cross-linked library documentation, run `cargo doc --locked --workspace --no-deps`
and open `target/doc/flow_coordinator/index.html`.
