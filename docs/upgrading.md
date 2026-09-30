# Upgrading

Flow keeps durable state in two places. The Iceberg tables are standard tables
that any compatible reader can use without Flow. The `state_dir` holds the
transaction journal, the RocksDB control store and row index, checkpoints and
the source identity. This page describes what a new release promises about
each, and how to upgrade.

## Compatibility policy

Flow follows [Semantic Versioning](https://semver.org/). While it is in beta
(0.x):

- **Patch releases** (`0.y.z` to `0.y.z+1`) run against an existing
  `state_dir`, configuration and Iceberg tables without changes. They may add
  optional settings, never required ones.
- **Minor releases** (`0.y` to `0.y+1`) may change configuration or on-disk
  state. Each [changelog](../CHANGELOG.md) entry begins with **Upgrade** notes
  saying whether the release reads older state in place, rebuilds part of it
  on startup, or requires resynchronizing the source.
- **Iceberg tables** stay readable by standard Iceberg readers across all
  releases. Flow never rewrites a table into a Flow-specific format.
- **Downgrades** are unsupported once a newer release has run against a
  `state_dir`, because it may have written newer records.

From 1.0, these guarantees apply within a major version, and only a major
release may require resynchronization.

Flow checks compatibility instead of assuming it. Versioned records it cannot
read stop startup with an explicit error, and the row index can be rebuilt
from the Iceberg tables when its encoding changes. An upgrade that cannot
proceed stops before it acknowledges or publishes anything.

## Upgrade procedure

1. Read the changelog entries between your version and the target, starting
   with their **Upgrade** notes.
2. Stop the daemon with SIGTERM and wait for it to exit. Readiness clears
   before shutdown.
3. Copy the stopped `state_dir` somewhere safe. Keep the copy until the new
   version has run successfully.
4. Replace the binary or image tag. Keep the same configuration unless the
   notes say otherwise.
5. Run `embrasure-flow --config flow.toml check` and `check --source`.
6. Start `run` and confirm that `status` (or `/readyz`) becomes ready and that
   the materialized watermark advances.

The source slot keeps retaining WAL while Flow is stopped, so keep the upgrade
window short or within your `max_slot_wal_keep_size` headroom.

If the new version refuses the existing state and the notes do not describe
a migration, restore the previous binary with the saved `state_dir`, or
resynchronize as described in [operations](operations.md#resynchronize-a-source).
