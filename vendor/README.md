# Vendored dependencies

This directory contains Apache Iceberg Rust 0.10.1 and its REST catalog. The
workspace uses them through `[patch.crates-io]` because the service needs changes
not available through the released extension interfaces.

- [Iceberg changes](iceberg/LOCAL_CHANGES.md)
- [REST catalog changes](iceberg-catalog-rest/LOCAL_CHANGES.md)
- [Reference review and patch inventory](../docs/references-iceberg.md)
- [Prepared upstream submissions](../docs/patches/upstream/README.md)

Preserve the upstream licenses, notices and source provenance when updating
these dependencies. Generated distribution notices are handled by
[the license collector](../licenses/README.md).
