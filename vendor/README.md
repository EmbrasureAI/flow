# Vendored dependencies

This directory contains Apache Iceberg Rust 0.10.1, its REST catalog and OpenDAL
storage adapter, and the pinned PostgreSQL transport. Cargo patches select these local copies for changes
not available through their released extension interfaces.

- [PostgreSQL transport changes](tokio-postgres/LOCAL_CHANGES.md)
- [Iceberg changes](iceberg/LOCAL_CHANGES.md)
- [REST catalog changes](iceberg-catalog-rest/LOCAL_CHANGES.md)
- [OpenDAL storage changes](iceberg-storage-opendal/LOCAL_CHANGES.md)
- [Iceberg reference review](../docs/references-iceberg.md) (the `LOCAL_CHANGES.md` files above are the patch inventory)
- [Prepared upstream submissions](../docs/patches/upstream/README.md)

Preserve the upstream licenses, notices and source provenance when updating
these dependencies. Generated distribution notices are handled by
[the license collector](../licenses/README.md).
