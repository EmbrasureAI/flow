# Embrasure Flow transport patch

Vendored from iambriccardo/rust-postgres at c4b8de06aaa99f71800126bedcca6e623d368357 (tokio-postgres 0.7.18). Upstream license files are retained. Source is unchanged except:

- Config accepts an optional maximum backend frame payload size.
- connect_raw passes that limit to PostgresCodec.
- PostgresCodec rejects an advertised payload exceeding the limit immediately after parsing its five-byte header, before waiting for its body.
- A shared admission-failure flag preserves the InvalidData cause in Client/Responses and CopyBothDuplex stream/sink errors when the failed driver closes their channels. This prevents oversized frames from looking like ordinary EOF or reconnectable connection loss.

The default remains unlimited for upstream compatibility. Flow sets the limit on all source connections, covering SQL COPY and replication CopyData. The existing logical-message limit still applies after decoding. Cargo paths to sibling crates are replaced by the same pinned git revision; upstream development dependencies and absent benchmarks are omitted. Wire regression tests live in crates/pg-source/tests/transport.rs.

The [replayable patch](../../docs/patches/tokio-postgres-admission.patch) applies with `patch -p1` inside the upstream `tokio-postgres` directory at the revision above. The vendored copy omits upstream tests and benchmarks; workspace wire tests exercise the patch.

Packaging differences from the upstream `tokio-postgres` directory: `README.md` is a verbatim copy of the fork's repository-root README (created by the patch above), so it describes the repository as the "Materialize fork of Rust-Postgres" rather than documenting this package. The crate's `CHANGELOG.md` is omitted. `LICENSE-APACHE` and `LICENSE-MIT` are copies of the files that upstream links from the repository root, and `.cargo_vcs_info.json` records the revision and path above.
