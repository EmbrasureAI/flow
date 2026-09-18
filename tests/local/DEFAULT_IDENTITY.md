# DEFAULT replica identity

Mutable tables must have a primary key. FULL continues to support the existing
source type mappings. DEFAULT is eligible only when every **selected** column is
one of PostgreSQL's fixed-width supported builtins: bool, int2, int4, int8,
float4, float8, date, time, timestamp, timestamptz, uuid; an enum; or a domain
recursively based on an eligible type. Existing restrictions on key types and
nullability still apply. All primary-key columns must be selected.

This intentionally excludes all variable-width types, including numeric,
bounded varchar, text, bytea, JSON, arrays and vector. Small current values,
length modifiers, and SET STORAGE PLAIN are not eligibility proofs. Changing
storage settings does not rewrite previously stored external values. The policy
uses the intrinsic PostgreSQL type representation, not sampled row values.
Unselected variable-width columns do not block an explicit projection.

PostgreSQL references: [TOAST storage](https://www.postgresql.org/docs/16/storage-toast.html)
and [logical message formats](https://www.postgresql.org/docs/16/protocol-logicalrep-message-formats.html).

The engine checks this rule during source/schema validation, including snapshot,
reconnect and relation changes. Product preflight uses the same resolved type
rule. NOTHING and USING INDEX are not enabled. Append-only behavior is unchanged.
No source DDL or ingestion-mode changes are performed by the implementation.

Key tuples retain relation column positions: only primary-key fields are decoded,
in configured key order. Non-key NULL placeholders never become replacement
row values. Updates without an old tuple derive the unchanged key from the new
row. A changed key uses the existing old-key delete/new-row insert application.
Only FULL may fill an unchanged-TOAST marker from its complete old tuple; all
unresolved markers fail. Invalid events cannot produce a successful capture
commit. The materialized checkpoint and source acknowledgement remain behind
quarantined/unapplied changes; the ingress journal may safely retain later WAL.

## Repeatable local qualification

From the **internal engine worktree** matching the product's local source pin:

```sh
tests/local/default_identity.sh /absolute/path/to/embrasure-product /tmp/new-default-cdc-run
```

Requires Docker Compose. The script builds the product's pinned engine with its
production Dockerfile and jemalloc feature, builds its actual runner image, then
adds only test reader dependencies in a separate layer. The product source pin
must point to a commit available in this checkout. This never fetches an
unmodified engine image as a substitute for the changed source.

The fixture uses PostgreSQL 16 logical replication, MinIO, Apache Iceberg REST
and stock DuckDB Iceberg scans. Real product catalog discovery/preflight runs
inside the runner image. Engine lifecycle is driven directly by the test; hosted
API authentication, Temporal scheduling and managed repository persistence are
not part of this qualification. Existing product unit tests cover the unchanged
supervisor/configuration path separately.

Each run creates a unique Compose project with dynamically allocated host ports.
It removes only that project's containers/volumes on exit, retaining reports,
logs, source revisions/diffs, image identities and the daemon's persisted state
in the supplied new artifact directory. It does not deploy, push or publish.

Coverage includes exact snapshot-to-CDC convergence during writes, composite
keys in non-attribute order, old-key moves, repeated row changes within/across
transactions, rollback, NULL, projection excluding a large TOAST value, FULL
with externally stored values, two SIGKILL/recovery cycles, nullable fixed-column
addition, unsafe DEFAULT rejection, and checkpoint pinning on unsupported DDL.
The harness records failure rather than treating job/log success as row proof.
