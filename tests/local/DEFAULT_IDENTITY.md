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
reconnect and relation changes. NOTHING and USING INDEX are not enabled.
Append-only behavior is unchanged. No source DDL or ingestion-mode changes are
performed by the implementation.

Key tuples retain relation column positions: only primary-key fields are decoded,
in configured key order. Non-key NULL placeholders never become replacement
row values. Updates without an old tuple derive the unchanged key from the new
row. A changed key uses the existing old-key delete/new-row insert application.
Only FULL may fill an unchanged-TOAST marker from its complete old tuple; all
unresolved markers fail. Invalid events cannot produce a successful capture
commit. The materialized checkpoint and source acknowledgement remain behind
quarantined/unapplied changes; the ingress journal may safely retain later WAL.

## Engine-only run

`default_identity.py` runs against any disposable PostgreSQL/REST/MinIO
services, such as the local or production Compose fixtures. CI runs it on
PostgreSQL 14 and 18. With the [local Compose fixture](README.md#run) running
and its `AWS_*` and `FLOW_POSTGRES_URL` variables exported:

```sh
uv run tests/local/default_identity.py \
  --catalog-uri "http://$(docker compose -f tests/local/compose.yaml port rest 8181)" \
  --s3-endpoint "http://$(docker compose -f tests/local/compose.yaml port minio 9000)" \
  --binary target/debug/embrasure-flow --artifacts /tmp/new-default-identity-run
```

It creates `run/` and `identity-change/` below the artifact directory, which
must be new. Reports, logs and the daemon's persisted state are retained there.
The fixture uses PostgreSQL logical replication, MinIO, Apache Iceberg REST and
stock DuckDB Iceberg scans; the test drives the daemon lifecycle directly.

Coverage includes exact snapshot-to-CDC convergence during writes, composite
keys in non-attribute order, old-key moves, repeated row changes within/across
transactions, rollback, NULL, projection excluding a large TOAST value, FULL
with externally stored values, two SIGKILL/recovery cycles, nullable fixed-column
addition, unsafe DEFAULT rejection, and checkpoint pinning on unsupported DDL.
The harness records failure rather than treating job/log success as row proof.

### Deterministic interrupted-batch recovery

The crash case gates an actual `orders` ingestion commit in the existing catalog
proxy before forwarding it to Iceberg. Before SIGKILL it asserts that the batch's
post-commit WAL fence is durably captured, transactions remain pending, exact
`orders` rows are unchanged, and both materialized and source-confirmed LSNs are
below the batch. `pending-before-sigkill.json` records that evidence.

Only after confirming SIGKILL does the fixture release the held request. The
real catalog commits after the caller's death; restart must recover the same
prepared operation exactly once. The test checks exact rows and advancement of
both materialized and source-confirmed checkpoints beyond the original batch
fence before starting the separate WAL-backlog case. Batch and backlog LSNs are
retained separately in `report.json`; `catalog-proxy.jsonl` records the held
operation and actual upstream result.
