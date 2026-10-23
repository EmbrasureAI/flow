# DEFAULT replica identity

Mutable tables must have a primary key. DEFAULT supports every source type
mapping that FULL supports. Existing restrictions on key types and nullability
still apply. All primary-key columns must be selected.

Under DEFAULT, PostgreSQL omits an out-of-line (TOAST) value from an update that
does not change it. Capture journals that update with an explicit unchanged
value. Collapse carries the value forward from the key's earlier image in the
epoch; publication carries the rest forward from the key's indexed Iceberg row
after its recorded fingerprint matches. The source is never queried for a value
and no placeholder is published. A primary-key change that leaves a TOASTed
value unchanged has no prior image, so that table blocks; keep FULL for tables
whose keys change.

PostgreSQL references: [TOAST storage](https://www.postgresql.org/docs/16/storage-toast.html)
and [logical message formats](https://www.postgresql.org/docs/16/protocol-logicalrep-message-formats.html).

The engine checks identity during source/schema validation, including snapshot,
reconnect and relation changes. NOTHING and USING INDEX are not enabled.
Append-only behavior is unchanged. No source DDL or ingestion-mode changes are
performed by the implementation.

Key tuples retain relation column positions: only primary-key fields are decoded,
in configured key order. Non-key NULL placeholders never become replacement
row values. Updates without an old tuple derive the unchanged key from the new
row. A changed key uses the existing old-key delete/new-row insert application.
FULL fills an unchanged-TOAST marker from its complete old tuple; DEFAULT
carries it forward as described above. A marker with no prior image fails.
Invalid events cannot produce a successful capture commit. An unsupported
change blocks its table until a resync: the target stays unchanged, and the
source acknowledgement passes the table's dropped changes.

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
addition, DEFAULT carry-forward of externally stored text across epochs and
crash recovery, and checkpoint pinning on unsupported DDL.
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
