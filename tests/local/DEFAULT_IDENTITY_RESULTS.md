# DEFAULT CDC qualification results

Local qualification completed 2026-09-17 (America/Los_Angeles).

## Implementation and branches

All work is on local `codex/default-pk-cdc` branches. Nothing was pushed,
published, merged or deployed remotely.

- Public engine: `/Users/victor/.codex/worktrees/9fff/embrasure-flow`, based on
  `b58244b49a923230bb1da37df9c31e164319bb4a`.
- Internal engine: `/Users/victor/.codex/worktrees/default-pk-internal`, based on
  the product's original engine pin `4d9a57e414666989bfff976e3d235a561c009e76`.
- Product: `/Users/victor/.codex/worktrees/default-pk-product`, based on
  `89e2feee9d88c53cc35d328b394cbd12bef2207e`.

The engine diff changes capture key extraction, identity/type validation and
safe schema-error classification/logging. The mutation model, durable key
encoding and downstream delete/insert application are reused unchanged.
The product diff updates preflight, guidance, tests and the source build pin.
The same native changes and qualification harness are in both engine repositories.

[Eligibility and repeatable command](DEFAULT_IDENTITY.md).

## Verification

- `cargo test --locked -p flow-pg-source`: **26 passed** in each engine.
  One existing PostgreSQL-dependent test is ignored in public; three are ignored
  in internal (snapshot timeout and TLS fixtures). The new CDC path is exercised
  separately against real PostgreSQL below.
- Product `PYTHONPATH=. uv run --frozen pytest -q -n 0 tests/test_warehouse_flow.py`:
  **136 passed**.
- Production-image qualification: **passed**, both the mutation/recovery/schema
  case and the separate replica-identity-change case.
- Exact initial snapshots: 10,000 DEFAULT composite-key rows, three FULL rows with
  128 KiB external TOAST values, and one explicitly projected DEFAULT row.
- Exact Iceberg/PostgreSQL row comparisons passed after snapshot-time writes,
  inserts, unchanged-key updates, composite-key moves, deletes, repeated changes
  within/across transactions, rollback, two SIGKILL/restart cycles and nullable
  fixed-width schema addition. Recovery converged to 9,092 composite-key rows,
  two FULL rows and one projected row. Background compaction remained enabled.
- Native and real product preflight rejected the bounded-varchar DEFAULT table
  with table/column-specific FULL guidance. A selected projection excluding
  large text was accepted. FULL unchanged-TOAST values remained exact.
- Unsafe added text column: failed batch LSN **33,466,200**; confirmed source and
  materialized checkpoint both **33,450,464**.
- DEFAULT-to-NOTHING identity change: failed batch LSN **35,595,568**; confirmed
  source and materialized checkpoint both **35,594,728**.
- Both failed batches left exact target rows unchanged and source acknowledgement
  behind the failed transaction after restart. The ingress journal deliberately
  retained quarantined WAL beyond the materialized checkpoint; this is not an
  applied-data checkpoint.
- Dedicated source tests cover NULL versus unchanged TOAST, incomplete key values,
  non-key NOT NULL placeholders, composite key encoding order, duplicate delivery
  after journal reopen, unsupported types/identities and preserved append-only/FULL
  eligibility. Existing FULL TOAST regression tests also passed.

## Tested build identity

Actual production engine Dockerfile: product `apps/flow/Dockerfile.engine`,
with `--features jemalloc`, source commit:
`63e0a092d931e8142c324343d04adc6afb956807`.
Later local changes are test assertions, documentation and a diagnostic comment;
the tested engine implementation is unchanged.

- Engine image: `sha256:59623ebf75a3f7ba1492e943f6cd96b126c17dffe4b4173ec48975da77ebc825`
- Production runner image: `sha256:51c0af43150b176b02065a5e5a0d52890e113451feea927f437faa51faa69f29`
- Reader qualification layer: `sha256:9ba1e9b1c869b117fb9268830b216000fea4b8ed8a7ec89e0fb7b242551c4d7f`
- Executed binary SHA-256: `f8c0b8714ddf2fdcfe9f895f46c0534b942067fe0376409784e6499ac417af4e`
- Executed harness SHA-256: `96150fed1833758b52e6acc2a2e7f443216b110f228ce2a416c215f6e4a674b1`
- PostgreSQL **16.15**; DuckDB **1.5.5**, actual Iceberg REST and MinIO services.

Evidence is retained at `/tmp/flow-default-pk-e2e-03`: `images.json`, source
revisions/diffs, build logs, both `report.json` files, daemon logs, product preflight
reports, and persisted state. All fixture containers/network/volumes were removed;
existing services were untouched. Earlier attempts remain in sibling `-01` and
`-02` directories: one fixture chunk limit was too small for the FULL test value;
one assertion incorrectly treated a compaction snapshot change as changed rows.
Both harness issues were corrected before the successful run.

## Limits and release follow-up

No general TOAST reconstruction, variable-width DEFAULT support, NOTHING identity
or USING INDEX support was added. Source identity changes require repair/resync;
they do not silently switch ingestion modes. No source DDL is changed outside
the disposable test fixture.

This qualifies the real production binary/runner plus real product discovery and
preflight, using the existing local Flow REST/DuckDB path. Hosted API authentication,
Temporal scheduling, managed control-plane persistence, alternate Iceberg readers,
and journaled-acknowledgement mode were not exercised. Materialized acknowledgement
is the tested product configuration. No performance-parity claim is made.

The product's **published** `apps/flow/engine-image.txt` digest is intentionally
unchanged: no engine image was pushed. Before any future release, publish the
reviewed internal engine and update that digest together with the product
preflight change. The local source build pin and harness already select the
modified source; the existing production image does not contain this change.
