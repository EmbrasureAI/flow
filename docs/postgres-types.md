# PostgreSQL type mappings

Flow uses the [Fivetran PostgreSQL connector's mapping conventions](https://fivetran.com/docs/connectors/databases/postgresql)
and [Iceberg destination representations](https://fivetran.com/docs/destinations/s3-data-lake)
for common application types. This is a type-mapping compatibility policy, not a
claim of complete Fivetran feature parity.

| PostgreSQL | Flow configuration / Iceberg output |
| --- | --- |
| UUID | `String`: canonical lowercase hyphenated UUID |
| JSON / JSONB | `String`: normalized JSON text |
| Arrays of supported types, including enum/domain elements | `String`: JSON array text |
| Enum | `String`: the source label |
| Domain | Underlying type's mapping, recursively |
| Unbounded numeric, precision above 38, negative scale, or scale above precision | `String`: exact normalized decimal text |
| Bounded numeric with `0 <= scale <= precision <= 38` | `Decimal { precision, scale }`, or explicit `String` |
| Stored generated columns on PostgreSQL 18+ | The generated value's ordinary mapping |

For example, a table with a UUID key, JSONB payload and unbounded amount uses:

```toml
[[tables]]
source_namespace = "public"
source_table = "events"
target_namespace = ["analytics"]
target_table = "events"
primary_key = [0]
columns = [
  { field_id = 1, name = "id", data_type = "String", nullable = false },
  { field_id = 2, name = "payload", data_type = "String", nullable = true },
  { field_id = 3, name = "amount", data_type = "String", nullable = true },
]
```

These use the existing model `String` value and Iceberg string type. RocksDB,
spooled row serialization, key encoding, writer and compactor formats do not
change. Existing configurations with native `Uuid` still decode/write native
UUIDs; do not change the representation of an initialized table in place. New
nullable UUID columns inferred during schema evolution use strings.

## Value semantics

* JSON is parsed with arbitrary-precision numbers. SQL NULL remains a missing
  value; JSON `null` becomes the string `null`. Object key order and whitespace
  normalize, and the last duplicate object key wins. JSONB already has PostgreSQL's
  normalized semantics. Large unchanged TOAST fields still use the validated FULL
  old tuple; the decoder never queries the source per row.
* Arrays preserve order, dimensions as nested JSON arrays, and null elements.
  A null array and an empty array remain different. PostgreSQL lower-bound labels
  are discarded: `[0:1]={10,20}` becomes `[10,20]`. The binary decoder reuses
  `postgres-protocol` framing and supports up to PostgreSQL's six dimensions.
  JSON elements remain JSON values, numeric elements retain exact JSON numbers,
  and UUID/enum elements become strings. Binary elements use base64; dates and
  timestamps use ISO text. Non-finite numeric/float elements become strings.
  JSON and array primary keys are rejected because their PostgreSQL equality
  semantics cannot safely be represented by the encoded string key.
* Numeric strings preserve mathematical values, not display scale: `12.3400`
  becomes `12.34`, and negative zero becomes `0`. This also makes equivalent
  numeric primary keys identical during snapshot and CDC. `NaN`, `Infinity`, and
  `-Infinity` are strings. This intentionally differs from Fivetran's documented
  Iceberg DOUBLE fallback for unspecified precision: Flow does not silently lose
  numeric precision. Decimal output retains its existing finite, checked contract.
* Domain constraints remain enforced by PostgreSQL, not copied to Iceberg.
  Unsupported domain base types are rejected. Enum label additions are accepted;
  label renames or other DDL that changes existing values without row CDC require
  coordinated resynchronization. The normal stable-primary-key, table-incarnation,
  immutable existing-column and nullable-addition rules still apply.

## Generated columns

PostgreSQL versions before 18 do not publish generated values through pgoutput.
Flow rejects those columns. PostgreSQL 18 stored generated columns must be
included in the publication, for example:

```sql
CREATE PUBLICATION events_flow FOR TABLE public.events
WITH (publish_generated_columns = stored);
```

Flow checks the full published column list against the snapshot projection.
An existing publication is never silently changed. Virtual generated columns
remain unsupported. Generated-column additions that require backfilling existing
rows require resynchronization. See [PostgreSQL's replication contract](https://www.postgresql.org/docs/18/logical-replication-gencols.html).

## Verification

`tests/production/type_compat.py` runs the actual daemon against disposable
PostgreSQL, an Iceberg REST catalog and S3-compatible storage. Stock DuckDB reads
Iceberg snapshots and compares every row with source values after initial copy,
CDC, UUID key movement, deletes, SIGKILL/restart, native compaction and nullable
UUID/domain additions followed by another restart. It includes nested arrays,
null transitions, large exact JSON/numeric values, nested domains, enum/domain
arrays and unchanged toasted JSON/text. PostgreSQL 18 runs additionally check
stored generated values after base-column updates. CI runs it on PostgreSQL
14–18 using the existing service matrix.

```sh
uv run tests/production/type_compat.py \
  --catalog-uri "$FLOW_REST_URL" --s3-endpoint "$FLOW_S3_URL" \
  --binary target/debug/embrasure-flow --artifacts /tmp/flow-type-check
```

Use the disposable services and environment variables from the
[local service guide](../tests/local/README.md). These are correctness tests, not
throughput benchmarks or an AWS Glue/Athena qualification run.
