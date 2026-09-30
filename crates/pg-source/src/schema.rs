//! Source schema contracts. pgoutput supplies wire types but omits nullability,
//! defaults and missing-column values, so additive evolution requires a catalog
//! proof after the source transaction commits.

use crate::{Column, Error, Relation, Result};
use flow_model::{Column as ModelColumn, ColumnType, TableSchema};
use tokio_postgres::GenericClient;

#[derive(Debug)]
pub struct ColumnMetadata {
    pub nullable: bool,
    pub unsupported_generated: bool,
    /// No `attmissingval`: rows that predate the column read NULL for it.
    pub null_missing_value: bool,
}
#[derive(Debug)]
pub struct TableMetadata {
    pub types: crate::TypeRegistry,
    pub storage_id: u32,
    pub attribute_numbers: Vec<i16>,
    pub relation: Relation,
    pub columns: Vec<ColumnMetadata>,
}

/// A single SQL snapshot prevents independently fetched relation, key and
/// column metadata from describing different committed catalog versions.
pub async fn fetch_table_metadata(
    client: &(impl GenericClient + Sync),
    namespace: &str,
    table: &str,
) -> Result<TableMetadata> {
    fetch_table_metadata_selected(client, namespace, table, None).await
}

/// Resolve only selected types, retaining original attribute identities and the complete key.
pub async fn fetch_table_metadata_selected(
    client: &(impl GenericClient + Sync),
    namespace: &str,
    table: &str,
    selected: Option<&[String]>,
) -> Result<TableMetadata> {
    let request = TableMetadataRequest {
        namespace: namespace.to_owned(),
        table: table.to_owned(),
        selected: selected.map(<[String]>::to_vec),
    };
    fetch_table_metadata_batch(client, &[request])
        .await?
        .remove(0)
}

/// Bound transient catalog rows while retaining the existing refresh cadence.
pub const TABLE_METADATA_BATCH_SIZE: usize = 32;

pub struct TableMetadataRequest {
    pub namespace: String,
    pub table: String,
    pub selected: Option<Vec<String>>,
}

/// One catalog snapshot per bounded batch; validation errors remain per table.
pub async fn fetch_table_metadata_batch(
    client: &(impl GenericClient + Sync),
    requests: &[TableMetadataRequest],
) -> Result<Vec<Result<TableMetadata>>> {
    let mut output = Vec::with_capacity(requests.len());
    for batch in requests.chunks(TABLE_METADATA_BATCH_SIZE) {
        let namespaces: Vec<_> = batch.iter().map(|r| r.namespace.as_str()).collect();
        let tables: Vec<_> = batch.iter().map(|r| r.table.as_str()).collect();
        // Keep the indexed singleton lookup used by commit-time validation.
        let filter = if batch.len() == 1 {
            "n.nspname=$1 AND c.relname=$2"
        } else {
            "(n.nspname, c.relname) IN (SELECT * FROM unnest($1::text[], $2::text[]))"
        };
        let params: [&(dyn tokio_postgres::types::ToSql + Sync); 2] = if batch.len() == 1 {
            [&batch[0].namespace, &batch[0].table]
        } else {
            [&namespaces, &tables]
        };
        let query = format!("SELECT c.oid, c.relkind::text, c.relreplident::text, a.attname, a.atttypid, a.atttypmod,
                EXISTS (SELECT 1 FROM pg_catalog.pg_index i
                        CROSS JOIN LATERAL unnest(i.indkey) WITH ORDINALITY k(attnum, ordinal)
                        WHERE i.indrelid=c.oid AND i.indisprimary
                          AND k.ordinal <= i.indnkeyatts AND k.attnum=a.attnum),
                NOT a.attnotnull, (a.attgenerated::text <> '' AND (a.attgenerated::text <> 's' OR current_setting('server_version_num')::int < 180000)),
                (NOT a.atthasmissing OR a.attmissingval IS NULL OR a.attmissingval::text = '{{NULL}}'), c.relfilenode, a.attnum, n.nspname, c.relname
         FROM pg_catalog.pg_class c
         JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
         JOIN pg_catalog.pg_attribute a ON a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped
         WHERE {filter} AND c.relkind IN ('r','p')
         ORDER BY n.nspname, c.relname, a.attnum");
        let rows = client.query(&query, &params).await?;
        let mut grouped =
            std::collections::BTreeMap::<(String, String), Vec<tokio_postgres::Row>>::new();
        for row in rows {
            grouped
                .entry((row.get(12), row.get(13)))
                .or_default()
                .push(row);
        }
        for request in batch {
            let key = (request.namespace.clone(), request.table.clone());
            let rows = grouped.get(&key).map(Vec::as_slice).unwrap_or_default();
            output.push(
                decode_table_metadata(
                    client,
                    &request.namespace,
                    &request.table,
                    request.selected.as_deref(),
                    rows,
                )
                .await,
            );
        }
    }
    Ok(output)
}

async fn decode_table_metadata(
    client: &(impl GenericClient + Sync),
    namespace: &str,
    table: &str,
    selected: Option<&[String]>,
    rows: &[tokio_postgres::Row],
) -> Result<TableMetadata> {
    if let Some(selected) = selected {
        for row in rows {
            if row.get::<_, bool>(6) && !selected.contains(&row.get::<_, String>(3)) {
                return Err(Error::Config(
                    "column selection must include the complete primary key",
                ));
            }
        }
    }
    let rows = if let Some(selected) = selected {
        selected
            .iter()
            .map(|name| {
                rows.iter()
                    .find(|row| row.get::<_, String>(3) == *name)
                    .ok_or(Error::Config(
                        "selected source column disappeared; resynchronization is required",
                    ))
            })
            .collect::<Result<Vec<_>>>()?
    } else {
        rows.iter().collect()
    };
    let first = rows
        .first()
        .ok_or(Error::Config("source table has no supported columns"))?;
    let relation_kind: String = first.get(1);
    if relation_kind != "r" {
        return Err(Error::Config(
            "partitioned PostgreSQL source roots are unsupported; replicate a regular table",
        ));
    }
    let identity: String = first.get(2);
    let relation = Relation {
        id: first.get(0),
        namespace: namespace.to_owned(),
        name: table.to_owned(),
        replica_identity: *identity
            .as_bytes()
            .first()
            .ok_or(Error::Protocol("empty replica identity"))?,
        columns: rows
            .iter()
            .map(|row| Column {
                name: row.get(3),
                type_oid: row.get(4),
                type_modifier: row.get(5),
                identity: row.get(6),
            })
            .collect(),
    };
    let columns = rows
        .iter()
        .map(|row| ColumnMetadata {
            nullable: row.get(7),
            unsupported_generated: row.get(8),
            null_missing_value: row.get(9),
        })
        .collect();
    let types = crate::TypeRegistry::fetch(client, &relation).await?;
    Ok(TableMetadata {
        types,
        storage_id: first.get(10),
        attribute_numbers: rows.iter().map(|row| row.get(11)).collect(),
        relation,
        columns,
    })
}

/// Ignore replica-identity flags here: FULL wire tuples mark every field, while
/// the SQL metadata records the actual uniqueness constraint separately.
pub fn same_wire_schema(left: &Relation, right: &Relation) -> bool {
    left.id == right.id
        && left.namespace == right.namespace
        && left.name == right.name
        && left.replica_identity == right.replica_identity
        && left.columns.len() == right.columns.len()
        && wire_prefix(left, right)
}
fn wire_prefix(prefix: &Relation, next: &Relation) -> bool {
    prefix.id == next.id
        && prefix.namespace == next.namespace
        && prefix.name == next.name
        && prefix.replica_identity == next.replica_identity
        && prefix.columns.len() <= next.columns.len()
        && prefix.columns.iter().zip(&next.columns).all(|(old, new)| {
            old.name == new.name
                && old.type_oid == new.type_oid
                && old.type_modifier == new.type_modifier
        })
}

/// Infer only new column types. Existing OIDs and type modifiers are immutable,
/// even where two PostgreSQL types share the same Rust representation.
pub fn nullable_successor(
    base: &TableSchema,
    base_relation: &Relation,
    relation: &Relation,
    version: u32,
) -> Result<TableSchema> {
    nullable_successor_with_types(
        base,
        base_relation,
        relation,
        version,
        &crate::TypeRegistry::default(),
    )
}
pub fn nullable_successor_with_types(
    base: &TableSchema,
    base_relation: &Relation,
    relation: &Relation,
    version: u32,
    types: &crate::TypeRegistry,
) -> Result<TableSchema> {
    if !wire_prefix(base_relation, relation) || relation.columns.len() <= base.columns.len() {
        return Err(Error::Config(
            "schema evolution must only append nullable columns; existing names and wire types cannot change",
        ));
    }
    let mut next = base.clone();
    next.version = version;
    let mut field_id = base
        .columns
        .iter()
        .map(|column| column.field_id)
        .max()
        .unwrap_or(0);
    for column in &relation.columns[base.columns.len()..] {
        field_id = field_id
            .checked_add(1)
            .ok_or(Error::Config("Iceberg field ID overflow"))?;
        next.columns.push(ModelColumn {
            field_id,
            name: column.name.clone(),
            data_type: types.column_type(column)?,
            nullable: true,
        });
    }
    base.validate_successor(&next)?;
    types.validate_relation(&next, relation)?;
    Ok(next)
}

pub fn validate_schema_metadata(
    base: &TableSchema,
    schema: &TableSchema,
    wire: &Relation,
    metadata: &TableMetadata,
) -> Result<()> {
    metadata.types.validate_relation(schema, wire)?;
    if !wire_prefix(wire, &metadata.relation) {
        return Err(Error::Config(
            "committed source metadata no longer proves the decoded relation; schema migration or resynchronization is required",
        ));
    }
    if !schema.append_only {
        metadata.relation.validate_mutable()?;
    }
    let actual_key: Vec<_> = metadata
        .relation
        .columns
        .iter()
        .enumerate()
        .filter_map(|(index, column)| column.identity.then_some(index))
        .collect();
    if !(schema.append_only && schema.primary_key.is_empty()) {
        let mut expected = schema.primary_key.clone();
        expected.sort_unstable();
        if expected != actual_key {
            return Err(Error::Config("source primary key changed"));
        }
    }
    for (index, attributes) in metadata
        .columns
        .iter()
        .take(schema.columns.len())
        .enumerate()
    {
        if attributes.unsupported_generated {
            return Err(Error::Config(
                "generated columns require PostgreSQL 18 stored publication",
            ));
        }
        // Rows that predate an added column must read NULL. A constant ADD
        // COLUMN default is stored as `attmissingval` and backfills them
        // without row events; a volatile one rewrites the heap, which the
        // storage identity check rejects. Later SET DEFAULT, backfill UPDATEs
        // and SET NOT NULL are ordinary row events, so the live catalog's
        // default and nullability do not matter: the column stays optional.
        if index >= base.columns.len() && !attributes.null_missing_value {
            return Err(Error::Config(
                "new column has a non-NULL ADD COLUMN default that backfills existing rows without row changes; resynchronization is required",
            ));
        }
    }
    Ok(())
}

pub(crate) fn column_type(oid: u32, modifier: i32) -> Result<ColumnType> {
    Ok(match oid {
        16 => ColumnType::Bool,
        21 | 23 => ColumnType::Int32,
        20 => ColumnType::Int64,
        700 | 701 => ColumnType::Float64,
        25 | 1042 | 1043 | 114 | 3802 | 1083 => ColumnType::String,
        17 => ColumnType::Binary,
        1082 => ColumnType::Date,
        1114 => ColumnType::TimestampMicros,
        1184 => ColumnType::TimestampTzMicros,
        2950 => ColumnType::String,
        1700 if modifier < 4 => ColumnType::String,
        1700 if modifier >= 4 => {
            let modifier = modifier - 4;
            let precision = ((modifier >> 16) & 0xffff) as u16;
            let scale = ((modifier & 0x7ff) ^ 1024) - 1024;
            if precision == 0 || precision > 38 || scale < 0 || scale > i32::from(precision) {
                return Ok(ColumnType::String);
            }
            ColumnType::Decimal {
                precision: precision as u8,
                scale: scale as u8,
            }
        }
        _ => {
            return Err(Error::Config(
                "new column has an unsupported or unbounded PostgreSQL type",
            ));
        }
    })
}
