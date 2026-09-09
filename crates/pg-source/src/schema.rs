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
    pub null_default: bool,
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
    let rows = client.query(
        "SELECT c.oid, c.relkind::text, c.relreplident::text, a.attname, a.atttypid, a.atttypmod,
                EXISTS (SELECT 1 FROM pg_catalog.pg_index i
                        CROSS JOIN LATERAL unnest(i.indkey) WITH ORDINALITY k(attnum, ordinal)
                        WHERE i.indrelid=c.oid AND i.indisprimary
                          AND k.ordinal <= i.indnkeyatts AND k.attnum=a.attnum),
                NOT a.attnotnull, (a.attgenerated::text <> '' AND (a.attgenerated::text <> 's' OR current_setting('server_version_num')::int < 180000)),
                (NOT a.atthasdef OR pg_catalog.pg_get_expr(d.adbin, d.adrelid)
                    IN ('NULL', 'NULL::' || pg_catalog.format_type(a.atttypid, a.atttypmod),
                                'NULL::' || pg_catalog.format_type(a.atttypid, NULL))),
                (NOT a.atthasmissing OR a.attmissingval IS NULL OR a.attmissingval::text = '{NULL}'), c.relfilenode, a.attnum
         FROM pg_catalog.pg_class c
         JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
         JOIN pg_catalog.pg_attribute a ON a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped
         LEFT JOIN pg_catalog.pg_attrdef d ON d.adrelid=c.oid AND d.adnum=a.attnum
         WHERE n.nspname=$1 AND c.relname=$2 AND c.relkind IN ('r','p')
         ORDER BY a.attnum", &[&namespace, &table]
    ).await?;
    if let Some(selected) = selected {
        for row in &rows {
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
            null_default: row.get::<_, Option<bool>>(9).unwrap_or(false),
            null_missing_value: row.get(10),
        })
        .collect();
    let types = crate::TypeRegistry::fetch(client, &relation).await?;
    Ok(TableMetadata {
        types,
        storage_id: first.get(11),
        attribute_numbers: rows.iter().map(|row| row.get(12)).collect(),
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
    if !schema.append_only && metadata.relation.replica_identity != b'f' {
        return Err(Error::ReplicaIdentity(wire.id));
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
    for (index, (column, attributes)) in schema.columns.iter().zip(&metadata.columns).enumerate() {
        if attributes.unsupported_generated {
            return Err(Error::Config(
                "generated columns require PostgreSQL 18 stored publication",
            ));
        }
        if !column.nullable && attributes.nullable {
            return Err(Error::Config("relaxed source nullability is unsupported"));
        }
        if index >= base.columns.len()
            && (!attributes.nullable || !attributes.null_default || !attributes.null_missing_value)
        {
            return Err(Error::Config(
                "new columns must be nullable with no default or a literal NULL default and no non-NULL missing value; backfill is required",
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
