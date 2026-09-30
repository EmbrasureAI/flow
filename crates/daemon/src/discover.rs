//! `discover` prints `[[tables]]` configuration for existing PostgreSQL tables.
//! It is read-only and uses the same type mapping and table validation as
//! `init`, so accepted output needs no hand-written column definitions.
use crate::config::{ColumnSelection, Config, Table};
use anyhow::Result;
use flow_model::{Column, ColumnType};
use flow_pg_source::{TypeRegistry, fetch_relation, tokio_postgres::Client};
use std::fmt::Write;

pub(crate) async fn discover(
    config: &Config,
    requested: &[String],
    schema: Option<&str>,
    target_namespace: Option<&str>,
) -> Result<()> {
    let sql = crate::source::connect(config, false).await?;
    let names = match (requested.is_empty(), schema) {
        (false, _) => requested
            .iter()
            .map(|name| split_name(name))
            .collect::<Result<Vec<_>>>()?,
        (true, Some(schema)) => schema_tables(&sql, schema).await?,
        (true, None) => publication_tables(&sql, &config.source.publication).await?,
    };
    let configured: std::collections::HashSet<_> = config
        .tables
        .iter()
        .map(|table| (table.source_namespace.clone(), table.source_table.clone()))
        .collect();
    let mut output = String::new();
    let mut discovered = 0;
    for (namespace, table) in names {
        if configured.contains(&(namespace.clone(), table.clone())) {
            eprintln!("skipping {namespace}.{table}: already configured");
            continue;
        }
        match describe(&sql, &namespace, &table, target_namespace).await {
            Ok(block) => {
                output.push_str(&block);
                discovered += 1;
            }
            Err(error) => eprintln!("skipping {namespace}.{table}: {error:#}"),
        }
    }
    print!("{output}");
    eprintln!("discovered {discovered} tables");
    Ok(())
}

fn split_name(name: &str) -> Result<(String, String)> {
    let (namespace, table) = name.split_once('.').unwrap_or(("public", name));
    anyhow::ensure!(
        !namespace.is_empty() && !table.is_empty() && !table.contains('.'),
        "table names are schema.table: {name}"
    );
    Ok((namespace.to_owned(), table.to_owned()))
}

async fn schema_tables(sql: &Client, schema: &str) -> Result<Vec<(String, String)>> {
    Ok(sql
        .query(
            "SELECT n.nspname::text, c.relname::text FROM pg_catalog.pg_class c
               JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
              WHERE n.nspname = $1 AND c.relkind IN ('r', 'p') ORDER BY c.relname",
            &[&schema],
        )
        .await?
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect())
}

async fn publication_tables(sql: &Client, publication: &str) -> Result<Vec<(String, String)>> {
    let rows = sql
        .query(
            "SELECT schemaname::text, tablename::text FROM pg_catalog.pg_publication_tables
              WHERE pubname = $1 ORDER BY 1, 2",
            &[&publication],
        )
        .await?;
    anyhow::ensure!(
        !rows.is_empty(),
        "publication {publication} has no tables; name tables or pass --schema"
    );
    Ok(rows
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect())
}

struct Attribute {
    not_null: bool,
    key_position: Option<usize>,
}

async fn describe(
    sql: &Client,
    namespace: &str,
    table: &str,
    target_namespace: Option<&str>,
) -> Result<String> {
    // Read the relation without the mutable-table checks; those are reported
    // below by the same validation `init` runs on the generated block.
    let (relation, preflight) = fetch_relation(sql, namespace, table, false).await?;
    let attributes = sql
        .query(
            "SELECT attnum, attnotnull FROM pg_catalog.pg_attribute
              WHERE attrelid = $1 AND attnum > 0 AND NOT attisdropped ORDER BY attnum",
            &[&relation.id],
        )
        .await?
        .into_iter()
        .map(|row| {
            let number: i16 = row.get(0);
            Attribute {
                not_null: row.get(1),
                key_position: preflight
                    .primary_key_attributes
                    .iter()
                    .position(|key| *key == number),
            }
        })
        .collect::<Vec<_>>();
    anyhow::ensure!(
        attributes.len() == relation.columns.len(),
        "table columns changed while reading"
    );
    let types = TypeRegistry::fetch(sql, &relation).await?;
    let mut columns = Vec::new();
    let mut keys = Vec::new();
    let mut excluded = Vec::new();
    for (column, attribute) in relation.columns.iter().zip(&attributes) {
        match types.column_type(column) {
            Ok(data_type) => {
                if let Some(position) = attribute.key_position {
                    keys.push((position, columns.len()));
                }
                columns.push(Column {
                    field_id: i32::try_from(columns.len() + 1)?,
                    name: column.name.clone(),
                    data_type,
                    nullable: !attribute.not_null,
                });
            }
            Err(error) => {
                anyhow::ensure!(
                    attribute.key_position.is_none(),
                    "primary-key column {} has an unsupported type ({error})",
                    column.name
                );
                excluded.push(format!("{} ({error})", column.name));
            }
        }
    }
    anyhow::ensure!(!columns.is_empty(), "no column has a supported type");
    keys.sort();
    let configured = Table {
        column_selection: if excluded.is_empty() {
            ColumnSelection::AllCurrent
        } else {
            ColumnSelection::Explicit
        },
        source_namespace: namespace.to_owned(),
        source_table: table.to_owned(),
        target_namespace: vec![target_namespace.unwrap_or(namespace).to_owned()],
        target_table: table.to_owned(),
        format_version: iceberg::spec::FormatVersion::V2,
        columns,
        primary_key: keys.into_iter().map(|(_, index)| index).collect(),
        append_only: preflight.primary_key_attributes.is_empty(),
        priority: Default::default(),
    };
    let mut notes = Vec::new();
    if configured.append_only {
        notes.push("no primary key: append-only, so UPDATE and DELETE are rejected".to_owned());
    }
    for column in &excluded {
        notes.push(format!("excluded unsupported column {column}"));
    }
    let schema = configured.schema(relation.id);
    if let Err(error) = crate::source::validate_source_table(sql, &configured, &schema).await {
        notes.push(format!("fix before init: {error:#}"));
    }
    render(&configured, &notes)
}

fn quoted(value: &str) -> String {
    toml::Value::String(value.to_owned()).to_string()
}

fn render(table: &Table, notes: &[String]) -> Result<String> {
    let mut block = String::from("\n");
    for note in notes {
        writeln!(block, "# {}", note.replace('\n', " "))?;
    }
    writeln!(block, "[[tables]]")?;
    if table.column_selection == ColumnSelection::Explicit {
        writeln!(block, "column_selection = \"explicit\"")?;
    }
    writeln!(
        block,
        "source_namespace = {}",
        quoted(&table.source_namespace)
    )?;
    writeln!(block, "source_table = {}", quoted(&table.source_table))?;
    let namespace: Vec<_> = table.target_namespace.iter().map(|n| quoted(n)).collect();
    writeln!(block, "target_namespace = [{}]", namespace.join(", "))?;
    writeln!(block, "target_table = {}", quoted(&table.target_table))?;
    let keys: Vec<_> = table.primary_key.iter().map(usize::to_string).collect();
    writeln!(block, "primary_key = [{}]", keys.join(", "))?;
    writeln!(block, "append_only = {}", table.append_only)?;
    writeln!(block, "columns = [")?;
    for column in &table.columns {
        writeln!(
            block,
            "  {{ field_id = {}, name = {}, data_type = {}, nullable = {} }},",
            column.field_id,
            quoted(&column.name),
            data_type(&column.data_type),
            column.nullable
        )?;
    }
    writeln!(block, "]")?;
    Ok(block)
}

fn data_type(kind: &ColumnType) -> String {
    match kind {
        ColumnType::Decimal { precision, scale } => {
            format!("{{ Decimal = {{ precision = {precision}, scale = {scale} }} }}")
        }
        other => quoted(&format!("{other:?}")),
    }
}

/// Used by tests: the rendered block must parse as ordinary configuration.
#[cfg(test)]
fn parse_block(block: &str) -> Result<Table> {
    use anyhow::Context;
    #[derive(serde::Deserialize)]
    struct Blocks {
        tables: Vec<Table>,
    }
    let mut parsed: Blocks = toml::from_str(block).context("rendered block is invalid TOML")?;
    parsed.tables.pop().context("no table rendered")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rendered_blocks_round_trip_through_configuration() {
        let table = Table {
            column_selection: ColumnSelection::Explicit,
            source_namespace: "sales".into(),
            source_table: "order \"lines\"".into(),
            target_namespace: vec!["sales".into()],
            target_table: "order \"lines\"".into(),
            format_version: iceberg::spec::FormatVersion::V2,
            columns: vec![
                Column {
                    field_id: 1,
                    name: "order_id".into(),
                    data_type: ColumnType::Int64,
                    nullable: false,
                },
                Column {
                    field_id: 2,
                    name: "amount".into(),
                    data_type: ColumnType::Decimal {
                        precision: 12,
                        scale: 2,
                    },
                    nullable: true,
                },
                Column {
                    field_id: 3,
                    name: "created".into(),
                    data_type: ColumnType::TimestampTzMicros,
                    nullable: true,
                },
            ],
            primary_key: vec![0],
            append_only: false,
            priority: Default::default(),
        };
        let block = render(&table, &["a note\nspanning lines".into()]).unwrap();
        assert!(block.contains("# a note spanning lines\n"));
        let parsed = parse_block(&block).unwrap();
        assert_eq!(parsed.columns, table.columns);
        assert_eq!(parsed.primary_key, table.primary_key);
        assert_eq!(parsed.source_table, table.source_table);
        assert!(parsed.column_selection == ColumnSelection::Explicit);
        assert!(!parsed.append_only);
    }

    #[test]
    fn table_names_default_to_public() {
        assert_eq!(
            split_name("orders").unwrap(),
            ("public".into(), "orders".into())
        );
        assert_eq!(
            split_name("sales.orders").unwrap(),
            ("sales".into(), "orders".into())
        );
        assert!(split_name("a.b.c").is_err());
    }
}
