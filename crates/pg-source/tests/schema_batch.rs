use flow_model::{Column, ColumnType, TableId, TableSchema};
use flow_pg_source::{
    TableMetadataRequest, fetch_table_metadata, fetch_table_metadata_batch,
    nullable_successor_with_types,
    tokio_postgres::{self, NoTls},
    validate_schema_metadata,
};

#[tokio::test]
#[ignore = "requires FLOW_POSTGRES_URL pointing to a disposable PostgreSQL database"]
async fn batched_metadata_preserves_table_errors_projections_and_ddl_detection() {
    let (client, connection) =
        tokio_postgres::connect(&std::env::var("FLOW_POSTGRES_URL").unwrap(), NoTls)
            .await
            .unwrap();
    tokio::spawn(async move {
        connection.await.unwrap();
    });
    let version: String = client
        .query_one("SHOW server_version_num", &[])
        .await
        .unwrap()
        .get(0);
    let stored_generated_supported = version.parse::<u32>().unwrap() >= 180000;
    let schema = format!("metadata_{}", uuid::Uuid::new_v4().simple());
    client
        .batch_execute(&format!(
            "CREATE SCHEMA {schema}; CREATE DOMAIN {schema}.label AS text;"
        ))
        .await
        .unwrap();
    for i in 0..65 {
        client.batch_execute(&format!("CREATE TABLE {schema}.t{i} (id integer PRIMARY KEY, label {schema}.label, defaulted text DEFAULT 'present', generated integer GENERATED ALWAYS AS (id + 1) STORED); ALTER TABLE {schema}.t{i} REPLICA IDENTITY FULL;")).await.unwrap();
    }
    let mut requests: Vec<_> = (0..65)
        .rev()
        .map(|i| TableMetadataRequest {
            namespace: schema.clone(),
            table: format!("t{i}"),
            selected: None,
        })
        .collect();
    requests.push(TableMetadataRequest {
        namespace: schema.clone(),
        table: "missing".into(),
        selected: None,
    });
    requests.push(TableMetadataRequest {
        namespace: schema.clone(),
        table: "t0".into(),
        selected: Some(vec!["label".into()]),
    });
    requests.push(TableMetadataRequest {
        namespace: schema.clone(),
        table: "t0".into(),
        selected: Some(vec!["label".into(), "id".into()]),
    });
    let mut results = fetch_table_metadata_batch(&client, &requests)
        .await
        .unwrap();
    assert_eq!(results.len(), requests.len());
    for (i, result) in results.iter().take(65).enumerate() {
        let metadata = result.as_ref().unwrap();
        assert_eq!(metadata.relation.name, format!("t{}", 64 - i));
        assert_eq!(metadata.relation.namespace, schema);
        assert_eq!(metadata.relation.replica_identity, b'f');
        assert_eq!(metadata.attribute_numbers, vec![1, 2, 3, 4]);
        assert!(metadata.relation.columns[0].identity);
        assert!(!metadata.columns[0].nullable);
        assert!(metadata.columns[1].nullable);
        // A CREATE TABLE default never backfills: there were no earlier rows.
        assert!(metadata.columns[2].null_missing_value);
        assert_eq!(
            metadata.columns[3].unsupported_generated,
            !stored_generated_supported
        );
    }
    assert!(results[65].is_err());
    assert!(results[66].is_err());
    let selected = results.pop().unwrap().unwrap();
    assert_eq!(selected.attribute_numbers, vec![2, 1]);
    assert_eq!(selected.relation.columns[0].name, "label");
    assert!(selected.relation.columns[1].identity);
    let old_oid = results[64].as_ref().unwrap().relation.id;
    let old_storage = results[64].as_ref().unwrap().storage_id;
    client.batch_execute(&format!("ALTER TABLE {schema}.t0 ADD COLUMN extra text; ALTER TABLE {schema}.t1 RENAME COLUMN label TO renamed; TRUNCATE {schema}.t2;")).await.unwrap();
    let changed = fetch_table_metadata_batch(&client, &requests)
        .await
        .unwrap();
    assert_eq!(changed[64].as_ref().unwrap().relation.columns.len(), 5);
    assert_eq!(
        changed[63].as_ref().unwrap().relation.columns[1].name,
        "renamed"
    );
    assert_ne!(
        changed[62].as_ref().unwrap().storage_id,
        results[62].as_ref().unwrap().storage_id
    );
    client
        .batch_execute(&format!(
            "DROP TABLE {schema}.t0; CREATE TABLE {schema}.t0(id integer PRIMARY KEY, label text);"
        ))
        .await
        .unwrap();
    let replacement = fetch_table_metadata_batch(&client, &requests[64..65])
        .await
        .unwrap()
        .remove(0)
        .unwrap();
    assert_ne!(replacement.relation.id, old_oid);
    assert_ne!(replacement.storage_id, old_storage);
    // Identically named relations in different namespaces must never mix rows.
    client.batch_execute(&format!("CREATE SCHEMA {schema}_other; CREATE TABLE {schema}_other.t0 (other_id bigint PRIMARY KEY)")).await.unwrap();
    let distinct = fetch_table_metadata_batch(
        &client,
        &[
            TableMetadataRequest {
                namespace: schema.clone(),
                table: "t0".into(),
                selected: None,
            },
            TableMetadataRequest {
                namespace: format!("{schema}_other"),
                table: "t0".into(),
                selected: None,
            },
        ],
    )
    .await
    .unwrap();
    assert_eq!(distinct[0].as_ref().unwrap().relation.columns.len(), 2);
    assert_eq!(
        distinct[1].as_ref().unwrap().relation.columns[0].name,
        "other_id"
    );
    assert_ne!(
        distinct[0].as_ref().unwrap().relation.id,
        distinct[1].as_ref().unwrap().relation.id
    );
    client
        .batch_execute(&format!("DROP SCHEMA {schema}, {schema}_other CASCADE"))
        .await
        .unwrap();
}

/// Only an ADD COLUMN default that backfills existing rows without row events
/// (`attmissingval`) or a heap rewrite is unsafe. An ORM migration's later SET
/// DEFAULT, backfill and SET NOT NULL are ordinary row events and must be
/// accepted however soon after the ADD COLUMN the catalog is checked.
#[tokio::test]
#[ignore = "requires FLOW_POSTGRES_URL pointing to a disposable PostgreSQL database"]
async fn batched_metadata_accepts_orm_column_migrations_but_not_backfilling_defaults() {
    let (client, connection) =
        tokio_postgres::connect(&std::env::var("FLOW_POSTGRES_URL").unwrap(), NoTls)
            .await
            .unwrap();
    tokio::spawn(async move {
        connection.await.unwrap();
    });
    let namespace = format!("added_{}", uuid::Uuid::new_v4().simple());
    client
        .batch_execute(&format!("CREATE SCHEMA {namespace}"))
        .await
        .unwrap();
    let column = |field_id, name: &str, data_type, nullable| Column {
        field_id,
        name: name.into(),
        data_type,
        nullable,
    };
    let base = TableSchema {
        table_id: TableId(0),
        version: 1,
        columns: vec![
            column(1, "id", ColumnType::Int32, false),
            column(2, "body", ColumnType::String, true),
        ],
        primary_key: vec![0],
        append_only: false,
    };
    for (table, migration, accepted) in [
        (
            "set_default",
            "ALTER TABLE {t} ADD COLUMN added text; ALTER TABLE {t} ALTER COLUMN added SET DEFAULT 'later'",
            true,
        ),
        (
            "backfill_not_null",
            "ALTER TABLE {t} ADD COLUMN added text; UPDATE {t} SET added = 'filled'; ALTER TABLE {t} ALTER COLUMN added SET NOT NULL; ALTER TABLE {t} ALTER COLUMN added SET DEFAULT 'new'",
            true,
        ),
        (
            "null_default",
            "ALTER TABLE {t} ADD COLUMN added text DEFAULT NULL",
            true,
        ),
        (
            "fast_default",
            "ALTER TABLE {t} ADD COLUMN added text DEFAULT 'old'",
            false,
        ),
        (
            "dropped_fast_default",
            "ALTER TABLE {t} ADD COLUMN added text NOT NULL DEFAULT 'old'; ALTER TABLE {t} ALTER COLUMN added DROP DEFAULT; ALTER TABLE {t} ALTER COLUMN added DROP NOT NULL",
            false,
        ),
    ] {
        let name = format!("{namespace}.{table}");
        client
            .batch_execute(&format!(
                "CREATE TABLE {name} (id integer PRIMARY KEY, body text); ALTER TABLE {name} REPLICA IDENTITY FULL; INSERT INTO {name} VALUES (1, 'existing');"
            ))
            .await
            .unwrap();
        let before = fetch_table_metadata(&client, &namespace, table)
            .await
            .unwrap();
        let mut base = base.clone();
        base.table_id = TableId(before.relation.id);
        client
            .batch_execute(&migration.replace("{t}", &name))
            .await
            .unwrap();
        let after = fetch_table_metadata(&client, &namespace, table)
            .await
            .unwrap();
        assert_eq!(
            after.storage_id, before.storage_id,
            "{table} rewrote the heap"
        );
        let next = nullable_successor_with_types(
            &base,
            &before.relation,
            &after.relation,
            2,
            &after.types,
        )
        .unwrap();
        // The added field stays optional even where the source is NOT NULL,
        // so the Iceberg schema never needs an illegal optional-to-required step.
        assert!(next.columns[2].nullable);
        let validated = validate_schema_metadata(&base, &next, &after.relation, &after);
        assert_eq!(validated.is_ok(), accepted, "{table}: {validated:?}");
        // Once accepted, the column is part of the parent schema: its later
        // default and nullability changes are never re-validated as new.
        if accepted {
            validate_schema_metadata(&next, &next, &after.relation, &after).unwrap();
        }
    }
    client
        .batch_execute(&format!("DROP SCHEMA {namespace} CASCADE"))
        .await
        .unwrap();
}

/// Repeatable loopback benchmark; round-trip savings grow with network latency.
#[tokio::test]
#[ignore = "requires FLOW_POSTGRES_URL pointing to a disposable PostgreSQL database"]
async fn benchmark_metadata_round_trips() {
    let (client, connection) =
        tokio_postgres::connect(&std::env::var("FLOW_POSTGRES_URL").unwrap(), NoTls)
            .await
            .unwrap();
    tokio::spawn(async move {
        connection.await.unwrap();
    });
    let namespace = format!("bench_{}", uuid::Uuid::new_v4().simple());
    client
        .batch_execute(&format!("CREATE SCHEMA {namespace}"))
        .await
        .unwrap();
    let requests: Vec<_> = (0..416)
        .map(|i| TableMetadataRequest {
            namespace: namespace.clone(),
            table: format!("t{i}"),
            selected: None,
        })
        .collect();
    for request in &requests {
        client.batch_execute(&format!("CREATE TABLE {namespace}.{} (id bigint PRIMARY KEY, a text, b text, c text, d text, e text, f text, g text, h text, i text, j text, k text)", request.table)).await.unwrap();
    }
    let mut singleton = Vec::new();
    let mut batched = Vec::new();
    for _ in 0..5 {
        let start = std::time::Instant::now();
        for request in &requests {
            flow_pg_source::fetch_table_metadata(&client, &request.namespace, &request.table)
                .await
                .unwrap();
        }
        singleton.push(start.elapsed().as_micros());
        let start = std::time::Instant::now();
        let metadata = fetch_table_metadata_batch(&client, &requests)
            .await
            .unwrap();
        assert_eq!(metadata.len(), 416);
        assert!(metadata.iter().all(Result::is_ok));
        batched.push(start.elapsed().as_micros());
    }
    singleton.sort();
    batched.sort();
    println!(
        "416 tables, 12 builtin columns, median of 5 sweeps: singleton={}us (416 catalog queries); batch={}us (13 catalog queries)",
        singleton[2], batched[2]
    );
    client
        .batch_execute(&format!("DROP SCHEMA {namespace} CASCADE"))
        .await
        .unwrap();
}
