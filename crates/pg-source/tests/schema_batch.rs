use flow_pg_source::{
    TableMetadataRequest, fetch_table_metadata_batch,
    tokio_postgres::{self, NoTls},
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
        assert!(!metadata.columns[2].null_default);
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
