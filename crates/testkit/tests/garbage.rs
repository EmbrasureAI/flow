use flow_compactor::Policy;
use flow_coordinator::{Epoch, GarbagePolicy, GarbageProtection, TableMaintenance, TablePublisher};
use flow_materializer::WriterConfig;
use flow_model::{OperationId, PgLsn, SourceId, TableSchema, Value};
use flow_state_store::{Change, ControlStore, StateStore, StateStoreOptions};
use flow_testkit::{catalog, scan, schema, table};
use iceberg::{Catalog, table::Table};
use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tempfile::TempDir;

fn files(root: &Path) -> BTreeSet<PathBuf> {
    let mut result = BTreeSet::new();
    if !root.exists() {
        return result;
    }
    for entry in std::fs::read_dir(root).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            result.extend(files(&path));
        } else {
            result.insert(path);
        }
    }
    result
}
fn epoch(
    store: &StateStore,
    table: &Table,
    schema: &TableSchema,
    lsn: u64,
    changes: Vec<(i64, Change)>,
) -> (Epoch, flow_coordinator::CollapsedEpoch) {
    flow_testkit::collapse_fixture(
        store,
        table,
        schema,
        SourceId("local".into()),
        PgLsn(lsn),
        changes
            .into_iter()
            .map(|(id, change)| (schema.encode_key(&row(id, "key")).unwrap(), change)),
    )
}
fn row(id: i64, value: &str) -> Vec<Value> {
    vec![Value::Int64(id), Value::String(value.into())]
}
async fn collect(
    maintenance: &TableMaintenance,
    table: &Table,
    protection: &GarbageProtection,
) -> usize {
    let mut deleted = 0;
    // Three-object passes force durable cursor continuation across restarts and
    // across live owners. Repeated complete scans must remain harmless.
    for _ in 0..80 {
        let report = maintenance
            .collect_garbage(
                table,
                schema(1).table_id,
                &GarbagePolicy {
                    grace: Duration::from_millis(1),
                    max_objects: 3,
                    max_records: 2,
                },
                protection,
            )
            .await
            .unwrap();
        assert!(report.examined_objects <= 3);
        deleted += report.delete_requests;
    }
    deleted
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn registered_gc_preserves_readers_checkpoints_and_pending_uploads_across_restart() {
    let temp = TempDir::new().unwrap();
    let catalog = Arc::new(catalog(&temp.path().join("warehouse")).await);
    let schema = schema(1);
    let table = table(catalog.as_ref(), &schema).await;
    let control = ControlStore::open(temp.path().join("control")).unwrap();
    let options = StateStoreOptions {
        apply_batch_rows: 2,
        ..Default::default()
    };
    let store = control
        .initialize_index(temp.path().join("index"), options.clone())
        .unwrap();
    let writer = WriterConfig {
        target_file_bytes: 1,
        row_group_rows: 1,
        batch_bytes: 1024,
        ..Default::default()
    };
    let publisher =
        TablePublisher::new(store.clone(), catalog.clone(), writer.clone(), 2, 1 << 20).unwrap();
    let (first, collapsed) = epoch(
        &store,
        &table,
        &schema,
        10,
        (0..130)
            .map(|id| (id, Change::Insert(row(id, "initial"))))
            .collect(),
    );
    publisher.publish(&table, &schema, collapsed).await.unwrap();
    store.forget_applied(&first.id).unwrap();
    let initial = catalog.load_table(table.identifier()).await.unwrap();
    let checkpoint = control
        .checkpoint(&store, temp.path().join("checkpoint"))
        .unwrap();
    let initial_data = files(&Path::new(initial.metadata().location()).join("data"));
    assert!(
        initial_data.len() > 64,
        "exercise ordinal reservation extension"
    );
    let prefix = format!("owned-artifacts/v1/{}/", table.metadata().uuid());
    let records: Vec<_> = store
        .source_transactions_after(prefix.as_bytes(), None)
        .map(Result::unwrap)
        .collect();
    assert_eq!(
        records.len(),
        2,
        "one data range and one catalog metadata attempt"
    );
    assert!(
        records.iter().all(|(_, bytes)| bytes.len() < 2_000),
        "registry size is independent of row/file count"
    );
    let (update, collapsed) = epoch(
        &store,
        &initial,
        &schema,
        20,
        vec![(0, Change::Update(row(0, "updated"))), (1, Change::Delete)],
    );
    publisher
        .publish(&initial, &schema, collapsed)
        .await
        .unwrap();
    store.forget_applied(&update.id).unwrap();
    let maintain = |store| {
        TableMaintenance::new(
            store,
            catalog.clone(),
            Policy {
                l0_soft_files: 2,
                l0_hard_files: 4,
                // Retire the entire initial generation and the update file;
                // writer rolls may produce one file per input row group.
                max_group_files: initial_data.len() + 1,
                min_file_age_ms: 0,
                ..Default::default()
            },
            WriterConfig::default(),
        )
        .unwrap()
    };
    let maintenance = maintain(store.clone());
    let scratch = StateStore::open(temp.path().join("scratch"), options.clone()).unwrap();
    maintenance
        .compact(&table, &schema, scratch)
        .await
        .unwrap()
        .unwrap();
    let current = catalog.load_table(table.identifier()).await.unwrap();
    let current_rows = scan(&current, &schema).await.unwrap();
    assert_eq!(current_rows.len(), 129);
    assert!(current_rows.contains(&row(0, "updated")));
    let unknown =
        Path::new(current.metadata().location()).join("data/external-unregistered.parquet");
    std::fs::write(&unknown, b"external owner").unwrap();
    tokio::time::sleep(Duration::from_millis(3)).await;
    let protection =
        GarbageProtection::from_checkpoints(schema.table_id, &control.checkpoints().unwrap());
    maintenance
        .expire_history(
            &current,
            schema.table_id,
            Duration::from_millis(1),
            &protection.snapshots,
        )
        .await
        .unwrap();
    collect(&maintenance, &current, &protection).await;
    assert!(initial_data.iter().all(|path| path.is_file()));
    assert_eq!(scan(&initial, &schema).await.unwrap().len(), 130);
    assert!(unknown.is_file());
    control.forget_checkpoint(&checkpoint).unwrap();
    maintenance
        .expire_history(
            &current,
            schema.table_id,
            Duration::from_millis(1),
            &BTreeSet::new(),
        )
        .await
        .unwrap();
    // A valid row exceeding writer admission interrupts a later batch after files closed.
    let (failed, collapsed) = epoch(
        &store,
        &current,
        &schema,
        30,
        vec![
            (2, Change::Update(row(2, "temporary"))),
            (3, Change::Update(row(3, "temporary"))),
            (4, Change::Update(row(4, &"x".repeat(2048)))),
        ],
    );
    let before_failure = files(&Path::new(current.metadata().location()).join("data"));
    assert!(
        publisher
            .publish(&current, &schema, collapsed)
            .await
            .is_err()
    );
    let after_failure = files(&Path::new(current.metadata().location()).join("data"));
    let abandoned: Vec<_> = after_failure.difference(&before_failure).cloned().collect();
    assert!(!abandoned.is_empty());
    tokio::time::sleep(Duration::from_millis(3)).await;
    collect(&maintenance, &current, &GarbageProtection::default()).await;
    assert!(
        abandoned.iter().all(|path| path.is_file()),
        "pending uploads are protected independently of age"
    );
    store.discard_uncommitted(&failed.id).unwrap();
    drop(publisher);
    drop(maintenance);
    drop(store);
    let reopened =
        StateStore::open_with_control(temp.path().join("index"), options, control).unwrap();
    let maintenance = maintain(reopened.clone());
    assert!(collect(&maintenance, &current, &GarbageProtection::default()).await > 0);
    assert!(abandoned.iter().all(|path| !path.exists()));
    assert!(initial_data.iter().all(|path| !path.exists()));
    assert!(unknown.is_file());
    let final_table = catalog.load_table(table.identifier()).await.unwrap();
    assert_eq!(scan(&final_table, &schema).await.unwrap(), current_rows);
    assert_eq!(
        reopened
            .table_state(&schema.table_id)
            .unwrap()
            .materialized_lsn,
        PgLsn(20)
    );
}

#[tokio::test]
async fn index_loss_catalog_replay_registers_new_metadata_before_publication() {
    use flow_coordinator::resolve_catalog_operation;
    use flow_iceberg_ext::{CommitBase, write_artifact_plan};
    use flow_materializer::DataWriter;
    use flow_state_store::{IndexDelta, OperationKind, PreparedOperation};
    let temp = TempDir::new().unwrap();
    let catalog = catalog(&temp.path().join("warehouse")).await;
    let schema = schema(1);
    let table = table(&catalog, &schema).await;
    let control = ControlStore::open(temp.path().join("control")).unwrap();
    let index = control
        .initialize_index(temp.path().join("index"), StateStoreOptions::default())
        .unwrap();
    let id = OperationId("offline-replay".into());
    let mut writer = DataWriter::new(
        table.file_io().clone(),
        table.metadata().location(),
        &id,
        schema.clone(),
        0,
        WriterConfig::default(),
    )
    .unwrap();
    let value = row(1, "survives");
    let location = writer
        .write(std::slice::from_ref(&value), PgLsn(10))
        .await
        .unwrap()
        .locations
        .remove(0);
    let data = writer.close().await.unwrap();
    let plan = format!("{}/metadata/prepared.avro", table.metadata().location());
    write_artifact_plan(&table, &plan, &data, &[])
        .await
        .unwrap();
    index
        .prepare(
            PreparedOperation {
                id: id.clone(),
                table_id: schema.table_id,
                kind: OperationKind::Ingest,
                base_snapshot_id: None,
                last_lsn: PgLsn(10),
                schema_version: 0,
                artifacts: vec![plan.clone(), data[0].file_path().into()],
                payload: serde_json::to_vec(&serde_json::json!({
                    "base": CommitBase::new(&table), "manifest_list": plan,
                    "referenced_data": [], "properties": {},
                }))
                .unwrap(),
            },
            [IndexDelta {
                key: schema.encode_key(&value).unwrap(),
                expected: None,
                replacement: Some(location),
            }],
        )
        .unwrap();
    drop(index);
    std::fs::remove_dir_all(temp.path().join("index")).unwrap();
    let record = control.pending_operations().unwrap().remove(0);
    let before = files(Path::new(table.metadata().location()));
    let result = resolve_catalog_operation(&record, &catalog, &table, &control)
        .await
        .unwrap()
        .unwrap();
    let after = files(Path::new(table.metadata().location()));
    assert_eq!(
        after
            .difference(&before)
            .filter(|path| path
                .extension()
                .is_some_and(|extension| extension == "avro"))
            .count(),
        2,
        "new data manifest and manifest list; catalog owns its metadata JSON"
    );
    let prefix = format!("owned-artifacts/v1/{}/", table.metadata().uuid());
    assert_eq!(
        control
            .source_transactions_after(prefix.as_bytes(), None)
            .count(),
        1
    );
    control.resolve_operation(&id, Some(result)).unwrap();
    assert_eq!(
        control
            .table_state(&schema.table_id)
            .unwrap()
            .unwrap()
            .materialized_lsn,
        PgLsn(10)
    );
    let committed = catalog.load_table(table.identifier()).await.unwrap();
    assert_eq!(scan(&committed, &schema).await.unwrap(), vec![value]);
}
