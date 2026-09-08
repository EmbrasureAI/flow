use flow_compactor::Policy;
use flow_coordinator::{Epoch, GarbagePolicy, GarbageProtection, TableMaintenance, TablePublisher};
use flow_iceberg_ext::ArtifactSet;
use flow_materializer::WriterConfig;
use flow_model::{OperationId, PgLsn, SourceId, TableSchema, Value};
use flow_state_store::{Change, ControlStore, StateStore, StateStoreOptions};
use flow_testkit::{catalog, scan, schema, table};
use iceberg::{Catalog, table::Table};
use serde::Serialize;
use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tempfile::TempDir;

#[derive(Serialize)]
struct RegistryFixture {
    table_uuid: uuid::Uuid,
    table_id: flow_model::TableId,
    location: String,
    operation: OperationId,
    created_ms: u64,
    unfenced_since_ms: Option<u64>,
    artifacts: ArtifactSet,
    cursor: u64,
    protected: bool,
}

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
                    max_duration: Duration::from_secs(1),
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
async fn registry_backlog_drains_in_bounded_pages_and_ineligible_records_do_not_spin() {
    let temp = TempDir::new().unwrap();
    let catalog = Arc::new(catalog(&temp.path().join("warehouse")).await);
    let schema = schema(1);
    let table = table(catalog.as_ref(), &schema).await;
    let control = ControlStore::open(temp.path().join("control")).unwrap();
    let index_path = temp.path().join("index");
    let options = StateStoreOptions::default();
    let store = control
        .initialize_index(&index_path, options.clone())
        .unwrap();
    let make_maintenance = |store| {
        TableMaintenance::new(
            store,
            catalog.clone(),
            Policy::default(),
            WriterConfig::default(),
        )
        .unwrap()
    };
    let maintenance = make_maintenance(store.clone());
    let location = table.metadata().location().trim_end_matches('/');
    let data = Path::new(location).join("data");
    std::fs::create_dir_all(&data).unwrap();
    let prefix = format!("owned-artifacts/v1/{}/", table.metadata().uuid());
    let now_ms = u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap();
    let mut protected_operations = BTreeSet::new();
    let mut paths = Vec::new();
    for ordinal in 0..80 {
        let path = data.join(format!("gc-{ordinal:03}.parquet"));
        std::fs::write(&path, b"registered orphan").unwrap();
        let operation = OperationId(format!("gc-{ordinal:03}"));
        if ordinal < 8 {
            protected_operations.insert(operation.clone());
        }
        let young = ordinal == 79;
        let record = RegistryFixture {
            table_uuid: table.metadata().uuid(),
            table_id: schema.table_id,
            location: location.to_owned(),
            operation,
            created_ms: if young { now_ms } else { 0 },
            unfenced_since_ms: Some(if young { now_ms } else { 0 }),
            artifacts: ArtifactSet {
                paths: vec![path.to_string_lossy().into_owned()],
                ranges: Vec::new(),
            },
            cursor: 0,
            protected: false,
        };
        store
            .put_source_transaction(
                format!("{prefix}{ordinal:03}").as_bytes(),
                &bincode::serialize(&record).unwrap(),
            )
            .unwrap();
        paths.push(path);
    }
    let protection = GarbageProtection {
        operations: protected_operations,
        ..Default::default()
    };
    let policy = GarbagePolicy {
        grace: Duration::from_secs(60 * 60),
        max_objects: 8,
        max_records: 8,
        max_duration: Duration::from_secs(1),
    };
    let first = maintenance
        .collect_garbage(&table, schema.table_id, &policy, &protection)
        .await
        .unwrap();
    assert_eq!(first.examined_records, 8);
    assert_eq!(first.delete_requests, 0);
    assert!(first.continuation_required);
    drop(maintenance);
    drop(store);

    let store = StateStore::open_with_control(index_path, options, control).unwrap();
    let maintenance = make_maintenance(store.clone());
    let mut passes = 1;
    let mut retired = first.retired_records;
    let mut continuation_required = first.continuation_required;
    while continuation_required {
        let report = maintenance
            .collect_garbage(&table, schema.table_id, &policy, &protection)
            .await
            .unwrap();
        passes += 1;
        retired += report.retired_records;
        assert!(report.examined_records <= policy.max_records);
        assert!(report.examined_objects <= policy.max_objects);
        if passes == 2 {
            assert_eq!(report.delete_requests, 8, "restart resumed after page one");
        }
        continuation_required = report.continuation_required;
        assert!(passes < 20, "bounded forward sweep must terminate");
    }
    assert_eq!(passes, 10, "the 80-record sweep must span bounded pages");
    assert_eq!(retired, 71);
    assert!(
        paths[8..79].iter().all(|path| !path.exists()),
        "every eligible registered orphan is removed"
    );
    assert!(
        paths[..8].iter().all(|path| path.exists()),
        "explicit operation protection is retained"
    );
    assert!(paths[79].exists(), "the grace period is retained");
    assert_eq!(
        store
            .source_transactions_after(prefix.as_bytes(), None)
            .count(),
        9
    );

    let mut idle_passes = 0;
    loop {
        let idle = maintenance
            .collect_garbage(&table, schema.table_id, &policy, &protection)
            .await
            .unwrap();
        idle_passes += 1;
        assert_eq!(idle.delete_requests, 0);
        assert_eq!(idle.retired_records, 0);
        if !idle.continuation_required {
            break;
        }
        assert!(idle_passes < 3, "ineligible-only sweep must terminate");
    }
    assert_eq!(idle_passes, 2);
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
