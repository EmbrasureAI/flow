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
    let mut sweeps = 0;
    for _ in 0..1000 {
        let report = maintenance
            .collect_garbage(
                table,
                schema(1).table_id,
                &GarbagePolicy {
                    grace: Duration::from_millis(1),
                    metadata_grace: Duration::from_millis(1),
                    max_objects: 3,
                    max_records: 2,
                    max_deletes: 2,
                    max_duration: Duration::from_secs(1),
                },
                protection,
            )
            .await
            .unwrap();
        assert!(report.examined_objects <= 3);
        assert!(report.delete_requests <= 2);
        deleted += report.delete_requests;
        if !report.continuation_required {
            sweeps += 1;
            if sweeps == 3 {
                break;
            }
            // Complete real bounded sweeps, allowing each 1 ms test grace to
            // elapse between them: the first observation starts the clock.
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    }
    assert_eq!(sweeps, 3, "bounded registry sweeps must terminate");
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
                format!(
                    "{}{ordinal:03}",
                    if ordinal < 40 {
                        prefix.clone()
                    } else {
                        prefix.replace("/v1/", "/v2/")
                    }
                )
                .as_bytes(),
                &bincode::serialize(&record).unwrap(),
            )
            .unwrap();
        // These synthetic orphans were already observed unreferenced before
        // the grace boundary. The young record has never been observed.
        paths.push(path.clone());
        if young {
            continue;
        }
        let unreferenced = format!(
            "artifact-unreferenced/v1/{}/{}",
            table.metadata().uuid(),
            uuid::Uuid::new_v5(&table.metadata().uuid(), path.to_string_lossy().as_bytes())
        );
        store
            .put_source_transaction(
                unreferenced.as_bytes(),
                &bincode::serialize(&0_u64).unwrap(),
            )
            .unwrap();
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
        ..Default::default()
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
    // Fenced records stay under their unordered keys; the young one moved to
    // the due queue, which remains inside the rollback-readable v2 namespace.
    assert_eq!(
        store
            .source_transactions_after(prefix.as_bytes(), None)
            .chain(store.source_transactions_after(prefix.replace("/v1/", "/v2/").as_bytes(), None))
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
        assert_eq!(
            idle.examined_records, 8,
            "only fenced records; the queue stops at the first record not yet due"
        );
        assert_eq!(idle.delete_requests, 0);
        assert_eq!(idle.retired_records, 0);
        if !idle.continuation_required {
            break;
        }
        assert!(idle_passes < 3, "ineligible-only sweep must terminate");
    }
    assert_eq!(idle_passes, 1);
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
    let prefix = format!("owned-artifacts/v2/{}/", table.metadata().uuid());
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
    let prefix = format!("owned-artifacts/v2/{}/", table.metadata().uuid());
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn catalog_json_gc_preserves_current_tracked_and_unowned_files_across_restart() {
    use iceberg::transaction::{ApplyTransactionAction, Transaction};
    let temp = TempDir::new().unwrap();
    let catalog = Arc::new(catalog(&temp.path().join("warehouse")).await);
    let schema = schema(1);
    let table = table(catalog.as_ref(), &schema).await;
    let tx = Transaction::new(&table);
    let table = tx
        .update_table_properties()
        .set("write.metadata.previous-versions-max".into(), "1".into())
        .apply(tx)
        .unwrap()
        .commit(catalog.as_ref())
        .await
        .unwrap();
    let path = temp.path().join("index");
    let store = StateStore::open(&path, Default::default()).unwrap();
    let publisher = TablePublisher::new(
        store.clone(),
        catalog.clone(),
        WriterConfig::default(),
        100,
        1 << 20,
    )
    .unwrap();
    let obsolete_json = table.metadata_location().unwrap().to_owned();
    let mut current = table;
    for n in 1..=3 {
        let (e, collapsed) = epoch(
            &store,
            &current,
            &schema,
            n * 10,
            vec![(n as i64, Change::Insert(row(n as i64, "kept")))],
        );
        publisher
            .publish(&current, &schema, collapsed)
            .await
            .unwrap();
        store.forget_applied(&e.id).unwrap();
        current = catalog.load_table(current.identifier()).await.unwrap();
    }
    // Also register the head: GC must not delete it even after its grace.
    flow_coordinator::register_catalog_metadata(&store, &current, schema.table_id)
        .await
        .unwrap();
    let current_json = current.metadata_location().unwrap().to_owned();
    let tracked_json = current.metadata().metadata_log()[0].metadata_file.clone();
    let unowned = Path::new(current.metadata().location()).join("metadata/external.metadata.json");
    std::fs::write(&unowned, b"not owned by Flow").unwrap();
    drop(publisher);
    drop(store);
    let store = StateStore::open(&path, Default::default()).unwrap();
    let maintenance = TableMaintenance::new(
        store.clone(),
        catalog.clone(),
        Policy::default(),
        WriterConfig::default(),
    )
    .unwrap();
    // gc.enabled=false must disable physical cleanup as well as expiration.
    let tx = Transaction::new(&current);
    let disabled = tx
        .update_table_properties()
        .set("gc.enabled".into(), "false".into())
        .apply(tx)
        .unwrap()
        .commit(catalog.as_ref())
        .await
        .unwrap();
    flow_coordinator::register_catalog_metadata(&store, &disabled, schema.table_id)
        .await
        .unwrap();
    assert_eq!(
        collect(&maintenance, &disabled, &GarbageProtection::default()).await,
        0
    );
    assert!(Path::new(&obsolete_json).exists());
    let tx = Transaction::new(&disabled);
    let enabled = tx
        .update_table_properties()
        .set("gc.enabled".into(), "true".into())
        .apply(tx)
        .unwrap()
        .commit(catalog.as_ref())
        .await
        .unwrap();
    flow_coordinator::register_catalog_metadata(&store, &enabled, schema.table_id)
        .await
        .unwrap();
    let enabled_json = enabled.metadata_location().unwrap().to_owned();
    let enabled_previous = enabled.metadata().metadata_log()[0].metadata_file.clone();
    tokio::time::sleep(Duration::from_millis(5)).await;
    assert!(collect(&maintenance, &enabled, &GarbageProtection::default()).await > 0);
    assert!(
        !Path::new(&obsolete_json).exists(),
        "catalog JSON must actually be reclaimed"
    );
    assert!(
        !Path::new(&current_json).exists(),
        "a once-current JSON becomes collectible"
    );
    assert!(
        !Path::new(&tracked_json).exists(),
        "a once-tracked JSON becomes collectible"
    );
    assert!(Path::new(&enabled_json).exists());
    assert!(Path::new(&enabled_previous).exists());
    assert!(unowned.exists());
    assert_eq!(scan(&enabled, &schema).await.unwrap().len(), 3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn legacy_metadata_import_is_scoped_idempotent_and_grace_delayed() {
    use iceberg::transaction::{ApplyTransactionAction, Transaction};
    let temp = TempDir::new().unwrap();
    let catalog = Arc::new(catalog(&temp.path().join("warehouse")).await);
    let schema = schema(1);
    let original = table(catalog.as_ref(), &schema).await;
    let old_path = original.metadata_location().unwrap().to_owned();
    let mut current = original.clone();
    for n in 0..3 {
        let tx = Transaction::new(&current);
        current = tx
            .update_table_properties()
            .set("write.metadata.previous-versions-max".into(), "1".into())
            .set("test.version".into(), n.to_string())
            .apply(tx)
            .unwrap()
            .commit(catalog.as_ref())
            .await
            .unwrap();
    }
    let store = StateStore::open(temp.path().join("index"), Default::default()).unwrap();
    let import = |path: String, apply| {
        let store = store.clone();
        let table = current.clone();
        let id = schema.table_id;
        async move { flow_coordinator::import_catalog_metadata(&store, &table, id, &path, apply).await }
    };
    let prefix = format!("owned-artifacts/v2/{}/", current.metadata().uuid());
    import(old_path.clone(), false).await.unwrap();
    assert_eq!(
        store
            .source_transactions_after(prefix.as_bytes(), None)
            .count(),
        0
    );
    assert!(
        import(
            format!("{}/../sibling.metadata.json", current.metadata().location()),
            true
        )
        .await
        .is_err()
    );
    let foreign = Path::new(current.metadata().location()).join("metadata/foreign.metadata.json");
    for replacement in [
        serde_json::json!({"table-uuid": uuid::Uuid::new_v4(), "location": current.metadata().location(), "last-updated-ms": 0}),
        serde_json::json!({"table-uuid": current.metadata().uuid(), "location": "s3://foreign/table", "last-updated-ms": 0}),
        serde_json::json!({"table-uuid": current.metadata().uuid(), "location": current.metadata().location(), "last-updated-ms": i64::MAX}),
    ] {
        std::fs::write(&foreign, serde_json::to_vec(&replacement).unwrap()).unwrap();
        assert!(
            import(foreign.to_string_lossy().into_owned(), true)
                .await
                .is_err()
        );
    }
    import(old_path.clone(), true).await.unwrap();
    let before: Vec<_> = store
        .source_transactions_after(prefix.as_bytes(), None)
        .map(Result::unwrap)
        .collect();
    import(old_path.clone(), true).await.unwrap();
    let after: Vec<_> = store
        .source_transactions_after(prefix.as_bytes(), None)
        .map(Result::unwrap)
        .collect();
    assert_eq!(
        before, after,
        "repeated imports must not reset grace or duplicate records"
    );
    let legacy_prefix = format!("owned-artifacts/v1/{}/", current.metadata().uuid());
    assert_eq!(
        store
            .source_transactions_after(legacy_prefix.as_bytes(), None)
            .count(),
        0,
        "rollback must not expose JSON registrations to an older unsafe collector"
    );
    let maintenance = TableMaintenance::new(
        store,
        catalog.clone(),
        Policy::default(),
        WriterConfig::default(),
    )
    .unwrap();
    let grace = GarbagePolicy {
        grace: Duration::from_millis(300),
        metadata_grace: Duration::from_millis(300),
        ..Default::default()
    };
    let report = maintenance
        .collect_garbage(
            &current,
            schema.table_id,
            &grace,
            &GarbageProtection::default(),
        )
        .await
        .unwrap();
    assert_eq!(report.delete_requests, 0, "adoption starts a fresh grace");
    assert!(Path::new(&old_path).exists());
    tokio::time::sleep(Duration::from_millis(350)).await;
    assert_eq!(
        collect(&maintenance, &current, &GarbageProtection::default()).await,
        1
    );
    assert!(!Path::new(&old_path).exists());
    assert!(Path::new(current.metadata_location().unwrap()).exists());
    assert!(foreign.exists());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn long_lived_json_gets_a_new_reader_grace_when_it_leaves_catalog_history() {
    use iceberg::transaction::{ApplyTransactionAction, Transaction};
    let temp = TempDir::new().unwrap();
    let catalog = Arc::new(catalog(&temp.path().join("warehouse")).await);
    let schema = schema(1);
    let table = table(catalog.as_ref(), &schema).await;
    let path = table.metadata_location().unwrap().to_owned();
    let index = temp.path().join("index");
    let store = StateStore::open(&index, Default::default()).unwrap();
    let record = RegistryFixture {
        table_uuid: table.metadata().uuid(),
        table_id: schema.table_id,
        location: table.metadata().location().to_owned(),
        operation: OperationId("old-upload".into()),
        created_ms: 0,
        unfenced_since_ms: Some(0),
        cursor: 0,
        protected: false,
        artifacts: ArtifactSet {
            paths: vec![path.clone()],
            ranges: Vec::new(),
        },
    };
    store
        .put_source_transaction(
            format!("owned-artifacts/v2/{}/old", table.metadata().uuid()).as_bytes(),
            &bincode::serialize(&record).unwrap(),
        )
        .unwrap();
    let maintenance = TableMaintenance::new(
        store.clone(),
        catalog.clone(),
        Policy::default(),
        WriterConfig::default(),
    )
    .unwrap();
    assert_eq!(
        collect(&maintenance, &table, &GarbageProtection::default()).await,
        0
    );
    let mut current = table;
    for n in 0..2 {
        let tx = Transaction::new(&current);
        current = tx
            .update_table_properties()
            .set("write.metadata.previous-versions-max".into(), "1".into())
            .set("test.version".into(), n.to_string())
            .apply(tx)
            .unwrap()
            .commit(catalog.as_ref())
            .await
            .unwrap();
    }
    let grace = GarbagePolicy {
        grace: Duration::from_millis(300),
        metadata_grace: Duration::from_millis(300),
        ..Default::default()
    };
    let report = maintenance
        .collect_garbage(
            &current,
            schema.table_id,
            &grace,
            &GarbageProtection::default(),
        )
        .await
        .unwrap();
    assert_eq!(
        report.delete_requests, 0,
        "upload age must not substitute for time since unreference"
    );
    assert!(Path::new(&path).exists());
    drop(maintenance);
    drop(store);
    let store = StateStore::open(&index, Default::default()).unwrap();
    let maintenance = TableMaintenance::new(
        store,
        catalog.clone(),
        Policy::default(),
        WriterConfig::default(),
    )
    .unwrap();
    let report = maintenance
        .collect_garbage(
            &current,
            schema.table_id,
            &grace,
            &GarbageProtection::default(),
        )
        .await
        .unwrap();
    assert_eq!(
        report.delete_requests, 0,
        "restart must preserve the reader grace"
    );
    tokio::time::sleep(Duration::from_millis(350)).await;
    assert_eq!(
        collect(&maintenance, &current, &GarbageProtection::default()).await,
        1
    );
    assert!(!Path::new(&path).exists());
    assert!(Path::new(current.metadata_location().unwrap()).exists());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn partially_live_owner_does_not_redelete_absent_siblings() {
    let temp = TempDir::new().unwrap();
    let catalog = Arc::new(catalog(&temp.path().join("warehouse")).await);
    let schema = schema(1);
    let table = table(catalog.as_ref(), &schema).await;
    let orphan = Path::new(table.metadata().location()).join("metadata/obsolete.avro");
    std::fs::write(&orphan, b"orphan").unwrap();
    let store = StateStore::open(temp.path().join("index"), Default::default()).unwrap();
    let owner = RegistryFixture {
        table_uuid: table.metadata().uuid(),
        table_id: schema.table_id,
        location: table.metadata().location().to_owned(),
        operation: OperationId("mixed".into()),
        created_ms: 0,
        unfenced_since_ms: Some(0),
        cursor: 0,
        protected: false,
        artifacts: ArtifactSet {
            paths: vec![
                orphan.to_string_lossy().into_owned(),
                table.metadata_location().unwrap().into(),
            ],
            ranges: Vec::new(),
        },
    };
    store
        .put_source_transaction(
            format!("owned-artifacts/v2/{}/mixed", table.metadata().uuid()).as_bytes(),
            &bincode::serialize(&owner).unwrap(),
        )
        .unwrap();
    let maintenance =
        TableMaintenance::new(store, catalog, Policy::default(), WriterConfig::default()).unwrap();
    assert_eq!(
        collect(&maintenance, &table, &GarbageProtection::default()).await,
        1
    );
    assert_eq!(
        collect(&maintenance, &table, &GarbageProtection::default()).await,
        0,
        "repeat scans must not issue more deletes for an already absent object"
    );
    assert!(Path::new(table.metadata_location().unwrap()).exists());
    assert!(!orphan.exists());
}

fn now_ms() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap()
}
fn register(store: &StateStore, table: &Table, key: &str, paths: Vec<String>, created_ms: u64) {
    let record = RegistryFixture {
        table_uuid: table.metadata().uuid(),
        table_id: schema(1).table_id,
        location: table.metadata().location().to_owned(),
        operation: OperationId(format!("fixture-{key}")),
        created_ms,
        unfenced_since_ms: None,
        artifacts: ArtifactSet {
            paths,
            ranges: Vec::new(),
        },
        cursor: 0,
        protected: false,
    };
    store
        .put_source_transaction(key.as_bytes(), &bincode::serialize(&record).unwrap())
        .unwrap();
}
fn registry_keys(store: &StateStore, prefix: &str) -> Vec<String> {
    store
        .source_transactions_after(prefix.as_bytes(), None)
        .map(|entry| String::from_utf8(entry.unwrap().0.to_vec()).unwrap())
        .collect()
}
/// Run pages until one sweep completes; returns (pages, manifest-list reads, deletes).
async fn sweep(
    maintenance: &TableMaintenance,
    table: &Table,
    policy: &GarbagePolicy,
) -> (usize, usize, usize) {
    let (mut pages, mut reads, mut deletes) = (0, 0, 0);
    loop {
        let report = maintenance
            .collect_garbage(
                table,
                schema(1).table_id,
                policy,
                &GarbageProtection::default(),
            )
            .await
            .unwrap();
        pages += 1;
        reads += report.manifest_list_reads;
        deletes += report.delete_requests;
        assert!(report.examined_records <= policy.max_records);
        if !report.continuation_required {
            return (pages, reads, deletes);
        }
        assert!(pages < 1000, "a sweep must terminate");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sweep_stops_at_records_not_yet_due_and_still_collects_uuid_keyed_records() {
    let temp = TempDir::new().unwrap();
    let catalog = Arc::new(catalog(&temp.path().join("warehouse")).await);
    let table = table(catalog.as_ref(), &schema(1)).await;
    let store = StateStore::open(temp.path().join("index"), Default::default()).unwrap();
    let maintenance = TableMaintenance::new(
        store.clone(),
        catalog.clone(),
        Policy::default(),
        WriterConfig::default(),
    )
    .unwrap();
    let uuid = table.metadata().uuid();
    let data = Path::new(table.metadata().location()).join("data");
    std::fs::create_dir_all(&data).unwrap();
    let mut young = Vec::new();
    for n in 0..100 {
        let path = data.join(format!("young-{n:03}.parquet"));
        std::fs::write(&path, b"never observed").unwrap();
        register(
            &store,
            &table,
            &format!("owned-artifacts/v2/{uuid}/{}", uuid::Uuid::new_v4()),
            vec![path.to_string_lossy().into_owned()],
            now_ms(),
        );
        young.push(path);
    }
    // Records written by earlier releases under random UUIDv4 keys, in both
    // namespaces, whose objects were first observed unreferenced long ago.
    let mut legacy = Vec::new();
    for (n, namespace) in ["v1", "v2"].into_iter().enumerate() {
        let path = data.join(format!("legacy-{n}.parquet"));
        std::fs::write(&path, b"old orphan").unwrap();
        let path_string = path.to_string_lossy().into_owned();
        register(
            &store,
            &table,
            &format!(
                "owned-artifacts/{namespace}/{uuid}/{}",
                uuid::Uuid::new_v4()
            ),
            vec![path_string.clone()],
            0,
        );
        store
            .put_source_transaction(
                format!(
                    "artifact-unreferenced/v1/{uuid}/{}",
                    uuid::Uuid::new_v5(&uuid, path_string.as_bytes())
                )
                .as_bytes(),
                &bincode::serialize(&0_u64).unwrap(),
            )
            .unwrap();
        legacy.push(path);
    }
    let policy = GarbagePolicy {
        grace: Duration::from_secs(60 * 60),
        ..Default::default()
    };
    let first = maintenance
        .collect_garbage(
            &table,
            schema(1).table_id,
            &policy,
            &GarbageProtection::default(),
        )
        .await
        .unwrap();
    assert!(!first.continuation_required);
    assert_eq!(first.examined_records, 102);
    assert_eq!(first.delete_requests, 2);
    assert_eq!(first.retired_records, 2);
    assert_eq!(first.deferred_objects, 100);
    assert_eq!(first.rescheduled_records, 100);
    assert!(legacy.iter().all(|path| !path.exists()));
    assert!(young.iter().all(|path| path.exists()));
    // Every observed record waits in the due queue, keyed by its due time.
    let queued = registry_keys(&store, &format!("owned-artifacts/v2/{uuid}/~/"));
    assert_eq!(queued.len(), 100);
    assert_eq!(
        registry_keys(&store, &format!("owned-artifacts/v2/{uuid}/")).len(),
        100
    );
    assert!(registry_keys(&store, &format!("owned-artifacts/v1/{uuid}/")).is_empty());
    // A sweep reads only work that is due: none of the young records.
    for _ in 0..2 {
        let idle = maintenance
            .collect_garbage(
                &table,
                schema(1).table_id,
                &policy,
                &GarbageProtection::default(),
            )
            .await
            .unwrap();
        assert!(!idle.continuation_required);
        assert_eq!(idle.examined_records, 0);
        assert_eq!(idle.delete_requests, 0);
    }
    assert_eq!(
        registry_keys(&store, &format!("owned-artifacts/v2/{uuid}/~/")),
        queued
    );
}

async fn expired_history_is_reclaimed_once_indexed(budget: Option<usize>) {
    use iceberg::transaction::{ApplyTransactionAction, Transaction};
    let temp = TempDir::new().unwrap();
    let catalog = Arc::new(catalog(&temp.path().join("warehouse")).await);
    let schema = schema(1);
    let mut current = table(catalog.as_ref(), &schema).await;
    let store = StateStore::open(temp.path().join("index"), Default::default()).unwrap();
    let publisher = TablePublisher::new(
        store.clone(),
        catalog.clone(),
        WriterConfig::default(),
        100,
        1 << 20,
    )
    .unwrap();
    let publish = |current: Table, n: i64| {
        let store = store.clone();
        let publisher = &publisher;
        let schema = schema.clone();
        let catalog = catalog.clone();
        async move {
            let (e, collapsed) = epoch(
                &store,
                &current,
                &schema,
                n as u64 * 10,
                vec![(n, Change::Insert(row(n, "kept")))],
            );
            publisher
                .publish(&current, &schema, collapsed)
                .await
                .unwrap();
            store.forget_applied(&e.id).unwrap();
            catalog.load_table(current.identifier()).await.unwrap()
        }
    };
    for n in 1..=4 {
        current = publish(current, n).await;
    }
    let mut maintenance = TableMaintenance::new(
        store.clone(),
        catalog.clone(),
        Policy::default(),
        WriterConfig::default(),
    )
    .unwrap();
    if let Some(budget) = budget {
        maintenance = maintenance.with_retained_index_budget(budget);
    }
    let policy = GarbagePolicy {
        grace: Duration::from_millis(1),
        metadata_grace: Duration::from_millis(1),
        max_records: 1,
        ..Default::default()
    };
    let indexed = budget.is_none();
    let (pages, reads, deletes) = sweep(&maintenance, &current, &policy).await;
    assert!(pages > 1, "one-record pages force a multi-page sweep");
    assert_eq!(deletes, 0, "the first observation starts every clock");
    if indexed {
        assert_eq!(
            reads, 4,
            "each retained manifest list is read once for the whole sweep"
        );
    }
    tokio::time::sleep(Duration::from_millis(3)).await;
    let (_, reads, _) = sweep(&maintenance, &current, &policy).await;
    if indexed {
        assert_eq!(reads, 0, "immutable lists are reused by later sweeps");
    }
    current = publish(current, 5).await;
    tokio::time::sleep(Duration::from_millis(3)).await;
    let (_, reads, _) = sweep(&maintenance, &current, &policy).await;
    if indexed {
        assert_eq!(reads, 1, "only the new commit's list is read");
    }

    let mut snapshots: Vec<_> = current.metadata().snapshots().cloned().collect();
    snapshots.sort_by_key(|snapshot| snapshot.sequence_number());
    let (expired, retained) = snapshots.split_at(snapshots.len() - 2);
    let tx = Transaction::new(&current);
    current = tx
        .expire_snapshots()
        .expire_snapshot_ids(expired.iter().map(|snapshot| snapshot.snapshot_id()))
        .apply(tx)
        .unwrap()
        .commit(catalog.as_ref())
        .await
        .unwrap();
    assert_eq!(current.metadata().snapshots().count(), 2);
    for _ in 0..3 {
        tokio::time::sleep(Duration::from_millis(3)).await;
        sweep(&maintenance, &current, &policy).await;
    }
    assert!(
        expired
            .iter()
            .all(|snapshot| !Path::new(snapshot.manifest_list()).exists()),
        "an index kept across expiration must release expired history"
    );
    let mut referenced = BTreeSet::new();
    for snapshot in retained {
        referenced.insert(snapshot.manifest_list().to_owned());
        for manifest in current
            .manifest_list_reader(snapshot)
            .load()
            .await
            .unwrap()
            .entries()
        {
            referenced.insert(manifest.manifest_path.clone());
            let entries = manifest.load_manifest(current.file_io()).await.unwrap();
            for entry in entries.entries().iter().filter(|entry| entry.is_alive()) {
                referenced.insert(entry.file_path().to_owned());
            }
        }
    }
    assert!(referenced.len() > 2);
    assert!(
        referenced.iter().all(|path| Path::new(path).exists()),
        "retained snapshots keep every referenced object"
    );
    assert_eq!(scan(&current, &schema).await.unwrap().len(), 5);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reachability_is_indexed_once_per_sweep_and_tracks_expiration() {
    expired_history_is_reclaimed_once_indexed(None).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oversized_reachability_index_falls_back_to_manifest_walks() {
    expired_history_is_reclaimed_once_indexed(Some(1)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn superseded_metadata_json_uses_its_shorter_grace() {
    use iceberg::transaction::{ApplyTransactionAction, Transaction};
    let temp = TempDir::new().unwrap();
    let catalog = Arc::new(catalog(&temp.path().join("warehouse")).await);
    let mut current = table(catalog.as_ref(), &schema(1)).await;
    let superseded = current.metadata_location().unwrap().to_owned();
    for n in 0..2 {
        let tx = Transaction::new(&current);
        current = tx
            .update_table_properties()
            .set("write.metadata.previous-versions-max".into(), "1".into())
            .set("test.version".into(), n.to_string())
            .apply(tx)
            .unwrap()
            .commit(catalog.as_ref())
            .await
            .unwrap();
    }
    let orphan = Path::new(current.metadata().location()).join("metadata/orphan.avro");
    std::fs::write(&orphan, b"orphan").unwrap();
    let store = StateStore::open(temp.path().join("index"), Default::default()).unwrap();
    let uuid = current.metadata().uuid();
    let tracked = current.metadata().metadata_log()[0].metadata_file.clone();
    register(
        &store,
        &current,
        &format!("owned-artifacts/v2/{uuid}/json"),
        vec![superseded.clone(), tracked.clone()],
        0,
    );
    register(
        &store,
        &current,
        &format!("owned-artifacts/v2/{uuid}/manifest"),
        vec![orphan.to_string_lossy().into_owned()],
        0,
    );
    let maintenance =
        TableMaintenance::new(store, catalog, Policy::default(), WriterConfig::default()).unwrap();
    let policy = GarbagePolicy {
        grace: Duration::from_secs(60 * 60),
        metadata_grace: Duration::from_millis(1),
        ..Default::default()
    };
    let first = maintenance
        .collect_garbage(
            &current,
            schema(1).table_id,
            &policy,
            &GarbageProtection::default(),
        )
        .await
        .unwrap();
    assert_eq!(
        first.delete_requests, 0,
        "creation age does not start a clock"
    );
    assert_eq!(first.deferred_objects, 2);
    assert_eq!(
        first.protected_objects, 1,
        "the metadata log keeps its JSON"
    );
    tokio::time::sleep(Duration::from_millis(5)).await;
    let second = maintenance
        .collect_garbage(
            &current,
            schema(1).table_id,
            &policy,
            &GarbageProtection::default(),
        )
        .await
        .unwrap();
    assert_eq!(second.examined_records, 1, "the data grace is not yet due");
    assert_eq!(second.delete_requests, 1);
    assert_eq!(second.metadata_json_delete_requests, 1);
    assert!(!Path::new(&superseded).exists());
    assert!(Path::new(&tracked).exists());
    assert!(Path::new(current.metadata_location().unwrap()).exists());
    assert!(orphan.exists());
}
