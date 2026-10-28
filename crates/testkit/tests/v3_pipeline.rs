//! Local cross-crate v3 acceptance: actual journal frames, RocksDB, Parquet,
//! Puffin, catalog publication, stock scans, and maintenance scans.
use flow_compactor::{Policy, ReadLimits, scan_live_files};
use flow_coordinator::{DeleteRewritePolicy, TableMaintenance, TablePublisher};
use flow_iceberg_ext::{RewriteFilesAction, SnapshotView};
use flow_materializer::{DataWriter, RowLineage, WriterConfig, iceberg_schema};
use flow_model::{FileId, OperationId, PgLsn, Row, SourceId, TableSchema, Value};
use flow_state_store::{Change, OperationPhase, StateStore, StateStoreOptions};
use flow_testkit::{LostResponseCatalog, catalog, collapse_fixture, scan, schema};
use iceberg::{
    Catalog, NamespaceIdent, TableCommit, TableCreation, TableUpdate,
    spec::{DataContentType, DataFileFormat, FormatVersion},
    table::Table,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

fn row(id: i64, value: &str) -> Row {
    vec![Value::Int64(id), Value::String(value.into())]
}
fn id(row: &Row) -> i64 {
    match row[0] {
        Value::Int64(id) => id,
        _ => unreachable!(),
    }
}
fn options() -> StateStoreOptions {
    StateStoreOptions {
        apply_batch_rows: 2,
        ..Default::default()
    }
}
struct Fixture {
    temp: tempfile::TempDir,
    catalog: Arc<LostResponseCatalog>,
    store: StateStore,
    schema: TableSchema,
    head: Table,
    batch_bytes: usize,
}
impl Fixture {
    async fn new(version: FormatVersion) -> Self {
        Self::create(version, false).await
    }
    // Object-store URIs sort after `dv:` file IDs, unlike bare local paths.
    async fn with_uri_location(version: FormatVersion) -> Self {
        Self::create(version, true).await
    }
    async fn create(version: FormatVersion, uri: bool) -> Self {
        let temp = tempfile::tempdir().unwrap();
        let catalog = Arc::new(LostResponseCatalog::new(
            catalog(&temp.path().join("lake")).await,
        ));
        let schema = schema(1);
        let location = uri.then(|| format!("file://{}", temp.path().join("uri-table").display()));
        let head = catalog
            .create_table(
                &NamespaceIdent::new("test".into()),
                TableCreation::builder()
                    .name("v3_pipeline".into())
                    .location_opt(location)
                    .format_version(version)
                    .schema(iceberg_schema(&schema).unwrap())
                    .build(),
            )
            .await
            .unwrap();
        let store = StateStore::open(temp.path().join("index"), options()).unwrap();
        Self {
            temp,
            catalog,
            store,
            schema,
            head,
            batch_bytes: 1 << 20,
        }
    }
    // Optional retained artifacts let independent engines verify this same fixture.
    async fn export(self, name: &str) {
        if let Ok(directory) = std::env::var("FLOW_V3_EXPORT") {
            let directory = std::path::PathBuf::from(directory);
            std::fs::create_dir_all(&directory).unwrap();
            let metadata = serde_json::to_vec_pretty(self.head.metadata()).unwrap();
            let live = self.live().await;
            let expected: Vec<_> = live
                .iter()
                .map(|(id, (row, (row_id, sequence)))| {
                    let value = match &row[1] {
                        Value::String(value) => Some(value.as_str()),
                        Value::Null => None,
                        _ => unreachable!(),
                    };
                    serde_json::json!([id, value, row_id, sequence])
                })
                .collect();
            std::fs::write(directory.join(format!("{name}.metadata.json")), metadata).unwrap();
            std::fs::write(
                directory.join(format!("{name}.expected.json")),
                serde_json::to_vec(&expected).unwrap(),
            )
            .unwrap();
            let _ = self.temp.keep();
        }
    }
    fn publisher(&self) -> TablePublisher {
        TablePublisher::new(
            self.store.clone(),
            self.catalog.clone(),
            WriterConfig::default(),
            2,
            self.batch_bytes,
        )
        .unwrap()
    }
    fn scratch(&self) -> StateStore {
        StateStore::open(
            self.temp
                .path()
                .join(format!("scratch-{}", uuid::Uuid::new_v4())),
            options(),
        )
        .unwrap()
    }
    fn maintenance(&self) -> TableMaintenance {
        self.maintenance_with(&self.store, Policy::default())
    }
    fn maintenance_with(&self, store: &StateStore, policy: Policy) -> TableMaintenance {
        TableMaintenance::new(
            store.clone(),
            self.catalog.clone(),
            Policy {
                l0_soft_files: 2,
                min_file_age_ms: 0,
                ..policy
            },
            WriterConfig::default(),
        )
        .unwrap()
    }
    async fn refresh(&mut self) {
        self.head = self
            .catalog
            .load_table(self.head.identifier())
            .await
            .unwrap();
    }
    async fn publish(&mut self, lsn: u64, changes: Vec<(i64, Change)>) {
        let (epoch, collapsed) = collapse_fixture(
            &self.store,
            &self.head,
            &self.schema,
            SourceId("v3-pipeline".into()),
            PgLsn(lsn),
            changes
                .into_iter()
                .map(|(id, change)| (self.schema.encode_key(&row(id, "")).unwrap(), change)),
        );
        self.publisher()
            .publish(&self.head, &self.schema, collapsed)
            .await
            .unwrap();
        self.store.forget_applied(&epoch.id).unwrap();
        self.refresh().await;
    }
    async fn assert_rows(&self, expected: Vec<Row>) {
        let mut actual = scan(&self.head, &self.schema).await.unwrap();
        actual.sort_by_key(id);
        assert_eq!(actual, expected);
    }
    async fn live(&self) -> BTreeMap<i64, (Row, (Option<i64>, Option<i64>))> {
        let view = SnapshotView::current(&self.head).await.unwrap();
        let files = view
            .live_files
            .values()
            .filter(|entry| entry.content_type() == DataContentType::Data)
            .map(|entry| FileId(entry.file_path().into()))
            .collect();
        let mut result = BTreeMap::new();
        scan_live_files(
            &self.head,
            &self.schema,
            &view,
            &files,
            &self.scratch(),
            "truth",
            2,
            &ReadLimits::default(),
            async |batch| {
                let lineage = batch.lineage.expect("v3 scan must expose row lineage");
                assert_eq!(lineage.len(), batch.rows.len());
                for (row, lineage) in batch.rows.into_iter().zip(lineage) {
                    assert!(
                        result
                            .insert(
                                id(&row),
                                (row, (lineage.row_id, lineage.last_updated_sequence_number))
                            )
                            .is_none(),
                        "duplicate live primary key"
                    );
                }
                Ok(())
            },
        )
        .await
        .unwrap();
        result
    }
}

fn puffin_objects(directory: &std::path::Path) -> usize {
    std::fs::read_dir(directory)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .map(|path| {
            if path.is_dir() {
                puffin_objects(&path)
            } else {
                usize::from(path.extension().is_some_and(|ext| ext == "puffin"))
            }
        })
        .sum()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_consolidation_must_retire_shared_puffin_objects() {
    let mut f = Fixture::new(FormatVersion::V3).await;
    for group in 0..4 {
        f.publish(
            (group + 1) * 10,
            (group * 3..group * 3 + 3)
                .map(|id| (id as i64, Change::Insert(row(id as i64, "initial"))))
                .collect(),
        )
        .await;
    }
    // Each epoch creates a shared object with a one-position and a two-position
    // vector. The two smallest inputs belong to different physical objects.
    f.publish(
        50,
        vec![
            (0, Change::Delete),
            (3, Change::Delete),
            (4, Change::Delete),
        ],
    )
    .await;
    f.publish(
        60,
        vec![
            (6, Change::Delete),
            (9, Change::Delete),
            (10, Change::Delete),
        ],
    )
    .await;
    let before = SnapshotView::current(&f.head).await.unwrap();
    let mut vectors = before
        .live_files
        .values()
        .filter(|entry| entry.file_format() == DataFileFormat::Puffin)
        .collect::<Vec<_>>();
    vectors.sort_by_key(|entry| {
        (
            flow_iceberg_ext::delete_content_size(&entry.data_file),
            entry.file_path(),
        )
    });
    assert_eq!(vectors.len(), 4);
    assert_ne!(vectors[0].file_path(), vectors[1].file_path());
    let live = f.live().await;
    let snapshot = f.head.metadata().current_snapshot_id();
    let mut policy = DeleteRewritePolicy {
        min_input_files: 2,
        max_input_files: 2,
        ..Default::default()
    };
    let written = puffin_objects(f.temp.path());
    assert_eq!(
        f.maintenance()
            .compact_deletes(&f.head, &f.schema, f.scratch(), &policy)
            .await
            .unwrap(),
        None,
        "partial selection keeps both input objects live and must not add a third"
    );
    assert_eq!(
        puffin_objects(f.temp.path()),
        written,
        "a rewrite known to be nonviable uploads nothing"
    );
    f.refresh().await;
    assert_eq!(f.head.metadata().current_snapshot_id(), snapshot);
    policy.max_input_files = 4;
    assert!(
        f.maintenance()
            .compact_deletes(&f.head, &f.schema, f.scratch(), &policy)
            .await
            .unwrap()
            .is_some()
    );
    f.refresh().await;
    let after = SnapshotView::current(&f.head).await.unwrap();
    let objects = after
        .live_files
        .values()
        .filter(|entry| entry.file_format() == DataFileFormat::Puffin)
        .map(|entry| entry.file_path())
        .collect::<BTreeSet<_>>();
    assert_eq!(objects.len(), 1);
    assert_eq!(
        f.live().await,
        live,
        "rows and lineage survive consolidation"
    );
    assert_eq!(
        f.store
            .table_state(&f.schema.table_id)
            .unwrap()
            .materialized_lsn,
        PgLsn(60)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cumulative_vectors_share_puffin_and_survive_lost_response_and_reopen() {
    let mut f = Fixture::new(FormatVersion::V3).await;
    f.publish(
        10,
        (1..=3)
            .map(|id| (id, Change::Insert(row(id, "initial"))))
            .collect(),
    )
    .await;
    f.publish(
        20,
        (4..=6)
            .map(|id| (id, Change::Insert(row(id, "initial"))))
            .collect(),
    )
    .await;
    let original: Vec<_> = [1, 4]
        .into_iter()
        .map(|id| {
            f.store
                .lookup(
                    &f.schema.table_id,
                    &f.schema.encode_key(&row(id, "")).unwrap(),
                )
                .unwrap()
                .unwrap()
                .data_file_id
                .0
        })
        .collect();
    assert_ne!(original[0], original[1]);
    f.publish(
        30,
        vec![
            (1, Change::Update(row(1, "first"))),
            (4, Change::Update(row(4, "first"))),
        ],
    )
    .await;
    let view = SnapshotView::current(&f.head).await.unwrap();
    let vectors: Vec<_> = view
        .live_files
        .values()
        .filter(|entry| entry.file_format() == DataFileFormat::Puffin)
        .collect();
    assert_eq!(vectors.len(), 2);
    assert_eq!(
        vectors[0].file_path(),
        vectors[1].file_path(),
        "two target vectors share one physical Puffin file"
    );
    assert_ne!(
        vectors[0].data_file().content_offset(),
        vectors[1].data_file().content_offset()
    );
    let old_vector_ids: BTreeSet<_> = view
        .live_files
        .iter()
        .filter(|(_, entry)| entry.file_format() == DataFileFormat::Puffin)
        .map(|(id, _)| id.clone())
        .collect();
    f.publish(40, vec![(2, Change::Delete), (5, Change::Delete)])
        .await;
    let view = SnapshotView::current(&f.head).await.unwrap();
    assert!(
        old_vector_ids
            .iter()
            .all(|id| !view.live_files.contains_key(id)),
        "replacement retires old blob identities"
    );
    for path in &original {
        let deletes = view.applicable_deletes(path).unwrap();
        assert_eq!(deletes.len(), 1);
        assert_eq!(
            deletes[0].data_file().record_count(),
            2,
            "the new vector includes earlier deletes"
        );
    }
    f.publish(50, vec![(1, Change::Update(row(1, "second")))])
        .await;
    let (epoch, collapsed) = collapse_fixture(
        &f.store,
        &f.head,
        &f.schema,
        SourceId("v3-pipeline".into()),
        PgLsn(60),
        [
            (
                f.schema.encode_key(&row(4, "")).unwrap(),
                Change::Update(row(4, "recovered")),
            ),
            (f.schema.encode_key(&row(3, "")).unwrap(), Change::Delete),
        ],
    );
    f.catalog.lose_next_response_and_disconnect();
    let error = f
        .publisher()
        .publish(&f.head, &f.schema, collapsed)
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("lost catalog response"),
        "{error:#}"
    );
    assert_eq!(
        f.store.operation(&epoch.id).unwrap().unwrap().phase,
        OperationPhase::Prepared
    );
    // Drop the actual RocksDB handle before recovery; no process-local cache can prove the commit.
    let placeholder = f.scratch();
    drop(std::mem::replace(&mut f.store, placeholder));
    f.store = StateStore::open(f.temp.path().join("index"), options()).unwrap();
    f.catalog.reconnect();
    f.refresh().await;
    let committed = f.head.metadata().current_snapshot_id();
    let count = f.head.metadata().snapshots().len();
    assert_eq!(
        f.publisher().recover(&f.head, &epoch.id).await.unwrap(),
        committed
    );
    assert_eq!(
        f.publisher().recover(&f.head, &epoch.id).await.unwrap(),
        committed
    );
    f.refresh().await;
    assert_eq!(f.head.metadata().snapshots().len(), count);
    assert_eq!(
        f.store
            .table_state(&f.schema.table_id)
            .unwrap()
            .materialized_lsn,
        PgLsn(60)
    );
    f.assert_rows(vec![
        row(1, "second"),
        row(4, "recovered"),
        row(6, "initial"),
    ])
    .await;
    let live = f.live().await;
    assert_eq!(live.len(), 3);
    assert_eq!(
        live.values()
            .filter_map(|(_, (id, _))| *id)
            .collect::<BTreeSet<_>>()
            .len(),
        3
    );
    f.export("cumulative").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compaction_and_external_reconciliation_preserve_lineage_and_resume_updates() {
    let mut f = Fixture::new(FormatVersion::V3).await;
    f.publish(
        10,
        (1..=4)
            .map(|id| (id, Change::Insert(row(id, "initial"))))
            .collect(),
    )
    .await;
    f.publish(
        20,
        vec![(1, Change::Update(row(1, "updated"))), (2, Change::Delete)],
    )
    .await;
    let before = f.live().await;
    assert!(
        before
            .values()
            .all(|(_, (id, seq))| id.is_some() && seq.is_some())
    );
    let old_locations: Vec<_> = [1, 3, 4]
        .into_iter()
        .map(|id| {
            f.store
                .lookup(
                    &f.schema.table_id,
                    &f.schema.encode_key(&row(id, "")).unwrap(),
                )
                .unwrap()
                .unwrap()
                .data_file_id
        })
        .collect();
    assert!(
        f.maintenance()
            .compact(&f.head, &f.schema, f.scratch())
            .await
            .unwrap()
            .is_some()
    );
    f.refresh().await;
    assert_eq!(
        f.live().await,
        before,
        "physical compaction must retain row IDs and update sequences"
    );
    let view = SnapshotView::current(&f.head).await.unwrap();
    assert!(
        old_locations
            .iter()
            .any(|file| !view.live_files.contains_key(&file.0))
    );
    // An independent writer preserves the scanned metadata but has no access to the CDC index.
    let operation = OperationId("external-v3-rewrite".into());
    let mut writer = DataWriter::new(
        f.head.file_io().clone(),
        f.head.metadata().location(),
        &operation,
        f.schema.clone(),
        0,
        WriterConfig::default(),
    )
    .unwrap()
    .with_row_lineage()
    .unwrap();
    let rows: Vec<_> = before.values().map(|(row, _)| row.clone()).collect();
    let lineage: Vec<_> = before
        .values()
        .map(|(_, (row_id, last_updated_sequence_number))| RowLineage {
            row_id: *row_id,
            last_updated_sequence_number: *last_updated_sequence_number,
        })
        .collect();
    writer
        .write_with_lineage(&rows, Some(&lineage), PgLsn(0))
        .await
        .unwrap();
    f.head = RewriteFilesAction::new(&f.head, operation.0)
        .with_operation_id_key("external.operation-id")
        .unwrap()
        .remove_data_files(
            view.live_files
                .iter()
                .filter(|(_, e)| e.content_type() == DataContentType::Data)
                .map(|(id, _)| id.clone()),
        )
        .remove_delete_files(
            view.live_files
                .iter()
                .filter(|(_, e)| e.content_type() == DataContentType::PositionDeletes)
                .map(|(id, _)| id.clone()),
        )
        .add_data_files(writer.close().await.unwrap())
        .commit(f.catalog.as_ref(), &f.head)
        .await
        .unwrap()
        .table;
    f.maintenance()
        .reconcile(&f.head, &f.schema, f.scratch())
        .await
        .unwrap();
    assert_eq!(f.live().await, before);
    assert_eq!(
        f.store
            .table_state(&f.schema.table_id)
            .unwrap()
            .materialized_lsn,
        PgLsn(20)
    );
    f.publish(
        30,
        vec![
            (3, Change::Update(row(3, "after-external"))),
            (4, Change::Delete),
        ],
    )
    .await;
    f.assert_rows(vec![row(1, "updated"), row(3, "after-external")])
        .await;
    for id in [1, 3] {
        let location = f
            .store
            .lookup(
                &f.schema.table_id,
                &f.schema.encode_key(&row(id, "")).unwrap(),
            )
            .unwrap()
            .unwrap();
        assert!(
            SnapshotView::current(&f.head)
                .await
                .unwrap()
                .live_files
                .contains_key(&location.data_file_id.0)
        );
    }
    f.export("compacted").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn upgrade_unions_legacy_deletes_into_vectors_without_resurrecting_rows() {
    let mut f = Fixture::new(FormatVersion::V2).await;
    f.publish(
        10,
        (1..=5)
            .map(|id| (id, Change::Insert(row(id, "legacy"))))
            .collect(),
    )
    .await;
    let original = f
        .store
        .lookup(
            &f.schema.table_id,
            &f.schema.encode_key(&row(3, "")).unwrap(),
        )
        .unwrap()
        .unwrap()
        .data_file_id
        .0;
    f.publish(
        20,
        vec![
            (1, Change::Delete),
            (2, Change::Update(row(2, "before-upgrade"))),
        ],
    )
    .await;
    assert!(
        SnapshotView::current(&f.head)
            .await
            .unwrap()
            .live_files
            .values()
            .any(
                |entry| entry.content_type() == DataContentType::PositionDeletes
                    && entry.file_format() == DataFileFormat::Parquet
            )
    );
    f.head = f
        .catalog
        .update_table(
            TableCommit::builder()
                .ident(f.head.identifier().clone())
                .requirements(vec![])
                .updates(vec![TableUpdate::UpgradeFormatVersion {
                    format_version: FormatVersion::V3,
                }])
                .build(),
        )
        .await
        .unwrap();
    f.publish(30, vec![(3, Change::Delete)]).await;
    let view = SnapshotView::current(&f.head).await.unwrap();
    let deletes = view.applicable_deletes(&original).unwrap();
    assert_eq!(deletes.len(), 1);
    assert_eq!(deletes[0].file_format(), DataFileFormat::Puffin);
    assert_eq!(
        deletes[0].data_file().record_count(),
        3,
        "v3 vector includes both legacy tombstones"
    );
    f.assert_rows(vec![
        row(2, "before-upgrade"),
        row(4, "legacy"),
        row(5, "legacy"),
    ])
    .await;
    f.publish(40, vec![(4, Change::Update(row(4, "after-upgrade")))])
        .await;
    assert_eq!(
        SnapshotView::current(&f.head)
            .await
            .unwrap()
            .applicable_deletes(&original)
            .unwrap()[0]
            .data_file()
            .record_count(),
        4
    );
    f.assert_rows(vec![
        row(2, "before-upgrade"),
        row(4, "after-upgrade"),
        row(5, "legacy"),
    ])
    .await;
    assert!(
        f.maintenance()
            .compact(&f.head, &f.schema, f.scratch())
            .await
            .unwrap()
            .is_some()
    );
    f.refresh().await;
    f.assert_rows(vec![
        row(2, "before-upgrade"),
        row(4, "after-upgrade"),
        row(5, "legacy"),
    ])
    .await;
    assert!(
        f.live()
            .await
            .values()
            .all(|(_, (row_id, _))| row_id.is_some()),
        "rewritten legacy rows acquire IDs"
    );
    f.export("upgraded").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn speculative_compaction_catches_up_cumulative_vector_replacements() {
    let mut f = Fixture::new(FormatVersion::V3).await;
    for (lsn, ids) in [(10, 1..=6), (20, 7..=12)] {
        f.publish(
            lsn,
            ids.map(|id| (id, Change::Insert(row(id, "initial"))))
                .collect(),
        )
        .await;
    }
    f.publish(30, vec![(1, Change::Delete), (7, Change::Delete)])
        .await;
    let base = SnapshotView::current(&f.head).await.unwrap();
    assert_eq!(
        base.live_files
            .values()
            .filter(|e| e.file_format() == DataFileFormat::Puffin)
            .count(),
        2
    );
    let maintenance = f.maintenance();
    // Finish the expensive data build, then advance CDC while its output is
    // still speculative. This makes the race deterministic without timing sleeps.
    let ready = maintenance
        .start_compaction(&f.head, &f.schema, f.scratch())
        .await
        .unwrap()
        .unwrap()
        .wait()
        .await
        .unwrap();
    assert!(f.store.pending_operations().unwrap().is_empty());
    f.publish(
        40,
        vec![
            (2, Change::Update(row(2, "late"))),
            (8, Change::Update(row(8, "late"))),
        ],
    )
    .await;
    f.publish(
        50,
        vec![
            (3, Change::Update(row(3, "later"))),
            (8, Change::Update(row(8, "twice"))),
            (4, Change::Delete),
        ],
    )
    .await;
    let before = f.live().await;
    let expected = vec![
        row(2, "late"),
        row(3, "later"),
        row(5, "initial"),
        row(6, "initial"),
        row(8, "twice"),
        row(9, "initial"),
        row(10, "initial"),
        row(11, "initial"),
        row(12, "initial"),
    ];
    f.assert_rows(expected.clone()).await;
    let late = SnapshotView::current(&f.head).await.unwrap();
    assert!(
        base.live_files
            .iter()
            .filter(|(_, e)| e.file_format() == DataFileFormat::Puffin)
            .all(|(id, _)| !late.live_files.contains_key(id)),
        "both original vectors were cumulatively replaced"
    );
    maintenance
        .finish_compaction(&f.head, &f.schema, ready)
        .await
        .unwrap()
        .unwrap();
    f.refresh().await;
    f.assert_rows(expected).await;
    assert_eq!(
        f.live().await,
        before,
        "catch-up preserves each surviving logical row and lineage"
    );
    assert_eq!(
        f.store
            .table_state(&f.schema.table_id)
            .unwrap()
            .materialized_lsn,
        PgLsn(50)
    );
    let after = SnapshotView::current(&f.head).await.unwrap();
    let masks: Vec<_> = after
        .live_files
        .values()
        .filter(|entry| {
            entry.file_format() == DataFileFormat::Puffin
                && entry
                    .data_file()
                    .referenced_data_file()
                    .is_some_and(|path| !late.live_files.contains_key(&path))
        })
        .collect();
    assert!(
        !masks.is_empty(),
        "late changes must mask stale rows in the speculative output"
    );
    for mask in masks {
        let target = mask.data_file().referenced_data_file().unwrap();
        assert!(after.live_files.contains_key(&target));
        assert!(mask.data_file().content_offset().is_some());
        assert!(mask.data_file().content_size_in_bytes().is_some());
    }
    f.publish(60, vec![(5, Change::Update(row(5, "after-catch-up")))])
        .await;
    assert_eq!(f.live().await[&5].0, row(5, "after-catch-up"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn external_first_v3_manifest_rewrite_reconciles_inherited_row_ids() {
    use flow_iceberg_ext::{ManifestCache, ManifestRewritePolicy, RewriteManifestsAction};
    let mut f = Fixture::new(FormatVersion::V2).await;
    f.publish(
        10,
        vec![
            (1, Change::Insert(row(1, "legacy"))),
            (2, Change::Insert(row(2, "legacy"))),
        ],
    )
    .await;
    f.publish(
        20,
        vec![
            (3, Change::Insert(row(3, "legacy"))),
            (4, Change::Insert(row(4, "legacy"))),
        ],
    )
    .await;
    let before = SnapshotView::current(&f.head).await.unwrap();
    assert!(
        before
            .live_files
            .values()
            .all(|e| e.data_file().first_row_id().is_none())
    );
    f.head = f
        .catalog
        .update_table(
            TableCommit::builder()
                .ident(f.head.identifier().clone())
                .requirements(vec![])
                .updates(vec![TableUpdate::UpgradeFormatVersion {
                    format_version: FormatVersion::V3,
                }])
                .build(),
        )
        .await
        .unwrap();
    let rewrite = RewriteManifestsAction::plan(
        &f.head,
        "external-first-v3-manifests",
        &ManifestRewritePolicy {
            min_manifest_count: 2,
            ..Default::default()
        },
    )
    .await
    .unwrap()
    .unwrap();
    rewrite
        .write_artifacts(&f.head, &ManifestCache::default())
        .await
        .unwrap();
    // Reuse the artifact writer, but publish as an external engine: the
    // snapshot carries no service operation ID or service recovery claim.
    use iceberg::spec::{Operation, Snapshot, SnapshotReference, SnapshotRetention, Summary};
    let plan = serde_json::to_value(&rewrite).unwrap();
    let snapshot_id = plan["snapshot_id"].as_i64().unwrap();
    let snapshot = Snapshot::builder()
        .with_snapshot_id(snapshot_id)
        .with_parent_snapshot_id(f.head.metadata().current_snapshot_id())
        .with_sequence_number(f.head.metadata().next_sequence_number())
        .with_timestamp_ms(f.head.metadata().current_snapshot().unwrap().timestamp_ms() + 1)
        .with_schema_id(f.head.metadata().current_schema_id())
        .with_manifest_list(rewrite.artifacts().pop().unwrap())
        .with_summary(Summary {
            operation: Operation::Replace,
            additional_properties: Default::default(),
        })
        .with_row_range(f.head.metadata().next_row_id(), 4)
        .build();
    f.head = f
        .catalog
        .update_table(
            TableCommit::builder()
                .ident(f.head.identifier().clone())
                .requirements(vec![])
                .updates(vec![
                    TableUpdate::AddSnapshot { snapshot },
                    TableUpdate::SetSnapshotRef {
                        ref_name: "main".into(),
                        reference: SnapshotReference::new(
                            snapshot_id,
                            SnapshotRetention::branch(None, None, None),
                        ),
                    },
                ])
                .build(),
        )
        .await
        .unwrap();
    let after = SnapshotView::current(&f.head).await.unwrap();
    assert_eq!(
        after.live_files.keys().collect::<Vec<_>>(),
        before.live_files.keys().collect::<Vec<_>>()
    );
    assert!(
        after
            .live_files
            .values()
            .all(|e| e.data_file().first_row_id().is_some()),
        "the first v3 snapshot assigns IDs to unchanged legacy files"
    );
    f.maintenance()
        .reconcile(&f.head, &f.schema, f.scratch())
        .await
        .unwrap();
    assert_eq!(
        f.store.table_state(&f.schema.table_id).unwrap().snapshot_id,
        f.head.metadata().current_snapshot_id()
    );
    assert_eq!(
        f.store
            .table_state(&f.schema.table_id)
            .unwrap()
            .materialized_lsn,
        PgLsn(20)
    );
    assert_eq!(
        f.live()
            .await
            .values()
            .filter_map(|(_, (id, _))| *id)
            .collect::<BTreeSet<_>>()
            .len(),
        4
    );
    f.publish(
        30,
        vec![(1, Change::Update(row(1, "updated"))), (4, Change::Delete)],
    )
    .await;
    f.assert_rows(vec![row(1, "updated"), row(2, "legacy"), row(3, "legacy")])
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sparse_vectors_in_shared_puffin_do_not_pause_healthy_tables() {
    use flow_compactor::{Level, PublicationPressure};
    let mut f = Fixture::new(FormatVersion::V3).await;
    f.publish(
        10,
        (0..200)
            .map(|id| (id, Change::Insert(row(id, "initial"))))
            .collect(),
    )
    .await;
    let live = f.live().await;
    let input = SnapshotView::current(&f.head).await.unwrap();
    let mut output = Vec::new();
    // Real externally compacted L2 files, each already above the configured
    // target size. Neither small-file count nor density should cause pressure.
    for part in 0..2 {
        let mut writer = DataWriter::new(
            f.head.file_io().clone(),
            f.head.metadata().location(),
            &OperationId(format!("flow-l2-pressure-{part}")),
            f.schema.clone(),
            0,
            WriterConfig::default(),
        )
        .unwrap()
        .with_row_lineage()
        .unwrap();
        let values: Vec<_> = live
            .range(part * 100..(part + 1) * 100)
            .map(|(_, value)| value)
            .collect();
        let rows: Vec<_> = values.iter().map(|(row, _)| row.clone()).collect();
        let lineage: Vec<_> = values
            .iter()
            .map(|(_, (row_id, last_updated_sequence_number))| RowLineage {
                row_id: *row_id,
                last_updated_sequence_number: *last_updated_sequence_number,
            })
            .collect();
        writer
            .write_with_lineage(&rows, Some(&lineage), PgLsn(0))
            .await
            .unwrap();
        output.extend(writer.close().await.unwrap());
    }
    f.head = RewriteFilesAction::new(&f.head, "external-pressure-layout")
        .with_operation_id_key("external.operation-id")
        .unwrap()
        .remove_data_files(input.live_files.keys().cloned())
        .add_data_files(output)
        .commit(f.catalog.as_ref(), &f.head)
        .await
        .unwrap()
        .table;
    f.maintenance()
        .reconcile(&f.head, &f.schema, f.scratch())
        .await
        .unwrap();
    f.publish(20, vec![(0, Change::Delete), (100, Change::Delete)])
        .await;
    let view = SnapshotView::current(&f.head).await.unwrap();
    let vectors: Vec<_> = view
        .live_files
        .values()
        .filter(|e| e.file_format() == DataFileFormat::Puffin)
        .collect();
    assert_eq!(vectors.len(), 2);
    assert_eq!(vectors[0].file_path(), vectors[1].file_path());
    let maintenance = TableMaintenance::new(
        f.store.clone(),
        f.catalog.clone(),
        Policy {
            delete_files_soft: 2,
            delete_files_hard: 2,
            l1_target_bytes: 1,
            l2_target_bytes: 1,
            ..Default::default()
        },
        WriterConfig::default(),
    )
    .unwrap();
    let inventory = maintenance
        .inventory(&f.head, f.schema.table_id)
        .await
        .unwrap();
    assert_eq!(inventory.files.len(), 2);
    assert!(inventory.files.iter().all(|file| file.level == Level::L2
        && file.deleted_rows == 1
        && file.delete_file_count == 1));
    assert_eq!(
        inventory.debt.pressure,
        PublicationPressure::Healthy,
        "disjoint sparse vectors must not be charged as overlapping delete files"
    );
    f.publish(30, vec![(1, Change::Update(row(1, "still-running")))])
        .await;
    let live = f.live().await;
    assert_eq!(live.len(), 198);
    assert_eq!(live[&1].0, row(1, "still-running"));
    assert!(!live.contains_key(&0) && !live.contains_key(&100));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn external_rewrite_of_vector_targets_replans_speculative_compaction() {
    let mut f = Fixture::with_uri_location(FormatVersion::V3).await;
    for (lsn, ids) in [(10, 1..=6), (20, 7..=12)] {
        f.publish(
            lsn,
            ids.map(|id| (id, Change::Insert(row(id, "initial"))))
                .collect(),
        )
        .await;
    }
    f.publish(30, vec![(1, Change::Delete), (7, Change::Delete)])
        .await;
    let maintenance = f.maintenance();
    let ready = maintenance
        .start_compaction(&f.head, &f.schema, f.scratch())
        .await
        .unwrap()
        .unwrap()
        .wait()
        .await
        .unwrap();
    let before = f.live().await;
    let view = SnapshotView::current(&f.head).await.unwrap();
    // Catch-up visits required files in ID order; reach a vector before its target.
    assert!(view.live_files.iter().any(|(id, entry)| {
        entry.file_format() == DataFileFormat::Puffin
            && entry
                .data_file()
                .referenced_data_file()
                .is_some_and(|target| id.as_str() < target.as_str())
    }));
    // An external rewrite retires every selected input together with its vector.
    let operation = OperationId("external-v3-input-rewrite".into());
    let mut writer = DataWriter::new(
        f.head.file_io().clone(),
        f.head.metadata().location(),
        &operation,
        f.schema.clone(),
        0,
        WriterConfig::default(),
    )
    .unwrap()
    .with_row_lineage()
    .unwrap();
    let rows: Vec<_> = before.values().map(|(row, _)| row.clone()).collect();
    let lineage: Vec<_> = before
        .values()
        .map(|(_, (row_id, last_updated_sequence_number))| RowLineage {
            row_id: *row_id,
            last_updated_sequence_number: *last_updated_sequence_number,
        })
        .collect();
    writer
        .write_with_lineage(&rows, Some(&lineage), PgLsn(0))
        .await
        .unwrap();
    f.head = RewriteFilesAction::new(&f.head, operation.0)
        .with_operation_id_key("external.operation-id")
        .unwrap()
        .remove_data_files(
            view.live_files
                .iter()
                .filter(|(_, e)| e.content_type() == DataContentType::Data)
                .map(|(id, _)| id.clone()),
        )
        .remove_delete_files(
            view.live_files
                .iter()
                .filter(|(_, e)| e.content_type() == DataContentType::PositionDeletes)
                .map(|(id, _)| id.clone()),
        )
        .add_data_files(writer.close().await.unwrap())
        .commit(f.catalog.as_ref(), &f.head)
        .await
        .unwrap()
        .table;
    f.maintenance()
        .reconcile(&f.head, &f.schema, f.scratch())
        .await
        .unwrap();
    let rejected = maintenance
        .finish_compaction(&f.head, &f.schema, ready)
        .await
        .unwrap_err();
    assert!(
        rejected.is::<flow_coordinator::ReplanRequired>(),
        "{rejected:#}"
    );
    assert!(f.store.pending_operations().unwrap().is_empty());
    f.refresh().await;
    assert_eq!(f.live().await, before);
}

async fn prior_vector_scenario(batch_bytes: usize) -> Fixture {
    let mut f = Fixture::new(FormatVersion::V3).await;
    f.batch_bytes = batch_bytes;
    f.publish(
        10,
        (0..800)
            .map(|id| (id, Change::Insert(row(id, "initial"))))
            .collect(),
    )
    .await;
    f.publish(20, (0..600).map(|id| (id, Change::Delete)).collect())
        .await;
    f.publish(30, vec![(700, Change::Update(row(700, "late")))])
        .await;
    f.publish(40, vec![(650, Change::Delete), (750, Change::Delete)])
        .await;
    f
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn prior_vectors_merge_in_memory_or_through_disk_identically() {
    // The 600-position vector exceeds a 1 KiB batch, forcing disk staging.
    let memory = prior_vector_scenario(1 << 20).await;
    let disk = prior_vector_scenario(1024).await;
    let expected: Vec<_> = (600..800)
        .filter(|id| ![650, 750].contains(id))
        .map(|id| row(id, if id == 700 { "late" } else { "initial" }))
        .collect();
    for f in [&memory, &disk] {
        f.assert_rows(expected.clone()).await;
        let view = SnapshotView::current(&f.head).await.unwrap();
        let vectors: Vec<_> = view
            .live_files
            .values()
            .filter(|entry| entry.file_format() == DataFileFormat::Puffin)
            .collect();
        assert_eq!(
            vectors.len(),
            1,
            "one cumulative vector for the shared file"
        );
        assert_eq!(vectors[0].data_file().record_count(), 603);
        assert!(flow_iceberg_ext::delete_content_size(vectors[0].data_file()) > 1024);
    }
    assert_eq!(memory.live().await, disk.live().await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn corrupt_prior_vector_fails_publication_without_dropping_deletes() {
    let mut f = Fixture::new(FormatVersion::V3).await;
    f.publish(
        10,
        (1..=4)
            .map(|id| (id, Change::Insert(row(id, "initial"))))
            .collect(),
    )
    .await;
    f.publish(20, vec![(1, Change::Delete)]).await;
    let view = SnapshotView::current(&f.head).await.unwrap();
    let vector = view
        .live_files
        .values()
        .find(|entry| entry.file_format() == DataFileFormat::Puffin)
        .unwrap();
    let path = vector.file_path().trim_start_matches("file://").to_owned();
    let original = std::fs::read(&path).unwrap();
    let mut corrupt = original.clone();
    // Flip a bitmap byte inside the blob; the CRC must reject it.
    corrupt[vector.data_file().content_offset().unwrap() as usize + 10] ^= 1;
    std::fs::write(&path, &corrupt).unwrap();
    let snapshot = f.head.metadata().current_snapshot_id();
    let (_, collapsed) = collapse_fixture(
        &f.store,
        &f.head,
        &f.schema,
        SourceId("v3-pipeline".into()),
        PgLsn(30),
        [(
            f.schema.encode_key(&row(2, "")).unwrap(),
            Change::Update(row(2, "late")),
        )]
        .into_iter(),
    );
    let error = f
        .publisher()
        .publish(&f.head, &f.schema, collapsed)
        .await
        .unwrap_err();
    assert!(
        format!("{error:#}").contains("checksum mismatch"),
        "{error:#}"
    );
    f.refresh().await;
    assert_eq!(f.head.metadata().current_snapshot_id(), snapshot);
    std::fs::write(&path, &original).unwrap();
    f.assert_rows(vec![
        row(2, "initial"),
        row(3, "initial"),
        row(4, "initial"),
    ])
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn catch_up_charges_each_intermediate_vector_once() {
    let mut f = Fixture::new(FormatVersion::V3).await;
    for (lsn, ids) in [(10, 1..=6), (20, 7..=12)] {
        f.publish(
            lsn,
            ids.map(|id| (id, Change::Insert(row(id, "initial"))))
                .collect(),
        )
        .await;
    }
    f.publish(30, vec![(1, Change::Delete), (7, Change::Delete)])
        .await;
    // History reads A {1} -> {2} and B {1} -> {2}, then A {2} -> {4}: ten
    // distinct positions. Charging A's middle vector twice would need twelve.
    let maintenance = f.maintenance_with(
        &f.store,
        Policy {
            max_delete_input_rows: 11,
            ..Policy::default()
        },
    );
    let ready = maintenance
        .start_compaction(&f.head, &f.schema, f.scratch())
        .await
        .unwrap()
        .unwrap()
        .wait()
        .await
        .unwrap();
    f.publish(
        40,
        vec![
            (2, Change::Update(row(2, "late"))),
            (8, Change::Update(row(8, "late"))),
        ],
    )
    .await;
    f.publish(
        50,
        vec![(3, Change::Update(row(3, "later"))), (4, Change::Delete)],
    )
    .await;
    let before = f.live().await;
    assert!(
        maintenance
            .finish_compaction(&f.head, &f.schema, ready)
            .await
            .unwrap()
            .is_some()
    );
    f.refresh().await;
    assert_eq!(f.live().await, before);
}

/// Every indexed key with its location, fingerprint and data sequence.
fn index_contents(
    store: &StateStore,
    schema: &TableSchema,
    view: &SnapshotView,
) -> BTreeMap<(FileId, u64), Vec<u8>> {
    let mut rows = BTreeMap::new();
    for entry in view
        .live_files
        .values()
        .filter(|entry| entry.content_type() == DataContentType::Data)
    {
        let file = FileId(entry.file_path().into());
        for item in store.file_rows(&schema.table_id, &file) {
            let (position, key) = item.unwrap();
            let location = store.lookup(&schema.table_id, &key).unwrap().unwrap();
            assert_eq!(
                (&location.data_file_id, location.row_position),
                (&file, position)
            );
            let mut summary = key.0.clone();
            summary.extend_from_slice(&location.row_fingerprint);
            summary.extend_from_slice(&location.data_sequence_number.to_be_bytes());
            rows.insert((file.clone(), position), summary);
        }
    }
    rows
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stale_checkpoint_catches_up_through_replaced_vectors() {
    let mut f = Fixture::new(FormatVersion::V3).await;
    f.publish(
        10,
        (1..=6)
            .map(|id| (id, Change::Insert(row(id, "initial"))))
            .collect(),
    )
    .await;
    f.publish(
        20,
        vec![(1, Change::Delete), (2, Change::Update(row(2, "a")))],
    )
    .await;
    let checkpoint = f.temp.path().join("checkpoint");
    f.store.checkpoint(&checkpoint).unwrap();
    let at_checkpoint = f.store.table_state(&f.schema.table_id).unwrap();
    // Each epoch replaces the shared file's vector with a superset.
    f.publish(
        30,
        vec![(3, Change::Delete), (2, Change::Update(row(2, "b")))],
    )
    .await;
    f.publish(40, vec![(4, Change::Update(row(4, "c")))]).await;
    let current = f.store.table_state(&f.schema.table_id).unwrap();
    let restored = StateStore::open(&checkpoint, options()).unwrap();
    let stats = f
        .maintenance_with(&restored, Policy::default())
        .catch_up_index(
            &f.head,
            &f.schema,
            restored.clone(),
            f.scratch(),
            at_checkpoint.snapshot_id,
            current.materialized_lsn,
            current.schema_version,
        )
        .await
        .unwrap();
    assert!(stats.changed_files > 0 && stats.removed_rows > 0);
    let rebuilt = StateStore::open(f.temp.path().join("rebuilt"), options()).unwrap();
    f.maintenance_with(&rebuilt, Policy::default())
        .rebuild_index(
            &f.head,
            &f.schema,
            rebuilt.clone(),
            f.scratch(),
            current.materialized_lsn,
            current.schema_version,
        )
        .await
        .unwrap();
    let view = SnapshotView::current(&f.head).await.unwrap();
    assert_eq!(
        restored.table_state(&f.schema.table_id).unwrap(),
        rebuilt.table_state(&f.schema.table_id).unwrap()
    );
    assert_eq!(
        index_contents(&restored, &f.schema, &view),
        index_contents(&rebuilt, &f.schema, &view)
    );
}

/// Every live row is indexed at its exact physical location and data sequence,
/// through any lazy remap, and per-file live counts equal the effective rows.
async fn assert_index_exact(f: &Fixture) {
    let view = SnapshotView::current(&f.head).await.unwrap();
    let files: BTreeSet<_> = view
        .live_files
        .values()
        .filter(|entry| entry.content_type() == DataContentType::Data)
        .map(|entry| FileId(entry.file_path().into()))
        .collect();
    let mut live = Vec::new();
    scan_live_files(
        &f.head,
        &f.schema,
        &view,
        &files,
        &f.scratch(),
        "index-audit",
        2,
        &ReadLimits::default(),
        async |batch| {
            for (row, position) in batch.rows.iter().zip(&batch.positions) {
                live.push((
                    f.schema.encode_key(row).unwrap(),
                    batch.file_id.clone(),
                    *position,
                ));
            }
            Ok(())
        },
    )
    .await
    .unwrap();
    let keys: Vec<_> = live.iter().map(|(key, _, _)| key.clone()).collect();
    let located = f.store.lookup_many(&f.schema.table_id, &keys).unwrap();
    for ((key, file, position), location) in live.iter().zip(located) {
        let location = location.unwrap_or_else(|| panic!("{key:?} is not indexed"));
        assert_eq!(
            (&location.data_file_id, location.row_position),
            (file, *position),
            "{key:?} is indexed at a different row"
        );
        assert_eq!(
            Some(location.data_sequence_number),
            view.live_files[&file.0].sequence_number
        );
    }
    let files: Vec<_> = files.into_iter().collect();
    let counts = f
        .store
        .file_live_row_counts(
            &f.schema.table_id,
            f.head.metadata().current_snapshot_id(),
            &files,
        )
        .unwrap();
    for (file, count) in files.iter().zip(counts) {
        assert_eq!(
            count,
            live.iter().filter(|(_, owner, _)| owner == file).count() as u64,
            "live count of {}",
            file.0
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn v3_rewrites_with_or_without_lazy_remap_keep_vectors_and_index_exact() {
    for lazy in [false, true] {
        let mut f = Fixture::new(FormatVersion::V3).await;
        let maintenance = f.maintenance_with(
            &f.store,
            Policy {
                lazy_index_remap: lazy,
                ..Policy::default()
            },
        );
        for (lsn, ids) in [(10, 1..=6), (20, 7..=12)] {
            f.publish(
                lsn,
                ids.map(|id| (id, Change::Insert(row(id, "initial"))))
                    .collect(),
            )
            .await;
        }
        // Deletion vectors on both inputs at the build base.
        f.publish(30, vec![(1, Change::Delete), (7, Change::Delete)])
            .await;
        let ready = maintenance
            .start_compaction(&f.head, &f.schema, f.scratch())
            .await
            .unwrap()
            .unwrap()
            .wait()
            .await
            .unwrap();
        // Late changes replace both inputs' vectors cumulatively; catch-up
        // translates them into masks on the outputs.
        f.publish(
            40,
            vec![
                (2, Change::Update(row(2, "late"))),
                (8, Change::Delete),
                (13, Change::Insert(row(13, "new"))),
            ],
        )
        .await;
        maintenance
            .finish_compaction(&f.head, &f.schema, ready)
            .await
            .unwrap()
            .unwrap();
        f.refresh().await;
        assert_eq!(f.store.has_remaps(&f.schema.table_id).unwrap(), lazy);
        let mut expected: BTreeMap<i64, Row> = [3, 4, 5, 6, 9, 10, 11, 12]
            .into_iter()
            .map(|id| (id, row(id, "initial")))
            .collect();
        expected.insert(2, row(2, "late"));
        expected.insert(13, row(13, "new"));
        f.assert_rows(expected.values().cloned().collect()).await;
        assert_index_exact(&f).await;

        // CDC on moved rows merges each output's prior vector (the catch-up
        // mask) with new positions at the translated locations.
        let before = f.live().await;
        f.publish(
            50,
            vec![
                (3, Change::Update(row(3, "after"))),
                (9, Change::Delete),
                (12, Change::Update(row(12, "after"))),
                (4, Change::Delete),
            ],
        )
        .await;
        for id in [9, 4] {
            expected.remove(&id);
        }
        for id in [3, 12] {
            expected.insert(id, row(id, "after"));
        }
        f.assert_rows(expected.values().cloned().collect()).await;
        assert_index_exact(&f).await;
        let after = f.live().await;
        for id in [5, 6, 10, 11] {
            assert_eq!(after[&id], before[&id], "untouched rows keep their lineage");
        }
        let view = SnapshotView::current(&f.head).await.unwrap();
        let mut targets = BTreeSet::new();
        for entry in view
            .live_files
            .values()
            .filter(|entry| entry.file_format() == DataFileFormat::Puffin)
        {
            let target = entry.data_file().referenced_data_file().unwrap();
            assert!(
                view.live_files.contains_key(&target),
                "vector of a live file"
            );
            assert!(
                targets.insert(target),
                "one cumulative vector per data file"
            );
        }

        // A strict rewrite of the outputs chains a second remap, then more CDC.
        assert!(
            maintenance
                .compact(&f.head, &f.schema, f.scratch())
                .await
                .unwrap()
                .is_some()
        );
        f.refresh().await;
        f.assert_rows(expected.values().cloned().collect()).await;
        assert_index_exact(&f).await;
        f.publish(
            60,
            vec![
                (5, Change::Delete),
                (10, Change::Update(row(10, "last"))),
                (14, Change::Insert(row(14, "new"))),
            ],
        )
        .await;
        expected.remove(&5);
        expected.insert(10, row(10, "last"));
        expected.insert(14, row(14, "new"));
        f.assert_rows(expected.values().cloned().collect()).await;
        assert_index_exact(&f).await;

        // Cleanup materializes every remap without changing any lookup.
        f.store.materialize_remaps(&f.schema.table_id).unwrap();
        assert!(!f.store.has_remaps(&f.schema.table_id).unwrap());
        assert_index_exact(&f).await;
        f.publish(70, vec![(6, Change::Update(row(6, "final")))])
            .await;
        expected.insert(6, row(6, "final"));
        f.assert_rows(expected.values().cloned().collect()).await;
        assert_index_exact(&f).await;
    }
}
