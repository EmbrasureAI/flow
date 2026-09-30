use flow_compactor::Policy;
use flow_coordinator::TableMaintenance;
use flow_iceberg_ext::RowDeltaAction;
use flow_materializer::{DataWriter, WriterConfig, iceberg_schema, rows_from_batch};
use flow_model::{Column, ColumnType, OperationId, PgLsn, Row, TableId, TableSchema, Value};
use flow_state_store::{
    IndexDelta, OperationKind, PreparedOperation, StateStore, StateStoreOptions,
};
use futures::TryStreamExt;
use iceberg::{
    Catalog, CatalogBuilder, NamespaceIdent, TableCreation,
    memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder},
    spec::FormatVersion,
};
use std::{collections::HashMap, sync::Arc};
use tempfile::TempDir;

#[tokio::test]
async fn coordinator_compaction_and_full_rebuild_preserve_the_published_rows_and_index() {
    let temp = TempDir::new().unwrap();
    let open = |name: &str| {
        StateStore::open(
            temp.path().join(name),
            StateStoreOptions {
                apply_batch_rows: 2,
                ..Default::default()
            },
        )
        .unwrap()
    };
    let index = open("index");
    let schema = TableSchema {
        table_id: TableId(1),
        version: 0,
        columns: vec![Column {
            field_id: 1,
            name: "id".into(),
            data_type: ColumnType::Int64,
            nullable: false,
        }],
        primary_key: vec![0],
        append_only: false,
    };
    let catalog = Arc::new(
        MemoryCatalogBuilder::default()
            .load(
                "maintenance",
                HashMap::from([(MEMORY_CATALOG_WAREHOUSE.into(), "memory://warehouse".into())]),
            )
            .await
            .unwrap(),
    );
    let namespace = NamespaceIdent::new("test".into());
    catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .unwrap();
    let table = catalog
        .create_table(
            &namespace,
            TableCreation::builder()
                .name("data".into())
                .schema(iceberg_schema(&schema).unwrap())
                .format_version(FormatVersion::V2)
                .build(),
        )
        .await
        .unwrap();
    let rows: Vec<Row> = (0..6).map(|id| vec![Value::Int64(id)]).collect();
    let mut initial = None;
    let mut current = table.clone();
    let mut locations = Vec::new();
    for (index, batch) in rows.chunks(2).enumerate() {
        let mut writer = DataWriter::new(
            table.file_io().clone(),
            table.metadata().location(),
            &OperationId(format!("flow-l0-initial-{index}")),
            schema.clone(),
            0,
            WriterConfig::default(),
        )
        .unwrap();
        let mut written = writer.write(batch, PgLsn(10)).await.unwrap().locations;
        let committed = RowDeltaAction::new(&current, format!("initial-{index}"))
            .add_data_files(writer.close().await.unwrap())
            .commit(catalog.as_ref(), &current)
            .await
            .unwrap();
        for location in &mut written {
            location.data_sequence_number = committed.sequence_number;
        }
        locations.extend(written);
        current = committed.table.clone();
        initial = Some(committed);
    }
    let initial = initial.unwrap();
    let operation = OperationId("initial".into());
    index
        .prepare(
            PreparedOperation {
                id: operation.clone(),
                table_id: schema.table_id,
                kind: OperationKind::Ingest,
                base_snapshot_id: None,
                last_lsn: PgLsn(10),
                schema_version: 0,
                artifacts: vec![],
                payload: vec![],
            },
            rows.iter()
                .zip(locations)
                .map(|(row, location)| IndexDelta {
                    key: schema.encode_key(row).unwrap(),
                    expected: None,
                    replacement: Some(location),
                }),
        )
        .unwrap();
    index
        .mark_committed(&operation, initial.snapshot_id, initial.sequence_number)
        .unwrap();
    index.apply_committed(&operation).unwrap();
    let maintain = |store| {
        TableMaintenance::new(
            store,
            catalog.clone(),
            Policy {
                l0_soft_files: 2,
                l0_hard_files: 4,
                min_file_age_ms: 0,
                ..Default::default()
            },
            WriterConfig::default(),
        )
        .unwrap()
    };
    let maintenance = maintain(index.clone());
    assert_eq!(
        maintenance
            .inventory(&initial.table, schema.table_id)
            .await
            .unwrap()
            .manifest_count,
        3
    );
    let recovery_id = OperationId("recover-manifests".into());
    let action = flow_iceberg_ext::RewriteManifestsAction::plan(
        &initial.table,
        &recovery_id.0,
        &flow_iceberg_ext::ManifestRewritePolicy {
            min_manifest_count: 2,
            max_input_manifests: 2,
            ..Default::default()
        },
    )
    .await
    .unwrap()
    .unwrap();
    let artifacts = action.artifacts();
    let payload = serde_json::to_vec(&action).unwrap();
    index
        .begin_prepare(PreparedOperation {
            id: recovery_id.clone(),
            table_id: schema.table_id,
            kind: OperationKind::ManifestRewrite,
            base_snapshot_id: Some(initial.snapshot_id),
            last_lsn: PgLsn(10),
            schema_version: 0,
            artifacts: artifacts.clone(),
            payload: payload.clone(),
        })
        .unwrap();
    for path in &artifacts {
        assert!(!table.file_io().exists(path).await.unwrap());
    }
    action
        .write_artifacts(&initial.table, &Default::default())
        .await
        .unwrap();
    index
        .seal_prepare(&recovery_id, artifacts, payload)
        .unwrap();
    let committed = action
        .commit(catalog.as_ref(), &initial.table)
        .await
        .unwrap();
    let snapshot_count = committed.table.metadata().snapshots().len();
    // Lose the process after catalog success but before recording it locally.
    drop(maintenance);
    drop(index);
    let index = open("index");
    let maintenance = maintain(index.clone());
    assert_eq!(
        maintenance
            .recover(&committed.table, &recovery_id)
            .await
            .unwrap(),
        Some(committed.snapshot_id)
    );
    let recovered = index.operation(&recovery_id).unwrap().unwrap();
    assert_eq!(recovered.delta_count, 0);
    index.forget_applied(&recovery_id).unwrap();
    assert_eq!(
        catalog
            .load_table(table.identifier())
            .await
            .unwrap()
            .metadata()
            .snapshots()
            .len(),
        snapshot_count
    );
    assert_eq!(
        maintenance
            .inventory(&committed.table, schema.table_id)
            .await
            .unwrap()
            .manifest_count,
        2
    );
    let manifest_snapshot = maintenance
        .rewrite_manifests(
            &initial.table,
            schema.table_id,
            &flow_iceberg_ext::ManifestRewritePolicy {
                min_manifest_count: 2,
                ..Default::default()
            },
        )
        .await
        .unwrap()
        .unwrap();
    let merged = catalog.load_table(table.identifier()).await.unwrap();
    assert_eq!(
        maintenance
            .inventory(&merged, schema.table_id)
            .await
            .unwrap()
            .manifest_count,
        1
    );
    assert_eq!(
        index.table_state(&schema.table_id).unwrap().snapshot_id,
        Some(manifest_snapshot)
    );
    assert_eq!(
        index
            .table_state(&schema.table_id)
            .unwrap()
            .materialized_lsn,
        PgLsn(10)
    );
    let snapshot = maintenance
        .compact(&initial.table, &schema, open("compact-scratch"))
        .await
        .unwrap()
        .unwrap();
    let head = catalog.load_table(table.identifier()).await.unwrap();
    assert_eq!(head.metadata().current_snapshot_id(), Some(snapshot));
    assert_eq!(
        index.table_state(&schema.table_id).unwrap().snapshot_id,
        Some(snapshot)
    );
    let rewrite_id = OperationId(
        head.metadata()
            .current_snapshot()
            .unwrap()
            .summary()
            .additional_properties[flow_iceberg_ext::OPERATION_ID_KEY]
            .clone(),
    );
    assert!(index.operation(&rewrite_id).unwrap().is_none());
    assert!(index.prepared_deltas(&rewrite_id).is_err());
    maintenance
        .expire_history(
            &head,
            schema.table_id,
            &flow_coordinator::HistoryPolicy::window(std::time::Duration::from_secs(60)),
            &Default::default(),
        )
        .await
        .unwrap();
    assert_eq!(
        catalog
            .load_table(table.identifier())
            .await
            .unwrap()
            .metadata()
            .current_snapshot_id(),
        Some(snapshot)
    );
    assert_eq!(
        index.table_state(&schema.table_id).unwrap().snapshot_id,
        Some(snapshot)
    );
    let scan = head.scan().build().unwrap();
    let mut batches = scan.to_arrow().await.unwrap();
    let mut actual = Vec::new();
    while let Some(batch) = batches.try_next().await.unwrap() {
        actual.extend(rows_from_batch(&schema, &batch).unwrap());
    }
    actual.sort_by_key(|row| match row[0] {
        Value::Int64(id) => id,
        _ => unreachable!(),
    });
    assert_eq!(actual, rows);

    let rebuilt = maintenance
        .rebuild_index(
            &head,
            &schema,
            open("replacement"),
            open("rebuild-scratch"),
            PgLsn(10),
            schema.version,
        )
        .await
        .unwrap();
    assert_eq!(
        rebuilt.table_state(&schema.table_id).unwrap().snapshot_id,
        Some(snapshot)
    );
    assert_eq!(
        rebuilt
            .table_state(&schema.table_id)
            .unwrap()
            .materialized_lsn,
        PgLsn(10)
    );
    for row in rows {
        let key = schema.encode_key(&row).unwrap();
        let original = index.lookup(&schema.table_id, &key).unwrap().unwrap();
        let replacement = rebuilt.lookup(&schema.table_id, &key).unwrap().unwrap();
        assert_eq!(replacement, original);
    }
    // A later metadata-only snapshot and history expiration must not turn the
    // retained compacted file back into L0 and trigger an idle rewrite loop.
    let metadata_head = metadata_rewrite(catalog.as_ref(), &head).await;
    maintenance
        .reconcile(&metadata_head, &schema, open("metadata-scratch"))
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(3)).await;
    assert!(
        maintenance
            .expire_history(
                &metadata_head,
                schema.table_id,
                &flow_coordinator::HistoryPolicy::window(std::time::Duration::from_millis(1)),
                &Default::default()
            )
            .await
            .unwrap()
            > 0
    );
    let expired = catalog.load_table(table.identifier()).await.unwrap();
    assert!(expired.metadata().snapshot_by_id(snapshot).is_none());
    let inventory = maintenance
        .inventory(&expired, schema.table_id)
        .await
        .unwrap();
    assert_eq!(inventory.files.len(), 1);
    assert_eq!(inventory.files[0].level, flow_compactor::Level::L1);
    assert_eq!(inventory.debt.l0_files, 0);
    assert!(
        maintenance
            .compact(&expired, &schema, open("idle-scratch"))
            .await
            .unwrap()
            .is_none()
    );
    // A source-approved nullable DDL need not create a snapshot. External
    // metadata maintenance may be the first snapshot carrying the new schema.
    // One catalog update adds two fields: its schema ID advances once, while
    // the durable source version advances by the number of added columns.
    use iceberg::transaction::{AddColumn, ApplyTransactionAction, Transaction};
    let transaction = Transaction::new(&expired);
    let evolved = transaction
        .update_schema()
        .add_column(AddColumn::optional(
            "note",
            iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::String),
        ))
        .add_column(AddColumn::optional(
            "score",
            iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Long),
        ))
        .apply(transaction)
        .unwrap()
        .commit(catalog.as_ref())
        .await
        .unwrap();
    let mut evolved_schema = schema.clone();
    evolved_schema.version = 2;
    evolved_schema.columns.push(Column {
        field_id: 2,
        name: "note".into(),
        data_type: ColumnType::String,
        nullable: true,
    });
    evolved_schema.columns.push(Column {
        field_id: 3,
        name: "score".into(),
        data_type: ColumnType::Int64,
        nullable: true,
    });
    assert_ne!(
        evolved.metadata().current_schema_id(),
        evolved_schema.version as i32
    );
    let external = metadata_rewrite(catalog.as_ref(), &evolved).await;
    maintenance
        .reconcile(&external, &evolved_schema, open("schema-scratch"))
        .await
        .unwrap();
    assert_eq!(
        index.table_state(&schema.table_id).unwrap().schema_version,
        2
    );
    assert_eq!(
        index
            .table_state(&schema.table_id)
            .unwrap()
            .materialized_lsn,
        PgLsn(10)
    );
    let mut batches = external.scan().build().unwrap().to_arrow().await.unwrap();
    let mut count = 0;
    while let Some(batch) = batches.try_next().await.unwrap() {
        for row in rows_from_batch(&evolved_schema, &batch).unwrap() {
            assert_eq!(row[1], Value::Null);
            assert_eq!(row[2], Value::Null);
            count += 1;
        }
    }
    assert_eq!(count, 6);
}

async fn metadata_rewrite(
    catalog: &dyn Catalog,
    table: &iceberg::table::Table,
) -> iceberg::table::Table {
    use iceberg::spec::{
        MAIN_BRANCH, ManifestListWriter, Operation, Snapshot, SnapshotReference, SnapshotRetention,
        Summary,
    };
    use iceberg::{TableCommit, TableRequirement, TableUpdate};
    let metadata = table.metadata();
    let snapshot_id = (uuid::Uuid::new_v4().as_u64_pair().0 & i64::MAX as u64) as i64;
    let sequence = metadata.next_sequence_number();
    let path = format!(
        "{}/metadata/{snapshot_id}-external.avro",
        metadata.location()
    );
    let mut writer = ManifestListWriter::v2(
        table
            .file_io()
            .new_output(&path)
            .unwrap()
            .writer()
            .await
            .unwrap(),
        snapshot_id,
        metadata.current_snapshot_id(),
        sequence,
    );
    let bytes = table
        .file_io()
        .new_input(metadata.current_snapshot().unwrap().manifest_list())
        .unwrap()
        .read()
        .await
        .unwrap();
    let manifests =
        iceberg::spec::ManifestList::parse_with_version(&bytes, FormatVersion::V2).unwrap();
    writer
        .add_manifests(manifests.entries().iter().cloned())
        .unwrap();
    writer.close().await.unwrap();
    let snapshot = Snapshot::builder()
        .with_snapshot_id(snapshot_id)
        .with_parent_snapshot_id(metadata.current_snapshot_id())
        .with_sequence_number(sequence)
        .with_timestamp_ms(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64,
        )
        .with_schema_id(metadata.current_schema_id())
        .with_manifest_list(path)
        .with_summary(Summary {
            operation: Operation::Replace,
            additional_properties: HashMap::new(),
        })
        .build();
    catalog
        .update_table(
            TableCommit::builder()
                .ident(table.identifier().clone())
                .requirements(vec![TableRequirement::RefSnapshotIdMatch {
                    r#ref: MAIN_BRANCH.into(),
                    snapshot_id: metadata.current_snapshot_id(),
                }])
                .updates(vec![
                    TableUpdate::AddSnapshot { snapshot },
                    TableUpdate::SetSnapshotRef {
                        ref_name: MAIN_BRANCH.into(),
                        reference: SnapshotReference::new(
                            snapshot_id,
                            SnapshotRetention::branch(None, None, None),
                        ),
                    },
                ])
                .build(),
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn keyless_external_rewrites_preserve_duplicates_and_identity_across_restart() {
    use flow_iceberg_ext::{RewriteFilesAction, SnapshotView};
    use flow_model::PrimaryKey;
    let temp = TempDir::new().unwrap();
    let open = |name: &str| {
        StateStore::open(
            temp.path().join(name),
            StateStoreOptions {
                apply_batch_rows: 2,
                ..Default::default()
            },
        )
        .unwrap()
    };
    let index = open("index");
    let schema = TableSchema {
        table_id: TableId(2),
        version: 0,
        columns: vec![Column {
            field_id: 1,
            name: "value".into(),
            data_type: ColumnType::Int64,
            nullable: false,
        }],
        primary_key: Vec::new(),
        append_only: true,
    };
    let catalog = Arc::new(
        MemoryCatalogBuilder::default()
            .load(
                "keyless",
                HashMap::from([(
                    MEMORY_CATALOG_WAREHOUSE.into(),
                    "memory://keyless-warehouse".into(),
                )]),
            )
            .await
            .unwrap(),
    );
    let namespace = NamespaceIdent::new("test".into());
    catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .unwrap();
    let table = catalog
        .create_table(
            &namespace,
            TableCreation::builder()
                .name("duplicates".into())
                .schema(iceberg_schema(&schema).unwrap())
                .format_version(FormatVersion::V2)
                .build(),
        )
        .await
        .unwrap();
    let rows: Vec<Row> = [1, 1, 2, 2, 2, 3]
        .into_iter()
        .map(|value| vec![Value::Int64(value)])
        .collect();
    let mut writer = DataWriter::new(
        table.file_io().clone(),
        table.metadata().location(),
        &OperationId("flow-l1-initial-keyless".into()),
        schema.clone(),
        0,
        WriterConfig::default(),
    )
    .unwrap();
    let locations = writer.write(&rows, PgLsn(10)).await.unwrap().locations;
    let initial = RowDeltaAction::new(&table, "initial")
        .add_data_files(writer.close().await.unwrap())
        .commit(catalog.as_ref(), &table)
        .await
        .unwrap();
    let operation = OperationId("initial".into());
    index
        .prepare(
            PreparedOperation {
                id: operation.clone(),
                table_id: schema.table_id,
                kind: OperationKind::Ingest,
                base_snapshot_id: None,
                last_lsn: PgLsn(10),
                schema_version: 0,
                artifacts: Vec::new(),
                payload: Vec::new(),
            },
            locations
                .into_iter()
                .enumerate()
                .map(|(ordinal, mut location)| {
                    location.source_commit_lsn = PgLsn(ordinal as u64 + 1);
                    location.row_version = ordinal as u64;
                    IndexDelta {
                        key: PrimaryKey(vec![ordinal as u8]),
                        expected: None,
                        replacement: Some(location),
                    }
                }),
        )
        .unwrap();
    index
        .mark_committed(&operation, initial.snapshot_id, initial.sequence_number)
        .unwrap();
    index.apply_committed(&operation).unwrap();
    let identities: Vec<_> = (0..rows.len())
        .map(|ordinal| {
            let key = PrimaryKey(vec![ordinal as u8]);
            let location = index.lookup(&schema.table_id, &key).unwrap().unwrap();
            (key, location)
        })
        .collect();
    let mut head = initial.table;
    for attempt in 0..2 {
        // Change file boundaries and reverse physical order without changing the
        // row multiset. The duplicate values cross the two-row staging boundary.
        let mut output = Vec::new();
        for (part, values) in [3, 2, 1, 2, 1, 2].chunks(3).enumerate() {
            let mut writer = DataWriter::new(
                head.file_io().clone(),
                head.metadata().location(),
                &OperationId(format!("external-{attempt}-{part}")),
                schema.clone(),
                0,
                WriterConfig::default(),
            )
            .unwrap();
            writer
                .write(
                    &values
                        .iter()
                        .map(|value| vec![Value::Int64(*value)])
                        .collect::<Vec<_>>(),
                    PgLsn(0),
                )
                .await
                .unwrap();
            output.extend(writer.close().await.unwrap());
        }
        let view = SnapshotView::current(&head).await.unwrap();
        head = RewriteFilesAction::new(&head, format!("external-{attempt}"))
            .with_operation_id_key("external.compaction-id")
            .unwrap()
            .remove_data_files(view.live_files.keys().cloned())
            .add_data_files(output)
            .commit(catalog.as_ref(), &head)
            .await
            .unwrap()
            .table;
        let maintenance = TableMaintenance::new(
            index.clone(),
            catalog.clone(),
            Policy::default(),
            WriterConfig::default(),
        )
        .unwrap();
        maintenance
            .reconcile(&head, &schema, open(&format!("scratch-{attempt}")))
            .await
            .unwrap();
        for (key, original) in &identities {
            let current = index.lookup(&schema.table_id, key).unwrap().unwrap();
            assert_eq!(current.row_fingerprint, original.row_fingerprint);
            assert_eq!(current.source_commit_lsn, original.source_commit_lsn);
            assert_eq!(current.row_version, original.row_version);
            assert_ne!(current.data_file_id, original.data_file_id);
        }
    }
    let maintenance = TableMaintenance::new(
        index.clone(),
        catalog.clone(),
        Policy {
            stable_small_soft_files: 2,
            stable_small_hard_files: 4,
            min_file_age_ms: 0,
            ..Default::default()
        },
        WriterConfig::default(),
    )
    .unwrap();
    assert!(
        maintenance
            .compact(&head, &schema, open("native-scratch"))
            .await
            .unwrap()
            .is_some()
    );
    head = catalog.load_table(table.identifier()).await.unwrap();
    drop(maintenance);
    drop(index);
    let index = open("index");
    assert_eq!(
        index.table_state(&schema.table_id).unwrap().snapshot_id,
        head.metadata().current_snapshot_id()
    );
    for (key, original) in &identities {
        let current = index.lookup(&schema.table_id, key).unwrap().unwrap();
        assert_eq!(current.row_fingerprint, original.row_fingerprint);
        assert_eq!(current.source_commit_lsn, original.source_commit_lsn);
        assert_eq!(current.row_version, original.row_version);
    }
    let mut actual = Vec::new();
    let mut batches = head.scan().build().unwrap().to_arrow().await.unwrap();
    while let Some(batch) = batches.try_next().await.unwrap() {
        actual.extend(rows_from_batch(&schema, &batch).unwrap());
    }
    actual.sort_by_key(|row| match row[0] {
        Value::Int64(value) => value,
        _ => unreachable!(),
    });
    assert_eq!(actual, rows);

    // Equal total row counts do not suffice: changing duplicate multiplicities
    // is a logical mutation, even when the external writer labels it Replace.
    let mut writer = DataWriter::new(
        head.file_io().clone(),
        head.metadata().location(),
        &OperationId("external-changed-multiset".into()),
        schema.clone(),
        0,
        WriterConfig::default(),
    )
    .unwrap();
    writer
        .write(
            &[1, 1, 1, 2, 2, 3].map(|value| vec![Value::Int64(value)]),
            PgLsn(0),
        )
        .await
        .unwrap();
    let view = SnapshotView::current(&head).await.unwrap();
    let changed = RewriteFilesAction::new(&head, "external-changed")
        .with_operation_id_key("external.compaction-id")
        .unwrap()
        .remove_data_files(view.live_files.keys().cloned())
        .add_data_files(writer.close().await.unwrap())
        .commit(catalog.as_ref(), &head)
        .await
        .unwrap()
        .table;
    let maintenance = TableMaintenance::new(
        index.clone(),
        catalog,
        Policy::default(),
        WriterConfig::default(),
    )
    .unwrap();
    assert!(
        maintenance
            .reconcile(&changed, &schema, open("changed-scratch"))
            .await
            .is_err()
    );
    let state = index.table_state(&schema.table_id).unwrap();
    assert_eq!(state.snapshot_id, head.metadata().current_snapshot_id());
    assert!(state.pending_operation.is_some());
    for (key, original) in identities {
        assert_eq!(
            index
                .lookup(&schema.table_id, &key)
                .unwrap()
                .unwrap()
                .row_fingerprint,
            original.row_fingerprint
        );
    }
}
