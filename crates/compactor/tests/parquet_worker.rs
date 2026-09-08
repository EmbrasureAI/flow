use flow_compactor::{CompactionPlan, Level, compact};
use flow_iceberg_ext::{RewriteFilesAction, RowDeltaAction, SnapshotView};
use flow_materializer::{DataWriter, DeleteWriter, WriterConfig, iceberg_schema, rows_from_batch};
use flow_model::{
    Column, ColumnType, FileId, OperationId, PgLsn, Row, TableId, TableSchema, Value,
};
use flow_state_store::{
    IndexDelta, OperationKind, PreparedOperation, StateStore, StateStoreOptions,
};
use futures::TryStreamExt;
use iceberg::{
    Catalog, CatalogBuilder, NamespaceIdent, TableCreation,
    memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder},
    spec::FormatVersion,
};
use std::collections::{BTreeSet, HashMap};
use tempfile::TempDir;

fn operation(id: &str, kind: OperationKind, base: Option<i64>, lsn: u64) -> PreparedOperation {
    PreparedOperation {
        id: OperationId(id.into()),
        table_id: TableId(1),
        kind,
        base_snapshot_id: base,
        last_lsn: PgLsn(lsn),
        schema_version: 0,
        artifacts: vec![],
        payload: vec![],
    }
}

#[tokio::test]
async fn parquet_compaction_applies_position_deletes_and_stock_reader_sees_identical_rows() {
    let temp = TempDir::new().unwrap();
    let index = StateStore::open(temp.path().join("index"), StateStoreOptions::default()).unwrap();
    let schema = TableSchema {
        table_id: TableId(1),
        version: 0,
        columns: vec![
            Column {
                field_id: 1,
                name: "id".into(),
                data_type: ColumnType::Int64,
                nullable: false,
            },
            Column {
                field_id: 2,
                name: "value".into(),
                data_type: ColumnType::String,
                nullable: true,
            },
        ],
        primary_key: vec![0],
        append_only: false,
    };
    let catalog = MemoryCatalogBuilder::default()
        .load(
            "worker-test",
            HashMap::from([(MEMORY_CATALOG_WAREHOUSE.into(), "memory://warehouse".into())]),
        )
        .await
        .unwrap();
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
    let rows: Vec<Row> = (0..9)
        .map(|id| vec![Value::Int64(id), Value::String(format!("row-{id}"))])
        .collect();
    let mut writer = DataWriter::new(
        table.file_io().clone(),
        table.metadata().location(),
        &OperationId("initial".into()),
        schema.clone(),
        0,
        WriterConfig {
            row_group_rows: 3,
            ..Default::default()
        },
    )
    .unwrap();
    let original = writer.write(&rows, PgLsn(10)).await.unwrap().locations;
    let files = writer.close().await.unwrap();
    let initial = RowDeltaAction::new(&table, "initial")
        .add_data_files(files.clone())
        .commit(&catalog, &table)
        .await
        .unwrap();
    index
        .prepare(
            operation("initial", OperationKind::Ingest, None, 10),
            rows.iter()
                .zip(&original)
                .map(|(row, location)| IndexDelta {
                    key: schema.encode_key(row).unwrap(),
                    expected: None,
                    replacement: Some(location.clone()),
                }),
        )
        .unwrap();
    index
        .mark_committed(
            &OperationId("initial".into()),
            initial.snapshot_id,
            initial.sequence_number,
        )
        .unwrap();
    index
        .apply_committed(&OperationId("initial".into()))
        .unwrap();

    let deleted_keys = [
        schema.encode_key(&rows[1]).unwrap(),
        schema.encode_key(&rows[7]).unwrap(),
    ];
    let deleted = deleted_keys
        .iter()
        .map(|key| index.lookup(&schema.table_id, key).unwrap().unwrap())
        .collect::<Vec<_>>();
    let mut writer = DeleteWriter::new(
        initial.table.file_io().clone(),
        initial.table.metadata().location(),
        &OperationId("delete".into()),
        0,
        WriterConfig::default(),
    )
    .unwrap();
    writer.write(&deleted).await.unwrap();
    let delete_files = writer.close().await.unwrap();
    let deletion = RowDeltaAction::new(&initial.table, "delete")
        .add_delete_files(delete_files.clone())
        .validate_data_files_exist([files[0].file_path().to_owned()])
        .commit(&catalog, &initial.table)
        .await
        .unwrap();
    index
        .prepare(
            operation(
                "delete",
                OperationKind::Ingest,
                Some(initial.snapshot_id),
                20,
            ),
            deleted_keys
                .into_iter()
                .zip(deleted)
                .map(|(key, original)| IndexDelta {
                    key,
                    expected: Some(original),
                    replacement: None,
                }),
        )
        .unwrap();
    index
        .mark_committed(
            &OperationId("delete".into()),
            deletion.snapshot_id,
            deletion.sequence_number,
        )
        .unwrap();
    index
        .apply_committed(&OperationId("delete".into()))
        .unwrap();

    let scan = deletion.table.scan().build().unwrap();
    let mut batches = scan.to_arrow().await.unwrap();
    let mut before = Vec::new();
    while let Some(batch) = batches.try_next().await.unwrap() {
        before.extend(rows_from_batch(&schema, &batch).unwrap());
    }
    assert_eq!(before.len(), 7);
    let scratch = StateStore::open(
        temp.path().join("scratch"),
        StateStoreOptions {
            apply_batch_rows: 2,
            ..Default::default()
        },
    )
    .unwrap();
    let plan = CompactionPlan {
        base_snapshot_id: deletion.snapshot_id,
        schema_id: 0,
        input_files: files
            .iter()
            .map(|file| FileId(file.file_path().into()))
            .collect(),
        delete_files: delete_files
            .iter()
            .map(|file| FileId(file.file_path().into()))
            .collect(),
        spec_id: 0,
        partition: vec![],
        output_level: Level::L1,
        target_file_bytes: 128 << 20,
        input_bytes: files.iter().map(|file| file.file_size_in_bytes()).sum(),
        delete_input_bytes: delete_files
            .iter()
            .map(|file| file.file_size_in_bytes())
            .sum(),
        delete_input_rows: delete_files.iter().map(|file| file.record_count()).sum(),
    };
    let keyless_scratch = StateStore::open(
        temp.path().join("keyless-scratch"),
        StateStoreOptions {
            apply_batch_rows: 2,
            ..Default::default()
        },
    )
    .unwrap();
    let mut keyless_schema = schema.clone();
    keyless_schema.primary_key.clear();
    keyless_schema.append_only = true;
    let keyless = compact(
        &deletion.table,
        keyless_schema,
        plan.clone(),
        OperationId("keyless-compact".into()),
        &index,
        keyless_scratch,
        WriterConfig::default(),
        &flow_compactor::ReadLimits::default(),
    )
    .await
    .unwrap();
    let keyless_mappings = keyless
        .index_deltas()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let output = compact(
        &deletion.table,
        schema.clone(),
        plan,
        OperationId("compact".into()),
        &index,
        scratch,
        WriterConfig::default(),
        &flow_compactor::ReadLimits::default(),
    )
    .await
    .unwrap();
    let mappings = output
        .index_deltas()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(mappings.len(), 7);
    assert_eq!(
        keyless_mappings
            .iter()
            .map(|delta| (&delta.key, &delta.expected))
            .collect::<Vec<_>>(),
        mappings
            .iter()
            .map(|delta| (&delta.key, &delta.expected))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        mappings
            .iter()
            .map(|delta| delta.replacement.as_ref().unwrap().row_position)
            .collect::<BTreeSet<_>>(),
        (0..7).collect()
    );
    assert!(mappings.iter().all(
        |delta| delta.replacement.as_ref().unwrap().data_sequence_number
            == deletion.sequence_number
    ));
    let rewritten = RewriteFilesAction::new(&deletion.table, "compact")
        .remove_data_files(output.plan.input_files.iter().map(|file| file.0.clone()))
        .remove_delete_files(output.plan.delete_files.iter().map(|file| file.0.clone()))
        .add_data_files(output.data_files)
        .commit(&catalog, &deletion.table)
        .await
        .unwrap();
    index
        .prepare(
            operation(
                "compact",
                OperationKind::Rewrite,
                Some(deletion.snapshot_id),
                20,
            ),
            mappings,
        )
        .unwrap();
    index
        .mark_committed(
            &OperationId("compact".into()),
            rewritten.snapshot_id,
            rewritten.sequence_number,
        )
        .unwrap();
    index
        .apply_committed(&OperationId("compact".into()))
        .unwrap();
    let scan = rewritten.table.scan().build().unwrap();
    let mut batches = scan.to_arrow().await.unwrap();
    let mut after = Vec::new();
    while let Some(batch) = batches.try_next().await.unwrap() {
        after.extend(rows_from_batch(&schema, &batch).unwrap());
    }
    assert_eq!(before, after);
    assert_eq!(
        SnapshotView::current(&rewritten.table)
            .await
            .unwrap()
            .live_files
            .len(),
        1
    );
    assert_eq!(
        index
            .file_rows(&schema.table_id, &FileId(files[0].file_path().into()))
            .count(),
        0
    );
}
