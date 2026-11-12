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
        lazy_index_remap: false,
        per_target_deletes: false,
        target_ranges: Default::default(),
        reason: flow_compactor::PlanReason::L0Debt,
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
        &flow_iceberg_ext::ManifestCache::default(),
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
        &flow_iceberg_ext::ManifestCache::default(),
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
    assert!(mappings.iter().all(|delta| {
        let replacement = delta.replacement.as_ref().unwrap();
        replacement.data_sequence_number == deletion.sequence_number
            && replacement.row_fingerprint == delta.expected.as_ref().unwrap().row_fingerprint
    }));
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

/// Commit one file of `ids` and index it, returning the refreshed table.
async fn commit_rows(
    catalog: &impl Catalog,
    table: &iceberg::table::Table,
    index: &StateStore,
    schema: &TableSchema,
    operation_id: &str,
    ids: std::ops::Range<i64>,
    lsn: u64,
) -> (iceberg::table::Table, FileId) {
    let rows: Vec<Row> = ids
        .map(|id| vec![Value::Int64(id), Value::String(format!("row-{id}"))])
        .collect();
    let mut writer = DataWriter::new(
        table.file_io().clone(),
        table.metadata().location(),
        &OperationId(operation_id.into()),
        schema.clone(),
        0,
        WriterConfig::default(),
    )
    .unwrap();
    let locations = writer.write(&rows, PgLsn(lsn)).await.unwrap().locations;
    let files = writer.close().await.unwrap();
    let file = FileId(files[0].file_path().into());
    let committed = RowDeltaAction::new(table, operation_id)
        .add_data_files(files)
        .commit(catalog, table)
        .await
        .unwrap();
    let base = table.metadata().current_snapshot_id();
    index
        .prepare(
            operation(operation_id, OperationKind::Ingest, base, lsn),
            rows.iter()
                .zip(locations)
                .map(|(row, location)| IndexDelta {
                    key: schema.encode_key(row).unwrap(),
                    expected: None,
                    replacement: Some(location),
                }),
        )
        .unwrap();
    let id = OperationId(operation_id.into());
    index
        .mark_committed(&id, committed.snapshot_id, committed.sequence_number)
        .unwrap();
    index.apply_committed(&id).unwrap();
    (committed.table, file)
}

/// A keyed table with three committed files whose path order is the reverse of
/// their commit order, as with Flow's random artifact names.
struct ReversedInputs {
    _temp: TempDir,
    root: std::path::PathBuf,
    index: StateStore,
    schema: TableSchema,
    catalog: iceberg::memory::MemoryCatalog,
    table: iceberg::table::Table,
    /// Inputs in commit order; ids 0..4, 4..8 and 8..12.
    files: [FileId; 3],
}

async fn reversed_inputs() -> ReversedInputs {
    let temp = TempDir::new().unwrap();
    let root = temp.path().to_owned();
    let index = StateStore::open(root.join("index"), StateStoreOptions::default()).unwrap();
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
            "order-test",
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
    let (table, first) = commit_rows(&catalog, &table, &index, &schema, "zz-first", 0..4, 10).await;
    let (table, second) =
        commit_rows(&catalog, &table, &index, &schema, "mm-second", 4..8, 20).await;
    let (table, third) =
        commit_rows(&catalog, &table, &index, &schema, "aa-third", 8..12, 30).await;
    assert!(third < second && second < first);
    ReversedInputs {
        _temp: temp,
        root,
        index,
        schema,
        catalog,
        table,
        files: [first, second, third],
    }
}

async fn plan_all(
    table: &iceberg::table::Table,
    files: &[FileId],
    lazy_index_remap: bool,
) -> CompactionPlan {
    let view = SnapshotView::current(table).await.unwrap();
    CompactionPlan {
        base_snapshot_id: table.metadata().current_snapshot_id().unwrap(),
        schema_id: 0,
        input_bytes: files
            .iter()
            .map(|file| view.live_files[&file.0].data_file.file_size_in_bytes())
            .sum(),
        input_files: files.iter().cloned().collect(),
        delete_files: BTreeSet::new(),
        spec_id: 0,
        partition: vec![],
        output_level: Level::L1,
        target_file_bytes: 128 << 20,
        delete_input_bytes: 0,
        delete_input_rows: 0,
        lazy_index_remap,
        per_target_deletes: false,
        target_ranges: Default::default(),
        reason: flow_compactor::PlanReason::L0Debt,
    }
}

#[tokio::test]
async fn compaction_keeps_commit_order_and_writes_page_indexes() {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::file::reader::{FileReader, SerializedFileReader};
    use parquet::file::serialized_reader::ReadOptionsBuilder;

    let ReversedInputs {
        root,
        index,
        schema,
        table,
        files,
        _temp: _guard,
        ..
    } = reversed_inputs().await;
    let plan = plan_all(&table, &files, true).await;
    let scratch = StateStore::open(root.join("scratch"), StateStoreOptions::default()).unwrap();
    let output = compact(
        &table,
        schema.clone(),
        plan,
        OperationId("compact".into()),
        &index,
        scratch,
        WriterConfig::default(),
        &flow_compactor::ReadLimits::default(),
        &flow_iceberg_ext::ManifestCache::default(),
    )
    .await
    .unwrap();
    assert_eq!(output.data_files.len(), 1);
    let bytes = table
        .file_io()
        .new_input(output.data_files[0].file_path())
        .unwrap()
        .read()
        .await
        .unwrap();
    let mut ids = Vec::new();
    for batch in ParquetRecordBatchReaderBuilder::try_new(bytes.clone())
        .unwrap()
        .build()
        .unwrap()
    {
        let batch = batch.unwrap();
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap();
        ids.extend(column.values().iter().copied());
    }
    assert_eq!(ids, (0..12).collect::<Vec<_>>(), "rows follow commit order");
    let reader = SerializedFileReader::new_with_options(
        bytes,
        ReadOptionsBuilder::new().with_page_index().build(),
    )
    .unwrap();
    let metadata = reader.metadata();
    assert!(
        metadata
            .column_index()
            .is_some_and(|groups| !groups.is_empty()),
        "page statistics produce a column index"
    );
    assert!(metadata.offset_index().is_some());
}

/// The per-row path (keyless tables, or lazy remap off) joins the worker's
/// mappings, which follow commit order, with late deletes stored in path order.
/// Late deletes on the first and last committed inputs, whose path order is
/// reversed, must both mask their rows at catch-up and at publication.
#[tokio::test]
async fn per_row_catch_up_masks_late_deletes_on_commit_ordered_inputs() {
    let ReversedInputs {
        root,
        index,
        schema,
        catalog,
        table: base,
        files,
        _temp: _guard,
    } = reversed_inputs().await;
    let plan = plan_all(&base, &files, false).await;
    let built = flow_compactor::build_data(
        &base,
        schema.clone(),
        plan,
        OperationId("rewrite".into()),
        &index,
        StateStore::open(root.join("scratch"), StateStoreOptions::default()).unwrap(),
        WriterConfig::default(),
        &flow_compactor::ReadLimits::default(),
        &flow_iceberg_ext::ManifestCache::default(),
        &std::sync::atomic::AtomicBool::new(false),
    )
    .await
    .unwrap();

    // Delete id 1 (first commit, last by path) and id 9 (last commit, first by path).
    let originals: Vec<_> = [1, 9]
        .map(|id| {
            let key = schema.encode_key_values(&[Value::Int64(id)]).unwrap();
            let location = index.lookup(&schema.table_id, &key).unwrap().unwrap();
            (key, location)
        })
        .into();
    let mut locations: Vec<_> = originals
        .iter()
        .map(|(_, location)| location.clone())
        .collect();
    locations
        .sort_by(|a, b| (&a.data_file_id, a.row_position).cmp(&(&b.data_file_id, b.row_position)));
    let mut writer = DeleteWriter::new(
        base.file_io().clone(),
        base.metadata().location(),
        &OperationId("late".into()),
        0,
        WriterConfig::default(),
    )
    .unwrap();
    writer.write(&locations).await.unwrap();
    let late = RowDeltaAction::new(&base, "late")
        .add_delete_files(writer.close().await.unwrap())
        .validate_data_files_exist(
            locations
                .iter()
                .map(|location| location.data_file_id.0.clone()),
        )
        .commit(&catalog, &base)
        .await
        .unwrap();
    index
        .prepare(
            operation(
                "late",
                OperationKind::Ingest,
                base.metadata().current_snapshot_id(),
                40,
            ),
            originals.into_iter().map(|(key, original)| IndexDelta {
                key,
                expected: Some(original),
                replacement: None,
            }),
        )
        .unwrap();
    let late_id = OperationId("late".into());
    index
        .mark_committed(&late_id, late.snapshot_id, late.sequence_number)
        .unwrap();
    index.apply_committed(&late_id).unwrap();

    let counts = files
        .iter()
        .cloned()
        .zip(
            index
                .file_live_row_counts(&schema.table_id, Some(late.snapshot_id), &files)
                .unwrap(),
        )
        .collect();
    let mut staged = Vec::new();
    let output = flow_compactor::catch_up(
        &late.table,
        &schema,
        &flow_iceberg_ext::CommitBase::new(&base),
        built,
        &index,
        &counts,
        &flow_compactor::Policy::default(),
        WriterConfig::default(),
        &flow_compactor::ReadLimits::default(),
        &flow_iceberg_ext::ManifestCache::default(),
        |deltas| {
            staged.extend(deltas);
            Ok(())
        },
    )
    .await
    .unwrap();
    let survivors: BTreeSet<_> = [0, 2, 3, 4, 5, 6, 7, 8, 10, 11]
        .map(|id| schema.encode_key_values(&[Value::Int64(id)]).unwrap())
        .into();
    assert_eq!(
        staged
            .iter()
            .map(|delta| delta.key.clone())
            .collect::<BTreeSet<_>>(),
        survivors,
        "catch-up stages exactly the surviving rows"
    );
    let published = output
        .index_deltas()
        .unwrap()
        .map(|delta| delta.unwrap().key)
        .collect::<BTreeSet<_>>();
    assert_eq!(published, survivors, "publication omits masked rows");
}
