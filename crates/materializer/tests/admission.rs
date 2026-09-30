use std::{
    fs::File,
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use arrow_array::{Int64Array, StringArray};
use flow_iceberg_ext::{ArtifactSet, ArtifactTracker, position_delete_may_apply};
use flow_materializer::{DataWriter, DeleteWriter, WriterConfig, rows_from_batch};
use flow_model::{
    Column, ColumnType, FileId, OperationId, PgLsn, Row, RowLocation, TableId, TableSchema, Value,
};
use iceberg::{
    io::FileIO,
    metadata_columns::{RESERVED_FIELD_ID_DELETE_FILE_PATH, RESERVED_FIELD_ID_DELETE_FILE_POS},
    spec::{
        DataContentType, DataFileBuilder, DataFileFormat, Datum, ManifestEntry, ManifestStatus,
    },
};
use parquet::{
    arrow::arrow_reader::ParquetRecordBatchReaderBuilder, basic::Encoding,
    file::metadata::ParquetMetaData,
};

fn schema() -> TableSchema {
    TableSchema {
        table_id: TableId(1),
        version: 0,
        columns: [ColumnType::Int64, ColumnType::String, ColumnType::Binary]
            .into_iter()
            .enumerate()
            .map(|(index, data_type)| Column {
                field_id: index as i32 + 1,
                name: format!("field_{index}"),
                data_type,
                nullable: index != 0,
            })
            .collect(),
        primary_key: vec![0],
        append_only: false,
    }
}

fn io() -> FileIO {
    FileIO::new_with_fs()
}

fn check_groups(metadata: &ParquetMetaData, overhead: usize, config: &WriterConfig) {
    for group in metadata.row_groups() {
        let variable: i64 = group
            .columns()
            .iter()
            .filter_map(|column| column.unencoded_byte_array_data_bytes())
            .sum();
        assert!(group.num_rows() > 0);
        assert!(group.num_rows() as usize <= config.row_group_rows);
        assert!(variable as usize + overhead * group.num_rows() as usize <= config.row_group_bytes);
    }
}

#[tokio::test]
async fn wide_dictionary_rows_obey_logical_group_limits_and_keep_file_positions()
-> anyhow::Result<()> {
    let schema = schema();
    let rows: Vec<Row> = (0..29)
        .map(|id| {
            if id < 2 {
                vec![Value::Int64(id), Value::Null, Value::Null]
            } else {
                vec![
                    Value::Int64(id),
                    Value::String("x".repeat(1500)),
                    Value::Binary(vec![42; 256]),
                ]
            }
        })
        .collect();
    // One run accumulates multiple batches per row group; the other rotates
    // files between admitted batches and checks the returned physical mapping.
    for target_file_bytes in [usize::MAX, 1] {
        let directory = tempfile::tempdir()?;
        let config = WriterConfig {
            target_file_bytes,
            batch_bytes: 4096,
            row_group_bytes: 8192,
            row_group_rows: 7,
            ..WriterConfig::default()
        };
        let mut writer = DataWriter::new(
            io(),
            directory.path().to_str().unwrap(),
            &OperationId("wide".into()),
            schema.clone(),
            0,
            config.clone(),
        )?;
        let mut locations = writer.write(&rows[..2], PgLsn(10)).await?.locations;
        let borrowed = rows[2..].iter().collect::<Vec<_>>();
        locations.extend(writer.write(&borrowed, PgLsn(10)).await?.locations);
        let files = writer.close().await?;
        assert_eq!(files.len() > 1, target_file_bytes == 1);
        let mut recovered = Vec::new();
        let mut saw_dictionary = false;
        let mut saw_multiple_batches = false;
        for file in files {
            let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(file.file_path())?)?;
            check_groups(builder.metadata(), 27, &config);
            for group in builder.metadata().row_groups() {
                saw_dictionary |= group
                    .column(1)
                    .encodings()
                    .any(|encoding| encoding == Encoding::RLE_DICTIONARY);
                let variable: i64 = group
                    .columns()
                    .iter()
                    .filter_map(|column| column.unencoded_byte_array_data_bytes())
                    .sum();
                saw_multiple_batches |=
                    variable as usize + 27 * group.num_rows() as usize > config.batch_bytes;
            }
            let mut position = 0;
            for batch in builder.with_batch_size(3).build()? {
                for row in rows_from_batch(&schema, &batch?)? {
                    let location = &locations[recovered.len()];
                    assert_eq!(location.data_file_id.0, file.file_path());
                    assert_eq!(location.row_position, position);
                    assert_eq!(location.row_fingerprint, schema.fingerprint(&row)?);
                    assert_eq!(location.source_commit_lsn, PgLsn(10));
                    recovered.push(row);
                    position += 1;
                }
            }
        }
        assert!(
            saw_dictionary,
            "test must exercise compressed repeated values"
        );
        if target_file_bytes == usize::MAX {
            assert!(saw_multiple_batches);
        }
        assert_eq!(recovered, rows);
    }
    Ok(())
}

fn location(path: String, position: u64) -> RowLocation {
    RowLocation {
        data_file_id: FileId(path),
        row_position: position,
        data_sequence_number: 1,
        spec_id: 0,
        partition: vec![],
        source_commit_lsn: PgLsn(10),
        row_version: 0,
        row_fingerprint: [0; 16],
    }
}

#[tokio::test]
async fn long_delete_paths_are_admitted_and_sorted_across_batches_and_files() -> anyhow::Result<()>
{
    let directory = tempfile::tempdir()?;
    let config = WriterConfig {
        target_file_bytes: 1024,
        batch_bytes: 4096,
        row_group_bytes: 6000,
        row_group_rows: 7,
        ..WriterConfig::default()
    };
    let positions: Vec<_> = (0..31)
        .map(|position| location(format!("s3://bucket/{}", "x".repeat(1024)), position))
        .collect();
    let mut writer = DeleteWriter::new(
        io(),
        directory.path().to_str().unwrap(),
        &OperationId("deletes".into()),
        0,
        config.clone(),
    )?;
    writer.write(&positions[..2]).await?;
    writer.write(&positions[2..]).await?;
    let files = writer.close().await?;
    assert!(files.len() > 1);
    let mut recovered = Vec::new();
    for file in files {
        let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(file.file_path())?)?;
        check_groups(builder.metadata(), 18, &config);
        for batch in builder.with_batch_size(2).build()? {
            let batch = batch?;
            let paths = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let positions = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for row in 0..batch.num_rows() {
                recovered.push((paths.value(row).to_owned(), positions.value(row) as u64));
            }
        }
    }
    assert_eq!(
        recovered,
        positions
            .iter()
            .map(|position| (position.data_file_id.0.clone(), position.row_position))
            .collect::<Vec<_>>()
    );
    Ok(())
}

#[tokio::test]
async fn explicit_delete_rotation_preserves_order_and_exact_path_bounds() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    // Long common prefixes must not turn disjoint ranges into global dependencies.
    let prefix = format!("s3://bucket/{}/", "x".repeat(1024));
    let paths: Vec<_> = ["a", "b", "c", "d"]
        .map(|suffix| format!("{prefix}{suffix}.parquet"))
        .into();
    let positions = [
        location(paths[0].clone(), 1),
        location(paths[0].clone(), 7),
        location(paths[1].clone(), 2),
        location(paths[1].clone(), 9),
        location(paths[2].clone(), 1),
        location(paths[3].clone(), 5),
    ];
    let mut writer = DeleteWriter::new(
        io(),
        directory.path().to_str().unwrap(),
        &OperationId("ranges".into()),
        0,
        WriterConfig {
            target_file_bytes: usize::MAX,
            ..WriterConfig::default()
        },
    )?;
    writer.finish_file().await?;
    writer.write(&positions[..2]).await?;
    writer.write(&positions[2..3]).await?;
    writer.finish_file().await?;
    for invalid in [&positions[..1], &positions[2..3]] {
        assert!(
            writer
                .write(invalid)
                .await
                .unwrap_err()
                .to_string()
                .contains("sorted and unique")
        );
    }
    writer.write(&positions[3..5]).await?;
    writer.finish_file().await?;
    writer.finish_file().await?;
    writer.write(&positions[5..]).await?;
    let files = writer.close().await?;
    assert_eq!(files.len(), 3);
    let mut recovered = Vec::new();
    for (index, (file, (first, last))) in
        files.into_iter().zip([(0, 1), (1, 2), (3, 3)]).enumerate()
    {
        assert!(
            file.file_path()
                .ends_with(&format!("ranges-delete-{index:06}.parquet"))
        );
        assert_eq!(
            file.lower_bounds()[&RESERVED_FIELD_ID_DELETE_FILE_PATH],
            Datum::string(&paths[first])
        );
        assert_eq!(
            file.upper_bounds()[&RESERVED_FIELD_ID_DELETE_FILE_PATH],
            Datum::string(&paths[last])
        );
        assert!(
            file.lower_bounds()
                .contains_key(&RESERVED_FIELD_ID_DELETE_FILE_POS)
        );
        assert!(
            file.upper_bounds()
                .contains_key(&RESERVED_FIELD_ID_DELETE_FILE_POS)
        );
        let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(file.file_path())?)?
            .with_batch_size(1)
            .build()?;
        for batch in reader {
            let batch = batch?;
            let paths = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let positions = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            recovered.push((paths.value(0).to_owned(), positions.value(0) as u64));
        }
        let delete = ManifestEntry::builder()
            .status(ManifestStatus::Added)
            .sequence_number(2)
            .data_file(file)
            .build();
        for (target, path) in paths.iter().enumerate() {
            let data = ManifestEntry::builder()
                .status(ManifestStatus::Added)
                .sequence_number(1)
                .data_file(
                    DataFileBuilder::default()
                        .content(DataContentType::Data)
                        .file_path(path.clone())
                        .file_format(DataFileFormat::Parquet)
                        .file_size_in_bytes(1)
                        .record_count(10)
                        .build()?,
                )
                .build();
            assert_eq!(
                position_delete_may_apply(&delete, &data),
                (first..=last).contains(&target)
            );
        }
    }
    assert_eq!(
        recovered,
        positions
            .iter()
            .map(|p| (p.data_file_id.0.clone(), p.row_position))
            .collect::<Vec<_>>()
    );
    Ok(())
}

#[derive(Debug, Default)]
struct RegistrationCount(AtomicUsize);

impl ArtifactTracker for RegistrationCount {
    fn register(
        &self,
        _: ArtifactSet,
    ) -> Pin<Box<dyn Future<Output = iceberg::Result<()>> + Send + '_>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Ok(()) })
    }
}

#[tokio::test]
async fn oversized_late_rows_fail_before_any_output_and_leave_writers_reusable()
-> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let tracker = Arc::new(RegistrationCount::default());
    let config = WriterConfig {
        batch_bytes: 1024,
        row_group_bytes: 2048,
        artifact_tracker: Some(tracker.clone()),
        ..WriterConfig::default()
    };
    let mut data = DataWriter::new(
        io(),
        directory.path().to_str().unwrap(),
        &OperationId("data".into()),
        schema(),
        0,
        config.clone(),
    )?;
    let good = vec![Value::Int64(1), Value::Null, Value::Null];
    let bad = vec![
        Value::Int64(2),
        Value::String("x".repeat(2048)),
        Value::Null,
    ];
    assert!(
        data.write(&[good.clone(), bad], PgLsn(10))
            .await
            .unwrap_err()
            .to_string()
            .contains("admission limit")
    );
    let mut deletes = DeleteWriter::new(
        io(),
        directory.path().to_str().unwrap(),
        &OperationId("deletes".into()),
        0,
        config,
    )?;
    let good_delete = location("s3://bucket/a.parquet".into(), 0);
    let bad_delete = location(format!("s3://bucket/z{}", "x".repeat(2048)), 0);
    assert!(
        deletes
            .write(&[good_delete.clone(), bad_delete])
            .await
            .unwrap_err()
            .to_string()
            .contains("admission limit")
    );
    assert_eq!(
        tracker.0.load(Ordering::SeqCst),
        0,
        "registration precedes the first PUT"
    );
    assert_eq!(std::fs::read_dir(directory.path())?.count(), 0);
    let written = data.write(&[good], PgLsn(10)).await?;
    assert_eq!(written.locations[0].row_position, 0);
    assert!(
        written.locations[0]
            .data_file_id
            .0
            .ends_with("data-data-000000.parquet")
    );
    assert_eq!(data.close().await?.len(), 1);
    deletes.write(&[good_delete]).await?;
    assert_eq!(deletes.close().await?.len(), 1);
    Ok(())
}

/// A v3 rewrite preserves row lineage for every column type, including binary
/// values, whose rows are built as `Binary` while Iceberg's Arrow schema
/// conversion yields `LargeBinary`.
#[tokio::test]
async fn lineage_rewrite_accepts_binary_columns() -> anyhow::Result<()> {
    let schema = schema();
    let directory = tempfile::tempdir()?;
    let mut writer = DataWriter::new(
        io(),
        directory.path().to_str().unwrap(),
        &OperationId("lineage-binary".into()),
        schema.clone(),
        0,
        WriterConfig::default(),
    )?
    .with_row_lineage()?;
    let rows: Vec<Row> = (0..3)
        .map(|id| {
            vec![
                Value::Int64(id),
                Value::String(format!("row-{id}")),
                if id == 1 {
                    Value::Null
                } else {
                    Value::Binary(vec![id as u8, 0, 255])
                },
            ]
        })
        .collect();
    let lineage: Vec<_> = (0..3)
        .map(|id| flow_materializer::RowLineage {
            row_id: Some(100 + id),
            last_updated_sequence_number: Some(7),
        })
        .collect();
    writer
        .write_with_lineage(&rows, Some(&lineage), PgLsn(10))
        .await?;
    let files = writer.close().await?;
    let mut recovered = Vec::new();
    for file in files {
        for batch in
            ParquetRecordBatchReaderBuilder::try_new(File::open(file.file_path())?)?.build()?
        {
            let batch = batch?;
            recovered.extend(rows_from_batch(&schema, &batch.project(&[0, 1, 2])?)?);
            let row_ids = batch
                .column(3)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            assert_eq!(row_ids.values(), &[100, 101, 102]);
        }
    }
    assert_eq!(recovered, rows);
    Ok(())
}
