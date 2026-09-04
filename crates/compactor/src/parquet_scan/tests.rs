use std::{fs::File, io::Write, path::Path, sync::Mutex};

use flow_materializer::{arrow_schema, rows_from_batch, rows_to_batch};
use flow_model::{Column, ColumnType, Row, TableId, TableSchema, Value};
use parquet::{
    arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder},
    basic::{Compression, Encoding},
    file::{metadata::ParquetMetaDataWriter, properties::WriterProperties, writer::TrackedWrite},
};

use super::*;

fn schema(types: &[ColumnType], ids: &[i32]) -> TableSchema {
    TableSchema {
        table_id: TableId(1),
        version: 0,
        columns: types
            .iter()
            .zip(ids)
            .enumerate()
            .map(|(index, (data_type, id))| Column {
                field_id: *id,
                name: format!("field_{index}"),
                data_type: data_type.clone(),
                nullable: false,
            })
            .collect(),
        primary_key: vec![0],
        append_only: false,
    }
}

fn write_file(path: &Path, schema: &TableSchema, rows: &[Row], group_rows: usize) -> Result<()> {
    let properties = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_max_row_group_row_count(Some(group_rows))
        .build();
    let mut writer =
        ArrowWriter::try_new(File::create(path)?, arrow_schema(schema), Some(properties))?;
    writer.write(&rows_to_batch(schema, rows)?)?;
    writer.close()?;
    Ok(())
}

fn remove_decoded_size_statistics(path: &Path) -> Result<()> {
    let metadata = ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)?
        .metadata()
        .as_ref()
        .clone();
    let groups = metadata
        .row_groups()
        .iter()
        .map(|group| {
            let columns = group
                .columns()
                .iter()
                .map(|column| {
                    column
                        .clone()
                        .into_builder()
                        .set_unencoded_byte_array_data_bytes(None)
                        .build()
                })
                .collect::<ParquetResult<Vec<_>>>()?;
            group
                .clone()
                .into_builder()
                .set_column_metadata(columns)
                .build()
        })
        .collect::<ParquetResult<Vec<_>>>()?;
    let metadata = metadata
        .into_builder()
        .set_row_groups(groups)
        .set_column_index(None)
        .set_offset_index(None)
        .build();
    let original = std::fs::read(path)?;
    let metadata_len =
        u32::from_le_bytes(original[original.len() - 8..original.len() - 4].try_into()?) as usize;
    let mut output = Vec::new();
    let mut tracked = TrackedWrite::new(&mut output);
    tracked.write_all(&original[..original.len() - metadata_len - 8])?;
    ParquetMetaDataWriter::new_with_tracked(tracked, &metadata).finish()?;
    std::fs::write(path, output)?;
    Ok(())
}

#[tokio::test]
async fn dictionary_expansion_and_decimal_slots_are_bounded_with_or_without_size_statistics()
-> Result<()> {
    let directory = tempfile::tempdir()?;
    let schema = schema(
        &[
            ColumnType::Int64,
            ColumnType::String,
            ColumnType::Decimal {
                precision: 8,
                scale: 2,
            },
        ],
        &[1, 2, 3],
    );
    let rows: Vec<Row> = (0..71)
        .map(|id| {
            vec![
                Value::Int64(id),
                Value::String(if id < 3 {
                    String::new()
                } else {
                    "wide".repeat(1024)
                }),
                Value::Decimal {
                    unscaled: id as i128,
                    scale: 2,
                },
            ]
        })
        .collect();
    for omit_statistics in [false, true] {
        let path = directory
            .path()
            .join(format!("dictionary-{omit_statistics}.parquet"));
        write_file(&path, &schema, &rows, 13)?;
        if omit_statistics {
            remove_decoded_size_statistics(&path)?;
        }
        let metadata = ParquetRecordBatchReaderBuilder::try_new(File::open(&path)?)?;
        assert_eq!(
            metadata
                .metadata()
                .row_group(0)
                .column(1)
                .unencoded_byte_array_data_bytes()
                .is_none(),
            omit_statistics
        );
        assert!(
            metadata
                .metadata()
                .row_group(0)
                .column(1)
                .encodings()
                .any(|encoding| encoding == Encoding::RLE_DICTIONARY)
        );
        assert_eq!(
            metadata.metadata().row_group(0).column(2).column_type(),
            PhysicalType::INT32
        );
        let limits = ReadLimits {
            batch_bytes: 18_000,
            ..ReadLimits::default()
        };
        let mut recovered = Vec::new();
        let mut largest_batch = 0;
        read_batches(
            &FileIO::new_with_fs(),
            path.to_str().unwrap(),
            None,
            1024,
            &limits,
            async |batch| {
                let decoded = rows_from_batch(&schema, &batch)?;
                // Int64 + Decimal128 + variable-width offset/validity allowance.
                let bytes: usize = decoded
                    .iter()
                    .map(|row| match &row[1] {
                        Value::String(value) => 9 + 17 + 17 + value.len(),
                        _ => unreachable!(),
                    })
                    .sum();
                assert!(bytes as u64 <= limits.batch_bytes);
                largest_batch = largest_batch.max(batch.num_rows());
                recovered.extend(decoded);
                Ok(())
            },
        )
        .await?;
        assert!(
            largest_batch < 13,
            "dictionary expansion must reduce the requested batch rows"
        );
        assert_eq!(
            recovered, rows,
            "row order must survive each row-group boundary"
        );
    }
    Ok(())
}

#[tokio::test]
async fn reserved_delete_projection_ignores_large_optional_payload() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("delete.parquet");
    let schema = schema(
        &[ColumnType::String, ColumnType::String, ColumnType::Int64],
        &[2_147_483_546, 2_147_483_544, 2_147_483_545],
    );
    let rows: Vec<Row> = (0..19)
        .map(|position| {
            vec![
                Value::String("s3://bucket/data.parquet".into()),
                Value::String("payload".repeat(4096)),
                Value::Int64(position),
            ]
        })
        .collect();
    write_file(&path, &schema, &rows, 5)?;
    let limits = ReadLimits {
        row_group_uncompressed_bytes: 1024,
        batch_bytes: 1024,
        ..ReadLimits::default()
    };
    let mut positions = Vec::new();
    read_batches(
        &FileIO::new_with_fs(),
        path.to_str().unwrap(),
        Some(&[2_147_483_546, 2_147_483_545]),
        1024,
        &limits,
        async |batch| {
            assert_eq!(batch.num_columns(), 2);
            let decoded = rows_from_batch(
                &TableSchema {
                    columns: vec![schema.columns[0].clone(), schema.columns[2].clone()],
                    ..schema.clone()
                },
                &batch,
            )?;
            positions.extend(decoded.into_iter().map(|row| match row[1] {
                Value::Int64(position) => position,
                _ => unreachable!(),
            }));
            Ok(())
        },
    )
    .await?;
    assert_eq!(positions, (0..19).collect::<Vec<_>>());
    let error = read_batches(
        &FileIO::new_with_fs(),
        path.to_str().unwrap(),
        None,
        1024,
        &limits,
        async |_| panic!("unprojected oversized payload must fail before decoding"),
    )
    .await
    .unwrap_err();
    assert!(format!("{error:#}").contains("uncompressed size"));
    Ok(())
}

#[tokio::test]
async fn oversized_footer_rowgroup_and_single_row_are_rejected_before_delivery() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("large.parquet");
    let schema = schema(&[ColumnType::String], &[1]);
    write_file(&path, &schema, &[vec![Value::String("x".repeat(4096))]], 1)?;
    let cases = [
        (
            ReadLimits {
                footer_bytes: 8,
                ..ReadLimits::default()
            },
            "footer",
        ),
        (
            ReadLimits {
                row_group_fetch_bytes: 1,
                ..ReadLimits::default()
            },
            "fetch span",
        ),
        (
            ReadLimits {
                row_group_uncompressed_bytes: 1,
                ..ReadLimits::default()
            },
            "uncompressed size",
        ),
        (
            ReadLimits {
                batch_bytes: 1024,
                ..ReadLimits::default()
            },
            "cannot admit one row",
        ),
    ];
    for (limits, expected) in cases {
        let error = read_batches(
            &FileIO::new_with_fs(),
            path.to_str().unwrap(),
            None,
            128,
            &limits,
            async |_| panic!("oversized input must fail before the visitor"),
        )
        .await
        .unwrap_err();
        assert!(format!("{error:#}").contains(expected), "{error:#}");
    }
    Ok(())
}

struct CountReads {
    data: Bytes,
    ranges: Arc<Mutex<Vec<Range<u64>>>>,
}

impl AsyncFileReader for CountReads {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, ParquetResult<Bytes>> {
        Box::pin(async move {
            self.ranges.lock().unwrap().push(range.clone());
            Ok(self.data.slice(range.start as usize..range.end as usize))
        })
    }

    fn get_metadata<'a>(
        &'a mut self,
        _: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, ParquetResult<Arc<ParquetMetaData>>> {
        Box::pin(async { panic!("guard must load metadata through its own range checks") })
    }
}

#[tokio::test]
async fn guarded_io_rejects_footer_and_coalescing_span_before_fetching() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("guarded.parquet");
    write_file(
        &path,
        &schema(&[ColumnType::Int64], &[1]),
        &[vec![Value::Int64(1)]],
        1,
    )?;
    let data = Bytes::from(std::fs::read(path)?);
    let size = data.len() as u64;
    let ranges = Arc::new(Mutex::new(Vec::new()));
    let mut reader = BoundedReader::new(
        CountReads {
            data,
            ranges: ranges.clone(),
        },
        size,
        &ReadLimits {
            footer_bytes: 8,
            row_group_fetch_bytes: 16,
            ..ReadLimits::default()
        },
    );
    assert!(reader.get_metadata(None).await.is_err());
    assert_eq!(ranges.lock().unwrap().len(), 1);
    assert_eq!(ranges.lock().unwrap()[0], size - 8..size);
    // Both individual requests fit; retaining their coalesced gap does not.
    assert!(reader.get_byte_ranges(vec![0..4, 64..68]).await.is_err());
    assert!(reader.get_bytes(0..17).await.is_err());
    assert!(reader.get_bytes(size..size + 1).await.is_err());
    assert_eq!(
        ranges.lock().unwrap().len(),
        1,
        "no rejected request may reach storage"
    );
    Ok(())
}
