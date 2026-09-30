use anyhow::{Context, Result, ensure};
use arrow_array::{
    Array, ArrayRef, BinaryArray, Int64Array, LargeBinaryArray, RecordBatch, StringArray,
};
use flow_iceberg_ext::{ArtifactSet, ArtifactTracker};
use flow_model::{ColumnType, FileId, OperationId, PgLsn, Row, RowLocation, TableSchema, Value};
use iceberg::{
    arrow::schema_to_arrow_schema,
    io::FileIO,
    metadata_columns::{RESERVED_FIELD_ID_DELETE_FILE_PATH, RESERVED_FIELD_ID_DELETE_FILE_POS},
    spec::{DataContentType, DataFile, Datum, NestedField, PrimitiveType, Schema, Struct, Type},
    writer::{
        CurrentFileStatus,
        file_writer::{FileWriter, FileWriterBuilder, ParquetWriter, ParquetWriterBuilder},
    },
};
use parquet::{
    basic::{Compression, ZstdLevel},
    file::properties::{EnabledStatistics, WriterProperties},
};
use std::{borrow::Borrow, sync::Arc};

use crate::{iceberg_schema, rows_to_batch};

#[derive(Debug, Clone)]
pub struct WriterConfig {
    pub target_file_bytes: usize,
    pub row_group_rows: usize,
    /// Logical input bytes per Arrow batch, including offsets and validity allowance.
    /// This is not a bound on codec buffers or process memory.
    pub batch_bytes: usize,
    /// Logical input bytes retained in a row group before it is flushed.
    pub row_group_bytes: usize,
    pub compression: Compression,
    pub artifact_tracker: Option<Arc<dyn ArtifactTracker>>,
}
impl Default for WriterConfig {
    fn default() -> Self {
        Self {
            target_file_bytes: 16 * 1024 * 1024,
            row_group_rows: 8192,
            batch_bytes: 8 * 1024 * 1024,
            row_group_bytes: 32 * 1024 * 1024,
            compression: Compression::ZSTD(ZstdLevel::default()),
            artifact_tracker: None,
        }
    }
}
impl WriterConfig {
    fn properties(&self) -> Result<WriterProperties> {
        ensure!(
            self.target_file_bytes > 0
                && self.row_group_rows > 0
                && self.batch_bytes > 0
                && self.row_group_bytes > 0,
            "writer limits must be positive"
        );
        ensure!(
            self.batch_limit() <= i32::MAX as usize,
            "writer batch limit exceeds Arrow's variable-width offset range"
        );
        Ok(WriterProperties::builder()
            .set_compression(self.compression)
            .set_max_row_group_row_count(Some(self.row_group_rows))
            .set_statistics_enabled(EnabledStatistics::Chunk)
            .build())
    }

    fn batch_limit(&self) -> usize {
        self.batch_bytes.min(self.row_group_bytes)
    }

    fn admit_row(&self, bytes: usize) -> Result<()> {
        ensure!(
            bytes <= self.batch_limit(),
            "row logical payload {bytes} bytes exceeds writer admission limit {} bytes",
            self.batch_limit()
        );
        Ok(())
    }
}

/// Charge fixed-width slots even for nulls, one validity byte per field, and
/// two i32 offsets per variable-width value. The offset allowance also covers
/// the final offset of a one-row Arrow array. Allocator capacity is excluded.
fn row_bytes(schema: &TableSchema, row: &Row) -> Result<usize> {
    schema
        .columns
        .iter()
        .zip(row)
        .try_fold(0usize, |bytes, (column, value)| {
            let fixed = match column.data_type {
                ColumnType::Bool => 1,
                ColumnType::Int32 | ColumnType::Date => 4,
                ColumnType::Int64
                | ColumnType::Float64
                | ColumnType::TimestampMicros
                | ColumnType::TimestampTzMicros
                | ColumnType::String
                | ColumnType::Binary => 8,
                ColumnType::Uuid | ColumnType::Decimal { .. } => 16,
            };
            let variable = match value {
                Value::String(value) => value.len(),
                Value::Binary(value) => value.len(),
                _ => 0,
            };
            bytes
                .checked_add(fixed + 1)
                .and_then(|bytes| bytes.checked_add(variable))
                .context("row logical payload size overflow")
        })
}

fn delete_bytes(position: &RowLocation) -> Result<usize> {
    // String offsets, Int64 position, and a validity byte for each column.
    position
        .data_file_id
        .0
        .len()
        .checked_add(18)
        .context("delete row logical payload size overflow")
}

fn bounded_batch<T>(
    items: &[T],
    config: &WriterConfig,
    size: impl Fn(&T) -> Result<usize>,
) -> Result<(usize, usize)> {
    let mut count = 0;
    let mut bytes = 0;
    for item in items.iter().take(config.row_group_rows) {
        let next = size(item)?;
        config.admit_row(next)?;
        if next > config.batch_limit() - bytes {
            break;
        }
        count += 1;
        bytes += next;
    }
    Ok((count, bytes))
}

#[derive(Default)]
struct RowGroupBudget {
    rows: usize,
    bytes: usize,
}

impl RowGroupBudget {
    async fn prepare(
        &mut self,
        writer: &mut ParquetWriter,
        rows: usize,
        bytes: usize,
        config: &WriterConfig,
    ) -> Result<()> {
        if rows > config.row_group_rows - self.rows || bytes > config.row_group_bytes - self.bytes {
            writer.flush_row_group().await?;
            *self = Self::default();
        }
        Ok(())
    }

    fn written(&mut self, rows: usize, bytes: usize, config: &WriterConfig) {
        self.rows += rows;
        self.bytes += bytes;
        // Parquet's configured row limit flushes exactly at this boundary.
        if self.rows == config.row_group_rows {
            *self = Self::default();
        }
    }
}

/// One file is rotated only between batches, so positions are known as rows are written.
/// An operation ID is immutable: retries must reuse prepared artifacts, never overwrite them.
pub struct DataWriter {
    file_io: FileIO,
    schema: TableSchema,
    builder: ParquetWriterBuilder,
    config: WriterConfig,
    prefix: String,
    next_file: usize,
    current: Option<ParquetWriter>,
    row_group: RowGroupBudget,
    files: Vec<DataFile>,
    spec_id: i32,
    lineage_schema: Option<arrow_schema::SchemaRef>,
}
#[derive(Debug)]
pub struct WrittenBatch {
    pub locations: Vec<RowLocation>,
}
impl DataWriter {
    pub fn new(
        file_io: FileIO,
        location: &str,
        operation: &OperationId,
        schema: TableSchema,
        spec_id: i32,
        config: WriterConfig,
    ) -> Result<Self> {
        ensure!(spec_id >= 0, "invalid spec ID");
        let builder =
            ParquetWriterBuilder::new(config.properties()?, Arc::new(iceberg_schema(&schema)?));
        ensure!(
            !operation.0.is_empty()
                && operation
                    .0
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_'),
            "invalid operation ID for artifact path"
        );
        Ok(Self {
            file_io,
            schema,
            builder,
            config,
            prefix: format!("{}/data/{}", location.trim_end_matches('/'), operation.0),
            next_file: 0,
            current: None,
            row_group: RowGroupBudget::default(),
            files: Vec::new(),
            spec_id,
            lineage_schema: None,
        })
    }

    /// Preserve reserved Iceberg row lineage while rewriting existing rows.
    pub fn with_row_lineage(mut self) -> Result<Self> {
        ensure!(
            self.current.is_none() && self.files.is_empty(),
            "lineage must be configured before writing"
        );
        let schema = crate::lineage::writer_schema(&self.schema)?;
        self.lineage_schema = Some(Arc::new(schema_to_arrow_schema(&schema)?));
        self.builder = ParquetWriterBuilder::new(self.config.properties()?, Arc::new(schema));
        Ok(self)
    }

    /// Accept owned or borrowed rows without copying their payloads.
    pub async fn write(
        &mut self,
        rows: &[impl Borrow<Row>],
        source_lsn: PgLsn,
    ) -> Result<WrittenBatch> {
        self.write_with_lineage(rows, None, source_lsn).await
    }

    pub async fn write_with_lineage(
        &mut self,
        rows: &[impl Borrow<Row>],
        lineage: Option<&[crate::RowLineage]>,
        source_lsn: PgLsn,
    ) -> Result<WrittenBatch> {
        ensure!(
            lineage.is_none_or(|values| values.len() == rows.len()),
            "row lineage length mismatch"
        );
        ensure!(
            lineage.is_none() || self.lineage_schema.is_some(),
            "writer is not configured for row lineage"
        );
        if rows.is_empty() {
            return Ok(WrittenBatch { locations: vec![] });
        }
        // Validate the entire call before opening any output. A late oversized
        // row must not leave a partially written artifact behind.
        for row in rows {
            let row = row.borrow();
            self.schema.validate_row(row)?;
            self.config.admit_row(
                row_bytes(&self.schema, row)? + usize::from(self.lineage_schema.is_some()) * 18,
            )?;
        }
        let mut locations = Vec::with_capacity(rows.len());
        let mut remaining = rows;
        while !remaining.is_empty() {
            let (count, bytes) = bounded_batch(remaining, &self.config, |row| {
                Ok(row_bytes(&self.schema, row.borrow())?
                    + usize::from(self.lineage_schema.is_some()) * 18)
            })?;
            let rows = &remaining[..count];
            let offset = locations.len();
            self.write_batch(
                rows,
                lineage.map(|values| &values[offset..offset + count]),
                bytes,
                source_lsn,
                &mut locations,
            )
            .await?;
            remaining = &remaining[count..];
        }
        Ok(WrittenBatch { locations })
    }

    async fn write_batch(
        &mut self,
        rows: &[impl Borrow<Row>],
        lineage: Option<&[crate::RowLineage]>,
        bytes: usize,
        source_lsn: PgLsn,
        locations: &mut Vec<RowLocation>,
    ) -> Result<()> {
        if self
            .current
            .as_ref()
            .is_some_and(|w| w.current_written_size() >= self.config.target_file_bytes)
        {
            self.finish_file().await?;
        }
        if self.current.is_none() {
            let path = format!("{}-data-{:06}.parquet", self.prefix, self.next_file);
            self.next_file += 1;
            if let Some(tracker) = &self.config.artifact_tracker {
                tracker
                    .register(ArtifactSet {
                        paths: vec![path.clone()],
                        ranges: Vec::new(),
                    })
                    .await?;
            }
            ensure!(
                !self.file_io.exists(&path).await?,
                "refusing to overwrite prepared artifact {path}"
            );
            self.current = Some(self.builder.build(self.file_io.new_output(path)?).await?);
        }
        let writer = self.current.as_mut().expect("writer opened above");
        self.row_group
            .prepare(writer, rows.len(), bytes, &self.config)
            .await?;
        let mut batch = rows_to_batch(&self.schema, rows)?;
        if let Some(schema) = &self.lineage_schema {
            // Iceberg's Arrow conversion maps binary to LargeBinary; rows are
            // built as Binary. Both encode the same Parquet BYTE_ARRAY values.
            let mut columns = batch
                .columns()
                .iter()
                .zip(schema.fields())
                .map(
                    |(column, field)| match column.as_any().downcast_ref::<BinaryArray>() {
                        Some(binary)
                            if field.data_type() == &arrow_schema::DataType::LargeBinary =>
                        {
                            Arc::new(binary.iter().collect::<LargeBinaryArray>()) as ArrayRef
                        }
                        _ => column.clone(),
                    },
                )
                .collect::<Vec<_>>();
            for row_id in [true, false] {
                let values = (0..rows.len()).map(|index| {
                    lineage.and_then(|values| {
                        if row_id {
                            values[index].row_id
                        } else {
                            values[index].last_updated_sequence_number
                        }
                    })
                });
                columns.push(Arc::new(values.collect::<Int64Array>()));
            }
            batch = RecordBatch::try_new(schema.clone(), columns)?;
        }
        let path = writer.current_file_path();
        let offset = writer.current_row_num() as u64;
        writer.write(&batch).await?;
        self.row_group.written(rows.len(), bytes, &self.config);
        for (i, row) in rows.iter().enumerate() {
            locations.push(RowLocation {
                data_file_id: FileId(path.clone()),
                row_position: offset + i as u64,
                data_sequence_number: -1,
                spec_id: self.spec_id,
                partition: vec![],
                source_commit_lsn: source_lsn,
                row_version: 0,
                row_fingerprint: self.schema.fingerprint(row.borrow())?,
            });
        }
        Ok(())
    }
    async fn finish_file(&mut self) -> Result<()> {
        if let Some(writer) = self.current.take() {
            for mut file in writer.close().await? {
                file.content(DataContentType::Data)
                    .partition(Struct::empty())
                    .partition_spec_id(self.spec_id);
                self.files.push(file.build()?);
            }
        }
        self.row_group = RowGroupBudget::default();
        Ok(())
    }
    pub async fn close(mut self) -> Result<Vec<DataFile>> {
        self.finish_file().await?;
        Ok(self.files)
    }
}

/// Streaming position-delete writer. Input must be sorted globally by path and position.
/// The caller uses the disk-backed sort in the state store, even for large transactions.
pub(crate) struct ParquetDeleteWriter {
    file_io: FileIO,
    builder: ParquetWriterBuilder,
    arrow_schema: arrow_schema::SchemaRef,
    config: WriterConfig,
    prefix: String,
    next_file: usize,
    current: Option<ParquetWriter>,
    row_group: RowGroupBudget,
    files: Vec<DataFile>,
    last: Option<(String, u64)>,
    file_paths: Option<(String, String)>,
    spec_id: i32,
}
impl ParquetDeleteWriter {
    pub fn new(
        file_io: FileIO,
        location: &str,
        operation: &OperationId,
        spec_id: i32,
        config: WriterConfig,
    ) -> Result<Self> {
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    Arc::new(NestedField::required(
                        RESERVED_FIELD_ID_DELETE_FILE_PATH,
                        "file_path",
                        Type::Primitive(PrimitiveType::String),
                    )),
                    Arc::new(NestedField::required(
                        RESERVED_FIELD_ID_DELETE_FILE_POS,
                        "pos",
                        Type::Primitive(PrimitiveType::Long),
                    )),
                ])
                .build()?,
        );
        let arrow_schema = Arc::new(schema_to_arrow_schema(&schema)?);
        let builder = ParquetWriterBuilder::new(config.properties()?, schema);
        ensure!(
            !operation.0.is_empty()
                && operation
                    .0
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_'),
            "invalid operation ID"
        );
        Ok(Self {
            file_io,
            builder,
            arrow_schema,
            config,
            prefix: format!("{}/data/{}", location.trim_end_matches('/'), operation.0),
            next_file: 0,
            current: None,
            row_group: RowGroupBudget::default(),
            files: vec![],
            last: None,
            file_paths: None,
            spec_id,
        })
    }
    pub async fn write(&mut self, positions: &[RowLocation]) -> Result<()> {
        if positions.is_empty() {
            return Ok(());
        }
        let mut last = self
            .last
            .as_ref()
            .map(|(path, position)| (path.as_str(), *position));
        for p in positions {
            ensure!(
                p.spec_id == self.spec_id && p.partition.is_empty(),
                "position deletes require matching unpartitioned spec"
            );
            ensure!(
                p.row_position <= i64::MAX as u64,
                "position overflows Iceberg long"
            );
            self.config.admit_row(delete_bytes(p)?)?;
            if let Some((path, pos)) = last {
                ensure!(
                    (path, pos) < (p.data_file_id.0.as_str(), p.row_position),
                    "delete positions must be sorted and unique"
                );
            }
            last = Some((p.data_file_id.0.as_str(), p.row_position));
        }
        let mut remaining = positions;
        while !remaining.is_empty() {
            let (count, bytes) = bounded_batch(remaining, &self.config, delete_bytes)?;
            self.write_batch(&remaining[..count], bytes).await?;
            remaining = &remaining[count..];
        }
        let last = positions.last().expect("nonempty positions");
        self.last = Some((last.data_file_id.0.clone(), last.row_position));
        Ok(())
    }

    async fn write_batch(&mut self, positions: &[RowLocation], bytes: usize) -> Result<()> {
        if self
            .current
            .as_ref()
            .is_some_and(|w| w.current_written_size() >= self.config.target_file_bytes)
        {
            self.finish_file().await?;
        }
        if self.current.is_none() {
            let path = format!("{}-delete-{:06}.parquet", self.prefix, self.next_file);
            self.next_file += 1;
            if let Some(tracker) = &self.config.artifact_tracker {
                tracker
                    .register(ArtifactSet {
                        paths: vec![path.clone()],
                        ranges: Vec::new(),
                    })
                    .await?;
            }
            ensure!(
                !self.file_io.exists(&path).await?,
                "refusing to overwrite prepared delete artifact"
            );
            self.current = Some(self.builder.build(self.file_io.new_output(path)?).await?);
        }
        let writer = self.current.as_mut().expect("writer opened above");
        self.row_group
            .prepare(writer, positions.len(), bytes, &self.config)
            .await?;
        let batch = RecordBatch::try_new(
            self.arrow_schema.clone(),
            vec![
                Arc::new(
                    positions
                        .iter()
                        .map(|p| Some(p.data_file_id.0.as_str()))
                        .collect::<StringArray>(),
                ),
                Arc::new(
                    positions
                        .iter()
                        .map(|p| Some(p.row_position as i64))
                        .collect::<Int64Array>(),
                ),
            ],
        )?;
        writer.write(&batch).await?;
        self.row_group.written(positions.len(), bytes, &self.config);
        let first = &positions.first().expect("nonempty batch").data_file_id.0;
        let last = &positions.last().expect("nonempty batch").data_file_id.0;
        if let Some((_, upper)) = &mut self.file_paths {
            upper.clone_from(last);
        } else {
            self.file_paths = Some((first.clone(), last.clone()));
        }
        Ok(())
    }

    /// Close the current file before admitting another sorted range of deletes.
    /// Calling this without an open file is a no-op. Global path/position ordering
    /// and uniqueness remain enforced across rotations; artifact names are never reused.
    pub async fn finish_file(&mut self) -> Result<()> {
        if let Some(writer) = self.current.take() {
            let (first, last) = self.file_paths.take().context("delete file has no rows")?;
            for mut file in writer.close().await? {
                file.content(DataContentType::PositionDeletes)
                    .partition(Struct::empty())
                    .partition_spec_id(self.spec_id);
                // Parquet's string statistics can be omitted or truncated. These
                // exact bounds let readers exclude unrelated delete-file ranges.
                let metadata = file.build()?;
                let mut lower = metadata.lower_bounds().clone();
                let mut upper = metadata.upper_bounds().clone();
                lower.insert(RESERVED_FIELD_ID_DELETE_FILE_PATH, Datum::string(&first));
                upper.insert(RESERVED_FIELD_ID_DELETE_FILE_PATH, Datum::string(&last));
                file.lower_bounds(lower).upper_bounds(upper);
                self.files.push(file.build()?);
            }
        }
        self.row_group = RowGroupBudget::default();
        Ok(())
    }
    pub async fn close(mut self) -> Result<Vec<DataFile>> {
        self.finish_file().await?;
        Ok(self.files)
    }
}
