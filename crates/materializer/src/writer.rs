use anyhow::{Context, Result, ensure};
use arrow_array::{
    Array, ArrayRef, Decimal128Array, Int64Array, RecordBatch, StringArray, cast::AsArray,
};
use flow_iceberg_ext::{ArtifactSet, ArtifactTracker};
use flow_model::{
    ColumnType, FileId, OperationId, PgLsn, Row, RowLocation, TableSchema, Value, fingerprint,
};
use futures::{StreamExt, future::BoxFuture, stream::FuturesUnordered};
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

use crate::{
    arrow::{validated_columns, writer_arrow_schema},
    iceberg_schema,
};

#[derive(Debug, Clone)]
pub struct WriterConfig {
    pub target_file_bytes: usize,
    pub row_group_rows: usize,
    /// Rows per batch when compaction reads and rewrites files, independent of
    /// how many rows each written row group holds.
    pub batch_rows: usize,
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
            batch_rows: 8192,
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
                && self.batch_rows > 0
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
fn fixed_bytes(data_type: &ColumnType) -> usize {
    1 + match data_type {
        ColumnType::Bool => 1,
        ColumnType::Int32 | ColumnType::Date => 4,
        ColumnType::Int64
        | ColumnType::Float64
        | ColumnType::TimestampMicros
        | ColumnType::TimestampTzMicros
        | ColumnType::String
        | ColumnType::Binary => 8,
        ColumnType::Uuid | ColumnType::Decimal { .. } => 16,
    }
}

fn row_bytes(schema: &TableSchema, row: &Row) -> Result<usize> {
    schema
        .columns
        .iter()
        .zip(row)
        .try_fold(0usize, |bytes, (column, value)| {
            let variable = match value {
                Value::String(value) => value.len(),
                Value::Binary(value) => value.len(),
                _ => 0,
            };
            bytes
                .checked_add(fixed_bytes(&column.data_type))
                .and_then(|bytes| bytes.checked_add(variable))
                .context("row logical payload size overflow")
        })
}

/// [`row_bytes`] for each row of a batch in the writer's Arrow schema.
fn batch_row_bytes(schema: &TableSchema, batch: &RecordBatch) -> Result<Vec<usize>> {
    let fixed: usize = schema
        .columns
        .iter()
        .map(|column| fixed_bytes(&column.data_type))
        .sum();
    let mut bytes = vec![fixed; batch.num_rows()];
    for (column, array) in schema.columns.iter().zip(batch.columns()) {
        // Offsets give each value's length without touching its payload.
        let offsets: Vec<usize> = match column.data_type {
            ColumnType::String => array
                .as_string_opt::<i32>()
                .map(|array| array.offsets().windows(2).map(|r| (r[1] - r[0]) as usize))
                .context("string type mismatch")?
                .collect(),
            ColumnType::Binary => array
                .as_binary_opt::<i64>()
                .map(|array| array.offsets().windows(2).map(|r| (r[1] - r[0]) as usize))
                .context("binary type mismatch")?
                .collect(),
            _ => continue,
        };
        for (index, (bytes, length)) in bytes.iter_mut().zip(offsets).enumerate() {
            if array.is_valid(index) {
                *bytes = bytes
                    .checked_add(length)
                    .context("row logical payload size overflow")?;
            }
        }
    }
    Ok(bytes)
}

/// One position for a delete writer: an owned [`RowLocation`], or a borrowed
/// `(data file path, row position)` pair already in the writer's spec, which
/// avoids an owned path per position.
pub trait DeletePosition {
    fn data_file(&self) -> &str;
    fn row_position(&self) -> u64;
    /// Whether the position is in the writer's unpartitioned spec `spec_id`.
    fn in_spec(&self, spec_id: i32) -> bool;
}

impl DeletePosition for RowLocation {
    fn data_file(&self) -> &str {
        &self.data_file_id.0
    }
    fn row_position(&self) -> u64 {
        self.row_position
    }
    fn in_spec(&self, spec_id: i32) -> bool {
        self.spec_id == spec_id && self.partition.is_empty()
    }
}

impl DeletePosition for (&str, u64) {
    fn data_file(&self) -> &str {
        self.0
    }
    fn row_position(&self) -> u64 {
        self.1
    }
    fn in_spec(&self, _: i32) -> bool {
        true
    }
}

fn delete_bytes(position: &impl DeletePosition) -> Result<usize> {
    // String offsets, Int64 position, and a validity byte for each column.
    position
        .data_file()
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
    arrow_schema: arrow_schema::SchemaRef,
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
            arrow_schema: writer_arrow_schema(&schema),
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
            // Rows were validated when the write call was admitted.
            let fingerprints: Vec<_> = rows.iter().map(|row| fingerprint(row.borrow())).collect();
            self.write_batch(
                validated_columns(&self.schema, rows, true)?,
                count,
                lineage.map(|values| &values[offset..offset + count]),
                &fingerprints,
                bytes,
                source_lsn,
                &mut locations,
            )
            .await?;
            remaining = &remaining[count..];
        }
        Ok(WrittenBatch { locations })
    }

    /// Write rows already in [`crate::writer_arrow_schema`], such as rewritten scan
    /// output, without converting them through [`Row`]. Applies the same
    /// validation and admission limits as [`Self::write_with_lineage`].
    /// `fingerprints` are the caller's [`flow_model::fingerprint`] per row.
    pub async fn write_arrow(
        &mut self,
        batch: &RecordBatch,
        lineage: Option<&[crate::RowLineage]>,
        fingerprints: &[[u8; 16]],
        source_lsn: PgLsn,
    ) -> Result<WrittenBatch> {
        let rows = batch.num_rows();
        ensure!(
            lineage.is_none_or(|values| values.len() == rows) && fingerprints.len() == rows,
            "row lineage or fingerprint length mismatch"
        );
        ensure!(
            lineage.is_none() || self.lineage_schema.is_some(),
            "writer is not configured for row lineage"
        );
        ensure!(
            batch.schema().fields() == self.arrow_schema.fields(),
            "batch is not in the writer's Arrow schema"
        );
        if rows == 0 {
            return Ok(WrittenBatch { locations: vec![] });
        }
        // The Arrow schema already fixes types and required fields. Decimal
        // precision is the remaining check `validate_row` would apply.
        for (column, array) in self.schema.columns.iter().zip(batch.columns()) {
            if let ColumnType::Decimal { precision, .. } = column.data_type {
                array
                    .as_any()
                    .downcast_ref::<Decimal128Array>()
                    .context("decimal type mismatch")?
                    .validate_decimal_precision(precision)
                    .with_context(|| format!("type or nullability mismatch for {}", column.name))?;
            }
        }
        let lineage_bytes = usize::from(self.lineage_schema.is_some()) * 18;
        let sizes: Vec<_> = batch_row_bytes(&self.schema, batch)?
            .into_iter()
            .map(|bytes| bytes + lineage_bytes)
            .collect();
        // Admit the whole call before opening any output, as the row path does.
        for bytes in &sizes {
            self.config.admit_row(*bytes)?;
        }
        let mut locations = Vec::with_capacity(rows);
        let mut offset = 0;
        while offset < rows {
            let (count, bytes) = bounded_batch(&sizes[offset..], &self.config, |bytes| Ok(*bytes))?;
            self.write_batch(
                batch.slice(offset, count).columns().to_vec(),
                count,
                lineage.map(|values| &values[offset..offset + count]),
                &fingerprints[offset..offset + count],
                bytes,
                source_lsn,
                &mut locations,
            )
            .await?;
            offset += count;
        }
        Ok(WrittenBatch { locations })
    }

    #[allow(clippy::too_many_arguments)]
    async fn write_batch(
        &mut self,
        mut columns: Vec<ArrayRef>,
        rows: usize,
        lineage: Option<&[crate::RowLineage]>,
        fingerprints: &[[u8; 16]],
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
            self.current = Some(
                self.builder
                    .build(self.file_io.new_output(path)?.if_absent())
                    .await?,
            );
        }
        let writer = self.current.as_mut().expect("writer opened above");
        self.row_group
            .prepare(writer, rows, bytes, &self.config)
            .await?;
        let batch = if let Some(schema) = &self.lineage_schema {
            for row_id in [true, false] {
                let values = (0..rows).map(|index| {
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
            RecordBatch::try_new(schema.clone(), columns)?
        } else {
            RecordBatch::try_new(self.arrow_schema.clone(), columns)?
        };
        let path = writer.current_file_path();
        let offset = writer.current_row_num() as u64;
        writer.write(&batch).await?;
        self.row_group.written(rows, bytes, &self.config);
        for (i, fingerprint) in fingerprints.iter().enumerate() {
            locations.push(RowLocation {
                data_file_id: FileId(path.clone()),
                row_position: offset + i as u64,
                data_sequence_number: -1,
                spec_id: self.spec_id,
                partition: vec![],
                source_commit_lsn: source_lsn,
                row_version: 0,
                row_fingerprint: *fingerprint,
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
/// Callers sort with the state store's disk-backed spool or, within a bounded
/// position budget, in memory.
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
    /// Files closing concurrently; see [`Self::finish_file_concurrently`].
    closing: FuturesUnordered<BoxFuture<'static, Result<Vec<DataFile>>>>,
    closed_concurrently: bool,
}

/// Small per-target files cost one object upload each. Overlap that many.
const MAX_CONCURRENT_CLOSES: usize = 8;

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
            closing: FuturesUnordered::new(),
            closed_concurrently: false,
        })
    }
    pub async fn write(&mut self, positions: &[impl DeletePosition]) -> Result<()> {
        if positions.is_empty() {
            return Ok(());
        }
        let mut last = self
            .last
            .as_ref()
            .map(|(path, position)| (path.as_str(), *position));
        for p in positions {
            ensure!(
                p.in_spec(self.spec_id),
                "position deletes require matching unpartitioned spec"
            );
            ensure!(
                p.row_position() <= i64::MAX as u64,
                "position overflows Iceberg long"
            );
            self.config.admit_row(delete_bytes(p)?)?;
            if let Some((path, pos)) = last {
                ensure!(
                    (path, pos) < (p.data_file(), p.row_position()),
                    "delete positions must be sorted and unique"
                );
            }
            last = Some((p.data_file(), p.row_position()));
        }
        let mut remaining = positions;
        while !remaining.is_empty() {
            let (count, bytes) = bounded_batch(remaining, &self.config, delete_bytes)?;
            self.write_batch(&remaining[..count], bytes).await?;
            remaining = &remaining[count..];
        }
        let last = positions.last().expect("nonempty positions");
        self.last = Some((last.data_file().to_owned(), last.row_position()));
        Ok(())
    }

    async fn write_batch(&mut self, positions: &[impl DeletePosition], bytes: usize) -> Result<()> {
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
            self.current = Some(
                self.builder
                    .build(self.file_io.new_output(path)?.if_absent())
                    .await?,
            );
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
                        .map(|p| Some(p.data_file()))
                        .collect::<StringArray>(),
                ),
                Arc::new(
                    positions
                        .iter()
                        .map(|p| Some(p.row_position() as i64))
                        .collect::<Int64Array>(),
                ),
            ],
        )?;
        writer.write(&batch).await?;
        self.row_group.written(positions.len(), bytes, &self.config);
        let first = positions.first().expect("nonempty batch").data_file();
        let last = positions.last().expect("nonempty batch").data_file();
        if let Some((_, upper)) = &mut self.file_paths {
            upper.clear();
            upper.push_str(last);
        } else {
            self.file_paths = Some((first.to_owned(), last.to_owned()));
        }
        Ok(())
    }

    /// Close the current file before admitting another sorted range of deletes.
    /// Calling this without an open file is a no-op. Global path/position ordering
    /// and uniqueness remain enforced across rotations; artifact names are never reused.
    pub async fn finish_file(&mut self) -> Result<()> {
        if let Some(close) = self.take_file()? {
            self.files.extend(close.await?);
        }
        Ok(())
    }

    /// [`Self::finish_file`], but the closed file's upload overlaps later
    /// writes, up to [`MAX_CONCURRENT_CLOSES`] at once. `close` waits for all
    /// of them; dropping the writer cancels unfinished uploads.
    pub async fn finish_file_concurrently(&mut self) -> Result<()> {
        if let Some(close) = self.take_file()? {
            self.closing.push(close);
            self.closed_concurrently = true;
            while self.closing.len() >= MAX_CONCURRENT_CLOSES {
                let files = self.closing.next().await.expect("pending close")?;
                self.files.extend(files);
            }
        }
        Ok(())
    }

    fn take_file(&mut self) -> Result<Option<BoxFuture<'static, Result<Vec<DataFile>>>>> {
        self.row_group = RowGroupBudget::default();
        let Some(writer) = self.current.take() else {
            return Ok(None);
        };
        let (first, last) = self.file_paths.take().context("delete file has no rows")?;
        let spec_id = self.spec_id;
        Ok(Some(Box::pin(async move {
            let mut files = Vec::new();
            for mut file in writer.close().await? {
                file.content(DataContentType::PositionDeletes)
                    .partition(Struct::empty())
                    .partition_spec_id(spec_id);
                // Parquet's string statistics can be omitted or truncated. These
                // exact bounds let readers exclude unrelated delete-file ranges.
                let metadata = file.build()?;
                let mut lower = metadata.lower_bounds().clone();
                let mut upper = metadata.upper_bounds().clone();
                lower.insert(RESERVED_FIELD_ID_DELETE_FILE_PATH, Datum::string(&first));
                upper.insert(RESERVED_FIELD_ID_DELETE_FILE_PATH, Datum::string(&last));
                file.lower_bounds(lower).upper_bounds(upper);
                files.push(file.build()?);
            }
            Ok(files)
        })))
    }

    pub async fn close(mut self) -> Result<Vec<DataFile>> {
        self.finish_file().await?;
        while let Some(files) = self.closing.next().await {
            self.files.extend(files?);
        }
        if self.closed_concurrently {
            // Uploads finish in any order; artifact names follow write order.
            self.files
                .sort_by(|left, right| left.file_path().cmp(right.file_path()));
        }
        Ok(self.files)
    }
}
