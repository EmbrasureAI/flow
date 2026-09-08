//! Admission limits for the flat Parquet columns consumed by maintenance.
//!
//! Limits cover fetched bytes and valid-file logical payloads. They do not
//! bound allocator overhead or allocations caused by corrupt codec/page data.

use std::{collections::BTreeSet, ops::Range, sync::Arc};

use anyhow::{Context, Result, bail, ensure};
use arrow_array::RecordBatch;
use bytes::Bytes;
use futures::{TryStreamExt, future::BoxFuture};
use iceberg::{
    arrow::ArrowFileReader,
    io::{FileIO, FileMetadata},
};
use parquet::{
    arrow::{
        ProjectionMask,
        arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions},
        async_reader::{AsyncFileReader, ParquetRecordBatchStreamBuilder},
    },
    basic::Type as PhysicalType,
    errors::{ParquetError, Result as ParquetResult},
    file::metadata::{PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader, RowGroupMetaData},
};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ReadLimits {
    pub footer_bytes: u64,
    /// Span of selected compressed column chunks, including coalescing gaps.
    pub row_group_fetch_bytes: u64,
    /// Uncompressed encoded pages, before dictionary/delta value expansion.
    pub row_group_uncompressed_bytes: u64,
    /// Expanded values, fixed slots, offsets and validity allowance per batch.
    pub batch_bytes: u64,
}

impl Default for ReadLimits {
    fn default() -> Self {
        Self {
            footer_bytes: 8 << 20,
            row_group_fetch_bytes: 128 << 20,
            row_group_uncompressed_bytes: 256 << 20,
            batch_bytes: 128 << 20,
        }
    }
}

impl ReadLimits {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.footer_bytes >= 8
                && self.row_group_fetch_bytes > 0
                && self.row_group_uncompressed_bytes > 0
                && self.batch_bytes > 0,
            "Parquet read limits must be positive and the footer limit at least eight bytes"
        );
        ensure!(
            [
                self.footer_bytes,
                self.row_group_fetch_bytes,
                self.row_group_uncompressed_bytes
            ]
            .into_iter()
            .all(|bytes| usize::try_from(bytes).is_ok())
                && self.batch_bytes <= i32::MAX as u64,
            "Parquet read limits exceed platform or Arrow offset capacity"
        );
        Ok(())
    }
}

struct BoundedReader<R> {
    inner: R,
    file_size: u64,
    fetch_limit: u64,
    footer_limit: u64,
    metadata_remaining: Option<u64>,
}

impl<R> BoundedReader<R> {
    fn new(inner: R, file_size: u64, limits: &ReadLimits) -> Self {
        Self {
            inner,
            file_size,
            fetch_limit: limits.row_group_fetch_bytes,
            footer_limit: limits.footer_bytes,
            metadata_remaining: None,
        }
    }

    fn admit_ranges(&mut self, ranges: &[Range<u64>]) -> ParquetResult<()> {
        let mut start = self.file_size;
        let mut end = 0;
        for range in ranges {
            if range.start > range.end || range.end > self.file_size {
                return Err(ParquetError::General(
                    "Parquet range is outside the input file".into(),
                ));
            }
            start = start.min(range.start);
            end = end.max(range.end);
        }
        if let Some(remaining) = &mut self.metadata_remaining {
            *remaining = remaining
                .checked_sub(end.saturating_sub(start))
                .ok_or_else(|| {
                    ParquetError::General(format!(
                        "Parquet footer exceeds byte limit {}",
                        self.footer_limit
                    ))
                })?;
        }
        if end.saturating_sub(start) > self.fetch_limit {
            return Err(ParquetError::General(format!(
                "Parquet fetch span {} exceeds byte limit {}",
                end - start,
                self.fetch_limit
            )));
        }
        Ok(())
    }
}

impl<R: AsyncFileReader> AsyncFileReader for BoundedReader<R> {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, ParquetResult<Bytes>> {
        Box::pin(async move {
            self.admit_ranges(std::slice::from_ref(&range))?;
            let expected = range.end - range.start;
            let bytes = self.inner.get_bytes(range).await?;
            if bytes.len() as u64 != expected {
                return Err(ParquetError::General(
                    "Parquet range response length mismatch".into(),
                ));
            }
            Ok(bytes)
        })
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<u64>>,
    ) -> BoxFuture<'_, ParquetResult<Vec<Bytes>>> {
        Box::pin(async move {
            // ArrowFileReader coalesces nearby ranges and retains those larger
            // allocations behind Bytes slices. Bound the span before it does so.
            self.admit_ranges(&ranges)?;
            let bytes = self.inner.get_byte_ranges(ranges.clone()).await?;
            if bytes.len() != ranges.len()
                || bytes
                    .iter()
                    .zip(&ranges)
                    .any(|(bytes, range)| bytes.len() as u64 != range.end - range.start)
            {
                return Err(ParquetError::General(
                    "Parquet range response length mismatch".into(),
                ));
            }
            Ok(bytes)
        })
    }

    fn get_metadata<'a>(
        &'a mut self,
        options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, ParquetResult<Arc<ParquetMetaData>>> {
        Box::pin(async move {
            let fetch_limit = self.fetch_limit;
            self.fetch_limit = self.footer_limit;
            self.metadata_remaining = Some(self.footer_limit);
            let size = self.file_size;
            // Read only the tail first. A prefetch hint is not a footer cap.
            // Full scans need neither page indexes nor their extra allocations.
            let result = ParquetMetaDataReader::new()
                .with_prefetch_hint(Some(8))
                .with_page_index_policy(PageIndexPolicy::Skip)
                .with_metadata_options(options.map(|options| options.metadata_options().clone()))
                .load_and_finish(&mut *self, size)
                .await;
            self.fetch_limit = fetch_limit;
            self.metadata_remaining = None;
            result.map(Arc::new)
        })
    }
}

struct BatchCharge {
    fixed_per_row: u64,
    // Worst-case size of one value and optional total decoded column payload.
    variable: Vec<(u64, Option<u64>)>,
}

impl BatchCharge {
    fn bytes(&self, rows: usize) -> Option<u64> {
        let rows = rows as u64;
        let mut bytes = self.fixed_per_row.checked_mul(rows)?;
        for (per_value, total) in &self.variable {
            let variable = per_value
                .saturating_mul(rows)
                .min(total.unwrap_or(u64::MAX));
            bytes = bytes.checked_add(variable)?;
        }
        Some(bytes)
    }
}

fn admit_group(
    metadata: &ArrowReaderMetadata,
    group: &RowGroupMetaData,
    columns: &[usize],
    requested_rows: usize,
    file_size: u64,
    limits: &ReadLimits,
) -> Result<usize> {
    let parquet_schema = metadata.parquet_schema();
    let mut charge = BatchCharge {
        fixed_per_row: 0,
        variable: Vec::new(),
    };
    let mut start = file_size;
    let mut end = 0;
    let mut uncompressed = 0u64;
    for &index in columns {
        let column = group.column(index);
        let descriptor = parquet_schema.column(index);
        ensure!(
            descriptor.max_rep_level() == 0 && descriptor.path().parts().len() == 1,
            "Parquet admission supports selected flat columns only"
        );
        let offset = u64::try_from(
            column
                .dictionary_page_offset()
                .unwrap_or(column.data_page_offset()),
        )
        .context("negative Parquet column offset")?;
        let length =
            u64::try_from(column.compressed_size()).context("negative Parquet compressed size")?;
        let column_end = offset
            .checked_add(length)
            .context("Parquet column range overflow")?;
        ensure!(
            column_end <= file_size,
            "Parquet column range exceeds input file"
        );
        start = start.min(offset);
        end = end.max(column_end);
        let encoded = u64::try_from(column.uncompressed_size())
            .context("negative Parquet uncompressed size")?;
        uncompressed = uncompressed
            .checked_add(encoded)
            .context("Parquet uncompressed size overflow")?;
        let field = metadata
            .schema()
            .field(parquet_schema.get_column_root_idx(index));
        let fixed = if let Some(width) = field.data_type().primitive_width() {
            width as u64
        } else {
            match descriptor.physical_type() {
                PhysicalType::BOOLEAN => 1,
                PhysicalType::FIXED_LEN_BYTE_ARRAY => u64::try_from(descriptor.type_length())
                    .context("negative fixed-width Parquet size")?,
                PhysicalType::BYTE_ARRAY => {
                    let total = column
                        .unencoded_byte_array_data_bytes()
                        .map(u64::try_from)
                        .transpose()
                        .context("negative decoded Parquet column size")?;
                    // For valid flat encodings, a single value cannot contain
                    // more bytes than all uncompressed encoded column pages.
                    // Repetition can expand that value across every batch row;
                    // dividing encoded size by row count would be unsafe.
                    charge.variable.push((encoded, total));
                    16 // two i64 offsets, including the last offset in a one-row batch
                }
                other => bail!(
                    "unsupported projected Arrow type {:?} for Parquet {other:?}",
                    field.data_type()
                ),
            }
        };
        charge.fixed_per_row = charge
            .fixed_per_row
            .checked_add(fixed)
            .and_then(|bytes| bytes.checked_add(1))
            .context("Parquet fixed-width batch size overflow")?;
    }
    ensure!(
        end.saturating_sub(start) <= limits.row_group_fetch_bytes,
        "Parquet row group fetch span {} exceeds byte limit {}",
        end.saturating_sub(start),
        limits.row_group_fetch_bytes
    );
    ensure!(
        uncompressed <= limits.row_group_uncompressed_bytes,
        "Parquet row group uncompressed size {uncompressed} exceeds byte limit {}",
        limits.row_group_uncompressed_bytes
    );
    ensure!(
        charge
            .bytes(1)
            .is_some_and(|bytes| bytes <= limits.batch_bytes),
        "Parquet cannot admit one row within expanded batch byte limit {}; use smaller source row groups or increase the read budget",
        limits.batch_bytes
    );
    let mut low = 1;
    let mut high = requested_rows.min(usize::try_from(group.num_rows())?);
    while low < high {
        let middle = low + (high - low).div_ceil(2);
        if charge
            .bytes(middle)
            .is_some_and(|bytes| bytes <= limits.batch_bytes)
        {
            low = middle;
        } else {
            high = middle - 1;
        }
    }
    Ok(low)
}

pub(crate) async fn read_batches(
    file_io: &FileIO,
    path: &str,
    field_ids: Option<&[i32]>,
    batch_rows: usize,
    limits: &ReadLimits,
    mut visit: impl AsyncFnMut(RecordBatch) -> Result<()>,
) -> Result<()> {
    limits.validate()?;
    ensure!(batch_rows > 0, "Parquet batch row limit must be positive");
    let input = file_io.new_input(path)?;
    let file_size = input.metadata().await?.size;
    let reader = ArrowFileReader::new(FileMetadata { size: file_size }, input.reader().await?);
    let mut reader = BoundedReader::new(reader, file_size, limits);
    let metadata = ArrowReaderMetadata::load_async(
        &mut reader,
        ArrowReaderOptions::new()
            .with_skip_arrow_metadata(true)
            .with_page_index_policy(PageIndexPolicy::Skip),
    )
    .await?;
    let parquet_schema = metadata.parquet_schema();
    let mut selected = BTreeSet::new();
    let mut columns = Vec::new();
    for (index, column) in parquet_schema.columns().iter().enumerate() {
        let info = column.self_type().get_basic_info();
        if field_ids.is_none_or(|ids| info.has_id() && ids.contains(&info.id())) {
            if field_ids.is_some() {
                ensure!(
                    selected.insert(info.id()),
                    "duplicate selected Parquet field ID {}",
                    info.id()
                );
            }
            columns.push(index);
        }
    }
    if let Some(ids) = field_ids {
        ensure!(
            ids.iter().copied().collect::<BTreeSet<_>>() == selected,
            "Parquet projection field ID is missing"
        );
    }
    ensure!(!columns.is_empty(), "Parquet projection has no columns");
    let projection = ProjectionMask::leaves(parquet_schema, columns.iter().copied());
    let expected_rows = u64::try_from(metadata.metadata().file_metadata().num_rows())?;
    let declared_rows = metadata
        .metadata()
        .row_groups()
        .iter()
        .try_fold(0u64, |rows, group| {
            rows.checked_add(u64::try_from(group.num_rows())?)
                .context("Parquet declared row count overflow")
        })?;
    ensure!(
        declared_rows == expected_rows,
        "Parquet row-group counts differ from file metadata"
    );
    let mut total_rows = 0u64;
    for (index, group) in metadata.metadata().row_groups().iter().enumerate() {
        let expected_group_rows = u64::try_from(group.num_rows())?;
        if expected_group_rows == 0 {
            continue;
        }
        let rows = admit_group(&metadata, group, &columns, batch_rows, file_size, limits)
            .with_context(|| format!("Parquet input {path}, row group {index}"))?;
        let reader = ArrowFileReader::new(FileMetadata { size: file_size }, input.reader().await?);
        let reader = BoundedReader::new(reader, file_size, limits);
        let mut batches =
            ParquetRecordBatchStreamBuilder::new_with_metadata(reader, metadata.clone())
                .with_projection(projection.clone())
                .with_row_groups(vec![index])
                .with_batch_size(rows)
                .build()?;
        let mut group_rows = 0u64;
        while let Some(batch) = batches.try_next().await? {
            group_rows = group_rows
                .checked_add(batch.num_rows() as u64)
                .context("Parquet row count overflow")?;
            ensure!(
                group_rows <= expected_group_rows,
                "Parquet decoded row count exceeds row group metadata"
            );
            visit(batch).await?;
        }
        ensure!(
            group_rows == expected_group_rows,
            "Parquet decoded row count differs from row group metadata"
        );
        total_rows = total_rows
            .checked_add(group_rows)
            .context("Parquet file row count overflow")?;
    }
    ensure!(
        total_rows == expected_rows,
        "Parquet decoded row count differs from file metadata"
    );
    Ok(())
}

#[cfg(test)]
mod tests;
