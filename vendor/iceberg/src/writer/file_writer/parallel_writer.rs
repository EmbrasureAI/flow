// Added by Embrasure Flow; see LOCAL_CHANGES.md.
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! An async Arrow-to-Parquet writer that can encode the columns of a batch on
//! several threads. It produces exactly the bytes [`AsyncArrowWriter`] would:
//! the same column writers, written in the same order per column, appended to
//! row groups at the same row and byte boundaries. Only which thread encodes
//! each top-level field changes.
//!
//! Parallelism is adaptive. A batch is encoded on more threads only when it is
//! large enough to amortize the handoff, when its work is spread across fields
//! (one dominant field cannot be split, so a narrow table stays sequential),
//! and only with helper threads that are free in a process-wide budget, so
//! concurrent writers fall back to encoding on their own thread instead of
//! oversubscribing the host.
//!
//! [`AsyncArrowWriter`]: parquet::arrow::AsyncArrowWriter

use std::mem;
use std::ops::Range;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

use arrow_array::{Array, RecordBatch};
use arrow_schema::SchemaRef;
use bytes::Bytes;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_writer::{
    ArrowColumnWriter, ArrowRowGroupWriterFactory, ArrowWriterOptions, compute_leaves,
};
use parquet::arrow::async_writer::AsyncFileWriter;
use parquet::errors::{ParquetError, Result};
use parquet::file::metadata::ParquetMetaData;
use parquet::file::properties::WriterProperties;
use parquet::file::writer::SerializedFileWriter;

/// Helper threads available to all writers in the process, beyond the calling
/// thread of each. `usize::MAX` means not yet configured.
static HELPERS_AVAILABLE: AtomicUsize = AtomicUsize::new(usize::MAX);

/// Set the number of threads that may encode Parquet columns at once across
/// the whole process, including each writer's own thread: `0` or `1` keeps
/// every write sequential. Without a call, the default is
/// [`default_encode_threads`]. Takes effect for writes that start afterwards.
pub fn set_parquet_encode_threads(threads: usize) {
    HELPERS_AVAILABLE.store(threads.saturating_sub(1), Ordering::SeqCst);
}

/// Half the available cores, at most four, and at least one.
pub fn default_encode_threads() -> usize {
    std::thread::available_parallelism()
        .map(|cores| (cores.get() / 2).clamp(1, 4))
        .unwrap_or(1)
}

/// Take up to `wanted` helpers from the process-wide budget.
fn acquire_helpers(wanted: usize) -> usize {
    if wanted == 0 {
        return 0;
    }
    let _ = HELPERS_AVAILABLE.compare_exchange(
        usize::MAX,
        default_encode_threads().saturating_sub(1),
        Ordering::SeqCst,
        Ordering::SeqCst,
    );
    let mut available = HELPERS_AVAILABLE.load(Ordering::SeqCst);
    loop {
        let taken = available.min(wanted);
        if taken == 0 {
            return 0;
        }
        match HELPERS_AVAILABLE.compare_exchange_weak(
            available,
            available - taken,
            Ordering::SeqCst,
            Ordering::SeqCst,
        ) {
            Ok(_) => return taken,
            Err(current) => available = current,
        }
    }
}

static PARALLEL_BATCHES: AtomicUsize = AtomicUsize::new(0);

/// Batches encoded with at least one helper thread since the process started.
pub fn parquet_parallel_batches() -> usize {
    PARALLEL_BATCHES.load(Ordering::Relaxed)
}

/// Returns helpers to the budget when dropped, including on panic.
struct Helpers(usize);

impl Drop for Helpers {
    fn drop(&mut self) {
        if self.0 > 0 {
            HELPERS_AVAILABLE.fetch_add(self.0, Ordering::SeqCst);
        }
    }
}

/// When a batch is worth encoding on several threads.
#[derive(Clone, Copy, Debug)]
pub struct Adaptive {
    /// Minimum Arrow memory of the batch: below it the handoff costs more
    /// than the encoding it spreads.
    pub min_batch_bytes: usize,
    /// Minimum achievable speedup, total work over the largest field's.
    pub min_speedup: f64,
}

impl Default for Adaptive {
    fn default() -> Self {
        Self {
            min_batch_bytes: 256 << 10,
            min_speedup: 1.5,
        }
    }
}

impl Adaptive {
    /// Helper threads worth asking for, given each field's estimated cost.
    pub fn helpers_wanted(&self, costs: &[usize]) -> usize {
        let total: usize = costs.iter().sum();
        let largest = costs.iter().copied().max().unwrap_or(0);
        if costs.len() < 2 || total < self.min_batch_bytes || largest == 0 {
            return 0;
        }
        let bound = total as f64 / largest as f64;
        if bound < self.min_speedup {
            return 0;
        }
        // More threads than the bound cannot help: the largest field alone
        // takes a 1/bound share of the work.
        (bound.floor() as usize).min(costs.len()) - 1
    }
}

struct InProgress {
    writers: Vec<ArrowColumnWriter>,
    buffered_rows: usize,
}

impl InProgress {
    fn estimated_bytes(&self) -> usize {
        self.writers
            .iter()
            .map(|writer| writer.get_estimated_total_bytes())
            .sum()
    }
}

/// Drop-in for the subset of [`parquet::arrow::AsyncArrowWriter`] that
/// [`super::ParquetWriter`] uses, for writer properties without content-defined
/// chunking.
pub struct ParallelArrowWriter<W> {
    file: SerializedFileWriter<Vec<u8>>,
    factory: ArrowRowGroupWriterFactory,
    schema: SchemaRef,
    /// The column writers (leaf columns) of each top-level field.
    fields: Vec<Range<usize>>,
    max_rows: Option<usize>,
    max_bytes: Option<usize>,
    in_progress: Option<InProgress>,
    adaptive: Adaptive,
    sink: W,
}

impl<W: AsyncFileWriter> ParallelArrowWriter<W> {
    /// A writer of `schema` with `props` into `sink`, using `adaptive` to
    /// decide which batches to encode on several threads.
    pub fn try_new(
        sink: W,
        schema: SchemaRef,
        props: WriterProperties,
        adaptive: Adaptive,
    ) -> Result<Self> {
        let max_rows = props.max_row_group_row_count();
        let max_bytes = props.max_row_group_bytes();
        // ArrowWriter embeds the Arrow schema and coerces types exactly as the
        // sequential writer does; take its file writer and column factory.
        let options = ArrowWriterOptions::new().with_properties(props);
        let (file, factory) =
            ArrowWriter::try_new_with_options(Vec::new(), schema.clone(), options)?
                .into_serialized_writer()?;
        let fields = field_leaves(&file, &schema)?;
        Ok(Self {
            file,
            factory,
            schema,
            fields,
            max_rows,
            max_bytes,
            in_progress: None,
            adaptive,
            sink,
        })
    }

    /// Bytes written to completed row groups.
    pub fn bytes_written(&self) -> usize {
        self.file.bytes_written()
    }

    /// Estimated encoded size of the row group in progress.
    pub fn in_progress_size(&self) -> usize {
        self.in_progress
            .as_ref()
            .map_or(0, InProgress::estimated_bytes)
    }

    /// Encode `batch`, starting new row groups at the same row and byte limits
    /// as [`ArrowWriter::write`], and hand completed row groups to the sink.
    pub async fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        let before = self.file.flushed_row_groups().len();
        self.write_sync(batch)?;
        if before != self.file.flushed_row_groups().len() {
            self.drain().await?;
        }
        Ok(())
    }

    /// Close the row group in progress and hand it to the sink.
    pub async fn flush(&mut self) -> Result<()> {
        self.flush_sync()?;
        self.drain().await
    }

    /// Write the footer and complete the sink.
    pub async fn finish(&mut self) -> Result<ParquetMetaData> {
        self.flush_sync()?;
        let metadata = self.file.finish()?;
        self.drain().await?;
        self.sink.complete().await?;
        Ok(metadata)
    }

    /// The sink, after [`Self::finish`].
    pub fn into_inner(self) -> W {
        self.sink
    }

    async fn drain(&mut self) -> Result<()> {
        let buffer = mem::take(self.file.inner_mut());
        self.sink
            .write(Bytes::from(buffer))
            .await
            .map_err(|error| ParquetError::External(Box::new(error)))
    }

    /// [`ArrowWriter::write`] without content-defined chunking.
    fn write_sync(&mut self, batch: &RecordBatch) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        if self.in_progress.is_none() {
            let writers = self
                .factory
                .create_column_writers(self.file.flushed_row_groups().len())?;
            self.in_progress = Some(InProgress {
                writers,
                buffered_rows: 0,
            });
        }
        let in_progress = self.in_progress.as_ref().expect("row group in progress");
        if let Some(max_rows) = self.max_rows
            && in_progress.buffered_rows + batch.num_rows() > max_rows
        {
            let to_write = max_rows - in_progress.buffered_rows;
            let head = batch.slice(0, to_write);
            let tail = batch.slice(to_write, batch.num_rows() - to_write);
            self.write_sync(&head)?;
            return self.write_sync(&tail);
        }
        if let Some(max_bytes) = self.max_bytes
            && in_progress.buffered_rows > 0
        {
            let current_bytes = in_progress.estimated_bytes();
            if current_bytes >= max_bytes {
                self.flush_sync()?;
                return self.write_sync(batch);
            }
            let average = current_bytes / in_progress.buffered_rows;
            if average > 0 {
                let fit = (max_bytes - current_bytes) / average;
                if batch.num_rows() > fit {
                    if fit > 0 {
                        let head = batch.slice(0, fit);
                        let tail = batch.slice(fit, batch.num_rows() - fit);
                        self.write_sync(&head)?;
                        return self.write_sync(&tail);
                    }
                    self.flush_sync()?;
                    return self.write_sync(batch);
                }
            }
        }
        let in_progress = self.in_progress.as_mut().expect("row group in progress");
        encode(
            &self.schema,
            &self.fields,
            &mut in_progress.writers,
            batch,
            &self.adaptive,
        )?;
        in_progress.buffered_rows += batch.num_rows();
        let full = self
            .max_rows
            .is_some_and(|max| in_progress.buffered_rows >= max)
            || self
                .max_bytes
                .is_some_and(|max| in_progress.estimated_bytes() >= max);
        if full {
            self.flush_sync()?;
        }
        Ok(())
    }

    fn flush_sync(&mut self) -> Result<()> {
        let Some(in_progress) = self.in_progress.take() else {
            return Ok(());
        };
        let mut row_group = self.file.next_row_group()?;
        for writer in in_progress.writers {
            writer.close()?.append_to_row_group(&mut row_group)?;
        }
        row_group.close()?;
        Ok(())
    }
}

/// Each top-level field's leaf columns, which are contiguous and in order.
fn field_leaves(file: &SerializedFileWriter<Vec<u8>>, schema: &SchemaRef) -> Result<Vec<Range<usize>>> {
    let descriptor = file.schema_descr();
    let mut fields: Vec<Range<usize>> = (0..schema.fields().len()).map(|_| 0..0).collect();
    for leaf in 0..descriptor.num_columns() {
        let root = descriptor.get_column_root_idx(leaf);
        let range = fields
            .get_mut(root)
            .ok_or_else(|| ParquetError::General("Parquet leaf outside the Arrow schema".into()))?;
        if range.start == range.end {
            *range = leaf..leaf + 1;
        } else if range.end == leaf {
            range.end = leaf + 1;
        } else {
            return Err(ParquetError::General(
                "Parquet leaves of a field are not contiguous".into(),
            ));
        }
    }
    let mut next = 0;
    for range in &fields {
        if range.start != next {
            return Err(ParquetError::General(
                "Parquet leaves are not in field order".into(),
            ));
        }
        next = range.end;
    }
    Ok(fields)
}

/// Write each field's leaves to its column writers, as `ArrowRowGroupWriter`
/// does, on the calling thread and any helpers worth taking.
fn encode(
    schema: &SchemaRef,
    fields: &[Range<usize>],
    writers: &mut [ArrowColumnWriter],
    batch: &RecordBatch,
    adaptive: &Adaptive,
) -> Result<()> {
    let costs: Vec<usize> = batch
        .columns()
        .iter()
        .map(|column| column.get_array_memory_size())
        .collect();
    let helpers = Helpers(acquire_helpers(adaptive.helpers_wanted(&costs)));
    if helpers.0 > 0 {
        PARALLEL_BATCHES.fetch_add(1, Ordering::Relaxed);
    }
    if helpers.0 == 0 {
        for (index, range) in fields.iter().enumerate() {
            write_field(schema, batch, index, &mut writers[range.clone()])?;
        }
        return Ok(());
    }
    // Hand out fields largest first, so the long ones start early and the
    // short ones fill in around them.
    let mut slices: Vec<(usize, &mut [ArrowColumnWriter])> = Vec::with_capacity(fields.len());
    let mut rest = writers;
    for (index, range) in fields.iter().enumerate() {
        let (field, tail) = rest.split_at_mut(range.len());
        slices.push((index, field));
        rest = tail;
    }
    slices.sort_by_key(|(index, _)| std::cmp::Reverse(costs[*index]));
    let work: Vec<Mutex<Option<(usize, &mut [ArrowColumnWriter])>>> =
        slices.into_iter().map(|slice| Mutex::new(Some(slice))).collect();
    let next = AtomicUsize::new(0);
    let failure = Mutex::new(None::<ParquetError>);
    let run = || {
        loop {
            let claimed = next.fetch_add(1, Ordering::Relaxed);
            let Some(slot) = work.get(claimed) else {
                return;
            };
            let (index, field) = slot
                .lock()
                .expect("each field is claimed once")
                .take()
                .expect("each field is claimed once");
            if let Err(error) = write_field(schema, batch, index, field) {
                failure
                    .lock()
                    .expect("failure slot")
                    .get_or_insert(error);
                // Stop handing out work; the batch has failed.
                next.store(work.len(), Ordering::Relaxed);
                return;
            }
        }
    };
    std::thread::scope(|scope| {
        for _ in 0..helpers.0 {
            scope.spawn(run);
        }
        run();
    });
    drop(helpers);
    match failure.into_inner().expect("failure slot") {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

fn write_field(
    schema: &SchemaRef,
    batch: &RecordBatch,
    index: usize,
    writers: &mut [ArrowColumnWriter],
) -> Result<()> {
    let leaves = compute_leaves(schema.field(index), batch.column(index))?;
    if leaves.len() != writers.len() {
        return Err(ParquetError::General(
            "Arrow leaves differ from Parquet columns".into(),
        ));
    }
    for (leaf, writer) in leaves.iter().zip(writers) {
        writer.write(leaf)?;
    }
    Ok(())
}
