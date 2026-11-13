//! An input data file's live rows at a rewrite's base, read one batch at a
//! time. Both the sequential scan and the merge read inputs through this.

use crate::{
    ReadLimits,
    columnar::conform,
    parquet_scan::{Input, ParquetInput},
};
use anyhow::{Context, Result, ensure};
use arrow_array::{BooleanArray, RecordBatch};
use arrow_schema::SchemaRef;
use arrow_select::filter::filter_record_batch;
use flow_iceberg_ext::SnapshotView;
use flow_model::{FileId, RowLocation, TableSchema};
use flow_state_store::StateStore;
use iceberg::table::Table;

/// One decoded batch's live rows, in the table's Arrow schema.
pub(crate) struct LiveColumns {
    pub(crate) file_id: FileId,
    pub(crate) batch: RecordBatch,
    /// Physical ordinals in the input file, before filtering position deletes.
    pub(crate) positions: Vec<u64>,
    pub(crate) lineage: Option<Vec<flow_materializer::RowLineage>>,
}

type Deletes<'a> = Box<dyn Iterator<Item = flow_state_store::Result<RowLocation>> + Send + 'a>;

/// Live rows of one input: staged position deletes removed, columns conformed
/// to the writer schema and, for v3, row lineage resolved. The caller must
/// have staged every applicable delete under `scan_id` first.
pub(crate) struct LiveInput<'a> {
    file_id: FileId,
    schema: &'a TableSchema,
    target: SchemaRef,
    reader: ParquetInput,
    deletes: Deletes<'a>,
    next_delete: Option<u64>,
    position: u64,
    record_count: u64,
    batch_rows: usize,
    /// `(first_row_id, data sequence)` of a v3 input.
    lineage: Option<(Option<i64>, i64)>,
    finished: bool,
}

impl<'a> LiveInput<'a> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn open(
        table: &Table,
        schema: &'a TableSchema,
        view: &SnapshotView,
        scratch: &'a StateStore,
        scan_id: &str,
        file: &FileId,
        source: Input,
        batch_rows: usize,
        read_limits: &ReadLimits,
    ) -> Result<Self> {
        let entry = view
            .live_files
            .get(&file.0)
            .with_context(|| format!("scan input {} is not live", file.0))?;
        let v3 = table.metadata().format_version() == iceberg::spec::FormatVersion::V3;
        let lineage = if v3 {
            Some((
                entry.data_file.first_row_id(),
                entry.sequence_number.context("missing data sequence")?,
            ))
        } else {
            None
        };
        let reader = ParquetInput::open(
            table.file_io(),
            &file.0,
            source,
            None,
            batch_rows,
            read_limits,
        )
        .await?;
        let mut deletes: Deletes<'a> =
            Box::new(scratch.file_position_deletes(scan_id, &schema.table_id, file));
        let next_delete = deletes
            .next()
            .transpose()?
            .map(|location| location.row_position);
        Ok(Self {
            file_id: file.clone(),
            schema,
            target: flow_materializer::writer_arrow_schema(schema),
            reader,
            deletes,
            next_delete,
            position: 0,
            record_count: entry.data_file.record_count(),
            batch_rows,
            lineage,
            finished: false,
        })
    }

    pub(crate) fn file_id(&self) -> &FileId {
        &self.file_id
    }

    /// See [`ParquetInput::held_bytes`].
    pub(crate) fn held_bytes(&self) -> Result<u64> {
        // Schema evolution can add nullable columns absent from the file.
        // Their null arrays (and corresponding sort/gather slots) still occupy
        // memory, so charge the target schema's fixed slots independently.
        let slots = self.target.fields().iter().fold(0u64, |bytes, field| {
            bytes.saturating_add(field.data_type().primitive_width().unwrap_or(16) as u64 + 1)
        });
        Ok(self.reader.held_bytes()?.saturating_add(
            slots
                .saturating_mul(self.batch_rows as u64)
                .saturating_mul(4),
        ))
    }

    pub(crate) async fn prefetch(&mut self) -> Result<()> {
        self.reader.prefetch().await
    }

    /// The next batch with at least one live row, or `None` once the file is
    /// read and its deletes and row count are checked.
    pub(crate) async fn next(&mut self) -> Result<Option<LiveColumns>> {
        if self.finished {
            return Ok(None);
        }
        while let Some(batch) = self.reader.next_batch().await? {
            let mut lineage = match self.lineage {
                Some((first_row_id, sequence)) => Some(flow_materializer::read_row_lineage(
                    &batch,
                    self.position,
                    first_row_id,
                    sequence,
                )?),
                None => None,
            };
            let batch = conform(self.schema, &self.target, &batch)?;
            let mut live = Vec::with_capacity(batch.num_rows());
            let mut positions = Vec::with_capacity(batch.num_rows());
            for _ in 0..batch.num_rows() {
                ensure!(
                    self.next_delete
                        .is_none_or(|deleted| deleted >= self.position),
                    "position delete fell behind scan"
                );
                if self.next_delete == Some(self.position) {
                    self.next_delete = self
                        .deletes
                        .next()
                        .transpose()?
                        .map(|location| location.row_position);
                    live.push(false);
                } else {
                    positions.push(self.position);
                    live.push(true);
                }
                self.position += 1;
            }
            if positions.is_empty() {
                continue;
            }
            let batch = if positions.len() == batch.num_rows() {
                batch
            } else {
                if let Some(lineage) = &mut lineage {
                    let mut live = live.iter();
                    lineage.retain(|_| *live.next().expect("one flag per row"));
                }
                filter_record_batch(&batch, &BooleanArray::from(live))?
            };
            return Ok(Some(LiveColumns {
                file_id: self.file_id.clone(),
                batch,
                positions,
                lineage,
            }));
        }
        ensure!(
            self.next_delete.is_none(),
            "position delete is outside input file"
        );
        ensure!(
            self.position == self.record_count,
            "data file row count differs from metadata"
        );
        self.finished = true;
        Ok(None)
    }
}
