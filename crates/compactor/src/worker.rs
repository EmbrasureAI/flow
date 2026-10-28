use crate::{
    CompactionPlan, ReadLimits,
    columnar::{RowEncoder, conform},
    parquet_scan::{Input, Prefetch, read_input_batches},
};
use anyhow::{Result, bail, ensure};
use arrow_array::{Array, BooleanArray, Int64Array, RecordBatch, StringArray};
use arrow_select::filter::filter_record_batch;
use flow_iceberg_ext::{ManifestCache, SnapshotView};
use flow_materializer::{
    DataWriter, DeleteWriter, WriterConfig, rows_from_batch, writer_arrow_schema,
};
use flow_model::{FileId, OperationId, PgLsn, Row, RowLocation, TableId, TableSchema, Value};
use flow_state_store::{
    FileRemap, IndexDelta, LazyRemap, OperationKind, PreparedOperation, RowIndex, StateStore,
};
use iceberg::{io::FileIO, spec::DataFile, table::Table};
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use std::collections::{BTreeSet, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};

/// Artifacts and durable row mappings only. There is deliberately no catalog
/// handle here: the worker cannot commit its output.
pub struct WorkerOutput {
    pub plan: CompactionPlan,
    pub data_files: Vec<DataFile>,
    pub delete_files: Vec<DataFile>,
    pub delete_sequence: Option<i64>,
    pub operation_id: OperationId,
    pub scratch: StateStore,
    pub(crate) masked_inputs: Option<String>,
    pub(crate) table_id: TableId,
    /// Per-input remaps of a lazy build, before catch-up.
    pub(crate) lazy_files: Option<Vec<FileRemap>>,
    /// The complete lazy remap, after catch-up derived exact output counts.
    pub(crate) lazy: Option<LazyRemap>,
}
impl WorkerOutput {
    pub fn table_id(&self) -> TableId {
        self.table_id
    }

    /// Whether this build replaces per-row index deltas with a lazy remap.
    pub fn is_lazy(&self) -> bool {
        self.lazy_files.is_some() || self.lazy.is_some()
    }

    /// The lazy remap to stage instead of [`Self::index_deltas`]. Only a
    /// caught-up build has one; publishing a lazy build without it is an error.
    pub fn lazy_remap(&self) -> Option<&LazyRemap> {
        self.lazy.as_ref()
    }

    pub fn index_deltas(
        &self,
    ) -> flow_state_store::Result<impl Iterator<Item = flow_state_store::Result<IndexDelta>> + '_>
    {
        let mut masked = self.masked_inputs.as_ref().map(|scan| {
            self.scratch
                .position_deletes(scan, &self.table_id)
                .peekable()
        });
        let mut deltas = self.scratch.prepared_deltas(&self.operation_id)?;
        Ok(std::iter::from_fn(move || {
            loop {
                let delta = match deltas.next()? {
                    Ok(delta) => delta,
                    Err(error) => return Some(Err(error)),
                };
                let Some(masked) = &mut masked else {
                    return Some(Ok(delta));
                };
                let Some(expected) = delta.expected.as_ref() else {
                    return Some(Err(flow_state_store::Error::InvalidState(
                        "worker mapping lacks original".into(),
                    )));
                };
                loop {
                    let Some(position) = masked.peek() else {
                        return Some(Ok(delta));
                    };
                    let position = match position {
                        Ok(position) => position,
                        Err(_) => {
                            return Some(Err(masked.next().expect("peeked position").unwrap_err()));
                        }
                    };
                    match (&position.data_file_id, position.row_position)
                        .cmp(&(&expected.data_file_id, expected.row_position))
                    {
                        std::cmp::Ordering::Less => {
                            masked.next();
                        }
                        std::cmp::Ordering::Equal => {
                            masked.next();
                            break;
                        }
                        std::cmp::Ordering::Greater => return Some(Ok(delta)),
                    }
                }
            }
        }))
    }
}

/// Completed data artifacts and original-to-replacement mappings at one frozen
/// index snapshot. No delete outputs have been allocated yet.
pub struct BuiltData {
    pub(crate) output: WorkerOutput,
    pub(crate) base: SnapshotView,
}
impl BuiltData {
    pub fn plan(&self) -> &CompactionPlan {
        &self.output.plan
    }
    pub fn table_id(&self) -> TableId {
        self.output.table_id
    }
    pub fn operation_id(&self) -> &OperationId {
        &self.output.operation_id
    }
}

/// Existing strict worker path, using the same data builder as speculative work.
#[allow(clippy::too_many_arguments)]
pub async fn compact(
    table: &Table,
    schema: TableSchema,
    plan: CompactionPlan,
    operation_id: OperationId,
    live_index: &(impl RowIndex + ?Sized),
    scratch: StateStore,
    config: WriterConfig,
    read_limits: &ReadLimits,
    cache: &ManifestCache,
) -> Result<WorkerOutput> {
    // The strict path publishes without catch-up; it keeps per-row deltas.
    let mut plan = plan;
    plan.lazy_index_remap = false;
    let built = build_data(
        table,
        schema.clone(),
        plan,
        operation_id,
        live_index,
        scratch,
        config.clone(),
        read_limits,
        cache,
        &AtomicBool::new(false),
    )
    .await?;
    let mut output = built.output;
    if table.metadata().format_version() == iceberg::spec::FormatVersion::V3 {
        retain_shared_legacy_deletes(&mut output, &built.base)?;
        return Ok(output);
    }
    output.delete_files =
        write_residuals(table, &schema, &output, &output.operation_id.0, config).await?;
    Ok(output)
}

fn check_cancelled(cancelled: Option<&AtomicBool>) -> Result<()> {
    ensure!(
        cancelled.is_none_or(|flag| !flag.load(Ordering::Relaxed)),
        crate::CatchUpRejected("compaction build cancelled")
    );
    Ok(())
}

/// Sequential files, bounded Arrow batches, and disk-backed delete merge. The
/// scratch store must be exclusive to this invocation and live until the table
/// coordinator durably stages the returned delta. Immutable source files are
/// read through Iceberg FileIO, including its coalesced object-store range reads.
///
/// Supply a dedicated worker runtime: the bounded RocksDB point reads below are
/// blocking. The index may advance while files are read; any observed location
/// mismatch aborts work. Final catalog validation is required even if no mismatch
/// was observed here.
#[allow(clippy::too_many_arguments)]
pub async fn build_data(
    table: &Table,
    schema: TableSchema,
    plan: CompactionPlan,
    operation_id: OperationId,
    live_index: &(impl RowIndex + ?Sized),
    scratch: StateStore,
    mut config: WriterConfig,
    read_limits: &ReadLimits,
    cache: &ManifestCache,
    cancelled: &AtomicBool,
) -> Result<BuiltData> {
    ensure!(
        plan.base_snapshot_id == table.metadata().current_snapshot_id().unwrap_or_default(),
        "worker table must describe the plan's base snapshot"
    );
    ensure!(
        plan.schema_id == table.metadata().current_schema_id()
            && plan.spec_id == table.metadata().default_partition_spec_id(),
        "compaction schema/spec changed"
    );
    ensure!(
        table
            .metadata()
            .default_partition_spec()
            .fields()
            .is_empty(),
        "only unpartitioned compaction is supported"
    );
    ensure!(
        table.metadata().default_sort_order().fields.is_empty(),
        "sorted tables require a sort-preserving compaction worker"
    );
    ensure!(!plan.input_files.is_empty(), "compaction requires inputs");
    check_cancelled(Some(cancelled))?;
    // Planning normally just loaded these manifests through the same cache.
    let view = SnapshotView::load_with_cache(table, plan.base_snapshot_id, cache).await?;
    check_cancelled(Some(cancelled))?;
    let required_deletes: BTreeSet<_> = plan
        .input_files
        .iter()
        .map(|file| view.applicable_deletes(&file.0))
        .collect::<iceberg::Result<Vec<_>>>()?
        .into_iter()
        .flatten()
        .map(|entry| FileId(flow_iceberg_ext::content_file_id(&entry.data_file)))
        .collect();
    ensure!(
        required_deletes == plan.delete_files,
        "plan must consume exactly every applicable delete file"
    );
    let input_data_rows = plan.input_files.iter().try_fold(0u64, |rows, id| {
        let entry = view
            .live_files
            .get(&id.0)
            .ok_or_else(|| anyhow::anyhow!("missing compaction input"))?;
        rows.checked_add(entry.data_file.record_count())
            .ok_or_else(|| anyhow::anyhow!("compaction input row count overflow"))
    })?;
    tracing::info!(
        event = "compaction_input_admitted",
        operation_id = %operation_id.0,
        table_id = schema.table_id.0,
        build_snapshot_id = plan.base_snapshot_id,
        output_level = ?plan.output_level,
        input_data_files = plan.input_files.len(),
        input_data_rows,
        input_data_bytes = plan.input_bytes,
        input_delete_files = plan.delete_files.len(),
        input_delete_rows = plan.delete_input_rows,
        input_delete_bytes = plan.delete_input_bytes,
        "compaction metadata admitted before file reads"
    );
    let batch_rows = scratch.batch_rows().min(config.batch_rows).max(1);
    scratch.begin_prepare(PreparedOperation {
        id: operation_id.clone(),
        table_id: schema.table_id,
        kind: OperationKind::Rewrite,
        base_snapshot_id: None,
        last_lsn: PgLsn(0),
        schema_version: schema.version,
        artifacts: Vec::new(),
        payload: Vec::new(),
    })?;
    let data_files = view
        .live_files
        .values()
        .filter(|entry| entry.content_type() == iceberg::spec::DataContentType::Data)
        .map(|entry| FileId(flow_iceberg_ext::content_file_id(&entry.data_file)))
        .collect();
    let scanned_delete_rows = stage_delete_files_inner(
        table,
        &schema,
        &view,
        &data_files,
        &plan.delete_files,
        &scratch,
        &operation_id.0,
        batch_rows,
        DeleteReadLimits {
            max_bytes: plan.delete_input_bytes,
            max_rows: plan.delete_input_rows,
        },
        read_limits,
        Some(cancelled),
    )
    .await?;
    check_cancelled(Some(cancelled))?;
    let delete_sequence = plan
        .delete_files
        .iter()
        .map(|id| view.live_files[&id.0].sequence_number)
        .try_fold(None, |maximum, sequence| {
            let sequence =
                sequence.ok_or_else(|| anyhow::anyhow!("missing delete data sequence"))?;
            Ok::<_, anyhow::Error>(Some(
                maximum.map_or(sequence, |value: i64| value.max(sequence)),
            ))
        })?;
    config.target_file_bytes = usize::try_from(plan.target_file_bytes)?;
    let mut writer = DataWriter::new(
        table.file_io().clone(),
        table.metadata().location(),
        &operation_id,
        schema.clone(),
        plan.spec_id,
        config,
    )?;
    if table.metadata().format_version() == iceberg::spec::FormatVersion::V3 {
        writer = writer.with_row_lineage()?;
    }
    let mut reverse_file = None;
    type ReverseRows<'a> = Box<
        dyn Iterator<Item = flow_state_store::Result<(u64, flow_model::PrimaryKey)>> + Send + 'a,
    >;
    let mut reverse_rows: Option<ReverseRows<'_>> = None;
    let mut mapped_rows = 0u64;
    // Keyless tables rely on exact reverse scans of every input file.
    let mut lazy = (plan.lazy_index_remap && !schema.primary_key.is_empty())
        .then(std::collections::BTreeMap::<FileId, FileRemap>::new);
    let output_sequence = table
        .metadata()
        .current_snapshot()
        .expect("validated base snapshot")
        .sequence_number();
    let mut stager = Stager::spawn(scratch.clone(), operation_id.clone())?;
    scan_staged_live_columns(
        table,
        &schema,
        &view,
        &plan.input_files,
        &scratch,
        &operation_id.0,
        batch_rows,
        read_limits,
        Some(cancelled),
        async |live| {
            let rows = live.batch.num_rows();
            let mut encoder = RowEncoder::new(&schema, &live.batch)?;
            let keys = if schema.primary_key.is_empty() {
                ensure!(schema.append_only, "mutable compaction requires a key");
                if reverse_file.as_ref() != Some(&live.file_id) {
                    if let Some(rows) = &mut reverse_rows {
                        ensure!(
                            rows.next().transpose()?.is_none(),
                            "unconsumed reverse rows in compaction input"
                        );
                    }
                    reverse_file = Some(live.file_id.clone());
                    reverse_rows = Some(Box::new(
                        live_index.file_rows(&schema.table_id, &live.file_id),
                    ));
                }
                let mut keys = Vec::with_capacity(rows);
                let rows = reverse_rows.as_mut().expect("reverse iterator initialized");
                for position in &live.positions {
                    let Some((indexed_position, key)) = rows.next().transpose()? else {
                        bail!("keyless compaction reverse index is incomplete");
                    };
                    ensure!(
                        *position == indexed_position,
                        "keyless compaction reverse index positions differ"
                    );
                    keys.push(key);
                }
                keys
            } else {
                (0..rows).map(|row| encoder.key(row)).collect()
            };
            // A rewritten row keeps its values, so the original's fingerprint
            // is also its replacement's.
            let fingerprints: Vec<_> = (0..rows).map(|row| encoder.fingerprint(row)).collect();
            let old_locations = live_index.lookup_many(&schema.table_id, &keys)?;
            let mut originals = Vec::with_capacity(rows);
            for ((key, old), (position, fingerprint)) in keys
                .iter()
                .zip(old_locations)
                .zip(live.positions.iter().zip(&fingerprints))
            {
                let Some(old) = old else {
                    bail!("compaction key {key:?} no longer exists; replan");
                };
                ensure!(
                    old.data_file_id == live.file_id
                        && old.row_position == *position
                        && old.row_fingerprint == *fingerprint,
                    "compaction input row changed or index is stale; replan"
                );
                originals.push(old);
            }
            check_cancelled(Some(cancelled))?;
            let written = writer
                .write_arrow(
                    &live.batch,
                    live.lineage.as_deref(),
                    &fingerprints,
                    PgLsn(0),
                )
                .await?;
            check_cancelled(Some(cancelled))?;
            if let Some(remaps) = &mut lazy {
                // Survivors are written in input order, so each input file's
                // rows form a few contiguous runs of the outputs.
                ensure!(
                    written.locations.len() == live.positions.len(),
                    "writer returned a location per row"
                );
                let remap = remaps
                    .entry(live.file_id.clone())
                    .or_insert_with(|| FileRemap::new(live.file_id.clone()));
                for (position, location) in live.positions.iter().zip(&written.locations) {
                    remap.push(*position, location, output_sequence)?;
                }
                mapped_rows += rows as u64;
                return Ok(());
            }
            let deltas = keys.into_iter().zip(originals).zip(written.locations).map(
                |((key, original), mut replacement)| {
                    replacement.data_sequence_number = table
                        .metadata()
                        .current_snapshot()
                        .expect("validated base snapshot")
                        .sequence_number();
                    replacement.source_commit_lsn = original.source_commit_lsn;
                    replacement.row_version = original.row_version;
                    IndexDelta {
                        key,
                        expected: Some(original),
                        replacement: Some(replacement),
                    }
                },
            );
            stager.stage(deltas.collect())?;
            mapped_rows += rows as u64;
            Ok(())
        },
    )
    .await?;
    if let Some(rows) = &mut reverse_rows {
        ensure!(
            rows.next().transpose()?.is_none(),
            "unconsumed reverse rows in compaction input"
        );
    }
    stager.finish()?;
    check_cancelled(Some(cancelled))?;
    let data_files = writer.close().await?;
    scratch.seal_prepare(
        &operation_id,
        data_files
            .iter()
            .map(|file| file.file_path().to_owned())
            .collect(),
        Vec::new(),
    )?;
    tracing::info!(
        event = "compaction_data_built",
        operation_id = %operation_id.0,
        table_id = schema.table_id.0,
        build_snapshot_id = plan.base_snapshot_id,
        mapped_rows,
        scanned_delete_rows,
        output_data_files = data_files.len(),
        output_data_rows = data_files.iter().map(|file| file.record_count()).sum::<u64>(),
        output_data_bytes = data_files.iter().map(|file| file.file_size_in_bytes()).sum::<u64>(),
        "compaction data and mappings built before catalog publication"
    );
    Ok(BuiltData {
        base: view,
        output: WorkerOutput {
            plan,
            data_files,
            delete_files: Vec::new(),
            delete_sequence,
            operation_id,
            scratch,
            masked_inputs: None,
            table_id: schema.table_id,
            lazy_files: lazy.map(|files| files.into_values().collect()),
            lazy: None,
        },
    })
}

/// Stages mapping batches in order on a dedicated thread, so scratch RocksDB
/// writes overlap the next batch's decode, index lookups and Parquet encode.
/// At most `STAGED_BATCHES` batches wait; a full queue blocks the worker as
/// inline staging did. Dropping it waits for queued batches, so no scratch
/// write outlives the build.
struct Stager {
    sender: Option<std::sync::mpsc::SyncSender<Vec<IndexDelta>>>,
    thread: Option<std::thread::JoinHandle<flow_state_store::Result<()>>>,
}

const STAGED_BATCHES: usize = 2;

impl Stager {
    fn spawn(scratch: StateStore, operation: OperationId) -> Result<Self> {
        let (sender, receiver) = std::sync::mpsc::sync_channel::<Vec<IndexDelta>>(STAGED_BATCHES);
        let thread = std::thread::Builder::new()
            .name("flow-compaction-stage".into())
            .spawn(move || {
                for deltas in receiver {
                    scratch.stage_deltas(&operation, deltas)?;
                }
                Ok(())
            })?;
        Ok(Self {
            sender: Some(sender),
            thread: Some(thread),
        })
    }

    fn stage(&mut self, deltas: Vec<IndexDelta>) -> Result<()> {
        let sender = self.sender.as_ref().expect("stager is open");
        if sender.send(deltas).is_err() {
            // The thread stopped on an error; report it rather than the send.
            self.finish()?;
            bail!("compaction staging stopped");
        }
        Ok(())
    }

    /// Wait until every queued batch is staged, returning the first failure.
    fn finish(&mut self) -> Result<()> {
        self.sender.take();
        match self.thread.take() {
            Some(thread) => Ok(thread
                .join()
                .map_err(|_| anyhow::anyhow!("compaction staging panicked"))??),
            None => Ok(()),
        }
    }
}

impl Drop for Stager {
    fn drop(&mut self) {
        let _ = self.finish();
    }
}

/// Shared v2 files retained after upgrade still protect surviving targets.
/// Keeping them avoids publishing an incomplete residual vector for a survivor.
pub(crate) fn retain_shared_legacy_deletes(
    output: &mut WorkerOutput,
    view: &SnapshotView,
) -> Result<()> {
    let mut retained = BTreeSet::new();
    for (id, entry) in &view.live_files {
        if entry.content_type() != iceberg::spec::DataContentType::Data
            || output.plan.input_files.contains(&FileId(id.clone()))
        {
            continue;
        }
        for delete in view.applicable_deletes(id)? {
            if delete.file_format() == iceberg::spec::DataFileFormat::Parquet {
                retained.insert(FileId(flow_iceberg_ext::content_file_id(&delete.data_file)));
            }
        }
    }
    output.plan.delete_files.retain(|id| !retained.contains(id));
    output.delete_sequence = output
        .plan
        .delete_files
        .iter()
        .map(|id| view.live_files[&id.0].sequence_number)
        .collect::<Option<Vec<_>>>()
        .ok_or_else(|| anyhow::anyhow!("missing delete sequence"))?
        .into_iter()
        .max();
    Ok(())
}

/// Write staged positions outside the selected data files for a strict build.
pub(crate) async fn write_residuals(
    table: &Table,
    schema: &TableSchema,
    output: &WorkerOutput,
    scan: &str,
    config: WriterConfig,
) -> Result<Vec<DataFile>> {
    let positions = output
        .scratch
        .position_deletes(scan, &schema.table_id)
        .filter(|position| match position {
            Ok(position) => !output.plan.input_files.contains(&position.data_file_id),
            Err(_) => true,
        });
    write_residual_positions(table, output, positions, config).await
}

/// Consume a sorted, unique union without restaging its already ordered inputs.
pub(crate) async fn write_residual_positions(
    table: &Table,
    output: &WorkerOutput,
    positions: impl Iterator<Item = flow_state_store::Result<RowLocation>>,
    mut config: WriterConfig,
) -> Result<Vec<DataFile>> {
    // A shared residual must not grow into one globally charged dependency.
    // At most eight sorted ranges bound both file fanout and each read charge.
    // Input admission already bounds total rows; row-group memory limits remain
    // in force while explicit rotation replaces the advisory file byte target.
    const MAX_COHORTS: u64 = 8;
    let cohort_rows = output
        .plan
        .delete_input_rows
        .div_ceil(MAX_COHORTS)
        .max(config.row_group_rows as u64)
        .max(1);
    let batch_rows = output.scratch.batch_rows().min(config.batch_rows).max(1);
    config.target_file_bytes = usize::MAX;
    let mut writer = DeleteWriter::new_for_version(
        table.file_io().clone(),
        table.metadata().location(),
        &output.operation_id,
        output.plan.spec_id,
        config,
        table.metadata().format_version(),
    )?;
    let mut batch = Vec::with_capacity(batch_rows);
    let mut total_rows = 0u64;
    let mut current_rows = 0u64;
    for position in positions {
        let position = position?;
        total_rows += 1;
        ensure!(
            total_rows <= output.plan.delete_input_rows,
            "residual positions exceed admitted delete inputs"
        );
        current_rows += 1;
        batch.push(position);
        if batch.len() == batch_rows || current_rows == cohort_rows {
            writer.write(&batch).await?;
            batch.clear();
        }
        if current_rows == cohort_rows {
            writer.finish_file().await?;
            current_rows = 0;
        }
    }
    writer.write(&batch).await?;
    writer.close().await
}

/// Physical ordinals refer to the input file, before filtering position deletes.
#[derive(Debug)]
pub struct LiveBatch {
    pub file_id: FileId,
    pub rows: Vec<Row>,
    pub positions: Vec<u64>,
    pub lineage: Option<Vec<flow_materializer::RowLineage>>,
}

// Charge retained row slots and owned variable-width payloads, plus physical
// positions. This bounds coalescing independently of conservative decoder batches.
fn live_row_bytes(row: &Row) -> u64 {
    let slots = std::mem::size_of::<Row>()
        .saturating_add(std::mem::size_of::<u64>())
        .saturating_add(row.capacity().saturating_mul(std::mem::size_of::<Value>()));
    row.iter().fold(slots as u64, |bytes, value| {
        bytes.saturating_add(match value {
            Value::String(value) => value.capacity() as u64,
            Value::Binary(value) => value.capacity() as u64,
            _ => 0,
        })
    })
}

/// Shared scan path for compaction, external rewrite reconciliation, and full
/// index reconstruction. Applies all applicable v2 position deletes before
/// calling the visitor. The scratch namespace must be empty before first use.
#[allow(clippy::too_many_arguments)]
pub async fn scan_live_files(
    table: &Table,
    schema: &TableSchema,
    view: &SnapshotView,
    files: &BTreeSet<FileId>,
    scratch: &StateStore,
    scan_id: &str,
    batch_rows: usize,
    read_limits: &ReadLimits,
    visit: impl AsyncFnMut(LiveBatch) -> Result<()>,
) -> Result<()> {
    stage_position_deletes(
        table,
        schema,
        view,
        files,
        scratch,
        scan_id,
        batch_rows,
        read_limits,
    )
    .await?;
    scan_staged_live_files(
        table,
        schema,
        view,
        files,
        scratch,
        scan_id,
        batch_rows,
        read_limits,
        None,
        visit,
    )
    .await
}

/// Row view of [`scan_staged_live_columns`], coalescing each file's live rows
/// into batches bounded by rows and owned bytes.
#[allow(clippy::too_many_arguments)]
async fn scan_staged_live_files(
    table: &Table,
    schema: &TableSchema,
    view: &SnapshotView,
    files: &BTreeSet<FileId>,
    scratch: &StateStore,
    scan_id: &str,
    batch_rows: usize,
    read_limits: &ReadLimits,
    cancelled: Option<&AtomicBool>,
    mut visit: impl AsyncFnMut(LiveBatch) -> Result<()>,
) -> Result<()> {
    let v3 = table.metadata().format_version() == iceberg::spec::FormatVersion::V3;
    let mut pending: Option<LiveBatch> = None;
    let mut owned_bytes = 0u64;
    scan_staged_live_columns(
        table,
        schema,
        view,
        files,
        scratch,
        scan_id,
        batch_rows,
        read_limits,
        cancelled,
        async |live| {
            if let Some(batch) = pending.take_if(|batch| batch.file_id != live.file_id) {
                visit(batch).await?;
                owned_bytes = 0;
            }
            let rows = rows_from_batch(schema, &live.batch)?;
            for (index, (row, position)) in rows.into_iter().zip(live.positions).enumerate() {
                let row_bytes = live_row_bytes(&row)
                    + if v3 {
                        std::mem::size_of::<flow_materializer::RowLineage>() as u64
                    } else {
                        0
                    };
                ensure!(
                    row_bytes <= read_limits.batch_bytes,
                    "owned scan row exceeds batch byte limit"
                );
                if let Some(batch) = pending.take_if(|batch| {
                    batch.rows.len() >= batch_rows
                        || row_bytes > read_limits.batch_bytes - owned_bytes
                }) {
                    check_cancelled(cancelled)?;
                    visit(batch).await?;
                    owned_bytes = 0;
                }
                let batch = pending.get_or_insert_with(|| LiveBatch {
                    file_id: live.file_id.clone(),
                    rows: Vec::new(),
                    positions: Vec::new(),
                    lineage: v3.then(Vec::new),
                });
                owned_bytes += row_bytes;
                batch.rows.push(row);
                batch.positions.push(position);
                if let (Some(output), Some(lineage)) = (&mut batch.lineage, &live.lineage) {
                    output.push(lineage[index]);
                }
            }
            Ok(())
        },
    )
    .await?;
    if let Some(batch) = pending {
        check_cancelled(cancelled)?;
        visit(batch).await?;
    }
    Ok(())
}

/// One decoded batch's live rows, in the table's Arrow schema.
struct LiveColumns {
    file_id: FileId,
    batch: RecordBatch,
    /// Physical ordinals in the input file, before filtering position deletes.
    positions: Vec<u64>,
    lineage: Option<Vec<flow_materializer::RowLineage>>,
}

/// Data scan after delete staging. The caller owns the scratch namespace and
/// must stage every applicable delete before entering this loop. Batches stay
/// columnar: deleted rows are filtered out and the rest are not copied.
#[allow(clippy::too_many_arguments)]
async fn scan_staged_live_columns(
    table: &Table,
    schema: &TableSchema,
    view: &SnapshotView,
    files: &BTreeSet<FileId>,
    scratch: &StateStore,
    scan_id: &str,
    batch_rows: usize,
    read_limits: &ReadLimits,
    cancelled: Option<&AtomicBool>,
    mut visit: impl AsyncFnMut(LiveColumns) -> Result<()>,
) -> Result<()> {
    let target = writer_arrow_schema(schema);
    let v3 = table.metadata().format_version() == iceberg::spec::FormatVersion::V3;
    let mut prefetch = Prefetch::new(
        table.file_io(),
        files.iter().map(|file| {
            (
                file.0.clone(),
                view.live_files[&file.0].data_file.file_size_in_bytes(),
            )
        }),
        read_limits,
    );
    for file in files {
        check_cancelled(cancelled)?;
        let source = prefetch.next(&file.0).await?;
        let mut deletes = scratch.file_position_deletes(scan_id, &schema.table_id, file);
        let mut next_delete = deletes
            .next()
            .transpose()?
            .map(|location| location.row_position);
        let mut position = 0u64;
        let entry = &view.live_files[&file.0];
        read_input_batches(
            table.file_io(),
            &file.0,
            source,
            None,
            batch_rows,
            read_limits,
            async |batch| {
                check_cancelled(cancelled)?;
                let mut lineage = if v3 {
                    Some(flow_materializer::read_row_lineage(
                        &batch,
                        position,
                        entry.data_file.first_row_id(),
                        entry
                            .sequence_number
                            .ok_or_else(|| anyhow::anyhow!("missing data sequence"))?,
                    )?)
                } else {
                    None
                };
                let batch = conform(schema, &target, &batch)?;
                let mut live = Vec::with_capacity(batch.num_rows());
                let mut positions = Vec::with_capacity(batch.num_rows());
                for _ in 0..batch.num_rows() {
                    ensure!(
                        next_delete.is_none_or(|deleted| deleted >= position),
                        "position delete fell behind scan"
                    );
                    if next_delete == Some(position) {
                        next_delete = deletes
                            .next()
                            .transpose()?
                            .map(|location| location.row_position);
                        live.push(false);
                    } else {
                        positions.push(position);
                        live.push(true);
                    }
                    position += 1;
                }
                if positions.is_empty() {
                    return Ok(());
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
                check_cancelled(cancelled)?;
                visit(LiveColumns {
                    file_id: file.clone(),
                    batch,
                    positions,
                    lineage,
                })
                .await
            },
        )
        .await?;
        ensure!(
            next_delete.is_none(),
            "position delete is outside input file"
        );
        ensure!(
            position == entry.data_file.record_count(),
            "data file row count differs from metadata"
        );
    }
    Ok(())
}

/// Stage the exact effective position-delete set for current data-file identities,
/// without scanning data rows. Reused by delete-maintenance reconciliation.
#[allow(clippy::too_many_arguments)]
pub async fn stage_position_deletes(
    table: &Table,
    schema: &TableSchema,
    view: &SnapshotView,
    files: &BTreeSet<FileId>,
    scratch: &StateStore,
    scan_id: &str,
    batch_rows: usize,
    read_limits: &ReadLimits,
) -> Result<()> {
    let delete_files: BTreeSet<_> = files
        .iter()
        .map(|file| view.applicable_deletes(&file.0))
        .collect::<iceberg::Result<Vec<_>>>()?
        .into_iter()
        .flatten()
        .map(|entry| FileId(flow_iceberg_ext::content_file_id(&entry.data_file)))
        .collect();
    stage_delete_files(
        table,
        schema,
        view,
        files,
        &delete_files,
        scratch,
        scan_id,
        batch_rows,
        DeleteReadLimits {
            max_bytes: u64::MAX,
            max_rows: u64::MAX,
        },
        read_limits,
    )
    .await?;
    Ok(())
}

/// Physical scan budgets, checked against object sizes and decoded row counts.
#[derive(Clone, Copy)]
pub struct DeleteReadLimits {
    pub max_bytes: u64,
    pub max_rows: u64,
}

/// Stage selected delete files into a disk-backed sorted set. Applicability is
/// evaluated at each original delete sequence before records are merged; raising
/// all input sequences before filtering could incorrectly delete newer data.
#[allow(clippy::too_many_arguments)]
pub async fn stage_delete_files(
    table: &Table,
    schema: &TableSchema,
    view: &SnapshotView,
    files: &BTreeSet<FileId>,
    delete_files: &BTreeSet<FileId>,
    scratch: &StateStore,
    scan_id: &str,
    batch_rows: usize,
    limits: DeleteReadLimits,
    read_limits: &ReadLimits,
) -> Result<u64> {
    stage_delete_files_inner(
        table,
        schema,
        view,
        files,
        delete_files,
        scratch,
        scan_id,
        batch_rows,
        limits,
        read_limits,
        None,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn stage_delete_files_inner(
    table: &Table,
    schema: &TableSchema,
    view: &SnapshotView,
    files: &BTreeSet<FileId>,
    delete_files: &BTreeSet<FileId>,
    scratch: &StateStore,
    scan_id: &str,
    batch_rows: usize,
    limits: DeleteReadLimits,
    read_limits: &ReadLimits,
    cancelled: Option<&AtomicBool>,
) -> Result<u64> {
    check_cancelled(cancelled)?;
    ensure!(
        batch_rows > 0 && batch_rows <= scratch.batch_rows(),
        "scan batch exceeds scratch budget"
    );
    ensure!(
        matches!(
            table.metadata().format_version(),
            iceberg::spec::FormatVersion::V2 | iceberg::spec::FormatVersion::V3
        ),
        "live-row scanner requires Iceberg v2 or v3"
    );
    ensure!(
        table
            .metadata()
            .table_properties()?
            .encryption_key_id
            .is_none(),
        "encrypted compaction/rebuild is not supported"
    );
    ensure!(
        view.live_files.values().all(|entry| matches!(
            entry.content_type(),
            iceberg::spec::DataContentType::Data | iceberg::spec::DataContentType::PositionDeletes
        )),
        "live-row scanner refuses unsupported equality deletes"
    );
    ensure!(
        files.iter().all(|id| view
            .live_files
            .get(&id.0)
            .is_some_and(|entry| entry.content_type() == iceberg::spec::DataContentType::Data)),
        "delete scan requires live data-file identities"
    );
    // A reused scratch directory after failure must not retain old delete state.
    scratch.discard_transaction(scan_id)?;
    ensure!(
        delete_files
            .iter()
            .all(|id| view.live_files.get(&id.0).is_some_and(
                |entry| entry.content_type() == iceberg::spec::DataContentType::PositionDeletes
            )),
        "delete scan requires live position-delete files"
    );
    load_deletes(
        table.file_io(),
        view,
        files,
        delete_files,
        table.metadata().default_partition_spec_id(),
        scratch,
        scan_id,
        schema,
        batch_rows,
        limits,
        read_limits,
        cancelled,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn load_deletes(
    file_io: &FileIO,
    view: &SnapshotView,
    input_files: &BTreeSet<FileId>,
    delete_files: &BTreeSet<FileId>,
    spec_id: i32,
    scratch: &StateStore,
    scan_id: &str,
    schema: &TableSchema,
    batch_rows: usize,
    limits: DeleteReadLimits,
    read_limits: &ReadLimits,
    cancelled: Option<&AtomicBool>,
) -> Result<u64> {
    let vector_targets = view
        .live_files
        .values()
        .filter(|entry| entry.file_format() == iceberg::spec::DataFileFormat::Puffin)
        .filter_map(|entry| entry.data_file.referenced_data_file())
        .collect::<BTreeSet<_>>();
    // Charge every declared input before reading any object.
    let mut bytes = 0u64;
    let mut declared = 0u64;
    let mut vectors = Vec::new();
    let mut vector_files = Vec::new();
    let mut parquet_files = Vec::new();
    for file in delete_files {
        let delete_entry = &view.live_files[&file.0];
        bytes = bytes
            .checked_add(flow_iceberg_ext::delete_content_size(
                &delete_entry.data_file,
            ))
            .ok_or_else(|| anyhow::anyhow!("delete input size overflow"))?;
        ensure!(
            bytes <= limits.max_bytes,
            "delete scan exceeds input byte budget"
        );
        let declared_rows = delete_entry.data_file.record_count();
        ensure!(
            declared_rows <= limits.max_rows.saturating_sub(declared),
            "delete scan exceeds input row budget"
        );
        if delete_entry.file_format() == iceberg::spec::DataFileFormat::Puffin {
            let read = flow_materializer::VectorRead::new(
                &delete_entry.data_file,
                iceberg::puffin::DeletionVectorLimits {
                    max_blob_bytes: read_limits
                        .row_group_uncompressed_bytes
                        .min(limits.max_bytes),
                    max_footer_bytes: read_limits.footer_bytes,
                    max_cardinality: limits.max_rows.saturating_sub(declared),
                },
            )?;
            let data = view
                .live_files
                .get(&read.referenced_data_file)
                .ok_or_else(|| anyhow::anyhow!("DV target is not live"))?;
            let target = FileId(read.referenced_data_file.clone());
            let applies = input_files.contains(&target)
                && flow_iceberg_ext::position_delete_may_apply(delete_entry, data);
            vectors.push(read);
            vector_files.push((target, data, applies));
        } else {
            parquet_files.push((file, delete_entry));
        }
        declared += declared_rows;
    }
    check_cancelled(cancelled)?;
    // Vectors share Puffin objects. Each object's size and footer are read
    // once per scan; every descriptor is still validated against both.
    let mut rows = 0u64;
    let concurrency = read_limits.prefetch_files.max(1);
    flow_materializer::read_vectors(
        file_io,
        &vectors,
        flow_materializer::VectorReadLimits {
            max_footer_bytes: read_limits.footer_bytes,
            concurrency,
            max_batch_bytes: read_limits
                .prefetch_file_bytes
                .saturating_mul(concurrency as u64),
        },
        |index, vector| {
            check_cancelled(cancelled)?;
            let (target, data, applies) = &vector_files[index];
            rows = rows
                .checked_add(vector.len())
                .ok_or_else(|| anyhow::anyhow!("delete row count overflow"))?;
            ensure!(
                vector
                    .iter()
                    .last()
                    .is_none_or(|position| position < data.data_file.record_count()),
                "delete position exceeds data row count"
            );
            if !applies {
                return Ok(());
            }
            let sequence = data
                .sequence_number
                .ok_or_else(|| anyhow::anyhow!("missing data sequence"))?;
            let mut positions = vector.iter();
            loop {
                let mut batch = positions.by_ref().take(batch_rows).peekable();
                if batch.peek().is_none() {
                    return Ok(());
                }
                scratch.put_position_deletes(
                    scan_id,
                    &schema.table_id,
                    batch.map(|position| RowLocation {
                        data_file_id: target.clone(),
                        row_position: position,
                        data_sequence_number: sequence,
                        spec_id,
                        partition: Vec::new(),
                        source_commit_lsn: PgLsn(0),
                        row_version: 0,
                        row_fingerprint: [0; 16],
                    }),
                )?;
            }
        },
    )
    .await?;
    let input_paths: HashSet<&str> = input_files.iter().map(|file| file.0.as_str()).collect();
    let mut prefetch = Prefetch::new(
        file_io,
        parquet_files
            .iter()
            .map(|(file, entry)| (file.0.clone(), entry.data_file.file_size_in_bytes())),
        read_limits,
    );
    for (file, delete_entry) in parquet_files {
        check_cancelled(cancelled)?;
        // Observe a lazy input's size once; the reader below reuses it.
        let (source, size) = match prefetch.next(&file.0).await? {
            Input::Fetched(bytes) => {
                let size = bytes.len() as u64;
                (Input::Fetched(bytes), size)
            }
            _ => {
                let input = file_io.new_input(delete_entry.file_path())?;
                let size = input.metadata().await?.size;
                (Input::Sized(size), size)
            }
        };
        check_cancelled(cancelled)?;
        ensure!(
            size == delete_entry.data_file.file_size_in_bytes(),
            "delete file size differs from manifest"
        );
        let declared_rows = delete_entry.data_file.record_count();
        let mut file_rows = 0u64;
        read_input_batches(
            file_io,
            delete_entry.file_path(),
            source,
            Some(&[2147483546, 2147483545]),
            batch_rows,
            read_limits,
            async |batch| {
                check_cancelled(cancelled)?;
                let field_index = |id: &str| -> Result<usize> {
                    batch
                        .schema()
                        .fields()
                        .iter()
                        .position(|field| {
                            field
                                .metadata()
                                .get(PARQUET_FIELD_ID_META_KEY)
                                .is_some_and(|value| value == id)
                        })
                        .ok_or_else(|| anyhow::anyhow!("delete file missing field ID {id}"))
                };
                let path_column = field_index("2147483546")?;
                let pos_column = field_index("2147483545")?;
                file_rows += batch.num_rows() as u64;
                rows = rows
                    .checked_add(batch.num_rows() as u64)
                    .ok_or_else(|| anyhow::anyhow!("delete row count overflow"))?;
                ensure!(
                    rows <= limits.max_rows,
                    "delete scan exceeds input row budget"
                );
                let paths = batch
                    .column(path_column)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| anyhow::anyhow!("delete path must be UTF-8"))?;
                let positions = batch
                    .column(pos_column)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| anyhow::anyhow!("delete position must be long"))?;
                let mut locations = Vec::new();
                for idx in 0..batch.num_rows() {
                    ensure!(
                        !paths.is_null(idx) && !positions.is_null(idx) && positions.value(idx) >= 0,
                        "invalid null or negative position delete"
                    );
                    let path = paths.value(idx);
                    if !input_paths.contains(path) || vector_targets.contains(path) {
                        continue;
                    }
                    let data = &view.live_files[path];
                    if !flow_iceberg_ext::position_delete_may_apply(delete_entry, data) {
                        continue;
                    }
                    ensure!(
                        (positions.value(idx) as u64) < data.data_file.record_count(),
                        "delete position exceeds data row count"
                    );
                    locations.push(RowLocation {
                        data_file_id: FileId(path.to_owned()),
                        row_position: positions.value(idx) as u64,
                        data_sequence_number: data
                            .sequence_number
                            .ok_or_else(|| anyhow::anyhow!("missing data sequence"))?,
                        spec_id,
                        partition: Vec::new(),
                        source_commit_lsn: PgLsn(0),
                        row_version: 0,
                        row_fingerprint: [0; 16],
                    });
                }
                scratch.put_position_deletes(scan_id, &schema.table_id, locations)?;
                Ok(())
            },
        )
        .await?;
        ensure!(
            file_rows == declared_rows,
            "delete file row count differs from manifest"
        );
    }
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use flow_model::PrimaryKey;
    use flow_state_store::StateStoreOptions;

    fn delta(key: u8, position: u64) -> IndexDelta {
        IndexDelta {
            key: PrimaryKey(vec![1, key]),
            expected: None,
            replacement: Some(RowLocation {
                data_file_id: FileId("out.parquet".into()),
                row_position: position,
                data_sequence_number: 1,
                spec_id: 0,
                partition: Vec::new(),
                source_commit_lsn: PgLsn(0),
                row_version: 0,
                row_fingerprint: [0; 16],
            }),
        }
    }

    fn building(name: &str) -> (tempfile::TempDir, StateStore, OperationId) {
        let directory = tempfile::tempdir().unwrap();
        let store = StateStore::open(directory.path(), StateStoreOptions::default()).unwrap();
        let operation = OperationId(name.into());
        store
            .begin_prepare(PreparedOperation {
                id: operation.clone(),
                table_id: TableId(1),
                kind: OperationKind::Rewrite,
                base_snapshot_id: None,
                last_lsn: PgLsn(0),
                schema_version: 0,
                artifacts: Vec::new(),
                payload: Vec::new(),
            })
            .unwrap();
        (directory, store, operation)
    }

    #[test]
    fn stager_keeps_batch_order() {
        let (_directory, store, operation) = building("ordered");
        let mut stager = Stager::spawn(store.clone(), operation.clone()).unwrap();
        for batch in 0..5u8 {
            let deltas = (0..3).map(|row| delta(batch * 3 + row, u64::from(batch * 3 + row)));
            stager.stage(deltas.collect()).unwrap();
        }
        stager.finish().unwrap();
        store
            .seal_prepare(&operation, Vec::new(), Vec::new())
            .unwrap();
        let staged: Vec<_> = store
            .prepared_deltas(&operation)
            .unwrap()
            .map(|delta| delta.unwrap().key.0[1])
            .collect();
        assert_eq!(staged, (0..15).collect::<Vec<u8>>());
    }

    #[test]
    fn stager_reports_the_staging_error() {
        let (_directory, store, operation) = building("duplicate");
        let mut stager = Stager::spawn(store, operation).unwrap();
        stager.stage(vec![delta(1, 0)]).unwrap();
        // The duplicate key fails on the staging thread; the worker sees it at
        // a later send or when it waits for the queue to drain.
        let mut result = stager.stage(vec![delta(1, 1)]);
        for position in 2..10 {
            if result.is_err() {
                break;
            }
            result = stager.stage(vec![delta(100 + position as u8, position)]);
        }
        let error = result.and_then(|()| stager.finish()).unwrap_err();
        assert!(
            error.to_string().contains("duplicate primary key"),
            "{error:#}"
        );
    }
}
