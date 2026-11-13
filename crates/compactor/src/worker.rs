use crate::{
    CompactionPlan, ReadLimits,
    columnar::RowEncoder,
    live::{LiveColumns, LiveInput},
    merge::{Merge, MergeBatch, SortKeys},
    parquet_scan::{Input, Prefetch, read_input_batches},
    residual::{ResidualInput, ResidualMerge},
};
use anyhow::{Result, bail, ensure};
use arrow_array::{Array, Int64Array, StringArray};
use flow_iceberg_ext::{ManifestCache, SnapshotView};
use flow_materializer::{
    DataWriter, DeleteWriter, SortMode, SortSpec, TableSort, WriterConfig, rows_from_batch,
};
use flow_model::{FileId, OperationId, PgLsn, Row, RowLocation, TableId, TableSchema, Value};
use flow_state_store::{
    FileRemap, IndexDelta, LazyRemap, OperationKind, PreparedOperation, RowIndex, StateStore,
};
use futures::{StreamExt, TryStreamExt};
use iceberg::{io::FileIO, spec::DataFile, table::Table};
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};

/// Artifacts and durable row mappings only. There is deliberately no catalog
/// handle here: the worker cannot commit its output.
pub struct WorkerOutput {
    pub plan: CompactionPlan,
    pub data_files: Vec<DataFile>,
    pub delete_files: Vec<DataFile>,
    pub delete_sequence: Option<i64>,
    /// Positions in `delete_files` that target unselected data files, as
    /// opposed to masks on this rewrite's output.
    pub residual_delete_rows: u64,
    pub operation_id: OperationId,
    pub scratch: StateStore,
    pub(crate) masked_inputs: Option<String>,
    pub(crate) table_id: TableId,
    /// Per-input remaps of a lazy build, before catch-up.
    pub(crate) lazy_files: Option<Vec<FileRemap>>,
    /// The complete lazy remap, after catch-up derived exact output counts.
    pub(crate) lazy: Option<LazyRemap>,
    pub(crate) layout: RewriteLayout,
}

/// How a rewrite ordered its output rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RewriteLayout {
    /// Inputs concatenated in commit order: the default for unsorted tables
    /// and for inputs that cannot be merged.
    CommitOrder,
    /// Sorted inputs with disjoint key ranges, concatenated in key order.
    KeyOrder,
    /// Sorted inputs merged row by row into one sorted stream.
    Merge,
}

impl RewriteLayout {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::CommitOrder => "commit_order",
            Self::KeyOrder => "key_order",
            Self::Merge => "merge",
        }
    }
}

impl WorkerOutput {
    /// How this rewrite ordered its output rows.
    pub fn layout(&self) -> RewriteLayout {
        self.layout
    }

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
        // Mappings follow the worker's commit-order scan, so the masked
        // positions are read per input file as its mappings begin.
        let mut masked = None;
        let mut masked_file = None::<FileId>;
        let mut deltas = self.scratch.prepared_deltas(&self.operation_id)?;
        Ok(std::iter::from_fn(move || {
            loop {
                let delta = match deltas.next()? {
                    Ok(delta) => delta,
                    Err(error) => return Some(Err(error)),
                };
                let Some(scan) = &self.masked_inputs else {
                    return Some(Ok(delta));
                };
                let Some(expected) = delta.expected.as_ref() else {
                    return Some(Err(flow_state_store::Error::InvalidState(
                        "worker mapping lacks original".into(),
                    )));
                };
                if masked_file.as_ref() != Some(&expected.data_file_id) {
                    masked = Some(
                        self.scratch
                            .file_position_deletes(scan, &self.table_id, &expected.data_file_id)
                            .peekable(),
                    );
                    masked_file = Some(expected.data_file_id.clone());
                }
                let masked = masked.as_mut().expect("opened for this mapping's file");
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
    if let Some(files) = output.lazy_files.take() {
        // The table stays fenced from the build base through publication, so
        // there is no late change to catch up: every output row is live.
        output.lazy = Some(strict_lazy_remap(&output, files)?);
    }
    if table.metadata().format_version() == iceberg::spec::FormatVersion::V3 {
        retain_shared_legacy_deletes(&mut output, &built.base)?;
        return Ok(output);
    }
    (output.delete_files, output.residual_delete_rows) =
        write_residuals(table, &schema, &output, &output.operation_id.0, config).await?;
    Ok(output)
}

/// The lazy remap of a strict build: the worker's per-input runs at the base,
/// with every output row live. Apply still requires the retired inputs'
/// indexed counts at the fenced head to equal the outputs' rows.
fn strict_lazy_remap(output: &WorkerOutput, files: Vec<FileRemap>) -> Result<LazyRemap> {
    ensure!(
        files
            .iter()
            .all(|remap| output.plan.input_files.contains(&remap.source)),
        "lazy remap names a file outside the plan"
    );
    let output_live_rows: Vec<_> = output
        .data_files
        .iter()
        .map(|file| (FileId(file.file_path().to_owned()), file.record_count()))
        .collect();
    ensure!(
        files.iter().map(FileRemap::survivors).sum::<u64>()
            == output_live_rows.iter().map(|(_, rows)| rows).sum::<u64>(),
        "lazy remap and output files disagree on the moved rows"
    );
    Ok(LazyRemap {
        files,
        output_live_rows,
    })
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
    // Sorted inputs with disjoint key ranges are concatenated in key order:
    // one input open at a time. Other sorted inputs are merged when the plan
    // allows it, the remap is lazy and the inputs fit the merge's memory.
    let sort = TableSort::of_table(&schema, table.metadata());
    let lazy_remap = plan.lazy_index_remap && !schema.primary_key.is_empty();
    let (scan_order, mut layout) = match key_order(&view, &plan.input_files, &schema, sort.spec())?
    {
        Some(order) => (order, RewriteLayout::KeyOrder),
        None => (
            commit_order(&view, &plan.input_files)?,
            RewriteLayout::CommitOrder,
        ),
    };
    let merge_spec = sort.spec().filter(|spec| {
        layout == RewriteLayout::CommitOrder
            && plan.merge.enabled
            && lazy_remap
            && plan.input_files.len() > 1
            && stamped(&view, &plan.input_files, spec)
    });
    if merge_spec.is_some() {
        layout = RewriteLayout::Merge;
    }
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
        selection_reason = plan.reason.as_str(),
        input_data_files = plan.input_files.len(),
        input_data_rows,
        input_data_bytes = plan.input_bytes,
        input_delete_files = plan.delete_files.len(),
        input_delete_rows = plan.delete_input_rows,
        input_delete_bytes = plan.delete_input_bytes,
        layout = layout.as_str(),
        sort_order_id = sort.order_id(),
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
    // A merge opens every input before reading data, so its memory is known
    // from their footers and prefetched files; over budget, it concatenates.
    let mut merge_inputs = None;
    let mut merge_held_bytes = 0u64;
    let mut merge_declined = None::<&str>;
    if layout == RewriteLayout::Merge {
        let (inputs, held) = open_inputs(
            table,
            &schema,
            &view,
            &scan_order,
            &scratch,
            &operation_id.0,
            batch_rows,
            read_limits,
            plan.merge.memory_bytes,
            cancelled,
        )
        .await?;
        merge_held_bytes = held;
        if inputs.is_none() {
            layout = RewriteLayout::CommitOrder;
            merge_declined = Some("memory");
        }
        merge_inputs = inputs;
    }
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
    if let Some(spec) = sort.spec() {
        // A merge's output is sorted by construction: declare it, and fail
        // rather than write a row out of order.
        let mode = if merge_inputs.is_some() {
            SortMode::Sorted
        } else {
            SortMode::Verify
        };
        writer = writer.with_sort(spec, mode)?;
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
    let mut merge_stats = None;
    if let Some(mut inputs) = merge_inputs.take() {
        let spec = merge_spec.expect("merge layout has a sort order");
        let files: Vec<FileId> = inputs.iter().map(|input| input.file_id().clone()).collect();
        let mut merge = Merge::new(SortKeys::new(&schema, spec)?, files.clone());
        let mut remaps: Vec<Option<FileRemap>> = files.iter().map(|_| None).collect();
        loop {
            while let Some(input) = merge.pending() {
                check_cancelled(Some(cancelled))?;
                match inputs[input].next().await? {
                    Some(live) => {
                        let VerifiedInput { fingerprints, .. } =
                            verify_inputs(&schema, live_index, &live, None)?;
                        merge.push(input, MergeBatch { live, fingerprints })?;
                    }
                    None => merge.finish(input),
                }
            }
            let Some(merged) = merge.next(batch_rows)? else {
                break;
            };
            check_cancelled(Some(cancelled))?;
            let written = writer
                .write_arrow(
                    &merged.batch,
                    merged.lineage.as_deref(),
                    &merged.fingerprints,
                    PgLsn(0),
                )
                .await?;
            ensure!(
                written.locations.len() == merged.sources.len(),
                "writer returned a location per row"
            );
            // Each input's rows keep their relative order, so every remap
            // receives ascending old positions; output positions interleave.
            for ((input, position), location) in merged.sources.iter().zip(&written.locations) {
                remaps[*input]
                    .get_or_insert_with(|| FileRemap::new(files[*input].clone()))
                    .push(*position, location, output_sequence)?;
            }
            mapped_rows += merged.sources.len() as u64;
        }
        let remap_files = lazy.as_mut().expect("a merge needs a lazy remap");
        for remap in remaps.into_iter().flatten() {
            remap_files.insert(remap.source.clone(), remap);
        }
        merge_stats = Some(merge.stats());
    } else {
        scan_staged_live_columns(
            table,
            &schema,
            &view,
            &scan_order,
            &scratch,
            &operation_id.0,
            batch_rows,
            read_limits,
            Some(cancelled),
            async |live| {
                let rows = live.batch.num_rows();
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
                    Some(keys)
                } else {
                    None
                };
                let VerifiedInput {
                    keys,
                    fingerprints,
                    originals,
                } = verify_inputs(&schema, live_index, &live, keys)?;
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
    }
    // Inputs borrow the scratch store, which the output takes.
    drop(merge_inputs);
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
        layout = layout.as_str(),
        merge_declined,
        merge_held_bytes,
        merge_runs = merge_stats.map_or(0, |stats| stats.runs),
        merge_gathered_batches = merge_stats.map_or(0, |stats| stats.gathered_batches),
        merge_sliced_batches = merge_stats.map_or(0, |stats| stats.sliced_batches),
        remap_runs = lazy
            .as_ref()
            .map_or(0, |files| files.values().map(FileRemap::runs).sum::<u64>()),
        remap_bytes = lazy
            .as_ref()
            .map_or(0, |files| files.values().map(FileRemap::encoded_bytes).sum::<u64>()),
        sorted_output_files = data_files
            .iter()
            .filter(|file| file.sort_order_id().is_some_and(|id| i64::from(id) == sort.order_id()))
            .count(),
        "compaction data and mappings built before catalog publication"
    );
    Ok(BuiltData {
        base: view,
        output: WorkerOutput {
            plan,
            data_files,
            delete_files: Vec::new(),
            delete_sequence,
            residual_delete_rows: 0,
            operation_id,
            scratch,
            masked_inputs: None,
            table_id: schema.table_id,
            lazy_files: lazy.map(|files| files.into_values().collect()),
            lazy: None,
            layout,
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
) -> Result<(Vec<DataFile>, u64)> {
    let positions = ResidualMerge::new(vec![ResidualInput::new(
        output
            .scratch
            .position_delete_cursor(scan, &schema.table_id),
        |file| !output.plan.input_files.contains(file),
    )]);
    write_residual_positions(table, output, positions, config).await
}

/// Consume a sorted, unique union without restaging its already ordered inputs.
/// Also returns how many positions target unselected data files rather than
/// this rewrite's output.
pub(crate) async fn write_residual_positions(
    table: &Table,
    output: &WorkerOutput,
    mut positions: ResidualMerge<'_>,
    mut config: WriterConfig,
) -> Result<(Vec<DataFile>, u64)> {
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
    // Per-target residuals rotate at each full-size target instead, so no
    // full-size data file gains a shared dependency; positions on L0 and small
    // files, this rewrite's small outputs included, share ranges. Format v3
    // vectors are per target already.
    let per_target = output.plan.per_target_deletes
        && table.metadata().format_version() < iceberg::spec::FormatVersion::V3;
    let mut ranges = output.plan.target_ranges.clone();
    for file in &output.data_files {
        ranges.add(
            FileId(file.file_path().to_owned()),
            file.file_size_in_bytes(),
        );
    }
    let outputs = output
        .data_files
        .iter()
        .map(DataFile::file_path)
        .collect::<HashSet<_>>();
    // Batch slots keep their allocations; the first `filled` are pending.
    let mut batch = Vec::with_capacity(batch_rows);
    let mut filled = 0;
    let mut total_rows = 0u64;
    let mut current_rows = 0u64;
    let mut residual_rows = 0u64;
    let mut residual_target = false;
    let mut target = None::<FileId>;
    loop {
        if filled == batch.len() {
            batch.push(crate::residual::empty_location());
        }
        let Some(starts) = positions.next_into(&mut batch[filled])? else {
            break;
        };
        let position = &batch[filled];
        total_rows += 1;
        ensure!(
            total_rows <= output.plan.delete_input_rows,
            "residual positions exceed admitted delete inputs"
        );
        if starts {
            residual_target = !outputs.contains(position.data_file_id.0.as_str());
        }
        residual_rows += u64::from(residual_target);
        if per_target {
            if starts {
                if let Some(previous) = &target
                    && !ranges.continues(previous, &position.data_file_id)
                {
                    writer.write(&batch[..filled]).await?;
                    batch.swap(0, filled);
                    filled = 0;
                    writer.finish_target().await?;
                }
                target = Some(batch[filled].data_file_id.clone());
            }
            filled += 1;
            if filled == batch_rows {
                writer.write(&batch[..filled]).await?;
                filled = 0;
            }
            continue;
        }
        current_rows += 1;
        filled += 1;
        if filled == batch_rows || current_rows == cohort_rows {
            writer.write(&batch[..filled]).await?;
            filled = 0;
        }
        if current_rows == cohort_rows {
            writer.finish_file().await?;
            current_rows = 0;
        }
    }
    writer.write(&batch[..filled]).await?;
    Ok((writer.close().await?, residual_rows))
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
        &files.iter().collect::<Vec<_>>(),
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

/// Data rewrite inputs in commit order: data sequence number, then path. Files
/// written by one operation share a sequence and their paths number them in
/// write order. Paths alone start with a random operation ID, so path order
/// would interleave epochs at random and spread every output row group over
/// the key and time ranges of all its inputs. Commit order keeps an
/// append-mostly table's rows in arrival order through every rewrite. Scans
/// that write no data, such as reconciliation and index rebuild, keep path order.
fn commit_order<'a>(view: &SnapshotView, files: &'a BTreeSet<FileId>) -> Result<Vec<&'a FileId>> {
    let mut ordered = files
        .iter()
        .map(|file| {
            let entry = view
                .live_files
                .get(&file.0)
                .ok_or_else(|| anyhow::anyhow!("scan input {} is not live", file.0))?;
            Ok((entry.sequence_number, file))
        })
        .collect::<Result<Vec<_>>>()?;
    ordered.sort();
    Ok(ordered.into_iter().map(|(_, file)| file).collect())
}

/// Inputs in key order, when every input is stamped with the current sort
/// order and their leading-column ranges are disjoint, so concatenating them
/// keeps the output sorted. The writer still verifies the result.
fn key_order<'a>(
    view: &SnapshotView,
    files: &'a BTreeSet<FileId>,
    schema: &TableSchema,
    spec: Option<&SortSpec>,
) -> Result<Option<Vec<&'a FileId>>> {
    let Some(spec) = spec else {
        return Ok(None);
    };
    let Some(leading) = spec.keys.first() else {
        return Ok(None);
    };
    let order_id = spec.iceberg_order_id()?;
    let field = schema.columns[leading.column].field_id;
    let mut ranges = Vec::with_capacity(files.len());
    for file in files {
        let entry = view
            .live_files
            .get(&file.0)
            .ok_or_else(|| anyhow::anyhow!("scan input {} is not live", file.0))?;
        let data = &entry.data_file;
        let (Some(lower), Some(upper)) = (
            data.lower_bounds().get(&field),
            data.upper_bounds().get(&field),
        ) else {
            return Ok(None);
        };
        if data.sort_order_id() != Some(order_id)
            || data
                .null_value_counts()
                .get(&field)
                .is_some_and(|nulls| *nulls > 0)
        {
            return Ok(None);
        }
        ranges.push((lower.clone(), upper.clone(), file));
    }
    let ascending = !leading.descending;
    ranges.sort_by(|left, right| {
        let ordering = if ascending {
            left.0.partial_cmp(&right.0)
        } else {
            right.1.partial_cmp(&left.1)
        };
        ordering.unwrap_or(std::cmp::Ordering::Equal)
    });
    let disjoint = ranges.windows(2).all(|pair| {
        if ascending {
            pair[0].1 < pair[1].0
        } else {
            pair[0].0 > pair[1].1
        }
    });
    Ok(disjoint.then(|| ranges.into_iter().map(|(_, _, file)| file).collect()))
}

/// Whether every input is stamped with `spec`'s order, which its writer set
/// only after checking every row. A merge still checks each row's order.
fn stamped(view: &SnapshotView, files: &BTreeSet<FileId>, spec: &SortSpec) -> bool {
    let Ok(order_id) = spec.iceberg_order_id() else {
        return false;
    };
    files.iter().all(|file| {
        view.live_files
            .get(&file.0)
            .is_some_and(|entry| entry.data_file.sort_order_id() == Some(order_id))
    })
}

/// Opens every input of a merge in `files` order, or returns `None` as soon
/// as their held bytes exceed `budget`, before reading any more of them.
#[allow(clippy::too_many_arguments)]
async fn open_inputs<'a>(
    table: &Table,
    schema: &'a TableSchema,
    view: &SnapshotView,
    files: &[&FileId],
    scratch: &'a StateStore,
    scan_id: &str,
    batch_rows: usize,
    read_limits: &ReadLimits,
    budget: u64,
    cancelled: &AtomicBool,
) -> Result<(Option<Vec<LiveInput<'a>>>, u64)> {
    let mut inputs = Vec::with_capacity(files.len());
    let mut held = 0u64;
    for &file in files {
        check_cancelled(Some(cancelled))?;
        let source = Input::Sized(view.live_files[&file.0].data_file.file_size_in_bytes());
        let input = LiveInput::open(
            table,
            schema,
            view,
            scratch,
            scan_id,
            file,
            source,
            batch_rows,
            read_limits,
        )
        .await?;
        held = held.saturating_add(input.held_bytes()?);
        if held > budget {
            return Ok((None, held));
        }
        inputs.push(input);
    }
    futures::stream::iter(inputs.iter_mut().map(|input| async move {
        check_cancelled(Some(cancelled))?;
        input.prefetch().await
    }))
    .buffer_unordered(read_limits.prefetch_files.max(1))
    .try_collect::<Vec<_>>()
    .await?;
    Ok((Some(inputs), held))
}

struct VerifiedInput {
    keys: Vec<flow_model::PrimaryKey>,
    fingerprints: Vec<[u8; 16]>,
    originals: Vec<RowLocation>,
}

/// Encodes a batch's keys, unless given, and fingerprints, and checks every
/// row against the index: a row that moved or changed since planning means
/// the input is stale and the build must be replanned.
fn verify_inputs(
    schema: &TableSchema,
    live_index: &(impl RowIndex + ?Sized),
    live: &LiveColumns,
    keys: Option<Vec<flow_model::PrimaryKey>>,
) -> Result<VerifiedInput> {
    let rows = live.batch.num_rows();
    let mut encoder = RowEncoder::new(schema, &live.batch)?;
    let keys = match keys {
        Some(keys) => keys,
        None => (0..rows).map(|row| encoder.key(row)).collect(),
    };
    // A rewritten row keeps its values, so the original's fingerprint is
    // also its replacement's.
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
    Ok(VerifiedInput {
        keys,
        fingerprints,
        originals,
    })
}

/// Data scan after delete staging. The caller owns the scratch namespace and
/// must stage every applicable delete before entering this loop. Batches stay
/// columnar: deleted rows are filtered out and the rest are not copied.
#[allow(clippy::too_many_arguments)]
async fn scan_staged_live_columns(
    table: &Table,
    schema: &TableSchema,
    view: &SnapshotView,
    files: &[&FileId],
    scratch: &StateStore,
    scan_id: &str,
    batch_rows: usize,
    read_limits: &ReadLimits,
    cancelled: Option<&AtomicBool>,
    mut visit: impl AsyncFnMut(LiveColumns) -> Result<()>,
) -> Result<()> {
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
    for &file in files {
        check_cancelled(cancelled)?;
        let source = prefetch.next(&file.0).await?;
        let mut input = LiveInput::open(
            table,
            schema,
            view,
            scratch,
            scan_id,
            file,
            source,
            batch_rows,
            read_limits,
        )
        .await?;
        while let Some(live) = input.next().await? {
            check_cancelled(cancelled)?;
            visit(live).await?;
        }
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
    check_delete_scan(table, view, files)?;
    // A reused scratch directory after failure must not retain old delete state.
    scratch.discard_transaction(scan_id)?;
    check_delete_inputs(view, delete_files)?;
    let targets = DeleteTargets::new(view, files)?;
    let spec_id = table.metadata().default_partition_spec_id();
    load_deletes(
        table.file_io(),
        view,
        &targets,
        delete_files,
        batch_rows,
        limits,
        read_limits,
        cancelled,
        |positions| {
            let locations = positions
                .iter()
                .map(|&(rank, position)| {
                    Ok(RowLocation {
                        data_file_id: targets.ids[rank as usize].clone(),
                        row_position: position,
                        data_sequence_number: targets.entries[rank as usize]
                            .sequence_number
                            .ok_or_else(|| anyhow::anyhow!("missing data sequence"))?,
                        spec_id,
                        partition: Vec::new(),
                        source_commit_lsn: PgLsn(0),
                        row_version: 0,
                        row_fingerprint: [0; 16],
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(scratch.put_position_deletes(scan_id, &schema.table_id, locations)?)
        },
    )
    .await
}

/// The effective positions of a set of delete files, sorted by data-file path
/// and row position, without duplicates: the set [`stage_delete_files`] stages.
pub struct DeletePositions {
    targets: Vec<FileId>,
    /// Indexes into `targets`, which is in path order, and row positions.
    positions: Vec<(u32, u64)>,
}

impl DeletePositions {
    pub fn len(&self) -> usize {
        self.positions.len()
    }

    pub fn is_empty(&self) -> bool {
        self.positions.is_empty()
    }

    /// Each position's data file and row, in path and row order.
    pub fn iter(&self) -> impl ExactSizeIterator<Item = (&FileId, u64)> + '_ {
        self.positions
            .iter()
            .map(|&(rank, position)| (&self.targets[rank as usize], position))
    }

    /// `targets` in path order and sorted, unique `(rank, row)` pairs.
    #[cfg(test)]
    pub(crate) fn from_sorted(targets: Vec<FileId>, positions: Vec<(u32, u64)>) -> Self {
        assert!(targets.is_sorted() && positions.is_sorted());
        Self { targets, positions }
    }

    /// The `(target rank, row)` pairs, sorted; ranks follow path order.
    pub(crate) fn pairs(&self) -> &[(u32, u64)] {
        &self.positions
    }

    pub(crate) fn target(&self, rank: u32) -> &FileId {
        &self.targets[rank as usize]
    }

    /// Each target with positions and its pairs, in path order.
    pub(crate) fn runs(&self) -> impl Iterator<Item = (&FileId, &[(u32, u64)])> + '_ {
        self.positions
            .chunk_by(|left, right| left.0 == right.0)
            .map(|run| (&self.targets[run[0].0 as usize], run))
    }
}

/// [`stage_delete_files`] without a scratch store: the same checks, limits and
/// errors, returning the effective positions and the rows scanned. Positions
/// are merged in memory at 16 bytes each, so `limits.max_rows` also bounds
/// memory; inputs need not be sorted. A consolidation then writes them
/// without a scratch-store round trip.
#[allow(clippy::too_many_arguments)]
pub async fn read_delete_files(
    table: &Table,
    view: &SnapshotView,
    files: &BTreeSet<FileId>,
    delete_files: &BTreeSet<FileId>,
    batch_rows: usize,
    limits: DeleteReadLimits,
    read_limits: &ReadLimits,
) -> Result<(DeletePositions, u64)> {
    ensure!(batch_rows > 0, "scan batch must be positive");
    check_delete_scan(table, view, files)?;
    check_delete_inputs(view, delete_files)?;
    let targets = DeleteTargets::new(view, files)?;
    let mut positions = Vec::new();
    let rows = load_deletes(
        table.file_io(),
        view,
        &targets,
        delete_files,
        batch_rows,
        limits,
        read_limits,
        None,
        |batch| {
            positions.extend_from_slice(batch);
            Ok(())
        },
    )
    .await?;
    // Each input is usually one sorted run, which the stable sort merges.
    positions.sort();
    positions.dedup();
    Ok((
        DeletePositions {
            targets: files.iter().cloned().collect(),
            positions,
        },
        rows,
    ))
}

fn check_delete_scan(table: &Table, view: &SnapshotView, files: &BTreeSet<FileId>) -> Result<()> {
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
    Ok(())
}

fn check_delete_inputs(view: &SnapshotView, delete_files: &BTreeSet<FileId>) -> Result<()> {
    ensure!(
        delete_files
            .iter()
            .all(|id| view.live_files.get(&id.0).is_some_and(
                |entry| entry.content_type() == iceberg::spec::DataContentType::PositionDeletes
            )),
        "delete scan requires live position-delete files"
    );
    Ok(())
}

/// The live data files a delete scan keeps positions for. Their indexes
/// follow path order, so `(index, position)` sorts like `(path, position)`.
struct DeleteTargets<'a> {
    ids: Vec<&'a FileId>,
    entries: Vec<&'a iceberg::spec::ManifestEntry>,
    indexes: HashMap<&'a str, u32>,
}

impl<'a> DeleteTargets<'a> {
    /// `files` must be live data files.
    fn new(view: &'a SnapshotView, files: &'a BTreeSet<FileId>) -> Result<Self> {
        ensure!(
            u32::try_from(files.len()).is_ok(),
            "delete scan has too many data files"
        );
        Ok(Self {
            ids: files.iter().collect(),
            entries: files
                .iter()
                .map(|file| view.live_files[&file.0].as_ref())
                .collect(),
            indexes: files
                .iter()
                .enumerate()
                .map(|(index, file)| (file.0.as_str(), index as u32))
                .collect(),
        })
    }
}

/// Read the effective positions of `delete_files` on `targets`, passing each
/// bounded batch to `sink` as `(target index, row position)` in read order.
#[allow(clippy::too_many_arguments)]
async fn load_deletes(
    file_io: &FileIO,
    view: &SnapshotView,
    targets: &DeleteTargets<'_>,
    delete_files: &BTreeSet<FileId>,
    batch_rows: usize,
    limits: DeleteReadLimits,
    read_limits: &ReadLimits,
    cancelled: Option<&AtomicBool>,
    mut sink: impl FnMut(&[(u32, u64)]) -> Result<()>,
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
            let target = targets
                .indexes
                .get(read.referenced_data_file.as_str())
                .copied()
                .filter(|_| flow_iceberg_ext::position_delete_may_apply(delete_entry, data));
            vectors.push(read);
            vector_files.push((target, data));
        } else {
            parquet_files.push((file, delete_entry));
        }
        declared += declared_rows;
    }
    check_cancelled(cancelled)?;
    // Vectors share Puffin objects. Each object's size and footer are read
    // once per scan; every descriptor is still validated against both.
    let mut rows = 0u64;
    let mut accepted = Vec::with_capacity(batch_rows);
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
            let (target, data) = &vector_files[index];
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
            let Some(target) = *target else {
                return Ok(());
            };
            data.sequence_number
                .ok_or_else(|| anyhow::anyhow!("missing data sequence"))?;
            let mut positions = vector.iter();
            loop {
                accepted.clear();
                accepted.extend(
                    positions
                        .by_ref()
                        .take(batch_rows)
                        .map(|position| (target, position)),
                );
                if accepted.is_empty() {
                    return Ok(());
                }
                sink(&accepted)?;
            }
        },
    )
    .await?;
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
        // Sorted inputs repeat each path for a run of positions; resolve the
        // target and its applicability once per run, without allocating.
        let mut run_path = String::new();
        let mut run_target = None::<Option<u32>>;
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
                accepted.clear();
                for idx in 0..batch.num_rows() {
                    ensure!(
                        !paths.is_null(idx) && !positions.is_null(idx) && positions.value(idx) >= 0,
                        "invalid null or negative position delete"
                    );
                    let path = paths.value(idx);
                    if run_target.is_none() || run_path != path {
                        run_path.clear();
                        run_path.push_str(path);
                        run_target = Some(targets.indexes.get(path).copied().filter(|&target| {
                            !vector_targets.contains(path)
                                && flow_iceberg_ext::position_delete_may_apply(
                                    delete_entry,
                                    targets.entries[target as usize],
                                )
                        }));
                    }
                    let Some(Some(target)) = run_target else {
                        continue;
                    };
                    let data = targets.entries[target as usize];
                    ensure!(
                        (positions.value(idx) as u64) < data.data_file.record_count(),
                        "delete position exceeds data row count"
                    );
                    data.sequence_number
                        .ok_or_else(|| anyhow::anyhow!("missing data sequence"))?;
                    accepted.push((target, positions.value(idx) as u64));
                }
                if !accepted.is_empty() {
                    sink(&accepted)?;
                }
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
