use crate::{CompactionPlan, ReadLimits, parquet_scan::read_batches};
use anyhow::{Result, bail, ensure};
use arrow_array::{Array, Int64Array, StringArray};
use flow_iceberg_ext::SnapshotView;
use flow_materializer::{DataWriter, DeleteWriter, WriterConfig, rows_from_batch};
use flow_model::{FileId, OperationId, PgLsn, Row, RowLocation, TableId, TableSchema};
use flow_state_store::{IndexDelta, OperationKind, PreparedOperation, RowIndex, StateStore};
use iceberg::{io::FileIO, spec::DataFile, table::Table};
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use std::collections::BTreeSet;
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
}
impl WorkerOutput {
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
        &AtomicBool::new(false),
    )
    .await?;
    let mut output = built.output;
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
    let view = SnapshotView::load(table, plan.base_snapshot_id).await?;
    check_cancelled(Some(cancelled))?;
    let required_deletes: BTreeSet<_> = plan
        .input_files
        .iter()
        .map(|file| view.applicable_deletes(&file.0))
        .collect::<iceberg::Result<Vec<_>>>()?
        .into_iter()
        .flatten()
        .map(|entry| FileId(entry.file_path().to_owned()))
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
    let batch_rows = scratch.batch_rows().min(config.row_group_rows).max(1);
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
        .map(|entry| FileId(entry.file_path().to_owned()))
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
    let mut reverse_file = None;
    type ReverseRows<'a> = Box<
        dyn Iterator<Item = flow_state_store::Result<(u64, flow_model::PrimaryKey)>> + Send + 'a,
    >;
    let mut reverse_rows: Option<ReverseRows<'_>> = None;
    let mut mapped_rows = 0u64;
    scan_staged_live_files(
        table,
        &schema,
        &view,
        &plan.input_files,
        &scratch,
        &operation_id.0,
        batch_rows,
        read_limits,
        Some(cancelled),
        async |batch| {
            let keys = if schema.primary_key.is_empty() {
                ensure!(schema.append_only, "mutable compaction requires a key");
                if reverse_file.as_ref() != Some(&batch.file_id) {
                    if let Some(rows) = &mut reverse_rows {
                        ensure!(
                            rows.next().transpose()?.is_none(),
                            "unconsumed reverse rows in compaction input"
                        );
                    }
                    reverse_file = Some(batch.file_id.clone());
                    reverse_rows = Some(Box::new(
                        live_index.file_rows(&schema.table_id, &batch.file_id),
                    ));
                }
                let mut keys = Vec::with_capacity(batch.positions.len());
                let rows = reverse_rows.as_mut().expect("reverse iterator initialized");
                for position in &batch.positions {
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
                batch
                    .rows
                    .iter()
                    .map(|row| schema.encode_key(row))
                    .collect::<std::result::Result<Vec<_>, _>>()?
            };
            let old_locations = live_index.lookup_many(&schema.table_id, &keys)?;
            let mut originals = Vec::with_capacity(batch.rows.len());
            for ((key, old), (row, position)) in keys
                .iter()
                .zip(old_locations)
                .zip(batch.rows.iter().zip(batch.positions))
            {
                let Some(old) = old else {
                    bail!("compaction key {key:?} no longer exists; replan");
                };
                ensure!(
                    old.data_file_id == batch.file_id
                        && old.row_position == position
                        && old.row_fingerprint == schema.fingerprint(row)?,
                    "compaction input row changed or index is stale; replan"
                );
                originals.push(old);
            }
            check_cancelled(Some(cancelled))?;
            let written = writer.write(&batch.rows, PgLsn(0)).await?;
            check_cancelled(Some(cancelled))?;
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
            scratch.stage_deltas(&operation_id, deltas)?;
            mapped_rows += batch.rows.len() as u64;
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
        },
    })
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
    let batch_rows = output
        .scratch
        .batch_rows()
        .min(config.row_group_rows)
        .max(1);
    config.target_file_bytes = usize::MAX;
    let mut writer = DeleteWriter::new(
        table.file_io().clone(),
        table.metadata().location(),
        &output.operation_id,
        output.plan.spec_id,
        config,
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

/// Data scan after delete staging. The caller owns the scratch namespace and
/// must stage every applicable delete before entering this loop.
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
    for file in files {
        check_cancelled(cancelled)?;
        let mut deletes = scratch.file_position_deletes(scan_id, &schema.table_id, file);
        let mut next_delete = deletes
            .next()
            .transpose()?
            .map(|location| location.row_position);
        let mut position = 0u64;
        read_batches(
            table.file_io(),
            &file.0,
            None,
            batch_rows,
            read_limits,
            async |batch| {
                check_cancelled(cancelled)?;
                let all_rows = rows_from_batch(schema, &batch)?;
                let mut rows = Vec::with_capacity(all_rows.len());
                let mut positions = Vec::with_capacity(all_rows.len());
                for row in all_rows {
                    ensure!(
                        next_delete.is_none_or(|deleted| deleted >= position),
                        "position delete fell behind scan"
                    );
                    if next_delete == Some(position) {
                        next_delete = deletes
                            .next()
                            .transpose()?
                            .map(|location| location.row_position);
                    } else {
                        rows.push(row);
                        positions.push(position);
                    }
                    position += 1;
                }
                if !rows.is_empty() {
                    visit(LiveBatch {
                        file_id: file.clone(),
                        rows,
                        positions,
                    })
                    .await?;
                }
                Ok(())
            },
        )
        .await?;
        ensure!(
            next_delete.is_none(),
            "position delete is outside input file"
        );
        ensure!(
            position == view.live_files[&file.0].data_file.record_count(),
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
        .map(|entry| FileId(entry.file_path().to_owned()))
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
        table.metadata().format_version() == iceberg::spec::FormatVersion::V2,
        "live-row scanner supports only Iceberg v2"
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
        "live-row scanner refuses unsupported equality deletes or deletion vectors"
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
    let mut bytes = 0u64;
    let mut rows = 0u64;
    for file in delete_files {
        check_cancelled(cancelled)?;
        let delete_entry = &view.live_files[&file.0];
        let input = file_io.new_input(&file.0)?;
        let metadata = input.metadata().await?;
        check_cancelled(cancelled)?;
        ensure!(
            metadata.size == delete_entry.data_file.file_size_in_bytes(),
            "delete file size differs from manifest"
        );
        bytes = bytes
            .checked_add(metadata.size)
            .ok_or_else(|| anyhow::anyhow!("delete input size overflow"))?;
        ensure!(
            bytes <= limits.max_bytes,
            "delete scan exceeds input byte budget"
        );
        let declared_rows = delete_entry.data_file.record_count();
        ensure!(
            declared_rows <= limits.max_rows.saturating_sub(rows),
            "delete scan exceeds input row budget"
        );
        let mut file_rows = 0u64;
        read_batches(
            file_io,
            &file.0,
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
                    let id = FileId(paths.value(idx).to_owned());
                    if !input_files.contains(&id) {
                        continue;
                    }
                    let data = &view.live_files[&id.0];
                    if !flow_iceberg_ext::position_delete_may_apply(delete_entry, data) {
                        continue;
                    }
                    ensure!(
                        (positions.value(idx) as u64) < data.data_file.record_count(),
                        "delete position exceeds data row count"
                    );
                    locations.push(RowLocation {
                        data_file_id: id,
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
