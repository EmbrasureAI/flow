use super::{RewriteRecovery, TableMaintenance, artifacts::WriterRegistration};
use crate::publication::{ReplanRequired, blocking};
use anyhow::{Result, ensure};
use flow_compactor::{DeleteReadLimits, Level, PublicationPressure, stage_delete_files};
use flow_iceberg_ext::{CommitBase, SnapshotView, position_delete_may_apply, write_artifact_plan};
use flow_materializer::{DeleteWriter, iceberg_schema};
use flow_model::{FileId, OperationId, TableSchema};
use flow_state_store::{OperationKind, PreparedOperation, StateStore};
use iceberg::{spec::DataContentType, table::Table};
use std::collections::{BTreeSet, HashMap};

/// Limits one position-delete consolidation. Sorting and deduplication spill to
/// the worker's scratch index; neither data files nor the live row index are read.
#[derive(Debug, Clone)]
pub struct DeleteRewritePolicy {
    pub min_input_files: usize,
    pub max_input_files: usize,
    pub max_input_bytes: u64,
    pub max_input_rows: u64,
    pub target_file_bytes: usize,
}

impl Default for DeleteRewritePolicy {
    fn default() -> Self {
        Self {
            min_input_files: 5,
            max_input_files: 32,
            max_input_bytes: 32 << 20,
            max_input_rows: 1_000_000,
            target_file_bytes: 16 << 20,
        }
    }
}

/// Remembers nonviable inputs at one immutable catalog head. Keep this cursor
/// across maintenance retries; a refreshed head automatically clears it.
#[derive(Default)]
pub struct DeleteRepairCursor {
    head: Option<(uuid::Uuid, Option<i64>)>,
    attempted: BTreeSet<FileId>,
}

struct RepairTarget {
    id: FileId,
    remaining_rows: u64,
    remaining_files: usize,
}

struct RepairInput {
    id: FileId,
    targets: Vec<RepairTarget>,
}

impl TableMaintenance {
    /// Consolidate a bounded group of small position-delete files at the exact
    /// indexed snapshot, preserving row locations and the source watermark.
    pub async fn compact_deletes(
        &self,
        table: &Table,
        schema: &TableSchema,
        scratch: StateStore,
        policy: &DeleteRewritePolicy,
    ) -> Result<Option<i64>> {
        self.rewrite_deletes(table, schema, scratch, policy, None)
            .await
    }

    /// Localize one shared dependency into at most three sorted path ranges.
    /// Commit only when a previously blocked dense or old L0 input becomes
    /// readable within every existing compaction limit. No row locations change.
    /// Each call reads at most one input within the configured delete row/byte
    /// limits and writes at most three files. A nonviable pass advances `cursor`;
    /// exhausted inputs are not read again until the head changes. I/O failures
    /// remain retryable. Effective target counts come from the same staged input.
    pub async fn repair_delete_dependencies(
        &self,
        table: &Table,
        schema: &TableSchema,
        scratch: StateStore,
        cursor: &mut DeleteRepairCursor,
    ) -> Result<Option<i64>> {
        let policy = DeleteRewritePolicy {
            min_input_files: 1,
            max_input_files: self.policy.max_delete_input_files,
            max_input_bytes: self.policy.max_delete_input_bytes,
            max_input_rows: self.policy.max_delete_input_rows,
            target_file_bytes: usize::MAX,
        };
        self.rewrite_deletes(table, schema, scratch, &policy, Some(cursor))
            .await
    }

    async fn rewrite_deletes(
        &self,
        table: &Table,
        schema: &TableSchema,
        scratch: StateStore,
        policy: &DeleteRewritePolicy,
        mut cursor: Option<&mut DeleteRepairCursor>,
    ) -> Result<Option<i64>> {
        let repair = cursor.is_some();
        ensure!(
            policy.min_input_files >= 1
                && policy.min_input_files <= policy.max_input_files
                && policy.max_input_bytes > 0
                && policy.max_input_rows > 0
                && policy.target_file_bytes > 0,
            "invalid delete rewrite limits"
        );
        let table = self.catalog.load_table(table.identifier()).await?;
        let base = CommitBase::new(&table);
        base.validate(&table)?;
        if let Some(cursor) = cursor.as_deref_mut() {
            let head = (table.metadata().uuid(), base.snapshot_id);
            if cursor.head != Some(head) {
                cursor.head = Some(head);
                cursor.attempted.clear();
            }
        }
        ensure!(
            crate::same_iceberg_schema(table.metadata().current_schema(), &iceberg_schema(schema)?),
            "source schema differs from Iceberg"
        );
        let table_id = schema.table_id;
        let store = self.store.clone();
        let indexed = blocking(move || Ok(store.table_state(&table_id)?)).await?;
        ensure!(
            indexed.pending_operation.is_none(),
            "recover pending operation before rewriting deletes"
        );
        if indexed.snapshot_id != base.snapshot_id {
            return Err(ReplanRequired.into());
        }
        let view = SnapshotView::current_with_cache(&table, &self.cache).await?;
        let repair_input = if let Some(cursor) = &cursor {
            let inventory = self.inventory(&table, table_id).await?;
            match self.policy.plan(
                base.snapshot_id.ok_or(ReplanRequired)?,
                table.metadata().current_schema_id(),
                &inventory.files,
                &inventory.deletes,
            ) {
                Err(flow_compactor::Error::DependencyBudget) => {}
                Err(error) => return Err(error.into()),
                Ok(_) => return Ok(None),
            }
            choose_repair_input(&inventory, &self.policy, cursor)
        } else {
            None
        };
        if repair && repair_input.is_none() {
            return Ok(None);
        }
        let mut candidates = view
            .live_files
            .values()
            .filter(|entry| entry.content_type() == DataContentType::PositionDeletes)
            .filter(|entry| {
                repair_input
                    .as_ref()
                    .is_none_or(|input| entry.file_path() == input.id.0)
            })
            .collect::<Vec<_>>();
        candidates.sort_by_key(|entry| (entry.data_file.file_size_in_bytes(), entry.file_path()));
        let mut input_bytes = 0u64;
        let mut input_rows = 0u64;
        let mut sequence = 0;
        let mut selected = BTreeSet::new();
        for entry in candidates {
            let bytes = input_bytes.saturating_add(entry.data_file.file_size_in_bytes());
            let rows = input_rows.saturating_add(entry.data_file.record_count());
            if bytes > policy.max_input_bytes || rows > policy.max_input_rows {
                continue;
            }
            sequence = sequence.max(
                entry
                    .sequence_number
                    .ok_or_else(|| anyhow::anyhow!("missing delete data sequence"))?,
            );
            input_bytes = bytes;
            input_rows = rows;
            selected.insert(FileId(entry.file_path().to_owned()));
            if selected.len() == policy.max_input_files {
                break;
            }
        }
        if selected.len() < policy.min_input_files {
            return Ok(None);
        }
        let input_files = selected.len();
        let removed_deletes: BTreeSet<String> = selected.iter().map(|id| id.0.clone()).collect();
        let data_files = view
            .live_files
            .values()
            .filter(|entry| entry.content_type() == DataContentType::Data)
            .map(|entry| FileId(entry.file_path().to_owned()))
            .collect();
        let id = OperationId(format!("delete-rewrite-{}", uuid::Uuid::new_v4()));
        let ownership =
            WriterRegistration::new(self.store.clone(), &table, table_id, id.clone(), &id)?;
        let reservation = ownership.record().await?;
        let operation = PreparedOperation {
            id: id.clone(),
            table_id,
            kind: OperationKind::Rewrite,
            base_snapshot_id: indexed.snapshot_id,
            last_lsn: indexed.materialized_lsn,
            schema_version: indexed.schema_version,
            artifacts: Vec::new(),
            payload: Vec::new(),
        };
        let store = self.store.clone();
        blocking(move || {
            Ok(store
                .begin_prepare_with_record(operation, Some((&reservation.0, &reservation.1)))?)
        })
        .await?;
        let worker_table = table.clone();
        let worker_schema = schema.clone();
        let worker_id = id.clone();
        let limits = DeleteReadLimits {
            max_bytes: policy.max_input_bytes,
            max_rows: policy.max_input_rows,
        };
        let mut writer_config = self.writer.clone();
        writer_config.target_file_bytes = policy.target_file_bytes;
        writer_config.artifact_tracker = Some(ownership.clone());
        let read_limits = self.read_limits.clone();
        let worker_view = view.clone();
        let attempted_input = repair_input.as_ref().map(|input| input.id.clone());
        let max_input_files = policy.max_input_files;
        let started = std::time::Instant::now();
        let (output, scanned_rows, isolation) = tokio::task::spawn_blocking(move || {
            super::worker_runtime()?.block_on(async move {
                let batch_rows = scratch.batch_rows().min(8192);
                let scanned_rows = stage_delete_files(
                    &worker_table,
                    &worker_schema,
                    &worker_view,
                    &data_files,
                    &selected,
                    &scratch,
                    &worker_id.0,
                    batch_rows,
                    limits,
                    &read_limits,
                )
                .await?;
                let isolation = if let Some(input) = repair_input {
                    let mut counts = input
                        .targets
                        .iter()
                        .map(|target| (target.id.clone(), 0u64))
                        .collect::<HashMap<_, _>>();
                    for position in scratch.position_deletes(&worker_id.0, &table_id) {
                        if let Some(count) = counts.get_mut(&position?.data_file_id) {
                            *count += 1;
                        }
                    }
                    let Some(target) = input.targets.into_iter().find(|target| {
                        let count = counts[&target.id];
                        target.remaining_rows.saturating_add(count) <= limits.max_rows
                            && target.remaining_files + usize::from(count != 0) <= max_input_files
                    }) else {
                        return Ok((None, scanned_rows, None));
                    };
                    Some((target.id, input.id))
                } else {
                    None
                };
                let mut positions = scratch.position_deletes(&worker_id.0, &table_id);
                let first = positions.next().transpose()?;
                if !repair && input_files == 1 && first.is_some() {
                    return Ok((None, scanned_rows, None));
                }
                let mut writer = DeleteWriter::new(
                    worker_table.file_io().clone(),
                    worker_table.metadata().location(),
                    &worker_id,
                    worker_table.metadata().default_partition_spec_id(),
                    writer_config,
                )?;
                let mut batch = Vec::with_capacity(batch_rows);
                let mut range = None;
                for position in first.map(Ok).into_iter().chain(positions) {
                    let position = position?;
                    if let Some((target, _)) = &isolation {
                        let next_range = position.data_file_id.cmp(target);
                        if range.is_some_and(|previous| previous != next_range) {
                            writer.write(&batch).await?;
                            batch.clear();
                            writer.finish_file().await?;
                        }
                        range = Some(next_range);
                    }
                    batch.push(position);
                    if batch.len() == batch_rows {
                        writer.write(&batch).await?;
                        batch.clear();
                    }
                }
                writer.write(&batch).await?;
                Ok::<_, anyhow::Error>((Some(writer.close().await?), scanned_rows, isolation))
            })
        })
        .await??;
        let viable = |files: &Vec<iceberg::spec::DataFile>| {
            if repair && isolation.is_none() {
                return false;
            }
            let Some((target, input)) = &isolation else {
                return files.len() < input_files
                    && preserves_delete_locality(
                        &view,
                        &removed_deletes,
                        files,
                        sequence,
                        &self.policy,
                    );
            };
            let data = &view.live_files[&target.0];
            let original = &view.live_files[&input.0];
            let mut count = 0usize;
            let mut bytes = 0u64;
            let mut rows = 0u64;
            for entry in view
                .live_files
                .values()
                .filter(|entry| entry.file_path() != input.0)
            {
                if position_delete_may_apply(entry, data) {
                    count += 1;
                    bytes = bytes.saturating_add(entry.data_file.file_size_in_bytes());
                    rows = rows.saturating_add(entry.data_file.record_count());
                }
            }
            for file in files {
                let mut entry = original.as_ref().clone();
                entry.data_file = file.clone();
                if position_delete_may_apply(&entry, data) {
                    count += 1;
                    bytes = bytes.saturating_add(file.file_size_in_bytes());
                    rows = rows.saturating_add(file.record_count());
                }
            }
            // Before was over budget; requiring every charge to fit proves
            // bounded progress, even if compression or path bounds are unusual.
            files.len() <= 3
                && count <= policy.max_input_files
                && bytes <= policy.max_input_bytes
                && rows <= policy.max_input_rows
        };
        let Some(output) = output.filter(viable) else {
            // Target sizing cannot reduce this group. Do not publish an idle
            // replacement; ownership records retain the unused output for GC.
            let store = self.store.clone();
            blocking(move || Ok(store.discard_uncommitted(&id)?)).await?;
            if let (Some(cursor), Some(input)) = (cursor, attempted_input) {
                cursor.attempted.insert(input);
            }
            return Ok(None);
        };
        let path = format!(
            "{}/metadata/{}-prepared.avro",
            table.metadata().location().trim_end_matches('/'),
            id.0
        );
        write_artifact_plan(&table, &path, &[], &output).await?;
        let payload = serde_json::to_vec(&RewriteRecovery {
            output_data_sequence: None,
            base,
            manifest_list: path.clone(),
            removed_data: BTreeSet::new(),
            removed_deletes,
            delete_sequence: Some(sequence),
            properties: HashMap::from([
                ("streaming.writer-id".into(), "embrasure-flow".into()),
                (
                    "streaming.operation".into(),
                    if repair {
                        "repair-delete-dependencies"
                    } else {
                        "compact-deletes"
                    }
                    .into(),
                ),
                (
                    "streaming.last-lsn".into(),
                    indexed.materialized_lsn.to_string(),
                ),
            ]),
        })?;
        let artifacts = output
            .iter()
            .map(|file| file.file_path().to_owned())
            .chain(std::iter::once(path))
            .collect();
        let reservation = ownership.finish(0, output.len()).await?;
        let store = self.store.clone();
        let operation_id = id.clone();
        blocking(move || {
            Ok(store.seal_prepare_with_record(
                &operation_id,
                artifacts,
                payload,
                Some((&reservation.0, &reservation.1)),
            )?)
        })
        .await?;
        let snapshot = self.recover(&table, &id).await?;
        let store = self.store.clone();
        let operation_id = id.clone();
        blocking(move || Ok(store.forget_applied(&operation_id)?)).await?;
        tracing::info!(
            event = "delete_rewrite_completed",
            table_id = table_id.0,
            operation_id = %id.0,
            snapshot_id = snapshot,
            repair,
            input_files,
            input_bytes,
            input_rows,
            scanned_rows,
            output_files = output.len(),
            output_rows = output.iter().map(|file| file.record_count()).sum::<u64>(),
            output_bytes = output
                .iter()
                .map(|file| file.file_size_in_bytes())
                .sum::<u64>(),
            elapsed_ms = started.elapsed().as_secs_f64() * 1000.0,
            "position-delete files consolidated"
        );
        Ok(snapshot)
    }
}

/// A smaller delete-file count can still widen path bounds enough to make an
/// independently readable data file exceed the worker's dependency budget.
/// Keep every currently budget-admissible data file readable by evaluating the
/// output metadata at its inherited sequence, including after a restart.
fn preserves_delete_locality(
    view: &SnapshotView,
    removed: &BTreeSet<String>,
    output: &[iceberg::spec::DataFile],
    sequence: i64,
    policy: &flow_compactor::Policy,
) -> bool {
    if output.is_empty() {
        return true;
    }
    let Some(template) = removed.first().and_then(|path| view.live_files.get(path)) else {
        return false;
    };
    let output = output
        .iter()
        .map(|file| iceberg::spec::ManifestEntry {
            data_file: file.clone(),
            sequence_number: Some(sequence),
            ..template.as_ref().clone()
        })
        .collect::<Vec<_>>();
    let deletes = view
        .live_files
        .values()
        .filter(|entry| entry.content_type() == DataContentType::PositionDeletes)
        .map(AsRef::as_ref)
        .collect::<Vec<_>>();
    view.live_files
        .values()
        .filter(|entry| entry.content_type() == DataContentType::Data)
        .all(|data| {
            !within_delete_budget(deletes.iter().copied(), data, policy)
                || within_delete_budget(
                    deletes
                        .iter()
                        .copied()
                        .filter(|entry| !removed.contains(entry.file_path()))
                        .chain(&output),
                    data,
                    policy,
                )
        })
}

fn within_delete_budget<'a>(
    mut entries: impl Iterator<Item = &'a iceberg::spec::ManifestEntry>,
    data: &iceberg::spec::ManifestEntry,
    policy: &flow_compactor::Policy,
) -> bool {
    entries
        .try_fold((0usize, 0u64, 0u64), |(count, bytes, rows), entry| {
            if !position_delete_may_apply(entry, data) {
                return Some((count, bytes, rows));
            }
            let count = count.checked_add(1)?;
            let bytes = bytes.checked_add(entry.data_file.file_size_in_bytes())?;
            let rows = rows.checked_add(entry.data_file.record_count())?;
            (count <= policy.max_delete_input_files
                && bytes <= policy.max_delete_input_bytes
                && rows <= policy.max_delete_input_rows)
                .then_some((count, bytes, rows))
        })
        .is_some()
}

fn choose_repair_input(
    inventory: &super::Inventory,
    policy: &flow_compactor::Policy,
    cursor: &DeleteRepairCursor,
) -> Option<RepairInput> {
    let mut targets = inventory
        .files
        .iter()
        .filter_map(|file| {
            if file.size_bytes > policy.max_input_bytes
                || (file.age_ms < policy.min_file_age_ms
                    && inventory.debt.pressure != PublicationPressure::Pause)
                || !(policy.needs_delete_reclamation(file)
                    || (file.level == Level::L0 && file.age_ms >= policy.oldest_l0_soft_ms))
            {
                return None;
            }
            let (rows, bytes, count) = inventory
                .deletes
                .iter()
                .filter(|delete| delete.targets.contains(&file.id))
                .fold((0u64, 0u64, 0usize), |(rows, bytes, count), delete| {
                    (
                        rows.saturating_add(delete.row_count),
                        bytes.saturating_add(delete.size_bytes),
                        count + 1,
                    )
                });
            (rows > policy.max_delete_input_rows
                || bytes > policy.max_delete_input_bytes
                || count > policy.max_delete_input_files)
                .then_some((file, rows, bytes, count))
        })
        .collect::<Vec<_>>();
    targets.sort_by_key(|(file, ..)| (std::cmp::Reverse(file.deleted_rows), &file.id));
    // Prefer a metadata-certified row bound, but don't treat an inconclusive
    // bound as proof of failure. The staged input provides exact target counts.
    let (_, input) = inventory
        .deletes
        .iter()
        .filter(|input| {
            input.targets.len() > 1
                && input.row_count <= policy.max_delete_input_rows
                && input.size_bytes <= policy.max_delete_input_bytes
                && !cursor.attempted.contains(&input.id)
        })
        .filter_map(|input| {
            let priority = targets
                .iter()
                .filter(|(file, rows, bytes, count)| {
                    input.targets.contains(&file.id)
                        && rows.saturating_sub(input.row_count) <= policy.max_delete_input_rows
                        && bytes.saturating_sub(input.size_bytes) <= policy.max_delete_input_bytes
                        && count - 1 <= policy.max_delete_input_files
                })
                .map(|(file, rows, _, count)| {
                    let certified = rows
                        .saturating_sub(input.row_count)
                        .saturating_add(input.row_count.min(file.deleted_rows))
                        <= policy.max_delete_input_rows
                        && count - 1 + usize::from(file.deleted_rows != 0)
                            <= policy.max_delete_input_files;
                    (
                        !certified,
                        std::cmp::Reverse(file.deleted_rows),
                        std::cmp::Reverse(input.row_count),
                        input.id.clone(),
                    )
                })
                .min()?;
            Some((priority, input))
        })
        .min_by_key(|(priority, _)| priority.clone())?;
    Some(RepairInput {
        id: input.id.clone(),
        targets: targets
            .into_iter()
            .filter(|(file, _, bytes, _)| {
                input.targets.contains(&file.id)
                    && bytes.saturating_sub(input.size_bytes) <= policy.max_delete_input_bytes
            })
            .map(|(file, rows, _, count)| RepairTarget {
                id: file.id.clone(),
                remaining_rows: rows.saturating_sub(input.row_count),
                remaining_files: count - 1,
            })
            .collect(),
    })
}
