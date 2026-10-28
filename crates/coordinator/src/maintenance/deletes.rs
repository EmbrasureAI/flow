use super::{RewriteRecovery, TableMaintenance};
use crate::artifacts::WriterRegistration;
use crate::{blocking, publication::ReplanRequired};
use anyhow::{Result, ensure};
use flow_compactor::{
    DeleteLayout, DeleteReadLimits, Level, MergeBudget, PublicationPressure, TargetDeleteFile,
    TargetDeletes, stage_delete_files,
};
use flow_iceberg_ext::{CommitBase, SnapshotView, position_delete_may_apply, write_artifact_plan};
use flow_materializer::{DeleteWriter, iceberg_schema};
use flow_model::{FileId, OperationId, RowLocation, TableSchema};
use flow_state_store::{OperationKind, PreparedOperation, StateStore};
use iceberg::{spec::DataContentType, table::Table};
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, HashMap};

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

/// Remembers nonviable input/target pairs at one immutable catalog head. Keep this cursor
/// across maintenance retries; a refreshed head automatically clears it.
#[derive(Default)]
pub struct DeleteRepairCursor {
    head: Option<(uuid::Uuid, Option<i64>)>,
    // None exhausts an input whose remaining targets all fail exact row/count limits.
    attempted: BTreeSet<(FileId, Option<FileId>)>,
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
        if !repair
            && self
                .policy
                .for_format_version(table.metadata().format_version())
                .delete_layout
                == DeleteLayout::PerTarget
        {
            return self
                .rewrite_deletes_per_target(&table, schema, scratch, policy, base, indexed, view)
                .await;
        }
        let repair_input = if let Some(cursor) = &cursor {
            let inventory = self.inventory(&table, table_id).await?;
            match self
                .policy
                .for_format_version(table.metadata().format_version())
                .plan(
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
                repair_input.as_ref().is_none_or(|input| {
                    flow_iceberg_ext::content_file_id(&entry.data_file) == input.id.0
                })
            })
            .collect::<Vec<_>>();
        candidates.sort_by_key(|entry| {
            (
                flow_iceberg_ext::delete_content_size(&entry.data_file),
                entry.file_path(),
            )
        });
        let mut input_bytes = 0u64;
        let mut input_rows = 0u64;
        let mut sequence = 0;
        let mut selected = BTreeSet::new();
        for entry in candidates {
            let bytes =
                input_bytes.saturating_add(flow_iceberg_ext::delete_content_size(&entry.data_file));
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
            selected.insert(FileId(flow_iceberg_ext::content_file_id(&entry.data_file)));
            if selected.len() == policy.max_input_files {
                break;
            }
        }
        if table.metadata().format_version() == iceberg::spec::FormatVersion::V3 {
            // A new vector must contain the complete effective union for its
            // target, including positions from unselected legacy shared files.
            loop {
                let mut expanded = selected.clone();
                for (path, entry) in &view.live_files {
                    if entry.content_type() != DataContentType::Data {
                        continue;
                    }
                    let applicable = view.applicable_deletes(path)?;
                    if applicable.iter().any(|entry| {
                        selected
                            .contains(&FileId(flow_iceberg_ext::content_file_id(&entry.data_file)))
                    }) {
                        expanded.extend(applicable.iter().map(|entry| {
                            FileId(flow_iceberg_ext::content_file_id(&entry.data_file))
                        }));
                    }
                }
                if expanded == selected {
                    break;
                }
                selected = expanded;
                input_bytes = selected
                    .iter()
                    .map(|id| {
                        flow_iceberg_ext::delete_content_size(&view.live_files[&id.0].data_file)
                    })
                    .fold(0u64, u64::saturating_add);
                input_rows = selected
                    .iter()
                    .map(|id| view.live_files[&id.0].data_file.record_count())
                    .fold(0u64, u64::saturating_add);
                if selected.len() > policy.max_input_files
                    || input_bytes > policy.max_input_bytes
                    || input_rows > policy.max_input_rows
                {
                    return Ok(None);
                }
            }
            sequence = selected
                .iter()
                .filter_map(|id| view.live_files[&id.0].sequence_number)
                .max()
                .unwrap_or(0);
        }
        if selected.len() < policy.min_input_files {
            return Ok(None);
        }
        let input_files = selected.len();
        let removed_deletes: BTreeSet<String> = selected.iter().map(|id| id.0.clone()).collect();
        let mut retired_objects = selected
            .iter()
            .map(|id| view.live_files[&id.0].file_path())
            .collect::<BTreeSet<_>>();
        // Shared Puffin objects remain live until every descriptor is removed.
        // Repacking only some blobs must not count their object as retired.
        for (id, entry) in &view.live_files {
            if !removed_deletes.contains(id) {
                retired_objects.remove(entry.file_path());
            }
        }
        let retired_objects = retired_objects.len();
        let v3 = table.metadata().format_version() == iceberg::spec::FormatVersion::V3;
        // A selected vector that still applies to its live target guarantees
        // at least one output object, so fewer than two retired objects cannot
        // shrink the table. Skip the reads and upload of a rejected rewrite.
        if v3
            && !repair
            && retired_objects < 2
            && selected.iter().any(|id| {
                let entry = &view.live_files[&id.0];
                entry.file_format() == iceberg::spec::DataFileFormat::Puffin
                    && entry
                        .data_file
                        .referenced_data_file()
                        .and_then(|target| view.live_files.get(&target))
                        .is_some_and(|data| position_delete_may_apply(entry, data))
            })
        {
            return Ok(None);
        }
        let data_files = view
            .live_files
            .values()
            .filter(|entry| entry.content_type() == DataContentType::Data)
            .map(|entry| FileId(flow_iceberg_ext::content_file_id(&entry.data_file)))
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
                let mut writer = DeleteWriter::new_for_version(
                    worker_table.file_io().clone(),
                    worker_table.metadata().location(),
                    &worker_id,
                    worker_table.metadata().default_partition_spec_id(),
                    writer_config,
                    worker_table.metadata().format_version(),
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
                return (if v3 {
                    files
                        .iter()
                        .map(|file| file.file_path())
                        .collect::<BTreeSet<_>>()
                        .len()
                        < retired_objects
                } else {
                    files.len() < input_files
                }) && preserves_delete_locality(
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
                .iter()
                .filter(|(id, _)| *id != &input.0)
                .map(|(_, entry)| entry)
            {
                if position_delete_may_apply(entry, data) {
                    count += 1;
                    bytes = bytes
                        .saturating_add(flow_iceberg_ext::delete_content_size(&entry.data_file));
                    rows = rows.saturating_add(entry.data_file.record_count());
                }
            }
            for file in files {
                let mut entry = original.as_ref().clone();
                entry.data_file = file.clone();
                if position_delete_may_apply(&entry, data) {
                    count += 1;
                    bytes = bytes.saturating_add(flow_iceberg_ext::delete_content_size(file));
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
                cursor
                    .attempted
                    .insert((input, isolation.map(|(target, _)| target)));
            }
            return Ok(None);
        };
        let stats = RewriteStats {
            repair,
            layout: flow_compactor::DeleteLayout::Shared,
            input_files,
            input_bytes,
            input_rows,
            scanned_rows,
            shared_input_files: removed_deletes
                .iter()
                .filter(|id| view.single_target(&view.live_files[*id]).is_none())
                .count(),
            largest_rewritten_rows: largest_rows(&view, &removed_deletes),
            started,
        };
        self.publish_delete_rewrite(
            &table,
            table_id,
            id,
            &ownership,
            base,
            indexed.materialized_lsn,
            output,
            removed_deletes,
            sequence,
            stats,
        )
        .await
    }

    /// [`DeleteLayout::PerTarget`] consolidation. Read the bounded smallest
    /// shared delete files (CDC epochs, legacy shared or dangling files), count
    /// their effective positions per target, then also read only the existing
    /// single-target files that [`flow_compactor::Policy::select_target_merges`]
    /// picks for each target. Write one file per target data file, so the
    /// rewrite costs about the new deletes, not a target's accumulated ones.
    /// Publish only if the table-wide shared count or the largest per-file
    /// delete-file count falls.
    #[allow(clippy::too_many_arguments)]
    async fn rewrite_deletes_per_target(
        &self,
        table: &Table,
        schema: &TableSchema,
        scratch: StateStore,
        policy: &DeleteRewritePolicy,
        base: CommitBase,
        indexed: flow_state_store::TableState,
        view: SnapshotView,
    ) -> Result<Option<i64>> {
        let table_id = schema.table_id;
        let cap = self.policy.target_delete_files();
        let inventory = self.inventory(table, table_id).await?;
        let full_size = inventory
            .files
            .iter()
            .filter(|file| self.policy.full_size_target(file))
            .map(|file| file.id.clone())
            .collect::<BTreeSet<_>>();
        // Positions on L0 and small files share ranges; see `TargetRanges`.
        let ranges = self
            .policy
            .target_ranges(&inventory.files, &BTreeSet::new());
        let worker_ranges = ranges.clone();
        let mut shared = Vec::new();
        let mut targeted = BTreeMap::<FileId, Vec<TargetDeleteFile>>::new();
        for (id, entry) in &view.live_files {
            if entry.content_type() != DataContentType::PositionDeletes {
                continue;
            }
            match view.single_target(entry) {
                Some(data) => targeted
                    .entry(FileId(data.file_path().to_owned()))
                    .or_default()
                    .push(TargetDeleteFile {
                        id: FileId(id.clone()),
                        rows: entry.data_file.record_count(),
                        bytes: flow_iceberg_ext::delete_content_size(&entry.data_file),
                    }),
                None => shared.push((id, entry)),
            }
        }
        shared.sort_by_key(|(_, entry)| {
            (
                flow_iceberg_ext::delete_content_size(&entry.data_file),
                entry.file_path(),
            )
        });
        let mut input_bytes = 0u64;
        let mut input_rows = 0u64;
        let mut selected = BTreeSet::new();
        for (id, entry) in &shared {
            let bytes =
                input_bytes.saturating_add(flow_iceberg_ext::delete_content_size(&entry.data_file));
            let rows = input_rows.saturating_add(entry.data_file.record_count());
            if bytes > policy.max_input_bytes || rows > policy.max_input_rows {
                continue;
            }
            input_bytes = bytes;
            input_rows = rows;
            selected.insert(FileId((*id).clone()));
            if selected.len() == policy.max_input_files {
                break;
            }
        }
        let mut other_files = HashMap::<FileId, usize>::new();
        let mut data_files = BTreeSet::new();
        for (path, entry) in &view.live_files {
            if entry.content_type() != DataContentType::Data {
                continue;
            }
            let kept = view
                .applicable_deletes(path)?
                .into_iter()
                .filter(|delete| {
                    view.single_target(delete).is_none()
                        && !selected.contains(&FileId(flow_iceberg_ext::content_file_id(
                            &delete.data_file,
                        )))
                })
                .count();
            other_files.insert(FileId(path.clone()), kept);
            data_files.insert(FileId(path.clone()));
        }
        let before = delete_file_debt(&view, &BTreeSet::new(), &[], 0);
        let over_cap = targeted.iter().any(|(target, files)| {
            files.len() >= 2 && files.len() + other_files.get(target).copied().unwrap_or(0) > cap
        });
        // The table-wide budget can be paid without shared inputs.
        let over_budget = self.policy.over_target_budget(&inventory.debt);
        if selected.is_empty() && !over_cap && !over_budget
            || !over_budget
                && before.most.max(before.shared).max(selected.len()) < policy.min_input_files
        {
            return Ok(None);
        }
        let id = OperationId(format!("delete-rewrite-{}", uuid::Uuid::new_v4()));
        let ownership =
            WriterRegistration::new(self.store.clone(), table, table_id, id.clone(), &id)?;
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
        let budget = MergeBudget {
            files: policy.max_input_files.saturating_sub(selected.len()),
            bytes: policy.max_input_bytes.saturating_sub(input_bytes),
            rows: policy.max_input_rows.saturating_sub(input_rows),
        };
        let mut writer_config = self.writer.clone();
        writer_config.target_file_bytes = policy.target_file_bytes;
        writer_config.artifact_tracker = Some(ownership.clone());
        let read_limits = self.read_limits.clone();
        let worker_view = view.clone();
        let compaction = self.policy.clone();
        let worker_selected = selected.clone();
        let started = std::time::Instant::now();
        let (output, scanned_rows, merged) = tokio::task::spawn_blocking(move || {
            super::worker_runtime()?.block_on(async move {
                let batch_rows = scratch.batch_rows().min(8192);
                let mut scanned_rows = stage_delete_files(
                    &worker_table,
                    &worker_schema,
                    &worker_view,
                    &data_files,
                    &worker_selected,
                    &scratch,
                    &worker_id.0,
                    batch_rows,
                    limits,
                    &read_limits,
                )
                .await?;
                let mut new_rows = HashMap::<FileId, u64>::new();
                for position in scratch.position_deletes(&worker_id.0, &table_id) {
                    *new_rows.entry(position?.data_file_id).or_default() += 1;
                }
                // Every target that keeps a single-target file afterwards.
                for target in new_rows.keys() {
                    targeted.entry(target.clone()).or_default();
                }
                let targets = targeted
                    .into_iter()
                    .map(|(target, files)| TargetDeletes {
                        new_rows: new_rows.get(&target).copied().unwrap_or(0),
                        other_files: other_files.get(&target).copied().unwrap_or(0),
                        full_size: full_size.contains(&target),
                        target,
                        files,
                    })
                    .collect::<Vec<_>>();
                let merged = compaction.select_target_merges(&targets, budget);
                let merge_scan = format!("{}-targets", worker_id.0);
                if !merged.is_empty() {
                    scanned_rows += stage_delete_files(
                        &worker_table,
                        &worker_schema,
                        &worker_view,
                        &data_files,
                        &merged,
                        &scratch,
                        &merge_scan,
                        batch_rows,
                        DeleteReadLimits {
                            max_bytes: budget.bytes,
                            max_rows: budget.rows,
                        },
                        &read_limits,
                    )
                    .await?;
                }
                let mut writer = DeleteWriter::new_for_version(
                    worker_table.file_io().clone(),
                    worker_table.metadata().location(),
                    &worker_id,
                    worker_table.metadata().default_partition_spec_id(),
                    writer_config,
                    worker_table.metadata().format_version(),
                )?;
                let merged_positions = (!merged.is_empty())
                    .then(|| scratch.position_deletes(&merge_scan, &table_id))
                    .into_iter()
                    .flatten();
                let mut batch = Vec::with_capacity(batch_rows);
                let mut target = None::<FileId>;
                for position in merge_positions(
                    scratch.position_deletes(&worker_id.0, &table_id),
                    merged_positions,
                ) {
                    let position = position?;
                    if target.as_ref() != Some(&position.data_file_id) {
                        if let Some(previous) = &target
                            && !worker_ranges.continues(previous, &position.data_file_id)
                        {
                            writer.write(&batch).await?;
                            batch.clear();
                            writer.finish_target().await?;
                        }
                        target = Some(position.data_file_id.clone());
                    }
                    batch.push(position);
                    if batch.len() == batch_rows {
                        writer.write(&batch).await?;
                        batch.clear();
                    }
                }
                writer.write(&batch).await?;
                Ok::<_, anyhow::Error>((writer.close().await?, scanned_rows, merged))
            })
        })
        .await??;
        let removed_deletes = selected
            .iter()
            .chain(&merged)
            .map(|id| id.0.clone())
            .collect::<BTreeSet<_>>();
        let sequence = removed_deletes
            .iter()
            .map(|id| view.live_files[id].sequence_number)
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| anyhow::anyhow!("missing delete data sequence"))?
            .into_iter()
            .max()
            .unwrap_or(0);
        ensure!(
            output.iter().all(|file| {
                flow_iceberg_ext::exact_delete_target(file).is_some()
                    || delete_path_bounds(file).is_some_and(|(lower, upper)| {
                        ranges.pooled.contains(&lower)
                            && ranges.pooled.contains(&upper)
                            && ranges.continues(&lower, &upper)
                    })
            }),
            "per-target delete output spans a data file it may not share"
        );
        let after = delete_file_debt(&view, &removed_deletes, &output, sequence);
        if !(after.shared < before.shared
            || after.most < before.most
            || over_budget && after.single < before.single)
            || !preserves_delete_locality(&view, &removed_deletes, &output, sequence, &self.policy)
        {
            // The rewrite would not lower delete-file count debt. Ownership
            // records retain the unused output for GC.
            let store = self.store.clone();
            blocking(move || Ok(store.discard_uncommitted(&id)?)).await?;
            return Ok(None);
        }
        let (input_bytes, input_rows) =
            removed_deletes
                .iter()
                .fold((0u64, 0u64), |(bytes, rows), id| {
                    let file = &view.live_files[id].data_file;
                    (
                        bytes.saturating_add(flow_iceberg_ext::delete_content_size(file)),
                        rows.saturating_add(file.record_count()),
                    )
                });
        let stats = RewriteStats {
            repair: false,
            layout: DeleteLayout::PerTarget,
            input_files: removed_deletes.len(),
            input_bytes,
            input_rows,
            scanned_rows,
            shared_input_files: selected.len(),
            largest_rewritten_rows: largest_rows(&view, &removed_deletes),
            started,
        };
        self.publish_delete_rewrite(
            table,
            table_id,
            id,
            &ownership,
            base,
            indexed.materialized_lsn,
            output,
            removed_deletes,
            sequence,
            stats,
        )
        .await
    }

    /// Commit a delete-only rewrite whose outputs are complete and viable,
    /// then report it.
    #[allow(clippy::too_many_arguments)]
    async fn publish_delete_rewrite(
        &self,
        table: &Table,
        table_id: flow_model::TableId,
        id: OperationId,
        ownership: &std::sync::Arc<WriterRegistration>,
        base: CommitBase,
        materialized_lsn: flow_model::PgLsn,
        output: Vec<iceberg::spec::DataFile>,
        removed_deletes: BTreeSet<String>,
        sequence: i64,
        stats: RewriteStats,
    ) -> Result<Option<i64>> {
        let RewriteStats {
            repair,
            layout,
            input_files,
            input_bytes,
            input_rows,
            scanned_rows,
            shared_input_files,
            largest_rewritten_rows,
            started,
        } = stats;
        let path = crate::artifacts::prepared_plan_path(table, &id);
        write_artifact_plan(table, &path, &[], &output).await?;
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
                ("streaming.last-lsn".into(), materialized_lsn.to_string()),
            ]),
        })?;
        let artifacts = output
            .iter()
            .map(|file| file.file_path().to_owned())
            .chain(std::iter::once(path))
            .collect();
        let (output_files, output_bytes) = flow_iceberg_ext::physical_file_stats(&output);
        let reservation = ownership.finish(0, output_files).await?;
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
        let snapshot = self.recover(table, &id).await?;
        let store = self.store.clone();
        let operation_id = id.clone();
        blocking(move || Ok(store.forget_applied(&operation_id)?)).await?;
        tracing::info!(
            event = "delete_rewrite_completed",
            table_id = table_id.0,
            operation_id = %id.0,
            snapshot_id = snapshot,
            repair,
            delete_layout = layout.as_str(),
            input_files,
            shared_input_files,
            target_input_files = input_files - shared_input_files,
            largest_rewritten_rows,
            input_bytes,
            input_rows,
            scanned_rows,
            output_files,
            output_targets = output
                .iter()
                .filter_map(flow_iceberg_ext::exact_delete_target)
                .collect::<BTreeSet<_>>()
                .len(),
            output_rows = output.iter().map(|file| file.record_count()).sum::<u64>(),
            largest_output_rows = output.iter().map(|file| file.record_count()).max().unwrap_or(0),
            output_bytes,
            elapsed_ms = started.elapsed().as_secs_f64() * 1000.0,
            "position-delete files consolidated"
        );
        Ok(snapshot)
    }
}

/// What a delete-only rewrite read, for its completion event.
struct RewriteStats {
    repair: bool,
    layout: flow_compactor::DeleteLayout,
    input_files: usize,
    input_bytes: u64,
    input_rows: u64,
    scanned_rows: u64,
    /// Inputs that do not target exactly one live data file.
    shared_input_files: usize,
    /// The largest input's rows: the most one rewrite re-reads of one file.
    largest_rewritten_rows: u64,
    started: std::time::Instant,
}

fn largest_rows(view: &SnapshotView, files: &BTreeSet<String>) -> u64 {
    files
        .iter()
        .map(|id| view.live_files[id].data_file.record_count())
        .max()
        .unwrap_or(0)
}

/// Delete-file counts once `removed` is replaced by `output`.
struct DeleteFileDebt {
    /// The most delete files applying to one live data file.
    most: usize,
    /// Files that do not target exactly one live data file, table-wide.
    shared: usize,
    /// Files that do, table-wide.
    single: usize,
}

/// Delete-file count debt once `removed` is replaced by per-target `output`
/// at `sequence`.
fn delete_file_debt(
    view: &SnapshotView,
    removed: &BTreeSet<String>,
    output: &[iceberg::spec::DataFile],
    sequence: i64,
) -> DeleteFileDebt {
    let output = removed
        .first()
        .and_then(|path| view.live_files.get(path))
        .map(|template| {
            output
                .iter()
                .map(|file| iceberg::spec::ManifestEntry {
                    data_file: file.clone(),
                    sequence_number: Some(sequence),
                    ..template.as_ref().clone()
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let deletes = view
        .live_files
        .iter()
        .filter(|(id, entry)| {
            entry.content_type() == DataContentType::PositionDeletes && !removed.contains(*id)
        })
        .map(|(_, entry)| entry.as_ref())
        .collect::<Vec<_>>();
    let shared = deletes
        .iter()
        .filter(|entry| view.single_target(entry).is_none())
        .count();
    // Pooled ranges are shared; every other output targets one file.
    let pooled = output
        .iter()
        .filter(|entry| flow_iceberg_ext::exact_delete_target(&entry.data_file).is_none())
        .count();
    let single = deletes.len() - shared + output.len() - pooled;
    let shared = shared + pooled;
    let most = view
        .live_files
        .values()
        .filter(|entry| entry.content_type() == DataContentType::Data)
        .map(|data| {
            deletes
                .iter()
                .copied()
                .chain(&output)
                .filter(|delete| position_delete_may_apply(delete, data))
                .count()
        })
        .max()
        .unwrap_or(0);
    DeleteFileDebt {
        most,
        shared,
        single,
    }
}

/// The lower and upper `file_path` bounds of a position-delete file.
fn delete_path_bounds(file: &iceberg::spec::DataFile) -> Option<(FileId, FileId)> {
    let bound = |bounds: &std::collections::HashMap<i32, iceberg::spec::Datum>| match bounds
        .get(&iceberg::metadata_columns::RESERVED_FIELD_ID_DELETE_FILE_PATH)?
        .literal()
    {
        iceberg::spec::PrimitiveLiteral::String(path) => Some(FileId(path.clone())),
        _ => None,
    };
    Some((bound(file.lower_bounds())?, bound(file.upper_bounds())?))
}

/// Merge two position streams, each sorted and unique by path and position,
/// into one, keeping a position present in both once.
fn merge_positions<E>(
    left: impl Iterator<Item = std::result::Result<RowLocation, E>>,
    right: impl Iterator<Item = std::result::Result<RowLocation, E>>,
) -> impl Iterator<Item = std::result::Result<RowLocation, E>> {
    let mut left = left.peekable();
    let mut right = right.peekable();
    std::iter::from_fn(move || {
        let order = match (left.peek(), right.peek()) {
            (None, None) => return None,
            (Some(Err(_)), _) | (Some(_), None) => Ordering::Less,
            (_, Some(Err(_))) | (None, Some(_)) => Ordering::Greater,
            (Some(Ok(left)), Some(Ok(right))) => (&left.data_file_id, left.row_position)
                .cmp(&(&right.data_file_id, right.row_position)),
        };
        match order {
            Ordering::Less => left.next(),
            Ordering::Greater => right.next(),
            Ordering::Equal => {
                right.next();
                left.next()
            }
        }
    })
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
        .iter()
        .filter(|(_, entry)| entry.content_type() == DataContentType::PositionDeletes)
        .map(|(id, entry)| (id, entry.as_ref()))
        .collect::<Vec<_>>();
    view.live_files
        .values()
        .filter(|entry| entry.content_type() == DataContentType::Data)
        .all(|data| {
            !within_delete_budget(deletes.iter().map(|(_, entry)| *entry), data, policy)
                || within_delete_budget(
                    deletes
                        .iter()
                        .copied()
                        .filter(|(id, _)| !removed.contains(*id))
                        .map(|(_, entry)| entry)
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
            let bytes =
                bytes.checked_add(flow_iceberg_ext::delete_content_size(&entry.data_file))?;
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
                && !cursor.attempted.contains(&(input.id.clone(), None))
        })
        .filter_map(|input| {
            let priority = targets
                .iter()
                .filter(|(file, rows, bytes, count)| {
                    input.targets.contains(&file.id)
                        && !cursor
                            .attempted
                            .contains(&(input.id.clone(), Some(file.id.clone())))
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
                    && !cursor
                        .attempted
                        .contains(&(input.id.clone(), Some(file.id.clone())))
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

#[cfg(test)]
mod tests {
    use super::*;
    use iceberg::spec::{DataFileBuilder, DataFileFormat, ManifestEntry, ManifestStatus, Struct};
    use std::sync::Arc;

    #[test]
    fn vector_replacement_does_not_count_removed_blobs_against_locality_budget() {
        let mut view = SnapshotView::default();
        let mut removed = BTreeSet::new();
        let mut output = Vec::new();
        for (target, offset) in [("a.parquet", 4), ("b.parquet", 24)] {
            let data = DataFileBuilder::default()
                .content(DataContentType::Data)
                .file_format(DataFileFormat::Parquet)
                .file_path(target.to_owned())
                .partition(Struct::empty())
                .record_count(2)
                .file_size_in_bytes(100)
                .build()
                .unwrap();
            let vector = |path: &str| {
                DataFileBuilder::default()
                    .content(DataContentType::PositionDeletes)
                    .file_format(DataFileFormat::Puffin)
                    .file_path(path.to_owned())
                    .partition(Struct::empty())
                    .record_count(1)
                    .file_size_in_bytes(100)
                    .referenced_data_file(Some(target.to_owned()))
                    .content_offset(Some(offset))
                    .content_size_in_bytes(Some(20))
                    .build()
                    .unwrap()
            };
            let old = vector("old.puffin");
            removed.insert(flow_iceberg_ext::content_file_id(&old));
            output.push(vector("new.puffin"));
            for file in [data, old] {
                view.live_files.insert(
                    flow_iceberg_ext::content_file_id(&file),
                    Arc::new(ManifestEntry {
                        status: ManifestStatus::Added,
                        snapshot_id: Some(1),
                        sequence_number: Some(1),
                        file_sequence_number: Some(1),
                        data_file: file,
                    }),
                );
            }
        }
        // Each target remains at exactly one logical delete, even though both
        // targets share the old and new physical Puffin objects.
        let policy = flow_compactor::Policy {
            max_delete_input_files: 1,
            max_delete_input_rows: 1,
            max_delete_input_bytes: 20,
            ..Default::default()
        };
        assert!(preserves_delete_locality(
            &view, &removed, &output, 1, &policy
        ));
    }
}
