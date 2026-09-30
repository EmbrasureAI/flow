//! Maintain the table's physical layout and reconcile external catalog commits.
//!
//! [`TableMaintenance`] shares the caller's single-writer table actor with
//! [`crate::TablePublisher`]. Background builds leave publication to that actor.

mod builds;
mod concurrent;
pub use builds::{BuildRegistration, active_build_protection, discard_abandoned_builds};
pub use concurrent::{
    PreparationWait, PreparedCompaction, ReadyCompaction, RetiringCompactionPreparation,
    RunningCompaction, RunningCompactionPreparation,
};
mod garbage;
pub use garbage::{GarbagePolicy, GarbageProtection, GarbageReport};
mod history;
pub use history::HistoryPolicy;
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Result, bail, ensure};
use flow_compactor::{Debt, DeleteDependency, FileCandidate, Level, Policy};
use flow_iceberg_ext::{
    CommitBase, ManifestCache, OPERATION_ID_KEY, SnapshotView, write_artifact_plan,
};
use flow_materializer::{WriterConfig, iceberg_schema};
use flow_model::{FileId, OperationId, PgLsn, PrimaryKey, RowLocation, TableId, TableSchema};
use flow_state_store::{IndexDelta, OperationKind, OperationPhase, PreparedOperation, StateStore};
use iceberg::spec::DataContentType;
use iceberg::{Catalog, table::Table};
use serde::{Deserialize, Serialize};

use crate::{artifacts, blocking, publication::ReplanRequired};

mod deletes;
pub use deletes::{DeleteRepairCursor, DeleteRewritePolicy};
mod levels;
mod manifests;
mod recovery;
use levels::file_level;
pub(crate) use recovery::resolve_maintenance_catalog_operation;
use recovery::{MaintenanceResolution, resolve_maintenance_operation};

/// OpenDAL shares its HTTP pool across the process. Connection tasks started by
/// one worker must remain driven after that worker finishes or its caller shuts
/// down. Keep one lazy I/O driver for the same lifetime as that pool; row work
/// still runs on the existing blocking workers that call `block_on`.
fn worker_runtime() -> Result<&'static tokio::runtime::Runtime> {
    static RUNTIME: std::sync::OnceLock<std::io::Result<tokio::runtime::Runtime>> =
        std::sync::OnceLock::new();
    RUNTIME
        .get_or_init(|| {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .thread_name("flow-maintenance-io")
                .enable_all()
                .build()
        })
        .as_ref()
        .map_err(|error| anyhow::anyhow!("start maintenance I/O runtime: {error}"))
}

/// Reader-health inventory with exact live-row counts from the index at the
/// same snapshot. Shared delete files cannot inflate per-file deletion density.
pub struct Inventory {
    pub files: Vec<FileCandidate>,
    pub deletes: Vec<DeleteDependency>,
    pub debt: Debt,
    pub manifest_count: usize,
    pub manifest_entries: usize,
    pub delete_file_count: usize,
}

#[derive(Serialize, Deserialize)]
struct RewriteRecovery {
    base: CommitBase,
    manifest_list: String,
    removed_data: BTreeSet<String>,
    removed_deletes: BTreeSet<String>,
    #[serde(default)]
    delete_sequence: Option<i64>,
    #[serde(default)]
    output_data_sequence: Option<i64>,
    properties: HashMap<String, String>,
}

struct RewriteCommit {
    base: CommitBase,
    ownership: Arc<artifacts::WriterRegistration>,
    output_data_sequence: Option<i64>,
    build_snapshot_id: Option<i64>,
    deltas_staged: bool,
    worker_ms: f64,
    prepare_started: std::time::Instant,
    // Preflight and plan sealing partition prepare_ms. A speculative catch-up
    // can be completed before activation and is reported independently.
    prepare_preflight_ms: f64,
    catch_up_ms: f64,
    plan_seal_started: std::time::Instant,
}

#[derive(Serialize, Deserialize)]
struct ReconcileRecovery {
    table_uuid: uuid::Uuid,
    snapshot_id: i64,
    sequence_number: i64,
}

struct RecoveryDiagnostics {
    started: Instant,
    record_found: bool,
    table_id: u32,
    kind: &'static str,
    phase_before: Option<OperationPhase>,
    total_delta_rows: u64,
    apply_start_cursor: u64,
    durable_lookup_phase: Duration,
    durable_lookup_call: Duration,
    catalog_resolution_path: &'static str,
    catalog_resolution_phase: Duration,
    catalog_update_call: Option<Duration>,
    catalog_already_committed: bool,
    mark_committed_required: bool,
    mark_committed_phase: Duration,
    mark_committed_call: Duration,
    state_apply_phase: Duration,
    state_apply_call: Duration,
    discard_before_replan_phase: Duration,
    discard_uncommitted_call: Duration,
    catalog_resolve_boundary: Duration,
    mark_apply_boundary: Duration,
    completed_apply: bool,
    snapshot_id: Option<i64>,
    applied_rows: u64,
    obsolete_rows: u64,
}

impl RecoveryDiagnostics {
    fn new() -> Self {
        Self {
            started: Instant::now(),
            record_found: false,
            table_id: 0,
            kind: "unknown",
            phase_before: None,
            total_delta_rows: 0,
            apply_start_cursor: 0,
            durable_lookup_phase: Duration::ZERO,
            durable_lookup_call: Duration::ZERO,
            catalog_resolution_path: "not_started",
            catalog_resolution_phase: Duration::ZERO,
            catalog_update_call: None,
            catalog_already_committed: false,
            mark_committed_required: false,
            mark_committed_phase: Duration::ZERO,
            mark_committed_call: Duration::ZERO,
            state_apply_phase: Duration::ZERO,
            state_apply_call: Duration::ZERO,
            discard_before_replan_phase: Duration::ZERO,
            discard_uncommitted_call: Duration::ZERO,
            catalog_resolve_boundary: Duration::ZERO,
            mark_apply_boundary: Duration::ZERO,
            completed_apply: false,
            snapshot_id: None,
            applied_rows: 0,
            obsolete_rows: 0,
        }
    }

    fn emit(
        &self,
        event_name: &'static str,
        operation_id: &OperationId,
        outcome: &'static str,
        error: Option<&anyhow::Error>,
    ) {
        let total = self.started.elapsed();
        let catalog_resolution_other = self
            .catalog_resolution_phase
            .saturating_sub(self.catalog_update_call.unwrap_or_default());
        let mark_committed_other = self
            .mark_committed_phase
            .saturating_sub(self.mark_committed_call);
        let state_apply_other = self.state_apply_phase.saturating_sub(self.state_apply_call);
        let discard_before_replan_other = self
            .discard_before_replan_phase
            .saturating_sub(self.discard_uncommitted_call);
        let recovery_other = total
            .saturating_sub(self.durable_lookup_phase)
            .saturating_sub(self.catalog_resolution_phase)
            .saturating_sub(self.mark_committed_phase)
            .saturating_sub(self.state_apply_phase)
            .saturating_sub(self.discard_before_replan_phase);
        let error = error.map(ToString::to_string).unwrap_or_default();
        tracing::info!(
            event = event_name,
            operation_id = %operation_id.0,
            outcome,
            error,
            record_found = self.record_found,
            table_id = self.table_id,
            kind = self.kind,
            phase_before = ?self.phase_before,
            snapshot_known = self.snapshot_id.is_some(),
            snapshot_id = self.snapshot_id.unwrap_or_default(),
            total_delta_rows = self.total_delta_rows,
            apply_start_cursor = self.apply_start_cursor,
            applied_rows_this_call = self.applied_rows,
            obsolete_rows_this_call = self.obsolete_rows,
            catalog_resolve_ms = self.catalog_resolve_boundary.as_secs_f64() * 1000.0,
            mark_apply_ms = self.mark_apply_boundary.as_secs_f64() * 1000.0,
            durable_lookup_phase_ms = self.durable_lookup_phase.as_secs_f64() * 1000.0,
            durable_lookup_call_ms = self.durable_lookup_call.as_secs_f64() * 1000.0,
            durable_lookup_other_ms = self
                .durable_lookup_phase
                .saturating_sub(self.durable_lookup_call)
                .as_secs_f64()
                * 1000.0,
            catalog_resolution_path = self.catalog_resolution_path,
            catalog_resolution_phase_ms = self.catalog_resolution_phase.as_secs_f64() * 1000.0,
            catalog_update_attempted = self.catalog_update_call.is_some(),
            catalog_update_call_ms = self.catalog_update_call.unwrap_or_default().as_secs_f64()
                * 1000.0,
            catalog_already_committed = self.catalog_already_committed,
            catalog_resolution_other_ms = catalog_resolution_other.as_secs_f64() * 1000.0,
            mark_committed_required = self.mark_committed_required,
            mark_committed_phase_ms = self.mark_committed_phase.as_secs_f64() * 1000.0,
            mark_committed_call_ms = self.mark_committed_call.as_secs_f64() * 1000.0,
            mark_committed_other_ms = mark_committed_other.as_secs_f64() * 1000.0,
            state_apply_phase_ms = self.state_apply_phase.as_secs_f64() * 1000.0,
            state_apply_call_ms = self.state_apply_call.as_secs_f64() * 1000.0,
            state_apply_other_ms = state_apply_other.as_secs_f64() * 1000.0,
            discard_before_replan_phase_ms = self.discard_before_replan_phase.as_secs_f64()
                * 1000.0,
            discard_uncommitted_call_ms = self.discard_uncommitted_call.as_secs_f64() * 1000.0,
            discard_before_replan_other_ms = discard_before_replan_other.as_secs_f64() * 1000.0,
            recovery_other_ms = recovery_other.as_secs_f64() * 1000.0,
            recovery_total_ms = total.as_secs_f64() * 1000.0,
            "maintenance recovery phase diagnostics"
        );
    }

    fn emit_result(&self, operation_id: &OperationId, result: &Result<Option<i64>>) {
        let outcome = match result {
            Ok(_) => "success",
            Err(error) if error.downcast_ref::<ReplanRequired>().is_some() => "replan",
            Err(_) => "error",
        };
        self.emit(
            "maintenance_recovery_terminal",
            operation_id,
            outcome,
            result.as_ref().err(),
        );
        if self.completed_apply && result.is_ok() {
            self.emit("maintenance_recovered", operation_id, "success", None);
        }
    }
}

/// Uses the same durable table fence as ingestion. Work may run concurrently,
/// but preparing and committing must be serialized by the owning table actor.
pub struct TableMaintenance {
    store: StateStore,
    catalog: Arc<dyn Catalog>,
    policy: Policy,
    writer: WriterConfig,
    read_limits: flow_compactor::ReadLimits,
    cache: ManifestCache,
}

impl TableMaintenance {
    pub fn new(
        store: StateStore,
        catalog: Arc<dyn Catalog>,
        policy: Policy,
        writer: WriterConfig,
    ) -> Result<Self> {
        policy.validate()?;
        Ok(Self {
            store,
            catalog,
            policy,
            writer,
            read_limits: flow_compactor::ReadLimits::default(),
            cache: ManifestCache::default(),
        })
    }

    pub fn with_read_limits(mut self, limits: flow_compactor::ReadLimits) -> Result<Self> {
        limits.validate()?;
        self.read_limits = limits;
        Ok(self)
    }

    pub async fn inventory(&self, table: &Table, table_id: TableId) -> Result<Inventory> {
        let view = SnapshotView::current_with_cache(table, &self.cache).await?;
        ensure!(
            view.live_files
                .values()
                .all(|entry| entry.content_type() != DataContentType::EqualityDeletes),
            "equality deletes require reconciliation before maintenance"
        );
        let now = u64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis())?;
        let data_prefix = format!(
            "{}/data/",
            table.metadata().location().trim_end_matches('/')
        );
        let mut files = Vec::new();
        let mut dependencies: HashMap<String, BTreeSet<FileId>> = HashMap::new();
        for entry in view
            .live_files
            .values()
            .filter(|entry| entry.content_type() == DataContentType::Data)
        {
            let file = entry.data_file();
            let snapshot = entry
                .snapshot_id
                .and_then(|id| table.metadata().snapshot_by_id(id));
            let summary = snapshot.map(|snapshot| &snapshot.summary().additional_properties);
            let level = file_level(&data_prefix, file.file_path(), summary);
            let deletes = view.applicable_deletes(file.file_path())?;
            let id = FileId(file.file_path().to_owned());
            for delete in &deletes {
                dependencies
                    .entry(flow_iceberg_ext::content_file_id(&delete.data_file))
                    .or_default()
                    .insert(id.clone());
            }
            let age_ms = snapshot
                .map(|snapshot| {
                    now.saturating_sub(u64::try_from(snapshot.timestamp_ms()).unwrap_or(0))
                })
                .unwrap_or(self.policy.oldest_l0_hard_ms);
            files.push(FileCandidate {
                id,
                level,
                size_bytes: file.file_size_in_bytes(),
                row_count: file.record_count(),
                deleted_rows: 0,
                delete_file_count: deletes.len(),
                age_ms,
                spec_id: table.metadata().default_partition_spec_id(),
                partition: Vec::new(),
            });
        }
        let paths = files.iter().map(|file| file.id.clone()).collect::<Vec<_>>();
        let store = self.store.clone();
        let snapshot = table.metadata().current_snapshot_id();
        let live_counts =
            blocking(
                move || match store.file_live_row_counts(&table_id, snapshot, &paths) {
                    Ok(counts) => Ok(counts),
                    Err(flow_state_store::Error::SnapshotMismatch { .. }) => {
                        Err(ReplanRequired.into())
                    }
                    Err(error) => Err(error.into()),
                },
            )
            .await?;
        for (file, live_rows) in files.iter_mut().zip(live_counts) {
            file.deleted_rows = file.row_count.checked_sub(live_rows).ok_or_else(|| {
                anyhow::anyhow!("indexed live rows exceed physical rows in {}", file.id.0)
            })?;
        }
        let deletes = dependencies
            .into_iter()
            .map(|(path, targets)| DeleteDependency {
                size_bytes: flow_iceberg_ext::delete_content_size(
                    &view.live_files[&path].data_file,
                ),
                row_count: view.live_files[&path].data_file.record_count(),
                id: FileId(path),
                targets,
            })
            .collect();
        let delete_file_count = view
            .live_files
            .values()
            .filter(|entry| entry.content_type() == DataContentType::PositionDeletes)
            .map(|entry| entry.file_path())
            .collect::<BTreeSet<_>>()
            .len();
        let legacy_delete_count = view
            .live_files
            .values()
            .filter(|entry| {
                entry.content_type() == DataContentType::PositionDeletes
                    && entry.file_format() != iceberg::spec::DataFileFormat::Puffin
            })
            .count();
        let mut debt = self.policy.debt(&files)?;
        // Legacy shared/dangling files add planning work across targets. A DV
        // is scoped to one file; sparse vectors on healthy files are not global
        // debt. Per-file density still schedules reclamation.
        if legacy_delete_count >= self.policy.delete_files_hard {
            debt.pressure = flow_compactor::PublicationPressure::Pause;
        } else if legacy_delete_count >= self.policy.delete_files_soft {
            debt.pressure = debt
                .pressure
                .max(flow_compactor::PublicationPressure::Delay);
        }
        Ok(Inventory {
            files,
            deletes,
            debt,
            manifest_count: view.manifest_count(),
            manifest_entries: view.manifest_entries(),
            delete_file_count,
        })
    }

    /// Plan and execute one bounded rewrite. The scratch store is exclusive to
    /// this worker and must be fresh; it is never part of the public read plane.
    pub async fn compact(
        &self,
        table: &Table,
        schema: &TableSchema,
        scratch: StateStore,
    ) -> Result<Option<i64>> {
        let table = self.catalog.load_table(table.identifier()).await?;
        ensure!(
            crate::same_iceberg_schema(table.metadata().current_schema(), &iceberg_schema(schema)?),
            "source schema differs from Iceberg"
        );
        let table_id = schema.table_id;
        let schema_version = schema.version;
        let store = self.store.clone();
        let initial_index = blocking(move || Ok(store.table_state(&table_id)?)).await?;
        ensure!(
            initial_index.pending_operation.is_none(),
            "recover pending operation before planning maintenance"
        );
        if initial_index.snapshot_id != table.metadata().current_snapshot_id() {
            return Err(ReplanRequired.into());
        }
        let Some(snapshot) = table.metadata().current_snapshot_id() else {
            return Ok(None);
        };
        let inventory = self.inventory(&table, table_id).await?;
        let Some(plan) = self.policy.plan(
            snapshot,
            table.metadata().current_schema_id(),
            &inventory.files,
            &inventory.deletes,
        )?
        else {
            return Ok(None);
        };
        let base = CommitBase::new(&table);
        let level = match plan.output_level {
            Level::L0 => "l0",
            Level::L1 => "l1",
            Level::L2 => "l2",
        };
        let operation_id = OperationId(format!("flow-{level}-rewrite-{}", uuid::Uuid::new_v4()));
        let store = self.store.clone();
        let worker_table = table.clone();
        let worker_schema = schema.clone();
        let ownership = artifacts::WriterRegistration::new(
            self.store.clone(),
            &table,
            table_id,
            operation_id.clone(),
            &operation_id,
        )?;
        let reservation = ownership.record().await?;
        let building = PreparedOperation {
            id: operation_id.clone(),
            table_id,
            kind: OperationKind::Rewrite,
            base_snapshot_id: initial_index.snapshot_id,
            last_lsn: initial_index.materialized_lsn,
            schema_version,
            artifacts: Vec::new(),
            payload: Vec::new(),
        };
        let prepare_store = self.store.clone();
        blocking(move || {
            Ok(prepare_store
                .begin_prepare_with_record(building, Some((&reservation.0, &reservation.1)))?)
        })
        .await?;
        let mut writer = self.writer.clone();
        writer.artifact_tracker = Some(ownership.clone());
        let read_limits = self.read_limits.clone();
        let worker_started = std::time::Instant::now();
        let operation_id_for_result = operation_id.clone();
        let output = tokio::task::spawn_blocking(move || {
            worker_runtime()?.block_on(flow_compactor::compact(
                &worker_table,
                worker_schema,
                plan,
                operation_id,
                &store,
                scratch,
                writer,
                &read_limits,
            ))
        })
        .await??;
        let worker_ms = worker_started.elapsed().as_secs_f64() * 1000.0;
        let prepare_started = std::time::Instant::now();
        let head = self.catalog.load_table(table.identifier()).await?;
        base.validate(&head)?;
        let plan_seal_started = std::time::Instant::now();
        let snapshot = self
            .publish_rewrite(
                &head,
                schema,
                output,
                RewriteCommit {
                    base,
                    ownership,
                    output_data_sequence: None,
                    build_snapshot_id: None,
                    deltas_staged: false,
                    worker_ms,
                    prepare_started,
                    prepare_preflight_ms: plan_seal_started
                        .duration_since(prepare_started)
                        .as_secs_f64()
                        * 1000.0,
                    catch_up_ms: 0.0,
                    plan_seal_started,
                },
            )
            .await?;
        self.forget_rewrite(&operation_id_for_result).await?;
        Ok(snapshot)
    }

    async fn publish_rewrite(
        &self,
        head: &Table,
        schema: &TableSchema,
        output: flow_compactor::WorkerOutput,
        context: RewriteCommit,
    ) -> Result<Option<i64>> {
        let table_id = schema.table_id;
        let schema_version = schema.version;
        let store = self.store.clone();
        let indexed = blocking(move || Ok(store.table_state(&table_id)?)).await?;
        ensure!(
            indexed.pending_operation.as_ref() == Some(&output.operation_id),
            "rewrite lost its index fence"
        );
        if indexed.snapshot_id != head.metadata().current_snapshot_id() {
            return Err(ReplanRequired.into());
        }
        let path = format!(
            "{}/metadata/{}-prepared.avro",
            head.metadata().location().trim_end_matches('/'),
            output.operation_id.0
        );
        write_artifact_plan(head, &path, &output.data_files, &output.delete_files).await?;
        let level = match output.plan.output_level {
            Level::L0 => "L0",
            Level::L1 => "L1",
            Level::L2 => "L2",
        };
        let mut recovery = RewriteRecovery {
            delete_sequence: output.delete_sequence,
            output_data_sequence: context.output_data_sequence,
            base: context.base,
            manifest_list: path.clone(),
            removed_data: output
                .plan
                .input_files
                .iter()
                .map(|file| file.0.clone())
                .collect(),
            removed_deletes: output
                .plan
                .delete_files
                .iter()
                .map(|file| file.0.clone())
                .collect(),
            properties: HashMap::from([
                ("streaming.writer-id".into(), "embrasure-flow".into()),
                ("streaming.operation".into(), "compact".into()),
                ("streaming.level".into(), level.into()),
                (
                    "streaming.last-lsn".into(),
                    indexed.materialized_lsn.to_string(),
                ),
            ]),
        };
        if let Some(snapshot) = context.build_snapshot_id {
            recovery
                .properties
                .insert("streaming.build-snapshot-id".into(), snapshot.to_string());
            recovery.properties.insert(
                "streaming.build-sequence-number".into(),
                context
                    .output_data_sequence
                    .expect("speculative data sequence")
                    .to_string(),
            );
        }
        let operation = PreparedOperation {
            id: output.operation_id.clone(),
            table_id,
            kind: OperationKind::Rewrite,
            base_snapshot_id: indexed.snapshot_id,
            last_lsn: indexed.materialized_lsn,
            schema_version,
            artifacts: output
                .data_files
                .iter()
                .chain(&output.delete_files)
                .map(|file| file.file_path().to_owned())
                .chain(std::iter::once(path))
                .collect(),
            payload: serde_json::to_vec(&recovery)?,
        };
        let id = operation.id.clone();
        // Physical output includes rows masked during catch-up. It is not the
        // number of surviving mappings or the catalog's logical row count.
        let output_data_files = output.data_files.len();
        let output_data_rows = output
            .data_files
            .iter()
            .map(|file| file.record_count())
            .sum::<u64>();
        let output_data_bytes = output
            .data_files
            .iter()
            .map(|file| file.file_size_in_bytes())
            .sum::<u64>();
        let (output_delete_files, output_delete_bytes) =
            flow_iceberg_ext::physical_file_stats(&output.delete_files);
        let output_delete_rows = output
            .delete_files
            .iter()
            .map(|file| file.record_count())
            .sum::<u64>();
        let reservation = context
            .ownership
            .finish(output.data_files.len(), output_delete_files)
            .await?;
        let store = self.store.clone();
        blocking(move || {
            if !context.deltas_staged {
                store.stage_deltas_fallible(&operation.id, output.index_deltas()?)?;
            }
            store.seal_prepare_with_record(
                &operation.id,
                operation.artifacts,
                operation.payload,
                Some((&reservation.0, &reservation.1)),
            )?;
            Ok(())
        })
        .await?;
        let prepared_at = std::time::Instant::now();
        let prepare_ms = prepared_at
            .duration_since(context.prepare_started)
            .as_secs_f64()
            * 1000.0;
        let plan_seal_ms = prepared_at
            .duration_since(context.plan_seal_started)
            .as_secs_f64()
            * 1000.0;
        for (phase, elapsed_ms) in [
            ("preflight", context.prepare_preflight_ms),
            ("catch_up", context.catch_up_ms),
            ("plan_seal", plan_seal_ms),
        ] {
            metrics::histogram!("flow_maintenance_phase_seconds", "table_id" => table_id.0.to_string(), "phase" => phase)
                .record(elapsed_ms / 1000.0);
        }
        let publish_started = std::time::Instant::now();
        let snapshot = self.recover(head, &id).await?;
        let recovered_at = std::time::Instant::now();
        let publish_and_apply_ms =
            recovered_at.duration_since(publish_started).as_secs_f64() * 1000.0;
        let coordinator_prepare_publish_ms = recovered_at
            .duration_since(context.prepare_started)
            .as_secs_f64()
            * 1000.0;
        let coordinator_prepare_publish_other_ms =
            (coordinator_prepare_publish_ms - prepare_ms - publish_and_apply_ms).max(0.0);
        tracing::info!(
            event = "compaction_completed",
            table_id = table_id.0,
            operation_id = %id.0,
            candidate_kind = "data_rewrite",
            snapshot_id = snapshot,
            output_level = level,
            output_data_files,
            output_data_rows,
            output_data_bytes,
            output_delete_files,
            output_delete_rows,
            output_delete_bytes,
            worker_ms = context.worker_ms,
            prepare_ms,
            prepare_preflight_ms = context.prepare_preflight_ms,
            catch_up_ms = context.catch_up_ms,
            plan_seal_ms,
            publish_and_apply_ms,
            coordinator_prepare_publish_ms,
            coordinator_prepare_publish_other_ms,
            "table rewrite completed"
        );
        Ok(snapshot)
    }

    async fn forget_rewrite(&self, id: &OperationId) -> Result<()> {
        let store = self.store.clone();
        let id = id.clone();
        blocking(move || {
            store.forget_applied(&id)?;
            Ok(())
        })
        .await
    }

    pub async fn recover(&self, table: &Table, id: &OperationId) -> Result<Option<i64>> {
        let mut diagnostics = RecoveryDiagnostics::new();
        let result = self
            .recover_with_diagnostics(table, id, &mut diagnostics)
            .await;
        diagnostics.emit_result(id, &result);
        result
    }

    async fn recover_with_diagnostics(
        &self,
        table: &Table,
        id: &OperationId,
        diagnostics: &mut RecoveryDiagnostics,
    ) -> Result<Option<i64>> {
        let lookup_started = Instant::now();
        let store = self.store.clone();
        let operation_id = id.clone();
        let lookup = blocking(move || {
            let call_started = Instant::now();
            let result = store.operation(&operation_id);
            Ok((result, call_started.elapsed()))
        })
        .await;
        diagnostics.durable_lookup_phase = lookup_started.elapsed();
        let (record, lookup_call) = lookup?;
        diagnostics.durable_lookup_call = lookup_call;
        let record = record?.ok_or_else(|| anyhow::anyhow!("unknown maintenance operation"))?;
        diagnostics.record_found = true;
        diagnostics.table_id = record.operation.table_id.0;
        diagnostics.phase_before = Some(record.phase);
        diagnostics.total_delta_rows = record.delta_count;
        diagnostics.apply_start_cursor = record.applied_count;
        diagnostics.kind = match record.operation.kind {
            OperationKind::Rewrite => "rewrite",
            OperationKind::Reconcile => "reconcile",
            OperationKind::ManifestRewrite => "manifest_rewrite",
            _ => bail!("operation is not maintenance"),
        };
        if record.phase == OperationPhase::Building {
            bail!("incomplete rewrite build must be discarded and replanned");
        }
        if record.phase == OperationPhase::Applied {
            diagnostics.catalog_resolution_path = "already_applied";
            diagnostics.catalog_already_committed = true;
            diagnostics.snapshot_id = record.snapshot_id;
            return Ok(record.snapshot_id);
        }

        let tracker = Some(artifacts::MetadataRegistration::live(
            self.store.clone(),
            table,
            record.operation.table_id,
            record.operation.id.clone(),
        )?);
        let catalog_resolution_started = Instant::now();
        let resolution_attempt = resolve_maintenance_operation(
            &record,
            self.catalog.as_ref(),
            table,
            self.cache.clone(),
            tracker,
        )
        .await;
        diagnostics.catalog_resolution_phase = catalog_resolution_started.elapsed();
        diagnostics.catalog_update_call = resolution_attempt.catalog_update_elapsed;
        diagnostics.catalog_resolution_path = "error";
        let resolved_at = Instant::now();
        diagnostics.catalog_resolve_boundary = resolved_at.duration_since(diagnostics.started);
        let MaintenanceResolution {
            outcome,
            path,
            catalog_already_committed,
        } = resolution_attempt.result?;
        diagnostics.catalog_resolution_path = path;
        diagnostics.catalog_already_committed = catalog_already_committed;

        let Some((snapshot, sequence)) = outcome else {
            let discard_started = Instant::now();
            let store = self.store.clone();
            let id = id.clone();
            let discard = blocking(move || {
                let call_started = Instant::now();
                let result = store.discard_uncommitted(&id);
                Ok((result, call_started.elapsed()))
            })
            .await;
            diagnostics.discard_before_replan_phase = discard_started.elapsed();
            let (discard, discard_call) = discard?;
            diagnostics.discard_uncommitted_call = discard_call;
            discard?;
            return Err(ReplanRequired.into());
        };
        diagnostics.snapshot_id = Some(snapshot);

        diagnostics.mark_committed_required = record.phase == OperationPhase::Prepared;
        if diagnostics.mark_committed_required {
            let mark_committed_started = Instant::now();
            let store = self.store.clone();
            let id = id.clone();
            let mark_committed = blocking(move || {
                let call_started = Instant::now();
                let result = store.mark_committed(&id, snapshot, sequence);
                Ok((result, call_started.elapsed()))
            })
            .await;
            diagnostics.mark_committed_phase = mark_committed_started.elapsed();
            let (mark_committed, mark_committed_call) = mark_committed?;
            diagnostics.mark_committed_call = mark_committed_call;
            mark_committed?;
        }

        let state_apply_started = Instant::now();
        let store = self.store.clone();
        let id = id.clone();
        let state_apply = blocking(move || {
            let call_started = Instant::now();
            let applied = store.apply_committed(&id);
            let call_elapsed = call_started.elapsed();
            let result = applied.and_then(|applied| {
                Ok((
                    store.operation(&id)?.and_then(|record| record.snapshot_id),
                    applied,
                ))
            });
            Ok((result, call_elapsed))
        })
        .await;
        diagnostics.state_apply_phase = state_apply_started.elapsed();
        let (state_apply, state_apply_call) = state_apply?;
        diagnostics.state_apply_call = state_apply_call;
        let (snapshot, applied) = state_apply?;
        diagnostics.snapshot_id = snapshot;
        diagnostics.applied_rows = applied.applied_rows;
        diagnostics.obsolete_rows = applied.obsolete_rows;
        diagnostics.mark_apply_boundary = Instant::now().duration_since(resolved_at);
        diagnostics.completed_apply = true;

        for (phase, elapsed) in [
            ("catalog_resolve", diagnostics.catalog_resolve_boundary),
            ("mark_apply", diagnostics.mark_apply_boundary),
        ] {
            metrics::histogram!("flow_maintenance_phase_seconds", "table_id" => record.operation.table_id.0.to_string(), "phase" => phase)
                .record(elapsed.as_secs_f64());
        }
        Ok(snapshot)
    }

    /// Verify and apply unknown snapshots in ancestry order. Labels only choose
    /// the verification path: physical rewrites must preserve every live key,
    /// row fingerprint, and effective delete on surviving files.
    pub async fn reconcile(
        &self,
        table: &Table,
        schema: &TableSchema,
        scratch: StateStore,
    ) -> Result<Option<i64>> {
        use flow_catalog_watch::{Classification, SnapshotChange, SnapshotOperation, classify};
        let head = self.catalog.load_table(table.identifier()).await?;
        CommitBase::new(&head).validate(&head)?;
        ensure!(
            crate::same_iceberg_schema(head.metadata().current_schema(), &iceberg_schema(schema)?),
            "source schema differs from Iceberg"
        );
        let store = self.store.clone();
        let table_id = schema.table_id;
        let indexed = blocking(move || Ok(store.table_state(&table_id)?)).await?;
        ensure!(
            indexed.pending_operation.is_none(),
            "recover pending index transition before external reconciliation"
        );
        if indexed.snapshot_id == head.metadata().current_snapshot_id() {
            return Ok(indexed.snapshot_id);
        }
        let base = indexed.snapshot_id.ok_or_else(|| {
            anyhow::anyhow!("existing Iceberg table requires a full index rebuild")
        })?;
        let mut ids = Vec::new();
        let mut cursor = head.metadata().current_snapshot_id();
        while cursor != Some(base) {
            let id = cursor.ok_or_else(|| {
                anyhow::anyhow!("indexed snapshot is not an ancestor of the table head")
            })?;
            ensure!(!ids.contains(&id), "snapshot ancestry contains a cycle");
            let snapshot = head
                .metadata()
                .snapshot_by_id(id)
                .ok_or_else(|| anyhow::anyhow!("snapshot history expired; rebuild the index"))?;
            ids.push(id);
            cursor = snapshot.parent_snapshot_id();
        }
        let mut before = SnapshotView::load_with_cache(&head, base, &self.cache).await?;
        for id in ids.into_iter().rev() {
            let target = head
                .metadata()
                .snapshot_by_id(id)
                .ok_or_else(|| anyhow::anyhow!("snapshot disappeared from loaded metadata"))?;
            let after = SnapshotView::load_with_cache(&head, id, &self.cache).await?;
            ensure!(
                before
                    .live_files
                    .values()
                    .chain(after.live_files.values())
                    .all(|entry| entry.content_type() != DataContentType::EqualityDeletes),
                "equality-delete maintenance is unsupported"
            );
            let before_data = file_paths(&before, DataContentType::Data);
            let after_data = file_paths(&after, DataContentType::Data);
            let before_deletes = file_paths(&before, DataContentType::PositionDeletes);
            let after_deletes = file_paths(&after, DataContentType::PositionDeletes);
            let upgrade_range = target.row_range().filter(|_| {
                before
                    .snapshot_id
                    .and_then(|id| head.metadata().snapshot_by_id(id))
                    .is_some_and(|snapshot| snapshot.row_range().is_none())
            });
            for (path, previous) in &before.live_files {
                if let Some(current) = after.live_files.get(path) {
                    ensure!(
                        (previous.data_file == current.data_file
                            || upgrade_range.is_some_and(|(first, count)| {
                                let Some(id) = current
                                    .data_file
                                    .first_row_id()
                                    .and_then(|id| u64::try_from(id).ok())
                                else {
                                    return false;
                                };
                                previous.content_type() == DataContentType::Data
                                    && previous.data_file.first_row_id().is_none()
                                    && id >= first
                                    && id
                                        .checked_add(current.data_file.record_count())
                                        .zip(first.checked_add(count))
                                        .is_some_and(|(end, limit)| end <= limit)
                                    && previous.data_file.clone().with_first_row_id(id as i64)
                                        == current.data_file
                            }))
                            && previous.sequence_number == current.sequence_number
                            && previous.file_sequence_number == current.file_sequence_number,
                        "external snapshot reused immutable file identity {path}"
                    );
                }
            }
            let change = SnapshotChange {
                snapshot_id: id,
                parent_snapshot_id: target.parent_snapshot_id(),
                operation: match target.summary().operation {
                    iceberg::spec::Operation::Append => SnapshotOperation::Append,
                    iceberg::spec::Operation::Overwrite => SnapshotOperation::Overwrite,
                    iceberg::spec::Operation::Replace => SnapshotOperation::Replace,
                    iceberg::spec::Operation::Delete => SnapshotOperation::Delete,
                },
                service_operation: target
                    .summary()
                    .additional_properties
                    .get(OPERATION_ID_KEY)
                    .cloned()
                    .map(OperationId),
                added_data: after_data.difference(&before_data).cloned().collect(),
                removed_data: before_data.difference(&after_data).cloned().collect(),
                added_deletes: after_deletes.difference(&before_deletes).cloned().collect(),
                removed_deletes: before_deletes.difference(&after_deletes).cloned().collect(),
                schema_changed: !schema_transition_is_supported(
                    head.metadata(),
                    before
                        .snapshot_id
                        .and_then(|id| head.metadata().snapshot_by_id(id))
                        .and_then(|snapshot| snapshot.schema_id()),
                    target.schema_id(),
                ),
                spec_changed: false,
            };
            let classification = classify(&change, before.snapshot_id, false);
            match classification {
                Classification::Pause(reason) => {
                    bail!("external snapshot {id} paused publication: {reason:?}")
                }
                Classification::ServiceOperation => {
                    bail!("recover the service operation before reconciliation")
                }
                _ => {}
            }
            let operation_id = OperationId(format!("reconcile-{}-{id}", head.metadata().uuid()));
            let recovery = ReconcileRecovery {
                table_uuid: head.metadata().uuid(),
                snapshot_id: id,
                sequence_number: target.sequence_number(),
            };
            let operation = PreparedOperation {
                id: operation_id.clone(),
                table_id: schema.table_id,
                kind: OperationKind::Reconcile,
                base_snapshot_id: before.snapshot_id,
                last_lsn: indexed.materialized_lsn,
                schema_version: schema.version,
                artifacts: Vec::new(),
                payload: serde_json::to_vec(&recovery)?,
            };
            if matches!(
                classification,
                Classification::VerifyDeleteRewrite | Classification::ReconcileDataRewrite
            ) {
                let survivors: BTreeSet<_> =
                    before_data.intersection(&after_data).cloned().collect();
                let old_scan = format!("{}-before", operation_id.0);
                let new_scan = format!("{}-after", operation_id.0);
                let scan_table = head.clone();
                let scan_schema = schema.clone();
                let old = before.clone();
                let new = after.clone();
                let scratch = scratch.clone();
                let read_limits = self.read_limits.clone();
                tokio::task::spawn_blocking(move || -> Result<()> {
                    let runtime = worker_runtime()?;
                    runtime.block_on(flow_compactor::stage_position_deletes(
                        &scan_table,
                        &scan_schema,
                        &old,
                        &survivors,
                        &scratch,
                        &old_scan,
                        scratch.batch_rows(),
                        &read_limits,
                    ))?;
                    runtime.block_on(flow_compactor::stage_position_deletes(
                        &scan_table,
                        &scan_schema,
                        &new,
                        &survivors,
                        &scratch,
                        &new_scan,
                        scratch.batch_rows(),
                        &read_limits,
                    ))?;
                    let position = |item: flow_state_store::Result<RowLocation>| {
                        item.map(|location| (location.data_file_id, location.row_position))
                    };
                    flow_catalog_watch::validate_delete_rewrite_fallible(
                        scratch
                            .position_deletes(&old_scan, &scan_schema.table_id)
                            .map(position),
                        scratch
                            .position_deletes(&new_scan, &scan_schema.table_id)
                            .map(position),
                    )?;
                    scratch.discard_transaction(&old_scan)?;
                    scratch.discard_transaction(&new_scan)?;
                    Ok(())
                })
                .await??;
            }
            if classification == Classification::ReconcileDataRewrite {
                ensure!(
                    !schema.primary_key.is_empty() || schema.append_only,
                    "mutable table reconciliation requires a primary key"
                );
                let scan_table = head.clone();
                let scan_schema = schema.clone();
                let view = after.clone();
                let files = change.added_data.clone();
                let scan_scratch = scratch.clone();
                let scan_id = operation_id.clone();
                let read_limits = self.read_limits.clone();
                tokio::task::spawn_blocking(move || -> Result<()> {
                    scan_scratch.begin_prepare(PreparedOperation {
                        id: scan_id.clone(),
                        table_id: scan_schema.table_id,
                        kind: OperationKind::Rebuild,
                        base_snapshot_id: None,
                        last_lsn: PgLsn(0),
                        schema_version: scan_schema.version,
                        artifacts: Vec::new(),
                        payload: Vec::new(),
                    })?;
                    worker_runtime()?.block_on(flow_compactor::scan_live_files(
                        &scan_table,
                        &scan_schema,
                        &view,
                        &files,
                        &scan_scratch,
                        &scan_id.0,
                        scan_scratch.batch_rows(),
                        &read_limits,
                        async |batch| {
                            let deltas = index_rows(
                                &scan_schema,
                                &batch,
                                &view,
                                scan_table.metadata().default_partition_spec_id(),
                                PgLsn(0),
                            )?;
                            scan_scratch.stage_deltas(&scan_id, deltas)?;
                            Ok(())
                        },
                    ))?;
                    scan_scratch.seal_prepare(&scan_id, Vec::new(), Vec::new())?;
                    Ok(())
                })
                .await??;
            }
            let refreshed = self.catalog.load_table(head.identifier()).await?;
            ensure!(
                refreshed.metadata().uuid() == head.metadata().uuid()
                    && refreshed.metadata().current_snapshot_id()
                        == head.metadata().current_snapshot_id()
                    && refreshed.metadata().current_schema_id()
                        == head.metadata().current_schema_id(),
                "table changed during external reconciliation; retry"
            );
            let store = self.store.clone();
            let scratch = scratch.clone();
            let sequence = target.sequence_number();
            let keyless = schema.primary_key.is_empty();
            let reconciliation_kind = match classification {
                Classification::MetadataOnly => "metadata-only",
                Classification::VerifyDeleteRewrite => "delete-rewrite",
                Classification::ReconcileDataRewrite => "data-rewrite",
                Classification::ServiceOperation | Classification::Pause(_) => unreachable!(),
            };
            blocking(move || {
                if classification == Classification::ReconcileDataRewrite {
                    let rows = scratch.prepared_deltas(&operation_id)?.map(|delta| {
                        delta.and_then(|delta| {
                            let location = delta.replacement.ok_or_else(|| {
                                flow_state_store::Error::InvalidState(
                                    "scanned row lacks location".into(),
                                )
                            })?;
                            Ok((delta.key, location))
                        })
                    });
                    let old_scan = format!("{}-keyless-before", operation_id.0);
                    let new_scan = format!("{}-keyless-after", operation_id.0);
                    type ScannedRows<'a> = Box<
                        dyn Iterator<Item = flow_state_store::Result<(PrimaryKey, RowLocation)>>
                            + 'a,
                    >;
                    let rows: ScannedRows<'_> = if keyless {
                        Box::new(flow_catalog_watch::match_append_only_rows(
                            &store,
                            &scratch,
                            (&old_scan, &new_scan),
                            operation.table_id,
                            &change.removed_data,
                            rows,
                        )?)
                    } else {
                        Box::new(rows)
                    };
                    flow_catalog_watch::reconcile_rewrite(
                        &store,
                        operation,
                        id,
                        sequence,
                        &change.removed_data,
                        &change.added_data,
                        rows,
                    )?;
                    if keyless {
                        scratch.discard_transaction(&old_scan)?;
                        scratch.discard_transaction(&new_scan)?;
                    }
                    scratch.discard_uncommitted(&operation_id)?;
                    scratch.discard_transaction(&operation_id.0)?;
                } else {
                    store.prepare(operation, [])?;
                    store.mark_committed(&operation_id, id, sequence)?;
                    store.apply_committed(&operation_id)?;
                }
                store.forget_applied(&operation_id)?;
                Ok(())
            })
            .await?;
            metrics::counter!(
                "flow_external_reconciliations_total",
                "table_id" => table_id.0.to_string(),
                "kind" => reconciliation_kind
            )
            .increment(1);
            tracing::info!(
                event = "external_snapshot_reconciled",
                table_id = table_id.0,
                snapshot_id = id,
                sequence_number = sequence,
                reconciliation_kind,
                "external snapshot verified and durably applied to the row index"
            );
            before = after;
        }
        Ok(head.metadata().current_snapshot_id())
    }

    /// Reconstruct a fresh index from ordinary Iceberg files. The caller provides
    /// the durable source watermark and swaps the returned store into service
    /// only after this succeeds. The existing index remains untouched on error.
    pub async fn rebuild_index(
        &self,
        table: &Table,
        schema: &TableSchema,
        replacement: StateStore,
        scratch: StateStore,
        materialized_lsn: PgLsn,
        materialized_schema_version: u32,
    ) -> Result<StateStore> {
        let head = self.catalog.load_table(table.identifier()).await?;
        CommitBase::new(&head).validate(&head)?;
        ensure!(
            crate::same_iceberg_schema(head.metadata().current_schema(), &iceberg_schema(schema)?),
            "source schema differs from Iceberg"
        );
        let view = SnapshotView::current_with_cache(&head, &self.cache).await?;
        ensure!(
            view.live_files
                .values()
                .all(|entry| entry.content_type() != DataContentType::EqualityDeletes),
            "index rebuild requires position-delete state"
        );
        let id = OperationId(format!("rebuild-{}", uuid::Uuid::new_v4()));
        let files = view
            .live_files
            .values()
            .filter(|entry| entry.content_type() == DataContentType::Data)
            .map(|entry| FileId(flow_iceberg_ext::content_file_id(&entry.data_file)))
            .collect();
        let worker_table = head.clone();
        let table_id = schema.table_id;
        let version = materialized_schema_version;
        let schema = schema.clone();
        let rebuilt = replacement.clone();
        let worker_id = id.clone();
        let read_limits = self.read_limits.clone();
        tokio::task::spawn_blocking(move || -> Result<()> {
            ensure!(
                rebuilt.index_is_empty(&schema.table_id)?
                    && rebuilt.table_state(&schema.table_id)?.snapshot_id.is_none(),
                "replacement index must be empty"
            );
            rebuilt.begin_prepare(PreparedOperation {
                id: worker_id.clone(),
                table_id: schema.table_id,
                kind: OperationKind::Rebuild,
                base_snapshot_id: None,
                last_lsn: materialized_lsn,
                schema_version: materialized_schema_version,
                artifacts: Vec::new(),
                payload: Vec::new(),
            })?;
            worker_runtime()?.block_on(flow_compactor::scan_live_files(
                &worker_table,
                &schema,
                &view,
                &files,
                &scratch,
                &worker_id.0,
                rebuilt.batch_rows(),
                &read_limits,
                async |batch| {
                    let deltas = index_rows(
                        &schema,
                        &batch,
                        &view,
                        worker_table.metadata().default_partition_spec_id(),
                        materialized_lsn,
                    )?;
                    rebuilt.stage_deltas(&worker_id, deltas)?;
                    Ok(())
                },
            ))?;
            rebuilt.seal_prepare(&worker_id, Vec::new(), Vec::new())?;
            Ok(())
        })
        .await??;
        let refreshed = self.catalog.load_table(head.identifier()).await?;
        ensure!(
            refreshed.metadata().uuid() == head.metadata().uuid()
                && refreshed.metadata().current_snapshot_id()
                    == head.metadata().current_snapshot_id()
                && refreshed.metadata().current_schema_id() == head.metadata().current_schema_id(),
            "table changed during rebuild; retry against a fresh snapshot"
        );
        if let Some(snapshot) = head.metadata().current_snapshot() {
            let rebuilt = replacement.clone();
            let snapshot_id = snapshot.snapshot_id();
            let sequence = snapshot.sequence_number();
            blocking(move || {
                rebuilt.mark_committed(&id, snapshot_id, sequence)?;
                rebuilt.apply_committed(&id)?;
                rebuilt.forget_applied(&id)?;
                Ok(())
            })
            .await?;
        } else {
            let rebuilt = replacement.clone();
            blocking(move || {
                rebuilt.discard_uncommitted(&id)?;
                rebuilt.complete_noop(&table_id, materialized_lsn, version)?;
                Ok(())
            })
            .await?;
        }
        Ok(replacement)
    }
}

fn file_paths(view: &SnapshotView, content: DataContentType) -> BTreeSet<FileId> {
    view.live_files
        .values()
        .filter(|entry| entry.content_type() == content)
        .map(|entry| FileId(flow_iceberg_ext::content_file_id(&entry.data_file)))
        .collect()
}

fn schema_transition_is_supported(
    metadata: &iceberg::spec::TableMetadata,
    before: Option<i32>,
    after: Option<i32>,
) -> bool {
    if before == after {
        return true;
    }
    let (Some(before), Some(after)) = (
        before.and_then(|id| metadata.schema_by_id(id)),
        after.and_then(|id| metadata.schema_by_id(id)),
    ) else {
        return false;
    };
    // A source-owned nullable addition updates table metadata before the next
    // snapshot. An external compactor may publish that next snapshot first.
    // Both schemas must stay within the already-validated source schema.
    let nullable_extension = |old: &iceberg::spec::Schema, new: &iceberg::spec::Schema| {
        let old_fields = old.as_struct().fields();
        let new_fields = new.as_struct().fields();
        new_fields.starts_with(old_fields)
            && new_fields[old_fields.len()..].iter().all(|field| {
                !field.required && field.initial_default.is_none() && field.write_default.is_none()
            })
            && old.identifier_field_ids().collect::<BTreeSet<_>>()
                == new.identifier_field_ids().collect::<BTreeSet<_>>()
    };
    nullable_extension(before, after) && nullable_extension(after, metadata.current_schema())
}

fn index_rows(
    schema: &TableSchema,
    batch: &flow_compactor::LiveBatch,
    view: &SnapshotView,
    spec_id: i32,
    last_lsn: PgLsn,
) -> Result<Vec<IndexDelta>> {
    let entry = &view.live_files[&batch.file_id.0];
    batch
        .rows
        .iter()
        .zip(&batch.positions)
        .map(|(row, &position)| {
            let key = if schema.primary_key.is_empty() {
                ensure!(schema.append_only, "mutable table requires a primary key");
                // This namespace cannot overlap ingestion's ASCII epoch identifiers.
                let mut bytes = vec![0xff, b'R'];
                bytes.extend_from_slice(&(batch.file_id.0.len() as u64).to_be_bytes());
                bytes.extend_from_slice(batch.file_id.0.as_bytes());
                bytes.extend_from_slice(&position.to_be_bytes());
                PrimaryKey(bytes)
            } else {
                schema.encode_key(row)?
            };
            Ok(IndexDelta {
                key,
                expected: None,
                replacement: Some(RowLocation {
                    data_file_id: batch.file_id.clone(),
                    row_position: position,
                    data_sequence_number: entry
                        .sequence_number
                        .ok_or_else(|| anyhow::anyhow!("missing data sequence"))?,
                    spec_id,
                    partition: Vec::new(),
                    source_commit_lsn: last_lsn,
                    row_version: 0,
                    row_fingerprint: schema.fingerprint(row)?,
                }),
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TraceWriter(Arc<std::sync::Mutex<Vec<u8>>>);

    impl std::io::Write for TraceWriter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn recovery_diagnostics_identify_every_terminal_outcome() {
        let trace = Arc::new(std::sync::Mutex::new(Vec::new()));
        let output = trace.clone();
        let subscriber = tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_target(false)
            .with_writer(move || TraceWriter(output.clone()))
            .finish();

        tracing::subscriber::with_default(subscriber, || {
            let mut success = RecoveryDiagnostics::new();
            success.record_found = true;
            success.table_id = 7;
            success.kind = "rewrite";
            success.phase_before = Some(OperationPhase::Prepared);
            success.catalog_resolution_path = "catalog_updated";
            success.catalog_update_call = Some(Duration::from_millis(2));
            success.mark_committed_required = true;
            success.mark_committed_phase = Duration::from_millis(3);
            success.mark_committed_call = Duration::from_millis(2);
            success.completed_apply = true;
            success.emit_result(&OperationId("terminal-success".into()), &Ok(Some(101)));

            let mut replan = RecoveryDiagnostics::new();
            replan.record_found = true;
            replan.table_id = 7;
            replan.kind = "rewrite";
            replan.discard_before_replan_phase = Duration::from_millis(4);
            replan.discard_uncommitted_call = Duration::from_millis(3);
            let result: Result<Option<i64>> = Err(ReplanRequired.into());
            replan.emit_result(&OperationId("terminal-replan".into()), &result);

            let failure: Result<Option<i64>> = Err(anyhow::anyhow!("catalog unavailable"));
            RecoveryDiagnostics::new().emit_result(&OperationId("terminal-error".into()), &failure);
        });

        let trace = String::from_utf8(trace.lock().unwrap().clone()).unwrap();
        let terminal = |operation: &str| {
            trace
                .lines()
                .filter(|line| {
                    line.contains("event=\"maintenance_recovery_terminal\"")
                        && line.contains(operation)
                })
                .collect::<Vec<_>>()
        };
        let success = terminal("operation_id=terminal-success");
        let replan = terminal("operation_id=terminal-replan");
        let error = terminal("operation_id=terminal-error");
        assert_eq!(success.len(), 1, "{trace}");
        assert_eq!(replan.len(), 1, "{trace}");
        assert_eq!(error.len(), 1, "{trace}");
        assert!(
            success[0].contains("outcome=\"success\"")
                && success[0].contains("catalog_update_attempted=true")
                && success[0].contains("mark_committed_call_ms="),
            "{trace}"
        );
        assert!(
            replan[0].contains("outcome=\"replan\"")
                && replan[0].contains("discard_before_replan_phase_ms=")
                && replan[0].contains("discard_uncommitted_call_ms="),
            "{trace}"
        );
        assert!(
            error[0].contains("outcome=\"error\"")
                && error[0].contains("error=\"catalog unavailable\""),
            "{trace}"
        );
        assert_eq!(
            trace
                .lines()
                .filter(|line| line.contains("event=\"maintenance_recovered\""))
                .count(),
            1,
            "{trace}"
        );
    }

    #[test]
    fn pooled_http_survives_worker_completion_and_parent_runtime_shutdown() -> Result<()> {
        use std::io::{BufRead, BufReader, Write};
        use std::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0")?;
        listener.set_nonblocking(true)?;
        let url = format!("http://{}", listener.local_addr()?);
        let (held, request_held) = std::sync::mpsc::channel();
        let (respond, response_allowed) = std::sync::mpsc::channel();
        let server = std::thread::spawn(move || -> Result<()> {
            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            let mut stream = loop {
                match listener.accept() {
                    Ok((stream, _)) => break stream,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        ensure!(
                            std::time::Instant::now() < deadline,
                            "HTTP accept timed out"
                        );
                        std::thread::sleep(Duration::from_millis(1));
                    }
                    Err(error) => return Err(error.into()),
                }
            };
            stream.set_nonblocking(false)?;
            stream.set_read_timeout(Some(Duration::from_secs(5)))?;
            stream.set_write_timeout(Some(Duration::from_secs(5)))?;
            let mut input = BufReader::new(stream.try_clone()?);
            // Accept only one TCP connection: the second request must reuse the
            // first worker's actual OpenDAL/reqwest pooled connection.
            for path in ["/first", "/held"] {
                let mut line = String::new();
                input.read_line(&mut line)?;
                ensure!(
                    line == format!("GET {path} HTTP/1.1\r\n"),
                    "unexpected request: {line:?}"
                );
                loop {
                    line.clear();
                    ensure!(input.read_line(&mut line)? > 0, "request headers truncated");
                    if line == "\r\n" {
                        break;
                    }
                }
                if path == "/held" {
                    held.send(())?;
                    response_allowed.recv_timeout(Duration::from_secs(5))?;
                }
                stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")?;
            }
            Ok(())
        });
        let parent = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()?;
        let (entered, started) = std::sync::mpsc::channel();
        let (release_first, hold_first) = tokio::sync::oneshot::channel();
        let first_url = format!("{url}/first");
        let first = parent.spawn_blocking(move || {
            worker_runtime()?.block_on(async move {
                let body = opendal::raw::GLOBAL_REQWEST_CLIENT
                    .get(first_url)
                    .send()
                    .await?
                    .bytes()
                    .await?;
                ensure!(body == "ok");
                entered.send(())?;
                hold_first.await?;
                Ok::<_, anyhow::Error>(())
            })
        });
        if let Err(error) = started.recv_timeout(Duration::from_secs(5)) {
            parent.block_on(async {
                tokio::time::timeout(Duration::from_secs(5), first).await
            })???;
            return Err(error.into());
        }
        let worker = parent.spawn_blocking(move || {
            worker_runtime()?.block_on(async move {
                let body = opendal::raw::GLOBAL_REQWEST_CLIENT
                    .get(format!("{url}/held"))
                    .send()
                    .await?
                    .bytes()
                    .await?;
                ensure!(body == "ok");
                // Retry timers must also remain available after caller shutdown.
                tokio::time::sleep(Duration::from_millis(1)).await;
                Ok::<_, anyhow::Error>(())
            })
        });
        request_held.recv_timeout(Duration::from_secs(5))?;
        release_first.send(()).expect("first worker still held");
        parent
            .block_on(async { tokio::time::timeout(Duration::from_secs(5), first).await })???;
        parent.shutdown_timeout(Duration::from_millis(1));
        assert!(
            !worker.is_finished(),
            "runtime shutdown is not a worker join"
        );
        respond.send(())?;
        worker_runtime()?.block_on(async {
            tokio::time::timeout(Duration::from_secs(5), worker).await???;
            Ok::<_, anyhow::Error>(())
        })?;
        server.join().expect("HTTP server thread panicked")
    }
}
