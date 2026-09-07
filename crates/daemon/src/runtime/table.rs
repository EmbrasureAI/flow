//! Execute one table's publication or maintenance visit under scheduler ownership.

use super::{BUILD_MAX_AGE, OPTIONAL_MAINTENANCE_DELAY};
use crate::config::Config;
use anyhow::{Context, Result, bail, ensure};
use flow_coordinator::{
    CollapseLimits, DeleteRepairCursor, Epoch, PreparedCompaction, ReadyCompaction, ReplanRequired,
    RunningCompaction, RunningCompactionPreparation, TableMaintenance, TablePublisher,
    collapse_epoch,
};
use flow_ingress_journal::JournalReader;
use flow_model::{OperationId, SourceId, SourceTransaction, TableId, TableSchema};
use flow_state_store::{
    ControlStore, OperationKind, OperationPhase, StateStore, StateStoreOptions,
};
use iceberg::{Catalog, table::Table};
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

fn history_due(table: &Table, retention_seconds: u64) -> Result<bool> {
    if table.metadata().snapshots().len() < 128 {
        return Ok(false);
    }
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
    let cutoff = now.saturating_sub(u128::from(retention_seconds) * 1000);
    Ok(table
        .metadata()
        .snapshots()
        .any(|s| s.timestamp_ms() >= 0 && (s.timestamp_ms() as u128) < cutoff))
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum BuildAdmission {
    Fenced,
    Wait,
    Start,
    Probe,
}

#[derive(Clone, Copy)]
pub(super) struct WorkOptions {
    pub(super) periodic_maintenance: bool,
    pub(super) build_active: bool,
    pub(super) build_admission: BuildAdmission,
    pub(super) actor_acquired_at: Instant,
}

pub(super) struct StartedBuild {
    pub(super) running: RunningCompaction,
    pub(super) path: PathBuf,
    pub(super) started: Instant,
}

pub(super) struct CompletedBuild {
    pub(super) ready: ReadyCompaction,
    pub(super) path: PathBuf,
    pub(super) started: Instant,
}

pub(super) struct StartedPreparation {
    pub(super) running: RunningCompactionPreparation,
    pub(super) path: PathBuf,
    pub(super) started: Instant,
    pub(super) activation_stall_started: Instant,
}

pub(super) struct CompletedPreparation {
    pub(super) prepared: PreparedCompaction,
    pub(super) path: PathBuf,
    pub(super) started: Instant,
    pub(super) activation_stall_started: Instant,
}

pub(super) enum CompactionCandidate {
    Built(CompletedBuild),
    Prepared(CompletedPreparation),
}

impl CompactionCandidate {
    fn path(&self) -> &PathBuf {
        match self {
            Self::Built(candidate) => &candidate.path,
            Self::Prepared(candidate) => &candidate.path,
        }
    }

    pub(super) fn operation_id(&self) -> &OperationId {
        match self {
            Self::Built(candidate) => candidate.ready.operation_id(),
            Self::Prepared(candidate) => candidate.prepared.operation_id(),
        }
    }

    pub(super) fn is_prepared(&self) -> bool {
        matches!(self, Self::Prepared(_))
    }

    pub(super) fn activation_stall_started(&self) -> Option<Instant> {
        match self {
            Self::Built(_) => None,
            Self::Prepared(candidate) => Some(candidate.activation_stall_started),
        }
    }

    async fn discard(self) -> Result<()> {
        match self {
            Self::Built(candidate) => candidate.ready.discard().await,
            Self::Prepared(candidate) => candidate.prepared.discard().await,
        }
    }
}

pub(super) struct MaintenancePending {
    pub(super) data: bool,
    pub(super) periodic: bool,
}

pub(super) enum TableOutcome {
    Complete {
        snapshot: Option<i64>,
        maintenance_pending: MaintenancePending,
    },
    Build(Box<StartedBuild>),
    ProbeComplete,
    PeriodicComplete {
        next_due: Option<Instant>,
    },
    Preparation(Box<StartedPreparation>),
    CandidateInvalidated,
    Deferred,
}

enum MaintenanceOutcome {
    Complete(Table, MaintenancePending),
    Build(StartedBuild),
    Deferred,
}

enum CompactionKind<'a> {
    Data,
    Deletes,
    DependencyRepair(&'a mut DeleteRepairCursor),
}

pub(super) struct TableCompletion {
    pub(super) id: TableId,
    pub(super) transactions: Vec<SourceTransaction>,
    pub(super) outcome: TableOutcome,
    pub(super) elapsed: Duration,
    pub(super) reserved_build: bool,
    pub(super) finalization_lane: Option<FinalizationLane>,
}

pub(super) struct FinalizationLane {
    pub(super) operation_id: OperationId,
    pub(super) acquired_at: Instant,
    pub(super) publication_stall_started: Instant,
}

impl FinalizationLane {
    pub(super) fn record_release(self, table_id: TableId, released_at: Instant) -> Duration {
        let held = released_at.saturating_duration_since(self.acquired_at);
        let publication_stall =
            released_at.saturating_duration_since(self.publication_stall_started);
        metrics::histogram!(
            "flow_compaction_finalization_actor_lane_hold_seconds",
            "table_id" => table_id.0.to_string()
        )
        .record(held.as_secs_f64());
        metrics::histogram!(
            "flow_compaction_publication_stall_seconds",
            "table_id" => table_id.0.to_string()
        )
        .record(publication_stall.as_secs_f64());
        tracing::info!(
            event = "compaction_finalization_actor_lane_released",
            operation_id = %self.operation_id.0,
            table_id = table_id.0,
            lane_hold_ms = held.as_secs_f64() * 1000.0,
            publication_stall_ms = publication_stall.as_secs_f64() * 1000.0,
            "completed compaction finalization released the table actor lane"
        );
        held
    }
}

pub(super) fn retry_table_work(error: &anyhow::Error) -> bool {
    error.downcast_ref::<ReplanRequired>().is_some()
        || crate::retry::transient(error)
        || matches!(
            error.downcast_ref::<flow_compactor::Error>(),
            Some(
                flow_compactor::Error::MaintenanceRequired
                    | flow_compactor::Error::DependencyBudget
            )
        )
}

fn candidate_invalidation_reason(error: &anyhow::Error) -> &'static str {
    if error
        .downcast_ref::<flow_state_store::Error>()
        .is_some_and(|error| matches!(error, flow_state_store::Error::ExactStateMismatch { .. }))
    {
        "index_head"
    } else if error
        .downcast_ref::<flow_state_store::Error>()
        .is_some_and(|error| matches!(error, flow_state_store::Error::SnapshotMismatch { .. }))
    {
        "snapshot"
    } else if error
        .downcast_ref::<flow_compactor::CatchUpRejected>()
        .is_some()
    {
        "catch_up"
    } else {
        "catalog_or_schema"
    }
}

#[derive(Clone)]
pub(super) struct TableWork {
    pub(super) config: Config,
    pub(super) store: StateStore,
    pub(super) control: ControlStore,
    pub(super) reader: JournalReader,
    pub(super) catalog: Arc<dyn Catalog>,
    pub(super) publisher: Arc<TablePublisher>,
    pub(super) maintenance: Arc<TableMaintenance>,
    pub(super) garbage_checked: Arc<Mutex<BTreeMap<TableId, Instant>>>,
    pub(super) compaction: bool,
}
impl TableWork {
    async fn start_build(
        &self,
        table: &Table,
        schema: &TableSchema,
    ) -> Result<Option<StartedBuild>> {
        let path = self
            .config
            .state_dir
            .join("compaction")
            .join(uuid::Uuid::new_v4().to_string());
        let scratch = StateStore::open(
            &path,
            StateStoreOptions {
                apply_batch_rows: self.config.limits.batch_rows,
                ..Default::default()
            },
        )?;
        let started = Instant::now();
        match self
            .maintenance
            .start_compaction(table, schema, scratch)
            .await
        {
            Ok(Some(running)) => Ok(Some(StartedBuild {
                running,
                path,
                started,
            })),
            result => {
                std::fs::remove_dir_all(path)?;
                result.map(|_| None)
            }
        }
    }

    async fn compact(
        &self,
        table: &Table,
        schema: &TableSchema,
        kind: CompactionKind<'_>,
    ) -> Result<Option<i64>> {
        let path = self
            .config
            .state_dir
            .join("compaction")
            .join(uuid::Uuid::new_v4().to_string());
        let scratch = StateStore::open(
            &path,
            StateStoreOptions {
                apply_batch_rows: self.config.limits.batch_rows,
                ..Default::default()
            },
        )?;
        let started = Instant::now();
        let kind_name = match &kind {
            CompactionKind::Data => "data",
            _ => "delete",
        };
        let result = match kind {
            CompactionKind::Deletes => {
                let mut policy = flow_coordinator::DeleteRewritePolicy::default();
                policy.min_input_files = self
                    .config
                    .compaction
                    .delete_files_soft
                    .min(policy.max_input_files);
                self.maintenance
                    .compact_deletes(table, schema, scratch, &policy)
                    .await
            }
            CompactionKind::Data => self.maintenance.compact(table, schema, scratch).await,
            CompactionKind::DependencyRepair(cursor) => {
                self.maintenance
                    .repair_delete_dependencies(table, schema, scratch, cursor)
                    .await
            }
        };
        let cleanup = std::fs::remove_dir_all(path);
        let snapshot = result?;
        cleanup?;
        if snapshot.is_some() {
            metrics::histogram!("flow_compaction_seconds", "table_id" => schema.table_id.0.to_string(), "kind" => kind_name)
                .record(started.elapsed().as_secs_f64());
            metrics::counter!("flow_compactions_total", "table_id" => schema.table_id.0.to_string(), "kind" => kind_name)
                .increment(1);
        }
        Ok(snapshot)
    }

    async fn maintain_data(
        &self,
        table: &Table,
        schema: &TableSchema,
        background: bool,
        repair_cursor: &mut DeleteRepairCursor,
    ) -> Result<Option<StartedBuild>> {
        let result = if background {
            self.start_build(table, schema).await
        } else {
            self.compact(table, schema, CompactionKind::Data)
                .await
                .map(|_| None)
        };
        match result {
            Err(error)
                if error.downcast_ref::<flow_compactor::Error>()
                    == Some(&flow_compactor::Error::DependencyBudget) =>
            {
                // Recovery owns any already-started operation. Otherwise a
                // bounded physical delete rewrite may unblock the data plan.
                if self
                    .store
                    .table_state(&schema.table_id)?
                    .pending_operation
                    .is_some()
                    || self
                        .compact(
                            table,
                            schema,
                            CompactionKind::DependencyRepair(repair_cursor),
                        )
                        .await?
                        .is_none()
                {
                    return Err(error);
                }
                Ok(None)
            }
            result => result,
        }
    }

    pub(super) async fn run(
        self,
        table: Table,
        schema: TableSchema,
        transactions: Vec<SourceTransaction>,
        options: WorkOptions,
        mut candidate: Option<CompactionCandidate>,
        finalization_lane: Option<FinalizationLane>,
    ) -> Result<TableCompletion> {
        let started = Instant::now();
        let mut attempt = 0u32;
        let mut repair_cursor = DeleteRepairCursor::default();
        let scratch_path = candidate.as_ref().map(|candidate| candidate.path().clone());
        let reserved_build = candidate.is_some()
            || matches!(
                options.build_admission,
                BuildAdmission::Start | BuildAdmission::Probe
            );
        let result = loop {
            let candidate_context = candidate.as_ref().map(|candidate| {
                (
                    if candidate.is_prepared() {
                        "activation"
                    } else {
                        "preparation"
                    },
                    candidate.operation_id().clone(),
                )
            });
            match self
                .clone()
                .run_once(
                    table.clone(),
                    schema.clone(),
                    transactions.clone(),
                    options,
                    &mut candidate,
                    &mut repair_cursor,
                )
                .await
            {
                Err(error)
                    if options.periodic_maintenance
                        && retry_table_work(&error)
                        && self
                            .store
                            .table_state(&schema.table_id)?
                            .pending_operation
                            .is_none() =>
                {
                    tracing::debug!(table = ?schema.table_id, %error,
                        "periodic maintenance yielded after a transient failure");
                    break Ok(TableCompletion {
                        id: schema.table_id,
                        transactions,
                        outcome: TableOutcome::PeriodicComplete {
                            next_due: Some(Instant::now() + OPTIONAL_MAINTENANCE_DELAY),
                        },
                        elapsed: started.elapsed(),
                        reserved_build,
                        finalization_lane,
                    });
                }
                Err(error)
                    if options.build_admission == BuildAdmission::Probe
                        && retry_table_work(&error) =>
                {
                    tracing::debug!(table = ?schema.table_id, %error,
                        "background build probe yielded to normal table work");
                    break Ok(TableCompletion {
                        id: schema.table_id,
                        transactions,
                        outcome: TableOutcome::ProbeComplete,
                        elapsed: started.elapsed(),
                        reserved_build,
                        finalization_lane,
                    });
                }
                Err(error) if candidate_context.is_some() && retry_table_work(&error) => {
                    let (stage, operation_id) =
                        candidate_context.expect("checked candidate context");
                    if self.store.operation(&operation_id)?.is_some() {
                        let delay = crate::retry::delay(attempt);
                        metrics::counter!("flow_table_retries_total", "table_id" => schema.table_id.0.to_string()).increment(1);
                        tracing::warn!(
                            table = ?schema.table_id,
                            operation_id = %operation_id.0,
                            attempt,
                            delay_ms = delay.as_millis(),
                            error = ?error,
                            "durable compaction activation interrupted; recovering before releasing the table lane"
                        );
                        tokio::time::sleep(delay).await;
                        attempt = attempt.saturating_add(1);
                        continue;
                    }
                    let reason = candidate_invalidation_reason(&error);
                    metrics::counter!(
                        "flow_compaction_candidate_invalidations_total",
                        "table_id" => schema.table_id.0.to_string(),
                        "stage" => stage,
                        "reason" => reason
                    )
                    .increment(1);
                    if stage == "activation" {
                        metrics::counter!(
                            "flow_compaction_activations_total",
                            "table_id" => schema.table_id.0.to_string(),
                            "result" => "invalidated"
                        )
                        .increment(1);
                    }
                    tracing::warn!(
                        event = "compaction_candidate_invalidated",
                        table = ?schema.table_id,
                        stage,
                        reason,
                        error = ?error,
                        "retired stale compaction candidate before rescheduling"
                    );
                    break Ok(TableCompletion {
                        id: schema.table_id,
                        transactions,
                        outcome: TableOutcome::CandidateInvalidated,
                        elapsed: started.elapsed(),
                        reserved_build,
                        finalization_lane,
                    });
                }
                Err(error) if retry_table_work(&error) => {
                    let delay = crate::retry::delay(attempt);
                    metrics::counter!("flow_table_retries_total", "table_id" => schema.table_id.0.to_string()).increment(1);
                    tracing::warn!(table = ?schema.table_id, attempt, delay_ms = delay.as_millis(), %error,
                        "table work interrupted; recovering the durable operation before retrying");
                    tokio::time::sleep(delay).await;
                    attempt = attempt.saturating_add(1);
                }
                result => {
                    break result.map(|outcome| TableCompletion {
                        id: schema.table_id,
                        transactions,
                        outcome,
                        elapsed: started.elapsed(),
                        reserved_build,
                        finalization_lane,
                    });
                }
            }
        };
        let preparation_handoff = result
            .as_ref()
            .is_ok_and(|completion| matches!(completion.outcome, TableOutcome::Preparation(_)));
        // A candidate's workers have already joined. If table refresh failed
        // before ownership was transferred, retire its independent BUILD record.
        if let Some(candidate) = candidate {
            candidate.discard().await?;
        }
        if !preparation_handoff && let Some(path) = scratch_path {
            std::fs::remove_dir_all(path)?;
        }
        result
    }

    async fn run_once(
        self,
        table: Table,
        schema: TableSchema,
        transactions: Vec<SourceTransaction>,
        options: WorkOptions,
        candidate: &mut Option<CompactionCandidate>,
        repair_cursor: &mut DeleteRepairCursor,
    ) -> Result<TableOutcome> {
        let id = schema.table_id;
        if options.build_admission == BuildAdmission::Probe {
            ensure!(
                transactions.is_empty() && candidate.is_none(),
                "build probe contains publication work"
            );
            return self.probe_build(&table, schema).await;
        }
        if let Some(operation) = self.store.table_state(&id)?.pending_operation {
            let record = self
                .store
                .operation(&operation)?
                .context("table fence has no operation")?;
            match (record.phase, record.operation.kind) {
                (OperationPhase::Building, _) => {
                    self.store.discard_uncommitted(&operation)?;
                }
                (_, OperationKind::Ingest) => {
                    self.publisher.recover(&table, &operation).await?;
                }
                (
                    _,
                    OperationKind::Rewrite
                    | OperationKind::Reconcile
                    | OperationKind::ManifestRewrite,
                ) => {
                    self.maintenance.recover(&table, &operation).await?;
                    let store = self.store.clone();
                    tokio::task::spawn_blocking(move || store.forget_applied(&operation)).await??;
                }
                _ => bail!("index reconstruction must finish before publication"),
            }
        }
        let indexed = self.store.table_state(&id)?;
        if transactions
            .last()
            .is_some_and(|transaction| transaction.end_lsn <= indexed.materialized_lsn)
        {
            return Ok(TableOutcome::Complete {
                snapshot: indexed.snapshot_id,
                maintenance_pending: MaintenancePending {
                    data: true,
                    periodic: false,
                },
            });
        }
        let source = SourceId(self.config.source.id.clone());
        let schema = crate::schema::latest_schema(&self.store, &source, id)?.unwrap_or(schema);
        for version in transactions
            .iter()
            .flat_map(|transaction| &transaction.schema_versions)
            .filter(|version| version.table_id == id)
        {
            ensure!(
                version.version <= schema.version,
                "journal schema is newer than durable source registry"
            );
        }
        let mut current = crate::schema::refresh_table(self.catalog.as_ref(), &table).await?;
        current =
            crate::schema::ensure_table_schema(self.catalog.as_ref(), &current, &schema).await?;
        let indexed = self.store.table_state(&id)?;
        if indexed.snapshot_id != current.metadata().current_snapshot_id() {
            let path = self
                .config
                .state_dir
                .join("reconcile")
                .join(uuid::Uuid::new_v4().to_string());
            let scratch = StateStore::open(
                &path,
                StateStoreOptions {
                    apply_batch_rows: self.config.limits.batch_rows,
                    ..Default::default()
                },
            )?;
            let result = self.maintenance.reconcile(&current, &schema, scratch).await;
            let cleanup = std::fs::remove_dir_all(path);
            result?;
            cleanup?;
            current = crate::schema::refresh_table(self.catalog.as_ref(), &table).await?;
        }
        if indexed.schema_version != schema.version {
            let store = self.store.clone();
            let table = current.clone();
            let schema = schema.clone();
            tokio::task::spawn_blocking(move || {
                flow_coordinator::reconcile_index_schema(&store, &source, &table, &schema)
            })
            .await??;
        }
        if options.periodic_maintenance {
            ensure!(
                transactions.is_empty() && candidate.is_none(),
                "periodic maintenance contains publication work"
            );
            let inventory = self.maintenance.inventory(&current, id).await?;
            crate::observation::table_inventory(id, &inventory);
            let (_, continuation) = self
                .maintain_metadata(current, id, inventory.manifest_count)
                .await?;
            tracing::info!(
                event = "periodic_maintenance_completed",
                table_id = id.0,
                continuation,
                "metadata and garbage maintenance yielded the table actor"
            );
            return Ok(TableOutcome::PeriodicComplete {
                next_due: continuation.then(Instant::now),
            });
        }
        if let Some(candidate) = candidate.take() {
            match candidate {
                CompactionCandidate::Built(candidate) => {
                    if candidate.started.elapsed() > BUILD_MAX_AGE {
                        candidate.ready.discard().await?;
                        return Err(ReplanRequired.into());
                    }
                    let running = self
                        .maintenance
                        .start_compaction_preparation(&current, &schema, candidate.ready)
                        .await;
                    let handoff_elapsed = options.actor_acquired_at.elapsed();
                    metrics::histogram!(
                        "flow_compaction_preparation_handoff_actor_lane_hold_seconds",
                        "table_id" => id.0.to_string()
                    )
                    .record(handoff_elapsed.as_secs_f64());
                    tracing::info!(
                        event = "compaction_preparation_handoff_actor_lane_released",
                        table_id = id.0,
                        lane_hold_ms = handoff_elapsed.as_secs_f64() * 1000.0,
                        "handed compaction preparation to the background"
                    );
                    let running = running?;
                    return Ok(TableOutcome::Preparation(Box::new(StartedPreparation {
                        running,
                        path: candidate.path,
                        started: candidate.started,
                        activation_stall_started: options.actor_acquired_at,
                    })));
                }
                CompactionCandidate::Prepared(candidate) => {
                    if candidate.started.elapsed() > BUILD_MAX_AGE {
                        candidate.prepared.discard().await?;
                        return Err(ReplanRequired.into());
                    }
                    let snapshot = self
                        .maintenance
                        .activate_compaction(&current, &schema, candidate.prepared)
                        .await?;
                    metrics::counter!(
                        "flow_compaction_activations_total",
                        "table_id" => id.0.to_string(),
                        "result" => if snapshot.is_some() { "committed" } else { "no_change" }
                    )
                    .increment(1);
                    if snapshot.is_some() {
                        metrics::histogram!("flow_compaction_seconds", "table_id" => id.0.to_string(), "kind" => "data")
                            .record(candidate.started.elapsed().as_secs_f64());
                        metrics::counter!("flow_compactions_total", "table_id" => id.0.to_string(), "kind" => "data").increment(1);
                    }
                    return Ok(TableOutcome::Complete {
                        snapshot,
                        maintenance_pending: MaintenancePending {
                            data: true,
                            periodic: false,
                        },
                    });
                }
            }
        }
        let (current, maintenance_pending) = match self
            .maintain(
                current,
                &schema,
                transactions.is_empty(),
                options,
                repair_cursor,
            )
            .await?
        {
            MaintenanceOutcome::Complete(table, pending) => (table, pending),
            MaintenanceOutcome::Build(build) => return Ok(TableOutcome::Build(Box::new(build))),
            MaintenanceOutcome::Deferred => return Ok(TableOutcome::Deferred),
        };
        if transactions.is_empty() {
            return Ok(TableOutcome::Complete {
                snapshot: current.metadata().current_snapshot_id(),
                maintenance_pending,
            });
        }
        let epoch = Epoch::new(SourceId(self.config.source.id.clone()), id, &transactions)?;
        let collapse_started = Instant::now();
        let store = self.store.clone();
        let prepare_schema = schema.clone();
        let prepare_epoch = epoch.clone();
        let prepare_transactions = transactions.clone();
        let prepare_table = current.clone();
        let collapse_limits = CollapseLimits {
            batch_rows: self.config.limits.batch_rows,
            batch_bytes: self.config.limits.batch_bytes,
            memory_bytes: self.config.limits.collapse_memory_bytes,
        };
        let collapsed = tokio::task::spawn_blocking(move || {
            collapse_epoch(
                &store,
                &self.reader,
                &prepare_table,
                &prepare_schema,
                &prepare_epoch,
                &prepare_transactions,
                collapse_limits,
            )
        })
        .await??;
        let seconds = collapse_started.elapsed().as_secs_f64();
        metrics::histogram!("flow_table_local_phase_seconds", "table_id" => id.0.to_string(), "phase" => "collapse").record(seconds);
        tracing::info!(event = "epoch_collapsed", operation_id = %epoch.id.0,
            table_id = id.0, transactions = transactions.len(), elapsed_ms = seconds * 1000.0,
            collapse_mode = collapsed.mode(),
            collapse_memory_limit_bytes = collapse_limits.memory_bytes,
            collapse_memory_peak_bytes = collapsed.memory_peak_bytes(),
            collapse_memory_peak_known = collapsed.memory_peak_bytes().is_some(),
            "epoch mutations collapsed against the row index");
        let snapshot = self.publisher.publish(&current, &schema, collapsed).await?;
        self.store.discard_transaction(&epoch.id.0)?;
        Ok(TableOutcome::Complete {
            snapshot,
            maintenance_pending,
        })
    }

    /// Capture only a soft data build. Recovery, schema changes, dependency
    /// repair and synchronous maintenance belong to ordinary table work.
    async fn probe_build(&self, table: &Table, schema: TableSchema) -> Result<TableOutcome> {
        if !self.compaction {
            return Ok(TableOutcome::ProbeComplete);
        }
        let id = schema.table_id;
        let indexed = self.store.table_state(&id)?;
        if indexed.pending_operation.is_some() {
            return Ok(TableOutcome::ProbeComplete);
        }
        let source = SourceId(self.config.source.id.clone());
        let schema = crate::schema::latest_schema(&self.store, &source, id)?.unwrap_or(schema);
        let current = crate::schema::refresh_table(self.catalog.as_ref(), table).await?;
        if indexed.snapshot_id != current.metadata().current_snapshot_id()
            || indexed.schema_version != schema.version
            || !flow_coordinator::same_iceberg_schema(
                current.metadata().current_schema(),
                &flow_materializer::iceberg_schema(&schema)?,
            )
        {
            return Ok(TableOutcome::ProbeComplete);
        }
        let inventory = self.maintenance.inventory(&current, id).await?;
        crate::observation::table_inventory(id, &inventory);
        if inventory.debt.pressure != flow_compactor::PublicationPressure::Delay
            || self.prefers_delete_rewrite(&inventory)
        {
            return Ok(TableOutcome::ProbeComplete);
        }
        Ok(match self.start_build(&current, &schema).await? {
            Some(build) => TableOutcome::Build(Box::new(build)),
            None => TableOutcome::ProbeComplete,
        })
    }

    fn prefers_delete_rewrite(&self, inventory: &flow_coordinator::Inventory) -> bool {
        inventory.delete_file_count >= self.config.compaction.delete_files_soft
            && (self.config.compaction.data_rewrite_scope != flow_compactor::DataRewriteScope::All
                || !inventory
                    .files
                    .iter()
                    .any(|file| self.config.compaction.needs_delete_reclamation(file)))
    }

    /// CDC does only mandatory maintenance. Optional work runs in its own job,
    /// after the completed publication has advanced the source ledger.
    async fn maintain(
        &self,
        mut current: Table,
        schema: &TableSchema,
        optional: bool,
        options: WorkOptions,
        repair_cursor: &mut DeleteRepairCursor,
    ) -> Result<MaintenanceOutcome> {
        let id = schema.table_id;
        let mut inventory = self.maintenance.inventory(&current, id).await?;
        crate::observation::table_inventory(id, &inventory);
        let mut pressure = inventory.debt.pressure;
        if options.build_active && pressure == flow_compactor::PublicationPressure::Pause {
            // Release the actor instead of retrying while a completed build
            // waits to acquire that same actor for finalization.
            return Ok(MaintenanceOutcome::Deferred);
        }
        while self.compaction
            && (pressure == flow_compactor::PublicationPressure::Pause
                || optional && pressure == flow_compactor::PublicationPressure::Delay)
        {
            let before = current.metadata().current_snapshot_id();
            let data_rewrite_scope = self.config.compaction.data_rewrite_scope;
            let rewritten = if self.prefers_delete_rewrite(&inventory) {
                self.compact(&current, schema, CompactionKind::Deletes)
                    .await?
            } else {
                None
            };
            if rewritten.is_none() {
                let soft = pressure == flow_compactor::PublicationPressure::Delay;
                if soft && options.build_admission == BuildAdmission::Wait {
                    let periodic = self.metadata_due(&current, id, inventory.manifest_count)?;
                    return Ok(MaintenanceOutcome::Complete(
                        current,
                        MaintenancePending {
                            data: true,
                            periodic,
                        },
                    ));
                }
                if data_rewrite_scope != flow_compactor::DataRewriteScope::Disabled
                    && let Some(build) = self
                        .maintain_data(
                            &current,
                            schema,
                            soft && options.build_admission == BuildAdmission::Start,
                            repair_cursor,
                        )
                        .await?
                {
                    return Ok(MaintenanceOutcome::Build(build));
                }
            }
            current = self.catalog.load_table(current.identifier()).await?;
            inventory = self.maintenance.inventory(&current, id).await?;
            crate::observation::table_inventory(id, &inventory);
            pressure = inventory.debt.pressure;
            if pressure != flow_compactor::PublicationPressure::Pause {
                break;
            }
            // A bounded rewrite may leave another group above the hard limit.
            // Keep publication paused while maintenance makes progress.
            if current.metadata().current_snapshot_id() == before {
                return Err(flow_compactor::Error::MaintenanceRequired.into());
            }
        }
        if pressure == flow_compactor::PublicationPressure::Pause {
            return Err(flow_compactor::Error::MaintenanceRequired.into());
        }
        let periodic = self.metadata_due(&current, id, inventory.manifest_count)?;
        if !optional {
            return Ok(MaintenanceOutcome::Complete(
                current,
                MaintenancePending {
                    data: self.compaction && pressure == flow_compactor::PublicationPressure::Delay,
                    periodic,
                },
            ));
        }
        let (current, continuation) = self
            .maintain_metadata(current, id, inventory.manifest_count)
            .await?;
        Ok(MaintenanceOutcome::Complete(
            current,
            MaintenancePending {
                data: false,
                periodic: continuation,
            },
        ))
    }

    fn garbage_due(&self, id: TableId) -> Result<bool> {
        Ok(self
            .garbage_checked
            .lock()
            .map_err(|_| anyhow::anyhow!("maintenance clock lock poisoned"))?
            .get(&id)
            .is_none_or(|checked| {
                checked.elapsed() >= Duration::from_secs(self.config.limits.garbage_interval_secs)
            }))
    }

    fn metadata_due(&self, table: &Table, id: TableId, manifests: usize) -> Result<bool> {
        Ok(manifests >= self.config.limits.manifest_max_count
            || self.config.limits.snapshot_expiration
                && history_due(table, self.config.limits.snapshot_retention_secs)?
            || self.garbage_due(id)?)
    }

    /// Run on the table actor independently of soft data debt. Garbage collection
    /// visits one bounded page, then yields before its continuation is admitted.
    async fn maintain_metadata(
        &self,
        mut current: Table,
        id: TableId,
        manifests: usize,
    ) -> Result<(Table, bool)> {
        let manifests_due = manifests >= self.config.limits.manifest_max_count;
        let history_due = self.config.limits.snapshot_expiration
            && history_due(&current, self.config.limits.snapshot_retention_secs)?;
        let garbage_due = self.garbage_due(id)?;
        if manifests_due {
            self.maintenance
                .rewrite_manifests(
                    &current,
                    id,
                    &flow_iceberg_ext::ManifestRewritePolicy {
                        min_manifest_count: self.config.limits.manifest_max_count,
                        ..Default::default()
                    },
                )
                .await?;
            current = self.catalog.load_table(current.identifier()).await?;
        }
        if history_due {
            let checkpoints = self.control.checkpoints()?;
            let unresolved_initial = checkpoints
                .iter()
                .flat_map(|checkpoint| &checkpoint.pending_operations)
                .any(|record| {
                    record.operation.table_id == id && record.operation.base_snapshot_id.is_none()
                });
            if !unresolved_initial {
                let protected =
                    flow_coordinator::GarbageProtection::from_checkpoints(id, &checkpoints);
                self.maintenance
                    .expire_history(
                        &current,
                        id,
                        Duration::from_secs(self.config.limits.snapshot_retention_secs),
                        &protected.snapshots,
                    )
                    .await?;
                current = self.catalog.load_table(current.identifier()).await?;
            }
        }
        let mut garbage_pending = false;
        if garbage_due {
            let protection = flow_coordinator::GarbageProtection::from_checkpoints(
                id,
                &self.control.checkpoints()?,
            );
            let started = Instant::now();
            let report = self
                .maintenance
                .collect_garbage(
                    &current,
                    id,
                    &flow_coordinator::GarbagePolicy {
                        grace: Duration::from_secs(self.config.limits.orphan_grace_secs),
                        ..Default::default()
                    },
                    &protection,
                )
                .await?;
            garbage_pending = report.continuation_required;
            if !garbage_pending {
                self.garbage_checked
                    .lock()
                    .map_err(|_| anyhow::anyhow!("maintenance clock lock poisoned"))?
                    .insert(id, Instant::now());
            }
            metrics::counter!("flow_garbage_delete_requests_total", "table_id" => id.0.to_string())
                .increment(report.delete_requests as u64);
            metrics::histogram!("flow_garbage_collection_seconds", "table_id" => id.0.to_string())
                .record(started.elapsed().as_secs_f64());
        }
        Ok((current, garbage_pending))
    }
}

#[cfg(test)]
mod finalization_lane_tests {
    use super::*;
    use metrics_exporter_prometheus::PrometheusBuilder;
    use std::io::{self, Write};

    struct TraceWriter(Arc<Mutex<Vec<u8>>>);

    impl Write for TraceWriter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn completed_build_reports_the_scheduler_lane_interval_and_operation() {
        let recorder = PrometheusBuilder::new().build_recorder();
        let metrics = recorder.handle();
        let _guard = metrics::set_default_local_recorder(&recorder);
        let trace = Arc::new(Mutex::new(Vec::new()));
        let trace_writer = trace.clone();
        let subscriber = tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_target(false)
            .with_writer(move || TraceWriter(trace_writer.clone()))
            .finish();
        let acquired_at = Instant::now();
        let lane = FinalizationLane {
            operation_id: OperationId("rewrite-observability-test".into()),
            acquired_at,
            publication_stall_started: acquired_at,
        };

        let held = tracing::subscriber::with_default(subscriber, || {
            lane.record_release(TableId(17), acquired_at + Duration::from_millis(25))
        });
        assert_eq!(held, Duration::from_millis(25));

        let metrics = metrics.render();
        assert!(
            metrics.contains(
                "flow_compaction_finalization_actor_lane_hold_seconds_sum{table_id=\"17\"} 0.025"
            ),
            "{metrics}"
        );
        let trace = String::from_utf8(trace.lock().unwrap().clone()).unwrap();
        assert!(
            trace.contains("event=\"compaction_finalization_actor_lane_released\"")
                && trace.contains("operation_id=rewrite-observability-test")
                && trace.contains("table_id=17")
                && trace.contains("lane_hold_ms=25"),
            "{trace}"
        );
    }
}
