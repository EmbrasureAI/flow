//! Execute one table's publication or maintenance visit under scheduler ownership.

use super::{BUILD_MAX_AGE, OPTIONAL_MAINTENANCE_DELAY, blocked::publication_error_code};
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
    time::{Duration, Instant},
};

async fn discard_unowned_spool(store: StateStore, operation: OperationId) -> Result<()> {
    tokio::task::spawn_blocking(move || {
        if store.operation(&operation)?.is_none() {
            store.discard_transaction(&operation.0)?;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    Ok(())
}

/// Wait after a table-scoped metadata maintenance failure, doubling per
/// consecutive failure. CDC for the table and every other table continues.
const METADATA_RETRY_MIN: Duration = Duration::from_secs(30);
const METADATA_RETRY_MAX: Duration = Duration::from_secs(15 * 60);
/// Consecutive failures of one task after which it is reported as failing
/// (`flow_table_maintenance_failing` and an ERROR log) until it succeeds.
const METADATA_FAILURE_ESCALATION: u32 = 5;
/// A mandatory manifest rewrite that could not get under its hard limit is
/// not retried with every epoch.
const MANDATORY_METADATA_STALL: Duration = Duration::from_secs(60);

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum MetadataTask {
    Manifests,
    History,
    Garbage,
}

impl MetadataTask {
    fn name(self) -> &'static str {
        match self {
            Self::Manifests => "manifest_rewrite",
            Self::History => "snapshot_expiration",
            Self::Garbage => "garbage_collection",
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum MetadataScope {
    /// Everything due, including garbage collection.
    Full,
    /// Only work past a hard limit. It reduces commit cost, so it runs in the
    /// publication path even while source pressure suppresses optional
    /// maintenance.
    Mandatory,
}

/// Per-table retry state for metadata maintenance. Process-local: a restart
/// retries immediately, which is harmless.
#[derive(Default)]
pub(super) struct MetadataBackoff {
    failures: BTreeMap<(TableId, MetadataTask), (Instant, u32)>,
    stalled: BTreeMap<(TableId, MetadataTask), Instant>,
}

impl MetadataBackoff {
    fn failed_until(&self, id: TableId, task: MetadataTask, now: Instant) -> bool {
        self.failures
            .get(&(id, task))
            .is_some_and(|(retry_at, _)| now < *retry_at)
    }

    fn stalled_until(&self, id: TableId, task: MetadataTask, now: Instant) -> bool {
        self.failed_until(id, task, now)
            || self
                .stalled
                .get(&(id, task))
                .is_some_and(|until| now < *until)
    }

    /// Returns the retry delay and the number of consecutive failures.
    fn fail(&mut self, id: TableId, task: MetadataTask, now: Instant) -> (Duration, u32) {
        let failures = self
            .failures
            .get(&(id, task))
            .map_or(1, |(_, failures)| failures.saturating_add(1));
        let delay = METADATA_RETRY_MIN
            .saturating_mul(1 << failures.saturating_sub(1).min(8))
            .min(METADATA_RETRY_MAX);
        self.failures.insert((id, task), (now + delay, failures));
        (delay, failures)
    }

    /// Returns the number of consecutive failures this success ends.
    fn succeed(&mut self, id: TableId, task: MetadataTask) -> u32 {
        self.failures
            .remove(&(id, task))
            .map_or(0, |(_, failures)| failures)
    }

    fn stall(&mut self, id: TableId, task: MetadataTask, until: Instant) {
        self.stalled.insert((id, task), until);
    }
}

/// Local index, journal, model and disk failures stay connection-wide, as they
/// do for publication. Other metadata maintenance failures are table-scoped.
fn local_failure(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        cause.is::<flow_state_store::Error>()
            || cause.is::<flow_ingress_journal::Error>()
            || cause.is::<flow_model::ModelError>()
    }) || error.chain().any(|cause| cause.is::<std::io::Error>())
        && !error.chain().any(|cause| cause.is::<reqwest::Error>())
}

async fn manifest_count(table: &Table) -> Result<usize> {
    let Some(snapshot) = table.metadata().current_snapshot() else {
        return Ok(0);
    };
    Ok(table
        .manifest_list_reader(snapshot)
        .load()
        .await?
        .consume_entries()
        .into_iter()
        .count())
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
        operation: Option<OperationId>,
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
    Recovered,
    Blocked {
        error_code: &'static str,
    },
}

enum MaintenanceOutcome {
    /// The head, pending work and its current manifest count.
    Complete(Table, MaintenancePending, usize),
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
    pub(super) periodic_maintenance: bool,
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
    // Optional maintenance must obey the same shared-failure precedence as CDC.
    if publication_error_code(error).is_none() {
        return false;
    }
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
    pub(super) config: Arc<Config>,
    pub(super) store: StateStore,
    pub(super) control: ControlStore,
    pub(super) reader: JournalReader,
    pub(super) catalog: Arc<dyn Catalog>,
    pub(super) targets: Arc<BTreeMap<TableId, (iceberg::TableIdent, uuid::Uuid)>>,
    pub(super) publisher: Arc<TablePublisher>,
    pub(super) maintenance: Arc<TableMaintenance>,
    pub(super) garbage_checked: Arc<Mutex<BTreeMap<TableId, Instant>>>,
    pub(super) metadata_backoff: Arc<Mutex<MetadataBackoff>>,
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
        schema: TableSchema,
        transactions: Vec<SourceTransaction>,
        options: WorkOptions,
        mut candidate: Option<CompactionCandidate>,
        finalization_lane: Option<FinalizationLane>,
    ) -> Result<TableCompletion> {
        let started = Instant::now();
        let mut repair_cursor = DeleteRepairCursor::default();
        let scratch_path = candidate.as_ref().map(|candidate| candidate.path().clone());
        let reserved_build = candidate.is_some()
            || matches!(
                options.build_admission,
                BuildAdmission::Start | BuildAdmission::Probe
            );
        let candidate_stage = candidate.as_ref().map(|candidate| {
            if candidate.is_prepared() {
                "activation"
            } else {
                "preparation"
            }
        });
        // Catalog loading and recovery use the same bounded worker admission as
        // publication. No retry delay holds a worker or its transaction batch.
        let result = async {
            match crate::schema::capture_block(
                &self.store,
                &SourceId(self.config.source.id.clone()),
                schema.table_id,
            )? {
                None => {}
                // Resync is the only recovery, but an unfinished operation
                // would hold its transactions forever. Settle it, then stay
                // blocked; the runtime completes the rest without publishing.
                Some(crate::schema::CaptureBlock::PublicationChanged)
                    if self
                        .store
                        .table_state(&schema.table_id)?
                        .pending_operation
                        .is_some() =>
                {
                    self.settle_blocked_operation(schema.table_id).await?;
                    return Err(flow_model::SourceTableBlocked.into());
                }
                Some(_) => return Err(flow_model::SourceTableBlocked.into()),
            }
            let table = self.load_target(schema.table_id).await?;
            self.clone()
                .run_once(
                    table,
                    schema.clone(),
                    transactions.clone(),
                    options,
                    &mut candidate,
                    &mut repair_cursor,
                )
                .await
        }
        .await;
        let outcome = match result {
            Err(error)
                if options.periodic_maintenance
                    && retry_table_work(&error)
                    && self
                        .store
                        .table_state(&schema.table_id)?
                        .pending_operation
                        .is_none() =>
            {
                TableOutcome::PeriodicComplete {
                    next_due: Some(Instant::now() + OPTIONAL_MAINTENANCE_DELAY),
                }
            }
            Err(error)
                if options.build_admission == BuildAdmission::Probe && retry_table_work(&error) =>
            {
                TableOutcome::ProbeComplete
            }
            Err(error)
                if candidate_stage.is_some()
                    && retry_table_work(&error)
                    && self
                        .store
                        .table_state(&schema.table_id)?
                        .pending_operation
                        .is_none() =>
            {
                let stage = candidate_stage.expect("checked candidate stage");
                let reason = candidate_invalidation_reason(&error);
                metrics::counter!("flow_compaction_candidate_invalidations_total",
                    "table_id" => schema.table_id.0.to_string(), "stage" => stage, "reason" => reason)
                .increment(1);
                if stage == "activation" {
                    metrics::counter!("flow_compaction_activations_total",
                        "table_id" => schema.table_id.0.to_string(), "result" => "invalidated")
                    .increment(1);
                }
                tracing::warn!(event = "compaction_candidate_invalidated",
                    table = ?schema.table_id, stage, reason, %error,
                    "retired stale compaction candidate before rescheduling");
                TableOutcome::CandidateInvalidated
            }
            Err(error) if publication_error_code(&error).is_some() => {
                let mut error_code =
                    publication_error_code(&error).expect("classified publication error");
                // A capture block reports the cause capture recorded for it.
                if error_code == "source_schema_incompatible"
                    && let Some(cause) = crate::schema::capture_block(
                        &self.store,
                        &SourceId(self.config.source.id.clone()),
                        schema.table_id,
                    )?
                {
                    error_code = cause.code();
                }
                tracing::warn!(table = ?schema.table_id, error_code, %error,
                    "table publication blocked; releasing worker until retry");
                TableOutcome::Blocked { error_code }
            }
            result => result?,
        };
        let preparation_handoff = matches!(outcome, TableOutcome::Preparation(_));
        // Independent build state can be retired after its workers join. A
        // publication operation and its staged files retain durable ownership.
        if let Some(candidate) = candidate {
            candidate.discard().await?;
        }
        if !preparation_handoff && let Some(path) = scratch_path {
            std::fs::remove_dir_all(path)?;
        }
        Ok(TableCompletion {
            id: schema.table_id,
            transactions,
            outcome,
            elapsed: started.elapsed(),
            reserved_build,
            finalization_lane,
            periodic_maintenance: options.periodic_maintenance,
        })
    }

    async fn load_target(&self, id: TableId) -> Result<Table> {
        let (target, uuid) = self
            .targets
            .get(&id)
            .context("table has no persisted target identity")?;
        let table = self.catalog.load_table(target).await?;
        ensure!(
            table.metadata().uuid() == *uuid,
            "target table was replaced; refusing to reuse its source watermark"
        );
        Ok(table)
    }

    /// Settle the unfinished operation of a table that will never publish again.
    /// A building operation is discarded without the catalog. Later phases are
    /// resolved through ordinary recovery, which needs the target. If the target
    /// is gone or was replaced, whatever an uncertain commit wrote belongs to a
    /// table this pipeline no longer owns and a resync rebuilds it, so the
    /// operation is abandoned locally: a prepared one is discarded and a
    /// committed one is applied to the local index, which needs no catalog.
    /// Transient catalog errors propagate and the attempt is retried.
    async fn settle_blocked_operation(&self, id: TableId) -> Result<()> {
        let Some(operation) = self.store.table_state(&id)?.pending_operation else {
            return Ok(());
        };
        let record = self
            .store
            .operation(&operation)?
            .context("table fence has no operation")?;
        if record.phase == OperationPhase::Building {
            self.store.discard_uncommitted(&operation)?;
            self.store.discard_transaction(&operation.0)?;
            return Ok(());
        }
        if let Some(table) = self.owned_target(id).await? {
            self.recover_pending(&table, id).await?;
            return Ok(());
        }
        tracing::warn!(table = ?id, operation_id = %operation.0, phase = ?record.phase,
            "publication-blocked target is gone or replaced; abandoning its unfinished operation");
        let store = self.store.clone();
        tokio::task::spawn_blocking(move || -> Result<()> {
            match record.phase {
                OperationPhase::Prepared => {
                    store.discard_uncommitted(&operation)?;
                    store.discard_transaction(&operation.0)?;
                }
                OperationPhase::Committed => while !store.apply_committed(&operation)?.complete {},
                _ => {}
            }
            // Ingest operations complete their transactions through the
            // runtime's applied-operation path; maintenance ones have none.
            if record.operation.kind != OperationKind::Ingest
                && store
                    .operation(&operation)?
                    .is_some_and(|record| record.phase == OperationPhase::Applied)
            {
                store.forget_applied(&operation)?;
            }
            Ok(())
        })
        .await??;
        Ok(())
    }

    /// The configured target, or `None` when it is gone or was replaced.
    async fn owned_target(&self, id: TableId) -> Result<Option<Table>> {
        let (target, uuid) = self
            .targets
            .get(&id)
            .context("table has no persisted target identity")?;
        let table = match self.catalog.load_table(target).await {
            Ok(table) => table,
            Err(error)
                if matches!(
                    error.kind(),
                    iceberg::ErrorKind::TableNotFound | iceberg::ErrorKind::NamespaceNotFound
                ) =>
            {
                return Ok(None);
            }
            Err(error) => return Err(error.into()),
        };
        Ok((table.metadata().uuid() == *uuid).then_some(table))
    }

    /// Discard a building operation or resolve a prepared one. Returns true
    /// when an ingest operation was recovered and must be reconciled first.
    async fn recover_pending(&self, table: &Table, id: TableId) -> Result<bool> {
        let Some(operation) = self.store.table_state(&id)?.pending_operation else {
            return Ok(false);
        };
        let record = self
            .store
            .operation(&operation)?
            .context("table fence has no operation")?;
        match (record.phase, record.operation.kind) {
            (OperationPhase::Building, _) => {
                self.store.discard_uncommitted(&operation)?;
                self.store.discard_transaction(&operation.0)?;
                Ok(false)
            }
            (_, OperationKind::Ingest) => {
                let recovery = self.publisher.recover(table, &operation).await;
                discard_unowned_spool(self.store.clone(), operation.clone()).await?;
                recovery?;
                Ok(true)
            }
            (
                _,
                OperationKind::Rewrite | OperationKind::Reconcile | OperationKind::ManifestRewrite,
            ) => {
                self.maintenance.recover(table, &operation).await?;
                let store = self.store.clone();
                tokio::task::spawn_blocking(move || store.forget_applied(&operation)).await??;
                Ok(false)
            }
            _ => bail!("index reconstruction must finish before publication"),
        }
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
        if self.recover_pending(&table, id).await? {
            // Reconcile the recovered operation separately from newly
            // admitted CDC. Its original batch may differ after restart.
            return Ok(TableOutcome::Recovered);
        }
        let indexed = self.store.table_state(&id)?;
        if transactions
            .last()
            .is_some_and(|transaction| transaction.end_lsn <= indexed.materialized_lsn)
        {
            return Ok(TableOutcome::Complete {
                operation: None,
                snapshot: indexed.snapshot_id,
                maintenance_pending: MaintenancePending {
                    data: true,
                    periodic: false,
                },
            });
        }
        let transactions = transactions
            .into_iter()
            .filter(|transaction| transaction.end_lsn > indexed.materialized_lsn)
            .collect::<Vec<_>>();
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
        current = crate::schema::ensure_table_schema(
            &self.store,
            self.catalog.as_ref(),
            &current,
            &schema,
        )
        .await?;
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
            let (_, continuation, _) = self
                .maintain_metadata(current, id, inventory.manifest_count, MetadataScope::Full)
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
                        operation: None,
                        snapshot,
                        maintenance_pending: MaintenancePending {
                            data: true,
                            periodic: false,
                        },
                    });
                }
            }
        }
        let (current, mut maintenance_pending, manifests) = match self
            .maintain(
                current,
                &schema,
                transactions.is_empty(),
                options,
                repair_cursor,
            )
            .await?
        {
            MaintenanceOutcome::Complete(table, pending, manifests) => (table, pending, manifests),
            MaintenanceOutcome::Build(build) => return Ok(TableOutcome::Build(Box::new(build))),
            MaintenanceOutcome::Deferred => return Ok(TableOutcome::Deferred),
        };
        if transactions.is_empty() {
            return Ok(TableOutcome::Complete {
                operation: None,
                snapshot: current.metadata().current_snapshot_id(),
                maintenance_pending,
            });
        }
        let epoch = Epoch::new(SourceId(self.config.source.id.clone()), id, &transactions)?;
        let result = async {
        let collapse_started = Instant::now();
        let store = self.store.clone();
        let prepare_schema = schema.clone();
        let prepare_epoch = epoch.clone();
        let prepare_transactions = transactions.clone();
        let prepare_table = current.clone();
        let reader = self.reader.clone();
        let collapse_limits = CollapseLimits {
            batch_rows: self.config.limits.batch_rows,
            batch_bytes: self.config.limits.batch_bytes,
            memory_bytes: self.config.limits.collapse_memory_bytes,
        };
        let collapsed = tokio::task::spawn_blocking(move || {
            collapse_epoch(
                &store,
                &reader,
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
        tracing::debug!(target: "flow_events", event = "epoch_collapsed", operation_id = %epoch.id.0,
            table_id = id.0, transactions = transactions.len(), elapsed_ms = seconds * 1000.0,
            collapse_mode = collapsed.mode(),
            collapse_memory_limit_bytes = collapse_limits.memory_bytes,
            collapse_memory_peak_bytes = collapsed.memory_peak_bytes(),
            collapse_memory_peak_known = collapsed.memory_peak_bytes().is_some(),
            "epoch mutations collapsed against the row index");
        let snapshot = self.publisher.publish(&current, &schema, collapsed).await?;
        if !options.build_active
            && let Some(periodic) = self
                .maintain_after_publication(&current, id, manifests)
                .await?
        {
            maintenance_pending.periodic = periodic;
        }
        Ok(TableOutcome::Complete {
            operation: Some(epoch.id.clone()),
            snapshot,
            maintenance_pending,
        })
        }.await;
        // Changed retry batches must not orphan a previous disk collapse. The
        // durable operation retains any spool until its recovery/retirement.
        discard_unowned_spool(self.store.clone(), epoch.id).await?;
        result
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
                    let periodic = self
                        .metadata_due(&current, id, inventory.manifest_count)
                        .await?;
                    return Ok(MaintenanceOutcome::Complete(
                        current,
                        MaintenancePending {
                            data: true,
                            periodic,
                        },
                        inventory.manifest_count,
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
        if !optional {
            // Mandatory metadata work runs after this epoch is published.
            let periodic = self
                .metadata_due(&current, id, inventory.manifest_count)
                .await?;
            return Ok(MaintenanceOutcome::Complete(
                current,
                MaintenancePending {
                    data: self.compaction && pressure == flow_compactor::PublicationPressure::Delay,
                    periodic,
                },
                inventory.manifest_count,
            ));
        }
        let (current, continuation, manifests) = self
            .maintain_metadata(current, id, inventory.manifest_count, MetadataScope::Full)
            .await?;
        Ok(MaintenanceOutcome::Complete(
            current,
            MaintenancePending {
                data: false,
                periodic: continuation,
            },
            manifests,
        ))
    }

    /// History and manifests far past their limits make every commit slower.
    /// Reduce them in the publication path, after the epoch is published, so
    /// source pressure (which suppresses optional maintenance) cannot let them
    /// grow without bound. `published` is the head this epoch was published
    /// on. Returns the refreshed periodic flag when any work ran. A failure
    /// never changes the published epoch's outcome; only local state failures
    /// stop the daemon.
    async fn maintain_after_publication(
        &self,
        published: &Table,
        id: TableId,
        manifests: usize,
    ) -> Result<Option<bool>> {
        let result = async {
            if !self.manifests_due(id, manifests, MetadataScope::Mandatory)?
                && !self
                    .history_due(published, id, MetadataScope::Mandatory)
                    .await?
            {
                return Ok(None);
            }
            let current = self.catalog.load_table(published.identifier()).await?;
            let (current, _, manifests) = self
                .maintain_metadata(current, id, manifests, MetadataScope::Mandatory)
                .await?;
            Ok(Some(self.metadata_due(&current, id, manifests).await?))
        }
        .await;
        match result {
            Err(error) if !local_failure(&error) => {
                tracing::warn!(
                    event = "post_publication_maintenance_deferred",
                    table_id = id.0,
                    %error,
                    "mandatory metadata maintenance after publication deferred"
                );
                Ok(None)
            }
            result => result,
        }
    }

    fn backoff(&self) -> Result<std::sync::MutexGuard<'_, MetadataBackoff>> {
        self.metadata_backoff
            .lock()
            .map_err(|_| anyhow::anyhow!("maintenance backoff lock poisoned"))
    }

    fn garbage_due(&self, id: TableId) -> Result<bool> {
        if self
            .backoff()?
            .failed_until(id, MetadataTask::Garbage, Instant::now())
        {
            return Ok(false);
        }
        Ok(self
            .garbage_checked
            .lock()
            .map_err(|_| anyhow::anyhow!("maintenance clock lock poisoned"))?
            .get(&id)
            .is_none_or(|checked| {
                checked.elapsed() >= Duration::from_secs(self.config.limits.garbage_interval_secs)
            }))
    }

    fn manifests_due(&self, id: TableId, manifests: usize, scope: MetadataScope) -> Result<bool> {
        let limit = self.config.limits.manifest_max_count;
        let now = Instant::now();
        Ok(match scope {
            MetadataScope::Full => {
                manifests >= limit
                    && !self
                        .backoff()?
                        .failed_until(id, MetadataTask::Manifests, now)
            }
            // Manifest count drives the cost of every commit and scan plan.
            // A stalled rewrite is continued by periodic maintenance only.
            MetadataScope::Mandatory => {
                manifests >= limit.saturating_mul(2)
                    && !self
                        .backoff()?
                        .stalled_until(id, MetadataTask::Manifests, now)
            }
        })
    }

    /// Plans against `table` with every protection the actor knows about, so
    /// history held by checkpoints, the retain floor or table policy is not
    /// due. A history plan is a few local reads; it needs no catalog request.
    async fn history_due(&self, table: &Table, id: TableId, scope: MetadataScope) -> Result<bool> {
        if !self.config.limits.snapshot_expiration
            || self
                .backoff()?
                .stalled_until(id, MetadataTask::History, Instant::now())
        {
            return Ok(false);
        }
        let checkpoints = self.control.checkpoints()?;
        if unresolved_initial(id, &checkpoints) {
            return Ok(false);
        }
        let protected = flow_coordinator::GarbageProtection::from_checkpoints(id, &checkpoints);
        let Some(plan) = self
            .maintenance
            .history_plan(
                table,
                id,
                &self.config.limits.history_policy(),
                &protected.snapshots,
            )
            .await?
        else {
            return Ok(false);
        };
        Ok(match scope {
            MetadataScope::Full => plan.due(),
            MetadataScope::Mandatory => plan.over_limit(),
        })
    }

    async fn metadata_due(&self, table: &Table, id: TableId, manifests: usize) -> Result<bool> {
        Ok(self.manifests_due(id, manifests, MetadataScope::Full)?
            || self.garbage_due(id)?
            || self.history_due(table, id, MetadataScope::Full).await?)
    }

    /// Record the outcome of one metadata maintenance task. A table-scoped
    /// failure is logged, counted and retried later; `None` is returned and
    /// CDC continues. After repeated failures the task is reported as failing
    /// until it succeeds. Local failures and replans keep their existing
    /// handling. A failure that left an operation for recovery blocks only
    /// this table until the operation is resolved.
    fn settle<T>(&self, id: TableId, task: MetadataTask, result: Result<T>) -> Result<Option<T>> {
        let error = match result {
            Ok(value) => {
                if self.backoff()?.succeed(id, task) >= METADATA_FAILURE_ESCALATION {
                    tracing::info!(
                        event = "metadata_maintenance_recovered",
                        table_id = id.0,
                        task = task.name(),
                        "table metadata maintenance succeeded again"
                    );
                }
                metrics::gauge!("flow_table_maintenance_failing",
                    "table_id" => id.0.to_string(), "task" => task.name())
                .set(0.0);
                return Ok(Some(value));
            }
            Err(error) => error,
        };
        if local_failure(&error) || error.downcast_ref::<ReplanRequired>().is_some() {
            return Err(error);
        }
        let (delay, failures) = self.backoff()?.fail(id, task, Instant::now());
        metrics::counter!("flow_metadata_maintenance_failures_total",
            "table_id" => id.0.to_string(), "task" => task.name())
        .increment(1);
        if failures >= METADATA_FAILURE_ESCALATION {
            metrics::gauge!("flow_table_maintenance_failing",
                "table_id" => id.0.to_string(), "task" => task.name())
            .set(1.0);
            tracing::error!(
                event = "metadata_maintenance_failing",
                table_id = id.0,
                task = task.name(),
                failures,
                retry_in_secs = delay.as_secs(),
                %error,
                "table metadata maintenance keeps failing; CDC continues but this task needs attention"
            );
        } else {
            tracing::warn!(
                event = "metadata_maintenance_failed",
                table_id = id.0,
                task = task.name(),
                failures,
                retry_in_secs = delay.as_secs(),
                %error,
                "table metadata maintenance failed; retrying later without stopping CDC"
            );
        }
        if self.store.table_state(&id)?.pending_operation.is_some() {
            return Err(error.context(flow_compactor::Error::MaintenanceRequired));
        }
        Ok(None)
    }

    /// Run on the table actor independently of soft data debt. Garbage collection
    /// visits one bounded page, then yields before its continuation is admitted.
    /// Returns the head, whether garbage collection continues, and the head's
    /// manifest count.
    async fn maintain_metadata(
        &self,
        mut current: Table,
        id: TableId,
        mut manifests: usize,
        scope: MetadataScope,
    ) -> Result<(Table, bool, usize)> {
        let manifests_due = self.manifests_due(id, manifests, scope)?;
        let history_due = self.history_due(&current, id, scope).await?;
        let garbage_due = scope == MetadataScope::Full && self.garbage_due(id)?;
        if manifests_due {
            let result = self
                .maintenance
                .rewrite_manifests(
                    &current,
                    id,
                    &flow_iceberg_ext::ManifestRewritePolicy {
                        min_manifest_count: self.config.limits.manifest_max_count,
                        ..Default::default()
                    },
                )
                .await;
            let rewritten = self.settle(id, MetadataTask::Manifests, result)?;
            current = self.catalog.load_table(current.identifier()).await?;
            if rewritten.is_some() {
                // Later due checks must not act on the stale inventory count.
                manifests = manifest_count(&current).await?;
            }
            // One bounded rewrite may leave the table above the hard limit.
            // Periodic maintenance continues; the epoch path waits.
            if scope == MetadataScope::Mandatory
                && rewritten.is_some()
                && manifests >= self.config.limits.manifest_max_count.saturating_mul(2)
            {
                self.backoff()?.stall(
                    id,
                    MetadataTask::Manifests,
                    Instant::now() + MANDATORY_METADATA_STALL,
                );
            }
        }
        if history_due {
            let checkpoints = self.control.checkpoints()?;
            if !unresolved_initial(id, &checkpoints) {
                let protected =
                    flow_coordinator::GarbageProtection::from_checkpoints(id, &checkpoints);
                let policy = self.config.limits.history_policy();
                let result = self
                    .maintenance
                    .expire_history(&current, id, &policy, &protected.snapshots)
                    .await;
                // The plan counts only removable snapshots, so a successful
                // expiration leaves nothing mandatory; protected history above
                // the cap is reported by `flow_snapshots_over_cap`.
                self.settle(id, MetadataTask::History, result)?;
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
            let result = self
                .maintenance
                .collect_garbage(
                    &current,
                    id,
                    &flow_coordinator::GarbagePolicy {
                        grace: Duration::from_secs(self.config.limits.orphan_grace_secs),
                        metadata_grace: Duration::from_secs(
                            self.config.limits.metadata_json_grace_secs(),
                        ),
                        ..Default::default()
                    },
                    &protection,
                )
                .await;
            if let Some(report) = self.settle(id, MetadataTask::Garbage, result)? {
                garbage_pending = report.continuation_required;
                if !garbage_pending {
                    self.garbage_checked
                        .lock()
                        .map_err(|_| anyhow::anyhow!("maintenance clock lock poisoned"))?
                        .insert(id, Instant::now());
                }
                metrics::counter!("flow_garbage_delete_requests_total", "table_id" => id.0.to_string())
                    .increment(report.delete_requests as u64);
                metrics::counter!("flow_garbage_metadata_json_delete_requests_total", "table_id" => id.0.to_string())
                    .increment(report.metadata_json_delete_requests as u64);
                metrics::histogram!("flow_garbage_collection_seconds", "table_id" => id.0.to_string())
                    .record(started.elapsed().as_secs_f64());
            }
        }
        Ok((current, garbage_pending, manifests))
    }
}

/// Initial-copy recovery may need to prove an operation was never committed
/// anywhere in the table's lineage, so expiration waits for it.
fn unresolved_initial(id: TableId, checkpoints: &[flow_state_store::CheckpointRecord]) -> bool {
    checkpoints
        .iter()
        .flat_map(|checkpoint| &checkpoint.pending_operations)
        .any(|record| {
            record.operation.table_id == id && record.operation.base_snapshot_id.is_none()
        })
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

#[cfg(test)]
mod publication_retry_tests {
    use super::*;
    use flow_model::{PgLsn, PrimaryKey};
    use flow_state_store::{Change, PreparedOperation};

    #[tokio::test]
    async fn regrouped_retry_removes_unowned_spool_but_preserves_durable_operations() {
        let directory = tempfile::tempdir().unwrap();
        let store = StateStore::open(directory.path(), StateStoreOptions::default()).unwrap();
        let id = TableId(7);
        let failed = OperationId("failed-before-prepare".into());
        let owned = OperationId("uncertain-commit".into());
        for operation in [&failed, &owned] {
            store
                .collapse_changes(
                    &operation.0,
                    [(id, PrimaryKey(vec![1]), Change::Insert(vec![]))],
                )
                .unwrap();
            store.seal_transaction(&operation.0).unwrap();
        }
        store
            .begin_prepare(PreparedOperation {
                id: owned.clone(),
                table_id: id,
                kind: OperationKind::Ingest,
                base_snapshot_id: None,
                last_lsn: PgLsn(9),
                schema_version: 1,
                artifacts: vec![],
                payload: vec![],
            })
            .unwrap();
        discard_unowned_spool(store.clone(), failed.clone())
            .await
            .unwrap();
        discard_unowned_spool(store.clone(), owned.clone())
            .await
            .unwrap();
        store.seal_transaction(&failed.0).unwrap();
        assert_eq!(store.collapsed(&failed.0, &id).unwrap().count(), 0);
        assert_eq!(store.collapsed(&owned.0, &id).unwrap().count(), 1);
        assert_eq!(
            store.table_state(&id).unwrap().pending_operation,
            Some(owned.clone())
        );
        store.discard_uncommitted(&owned).unwrap();
        discard_unowned_spool(store.clone(), owned.clone())
            .await
            .unwrap();
        store.seal_transaction(&owned.0).unwrap();
        assert_eq!(store.collapsed(&owned.0, &id).unwrap().count(), 0);
    }

    #[test]
    fn maintenance_retry_never_masks_shared_corruption() {
        let error = anyhow::Error::new(
            iceberg::Error::new(iceberg::ErrorKind::Unexpected, "remote wrapper").with_source(
                flow_state_store::Error::AuthorityCorruption("bad revision".into()),
            ),
        );
        assert!(!retry_table_work(&error));
        assert!(retry_table_work(&ReplanRequired.into()));
        assert!(retry_table_work(
            &flow_compactor::Error::MaintenanceRequired.into()
        ));
    }

    /// After a restart, an operation a blocked table left unfinished would
    /// hold its transactions forever. A recovery-only attempt settles it and
    /// the table stays blocked, so acknowledgement can advance.
    #[derive(Clone, Copy, PartialEq)]
    enum Target {
        Present,
        Dropped,
        Replaced,
    }

    async fn settle_unfinished_operation(target: Target) {
        use flow_coordinator::{AckMode, JournalDurability, SourceLedger};
        use flow_model::{JournalChunks, TableMutationCount};
        use iceberg::{
            CatalogBuilder, NamespaceIdent, TableCreation,
            memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder},
        };

        let root = tempfile::tempdir().unwrap();
        let mut config: Config =
            toml::from_str(include_str!("../../../../examples/flow.toml")).unwrap();
        config.state_dir = root.path().join("state");
        let schema = config.tables[0].schema(7);
        let id = schema.table_id;
        let source = SourceId(config.source.id.clone());
        let control = ControlStore::open(root.path().join("control")).unwrap();
        let store = control
            .initialize_index(root.path().join("index"), StateStoreOptions::default())
            .unwrap();
        let catalog: Arc<dyn Catalog> = Arc::new(
            MemoryCatalogBuilder::default()
                .load(
                    "dropped",
                    std::collections::HashMap::from([(
                        MEMORY_CATALOG_WAREHOUSE.into(),
                        "memory://dropped".into(),
                    )]),
                )
                .await
                .unwrap(),
        );
        let namespace = NamespaceIdent::new("replicated".into());
        catalog
            .create_namespace(&namespace, std::collections::HashMap::new())
            .await
            .unwrap();
        let table = catalog
            .create_table(
                &namespace,
                TableCreation::builder()
                    .name("orders".into())
                    .schema(flow_materializer::iceberg_schema(&schema).unwrap())
                    .build(),
            )
            .await
            .unwrap();
        let (journal, _) = flow_ingress_journal::Journal::open(
            root.path().join("journal"),
            flow_ingress_journal::JournalConfig::default(),
        )
        .unwrap();
        let writer = crate::services::writer_config(&config);
        let work = TableWork {
            config: Arc::new(config.clone()),
            store: store.clone(),
            control,
            reader: journal.reader(),
            catalog: catalog.clone(),
            targets: Arc::new(BTreeMap::from([(
                id,
                (table.identifier().clone(), table.metadata().uuid()),
            )])),
            publisher: Arc::new(
                TablePublisher::new(
                    store.clone(),
                    catalog.clone(),
                    writer.clone(),
                    config.limits.batch_rows,
                    config.limits.batch_bytes,
                )
                .unwrap(),
            ),
            maintenance: Arc::new(
                TableMaintenance::new(
                    store.clone(),
                    catalog.clone(),
                    config.compaction.clone(),
                    writer,
                )
                .unwrap(),
            ),
            garbage_checked: Arc::default(),
            metadata_backoff: Arc::default(),
            compaction: false,
        };

        // Journaled and fenced by an operation before the latch.
        let mut ledger = SourceLedger::open(
            store.clone(),
            source.clone(),
            AckMode::Materialized,
            JournalDurability::LocalDisk,
        )
        .unwrap();
        ledger
            .journaled(SourceTransaction {
                source_id: source.clone(),
                xid: 1,
                begin_lsn: PgLsn(8),
                commit_lsn: PgLsn(9),
                end_lsn: PgLsn(10),
                commit_timestamp_micros: 0,
                schema_versions: vec![],
                affected_tables: vec![id],
                mutation_chunks: JournalChunks::default(),
                table_mutation_counts: Some(vec![TableMutationCount {
                    table_id: id,
                    mutations: 0,
                }]),
            })
            .unwrap();
        let operation = OperationId("unfinished-before-latch".into());
        let prepared = PreparedOperation {
            id: operation.clone(),
            table_id: id,
            kind: OperationKind::Ingest,
            base_snapshot_id: None,
            last_lsn: PgLsn(10),
            schema_version: 1,
            artifacts: vec![],
            payload: vec![],
        };
        if target == Target::Present {
            // Building: discarded without consulting the catalog.
            store.begin_prepare(prepared).unwrap();
        } else {
            // Prepared: its commit is uncertain, and its target is gone or
            // replaced, so it is abandoned locally instead of retried forever.
            store.prepare(prepared, []).unwrap();
            let ident = table.identifier().clone();
            catalog.drop_table(&ident).await.unwrap();
            if target == Target::Replaced {
                catalog
                    .create_table(
                        &namespace,
                        TableCreation::builder()
                            .name("orders".into())
                            .schema(flow_materializer::iceberg_schema(&schema).unwrap())
                            .build(),
                    )
                    .await
                    .unwrap();
            }
        }
        crate::schema::latch_publication_block(&store, &source, id).unwrap();

        let completion = work
            .run(
                schema,
                Vec::new(),
                WorkOptions {
                    periodic_maintenance: false,
                    build_active: false,
                    build_admission: BuildAdmission::Wait,
                    actor_acquired_at: Instant::now(),
                },
                None,
                None,
            )
            .await
            .unwrap();
        assert!(matches!(
            completion.outcome,
            TableOutcome::Blocked {
                error_code: "publication_changed"
            }
        ));
        assert_eq!(store.table_state(&id).unwrap().pending_operation, None);

        let mut blocked = super::super::blocked::BlockedTables::load(&store, &source).unwrap();
        blocked.record(id, "publication_changed", None).unwrap();
        super::super::settle_dropped_table(&mut ledger, &store, &mut blocked, id).unwrap();
        ledger.drain_completed_prefix().unwrap();
        assert_eq!(ledger.watermarks().materialized_lsn, PgLsn(10));
    }

    #[tokio::test]
    async fn a_dropped_table_settles_its_unfinished_operation_and_stays_blocked() {
        settle_unfinished_operation(Target::Present).await;
        settle_unfinished_operation(Target::Dropped).await;
        settle_unfinished_operation(Target::Replaced).await;
    }
}

#[cfg(test)]
mod metadata_maintenance_tests {
    use super::*;
    use flow_iceberg_ext::RowDeltaAction;
    use flow_materializer::DataWriter;
    use flow_model::{PgLsn, Value};
    use flow_state_store::{IndexDelta, PreparedOperation};
    use iceberg::{
        CatalogBuilder, NamespaceIdent, TableCreation,
        memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder},
    };

    struct Fixture {
        _root: tempfile::TempDir,
        journal: flow_ingress_journal::Journal,
        work: TableWork,
        schema: TableSchema,
        head: Table,
    }

    /// A target with `commits` Flow-indexed snapshots.
    async fn fixture(limits: impl FnOnce(&mut crate::config::Limits), commits: u64) -> Fixture {
        let root = tempfile::tempdir().unwrap();
        let mut config: Config =
            toml::from_str(include_str!("../../../../examples/flow.toml")).unwrap();
        config.state_dir = root.path().join("state");
        limits(&mut config.limits);
        config.validate().unwrap();
        let schema = config.tables[0].schema(7);
        let id = schema.table_id;
        let control = ControlStore::open(root.path().join("control")).unwrap();
        let store = control
            .initialize_index(root.path().join("index"), StateStoreOptions::default())
            .unwrap();
        let catalog: Arc<dyn Catalog> = Arc::new(
            MemoryCatalogBuilder::default()
                .load(
                    "retention",
                    std::collections::HashMap::from([(
                        MEMORY_CATALOG_WAREHOUSE.into(),
                        "memory://retention".into(),
                    )]),
                )
                .await
                .unwrap(),
        );
        let namespace = NamespaceIdent::new("replicated".into());
        catalog
            .create_namespace(&namespace, std::collections::HashMap::new())
            .await
            .unwrap();
        let mut head = catalog
            .create_table(
                &namespace,
                TableCreation::builder()
                    .name("orders".into())
                    .schema(flow_materializer::iceberg_schema(&schema).unwrap())
                    .build(),
            )
            .await
            .unwrap();
        for n in 1..=commits {
            let operation = OperationId(format!("seed-{n}"));
            let row = vec![Value::Int64(n as i64), Value::String("kept".into())];
            let lsn = PgLsn(n * 10);
            let mut writer = DataWriter::new(
                head.file_io().clone(),
                head.metadata().location(),
                &operation,
                schema.clone(),
                0,
                flow_materializer::WriterConfig::default(),
            )
            .unwrap();
            let mut locations = writer
                .write(std::slice::from_ref(&row), lsn)
                .await
                .unwrap()
                .locations;
            let committed = RowDeltaAction::new(&head, operation.0.clone())
                .add_data_files(writer.close().await.unwrap())
                .commit(catalog.as_ref(), &head)
                .await
                .unwrap();
            for location in &mut locations {
                location.data_sequence_number = committed.sequence_number;
            }
            store
                .prepare(
                    PreparedOperation {
                        id: operation.clone(),
                        table_id: id,
                        kind: OperationKind::Ingest,
                        base_snapshot_id: head.metadata().current_snapshot_id(),
                        last_lsn: lsn,
                        schema_version: schema.version,
                        artifacts: vec![],
                        payload: vec![],
                    },
                    locations.into_iter().map(|location| IndexDelta {
                        key: schema.encode_key(&row).unwrap(),
                        expected: None,
                        replacement: Some(location),
                    }),
                )
                .unwrap();
            store
                .mark_committed(&operation, committed.snapshot_id, committed.sequence_number)
                .unwrap();
            store.apply_committed(&operation).unwrap();
            store.forget_applied(&operation).unwrap();
            head = committed.table;
        }
        let (journal, _) = flow_ingress_journal::Journal::open(
            root.path().join("journal"),
            flow_ingress_journal::JournalConfig::default(),
        )
        .unwrap();
        let writer = crate::services::writer_config(&config);
        let work = TableWork {
            config: Arc::new(config.clone()),
            store: store.clone(),
            control,
            reader: journal.reader(),
            catalog: catalog.clone(),
            targets: Arc::new(BTreeMap::from([(
                id,
                (head.identifier().clone(), head.metadata().uuid()),
            )])),
            publisher: Arc::new(
                TablePublisher::new(
                    store.clone(),
                    catalog.clone(),
                    writer.clone(),
                    config.limits.batch_rows,
                    config.limits.batch_bytes,
                )
                .unwrap(),
            ),
            maintenance: Arc::new(
                TableMaintenance::new(store, catalog, config.compaction.clone(), writer).unwrap(),
            ),
            garbage_checked: Arc::default(),
            metadata_backoff: Arc::default(),
            compaction: false,
        };
        Fixture {
            _root: root,
            journal,
            work,
            schema,
            head,
        }
    }

    fn options() -> WorkOptions {
        WorkOptions {
            periodic_maintenance: false,
            build_active: false,
            build_admission: BuildAdmission::Wait,
            actor_acquired_at: Instant::now(),
        }
    }

    async fn load(f: &Fixture) -> Table {
        f.work
            .catalog
            .load_table(f.head.identifier())
            .await
            .unwrap()
    }

    /// Journal one committed source transaction inserting `id`.
    fn journal_insert(f: &mut Fixture, id: i64, lsn: u64) -> SourceTransaction {
        use flow_model::{JournalChunks, Mutation, MutationKind, TableMutationCount};
        let table_id = f.schema.table_id;
        let mutations = vec![Mutation {
            table_id,
            schema_version: f.schema.version,
            kind: MutationKind::Insert {
                row: vec![Value::Int64(id), Value::String("published".into())],
            },
        }];
        f.journal
            .append_chunk(lsn as u32, &bincode::serialize(&mutations).unwrap())
            .unwrap();
        let mutation_chunks: JournalChunks = f.journal.transaction_chunks(lsn as u32);
        let transaction = SourceTransaction {
            source_id: SourceId(f.work.config.source.id.clone()),
            xid: lsn as u32,
            begin_lsn: PgLsn(lsn - 2),
            commit_lsn: PgLsn(lsn - 1),
            end_lsn: PgLsn(lsn),
            commit_timestamp_micros: 0,
            affected_tables: vec![table_id],
            schema_versions: vec![flow_model::TableSchemaVersion {
                table_id,
                version: f.schema.version,
            }],
            table_mutation_counts: Some(vec![TableMutationCount {
                table_id,
                mutations: 1,
            }]),
            mutation_chunks,
        };
        f.journal.commit(transaction.clone()).unwrap();
        transaction
    }

    /// Mandatory metadata work runs in the publication path after the epoch
    /// is published, independently of source-pressure admission: manifests
    /// past twice their limit are rewritten, then history far past the cap
    /// is expired inside the reader window.
    #[tokio::test]
    async fn publication_runs_mandatory_maintenance_after_publishing() {
        let mut f = fixture(
            |limits| {
                limits.manifest_max_count = 2;
                limits.snapshot_retain_last = 1;
                limits.snapshot_max_count = 3;
            },
            6,
        )
        .await;
        let before = f.head.metadata().current_snapshot_id();
        // Garbage collection is not due, so the refreshed manifest count and
        // history alone decide whether a periodic visit follows.
        f.work
            .garbage_checked
            .lock()
            .unwrap()
            .insert(f.schema.table_id, Instant::now());
        let transaction = journal_insert(&mut f, 100, 1000);
        let completion = f
            .work
            .clone()
            .run(f.schema.clone(), vec![transaction], options(), None, None)
            .await
            .unwrap();
        let TableOutcome::Complete {
            operation: Some(operation),
            snapshot: Some(published),
            maintenance_pending,
        } = completion.outcome
        else {
            panic!("expected a published epoch");
        };
        assert!(!maintenance_pending.periodic);
        let head = load(&f).await;
        // Published on the old head, then consolidated and expired.
        let epoch = flow_iceberg_ext::find_operation(head.metadata(), &operation.0).unwrap();
        assert_eq!(epoch.snapshot_id(), published);
        assert_eq!(epoch.parent_snapshot_id(), before);
        assert_eq!(
            head.metadata()
                .current_snapshot()
                .unwrap()
                .parent_snapshot_id(),
            Some(published)
        );
        assert_eq!(manifest_count(&head).await.unwrap(), 1);
        assert_eq!(head.metadata().snapshots().len(), 3);
        let indexed = f.work.store.table_state(&f.schema.table_id).unwrap();
        assert_eq!(indexed.snapshot_id, head.metadata().current_snapshot_id());
        assert_eq!(indexed.materialized_lsn, PgLsn(1000));

        // Small overruns wait for periodic maintenance instead of adding a
        // commit to every epoch.
        let transaction = journal_insert(&mut f, 101, 1010);
        f.work
            .clone()
            .run(f.schema.clone(), vec![transaction], options(), None, None)
            .await
            .unwrap();
        assert_eq!(load(&f).await.metadata().snapshots().len(), 4);
    }

    /// A bounded rewrite that stays above the hard limit is not retried by
    /// every epoch; periodic maintenance continues it.
    #[tokio::test]
    async fn mandatory_manifest_rewrite_that_cannot_reach_the_limit_stalls() {
        let f = fixture(|limits| limits.manifest_max_count = 2, 70).await;
        let id = f.schema.table_id;
        assert_eq!(manifest_count(&f.head).await.unwrap(), 70);
        let (head, _, manifests) = f
            .work
            .maintain_metadata(f.head.clone(), id, 70, MetadataScope::Mandatory)
            .await
            .unwrap();
        assert_eq!(manifests, manifest_count(&head).await.unwrap());
        assert!((4..70).contains(&manifests), "{manifests}");
        assert!(
            !f.work
                .manifests_due(id, manifests, MetadataScope::Mandatory)
                .unwrap()
        );
        assert!(
            f.work
                .manifests_due(id, manifests, MetadataScope::Full)
                .unwrap()
        );
        let (again, _, _) = f
            .work
            .maintain_metadata(head.clone(), id, manifests, MetadataScope::Mandatory)
            .await
            .unwrap();
        assert_eq!(
            again.metadata().current_snapshot_id(),
            head.metadata().current_snapshot_id()
        );
    }

    /// History above the cap that checkpoints protect is reported, but it is
    /// never due: expiration could not remove anything.
    #[tokio::test]
    async fn protected_history_above_the_cap_is_not_due() {
        let f = fixture(
            |limits| {
                limits.snapshot_retain_last = 1;
                limits.snapshot_max_count = 2;
            },
            6,
        )
        .await;
        let id = f.schema.table_id;
        assert!(
            f.work
                .history_due(&f.head, id, MetadataScope::Mandatory)
                .await
                .unwrap()
        );
        let oldest = f
            .head
            .metadata()
            .snapshots()
            .min_by_key(|snapshot| snapshot.sequence_number())
            .unwrap()
            .snapshot_id();
        let plan = f
            .work
            .maintenance
            .history_plan(
                &f.head,
                id,
                &f.work.config.limits.history_policy(),
                &std::collections::BTreeSet::from([oldest]),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(plan.over_cap(), 4);
        assert!(!plan.due() && !plan.over_limit());
    }

    #[tokio::test]
    async fn gc_disabled_target_is_skipped_without_error() {
        let f = fixture(
            |limits| {
                limits.snapshot_retain_last = 1;
                limits.snapshot_max_count = 1;
            },
            3,
        )
        .await;
        let transaction = iceberg::transaction::Transaction::new(&f.head);
        let head = {
            use iceberg::transaction::ApplyTransactionAction;
            transaction
                .update_table_properties()
                .set("gc.enabled".into(), "false".into())
                .apply(transaction)
                .unwrap()
                .commit(f.work.catalog.as_ref())
                .await
                .unwrap()
        };
        let id = f.schema.table_id;
        // Far past the cap, but another process owns its history.
        assert!(
            !f.work
                .history_due(&head, id, MetadataScope::Full)
                .await
                .unwrap()
        );
        assert!(
            !f.work
                .history_due(&head, id, MetadataScope::Mandatory)
                .await
                .unwrap()
        );
        let (head, _, _) = f
            .work
            .maintain_metadata(head, id, 0, MetadataScope::Full)
            .await
            .unwrap();
        assert_eq!(head.metadata().snapshots().len(), 3);
        assert!(f.work.metadata_backoff.lock().unwrap().failures.is_empty());
    }

    /// A maintenance-only failure is table-scoped: it is retried with backoff
    /// and neither stops publication nor exits the process.
    #[tokio::test]
    async fn maintenance_failure_backs_off_without_stopping_publication() {
        let f = fixture(
            |limits| {
                limits.snapshot_retain_last = 1;
                limits.snapshot_max_count = 1;
            },
            3,
        )
        .await;
        let id = f.schema.table_id;
        // An external commit the index has not reconciled makes expiration fail.
        let external = {
            use iceberg::transaction::ApplyTransactionAction;
            let transaction = iceberg::transaction::Transaction::new(&f.head);
            transaction
                .fast_append()
                .set_snapshot_properties(std::collections::HashMap::from([(
                    "external".into(),
                    "true".into(),
                )]))
                .apply(transaction)
                .unwrap()
                .commit(f.work.catalog.as_ref())
                .await
                .unwrap()
        };
        f.work
            .garbage_checked
            .lock()
            .unwrap()
            .insert(id, Instant::now());
        assert!(
            f.work
                .history_due(&external, id, MetadataScope::Full)
                .await
                .unwrap()
        );
        f.work
            .maintain_metadata(external.clone(), id, 0, MetadataScope::Full)
            .await
            .unwrap();
        assert_eq!(load(&f).await.metadata().snapshots().len(), 4);
        assert!(
            !f.work
                .history_due(&external, id, MetadataScope::Full)
                .await
                .unwrap()
        );
        assert!(
            !f.work
                .history_due(&external, id, MetadataScope::Mandatory)
                .await
                .unwrap()
        );
        assert!(!f.work.metadata_due(&external, id, 0).await.unwrap());
        let (retry_at, failures) =
            f.work.metadata_backoff.lock().unwrap().failures[&(id, MetadataTask::History)];
        assert_eq!(failures, 1);
        assert!(retry_at > Instant::now() + METADATA_RETRY_MIN / 2);
    }

    #[test]
    fn failure_backoff_doubles_is_capped_and_clears_on_success() {
        let mut backoff = MetadataBackoff::default();
        let id = TableId(1);
        let now = Instant::now();
        let failures: Vec<_> = (0..8)
            .map(|_| backoff.fail(id, MetadataTask::Garbage, now))
            .collect();
        assert_eq!(failures[0], (METADATA_RETRY_MIN, 1));
        assert_eq!(failures[1], (METADATA_RETRY_MIN * 2, 2));
        assert_eq!(*failures.last().unwrap(), (METADATA_RETRY_MAX, 8));
        assert!(backoff.failed_until(id, MetadataTask::Garbage, now));
        assert!(!backoff.failed_until(id, MetadataTask::History, now));
        assert!(!backoff.failed_until(TableId(2), MetadataTask::Garbage, now));
        assert_eq!(backoff.succeed(id, MetadataTask::Garbage), 8);
        assert!(!backoff.failed_until(id, MetadataTask::Garbage, now));
        assert_eq!(backoff.succeed(id, MetadataTask::Garbage), 0);
        backoff.stall(id, MetadataTask::Manifests, now + MANDATORY_METADATA_STALL);
        assert!(backoff.stalled_until(id, MetadataTask::Manifests, now));
        assert!(!backoff.failed_until(id, MetadataTask::Manifests, now));
    }

    #[tokio::test]
    async fn repeated_failures_escalate_until_the_task_succeeds() {
        use metrics_exporter_prometheus::PrometheusBuilder;
        let recorder = PrometheusBuilder::new().build_recorder();
        let metrics = recorder.handle();
        let _guard = metrics::set_default_local_recorder(&recorder);
        let f = fixture(|_| {}, 1).await;
        let id = f.schema.table_id;
        let failing = || {
            format!(
                "flow_table_maintenance_failing{{table_id=\"{}\",task=\"garbage_collection\"}} ",
                id.0
            )
        };
        let invariant =
            || Err::<(), _>(anyhow::anyhow!("artifact registry table identity mismatch"));
        for _ in 1..METADATA_FAILURE_ESCALATION {
            assert!(
                f.work
                    .settle(id, MetadataTask::Garbage, invariant())
                    .unwrap()
                    .is_none()
            );
        }
        assert!(!metrics.render().contains(&format!("{}1", failing())));
        assert!(
            f.work
                .settle(id, MetadataTask::Garbage, invariant())
                .unwrap()
                .is_none()
        );
        assert!(metrics.render().contains(&format!("{}1", failing())));
        assert!(
            f.work
                .settle(id, MetadataTask::Garbage, Ok(()))
                .unwrap()
                .is_some()
        );
        assert!(metrics.render().contains(&format!("{}0", failing())));
    }

    #[tokio::test]
    async fn settle_keeps_local_failures_fatal_and_blocks_only_unresolved_operations() {
        let f = fixture(|_| {}, 1).await;
        let id = f.schema.table_id;
        let remote = || {
            Err::<(), _>(anyhow::Error::new(iceberg::Error::new(
                iceberg::ErrorKind::DataInvalid,
                "Cannot expire snapshots",
            )))
        };
        assert!(
            f.work
                .settle(id, MetadataTask::History, remote())
                .unwrap()
                .is_none()
        );
        let disk = Err::<(), _>(anyhow::Error::new(std::io::Error::other("disk")));
        assert!(f.work.settle(id, MetadataTask::History, disk).is_err());
        let corrupt = Err::<(), _>(anyhow::Error::new(
            flow_state_store::Error::AuthorityCorruption("bad".into()),
        ));
        assert!(f.work.settle(id, MetadataTask::History, corrupt).is_err());
        let replan = Err::<(), _>(ReplanRequired.into());
        assert!(f.work.settle(id, MetadataTask::Garbage, replan).is_err());

        // A failure that left an operation must be recovered before this
        // table publishes again; it blocks only this table and is retried.
        f.work
            .store
            .begin_prepare(PreparedOperation {
                id: OperationId("manifest-rewrite-unresolved".into()),
                table_id: id,
                kind: OperationKind::ManifestRewrite,
                base_snapshot_id: f.head.metadata().current_snapshot_id(),
                last_lsn: PgLsn(10),
                schema_version: f.schema.version,
                artifacts: vec![],
                payload: vec![],
            })
            .unwrap();
        let error = f
            .work
            .settle(id, MetadataTask::Manifests, remote())
            .unwrap_err();
        assert_eq!(publication_error_code(&error), Some("maintenance_pressure"));
        assert!(retry_table_work(&error));
    }
}
