//! Recover durable state and coordinate source capture, publication, and maintenance.

pub(crate) mod blocked;
mod health;
mod pending;
mod table;

use self::{
    blocked::{BlockedTables, PUBLICATION_CHANGED},
    health::{HealthCheckResult, check_health, observe_health, record_publication_changed},
    pending::{EPOCH_MAX_BYTES, EPOCH_MUTATION_TRIGGER, PendingWork, schedule_transactions},
    table::{
        BuildAdmission, CompactionCandidate, CompletedBuild, CompletedPreparation,
        FinalizationLane, StartedBuild, StartedPreparation, TableCompletion, TableOutcome,
        TableWork, WorkOptions, retry_table_work,
    },
};
use crate::{
    bootstrap::{bootstrap, persist_bootstrap, tables},
    config::Config,
    lifecycle::SourceHealthStatus,
    services::{catalog, journal_config, ledger},
    source::{CaptureProgress, PublicationChanged, capture_loop, connect, validate_publication},
};
use anyhow::{Context, Result, bail, ensure};
use flow_coordinator::{
    PreparationWait, Priority, ReadyCompaction, ReplanRequired, Scheduler, SourceLedger,
    TableMaintenance, TablePublisher,
};
use flow_ingress_journal::Journal;
use flow_model::{PgLsn, SourceId, TableId, TableSchema};
use flow_pg_source::Acknowledgement;
use flow_state_store::{ControlStore, OperationKind, StateStore};
use futures::{FutureExt, StreamExt, future::BoxFuture, stream::FuturesUnordered};
use std::{
    collections::{BTreeMap, BTreeSet, HashSet, VecDeque},
    future::Future,
    path::PathBuf,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context as TaskContext, Poll},
    time::{Duration, Instant},
};
use tokio::sync::watch;

pub use crate::bootstrap::initialize;

pub fn status(config: Config) -> Result<()> {
    println!(
        "{}",
        serde_json::to_string_pretty(&crate::lifecycle::read(&config)?)?
    );
    Ok(())
}

pub async fn run(config: Config, compaction: bool) -> Result<()> {
    let mut observation = crate::observation::Observation::install()?;
    let control = ControlStore::open(config.state_dir.join("control"))?;
    let opened = crate::generation::open(&config, control.clone());
    let _lifecycle = crate::lifecycle::Lifecycle::start(&config)?;
    crate::lifecycle::refuse_if_resync_required(&config)?;
    let mut boot = bootstrap(&control)?;
    crate::bootstrap::validate_identity(&config, &boot)?;
    ensure!(
        boot.schemas.len() == config.tables.len(),
        "configured tables changed; explicit resynchronization is required"
    );
    for (index, configured) in config.tables.iter().enumerate() {
        ensure!(
            configured.schema(boot.schemas[index].table_id.0) == boot.schemas[index]
                && (
                    configured.target_namespace.clone(),
                    configured.target_table.clone()
                ) == boot.targets[index],
            "configured schema or target changed; apply a coordinated schema migration before restarting"
        );
    }
    let catalog = catalog(&config).await?;
    // An intact completed bootstrap has durable target identity. Load each
    // target in its worker so a remote table outage cannot block startup.
    // Bootstrap, legacy identity adoption and index rebuild still need all tables.
    let isolated_start =
        opened.is_ok() && boot.copied && boot.layout > 0 && !boot.target_uuids.is_empty();
    let tables = if isolated_start {
        BTreeMap::new()
    } else {
        tables(catalog.as_ref(), &boot).await?
    };
    if boot.target_uuids.is_empty() {
        // Upgrade only an intact legacy index with retained source history.
        // A missing index cannot establish the identity of a replaced target.
        let store = opened.as_ref().map_err(|error| {
            anyhow::anyhow!(
                "legacy target identity cannot be established after index loss: {error}"
            )
        })?;
        for schema in &boot.schemas {
            let table = &tables[&schema.table_id];
            let indexed = store.table_state(&schema.table_id)?;
            ensure!(
                indexed
                    .snapshot_id
                    .is_some_and(|id| table.metadata().snapshot_by_id(id).is_some())
                    && table.metadata().snapshots().any(|snapshot| snapshot
                        .summary()
                        .additional_properties
                        .get("streaming.source-id")
                        == Some(&boot.source_id)),
                "legacy target identity has no retained source proof; explicit migration is required"
            );
            boot.target_uuids.push(table.metadata().uuid());
        }
        persist_bootstrap(store, &boot)?;
    }
    ensure!(
        boot.target_uuids.len() == boot.schemas.len(),
        "invalid persisted target identities"
    );
    for (schema, uuid) in boot.schemas.iter().zip(&boot.target_uuids) {
        ensure!(
            tables
                .get(&schema.table_id)
                .is_none_or(|table| table.metadata().uuid() == *uuid),
            "target table was replaced; refusing to reuse its source watermark"
        );
    }
    let store = match opened {
        Ok(store) => store,
        Err(error)
            if error
                .downcast_ref::<flow_state_store::Error>()
                .is_some_and(flow_state_store::Error::requires_index_rebuild) =>
        {
            tracing::warn!(%error, "rebuilding the row index from durable control and Iceberg");
            let mut attempt = 0u32;
            loop {
                match crate::generation::rebuild(
                    &config,
                    control.clone(),
                    catalog.clone(),
                    &boot.schemas,
                    &tables,
                )
                .await
                {
                    Ok(store) => break store,
                    Err(error)
                        if crate::retry::transient(&error)
                            || error.downcast_ref::<ReplanRequired>().is_some() =>
                    {
                        tracing::warn!(%error, attempt, "index recovery interrupted; retrying with durable authority");
                        tokio::time::sleep(crate::retry::delay(attempt)).await;
                        attempt = attempt.saturating_add(1);
                    }
                    Err(error) => return Err(error),
                }
            }
        }
        Err(error) => return Err(error),
    };
    // Recovery may have loaded every table's snapshot history. Workers reload
    // targets on admission, so do not retain this inventory for the daemon's lifetime.
    drop(tables);
    if !boot.copied || boot.layout == 0 {
        let resumed =
            crate::bootstrap::resume(&config, store.clone(), catalog.clone(), &mut boot).await;
        crate::lifecycle::record_publication_changed(&config, resumed)?;
    }
    // Recovery may truncate only a torn tail above what the ledger recorded.
    let mut ledger = ledger(&store, &config)?;
    let (journal, recovery) = Journal::open_with_floor(
        config.state_dir.join("journal"),
        journal_config(&config),
        ledger.watermarks().journal_durable_lsn,
    )?;
    ensure!(
        journal.durable_lsn() >= ledger.watermarks().journal_durable_lsn,
        "journal lost previously durable transactions; source recovery is required before acknowledgement"
    );
    if recovery.truncated_bytes > 0 {
        tracing::warn!(
            bytes = recovery.truncated_bytes,
            "journal tail truncated during recovery"
        );
    }
    observation.table_sources(&config, &boot.schemas);
    {
        let sql = connect(&config, false).await?;
        let mut registry = crate::schema::SchemaRegistry::new(
            store.clone(),
            SourceId(config.source.id.clone()),
            &boot.schemas,
        )?;
        let validated = validate_publication(&sql, &config, &boot.schemas, false).await;
        // Table-scoped violations block only those tables; the rest start.
        // Latch them before schema refresh, which could otherwise quarantine.
        for violation in crate::lifecycle::record_publication_changed(&config, validated)? {
            registry.block_publication(violation.table, &violation.reason)?;
        }
        registry.initialize(&sql, &config.tables).await?;
    }
    let publisher = Arc::new(TablePublisher::new(
        store.clone(),
        catalog.clone(),
        crate::services::writer_config(&config),
        config.limits.batch_rows,
        config.limits.batch_bytes,
    )?);
    let maintenance = Arc::new(
        TableMaintenance::new(
            store.clone(),
            catalog.clone(),
            config.compaction.clone(),
            crate::services::writer_config(&config),
        )?
        .with_read_limits(config.parquet_read.clone())?,
    );
    for operation in store.pending_operations()? {
        ensure!(
            boot.schemas
                .iter()
                .any(|schema| schema.table_id == operation.operation.table_id),
            "pending operation has no configured table"
        );
        ensure!(
            matches!(
                operation.operation.kind,
                OperationKind::Ingest
                    | OperationKind::Rewrite
                    | OperationKind::Reconcile
                    | OperationKind::ManifestRewrite
            ),
            "index reconciliation must complete before source capture"
        );
    }
    let retired = reconcile_source_progress(&store, &mut ledger)?;
    if retired > 0 {
        tracing::info!(
            retired,
            "retired applied operations after source-ledger recovery"
        );
    }
    let abandoned = flow_coordinator::discard_abandoned_builds(&store)?;
    if abandoned > 0 {
        tracing::info!(
            abandoned,
            "discarded compaction builds from the previous process"
        );
    }
    // The control database excludes another daemon, recovery is complete and
    // no worker has started. These stores contain temporary mappings only.
    remove_abandoned_scratch(&config)?;
    let reader = journal.reader();
    let (events, mut receive) = watch::channel(CaptureProgress {
        durable_lsn: journal.durable_lsn(),
        error: None,
        publication_changed: false,
    });
    let (ack_send, ack_receive) = watch::channel(feedback(&ledger));
    let capture_config = config.clone();
    let capture_schemas = boot.schemas.clone();
    let capture_store = store.clone();
    let capture = tokio::task::spawn_blocking(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?
            .block_on(capture_loop(
                capture_config,
                capture_schemas,
                capture_store,
                journal,
                events,
                ack_receive,
            ))
    });
    // Workers and health checks share the immutable admission configuration.
    // Evolving source schemas are read from the durable schema registry.
    let config = Arc::new(config);
    let publishing = PublishRuntime {
        work: TableWork {
            config: config.clone(),
            store: store.clone(),
            control,
            reader,
            catalog,
            targets: Arc::new(
                boot.schemas
                    .iter()
                    .zip(&boot.targets)
                    .zip(&boot.target_uuids)
                    .map(|((schema, (namespace, name)), uuid)| {
                        Ok((
                            schema.table_id,
                            (crate::services::target(namespace, name)?, *uuid),
                        ))
                    })
                    .collect::<Result<_>>()?,
            ),
            publisher,
            maintenance,
            garbage_checked: Arc::default(),
            compaction,
        },
        profiles: boot
            .schemas
            .iter()
            .zip(&config.tables)
            .map(|(schema, table)| (schema.table_id, table.priority))
            .collect(),
        schemas: boot
            .schemas
            .into_iter()
            .map(|schema| (schema.table_id, schema))
            .collect(),
    };
    let result = publishing
        .run(&mut ledger, &mut receive, &ack_send, &mut observation)
        .await;
    drop(receive);
    drop(ack_send);
    // Closing both channels interrupts a healthy capture actor. Socket operations
    // may finish later, so do not wait indefinitely during shutdown.
    match tokio::time::timeout(Duration::from_secs(10), capture).await {
        Ok(joined) => {
            joined??;
        }
        Err(_) => tracing::warn!("capture shutdown timed out; journal remains replayable"),
    }
    result
}

/// Capture drops a `publication_changed` table's changes; only a resync
/// recovers it. Call only while no run owns the table: its outcome may still
/// complete its transactions at a new snapshot, which must not race an earlier
/// completion. Builds and preparations never complete transactions.
/// - An unfinished operation first needs a recovery-only attempt; its
///   transactions complete through the ordinary applied-operation path.
/// - Otherwise one bounded page of transactions the table journaled before the
///   latch completes at its unchanged snapshot, so they stop holding the
///   acknowledgement frontier. Returns whether a page was completed.
fn settle_dropped_table(
    ledger: &mut SourceLedger,
    store: &StateStore,
    blocked: &mut BlockedTables,
    table: TableId,
) -> Result<bool> {
    let state = store.table_state(&table)?;
    if state.pending_operation.is_some() {
        blocked.schedule_recovery(table);
        return Ok(false);
    }
    let ends = ledger
        .pending_table_transactions_after(table, PgLsn(0))
        .take(ledger.batch_capacity())
        .map(|transaction| transaction.map(|transaction| transaction.end_lsn))
        .collect::<Result<Vec<_>>>()?;
    if ends.is_empty() {
        return Ok(false);
    }
    ledger.table_materialized_batch(&ends, table, state.snapshot_id.unwrap_or(0))?;
    Ok(true)
}

/// A table that will never publish again cannot activate compaction, and its
/// finished candidates would hold build slots forever. Take them for
/// retirement and release the table's build state. A build or preparation
/// still running completes into these maps and is released on a later tick.
fn release_blocked_candidates<B, P>(
    table: TableId,
    ready_builds: &mut BTreeMap<TableId, B>,
    ready_preparations: &mut BTreeMap<TableId, P>,
    build_active: &mut HashSet<TableId>,
    activation_pending: &mut HashSet<TableId>,
    waiting_for_build: &mut HashSet<TableId>,
    retiring_builds: &mut HashSet<TableId>,
) -> (Option<B>, Option<P>) {
    let released = (
        ready_builds.remove(&table),
        ready_preparations.remove(&table),
    );
    if released.0.is_some() || released.1.is_some() {
        build_active.remove(&table);
        activation_pending.remove(&table);
        waiting_for_build.remove(&table);
        retiring_builds.insert(table);
    }
    released
}

fn feedback(ledger: &SourceLedger) -> Acknowledgement {
    Acknowledgement {
        received: ledger.watermarks().received_lsn,
        durable: ledger.acknowledgement(),
        materialized: ledger.watermarks().materialized_lsn,
    }
}

fn reconcile_source_progress(store: &StateStore, ledger: &mut SourceLedger) -> Result<usize> {
    // An Applied ingest operation is the durable proof used to recover the
    // crash between index application and ledger completion. Never remove it
    // until reconciliation has persisted that source progress.
    ledger.reconcile_table_progress()?;
    let mut retired = 0usize;
    loop {
        let operations = store.applied_operations(store.batch_rows())?;
        if operations.is_empty() {
            return Ok(retired);
        }
        let ids = operations
            .into_iter()
            .map(|record| record.operation.id)
            .collect::<Vec<_>>();
        store.forget_applied_batch(&ids)?;
        retired = retired
            .checked_add(ids.len())
            .context("applied-operation retirement count overflow")?;
    }
}

fn remove_abandoned_scratch(config: &Config) -> Result<()> {
    for name in ["compaction", "reconcile"] {
        let directory = config.state_dir.join(name);
        let entries = match std::fs::read_dir(&directory) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error.into()),
        };
        for entry in entries {
            let entry = entry?;
            if entry.file_type()?.is_dir()
                && entry.file_name().to_str().is_some_and(|name| {
                    uuid::Uuid::parse_str(name).is_ok_and(|id| id.to_string() == name)
                })
            {
                std::fs::remove_dir_all(entry.path())?;
            }
        }
    }
    Ok(())
}

struct PublishRuntime {
    work: TableWork,
    schemas: BTreeMap<TableId, TableSchema>,
    profiles: BTreeMap<TableId, Priority>,
}

/// WAL pressure must reduce warehouse work that cannot advance the source ACK.
/// Existing candidates and mandatory recovery keep their ownership and may finish.
#[derive(Default)]
struct MaintenanceAdmission {
    source_pressure: bool,
}

impl MaintenanceAdmission {
    fn observe(&mut self, health: SourceHealthStatus) {
        match health {
            SourceHealthStatus::Healthy => self.source_pressure = false,
            SourceHealthStatus::Warning
            | SourceHealthStatus::AtRisk
            | SourceHealthStatus::SlotLost
            | SourceHealthStatus::PublicationChanged => {
                self.source_pressure = true;
            }
            // Losing the health connection is not proof that WAL pressure cleared.
            SourceHealthStatus::Unknown | SourceHealthStatus::Unavailable => {}
        }
    }

    fn optional_allowed(
        &self,
        table_workers: usize,
        free_workers: usize,
        cdc_waiting: bool,
    ) -> bool {
        // A shared single actor keeps its bounded CDC/maintenance alternation.
        // Pools can reserve a separate lane before starting speculative work.
        !self.source_pressure && free_workers > usize::from(table_workers > 1 && cdc_waiting)
    }
}

impl PublishRuntime {
    async fn run(
        self,
        ledger: &mut SourceLedger,
        receive: &mut watch::Receiver<CaptureProgress>,
        ack: &watch::Sender<Acknowledgement>,
        observation: &mut crate::observation::Observation,
    ) -> Result<()> {
        let Self {
            work,
            schemas,
            profiles,
        } = self;
        let config = &work.config;
        let store = &work.store;
        let mut blocked = BlockedTables::load(store, &SourceId(config.source.id.clone()))?;
        let mut excluded: HashSet<_> = blocked.ids().collect();
        let mut recovering: HashSet<TableId>;
        let mut retrying = HashSet::new();
        let mut pending = PendingWork::default();
        let mut scheduler = Scheduler::new(
            EPOCH_MUTATION_TRIGGER,
            EPOCH_MAX_BYTES,
            config.limits.commits_per_second,
            Instant::now(),
        )?;
        pending.refill(
            ledger,
            &mut scheduler,
            &profiles,
            config.limits.pending_transactions,
            &excluded,
        )?;
        let mut capture_goal = receive.borrow_and_update().durable_lsn;
        let mut busy = HashSet::new();
        let mut running = FuturesUnordered::<BoxFuture<'static, Result<TableCompletion>>>::new();
        // At most two speculative builds share the worker budget with CDC,
        // leaving one actor slot reusable for publication. Reservations span
        // capture, build, preparation, ready candidates and finalization.
        let background_build_limit = config.limits.table_workers.saturating_sub(1).min(2);
        let mut build_active = HashSet::new();
        let mut builds = FuturesUnordered::<BoxFuture<'static, BuildCompletion>>::new();
        let mut ready_builds = BTreeMap::<TableId, CompletedBuild>::new();
        let mut preparations = FuturesUnordered::<BoxFuture<'static, PreparationCompletion>>::new();
        let mut retirements = FuturesUnordered::<BoxFuture<'static, RetirementCompletion>>::new();
        let mut ready_preparations = BTreeMap::<TableId, CompletedPreparation>::new();
        let mut waiting_for_build = HashSet::new();
        // Timed-out workers retain durable ownership and resource accounting,
        // but are no longer candidates that may pause foreground publication.
        // New builds wait until every retiring worker has joined.
        let mut retiring_builds = HashSet::new();
        // Preparation reads a frozen index snapshot. Briefly gate this table's
        // next epoch, then allow CDC while slower candidates finish off-lane.
        let mut activation_pending = HashSet::new();
        // Publication-blocked tables with journaled transactions left to settle.
        let mut settling = BTreeSet::new();
        let mut fenced_fallback = HashSet::new();
        let mut health = tokio::time::interval(Duration::from_secs(5));
        health.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut health_client = None;
        let mut health_checks = FuturesUnordered::new();
        let mut checkpoints = FuturesUnordered::new();
        let mut idle_work: VecDeque<_> = schemas
            .keys()
            .filter(|id| !excluded.contains(id) && blocked.get(**id).is_none())
            .copied()
            .collect();
        // Quiet tables also retain maintenance debt across restart. The idle
        // queue yields to CDC and may be cleared during catch-up; seed the
        // bounded fair-admission queues without requiring a new source write.
        let first_maintenance = Instant::now() + OPTIONAL_MAINTENANCE_DELAY;
        let mut maintenance_due: BTreeMap<TableId, Instant> = idle_work
            .iter()
            .map(|id| (*id, first_maintenance))
            .collect();
        let mut periodic_due = maintenance_due.clone();
        let mut periodic_active = false;
        let mut periodic_yield_to_cdc = false;
        let mut retry_yield_to_cdc = false;
        let mut maintenance_admission = MaintenanceAdmission::default();
        let mut checkpoint_at = Instant::now();
        let shutdown = crate::lifecycle::shutdown_signal();
        tokio::pin!(shutdown);
        observation.table_states(store, &blocked, schemas.keys().copied())?;
        observation.write(config, ledger, capture_goal, true)?;
        loop {
            recovering = store
                .applied_operations(schemas.len().max(1))?
                .into_iter()
                .map(|operation| operation.operation.table_id)
                .filter(|id| !busy.contains(id))
                .collect();
            excluded = blocked
                .ids()
                .filter(|id| !retrying.contains(id))
                .chain(recovering.iter().copied())
                .collect();
            for id in &recovering {
                pending.defer(*id, &mut scheduler);
            }
            let free_workers = config.limits.table_workers.saturating_sub(
                running.len() + builds.len() + preparations.len() + retirements.len(),
            );
            let available = free_workers > 0;
            let optional_allowed = maintenance_admission.optional_allowed(
                config.limits.table_workers,
                free_workers,
                pending.has_work()
                    || pending.has_runnable_unloaded(ledger, &excluded)
                    || ledger.watermarks().journal_durable_lsn < capture_goal,
            );
            let cdc_deadline = scheduler.next_deadline();
            let periodic_allowed = optional_allowed
                && !periodic_active
                && !(periodic_yield_to_cdc
                    && cdc_deadline.is_some_and(|due| due <= Instant::now()));
            let deadline = if (optional_allowed
                && idle_work
                    .iter()
                    .any(|id| !excluded.contains(id) && blocked.get(*id).is_none()))
                || ready_builds.keys().any(|id| {
                    !busy.contains(id) && !excluded.contains(id) && blocked.get(*id).is_none()
                })
                || ready_preparations.keys().any(|id| {
                    !busy.contains(id) && !excluded.contains(id) && blocked.get(*id).is_none()
                }) {
                Instant::now()
            } else {
                cdc_deadline
                    .into_iter()
                    .chain(
                        maintenance_due
                            .iter()
                            .filter(|(id, _)| optional_allowed || fenced_fallback.contains(*id))
                            .chain(periodic_due.iter().filter(|_| periodic_allowed))
                            .filter(|(id, _)| {
                                !busy.contains(*id)
                                    && !excluded.contains(*id)
                                    && blocked.get(**id).is_none()
                                    && !build_active.contains(*id)
                                    && !retiring_builds.contains(*id)
                            })
                            .map(|(_, due)| *due),
                    )
                    .chain(
                        blocked
                            .records()
                            .filter(|record| {
                                !busy.contains(&record.table_id)
                                    && !recovering.contains(&record.table_id)
                            })
                            .filter_map(|record| blocked.deadline_for(record.table_id)),
                    )
                    .min()
                    .unwrap_or_else(|| Instant::now() + Duration::from_secs(3600))
            };
            tokio::select! {
                signal = &mut shutdown => {
                    signal?;
                    observation.write(config, ledger, capture_goal, false)?;
                    tracing::info!("shutdown requested; remaining work is durable in the journal");
                    return Ok(());
                }
                changed = receive.changed() => {
                    let progress = receive.borrow_and_update().clone();
                    if let Some(error) = progress.failure() {
                        if error.is::<PublicationChanged>() {
                            record_publication_changed(observation, config, ledger, capture_goal, &error);
                        }
                        bail!("source capture stopped: {error}");
                    }
                    changed.context("source actor stopped")?;
                    capture_goal = capture_goal.max(progress.durable_lsn).max(ledger.watermarks().journal_durable_lsn);
                }
                _ = tokio::task::yield_now(), if ledger.watermarks().journal_durable_lsn < capture_goal => {
                    let registration_started = Instant::now();
                    let transactions = work.reader.transactions_after(ledger.watermarks().journal_durable_lsn)?
                        .take(config.limits.pending_transactions.min(ledger.batch_capacity()))
                        .collect::<flow_ingress_journal::Result<Vec<_>>>()?;
                    let registered = ledger.journaled_batch(&transactions)?;
                    ensure!(registered > 0, "capture progress has no durable journal transaction");
                    metrics::histogram!("flow_source_registration_seconds")
                        .record(registration_started.elapsed().as_secs_f64());
                    metrics::histogram!("flow_source_registration_transactions")
                        .record(registered as f64);
                    // The disk cursor may observe a just-synced terminal before
                    // its coalesced notification reaches this task.
                    capture_goal = capture_goal.max(ledger.watermarks().journal_durable_lsn);
                    ack.send(feedback(ledger))?;
                }
                _ = tokio::task::yield_now(), if settling.iter().any(|id| !busy.contains(id)) => {
                    // One bounded page per loop iteration, like other completion work.
                    let id = *settling.iter().find(|id| !busy.contains(*id)).expect("settling table");
                    if !settle_dropped_table(ledger, store, &mut blocked, id)? {
                        settling.remove(&id);
                    }
                    ledger.drain_completed_prefix()?;
                    ack.send(feedback(ledger))?;
                }
                _ = tokio::task::yield_now(), if !recovering.is_empty() || ledger.has_completed_prefix()? => {
                    if let Some(operation) = store.applied_operations(schemas.len().max(1))?
                        .into_iter().find(|operation| !busy.contains(&operation.operation.table_id)) {
                        let id = operation.operation.table_id;
                        let transactions = ledger.pending_table_transactions_after(id, PgLsn(0))
                            .take(ledger.batch_capacity()).collect::<Result<Vec<_>>>()?;
                        let ends = transactions.iter()
                            .take_while(|transaction| transaction.end_lsn <= operation.operation.last_lsn)
                            .map(|transaction| transaction.end_lsn).collect::<Vec<_>>();
                        if ends.is_empty() {
                            store.forget_applied(&operation.operation.id)?;
                        } else {
                            ledger.table_materialized_batch(&ends, id,
                                store.table_state(&id)?.snapshot_id.unwrap_or(0))?;
                        }
                    } else {
                        ledger.drain_completed_prefix()?;
                    }
                    ack.send(feedback(ledger))?;
                }
                Some(result) = running.next(), if !running.is_empty() => {
                    let TableCompletion { id, transactions, outcome, elapsed, reserved_build, finalization_lane, periodic_maintenance } = result?;
                    busy.remove(&id);
                    retrying.remove(&id);
                    if let Some(lane) = finalization_lane {
                        lane.record_release(id, Instant::now());
                    }
                    scheduler.stall(id, false);
                    match outcome {
                        TableOutcome::Blocked { error_code } => {
                            let indexed = store.table_state(&id)?;
                            blocked.record(id, error_code, indexed.pending_operation)?;
                            metrics::counter!("flow_table_retries_total", "table_id" => id.0.to_string()).increment(1);
                            pending.defer(id, &mut scheduler);
                            observation.table_states(store, &blocked, [id])?;
                            observation.write(config, ledger, capture_goal, true)?;
                            if reserved_build {
                                build_active.remove(&id);
                                activation_pending.remove(&id);
                                waiting_for_build.remove(&id);
                            }
                            maintenance_due.remove(&id);
                            periodic_due.remove(&id);
                            idle_work.retain(|queued| *queued != id);
                            if periodic_maintenance {
                                periodic_active = false;
                            }
                        }
                        TableOutcome::Recovered => {
                            if reserved_build {
                                build_active.remove(&id);
                                activation_pending.remove(&id);
                                waiting_for_build.remove(&id);
                            }
                            pending.defer(id, &mut scheduler);
                            blocked.clear(id)?;
                        }
                        TableOutcome::Build(build) => {
                            ensure!(reserved_build && transactions.is_empty(), "invalid background build handoff");
                            builds.push(Box::pin(async move {
                                let StartedBuild { running, path, started } = *build;
                                BuildCompletion { id, path, started, result: running.wait().await }
                            }));
                            maintenance_due.remove(&id);
                        }
                        TableOutcome::PeriodicComplete { next_due } => {
                            periodic_active = false;
                            ensure!(!reserved_build && transactions.is_empty(), "invalid periodic maintenance completion");
                            if let Some(due) = next_due {
                                periodic_due.insert(id, due);
                            } else {
                                periodic_due.remove(&id);
                            }
                        }
                        TableOutcome::ProbeComplete => {
                            ensure!(reserved_build && transactions.is_empty(), "invalid background build probe");
                            build_active.remove(&id);
                            maintenance_due.insert(id, Instant::now() + OPTIONAL_MAINTENANCE_DELAY);
                        }
                        TableOutcome::Preparation(preparation) => {
                            ensure!(reserved_build && transactions.is_empty(), "invalid background preparation handoff");
                            ensure!(
                                activation_pending.insert(id),
                                "duplicate compaction preparation handoff"
                            );
                            scheduler.stall(id, true);
                            metrics::counter!(
                                "flow_compaction_preparations_total",
                                "table_id" => id.0.to_string(),
                                "result" => "started"
                            ).increment(1);
                            let hard_pressure = waiting_for_build.contains(&id);
                            preparations.push(Box::pin(async move {
                                let StartedPreparation {
                                    running,
                                    path,
                                    started,
                                    activation_stall_started,
                                } = *preparation;
                                // Hard debt already prevents CDC publication. Finish the
                                // existing candidate instead of discarding it and rebuilding
                                // the same inputs synchronously, within its total age bound.
                                let remaining = if hard_pressure {
                                    BUILD_MAX_AGE.saturating_sub(started.elapsed())
                                } else {
                                    PREPARATION_STALL_LIMIT
                                        .saturating_sub(activation_stall_started.elapsed())
                                };
                                PreparationCompletion {
                                    id,
                                    path,
                                    started,
                                    activation_stall_started,
                                    publication_resumed: false,
                                    result: running.wait_for(remaining).await,
                                }
                            }));
                            maintenance_due.remove(&id);
                        }
                        TableOutcome::Deferred => {
                            blocked.clear(id)?;
                            ensure!(!reserved_build, "build reservation deferred behind itself");
                            pending.restore(id, transactions, &mut scheduler, profiles[&id]);
                            if build_active.contains(&id) {
                                waiting_for_build.insert(id);
                                scheduler.stall(id, true);
                                tracing::info!(event = "compaction_build_pressure", table = ?id,
                                    "hard pressure paused publication until the local build completes");
                            }
                        }
                        TableOutcome::CandidateInvalidated => {
                            ensure!(reserved_build && transactions.is_empty(), "invalid candidate retirement");
                            build_active.remove(&id);
                            activation_pending.remove(&id);
                            let hard_pressure = waiting_for_build.remove(&id);
                            scheduler.stall(id, false);
                            if hard_pressure {
                                fenced_fallback.insert(id);
                            }
                            maintenance_due.insert(id, Instant::now());
                        }
                        TableOutcome::Complete { snapshot, operation, maintenance_pending } => {
                            let completion_started = Instant::now();
                            if reserved_build {
                                build_active.remove(&id);
                                activation_pending.remove(&id);
                                waiting_for_build.remove(&id);
                            }
                            scheduler.record_completion(id, elapsed);
                            for batch in transactions.chunks(ledger.batch_capacity()) {
                                let ends = batch.iter().map(|transaction| transaction.end_lsn).collect::<Vec<_>>();
                                ledger.table_materialized_batch(&ends, id, snapshot.unwrap_or(0))?;
                                for end in ends { pending.complete(id, end)?; }
                            }
                            let completed_epoch = operation;
                            if let Some(operation) = &completed_epoch
                                && store.operation(operation)?.is_some() {
                                store.forget_applied(operation)?;
                            }
                            blocked.clear(id)?;
                            ack.send(feedback(ledger))?;
                            let durable_completed = Instant::now();
                            observation.table_states(store, &blocked, [id])?;
                            let metrics_exported = observation.write(config, ledger, capture_goal, true)?;
                            let completion_finished = Instant::now();
                            if let Some(epoch) = completed_epoch {
                                let seconds = completion_finished.duration_since(completion_started).as_secs_f64();
                                metrics::histogram!("flow_table_local_phase_seconds", "table_id" => id.0.to_string(), "phase" => "complete").record(seconds);
                                tracing::info!(event = "epoch_completed", operation_id = %epoch.0,
                                    table_id = id.0, transactions = transactions.len(), elapsed_ms = seconds * 1000.0,
                                    durable_completion_ms = durable_completed.duration_since(completion_started).as_secs_f64() * 1000.0,
                                    observation_ms = completion_finished.duration_since(durable_completed).as_secs_f64() * 1000.0,
                                    metrics_exported,
                                    "source ledger and acknowledgement feedback completed");
                            }
                            if maintenance_pending.periodic {
                                periodic_due.entry(id).or_insert_with(|| Instant::now() + OPTIONAL_MAINTENANCE_DELAY);
                            }
                            if maintenance_pending.data {
                                // Publish the source proof before admitting optional work.
                                maintenance_due.entry(id).or_insert_with(|| Instant::now() + OPTIONAL_MAINTENANCE_DELAY);
                                if !transactions.is_empty() && !build_active.contains(&id) && !idle_work.contains(&id) {
                                    idle_work.push_back(id);
                                }
                            } else {
                                maintenance_due.remove(&id);
                            }
                        }
                    }
                }
                Some(completion) = builds.next(), if !builds.is_empty() => {
                    let BuildCompletion { id, path, started, result } = completion;
                    match result {
                        Ok(ready) => {
                            tracing::info!(event = "compaction_build_ready", table = ?id,
                                elapsed_ms = started.elapsed().as_millis(), "local compaction worker joined");
                            ready_builds.insert(id, CompletedBuild { ready, path, started });
                        }
                        Err(error) => {
                            // wait() joins the actual worker before releasing ownership.
                            std::fs::remove_dir_all(path)?;
                            build_active.remove(&id);
                            let hard_pressure = waiting_for_build.remove(&id);
                            scheduler.stall(id, busy.contains(&id));
                            if !retry_table_work(&error) { return Err(error); }
                            tracing::warn!(event = "compaction_build_discarded", table = ?id, %error,
                                hard_pressure, "local build invalidated; scheduling maintenance");
                            if hard_pressure {
                                fenced_fallback.insert(id);
                            }
                            maintenance_due.insert(id, Instant::now());
                        }
                    }
                }
                Some(completion) = preparations.next(), if !preparations.is_empty() => {
                    let PreparationCompletion {
                        id,
                        path,
                        started,
                        mut activation_stall_started,
                        publication_resumed,
                        result,
                    } = completion;
                    match result {
                        Ok(PreparationWait::Ready(prepared)) => {
                            if publication_resumed {
                                ensure!(activation_pending.insert(id), "duplicate deferred preparation");
                                scheduler.stall(id, true);
                                activation_stall_started = Instant::now();
                            }
                            metrics::counter!(
                                "flow_compaction_preparations_total",
                                "table_id" => id.0.to_string(),
                                "result" => "ready"
                            ).increment(1);
                            ready_preparations.insert(id, CompletedPreparation {
                                prepared: *prepared,
                                path,
                                started,
                                activation_stall_started,
                            });
                        }
                        Ok(PreparationWait::Deadline(retiring))
                            if !publication_resumed && started.elapsed() < BUILD_MAX_AGE =>
                        {
                            // The immutable candidate can outlive the short CDC
                            // stall budget. Keep its worker and ownership instead
                            // of repeatedly rebuilding the same unchanged inputs.
                            let stall = activation_stall_started.elapsed();
                            metrics::counter!("flow_compaction_preparations_total",
                                "table_id" => id.0.to_string(), "result" => "deferred").increment(1);
                            metrics::histogram!("flow_compaction_publication_stall_seconds",
                                "table_id" => id.0.to_string())
                                .record(stall.as_secs_f64());
                            tracing::info!(event = "compaction_preparation_deferred",
                                operation_id = %retiring.operation_id().0, table = ?id,
                                publication_stall_ms = stall.as_secs_f64() * 1000.0,
                                "resuming publication while the same preparation completes");
                            activation_pending.remove(&id);
                            scheduler.stall(id, busy.contains(&id));
                            preparations.push(Box::pin(async move {
                                let remaining = BUILD_MAX_AGE.saturating_sub(started.elapsed());
                                PreparationCompletion {
                                    id, path, started, activation_stall_started,
                                    publication_resumed: true,
                                    result: retiring.into_running().wait_for(remaining).await,
                                }
                            }));
                        }
                        Ok(PreparationWait::Deadline(retiring)) => {
                            let operation_id = retiring.operation_id().clone();
                            let stall = if publication_resumed { Duration::ZERO }
                                else { activation_stall_started.elapsed() };
                            metrics::counter!(
                                "flow_compaction_preparations_total",
                                "table_id" => id.0.to_string(),
                                "result" => "deadline"
                            ).increment(1);
                            if !publication_resumed {
                                metrics::histogram!(
                                    "flow_compaction_publication_stall_seconds",
                                    "table_id" => id.0.to_string()
                                ).record(stall.as_secs_f64());
                            }
                            tracing::warn!(
                                event = "compaction_preparation_deadline",
                                operation_id = %operation_id.0,
                                table = ?id,
                                publication_resumed,
                                publication_stall_ms = stall.as_secs_f64() * 1000.0,
                                "joining preparation after its build-age limit"
                            );
                            activation_pending.remove(&id);
                            build_active.remove(&id);
                            retiring_builds.insert(id);
                            waiting_for_build.remove(&id);
                            scheduler.stall(id, busy.contains(&id));
                            retirements.push(Box::pin(async move {
                                RetirementCompletion {
                                    id,
                                    path,
                                    result: retiring.wait().await,
                                }
                            }));
                        }
                        Err(error) => {
                            // wait() joins the actual worker before releasing BUILD.
                            std::fs::remove_dir_all(path)?;
                            build_active.remove(&id);
                            activation_pending.remove(&id);
                            let hard_pressure = waiting_for_build.remove(&id);
                            scheduler.stall(id, busy.contains(&id));
                            if !retry_table_work(&error) { return Err(error); }
                            metrics::counter!(
                                "flow_compaction_preparations_total",
                                "table_id" => id.0.to_string(),
                                "result" => "invalidated"
                            ).increment(1);
                            if !publication_resumed {
                                metrics::histogram!(
                                    "flow_compaction_publication_stall_seconds",
                                    "table_id" => id.0.to_string()
                                ).record(activation_stall_started.elapsed().as_secs_f64());
                            }
                            tracing::warn!(
                                event = "compaction_preparation_discarded",
                                table = ?id,
                                hard_pressure,
                                %error,
                                "background compaction preparation invalidated; scheduling maintenance"
                            );
                            if hard_pressure {
                                fenced_fallback.insert(id);
                            }
                            maintenance_due.insert(id, Instant::now());
                        }
                    }
                }
                Some(completion) = retirements.next(), if !retirements.is_empty() => {
                    let RetirementCompletion { id, path, result } = completion;
                    std::fs::remove_dir_all(path)?;
                    retiring_builds.remove(&id);
                    let hard_pressure = waiting_for_build.remove(&id);
                    scheduler.stall(id, busy.contains(&id));
                    if let Err(error) = result {
                        if !retry_table_work(&error) {
                            return Err(error);
                        }
                        tracing::info!(
                            event = "compaction_preparation_retirement_error",
                            table = ?id,
                            error = ?error,
                            "retired invalidated background preparation"
                        );
                    }
                    if hard_pressure {
                        fenced_fallback.insert(id);
                    }
                    maintenance_due.insert(id, Instant::now());
                }
                Some(result) = health_checks.next(), if !health_checks.is_empty() => {
                    let result: HealthCheckResult = observe_health(result, observation, config, ledger, capture_goal)?;
                    maintenance_admission.observe(result.source_health);
                    health_client = result.connection;
                }
                Some(result) = checkpoints.next(), if !checkpoints.is_empty() => {
                    result?;
                    checkpoint_at = Instant::now();
                }
                _ = tokio::time::sleep_until(deadline.into()), if available => {
                    while running.len() + builds.len() + preparations.len() + retirements.len()
                        < config.limits.table_workers
                    {
                        let now = Instant::now();
                        let free_workers = config.limits.table_workers.saturating_sub(
                            running.len() + builds.len() + preparations.len() + retirements.len()
                        );
                        let optional_allowed = maintenance_admission.optional_allowed(
                            config.limits.table_workers,
                            free_workers,
                            pending.has_work()
                                || pending.has_runnable_unloaded(ledger, &excluded)
                                || ledger.watermarks().journal_durable_lsn < capture_goal,
                        );
                        let prepared = ready_preparations
                            .keys()
                            .find(|id| !busy.contains(*id) && !excluded.contains(*id) && blocked.get(**id).is_none())
                            .copied();
                        let built = ready_builds
                            .keys()
                            .find(|id| !busy.contains(*id) && !excluded.contains(*id) && blocked.get(**id).is_none())
                            .copied();
                        let overdue = maintenance_due.iter()
                            .filter(|(id, due)| {
                                **due <= now
                                    && (optional_allowed || fenced_fallback.contains(*id))
                                    && !busy.contains(*id) && !excluded.contains(*id) && blocked.get(**id).is_none()
                                    && !build_active.contains(*id)
                                    && !retiring_builds.contains(*id)
                            })
                            .min_by_key(|(_, due)| **due)
                            .map(|(id, _)| *id);
                        let cdc_ready = scheduler.next_deadline().is_some_and(|due| due <= now);
                        // One periodic visit at a time; ready CDC must receive a
                        // dispatch between visits even across different tables.
                        let periodic = if optional_allowed && !periodic_active && !(periodic_yield_to_cdc && cdc_ready) {
                            periodic_due.iter()
                                .filter(|(id, due)| **due <= now && !busy.contains(*id) && !excluded.contains(*id) && blocked.get(**id).is_none()
                                    && !build_active.contains(*id) && !retiring_builds.contains(*id))
                                .min_by_key(|(_, due)| **due)
                                .map(|(id, _)| *id)
                        } else {
                            None
                        };
                        // A continuous CDC backlog must not postpone every soft
                        // build until reader debt requires a synchronous rewrite.
                        // The probe replaces its slot with a build. At least
                        // one other actor slot remains reusable for CDC.
                        let probe = if optional_allowed && work.compaction && background_build_limit > 0
                            && config.compaction.data_rewrite_scope != flow_compactor::DataRewriteScope::Disabled
                            && build_active.len() < background_build_limit
                            && retiring_builds.is_empty()
                            && cdc_ready
                        {
                            maintenance_due.iter()
                                .filter(|(id, due)| **due <= now && !busy.contains(*id) && !excluded.contains(*id) && blocked.get(**id).is_none()
                                    && !build_active.contains(*id) && !retiring_builds.contains(*id)
                                    && !fenced_fallback.contains(*id))
                                .min_by_key(|(_, due)| **due)
                                .map(|(id, _)| *id)
                        } else {
                            None
                        };
                        let retry = if retry_yield_to_cdc && cdc_ready { None } else {
                            blocked.due(now).into_iter()
                                .find(|id| !busy.contains(id) && !recovering.contains(id))
                        };
                        if let Some(id) = retry
                            && blocked.get(id).is_none_or(|record| record.error_code != PUBLICATION_CHANGED)
                            && store.table_state(&id)?.pending_operation.is_none()
                            && ledger.pending_table_transactions_after(id, PgLsn(0)).next().transpose()?.is_some()
                        {
                                blocked.release_due(id);
                                retrying.insert(id);
                                excluded.remove(&id);
                                pending.refill(ledger, &mut scheduler, &profiles,
                                    config.limits.pending_transactions, &excluded)?;
                                continue;
                        }
                        let (id, idle, candidate) = if let Some(id) = retry {
                            blocked.release_due(id);
                            retrying.insert(id);
                            retry_yield_to_cdc = true;
                            (id, true, None)
                        } else if let Some(id) = prepared {
                            (
                                id,
                                true,
                                Some(CompactionCandidate::Prepared(
                                    ready_preparations.remove(&id).expect("selected preparation"),
                                )),
                            )
                        } else if let Some(id) = built {
                            (
                                id,
                                true,
                                Some(CompactionCandidate::Built(
                                    ready_builds.remove(&id).expect("selected build"),
                                )),
                            )
                        } else if let Some(id) = periodic {
                            (id, true, None)
                        } else if let Some(id) = probe {
                            (id, true, None)
                        } else if let Some(id) = scheduler.take_ready(now) {
                            idle_work.retain(|queued| *queued != id);
                            (id, false, None)
                        } else if let Some(id) = overdue {
                            (id, true, None)
                        } else if optional_allowed && let Some(id) = idle_work.pop_front() {
                            if excluded.contains(&id) || build_active.contains(&id) || retiring_builds.contains(&id) {
                                continue;
                            }
                            if ledger.watermarks().journal_durable_lsn < capture_goal
                                || pending.has_runnable_unloaded(ledger, &excluded)
                            {
                                idle_work.clear();
                                break;
                            }
                            if pending.tables.get(&id).is_some_and(|queue| !queue.is_empty()) {
                                continue;
                            }
                            (id, true, None)
                        } else {
                            break;
                        };
                        if busy.contains(&id) || (excluded.contains(&id) && retry != Some(id)) {
                            scheduler.stall(id, true);
                            continue;
                        }
                        let periodic_maintenance = periodic == Some(id) && candidate.is_none();
                        let probing = !periodic_maintenance && probe == Some(id) && candidate.is_none();
                        if periodic_maintenance {
                            periodic_active = true;
                            periodic_yield_to_cdc = true;
                            tracing::info!(event = "periodic_maintenance_admitted", table_id = id.0,
                                queued_transactions = pending.tables.get(&id).map_or(0, VecDeque::len),
                                "admitting overdue metadata and garbage maintenance");
                            periodic_due.remove(&id);
                        }
                        if probing {
                            let overdue_ms = maintenance_due.get(&id)
                                .map_or(0.0, |due| now.saturating_duration_since(*due).as_secs_f64() * 1000.0);
                            tracing::info!(event = "compaction_build_probe_admitted", table_id = id.0,
                                queued_transactions = pending.tables.get(&id).map_or(0, VecDeque::len),
                                overdue_ms, free_workers,
                                "admitting overdue background build capture beside ready CDC");
                        }
                        if idle {
                            if !periodic_maintenance {
                                maintenance_due.remove(&id);
                            }
                            idle_work.retain(|queued| *queued != id);
                        }
                        let build_admission = if retry == Some(id) {
                            BuildAdmission::Fenced
                        } else if periodic_maintenance {
                            BuildAdmission::Wait
                        } else if probing {
                            build_active.insert(id);
                            BuildAdmission::Probe
                        } else if candidate.is_some() {
                            if candidate.as_ref().is_some_and(CompactionCandidate::is_prepared) {
                                waiting_for_build.remove(&id);
                            }
                            BuildAdmission::Fenced
                        } else if background_build_limit == 0 || fenced_fallback.remove(&id) {
                            BuildAdmission::Fenced
                        } else if idle && build_active.len() < background_build_limit
                            && !build_active.contains(&id) && retiring_builds.is_empty()
                        {
                            build_active.insert(id);
                            BuildAdmission::Start
                        } else {
                            BuildAdmission::Wait
                        };
                        let lane_acquired_at = Instant::now();
                        let options = WorkOptions {
                            periodic_maintenance,
                            build_active: candidate.is_none() && build_active.contains(&id)
                                && !matches!(build_admission, BuildAdmission::Start | BuildAdmission::Probe),
                            build_admission,
                            actor_acquired_at: lane_acquired_at,
                        };
                        let transactions = if idle { Vec::new() } else { pending.take_epoch(id) };
                        if transactions.is_empty() && !idle {
                            continue;
                        }
                        if !idle {
                            periodic_yield_to_cdc = false;
                            retry_yield_to_cdc = false;
                        }
                        if !idle && let Some(queue) = pending.tables.get(&id).filter(|queue| !queue.is_empty()) {
                            schedule_transactions(id, queue, &mut scheduler, profiles[&id]);
                            scheduler.stall(id, true);
                        }
                        busy.insert(id);
                        scheduler.stall(id, true);
                        let finalization_lane = candidate.as_ref().and_then(|candidate| {
                            candidate.activation_stall_started().map(|stall_started| FinalizationLane {
                                operation_id: candidate.operation_id().clone(),
                                acquired_at: lane_acquired_at,
                                publication_stall_started: stall_started,
                            })
                        });
                        let schema = schemas[&id].clone();
                        let work = work.clone();
                        running.push(Box::pin(async move {
                            work.run(
                                schema,
                                transactions,
                                options,
                                candidate,
                                finalization_lane,
                            )
                            .await
                        }));
                    }
                }
                _ = health.tick() => {
                    for id in schemas.keys().copied() {
                        let cause = crate::schema::capture_block(store, &SourceId(config.source.id.clone()), id)?;
                        let recorded = blocked.get(id).map(|record| record.error_code.clone());
                        // A publication block supersedes an earlier schema block.
                        if let Some(cause) = cause
                            && (recorded.is_none()
                                || cause == crate::schema::CaptureBlock::PublicationChanged
                                    && recorded.as_deref() != Some(PUBLICATION_CHANGED))
                        {
                            blocked.record(id, cause.code(), None)?;
                            // Queued work stops counting against the admission budget.
                            if !busy.contains(&id) {
                                pending.defer(id, &mut scheduler);
                            }
                        }
                        if blocked.get(id).is_some_and(|record| record.error_code == PUBLICATION_CHANGED) {
                            let (build, preparation) = release_blocked_candidates(
                                id, &mut ready_builds, &mut ready_preparations, &mut build_active,
                                &mut activation_pending, &mut waiting_for_build, &mut retiring_builds);
                            if build.is_some() || preparation.is_some() {
                                scheduler.stall(id, busy.contains(&id));
                            }
                            for (path, discard) in [
                                build.map(|build| (build.path, build.ready.discard().boxed())),
                                preparation.map(|candidate| (candidate.path, candidate.prepared.discard().boxed())),
                            ].into_iter().flatten() {
                                retirements.push(Box::pin(async move {
                                    RetirementCompletion { id, path, result: discard.await }
                                }));
                            }
                            // Whenever no run owns it, its reservations (including ones a
                            // completed or deferred run left) stop counting against admission.
                            if !busy.contains(&id) && pending.holds(id) {
                                pending.defer(id, &mut scheduler);
                            }
                            settling.insert(id);
                        }
                    }
                    // Keep durability visible while a catalog publication is stalled.
                    observation.table_states(store, &blocked, schemas.keys().copied())?;
                    observation.write(config, ledger, capture_goal, true)?;
                    if checkpoints.is_empty()
                        && checkpoint_at.elapsed() >= Duration::from_secs(config.limits.checkpoint_interval_secs)
                    {
                        // Checkpoints capture pending records consistently. Recovery
                        // may use the full catalog rebuild instead of their index.
                        checkpoints.push(CheckpointTask::start(config, work.control.clone(), store.clone()));
                    }
                    if health_checks.is_empty() {
                        health_checks.push(check_health(config.clone(), health_client.take()));
                    }
                    // Queue at most one idle check per table. Admission rechecks
                    // for CDC and gives due publication work the available slots.
                    if ledger.watermarks().journal_durable_lsn >= capture_goal
                        && !pending.has_runnable_unloaded(ledger, &excluded)
                    {
                        for id in schemas.keys() {
                            if !busy.contains(id) && !excluded.contains(id) && blocked.get(*id).is_none()
                                && !build_active.contains(id)
                                && !retiring_builds.contains(id)
                                && pending.tables.get(id).is_none_or(VecDeque::is_empty)
                                && !idle_work.contains(id)
                            {
                                idle_work.push_back(*id);
                            }
                        }
                    }
                }
            }
            excluded = blocked
                .ids()
                .filter(|id| !retrying.contains(id))
                .chain(recovering.iter().copied())
                .collect();
            pending.refill(
                ledger,
                &mut scheduler,
                &profiles,
                config.limits.pending_transactions,
                &excluded,
            )?;
            for id in busy
                .iter()
                .chain(&waiting_for_build)
                .chain(&activation_pending)
            {
                scheduler.stall(*id, true);
            }
        }
    }
}

#[cfg(test)]
mod maintenance_admission_tests {
    use super::*;
    use flow_coordinator::WalPressure;

    #[test]
    fn warning_suppresses_optional_work_until_positive_recovery() {
        let mut admission = MaintenanceAdmission::default();
        let health = WalPressure {
            retained_bytes: 600 << 20,
            journal_bytes: 0,
            safe_wal_bytes: Some(1400 << 20),
            slot_lost: false,
        }
        .health(512 << 20, 2 << 30, 8 << 30);
        admission.observe(health.into());

        // A source warning protects capacity even before the server reaches its
        // hard limit, including when maintenance shares a single table actor.
        assert!(!admission.optional_allowed(4, 4, true));
        assert!(!admission.optional_allowed(1, 1, true));
        for health in [
            SourceHealthStatus::AtRisk,
            SourceHealthStatus::Unavailable,
            SourceHealthStatus::Unknown,
        ] {
            admission.observe(health);
            assert!(!admission.optional_allowed(4, 4, false));
        }
        admission.observe(SourceHealthStatus::Healthy);
        assert!(admission.optional_allowed(4, 4, false));
        assert!(admission.optional_allowed(1, 1, true));
    }

    #[test]
    fn maintenance_cannot_take_the_last_lane_during_a_cdc_permit_gap() {
        let admission = MaintenanceAdmission::default();
        let now = Instant::now();
        let mut scheduler = Scheduler::new(10_000, 32 << 20, 1, now).unwrap();
        scheduler.push(
            TableId(1),
            Some(1),
            128,
            Priority::Realtime,
            Duration::from_secs(10),
            now,
        );
        assert_eq!(scheduler.take_ready(now), Some(TableId(1)));
        scheduler.push(
            TableId(2),
            Some(1),
            128,
            Priority::Realtime,
            Duration::from_secs(10),
            now,
        );
        assert!(scheduler.next_deadline().unwrap() > now);

        // One CDC actor is running. A periodic visit and a build may use two
        // more lanes, but the next probe must leave room for the queued epoch.
        let mut free_workers = 3;
        while admission.optional_allowed(4, free_workers, true) {
            free_workers -= 1;
        }
        assert_eq!(free_workers, 1);
        assert_eq!(
            scheduler.take_ready(now + Duration::from_secs(1)),
            Some(TableId(2))
        );
        assert!(admission.optional_allowed(4, 1, false));
    }
}

struct CheckpointTask {
    task: tokio::task::JoinHandle<Result<()>>,
    canceled: Arc<AtomicBool>,
}

impl CheckpointTask {
    fn start(config: &Config, control: ControlStore, store: StateStore) -> Self {
        let directory = config.state_dir.join("checkpoints");
        let retain = config.limits.retained_checkpoints;
        let canceled = Arc::new(AtomicBool::new(false));
        let cancellation = canceled.clone();
        let task = tokio::task::spawn_blocking(move || {
            if cancellation.load(Ordering::Acquire) {
                return Ok(());
            }
            std::fs::create_dir_all(&directory)?;
            let checkpoint =
                control.checkpoint(&store, directory.join(uuid::Uuid::new_v4().to_string()))?;
            tracing::info!(event = "index_checkpoint_completed", revision = checkpoint.revision,
                path = %checkpoint.path.display(), "row index checkpoint durable");
            let mut checkpoints = control.checkpoints()?;
            checkpoints.sort_by_key(|checkpoint| std::cmp::Reverse(checkpoint.revision));
            for obsolete in checkpoints.into_iter().skip(retain) {
                if cancellation.load(Ordering::Acquire) {
                    break;
                }
                control.forget_checkpoint(&obsolete)?;
                std::fs::remove_dir_all(obsolete.path)?;
            }
            remove_unregistered_checkpoints(&control, &directory, &cancellation)?;
            Ok(())
        });
        Self { task, canceled }
    }
}

// A crash can leave files before checkpoint registration or after forgetting it.
// RocksDB stages checkpoints at <UUID>.tmp before renaming them into place.
// Only the serialized checkpoint task sweeps its own UUID-named directories;
// registered checkpoints remain authoritative and are never removed here.
fn remove_unregistered_checkpoints(
    control: &ControlStore,
    directory: &std::path::Path,
    canceled: &AtomicBool,
) -> Result<()> {
    let registered: std::collections::HashSet<_> = control
        .checkpoints()?
        .into_iter()
        .map(|checkpoint| checkpoint.path)
        .collect();
    let directory = std::fs::canonicalize(directory)?;
    for entry in std::fs::read_dir(directory)? {
        if canceled.load(Ordering::Acquire) {
            break;
        }
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let checkpoint_name = name.strip_suffix(".tmp").unwrap_or(&name);
        if entry.file_type()?.is_dir()
            && uuid::Uuid::parse_str(checkpoint_name).is_ok()
            && !registered.contains(&entry.path())
        {
            std::fs::remove_dir_all(entry.path())?;
        }
    }
    Ok(())
}

impl Future for CheckpointTask {
    type Output = Result<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.get_mut().task).poll(cx).map(|result| {
            result
                .context("checkpoint task failed")
                .and_then(|result| result)
        })
    }
}

impl Drop for CheckpointTask {
    fn drop(&mut self) {
        // Abort work still queued in the blocking pool. A checkpoint already
        // writing its durable boundary must finish; skip subsequent cleanup.
        self.canceled.store(true, Ordering::Release);
        self.task.abort();
    }
}

// Once due, build capture or one periodic maintenance visit may precede another
// CDC epoch. Running table actors are never preempted.
const OPTIONAL_MAINTENANCE_DELAY: Duration = Duration::from_secs(1);
const BUILD_MAX_AGE: Duration = Duration::from_secs(30);
// Leave enough of the realtime budget for atomic activation and ledger ACK.
const PREPARATION_STALL_LIMIT: Duration = Duration::from_millis(750);

struct BuildCompletion {
    id: TableId,
    path: PathBuf,
    started: Instant,
    result: Result<ReadyCompaction>,
}

struct PreparationCompletion {
    id: TableId,
    path: PathBuf,
    started: Instant,
    activation_stall_started: Instant,
    publication_resumed: bool,
    result: Result<PreparationWait>,
}

struct RetirementCompletion {
    id: TableId,
    path: PathBuf,
    result: Result<()>,
}

#[cfg(test)]
mod startup_recovery_tests {
    use super::*;
    use flow_coordinator::{AckMode, JournalDurability};
    use flow_model::{
        FileId, JournalChunks, OperationId, PgLsn, PrimaryKey, RowLocation, SourceTransaction,
        TableMutationCount,
    };
    use flow_state_store::{IndexDelta, OperationPhase, PreparedOperation, StateStoreOptions};

    #[test]
    fn checkpoint_rotation_reclaims_crash_orphans_and_preserves_registered_state() {
        let root = tempfile::tempdir().unwrap();
        let directory = root.path().join("checkpoints");
        std::fs::create_dir(&directory).unwrap();
        let control = ControlStore::open(root.path().join("control")).unwrap();
        let store = control
            .initialize_index(root.path().join("index"), StateStoreOptions::default())
            .unwrap();
        let retained = control
            .checkpoint(&store, directory.join(uuid::Uuid::new_v4().to_string()))
            .unwrap();
        let forgotten = control
            .checkpoint(&store, directory.join(uuid::Uuid::new_v4().to_string()))
            .unwrap();
        control.forget_checkpoint(&forgotten).unwrap();
        let unfinished = directory.join(uuid::Uuid::new_v4().to_string());
        std::fs::create_dir(&unfinished).unwrap();
        std::fs::write(unfinished.join("CURRENT"), "unfinished").unwrap();
        let staging = directory.join(format!("{}.tmp", uuid::Uuid::new_v4()));
        std::fs::create_dir(&staging).unwrap();
        std::fs::write(staging.join("CURRENT"), "unfinished RocksDB checkpoint").unwrap();
        let operator_files = directory.join("operator-notes.tmp");
        std::fs::create_dir(&operator_files).unwrap();
        remove_unregistered_checkpoints(&control, &directory, &AtomicBool::new(true)).unwrap();
        assert!(forgotten.path.exists() && unfinished.exists() && staging.exists());
        remove_unregistered_checkpoints(&control, &directory, &AtomicBool::new(false)).unwrap();
        assert!(!forgotten.path.exists() && !unfinished.exists() && !staging.exists());
        assert!(retained.path.exists() && operator_files.exists());
        assert_eq!(control.checkpoints().unwrap().len(), 1);
        control
            .restore_checkpoint(
                &retained,
                root.path().join("restored"),
                StateStoreOptions::default(),
            )
            .unwrap();
    }

    #[test]
    fn startup_reconciles_source_progress_before_retiring_applied_state() {
        let directory = tempfile::tempdir().unwrap();
        let control_path = directory.path().join("control");
        let index_path = directory.path().join("index");
        let options = StateStoreOptions {
            apply_batch_rows: 2,
            ..Default::default()
        };
        let source = SourceId("startup-reconciliation".into());
        let published = TableId(7);
        let blocked = TableId(8);
        let end = PgLsn(11);
        let id = OperationId("durable-applied-epoch".into());

        let control = ControlStore::open(&control_path).unwrap();
        let store = control
            .initialize_index(&index_path, options.clone())
            .unwrap();
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
                begin_lsn: PgLsn(9),
                commit_lsn: PgLsn(10),
                end_lsn: end,
                commit_timestamp_micros: 0,
                schema_versions: vec![],
                affected_tables: vec![published, blocked],
                mutation_chunks: JournalChunks::default(),
                table_mutation_counts: Some(vec![
                    TableMutationCount {
                        table_id: published,
                        mutations: 1,
                    },
                    TableMutationCount {
                        table_id: blocked,
                        mutations: 0,
                    },
                ]),
            })
            .unwrap();
        store
            .prepare(
                PreparedOperation {
                    id: id.clone(),
                    table_id: published,
                    kind: OperationKind::Ingest,
                    base_snapshot_id: None,
                    last_lsn: end,
                    schema_version: 1,
                    artifacts: vec!["data.parquet".into()],
                    payload: vec![1],
                },
                [IndexDelta {
                    key: PrimaryKey(vec![1]),
                    expected: None,
                    replacement: Some(RowLocation {
                        data_file_id: FileId("data.parquet".into()),
                        row_position: 0,
                        data_sequence_number: -1,
                        spec_id: 0,
                        partition: vec![],
                        source_commit_lsn: end,
                        row_version: 1,
                        row_fingerprint: [1; 16],
                    }),
                }],
            )
            .unwrap();
        store.mark_committed(&id, 100, 1).unwrap();
        assert!(store.apply_committed(&id).unwrap().complete);
        assert_eq!(store.applied_operations(2).unwrap().len(), 1);
        assert_eq!(
            control.operation(&id).unwrap().unwrap().phase,
            OperationPhase::Applied
        );
        drop(ledger);
        drop(store);
        drop(control);

        let control = ControlStore::open(&control_path).unwrap();
        let active = control.active_generation().unwrap().unwrap();
        let store =
            StateStore::open_with_control(&active.path, options.clone(), control.clone()).unwrap();
        let mut ledger = SourceLedger::open(
            store.clone(),
            source.clone(),
            AckMode::Materialized,
            JournalDurability::LocalDisk,
        )
        .unwrap();
        assert_eq!(ledger.acknowledgement(), PgLsn(0));
        assert_eq!(reconcile_source_progress(&store, &mut ledger).unwrap(), 1);
        assert_eq!(ledger.acknowledgement(), PgLsn(0));
        assert_eq!(ledger.pending_tables(end).unwrap(), [blocked]);
        assert!(store.operation(&id).unwrap().is_none());
        assert!(control.operation(&id).unwrap().is_none());
        assert_eq!(
            control.table_state(&published).unwrap(),
            Some(store.table_state(&published).unwrap())
        );
        drop(ledger);
        drop(store);
        drop(control);

        // A second process sees the ledger's per-table completion even though
        // its global acknowledgement was held back by another affected table.
        let control = ControlStore::open(&control_path).unwrap();
        let active = control.active_generation().unwrap().unwrap();
        let store = StateStore::open_with_control(&active.path, options, control.clone()).unwrap();
        let mut ledger = SourceLedger::open(
            store.clone(),
            source,
            AckMode::Materialized,
            JournalDurability::LocalDisk,
        )
        .unwrap();
        assert_eq!(ledger.pending_tables(end).unwrap(), [blocked]);
        assert!(store.applied_operations(2).unwrap().is_empty());
        assert_eq!(
            control.table_state(&published).unwrap(),
            Some(store.table_state(&published).unwrap())
        );

        store.complete_noop(&blocked, end, 1).unwrap();
        ledger.reconcile_table_progress().unwrap();
        assert_eq!(ledger.acknowledgement(), end);
        assert_eq!(ledger.pending_count(), 0);
    }

    fn dropped_table_fixture() -> (
        tempfile::TempDir,
        StateStore,
        SourceLedger,
        BlockedTables,
        impl Fn(u64, &[TableId]) -> SourceTransaction,
    ) {
        let root = tempfile::tempdir().unwrap();
        let control = ControlStore::open(root.path().join("control")).unwrap();
        let store = control
            .initialize_index(
                root.path().join("index"),
                StateStoreOptions {
                    apply_batch_rows: 2,
                    ..Default::default()
                },
            )
            .unwrap();
        let source = SourceId("dropped-table".into());
        let ledger = SourceLedger::open(
            store.clone(),
            source.clone(),
            AckMode::Materialized,
            JournalDurability::LocalDisk,
        )
        .unwrap();
        let mut blocked = BlockedTables::load(&store, &source).unwrap();
        blocked
            .record(TableId(8), PUBLICATION_CHANGED, None)
            .unwrap();
        let transaction = move |end: u64, tables: &[TableId]| SourceTransaction {
            source_id: source.clone(),
            xid: end as u32,
            begin_lsn: PgLsn(end - 2),
            commit_lsn: PgLsn(end - 1),
            end_lsn: PgLsn(end),
            commit_timestamp_micros: 0,
            schema_versions: vec![],
            affected_tables: tables.to_vec(),
            mutation_chunks: JournalChunks::default(),
            table_mutation_counts: Some(
                tables
                    .iter()
                    .map(|table_id| TableMutationCount {
                        table_id: *table_id,
                        mutations: 0,
                    })
                    .collect(),
            ),
        };
        (root, store, ledger, blocked, transaction)
    }

    /// A table the publication stopped covering never holds acknowledgement:
    /// its already-journaled share completes at its unchanged snapshot, one
    /// bounded page per tick.
    #[test]
    fn dropped_table_transactions_stop_holding_the_acknowledgement_frontier() {
        let (_root, store, mut ledger, mut blocked, transaction) = dropped_table_fixture();
        let (healthy, dropped) = (TableId(7), TableId(8));
        ledger
            .journaled_batch(&[
                transaction(10, &[healthy, dropped]),
                transaction(20, &[dropped]),
            ])
            .unwrap();
        ledger
            .journaled_batch(&[transaction(30, &[dropped]), transaction(40, &[healthy])])
            .unwrap();
        ledger
            .table_materialized_batch(&[PgLsn(10), PgLsn(40)], healthy, 5)
            .unwrap();
        ledger.drain_completed_prefix().unwrap();
        assert_eq!(ledger.watermarks().materialized_lsn, PgLsn(0));

        // The batch capacity is two: one tick settles one page.
        settle_dropped_table(&mut ledger, &store, &mut blocked, dropped).unwrap();
        ledger.drain_completed_prefix().unwrap();
        assert_eq!(ledger.watermarks().materialized_lsn, PgLsn(20));
        settle_dropped_table(&mut ledger, &store, &mut blocked, dropped).unwrap();
        ledger.drain_completed_prefix().unwrap();
        assert_eq!(ledger.watermarks().materialized_lsn, PgLsn(40));
        assert_eq!(ledger.pending_count(), 0);
        settle_dropped_table(&mut ledger, &store, &mut blocked, dropped).unwrap();
    }

    /// A run admitted before the latch may still commit its transactions at a
    /// new snapshot. Settling first would make that completion a replay
    /// conflict that stops the daemon, so only a table no run owns settles.
    #[test]
    fn settling_a_dropped_table_waits_for_its_in_flight_run() {
        let (_root, store, mut ledger, mut blocked, transaction) = dropped_table_fixture();
        let dropped = TableId(8);
        ledger
            .journaled_batch(&[transaction(10, &[dropped])])
            .unwrap();
        // While the run owns the table it is never settled; the run commits
        // snapshot 9 and completes normally.
        ledger
            .table_materialized_batch(&[PgLsn(10)], dropped, 9)
            .unwrap();
        // A completed publication does not clear the resync-only block.
        blocked.clear(dropped).unwrap();
        assert_eq!(
            blocked.get(dropped).unwrap().error_code,
            PUBLICATION_CHANGED
        );
        settle_dropped_table(&mut ledger, &store, &mut blocked, dropped).unwrap();
        ledger.drain_completed_prefix().unwrap();
        assert_eq!(ledger.watermarks().materialized_lsn, PgLsn(10));

        // The race this avoids: behind an incomplete earlier transaction,
        // completing at the old snapshot first makes the run's completion fail.
        let (_root, _store, mut ledger, _blocked, transaction) = dropped_table_fixture();
        ledger
            .journaled_batch(&[transaction(5, &[TableId(7)]), transaction(10, &[dropped])])
            .unwrap();
        ledger
            .table_materialized_batch(&[PgLsn(10)], dropped, 0)
            .unwrap();
        let error = ledger
            .table_materialized_batch(&[PgLsn(10)], dropped, 9)
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("table publication changed on replay")
        );
    }

    /// An unfinished operation is settled by a recovery-only attempt; its
    /// transactions do not complete at the old snapshot meanwhile.
    #[test]
    fn a_prepared_operation_on_a_dropped_table_schedules_recovery() {
        let (_root, store, mut ledger, mut blocked, transaction) = dropped_table_fixture();
        let dropped = TableId(8);
        ledger
            .journaled_batch(&[transaction(10, &[dropped])])
            .unwrap();
        let id = OperationId("prepared-before-latch".into());
        store
            .begin_prepare(PreparedOperation {
                id: id.clone(),
                table_id: dropped,
                kind: OperationKind::Ingest,
                base_snapshot_id: None,
                last_lsn: PgLsn(10),
                schema_version: 1,
                artifacts: vec![],
                payload: vec![],
            })
            .unwrap();
        assert_eq!(blocked.deadline_for(dropped), None);
        settle_dropped_table(&mut ledger, &store, &mut blocked, dropped).unwrap();
        assert!(
            blocked.deadline_for(dropped).is_some(),
            "recovery-only attempt admitted"
        );
        assert_eq!(ledger.pending_count(), 1);
        // A transient recovery failure keeps the code and retries later.
        blocked
            .record(dropped, "catalog_unavailable", Some(id.clone()))
            .unwrap();
        assert_eq!(
            blocked.get(dropped).unwrap().error_code,
            PUBLICATION_CHANGED
        );
        assert!(blocked.deadline_for(dropped).is_some());
        // Once the attempt discards the building operation, the rest settles.
        store.discard_uncommitted(&id).unwrap();
        blocked.record(dropped, PUBLICATION_CHANGED, None).unwrap();
        assert_eq!(blocked.deadline_for(dropped), None);
        settle_dropped_table(&mut ledger, &store, &mut blocked, dropped).unwrap();
        ledger.drain_completed_prefix().unwrap();
        assert_eq!(ledger.watermarks().materialized_lsn, PgLsn(10));
    }

    /// Finished compaction candidates for a table that will never publish
    /// again are retired; a still-running build is left to complete first.
    /// Neither holds the acknowledgement frontier.
    #[test]
    fn a_ready_build_on_a_dropped_table_is_retired_and_the_watermark_advances() {
        let (_root, store, mut ledger, mut blocked, transaction) = dropped_table_fixture();
        let (healthy, dropped, running) = (TableId(7), TableId(8), TableId(9));
        ledger
            .journaled_batch(&[transaction(10, &[dropped]), transaction(20, &[healthy])])
            .unwrap();
        ledger
            .table_materialized_batch(&[PgLsn(20)], healthy, 5)
            .unwrap();
        let mut ready_builds = BTreeMap::from([(dropped, "built")]);
        let mut ready_preparations = BTreeMap::<TableId, &str>::new();
        let mut build_active = HashSet::from([dropped, running]);
        let mut activation_pending = HashSet::from([running]);
        let mut waiting_for_build = HashSet::from([dropped]);
        let mut retiring_builds = HashSet::new();
        let mut release = |table| {
            release_blocked_candidates(
                table,
                &mut ready_builds,
                &mut ready_preparations,
                &mut build_active,
                &mut activation_pending,
                &mut waiting_for_build,
                &mut retiring_builds,
            )
        };
        assert_eq!(release(dropped), (Some("built"), None));
        // Still running: nothing to retire yet, and its state stays owned.
        assert_eq!(release(running), (None, None));
        assert!(build_active.contains(&running) && activation_pending.contains(&running));
        assert!(!build_active.contains(&dropped) && !waiting_for_build.contains(&dropped));
        assert!(retiring_builds.contains(&dropped) && ready_builds.is_empty());

        // Only a run owns the table's ledger share, so settling proceeds.
        while settle_dropped_table(&mut ledger, &store, &mut blocked, dropped).unwrap() {}
        ledger.drain_completed_prefix().unwrap();
        assert_eq!(ledger.watermarks().materialized_lsn, PgLsn(20));
    }
}

#[cfg(test)]
#[path = "publication_live_tests.rs"]
mod publication_live_tests;
