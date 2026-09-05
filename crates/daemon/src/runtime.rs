use crate::{
    bootstrap::{bootstrap, persist_bootstrap, tables},
    config::Config,
    lifecycle::SourceHealthStatus,
    services::{catalog, journal_config, ledger},
    source::{
        CaptureProgress, capture_loop, connect, connect_owned, retryable_connection,
        validate_publication,
    },
};
use anyhow::{Context, Result, bail, ensure};
use flow_coordinator::{
    CollapseLimits, DeleteRepairCursor, Epoch, PreparationWait, PreparedCompaction, Priority,
    ReadyCompaction, ReplanRequired, RunningCompaction, RunningCompactionPreparation, Scheduler,
    SourceHealth, SourceLedger, TableMaintenance, TablePublisher, WalPressure, collapse_epoch,
};
use flow_ingress_journal::{Journal, JournalReader};
use flow_model::{OperationId, PgLsn, SourceId, SourceTransaction, TableId, TableSchema};
use flow_pg_source::{Acknowledgement, tokio_postgres::Client};
use flow_state_store::{
    ControlStore, OperationKind, OperationPhase, StateStore, StateStoreOptions,
};
use futures::{StreamExt, future::BoxFuture, stream::FuturesUnordered};
use iceberg::{Catalog, table::Table};
use std::{
    collections::{BTreeMap, HashSet, VecDeque},
    future::Future,
    path::PathBuf,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context as TaskContext, Poll},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
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
    let mut boot = bootstrap(&control)?;
    ensure!(
        boot.source_id == config.source.id
            && boot.slot == config.source.slot
            && boot.publication == config.source.publication,
        "source identity changed; use a new state directory and slot incarnation"
    );
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
    let mut tables = tables(catalog.as_ref(), &boot).await?;
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
            tables[&schema.table_id].metadata().uuid() == *uuid,
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
    if !boot.copied || boot.layout == 0 {
        crate::bootstrap::resume(&config, store.clone(), catalog.clone(), &mut boot).await?;
        tables = self::tables(catalog.as_ref(), &boot).await?;
    }
    let (journal, recovery) =
        Journal::open(config.state_dir.join("journal"), journal_config(&config))?;
    let mut ledger = ledger(&store, &config)?;
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
    let sql = connect(&config, false).await?;
    crate::schema::SchemaRegistry::new(
        store.clone(),
        SourceId(config.source.id.clone()),
        &boot.schemas,
    )?
    .initialize(&sql, &config.tables)
    .await?;
    validate_publication(&sql, &config, &boot.schemas, false).await?;
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
        let table = tables
            .get(&operation.operation.table_id)
            .context("pending operation has no configured table")?;
        match operation.phase {
            OperationPhase::Building => {
                store.discard_uncommitted(&operation.operation.id)?;
            }
            _ if operation.operation.kind == OperationKind::Ingest => {
                if let Err(error) = publisher.recover(table, &operation.operation.id).await {
                    if error.downcast_ref::<ReplanRequired>().is_none() {
                        return Err(error);
                    }
                    tracing::info!(operation = %operation.operation.id.0, "replaying invalidated publication from the journal");
                }
            }
            _ if matches!(
                operation.operation.kind,
                OperationKind::Rewrite | OperationKind::Reconcile | OperationKind::ManifestRewrite
            ) =>
            {
                if let Err(error) = maintenance.recover(table, &operation.operation.id).await {
                    if error.downcast_ref::<ReplanRequired>().is_none() {
                        return Err(error);
                    }
                    tracing::info!(operation = %operation.operation.id.0, "replanning invalidated maintenance");
                } else {
                    let store = store.clone();
                    tokio::task::spawn_blocking(move || {
                        store.forget_applied(&operation.operation.id)
                    })
                    .await??;
                }
            }
            _ => bail!("index reconciliation must complete before source capture"),
        }
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
    tables = self::tables(catalog.as_ref(), &boot).await?;
    let reader = journal.reader();
    let (events, mut receive) = watch::channel(CaptureProgress {
        durable_lsn: journal.durable_lsn(),
        error: None,
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
    let publishing = PublishRuntime {
        work: TableWork {
            config: config.clone(),
            store: store.clone(),
            control,
            reader,
            catalog,
            publisher,
            maintenance,
            garbage_checked: Arc::default(),
            compaction,
        },
        tables,
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
    tables: BTreeMap<TableId, Table>,
    schemas: BTreeMap<TableId, TableSchema>,
    profiles: BTreeMap<TableId, Priority>,
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
            tables,
            schemas,
            profiles,
        } = self;
        let config = &work.config;
        let store = &work.store;
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
        )?;
        let mut capture_goal = receive.borrow_and_update().durable_lsn;
        let mut busy = HashSet::new();
        let mut running = FuturesUnordered::<BoxFuture<'static, Result<TableCompletion>>>::new();
        // One speculative build shares the configured worker budget with CDC.
        // Reservations include capture and completed candidates, so another
        // table cannot start a build before the first one is finalized.
        let background_builds = config.limits.table_workers > 1;
        let mut build_active = HashSet::new();
        let mut builds = FuturesUnordered::<BoxFuture<'static, BuildCompletion>>::new();
        let mut ready_builds = BTreeMap::<TableId, CompletedBuild>::new();
        let mut preparations = FuturesUnordered::<BoxFuture<'static, PreparationCompletion>>::new();
        let mut retirements = FuturesUnordered::<BoxFuture<'static, RetirementCompletion>>::new();
        let mut ready_preparations = BTreeMap::<TableId, CompletedPreparation>::new();
        let mut waiting_for_build = HashSet::new();
        // Timed-out workers retain durable ownership and resource accounting,
        // but are no longer candidates that may pause foreground publication.
        let mut retiring_builds = HashSet::new();
        // Preparation reads a frozen index snapshot. Hold only this table's
        // next CDC epoch until that candidate activates or is retired.
        let mut activation_pending = HashSet::new();
        let mut fenced_fallback = HashSet::new();
        let mut health = tokio::time::interval(Duration::from_secs(5));
        health.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut health_client = None;
        let mut health_checks = FuturesUnordered::new();
        let mut checkpoints = FuturesUnordered::new();
        let mut idle_work = VecDeque::new();
        let mut maintenance_due = BTreeMap::<TableId, Instant>::new();
        let mut periodic_due = BTreeMap::<TableId, Instant>::new();
        let mut periodic_active = false;
        let mut periodic_yield_to_cdc = false;
        let mut checkpoint_at = Instant::now();
        let shutdown = crate::lifecycle::shutdown_signal();
        tokio::pin!(shutdown);
        observation.write(config, ledger, capture_goal, true)?;
        loop {
            let available = running.len() + builds.len() + preparations.len() + retirements.len()
                < config.limits.table_workers;
            let cdc_deadline = scheduler.next_deadline();
            let periodic_allowed = !periodic_active
                && !(periodic_yield_to_cdc
                    && cdc_deadline.is_some_and(|due| due <= Instant::now()));
            let deadline = if !idle_work.is_empty()
                || ready_builds.keys().any(|id| !busy.contains(id))
                || ready_preparations.keys().any(|id| !busy.contains(id))
            {
                Instant::now()
            } else {
                cdc_deadline
                    .into_iter()
                    .chain(
                        maintenance_due
                            .iter()
                            .chain(periodic_due.iter().filter(|_| periodic_allowed))
                            .filter(|(id, _)| {
                                !busy.contains(*id)
                                    && !build_active.contains(*id)
                                    && !retiring_builds.contains(*id)
                            })
                            .map(|(_, due)| *due),
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
                    if let Some(error) = progress.error { bail!("source capture stopped: {error}"); }
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
                Some(result) = running.next(), if !running.is_empty() => {
                    let TableCompletion { id, transactions, outcome, elapsed, reserved_build, finalization_lane } = result?;
                    busy.remove(&id);
                    if let Some(lane) = finalization_lane {
                        lane.record_release(id, Instant::now());
                    }
                    scheduler.stall(id, false);
                    match outcome {
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
                            preparations.push(Box::pin(async move {
                                let StartedPreparation {
                                    running,
                                    path,
                                    started,
                                    activation_stall_started,
                                } = *preparation;
                                let remaining = PREPARATION_STALL_LIMIT
                                    .saturating_sub(activation_stall_started.elapsed());
                                PreparationCompletion {
                                    id,
                                    path,
                                    started,
                                    activation_stall_started,
                                    result: running.wait_for(remaining).await,
                                }
                            }));
                            maintenance_due.remove(&id);
                        }
                        TableOutcome::Deferred => {
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
                        TableOutcome::Complete { snapshot, maintenance_pending } => {
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
                                for end in ends { pending.complete(end)?; }
                            }
                            let completed_epoch = if !transactions.is_empty() {
                                let epoch = Epoch::new(SourceId(config.source.id.clone()), id, &transactions)?;
                                if store.operation(&epoch.id)?.is_some() {
                                    store.forget_applied(&epoch.id)?;
                                }
                                Some(epoch.id)
                            } else { None };
                            ack.send(feedback(ledger))?;
                            let durable_completed = Instant::now();
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
                        activation_stall_started,
                        result,
                    } = completion;
                    match result {
                        Ok(PreparationWait::Ready(prepared)) => {
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
                        Ok(PreparationWait::Deadline(retiring)) => {
                            let operation_id = retiring.operation_id().clone();
                            let stall = activation_stall_started.elapsed();
                            metrics::counter!(
                                "flow_compaction_preparations_total",
                                "table_id" => id.0.to_string(),
                                "result" => "deadline"
                            ).increment(1);
                            metrics::histogram!(
                                "flow_compaction_publication_stall_seconds",
                                "table_id" => id.0.to_string()
                            ).record(stall.as_secs_f64());
                            tracing::warn!(
                                event = "compaction_preparation_deadline",
                                operation_id = %operation_id.0,
                                table = ?id,
                                publication_stall_ms = stall.as_secs_f64() * 1000.0,
                                "resuming table publication while the preparation worker retires"
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
                            metrics::histogram!(
                                "flow_compaction_publication_stall_seconds",
                                "table_id" => id.0.to_string()
                            ).record(activation_stall_started.elapsed().as_secs_f64());
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
                    let result: HealthCheckResult = result?;
                    health_client = result.connection;
                    observation.record_source_health(result.source_health);
                    observation.write(config, ledger, capture_goal, true)?;
                    if result.source_health == SourceHealthStatus::SlotLost {
                        bail!("replication slot lost WAL; resynchronization required");
                    }
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
                        let prepared = ready_preparations
                            .keys()
                            .find(|id| !busy.contains(*id))
                            .copied();
                        let built = ready_builds
                            .keys()
                            .find(|id| !busy.contains(*id))
                            .copied();
                        let overdue = maintenance_due.iter()
                            .filter(|(id, due)| {
                                **due <= now
                                    && !busy.contains(*id)
                                    && !build_active.contains(*id)
                                    && !retiring_builds.contains(*id)
                            })
                            .min_by_key(|(_, due)| **due)
                            .map(|(id, _)| *id);
                        let cdc_ready = scheduler.next_deadline().is_some_and(|due| due <= now);
                        // One periodic visit at a time; ready CDC must receive a
                        // dispatch between visits even across different tables.
                        let periodic = if !periodic_active && !(periodic_yield_to_cdc && cdc_ready) {
                            periodic_due.iter()
                                .filter(|(id, due)| **due <= now && !busy.contains(*id)
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
                        let free_workers = config.limits.table_workers.saturating_sub(
                            running.len() + builds.len() + preparations.len() + retirements.len()
                        );
                        let probe = if work.compaction && background_builds
                            && config.compaction.data_rewrite_scope != flow_compactor::DataRewriteScope::Disabled
                            && free_workers >= 1
                            && build_active.is_empty()
                            && retiring_builds.is_empty()
                            && scheduler.next_deadline().is_some_and(|due| due <= now)
                        {
                            maintenance_due.iter()
                                .filter(|(id, due)| **due <= now && !busy.contains(*id) && !fenced_fallback.contains(*id))
                                .min_by_key(|(_, due)| **due)
                                .map(|(id, _)| *id)
                        } else {
                            None
                        };
                        let (id, idle, candidate) = if let Some(id) = prepared {
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
                        } else if let Some(id) = idle_work.pop_front() {
                            if build_active.contains(&id) || retiring_builds.contains(&id) {
                                continue;
                            }
                            if ledger.watermarks().journal_durable_lsn < capture_goal
                                || ledger.pending_count() != pending.loaded.len()
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
                        if busy.contains(&id) {
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
                        let build_admission = if periodic_maintenance {
                            BuildAdmission::Wait
                        } else if probing {
                            build_active.insert(id);
                            BuildAdmission::Probe
                        } else if candidate.is_some() {
                            if candidate.as_ref().is_some_and(CompactionCandidate::is_prepared) {
                                waiting_for_build.remove(&id);
                            }
                            BuildAdmission::Fenced
                        } else if !background_builds || fenced_fallback.remove(&id) {
                            BuildAdmission::Fenced
                        } else if idle && build_active.is_empty() && retiring_builds.is_empty() {
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
                        let table = tables[&id].clone();
                        let work = work.clone();
                        running.push(Box::pin(async move {
                            work.run(
                                table,
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
                    // Keep durability visible while a catalog publication is stalled.
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
                        && ledger.pending_count() == pending.loaded.len()
                    {
                        for id in tables.keys() {
                            if !busy.contains(id)
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
            pending.refill(
                ledger,
                &mut scheduler,
                &profiles,
                config.limits.pending_transactions,
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

const HEALTH_TIMEOUT: Duration = Duration::from_secs(5);

struct HealthConnection {
    client: Client,
    _connection: crate::source::ConnectionTask,
}

struct HealthCheckResult {
    connection: Option<HealthConnection>,
    source_health: SourceHealthStatus,
}

async fn check_health(
    config: Config,
    client: Option<HealthConnection>,
) -> Result<HealthCheckResult> {
    let result: Result<(HealthConnection, SourceHealth)> = async {
        let client = match client {
            Some(client) => client,
            None => {
                let (client, connection) =
                    tokio::time::timeout(HEALTH_TIMEOUT, connect_owned(&config, false))
                        .await
                        .context("WAL health connection timed out")??;
                HealthConnection {
                    client,
                    _connection: connection,
                }
            }
        };
        let source_health = check_wal(&client.client, &config).await?;
        Ok((client, source_health))
    }
    .await;
    match result {
        Ok((client, source_health)) => Ok(HealthCheckResult {
            connection: Some(client),
            source_health: source_health.into(),
        }),
        Err(error) if retryable_connection(&error) || error.is::<tokio::time::error::Elapsed>() => {
            // Dropping the owned driver closes even an unanswered query.
            tracing::warn!(%error, "WAL health check interrupted; reconnecting on next check");
            Ok(HealthCheckResult {
                connection: None,
                source_health: SourceHealthStatus::Unavailable,
            })
        }
        Err(error) => Err(error),
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
            Ok(())
        });
        Self { task, canceled }
    }
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

async fn check_wal(client: &Client, config: &Config) -> Result<SourceHealth> {
    let row = tokio::time::timeout(HEALTH_TIMEOUT, client.query_opt(
        "SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)::bigint, wal_status, safe_wal_size FROM pg_catalog.pg_replication_slots WHERE slot_name=$1",
        &[&config.source.slot],
    )).await.context("WAL health query timed out")??;
    let Some(row) = row else {
        return Ok(SourceHealth::SlotLost);
    };
    let retained: Option<i64> = row.get(0);
    let status: Option<String> = row.get(1);
    let safe: Option<i64> = row.get(2);
    let directory = config.state_dir.join("journal");
    let journal_bytes = tokio::task::spawn_blocking(move || -> Result<u64> {
        let mut bytes = 0u64;
        for entry in std::fs::read_dir(directory)? {
            let entry = entry?;
            if entry.path().extension().is_some_and(|s| s == "segment") {
                match entry.metadata() {
                    Ok(metadata) => bytes = bytes.saturating_add(metadata.len()),
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {} // reclaimed concurrently
                    Err(error) => return Err(error.into()),
                }
            }
        }
        Ok(bytes)
    })
    .await??;
    let pressure = WalPressure {
        retained_bytes: retained.unwrap_or(0).max(0) as u64,
        journal_bytes,
        safe_wal_bytes: safe.map(|s| s.max(0) as u64),
        slot_lost: status.as_deref() == Some("lost"),
    };
    metrics::gauge!("flow_source_retained_wal_bytes").set(pressure.retained_bytes as f64);
    metrics::gauge!("flow_journal_bytes").set(pressure.journal_bytes as f64);
    if let Some(bytes) = pressure.safe_wal_bytes {
        metrics::gauge!("flow_source_safe_wal_bytes").set(bytes as f64);
    }
    let health = pressure.health(
        config.limits.wal_soft_bytes,
        config.limits.wal_hard_bytes,
        config.limits.journal_bytes,
    );
    match health {
        SourceHealth::Healthy | SourceHealth::SlotLost => {}
        health => tracing::error!(
            ?health,
            retained_wal_bytes = pressure.retained_bytes,
            journal_bytes,
            "source storage pressure; increase capacity or drain publication lag"
        ),
    }
    Ok(health)
}

fn source_age(transaction: &SourceTransaction) -> Duration {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros();
    let committed = transaction.commit_timestamp_micros.max(0) as u128;
    Duration::from_micros(now.saturating_sub(committed).min(u128::from(u64::MAX)) as u64)
}

const EPOCH_MUTATION_TRIGGER: u64 = 10_000;
const EPOCH_MAX_BYTES: u64 = 32 << 20;

fn schedule_transactions<'a>(
    table: TableId,
    transactions: impl IntoIterator<Item = &'a SourceTransaction>,
    scheduler: &mut Scheduler,
    priority: Priority,
) {
    let mut transactions = transactions.into_iter();
    let Some(first) = transactions.next() else {
        return;
    };
    let (rows, bytes) = std::iter::once(first).chain(transactions).fold(
        (Some(0u64), 0u64),
        |(rows, bytes), transaction| {
            (
                rows.zip(transaction.mutation_count(table))
                    .map(|(sum, count)| sum.saturating_add(count)),
                bytes.saturating_add(transaction.mutation_chunks.payload_bytes()),
            )
        },
    );
    scheduler.push(
        table,
        rows,
        bytes,
        priority,
        source_age(first),
        Instant::now(),
    );
}

#[derive(Default)]
struct PendingWork {
    tables: BTreeMap<TableId, VecDeque<SourceTransaction>>,
    loaded: BTreeMap<PgLsn, usize>,
    through: PgLsn,
}
impl PendingWork {
    /// Dequeue a table's ordered prefix without splitting a source transaction.
    /// Readiness never waits to fill a batch. Already-queued ordinary work may
    /// exceed the row trigger, bounded by payload bytes and the loaded window.
    /// Unknown legacy counts and oversized transactions each publish alone.
    fn take_epoch(&mut self, table: TableId) -> Vec<SourceTransaction> {
        let queue = self.tables.entry(table).or_default();
        let mut transactions = Vec::new();
        let mut bytes = 0u64;
        while let Some(transaction) = queue.front() {
            let count = transaction.mutation_count(table);
            let size = transaction.mutation_chunks.payload_bytes();
            let standalone =
                count.is_none_or(|rows| rows > EPOCH_MUTATION_TRIGGER) || size > EPOCH_MAX_BYTES;
            if !transactions.is_empty()
                && (standalone || bytes.saturating_add(size) > EPOCH_MAX_BYTES)
            {
                break;
            }
            bytes = bytes.saturating_add(size);
            transactions.push(queue.pop_front().expect("queue front exists"));
            if standalone || bytes >= EPOCH_MAX_BYTES {
                break;
            }
        }
        transactions
    }

    fn restore(
        &mut self,
        id: TableId,
        transactions: Vec<SourceTransaction>,
        scheduler: &mut Scheduler,
        priority: Priority,
    ) {
        if !transactions.is_empty() {
            // Only restore the dequeued prefix. The scheduler already owns
            // the tail and any transactions loaded while this job was running.
            schedule_transactions(id, &transactions, scheduler, priority);
            let queue = self.tables.entry(id).or_default();
            for transaction in transactions.into_iter().rev() {
                queue.push_front(transaction);
            }
        }
    }

    fn refill(
        &mut self,
        ledger: &SourceLedger,
        scheduler: &mut Scheduler,
        profiles: &BTreeMap<TableId, Priority>,
        capacity: usize,
    ) -> Result<()> {
        for transaction in ledger
            .pending_transactions_after(self.through)
            .take(capacity.saturating_sub(self.loaded.len()))
        {
            let transaction = transaction?;
            self.through = transaction.end_lsn;
            let mut tables = 0usize;
            for table in ledger.pending_tables(transaction.end_lsn)? {
                self.tables
                    .entry(table)
                    .or_default()
                    .push_back(transaction.clone());
                scheduler.push(
                    table,
                    transaction.mutation_count(table),
                    transaction.mutation_chunks.payload_bytes(),
                    profiles[&table],
                    source_age(&transaction),
                    Instant::now(),
                );
                tables += 1;
            }
            if tables > 0 {
                self.loaded.insert(transaction.end_lsn, tables);
            }
        }
        Ok(())
    }
    fn complete(&mut self, end: PgLsn) -> Result<()> {
        let remaining = self
            .loaded
            .get_mut(&end)
            .context("completed transaction is outside the publication window")?;
        *remaining -= 1;
        if *remaining == 0 {
            self.loaded.remove(&end);
        }
        Ok(())
    }
}

// Once due, build capture or one periodic maintenance visit may precede another
// CDC epoch. Running table actors are never preempted.
const OPTIONAL_MAINTENANCE_DELAY: Duration = Duration::from_secs(1);
const BUILD_MAX_AGE: Duration = Duration::from_secs(30);
// Leave enough of the realtime budget for atomic activation and ledger ACK.
const PREPARATION_STALL_LIMIT: Duration = Duration::from_millis(750);

#[derive(Clone, Copy, PartialEq, Eq)]
enum BuildAdmission {
    Fenced,
    Wait,
    Start,
    Probe,
}

#[derive(Clone, Copy)]
struct WorkOptions {
    periodic_maintenance: bool,
    build_active: bool,
    build_admission: BuildAdmission,
    actor_acquired_at: Instant,
}

struct StartedBuild {
    running: RunningCompaction,
    path: PathBuf,
    started: Instant,
}

struct CompletedBuild {
    ready: ReadyCompaction,
    path: PathBuf,
    started: Instant,
}

struct BuildCompletion {
    id: TableId,
    path: PathBuf,
    started: Instant,
    result: Result<ReadyCompaction>,
}

struct StartedPreparation {
    running: RunningCompactionPreparation,
    path: PathBuf,
    started: Instant,
    activation_stall_started: Instant,
}

struct CompletedPreparation {
    prepared: PreparedCompaction,
    path: PathBuf,
    started: Instant,
    activation_stall_started: Instant,
}

struct PreparationCompletion {
    id: TableId,
    path: PathBuf,
    started: Instant,
    activation_stall_started: Instant,
    result: Result<PreparationWait>,
}

struct RetirementCompletion {
    id: TableId,
    path: PathBuf,
    result: Result<()>,
}

enum CompactionCandidate {
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

    fn operation_id(&self) -> &OperationId {
        match self {
            Self::Built(candidate) => candidate.ready.operation_id(),
            Self::Prepared(candidate) => candidate.prepared.operation_id(),
        }
    }

    fn is_prepared(&self) -> bool {
        matches!(self, Self::Prepared(_))
    }

    fn activation_stall_started(&self) -> Option<Instant> {
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

struct MaintenancePending {
    data: bool,
    periodic: bool,
}

enum TableOutcome {
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

struct TableCompletion {
    id: TableId,
    transactions: Vec<SourceTransaction>,
    outcome: TableOutcome,
    elapsed: Duration,
    reserved_build: bool,
    finalization_lane: Option<FinalizationLane>,
}

struct FinalizationLane {
    operation_id: OperationId,
    acquired_at: Instant,
    publication_stall_started: Instant,
}

impl FinalizationLane {
    fn record_release(self, table_id: TableId, released_at: Instant) -> Duration {
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

fn retry_table_work(error: &anyhow::Error) -> bool {
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
struct TableWork {
    config: Config,
    store: StateStore,
    control: ControlStore,
    reader: JournalReader,
    catalog: Arc<dyn Catalog>,
    publisher: Arc<TablePublisher>,
    maintenance: Arc<TableMaintenance>,
    garbage_checked: Arc<Mutex<BTreeMap<TableId, Instant>>>,
    compaction: bool,
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

    async fn run(
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
        let mut current = self.catalog.load_table(table.identifier()).await?;
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
            current = self.catalog.load_table(table.identifier()).await?;
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
        let current = self.catalog.load_table(table.identifier()).await?;
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
            || history_due(table, self.config.limits.snapshot_retention_secs)?
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
        let history_due = history_due(&current, self.config.limits.snapshot_retention_secs)?;
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

#[cfg(test)]
mod admission_tests {
    use super::*;
    use flow_coordinator::{AckMode, JournalDurability};
    use flow_model::{JournalChunks, TableMutationCount};

    #[test]
    fn restored_prefixes_keep_whole_transactions_and_all_table_ack_order() {
        let directory = tempfile::tempdir().unwrap();
        let control = ControlStore::open(directory.path().join("control")).unwrap();
        let store = control
            .initialize_index(directory.path().join("index"), StateStoreOptions::default())
            .unwrap();
        let source = SourceId("admission".into());
        let (first, second) = (TableId(1), TableId(2));
        let transaction = |xid: u32, rows: Option<u64>, bytes| SourceTransaction {
            source_id: source.clone(),
            xid,
            begin_lsn: PgLsn(u64::from(xid) * 10 - 1),
            commit_lsn: PgLsn(u64::from(xid) * 10),
            end_lsn: PgLsn(u64::from(xid) * 10 + 1),
            commit_timestamp_micros: 0,
            schema_versions: vec![],
            affected_tables: vec![first, second],
            mutation_chunks: JournalChunks {
                payload_bytes: bytes,
                ..Default::default()
            },
            table_mutation_counts: rows.map(|mutations| {
                vec![
                    TableMutationCount {
                        table_id: first,
                        mutations,
                    },
                    TableMutationCount {
                        table_id: second,
                        mutations: 0,
                    },
                ]
            }),
        };
        let mut ledger = SourceLedger::open(
            store.clone(),
            source.clone(),
            AckMode::Materialized,
            JournalDurability::LocalDisk,
        )
        .unwrap();
        ledger
            .journaled_batch(&[
                transaction(1, Some(6000), 1),
                transaction(2, Some(5000), 1),
                transaction(3, Some(15_000), 1),
                transaction(4, Some(1), EPOCH_MAX_BYTES + 1),
                transaction(5, None, 1),
                transaction(6, Some(0), 0),
            ])
            .unwrap();
        drop(ledger);
        let mut ledger = SourceLedger::open(
            store,
            source.clone(),
            AckMode::Materialized,
            JournalDurability::LocalDisk,
        )
        .unwrap();
        let mut pending = PendingWork::default();
        let mut scheduler =
            Scheduler::new(EPOCH_MUTATION_TRIGGER, EPOCH_MAX_BYTES, 100, Instant::now()).unwrap();
        let profiles = BTreeMap::from([(first, Priority::Realtime), (second, Priority::Realtime)]);
        pending
            .refill(&ledger, &mut scheduler, &profiles, 6)
            .unwrap();
        assert_eq!(scheduler.take_ready(Instant::now()), Some(first));
        let taken = pending.take_epoch(first);
        assert_eq!(
            taken.iter().map(|txn| txn.xid).collect::<Vec<_>>(),
            [1, 2],
            "the row trigger does not cap already-queued ordinary work"
        );
        schedule_transactions(
            first,
            &pending.tables[&first],
            &mut scheduler,
            Priority::Realtime,
        );
        // New work arrives while the dequeued prefix is in flight. A retry puts
        // only that prefix back ahead of the existing tail and the new record.
        ledger
            .journaled_batch(&[
                transaction(7, Some(10_000), EPOCH_MAX_BYTES - 1),
                transaction(8, Some(1), 1),
                transaction(9, Some(0), 1),
            ])
            .unwrap();
        pending
            .refill(&ledger, &mut scheduler, &profiles, 9)
            .unwrap();
        pending.restore(first, taken, &mut scheduler, Priority::Realtime);
        // The last byte fits exactly; the next transaction remains whole in the
        // following epoch. A transaction at the row trigger can still coalesce.
        for expected in [
            vec![1, 2],
            vec![3],
            vec![4],
            vec![5],
            vec![6, 7, 8],
            vec![9],
        ] {
            let epoch = pending.take_epoch(first);
            assert_eq!(
                epoch.iter().map(|txn| txn.xid).collect::<Vec<_>>(),
                expected
            );
            for txn in epoch {
                ledger.table_materialized(txn.end_lsn, first, 100).unwrap();
                pending.complete(txn.end_lsn).unwrap();
            }
        }
        assert!(pending.tables[&first].is_empty());
        assert_eq!(
            pending.loaded.len(),
            9,
            "another affected table still owns every slot"
        );
        assert_eq!(ledger.acknowledgement(), PgLsn(0));
        loop {
            let epoch = pending.take_epoch(second);
            if epoch.is_empty() {
                break;
            }
            for txn in epoch {
                ledger.table_materialized(txn.end_lsn, second, 101).unwrap();
                pending.complete(txn.end_lsn).unwrap();
            }
        }
        assert!(pending.loaded.is_empty());
        assert_eq!(ledger.pending_count(), 0);
        assert_eq!(ledger.acknowledgement(), PgLsn(91));
    }
}

#[cfg(test)]
mod startup_recovery_tests {
    use super::*;
    use flow_coordinator::{AckMode, JournalDurability};
    use flow_model::{FileId, JournalChunks, PrimaryKey, RowLocation, TableMutationCount};
    use flow_state_store::{IndexDelta, PreparedOperation};

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
}
