use crate::{
    bootstrap::{bootstrap, persist_bootstrap, tables},
    config::Config,
    services::{catalog, journal_config, ledger},
    source::{
        CaptureProgress, capture_loop, connect, connect_owned, retryable_connection,
        validate_publication,
    },
};
use anyhow::{Context, Result, bail, ensure};
use flow_coordinator::{
    CollapseLimits, DeleteRepairCursor, Epoch, Priority, ReadyCompaction, ReplanRequired,
    RunningCompaction, Scheduler, SourceHealth, SourceLedger, TableMaintenance, TablePublisher,
    WalPressure, collapse_epoch,
};
use flow_ingress_journal::{Journal, JournalReader};
use flow_model::{PgLsn, SourceId, SourceTransaction, TableId, TableSchema};
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
            if matches!(
                error.downcast_ref::<flow_state_store::Error>(),
                Some(flow_state_store::Error::RecoveryRequired(_))
            ) =>
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
    ledger.reconcile_table_progress()?;
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
        let mut waiting_for_build = HashSet::new();
        let mut fenced_fallback = HashSet::new();
        let mut health = tokio::time::interval(Duration::from_secs(5));
        health.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut health_client = None;
        let mut health_checks = FuturesUnordered::new();
        let mut checkpoints = FuturesUnordered::new();
        let mut idle_work = VecDeque::new();
        let mut maintenance_due = BTreeMap::<TableId, Instant>::new();
        let mut checkpoint_at = Instant::now();
        let shutdown = crate::lifecycle::shutdown_signal();
        tokio::pin!(shutdown);
        observation.write(config, ledger, capture_goal, true)?;
        loop {
            let available = running.len() + builds.len() < config.limits.table_workers;
            let deadline = if !idle_work.is_empty()
                || ready_builds.keys().any(|id| !busy.contains(id))
            {
                Instant::now()
            } else {
                scheduler
                    .next_deadline()
                    .into_iter()
                    .chain(
                        maintenance_due
                            .iter()
                            .filter(|(id, _)| !busy.contains(*id) && !build_active.contains(*id))
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
                    let TableCompletion { id, transactions, outcome, elapsed, reserved_build } = result?;
                    busy.remove(&id);
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
                        TableOutcome::Complete { snapshot, maintenance_pending } => {
                            let completion_started = Instant::now();
                            if reserved_build { build_active.remove(&id); }
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
                            if maintenance_pending {
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
                            waiting_for_build.remove(&id);
                            scheduler.stall(id, busy.contains(&id));
                            if !retry_table_work(&error) { return Err(error); }
                            tracing::warn!(event = "compaction_build_discarded", table = ?id, %error,
                                "local build invalidated; scheduling fenced maintenance");
                            fenced_fallback.insert(id);
                            maintenance_due.insert(id, Instant::now());
                        }
                    }
                }
                Some(result) = health_checks.next(), if !health_checks.is_empty() => {
                    health_client = result?;
                }
                Some(result) = checkpoints.next(), if !checkpoints.is_empty() => {
                    result?;
                    checkpoint_at = Instant::now();
                }
                _ = tokio::time::sleep_until(deadline.into()), if available => {
                    while running.len() + builds.len() < config.limits.table_workers {
                        let now = Instant::now();
                        let completed = ready_builds.keys().find(|id| !busy.contains(*id)).copied();
                        let overdue = maintenance_due.iter()
                            .filter(|(id, due)| **due <= now && !busy.contains(*id) && !build_active.contains(*id))
                            .min_by_key(|(_, due)| **due)
                            .map(|(id, _)| *id);
                        let (id, idle) = if let Some(id) = completed.or(overdue) {
                            (id, true)
                        } else if let Some(id) = scheduler.take_ready(now) {
                            idle_work.retain(|queued| *queued != id);
                            (id, false)
                        } else if let Some(id) = idle_work.pop_front() {
                            if build_active.contains(&id) { continue; }
                            if ledger.watermarks().journal_durable_lsn < capture_goal
                                || ledger.pending_count() != pending.loaded.len()
                            {
                                idle_work.clear();
                                break;
                            }
                            if pending.tables.get(&id).is_some_and(|queue| !queue.is_empty()) {
                                continue;
                            }
                            (id, true)
                        } else {
                            break;
                        };
                        if busy.contains(&id) {
                            scheduler.stall(id, true);
                            continue;
                        }
                        if idle {
                            maintenance_due.remove(&id);
                            idle_work.retain(|queued| *queued != id);
                        }
                        let candidate = ready_builds.remove(&id);
                        let build_admission = if candidate.is_some() {
                            waiting_for_build.remove(&id);
                            BuildAdmission::Fenced
                        } else if !background_builds || fenced_fallback.remove(&id) {
                            BuildAdmission::Fenced
                        } else if idle && build_active.is_empty() {
                            build_active.insert(id);
                            BuildAdmission::Start
                        } else {
                            BuildAdmission::Wait
                        };
                        let options = WorkOptions {
                            build_active: candidate.is_none() && build_active.contains(&id) && build_admission != BuildAdmission::Start,
                            build_admission,
                        };
                        let transactions = if idle { Vec::new() } else { pending.take_epoch(id) };
                        if transactions.is_empty() && !idle {
                            continue;
                        }
                        if !idle && let Some(queue) = pending.tables.get(&id).filter(|queue| !queue.is_empty()) {
                            schedule_transactions(id, queue, &mut scheduler, profiles[&id]);
                            scheduler.stall(id, true);
                        }
                        busy.insert(id);
                        scheduler.stall(id, true);
                        let schema = schemas[&id].clone();
                        let table = tables[&id].clone();
                        let work = work.clone();
                        running.push(Box::pin(async move { work.run(table, schema, transactions, options, candidate).await }));
                    }
                }
                _ = health.tick() => {
                    // Keep durability visible while a catalog publication is stalled.
                    observation.write(config, ledger, capture_goal, true)?;
                    if checkpoints.is_empty()
                        && checkpoint_at.elapsed() >= Duration::from_secs(config.limits.checkpoint_interval_secs)
                        && store.pending_operations()?.is_empty()
                    {
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
            for id in busy.iter().chain(&waiting_for_build) {
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

async fn check_health(
    config: Config,
    client: Option<HealthConnection>,
) -> Result<Option<HealthConnection>> {
    let result: Result<HealthConnection> = async {
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
        check_wal(&client.client, &config).await?;
        Ok(client)
    }
    .await;
    match result {
        Ok(client) => Ok(Some(client)),
        Err(error) if retryable_connection(&error) || error.is::<tokio::time::error::Elapsed>() => {
            // Dropping the owned driver closes even an unanswered query.
            tracing::warn!(%error, "WAL health check interrupted; reconnecting on next check");
            Ok(None)
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

async fn check_wal(client: &Client, config: &Config) -> Result<()> {
    let row = tokio::time::timeout(HEALTH_TIMEOUT, client.query_opt(
        "SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)::bigint, wal_status, safe_wal_size FROM pg_catalog.pg_replication_slots WHERE slot_name=$1",
        &[&config.source.slot],
    )).await.context("WAL health query timed out")??
        .context("replication slot disappeared; resynchronization required")?;
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
    match pressure.health(
        config.limits.wal_soft_bytes,
        config.limits.wal_hard_bytes,
        config.limits.journal_bytes,
    ) {
        SourceHealth::Healthy => {}
        SourceHealth::SlotLost => bail!("replication slot lost WAL; resynchronization required"),
        health => tracing::error!(
            ?health,
            retained_wal_bytes = pressure.retained_bytes,
            journal_bytes,
            "source storage pressure; increase capacity or drain publication lag"
        ),
    }
    Ok(())
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

// Optional work may yield to more CDC for this long. Once due, it receives
// the next available worker; running table operations are never preempted.
const OPTIONAL_MAINTENANCE_DELAY: Duration = Duration::from_secs(1);
const BUILD_MAX_AGE: Duration = Duration::from_secs(30);

#[derive(Clone, Copy, PartialEq, Eq)]
enum BuildAdmission {
    Fenced,
    Wait,
    Start,
}

#[derive(Clone, Copy)]
struct WorkOptions {
    build_active: bool,
    build_admission: BuildAdmission,
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

enum TableOutcome {
    Complete {
        snapshot: Option<i64>,
        maintenance_pending: bool,
    },
    Build(Box<StartedBuild>),
    Deferred,
}

enum MaintenanceOutcome {
    Complete(Table, bool),
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
        mut candidate: Option<CompletedBuild>,
    ) -> Result<TableCompletion> {
        let started = Instant::now();
        let mut attempt = 0u32;
        let mut repair_cursor = DeleteRepairCursor::default();
        let scratch_path = candidate.as_ref().map(|build| build.path.clone());
        let reserved_build =
            candidate.is_some() || options.build_admission == BuildAdmission::Start;
        let result = loop {
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
                    });
                }
            }
        };
        // A candidate's worker has already joined. If table refresh failed
        // before the finalizer took it, retire its independent BUILD record.
        if let Some(candidate) = candidate {
            candidate.ready.discard().await?;
        }
        if let Some(path) = scratch_path {
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
        candidate: &mut Option<CompletedBuild>,
        repair_cursor: &mut DeleteRepairCursor,
    ) -> Result<TableOutcome> {
        let id = schema.table_id;
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
                maintenance_pending: true,
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
        if let Some(candidate) = candidate.take() {
            if candidate.started.elapsed() > BUILD_MAX_AGE {
                candidate.ready.discard().await?;
                return Err(ReplanRequired.into());
            }
            let snapshot = self
                .maintenance
                .finish_compaction(&current, &schema, candidate.ready)
                .await?;
            if snapshot.is_some() {
                metrics::histogram!("flow_compaction_seconds", "table_id" => id.0.to_string(), "kind" => "data")
                    .record(candidate.started.elapsed().as_secs_f64());
                metrics::counter!("flow_compactions_total", "table_id" => id.0.to_string(), "kind" => "data").increment(1);
            }
            return Ok(TableOutcome::Complete {
                snapshot,
                maintenance_pending: true,
            });
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
            let consolidate_deletes = inventory.delete_file_count
                >= self.config.compaction.delete_files_soft
                && !inventory
                    .files
                    .iter()
                    .any(|file| self.config.compaction.needs_delete_reclamation(file));
            let rewritten = if consolidate_deletes {
                self.compact(&current, schema, CompactionKind::Deletes)
                    .await?
            } else {
                None
            };
            if rewritten.is_none() {
                let soft = pressure == flow_compactor::PublicationPressure::Delay;
                if soft && options.build_admission == BuildAdmission::Wait {
                    return Ok(MaintenanceOutcome::Complete(current, true));
                }
                if let Some(build) = self
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
        let manifests_due = inventory.manifest_count >= self.config.limits.manifest_max_count;
        let history_due = history_due(&current, self.config.limits.snapshot_retention_secs)?;
        let garbage_due = self
            .garbage_checked
            .lock()
            .map_err(|_| anyhow::anyhow!("maintenance clock lock poisoned"))?
            .get(&id)
            .is_none_or(|checked| {
                checked.elapsed() >= Duration::from_secs(self.config.limits.garbage_interval_secs)
            });
        if !optional {
            let pending = self.compaction && pressure == flow_compactor::PublicationPressure::Delay
                || manifests_due
                || history_due
                || garbage_due;
            return Ok(MaintenanceOutcome::Complete(current, pending));
        }
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
            self.garbage_checked
                .lock()
                .map_err(|_| anyhow::anyhow!("maintenance clock lock poisoned"))?
                .insert(id, Instant::now());
            metrics::counter!("flow_garbage_delete_requests_total", "table_id" => id.0.to_string())
                .increment(report.delete_requests as u64);
            metrics::histogram!("flow_garbage_collection_seconds", "table_id" => id.0.to_string())
                .record(started.elapsed().as_secs_f64());
        }
        Ok(MaintenanceOutcome::Complete(current, false))
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
