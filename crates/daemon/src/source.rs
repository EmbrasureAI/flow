//! PostgreSQL connection ownership, source identity, and bounded capture delivery.
use crate::config::{Config, Table};
use crate::schema::SchemaRegistry;
use anyhow::{Context, Result, ensure};
use flow_ingress_journal::Journal;
use flow_model::{PgLsn, SourceId, TableId, TableSchema};
use flow_pg_source::{
    Acknowledgement, CaptureAssembler, PgOutputSource, PostgresSource, Relation, SourceEvent,
    SpoolConfig, TransactionSpool, fetch_relation,
    tokio_postgres::{Client, Config as PgConfig, config::ReplicationMode, error::SqlState},
};
use flow_state_store::StateStore;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::watch;

// Bound added low-load latency and the amount of terminal metadata held for one
// durability barrier. An individual larger transaction still remains atomic.
const JOURNAL_GROUP_TRANSACTIONS: usize = 32;
const JOURNAL_GROUP_BYTES: u64 = 4 << 20;
const JOURNAL_GROUP_DELAY: Duration = Duration::from_millis(5);
const SOURCE_SQL_TIMEOUT: Duration = Duration::from_secs(30);
/// pgoutput silently omits configured changes after an admin edits the
/// publication; reconnect alone would notice only by chance.
const PUBLICATION_CHECK_INTERVAL: Duration = if cfg!(test) {
    Duration::from_secs(2)
} else {
    Duration::from_secs(60)
};

async fn source_deadline<T>(operation: impl std::future::Future<Output = Result<T>>) -> Result<T> {
    tokio::time::timeout(SOURCE_SQL_TIMEOUT, operation)
        .await
        .context("PostgreSQL source operation timed out")?
}

fn spool_config(config: &Config) -> SpoolConfig {
    SpoolConfig {
        quota_bytes: config.limits.spool_bytes,
        max_chunk_bytes: config.limits.chunk_bytes,
        ..Default::default()
    }
}

/// FULL replica identity describes row images, not the uniqueness contract.
/// Check the actual primary key before COPY, on reconnect, and when pgoutput
/// invalidates relation metadata, before any changed rows enter the journal.
pub(crate) async fn validate_source_table(
    client: &(impl flow_pg_source::tokio_postgres::GenericClient + Sync),
    configured: &Table,
    schema: &TableSchema,
) -> Result<(Relation, flow_pg_source::TypeRegistry)> {
    let (mut relation, _) = fetch_relation(
        client,
        &configured.source_namespace,
        &configured.source_table,
        !schema.append_only,
    )
    .await?;
    ensure!(
        relation.id == schema.table_id.0,
        "source table was replaced; resynchronization is required"
    );
    if let Some(selected) = configured.projection() {
        ensure!(
            relation
                .columns
                .iter()
                .filter(|column| column.identity)
                .all(|column| selected.contains(&column.name)),
            "column selection must include the complete primary key"
        );
        relation = flow_pg_source::project_relation(&relation, &selected)?;
    }
    let types = flow_pg_source::TypeRegistry::fetch(client, &relation).await?;
    types.validate_relation(schema, &relation)?;
    let actual_key = relation
        .columns
        .iter()
        .enumerate()
        .filter_map(|(index, column)| column.identity.then_some(index))
        .collect::<HashSet<_>>();
    ensure!(
        schema.primary_key.is_empty() && schema.append_only
            || schema.primary_key.iter().copied().collect::<HashSet<_>>() == actual_key,
        "configured primary key differs from PostgreSQL primary key"
    );
    Ok((relation, types))
}

pub(crate) async fn connect(config: &Config, replication: bool) -> Result<Client> {
    let (client, connection) = connect_owned(config, replication).await?;
    connection.detach();
    Ok(client)
}

/// Own the driver as well as the client when cancellation must close the socket.
/// Dropping only a Client can leave its driver awaiting an unanswered query.
pub(crate) struct ConnectionTask(Option<tokio::task::JoinHandle<()>>);

impl ConnectionTask {
    pub(crate) fn spawn(
        connection: impl std::future::Future<
            Output = std::result::Result<(), flow_pg_source::tokio_postgres::Error>,
        > + Send
        + 'static,
    ) -> Self {
        Self(Some(tokio::spawn(async move {
            if let Err(error) = connection.await {
                tracing::warn!(%error, "PostgreSQL connection stopped");
            }
        })))
    }

    fn detach(mut self) {
        drop(self.0.take());
    }
}

impl Drop for ConnectionTask {
    fn drop(&mut self) {
        if let Some(task) = self.0.take() {
            task.abort();
        }
    }
}

pub(crate) async fn connect_owned(
    config: &Config,
    replication: bool,
) -> Result<(Client, ConnectionTask)> {
    let connection_env = &config.source.connection_env;
    let url = std::env::var(connection_env).map_err(|_| {
        crate::exit::config(format!(
            "source connection environment variable is missing: {connection_env}"
        ))
    })?;
    let mut pg: PgConfig = url
        .parse()
        .context("invalid PostgreSQL connection settings")?;
    if replication {
        pg.replication_mode(ReplicationMode::Logical);
    }
    pg.application_name("embrasure-flow");
    // Include the 25-byte XLogData envelope around one pgoutput message.
    // The same admission cap protects binary COPY workers before allocation.
    pg.max_backend_message_bytes(config.limits.source_message_bytes.saturating_add(25));
    pg.connect_timeout(Duration::from_secs(10));
    let tls = postgres_native_tls::MakeTlsConnector::new(crate::source_tls::connector(&pg)?);
    let (client, connection) = tokio::time::timeout(Duration::from_secs(30), pg.connect(tls))
        .await
        .context("PostgreSQL connection timed out")?
        .context("connect to PostgreSQL")?;
    let connection = ConnectionTask::spawn(connection);
    if !replication {
        source_deadline(async {
            client.batch_execute("SET timezone = 'UTC'; SET datestyle = 'ISO, YMD'; SET bytea_output = 'hex'; SET extra_float_digits = 3").await?;
            Ok(())
        }).await?;
    }
    Ok((client, connection))
}

#[derive(Clone, Debug)]
pub(crate) struct CaptureProgress {
    pub durable_lsn: PgLsn,
    pub error: Option<String>,
    /// `error` is a definite publication contract violation.
    pub publication_changed: bool,
}

impl CaptureProgress {
    /// The capture actor's terminal error, keeping a publication contract
    /// violation identifiable across the channel.
    pub(crate) fn failure(&self) -> Option<anyhow::Error> {
        let error = self.error.clone()?;
        Some(if self.publication_changed {
            PublicationChanged(error).into()
        } else {
            anyhow::Error::msg(error)
        })
    }
}

pub(crate) async fn capture_loop(
    config: Config,
    schemas: Vec<TableSchema>,
    store: StateStore,
    mut journal: Journal,
    send: watch::Sender<CaptureProgress>,
    mut ack: watch::Receiver<Acknowledgement>,
) -> Result<()> {
    let capture = async {
        let mut registry =
            SchemaRegistry::new(store, SourceId(config.source.id.clone()), &schemas)?;
        let mut delay = Duration::from_millis(250);
        let mut journal_drained_at = None;
        metrics::gauge!("flow_capture_journal_full").set(0.0);
        loop {
            let connected = tokio::select! {
                _ = send.closed() => return Ok(()),
                result = connect_owned(&config, true) => result,
            };
            let (client, replication_connection) = match connected {
                Ok(client) => client,
                Err(error) if retryable_connection(&error) => {
                    tracing::warn!(%error, "source reconnect pending");
                    tokio::select! { _ = tokio::time::sleep(delay) => {}, _ = send.closed() => return Ok(()) }
                    delay = (delay * 2).min(Duration::from_secs(30));
                    continue;
                }
                Err(error) => return Err(error),
            };
            let started = source_deadline(async {
                verify_source_identity(&client, &config, false).await?;
                let (sql, sql_connection) = connect_owned(&config, false).await?;
                prepare_heartbeat_session(&sql).await?;
                validate_slot(&sql, &config, journal.durable_lsn()).await?;
                // IDENTIFY_SYSTEM above proved this URL on the replication
                // connection, and this SQL session was opened to it just now; a
                // session cannot move to another server afterwards. Where this
                // login can read the system identifier, bind the session too.
                if !source_identity_matches(&sql, &config).await? {
                    tracing::warn!("pg_catalog.pg_control_system() is not executable; the SQL session relies on IDENTIFY_SYSTEM");
                }
                // Before schema refresh: a table the publication stopped covering
                // must latch the drop block, not a quarantine block that would
                // hold acknowledgement.
                for violation in verify_publication(&sql, &config, &schemas).await? {
                    registry.block_publication(violation.table, &violation.reason)?;
                }
                let effective_schemas = registry.initialize(&sql, &config.tables).await?;
                let mut source = PgOutputSource::start(
                    &client,
                    &config.source.slot,
                    &config.source.publication,
                    journal.durable_lsn(),
                    config.limits.source_message_bytes,
                )
                .await?;
                let initial_progress = *ack.borrow_and_update();
                acknowledge_and_reclaim(&mut source, &mut journal, initial_progress).await?;
                Ok((source, sql, sql_connection, effective_schemas))
            })
            .await;
            let (mut source, sql, sql_connection, effective_schemas) = match started {
                Ok(source) => source,
                Err(error) if retryable_connection(&error) => {
                    drop(replication_connection);
                    tracing::warn!(%error, "source disconnected during replication setup");
                    tokio::select! { _ = tokio::time::sleep(delay) => {}, _ = send.closed() => return Ok(()) }
                    delay = (delay * 2).min(Duration::from_secs(30));
                    continue;
                }
                Err(error) => return Err(error),
            };
            // Reconnect proved this session reaches the bound source, so the
            // running check below needs no separate identity proof.
            let publication_schemas = effective_schemas.clone();
            let mut publication_checked_at = Instant::now();
            let spool =
                TransactionSpool::open(config.state_dir.join("spool"), spool_config(&config))?;
            let mut projector = flow_pg_source::EventProjector::new(
                config
                    .tables
                    .iter()
                    .zip(&effective_schemas)
                    .filter_map(|(table, schema)| {
                        table
                            .projection()
                            .map(|columns| (schema.table_id.0, columns))
                    }),
            )?;
            let mut assembler = CaptureAssembler::new(
                SourceId(config.source.id.clone()),
                spool,
                effective_schemas,
                config.limits.chunk_bytes,
            )?
            .with_pending_commit_limit(JOURNAL_GROUP_TRANSACTIONS)?;
            for table in registry.dropped() {
                assembler.drop_table(table)?;
            }
            let mut wire_relations = std::collections::HashMap::new();
            let mut feedback_tick = tokio::time::interval(Duration::from_secs(5));
            feedback_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let mut heartbeat_tick = tokio::time::interval(Duration::from_secs(30));
            heartbeat_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            delay = Duration::from_millis(250);
            let mut group_deadline = None;
            let mut journal_full = None;
            loop {
                tokio::select! {
                    biased;
                    _ = send.closed() => return Ok(()),
                    _ = tokio::time::sleep_until(group_deadline.unwrap_or_else(tokio::time::Instant::now)), if group_deadline.is_some() => {
                        flush_capture(&mut assembler, &mut journal, &send)?;
                        group_deadline = None;
                    }
                    changed = ack.changed() => {
                        if changed.is_err() { return Ok(()); }
                        // Feedback covers an already durable prefix. Let the
                        // next group fill instead of sealing it for its own ACK.
                        let progress = *ack.borrow_and_update();
                        match acknowledge_and_reclaim(&mut source, &mut journal, progress).await {
                            Ok(()) => {},
                            Err(error) if retryable_connection(&error) => {
                                tracing::warn!(%error, "source feedback disconnected"); break;
                            }
                            Err(error) => return Err(error),
                        }
                    }
                    _ = heartbeat_tick.tick() => {
                        // Idle publications still retain WAL from unrelated tables.
                        // A real committed logical message traverses the same durable
                        // journal/ledger path as row changes, without modifying rows.
                        if let Err(error) = source_deadline(async {
                            sql.query_one(
                                "SELECT pg_catalog.pg_logical_emit_message(true, 'embrasure-flow-heartbeat', '')",
                                &[],
                            ).await?;
                            Ok(())
                        }).await {
                            if retryable_connection(&error) {
                                tracing::warn!(%error, "source WAL heartbeat disconnected");
                                break;
                            }
                            return Err(heartbeat_error(error));
                        }
                    }
                    _ = feedback_tick.tick() => {
                        // Do not hold a validated group across SQL/network waits.
                        flush_capture(&mut assembler, &mut journal, &send)?;
                        group_deadline = None;
                        if let Err(error) = source_deadline(registry.initialize(&sql, &config.tables)).await {
                            if retryable_connection(&error) {
                                tracing::warn!(%error, "source schema refresh interrupted; reconnecting");
                                break;
                            }
                            return Err(error);
                        }
                        if publication_checked_at.elapsed() >= PUBLICATION_CHECK_INTERVAL {
                            match source_deadline(verify_publication(&sql, &config, &publication_schemas)).await {
                                Ok(violations) => {
                                    for violation in violations {
                                        registry.block_publication(violation.table, &violation.reason)?;
                                        // An earlier schema block keeps its cause and quarantine.
                                        if registry.is_dropped(violation.table) {
                                            assembler.drop_table(violation.table)?;
                                        }
                                    }
                                    publication_checked_at = Instant::now();
                                }
                                Err(error) if retryable_connection(&error) => {
                                    tracing::warn!(%error, "publication check interrupted; reconnecting");
                                    break;
                                }
                                Err(error) => return Err(error),
                            }
                        }
                        // Feedback is independent of publication notifications;
                        // the journal quota bounds capture during a long backlog.
                        let progress = *ack.borrow();
                        match acknowledge_and_reclaim(&mut source, &mut journal, progress).await {
                            Ok(()) => {},
                            Err(error) if retryable_connection(&error) => {
                                tracing::warn!(%error, "source heartbeat disconnected"); break;
                            }
                            Err(error) => return Err(error),
                        }
                    }
                    next = source.next() => {
                        match next {
                            Ok(Some(event)) => {
                                // Unconfigured publication members never reach
                                // the registry, projector, quarantine or spool.
                                let Some(event) = assembler.configured(event) else { continue };
                                if let SourceEvent::Truncate { xid, subxid, relations, cascade, restart_identity } = &event {
                                    for id in relations {
                                        let table = TableId(*id);
                                        registry.block(table, "source table was truncated; resynchronization is required")?;
                                        registry.block_decoder(table, &mut assembler)?;
                                        assembler.quarantine(SourceEvent::Truncate { xid:*xid,subxid:*subxid,
                                            relations:vec![*id],cascade:*cascade,restart_identity:*restart_identity },
                                            &registry.saved_relation(table)?)?;
                                    }
                                    continue;
                                }
                                let event = if let SourceEvent::Relation(relation) = &event {
                                    let id = TableId(relation.id);
                                    let projected = if registry.is_blocked(id) {
                                        SourceEvent::Relation(projector.quarantine_relation(relation))
                                    } else {
                                        match projector.project(event.clone()) {
                                            Ok(projected) => projected,
                                            Err(error) => {
                                                let error = anyhow::Error::new(error);
                                                if !crate::schema::table_schema_error(&error) { return Err(error); }
                                                registry.block(id, crate::schema::schema_block_reason(&error))?;
                                                SourceEvent::Relation(projector.quarantine_relation(relation))
                                            }
                                        }
                                    };
                                    let SourceEvent::Relation(projected_relation) = &projected else { unreachable!() };
                                    if !registry.is_blocked(id)
                                        && let Err(error) = source_deadline(registry.observe_relation(&sql, projected_relation, &mut assembler)).await {
                                            if retryable_connection(&error) { break; }
                                            if !crate::schema::table_schema_error(&error) { return Err(error); }
                                            registry.block(id, crate::schema::schema_block_reason(&error))?;
                                        }
                                    wire_relations.insert(relation.id, projected_relation.clone());
                                    if registry.is_blocked(id) {
                                        registry.block_decoder(id, &mut assembler)?;
                                        continue;
                                    }
                                    projected
                                } else { projector.project(event)? };
                                let row_table = match &event {
                                    SourceEvent::Insert { relation, .. } | SourceEvent::Update { relation, .. }
                                    | SourceEvent::Delete { relation, .. } => Some(TableId(*relation)),
                                    _ => None,
                                };
                                if let Some(id) = row_table
                                    && registry.is_blocked(id) {
                                        registry.block_decoder(id, &mut assembler)?;
                                        assembler.quarantine(event, wire_relations.get(&id.0).context("row before source relation")?)?;
                                        continue;
                                    }
                                if let SourceEvent::Commit { xid, end_lsn, .. } = &event
                                    && *end_lsn > journal.staged_lsn() {
                                    if registry.validation_may_query() {
                                        flush_capture(&mut assembler, &mut journal, &send)?;
                                        group_deadline = None;
                                    }
                                    let started = Instant::now();
                                    let validated = source_deadline(registry.validate_commit(&sql, *xid, &mut assembler)).await;
                                    metrics::histogram!(
                                        "flow_capture_phase_seconds",
                                        "phase" => "schema_validation",
                                        "result" => if validated.is_ok() { "success" } else { "error" }
                                    ).record(started.elapsed().as_secs_f64());
                                    if let Err(error) = validated {
                                        if retryable_connection(&error) {
                                            tracing::warn!(%error, "source schema proof interrupted; replaying before journal commit");
                                            break;
                                        }
                                        return Err(error);
                                    }
                                }
                                registry.observe_nulls(&event, &mut assembler)?;
                                let retained = row_table.map(|_| event.clone());
                                let commit_xid = match &event { SourceEvent::Commit { xid, .. } => Some(*xid), _ => None };
                                if let Err(error) = assembler.push_buffered_at(event, source.received_lsn, &mut journal) {
                                    if let Some(id) = row_table
                                        && matches!(&error, flow_pg_source::Error::Row(_) | flow_pg_source::Error::Value(_)
                                            | flow_pg_source::Error::ReplicaIdentity(_) | flow_pg_source::Error::DefaultIdentity(_) | flow_pg_source::Error::UnchangedToast(_)) {
                                            registry.block(id, crate::schema::schema_block_reason(&error.into()))?;
                                            registry.block_decoder(id, &mut assembler)?;
                                            assembler.quarantine(retained.expect("row retained"), wire_relations.get(&id.0).context("row before source relation")?)?;
                                            continue;
                                        }
                                    if let Some(xid) = commit_xid
                                        && matches!(&error, flow_pg_source::Error::Journal(flow_ingress_journal::Error::Quota { .. })) {
                                            // Nothing past the durable journal was acknowledged. Drop the
                                            // partial terminal and replay this commit once publication drains.
                                            journal.abort(xid)?;
                                            tracing::warn!(%error, xid, "journal quota reached; pausing capture until publication drains");
                                            journal_full = Some(xid);
                                            break;
                                        }
                                    return Err(error.into());
                                }
                                if assembler.pending_commit_count() > 0 {
                                    group_deadline.get_or_insert_with(|| tokio::time::Instant::now() + JOURNAL_GROUP_DELAY);
                                    if assembler.pending_commit_count() >= JOURNAL_GROUP_TRANSACTIONS
                                        || assembler.pending_commit_bytes() >= JOURNAL_GROUP_BYTES {
                                        flush_capture(&mut assembler, &mut journal, &send)?;
                                        group_deadline = None;
                                    }
                                }
                            }
                            Ok(None) => break,
                            Err(flow_pg_source::Error::Postgres(error)) if retryable_postgres(&error) => {
                                tracing::warn!(%error, "source disconnected; reconnecting from durable journal"); break;
                            }
                            Err(error) => return Err(error.into()),
                        }
                    }
                }
            }
            // Earlier validated commits survive a source disconnect. A partial
            // transaction stays in the disposable spool and is replayed.
            flush_capture(&mut assembler, &mut journal, &send)?;
            drop(sql_connection);
            drop(replication_connection);
            if let Some(xid) = journal_full {
                if !wait_for_journal_drain(
                    &mut journal,
                    &mut ack,
                    &send,
                    xid,
                    config.limits.journal_bytes,
                    &mut journal_drained_at,
                )
                .await?
                {
                    return Ok(());
                }
                continue;
            }
            tokio::select! { _ = tokio::time::sleep(delay) => {}, _ = send.closed() => return Ok(()) }
        }
    };
    // Cancel setup, identity checks, and feedback as well as the receive loop.
    // Closing the coordinator's channel must not wait for a network timeout.
    let result: Result<()> = tokio::select! {
        _ = send.closed() => return Ok(()),
        result = capture => result,
    };
    if let Err(error) = result {
        crate::exit::remember_capture_stop(&error);
        send.send_replace(CaptureProgress {
            durable_lsn: journal.durable_lsn(),
            error: Some(format!("{error:#}")),
            publication_changed: error.is::<PublicationChanged>(),
        });
    }
    Ok(())
}

// Startup and later notifications must reclaim the same proven prefix before
// capture can append more WAL, including when no new watch notification arrives.
async fn acknowledge_and_reclaim(
    source: &mut impl PostgresSource,
    journal: &mut Journal,
    progress: Acknowledgement,
) -> Result<()> {
    source_deadline(async {
        source.acknowledge(progress).await?;
        Ok(())
    })
    .await?;
    journal.reclaim(progress.materialized)?;
    Ok(())
}

/// A full journal pauses capture instead of stopping the daemon: journaled work
/// still drains through publication while the slot retains the unjournaled WAL.
/// The next commit is retried once from a drained journal; failing again at the
/// same durable frontier means it cannot fit and is fatal. Returns false when
/// the coordinator stops capture while waiting.
async fn wait_for_journal_drain(
    journal: &mut Journal,
    ack: &mut watch::Receiver<Acknowledgement>,
    send: &watch::Sender<CaptureProgress>,
    xid: u32,
    quota_bytes: u64,
    drained_at: &mut Option<PgLsn>,
) -> Result<bool> {
    let target = journal.durable_lsn();
    let progress = *ack.borrow_and_update();
    journal.reclaim(progress.materialized)?;
    ensure!(
        *drained_at != Some(target),
        "source transaction {xid} does not fit in the ingress journal after all journaled work was published; increase limits.journal_bytes (currently {quota_bytes} bytes)"
    );
    metrics::gauge!("flow_capture_journal_full").set(1.0);
    let started = Instant::now();
    let mut materialized = progress.materialized;
    let drained = loop {
        if materialized >= target {
            break true;
        }
        tokio::select! {
            _ = send.closed() => break false,
            changed = ack.changed() => {
                if changed.is_err() {
                    break false;
                }
                materialized = ack.borrow_and_update().materialized;
                journal.reclaim(materialized)?;
            }
        }
    };
    metrics::gauge!("flow_capture_journal_full").set(0.0);
    if drained {
        *drained_at = Some(target);
        tracing::info!(
            xid,
            paused_seconds = started.elapsed().as_secs_f64(),
            journal_bytes = journal.bytes_used(),
            "journal drained; resuming capture"
        );
    }
    Ok(drained)
}

/// Publish observations only after the shared journal reader frontier is durable.
fn flush_capture(
    assembler: &mut CaptureAssembler,
    journal: &mut Journal,
    send: &watch::Sender<CaptureProgress>,
) -> Result<()> {
    let transactions = assembler.flush_commits(journal)?;
    if transactions.is_empty() {
        return Ok(());
    }
    for transaction in transactions {
        record_journaled(&transaction);
    }
    send.send_replace(CaptureProgress {
        durable_lsn: journal.durable_lsn(),
        error: None,
        publication_changed: false,
    });
    Ok(())
}

/// Shared durable-capture timing for native source adapters.
pub(crate) fn record_journaled(transaction: &flow_model::SourceTransaction) {
    let journaled_at_micros = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64;
    if transaction.xid != 0 {
        metrics::counter!("flow_journal_transactions_total").increment(1);
        metrics::counter!("flow_journal_committed_payload_bytes_total")
            .increment(transaction.mutation_chunks.payload_bytes());
        if transaction.commit_timestamp_micros > 0
            && let Ok(source_micros) = u64::try_from(transaction.commit_timestamp_micros)
            && let Some(latency) = journaled_at_micros.checked_sub(source_micros)
        {
            metrics::histogram!("flow_commit_to_journal_seconds")
                .record(latency as f64 / 1_000_000.0);
        }
    }
    // Per-transaction evidence; enable with RUST_LOG=info,flow_events=debug.
    tracing::debug!(
        target: "flow_events",
        event = "transaction_journaled",
        source_id = %transaction.source_id.0,
        xid = transaction.xid,
        end_lsn = %transaction.end_lsn,
        commit_timestamp_micros = transaction.commit_timestamp_micros,
        journaled_at_micros,
        journal_payload_bytes = transaction.mutation_chunks.payload_bytes(),
        "source transaction durable"
    );
}

/// The heartbeat commits on capture's SQL session. With a matching
/// `synchronous_standby_names`, Flow's own walsender can be the synchronous
/// standby; a remote-ack commit would then wait for the capture loop it blocks.
async fn prepare_heartbeat_session(sql: &Client) -> Result<()> {
    sql.batch_execute("SET synchronous_commit = local").await?;
    Ok(())
}

/// Name the missing grant only when PostgreSQL reported one.
fn heartbeat_error(error: anyhow::Error) -> anyhow::Error {
    let denied = error.chain().any(|cause| {
        cause
            .downcast_ref::<flow_pg_source::tokio_postgres::Error>()
            .and_then(flow_pg_source::tokio_postgres::Error::code)
            == Some(&SqlState::INSUFFICIENT_PRIVILEGE)
    });
    error.context(if denied {
        "Postgres CDC requires EXECUTE on pg_logical_emit_message for idle WAL progress"
    } else {
        "source WAL heartbeat failed"
    })
}

#[cfg(test)]
mod heartbeat_tests {
    use super::*;

    #[test]
    fn heartbeat_failures_name_a_missing_grant_only_when_reported() {
        let error = heartbeat_error(anyhow::anyhow!("server closed the connection"));
        assert_eq!(
            format!("{error:#}"),
            "source WAL heartbeat failed: server closed the connection"
        );
    }

    #[tokio::test]
    #[ignore = "requires FLOW_POSTGRES_URL pointing to a disposable PostgreSQL database"]
    async fn live_heartbeat_session_commits_without_synchronous_standbys() {
        let mut config: Config =
            toml::from_str(include_str!("../../../examples/flow.toml")).unwrap();
        config.source.connection_env = "FLOW_POSTGRES_URL".into();
        let (sql, _connection) = connect_owned(&config, false).await.unwrap();
        prepare_heartbeat_session(&sql).await.unwrap();
        let setting: String = sql
            .query_one("SHOW synchronous_commit", &[])
            .await
            .unwrap()
            .get(0);
        assert_eq!(setting, "local");
    }
}

fn retryable_postgres(error: &flow_pg_source::tokio_postgres::Error) -> bool {
    // Malformed or over-limit advertised frames cannot be repaired by replay.
    // Treat transport admission failures as fatal rather than reconnecting to
    // the same unprocessable row forever.
    let mut cause = std::error::Error::source(error);
    while let Some(error) = cause {
        if error
            .downcast_ref::<std::io::Error>()
            .is_some_and(|error| error.kind() == std::io::ErrorKind::InvalidData)
        {
            return false;
        }
        cause = error.source();
    }
    // PostgreSQL sends ErrorResponse before closing an administratively stopped
    // connection. Retry those connection failures, not arbitrary SQL errors such
    // as authentication failures, missing slots, or invalid replication options.
    error.code().is_none_or(|code| {
        matches!(
            *code,
            SqlState::ADMIN_SHUTDOWN
                | SqlState::CRASH_SHUTDOWN
                | SqlState::CANNOT_CONNECT_NOW
                | SqlState::CONNECTION_EXCEPTION
                | SqlState::CONNECTION_DOES_NOT_EXIST
                | SqlState::CONNECTION_FAILURE
                | SqlState::SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION
        )
    })
}

pub(crate) fn retryable_connection(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        cause
            .downcast_ref::<flow_pg_source::tokio_postgres::Error>()
            .is_some_and(retryable_postgres)
            || cause
                .downcast_ref::<tokio::time::error::Elapsed>()
                .is_some()
    })
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
struct SourceIdentity {
    source_id: String,
    slot: String,
    system_identifier: String,
    database: String,
    timeline: u32,
}

pub(crate) async fn verify_source_identity(
    client: &Client,
    config: &Config,
    initialize: bool,
) -> Result<()> {
    use flow_pg_source::tokio_postgres::SimpleQueryMessage;
    use std::io::Write;
    let response = client.simple_query("IDENTIFY_SYSTEM").await?;
    let row = response
        .iter()
        .find_map(|message| match message {
            SimpleQueryMessage::Row(row) => Some(row),
            _ => None,
        })
        .context("IDENTIFY_SYSTEM returned no source identity")?;
    let identity = SourceIdentity {
        source_id: config.source.id.clone(),
        slot: config.source.slot.clone(),
        system_identifier: row
            .get("systemid")
            .context("source system identifier is missing")?
            .to_owned(),
        database: row
            .get("dbname")
            .context("replication connection must identify a database")?
            .to_owned(),
        timeline: row
            .get("timeline")
            .context("source timeline is missing")?
            .parse()?,
    };
    let path = config.state_dir.join("source-identity.json");
    match std::fs::read(&path) {
        Ok(bytes) => {
            let expected: SourceIdentity = serde_json::from_slice(&bytes).context("invalid saved source identity")?;
            ensure!(identity == expected, crate::exit::resync("PostgreSQL source system, database, timeline, or slot lineage changed; coordinated failover/resynchronization is required"));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound && initialize => {
            let temporary = path.with_extension("json.tmp");
            let mut file = std::fs::File::create(&temporary)?;
            file.write_all(&serde_json::to_vec(&identity)?)?;
            file.sync_all()?;
            std::fs::rename(temporary, path)?;
            std::fs::File::open(&config.state_dir)?.sync_all()?;
        }
        Err(error) => return Err(error).context("source identity proof is missing or unreadable; do not bind existing state to an unverified source"),
    }
    Ok(())
}

async fn validate_slot(client: &Client, config: &Config, durable_lsn: PgLsn) -> Result<()> {
    let row = client.query_opt("SELECT plugin, slot_type, database = current_database(), confirmed_flush_lsn::text, restart_lsn::text, wal_status FROM pg_catalog.pg_replication_slots WHERE slot_name = $1", &[&config.source.slot]).await?
        .ok_or_else(|| crate::exit::resync("replication slot disappeared; source resynchronization is required"))?;
    ensure!(
        row.get::<_, Option<String>>(0).as_deref() == Some("pgoutput")
            && row.get::<_, String>(1) == "logical"
            && row.get::<_, Option<bool>>(2) == Some(true),
        crate::exit::resync(
            "replication slot plugin/type/database differs from the initialized source"
        )
    );
    ensure!(
        row.get::<_, Option<String>>(5).as_deref() != Some("lost")
            && row.get::<_, Option<String>>(4).is_some(),
        crate::exit::resync(
            "replication slot lost required WAL; source resynchronization is required"
        )
    );
    let confirmed: PgLsn = row
        .get::<_, Option<String>>(3)
        .context("replication slot has no confirmed LSN")?
        .parse()?;
    ensure!(
        confirmed <= durable_lsn,
        crate::exit::resync(
            "PostgreSQL slot has acknowledged beyond the local durable journal; another consumer or storage loss requires source reconciliation"
        )
    );
    Ok(())
}

/// Before slot creation any violation is a setup error. Afterwards,
/// publication-scoped violations fail with [`PublicationChanged`] and
/// table-scoped ones are returned for the caller to block.
pub(crate) async fn validate_publication(
    client: &Client,
    config: &Config,
    schemas: &[TableSchema],
    initialize: bool,
) -> Result<Vec<TableViolation>> {
    let replication = connect(config, true).await?;
    if initialize {
        // Before slot creation nothing can have been skipped yet, and the
        // identity proof is written only after the publication validates.
        validate_publication_contract(client, config, schemas).await?;
        verify_source_identity(&replication, config, true).await?;
        return Ok(Vec::new());
    }
    // A verdict that permanently requires resync must concern the bound source;
    // a misdirected connection is an identity error instead.
    verify_source_identity(&replication, config, false).await?;
    // IDENTIFY_SYSTEM proved this URL; also reject a SQL session that reached
    // another source, where this login can read the system identifier.
    source_identity_matches(client, config).await?;
    verify_publication(client, config, schemas).await
}

/// A query connection cannot run IDENTIFY_SYSTEM. Prove it reaches the bound
/// source's system and database before a check that can require a resync;
/// timeline changes break replication and are proven at its reconnect.
/// `false` means this login cannot read the system identifier.
pub(crate) async fn source_identity_matches(client: &Client, config: &Config) -> Result<bool> {
    let row = match client
        .query_one(
            "SELECT system_identifier::text, current_database()::text FROM pg_catalog.pg_control_system()",
            &[],
        )
        .await
    {
        Ok(row) => row,
        Err(error) if error.code() == Some(&SqlState::INSUFFICIENT_PRIVILEGE) => return Ok(false),
        Err(error) => return Err(error.into()),
    };
    let expected: SourceIdentity = serde_json::from_slice(
        &std::fs::read(config.state_dir.join("source-identity.json"))
            .context("source identity proof is missing or unreadable")?,
    )
    .context("invalid saved source identity")?;
    ensure!(
        row.try_get::<_, String>(0)? == expected.system_identifier
            && row.try_get::<_, String>(1)? == expected.database,
        crate::exit::resync(
            "PostgreSQL source system or database changed; coordinated failover/resynchronization is required"
        )
    );
    Ok(true)
}

/// The publication does not satisfy the capture contract, as opposed to a
/// failed check. Before slot creation this is a setup error.
#[derive(Debug)]
pub(crate) struct PublicationMismatch(String);
impl std::fmt::Display for PublicationMismatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for PublicationMismatch {}

/// A mismatch after slot creation. Status records it as `publication_changed`
/// before the process exits, like a lost slot.
#[derive(Debug)]
pub(crate) struct PublicationChanged(String);
impl std::fmt::Display for PublicationChanged {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for PublicationChanged {}

/// One configured table no longer satisfies the contract; pgoutput may have
/// skipped its changes, so it needs a resync. `reason` is for logs only.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TableViolation {
    pub(crate) table: TableId,
    pub(crate) reason: String,
}

fn publication_changed(publication: &str, error: anyhow::Error) -> anyhow::Error {
    error.context(PublicationChanged(format!(
        "PostgreSQL publication {publication:?} no longer matches the capture contract; changes to configured tables may have been skipped, so resynchronization is required"
    )))
}

/// Once the slot exists, pgoutput silently omits changes outside the contract.
/// Restoring the setting cannot recover them, so a violation needs a resync.
/// A missing publication or unpublished operation affects every table and
/// fails with [`PublicationChanged`]. Violations limited to configured tables
/// (membership, row filter, columns) are returned so only those are blocked.
/// Startup, reconnect and the running check all use this proof.
pub(crate) async fn verify_publication(
    client: &Client,
    config: &Config,
    schemas: &[TableSchema],
) -> Result<Vec<TableViolation>> {
    let publication = &config.source.publication;
    publication_contract(client, config, schemas)
        .await
        .map_err(|error| {
            if error.is::<PublicationMismatch>() {
                publication_changed(publication, error)
            } else {
                error
            }
        })
}

/// Where only a connection-wide stop is safe, as during bootstrap, a table
/// violation fails like a publication-scoped one.
pub(crate) fn violations_are_fatal(config: &Config, violations: Vec<TableViolation>) -> Result<()> {
    let Some(first) = violations.into_iter().next() else {
        return Ok(());
    };
    Err(publication_changed(
        &config.source.publication,
        PublicationMismatch(first.reason).into(),
    ))
}

/// Before slot creation every violation is a setup error.
pub(crate) async fn validate_publication_contract(
    client: &Client,
    config: &Config,
    schemas: &[TableSchema],
) -> Result<()> {
    let violations = publication_contract(client, config, schemas).await?;
    if violations.is_empty() {
        return Ok(());
    }
    Err(PublicationMismatch(
        violations
            .into_iter()
            .map(|violation| violation.reason)
            .collect::<Vec<_>>()
            .join("; "),
    )
    .into())
}

/// The publication must contain every configured table and may contain
/// others, such as an admin-owned `FOR ALL TABLES` or `FOR TABLES IN SCHEMA`
/// publication. Only configured tables' filters and columns matter.
pub(crate) async fn publication_contract(
    client: &Client,
    config: &Config,
    schemas: &[TableSchema],
) -> Result<Vec<TableViolation>> {
    ensure!(
        schemas.len() == config.tables.len(),
        "schema/config table count differs"
    );
    let relations: Vec<u32> = schemas.iter().map(|schema| schema.table_id.0).collect();
    let publication = &config.source.publication;
    let configured: Vec<_> = config
        .tables
        .iter()
        .zip(schemas)
        .map(|(table, schema)| {
            (
                format!("{}.{}", table.source_namespace, table.source_table),
                schema,
            )
        })
        .collect();
    let facts = publication_facts(client, publication, &relations).await?;
    match check_publication(&facts, &configured) {
        Ok(violations) if violations.is_empty() => Ok(violations),
        // pg_publication_tables is computed through the catalog cache, which a
        // concurrent ALTER PUBLICATION can advance past the transaction
        // snapshot. Only a mismatch that a fresh read confirms is definite,
        // whether it is publication- or table-scoped.
        _ => {
            let facts = publication_facts(client, publication, &relations).await?;
            Ok(check_publication(&facts, &configured)?)
        }
    }
}

/// Catalog facts for the configured members of one publication.
#[derive(Debug, Default)]
struct PublicationFacts {
    version: i32,
    /// INSERT, UPDATE, DELETE and TRUNCATE; `None` when the publication is missing.
    operations: Option<[bool; 4]>,
    publish_generated: bool,
    members: Vec<PublishedTable>,
}

/// PostgreSQL 14 has neither row filters nor column lists; its column
/// vectors stay empty and trivially equal.
#[derive(Clone, Debug, Default)]
struct PublishedTable {
    relation_id: u32,
    row_filter: bool,
    full_identity: bool,
    published: Vec<String>,
    current: Vec<String>,
    generated: Vec<String>,
}

/// Read every catalog fact of one contract check in one snapshot, so an ALTER
/// PUBLICATION between reads cannot combine two states. A failed read rolls
/// back, leaving the session usable; a lost session stays a transient error.
async fn publication_facts(
    client: &Client,
    publication: &str,
    relations: &[u32],
) -> Result<PublicationFacts> {
    client
        .batch_execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
        .await?;
    let facts = read_publication_facts(client, publication, relations).await;
    let end = client
        .batch_execute(if facts.is_ok() { "COMMIT" } else { "ROLLBACK" })
        .await;
    let facts = facts?;
    end?;
    Ok(facts)
}

async fn read_publication_facts(
    client: &Client,
    publication: &str,
    relations: &[u32],
) -> Result<PublicationFacts> {
    let version: i32 = client
        .query_one("SELECT current_setting('server_version_num')::integer", &[])
        .await?
        .try_get(0)?;
    ensure!(
        (140000..190000).contains(&version),
        "supported PostgreSQL versions are 14 through 18"
    );
    let Some(row) = client
        .query_opt(
            if version >= 180000 {
                "SELECT pubinsert, pubupdate, pubdelete, pubtruncate, pubgencols::text = 's' FROM pg_catalog.pg_publication WHERE pubname=$1"
            } else {
                "SELECT pubinsert, pubupdate, pubdelete, pubtruncate, false FROM pg_catalog.pg_publication WHERE pubname=$1"
            },
            &[&publication],
        )
        .await?
    else {
        return Ok(PublicationFacts {
            version,
            ..Default::default()
        });
    };
    let operations = [
        row.try_get(0)?,
        row.try_get(1)?,
        row.try_get(2)?,
        row.try_get(3)?,
    ];
    let publish_generated: bool = row.try_get(4)?;
    // Filter to configured relations in SQL: FOR ALL TABLES and schema
    // publications also list every unrelated table, and this runs periodically.
    let members = if version < 150000 {
        client
            .query(
                "SELECT c.oid FROM pg_catalog.pg_publication_tables p
             JOIN pg_catalog.pg_namespace n ON p.schemaname=n.nspname
             JOIN pg_catalog.pg_class c ON c.relnamespace=n.oid AND c.relname=p.tablename
             WHERE p.pubname=$1 AND c.oid = ANY($2)",
                &[&publication, &relations],
            )
            .await?
            .into_iter()
            .map(|row| -> Result<_> {
                Ok(PublishedTable {
                    relation_id: row.try_get(0)?,
                    ..Default::default()
                })
            })
            .collect::<Result<_>>()?
    } else {
        client
            .query(
                "SELECT c.oid, p.rowfilter IS NOT NULL, c.relreplident = 'f', p.attnames,
             ARRAY(SELECT a.attname FROM pg_catalog.pg_attribute a
             WHERE a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped AND (a.attgenerated = '' OR ($3 AND a.attgenerated = 's')) ORDER BY a.attnum),
             ARRAY(SELECT a.attname FROM pg_catalog.pg_attribute a
             WHERE a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped AND a.attgenerated = 's')
             FROM pg_catalog.pg_publication_tables p
             JOIN pg_catalog.pg_namespace n ON p.schemaname=n.nspname
             JOIN pg_catalog.pg_class c ON c.relnamespace=n.oid AND c.relname=p.tablename
             WHERE p.pubname=$1 AND c.oid = ANY($2)",
                &[&publication, &relations, &publish_generated],
            )
            .await?
            .into_iter()
            .map(|row| -> Result<_> {
                Ok(PublishedTable {
                    relation_id: row.try_get(0)?,
                    row_filter: row.try_get(1)?,
                    full_identity: row.try_get(2)?,
                    published: row.try_get(3)?,
                    current: row.try_get(4)?,
                    generated: row.try_get(5)?,
                })
            })
            .collect::<Result<_>>()?
    };
    Ok(PublicationFacts {
        version,
        operations: Some(operations),
        publish_generated,
        members,
    })
}

/// Pure contract decision over the catalog facts. Unconfigured members are
/// ignored, including their row filters, column lists and replica identity.
/// A missing publication or unpublished operation affects every table and is
/// an error; per-table violations are returned, one per table.
fn check_publication(
    facts: &PublicationFacts,
    configured: &[(String, &TableSchema)],
) -> std::result::Result<Vec<TableViolation>, PublicationMismatch> {
    let Some(operations) = facts.operations else {
        return Err(PublicationMismatch("publication not found".into()));
    };
    let unpublished: Vec<_> = ["INSERT", "UPDATE", "DELETE", "TRUNCATE"]
        .into_iter()
        .zip(operations)
        .filter_map(|(operation, published)| (!published).then_some(operation))
        .collect();
    if !unpublished.is_empty() {
        return Err(PublicationMismatch(format!(
            "publication must include INSERT, UPDATE, DELETE, and TRUNCATE so unsupported operations cannot be silently skipped; missing {}",
            unpublished.join(", ")
        )));
    }
    let members: HashMap<u32, &PublishedTable> = facts
        .members
        .iter()
        .map(|table| (table.relation_id, table))
        .collect();
    Ok(configured
        .iter()
        .filter_map(|(name, schema)| {
            table_violation(facts, members.get(&schema.table_id.0).copied(), schema).map(|reason| {
                TableViolation {
                    table: schema.table_id,
                    reason: format!("{reason}: {name}"),
                }
            })
        })
        .collect())
}

fn table_violation(
    facts: &PublicationFacts,
    table: Option<&PublishedTable>,
    schema: &TableSchema,
) -> Option<&'static str> {
    let Some(table) = table else {
        return Some("publication must include every configured source table; missing");
    };
    if table.row_filter {
        return Some("publication row filters are not supported by initial COPY");
    }
    if facts.version >= 180000
        && !facts.publish_generated
        && table.full_identity
        && !table.generated.is_empty()
    {
        return Some(
            "PostgreSQL 18 FULL replica identity requires publish_generated_columns=stored even when generated columns are excluded",
        );
    }
    // PG15's catalog view includes generated columns even though pgoutput
    // omits them; newer views omit them too. Normalize only these known
    // non-published fields, never ordinary ones.
    let published: Vec<&String> = table
        .published
        .iter()
        .filter(|column| facts.version >= 180000 || !table.generated.contains(column))
        .collect();
    if !published.iter().copied().eq(&table.current) {
        return Some("publication must include every current source column");
    }
    // Current ordinary columns are covered by the full-publication check
    // above. Missing historical names belong to the schema registry's durable
    // table block, so they must not stop healthy tables on restart. Still
    // reject selected generated fields that COPY would read but pgoutput omits.
    if !schema
        .columns
        .iter()
        .all(|column| !table.generated.contains(&column.name) || published.contains(&&column.name))
    {
        return Some(
            "publication omits a configured source column; snapshot and CDC must use the same columns",
        );
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use flow_ingress_journal::JournalConfig;
    use flow_model::{SourceTransaction, TableId, TableMutationCount, TableSchemaVersion};

    struct Feedback;
    #[async_trait::async_trait]
    impl PostgresSource for Feedback {
        async fn next(&mut self) -> flow_pg_source::Result<Option<SourceEvent>> {
            unreachable!()
        }
        async fn acknowledge(&mut self, _: Acknowledgement) -> flow_pg_source::Result<()> {
            Ok(())
        }
    }

    fn commit_test_transaction(journal: &mut Journal, xid: u32, payload: &[u8]) {
        journal.append_chunk(xid, payload).unwrap();
        commit_appended(journal, xid);
    }

    fn commit_appended(journal: &mut Journal, xid: u32) {
        journal
            .commit(SourceTransaction {
                source_id: SourceId("source".into()),
                xid,
                begin_lsn: PgLsn(u64::from(xid) * 10 - 2),
                commit_lsn: PgLsn(u64::from(xid) * 10),
                end_lsn: PgLsn(u64::from(xid) * 10 + 1),
                commit_timestamp_micros: 0,
                schema_versions: vec![TableSchemaVersion {
                    table_id: TableId(1),
                    version: 1,
                }],
                affected_tables: vec![TableId(1)],
                mutation_chunks: journal.transaction_chunks(xid),
                table_mutation_counts: Some(vec![TableMutationCount {
                    table_id: TableId(1),
                    mutations: 1,
                }]),
            })
            .unwrap();
    }

    #[tokio::test]
    async fn full_journal_pauses_capture_until_publication_drains() {
        let directory = tempfile::tempdir().unwrap();
        let config = JournalConfig {
            segment_bytes: 600,
            quota_bytes: 2_400,
            max_frame_bytes: 400,
            max_open_transactions: 8,
        };
        let (mut journal, _) = Journal::open(directory.path(), config).unwrap();
        let mut xid = 1;
        let error = loop {
            match journal.append_chunk(xid, &[xid as u8; 300]) {
                Ok(_) => {
                    commit_appended(&mut journal, xid);
                    xid += 1;
                }
                Err(error) => break error,
            }
        };
        assert!(matches!(error, flow_ingress_journal::Error::Quota { .. }));
        journal.abort(xid).unwrap();
        let durable = journal.durable_lsn();
        let (acknowledge, mut ack) = watch::channel(Acknowledgement::default());
        let (send, _progress) = watch::channel(CaptureProgress {
            durable_lsn: durable,
            error: None,
            publication_changed: false,
        });
        let waiting = tokio::spawn(async move {
            let drained =
                wait_for_journal_drain(&mut journal, &mut ack, &send, xid, 2_400, &mut None)
                    .await
                    .unwrap();
            (drained, journal)
        });
        acknowledge.send_replace(Acknowledgement {
            received: durable,
            durable,
            materialized: PgLsn(11),
        });
        tokio::task::yield_now().await;
        assert!(
            !waiting.is_finished(),
            "capture resumed before the journal drained"
        );
        acknowledge.send_replace(Acknowledgement {
            received: durable,
            durable,
            materialized: durable,
        });
        let (drained, mut journal) = waiting.await.unwrap();
        assert!(drained);
        commit_test_transaction(&mut journal, xid, &[xid as u8; 300]);
    }

    #[tokio::test]
    async fn transaction_larger_than_a_drained_journal_is_fatal() {
        let directory = tempfile::tempdir().unwrap();
        let config = JournalConfig {
            segment_bytes: 600,
            quota_bytes: 1_200,
            max_frame_bytes: 400,
            max_open_transactions: 8,
        };
        let (mut journal, _) = Journal::open(directory.path(), config).unwrap();
        commit_test_transaction(&mut journal, 1, &[1; 300]);
        let durable = journal.durable_lsn();
        let (_acknowledge, mut ack) = watch::channel(Acknowledgement {
            received: durable,
            durable,
            materialized: durable,
        });
        journal.reclaim(durable).unwrap();
        let error = loop {
            match journal.append_chunk(2, &[2; 300]) {
                Ok(_) => {}
                Err(error) => break error,
            }
        };
        assert!(matches!(error, flow_ingress_journal::Error::Quota { .. }));
        journal.abort(2).unwrap();
        let (send, _progress) = watch::channel(CaptureProgress {
            durable_lsn: durable,
            error: None,
            publication_changed: false,
        });
        // Retry once from the drained journal; the same failure again is fatal.
        let mut drained_at = None;
        assert!(
            wait_for_journal_drain(&mut journal, &mut ack, &send, 2, 1_200, &mut drained_at)
                .await
                .unwrap()
        );
        let error = loop {
            match journal.append_chunk(2, &[2; 300]) {
                Ok(_) => {}
                Err(error) => break error,
            }
        };
        assert!(matches!(error, flow_ingress_journal::Error::Quota { .. }));
        journal.abort(2).unwrap();
        let error =
            wait_for_journal_drain(&mut journal, &mut ack, &send, 2, 1_200, &mut drained_at)
                .await
                .unwrap_err();
        assert!(
            error.to_string().contains("increase limits.journal_bytes"),
            "{error:#}"
        );
    }

    #[tokio::test]
    async fn journal_drain_wait_stops_with_capture() {
        let directory = tempfile::tempdir().unwrap();
        let (mut journal, _) = Journal::open(
            directory.path(),
            JournalConfig {
                segment_bytes: 600,
                quota_bytes: 2_400,
                max_frame_bytes: 400,
                max_open_transactions: 8,
            },
        )
        .unwrap();
        commit_test_transaction(&mut journal, 1, &[1; 300]);
        let (acknowledge, mut ack) = watch::channel(Acknowledgement::default());
        let (send, progress) = watch::channel(CaptureProgress {
            durable_lsn: journal.durable_lsn(),
            error: None,
            publication_changed: false,
        });
        drop(progress);
        assert!(
            !wait_for_journal_drain(&mut journal, &mut ack, &send, 2, 2_400, &mut None)
                .await
                .unwrap()
        );
        drop(acknowledge);
    }

    #[tokio::test]
    async fn recovered_initial_ack_reclaims_before_the_next_append_without_watch_change() {
        let directory = tempfile::tempdir().unwrap();
        let mut config = JournalConfig {
            segment_bytes: 600,
            quota_bytes: 16_000,
            max_frame_bytes: 400,
            max_open_transactions: 8,
        };
        let (mut journal, _) = Journal::open(directory.path(), config.clone()).unwrap();
        for xid in 1..=6 {
            journal.append_chunk(xid, &[xid as u8; 300]).unwrap();
            journal
                .commit(SourceTransaction {
                    source_id: SourceId("source".into()),
                    xid,
                    begin_lsn: PgLsn(u64::from(xid) * 10 - 2),
                    commit_lsn: PgLsn(u64::from(xid) * 10),
                    end_lsn: PgLsn(u64::from(xid) * 10 + 1),
                    commit_timestamp_micros: 0,
                    schema_versions: vec![TableSchemaVersion {
                        table_id: TableId(1),
                        version: 1,
                    }],
                    affected_tables: vec![TableId(1)],
                    mutation_chunks: journal.transaction_chunks(xid),
                    table_mutation_counts: Some(vec![TableMutationCount {
                        table_id: TableId(1),
                        mutations: 1,
                    }]),
                })
                .unwrap();
        }
        config.quota_bytes = journal.bytes_used() + 128;
        drop(journal);
        let (mut journal, _) = Journal::open(directory.path(), config).unwrap();
        assert!(matches!(
            journal.append_chunk(7, &[7; 300]),
            Err(flow_ingress_journal::Error::Quota { .. })
        ));
        let (_sender, mut ack) = watch::channel(Acknowledgement {
            received: PgLsn(61),
            durable: PgLsn(61),
            materialized: PgLsn(41),
        });
        let progress = *ack.borrow_and_update();
        acknowledge_and_reclaim(&mut Feedback, &mut journal, progress)
            .await
            .unwrap();
        assert!(!ack.has_changed().unwrap());
        assert_eq!(
            journal
                .transactions()
                .iter()
                .unwrap()
                .map(|t| t.unwrap().xid)
                .collect::<Vec<_>>(),
            [5, 6]
        );
        journal.append_chunk(7, &[7; 300]).unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn unanswered_source_operation_is_retryable_and_its_owned_driver_is_cancelled() {
        let (closed, observed) = tokio::sync::oneshot::channel::<()>();
        let driver = tokio::spawn(async move {
            let _closed = closed;
            std::future::pending::<()>().await;
        });
        let connection = ConnectionTask(Some(driver));
        let error = source_deadline(std::future::pending::<Result<()>>())
            .await
            .unwrap_err();
        assert!(retryable_connection(&error));
        drop(connection);
        assert!(
            observed.await.is_err(),
            "driver must release resources on reconnect"
        );
    }

    #[test]
    fn only_violations_after_slot_creation_are_recorded_as_publication_changed() {
        let root = tempfile::tempdir().unwrap();
        let mut config: Config =
            toml::from_str(include_str!("../../../examples/flow.toml")).unwrap();
        config.state_dir = root.path().to_owned();
        let status = || -> serde_json::Value {
            serde_json::from_slice(&std::fs::read(root.path().join("status.json")).unwrap())
                .unwrap()
        };
        // Before slot creation a mismatch is an ordinary setup error.
        let setup = anyhow::Error::new(PublicationMismatch("publication not found".into()));
        crate::lifecycle::record_publication_changed(&config, Err::<(), _>(setup)).unwrap_err();
        assert!(!root.path().join("status.json").exists());

        let mut progress = CaptureProgress {
            durable_lsn: PgLsn(0),
            error: Some("source disconnected".into()),
            publication_changed: false,
        };
        let failure = progress.failure().unwrap();
        assert!(!failure.is::<PublicationChanged>());
        crate::lifecycle::record_publication_changed(&config, Err::<(), _>(failure)).unwrap_err();
        assert!(!root.path().join("status.json").exists());
        assert!(
            !root
                .path()
                .join("publication-resync-required.json")
                .exists()
        );
        crate::lifecycle::refuse_if_resync_required(&config).unwrap();

        // The capture actor's violation keeps its type and exact text.
        progress.error =
            Some("PostgreSQL publication no longer matches: missing public.orders".into());
        progress.publication_changed = true;
        let failure = progress.failure().unwrap();
        assert!(failure.is::<PublicationChanged>());
        assert_eq!(
            failure.to_string(),
            "PostgreSQL publication no longer matches: missing public.orders"
        );
        crate::lifecycle::record_publication_changed(&config, Err::<(), _>(failure)).unwrap_err();
        assert_eq!(status()["source_health"], "publication_changed");
        assert_eq!(status()["ready"], false);
        crate::lifecycle::refuse_if_resync_required(&config).unwrap_err();
    }

    /// A URL that now reaches another database of the bound cluster, where the
    /// publication is absent, fails identity before any resync verdict.
    #[tokio::test]
    #[ignore = "requires FLOW_POSTGRES_URL pointing to a disposable PostgreSQL database"]
    async fn live_wrong_database_is_an_identity_error_without_resync_marker() {
        use flow_pg_source::tokio_postgres::{self, NoTls};

        let url = std::env::var("FLOW_POSTGRES_URL").expect("FLOW_POSTGRES_URL");
        let (sql, connection) = tokio_postgres::connect(&url, NoTls).await.unwrap();
        let sql_task = tokio::spawn(connection);
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = tempfile::tempdir().unwrap();
        let mut config: Config =
            toml::from_str(include_str!("../../../examples/flow.toml")).unwrap();
        config.state_dir = root.path().to_owned();
        let slot = format!("flow_public_identity_{suffix}");
        config.source.connection_env = "FLOW_POSTGRES_URL".into();
        config.source.slot = slot.clone();
        config.source.publication = format!("flow_public_missing_{suffix}");
        let row = sql
            .query_one(
                "SELECT s.system_identifier::text, c.timeline_id FROM pg_catalog.pg_control_system() s, pg_catalog.pg_control_checkpoint() c",
                &[],
            )
            .await
            .unwrap();
        std::fs::write(
            root.path().join("source-identity.json"),
            serde_json::to_vec(&SourceIdentity {
                source_id: config.source.id.clone(),
                slot,
                system_identifier: row.get(0),
                database: "flow_public_original".into(),
                timeline: row.get::<_, i32>(1) as u32,
            })
            .unwrap(),
        )
        .unwrap();
        let schemas = [config.tables[0].schema(1)];
        // Checked alone, the missing publication would demand a resync.
        assert!(
            verify_publication(&sql, &config, &schemas)
                .await
                .unwrap_err()
                .is::<PublicationChanged>()
        );
        let error = crate::lifecycle::record_publication_changed(
            &config,
            validate_publication(&sql, &config, &schemas, false).await,
        )
        .unwrap_err();
        assert!(!error.is::<PublicationChanged>(), "{error:#}");
        assert!(
            format!("{error:#}").contains("slot lineage changed"),
            "{error:#}"
        );
        assert!(
            !root
                .path()
                .join("publication-resync-required.json")
                .exists()
        );
        assert!(!root.path().join("status.json").exists());
        drop(sql);
        sql_task.await.unwrap().unwrap();
    }

    /// The catalog queries and parameter types against real PostgreSQL 15+.
    #[tokio::test]
    #[ignore = "requires FLOW_POSTGRES_URL pointing to a disposable PostgreSQL database"]
    async fn live_publication_contract_accepts_supersets_and_detects_changes() {
        use flow_pg_source::tokio_postgres::{self, NoTls};
        use futures::FutureExt;

        let url = std::env::var("FLOW_POSTGRES_URL").expect("FLOW_POSTGRES_URL");
        let (sql, connection) = tokio_postgres::connect(&url, NoTls).await.unwrap();
        let sql_task = tokio::spawn(connection);
        let name = format!(
            "flow_public_contract_{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        sql.batch_execute(&format!(
            "CREATE SCHEMA {name};
             CREATE TABLE {name}.orders (id bigint PRIMARY KEY, status text);
             ALTER TABLE {name}.orders REPLICA IDENTITY FULL;
             CREATE TABLE {name}.other (id integer PRIMARY KEY, payload text);
             CREATE PUBLICATION {name}_schema FOR TABLES IN SCHEMA {name};
             CREATE PUBLICATION {name} FOR TABLE {name}.orders, {name}.other (id) WHERE (id > 0);"
        ))
        .await
        .unwrap();
        let body = std::panic::AssertUnwindSafe(async {
            let mut config: Config =
                toml::from_str(include_str!("../../../examples/flow.toml")).unwrap();
            config.tables[0].source_namespace = name.clone();
            let orders: u32 = sql
                .query_one("SELECT to_regclass($1)::oid", &[&format!("{name}.orders")])
                .await
                .unwrap()
                .get(0);
            let schemas = [config.tables[0].schema(orders)];
            let mut verify = async |publication: String, change: &str| {
                if !change.is_empty() {
                    sql.batch_execute(change).await.unwrap();
                }
                config.source.connection_env = "FLOW_POSTGRES_URL".into();
                config.source.slot = name.clone();
                config.source.publication = publication;
                verify_publication(&sql, &config, &schemas)
                    .await
                    .map(|violations| {
                        violations
                            .into_iter()
                            .map(|violation| {
                                assert_eq!(violation.table, TableId(orders));
                                violation.reason
                            })
                            .collect::<Vec<_>>()
                    })
                    .map_err(|error| {
                        assert!(error.is::<PublicationChanged>(), "{error:#}");
                        format!("{error:#}")
                    })
            };
            let clean = Vec::<String>::new();
            // Schema publications gain every new table automatically.
            assert_eq!(verify(format!("{name}_schema"), "").await.unwrap(), clean);
            assert_eq!(
                verify(
                    format!("{name}_schema"),
                    &format!("CREATE TABLE {name}.later (id integer)"),
                )
                .await
                .unwrap(),
                clean
            );
            // Other tables' row filters and column lists belong to other consumers.
            assert_eq!(verify(name.clone(), "").await.unwrap(), clean);
            // Changes to a configured table block only that table.
            for (change, cause) in [
                (
                    format!("ALTER PUBLICATION {name} SET TABLE {name}.other"),
                    format!("missing: {name}.orders"),
                ),
                (
                    format!("ALTER PUBLICATION {name} SET TABLE {name}.orders WHERE (id > 0)"),
                    format!("not supported by initial COPY: {name}.orders"),
                ),
                (
                    format!("ALTER PUBLICATION {name} SET TABLE {name}.orders (id)"),
                    format!("every current source column: {name}.orders"),
                ),
            ] {
                let reasons = verify(name.clone(), &change).await.unwrap();
                assert!(
                    reasons.len() == 1 && reasons[0].ends_with(&cause),
                    "{reasons:?}"
                );
            }
            // Publication-scoped changes affect every table.
            for (change, cause) in [
                (
                    format!(
                        "ALTER PUBLICATION {name} SET TABLE {name}.orders, {name}.other;
                         ALTER PUBLICATION {name} SET (publish = 'insert, update, truncate')"
                    ),
                    "silently skipped; missing DELETE",
                ),
                (format!("DROP PUBLICATION {name}"), "publication not found"),
            ] {
                let message = verify(name.clone(), &change).await.unwrap_err();
                assert!(message.ends_with(cause), "{message}");
            }
            assert_eq!(
                verify(
                    name.clone(),
                    &format!("CREATE PUBLICATION {name} FOR TABLE {name}.orders"),
                )
                .await
                .unwrap(),
                clean
            );
        });
        let result = body.catch_unwind().await;
        sql.batch_execute(&format!(
            "DROP PUBLICATION IF EXISTS {name}, {name}_schema; DROP SCHEMA {name} CASCADE"
        ))
        .await
        .unwrap();
        drop(sql);
        sql_task.await.unwrap().unwrap();
        if let Err(panic) = result {
            std::panic::resume_unwind(panic);
        }
    }

    fn published(id: u32) -> PublishedTable {
        let columns: Vec<String> = vec!["id".into(), "status".into()];
        PublishedTable {
            relation_id: id,
            full_identity: true,
            published: columns.clone(),
            current: columns,
            ..Default::default()
        }
    }

    fn facts(version: i32, members: Vec<PublishedTable>) -> PublicationFacts {
        PublicationFacts {
            version,
            operations: Some([true; 4]),
            publish_generated: false,
            members,
        }
    }

    #[test]
    fn publication_contract_scopes_violations_and_ignores_unconfigured_members() {
        let config: Config = toml::from_str(include_str!("../../../examples/flow.toml")).unwrap();
        let orders = config.tables[0].schema(11);
        let mut items = orders.clone();
        items.table_id = TableId(12);
        let configured = [
            ("public.orders".to_string(), &orders),
            ("public.items".to_string(), &items),
        ];
        // Table-scoped violations as (table, reason); publication-scoped as Err.
        let check = |facts: &PublicationFacts| {
            check_publication(facts, &configured)
                .map(|violations| {
                    violations
                        .into_iter()
                        .map(|violation| (violation.table.0, violation.reason))
                        .collect::<Vec<_>>()
                })
                .map_err(|error| error.to_string())
        };
        let clean = |facts: &PublicationFacts| assert_eq!(check(facts).unwrap(), []);
        // FOR ALL TABLES or schema publications list other tables, whose row
        // filters and column lists belong to other consumers.
        let mut other = published(99);
        other.row_filter = true;
        other.published.pop();
        clean(&facts(170000, vec![published(11), other, published(12)]));
        // PostgreSQL 14 has neither row filters nor column lists.
        let bare = |relation_id| PublishedTable {
            relation_id,
            ..Default::default()
        };
        clean(&facts(140000, vec![bare(12), bare(11), bare(99)]));

        // A missing publication or unpublished operation affects every table.
        assert_eq!(
            check(&PublicationFacts::default()).unwrap_err(),
            "publication not found"
        );
        let mut operations = facts(170000, vec![published(11), published(12)]);
        operations.operations = Some([true, false, true, false]);
        assert!(
            check(&operations)
                .unwrap_err()
                .ends_with("silently skipped; missing UPDATE, TRUNCATE")
        );

        // Everything else concerns one configured table; others stay healthy.
        assert_eq!(
            check(&facts(170000, vec![published(11), published(99)])).unwrap(),
            [(
                12,
                "publication must include every configured source table; missing: public.items"
                    .into()
            )]
        );
        let mut filtered = published(12);
        filtered.row_filter = true;
        assert_eq!(
            check(&facts(170000, vec![published(11), filtered])).unwrap(),
            [(
                12,
                "publication row filters are not supported by initial COPY: public.items".into()
            )]
        );
        let mut listed = published(11);
        listed.published.pop();
        assert_eq!(
            check(&facts(170000, vec![listed, bare(99)])).unwrap(),
            [
                (
                    11,
                    "publication must include every current source column: public.orders".into()
                ),
                (
                    12,
                    "publication must include every configured source table; missing: public.items"
                        .into()
                ),
            ]
        );
        // PostgreSQL 15-17 list stored generated columns that pgoutput omits.
        let mut generated = published(11);
        generated.published.push("total".into());
        generated.generated = vec!["total".into()];
        clean(&facts(170000, vec![generated.clone(), published(12)]));
        let violations = check(&facts(180000, vec![generated, published(12)])).unwrap();
        assert_eq!(violations.len(), 1);
        assert!(
            violations[0]
                .1
                .starts_with("PostgreSQL 18 FULL replica identity requires")
        );
    }
}

#[cfg(test)]
#[path = "publication_tests.rs"]
mod publication_tests;
