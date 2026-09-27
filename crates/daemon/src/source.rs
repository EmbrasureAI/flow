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
    collections::HashSet,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::watch;

// Bound added low-load latency and the amount of terminal metadata held for one
// durability barrier. An individual larger transaction still remains atomic.
const JOURNAL_GROUP_TRANSACTIONS: usize = 32;
const JOURNAL_GROUP_BYTES: u64 = 4 << 20;
const JOURNAL_GROUP_DELAY: Duration = Duration::from_millis(5);
const SOURCE_SQL_TIMEOUT: Duration = Duration::from_secs(30);

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
    let url = std::env::var(&config.source.connection_env).with_context(|| {
        format!(
            "source connection environment variable is missing: {}",
            config.source.connection_env
        )
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
    let connection = ConnectionTask(Some(tokio::spawn(async move {
        if let Err(error) = connection.await {
            tracing::warn!(%error, "PostgreSQL connection stopped");
        }
    })));
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
                validate_slot(&sql, &config, journal.durable_lsn()).await?;
                let effective_schemas = registry.initialize(&sql, &config.tables).await?;
                validate_publication_membership(&sql, &config, &effective_schemas).await?;
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
            let mut wire_relations = std::collections::HashMap::new();
            let mut feedback_tick = tokio::time::interval(Duration::from_secs(5));
            feedback_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let mut heartbeat_tick = tokio::time::interval(Duration::from_secs(30));
            heartbeat_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            delay = Duration::from_millis(250);
            let mut group_deadline = None;
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
                            return Err(error.context("Postgres CDC requires EXECUTE on pg_logical_emit_message for idle WAL progress"));
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
                                // The publication may contain other tables. Their metadata
                                // and changes never reach schema selection, quarantine or the
                                // spool; the enclosing commit proceeds like an empty one.
                                let Some(event) = event.retain_relations(|id| assembler.is_captured(TableId(id))) else {
                                    continue;
                                };
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
                                if let Err(error) = assembler.push_buffered_at(event, source.received_lsn, &mut journal) {
                                    if let Some(id) = row_table
                                        && matches!(&error, flow_pg_source::Error::Row(_) | flow_pg_source::Error::Value(_)
                                            | flow_pg_source::Error::ReplicaIdentity(_) | flow_pg_source::Error::DefaultIdentity(_) | flow_pg_source::Error::UnchangedToast(_)) {
                                            registry.block(id, crate::schema::schema_block_reason(&error.into()))?;
                                            registry.block_decoder(id, &mut assembler)?;
                                            assembler.quarantine(retained.expect("row retained"), wire_relations.get(&id.0).context("row before source relation")?)?;
                                            continue;
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
        send.send_replace(CaptureProgress {
            durable_lsn: journal.durable_lsn(),
            error: Some(format!("{error:#}")),
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
    let journaled_at_micros = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64;
    for transaction in transactions {
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
        tracing::info!(
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
    send.send_replace(CaptureProgress {
        durable_lsn: journal.durable_lsn(),
        error: None,
    });
    Ok(())
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

async fn verify_source_identity(client: &Client, config: &Config, initialize: bool) -> Result<()> {
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
            ensure!(identity == expected, "PostgreSQL source system, database, timeline, or slot lineage changed; coordinated failover/resynchronization is required");
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
        .context("replication slot disappeared; source resynchronization is required")?;
    ensure!(
        row.get::<_, Option<String>>(0).as_deref() == Some("pgoutput")
            && row.get::<_, String>(1) == "logical"
            && row.get::<_, Option<bool>>(2) == Some(true),
        "replication slot plugin/type/database differs from the initialized source"
    );
    ensure!(
        row.get::<_, Option<String>>(5).as_deref() != Some("lost")
            && row.get::<_, Option<String>>(4).is_some(),
        "replication slot lost required WAL; source resynchronization is required"
    );
    let confirmed: PgLsn = row
        .get::<_, Option<String>>(3)
        .context("replication slot has no confirmed LSN")?
        .parse()?;
    ensure!(
        confirmed <= durable_lsn,
        "PostgreSQL slot has acknowledged beyond the local durable journal; another consumer or storage loss requires source reconciliation"
    );
    Ok(())
}

pub(crate) async fn validate_publication(
    client: &Client,
    config: &Config,
    schemas: &[TableSchema],
    initialize: bool,
) -> Result<()> {
    validate_publication_membership(client, config, schemas).await?;
    let replication = connect(config, true).await?;
    verify_source_identity(&replication, config, initialize).await
}

/// A definite publication contract violation, as opposed to a failed or
/// interrupted catalog query. pgoutput may already have omitted changes for a
/// configured table, so neither reconnecting nor restoring the setting repairs it.
#[derive(Debug)]
pub(crate) struct PublicationViolation(pub(crate) String);

impl std::fmt::Display for PublicationViolation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for PublicationViolation {}

/// Publication catalog facts for the configured tables only. Other members are
/// never read: administrators may publish additional tables for other consumers.
#[derive(Debug, Default)]
struct PublicationFacts {
    server_version: i32,
    /// INSERT, UPDATE, DELETE and TRUNCATE flags; `None` if the publication is missing.
    operations: Option<[bool; 4]>,
    publish_generated: bool,
    members: Vec<PublishedTable>,
}

/// A configured table found in the publication. Row filters and column lists
/// exist only on PostgreSQL 15 and later; older servers report membership only.
#[derive(Debug, Default)]
struct PublishedTable {
    relation_id: u32,
    row_filter: bool,
    full_replica_identity: bool,
    published_columns: Vec<String>,
    current_columns: Vec<String>,
    generated_columns: Vec<String>,
}

/// Startup, each capture reconnect and the running health check share this
/// validation of the configured tables' publication contract.
pub(crate) async fn validate_publication_membership(
    client: &Client,
    config: &Config,
    schemas: &[TableSchema],
) -> Result<()> {
    ensure!(
        schemas.len() == config.tables.len(),
        "schema/config table count differs"
    );
    let relations = schemas
        .iter()
        .map(|schema| schema.table_id.0)
        .collect::<Vec<_>>();
    let facts = publication_facts(client, &config.source.publication, &relations).await?;
    check_publication(&config.tables, schemas, &facts)
}

async fn publication_facts(
    client: &Client,
    publication: &str,
    relations: &[u32],
) -> Result<PublicationFacts> {
    let server_version: i32 = client
        .query_one("SELECT current_setting('server_version_num')::integer", &[])
        .await?
        .get(0);
    let mut facts = PublicationFacts {
        server_version,
        ..Default::default()
    };
    if !(140000..190000).contains(&server_version) {
        return Ok(facts);
    }
    let publish_generated = if server_version >= 180000 {
        "pubgencols::text = 's'"
    } else {
        "false"
    };
    let Some(row) = client
        .query_opt(
            &format!("SELECT pubinsert, pubupdate, pubdelete, pubtruncate, {publish_generated} FROM pg_catalog.pg_publication WHERE pubname=$1"),
            &[&publication],
        )
        .await?
    else {
        return Ok(facts);
    };
    facts.operations = Some([row.get(0), row.get(1), row.get(2), row.get(3)]);
    facts.publish_generated = row.get(4);
    // Resolve names to the configured relation identities, so a replaced
    // table is not mistaken for its predecessor.
    const MEMBERS: &str = "FROM pg_catalog.pg_publication_tables p
         JOIN pg_catalog.pg_namespace n ON p.schemaname=n.nspname
         JOIN pg_catalog.pg_class c ON c.relnamespace=n.oid AND c.relname=p.tablename
         WHERE p.pubname=$1 AND c.oid = ANY($2)";
    if server_version < 150000 {
        for row in client
            .query(
                &format!("SELECT c.oid {MEMBERS}"),
                &[&publication, &relations],
            )
            .await?
        {
            facts.members.push(PublishedTable {
                relation_id: row.get(0),
                ..Default::default()
            });
        }
        return Ok(facts);
    }
    for row in client
        .query(
            &format!(
                "SELECT c.oid, p.rowfilter IS NOT NULL, c.relreplident = 'f', p.attnames,
             ARRAY(SELECT a.attname FROM pg_catalog.pg_attribute a
             WHERE a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped AND (a.attgenerated = '' OR ($3 AND a.attgenerated = 's')) ORDER BY a.attnum),
             ARRAY(SELECT a.attname FROM pg_catalog.pg_attribute a
             WHERE a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped AND a.attgenerated = 's')
             {MEMBERS}"
            ),
            &[&publication, &relations, &facts.publish_generated],
        )
        .await?
    {
        facts.members.push(PublishedTable {
            relation_id: row.get(0),
            row_filter: row.get(1),
            full_replica_identity: row.get(2),
            published_columns: row.get(3),
            current_columns: row.get(4),
            generated_columns: row.get(5),
        });
    }
    Ok(facts)
}

/// The publication must contain every configured table and may contain others.
/// Only configured tables' filters, columns and identities are considered.
fn check_publication(
    tables: &[Table],
    schemas: &[TableSchema],
    facts: &PublicationFacts,
) -> Result<()> {
    ensure!(
        (140000..190000).contains(&facts.server_version),
        "supported PostgreSQL versions are 14 through 18"
    );
    let operations = facts
        .operations
        .ok_or_else(|| PublicationViolation("publication not found".into()))?;
    ensure!(
        operations.iter().all(|published| *published),
        PublicationViolation(
            "publication must include INSERT, UPDATE, DELETE, and TRUNCATE so unsupported operations cannot be silently skipped".into()
        )
    );
    let mut members = Vec::with_capacity(schemas.len());
    let mut missing = Vec::new();
    for (table, schema) in tables.iter().zip(schemas) {
        let name = format!("{}.{}", table.source_namespace, table.source_table);
        match facts
            .members
            .iter()
            .find(|member| member.relation_id == schema.table_id.0)
        {
            Some(member) => members.push((name, schema, member)),
            None => missing.push(name),
        }
    }
    ensure!(
        missing.is_empty(),
        PublicationViolation(format!(
            "publication must contain every configured source table; missing {}",
            missing.join(", ")
        ))
    );
    if facts.server_version < 150000 {
        return Ok(());
    }
    for (name, _, member) in &members {
        ensure!(
            !member.row_filter,
            PublicationViolation(format!(
                "publication row filters are not supported by initial COPY ({name})"
            ))
        );
    }
    if facts.server_version >= 180000 && !facts.publish_generated {
        for (name, _, member) in &members {
            ensure!(
                !(member.full_replica_identity && !member.generated_columns.is_empty()),
                PublicationViolation(format!(
                    "PostgreSQL 18 FULL replica identity requires publish_generated_columns=stored even when generated columns are excluded ({name})"
                ))
            );
        }
    }
    for (name, schema, member) in &members {
        // PG15's catalog view includes generated columns even though pgoutput
        // omits them; newer views omit them too. Normalize only these known
        // non-published fields, never ordinary ones.
        let published = member
            .published_columns
            .iter()
            .filter(|column| {
                facts.server_version >= 180000 || !member.generated_columns.contains(column)
            })
            .collect::<Vec<_>>();
        ensure!(
            published.iter().copied().eq(&member.current_columns),
            PublicationViolation(format!(
                "publication must include every current source column ({name})"
            ))
        );
        // Current ordinary columns are covered by the full-publication
        // check above. Missing historical names belong to the schema
        // registry's durable table block, so they must not stop healthy
        // tables on restart. Still reject selected generated fields that
        // COPY would read but pgoutput would omit.
        ensure!(
            schema.columns.iter().all(|column| {
                !member.generated_columns.contains(&column.name)
                    || published.contains(&&column.name)
            }),
            PublicationViolation(format!(
                "publication omits a configured source column; snapshot and CDC must use the same columns ({name})"
            ))
        );
    }
    Ok(())
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

    const ORDERS: u32 = 11;
    const CUSTOMERS: u32 = 12;
    const OTHER: u32 = 99;

    fn configured() -> (Vec<Table>, Vec<TableSchema>) {
        let config: Config = toml::from_str(include_str!("../../../examples/flow.toml")).unwrap();
        let orders = config.tables[0].clone();
        let mut customers = orders.clone();
        customers.source_table = "customers".into();
        let schema = |id| TableSchema {
            table_id: TableId(id),
            version: 1,
            columns: ["id", "status"]
                .into_iter()
                .zip(1..)
                .map(|(name, field_id)| flow_model::Column {
                    field_id,
                    name: name.into(),
                    data_type: flow_model::ColumnType::String,
                    nullable: false,
                })
                .collect(),
            primary_key: vec![0],
            append_only: false,
        };
        (
            vec![orders, customers],
            vec![schema(ORDERS), schema(CUSTOMERS)],
        )
    }

    fn member(relation_id: u32, published: &[&str]) -> PublishedTable {
        PublishedTable {
            relation_id,
            published_columns: published.iter().map(|name| (*name).into()).collect(),
            current_columns: vec!["id".into(), "status".into()],
            ..Default::default()
        }
    }

    fn facts(server_version: i32, members: Vec<PublishedTable>) -> PublicationFacts {
        PublicationFacts {
            server_version,
            operations: Some([true; 4]),
            publish_generated: false,
            members,
        }
    }

    fn check(facts: &PublicationFacts) -> Result<()> {
        let (tables, schemas) = configured();
        check_publication(&tables, &schemas, facts)
    }

    /// A definite contract violation, which the running check reports rather
    /// than retrying.
    fn violation(facts: &PublicationFacts) -> String {
        let error = check(facts).unwrap_err();
        assert!(error.is::<PublicationViolation>(), "{error:#}");
        error.to_string()
    }

    #[test]
    fn publication_may_contain_other_tables_but_must_contain_every_configured_one() {
        let full = ["id", "status"];
        for version in [140000, 150000, 170000, 180000] {
            // Other members, even ones that violate the configured-table
            // contract, belong to other consumers.
            let mut other = member(OTHER, &["id"]);
            other.row_filter = true;
            other.full_replica_identity = true;
            other.generated_columns = vec!["total".into()];
            check(&facts(
                version,
                vec![member(CUSTOMERS, &full), other, member(ORDERS, &full)],
            ))
            .unwrap();

            let missing = violation(&facts(version, vec![member(ORDERS, &full)]));
            assert!(
                missing.contains("missing public.customers") && !missing.contains("orders"),
                "{missing}"
            );
            let missing = violation(&facts(version, vec![member(OTHER, &full)]));
            assert!(
                missing.contains("missing public.orders, public.customers"),
                "{missing}"
            );
        }
        // PostgreSQL 14 has no row filters or column lists, only membership.
        check(&facts(
            140000,
            vec![
                PublishedTable {
                    relation_id: ORDERS,
                    ..Default::default()
                },
                PublishedTable {
                    relation_id: CUSTOMERS,
                    ..Default::default()
                },
            ],
        ))
        .unwrap();

        let mut dropped = facts(170000, vec![]);
        dropped.operations = None;
        assert_eq!(violation(&dropped), "publication not found");
        for operation in 0..4 {
            let mut partial = facts(
                170000,
                vec![member(ORDERS, &full), member(CUSTOMERS, &full)],
            );
            partial.operations.as_mut().unwrap()[operation] = false;
            assert!(violation(&partial).contains("INSERT, UPDATE, DELETE, and TRUNCATE"));
        }
        // An unsupported server is a configuration error, not a contract change.
        let error = check(&facts(130000, vec![])).unwrap_err();
        assert!(!error.is::<PublicationViolation>(), "{error:#}");
    }

    #[test]
    fn configured_table_contract_violations_name_the_table() {
        let full = ["id", "status"];
        let mut filtered = member(CUSTOMERS, &full);
        filtered.row_filter = true;
        assert_eq!(
            violation(&facts(170000, vec![member(ORDERS, &full), filtered])),
            "publication row filters are not supported by initial COPY (public.customers)"
        );
        assert_eq!(
            violation(&facts(
                170000,
                vec![member(ORDERS, &["id"]), member(CUSTOMERS, &full)]
            )),
            "publication must include every current source column (public.orders)"
        );

        // PostgreSQL 15 lists generated columns that pgoutput omits.
        let generated = |published: &[&str]| {
            let mut orders = member(ORDERS, published);
            orders.generated_columns = vec!["total".into()];
            orders
        };
        check(&facts(
            150000,
            vec![
                generated(&["id", "status", "total"]),
                member(CUSTOMERS, &full),
            ],
        ))
        .unwrap();
        let (tables, mut schemas) = configured();
        let mut total = schemas[0].columns[1].clone();
        total.field_id = 3;
        total.name = "total".into();
        schemas[0].columns.push(total);
        let error = check_publication(
            &tables,
            &schemas,
            &facts(
                150000,
                vec![
                    generated(&["id", "status", "total"]),
                    member(CUSTOMERS, &full),
                ],
            ),
        )
        .unwrap_err();
        assert!(error.is::<PublicationViolation>());
        assert!(error.to_string().ends_with("(public.orders)"), "{error}");

        // PostgreSQL 18 FULL identity logs generated columns unless published.
        let mut identity = generated(&full);
        identity.full_replica_identity = true;
        assert!(
            violation(&facts(180000, vec![identity, member(CUSTOMERS, &full)]))
                .ends_with("excluded (public.orders)")
        );
        let mut identity = generated(&["id", "status", "total"]);
        identity.full_replica_identity = true;
        identity.current_columns.push("total".into());
        let mut published = facts(180000, vec![identity, member(CUSTOMERS, &full)]);
        published.publish_generated = true;
        check(&published).unwrap();
    }
}
