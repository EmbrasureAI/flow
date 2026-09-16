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
    let url = std::env::var(&config.source.connection_env)
        .context("source connection environment variable is missing")?;
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
    let tls = postgres_native_tls::MakeTlsConnector::new(native_tls::TlsConnector::new()?);
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
                                if let SourceEvent::Truncate { xid, subxid, relations, cascade, restart_identity } = &event {
                                    for id in relations {
                                        let table = TableId(*id);
                                        registry.block(table)?;
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
                                                registry.block(id)?;
                                                SourceEvent::Relation(projector.quarantine_relation(relation))
                                            }
                                        }
                                    };
                                    let SourceEvent::Relation(projected_relation) = &projected else { unreachable!() };
                                    if !registry.is_blocked(id)
                                        && let Err(error) = source_deadline(registry.observe_relation(&sql, projected_relation, &mut assembler)).await {
                                            if retryable_connection(&error) { break; }
                                            if !crate::schema::table_schema_error(&error) { return Err(error); }
                                            registry.block(id)?;
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
                                            | flow_pg_source::Error::ReplicaIdentity(_) | flow_pg_source::Error::UnchangedToast(_)) {
                                            registry.block(id)?;
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

async fn validate_publication_membership(
    client: &Client,
    config: &Config,
    schemas: &[TableSchema],
) -> Result<()> {
    let version: i32 = client
        .query_one("SELECT current_setting('server_version_num')::integer", &[])
        .await?
        .get(0);
    ensure!(
        (140000..190000).contains(&version),
        "supported PostgreSQL versions are 14 through 18"
    );
    let publication=client.query_opt("SELECT pubinsert, pubupdate, pubdelete, pubtruncate FROM pg_catalog.pg_publication WHERE pubname=$1",&[&config.source.publication]).await?.context("publication not found")?;
    ensure!(
        publication.get::<_, bool>(0)
            && publication.get::<_, bool>(1)
            && publication.get::<_, bool>(2)
            && publication.get::<_, bool>(3),
        "publication must include INSERT, UPDATE, DELETE, and TRUNCATE so unsupported operations cannot be silently skipped"
    );
    let mut found = HashSet::new();
    for row in client.query("SELECT c.oid FROM pg_catalog.pg_publication_tables p JOIN pg_catalog.pg_namespace n ON p.schemaname=n.nspname JOIN pg_catalog.pg_class c ON c.relnamespace=n.oid AND c.relname=p.tablename WHERE p.pubname=$1",&[&config.source.publication]).await? {found.insert(row.get::<_,u32>(0));}
    ensure!(
        found == schemas.iter().map(|s| s.table_id.0).collect(),
        "publication must contain exactly the configured source tables"
    );
    if version >= 150000 {
        let filters:bool=client.query_one("SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_publication_tables WHERE pubname=$1 AND rowfilter IS NOT NULL)",&[&config.source.publication]).await?.get(0);
        ensure!(
            !filters,
            "publication row filters are not supported by initial COPY"
        );
        let publish_generated = if version >= 180000 {
            client
                .query_one(
                    "SELECT pubgencols::text = 's' FROM pg_catalog.pg_publication WHERE pubname=$1",
                    &[&config.source.publication],
                )
                .await?
                .get::<_, bool>(0)
        } else {
            false
        };
        if version >= 180000 && !publish_generated {
            let generated_identity: bool = client.query_one(
                "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_publication_tables p JOIN pg_catalog.pg_namespace n ON n.nspname=p.schemaname JOIN pg_catalog.pg_class c ON c.relnamespace=n.oid AND c.relname=p.tablename JOIN pg_catalog.pg_attribute a ON a.attrelid=c.oid WHERE p.pubname=$1 AND c.relreplident='f' AND a.attnum>0 AND NOT a.attisdropped AND a.attgenerated='s')",
                &[&config.source.publication]).await?.get(0);
            ensure!(
                !generated_identity,
                "PostgreSQL 18 FULL replica identity requires publish_generated_columns=stored even when generated columns are excluded"
            );
        }
        for row in client
            .query(
                "SELECT p.attnames, ARRAY(SELECT a.attname FROM pg_catalog.pg_attribute a
             WHERE a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped AND (a.attgenerated = '' OR ($2 AND a.attgenerated = 's')) ORDER BY a.attnum),
             ARRAY(SELECT a.attname FROM pg_catalog.pg_attribute a
             WHERE a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped AND a.attgenerated = 's'), c.oid
             FROM pg_catalog.pg_publication_tables p
             JOIN pg_catalog.pg_namespace n ON p.schemaname=n.nspname
             JOIN pg_catalog.pg_class c ON c.relnamespace=n.oid AND c.relname=p.tablename
             WHERE p.pubname=$1",
                &[&config.source.publication, &publish_generated],
            )
            .await?
        {
            let mut published: Vec<String> = row.get(0);
            let full: Vec<String> = row.get(1);
            let generated: Vec<String> = row.get(2);
            if version < 180000 {
                // PG15's catalog view includes generated columns even though
                // pgoutput omits them; newer views omit them too. Normalize
                // only these known non-published fields, never ordinary ones.
                published.retain(|name| !generated.contains(name));
            }
            ensure!(
                published == full,
                "publication must include every current source column"
            );
            let relation_id: u32 = row.get(3);
            let schema = schemas
                .iter()
                .find(|schema| schema.table_id.0 == relation_id)
                .context("publication contains an unconfigured relation")?;
            // Current ordinary columns are covered by the full-publication
            // check above. Missing historical names belong to the schema
            // registry's durable table block, so they must not stop healthy
            // tables on restart. Still reject selected generated fields that
            // COPY would read but pgoutput would omit.
            ensure!(
                schema.columns.iter().all(|column| {
                    !generated.contains(&column.name) || published.contains(&column.name)
                }),
                "publication omits a configured source column; snapshot and CDC must use the same columns"
            );
        }
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
}
