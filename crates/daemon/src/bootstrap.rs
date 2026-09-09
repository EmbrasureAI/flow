//! Concurrent initial COPY, durable per-table staging, and restartable handoff.
use crate::{
    config::Config,
    services::{catalog, journal_config, ledger, state, target},
    source::{CaptureProgress, capture_loop, connect, validate_publication, validate_source_table},
};
use anyhow::{Context, Result, ensure};
use flow_coordinator::{CollapseLimits, Epoch, SourceLedger, TablePublisher, collapse_epoch};
use flow_ingress_journal::{Journal, JournalReader};
use flow_materializer::WriterConfig;
use flow_model::{
    Mutation, MutationKind, PgLsn, SourceId, SourceTransaction, TableId, TableMutationCount,
    TableSchema, TableSchemaVersion,
};
use flow_pg_source::{
    Acknowledgement, SnapshotSession, decode_copy_row_with_types, export_snapshot,
    export_temporary_snapshot, fetch_relation,
    tokio_postgres::{binary_copy::BinaryCopyOutStream, types::Type},
};
use flow_state_store::{ControlStore, OperationKind, OperationPhase, StateStore};
use futures::{StreamExt, TryStreamExt};
use iceberg::{Catalog, TableCreation, table::Table};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use tokio::sync::watch;

pub(crate) const BOOTSTRAP: &[u8] = b"flow-daemon/bootstrap/v1";
#[derive(Serialize, Deserialize)]
pub(crate) struct Bootstrap {
    pub(crate) source_id: String,
    pub(crate) slot: String,
    pub(crate) publication: String,
    pub(crate) schemas: Vec<TableSchema>,
    pub(crate) targets: Vec<(Vec<String>, String)>,
    #[serde(default)]
    pub(crate) target_uuids: Vec<uuid::Uuid>,
    pub(crate) consistent_lsn: Option<PgLsn>,
    pub(crate) copied: bool,
    #[serde(default)]
    pub(crate) layout: u8,
}
pub(crate) fn persist_bootstrap(store: &StateStore, bootstrap: &Bootstrap) -> Result<()> {
    store.put_source_transaction(BOOTSTRAP, &serde_json::to_vec(bootstrap)?)?;
    Ok(())
}
pub(crate) fn bootstrap(store: &ControlStore) -> Result<Bootstrap> {
    Ok(serde_json::from_slice(
        &store
            .source_transaction(BOOTSTRAP)?
            .context("source is not initialized; run init first")?,
    )?)
}
pub(crate) async fn tables(
    catalog: &dyn Catalog,
    bootstrap: &Bootstrap,
) -> Result<BTreeMap<TableId, Table>> {
    let mut tables = BTreeMap::new();
    for (schema, (namespace, name)) in bootstrap.schemas.iter().zip(&bootstrap.targets) {
        tables.insert(
            schema.table_id,
            catalog.load_table(&target(namespace, name)?).await?,
        );
    }
    Ok(tables)
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
enum CopyPhase {
    Copying,
    Staged,
    Published,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TableCopy {
    cut: PgLsn,
    /// Empty only for migration from the original shared initial-copy journal.
    staging: String,
    phase: CopyPhase,
    rows: u64,
    #[serde(default)]
    schema_version: u32,
}
impl TableCopy {
    fn key(table: TableId) -> Vec<u8> {
        let mut key = b"flow-daemon/bootstrap-table/v1/".to_vec();
        key.extend_from_slice(&table.0.to_be_bytes());
        key
    }
    fn load(store: &StateStore, table: TableId) -> Result<Option<Self>> {
        store
            .source_transaction(&Self::key(table))?
            .map(|bytes| Ok(serde_json::from_slice(&bytes)?))
            .transpose()
    }
    fn save(&self, store: &StateStore, table: TableId) -> Result<()> {
        store.put_source_transaction(&Self::key(table), &serde_json::to_vec(self)?)?;
        Ok(())
    }
    fn path(&self, config: &Config, table: TableId) -> Result<std::path::PathBuf> {
        let prefix = format!("table-{}-", table.0);
        let identifier = self
            .staging
            .strip_prefix(&prefix)
            .context("bootstrap staging directory belongs to another table")?;
        let identifier = uuid::Uuid::parse_str(identifier)
            .context("invalid bootstrap staging directory identifier")?;
        ensure!(
            self.staging == format!("{prefix}{identifier}"),
            "invalid bootstrap staging directory"
        );
        Ok(config.state_dir.join("bootstrap").join(&self.staging))
    }
}

fn remove_staging(config: &Config, table: TableId, copy: &TableCopy) -> Result<()> {
    if copy.staging.is_empty() {
        return Ok(());
    }
    let path = copy.path(config, table)?;
    match std::fs::remove_dir_all(&path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(error).with_context(|| format!("remove {}", path.display()));
        }
    }
    std::fs::File::open(config.state_dir.join("bootstrap"))?
        .sync_all()
        .context("sync bootstrap staging deletion")
}

fn recover_published_copy(
    config: &Config,
    store: &StateStore,
    ledger: &mut SourceLedger,
    table: TableId,
    copy: &TableCopy,
    ledger_cut: PgLsn,
    snapshot: i64,
) -> Result<()> {
    ensure!(
        copy.phase == CopyPhase::Published,
        "cannot recover an unpublished table copy"
    );
    copy.save(store, table)?;
    if ledger.watermarks().materialized_lsn < ledger_cut {
        ledger.table_materialized(ledger_cut, table, snapshot)?;
    }
    remove_staging(config, table, copy)
}

/// Creates the permanent source incarnation once; subsequent calls resume its
/// per-table copies without resetting that slot or its durable CDC journal.
pub async fn initialize(config: Config) -> Result<()> {
    // Storage metric handles bind to the recorder on first FileIO access.
    let observation = crate::observation::Observation::install()?;
    let started = std::time::Instant::now();
    let result = async {
        let store = state(&config)?;
        let catalog = catalog(&config).await?;
        let mut boot = match store.source_transaction(BOOTSTRAP)? {
            Some(bytes) => serde_json::from_slice(&bytes)?,
            None => prepare_source(&config, &store, catalog.as_ref()).await?,
        };
        validate_config(&config, &boot)?;
        resume(&config, store, catalog, &mut boot).await
    }
    .await;
    let outcome = if result.is_ok() { "success" } else { "error" };
    metrics::counter!("flow_bootstrap_runs_total", "outcome" => outcome).increment(1);
    metrics::histogram!("flow_bootstrap_seconds", "outcome" => outcome)
        .record(started.elapsed().as_secs_f64());
    if let Err(error) = observation.flush(&config) {
        // Diagnostics cannot turn a successful copy into a failed one, or hide
        // the primary cause when initialization itself failed.
        tracing::warn!(%error, "failed to flush bootstrap diagnostics");
    }
    result
}

pub(crate) fn validate_config(config: &Config, boot: &Bootstrap) -> Result<()> {
    ensure!(
        boot.source_id == config.source.id
            && boot.slot == config.source.slot
            && boot.publication == config.source.publication,
        "source incarnation differs from durable bootstrap"
    );
    ensure!(
        boot.schemas.len() == config.tables.len() && boot.targets.len() == config.tables.len(),
        "configured source tables differ from durable bootstrap"
    );
    for (index, configured) in config.tables.iter().enumerate() {
        ensure!(
            configured.schema(boot.schemas[index].table_id.0) == boot.schemas[index]
                && (
                    configured.target_namespace.clone(),
                    configured.target_table.clone()
                ) == boot.targets[index],
            "configured schema or target differs from durable bootstrap"
        );
    }
    Ok(())
}

async fn prepare_source(
    config: &Config,
    store: &StateStore,
    catalog: &dyn Catalog,
) -> Result<Bootstrap> {
    let sql = connect(config, false).await?;
    ensure!(
        slot_cut(&sql, &config.source.slot).await?.is_none(),
        "initialization requires a new permanent replication slot"
    );
    let mut schemas = Vec::new();
    let mut target_uuids = Vec::new();
    for configured in &config.tables {
        let (relation, _) = fetch_relation(
            &sql,
            &configured.source_namespace,
            &configured.source_table,
            !configured.append_only,
        )
        .await?;
        let schema = configured.schema(relation.id);
        validate_source_table(&sql, configured, &schema).await?;
        let id = target(&configured.target_namespace, &configured.target_table)?;
        if !catalog.namespace_exists(&id.namespace).await? {
            catalog
                .create_namespace(&id.namespace, HashMap::new())
                .await?;
        }
        if !catalog.table_exists(&id).await? {
            let created = catalog
                .create_table(
                    &id.namespace,
                    TableCreation::builder()
                        .name(id.name.clone())
                        .format_version(configured.format_version)
                        .schema(flow_materializer::iceberg_schema(&schema)?)
                        .build(),
                )
                .await?;
            ensure!(
                created.metadata().format_version() == configured.format_version,
                "catalog created target with format {}, requested {}",
                created.metadata().format_version(),
                configured.format_version,
            );
        }
        let existing = catalog.load_table(&id).await?;
        ensure!(
            existing.metadata().current_snapshot_id().is_none()
                && *existing.metadata().current_schema().as_ref()
                    == flow_materializer::iceberg_schema(&schema)?,
            "initial target must be empty and match the configured schema"
        );
        target_uuids.push(existing.metadata().uuid());
        // Even an empty, not-yet-copied table has explicit recovery authority.
        store.complete_noop(&schema.table_id, PgLsn(0), schema.version)?;
        schemas.push(schema);
    }
    validate_publication(&sql, config, &schemas, true).await?;
    let boot = Bootstrap {
        source_id: config.source.id.clone(),
        slot: config.source.slot.clone(),
        publication: config.source.publication.clone(),
        schemas,
        targets: config
            .tables
            .iter()
            .map(|table| (table.target_namespace.clone(), table.target_table.clone()))
            .collect(),
        target_uuids,
        consistent_lsn: None,
        copied: false,
        layout: 1,
    };
    // This records slot-creation intent before CREATE_REPLICATION_SLOT. If its
    // response is lost, the same configured slot can be recovered without reset.
    persist_bootstrap(store, &boot)?;
    Ok(boot)
}

async fn slot_cut(
    sql: &flow_pg_source::tokio_postgres::Client,
    slot: &str,
) -> Result<Option<PgLsn>> {
    let Some(row) = sql.query_opt("SELECT confirmed_flush_lsn::text, plugin, slot_type, database = current_database(), temporary, active FROM pg_catalog.pg_replication_slots WHERE slot_name = $1", &[&slot]).await? else { return Ok(None); };
    ensure!(
        row.get::<_, Option<String>>(1).as_deref() == Some("pgoutput")
            && row.get::<_, String>(2) == "logical"
            && row.get::<_, Option<bool>>(3) == Some(true)
            && !row.get::<_, bool>(4)
            && !row.get::<_, bool>(5),
        "bootstrap slot is active or differs from its recorded source contract"
    );
    Ok(Some(
        row.get::<_, Option<String>>(0)
            .context("bootstrap slot has no consistent point")?
            .parse()?,
    ))
}

fn envelope(
    config: &Config,
    schemas: &[TableSchema],
    cut: PgLsn,
    chunks: flow_model::JournalChunks,
    rows: u64,
) -> Result<SourceTransaction> {
    ensure!(cut.0 > 0, "invalid snapshot cut");
    ensure!(
        schemas.len() == 1 || rows == 0,
        "COPY row counts require one table"
    );
    let mut counts = schemas
        .iter()
        .map(|schema| TableMutationCount {
            table_id: schema.table_id,
            mutations: rows,
        })
        .collect::<Vec<_>>();
    counts.sort_unstable_by_key(|count| count.table_id);
    Ok(SourceTransaction {
        source_id: SourceId(config.source.id.clone()),
        xid: 0,
        begin_lsn: PgLsn(cut.0 - 1),
        commit_lsn: PgLsn(cut.0 - 1),
        end_lsn: cut,
        commit_timestamp_micros: i64::try_from(
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_micros(),
        )?,
        schema_versions: schemas
            .iter()
            .map(|schema| TableSchemaVersion {
                table_id: schema.table_id,
                version: schema.version,
            })
            .collect(),
        affected_tables: schemas.iter().map(|schema| schema.table_id).collect(),
        mutation_chunks: chunks,
        table_mutation_counts: Some(counts),
    })
}

/// Runs capture while four bounded COPY/publication workers initialize tables.
/// A lost SQL snapshot causes only unfinished tables to take a newer temporary
/// snapshot cut. Their watermarks then suppress CDC already represented by COPY.
pub(crate) async fn resume(
    config: &Config,
    store: StateStore,
    catalog: Arc<dyn Catalog>,
    boot: &mut Bootstrap,
) -> Result<()> {
    if boot.copied && boot.layout == 1 {
        return Ok(());
    }
    validate_config(config, boot)?;
    let sql = connect(config, false).await?;
    let mut registry = crate::schema::SchemaRegistry::new(
        store.clone(),
        SourceId(config.source.id.clone()),
        &boot.schemas,
    )?;
    let current_schemas = registry.initialize(&sql, &config.tables).await?;
    validate_publication(&sql, config, &current_schemas, false).await?;
    let targets = tables(catalog.as_ref(), boot).await?;
    ensure!(
        boot.target_uuids.len() == boot.schemas.len(),
        "bootstrap requires durable target identities"
    );
    for (schema, uuid) in boot.schemas.iter().zip(&boot.target_uuids) {
        ensure!(
            targets[&schema.table_id].metadata().uuid() == *uuid,
            "initial target incarnation changed"
        );
    }
    let publisher = Arc::new(TablePublisher::new(
        store.clone(),
        catalog.clone(),
        WriterConfig {
            target_file_bytes: usize::try_from(config.compaction.l1_target_bytes)
                .context("initial target file size exceeds platform limit")?,
            ..crate::services::writer_config(config)
        },
        config.limits.batch_rows,
        config.limits.batch_bytes,
    )?);
    for operation in store.pending_operations()? {
        ensure!(
            operation.operation.kind == OperationKind::Ingest,
            "unexpected maintenance operation during bootstrap"
        );
        if operation.phase == OperationPhase::Building {
            store.discard_uncommitted(&operation.operation.id)?;
        } else {
            publisher
                .recover(
                    &targets[&operation.operation.table_id],
                    &operation.operation.id,
                )
                .await?;
        }
    }

    let replication = connect(config, true).await?;
    let mut keeper_sql = connect(config, false).await?;
    let mut retry_keeper_sql;
    let mut keeper = None;
    if boot.consistent_lsn.is_none() {
        let cut = if let Some(cut) = slot_cut(&sql, &config.source.slot).await? {
            cut
        } else {
            let snapshot =
                export_snapshot(&replication, &mut keeper_sql, &config.source.slot).await?;
            let cut = snapshot.consistent_lsn;
            keeper = Some(snapshot);
            cut
        };
        boot.consistent_lsn = Some(cut);
        persist_bootstrap(&store, boot)?;
    }
    let initial_cut = boot
        .consistent_lsn
        .context("missing initial snapshot cut")?;
    let (mut journal, _) = Journal::open(config.state_dir.join("journal"), journal_config(config))?;
    if journal.durable_lsn() < initial_cut {
        ensure!(
            journal.durable_lsn() == PgLsn(0),
            "journal precedes the recorded bootstrap cut"
        );
        journal.commit(envelope(
            config,
            &boot.schemas,
            initial_cut,
            journal.transaction_chunks(0),
            0,
        )?)?;
    }
    let reader = journal.reader();
    let initial = reader
        .transactions_after(PgLsn(initial_cut.0 - 1))?
        .next()
        .transpose()?
        .filter(|transaction| transaction.xid == 0 && transaction.end_lsn == initial_cut);
    let mut ledger = ledger(&store, config)?;
    ensure!(
        journal.durable_lsn() >= ledger.watermarks().journal_durable_lsn,
        "bootstrap journal lost previously durable transactions; recover storage before acknowledging"
    );
    register_captured(&reader, &mut ledger, usize::MAX)?;
    let mut plans = Vec::new();
    let mut missing = Vec::new();
    for (index, schema) in boot.schemas.iter().enumerate() {
        let table_state = store.table_state(&schema.table_id)?;
        let mut record = TableCopy::load(&store, schema.table_id)?;
        if record.is_none() && table_state.materialized_lsn >= initial_cut {
            record = Some(TableCopy {
                cut: initial_cut,
                staging: String::new(),
                phase: CopyPhase::Published,
                rows: 0,
                schema_version: table_state.schema_version,
            });
        }
        if record.is_none()
            && boot.layout == 0
            && initial
                .as_ref()
                .is_some_and(|transaction| transaction.mutation_chunks.count > 0)
        {
            record = Some(TableCopy {
                cut: initial_cut,
                staging: String::new(),
                phase: CopyPhase::Staged,
                rows: 0,
                schema_version: schema.version,
            });
        }
        if let Some(mut copy) = record {
            if table_state.materialized_lsn >= copy.cut && table_state.pending_operation.is_none() {
                copy.phase = CopyPhase::Published;
            }
            if copy.phase == CopyPhase::Published {
                recover_published_copy(
                    config,
                    &store,
                    &mut ledger,
                    schema.table_id,
                    &copy,
                    initial_cut,
                    table_state.snapshot_id.unwrap_or(0),
                )?;
                continue;
            }
            let copy_schema = flow_coordinator::load_source_schema(
                &store,
                &SourceId(config.source.id.clone()),
                schema.table_id,
                copy.schema_version,
            )?;
            if let Some((staged, transaction)) = staged_copy(config, &copy_schema, &copy, &reader)?
            {
                copy.phase = CopyPhase::Staged;
                copy.save(&store, schema.table_id)?;
                plans.push((index, copy_schema, copy, Some((staged, transaction))));
                continue;
            }
        }
        ensure!(
            table_state.pending_operation.is_none()
                && table_state.snapshot_id.is_none()
                && targets[&schema.table_id]
                    .metadata()
                    .current_snapshot_id()
                    .is_none(),
            "cannot recopy a table with eligible or visible publication"
        );
        missing.push(index);
    }
    if !missing.is_empty() && keeper.is_none() {
        retry_keeper_sql = connect(config, false).await?;
        keeper = Some(
            export_temporary_snapshot(
                &replication,
                &mut retry_keeper_sql,
                &format!("flow_copy_{}", uuid::Uuid::new_v4().simple()),
            )
            .await?,
        );
    }
    let mut exported = None;
    if let Some(snapshot) = &keeper {
        for index in &missing {
            let configured = &config.tables[*index];
            snapshot
                .lock_table(&configured.source_namespace, &configured.source_table)
                .await?;
        }
        let copy_schemas = registry
            .initialize(snapshot.transaction(), &config.tables)
            .await?;
        validate_publication(&sql, config, &copy_schemas, false).await?;
        exported = Some((snapshot.reexport().await?, snapshot.consistent_lsn));
        std::fs::create_dir_all(config.state_dir.join("bootstrap"))?;
        std::fs::File::open(&config.state_dir)?.sync_all()?;
        for index in missing {
            let copy = TableCopy {
                cut: snapshot.consistent_lsn,
                staging: format!(
                    "table-{}-{}",
                    boot.schemas[index].table_id.0,
                    uuid::Uuid::new_v4()
                ),
                phase: CopyPhase::Copying,
                rows: 0,
                schema_version: copy_schemas[index].version,
            };
            let old = TableCopy::load(&store, boot.schemas[index].table_id)?;
            if let Some(old) = old
                && !old.staging.is_empty()
            {
                remove_staging(config, boot.schemas[index].table_id, &old)?;
            }
            copy.save(&store, boot.schemas[index].table_id)?;
            plans.push((index, copy_schemas[index].clone(), copy, None));
        }
    }
    boot.layout = 1;
    boot.copied = false;
    persist_bootstrap(&store, boot)?;
    // The SQL keeper owns the re-export. Closing replication releases its slot
    // ownership and invalidates only the original export, not the keeper's view.
    drop(replication);
    let ack = Acknowledgement {
        received: journal.durable_lsn(),
        durable: ledger.acknowledgement().min(initial_cut),
        materialized: ledger.watermarks().materialized_lsn,
    };
    let (ack_send, ack_recv) = watch::channel(ack);
    let (progress_send, mut progress) = watch::channel(CaptureProgress {
        durable_lsn: journal.durable_lsn(),
        error: None,
    });
    let capture = tokio::spawn(capture_loop(
        config.clone(),
        boot.schemas.clone(),
        store.clone(),
        journal,
        progress_send,
        ack_recv,
    ));
    let work = futures::stream::iter(plans.into_iter().map(|(index, schema, copy, staged)| {
        let store = store.clone();
        let publisher = publisher.clone();
        let catalog = catalog.clone();
        let table = targets[&schema.table_id].clone();
        let exported = exported.clone();
        async move {
            copy_and_publish(
                config,
                &store,
                &publisher,
                catalog.as_ref(),
                CopyWork {
                    schema,
                    table,
                    index,
                    copy,
                    staged,
                    exported,
                },
            )
            .await
        }
    }))
    .buffer_unordered(4);
    tokio::pin!(work);
    let mut tick = tokio::time::interval(std::time::Duration::from_millis(20));
    let mut failure = None;
    let mut capture_finished = false;
    loop {
        tokio::select! {
            result = work.next() => {
                let Some(result) = result else { break; };
                match result {
                    Ok((table, snapshot)) => {
                        if ledger.watermarks().materialized_lsn < initial_cut
                            && let Err(error) = ledger.table_materialized(initial_cut, table, snapshot) {
                            failure.get_or_insert(error);
                        }
                    }
                    Err(error) => { if failure.is_none() { failure = Some(error); } }
                }
            }
            changed = progress.changed(), if !capture_finished => {
                capture_finished = changed.is_err();
                if let Some(error) = &progress.borrow_and_update().error {
                    failure.get_or_insert_with(|| anyhow::anyhow!(error.clone()));
                } else if capture_finished {
                    failure.get_or_insert_with(|| anyhow::anyhow!("source capture ended before bootstrap completed"));
                }
            }
            _ = tick.tick() => {
                if let Err(error) = register_captured(&reader, &mut ledger, config.limits.pending_transactions) { failure.get_or_insert(error); }
            }
        }
        ack_send.send_replace(Acknowledgement {
            received: progress.borrow().durable_lsn,
            durable: ledger.acknowledgement().min(initial_cut),
            materialized: ledger.watermarks().materialized_lsn.min(initial_cut),
        });
    }
    if let Some(error) = &progress.borrow().error {
        failure.get_or_insert_with(|| anyhow::anyhow!(error.clone()));
    }
    drop(progress);
    drop(ack_send);
    capture.await??;
    if let Some(error) = failure {
        return Err(error);
    }
    if let Some(snapshot) = keeper {
        snapshot.finish().await?;
    }
    register_captured(&reader, &mut ledger, usize::MAX)?;
    ledger.reconcile_table_progress()?;
    boot.copied = true;
    persist_bootstrap(&store, boot)?;
    tracing::info!(event = "bootstrap_completed", tables = boot.schemas.len(), lsn = %initial_cut, "initial bases published with concurrent CDC retained");
    Ok(())
}

fn register_captured(
    reader: &JournalReader,
    ledger: &mut SourceLedger,
    limit: usize,
) -> Result<()> {
    let mut transactions = reader
        .transactions_after(ledger.watermarks().journal_durable_lsn)?
        .take(limit);
    loop {
        let batch = transactions
            .by_ref()
            .take(ledger.batch_capacity())
            .collect::<flow_ingress_journal::Result<Vec<_>>>()?;
        if batch.is_empty() {
            break;
        }
        ledger.journaled_batch(&batch)?;
    }
    Ok(())
}

fn staged_copy(
    config: &Config,
    schema: &TableSchema,
    copy: &TableCopy,
    main: &JournalReader,
) -> Result<Option<(JournalReader, SourceTransaction)>> {
    let reader = if copy.staging.is_empty() {
        main.clone()
    } else {
        let path = copy.path(config, schema.table_id)?;
        if !path.is_dir() {
            return Ok(None);
        }
        let (journal, _) = Journal::open(path, journal_config(config))?;
        journal.reader()
    };
    let transaction = reader
        .transactions_after(PgLsn(copy.cut.0.saturating_sub(1)))?
        .next()
        .transpose()?;
    let Some(transaction) = transaction else {
        return Ok(None);
    };
    ensure!(
        transaction.source_id.0 == config.source.id
            && transaction.xid == 0
            && transaction.end_lsn == copy.cut
            && transaction.affected_tables.contains(&schema.table_id),
        "bootstrap staging terminal differs from durable table cut"
    );
    Ok(Some((reader, transaction)))
}

struct CopyWork {
    schema: TableSchema,
    table: Table,
    index: usize,
    copy: TableCopy,
    staged: Option<(JournalReader, SourceTransaction)>,
    exported: Option<(String, PgLsn)>,
}

async fn copy_and_publish(
    config: &Config,
    store: &StateStore,
    publisher: &TablePublisher,
    catalog: &dyn Catalog,
    work: CopyWork,
) -> Result<(TableId, i64)> {
    let CopyWork {
        schema,
        table,
        index,
        mut copy,
        staged,
        exported,
    } = work;
    let schema = &schema;
    let table = &table;
    let (reader, transaction) = if let Some(staged) = staged {
        staged
    } else {
        let (snapshot_name, cut) = exported.context("COPY worker has no keeper snapshot")?;
        ensure!(cut == copy.cut, "COPY cut changed after staging intent");
        let mut sql = connect(config, false).await?;
        let snapshot =
            SnapshotSession::import(&mut sql, &config.source.slot, cut, &snapshot_name).await?;
        let configured = &config.tables[index];
        let (relation, resolved_types) =
            validate_source_table(snapshot.transaction(), configured, schema).await?;
        let projection = schema
            .columns
            .iter()
            .map(|column| column.name.clone())
            .collect::<Vec<_>>();
        let output = snapshot
            .copy_table(&relation.namespace, &relation.name, &projection)
            .await?;
        // COPY framing reads raw bytes. The catalog-validated resolver above
        // interprets user-defined OIDs, not rust-postgres's built-in Type list.
        let types = relation
            .columns
            .iter()
            .map(|column| {
                Type::from_oid(column.type_oid).unwrap_or_else(|| {
                    Type::new(
                        column.name.clone(),
                        column.type_oid,
                        flow_pg_source::tokio_postgres::types::Kind::Simple,
                        "public".into(),
                    )
                })
            })
            .collect::<Vec<_>>();
        let stream = BinaryCopyOutStream::new(output, &types);
        tokio::pin!(stream);
        let (mut journal, recovered) =
            Journal::open(copy.path(config, schema.table_id)?, journal_config(config))?;
        ensure!(
            recovered.transactions.is_empty(),
            "new COPY staging directory is not empty"
        );
        let mut rows = Vec::new();
        let mut bytes = 8u64;
        while let Some(row) = stream.as_mut().try_next().await? {
            let mutation = Mutation {
                table_id: schema.table_id,
                schema_version: schema.version,
                kind: MutationKind::Insert {
                    row: decode_copy_row_with_types(schema, &relation, &row, &resolved_types)?,
                },
            };
            let size = bincode::serialized_size(&mutation)?;
            ensure!(
                size + 8 <= u64::from(config.limits.chunk_bytes),
                "snapshot row exceeds configured chunk limit"
            );
            if !rows.is_empty()
                && (rows.len() >= config.limits.batch_rows
                    || bytes + size > u64::from(config.limits.chunk_bytes))
            {
                journal.append_chunk(0, &bincode::serialize(&rows)?)?;
                rows.clear();
                bytes = 8;
            }
            rows.push(mutation);
            bytes += size;
            copy.rows = copy
                .rows
                .checked_add(1)
                .context("COPY row count overflow")?;
        }
        if !rows.is_empty() {
            journal.append_chunk(0, &bincode::serialize(&rows)?)?;
        }
        let transaction = envelope(
            config,
            std::slice::from_ref(schema),
            cut,
            journal.transaction_chunks(0),
            copy.rows,
        )?;
        journal.commit(transaction.clone())?;
        copy.phase = CopyPhase::Staged;
        copy.save(store, schema.table_id)?;
        snapshot.finish().await?;
        tracing::info!(event = "bootstrap_table_staged", table_id = schema.table_id.0, rows = copy.rows, lsn = %cut, "initial table COPY durable");
        (journal.reader(), transaction)
    };
    let epoch = Epoch::new(
        transaction.source_id.clone(),
        schema.table_id,
        std::slice::from_ref(&transaction),
    )?;
    let current = catalog.load_table(table.identifier()).await?;
    ensure!(
        current.metadata().uuid() == table.metadata().uuid(),
        "initial target incarnation changed"
    );
    let current = crate::schema::ensure_table_schema(catalog, &current, schema).await?;
    let worker_table = current.clone();
    let worker_store = store.clone();
    let worker_schema = schema.clone();
    let worker_epoch = epoch.clone();
    let rows = config.limits.batch_rows;
    let bytes = config.limits.batch_bytes;
    let memory_bytes = config.limits.collapse_memory_bytes;
    let collapsed = tokio::task::spawn_blocking(move || {
        collapse_epoch(
            &worker_store,
            &reader,
            &worker_table,
            &worker_schema,
            &worker_epoch,
            &[transaction],
            CollapseLimits {
                batch_rows: rows,
                batch_bytes: bytes,
                memory_bytes,
            },
        )
    })
    .await??;
    let snapshot = publisher.publish(&current, schema, collapsed).await?;
    copy.phase = CopyPhase::Published;
    copy.save(store, schema.table_id)?;
    if store.operation(&epoch.id)?.is_some() {
        store.forget_applied(&epoch.id)?;
    } else {
        store.discard_transaction(&epoch.id.0)?;
    }
    remove_staging(config, schema.table_id, &copy)?;
    tracing::info!(event = "bootstrap_table_published", table_id = schema.table_id.0, rows = copy.rows, lsn = %copy.cut, "initial table base published");
    Ok((schema.table_id, snapshot.unwrap_or(0)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use flow_coordinator::{AckMode, JournalDurability};
    use flow_state_store::StateStoreOptions;

    #[test]
    fn published_copy_recovery_reconciles_ledger_and_reclaims_owned_staging() {
        let root = tempfile::tempdir().unwrap();
        let mut config: Config =
            toml::from_str(include_str!("../../../examples/flow.toml")).unwrap();
        config.state_dir = root.path().to_owned();
        let table = TableId(17);
        let cut = PgLsn(41);
        let copy = TableCopy {
            cut,
            staging: format!("table-{}-{}", table.0, uuid::Uuid::new_v4()),
            phase: CopyPhase::Published,
            rows: 3,
            schema_version: 0,
        };
        let staging = copy.path(&config, table).unwrap();
        std::fs::create_dir_all(&staging).unwrap();
        std::fs::write(
            staging.join("00000000000000000000.journal"),
            b"durable copy",
        )
        .unwrap();

        let store_path = root.path().join("index");
        let store = StateStore::open(&store_path, StateStoreOptions::default()).unwrap();
        let source = SourceId(config.source.id.clone());
        let mut ledger = SourceLedger::open(
            store.clone(),
            source.clone(),
            AckMode::Materialized,
            JournalDurability::LocalDisk,
        )
        .unwrap();
        let schema = config.tables[0].schema(table.0);
        ledger
            .journaled(envelope(&config, &[schema], cut, Default::default(), 0).unwrap())
            .unwrap();
        store.complete_noop(&table, cut, 0).unwrap();
        copy.save(&store, table).unwrap();
        drop(ledger);
        drop(store);

        let store = StateStore::open(&store_path, StateStoreOptions::default()).unwrap();
        let copy = TableCopy::load(&store, table).unwrap().unwrap();
        let mut ledger = SourceLedger::open(
            store.clone(),
            source.clone(),
            AckMode::Materialized,
            JournalDurability::LocalDisk,
        )
        .unwrap();
        assert_eq!(ledger.acknowledgement(), PgLsn(0));
        assert!(remove_staging(&config, TableId(table.0 + 1), &copy).is_err());
        assert!(staging.is_dir());

        recover_published_copy(&config, &store, &mut ledger, table, &copy, cut, 0).unwrap();
        assert_eq!(ledger.acknowledgement(), cut);
        assert!(!staging.exists());
        drop(ledger);
        drop(store);

        let store = StateStore::open(&store_path, StateStoreOptions::default()).unwrap();
        let copy = TableCopy::load(&store, table).unwrap().unwrap();
        let mut ledger = SourceLedger::open(
            store.clone(),
            source,
            AckMode::Materialized,
            JournalDurability::LocalDisk,
        )
        .unwrap();
        recover_published_copy(&config, &store, &mut ledger, table, &copy, cut, 0).unwrap();
        assert_eq!(ledger.acknowledgement(), cut);
        assert!(!staging.exists());
    }
}
