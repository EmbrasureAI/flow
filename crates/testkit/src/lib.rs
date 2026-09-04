//! Shared local integration fixtures. No PostgreSQL or external catalog is needed.
use async_trait::async_trait;
use flow_iceberg_ext::RewriteFilesAction;
use flow_materializer::{iceberg_schema, rows_from_batch};
use flow_model::{Column, ColumnType, Row, TableId, TableSchema};
use futures::TryStreamExt;
use iceberg::{
    Catalog, CatalogBuilder, Error, ErrorKind, MemoryCatalog, Namespace, NamespaceIdent, Result,
    TableCommit, TableCreation, TableIdent,
    io::LocalFsStorageFactory,
    memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder},
    spec::FormatVersion,
    table::Table,
};
use std::{
    collections::HashMap,
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

pub fn schema(table: u32) -> TableSchema {
    TableSchema {
        table_id: TableId(table),
        version: 0,
        columns: vec![
            Column {
                field_id: 1,
                name: "id".into(),
                data_type: ColumnType::Int64,
                nullable: false,
            },
            Column {
                field_id: 2,
                name: "value".into(),
                data_type: ColumnType::String,
                nullable: true,
            },
        ],
        primary_key: vec![0],
        append_only: false,
    }
}
pub async fn catalog(path: &Path) -> MemoryCatalog {
    let catalog = MemoryCatalogBuilder::default()
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load(
            "local",
            HashMap::from([(
                MEMORY_CATALOG_WAREHOUSE.into(),
                path.to_string_lossy().into_owned(),
            )]),
        )
        .await
        .unwrap();
    catalog
        .create_namespace(&NamespaceIdent::new("test".into()), HashMap::new())
        .await
        .unwrap();
    catalog
}
pub async fn table(catalog: &dyn Catalog, schema: &TableSchema) -> Table {
    catalog
        .create_table(
            &NamespaceIdent::new("test".into()),
            TableCreation::builder()
                .name(format!("table_{}", schema.table_id.0))
                .format_version(FormatVersion::V2)
                .schema(iceberg_schema(schema).unwrap())
                .build(),
        )
        .await
        .unwrap()
}
/// Uses the stock iceberg-rust scan and its position-delete application.
pub async fn scan(table: &Table, schema: &TableSchema) -> anyhow::Result<Vec<Row>> {
    let mut stream = table.scan().build()?.to_arrow().await?;
    let mut rows = Vec::new();
    while let Some(batch) = stream.try_next().await? {
        rows.extend(rows_from_batch(schema, &batch)?);
    }
    Ok(rows)
}

/// Build publication input through real journal frames, including schema/key
/// validation. Maintenance fixtures use this instead of pre-sealing index rows.
pub fn collapse_fixture(
    store: &flow_state_store::StateStore,
    table: &Table,
    schema: &TableSchema,
    source: flow_model::SourceId,
    lsn: flow_model::PgLsn,
    changes: impl IntoIterator<Item = (flow_model::PrimaryKey, flow_state_store::Change)>,
) -> (flow_coordinator::Epoch, flow_coordinator::CollapsedEpoch) {
    use flow_ingress_journal::{Journal, JournalConfig};
    use flow_model::{
        Mutation, MutationKind, SourceTransaction, TableMutationCount, TableSchemaVersion,
    };
    use flow_state_store::Change;
    let temp = tempfile::tempdir().unwrap();
    let (mut journal, _) = Journal::open(temp.path(), JournalConfig::default()).unwrap();
    let mutations = changes
        .into_iter()
        .map(|(key, change)| Mutation {
            table_id: schema.table_id,
            schema_version: schema.version,
            kind: match change {
                Change::Insert(row) => MutationKind::Insert { row },
                Change::Update(row) => MutationKind::Update { old_key: key, row },
                Change::Delete => MutationKind::Delete { key },
            },
        })
        .collect::<Vec<_>>();
    for batch in mutations.chunks(store.batch_rows()) {
        journal
            .append_chunk(1, &bincode::serialize(batch).unwrap())
            .unwrap();
    }
    let transaction = SourceTransaction {
        source_id: source.clone(),
        xid: 1,
        begin_lsn: flow_model::PgLsn(lsn.0.checked_sub(2).unwrap()),
        commit_lsn: flow_model::PgLsn(lsn.0 - 1),
        end_lsn: lsn,
        commit_timestamp_micros: 0,
        affected_tables: vec![schema.table_id],
        schema_versions: vec![TableSchemaVersion {
            table_id: schema.table_id,
            version: schema.version,
        }],
        table_mutation_counts: Some(vec![TableMutationCount {
            table_id: schema.table_id,
            mutations: mutations.len() as u64,
        }]),
        mutation_chunks: journal.transaction_chunks(1),
    };
    journal.commit(transaction.clone()).unwrap();
    let epoch =
        flow_coordinator::Epoch::new(source, schema.table_id, std::slice::from_ref(&transaction))
            .unwrap();
    let collapsed = flow_coordinator::collapse_epoch(
        store,
        &journal,
        table,
        schema,
        &epoch,
        &[transaction],
        flow_coordinator::CollapseLimits {
            batch_rows: store.batch_rows(),
            batch_bytes: 1 << 20,
            memory_bytes: 0,
        },
    )
    .unwrap();
    (epoch, collapsed)
}

/// Simulates a server which durably committed but whose response was lost.
/// Recovery must inspect the published snapshot, not infer failure from transport.
#[derive(Debug)]
pub struct LostResponseCatalog {
    pub inner: MemoryCatalog,
    lose_next: AtomicBool,
    disconnected: AtomicBool,
    next_rewrite: Mutex<Option<RewriteFilesAction>>,
}
impl LostResponseCatalog {
    pub fn new(inner: MemoryCatalog) -> Self {
        Self {
            inner,
            lose_next: AtomicBool::new(false),
            disconnected: AtomicBool::new(false),
            next_rewrite: Mutex::new(None),
        }
    }
    /// Interleave external maintenance after a caller prepares its catalog CAS.
    pub fn rewrite_before_next_commit(&self, action: RewriteFilesAction) {
        *self.next_rewrite.lock().unwrap() = Some(action);
    }
    /// Keep recovery reads unavailable until the caller explicitly reconnects.
    pub fn lose_next_response_and_disconnect(&self) {
        self.lose_next.store(true, Ordering::SeqCst);
    }
    pub fn reconnect(&self) {
        self.disconnected.store(false, Ordering::SeqCst);
    }
    fn check_connection(&self) -> Result<()> {
        if self.disconnected.load(Ordering::SeqCst) {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "catalog disconnected after lost catalog response",
            ));
        }
        Ok(())
    }
}
#[async_trait]
impl Catalog for LostResponseCatalog {
    async fn list_namespaces(&self, p: Option<&NamespaceIdent>) -> Result<Vec<NamespaceIdent>> {
        self.inner.list_namespaces(p).await
    }
    async fn create_namespace(
        &self,
        n: &NamespaceIdent,
        p: HashMap<String, String>,
    ) -> Result<Namespace> {
        self.inner.create_namespace(n, p).await
    }
    async fn get_namespace(&self, n: &NamespaceIdent) -> Result<Namespace> {
        self.inner.get_namespace(n).await
    }
    async fn namespace_exists(&self, n: &NamespaceIdent) -> Result<bool> {
        self.inner.namespace_exists(n).await
    }
    async fn update_namespace(&self, n: &NamespaceIdent, p: HashMap<String, String>) -> Result<()> {
        self.inner.update_namespace(n, p).await
    }
    async fn drop_namespace(&self, n: &NamespaceIdent) -> Result<()> {
        self.inner.drop_namespace(n).await
    }
    async fn list_tables(&self, n: &NamespaceIdent) -> Result<Vec<TableIdent>> {
        self.inner.list_tables(n).await
    }
    async fn create_table(&self, n: &NamespaceIdent, c: TableCreation) -> Result<Table> {
        self.inner.create_table(n, c).await
    }
    async fn load_table(&self, t: &TableIdent) -> Result<Table> {
        self.check_connection()?;
        self.inner.load_table(t).await
    }
    async fn drop_table(&self, t: &TableIdent) -> Result<()> {
        self.inner.drop_table(t).await
    }
    async fn purge_table(&self, t: &TableIdent) -> Result<()> {
        self.inner.purge_table(t).await
    }
    async fn table_exists(&self, t: &TableIdent) -> Result<bool> {
        self.inner.table_exists(t).await
    }
    async fn rename_table(&self, s: &TableIdent, d: &TableIdent) -> Result<()> {
        self.inner.rename_table(s, d).await
    }
    async fn register_table(&self, t: &TableIdent, m: String) -> Result<Table> {
        self.inner.register_table(t, m).await
    }
    async fn update_table(&self, c: TableCommit) -> Result<Table> {
        self.check_connection()?;
        let rewrite = self.next_rewrite.lock().unwrap().take();
        if let Some(rewrite) = rewrite {
            let head = self.inner.load_table(c.identifier()).await?;
            rewrite.commit(&self.inner, &head).await?;
        }
        let table = self.inner.update_table(c).await?;
        if self.lose_next.swap(false, Ordering::SeqCst) {
            self.disconnected.store(true, Ordering::SeqCst);
            return Err(Error::new(
                ErrorKind::Unexpected,
                "injected lost catalog response",
            ));
        }
        Ok(table)
    }
}
