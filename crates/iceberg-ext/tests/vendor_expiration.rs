use async_trait::async_trait;
use flow_iceberg_ext::RowDeltaAction;
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::spec::{
    DataContentType, DataFile, DataFileBuilder, DataFileFormat, FormatVersion, NestedField,
    PrimitiveType, Schema, SnapshotReference, SnapshotRetention, Struct, Type,
};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{
    Catalog, CatalogBuilder, Namespace, NamespaceIdent, Result, TableCommit, TableCreation,
    TableIdent, TableUpdate,
};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

async fn setup() -> (iceberg::MemoryCatalog, Table) {
    let catalog = MemoryCatalogBuilder::default()
        .load(
            "test",
            HashMap::from([(
                MEMORY_CATALOG_WAREHOUSE.to_owned(),
                "memory://warehouse".to_owned(),
            )]),
        )
        .await
        .unwrap();
    let namespace = NamespaceIdent::new("replicated".to_owned());
    catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .unwrap();
    let schema = Schema::builder()
        .with_fields(vec![Arc::new(NestedField::required(
            1,
            "id",
            Type::Primitive(PrimitiveType::Long),
        ))])
        .build()
        .unwrap();
    let table = catalog
        .create_table(
            &namespace,
            TableCreation::builder()
                .name("rows".to_owned())
                .schema(schema)
                .format_version(FormatVersion::V2)
                .build(),
        )
        .await
        .unwrap();
    (catalog, table)
}

fn data(name: &str) -> DataFile {
    DataFileBuilder::default()
        .content(DataContentType::Data)
        .file_path(format!("memory://warehouse/{name}.parquet"))
        .file_format(DataFileFormat::Parquet)
        .partition(Struct::empty())
        .file_size_in_bytes(128)
        .record_count(10)
        .build()
        .unwrap()
}

#[derive(Debug)]
struct RacingCatalog {
    inner: iceberg::MemoryCatalog,
    mutation: Mutex<Option<TableUpdate>>,
}
#[async_trait]
impl Catalog for RacingCatalog {
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
    async fn update_table(&self, commit: TableCommit) -> Result<Table> {
        let mutation = self.mutation.lock().unwrap().take();
        if let Some(mutation) = mutation {
            self.inner
                .update_table(
                    TableCommit::builder()
                        .ident(commit.identifier().clone())
                        .requirements(vec![])
                        .updates(vec![mutation])
                        .build(),
                )
                .await?;
        }
        self.inner.update_table(commit).await
    }
}

async fn history() -> (iceberg::MemoryCatalog, Table, Vec<i64>) {
    let (catalog, mut head) = setup().await;
    let mut ids = vec![];
    for name in ["first", "second", "third"] {
        let published = RowDeltaAction::new(&head, name)
            .add_data_files(vec![data(name)])
            .commit(&catalog, &head)
            .await
            .unwrap();
        ids.push(published.snapshot_id);
        head = published.table;
    }
    (catalog, head, ids)
}

#[tokio::test]
async fn expiration_replans_when_an_existing_tag_moves_to_a_selected_snapshot() {
    let (inner, head, ids) = history().await;
    let head = inner
        .update_table(
            TableCommit::builder()
                .ident(head.identifier().clone())
                .requirements(vec![])
                .updates(vec![TableUpdate::SetSnapshotRef {
                    ref_name: "saved".into(),
                    reference: SnapshotReference::new(
                        ids[1],
                        SnapshotRetention::Tag {
                            max_ref_age_ms: None,
                        },
                    ),
                }])
                .build(),
        )
        .await
        .unwrap();
    let catalog = RacingCatalog {
        inner,
        mutation: Mutex::new(Some(TableUpdate::SetSnapshotRef {
            ref_name: "saved".into(),
            reference: SnapshotReference::new(
                ids[0],
                SnapshotRetention::Tag {
                    max_ref_age_ms: None,
                },
            ),
        })),
    };
    let tx = Transaction::new(&head);
    let retained = tx
        .expire_snapshots()
        .expire_older_than_ms(i64::MAX)
        .retain_last(1)
        .apply(tx)
        .unwrap()
        .commit(&catalog)
        .await
        .unwrap();
    assert_eq!(
        retained
            .metadata()
            .snapshot_reference("saved")
            .unwrap()
            .snapshot_id,
        ids[0]
    );
    assert!(retained.metadata().snapshot_by_id(ids[0]).is_some());
    assert!(retained.metadata().snapshot_by_id(ids[1]).is_none());
}

#[tokio::test]
async fn explicit_protection_precedes_validation_of_current_and_tag_heads() {
    let (catalog, head, ids) = history().await;
    let head = catalog
        .update_table(
            TableCommit::builder()
                .ident(head.identifier().clone())
                .requirements(vec![])
                .updates(vec![TableUpdate::SetSnapshotRef {
                    ref_name: "saved".into(),
                    reference: SnapshotReference::new(
                        ids[0],
                        SnapshotRetention::Tag {
                            max_ref_age_ms: Some(0),
                        },
                    ),
                }])
                .build(),
        )
        .await
        .unwrap();
    let tx = Transaction::new(&head);
    let retained = tx
        .expire_snapshots()
        .expire_snapshot_ids(ids.clone())
        .expire_older_than_ms(i64::MIN)
        .protect_snapshots([ids[0], ids[2]])
        .apply(tx)
        .unwrap()
        .commit(&catalog)
        .await
        .unwrap();
    assert!(retained.metadata().snapshot_by_id(ids[0]).is_some());
    assert!(retained.metadata().snapshot_reference("saved").is_some());
    assert!(retained.metadata().snapshot_by_id(ids[1]).is_none());
    assert!(retained.metadata().snapshot_by_id(ids[2]).is_some());
    let tx = Transaction::new(&retained);
    let timestamp = retained
        .metadata()
        .current_snapshot()
        .unwrap()
        .timestamp_ms();
    assert!(
        tx.expire_snapshots()
            .expire_snapshot_ids([ids[2]])
            .protect_newer_than_ms(timestamp)
            .apply(tx)
            .unwrap()
            .commit(&catalog)
            .await
            .is_ok()
    );
}
