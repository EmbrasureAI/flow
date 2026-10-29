use async_trait::async_trait;
use flow_iceberg_ext::{
    ArtifactSet, ArtifactTracker, RewriteFilesAction, RowDeltaAction, commit_dependency,
};
use flow_materializer::{DataWriter, DeleteWriter, WriterConfig};
use flow_model::{OperationId, PgLsn, Value};
use flow_testkit::{catalog, scan, schema, table};
use futures::future::BoxFuture;
use iceberg::io::FileIO;
use iceberg::memory::MemoryCatalog;
use iceberg::table::Table;
use iceberg::{Catalog, Namespace, NamespaceIdent, TableCommit, TableCreation, TableIdent};
use std::collections::HashMap;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use tempfile::TempDir;

#[derive(Debug)]
struct RegistrationGate {
    io: FileIO,
    reject: AtomicBool,
    registered: Mutex<Vec<ArtifactSet>>,
    catalog_pointer: Mutex<Option<String>>,
}
impl ArtifactTracker for RegistrationGate {
    fn register(&self, artifacts: ArtifactSet) -> BoxFuture<'_, iceberg::Result<()>> {
        Box::pin(async move {
            for index in 0..artifacts.len().unwrap() {
                let path = artifacts.path(index).unwrap();
                if self.catalog_pointer.lock().unwrap().as_deref() == Some(&path) {
                    assert!(self.io.exists(&path).await.unwrap());
                    continue;
                }
                assert!(
                    !self
                        .io
                        .exists(&artifacts.path(index).unwrap())
                        .await
                        .unwrap(),
                    "registration must precede every PUT"
                );
            }
            self.registered.lock().unwrap().push(artifacts);
            if self.reject.load(Ordering::SeqCst) {
                return Err(iceberg::Error::new(
                    iceberg::ErrorKind::Unexpected,
                    "ownership authority unavailable",
                ));
            }
            Ok(())
        })
    }
}
impl RegistrationGate {
    async fn verify_outputs(&self, present: bool) {
        let groups = self
            .registered
            .lock()
            .unwrap()
            .drain(..)
            .collect::<Vec<_>>();
        assert!(!groups.is_empty());
        for group in groups {
            for ordinal in 0..group.len().unwrap() {
                let path = group.path(ordinal).unwrap();
                if self.catalog_pointer.lock().unwrap().as_deref() == Some(&path) {
                    assert!(self.io.exists(&path).await.unwrap());
                    continue;
                }
                assert_eq!(
                    self.io.exists(&group.path(ordinal).unwrap()).await.unwrap(),
                    present
                );
            }
        }
    }
}

#[tokio::test]
async fn registration_failure_prevents_data_delete_and_catalog_metadata_uploads() {
    let temp = TempDir::new().unwrap();
    let catalog = catalog(&temp.path().join("warehouse")).await;
    let schema = schema(1);
    let table = table(&catalog, &schema).await;
    let gate = Arc::new(RegistrationGate {
        io: table.file_io().clone(),
        reject: AtomicBool::new(true),
        registered: Mutex::new(Vec::new()),
        catalog_pointer: Mutex::new(table.metadata_location().map(str::to_owned)),
    });
    let config = WriterConfig {
        artifact_tracker: Some(gate.clone()),
        ..Default::default()
    };
    let rows = vec![
        vec![Value::Int64(1), Value::Null],
        vec![Value::Int64(2), Value::Null],
    ];
    let make_writer = |name: &str| {
        DataWriter::new(
            table.file_io().clone(),
            table.metadata().location(),
            &OperationId(name.into()),
            schema.clone(),
            0,
            config.clone(),
        )
        .unwrap()
    };
    let mut writer = make_writer("rejected-data");
    assert!(writer.write(&rows, PgLsn(10)).await.is_err());
    gate.verify_outputs(false).await;
    gate.reject.store(false, Ordering::SeqCst);
    let mut writer = make_writer("accepted-data");
    let locations = writer.write(&rows, PgLsn(10)).await.unwrap().locations;
    let data = writer.close().await.unwrap();
    gate.verify_outputs(true).await;
    gate.reject.store(true, Ordering::SeqCst);
    let action = RowDeltaAction::new(&table, "ingest")
        .add_data_files(data.clone())
        .with_artifact_tracker(Some(gate.clone()));
    assert!(action.commit(&catalog, &table).await.is_err());
    gate.verify_outputs(false).await;
    gate.reject.store(false, Ordering::SeqCst);
    let initial = action.commit(&catalog, &table).await.unwrap().table;
    gate.verify_outputs(true).await;
    *gate.catalog_pointer.lock().unwrap() = initial.metadata_location().map(str::to_owned);
    let make_delete = |name: &str| {
        DeleteWriter::new(
            table.file_io().clone(),
            table.metadata().location(),
            &OperationId(name.into()),
            0,
            config.clone(),
        )
        .unwrap()
    };
    gate.reject.store(true, Ordering::SeqCst);
    assert!(
        make_delete("rejected-delete")
            .write(&locations[..1])
            .await
            .is_err()
    );
    gate.verify_outputs(false).await;
    gate.reject.store(false, Ordering::SeqCst);
    let mut delete = make_delete("accepted-delete");
    delete.write(&locations[..1]).await.unwrap();
    let deletes = delete.close().await.unwrap();
    gate.verify_outputs(true).await;
    let updated = RowDeltaAction::new(&initial, "delete")
        .add_delete_files(deletes.clone())
        .validate_data_files_exist([data[0].file_path().to_owned()])
        .with_artifact_tracker(Some(gate.clone()))
        .commit(&catalog, &initial)
        .await
        .unwrap()
        .table;
    gate.verify_outputs(true).await;
    *gate.catalog_pointer.lock().unwrap() = updated.metadata_location().map(str::to_owned);
    let mut writer = make_writer("rewrite-data");
    writer.write(&rows[1..], PgLsn(20)).await.unwrap();
    let replacement = writer.close().await.unwrap();
    gate.verify_outputs(true).await;
    let rewritten = RewriteFilesAction::new(&updated, "rewrite")
        .remove_data_files([data[0].file_path().to_owned()])
        .remove_delete_files([deletes[0].file_path().to_owned()])
        .add_data_files(replacement)
        .with_artifact_tracker(Some(gate.clone()))
        .commit(&catalog, &updated)
        .await
        .unwrap()
        .table;
    gate.verify_outputs(true).await;
    assert_eq!(scan(&rewritten, &schema).await.unwrap(), rows[1..]);
}

/// Registers nothing and reports a fixed replication position.
#[derive(Debug)]
struct Replicated(u64);
impl ArtifactTracker for Replicated {
    fn register(&self, _: ArtifactSet) -> BoxFuture<'_, iceberg::Result<()>> {
        Box::pin(async { Ok(()) })
    }
    fn commit_dependency(&self) -> Option<u64> {
        Some(self.0)
    }
}

/// Records the replication dependency each catalog commit observes.
#[derive(Debug)]
struct DependencyProbe {
    inner: MemoryCatalog,
    seen: Mutex<Vec<Option<u64>>>,
}
#[async_trait]
impl Catalog for DependencyProbe {
    async fn list_namespaces(
        &self,
        p: Option<&NamespaceIdent>,
    ) -> iceberg::Result<Vec<NamespaceIdent>> {
        self.inner.list_namespaces(p).await
    }
    async fn create_namespace(
        &self,
        n: &NamespaceIdent,
        p: HashMap<String, String>,
    ) -> iceberg::Result<Namespace> {
        self.inner.create_namespace(n, p).await
    }
    async fn get_namespace(&self, n: &NamespaceIdent) -> iceberg::Result<Namespace> {
        self.inner.get_namespace(n).await
    }
    async fn namespace_exists(&self, n: &NamespaceIdent) -> iceberg::Result<bool> {
        self.inner.namespace_exists(n).await
    }
    async fn update_namespace(
        &self,
        n: &NamespaceIdent,
        p: HashMap<String, String>,
    ) -> iceberg::Result<()> {
        self.inner.update_namespace(n, p).await
    }
    async fn drop_namespace(&self, n: &NamespaceIdent) -> iceberg::Result<()> {
        self.inner.drop_namespace(n).await
    }
    async fn list_tables(&self, n: &NamespaceIdent) -> iceberg::Result<Vec<TableIdent>> {
        self.inner.list_tables(n).await
    }
    async fn create_table(&self, n: &NamespaceIdent, c: TableCreation) -> iceberg::Result<Table> {
        self.inner.create_table(n, c).await
    }
    async fn load_table(&self, t: &TableIdent) -> iceberg::Result<Table> {
        self.inner.load_table(t).await
    }
    async fn drop_table(&self, t: &TableIdent) -> iceberg::Result<()> {
        self.inner.drop_table(t).await
    }
    async fn purge_table(&self, t: &TableIdent) -> iceberg::Result<()> {
        self.inner.purge_table(t).await
    }
    async fn table_exists(&self, t: &TableIdent) -> iceberg::Result<bool> {
        self.inner.table_exists(t).await
    }
    async fn rename_table(&self, s: &TableIdent, d: &TableIdent) -> iceberg::Result<()> {
        self.inner.rename_table(s, d).await
    }
    async fn register_table(&self, t: &TableIdent, m: String) -> iceberg::Result<Table> {
        self.inner.register_table(t, m).await
    }
    async fn update_table(&self, c: TableCommit) -> iceberg::Result<Table> {
        self.seen.lock().unwrap().push(commit_dependency());
        self.inner.update_table(c).await
    }
}

#[tokio::test]
async fn publication_commits_depend_only_on_what_their_tracker_replicated() {
    let temp = TempDir::new().unwrap();
    let probe = DependencyProbe {
        inner: catalog(&temp.path().join("warehouse")).await,
        seen: Mutex::new(Vec::new()),
    };
    let schema = schema(1);
    let table = table(&probe.inner, &schema).await;
    let write = |name: &'static str, rows: Vec<Vec<Value>>| {
        let table = table.clone();
        let schema = schema.clone();
        async move {
            let mut writer = DataWriter::new(
                table.file_io().clone(),
                table.metadata().location(),
                &OperationId(name.into()),
                schema,
                0,
                WriterConfig::default(),
            )
            .unwrap();
            writer.write(&rows, PgLsn(10)).await.unwrap();
            writer.close().await.unwrap()
        }
    };
    let first = write("untracked", vec![vec![Value::Int64(1), Value::Null]]).await;
    let second = write("tracked", vec![vec![Value::Int64(2), Value::Null]]).await;
    let replacement = write("rewrite", vec![vec![Value::Int64(1), Value::Null]]).await;
    // Without a replicating tracker the commit has no narrower dependency.
    let untracked = RowDeltaAction::new(&table, "untracked")
        .add_data_files(first.clone())
        .commit(&probe, &table)
        .await
        .unwrap()
        .table;
    let tracked = RowDeltaAction::new(&untracked, "tracked")
        .add_data_files(second)
        .with_artifact_tracker(Some(Arc::new(Replicated(7))))
        .commit(&probe, &untracked)
        .await
        .unwrap()
        .table;
    RewriteFilesAction::new(&tracked, "rewrite")
        .remove_data_files([first[0].file_path().to_owned()])
        .add_data_files(replacement)
        .with_artifact_tracker(Some(Arc::new(Replicated(9))))
        .commit(&probe, &tracked)
        .await
        .unwrap();
    assert_eq!(*probe.seen.lock().unwrap(), [None, Some(7), Some(9)]);
    // The dependency is scoped to its publication.
    assert_eq!(commit_dependency(), None);
}
