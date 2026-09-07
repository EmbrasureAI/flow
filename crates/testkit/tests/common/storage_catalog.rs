use async_trait::async_trait;
use iceberg::{Catalog, Namespace, NamespaceIdent, Result, TableCommit, TableCreation, TableIdent};
use iceberg::{io::FileIO, table::Table};
use std::{collections::HashMap, sync::Arc};
#[derive(Debug)]
pub struct StorageCatalog {
    pub inner: Arc<dyn Catalog>,
    pub file_io: FileIO,
}
#[async_trait]
impl Catalog for StorageCatalog {
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
        let table = self.inner.load_table(t).await?;
        let mut builder = Table::builder()
            .identifier(table.identifier().clone())
            .metadata(table.metadata_ref())
            .file_io(self.file_io.clone())
            .runtime(iceberg::Runtime::current());
        if let Some(location) = table.metadata_location() {
            builder = builder.metadata_location(location);
        }
        builder.build()
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
        self.inner.update_table(commit).await
    }
}
