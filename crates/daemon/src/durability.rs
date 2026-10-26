//! Optional object-storage durability for this process's state directory.
//!
//! With `[durability]` configured, the journal, control store and authority
//! files are replicated write-behind to object storage, and every external
//! effect waits for a barrier first: catalog mutations through
//! [`FencedCatalog`], object deletions through the storage observer, and
//! source acknowledgements through [`AckGate`]. Without it, every function
//! here is a no-op and local disk remains the authority.
use crate::config::Config;
use anyhow::{Context, Result};
use flow_durability::{Durability, Layout};
use flow_ingress_journal::Journal;
use flow_pg_source::Acknowledgement;
use flow_state_store::ControlStore;
use iceberg::{
    Catalog, Namespace, NamespaceIdent, TableCommit, TableCreation, TableIdent, table::Table,
};
use std::{
    collections::{HashMap, VecDeque},
    path::Path,
    sync::{Arc, OnceLock},
};

static ACTIVE: OnceLock<Durability> = OnceLock::new();

/// The state directory entries recovery owns, named where they are written.
fn layout() -> Layout {
    Layout {
        control: "control".into(),
        journals: vec!["journal".into()],
        files: vec![
            crate::source::SOURCE_IDENTITY.into(),
            crate::lifecycle::RESYNC_MARKER.into(),
        ],
        derived: [
            "index",
            "index-generations",
            "checkpoints",
            "bootstrap",
            "spool",
            crate::generation::CLEAN_SHUTDOWN,
        ]
        .map(String::from)
        .to_vec(),
    }
}

/// Claim and, if needed, restore the state directory before anything opens it.
pub(crate) async fn start(config: &Config) -> Result<()> {
    let Some(settings) = &config.durability else {
        return flow_durability::ensure_not_replicated(&config.state_dir);
    };
    let durability = Durability::open(settings, &config.state_dir, &config.source.id, layout())
        .await
        .context("start remote durability")?;
    ACTIVE
        .set(durability)
        .map_err(|_| anyhow::anyhow!("remote durability already started"))
}

fn active() -> Option<&'static Durability> {
    ACTIVE.get()
}

/// Replicate control writes, then checkpoint everything this process
/// inherited. Barriers pass only after that checkpoint is durable.
pub(crate) async fn attach_control(control: &ControlStore) -> Result<()> {
    match active() {
        Some(durability) => attach_with(durability, control).await,
        None => Ok(()),
    }
}

async fn attach_with(durability: &Durability, control: &ControlStore) -> Result<()> {
    durability.attach_control(control).await?;
    durability.baseline().await
}

pub(crate) fn attach_journal(journal: &mut Journal) -> Result<()> {
    match active() {
        Some(durability) => durability.attach_journal("journal", journal),
        None => Ok(()),
    }
}

/// Write an authority file below the state directory atomically and, with
/// remote durability, replicate it.
pub(crate) fn write_authority_file(state_dir: &Path, name: &str, data: &[u8]) -> Result<()> {
    flow_durability::write_durable(&state_dir.join(name), data)?;
    match active() {
        Some(durability) => durability.write_file(name, data),
        None => Ok(()),
    }
}

/// Wait until local state is durable before an external effect.
pub(crate) async fn barrier() -> Result<()> {
    match active() {
        Some(durability) => durability.barrier().await,
        None => Ok(()),
    }
}

/// Resolves only if replication stopped, for example because a newer owner
/// claimed the state. The process must then stop all external effects.
pub(crate) async fn failed() -> anyhow::Error {
    match active() {
        Some(durability) => durability.failed().await,
        None => std::future::pending().await,
    }
}

/// Release ownership after a clean exit so the next start need not wait.
/// After an error, first let what was written before it replicate, such as
/// a resync marker. Both wait at most one lease.
pub(crate) async fn finish(result: &Result<std::process::ExitCode>) {
    let Some(durability) = active() else {
        return;
    };
    if result.is_ok() {
        if let Err(error) = durability.release().await {
            tracing::warn!(error = %format!("{error:#}"), "remote durability release failed; the next start waits for this lease");
            durability.stop();
        }
    } else {
        if durability.failure().is_none()
            && let Err(error) = durability.flush().await
        {
            tracing::warn!(error = %format!("{error:#}"), "remote durability could not replicate the final records");
        }
        durability.stop();
    }
}

/// Replicate a newly registered index checkpoint and drop replicas that are
/// no longer registered. Index checkpoints are not authority: a failure only
/// leaves a later recovery to an older checkpoint or a full rebuild.
pub(crate) fn replicate_index_checkpoint(
    control: &ControlStore,
    checkpoint: &flow_state_store::CheckpointRecord,
) {
    let Some(durability) = active() else {
        return;
    };
    let result = (|| -> Result<()> {
        let (total, uploaded) = durability.upload_index_checkpoint(&checkpoint.path)?;
        let registered: Vec<String> = control
            .checkpoints()?
            .iter()
            .filter_map(|record| record.path.file_name()?.to_str().map(str::to_owned))
            .collect();
        let deleted = durability.retain_index_checkpoints(&registered)?;
        tracing::info!(
            event = "index_checkpoint_replicated",
            revision = checkpoint.revision,
            total_bytes = total,
            uploaded_bytes = uploaded,
            deleted_objects = deleted,
            "row index checkpoint replicated"
        );
        Ok(())
    })();
    if let Err(error) = result {
        tracing::warn!(error = %format!("{error:#}"), "row index checkpoint replication failed");
    }
}

/// Before rebuilding a missing index, download the newest replicated index
/// checkpoint so recovery can advance it instead of scanning every table.
pub(crate) async fn restore_index_checkpoint(
    control: &ControlStore,
    state_dir: &Path,
) -> Result<()> {
    let Some(durability) = active() else {
        return Ok(());
    };
    if control
        .active_generation()?
        .is_none_or(|active| active.path.join("CURRENT").is_file())
    {
        return Ok(());
    }
    let mut checkpoints = control.checkpoints()?;
    checkpoints.sort_by_key(|checkpoint| std::cmp::Reverse(checkpoint.revision));
    for checkpoint in checkpoints {
        if checkpoint.path.join("CURRENT").is_file() {
            return Ok(());
        }
        let Some(name) = checkpoint.path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        let target = state_dir.join("checkpoints").join(name);
        if target.exists() {
            continue;
        }
        match durability.download_index_checkpoint(name, &target).await {
            Ok(true) => {
                if checkpoint.path != target {
                    let control = control.clone();
                    tokio::task::spawn_blocking(move || {
                        control.relocate_checkpoint(&checkpoint, &target)
                    })
                    .await??;
                }
                return Ok(());
            }
            Ok(false) => {}
            Err(error) => {
                tracing::warn!(error = %format!("{error:#}"), checkpoint = name, "replicated index checkpoint is unusable");
            }
        }
    }
    Ok(())
}

pub(crate) fn status() -> Option<flow_durability::Status> {
    active().map(Durability::status)
}

/// Source acknowledgements computed from local state, released in order once
/// the replicated log covers the state they were computed from.
pub(crate) struct AckGate {
    durability: Option<Durability>,
    pending: VecDeque<(u64, Acknowledgement)>,
}

impl Default for AckGate {
    fn default() -> Self {
        Self::new(active().cloned())
    }
}

impl AckGate {
    const LIMIT: usize = 64;

    pub(crate) fn new(durability: Option<Durability>) -> Self {
        Self {
            durability,
            pending: VecDeque::new(),
        }
    }

    /// Record progress observed now; returns the newest progress safe to send.
    pub(crate) fn admit(&mut self, progress: Acknowledgement) -> Option<Acknowledgement> {
        let Some(durability) = &self.durability else {
            return Some(progress);
        };
        let position = durability.position();
        if self.pending.len() >= Self::LIMIT {
            // A later position subsumes the newest entry.
            self.pending.pop_back();
        }
        self.pending.push_back((position, progress));
        self.ready()
    }

    /// The newest admitted progress that is now durable, if any.
    pub(crate) fn ready(&mut self) -> Option<Acknowledgement> {
        let durability = self.durability.as_ref()?;
        let mut latest = None;
        while self
            .pending
            .front()
            .is_some_and(|(position, _)| durability.covers(*position))
        {
            latest = self.pending.pop_front().map(|(_, progress)| progress);
        }
        latest
    }

    /// Resolves once the oldest pending progress can be sent; never resolves
    /// while nothing is pending. Owns its inputs so callers may keep using the
    /// gate in other `select!` branches.
    pub(crate) fn ready_signal(&self) -> impl std::future::Future<Output = Result<()>> + use<> {
        let durability = self.durability.clone();
        let target = self.pending.front().map(|(position, _)| *position);
        async move {
            match (durability, target) {
                (Some(durability), Some(position)) => durability.wait_for(position).await,
                _ => std::future::pending().await,
            }
        }
    }

    pub(crate) fn is_pending(&self) -> bool {
        !self.pending.is_empty()
    }
}

/// Wrap the destination catalog so every mutation waits for a barrier.
pub(crate) fn fence(catalog: Arc<dyn Catalog>) -> Arc<dyn Catalog> {
    match active() {
        Some(durability) => Arc::new(FencedCatalog {
            inner: catalog,
            durability: durability.clone(),
        }),
        None => catalog,
    }
}

struct FencedCatalog {
    inner: Arc<dyn Catalog>,
    durability: Durability,
}

impl std::fmt::Debug for FencedCatalog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FencedCatalog")
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}

impl FencedCatalog {
    async fn guard(&self) -> iceberg::Result<()> {
        barrier_error(self.durability.barrier().await, "a catalog change")
    }
}

fn barrier_error(result: Result<()>, effect: &str) -> iceberg::Result<()> {
    result.map_err(|error| {
        iceberg::Error::new(
            iceberg::ErrorKind::Unexpected,
            format!("remote durability barrier failed before {effect}: {error:#}"),
        )
    })
}

#[async_trait::async_trait]
impl Catalog for FencedCatalog {
    async fn list_namespaces(
        &self,
        parent: Option<&NamespaceIdent>,
    ) -> iceberg::Result<Vec<NamespaceIdent>> {
        self.inner.list_namespaces(parent).await
    }
    async fn create_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> iceberg::Result<Namespace> {
        self.guard().await?;
        self.inner.create_namespace(namespace, properties).await
    }
    async fn get_namespace(&self, namespace: &NamespaceIdent) -> iceberg::Result<Namespace> {
        self.inner.get_namespace(namespace).await
    }
    async fn namespace_exists(&self, namespace: &NamespaceIdent) -> iceberg::Result<bool> {
        self.inner.namespace_exists(namespace).await
    }
    async fn update_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> iceberg::Result<()> {
        self.guard().await?;
        self.inner.update_namespace(namespace, properties).await
    }
    async fn drop_namespace(&self, namespace: &NamespaceIdent) -> iceberg::Result<()> {
        self.guard().await?;
        self.inner.drop_namespace(namespace).await
    }
    async fn list_tables(&self, namespace: &NamespaceIdent) -> iceberg::Result<Vec<TableIdent>> {
        self.inner.list_tables(namespace).await
    }
    async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> iceberg::Result<Table> {
        self.guard().await?;
        self.inner.create_table(namespace, creation).await
    }
    async fn load_table(&self, table: &TableIdent) -> iceberg::Result<Table> {
        self.inner.load_table(table).await
    }
    async fn drop_table(&self, table: &TableIdent) -> iceberg::Result<()> {
        self.guard().await?;
        self.inner.drop_table(table).await
    }
    async fn purge_table(&self, table: &TableIdent) -> iceberg::Result<()> {
        self.guard().await?;
        self.inner.purge_table(table).await
    }
    async fn table_exists(&self, table: &TableIdent) -> iceberg::Result<bool> {
        self.inner.table_exists(table).await
    }
    async fn rename_table(&self, src: &TableIdent, dest: &TableIdent) -> iceberg::Result<()> {
        self.guard().await?;
        self.inner.rename_table(src, dest).await
    }
    async fn register_table(
        &self,
        table: &TableIdent,
        metadata_location: String,
    ) -> iceberg::Result<Table> {
        self.guard().await?;
        self.inner.register_table(table, metadata_location).await
    }
    async fn update_table(&self, commit: TableCommit) -> iceberg::Result<Table> {
        self.guard().await?;
        self.inner.update_table(commit).await
    }
}

/// Barrier for storage deletions, mapped to the storage error type.
pub(crate) async fn storage_guard() -> iceberg::Result<()> {
    barrier_error(barrier().await, "deleting objects")
}

#[cfg(test)]
mod tests {
    use super::*;
    use iceberg::{
        CatalogBuilder,
        memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder},
    };
    use opendal::{Operator, services::Memory};
    use std::time::Duration;

    fn settings() -> flow_durability::Config {
        flow_durability::Config {
            lease_secs: 4,
            request_timeout_secs: 1,
            stall_warning_secs: 2,
            checkpoint_interval_secs: 3600,
            ..flow_durability::Config::new("memory://daemon")
        }
    }

    async fn open(operator: &Operator, state: &std::path::Path) -> Durability {
        Durability::open_with_operator(&settings(), operator.clone(), state, "source", layout())
            .await
            .unwrap()
    }

    async fn catalog(name: &str) -> Arc<dyn Catalog> {
        Arc::new(
            MemoryCatalogBuilder::default()
                .load(
                    name,
                    HashMap::from([(MEMORY_CATALOG_WAREHOUSE.into(), format!("memory://{name}"))]),
                )
                .await
                .unwrap(),
        )
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn catalog_changes_wait_for_inherited_state_and_stop_after_replacement() {
        let operator = Operator::new(Memory::default()).unwrap().finish();
        let state = tempfile::tempdir().unwrap();
        let durability = open(&operator, state.path()).await;
        let fenced = FencedCatalog {
            inner: catalog("fenced").await,
            durability: durability.clone(),
        };
        let namespace = NamespaceIdent::new("flow".into());
        // Nothing inherited is replicated before the baseline: no catalog change.
        assert!(
            tokio::time::timeout(
                Duration::from_millis(500),
                fenced.create_namespace(&namespace, HashMap::new())
            )
            .await
            .is_err()
        );
        let control = ControlStore::open(state.path().join("control")).unwrap();
        attach_with(&durability, &control).await.unwrap();
        fenced
            .create_namespace(&namespace, HashMap::new())
            .await
            .unwrap();
        // Reads never wait; after a successor claims the state, changes fail.
        let successor_dir = tempfile::tempdir().unwrap();
        let successor = tokio::spawn({
            let operator = operator.clone();
            let path = successor_dir.path().to_path_buf();
            async move { open(&operator, &path).await }
        });
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while durability.failure().is_none() && std::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(fenced.namespace_exists(&namespace).await.unwrap());
        let error = fenced
            .create_namespace(&NamespaceIdent::new("other".into()), HashMap::new())
            .await
            .unwrap_err();
        assert!(error.to_string().contains("remote durability"));
        successor.await.unwrap().stop();
        durability.stop();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn acknowledgements_are_released_only_after_their_state_is_replicated() {
        let progress = |lsn| Acknowledgement {
            received: flow_model::PgLsn(lsn),
            durable: flow_model::PgLsn(lsn),
            materialized: flow_model::PgLsn(lsn),
        };
        let mut local = AckGate::new(None);
        assert_eq!(local.admit(progress(5)).map(|ack| ack.durable.0), Some(5));
        assert!(!local.is_pending());

        let operator = Operator::new(Memory::default()).unwrap().finish();
        let state = tempfile::tempdir().unwrap();
        let durability = open(&operator, state.path()).await;
        let control = ControlStore::open(state.path().join("control")).unwrap();
        attach_with(&durability, &control).await.unwrap();
        let mut gate = AckGate::new(Some(durability.clone()));
        // A new authority record has not reached object storage yet.
        durability
            .write_file(crate::source::SOURCE_IDENTITY, b"{}")
            .unwrap();
        let first = gate.admit(progress(10));
        durability
            .write_file(crate::source::SOURCE_IDENTITY, b"{}")
            .unwrap();
        let second = gate.admit(progress(20));
        if first.is_none() {
            assert!(gate.is_pending());
            gate.ready_signal().await.unwrap();
        }
        // Released in order, newest replicated progress last.
        let mut released: Vec<_> = first.into_iter().chain(second).collect();
        loop {
            if let Some(ready) = gate.ready() {
                released.push(ready);
            }
            if !gate.is_pending() {
                break;
            }
            gate.ready_signal().await.unwrap();
        }
        assert_eq!(released.last().map(|ack| ack.durable.0), Some(20));
        assert!(
            released
                .windows(2)
                .all(|pair| pair[0].durable <= pair[1].durable)
        );
        durability.stop();
    }
}
