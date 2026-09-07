//! Durable ownership of immutable service outputs. Reservations precede uploads;
//! ordinal blocks keep source-control writes independent of ordinary file rolls.
use crate::blocking;
use anyhow::{Result, ensure};
use flow_iceberg_ext::{ArtifactSet, ArtifactTracker, NumberedArtifacts};
use flow_model::{OperationId, TableId};
use flow_state_store::{ControlStore, StateStore};
use futures::future::BoxFuture;
use iceberg::table::Table;
use serde::{Deserialize, Serialize};
use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::sync::Mutex;

const RESERVATION_FILES: u64 = 64;

#[derive(Clone, Serialize, Deserialize)]
pub(super) struct OwnedArtifacts {
    pub table_uuid: uuid::Uuid,
    pub table_id: TableId,
    pub location: String,
    pub operation: OperationId,
    pub created_ms: u64,
    pub unfenced_since_ms: Option<u64>,
    pub artifacts: ArtifactSet,
    pub cursor: u64,
    pub protected: bool,
}
impl OwnedArtifacts {
    pub(super) fn new(
        table: &Table,
        table_id: TableId,
        operation: OperationId,
        artifacts: ArtifactSet,
    ) -> Result<Self> {
        let result = Self {
            table_uuid: table.metadata().uuid(),
            table_id,
            location: table.metadata().location().trim_end_matches('/').to_owned(),
            operation,
            created_ms: now_ms()?,
            unfenced_since_ms: None,
            artifacts,
            cursor: 0,
            protected: false,
        };
        result.validate(table, table_id)?;
        Ok(result)
    }
    pub(super) fn validate(&self, table: &Table, table_id: TableId) -> Result<()> {
        ensure!(
            self.table_uuid == table.metadata().uuid()
                && self.table_id == table_id
                && self.location == table.metadata().location().trim_end_matches('/'),
            "artifact registry table identity mismatch"
        );
        self.validate_paths()
    }
    fn validate_paths(&self) -> Result<()> {
        let valid = |path: &str| {
            ["data", "metadata"].into_iter().any(|directory| {
                path.strip_prefix(&format!("{}/{directory}/", self.location))
                    .is_some_and(|name| {
                        !name.is_empty()
                            && name != "."
                            && name != ".."
                            && name
                                .bytes()
                                .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
                    })
            })
        };
        ensure!(
            self.artifacts.paths.iter().all(|path| valid(path)),
            "owned artifact is outside the table's flat service paths"
        );
        for range in &self.artifacts.ranges {
            ensure!(
                range.digits <= 20
                    && valid(&range.path(0))
                    && valid(&range.path(range.count.saturating_sub(1))),
                "invalid owned artifact range"
            );
        }
        let length = self
            .artifacts
            .len()
            .ok_or_else(|| anyhow::anyhow!("artifact count overflow"))?;
        ensure!(self.cursor <= length, "invalid artifact garbage cursor");
        Ok(())
    }
    pub(super) fn key(&self) -> Vec<u8> {
        format!(
            "{}{}",
            registry_prefix(self.table_uuid),
            uuid::Uuid::new_v4()
        )
        .into_bytes()
    }
}
pub(super) fn registry_prefix(uuid: uuid::Uuid) -> String {
    format!("owned-artifacts/v1/{uuid}/")
}
pub(super) fn now_ms() -> Result<u64> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)?
        .as_millis()
        .try_into()?)
}
fn iceberg_error(error: impl std::fmt::Display) -> iceberg::Error {
    iceberg::Error::new(
        iceberg::ErrorKind::Unexpected,
        format!("artifact ownership: {error}"),
    )
}

/// One metadata publication attempt creates one bounded durable record, even
/// when it replaces many manifests. Replays get new immutable metadata roots.
#[derive(Clone)]
pub(crate) struct MetadataRegistration {
    backend: RegistrationBackend,
    owner: OwnedArtifacts,
}
#[derive(Clone)]
enum RegistrationBackend {
    Live(StateStore),
    Recovery(ControlStore),
}
impl std::fmt::Debug for MetadataRegistration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetadataRegistration")
            .field("operation", &self.owner.operation)
            .finish()
    }
}
impl MetadataRegistration {
    pub(crate) fn live(
        store: StateStore,
        table: &Table,
        table_id: TableId,
        operation: OperationId,
    ) -> Result<Arc<dyn ArtifactTracker>> {
        Ok(Arc::new(Self {
            backend: RegistrationBackend::Live(store),
            owner: OwnedArtifacts::new(table, table_id, operation, ArtifactSet::default())?,
        }))
    }
    pub(crate) fn recovery(
        control: ControlStore,
        table: &Table,
        table_id: TableId,
        operation: OperationId,
    ) -> Result<Arc<dyn ArtifactTracker>> {
        Ok(Arc::new(Self {
            backend: RegistrationBackend::Recovery(control),
            owner: OwnedArtifacts::new(table, table_id, operation, ArtifactSet::default())?,
        }))
    }
}
impl ArtifactTracker for MetadataRegistration {
    fn register(&self, artifacts: ArtifactSet) -> BoxFuture<'_, iceberg::Result<()>> {
        Box::pin(async move {
            let mut owner = self.owner.clone();
            owner.artifacts = artifacts;
            owner.validate_paths().map_err(iceberg_error)?;
            owner.created_ms = now_ms().map_err(iceberg_error)?;
            let key = owner.key();
            let value = bincode::serialize(&owner).map_err(iceberg_error)?;
            let backend = self.backend.clone();
            blocking(move || {
                match backend {
                    RegistrationBackend::Live(store) => {
                        ensure!(
                            store
                                .table_state(&owner.table_id)?
                                .pending_operation
                                .as_ref()
                                == Some(&owner.operation),
                            "metadata registration lost its operation fence"
                        );
                        store.put_source_transaction(&key, &value)?;
                    }
                    RegistrationBackend::Recovery(control) => {
                        control.register_recovery_record(&owner.operation, &key, &value)?
                    }
                }
                Ok(())
            })
            .await
            .map_err(iceberg_error)
        })
    }
}

pub(crate) struct WriterRegistration {
    store: StateStore,
    key: Vec<u8>,
    owner: Mutex<OwnedArtifacts>,
}
impl std::fmt::Debug for WriterRegistration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WriterRegistration").finish_non_exhaustive()
    }
}
impl WriterRegistration {
    pub(crate) fn new(
        store: StateStore,
        table: &Table,
        table_id: TableId,
        operation: OperationId,
        attempt: &OperationId,
    ) -> Result<Arc<Self>> {
        let location = table.metadata().location().trim_end_matches('/');
        let plan = Self::plan_path(table, attempt);
        let owner = OwnedArtifacts::new(
            table,
            table_id,
            operation,
            ArtifactSet {
                paths: vec![
                    plan.clone(),
                    format!("{plan}-m0.avro"),
                    format!("{plan}-m1.avro"),
                ],
                ranges: ["data", "delete"]
                    .into_iter()
                    .map(|kind| NumberedArtifacts {
                        prefix: format!("{location}/data/{}-{kind}-", attempt.0),
                        suffix: if kind == "delete"
                            && table.metadata().format_version() == iceberg::spec::FormatVersion::V3
                        {
                            ".puffin"
                        } else {
                            ".parquet"
                        }
                        .into(),
                        count: RESERVATION_FILES,
                        digits: 6,
                    })
                    .collect(),
            },
        )?;
        Ok(Arc::new(Self {
            key: owner.key(),
            owner: Mutex::new(owner),
            store,
        }))
    }
    pub(crate) fn plan_path(table: &Table, attempt: &OperationId) -> String {
        format!(
            "{}/metadata/{}-prepared.avro",
            table.metadata().location().trim_end_matches('/'),
            attempt.0
        )
    }
    pub(crate) async fn record(&self) -> Result<(Vec<u8>, Vec<u8>)> {
        Ok((
            self.key.clone(),
            bincode::serialize(&*self.owner.lock().await)?,
        ))
    }
    /// Called only after all writers close. The smaller reservation is persisted
    /// atomically with Prepared; an interrupted build keeps its full reserved range.
    pub(crate) async fn finish(
        &self,
        data_count: usize,
        delete_count: usize,
    ) -> Result<(Vec<u8>, Vec<u8>)> {
        let mut owner = self.owner.lock().await;
        owner.created_ms = now_ms()?;
        owner.artifacts.ranges[0].count = data_count as u64;
        owner.artifacts.ranges[1].count = delete_count as u64;
        if delete_count == 0 {
            owner.artifacts.paths.remove(2);
        }
        if data_count == 0 {
            owner.artifacts.paths.remove(1);
        }
        Ok((self.key.clone(), bincode::serialize(&*owner)?))
    }
}
impl ArtifactTracker for WriterRegistration {
    fn register(&self, artifacts: ArtifactSet) -> BoxFuture<'_, iceberg::Result<()>> {
        Box::pin(async move {
            if artifacts.paths.len() != 1 || !artifacts.ranges.is_empty() {
                return Err(iceberg_error("writer must reserve one sequential output"));
            }
            let path = &artifacts.paths[0];
            let mut owner = self.owner.lock().await;
            let mut proposed = owner.clone();
            let mut matched = false;
            let mut changed = false;
            for range in &mut proposed.artifacts.ranges {
                let Some(ordinal) = path
                    .strip_prefix(&range.prefix)
                    .and_then(|s| s.strip_suffix(&range.suffix))
                    .and_then(|s| s.parse::<u64>().ok())
                else {
                    continue;
                };
                if range.path(ordinal) != *path {
                    continue;
                }
                matched = true;
                if ordinal >= range.count {
                    range.count = ordinal
                        .checked_div(RESERVATION_FILES)
                        .and_then(|n| n.checked_add(1))
                        .and_then(|n| n.checked_mul(RESERVATION_FILES))
                        .ok_or_else(|| iceberg_error("file reservation overflow"))?;
                    changed = true;
                }
                break;
            }
            if !matched {
                return Err(iceberg_error("output is outside its reserved operation"));
            }
            if changed {
                let value = bincode::serialize(&proposed).map_err(iceberg_error)?;
                let key = self.key.clone();
                let store = self.store.clone();
                blocking(move || Ok(store.put_source_transaction(&key, &value)?))
                    .await
                    .map_err(iceberg_error)?;
                *owner = proposed;
            }
            Ok(())
        })
    }
}
