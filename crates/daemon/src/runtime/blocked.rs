//! Durable table publication failures. The operation and journal remain authority.
use anyhow::{Result, ensure};
use flow_model::{OperationId, SourceId, TableId};
use flow_state_store::StateStore;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

const MAX_RETRY_DELAY: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct BlockedTable {
    pub(crate) table_id: TableId,
    pub(crate) error_code: String,
    pub(crate) blocked_at_ms: u64,
    pub(crate) last_failed_at_ms: u64,
    pub(crate) retry_at_ms: u64,
    pub(crate) attempts: u32,
    pub(crate) pending_operation: Option<OperationId>,
}

pub(crate) struct BlockedTables {
    store: StateStore,
    prefix: Vec<u8>,
    records: BTreeMap<TableId, BlockedTable>,
    deadlines: BTreeMap<TableId, Instant>,
}

impl BlockedTables {
    pub(crate) fn load(store: &StateStore, source: &SourceId) -> Result<Self> {
        Self::load_at(store, source, now_ms()?, Instant::now())
    }

    fn load_at(store: &StateStore, source: &SourceId, wall_ms: u64, now: Instant) -> Result<Self> {
        let prefix = prefix(source);
        let mut records = BTreeMap::new();
        let mut deadlines = BTreeMap::new();
        for item in store.source_transactions_after(&prefix, None) {
            let (key, bytes) = item?;
            let mut record: BlockedTable = serde_json::from_slice(&bytes)?;
            ensure!(
                key.as_ref() == record_key(&prefix, record.table_id).as_slice()
                    && record.attempts > 0
                    && valid_error_code(&record.error_code),
                "invalid durable blocked-table record"
            );
            // Persisted wall clocks can move backwards between processes. A
            // clock correction must not suppress recovery indefinitely.
            let delay = Duration::from_millis(record.retry_at_ms.saturating_sub(wall_ms))
                .min(MAX_RETRY_DELAY);
            record.retry_at_ms = wall_ms.saturating_add(delay.as_millis() as u64);
            if !requires_resync(&record.error_code) {
                deadlines.insert(record.table_id, now + delay);
            }
            records.insert(record.table_id, record);
        }
        Ok(Self {
            store: store.clone(),
            prefix,
            records,
            deadlines,
        })
    }

    pub(crate) fn record(
        &mut self,
        id: TableId,
        error_code: &str,
        pending_operation: Option<OperationId>,
    ) -> Result<()> {
        ensure!(
            valid_error_code(error_code),
            "invalid publication error code"
        );
        let wall_ms = now_ms()?;
        let previous = self.records.get(&id);
        // Only a resync clears a publication block. A transient failure while
        // settling its unfinished operation keeps the code and retries.
        let incoming = error_code;
        let error_code = if previous.is_some_and(|record| record.error_code == PUBLICATION_CHANGED)
        {
            PUBLICATION_CHANGED
        } else {
            error_code
        };
        let attempts = previous.map_or(1, |record| record.attempts.saturating_add(1));
        let delay = crate::retry::delay(attempts - 1).min(MAX_RETRY_DELAY);
        let record = BlockedTable {
            table_id: id,
            error_code: error_code.to_owned(),
            blocked_at_ms: previous.map_or(wall_ms, |record| record.blocked_at_ms),
            last_failed_at_ms: wall_ms,
            retry_at_ms: wall_ms.saturating_add(delay.as_millis() as u64),
            attempts,
            pending_operation,
        };
        self.store
            .put_source_transaction(&record_key(&self.prefix, id), &serde_json::to_vec(&record)?)?;
        // Never alter scheduler-visible state before its durable write succeeds.
        if requires_resync(incoming) {
            self.deadlines.remove(&id);
        } else {
            self.deadlines.insert(id, Instant::now() + delay);
        }
        self.records.insert(id, record);
        Ok(())
    }

    /// A completed publication cannot clear a publication block: the table's
    /// changes may have been skipped, so only a resync replaces it.
    pub(crate) fn clear(&mut self, id: TableId) -> Result<()> {
        if self
            .records
            .get(&id)
            .is_none_or(|record| record.error_code == PUBLICATION_CHANGED)
        {
            return Ok(());
        }
        self.store
            .delete_source_transaction(&record_key(&self.prefix, id))?;
        self.records.remove(&id);
        self.deadlines.remove(&id);
        Ok(())
    }

    pub(crate) fn get(&self, id: TableId) -> Option<&BlockedTable> {
        self.records.get(&id)
    }

    pub(crate) fn ids(&self) -> impl Iterator<Item = TableId> + '_ {
        self.records.keys().copied()
    }

    pub(crate) fn records(&self) -> impl Iterator<Item = &BlockedTable> {
        self.records.values()
    }

    pub(crate) fn deadline_for(&self, id: TableId) -> Option<Instant> {
        self.deadlines.get(&id).copied()
    }

    pub(crate) fn due(&self, now: Instant) -> Vec<TableId> {
        self.deadlines
            .iter()
            .filter_map(|(id, due)| (*due <= now).then_some(*id))
            .collect()
    }

    /// Admit one recovery-only attempt for a publication block's unfinished
    /// operation; the block itself never retries.
    pub(crate) fn schedule_recovery(&mut self, id: TableId) {
        if self.records.contains_key(&id) && !self.deadlines.contains_key(&id) {
            self.deadlines.insert(id, Instant::now());
        }
    }

    /// The attempt owns a worker now; keep its block visible without a hot timer.
    pub(crate) fn release_due(&mut self, id: TableId) {
        self.deadlines.remove(&id);
    }
}

fn prefix(source: &SourceId) -> Vec<u8> {
    let mut prefix = b"flow-blocked/v1/".to_vec();
    prefix.extend((source.0.len() as u64).to_be_bytes());
    prefix.extend(source.0.as_bytes());
    prefix.push(b'/');
    prefix
}

fn record_key(prefix: &[u8], id: TableId) -> Vec<u8> {
    let mut key = prefix.to_vec();
    key.extend(id.0.to_be_bytes());
    key
}

fn now_ms() -> Result<u64> {
    Ok(u64::try_from(
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis(),
    )?)
}

pub(crate) const PUBLICATION_CHANGED: &str = "publication_changed";

/// Capture latched these tables; retrying publication cannot clear them.
fn requires_resync(code: &str) -> bool {
    matches!(code, "source_schema_incompatible" | "publication_changed")
}

fn valid_error_code(code: &str) -> bool {
    matches!(
        code,
        "source_schema_incompatible"
            | "publication_changed"
            | "publication_replan"
            | "maintenance_pressure"
            | "catalog_unavailable"
            | "catalog_conflict"
            | "target_not_found"
            | "publication_transport"
            | "publication_http"
            | "storage_unavailable"
            | "storage_permission_denied"
            | "storage_not_found"
            | "storage_rate_limited"
            | "storage_conflict"
    )
}

/// Only call at the table publication boundary. Raw messages may contain URLs,
/// credentials or row values and must never enter durable status observations.
pub(crate) fn publication_error_code(error: &anyhow::Error) -> Option<&'static str> {
    if error.is::<flow_model::SourceTableBlocked>() {
        return Some("source_schema_incompatible");
    }
    let replanning = error
        .downcast_ref::<flow_coordinator::ReplanRequired>()
        .is_some();
    // Corruption and local state errors stay connection-wide even when wrapped
    // by a remote library. A transport's nested socket I/O is handled below.
    if error.chain().any(|cause| {
        if let Some(error) = cause.downcast_ref::<flow_state_store::Error>() {
            // A compaction candidate may race a newer indexed head. The
            // coordinator explicitly labels only these races safe to replan.
            return !replanning
                || !matches!(
                    error,
                    flow_state_store::Error::SnapshotMismatch { .. }
                        | flow_state_store::Error::ExactStateMismatch { .. }
                );
        }
        cause.is::<flow_ingress_journal::Error>() || cause.is::<flow_model::ModelError>()
    }) {
        return None;
    }
    // A reqwest socket failure has remote provenance. Other nested filesystem
    // errors, including OpenDAL's filesystem backend, must not be isolated.
    if error.chain().any(|cause| cause.is::<std::io::Error>())
        && !error.chain().any(|cause| cause.is::<reqwest::Error>())
    {
        return None;
    }
    if replanning {
        return Some("publication_replan");
    }
    if matches!(
        error.downcast_ref::<flow_compactor::Error>(),
        Some(flow_compactor::Error::MaintenanceRequired | flow_compactor::Error::DependencyBudget)
    ) {
        return Some("maintenance_pressure");
    }
    for cause in error.chain() {
        if let Some(error) = cause.downcast_ref::<reqwest::Error>() {
            if error.is_timeout() || error.is_connect() || error.is_body() || error.is_request() {
                return Some("publication_transport");
            }
            if error.status().is_some() {
                return Some("publication_http");
            }
        }
    }
    for cause in error.chain() {
        if let Some(error) = cause.downcast_ref::<opendal::Error>() {
            return match error.kind() {
                opendal::ErrorKind::PermissionDenied => Some("storage_permission_denied"),
                opendal::ErrorKind::NotFound => Some("storage_not_found"),
                opendal::ErrorKind::RateLimited => Some("storage_rate_limited"),
                opendal::ErrorKind::ConditionNotMatch => Some("storage_conflict"),
                opendal::ErrorKind::Unexpected if error.is_temporary() || error.is_persistent() => {
                    Some("storage_unavailable")
                }
                _ => None,
            };
        }
    }
    for cause in error.chain() {
        if let Some(error) = cause.downcast_ref::<iceberg::Error>() {
            return match error.kind() {
                iceberg::ErrorKind::Unexpected => Some("catalog_unavailable"),
                iceberg::ErrorKind::CatalogCommitConflicts => Some("catalog_conflict"),
                iceberg::ErrorKind::TableNotFound | iceberg::ErrorKind::NamespaceNotFound => {
                    Some("target_not_found")
                }
                _ => None,
            };
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use flow_state_store::{ControlStore, StateStoreOptions};

    #[test]
    fn restart_preserves_block_and_operation_without_sharing_source_identity() {
        let temp = tempfile::tempdir().unwrap();
        let control = ControlStore::open(temp.path().join("control")).unwrap();
        let store = control
            .initialize_index(temp.path().join("index"), StateStoreOptions::default())
            .unwrap();
        let source = SourceId("source".into());
        let id = TableId(2);
        let operation = OperationId("original-epoch".into());
        let mut blocked = BlockedTables::load(&store, &source).unwrap();
        blocked
            .record(id, "catalog_unavailable", Some(operation.clone()))
            .unwrap();
        let first = blocked.get(id).unwrap().clone();
        blocked.release_due(id);
        assert!(blocked.deadline_for(id).is_none());
        assert!(blocked.get(id).is_some());
        drop(blocked);
        drop(store);
        drop(control);
        let control = ControlStore::open(temp.path().join("control")).unwrap();
        let store = StateStore::open_with_control(
            temp.path().join("index"),
            StateStoreOptions::default(),
            control,
        )
        .unwrap();
        let mut blocked = BlockedTables::load(&store, &source).unwrap();
        assert_eq!(blocked.get(id).unwrap().pending_operation, Some(operation));
        assert_eq!(blocked.ids().collect::<Vec<_>>(), [id]);
        assert!(
            BlockedTables::load(&store, &SourceId("source/other".into()))
                .unwrap()
                .ids()
                .next()
                .is_none()
        );
        blocked
            .record(id, "storage_permission_denied", None)
            .unwrap();
        assert_eq!(blocked.get(id).unwrap().blocked_at_ms, first.blocked_at_ms);
        assert_eq!(blocked.get(id).unwrap().attempts, 2);
        blocked.clear(id).unwrap();
        assert!(
            BlockedTables::load(&store, &source)
                .unwrap()
                .ids()
                .next()
                .is_none()
        );
    }

    #[test]
    fn persisted_future_deadline_is_clamped_and_corrupt_identity_is_rejected() {
        let temp = tempfile::tempdir().unwrap();
        let store =
            StateStore::open(temp.path().join("index"), StateStoreOptions::default()).unwrap();
        let source = SourceId("clock".into());
        let record = BlockedTable {
            table_id: TableId(1),
            error_code: "catalog_unavailable".into(),
            blocked_at_ms: 100,
            last_failed_at_ms: 100,
            retry_at_ms: u64::MAX,
            attempts: 1,
            pending_operation: None,
        };
        let key = record_key(&prefix(&source), record.table_id);
        store
            .put_source_transaction(&key, &serde_json::to_vec(&record).unwrap())
            .unwrap();
        let now = Instant::now();
        let blocked = BlockedTables::load_at(&store, &source, 1000, now).unwrap();
        assert_eq!(
            blocked.deadline_for(TableId(1)),
            Some(now + MAX_RETRY_DELAY)
        );
        assert!(blocked.due(now).is_empty());
        assert_eq!(blocked.due(now + MAX_RETRY_DELAY), [TableId(1)]);
        assert_eq!(blocked.get(TableId(1)).unwrap().retry_at_ms, 31_000);
        let mut record = record;
        record.retry_at_ms = 1;
        store
            .put_source_transaction(&key, &serde_json::to_vec(&record).unwrap())
            .unwrap();
        let overdue = BlockedTables::load_at(&store, &source, 1000, now).unwrap();
        assert_eq!(overdue.due(now), [TableId(1)]);
        record.table_id = TableId(2);
        store
            .put_source_transaction(&key, &serde_json::to_vec(&record).unwrap())
            .unwrap();
        assert!(BlockedTables::load(&store, &source).is_err());
    }

    #[test]
    fn publication_scope_never_hides_shared_state_or_unknown_failures() {
        let remote = iceberg::Error::new(iceberg::ErrorKind::Unexpected, "sensitive URL and token");
        assert_eq!(
            publication_error_code(&remote.into()),
            Some("catalog_unavailable")
        );
        let denied = opendal::Error::new(opendal::ErrorKind::PermissionDenied, "secret path");
        assert_eq!(
            publication_error_code(&denied.into()),
            Some("storage_permission_denied")
        );
        let wrapped = iceberg::Error::new(iceberg::ErrorKind::Unexpected, "outer")
            .with_source(flow_state_store::Error::AuthorityCorruption("inner".into()));
        assert_eq!(publication_error_code(&wrapped.into()), None);
        let wrapped = iceberg::Error::new(iceberg::ErrorKind::Unexpected, "outer")
            .with_source(flow_ingress_journal::Error::Corrupt);
        assert_eq!(publication_error_code(&wrapped.into()), None);
        let invalid = iceberg::Error::new(iceberg::ErrorKind::DataInvalid, "invalid metadata");
        assert_eq!(publication_error_code(&invalid.into()), None);
        assert_eq!(
            publication_error_code(&anyhow::anyhow!("table incarnation changed")),
            None
        );
        assert_eq!(
            publication_error_code(&std::io::Error::other("disk full").into()),
            None
        );
        let local_storage = opendal::Error::new(opendal::ErrorKind::PermissionDenied, "local file")
            .set_source(std::io::Error::from(std::io::ErrorKind::PermissionDenied));
        assert_eq!(publication_error_code(&local_storage.into()), None);
        let local_replan = anyhow::Error::from(std::io::Error::other("disk full"))
            .context(flow_coordinator::ReplanRequired);
        assert_eq!(publication_error_code(&local_replan), None);
        assert_eq!(
            publication_error_code(&flow_coordinator::ReplanRequired.into()),
            Some("publication_replan")
        );
        let raced =
            anyhow::Error::from(flow_state_store::Error::ExactStateMismatch { table: TableId(1) })
                .context(flow_coordinator::ReplanRequired);
        assert_eq!(publication_error_code(&raced), Some("publication_replan"));
        let corrupt =
            anyhow::Error::from(flow_state_store::Error::AuthorityCorruption("bad".into()))
                .context(flow_coordinator::ReplanRequired);
        assert_eq!(publication_error_code(&corrupt), None);
        for error in [
            flow_compactor::Error::MaintenanceRequired,
            flow_compactor::Error::DependencyBudget,
        ] {
            assert_eq!(
                publication_error_code(&error.into()),
                Some("maintenance_pressure")
            );
        }
        assert_eq!(
            publication_error_code(&flow_compactor::Error::InvalidInventory("corrupt").into()),
            None
        );
    }

    #[test]
    fn publication_changed_is_permanent_and_survives_reload() {
        let root = tempfile::tempdir().unwrap();
        let control = ControlStore::open(root.path().join("control")).unwrap();
        let store = control
            .initialize_index(root.path().join("index"), StateStoreOptions::default())
            .unwrap();
        let source = SourceId("publication".into());
        let mut blocked = BlockedTables::load(&store, &source).unwrap();
        blocked
            .record(TableId(12), "publication_changed", None)
            .unwrap();
        assert_eq!(
            blocked.deadline_for(TableId(12)),
            None,
            "no retry clears it"
        );
        let reloaded = BlockedTables::load(&store, &source).unwrap();
        let record = reloaded.get(TableId(12)).unwrap();
        assert_eq!(record.error_code, "publication_changed");
        assert_eq!(reloaded.deadline_for(TableId(12)), None);
        assert!(reloaded.get(TableId(11)).is_none());
    }
}
