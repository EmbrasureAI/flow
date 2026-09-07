//! Durable ownership of speculative compaction, without a table publication
//! fence. The table actor serializes registration, retirement and retention.

use super::GarbageProtection;
use crate::artifacts::WriterRegistration;
use crate::{blocking, publication::ReplanRequired};
use anyhow::{Result, ensure};
use bincode::Options;
use flow_iceberg_ext::ArtifactTracker;
use flow_model::{OperationId, TableId};
use flow_state_store::{OperationKind, OperationPhase, StateStore};
use iceberg::table::Table;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

const ROOT_PREFIX: &[u8] = b"active-build/";
const PREFIX: &str = "active-build/v1/";
const MAX_RECORD_BYTES: u64 = 16 << 10;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ActiveBuild {
    table_uuid: uuid::Uuid,
    table_id: TableId,
    location: String,
    operation: OperationId,
    base_snapshot_id: i64,
}

impl ActiveBuild {
    fn key(&self) -> Vec<u8> {
        format!("{PREFIX}{}", self.table_uuid).into_bytes()
    }

    fn validate(&self) -> Result<()> {
        ensure!(
            !self.location.is_empty()
                && self.location.len() <= 8192
                && !self.operation.0.is_empty()
                && self.operation.0.len() <= 256,
            "invalid or oversized compaction build identity"
        );
        Ok(())
    }

    fn validate_table(&self, table: &Table, table_id: TableId) -> Result<()> {
        self.validate()?;
        ensure!(
            self.table_uuid == table.metadata().uuid()
                && self.table_id == table_id
                && self.location == table.metadata().location().trim_end_matches('/'),
            "compaction build table identity mismatch"
        );
        Ok(())
    }

    fn decode(key: &[u8], bytes: &[u8]) -> Result<Self> {
        ensure!(
            key.starts_with(PREFIX.as_bytes()),
            "unsupported compaction build record version"
        );
        ensure!(
            bytes.len() as u64 <= MAX_RECORD_BYTES,
            "oversized compaction build record"
        );
        let record: Self = bincode::DefaultOptions::new()
            .with_fixint_encoding()
            .with_limit(MAX_RECORD_BYTES)
            .reject_trailing_bytes()
            .deserialize(bytes)?;
        record.validate()?;
        ensure!(record.key() == key, "compaction build record key mismatch");
        Ok(record)
    }
}

/// Ownership of one build at a retained snapshot. Dropping this handle does not
/// release protection: only explicit retirement or startup cleanup does so.
///
/// Call registration and retirement on the serialized table actor. Before
/// retirement, join the actual worker and all its uploads; aborting an async
/// wrapper around `spawn_blocking` does not stop that worker.
#[must_use = "retain build ownership until its worker has joined"]
#[derive(Clone)]
pub struct BuildRegistration {
    store: StateStore,
    record: ActiveBuild,
    writer: Arc<WriterRegistration>,
}

impl BuildRegistration {
    /// Reserve outputs and the base ancestry before any speculative work starts.
    /// Ordinary CDC may advance immediately after this method returns.
    pub async fn begin(
        store: StateStore,
        table: &Table,
        table_id: TableId,
        operation: OperationId,
    ) -> Result<Self> {
        let base_snapshot_id = table
            .metadata()
            .current_snapshot_id()
            .ok_or_else(|| anyhow::anyhow!("compaction build requires a base snapshot"))?;
        let record = ActiveBuild {
            table_uuid: table.metadata().uuid(),
            table_id,
            location: table.metadata().location().trim_end_matches('/').to_owned(),
            operation: operation.clone(),
            base_snapshot_id,
        };
        record.validate_table(table, table_id)?;
        let writer = WriterRegistration::new(
            store.clone(),
            table,
            table_id,
            operation.clone(),
            &operation,
        )?;
        let reservation = writer.record().await?;
        let key = record.key();
        let value = bincode::serialize(&record)?;
        let saved_store = store.clone();
        blocking(move || {
            ensure!(
                saved_store.source_transaction(&key)?.is_none(),
                "table already has an active compaction build"
            );
            ensure!(
                saved_store.operation(&operation)?.is_none(),
                "compaction build operation already exists"
            );
            let state = saved_store.table_state(&table_id)?;
            if state.pending_operation.is_some() || state.snapshot_id != Some(base_snapshot_id) {
                return Err(ReplanRequired.into());
            }
            // No writer is launched until both barriers succeed. A crash between
            // them leaves only unused artifact reservations for ordinary GC.
            saved_store.put_source_transaction(&reservation.0, &reservation.1)?;
            saved_store.put_source_transaction(&key, &value)?;
            Ok(())
        })
        .await?;
        Ok(Self {
            store,
            record,
            writer,
        })
    }

    pub fn operation_id(&self) -> &OperationId {
        &self.record.operation
    }

    pub fn artifact_tracker(&self) -> Arc<dyn ArtifactTracker> {
        self.writer.clone()
    }

    pub(crate) fn validate_table(&self, table: &Table, table_id: TableId) -> Result<()> {
        self.record.validate_table(table, table_id)
    }

    pub(crate) fn writer(&self) -> Arc<WriterRegistration> {
        self.writer.clone()
    }

    /// Abandon a joined worker. A normal publication operation, if one was
    /// started, must first be resolved through the existing recovery protocol.
    pub async fn release_after_join(&self) -> Result<()> {
        self.retire(false).await
    }

    /// Transfer protection to the normal durable publication fence. The caller
    /// has joined the worker and sealed Prepared; unknown outcomes thereafter
    /// belong exclusively to normal operation recovery, never catch-up.
    pub async fn promoted(&self) -> Result<()> {
        self.retire(true).await
    }

    pub(crate) async fn verify_owned(&self) -> Result<()> {
        let this = self.clone();
        blocking(move || this.current_record().map(|_| ())).await
    }

    fn current_record(&self) -> Result<ActiveBuild> {
        let key = self.record.key();
        let value = self
            .store
            .source_transaction(&key)?
            .ok_or_else(|| anyhow::anyhow!("compaction build ownership is missing"))?;
        let current = ActiveBuild::decode(&key, &value)?;
        ensure!(
            current.operation == self.record.operation
                && current.base_snapshot_id == self.record.base_snapshot_id
                && current.table_id == self.record.table_id
                && current.location == self.record.location,
            "compaction build ownership changed"
        );
        Ok(current)
    }

    async fn retire(&self, promoted: bool) -> Result<()> {
        let this = self.clone();
        blocking(move || {
            let key = this.record.key();
            let current = this.current_record()?;
            let operation = this.store.operation(&current.operation)?;
            if promoted {
                let operation = operation
                    .ok_or_else(|| anyhow::anyhow!("compaction publication is not durable"))?;
                ensure!(
                    operation.operation.table_id == current.table_id
                        && operation.operation.kind == OperationKind::Rewrite
                        && matches!(
                            operation.phase,
                            OperationPhase::Prepared
                                | OperationPhase::Committed
                                | OperationPhase::Applied
                        ),
                    "compaction build promotion requires sealed durable publication"
                );
                if operation.phase != OperationPhase::Applied {
                    ensure!(
                        this.store
                            .table_state(&current.table_id)?
                            .pending_operation
                            .as_ref()
                            == Some(&current.operation),
                        "compaction publication lost its durable fence"
                    );
                }
            } else {
                ensure!(
                    operation.is_none(),
                    "recover normal publication before abandoning its build"
                );
            }
            this.store.delete_source_transaction(&key)?;
            Ok(())
        })
        .await
    }
}

/// Include the build's base and every retained descendant in maintenance
/// protection. Keeping the intervening ancestry is required for catch-up proof.
pub fn active_build_protection(
    store: &StateStore,
    table: &Table,
    table_id: TableId,
) -> Result<GarbageProtection> {
    let key = format!("{PREFIX}{}", table.metadata().uuid()).into_bytes();
    let Some(bytes) = store.source_transaction(&key)? else {
        return Ok(GarbageProtection::default());
    };
    let record = ActiveBuild::decode(&key, &bytes)?;
    record.validate_table(table, table_id)?;
    let base = table
        .metadata()
        .snapshot_by_id(record.base_snapshot_id)
        .ok_or_else(|| {
            anyhow::anyhow!("active compaction build base snapshot is no longer retained")
        })?;
    let mut protection = GarbageProtection::default();
    protection.operations.insert(record.operation);
    protection.snapshots.extend(
        table
            .metadata()
            .snapshots()
            .filter(|snapshot| snapshot.sequence_number() >= base.sequence_number())
            .map(|snapshot| snapshot.snapshot_id()),
    );
    Ok(protection)
}

/// Retire builds left by the previous process, without changing publication
/// operations. Call only after opening/rebuilding the authoritative index and
/// before launching any workers or retention/GC actors. SOURCE records survive
/// generation rebuilds; stale checkpoint contents cannot restore old ownership.
pub fn discard_abandoned_builds(store: &StateStore) -> Result<usize> {
    let mut discarded = 0;
    for entry in store.source_transactions_after(ROOT_PREFIX, None) {
        let (key, value) = entry?;
        ActiveBuild::decode(&key, &value)?;
        store.delete_source_transaction(&key)?;
        discarded += 1;
    }
    Ok(discarded)
}
