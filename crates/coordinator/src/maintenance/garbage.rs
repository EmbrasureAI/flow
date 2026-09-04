use super::{
    TableMaintenance,
    artifacts::{OwnedArtifacts, now_ms, registry_prefix},
    builds::active_build_protection,
};
use crate::publication::{ReplanRequired, blocking};
use anyhow::{Result, ensure};
use flow_iceberg_ext::retained_artifacts;
use flow_model::{OperationId, TableId};
use flow_state_store::CheckpointRecord;
use iceberg::table::Table;
use std::{collections::BTreeSet, time::Duration};

/// Object grace must cover readers that already planned an expired snapshot and
/// the maximum duration of an interrupted upload. Only registered objects qualify.
#[derive(Debug, Clone)]
pub struct GarbagePolicy {
    pub grace: Duration,
    pub max_objects: usize,
    pub max_records: usize,
}
impl Default for GarbagePolicy {
    fn default() -> Self {
        Self {
            grace: Duration::from_secs(24 * 60 * 60),
            max_objects: 1024,
            max_records: 64,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct GarbageProtection {
    pub snapshots: BTreeSet<i64>,
    pub operations: BTreeSet<OperationId>,
}
impl GarbageProtection {
    /// Include registrations even when their files are unavailable: silently
    /// dropping a missing checkpoint's protection would hide a recovery failure.
    pub fn from_checkpoints(table: TableId, checkpoints: &[CheckpointRecord]) -> Self {
        let mut result = Self::default();
        for checkpoint in checkpoints {
            for (id, state) in &checkpoint.tables {
                if *id == table {
                    result.snapshots.extend(state.snapshot_id);
                    result.operations.extend(state.pending_operation.clone());
                }
            }
            for record in &checkpoint.pending_operations {
                if record.operation.table_id == table {
                    result.snapshots.extend(record.operation.base_snapshot_id);
                    result.snapshots.extend(record.snapshot_id);
                    result.operations.insert(record.operation.id.clone());
                }
            }
        }
        result
    }
}

#[derive(Debug, Clone, Default)]
pub struct GarbageReport {
    pub examined_objects: usize,
    pub protected_objects: usize,
    pub delete_requests: usize,
    pub retired_records: usize,
}

impl TableMaintenance {
    /// Run on the serialized table actor, after reconciliation and expiration.
    /// Each pass scans a bounded registry slice and intersects only its finite
    /// candidates with retained metadata. No warehouse listing is performed.
    pub async fn collect_garbage(
        &self,
        table: &Table,
        table_id: TableId,
        policy: &GarbagePolicy,
        protection: &GarbageProtection,
    ) -> Result<GarbageReport> {
        ensure!(
            !policy.grace.is_zero() && policy.max_objects > 0 && policy.max_records > 0,
            "garbage collection needs positive grace and budgets"
        );
        let head = self.catalog.load_table(table.identifier()).await?;
        let prefix = registry_prefix(head.metadata().uuid()).into_bytes();
        let cursor_key = format!("artifact-gc/v1/{}", head.metadata().uuid()).into_bytes();
        let store = self.store.clone();
        let limit = policy.max_records;
        let saved_cursor_key = cursor_key.clone();
        let protected_head = head.clone();
        let (indexed, pending, records, builds) = blocking(move || {
            let cursor = store.source_transaction(&saved_cursor_key)?;
            let read = |after: Option<&[u8]>| -> Result<Vec<_>> {
                store
                    .source_transactions_after(&prefix, after)
                    .take(limit)
                    .map(|entry| {
                        let (key, value) = entry?;
                        Ok((
                            key.to_vec(),
                            bincode::deserialize::<OwnedArtifacts>(&value)?,
                        ))
                    })
                    .collect()
            };
            let mut records = read(cursor.as_deref())?;
            if records.is_empty() && cursor.is_some() {
                records = read(None)?;
            }
            Ok((
                store.table_state(&table_id)?,
                store.pending_operations()?,
                records,
                active_build_protection(&store, &protected_head, table_id)?,
            ))
        })
        .await?;
        if indexed.snapshot_id != head.metadata().current_snapshot_id() {
            return Err(ReplanRequired.into());
        }
        let mut protected_operations = protection.operations.clone();
        let mut protected_snapshots = protection.snapshots.clone();
        protected_operations.extend(builds.operations);
        protected_snapshots.extend(builds.snapshots);
        protected_snapshots.extend(indexed.snapshot_id);
        for record in pending
            .iter()
            .filter(|record| record.operation.table_id == table_id)
        {
            protected_operations.insert(record.operation.id.clone());
            protected_snapshots.extend(record.operation.base_snapshot_id);
            protected_snapshots.extend(record.snapshot_id);
        }
        for snapshot in protected_snapshots {
            ensure!(
                head.metadata().snapshot_by_id(snapshot).is_some(),
                "protected garbage-collection snapshot {snapshot} is not retained"
            );
        }
        let now = now_ms()?;
        let cutoff = now.saturating_sub(policy.grace.as_millis().try_into()?);
        let mut candidates = BTreeSet::new();
        let mut selected = Vec::new();
        let mut next_cursor = None;
        for (key, mut owner) in records {
            owner.validate(&head, table_id)?;
            if protected_operations.contains(&owner.operation) {
                next_cursor = Some(key);
                continue;
            }
            // A reserved ordinal may be uploaded long after the operation was
            // created. Start a full grace only after its fence has disappeared;
            // this also covers uploads interrupted near the end of a long build.
            if owner.unfenced_since_ms.is_none() {
                owner.unfenced_since_ms = Some(now);
                let store = self.store.clone();
                let saved_key = key.clone();
                let value = bincode::serialize(&owner)?;
                blocking(move || Ok(store.put_source_transaction(&saved_key, &value)?)).await?;
            }
            if owner.created_ms > cutoff
                || owner.unfenced_since_ms.is_some_and(|time| time > cutoff)
            {
                next_cursor = Some(key);
                continue;
            }
            let length = owner.artifacts.len().expect("validated count");
            let available = policy.max_objects.saturating_sub(candidates.len());
            if available == 0 {
                break;
            }
            let end = length.min(owner.cursor.saturating_add(available as u64));
            for ordinal in owner.cursor..end {
                candidates.insert(owner.artifacts.path(ordinal).expect("validated cursor"));
            }
            selected.push((key.clone(), owner, end));
            if end == length {
                next_cursor = Some(key);
            } else {
                break;
            }
        }
        if candidates.is_empty() {
            if let Some(cursor) = next_cursor {
                let store = self.store.clone();
                blocking(move || Ok(store.put_source_transaction(&cursor_key, &cursor)?)).await?;
            }
            return Ok(GarbageReport::default());
        }
        let protected = retained_artifacts(&head, &candidates, &self.cache).await?;
        let refreshed = self.catalog.load_table(table.identifier()).await?;
        if refreshed.metadata() != head.metadata()
            || refreshed.metadata_location() != head.metadata_location()
        {
            return Err(ReplanRequired.into());
        }
        let store = self.store.clone();
        let current = blocking(move || Ok(store.table_state(&table_id)?)).await?;
        if current != indexed {
            return Err(ReplanRequired.into());
        }
        let mut report = GarbageReport {
            examined_objects: candidates.len(),
            protected_objects: protected.len(),
            ..Default::default()
        };
        for path in candidates.difference(&protected) {
            head.file_io().delete(path).await?;
            report.delete_requests += 1;
        }
        for (key, mut owner, end) in selected {
            owner.protected |= (owner.cursor..end).any(|ordinal| {
                protected.contains(&owner.artifacts.path(ordinal).expect("validated range"))
            });
            owner.cursor = end;
            let finished = end == owner.artifacts.len().expect("validated count");
            let retire = finished && !owner.protected;
            if finished {
                owner.cursor = 0;
                owner.protected = false;
            }
            let value = bincode::serialize(&owner)?;
            let store = self.store.clone();
            blocking(move || {
                if retire {
                    store.delete_source_transaction(&key)?;
                } else {
                    store.put_source_transaction(&key, &value)?;
                }
                Ok(())
            })
            .await?;
            report.retired_records += usize::from(retire);
        }
        if let Some(cursor) = next_cursor {
            let store = self.store.clone();
            blocking(move || Ok(store.put_source_transaction(&cursor_key, &cursor)?)).await?;
        }
        Ok(report)
    }
}
