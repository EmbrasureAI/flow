use super::{TableMaintenance, builds::active_build_protection};
use crate::artifacts::{OwnedArtifacts, now_ms, registry_prefix};
use crate::{blocking, publication::ReplanRequired};
use anyhow::{Result, ensure};
use flow_iceberg_ext::retained_artifacts;
use flow_model::{OperationId, TableId};
use flow_state_store::CheckpointRecord;
use iceberg::table::Table;
use std::{
    collections::BTreeSet,
    time::{Duration, Instant},
};

/// Object grace must cover readers that already planned an expired snapshot and
/// the maximum duration of an interrupted upload. Only registered objects qualify.
#[derive(Debug, Clone)]
pub struct GarbagePolicy {
    pub grace: Duration,
    pub max_objects: usize,
    /// Maximum registry rows decoded by one invocation. A caller should run an
    /// immediate follow-up when the report requests continuation.
    pub max_records: usize,
    /// Cooperative wall-clock budget for registry scanning. In-flight catalog
    /// and object-store requests are allowed to finish.
    pub max_duration: Duration,
}
impl Default for GarbagePolicy {
    fn default() -> Self {
        Self {
            grace: Duration::from_secs(24 * 60 * 60),
            max_objects: 1024,
            max_records: 64,
            max_duration: Duration::from_millis(250),
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
    pub examined_records: usize,
    pub examined_objects: usize,
    pub protected_objects: usize,
    pub delete_requests: usize,
    pub retired_records: usize,
    /// The current forward sweep stopped at a record or time budget. Schedule
    /// another pass without waiting for the periodic garbage interval.
    pub continuation_required: bool,
}

impl TableMaintenance {
    /// Run on the serialized table actor, after reconciliation and expiration.
    /// Each invocation scans one bounded registry page and intersects only its
    /// finite candidates with retained metadata. The durable cursor advances
    /// over protected and immature records too, so a sweep containing no
    /// eligible work terminates instead of spinning. No warehouse listing is
    /// performed.
    pub async fn collect_garbage(
        &self,
        table: &Table,
        table_id: TableId,
        policy: &GarbagePolicy,
        protection: &GarbageProtection,
    ) -> Result<GarbageReport> {
        ensure!(
            !policy.grace.is_zero()
                && policy.max_objects > 0
                && policy.max_records > 0
                && !policy.max_duration.is_zero(),
            "garbage collection needs positive grace and budgets"
        );
        let started = Instant::now();
        let head = self.catalog.load_table(table.identifier()).await?;
        let prefix = registry_prefix(head.metadata().uuid()).into_bytes();
        let cursor_key = format!("artifact-gc/v1/{}", head.metadata().uuid()).into_bytes();
        let store = self.store.clone();
        let limit = policy.max_records;
        let saved_cursor_key = cursor_key.clone();
        let protected_head = head.clone();
        let (cursor, indexed, pending, records, has_unseen_records, builds) = blocking(move || {
            let cursor = store.source_transaction(&saved_cursor_key)?;
            let mut entries = store.source_transactions_after(&prefix, cursor.as_deref());
            let records = entries
                .by_ref()
                .take(limit)
                .map(|entry| {
                    let (key, value) = entry?;
                    Ok((
                        key.to_vec(),
                        bincode::deserialize::<OwnedArtifacts>(&value)?,
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            let has_unseen_records = entries.next().transpose()?.is_some();
            drop(entries);
            Ok((
                cursor,
                store.table_state(&table_id)?,
                store.pending_operations()?,
                records,
                has_unseen_records,
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
        let page_records = records.len();
        let mut candidates = BTreeSet::new();
        let mut selected = Vec::new();
        let mut next_cursor = cursor;
        let mut stopped_early = false;
        let mut report = GarbageReport::default();
        for (position, (key, mut owner)) in records.into_iter().enumerate() {
            // Always examine one row when a page is non-empty. This guarantees
            // progress even when loading the catalog consumed the soft budget.
            if position > 0 && started.elapsed() >= policy.max_duration {
                stopped_early = true;
                break;
            }
            report.examined_records += 1;
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
                stopped_early = true;
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
                stopped_early = true;
                break;
            }
        }
        report.continuation_required =
            stopped_early || has_unseen_records || report.examined_records < page_records;

        if !selected.is_empty() {
            let protected = if candidates.is_empty() {
                BTreeSet::new()
            } else {
                retained_artifacts(&head, &candidates, &self.cache).await?
            };
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
            report.examined_objects = candidates.len();
            report.protected_objects = protected.len();
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
        }
        let continuation_required = report.continuation_required;
        let store = self.store.clone();
        blocking(move || {
            if continuation_required {
                if let Some(cursor) = next_cursor {
                    store.put_source_transaction(&cursor_key, &cursor)?;
                } else {
                    store.delete_source_transaction(&cursor_key)?;
                }
            } else {
                // Reaching the end completes this sweep. A future periodic pass
                // starts at the beginning, including records that were too young
                // or protected during this one.
                store.delete_source_transaction(&cursor_key)?;
            }
            Ok(())
        })
        .await?;
        Ok(report)
    }
}
