//! Snapshot expiration. Rules are listed with the strongest first:
//!
//! 1. Snapshots Flow still needs are never expired: every branch and tag head,
//!    the indexed head, active build bases, and checkpoint and pending-operation
//!    bases with all of their descendants. A protected snapshot that another
//!    process has already expired is reported and its protection is dropped.
//! 2. The retain floor keeps the newest [`HistoryPolicy::retain_last`] snapshots
//!    of `main` (or of the current snapshot without a `main` ref), raised by the
//!    table's `history.expire.min-snapshots-to-keep` or `main`'s own
//!    `min-snapshots-to-keep`. Other branches keep their own minimum.
//! 3. The count cap expires the oldest remaining snapshots, whatever their age,
//!    until at most [`HistoryPolicy::max_snapshots`] remain.
//! 4. Age expiration removes snapshots older than the older of Flow's window
//!    and an explicit table `history.expire.max-snapshot-age-ms`, subject to the
//!    Iceberg per-ref policies. No per-branch window can shorten Flow's window.
//!
//! Tables with `gc.enabled=false` are never expired. Expiration only rewrites
//! metadata; registered garbage collection deletes files later.

use super::{TableMaintenance, builds::active_build_protection};
use crate::{artifacts, blocking};
use anyhow::{Result, ensure};
use flow_iceberg_ext::find_operation;
use flow_model::TableId;
use iceberg::spec::{MAIN_BRANCH, SnapshotRetention, TableMetadata, TableProperties};
use iceberg::table::Table;
use std::collections::BTreeSet;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Flow's snapshot retention for one table. See the module documentation for
/// how it combines with protected snapshots and table properties.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoryPolicy {
    /// Minimum reader window. A longer explicit table
    /// `history.expire.max-snapshot-age-ms` takes precedence.
    pub retention: Duration,
    /// Never expire the newest snapshots of `main` below this count.
    pub retain_last: usize,
    /// Expire the oldest unprotected snapshots beyond this count, even inside
    /// the reader window. Commit cost grows with the retained history.
    pub max_snapshots: usize,
}

impl HistoryPolicy {
    /// Age-only expiration that keeps at least the current snapshot.
    pub fn window(retention: Duration) -> Self {
        Self {
            retention,
            retain_last: 1,
            max_snapshots: usize::MAX,
        }
    }

    pub fn validate(&self) -> Result<()> {
        ensure!(
            !self.retention.is_zero(),
            "snapshot retention needs a positive reader window"
        );
        ensure!(
            self.retain_last > 0 && self.max_snapshots >= self.retain_last,
            "snapshot history needs 0 < retain_last <= max_snapshots"
        );
        Ok(())
    }

    /// The older of Flow's cutoff and the table's explicit maximum age.
    pub fn cutoff_ms(&self, metadata: &TableMetadata, now_ms: i64) -> Result<i64> {
        let window = i64::try_from(self.retention.as_millis()).unwrap_or(i64::MAX);
        let mut cutoff = now_ms.saturating_sub(window);
        if metadata
            .properties()
            .contains_key(TableProperties::PROPERTY_MAX_SNAPSHOT_AGE_MS)
        {
            let table = metadata.table_properties()?.max_snapshot_age_ms.max(0);
            cutoff = cutoff.min(now_ms.saturating_sub(table));
        }
        Ok(cutoff)
    }

    /// Whether expiration could remove anything. Protected snapshots are only
    /// known to the table actor, so this may be a false positive.
    pub fn due(&self, metadata: &TableMetadata) -> Result<bool> {
        let properties = metadata.table_properties()?;
        if !properties.gc_enabled {
            return Ok(false);
        }
        let count = metadata.snapshots().len();
        if count > self.max_snapshots {
            return Ok(true);
        }
        if count <= self.main_floor(metadata, properties.min_snapshots_to_keep) {
            return Ok(false);
        }
        let cutoff = self.cutoff_ms(metadata, now_ms()?)?;
        Ok(metadata
            .snapshots()
            .any(|snapshot| snapshot.timestamp_ms() < cutoff))
    }

    /// History is far enough past the cap that expiration must run even when
    /// optional maintenance is suppressed. The slack (1/16 of the cap, at least
    /// one snapshot) keeps this from adding a catalog commit to every epoch.
    pub fn over_limit(&self, metadata: &TableMetadata) -> Result<bool> {
        let slack = (self.max_snapshots / 16).max(1);
        Ok(metadata.table_properties()?.gc_enabled
            && metadata.snapshots().len() > self.max_snapshots.saturating_add(slack))
    }

    fn main_floor(&self, metadata: &TableMetadata, table_minimum: usize) -> usize {
        let reference = metadata
            .snapshot_references()
            .find(|(name, _)| *name == MAIN_BRANCH)
            .and_then(|(_, reference)| match reference.retention {
                SnapshotRetention::Branch {
                    min_snapshots_to_keep,
                    ..
                } => min_snapshots_to_keep.and_then(|count| usize::try_from(count).ok()),
                SnapshotRetention::Tag { .. } => None,
            });
        self.retain_last
            .max(table_minimum)
            .max(reference.unwrap_or(0))
    }
}

fn now_ms() -> Result<i64> {
    Ok(i64::try_from(
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis(),
    )?)
}

/// A snapshot and its ancestors, newest first.
fn ancestors(metadata: &TableMetadata, head: i64) -> impl Iterator<Item = i64> + '_ {
    let mut next = Some(head);
    std::iter::from_fn(move || {
        let id = next?;
        next = metadata
            .snapshot_by_id(id)
            .and_then(|snapshot| snapshot.parent_snapshot_id());
        Some(id)
    })
}

impl TableMaintenance {
    /// Expire metadata snapshots under `policy`. No object deletion is
    /// performed. Call from the table actor and include any active worker bases
    /// or index checkpoints whose descendant history is still needed.
    /// The caller must coordinate external reference creation and retention-policy
    /// changes: REST assertions protect existing heads, not the entire reference set.
    pub async fn expire_history(
        &self,
        table: &Table,
        table_id: TableId,
        policy: &HistoryPolicy,
        protected_bases: &BTreeSet<i64>,
    ) -> Result<usize> {
        use iceberg::transaction::{ApplyTransactionAction, Transaction};
        policy.validate()?;
        let head = self.catalog.load_table(table.identifier()).await?;
        let metadata = head.metadata();
        let properties = metadata.table_properties()?;
        // Expiring metadata defeats an explicit decision to disable GC, and
        // Iceberg refuses it. Garbage collection skips these tables too.
        if !properties.gc_enabled {
            return Ok(0);
        }
        let store = self.store.clone();
        let (indexed, pending) =
            blocking(move || Ok((store.table_state(&table_id)?, store.pending_operations()?)))
                .await?;
        ensure!(
            indexed.snapshot_id == metadata.current_snapshot_id(),
            "reconcile the index before expiring history"
        );
        let pending: Vec<_> = pending
            .into_iter()
            .filter(|record| record.operation.table_id == table_id)
            .collect();
        // Initial-copy/first-publication recovery may need to prove an operation
        // was never committed anywhere in the entire original lineage.
        if pending
            .iter()
            .any(|record| record.operation.base_snapshot_id.is_none())
        {
            return Ok(0);
        }
        let mut bases = protected_bases.clone();
        bases.extend(active_build_protection(&self.store, &head, table_id)?.snapshots);
        for record in &pending {
            if let Some(base) = record.operation.base_snapshot_id {
                bases.insert(base);
            }
            if let Some(snapshot) = record.snapshot_id {
                bases.insert(snapshot);
            }
            if let Some(snapshot) = find_operation(metadata, &record.operation.id.0) {
                bases.insert(snapshot.snapshot_id());
            }
        }
        // Another process may already have expired a protected snapshot. Its
        // history is gone either way; failing would only stop maintenance.
        let missing: Vec<i64> = bases
            .iter()
            .copied()
            .filter(|id| metadata.snapshot_by_id(*id).is_none())
            .collect();
        if !missing.is_empty() {
            metrics::counter!("flow_expiration_missing_protections_total",
                "table_id" => table_id.0.to_string())
            .increment(missing.len() as u64);
            tracing::warn!(
                event = "expiration_protection_missing",
                table_id = table_id.0,
                snapshots = ?missing,
                "protected snapshots were already expired by another process; dropping their protection"
            );
            bases.retain(|id| !missing.contains(id));
        }
        let oldest_sequence = bases
            .iter()
            .filter_map(|id| metadata.snapshot_by_id(*id))
            .map(|snapshot| snapshot.sequence_number())
            .min();
        let mut protected: BTreeSet<_> = metadata
            .snapshot_references()
            .map(|(_, reference)| reference.snapshot_id)
            .collect();
        protected.extend(indexed.snapshot_id);
        protected.extend(bases);
        if let Some(oldest) = oldest_sequence {
            protected.extend(
                metadata
                    .snapshots()
                    .filter(|snapshot| snapshot.sequence_number() >= oldest)
                    .map(|snapshot| snapshot.snapshot_id()),
            );
        }
        // Retain floor. It is enforced here rather than through the action's
        // `retain_last` so that neither the cap nor a smaller per-ref minimum
        // on `main` can go below it.
        let mut has_main = false;
        for (name, reference) in metadata.snapshot_references() {
            let SnapshotRetention::Branch {
                min_snapshots_to_keep,
                ..
            } = reference.retention
            else {
                continue;
            };
            let mut keep = min_snapshots_to_keep
                .and_then(|count| usize::try_from(count).ok())
                .unwrap_or(properties.min_snapshots_to_keep);
            if name == MAIN_BRANCH {
                has_main = true;
                keep = policy.main_floor(metadata, properties.min_snapshots_to_keep);
            }
            protected.extend(ancestors(metadata, reference.snapshot_id).take(keep));
        }
        if !has_main && let Some(current) = metadata.current_snapshot_id() {
            protected.extend(
                ancestors(metadata, current)
                    .take(policy.main_floor(metadata, properties.min_snapshots_to_keep)),
            );
        }
        let now = now_ms()?;
        let cutoff = policy.cutoff_ms(metadata, now)?;
        // Count cap: the oldest unprotected snapshots, regardless of age.
        let excess = metadata
            .snapshots()
            .len()
            .saturating_sub(policy.max_snapshots);
        let mut candidates: Vec<_> = metadata
            .snapshots()
            .filter(|snapshot| !protected.contains(&snapshot.snapshot_id()))
            .map(|snapshot| {
                (
                    snapshot.timestamp_ms(),
                    snapshot.sequence_number(),
                    snapshot.snapshot_id(),
                )
            })
            .collect();
        candidates.sort_unstable();
        let forced: BTreeSet<i64> = candidates
            .into_iter()
            .take(excess)
            .map(|(_, _, id)| id)
            .collect();
        // The cap may select snapshots inside the reader window. Everything
        // else in that window stays protected, even under a shorter per-branch
        // window, and snapshots committed during a retry are newer still.
        let reader_since = forced
            .iter()
            .filter_map(|id| metadata.snapshot_by_id(*id))
            .map(|snapshot| snapshot.timestamp_ms().saturating_add(1))
            .fold(cutoff, i64::max);
        protected.extend(
            metadata
                .snapshots()
                .filter(|snapshot| {
                    snapshot.timestamp_ms() >= cutoff && !forced.contains(&snapshot.snapshot_id())
                })
                .map(|snapshot| snapshot.snapshot_id()),
        );
        if forced.is_empty()
            && metadata
                .snapshots()
                .all(|snapshot| protected.contains(&snapshot.snapshot_id()))
        {
            return Ok(0);
        }
        let retained: BTreeSet<_> = metadata
            .snapshots()
            .map(|snapshot| snapshot.snapshot_id())
            .collect();
        artifacts::register_catalog_metadata(&self.store, &head, table_id).await?;
        let transaction = Transaction::new(&head);
        let updated = transaction
            .expire_snapshots()
            .expire_older_than_ms(cutoff)
            .expire_snapshot_ids(forced.iter().copied())
            .protect_snapshots(protected)
            .protect_newer_than_ms(reader_since)
            .apply(transaction)?
            .commit(self.catalog.as_ref())
            .await?;
        let expired = retained
            .into_iter()
            .filter(|id| updated.metadata().snapshot_by_id(*id).is_none())
            .count();
        tracing::info!(
            event = "snapshot_history_expired",
            table_id = table_id.0,
            expired,
            capped = forced.len(),
            retained = updated.metadata().snapshots().len(),
            "expired snapshot history"
        );
        Ok(expired)
    }
}
