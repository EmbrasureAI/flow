//! Snapshot expiration. Rules are listed with the strongest first:
//!
//! 1. Snapshots Flow still needs are never expired: every branch and tag head,
//!    the indexed head, active build bases, and checkpoint and pending-operation
//!    bases with all of their descendants. A protected snapshot that another
//!    process has already expired is reported and its protection is dropped;
//!    its surviving descendants stay protected.
//! 2. The retain floor keeps the newest [`HistoryPolicy::retain_last`] snapshots
//!    of `main` (or of the current snapshot without a `main` ref), raised by the
//!    table's `history.expire.min-snapshots-to-keep` or `main`'s own
//!    `min-snapshots-to-keep`. Other branches keep their own minimum and
//!    maximum age.
//! 3. An explicit table `history.expire.max-snapshot-age-ms` keeps every
//!    snapshot inside that window, even above the count cap.
//! 4. The count cap expires the oldest remaining snapshots (by sequence number),
//!    whatever their age within Flow's own window, until at most
//!    [`HistoryPolicy::max_snapshots`] remain.
//! 5. Age expiration removes snapshots older than the older of Flow's window
//!    and the explicit table window, subject to the Iceberg per-ref policies.
//!    No per-branch window can shorten Flow's window.
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
use std::collections::{BTreeSet, HashMap};
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Repeated conditions are logged at most this often per table.
const LOG_INTERVAL: Duration = Duration::from_secs(10 * 60);

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
    /// Flow's reader window. Commit cost grows with the retained history.
    pub max_snapshots: usize,
}

/// What expiration would remove from one table's metadata now.
#[derive(Debug, Clone)]
pub struct HistoryPlan {
    cutoff: i64,
    reader_since: i64,
    protected: BTreeSet<i64>,
    /// Selected by the count cap, possibly inside the reader window.
    forced: BTreeSet<i64>,
    /// Unprotected and older than the cutoff.
    aged: BTreeSet<i64>,
    missing: Vec<i64>,
    snapshots: usize,
    max_snapshots: usize,
    over_cap: usize,
    held_by_table_policy: bool,
}

impl HistoryPlan {
    /// Snapshots expiration would remove.
    pub fn expirable(&self) -> usize {
        self.forced.union(&self.aged).count()
    }

    /// Expirable history amounts to 1/16 of the retained history (at least one
    /// snapshot), so a steady commit stream does not add an expiration commit
    /// to every epoch. Protected snapshots are never counted.
    pub fn due(&self) -> bool {
        self.expirable() >= (self.snapshots.min(self.max_snapshots) / 16).max(1)
    }

    /// The count cap can remove more than 1/16 of the cap (at least one
    /// snapshot). Expiration is then mandatory, even under source pressure.
    pub fn over_limit(&self) -> bool {
        self.forced.len() > (self.max_snapshots / 16).max(1)
    }

    /// Snapshots above the cap that protection or an explicit table window
    /// retain.
    pub fn over_cap(&self) -> usize {
        self.over_cap
    }

    /// An explicit table `history.expire.max-snapshot-age-ms` keeps the table
    /// above the cap.
    pub fn held_by_table_policy(&self) -> bool {
        self.held_by_table_policy
    }

    pub fn snapshots(&self) -> usize {
        self.snapshots
    }
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
            "limits.snapshot_retention_secs must be positive"
        );
        ensure!(
            self.retain_last > 0,
            "limits.snapshot_retain_last must be positive"
        );
        ensure!(
            self.max_snapshots >= self.retain_last,
            "limits.snapshot_max_count must be at least limits.snapshot_retain_last"
        );
        Ok(())
    }

    fn table_window_ms(metadata: &TableMetadata) -> Result<Option<i64>> {
        if !metadata
            .properties()
            .contains_key(TableProperties::PROPERTY_MAX_SNAPSHOT_AGE_MS)
        {
            return Ok(None);
        }
        Ok(Some(
            metadata.table_properties()?.max_snapshot_age_ms.max(0),
        ))
    }

    /// The older of Flow's cutoff and the table's explicit maximum age.
    pub fn cutoff_ms(&self, metadata: &TableMetadata, now_ms: i64) -> Result<i64> {
        let window = i64::try_from(self.retention.as_millis()).unwrap_or(i64::MAX);
        let cutoff = now_ms.saturating_sub(window);
        Ok(match Self::table_window_ms(metadata)? {
            Some(table) => cutoff.min(now_ms.saturating_sub(table)),
            None => cutoff,
        })
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

    /// Plan expiration against `metadata`. `bases` are snapshots that must be
    /// kept together with all of their descendants.
    pub fn plan(
        &self,
        metadata: &TableMetadata,
        bases: &BTreeSet<i64>,
        now_ms: i64,
    ) -> Result<HistoryPlan> {
        self.validate()?;
        let properties = metadata.table_properties()?;
        let sequence = |id: i64| metadata.snapshot_by_id(id).map(|s| s.sequence_number());
        let timestamp = |id: i64| metadata.snapshot_by_id(id).map(|s| s.timestamp_ms());
        let missing: Vec<i64> = bases
            .iter()
            .copied()
            .filter(|id| metadata.snapshot_by_id(*id).is_none())
            .collect();
        // A missing base's surviving children still carry its descendants.
        let oldest = bases
            .iter()
            .filter_map(|id| sequence(*id))
            .chain(
                metadata
                    .snapshots()
                    .filter(|snapshot| {
                        snapshot
                            .parent_snapshot_id()
                            .is_some_and(|parent| missing.contains(&parent))
                    })
                    .map(|snapshot| snapshot.sequence_number()),
            )
            .min();
        let mut protected: BTreeSet<i64> = metadata
            .snapshot_references()
            .map(|(_, reference)| reference.snapshot_id)
            .collect();
        protected.extend(metadata.current_snapshot_id());
        protected.extend(bases.iter().filter(|id| sequence(**id).is_some()));
        if let Some(oldest) = oldest {
            protected.extend(
                metadata
                    .snapshots()
                    .filter(|snapshot| snapshot.sequence_number() >= oldest)
                    .map(|snapshot| snapshot.snapshot_id()),
            );
        }
        // Retain floor and per-branch policy. They are enforced here rather
        // than through the action's `retain_last`, so neither the cap nor a
        // smaller per-ref minimum on `main` can go below the floor.
        let main_floor = self.main_floor(metadata, properties.min_snapshots_to_keep);
        let mut has_main = false;
        for (name, reference) in metadata.snapshot_references() {
            let SnapshotRetention::Branch {
                min_snapshots_to_keep,
                max_snapshot_age_ms,
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
                keep = main_floor;
            }
            let branch_cutoff = max_snapshot_age_ms.map(|age| now_ms.saturating_sub(age));
            for (position, id) in ancestors(metadata, reference.snapshot_id).enumerate() {
                if position < keep
                    || branch_cutoff
                        .zip(timestamp(id))
                        .is_some_and(|(cutoff, timestamp)| timestamp >= cutoff)
                {
                    protected.insert(id);
                }
            }
        }
        if !has_main && let Some(current) = metadata.current_snapshot_id() {
            protected.extend(ancestors(metadata, current).take(main_floor));
        }
        let cutoff = self.cutoff_ms(metadata, now_ms)?;
        // Count cap, oldest first by sequence number: timestamps can be skewed
        // and must not punch holes in the ancestry of `main`.
        let table_since = Self::table_window_ms(metadata)?.map(|age| now_ms.saturating_sub(age));
        let mut open: Vec<_> = metadata
            .snapshots()
            .filter(|snapshot| !protected.contains(&snapshot.snapshot_id()))
            .map(|snapshot| {
                (
                    snapshot.sequence_number(),
                    snapshot.snapshot_id(),
                    snapshot.timestamp_ms(),
                )
            })
            .collect();
        open.sort_unstable();
        let snapshots = metadata.snapshots().len();
        let excess = snapshots.saturating_sub(self.max_snapshots);
        let forced: BTreeSet<i64> = open
            .iter()
            .filter(|(_, _, timestamp)| table_since.is_none_or(|since| *timestamp < since))
            .take(excess)
            .map(|(_, id, _)| *id)
            .collect();
        let held_by_table_policy = forced.len() < excess.min(open.len());
        // The cap may select snapshots inside Flow's reader window. Everything
        // else in that window stays protected, even under a shorter per-branch
        // window, and snapshots committed during a retry are newer still.
        let reader_since = forced
            .iter()
            .filter_map(|id| timestamp(*id))
            .map(|timestamp| timestamp.saturating_add(1))
            .fold(cutoff, i64::max);
        protected.extend(
            metadata
                .snapshots()
                .filter(|snapshot| {
                    snapshot.timestamp_ms() >= cutoff && !forced.contains(&snapshot.snapshot_id())
                })
                .map(|snapshot| snapshot.snapshot_id()),
        );
        let aged = metadata
            .snapshots()
            .map(|snapshot| snapshot.snapshot_id())
            .filter(|id| !protected.contains(id) && !forced.contains(id))
            .collect();
        Ok(HistoryPlan {
            cutoff,
            reader_since,
            protected,
            aged,
            missing,
            snapshots,
            max_snapshots: self.max_snapshots,
            over_cap: excess.saturating_sub(forced.len()),
            held_by_table_policy,
            forced,
        })
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
    .take(metadata.snapshots().len())
}

/// Whether a repeated condition should be logged again for this table.
fn log_due(table_id: TableId, event: &'static str, every: Option<Duration>) -> bool {
    static LOGGED: Mutex<Option<HashMap<(u32, &'static str), Instant>>> = Mutex::new(None);
    let Ok(mut logged) = LOGGED.lock() else {
        return true;
    };
    let logged = logged.get_or_insert_with(HashMap::new);
    let now = Instant::now();
    match logged.get(&(table_id.0, event)) {
        Some(last) if every.is_none_or(|every| now.duration_since(*last) < every) => false,
        _ => {
            logged.insert((table_id.0, event), now);
            true
        }
    }
}

impl TableMaintenance {
    /// Plan expiration of `head` with the index, operation and build
    /// protections the table actor knows about. `None` when expiration must
    /// not run: `gc.enabled=false`, or an unresolved initial operation.
    /// Also publishes the table's history gauges.
    pub async fn history_plan(
        &self,
        head: &Table,
        table_id: TableId,
        policy: &HistoryPolicy,
        protected_bases: &BTreeSet<i64>,
    ) -> Result<Option<HistoryPlan>> {
        let label = table_id.0.to_string();
        let metadata = head.metadata();
        // Expiring metadata defeats an explicit decision to disable GC, and
        // Iceberg refuses it. Garbage collection skips these tables too.
        let disabled = !metadata.table_properties()?.gc_enabled;
        metrics::gauge!("flow_snapshot_expiration_disabled", "table_id" => label.clone())
            .set(f64::from(u8::from(disabled)));
        if disabled {
            if log_due(table_id, "snapshot_expiration_disabled", None) {
                tracing::warn!(
                    event = "snapshot_expiration_disabled",
                    table_id = table_id.0,
                    snapshots = metadata.snapshots().len(),
                    "gc.enabled=false: snapshot history of this table is never expired by Flow"
                );
            }
            return Ok(None);
        }
        let store = self.store.clone();
        let (indexed, pending) =
            blocking(move || Ok((store.table_state(&table_id)?, store.pending_operations()?)))
                .await?;
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
            return Ok(None);
        }
        let mut bases = protected_bases.clone();
        bases.extend(indexed.snapshot_id);
        bases.extend(active_build_protection(&self.store, head, table_id)?.snapshots);
        for record in &pending {
            bases.extend(record.operation.base_snapshot_id);
            bases.extend(record.snapshot_id);
            if let Some(snapshot) = find_operation(metadata, &record.operation.id.0) {
                bases.insert(snapshot.snapshot_id());
            }
        }
        let plan = policy.plan(metadata, &bases, now_ms()?)?;
        if !plan.missing.is_empty()
            && log_due(
                table_id,
                "expiration_protection_missing",
                Some(LOG_INTERVAL),
            )
        {
            tracing::warn!(
                event = "expiration_protection_missing",
                table_id = table_id.0,
                snapshots = ?plan.missing,
                "protected snapshots were already expired by another process; dropping their protection"
            );
        }
        metrics::gauge!("flow_snapshots_over_cap", "table_id" => label.clone())
            .set(plan.over_cap as f64);
        metrics::gauge!("flow_snapshot_cap_exceeded_by_table_policy", "table_id" => label)
            .set(f64::from(u8::from(plan.held_by_table_policy)));
        if plan.held_by_table_policy
            && log_due(
                table_id,
                "snapshot_cap_exceeded_by_table_policy",
                Some(LOG_INTERVAL),
            )
        {
            tracing::warn!(
                event = "snapshot_cap_exceeded_by_table_policy",
                table_id = table_id.0,
                snapshots = plan.snapshots,
                limit = policy.max_snapshots,
                "history.expire.max-snapshot-age-ms keeps more snapshots than limits.snapshot_max_count; every commit rewrites that larger metadata"
            );
        }
        Ok(Some(plan))
    }

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
        let store = self.store.clone();
        let indexed = blocking(move || Ok(store.table_state(&table_id)?)).await?;
        ensure!(
            indexed.snapshot_id == head.metadata().current_snapshot_id(),
            "reconcile the index before expiring history"
        );
        let Some(plan) = self
            .history_plan(&head, table_id, policy, protected_bases)
            .await?
        else {
            return Ok(0);
        };
        if !plan.missing.is_empty() {
            metrics::counter!("flow_expiration_missing_protections_total",
                "table_id" => table_id.0.to_string())
            .increment(plan.missing.len() as u64);
        }
        if plan.forced.is_empty() && plan.aged.is_empty() {
            return Ok(0);
        }
        let retained: BTreeSet<_> = head
            .metadata()
            .snapshots()
            .map(|snapshot| snapshot.snapshot_id())
            .collect();
        artifacts::register_catalog_metadata(&self.store, &head, table_id).await?;
        let transaction = Transaction::new(&head);
        let updated = transaction
            .expire_snapshots()
            .expire_older_than_ms(plan.cutoff)
            .expire_snapshot_ids(plan.forced.iter().copied())
            .protect_snapshots(plan.protected.iter().copied())
            .protect_newer_than_ms(plan.reader_since)
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
            capped = plan.forced.len(),
            retained = updated.metadata().snapshots().len(),
            "expired snapshot history"
        );
        Ok(expired)
    }
}
