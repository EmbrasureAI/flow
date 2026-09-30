use super::{TableMaintenance, builds::active_build_protection};
use crate::artifacts::{METADATA_IMPORT, OwnedArtifacts, now_ms, registry_prefix};
use crate::{blocking, publication::ReplanRequired};
use anyhow::{Result, ensure};
use flow_iceberg_ext::{RetainedIndex, retained_artifacts};
use flow_model::{OperationId, TableId};
use flow_state_store::CheckpointRecord;
use futures::{StreamExt, TryStreamExt, stream};
use iceberg::table::Table;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeSet, HashMap, HashSet},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

/// Object grace must cover readers that already planned an expired snapshot and
/// the maximum duration of an interrupted upload. Only registered objects qualify.
#[derive(Debug, Clone)]
pub struct GarbagePolicy {
    /// Minimum time an object must stay unreferenced before deletion. The
    /// clock starts when a sweep first observes it unreferenced by retained
    /// metadata after its operation fence was released, which is no earlier
    /// than the moment a reader could last have planned it.
    pub grace: Duration,
    /// The same clock for catalog metadata JSON once it has left the current
    /// pointer and the catalog metadata log. Readers load the current JSON, so
    /// this can be shorter than `grace`; it breaks consumers that use an old
    /// JSON by location. JSON adopted by `metadata-import` uses `grace`.
    pub metadata_grace: Duration,
    pub max_objects: usize,
    /// Maximum registry rows decoded by one invocation. A caller should run an
    /// immediate follow-up when the report requests continuation.
    pub max_records: usize,
    /// Maximum object deletions per invocation, each preceded by an existence
    /// check. Requests run concurrently in small batches.
    pub max_deletes: usize,
    /// Cooperative wall-clock budget for registry selection and reachability
    /// indexing. In-flight catalog and object-store requests finish.
    pub max_duration: Duration,
}
impl Default for GarbagePolicy {
    fn default() -> Self {
        Self {
            grace: Duration::from_secs(24 * 60 * 60),
            metadata_grace: Duration::from_secs(60 * 60),
            max_objects: 4096,
            max_records: 512,
            max_deletes: 64,
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
    /// Unreferenced objects still inside their grace.
    pub deferred_objects: usize,
    pub delete_requests: usize,
    pub metadata_json_delete_requests: usize,
    pub retired_records: usize,
    /// Records moved to a later position of the due queue.
    pub rescheduled_records: usize,
    /// Manifest lists read to extend the retained-artifact index.
    pub manifest_list_reads: usize,
    /// The current forward sweep stopped at a record or time budget. Schedule
    /// another pass without waiting for the periodic garbage interval.
    pub continuation_required: bool,
}

/// Registration keeps its existing keys: `owned-artifacts/v1/` records of
/// older engines and random or `catalog-` keys under `v2/`. The collector
/// examines each such record once and then moves it to the due queue under
/// `v2/{table}/~/`, ordered by the earliest time any of its objects can change
/// state. A sweep reads unordered records, then the queue only up to its start
/// time, so its cost follows new and due work rather than registry size. The
/// queue stays inside `v2/`, whose records a rolled-back engine still reads.
const QUEUE: &str = "~/";
const DUE_DIGITS: usize = 20;
/// Referenced objects are rechecked after half their age, within these bounds
/// and never later than their grace. Observation only starts the grace clock,
/// so a late recheck delays deletion and cannot shorten a reader's grace.
const MIN_RECHECK: Duration = Duration::from_secs(60);
const MAX_RECHECK: Duration = Duration::from_secs(60 * 60);
const DELETE_CONCURRENCY: usize = 16;
/// Estimated memory for all tables' retained-artifact indexes. A table that
/// alone exceeds it falls back to per-page manifest walks.
pub(super) const RETAINED_INDEX_BYTES: usize = 128 << 20;
const OVERSIZED_RETRY: Duration = Duration::from_secs(60 * 60);
/// A partial index that yielded the budget to another build waits this long,
/// walking manifests per page, before it starts again.
const YIELDED_RETRY: Duration = Duration::from_secs(5 * 60);

#[derive(Serialize, Deserialize)]
struct Sweep {
    started_ms: u64,
    after: Option<Vec<u8>>,
}

#[derive(Default)]
struct TableIndex {
    index: RetainedIndex,
    retry_after: Option<Instant>,
}
struct IndexSlot {
    index: Arc<tokio::sync::Mutex<TableIndex>>,
    bytes: usize,
    complete: bool,
    used: Instant,
}

#[derive(Default)]
struct IndexSlots {
    tables: HashMap<uuid::Uuid, IndexSlot>,
    /// The one partial index allowed to keep building when the budget is
    /// tight. Other partial builds yield instead of evicting each other.
    builder: Option<uuid::Uuid>,
}

/// Per-process reachability indexes shared by all tables' collection pages.
/// They are rebuilt after restart; nothing here is durable.
pub(super) struct RetainedIndexes {
    budget: usize,
    slots: Mutex<IndexSlots>,
}
impl RetainedIndexes {
    pub(super) fn new(budget: usize) -> Self {
        Self {
            budget,
            slots: Mutex::default(),
        }
    }
    fn slot(&self, table: uuid::Uuid) -> Result<Arc<tokio::sync::Mutex<TableIndex>>> {
        let mut slots = self
            .slots
            .lock()
            .map_err(|_| anyhow::anyhow!("retained index lock poisoned"))?;
        let slot = slots.tables.entry(table).or_insert_with(|| IndexSlot {
            index: Arc::default(),
            bytes: 0,
            complete: false,
            used: Instant::now(),
        });
        slot.used = Instant::now();
        Ok(slot.index.clone())
    }
    fn forget_builder(&self, table: uuid::Uuid) -> Result<()> {
        let mut slots = self
            .slots
            .lock()
            .map_err(|_| anyhow::anyhow!("retained index lock poisoned"))?;
        if slots.builder == Some(table) {
            slots.builder = None;
        }
        if let Some(slot) = slots.tables.get_mut(&table) {
            slot.bytes = 0;
            slot.complete = false;
        }
        Ok(())
    }
    /// Record a table's size and evict least recently used complete indexes
    /// of other tables. Partial indexes are evicted only for a complete one,
    /// and never the current builder, so concurrent builds cannot thrash each
    /// other. Returns false when this table's partial index must yield: the
    /// caller clears it and walks manifests for a while.
    fn account(&self, table: uuid::Uuid, bytes: usize, complete: bool) -> Result<bool> {
        let mut slots = self
            .slots
            .lock()
            .map_err(|_| anyhow::anyhow!("retained index lock poisoned"))?;
        if let Some(slot) = slots.tables.get_mut(&table) {
            slot.bytes = bytes;
            slot.complete = complete;
        }
        if complete && slots.builder == Some(table) {
            slots.builder = None;
        }
        if !complete && slots.builder.is_none() {
            slots.builder = Some(table);
        }
        while slots.tables.values().map(|slot| slot.bytes).sum::<usize>() > self.budget {
            let builder = slots.builder;
            let victim = |partial: bool| {
                slots
                    .tables
                    .iter()
                    .filter(|(id, slot)| {
                        **id != table && slot.complete != partial && Some(**id) != builder
                    })
                    .min_by_key(|(_, slot)| slot.used)
                    .map(|(id, _)| *id)
            };
            let victim = victim(false).or_else(|| if complete { victim(true) } else { None });
            if let Some(victim) = victim {
                slots.tables.remove(&victim);
                continue;
            }
            if !complete && builder != Some(table) {
                if let Some(slot) = slots.tables.get_mut(&table) {
                    slot.bytes = 0;
                }
                return Ok(false);
            }
            // Only the builder or this complete index remains over budget.
            // `RetainedIndex::sync` bounds each table by the whole budget.
            break;
        }
        Ok(true)
    }
}

/// Reference oracle for one page: catalog JSON from the head, plus either the
/// complete incremental index or a walk intersected with this page's candidates.
enum Referenced {
    Index(tokio::sync::OwnedMutexGuard<TableIndex>),
    Walk(BTreeSet<String>),
}
impl Referenced {
    fn contains(&self, path: &str) -> bool {
        match self {
            Self::Index(table) => table.index.contains(path),
            Self::Walk(protected) => protected.contains(path),
        }
    }
}

struct Selected {
    key: Vec<u8>,
    owner: OwnedArtifacts,
    end: u64,
    retained: bool,
    next_due: Option<u64>,
    finished: bool,
}

fn queue_prefix(table: uuid::Uuid) -> String {
    format!("{}{QUEUE}", registry_prefix(table))
}
/// The due time of a queue key. Unparseable queue keys are due immediately.
fn queue_due(key: &[u8], queue: &[u8]) -> Option<u64> {
    let rest = key.strip_prefix(queue)?;
    Some(
        rest.get(..DUE_DIGITS)
            .and_then(|digits| std::str::from_utf8(digits).ok())
            .and_then(|digits| digits.parse().ok())
            .unwrap_or(0),
    )
}
/// A stable identity across queue moves; unordered keys map deterministically.
fn record_id(key: &[u8], queue: &[u8], table: uuid::Uuid) -> String {
    if let Some(id) = key
        .strip_prefix(queue)
        .and_then(|rest| rest.get(DUE_DIGITS + 1..))
        .filter(|id| !id.is_empty())
        .and_then(|id| std::str::from_utf8(id).ok())
    {
        return id.to_owned();
    }
    uuid::Uuid::new_v5(&table, key).to_string()
}
fn marker_key(table: uuid::Uuid, path: &str) -> Vec<u8> {
    format!(
        "artifact-unreferenced/v1/{table}/{}",
        uuid::Uuid::new_v5(&table, path.as_bytes())
    )
    .into_bytes()
}
fn is_metadata_json(path: &str) -> bool {
    path.ends_with(".metadata.json")
}
fn millis(duration: Duration) -> u64 {
    duration.as_millis().try_into().unwrap_or(u64::MAX)
}

impl TableMaintenance {
    /// Bound the process-wide memory estimate of retained-artifact indexes.
    pub fn with_retained_index_budget(mut self, bytes: usize) -> Self {
        self.garbage = RetainedIndexes::new(bytes);
        self
    }

    /// Run on the serialized table actor, after reconciliation and expiration.
    /// Each invocation processes one bounded page of unordered and due registry
    /// records. Reachability comes from an incremental in-memory index of the
    /// retained snapshots, so a page reads only manifest lists committed since
    /// the previous page. The durable cursor advances over protected records
    /// too, so a sweep without eligible work terminates instead of spinning.
    /// No warehouse listing is performed.
    pub async fn collect_garbage(
        &self,
        table: &Table,
        table_id: TableId,
        policy: &GarbagePolicy,
        protection: &GarbageProtection,
    ) -> Result<GarbageReport> {
        ensure!(
            !policy.grace.is_zero()
                && !policy.metadata_grace.is_zero()
                && policy.max_objects > 0
                && policy.max_records > 0
                && policy.max_deletes > 0
                && !policy.max_duration.is_zero(),
            "garbage collection needs positive grace and budgets"
        );
        let started = Instant::now();
        let deadline = started + policy.max_duration;
        let head = self.catalog.load_table(table.identifier()).await?;
        if !head.metadata().table_properties()?.gc_enabled {
            return Ok(GarbageReport::default());
        }
        let uuid = head.metadata().uuid();
        let now = now_ms()?;
        let queue = queue_prefix(uuid).into_bytes();
        let prefix = registry_prefix(uuid).into_bytes();
        let legacy_prefix = format!("owned-artifacts/v1/{uuid}/").into_bytes();
        let cursor_key = format!("artifact-gc/v2/{uuid}").into_bytes();
        let store = self.store.clone();
        let limit = policy.max_records;
        let saved_cursor_key = cursor_key.clone();
        let saved_queue = queue.clone();
        let protected_head = head.clone();
        // No schedule legitimately exceeds the larger grace plus a recheck.
        // Later due times come from a clock that ran ahead; treat them as due.
        let horizon = millis(policy.grace.max(policy.metadata_grace) + MAX_RECHECK);
        let (sweep, indexed, pending, records, has_unseen_records, builds) = blocking(move || {
            let sweep = match store.source_transaction(&saved_cursor_key)? {
                Some(bytes) => bincode::deserialize::<Sweep>(&bytes)?,
                None => Sweep {
                    started_ms: now,
                    after: None,
                },
            };
            // v1 records remain readable across upgrades; new v2 records are
            // invisible to pre-JSON-protection engines after a rollback. The
            // full key cursor orders v1, unordered v2 and the v2 queue.
            let entries = store
                .source_transactions_after(&legacy_prefix, sweep.after.as_deref())
                .chain(store.source_transactions_after(&prefix, sweep.after.as_deref()));
            let parked_after = sweep.started_ms.saturating_add(horizon);
            let parked = |due: u64| due > parked_after;
            let mut records = Vec::new();
            let mut has_unseen_records = false;
            let mut reached_schedule = false;
            for entry in entries {
                let (key, value) = entry?;
                // Queue keys are ordered by due time. Records moved during this
                // sweep are due after its start, which bounds the sweep.
                if queue_due(&key, &saved_queue)
                    .is_some_and(|due| due > sweep.started_ms && !parked(due))
                {
                    reached_schedule = true;
                    break;
                }
                if records.len() == limit {
                    has_unseen_records = true;
                    break;
                }
                records.push((
                    key.to_vec(),
                    bincode::deserialize::<OwnedArtifacts>(&value)?,
                ));
            }
            if reached_schedule {
                // Skip the legitimately scheduled range to parked records.
                let boundary = [
                    saved_queue.as_slice(),
                    format!("{parked_after:0DUE_DIGITS$}~").as_bytes(),
                ]
                .concat();
                let after = sweep.after.clone().filter(|after| *after > boundary);
                for entry in store.source_transactions_after(
                    &saved_queue,
                    Some(after.as_deref().unwrap_or(&boundary)),
                ) {
                    let (key, value) = entry?;
                    if records.len() == limit {
                        has_unseen_records = true;
                        break;
                    }
                    records.push((
                        key.to_vec(),
                        bincode::deserialize::<OwnedArtifacts>(&value)?,
                    ));
                }
            }
            Ok((
                sweep,
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

        // Select whole records, or a prefix of one large record, in key order.
        // `order` keeps fenced records (None) so the sweep cursor can pass them.
        let page_records = records.len();
        let mut candidates = BTreeSet::new();
        let mut selected = Vec::new();
        let mut order = Vec::new();
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
            // A fenced record may still gain reserved uploads, and its writer
            // may rewrite it under the same key. Never examine or move it.
            if protected_operations.contains(&owner.operation) {
                order.push((key, None));
                continue;
            }
            // Informational since the single-grace collector; older engines
            // still start their record grace from it after a rollback.
            owner.unfenced_since_ms.get_or_insert(now);
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
            order.push((key.clone(), Some(selected.len())));
            selected.push(Selected {
                key,
                owner,
                end,
                retained: false,
                next_due: None,
                finished: false,
            });
            if end < length {
                stopped_early = true;
                break;
            }
        }
        report.continuation_required =
            stopped_early || has_unseen_records || report.examined_records < page_records;
        report.examined_objects = candidates.len();

        let referenced = if candidates.iter().all(|path| is_metadata_json(path)) {
            // JSON-only pages do not require reading any manifests.
            Referenced::Walk(BTreeSet::new())
        } else {
            match self
                .referenced(&head, &candidates, deadline, &mut report)
                .await?
            {
                Some(referenced) => referenced,
                None => {
                    // The index is still being built. Keep every record and
                    // the sweep cursor unchanged, and continue next page.
                    report.continuation_required = true;
                    return Ok(report);
                }
            }
        };
        // Catalog JSON is independent of snapshot expiration. Protect the
        // current pointer and every version the catalog still advertises.
        let catalog_json: HashSet<&str> = head
            .metadata_location()
            .into_iter()
            .chain(
                head.metadata()
                    .metadata_log()
                    .iter()
                    .map(|entry| entry.metadata_file.as_str()),
            )
            .collect();

        let store = self.store.clone();
        let marker_paths: Vec<_> = candidates.iter().cloned().collect();
        let markers: HashMap<String, u64> = blocking(move || {
            let mut markers = HashMap::new();
            for path in marker_paths {
                if let Some(bytes) = store.source_transaction(&marker_key(uuid, &path))? {
                    markers.insert(path, bincode::deserialize::<u64>(&bytes)?);
                }
            }
            Ok(markers)
        })
        .await?;
        let mut puts: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        let mut deletes: Vec<Vec<u8>> = Vec::new();
        let mut due: Vec<String> = Vec::new();
        let mut due_paths = HashSet::new();
        let mut observed = HashSet::new();
        let mut truncated = None;
        'records: for (position, record) in selected.iter_mut().enumerate() {
            let age = now.saturating_sub(record.owner.created_ms);
            let adopted = record.owner.operation.0.starts_with(METADATA_IMPORT);
            for ordinal in record.owner.cursor..record.end {
                let path = record
                    .owner
                    .artifacts
                    .path(ordinal)
                    .expect("validated range");
                // Adopted unregistered JSON has unknown readers: full grace.
                let grace = millis(if is_metadata_json(&path) && !adopted {
                    policy.metadata_grace
                } else {
                    policy.grace
                });
                let marker = markers.get(&path).copied();
                if catalog_json.contains(path.as_str()) || referenced.contains(&path) {
                    // A retained file may stay live long after its upload.
                    // Its next unreferenced observation starts a new grace.
                    if marker.is_some() && observed.insert(path.clone()) {
                        deletes.push(marker_key(uuid, &path));
                    }
                    report.protected_objects += 1;
                    record.retained = true;
                    let recheck =
                        millis(Duration::from_millis(age / 2).clamp(MIN_RECHECK, MAX_RECHECK))
                            .min(grace)
                            .max(1);
                    let recheck = now.saturating_add(recheck);
                    record.next_due = Some(record.next_due.map_or(recheck, |due| due.min(recheck)));
                    continue;
                }
                let since = match marker {
                    Some(since) => since,
                    None => {
                        if observed.insert(path.clone()) {
                            puts.push((marker_key(uuid, &path), bincode::serialize(&now)?));
                        }
                        now
                    }
                };
                if since.saturating_add(grace) > now {
                    report.deferred_objects += 1;
                    record.retained = true;
                    // Recheck at least hourly, so a reference that reappears
                    // inside the grace resets the clock.
                    let eligible = since
                        .saturating_add(grace)
                        .min(now.saturating_add(millis(MAX_RECHECK)));
                    record.next_due =
                        Some(record.next_due.map_or(eligible, |due| due.min(eligible)));
                    continue;
                }
                if due_paths.contains(&path) {
                    continue;
                }
                if due.len() == policy.max_deletes {
                    // Leave the rest of this record, and later records, for
                    // the next page.
                    record.end = ordinal;
                    truncated = Some(position);
                    break 'records;
                }
                due_paths.insert(path.clone());
                due.push(path);
            }
        }
        if let Some(position) = truncated {
            report.continuation_required = true;
            // Nothing of the truncated record may be recorded when it made no
            // progress; its key and cursor stay unchanged.
            let keep = if selected[position].end == selected[position].owner.cursor {
                position
            } else {
                position + 1
            };
            selected.truncate(keep);
        }
        drop(referenced);

        if !due.is_empty() || !puts.is_empty() {
            // Deletion and new clocks depend on the complete metadata above.
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
        }
        // Check existence first: versioned S3 creates a delete marker for every
        // DELETE, including one for an object that is already absent.
        // Owned inputs keep this future `Send` for the table actor.
        let io = head.file_io().clone();
        let deleted: Vec<bool> = stream::iter(due.clone())
            .map(move |path| {
                let io = io.clone();
                async move {
                    if !io.exists(&path).await? {
                        return Ok::<_, iceberg::Error>(false);
                    }
                    io.delete(&path).await?;
                    Ok(true)
                }
            })
            .buffered(DELETE_CONCURRENCY)
            .try_collect()
            .await?;
        for (path, deleted) in due.iter().zip(deleted) {
            deletes.push(marker_key(uuid, path));
            if deleted {
                report.delete_requests += 1;
                report.metadata_json_delete_requests += usize::from(is_metadata_json(path));
            }
        }

        for record in &mut selected {
            record.owner.protected |= record.retained;
            record.owner.cursor = record.end;
            let length = record.owner.artifacts.len().expect("validated count");
            if record.end < length {
                // Resume this record under the same key on the next page.
                puts.push((record.key.clone(), bincode::serialize(&record.owner)?));
                continue;
            }
            record.finished = true;
            let retire = !record.owner.protected;
            // A record completed across pages knows only its last chunk's
            // schedule; earlier protected chunks are rechecked soon.
            let next_due = record.next_due.unwrap_or_else(|| {
                now + millis(MIN_RECHECK)
                    .min(millis(policy.grace.min(policy.metadata_grace)))
                    .max(1)
            });
            record.owner.cursor = 0;
            record.owner.protected = false;
            if retire {
                deletes.push(record.key.clone());
                report.retired_records += 1;
                continue;
            }
            let moved = format!(
                "{}{next_due:0width$}-{}",
                String::from_utf8_lossy(&queue),
                record_id(&record.key, &queue, uuid),
                width = DUE_DIGITS
            )
            .into_bytes();
            if moved != record.key {
                deletes.push(record.key.clone());
            }
            puts.push((moved, bincode::serialize(&record.owner)?));
            report.rescheduled_records += 1;
        }
        let mut next_cursor = sweep.after.clone();
        for (key, index) in order {
            // Stop before a partial or untouched record; it is resumed next page.
            if index.is_some_and(|index| selected.get(index).is_none_or(|record| !record.finished))
            {
                break;
            }
            next_cursor = Some(key);
        }
        if sweep.after.is_none() {
            // The cursor of engines before the due queue is obsolete.
            deletes.push(format!("artifact-gc/v1/{uuid}").into_bytes());
        }
        if report.continuation_required {
            let cursor = Sweep {
                started_ms: sweep.started_ms,
                after: next_cursor,
            };
            puts.push((cursor_key, bincode::serialize(&cursor)?));
        } else {
            // Reaching the due boundary completes this sweep. The next one
            // starts again with unordered records and newly due queue entries.
            deletes.push(cursor_key);
        }
        let store = self.store.clone();
        blocking(move || {
            Ok(store.write_source_records(
                puts.iter()
                    .map(|(key, value)| (key.as_slice(), value.as_slice())),
                deletes.iter().map(Vec::as_slice),
            )?)
        })
        .await?;
        Ok(report)
    }

    /// Returns `None` while the incremental index is incomplete. A table whose
    /// index exceeds the memory budget intersects this page's candidates with a
    /// full manifest walk instead, as older engines did on every page.
    async fn referenced(
        &self,
        head: &Table,
        candidates: &BTreeSet<String>,
        deadline: Instant,
        report: &mut GarbageReport,
    ) -> Result<Option<Referenced>> {
        let uuid = head.metadata().uuid();
        let mut table = self.garbage.slot(uuid)?.lock_owned().await;
        if table
            .retry_after
            .is_none_or(|retry| retry <= Instant::now())
        {
            let progress = table
                .index
                .sync(head, &self.cache, deadline, self.garbage.budget)
                .await?;
            report.manifest_list_reads += progress.manifest_lists_read;
            if progress.oversized {
                self.garbage.forget_builder(uuid)?;
                table.retry_after = Some(Instant::now() + OVERSIZED_RETRY);
                tracing::warn!(
                    table_uuid = %uuid,
                    budget_bytes = self.garbage.budget,
                    "retained-artifact index exceeds its memory budget; garbage collection walks manifests per page"
                );
            } else {
                table.retry_after = None;
            }
            if !progress.oversized {
                if self
                    .garbage
                    .account(uuid, table.index.estimated_bytes(), progress.complete)?
                {
                    return Ok(progress.complete.then_some(Referenced::Index(table)));
                }
                table.index = RetainedIndex::default();
                table.retry_after = Some(Instant::now() + YIELDED_RETRY);
            }
        }
        drop(table);
        Ok(Some(Referenced::Walk(
            retained_artifacts(head, candidates, &self.cache).await?,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn one_partial_build_keeps_priority_over_the_budget() {
        let indexes = RetainedIndexes::new(100);
        let [a, b, c] = [(); 3].map(|_| uuid::Uuid::new_v4());
        for table in [a, b, c] {
            indexes.slot(table).unwrap();
        }
        assert!(
            indexes.account(a, 80, false).unwrap(),
            "first build has priority"
        );
        assert!(
            !indexes.account(b, 50, false).unwrap(),
            "a second partial build yields instead of evicting the first"
        );
        assert!(indexes.slots.lock().unwrap().tables.contains_key(&a));
        assert!(indexes.account(a, 80, true).unwrap());
        assert!(
            indexes.account(b, 50, false).unwrap(),
            "a complete index is evicted for a build"
        );
        let slots = indexes.slots.lock().unwrap();
        assert!(!slots.tables.contains_key(&a));
        assert_eq!(slots.builder, Some(b));
        drop(slots);
        assert!(
            indexes.account(c, 60, true).unwrap(),
            "a complete index never evicts the builder"
        );
        assert!(indexes.slots.lock().unwrap().tables.contains_key(&b));
    }

    #[test]
    fn queue_keys_order_by_due_time_and_keep_their_identity() {
        let table = uuid::Uuid::new_v4();
        let queue = queue_prefix(table).into_bytes();
        let key =
            |due: u64, id: &str| format!("{}{due:020}-{id}", queue_prefix(table)).into_bytes();
        assert!(key(9, "b") < key(10, "a"));
        assert!(format!("{}{}", registry_prefix(table), uuid::Uuid::new_v4()).into_bytes() < queue);
        assert!(format!("{}catalog-x", registry_prefix(table)).into_bytes() < queue);
        assert_eq!(queue_due(&key(42, "id"), &queue), Some(42));
        assert_eq!(queue_due(b"owned-artifacts/v2/x/abc", &queue), None);
        assert_eq!(record_id(&key(42, "stable"), &queue, table), "stable");
        let unordered = format!("{}catalog-x", registry_prefix(table)).into_bytes();
        assert_eq!(
            record_id(&unordered, &queue, table),
            record_id(&unordered, &queue, table)
        );
    }
}
