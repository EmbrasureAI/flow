//! Bounded transaction admission while preserving each table's source order.

use anyhow::{Context, Result, ensure};
use flow_coordinator::{Priority, Scheduler, SourceLedger};
use flow_model::{PgLsn, SourceTransaction, TableId};
use std::{
    collections::{BTreeMap, BTreeSet, HashSet, VecDeque},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

fn source_age(transaction: &SourceTransaction) -> Duration {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros();
    let committed = transaction.commit_timestamp_micros.max(0) as u128;
    Duration::from_micros(now.saturating_sub(committed).min(u128::from(u64::MAX)) as u64)
}

pub(super) const EPOCH_MUTATION_TRIGGER: u64 = 10_000;
pub(super) const EPOCH_MAX_BYTES: u64 = 32 << 20;
/// Estimated descriptor memory of one dispatched epoch. A worker keeps a few
/// copies while it publishes, so this bounds each lane, not the lookahead.
pub(super) const EPOCH_MAX_DESCRIPTOR_BYTES: u64 = 4 << 20;

/// Heap and inline size of one queued descriptor. Multi-table transactions
/// carry per-table vectors, so they cost more than single-table ones.
fn descriptor_bytes(transaction: &SourceTransaction) -> u64 {
    (size_of::<SourceTransaction>()
        + transaction.source_id.0.len()
        + size_of_val(transaction.schema_versions.as_slice())
        + size_of_val(transaction.affected_tables.as_slice())
        + transaction
            .table_mutation_counts
            .as_deref()
            .map_or(0, size_of_val)) as u64
}

/// Limits one epoch without splitting a source transaction.
struct EpochBudget {
    payload: u64,
    memory: u64,
    count: usize,
    limit: Option<usize>,
    closed: bool,
}
impl EpochBudget {
    fn admit(&mut self, table: TableId, transaction: &SourceTransaction) -> bool {
        if self.closed {
            return false;
        }
        let size = transaction.mutation_chunks.payload_bytes();
        let memory = descriptor_bytes(transaction);
        let standalone = transaction
            .mutation_count(table)
            .is_none_or(|rows| rows > EPOCH_MUTATION_TRIGGER)
            || size > EPOCH_MAX_BYTES;
        if self.memory > 0
            && (standalone
                || self.payload.saturating_add(size) > EPOCH_MAX_BYTES
                || self.memory.saturating_add(memory) > EPOCH_MAX_DESCRIPTOR_BYTES
                || self.limit.is_some_and(|limit| self.count >= limit))
        {
            self.closed = true;
            return false;
        }
        self.payload = self.payload.saturating_add(size);
        self.memory = self.memory.saturating_add(memory);
        self.count += 1;
        self.closed = standalone
            || self.payload >= EPOCH_MAX_BYTES
            || self.memory >= EPOCH_MAX_DESCRIPTOR_BYTES
            || self.limit.is_some_and(|limit| self.count >= limit);
        true
    }
}

pub(super) fn schedule_transactions<'a>(
    table: TableId,
    transactions: impl IntoIterator<Item = &'a SourceTransaction>,
    scheduler: &mut Scheduler,
    priority: Priority,
) {
    let mut transactions = transactions.into_iter();
    let Some(first) = transactions.next() else {
        return;
    };
    let (rows, bytes) = std::iter::once(first).chain(transactions).fold(
        (Some(0u64), 0u64),
        |(rows, bytes), transaction| {
            (
                rows.zip(transaction.mutation_count(table))
                    .map(|(sum, count)| sum.saturating_add(count)),
                bytes.saturating_add(transaction.mutation_chunks.payload_bytes()),
            )
        },
    );
    scheduler.push(
        table,
        rows,
        bytes,
        priority,
        source_age(first),
        Instant::now(),
    );
}

/// `capacity` bounds only the queued lookahead that makes tables schedulable.
/// It is shared by all tables and admitted round-robin. A dispatched epoch
/// leaves that lookahead and continues through the table's durable references,
/// so its size is set by payload and memory limits, not by the lookahead share.
#[derive(Default)]
pub(super) struct PendingWork {
    pub(super) tables: BTreeMap<TableId, VecDeque<SourceTransaction>>,
    // Reservations include queued and running descriptors. A failure releases
    // both only after the actual worker has returned; the ledger owns replay.
    reserved: BTreeMap<TableId, BTreeSet<PgLsn>>,
    // Queued lookahead, at most `capacity`.
    loaded: usize,
    // Dispatched epochs, each within `EPOCH_MAX_DESCRIPTOR_BYTES`.
    running: usize,
    through: BTreeMap<TableId, PgLsn>,
    next_after: Option<TableId>,
    observed_registered: PgLsn,
    excluded: HashSet<TableId>,
    capacity: usize,
    unloaded: bool,
    /// Optional `limits.epoch_max_transactions`.
    epoch_limit: Option<usize>,
}
impl PendingWork {
    pub(super) fn with_epoch_limit(epoch_limit: Option<usize>) -> Self {
        Self {
            epoch_limit,
            ..Self::default()
        }
    }

    /// Dequeue a table's ordered prefix without splitting a source transaction.
    /// Only admitted lookahead can start an epoch. Once that queue is drained,
    /// the epoch continues in order through the table's durable references.
    pub(super) fn take_epoch(
        &mut self,
        table: TableId,
        ledger: &SourceLedger,
    ) -> Result<Vec<SourceTransaction>> {
        let mut budget = EpochBudget {
            payload: 0,
            memory: 0,
            count: 0,
            limit: self.epoch_limit,
            closed: false,
        };
        let queue = self.tables.entry(table).or_default();
        let mut transactions = Vec::new();
        while let Some(transaction) = queue.front() {
            if !budget.admit(table, transaction) {
                break;
            }
            transactions.push(queue.pop_front().expect("queue front exists"));
        }
        let drained = queue.is_empty();
        if transactions.is_empty() {
            return Ok(transactions);
        }
        self.loaded -= transactions.len();
        self.running += transactions.len();
        // The freed lookahead can admit another table's work.
        self.unloaded = true;
        if drained && !budget.closed {
            let through = *self
                .through
                .get(&table)
                .context("admitted table has no admission cursor")?;
            let reserved = self.reserved.entry(table).or_default();
            for transaction in ledger.pending_table_transactions_after(table, through) {
                let transaction = transaction?;
                if !budget.admit(table, &transaction) {
                    break;
                }
                ensure!(
                    reserved.insert(transaction.end_lsn),
                    "duplicate table admission reservation"
                );
                self.through.insert(table, transaction.end_lsn);
                self.running += 1;
                transactions.push(transaction);
            }
        }
        Ok(transactions)
    }

    pub(super) fn refill(
        &mut self,
        ledger: &SourceLedger,
        scheduler: &mut Scheduler,
        profiles: &BTreeMap<TableId, Priority>,
        capacity: usize,
        blocked: &HashSet<TableId>,
    ) -> Result<()> {
        self.capacity = capacity;
        ensure!(
            self.loaded <= capacity,
            "table admission exceeds its descriptor budget"
        );
        let resumed = self.excluded.iter().any(|id| !blocked.contains(id));
        self.excluded.clone_from(blocked);
        if !self.unloaded
            && !resumed
            && self.observed_registered >= ledger.watermarks().journal_durable_lsn
        {
            return Ok(());
        }
        self.observed_registered = ledger.watermarks().journal_durable_lsn;
        // One reference per table per pass, rotating the starting table even
        // when the table count exceeds the entire descriptor budget.
        let mut ids = profiles
            .keys()
            .copied()
            .filter(|id| !blocked.contains(id))
            .collect::<Vec<_>>();
        if let Some(after) = self.next_after {
            let split = ids.partition_point(|id| *id <= after);
            ids.rotate_left(split);
        }
        let mut exhausted = HashSet::new();
        while self.loaded < capacity && exhausted.len() < ids.len() {
            for id in &ids {
                if self.loaded == capacity {
                    break;
                }
                if exhausted.contains(id) {
                    continue;
                }
                self.next_after = Some(*id);
                let through = self.through.get(id).copied().unwrap_or_default();
                let transaction = ledger
                    .pending_table_transactions_after(*id, through)
                    .next()
                    .transpose()?;
                let Some(transaction) = transaction else {
                    exhausted.insert(*id);
                    continue;
                };
                let end = transaction.end_lsn;
                ensure!(
                    self.reserved.entry(*id).or_default().insert(end),
                    "duplicate table admission reservation"
                );
                self.loaded += 1;
                self.through.insert(*id, end);
                scheduler.push(
                    *id,
                    transaction.mutation_count(*id),
                    transaction.mutation_chunks.payload_bytes(),
                    profiles[id],
                    source_age(&transaction),
                    Instant::now(),
                );
                self.tables.entry(*id).or_default().push_back(transaction);
            }
        }
        self.unloaded = exhausted.len() < ids.len();
        Ok(())
    }

    /// Release one descriptor of a dispatched epoch after its table published.
    pub(super) fn complete(&mut self, table: TableId, end: PgLsn) -> Result<()> {
        ensure!(
            self.tables
                .get(&table)
                .and_then(VecDeque::front)
                .is_none_or(|queued| queued.end_lsn > end),
            "completed transaction was never dispatched"
        );
        let reserved = self
            .reserved
            .get_mut(&table)
            .context("completed table is outside the publication window")?;
        ensure!(
            reserved.remove(&end),
            "completed transaction is outside the publication window"
        );
        self.running -= 1;
        self.unloaded = true;
        Ok(())
    }

    /// Evict admission after this table's worker returns. Its durable index and
    /// unfinished operation retain all changes without occupying another lane.
    /// A deferred epoch is reloaded from the ledger rather than kept in memory.
    pub(super) fn defer(&mut self, table: TableId, scheduler: &mut Scheduler) {
        let reserved = self
            .reserved
            .remove(&table)
            .map_or(0, |entries| entries.len());
        let queued = self.tables.remove(&table).map_or(0, |queue| queue.len());
        self.loaded -= queued;
        self.running -= reserved - queued;
        self.through.remove(&table);
        scheduler.remove(table);
        self.unloaded = true;
    }

    /// Whether the table holds any admission reservation or queued work.
    pub(super) fn holds(&self, table: TableId) -> bool {
        self.reserved.contains_key(&table) || self.tables.contains_key(&table)
    }

    pub(super) fn has_work(&self) -> bool {
        self.loaded + self.running > 0
    }

    /// True only when another bounded refill can admit runnable work. Full
    /// queues and durable references for blocked tables must not cause a spin.
    pub(super) fn has_runnable_unloaded(
        &self,
        ledger: &SourceLedger,
        blocked: &HashSet<TableId>,
    ) -> bool {
        self.loaded < self.capacity
            && (self.unloaded
                || self.observed_registered < ledger.watermarks().journal_durable_lsn
                || self.excluded.iter().any(|id| !blocked.contains(id)))
    }
}

#[cfg(test)]
mod admission_tests {
    use super::*;
    use flow_coordinator::{AckMode, JournalDurability};
    use flow_model::{JournalChunks, SourceId, TableMutationCount};
    use flow_state_store::{ControlStore, StateStoreOptions};

    #[test]
    fn deferred_prefixes_reload_whole_transactions_and_all_table_ack_order() {
        let directory = tempfile::tempdir().unwrap();
        let control = ControlStore::open(directory.path().join("control")).unwrap();
        let store = control
            .initialize_index(directory.path().join("index"), StateStoreOptions::default())
            .unwrap();
        let source = SourceId("admission".into());
        let (first, second) = (TableId(1), TableId(2));
        let transaction = |xid: u32, rows: Option<u64>, bytes| SourceTransaction {
            source_id: source.clone(),
            xid,
            begin_lsn: PgLsn(u64::from(xid) * 10 - 1),
            commit_lsn: PgLsn(u64::from(xid) * 10),
            end_lsn: PgLsn(u64::from(xid) * 10 + 1),
            commit_timestamp_micros: 0,
            schema_versions: vec![],
            affected_tables: vec![first, second],
            mutation_chunks: JournalChunks {
                payload_bytes: bytes,
                ..Default::default()
            },
            table_mutation_counts: rows.map(|mutations| {
                vec![
                    TableMutationCount {
                        table_id: first,
                        mutations,
                    },
                    TableMutationCount {
                        table_id: second,
                        mutations: 0,
                    },
                ]
            }),
        };
        let mut ledger = SourceLedger::open(
            store.clone(),
            source.clone(),
            AckMode::Materialized,
            JournalDurability::LocalDisk,
        )
        .unwrap();
        ledger
            .journaled_batch(&[
                transaction(1, Some(6000), 1),
                transaction(2, Some(5000), 1),
                transaction(3, Some(15_000), 1),
                transaction(4, Some(1), EPOCH_MAX_BYTES + 1),
                transaction(5, None, 1),
                transaction(6, Some(0), 0),
            ])
            .unwrap();
        drop(ledger);
        let mut ledger = SourceLedger::open(
            store,
            source.clone(),
            AckMode::Materialized,
            JournalDurability::LocalDisk,
        )
        .unwrap();
        let mut pending = PendingWork::default();
        let mut scheduler =
            Scheduler::new(EPOCH_MUTATION_TRIGGER, EPOCH_MAX_BYTES, 100, Instant::now()).unwrap();
        let profiles = BTreeMap::from([(first, Priority::Realtime), (second, Priority::Realtime)]);
        pending
            .refill(&ledger, &mut scheduler, &profiles, 12, &HashSet::new())
            .unwrap();
        assert_eq!(scheduler.take_ready(Instant::now()), Some(first));
        let taken = pending.take_epoch(first, &ledger).unwrap();
        assert_eq!(
            taken.iter().map(|txn| txn.xid).collect::<Vec<_>>(),
            [1, 2],
            "the row trigger does not cap already-queued ordinary work"
        );
        assert_eq!((pending.loaded, pending.running), (10, 2));
        schedule_transactions(
            first,
            &pending.tables[&first],
            &mut scheduler,
            Priority::Realtime,
        );
        // New work arrives while the dequeued prefix is in flight. Deferring
        // the returned run reloads that prefix ahead of the tail and new work.
        ledger
            .journaled_batch(&[
                transaction(7, Some(10_000), EPOCH_MAX_BYTES - 1),
                transaction(8, Some(1), 1),
                transaction(9, Some(0), 1),
            ])
            .unwrap();
        pending.defer(first, &mut scheduler);
        assert_eq!((pending.loaded, pending.running), (6, 0));
        pending
            .refill(&ledger, &mut scheduler, &profiles, 18, &HashSet::new())
            .unwrap();
        assert_eq!(pending.loaded, 18);
        // The last byte fits exactly; the next transaction remains whole in the
        // following epoch. A transaction at the row trigger can still coalesce.
        for expected in [
            vec![1, 2],
            vec![3],
            vec![4],
            vec![5],
            vec![6, 7, 8],
            vec![9],
        ] {
            let epoch = pending.take_epoch(first, &ledger).unwrap();
            assert_eq!(
                epoch.iter().map(|txn| txn.xid).collect::<Vec<_>>(),
                expected
            );
            for txn in epoch {
                ledger.table_materialized(txn.end_lsn, first, 100).unwrap();
                pending.complete(first, txn.end_lsn).unwrap();
            }
        }
        assert!(pending.tables[&first].is_empty());
        assert_eq!(
            pending.loaded, 9,
            "only the unfinished table retains its reservations"
        );
        assert_eq!(ledger.acknowledgement(), PgLsn(0));
        loop {
            let epoch = pending.take_epoch(second, &ledger).unwrap();
            if epoch.is_empty() {
                break;
            }
            for txn in epoch {
                ledger.table_materialized(txn.end_lsn, second, 101).unwrap();
                pending.complete(second, txn.end_lsn).unwrap();
            }
        }
        assert!(!pending.has_work());
        assert_eq!(ledger.pending_count(), 0);
        assert_eq!(ledger.acknowledgement(), PgLsn(91));
    }

    #[test]
    fn blocked_table_releases_the_global_budget_and_healthy_tables_pass_its_prefix() {
        let directory = tempfile::tempdir().unwrap();
        let store =
            flow_state_store::StateStore::open(directory.path(), StateStoreOptions::default())
                .unwrap();
        let source = SourceId("isolation".into());
        let (blocked, healthy) = (TableId(1), TableId(2));
        let mut ledger = SourceLedger::open(
            store.clone(),
            source.clone(),
            AckMode::Materialized,
            JournalDurability::LocalDisk,
        )
        .unwrap();
        for xid in 1..=20 {
            ledger
                .journaled(SourceTransaction {
                    source_id: source.clone(),
                    xid,
                    begin_lsn: PgLsn(xid as u64 * 10 - 1),
                    commit_lsn: PgLsn(xid as u64 * 10),
                    end_lsn: PgLsn(xid as u64 * 10 + 1),
                    commit_timestamp_micros: 0,
                    schema_versions: vec![],
                    affected_tables: vec![blocked, healthy],
                    mutation_chunks: Default::default(),
                    table_mutation_counts: None,
                })
                .unwrap();
        }
        let profiles =
            BTreeMap::from([(blocked, Priority::Realtime), (healthy, Priority::Realtime)]);
        let mut scheduler = Scheduler::new(1, EPOCH_MAX_BYTES, 100, Instant::now()).unwrap();
        let mut pending = PendingWork::default();
        pending
            .refill(&ledger, &mut scheduler, &profiles, 2, &HashSet::new())
            .unwrap();
        assert_eq!(pending.loaded, 2);
        let failed = pending.take_epoch(blocked, &ledger).unwrap();
        assert_eq!(failed.len(), 1);
        assert_eq!(
            (pending.loaded, pending.running),
            (1, 1),
            "running descriptors stay reserved outside the lookahead"
        );
        pending.defer(blocked, &mut scheduler);
        let exclusions = HashSet::from([blocked]);
        for expected in 1..=20 {
            pending
                .refill(&ledger, &mut scheduler, &profiles, 2, &exclusions)
                .unwrap();
            assert!(pending.loaded <= 2);
            let epoch = pending.take_epoch(healthy, &ledger).unwrap();
            assert_eq!(epoch.len(), 1);
            assert_eq!(epoch[0].xid, expected);
            ledger
                .table_materialized(epoch[0].end_lsn, healthy, 100)
                .unwrap();
            pending.complete(healthy, epoch[0].end_lsn).unwrap();
        }
        pending
            .refill(&ledger, &mut scheduler, &profiles, 2, &exclusions)
            .unwrap();
        assert!(!pending.has_work());
        assert!(!pending.has_runnable_unloaded(&ledger, &exclusions));
        assert_eq!(ledger.acknowledgement(), PgLsn(0));
        assert_eq!(ledger.pending_count(), 20);
        drop(ledger);
        let mut ledger = SourceLedger::open(
            store,
            source,
            AckMode::Materialized,
            JournalDurability::LocalDisk,
        )
        .unwrap();
        let mut pending = PendingWork::default();
        for expected in 1..=20 {
            pending
                .refill(&ledger, &mut scheduler, &profiles, 2, &HashSet::new())
                .unwrap();
            let epoch = pending.take_epoch(blocked, &ledger).unwrap();
            assert_eq!(epoch[0].xid, expected);
            assert!(pending.tables.get(&healthy).is_none_or(VecDeque::is_empty));
            ledger
                .table_materialized(epoch[0].end_lsn, blocked, 101)
                .unwrap();
            pending.complete(blocked, epoch[0].end_lsn).unwrap();
        }
        while ledger.drain_completed_prefix().unwrap() {}
        assert_eq!(ledger.acknowledgement(), PgLsn(201));
    }

    #[test]
    fn more_tables_than_budget_are_admitted_fairly_and_defer_reloads_durable_work() {
        let directory = tempfile::tempdir().unwrap();
        let store =
            flow_state_store::StateStore::open(directory.path(), StateStoreOptions::default())
                .unwrap();
        let source = SourceId("fairness".into());
        let profiles = (1..=7)
            .map(|id| (TableId(id), Priority::Realtime))
            .collect::<BTreeMap<_, _>>();
        let mut ledger = SourceLedger::open(
            store,
            source.clone(),
            AckMode::Materialized,
            JournalDurability::LocalDisk,
        )
        .unwrap();
        let journal = |ledger: &mut SourceLedger, xids: std::ops::RangeInclusive<u32>| {
            for xid in xids {
                ledger
                    .journaled(SourceTransaction {
                        source_id: source.clone(),
                        xid,
                        begin_lsn: PgLsn(xid as u64 * 10 - 1),
                        commit_lsn: PgLsn(xid as u64 * 10),
                        end_lsn: PgLsn(xid as u64 * 10 + 1),
                        commit_timestamp_micros: 0,
                        schema_versions: vec![],
                        affected_tables: profiles.keys().copied().collect(),
                        mutation_chunks: Default::default(),
                        table_mutation_counts: Some(
                            profiles
                                .keys()
                                .map(|table| TableMutationCount {
                                    table_id: *table,
                                    mutations: 1,
                                })
                                .collect(),
                        ),
                    })
                    .unwrap();
            }
        };
        journal(&mut ledger, 1..=3);
        let mut scheduler = Scheduler::new(1, EPOCH_MAX_BYTES, 100, Instant::now()).unwrap();
        let mut pending = PendingWork::default();
        let mut admitted = Vec::new();
        for _ in 0..7 {
            pending
                .refill(&ledger, &mut scheduler, &profiles, 1, &HashSet::new())
                .unwrap();
            assert_eq!(pending.loaded, 1);
            assert!(
                !pending.has_runnable_unloaded(&ledger, &HashSet::new()),
                "a full budget must not spin"
            );
            let id = *pending
                .tables
                .iter()
                .find(|(_, queue)| !queue.is_empty())
                .unwrap()
                .0;
            admitted.push(id);
            scheduler.remove(id);
            let epoch = pending.take_epoch(id, &ledger).unwrap();
            assert_eq!(
                epoch.iter().map(|txn| txn.xid).collect::<Vec<_>>(),
                vec![1, 2, 3],
                "a one-descriptor lookahead share does not cap the epoch"
            );
            for txn in epoch {
                ledger.table_materialized(txn.end_lsn, id, 100).unwrap();
                pending.complete(id, txn.end_lsn).unwrap();
            }
        }
        assert_eq!(admitted, profiles.keys().copied().collect::<Vec<_>>());
        assert!(!pending.has_work());
        journal(&mut ledger, 4..=5);
        let blocked = profiles
            .keys()
            .copied()
            .filter(|id| *id != TableId(1))
            .collect();
        pending
            .refill(&ledger, &mut scheduler, &profiles, 4, &blocked)
            .unwrap();
        let epoch = pending.take_epoch(TableId(1), &ledger).unwrap();
        assert_eq!(
            epoch.iter().map(|txn| txn.xid).collect::<Vec<_>>(),
            vec![4, 5]
        );
        pending.defer(TableId(1), &mut scheduler);
        assert!(!pending.has_work());
        assert!(scheduler.next_deadline().is_none());
        pending
            .refill(&ledger, &mut scheduler, &profiles, 4, &blocked)
            .unwrap();
        assert_eq!(
            pending
                .take_epoch(TableId(1), &ledger)
                .unwrap()
                .iter()
                .map(|txn| txn.xid)
                .collect::<Vec<_>>(),
            vec![4, 5]
        );
    }

    /// A newly blocked table's queued backlog must release its admission
    /// budget; otherwise healthy tables stall behind it.
    #[test]
    fn deferring_a_blocked_backlog_admits_healthy_tables() {
        let directory = tempfile::tempdir().unwrap();
        let control = ControlStore::open(directory.path().join("control")).unwrap();
        let store = control
            .initialize_index(directory.path().join("index"), StateStoreOptions::default())
            .unwrap();
        let source = SourceId("deferred".into());
        let (blocked, healthy) = (TableId(1), TableId(2));
        let transaction = |xid: u32, table: TableId| SourceTransaction {
            source_id: source.clone(),
            xid,
            begin_lsn: PgLsn(u64::from(xid) * 10 - 1),
            commit_lsn: PgLsn(u64::from(xid) * 10),
            end_lsn: PgLsn(u64::from(xid) * 10 + 1),
            commit_timestamp_micros: 0,
            schema_versions: vec![],
            affected_tables: vec![table],
            mutation_chunks: JournalChunks::default(),
            table_mutation_counts: Some(vec![TableMutationCount {
                table_id: table,
                mutations: 1,
            }]),
        };
        let mut ledger = SourceLedger::open(
            store,
            source.clone(),
            AckMode::Materialized,
            JournalDurability::LocalDisk,
        )
        .unwrap();
        ledger
            .journaled_batch(&[transaction(1, blocked), transaction(2, blocked)])
            .unwrap();
        let mut pending = PendingWork::default();
        let mut scheduler =
            Scheduler::new(EPOCH_MUTATION_TRIGGER, EPOCH_MAX_BYTES, 100, Instant::now()).unwrap();
        let profiles =
            BTreeMap::from([(blocked, Priority::Realtime), (healthy, Priority::Realtime)]);
        pending
            .refill(&ledger, &mut scheduler, &profiles, 2, &HashSet::new())
            .unwrap();
        ledger.journaled_batch(&[transaction(3, healthy)]).unwrap();
        let excluded = HashSet::from([blocked]);

        // Without deferral the blocked backlog keeps the whole budget.
        pending
            .refill(&ledger, &mut scheduler, &profiles, 2, &excluded)
            .unwrap();
        assert!(!pending.tables.contains_key(&healthy));

        pending.defer(blocked, &mut scheduler);
        pending
            .refill(&ledger, &mut scheduler, &profiles, 2, &excluded)
            .unwrap();
        assert_eq!(scheduler.take_ready(Instant::now()), Some(healthy));
        let taken = pending.take_epoch(healthy, &ledger).unwrap();
        assert_eq!(taken.len(), 1);
        pending.complete(healthy, taken[0].end_lsn).unwrap();
        assert!(
            !pending.has_work(),
            "no queued work remains for the blocked table"
        );
    }

    /// A latch that lands while the blocked table's run is in flight: the run
    /// completes part of its epoch or is deferred, and the rest must still be
    /// released once no run owns the table.
    #[test]
    fn a_latch_during_a_run_still_releases_the_blocked_backlog() {
        let directory = tempfile::tempdir().unwrap();
        let control = ControlStore::open(directory.path().join("control")).unwrap();
        let store = control
            .initialize_index(directory.path().join("index"), StateStoreOptions::default())
            .unwrap();
        let source = SourceId("mid-run".into());
        let (blocked, healthy) = (TableId(1), TableId(2));
        let transaction = |xid: u32, table: TableId| SourceTransaction {
            source_id: source.clone(),
            xid,
            begin_lsn: PgLsn(u64::from(xid) * 10 - 1),
            commit_lsn: PgLsn(u64::from(xid) * 10),
            end_lsn: PgLsn(u64::from(xid) * 10 + 1),
            commit_timestamp_micros: 0,
            schema_versions: vec![],
            affected_tables: vec![table],
            mutation_chunks: JournalChunks::default(),
            table_mutation_counts: Some(vec![TableMutationCount {
                table_id: table,
                mutations: 1,
            }]),
        };
        let mut ledger = SourceLedger::open(
            store,
            source.clone(),
            AckMode::Materialized,
            JournalDurability::LocalDisk,
        )
        .unwrap();
        ledger
            .journaled_batch(&[transaction(1, blocked), transaction(2, blocked)])
            .unwrap();
        let mut pending = PendingWork::default();
        let mut scheduler =
            Scheduler::new(EPOCH_MUTATION_TRIGGER, EPOCH_MAX_BYTES, 100, Instant::now()).unwrap();
        let profiles =
            BTreeMap::from([(blocked, Priority::Realtime), (healthy, Priority::Realtime)]);
        pending
            .refill(&ledger, &mut scheduler, &profiles, 2, &HashSet::new())
            .unwrap();
        // The run takes its epoch; the latch lands while it is in flight.
        assert_eq!(scheduler.take_ready(Instant::now()), Some(blocked));
        let taken = pending.take_epoch(blocked, &ledger).unwrap();
        assert_eq!(taken.len(), 2);
        ledger.journaled_batch(&[transaction(3, healthy)]).unwrap();
        let excluded = HashSet::from([blocked]);
        pending
            .refill(&ledger, &mut scheduler, &profiles, 2, &excluded)
            .unwrap();
        assert_eq!(
            pending.tables[&healthy].len(),
            1,
            "a running epoch does not hold the lookahead"
        );

        // No run owns it any more, and it still holds reservations.
        assert!(pending.holds(blocked));
        pending.defer(blocked, &mut scheduler);
        assert!(!pending.holds(blocked));
        assert_eq!((pending.loaded, pending.running), (1, 0));
        pending
            .refill(&ledger, &mut scheduler, &profiles, 2, &excluded)
            .unwrap();
        assert_eq!(
            pending.tables[&healthy].len(),
            1,
            "the healthy table is admitted"
        );
    }

    fn descriptor(
        source: &SourceId,
        xid: u32,
        tables: &[TableId],
        payload: u64,
    ) -> SourceTransaction {
        SourceTransaction {
            source_id: source.clone(),
            xid,
            begin_lsn: PgLsn(u64::from(xid) * 10 - 1),
            commit_lsn: PgLsn(u64::from(xid) * 10),
            end_lsn: PgLsn(u64::from(xid) * 10 + 1),
            commit_timestamp_micros: 0,
            schema_versions: vec![],
            affected_tables: tables.to_vec(),
            mutation_chunks: JournalChunks {
                payload_bytes: payload,
                ..Default::default()
            },
            table_mutation_counts: Some(
                tables
                    .iter()
                    .map(|table| TableMutationCount {
                        table_id: *table,
                        mutations: 1,
                    })
                    .collect(),
            ),
        }
    }

    fn open_ledger(store: &flow_state_store::StateStore, source: &SourceId) -> SourceLedger {
        SourceLedger::open(
            store.clone(),
            source.clone(),
            AckMode::Materialized,
            JournalDurability::LocalDisk,
        )
        .unwrap()
    }

    fn journal(ledger: &mut SourceLedger, transactions: &[SourceTransaction]) {
        for page in transactions.chunks(ledger.batch_capacity()) {
            assert_eq!(ledger.journaled_batch(page).unwrap(), page.len());
        }
    }

    /// Mirrors the runtime: complete the ledger in bounded pages, then release.
    fn publish(
        ledger: &mut SourceLedger,
        pending: &mut PendingWork,
        table: TableId,
        epoch: &[SourceTransaction],
    ) {
        for page in epoch.chunks(ledger.batch_capacity()) {
            let ends = page.iter().map(|txn| txn.end_lsn).collect::<Vec<_>>();
            ledger.table_materialized_batch(&ends, table, 100).unwrap();
            for end in ends {
                pending.complete(table, end).unwrap();
            }
        }
        while ledger.drain_completed_prefix().unwrap() {}
    }

    fn xids(epoch: &[SourceTransaction]) -> Vec<u32> {
        epoch.iter().map(|txn| txn.xid).collect()
    }

    /// With single-row transactions spread over 100 busy tables, the shared
    /// lookahead gives each table about 256/100 descriptors. That share only
    /// makes a table schedulable; its epoch still carries the whole backlog.
    #[test]
    fn busy_tables_share_the_lookahead_without_capping_their_epochs() {
        let directory = tempfile::tempdir().unwrap();
        let store =
            flow_state_store::StateStore::open(directory.path(), StateStoreOptions::default())
                .unwrap();
        let source = SourceId("many-tables".into());
        let tables = (1..=100).map(TableId).collect::<Vec<_>>();
        let profiles = tables
            .iter()
            .map(|table| (*table, Priority::Realtime))
            .collect::<BTreeMap<_, _>>();
        let mut ledger = open_ledger(&store, &source);
        let transactions = (0..40u32)
            .flat_map(|round| tables.iter().map(move |table| (round, *table)))
            .map(|(round, table)| descriptor(&source, round * 100 + table.0, &[table], 100))
            .collect::<Vec<_>>();
        journal(&mut ledger, &transactions);
        let mut scheduler =
            Scheduler::new(EPOCH_MUTATION_TRIGGER, EPOCH_MAX_BYTES, 100, Instant::now()).unwrap();
        let mut pending = PendingWork::default();
        pending
            .refill(&ledger, &mut scheduler, &profiles, 256, &HashSet::new())
            .unwrap();
        assert_eq!(pending.loaded, 256);
        assert!(
            tables
                .iter()
                .all(|table| (2..=3).contains(&pending.tables[table].len())),
            "every table receives its round-robin share"
        );
        // Every table dispatches before any publishes, as with slow commits.
        let mut epochs = BTreeMap::new();
        for table in &tables {
            let epoch = pending.take_epoch(*table, &ledger).unwrap();
            assert_eq!(
                xids(&epoch),
                (0..40)
                    .map(|round| round * 100 + table.0)
                    .collect::<Vec<_>>()
            );
            pending
                .refill(&ledger, &mut scheduler, &profiles, 256, &HashSet::new())
                .unwrap();
            assert!(pending.loaded <= 256);
            assert!(
                tables
                    .iter()
                    .filter(|other| !epochs.contains_key(*other) && *other != table)
                    .all(|other| !pending.tables[other].is_empty()),
                "a large running epoch does not starve waiting tables"
            );
            epochs.insert(*table, epoch);
        }
        assert_eq!((pending.loaded, pending.running), (0, 4000));
        // Tables publish in reverse; the shared frontier waits for the oldest.
        for table in tables.iter().rev() {
            publish(&mut ledger, &mut pending, *table, &epochs[table]);
            let expected = if *table == TableId(1) { 40_000 + 1 } else { 0 };
            assert_eq!(ledger.acknowledgement(), PgLsn(expected));
        }
        assert!(!pending.has_work());
        assert_eq!(ledger.pending_count(), 0);
    }

    #[test]
    fn epoch_descriptor_memory_is_bounded_and_the_next_epoch_resumes_in_order() {
        let directory = tempfile::tempdir().unwrap();
        let store =
            flow_state_store::StateStore::open(directory.path(), StateStoreOptions::default())
                .unwrap();
        let source = SourceId("memory".into());
        let table = TableId(1);
        let profiles = BTreeMap::from([(table, Priority::Realtime)]);
        let mut ledger = open_ledger(&store, &source);
        let transactions = (1..=20_000)
            .map(|xid| descriptor(&source, xid, &[table], 100))
            .collect::<Vec<_>>();
        journal(&mut ledger, &transactions);
        let mut scheduler =
            Scheduler::new(EPOCH_MUTATION_TRIGGER, EPOCH_MAX_BYTES, 100, Instant::now()).unwrap();
        let mut pending = PendingWork::default();
        let mut next = 1;
        let mut sizes = Vec::new();
        loop {
            pending
                .refill(&ledger, &mut scheduler, &profiles, 256, &HashSet::new())
                .unwrap();
            assert!(pending.loaded <= 256);
            let epoch = pending.take_epoch(table, &ledger).unwrap();
            if epoch.is_empty() {
                break;
            }
            let memory = epoch.iter().map(descriptor_bytes).sum::<u64>();
            assert!(memory <= EPOCH_MAX_DESCRIPTOR_BYTES);
            assert_eq!(pending.running, epoch.len(), "only the epoch is reserved");
            assert_eq!(
                xids(&epoch),
                (next..next + epoch.len() as u32).collect::<Vec<_>>()
            );
            next += epoch.len() as u32;
            sizes.push(epoch.len());
            publish(&mut ledger, &mut pending, table, &epoch);
        }
        assert_eq!(next, 20_001);
        assert!(
            sizes.len() >= 2 && sizes[0] > 10_000,
            "the memory bound, not the lookahead, splits epochs: {sizes:?}"
        );
        assert_eq!(ledger.acknowledgement(), PgLsn(200_001));
        assert!(!pending.has_work());
    }

    #[test]
    fn configured_epoch_transaction_limit_splits_epochs_in_order() {
        let directory = tempfile::tempdir().unwrap();
        let store =
            flow_state_store::StateStore::open(directory.path(), StateStoreOptions::default())
                .unwrap();
        let source = SourceId("limited".into());
        let table = TableId(1);
        let profiles = BTreeMap::from([(table, Priority::Realtime)]);
        let mut ledger = open_ledger(&store, &source);
        let transactions = (1..=10)
            .map(|xid| descriptor(&source, xid, &[table], 100))
            .collect::<Vec<_>>();
        journal(&mut ledger, &transactions);
        let mut scheduler =
            Scheduler::new(EPOCH_MUTATION_TRIGGER, EPOCH_MAX_BYTES, 100, Instant::now()).unwrap();
        let mut pending = PendingWork::with_epoch_limit(Some(4));
        let mut sizes = Vec::new();
        let mut next = 1;
        loop {
            pending
                .refill(&ledger, &mut scheduler, &profiles, 2, &HashSet::new())
                .unwrap();
            let epoch = pending.take_epoch(table, &ledger).unwrap();
            if epoch.is_empty() {
                break;
            }
            assert_eq!(
                xids(&epoch),
                (next..next + epoch.len() as u32).collect::<Vec<_>>()
            );
            next += epoch.len() as u32;
            sizes.push(epoch.len());
            publish(&mut ledger, &mut pending, table, &epoch);
        }
        assert_eq!(sizes, [4, 4, 2]);
        assert!(!pending.has_work());
    }

    /// Multi-table transactions reserve one descriptor per table. Each table's
    /// epoch may run far ahead of the others, but acknowledgement advances only
    /// through transactions every affected table published, across a restart.
    #[test]
    fn multi_table_transactions_complete_after_every_table_across_defer_and_restart() {
        let directory = tempfile::tempdir().unwrap();
        let store =
            flow_state_store::StateStore::open(directory.path(), StateStoreOptions::default())
                .unwrap();
        let source = SourceId("multi-table".into());
        let tables = [TableId(1), TableId(2), TableId(3)];
        let [first, second, third] = tables;
        let profiles = tables
            .iter()
            .map(|table| (*table, Priority::Realtime))
            .collect::<BTreeMap<_, _>>();
        let mut ledger = open_ledger(&store, &source);
        let transactions = (1..=600)
            .map(|xid| descriptor(&source, xid, &tables, 10))
            .collect::<Vec<_>>();
        journal(&mut ledger, &transactions);
        let mut scheduler =
            Scheduler::new(EPOCH_MUTATION_TRIGGER, EPOCH_MAX_BYTES, 100, Instant::now()).unwrap();
        let mut pending = PendingWork::default();
        pending
            .refill(&ledger, &mut scheduler, &profiles, 16, &HashSet::new())
            .unwrap();
        assert_eq!(pending.loaded, 16);
        let all = (1..=600).collect::<Vec<_>>();

        let epoch = pending.take_epoch(first, &ledger).unwrap();
        assert_eq!(xids(&epoch), all);
        publish(&mut ledger, &mut pending, first, &epoch);
        assert_eq!(ledger.acknowledgement(), PgLsn(0));

        // A failed worker returns; its whole epoch is released, not completed.
        let failed = pending.take_epoch(third, &ledger).unwrap();
        assert_eq!(xids(&failed), all);
        pending.defer(third, &mut scheduler);
        assert!(!pending.holds(third));
        assert_eq!(pending.running, 0);

        // The second table publishes half its epoch before the process stops.
        pending
            .refill(&ledger, &mut scheduler, &profiles, 16, &HashSet::new())
            .unwrap();
        let epoch = pending.take_epoch(second, &ledger).unwrap();
        assert_eq!(xids(&epoch), all);
        publish(&mut ledger, &mut pending, second, &epoch[..300]);
        assert_eq!(ledger.acknowledgement(), PgLsn(0));
        drop((pending, ledger));

        let mut ledger = open_ledger(&store, &source);
        let mut pending = PendingWork::default();
        pending
            .refill(&ledger, &mut scheduler, &profiles, 16, &HashSet::new())
            .unwrap();
        assert!(pending.tables.get(&first).is_none_or(VecDeque::is_empty));
        let epoch = pending.take_epoch(third, &ledger).unwrap();
        assert_eq!(xids(&epoch), all);
        publish(&mut ledger, &mut pending, third, &epoch);
        assert_eq!(
            ledger.acknowledgement(),
            PgLsn(3001),
            "the frontier stops at the second table's published prefix"
        );
        let epoch = pending.take_epoch(second, &ledger).unwrap();
        assert_eq!(xids(&epoch), (301..=600).collect::<Vec<_>>());
        publish(&mut ledger, &mut pending, second, &epoch);
        assert_eq!(ledger.acknowledgement(), PgLsn(6001));
        assert!(!pending.has_work());
        assert_eq!(ledger.pending_count(), 0);
    }
}
