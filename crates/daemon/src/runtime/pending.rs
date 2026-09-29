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

#[derive(Default)]
pub(super) struct PendingWork {
    pub(super) tables: BTreeMap<TableId, VecDeque<SourceTransaction>>,
    // Reservations include queued and running descriptors. A failure releases
    // both only after the actual worker has returned; the ledger owns replay.
    reserved: BTreeMap<TableId, BTreeSet<PgLsn>>,
    loaded: usize,
    through: BTreeMap<TableId, PgLsn>,
    next_after: Option<TableId>,
    observed_registered: PgLsn,
    excluded: HashSet<TableId>,
    capacity: usize,
    unloaded: bool,
}
impl PendingWork {
    /// Dequeue a table's ordered prefix without splitting a source transaction.
    pub(super) fn take_epoch(&mut self, table: TableId) -> Vec<SourceTransaction> {
        let queue = self.tables.entry(table).or_default();
        let mut transactions = Vec::new();
        let mut bytes = 0u64;
        while let Some(transaction) = queue.front() {
            let count = transaction.mutation_count(table);
            let size = transaction.mutation_chunks.payload_bytes();
            let standalone =
                count.is_none_or(|rows| rows > EPOCH_MUTATION_TRIGGER) || size > EPOCH_MAX_BYTES;
            if !transactions.is_empty()
                && (standalone || bytes.saturating_add(size) > EPOCH_MAX_BYTES)
            {
                break;
            }
            bytes = bytes.saturating_add(size);
            transactions.push(queue.pop_front().expect("queue front exists"));
            if standalone || bytes >= EPOCH_MAX_BYTES {
                break;
            }
        }
        transactions
    }

    pub(super) fn restore(
        &mut self,
        id: TableId,
        transactions: Vec<SourceTransaction>,
        scheduler: &mut Scheduler,
        priority: Priority,
    ) {
        if !transactions.is_empty() {
            schedule_transactions(id, &transactions, scheduler, priority);
            let queue = self.tables.entry(id).or_default();
            for transaction in transactions.into_iter().rev() {
                queue.push_front(transaction);
            }
        }
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

    pub(super) fn complete(&mut self, table: TableId, end: PgLsn) -> Result<()> {
        let reserved = self
            .reserved
            .get_mut(&table)
            .context("completed table is outside the publication window")?;
        ensure!(
            reserved.remove(&end),
            "completed transaction is outside the publication window"
        );
        self.loaded -= 1;
        self.unloaded = true;
        Ok(())
    }

    /// Evict admission after this table's worker returns. Its durable index and
    /// unfinished operation retain all changes without occupying another lane.
    pub(super) fn defer(&mut self, table: TableId, scheduler: &mut Scheduler) {
        self.loaded -= self
            .reserved
            .remove(&table)
            .map_or(0, |entries| entries.len());
        self.tables.remove(&table);
        self.through.remove(&table);
        scheduler.remove(table);
        self.unloaded = true;
    }

    /// Whether the table holds any admission reservation or queued work.
    pub(super) fn holds(&self, table: TableId) -> bool {
        self.reserved.contains_key(&table) || self.tables.contains_key(&table)
    }

    pub(super) fn has_work(&self) -> bool {
        self.loaded > 0
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
    fn restored_prefixes_keep_whole_transactions_and_all_table_ack_order() {
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
        let taken = pending.take_epoch(first);
        assert_eq!(
            taken.iter().map(|txn| txn.xid).collect::<Vec<_>>(),
            [1, 2],
            "the row trigger does not cap already-queued ordinary work"
        );
        schedule_transactions(
            first,
            &pending.tables[&first],
            &mut scheduler,
            Priority::Realtime,
        );
        // New work arrives while the dequeued prefix is in flight. A retry puts
        // only that prefix back ahead of the existing tail and the new record.
        ledger
            .journaled_batch(&[
                transaction(7, Some(10_000), EPOCH_MAX_BYTES - 1),
                transaction(8, Some(1), 1),
                transaction(9, Some(0), 1),
            ])
            .unwrap();
        pending
            .refill(&ledger, &mut scheduler, &profiles, 18, &HashSet::new())
            .unwrap();
        pending.restore(first, taken, &mut scheduler, Priority::Realtime);
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
            let epoch = pending.take_epoch(first);
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
            let epoch = pending.take_epoch(second);
            if epoch.is_empty() {
                break;
            }
            for txn in epoch {
                ledger.table_materialized(txn.end_lsn, second, 101).unwrap();
                pending.complete(second, txn.end_lsn).unwrap();
            }
        }
        assert_eq!(pending.loaded, 0);
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
        let failed = pending.take_epoch(blocked);
        assert_eq!(failed.len(), 1);
        assert_eq!(
            pending.loaded, 2,
            "running descriptors still reserve memory"
        );
        pending.defer(blocked, &mut scheduler);
        let exclusions = HashSet::from([blocked]);
        for expected in 1..=20 {
            pending
                .refill(&ledger, &mut scheduler, &profiles, 2, &exclusions)
                .unwrap();
            assert!(pending.loaded <= 2);
            let epoch = pending.take_epoch(healthy);
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
            let epoch = pending.take_epoch(blocked);
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
        for xid in 1..=3 {
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
            let epoch = pending.take_epoch(id);
            ledger
                .table_materialized(epoch[0].end_lsn, id, 100)
                .unwrap();
            pending.complete(id, epoch[0].end_lsn).unwrap();
        }
        assert_eq!(admitted, profiles.keys().copied().collect::<Vec<_>>());
        let blocked = profiles
            .keys()
            .copied()
            .filter(|id| *id != TableId(1))
            .collect();
        pending
            .refill(&ledger, &mut scheduler, &profiles, 4, &blocked)
            .unwrap();
        let epoch = pending.take_epoch(TableId(1));
        assert_eq!(
            epoch.iter().map(|txn| txn.xid).collect::<Vec<_>>(),
            vec![2, 3]
        );
        pending.defer(TableId(1), &mut scheduler);
        assert_eq!(pending.loaded, 0);
        assert!(scheduler.next_deadline().is_none());
        pending
            .refill(&ledger, &mut scheduler, &profiles, 4, &blocked)
            .unwrap();
        assert_eq!(
            pending
                .take_epoch(TableId(1))
                .iter()
                .map(|txn| txn.xid)
                .collect::<Vec<_>>(),
            vec![2, 3]
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
        let taken = pending.take_epoch(healthy);
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
        // The run takes its epoch; the latch lands; the run is deferred.
        assert_eq!(scheduler.take_ready(Instant::now()), Some(blocked));
        let taken = pending.take_epoch(blocked);
        pending.restore(blocked, taken, &mut scheduler, Priority::Realtime);
        ledger.journaled_batch(&[transaction(3, healthy)]).unwrap();
        let excluded = HashSet::from([blocked]);
        pending
            .refill(&ledger, &mut scheduler, &profiles, 2, &excluded)
            .unwrap();
        assert!(
            !pending.tables.contains_key(&healthy),
            "the leak this fixes"
        );

        // No run owns it any more, and it still holds reservations.
        assert!(pending.holds(blocked));
        pending.defer(blocked, &mut scheduler);
        assert!(!pending.holds(blocked));
        pending
            .refill(&ledger, &mut scheduler, &profiles, 2, &excluded)
            .unwrap();
        assert_eq!(
            pending.tables[&healthy].len(),
            1,
            "the healthy table is admitted"
        );
    }
}
