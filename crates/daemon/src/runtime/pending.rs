//! Bounded transaction admission while preserving each table's source order.

use anyhow::{Context, Result};
use flow_coordinator::{Priority, Scheduler, SourceLedger};
use flow_model::{PgLsn, SourceTransaction, TableId};
use std::{
    collections::{BTreeMap, VecDeque},
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
    pub(super) loaded: BTreeMap<PgLsn, usize>,
    through: PgLsn,
}
impl PendingWork {
    /// Dequeue a table's ordered prefix without splitting a source transaction.
    /// Readiness never waits to fill a batch. Already-queued ordinary work may
    /// exceed the row trigger, bounded by payload bytes and the loaded window.
    /// Unknown legacy counts and oversized transactions each publish alone.
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
            // Only restore the dequeued prefix. The scheduler already owns
            // the tail and any transactions loaded while this job was running.
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
    ) -> Result<()> {
        for transaction in ledger
            .pending_transactions_after(self.through)
            .take(capacity.saturating_sub(self.loaded.len()))
        {
            let transaction = transaction?;
            self.through = transaction.end_lsn;
            let mut tables = 0usize;
            for table in ledger.pending_tables(transaction.end_lsn)? {
                self.tables
                    .entry(table)
                    .or_default()
                    .push_back(transaction.clone());
                scheduler.push(
                    table,
                    transaction.mutation_count(table),
                    transaction.mutation_chunks.payload_bytes(),
                    profiles[&table],
                    source_age(&transaction),
                    Instant::now(),
                );
                tables += 1;
            }
            if tables > 0 {
                self.loaded.insert(transaction.end_lsn, tables);
            }
        }
        Ok(())
    }
    pub(super) fn complete(&mut self, end: PgLsn) -> Result<()> {
        let remaining = self
            .loaded
            .get_mut(&end)
            .context("completed transaction is outside the publication window")?;
        *remaining -= 1;
        if *remaining == 0 {
            self.loaded.remove(&end);
        }
        Ok(())
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
            .refill(&ledger, &mut scheduler, &profiles, 6)
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
            .refill(&ledger, &mut scheduler, &profiles, 9)
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
                pending.complete(txn.end_lsn).unwrap();
            }
        }
        assert!(pending.tables[&first].is_empty());
        assert_eq!(
            pending.loaded.len(),
            9,
            "another affected table still owns every slot"
        );
        assert_eq!(ledger.acknowledgement(), PgLsn(0));
        loop {
            let epoch = pending.take_epoch(second);
            if epoch.is_empty() {
                break;
            }
            for txn in epoch {
                ledger.table_materialized(txn.end_lsn, second, 101).unwrap();
                pending.complete(txn.end_lsn).unwrap();
            }
        }
        assert!(pending.loaded.is_empty());
        assert_eq!(ledger.pending_count(), 0);
        assert_eq!(ledger.acknowledgement(), PgLsn(91));
    }
}
