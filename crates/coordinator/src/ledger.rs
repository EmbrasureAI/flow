//! One acknowledgement ledger per logical slot, never one per table.
use anyhow::{Context, Result, ensure};
use bincode::Options;
use flow_model::{PgLsn, SourceId, SourceTransaction, TableId};
use flow_state_store::StateStore;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AckMode {
    #[default]
    Materialized,
    Journaled,
}
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum JournalDurability {
    #[default]
    LocalDisk,
    IndependentStorage,
}
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Watermarks {
    pub received_lsn: PgLsn,
    pub journal_durable_lsn: PgLsn,
    pub materialized_lsn: PgLsn,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Entry {
    transaction: SourceTransaction,
    committed_tables: BTreeMap<TableId, i64>,
}
const ENTRY_FORMAT: &[u8; 8] = b"FLLEDG03";
impl Entry {
    fn decode(bytes: &[u8]) -> Result<Self> {
        let codec = bincode::DefaultOptions::new()
            .with_fixint_encoding()
            .with_limit(bytes.len() as u64)
            .reject_trailing_bytes();
        if let Some(payload) = bytes.strip_prefix(ENTRY_FORMAT) {
            let entry: Self = codec.deserialize(payload)?;
            entry.transaction.validate_mutation_counts()?;
            return Ok(entry);
        }
        if let Some(payload) = bytes.strip_prefix(b"FLLEDG02") {
            let (transaction, committed_tables): (
                flow_ingress_journal::legacy::BoundedSourceTransaction,
                BTreeMap<TableId, i64>,
            ) = codec.deserialize(payload)?;
            return Ok(Self {
                transaction: transaction.into_current()?,
                committed_tables,
            });
        }
        ensure!(
            !bytes.starts_with(b"FLLEDG"),
            "unsupported source ledger entry version"
        );
        #[derive(Deserialize)]
        struct LegacyEntry {
            transaction: flow_ingress_journal::legacy::SourceTransaction,
            committed_tables: BTreeMap<TableId, i64>,
        }
        let legacy: LegacyEntry = codec.deserialize(bytes)?;
        Ok(Self {
            transaction: legacy.transaction.into_current()?,
            committed_tables: legacy.committed_tables,
        })
    }
    fn encode(&self) -> Result<Vec<u8>> {
        let mut bytes = ENTRY_FORMAT.to_vec();
        bincode::serialize_into(&mut bytes, self)?;
        Ok(bytes)
    }
    fn complete(&self) -> bool {
        self.transaction
            .affected_tables
            .iter()
            .all(|t| self.committed_tables.contains_key(t))
    }
}

pub struct SourceLedger {
    store: StateStore,
    source: SourceId,
    mode: AckMode,
    watermarks: Watermarks,
    pending_count: usize,
    prefix: Vec<u8>,
}
impl SourceLedger {
    pub fn open(
        store: StateStore,
        source: SourceId,
        mode: AckMode,
        durability: JournalDurability,
    ) -> Result<Self> {
        ensure!(
            mode != AckMode::Journaled || durability == JournalDurability::IndependentStorage,
            "journaled ACK requires independently durable storage; a local disk is insufficient"
        );
        let mut prefix = b"flow-ledger/v1/".to_vec();
        prefix.extend((source.0.len() as u64).to_be_bytes());
        prefix.extend(source.0.as_bytes());
        let watermarks = store
            .source_transaction(&meta_key(&prefix))?
            .map(|v| bincode::deserialize::<Watermarks>(&v))
            .transpose()?
            .unwrap_or_default();
        let mut ledger = Self {
            store,
            source,
            mode,
            watermarks,
            pending_count: 0,
            prefix,
        };
        let mut count = 0usize;
        let mut latest = ledger.watermarks.journal_durable_lsn;
        for entry in ledger.entries_after(ledger.watermarks.materialized_lsn) {
            let entry = entry?;
            count = count
                .checked_add(1)
                .ok_or_else(|| anyhow::anyhow!("source ledger count overflow"))?;
            latest = latest.max(entry.transaction.end_lsn);
        }
        ledger.pending_count = count;
        // Recover the older protocol's crash between entry sync and meta sync.
        ledger.watermarks.journal_durable_lsn = latest;
        ledger.watermarks.received_lsn = ledger.watermarks.received_lsn.max(latest);
        ledger.ensure_table_index()?;
        // Reclaim stale entries from the older entry/meta/delete write protocol
        // in the same batch as any recovered watermark advancement.
        ledger.persist_updates(BTreeMap::new(), ledger.watermarks.clone())?;
        Ok(ledger)
    }
    pub fn watermarks(&self) -> &Watermarks {
        &self.watermarks
    }
    pub fn acknowledgement(&self) -> PgLsn {
        match self.mode {
            AckMode::Materialized => self.watermarks.materialized_lsn,
            AckMode::Journaled => self.watermarks.journal_durable_lsn,
        }
    }
    pub fn received(&mut self, lsn: PgLsn) {
        self.watermarks.received_lsn = self.watermarks.received_lsn.max(lsn);
    }
    /// Maximum descriptors accepted in one atomic ledger transition.
    pub fn batch_capacity(&self) -> usize {
        self.store.batch_rows()
    }
    /// Call only after Journal::commit returned successfully. Returns false for replay.
    pub fn journaled(&mut self, transaction: SourceTransaction) -> Result<bool> {
        Ok(self.journaled_batch(std::slice::from_ref(&transaction))? != 0)
    }
    /// Register a bounded, commit-ordered journal page with one durable write.
    /// Replay is idempotent; any invalid descriptor rejects the entire page.
    pub fn journaled_batch(&mut self, transactions: &[SourceTransaction]) -> Result<usize> {
        ensure!(
            transactions.len() <= self.batch_capacity(),
            "source ledger batch exceeds capacity"
        );
        let mut changed: BTreeMap<PgLsn, Entry> = BTreeMap::new();
        let mut next = self.watermarks.clone();
        for transaction in transactions {
            transaction.validate_mutation_counts()?;
            ensure!(
                transaction.source_id == self.source,
                "transaction belongs to a different source incarnation"
            );
            ensure!(
                transaction.begin_lsn <= transaction.commit_lsn
                    && transaction.commit_lsn < transaction.end_lsn,
                "invalid transaction LSN ordering"
            );
            let unique: BTreeSet<_> = transaction.affected_tables.iter().collect();
            ensure!(
                unique.len() == transaction.affected_tables.len(),
                "duplicate affected table"
            );
            let end = transaction.end_lsn;
            if end <= self.watermarks.materialized_lsn {
                continue;
            }
            let persisted = if end <= self.watermarks.journal_durable_lsn {
                self.entry(end)?
            } else {
                None
            };
            if let Some(existing) = changed.get(&end).or(persisted.as_ref()) {
                ensure!(
                    existing.transaction == *transaction,
                    "replayed transaction identity or content differs"
                );
                continue;
            }
            ensure!(
                end > next.journal_durable_lsn,
                "out-of-order transaction registration"
            );
            let mut committed_tables = BTreeMap::new();
            for table in &transaction.affected_tables {
                let state = self.store.table_state(table)?;
                if state.pending_operation.is_none() && state.materialized_lsn >= end {
                    committed_tables.insert(*table, state.snapshot_id.unwrap_or(0));
                }
            }
            changed.insert(
                end,
                Entry {
                    transaction: transaction.clone(),
                    committed_tables,
                },
            );
            next.journal_durable_lsn = end;
            next.received_lsn = next.received_lsn.max(end);
        }
        let added = changed.len();
        if added != 0 {
            self.persist_updates(changed, next)?;
        }
        Ok(added)
    }
    /// Called after the catalog commit AND the full index transition are durable.
    pub fn table_materialized(
        &mut self,
        end_lsn: PgLsn,
        table: TableId,
        snapshot: i64,
    ) -> Result<()> {
        self.table_materialized_batch(&[end_lsn], table, snapshot)
    }
    /// Record one table publication's bounded transaction page atomically.
    /// Completion can arrive out of order; ACK still follows the complete prefix.
    pub fn table_materialized_batch(
        &mut self,
        end_lsns: &[PgLsn],
        table: TableId,
        snapshot: i64,
    ) -> Result<()> {
        ensure!(
            end_lsns.len() <= self.batch_capacity(),
            "source ledger batch exceeds capacity"
        );
        let mut changed = BTreeMap::new();
        let mut modified = false;
        for end in end_lsns {
            if *end <= self.watermarks.materialized_lsn {
                continue;
            }
            let entry = match changed.entry(*end) {
                std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
                std::collections::btree_map::Entry::Vacant(entry) => entry.insert(
                    self.entry(*end)?
                        .ok_or_else(|| anyhow::anyhow!("unknown source transaction {end}"))?,
                ),
            };
            ensure!(
                entry.transaction.affected_tables.contains(&table),
                "table is not part of transaction"
            );
            if let Some(old) = entry.committed_tables.get(&table) {
                ensure!(*old == snapshot, "table publication changed on replay");
            } else {
                entry.committed_tables.insert(table, snapshot);
                modified = true;
            }
        }
        if modified {
            self.persist_updates(changed, self.watermarks.clone())?;
        }
        Ok(())
    }
    /// Recover the crash between index application and ledger completion.
    pub fn reconcile_table_progress(&mut self) -> Result<()> {
        let prefix = table_index_prefix(&self.prefix);
        let mut after = None;
        loop {
            // Seek directly to the next table prefix. Its retained backlog may
            // contain millions of references, but identifying the table takes
            // one lookup even when none of them can yet be completed.
            let next = self
                .store
                .source_transactions_after(&prefix, after.as_deref())
                .next()
                .transpose()?;
            let Some((key, _)) = next else { break };
            let suffix = key
                .strip_prefix(prefix.as_slice())
                .context("table admission reference has the wrong prefix")?;
            ensure!(
                suffix.len() == 13 && suffix[4] == b'/',
                "invalid table admission reference key"
            );
            let table = TableId(u32::from_be_bytes(suffix[..4].try_into()?));
            after = Some(table_entry_key(&self.prefix, table, PgLsn(u64::MAX)));
            let state = self.store.table_state(&table)?;
            if state.pending_operation.is_some() {
                continue;
            }
            let mut through = PgLsn(0);
            loop {
                let ends = self
                    .pending_table_transactions_after(table, through)
                    .take_while(|result| match result {
                        Ok(transaction) => transaction.end_lsn <= state.materialized_lsn,
                        Err(_) => true,
                    })
                    .take(self.batch_capacity())
                    .map(|transaction| transaction.map(|transaction| transaction.end_lsn))
                    .collect::<Result<Vec<_>>>()?;
                let Some(last) = ends.last() else { break };
                through = *last;
                self.table_materialized_batch(&ends, table, state.snapshot_id.unwrap_or(0))?;
            }
        }
        Ok(())
    }
    pub fn pending_tables(&self, end_lsn: PgLsn) -> Result<Vec<TableId>> {
        if end_lsn <= self.watermarks.materialized_lsn {
            return Ok(Vec::new());
        }
        let entry = self
            .entry(end_lsn)?
            .ok_or_else(|| anyhow::anyhow!("unknown source transaction {end_lsn}"))?;
        Ok(entry
            .transaction
            .affected_tables
            .into_iter()
            .filter(|table| !entry.committed_tables.contains_key(table))
            .collect())
    }
    pub fn pending_count(&self) -> usize {
        self.pending_count
    }
    /// Seek a table's durable pending references without scanning other tables'
    /// retained transactions. The admission cursor is disposable: completion
    /// removes references atomically, so a restart can safely begin at zero.
    pub fn pending_table_transactions_after(
        &self,
        table: TableId,
        through: PgLsn,
    ) -> impl Iterator<Item = Result<SourceTransaction>> + '_ {
        let prefix = table_entry_prefix(&self.prefix, table);
        let after = table_entry_key(&self.prefix, table, through);
        self.store
            .source_transactions_after(&prefix, Some(&after))
            .map(move |item| {
                let (key, value) = item?;
                ensure!(value.is_empty(), "invalid table admission reference value");
                let suffix = key
                    .strip_prefix(prefix.as_slice())
                    .context("table admission reference has the wrong prefix")?;
                let end = PgLsn(u64::from_be_bytes(
                    suffix
                        .try_into()
                        .context("invalid table admission reference key")?,
                ));
                let entry = self
                    .entry(end)?
                    .context("table admission reference has no source transaction")?;
                ensure!(
                    entry.transaction.source_id == self.source
                        && entry.transaction.end_lsn == end
                        && entry.transaction.affected_tables.contains(&table)
                        && !entry.committed_tables.contains_key(&table),
                    "table admission reference disagrees with source progress"
                );
                Ok(entry.transaction)
            })
    }

    /// A delayed table can release a large already-completed source prefix.
    /// Retire it in bounded pages so cleanup and ACK catch-up yield
    /// between pages instead of holding the publication loop for the backlog.
    pub fn has_completed_prefix(&self) -> Result<bool> {
        Ok(self
            .entries_after(self.watermarks.materialized_lsn)
            .next()
            .transpose()?
            .is_some_and(|entry| entry.complete()))
    }

    pub fn drain_completed_prefix(&mut self) -> Result<bool> {
        if !self.has_completed_prefix()? {
            return Ok(false);
        }
        self.persist_updates(BTreeMap::new(), self.watermarks.clone())?;
        Ok(true)
    }
    pub fn pending_transactions(&self) -> impl Iterator<Item = Result<SourceTransaction>> + '_ {
        self.pending_transactions_after(self.watermarks.materialized_lsn)
    }
    pub fn pending_transactions_after(
        &self,
        lsn: PgLsn,
    ) -> impl Iterator<Item = Result<SourceTransaction>> + '_ {
        self.entries_after(lsn.max(self.watermarks.materialized_lsn))
            .map(|entry| Ok(entry?.transaction))
    }
    fn entry(&self, end_lsn: PgLsn) -> Result<Option<Entry>> {
        self.store
            .source_transaction(&entry_key(&self.prefix, end_lsn))?
            .map(|bytes| Entry::decode(&bytes))
            .transpose()
    }
    fn entries_after(&self, lsn: PgLsn) -> impl Iterator<Item = Result<Entry>> + '_ {
        self.store
            .source_transactions_after(
                &entry_prefix(&self.prefix),
                Some(&entry_key(&self.prefix, lsn)),
            )
            .map(|item| {
                let (key, bytes) = item?;
                let entry = Entry::decode(&bytes)?;
                ensure!(
                    entry.transaction.source_id == self.source
                        && key.as_ref()
                            == entry_key(&self.prefix, entry.transaction.end_lsn).as_slice(),
                    "source ledger key and transaction identity disagree"
                );
                Ok(entry)
            })
    }

    fn ensure_table_index(&self) -> Result<()> {
        let marker = table_index_marker(&self.prefix);
        let mut through = match self.store.source_transaction(&marker)? {
            Some(bytes) => PgLsn(u64::from_be_bytes(
                bytes
                    .as_slice()
                    .try_into()
                    .context("invalid table admission index version")?,
            )),
            None => PgLsn(0),
        };
        ensure!(
            through <= self.watermarks.journal_durable_lsn,
            "table admission index exceeds durable source progress"
        );
        // Older write protocols can leave descriptor rows below an already
        // durable completed watermark. Never create admission references for
        // those rows: reopen reclaims them immediately after this migration.
        through = through.max(self.watermarks.materialized_lsn);
        while through < self.watermarks.journal_durable_lsn {
            let entries = self
                .entries_after(through)
                .take(self.batch_capacity())
                .collect::<Result<Vec<_>>>()?;
            let last = entries.last().context(
                "durable source progress has no retained transaction during table index migration",
            )?;
            through = last.transaction.end_lsn;
            let mut references = Vec::new();
            for entry in &entries {
                for table in &entry.transaction.affected_tables {
                    if !entry.committed_tables.contains_key(table) {
                        references.push(table_entry_key(
                            &self.prefix,
                            *table,
                            entry.transaction.end_lsn,
                        ));
                    }
                }
            }
            // Reference fanout and migration progress share one durable write.
            // A crash resumes after the last whole page; no payload is copied.
            self.store.update_source_ledger(
                (&marker, &through.0.to_be_bytes()),
                references.iter().map(|key| (key.as_slice(), [].as_slice())),
                None,
            )?;
        }
        Ok(())
    }
    fn persist_updates(
        &mut self,
        changed: BTreeMap<PgLsn, Entry>,
        mut next: Watermarks,
    ) -> Result<()> {
        let added = changed
            .keys()
            .filter(|end| **end > self.watermarks.journal_durable_lsn)
            .count();
        let mut completed = 0usize;
        // A blocked oldest transaction requires one lookup. Replacing entries
        // from this bounded page never collects the rest of the retained backlog.
        for entry in self
            .entries_after(self.watermarks.materialized_lsn)
            .take(self.batch_capacity())
        {
            let entry = entry?;
            let updated = changed.get(&entry.transaction.end_lsn).unwrap_or(&entry);
            if !updated.complete() {
                break;
            }
            next.materialized_lsn = entry.transaction.end_lsn;
            completed = completed
                .checked_add(1)
                .ok_or_else(|| anyhow::anyhow!("source ledger count overflow"))?;
        }
        // New entries follow every persisted transaction. They can extend the
        // completed prefix only after all previously retained entries completed.
        if completed == self.pending_count {
            for entry in changed
                .values()
                .filter(|entry| entry.transaction.end_lsn > self.watermarks.journal_durable_lsn)
                .take(self.batch_capacity().saturating_sub(completed))
            {
                if !entry.complete() {
                    break;
                }
                next.materialized_lsn = entry.transaction.end_lsn;
                completed = completed
                    .checked_add(1)
                    .ok_or_else(|| anyhow::anyhow!("source ledger count overflow"))?;
            }
        }
        let pending_count = self
            .pending_count
            .checked_add(added)
            .and_then(|count| count.checked_sub(completed))
            .ok_or_else(|| anyhow::anyhow!("source ledger count is inconsistent"))?;
        let metadata_key = meta_key(&self.prefix);
        let metadata = bincode::serialize(&next)?;
        let mut transactions = changed
            .values()
            .filter(|entry| entry.transaction.end_lsn > next.materialized_lsn)
            .map(|entry| {
                Ok((
                    entry_key(&self.prefix, entry.transaction.end_lsn),
                    entry.encode()?,
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        let mut reference_deletes = Vec::new();
        for entry in changed.values() {
            for table in &entry.transaction.affected_tables {
                let key = table_entry_key(&self.prefix, *table, entry.transaction.end_lsn);
                if entry.committed_tables.contains_key(table) {
                    reference_deletes.push(key);
                } else {
                    transactions.push((key, Vec::new()));
                }
            }
        }
        transactions.push((
            table_index_marker(&self.prefix),
            next.journal_durable_lsn.0.to_be_bytes().to_vec(),
        ));
        let completed_range = if changed.is_empty() && next.materialized_lsn > PgLsn(0) {
            // Reopen also removes stale records left by the older write protocol.
            Some((
                entry_prefix(&self.prefix),
                after_entry(&self.prefix, next.materialized_lsn),
            ))
        } else if next.materialized_lsn > self.watermarks.materialized_lsn {
            Some((
                after_entry(&self.prefix, self.watermarks.materialized_lsn),
                after_entry(&self.prefix, next.materialized_lsn),
            ))
        } else {
            None
        };
        self.store.update_source_ledger_with_deletes(
            (&metadata_key, &metadata),
            transactions
                .iter()
                .map(|(key, value)| (key.as_slice(), value.as_slice())),
            reference_deletes.iter().map(Vec::as_slice),
            completed_range
                .as_ref()
                .map(|(start, end)| (start.as_slice(), end.as_slice())),
        )?;
        // An unsuccessful write must not advance the observable ACK or count.
        self.watermarks = next;
        self.pending_count = pending_count;
        Ok(())
    }
}
fn meta_key(prefix: &[u8]) -> Vec<u8> {
    [prefix, b"/meta"].concat()
}
fn entry_prefix(prefix: &[u8]) -> Vec<u8> {
    [prefix, b"/txn/"].concat()
}
fn entry_key(prefix: &[u8], end: PgLsn) -> Vec<u8> {
    let mut key = entry_prefix(prefix);
    key.extend(end.0.to_be_bytes());
    key
}
fn after_entry(prefix: &[u8], end: PgLsn) -> Vec<u8> {
    let mut key = entry_key(prefix, end);
    key.push(0);
    key
}

fn table_index_marker(prefix: &[u8]) -> Vec<u8> {
    [prefix, b"/table-index/v1/through"].concat()
}

fn table_entry_prefix(prefix: &[u8], table: TableId) -> Vec<u8> {
    let mut key = table_index_prefix(prefix);
    key.extend(table.0.to_be_bytes());
    key.push(b'/');
    key
}

fn table_index_prefix(prefix: &[u8]) -> Vec<u8> {
    [prefix, b"/table-index/v1/pending/"].concat()
}

fn table_entry_key(prefix: &[u8], table: TableId, end: PgLsn) -> Vec<u8> {
    let mut key = table_entry_prefix(prefix, table);
    key.extend(end.0.to_be_bytes());
    key
}

#[cfg(test)]
mod table_index_tests {
    use super::*;
    use flow_state_store::{ControlStore, StateStoreOptions};

    fn transaction(source: &SourceId, xid: u32) -> SourceTransaction {
        SourceTransaction {
            source_id: source.clone(),
            xid,
            begin_lsn: PgLsn(u64::from(xid) * 10 - 1),
            commit_lsn: PgLsn(u64::from(xid) * 10),
            end_lsn: PgLsn(u64::from(xid) * 10 + 1),
            commit_timestamp_micros: 0,
            schema_versions: vec![],
            affected_tables: vec![TableId(1), TableId(2)],
            mutation_chunks: Default::default(),
            table_mutation_counts: None,
        }
    }

    fn pending(ledger: &SourceLedger, table: u32) -> Vec<u32> {
        ledger
            .pending_table_transactions_after(TableId(table), PgLsn(0))
            .map(|transaction| transaction.unwrap().xid)
            .collect()
    }

    #[test]
    fn reference_migration_resumes_and_preserves_partial_tables_after_control_reopen() {
        let directory = tempfile::tempdir().unwrap();
        let options = StateStoreOptions {
            apply_batch_rows: 2,
            ..Default::default()
        };
        let control = ControlStore::open(directory.path().join("control")).unwrap();
        let store = control
            .initialize_index(directory.path().join("index"), options.clone())
            .unwrap();
        let source = SourceId("migration".into());
        let mut ledger = SourceLedger::open(
            store.clone(),
            source.clone(),
            AckMode::Materialized,
            JournalDurability::LocalDisk,
        )
        .unwrap();
        for xid in 1..=7 {
            ledger.journaled(transaction(&source, xid)).unwrap();
        }
        for xid in [1, 3, 5, 7] {
            ledger
                .table_materialized(PgLsn(xid * 10 + 1), TableId(1), 100)
                .unwrap();
        }
        assert_eq!(pending(&ledger, 1), [2, 4, 6]);
        let prefix = ledger.prefix.clone();
        // A legacy database has no reference index. Simulate interruption after
        // its first two-descriptor migration page is durable.
        for table in [TableId(1), TableId(2)] {
            for xid in 1..=7 {
                store
                    .delete_source_transaction(&table_entry_key(
                        &prefix,
                        table,
                        PgLsn(xid * 10 + 1),
                    ))
                    .unwrap();
            }
        }
        let refs = [
            table_entry_key(&prefix, TableId(1), PgLsn(21)),
            table_entry_key(&prefix, TableId(2), PgLsn(11)),
            table_entry_key(&prefix, TableId(2), PgLsn(21)),
        ];
        store
            .update_source_ledger(
                (&table_index_marker(&prefix), &21u64.to_be_bytes()),
                refs.iter().map(|key| (key.as_slice(), [].as_slice())),
                None,
            )
            .unwrap();
        drop(ledger);
        drop(store);
        drop(control);
        let control = ControlStore::open(directory.path().join("control")).unwrap();
        let store =
            StateStore::open_with_control(directory.path().join("index"), options, control.clone())
                .unwrap();
        let mut ledger = SourceLedger::open(
            store.clone(),
            source.clone(),
            AckMode::Materialized,
            JournalDurability::LocalDisk,
        )
        .unwrap();
        assert_eq!(pending(&ledger, 1), [2, 4, 6]);
        assert_eq!(pending(&ledger, 2), [1, 2, 3, 4, 5, 6, 7]);
        assert_eq!(ledger.acknowledgement(), PgLsn(0));
        assert_eq!(
            control
                .source_transaction(&table_index_marker(&prefix))
                .unwrap(),
            Some(71u64.to_be_bytes().to_vec())
        );
        ledger
            .table_materialized(PgLsn(21), TableId(1), 100)
            .unwrap();
        assert_eq!(pending(&ledger, 1), [4, 6]);
        assert!(
            control
                .source_transaction(&table_entry_key(&prefix, TableId(1), PgLsn(21)))
                .unwrap()
                .is_none()
        );
        drop(ledger);
        let ledger = SourceLedger::open(
            store,
            source,
            AckMode::Materialized,
            JournalDurability::LocalDisk,
        )
        .unwrap();
        assert_eq!(pending(&ledger, 1), [4, 6]);
    }

    #[test]
    fn missing_descriptor_and_invalid_reference_fail_closed() {
        let directory = tempfile::tempdir().unwrap();
        let store = StateStore::open(directory.path(), StateStoreOptions::default()).unwrap();
        let source = SourceId("corrupt-reference".into());
        let mut ledger = SourceLedger::open(
            store.clone(),
            source.clone(),
            AckMode::Materialized,
            JournalDurability::LocalDisk,
        )
        .unwrap();
        ledger.journaled(transaction(&source, 1)).unwrap();
        store
            .delete_source_transaction(&entry_key(&ledger.prefix, PgLsn(11)))
            .unwrap();
        assert!(
            ledger
                .pending_table_transactions_after(TableId(1), PgLsn(0))
                .next()
                .unwrap()
                .is_err()
        );
        let bad = table_entry_key(&ledger.prefix, TableId(2), PgLsn(11));
        store
            .put_source_transaction(&bad, b"not-a-reference")
            .unwrap();
        assert!(
            ledger
                .pending_table_transactions_after(TableId(2), PgLsn(0))
                .next()
                .unwrap()
                .is_err()
        );
    }

    #[test]
    fn recovering_the_oldest_transaction_drains_only_one_bounded_prefix_page() {
        let directory = tempfile::tempdir().unwrap();
        let store = StateStore::open(
            directory.path(),
            StateStoreOptions {
                apply_batch_rows: 2,
                ..Default::default()
            },
        )
        .unwrap();
        let source = SourceId("bounded-drain".into());
        let mut ledger = SourceLedger::open(
            store,
            source.clone(),
            AckMode::Materialized,
            JournalDurability::LocalDisk,
        )
        .unwrap();
        for xid in 1..=9 {
            ledger.journaled(transaction(&source, xid)).unwrap();
        }
        for xid in 1..=9 {
            ledger
                .table_materialized(PgLsn(xid * 10 + 1), TableId(1), 100)
                .unwrap();
            if xid != 1 {
                ledger
                    .table_materialized(PgLsn(xid * 10 + 1), TableId(2), 101)
                    .unwrap();
            }
        }
        assert_eq!(pending(&ledger, 1), Vec::<u32>::new());
        assert_eq!(pending(&ledger, 2), [1]);
        ledger
            .table_materialized(PgLsn(11), TableId(2), 101)
            .unwrap();
        assert_eq!(ledger.acknowledgement(), PgLsn(21));
        assert_eq!(ledger.pending_count(), 7);
        let mut pages = Vec::new();
        while ledger.has_completed_prefix().unwrap() {
            assert!(ledger.drain_completed_prefix().unwrap());
            pages.push(ledger.acknowledgement());
        }
        assert_eq!(pages, [PgLsn(41), PgLsn(61), PgLsn(81), PgLsn(91)]);
        assert_eq!(ledger.pending_count(), 0);
        assert!(!ledger.drain_completed_prefix().unwrap());
    }

    #[test]
    fn noop_index_progress_reconciles_pending_references_without_an_applied_operation() {
        let directory = tempfile::tempdir().unwrap();
        let store = StateStore::open(
            directory.path(),
            StateStoreOptions {
                apply_batch_rows: 2,
                ..Default::default()
            },
        )
        .unwrap();
        let source = SourceId("noop-recovery".into());
        let mut ledger = SourceLedger::open(
            store.clone(),
            source.clone(),
            AckMode::Materialized,
            JournalDurability::LocalDisk,
        )
        .unwrap();
        for xid in 1..=7 {
            ledger.journaled(transaction(&source, xid)).unwrap();
        }
        store.complete_noop(&TableId(1), PgLsn(71), 1).unwrap();
        assert!(store.applied_operations(1).unwrap().is_empty());
        drop(ledger);
        let mut ledger = SourceLedger::open(
            store.clone(),
            source,
            AckMode::Materialized,
            JournalDurability::LocalDisk,
        )
        .unwrap();
        ledger.reconcile_table_progress().unwrap();
        assert_eq!(pending(&ledger, 1), Vec::<u32>::new());
        assert_eq!(pending(&ledger, 2), [1, 2, 3, 4, 5, 6, 7]);
        assert_eq!(ledger.acknowledgement(), PgLsn(0));
        store.complete_noop(&TableId(2), PgLsn(71), 1).unwrap();
        ledger.reconcile_table_progress().unwrap();
        while ledger.drain_completed_prefix().unwrap() {}
        assert_eq!(ledger.acknowledgement(), PgLsn(71));
        assert_eq!(ledger.pending_count(), 0);
        ledger.reconcile_table_progress().unwrap();
    }

    #[test]
    fn reference_migration_rejects_missing_durable_descriptors_on_reopen() {
        for partial_marker in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let store = StateStore::open(directory.path(), StateStoreOptions::default()).unwrap();
            let source = SourceId("missing-migration-descriptor".into());
            let mut ledger = SourceLedger::open(
                store.clone(),
                source.clone(),
                AckMode::Materialized,
                JournalDurability::LocalDisk,
            )
            .unwrap();
            ledger.journaled(transaction(&source, 1)).unwrap();
            ledger.journaled(transaction(&source, 2)).unwrap();
            let prefix = ledger.prefix.clone();
            assert_eq!(ledger.watermarks().journal_durable_lsn, PgLsn(21));
            assert_eq!(ledger.acknowledgement(), PgLsn(0));
            store
                .delete_source_transaction(&entry_key(&prefix, PgLsn(21)))
                .unwrap();
            let marker = table_index_marker(&prefix);
            if partial_marker {
                store
                    .put_source_transaction(&marker, &11u64.to_be_bytes())
                    .unwrap();
            } else {
                store.delete_source_transaction(&marker).unwrap();
            }
            drop(ledger);
            let reopened = SourceLedger::open(
                store.clone(),
                source,
                AckMode::Materialized,
                JournalDurability::LocalDisk,
            );
            let error = reopened
                .err()
                .expect("missing durable descriptor must reject migration");
            assert!(
                error.to_string().contains("no retained transaction"),
                "{error}"
            );
            assert_eq!(
                store.source_transaction(&marker).unwrap(),
                Some(11u64.to_be_bytes().to_vec()),
                "migration must never bless missing durable source history"
            );
        }
    }
}
