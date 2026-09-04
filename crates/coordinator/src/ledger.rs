//! One acknowledgement ledger per logical slot, never one per table.
use anyhow::{Result, ensure};
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
        let mut cursor = self.watermarks.materialized_lsn;
        loop {
            let next = self.entries_after(cursor).next().transpose()?;
            let Some(entry) = next else { break };
            cursor = entry.transaction.end_lsn;
            for table in &entry.transaction.affected_tables {
                if entry.committed_tables.contains_key(table) {
                    continue;
                }
                let state = self.store.table_state(table)?;
                if state.pending_operation.is_none() && state.materialized_lsn >= cursor {
                    self.table_materialized(cursor, *table, state.snapshot_id.unwrap_or(0))?;
                }
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
        for entry in self.entries_after(self.watermarks.materialized_lsn) {
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
        let transactions = changed
            .values()
            .filter(|entry| entry.transaction.end_lsn > next.materialized_lsn)
            .map(|entry| {
                Ok((
                    entry_key(&self.prefix, entry.transaction.end_lsn),
                    entry.encode()?,
                ))
            })
            .collect::<Result<Vec<_>>>()?;
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
        self.store.update_source_ledger(
            (&metadata_key, &metadata),
            transactions
                .iter()
                .map(|(key, value)| (key.as_slice(), value.as_slice())),
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
