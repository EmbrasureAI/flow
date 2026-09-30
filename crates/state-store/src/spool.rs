use crate::StateBatch as WriteBatch;
use crate::{Error, PK, Result, SPOOL, StateStore, append_component, decode, pk_key};
use flow_model::{PrimaryKey, Row, RowLocation, TableId};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, btree_map::Entry},
    time::Instant,
};

/// A complete row image; unchanged-TOAST markers must be rejected upstream.
#[derive(Debug, Clone)]
pub enum Change {
    Insert(Row),
    Update(Row),
    Delete,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CollapsedMutation {
    pub table_id: TableId,
    pub key: PrimaryKey,
    pub original: Option<RowLocation>,
    pub row: Option<Row>,
}

/// The first event constrains the indexed row; later events see only this row.
#[derive(Debug)]
pub(crate) struct PendingRow {
    required_present: bool,
    pub(crate) row: Option<Row>,
}

impl PendingRow {
    pub(crate) fn new(change: Change) -> Result<Self> {
        let required_present = !matches!(change, Change::Insert(_));
        Ok(Self {
            required_present,
            row: Self::transition(change, required_present)?,
        })
    }

    pub(crate) fn push(&mut self, change: Change) -> Result<()> {
        self.row = Self::transition(change, self.row.is_some())?;
        Ok(())
    }

    fn transition(change: Change, present: bool) -> Result<Option<Row>> {
        match change {
            Change::Insert(row) if !present => Ok(Some(row)),
            Change::Update(row) if present => Ok(Some(row)),
            Change::Delete if present => Ok(None),
            _ => Err(Self::conflict()),
        }
    }

    pub(crate) fn validate_initial(&self, present: bool) -> Result<()> {
        if present != self.required_present {
            return Err(Self::conflict());
        }
        Ok(())
    }

    fn conflict() -> Error {
        Error::InvalidState("source mutation conflicts with transaction-local key existence; reconcile before continuing".into())
    }

    pub(crate) fn into_mutation(
        self,
        table_id: TableId,
        key: PrimaryKey,
        original: Option<RowLocation>,
    ) -> CollapsedMutation {
        CollapsedMutation {
            table_id,
            key,
            original,
            row: self.row,
        }
    }
}

impl StateStore {
    /// Collapse one bounded source chunk atomically. This is the hot-path API:
    /// one WAL append per chunk; sealing supplies the durability barrier. A PK change is submitted
    /// as delete-old and insert-new in the same chunk.
    pub fn collapse_changes(
        &self,
        transaction: &str,
        changes: impl IntoIterator<Item = (TableId, PrimaryKey, Change)>,
    ) -> Result<()> {
        let started = Instant::now();
        let _guard = self.lock()?;
        if self
            .get::<bool>(SPOOL, &transaction_prefix(transaction))?
            .unwrap_or(false)
        {
            return Err(Error::InvalidState("transaction spool is sealed".into()));
        }
        let fold_started = Instant::now();
        // Each key keeps its required initial presence and its final row. This
        // validates event order without retaining or cloning intermediate rows.
        let mut pending = BTreeMap::<Vec<u8>, (TableId, PrimaryKey, PendingRow)>::new();
        let mut count = 0;
        for (table_id, key, change) in changes {
            count += 1;
            if count > self.0.batch_rows {
                return Err(Error::InvalidState(
                    "source chunk exceeds state-store apply_batch_rows; split at event boundaries"
                        .into(),
                ));
            }
            let encoded = mutation_key(transaction, &table_id, &key);
            match pending.entry(encoded) {
                Entry::Occupied(entry) => {
                    entry.into_mut().2.push(change)?;
                }
                Entry::Vacant(entry) => {
                    entry.insert((table_id, key, PendingRow::new(change)?));
                }
            }
        }
        let spool_started = Instant::now();
        let spool_cf = self.0.db.cf_handle(SPOOL).expect("opened column family");
        let mut existing = Vec::with_capacity(pending.len());
        let mut keys = Vec::new();
        for (key, (table_id, primary_key, _)) in &pending {
            // Read old row images one at a time: a wide previous image must
            // not multiply the memory required by a small replacement chunk.
            let previous = self
                .0
                .db
                .get_pinned_cf(&spool_cf, key)?
                .map(|value| decode::<CollapsedMutation>(&value))
                .transpose()?;
            existing.push(previous.map(|value| (value.original, value.row.is_some())));
            if existing.last().is_some_and(Option::is_none) {
                keys.push(pk_key(table_id, primary_key));
            }
        }
        // Canonical spool order is also table/PK order. Only first occurrences
        // in this transaction need the authoritative index lookup.
        let lookup_started = Instant::now();
        let pk_cf = self.0.db.cf_handle(PK).expect("opened column family");
        let mut originals = self
            .0
            .db
            .batched_multi_get_cf(&pk_cf, keys.iter(), true)
            .into_iter();
        let bind_started = Instant::now();
        let mut batch = WriteBatch::default();
        for ((key, (table_id, primary_key, pending)), existing) in pending.into_iter().zip(existing)
        {
            let (original, present) = match existing {
                Some(existing) => existing,
                None => {
                    let original: Option<RowLocation> = originals
                        .next()
                        .expect("one index result per missing spool key")?
                        .map(|value| decode(&value))
                        .transpose()?;
                    let present = original.is_some();
                    (original, present)
                }
            };
            pending.validate_initial(present)?;
            let mutation = pending.into_mutation(table_id, primary_key, original);
            self.put(&mut batch, SPOOL, key, &mutation)?;
        }
        let write_started = Instant::now();
        let result = self.write_staged(batch);
        let finished = Instant::now();
        let role = if self.0.control.is_some() {
            "controlled"
        } else {
            "standalone"
        };
        for (phase, start, end) in [
            ("setup", started, fold_started),
            ("fold", fold_started, spool_started),
            ("prior_spool", spool_started, lookup_started),
            ("index_lookup", lookup_started, bind_started),
            ("bind", bind_started, write_started),
            ("write", write_started, finished),
        ] {
            metrics::histogram!("flow_state_collapse_batch_seconds", "role" => role, "phase" => phase)
                .record(end.duration_since(start).as_secs_f64());
        }
        result
    }

    /// Seal only after the complete source transaction and terminal commit record
    /// have become durable in the source-wide ingress journal.
    pub fn seal_transaction(&self, transaction: &str) -> Result<()> {
        let _guard = self.lock()?;
        let mut batch = WriteBatch::default();
        self.put(&mut batch, SPOOL, transaction_prefix(transaction), &true)?;
        self.write(batch)
    }

    /// Returns rows in deterministic canonical-key order. Insert-then-delete
    /// entries are suppressed while their disk tombstones remain until cleanup.
    pub fn collapsed<'a>(
        &'a self,
        transaction: &str,
        table: &TableId,
    ) -> Result<impl Iterator<Item = Result<CollapsedMutation>> + 'a> {
        if !self
            .get::<bool>(SPOOL, &transaction_prefix(transaction))?
            .unwrap_or(false)
        {
            return Err(Error::InvalidState(
                "cannot materialize an unsealed transaction".into(),
            ));
        }
        Ok(self
            .scan(SPOOL, table_prefix(transaction, table))
            .filter_map(|entry| {
                match entry.and_then(|(_, value)| decode::<CollapsedMutation>(&value)) {
                    Ok(mutation) if mutation.original.is_none() && mutation.row.is_none() => None,
                    other => Some(other),
                }
            }))
    }

    /// Stage one bounded batch of position deletes. The key is order-preserving
    /// for file paths and row ordinals, and naturally removes duplicates.
    pub fn put_position_deletes(
        &self,
        transaction: &str,
        table: &TableId,
        locations: impl IntoIterator<Item = RowLocation>,
    ) -> Result<()> {
        let _guard = self.lock()?;
        let mut batch = WriteBatch::default();
        for (count, location) in locations.into_iter().enumerate() {
            if count >= self.0.batch_rows {
                return Err(Error::InvalidState(
                    "delete batch exceeds apply_batch_rows".into(),
                ));
            }
            let mut key = delete_prefix(transaction, table);
            for byte in location.data_file_id.0.bytes() {
                key.push(byte);
                if byte == 0 {
                    key.push(255);
                }
            }
            key.extend_from_slice(&[0, 0]);
            key.extend_from_slice(&location.row_position.to_be_bytes());
            self.put(&mut batch, SPOOL, key, &location)?;
        }
        self.write_staged(batch)
    }

    pub fn position_deletes<'a>(
        &'a self,
        transaction: &str,
        table: &TableId,
    ) -> impl Iterator<Item = Result<RowLocation>> + 'a {
        self.scan(SPOOL, delete_prefix(transaction, table))
            .map(|entry| {
                let (_, bytes) = entry?;
                decode(&bytes)
            })
    }

    pub fn file_position_deletes<'a>(
        &'a self,
        transaction: &str,
        table: &TableId,
        file: &flow_model::FileId,
    ) -> impl Iterator<Item = Result<RowLocation>> + 'a {
        let mut prefix = delete_prefix(transaction, table);
        for byte in file.0.bytes() {
            prefix.push(byte);
            if byte == 0 {
                prefix.push(255);
            }
        }
        prefix.extend_from_slice(&[0, 0]);
        self.scan(SPOOL, prefix).map(|entry| {
            let (_, bytes) = entry?;
            decode(&bytes)
        })
    }

    /// Sort a bounded batch of physical rows for an external-rewrite multiset
    /// join. The physical location disambiguates identical row fingerprints,
    /// so duplicate values retain their multiplicity and synthetic identities.
    pub fn put_reconcile_rows(
        &self,
        scan: &str,
        table: &TableId,
        rows: impl IntoIterator<Item = (PrimaryKey, RowLocation)>,
    ) -> Result<()> {
        let _guard = self.lock()?;
        let mut batch = WriteBatch::default();
        for (count, (identity, location)) in rows.into_iter().enumerate() {
            if count >= self.0.batch_rows {
                return Err(Error::InvalidState(
                    "reconciliation batch exceeds apply_batch_rows".into(),
                ));
            }
            let mut key = reconcile_prefix(scan, table);
            key.extend_from_slice(&location.row_fingerprint);
            append_component(&mut key, location.data_file_id.0.as_bytes());
            key.extend_from_slice(&location.row_position.to_be_bytes());
            self.put(&mut batch, SPOOL, key, &(identity, location))?;
        }
        self.write_staged(batch)
    }

    /// Rows ordered by fingerprint and then physical identity. The scratch
    /// scan must be complete before consumption; it is rebuildable after a crash.
    pub fn reconcile_rows<'a>(
        &'a self,
        scan: &str,
        table: &TableId,
    ) -> impl Iterator<Item = Result<(PrimaryKey, RowLocation)>> + use<'a> {
        self.scan(SPOOL, reconcile_prefix(scan, table))
            .map(|entry| {
                let (_, bytes) = entry?;
                decode(&bytes)
            })
    }

    /// Used after abort, or after every table has durably published a transaction.
    pub fn discard_transaction(&self, transaction: &str) -> Result<()> {
        let _guard = self.lock()?;
        let mut batch = WriteBatch::default();
        self.delete_prefix(&mut batch, SPOOL, &transaction_prefix(transaction));
        self.write(batch)
    }
}

fn transaction_prefix(transaction: &str) -> Vec<u8> {
    // Table prefixes append a marker and table ID.
    let mut result =
        Vec::with_capacity(size_of::<u64>() + transaction.len() + 1 + size_of::<u32>());
    append_component(&mut result, transaction.as_bytes());
    result
}
fn table_prefix(transaction: &str, table: &TableId) -> Vec<u8> {
    let mut result = transaction_prefix(transaction);
    result.push(0);
    result.extend_from_slice(&table.0.to_be_bytes());
    result
}
fn mutation_key(transaction: &str, table: &TableId, key: &PrimaryKey) -> Vec<u8> {
    let mut result = Vec::with_capacity(
        size_of::<u64>() + transaction.len() + 1 + size_of::<u32>() + key.0.len(),
    );
    append_component(&mut result, transaction.as_bytes());
    result.push(0);
    result.extend_from_slice(&table.0.to_be_bytes());
    result.extend_from_slice(&key.0);
    result
}

fn delete_prefix(transaction: &str, table: &TableId) -> Vec<u8> {
    let mut result = transaction_prefix(transaction);
    result.push(1);
    result.extend_from_slice(&table.0.to_be_bytes());
    result
}

fn reconcile_prefix(scan: &str, table: &TableId) -> Vec<u8> {
    let mut result = transaction_prefix(scan);
    result.push(2);
    result.extend_from_slice(&table.0.to_be_bytes());
    result
}
