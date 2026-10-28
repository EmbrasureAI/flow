use crate::StateBatch as WriteBatch;
use crate::{Error, PK, Result, SPOOL, StateStore, append_component, decode, pk_key, prefix_end};
use flow_model::{
    PrimaryKey, Row, RowLocation, TableId, carry_forward_unchanged, row_has_unchanged,
};
use rocksdb::ReadOptions;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, btree_map::Entry},
    time::Instant,
};

/// A row image. Only an update may contain [`flow_model::Value::Unchanged`];
/// collapse carries the key's previous image into those positions when one is
/// available, and publication resolves the rest from the indexed original row.
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

/// A stored [`CollapsedMutation`] without its row image: bincode encodes an
/// option's tag before its value, and decoding ignores the trailing row bytes.
#[derive(Deserialize)]
struct CollapsedPresence {
    _table_id: TableId,
    _key: PrimaryKey,
    original: Option<RowLocation>,
    row: Option<()>,
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
            row: Self::transition(change, required_present, None)?,
        })
    }

    pub(crate) fn push(&mut self, change: Change) -> Result<()> {
        let previous = self.row.take();
        self.row = Self::transition(change, previous.is_some(), previous)?;
        Ok(())
    }

    /// `previous` is the key's current image within this epoch, when known.
    fn transition(change: Change, present: bool, previous: Option<Row>) -> Result<Option<Row>> {
        match change {
            Change::Insert(row) if row_has_unchanged(&row) => Err(Error::InvalidState(
                "an inserted row cannot omit unchanged values".into(),
            )),
            Change::Insert(row) if !present => Ok(Some(row)),
            Change::Update(mut row) if present => {
                if let Some(previous) = previous.filter(|_| row_has_unchanged(&row)) {
                    carry_forward_unchanged(&mut row, &previous)
                        .map_err(|error| Error::InvalidState(error.to_string()))?;
                }
                Ok(Some(row))
            }
            Change::Delete if present => Ok(None),
            _ => Err(Self::conflict()),
        }
    }

    /// Whether the final image still needs values from before this fold.
    pub(crate) fn is_partial(&self) -> bool {
        self.row.as_ref().is_some_and(row_has_unchanged)
    }

    /// Fill unchanged values from the key's image before this fold.
    pub(crate) fn carry_forward(&mut self, previous: &Row) -> Result<()> {
        if let Some(row) = &mut self.row {
            carry_forward_unchanged(row, previous)
                .map_err(|error| Error::InvalidState(error.to_string()))?;
        }
        Ok(())
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
        let fold_started = Instant::now();
        // Each key keeps its required initial presence and its final row. This
        // validates event order without retaining or cloning intermediate rows.
        // The fold reads no stored state, so it runs before taking the row lock.
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
        let started = Instant::now();
        let _guard = self.lock()?;
        if self
            .get::<bool>(SPOOL, &transaction_prefix(transaction))?
            .unwrap_or(false)
        {
            return Err(Error::InvalidState("transaction spool is sealed".into()));
        }
        let spool_started = Instant::now();
        let spool_cf = self.0.db.cf_handle(SPOOL).expect("opened column family");
        let mut existing = Vec::with_capacity(pending.len());
        let mut keys = Vec::new();
        for (key, (table_id, primary_key, folded)) in &mut pending {
            // Read old row images one at a time: a wide previous image must
            // not multiply the memory required by a small replacement chunk.
            // A still-partial fold takes its unchanged values immediately; a
            // fold that does not require the key fails validation below.
            // Otherwise only the original location and presence are decoded.
            let Some(value) = self.0.db.get_pinned_cf(&spool_cf, key)? else {
                existing.push(None);
                keys.push(pk_key(table_id, primary_key));
                continue;
            };
            existing.push(Some(if folded.is_partial() {
                let previous = decode::<CollapsedMutation>(&value)?;
                if let Some(row) = &previous.row {
                    folded.carry_forward(row)?;
                }
                (previous.original, previous.row.is_some())
            } else {
                let previous = decode::<CollapsedPresence>(&value)?;
                (previous.original, previous.row.is_some())
            }));
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
        let remap_options = ReadOptions::default();
        let mut resolvers = std::collections::HashMap::new();
        for ((key, (table_id, primary_key, pending)), existing) in pending.into_iter().zip(existing)
        {
            let (original, present) = match existing {
                Some(existing) => existing,
                None => {
                    let mut original: Option<RowLocation> = originals
                        .next()
                        .expect("one index result per missing spool key")?
                        .map(|value| decode(&value))
                        .transpose()?;
                    if let Some(location) = &mut original
                        && self.remaps_possible()
                    {
                        resolvers
                            .entry(table_id)
                            .or_insert_with(|| {
                                crate::remap::Resolver::new(
                                    &self.0.db,
                                    &self.0.remap_cache,
                                    table_id,
                                    &remap_options,
                                )
                            })
                            .resolve_in_place(location)?;
                    }
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
            ("fold", fold_started, started),
            ("setup", started, spool_started),
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

    /// Bind already-folded rows of one table, in canonical key order, and write
    /// them to an empty spool. Validation and suppression match `collapse_changes`.
    pub(crate) fn spill_folded(
        &self,
        transaction: &str,
        table: &TableId,
        rows: Vec<(PrimaryKey, PendingRow)>,
    ) -> Result<()> {
        let _guard = self.lock()?;
        if self
            .get::<bool>(SPOOL, &transaction_prefix(transaction))?
            .unwrap_or(false)
        {
            return Err(Error::InvalidState("transaction spool is sealed".into()));
        }
        let pk_cf = self.0.db.cf_handle(PK).expect("opened column family");
        let keys: Vec<_> = rows.iter().map(|(key, _)| pk_key(table, key)).collect();
        let originals = self.0.db.batched_multi_get_cf(&pk_cf, keys.iter(), true);
        let mut batch = WriteBatch::default();
        let remap_options = ReadOptions::default();
        let mut resolver = self.remaps_possible().then(|| {
            crate::remap::Resolver::new(&self.0.db, &self.0.remap_cache, *table, &remap_options)
        });
        for ((key, pending), original) in rows.into_iter().zip(originals) {
            let mut original: Option<RowLocation> =
                original?.map(|value| decode(&value)).transpose()?;
            if let (Some(location), Some(resolver)) = (&mut original, &mut resolver) {
                resolver.resolve_in_place(location)?;
            }
            let present = original.is_some();
            pending.validate_initial(present)?;
            let encoded = mutation_key(transaction, table, &key);
            self.put(
                &mut batch,
                SPOOL,
                encoded,
                &pending.into_mutation(*table, key, original),
            )?;
        }
        self.write_staged(batch)
    }

    pub(crate) fn ensure_spool_empty(&self, transaction: &str, table: &TableId) -> Result<()> {
        if self
            .scan(SPOOL, table_prefix(transaction, table))
            .next()
            .is_some()
        {
            return Err(Error::InvalidState(
                "a collapse fold can only spill into an empty transaction spool".into(),
            ));
        }
        Ok(())
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

    /// [`Self::position_deletes`] as a cursor that borrows each record from
    /// the store instead of decoding an owned [`RowLocation`].
    pub fn position_delete_cursor(
        &self,
        transaction: &str,
        table: &TableId,
    ) -> PositionDeleteCursor<'_> {
        let prefix = delete_prefix(transaction, table);
        let mut options = ReadOptions::default();
        options.fill_cache(false);
        options.set_iterate_upper_bound(
            prefix_end(&prefix).expect("length-prefixed internal key always has an upper bound"),
        );
        let mut records = self.0.db.raw_iterator_cf_opt(
            &self.0.db.cf_handle(SPOOL).expect("opened column family"),
            options,
        );
        records.seek(&prefix);
        PositionDeleteCursor {
            records,
            prefix: prefix.len(),
        }
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
        // Every collapse discards first, and its spool is almost always empty.
        // Deleting nothing needs neither a range tombstone nor a durable write.
        if self
            .scan(SPOOL, transaction_prefix(transaction))
            .next()
            .is_none()
        {
            return Ok(());
        }
        let mut batch = WriteBatch::default();
        self.delete_prefix(&mut batch, SPOOL, &transaction_prefix(transaction));
        self.write(batch)
    }
}

/// Forward cursor over one staged position-delete set, in path and then
/// position order. It can skip the rest of a target file with one seek.
pub struct PositionDeleteCursor<'a> {
    records: rocksdb::DBRawIteratorWithThreadMode<'a, rocksdb::DB>,
    prefix: usize,
}

impl PositionDeleteCursor<'_> {
    /// The current target's encoded key part and the row position, or `None`
    /// after the last record. Encoded targets compare as their paths do, and
    /// equal only for the same path.
    pub fn current(&self) -> Result<Option<(&[u8], u64)>> {
        let Some(key) = self.records.key() else {
            self.records.status()?;
            return Ok(None);
        };
        let Some((target, position)) = key
            .len()
            .checked_sub(size_of::<u64>())
            .filter(|split| *split >= self.prefix + 2 && key[split - 2..*split] == [0, 0])
            .map(|split| key.split_at(split))
        else {
            return Err(Error::InvalidState("malformed staged delete key".into()));
        };
        Ok(Some((
            &target[self.prefix..],
            u64::from_be_bytes(position.try_into().expect("eight position bytes")),
        )))
    }

    /// The current record, borrowed from the store.
    pub fn record(&self) -> Result<StagedPosition<'_>> {
        let value = self
            .records
            .value()
            .ok_or_else(|| Error::InvalidState("staged delete cursor is exhausted".into()))?;
        Ok(bincode::deserialize(value)?)
    }

    pub fn advance(&mut self) {
        self.records.next();
    }

    /// Move past the current target's remaining positions.
    pub fn skip_target(&mut self) {
        let Some(key) = self.records.key() else {
            return;
        };
        // Each target ends with the terminator `0, 0`; `0, 1` follows every
        // position of this target and precedes the next target.
        let mut next = key[..key.len().saturating_sub(size_of::<u64>())].to_vec();
        if let Some(last) = next.last_mut() {
            *last = 1;
        }
        self.records.seek(next);
    }
}

/// The leading fields of a staged [`RowLocation`], borrowed from its
/// encoding: what a position-delete file records and its writer checks. The
/// identity fields that follow are not decoded.
#[derive(Debug, Deserialize)]
pub struct StagedPosition<'a> {
    data_file_id: &'a [u8],
    pub row_position: u64,
    pub data_sequence_number: i64,
    pub spec_id: i32,
    pub partition: &'a [u8],
}

impl StagedPosition<'_> {
    /// The target data file's path.
    pub fn path(&self) -> Result<&str> {
        std::str::from_utf8(self.data_file_id)
            .map_err(|_| Error::InvalidState("staged delete path is not UTF-8".into()))
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

#[cfg(test)]
mod tests {
    use super::*;
    use flow_model::{FileId, PgLsn, Value};

    #[test]
    fn presence_decoding_matches_the_stored_mutation() {
        let location = RowLocation {
            data_file_id: FileId("s3://bucket/data/file.parquet".into()),
            row_position: 41,
            data_sequence_number: 7,
            spec_id: 0,
            partition: vec![1, 2],
            source_commit_lsn: PgLsn(99),
            row_version: 3,
            row_fingerprint: [5; 16],
        };
        let row = vec![
            Value::Int64(1),
            Value::String("wide".repeat(100)),
            Value::Null,
            Value::Unchanged,
        ];
        for original in [None, Some(location)] {
            for row in [None, Some(row.clone()), Some(Vec::new())] {
                let mutation = CollapsedMutation {
                    table_id: TableId(9),
                    key: PrimaryKey(vec![1, 3, 0, 255]),
                    original: original.clone(),
                    row,
                };
                let bytes = bincode::serialize(&mutation).unwrap();
                let presence = decode::<CollapsedPresence>(&bytes).unwrap();
                assert_eq!(presence.original, mutation.original);
                assert_eq!(presence.row.is_some(), mutation.row.is_some());
            }
        }
    }
}
