//! Apply committed index deltas with atomic progress cursors and bounded lookups.
//!
//! Compare-and-swap checks, live-row counts and the table fence advance in the
//! same write batch. Intermediate batches retain the existing durability rules.

use crate::{
    ApplyResult, DELTAS, Error, FILE_COUNTS, IndexDelta, OPERATIONS, OperationKind, OperationPhase,
    PK, REVERSE, Result, StateBatch, StateStore, TABLES, decode, decode_file_count, delta_key,
    file_prefix, pk_key, reverse_key,
};
use flow_model::{FileId, OperationId, PgLsn, PrimaryKey, RowLocation};
use rocksdb::ReadOptions;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    time::{Duration, Instant},
};

#[cfg(all(test, feature = "apply-profile"))]
#[path = "../tests/profiles/apply_batch.rs"]
mod apply_batch_profile;

// This mirrors RowLocation's durable bincode shape while borrowing its variable-size
// fields from a pinned RocksDB value. `matches` exhaustively destructures RowLocation so
// adding a field cannot silently weaken the apply compare-and-swap check.
#[derive(Debug, Serialize, Deserialize)]
struct BorrowedFileId<'a>(#[serde(borrow)] &'a str);

#[derive(Debug, Serialize, Deserialize)]
struct BorrowedRowLocation<'a> {
    #[serde(borrow)]
    data_file_id: BorrowedFileId<'a>,
    row_position: u64,
    data_sequence_number: i64,
    spec_id: i32,
    #[serde(borrow)]
    partition: &'a [u8],
    source_commit_lsn: PgLsn,
    row_version: u64,
    row_fingerprint: [u8; 16],
}

impl BorrowedRowLocation<'_> {
    fn matches(&self, expected: &RowLocation) -> bool {
        let RowLocation {
            data_file_id,
            row_position,
            data_sequence_number,
            spec_id,
            partition,
            source_commit_lsn,
            row_version,
            row_fingerprint,
        } = expected;
        self.data_file_id.0 == data_file_id.0
            && self.row_position == *row_position
            && self.data_sequence_number == *data_sequence_number
            && self.spec_id == *spec_id
            && self.partition == partition
            && self.source_commit_lsn == *source_commit_lsn
            && self.row_version == *row_version
            && self.row_fingerprint == *row_fingerprint
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct ApplyBatchStats {
    cursor_rows: u64,
    write_batch_bytes: usize,
    write_batch_operations: usize,
    reverse_owner_reads: usize,
    decode: Duration,
    pk_lookup: Duration,
    reverse_lookup: Duration,
    mutation_build: Duration,
    file_count: Duration,
    write: Duration,
    lock_wait: Duration,
    lock_hold: Duration,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct ApplyBatchOutcome {
    result: ApplyResult,
    stats: ApplyBatchStats,
}

impl ApplyBatchOutcome {
    pub(crate) fn into_result(self) -> ApplyResult {
        let _ = self.stats;
        self.result
    }
}

#[derive(Debug, Clone, Copy)]
struct FileCountShadow {
    initial: u64,
    current: u64,
}

impl StateStore {
    /// Apply one bounded, durable atomic batch. Useful for cooperative recovery and fault
    /// injection. No new publication may start until `complete` is true.
    pub fn apply_committed_batch(&self, id: &OperationId) -> Result<ApplyResult> {
        Ok(self.apply_batch(id, true)?.into_result())
    }

    pub(crate) fn apply_batch(
        &self,
        id: &OperationId,
        sync_progress: bool,
    ) -> Result<ApplyBatchOutcome> {
        let lock_started = Instant::now();
        let guard = self.lock()?;
        let lock_held = Instant::now();
        let mut stats = ApplyBatchStats {
            lock_wait: lock_started.elapsed(),
            ..ApplyBatchStats::default()
        };
        let prepare_started = Instant::now();
        let mut record = self.require_operation(id)?;
        if record.phase == OperationPhase::Applied {
            let result = ApplyResult {
                applied_rows: 0,
                obsolete_rows: 0,
                complete: true,
            };
            stats.lock_hold = lock_held.elapsed();
            drop(guard);
            return Ok(ApplyBatchOutcome { result, stats });
        }
        if record.phase != OperationPhase::Committed {
            return Err(Error::InvalidState(
                "index delta requires a proven catalog commit".into(),
            ));
        }
        let role = if self.0.control.is_some() {
            "controlled"
        } else {
            "standalone"
        };
        let kind = match record.operation.kind {
            OperationKind::Ingest => "ingest",
            OperationKind::Rewrite => "rewrite",
            OperationKind::Reconcile => "reconcile",
            OperationKind::Rebuild => "rebuild",
            OperationKind::ManifestRewrite => "manifest_rewrite",
        };
        let mut state = self.table_state(&record.operation.table_id)?;
        if state.pending_operation.as_ref() != Some(id) {
            return Err(Error::InvalidState(
                "table fence does not match operation".into(),
            ));
        }
        let mut batch = StateBatch::default();
        let mut applied = 0;
        let mut obsolete = 0;
        let mut file_counts = BTreeMap::<FileId, FileCountShadow>::new();
        let start = record.applied_count;
        let end = record.delta_count.min(
            record
                .applied_count
                .saturating_add(self.0.batch_rows as u64),
        );
        stats.cursor_rows = end - start;
        let delta_cf = self.0.db.cf_handle(DELTAS).expect("opened column family");
        let pk_cf = self.0.db.cf_handle(PK).expect("opened column family");
        let reverse_cf = self.0.db.cf_handle(REVERSE).expect("opened column family");
        let counts_cf = self
            .0
            .db
            .cf_handle(FILE_COUNTS)
            .expect("opened column family");
        let mut expected = delta_key(id, start);
        let ordinal_start = expected.len() - size_of::<u64>();
        let mut options = ReadOptions::default();
        options.fill_cache(false);
        options.set_iterate_upper_bound(delta_key(id, end));
        let mut records = self.0.db.raw_iterator_cf_opt(&delta_cf, options);
        records.seek(&expected);
        let mut cursor = start;
        while cursor < end {
            let chunk_end = end.min(cursor.saturating_add(self.0.lookup_rows as u64));
            let decode_started = Instant::now();
            let mut deltas = Vec::<IndexDelta>::with_capacity((chunk_end - cursor) as usize);
            for offset in cursor..chunk_end {
                let Some((key, value)) = records.item() else {
                    records.status()?;
                    return Err(Error::RecoveryRequired("prepared delta is missing".into()));
                };
                expected[ordinal_start..].copy_from_slice(&offset.to_be_bytes());
                if key != expected {
                    return Err(Error::RecoveryRequired(
                        "prepared delta ordinal is missing".into(),
                    ));
                }
                deltas.push(decode(value)?);
                if offset + 1 < end {
                    records.next();
                }
            }
            stats.decode += decode_started.elapsed();

            let pk_lookup_started = Instant::now();
            let keys: Vec<_> = deltas
                .iter()
                .map(|delta| pk_key(&record.operation.table_id, &delta.key))
                .collect();
            let current = self.0.db.batched_multi_get_cf(&pk_cf, keys.iter(), false);
            stats.pk_lookup += pk_lookup_started.elapsed();

            let reverse_lookup_started = Instant::now();
            let targets: Vec<_> = deltas
                .iter()
                .filter_map(|delta| delta.replacement.as_ref())
                .map(|location| reverse_key(&record.operation.table_id, location))
                .collect();
            let mut owners = (!self.reverse_targets_are_fresh(&reverse_cf, &targets)).then(|| {
                stats.reverse_owner_reads += targets.len();
                self.0
                    .db
                    .batched_multi_get_cf(&reverse_cf, targets.iter(), false)
                    .into_iter()
            });
            stats.reverse_lookup += reverse_lookup_started.elapsed();

            let mutation_started = Instant::now();
            let mut targets = targets.into_iter();
            let mut encoded = Vec::new();
            let mut chunk_file_counts = BTreeMap::<FileId, i64>::new();
            for ((mut delta, current), key) in deltas.into_iter().zip(current).zip(keys) {
                let target = delta.replacement.as_ref().map(|_| {
                    let owner = owners.as_mut().map(|owners| {
                        owners
                            .next()
                            .expect("one reverse-owner result per replacement")
                    });
                    (owner, targets.next().expect("one target per replacement"))
                });
                let current = current?;
                let current = current
                    .as_deref()
                    .map(decode_borrowed_row_location)
                    .transpose()?;
                if !matches_expected(current.as_ref(), delta.expected.as_ref()) {
                    // Never overwrite an ingestion version from after the worker's
                    // read. Catalog validation is still mandatory before this point.
                    if record.operation.kind == OperationKind::Rewrite
                        && is_obsolete(current.as_ref(), delta.expected.as_ref())
                    {
                        obsolete += 1;
                        continue;
                    }
                    return Err(Error::IndexConflict {
                        table: record.operation.table_id,
                        key: delta.key,
                    });
                }
                if let Some(replacement) = &mut delta.replacement
                    && replacement.data_sequence_number == -1
                {
                    replacement.data_sequence_number = record.sequence_number.ok_or_else(|| {
                        Error::InvalidState("committed sequence number is missing".into())
                    })?;
                }
                // Every owner lookup observes the committed index from before this
                // outer batch. A delete in an earlier lookup chunk cannot free a
                // physical row for a later chunk in the same atomic commit.
                if let Some(old) = &delta.expected {
                    batch.delete_cf(&reverse_cf, reverse_key(&record.operation.table_id, old));
                }
                match (&delta.replacement, target) {
                    (Some(new), Some((owner, target))) => {
                        let owner: Option<PrimaryKey> = owner
                            .transpose()?
                            .flatten()
                            .map(|value| decode(&value))
                            .transpose()?;
                        if owner.as_ref().is_some_and(|owner| owner != &delta.key) {
                            return Err(Error::InvalidState(
                                "two live keys claim the same physical row".into(),
                            ));
                        }
                        // WriteBatch copies bytes, so one buffer serves every row
                        // without repeated allocation or column-family lookups.
                        encoded.clear();
                        bincode::serialize_into(&mut encoded, new)?;
                        batch.put_cf(&pk_cf, key, &encoded);
                        encoded.clear();
                        bincode::serialize_into(&mut encoded, &delta.key)?;
                        batch.put_cf(&reverse_cf, target, &encoded);
                    }
                    (None, None) => batch.delete_cf(&pk_cf, key),
                    _ => unreachable!("one reverse-owner result per replacement"),
                }
                let old = delta.expected.as_ref().map(|row| &row.data_file_id);
                let new = delta.replacement.as_ref().map(|row| &row.data_file_id);
                if old != new {
                    for (file, change) in old
                        .into_iter()
                        .map(|file| (file, -1))
                        .chain(new.into_iter().map(|file| (file, 1)))
                    {
                        if let Some(count) = chunk_file_counts.get_mut(file) {
                            *count = count.checked_add(change).ok_or_else(|| {
                                Error::InvalidState(
                                    "file count delta exceeds the bounded lookup chunk".into(),
                                )
                            })?;
                        } else {
                            chunk_file_counts.insert(file.clone(), change);
                        }
                    }
                }
                applied += 1;
            }
            stats.mutation_build += mutation_started.elapsed();

            let file_count_started = Instant::now();
            for (file, change) in chunk_file_counts {
                if change == 0 {
                    continue;
                }
                let previous = if let Some(count) = file_counts.get(&file) {
                    count.current
                } else {
                    let key = file_prefix(&record.operation.table_id, &file);
                    self.0
                        .db
                        .get_cf(&counts_cf, key)?
                        .map(|bytes| decode_file_count(&bytes))
                        .transpose()?
                        .unwrap_or(0)
                };
                let current = previous.checked_add_signed(change).ok_or_else(|| {
                    Error::RecoveryRequired(
                        "per-file live-row count overflow or underflow; rebuild the index".into(),
                    )
                })?;
                file_counts
                    .entry(file)
                    .and_modify(|count| count.current = current)
                    .or_insert(FileCountShadow {
                        initial: previous,
                        current,
                    });
            }
            stats.file_count += file_count_started.elapsed();
            cursor = chunk_end;
        }
        records.status()?;
        drop(records);

        let file_count_started = Instant::now();
        for (file, count) in file_counts {
            if count.current == count.initial {
                continue;
            }
            let key = file_prefix(&record.operation.table_id, &file);
            if count.current == 0 {
                batch.delete_cf(&counts_cf, key);
            } else {
                batch.put_cf(&counts_cf, key, count.current.to_be_bytes());
            }
        }
        stats.file_count += file_count_started.elapsed();
        record.applied_count = end;
        let complete = end == record.delta_count;
        if complete {
            record.phase = OperationPhase::Applied;
            state.snapshot_id = record.snapshot_id;
            state.materialized_lsn = record.operation.last_lsn;
            state.schema_version = record.operation.schema_version;
            state.pending_operation = None;
            self.put(
                &mut batch,
                TABLES,
                record.operation.table_id.0.to_be_bytes(),
                &state,
            )?;
        }
        self.put(&mut batch, OPERATIONS, id.0.as_bytes(), &record)?;
        let prepare = prepare_started.elapsed();
        let write_started = Instant::now();
        let observation = if complete || sync_progress {
            self.write_observed(batch)
        } else {
            self.write_staged_observed(batch)
        }?;
        stats.write = write_started.elapsed();
        stats.write_batch_bytes = observation.bytes;
        stats.write_batch_operations = observation.operations;
        let result = ApplyResult {
            applied_rows: applied,
            obsolete_rows: obsolete,
            complete,
        };
        stats.lock_hold = lock_held.elapsed();
        drop(guard);
        record_apply_batch_metrics(role, kind, prepare, &stats);
        Ok(ApplyBatchOutcome { result, stats })
    }

    /// Each progress cursor is atomic with its index changes. The final batch
    /// advances the table LSN and clears its fence. Standalone stores synchronize
    /// the index WAL; controlled stores synchronize the corresponding control
    /// revision and require index recovery or rebuild to that revision before reuse.
    pub fn apply_committed(&self, id: &OperationId) -> Result<ApplyResult> {
        let mut total = ApplyResult {
            applied_rows: 0,
            obsolete_rows: 0,
            complete: false,
        };
        while !total.complete {
            let result = self.apply_batch(id, false)?.into_result();
            total.applied_rows += result.applied_rows;
            total.obsolete_rows += result.obsolete_rows;
            total.complete = result.complete;
        }
        Ok(total)
    }
}

fn record_apply_batch_metrics(
    role: &'static str,
    kind: &'static str,
    prepare: Duration,
    stats: &ApplyBatchStats,
) {
    for (phase, duration) in [
        ("lock_wait", stats.lock_wait),
        ("prepare", prepare),
        ("decode", stats.decode),
        ("pk_lookup", stats.pk_lookup),
        ("reverse_lookup", stats.reverse_lookup),
        ("mutation_build", stats.mutation_build),
        ("file_count", stats.file_count),
        ("commit", stats.write),
        ("lock_hold", stats.lock_hold),
    ] {
        metrics::histogram!("flow_state_apply_batch_seconds", "role" => role, "kind" => kind, "phase" => phase)
            .record(duration.as_secs_f64());
    }
    for (measurement, value) in [
        ("cursor_rows", stats.cursor_rows),
        ("write_batch_bytes", stats.write_batch_bytes as u64),
        (
            "write_batch_operations",
            stats.write_batch_operations as u64,
        ),
        ("reverse_owner_reads", stats.reverse_owner_reads as u64),
    ] {
        metrics::histogram!("flow_state_apply_batch_size", "role" => role, "kind" => kind, "measurement" => measurement)
            .record(value as f64);
    }
}

fn decode_borrowed_row_location(value: &[u8]) -> Result<BorrowedRowLocation<'_>> {
    Ok(bincode::deserialize(value)?)
}
fn matches_expected(
    current: Option<&BorrowedRowLocation<'_>>,
    expected: Option<&RowLocation>,
) -> bool {
    match (current, expected) {
        (None, None) => true,
        (Some(current), Some(expected)) => current.matches(expected),
        _ => false,
    }
}
fn is_obsolete(current: Option<&BorrowedRowLocation<'_>>, expected: Option<&RowLocation>) -> bool {
    match (current, expected) {
        // Absence alone is not a durable proof of a newer source delete.
        (None, Some(_)) => false,
        (Some(current), Some(expected)) => {
            current.source_commit_lsn > expected.source_commit_lsn
                && current.row_version > expected.row_version
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn borrowed_row_location_preserves_codec_and_malformed_boundaries() {
        let location = RowLocation {
            data_file_id: FileId("s3://bucket/δ\0/data.parquet".into()),
            row_position: u64::MAX - 1,
            data_sequence_number: -7,
            spec_id: -3,
            partition: vec![0, 255, 1, 128],
            source_commit_lsn: PgLsn(u64::MAX - 2),
            row_version: u64::MAX - 3,
            row_fingerprint: [0xa5; 16],
        };
        let encoded = bincode::serialize(&location).unwrap();
        let borrowed = decode_borrowed_row_location(&encoded).unwrap();
        assert!(borrowed.matches(&location));
        assert_eq!(bincode::serialize(&borrowed).unwrap(), encoded);

        let mut trailing = encoded.clone();
        trailing.extend_from_slice(&[0xde, 0xad]);
        assert_eq!(
            bincode::deserialize::<RowLocation>(&trailing).unwrap(),
            location
        );
        assert!(
            decode_borrowed_row_location(&trailing)
                .unwrap()
                .matches(&location)
        );

        let mut invalid_utf8 = encoded.clone();
        let path_start = invalid_utf8
            .windows(b"s3://".len())
            .position(|bytes| bytes == b"s3://")
            .unwrap();
        invalid_utf8[path_start] = 0xff;
        assert!(bincode::deserialize::<RowLocation>(&invalid_utf8).is_err());
        assert!(decode_borrowed_row_location(&invalid_utf8).is_err());

        let truncated = &encoded[..encoded.len() - 1];
        assert!(bincode::deserialize::<RowLocation>(truncated).is_err());
        assert!(decode_borrowed_row_location(truncated).is_err());
    }
}
