//! Apply committed index deltas with atomic progress cursors and bounded lookups.
//!
//! Compare-and-swap checks, live-row counts and the table fence advance in the
//! same write batch. Intermediate batches retain the existing durability rules.

use crate::{
    ApplyResult, DELTAS, Error, FILE_COUNTS, OPERATIONS, OperationKind, OperationPhase, PK,
    REVERSE, Result, StateBatch, StateStore, TABLES, decode, decode_file_count, delta_key,
    file_prefix,
    files::{self, NO_FILE, StoredDelta, StoredLocation, reverse_key},
};
use flow_model::{FileId, OperationId, PrimaryKey};
use rocksdb::ReadOptions;
use std::{
    collections::BTreeMap,
    time::{Duration, Instant},
};

#[cfg(all(test, feature = "apply-profile"))]
#[path = "../tests/profiles/apply_batch.rs"]
mod apply_batch_profile;

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
pub(crate) struct FileCountShadow {
    pub(crate) initial: u64,
    pub(crate) current: u64,
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
            let mut deltas =
                Vec::<StoredDelta<'static>>::with_capacity((chunk_end - cursor) as usize);
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
                deltas.push(files::decode_delta(value)?.into_owned());
                if offset + 1 < end {
                    records.next();
                }
            }
            stats.decode += decode_started.elapsed();

            let keys: Vec<_> = deltas
                .iter()
                .map(|delta| crate::pk_key_bytes(&record.operation.table_id, &delta.key))
                .collect();
            let reverse_lookup_started = Instant::now();
            let targets: Vec<_> = deltas
                .iter()
                .filter_map(|delta| delta.replacement.as_ref())
                .map(|location| {
                    reverse_key(
                        &record.operation.table_id,
                        location.file,
                        location.row_position,
                    )
                })
                .collect();
            let fresh = self.reverse_targets_are_fresh(&reverse_cf, &targets);
            if !fresh {
                stats.reverse_owner_reads += targets.len();
            }
            stats.reverse_lookup += reverse_lookup_started.elapsed();

            // Every lookup of every part observes the index from before this
            // batch, as the serial path does; parts are merged in key order.
            let parts = split_parts(deltas, keys, targets, self.0.apply_threads);
            let outputs = if parts.len() == 1 {
                let part = parts.into_iter().next().expect("one part");
                vec![build_part(self, &record, part, fresh)]
            } else {
                std::thread::scope(|scope| {
                    let handles: Vec<_> = parts
                        .into_iter()
                        .map(|part| scope.spawn(|| build_part(self, &record, part, fresh)))
                        .collect();
                    handles
                        .into_iter()
                        .map(|handle| handle.join().expect("apply part panicked"))
                        .collect::<Vec<_>>()
                })
            };
            let mut chunk_file_counts = BTreeMap::<u64, i64>::new();
            for output in outputs {
                let output = output?;
                for operation in output.operations {
                    match operation {
                        PartOperation::PutPk(key, value) => batch.put_cf(&pk_cf, key, value),
                        PartOperation::DeletePk(key) => batch.delete_cf(&pk_cf, key),
                        PartOperation::PutReverse(key, value) => {
                            batch.put_cf(&reverse_cf, key, value)
                        }
                        PartOperation::DeleteReverse(key) => batch.delete_cf(&reverse_cf, key),
                    }
                }
                for (file, change) in output.file_counts {
                    let count = chunk_file_counts.entry(file).or_insert(0);
                    *count = count.checked_add(change).ok_or_else(|| {
                        Error::InvalidState(
                            "file count delta exceeds the bounded lookup chunk".into(),
                        )
                    })?;
                }
                applied += output.applied;
                obsolete += output.obsolete;
                stats.pk_lookup += output.pk_lookup;
                stats.reverse_lookup += output.reverse_lookup;
                stats.mutation_build += output.mutation_build;
            }

            let file_count_started = Instant::now();
            for (file, change) in chunk_file_counts {
                if change == 0 {
                    continue;
                }
                // Counts stay keyed by path, like the remaps that move them.
                let file = FileId(
                    self.0
                        .files
                        .path(record.operation.table_id, file)
                        .ok_or_else(files::unknown_file)?
                        .to_string(),
                );
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

        let complete = end == record.delta_count;
        let mut remapped = false;
        if complete
            && record.operation.kind == OperationKind::Rewrite
            && let Some(remap) = self.staged_remap(id)?
        {
            self.commit_remap(
                &mut batch,
                &record.operation.table_id,
                &remap,
                &mut file_counts,
            )?;
            remapped = true;
        }
        let file_count_started = Instant::now();
        for (file, count) in file_counts {
            if count.current == count.initial {
                continue;
            }
            let key = file_prefix(&record.operation.table_id, &file);
            if count.current == 0 {
                batch.delete_cf(&counts_cf, key);
                self.mark_file_released(&mut batch, record.operation.table_id, &file);
            } else {
                batch.put_cf(&counts_cf, key, count.current.to_be_bytes());
            }
        }
        stats.file_count += file_count_started.elapsed();
        record.applied_count = end;
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
        if remapped {
            // Still under the row lock, so no stale size is cached afterwards.
            self.0
                .remap_cache
                .forget_stats(&record.operation.table_id)?;
        }
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

/// Rows of one lookup chunk handled by one thread.
struct PartInput {
    deltas: Vec<StoredDelta<'static>>,
    keys: Vec<Vec<u8>>,
    targets: Vec<Vec<u8>>,
}

enum PartOperation {
    PutPk(Vec<u8>, Vec<u8>),
    DeletePk(Vec<u8>),
    PutReverse(Vec<u8>, Vec<u8>),
    DeleteReverse(Vec<u8>),
}

struct PartOutput {
    operations: Vec<PartOperation>,
    /// Keyed by ordinal; the caller converts to paths once per chunk.
    file_counts: BTreeMap<u64, i64>,
    applied: u64,
    obsolete: u64,
    pk_lookup: Duration,
    reverse_lookup: Duration,
    mutation_build: Duration,
}

/// Split a chunk into up to `threads` contiguous parts of at least 128 rows.
fn split_parts(
    deltas: Vec<StoredDelta<'static>>,
    keys: Vec<Vec<u8>>,
    targets: Vec<Vec<u8>>,
    threads: usize,
) -> Vec<PartInput> {
    let parts = threads.clamp(1, deltas.len().div_ceil(128).max(1));
    if parts == 1 {
        return vec![PartInput {
            deltas,
            keys,
            targets,
        }];
    }
    let size = deltas.len().div_ceil(parts);
    let mut result = Vec::with_capacity(parts);
    let mut deltas = deltas.into_iter();
    let mut keys = keys.into_iter();
    let mut targets = targets.into_iter();
    loop {
        let part: Vec<_> = deltas.by_ref().take(size).collect();
        if part.is_empty() {
            break;
        }
        let replacements = part
            .iter()
            .filter(|delta| delta.replacement.is_some())
            .count();
        result.push(PartInput {
            keys: keys.by_ref().take(part.len()).collect(),
            targets: targets.by_ref().take(replacements).collect(),
            deltas: part,
        });
    }
    result
}

fn build_part(
    store: &StateStore,
    record: &crate::OperationRecord,
    part: PartInput,
    fresh: bool,
) -> Result<PartOutput> {
    let PartInput {
        deltas,
        keys,
        targets,
    } = part;
    let db = &store.0.db;
    let pk_cf = db.cf_handle(PK).expect("opened column family");
    let reverse_cf = db.cf_handle(REVERSE).expect("opened column family");
    let mut output = PartOutput {
        operations: Vec::with_capacity(deltas.len() * 3),
        file_counts: BTreeMap::new(),
        applied: 0,
        obsolete: 0,
        pk_lookup: Duration::ZERO,
        reverse_lookup: Duration::ZERO,
        mutation_build: Duration::ZERO,
    };
    let pk_lookup_started = Instant::now();
    let current = db.batched_multi_get_cf(&pk_cf, keys.iter(), keys.is_sorted());
    output.pk_lookup = pk_lookup_started.elapsed();
    let reverse_lookup_started = Instant::now();
    let mut owners = (!fresh).then(|| {
        db.batched_multi_get_cf(&reverse_cf, targets.iter(), targets.is_sorted())
            .into_iter()
    });
    output.reverse_lookup = reverse_lookup_started.elapsed();

    let mutation_started = Instant::now();
    let remap_options = ReadOptions::default();
    let mut resolver = store.remaps_possible().then(|| {
        crate::remap::Resolver::new(
            db,
            &store.0.remap_cache,
            record.operation.table_id,
            &remap_options,
        )
    });
    let table = record.operation.table_id;
    let mut targets = targets.into_iter();
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
        let current = current.as_deref().map(files::decode_location).transpose()?;
        // A raw entry in a lazily retired file compares by its translated
        // location; its reverse entry stays at the raw location.
        let mut raw_reverse = None;
        let translated = match (&mut resolver, &current) {
            (Some(resolver), Some(raw)) => {
                // Parts run under the row lock, so the ordinal cache is exact.
                let path = store
                    .0
                    .files
                    .path(table, raw.file)
                    .ok_or_else(files::unknown_file)?;
                let translated = resolver.resolve(&raw.to_location(&path))?;
                if translated.is_some() {
                    raw_reverse = Some(reverse_key(&table, raw.file, raw.row_position));
                }
                translated
            }
            _ => None,
        };
        let current = match &translated {
            // A translated file without an ordinal matches no expectation.
            Some(location) => Some(StoredLocation::new(
                store
                    .0
                    .files
                    .ordinal(table, &location.data_file_id.0)
                    .unwrap_or(NO_FILE),
                location,
            )),
            None => current,
        };
        if !matches_expected(current.as_ref(), delta.expected.as_ref()) {
            // Never overwrite an ingestion version from after the worker's
            // read. Catalog validation is still mandatory before this point.
            if record.operation.kind == OperationKind::Rewrite
                && is_obsolete(current.as_ref(), delta.expected.as_ref())
            {
                output.obsolete += 1;
                continue;
            }
            return Err(Error::IndexConflict {
                table: record.operation.table_id,
                key: PrimaryKey(delta.key.into_owned()),
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
        if let Some(raw) = raw_reverse {
            output.operations.push(PartOperation::DeleteReverse(raw));
        } else if let Some(old) = &delta.expected {
            output
                .operations
                .push(PartOperation::DeleteReverse(reverse_key(
                    &table,
                    old.file,
                    old.row_position,
                )));
        }
        match (&delta.replacement, target) {
            (Some(new), Some((owner, target))) => {
                let owner: Option<PrimaryKey> = owner
                    .transpose()?
                    .flatten()
                    .map(|value| decode(&value))
                    .transpose()?;
                if owner.as_ref().is_some_and(|owner| owner.0 != *delta.key) {
                    return Err(Error::InvalidState(
                        "two live keys claim the same physical row".into(),
                    ));
                }
                let mut value = Vec::new();
                files::encode_location(&mut value, new)?;
                output.operations.push(PartOperation::PutPk(key, value));
                // The owner keeps PrimaryKey's encoding: a length-prefixed byte
                // sequence, which is how bincode writes the slice.
                output.operations.push(PartOperation::PutReverse(
                    target,
                    bincode::serialize(&*delta.key)?,
                ));
            }
            (None, None) => output.operations.push(PartOperation::DeletePk(key)),
            _ => unreachable!("one reverse-owner result per replacement"),
        }
        let old = delta.expected.as_ref().map(|row| row.file);
        let new = delta.replacement.as_ref().map(|row| row.file);
        if old != new {
            for (file, change) in old
                .into_iter()
                .map(|file| (file, -1))
                .chain(new.into_iter().map(|file| (file, 1)))
            {
                if let Some(count) = output.file_counts.get_mut(&file) {
                    *count = count.checked_add(change).ok_or_else(|| {
                        Error::InvalidState(
                            "file count delta exceeds the bounded lookup chunk".into(),
                        )
                    })?;
                } else {
                    output.file_counts.insert(file, change);
                }
            }
        }
        output.applied += 1;
    }
    output.mutation_build = mutation_started.elapsed();
    Ok(output)
}

fn matches_expected(
    current: Option<&StoredLocation<'_>>,
    expected: Option<&StoredLocation<'_>>,
) -> bool {
    // Exhaustive field equality, with the ordinal standing in for the path.
    current == expected
}
fn is_obsolete(
    current: Option<&StoredLocation<'_>>,
    expected: Option<&StoredLocation<'_>>,
) -> bool {
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
    use flow_model::{PgLsn, RowLocation};

    #[test]
    fn stored_location_preserves_every_field_and_rejects_damage() {
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
        let stored = StoredLocation::new(u64::MAX - 5, &location);
        let mut encoded = Vec::new();
        files::encode_location(&mut encoded, &stored).unwrap();
        let decoded = files::decode_location(&encoded).unwrap();
        assert_eq!(decoded, stored);
        assert!(matches_expected(Some(&decoded), Some(&stored)));
        assert_eq!(decoded.to_location(&location.data_file_id.0), location);

        // Each field takes part in the compare-and-swap.
        let mut other = stored.clone();
        other.file -= 1;
        assert!(!matches_expected(Some(&decoded), Some(&other)));
        let mut other = stored.clone();
        other.row_fingerprint[15] ^= 1;
        assert!(!matches_expected(Some(&decoded), Some(&other)));
        let mut other = stored.clone();
        other.partition = std::borrow::Cow::Owned(vec![0, 255, 1]);
        assert!(!matches_expected(Some(&decoded), Some(&other)));
        assert!(!matches_expected(None, Some(&stored)));

        let mut trailing = encoded.clone();
        trailing.extend_from_slice(&[0xde, 0xad]);
        assert!(files::decode_location(&trailing).is_err());
        assert!(files::decode_location(&encoded[..encoded.len() - 1]).is_err());

        // Typical values stay small whatever the path length.
        let long = RowLocation {
            data_file_id: FileId("x".repeat(400)),
            row_position: 1_000,
            data_sequence_number: 12,
            spec_id: 0,
            partition: Vec::new(),
            source_commit_lsn: PgLsn(1 << 40),
            row_version: 7,
            row_fingerprint: [1; 16],
        };
        files::encode_location(&mut encoded, &StoredLocation::new(3, &long)).unwrap();
        assert!(encoded.len() <= 40, "{} bytes", encoded.len());
    }
}
