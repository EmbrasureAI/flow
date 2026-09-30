//! Recovery authority independent of disposable row-index generations.
//!
//! The index WAL and control WAL deliberately do not form a distributed commit.
//! Each durable transition syncs an atomic index batch with revision N, then
//! syncs one control batch with the same revision. Recovery accepts only equal
//! revisions. A crash between the two syncs fences the index; the control
//! records retain source progress and catalog operation identity for
//! reconstruction. Syncing the index first means host crash or power loss
//! outside that window leaves equal revisions, so it does not force a rebuild.
//! A matching index revision is in the same WAL batch as its rows/cursor, and
//! RocksDB WAL recovery preserves that prefix across rotation. Intermediate
//! staging batches may be replayed or discarded under their still-durable
//! publication fence.

use crate::{
    Error, OPERATIONS, OperationPhase, OperationRecord, RawEntry, Result, SOURCE, StateBatch,
    StateStore, StateStoreOptions, TABLES, TableState, WriteObservation, decode, durable_write,
    prefix_end,
};
use flow_model::{OperationId, TableId};
use rocksdb::{DB, Direction, IteratorMode, Options, ReadOptions, WriteBatch};
use serde::{Deserialize, Serialize};
use std::{
    fs,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, MutexGuard, atomic::Ordering},
};

const ACTIVE: &[u8] = b"active-generation";
const INITIALIZING: &[u8] = b"initializing-generation";
const REVISION: &[u8] = b"control-revision";
const CHECKPOINT: u8 = 4;
const VERSION: &[u8] = b"control-format-version";
const CONTROL_MAX_OPEN_FILES: i32 = 256;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GenerationState {
    pub path: PathBuf,
    pub revision: u64,
}

/// A checkpoint is usable only after checking its table snapshots against the
/// retained Iceberg ancestry. Registration follows syncing every checkpoint file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointRecord {
    pub path: PathBuf,
    pub revision: u64,
    pub tables: Vec<(TableId, TableState)>,
    pub pending_operations: Vec<OperationRecord>,
}

struct Inner {
    db: DB,
    writer: Mutex<()>,
}

#[derive(Clone)]
pub struct ControlStore(Arc<Inner>);

impl ControlStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let mut options = Options::default();
        options.create_if_missing(true);
        options.set_write_buffer_size(4 << 20);
        options.set_max_write_buffer_number(2);
        options.set_max_background_jobs(2);
        // Authority is small; bound its share of the process descriptor limit.
        options.set_max_open_files(CONTROL_MAX_OPEN_FILES);
        let db = DB::open(&options, path)?;
        match db.get(VERSION)? {
            Some(version) if version != [1] => {
                return Err(Error::InvalidState(
                    "unsupported control format version".into(),
                ));
            }
            None => db.put_opt(VERSION, [1], &durable_write())?,
            _ => {}
        }
        let store = Self(Arc::new(Inner {
            db,
            writer: Mutex::new(()),
        }));
        store.validate_operation_records()?;
        Ok(store)
    }

    fn lock(&self) -> Result<MutexGuard<'_, ()>> {
        self.0.writer.lock().map_err(|_| Error::Poisoned)
    }

    pub fn active_generation(&self) -> Result<Option<GenerationState>> {
        self.0
            .db
            .get(ACTIVE)?
            .map(|value| decode(&value))
            .transpose()
    }

    /// Adopt an intact legacy index, or initialize a new one. This is allowed
    /// only before a recovery authority exists; index loss never uses this path.
    pub fn initialize_index(
        &self,
        path: impl AsRef<Path>,
        config: StateStoreOptions,
    ) -> Result<StateStore> {
        let guard = self.lock()?;
        if self.active_generation()?.is_some() {
            return Err(Error::InvalidState(
                "control store already has an index generation".into(),
            ));
        }
        let index = StateStore::open_inner(path.as_ref(), config.clone(), None, true)?;
        let active = GenerationState {
            path: fs::canonicalize(path.as_ref())?,
            revision: 1,
        };
        let encoded = bincode::serialize(&active)?;
        if index.0.db.get(REVISION)?.is_some()
            && self.0.db.get(INITIALIZING)?.as_deref() != Some(encoded.as_slice())
        {
            return Err(Error::RecoveryRequired(
                "control authority is missing; refusing to adopt a previously managed index".into(),
            ));
        }
        self.0
            .db
            .put_opt(INITIALIZING, &encoded, &durable_write())?;
        // Copy bounded records first; only ACTIVE makes the import authoritative.
        self.copy_authority_from(&index)?;
        index
            .0
            .db
            .put_opt(REVISION, 1_u64.to_be_bytes(), &durable_write())?;
        sync_directory(&active.path)?;
        let mut commit = WriteBatch::default();
        commit.put(ACTIVE, encoded);
        commit.delete(INITIALIZING);
        self.0.db.write_opt(commit, &durable_write())?;
        drop(index);
        drop(guard);
        StateStore::open_with_control(path, config, self.clone())
    }

    fn copy_authority_from(&self, index: &StateStore) -> Result<()> {
        for family in [OPERATIONS, TABLES, SOURCE] {
            let prefix = record_key(family, &[]);
            let mut clear = WriteBatch::default();
            clear.delete_range(
                &prefix,
                &prefix_end(&prefix).expect("bounded control prefix"),
            );
            self.0.db.write(clear)?;
            let mut records = index.scan(family, Vec::new());
            loop {
                let mut batch = WriteBatch::default();
                let mut count = 0;
                for item in records.by_ref().take(index.batch_rows()) {
                    let (key, value) = item?;
                    batch.put(record_key(family, &key), value);
                    count += 1;
                }
                if count == 0 {
                    break;
                }
                self.0.db.write(batch)?;
            }
        }
        Ok(())
    }

    pub(crate) fn validate_index_path(&self, path: &Path) -> Result<()> {
        let active = self.active_generation()?.ok_or_else(|| {
            Error::RecoveryRequired("control authority is not initialized".into())
        })?;
        if !path.join("CURRENT").is_file() {
            return Err(Error::RecoveryRequired(
                "selected index is missing; retain control and rebuild".into(),
            ));
        }
        if fs::canonicalize(path)? != active.path {
            return Err(Error::RecoveryRequired(
                "index is not the selected generation".into(),
            ));
        }
        Ok(())
    }

    pub(crate) fn validate_index(&self, index: &DB) -> Result<u64> {
        let active = self.active_generation()?.ok_or_else(|| {
            Error::RecoveryRequired("control authority is not initialized".into())
        })?;
        if index.get(REVISION)?.as_deref() != Some(active.revision.to_be_bytes().as_slice()) {
            return Err(Error::RecoveryRequired(
                "index/control revision differs; rebuild before acknowledgement or publication"
                    .into(),
            ));
        }
        Ok(active.revision)
    }

    pub(crate) fn commit_index(
        &self,
        index: &StateStore,
        batch: &mut StateBatch,
    ) -> Result<WriteObservation> {
        let _guard = self.lock()?;
        let result = (|| {
            index.ensure_writable()?;
            let mut active = self.active_generation()?.ok_or_else(|| {
                Error::RecoveryRequired("control authority is not initialized".into())
            })?;
            if active.revision != index.0.revision.load(Ordering::Acquire) {
                return Err(Error::RecoveryRequired(
                    "index generation is no longer authoritative".into(),
                ));
            }
            active.revision = next_revision(active.revision)?;
            batch.index.put(REVISION, active.revision.to_be_bytes());
            batch.control.put(ACTIVE, bincode::serialize(&active)?);
            let observation = WriteObservation::capture(&batch.index);
            // Control may name revision N only after the index WAL holding N
            // (and every staged batch before it) is on stable storage.
            index
                .0
                .db
                .write_opt(std::mem::take(&mut batch.index), &durable_write())?;
            self.0
                .db
                .write_opt(std::mem::take(&mut batch.control), &durable_write())?;
            index.0.revision.store(active.revision, Ordering::Release);
            Ok(observation)
        })();
        // SOURCE ledger writes can bypass the row mutex. Fence a failed two-DB
        // transition before releasing this lock to any subsequent durable write.
        if result.is_err() {
            index.0.failed.store(true, Ordering::Release);
        }
        result
    }

    pub fn source_transaction(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self.0.db.get(record_key(SOURCE, key))?)
    }

    pub fn source_transactions(&self) -> impl Iterator<Item = Result<RawEntry>> + '_ {
        self.scan(record_key(SOURCE, &[])).map(|entry| {
            let (key, value) = entry?;
            Ok((key[1..].into(), value))
        })
    }

    pub fn source_transactions_after<'a>(
        &'a self,
        prefix: &[u8],
        after: Option<&[u8]>,
    ) -> impl Iterator<Item = Result<RawEntry>> + use<'a> {
        let prefix = record_key(SOURCE, prefix);
        let mut start = after.map_or_else(
            || prefix.clone(),
            |key| {
                let mut start = record_key(SOURCE, key);
                start.push(0);
                start
            },
        );
        if start < prefix {
            start.clone_from(&prefix);
        }
        let mut options = ReadOptions::default();
        options.fill_cache(false);
        options.set_iterate_upper_bound(prefix_end(&prefix).expect("bounded source prefix"));
        self.0
            .db
            .iterator_opt(IteratorMode::From(&start, Direction::Forward), options)
            .map(|entry| {
                let (key, value) = entry?;
                Ok((key[1..].into(), value))
            })
    }

    /// Missing table authority is reported explicitly, never as a zero watermark.
    pub fn table_state(&self, table: &TableId) -> Result<Option<TableState>> {
        self.0
            .db
            .get(record_key(TABLES, &table.0.to_be_bytes()))?
            .map(|bytes| decode(&bytes))
            .transpose()
    }

    pub fn table_states(&self) -> Result<Vec<(TableId, TableState)>> {
        self.scan(record_key(TABLES, &[]))
            .map(|entry| {
                let (key, value) = entry?;
                let raw: [u8; 4] = key[1..]
                    .try_into()
                    .map_err(|_| Error::InvalidState("invalid control table key".into()))?;
                Ok((TableId(u32::from_be_bytes(raw)), decode(&value)?))
            })
            .collect()
    }

    pub fn operation(&self, id: &OperationId) -> Result<Option<OperationRecord>> {
        let key = record_key(OPERATIONS, id.0.as_bytes());
        self.0
            .db
            .get(&key)?
            .map(|value| decode_control_operation(&key, &value))
            .transpose()
    }

    pub fn pending_operations(&self) -> Result<Vec<OperationRecord>> {
        self.scan(record_key(OPERATIONS, &[]))
            .filter_map(|entry| {
                match entry.and_then(|(key, value)| decode_control_operation(&key, &value)) {
                    Ok(record) if record.phase == OperationPhase::Applied => None,
                    other => Some(other),
                }
            })
            .collect()
    }

    /// Register recovery-created artifacts before their first upload. This is an
    /// offline-authority write: it deliberately invalidates the old row index.
    pub fn register_recovery_record(
        &self,
        id: &OperationId,
        key: &[u8],
        value: &[u8],
    ) -> Result<()> {
        let _guard = self.lock()?;
        let operation_key = record_key(OPERATIONS, id.0.as_bytes());
        let record: OperationRecord = self
            .0
            .db
            .get(&operation_key)?
            .ok_or_else(|| Error::InvalidState("unknown recovery operation".into()))
            .and_then(|bytes| decode_control_operation(&operation_key, &bytes))?;
        let state = self
            .table_state(&record.operation.table_id)?
            .ok_or_else(|| Error::RecoveryRequired("missing table authority".into()))?;
        if record.phase != OperationPhase::Prepared || state.pending_operation.as_ref() != Some(id)
        {
            return Err(Error::InvalidState(
                "recovery registration requires the prepared table fence".into(),
            ));
        }
        let mut active = self
            .active_generation()?
            .ok_or_else(|| Error::RecoveryRequired("missing active generation".into()))?;
        active.revision = next_revision(active.revision)?;
        let mut batch = WriteBatch::default();
        batch.put(record_key(SOURCE, key), value);
        batch.put(ACTIVE, bincode::serialize(&active)?);
        self.0.db.write_opt(batch, &durable_write())?;
        Ok(())
    }

    /// Resolve a catalog outcome after index loss. `Some` requires an operation
    /// marker proving its exact snapshot/sequence; `None` requires proof of absence.
    /// This advances the control revision, deliberately fencing any old index.
    pub fn resolve_operation(&self, id: &OperationId, committed: Option<(i64, i64)>) -> Result<()> {
        let _guard = self.lock()?;
        let key = record_key(OPERATIONS, id.0.as_bytes());
        let record: OperationRecord = self
            .0
            .db
            .get(&key)?
            .ok_or_else(|| Error::InvalidState("unknown control operation".into()))
            .and_then(|bytes| decode_control_operation(&key, &bytes))?;
        let mut state = self
            .table_state(&record.operation.table_id)?
            .ok_or_else(|| Error::RecoveryRequired("operation has no table authority".into()))?;
        if state.pending_operation.as_ref() != Some(id) {
            return Err(Error::InvalidState(
                "operation does not own the durable table fence".into(),
            ));
        }
        match committed {
            Some((snapshot, sequence)) => {
                if sequence < 0
                    || record.phase == OperationPhase::Building
                    || record.snapshot_id.is_some_and(|value| value != snapshot)
                    || record
                        .sequence_number
                        .is_some_and(|value| value != sequence)
                {
                    return Err(Error::InvalidState(
                        "catalog proof conflicts with prepared operation".into(),
                    ));
                }
                state.snapshot_id = Some(snapshot);
                state.materialized_lsn = record.operation.last_lsn;
                state.schema_version = record.operation.schema_version;
            }
            None if matches!(
                record.phase,
                OperationPhase::Building | OperationPhase::Prepared
            ) => {}
            None => {
                return Err(Error::InvalidState(
                    "cannot discard a proven committed operation".into(),
                ));
            }
        }
        state.pending_operation = None;
        let mut active = self
            .active_generation()?
            .ok_or_else(|| Error::RecoveryRequired("missing active generation".into()))?;
        active.revision = next_revision(active.revision)?;
        let mut batch = WriteBatch::default();
        batch.delete(key);
        batch.put(
            record_key(TABLES, &record.operation.table_id.0.to_be_bytes()),
            bincode::serialize(&state)?,
        );
        batch.put(ACTIVE, bincode::serialize(&active)?);
        self.0.db.write_opt(batch, &durable_write())?;
        Ok(())
    }

    /// Activate a separately built and catalog-validated generation. Every table
    /// watermark must equal durable authority; operation ambiguity must already
    /// be resolved. The old generation remains intact for diagnosis or recovery.
    pub fn activate_rebuilt(&self, replacement: &StateStore) -> Result<GenerationState> {
        let _index_guard = replacement.lock()?;
        let _guard = self.lock()?;
        if replacement.0.control.is_some()
            || !self.pending_operations()?.is_empty()
            || !replacement.pending_operations()?.is_empty()
        {
            return Err(Error::InvalidState(
                "activation requires an offline generation and resolved operations".into(),
            ));
        }
        let previous = self
            .active_generation()?
            .ok_or_else(|| Error::RecoveryRequired("missing control authority".into()))?;
        let path = fs::canonicalize(replacement.0.db.path())?;
        if path == previous.path {
            return Err(Error::InvalidState(
                "rebuild must use a distinct index generation".into(),
            ));
        }
        let mut control = WriteBatch::default();
        for (table, authority) in self.table_states()? {
            let rebuilt: TableState = replacement
                .get(TABLES, &table.0.to_be_bytes())?
                .ok_or_else(|| {
                    Error::RecoveryRequired("rebuilt generation is missing a durable table".into())
                })?;
            if rebuilt.materialized_lsn != authority.materialized_lsn
                || rebuilt.schema_version != authority.schema_version
                || rebuilt.pending_operation.is_some()
            {
                return Err(Error::RecoveryRequired(
                    "rebuilt table watermark/schema differs from authority".into(),
                ));
            }
            control.put(
                record_key(TABLES, &table.0.to_be_bytes()),
                bincode::serialize(&rebuilt)?,
            );
        }
        let rebuilt_tables = replacement.scan(TABLES, Vec::new()).count();
        if rebuilt_tables != self.table_states()?.len() {
            return Err(Error::InvalidState(
                "rebuilt generation contains unexpected table state".into(),
            ));
        }
        let source_cf = replacement
            .0
            .db
            .cf_handle(SOURCE)
            .expect("opened source family");
        let mut clear = WriteBatch::default();
        for entry in replacement.scan(SOURCE, Vec::new()) {
            let (key, _) = entry?;
            clear.delete_cf(&source_cf, key);
            if clear.len() >= replacement.batch_rows() {
                replacement.0.db.write(std::mem::take(&mut clear))?;
            }
        }
        if !clear.is_empty() {
            replacement.0.db.write(clear)?;
        }
        let mut source = self.source_transactions();
        loop {
            let mut batch = WriteBatch::default();
            let mut count = 0;
            for entry in source.by_ref().take(replacement.batch_rows()) {
                let (key, value) = entry?;
                batch.put_cf(&source_cf, key, value);
                count += 1;
            }
            if count == 0 {
                break;
            }
            replacement.0.db.write(batch)?;
        }
        let active = GenerationState {
            path,
            revision: next_revision(previous.revision)?,
        };
        replacement
            .0
            .db
            .put_opt(REVISION, active.revision.to_be_bytes(), &durable_write())?;
        sync_directory(&active.path)?;
        control.put(ACTIVE, bincode::serialize(&active)?);
        self.0.db.write_opt(control, &durable_write())?;
        Ok(active)
    }

    /// Register a consistent local checkpoint. Callers retain its Iceberg snapshots
    /// until removing the registration, and may then delete the checkpoint files.
    pub fn checkpoint(
        &self,
        index: &StateStore,
        path: impl AsRef<Path>,
    ) -> Result<CheckpointRecord> {
        let _index_guard = index.lock()?;
        let _guard = self.lock()?;
        index.ensure_writable()?;
        self.validate_index_path(index.0.db.path())?;
        let revision = self.validate_index(&index.0.db)?;
        index.0.db.flush_wal(true)?;
        rocksdb::checkpoint::Checkpoint::new(&index.0.db)?.create_checkpoint(path.as_ref())?;
        sync_directory_tree(path.as_ref())?;
        let record = CheckpointRecord {
            path: fs::canonicalize(path.as_ref())?,
            revision,
            tables: self.table_states()?,
            pending_operations: self.pending_operations()?,
        };
        self.0.db.put_opt(
            checkpoint_key(&record.path),
            bincode::serialize(&record)?,
            &durable_write(),
        )?;
        Ok(record)
    }

    /// Restore a registered checkpoint into a new offline directory. Callers
    /// reconcile its recorded Iceberg snapshots to current heads, preserve the
    /// control watermarks, and then call `activate_rebuilt`. This never selects
    /// stale checkpoint progress as current authority.
    pub fn restore_checkpoint(
        &self,
        record: &CheckpointRecord,
        destination: impl AsRef<Path>,
        config: StateStoreOptions,
    ) -> Result<StateStore> {
        let _guard = self.lock()?;
        if self.0.db.get(checkpoint_key(&record.path))?.as_deref()
            != Some(bincode::serialize(record)?.as_slice())
        {
            return Err(Error::InvalidState(
                "checkpoint registration changed or is missing".into(),
            ));
        }
        copy_directory_tree(&record.path, destination.as_ref())?;
        sync_directory_tree(destination.as_ref())?;
        StateStore::open(destination, config)
    }

    pub fn checkpoints(&self) -> Result<Vec<CheckpointRecord>> {
        self.scan(vec![CHECKPOINT])
            .map(|entry| entry.and_then(|(_, value)| decode(&value)))
            .collect()
    }

    pub fn forget_checkpoint(&self, record: &CheckpointRecord) -> Result<()> {
        let _guard = self.lock()?;
        let key = checkpoint_key(&record.path);
        if self.0.db.get(&key)?.as_deref() != Some(bincode::serialize(record)?.as_slice()) {
            return Err(Error::InvalidState(
                "checkpoint registration changed or is missing".into(),
            ));
        }
        self.0.db.delete_opt(key, &durable_write())?;
        Ok(())
    }

    fn scan(&self, prefix: Vec<u8>) -> impl Iterator<Item = Result<RawEntry>> + '_ {
        let mut options = ReadOptions::default();
        options.fill_cache(false);
        options.set_iterate_upper_bound(prefix_end(&prefix).expect("bounded control prefix"));
        self.0
            .db
            .iterator_opt(IteratorMode::From(&prefix, Direction::Forward), options)
            .map(|entry| entry.map_err(Error::from))
    }

    fn validate_operation_records(&self) -> Result<()> {
        for entry in self.scan(record_key(OPERATIONS, &[])) {
            let (key, value) = entry?;
            decode_control_operation(&key, &value)?;
        }
        Ok(())
    }
}

fn decode_control_operation(key: &[u8], value: &[u8]) -> Result<OperationRecord> {
    let record = decode::<OperationRecord>(value).map_err(|error| {
        Error::AuthorityCorruption(format!("operation record cannot be decoded: {error}"))
    })?;
    if key.get(1..) != Some(record.operation.id.0.as_bytes()) {
        return Err(Error::AuthorityCorruption(
            "operation key does not match its record".into(),
        ));
    }
    if let Some(reason) = record.invariant_violation() {
        return Err(Error::AuthorityCorruption(format!(
            "operation {} has an invalid persisted {reason}",
            record.operation.id.0
        )));
    }
    Ok(record)
}

pub(crate) fn is_authority(family: &str) -> bool {
    matches!(family, OPERATIONS | TABLES | SOURCE)
}
pub(crate) fn record_key(family: &str, key: &[u8]) -> Vec<u8> {
    let marker = match family {
        OPERATIONS => 1,
        TABLES => 2,
        SOURCE => 3,
        _ => unreachable!("not an authoritative column family"),
    };
    let mut result = Vec::with_capacity(1 + key.len());
    result.push(marker);
    result.extend_from_slice(key);
    result
}
fn next_revision(revision: u64) -> Result<u64> {
    revision
        .checked_add(1)
        .ok_or_else(|| Error::InvalidState("control revision exhausted".into()))
}
fn checkpoint_key(path: &Path) -> Vec<u8> {
    let mut key = vec![CHECKPOINT];
    key.extend_from_slice(path.as_os_str().as_encoded_bytes());
    key
}

/// Checkpoint and generation directory entries must survive the authority pointer.
/// Refuse symlinks so restoration cannot escape the selected local state tree.
pub(crate) fn sync_directory_tree(path: &Path) -> Result<()> {
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let kind = entry.file_type()?;
        if kind.is_dir() {
            sync_directory_tree(&entry.path())?;
        } else if kind.is_file() {
            fs::File::open(entry.path())?.sync_all()?;
        } else {
            return Err(Error::InvalidState(
                "state directory contains a non-regular entry".into(),
            ));
        }
    }
    sync_directory(path)
}

fn sync_directory(path: &Path) -> Result<()> {
    fs::File::open(path)?.sync_all()?;
    if let Some(parent) = path.parent() {
        fs::File::open(parent)?.sync_all()?;
    }
    Ok(())
}

fn copy_directory_tree(source: &Path, destination: &Path) -> Result<()> {
    fs::create_dir(destination)?;
    for entry in fs::read_dir(source)? {
        let entry = entry?;
        let kind = entry.file_type()?;
        let target = destination.join(entry.file_name());
        if kind.is_dir() {
            copy_directory_tree(&entry.path(), &target)?;
        } else if kind.is_file() {
            fs::copy(entry.path(), target)?;
        } else {
            return Err(Error::InvalidState(
                "checkpoint contains a non-regular entry".into(),
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::SIMULATE_POWER_LOSS;
    use flow_model::PgLsn;

    /// The bytes a host crash would leave: a copy of every file as written so
    /// far. Under `SIMULATE_POWER_LOSS`, unsynced index WAL is not yet written.
    fn crash_image(path: &Path) -> tempfile::TempDir {
        let image = tempfile::tempdir().unwrap();
        fs::remove_dir(image.path()).unwrap();
        copy_directory_tree(path, image.path()).unwrap();
        image
    }

    fn restore_image(image: &tempfile::TempDir, path: &Path) {
        fs::remove_dir_all(path).unwrap();
        copy_directory_tree(image.path(), path).unwrap();
    }

    #[test]
    fn power_loss_after_committed_transitions_does_not_require_a_rebuild() {
        SIMULATE_POWER_LOSS.set(true);
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("index");
        let control_path = root.path().join("control");
        let control = ControlStore::open(&control_path).unwrap();
        let index = control
            .initialize_index(&path, StateStoreOptions::default())
            .unwrap();
        for sequence in 0..8_u32 {
            index
                .update_source_ledger((b"ledger", &sequence.to_be_bytes()), [], None)
                .unwrap();
        }
        index.complete_noop(&TableId(1), PgLsn(10), 1).unwrap();
        let revision = control.active_generation().unwrap().unwrap().revision;
        let index_image = crash_image(&path);
        let control_image = crash_image(&control_path);
        drop(index);
        drop(control);
        SIMULATE_POWER_LOSS.set(false);
        restore_image(&index_image, &path);
        restore_image(&control_image, &control_path);

        let control = ControlStore::open(&control_path).unwrap();
        assert_eq!(
            control.active_generation().unwrap().unwrap().revision,
            revision
        );
        let index =
            StateStore::open_with_control(&path, StateStoreOptions::default(), control.clone())
                .unwrap();
        assert_eq!(
            index.table_state(&TableId(1)).unwrap().materialized_lsn,
            PgLsn(10)
        );
        assert_eq!(
            index.source_transaction(b"ledger").unwrap(),
            Some(7_u32.to_be_bytes().to_vec())
        );
    }

    #[test]
    fn index_revision_is_durable_before_control_is_written() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("index");
        let control_path = root.path().join("control");
        let control = ControlStore::open(&control_path).unwrap();
        drop(
            control
                .initialize_index(&path, StateStoreOptions::default())
                .unwrap(),
        );
        let before = control.active_generation().unwrap().unwrap().revision;
        drop(control);

        // Read-only authority rejects the control write after the index write.
        SIMULATE_POWER_LOSS.set(true);
        let control = ControlStore(Arc::new(Inner {
            db: DB::open_for_read_only(&Options::default(), &control_path, false).unwrap(),
            writer: Mutex::new(()),
        }));
        let index =
            StateStore::open_with_control(&path, StateStoreOptions::default(), control.clone())
                .unwrap();
        let mut batch = StateBatch::default();
        index.put_source_record(&mut batch, b"ledger", b"20");
        assert!(control.commit_index(&index, &mut batch).is_err());
        let image = crash_image(&path);
        drop(index);
        drop(control);
        SIMULATE_POWER_LOSS.set(false);

        let recovered = DB::open_cf_for_read_only(
            &Options::default(),
            image.path(),
            crate::STATE_COLUMN_FAMILIES,
            false,
        )
        .unwrap();
        assert_eq!(
            recovered.get(REVISION).unwrap(),
            Some((before + 1).to_be_bytes().to_vec())
        );
    }

    #[test]
    fn failed_control_write_fences_following_ledger_commits_before_unlock() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("index");
        let control_path = root.path().join("control");
        let control = ControlStore::open(&control_path).unwrap();
        let index = control
            .initialize_index(&path, StateStoreOptions::default())
            .unwrap();
        index
            .update_source_ledger((b"ledger", b"10"), [], None)
            .unwrap();
        let before = control.active_generation().unwrap().unwrap().revision;
        drop(index);
        drop(control);

        // A real read-only authority DB rejects the second write, after the
        // writable index accepted its next revision. No production failpoint.
        let control = ControlStore(Arc::new(Inner {
            db: DB::open_for_read_only(&Options::default(), &control_path, false).unwrap(),
            writer: Mutex::new(()),
        }));
        let index =
            StateStore::open_with_control(&path, StateStoreOptions::default(), control.clone())
                .unwrap();
        let mut batch = StateBatch::default();
        index.put_source_record(&mut batch, b"ledger", b"20");
        // Call the commit boundary directly: it must poison before returning,
        // without relying on a caller that still owns the separate row mutex.
        assert!(matches!(
            control.commit_index(&index, &mut batch),
            Err(Error::Database(_))
        ));
        assert!(index.0.failed.load(Ordering::Acquire));
        assert_eq!(
            index.0.db.get(REVISION).unwrap(),
            Some((before + 1).to_be_bytes().to_vec())
        );
        assert_eq!(
            control.active_generation().unwrap().unwrap().revision,
            before
        );
        assert_eq!(
            control.source_transaction(b"ledger").unwrap(),
            Some(b"10".to_vec())
        );
        assert!(matches!(
            index.update_source_ledger((b"ledger", b"30"), [], None),
            Err(Error::RecoveryRequired(_))
        ));
        assert!(matches!(
            index.complete_noop(&TableId(1), PgLsn(30), 1),
            Err(Error::RecoveryRequired(_))
        ));
        assert_eq!(
            control.source_transaction(b"ledger").unwrap(),
            Some(b"10".to_vec())
        );
        drop(index);
        drop(control);
        let control = ControlStore::open(&control_path).unwrap();
        assert!(matches!(
            StateStore::open_with_control(&path, StateStoreOptions::default(), control),
            Err(Error::RecoveryRequired(_))
        ));
    }
}
