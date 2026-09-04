//! Durable write-side state. This database is never part of the public read path.
//!
//! Blocking RocksDB operations belong on a dedicated actor or `spawn_blocking`,
//! not a Tokio executor thread. A table remains fenced until a committed delta is
//! fully applied; recovery can resume between any two batches.

mod buffer;
mod control;
mod reader;
mod spool;
#[cfg(all(test, feature = "sst-profile"))]
#[path = "../tests/profiles/sst_staging.rs"]
mod sst_staging_profile;
pub use buffer::{BufferResult, CollapseBuffer, MemoryRows};
pub use control::{CheckpointRecord, ControlStore, GenerationState};
pub use reader::{IndexSnapshot, RowIndex};
pub use spool::{Change, CollapsedMutation};

use flow_model::{FileId, OperationId, PgLsn, PrimaryKey, RowLocation, TableId};
use rocksdb::{
    BlockBasedIndexType, BlockBasedOptions, Cache, ColumnFamilyDescriptor, DB, DBCompressionType,
    Direction, IteratorMode, Options, ReadOptions, WriteBatch, WriteOptions,
    checkpoint::Checkpoint,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::{
    collections::{BTreeMap, BTreeSet},
    ops::{Deref, DerefMut},
    path::Path,
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Instant,
};
use thiserror::Error;

const PK: &str = "pk_location";
const REVERSE: &str = "file_rows";
const FILE_COUNTS: &str = "file_live_rows";
const OPERATIONS: &str = "prepared_operations";
const DELTAS: &str = "index_deltas";
const TABLES: &str = "table_state";
const SPOOL: &str = "transaction_mutations";
const SOURCE: &str = "source_transactions";
// Version 3 adds exact per-file counts; older indexes require reconstruction.
const FORMAT_VERSION: u8 = 3;

#[derive(Debug, Error)]
pub enum Error {
    #[error("state database: {0}")]
    Database(#[from] rocksdb::Error),
    #[error("state filesystem: {0}")]
    Io(#[from] std::io::Error),
    #[error("index generation requires recovery: {0}")]
    RecoveryRequired(String),
    #[error("state serialization: {0}")]
    Encoding(#[from] bincode::Error),
    #[error("invalid state transition: {0}")]
    InvalidState(String),
    #[error("index compare-and-swap failed for table {table:?}, key {key:?}")]
    IndexConflict { table: TableId, key: PrimaryKey },
    #[error(
        "row index snapshot changed for table {table:?}: expected {expected:?}, found {actual:?}, pending {pending:?}"
    )]
    SnapshotMismatch {
        table: TableId,
        expected: Option<i64>,
        actual: Option<i64>,
        pending: Option<OperationId>,
    },
    #[error("table state changed before exact prepare for table {table:?}")]
    ExactStateMismatch { table: TableId },
    #[error("state mutex poisoned; restart and recover before publishing")]
    Poisoned,
}
pub type Result<T> = std::result::Result<T, Error>;
/// Raw durable ledger record, owned by its calling subsystem.
pub type RawEntry = (Box<[u8]>, Box<[u8]>);

#[derive(Debug, Clone)]
pub struct StateStoreOptions {
    pub block_cache_bytes: usize,
    pub write_buffer_bytes: usize,
    /// Maximum index mutations in one durable batch.
    pub apply_batch_rows: usize,
}
impl Default for StateStoreOptions {
    fn default() -> Self {
        Self {
            block_cache_bytes: 128 << 20,
            write_buffer_bytes: 16 << 20,
            apply_batch_rows: 1024,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OperationKind {
    Ingest,
    Rewrite,
    Reconcile,
    Rebuild,
    ManifestRewrite,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PreparedOperation {
    pub id: OperationId,
    pub table_id: TableId,
    pub kind: OperationKind,
    pub base_snapshot_id: Option<i64>,
    pub last_lsn: PgLsn,
    pub schema_version: u32,
    pub artifacts: Vec<String>,
    /// Serialized catalog action, bounded by artifact count, never row payloads.
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexDelta {
    pub key: PrimaryKey,
    pub expected: Option<RowLocation>,
    pub replacement: Option<RowLocation>,
}

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OperationPhase {
    Building,
    Prepared,
    Committed,
    Applied,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperationRecord {
    pub operation: PreparedOperation,
    pub phase: OperationPhase,
    pub snapshot_id: Option<i64>,
    pub sequence_number: Option<i64>,
    pub delta_count: u64,
    pub applied_count: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableState {
    pub snapshot_id: Option<i64>,
    pub materialized_lsn: PgLsn,
    pub schema_version: u32,
    pub pending_operation: Option<OperationId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ApplyResult {
    pub applied_rows: u64,
    pub obsolete_rows: u64,
    pub complete: bool,
}

struct Inner {
    db: DB,
    writer: Mutex<()>,
    batch_rows: usize,
    control: Option<ControlStore>,
    revision: AtomicU64,
    failed: AtomicBool,
}
#[derive(Clone)]
pub struct StateStore(Arc<Inner>);

impl StateStore {
    pub fn open(path: impl AsRef<Path>, config: StateStoreOptions) -> Result<Self> {
        Self::open_inner(path.as_ref(), config, None, false)
    }

    /// Open the selected replaceable index against its durable recovery authority.
    /// A missing or divergent generation is fenced before callers can read or ACK.
    pub fn open_with_control(
        path: impl AsRef<Path>,
        config: StateStoreOptions,
        control: ControlStore,
    ) -> Result<Self> {
        Self::open_inner(path.as_ref(), config, Some(control), false)
    }

    fn open_inner(
        path: &Path,
        config: StateStoreOptions,
        control: Option<ControlStore>,
        authority_import: bool,
    ) -> Result<Self> {
        if config.apply_batch_rows == 0
            || config.write_buffer_bytes == 0
            || config.block_cache_bytes == 0
        {
            return Err(Error::InvalidState(
                "state store budgets must be positive".into(),
            ));
        }
        if let Some(authority) = &control {
            authority.validate_index_path(path)?;
        }
        let cache = Cache::new_lru_cache(config.block_cache_bytes);
        let mut table = BlockBasedOptions::default();
        table.set_block_cache(&cache);
        table.set_bloom_filter(10.0, false);
        table.set_cache_index_and_filter_blocks(true);
        // Monolithic SST indexes can exceed an individual cache shard and be
        // decompressed again on every lookup. Keep index/filter reads bounded.
        table.set_index_type(BlockBasedIndexType::TwoLevelIndexSearch);
        table.set_partition_filters(true);
        table.set_metadata_block_size(4096);
        let mut options = Options::default();
        options.create_if_missing(true);
        options.create_missing_column_families(true);
        options.set_max_background_jobs(4);
        options.set_atomic_flush(true);
        let columns = [
            PK,
            REVERSE,
            FILE_COUNTS,
            OPERATIONS,
            DELTAS,
            TABLES,
            SPOOL,
            SOURCE,
        ]
        .map(|name| {
            let mut cf = Options::default();
            cf.set_block_based_table_factory(&table);
            cf.set_write_buffer_size(config.write_buffer_bytes);
            cf.set_max_write_buffer_number(2);
            cf.set_compression_type(DBCompressionType::Lz4);
            ColumnFamilyDescriptor::new(name, cf)
        });
        let db = DB::open_cf_descriptors(&options, path, columns)?;
        match db.get(b"format-version")? {
            Some(version) if version == [1] || version == [2] => {
                if !authority_import {
                    return Err(Error::RecoveryRequired("row fingerprint encoding or per-file live-row counts changed; rebuild the index from durable authority".into()));
                }
            }
            Some(version) if version != [FORMAT_VERSION] => {
                return Err(Error::InvalidState(
                    "unsupported state format version".into(),
                ));
            }
            None => {
                let mut batch = WriteBatch::default();
                batch.put(b"format-version", [FORMAT_VERSION]);
                db.write_opt(batch, &durable_write())?;
            }
            _ => {}
        }
        let revision = if let Some(authority) = &control {
            authority.validate_index(&db)?
        } else {
            0
        };
        Ok(Self(Arc::new(Inner {
            db,
            writer: Mutex::new(()),
            batch_rows: config.apply_batch_rows,
            control,
            revision: AtomicU64::new(revision),
            failed: AtomicBool::new(false),
        })))
    }

    fn lock(&self) -> Result<MutexGuard<'_, ()>> {
        let guard = self.0.writer.lock().map_err(|_| Error::Poisoned)?;
        self.ensure_writable()?;
        Ok(guard)
    }

    fn ensure_writable(&self) -> Result<()> {
        if self.0.failed.load(Ordering::Acquire) {
            return Err(Error::RecoveryRequired(
                "a previous durable transition failed".into(),
            ));
        }
        Ok(())
    }

    pub fn lookup(&self, table: &TableId, key: &PrimaryKey) -> Result<Option<RowLocation>> {
        self.get(PK, &pk_key(table, key))
    }

    pub fn index_is_empty(&self, table: &TableId) -> Result<bool> {
        Ok(self
            .scan(PK, table.0.to_be_bytes().to_vec())
            .next()
            .transpose()?
            .is_none())
    }

    pub fn batch_rows(&self) -> usize {
        self.0.batch_rows
    }

    /// Stream the immutable prepared delta in staging order.
    pub fn prepared_deltas<'a>(
        &'a self,
        id: &OperationId,
    ) -> Result<impl Iterator<Item = Result<IndexDelta>> + 'a> {
        let record = self.require_operation(id)?;
        if record.phase == OperationPhase::Building {
            return Err(Error::InvalidState("delta is not sealed".into()));
        }
        let mut prefix = operation_prefix(id);
        prefix.push(0);
        Ok(self.scan(DELTAS, prefix).map(|entry| {
            let (_, bytes) = entry?;
            decode(&bytes)
        }))
    }

    pub fn table_state(&self, table: &TableId) -> Result<TableState> {
        Ok(self
            .get(TABLES, &table.0.to_be_bytes())?
            .unwrap_or_default())
    }

    pub fn operation(&self, id: &OperationId) -> Result<Option<OperationRecord>> {
        self.get(OPERATIONS, id.0.as_bytes())
    }

    pub fn pending_operations(&self) -> Result<Vec<OperationRecord>> {
        self.scan(OPERATIONS, Vec::new())
            .filter_map(|value| match value {
                Ok((_, bytes)) => match decode::<OperationRecord>(&bytes) {
                    Ok(record) if record.phase != OperationPhase::Applied => Some(Ok(record)),
                    Ok(_) => None,
                    Err(error) => Some(Err(error)),
                },
                Err(error) => Some(Err(error)),
            })
            .collect()
    }

    /// Persist output identities before publishing. Deltas are streamed into
    /// separate disk records. Failure leaves a fenced Building operation.
    pub fn prepare(
        &self,
        operation: PreparedOperation,
        deltas: impl IntoIterator<Item = IndexDelta>,
    ) -> Result<()> {
        self.prepare_fallible(operation, deltas.into_iter().map(Ok))
    }

    pub fn prepare_fallible(
        &self,
        operation: PreparedOperation,
        deltas: impl IntoIterator<Item = Result<IndexDelta>>,
    ) -> Result<()> {
        let id = operation.id.clone();
        let artifacts = operation.artifacts.clone();
        let payload = operation.payload.clone();
        self.begin_prepare(operation)?;
        self.stage_deltas_fallible(&id, deltas)?;
        self.seal_prepare(&id, artifacts, payload)
    }

    pub fn begin_prepare(&self, operation: PreparedOperation) -> Result<()> {
        self.begin_prepare_with_record(operation, None)
    }

    /// Atomically reserve owned artifacts with the operation's durable fence.
    pub fn begin_prepare_with_record(
        &self,
        operation: PreparedOperation,
        source_record: Option<(&[u8], &[u8])>,
    ) -> Result<()> {
        self.begin_prepare_checked(operation, source_record, |state, operation| {
            if state.pending_operation.is_some() || state.snapshot_id != operation.base_snapshot_id
            {
                return Err(Error::InvalidState(
                    "table is fenced or base snapshot does not match index".into(),
                ));
            }
            if operation.last_lsn < state.materialized_lsn {
                return Err(Error::InvalidState(
                    "operation would regress materialized LSN".into(),
                ));
            }
            Ok(())
        })
    }

    /// Atomically fence the exact table version admitted by a background build.
    pub fn begin_prepare_exact(
        &self,
        operation: PreparedOperation,
        expected: &TableState,
        source_record: Option<(&[u8], &[u8])>,
    ) -> Result<()> {
        self.begin_prepare_checked(operation, source_record, |state, operation| {
            if expected.pending_operation.is_some() {
                return Err(Error::InvalidState(
                    "exact prepare requires an unfenced expected table state".into(),
                ));
            }
            if operation.base_snapshot_id != expected.snapshot_id
                || operation.last_lsn != expected.materialized_lsn
                || operation.schema_version != expected.schema_version
            {
                return Err(Error::InvalidState(
                    "operation does not match the expected table state".into(),
                ));
            }
            if state != expected {
                return Err(Error::ExactStateMismatch {
                    table: operation.table_id,
                });
            }
            Ok(())
        })
    }

    fn begin_prepare_checked(
        &self,
        operation: PreparedOperation,
        source_record: Option<(&[u8], &[u8])>,
        validate: impl FnOnce(&TableState, &PreparedOperation) -> Result<()>,
    ) -> Result<()> {
        let _guard = self.lock()?;
        if self.operation(&operation.id)?.is_some() {
            return Err(Error::InvalidState(
                "operation already exists; recover its persisted state".into(),
            ));
        }
        let mut state = self.table_state(&operation.table_id)?;
        validate(&state, &operation)?;
        state.pending_operation = Some(operation.id.clone());
        let record = OperationRecord {
            operation,
            phase: OperationPhase::Building,
            snapshot_id: None,
            sequence_number: None,
            delta_count: 0,
            applied_count: 0,
        };
        let mut batch = StateBatch::default();
        self.put(
            &mut batch,
            TABLES,
            record.operation.table_id.0.to_be_bytes(),
            &state,
        )?;
        self.put(
            &mut batch,
            OPERATIONS,
            record.operation.id.0.as_bytes(),
            &record,
        )?;
        if let Some((key, value)) = source_record {
            self.put_source_record(&mut batch, key, value);
        }
        self.write(batch)
    }

    pub fn stage_deltas(
        &self,
        id: &OperationId,
        deltas: impl IntoIterator<Item = IndexDelta>,
    ) -> Result<()> {
        self.stage_deltas_fallible(id, deltas.into_iter().map(Ok))
    }

    pub fn stage_deltas_fallible(
        &self,
        id: &OperationId,
        deltas: impl IntoIterator<Item = Result<IndexDelta>>,
    ) -> Result<()> {
        let _guard = self.lock()?;
        let mut record = self.require_operation(id)?;
        if record.phase != OperationPhase::Building {
            return Err(Error::InvalidState(
                "cannot stage a sealed operation".into(),
            ));
        }
        let cf = self.0.db.cf_handle(DELTAS).expect("opened column family");
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
        let mut unique_prefix = operation_prefix(id);
        unique_prefix.push(1);
        let mut location_prefix = operation_prefix(id);
        location_prefix.push(2);
        location_prefix.extend_from_slice(&record.operation.table_id.0.to_be_bytes());
        let mut ordinal = delta_key(id, 0);
        let ordinal_offset = ordinal.len() - size_of::<u64>();
        let mut encoded = Vec::new();
        let mut deltas = deltas.into_iter().fuse();
        loop {
            let build_started = Instant::now();
            let mut batch = StateBatch::default();
            let mut batch_keys = BTreeSet::new();
            let mut batch_locations = BTreeSet::new();
            let start = record.delta_count;
            for delta in deltas.by_ref().take(self.0.batch_rows) {
                let delta = delta?;
                let mut unique = Vec::with_capacity(unique_prefix.len() + delta.key.0.len());
                unique.extend_from_slice(&unique_prefix);
                unique.extend_from_slice(&delta.key.0);
                if !batch_keys.insert(unique) {
                    return Err(Error::InvalidState(
                        "duplicate primary key in prepared delta".into(),
                    ));
                }
                if let Some(location) = &delta.replacement {
                    let mut target = Vec::with_capacity(
                        location_prefix.len()
                            + 2 * size_of::<u64>()
                            + location.data_file_id.0.len(),
                    );
                    target.extend_from_slice(&location_prefix);
                    append_component(&mut target, location.data_file_id.0.as_bytes());
                    target.extend_from_slice(&location.row_position.to_be_bytes());
                    if !batch_locations.insert(target) {
                        return Err(Error::InvalidState("duplicate output row location".into()));
                    }
                }
                ordinal[ordinal_offset..].copy_from_slice(&record.delta_count.to_be_bytes());
                encoded.clear();
                bincode::serialize_into(&mut encoded, &delta)?;
                batch.put_cf(&cf, &ordinal, &encoded);
                record.delta_count += 1;
            }
            if record.delta_count == start {
                return Ok(());
            }
            // The sets retain marker bytes for both validation and the write;
            // WriteBatch copies them, so staging needs no per-marker clone.
            for marker in batch_keys.iter().chain(&batch_locations) {
                batch.put_cf(&cf, marker, []);
            }
            self.put(&mut batch, OPERATIONS, id.0.as_bytes(), &record)?;
            let build = build_started.elapsed();
            let lookup_started = Instant::now();
            let check_keys = !self.keys_follow_existing(
                &cf,
                &unique_prefix,
                batch_keys.first().map(Vec::as_slice),
            )?;
            let check_locations = !self.keys_follow_existing(
                &cf,
                &location_prefix,
                batch_locations.first().map(Vec::as_slice),
            )?;
            if check_keys || check_locations {
                // PK marker 1 sorts before location marker 2. Only families
                // whose fresh range was not proven need the full sorted lookup.
                let keys = check_keys.then_some(&batch_keys).into_iter().flatten();
                let locations = check_locations
                    .then_some(&batch_locations)
                    .into_iter()
                    .flatten();
                let checked_keys = if check_keys { batch_keys.len() } else { 0 };
                let existing = self
                    .0
                    .db
                    .batched_multi_get_cf(&cf, keys.chain(locations), true);
                for (index, value) in existing.into_iter().enumerate() {
                    if value?.is_some() {
                        return Err(Error::InvalidState(
                            if index < checked_keys {
                                "duplicate primary key in prepared delta"
                            } else {
                                "duplicate output row location"
                            }
                            .into(),
                        ));
                    }
                }
            }
            let lookup = lookup_started.elapsed();
            metrics::histogram!("flow_state_stage_batch_seconds", "role" => role, "kind" => kind, "phase" => "build")
                .record(build.as_secs_f64());
            metrics::histogram!("flow_state_stage_batch_seconds", "role" => role, "kind" => kind, "phase" => "duplicate_lookup")
                .record(lookup.as_secs_f64());
            let write_started = Instant::now();
            let result = self.write_staged(batch);
            metrics::histogram!("flow_state_stage_batch_seconds", "role" => role, "kind" => kind, "phase" => "write")
                .record(write_started.elapsed().as_secs_f64());
            result?;
        }
    }

    // Callers hold the row writer lock and separately validate batch uniqueness.
    fn keys_follow_existing(
        &self,
        cf: &impl rocksdb::AsColumnFamilyRef,
        prefix: &[u8],
        minimum: Option<&[u8]>,
    ) -> Result<bool> {
        let Some(first) = minimum else {
            return Ok(true);
        };
        let mut options = ReadOptions::default();
        options.set_iterate_lower_bound(prefix.to_vec());
        options.set_iterate_upper_bound(
            prefix_end(prefix).expect("internal marker family has an upper bound"),
        );
        let mut existing = self.0.db.raw_iterator_cf_opt(cf, options);
        existing.seek_to_last();
        existing.status()?;
        Ok(existing.key().is_none_or(|last| first > last))
    }

    fn reverse_targets_are_fresh(
        &self,
        cf: &impl rocksdb::AsColumnFamilyRef,
        targets: &[Vec<u8>],
    ) -> bool {
        let Some(first) = targets.first() else {
            return true;
        };
        // reverse_key appends an unsigned big-endian ordinal to the exact file
        // prefix. Computing the minimum permits arbitrary input order.
        let prefix = &first[..first.len() - size_of::<u64>()];
        let mut minimum = first.as_slice();
        for target in &targets[1..] {
            if target.len() != first.len() || !target.starts_with(prefix) {
                return false;
            }
            if target[prefix.len()..] < minimum[prefix.len()..] {
                minimum = target;
            }
        }
        // An unavailable optional proof uses ordinary owner reads. In particular,
        // obsolete rows must still skip their per-row owner errors and checks.
        self.keys_follow_existing(cf, prefix, Some(minimum))
            .unwrap_or(false)
    }

    /// Attach the finished artifacts and serialized catalog action. A Building
    /// operation is never eligible for a catalog commit.
    pub fn seal_prepare(
        &self,
        id: &OperationId,
        artifacts: Vec<String>,
        payload: Vec<u8>,
    ) -> Result<()> {
        self.seal_prepare_with_record(id, artifacts, payload, None)
    }

    /// Seal publication and its final artifact reservation in the same barrier.
    pub fn seal_prepare_with_record(
        &self,
        id: &OperationId,
        artifacts: Vec<String>,
        payload: Vec<u8>,
        source_record: Option<(&[u8], &[u8])>,
    ) -> Result<()> {
        let _guard = self.lock()?;
        let mut record = self.require_operation(id)?;
        if record.phase != OperationPhase::Building {
            return Err(Error::InvalidState("operation is already sealed".into()));
        }
        record.operation.artifacts = artifacts;
        record.operation.payload = payload;
        record.phase = OperationPhase::Prepared;
        let mut batch = StateBatch::default();
        self.put(&mut batch, OPERATIONS, id.0.as_bytes(), &record)?;
        if let Some((key, value)) = source_record {
            self.put_source_record(&mut batch, key, value);
        }
        self.write(batch)
    }

    /// Acknowledges only a catalog outcome proven by the operation ID marker.
    pub fn mark_committed(
        &self,
        id: &OperationId,
        snapshot_id: i64,
        sequence_number: i64,
    ) -> Result<()> {
        let _guard = self.lock()?;
        if sequence_number < 0 {
            return Err(Error::InvalidState(
                "committed sequence number must be nonnegative".into(),
            ));
        }
        let mut record = self.require_operation(id)?;
        match record.phase {
            OperationPhase::Prepared => {}
            OperationPhase::Committed | OperationPhase::Applied
                if record.snapshot_id == Some(snapshot_id)
                    && record.sequence_number == Some(sequence_number) =>
            {
                return Ok(());
            }
            _ => {
                return Err(Error::InvalidState(
                    "operation cannot be marked committed in this phase".into(),
                ));
            }
        }
        record.phase = OperationPhase::Committed;
        record.snapshot_id = Some(snapshot_id);
        record.sequence_number = Some(sequence_number);
        let mut batch = StateBatch::default();
        self.put(&mut batch, OPERATIONS, id.0.as_bytes(), &record)?;
        self.write(batch)
    }

    /// Apply one bounded, durable atomic batch. Useful for cooperative recovery and fault
    /// injection. No new publication may start until `complete` is true.
    pub fn apply_committed_batch(&self, id: &OperationId) -> Result<ApplyResult> {
        self.apply_batch(id, true)
    }

    fn apply_batch(&self, id: &OperationId, sync_progress: bool) -> Result<ApplyResult> {
        let lock_started = Instant::now();
        let _guard = self.lock()?;
        let lock_wait = lock_started.elapsed();
        let prepare_started = Instant::now();
        let mut record = self.require_operation(id)?;
        if record.phase == OperationPhase::Applied {
            return Ok(ApplyResult {
                applied_rows: 0,
                obsolete_rows: 0,
                complete: true,
            });
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
        let mut file_counts = BTreeMap::<FileId, i64>::new();
        let end = record.delta_count.min(
            record
                .applied_count
                .saturating_add(self.0.batch_rows as u64),
        );
        let cf = self.0.db.cf_handle(DELTAS).expect("opened column family");
        let mut expected = delta_key(id, record.applied_count);
        let ordinal_start = expected.len() - size_of::<u64>();
        let mut options = ReadOptions::default();
        options.fill_cache(false);
        options.set_iterate_upper_bound(delta_key(id, end));
        let mut records = self.0.db.raw_iterator_cf_opt(&cf, options);
        records.seek(&expected);
        let mut deltas = Vec::<IndexDelta>::with_capacity((end - record.applied_count) as usize);
        for offset in record.applied_count..end {
            let Some((key, value)) = records.item() else {
                records.status()?;
                return Err(Error::InvalidState("prepared delta is missing".into()));
            };
            expected[ordinal_start..].copy_from_slice(&offset.to_be_bytes());
            if key != expected {
                return Err(Error::InvalidState(
                    "prepared delta ordinal is missing".into(),
                ));
            }
            deltas.push(decode(value)?);
            if offset + 1 < end {
                records.next();
            }
        }
        records.status()?;
        drop(records);
        // Staging guarantees unique keys and replacement positions. Under the
        // existing apply lock, every check observes the same pre-batch index.
        let pk_cf = self.0.db.cf_handle(PK).expect("opened column family");
        let reverse_cf = self.0.db.cf_handle(REVERSE).expect("opened column family");
        let keys: Vec<_> = deltas
            .iter()
            .map(|delta| pk_key(&record.operation.table_id, &delta.key))
            .collect();
        let targets: Vec<_> = deltas
            .iter()
            .filter_map(|delta| delta.replacement.as_ref())
            .map(|location| reverse_key(&record.operation.table_id, location))
            .collect();
        let current = self.0.db.batched_multi_get_cf(&pk_cf, keys.iter(), false);
        let mut owners = (!self.reverse_targets_are_fresh(&reverse_cf, &targets)).then(|| {
            self.0
                .db
                .batched_multi_get_cf(&reverse_cf, targets.iter(), false)
                .into_iter()
        });
        let mut targets = targets.into_iter();
        let mut encoded = Vec::new();
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
            // Obsolete rewrite rows must skip their owner check as well as the
            // write, even if their planned position now belongs to another key.
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
                    // Most rows share a file. Clone its path only once per batch.
                    if let Some(count) = file_counts.get_mut(file) {
                        *count = count.checked_add(change).ok_or_else(|| {
                            Error::InvalidState(
                                "file count delta exceeds the bounded apply batch".into(),
                            )
                        })?;
                    } else {
                        file_counts.insert(file.clone(), change);
                    }
                }
            }
            applied += 1;
        }
        let counts_cf = self
            .0
            .db
            .cf_handle(FILE_COUNTS)
            .expect("opened column family");
        for (file, change) in file_counts {
            if change == 0 {
                continue;
            }
            let key = file_prefix(&record.operation.table_id, &file);
            let previous = self
                .0
                .db
                .get_cf(&counts_cf, &key)?
                .map(|bytes| decode_file_count(&bytes))
                .transpose()?
                .unwrap_or(0);
            let count = previous.checked_add_signed(change).ok_or_else(|| {
                Error::RecoveryRequired(
                    "per-file live-row count overflow or underflow; rebuild the index".into(),
                )
            })?;
            if count == 0 {
                batch.delete_cf(&counts_cf, key);
            } else {
                batch.put_cf(&counts_cf, key, count.to_be_bytes());
            }
        }
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
        metrics::histogram!("flow_state_apply_batch_seconds", "role" => role, "kind" => kind, "phase" => "lock_wait")
            .record(lock_wait.as_secs_f64());
        metrics::histogram!("flow_state_apply_batch_seconds", "role" => role, "kind" => kind, "phase" => "prepare")
            .record(prepare.as_secs_f64());
        let commit_started = Instant::now();
        let result = if complete || sync_progress {
            self.write(batch)
        } else {
            self.write_staged(batch)
        };
        metrics::histogram!("flow_state_apply_batch_seconds", "role" => role, "kind" => kind, "phase" => "commit")
            .record(commit_started.elapsed().as_secs_f64());
        result?;
        Ok(ApplyResult {
            applied_rows: applied,
            obsolete_rows: obsolete,
            complete,
        })
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
            let result = self.apply_batch(id, false)?;
            total.applied_rows += result.applied_rows;
            total.obsolete_rows += result.obsolete_rows;
            total.complete = result.complete;
        }
        Ok(total)
    }

    /// Discard only when the caller proved there is no matching catalog snapshot.
    /// Uploaded artifacts become orphan candidates; this does not delete objects.
    pub fn discard_uncommitted(&self, id: &OperationId) -> Result<Vec<String>> {
        let _guard = self.lock()?;
        let record = self.require_operation(id)?;
        if !matches!(
            record.phase,
            OperationPhase::Building | OperationPhase::Prepared
        ) {
            return Err(Error::InvalidState(
                "cannot discard a committed operation".into(),
            ));
        }
        let mut state = self.table_state(&record.operation.table_id)?;
        if state.pending_operation.as_ref() != Some(id) {
            return Err(Error::InvalidState(
                "table fence does not match operation".into(),
            ));
        }
        state.pending_operation = None;
        let mut batch = StateBatch::default();
        self.delete_prefix(&mut batch, DELTAS, &operation_prefix(id));
        batch
            .control
            .delete(control::record_key(OPERATIONS, id.0.as_bytes()));
        batch.delete_cf(
            &self
                .0
                .db
                .cf_handle(OPERATIONS)
                .expect("opened column family"),
            id.0.as_bytes(),
        );
        self.put(
            &mut batch,
            TABLES,
            record.operation.table_id.0.to_be_bytes(),
            &state,
        )?;
        self.write(batch)?;
        Ok(record.operation.artifacts)
    }

    /// Exact live-row counts in input order, proven against one unfenced index
    /// snapshot. Point reads avoid scanning rows or decoding position deletes.
    pub fn file_live_row_counts(
        &self,
        table: &TableId,
        expected_snapshot: Option<i64>,
        files: &[FileId],
    ) -> Result<Vec<u64>> {
        self.index_snapshot(table, expected_snapshot)?
            .file_live_row_counts(files)
    }

    /// Source-wide transaction ledger payloads. One key per transaction, never
    /// raw transaction rows. The caller owns the payload schema.
    pub fn put_source_transaction(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let _guard = self.lock()?;
        let mut batch = StateBatch::default();
        self.put_source_record(&mut batch, key, value);
        self.write(batch)
    }

    fn put_source_record(&self, batch: &mut StateBatch, key: &[u8], value: &[u8]) {
        batch.control.put(control::record_key(SOURCE, key), value);
        batch.put_cf(
            &self.0.db.cf_handle(SOURCE).expect("opened column family"),
            key,
            value,
        );
    }

    pub fn delete_source_transaction(&self, key: &[u8]) -> Result<()> {
        let _guard = self.lock()?;
        let mut batch = StateBatch::default();
        batch.control.delete(control::record_key(SOURCE, key));
        batch.delete_cf(
            &self.0.db.cf_handle(SOURCE).expect("opened column family"),
            key,
        );
        self.write(batch)
    }

    /// Atomically persist ledger metadata, transaction updates, and
    /// reclaim a completed key range. Payloads contain journal references, never
    /// source rows; the caller bounds the number of descriptors in the batch.
    pub fn update_source_ledger<'a>(
        &self,
        metadata: (&[u8], &[u8]),
        transactions: impl IntoIterator<Item = (&'a [u8], &'a [u8])>,
        completed_range: Option<(&[u8], &[u8])>,
    ) -> Result<()> {
        // The single ledger owner supplies a complete SOURCE-only batch. Its
        // controlled commit needs revision serialization, not the row CAS lock.
        let _guard = if self.0.control.is_none() {
            Some(self.lock()?)
        } else {
            None
        };
        let cf = self.0.db.cf_handle(SOURCE).expect("opened column family");
        let mut batch = StateBatch::default();
        batch
            .control
            .put(control::record_key(SOURCE, metadata.0), metadata.1);
        batch.put_cf(&cf, metadata.0, metadata.1);
        for (key, value) in transactions {
            batch.control.put(control::record_key(SOURCE, key), value);
            batch.put_cf(&cf, key, value);
        }
        if let Some((start, end)) = completed_range {
            batch.control.delete_range(
                control::record_key(SOURCE, start),
                control::record_key(SOURCE, end),
            );
            batch.delete_range_cf(&cf, start, end);
        }
        self.write(batch)
    }

    /// Advance a table's source ledger for a transaction with no net rows. No
    /// Iceberg snapshot is created, and a pending publication cannot be skipped.
    pub fn complete_noop(
        &self,
        table: &TableId,
        last_lsn: PgLsn,
        schema_version: u32,
    ) -> Result<()> {
        let _guard = self.lock()?;
        let mut state = self.table_state(table)?;
        if state.pending_operation.is_some()
            || last_lsn < state.materialized_lsn
            || schema_version < state.schema_version
        {
            return Err(Error::InvalidState(
                "no-op completion is fenced or would regress table state".into(),
            ));
        }
        state.materialized_lsn = last_lsn;
        state.schema_version = schema_version;
        let mut batch = StateBatch::default();
        self.put(&mut batch, TABLES, table.0.to_be_bytes(), &state)?;
        self.write(batch)
    }

    /// Remove a completed operation after the caller durably records table
    /// completion in the source ledger. Public data files and row indices stay
    /// untouched; replay is subsequently fenced by the materialized watermark.
    pub fn forget_applied(&self, id: &OperationId) -> Result<()> {
        let _guard = self.lock()?;
        let record = self.require_operation(id)?;
        if record.phase != OperationPhase::Applied
            || self
                .table_state(&record.operation.table_id)?
                .pending_operation
                .as_ref()
                == Some(id)
        {
            return Err(Error::InvalidState(
                "cannot forget an incomplete or fenced operation".into(),
            ));
        }
        let mut batch = StateBatch::default();
        self.delete_prefix(&mut batch, DELTAS, &operation_prefix(id));
        batch
            .control
            .delete(control::record_key(OPERATIONS, id.0.as_bytes()));
        batch.delete_cf(
            &self
                .0
                .db
                .cf_handle(OPERATIONS)
                .expect("opened column family"),
            id.0.as_bytes(),
        );
        self.write(batch)
    }

    /// Reclaim completed delta payloads after source acknowledgement is durable.
    /// Keep the tiny operation marker for replay detection until snapshot history
    /// retention permits the caller to remove it with the checkpoint protocol.
    pub fn prune_applied_deltas(&self, id: &OperationId) -> Result<()> {
        let _guard = self.lock()?;
        let record = self.require_operation(id)?;
        if record.phase != OperationPhase::Applied {
            return Err(Error::InvalidState(
                "cannot prune an incomplete index transition".into(),
            ));
        }
        let mut batch = StateBatch::default();
        self.delete_prefix(&mut batch, DELTAS, &operation_prefix(id));
        self.write(batch)
    }

    pub fn source_transaction(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if let Some(control) = &self.0.control {
            return control.source_transaction(key);
        }
        Ok(self.0.db.get_cf(
            &self.0.db.cf_handle(SOURCE).expect("opened column family"),
            key,
        )?)
    }

    pub fn source_transactions(&self) -> Box<dyn Iterator<Item = Result<RawEntry>> + '_> {
        if let Some(control) = &self.0.control {
            Box::new(control.source_transactions())
        } else {
            Box::new(self.scan(SOURCE, Vec::new()))
        }
    }

    /// Stream a source-key prefix, optionally starting strictly after a prior key.
    /// The caller owns pagination; rows are never collected into a backlog vector.
    pub fn source_transactions_after(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
    ) -> Box<dyn Iterator<Item = Result<RawEntry>> + '_> {
        if let Some(control) = &self.0.control {
            Box::new(control.source_transactions_after(prefix, after))
        } else {
            Box::new(self.scan_after(SOURCE, prefix.to_vec(), after))
        }
    }

    pub fn checkpoint(&self, path: impl AsRef<Path>) -> Result<()> {
        let _guard = self.lock()?;
        self.0.db.flush_wal(true)?;
        Checkpoint::new(&self.0.db)?.create_checkpoint(path.as_ref())?;
        control::sync_directory_tree(path.as_ref())?;
        Ok(())
    }

    fn require_operation(&self, id: &OperationId) -> Result<OperationRecord> {
        self.operation(id)?
            .ok_or_else(|| Error::InvalidState("unknown operation".into()))
    }
    fn get<T: DeserializeOwned>(&self, cf: &str, key: &[u8]) -> Result<Option<T>> {
        self.0
            .db
            .get_pinned_cf(&self.0.db.cf_handle(cf).expect("opened column family"), key)?
            .map(|v| decode(&v))
            .transpose()
    }
    fn put<T: Serialize>(
        &self,
        batch: &mut StateBatch,
        cf: &str,
        key: impl AsRef<[u8]>,
        value: &T,
    ) -> Result<()> {
        let bytes = bincode::serialize(value)?;
        if self.0.control.is_some() && control::is_authority(cf) {
            batch
                .control
                .put(control::record_key(cf, key.as_ref()), &bytes);
        }
        batch.put_cf(
            &self.0.db.cf_handle(cf).expect("opened column family"),
            key,
            bytes,
        );
        Ok(())
    }
    fn write(&self, mut batch: StateBatch) -> Result<()> {
        if let Some(control) = &self.0.control {
            control.commit_index(self, &mut batch)
        } else {
            Ok(self.0.db.write_opt(batch.index, &durable_write())?)
        }
    }
    /// Derived state uses the WAL without forcing a disk flush per chunk.
    /// Sealing synchronizes the standalone index WAL or the separate control
    /// revision. Controlled index writes remain unsynced and require revision
    /// validation on reopen, with rebuild on mismatch. Never disable the WAL here.
    fn write_staged(&self, batch: StateBatch) -> Result<()> {
        Ok(self.0.db.write_opt(batch.index, &WriteOptions::default())?)
    }

    fn scan(&self, cf: &str, prefix: Vec<u8>) -> impl Iterator<Item = Result<RawEntry>> + '_ {
        self.scan_after(cf, prefix, None)
    }
    fn scan_after(
        &self,
        cf: &str,
        prefix: Vec<u8>,
        after: Option<&[u8]>,
    ) -> impl Iterator<Item = Result<RawEntry>> + '_ {
        let mut start = after.map_or_else(
            || prefix.clone(),
            |key| {
                let mut start = key.to_vec();
                start.push(0);
                start
            },
        );
        if start < prefix {
            start.clone_from(&prefix);
        }
        let handle = self.0.db.cf_handle(cf).expect("opened column family");
        let mut opts = ReadOptions::default();
        opts.fill_cache(false);
        if let Some(end) = prefix_end(&prefix) {
            opts.set_iterate_upper_bound(end);
        }
        self.0
            .db
            .iterator_cf_opt(
                &handle,
                opts,
                IteratorMode::From(&start, Direction::Forward),
            )
            .take_while(move |item| {
                item.as_ref()
                    .map(|(key, _)| key.starts_with(&prefix))
                    .unwrap_or(true)
            })
            .map(|item| item.map_err(Error::from))
    }
    fn delete_prefix(&self, batch: &mut StateBatch, cf: &str, prefix: &[u8]) {
        let end =
            prefix_end(prefix).expect("length-prefixed internal key always has an upper bound");
        batch.delete_range_cf(
            &self.0.db.cf_handle(cf).expect("opened column family"),
            prefix,
            &end,
        );
    }
}

#[derive(Default)]
struct StateBatch {
    index: WriteBatch,
    control: WriteBatch,
}
impl Deref for StateBatch {
    type Target = WriteBatch;
    fn deref(&self) -> &Self::Target {
        &self.index
    }
}
impl DerefMut for StateBatch {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.index
    }
}

fn durable_write() -> WriteOptions {
    let mut options = WriteOptions::default();
    options.set_sync(true);
    options
}
fn decode<T: DeserializeOwned>(value: &[u8]) -> Result<T> {
    Ok(bincode::deserialize(value)?)
}
fn decode_borrowed_row_location(value: &[u8]) -> Result<BorrowedRowLocation<'_>> {
    Ok(bincode::deserialize(value)?)
}
fn decode_file_count(bytes: &[u8]) -> Result<u64> {
    Ok(u64::from_be_bytes(bytes.try_into().map_err(|_| {
        Error::RecoveryRequired("invalid per-file live-row count; rebuild the index".into())
    })?))
}
fn pk_key(table: &TableId, key: &PrimaryKey) -> Vec<u8> {
    let mut result = Vec::with_capacity(size_of::<u32>() + key.0.len());
    result.extend_from_slice(&table.0.to_be_bytes());
    result.extend_from_slice(&key.0);
    result
}
fn file_prefix(table: &TableId, file: &FileId) -> Vec<u8> {
    // Reverse keys append a row position to this prefix.
    let mut result = Vec::with_capacity(size_of::<u32>() + 2 * size_of::<u64>() + file.0.len());
    result.extend_from_slice(&table.0.to_be_bytes());
    append_component(&mut result, file.0.as_bytes());
    result
}
fn reverse_key(table: &TableId, location: &RowLocation) -> Vec<u8> {
    let mut result = file_prefix(table, &location.data_file_id);
    result.extend_from_slice(&location.row_position.to_be_bytes());
    result
}
fn operation_prefix(id: &OperationId) -> Vec<u8> {
    // Delta keys append a marker and ordinal to this prefix.
    let mut result = Vec::with_capacity(2 * size_of::<u64>() + 1 + id.0.len());
    append_component(&mut result, id.0.as_bytes());
    result
}
fn delta_key(id: &OperationId, offset: u64) -> Vec<u8> {
    let mut key = operation_prefix(id);
    key.push(0);
    key.extend_from_slice(&offset.to_be_bytes());
    key
}
fn append_component(result: &mut Vec<u8>, component: &[u8]) {
    result.extend_from_slice(&(component.len() as u64).to_be_bytes());
    result.extend_from_slice(component);
}
fn prefix_end(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut result = prefix.to_vec();
    while let Some(byte) = result.pop() {
        if byte != u8::MAX {
            result.push(byte + 1);
            return Some(result);
        }
    }
    None
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
