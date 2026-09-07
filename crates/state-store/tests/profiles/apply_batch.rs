use super::*;
use crate::{ControlStore, PreparedOperation, StateStoreOptions, TableState};
use flow_model::TableId;
use serde_json::json;
use std::{
    fs,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};
use tempfile::TempDir;

const TABLE: TableId = TableId(51);
const SEED_LSN: PgLsn = PgLsn(1_000);
const REWRITE_LSN: PgLsn = PgLsn(2_000);
const SEED_SNAPSHOT: i64 = 100;
const REWRITE_SNAPSHOT: i64 = 101;
const SEED_SEQUENCE: i64 = 7;
const REWRITE_SEQUENCE: i64 = 8;
const SETUP_BATCH_ROWS: usize = 1_024;

#[derive(Clone, Copy)]
enum Scenario {
    Compaction,
    MixedUpdateDelete,
    WideCompaction,
}

impl Scenario {
    fn configured() -> Self {
        match std::env::var("FLOW_STATE_APPLY_PROFILE_SCENARIO")
            .unwrap_or_else(|_| "compaction".into())
            .as_str()
        {
            "compaction" => Self::Compaction,
            "mixed_update_delete" => Self::MixedUpdateDelete,
            "wide_compaction" => Self::WideCompaction,
            value => panic!(
                "FLOW_STATE_APPLY_PROFILE_SCENARIO must be compaction, mixed_update_delete, or wide_compaction, got {value}"
            ),
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Compaction => "compaction",
            Self::MixedUpdateDelete => "mixed_update_delete",
            Self::WideCompaction => "wide_compaction",
        }
    }

    fn is_wide(self) -> bool {
        matches!(self, Self::WideCompaction)
    }

    fn is_delete(self, row: u64) -> bool {
        matches!(self, Self::MixedUpdateDelete) && row.is_multiple_of(10)
    }

    fn rewrite_lsn(self) -> PgLsn {
        if matches!(self, Self::MixedUpdateDelete) {
            REWRITE_LSN
        } else {
            SEED_LSN
        }
    }

    fn logical_generation(self) -> u64 {
        u64::from(matches!(self, Self::MixedUpdateDelete))
    }

    fn lookup_chunk_rows(self) -> u64 {
        if self.is_wide() { 256 } else { 4_096 }
    }

    fn key(self, row: u64) -> PrimaryKey {
        if self.is_wide() {
            let len = 1024 + bounded_hash(row, 3_073);
            let mut key = patterned_bytes(len, row ^ 0x7a5b_d911);
            key[..size_of::<u64>()].copy_from_slice(&row.to_be_bytes());
            PrimaryKey(key)
        } else {
            PrimaryKey(row.to_be_bytes().to_vec())
        }
    }

    fn partition(self, file: u64) -> Vec<u8> {
        if self.is_wide() {
            let len = 1024 + bounded_hash(file.rotate_left(19), 3_073);
            patterned_bytes(len, file ^ 0x4d2a_91b7)
        } else {
            format!("tenant={:04}/day={:02}", file % 1_009, file % 31).into_bytes()
        }
    }

    fn file(self, generation: &str, file: u64) -> FileId {
        let mut path = format!(
            "s3://warehouse/flow-profile/events/data/tenant=production/day=2026-09-04/{generation}-{file:06}-"
        );
        if self.is_wide() {
            let target = 1024 + bounded_hash(file.wrapping_mul(0x9e37_79b9), 3_073);
            path.extend(std::iter::repeat_n('x', target.saturating_sub(path.len())));
        } else {
            path.push_str("000000.parquet");
        }
        FileId(path)
    }
}

#[derive(Clone, Copy)]
struct ProfileConfig {
    rows: u64,
    files: u64,
    batch_rows: usize,
    lookup_rows: usize,
    scenario: Scenario,
}

impl ProfileConfig {
    fn configured() -> Self {
        let rows = configured_u64("FLOW_STATE_APPLY_PROFILE_ROWS", 100_000);
        let files = configured_u64("FLOW_STATE_APPLY_PROFILE_FILES", 256);
        let batch_rows = configured_usize("FLOW_STATE_APPLY_PROFILE_BATCH_ROWS", 1_024);
        let lookup_rows = configured_usize("FLOW_STATE_APPLY_PROFILE_LOOKUP_ROWS", 1_024);
        assert!(rows >= 2, "profile needs at least two rows");
        assert_ne!(files, 0, "profile needs at least one file");
        assert_ne!(batch_rows, 0, "batch row ceiling must be positive");
        assert_ne!(lookup_rows, 0, "lookup row ceiling must be positive");
        Self {
            rows,
            files,
            batch_rows,
            lookup_rows,
            scenario: Scenario::configured(),
        }
    }

    fn apply_options(self) -> StateStoreOptions {
        StateStoreOptions {
            apply_batch_rows: self.batch_rows,
            apply_lookup_rows: self.lookup_rows,
            ..StateStoreOptions::default()
        }
    }
}

fn configured_u64(name: &str, default: u64) -> u64 {
    std::env::var(name).map_or(default, |value| {
        value
            .parse::<u64>()
            .unwrap_or_else(|_| panic!("{name} must be a u64"))
    })
}

fn configured_usize(name: &str, default: usize) -> usize {
    std::env::var(name).map_or(default, |value| {
        value
            .parse::<usize>()
            .unwrap_or_else(|_| panic!("{name} must be a usize"))
    })
}

fn bounded_hash(value: u64, modulus: usize) -> usize {
    (value.wrapping_mul(0x9e37_79b9_7f4a_7c15).rotate_left(23) % modulus as u64) as usize
}

fn patterned_bytes(len: usize, seed: u64) -> Vec<u8> {
    (0..len)
        .map(|offset| seed.rotate_left((offset % 64) as u32) as u8 ^ offset as u8)
        .collect()
}

fn fingerprint(row: u64, generation: u64) -> [u8; 16] {
    let mut fingerprint = [0_u8; 16];
    fingerprint[..8].copy_from_slice(&row.to_be_bytes());
    fingerprint[8..].copy_from_slice(&(row.rotate_left(17) ^ generation).to_be_bytes());
    fingerprint
}

#[derive(Clone, Copy)]
enum PhysicalGeneration {
    Old,
    New,
}

impl PhysicalGeneration {
    fn name(self) -> &'static str {
        match self {
            Self::Old => "old",
            Self::New => "new",
        }
    }
}

fn location(
    config: ProfileConfig,
    generation: PhysicalGeneration,
    row: u64,
    sequence: i64,
) -> RowLocation {
    let file = row % config.files;
    let ordinal = row / config.files;
    let logical_generation = match generation {
        PhysicalGeneration::Old => 0,
        PhysicalGeneration::New => config.scenario.logical_generation(),
    };
    let lsn = if logical_generation == 0 {
        SEED_LSN
    } else {
        REWRITE_LSN
    };
    RowLocation {
        data_file_id: config.scenario.file(generation.name(), file),
        row_position: ordinal
            .checked_mul(2)
            .and_then(|position| {
                position.checked_add(u64::from(matches!(generation, PhysicalGeneration::Old)))
            })
            .expect("profile row position fits u64"),
        data_sequence_number: sequence,
        spec_id: 0,
        partition: config.scenario.partition(file),
        source_commit_lsn: lsn,
        row_version: lsn.0,
        row_fingerprint: fingerprint(row, logical_generation),
    }
}

fn operation(
    id: &str,
    kind: OperationKind,
    base_snapshot_id: Option<i64>,
    lsn: PgLsn,
) -> PreparedOperation {
    PreparedOperation {
        id: OperationId(id.into()),
        table_id: TABLE,
        kind,
        base_snapshot_id,
        last_lsn: lsn,
        schema_version: 3,
        artifacts: Vec::new(),
        payload: Vec::new(),
    }
}

fn seed_delta(config: ProfileConfig, row: u64) -> IndexDelta {
    IndexDelta {
        key: config.scenario.key(row),
        expected: None,
        replacement: Some(location(config, PhysicalGeneration::Old, row, -1)),
    }
}

fn rewrite_delta(config: ProfileConfig, row: u64) -> IndexDelta {
    IndexDelta {
        key: config.scenario.key(row),
        expected: Some(location(
            config,
            PhysicalGeneration::Old,
            row,
            SEED_SEQUENCE,
        )),
        replacement: (!config.scenario.is_delete(row))
            .then(|| location(config, PhysicalGeneration::New, row, -1)),
    }
}

fn file_major_rows(config: ProfileConfig) -> impl Iterator<Item = u64> {
    let step = usize::try_from(config.files).expect("file count fits usize");
    (0..config.files).flat_map(move |file| (file..config.rows).step_by(step))
}

fn initialize(root: &Path) -> (ControlStore, StateStore, PathBuf, PathBuf) {
    let control_path = root.join("control");
    let index_path = root.join("index");
    let control = ControlStore::open(&control_path).unwrap();
    let store = control
        .initialize_index(
            &index_path,
            StateStoreOptions {
                apply_batch_rows: SETUP_BATCH_ROWS,
                apply_lookup_rows: SETUP_BATCH_ROWS,
                ..StateStoreOptions::default()
            },
        )
        .unwrap();
    (control, store, control_path, index_path)
}

fn reopen(
    control_path: &Path,
    index_path: &Path,
    config: ProfileConfig,
) -> (ControlStore, StateStore) {
    let control = ControlStore::open(control_path).unwrap();
    let store =
        StateStore::open_with_control(index_path, config.apply_options(), control.clone()).unwrap();
    (control, store)
}

fn seed(store: &StateStore, config: ProfileConfig) {
    let seed = operation("seed", OperationKind::Ingest, None, SEED_LSN);
    store
        .prepare(
            seed.clone(),
            file_major_rows(config).map(|row| seed_delta(config, row)),
        )
        .unwrap();
    store
        .mark_committed(&seed.id, SEED_SNAPSHOT, SEED_SEQUENCE)
        .unwrap();
    let result = store.apply_committed(&seed.id).unwrap();
    assert_eq!(result.applied_rows, config.rows);
    assert_eq!(result.obsolete_rows, 0);
    assert!(result.complete);
    store.forget_applied(&seed.id).unwrap();
}

fn prepare_rewrite(store: &StateStore, config: ProfileConfig, id: &str) -> OperationId {
    let rewrite = operation(
        id,
        OperationKind::Rewrite,
        Some(SEED_SNAPSHOT),
        config.scenario.rewrite_lsn(),
    );
    store
        .prepare(
            rewrite.clone(),
            file_major_rows(config).map(|row| rewrite_delta(config, row)),
        )
        .unwrap();
    store
        .mark_committed(&rewrite.id, REWRITE_SNAPSHOT, REWRITE_SEQUENCE)
        .unwrap();
    rewrite.id
}

fn expected_table_state(config: ProfileConfig) -> TableState {
    TableState {
        snapshot_id: Some(REWRITE_SNAPSHOT),
        materialized_lsn: config.scenario.rewrite_lsn(),
        schema_version: 3,
        pending_operation: None,
    }
}

fn verify_pre_apply(
    store: &StateStore,
    control: &ControlStore,
    id: &OperationId,
    config: ProfileConfig,
) {
    let expected = TableState {
        snapshot_id: Some(SEED_SNAPSHOT),
        materialized_lsn: SEED_LSN,
        schema_version: 3,
        pending_operation: Some(id.clone()),
    };
    assert_eq!(store.table_state(&TABLE).unwrap(), expected);
    assert_eq!(control.table_state(&TABLE).unwrap().unwrap(), expected);
    let record = store.operation(id).unwrap().unwrap();
    assert_eq!(record.phase, OperationPhase::Committed);
    assert_eq!(record.snapshot_id, Some(REWRITE_SNAPSHOT));
    assert_eq!(record.sequence_number, Some(REWRITE_SEQUENCE));
    assert_eq!(record.delta_count, config.rows);
    assert_eq!(record.applied_count, 0);
}

fn verify_exact(
    store: &StateStore,
    control: &ControlStore,
    id: &OperationId,
    config: ProfileConfig,
) {
    assert_eq!(
        store.table_state(&TABLE).unwrap(),
        expected_table_state(config)
    );
    assert_eq!(
        control.table_state(&TABLE).unwrap().unwrap(),
        expected_table_state(config)
    );
    assert!(control.pending_operations().unwrap().is_empty());

    let record = store.operation(id).unwrap().unwrap();
    assert_eq!(record.operation.kind, OperationKind::Rewrite);
    assert_eq!(record.phase, OperationPhase::Applied);
    assert_eq!(record.snapshot_id, Some(REWRITE_SNAPSHOT));
    assert_eq!(record.sequence_number, Some(REWRITE_SEQUENCE));
    assert_eq!(record.delta_count, config.rows);
    assert_eq!(record.applied_count, config.rows);

    let mut start = 0;
    while start < config.rows {
        let end = config.rows.min(start + config.scenario.lookup_chunk_rows());
        let keys = (start..end)
            .map(|row| config.scenario.key(row))
            .collect::<Vec<_>>();
        let actual = store.lookup_many(&TABLE, &keys).unwrap();
        for (row, actual) in (start..end).zip(actual) {
            let expected = (!config.scenario.is_delete(row))
                .then(|| location(config, PhysicalGeneration::New, row, REWRITE_SEQUENCE));
            assert_eq!(actual, expected, "forward row {row}");
        }
        start = end;
    }

    let file_count = usize::try_from(config.files).expect("file count fits usize");
    let old_files = (0..config.files)
        .map(|file| config.scenario.file("old", file))
        .collect::<Vec<_>>();
    let new_files = (0..config.files)
        .map(|file| config.scenario.file("new", file))
        .collect::<Vec<_>>();
    let files = old_files
        .iter()
        .chain(&new_files)
        .cloned()
        .collect::<Vec<_>>();
    let actual_counts = store
        .file_live_row_counts(&TABLE, Some(REWRITE_SNAPSHOT), &files)
        .unwrap();
    assert!(actual_counts[..file_count].iter().all(|count| *count == 0));
    let mut expected_counts = vec![0_u64; file_count];
    for row in 0..config.rows {
        if !config.scenario.is_delete(row) {
            expected_counts[(row % config.files) as usize] += 1;
        }
    }
    assert_eq!(&actual_counts[file_count..], expected_counts.as_slice());

    for file in old_files {
        assert!(store.file_rows(&TABLE, &file).next().is_none());
    }
    for (file_index, file) in new_files.iter().enumerate() {
        let mut actual = store.file_rows(&TABLE, file);
        for row in (file_index as u64..config.rows)
            .step_by(file_count)
            .filter(|row| !config.scenario.is_delete(*row))
        {
            let expected = location(config, PhysicalGeneration::New, row, REWRITE_SEQUENCE);
            assert_eq!(
                actual.next().unwrap().unwrap(),
                (expected.row_position, config.scenario.key(row)),
                "reverse row {row}"
            );
        }
        assert!(actual.next().is_none());
    }
}

#[derive(Default)]
struct DiskUsage {
    total: u64,
    wal_footprint: u64,
}

fn disk_usage(path: &Path) -> std::io::Result<DiskUsage> {
    fn visit(path: &Path, usage: &mut DiskUsage) -> std::io::Result<()> {
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            let kind = entry.file_type()?;
            if kind.is_dir() {
                visit(&entry.path(), usage)?;
            } else if kind.is_file() {
                let bytes = entry.metadata()?.len();
                usage.total += bytes;
                if entry
                    .path()
                    .extension()
                    .is_some_and(|extension| extension == "log")
                {
                    usage.wal_footprint += bytes;
                }
            }
        }
        Ok(())
    }

    let mut usage = DiskUsage::default();
    visit(path, &mut usage)?;
    Ok(usage)
}

fn signed_byte_delta(after: u64, before: u64) -> i64 {
    if after >= before {
        i64::try_from(after - before).expect("profile size delta fits i64")
    } else {
        -i64::try_from(before - after).expect("profile size delta fits i64")
    }
}

struct ApplyObservation {
    result: ApplyResult,
    batches: Vec<ApplyBatchStats>,
    batch_call_ms: Vec<f64>,
}

impl ApplyObservation {
    fn total_ms(&self) -> f64 {
        self.batch_call_ms.iter().sum()
    }

    fn batch_call_distribution(&self) -> serde_json::Value {
        distribution(self.batch_call_ms.clone())
    }
}

fn distribution(mut samples: Vec<f64>) -> serde_json::Value {
    assert!(!samples.is_empty(), "profile distribution needs a sample");
    samples.sort_by(f64::total_cmp);
    let percentile = |value: usize| {
        let index = (samples.len() * value).div_ceil(100).saturating_sub(1);
        samples[index]
    };
    json!({
        "p50": percentile(50),
        "p95": percentile(95),
        "p99": percentile(99),
        "max": percentile(100),
    })
}

fn duration_distribution(
    batches: &[ApplyBatchStats],
    field: impl Fn(&ApplyBatchStats) -> Duration,
) -> serde_json::Value {
    distribution(
        batches
            .iter()
            .map(|batch| field(batch).as_secs_f64() * 1_000.0)
            .collect(),
    )
}

fn observe_apply(store: &StateStore, id: &OperationId) -> ApplyObservation {
    let mut result = ApplyResult {
        applied_rows: 0,
        obsolete_rows: 0,
        complete: false,
    };
    let mut cursor = store.operation(id).unwrap().unwrap().applied_count;
    let mut batches = Vec::new();
    let mut batch_call_ms = Vec::new();
    while !result.complete {
        let batch_started = Instant::now();
        let outcome = store.apply_batch(id, false).unwrap();
        batch_call_ms.push(batch_started.elapsed().as_secs_f64() * 1_000.0);
        let batch = outcome.result;
        let next_cursor = store.operation(id).unwrap().unwrap().applied_count;
        let advanced = next_cursor
            .checked_sub(cursor)
            .expect("apply cursor only advances");
        assert_ne!(advanced, 0, "an incomplete apply must advance its cursor");
        assert_eq!(advanced, batch.applied_rows + batch.obsolete_rows);
        assert_eq!(advanced, outcome.stats.cursor_rows);
        batches.push(outcome.stats);
        cursor = next_cursor;
        result.applied_rows += batch.applied_rows;
        result.obsolete_rows += batch.obsolete_rows;
        result.complete = batch.complete;
    }
    ApplyObservation {
        result,
        batches,
        batch_call_ms,
    }
}

#[test]
#[ignore = "manual profile; configure with FLOW_STATE_APPLY_PROFILE_* variables"]
fn profiles_controlled_apply_batches() {
    let config = ProfileConfig::configured();
    let root = TempDir::new().unwrap();
    let (control, store, control_path, index_path) = initialize(root.path());
    seed(&store, config);
    let id = prepare_rewrite(&store, config, "profile-rewrite");
    drop(store);
    drop(control);
    let index_usage_pre_apply = disk_usage(&index_path).unwrap();
    let control_usage_pre_apply = disk_usage(&control_path).unwrap();

    let (control, store) = reopen(&control_path, &index_path, config);
    verify_pre_apply(&store, &control, &id, config);

    let observation = observe_apply(&store, &id);
    let result = observation.result;
    assert_eq!(result.applied_rows, config.rows);
    assert_eq!(result.obsolete_rows, 0);
    assert!(result.complete);
    verify_exact(&store, &control, &id, config);
    drop(store);
    drop(control);
    let index_usage_after_apply = disk_usage(&index_path).unwrap();
    let control_usage_after_apply = disk_usage(&control_path).unwrap();

    let (control, store) = reopen(&control_path, &index_path, config);
    verify_exact(&store, &control, &id, config);
    drop(store);
    drop(control);

    let index_usage_after_reopen = disk_usage(&index_path).unwrap();
    let control_usage_after_reopen = disk_usage(&control_path).unwrap();
    let deletes = if matches!(config.scenario, Scenario::MixedUpdateDelete) {
        config.rows.div_ceil(10)
    } else {
        0
    };
    eprintln!(
        "state_store_apply_profile {}",
        serde_json::to_string(&json!({
            "kind": "summary",
            "version": 2,
            "controlled_store": true,
            "config": {
                "scenario": config.scenario.name(),
                "rows": config.rows,
                "replacement_rows": config.rows - deletes,
                "delete_rows": deletes,
                "file_count": config.files,
                "setup_batch_row_ceiling": SETUP_BATCH_ROWS,
                "batch_row_ceiling": config.batch_rows,
                "lookup_row_ceiling": config.lookup_rows,
                "write_batch_byte_ceiling": serde_json::Value::Null,
            },
            "apply": {
                "batches": observation.batches.len(),
                "batch_count_source": "observed_cursor_advances",
                "batch_cursor_rows": observation.batches.iter().map(|batch| batch.cursor_rows).collect::<Vec<_>>(),
                "batch_serialized_write_bytes": observation.batches.iter().map(|batch| batch.write_batch_bytes).collect::<Vec<_>>(),
                "batch_write_operations": observation.batches.iter().map(|batch| batch.write_batch_operations).collect::<Vec<_>>(),
                "batch_reverse_owner_reads": observation.batches.iter().map(|batch| batch.reverse_owner_reads).collect::<Vec<_>>(),
                "batch_call_ms": observation.batch_call_distribution(),
                "phase_ms": {
                    "decode": duration_distribution(&observation.batches, |batch| batch.decode),
                    "pk_lookup": duration_distribution(&observation.batches, |batch| batch.pk_lookup),
                    "reverse_lookup": duration_distribution(&observation.batches, |batch| batch.reverse_lookup),
                    "mutation_build": duration_distribution(&observation.batches, |batch| batch.mutation_build),
                    "file_count": duration_distribution(&observation.batches, |batch| batch.file_count),
                    "write": duration_distribution(&observation.batches, |batch| batch.write),
                    "lock_wait": duration_distribution(&observation.batches, |batch| batch.lock_wait),
                    "lock_hold": duration_distribution(&observation.batches, |batch| batch.lock_hold),
                },
                "total_ms": observation.total_ms(),
                "timing_scope": "end_to_end_apply_batch_calls",
                "write_batch_observation": "serialized index WriteBatch as submitted; controlled durable batches include the revision mutation",
                "limit_note": "row ceilings only; no serialized 8 MiB WriteBatch cap is enforced",
                "applied_rows": result.applied_rows,
                "obsolete_rows": result.obsolete_rows,
            },
            "validation": { "after_apply": true, "after_reopen": true },
            "storage_measurement": "directory bytes after clean database close; WAL fields are file footprints",
            "storage": {
                "index_db_bytes_pre_apply": index_usage_pre_apply.total,
                "index_wal_footprint_bytes_pre_apply": index_usage_pre_apply.wal_footprint,
                "control_db_bytes_pre_apply": control_usage_pre_apply.total,
                "control_wal_footprint_bytes_pre_apply": control_usage_pre_apply.wal_footprint,
                "index_db_bytes_after_apply": index_usage_after_apply.total,
                "index_wal_footprint_bytes_after_apply": index_usage_after_apply.wal_footprint,
                "control_db_bytes_after_apply": control_usage_after_apply.total,
                "control_wal_footprint_bytes_after_apply": control_usage_after_apply.wal_footprint,
                "index_db_bytes_apply_delta": signed_byte_delta(index_usage_after_apply.total, index_usage_pre_apply.total),
                "index_wal_footprint_bytes_apply_delta": signed_byte_delta(index_usage_after_apply.wal_footprint, index_usage_pre_apply.wal_footprint),
                "control_db_bytes_apply_delta": signed_byte_delta(control_usage_after_apply.total, control_usage_pre_apply.total),
                "control_wal_footprint_bytes_apply_delta": signed_byte_delta(control_usage_after_apply.wal_footprint, control_usage_pre_apply.wal_footprint),
                "index_db_bytes_after_reopen": index_usage_after_reopen.total,
                "index_wal_footprint_bytes_after_reopen": index_usage_after_reopen.wal_footprint,
                "control_db_bytes_after_reopen": control_usage_after_reopen.total,
                "control_wal_footprint_bytes_after_reopen": control_usage_after_reopen.wal_footprint,
            },
            "reopen_coverage": "separate_clean_reopen_test",
        }))
        .unwrap()
    );
}

#[test]
fn clean_reopen_resumes_at_an_atomic_batch_cursor() {
    let config = ProfileConfig {
        rows: 257,
        files: 17,
        batch_rows: 64,
        lookup_rows: 11,
        scenario: Scenario::Compaction,
    };
    let root = TempDir::new().unwrap();
    let (control, store, control_path, index_path) = initialize(root.path());
    seed(&store, config);
    let id = prepare_rewrite(&store, config, "reopened-rewrite");
    drop(store);
    drop(control);

    let (control, store) = reopen(&control_path, &index_path, config);
    verify_pre_apply(&store, &control, &id, config);
    let first = store.apply_batch(&id, false).unwrap();
    assert_eq!(first.stats.cursor_rows, config.batch_rows as u64);
    assert!(first.stats.write_batch_bytes > 0);
    assert!(first.stats.write_batch_operations > 0);
    let first = first.result;
    assert_eq!(first.applied_rows, config.batch_rows as u64);
    assert_eq!(first.obsolete_rows, 0);
    assert!(!first.complete);
    assert_eq!(
        store.operation(&id).unwrap().unwrap().applied_count,
        config.batch_rows as u64
    );
    assert_eq!(
        store.table_state(&TABLE).unwrap(),
        TableState {
            snapshot_id: Some(SEED_SNAPSHOT),
            materialized_lsn: SEED_LSN,
            schema_version: 3,
            pending_operation: Some(id.clone()),
        }
    );
    drop(store);
    drop(control);

    let (control, store) = reopen(&control_path, &index_path, config);
    let reopened = store.operation(&id).unwrap().unwrap();
    assert_eq!(reopened.phase, OperationPhase::Committed);
    assert_eq!(reopened.applied_count, config.batch_rows as u64);
    let resumed = store.apply_committed(&id).unwrap();
    assert_eq!(resumed.applied_rows, config.rows - config.batch_rows as u64);
    assert!(resumed.complete);
    verify_exact(&store, &control, &id, config);
    drop(store);
    drop(control);

    let (control, store) = reopen(&control_path, &index_path, config);
    verify_exact(&store, &control, &id, config);
    eprintln!(
        "state_store_apply_profile {}",
        serde_json::to_string(&json!({
            "kind": "clean_reopen_resume",
            "version": 2,
            "rows": config.rows,
            "setup_batch_row_ceiling": SETUP_BATCH_ROWS,
            "batch_row_ceiling": config.batch_rows,
            "lookup_row_ceiling": config.lookup_rows,
            "reopened_after_rows": config.batch_rows,
            "validation_after_resume": true,
            "validation_after_second_reopen": true,
            "boundary": "wal_backed_intermediate_batch_then_clean_close",
            "arbitrary_process_kill_covered": false,
        }))
        .unwrap()
    );
}
