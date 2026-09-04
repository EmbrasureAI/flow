use super::*;
use serde_json::json;
use std::{
    fs,
    path::{Path, PathBuf},
    time::Instant,
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
    Mixed,
    Wide,
}

impl Scenario {
    fn configured() -> Self {
        match std::env::var("FLOW_STATE_APPLY_PROFILE_SCENARIO")
            .unwrap_or_else(|_| "mixed".into())
            .as_str()
        {
            "mixed" => Self::Mixed,
            "wide" => Self::Wide,
            value => panic!("FLOW_STATE_APPLY_PROFILE_SCENARIO must be mixed or wide, got {value}"),
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Mixed => "mixed",
            Self::Wide => "wide",
        }
    }

    fn key(self, row: u64) -> PrimaryKey {
        match self {
            Self::Mixed => PrimaryKey(row.to_be_bytes().to_vec()),
            Self::Wide => {
                let len = 1024 + bounded_hash(row, 3_073);
                let mut key = patterned_bytes(len, row ^ 0x7a5b_d911);
                key[..size_of::<u64>()].copy_from_slice(&row.to_be_bytes());
                PrimaryKey(key)
            }
        }
    }

    fn partition(self, row: u64, generation: u64) -> Vec<u8> {
        match self {
            Self::Mixed => format!(
                "tenant={:04}/day={:02}/generation={generation}",
                row % 1_009,
                row % 31
            )
            .into_bytes(),
            Self::Wide => {
                let len = 1024 + bounded_hash(row.rotate_left(19) ^ generation, 3_073);
                patterned_bytes(len, row ^ generation.rotate_left(7))
            }
        }
    }

    fn file(self, generation: &str, file: u64) -> FileId {
        let mut path = format!(
            "s3://warehouse/flow-profile/events/data/tenant=production/day=2026-09-04/{generation}-{file:06}-"
        );
        if matches!(self, Self::Wide) {
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
    scenario: Scenario,
}

impl ProfileConfig {
    fn configured() -> Self {
        let rows = configured_u64("FLOW_STATE_APPLY_PROFILE_ROWS", 100_000);
        let files = configured_u64("FLOW_STATE_APPLY_PROFILE_FILES", 256);
        let batch_rows = configured_usize("FLOW_STATE_APPLY_PROFILE_BATCH_ROWS", 1_024);
        assert!(rows >= 2, "profile needs at least two rows");
        assert_ne!(files, 0, "profile needs at least one file");
        assert_ne!(batch_rows, 0, "batch row ceiling must be positive");
        Self {
            rows,
            files,
            batch_rows,
            scenario: Scenario::configured(),
        }
    }

    fn apply_options(self) -> StateStoreOptions {
        StateStoreOptions {
            apply_batch_rows: self.batch_rows,
            ..StateStoreOptions::default()
        }
    }

    fn inferred_batches(self) -> u64 {
        self.rows.div_ceil(self.batch_rows as u64)
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

fn location(
    config: ProfileConfig,
    generation: &'static str,
    row: u64,
    sequence: i64,
    lsn: PgLsn,
) -> RowLocation {
    let generation_number = u64::from(generation == "new");
    RowLocation {
        data_file_id: config.scenario.file(generation, row % config.files),
        row_position: row / config.files,
        data_sequence_number: sequence,
        spec_id: 0,
        partition: config.scenario.partition(row, generation_number),
        source_commit_lsn: lsn,
        row_version: lsn.0,
        row_fingerprint: fingerprint(row, generation_number),
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
        replacement: Some(location(config, "old", row, -1, SEED_LSN)),
    }
}

fn rewrite_delta(config: ProfileConfig, row: u64) -> IndexDelta {
    IndexDelta {
        key: config.scenario.key(row),
        expected: Some(location(config, "old", row, SEED_SEQUENCE, SEED_LSN)),
        replacement: (!row.is_multiple_of(10))
            .then(|| location(config, "new", row, -1, REWRITE_LSN)),
    }
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
            (0..config.rows).map(|row| seed_delta(config, row)),
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
    let rewrite = operation(id, OperationKind::Rewrite, Some(SEED_SNAPSHOT), REWRITE_LSN);
    store
        .prepare(
            rewrite.clone(),
            (0..config.rows).map(|row| rewrite_delta(config, row)),
        )
        .unwrap();
    store
        .mark_committed(&rewrite.id, REWRITE_SNAPSHOT, REWRITE_SEQUENCE)
        .unwrap();
    rewrite.id
}

fn expected_table_state() -> TableState {
    TableState {
        snapshot_id: Some(REWRITE_SNAPSHOT),
        materialized_lsn: REWRITE_LSN,
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
    assert_eq!(store.table_state(&TABLE).unwrap(), expected_table_state());
    assert_eq!(
        control.table_state(&TABLE).unwrap().unwrap(),
        expected_table_state()
    );
    assert!(control.pending_operations().unwrap().is_empty());

    let record = store.operation(id).unwrap().unwrap();
    assert_eq!(record.operation.kind, OperationKind::Rewrite);
    assert_eq!(record.phase, OperationPhase::Applied);
    assert_eq!(record.snapshot_id, Some(REWRITE_SNAPSHOT));
    assert_eq!(record.sequence_number, Some(REWRITE_SEQUENCE));
    assert_eq!(record.delta_count, config.rows);
    assert_eq!(record.applied_count, config.rows);

    const LOOKUP_ROWS: u64 = 4_096;
    let mut start = 0;
    while start < config.rows {
        let end = config.rows.min(start + LOOKUP_ROWS);
        let keys = (start..end)
            .map(|row| config.scenario.key(row))
            .collect::<Vec<_>>();
        let actual = store.lookup_many(&TABLE, &keys).unwrap();
        for (row, actual) in (start..end).zip(actual) {
            let expected = (!row.is_multiple_of(10))
                .then(|| location(config, "new", row, REWRITE_SEQUENCE, REWRITE_LSN));
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
        if !row.is_multiple_of(10) {
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
            .filter(|row| !row.is_multiple_of(10))
        {
            assert_eq!(
                actual.next().unwrap().unwrap(),
                (row / config.files, config.scenario.key(row)),
                "reverse row {row}"
            );
        }
        assert!(actual.next().is_none());
    }
}

#[derive(Default)]
struct DiskUsage {
    total: u64,
    wal: u64,
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
                    usage.wal += bytes;
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

    let (control, store) = reopen(&control_path, &index_path, config);
    verify_pre_apply(&store, &control, &id, config);
    let index_usage_pre_apply = disk_usage(&index_path).unwrap();
    let control_usage_pre_apply = disk_usage(&control_path).unwrap();

    let apply_started = Instant::now();
    let result = store.apply_committed(&id).unwrap();
    let apply_ms = apply_started.elapsed().as_secs_f64() * 1_000.0;
    assert_eq!(result.applied_rows, config.rows);
    assert_eq!(result.obsolete_rows, 0);
    assert!(result.complete);
    let index_usage_after_apply = disk_usage(&index_path).unwrap();
    let control_usage_after_apply = disk_usage(&control_path).unwrap();
    verify_exact(&store, &control, &id, config);
    drop(store);
    drop(control);

    let (control, store) = reopen(&control_path, &index_path, config);
    verify_exact(&store, &control, &id, config);
    drop(store);
    drop(control);

    let index_usage_after_reopen = disk_usage(&index_path).unwrap();
    let control_usage_after_reopen = disk_usage(&control_path).unwrap();
    let deletes = config.rows.div_ceil(10);
    eprintln!(
        "state_store_apply_profile {}",
        serde_json::to_string(&json!({
            "kind": "summary",
            "version": 1,
            "controlled_store": true,
            "scenario": config.scenario.name(),
            "rows": config.rows,
            "replacement_rows": config.rows - deletes,
            "delete_rows": deletes,
            "file_count": config.files,
            "setup_batch_row_ceiling": SETUP_BATCH_ROWS,
            "batch_row_ceiling": config.batch_rows,
            "apply_batches": config.inferred_batches(),
            "apply_batch_count_source": "fixed_row_ceiling",
            "per_batch_bytes_reported": false,
            "per_batch_timing_reported": false,
            "apply_total_ms": apply_ms,
            "applied_rows": result.applied_rows,
            "obsolete_rows": result.obsolete_rows,
            "validation_after_apply": true,
            "validation_after_reopen": true,
            "index_db_bytes_pre_apply": index_usage_pre_apply.total,
            "index_wal_bytes_pre_apply": index_usage_pre_apply.wal,
            "control_db_bytes_pre_apply": control_usage_pre_apply.total,
            "control_wal_bytes_pre_apply": control_usage_pre_apply.wal,
            "index_db_bytes_after_apply": index_usage_after_apply.total,
            "index_wal_bytes_after_apply": index_usage_after_apply.wal,
            "control_db_bytes_after_apply": control_usage_after_apply.total,
            "control_wal_bytes_after_apply": control_usage_after_apply.wal,
            "index_db_bytes_apply_delta": signed_byte_delta(index_usage_after_apply.total, index_usage_pre_apply.total),
            "index_wal_bytes_apply_delta": signed_byte_delta(index_usage_after_apply.wal, index_usage_pre_apply.wal),
            "control_db_bytes_apply_delta": signed_byte_delta(control_usage_after_apply.total, control_usage_pre_apply.total),
            "control_wal_bytes_apply_delta": signed_byte_delta(control_usage_after_apply.wal, control_usage_pre_apply.wal),
            "index_db_bytes_after_reopen": index_usage_after_reopen.total,
            "index_wal_bytes_after_reopen": index_usage_after_reopen.wal,
            "control_db_bytes_after_reopen": control_usage_after_reopen.total,
            "control_wal_bytes_after_reopen": control_usage_after_reopen.wal,
            "interruption_coverage": "separate_controlled_reopen_test",
        }))
        .unwrap()
    );
}

#[test]
fn controlled_reopen_resumes_at_an_atomic_batch_cursor() {
    let config = ProfileConfig {
        rows: 257,
        files: 17,
        batch_rows: 64,
        scenario: Scenario::Mixed,
    };
    let root = TempDir::new().unwrap();
    let (control, store, control_path, index_path) = initialize(root.path());
    seed(&store, config);
    let id = prepare_rewrite(&store, config, "interrupted-rewrite");
    drop(store);
    drop(control);

    let (control, store) = reopen(&control_path, &index_path, config);
    verify_pre_apply(&store, &control, &id, config);
    let first = store.apply_committed_batch(&id).unwrap();
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
    let interrupted = store.operation(&id).unwrap().unwrap();
    assert_eq!(interrupted.phase, OperationPhase::Committed);
    assert_eq!(interrupted.applied_count, config.batch_rows as u64);
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
            "kind": "controlled_reopen_resume",
            "version": 1,
            "rows": config.rows,
            "setup_batch_row_ceiling": SETUP_BATCH_ROWS,
            "batch_row_ceiling": config.batch_rows,
            "interrupted_after_rows": config.batch_rows,
            "validation_after_resume": true,
            "validation_after_second_reopen": true,
            "boundary": "durable_batch_then_clean_process-equivalent_close",
            "arbitrary_process_kill_covered": false,
        }))
        .unwrap()
    );
}
