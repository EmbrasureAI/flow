use super::*;
use rocksdb::{IngestExternalFileOptions, SstFileWriter};
use std::{
    path::Path,
    time::{Duration, Instant},
};
use tempfile::TempDir;

const TABLE: TableId = TableId(41);
const SOURCE_LSN: PgLsn = PgLsn(10);
const SEED_SNAPSHOT: i64 = 100;
const SEED_SEQUENCE: i64 = 7;
const REWRITE_SNAPSHOT: i64 = 101;
const REWRITE_SEQUENCE: i64 = 8;
const FILE_COUNT: u64 = 256;

#[derive(Clone, Copy)]
enum Staging {
    Current,
    OrdinalWal,
    ExternalSst,
}

impl Staging {
    fn name(self) -> &'static str {
        match self {
            Self::Current => "current",
            Self::OrdinalWal => "ordinal_wal",
            Self::ExternalSst => "external_sst",
        }
    }

    fn index(self) -> usize {
        match self {
            Self::Current => 0,
            Self::OrdinalWal => 1,
            Self::ExternalSst => 2,
        }
    }

    fn assumption(self) -> &'static str {
        match self {
            Self::Current => "full_uniqueness_validation",
            Self::OrdinalWal | Self::ExternalSst => "prevalidated_unique_compaction_mapping",
        }
    }
}

struct Observation {
    activation: Duration,
    payload_install: Duration,
    seal: Duration,
    apply: Duration,
    sst_build: Duration,
    sst_ingest: Duration,
    sst_bytes: u64,
}

struct ExternalArtifact {
    count: u64,
    bytes: u64,
    build: Duration,
}

fn open(path: &Path) -> StateStore {
    StateStore::open(path, StateStoreOptions::default()).unwrap()
}

fn configured_rows() -> u64 {
    let rows = std::env::var("FLOW_STATE_PROFILE_ROWS")
        .map(|value| {
            value
                .parse::<u64>()
                .expect("FLOW_STATE_PROFILE_ROWS is a u64")
        })
        .unwrap_or(100_000);
    assert!(rows >= 2, "profile needs at least two rows");
    rows
}

fn configured_rounds() -> usize {
    let rounds = std::env::var("FLOW_STATE_PROFILE_ROUNDS")
        .map(|value| {
            value
                .parse::<usize>()
                .expect("FLOW_STATE_PROFILE_ROUNDS is a usize")
        })
        .unwrap_or(1);
    assert!((1..=9).contains(&rounds), "profile rounds must be 1..=9");
    rounds
}

fn key(row: u64) -> PrimaryKey {
    PrimaryKey(row.to_be_bytes().to_vec())
}

fn fingerprint(row: u64) -> [u8; 16] {
    let mut fingerprint = [0_u8; 16];
    fingerprint[..8].copy_from_slice(&row.to_be_bytes());
    fingerprint[8..].copy_from_slice(&row.rotate_left(17).to_be_bytes());
    fingerprint
}

fn old_file(row: u64) -> FileId {
    FileId(format!(
        "s3://warehouse/flow_profile/events/data/unpartitioned/old-{:03}-000000.parquet",
        row % FILE_COUNT
    ))
}

fn new_file(row: u64) -> FileId {
    FileId(format!(
        "s3://warehouse/flow_profile/events/data/unpartitioned/new-{:03}-000000.parquet",
        row % FILE_COUNT
    ))
}

fn location(file: FileId, row: u64, data_sequence_number: i64) -> RowLocation {
    RowLocation {
        data_file_id: file,
        row_position: row / FILE_COUNT,
        data_sequence_number,
        spec_id: 0,
        partition: Vec::new(),
        source_commit_lsn: SOURCE_LSN,
        row_version: SOURCE_LSN.0,
        row_fingerprint: fingerprint(row),
    }
}

fn operation(id: &str, kind: OperationKind, base_snapshot_id: Option<i64>) -> PreparedOperation {
    PreparedOperation {
        id: OperationId(id.into()),
        table_id: TABLE,
        kind,
        base_snapshot_id,
        last_lsn: SOURCE_LSN,
        schema_version: 1,
        artifacts: Vec::new(),
        payload: Vec::new(),
    }
}

fn seed_deltas(rows: u64) -> impl Iterator<Item = IndexDelta> {
    (0..rows).map(|row| IndexDelta {
        key: key(row),
        expected: None,
        replacement: Some(location(old_file(row), row, -1)),
    })
}

fn rewrite_delta(row: u64) -> IndexDelta {
    IndexDelta {
        key: key(row),
        expected: Some(location(old_file(row), row, SEED_SEQUENCE)),
        replacement: (!row.is_multiple_of(10)).then(|| location(new_file(row), row, -1)),
    }
}

fn rewrite_deltas(rows: u64) -> impl Iterator<Item = IndexDelta> {
    (0..rows).map(rewrite_delta)
}

fn create_seed(root: &Path, rows: u64) -> StateStore {
    let store = open(&root.join("seed"));
    let seed = operation("seed", OperationKind::Ingest, None);
    store.prepare(seed.clone(), seed_deltas(rows)).unwrap();
    store
        .mark_committed(&seed.id, SEED_SNAPSHOT, SEED_SEQUENCE)
        .unwrap();
    let applied = store.apply_committed(&seed.id).unwrap();
    assert_eq!(applied.applied_rows, rows);
    store.forget_applied(&seed.id).unwrap();
    store
}

fn stage_ordinal_wal(
    store: &StateStore,
    id: &OperationId,
    deltas: impl IntoIterator<Item = IndexDelta>,
) -> Result<()> {
    let _guard = store.lock()?;
    let mut record = store.require_operation(id)?;
    assert_eq!(record.phase, OperationPhase::Building);
    let cf = store.0.db.cf_handle(DELTAS).expect("opened column family");
    let mut ordinal = delta_key(id, record.delta_count);
    let ordinal_offset = ordinal.len() - size_of::<u64>();
    let mut encoded = Vec::new();
    let mut deltas = deltas.into_iter().fuse();
    loop {
        let mut batch = StateBatch::default();
        let start = record.delta_count;
        for delta in deltas.by_ref().take(store.0.batch_rows) {
            ordinal[ordinal_offset..].copy_from_slice(&record.delta_count.to_be_bytes());
            encoded.clear();
            bincode::serialize_into(&mut encoded, &delta)?;
            batch.put_cf(&cf, &ordinal, &encoded);
            record.delta_count = record
                .delta_count
                .checked_add(1)
                .ok_or_else(|| Error::InvalidState("prepared delta count overflow".into()))?;
        }
        if record.delta_count == start {
            return Ok(());
        }
        store.put(&mut batch, OPERATIONS, id.0.as_bytes(), &record)?;
        store.write_staged(batch)?;
    }
}

fn build_external_sst(
    id: &OperationId,
    path: &Path,
    deltas: impl IntoIterator<Item = IndexDelta>,
) -> Result<ExternalArtifact> {
    let build_started = Instant::now();
    let mut options = Options::default();
    options.set_compression_type(DBCompressionType::Lz4);
    let mut writer = SstFileWriter::create(&options);
    writer.open(path)?;
    let mut ordinal = delta_key(id, 0);
    let ordinal_offset = ordinal.len() - size_of::<u64>();
    let mut encoded = Vec::new();
    let mut count = 0_u64;
    for delta in deltas {
        ordinal[ordinal_offset..].copy_from_slice(&count.to_be_bytes());
        encoded.clear();
        bincode::serialize_into(&mut encoded, &delta)?;
        writer.put(&ordinal, &encoded)?;
        count = count
            .checked_add(1)
            .ok_or_else(|| Error::InvalidState("prepared delta count overflow".into()))?;
    }
    assert_ne!(count, 0);
    writer.finish()?;
    let bytes = std::fs::metadata(path)?.len();
    let build = build_started.elapsed();
    Ok(ExternalArtifact {
        count,
        bytes,
        build,
    })
}

fn ingest_external_sst(store: &StateStore, id: &OperationId, path: &Path) -> Result<Duration> {
    let ingest_started = Instant::now();
    let _guard = store.lock()?;
    let record = store.require_operation(id)?;
    assert_eq!(record.phase, OperationPhase::Building);
    assert_eq!(record.delta_count, 0);
    let cf = store.0.db.cf_handle(DELTAS).expect("opened column family");
    let mut ingest = IngestExternalFileOptions::default();
    ingest.set_move_files(true);
    ingest.set_snapshot_consistency(true);
    ingest.set_allow_global_seqno(true);
    ingest.set_allow_blocking_flush(true);
    store
        .0
        .db
        .ingest_external_file_cf_opts(&cf, &ingest, vec![path])?;
    Ok(ingest_started.elapsed())
}

fn seal_external(store: &StateStore, id: &OperationId, count: u64) -> Result<()> {
    let _guard = store.lock()?;
    let mut record = store.require_operation(id)?;
    assert_eq!(record.phase, OperationPhase::Building);
    assert_eq!(record.delta_count, 0);
    assert_ne!(count, 0);
    let cf = store.0.db.cf_handle(DELTAS).expect("opened column family");
    assert!(store.0.db.get_cf(&cf, delta_key(id, 0))?.is_some());
    assert!(store.0.db.get_cf(&cf, delta_key(id, count - 1))?.is_some());
    assert!(store.0.db.get_cf(&cf, delta_key(id, count))?.is_none());
    record.delta_count = count;
    record.phase = OperationPhase::Prepared;
    let mut batch = StateBatch::default();
    store.put(&mut batch, OPERATIONS, id.0.as_bytes(), &record)?;
    store.write(batch)
}

fn run_profile(root: &Path, strategy: Staging, rows: u64) -> Observation {
    let database = root.join(strategy.name());
    let rewrite = operation("rewrite", OperationKind::Rewrite, Some(SEED_SNAPSHOT));
    let sst = root.join("rewrite-deltas.sst");
    let external = matches!(strategy, Staging::ExternalSst)
        .then(|| build_external_sst(&rewrite.id, &sst, rewrite_deltas(rows)).unwrap());

    let store = open(&database);
    let activation_started = Instant::now();
    store.begin_prepare(rewrite.clone()).unwrap();
    let payload_started = Instant::now();
    let mut sst_ingest = Duration::ZERO;
    match strategy {
        Staging::Current => store
            .stage_deltas(&rewrite.id, rewrite_deltas(rows))
            .unwrap(),
        Staging::OrdinalWal => {
            stage_ordinal_wal(&store, &rewrite.id, rewrite_deltas(rows)).unwrap()
        }
        Staging::ExternalSst => {
            sst_ingest = ingest_external_sst(&store, &rewrite.id, &sst).unwrap();
        }
    }
    let payload_install = payload_started.elapsed();
    let seal_started = Instant::now();
    if let Some(external) = &external {
        seal_external(&store, &rewrite.id, external.count).unwrap();
    } else {
        store
            .seal_prepare(&rewrite.id, Vec::new(), Vec::new())
            .unwrap();
    }
    let seal = seal_started.elapsed();
    let activation = activation_started.elapsed();
    store
        .mark_committed(&rewrite.id, REWRITE_SNAPSHOT, REWRITE_SEQUENCE)
        .unwrap();
    let apply_started = Instant::now();
    let result = store.apply_committed(&rewrite.id).unwrap();
    let apply = apply_started.elapsed();
    assert_eq!(result.applied_rows, rows);
    drop(store);

    verify_reopened(&database, &rewrite.id, rows);
    Observation {
        activation,
        payload_install,
        seal,
        apply,
        sst_build: external
            .as_ref()
            .map_or(Duration::ZERO, |external| external.build),
        sst_ingest,
        sst_bytes: external.as_ref().map_or(0, |external| external.bytes),
    }
}

fn verify_reopened(path: &Path, id: &OperationId, rows: u64) {
    let store = open(path);
    assert_eq!(
        store.table_state(&TABLE).unwrap(),
        TableState {
            snapshot_id: Some(REWRITE_SNAPSHOT),
            materialized_lsn: SOURCE_LSN,
            schema_version: 1,
            pending_operation: None,
        }
    );
    let record = store.operation(id).unwrap().unwrap();
    assert_eq!(record.phase, OperationPhase::Applied);
    assert_eq!(record.delta_count, rows);
    assert_eq!(record.applied_count, rows);

    let mut staged = store.prepared_deltas(id).unwrap();
    for expected in rewrite_deltas(rows) {
        assert_eq!(staged.next().unwrap().unwrap(), expected);
    }
    assert!(staged.next().is_none());

    const LOOKUP_ROWS: u64 = 4096;
    let mut start = 0;
    while start < rows {
        let end = rows.min(start + LOOKUP_ROWS);
        let keys: Vec<_> = (start..end).map(key).collect();
        let actual = store.lookup_many(&TABLE, &keys).unwrap();
        for (row, actual) in (start..end).zip(actual) {
            let expected =
                (!row.is_multiple_of(10)).then(|| location(new_file(row), row, REWRITE_SEQUENCE));
            assert_eq!(actual, expected);
        }
        start = end;
    }

    let mut expected_counts = vec![0_u64; FILE_COUNT as usize];
    for row in 0..rows {
        if !row.is_multiple_of(10) {
            expected_counts[(row % FILE_COUNT) as usize] += 1;
        }
    }
    let old_files = (0..FILE_COUNT).map(old_file).collect::<Vec<_>>();
    let new_files = (0..FILE_COUNT).map(new_file).collect::<Vec<_>>();
    let files = old_files
        .iter()
        .chain(&new_files)
        .cloned()
        .collect::<Vec<_>>();
    let counts = store
        .file_live_row_counts(&TABLE, Some(REWRITE_SNAPSHOT), &files)
        .unwrap();
    assert!(
        counts[..FILE_COUNT as usize]
            .iter()
            .all(|count| *count == 0)
    );
    assert_eq!(&counts[FILE_COUNT as usize..], expected_counts.as_slice());
    for file in &old_files {
        assert!(store.file_rows(&TABLE, file).next().is_none());
    }
    for file_index in 0..FILE_COUNT {
        let mut actual = store.file_rows(&TABLE, &new_files[file_index as usize]);
        for row in (file_index..rows)
            .step_by(FILE_COUNT as usize)
            .filter(|row| !row.is_multiple_of(10))
        {
            assert_eq!(
                actual.next().unwrap().unwrap(),
                (row / FILE_COUNT, key(row))
            );
        }
        assert!(actual.next().is_none());
    }
}

fn strategy_order(round: usize) -> [Staging; 3] {
    match round % 3 {
        0 => [Staging::Current, Staging::OrdinalWal, Staging::ExternalSst],
        1 => [Staging::ExternalSst, Staging::Current, Staging::OrdinalWal],
        _ => [Staging::OrdinalWal, Staging::ExternalSst, Staging::Current],
    }
}

fn duration_stats(
    observations: &[Observation],
    value: impl Fn(&Observation) -> Duration,
) -> (f64, f64, f64) {
    let mut values = observations
        .iter()
        .map(|observation| value(observation).as_secs_f64() * 1_000.0)
        .collect::<Vec<_>>();
    values.sort_by(f64::total_cmp);
    let median = values[values.len() / 2];
    (median, values[0], values[values.len() - 1])
}

#[test]
#[ignore = "manual profile; set FLOW_STATE_PROFILE_ROWS and FLOW_STATE_PROFILE_ROUNDS"]
fn compares_current_ordinal_wal_and_external_sst_staging() {
    let rows = configured_rows();
    let rounds = configured_rounds();
    let mut observations: [Vec<Observation>; 3] = std::array::from_fn(|_| Vec::new());
    for round in 0..rounds {
        let root = TempDir::new().unwrap();
        let seed_started = Instant::now();
        let seed = create_seed(root.path(), rows);
        for strategy in [Staging::Current, Staging::OrdinalWal, Staging::ExternalSst] {
            seed.checkpoint(root.path().join(strategy.name())).unwrap();
        }
        let seed_elapsed = seed_started.elapsed();
        drop(seed);
        eprintln!(
            "state_store_staging_profile kind=seed round={} rows={} elapsed_ms={:.3}",
            round + 1,
            rows,
            seed_elapsed.as_secs_f64() * 1_000.0
        );
        for strategy in strategy_order(round) {
            let observation = run_profile(root.path(), strategy, rows);
            eprintln!(
                "state_store_staging_profile kind=sample round={} strategy={} assumption={} rows={} activation_ms={:.3} payload_install_ms={:.3} seal_ms={:.3} sst_build_ms={:.3} sst_ingest_ms={:.3} apply_ms={:.3} total_ms={:.3} sst_bytes={}",
                round + 1,
                strategy.name(),
                strategy.assumption(),
                rows,
                observation.activation.as_secs_f64() * 1_000.0,
                observation.payload_install.as_secs_f64() * 1_000.0,
                observation.seal.as_secs_f64() * 1_000.0,
                observation.sst_build.as_secs_f64() * 1_000.0,
                observation.sst_ingest.as_secs_f64() * 1_000.0,
                observation.apply.as_secs_f64() * 1_000.0,
                (observation.sst_build + observation.activation + observation.apply).as_secs_f64()
                    * 1_000.0,
                observation.sst_bytes,
            );
            observations[strategy.index()].push(observation);
        }
    }
    for strategy in [Staging::Current, Staging::OrdinalWal, Staging::ExternalSst] {
        let samples = &observations[strategy.index()];
        let activation = duration_stats(samples, |sample| sample.activation);
        let apply = duration_stats(samples, |sample| sample.apply);
        let total = duration_stats(samples, |sample| {
            sample.sst_build + sample.activation + sample.apply
        });
        eprintln!(
            "state_store_staging_profile kind=summary strategy={} assumption={} rows={} samples={} activation_median_ms={:.3} activation_min_ms={:.3} activation_max_ms={:.3} apply_median_ms={:.3} apply_min_ms={:.3} apply_max_ms={:.3} total_median_ms={:.3} total_min_ms={:.3} total_max_ms={:.3}",
            strategy.name(),
            strategy.assumption(),
            rows,
            samples.len(),
            activation.0,
            activation.1,
            activation.2,
            apply.0,
            apply.1,
            apply.2,
            total.0,
            total.1,
            total.2,
        );
    }
}

#[test]
#[ignore = "manual external-SST reopen boundary"]
fn external_sst_building_boundary_discards_ingested_ordinals_after_reopen() {
    let rows = configured_rows();
    let root = TempDir::new().unwrap();
    let seed = create_seed(root.path(), rows);
    let database = root.path().join("crash-boundary");
    seed.checkpoint(&database).unwrap();
    drop(seed);

    let id = OperationId("crash-boundary".into());
    {
        let store = open(&database);
        store
            .begin_prepare(operation(
                &id.0,
                OperationKind::Rewrite,
                Some(SEED_SNAPSHOT),
            ))
            .unwrap();
        let sst = root.path().join("crash-boundary.sst");
        let installed = rows.min(4096);
        let external = build_external_sst(&id, &sst, rewrite_deltas(installed)).unwrap();
        ingest_external_sst(&store, &id, &sst).unwrap();
        assert_eq!(external.count, installed);
        assert_eq!(store.operation(&id).unwrap().unwrap().delta_count, 0);
    }

    let store = open(&database);
    let building = store.operation(&id).unwrap().unwrap();
    assert_eq!(building.phase, OperationPhase::Building);
    assert_eq!(building.delta_count, 0);
    store.discard_uncommitted(&id).unwrap();
    assert!(store.operation(&id).unwrap().is_none());
    assert_eq!(
        store.table_state(&TABLE).unwrap(),
        TableState {
            snapshot_id: Some(SEED_SNAPSHOT),
            materialized_lsn: SOURCE_LSN,
            schema_version: 1,
            pending_operation: None,
        }
    );

    let retry = operation(&id.0, OperationKind::Rewrite, Some(SEED_SNAPSHOT));
    store.begin_prepare(retry.clone()).unwrap();
    store.stage_deltas(&id, [rewrite_delta(1)]).unwrap();
    store.seal_prepare(&id, Vec::new(), Vec::new()).unwrap();
    assert_eq!(store.prepared_deltas(&id).unwrap().count(), 1);
    store
        .mark_committed(&id, REWRITE_SNAPSHOT, REWRITE_SEQUENCE)
        .unwrap();
    assert_eq!(store.apply_committed(&id).unwrap().applied_rows, 1);
}
