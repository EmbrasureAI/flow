//! A fold that spills mid-epoch must produce exactly what an in-memory or an
//! all-disk collapse of the same changes produces.
use flow_model::{FileId, OperationId, PgLsn, PrimaryKey, RowLocation, TableId, Value};
use flow_state_store::{
    Bound, BufferResult, Change, CollapseBuffer, CollapsedMutation, IndexDelta, OperationKind,
    PreparedOperation, StateStore, StateStoreOptions,
};
use proptest::prelude::*;
use tempfile::TempDir;

const TABLE: TableId = TableId(7);
const CHUNK: usize = 8;

fn open(path: &std::path::Path) -> StateStore {
    StateStore::open(
        path,
        StateStoreOptions {
            apply_batch_rows: CHUNK,
            ..Default::default()
        },
    )
    .unwrap()
}

fn location(key: u8) -> RowLocation {
    RowLocation {
        data_file_id: FileId(format!("s3://bucket/data/{}.parquet", key % 3)),
        row_position: u64::from(key),
        data_sequence_number: 1,
        spec_id: 0,
        partition: vec![],
        source_commit_lsn: PgLsn(10),
        row_version: 1,
        row_fingerprint: [key; 16],
    }
}

/// Index `present` keys, committed at snapshot 1.
fn seeded(path: &std::path::Path, present: &[u8]) -> StateStore {
    let store = open(path);
    store
        .prepare(
            PreparedOperation {
                id: OperationId("seed".into()),
                table_id: TABLE,
                kind: OperationKind::Ingest,
                base_snapshot_id: None,
                last_lsn: PgLsn(10),
                schema_version: 1,
                artifacts: vec![],
                payload: vec![],
            },
            present.iter().map(|key| IndexDelta {
                key: PrimaryKey(vec![*key]),
                expected: None,
                replacement: Some(location(*key)),
            }),
        )
        .unwrap();
    store
        .mark_committed(&OperationId("seed".into()), 1, 1)
        .unwrap();
    store.apply_committed(&OperationId("seed".into())).unwrap();
    store
}

#[derive(Debug, Clone)]
enum Step {
    Write(u8, i64, bool),
    Delete(u8),
}

/// A valid change sequence: inserts only for absent keys, updates and deletes
/// only for present ones. A partial update omits the text column.
fn changes(present: &[u8], steps: &[Step]) -> Vec<(TableId, PrimaryKey, Change)> {
    let mut live: std::collections::BTreeSet<u8> = present.iter().copied().collect();
    let mut out = Vec::new();
    for step in steps {
        match *step {
            Step::Write(key, version, partial) => {
                let change = if live.insert(key) {
                    Change::Insert(vec![
                        Value::Int64(version),
                        Value::String(format!("v{version}")),
                    ])
                } else if partial {
                    Change::Update(vec![Value::Int64(version), Value::Unchanged])
                } else {
                    Change::Update(vec![
                        Value::Int64(version),
                        Value::String(format!("v{version}")),
                    ])
                };
                out.push((TABLE, PrimaryKey(vec![key]), change));
            }
            Step::Delete(key) => {
                if live.remove(&key) {
                    out.push((TABLE, PrimaryKey(vec![key]), Change::Delete));
                }
            }
        }
    }
    out
}

fn disk(
    store: &StateStore,
    transaction: &str,
    input: &[(TableId, PrimaryKey, Change)],
) -> Vec<CollapsedMutation> {
    for chunk in input.chunks(CHUNK) {
        store
            .collapse_changes(transaction, chunk.iter().cloned())
            .unwrap();
    }
    store.seal_transaction(transaction).unwrap();
    store
        .collapsed(transaction, &TABLE)
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
}

/// The coordinator's memory path: fold until over budget, then spill the fold
/// and continue on disk with the rest of the current chunk.
fn budgeted(
    store: &StateStore,
    transaction: &str,
    input: &[(TableId, PrimaryKey, Change)],
    limit: usize,
) -> (Vec<CollapsedMutation>, bool) {
    let expected = store.table_state(&TABLE).unwrap();
    let mut buffer = Some(CollapseBuffer::new(TABLE, limit));
    for chunk in input.chunks(CHUNK) {
        let Some(fold) = buffer.as_mut() else {
            store
                .collapse_changes(transaction, chunk.iter().cloned())
                .unwrap();
            continue;
        };
        let mut rest = chunk.iter().cloned();
        while let Some((_, key, change)) = rest.next() {
            if let BufferResult::CapacityExceeded = fold.push(key, change).unwrap() {
                store
                    .spill_collapse_buffer(transaction, buffer.take().unwrap())
                    .unwrap();
                store.collapse_changes(transaction, rest).unwrap();
                break;
            }
        }
    }
    let spilled = match buffer {
        Some(buffer) => match store.bind_collapse_buffer(buffer, &expected).unwrap() {
            Bound::Ready(rows) => return (rows.collect(), false),
            Bound::Overflow(buffer) => {
                store.spill_collapse_buffer(transaction, buffer).unwrap();
                true
            }
        },
        None => true,
    };
    store.seal_transaction(transaction).unwrap();
    let rows = store
        .collapsed(transaction, &TABLE)
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    (rows, spilled)
}

fn step() -> impl Strategy<Value = Step> {
    prop_oneof![
        3 => (0u8..24, 0i64..1000, any::<bool>()).prop_map(|(k, v, p)| Step::Write(k, v, p)),
        1 => (0u8..24).prop_map(Step::Delete),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(96))]
    #[test]
    fn spilling_matches_memory_and_disk(
        present in proptest::collection::btree_set(0u8..24, 0..24),
        steps in proptest::collection::vec(step(), 1..120),
        limit in 4_096usize..24_576,
    ) {
        let present: Vec<u8> = present.into_iter().collect();
        let input = changes(&present, &steps);
        prop_assume!(!input.is_empty());
        let temp = TempDir::new().unwrap();
        let store = seeded(temp.path(), &present);
        let (memory, memory_spilled) = budgeted(&store, "memory", &input, 64 << 20);
        prop_assert!(!memory_spilled);
        let (budget, _) = budgeted(&store, "budget", &input, limit);
        let on_disk = disk(&store, "disk", &input);
        prop_assert_eq!(&memory, &on_disk);
        prop_assert_eq!(&budget, &on_disk);
    }
}

#[test]
fn a_tiny_budget_spills_every_epoch_and_still_matches_disk() {
    let present = [1, 2, 3, 4, 5, 6];
    let steps: Vec<_> = (0..60u8)
        .map(|i| {
            if i % 7 == 0 {
                Step::Delete(i % 9)
            } else {
                Step::Write(i % 9, i64::from(i), i % 3 == 0)
            }
        })
        .collect();
    let input = changes(&present, &steps);
    let temp = TempDir::new().unwrap();
    let store = seeded(temp.path(), &present);
    let (budget, spilled) = budgeted(&store, "budget", &input, 4_096);
    assert!(spilled);
    assert_eq!(budget, disk(&store, "disk", &input));
}

#[test]
fn spilling_into_a_nonempty_spool_is_rejected() {
    let temp = TempDir::new().unwrap();
    let store = seeded(temp.path(), &[1]);
    store
        .collapse_changes(
            "t",
            [(
                TABLE,
                PrimaryKey(vec![2]),
                Change::Insert(vec![Value::Int64(1), Value::Null]),
            )],
        )
        .unwrap();
    let mut buffer = CollapseBuffer::new(TABLE, 64 << 20);
    buffer
        .push(
            PrimaryKey(vec![3]),
            Change::Insert(vec![Value::Int64(1), Value::Null]),
        )
        .unwrap();
    assert!(store.spill_collapse_buffer("t", buffer).is_err());
}

#[test]
fn an_ephemeral_store_keeps_its_data_across_a_clean_reopen() {
    let temp = TempDir::new().unwrap();
    let options = || StateStoreOptions {
        apply_batch_rows: CHUNK,
        ..StateStoreOptions::scratch()
    };
    assert!(options().ephemeral);
    let store = StateStore::open(temp.path(), options()).unwrap();
    let input = changes(&[], &[Step::Write(1, 1, false), Step::Write(2, 2, false)]);
    store.collapse_changes("t", input.iter().cloned()).unwrap();
    store.seal_transaction("t").unwrap();
    let before: Vec<_> = store
        .collapsed("t", &TABLE)
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    drop(store);
    // No write-ahead log: a clean close flushes the memtable instead.
    let store = StateStore::open(temp.path(), options()).unwrap();
    let after: Vec<_> = store
        .collapsed("t", &TABLE)
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(before.len(), 2);
    assert_eq!(before, after);
}
