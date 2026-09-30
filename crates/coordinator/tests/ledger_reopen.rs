use flow_coordinator::{AckMode, JournalDurability, SourceLedger};
use flow_model::{PgLsn, SourceId, SourceTransaction, TableId};
use flow_state_store::{StateStore, StateStoreOptions};
use tempfile::TempDir;

#[test]
fn reopen_removes_records_left_after_a_durable_completion_watermark() {
    let temp = TempDir::new().unwrap();
    let store = StateStore::open(temp.path(), StateStoreOptions::default()).unwrap();
    let source = SourceId("source".into());
    let mut ledger = SourceLedger::open(
        store.clone(),
        source.clone(),
        AckMode::Materialized,
        JournalDurability::LocalDisk,
    )
    .unwrap();
    let txn = SourceTransaction {
        source_id: source.clone(),
        xid: 7,
        begin_lsn: PgLsn(1),
        commit_lsn: PgLsn(10),
        end_lsn: PgLsn(11),
        commit_timestamp_micros: 0,
        schema_versions: vec![],
        affected_tables: vec![TableId(1)],
        mutation_chunks: Default::default(),
        table_mutation_counts: None,
    };
    ledger.journaled(txn).unwrap();
    let (key, value) = store
        .source_transactions()
        .map(Result::unwrap)
        .find(|(key, _)| key.windows(5).any(|part| part == b"/txn/"))
        .unwrap();
    ledger
        .table_materialized(PgLsn(11), TableId(1), 100)
        .unwrap();
    assert_eq!(ledger.acknowledgement(), PgLsn(11));
    // Recreate exactly the stale record left by a crash after the meta sync.
    store.put_source_transaction(&key, &value).unwrap();
    // That older protocol also predates the table admission index. Migration
    // must not create a reference to a descriptor that reopen will reclaim.
    let index_keys = store
        .source_transactions()
        .map(Result::unwrap)
        .map(|(key, _)| key)
        .filter(|key| key.windows(13).any(|part| part == b"/table-index/"))
        .collect::<Vec<_>>();
    for key in index_keys {
        store.delete_source_transaction(&key).unwrap();
    }
    drop(ledger);
    drop(store);
    let store = StateStore::open(temp.path(), StateStoreOptions::default()).unwrap();
    let ledger = SourceLedger::open(
        store.clone(),
        source,
        AckMode::Materialized,
        JournalDurability::LocalDisk,
    )
    .unwrap();
    assert_eq!(ledger.acknowledgement(), PgLsn(11));
    assert!(store.source_transaction(&key).unwrap().is_none());
    assert!(
        ledger
            .pending_table_transactions_after(TableId(1), PgLsn(0))
            .next()
            .is_none()
    );
}

#[test]
fn partial_tables_and_later_completions_survive_reopen_before_prefix_reclamation() {
    let temp = TempDir::new().unwrap();
    let store = StateStore::open(temp.path(), StateStoreOptions::default()).unwrap();
    let source = SourceId("ordered".into());
    let mut ledger = SourceLedger::open(
        store.clone(),
        source.clone(),
        AckMode::Materialized,
        JournalDurability::LocalDisk,
    )
    .unwrap();
    let first = SourceTransaction {
        source_id: source.clone(),
        xid: 1,
        begin_lsn: PgLsn(1),
        commit_lsn: PgLsn(10),
        end_lsn: PgLsn(11),
        commit_timestamp_micros: 0,
        schema_versions: vec![],
        affected_tables: vec![TableId(1), TableId(2)],
        mutation_chunks: Default::default(),
        table_mutation_counts: None,
    };
    let later = SourceTransaction {
        xid: 2,
        begin_lsn: PgLsn(12),
        commit_lsn: PgLsn(20),
        end_lsn: PgLsn(21),
        affected_tables: vec![TableId(1)],
        ..first.clone()
    };
    let empty = SourceTransaction {
        xid: 3,
        begin_lsn: PgLsn(22),
        commit_lsn: PgLsn(30),
        end_lsn: PgLsn(31),
        affected_tables: vec![],
        ..first.clone()
    };
    // Reclamation must stay inside one source's key range.
    let other_source = SourceId("ordered-extra".into());
    let mut other = SourceLedger::open(
        store.clone(),
        other_source.clone(),
        AckMode::Materialized,
        JournalDurability::LocalDisk,
    )
    .unwrap();
    other
        .journaled(SourceTransaction {
            source_id: other_source,
            ..first.clone()
        })
        .unwrap();
    ledger.journaled(first).unwrap();
    ledger.journaled(later.clone()).unwrap();
    ledger.journaled(empty).unwrap();
    ledger
        .table_materialized(PgLsn(21), TableId(1), 200)
        .unwrap();
    ledger
        .table_materialized(PgLsn(11), TableId(1), 100)
        .unwrap();
    assert_eq!(ledger.acknowledgement(), PgLsn(0));
    drop(other);
    drop(ledger);
    drop(store);

    let store = StateStore::open(temp.path(), StateStoreOptions::default()).unwrap();
    let mut ledger = SourceLedger::open(
        store.clone(),
        source,
        AckMode::Materialized,
        JournalDurability::LocalDisk,
    )
    .unwrap();
    assert_eq!(ledger.watermarks().journal_durable_lsn, PgLsn(31));
    assert_eq!(ledger.acknowledgement(), PgLsn(0));
    assert!(!ledger.journaled(later).unwrap());
    ledger
        .table_materialized(PgLsn(11), TableId(1), 100)
        .unwrap();
    assert!(
        ledger
            .table_materialized(PgLsn(11), TableId(1), 999)
            .is_err()
    );
    ledger
        .table_materialized(PgLsn(11), TableId(2), 101)
        .unwrap();
    assert_eq!(ledger.acknowledgement(), PgLsn(31));
    assert_eq!(ledger.pending_transactions().count(), 0);
    let remaining = store
        .source_transactions()
        .map(Result::unwrap)
        .filter(|(key, _)| key.windows(5).any(|part| part == b"/txn/"))
        .count();
    assert_eq!(remaining, 1, "another source's transaction remains intact");
}

#[test]
fn legacy_ledger_migrates_to_bounded_cursor_pages_in_separate_control_storage() {
    use flow_state_store::ControlStore;
    use std::collections::BTreeMap;
    let temp = TempDir::new().unwrap();
    let control = ControlStore::open(temp.path().join("control")).unwrap();
    let store = control
        .initialize_index(temp.path().join("index"), StateStoreOptions::default())
        .unwrap();
    let source = SourceId("cursor".into());
    let legacy = flow_ingress_journal::legacy::SourceTransaction {
        source_id: source.clone(),
        xid: 1,
        begin_lsn: PgLsn(1),
        commit_lsn: PgLsn(10),
        end_lsn: PgLsn(11),
        commit_timestamp_micros: 0,
        schema_versions: vec![],
        affected_tables: vec![TableId(1), TableId(2)],
        mutation_chunks: vec![flow_model::JournalChunkRef {
            segment: 3,
            offset: 28,
            length: 64,
        }],
    };
    let mut key = b"flow-ledger/v1/".to_vec();
    key.extend((source.0.len() as u64).to_be_bytes());
    key.extend(source.0.as_bytes());
    key.extend(b"/txn/");
    key.extend(11u64.to_be_bytes());
    store
        .put_source_transaction(
            &key,
            &bincode::serialize(&(legacy, BTreeMap::<TableId, i64>::new())).unwrap(),
        )
        .unwrap();
    let mut ledger = SourceLedger::open(
        store.clone(),
        source.clone(),
        AckMode::Materialized,
        JournalDurability::LocalDisk,
    )
    .unwrap();
    assert_eq!(ledger.pending_count(), 1);
    ledger
        .table_materialized(PgLsn(11), TableId(1), 100)
        .unwrap();
    assert!(
        store
            .source_transaction(&key)
            .unwrap()
            .unwrap()
            .starts_with(b"FLLEDG03")
    );
    let template = ledger.pending_transactions().next().unwrap().unwrap();
    assert_eq!(template.mutation_chunks.len(), 1);
    assert!(template.table_mutation_counts.is_none());
    for xid in 2..=513 {
        ledger
            .journaled(SourceTransaction {
                xid,
                begin_lsn: PgLsn(u64::from(xid) * 10 - 1),
                commit_lsn: PgLsn(u64::from(xid) * 10),
                end_lsn: PgLsn(u64::from(xid) * 10 + 1),
                mutation_chunks: Default::default(),
                table_mutation_counts: (xid != 2).then(|| {
                    vec![
                        flow_model::TableMutationCount {
                            table_id: TableId(1),
                            mutations: 2,
                        },
                        flow_model::TableMutationCount {
                            table_id: TableId(2),
                            mutations: 1,
                        },
                    ]
                }),
                ..template.clone()
            })
            .unwrap();
    }
    // Recreate a bounded, count-free FLLEDG02 entry alongside the original
    // unprefixed history and current counted records, then close both databases.
    let bounded = flow_ingress_journal::legacy::BoundedSourceTransaction {
        source_id: source.clone(),
        xid: 2,
        begin_lsn: PgLsn(19),
        commit_lsn: PgLsn(20),
        end_lsn: PgLsn(21),
        commit_timestamp_micros: 0,
        schema_versions: vec![],
        affected_tables: vec![TableId(1), TableId(2)],
        mutation_chunks: Default::default(),
    };
    let mut old_bytes = b"FLLEDG02".to_vec();
    bincode::serialize_into(&mut old_bytes, &(bounded, BTreeMap::<TableId, i64>::new())).unwrap();
    let mut bounded_key = key.clone();
    let offset = bounded_key.len() - 8;
    bounded_key[offset..].copy_from_slice(&21u64.to_be_bytes());
    store
        .put_source_transaction(&bounded_key, &old_bytes)
        .unwrap();
    // A nearby source prefix must never appear in this source's cursor pages.
    let mut other = SourceLedger::open(
        store.clone(),
        SourceId("cursor-extra".into()),
        AckMode::Materialized,
        JournalDurability::LocalDisk,
    )
    .unwrap();
    other
        .journaled(SourceTransaction {
            source_id: SourceId("cursor-extra".into()),
            ..template
        })
        .unwrap();
    drop(other);
    drop(ledger);
    drop(store);
    drop(control);
    let control = ControlStore::open(temp.path().join("control")).unwrap();
    let store = StateStore::open_with_control(
        temp.path().join("index"),
        StateStoreOptions::default(),
        control,
    )
    .unwrap();
    let mut ledger = SourceLedger::open(
        store.clone(),
        source.clone(),
        AckMode::Materialized,
        JournalDurability::LocalDisk,
    )
    .unwrap();
    assert_eq!(ledger.pending_count(), 513);
    let mut after = PgLsn(0);
    let mut seen = 0;
    loop {
        let page = ledger
            .pending_transactions_after(after)
            .take(17)
            .collect::<anyhow::Result<Vec<_>>>()
            .unwrap();
        if page.is_empty() {
            break;
        }
        for txn in page {
            seen += 1;
            assert_eq!(txn.xid, seen);
            if txn.xid <= 2 {
                assert!(txn.table_mutation_counts.is_none());
            } else {
                assert_eq!(txn.mutation_count(TableId(1)), Some(2));
                assert_eq!(txn.mutation_count(TableId(2)), Some(1));
            }
            assert!(txn.end_lsn > after);
            after = txn.end_lsn;
            ledger
                .table_materialized(txn.end_lsn, TableId(1), 100)
                .unwrap();
            ledger
                .table_materialized(txn.end_lsn, TableId(2), 101)
                .unwrap();
        }
    }
    assert_eq!(seen, 513);
    assert_eq!(ledger.pending_count(), 0);
    assert_eq!(ledger.acknowledgement(), PgLsn(5131));
    assert!(ledger.pending_transactions().next().is_none());
    store.complete_noop(&TableId(9), PgLsn(6001), 0).unwrap();
    ledger
        .journaled(SourceTransaction {
            source_id: source,
            xid: 600,
            begin_lsn: PgLsn(5999),
            commit_lsn: PgLsn(6000),
            end_lsn: PgLsn(6001),
            commit_timestamp_micros: 0,
            schema_versions: vec![],
            affected_tables: vec![TableId(9)],
            mutation_chunks: Default::default(),
            table_mutation_counts: None,
        })
        .unwrap();
    assert_eq!(ledger.pending_count(), 0);
    assert_eq!(ledger.acknowledgement(), PgLsn(6001));
}
