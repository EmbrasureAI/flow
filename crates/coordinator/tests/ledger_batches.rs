use flow_coordinator::{AckMode, JournalDurability, SourceLedger};
use flow_model::{PgLsn, SourceId, SourceTransaction, TableId};
use flow_state_store::{ControlStore, StateStoreOptions};
use std::time::Instant;
use tempfile::TempDir;

fn transaction(source: &SourceId, xid: u32) -> SourceTransaction {
    SourceTransaction {
        source_id: source.clone(),
        xid,
        begin_lsn: PgLsn(u64::from(xid) * 10 - 1),
        commit_lsn: PgLsn(u64::from(xid) * 10),
        end_lsn: PgLsn(u64::from(xid) * 10 + 1),
        commit_timestamp_micros: 0,
        schema_versions: vec![],
        affected_tables: vec![TableId(1), TableId(2)],
        mutation_chunks: Default::default(),
        table_mutation_counts: None,
    }
}

#[test]
#[ignore = "manual local RocksDB durability microbenchmark; reports timings without a speed assertion"]
fn measure_durable_ledger_batches() {
    for batch in [1, 32, 256] {
        for sample in 0..3 {
            let directory = TempDir::new().unwrap();
            let control = ControlStore::open(directory.path().join("control")).unwrap();
            let store = control
                .initialize_index(directory.path().join("index"), StateStoreOptions::default())
                .unwrap();
            let source = SourceId("measurement".into());
            let mut ledger = SourceLedger::open(
                store,
                source.clone(),
                AckMode::Materialized,
                JournalDurability::LocalDisk,
            )
            .unwrap();
            let transactions: Vec<_> = (1..=2048).map(|xid| transaction(&source, xid)).collect();
            let start = Instant::now();
            for page in transactions.chunks(batch) {
                ledger.journaled_batch(page).unwrap();
            }
            let register = start.elapsed();
            let end_lsns: Vec<_> = transactions
                .iter()
                .map(|transaction| transaction.end_lsn)
                .collect();
            let start = Instant::now();
            for table in [TableId(1), TableId(2)] {
                for page in end_lsns.chunks(batch) {
                    ledger.table_materialized_batch(page, table, 100).unwrap();
                }
            }
            let complete = start.elapsed();
            assert_eq!(ledger.pending_count(), 0);
            assert_eq!(
                ledger.acknowledgement(),
                transactions.last().unwrap().end_lsn
            );
            println!(
                "{{\"sample\":{sample},\"batch\":{batch},\"transactions\":2048,\"register_ms\":{},\"complete_ms\":{}}}",
                register.as_secs_f64() * 1000.0,
                complete.as_secs_f64() * 1000.0
            );
        }
    }
}

#[test]
fn atomic_pages_preserve_replay_and_contiguous_ack_across_control_reopen() {
    use flow_state_store::StateStore;
    let directory = TempDir::new().unwrap();
    let options = StateStoreOptions {
        apply_batch_rows: 4,
        ..Default::default()
    };
    let control = ControlStore::open(directory.path().join("control")).unwrap();
    let store = control
        .initialize_index(directory.path().join("index"), options.clone())
        .unwrap();
    let source = SourceId("batched".into());
    let transactions: Vec<_> = (1..=5).map(|xid| transaction(&source, xid)).collect();
    let mut ledger = SourceLedger::open(
        store.clone(),
        source.clone(),
        AckMode::Materialized,
        JournalDurability::LocalDisk,
    )
    .unwrap();
    assert_eq!(ledger.batch_capacity(), 4);
    assert!(ledger.journaled_batch(&transactions).is_err());
    assert_eq!(ledger.pending_count(), 0);
    assert_eq!(ledger.journaled_batch(&transactions[..3]).unwrap(), 3);
    assert_eq!(ledger.journaled_batch(&transactions[..3]).unwrap(), 0);
    let mut conflict = transactions[1].clone();
    conflict.xid = 99;
    assert!(
        ledger
            .journaled_batch(&[transactions[3].clone(), conflict])
            .is_err()
    );
    assert_eq!(ledger.watermarks().journal_durable_lsn, PgLsn(31));
    assert_eq!(ledger.pending_count(), 3);
    assert!(
        ledger
            .table_materialized_batch(&[PgLsn(11), PgLsn(999)], TableId(1), 100)
            .is_err()
    );
    assert_eq!(
        ledger.pending_tables(PgLsn(11)).unwrap(),
        vec![TableId(1), TableId(2)]
    );
    for table in [TableId(1), TableId(2)] {
        ledger
            .table_materialized_batch(&[PgLsn(31), PgLsn(21), PgLsn(21)], table, 100)
            .unwrap();
    }
    assert_eq!(ledger.acknowledgement(), PgLsn(0));
    drop(ledger);
    drop(store);
    drop(control);

    let control = ControlStore::open(directory.path().join("control")).unwrap();
    let store =
        StateStore::open_with_control(directory.path().join("index"), options, control).unwrap();
    let mut ledger = SourceLedger::open(
        store.clone(),
        source,
        AckMode::Materialized,
        JournalDurability::LocalDisk,
    )
    .unwrap();
    assert_eq!(ledger.pending_count(), 3);
    assert_eq!(
        ledger
            .pending_transactions()
            .collect::<anyhow::Result<Vec<_>>>()
            .unwrap(),
        transactions[..3]
    );
    assert!(ledger.pending_tables(PgLsn(21)).unwrap().is_empty());
    ledger
        .table_materialized_batch(&[PgLsn(11)], TableId(1), 100)
        .unwrap();
    // A conflicting replay late in a page must not persist the earlier update.
    assert!(
        ledger
            .table_materialized_batch(&[PgLsn(11), PgLsn(21)], TableId(2), 999)
            .is_err()
    );
    assert_eq!(ledger.pending_tables(PgLsn(11)).unwrap(), vec![TableId(2)]);
    ledger
        .table_materialized_batch(&[PgLsn(11)], TableId(2), 100)
        .unwrap();
    assert_eq!(ledger.acknowledgement(), PgLsn(31));
    assert_eq!(ledger.pending_count(), 0);

    // A late journal registration can already be covered by durable table state.
    for table in [TableId(1), TableId(2)] {
        store.complete_noop(&table, PgLsn(41), 0).unwrap();
    }
    assert_eq!(ledger.journaled_batch(&transactions[3..]).unwrap(), 2);
    assert_eq!(ledger.acknowledgement(), PgLsn(41));
    assert_eq!(ledger.pending_count(), 1);
    for table in [TableId(1), TableId(2)] {
        ledger
            .table_materialized_batch(&[PgLsn(41), PgLsn(51)], table, 200)
            .unwrap();
    }
    assert_eq!(ledger.acknowledgement(), PgLsn(51));
    assert_eq!(ledger.pending_count(), 0);
}
