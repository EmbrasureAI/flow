use flow_model::{PgLsn, PrimaryKey, TableId};
use flow_state_store::{Change, ControlStore, StateStore, StateStoreOptions};
use std::{sync::mpsc, thread, time::Duration};

#[test]
fn ledger_progress_does_not_wait_for_row_work_and_reopens_with_matching_authority() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("index");
    let control = ControlStore::open(root.path().join("control")).unwrap();
    let index = control
        .initialize_index(&path, StateStoreOptions::default())
        .unwrap();
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let row_index = index.clone();
    let rows = thread::spawn(move || {
        row_index
            .collapse_changes(
                "held-row-work",
                std::iter::once_with(|| {
                    // collapse_changes holds its row mutex before requesting input.
                    entered_tx.send(()).unwrap();
                    release_rx.recv().unwrap();
                    (TableId(1), PrimaryKey(vec![1]), Change::Insert(vec![]))
                }),
            )
            .unwrap();
        row_index.seal_transaction("held-row-work").unwrap();
        for lsn in 1..=32 {
            row_index.complete_noop(&TableId(1), PgLsn(lsn), 1).unwrap();
        }
    });
    entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    let (persisted_tx, persisted_rx) = mpsc::channel();
    let ledger_index = index.clone();
    let ledger = thread::spawn(move || {
        ledger_index
            .update_source_ledger(
                (b"ledger", b"0"),
                [(b"entry".as_slice(), b"value".as_slice())],
                None,
            )
            .unwrap();
        persisted_tx.send(()).unwrap();
        for lsn in 1u64..=32 {
            ledger_index
                .update_source_ledger((b"ledger", &lsn.to_be_bytes()), [], None)
                .unwrap();
        }
    });
    let persisted_while_rows_held = persisted_rx.recv_timeout(Duration::from_secs(5));
    // Release and join even on failure, so the old locking path fails cleanly.
    release_tx.send(()).unwrap();
    rows.join().unwrap();
    ledger.join().unwrap();
    assert!(
        persisted_while_rows_held.is_ok(),
        "ledger waited for held row work"
    );
    let checkpoint = control
        .checkpoint(&index, root.path().join("checkpoint"))
        .unwrap();
    assert_eq!(
        checkpoint.revision,
        control.active_generation().unwrap().unwrap().revision
    );
    drop(index);
    drop(control);
    let control = ControlStore::open(root.path().join("control")).unwrap();
    let index = StateStore::open_with_control(&path, StateStoreOptions::default(), control.clone())
        .unwrap();
    assert_eq!(
        index.source_transaction(b"ledger").unwrap(),
        Some(32u64.to_be_bytes().to_vec())
    );
    assert_eq!(
        index.source_transaction(b"entry").unwrap(),
        Some(b"value".to_vec())
    );
    assert_eq!(
        index.table_state(&TableId(1)).unwrap().materialized_lsn,
        PgLsn(32)
    );
    assert_eq!(
        index
            .collapsed("held-row-work", &TableId(1))
            .unwrap()
            .count(),
        1
    );
    assert_eq!(
        checkpoint.revision,
        control.active_generation().unwrap().unwrap().revision
    );
}
