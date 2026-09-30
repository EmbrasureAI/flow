//! Journaled acknowledgement: PostgreSQL may release WAL once a transaction is
//! durable in the local journal, so the ledger must keep every descriptor that
//! is not yet materialized and never let the journal-reclaim frontier pass it.
use flow_coordinator::{AckMode, JournalDurability, SourceLedger};
use flow_model::{PgLsn, SourceId, SourceTransaction, TableId};
use flow_state_store::{ControlStore, StateStore, StateStoreOptions};
use tempfile::TempDir;

fn transaction(source: &SourceId, xid: u32, tables: &[u32]) -> SourceTransaction {
    SourceTransaction {
        source_id: source.clone(),
        xid,
        begin_lsn: PgLsn(u64::from(xid) * 10 - 1),
        commit_lsn: PgLsn(u64::from(xid) * 10),
        end_lsn: PgLsn(u64::from(xid) * 10 + 1),
        commit_timestamp_micros: 0,
        schema_versions: vec![],
        affected_tables: tables.iter().copied().map(TableId).collect(),
        mutation_chunks: Default::default(),
        table_mutation_counts: None,
    }
}

fn journaled(store: &StateStore, source: &SourceId) -> SourceLedger {
    SourceLedger::open(
        store.clone(),
        source.clone(),
        AckMode::Journaled,
        JournalDurability::IndependentStorage,
    )
    .unwrap()
}

fn pending(ledger: &SourceLedger) -> Vec<u32> {
    ledger
        .pending_transactions()
        .map(|transaction| transaction.unwrap().xid)
        .collect()
}

fn pending_for(ledger: &SourceLedger, table: u32) -> Vec<u32> {
    ledger
        .pending_table_transactions_after(TableId(table), PgLsn(0))
        .map(|transaction| transaction.unwrap().xid)
        .collect()
}

#[test]
fn journaled_ack_requires_an_independent_storage_declaration() {
    let directory = TempDir::new().unwrap();
    let store = StateStore::open(directory.path(), StateStoreOptions::default()).unwrap();
    let error = SourceLedger::open(
        store,
        SourceId("local".into()),
        AckMode::Journaled,
        JournalDurability::LocalDisk,
    )
    .err()
    .expect("journaled ACK on a local disk must be rejected");
    assert!(
        error
            .to_string()
            .contains("journaled ACK requires independently durable storage"),
        "{error:#}"
    );
}

#[test]
fn ack_follows_the_journal_while_materialization_retains_every_descriptor() {
    let directory = TempDir::new().unwrap();
    let store = StateStore::open(directory.path(), StateStoreOptions::default()).unwrap();
    let source = SourceId("journaled".into());
    let mut ledger = journaled(&store, &source);
    ledger.journaled(transaction(&source, 1, &[1, 2])).unwrap();
    ledger.journaled(transaction(&source, 2, &[1])).unwrap();
    ledger.journaled(transaction(&source, 3, &[2])).unwrap();
    // The source may release WAL through the journal frontier...
    assert_eq!(ledger.acknowledgement(), PgLsn(31));
    // ...but the journal must be retained from the materialized frontier.
    assert_eq!(ledger.watermarks().materialized_lsn, PgLsn(0));
    assert_eq!(pending(&ledger), [1, 2, 3]);
    assert_eq!(pending_for(&ledger, 1), [1, 2]);
    assert_eq!(pending_for(&ledger, 2), [1, 3]);

    // Out-of-order completion never advances the retained prefix past an
    // incomplete transaction, and never lowers the acknowledgement.
    ledger.table_materialized(PgLsn(21), TableId(1), 7).unwrap();
    ledger.table_materialized(PgLsn(31), TableId(2), 8).unwrap();
    assert_eq!(ledger.watermarks().materialized_lsn, PgLsn(0));
    assert_eq!(ledger.acknowledgement(), PgLsn(31));
    assert_eq!(pending(&ledger), [1, 2, 3]);
    assert_eq!(
        ledger.pending_tables(PgLsn(11)).unwrap(),
        [TableId(1), TableId(2)]
    );

    ledger.table_materialized(PgLsn(11), TableId(1), 7).unwrap();
    assert_eq!(ledger.watermarks().materialized_lsn, PgLsn(0));
    ledger.table_materialized(PgLsn(11), TableId(2), 8).unwrap();
    assert_eq!(ledger.watermarks().materialized_lsn, PgLsn(31));
    assert_eq!(ledger.acknowledgement(), PgLsn(31));
    assert!(pending(&ledger).is_empty());
    assert_eq!(ledger.pending_count(), 0);
}

#[test]
fn a_crash_after_journaled_ack_retains_unmaterialized_work_across_reopen() {
    let directory = TempDir::new().unwrap();
    let options = StateStoreOptions {
        apply_batch_rows: 2,
        ..Default::default()
    };
    let control = ControlStore::open(directory.path().join("control")).unwrap();
    let store = control
        .initialize_index(directory.path().join("index"), options.clone())
        .unwrap();
    let source = SourceId("journaled-reopen".into());
    let mut ledger = journaled(&store, &source);
    // Bounded pages: every page is one durable transition.
    let transactions: Vec<_> = (1..=5)
        .map(|xid| transaction(&source, xid, &[1, 2]))
        .collect();
    for page in transactions.chunks(2) {
        assert_eq!(ledger.journaled_batch(page).unwrap(), page.len());
    }
    ledger.table_materialized(PgLsn(11), TableId(1), 1).unwrap();
    ledger.table_materialized(PgLsn(11), TableId(2), 2).unwrap();
    ledger.table_materialized(PgLsn(21), TableId(1), 3).unwrap();
    let acknowledged = ledger.acknowledgement();
    assert_eq!(acknowledged, PgLsn(51));
    assert_eq!(ledger.watermarks().materialized_lsn, PgLsn(11));
    // Simulate SIGKILL after PostgreSQL received the journaled ACK.
    drop(ledger);
    drop(store);
    drop(control);

    let control = ControlStore::open(directory.path().join("control")).unwrap();
    let store =
        StateStore::open_with_control(directory.path().join("index"), options, control).unwrap();
    let mut ledger = journaled(&store, &source);
    assert_eq!(ledger.acknowledgement(), acknowledged);
    assert_eq!(ledger.watermarks().journal_durable_lsn, PgLsn(51));
    assert_eq!(ledger.watermarks().materialized_lsn, PgLsn(11));
    assert_eq!(pending(&ledger), [2, 3, 4, 5]);
    assert_eq!(pending_for(&ledger, 1), [3, 4, 5]);
    assert_eq!(pending_for(&ledger, 2), [2, 3, 4, 5]);
    assert_eq!(ledger.pending_count(), 4);

    // The journal replays everything after the materialized frontier after a
    // restart; replayed descriptors are accepted only if identical.
    assert_eq!(ledger.journaled_batch(&transactions[1..3]).unwrap(), 0);
    let mut changed = transactions[3].clone();
    changed.affected_tables = vec![TableId(1)];
    let error = ledger.journaled(changed).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("replayed transaction identity or content differs"),
        "{error:#}"
    );
    assert!(!ledger.journaled(transaction(&source, 5, &[1, 2])).unwrap());
    // An unknown transaction behind the acknowledged frontier is never registered.
    let error = ledger
        .journaled(SourceTransaction {
            begin_lsn: PgLsn(40),
            commit_lsn: PgLsn(45),
            end_lsn: PgLsn(46),
            ..transaction(&source, 6, &[1])
        })
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("out-of-order transaction registration"),
        "{error:#}"
    );

    for xid in 2..=5u32 {
        let end = PgLsn(u64::from(xid) * 10 + 1);
        for table in [1, 2] {
            if !(xid == 2 && table == 1) {
                ledger.table_materialized(end, TableId(table), 9).unwrap();
            }
        }
        assert_eq!(ledger.watermarks().materialized_lsn, end);
        assert_eq!(ledger.acknowledgement(), PgLsn(51));
    }
    assert!(pending(&ledger).is_empty());
}

#[test]
fn switching_modes_on_reopen_never_drops_retained_descriptors() {
    let directory = TempDir::new().unwrap();
    let store = StateStore::open(directory.path(), StateStoreOptions::default()).unwrap();
    let source = SourceId("mode-switch".into());
    let mut ledger = journaled(&store, &source);
    ledger.journaled(transaction(&source, 1, &[1])).unwrap();
    ledger.journaled(transaction(&source, 2, &[1])).unwrap();
    ledger.table_materialized(PgLsn(11), TableId(1), 1).unwrap();
    assert_eq!(ledger.acknowledgement(), PgLsn(21));
    drop(ledger);

    // Reverting to the default mode reports the materialized frontier. The
    // source never moves a confirmed position backwards, so the retained
    // journal descriptors are what keep the unpublished transaction.
    let mut ledger = SourceLedger::open(
        store.clone(),
        source.clone(),
        AckMode::Materialized,
        JournalDurability::LocalDisk,
    )
    .unwrap();
    assert_eq!(ledger.acknowledgement(), PgLsn(11));
    assert_eq!(ledger.watermarks().journal_durable_lsn, PgLsn(21));
    assert_eq!(pending(&ledger), [2]);
    ledger.table_materialized(PgLsn(21), TableId(1), 2).unwrap();
    assert_eq!(ledger.acknowledgement(), PgLsn(21));
    drop(ledger);

    let ledger = journaled(&store, &source);
    assert_eq!(ledger.acknowledgement(), PgLsn(21));
    assert!(pending(&ledger).is_empty());
}

#[test]
fn transactions_without_configured_tables_complete_at_the_journal_frontier() {
    let directory = TempDir::new().unwrap();
    let store = StateStore::open(directory.path(), StateStoreOptions::default()).unwrap();
    let source = SourceId("empty".into());
    let mut ledger = journaled(&store, &source);
    ledger.journaled(transaction(&source, 1, &[])).unwrap();
    assert_eq!(ledger.watermarks().materialized_lsn, PgLsn(11));
    ledger.journaled(transaction(&source, 2, &[1])).unwrap();
    ledger.journaled(transaction(&source, 3, &[])).unwrap();
    assert_eq!(ledger.acknowledgement(), PgLsn(31));
    // The empty transaction behind a pending one stays retained with it.
    assert_eq!(ledger.watermarks().materialized_lsn, PgLsn(11));
    assert_eq!(pending(&ledger), [2, 3]);
    ledger.table_materialized(PgLsn(21), TableId(1), 1).unwrap();
    assert_eq!(ledger.watermarks().materialized_lsn, PgLsn(31));
    assert!(pending(&ledger).is_empty());
}
