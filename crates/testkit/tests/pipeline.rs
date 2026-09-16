use flow_coordinator::{
    AckMode, CollapseLimits, Epoch, JournalDurability, SourceLedger, TablePublisher, collapse_epoch,
};
use flow_ingress_journal::{Journal, JournalConfig};
use flow_materializer::WriterConfig;
use flow_model::{
    Mutation, MutationKind, PgLsn, SourceId, SourceTransaction, TableId, TableSchemaVersion, Value,
};
use flow_state_store::{OperationPhase, StateStore, StateStoreOptions};
use flow_testkit::{LostResponseCatalog, catalog, scan, schema, table};
use iceberg::Catalog;
use std::sync::Arc;
use tempfile::TempDir;

fn row(id: i64, value: &str) -> Vec<Value> {
    vec![Value::Int64(id), Value::String(value.into())]
}
fn transaction(
    journal: &mut Journal,
    end: u64,
    tables: Vec<TableId>,
    mutations: Vec<Mutation>,
) -> SourceTransaction {
    for rows in mutations.chunks(128) {
        journal
            .append_chunk(end as u32, &bincode::serialize(rows).unwrap())
            .unwrap();
    }
    let txn = SourceTransaction {
        source_id: SourceId("fixture".into()),
        xid: end as u32,
        begin_lsn: PgLsn(end - 2),
        commit_lsn: PgLsn(end - 1),
        end_lsn: PgLsn(end),
        commit_timestamp_micros: 0,
        schema_versions: tables
            .iter()
            .map(|id| TableSchemaVersion {
                table_id: *id,
                version: 0,
            })
            .collect(),
        table_mutation_counts: Some({
            let mut counts = tables
                .iter()
                .map(|&table_id| flow_model::TableMutationCount {
                    table_id,
                    mutations: mutations
                        .iter()
                        .filter(|mutation| mutation.table_id == table_id)
                        .count() as u64,
                })
                .collect::<Vec<_>>();
            counts.sort_unstable_by_key(|count| count.table_id);
            counts
        }),
        affected_tables: tables,
        mutation_chunks: journal.transaction_chunks(end as u32),
    };
    journal.commit(txn.clone()).unwrap();
    txn
}
fn mutation(table: u32, kind: MutationKind) -> Mutation {
    Mutation {
        table_id: TableId(table),
        schema_version: 0,
        kind,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pending_checkpoints_preserve_catalog_recovery_after_physical_index_loss() {
    use flow_compactor::Policy;
    use flow_coordinator::{GarbageProtection, TableMaintenance, resolve_catalog_operation};
    use flow_state_store::ControlStore;
    use std::time::Duration;

    for partial_apply in [false, true] {
        let temp = TempDir::new().unwrap();
        let control_path = temp.path().join("control");
        let index_path = temp.path().join("index");
        let options = StateStoreOptions {
            apply_batch_rows: 2,
            ..Default::default()
        };
        let control = ControlStore::open(&control_path).unwrap();
        let store = control
            .initialize_index(&index_path, options.clone())
            .unwrap();
        let (mut journal, _) =
            Journal::open(temp.path().join("journal"), JournalConfig::default()).unwrap();
        let catalog = Arc::new(LostResponseCatalog::new(
            catalog(&temp.path().join("warehouse")).await,
        ));
        let schema = schema(1);
        let mut head = table(catalog.as_ref(), &schema).await;
        let publisher = TablePublisher::new(
            store.clone(),
            catalog.clone(),
            WriterConfig::default(),
            2,
            1 << 20,
        )
        .unwrap();
        let mut ledger = SourceLedger::open(
            store.clone(),
            SourceId("fixture".into()),
            AckMode::Materialized,
            JournalDurability::LocalDisk,
        )
        .unwrap();
        let mut snapshots = Vec::new();
        for end in [10, 20, 30] {
            let mutations = if end == 10 {
                (1..=4)
                    .map(|id| {
                        mutation(
                            1,
                            MutationKind::Insert {
                                row: row(id, "initial"),
                            },
                        )
                    })
                    .collect()
            } else {
                (1..=if end == 20 { 1 } else { 3 })
                    .map(|id| {
                        mutation(
                            1,
                            MutationKind::Update {
                                old_key: schema.encode_key(&row(id, "initial")).unwrap(),
                                row: row(id, if end == 20 { "base" } else { "pending" }),
                            },
                        )
                    })
                    .collect()
            };
            let tx = transaction(&mut journal, end, vec![schema.table_id], mutations);
            ledger.journaled(tx.clone()).unwrap();
            let epoch = Epoch::new(
                tx.source_id.clone(),
                schema.table_id,
                std::slice::from_ref(&tx),
            )
            .unwrap();
            let collapsed = collapse_epoch(
                &store,
                &journal,
                &head,
                &schema,
                &epoch,
                std::slice::from_ref(&tx),
                CollapseLimits {
                    batch_rows: 2,
                    batch_bytes: 1 << 20,
                    memory_bytes: 1 << 20,
                },
            )
            .unwrap();
            if end == 30 {
                catalog.lose_next_response_and_disconnect();
                let error = publisher
                    .publish(&head, &schema, collapsed)
                    .await
                    .unwrap_err();
                assert!(error.to_string().contains("lost catalog response"));
                catalog.reconnect();
                head = catalog.load_table(head.identifier()).await.unwrap();
                if partial_apply {
                    let snapshot = head.metadata().current_snapshot().unwrap();
                    store
                        .mark_committed(
                            &epoch.id,
                            snapshot.snapshot_id(),
                            snapshot.sequence_number(),
                        )
                        .unwrap();
                    let applied = store.apply_committed_batch(&epoch.id).unwrap();
                    assert_eq!(applied.applied_rows, 2);
                    assert!(!applied.complete);
                }
            } else {
                let snapshot = publisher
                    .publish(&head, &schema, collapsed)
                    .await
                    .unwrap()
                    .unwrap();
                ledger
                    .table_materialized(tx.end_lsn, schema.table_id, snapshot)
                    .unwrap();
                store.forget_applied(&epoch.id).unwrap();
                head = catalog.load_table(head.identifier()).await.unwrap();
            }
            snapshots.push(head.metadata().current_snapshot_id().unwrap());
        }
        assert_eq!(ledger.acknowledgement(), PgLsn(20));
        let checkpoint = control
            .checkpoint(&store, temp.path().join("checkpoint"))
            .unwrap();
        assert_eq!(checkpoint.pending_operations.len(), 1);
        let pending = &checkpoint.pending_operations[0];
        assert_eq!(pending.operation.base_snapshot_id, Some(snapshots[1]));
        assert_eq!(pending.snapshot_id, partial_apply.then_some(snapshots[2]));
        assert_eq!(checkpoint.tables[0].1.snapshot_id, Some(snapshots[1]));
        assert_eq!(checkpoint.tables[0].1.materialized_lsn, PgLsn(20));
        drop(publisher);
        drop(ledger);
        drop(store);
        drop(control);
        std::fs::remove_dir_all(&index_path).unwrap();

        let control = ControlStore::open(&control_path).unwrap();
        assert_eq!(
            control.checkpoints().unwrap(),
            std::slice::from_ref(&checkpoint)
        );
        let operation = control.pending_operations().unwrap().pop().unwrap();
        let resolved = resolve_catalog_operation(&operation, catalog.as_ref(), &head, &control)
            .await
            .unwrap();
        assert_eq!(resolved.unwrap().0, snapshots[2]);
        control
            .resolve_operation(&operation.operation.id, resolved)
            .unwrap();
        let authority = control.table_state(&schema.table_id).unwrap().unwrap();
        assert_eq!(authority.materialized_lsn, PgLsn(30));

        // Pending checkpoints are not restored by the daemon. Exercise its
        // existing catalog-scan fallback with the resolved control watermark.
        let replacement = StateStore::open(temp.path().join("rebuilt"), options.clone()).unwrap();
        let maintenance = TableMaintenance::new(
            replacement.clone(),
            catalog.clone(),
            Policy::default(),
            WriterConfig::default(),
        )
        .unwrap();
        let scratch = StateStore::open(temp.path().join("scratch"), options.clone()).unwrap();
        maintenance
            .rebuild_index(
                &head,
                &schema,
                replacement.clone(),
                scratch,
                authority.materialized_lsn,
                authority.schema_version,
            )
            .await
            .unwrap();
        let active = control.activate_rebuilt(&replacement).unwrap();
        drop(maintenance);
        drop(replacement);
        let store = StateStore::open_with_control(&active.path, options, control.clone()).unwrap();
        let mut ledger = SourceLedger::open(
            store.clone(),
            SourceId("fixture".into()),
            AckMode::Materialized,
            JournalDurability::LocalDisk,
        )
        .unwrap();
        assert_eq!(ledger.acknowledgement(), PgLsn(20));
        ledger.reconcile_table_progress().unwrap();
        assert_eq!(ledger.acknowledgement(), PgLsn(30));
        assert_eq!(ledger.pending_count(), 0);
        assert_eq!(journal.durable_lsn(), PgLsn(30));
        let mut actual = scan(&head, &schema).await.unwrap();
        actual.sort_by_key(|row| match row[0] {
            Value::Int64(id) => id,
            _ => unreachable!(),
        });
        assert_eq!(
            actual,
            vec![
                row(1, "pending"),
                row(2, "pending"),
                row(3, "pending"),
                row(4, "initial")
            ]
        );
        for expected in &actual {
            assert!(
                store
                    .lookup(&schema.table_id, &schema.encode_key(expected).unwrap())
                    .unwrap()
                    .is_some()
            );
        }

        let protection =
            GarbageProtection::from_checkpoints(schema.table_id, &control.checkpoints().unwrap());
        assert!(protection.operations.contains(&operation.operation.id));
        assert!(protection.snapshots.contains(&snapshots[1]));
        let maintenance = TableMaintenance::new(
            store,
            catalog.clone(),
            Policy::default(),
            WriterConfig::default(),
        )
        .unwrap();
        tokio::time::sleep(Duration::from_millis(3)).await;
        assert_eq!(
            maintenance
                .expire_history(
                    &head,
                    schema.table_id,
                    Duration::from_millis(1),
                    &protection.snapshots,
                )
                .await
                .unwrap(),
            1
        );
        let retained = catalog.load_table(head.identifier()).await.unwrap();
        assert!(retained.metadata().snapshot_by_id(snapshots[0]).is_none());
        // An unknown committed response records only the old base; retaining
        // its full successor chain must still protect the operation marker.
        assert!(retained.metadata().snapshot_by_id(snapshots[1]).is_some());
        assert!(retained.metadata().snapshot_by_id(snapshots[2]).is_some());
        assert_eq!(scan(&retained, &schema).await.unwrap().len(), 4);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parquet_mor_recovery_and_source_wide_ack_survive_lost_commit_response() {
    let temp = TempDir::new().unwrap();
    let store = StateStore::open(temp.path().join("index"), StateStoreOptions::default()).unwrap();
    let (mut journal, _) =
        Journal::open(temp.path().join("journal"), JournalConfig::default()).unwrap();
    let catalog = Arc::new(LostResponseCatalog::new(
        catalog(&temp.path().join("warehouse")).await,
    ));
    let a = schema(1);
    let b = schema(2);
    let ta = table(catalog.as_ref(), &a).await;
    let tb = table(catalog.as_ref(), &b).await;
    let publisher = TablePublisher::new(
        store.clone(),
        catalog.clone(),
        WriterConfig::default(),
        128,
        1 << 20,
    )
    .unwrap();
    let mut ledger = SourceLedger::open(
        store.clone(),
        SourceId("fixture".into()),
        AckMode::Materialized,
        JournalDurability::LocalDisk,
    )
    .unwrap();
    let tx = transaction(
        &mut journal,
        10,
        vec![a.table_id, b.table_id],
        vec![
            mutation(1, MutationKind::Insert { row: row(1, "a") }),
            mutation(2, MutationKind::Insert { row: row(2, "b") }),
        ],
    );
    ledger.journaled(tx.clone()).unwrap();
    let ea = Epoch::new(tx.source_id.clone(), a.table_id, std::slice::from_ref(&tx)).unwrap();
    let collapsed = collapse_epoch(
        &store,
        &journal,
        &catalog.load_table(ta.identifier()).await.unwrap(),
        &a,
        &ea,
        std::slice::from_ref(&tx),
        CollapseLimits {
            batch_rows: 128,
            batch_bytes: 8 << 20,
            memory_bytes: 8 << 20,
        },
    )
    .unwrap();
    catalog.lose_next_response_and_disconnect();
    assert!(
        publisher
            .publish(&ta, &a, collapsed)
            .await
            .unwrap_err()
            .to_string()
            .contains("lost catalog response")
    );
    assert_eq!(
        store.operation(&ea.id).unwrap().unwrap().phase,
        OperationPhase::Prepared
    );
    assert!(
        store
            .lookup(&a.table_id, &a.encode_key(&row(1, "a")).unwrap())
            .unwrap()
            .is_none()
    );
    // Reopen the durable Prepared state before allowing recovery to reach the catalog.
    drop(publisher);
    drop(ledger);
    drop(store);
    let store = StateStore::open(temp.path().join("index"), StateStoreOptions::default()).unwrap();
    let publisher = TablePublisher::new(
        store.clone(),
        catalog.clone(),
        WriterConfig::default(),
        128,
        1 << 20,
    )
    .unwrap();
    let mut ledger = SourceLedger::open(
        store.clone(),
        SourceId("fixture".into()),
        AckMode::Materialized,
        JournalDurability::LocalDisk,
    )
    .unwrap();
    assert_eq!(
        store.operation(&ea.id).unwrap().unwrap().phase,
        OperationPhase::Prepared
    );
    assert_eq!(ledger.acknowledgement(), PgLsn(0));
    catalog.reconnect();
    // The published Iceberg state is already readable without the index.
    let head = catalog.load_table(ta.identifier()).await.unwrap();
    assert_eq!(scan(&head, &a).await.unwrap(), vec![row(1, "a")]);
    let snapshot = publisher.recover(&head, &ea.id).await.unwrap().unwrap();
    ledger
        .table_materialized(tx.end_lsn, a.table_id, snapshot)
        .unwrap();
    assert_eq!(ledger.acknowledgement(), PgLsn(0));
    let eb = Epoch::new(tx.source_id.clone(), b.table_id, std::slice::from_ref(&tx)).unwrap();
    let collapsed = collapse_epoch(
        &store,
        &journal,
        &catalog.load_table(tb.identifier()).await.unwrap(),
        &b,
        &eb,
        std::slice::from_ref(&tx),
        CollapseLimits {
            batch_rows: 128,
            batch_bytes: 8 << 20,
            memory_bytes: 8 << 20,
        },
    )
    .unwrap();
    let snapshot = publisher
        .publish(&tb, &b, collapsed)
        .await
        .unwrap()
        .unwrap();
    ledger
        .table_materialized(tx.end_lsn, b.table_id, snapshot)
        .unwrap();
    assert_eq!(ledger.acknowledgement(), PgLsn(10));
    // A replacement row and a position delete appear in ONE snapshot.
    let update = transaction(
        &mut journal,
        20,
        vec![a.table_id],
        vec![mutation(
            1,
            MutationKind::Update {
                old_key: a.encode_key(&row(1, "a")).unwrap(),
                row: row(1, "updated"),
            },
        )],
    );
    ledger.journaled(update.clone()).unwrap();
    let eu = Epoch::new(
        update.source_id.clone(),
        a.table_id,
        std::slice::from_ref(&update),
    )
    .unwrap();
    let collapsed = collapse_epoch(
        &store,
        &journal,
        &catalog.load_table(ta.identifier()).await.unwrap(),
        &a,
        &eu,
        std::slice::from_ref(&update),
        CollapseLimits {
            batch_rows: 128,
            batch_bytes: 8 << 20,
            memory_bytes: 8 << 20,
        },
    )
    .unwrap();
    let snapshot = publisher
        .publish(&ta, &a, collapsed)
        .await
        .unwrap()
        .unwrap();
    ledger
        .table_materialized(update.end_lsn, a.table_id, snapshot)
        .unwrap();
    let head = catalog.load_table(ta.identifier()).await.unwrap();
    assert_eq!(scan(&head, &a).await.unwrap(), vec![row(1, "updated")]);
    assert_eq!(head.metadata().snapshots().len(), 2);
    let location = store
        .lookup(&a.table_id, &a.encode_key(&row(1, "updated")).unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(location.data_sequence_number, 2);
    assert_eq!(location.row_version, 2);
    // Restart ledger and verify replay does not advance or duplicate publication.
    drop(ledger);
    let mut ledger = SourceLedger::open(
        store.clone(),
        SourceId("fixture".into()),
        AckMode::Materialized,
        JournalDurability::LocalDisk,
    )
    .unwrap();
    assert_eq!(ledger.acknowledgement(), PgLsn(20));
    assert!(!ledger.journaled(update).unwrap());
    let delete = transaction(
        &mut journal,
        30,
        vec![a.table_id],
        vec![mutation(
            1,
            MutationKind::Delete {
                key: a.encode_key(&row(1, "updated")).unwrap(),
            },
        )],
    );
    let ed = Epoch::new(
        delete.source_id.clone(),
        a.table_id,
        std::slice::from_ref(&delete),
    )
    .unwrap();
    let collapsed = collapse_epoch(
        &store,
        &journal,
        &catalog.load_table(ta.identifier()).await.unwrap(),
        &a,
        &ed,
        &[delete],
        CollapseLimits {
            batch_rows: 128,
            batch_bytes: 8 << 20,
            memory_bytes: 8 << 20,
        },
    )
    .unwrap();
    publisher.publish(&ta, &a, collapsed).await.unwrap();
    let head = catalog.load_table(ta.identifier()).await.unwrap();
    assert!(scan(&head, &a).await.unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn large_epoch_collapses_pk_changes_and_noops_without_partial_snapshots() {
    for memory_bytes in [0, 64 << 10, 8 << 20] {
        large_epoch(memory_bytes).await;
    }
}

async fn large_epoch(memory_bytes: usize) {
    let temp = TempDir::new().unwrap();
    let store = StateStore::open(temp.path().join("index"), StateStoreOptions::default()).unwrap();
    let (mut journal, _) =
        Journal::open(temp.path().join("journal"), JournalConfig::default()).unwrap();
    let catalog = Arc::new(catalog(&temp.path().join("warehouse")).await);
    let schema = schema(1);
    let table = table(catalog.as_ref(), &schema).await;
    let publisher = TablePublisher::new(
        store.clone(),
        catalog.clone(),
        WriterConfig {
            target_file_bytes: 1024,
            row_group_rows: 64,
            ..Default::default()
        },
        64,
        65536,
    )
    .unwrap();
    let mut mutations = (0..1024)
        .map(|i| {
            mutation(
                1,
                MutationKind::Insert {
                    row: row(i, "initial"),
                },
            )
        })
        .collect::<Vec<_>>();
    for i in 0..512 {
        mutations.push(mutation(
            1,
            MutationKind::Update {
                old_key: schema.encode_key(&row(i, "ignored")).unwrap(),
                row: row(i + 2048, "moved"),
            },
        ));
    }
    for i in 512..1024 {
        mutations.push(mutation(
            1,
            MutationKind::Delete {
                key: schema.encode_key(&row(i, "ignored")).unwrap(),
            },
        ));
    }
    let txn = transaction(&mut journal, 10, vec![schema.table_id], mutations);
    assert_eq!(txn.mutation_count(schema.table_id), Some(2048));
    let epoch = Epoch::new(
        txn.source_id.clone(),
        schema.table_id,
        std::slice::from_ref(&txn),
    )
    .unwrap();
    let collapsed = collapse_epoch(
        &store,
        &journal,
        &catalog.load_table(table.identifier()).await.unwrap(),
        &schema,
        &epoch,
        &[txn],
        CollapseLimits {
            batch_rows: 128,
            batch_bytes: 8 << 20,
            memory_bytes,
        },
    )
    .unwrap();
    assert_eq!(
        collapsed.mode(),
        match memory_bytes {
            0 => "disk_disabled",
            65536 => "disk_capacity",
            _ => "memory",
        }
    );
    assert!(
        collapsed
            .memory_peak_bytes()
            .is_none_or(|peak| peak <= memory_bytes)
    );
    publisher.publish(&table, &schema, collapsed).await.unwrap();
    let head = catalog.load_table(table.identifier()).await.unwrap();
    let rows = scan(&head, &schema).await.unwrap();
    assert_eq!(rows.len(), 512);
    assert!(
        rows.iter()
            .all(|r| matches!(r[0], Value::Int64(2048..=2559)))
    );
    assert_eq!(head.metadata().snapshots().len(), 1);
    let noop = transaction(
        &mut journal,
        20,
        vec![schema.table_id],
        vec![
            mutation(
                1,
                MutationKind::Insert {
                    row: row(9999, "gone"),
                },
            ),
            mutation(
                1,
                MutationKind::Delete {
                    key: schema.encode_key(&row(9999, "gone")).unwrap(),
                },
            ),
        ],
    );
    let epoch = Epoch::new(
        noop.source_id.clone(),
        schema.table_id,
        std::slice::from_ref(&noop),
    )
    .unwrap();
    assert_eq!(noop.mutation_count(schema.table_id), Some(2));
    let collapsed = collapse_epoch(
        &store,
        &journal,
        &catalog.load_table(table.identifier()).await.unwrap(),
        &schema,
        &epoch,
        &[noop],
        CollapseLimits {
            batch_rows: 128,
            batch_bytes: 8 << 20,
            memory_bytes,
        },
    )
    .unwrap();
    publisher.publish(&head, &schema, collapsed).await.unwrap();
    assert_eq!(
        catalog
            .load_table(table.identifier())
            .await
            .unwrap()
            .metadata()
            .snapshots()
            .len(),
        1
    );
    assert_eq!(
        store
            .table_state(&schema.table_id)
            .unwrap()
            .materialized_lsn,
        PgLsn(20)
    );
    let empty = transaction(&mut journal, 30, vec![schema.table_id], Vec::new());
    assert_eq!(empty.mutation_count(schema.table_id), Some(0));
    let epoch = Epoch::new(
        empty.source_id.clone(),
        schema.table_id,
        std::slice::from_ref(&empty),
    )
    .unwrap();
    let collapsed = collapse_epoch(
        &store,
        &journal,
        &catalog.load_table(table.identifier()).await.unwrap(),
        &schema,
        &epoch,
        &[empty],
        CollapseLimits {
            batch_rows: 128,
            batch_bytes: 8 << 20,
            memory_bytes,
        },
    )
    .unwrap();
    publisher.publish(&head, &schema, collapsed).await.unwrap();
    let unchanged = catalog.load_table(table.identifier()).await.unwrap();
    assert_eq!(
        unchanged.metadata().current_snapshot_id(),
        head.metadata().current_snapshot_id()
    );
    assert_eq!(unchanged.metadata().snapshots().len(), 1);
    assert_eq!(
        store
            .table_state(&schema.table_id)
            .unwrap()
            .materialized_lsn,
        PgLsn(30)
    );
    // Bind changes to existing file positions, including a key move and a
    // delete/reinsert split across transaction boundaries in the same epoch.
    let key = |id| schema.encode_key(&row(id, "ignored")).unwrap();
    let first = transaction(
        &mut journal,
        40,
        vec![schema.table_id],
        vec![
            mutation(
                1,
                MutationKind::Update {
                    old_key: key(2048),
                    row: row(4096, "intermediate"),
                },
            ),
            mutation(1, MutationKind::Delete { key: key(2049) }),
            mutation(1, MutationKind::Delete { key: key(2050) }),
        ],
    );
    let second = transaction(
        &mut journal,
        50,
        vec![schema.table_id],
        vec![
            mutation(
                1,
                MutationKind::Insert {
                    row: row(2049, "reborn"),
                },
            ),
            mutation(
                1,
                MutationKind::Update {
                    old_key: key(4096),
                    row: row(4096, "final"),
                },
            ),
            mutation(
                1,
                MutationKind::Update {
                    old_key: key(2051),
                    row: row(2051, "updated"),
                },
            ),
        ],
    );
    let transactions = [first, second];
    let epoch = Epoch::new(
        transactions[0].source_id.clone(),
        schema.table_id,
        &transactions,
    )
    .unwrap();
    let collapsed = collapse_epoch(
        &store,
        &journal,
        &unchanged,
        &schema,
        &epoch,
        &transactions,
        CollapseLimits {
            batch_rows: 2,
            batch_bytes: 8 << 20,
            memory_bytes,
        },
    )
    .unwrap();
    publisher
        .publish(&unchanged, &schema, collapsed)
        .await
        .unwrap();
    let current = catalog.load_table(table.identifier()).await.unwrap();
    let mut actual = scan(&current, &schema).await.unwrap();
    actual.sort_by_key(|row| match row[0] {
        Value::Int64(id) => id,
        _ => unreachable!(),
    });
    let mut expected = (2049..2560)
        .filter(|&id| id != 2050)
        .map(|id| {
            row(
                id,
                match id {
                    2049 => "reborn",
                    2051 => "updated",
                    _ => "moved",
                },
            )
        })
        .collect::<Vec<_>>();
    expected.push(row(4096, "final"));
    assert_eq!(actual, expected);
    assert_eq!(current.metadata().snapshots().len(), 2);
    assert_eq!(
        store
            .table_state(&schema.table_id)
            .unwrap()
            .materialized_lsn,
        PgLsn(50)
    );
    assert!(
        store
            .lookup(&schema.table_id, &key(2048))
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .lookup(&schema.table_id, &key(2050))
            .unwrap()
            .is_none()
    );
    assert_eq!(
        store
            .lookup(&schema.table_id, &key(2049))
            .unwrap()
            .unwrap()
            .row_version,
        2
    );
    let mut historical = scan(&head, &schema).await.unwrap();
    historical.sort_by_key(|row| match row[0] {
        Value::Int64(id) => id,
        _ => unreachable!(),
    });
    assert_eq!(
        historical,
        (2048..2560).map(|id| row(id, "moved")).collect::<Vec<_>>()
    );
}

async fn external_rewrite(
    head: &iceberg::table::Table,
    schema: &flow_model::TableSchema,
) -> flow_iceberg_ext::RewriteFilesAction {
    use flow_iceberg_ext::{RewriteFilesAction, SnapshotView};
    use flow_materializer::DataWriter;
    use flow_model::OperationId;
    use iceberg::spec::DataContentType;
    let rows = scan(head, schema).await.unwrap();
    let view = SnapshotView::current(head).await.unwrap();
    let operation = OperationId(format!("external-{}", uuid::Uuid::new_v4()));
    let mut writer = DataWriter::new(
        head.file_io().clone(),
        head.metadata().location(),
        &operation,
        schema.clone(),
        0,
        WriterConfig::default(),
    )
    .unwrap();
    writer.write(&rows, PgLsn(0)).await.unwrap();
    RewriteFilesAction::new(head, operation.0)
        .with_operation_id_key("external.operation-id")
        .unwrap()
        .remove_data_files(
            view.live_files
                .values()
                .filter(|f| f.content_type() == DataContentType::Data)
                .map(|f| f.file_path().to_owned()),
        )
        .remove_delete_files(
            view.live_files
                .values()
                .filter(|f| f.content_type() == DataContentType::PositionDeletes)
                .map(|f| f.file_path().to_owned()),
        )
        .add_data_files(writer.close().await.unwrap())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publication_replans_insert_update_and_delete_after_external_rewrite_wins_cas() {
    use flow_compactor::Policy;
    use flow_coordinator::{ReplanRequired, TableMaintenance};
    for change in 0..3 {
        let temp = TempDir::new().unwrap();
        let store =
            StateStore::open(temp.path().join("index"), StateStoreOptions::default()).unwrap();
        let (mut journal, _) =
            Journal::open(temp.path().join("journal"), JournalConfig::default()).unwrap();
        let catalog = Arc::new(LostResponseCatalog::new(
            catalog(&temp.path().join("warehouse")).await,
        ));
        let schema = schema(1);
        let table = table(catalog.as_ref(), &schema).await;
        let publisher = TablePublisher::new(
            store.clone(),
            catalog.clone(),
            WriterConfig::default(),
            128,
            1 << 20,
        )
        .unwrap();
        let maintenance = TableMaintenance::new(
            store.clone(),
            catalog.clone(),
            Policy::default(),
            WriterConfig::default(),
        )
        .unwrap();
        let initial = transaction(
            &mut journal,
            10,
            vec![schema.table_id],
            vec![
                mutation(1, MutationKind::Insert { row: row(1, "one") }),
                mutation(1, MutationKind::Insert { row: row(2, "two") }),
            ],
        );
        let epoch = Epoch::new(
            initial.source_id.clone(),
            schema.table_id,
            std::slice::from_ref(&initial),
        )
        .unwrap();
        let collapsed = collapse_epoch(
            &store,
            &journal,
            &catalog.load_table(table.identifier()).await.unwrap(),
            &schema,
            &epoch,
            &[initial],
            CollapseLimits {
                batch_rows: 128,
                batch_bytes: 8 << 20,
                memory_bytes: 8 << 20,
            },
        )
        .unwrap();
        let original_snapshot = publisher.publish(&table, &schema, collapsed).await.unwrap();
        let before = catalog.load_table(table.identifier()).await.unwrap();
        let key = schema.encode_key(&row(2, "two")).unwrap();
        let old_location = store.lookup(&schema.table_id, &key).unwrap().unwrap();
        let kind = match change {
            0 => MutationKind::Insert {
                row: row(3, "three"),
            },
            1 => MutationKind::Update {
                old_key: schema.encode_key(&row(1, "one")).unwrap(),
                row: row(1, "changed"),
            },
            _ => MutationKind::Delete {
                key: schema.encode_key(&row(1, "one")).unwrap(),
            },
        };
        let update = transaction(
            &mut journal,
            20,
            vec![schema.table_id],
            vec![mutation(1, kind)],
        );
        let epoch = Epoch::new(
            update.source_id.clone(),
            schema.table_id,
            std::slice::from_ref(&update),
        )
        .unwrap();
        let collapsed = collapse_epoch(
            &store,
            &journal,
            &catalog.load_table(table.identifier()).await.unwrap(),
            &schema,
            &epoch,
            std::slice::from_ref(&update),
            CollapseLimits {
                batch_rows: 128,
                batch_bytes: 8 << 20,
                memory_bytes: 8 << 20,
            },
        )
        .unwrap();
        // The external service wins after all data, deletes, and prepared index
        // deltas exist, but before the ingestion catalog CAS is applied.
        catalog.rewrite_before_next_commit(external_rewrite(&before, &schema).await);
        let error = publisher
            .publish(&table, &schema, collapsed)
            .await
            .unwrap_err();
        assert!(error.is::<ReplanRequired>(), "{error:#}");
        assert!(store.operation(&epoch.id).unwrap().is_none());
        let indexed = store.table_state(&schema.table_id).unwrap();
        assert_eq!(indexed.snapshot_id, original_snapshot);
        assert_eq!(indexed.materialized_lsn, PgLsn(10));
        assert!(indexed.pending_operation.is_none());
        assert_eq!(
            store.lookup(&schema.table_id, &key).unwrap(),
            Some(old_location.clone())
        );
        let head = catalog.load_table(table.identifier()).await.unwrap();
        assert_eq!(head.metadata().snapshots().len(), 2);
        maintenance
            .reconcile(
                &head,
                &schema,
                StateStore::open(temp.path().join("scratch"), StateStoreOptions::default())
                    .unwrap(),
            )
            .await
            .unwrap();
        let moved = store.lookup(&schema.table_id, &key).unwrap().unwrap();
        assert_ne!(moved.data_file_id, old_location.data_file_id);
        let collapsed = collapse_epoch(
            &store,
            &journal,
            &catalog.load_table(table.identifier()).await.unwrap(),
            &schema,
            &epoch,
            &[update],
            CollapseLimits {
                batch_rows: 128,
                batch_bytes: 8 << 20,
                memory_bytes: 8 << 20,
            },
        )
        .unwrap();
        publisher.publish(&table, &schema, collapsed).await.unwrap();
        // Updating an unchanged preexisting key catches the pure-append case:
        // advancing past a disjoint rewrite must not leave old index locations.
        let following = transaction(
            &mut journal,
            30,
            vec![schema.table_id],
            vec![mutation(
                1,
                MutationKind::Update {
                    old_key: key,
                    row: row(2, "follow-up"),
                },
            )],
        );
        let epoch = Epoch::new(
            following.source_id.clone(),
            schema.table_id,
            std::slice::from_ref(&following),
        )
        .unwrap();
        let collapsed = collapse_epoch(
            &store,
            &journal,
            &catalog.load_table(table.identifier()).await.unwrap(),
            &schema,
            &epoch,
            &[following],
            CollapseLimits {
                batch_rows: 128,
                batch_bytes: 8 << 20,
                memory_bytes: 8 << 20,
            },
        )
        .unwrap();
        publisher.publish(&table, &schema, collapsed).await.unwrap();
        let head = catalog.load_table(table.identifier()).await.unwrap();
        assert_eq!(head.metadata().snapshots().len(), 4);
        let mut actual = scan(&head, &schema).await.unwrap();
        actual.sort_by_key(|row| match row[0] {
            Value::Int64(id) => id,
            _ => unreachable!(),
        });
        let expected = match change {
            0 => vec![row(1, "one"), row(2, "follow-up"), row(3, "three")],
            1 => vec![row(1, "changed"), row(2, "follow-up")],
            _ => vec![row(2, "follow-up")],
        };
        assert_eq!(actual, expected);
        assert_eq!(
            store
                .table_state(&schema.table_id)
                .unwrap()
                .materialized_lsn,
            PgLsn(30)
        );
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lost_commit_response_then_external_rewrite_recovers_original_index_before_reconciliation()
{
    use flow_compactor::Policy;
    use flow_coordinator::TableMaintenance;
    use flow_iceberg_ext::find_operation;
    let temp = TempDir::new().unwrap();
    let store = StateStore::open(temp.path().join("index"), StateStoreOptions::default()).unwrap();
    let (mut journal, _) =
        Journal::open(temp.path().join("journal"), JournalConfig::default()).unwrap();
    let catalog = Arc::new(LostResponseCatalog::new(
        catalog(&temp.path().join("warehouse")).await,
    ));
    let schema = schema(1);
    let table = table(catalog.as_ref(), &schema).await;
    let publisher = TablePublisher::new(
        store.clone(),
        catalog.clone(),
        WriterConfig::default(),
        128,
        1 << 20,
    )
    .unwrap();
    let initial = transaction(
        &mut journal,
        10,
        vec![schema.table_id],
        vec![mutation(1, MutationKind::Insert { row: row(1, "one") })],
    );
    let epoch = Epoch::new(
        initial.source_id.clone(),
        schema.table_id,
        std::slice::from_ref(&initial),
    )
    .unwrap();
    let collapsed = collapse_epoch(
        &store,
        &journal,
        &catalog.load_table(table.identifier()).await.unwrap(),
        &schema,
        &epoch,
        &[initial],
        CollapseLimits {
            batch_rows: 128,
            batch_bytes: 8 << 20,
            memory_bytes: 8 << 20,
        },
    )
    .unwrap();
    publisher.publish(&table, &schema, collapsed).await.unwrap();
    let update = transaction(
        &mut journal,
        20,
        vec![schema.table_id],
        vec![mutation(
            1,
            MutationKind::Update {
                old_key: schema.encode_key(&row(1, "one")).unwrap(),
                row: row(1, "updated"),
            },
        )],
    );
    let epoch = Epoch::new(
        update.source_id.clone(),
        schema.table_id,
        std::slice::from_ref(&update),
    )
    .unwrap();
    let collapsed = collapse_epoch(
        &store,
        &journal,
        &catalog.load_table(table.identifier()).await.unwrap(),
        &schema,
        &epoch,
        &[update],
        CollapseLimits {
            batch_rows: 128,
            batch_bytes: 8 << 20,
            memory_bytes: 8 << 20,
        },
    )
    .unwrap();
    catalog.lose_next_response_and_disconnect();
    let error = publisher
        .publish(&table, &schema, collapsed)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("lost catalog response"));
    assert_eq!(
        store.operation(&epoch.id).unwrap().unwrap().phase,
        OperationPhase::Prepared
    );
    drop(publisher);
    drop(store);
    let store = StateStore::open(temp.path().join("index"), StateStoreOptions::default()).unwrap();
    let publisher = TablePublisher::new(
        store.clone(),
        catalog.clone(),
        WriterConfig::default(),
        128,
        1 << 20,
    )
    .unwrap();
    let maintenance = TableMaintenance::new(
        store.clone(),
        catalog.clone(),
        Policy::default(),
        WriterConfig::default(),
    )
    .unwrap();
    assert_eq!(
        store.operation(&epoch.id).unwrap().unwrap().phase,
        OperationPhase::Prepared
    );
    catalog.reconnect();
    let published = catalog.load_table(table.identifier()).await.unwrap();
    let committed = find_operation(published.metadata(), &epoch.id.0).unwrap();
    let rewrite = external_rewrite(&published, &schema)
        .await
        .commit(catalog.as_ref(), &published)
        .await
        .unwrap();
    assert_eq!(
        publisher.recover(&rewrite.table, &epoch.id).await.unwrap(),
        Some(committed.snapshot_id())
    );
    assert_eq!(
        store.table_state(&schema.table_id).unwrap().snapshot_id,
        Some(committed.snapshot_id())
    );
    let head = catalog.load_table(table.identifier()).await.unwrap();
    assert_eq!(
        head.metadata().snapshots().len(),
        3,
        "recovery must not publish a second logical update"
    );
    maintenance
        .reconcile(
            &head,
            &schema,
            StateStore::open(temp.path().join("scratch"), StateStoreOptions::default()).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(scan(&head, &schema).await.unwrap(), vec![row(1, "updated")]);
    let key = schema.encode_key(&row(1, "updated")).unwrap();
    let location = store.lookup(&schema.table_id, &key).unwrap().unwrap();
    let view = flow_iceberg_ext::SnapshotView::current(&head)
        .await
        .unwrap();
    assert!(view.live_files.contains_key(&location.data_file_id.0));
    assert_eq!(location.row_version, 2);
    assert_eq!(
        store.table_state(&schema.table_id).unwrap().snapshot_id,
        Some(rewrite.snapshot_id)
    );
    assert_eq!(
        store
            .table_state(&schema.table_id)
            .unwrap()
            .materialized_lsn,
        PgLsn(20)
    );
}

fn warehouse_files(root: &std::path::Path) -> std::collections::BTreeSet<std::path::PathBuf> {
    let mut files = std::collections::BTreeSet::new();
    if !root.exists() {
        return files;
    }
    for entry in std::fs::read_dir(root).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            files.extend(warehouse_files(&path));
        } else {
            files.insert(path);
        }
    }
    files
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stale_collapse_rejects_inserts_and_noops_before_building_or_creating_files() {
    use flow_coordinator::ReplanRequired;
    for memory_bytes in [0, 8 << 20] {
        for physical_change in [false, true] {
            for noop in [false, true] {
                let temp = TempDir::new().unwrap();
                let store =
                    StateStore::open(temp.path().join("index"), StateStoreOptions::default())
                        .unwrap();
                let (mut journal, _) =
                    Journal::open(temp.path().join("journal"), JournalConfig::default()).unwrap();
                let catalog = Arc::new(catalog(&temp.path().join("warehouse")).await);
                let schema = schema(1);
                let table = table(catalog.as_ref(), &schema).await;
                let publisher = TablePublisher::new(
                    store.clone(),
                    catalog.clone(),
                    WriterConfig::default(),
                    128,
                    1 << 20,
                )
                .unwrap();
                let limits = || CollapseLimits {
                    batch_rows: 128,
                    batch_bytes: 8 << 20,
                    memory_bytes,
                };
                let initial = transaction(
                    &mut journal,
                    10,
                    vec![schema.table_id],
                    vec![mutation(
                        1,
                        MutationKind::Insert {
                            row: row(1, "original"),
                        },
                    )],
                );
                let epoch = Epoch::new(
                    initial.source_id.clone(),
                    schema.table_id,
                    std::slice::from_ref(&initial),
                )
                .unwrap();
                let collapsed = collapse_epoch(
                    &store,
                    &journal,
                    &table,
                    &schema,
                    &epoch,
                    &[initial],
                    limits(),
                )
                .unwrap();
                publisher.publish(&table, &schema, collapsed).await.unwrap();
                let head = catalog.load_table(table.identifier()).await.unwrap();
                let mut changes = vec![mutation(1, MutationKind::Insert { row: row(2, "new") })];
                if noop {
                    changes.push(mutation(
                        1,
                        MutationKind::Delete {
                            key: schema.encode_key(&row(2, "new")).unwrap(),
                        },
                    ));
                }
                let next = transaction(&mut journal, 20, vec![schema.table_id], changes);
                let epoch = Epoch::new(
                    next.source_id.clone(),
                    schema.table_id,
                    std::slice::from_ref(&next),
                )
                .unwrap();
                let collapsed =
                    collapse_epoch(&store, &journal, &head, &schema, &epoch, &[next], limits())
                        .unwrap();
                if physical_change {
                    external_rewrite(&head, &schema)
                        .await
                        .commit(catalog.as_ref(), &head)
                        .await
                        .unwrap();
                } else {
                    // Same catalog snapshot and same key presence, different
                    // durable source frontier: full TableState must be checked.
                    store
                        .complete_noop(&schema.table_id, PgLsn(15), schema.version)
                        .unwrap();
                }
                let before = warehouse_files(&temp.path().join("warehouse"));
                let indexed = store.table_state(&schema.table_id).unwrap();
                let error = publisher
                    .publish(&head, &schema, collapsed)
                    .await
                    .unwrap_err();
                assert!(error.is::<ReplanRequired>(), "{error:#}");
                assert_eq!(store.table_state(&schema.table_id).unwrap(), indexed);
                assert!(store.operation(&epoch.id).unwrap().is_none());
                assert_eq!(warehouse_files(&temp.path().join("warehouse")), before);
                let current = catalog.load_table(table.identifier()).await.unwrap();
                assert_eq!(
                    scan(&current, &schema).await.unwrap(),
                    vec![row(1, "original")]
                );
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropped_keyless_collapse_replays_the_same_identities_after_reopen() {
    use flow_model::PrimaryKey;
    for memory_bytes in [0, 64 << 10, 8 << 20] {
        let temp = TempDir::new().unwrap();
        let store =
            StateStore::open(temp.path().join("index"), StateStoreOptions::default()).unwrap();
        let (mut journal, _) =
            Journal::open(temp.path().join("journal"), JournalConfig::default()).unwrap();
        let catalog = Arc::new(catalog(&temp.path().join("warehouse")).await);
        let mut schema = schema(1);
        schema.primary_key.clear();
        schema.append_only = true;
        let table = table(catalog.as_ref(), &schema).await;
        let mut changes = Vec::new();
        for ordinal in 0..300 {
            if ordinal % 100 == 0 {
                changes.push(mutation(
                    2,
                    MutationKind::Insert {
                        row: row(ordinal, "other table"),
                    },
                ));
            }
            changes.push(mutation(
                1,
                MutationKind::Insert {
                    row: row(7, "identical"),
                },
            ));
        }
        let tx = transaction(&mut journal, 10, vec![schema.table_id, TableId(2)], changes);
        let epoch = Epoch::new(
            tx.source_id.clone(),
            schema.table_id,
            std::slice::from_ref(&tx),
        )
        .unwrap();
        let limits = || CollapseLimits {
            batch_rows: 128,
            batch_bytes: 8 << 20,
            memory_bytes,
        };
        let before = warehouse_files(&temp.path().join("warehouse"));
        let collapsed =
            collapse_epoch(&store, &journal, &table, &schema, &epoch, &[tx], limits()).unwrap();
        assert!(store.operation(&epoch.id).unwrap().is_none());
        drop(collapsed);
        assert_eq!(warehouse_files(&temp.path().join("warehouse")), before);
        drop(store);
        drop(journal);
        let store =
            StateStore::open(temp.path().join("index"), StateStoreOptions::default()).unwrap();
        let (journal, _) =
            Journal::open(temp.path().join("journal"), JournalConfig::default()).unwrap();
        let transactions = journal
            .reader()
            .transactions_after(PgLsn(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(transactions.len(), 1);
        let replayed = Epoch::new(
            transactions[0].source_id.clone(),
            schema.table_id,
            &transactions,
        )
        .unwrap();
        assert_eq!(replayed.id, epoch.id);
        let collapsed = collapse_epoch(
            &store,
            &journal,
            &table,
            &schema,
            &replayed,
            &transactions,
            limits(),
        )
        .unwrap();
        let publisher = TablePublisher::new(
            store.clone(),
            catalog.clone(),
            WriterConfig::default(),
            128,
            1 << 20,
        )
        .unwrap();
        publisher.publish(&table, &schema, collapsed).await.unwrap();
        let head = catalog.load_table(table.identifier()).await.unwrap();
        assert_eq!(
            scan(&head, &schema).await.unwrap(),
            vec![row(7, "identical"); 300]
        );
        assert_eq!(head.metadata().snapshots().len(), 1);
        let mut positions = std::collections::BTreeSet::new();
        for ordinal in 0_u64..300 {
            let mut key = epoch.id.0.as_bytes().to_vec();
            key.extend(ordinal.to_be_bytes());
            let location = store
                .lookup(&schema.table_id, &PrimaryKey(key))
                .unwrap()
                .unwrap();
            positions.insert((location.data_file_id.0, location.row_position));
        }
        assert_eq!(
            positions.len(),
            300,
            "identical append-only rows retain distinct replay identities"
        );
        assert_eq!(
            store
                .table_state(&schema.table_id)
                .unwrap()
                .materialized_lsn,
            PgLsn(10)
        );
    }
}

#[tokio::test]
async fn malformed_journal_payloads_cannot_be_acknowledged_as_empty_publications() {
    for memory_bytes in [0, 8 << 20] {
        for invalid in ["trailing_bytes", "wrong_count", "undeclared_table"] {
            let temp = TempDir::new().unwrap();
            let store =
                StateStore::open(temp.path().join("index"), StateStoreOptions::default()).unwrap();
            let catalog = catalog(&temp.path().join("warehouse")).await;
            let schema = schema(1);
            let table = table(&catalog, &schema).await;
            let (mut journal, _) =
                Journal::open(temp.path().join("journal"), JournalConfig::default()).unwrap();
            let mutations = if invalid == "undeclared_table" {
                vec![mutation(
                    2,
                    MutationKind::Insert {
                        row: row(1, "unrouted"),
                    },
                )]
            } else {
                vec![]
            };
            let mut payload = bincode::serialize(&mutations).unwrap();
            if invalid == "trailing_bytes" {
                payload.push(1);
            }
            journal.append_chunk(1, &payload).unwrap();
            let transaction = SourceTransaction {
                source_id: SourceId("invalid-payload".into()),
                xid: 1,
                begin_lsn: PgLsn(0),
                commit_lsn: PgLsn(0),
                end_lsn: PgLsn(1),
                commit_timestamp_micros: 0,
                schema_versions: vec![TableSchemaVersion {
                    table_id: schema.table_id,
                    version: schema.version,
                }],
                affected_tables: vec![schema.table_id],
                mutation_chunks: journal.transaction_chunks(1),
                table_mutation_counts: Some(vec![flow_model::TableMutationCount {
                    table_id: schema.table_id,
                    mutations: u64::from(invalid == "wrong_count"),
                }]),
            };
            journal.commit(transaction.clone()).unwrap();
            let transactions = [transaction];
            let epoch = Epoch::new(
                transactions[0].source_id.clone(),
                schema.table_id,
                &transactions,
            )
            .unwrap();
            let result = collapse_epoch(
                &store,
                &journal,
                &table,
                &schema,
                &epoch,
                &transactions,
                CollapseLimits {
                    batch_rows: 128,
                    batch_bytes: 8 << 20,
                    memory_bytes,
                },
            );
            assert!(
                result.is_err(),
                "{invalid} must fail before publication (memory={memory_bytes})"
            );
            assert!(store.operation(&epoch.id).unwrap().is_none());
            assert_eq!(
                store
                    .table_state(&schema.table_id)
                    .unwrap()
                    .materialized_lsn,
                PgLsn(0)
            );
            assert!(table.metadata().current_snapshot_id().is_none());
        }
    }
}
