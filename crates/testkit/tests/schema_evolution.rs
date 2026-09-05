use flow_coordinator::{
    CollapseLimits, Epoch, ReplanRequired, SourceSchemaRecord, TableMaintenance, TablePublisher,
    collapse_epoch, reconcile_index_schema, store_source_schema,
};
use flow_ingress_journal::{Journal, JournalConfig};
use flow_materializer::{WriterConfig, iceberg_schema};
use flow_model::{
    Column, ColumnType, Mutation, MutationKind, OperationId, PgLsn, SourceId, SourceTransaction,
    TableSchemaVersion, Value,
};
use flow_pg_source::{Column as PgColumn, Relation};
use flow_state_store::{
    ControlStore, OperationKind, PreparedOperation, StateStore, StateStoreOptions,
};
use flow_testkit::{catalog, scan, schema, table};
use iceberg::{
    Catalog,
    transaction::{AddColumn, ApplyTransactionAction, Transaction},
};
use std::sync::Arc;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn historical_rows_project_through_a_schema_barrier_and_atomic_row_delta() {
    for memory_bytes in [0, 8 << 20] {
        schema_barrier(memory_bytes).await;
    }
}

async fn schema_barrier(memory_bytes: usize) {
    let temp = tempfile::tempdir().unwrap();
    let source = SourceId("schema-replay".into());
    let control = ControlStore::open(temp.path().join("control")).unwrap();
    let store = control
        .initialize_index(temp.path().join("index"), StateStoreOptions::default())
        .unwrap();
    let catalog = Arc::new(catalog(&temp.path().join("warehouse")).await);
    let old = schema(1);
    let mut next = old.clone();
    // One DDL adds two columns: logical schema version 2, Iceberg schema ID 1.
    next.version = 2;
    next.columns.extend([
        Column {
            field_id: 3,
            name: "flag".into(),
            data_type: ColumnType::Bool,
            nullable: true,
        },
        Column {
            field_id: 4,
            name: "occurred".into(),
            data_type: ColumnType::TimestampTzMicros,
            nullable: true,
        },
    ]);
    let relation = Relation {
        id: 1,
        namespace: "public".into(),
        name: "items".into(),
        replica_identity: b'f',
        columns: vec![
            PgColumn {
                name: "id".into(),
                type_oid: 20,
                type_modifier: -1,
                identity: true,
            },
            PgColumn {
                name: "value".into(),
                type_oid: 25,
                type_modifier: -1,
                identity: false,
            },
        ],
    };
    store_source_schema(
        &store,
        &source,
        &SourceSchemaRecord {
            format: 2,
            storage_id: 7,
            attribute_numbers: vec![1, 2],
            schema: old.clone(),
            relation: relation.clone(),
        },
    )
    .unwrap();
    let maintenance = TableMaintenance::new(
        store.clone(),
        catalog.clone(),
        flow_compactor::Policy {
            l0_soft_files: 1,
            min_file_age_ms: 0,
            ..Default::default()
        },
        WriterConfig::default(),
    )
    .unwrap();
    let mut next_relation = relation;
    next_relation.columns.extend([
        PgColumn {
            name: "flag".into(),
            type_oid: 16,
            type_modifier: -1,
            identity: false,
        },
        PgColumn {
            name: "occurred".into(),
            type_oid: 1184,
            type_modifier: -1,
            identity: false,
        },
    ]);
    store_source_schema(
        &store,
        &source,
        &SourceSchemaRecord {
            format: 2,
            storage_id: 7,
            attribute_numbers: vec![1, 2, 3, 4],
            schema: next.clone(),
            relation: next_relation,
        },
    )
    .unwrap();
    let mut target = table(catalog.as_ref(), &old).await;
    let empty = target.clone();
    let (mut journal, _) =
        Journal::open(temp.path().join("journal"), JournalConfig::default()).unwrap();
    let publisher = TablePublisher::new(
        store.clone(),
        catalog.clone(),
        WriterConfig::default(),
        128,
        1 << 20,
    )
    .unwrap();
    let row = |id, value: &str| vec![Value::Int64(id), Value::String(value.into())];
    let mut previous_metadata = None;
    for (xid, mutations, target_schema) in [
        (
            1,
            vec![
                Mutation {
                    table_id: old.table_id,
                    schema_version: 0,
                    kind: MutationKind::Insert {
                        row: row(1, "before"),
                    },
                },
                Mutation {
                    table_id: old.table_id,
                    schema_version: 0,
                    kind: MutationKind::Insert {
                        row: row(2, "delete"),
                    },
                },
            ],
            &old,
        ),
        (
            2,
            vec![
                Mutation {
                    table_id: old.table_id,
                    schema_version: 0,
                    kind: MutationKind::Insert {
                        row: row(3, "old decoder"),
                    },
                },
                Mutation {
                    table_id: old.table_id,
                    schema_version: 2,
                    kind: MutationKind::Update {
                        old_key: old.encode_key(&row(1, "before")).unwrap(),
                        row: vec![
                            Value::Int64(1),
                            Value::String("after".into()),
                            Value::Bool(true),
                            Value::TimestampTzMicros(946_684_800_000_001),
                        ],
                    },
                },
                Mutation {
                    table_id: old.table_id,
                    schema_version: 0,
                    kind: MutationKind::Delete {
                        key: old.encode_key(&row(2, "delete")).unwrap(),
                    },
                },
            ],
            &next,
        ),
    ] {
        if xid == 2 {
            previous_metadata = Some(target.clone());
            let before = store.table_state(&old.table_id).unwrap();
            let key = old.encode_key(&row(1, "before")).unwrap();
            let location = store.lookup(&old.table_id, &key).unwrap().unwrap();
            let stale = maintenance
                .start_compaction(
                    &target,
                    &old,
                    StateStore::open(temp.path().join("old-build"), StateStoreOptions::default())
                        .unwrap(),
                )
                .await
                .unwrap()
                .unwrap()
                .wait()
                .await
                .unwrap();
            let tx = Transaction::new(&target);
            let mut action = tx.update_schema();
            for field in &iceberg_schema(&next).unwrap().as_struct().fields()[2..] {
                action = action.add_column(AddColumn::optional(
                    &field.name,
                    field.field_type.as_ref().clone(),
                ));
            }
            target = action
                .apply(tx)
                .unwrap()
                .commit(catalog.as_ref())
                .await
                .unwrap();
            // Capture sees a fresh catalog schema even when the caller still
            // holds the prior head. Yield to the schema barrier before starting
            // another build or changing the row index.
            let rejected = maintenance
                .start_compaction(
                    previous_metadata.as_ref().unwrap(),
                    &old,
                    StateStore::open(
                        temp.path().join("schema-race-build"),
                        StateStoreOptions::default(),
                    )
                    .unwrap(),
                )
                .await
                .err()
                .expect("fresh catalog schema must invalidate stale build capture");
            assert!(rejected.is::<ReplanRequired>(), "{rejected:#}");
            assert_eq!(store.table_state(&old.table_id).unwrap(), before);
            assert_ne!(
                target.metadata().current_schema().schema_id(),
                next.version as i32
            );
            assert_eq!(target.metadata().current_snapshot_id(), before.snapshot_id);
            let rejected = reconcile_index_schema(&store, &source, &empty, &next).unwrap_err();
            assert!(
                rejected
                    .to_string()
                    .contains("resolve the indexed snapshot")
            );
            let pending = OperationId("schema-fence".into());
            store
                .begin_prepare(PreparedOperation {
                    id: pending.clone(),
                    table_id: old.table_id,
                    kind: OperationKind::Ingest,
                    base_snapshot_id: before.snapshot_id,
                    last_lsn: before.materialized_lsn,
                    schema_version: old.version,
                    artifacts: Vec::new(),
                    payload: Vec::new(),
                })
                .unwrap();
            let rejected = reconcile_index_schema(&store, &source, &target, &next).unwrap_err();
            assert!(
                rejected
                    .to_string()
                    .contains("resolve the indexed snapshot")
            );
            store.discard_uncommitted(&pending).unwrap();
            assert_eq!(store.table_state(&old.table_id).unwrap(), before);
            reconcile_index_schema(&store, &source, &target, &next).unwrap();
            let mut expected = before;
            expected.schema_version = next.version;
            assert_eq!(store.table_state(&old.table_id).unwrap(), expected);
            assert_eq!(
                location.row_fingerprint,
                next.fingerprint(&old.project_row(row(1, "before"), &next).unwrap())
                    .unwrap()
            );
            assert_eq!(store.lookup(&old.table_id, &key).unwrap(), Some(location));
            assert!(
                maintenance
                    .finish_compaction(&target, &next, stale)
                    .await
                    .unwrap_err()
                    .is::<ReplanRequired>(),
                "a build from the previous schema must still be rejected"
            );
            let ready = maintenance
                .start_compaction(
                    &target,
                    &next,
                    StateStore::open(temp.path().join("new-build"), StateStoreOptions::default())
                        .unwrap(),
                )
                .await
                .unwrap()
                .unwrap()
                .wait()
                .await
                .unwrap();
            maintenance
                .finish_compaction(&target, &next, ready)
                .await
                .unwrap()
                .unwrap();
            target = catalog.load_table(target.identifier()).await.unwrap();
            assert_eq!(
                store.table_state(&old.table_id).unwrap().materialized_lsn,
                expected.materialized_lsn,
                "schema promotion and physical compaction cannot invent source progress"
            );
            let mut projected = scan(&target, &next).await.unwrap();
            projected.sort_by_key(|row| match row[0] {
                Value::Int64(id) => id,
                _ => unreachable!(),
            });
            assert_eq!(
                projected,
                [row(1, "before"), row(2, "delete")]
                    .into_iter()
                    .map(|row| old.project_row(row, &next).unwrap())
                    .collect::<Vec<_>>()
            );
        }
        journal
            .append_chunk(xid, &bincode::serialize(&mutations).unwrap())
            .unwrap();
        let txn = SourceTransaction {
            source_id: source.clone(),
            xid,
            begin_lsn: PgLsn(u64::from(xid) * 10 - 1),
            commit_lsn: PgLsn(u64::from(xid) * 10),
            end_lsn: PgLsn(u64::from(xid) * 10 + 1),
            commit_timestamp_micros: 0,
            schema_versions: vec![TableSchemaVersion {
                table_id: old.table_id,
                version: target_schema.version,
            }],
            affected_tables: vec![old.table_id],
            mutation_chunks: journal.transaction_chunks(xid),
            table_mutation_counts: Some(vec![flow_model::TableMutationCount {
                table_id: old.table_id,
                mutations: mutations.len() as u64,
            }]),
        };
        journal.commit(txn.clone()).unwrap();
        let epoch = Epoch::new(source.clone(), old.table_id, std::slice::from_ref(&txn)).unwrap();
        let collapsed = collapse_epoch(
            &store,
            &journal,
            &target,
            target_schema,
            &epoch,
            std::slice::from_ref(&txn),
            CollapseLimits {
                batch_rows: 128,
                batch_bytes: 1 << 20,
                memory_bytes,
            },
        )
        .unwrap();
        publisher
            .publish(&target, target_schema, collapsed)
            .await
            .unwrap();
        target = catalog.load_table(target.identifier()).await.unwrap();
    }
    let mut actual = scan(&target, &next).await.unwrap();
    actual.sort_by_key(|row| match row[0] {
        Value::Int64(id) => id,
        _ => unreachable!(),
    });
    assert_eq!(
        actual,
        vec![
            vec![
                Value::Int64(1),
                Value::String("after".into()),
                Value::Bool(true),
                Value::TimestampTzMicros(946_684_800_000_001)
            ],
            vec![
                Value::Int64(3),
                Value::String("old decoder".into()),
                Value::Null,
                Value::Null
            ],
        ]
    );
    assert_eq!(
        scan(&previous_metadata.unwrap(), &old).await.unwrap().len(),
        2
    );
    assert_eq!(store.table_state(&old.table_id).unwrap().schema_version, 2);
}
