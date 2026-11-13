//! Tables with a declared sort order: epochs written in that order and
//! stamped with it, compaction that keeps it, and the index staying exact.

use flow_coordinator::{CollapseLimits, Epoch, TablePublisher, collapse_epoch};
use flow_ingress_journal::{Journal, JournalConfig};
use flow_materializer::WriterConfig;
use flow_model::{
    Mutation, MutationKind, PgLsn, SourceId, SourceTransaction, TableId, TableSchema,
    TableSchemaVersion, Value,
};
use flow_state_store::{StateStore, StateStoreOptions};
use flow_testkit::{catalog, scan, schema, table};
use iceberg::{
    Catalog, MemoryCatalog,
    spec::{DataContentType, NullOrder},
    table::Table,
    transaction::{ApplyTransactionAction, Transaction},
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::sync::Arc;
use tempfile::TempDir;

fn row(id: i64, value: &str) -> Vec<Value> {
    vec![Value::Int64(id), Value::String(value.into())]
}

fn mutation(kind: MutationKind) -> Mutation {
    Mutation {
        table_id: TableId(1),
        schema_version: 0,
        kind,
    }
}

struct Fixture {
    _temp: TempDir,
    store: StateStore,
    journal: Journal,
    catalog: Arc<MemoryCatalog>,
    schema: TableSchema,
    table: Table,
    publisher: TablePublisher,
}

impl Fixture {
    async fn new() -> Self {
        let temp = TempDir::new().unwrap();
        let store =
            StateStore::open(temp.path().join("index"), StateStoreOptions::default()).unwrap();
        let (journal, _) =
            Journal::open(temp.path().join("journal"), JournalConfig::default()).unwrap();
        let catalog = Arc::new(catalog(&temp.path().join("warehouse")).await);
        let schema = schema(1);
        let table = table(catalog.as_ref(), &schema).await;
        let publisher = TablePublisher::new(
            store.clone(),
            catalog.clone(),
            WriterConfig::default(),
            16,
            1 << 20,
        )
        .unwrap();
        Self {
            _temp: temp,
            store,
            journal,
            catalog,
            schema,
            table,
            publisher,
        }
    }

    /// Replace the table's default sort order, as `WRITE ORDERED BY` does.
    async fn order_by(&mut self, fields: &[(&str, bool)]) {
        let transaction = Transaction::new(&self.table);
        let mut action = transaction.replace_sort_order();
        for (name, descending) in fields {
            action = if *descending {
                action.desc(name, NullOrder::Last)
            } else {
                action.asc(name, NullOrder::First)
            };
        }
        self.table = action
            .apply(transaction)
            .unwrap()
            .commit(self.catalog.as_ref())
            .await
            .unwrap();
    }

    async fn publish(&mut self, end: u64, mutations: Vec<Mutation>, memory_bytes: usize) {
        for rows in mutations.chunks(128) {
            self.journal
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
            schema_versions: vec![TableSchemaVersion {
                table_id: TableId(1),
                version: 0,
            }],
            table_mutation_counts: Some(vec![flow_model::TableMutationCount {
                table_id: TableId(1),
                mutations: mutations.len() as u64,
            }]),
            affected_tables: vec![TableId(1)],
            mutation_chunks: self.journal.transaction_chunks(end as u32),
        };
        self.journal.commit(txn.clone()).unwrap();
        let epoch = Epoch::new(
            txn.source_id.clone(),
            TableId(1),
            std::slice::from_ref(&txn),
        )
        .unwrap();
        let head = self
            .catalog
            .load_table(self.table.identifier())
            .await
            .unwrap();
        let collapsed = collapse_epoch(
            &self.store,
            &self.journal,
            &head,
            &self.schema,
            &epoch,
            &[txn],
            CollapseLimits {
                batch_rows: 16,
                batch_bytes: 1 << 20,
                memory_bytes,
            },
        )
        .unwrap();
        self.publisher
            .publish(&head, &self.schema, collapsed)
            .await
            .unwrap();
        self.table = self
            .catalog
            .load_table(self.table.identifier())
            .await
            .unwrap();
    }

    fn insert(ids: impl IntoIterator<Item = i64>, value: &str) -> Vec<Mutation> {
        ids.into_iter()
            .map(|id| {
                mutation(MutationKind::Insert {
                    row: row(id, &format!("{value}-{id}")),
                })
            })
            .collect()
    }

    /// Each live data file's `sort_order_id`, physical ids and Parquet sorting columns.
    async fn files(&self) -> Vec<(Option<i32>, Vec<i64>, usize)> {
        let view = flow_iceberg_ext::SnapshotView::current(&self.table)
            .await
            .unwrap();
        let mut files = Vec::new();
        for entry in view
            .live_files
            .values()
            .filter(|entry| entry.content_type() == DataContentType::Data)
        {
            let bytes = self
                .table
                .file_io()
                .new_input(entry.file_path())
                .unwrap()
                .read()
                .await
                .unwrap();
            let builder = ParquetRecordBatchReaderBuilder::try_new(bytes).unwrap();
            let sorting = builder
                .metadata()
                .row_groups()
                .first()
                .and_then(|group| group.sorting_columns())
                .map_or(0, Vec::len);
            let mut ids = Vec::new();
            for batch in builder.build().unwrap() {
                let batch = batch.unwrap();
                for row in flow_materializer::rows_from_batch(&self.schema, &batch).unwrap() {
                    match row[0] {
                        Value::Int64(id) => ids.push(id),
                        _ => unreachable!(),
                    }
                }
            }
            files.push((entry.data_file.sort_order_id(), ids, sorting));
        }
        files
    }
}

/// Negative ids sort after positive ones in Flow's canonical key encoding.
/// A buffered epoch is written in value order anyway and stamped.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn buffered_epochs_are_written_in_sort_order_and_stamped() {
    let mut fixture = Fixture::new().await;
    fixture.order_by(&[("id", false)]).await;
    fixture
        .publish(
            10,
            Fixture::insert([5, -3, 2, -1, 0, i64::MIN, 40], "a"),
            8 << 20,
        )
        .await;
    let files = fixture.files().await;
    assert_eq!(files.len(), 1);
    let (order, ids, sorting) = &files[0];
    assert_eq!(*order, Some(1));
    assert_eq!(ids, &[i64::MIN, -3, -1, 0, 2, 5, 40]);
    assert_eq!(*sorting, 1, "Parquet declares the sorting column");

    // Updates and deletes still find their rows by index after reordering.
    let mut changes = vec![
        mutation(MutationKind::Update {
            old_key: fixture.schema.encode_key(&row(-3, "")).unwrap(),
            row: row(-3, "b"),
        }),
        mutation(MutationKind::Delete {
            key: fixture.schema.encode_key(&row(5, "")).unwrap(),
        }),
    ];
    changes.extend(Fixture::insert([-7, 100], "b"));
    fixture.publish(20, changes, 8 << 20).await;
    let mut rows = scan(&fixture.table, &fixture.schema).await.unwrap();
    rows.sort_by_key(|row| match row[0] {
        Value::Int64(id) => id,
        _ => unreachable!(),
    });
    let ids: Vec<_> = rows
        .iter()
        .map(|row| match (&row[0], &row[1]) {
            (Value::Int64(id), Value::String(value)) => (*id, value.clone()),
            _ => unreachable!(),
        })
        .collect();
    assert_eq!(
        ids,
        vec![
            (i64::MIN, format!("a-{}", i64::MIN)),
            (-7, "b--7".into()),
            (-3, "b".into()),
            (-1, "a--1".into()),
            (0, "a-0".into()),
            (2, "a-2".into()),
            (40, "a-40".into()),
            (100, "b-100".into()),
        ]
    );
    let files = fixture.files().await;
    assert!(files.iter().all(|(order, ids, _)| {
        *order == Some(1) && ids.windows(2).all(|pair| pair[0] <= pair[1])
    }));
}

/// A disk-collapsed epoch streams in canonical key order. It is stamped only
/// when that order is the sort order.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn disk_epochs_are_stamped_only_when_already_in_order() {
    let mut fixture = Fixture::new().await;
    fixture.order_by(&[("id", false)]).await;
    fixture
        .publish(10, Fixture::insert([3, 1, 2], "a"), 0)
        .await;
    fixture.publish(20, Fixture::insert([10, -4], "b"), 0).await;
    let mut files = fixture.files().await;
    files.sort_by_key(|(_, ids, _)| ids.len());
    assert_eq!(files[0].0, None, "canonical order puts -4 after 10");
    assert_eq!(files[0].1, [10, -4]);
    assert_eq!(files[1].0, Some(1));
    assert_eq!(files[1].1, [1, 2, 3]);
    assert!(
        files.iter().all(|(_, _, sorting)| *sorting == 0),
        "verified files do not declare Parquet sorting columns"
    );
}

/// A sort order on a column other than the key, descending.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn any_column_order_is_supported() {
    let mut fixture = Fixture::new().await;
    fixture.order_by(&[("value", true)]).await;
    let inserts = [(1, "m"), (2, "z"), (3, "a"), (4, "q")]
        .into_iter()
        .map(|(id, value)| {
            mutation(MutationKind::Insert {
                row: row(id, value),
            })
        })
        .collect();
    fixture.publish(10, inserts, 8 << 20).await;
    let files = fixture.files().await;
    assert_eq!(files[0].0, Some(1));
    assert_eq!(files[0].1, [2, 4, 1, 3], "value z, q, m, a");
}

/// A table without a sort order is written exactly as before.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unsorted_tables_keep_canonical_order_and_no_stamp() {
    let mut fixture = Fixture::new().await;
    fixture
        .publish(10, Fixture::insert([2, -1, 1], "a"), 8 << 20)
        .await;
    let files = fixture.files().await;
    assert_eq!(files[0].0, None);
    assert_eq!(files[0].1, [1, 2, -1]);
}

/// Compact every live data file and return the output's stamp and ids.
async fn compact_all(fixture: &Fixture) -> (Option<i32>, Vec<i64>) {
    let view = flow_iceberg_ext::SnapshotView::current(&fixture.table)
        .await
        .unwrap();
    let inputs: std::collections::BTreeSet<_> = view
        .live_files
        .values()
        .filter(|entry| entry.content_type() == DataContentType::Data)
        .map(|entry| flow_model::FileId(entry.file_path().to_owned()))
        .collect();
    let plan = flow_compactor::CompactionPlan {
        base_snapshot_id: fixture.table.metadata().current_snapshot_id().unwrap(),
        schema_id: fixture.table.metadata().current_schema_id(),
        input_bytes: view
            .live_files
            .values()
            .map(|entry| entry.data_file.file_size_in_bytes())
            .sum(),
        input_files: inputs,
        delete_files: Default::default(),
        spec_id: 0,
        partition: vec![],
        output_level: flow_compactor::Level::L1,
        target_file_bytes: 128 << 20,
        delete_input_bytes: 0,
        delete_input_rows: 0,
        lazy_index_remap: true,
        merge: flow_compactor::MergePolicy::default(),
        per_target_deletes: false,
        target_ranges: Default::default(),
        reason: flow_compactor::PlanReason::L0Debt,
    };
    let scratch = TempDir::new().unwrap();
    let output = flow_compactor::compact(
        &fixture.table,
        fixture.schema.clone(),
        plan,
        flow_model::OperationId("flow-l1-rewrite-test".into()),
        &fixture.store,
        StateStore::open(scratch.path(), StateStoreOptions::default()).unwrap(),
        WriterConfig::default(),
        &flow_compactor::ReadLimits::default(),
        &flow_iceberg_ext::ManifestCache::default(),
    )
    .await
    .unwrap();
    assert_eq!(output.data_files.len(), 1);
    let file = &output.data_files[0];
    let bytes = fixture
        .table
        .file_io()
        .new_input(file.file_path())
        .unwrap()
        .read()
        .await
        .unwrap();
    let mut ids = Vec::new();
    for batch in ParquetRecordBatchReaderBuilder::try_new(bytes)
        .unwrap()
        .build()
        .unwrap()
    {
        for row in flow_materializer::rows_from_batch(&fixture.schema, &batch.unwrap()).unwrap() {
            match row[0] {
                Value::Int64(id) => ids.push(id),
                _ => unreachable!(),
            }
        }
    }
    (file.sort_order_id(), ids)
}

/// Sorted inputs with disjoint key ranges are concatenated in key order, even
/// when they were committed in the opposite order, and the output is stamped.
/// Before the sort order was supported, this compaction failed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compaction_concatenates_disjoint_sorted_inputs_in_key_order() {
    let mut fixture = Fixture::new().await;
    fixture.order_by(&[("id", false)]).await;
    fixture
        .publish(10, Fixture::insert(10..14, "a"), 8 << 20)
        .await;
    fixture
        .publish(20, Fixture::insert(-3..2, "b"), 8 << 20)
        .await;
    assert!(
        fixture
            .files()
            .await
            .iter()
            .all(|(order, _, _)| *order == Some(1))
    );
    let (order, ids) = compact_all(&fixture).await;
    assert_eq!(order, Some(1));
    assert_eq!(ids, [-3, -2, -1, 0, 1, 10, 11, 12, 13]);
}

/// The default compactor merges overlapping sorted inputs and stamps output.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compaction_of_overlapping_inputs_merges_in_sort_order() {
    let mut fixture = Fixture::new().await;
    fixture.order_by(&[("id", false)]).await;
    fixture
        .publish(10, Fixture::insert([1, 3, 5], "a"), 8 << 20)
        .await;
    fixture
        .publish(20, Fixture::insert([2, 4], "b"), 8 << 20)
        .await;
    let (order, ids) = compact_all(&fixture).await;
    assert_eq!(order, Some(1));
    assert_eq!(ids, [1, 2, 3, 4, 5]);
}

/// An unsorted table's rewrite of in-order inputs is not stamped: there is
/// no order to claim.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compaction_of_unsorted_tables_is_unstamped() {
    let mut fixture = Fixture::new().await;
    fixture
        .publish(10, Fixture::insert(0..3, "a"), 8 << 20)
        .await;
    fixture
        .publish(20, Fixture::insert(3..6, "b"), 8 << 20)
        .await;
    let (order, ids) = compact_all(&fixture).await;
    assert_eq!(order, None);
    assert_eq!(ids, [0, 1, 2, 3, 4, 5]);
}
