//! An epoch folded ahead of time, while the table's previous epoch publishes,
//! must publish exactly what a cold collapse of the same epoch publishes, and
//! every fold that no longer continues the epoch must be ignored.

use flow_coordinator::{CollapseLimits, Epoch, Prefold, TablePublisher, collapse_epoch_prefolded};
use flow_ingress_journal::{Journal, JournalConfig, JournalReader};
use flow_materializer::WriterConfig;
use flow_model::{
    Mutation, MutationKind, PgLsn, Row, SourceId, SourceTransaction, TableMutationCount,
    TableSchema, TableSchemaVersion, Value,
};
use flow_state_store::{ControlStore, StateStore, StateStoreOptions};
use flow_testkit::{catalog, scan, schema, table};
use iceberg::{Catalog, table::Table};
use std::sync::Arc;
use tempfile::TempDir;

const SOURCE: &str = "prefold";

fn limits(memory_bytes: usize) -> CollapseLimits {
    CollapseLimits {
        batch_rows: 4,
        batch_bytes: 1 << 20,
        memory_bytes,
    }
}

fn row(id: i64, value: &str) -> Row {
    vec![Value::Int64(id), Value::String(value.into())]
}

struct Fixture {
    _dir: TempDir,
    catalog: Arc<dyn Catalog>,
    store: StateStore,
    journal: Journal,
    reader: JournalReader,
    publisher: TablePublisher,
    schema: TableSchema,
    head: Table,
    position: u64,
    xid: u32,
}

impl Fixture {
    async fn new(schema: TableSchema) -> Self {
        let dir = TempDir::new().unwrap();
        let catalog: Arc<dyn Catalog> = Arc::new(catalog(&dir.path().join("warehouse")).await);
        let head = table(catalog.as_ref(), &schema).await;
        let control = ControlStore::open(dir.path().join("control")).unwrap();
        let store = control
            .initialize_index(
                dir.path().join("index"),
                StateStoreOptions {
                    apply_batch_rows: 4,
                    ..Default::default()
                },
            )
            .unwrap();
        let (journal, _) =
            Journal::open(dir.path().join("journal"), JournalConfig::default()).unwrap();
        let reader = journal.reader();
        let publisher = TablePublisher::new(
            store.clone(),
            catalog.clone(),
            WriterConfig::default(),
            4,
            1 << 20,
        )
        .unwrap();
        Self {
            _dir: dir,
            catalog,
            store,
            journal,
            reader,
            publisher,
            schema,
            head,
            position: 0,
            xid: 0,
        }
    }

    /// Journal one source transaction for the fixture's table.
    fn transaction(&mut self, kinds: Vec<MutationKind>) -> SourceTransaction {
        self.xid += 1;
        self.position += 3;
        let table = self.schema.table_id;
        let mutations: Vec<_> = kinds
            .into_iter()
            .map(|kind| Mutation {
                table_id: table,
                schema_version: self.schema.version,
                kind,
            })
            .collect();
        for chunk in mutations.chunks(3) {
            self.journal
                .append_chunk(self.xid, &bincode::serialize(chunk).unwrap())
                .unwrap();
        }
        let transaction = SourceTransaction {
            source_id: SourceId(SOURCE.into()),
            xid: self.xid,
            begin_lsn: PgLsn(self.position - 2),
            commit_lsn: PgLsn(self.position - 1),
            end_lsn: PgLsn(self.position),
            commit_timestamp_micros: self.position as i64,
            schema_versions: vec![TableSchemaVersion {
                table_id: table,
                version: self.schema.version,
            }],
            affected_tables: vec![table],
            mutation_chunks: self.journal.transaction_chunks(self.xid),
            table_mutation_counts: Some(vec![TableMutationCount {
                table_id: table,
                mutations: mutations.len() as u64,
            }]),
        };
        self.journal.commit(transaction.clone()).unwrap();
        transaction
    }

    fn insert(&mut self, ids: std::ops::Range<i64>, value: &str) -> SourceTransaction {
        self.transaction(
            ids.map(|id| MutationKind::Insert {
                row: row(id, value),
            })
            .collect(),
        )
    }

    fn update(&mut self, ids: impl IntoIterator<Item = i64>, value: &str) -> SourceTransaction {
        let schema = self.schema.clone();
        self.transaction(
            ids.into_iter()
                .map(|id| MutationKind::Update {
                    old_key: schema.encode_key(&row(id, "")).unwrap(),
                    row: row(id, value),
                })
                .collect(),
        )
    }

    fn delete(&mut self, ids: impl IntoIterator<Item = i64>) -> SourceTransaction {
        let schema = self.schema.clone();
        self.transaction(
            ids.into_iter()
                .map(|id| MutationKind::Delete {
                    key: schema.encode_key(&row(id, "")).unwrap(),
                })
                .collect(),
        )
    }

    fn prefold(
        &self,
        transactions: &[SourceTransaction],
        limits: CollapseLimits,
    ) -> Option<Prefold> {
        Prefold::new(SourceId(SOURCE.into()), self.schema.clone(), limits)
            .unwrap()
            .extend(&self.store, &self.reader, transactions, limits)
            .unwrap()
    }

    /// Collapse and publish an epoch, returning how many transactions the
    /// collapse took from `prefold`.
    async fn publish(
        &mut self,
        transactions: &[SourceTransaction],
        limits: CollapseLimits,
        prefold: Option<Prefold>,
    ) -> usize {
        let epoch =
            Epoch::new(SourceId(SOURCE.into()), self.schema.table_id, transactions).unwrap();
        let collapsed = collapse_epoch_prefolded(
            &self.store,
            &self.reader,
            &self.head,
            &self.schema,
            &epoch,
            transactions,
            limits,
            prefold,
        )
        .unwrap();
        let prefolded = collapsed.prefolded_transactions();
        self.publisher
            .publish(&self.head, &self.schema, collapsed)
            .await
            .unwrap();
        self.store.forget_applied(&epoch.id).unwrap();
        self.head = self
            .catalog
            .load_table(self.head.identifier())
            .await
            .unwrap();
        prefolded
    }

    async fn rows(&self) -> Vec<Row> {
        let mut rows = scan(&self.head, &self.schema).await.unwrap();
        rows.sort_by_key(|row| match row[0] {
            Value::Int64(id) => id,
            _ => unreachable!(),
        });
        rows
    }
}

/// Runs the same three epochs twice: once cold, once with the third epoch's
/// leading transactions folded before the second epoch is even collapsed.
/// The second epoch moves, deletes and re-inserts keys the fold also touches.
#[tokio::test]
async fn a_prefolded_epoch_binds_to_the_same_rows_as_a_cold_collapse() {
    let mut published = Vec::new();
    for prefolding in [false, true] {
        let mut f = Fixture::new(schema(1)).await;
        let seed = f.insert(0..12, "seed");
        f.publish(&[seed], limits(1 << 20), None).await;
        let second = [
            f.update(0..4, "second"),
            f.delete([4, 5]),
            f.insert(20..24, "second"),
        ];
        let third = [
            f.update([0, 1, 20, 6], "third"),
            f.delete([2, 21, 7]),
            f.insert(4..6, "reinserted"),
            f.update([4, 3, 22], "third-late"),
        ];
        let prefold = prefolding.then(|| f.prefold(&third[..3], limits(1 << 20)).unwrap());
        assert_eq!(f.publish(&second, limits(1 << 20), None).await, 0);
        let prefolded = f.publish(&third, limits(1 << 20), prefold).await;
        assert_eq!(prefolded, if prefolding { 3 } else { 0 });
        published.push(f.rows().await);
    }
    assert_eq!(published[0], published[1]);
    assert_eq!(published[0].len(), 12 - 2 + 4 - 3 + 2);
}

/// A fold that is not a prefix of the epoch, or was made for another schema
/// or budget, is dropped and the epoch collapses from the journal.
#[tokio::test]
async fn a_fold_that_does_not_continue_the_epoch_is_ignored() {
    let mut f = Fixture::new(schema(1)).await;
    let seed = f.insert(0..8, "seed");
    f.publish(&[seed], limits(1 << 20), None).await;
    let first = f.update(0..2, "first");
    let second = f.update(2..4, "second");
    let third = f.delete([4]);

    // Folded a transaction the epoch does not begin with.
    let skipped = f.prefold(std::slice::from_ref(&second), limits(1 << 20));
    // Folded more than the epoch contains.
    let longer = f.prefold(
        &[first.clone(), second.clone(), third.clone()],
        limits(1 << 20),
    );
    // Folded under another budget.
    let budget = f.prefold(std::slice::from_ref(&first), limits(1 << 19));
    // Folded with a schema the epoch no longer publishes.
    let mut renamed = f.schema.clone();
    renamed.columns[1].name = "renamed".into();
    let stale = Prefold::new(SourceId(SOURCE.into()), renamed, limits(1 << 20))
        .unwrap()
        .extend(
            &f.store,
            &f.reader,
            std::slice::from_ref(&first),
            limits(1 << 20),
        )
        .unwrap();
    // Folded for another source.
    let foreign = Prefold::new(SourceId("other".into()), f.schema.clone(), limits(1 << 20))
        .unwrap()
        .extend(
            &f.store,
            &f.reader,
            std::slice::from_ref(&first),
            limits(1 << 20),
        )
        .unwrap();
    assert!(
        foreign.is_none(),
        "another source's transactions are not folded"
    );

    for prefold in [skipped, longer, budget, stale] {
        assert!(prefold.is_some(), "each fold succeeded on its own");
        let epoch = Epoch::new(
            SourceId(SOURCE.into()),
            f.schema.table_id,
            &[first.clone(), second.clone()],
        )
        .unwrap();
        let collapsed = collapse_epoch_prefolded(
            &f.store,
            &f.reader,
            &f.head,
            &f.schema,
            &epoch,
            &[first.clone(), second.clone()],
            limits(1 << 20),
            prefold,
        )
        .unwrap();
        assert_eq!(collapsed.prefolded_transactions(), 0);
        drop(collapsed);
        f.store.discard_transaction(&epoch.id.0).unwrap();
    }
    assert_eq!(
        f.publish(&[first, second, third], limits(1 << 20), None)
            .await,
        0
    );
    let rows = f.rows().await;
    assert_eq!(rows.len(), 7);
    assert_eq!(rows[0], row(0, "first"));
    assert_eq!(rows[3], row(3, "second"));
}

/// Folds that would leave the in-memory path give up instead of spilling:
/// over budget, initial copies, unordered input, keyless tables, disk-only.
#[tokio::test]
async fn folds_that_need_the_cold_path_give_up() {
    let mut f = Fixture::new(schema(1)).await;
    let wide = f.insert(0..64, &"x".repeat(1024));
    assert!(
        f.prefold(std::slice::from_ref(&wide), limits(16 << 10))
            .is_none(),
        "over budget"
    );
    let mut initial = f.insert(100..101, "copy");
    initial.xid = 0;
    assert!(
        f.prefold(&[initial], limits(1 << 20)).is_none(),
        "initial copy"
    );
    let later = f.insert(200..201, "later");
    let earlier = f.insert(300..301, "earlier");
    assert!(
        f.prefold(&[earlier.clone(), later.clone()], limits(1 << 20))
            .is_none(),
        "out of order"
    );
    let folded = f
        .prefold(std::slice::from_ref(&later), limits(1 << 20))
        .unwrap();
    assert!(
        folded
            .extend(
                &f.store,
                &f.reader,
                std::slice::from_ref(&later),
                limits(1 << 20)
            )
            .unwrap()
            .is_none(),
        "a transaction folded twice"
    );
    let keyless = TableSchema {
        primary_key: vec![],
        append_only: true,
        ..schema(2)
    };
    assert!(Prefold::new(SourceId(SOURCE.into()), keyless, limits(1 << 20)).is_none());
    assert!(Prefold::new(SourceId(SOURCE.into()), schema(1), limits(0)).is_none());

    // A fold that fits continues into an epoch that then spills: the result
    // still matches the journal.
    let seed = f.insert(1000..1010, "seed");
    f.publish(&[seed], limits(1 << 20), None).await;
    let small = f.update(1000..1002, "small");
    let big = f.insert(1100..1160, &"y".repeat(512));
    let prefold = f
        .prefold(std::slice::from_ref(&small), limits(16 << 10))
        .unwrap();
    assert_eq!(
        f.publish(&[small, big], limits(16 << 10), Some(prefold))
            .await,
        1
    );
    let rows = f.rows().await;
    assert_eq!(rows.len(), 10 + 60);
    assert_eq!(rows[0], row(1000, "small"));
}
