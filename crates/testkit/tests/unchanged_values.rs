//! Updates that omit unchanged values, such as DEFAULT-identity PostgreSQL
//! TOAST columns, publish complete rows through both collapse paths.
use flow_coordinator::{CollapseLimits, Epoch, TablePublisher, collapse_epoch};
use flow_ingress_journal::{Journal, JournalConfig};
use flow_materializer::WriterConfig;
use flow_model::{
    Column, ColumnType, Mutation, MutationKind, PgLsn, SourceId, SourceTransaction, TableId,
    TableMutationCount, TableSchema, TableSchemaVersion, Value,
};
use flow_state_store::{StateStore, StateStoreOptions};
use flow_testkit::{catalog, scan, table};
use iceberg::{Catalog, table::Table};
use std::{collections::BTreeMap, sync::Arc};
use tempfile::TempDir;

fn schema() -> TableSchema {
    let column = |field_id, name: &str, data_type, nullable| Column {
        field_id,
        name: name.into(),
        data_type,
        nullable,
    };
    TableSchema {
        table_id: TableId(1),
        version: 0,
        columns: vec![
            column(1, "id", ColumnType::Int64, false),
            column(2, "body", ColumnType::String, true),
            column(3, "version", ColumnType::Int64, true),
        ],
        primary_key: vec![0],
        append_only: false,
    }
}

fn full(id: i64, body: &str, version: i64) -> Vec<Value> {
    vec![
        Value::Int64(id),
        Value::String(body.into()),
        Value::Int64(version),
    ]
}

fn partial(id: i64, version: i64) -> Vec<Value> {
    vec![Value::Int64(id), Value::Unchanged, Value::Int64(version)]
}

fn body(id: i64) -> String {
    format!("{id}-{}", "toasted".repeat(64))
}

fn insert(row: Vec<Value>) -> Mutation {
    Mutation {
        table_id: TableId(1),
        schema_version: 0,
        kind: MutationKind::Insert { row },
    }
}

fn update(row: Vec<Value>) -> Mutation {
    let old_key = schema().encode_partial_key(&row).unwrap();
    Mutation {
        table_id: TableId(1),
        schema_version: 0,
        kind: MutationKind::Update { old_key, row },
    }
}

fn delete(id: i64) -> Mutation {
    Mutation {
        table_id: TableId(1),
        schema_version: 0,
        kind: MutationKind::Delete {
            key: schema().encode_partial_key(&partial(id, 0)).unwrap(),
        },
    }
}

fn transaction(journal: &mut Journal, end: u64, mutations: Vec<Mutation>) -> SourceTransaction {
    for rows in mutations.chunks(100) {
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
        affected_tables: vec![TableId(1)],
        schema_versions: vec![TableSchemaVersion {
            table_id: TableId(1),
            version: 0,
        }],
        table_mutation_counts: Some(vec![TableMutationCount {
            table_id: TableId(1),
            mutations: mutations.len() as u64,
        }]),
        mutation_chunks: journal.transaction_chunks(end as u32),
    };
    journal.commit(txn.clone()).unwrap();
    txn
}

struct Fixture {
    _temp: TempDir,
    store: StateStore,
    journal: Journal,
    catalog: Arc<iceberg::MemoryCatalog>,
    table: Table,
    publisher: TablePublisher,
    memory_bytes: usize,
}

impl Fixture {
    async fn new(memory_bytes: usize) -> Self {
        let temp = TempDir::new().unwrap();
        let store =
            StateStore::open(temp.path().join("index"), StateStoreOptions::default()).unwrap();
        let (journal, _) =
            Journal::open(temp.path().join("journal"), JournalConfig::default()).unwrap();
        let catalog = Arc::new(catalog(&temp.path().join("warehouse")).await);
        let table = table(catalog.as_ref(), &schema()).await;
        // Small files and row groups put originals in many files and groups.
        let publisher = TablePublisher::new(
            store.clone(),
            catalog.clone(),
            WriterConfig {
                target_file_bytes: 4096,
                row_group_rows: 16,
                ..Default::default()
            },
            64,
            1 << 20,
        )
        .unwrap();
        Self {
            _temp: temp,
            store,
            journal,
            catalog,
            table,
            publisher,
            memory_bytes,
        }
    }

    async fn publish(&mut self, end: u64, mutations: Vec<Mutation>) -> anyhow::Result<()> {
        let txn = transaction(&mut self.journal, end, mutations);
        let epoch = Epoch::new(
            txn.source_id.clone(),
            TableId(1),
            std::slice::from_ref(&txn),
        )?;
        let head = self.catalog.load_table(self.table.identifier()).await?;
        let collapsed = collapse_epoch(
            &self.store,
            &self.journal,
            &head,
            &schema(),
            &epoch,
            &[txn],
            CollapseLimits {
                batch_rows: 64,
                batch_bytes: 8 << 20,
                memory_bytes: self.memory_bytes,
            },
        )?;
        self.publisher.publish(&head, &schema(), collapsed).await?;
        Ok(())
    }

    async fn rows(&self) -> BTreeMap<i64, Vec<Value>> {
        let head = self
            .catalog
            .load_table(self.table.identifier())
            .await
            .unwrap();
        scan(&head, &schema())
            .await
            .unwrap()
            .into_iter()
            .map(|row| match row[0] {
                Value::Int64(id) => (id, row),
                _ => unreachable!("integer key"),
            })
            .collect()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unchanged_values_carry_forward_in_memory_and_on_disk() {
    // 0 selects the disk spool; 8 MiB keeps the epoch in memory.
    for memory_bytes in [0, 8 << 20] {
        let mut fixture = Fixture::new(memory_bytes).await;
        fixture
            .publish(
                10,
                (0..200).map(|id| insert(full(id, &body(id), 0))).collect(),
            )
            .await
            .unwrap();

        let mut mutations: Vec<_> = (0..100).map(|id| update(partial(id, 1))).collect();
        mutations.push(update(partial(160, 1)));
        mutations.push(update(full(150, "replaced", 1)));
        // Push the second images for 150 and 160 into later collapse chunks.
        mutations.extend((0..100).map(|id| update(partial(id, 2))));
        mutations.push(update(partial(160, 2)));
        mutations.push(update(partial(150, 2)));
        mutations.push(update(partial(170, 1)));
        mutations.push(delete(170));
        mutations.push(insert(full(500, "inserted", 0)));
        mutations.push(update(partial(500, 9)));
        fixture.publish(20, mutations).await.unwrap();

        let rows = fixture.rows().await;
        assert_eq!(rows.len(), 200, "memory_bytes={memory_bytes}");
        for id in 0..100 {
            assert_eq!(
                rows[&id],
                full(id, &body(id), 2),
                "memory_bytes={memory_bytes}"
            );
        }
        assert_eq!(rows[&150], full(150, "replaced", 2));
        assert_eq!(rows[&160], full(160, &body(160), 2));
        assert!(!rows.contains_key(&170));
        assert_eq!(rows[&180], full(180, &body(180), 0));
        assert_eq!(rows[&500], full(500, "inserted", 9));

        // The next epoch resolves from the rows written by the previous one.
        fixture
            .publish(30, vec![update(partial(0, 3)), update(partial(150, 3))])
            .await
            .unwrap();
        let rows = fixture.rows().await;
        assert_eq!(rows[&0], full(0, &body(0), 3));
        assert_eq!(rows[&150], full(150, "replaced", 3));
        assert_eq!(rows.len(), 200);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unchanged_values_never_publish_without_a_prior_image() {
    for memory_bytes in [0, 8 << 20] {
        let mut fixture = Fixture::new(memory_bytes).await;
        fixture
            .publish(10, vec![insert(full(1, &body(1), 0))])
            .await
            .unwrap();
        // An unknown key and an insert that omits a value both fail before
        // any snapshot is created.
        for (end, mutations) in [
            (20, vec![update(partial(2, 1))]),
            (30, vec![insert(partial(3, 1))]),
        ] {
            assert!(fixture.publish(end, mutations).await.is_err());
        }
        let head = fixture
            .catalog
            .load_table(fixture.table.identifier())
            .await
            .unwrap();
        assert_eq!(head.metadata().snapshots().len(), 1);
        assert_eq!(fixture.rows().await[&1], full(1, &body(1), 0));
    }
}
