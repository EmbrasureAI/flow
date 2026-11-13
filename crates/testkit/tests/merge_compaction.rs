//! Rewrites that merge sorted inputs with overlapping key ranges: sorted and
//! stamped output, late changes caught up through per-input remaps, the
//! index exact, and every fallback to concatenation.

use flow_compactor::Policy;
use flow_coordinator::{DeleteRewritePolicy, ReplanRequired, TableMaintenance, TablePublisher};
use flow_iceberg_ext::{RowDeltaAction, SnapshotView};
use flow_materializer::{DataWriter, SortKey, SortMode, SortSpec, WriterConfig, iceberg_schema};
use flow_model::{FileId, OperationId, PgLsn, Row, SourceId, TableSchema, Value};
use flow_state_store::{
    Change, IndexDelta, OperationKind, PreparedOperation, StateStore, StateStoreOptions,
};
use flow_testkit::{LostResponseCatalog, catalog, scan, schema};
use iceberg::{
    Catalog, NamespaceIdent, TableCreation,
    spec::{DataContentType, FormatVersion, NullOrder},
    table::Table,
    transaction::{ApplyTransactionAction, Transaction},
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use tempfile::TempDir;

/// Buffered epochs are written in the table's sort order and stamped.
const BUFFERED: usize = 8 << 20;

fn row(id: i64, value: &str) -> Row {
    vec![Value::Int64(id), Value::String(value.into())]
}

fn id_of(row: &Row) -> i64 {
    match row[0] {
        Value::Int64(id) => id,
        _ => unreachable!(),
    }
}

fn policy() -> Policy {
    Policy {
        l0_soft_files: 2,
        l0_hard_files: 4,
        oldest_l0_soft_ms: 1,
        oldest_l0_hard_ms: 2,
        deleted_rows_percent: 100,
        min_file_age_ms: 0,
        lazy_index_remap: true,
        sort_merge: true,
        ..Default::default()
    }
}

/// One live data file: its stamp, Parquet sorting columns and ids in file order.
#[derive(Debug)]
struct LiveFile {
    path: String,
    order: Option<i32>,
    sorting_columns: usize,
    ids: Vec<i64>,
}

struct Fixture {
    temp: TempDir,
    catalog: Arc<LostResponseCatalog>,
    index: StateStore,
    schema: TableSchema,
    head: Table,
    /// Every live row by id, as the source has it.
    model: BTreeMap<i64, Row>,
    lsn: u64,
    scratches: usize,
}

impl Fixture {
    async fn new(version: FormatVersion) -> Self {
        let temp = TempDir::new().unwrap();
        let catalog = Arc::new(LostResponseCatalog::new(
            catalog(&temp.path().join("warehouse")).await,
        ));
        let schema = schema(1);
        let head = catalog
            .create_table(
                &NamespaceIdent::new("test".into()),
                TableCreation::builder()
                    .name("merged".into())
                    .format_version(version)
                    .schema(iceberg_schema(&schema).unwrap())
                    .build(),
            )
            .await
            .unwrap();
        let index = StateStore::open(
            temp.path().join("index"),
            StateStoreOptions {
                apply_batch_rows: 3,
                ..Default::default()
            },
        )
        .unwrap();
        let mut fixture = Self {
            temp,
            catalog,
            index,
            schema,
            head,
            model: BTreeMap::new(),
            lsn: 10,
            scratches: 0,
        };
        fixture.order_by(&[("id", false)]).await;
        fixture
    }

    /// Replace the table's default sort order, as `WRITE ORDERED BY` does.
    async fn order_by(&mut self, fields: &[(&str, bool)]) {
        let transaction = Transaction::new(&self.head);
        let mut action = transaction.replace_sort_order();
        for (name, descending) in fields {
            action = if *descending {
                action.desc(name, NullOrder::Last)
            } else {
                action.asc(name, NullOrder::First)
            };
        }
        self.head = action
            .apply(transaction)
            .unwrap()
            .commit(self.catalog.as_ref())
            .await
            .unwrap();
    }

    fn scratch(&mut self) -> StateStore {
        self.scratches += 1;
        StateStore::open(
            self.temp.path().join(format!("scratch-{}", self.scratches)),
            StateStoreOptions {
                apply_batch_rows: 3,
                ..Default::default()
            },
        )
        .unwrap()
    }

    fn maintenance(&self, policy: Policy) -> TableMaintenance {
        TableMaintenance::new(
            self.index.clone(),
            self.catalog.clone(),
            policy,
            WriterConfig::default(),
        )
        .unwrap()
    }

    async fn refresh(&mut self) {
        self.head = self
            .catalog
            .load_table(self.head.identifier())
            .await
            .unwrap();
    }

    /// Publish one epoch of CDC and apply it to the model.
    async fn cdc(&mut self, changes: Vec<(i64, Change)>, memory_bytes: usize) {
        self.lsn += 10;
        for (id, change) in &changes {
            match change {
                Change::Insert(row) | Change::Update(row) => {
                    self.model.insert(*id, row.clone());
                }
                Change::Delete => {
                    self.model.remove(id);
                }
            }
        }
        let (epoch, collapsed) = flow_testkit::collapse_fixture_with(
            &self.index,
            &self.head,
            &self.schema,
            SourceId("fixture".into()),
            PgLsn(self.lsn),
            changes
                .into_iter()
                .map(|(id, change)| (self.schema.encode_key(&row(id, "")).unwrap(), change)),
            memory_bytes,
        );
        TablePublisher::new(
            self.index.clone(),
            self.catalog.clone(),
            WriterConfig::default(),
            2,
            1 << 20,
        )
        .unwrap()
        .publish(&self.head, &self.schema, collapsed)
        .await
        .unwrap();
        self.index.forget_applied(&epoch.id).unwrap();
        self.refresh().await;
    }

    async fn insert(&mut self, ids: impl IntoIterator<Item = i64>, value: &str) {
        let changes = ids
            .into_iter()
            .map(|id| (id, Change::Insert(row(id, &format!("{value}-{id}")))))
            .collect();
        self.cdc(changes, BUFFERED).await;
    }

    /// Commit and index a file written outside Flow's publication path: its
    /// rows are sorted by `value`, but it carries the table's `id` order ID,
    /// as a faulty external writer might.
    async fn commit_misstamped(&mut self, rows: &[(i64, &str)]) -> FileId {
        let order_id = self.head.metadata().default_sort_order_id();
        let lying = SortSpec {
            order_id,
            keys: vec![SortKey {
                column: 1,
                descending: false,
                nulls_first: true,
            }],
        };
        let rows: Vec<Row> = rows.iter().map(|(id, value)| row(*id, value)).collect();
        self.lsn += 10;
        // Named like an ingest file, so planning groups it with them.
        let operation = format!("flow-l0-misstamped-{}", self.lsn);
        let mut writer = DataWriter::new(
            self.head.file_io().clone(),
            self.head.metadata().location(),
            &OperationId(operation.clone()),
            self.schema.clone(),
            0,
            WriterConfig::default(),
        )
        .unwrap()
        .with_sort(&lying, SortMode::Verify)
        .unwrap();
        let locations = writer
            .write(&rows, PgLsn(self.lsn))
            .await
            .unwrap()
            .locations;
        let files = writer.close().await.unwrap();
        assert_eq!(files[0].sort_order_id(), Some(order_id as i32));
        let file = FileId(files[0].file_path().into());
        let base = self.head.metadata().current_snapshot_id();
        let committed = RowDeltaAction::new(&self.head, &operation)
            .add_data_files(files)
            .commit(self.catalog.as_ref(), &self.head)
            .await
            .unwrap();
        let id = OperationId(operation);
        self.index
            .prepare(
                PreparedOperation {
                    id: id.clone(),
                    table_id: self.schema.table_id,
                    kind: OperationKind::Ingest,
                    base_snapshot_id: base,
                    last_lsn: PgLsn(self.lsn),
                    schema_version: 0,
                    artifacts: vec![],
                    payload: vec![],
                },
                rows.iter()
                    .zip(locations)
                    .map(|(row, location)| IndexDelta {
                        key: self.schema.encode_key(row).unwrap(),
                        expected: None,
                        replacement: Some(location),
                    }),
            )
            .unwrap();
        self.index
            .mark_committed(&id, committed.snapshot_id, committed.sequence_number)
            .unwrap();
        self.index.apply_committed(&id).unwrap();
        self.index.forget_applied(&id).unwrap();
        for row in rows {
            self.model.insert(id_of(&row), row);
        }
        self.refresh().await;
        file
    }

    /// Checks Iceberg's rows against the model with two independent readers
    /// and the index against every live row; returns the live data files.
    async fn audit(&mut self) -> Vec<LiveFile> {
        let mut expected: Vec<Row> = self.model.values().cloned().collect();
        let mut scanned = scan(&self.head, &self.schema).await.unwrap();
        scanned.sort_by_key(id_of);
        assert_eq!(
            scanned, expected,
            "iceberg-rust scan differs from the source"
        );

        let view = SnapshotView::current(&self.head).await.unwrap();
        let files: BTreeSet<_> = view
            .live_files
            .values()
            .filter(|entry| entry.content_type() == DataContentType::Data)
            .map(|entry| FileId(entry.file_path().into()))
            .collect();
        let scratch = self.scratch();
        let mut live = Vec::new();
        flow_compactor::scan_live_files(
            &self.head,
            &self.schema,
            &view,
            &files,
            &scratch,
            "audit",
            3,
            &flow_compactor::ReadLimits::default(),
            async |batch| {
                for (row, position) in batch.rows.into_iter().zip(batch.positions) {
                    live.push((row, batch.file_id.clone(), position));
                }
                Ok(())
            },
        )
        .await
        .unwrap();
        let mut rows: Vec<Row> = live.iter().map(|(row, _, _)| row.clone()).collect();
        rows.sort_by_key(id_of);
        assert_eq!(rows, expected, "Flow's live scan differs from the source");

        let keys: Vec<_> = live
            .iter()
            .map(|(row, _, _)| self.schema.encode_key(row).unwrap())
            .collect();
        let located = self
            .index
            .lookup_many(&self.schema.table_id, &keys)
            .unwrap();
        for ((row, file, position), location) in live.iter().zip(located) {
            let location = location.unwrap_or_else(|| panic!("{} is not indexed", id_of(row)));
            assert_eq!(
                (&location.data_file_id, location.row_position),
                (file, *position),
                "{} is indexed at a different row",
                id_of(row)
            );
            assert_eq!(
                Some(location.data_sequence_number),
                view.live_files[&file.0].sequence_number
            );
        }
        let ordered: Vec<_> = files.iter().cloned().collect();
        let counts = self
            .index
            .file_live_row_counts(
                &self.schema.table_id,
                self.head.metadata().current_snapshot_id(),
                &ordered,
            )
            .unwrap();
        for (file, count) in ordered.iter().zip(counts) {
            assert_eq!(
                count,
                live.iter().filter(|(_, live, _)| live == file).count() as u64,
                "live count of {}",
                file.0
            );
        }
        expected.clear();

        let mut result = Vec::new();
        for file in files {
            let bytes = self
                .head
                .file_io()
                .new_input(&file.0)
                .unwrap()
                .read()
                .await
                .unwrap();
            let builder = ParquetRecordBatchReaderBuilder::try_new(bytes).unwrap();
            let sorting_columns = builder
                .metadata()
                .row_groups()
                .first()
                .and_then(|group| group.sorting_columns())
                .map_or(0, Vec::len);
            let mut ids = Vec::new();
            for batch in builder.build().unwrap() {
                let batch = batch.unwrap();
                ids.extend(
                    flow_materializer::rows_from_batch(&self.schema, &batch)
                        .unwrap()
                        .iter()
                        .map(id_of),
                );
            }
            let order = view.live_files[&file.0].data_file.sort_order_id();
            // `commit_misstamped` writes such a file on purpose.
            if order.is_some() && !file.0.contains("-misstamped-") {
                assert!(
                    ids.is_sorted(),
                    "{} is stamped but out of order: {ids:?}",
                    file.0
                );
            }
            result.push(LiveFile {
                path: file.0,
                order,
                sorting_columns,
                ids,
            });
        }
        result
    }

    /// Each live row's `(row_id, last_updated_sequence_number)`.
    async fn lineage(&mut self) -> BTreeMap<i64, (Option<i64>, Option<i64>)> {
        let view = SnapshotView::current(&self.head).await.unwrap();
        let files: BTreeSet<_> = view
            .live_files
            .values()
            .filter(|entry| entry.content_type() == DataContentType::Data)
            .map(|entry| FileId(entry.file_path().into()))
            .collect();
        let scratch = self.scratch();
        let mut result = BTreeMap::new();
        flow_compactor::scan_live_files(
            &self.head,
            &self.schema,
            &view,
            &files,
            &scratch,
            "lineage",
            3,
            &flow_compactor::ReadLimits::default(),
            async |batch| {
                let lineage = batch.lineage.expect("v3 lineage");
                for (row, lineage) in batch.rows.iter().zip(lineage) {
                    result.insert(
                        id_of(row),
                        (lineage.row_id, lineage.last_updated_sequence_number),
                    );
                }
                Ok(())
            },
        )
        .await
        .unwrap();
        result
    }

    /// Run one strict rewrite and return whether it published.
    async fn compact(&mut self, maintenance: &TableMaintenance) -> bool {
        let head = self.head.clone();
        let scratch = self.scratch();
        let published = maintenance
            .compact(&head, &self.schema, scratch)
            .await
            .unwrap()
            .is_some();
        self.refresh().await;
        published
    }
}

/// Files written by rewrites, which Flow names after the operation.
fn rewritten(files: &[LiveFile]) -> Vec<&LiveFile> {
    files
        .iter()
        .filter(|file| file.path.contains("-rewrite-"))
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn overlapping_sorted_inputs_merge_into_one_sorted_stamped_file() {
    let mut f = Fixture::new(FormatVersion::V2).await;
    let maintenance = f.maintenance(policy());
    // Three interleaved inputs, including negative ids, which Flow's key
    // encoding orders after positive ones.
    f.insert((0..40).step_by(3), "a").await;
    f.insert((1..40).step_by(3).chain([-5, -1]), "b").await;
    f.insert((2..40).step_by(3).chain([-3]), "c").await;
    let before = f.audit().await;
    assert_eq!(before.len(), 3);
    assert!(before.iter().all(|file| file.order == Some(1)));

    assert!(f.compact(&maintenance).await);
    let after = f.audit().await;
    assert_eq!(after.len(), 1, "{after:?}");
    let merged = &after[0];
    assert_eq!(merged.order, Some(1));
    assert_eq!(
        merged.sorting_columns, 1,
        "a merge declares its sorting columns"
    );
    let mut expected: Vec<i64> = f.model.keys().copied().collect();
    expected.sort();
    assert_eq!(merged.ids, expected);

    // CDC on merged rows finds them through the remaps.
    f.cdc(
        vec![
            (-5, Change::Delete),
            (7, Change::Update(row(7, "updated"))),
            (20, Change::Delete),
            (41, Change::Insert(row(41, "d"))),
            (-2, Change::Insert(row(-2, "d"))),
        ],
        BUFFERED,
    )
    .await;
    f.audit().await;
    f.cdc(
        vec![
            (8, Change::Update(row(8, "again"))),
            (-1, Change::Delete),
            (20, Change::Insert(row(20, "re-inserted"))),
        ],
        BUFFERED,
    )
    .await;
    f.audit().await;

    // The merged file and the new L0 files merge again, through the remaps
    // of the first rewrite.
    while f.compact(&maintenance).await {}
    let files = f.audit().await;
    assert!(
        rewritten(&files).iter().all(|file| file.order == Some(1)),
        "{files:?}"
    );
}

/// A merge disabled by policy, or one whose inputs exceed its memory budget,
/// concatenates in commit order and leaves the output unstamped.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn merges_off_or_over_memory_concatenate_in_commit_order() {
    for policy in [
        Policy {
            sort_merge: false,
            ..policy()
        },
        Policy {
            merge_memory_bytes: 1,
            ..policy()
        },
    ] {
        let mut f = Fixture::new(FormatVersion::V2).await;
        let maintenance = f.maintenance(policy);
        f.insert([1, 3, 5], "a").await;
        f.insert([2, 4], "b").await;
        assert!(f.compact(&maintenance).await);
        let files = f.audit().await;
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].order, None);
        assert_eq!(files[0].ids, [1, 3, 5, 2, 4]);
    }
}

/// Output files rotate between merged batches, so each is sorted, stamped
/// and declares its sorting columns, and their key ranges are disjoint.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn merged_output_rotates_into_disjoint_sorted_files() {
    let mut f = Fixture::new(FormatVersion::V2).await;
    f.insert((0..400).step_by(2), "a").await;
    f.insert((1..400).step_by(2), "b").await;
    // Planning groups inputs until they reach the target, so a target just
    // above one input's size selects both, and their output rotates.
    let largest = SnapshotView::current(&f.head)
        .await
        .unwrap()
        .live_files
        .values()
        .map(|entry| entry.data_file.file_size_in_bytes())
        .max()
        .unwrap();
    let maintenance = f.maintenance(Policy {
        l1_target_bytes: largest + 1,
        ..policy()
    });
    assert!(f.compact(&maintenance).await);
    let files = f.audit().await;
    assert!(files.len() >= 2, "{files:?}");
    let mut files: Vec<_> = files.iter().collect();
    files.sort_by_key(|file| file.ids[0]);
    for file in &files {
        assert_eq!(file.order, Some(1));
        assert_eq!(file.sorting_columns, 1);
    }
    let ids: Vec<i64> = files.iter().flat_map(|file| file.ids.clone()).collect();
    assert_eq!(ids, (0..400).collect::<Vec<_>>(), "ranges are disjoint");
}

/// A background build merges while CDC updates, deletes and reinserts its
/// input rows; catch-up translates those late changes through the
/// interleaved per-input remaps.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn background_merge_catches_up_late_changes_to_its_inputs() {
    let mut f = Fixture::new(FormatVersion::V2).await;
    let maintenance = f.maintenance(policy());
    f.insert((0..60).step_by(2), "a").await;
    f.insert((1..60).step_by(2), "b").await;
    f.cdc(
        vec![
            (3, Change::Delete),
            (10, Change::Update(row(10, "before-build"))),
        ],
        BUFFERED,
    )
    .await;
    let head = f.head.clone();
    let scratch = f.scratch();
    let running = maintenance
        .start_compaction(&head, &f.schema, scratch)
        .await
        .unwrap()
        .unwrap();
    f.cdc(
        vec![
            (0, Change::Delete),
            (1, Change::Update(row(1, "late"))),
            (31, Change::Delete),
            (58, Change::Update(row(58, "late"))),
            (59, Change::Delete),
            (100, Change::Insert(row(100, "late"))),
        ],
        BUFFERED,
    )
    .await;
    f.cdc(
        vec![
            (31, Change::Insert(row(31, "reinserted"))),
            (2, Change::Delete),
        ],
        BUFFERED,
    )
    .await;
    let scratch = f.scratch();
    maintenance
        .compact_deletes(
            &f.head,
            &f.schema,
            scratch,
            &DeleteRewritePolicy {
                min_input_files: 2,
                ..Default::default()
            },
        )
        .await
        .unwrap()
        .unwrap();
    f.refresh().await;
    let ready = running.wait().await.unwrap();
    let head = f.head.clone();
    assert!(
        maintenance
            .finish_compaction(&head, &f.schema, ready)
            .await
            .unwrap()
            .is_some()
    );
    f.refresh().await;
    let files = f.audit().await;
    let merged = rewritten(&files);
    assert_eq!(merged.len(), 1, "{files:?}");
    assert_eq!(merged[0].order, Some(1));
    assert!(merged[0].ids.len() > 50);

    // CDC after publication finds rows the build moved.
    f.cdc(
        vec![(4, Change::Update(row(4, "after"))), (57, Change::Delete)],
        BUFFERED,
    )
    .await;
    f.audit().await;
}

/// An input stamped with the sort order whose rows are out of order fails
/// its merge with a retryable error; the retry concatenates, and the output
/// is stamped only if its rows turn out sorted.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_misstamped_input_is_demoted_and_its_rewrite_concatenates() {
    let mut f = Fixture::new(FormatVersion::V2).await;
    let maintenance = f.maintenance(policy());
    f.insert([1, 4, 6], "a").await;
    let bad = f.commit_misstamped(&[(9, "a"), (2, "b"), (5, "c")]).await;
    let before = f.audit().await;
    assert!(before.iter().all(|file| file.order == Some(1)));

    let head = f.head.clone();
    let scratch = f.scratch();
    let error = maintenance
        .compact(&head, &f.schema, scratch)
        .await
        .unwrap_err();
    assert!(
        error.downcast_ref::<ReplanRequired>().is_some(),
        "{error:#}"
    );
    let out_of_order = error
        .downcast_ref::<flow_compactor::InputOutOfOrder>()
        .unwrap();
    assert_eq!(out_of_order.file, bad);
    let pending = f
        .index
        .table_state(&f.schema.table_id)
        .unwrap()
        .pending_operation;
    // The daemon discards a failed build's fence, as here, and replans.
    if let Some(pending) = pending {
        f.index.discard_uncommitted(&pending).unwrap();
        f.index.discard_transaction(&pending.0).unwrap();
    }
    f.refresh().await;
    assert_eq!(
        f.head.metadata().current_snapshot_id(),
        head.metadata().current_snapshot_id()
    );
    f.audit().await;

    assert!(f.compact(&maintenance).await);
    let files = f.audit().await;
    assert_eq!(files.len(), 1);
    assert_eq!(files[0].order, None);
    assert_eq!(files[0].ids, [1, 4, 6, 9, 2, 5], "commit order");
}

/// Format v3: a merge keeps every row's lineage, gathered with the row.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn v3_merges_keep_row_lineage() {
    let mut f = Fixture::new(FormatVersion::V3).await;
    let maintenance = f.maintenance(policy());
    f.insert((0..20).step_by(2), "a").await;
    f.insert((1..20).step_by(2), "b").await;
    f.cdc(vec![(5, Change::Update(row(5, "updated")))], BUFFERED)
        .await;
    let before = f.lineage().await;
    assert!(
        before
            .values()
            .all(|(id, seq)| id.is_some() && seq.is_some())
    );
    assert!(f.compact(&maintenance).await);
    let files = f.audit().await;
    assert_eq!(files.len(), 1);
    assert_eq!(files[0].order, Some(1));
    assert_eq!(files[0].ids, (0..20).collect::<Vec<_>>());
    assert_eq!(f.lineage().await, before);
}

/// A small deterministic generator, so failures reproduce from the seed.
struct Random(u64);

impl Random {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn below(&mut self, bound: u64) -> u64 {
        self.next() % bound
    }
}

/// Random CDC with interleaved keys, buffered and disk epochs, and strict
/// and background rewrites with late changes: rows, index and every stamp
/// stay exact.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn random_cdc_and_merges_keep_rows_index_and_order_exact() {
    let seeds: u64 = std::env::var("FLOW_MERGE_SEEDS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(4);
    // An unstamped disk epoch keeps a group from merging, so some seeds may
    // never merge; across seeds most groups do.
    let (mut merged, mut rewrites) = (0, 0);
    for seed in 1..=seeds {
        let mut random = Random(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15));
        let mut f = Fixture::new(FormatVersion::V2).await;
        let maintenance = f.maintenance(Policy {
            // Small targets group a few inputs and rotate their output.
            l1_target_bytes: if seed % 2 == 0 { 4 << 10 } else { 128 << 20 },
            ..policy()
        });
        let epoch = |f: &mut Fixture, random: &mut Random| {
            let mut changes = BTreeMap::new();
            for _ in 0..random.below(24) + 1 {
                let id = random.below(200) as i64 - 50;
                let change = match (f.model.contains_key(&id), random.below(3)) {
                    (false, _) => Change::Insert(row(id, &format!("v{}", random.below(1000)))),
                    (true, 0) => Change::Delete,
                    (true, _) => Change::Update(row(id, &format!("u{}", random.below(1000)))),
                };
                changes.entry(id).or_insert(change);
            }
            // Disk-collapsed epochs with negative ids are left unstamped.
            let memory = if random.below(4) == 0 { 0 } else { BUFFERED };
            (changes.into_iter().collect::<Vec<_>>(), memory)
        };
        for step in 0..10 {
            let (changes, memory) = epoch(&mut f, &mut random);
            f.cdc(changes, memory).await;
            if step % 3 != 2 {
                continue;
            }
            if random.below(2) == 0 {
                f.compact(&maintenance).await;
            } else {
                let head = f.head.clone();
                let scratch = f.scratch();
                let Some(running) = maintenance
                    .start_compaction(&head, &f.schema, scratch)
                    .await
                    .unwrap()
                else {
                    continue;
                };
                for _ in 0..random.below(3) {
                    let (changes, memory) = epoch(&mut f, &mut random);
                    f.cdc(changes, memory).await;
                }
                let ready = running.wait().await.unwrap();
                let head = f.head.clone();
                maintenance
                    .finish_compaction(&head, &f.schema, ready)
                    .await
                    .unwrap();
                f.refresh().await;
            }
            let files = f.audit().await;
            let outputs = rewritten(&files);
            rewrites += outputs.len();
            merged += outputs
                .iter()
                .filter(|file| file.sorting_columns > 0)
                .count();
        }
    }
    println!("{merged} of {rewrites} live rewrite outputs were merged");
    assert!(merged > 0, "no rewrite merged");
}

/// A committed interleaved remap must recover through response loss and a
/// reopened index, including late deletes translated during catch-up.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn merged_remaps_recover_after_lost_catalog_response_and_reopen() {
    for version in [FormatVersion::V2, FormatVersion::V3] {
        let mut f = Fixture::new(version).await;
        f.insert((0..60).step_by(2), "a").await;
        f.insert((1..60).step_by(2), "b").await;
        f.cdc(vec![(2, Change::Delete)], BUFFERED).await;
        let maintenance = f.maintenance(policy());
        let scratch = f.scratch();
        let running = maintenance
            .start_compaction(&f.head, &f.schema, scratch)
            .await
            .unwrap()
            .unwrap();
        f.cdc(
            vec![(3, Change::Delete), (8, Change::Update(row(8, "late")))],
            BUFFERED,
        )
        .await;
        let ready = running.wait().await.unwrap();
        if version == FormatVersion::V2 {
            let scratch = f.scratch();
            maintenance
                .compact_deletes(
                    &f.head,
                    &f.schema,
                    scratch,
                    &DeleteRewritePolicy {
                        min_input_files: 2,
                        ..Default::default()
                    },
                )
                .await
                .unwrap()
                .unwrap();
            f.refresh().await;
        }
        f.catalog.lose_next_response_and_disconnect();
        assert!(
            maintenance
                .finish_compaction(&f.head, &f.schema, ready)
                .await
                .is_err()
        );
        let pending = f.index.pending_operations().unwrap().remove(0);
        assert_eq!(pending.phase, flow_state_store::OperationPhase::Prepared);
        assert_eq!(pending.delta_count, 0, "merge stages remaps");
        drop(maintenance);
        drop(f.index);
        f.catalog.reconnect();
        f.index = StateStore::open(
            f.temp.path().join("index"),
            StateStoreOptions {
                apply_batch_rows: 3,
                ..Default::default()
            },
        )
        .unwrap();
        f.refresh().await;
        let snapshots = f.head.metadata().snapshots().len();
        f.maintenance(policy())
            .recover(&f.head, &pending.operation.id)
            .await
            .unwrap();
        f.refresh().await;
        assert_eq!(f.head.metadata().snapshots().len(), snapshots);
        assert!(f.index.has_remaps(&f.schema.table_id).unwrap());
        let files = f.audit().await;
        let merged = rewritten(&files);
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].order, Some(1));
        f.cdc(
            vec![
                (4, Change::Update(row(4, "after-recovery"))),
                (5, Change::Delete),
            ],
            BUFFERED,
        )
        .await;
        f.audit().await;
    }
}
