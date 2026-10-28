//! The daemon's maintenance choices with `delete_layout = "per_target"`
//! under continuous skewed CDC, driven through [`TableWork::run`] in the
//! runtime's order: each CDC epoch, then the probe or idle visit its pending
//! data maintenance schedules, then any background build's preparation and
//! activation.

use super::*;
use flow_iceberg_ext::{RowDeltaAction, SnapshotView};
use flow_materializer::DataWriter;
use flow_model::{
    FileId, JournalChunks, Mutation, MutationKind, PgLsn, TableMutationCount, TableSchemaVersion,
    Value,
};
use flow_state_store::{IndexDelta, PreparedOperation};
use futures::TryStreamExt;
use iceberg::{
    CatalogBuilder, NamespaceIdent, TableCreation,
    memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder},
    spec::{DataContentType, FormatVersion},
};

const BASE_FILES: i64 = 4;
const BASE_ROWS: i64 = 1_000;
const UPDATES: usize = 45;
const INSERTS: usize = 5;
/// CDC epochs between maintenance visits: the runtime admits a build probe
/// beside ready CDC only once a table's maintenance is a second overdue, and
/// a busy table publishes several epochs in that time.
const EPOCHS_PER_VISIT: u64 = 4;

struct Harness {
    _root: tempfile::TempDir,
    journal: flow_ingress_journal::Journal,
    work: TableWork,
    schema: TableSchema,
    ident: iceberg::TableIdent,
    expected: BTreeMap<i64, String>,
    cdf: Vec<f64>,
    rng: u64,
    lsn: u64,
    next_id: i64,
    stats: Stats,
}

#[derive(Debug, Default)]
struct Stats {
    builds: usize,
    activations: usize,
    history: Vec<Counts>,
}

/// Live position-delete files after one epoch's maintenance.
#[derive(Debug, Default, Clone, Copy)]
struct Counts {
    total: usize,
    shared: usize,
    /// Single-target files on full-size, small (outside L0) and L0 files.
    full_size: usize,
    small: usize,
    l0: usize,
    /// Full-size data files.
    full_size_files: usize,
    data_files: usize,
}

fn value(id: i64, version: u64) -> Vec<Value> {
    vec![Value::Int64(id), Value::String(format!("{id}:v{version}"))]
}

impl Harness {
    async fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let mut config: Config =
            toml::from_str(include_str!("../../../../../examples/flow.toml")).unwrap();
        config.state_dir = root.path().join("state");
        let schema = config.tables[0].schema(7);
        let id = schema.table_id;
        let control = ControlStore::open(root.path().join("control")).unwrap();
        let store = control
            .initialize_index(root.path().join("index"), StateStoreOptions::default())
            .unwrap();
        let catalog: Arc<dyn Catalog> = Arc::new(
            MemoryCatalogBuilder::default()
                .load(
                    "per-target",
                    std::collections::HashMap::from([(
                        MEMORY_CATALOG_WAREHOUSE.into(),
                        "memory://per-target".into(),
                    )]),
                )
                .await
                .unwrap(),
        );
        let namespace = NamespaceIdent::new("replicated".into());
        catalog
            .create_namespace(&namespace, std::collections::HashMap::new())
            .await
            .unwrap();
        let head = catalog
            .create_table(
                &namespace,
                TableCreation::builder()
                    .name("orders".into())
                    .format_version(FormatVersion::V2)
                    .schema(flow_materializer::iceberg_schema(&schema).unwrap())
                    .build(),
            )
            .await
            .unwrap();
        // Four full-size L2 base files; rewrite outputs below them are small.
        let expected = (0..BASE_FILES * BASE_ROWS)
            .map(|id| (id, format!("{id}:v0")))
            .collect::<BTreeMap<_, _>>();
        let rows = expected.keys().map(|id| value(*id, 0)).collect::<Vec<_>>();
        let mut files = Vec::new();
        let mut locations = Vec::new();
        for (ordinal, chunk) in rows.chunks(BASE_ROWS as usize).enumerate() {
            let mut writer = DataWriter::new(
                head.file_io().clone(),
                head.metadata().location(),
                &OperationId(format!("flow-l2-base{ordinal}")),
                schema.clone(),
                0,
                flow_materializer::WriterConfig::default(),
            )
            .unwrap();
            locations.extend(writer.write(chunk, PgLsn(80)).await.unwrap().locations);
            files.extend(writer.close().await.unwrap());
        }
        let smallest = files
            .iter()
            .map(|file| file.file_size_in_bytes())
            .min()
            .unwrap();
        let committed = RowDeltaAction::new(&head, "base-data")
            .add_data_files(files)
            .commit(catalog.as_ref(), &head)
            .await
            .unwrap();
        for location in &mut locations {
            location.data_sequence_number = committed.sequence_number;
        }
        let seed = OperationId("seed-index".into());
        store
            .prepare(
                PreparedOperation {
                    id: seed.clone(),
                    table_id: id,
                    kind: OperationKind::Ingest,
                    base_snapshot_id: None,
                    last_lsn: PgLsn(80),
                    schema_version: schema.version,
                    artifacts: vec![],
                    payload: vec![],
                },
                rows.iter()
                    .zip(locations)
                    .map(|(row, location)| IndexDelta {
                        key: schema.encode_key(row).unwrap(),
                        expected: None,
                        replacement: Some(location),
                    }),
            )
            .unwrap();
        store
            .mark_committed(&seed, committed.snapshot_id, committed.sequence_number)
            .unwrap();
        store.apply_committed(&seed).unwrap();
        store.forget_applied(&seed).unwrap();
        let head = committed.table;
        config.compaction = flow_compactor::Policy {
            delete_layout: flow_compactor::DeleteLayout::PerTarget,
            min_file_age_ms: 0,
            // L0 debt by count alone, as the daemon's age limits would.
            l0_soft_files: 8,
            l0_hard_files: 16,
            stable_small_soft_files: 24,
            oldest_l0_soft_ms: 600_000,
            oldest_l0_hard_ms: 1_200_000,
            l1_target_bytes: smallest,
            target_delete_min_file_bytes: smallest,
            l2_target_bytes: smallest * 4,
            ..config.compaction.clone()
        };
        config.validate().unwrap();
        let (journal, _) = flow_ingress_journal::Journal::open(
            root.path().join("journal"),
            flow_ingress_journal::JournalConfig::default(),
        )
        .unwrap();
        let writer = crate::services::writer_config(&config);
        let work = TableWork {
            config: Arc::new(config.clone()),
            store: store.clone(),
            control,
            reader: journal.reader(),
            catalog: catalog.clone(),
            targets: Arc::new(BTreeMap::from([(
                id,
                (head.identifier().clone(), head.metadata().uuid()),
            )])),
            publisher: Arc::new(
                TablePublisher::new(
                    store.clone(),
                    catalog.clone(),
                    writer.clone(),
                    config.limits.batch_rows,
                    config.limits.batch_bytes,
                )
                .unwrap(),
            ),
            maintenance: Arc::new(
                TableMaintenance::new(store, catalog, config.compaction.clone(), writer).unwrap(),
            ),
            garbage_checked: Arc::default(),
            metadata_backoff: Arc::default(),
            compaction: true,
        };
        // Garbage collection is not due during the run.
        work.garbage_checked
            .lock()
            .unwrap()
            .insert(id, Instant::now());
        let mut cdf = Vec::with_capacity(rows.len());
        let mut total = 0.0;
        for rank in 1..=rows.len() {
            total += (rank as f64).powf(-0.99);
            cdf.push(total);
        }
        Self {
            _root: root,
            journal,
            work,
            schema,
            ident: head.identifier().clone(),
            expected,
            cdf,
            rng: 0x9e37_79b9_7f4a_7c15,
            lsn: 100,
            next_id: BASE_FILES * BASE_ROWS,
            stats: Stats::default(),
        }
    }

    /// xorshift64*, uniform in [0, 1).
    fn uniform(&mut self) -> f64 {
        self.rng ^= self.rng >> 12;
        self.rng ^= self.rng << 25;
        self.rng ^= self.rng >> 27;
        (self.rng.wrapping_mul(0x2545_f491_4f6c_dd1d) >> 11) as f64 / (1u64 << 53) as f64
    }

    /// Journal one committed transaction: `UPDATES` distinct Zipf-drawn
    /// updates (weight rank^-0.99, hot keys are the lowest ids) and `INSERTS`.
    fn transaction(&mut self, epoch: u64) -> SourceTransaction {
        let total = *self.cdf.last().unwrap();
        let mut chosen = std::collections::BTreeSet::new();
        while chosen.len() < UPDATES {
            let draw = self.uniform() * total;
            chosen.insert(self.cdf.partition_point(|weight| *weight < draw) as i64);
        }
        let mut mutations = Vec::new();
        for id in chosen {
            let row = value(id, epoch);
            self.expected.insert(id, format!("{id}:v{epoch}"));
            mutations.push(MutationKind::Update {
                old_key: self.schema.encode_key(&row).unwrap(),
                row,
            });
        }
        for _ in 0..INSERTS {
            let id = self.next_id;
            self.next_id += 1;
            self.expected.insert(id, format!("{id}:v{epoch}"));
            mutations.push(MutationKind::Insert {
                row: value(id, epoch),
            });
        }
        let table_id = self.schema.table_id;
        let mutations = mutations
            .into_iter()
            .map(|kind| Mutation {
                table_id,
                schema_version: self.schema.version,
                kind,
            })
            .collect::<Vec<_>>();
        self.lsn += 10;
        let xid = self.lsn as u32;
        self.journal
            .append_chunk(xid, &bincode::serialize(&mutations).unwrap())
            .unwrap();
        let mutation_chunks: JournalChunks = self.journal.transaction_chunks(xid);
        let transaction = SourceTransaction {
            source_id: SourceId(self.work.config.source.id.clone()),
            xid,
            begin_lsn: PgLsn(self.lsn - 2),
            commit_lsn: PgLsn(self.lsn - 1),
            end_lsn: PgLsn(self.lsn),
            commit_timestamp_micros: 0,
            affected_tables: vec![table_id],
            schema_versions: vec![TableSchemaVersion {
                table_id,
                version: self.schema.version,
            }],
            table_mutation_counts: Some(vec![TableMutationCount {
                table_id,
                mutations: mutations.len() as u64,
            }]),
            mutation_chunks,
        };
        self.journal.commit(transaction.clone()).unwrap();
        transaction
    }

    async fn visit(
        &self,
        transactions: Vec<SourceTransaction>,
        build_admission: BuildAdmission,
        candidate: Option<CompactionCandidate>,
    ) -> TableOutcome {
        self.work
            .clone()
            .run(
                self.schema.clone(),
                transactions,
                WorkOptions {
                    periodic_maintenance: false,
                    build_active: false,
                    build_admission,
                    actor_acquired_at: Instant::now(),
                },
                candidate,
                None,
                PrefoldLink::default(),
            )
            .await
            .unwrap()
            .outcome
    }

    /// Finish a background build as the runtime does: join the worker, hand
    /// preparation off at a table boundary, then activate at the next one.
    async fn finish(&mut self, build: StartedBuild) {
        self.stats.builds += 1;
        let StartedBuild {
            running,
            path,
            started,
        } = build;
        let ready = running.wait().await.unwrap();
        let candidate = CompactionCandidate::Built(CompletedBuild {
            ready,
            path,
            started,
        });
        let TableOutcome::Preparation(preparation) = self
            .visit(Vec::new(), BuildAdmission::Fenced, Some(candidate))
            .await
        else {
            panic!("a completed build hands off preparation");
        };
        let StartedPreparation {
            running,
            path,
            started,
            activation_stall_started,
        } = *preparation;
        let prepared = running.wait().await.unwrap();
        let candidate = CompactionCandidate::Prepared(CompletedPreparation {
            prepared,
            path,
            started,
            activation_stall_started,
        });
        match self
            .visit(Vec::new(), BuildAdmission::Fenced, Some(candidate))
            .await
        {
            TableOutcome::Complete { snapshot, .. } => {
                self.stats.activations += usize::from(snapshot.is_some());
            }
            TableOutcome::CandidateInvalidated => {}
            _ => panic!("unexpected activation outcome"),
        }
    }

    /// One CDC epoch. Returns whether it left data maintenance pending.
    async fn epoch(&mut self, epoch: u64) -> bool {
        let transaction = self.transaction(epoch);
        let TableOutcome::Complete {
            operation: Some(_),
            maintenance_pending,
            ..
        } = self
            .visit(vec![transaction], BuildAdmission::Wait, None)
            .await
        else {
            panic!("the epoch publishes");
        };
        maintenance_pending.data
    }

    /// Pending data maintenance: a build probe beside ready CDC, or every
    /// third visit an idle one.
    async fn maintain(&mut self, visit: u64) {
        let admission = if visit.is_multiple_of(3) {
            BuildAdmission::Start
        } else {
            BuildAdmission::Probe
        };
        match self.visit(Vec::new(), admission, None).await {
            TableOutcome::Build(build) => self.finish(*build).await,
            TableOutcome::ProbeComplete | TableOutcome::Complete { .. } => {}
            _ => panic!("unexpected maintenance outcome"),
        }
    }

    async fn head(&self) -> Table {
        self.work.catalog.load_table(&self.ident).await.unwrap()
    }

    async fn counts(&self) -> Counts {
        let head = self.head().await;
        let view = SnapshotView::current(&head).await.unwrap();
        let files = self
            .work
            .maintenance
            .inventory(&head, self.schema.table_id)
            .await
            .unwrap()
            .files
            .into_iter()
            .map(|file| (file.id.clone(), file))
            .collect::<BTreeMap<_, _>>();
        let policy = &self.work.config.compaction;
        let mut counts = Counts {
            full_size_files: files
                .values()
                .filter(|file| policy.full_size_target(file))
                .count(),
            data_files: files.len(),
            ..Counts::default()
        };
        for entry in view.live_files.values() {
            if entry.content_type() != DataContentType::PositionDeletes {
                continue;
            }
            counts.total += 1;
            match view
                .single_target(entry)
                .map(|data| &files[&FileId(data.file_path().to_owned())])
            {
                None => counts.shared += 1,
                Some(data) if policy.full_size_target(data) => counts.full_size += 1,
                Some(data) if data.level == flow_compactor::Level::L0 => counts.l0 += 1,
                Some(_) => counts.small += 1,
            }
        }
        counts
    }

    /// Every row scans once with its latest value and the index is settled.
    async fn check(&self) {
        let head = self.head().await;
        let mut stream = head.scan().build().unwrap().to_arrow().await.unwrap();
        let mut rows = BTreeMap::new();
        while let Some(batch) = stream.try_next().await.unwrap() {
            for row in flow_materializer::rows_from_batch(&self.schema, &batch).unwrap() {
                let (Value::Int64(id), Value::String(status)) = (&row[0], &row[1]) else {
                    panic!("unexpected row {row:?}");
                };
                assert!(rows.insert(*id, status.clone()).is_none(), "duplicate {id}");
            }
        }
        assert_eq!(rows, self.expected);
        let indexed = self.work.store.table_state(&self.schema.table_id).unwrap();
        assert_eq!(indexed.pending_operation, None);
        assert_eq!(indexed.snapshot_id, head.metadata().current_snapshot_id());
        let view = SnapshotView::current(&head).await.unwrap();
        for id in self.expected.keys() {
            let location = self
                .work
                .store
                .lookup(
                    &self.schema.table_id,
                    &self.schema.encode_key(&value(*id, 0)).unwrap(),
                )
                .unwrap()
                .unwrap();
            assert!(
                view.live_files.contains_key(&location.data_file_id.0),
                "{id}"
            );
        }
    }
}

/// Under continuous Zipf-skewed CDC, the daemon's own maintenance choices
/// keep the per-target layout's delete files bounded. After each maintenance
/// visit, four CDC epochs apart, the table keeps fewer than
/// `delete_files_soft` shared files (pooled ranges included), fewer than
/// `small_file_target_deletes` single-target files on small and L0 data
/// files, and fewer than `target_delete_files_max` plus one per full-size
/// data file on full-size ones. Consolidation no longer takes every visit:
/// background builds run on nearly all of them. Rows and index stay exact.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn per_target_delete_files_stay_bounded_through_daemon_maintenance() {
    let mut harness = Harness::new().await;
    let mut pending = false;
    for epoch in 1..=160 {
        pending |= harness.epoch(epoch).await;
        if !epoch.is_multiple_of(EPOCHS_PER_VISIT) {
            continue;
        }
        if pending {
            harness.maintain(epoch / EPOCHS_PER_VISIT).await;
            pending = false;
        }
        let counts = harness.counts().await;
        harness.stats.history.push(counts);
        if epoch.is_multiple_of(40) {
            harness.check().await;
        }
    }
    harness.check().await;
    let stats = &harness.stats;
    let max = |field: fn(&Counts) -> usize| stats.history.iter().map(field).max().unwrap();
    let mean = stats
        .history
        .iter()
        .map(|counts| counts.total)
        .sum::<usize>() as f64
        / stats.history.len() as f64;
    eprintln!(
        "delete files mean {mean:.1}, max {} (shared {}, full-size {}, small {}, L0 {}); \
         {} builds, {} activations; full-size data files at most {}, data files at most {}",
        max(|counts| counts.total),
        max(|counts| counts.shared),
        max(|counts| counts.full_size),
        max(|counts| counts.small),
        max(|counts| counts.l0),
        stats.builds,
        stats.activations,
        max(|counts| counts.full_size_files),
        max(|counts| counts.data_files),
    );
    let policy = &harness.work.config.compaction;
    for counts in &stats.history {
        assert!(counts.shared < policy.delete_files_soft, "{counts:?}");
        assert!(
            counts.small + counts.l0 < policy.small_file_target_deletes,
            "{counts:?}"
        );
        // The rewrite that follows a consolidation adds at most one residual
        // per full-size data file before the next consolidation merges.
        assert!(
            counts.full_size < policy.target_delete_files_max + counts.full_size_files,
            "{counts:?}"
        );
        assert!(
            counts.total
                < policy.delete_files_soft
                    + policy.small_file_target_deletes
                    + policy.target_delete_files_max
                    + counts.full_size_files,
            "{counts:?}"
        );
    }
    assert!(mean < 20.0, "{mean}");
    // Background builds ran on most visits instead of yielding to
    // consolidation.
    assert!(
        stats.activations * 4 >= stats.history.len() * 3,
        "{stats:?}"
    );
}
