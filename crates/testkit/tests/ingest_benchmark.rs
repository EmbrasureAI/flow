//! Per-stage timing of the CDC ingest cycle over local storage: journal replay
//! and collapse, Parquet and index/delete staging, a catalog commit and index
//! apply. The journal, row index, publisher and Iceberg files are real; the
//! catalog is in memory and PostgreSQL, MinIO and REST are absent.
//!
//! ```sh
//! cargo test --release -p flow-testkit --test ingest_benchmark -- --ignored --nocapture
//! ```
//!
//! Each epoch folds a fixed number of mutations from 100-row source
//! transactions: 70% updates, 20% inserts and 10% deletes over uniformly
//! chosen live keys, as in `tests/production/benchmark.py`. Settings are read
//! from `INGEST_BENCH_*` variables (see [`Settings`]); `INGEST_BENCH_OUTPUT`
//! names a JSON file for the per-epoch samples. The final Iceberg rows and every
//! indexed key are checked against the generator, so a change that breaks
//! publication cannot report a faster epoch.

use flow_coordinator::{CollapseLimits, Epoch, Prefold, TablePublisher, collapse_epoch_prefolded};
use flow_iceberg_ext::SnapshotView;
use flow_ingress_journal::{Journal, JournalConfig, JournalReader};
use flow_materializer::{WriterConfig, iceberg_schema, rows_from_batch};
use flow_model::{
    Column, ColumnType, FileId, Mutation, MutationKind, PgLsn, PrimaryKey, Row, SourceId,
    SourceTransaction, TableId, TableMutationCount, TableSchema, TableSchemaVersion, Value,
};
use flow_state_store::{ControlStore, StateStore, StateStoreOptions};
use futures::TryStreamExt;
use iceberg::{
    Catalog, NamespaceIdent, TableCreation,
    spec::{DataContentType, FormatVersion},
    table::Table,
};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, OnceLock},
    time::{Duration, Instant},
};
use tempfile::TempDir;
use tracing::field::{Field, Visit};
use tracing_subscriber::{Layer, layer::SubscriberExt};

/// Benchmark shape. Defaults match the production harness's single-table
/// 50k/s case: an epoch of about 113,000 mutations over a 100,000-key table.
#[derive(Debug, Clone, serde::Serialize)]
struct Settings {
    tables: usize,
    /// Concurrent table epochs, like the daemon's `--table-workers`.
    workers: usize,
    epochs: usize,
    /// Leading epochs excluded from the medians.
    warmup_epochs: usize,
    mutations_per_epoch: usize,
    initial_keys: usize,
    row_bytes: usize,
    transaction_rows: usize,
    collapse_memory_bytes: usize,
    batch_rows: usize,
    batch_bytes: usize,
    seed: u64,
    /// Percent of each next epoch's leading transactions folded while the
    /// current epoch commits and applies, as the daemon's prefold does with
    /// whatever has arrived by then; zero collapses every epoch cold.
    prefold_percent: usize,
}

fn env(name: &str, default: usize) -> usize {
    std::env::var(format!("INGEST_BENCH_{name}"))
        .ok()
        .map(|value| value.parse().expect("numeric benchmark setting"))
        .unwrap_or(default)
}

impl Settings {
    fn from_env(tables: usize, mutations_per_epoch: usize) -> Self {
        Self {
            tables: env("TABLES", tables),
            workers: env("WORKERS", 4),
            epochs: env("EPOCHS", 12),
            warmup_epochs: env("WARMUP_EPOCHS", 2),
            mutations_per_epoch: env("MUTATIONS", mutations_per_epoch),
            initial_keys: env("KEYS", 100_000),
            row_bytes: env("ROW_BYTES", 256),
            transaction_rows: env("TRANSACTION_ROWS", 100),
            // Daemon defaults: `batch_rows`, `batch_bytes`, `collapse_memory_bytes`.
            collapse_memory_bytes: env("COLLAPSE_MEMORY_BYTES", 128 << 20),
            batch_rows: env("BATCH_ROWS", 1024),
            batch_bytes: env("BATCH_BYTES", 8 << 20),
            seed: env("SEED", 20_260_905) as u64,
            prefold_percent: env("PREFOLD_PERCENT", 0),
        }
    }
}

/// The benchmark table of `tests/production/benchmark.py`.
fn schema(table: u32) -> TableSchema {
    let column = |field_id, name: &str, data_type| Column {
        field_id,
        name: name.into(),
        data_type,
        nullable: false,
    };
    TableSchema {
        table_id: TableId(table),
        version: 0,
        columns: vec![
            column(1, "id", ColumnType::Int64),
            column(2, "version", ColumnType::Int64),
            column(3, "payload", ColumnType::String),
        ],
        primary_key: vec![0],
        append_only: false,
    }
}

/// Deterministic, compressible payloads, distinct per row and version, like
/// the harness's repeated-MD5 text.
fn row(id: i64, version: i64, bytes: usize) -> Row {
    let block = format!(
        "{:016x}{:016x}",
        (id as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15),
        (version as u64) ^ 0xd1b5_4a32_d192_ed03
    );
    vec![
        Value::Int64(id),
        Value::Int64(version),
        Value::String(block.repeat(bytes.div_ceil(block.len()))[..bytes].into()),
    ]
}

/// Source state for one table: live keys, their versions and the next new key.
struct Model {
    schema: TableSchema,
    live: Vec<i64>,
    versions: HashMap<i64, i64>,
    next_id: i64,
    rng: u64,
}
impl Model {
    fn new(table: u32, keys: usize, seed: u64) -> Self {
        Self {
            schema: schema(table),
            live: (1..=keys as i64).collect(),
            versions: (1..=keys as i64).map(|id| (id, 0)).collect(),
            next_id: keys as i64 + 1,
            rng: seed ^ u64::from(table).wrapping_mul(0x2545_f491_4f6c_dd1d) | 1,
        }
    }

    fn next(&mut self, bound: usize) -> usize {
        self.rng ^= self.rng << 13;
        self.rng ^= self.rng >> 7;
        self.rng ^= self.rng << 17;
        (self.rng % bound as u64) as usize
    }

    /// One source transaction: 70% updates, 20% inserts and 10% deletes.
    fn transaction(&mut self, rows: usize, row_bytes: usize) -> Vec<Mutation> {
        let kinds = (0..rows).map(|index| match index * 10 / rows {
            0..=6 => 0,
            7 | 8 => 1,
            _ => 2,
        });
        let mut mutations = Vec::with_capacity(rows);
        for kind in kinds.collect::<Vec<_>>() {
            let kind = match kind {
                0 => {
                    let index = self.next(self.live.len());
                    let id = self.live[index];
                    let version = self.versions.get_mut(&id).expect("live key");
                    *version += 1;
                    let row = row(id, *version, row_bytes);
                    MutationKind::Update {
                        old_key: self.schema.encode_key(&row).unwrap(),
                        row,
                    }
                }
                1 => {
                    let id = self.next_id;
                    self.next_id += 1;
                    self.live.push(id);
                    self.versions.insert(id, 0);
                    MutationKind::Insert {
                        row: row(id, 0, row_bytes),
                    }
                }
                _ => {
                    let index = self.next(self.live.len());
                    let id = self.live.swap_remove(index);
                    self.versions.remove(&id);
                    MutationKind::Delete { key: self.key(id) }
                }
            };
            mutations.push(Mutation {
                table_id: self.schema.table_id,
                schema_version: 0,
                kind,
            });
        }
        mutations
    }

    fn key(&self, id: i64) -> PrimaryKey {
        self.schema.encode_key(&row(id, 0, 0)).unwrap()
    }

    fn expected(&self) -> Vec<(i64, i64)> {
        let mut rows: Vec<_> = self.versions.iter().map(|(id, v)| (*id, *v)).collect();
        rows.sort_unstable();
        rows
    }
}

/// Appends whole source transactions with strictly increasing positions.
struct Source {
    journal: Journal,
    id: SourceId,
    position: u64,
    xid: u32,
}
impl Source {
    fn append(
        &mut self,
        table: TableId,
        mutations: &[Mutation],
        initial: bool,
    ) -> SourceTransaction {
        self.xid += 1;
        self.position += 3;
        for chunk in mutations.chunks(1024) {
            self.journal
                .append_chunk(self.xid, &bincode::serialize(chunk).unwrap())
                .unwrap();
        }
        let transaction = SourceTransaction {
            source_id: self.id.clone(),
            xid: if initial { 0 } else { self.xid },
            begin_lsn: PgLsn(self.position - 2),
            commit_lsn: PgLsn(self.position - 1),
            end_lsn: PgLsn(self.position),
            commit_timestamp_micros: 1_700_000_000_000_000 + self.position as i64,
            schema_versions: vec![TableSchemaVersion {
                table_id: table,
                version: 0,
            }],
            affected_tables: vec![table],
            mutation_chunks: self.journal.transaction_chunks(self.xid),
            table_mutation_counts: Some(vec![TableMutationCount {
                table_id: table,
                mutations: mutations.len() as u64,
            }]),
        };
        let mut staged = transaction.clone();
        // The journal keys open chunks by local XID; initial-copy terminals
        // carry XID 0 only in the descriptor collapse sees.
        staged.xid = self.xid;
        self.journal.stage_commit(&staged).unwrap();
        if self.xid.is_multiple_of(256) {
            self.journal.flush_commits().unwrap();
        }
        transaction
    }
}

/// Stage timings reported by the publisher's `flow_events`, by operation ID.
/// Preparation ends at `ingest_prepared`; the catalog commit at `table_published`.
#[derive(Default, Debug, Clone, Copy)]
struct Reported {
    prepare_ms: f64,
    index_delete_stage_ms: f64,
    data_write_ms: f64,
    delete_write_ms: f64,
    prepared_at: Option<Instant>,
    published_at: Option<Instant>,
}

#[derive(Default)]
struct EventFields {
    event: String,
    operation: String,
    numbers: HashMap<&'static str, f64>,
}
impl Visit for EventFields {
    fn record_f64(&mut self, field: &Field, value: f64) {
        self.numbers.insert(field.name(), value);
    }
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "event" {
            self.event = value.into();
        }
    }
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "operation_id" {
            self.operation = format!("{value:?}");
        }
    }
}

struct StageEvents(Arc<Mutex<HashMap<String, Reported>>>);
impl<S: tracing::Subscriber> Layer<S> for StageEvents {
    fn on_event(&self, event: &tracing::Event<'_>, _: tracing_subscriber::layer::Context<'_, S>) {
        if event.metadata().target() != "flow_events" {
            return;
        }
        let mut fields = EventFields::default();
        event.record(&mut fields);
        let number = |name| fields.numbers.get(name).copied().unwrap_or_default();
        let mut reported = self.0.lock().unwrap();
        let entry = reported.entry(fields.operation.clone()).or_default();
        match fields.event.as_str() {
            "ingest_prepared" => {
                entry.prepare_ms = number("precommit_prepare_ms");
                entry.index_delete_stage_ms = number("index_delete_stage_ms");
                entry.data_write_ms = number("data_write_ms");
                entry.delete_write_ms = number("delete_write_ms");
                entry.prepared_at = Some(Instant::now());
            }
            "table_published" => entry.published_at = Some(Instant::now()),
            _ => {}
        }
    }
}

fn stage_events() -> Arc<Mutex<HashMap<String, Reported>>> {
    static EVENTS: OnceLock<Arc<Mutex<HashMap<String, Reported>>>> = OnceLock::new();
    EVENTS
        .get_or_init(|| {
            let events = Arc::new(Mutex::new(HashMap::new()));
            let subscriber = tracing_subscriber::registry().with(
                StageEvents(events.clone()).with_filter(
                    tracing_subscriber::filter::Targets::new()
                        .with_target("flow_events", tracing::Level::DEBUG),
                ),
            );
            tracing::subscriber::set_global_default(subscriber).unwrap();
            events
        })
        .clone()
}

/// One table epoch's wall-clock stages, in milliseconds.
#[derive(Debug, Clone, serde::Serialize)]
struct Sample {
    table: u32,
    epoch: usize,
    mutations: usize,
    mode: &'static str,
    prefolded_transactions: usize,
    /// Collapse, including any wait for the prefold to finish.
    collapse_ms: f64,
    /// Publisher preparation: Parquet, index/delete staging and the plan.
    prepare_ms: f64,
    index_delete_stage_ms: f64,
    data_write_ms: f64,
    delete_write_ms: f64,
    /// From the sealed preparation to the catalog's acknowledgement.
    commit_ms: f64,
    /// Index apply, from the catalog's acknowledgement to the end of publish.
    apply_ms: f64,
    /// Collapse through local completion.
    epoch_ms: f64,
}

type Fold = tokio::task::JoinHandle<Option<Prefold>>;

struct Lane {
    model: Model,
    head: Table,
    /// Pre-journaled epochs, consumed in order.
    epochs: Vec<Vec<SourceTransaction>>,
}

struct Bench {
    _dir: TempDir,
    settings: Settings,
    catalog: Arc<dyn Catalog>,
    store: StateStore,
    reader: JournalReader,
    publisher: Arc<TablePublisher>,
    events: Arc<Mutex<HashMap<String, Reported>>>,
    source: SourceId,
}

impl Bench {
    async fn new(settings: Settings) -> (Self, Vec<Lane>) {
        let events = stage_events();
        // Not `/tmp`, which is memory-backed on some hosts.
        let dir = TempDir::new_in(
            std::env::var_os("INGEST_BENCH_DIR").unwrap_or(env!("CARGO_TARGET_TMPDIR").into()),
        )
        .unwrap();
        let catalog: Arc<dyn Catalog> =
            Arc::new(flow_testkit::catalog(&dir.path().join("warehouse")).await);
        let control = ControlStore::open(dir.path().join("control")).unwrap();
        let store = control
            .initialize_index(
                dir.path().join("index"),
                StateStoreOptions {
                    apply_batch_rows: settings.batch_rows,
                    ..Default::default()
                },
            )
            .unwrap();
        let (journal, _) =
            Journal::open(dir.path().join("journal"), JournalConfig::default()).unwrap();
        let reader = journal.reader();
        let source_id = SourceId("ingest-benchmark".into());
        let mut source = Source {
            journal,
            id: source_id.clone(),
            position: 0,
            xid: 0,
        };
        let publisher = Arc::new(
            TablePublisher::new(
                store.clone(),
                catalog.clone(),
                WriterConfig {
                    batch_bytes: settings.batch_bytes,
                    row_group_bytes: 32 << 20,
                    ..Default::default()
                },
                settings.batch_rows,
                settings.batch_bytes,
            )
            .unwrap(),
        );
        let started = Instant::now();
        let mut lanes = Vec::new();
        let mut seeds = Vec::new();
        for table in 1..=settings.tables as u32 {
            let mut model = Model::new(table, settings.initial_keys, settings.seed);
            let head = catalog
                .create_table(
                    &NamespaceIdent::new("test".into()),
                    TableCreation::builder()
                        .name(format!("table_{table}"))
                        .format_version(FormatVersion::V2)
                        .schema(iceberg_schema(&model.schema).unwrap())
                        .build(),
                )
                .await
                .unwrap();
            let rows: Vec<_> = (1..=settings.initial_keys as i64)
                .map(|id| Mutation {
                    table_id: TableId(table),
                    schema_version: 0,
                    kind: MutationKind::Insert {
                        row: row(id, 0, settings.row_bytes),
                    },
                })
                .collect();
            seeds.push(vec![source.append(TableId(table), &rows, true)]);
            let mut epochs = Vec::new();
            for _ in 0..settings.epochs {
                let mut transactions = Vec::new();
                let mut remaining = settings.mutations_per_epoch;
                while remaining > 0 {
                    let rows = settings.transaction_rows.min(remaining);
                    let mutations = model.transaction(rows, settings.row_bytes);
                    transactions.push(source.append(TableId(table), &mutations, false));
                    remaining -= rows;
                }
                epochs.push(transactions);
            }
            lanes.push(Lane {
                model,
                head,
                epochs,
            });
        }
        source.journal.flush_commits().unwrap();
        let bench = Self {
            _dir: dir,
            settings,
            catalog,
            store,
            reader,
            publisher,
            events,
            source: source_id,
        };
        // The initial copy is not timed; it publishes the seed through the same path.
        for (lane, seed) in lanes.iter_mut().zip(seeds) {
            bench.epoch(lane, &seed, None, None).await;
        }
        eprintln!(
            "ingest benchmark: journaled and seeded {} tables in {:.1}s",
            bench.settings.tables,
            started.elapsed().as_secs_f64()
        );
        (bench, lanes)
    }

    /// The table actor's cycle: collapse on a blocking task, publish, then
    /// release the spool and applied operation as completion does.
    fn limits(&self) -> CollapseLimits {
        CollapseLimits {
            batch_rows: self.settings.batch_rows,
            batch_bytes: self.settings.batch_bytes,
            memory_bytes: self.settings.collapse_memory_bytes,
        }
    }

    /// The table actor's cycle: collapse on a blocking task, continuing
    /// `prefold`, publish, then release the spool and applied operation as
    /// completion does. Once this epoch's rows are consumed, the leading part
    /// of `next` starts folding on a blocking task.
    async fn epoch(
        &self,
        lane: &mut Lane,
        transactions: &[SourceTransaction],
        prefold: Option<Fold>,
        next: Option<&[SourceTransaction]>,
    ) -> (Sample, Option<Fold>) {
        let id = lane.model.schema.table_id;
        let epoch = Epoch::new(self.source.clone(), id, transactions).unwrap();
        let started = Instant::now();
        let prefold = match prefold {
            Some(task) => task.await.unwrap(),
            None => None,
        };
        let collapsed = {
            let (store, reader, head, schema, epoch, transactions) = (
                self.store.clone(),
                self.reader.clone(),
                lane.head.clone(),
                lane.model.schema.clone(),
                epoch.clone(),
                transactions.to_vec(),
            );
            let limits = self.limits();
            tokio::task::spawn_blocking(move || {
                collapse_epoch_prefolded(
                    &store,
                    &reader,
                    &head,
                    &schema,
                    &epoch,
                    &transactions,
                    limits,
                    prefold,
                )
            })
            .await
            .unwrap()
            .unwrap()
        };
        let collapse = started.elapsed();
        let mode = collapsed.mode();
        let prefolded_transactions = collapsed.prefolded_transactions();
        let mut next_fold = None;
        let leading = next
            .map(|next| next[..next.len() * self.settings.prefold_percent.min(100) / 100].to_vec());
        self.publisher
            .publish_observed(&lane.head, &lane.model.schema, collapsed, || {
                let Some(leading) = leading.filter(|leading| !leading.is_empty()) else {
                    return;
                };
                let (store, reader, schema, limits) = (
                    self.store.clone(),
                    self.reader.clone(),
                    lane.model.schema.clone(),
                    self.limits(),
                );
                let source = self.source.clone();
                next_fold = Some(tokio::task::spawn_blocking(move || {
                    Prefold::new(source, schema, limits)?
                        .extend(&store, &reader, &leading, limits)
                        .unwrap()
                }));
            })
            .await
            .unwrap();
        let published = Instant::now();
        let store = self.store.clone();
        let operation = epoch.id.clone();
        tokio::task::spawn_blocking(move || {
            if store.operation(&operation).unwrap().is_none() {
                store.discard_transaction(&operation.0).unwrap();
            }
            store.forget_applied(&operation).unwrap();
        })
        .await
        .unwrap();
        let total = started.elapsed();
        lane.head = self
            .catalog
            .load_table(lane.head.identifier())
            .await
            .unwrap();
        let reported = self
            .events
            .lock()
            .unwrap()
            .remove(&epoch.id.0)
            .expect("publisher reported its stages");
        let ms = |duration: Duration| duration.as_secs_f64() * 1000.0;
        let (prepared_at, published_at) = reported
            .prepared_at
            .zip(reported.published_at)
            .expect("publisher reported its commit");
        let sample = Sample {
            table: id.0,
            epoch: 0,
            mutations: transactions
                .iter()
                .filter_map(|transaction| transaction.mutation_count(id))
                .sum::<u64>() as usize,
            mode,
            prefolded_transactions,
            collapse_ms: ms(collapse),
            prepare_ms: reported.prepare_ms,
            index_delete_stage_ms: reported.index_delete_stage_ms,
            data_write_ms: reported.data_write_ms,
            delete_write_ms: reported.delete_write_ms,
            commit_ms: ms(published_at - prepared_at),
            apply_ms: ms(published - published_at),
            epoch_ms: ms(total),
        };
        (sample, next_fold)
    }

    /// Run every lane's epochs, at most `workers` at once and never two of one
    /// table together, as the daemon's table lanes do.
    async fn run(self: Arc<Self>, lanes: Vec<Lane>) -> (Vec<Sample>, Duration, Vec<Lane>) {
        let workers = Arc::new(tokio::sync::Semaphore::new(self.settings.workers));
        let started = Instant::now();
        let mut tasks = Vec::new();
        for mut lane in lanes {
            let (bench, workers) = (self.clone(), workers.clone());
            tasks.push(tokio::spawn(async move {
                let mut samples = Vec::new();
                let epochs = std::mem::take(&mut lane.epochs);
                let mut prefold = None;
                for (number, transactions) in epochs.iter().enumerate() {
                    let _worker = workers.acquire().await.unwrap();
                    let next = epochs.get(number + 1).map(Vec::as_slice);
                    let (mut sample, fold) = bench
                        .epoch(&mut lane, transactions, prefold.take(), next)
                        .await;
                    prefold = fold;
                    sample.epoch = number;
                    samples.push(sample);
                }
                (samples, lane)
            }));
        }
        let mut samples = Vec::new();
        let mut lanes = Vec::new();
        for task in tasks {
            let (lane_samples, lane) = task.await.unwrap();
            samples.extend(lane_samples);
            lanes.push(lane);
        }
        (samples, started.elapsed(), lanes)
    }

    /// The published rows and the row index must equal the generator's state.
    async fn verify(&self, lane: &Lane) {
        let schema = &lane.model.schema;
        let mut stream = lane.head.scan().build().unwrap().to_arrow().await.unwrap();
        let mut published = Vec::new();
        while let Some(batch) = stream.try_next().await.unwrap() {
            for row in rows_from_batch(schema, &batch).unwrap() {
                match (&row[0], &row[1]) {
                    (Value::Int64(id), Value::Int64(version)) => published.push((*id, *version)),
                    _ => unreachable!("benchmark schema"),
                }
            }
        }
        published.sort_unstable();
        let expected = lane.model.expected();
        assert_eq!(
            published.len(),
            expected.len(),
            "table {} row count",
            schema.table_id.0
        );
        assert!(
            published == expected,
            "table {} rows differ from the source",
            schema.table_id.0
        );

        let keys: Vec<_> = expected.iter().map(|(id, _)| lane.model.key(*id)).collect();
        for chunk in keys.chunks(4096) {
            let found = self.store.lookup_many(&schema.table_id, chunk).unwrap();
            assert!(
                found.iter().all(Option::is_some),
                "an indexed key is missing"
            );
        }
        let view = SnapshotView::current(&lane.head).await.unwrap();
        let mut indexed = 0usize;
        for entry in view
            .live_files
            .values()
            .filter(|entry| entry.content_type() == DataContentType::Data)
        {
            let file = FileId(flow_iceberg_ext::content_file_id(&entry.data_file));
            indexed += self.store.file_rows(&schema.table_id, &file).count();
        }
        assert_eq!(
            indexed,
            expected.len(),
            "table {} reverse index entries",
            schema.table_id.0
        );
        let state = self.store.table_state(&schema.table_id).unwrap();
        assert_eq!(
            state.snapshot_id,
            lane.head.metadata().current_snapshot_id()
        );
        assert!(state.pending_operation.is_none());
    }
}

fn median(values: &mut [f64]) -> f64 {
    values.sort_by(f64::total_cmp);
    match values.len() {
        0 => f64::NAN,
        n if n % 2 == 1 => values[n / 2],
        n => (values[n / 2 - 1] + values[n / 2]) / 2.0,
    }
}

async fn benchmark(name: &str, settings: Settings) {
    // Benchmarks in one binary would otherwise run concurrently.
    static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
    let _serial = SERIAL.lock().await;
    let (bench, lanes) = Bench::new(settings.clone()).await;
    let bench = Arc::new(bench);
    let (samples, wall, lanes) = bench.clone().run(lanes).await;
    for lane in &lanes {
        bench.verify(lane).await;
    }
    let measured: Vec<_> = samples
        .iter()
        .filter(|sample| sample.epoch >= settings.warmup_epochs)
        .collect();
    let stage = |select: fn(&Sample) -> f64| {
        median(
            &mut measured
                .iter()
                .map(|sample| select(sample))
                .collect::<Vec<_>>(),
        )
    };
    let mutations: usize = samples.iter().map(|sample| sample.mutations).sum();
    let mut modes: Vec<_> = measured.iter().map(|sample| sample.mode).collect();
    modes.sort_unstable();
    modes.dedup();
    let stages: [(&str, f64); 8] = [
        ("collapse", stage(|s| s.collapse_ms)),
        ("prepare", stage(|s| s.prepare_ms)),
        ("  data_write", stage(|s| s.data_write_ms)),
        ("  index_delete_stage", stage(|s| s.index_delete_stage_ms)),
        ("  delete_write", stage(|s| s.delete_write_ms)),
        ("commit", stage(|s| s.commit_ms)),
        ("apply", stage(|s| s.apply_ms)),
        ("epoch", stage(|s| s.epoch_ms)),
    ];
    let summary = serde_json::json!({
        "benchmark": name,
        "settings": settings,
        "epochs_measured": measured.len(),
        "collapse_modes": modes,
        "prefolded_transactions": stage(|s| s.prefolded_transactions as f64),
        "median_ms": stages
            .iter()
            .map(|(name, value)| (name.trim().to_owned(), serde_json::json!(value)))
            .collect::<serde_json::Map<_, _>>(),
        "wall_seconds": wall.as_secs_f64(),
        "mutations_per_second": mutations as f64 / wall.as_secs_f64(),
        "verified_rows": lanes.iter().map(|lane| lane.model.versions.len()).sum::<usize>(),
    });
    eprintln!(
        "{name}: {} tables, {} epochs measured ({modes:?})",
        settings.tables,
        measured.len()
    );
    eprintln!("  stage                   median ms");
    for (stage, value) in stages {
        eprintln!("  {stage:<22} {value:>10.1}");
    }
    eprintln!(
        "  {mutations} mutations in {:.2}s: {:.0} mutations/s; {} rows verified",
        wall.as_secs_f64(),
        summary["mutations_per_second"].as_f64().unwrap(),
        summary["verified_rows"],
    );
    println!("INGEST_BENCH_RESULT {summary}");
    if let Some(path) = std::env::var_os("INGEST_BENCH_OUTPUT") {
        let report = serde_json::json!({"summary": summary, "samples": samples});
        std::fs::write(path, serde_json::to_vec_pretty(&report).unwrap()).unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "benchmark; run with --release -- --ignored --nocapture"]
async fn ingest_epochs_one_table() {
    benchmark("one-table", Settings::from_env(1, 113_000)).await;
}

/// Ten busy tables share the store, the journal and four table workers. Each
/// epoch is a tenth of the single-table epoch.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "benchmark; run with --release -- --ignored --nocapture"]
async fn ingest_epochs_ten_tables() {
    benchmark("ten-tables", Settings::from_env(10, 11_300)).await;
}
