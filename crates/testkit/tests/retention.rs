//! Snapshot expiration honors table policy, a retain floor and a count cap,
//! and never fails on tables or protections it cannot act on.
use flow_compactor::Policy;
use flow_coordinator::{HistoryPlan, HistoryPolicy, TableMaintenance, TablePublisher};
use flow_materializer::WriterConfig;
use flow_model::{PgLsn, SourceId, TableSchema, Value};
use flow_state_store::{Change, StateStore};
use flow_testkit::{catalog, schema, table};
use iceberg::memory::MemoryCatalog;
use iceberg::spec::{SnapshotReference, SnapshotRetention};
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{
    Catalog, Namespace, NamespaceIdent, TableCommit, TableCreation, TableIdent, TableRequirement,
    TableUpdate, table::Table,
};
use std::{
    collections::{BTreeSet, HashMap},
    sync::Arc,
    time::Duration,
};
use tempfile::TempDir;

const SEVEN_DAYS_MS: &str = "604800000";

struct Fixture {
    _temp: TempDir,
    catalog: Arc<MemoryCatalog>,
    store: StateStore,
    schema: TableSchema,
    head: Table,
}

impl Fixture {
    /// A table with `commits` published snapshots and the given properties.
    async fn new(properties: &[(&str, &str)], commits: u64) -> Self {
        let temp = TempDir::new().unwrap();
        let catalog = Arc::new(catalog(&temp.path().join("warehouse")).await);
        let schema = schema(1);
        let mut head = table(catalog.as_ref(), &schema).await;
        if !properties.is_empty() {
            let transaction = Transaction::new(&head);
            let mut update = transaction.update_table_properties();
            for (key, value) in properties {
                update = update.set((*key).into(), (*value).into());
            }
            head = update
                .apply(transaction)
                .unwrap()
                .commit(catalog.as_ref())
                .await
                .unwrap();
        }
        let store = StateStore::open(temp.path().join("index"), Default::default()).unwrap();
        let publisher = TablePublisher::new(
            store.clone(),
            catalog.clone(),
            WriterConfig::default(),
            100,
            1 << 20,
        )
        .unwrap();
        for n in 1..=commits {
            let row = vec![Value::Int64(n as i64), Value::String("kept".into())];
            let (epoch, collapsed) = flow_testkit::collapse_fixture(
                &store,
                &head,
                &schema,
                SourceId("local".into()),
                PgLsn(n * 10),
                [(schema.encode_key(&row).unwrap(), Change::Insert(row))],
            );
            publisher.publish(&head, &schema, collapsed).await.unwrap();
            store.forget_applied(&epoch.id).unwrap();
            head = catalog.load_table(head.identifier()).await.unwrap();
        }
        Self {
            _temp: temp,
            catalog,
            store,
            schema,
            head,
        }
    }

    fn maintenance(&self) -> TableMaintenance {
        TableMaintenance::new(
            self.store.clone(),
            self.catalog.clone(),
            Policy::default(),
            WriterConfig::default(),
        )
        .unwrap()
    }

    async fn expire(&self, policy: &HistoryPolicy, protected: &BTreeSet<i64>) -> usize {
        self.maintenance()
            .expire_history(&self.head, self.schema.table_id, policy, protected)
            .await
            .unwrap()
    }

    async fn plan(&self, policy: &HistoryPolicy, protected: &BTreeSet<i64>) -> Option<HistoryPlan> {
        self.maintenance()
            .history_plan(&self.head, self.schema.table_id, policy, protected)
            .await
            .unwrap()
    }

    async fn due(&self, policy: &HistoryPolicy) -> bool {
        self.plan(policy, &BTreeSet::new())
            .await
            .is_some_and(|plan| plan.due())
    }

    async fn reload(&mut self) -> Vec<i64> {
        self.head = self
            .catalog
            .load_table(self.head.identifier())
            .await
            .unwrap();
        self.snapshots()
    }

    /// Snapshot ids, oldest first.
    fn snapshots(&self) -> Vec<i64> {
        let mut snapshots: Vec<_> = self
            .head
            .metadata()
            .snapshots()
            .map(|snapshot| (snapshot.sequence_number(), snapshot.snapshot_id()))
            .collect();
        snapshots.sort_unstable();
        snapshots.into_iter().map(|(_, id)| id).collect()
    }

    async fn set_property(&mut self, key: &str, value: &str) {
        let transaction = Transaction::new(&self.head);
        self.head = transaction
            .update_table_properties()
            .set(key.into(), value.into())
            .apply(transaction)
            .unwrap()
            .commit(self.catalog.as_ref())
            .await
            .unwrap();
    }
}

fn policy(retention: Duration, retain_last: usize, max_snapshots: usize) -> HistoryPolicy {
    HistoryPolicy {
        retention,
        retain_last,
        max_snapshots,
    }
}

#[tokio::test]
async fn table_max_snapshot_age_longer_than_flow_retention_keeps_history() {
    let mut f = Fixture::new(&[("history.expire.max-snapshot-age-ms", SEVEN_DAYS_MS)], 5).await;
    let before = f.snapshots();
    assert_eq!(before.len(), 5);
    tokio::time::sleep(Duration::from_millis(3)).await;
    let window = policy(Duration::from_millis(1), 1, usize::MAX);
    // Flow's one-millisecond window alone would leave only the head.
    assert!(!f.due(&window).await);
    assert_eq!(f.expire(&window, &BTreeSet::new()).await, 0);
    assert_eq!(f.reload().await, before);

    // A shorter table window than Flow's never shortens Flow's window.
    f.set_property("history.expire.max-snapshot-age-ms", "1")
        .await;
    let hour = policy(Duration::from_secs(3600), 1, usize::MAX);
    assert!(!f.due(&hour).await);
    assert_eq!(f.expire(&hour, &BTreeSet::new()).await, 0);
    assert_eq!(f.reload().await, before);

    // Once both windows have passed, the retain floor still keeps the newest
    // snapshots even though every snapshot is past the window.
    let floor = policy(Duration::from_millis(1), 3, usize::MAX);
    assert!(f.due(&floor).await);
    assert_eq!(f.expire(&floor, &BTreeSet::new()).await, 2);
    assert_eq!(f.reload().await, before[2..]);
    assert!(!f.due(&floor).await);
}

#[tokio::test]
async fn table_min_snapshots_to_keep_raises_the_retain_floor() {
    let mut f = Fixture::new(&[("history.expire.min-snapshots-to-keep", "4")], 6).await;
    let before = f.snapshots();
    tokio::time::sleep(Duration::from_millis(3)).await;
    let window = policy(Duration::from_millis(1), 2, usize::MAX);
    assert_eq!(f.expire(&window, &BTreeSet::new()).await, 2);
    assert_eq!(f.reload().await, before[2..]);
}

#[tokio::test]
async fn snapshot_cap_expires_oldest_history_inside_the_window_but_keeps_refs_and_floor() {
    let mut f = Fixture::new(&[], 6).await;
    let before = f.snapshots();
    // A standard tag keeps a historical reader independently of the cap.
    let tagged = before[0];
    let commit = iceberg::TableCommit::builder()
        .ident(f.head.identifier().clone())
        .updates(vec![TableUpdate::SetSnapshotRef {
            ref_name: "retained".into(),
            reference: SnapshotReference {
                snapshot_id: tagged,
                retention: SnapshotRetention::Tag {
                    max_ref_age_ms: None,
                },
            },
        }])
        .requirements(vec![TableRequirement::UuidMatch {
            uuid: f.head.metadata().uuid(),
        }])
        .build();
    f.catalog.update_table(commit).await.unwrap();
    f.reload().await;

    let capped = policy(Duration::from_secs(3600), 2, 3);
    let plan = f.plan(&capped, &BTreeSet::new()).await.unwrap();
    assert!(plan.due());
    // The cap can remove three snapshots, more than its one-snapshot slack.
    assert!(plan.over_limit());
    assert_eq!(f.expire(&capped, &BTreeSet::new()).await, 3);
    let retained = f.reload().await;
    assert_eq!(retained, vec![tagged, before[4], before[5]]);
    let plan = f.plan(&capped, &BTreeSet::new()).await.unwrap();
    assert!(!plan.due() && !plan.over_limit());
    assert_eq!(plan.over_cap(), 0);

    // A cap below the floor cannot be configured.
    assert!(policy(Duration::from_secs(1), 4, 3).validate().is_err());
}

#[tokio::test]
async fn snapshot_cap_never_expires_checkpoint_protected_history() {
    let mut f = Fixture::new(&[], 6).await;
    let before = f.snapshots();
    let capped = policy(Duration::from_secs(3600), 1, 2);
    // A checkpoint base protects itself and every descendant.
    assert_eq!(f.expire(&capped, &BTreeSet::from([before[2]])).await, 2);
    assert_eq!(f.reload().await, before[2..]);
    // Protected history above the cap is reported but never makes expiration
    // due: there is nothing it could remove.
    let plan = f.plan(&capped, &BTreeSet::from([before[2]])).await.unwrap();
    assert_eq!(plan.over_cap(), 2);
    assert!(!plan.due() && !plan.over_limit() && !plan.held_by_table_policy());
}

#[tokio::test]
async fn explicit_table_window_is_exempt_from_the_snapshot_cap() {
    let mut f = Fixture::new(&[("history.expire.max-snapshot-age-ms", SEVEN_DAYS_MS)], 6).await;
    let before = f.snapshots();
    let capped = policy(Duration::from_secs(3600), 1, 3);
    let plan = f.plan(&capped, &BTreeSet::new()).await.unwrap();
    assert!(plan.held_by_table_policy());
    assert_eq!(plan.over_cap(), 3);
    assert!(!plan.due() && !plan.over_limit());
    assert_eq!(f.expire(&capped, &BTreeSet::new()).await, 0);
    assert_eq!(f.reload().await, before);

    // A table window shorter than Flow's leaves the rest of Flow's window
    // subject to the cap.
    tokio::time::sleep(Duration::from_millis(3)).await;
    f.set_property("history.expire.max-snapshot-age-ms", "1")
        .await;
    let plan = f.plan(&capped, &BTreeSet::new()).await.unwrap();
    assert!(!plan.held_by_table_policy() && plan.over_limit());
    assert_eq!(f.expire(&capped, &BTreeSet::new()).await, 3);
    assert_eq!(f.reload().await, before[3..]);
}

#[tokio::test]
async fn gc_disabled_table_is_not_expired_and_does_not_fail() {
    let mut f = Fixture::new(&[("gc.enabled", "false")], 4).await;
    let before = f.snapshots();
    tokio::time::sleep(Duration::from_millis(3)).await;
    let aggressive = policy(Duration::from_millis(1), 1, 1);
    assert!(f.plan(&aggressive, &BTreeSet::new()).await.is_none());
    assert_eq!(f.expire(&aggressive, &BTreeSet::new()).await, 0);
    assert_eq!(f.reload().await, before);
}

#[tokio::test]
async fn externally_expired_protected_snapshot_does_not_stop_expiration() {
    let mut f = Fixture::new(&[], 4).await;
    let before = f.snapshots();
    // An id another process already expired (a stale checkpoint base).
    let missing = before.iter().max().unwrap().wrapping_add(1);
    assert!(f.head.metadata().snapshot_by_id(missing).is_none());
    tokio::time::sleep(Duration::from_millis(3)).await;
    let window = HistoryPolicy::window(Duration::from_millis(1));
    assert_eq!(f.expire(&window, &BTreeSet::from([missing])).await, 3);
    assert_eq!(f.reload().await, before[3..]);
}

#[tokio::test]
async fn descendants_of_an_externally_expired_base_stay_protected() {
    let mut f = Fixture::new(&[], 5).await;
    let before = f.snapshots();
    // Another process expires a checkpoint base but keeps its descendants.
    let transaction = Transaction::new(&f.head);
    f.head = transaction
        .expire_snapshots()
        .expire_older_than_ms(0)
        .expire_snapshot_ids([before[1]])
        .apply(transaction)
        .unwrap()
        .commit(f.catalog.as_ref())
        .await
        .unwrap();
    assert!(f.head.metadata().snapshot_by_id(before[1]).is_none());
    tokio::time::sleep(Duration::from_millis(3)).await;
    let window = HistoryPolicy::window(Duration::from_millis(1));
    assert_eq!(f.expire(&window, &BTreeSet::from([before[1]])).await, 1);
    assert_eq!(f.reload().await, before[2..]);
}

/// Reports catalog JSON under a separate directory, as a catalog honoring
/// `write.metadata.path` does. The inner catalog still stores the JSON.
#[derive(Debug)]
struct ForeignMetadataCatalog(Arc<MemoryCatalog>);

fn relocate(table: Table) -> iceberg::Result<Table> {
    let location = table.metadata().location().trim_end_matches('/').to_owned();
    let mut builder = Table::builder()
        .identifier(table.identifier().clone())
        .metadata(table.metadata_ref())
        .file_io(table.file_io().clone())
        .runtime(iceberg::Runtime::current());
    if let Some(path) = table.metadata_location() {
        builder = builder.metadata_location(path.replace(
            &format!("{location}/metadata/"),
            &format!("{location}-custom-metadata/"),
        ));
    }
    builder.build()
}

#[async_trait::async_trait]
impl Catalog for ForeignMetadataCatalog {
    async fn list_namespaces(
        &self,
        p: Option<&NamespaceIdent>,
    ) -> iceberg::Result<Vec<NamespaceIdent>> {
        self.0.list_namespaces(p).await
    }
    async fn create_namespace(
        &self,
        n: &NamespaceIdent,
        p: HashMap<String, String>,
    ) -> iceberg::Result<Namespace> {
        self.0.create_namespace(n, p).await
    }
    async fn get_namespace(&self, n: &NamespaceIdent) -> iceberg::Result<Namespace> {
        self.0.get_namespace(n).await
    }
    async fn namespace_exists(&self, n: &NamespaceIdent) -> iceberg::Result<bool> {
        self.0.namespace_exists(n).await
    }
    async fn update_namespace(
        &self,
        n: &NamespaceIdent,
        p: HashMap<String, String>,
    ) -> iceberg::Result<()> {
        self.0.update_namespace(n, p).await
    }
    async fn drop_namespace(&self, n: &NamespaceIdent) -> iceberg::Result<()> {
        self.0.drop_namespace(n).await
    }
    async fn list_tables(&self, n: &NamespaceIdent) -> iceberg::Result<Vec<TableIdent>> {
        self.0.list_tables(n).await
    }
    async fn create_table(&self, n: &NamespaceIdent, c: TableCreation) -> iceberg::Result<Table> {
        relocate(self.0.create_table(n, c).await?)
    }
    async fn load_table(&self, t: &TableIdent) -> iceberg::Result<Table> {
        relocate(self.0.load_table(t).await?)
    }
    async fn drop_table(&self, t: &TableIdent) -> iceberg::Result<()> {
        self.0.drop_table(t).await
    }
    async fn purge_table(&self, t: &TableIdent) -> iceberg::Result<()> {
        self.0.purge_table(t).await
    }
    async fn table_exists(&self, t: &TableIdent) -> iceberg::Result<bool> {
        self.0.table_exists(t).await
    }
    async fn rename_table(&self, s: &TableIdent, d: &TableIdent) -> iceberg::Result<()> {
        self.0.rename_table(s, d).await
    }
    async fn register_table(&self, t: &TableIdent, m: String) -> iceberg::Result<Table> {
        relocate(self.0.register_table(t, m).await?)
    }
    async fn update_table(&self, commit: TableCommit) -> iceberg::Result<Table> {
        relocate(self.0.update_table(commit).await?)
    }
}

#[tokio::test]
async fn catalog_json_outside_the_table_metadata_directory_is_not_registered() {
    let mut f = Fixture::new(&[], 3).await;
    let catalog: Arc<dyn Catalog> = Arc::new(ForeignMetadataCatalog(f.catalog.clone()));
    let head = catalog.load_table(f.head.identifier()).await.unwrap();
    let foreign = head.metadata_location().unwrap().to_owned();
    assert!(foreign.contains("-custom-metadata/"));
    let registry = format!("owned-artifacts/v2/{}/", head.metadata().uuid());
    let records = |store: &StateStore| {
        store
            .source_transactions_after(registry.as_bytes(), None)
            .map(|entry| entry.unwrap().1.to_vec())
            .collect::<Vec<_>>()
    };
    let owns_foreign_json = |records: &[Vec<u8>]| {
        records.iter().any(|record| {
            record
                .windows("-custom-metadata/".len())
                .any(|window| window == b"-custom-metadata/")
        })
    };
    let before = records(&f.store);
    flow_coordinator::register_catalog_metadata(&f.store, &head, f.schema.table_id)
        .await
        .unwrap();
    assert_eq!(records(&f.store), before);

    // Publication registers its own outputs but neither owns nor refuses the
    // catalog's JSON. Expiration registers the current pointer the same way.
    let publisher = TablePublisher::new(
        f.store.clone(),
        catalog.clone(),
        WriterConfig::default(),
        100,
        1 << 20,
    )
    .unwrap();
    let row = vec![Value::Int64(100), Value::String("published".into())];
    let (epoch, collapsed) = flow_testkit::collapse_fixture(
        &f.store,
        &head,
        &f.schema,
        SourceId("local".into()),
        PgLsn(1000),
        [(f.schema.encode_key(&row).unwrap(), Change::Insert(row))],
    );
    publisher
        .publish(&head, &f.schema, collapsed)
        .await
        .unwrap();
    f.store.forget_applied(&epoch.id).unwrap();
    let published = records(&f.store);
    assert!(published.len() > before.len());
    assert!(!owns_foreign_json(&published));
    let maintenance = TableMaintenance::new(
        f.store.clone(),
        catalog.clone(),
        Policy::default(),
        WriterConfig::default(),
    )
    .unwrap();
    tokio::time::sleep(Duration::from_millis(3)).await;
    let head = catalog.load_table(head.identifier()).await.unwrap();
    assert_eq!(
        maintenance
            .expire_history(
                &head,
                f.schema.table_id,
                &HistoryPolicy::window(Duration::from_millis(1)),
                &BTreeSet::new(),
            )
            .await
            .unwrap(),
        3
    );
    assert!(!owns_foreign_json(&records(&f.store)));
    assert_eq!(f.reload().await.len(), 1);
}
