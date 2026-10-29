//! A stale index checkpoint advanced from Iceberg file changes must equal a
//! full rebuild of the current head, across CDC updates and deletes, key moves,
//! compaction and delete rewrites.
#[path = "common/rewrite.rs"]
mod fixture;
use fixture::{Fixture, options, row};
use flow_compactor::Policy;
use flow_coordinator::{DeleteRewritePolicy, TableMaintenance, TablePublisher};
use flow_iceberg_ext::RowDeltaAction;
use flow_iceberg_ext::SnapshotView;
use flow_materializer::WriterConfig;
use flow_model::{FileId, PgLsn, Row, SourceId, Value};
use flow_state_store::{Change, StateStore};
use iceberg::{Catalog, spec::DataContentType};
use std::collections::BTreeMap;

fn maintenance(f: &Fixture, store: &StateStore) -> TableMaintenance {
    TableMaintenance::new(
        store.clone(),
        f.catalog.clone(),
        Policy {
            l0_soft_files: 2,
            l0_hard_files: 4,
            oldest_l0_soft_ms: 1,
            oldest_l0_hard_ms: 2,
            deleted_rows_percent: 1,
            min_file_age_ms: 0,
            ..Default::default()
        },
        WriterConfig::default(),
    )
    .unwrap()
}

async fn cdc(f: &mut Fixture, lsn: u64, changes: Vec<(i64, Change)>) {
    let (epoch, collapsed) = flow_testkit::collapse_fixture(
        &f.index,
        &f.head,
        &f.schema,
        SourceId("fixture".into()),
        PgLsn(lsn),
        changes
            .into_iter()
            .map(|(id, change)| (f.schema.encode_key(&row(id)).unwrap(), change)),
    );
    TablePublisher::new(
        f.index.clone(),
        f.catalog.clone(),
        WriterConfig::default(),
        2,
        1 << 20,
    )
    .unwrap()
    .publish(&f.head, &f.schema, collapsed)
    .await
    .unwrap();
    f.index.forget_applied(&epoch.id).unwrap();
    f.head = f.catalog.load_table(f.head.identifier()).await.unwrap();
}

fn changed(id: i64, value: &str) -> Row {
    vec![Value::Int64(id), Value::String(value.into())]
}

/// Every indexed key with its physical location, fingerprint and sequence.
fn contents(f: &Fixture, store: &StateStore) -> BTreeMap<(FileId, u64), Vec<u8>> {
    let view = futures::executor::block_on(SnapshotView::current(&f.head)).unwrap();
    let mut rows = BTreeMap::new();
    for entry in view
        .live_files
        .values()
        .filter(|entry| entry.content_type() == DataContentType::Data)
    {
        let file = FileId(flow_iceberg_ext::content_file_id(&entry.data_file));
        for item in store.file_rows(&f.schema.table_id, &file) {
            let (position, key) = item.unwrap();
            let location = store
                .lookup(&f.schema.table_id, &key)
                .unwrap()
                .expect("reverse entry has a key");
            assert_eq!(
                (&location.data_file_id, location.row_position),
                (&file, position)
            );
            let mut summary = key.0.clone();
            summary.extend_from_slice(&location.row_fingerprint);
            summary.extend_from_slice(&location.data_sequence_number.to_be_bytes());
            rows.insert((file.clone(), position), summary);
        }
        let counts = store
            .file_live_row_counts(
                &f.schema.table_id,
                f.head.metadata().current_snapshot_id(),
                std::slice::from_ref(&file),
            )
            .unwrap();
        let indexed = rows.keys().filter(|(owner, _)| *owner == file).count() as u64;
        assert_eq!(counts, [indexed], "live-row count of {}", file.0);
    }
    rows
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stale_checkpoint_catches_up_to_exactly_a_full_rebuild() {
    let mut f = Fixture::new().await;
    cdc(
        &mut f,
        90,
        vec![
            (16, Change::Insert(row(16))),
            (3, Change::Update(changed(3, "first"))),
        ],
    )
    .await;
    let checkpoint = f.temp.path().join("checkpoint");
    f.index.checkpoint(&checkpoint).unwrap();
    let at_checkpoint = f.index.table_state(&f.schema.table_id).unwrap();

    // Updates and deletes against files the checkpoint already indexed, a key
    // that moves twice, new keys, compaction and a delete-only rewrite.
    cdc(
        &mut f,
        100,
        vec![
            (10, Change::Update(changed(10, "late"))),
            (13, Change::Delete),
            (140, Change::Insert(changed(140, "new"))),
            (15, Change::Delete),
            (17, Change::Insert(row(17))),
            (3, Change::Update(changed(3, "second"))),
        ],
    )
    .await;
    let compacted = maintenance(&f, &f.index)
        .compact(&f.head, &f.schema, f.scratch("compaction"))
        .await
        .unwrap();
    assert!(compacted.is_some(), "the fixture must remove data files");
    f.head = f.catalog.load_table(f.head.identifier()).await.unwrap();
    cdc(
        &mut f,
        110,
        vec![
            (15, Change::Insert(row(15))),
            (4, Change::Delete),
            (3, Change::Update(changed(3, "third"))),
        ],
    )
    .await;
    let rewritten = f
        .maintenance()
        .compact_deletes(
            &f.head,
            &f.schema,
            f.scratch("delete-rewrite"),
            &DeleteRewritePolicy {
                min_input_files: 2,
                ..DeleteRewritePolicy::default()
            },
        )
        .await
        .unwrap();
    assert!(rewritten.is_some(), "the fixture must rewrite delete files");
    f.head = f.catalog.load_table(f.head.identifier()).await.unwrap();
    // Recovery takes watermarks from durable control authority.
    let current = f.control.table_state(&f.schema.table_id).unwrap().unwrap();
    assert_eq!(current, f.index.table_state(&f.schema.table_id).unwrap());
    assert_ne!(current.snapshot_id, at_checkpoint.snapshot_id);

    let restored = StateStore::open(&checkpoint, options()).unwrap();
    for row in &f.expected {
        let key = f.schema.encode_key(row).unwrap();
        assert!(restored.lookup(&f.schema.table_id, &key).unwrap().is_some());
    }
    assert_eq!(
        restored
            .lookup(&f.schema.table_id, &f.schema.encode_key(&row(5)).unwrap())
            .unwrap()
            .as_ref(),
        Some(&f.locations[5]),
        "the checkpoint holds the original location of an untouched row"
    );
    let stats = maintenance(&f, &restored)
        .catch_up_index(
            &f.head,
            &f.schema,
            restored.clone(),
            f.scratch("catch-up"),
            at_checkpoint.snapshot_id,
            current.materialized_lsn,
            current.schema_version,
        )
        .await
        .unwrap();
    assert!(stats.removed_files > 0 && stats.added_files > 0);

    let rebuilt = StateStore::open(f.temp.path().join("rebuilt"), options()).unwrap();
    maintenance(&f, &rebuilt)
        .rebuild_index(
            &f.head,
            &f.schema,
            rebuilt.clone(),
            f.scratch("rebuild"),
            current.materialized_lsn,
            current.schema_version,
        )
        .await
        .unwrap();

    let state = restored.table_state(&f.schema.table_id).unwrap();
    assert_eq!(state, rebuilt.table_state(&f.schema.table_id).unwrap());
    assert_eq!(state.snapshot_id, f.head.metadata().current_snapshot_id());
    assert!(restored.pending_operations().unwrap().is_empty());
    assert_eq!(contents(&f, &restored), contents(&f, &rebuilt));
    // The live index's compaction committed a lazy remap; reverse scans of its
    // outputs are exact only after materialization.
    assert!(f.index.has_remaps(&f.schema.table_id).unwrap());
    f.index.materialize_remaps(&f.schema.table_id).unwrap();
    assert_eq!(contents(&f, &restored), contents(&f, &f.index));
    for id in [4, 13] {
        assert!(
            restored
                .lookup(&f.schema.table_id, &f.schema.encode_key(&row(id)).unwrap())
                .unwrap()
                .is_none()
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn catch_up_rejects_a_checkpoint_that_disagrees_with_its_snapshot() {
    let mut f = Fixture::new().await;
    let checkpoint = f.temp.path().join("checkpoint");
    f.index.checkpoint(&checkpoint).unwrap();
    let at_checkpoint = f.index.table_state(&f.schema.table_id).unwrap();
    cdc(&mut f, 100, vec![(10, Change::Update(changed(10, "late")))]).await;
    let current = f.index.table_state(&f.schema.table_id).unwrap();
    let restored = StateStore::open(&checkpoint, options()).unwrap();
    // Claiming the checkpoint describes a different snapshot must fail, never
    // produce an index from mismatched inputs.
    assert!(
        maintenance(&f, &restored)
            .catch_up_index(
                &f.head,
                &f.schema,
                restored.clone(),
                f.scratch("catch-up"),
                current.snapshot_id,
                current.materialized_lsn,
                current.schema_version,
            )
            .await
            .is_err()
    );
    assert_eq!(
        restored.table_state(&f.schema.table_id).unwrap(),
        at_checkpoint
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn catch_up_rejects_a_removed_delete_for_a_surviving_file() {
    let mut f = Fixture::new().await;
    let checkpoint = f.temp.path().join("checkpoint");
    f.index.checkpoint(&checkpoint).unwrap();
    let at_checkpoint = f.index.table_state(&f.schema.table_id).unwrap();
    // A new file gives catch-up work besides the delete change below.
    cdc(
        &mut f,
        100,
        vec![(140, Change::Insert(changed(140, "new")))],
    )
    .await;
    let current = f.index.table_state(&f.schema.table_id).unwrap();
    // An external writer removes a delete file. Rows 2 and 12 of both
    // surviving data files are live again, but the checkpoint lacks them.
    let view = SnapshotView::current(&f.head).await.unwrap();
    let removed = view
        .live_files
        .values()
        .find(|entry| {
            entry.content_type() == DataContentType::PositionDeletes
                && entry.file_path().contains("shared-b")
        })
        .unwrap();
    f.head = RowDeltaAction::new(&f.head, "external-undelete")
        .remove_delete_files([flow_iceberg_ext::content_file_id(&removed.data_file)])
        .commit(f.catalog.as_ref(), &f.head)
        .await
        .unwrap()
        .table;
    let live = flow_testkit::scan(&f.head, &f.schema).await.unwrap();
    assert!(live.contains(&row(2)) && live.contains(&row(12)));

    let restored = StateStore::open(&checkpoint, options()).unwrap();
    let error = maintenance(&f, &restored)
        .catch_up_index(
            &f.head,
            &f.schema,
            restored.clone(),
            f.scratch("catch-up"),
            at_checkpoint.snapshot_id,
            current.materialized_lsn,
            current.schema_version,
        )
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("removed or narrowed"),
        "{error:#}"
    );
    // The caller's fallback, a full rebuild, indexes the restored rows.
    let rebuilt = StateStore::open(f.temp.path().join("rebuilt"), options()).unwrap();
    maintenance(&f, &rebuilt)
        .rebuild_index(
            &f.head,
            &f.schema,
            rebuilt.clone(),
            f.scratch("rebuild"),
            current.materialized_lsn,
            current.schema_version,
        )
        .await
        .unwrap();
    for id in [2, 12, 140] {
        let key = f.schema.encode_key(&row(id)).unwrap();
        assert!(rebuilt.lookup(&f.schema.table_id, &key).unwrap().is_some());
    }
}
