#[path = "common/rewrite.rs"]
mod fixture;
use fixture::{Fixture, options, row, sorted};
use flow_coordinator::{
    BuildRegistration, GarbagePolicy, GarbageProtection, TablePublisher, active_build_protection,
    discard_abandoned_builds,
};
use flow_materializer::{DataWriter, WriterConfig};
use flow_model::{OperationId, PgLsn, SourceId};
use flow_state_store::{Change, OperationKind, OperationPhase, PreparedOperation, StateStore};
use flow_testkit::scan;
use iceberg::{Catalog, table::Table};
use std::{collections::BTreeSet, time::Duration};

async fn output(f: &Fixture, build: &BuildRegistration) -> String {
    let mut writer = DataWriter::new(
        f.head.file_io().clone(),
        f.head.metadata().location(),
        build.operation_id(),
        f.schema.clone(),
        0,
        WriterConfig {
            artifact_tracker: Some(build.artifact_tracker()),
            ..Default::default()
        },
    )
    .unwrap();
    writer.write(&[row(10)], PgLsn(80)).await.unwrap();
    writer.close().await.unwrap()[0].file_path().to_owned()
}

async fn collect(f: &Fixture, head: &Table) {
    f.maintenance()
        .collect_garbage(
            head,
            f.schema.table_id,
            &GarbagePolicy {
                grace: Duration::from_millis(1),
                metadata_grace: Duration::from_millis(1),
                max_objects: 512,
                max_records: 64,
                max_duration: Duration::from_secs(1),
                ..Default::default()
            },
            &GarbageProtection::default(),
        )
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unfenced_build_protects_ancestry_and_uploads_through_index_loss_until_startup_retirement()
{
    let mut f = Fixture::new().await;
    let base = f.head.metadata().current_snapshot_id().unwrap();
    let build = BuildRegistration::begin(
        f.index.clone(),
        &f.head,
        f.schema.table_id,
        OperationId("speculative-build".into()),
    )
    .await
    .unwrap();
    assert!(
        BuildRegistration::begin(
            f.index.clone(),
            &f.head,
            f.schema.table_id,
            OperationId("duplicate-build".into()),
        )
        .await
        .is_err()
    );
    let path = output(&f, &build).await;
    assert!(f.index.pending_operations().unwrap().is_empty());
    assert!(
        f.index
            .table_state(&f.schema.table_id)
            .unwrap()
            .pending_operation
            .is_none()
    );
    let publisher = TablePublisher::new(
        f.index.clone(),
        f.catalog.clone(),
        WriterConfig::default(),
        2,
        1 << 20,
    )
    .unwrap();

    let (epoch, collapsed) = flow_testkit::collapse_fixture(
        &f.index,
        &f.head,
        &f.schema,
        SourceId("fixture".into()),
        PgLsn(90),
        [(f.schema.encode_key(&row(3)).unwrap(), Change::Delete)],
    );
    publisher
        .publish(&f.head, &f.schema, collapsed)
        .await
        .unwrap();
    f.index.forget_applied(&epoch.id).unwrap();
    drop(publisher);
    let head = f.catalog.load_table(f.head.identifier()).await.unwrap();
    let protection = active_build_protection(&f.index, &head, f.schema.table_id).unwrap();
    assert!(protection.snapshots.contains(&base));
    assert!(
        protection
            .snapshots
            .contains(&head.metadata().current_snapshot_id().unwrap())
    );
    assert!(protection.operations.contains(build.operation_id()));
    tokio::time::sleep(Duration::from_millis(3)).await;
    f.maintenance()
        .expire_history(
            &head,
            f.schema.table_id,
            Duration::from_millis(1),
            &BTreeSet::new(),
        )
        .await
        .unwrap();
    let head = f.catalog.load_table(head.identifier()).await.unwrap();
    assert!(head.metadata().snapshot_by_id(base).is_some());
    collect(&f, &head).await;
    collect(&f, &head).await;
    assert!(head.file_io().exists(&path).await.unwrap());
    assert_eq!(sorted(scan(&f.head, &f.schema).await.unwrap()), f.expected);
    assert_eq!(
        f.index
            .lookup(&f.schema.table_id, &f.schema.encode_key(&row(10)).unwrap())
            .unwrap(),
        Some(f.locations[6].clone())
    );

    // The worker and its writer have finished; dropping ownership simulates a
    // process crash before explicit retirement. Control must retain the record.
    drop(build);
    let scratch = f.scratch("rebuild-scratch");
    drop(f.index);
    std::fs::remove_dir_all(f.temp.path().join("index")).unwrap();
    let replacement = StateStore::open(f.temp.path().join("replacement"), options()).unwrap();
    let builder = flow_coordinator::TableMaintenance::new(
        replacement.clone(),
        f.catalog.clone(),
        flow_compactor::Policy::default(),
        WriterConfig::default(),
    )
    .unwrap();
    let rebuilt = builder
        .rebuild_index(
            &head,
            &f.schema,
            replacement.clone(),
            scratch,
            PgLsn(90),
            f.schema.version,
        )
        .await
        .unwrap();
    let selected = f.control.activate_rebuilt(&rebuilt).unwrap();
    drop(builder);
    drop(replacement);
    drop(rebuilt);
    f.index = StateStore::open_with_control(selected.path, options(), f.control.clone()).unwrap();
    assert!(
        active_build_protection(&f.index, &head, f.schema.table_id)
            .unwrap()
            .snapshots
            .contains(&base)
    );
    assert_eq!(discard_abandoned_builds(&f.index).unwrap(), 1);
    assert!(
        active_build_protection(&f.index, &head, f.schema.table_id)
            .unwrap()
            .operations
            .is_empty()
    );
    // The grace clock starts only when a sweep observes the output unfenced
    // and unreferenced, after ownership is retired, even for old outputs.
    // Recovery must not bypass that window.
    collect(&f, &head).await;
    assert!(head.file_io().exists(&path).await.unwrap());
    tokio::time::sleep(Duration::from_millis(3)).await;
    collect(&f, &head).await;
    assert!(!head.file_io().exists(&path).await.unwrap());
    assert_eq!(
        sorted(scan(&head, &f.schema).await.unwrap()),
        f.expected
            .into_iter()
            .filter(|r| r[0] != flow_model::Value::Int64(3))
            .collect::<Vec<_>>()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn prepared_publication_takes_over_build_protection_and_survives_startup_cleanup() {
    for promote in [false, true] {
        let mut f = Fixture::new().await;
        let id = OperationId("prepared-build".into());
        let build =
            BuildRegistration::begin(f.index.clone(), &f.head, f.schema.table_id, id.clone())
                .await
                .unwrap();
        let path = output(&f, &build).await;
        f.index
            .begin_prepare(PreparedOperation {
                id: id.clone(),
                table_id: f.schema.table_id,
                kind: OperationKind::Rewrite,
                base_snapshot_id: f.head.metadata().current_snapshot_id(),
                last_lsn: PgLsn(80),
                schema_version: f.schema.version,
                artifacts: vec![],
                payload: vec![],
            })
            .unwrap();
        assert!(
            build.promoted().await.is_err(),
            "Building is not a durable publication plan"
        );
        assert!(build.release_after_join().await.is_err());
        f.index
            .seal_prepare(&id, vec![path.clone()], b"prepared-action".to_vec())
            .unwrap();
        if promote {
            build.promoted().await.unwrap();
        }
        drop(build);
        drop(f.index);
        f.index = StateStore::open_with_control(
            f.temp.path().join("index"),
            options(),
            f.control.clone(),
        )
        .unwrap();
        assert_eq!(
            discard_abandoned_builds(&f.index).unwrap(),
            usize::from(!promote)
        );
        assert_eq!(
            f.index.operation(&id).unwrap().unwrap().phase,
            OperationPhase::Prepared
        );
        assert_eq!(f.control.pending_operations().unwrap()[0].operation.id, id);
        tokio::time::sleep(Duration::from_millis(3)).await;
        collect(&f, &f.head).await;
        collect(&f, &f.head).await;
        assert!(f.head.file_io().exists(&path).await.unwrap());
        assert_eq!(sorted(scan(&f.head, &f.schema).await.unwrap()), f.expected);
    }
}
