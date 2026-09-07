use futures::{StreamExt, TryStreamExt};
use iceberg::Runtime;
use iceberg::arrow::ArrowReaderBuilder;
use iceberg::io::FileIOBuilder;
use iceberg::scan::{FileScanTask, FileScanTaskDeleteFile};
use std::sync::{Arc, atomic::Ordering};
use std::time::Duration;

#[path = "common/gated_storage.rs"]
mod gated_storage;
use gated_storage::{Gate, GatedStorage};

async fn input() -> (tempfile::TempDir, Vec<FileScanTask>) {
    use flow_iceberg_ext::RowDeltaAction;
    use flow_materializer::{DataWriter, WriterConfig};
    use flow_model::{OperationId, PgLsn, Value};
    let temp = tempfile::TempDir::new().unwrap();
    let catalog = flow_testkit::catalog(&temp.path().join("warehouse")).await;
    let schema = flow_testkit::schema(1);
    let head = flow_testkit::table(&catalog, &schema).await;
    let mut writer = DataWriter::new(
        head.file_io().clone(),
        head.metadata().location(),
        &OperationId("input".into()),
        schema,
        0,
        WriterConfig::default(),
    )
    .unwrap();
    writer
        .write(&[vec![Value::Int64(1), Value::Null]], PgLsn(1))
        .await
        .unwrap();
    let head = RowDeltaAction::new(&head, "input")
        .add_data_files(writer.close().await.unwrap())
        .commit(&catalog, &head)
        .await
        .unwrap()
        .table;
    let tasks = head
        .scan()
        .build()
        .unwrap()
        .plan_files()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    (temp, tasks)
}

fn delete(path: &str) -> FileScanTaskDeleteFile {
    FileScanTaskDeleteFile::builder()
        .with_file_path(path.into())
        .with_file_size_in_bytes(128)
        .with_file_type(iceberg::spec::DataContentType::PositionDeletes)
        .with_partition_spec_id(0)
        .build()
}

#[tokio::test]
async fn shared_delete_waiters_fail_on_owner_error_and_sibling_cancellation() {
    for sibling in [false, true] {
        let (_temp, mut tasks) = input().await;
        let gate = Arc::new(Gate::default());
        tasks[0].deletes = vec![delete("/owner.parquet")];
        let waiting = tasks[0].clone();
        if sibling {
            tasks[0].deletes.push(delete("/sibling.parquet"));
        }
        tasks.push(waiting);
        let io = FileIOBuilder::new(Arc::new(GatedStorage {
            gate: gate.clone(),
            fail: true,
            owner_path: "/owner.parquet".into(),
            sibling_path: if sibling {
                "/sibling.parquet".into()
            } else {
                String::new()
            },
        }))
        .build();
        let mut stream = ArrowReaderBuilder::new(io, Runtime::current())
            .with_data_file_concurrency_limit(2)
            .build()
            .read(futures::stream::iter(tasks.into_iter().map(Ok)).boxed())
            .unwrap()
            .stream();
        let scan = tokio::spawn(async move {
            let mut errors = 0;
            while let Some(batch) = stream.next().await {
                assert!(
                    batch.is_err(),
                    "a failed delete must not produce undeleted rows"
                );
                errors += 1;
            }
            errors
        });
        tokio::time::timeout(
            Duration::from_secs(5),
            gate.started.acquire_many(if sibling { 2 } else { 1 }),
        )
        .await
        .unwrap()
        .unwrap()
        .forget();
        gate.release.add_permits(1);
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(5), scan)
                .await
                .unwrap()
                .unwrap(),
            2
        );
        assert_eq!(gate.active.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn dropping_scan_cancels_owned_delete_io() {
    let (_temp, mut tasks) = input().await;
    let gate = Arc::new(Gate::default());
    tasks[0].deletes = vec![delete("/owner.parquet")];
    let io = FileIOBuilder::new(Arc::new(GatedStorage {
        gate: gate.clone(),
        fail: true,
        owner_path: "/owner.parquet".into(),
        sibling_path: String::new(),
    }))
    .build();
    let mut stream = ArrowReaderBuilder::new(io, Runtime::current())
        .build()
        .read(futures::stream::iter(tasks.into_iter().map(Ok)).boxed())
        .unwrap()
        .stream();
    let scan = tokio::spawn(async move { stream.next().await });
    tokio::time::timeout(Duration::from_secs(5), gate.started.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    scan.abort();
    assert!(scan.await.unwrap_err().is_cancelled());
    tokio::time::timeout(Duration::from_secs(5), gate.finished.notified())
        .await
        .unwrap();
    assert_eq!(
        gate.active.load(Ordering::SeqCst),
        0,
        "delete task outlived its scan"
    );
}
