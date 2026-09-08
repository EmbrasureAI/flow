use flow_iceberg_ext::{ArtifactSet, ArtifactTracker, RewriteFilesAction, RowDeltaAction};
use flow_materializer::{DataWriter, DeleteWriter, WriterConfig};
use flow_model::{OperationId, PgLsn, Value};
use flow_testkit::{catalog, scan, schema, table};
use futures::future::BoxFuture;
use iceberg::io::FileIO;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use tempfile::TempDir;

#[derive(Debug)]
struct RegistrationGate {
    io: FileIO,
    reject: AtomicBool,
    registered: Mutex<Vec<ArtifactSet>>,
}
impl ArtifactTracker for RegistrationGate {
    fn register(&self, artifacts: ArtifactSet) -> BoxFuture<'_, iceberg::Result<()>> {
        Box::pin(async move {
            for index in 0..artifacts.len().unwrap() {
                assert!(
                    !self
                        .io
                        .exists(&artifacts.path(index).unwrap())
                        .await
                        .unwrap(),
                    "registration must precede every PUT"
                );
            }
            self.registered.lock().unwrap().push(artifacts);
            if self.reject.load(Ordering::SeqCst) {
                return Err(iceberg::Error::new(
                    iceberg::ErrorKind::Unexpected,
                    "ownership authority unavailable",
                ));
            }
            Ok(())
        })
    }
}
impl RegistrationGate {
    async fn verify_outputs(&self, present: bool) {
        let groups = self
            .registered
            .lock()
            .unwrap()
            .drain(..)
            .collect::<Vec<_>>();
        assert!(!groups.is_empty());
        for group in groups {
            for ordinal in 0..group.len().unwrap() {
                assert_eq!(
                    self.io.exists(&group.path(ordinal).unwrap()).await.unwrap(),
                    present
                );
            }
        }
    }
}

#[tokio::test]
async fn registration_failure_prevents_data_delete_and_catalog_metadata_uploads() {
    let temp = TempDir::new().unwrap();
    let catalog = catalog(&temp.path().join("warehouse")).await;
    let schema = schema(1);
    let table = table(&catalog, &schema).await;
    let gate = Arc::new(RegistrationGate {
        io: table.file_io().clone(),
        reject: AtomicBool::new(true),
        registered: Mutex::new(Vec::new()),
    });
    let config = WriterConfig {
        artifact_tracker: Some(gate.clone()),
        ..Default::default()
    };
    let rows = vec![
        vec![Value::Int64(1), Value::Null],
        vec![Value::Int64(2), Value::Null],
    ];
    let make_writer = |name: &str| {
        DataWriter::new(
            table.file_io().clone(),
            table.metadata().location(),
            &OperationId(name.into()),
            schema.clone(),
            0,
            config.clone(),
        )
        .unwrap()
    };
    let mut writer = make_writer("rejected-data");
    assert!(writer.write(&rows, PgLsn(10)).await.is_err());
    gate.verify_outputs(false).await;
    gate.reject.store(false, Ordering::SeqCst);
    let mut writer = make_writer("accepted-data");
    let locations = writer.write(&rows, PgLsn(10)).await.unwrap().locations;
    let data = writer.close().await.unwrap();
    gate.verify_outputs(true).await;
    gate.reject.store(true, Ordering::SeqCst);
    let action = RowDeltaAction::new(&table, "ingest")
        .add_data_files(data.clone())
        .with_artifact_tracker(Some(gate.clone()));
    assert!(action.commit(&catalog, &table).await.is_err());
    gate.verify_outputs(false).await;
    gate.reject.store(false, Ordering::SeqCst);
    let initial = action.commit(&catalog, &table).await.unwrap().table;
    gate.verify_outputs(true).await;
    let make_delete = |name: &str| {
        DeleteWriter::new(
            table.file_io().clone(),
            table.metadata().location(),
            &OperationId(name.into()),
            0,
            config.clone(),
        )
        .unwrap()
    };
    gate.reject.store(true, Ordering::SeqCst);
    assert!(
        make_delete("rejected-delete")
            .write(&locations[..1])
            .await
            .is_err()
    );
    gate.verify_outputs(false).await;
    gate.reject.store(false, Ordering::SeqCst);
    let mut delete = make_delete("accepted-delete");
    delete.write(&locations[..1]).await.unwrap();
    let deletes = delete.close().await.unwrap();
    gate.verify_outputs(true).await;
    let updated = RowDeltaAction::new(&initial, "delete")
        .add_delete_files(deletes.clone())
        .validate_data_files_exist([data[0].file_path().to_owned()])
        .with_artifact_tracker(Some(gate.clone()))
        .commit(&catalog, &initial)
        .await
        .unwrap()
        .table;
    gate.verify_outputs(true).await;
    let mut writer = make_writer("rewrite-data");
    writer.write(&rows[1..], PgLsn(20)).await.unwrap();
    let replacement = writer.close().await.unwrap();
    gate.verify_outputs(true).await;
    let rewritten = RewriteFilesAction::new(&updated, "rewrite")
        .remove_data_files([data[0].file_path().to_owned()])
        .remove_delete_files([deletes[0].file_path().to_owned()])
        .add_data_files(replacement)
        .with_artifact_tracker(Some(gate.clone()))
        .commit(&catalog, &updated)
        .await
        .unwrap()
        .table;
    gate.verify_outputs(true).await;
    assert_eq!(scan(&rewritten, &schema).await.unwrap(), rows[1..]);
}
