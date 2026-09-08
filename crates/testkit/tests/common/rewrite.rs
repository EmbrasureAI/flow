use flow_compactor::Policy;
use flow_coordinator::TableMaintenance;
use flow_iceberg_ext::RowDeltaAction;
use flow_materializer::{DataWriter, DeleteWriter, WriterConfig};
use flow_model::{OperationId, PgLsn, Row, RowLocation, TableSchema, Value};
use flow_state_store::{
    ControlStore, IndexDelta, OperationKind, PreparedOperation, StateStore, StateStoreOptions,
};
use flow_testkit::{LostResponseCatalog, catalog, scan, schema, table};
use iceberg::table::Table;
use std::sync::Arc;
use tempfile::TempDir;

pub struct Fixture {
    pub temp: TempDir,
    pub catalog: Arc<LostResponseCatalog>,
    pub control: ControlStore,
    pub index: StateStore,
    pub schema: TableSchema,
    pub head: Table,
    pub locations: Vec<RowLocation>,
    pub expected: Vec<Row>,
}

pub fn row(id: i64) -> Row {
    vec![Value::Int64(id), Value::Null]
}
pub fn options() -> StateStoreOptions {
    StateStoreOptions {
        apply_batch_rows: 2,
        ..Default::default()
    }
}
pub fn sorted(mut rows: Vec<Row>) -> Vec<Row> {
    rows.sort_by_key(|row| match row[0] {
        Value::Int64(id) => id,
        _ => unreachable!(),
    });
    rows
}
pub async fn delete(
    table: &Table,
    id: &str,
    mut positions: Vec<RowLocation>,
) -> Vec<iceberg::spec::DataFile> {
    positions.sort_by_key(|p| (p.data_file_id.clone(), p.row_position));
    let mut writer = DeleteWriter::new(
        table.file_io().clone(),
        table.metadata().location(),
        &OperationId(id.into()),
        0,
        WriterConfig::default(),
    )
    .unwrap();
    writer.write(&positions).await.unwrap();
    writer.close().await.unwrap()
}
impl Fixture {
    pub async fn new() -> Self {
        let temp = TempDir::new().unwrap();
        let catalog = Arc::new(LostResponseCatalog::new(
            catalog(&temp.path().join("warehouse")).await,
        ));
        let schema = schema(1);
        let mut head = table(catalog.as_ref(), &schema).await;
        let mut locations = Vec::new();
        let mut data = Vec::new();
        for (part, start) in [0, 10].into_iter().enumerate() {
            let mut writer = DataWriter::new(
                head.file_io().clone(),
                head.metadata().location(),
                &OperationId(format!("flow-l{}-base-{part}", usize::from(part == 0))),
                schema.clone(),
                0,
                WriterConfig::default(),
            )
            .unwrap();
            locations.extend(
                writer
                    .write(&(start..start + 6).map(row).collect::<Vec<_>>(), PgLsn(80))
                    .await
                    .unwrap()
                    .locations,
            );
            data.extend(writer.close().await.unwrap());
        }
        let first = RowDeltaAction::new(&head, "first-data")
            .add_data_files(vec![data[0].clone()])
            .commit(catalog.as_ref(), &head)
            .await
            .unwrap();
        locations[..6]
            .iter_mut()
            .for_each(|p| p.data_sequence_number = first.sequence_number);
        head = first.table;
        // The older delete mentions a not-yet-live path. It must stay ineffective
        // after that path is added at a higher sequence and deletes are merged.
        let old = delete(
            &head,
            "old-deletes",
            vec![
                locations[0].clone(),
                locations[1].clone(),
                locations[6].clone(),
            ],
        )
        .await;
        head = RowDeltaAction::new(&head, "old-deletes")
            .add_delete_files(old)
            .validate_data_files_exist([data[0].file_path().into()])
            .commit(catalog.as_ref(), &head)
            .await
            .unwrap()
            .table;
        let next = RowDeltaAction::new(&head, "next-data")
            .add_data_files(vec![data[1].clone()])
            .commit(catalog.as_ref(), &head)
            .await
            .unwrap();
        locations[6..]
            .iter_mut()
            .for_each(|p| p.data_sequence_number = next.sequence_number);
        head = next.table;
        for (id, indices) in [("shared-a", vec![1, 7]), ("shared-b", vec![2, 8])] {
            let files = delete(
                &head,
                id,
                indices.into_iter().map(|i| locations[i].clone()).collect(),
            )
            .await;
            head = RowDeltaAction::new(&head, id)
                .add_delete_files(files)
                .validate_data_files_exist(data.iter().map(|file| file.file_path().into()))
                .commit(catalog.as_ref(), &head)
                .await
                .unwrap()
                .table;
        }
        let expected = [3, 4, 5, 10, 13, 14, 15]
            .into_iter()
            .map(row)
            .collect::<Vec<_>>();
        assert_eq!(sorted(scan(&head, &schema).await.unwrap()), expected);
        let control = ControlStore::open(temp.path().join("control")).unwrap();
        let index = control
            .initialize_index(temp.path().join("index"), options())
            .unwrap();
        let id = OperationId("seed-index".into());
        index
            .prepare(
                PreparedOperation {
                    id: id.clone(),
                    table_id: schema.table_id,
                    kind: OperationKind::Ingest,
                    base_snapshot_id: None,
                    last_lsn: PgLsn(80),
                    schema_version: schema.version,
                    artifacts: vec![],
                    payload: vec![],
                },
                [3, 4, 5, 6, 9, 10, 11].into_iter().map(|i| IndexDelta {
                    key: schema
                        .encode_key(&row(if i < 6 { i as i64 } else { i as i64 + 4 }))
                        .unwrap(),
                    expected: None,
                    replacement: Some(locations[i].clone()),
                }),
            )
            .unwrap();
        index
            .mark_committed(
                &id,
                head.metadata().current_snapshot_id().unwrap(),
                head.metadata()
                    .current_snapshot()
                    .unwrap()
                    .sequence_number(),
            )
            .unwrap();
        index.apply_committed(&id).unwrap();
        index.forget_applied(&id).unwrap();
        Self {
            temp,
            catalog,
            control,
            index,
            schema,
            head,
            locations,
            expected,
        }
    }
    pub fn scratch(&self, name: &str) -> StateStore {
        StateStore::open(self.temp.path().join(name), options()).unwrap()
    }
    pub fn maintenance(&self) -> TableMaintenance {
        TableMaintenance::new(
            self.index.clone(),
            self.catalog.clone(),
            Policy::default(),
            WriterConfig::default(),
        )
        .unwrap()
    }
}
