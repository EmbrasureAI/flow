use flow_materializer::{DataWriter, SortKey, SortMode, SortSpec, WriterConfig};
use flow_model::{Column, ColumnType, OperationId, PgLsn, Row, TableId, TableSchema, Value};
use iceberg::io::FileIO;

fn schema() -> TableSchema {
    TableSchema {
        table_id: TableId(1),
        version: 0,
        columns: vec![Column {
            field_id: 1,
            name: "id".into(),
            data_type: ColumnType::Int64,
            nullable: false,
        }],
        primary_key: vec![0],
        append_only: false,
    }
}

fn spec() -> SortSpec {
    SortSpec {
        order_id: 4,
        keys: vec![SortKey {
            column: 0,
            descending: false,
            nulls_first: true,
        }],
    }
}

fn rows(ids: &[i64]) -> Vec<Row> {
    ids.iter().map(|id| vec![Value::Int64(*id)]).collect()
}

fn writer(dir: &std::path::Path, config: WriterConfig) -> DataWriter {
    DataWriter::new(
        FileIO::new_with_fs(),
        dir.to_str().unwrap(),
        &OperationId("sorted".into()),
        schema(),
        0,
        config,
    )
    .unwrap()
}

#[tokio::test]
async fn verified_files_are_stamped_and_each_file_is_checked_on_its_own() {
    let dir = tempfile::tempdir().unwrap();
    // One file per batch: the second file starts below the first file's end,
    // the third is out of order inside, and only the third is unstamped.
    let mut writer = writer(
        dir.path(),
        WriterConfig {
            target_file_bytes: 1,
            ..Default::default()
        },
    )
    .with_sort(&spec(), SortMode::Verify)
    .unwrap();
    for batch in [&[1, 5, 9][..], &[2, 3], &[8, 7]] {
        writer.write(&rows(batch), PgLsn(1)).await.unwrap();
    }
    let files = writer.close().await.unwrap();
    let orders: Vec<_> = files.iter().map(|file| file.sort_order_id()).collect();
    assert_eq!(orders, [Some(4), Some(4), None]);
}

#[tokio::test]
async fn sorted_mode_rejects_rows_out_of_order() {
    let dir = tempfile::tempdir().unwrap();
    let mut writer = writer(dir.path(), WriterConfig::default())
        .with_sort(&spec(), SortMode::Sorted)
        .unwrap();
    writer.write(&rows(&[1, 2]), PgLsn(1)).await.unwrap();
    let error = writer.write(&rows(&[0]), PgLsn(1)).await.unwrap_err();
    assert!(error.to_string().contains("sort order"), "{error}");
}

#[tokio::test]
async fn unsorted_writers_never_stamp() {
    let dir = tempfile::tempdir().unwrap();
    let mut writer = writer(dir.path(), WriterConfig::default());
    writer.write(&rows(&[1, 2, 3]), PgLsn(1)).await.unwrap();
    let files = writer.close().await.unwrap();
    assert_eq!(files[0].sort_order_id(), None);
}
