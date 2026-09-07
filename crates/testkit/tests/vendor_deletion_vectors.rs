use std::collections::HashMap;

use flow_iceberg_ext::RowDeltaAction;
use flow_materializer::{DataWriter, WriterConfig};
use flow_model::{OperationId, PgLsn, Value};
use futures::{StreamExt, TryStreamExt};
use iceberg::Runtime;
use iceberg::arrow::ArrowReaderBuilder;
use iceberg::io::FileIO;
use iceberg::puffin::{
    Blob, CompressionCodec, DELETION_VECTOR_V1, DeleteVector, DeletionVectorLimits, PuffinWriter,
    encode_deletion_vector,
};
use iceberg::scan::{FileScanTask, FileScanTaskDeleteFile};
use iceberg::spec::{DataContentType, DataFileFormat};

async fn data_tasks(temp: &tempfile::TempDir) -> (FileIO, Vec<FileScanTask>) {
    let catalog = flow_testkit::catalog(&temp.path().join("warehouse")).await;
    let schema = flow_testkit::schema(1);
    let head = flow_testkit::table(&catalog, &schema).await;
    let mut files = Vec::new();
    for count in [3, 4] {
        let mut writer = DataWriter::new(
            head.file_io().clone(),
            head.metadata().location(),
            &OperationId(format!("data-{count}")),
            schema.clone(),
            0,
            WriterConfig::default(),
        )
        .unwrap();
        let rows = (0..count)
            .map(|id| vec![Value::Int64(id), Value::Null])
            .collect::<Vec<_>>();
        writer.write(&rows, PgLsn(1)).await.unwrap();
        files.extend(writer.close().await.unwrap());
    }
    let head = RowDeltaAction::new(&head, "data")
        .add_data_files(files)
        .commit(&catalog, &head)
        .await
        .unwrap()
        .table;
    let mut tasks: Vec<FileScanTask> = head
        .scan()
        .build()
        .unwrap()
        .plan_files()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    tasks.sort_by_key(|task| task.record_count);
    (head.file_io().clone(), tasks)
}

async fn attach_vectors(io: &FileIO, path: &str, tasks: &mut [FileScanTask], positions: &[&[u64]]) {
    let mut writer = PuffinWriter::new(&io.new_output(path).unwrap(), HashMap::new(), false)
        .await
        .unwrap();
    let mut offset = 4;
    for (task, positions) in tasks.iter_mut().zip(positions) {
        let mut vector = DeleteVector::default();
        for position in *positions {
            vector.insert(*position);
        }
        let bytes = encode_deletion_vector(&vector, DeletionVectorLimits::default()).unwrap();
        let length = bytes.len() as i64;
        let blob = Blob::builder()
            .r#type(DELETION_VECTOR_V1.into())
            .fields(vec![])
            .snapshot_id(-1)
            .sequence_number(-1)
            .data(bytes)
            .properties(HashMap::from([
                ("referenced-data-file".into(), task.data_file_path.clone()),
                ("cardinality".into(), positions.len().to_string()),
            ]))
            .build();
        writer.add(blob, CompressionCodec::None).await.unwrap();
        task.deletes = vec![
            FileScanTaskDeleteFile::builder()
                .with_file_path(path.into())
                .with_file_size_in_bytes(0)
                .with_file_type(DataContentType::PositionDeletes)
                .with_partition_spec_id(0)
                .with_file_format(DataFileFormat::Puffin)
                .with_record_count(Some(positions.len() as u64))
                .with_referenced_data_file(Some(task.data_file_path.clone()))
                .with_content_offset(Some(offset))
                .with_content_size_in_bytes(Some(length))
                .build(),
        ];
        offset += length;
    }
    writer.close().await.unwrap();
    let size = io.new_input(path).unwrap().metadata().await.unwrap().size;
    for task in tasks {
        task.deletes[0].file_size_in_bytes = size;
    }
}

#[tokio::test]
async fn stock_reader_keeps_puffin_blobs_separate_and_replaces_legacy_positions() {
    let temp = tempfile::TempDir::new().unwrap();
    let (io, mut tasks) = data_tasks(&temp).await;
    let path = temp
        .path()
        .join("deletes.puffin")
        .to_str()
        .unwrap()
        .to_owned();
    attach_vectors(&io, &path, &mut tasks, &[&[0], &[0, 2]]).await;
    // A DV replaces prior positional files. This nonexistent legacy file must not be opened.
    for task in &mut tasks {
        task.deletes.push(
            FileScanTaskDeleteFile::builder()
                .with_file_path("/obsolete-positions.parquet".into())
                .with_file_size_in_bytes(100)
                .with_file_type(DataContentType::PositionDeletes)
                .with_partition_spec_id(0)
                .build(),
        );
    }
    // Duplicate a task to exercise concurrent waiters for the same blob too.
    tasks.push(tasks[0].clone());
    for concurrency in [1, 3] {
        let batches = ArrowReaderBuilder::new(io.clone(), Runtime::current())
            .with_data_file_concurrency_limit(concurrency)
            .build()
            .read(futures::stream::iter(tasks.clone().into_iter().map(Ok)).boxed())
            .unwrap()
            .stream()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(
            batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
            6
        );
    }
}

#[tokio::test]
async fn stock_reader_rejects_bad_vector_metadata_and_out_of_range_positions() {
    let temp = tempfile::TempDir::new().unwrap();
    let (io, original) = data_tasks(&temp).await;
    for case in 0..5 {
        let mut tasks = vec![original[0].clone()];
        let path = temp
            .path()
            .join(format!("bad-{case}.puffin"))
            .to_str()
            .unwrap()
            .to_owned();
        attach_vectors(
            &io,
            &path,
            &mut tasks,
            &[if case == 4 { &[3] } else { &[0] }],
        )
        .await;
        match case {
            0 => tasks[0].deletes[0].referenced_data_file = Some("wrong-target".into()),
            1 => tasks[0].deletes[0].record_count = Some(2),
            2 => tasks[0].deletes[0].content_offset = Some(5),
            3 => {
                let duplicate = tasks[0].deletes[0].clone();
                tasks[0].deletes.push(duplicate);
            }
            _ => {}
        }
        let result = ArrowReaderBuilder::new(io.clone(), Runtime::current())
            .build()
            .read(futures::stream::iter(tasks.into_iter().map(Ok)).boxed())
            .unwrap()
            .stream()
            .try_collect::<Vec<_>>()
            .await;
        assert!(
            result.is_err(),
            "invalid vector case {case} returned undeleted rows"
        );
    }
}
