use flow_materializer::{DeleteWriter, WriterConfig};
use flow_model::{FileId, OperationId, PgLsn, RowLocation};
use iceberg::{
    io::FileIO,
    puffin::{DeletionVectorLimits, read_deletion_vector},
    spec::FormatVersion,
};

fn position(target: &str, row_position: u64) -> RowLocation {
    RowLocation {
        data_file_id: FileId(target.into()),
        row_position,
        data_sequence_number: 1,
        spec_id: 0,
        partition: vec![],
        source_commit_lsn: PgLsn(1),
        row_version: 0,
        row_fingerprint: [0; 16],
    }
}

#[tokio::test]
async fn batches_and_requested_rotation_keep_one_readable_vector_per_target() -> anyhow::Result<()>
{
    let directory = tempfile::tempdir()?;
    let io = FileIO::new_with_fs();
    let targets = ["a", "b"].map(|suffix| format!("s3://bucket/{}/{suffix}", "x".repeat(1024)));
    let mut writer = DeleteWriter::new_for_version(
        io.clone(),
        directory.path().to_str().unwrap(),
        &OperationId("rotation".into()),
        0,
        WriterConfig {
            row_group_rows: 3,
            row_group_bytes: 1024,
            target_file_bytes: usize::MAX,
            ..WriterConfig::default()
        },
        FormatVersion::V3,
    )?;
    writer.write(&[position(&targets[0], 0)]).await?;
    writer.finish_file().await?;
    let remaining = [1, 65535, 65536, u32::MAX as u64, 1 << 32];
    let mut rows: Vec<_> = remaining
        .iter()
        .map(|&row| position(&targets[0], row))
        .collect();
    rows.extend([position(&targets[1], 2), position(&targets[1], 8)]);
    writer.write(&rows).await?;
    let files = writer.close().await?;
    assert_eq!(files.len(), 2);
    assert_ne!(files[0].file_path(), files[1].file_path());
    for (index, expected) in [
        vec![0, 1, 65535, 65536, u32::MAX as u64, 1 << 32],
        vec![2, 8],
    ]
    .into_iter()
    .enumerate()
    {
        let file = &files[index];
        let vector = read_deletion_vector(
            &io,
            file.file_path(),
            file.content_offset().unwrap() as u64,
            file.content_size_in_bytes().unwrap() as u64,
            &targets[index],
            file.record_count(),
            DeletionVectorLimits::default(),
        )
        .await?;
        assert_eq!(vector.iter().collect::<Vec<_>>(), expected);
    }
    Ok(())
}

#[tokio::test]
async fn exact_budget_admits_container_transition_and_rejects_next_high_key() -> anyhow::Result<()>
{
    let directory = tempfile::tempdir()?;
    let io = FileIO::new_with_fs();
    let target = "s3://bucket/a.parquet";
    // 20 framing/treemap bytes + 4 high-key bytes + 16 bitmap header bytes
    // + 8192 container bytes. The 4097th value changes array to bitmap storage.
    let budget = 8232;
    let mut writer = DeleteWriter::new_for_version(
        io,
        directory.path().to_str().unwrap(),
        &OperationId("budget".into()),
        0,
        WriterConfig {
            row_group_rows: 128,
            row_group_bytes: budget,
            ..WriterConfig::default()
        },
        FormatVersion::V3,
    )?;
    let rows: Vec<_> = (0..4097).map(|row| position(target, row)).collect();
    writer.write(&rows[..127]).await?;
    writer.write(&rows[127..]).await?;
    let error = writer
        .write(&[position(target, 1 << 32)])
        .await
        .unwrap_err();
    assert!(error.to_string().contains("memory budget"));
    Ok(())
}
