use iceberg::Result;
use iceberg::io::FileIO;
use iceberg::spec::{
    DataContentType, DataFile, FormatVersion, ManifestContentType, ManifestList, ManifestListWriter,
};
use iceberg::table::Table;

use crate::action::manifest_writer;
use crate::{CommitBase, invalid};

/// Durable native-Iceberg descriptions of immutable prepared data artifacts.
#[derive(Debug, Clone)]
pub struct ArtifactPlan {
    pub added_data: Vec<DataFile>,
    pub added_deletes: Vec<DataFile>,
}

/// Write native manifests and their native manifest list, with the list written
/// last as the completion marker. `path` must be a unique immutable operation
/// location. Persist the operation's CommitBase separately before publication.
pub async fn write_artifact_plan(
    table: &Table,
    path: &str,
    added_data: &[DataFile],
    added_deletes: &[DataFile],
) -> Result<()> {
    CommitBase::new(table).validate(table)?;
    for candidate in [
        path.to_owned(),
        format!("{path}-m0.avro"),
        format!("{path}-m1.avro"),
    ] {
        if table.file_io().exists(&candidate).await? {
            return Err(invalid("artifact plan attempt already exists"));
        }
    }
    let root = path;
    let mut manifests = Vec::new();
    for (number, (files, content)) in [
        (added_data, ManifestContentType::Data),
        (added_deletes, ManifestContentType::Deletes),
    ]
    .into_iter()
    .enumerate()
    {
        if files.is_empty() {
            continue;
        }
        let mut writer = manifest_writer(table, 0, root, number, content)?;
        for file in files {
            writer.add_file(file.clone(), -1)?;
        }
        manifests.push(writer.write_manifest_file().await?);
    }
    let output = table.file_io().new_output(path)?.writer().await?;
    // Prepared plans deliberately leave lineage unassigned until publication.
    let mut writer = if table.metadata().format_version() == FormatVersion::V3 {
        ManifestListWriter::v3(output, 0, None, 0, None)
    } else {
        ManifestListWriter::v2(output, 0, None, 0)
    };
    writer.add_manifests(manifests.into_iter())?;
    writer.close().await
}

pub async fn read_artifact_plan(file_io: &FileIO, path: &str) -> Result<ArtifactPlan> {
    let bytes = file_io.new_input(path)?.read().await?;
    let manifests = ManifestList::parse_with_version(&bytes, FormatVersion::V3)?;
    let mut plan = ArtifactPlan {
        added_data: Vec::new(),
        added_deletes: Vec::new(),
    };
    for manifest in manifests.entries() {
        let entries = manifest.load_manifest(file_io).await?;
        for entry in entries.entries() {
            if !entry.is_alive() {
                return Err(invalid("artifact plan contains a removed entry"));
            }
            match entry.content_type() {
                DataContentType::Data => plan.added_data.push(entry.data_file.clone()),
                DataContentType::PositionDeletes => {
                    plan.added_deletes.push(entry.data_file.clone())
                }
                DataContentType::EqualityDeletes => {
                    return Err(invalid("artifact plan contains equality deletes"));
                }
            }
        }
    }
    Ok(plan)
}
