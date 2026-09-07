use futures::future::BoxFuture;
use iceberg::Result;
use serde::{Deserialize, Serialize};

/// Compact names for a finite, immutable family of objects.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NumberedArtifacts {
    pub prefix: String,
    pub suffix: String,
    pub count: u64,
    pub digits: u8,
}

impl NumberedArtifacts {
    pub fn path(&self, ordinal: u64) -> String {
        format!(
            "{}{:0width$}{}",
            self.prefix,
            ordinal,
            self.suffix,
            width = usize::from(self.digits)
        )
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ArtifactSet {
    pub paths: Vec<String>,
    pub ranges: Vec<NumberedArtifacts>,
}

impl ArtifactSet {
    pub fn metadata(root: &str, count: usize, manifest_list: String) -> Self {
        Self {
            paths: vec![manifest_list],
            ranges: vec![NumberedArtifacts {
                prefix: format!("{root}-m"),
                suffix: ".avro".into(),
                count: count as u64,
                digits: 0,
            }],
        }
    }

    pub fn len(&self) -> Option<u64> {
        self.ranges
            .iter()
            .try_fold(self.paths.len() as u64, |total, range| {
                total.checked_add(range.count)
            })
    }

    pub fn is_empty(&self) -> bool {
        self.paths.is_empty() && self.ranges.iter().all(|range| range.count == 0)
    }

    pub fn path(&self, mut ordinal: u64) -> Option<String> {
        if ordinal < self.paths.len() as u64 {
            return self.paths.get(ordinal as usize).cloned();
        }
        ordinal -= self.paths.len() as u64;
        for range in &self.ranges {
            if ordinal < range.count {
                return Some(range.path(ordinal));
            }
            ordinal -= range.count;
        }
        None
    }
}

/// Persist object ownership before a writer can issue its first PUT. Metadata
/// writers reserve a whole bounded group at once; data writers may reserve
/// ordinal blocks. Unmanaged callers can omit tracking entirely.
pub trait ArtifactTracker: std::fmt::Debug + Send + Sync {
    fn register(&self, artifacts: ArtifactSet) -> BoxFuture<'_, Result<()>>;
}

/// Intersect a bounded candidate set with every retained snapshot, including
/// branches and tags. Walk manifests sequentially through the bounded cache;
/// the protection set never exceeds the candidate set.
/// Deleted entries are not live references; older retained snapshots protect
/// their former live entries independently.
pub async fn retained_artifacts(
    table: &iceberg::table::Table,
    candidates: &std::collections::BTreeSet<String>,
    cache: &crate::ManifestCache,
) -> Result<std::collections::BTreeSet<String>> {
    let mut protected = std::collections::BTreeSet::new();
    let mut visited = std::collections::HashSet::new();
    if candidates.is_empty() {
        return Ok(protected);
    }
    for snapshot in table.metadata().snapshots() {
        if candidates.contains(snapshot.manifest_list()) {
            protected.insert(snapshot.manifest_list().to_owned());
        }
        for manifest in table.manifest_list_reader(snapshot).load().await?.entries() {
            if candidates.contains(&manifest.manifest_path) {
                protected.insert(manifest.manifest_path.clone());
            }
            if !visited.insert((
                manifest.manifest_path.clone(),
                manifest.sequence_number,
                manifest.added_snapshot_id,
            )) {
                continue;
            }
            let entries = cache.load(table, manifest).await?;
            for entry in entries.entries() {
                if entry.is_alive() && candidates.contains(entry.file_path()) {
                    protected.insert(entry.file_path().to_owned());
                }
            }
        }
        if protected.len() == candidates.len() {
            break;
        }
    }
    Ok(protected)
}
