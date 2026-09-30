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
/// ordinal blocks. Catalog replacement also registers the existing authoritative
/// JSON before superseding it. Unmanaged callers can omit tracking entirely.
pub trait ArtifactTracker: std::fmt::Debug + Send + Sync {
    fn register(&self, artifacts: ArtifactSet) -> BoxFuture<'_, Result<()>>;
}

// This is an optimization, not the retained-artifact protection set. Once its
// budget is full, unremembered manifests must still be scanned. A long history
// must not turn a bounded garbage-collection page into an unbounded path cache.
const MANIFEST_DEDUP_BYTES: usize = 1024 * 1024;

#[derive(Default)]
struct ManifestDedup {
    visited: std::collections::BTreeSet<(String, i64, i64)>,
    estimated_bytes: usize,
}

impl ManifestDedup {
    fn should_scan(&mut self, path: &str, sequence: i64, snapshot: i64) -> bool {
        if path.len().saturating_add(128) > MANIFEST_DEDUP_BYTES {
            return true;
        }
        let key = (path.to_owned(), sequence, snapshot);
        if self.visited.contains(&key) {
            return false;
        }
        // Include string capacity, the tuple and conservative B-tree overhead.
        // This bounds retained keys; it is not an allocator quota for the scan.
        let weight = key.0.capacity().saturating_add(128);
        if weight <= MANIFEST_DEDUP_BYTES.saturating_sub(self.estimated_bytes) {
            self.visited.insert(key);
            self.estimated_bytes += weight;
        }
        true
    }
}

/// Intersect a bounded candidate set with every retained snapshot, including
/// branches and tags. Walk manifests sequentially through the bounded cache;
/// the protection set never exceeds the candidate set. Deduplication retains at
/// most a fixed estimated byte budget; excess manifests are safely read again.
/// Deleted entries are not live references; older retained snapshots protect
/// their former live entries independently.
pub async fn retained_artifacts(
    table: &iceberg::table::Table,
    candidates: &std::collections::BTreeSet<String>,
    cache: &crate::ManifestCache,
) -> Result<std::collections::BTreeSet<String>> {
    let mut protected = std::collections::BTreeSet::new();
    let mut visited = ManifestDedup::default();
    if candidates.is_empty() {
        return Ok(protected);
    }
    // Catalog JSON is independent of snapshot expiration. Protect the current
    // pointer and every version the catalog still advertises for readers.
    for path in table.metadata_location().into_iter().chain(
        table
            .metadata()
            .metadata_log()
            .iter()
            .map(|entry| entry.metadata_file.as_str()),
    ) {
        if candidates.contains(path) {
            protected.insert(path.to_owned());
        }
    }
    // JSON-only pages do not require reading any manifests or data descriptors.
    if candidates
        .iter()
        .all(|path| path.ends_with(".metadata.json"))
    {
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
            if !visited.should_scan(
                &manifest.manifest_path,
                manifest.sequence_number,
                manifest.added_snapshot_id,
            ) {
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

const INDEX_LOAD_CONCURRENCY: usize = 16;
// Conservative per-entry estimates, including hash-table slack.
const INDEX_PATH_BYTES: usize = 32;
const INDEX_ENTRY_BYTES: usize = 96;
// A decoded manifest-list entry held until all of its list's manifests load.
const INDEX_PENDING_MANIFEST_BYTES: usize = 512;

/// Fixed-key SipHash of a live object path. Only the reference-count set of
/// paths is hashed, and membership there only retains objects: a collision can
/// keep an orphan, never delete a live file. Manifest lists and manifests, whose
/// contents decide which paths are live, are keyed by their full path.
fn path_hash(path: &str) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::hash::DefaultHasher::new();
    path.hash(&mut hasher);
    hasher.finish()
}

struct IndexedManifest {
    /// Applied lists that reference this manifest. Zero while it is loaded
    /// only for lists still pending.
    lists: u32,
    live: Box<[u64]>,
}

/// Progress of one [`RetainedIndex::sync`] call.
#[derive(Debug, Clone, Copy, Default)]
pub struct IndexProgress {
    pub manifest_lists_read: usize,
    pub manifests_read: usize,
    /// Every retained snapshot of the synchronized metadata is indexed.
    pub complete: bool,
    /// The index exceeded its memory budget and was cleared.
    pub oversized: bool,
}

/// Incremental reachability of one table's retained snapshots: their manifest
/// lists, manifests and live data/delete files, with the same semantics as
/// [`retained_artifacts`]. Manifest lists and manifests are immutable, so each
/// is read once while it stays retained, rather than on every collection page.
/// Synchronization adds newly retained lists and releases expired ones by
/// reference count, so the result tracks exactly the metadata last synced.
/// Catalog JSON is not included.
pub struct RetainedIndex {
    table: Option<uuid::Uuid>,
    hasher: fn(&str) -> u64,
    lists: std::collections::HashMap<String, Box<[std::sync::Arc<str>]>>,
    /// Lists already read whose manifests are not all loaded yet.
    pending: std::collections::HashMap<String, Vec<iceberg::spec::ManifestFile>>,
    manifests: std::collections::HashMap<std::sync::Arc<str>, IndexedManifest>,
    paths: std::collections::HashMap<u64, u32>,
    bytes: usize,
    complete: bool,
}

impl Default for RetainedIndex {
    fn default() -> Self {
        Self::with_path_hasher(path_hash)
    }
}

impl RetainedIndex {
    /// Tests use a degenerate hash to prove collisions only retain objects.
    #[doc(hidden)]
    pub fn with_path_hasher(hasher: fn(&str) -> u64) -> Self {
        Self {
            table: None,
            hasher,
            lists: Default::default(),
            pending: Default::default(),
            manifests: Default::default(),
            paths: Default::default(),
            bytes: 0,
            complete: false,
        }
    }

    fn reset(&mut self) {
        *self = Self::with_path_hasher(self.hasher);
    }

    pub fn estimated_bytes(&self) -> usize {
        self.bytes
    }

    /// Whether the last synchronization indexed every retained snapshot.
    pub fn is_complete(&self) -> bool {
        self.complete
    }

    /// Membership in the metadata last synchronized. Callers must check
    /// [`Self::is_complete`] first: a partial index protects too little.
    pub fn contains(&self, path: &str) -> bool {
        self.paths.contains_key(&(self.hasher)(path))
    }

    /// Bring the index to `table`'s retained snapshots. Manifest lists and
    /// manifests are read in bounded concurrent chunks until `deadline`, and
    /// the memory budget is checked after every chunk. At least one chunk runs,
    /// and partial progress is kept, so repeated calls always advance. A list
    /// joins the index only after all of its manifests have loaded, so an I/O
    /// error never leaves a partially counted list. Exceeding `max_bytes`
    /// clears the index and reports `oversized`.
    pub async fn sync(
        &mut self,
        table: &iceberg::table::Table,
        cache: &crate::ManifestCache,
        deadline: std::time::Instant,
        max_bytes: usize,
    ) -> Result<IndexProgress> {
        use futures::{StreamExt, TryStreamExt, stream};
        self.complete = false;
        let uuid = table.metadata().uuid();
        if self.table != Some(uuid) {
            self.reset();
            self.table = Some(uuid);
        }
        let mut missing = Vec::new();
        let mut retained = std::collections::HashSet::new();
        for snapshot in table.metadata().snapshots() {
            let list = snapshot.manifest_list();
            if retained.insert(list)
                && !self.lists.contains_key(list)
                && !self.pending.contains_key(list)
            {
                missing.push(snapshot.clone());
            }
        }
        // Release expired history before reading new lists.
        let expired: Vec<_> = self
            .lists
            .keys()
            .filter(|list| !retained.contains(list.as_str()))
            .cloned()
            .collect();
        for list in expired {
            self.remove_list(&list);
        }
        let abandoned: Vec<_> = self
            .pending
            .keys()
            .filter(|list| !retained.contains(list.as_str()))
            .cloned()
            .collect();
        if !abandoned.is_empty() {
            for list in abandoned {
                let manifests = self.pending.remove(&list).expect("pending list");
                self.bytes = self.bytes.saturating_sub(pending_bytes(&list, &manifests));
            }
            self.drop_unused_manifests();
        }
        let mut progress = IndexProgress::default();
        let hasher = self.hasher;
        let mut first = true;
        loop {
            self.apply_ready();
            if !first && std::time::Instant::now() >= deadline {
                break;
            }
            // Manifests needed by pending lists, each read once by full path.
            let mut unloaded = Vec::new();
            let mut seen = std::collections::HashSet::new();
            'lists: for manifests in self.pending.values() {
                for manifest in manifests {
                    if !self.manifests.contains_key(manifest.manifest_path.as_str())
                        && seen.insert(manifest.manifest_path.as_str())
                    {
                        unloaded.push(manifest.clone());
                        if unloaded.len() == INDEX_LOAD_CONCURRENCY {
                            break 'lists;
                        }
                    }
                }
            }
            if !unloaded.is_empty() {
                let loaded: Vec<(String, Box<[u64]>)> = stream::iter(unloaded)
                    .map(|manifest| async move {
                        let entries = cache.peek_or_read(table, &manifest).await?;
                        let mut live: Vec<_> = entries
                            .entries()
                            .iter()
                            .filter(|entry| entry.is_alive())
                            .map(|entry| hasher(entry.file_path()))
                            .collect();
                        live.sort_unstable();
                        live.dedup();
                        Ok::<_, iceberg::Error>((manifest.manifest_path, live.into_boxed_slice()))
                    })
                    .buffer_unordered(INDEX_LOAD_CONCURRENCY)
                    .try_collect()
                    .await?;
                progress.manifests_read += loaded.len();
                for (path, live) in loaded {
                    self.bytes += manifest_bytes(&path, &live);
                    self.manifests
                        .insert(path.into(), IndexedManifest { lists: 0, live });
                }
            } else if !missing.is_empty() {
                let batch: Vec<_> = missing
                    .drain(..missing.len().min(INDEX_LOAD_CONCURRENCY))
                    .collect();
                let lists: Vec<(String, Vec<iceberg::spec::ManifestFile>)> = stream::iter(batch)
                    .map(|snapshot| async move {
                        let list = table.manifest_list_reader(&snapshot).load().await?;
                        Ok::<_, iceberg::Error>((
                            snapshot.manifest_list().to_owned(),
                            list.consume_entries().into_iter().collect(),
                        ))
                    })
                    .buffer_unordered(INDEX_LOAD_CONCURRENCY)
                    .try_collect()
                    .await?;
                progress.manifest_lists_read += lists.len();
                for (list, manifests) in lists {
                    self.bytes += pending_bytes(&list, &manifests);
                    self.pending.insert(list, manifests);
                }
            } else {
                break;
            }
            first = false;
            if self.bytes > max_bytes {
                self.reset();
                progress.oversized = true;
                return Ok(progress);
            }
        }
        self.apply_ready();
        self.complete = missing.is_empty() && self.pending.is_empty();
        progress.complete = self.complete;
        Ok(progress)
    }

    /// Count every pending list whose manifests have all loaded.
    fn apply_ready(&mut self) {
        let ready: Vec<_> = self
            .pending
            .iter()
            .filter(|(_, manifests)| {
                manifests
                    .iter()
                    .all(|manifest| self.manifests.contains_key(manifest.manifest_path.as_str()))
            })
            .map(|(list, _)| list.clone())
            .collect();
        for list in ready {
            let manifests = self.pending.remove(&list).expect("ready list");
            self.bytes = self.bytes.saturating_sub(pending_bytes(&list, &manifests));
            let mut keys: Vec<std::sync::Arc<str>> = manifests
                .iter()
                .map(|manifest| {
                    self.manifests
                        .get_key_value(manifest.manifest_path.as_str())
                        .expect("loaded manifest")
                        .0
                        .clone()
                })
                .collect();
            keys.sort_unstable();
            keys.dedup();
            self.add_list(list, keys.into_boxed_slice());
        }
    }

    fn add_list(&mut self, list: String, manifests: Box<[std::sync::Arc<str>]>) {
        self.bytes += list_bytes(&list, &manifests);
        retain_path(&mut self.paths, &mut self.bytes, (self.hasher)(&list));
        for path in &manifests {
            let manifest = self
                .manifests
                .get_mut(path)
                .expect("list manifests are loaded before the list is added");
            if manifest.lists == 0 {
                retain_path(&mut self.paths, &mut self.bytes, (self.hasher)(path));
                for live in &manifest.live {
                    retain_path(&mut self.paths, &mut self.bytes, *live);
                }
            }
            manifest.lists += 1;
        }
        self.lists.insert(list, manifests);
    }

    fn remove_list(&mut self, list: &str) {
        let Some(manifests) = self.lists.remove(list) else {
            return;
        };
        self.bytes = self.bytes.saturating_sub(list_bytes(list, &manifests));
        release_path(&mut self.paths, &mut self.bytes, (self.hasher)(list));
        for path in &manifests {
            let Some(manifest) = self.manifests.get_mut(path) else {
                continue;
            };
            manifest.lists -= 1;
            if manifest.lists == 0 {
                let manifest = self.manifests.remove(path).expect("present manifest");
                self.bytes = self
                    .bytes
                    .saturating_sub(manifest_bytes(path, &manifest.live));
                release_path(&mut self.paths, &mut self.bytes, (self.hasher)(path));
                for live in &manifest.live {
                    release_path(&mut self.paths, &mut self.bytes, *live);
                }
            }
        }
    }

    /// Forget loaded manifests that neither an applied nor a pending list uses.
    fn drop_unused_manifests(&mut self) {
        let needed: std::collections::HashSet<&str> = self
            .pending
            .values()
            .flatten()
            .map(|manifest| manifest.manifest_path.as_str())
            .collect();
        let unused: Vec<_> = self
            .manifests
            .iter()
            .filter(|(path, manifest)| manifest.lists == 0 && !needed.contains(&***path))
            .map(|(path, _)| path.clone())
            .collect();
        for path in unused {
            let manifest = self.manifests.remove(&path).expect("unused manifest");
            self.bytes = self
                .bytes
                .saturating_sub(manifest_bytes(&path, &manifest.live));
        }
    }
}

fn manifest_bytes(path: &str, live: &[u64]) -> usize {
    INDEX_ENTRY_BYTES + path.len() + live.len() * 8
}
fn list_bytes(list: &str, manifests: &[std::sync::Arc<str>]) -> usize {
    INDEX_ENTRY_BYTES + list.len() + manifests.len() * 16
}
fn pending_bytes(list: &str, manifests: &[iceberg::spec::ManifestFile]) -> usize {
    INDEX_ENTRY_BYTES + list.len() + manifests.len() * INDEX_PENDING_MANIFEST_BYTES
}

fn retain_path(paths: &mut std::collections::HashMap<u64, u32>, bytes: &mut usize, hash: u64) {
    let count = paths.entry(hash).or_insert_with(|| {
        *bytes += INDEX_PATH_BYTES;
        0
    });
    *count += 1;
}

fn release_path(paths: &mut std::collections::HashMap<u64, u32>, bytes: &mut usize, hash: u64) {
    if let std::collections::hash_map::Entry::Occupied(mut entry) = paths.entry(hash) {
        *entry.get_mut() -= 1;
        if *entry.get() == 0 {
            entry.remove();
            *bytes = bytes.saturating_sub(INDEX_PATH_BYTES);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manifest_dedup_keeps_scanning_when_history_exceeds_its_memory_budget() {
        let mut dedup = ManifestDedup::default();
        assert!(dedup.should_scan("shared.avro", 1, 1));
        for snapshot in 2..20_000 {
            assert!(dedup.should_scan("history.avro", snapshot, snapshot));
        }
        assert!(dedup.estimated_bytes <= MANIFEST_DEDUP_BYTES);
        // A remembered manifest can still be skipped, while unremembered ones
        // must be read on every encounter so they cannot lose GC protection.
        assert!(!dedup.should_scan("shared.avro", 1, 1));
        let retained = dedup.estimated_bytes;
        for _ in 0..2 {
            assert!(dedup.should_scan("history.avro", 20_000, 20_000));
            assert!(dedup.should_scan(&"x".repeat(MANIFEST_DEDUP_BYTES), 1, 1));
        }
        assert_eq!(dedup.estimated_bytes, retained);
    }
}
