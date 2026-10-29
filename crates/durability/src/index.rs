//! Replicated row-index checkpoints.
//!
//! The row index is derived state, rebuilt from Iceberg after losing it. A
//! replicated checkpoint bounds that rebuild: a replacement owner downloads
//! the newest registered checkpoint and advances it from Iceberg file changes.
//! Checkpoint files are content addressed, so successive checkpoints upload
//! only new SST files. Nothing here is recovery authority; a missing or damaged
//! checkpoint only falls back to a full rebuild.
use crate::store;
use crate::writer::Shared;
use anyhow::{Context, Result, ensure};
use futures::{StreamExt, TryStreamExt};
use opendal::Operator;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeSet, HashMap},
    fs,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Instant,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const FILES: &str = "index/files/";
const CHECKPOINTS: &str = "index/checkpoints/";
const CONCURRENCY: usize = 8;
/// Files are hashed and uploaded in pieces of this size, never read whole.
const CHUNK: usize = 8 << 20;

/// Saved beside the local checkpoints, so a restart does not hash every SST
/// file again. Derived state: a missing or unreadable file only means hashing.
const HASHES_FILE: &str = "replicated-hashes.json";

/// Identifies an immutable file's contents without reading it. Successive
/// checkpoints hard-link the same SST files, so their hashes carry over.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
struct FileKey {
    device: u64,
    inode: u64,
    size: u64,
    modified_ns: i128,
}

impl FileKey {
    fn of(metadata: &fs::Metadata) -> Option<Self> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            Some(Self {
                device: metadata.dev(),
                inode: metadata.ino(),
                size: metadata.size(),
                modified_ns: i128::from(metadata.mtime()) * 1_000_000_000
                    + i128::from(metadata.mtime_nsec()),
            })
        }
        #[cfg(not(unix))]
        {
            let _ = metadata;
            None
        }
    }
}

/// Hashes of the SST files in the last uploaded checkpoint, loaded from the
/// checkpoints directory on first use.
#[derive(Default)]
pub(crate) struct Hashes(Mutex<Option<HashMap<FileKey, String>>>);

impl Hashes {
    fn known(&self, checkpoints: &Path) -> HashMap<FileKey, String> {
        let mut known = self.0.lock().unwrap_or_else(|e| e.into_inner());
        known
            .get_or_insert_with(|| {
                fs::read(checkpoints.join(HASHES_FILE))
                    .ok()
                    .and_then(|bytes| serde_json::from_slice::<Vec<(FileKey, String)>>(&bytes).ok())
                    .map(|entries| entries.into_iter().collect())
                    .unwrap_or_default()
            })
            .clone()
    }

    fn remember(&self, checkpoints: &Path, hashes: HashMap<FileKey, String>) {
        // Renamed into place, so a crash leaves the old file or the new one.
        let saved = (|| -> Result<()> {
            let entries: Vec<_> = hashes.iter().collect();
            let temporary = checkpoints.join(format!("{HASHES_FILE}.tmp"));
            fs::write(&temporary, serde_json::to_vec(&entries)?)?;
            fs::rename(&temporary, checkpoints.join(HASHES_FILE))?;
            Ok(())
        })();
        if let Err(error) = saved {
            tracing::warn!(error = %format!("{error:#}"), "could not save replicated index checkpoint hashes");
        }
        *self.0.lock().unwrap_or_else(|e| e.into_inner()) = Some(hashes);
    }
}

fn hash_file(path: &Path) -> Result<(String, u64)> {
    let mut hasher = blake3::Hasher::new();
    let size = std::io::copy(&mut fs::File::open(path)?, &mut hasher)?;
    Ok((hasher.finalize().to_hex().to_string(), size))
}

/// Stream a file into a new object, which is created only if the bytes sent
/// still have the hash the object is named by.
async fn upload_file(operator: &Operator, object: &str, path: &Path, hash: &str) -> Result<()> {
    let mut file = tokio::fs::File::open(path).await?;
    let mut writer = operator
        .writer_with(object)
        .chunk(CHUNK)
        .await
        .with_context(|| format!("upload {object}"))?;
    let mut hasher = blake3::Hasher::new();
    loop {
        let mut buffer = Vec::with_capacity(CHUNK);
        let read = (&mut file)
            .take(CHUNK as u64)
            .read_to_end(&mut buffer)
            .await?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer);
        if let Err(error) = writer.write(buffer).await {
            let _ = writer.abort().await;
            return Err(error).with_context(|| format!("upload {object}"));
        }
    }
    if hasher.finalize().to_hex().as_str() != hash {
        let _ = writer.abort().await;
        anyhow::bail!("{} changed while uploading", path.display());
    }
    writer
        .close()
        .await
        .with_context(|| format!("upload {object}"))?;
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Manifest {
    version: u32,
    name: String,
    files: Vec<File>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct File {
    path: String,
    object: String,
    size: u64,
    blake3: String,
}

fn manifest_key(name: &str) -> String {
    format!("{CHECKPOINTS}{name}.json")
}

fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
        && !name.starts_with('.')
}

/// Upload every file of a local checkpoint directory, then its manifest.
/// Like every write after startup, each object write starts only while the
/// lease outlasts the request; without one the checkpoint is skipped.
pub(crate) async fn upload(
    shared: &Shared,
    operator: &Operator,
    hashes: &Hashes,
    directory: &Path,
) -> Result<(u64, u64)> {
    let name = directory
        .file_name()
        .and_then(|name| name.to_str())
        .filter(|name| valid_name(name))
        .context("index checkpoint directory has no usable name")?
        .to_owned();
    let mut paths = Vec::new();
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        ensure!(
            entry.file_type()?.is_file(),
            "index checkpoint contains a non-regular entry"
        );
        paths.push(entry.path());
    }
    paths.sort();
    let existing: BTreeSet<String> = store::list_recursive(operator, FILES)
        .await?
        .into_iter()
        .collect();
    let existing = Arc::new(existing);
    let checkpoints = directory
        .parent()
        .context("index checkpoint directory has no parent")?;
    let known = Arc::new(hashes.known(checkpoints));
    let results: Vec<(File, u64, Option<FileKey>)> = futures::stream::iter(paths)
        .map(|path| {
            let operator = operator.clone();
            let existing = existing.clone();
            let known = known.clone();
            async move {
                let file_name = path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .context("checkpoint file name is not UTF-8")?
                    .to_owned();
                // RocksDB never rewrites an SST file; other files are small.
                let key = file_name
                    .ends_with(".sst")
                    .then(|| FileKey::of(&fs::metadata(&path).ok()?))
                    .flatten();
                let (hash, size) = match key.and_then(|key| Some((known.get(&key)?, key))) {
                    Some((hash, key)) => (hash.clone(), key.size),
                    None => {
                        let path = path.clone();
                        tokio::task::spawn_blocking(move || hash_file(&path)).await??
                    }
                };
                let object = format!("{FILES}{hash}-{size}");
                let mut uploaded = 0;
                if !existing.contains(&object) {
                    shared.require_lease(shared.tuning.request_timeout, "upload")?;
                    upload_file(&operator, &object, &path, &hash).await?;
                    uploaded = size;
                }
                Ok::<_, anyhow::Error>((
                    File {
                        path: file_name,
                        object,
                        size,
                        blake3: hash,
                    },
                    uploaded,
                    key,
                ))
            }
        })
        .buffered(CONCURRENCY)
        .try_collect()
        .await?;
    hashes.remember(
        checkpoints,
        results
            .iter()
            .filter_map(|(file, _, key)| Some(((*key)?, file.blake3.clone())))
            .collect(),
    );
    let uploaded = results.iter().map(|(_, bytes, _)| bytes).sum();
    let total = results.iter().map(|(file, _, _)| file.size).sum();
    let manifest = Manifest {
        version: 1,
        name: name.clone(),
        files: results.into_iter().map(|(file, _, _)| file).collect(),
    };
    shared.require_lease(shared.tuning.request_timeout, "upload")?;
    operator
        .write(&manifest_key(&name), serde_json::to_vec(&manifest)?)
        .await
        .context("write index checkpoint manifest")?;
    Ok((total, uploaded))
}

/// Delete replicated checkpoints other than `keep`, and files none of them use.
pub(crate) async fn retain(shared: &Shared, operator: &Operator, keep: &[String]) -> Result<usize> {
    let keep: BTreeSet<String> = keep.iter().map(|name| manifest_key(name)).collect();
    let mut referenced = BTreeSet::new();
    let mut doomed = Vec::new();
    for key in store::list_recursive(operator, CHECKPOINTS).await? {
        if keep.contains(&key) {
            if let Some(bytes) = store::read_optional(operator, &key).await? {
                let manifest: Manifest = serde_json::from_slice(&bytes)?;
                referenced.extend(manifest.files.into_iter().map(|file| file.object));
            }
        } else {
            doomed.push(key);
        }
    }
    for key in store::list_recursive(operator, FILES).await? {
        if !referenced.contains(&key) {
            doomed.push(key);
        }
    }
    if doomed.is_empty() {
        return Ok(0);
    }
    shared.require_lease(shared.tuning.lease / 2, "delete objects")?;
    let deleted = store::delete_all(operator, doomed).await?;
    metrics::counter!("flow_durability_deleted_objects_total").increment(deleted as u64);
    shared.lock().stats.deleted_objects += deleted as u64;
    Ok(deleted)
}

/// Download the replicated checkpoint `name` into `directory`. Returns false
/// when no complete replica exists.
pub(crate) async fn download(operator: &Operator, name: &str, directory: &Path) -> Result<bool> {
    ensure!(valid_name(name), "invalid index checkpoint name");
    let Some(bytes) = store::read_optional(operator, &manifest_key(name)).await? else {
        return Ok(false);
    };
    let manifest: Manifest = serde_json::from_slice(&bytes)?;
    ensure!(
        manifest.version == 1,
        "unsupported index checkpoint version"
    );
    let staging: PathBuf = directory.with_extension("download");
    if staging.exists() {
        fs::remove_dir_all(&staging)?;
    }
    fs::create_dir_all(&staging)?;
    let started = Instant::now();
    futures::stream::iter(manifest.files.clone())
        .map(|file| {
            let operator = operator.clone();
            let staging = staging.clone();
            async move {
                ensure!(valid_name(&file.path), "invalid index checkpoint file name");
                let mut stream = operator
                    .reader(&file.object)
                    .await
                    .with_context(|| format!("download {}", file.object))?
                    .into_bytes_stream(..)
                    .await
                    .with_context(|| format!("download {}", file.object))?;
                let mut output = tokio::fs::File::create(staging.join(&file.path)).await?;
                let mut hasher = blake3::Hasher::new();
                let mut size = 0u64;
                while let Some(chunk) = stream
                    .try_next()
                    .await
                    .with_context(|| format!("download {}", file.object))?
                {
                    size += chunk.len() as u64;
                    ensure!(
                        size <= file.size,
                        "index checkpoint file {} is damaged",
                        file.object
                    );
                    hasher.update(&chunk);
                    output.write_all(&chunk).await?;
                }
                ensure!(
                    size == file.size && hasher.finalize().to_hex().as_str() == file.blake3,
                    "index checkpoint file {} is damaged",
                    file.object
                );
                output.sync_all().await?;
                Ok::<_, anyhow::Error>(())
            }
        })
        .buffer_unordered(CONCURRENCY)
        .try_collect::<Vec<_>>()
        .await?;
    fs::File::open(&staging)?.sync_all()?;
    if let Some(parent) = directory.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::rename(&staging, directory)?;
    if let Some(parent) = directory.parent() {
        fs::File::open(parent)?.sync_all()?;
    }
    tracing::info!(
        event = "index_checkpoint_downloaded",
        name,
        files = manifest.files.len(),
        bytes = manifest.files.iter().map(|file| file.size).sum::<u64>(),
        elapsed_ms = started.elapsed().as_millis() as u64,
        "downloaded a replicated row-index checkpoint"
    );
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hashes_survive_a_restart_and_a_damaged_file_only_means_hashing() {
        let checkpoints = tempfile::tempdir().unwrap();
        let sst = checkpoints.path().join("000001.sst");
        fs::write(&sst, b"rows").unwrap();
        let key = FileKey::of(&fs::metadata(&sst).unwrap()).unwrap();
        let hashes = Hashes::default();
        assert!(hashes.known(checkpoints.path()).is_empty());
        hashes.remember(
            checkpoints.path(),
            HashMap::from([(key, "hash".to_owned())]),
        );
        // A new process loads what the last upload saved.
        let restarted = Hashes::default();
        assert_eq!(restarted.known(checkpoints.path())[&key], "hash");
        fs::write(checkpoints.path().join(HASHES_FILE), b"{torn").unwrap();
        assert!(Hashes::default().known(checkpoints.path()).is_empty());
    }
}
