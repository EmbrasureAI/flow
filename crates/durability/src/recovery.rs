//! Startup: claim the next owner epoch, fence the previous owner, decide
//! whether local files or object storage are authoritative, and restore.
//!
//! The previous owner may still be running after a crash or partition. Its
//! barriers and writes need a lease no older than `lease`, renewed only while
//! no successor claim exists. A successor therefore waits one lease after its
//! claim before reading the log tail, unless that owner wrote a clean release
//! or provably exited. Any batch the old owner writes afterwards is not part of
//! the recovered history and cannot have been the basis of an external effect.
use crate::checkpoint::History;
use crate::format::{
    self, Batch, Checkpoint, IDENTITY_KEY, Identity, LockIdentity, Owner, Record, SegmentImage,
    log_key, owner_key,
};
use crate::store::{self, Created};
use anyhow::{Context, Result, bail, ensure};
use bytes::Bytes;
use flow_state_store::{ControlRestore, ControlStore};
use futures::{StreamExt, TryStreamExt};
use opendal::Operator;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

/// Concurrent object reads while recovering.
const READ_CONCURRENCY: usize = 32;
pub(crate) const MARKER: &str = "remote-durability.json";

/// How this process obtained its local state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Mode {
    /// Neither local nor replicated state existed.
    Fresh,
    /// Existing local state is being replicated for the first time.
    Migrated,
    /// Local state belonged to the most recent owner and was kept.
    Local,
    /// Local state was rebuilt from object storage.
    Restored,
}

/// Local files, relative to the state directory, that recovery owns.
#[derive(Debug, Clone)]
pub struct Layout {
    pub control: String,
    pub journals: Vec<String>,
    /// Small authority files replicated whole.
    pub files: Vec<String>,
    /// Derived state discarded with stale authority.
    pub derived: Vec<String>,
}

impl Layout {
    fn local_state_exists(&self, state_dir: &Path) -> bool {
        state_dir.join(&self.control).join("CURRENT").exists()
            || self.journals.iter().any(|journal| {
                fs::read_dir(state_dir.join(journal)).is_ok_and(|mut entries| {
                    entries.any(|entry| {
                        entry.is_ok_and(|entry| {
                            entry
                                .file_name()
                                .to_str()
                                .is_some_and(|name| name.ends_with(".segment"))
                        })
                    })
                })
            })
            || self.files.iter().any(|file| state_dir.join(file).exists())
    }
    fn entries(&self) -> impl Iterator<Item = &str> {
        std::iter::once(self.control.as_str())
            .chain(self.journals.iter().map(String::as_str))
            .chain(self.files.iter().map(String::as_str))
            .chain(self.derived.iter().map(String::as_str))
            .chain(std::iter::once(MARKER))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Marker {
    uri: String,
    source_id: String,
    epoch: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RestoreStats {
    pub checkpoint: Option<String>,
    pub control_entries: u64,
    pub control_batches: u64,
    pub journal_bytes: u64,
    pub log_batches: u64,
    pub elapsed_ms: u64,
}

pub(crate) struct Recovered {
    pub epoch: u64,
    pub mode: Mode,
    pub next_seq: u64,
    pub next_ordinal: u64,
    pub history: History,
    pub restore: Option<RestoreStats>,
    pub waited_for_previous_owner: bool,
    pub replaced_local: Option<PathBuf>,
}

fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn sync_dir(path: &Path) -> Result<()> {
    File::open(path)?.sync_all()?;
    Ok(())
}

/// Replace `path` atomically and durably: write a temporary file, sync it,
/// rename it into place and sync the directory.
pub fn write_durable(path: &Path, data: &[u8]) -> Result<()> {
    let temporary = path.with_extension("durability-tmp");
    let mut file = File::create(&temporary)?;
    file.write_all(data)?;
    file.sync_all()?;
    fs::rename(&temporary, path)?;
    sync_dir(path.parent().context("file has a parent directory")?)
}

async fn ensure_identity(operator: &Operator, source_id: &str) -> Result<()> {
    let expected = Identity {
        format: format::FORMAT.into(),
        version: format::VERSION,
        source_id: source_id.into(),
    };
    let existing = match store::read_optional(operator, IDENTITY_KEY).await? {
        Some(bytes) => bytes,
        None => match store::create(
            operator,
            IDENTITY_KEY,
            Bytes::from(serde_json::to_vec(&expected)?),
        )
        .await?
        {
            Created::New | Created::Same => return Ok(()),
            Created::Different(bytes) => bytes,
        },
    };
    let identity: Identity =
        serde_json::from_slice(&existing).context("invalid durability identity")?;
    ensure!(
        identity.format == format::FORMAT,
        "durability prefix holds another format: {}",
        identity.format
    );
    ensure!(
        identity.version == format::VERSION,
        "durability prefix has format version {}; use a compatible binary",
        identity.version
    );
    ensure!(
        identity.source_id == source_id,
        "durability prefix belongs to source {}, not {source_id}; use a new prefix for a new source incarnation",
        identity.source_id
    );
    Ok(())
}

/// Identify the held lock file, where the platform allows it.
pub(crate) fn lock_identity(lock: &File) -> Option<LockIdentity> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let boot_id = fs::read_to_string("/proc/sys/kernel/random/boot_id").ok()?;
        let metadata = lock.metadata().ok()?;
        Some(LockIdentity {
            boot_id: boot_id.trim().to_owned(),
            device: metadata.dev(),
            inode: metadata.ino(),
        })
    }
    #[cfg(not(unix))]
    {
        let _ = lock;
        None
    }
}

/// Whether the previous claimant provably stopped: it held the lock this
/// process now holds, under the same running kernel. The same identity on a
/// recreated file proves it too, since an inode is reused only after every
/// descriptor closed, and an owner closes its lock only after replication
/// stopped.
async fn predecessor_exited(
    operator: &Operator,
    epoch: u64,
    lock: Option<&LockIdentity>,
) -> Result<bool> {
    let Some(lock) = lock else {
        return Ok(false);
    };
    let Some(bytes) = store::read_optional(operator, &owner_key(epoch - 1)).await? else {
        return Ok(false);
    };
    let previous: Owner = serde_json::from_slice(&bytes).context("invalid owner claim")?;
    Ok(previous.lock.as_ref() == Some(lock))
}

async fn claim(operator: &Operator, lock: Option<LockIdentity>) -> Result<u64> {
    let owners = store::list_numbered(operator, format::OWNERS, None).await?;
    let epoch = owners.keys().max().map_or(1, |latest| latest + 1);
    let owner = Owner {
        epoch,
        host: std::env::var("HOSTNAME").unwrap_or_default(),
        pid: std::process::id(),
        started_unix_ms: unix_ms(),
        lock,
    };
    match store::create(
        operator,
        &owner_key(epoch),
        Bytes::from(serde_json::to_vec(&owner)?),
    )
    .await?
    {
        Created::New | Created::Same => Ok(epoch),
        Created::Different(_) => {
            bail!("another Flow process claimed this durable state concurrently; retry later")
        }
    }
}

/// The newest checkpoint whose objects all exist, and its predecessor.
async fn checkpoints(operator: &Operator) -> Result<History> {
    let mut keys = store::list_recursive(operator, format::CHECKPOINTS).await?;
    keys.sort();
    let mut history = History::default();
    for key in keys.into_iter().rev() {
        let checkpoint: Checkpoint = match store::read_optional(operator, &key).await? {
            Some(bytes) => serde_json::from_slice(&bytes)
                .with_context(|| format!("invalid durability checkpoint {key}"))?,
            None => continue,
        };
        ensure!(
            checkpoint.version == format::VERSION,
            "checkpoint {key} has unsupported version {}",
            checkpoint.version
        );
        let missing = futures::stream::iter(checkpoint.objects())
            .map(|object| async move {
                match operator.stat(object).await {
                    Ok(_) => Ok(None),
                    Err(error) if error.kind() == opendal::ErrorKind::NotFound => {
                        Ok(Some(object.to_owned()))
                    }
                    Err(error) => Err(error).with_context(|| format!("stat {object}")),
                }
            })
            .buffer_unordered(READ_CONCURRENCY)
            .try_filter_map(|missing| async move { Ok(missing) })
            .try_collect::<Vec<_>>()
            .await?;
        if let Some(object) = missing.first() {
            tracing::warn!(checkpoint = %key, object, "durability checkpoint is incomplete; using an older one");
            continue;
        }
        if history.latest.is_none() {
            history.latest = Some((key, checkpoint));
        } else {
            history.previous = Some((key, checkpoint));
            break;
        }
    }
    Ok(history)
}

/// The log after a checkpoint, and the owner state it implies.
///
/// Each owner's history is the run of its batches with contiguous sequence
/// numbers and ordinals after its epoch start. An owner treats a batch as
/// durable only once every earlier batch is, so anything it wrote after a
/// missing sequence or an ordinal discontinuity was never durable for it and
/// is ignored, as are batches of an older epoch after a newer one began.
struct Scan {
    epoch: u64,
    last_ordinal: u64,
    previous_batch: Option<u64>,
    /// The current epoch's history has ended; only a newer epoch continues.
    sealed: bool,
    released: Option<u64>,
    /// Next sequence number to read.
    next: u64,
    /// A sequence number was missing.
    gaps: bool,
    batches: u64,
}

impl Scan {
    fn new(checkpoint: Option<&Checkpoint>) -> Self {
        Self {
            epoch: checkpoint.map_or(0, |checkpoint| checkpoint.epoch),
            last_ordinal: checkpoint.map_or(0, |checkpoint| checkpoint.ordinal),
            previous_batch: None,
            sealed: false,
            released: None,
            next: checkpoint.map_or(0, |checkpoint| checkpoint.replay_from),
            gaps: false,
            batches: 0,
        }
    }

    /// Whether the batch belongs to the recovered history.
    fn accept(&mut self, batch: &Batch) -> Result<bool> {
        if batch.epoch < self.epoch || (batch.epoch == self.epoch && self.sealed) {
            tracing::warn!(
                seq = batch.seq,
                epoch = batch.epoch,
                "ignoring a log batch that was never durable for its owner"
            );
            return Ok(false);
        }
        if batch.epoch > self.epoch {
            ensure!(
                matches!(batch.records.first(), Some(Record::EpochStart { epoch }) if *epoch == batch.epoch),
                "log batch {} starts epoch {} without an epoch record",
                batch.seq,
                batch.epoch
            );
            ensure!(
                self.previous_batch
                    .is_none_or(|_| batch.first_ordinal == self.last_ordinal + 1)
                    && batch.first_ordinal <= self.last_ordinal + 1,
                "epoch {} does not continue the recovered ordinals",
                batch.epoch
            );
            self.sealed = false;
        } else if let Some(previous) = self.previous_batch {
            if batch.first_ordinal != previous + 1 {
                self.sealed = true;
                tracing::warn!(
                    seq = batch.seq,
                    epoch = batch.epoch,
                    "log batch skips ordinals; ending this owner's history"
                );
                return Ok(false);
            }
        } else {
            ensure!(
                batch.first_ordinal <= self.last_ordinal + 1,
                "log batch {} skips ordinals after the checkpoint",
                batch.seq
            );
        }
        for record in &batch.records {
            self.released = match record {
                Record::EpochStart { epoch } => {
                    self.epoch = *epoch;
                    None
                }
                Record::Release { epoch } => Some(*epoch),
                _ => None,
            };
        }
        self.previous_batch = Some(batch.last_ordinal());
        self.last_ordinal = self.last_ordinal.max(batch.last_ordinal());
        self.batches += 1;
        Ok(true)
    }
}

fn feed(restorer: &mut Option<Restorer>) -> impl FnMut(&Batch) -> Result<()> + '_ {
    move |batch| match restorer.as_mut() {
        Some(restorer) => restorer.apply(batch),
        None => Ok(()),
    }
}

/// Read every listed batch from `scan.next` and feed accepted ones to `apply`.
async fn extend(
    operator: &Operator,
    scan: &mut Scan,
    mut apply: impl FnMut(&Batch) -> Result<()>,
) -> Result<()> {
    let listed = store::list_numbered(operator, format::LOG, scan.next.checked_sub(1)).await?;
    let mut batches = futures::stream::iter(listed)
        .map(|(seq, key)| async move {
            let batch = format::decode_batch(&store::read(operator, &key).await?)
                .with_context(|| format!("decode {key}"))?;
            ensure!(
                batch.seq == seq,
                "log batch {key} names sequence {}",
                batch.seq
            );
            Ok::<_, anyhow::Error>(batch)
        })
        .buffered(READ_CONCURRENCY);
    while let Some(batch) = batches.try_next().await? {
        if batch.seq != scan.next {
            // A missing batch ends its owner's history; see `Scan`.
            scan.sealed = true;
            scan.gaps = true;
        }
        scan.next = batch.seq + 1;
        if scan.accept(&batch)? {
            apply(&batch)?;
        }
    }
    Ok(())
}

/// Write one journal segment from its checkpoint parts, streaming each part
/// to disk so restore memory does not grow with part size.
async fn restore_segment(
    operator: &Operator,
    path: PathBuf,
    segment: u64,
    image: &SegmentImage,
) -> Result<u64> {
    let mut file = File::create(&path)?;
    let mut expected = 0;
    for part in &image.parts {
        let mut stream = operator
            .reader(&part.object)
            .await
            .with_context(|| format!("read {}", part.object))?
            .into_bytes_stream(..)
            .await
            .with_context(|| format!("read {}", part.object))?;
        let mut crc32 = crc32fast::Hasher::new();
        let mut length = 0u64;
        while let Some(chunk) = stream
            .try_next()
            .await
            .with_context(|| format!("read {}", part.object))?
        {
            crc32.update(&chunk);
            file.write_all(&chunk)?;
            length += chunk.len() as u64;
        }
        ensure!(
            part.start == expected
                && length == part.end - part.start
                && crc32.finalize() == part.crc32,
            "journal part {} is damaged or out of order",
            part.object
        );
        expected = part.end;
    }
    ensure!(
        expected == image.length,
        "journal segment {segment} image is incomplete"
    );
    Ok(expected)
}

/// Rebuilds local files in a private staging directory, then installs them.
struct Restorer {
    staging: PathBuf,
    control: Option<ControlRestore>,
    control_ordinal: u64,
    ordinal: u64,
    journals: BTreeMap<String, u64>,
    files: BTreeMap<String, Vec<u8>>,
    stats: RestoreStats,
}

impl Restorer {
    async fn begin(
        operator: &Operator,
        state_dir: &Path,
        layout: &Layout,
        checkpoint: Option<&(String, Checkpoint)>,
    ) -> Result<Self> {
        let staging = state_dir.join(format!(".durability-restore-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&staging)?;
        let mut control = ControlStore::begin_restore(staging.join(&layout.control))?;
        let mut restorer = Self {
            staging,
            control: None,
            control_ordinal: 0,
            ordinal: 0,
            journals: layout
                .journals
                .iter()
                .map(|name| (name.clone(), 0))
                .collect(),
            files: BTreeMap::new(),
            stats: RestoreStats::default(),
        };
        for journal in &layout.journals {
            fs::create_dir_all(restorer.staging.join(journal))?;
        }
        if let Some((key, checkpoint)) = checkpoint {
            restorer.stats.checkpoint = Some(key.clone());
            restorer.control_ordinal = checkpoint.control_ordinal;
            restorer.ordinal = checkpoint.ordinal;
            if let Some(image) = &checkpoint.control {
                let entries =
                    format::decode_snapshot(&store::read(operator, &image.object).await?)?;
                for (key, value) in &entries {
                    control.put(key, value)?;
                }
                restorer.stats.control_entries = entries.len() as u64;
            }
            for (name, image) in &checkpoint.journals {
                ensure!(
                    restorer.journals.contains_key(name),
                    "checkpoint names unknown journal {name}"
                );
                restorer.journals.insert(name.clone(), image.reclaimed);
            }
            let staging = &restorer.staging;
            let segments = checkpoint.journals.iter().flat_map(|(name, image)| {
                image.segments.iter().map(move |(segment, image)| {
                    let path = staging
                        .join(name)
                        .join(flow_ingress_journal::segment_file_name(*segment));
                    restore_segment(operator, path, *segment, image)
                })
            });
            let restored: Vec<u64> = futures::stream::iter(segments)
                .buffer_unordered(READ_CONCURRENCY)
                .try_collect()
                .await?;
            restorer.stats.journal_bytes += restored.iter().sum::<u64>();
            restorer.files = checkpoint.files.clone();
        }
        restorer.control = Some(control);
        Ok(restorer)
    }

    fn apply(&mut self, batch: &Batch) -> Result<()> {
        self.stats.log_batches += 1;
        for (index, record) in batch.records.iter().enumerate() {
            let ordinal = batch.first_ordinal + index as u64;
            let covered = match record {
                Record::Control(_) => self.control_ordinal,
                _ => self.ordinal,
            };
            if ordinal <= covered {
                continue;
            }
            match record {
                Record::Control(data) => {
                    self.control
                        .as_mut()
                        .expect("restore is open")
                        .apply(data)?;
                    self.stats.control_batches += 1;
                }
                Record::Extent {
                    journal,
                    segment,
                    offset,
                    data,
                } => {
                    let path = self.segment(journal, *segment)?;
                    let mut file = OpenOptions::new()
                        .create(true)
                        .truncate(false)
                        .write(true)
                        .open(&path)?;
                    ensure!(
                        file.metadata()?.len() >= *offset,
                        "journal {journal} segment {segment} is missing bytes before offset {offset}"
                    );
                    file.seek(SeekFrom::Start(*offset))?;
                    file.write_all(data)?;
                    file.set_len(offset + data.len() as u64)?;
                    self.stats.journal_bytes += data.len() as u64;
                }
                Record::Truncate {
                    journal,
                    segment,
                    length,
                } => {
                    let path = self.segment(journal, *segment)?;
                    OpenOptions::new()
                        .create(true)
                        .truncate(false)
                        .write(true)
                        .open(&path)?
                        .set_len(*length)?;
                }
                Record::Reclaim {
                    journal,
                    reclaimed,
                    first_retained,
                } => {
                    let current = self
                        .journals
                        .get_mut(journal)
                        .with_context(|| format!("log names unknown journal {journal}"))?;
                    *current = (*current).max(*reclaimed);
                    for entry in fs::read_dir(self.staging.join(journal))? {
                        let entry = entry?;
                        if let Some(id) = entry
                            .file_name()
                            .to_str()
                            .and_then(|name| name.strip_suffix(".segment"))
                            .and_then(|id| id.parse::<u64>().ok())
                            && id < *first_retained
                        {
                            fs::remove_file(entry.path())?;
                        }
                    }
                }
                Record::File { name, data } => {
                    self.files.insert(name.clone(), data.clone());
                }
                Record::EpochStart { .. } | Record::Release { .. } => {}
            }
        }
        Ok(())
    }

    fn segment(&self, journal: &str, segment: u64) -> Result<PathBuf> {
        ensure!(
            self.journals.contains_key(journal),
            "log names unknown journal {journal}"
        );
        Ok(self
            .staging
            .join(journal)
            .join(flow_ingress_journal::segment_file_name(segment)))
    }

    /// Make every restored file durable, then move it into the state directory.
    fn install(mut self, state_dir: &Path, layout: &Layout) -> Result<RestoreStats> {
        self.control.take().expect("restore is open").finish()?;
        for (journal, reclaimed) in &self.journals {
            let directory = self.staging.join(journal);
            if *reclaimed > 0 {
                flow_ingress_journal::write_reclaimed(&directory, flow_model::PgLsn(*reclaimed))?;
            }
            for entry in fs::read_dir(&directory)? {
                File::open(entry?.path())?.sync_all()?;
            }
            sync_dir(&directory)?;
        }
        for (name, data) in &self.files {
            ensure!(
                layout.files.contains(name),
                "log names unexpected authority file {name}"
            );
            let mut file = File::create(self.staging.join(name))?;
            file.write_all(data)?;
            file.sync_all()?;
        }
        sync_dir(&self.staging)?;
        let installed = std::iter::once(layout.control.as_str())
            .chain(layout.journals.iter().map(String::as_str))
            .chain(self.files.keys().map(String::as_str));
        for name in installed {
            let target = state_dir.join(name);
            ensure!(
                !target.exists(),
                "{} already exists; refusing to overwrite local state while restoring",
                target.display()
            );
            fs::rename(self.staging.join(name), target)?;
        }
        sync_dir(state_dir)?;
        fs::remove_dir_all(&self.staging)?;
        sync_dir(state_dir)?;
        Ok(std::mem::take(&mut self.stats))
    }
}

impl Drop for Restorer {
    fn drop(&mut self) {
        if self.staging.exists() {
            drop(self.control.take());
            let _ = fs::remove_dir_all(&self.staging);
        }
    }
}

/// Move superseded local state aside for diagnosis instead of deleting it.
fn replace_local(state_dir: &Path, layout: &Layout, epoch: u64) -> Result<Option<PathBuf>> {
    let target = state_dir.join(format!("replaced-{epoch}-{}", unix_ms()));
    let mut moved = false;
    for name in layout.entries() {
        let source = state_dir.join(name);
        if source.exists() {
            if !moved {
                fs::create_dir(&target)?;
                moved = true;
            }
            fs::rename(&source, target.join(name))?;
        }
    }
    if moved {
        sync_dir(&target)?;
        sync_dir(state_dir)?;
    }
    Ok(moved.then_some(target))
}

pub(crate) async fn recover(
    operator: &Operator,
    uri: &str,
    source_id: &str,
    state_dir: &Path,
    layout: &Layout,
    lease: Duration,
    lock: Option<LockIdentity>,
) -> Result<Recovered> {
    fs::create_dir_all(state_dir)?;
    for entry in fs::read_dir(state_dir)? {
        let entry = entry?;
        if entry
            .file_name()
            .to_str()
            .is_some_and(|name| name.starts_with(".durability-restore-"))
        {
            fs::remove_dir_all(entry.path())?;
        }
    }
    ensure_identity(operator, source_id).await?;
    let epoch = claim(operator, lock.clone()).await?;
    let history = checkpoints(operator).await?;
    let checkpoint = history.latest.as_ref().map(|(_, checkpoint)| checkpoint);
    let local = layout.local_state_exists(state_dir);
    let marker: Option<Marker> = match fs::read(state_dir.join(MARKER)) {
        Ok(bytes) => Some(serde_json::from_slice(&bytes).context("invalid durability marker")?),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => return Err(error.into()),
    };
    if let Some(marker) = &marker {
        ensure!(
            marker.uri == uri && marker.source_id == source_id,
            "state directory is replicated to {} for source {}; refusing to use {uri}. Remove {} to migrate deliberately",
            marker.uri,
            marker.source_id,
            state_dir.join(MARKER).display()
        );
    }
    let mut scan = Scan::new(checkpoint);
    // Without local state the outcome is a restore; apply while reading.
    let mut restorer = if local {
        None
    } else {
        Some(Restorer::begin(operator, state_dir, layout, history.latest.as_ref()).await?)
    };
    extend(operator, &mut scan, feed(&mut restorer)).await?;
    // The lock proves only that the previous claimant exited. Older owners are
    // fenced only if that claimant finished recovering, which its epoch start
    // in the log shows.
    let exited = epoch == 1
        || scan.released == Some(epoch - 1)
        || (scan.epoch == epoch - 1 && predecessor_exited(operator, epoch, lock.as_ref()).await?);
    let mut waited = false;
    if !exited {
        // The previous owner may still act until its lease expires.
        tracing::info!(
            epoch,
            lease_ms = lease.as_millis() as u64,
            "waiting for the previous owner's lease before reading its final records"
        );
        tokio::time::sleep(lease + Duration::from_secs(1)).await;
        waited = true;
        if scan.gaps {
            // A batch missing earlier may have landed while that owner's
            // lease was valid, extending its history; read it all again.
            scan = Scan::new(checkpoint);
            if restorer.take().is_some() {
                restorer = Some(
                    Restorer::begin(operator, state_dir, layout, history.latest.as_ref()).await?,
                );
            }
        }
        extend(operator, &mut scan, feed(&mut restorer)).await?;
    }
    let remote = checkpoint.is_some() || scan.batches > 0;
    let mode = match (&marker, local, remote) {
        (Some(marker), true, true) if marker.epoch == scan.epoch => Mode::Local,
        (Some(marker), true, true) if marker.epoch > scan.epoch => bail!(
            "replicated history ends at owner {} but this state directory was replicated by owner {}; object storage lost acknowledged records",
            scan.epoch,
            marker.epoch
        ),
        (Some(_), true, false) => {
            tracing::warn!("replicated history is empty; replicating local state again");
            Mode::Migrated
        }
        (None, true, false) => Mode::Migrated,
        (_, true, true) => Mode::Restored,
        (_, false, true) => Mode::Restored,
        (_, false, false) => Mode::Fresh,
    };
    let mut replaced_local = None;
    if mode == Mode::Restored {
        replaced_local = replace_local(state_dir, layout, epoch)?;
        if replaced_local.is_some() {
            tracing::warn!(
                moved_to = ?replaced_local,
                "local state was superseded by a newer owner; restoring from object storage"
            );
        }
    }
    if mode == Mode::Restored && local {
        let mut fresh = Scan::new(checkpoint);
        restorer =
            Some(Restorer::begin(operator, state_dir, layout, history.latest.as_ref()).await?);
        extend(operator, &mut fresh, feed(&mut restorer)).await?;
        ensure!(
            (fresh.next, fresh.epoch, fresh.last_ordinal)
                == (scan.next, scan.epoch, scan.last_ordinal),
            "the log changed while restoring; restart to recover again"
        );
    }
    let started = Instant::now();
    let restore = match (mode, restorer) {
        (Mode::Restored, Some(restorer)) => {
            let mut stats = restorer.install(state_dir, layout)?;
            stats.elapsed_ms = started.elapsed().as_millis() as u64;
            Some(stats)
        }
        _ => None,
    };
    // Start after every listed batch, including ignored ones, so no write of
    // this owner can collide with a late write of an earlier one.
    let start = Batch {
        epoch,
        seq: scan.next,
        first_ordinal: scan.last_ordinal + 1,
        records: vec![Record::EpochStart { epoch }],
    };
    match store::create(
        operator,
        &log_key(scan.next),
        Bytes::from(format::encode_batch(&start)?),
    )
    .await?
    {
        Created::New | Created::Same => {}
        Created::Different(_) => {
            bail!(
                "another owner extended the durable log during recovery; restart to recover again"
            )
        }
    }
    write_durable(
        &state_dir.join(MARKER),
        &serde_json::to_vec(&Marker {
            uri: uri.into(),
            source_id: source_id.into(),
            epoch,
        })?,
    )?;
    Ok(Recovered {
        epoch,
        mode,
        next_seq: scan.next + 1,
        next_ordinal: scan.last_ordinal + 2,
        history,
        restore,
        waited_for_previous_owner: waited,
        replaced_local,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn layout() -> Layout {
        Layout {
            control: "control".into(),
            journals: vec!["journal".into()],
            files: vec!["source-identity.json".into()],
            derived: vec![
                "index".into(),
                "index-generations".into(),
                "checkpoints".into(),
            ],
        }
    }

    fn batch(epoch: u64, seq: u64, first_ordinal: u64, records: Vec<Record>) -> Batch {
        Batch {
            epoch,
            seq,
            first_ordinal,
            records,
        }
    }

    fn control(byte: u8) -> Record {
        Record::Control(vec![byte])
    }

    async fn replay(batches: &[Batch]) -> (Scan, Vec<u64>) {
        let operator = Operator::new(opendal::services::Memory::default())
            .unwrap()
            .finish();
        for batch in batches {
            operator
                .write(&log_key(batch.seq), format::encode_batch(batch).unwrap())
                .await
                .unwrap();
        }
        let mut scan = Scan::new(None);
        let mut applied = Vec::new();
        extend(&operator, &mut scan, |batch| {
            applied.push(batch.seq);
            Ok(())
        })
        .await
        .unwrap();
        (scan, applied)
    }

    #[tokio::test]
    async fn a_missing_batch_ends_its_owners_history_but_not_the_next_owners() {
        let (scan, applied) = replay(&[
            batch(1, 0, 1, vec![Record::EpochStart { epoch: 1 }]),
            batch(1, 1, 2, vec![control(1)]),
            // Sequence 2 never landed, so 3 was never durable for owner 1.
            batch(1, 3, 4, vec![control(3)]),
            batch(2, 4, 3, vec![Record::EpochStart { epoch: 2 }]),
            batch(2, 5, 4, vec![control(4)]),
            // A late write of the replaced owner.
            batch(1, 6, 5, vec![control(5)]),
        ])
        .await;
        assert_eq!(applied, [0, 1, 4, 5]);
        assert_eq!((scan.epoch, scan.last_ordinal, scan.next), (2, 4, 7));
        assert!(scan.gaps);
        assert_eq!(scan.released, None);
    }

    #[tokio::test]
    async fn an_ordinal_discontinuity_ends_its_owners_history() {
        let (scan, applied) = replay(&[
            batch(1, 0, 1, vec![Record::EpochStart { epoch: 1 }]),
            batch(1, 1, 3, vec![control(3)]),
            batch(1, 2, 4, vec![control(4)]),
            batch(2, 3, 2, vec![Record::EpochStart { epoch: 2 }]),
            batch(2, 4, 3, vec![Record::Release { epoch: 2 }]),
        ])
        .await;
        assert_eq!(applied, [0, 3, 4]);
        assert_eq!((scan.epoch, scan.last_ordinal, scan.next), (2, 3, 5));
        assert!(!scan.gaps);
        assert_eq!(scan.released, Some(2));
    }

    #[tokio::test]
    async fn a_batch_landing_during_the_lease_wait_rejoins_the_history() {
        let operator = Operator::new(opendal::services::Memory::default())
            .unwrap()
            .finish();
        let file = |data: &[u8]| Record::File {
            name: "source-identity.json".into(),
            data: data.to_vec(),
        };
        let owner = Owner {
            epoch: 1,
            host: String::new(),
            pid: 0,
            started_unix_ms: 0,
            lock: None,
        };
        operator
            .write(&owner_key(1), serde_json::to_vec(&owner).unwrap())
            .await
            .unwrap();
        for batch in [
            batch(1, 0, 1, vec![Record::EpochStart { epoch: 1 }]),
            batch(1, 2, 3, vec![file(b"second")]),
        ] {
            operator
                .write(&log_key(batch.seq), format::encode_batch(&batch).unwrap())
                .await
                .unwrap();
        }
        // Owner 1's write of sequence 1 lands while its lease is still valid,
        // which makes sequence 2 durable for it too.
        let late = operator.clone();
        let landing = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(300)).await;
            late.write(
                &log_key(1),
                format::encode_batch(&batch(1, 1, 2, vec![file(b"first")])).unwrap(),
            )
            .await
            .unwrap();
        });
        let state = tempfile::tempdir().unwrap();
        let recovered = recover(
            &operator,
            "memory://test",
            "source",
            state.path(),
            &layout(),
            Duration::from_secs(1),
            None,
        )
        .await
        .unwrap();
        landing.await.unwrap();
        assert!(recovered.waited_for_previous_owner);
        assert_eq!(recovered.mode, Mode::Restored);
        assert_eq!(recovered.restore.unwrap().log_batches, 3);
        assert_eq!(
            fs::read(state.path().join("source-identity.json")).unwrap(),
            b"second"
        );
        assert_eq!((recovered.next_seq, recovered.next_ordinal), (4, 5));
    }
}
