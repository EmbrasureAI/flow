//! Object storage as Flow's durability authority.
//!
//! Local journal segments and the control store remain the working copy.
//! Every locally durable change is appended to an ordered, write-behind log of
//! immutable objects; periodic checkpoints bound recovery and retention. A
//! worker and its entire local disk can be lost: the next owner restores the
//! latest checkpoint plus the log tail.
//!
//! The contract with callers is the barrier. Appends never wait for object
//! storage, so capture and publication are not serialized on remote latency.
//! Anything visible outside Flow that depends on local state (a catalog
//! commit, a source acknowledgement, an object deletion) must first call
//! [`Durability::barrier`], which returns once every earlier change is
//! durable and this process still owns the state. A replaced process fails
//! its barriers and stops.
mod checkpoint;
mod format;
mod index;
mod recovery;
mod store;
mod writer;

pub use recovery::{Layout, Mode, RestoreStats, write_durable};
pub use writer::Stats;

use anyhow::{Context, Result, bail, ensure};
use flow_ingress_journal::{Journal, Mirror};
use flow_model::PgLsn;
use flow_state_store::{ControlMirror, ControlStore};
use opendal::Operator;
use serde::Deserialize;
use std::{
    collections::{BTreeMap, btree_map::Entry},
    fs,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::sync::{mpsc, oneshot};
use writer::{Item, JournalModel, MAX_EXTENT_BYTES, SegmentModel, Shared, Tuning};

/// `[durability]` configuration. Absent means local disk is the authority.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// `s3://bucket/prefix`, `file:///absolute/path` or `memory://` (tests).
    /// Use a new prefix for every source incarnation.
    pub uri: String,
    /// Object storage region; defaults to `AWS_REGION`.
    #[serde(default)]
    pub region: Option<String>,
    /// S3-compatible endpoint, such as a MinIO URL. Defaults to AWS.
    #[serde(default)]
    pub endpoint: Option<String>,
    /// Largest log batch.
    #[serde(default = "defaults::max_batch_bytes")]
    pub max_batch_bytes: u64,
    /// Log batches written concurrently.
    #[serde(default = "defaults::max_inflight_writes")]
    pub max_inflight_writes: usize,
    /// Average log writes per second; excess records form larger batches.
    /// Zero removes the limit.
    #[serde(default = "defaults::max_writes_per_second")]
    pub max_writes_per_second: f64,
    /// Memory for queued records before appends block. Journal extents count
    /// only their descriptors; their bytes are read when batched.
    #[serde(default = "defaults::queue_memory_bytes")]
    pub queue_memory_bytes: u64,
    #[serde(default = "defaults::checkpoint_interval_secs")]
    pub checkpoint_interval_secs: u64,
    /// Also checkpoint after this many log batches.
    #[serde(default = "defaults::checkpoint_batches")]
    pub checkpoint_batches: u64,
    /// A successor waits this long after an unclean stop. Barriers require a
    /// lease renewed within it.
    #[serde(default = "defaults::lease_secs")]
    pub lease_secs: u64,
    #[serde(default = "defaults::request_timeout_secs")]
    pub request_timeout_secs: u64,
    /// Barriers wait while object storage is unavailable, logging a warning
    /// at this interval. They fail only once replication has stopped.
    #[serde(default = "defaults::stall_warning_secs")]
    pub stall_warning_secs: u64,
}

mod defaults {
    pub(super) fn max_batch_bytes() -> u64 {
        8 << 20
    }
    pub(super) fn max_inflight_writes() -> usize {
        4
    }
    pub(super) fn max_writes_per_second() -> f64 {
        20.0
    }
    pub(super) fn queue_memory_bytes() -> u64 {
        64 << 20
    }
    pub(super) fn checkpoint_interval_secs() -> u64 {
        30
    }
    pub(super) fn checkpoint_batches() -> u64 {
        1000
    }
    pub(super) fn lease_secs() -> u64 {
        10
    }
    pub(super) fn request_timeout_secs() -> u64 {
        4
    }
    pub(super) fn stall_warning_secs() -> u64 {
        30
    }
}

impl Config {
    pub fn new(uri: impl Into<String>) -> Self {
        Self {
            uri: uri.into(),
            region: None,
            endpoint: None,
            max_batch_bytes: defaults::max_batch_bytes(),
            max_inflight_writes: defaults::max_inflight_writes(),
            max_writes_per_second: defaults::max_writes_per_second(),
            queue_memory_bytes: defaults::queue_memory_bytes(),
            checkpoint_interval_secs: defaults::checkpoint_interval_secs(),
            checkpoint_batches: defaults::checkpoint_batches(),
            lease_secs: defaults::lease_secs(),
            request_timeout_secs: defaults::request_timeout_secs(),
            stall_warning_secs: defaults::stall_warning_secs(),
        }
    }

    pub fn validate(&self) -> Result<()> {
        ensure!(
            (MAX_EXTENT_BYTES * 2..=64 << 20).contains(&self.max_batch_bytes),
            "durability.max_batch_bytes must be between 8 MiB and 64 MiB"
        );
        ensure!(
            (1..=64).contains(&self.max_inflight_writes),
            "durability.max_inflight_writes must be between 1 and 64"
        );
        ensure!(
            self.max_writes_per_second >= 0.0 && self.max_writes_per_second.is_finite(),
            "durability.max_writes_per_second must be zero or positive"
        );
        ensure!(
            self.queue_memory_bytes >= 1 << 20,
            "durability.queue_memory_bytes must be at least 1 MiB"
        );
        ensure!(
            self.checkpoint_interval_secs >= 1 && self.checkpoint_batches >= 1,
            "durability checkpoints need a positive interval and batch count"
        );
        ensure!(
            self.request_timeout_secs >= 1 && self.lease_secs >= 2 * self.request_timeout_secs + 2,
            "durability.lease_secs must exceed twice request_timeout_secs plus two seconds"
        );
        ensure!(
            self.stall_warning_secs >= 1,
            "durability.stall_warning_secs must be positive"
        );
        Ok(())
    }

    fn tuning(&self) -> Tuning {
        Tuning {
            max_batch_bytes: self.max_batch_bytes,
            max_inflight: self.max_inflight_writes,
            writes_per_second: self.max_writes_per_second,
            queue_memory_bytes: self.queue_memory_bytes,
            lease: Duration::from_secs(self.lease_secs),
            request_timeout: Duration::from_secs(self.request_timeout_secs),
            stall_warning: Duration::from_secs(self.stall_warning_secs),
        }
    }
}

/// Observable state for status reports and metrics.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Status {
    pub uri: String,
    pub mode: Mode,
    pub epoch: u64,
    pub appended: u64,
    pub durable: u64,
    pub lease_remaining_ms: Option<u64>,
    pub failure: Option<String>,
    pub restore: Option<RestoreStats>,
    pub recovery_ms: u64,
    pub waited_for_previous_owner: bool,
    #[serde(flatten)]
    pub stats: Stats,
}

struct Inner {
    shared: Arc<Shared>,
    /// Held for the life of the process: a second local process must not
    /// claim, and thereby fence, a running owner.
    lock: Mutex<Option<fs::File>>,
    requests: mpsc::Sender<checkpoint::Request>,
    thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Sends the deadline by which replication must have stopped.
    stop: Mutex<Option<oneshot::Sender<Instant>>>,
    /// One lease after the latest release or flush began. Stopping drains
    /// only until then, so a whole shutdown takes about one lease at most.
    shutdown: Mutex<Option<Instant>>,
    uri: String,
    mode: Mode,
    restore: Option<RestoreStats>,
    recovery: Duration,
    waited: bool,
    state_dir: PathBuf,
    layout: Layout,
    /// Journal images of the checkpoint recovery started from. The baseline
    /// reuses parts whose bytes still match the local segments.
    inherited: BTreeMap<String, format::JournalImage>,
    operator: Operator,
    runtime: tokio::runtime::Handle,
    index_hashes: Arc<index::Hashes>,
}

/// What the replication runtime hands back once recovery finished.
struct Ready {
    shared: Arc<Shared>,
    mode: Mode,
    restore: Option<RestoreStats>,
    recovery: Duration,
    waited: bool,
    replaced: Option<PathBuf>,
    inherited: BTreeMap<String, format::JournalImage>,
    operator: Operator,
    runtime: tokio::runtime::Handle,
}

/// Handle to the replication process for one source's state directory.
#[derive(Clone)]
pub struct Durability(Arc<Inner>);

impl Durability {
    /// Claim ownership, restore local state if object storage is
    /// authoritative, and start replication. Returns before any local state
    /// is opened by the caller.
    pub async fn open(
        config: &Config,
        state_dir: &Path,
        source_id: &str,
        layout: Layout,
    ) -> Result<Self> {
        let operator = store::operator(
            &config.uri,
            config.region.as_deref(),
            config.endpoint.as_deref(),
            Duration::from_secs(config.request_timeout_secs),
        )?;
        Self::open_with_operator(config, operator, state_dir, source_id, layout).await
    }

    /// As `open`, with a prepared operator and layout.
    pub async fn open_with_operator(
        config: &Config,
        operator: Operator,
        state_dir: &Path,
        source_id: &str,
        layout: Layout,
    ) -> Result<Self> {
        config.validate()?;
        fs::create_dir_all(state_dir)?;
        let lock = fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(state_dir.join("remote-durability.lock"))?;
        fs2::FileExt::try_lock_exclusive(&lock).with_context(|| {
            format!(
                "{} is already used by another Flow process",
                state_dir.display()
            )
        })?;
        let identity = recovery::lock_identity(&lock);
        let (ready, readiness) = oneshot::channel();
        let (stop, stopped) = oneshot::channel::<Instant>();
        let (requests, receiver) = mpsc::channel(16);
        let tuning = config.tuning();
        let lease = tuning.lease;
        let policy = checkpoint::Policy {
            interval: Duration::from_secs(config.checkpoint_interval_secs),
            batches: config.checkpoint_batches,
        };
        let uri = config.uri.clone();
        let source_id = source_id.to_owned();
        let state_dir = state_dir.to_path_buf();
        let directory = state_dir.clone();
        let recovery_layout = layout.clone();
        let thread = std::thread::Builder::new()
            .name("flow-durability".into())
            .spawn(move || {
                let runtime = match tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(2)
                    .thread_name("flow-durability")
                    .enable_all()
                    .build()
                {
                    Ok(runtime) => runtime,
                    Err(error) => {
                        let _ = ready.send(Err(anyhow::Error::from(error)));
                        return;
                    }
                };
                runtime.block_on(async move {
                    let started = Instant::now();
                    let recovered = match recovery::recover(
                        &operator,
                        &uri,
                        &source_id,
                        &directory,
                        &recovery_layout,
                        tuning.lease,
                        identity,
                    )
                    .await
                    {
                        Ok(recovered) => recovered,
                        Err(error) => {
                            let _ = ready.send(Err(error));
                            return;
                        }
                    };
                    let shared = Arc::new(Shared::new(
                        tuning,
                        recovered.epoch,
                        recovered.next_seq,
                        recovered.next_ordinal,
                    ));
                    let inherited = recovered
                        .history
                        .latest
                        .as_ref()
                        .map(|(_, checkpoint)| checkpoint.journals.clone())
                        .unwrap_or_default();
                    let tasks = [
                        tokio::spawn(writer::write_loop(shared.clone(), operator.clone())),
                        tokio::spawn(writer::heartbeat_loop(shared.clone(), operator.clone())),
                        tokio::spawn(checkpoint::checkpoint_loop(
                            shared.clone(),
                            operator.clone(),
                            receiver,
                            policy,
                            recovered.history,
                        )),
                    ];
                    let _ = ready.send(Ok(Ready {
                        shared: shared.clone(),
                        mode: recovered.mode,
                        restore: recovered.restore,
                        recovery: started.elapsed(),
                        waited: recovered.waited_for_previous_owner,
                        replaced: recovered.replaced_local,
                        inherited,
                        operator: operator.clone(),
                        runtime: tokio::runtime::Handle::current(),
                    }));
                    let deadline = stopped.await.unwrap_or_else(|_| Instant::now() + lease);
                    shared.close();
                    // Queued records drain until the deadline, at most one
                    // lease; renewal has stopped, so no write starts later.
                    let _ = tokio::time::timeout_at(
                        tokio::time::Instant::from_std(deadline),
                        futures::future::join_all(tasks),
                    )
                    .await;
                });
                runtime.shutdown_timeout(Duration::from_secs(1));
            })?;
        let Ready {
            shared,
            mode,
            restore,
            recovery,
            waited,
            replaced,
            inherited,
            operator,
            runtime,
        } = readiness
            .await
            .context("durability runtime stopped during recovery")??;
        tracing::info!(
            event = "durability_recovered",
            mode = ?mode,
            epoch = shared.epoch,
            elapsed_ms = recovery.as_millis() as u64,
            waited_for_previous_owner = waited,
            restored = ?restore,
            replaced_local = ?replaced,
            "remote durability ready"
        );
        Ok(Self(Arc::new(Inner {
            shared,
            lock: Mutex::new(Some(lock)),
            requests,
            thread: Mutex::new(Some(thread)),
            stop: Mutex::new(Some(stop)),
            shutdown: Mutex::new(None),
            uri: config.uri.clone(),
            mode,
            restore,
            recovery,
            waited,
            state_dir,
            layout,
            inherited,
            operator,
            runtime,
            index_hashes: Arc::default(),
        })))
    }

    pub fn mode(&self) -> Mode {
        self.0.mode
    }

    pub fn epoch(&self) -> u64 {
        self.0.shared.epoch
    }

    /// Replicate every later control write, and use this store for checkpoints.
    pub async fn attach_control(&self, control: &ControlStore) -> Result<()> {
        control.attach_mirror(Arc::new(ControlAdapter(self.0.shared.clone())))?;
        self.0
            .requests
            .send(checkpoint::Request::Control(control.clone()))
            .await
            .context("durability runtime stopped")
    }

    /// Replicate a journal opened at `state_dir/name`. Differences from the
    /// replicated image, such as a truncated torn tail, are appended first.
    pub fn attach_journal(&self, name: &str, journal: &mut Journal) -> Result<()> {
        let layout = journal.layout();
        let mut items = Vec::new();
        {
            let mut state = self.0.shared.lock();
            let model = state
                .journals
                .entry(name.to_owned())
                .or_insert_with(|| JournalModel {
                    root: layout.root.clone(),
                    reclaimed: 0,
                    segments: BTreeMap::new(),
                    last_extents: BTreeMap::new(),
                });
            model.root = layout.root.clone();
            let first = layout.segments.keys().next().copied().unwrap_or(0);
            if layout.reclaimed.0 > model.reclaimed
                || model.segments.keys().next().is_some_and(|id| *id < first)
            {
                items.push(Item::Reclaim {
                    journal: name.to_owned(),
                    reclaimed: layout.reclaimed.0.max(model.reclaimed),
                    first_retained: first,
                });
            }
            for (segment, length) in &layout.segments {
                let replicated = model.segments.get(segment).map_or(0, |state| state.length);
                // A repaired tail may since have been overwritten by records
                // of the same or greater length; replicate it again.
                let unchanged = match layout.repaired {
                    Some((repaired, offset)) if repaired == *segment => offset.min(*length),
                    _ => *length,
                };
                if unchanged < replicated {
                    items.push(Item::Truncate {
                        journal: name.to_owned(),
                        segment: *segment,
                        length: unchanged,
                    });
                }
                extents(
                    name,
                    *segment,
                    replicated.min(unchanged),
                    *length,
                    &mut items,
                );
            }
        }
        for item in items {
            self.0.shared.append(item)?;
        }
        journal.attach_mirror(Arc::new(JournalAdapter {
            shared: self.0.shared.clone(),
            name: name.to_owned(),
        }));
        Ok(())
    }

    /// Replicate a small authority file after writing it locally. Only files
    /// named by the layout are restored, so any other name is refused here.
    pub fn write_file(&self, name: &str, data: &[u8]) -> Result<()> {
        ensure!(
            self.0.layout.files.iter().any(|file| file == name),
            "{name} is not a replicated authority file"
        );
        self.0.shared.append(Item::File {
            name: name.to_owned(),
            data: data.to_vec(),
        })?;
        Ok(())
    }

    /// Checkpoint all local state, including journals and files not yet
    /// attached, and wait until it is durable. Barriers pass only afterwards,
    /// so nothing this process inherited is visible externally before it is
    /// replicated.
    pub async fn baseline(&self) -> Result<()> {
        // Model what is not attached yet without holding the shared state:
        // verifying reused parts reads every retained journal byte.
        let (journals, files) = {
            let state = self.0.shared.lock();
            let journals: Vec<String> = (self.0.layout.journals.iter())
                .filter(|name| !state.journals.contains_key(*name))
                .cloned()
                .collect();
            let files: Vec<String> = (self.0.layout.files.iter())
                .filter(|name| !state.files.contains_key(*name))
                .cloned()
                .collect();
            (journals, files)
        };
        let inner = self.0.clone();
        let (journals, files) = tokio::task::spawn_blocking(move || -> Result<_> {
            let mut models = Vec::new();
            for name in journals {
                let mut model = scan_journal(&inner.state_dir.join(&name))?;
                if let Some(image) = inner.inherited.get(&name) {
                    reuse_parts(&mut model, image)?;
                }
                models.push((name, model));
            }
            let mut contents = Vec::new();
            for name in files {
                match fs::read(inner.state_dir.join(&name)) {
                    Ok(data) => contents.push((name, data)),
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => return Err(error.into()),
                }
            }
            Ok((models, contents))
        })
        .await
        .context("baseline scan task")??;
        {
            // Anything attached or written meanwhile is already modeled.
            let mut state = self.0.shared.lock();
            let mut changed = false;
            for (name, model) in journals {
                if let Entry::Vacant(entry) = state.journals.entry(name) {
                    entry.insert(model);
                    changed = true;
                }
            }
            for (name, data) in files {
                if let Entry::Vacant(entry) = state.files.entry(name) {
                    entry.insert(data);
                    changed = true;
                }
            }
            state.models_changed |= changed;
        }
        let (reply, response) = oneshot::channel();
        self.0
            .requests
            .send(checkpoint::Request::Now(reply))
            .await
            .context("durability runtime stopped")?;
        response
            .await
            .context("durability runtime stopped during checkpoint")?
    }

    /// Wait until every change appended so far is durable and this process
    /// still owns the state.
    pub async fn barrier(&self) -> Result<()> {
        self.0.shared.barrier().await
    }

    /// Position of the last appended change.
    pub fn position(&self) -> u64 {
        self.0.shared.position()
    }

    /// Whether the change at `position`, and everything before it, is durable
    /// under a valid lease. Never waits.
    pub fn covers(&self, position: u64) -> bool {
        self.0.shared.covers(position)
    }

    /// Wait until `position` is covered.
    pub async fn wait_for(&self, position: u64) -> Result<()> {
        self.0.shared.wait_for(position).await
    }

    pub fn status(&self) -> Status {
        let state = self.0.shared.lock();
        Status {
            uri: self.0.uri.clone(),
            mode: self.0.mode,
            epoch: self.0.shared.epoch,
            appended: state.next_ordinal - 1,
            durable: state.durable - 1,
            lease_remaining_ms: state
                .lease_until
                .map(|until| until.saturating_duration_since(Instant::now()).as_millis() as u64),
            failure: state.failure.as_deref().map(str::to_owned),
            restore: self.0.restore.clone(),
            recovery_ms: self.0.recovery.as_millis() as u64,
            waited_for_previous_owner: self.0.waited,
            stats: state.stats.clone(),
        }
    }

    /// Replicate a registered local row-index checkpoint directory. Blocking;
    /// call from a blocking thread. Returns total and newly uploaded bytes.
    pub fn upload_index_checkpoint(&self, directory: &Path) -> Result<(u64, u64)> {
        let operator = self.0.operator.clone();
        let shared = self.0.shared.clone();
        let hashes = self.0.index_hashes.clone();
        let directory = directory.to_path_buf();
        let started = Instant::now();
        let (total, uploaded) = self.0.runtime.block_on(async move {
            index::upload(&shared, &operator, &hashes, &directory).await
        })?;
        metrics::histogram!("flow_durability_index_checkpoint_seconds")
            .record(started.elapsed().as_secs_f64());
        metrics::counter!("flow_durability_index_checkpoint_bytes_total").increment(uploaded);
        Ok((total, uploaded))
    }

    /// Delete replicated index checkpoints other than `keep` (directory names)
    /// and files none of them reference. Blocking; requires a fresh lease.
    pub fn retain_index_checkpoints(&self, keep: &[String]) -> Result<usize> {
        let operator = self.0.operator.clone();
        let shared = self.0.shared.clone();
        let keep = keep.to_vec();
        self.0
            .runtime
            .block_on(async move { index::retain(&shared, &operator, &keep).await })
    }

    /// Download the replicated index checkpoint `name` into `directory`.
    /// Returns false when none exists.
    pub async fn download_index_checkpoint(&self, name: &str, directory: &Path) -> Result<bool> {
        let operator = self.0.operator.clone();
        let name = name.to_owned();
        let directory = directory.to_path_buf();
        self.0
            .runtime
            .spawn(async move { index::download(&operator, &name, &directory).await })
            .await
            .context("index checkpoint download task")?
    }

    /// The first error that stopped replication, if any.
    pub fn failure(&self) -> Option<anyhow::Error> {
        writer::failure(&self.0.shared)
    }

    /// Resolves once replication has stopped, for example because a newer
    /// owner claimed the state.
    pub async fn failed(&self) -> anyhow::Error {
        self.0.shared.failed().await
    }

    /// Stop after a final barrier and a release record, so the next owner
    /// starts without waiting for this lease. Call after every user stopped.
    /// Gives up after one lease, which is all a release saves the next owner,
    /// so a clean shutdown does not hang while object storage is unavailable.
    pub async fn release(&self) -> Result<()> {
        self.within_lease("release", async {
            // A final checkpoint lets the next start replay almost no log. It
            // is an optimisation; the log already holds everything appended.
            if self.0.shared.lock().baseline {
                let (reply, response) = oneshot::channel();
                if self
                    .0
                    .requests
                    .send(checkpoint::Request::Now(reply))
                    .await
                    .is_ok()
                    && let Ok(Err(error)) = response.await
                {
                    tracing::warn!(error = %format!("{error:#}"), "final durability checkpoint failed");
                }
            }
            // No baseline is needed: without one this owner had no external effects.
            let ordinal = self.0.shared.append(Item::Release {
                epoch: self.0.shared.epoch,
            })?;
            self.0.shared.wait_durable(ordinal).await
        })
        .await?;
        self.stop();
        Ok(())
    }

    /// Wait, for at most one lease, until every change appended so far is
    /// durable. Before stopping after an error, this gives records written
    /// just before it, such as a resync marker, a chance to replicate.
    pub async fn flush(&self) -> Result<()> {
        let ordinal = self.0.shared.position();
        self.within_lease("flush", self.0.shared.wait_durable(ordinal))
            .await
    }

    async fn within_lease(
        &self,
        what: &str,
        work: impl std::future::Future<Output = Result<()>>,
    ) -> Result<()> {
        let lease = self.0.shared.tuning.lease;
        let deadline = Instant::now() + lease;
        *self
            .0
            .shutdown
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(deadline);
        let deadline = tokio::time::Instant::from_std(deadline);
        tokio::time::timeout_at(deadline, work)
            .await
            .with_context(|| {
                format!(
                    "remote durability {what} did not complete within the {}s lease",
                    lease.as_secs()
                )
            })?
    }

    /// Stop replication without a release. The next owner waits one lease.
    /// Queued records drain until one lease after the latest release or
    /// flush began, or otherwise for one lease.
    pub fn stop(&self) {
        let deadline = self
            .0
            .shutdown
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .unwrap_or_else(|| Instant::now() + self.0.shared.tuning.lease);
        if let Some(stop) = self
            .0
            .stop
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        {
            let _ = stop.send(deadline);
        }
        if let Some(thread) = self
            .0
            .thread
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        {
            let _ = thread.join();
        }
        self.0
            .lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
    }
}

fn extents(name: &str, segment: u64, start: u64, end: u64, items: &mut Vec<Item>) {
    let mut offset = start;
    while offset < end {
        let len = (end - offset).min(MAX_EXTENT_BYTES);
        items.push(Item::Extent {
            journal: name.to_owned(),
            segment,
            offset,
            len,
        });
        offset += len;
    }
}

/// Adopt replicated parts whose bytes still match the local segment prefix,
/// so a restart does not upload unchanged journal bytes again.
fn reuse_parts(model: &mut JournalModel, image: &format::JournalImage) -> Result<()> {
    use std::io::{Read, Seek, SeekFrom};
    for (segment, state) in &mut model.segments {
        let Some(replicated) = image.segments.get(segment) else {
            continue;
        };
        if replicated.length > state.length || replicated.parts.is_empty() {
            continue;
        }
        let path = model
            .root
            .join(flow_ingress_journal::segment_file_name(*segment));
        let mut file = fs::File::open(&path)?;
        let mut matched = Vec::new();
        for part in &replicated.parts {
            let mut data = vec![0; (part.end - part.start) as usize];
            file.seek(SeekFrom::Start(part.start))?;
            file.read_exact(&mut data)?;
            if crc32fast::hash(&data) != part.crc32 {
                break;
            }
            matched.push(part.clone());
        }
        state.parts = matched;
    }
    Ok(())
}

/// Model a journal directory that is not open yet from its files.
fn scan_journal(root: &Path) -> Result<JournalModel> {
    let mut segments = BTreeMap::new();
    match fs::read_dir(root) {
        Ok(entries) => {
            for entry in entries {
                let entry = entry?;
                if let Some(id) = entry
                    .file_name()
                    .to_str()
                    .and_then(|name| name.strip_suffix(".segment"))
                    .and_then(|id| id.parse::<u64>().ok())
                {
                    segments.insert(
                        id,
                        SegmentModel {
                            length: entry.metadata()?.len(),
                            ..SegmentModel::default()
                        },
                    );
                }
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    let reclaimed = if root.exists() {
        flow_ingress_journal::read_reclaimed(root)?.0
    } else {
        0
    };
    Ok(JournalModel {
        root: root.to_path_buf(),
        reclaimed,
        segments,
        last_extents: BTreeMap::new(),
    })
}

struct ControlAdapter(Arc<Shared>);

impl ControlMirror for ControlAdapter {
    fn batch(&self, data: Vec<u8>) -> std::io::Result<()> {
        self.0.append(Item::Control(data)).map(|_| ())
    }
    fn position(&self) -> u64 {
        self.0.position()
    }
}

struct JournalAdapter {
    shared: Arc<Shared>,
    name: String,
}

impl Mirror for JournalAdapter {
    fn extent(&self, segment: u64, offset: u64, len: u64) -> std::io::Result<()> {
        let mut items = Vec::new();
        extents(&self.name, segment, offset, offset + len, &mut items);
        for item in items {
            self.shared.append(item)?;
        }
        Ok(())
    }

    fn reclaim(&self, reclaimed: PgLsn, first_retained: u64) -> std::io::Result<u64> {
        // Extents are read from their segment when uploaded, so a segment
        // stays until its bytes are durable. Normally they already are,
        // because publishing its transactions required a barrier.
        let (kept, changed) = {
            let state = self.shared.lock();
            let Some(model) = state.journals.get(&self.name) else {
                return Ok(first_retained);
            };
            let kept = model
                .last_extents
                .range(..first_retained)
                .find(|(_, ordinal)| **ordinal >= state.durable)
                .map_or(first_retained, |(segment, _)| *segment);
            let changed = reclaimed.0 > model.reclaimed
                || model
                    .segments
                    .keys()
                    .next()
                    .is_some_and(|first| *first < kept);
            (kept, changed)
        };
        if changed {
            self.shared.append(Item::Reclaim {
                journal: self.name.clone(),
                reclaimed: reclaimed.0,
                first_retained: kept,
            })?;
        }
        Ok(kept)
    }
}

/// Refuse to run a replicated state directory without `[durability]`.
pub fn ensure_not_replicated(state_dir: &Path) -> Result<()> {
    let marker = state_dir.join(recovery::MARKER);
    if marker.exists() {
        bail!(
            "{} is replicated to object storage; enable [durability] with the same uri, or remove {} to return it to local-only durability",
            state_dir.display(),
            marker.display()
        );
    }
    Ok(())
}
