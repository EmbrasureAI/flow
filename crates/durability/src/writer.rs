//! Ordered write-behind replication.
//!
//! Callers append records synchronously and in their local commit order; the
//! append only queues work. One task groups queued records into immutable log
//! batches and keeps several conditional creates in flight. The durable
//! frontier advances only over a contiguous prefix of completed batches, so
//! recovery always observes a prefix of local history.
//!
//! Nothing outside Flow may depend on a record until `barrier` has returned:
//! the barrier waits for every record appended before it and for a valid owner
//! lease. Catalog commits, source acknowledgements and object deletions call it.
use crate::format::{self, Batch, Part, Record, owner_key};
use crate::store::{self, Created};
use anyhow::{Context, Result, anyhow, bail};
use bytes::Bytes;
use futures::{StreamExt, stream::FuturesUnordered};
use opendal::{ErrorKind, Operator};
use std::{
    collections::{BTreeMap, VecDeque},
    fs::File,
    io::{Read, Seek, SeekFrom},
    path::PathBuf,
    sync::{Arc, Condvar, Mutex, MutexGuard},
    time::{Duration, Instant},
};
use tokio::sync::{Notify, watch};

/// A barrier needs at least this much lease left, so a request it allows
/// finishes before a successor can act.
const LEASE_MARGIN: Duration = Duration::from_secs(2);
/// Journal extents are split so any single record fits a batch.
pub(crate) const MAX_EXTENT_BYTES: u64 = 4 << 20;

#[derive(Debug, Clone)]
pub(crate) struct Tuning {
    pub max_batch_bytes: u64,
    pub max_inflight: usize,
    pub writes_per_second: f64,
    pub queue_memory_bytes: u64,
    pub lease: Duration,
    /// Longest a single object request may take.
    pub request_timeout: Duration,
    pub stall_warning: Duration,
}

#[derive(Debug)]
pub(crate) enum Item {
    Control(Vec<u8>),
    /// Read from the local segment when its batch is built.
    Extent {
        journal: String,
        segment: u64,
        offset: u64,
        len: u64,
    },
    Truncate {
        journal: String,
        segment: u64,
        length: u64,
    },
    Reclaim {
        journal: String,
        reclaimed: u64,
        first_retained: u64,
    },
    File {
        name: String,
        data: Vec<u8>,
    },
    Release {
        epoch: u64,
    },
}

impl Item {
    fn memory(&self) -> u64 {
        match self {
            Self::Control(data) | Self::File { data, .. } => data.len() as u64,
            _ => 0,
        }
    }
    fn encoded_bytes(&self) -> u64 {
        64 + match self {
            Self::Control(data) | Self::File { data, .. } => data.len() as u64,
            Self::Extent { len, .. } => *len,
            _ => 0,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct SegmentModel {
    pub length: u64,
    /// Parts named by the latest durable checkpoint, covering a prefix.
    pub parts: Vec<Part>,
    /// Ordinal of the latest truncation, which invalidates parts uploaded
    /// before it even once the segment grows back.
    pub truncated: u64,
}

impl SegmentModel {
    pub(crate) fn covered(&self) -> u64 {
        self.parts.last().map_or(0, |part| part.end)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct JournalModel {
    pub root: PathBuf,
    pub reclaimed: u64,
    pub segments: BTreeMap<u64, SegmentModel>,
    /// Ordinal of the latest extent appended for each segment.
    pub last_extents: BTreeMap<u64, u64>,
}

pub(crate) struct State {
    /// The next ordinal to assign; every lower ordinal was appended.
    pub next_ordinal: u64,
    /// Every ordinal below this is durable.
    pub durable: u64,
    queue: VecDeque<(u64, Item)>,
    memory: u64,
    pub failure: Option<Arc<str>>,
    pub lease_until: Option<Instant>,
    /// Set once a checkpoint covering all local state from before this owner is durable.
    pub baseline: bool,
    /// Journal or file models changed without a record, as by a baseline scan.
    pub models_changed: bool,
    pub journals: BTreeMap<String, JournalModel>,
    pub files: BTreeMap<String, Vec<u8>>,
    /// Batch sequence and last ordinal of batches not yet retired by a checkpoint.
    pub batches: VecDeque<(u64, u64)>,
    pub next_seq: u64,
    pub closing: bool,
    pub stats: Stats,
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct Stats {
    pub batches: u64,
    pub records: u64,
    pub bytes: u64,
    pub retries: u64,
    pub checkpoints: u64,
    pub deleted_objects: u64,
}

pub(crate) struct Shared {
    pub state: Mutex<State>,
    space: Condvar,
    /// Increments on every frontier, lease or failure change for async waiters.
    changed: watch::Sender<u64>,
    pub wake_writer: Notify,
    pub renew_lease: Notify,
    pub wake_checkpoint: Notify,
    pub tuning: Tuning,
    pub epoch: u64,
}

impl Shared {
    pub(crate) fn new(tuning: Tuning, epoch: u64, next_seq: u64, next_ordinal: u64) -> Self {
        Self {
            state: Mutex::new(State {
                next_ordinal,
                durable: next_ordinal,
                queue: VecDeque::new(),
                memory: 0,
                failure: None,
                lease_until: None,
                baseline: false,
                models_changed: false,
                journals: BTreeMap::new(),
                files: BTreeMap::new(),
                batches: VecDeque::new(),
                next_seq,
                closing: false,
                stats: Stats::default(),
            }),
            space: Condvar::new(),
            changed: watch::channel(0).0,
            wake_writer: Notify::new(),
            renew_lease: Notify::new(),
            wake_checkpoint: Notify::new(),
            tuning,
            epoch,
        }
    }

    pub(crate) fn lock(&self) -> MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn notify(&self) {
        self.space.notify_all();
        self.changed.send_modify(|version| *version += 1);
    }

    pub(crate) fn fail(&self, message: String) {
        let mut state = self.lock();
        if state.failure.is_none() {
            tracing::error!(error = %message, "remote durability stopped");
            metrics::gauge!("flow_durability_failed").set(1.0);
            state.failure = Some(message.into());
        }
        drop(state);
        self.notify();
        self.wake_writer.notify_one();
        self.wake_checkpoint.notify_one();
    }

    pub(crate) fn renewed(&self, until: Instant) {
        self.lock().lease_until = Some(until);
        self.notify();
    }

    pub(crate) fn set_baseline(&self) {
        self.lock().baseline = true;
        self.notify();
    }

    /// Queue one record. Blocks only while queued in-memory payloads exceed
    /// their budget; returns the record's ordinal.
    pub(crate) fn append(&self, item: Item) -> std::io::Result<u64> {
        let memory = item.memory();
        let mut state = self.lock();
        loop {
            if let Some(failure) = &state.failure {
                return Err(std::io::Error::other(format!(
                    "remote durability unavailable: {failure}"
                )));
            }
            if state.closing {
                return Err(std::io::Error::other("remote durability is closed"));
            }
            if state.memory == 0 || state.memory + memory <= self.tuning.queue_memory_bytes {
                break;
            }
            state = self
                .space
                .wait(state)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
        let ordinal = state.next_ordinal;
        state.next_ordinal += 1;
        apply_to_model(&mut state, &item, ordinal);
        state.memory += memory;
        state.queue.push_back((ordinal, item));
        let pending = state.next_ordinal - state.durable;
        drop(state);
        metrics::gauge!("flow_durability_pending_records").set(pending as f64);
        self.wake_writer.notify_one();
        Ok(ordinal)
    }

    /// The last appended ordinal, or the predecessor of the first if none.
    pub(crate) fn position(&self) -> u64 {
        self.lock().next_ordinal - 1
    }

    fn satisfied(&self, state: &State, target: u64, now: Instant) -> Result<bool> {
        Ok(state.baseline && self.durable_under_lease(state, target, now)?)
    }

    fn durable_under_lease(&self, state: &State, target: u64, now: Instant) -> Result<bool> {
        if let Some(failure) = &state.failure {
            bail!("remote durability unavailable: {failure}");
        }
        if state.closing && state.durable <= target {
            bail!("remote durability is closed");
        }
        let lease = state
            .lease_until
            .is_some_and(|until| until > now + LEASE_MARGIN);
        if !lease {
            self.renew_lease.notify_one();
        }
        Ok(lease && state.durable > target)
    }

    /// Wait until the lease outlasts one request, so no write of this owner
    /// can land after a successor waited out the lease and read the log.
    pub(crate) async fn writable(&self) -> Result<()> {
        let mut changed = self.changed.subscribe();
        loop {
            {
                let state = self.lock();
                if let Some(failure) = &state.failure {
                    bail!("remote durability unavailable: {failure}");
                }
                if state
                    .lease_until
                    .is_some_and(|until| until > Instant::now() + self.tuning.request_timeout)
                {
                    return Ok(());
                }
            }
            self.renew_lease.notify_one();
            let _ = tokio::time::timeout(Duration::from_millis(250), changed.changed()).await;
        }
    }

    /// Fail unless the lease remains valid for at least `remaining`.
    pub(crate) fn require_lease(&self, remaining: Duration) -> Result<()> {
        let state = self.lock();
        if let Some(failure) = &state.failure {
            bail!("remote durability unavailable: {failure}");
        }
        if !state
            .lease_until
            .is_some_and(|until| until > Instant::now() + remaining)
        {
            self.renew_lease.notify_one();
            bail!("remote durability lease is not fresh enough to delete objects");
        }
        Ok(())
    }

    /// Wait for durability under a lease, without requiring the baseline
    /// checkpoint. Used while writing that checkpoint.
    pub(crate) async fn wait_durable(&self, target: u64) -> Result<()> {
        self.wait_async(target, false).await
    }

    /// Resolves once replication has stopped.
    pub(crate) async fn failed(&self) -> anyhow::Error {
        let mut changed = self.changed.subscribe();
        loop {
            if let Some(error) = failure(self) {
                return error;
            }
            // `self` owns the sender, so the channel never closes here.
            let _ = changed.changed().await;
        }
    }

    /// Resolves once replication has stopped or is closing.
    pub(crate) async fn stopped(&self) {
        let mut changed = self.changed.subscribe();
        loop {
            {
                let state = self.lock();
                if state.failure.is_some() || state.closing {
                    return;
                }
            }
            let _ = changed.changed().await;
        }
    }

    pub(crate) async fn barrier(&self) -> Result<()> {
        let target = self.position();
        self.wait_for(target).await
    }

    /// Wait until `target` and all earlier records are durable under a valid lease.
    pub(crate) async fn wait_for(&self, target: u64) -> Result<()> {
        self.wait_async(target, true).await
    }

    async fn wait_async(&self, target: u64, baseline: bool) -> Result<()> {
        let started = Instant::now();
        let mut warned = started;
        let mut changed = self.changed.subscribe();
        loop {
            {
                let state = self.lock();
                let now = Instant::now();
                let done = if baseline {
                    self.satisfied(&state, target, now)?
                } else {
                    self.durable_under_lease(&state, target, now)?
                };
                if done {
                    if baseline {
                        metrics::histogram!("flow_durability_barrier_seconds")
                            .record(started.elapsed().as_secs_f64());
                    }
                    return Ok(());
                }
                self.warn_stalled(&mut warned, started, target, &state);
            }
            let _ = tokio::time::timeout(Duration::from_millis(250), changed.changed()).await;
        }
    }

    /// Object storage being unavailable pauses external effects; say so
    /// periodically instead of failing them.
    fn warn_stalled(&self, warned: &mut Instant, started: Instant, target: u64, state: &State) {
        if warned.elapsed() < self.tuning.stall_warning {
            return;
        }
        *warned = Instant::now();
        tracing::warn!(
            waited_seconds = started.elapsed().as_secs(),
            target,
            durable = state.durable.saturating_sub(1),
            lease_valid = state
                .lease_until
                .is_some_and(|until| until > Instant::now()),
            baseline = state.baseline,
            "waiting for remote durability; external effects are paused until object storage catches up"
        );
    }

    /// Whether `ordinal` is durable under a valid lease, without waiting.
    pub(crate) fn covers(&self, ordinal: u64) -> bool {
        self.satisfied(&self.lock(), ordinal, Instant::now())
            .unwrap_or(false)
    }

    pub(crate) fn close(&self) {
        self.lock().closing = true;
        self.notify();
        self.wake_writer.notify_one();
        self.wake_checkpoint.notify_one();
        self.renew_lease.notify_one();
    }
}

fn apply_to_model(state: &mut State, item: &Item, ordinal: u64) {
    match item {
        Item::Extent {
            journal,
            segment,
            offset,
            len,
        } => {
            if let Some(model) = state.journals.get_mut(journal) {
                model.segments.entry(*segment).or_default().length = offset + len;
                model.last_extents.insert(*segment, ordinal);
            }
        }
        Item::Truncate {
            journal,
            segment,
            length,
        } => {
            if let Some(model) = state.journals.get_mut(journal) {
                let entry = model.segments.entry(*segment).or_default();
                entry.length = *length;
                entry.truncated = ordinal;
                if entry.covered() > *length {
                    entry.parts.clear();
                }
            }
        }
        Item::Reclaim {
            journal,
            reclaimed,
            first_retained,
        } => {
            if let Some(model) = state.journals.get_mut(journal) {
                model.reclaimed = model.reclaimed.max(*reclaimed);
                model.segments = model.segments.split_off(first_retained);
                model.last_extents = model.last_extents.split_off(first_retained);
            }
        }
        Item::File { name, data } => {
            state.files.insert(name.clone(), data.clone());
        }
        Item::Control(_) | Item::Release { .. } => {}
    }
}

/// Records taken for one batch, with lazy extents resolved to file reads.
struct Taken {
    first_ordinal: u64,
    last_ordinal: u64,
    items: Vec<(Item, Option<PathBuf>)>,
}

fn take(shared: &Shared) -> Option<Taken> {
    let mut state = shared.lock();
    let (first_ordinal, _) = state.queue.front()?;
    let first_ordinal = *first_ordinal;
    let mut items = Vec::new();
    let mut bytes = 0;
    let mut last_ordinal = first_ordinal;
    while let Some((ordinal, item)) = state.queue.front() {
        let size = item.encoded_bytes();
        if !items.is_empty() && bytes + size > shared.tuning.max_batch_bytes {
            break;
        }
        last_ordinal = *ordinal;
        bytes += size;
        let (_, item) = state.queue.pop_front().expect("front exists");
        state.memory -= item.memory();
        let path = match &item {
            Item::Extent {
                journal, segment, ..
            } => state.journals.get(journal).map(|model| {
                model
                    .root
                    .join(flow_ingress_journal::segment_file_name(*segment))
            }),
            _ => None,
        };
        items.push((item, path));
    }
    drop(state);
    shared.space.notify_all();
    Some(Taken {
        first_ordinal,
        last_ordinal,
        items,
    })
}

fn materialize(epoch: u64, seq: u64, taken: Taken) -> Result<(Bytes, u64, u64)> {
    let count = taken.items.len() as u64;
    let records = taken
        .items
        .into_iter()
        .map(|(item, path)| {
            Ok(match item {
                Item::Control(data) => Record::Control(data),
                Item::Extent {
                    journal,
                    segment,
                    offset,
                    len,
                } => {
                    let path =
                        path.with_context(|| format!("journal {journal} is not attached"))?;
                    let mut file = File::open(&path)
                        .with_context(|| format!("read replicated journal {}", path.display()))?;
                    file.seek(SeekFrom::Start(offset))?;
                    let mut data = vec![0; len as usize];
                    file.read_exact(&mut data).with_context(|| {
                        format!(
                            "journal segment {} is shorter than its replicated extent",
                            path.display()
                        )
                    })?;
                    Record::Extent {
                        journal,
                        segment,
                        offset,
                        data,
                    }
                }
                Item::Truncate {
                    journal,
                    segment,
                    length,
                } => Record::Truncate {
                    journal,
                    segment,
                    length,
                },
                Item::Reclaim {
                    journal,
                    reclaimed,
                    first_retained,
                } => Record::Reclaim {
                    journal,
                    reclaimed,
                    first_retained,
                },
                Item::File { name, data } => Record::File { name, data },
                Item::Release { epoch } => Record::Release { epoch },
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let encoded = format::encode_batch(&Batch {
        epoch,
        seq,
        first_ordinal: taken.first_ordinal,
        records,
    })?;
    Ok((Bytes::from(encoded), count, taken.last_ordinal))
}

/// Conditionally create one object, retrying transient failures until it
/// lands or replication stops. Every attempt starts under a lease that
/// outlasts the request. Only an existing object with other bytes, which
/// proves another owner wrote the key, fails, and it stops replication.
pub(crate) async fn create(
    shared: &Shared,
    operator: &Operator,
    kind: &'static str,
    key: &str,
    bytes: Bytes,
) -> Result<()> {
    let mut attempt = 0u32;
    loop {
        shared.writable().await?;
        let started = Instant::now();
        match store::create(operator, key, bytes.clone()).await {
            Ok(Created::New | Created::Same) => {
                metrics::histogram!("flow_durability_put_seconds", "kind" => kind)
                    .record(started.elapsed().as_secs_f64());
                return Ok(());
            }
            Ok(Created::Different(_)) => {
                let message =
                    format!("{key} was written by another owner; this process was replaced");
                shared.fail(message.clone());
                bail!(message);
            }
            Err(error) => {
                attempt += 1;
                shared.lock().stats.retries += 1;
                metrics::counter!("flow_durability_retries_total", "kind" => kind).increment(1);
                tracing::warn!(key, attempt, bytes = bytes.len(), error = %format!("{error:#}"), "remote durability write failed; retrying");
                if let Some(error) = failure(shared) {
                    return Err(error);
                }
                tokio::time::sleep(backoff(attempt)).await;
            }
        }
    }
}

pub(crate) fn backoff(attempt: u32) -> Duration {
    Duration::from_millis((50u64 << attempt.min(6)).min(2_000))
}

/// Admission budget for log writes: `max_inflight` may start at once, then
/// at most `writes_per_second` on average. Larger batches absorb the rest.
struct Budget {
    tokens: f64,
    capacity: f64,
    rate: f64,
    refreshed: Instant,
}

impl Budget {
    fn new(tuning: &Tuning) -> Self {
        let capacity = tuning.max_inflight.max(1) as f64;
        Self {
            tokens: capacity,
            capacity,
            rate: tuning.writes_per_second,
            refreshed: Instant::now(),
        }
    }
    fn refresh(&mut self) {
        if self.rate <= 0.0 {
            self.tokens = self.capacity;
            return;
        }
        let now = Instant::now();
        self.tokens = (self.tokens + now.duration_since(self.refreshed).as_secs_f64() * self.rate)
            .min(self.capacity);
        self.refreshed = now;
    }
    /// Time until a write may start; zero consumes a token.
    fn acquire(&mut self) -> Duration {
        self.refresh();
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            Duration::ZERO
        } else {
            Duration::from_secs_f64((1.0 - self.tokens) / self.rate)
        }
    }
}

/// Group queued records into batches and keep a bounded number in flight.
pub(crate) async fn write_loop(shared: Arc<Shared>, operator: Operator) {
    let mut inflight = FuturesUnordered::new();
    // Batches in sequence order with their completion state.
    let mut order: VecDeque<(u64, u64, bool)> = VecDeque::new();
    let mut budget = Budget::new(&shared.tuning);
    loop {
        let (queued, closing, failed) = {
            let state = shared.lock();
            (
                !state.queue.is_empty(),
                state.closing,
                state.failure.is_some(),
            )
        };
        if failed {
            return;
        }
        if closing && !queued && inflight.is_empty() {
            return;
        }
        let mut delay = None;
        if queued && inflight.len() < shared.tuning.max_inflight {
            let wait = budget.acquire();
            if wait.is_zero() {
                let taken = take(&shared).expect("queue is not empty");
                let seq = {
                    let mut state = shared.lock();
                    let seq = state.next_seq;
                    state.next_seq += 1;
                    state.batches.push_back((seq, taken.last_ordinal));
                    seq
                };
                let epoch = shared.epoch;
                let built =
                    tokio::task::spawn_blocking(move || materialize(epoch, seq, taken)).await;
                let (bytes, records, last_ordinal) = match built {
                    Ok(Ok(built)) => built,
                    Ok(Err(error)) => {
                        shared.fail(format!("build log batch {seq}: {error:#}"));
                        return;
                    }
                    Err(error) => {
                        shared.fail(format!("build log batch {seq}: {error}"));
                        return;
                    }
                };
                {
                    let mut state = shared.lock();
                    state.stats.batches += 1;
                    state.stats.records += records;
                    state.stats.bytes += bytes.len() as u64;
                }
                metrics::histogram!("flow_durability_batch_bytes").record(bytes.len() as f64);
                metrics::histogram!("flow_durability_batch_records").record(records as f64);
                order.push_back((seq, last_ordinal, false));
                let (task_shared, task_operator) = (shared.clone(), operator.clone());
                inflight.push(async move {
                    let key = format::log_key(seq);
                    (
                        seq,
                        create(&task_shared, &task_operator, "log", &key, bytes).await,
                    )
                });
                continue;
            }
            delay = Some(wait);
        }
        tokio::select! {
            Some((seq, result)) = inflight.next(), if !inflight.is_empty() => {
                if let Err(error) = result {
                    shared.fail(format!("{error:#}"));
                    return;
                }
                if let Some(entry) = order.iter_mut().find(|(candidate, _, _)| *candidate == seq) {
                    entry.2 = true;
                }
                let mut advanced = None;
                while order.front().is_some_and(|(_, _, done)| *done) {
                    let (_, last_ordinal, _) = order.pop_front().expect("front exists");
                    advanced = Some(last_ordinal);
                }
                if let Some(last_ordinal) = advanced {
                    let pending = {
                        let mut state = shared.lock();
                        state.durable = last_ordinal + 1;
                        state.next_ordinal - state.durable
                    };
                    metrics::gauge!("flow_durability_pending_records").set(pending as f64);
                    shared.notify();
                }
            }
            _ = shared.wake_writer.notified() => {}
            _ = tokio::time::sleep(delay.unwrap_or(Duration::from_secs(3600))), if delay.is_some() => {}
        }
    }
}

/// Renew the lease by proving no successor has claimed the next epoch.
pub(crate) async fn heartbeat_loop(shared: Arc<Shared>, operator: Operator) {
    let successor = owner_key(shared.epoch + 1);
    let interval = shared.tuning.lease / 4;
    loop {
        {
            let state = shared.lock();
            if state.failure.is_some() || state.closing {
                return;
            }
        }
        let started = Instant::now();
        match operator.stat(&successor).await {
            Err(error) if error.kind() == ErrorKind::NotFound => {
                shared.renewed(started + shared.tuning.lease);
                metrics::gauge!("flow_durability_lease_seconds")
                    .set(shared.tuning.lease.as_secs_f64());
            }
            Ok(_) => {
                shared.fail(format!(
                    "a newer owner (epoch {}) claimed this durable state",
                    shared.epoch + 1
                ));
                return;
            }
            Err(error) => {
                tracing::warn!(error = %error, "remote durability lease renewal failed");
            }
        }
        tokio::select! {
            _ = tokio::time::sleep(interval) => {}
            _ = shared.renew_lease.notified() => {
                // Coalesce bursts of barrier wake-ups into one request.
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
    }
}

pub(crate) fn failure(shared: &Shared) -> Option<anyhow::Error> {
    shared
        .lock()
        .failure
        .as_ref()
        .map(|failure| anyhow!("remote durability unavailable: {failure}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tuning() -> Tuning {
        Tuning {
            max_batch_bytes: 1 << 20,
            max_inflight: 4,
            writes_per_second: 0.0,
            queue_memory_bytes: 1 << 20,
            lease: Duration::from_secs(10),
            request_timeout: Duration::from_secs(4),
            stall_warning: Duration::from_secs(5),
        }
    }

    #[tokio::test]
    async fn barriers_wait_for_the_prefix_and_a_lease() {
        let operator = Operator::new(opendal::services::Memory::default())
            .unwrap()
            .finish();
        let shared = Arc::new(Shared::new(tuning(), 1, 0, 1));
        shared.set_baseline();
        let writer = tokio::spawn(write_loop(shared.clone(), operator.clone()));
        for index in 0..10u8 {
            shared.append(Item::Control(vec![index])).unwrap();
        }
        // Without a lease nothing is written and the barrier must not pass.
        let early = tokio::time::timeout(Duration::from_millis(300), shared.barrier()).await;
        assert!(early.is_err());
        assert!(
            store::list_numbered(&operator, format::LOG, None)
                .await
                .unwrap()
                .is_empty()
        );
        let heartbeat = tokio::spawn(heartbeat_loop(shared.clone(), operator.clone()));
        shared.barrier().await.unwrap();
        assert!(shared.covers(10));
        let mut seen = Vec::new();
        for (_, key) in store::list_numbered(&operator, format::LOG, None)
            .await
            .unwrap()
        {
            let batch = format::decode_batch(&store::read(&operator, &key).await.unwrap()).unwrap();
            for (index, record) in batch.records.into_iter().enumerate() {
                seen.push((batch.first_ordinal + index as u64, record));
            }
        }
        assert_eq!(
            seen,
            (0..10u8)
                .map(|index| (u64::from(index) + 1, Record::Control(vec![index])))
                .collect::<Vec<_>>()
        );
        // A successor claim fences this owner at its next renewal.
        operator.write(&owner_key(2), b"{}".to_vec()).await.unwrap();
        shared.renew_lease.notify_one();
        tokio::time::timeout(Duration::from_secs(5), heartbeat)
            .await
            .unwrap()
            .unwrap();
        assert!(shared.barrier().await.is_err());
        assert!(shared.append(Item::Control(vec![1])).is_err());
        shared.close();
        writer.await.unwrap();
    }

    #[tokio::test]
    async fn a_conflicting_batch_fences_the_writer() {
        let operator = Operator::new(opendal::services::Memory::default())
            .unwrap()
            .finish();
        operator
            .write(&format::log_key(0), b"someone else".to_vec())
            .await
            .unwrap();
        let shared = Arc::new(Shared::new(tuning(), 1, 0, 1));
        shared.renewed(Instant::now() + Duration::from_secs(10));
        let writer = tokio::spawn(write_loop(shared.clone(), operator));
        shared.append(Item::Control(vec![1])).unwrap();
        writer.await.unwrap();
        assert!(failure(&shared).is_some());
    }
}
