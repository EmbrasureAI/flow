//! Checkpoints bound recovery to a recent image plus a short log tail, and
//! let the owner delete everything older than the previous checkpoint.
use crate::format::{
    self, Checkpoint, ControlImage, JournalImage, Part, SegmentImage, checkpoint_key, control_key,
};
use crate::store::{self, Created};
use crate::writer::Shared;
use anyhow::{Context, Result, bail, ensure};
use bytes::Bytes;
use flow_state_store::RawEntry;
use futures::{StreamExt, TryStreamExt};
use opendal::Operator;
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::File,
    io::{Read, Seek, SeekFrom},
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{mpsc, oneshot};

/// A segment written in many small checkpoints is consolidated into one part.
const MAX_PARTS: usize = 16;
/// Journal parts uploaded at once; each is read into memory whole.
const PART_UPLOADS: usize = 4;

/// Produces a consistent control-store image and the log position it covers.
pub trait ControlSource: Send + Sync {
    fn snapshot(&self) -> Result<(u64, Vec<RawEntry>)>;
}

pub(crate) enum Request {
    Control(Arc<dyn ControlSource>),
    Now(oneshot::Sender<Result<()>>),
}

pub(crate) struct Policy {
    pub interval: Duration,
    pub batches: u64,
}

#[derive(Default)]
pub(crate) struct History {
    pub latest: Option<(String, Checkpoint)>,
    pub previous: Option<(String, Checkpoint)>,
}

pub(crate) async fn checkpoint_loop(
    shared: Arc<Shared>,
    operator: Operator,
    mut requests: mpsc::Receiver<Request>,
    policy: Policy,
    mut history: History,
) {
    let mut control: Option<Arc<dyn ControlSource>> = None;
    let mut waiters: Vec<oneshot::Sender<Result<()>>> = Vec::new();
    let mut last = Instant::now();
    let mut retention_pending = false;
    let mut count = 0;
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            request = requests.recv() => match request {
                Some(Request::Control(source)) => control = Some(source),
                Some(Request::Now(reply)) => waiters.push(reply),
                None => return,
            },
            _ = tick.tick() => {}
            _ = shared.wake_checkpoint.notified() => {}
        }
        let (failed, closing, changed, batches, baseline, models_changed) = {
            let state = shared.lock();
            let covered = history.latest.as_ref().map_or(0, |(_, c)| c.ordinal);
            let since = history.latest.as_ref().map_or(0, |(_, c)| c.replay_from);
            (
                state.failure.is_some(),
                state.closing,
                state.next_ordinal - 1 > covered,
                state.next_seq.saturating_sub(since),
                state.baseline,
                state.models_changed,
            )
        };
        if failed || closing {
            for waiter in waiters.drain(..) {
                let _ = waiter.send(Err(anyhow::anyhow!("remote durability stopped")));
            }
            if closing {
                return;
            }
            continue;
        }
        if retention_pending && waiters.is_empty() {
            match retain(&shared, &operator, &history).await {
                Ok(()) => retention_pending = false,
                Err(error) => {
                    tracing::debug!(error = %format!("{error:#}"), "remote durability retention still pending")
                }
            }
        }
        // Nothing to add: the latest checkpoint already covers every record and
        // every modeled file. Before the first baseline, a request must always
        // write one, since that baseline is what lets barriers pass.
        if baseline && !changed && !models_changed && history.latest.is_some() {
            for waiter in waiters.drain(..) {
                let _ = waiter.send(Ok(()));
            }
            continue;
        }
        // Periodic checkpoints wait for the baseline: an earlier one would
        // cover inherited state that the baseline has not modeled yet.
        let due = !waiters.is_empty()
            || (baseline
                && ((changed && last.elapsed() >= policy.interval) || batches >= policy.batches));
        let Some(source) = control.clone().filter(|_| due) else {
            continue;
        };
        let started = Instant::now();
        count += 1;
        let result = checkpoint(&shared, &operator, source, &history, count).await;
        last = Instant::now();
        match result {
            Ok((key, checkpoint)) => {
                let elapsed = started.elapsed();
                metrics::histogram!("flow_durability_checkpoint_seconds")
                    .record(elapsed.as_secs_f64());
                {
                    let mut state = shared.lock();
                    state.stats.checkpoints += 1;
                    state.stats.last_checkpoint_ms = elapsed.as_millis() as u64;
                }
                tracing::info!(
                    event = "durability_checkpoint",
                    key = %key,
                    ordinal = checkpoint.ordinal,
                    replay_from = checkpoint.replay_from,
                    elapsed_ms = elapsed.as_millis() as u64,
                    "remote durability checkpoint written"
                );
                shared.set_baseline();
                for waiter in waiters.drain(..) {
                    let _ = waiter.send(Ok(()));
                }
                history.previous = history.latest.replace((key, checkpoint));
                if let Err(error) = retain(&shared, &operator, &history).await {
                    tracing::warn!(error = %format!("{error:#}"), "remote durability retention failed; retrying");
                    retention_pending = true;
                }
            }
            Err(error) => {
                tracing::warn!(error = %format!("{error:#}"), "remote durability checkpoint failed; retrying");
                for waiter in waiters.drain(..) {
                    let _ = waiter.send(Err(anyhow::anyhow!("{error:#}")));
                }
            }
        }
    }
}

fn read_range(path: &Path, start: u64, end: u64) -> Result<Vec<u8>> {
    let mut file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    file.seek(SeekFrom::Start(start))?;
    let mut data = vec![0; (end - start) as usize];
    file.read_exact(&mut data)
        .with_context(|| format!("{} changed while checkpointing", path.display()))?;
    Ok(data)
}

/// Write a checkpoint object while the lease outlasts the request, so a
/// replaced owner does not keep adding objects.
async fn put(shared: &Shared, operator: &Operator, object: &str, data: Vec<u8>) -> Result<()> {
    shared.writable().await?;
    operator
        .write(object, data)
        .await
        .with_context(|| format!("write {object}"))?;
    Ok(())
}

async fn upload_part(
    shared: &Shared,
    operator: &Operator,
    root: &Path,
    journal: &str,
    segment: u64,
    start: u64,
    end: u64,
) -> Result<Part> {
    let path: PathBuf = root.join(flow_ingress_journal::segment_file_name(segment));
    let data = tokio::task::spawn_blocking(move || read_range(&path, start, end)).await??;
    let crc32 = crc32fast::hash(&data);
    let object = format!(
        "{}-{crc32:08x}.part",
        format::part_key(journal, segment, start, end).trim_end_matches(".part")
    );
    let started = Instant::now();
    put(shared, operator, &object, data).await?;
    metrics::histogram!("flow_durability_put_seconds", "kind" => "part")
        .record(started.elapsed().as_secs_f64());
    Ok(Part {
        object,
        start,
        end,
        crc32,
    })
}

async fn checkpoint(
    shared: &Arc<Shared>,
    operator: &Operator,
    control: Arc<dyn ControlSource>,
    history: &History,
    count: u64,
) -> Result<(String, Checkpoint)> {
    let (control_ordinal, entries) = tokio::task::spawn_blocking(move || control.snapshot())
        .await
        .context("control snapshot task")??;
    let (ordinal, journals, files) = {
        let mut state = shared.lock();
        // Cleared with the copy taken for this checkpoint; a later model
        // change sets it again.
        state.models_changed = false;
        (
            state.next_ordinal - 1,
            state.journals.clone(),
            state.files.clone(),
        )
    };
    ensure!(
        control_ordinal <= ordinal,
        "control snapshot is ahead of the log"
    );
    let reused = history
        .latest
        .as_ref()
        .filter(|(_, latest)| {
            latest.epoch == shared.epoch && latest.control_ordinal == control_ordinal
        })
        .and_then(|(_, latest)| latest.control.clone());
    let control = match reused {
        Some(image) => image,
        None => {
            let count = entries.len() as u64;
            let encoded =
                tokio::task::spawn_blocking(move || format::encode_snapshot(&entries)).await??;
            let bytes = encoded.len() as u64;
            let object = control_key(control_ordinal, shared.epoch, crc32fast::hash(&encoded));
            let started = Instant::now();
            put(shared, operator, &object, encoded).await?;
            metrics::histogram!("flow_durability_put_seconds", "kind" => "control")
                .record(started.elapsed().as_secs_f64());
            metrics::gauge!("flow_durability_control_snapshot_bytes").set(bytes as f64);
            ControlImage {
                object,
                entries: count,
                bytes,
            }
        }
    };
    let mut images = BTreeMap::new();
    let mut uploads = Vec::new();
    for (name, model) in &journals {
        let mut image = JournalImage {
            reclaimed: model.reclaimed,
            segments: BTreeMap::new(),
        };
        for (segment, state) in &model.segments {
            let mut parts = state.parts.clone();
            if state.covered() > state.length {
                parts.clear();
            }
            let covered = parts.last().map_or(0, |part| part.end);
            if state.length > covered {
                let start = if parts.len() >= MAX_PARTS {
                    parts.clear();
                    0
                } else {
                    covered
                };
                uploads.push((
                    name.clone(),
                    model.root.clone(),
                    *segment,
                    start,
                    state.length,
                ));
            }
            image.segments.insert(
                *segment,
                SegmentImage {
                    length: state.length,
                    parts,
                },
            );
        }
        images.insert(name.clone(), image);
    }
    let uploaded: Vec<_> = futures::stream::iter(uploads)
        .map(|(name, root, segment, start, end)| async move {
            let part = upload_part(shared, operator, &root, &name, segment, start, end).await?;
            Ok::<_, anyhow::Error>((name, segment, part))
        })
        .buffer_unordered(PART_UPLOADS)
        .try_collect()
        .await?;
    for (name, segment, part) in uploaded {
        images
            .get_mut(&name)
            .and_then(|image| image.segments.get_mut(&segment))
            .expect("an upload belongs to an imaged segment")
            .parts
            .push(part);
    }
    let replay_from = {
        let state = shared.lock();
        let cut = control_ordinal.min(ordinal);
        state
            .batches
            .iter()
            .find(|(_, last)| *last > cut)
            .map_or(state.next_seq, |(seq, _)| *seq)
    };
    // A checkpoint may only name state whose log records are already durable;
    // otherwise recovery could combine it with an older journal tail.
    shared.wait_durable(ordinal).await?;
    let checkpoint = Checkpoint {
        version: format::VERSION,
        epoch: shared.epoch,
        replay_from,
        ordinal,
        control_ordinal,
        control: Some(control),
        journals: images,
        files,
        created_unix_ms: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64,
    };
    let key = checkpoint_key(replay_from, ordinal, checkpoint.epoch, count);
    shared.writable().await?;
    match store::create(
        operator,
        &key,
        Bytes::from(serde_json::to_vec(&checkpoint)?),
    )
    .await?
    {
        Created::New | Created::Same => {}
        Created::Different(_) => bail!("checkpoint {key} was written by another owner"),
    }
    let mut state = shared.lock();
    for (name, image) in &checkpoint.journals {
        if let Some(model) = state.journals.get_mut(name) {
            for (segment, written) in &image.segments {
                if let Some(current) = model.segments.get_mut(segment)
                    && current.length >= written.length
                {
                    current.parts = written.parts.clone();
                }
            }
        }
    }
    Ok((key, checkpoint))
}

/// Delete objects older than the previous checkpoint. Only an owner with a
/// fresh lease deletes, so a replaced worker cannot remove its successor's
/// recovery inputs.
async fn retain(shared: &Arc<Shared>, operator: &Operator, history: &History) -> Result<()> {
    let (Some((latest_key, latest)), Some((previous_key, previous))) =
        (&history.latest, &history.previous)
    else {
        return Ok(());
    };
    let mut doomed = Vec::new();
    for (seq, key) in store::list_numbered(operator, format::LOG, None).await? {
        if seq >= previous.replay_from {
            break;
        }
        doomed.push(key);
    }
    for key in store::list_recursive(operator, format::CHECKPOINTS).await? {
        if key.as_str() < previous_key.as_str() && &key != latest_key {
            doomed.push(key);
        }
    }
    let keep: BTreeSet<&str> = latest.objects().chain(previous.objects()).collect();
    for directory in [format::CONTROL, format::JOURNALS] {
        for key in store::list_recursive(operator, directory).await? {
            if !keep.contains(key.as_str()) {
                doomed.push(key);
            }
        }
    }
    if doomed.is_empty() {
        return Ok(());
    }
    shared.require_lease(shared.tuning.lease / 2)?;
    let deleted = store::delete_all(operator, doomed).await?;
    metrics::counter!("flow_durability_deleted_objects_total").increment(deleted as u64);
    let mut state = shared.lock();
    state.stats.deleted_objects += deleted as u64;
    let floor = latest.replay_from;
    while state.batches.front().is_some_and(|(seq, _)| *seq < floor) {
        state.batches.pop_front();
    }
    Ok(())
}
