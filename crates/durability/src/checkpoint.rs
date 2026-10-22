//! Checkpoints bound recovery to a recent image plus a short log tail, and
//! let the owner delete everything older than the previous checkpoint.
use crate::format::{
    self, Checkpoint, ControlImage, JournalImage, Part, SegmentImage, checkpoint_key, control_key,
};
use crate::store;
use crate::writer::{self, Shared};
use anyhow::{Context, Result, ensure};
use bytes::Bytes;
use flow_state_store::ControlStore;
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

pub(crate) enum Request {
    Control(ControlStore),
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
    let mut control: Option<ControlStore> = None;
    let mut waiters: Vec<oneshot::Sender<Result<()>>> = Vec::new();
    let mut last = Instant::now();
    let mut retention_pending = false;
    let mut count = 0;
    let mut warned: Option<Instant> = None;
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
        // Object writes retry until they land, so stop waiting on them once
        // replication stops; the next pass answers any waiters.
        let result = tokio::select! {
            result = checkpoint(&shared, &operator, source, &history, count) => result,
            () = shared.stopped() => continue,
        };
        last = Instant::now();
        match result {
            Ok((key, checkpoint)) => {
                let elapsed = started.elapsed();
                metrics::histogram!("flow_durability_checkpoint_seconds")
                    .record(elapsed.as_secs_f64());
                shared.lock().stats.checkpoints += 1;
                tracing::info!(
                    event = "durability_checkpoint",
                    key = %key,
                    ordinal = checkpoint.ordinal,
                    replay_from = checkpoint.replay_from,
                    elapsed_ms = elapsed.as_millis() as u64,
                    "remote durability checkpoint written"
                );
                warned = None;
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
            // Waiters keep waiting, as at a barrier: the next tick retries
            // until a checkpoint lands or replication stops. The models are
            // still unwritten, so an unchanged log must not satisfy them.
            Err(error) => {
                shared.lock().models_changed = true;
                if warned.is_none_or(|at| at.elapsed() >= shared.tuning.stall_warning) {
                    warned = Some(Instant::now());
                    tracing::warn!(error = %format!("{error:#}"), waiting = waiters.len(), "remote durability checkpoint failed; retrying");
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
    writer::create(shared, operator, "part", &object, Bytes::from(data)).await?;
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
    control: ControlStore,
    history: &History,
    count: u64,
) -> Result<(String, Checkpoint)> {
    let (control_ordinal, entries) = tokio::task::spawn_blocking(move || control.mirror_snapshot())
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
            writer::create(shared, operator, "control", &object, Bytes::from(encoded)).await?;
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
    let bytes = Bytes::from(serde_json::to_vec(&checkpoint)?);
    writer::create(shared, operator, "checkpoint", &key, bytes).await?;
    // Later checkpoints reuse these parts unless the segment was truncated
    // meanwhile: bytes rewritten after a truncation differ from the parts
    // even once the segment grows back past them.
    let mut state = shared.lock();
    for (name, image) in &checkpoint.journals {
        let Some(model) = state.journals.get_mut(name) else {
            continue;
        };
        for (segment, written) in &image.segments {
            let truncated = journals[name].segments[segment].truncated;
            if let Some(current) = model.segments.get_mut(segment)
                && current.length >= written.length
                && current.truncated == truncated
            {
                current.parts = written.parts.clone();
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
    shared.require_lease(shared.tuning.lease / 2, "delete objects")?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::writer::{self, Item, JournalModel, SegmentModel, Tuning};

    fn tuning() -> Tuning {
        Tuning {
            max_batch_bytes: 8 << 20,
            max_inflight: 4,
            writes_per_second: 0.0,
            queue_memory_bytes: 1 << 20,
            lease: Duration::from_secs(10),
            request_timeout: Duration::from_secs(1),
            stall_warning: Duration::from_secs(5),
        }
    }

    fn segment(root: &Path) -> PathBuf {
        root.join(flow_ingress_journal::segment_file_name(0))
    }

    /// The journal bytes a checkpoint names for segment 0.
    async fn imaged(operator: &Operator, checkpoint: &Checkpoint) -> Vec<u8> {
        let mut bytes = Vec::new();
        for part in &checkpoint.journals["journal"].segments[&0].parts {
            bytes.extend_from_slice(&store::read(operator, &part.object).await.unwrap());
        }
        bytes
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_checkpoint_finishing_after_a_truncation_does_not_adopt_stale_parts() {
        let operator = Operator::new(opendal::services::Memory::default())
            .unwrap()
            .finish();
        let state = tempfile::tempdir().unwrap();
        let root = state.path().join("journal");
        std::fs::create_dir_all(&root).unwrap();
        // A torn tail copied by the baseline before the journal repaired it.
        std::fs::write(segment(&root), [b'a'; 100]).unwrap();
        let shared = Arc::new(Shared::new(tuning(), 1, 0, 1));
        shared.renewed(Instant::now() + Duration::from_secs(60));
        shared.lock().journals.insert(
            "journal".into(),
            JournalModel {
                root: root.clone(),
                reclaimed: 0,
                segments: BTreeMap::from([(
                    0,
                    SegmentModel {
                        length: 100,
                        ..SegmentModel::default()
                    },
                )]),
                last_extents: BTreeMap::new(),
            },
        );
        let writer = tokio::spawn(writer::write_loop(shared.clone(), operator.clone()));
        let control = ControlStore::open(state.path().join("control")).unwrap();
        control
            .attach_mirror(Arc::new(crate::ControlAdapter(shared.clone())))
            .unwrap();
        let first = checkpoint(&shared, &operator, control.clone(), &History::default(), 1)
            .await
            .unwrap();
        let history = History {
            latest: Some(first),
            previous: None,
        };

        // The next checkpoint copies the models, then stalls without a lease.
        shared.lock().lease_until = None;
        shared.lock().models_changed = true;
        let pending = tokio::spawn({
            let (shared, operator, control) = (shared.clone(), operator.clone(), control.clone());
            async move { checkpoint(&shared, &operator, control, &history, 2).await }
        });
        while shared.lock().models_changed {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        // Meanwhile the journal opens, cuts the torn tail and writes past it.
        let file = std::fs::OpenOptions::new()
            .write(true)
            .open(segment(&root))
            .unwrap();
        file.set_len(90).unwrap();
        std::os::unix::fs::FileExt::write_all_at(&file, &[b'b'; 30], 90).unwrap();
        drop(file);
        for item in [
            Item::Truncate {
                journal: "journal".into(),
                segment: 0,
                length: 90,
            },
            Item::Extent {
                journal: "journal".into(),
                segment: 0,
                offset: 90,
                len: 30,
            },
        ] {
            shared.append(item).unwrap();
        }
        shared.renewed(Instant::now() + Duration::from_secs(60));
        let (key, second) = pending.await.unwrap().unwrap();
        assert!(
            shared.lock().journals["journal"].segments[&0].covered() <= 90,
            "parts uploaded before the truncation were adopted"
        );

        // A later checkpoint names exactly the current bytes.
        let history = History {
            latest: Some((key, second)),
            previous: None,
        };
        let (_, third) = checkpoint(&shared, &operator, control, &history, 3)
            .await
            .unwrap();
        let mut expected = vec![b'a'; 90];
        expected.extend_from_slice(&[b'b'; 30]);
        assert_eq!(imaged(&operator, &third).await, expected);
        shared.close();
        writer.await.unwrap();
    }

    #[tokio::test]
    async fn checkpoint_objects_tolerate_lost_responses_but_not_another_owner() {
        let operator = Operator::new(opendal::services::Memory::default())
            .unwrap()
            .finish();
        let shared = Shared::new(tuning(), 1, 0, 1);
        shared.renewed(Instant::now() + Duration::from_secs(60));
        let ours = Bytes::from_static(b"ours");
        operator.write("control/a", ours.clone()).await.unwrap();
        // The same bytes are our own earlier attempt.
        writer::create(&shared, &operator, "control", "control/a", ours)
            .await
            .unwrap();
        assert!(writer::failure(&shared).is_none());
        operator
            .write("control/b", b"theirs".to_vec())
            .await
            .unwrap();
        let bytes = Bytes::from_static(b"ours");
        assert!(
            writer::create(&shared, &operator, "control", "control/b", bytes)
                .await
                .is_err()
        );
        assert!(writer::failure(&shared).is_some(), "replication must stop");
    }
}
