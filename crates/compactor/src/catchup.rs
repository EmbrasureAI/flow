//! Correct a completed data build using an exact catalog and index head.
//! Only additive position deletes and unrelated physical changes are accepted.

use crate::{BuiltData, DeleteReadLimits, Policy, ReadLimits, WorkerOutput, stage_delete_files};
use anyhow::{Result, ensure};
use flow_iceberg_ext::{CommitBase, ManifestCache, SnapshotView};
use flow_materializer::WriterConfig;
use flow_model::{FileId, RowLocation, TableSchema};
use flow_state_store::{IndexDelta, RowIndex};
use iceberg::{spec::DataContentType, table::Table};
use std::{
    collections::{BTreeMap, BTreeSet},
    time::{Duration, Instant},
};

/// A safe cancellation, before preparing or submitting a catalog commit.
#[derive(Debug, thiserror::Error)]
#[error("compaction catch-up requires replanning: {0}")]
pub struct CatchUpRejected(pub &'static str);

const MAX_ANCESTRY_SNAPSHOTS: usize = 256;

/// The coordinator supplies a row-index view and live counts captured at the
/// same head. The worker owns no catalog handle and cannot publish. Accepted
/// mappings are emitted in bounded batches as they are checked. A caller may
/// stage those batches into a fenced Building operation, or validate them in a
/// detached preparation and stream `WorkerOutput::index_deltas` only after an
/// exact activation check.
#[allow(clippy::too_many_arguments)]
pub async fn catch_up(
    table: &Table,
    schema: &TableSchema,
    base: &CommitBase,
    built: BuiltData,
    live_index: &(impl RowIndex + ?Sized),
    live_counts: &BTreeMap<FileId, u64>,
    policy: &Policy,
    config: WriterConfig,
    read_limits: &ReadLimits,
    cache: &ManifestCache,
    stage_deltas: impl FnMut(Vec<IndexDelta>) -> Result<()> + Send,
) -> Result<WorkerOutput> {
    let started = Instant::now();
    base.validate(table)
        .map_err(|_| CatchUpRejected("base ancestry or schema changed"))?;
    let BuiltData {
        mut output,
        base: original,
    } = built;
    ensure!(
        base.snapshot_id == Some(output.plan.base_snapshot_id)
            && original.snapshot_id == base.snapshot_id
            && schema.table_id == output.table_id
            && table
                .metadata()
                .snapshot_by_id(output.plan.base_snapshot_id)
                .is_some_and(|snapshot| snapshot.sequence_number() == base.sequence_number),
        "completed build differs from its captured base"
    );
    let head =
        validate_history(table, base, &original, &output, cache, policy, read_limits).await?;
    let deletes = required_deletes(&head, &output.plan.input_files)?;
    ensure!(
        deletes.len() <= policy.max_delete_input_files,
        CatchUpRejected("delete input count exceeds catch-up budget")
    );
    let (bytes, rows, sequence) =
        deletes
            .iter()
            .try_fold((0u64, 0u64, None), |(bytes, rows, sequence), id| {
                let entry = &head.live_files[&id.0];
                let next = entry
                    .sequence_number
                    .ok_or_else(|| anyhow::anyhow!("delete lacks data sequence"))?;
                Ok::<_, anyhow::Error>((
                    bytes
                        .checked_add(flow_iceberg_ext::delete_content_size(&entry.data_file))
                        .ok_or_else(|| anyhow::anyhow!("delete byte overflow"))?,
                    rows.checked_add(entry.data_file.record_count())
                        .ok_or_else(|| anyhow::anyhow!("delete row overflow"))?,
                    Some(sequence.map_or(next, |sequence: i64| sequence.max(next))),
                ))
            })?;
    ensure!(
        bytes <= policy.max_delete_input_bytes && rows <= policy.max_delete_input_rows,
        CatchUpRejected("delete input size exceeds catch-up budget")
    );
    let late_deletes = deletes
        .difference(&output.plan.delete_files)
        .cloned()
        .collect();
    let data = head
        .live_files
        .values()
        .filter(|entry| entry.content_type() == DataContentType::Data)
        .map(|entry| FileId(flow_iceberg_ext::content_file_id(&entry.data_file)))
        .collect();
    let batch_rows = output
        .scratch
        .batch_rows()
        .min(config.row_group_rows)
        .max(1);
    let batch_bytes = config.batch_bytes.min(config.row_group_bytes);
    ensure!(batch_bytes > 0, "mapping batch byte limit must be positive");
    let late_scan = format!("{}-late", output.operation_id.0);
    let output_scan = format!("{}-output", output.operation_id.0);
    let validation_finished = Instant::now();
    let scanned_late_delete_rows = stage_delete_files(
        table,
        schema,
        &head,
        &data,
        &late_deletes,
        &output.scratch,
        &late_scan,
        batch_rows,
        DeleteReadLimits {
            max_bytes: policy.max_delete_input_bytes,
            max_rows: policy.max_delete_input_rows,
        },
        read_limits,
    )
    .await?;
    let late_scan_finished = Instant::now();

    let mut positions = Vec::with_capacity(batch_rows);
    let mut mapping_lookup = Duration::ZERO;
    let mut mask_spool = Duration::ZERO;
    let mut late = output
        .scratch
        .position_deletes(&late_scan, &schema.table_id)
        .peekable();
    let mut deltas = output
        .scratch
        .prepared_deltas(&output.operation_id)?
        .peekable();
    let mut accepted = BTreeMap::<FileId, u64>::new();
    let mut mapped_rows = 0u64;
    let mut masked_rows = 0u64;
    let mut previous = None::<(FileId, u64)>;
    let mapping = stage_mapping_batches(stage_deltas, || {
        let batch = mapping_batch(&mut deltas, batch_rows, batch_bytes)?;
        if batch.is_empty() {
            return Ok(None);
        }
        mapped_rows += batch.len() as u64;
        let keys = batch
            .iter()
            .map(|delta| delta.key.clone())
            .collect::<Vec<_>>();
        let lookup_started = Instant::now();
        let current = live_index.lookup_many(&schema.table_id, &keys)?;
        mapping_lookup += lookup_started.elapsed();
        let mut accepted_deltas = Vec::with_capacity(batch.len());
        for (delta, current) in batch.into_iter().zip(current) {
            let old = delta
                .expected
                .ok_or_else(|| anyhow::anyhow!("worker mapping lacks original"))?;
            let replacement = delta
                .replacement
                .ok_or_else(|| anyhow::anyhow!("worker mapping lacks replacement"))?;
            if let Some((file, position)) = &mut previous {
                ensure!(
                    (&*file, *position) < physical(&old),
                    "worker mappings are not in input order"
                );
                if *file != old.data_file_id {
                    file.0.clone_from(&old.data_file_id.0);
                }
                *position = old.row_position;
            } else {
                previous = Some((old.data_file_id.clone(), old.row_position));
            }
            let deleted = loop {
                match late.peek() {
                    Some(Err(_)) => {
                        return Err(late.next().expect("peeked delete").unwrap_err().into());
                    }
                    Some(Ok(position)) if physical(position) < physical(&old) => {
                        late.next();
                    }
                    Some(Ok(position)) => break physical(position) == physical(&old),
                    None => break false,
                }
            };
            if !deleted {
                ensure!(
                    current.as_ref() == Some(&old),
                    CatchUpRejected("index moved without an effective position delete")
                );
                if let Some(count) = accepted.get_mut(&old.data_file_id) {
                    *count += 1;
                } else {
                    accepted.insert(old.data_file_id.clone(), 1);
                }
                accepted_deltas.push(IndexDelta {
                    key: delta.key,
                    expected: Some(old),
                    replacement: Some(replacement),
                });
            } else {
                ensure!(
                    current.as_ref().is_none_or(|current| {
                        !output.plan.input_files.contains(&current.data_file_id)
                            && current.source_commit_lsn > old.source_commit_lsn
                    }),
                    CatchUpRejected("late delete and indexed row disagree")
                );
                // Delete/reinsert can reset row_version. Source LSN and the
                // effective physical delete prove replacement, not that counter.
                ensure!(
                    sequence.is_some_and(|sequence| sequence >= base.sequence_number),
                    CatchUpRejected("late delete cannot mask output at its build sequence")
                );
                positions.push(replacement);
                masked_rows += 1;
            }
        }
        if !positions.is_empty() {
            let spool_started = Instant::now();
            output.scratch.put_position_deletes(
                &output_scan,
                &schema.table_id,
                positions.drain(..),
            )?;
            mask_spool += spool_started.elapsed();
        }
        Ok(Some(accepted_deltas))
    })?;
    for file in &output.plan.input_files {
        ensure!(
            live_counts.get(file).copied() == Some(accepted.get(file).copied().unwrap_or(0)),
            CatchUpRejected("current index coverage differs from replacement mappings")
        );
    }
    drop(deltas);
    drop(late);
    // The effective late-delete set is already ordered by original physical
    // position. Reuse it to filter mappings instead of writing every unchanged
    // row into a second selector on the publication path.
    let build_snapshot_id = output.plan.base_snapshot_id;
    output.masked_inputs = Some(late_scan.clone());
    output.plan.base_snapshot_id = head.snapshot_id.expect("validated nonempty head");
    output.plan.delete_files = deletes;
    output.plan.delete_input_bytes = bytes;
    output.plan.delete_input_rows = rows;
    output.delete_sequence = sequence;
    let v3 = table.metadata().format_version() == iceberg::spec::FormatVersion::V3;
    if v3 {
        crate::worker::retain_shared_legacy_deletes(&mut output, &head)?;
    }
    let mapping_finished = Instant::now();
    output.delete_files = crate::worker::write_residual_positions(
        table,
        &output,
        residual_positions(&output, &late_scan, &output_scan, &data).filter(|position| {
            !v3 || position.as_ref().map_or(true, |position| {
                output
                    .data_files
                    .iter()
                    .any(|file| file.file_path() == position.data_file_id.0)
            })
        }),
        config,
    )
    .await?;
    let finished = Instant::now();
    // The staging thread can overlap the next caller batch. Report the whole
    // mapping interval once; its inner elapsed durations are nested observations.
    let mapping_elapsed = mapping_finished.duration_since(late_scan_finished);
    let mapping_verify = mapping.compute - mapping_lookup - mask_spool;
    let mapping_other = mapping_elapsed + mapping.overlap - mapping.compute - mapping.stage;
    let (output_delete_files, output_delete_bytes) =
        flow_iceberg_ext::physical_file_stats(&output.delete_files);
    tracing::info!(
        event = "compaction_catch_up_completed",
        operation_id = %output.operation_id.0,
        table_id = schema.table_id.0,
        build_snapshot_id,
        head_snapshot_id = output.plan.base_snapshot_id,
        input_delete_files = output.plan.delete_files.len(),
        input_delete_rows = rows,
        input_delete_bytes = bytes,
        late_delete_files = late_deletes.len(),
        scanned_late_delete_rows,
        mapped_rows,
        accepted_rows = accepted.values().sum::<u64>(),
        masked_rows,
        output_data_files = output.data_files.len(),
        output_data_rows = output.data_files.iter().map(|file| file.record_count()).sum::<u64>(),
        output_data_bytes = output.data_files.iter().map(|file| file.file_size_in_bytes()).sum::<u64>(),
        output_delete_files,
        output_delete_rows = output.delete_files.iter().map(|file| file.record_count()).sum::<u64>(),
        output_delete_bytes,
        inner_elapsed_ms = finished.duration_since(started).as_secs_f64() * 1000.0,
        validation_setup_ms = validation_finished.duration_since(started).as_secs_f64() * 1000.0,
        late_delete_scan_stage_ms = late_scan_finished.duration_since(validation_finished).as_secs_f64() * 1000.0,
        residual_spool_ms = mask_spool.as_secs_f64() * 1000.0,
        mapping_lookup_ms = mapping_lookup.as_secs_f64() * 1000.0,
        mapping_stage_ms = mapping.stage.as_secs_f64() * 1000.0,
        mapping_elapsed_ms = mapping_elapsed.as_secs_f64() * 1000.0,
        mapping_overlap_ms = mapping.overlap.as_secs_f64() * 1000.0,
        mapping_other_ms = mapping_other.as_secs_f64() * 1000.0,
        mapping_verify_ms = mapping_verify.as_secs_f64() * 1000.0,
        residual_write_ms = finished.duration_since(mapping_finished).as_secs_f64() * 1000.0,
        "compaction catch-up validated and staged"
    );
    Ok(output)
}

// Charge the owned decoded payload, including fixed slots and both paths.
// The underlying iterator can decode one lookahead before this admission check;
// this is not a bound on decoder allocations, RocksDB buffers or process RSS.
fn mapping_bytes(delta: &IndexDelta) -> Result<usize> {
    std::iter::once(delta.key.0.len())
        .chain(
            delta
                .expected
                .iter()
                .chain(&delta.replacement)
                .flat_map(|row| [row.data_file_id.0.len(), row.partition.len()]),
        )
        .try_fold(size_of::<IndexDelta>(), |bytes, next| {
            bytes
                .checked_add(next)
                .ok_or_else(|| anyhow::anyhow!("mapping size overflow"))
        })
}

fn mapping_batch(
    deltas: &mut std::iter::Peekable<impl Iterator<Item = flow_state_store::Result<IndexDelta>>>,
    row_limit: usize,
    byte_limit: usize,
) -> Result<Vec<IndexDelta>> {
    let mut batch = Vec::with_capacity(row_limit);
    let mut bytes = 0;
    while batch.len() < row_limit {
        let next = match deltas.peek() {
            Some(Ok(delta)) => mapping_bytes(delta)?,
            Some(Err(_)) => return Err(deltas.next().expect("peeked mapping").unwrap_err().into()),
            None => break,
        };
        ensure!(
            next <= byte_limit,
            "mapping payload {next} bytes exceeds writer batch limit {byte_limit} bytes"
        );
        if next > byte_limit - bytes {
            break;
        }
        bytes += next;
        batch.push(deltas.next().expect("admitted mapping")?);
    }
    Ok(batch)
}

/// Elapsed intervals only, not CPU time. The caller retains the scratch
/// iterators; the staging thread owns at most one accepted batch at a time.
#[derive(Default)]
struct MappingTimings {
    compute: Duration,
    stage: Duration,
    overlap: Duration,
}

fn stage_mapping_batches(
    mut stage: impl FnMut(Vec<IndexDelta>) -> Result<()> + Send,
    mut compute: impl FnMut() -> Result<Option<Vec<IndexDelta>>>,
) -> Result<MappingTimings> {
    std::thread::scope(|scope| {
        // Endpoints live inside the scope: unwind closes them before scope
        // joins. One outstanding job means its result cannot fill a second slot.
        let (jobs, input) = std::sync::mpsc::sync_channel::<Vec<IndexDelta>>(1);
        let (output, results) = std::sync::mpsc::sync_channel(1);
        let worker = std::thread::Builder::new()
            .name("flow-catch-up-stage".into())
            .spawn_scoped(scope, move || {
                for batch in input {
                    let started = Instant::now();
                    let result = stage(batch);
                    let finished = Instant::now();
                    let failed = result.is_err();
                    if output.send((started, finished, result)).is_err() || failed {
                        break;
                    }
                }
            })?;
        let result = (|| {
            let mut timing = MappingTimings::default();
            let mut pending = false;
            loop {
                let started = Instant::now();
                let batch = compute();
                let finished = Instant::now();
                timing.compute += finished.duration_since(started);
                if pending {
                    // Do not propagate a compute error before the previous
                    // stage finishes. No cleanup may race its scratch/control use.
                    let (stage_started, stage_finished, result) = results.recv()?;
                    timing.stage += stage_finished.duration_since(stage_started);
                    timing.overlap += stage_finished
                        .min(finished)
                        .saturating_duration_since(stage_started.max(started));
                    result?;
                }
                let Some(batch) = batch? else {
                    break;
                };
                pending = !batch.is_empty();
                if pending {
                    jobs.send(batch)?;
                }
            }
            Ok(timing)
        })();
        // Closing both endpoints also releases a failed or panicking worker.
        // The actual join precedes any error return to Building cleanup.
        drop(jobs);
        drop(results);
        worker
            .join()
            .map_err(|_| anyhow::anyhow!("catch-up staging thread panicked"))?;
        result
    })
}

/// Each spool is ordered and unique by physical position, using bytewise path
/// order followed by the ordinal. Merge with three lookaheads; overlapping input
/// deletes need neither another encoding pass nor a second scratch write.
fn residual_positions<'a>(
    output: &'a WorkerOutput,
    late_scan: &'a str,
    mask_scan: &'a str,
    live_data: &'a BTreeSet<FileId>,
) -> impl Iterator<Item = flow_state_store::Result<RowLocation>> + 'a {
    let mut inputs = [&output.operation_id.0, late_scan, mask_scan]
        .map(|scan| output.scratch.position_deletes(scan, &output.table_id));
    let mut heads: [Option<RowLocation>; 3] = [None, None, None];
    std::iter::from_fn(move || {
        for (index, (input, head)) in inputs.iter_mut().zip(&mut heads).enumerate() {
            while head.is_none() {
                let position = match input.next() {
                    Some(Ok(position)) => position,
                    Some(Err(error)) => return Some(Err(error)),
                    None => break,
                };
                // Original and late residuals must still target live H files.
                // Masks target the replacement data, which is not published yet.
                if index == 2
                    || (!output.plan.input_files.contains(&position.data_file_id)
                        && live_data.contains(&position.data_file_id))
                {
                    *head = Some(position);
                }
            }
        }
        let next = heads
            .iter()
            .enumerate()
            .filter_map(|(index, head)| head.as_ref().map(|head| (index, head)))
            .min_by(|(_, left), (_, right)| physical(left).cmp(&physical(right)))?
            .0;
        let position = heads[next].take().expect("selected residual");
        for head in &mut heads {
            if head
                .as_ref()
                .is_some_and(|head| physical(head) == physical(&position))
            {
                *head = None;
            }
        }
        Some(Ok(position))
    })
}

fn physical(position: &RowLocation) -> (&FileId, u64) {
    (&position.data_file_id, position.row_position)
}

fn required_deletes(view: &SnapshotView, inputs: &BTreeSet<FileId>) -> Result<BTreeSet<FileId>> {
    Ok(inputs
        .iter()
        .map(|file| view.applicable_deletes(&file.0))
        .collect::<iceberg::Result<Vec<_>>>()?
        .into_iter()
        .flatten()
        .map(|entry| FileId(flow_iceberg_ext::content_file_id(&entry.data_file)))
        .collect())
}

#[allow(clippy::too_many_arguments)]
async fn validate_history(
    table: &Table,
    base: &CommitBase,
    original: &SnapshotView,
    output: &WorkerOutput,
    cache: &ManifestCache,
    policy: &Policy,
    read_limits: &ReadLimits,
) -> Result<SnapshotView> {
    let mut history = Vec::new();
    let mut cursor = table.metadata().current_snapshot_id();
    while cursor != base.snapshot_id {
        ensure!(
            history.len() < MAX_ANCESTRY_SNAPSHOTS,
            CatchUpRejected("build ancestry exceeds catch-up budget")
        );
        let id = cursor.ok_or(CatchUpRejected("build base is not an ancestor"))?;
        history.push(id);
        cursor = table
            .metadata()
            .snapshot_by_id(id)
            .ok_or(CatchUpRejected("build ancestry expired"))?
            .parent_snapshot_id();
    }
    let mut prior = original.clone();
    let mut checked_bytes = 0u64;
    let mut checked_rows = 0u64;
    for id in history.into_iter().rev() {
        let next = SnapshotView::load_with_cache(table, id, cache).await?;
        let mut required = output.plan.input_files.clone();
        required.extend(required_deletes(&prior, &output.plan.input_files)?);
        for path in required {
            let before = &prior.live_files[&path.0];
            if let Some(after) = next.live_files.get(&path.0) {
                ensure!(
                    before.data_file == after.data_file
                        && before.sequence_number == after.sequence_number
                        && before.file_sequence_number == after.file_sequence_number,
                    CatchUpRejected("selected input or delete identity changed")
                );
                continue;
            }
            ensure!(
                before.file_format() == iceberg::spec::DataFileFormat::Puffin,
                CatchUpRejected("selected input or delete was removed")
            );
            let target = before
                .data_file
                .referenced_data_file()
                .ok_or_else(|| anyhow::anyhow!("DV lacks target"))?;
            let replacements = next.applicable_deletes(&target)?;
            let after = replacements
                .first()
                .filter(|entry| entry.file_format() == iceberg::spec::DataFileFormat::Puffin)
                .ok_or(CatchUpRejected("cumulative deletion vector was removed"))?;
            let mut vectors = Vec::with_capacity(2);
            for entry in [before.as_ref(), *after] {
                let file = &entry.data_file;
                checked_bytes = checked_bytes
                    .checked_add(flow_iceberg_ext::delete_content_size(file))
                    .ok_or(CatchUpRejected("DV history byte overflow"))?;
                checked_rows = checked_rows
                    .checked_add(file.record_count())
                    .ok_or(CatchUpRejected("DV history row overflow"))?;
                ensure!(
                    checked_bytes <= policy.max_delete_input_bytes
                        && checked_rows <= policy.max_delete_input_rows,
                    CatchUpRejected("DV history exceeds catch-up budget")
                );
                vectors.push(
                    iceberg::puffin::read_deletion_vector(
                        table.file_io(),
                        file.file_path(),
                        u64::try_from(
                            file.content_offset()
                                .ok_or_else(|| anyhow::anyhow!("DV lacks offset"))?,
                        )?,
                        u64::try_from(
                            file.content_size_in_bytes()
                                .ok_or_else(|| anyhow::anyhow!("DV lacks length"))?,
                        )?,
                        &target,
                        file.record_count(),
                        iceberg::puffin::DeletionVectorLimits {
                            max_blob_bytes: read_limits
                                .row_group_uncompressed_bytes
                                .min(policy.max_delete_input_bytes),
                            max_footer_bytes: read_limits.footer_bytes,
                            max_cardinality: policy.max_delete_input_rows,
                        },
                    )
                    .await?,
                );
            }
            let mut newer = vectors[1].iter().peekable();
            for position in vectors[0].iter() {
                while newer.peek().is_some_and(|next| *next < position) {
                    newer.next();
                }
                ensure!(
                    newer.peek() == Some(&position),
                    CatchUpRejected("replacement DV resurrects an input row")
                );
            }
        }
        prior = next;
    }
    Ok(prior)
}

#[cfg(test)]
mod tests {
    use super::*;
    use flow_model::{OperationId, PgLsn, PrimaryKey, TableId};
    use flow_state_store::{OperationKind, PreparedOperation, StateStore};

    #[test]
    fn mapping_admission_splits_wide_keys_and_preserves_oversized_lookahead() {
        let temp = tempfile::TempDir::new().unwrap();
        let operation = OperationId("wide-mappings".into());
        let delta = |id: u8, key_bytes: usize| {
            let location = |prefix: &str| RowLocation {
                data_file_id: FileId(format!("{prefix}/{}", "x".repeat(512))),
                row_position: u64::from(id),
                data_sequence_number: 1,
                spec_id: 0,
                partition: vec![0; 128],
                source_commit_lsn: PgLsn(1),
                row_version: 1,
                row_fingerprint: [id; 16],
            };
            IndexDelta {
                key: PrimaryKey(vec![id; key_bytes]),
                expected: Some(location("old")),
                replacement: Some(location("new")),
            }
        };
        let normal = delta(1, 4096);
        let limit = mapping_bytes(&normal).unwrap() * 2;
        let values = vec![normal, delta(2, 4096), delta(3, 4096), delta(4, limit * 2)];
        let store = StateStore::open(temp.path().join("scratch"), Default::default()).unwrap();
        store
            .prepare(
                PreparedOperation {
                    id: operation.clone(),
                    table_id: TableId(1),
                    kind: OperationKind::Rewrite,
                    base_snapshot_id: None,
                    last_lsn: PgLsn(1),
                    schema_version: 1,
                    artifacts: Vec::new(),
                    payload: Vec::new(),
                },
                values.clone(),
            )
            .unwrap();
        drop(store);
        let store = StateStore::open(temp.path().join("scratch"), Default::default()).unwrap();
        let mut input = store.prepared_deltas(&operation).unwrap().peekable();
        assert_eq!(mapping_batch(&mut input, 10, limit).unwrap(), values[..2]);
        // The byte boundary retains the next mapping, rather than dropping it.
        assert_eq!(mapping_batch(&mut input, 1, limit).unwrap(), values[2..3]);
        let error = mapping_batch(&mut input, 10, limit).unwrap_err();
        assert!(error.to_string().contains("exceeds writer batch limit"));
        assert_eq!(input.next().unwrap().unwrap(), values[3]);
        assert!(input.next().is_none());
    }
}
