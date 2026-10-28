//! Correct a completed data build using an exact catalog and index head.
//! Only additive position deletes and unrelated physical changes are accepted.

use crate::{
    BuiltData, DeleteReadLimits, Policy, ReadLimits, WorkerOutput,
    residual::{ResidualInput, ResidualMerge},
    stage_delete_files,
};
use anyhow::{Result, ensure};
use flow_iceberg_ext::{CommitBase, ManifestCache, SnapshotView};
use flow_materializer::WriterConfig;
use flow_model::{FileId, RowLocation, TableSchema};
use flow_state_store::{IndexDelta, RowIndex};
use iceberg::{puffin::DeleteVector, spec::DataContentType, table::Table};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
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
    let batch_rows = output.scratch.batch_rows().min(config.batch_rows).max(1);
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

    let lazy = output.lazy_files.take();
    let (mapping, mapping_lookup, mask_spool, mapped_rows, accepted_rows, masked_rows) =
        if let Some(files) = lazy {
            let spool_started = Instant::now();
            let (lazy, mapped_rows, masked_rows) = lazy_catch_up(
                &output,
                files,
                &late_scan,
                &output_scan,
                live_counts,
                sequence,
                base.sequence_number,
                batch_rows,
            )?;
            let mask_spool = spool_started.elapsed();
            let accepted_rows = lazy.output_live_rows.iter().map(|(_, rows)| rows).sum();
            output.lazy = Some(lazy);
            (
                // Report translation as compute so the derived verify time is zero.
                MappingTimings {
                    compute: mask_spool,
                    ..MappingTimings::default()
                },
                Duration::ZERO,
                mask_spool,
                mapped_rows,
                accepted_rows,
                masked_rows,
            )
        } else {
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
                                return Err(late
                                    .next()
                                    .expect("peeked delete")
                                    .unwrap_err()
                                    .into());
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
                    live_counts.get(file).copied()
                        == Some(accepted.get(file).copied().unwrap_or(0)),
                    CatchUpRejected("current index coverage differs from replacement mappings")
                );
            }
            drop(deltas);
            drop(late);
            (
                mapping,
                mapping_lookup,
                mask_spool,
                mapped_rows,
                accepted.values().sum::<u64>(),
                masked_rows,
            )
        };
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
    (output.delete_files, output.residual_delete_rows) = crate::worker::write_residual_positions(
        table,
        &output,
        residual_positions(&output, &late_scan, &output_scan, &data, v3),
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
        accepted_rows,
        masked_rows,
        output_data_files = output.data_files.len(),
        output_data_rows = output.data_files.iter().map(|file| file.record_count()).sum::<u64>(),
        output_data_bytes = output.data_files.iter().map(|file| file.file_size_in_bytes()).sum::<u64>(),
        output_delete_files,
        output_delete_rows = output.delete_files.iter().map(|file| file.record_count()).sum::<u64>(),
        output_delete_bytes,
        residual_delete_rows = output.residual_delete_rows,
        shared_input_delete_files = output.plan.delete_files.iter().filter(|id| {
            head.single_target(&head.live_files[&id.0]).is_none()
        }).count(),
        per_target_deletes = output.plan.per_target_deletes,
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

/// Lazy remap catch-up: translate late deletes through the per-file remaps and
/// prove, per input file, that the index at H holds exactly the base survivors
/// that no late delete removed. Index entries keep pointing at the inputs, so no
/// per-row lookup or verification is needed; Flow never leaves an index entry on
/// a deleted row, so equal counts imply the same row set.
#[allow(clippy::too_many_arguments)]
fn lazy_catch_up(
    output: &WorkerOutput,
    files: Vec<flow_state_store::FileRemap>,
    late_scan: &str,
    output_scan: &str,
    live_counts: &BTreeMap<FileId, u64>,
    sequence: Option<i64>,
    build_sequence: i64,
    batch_rows: usize,
) -> Result<(flow_state_store::LazyRemap, u64, u64)> {
    let remaps: BTreeMap<&FileId, &flow_state_store::FileRemap> =
        files.iter().map(|remap| (&remap.source, remap)).collect();
    ensure!(
        remaps
            .keys()
            .all(|file| output.plan.input_files.contains(*file)),
        "lazy remap names a file outside the plan"
    );
    let mut late_by_input = BTreeMap::<&FileId, u64>::new();
    let mut masked_by_output = BTreeMap::<FileId, u64>::new();
    let mut positions = Vec::with_capacity(batch_rows);
    let mut masked_rows = 0u64;
    for position in output.scratch.position_deletes(late_scan, &output.table_id) {
        let position = position?;
        let Some(remap) = remaps.get(&position.data_file_id) else {
            continue;
        };
        // Positions already deleted at the base were never written.
        let Some(replacement) = remap.translate(&position) else {
            continue;
        };
        ensure!(
            sequence.is_some_and(|sequence| sequence >= build_sequence),
            CatchUpRejected("late delete cannot mask output at its build sequence")
        );
        *late_by_input.entry(&remap.source).or_default() += 1;
        *masked_by_output
            .entry(replacement.data_file_id.clone())
            .or_default() += 1;
        positions.push(replacement);
        masked_rows += 1;
        if positions.len() == batch_rows {
            output.scratch.put_position_deletes(
                output_scan,
                &output.table_id,
                positions.drain(..),
            )?;
        }
    }
    if !positions.is_empty() {
        output
            .scratch
            .put_position_deletes(output_scan, &output.table_id, positions.drain(..))?;
    }
    let mut mapped_rows = 0u64;
    for file in &output.plan.input_files {
        let survivors = remaps.get(file).map_or(0, |remap| remap.survivors());
        mapped_rows += survivors;
        let late = late_by_input.get(file).copied().unwrap_or(0);
        ensure!(
            live_counts.get(file).copied() == Some(survivors - late),
            CatchUpRejected("current index coverage differs from replacement mappings")
        );
    }
    let mut output_live_rows = Vec::with_capacity(output.data_files.len());
    for file in &output.data_files {
        let id = FileId(file.file_path().to_owned());
        let masked = masked_by_output.remove(&id).unwrap_or(0);
        let live = file
            .record_count()
            .checked_sub(masked)
            .ok_or_else(|| anyhow::anyhow!("masked rows exceed an output file"))?;
        output_live_rows.push((id, live));
    }
    ensure!(
        masked_by_output.is_empty(),
        "lazy remap targets an unknown output file"
    );
    Ok((
        flow_state_store::LazyRemap {
            files,
            output_live_rows,
        },
        mapped_rows,
        masked_rows,
    ))
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

/// Merge the original input deletes, late deletes and output masks; each
/// spool is already ordered, so overlapping input deletes need neither another
/// encoding pass nor a second scratch write. With `outputs_only` (format v3,
/// whose vectors are per target) only positions on this rewrite's output remain.
fn residual_positions<'a>(
    output: &'a WorkerOutput,
    late_scan: &str,
    mask_scan: &str,
    live_data: &'a BTreeSet<FileId>,
    outputs_only: bool,
) -> ResidualMerge<'a> {
    let on_output = move |file: &FileId| {
        !outputs_only
            || output
                .data_files
                .iter()
                .any(|data| data.file_path() == file.0)
    };
    // Original and late residuals must still target live H files. Masks
    // target the replacement data, which is not published yet.
    let carried = move |file: &FileId| {
        !output.plan.input_files.contains(file) && live_data.contains(file) && on_output(file)
    };
    let cursor = |scan: &str| {
        output
            .scratch
            .position_delete_cursor(scan, &output.table_id)
    };
    ResidualMerge::new(vec![
        ResidualInput::new(cursor(&output.operation_id.0), carried),
        ResidualInput::new(cursor(late_scan), carried),
        ResidualInput::new(cursor(mask_scan), on_output),
    ])
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
    let limits = iceberg::puffin::DeletionVectorLimits {
        max_blob_bytes: read_limits
            .row_group_uncompressed_bytes
            .min(policy.max_delete_input_bytes),
        max_footer_bytes: read_limits.footer_bytes,
        max_cardinality: policy.max_delete_input_rows,
    };
    let concurrency = read_limits.prefetch_files.max(1);
    let read_limits = flow_materializer::VectorReadLimits {
        max_footer_bytes: read_limits.footer_bytes,
        concurrency,
        max_batch_bytes: read_limits
            .prefetch_file_bytes
            .saturating_mul(concurrency as u64),
    };
    // A replacement read at one step is the original of a later one. Reuse
    // it instead of reading and charging the same vector twice.
    let mut replacements = HashMap::<String, DeleteVector>::new();
    for id in history.into_iter().rev() {
        let next = SnapshotView::load_with_cache(table, id, cache).await?;
        let mut required = output.plan.input_files.clone();
        required.extend(required_deletes(&prior, &output.plan.input_files)?);
        let mut pairs = Vec::new();
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
            // A rewrite that removes the target data file also retires its DV.
            // `dv:` file IDs sort before `s3://` data paths, so the DV comes first.
            ensure!(
                next.live_files.contains_key(&target),
                CatchUpRejected("selected input or delete was removed")
            );
            let replacement = next.applicable_deletes(&target)?;
            let after = replacement
                .first()
                .filter(|entry| entry.file_format() == iceberg::spec::DataFileFormat::Puffin)
                .ok_or(CatchUpRejected("cumulative deletion vector was removed"))?;
            pairs.push((path.0, before.as_ref(), *after));
        }
        let mut reads = Vec::new();
        let mut slots = Vec::new();
        for (pair, (path, before, after)) in pairs.iter().enumerate() {
            for (side, entry) in [(0, *before), (1, *after)] {
                if side == 0 && replacements.contains_key(path) {
                    continue;
                }
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
                reads.push(flow_materializer::VectorRead::new(file, limits)?);
                slots.push((pair, side));
            }
        }
        let mut vectors: Vec<[Option<DeleteVector>; 2]> =
            pairs.iter().map(|_| [None, None]).collect();
        for (pair, (path, _, _)) in pairs.iter().enumerate() {
            vectors[pair][0] = replacements.remove(path);
        }
        flow_materializer::read_vectors(table.file_io(), &reads, read_limits, |index, vector| {
            let (pair, side) = slots[index];
            vectors[pair][side] = Some(vector);
            Ok(())
        })
        .await?;
        for ((_, _, after), [before, replacement]) in pairs.iter().zip(vectors) {
            let (Some(before), Some(replacement)) = (before, replacement) else {
                anyhow::bail!("deletion vector history read is incomplete");
            };
            let mut newer = replacement.iter().peekable();
            for position in before.iter() {
                while newer.peek().is_some_and(|next| *next < position) {
                    newer.next();
                }
                ensure!(
                    newer.peek() == Some(&position),
                    CatchUpRejected("replacement DV resurrects an input row")
                );
            }
            replacements.insert(
                flow_iceberg_ext::content_file_id(&after.data_file),
                replacement,
            );
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

    /// Residual spools in the shape of a typical catch-up: positions carried
    /// forward to data files outside the rewrite dominate; others target the
    /// inputs, a file removed before the head, and this rewrite's output.
    struct ResidualFixture {
        _directory: tempfile::TempDir,
        table: Table,
        live: BTreeSet<FileId>,
        outputs: Vec<WorkerOutput>,
    }

    /// 78,000 carried positions, 2,000 late ones (660 repeat an original
    /// position) and 500 masks; 6,300 more target inputs or a removed file.
    const RESIDUAL_ROWS: u64 = 78_000 + 2_000 - 660;
    const MASK_ROWS: u64 = 500;

    async fn residual_fixture(outputs: usize, per_target: bool) -> ResidualFixture {
        use flow_state_store::StateStoreOptions;
        use iceberg::{
            Catalog, CatalogBuilder, NamespaceIdent, TableCreation,
            memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder},
            spec::{
                DataFileBuilder, DataFileFormat, NestedField, PrimitiveType, Schema, Struct, Type,
            },
        };
        let directory = tempfile::TempDir::new().unwrap();
        let catalog = MemoryCatalogBuilder::default()
            .load(
                "residual",
                HashMap::from([(MEMORY_CATALOG_WAREHOUSE.into(), "memory://residual".into())]),
            )
            .await
            .unwrap();
        let namespace = NamespaceIdent::new("bench".into());
        catalog
            .create_namespace(&namespace, HashMap::new())
            .await
            .unwrap();
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            ])
            .build()
            .unwrap();
        let table = catalog
            .create_table(
                &namespace,
                TableCreation::builder()
                    .name("orders".into())
                    .schema(schema)
                    .format_version(iceberg::spec::FormatVersion::V2)
                    .build(),
            )
            .await
            .unwrap();
        let path = |name: &str, file: usize| {
            FileId(format!(
                "s3://flow-bench/warehouse/cdc.db/orders/data/{name}-{file:05}-0-8d4f3c2a-91b7-4e6d-a0c5-{file:012}-00001.parquet"
            ))
        };
        // 20 carried targets, 3 inputs and one file removed before the head.
        let carried: Vec<_> = (0..20).map(|file| path("ingest", file)).collect();
        let inputs: BTreeSet<_> = (20..23).map(|file| path("ingest", file)).collect();
        let removed = path("ingest", 23);
        let mut live: BTreeSet<_> = carried.iter().cloned().collect();
        live.extend(inputs.iter().cloned());
        let location = |file: &FileId, position: u64| RowLocation {
            data_file_id: file.clone(),
            row_position: position,
            data_sequence_number: 3,
            spec_id: 0,
            partition: Vec::new(),
            source_commit_lsn: PgLsn(0),
            row_version: 0,
            row_fingerprint: [0; 16],
        };
        let mut built = Vec::new();
        for ordinal in 0..outputs {
            let operation = OperationId(format!("residual-{ordinal}"));
            // Each build has its own scratch store, small enough to stay in
            // its memtable.
            let scratch = StateStore::open(
                directory.path().join(&operation.0),
                StateStoreOptions {
                    apply_batch_rows: 8192,
                    ..StateStoreOptions::scratch()
                },
            )
            .unwrap();
            let stage = |scan: &str, positions: Vec<RowLocation>| {
                for batch in positions.chunks(8192) {
                    scratch
                        .put_position_deletes(scan, &TableId(1), batch.iter().cloned())
                        .unwrap();
                }
            };
            let output_file = path(&format!("{}-out", operation.0), 0);
            let mut original = Vec::new();
            for file in &carried {
                original.extend((0..3_900).map(|row| location(file, row * 3)));
            }
            for file in &inputs {
                original.extend((0..2_000).map(|row| location(file, row * 2)));
            }
            original.extend((0..1_000).map(|row| location(&removed, row)));
            stage(&operation.0, original);
            let mut late = Vec::new();
            for file in carried.iter().chain(&inputs) {
                late.extend((0..100).map(|row| location(file, row * 5 + 1)));
            }
            stage(&format!("{}-late", operation.0), late);
            stage(
                &format!("{}-output", operation.0),
                (0..MASK_ROWS)
                    .map(|row| location(&output_file, row * 7))
                    .collect(),
            );
            let data = DataFileBuilder::default()
                .content(DataContentType::Data)
                .file_path(output_file.0.clone())
                .file_format(DataFileFormat::Parquet)
                .partition(Struct::empty())
                .partition_spec_id(0)
                .file_size_in_bytes(1 << 20)
                .record_count(10_000)
                .build()
                .unwrap();
            built.push(WorkerOutput {
                plan: crate::CompactionPlan {
                    base_snapshot_id: 1,
                    schema_id: 0,
                    input_files: inputs.clone(),
                    delete_files: BTreeSet::new(),
                    spec_id: 0,
                    partition: Vec::new(),
                    output_level: crate::Level::L1,
                    target_file_bytes: 128 << 20,
                    input_bytes: 0,
                    delete_input_bytes: 0,
                    delete_input_rows: 200_000,
                    lazy_index_remap: false,
                    per_target_deletes: per_target,
                    target_ranges: crate::TargetRanges {
                        pooled: BTreeSet::new(),
                        breaks: live.clone(),
                        full_size_bytes: 1,
                    },
                    reason: crate::PlanReason::Promotion,
                },
                data_files: vec![data],
                delete_files: Vec::new(),
                delete_sequence: Some(3),
                residual_delete_rows: 0,
                operation_id: operation,
                scratch: scratch.clone(),
                masked_inputs: None,
                table_id: TableId(1),
                lazy_files: None,
                lazy: None,
            });
        }
        ResidualFixture {
            _directory: directory,
            table,
            live,
            outputs: built,
        }
    }

    async fn write_fixture_residuals(
        table: &Table,
        output: &WorkerOutput,
        live: &BTreeSet<FileId>,
    ) -> (Vec<iceberg::spec::DataFile>, u64) {
        let late = format!("{}-late", output.operation_id.0);
        let mask = format!("{}-output", output.operation_id.0);
        crate::worker::write_residual_positions(
            table,
            output,
            residual_positions(output, &late, &mask, live, false),
            WriterConfig {
                batch_rows: 8192,
                ..WriterConfig::default()
            },
        )
        .await
        .unwrap()
    }

    async fn residual_digest(table: &Table, files: &[iceberg::spec::DataFile]) -> String {
        let mut hasher = blake3::Hasher::new();
        for file in files {
            let bytes = table
                .file_io()
                .new_input(file.file_path())
                .unwrap()
                .read()
                .await
                .unwrap();
            hasher.update(file.file_path().as_bytes());
            hasher.update(&file.record_count().to_be_bytes());
            hasher.update(&bytes);
        }
        hasher.finalize().to_hex().to_string()
    }

    /// Times the residual merge and its Parquet write over a scratch spool.
    /// Run with `cargo test --release -p flow-compactor --lib
    /// residual_merge_benchmark -- --ignored --nocapture`.
    #[test]
    #[ignore = "focused timing workload; run explicitly with --release --ignored --nocapture"]
    fn residual_merge_benchmark() {
        let iterations = std::env::var("RESIDUAL_ITERATIONS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(15);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        for per_target in [false, true] {
            let fixture = runtime.block_on(residual_fixture(iterations + 1, per_target));
            let mut samples = Vec::new();
            let mut merges = Vec::new();
            let mut digest = None;
            for output in &fixture.outputs {
                let started = Instant::now();
                let (files, residual) = runtime.block_on(write_fixture_residuals(
                    &fixture.table,
                    output,
                    &fixture.live,
                ));
                let elapsed = started.elapsed().as_secs_f64() * 1000.0;
                let rows: u64 = files.iter().map(|file| file.record_count()).sum();
                assert_eq!((rows, residual), (RESIDUAL_ROWS + MASK_ROWS, RESIDUAL_ROWS));
                // The merge alone, without encoding or writing.
                let started = Instant::now();
                let late = format!("{}-late", output.operation_id.0);
                let mask = format!("{}-output", output.operation_id.0);
                let mut merge = residual_positions(output, &late, &mask, &fixture.live, false);
                let mut slot = crate::residual::empty_location();
                let mut merged = 0u64;
                while merge.next_into(&mut slot).unwrap().is_some() {
                    merged += 1;
                }
                let merge_elapsed = started.elapsed().as_secs_f64() * 1000.0;
                assert_eq!(merged, RESIDUAL_ROWS + MASK_ROWS);
                if digest.is_none() {
                    // The first run warms up; its digest identifies the bytes.
                    digest = Some(runtime.block_on(residual_digest(&fixture.table, &files)));
                    continue;
                }
                samples.push(elapsed);
                merges.push(merge_elapsed);
            }
            samples.sort_by(f64::total_cmp);
            merges.sort_by(f64::total_cmp);
            println!(
                "residual_merge per_target={per_target} rows={} runs={} median_ms={:.2} min_ms={:.2} max_ms={:.2} merge_only_median_ms={:.2} digest={}",
                RESIDUAL_ROWS + MASK_ROWS,
                samples.len(),
                samples[samples.len() / 2],
                samples[0],
                samples[samples.len() - 1],
                merges[merges.len() / 2],
                digest.expect("one run")
            );
        }
    }
}
