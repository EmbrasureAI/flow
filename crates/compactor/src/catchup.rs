//! Correct a completed data build using an exact catalog and index head.
//! Delete replacements must retain every base delete on the selected inputs.

use crate::{
    BuiltData, DeleteReadLimits, Policy, ReadLimits, WorkerOutput,
    residual::{MemoryPositions, MemoryTarget, ResidualInput, ResidualMerge},
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
    let (head, replaced_position_deletes) =
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
    // Consolidation can replace both original and newly added delete files.
    // Read the complete head set in that case, then prove that the build did
    // not omit a row which became live again. The additive path reads only
    // new files and reuses the base spool.
    let late_deletes = if replaced_position_deletes {
        deletes.clone()
    } else {
        deletes
            .difference(&output.plan.delete_files)
            .cloned()
            .collect()
    };
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
    // A small replacement set can prove retention before staging only the
    // genuinely late positions. Charge 48 bytes per declared row for the
    // compact (file rank, position) vector, capacity growth and sort scratch.
    // Larger sets retain the disk-backed path. The physical reader enforces
    // the declared row budget too, so malformed metadata cannot bypass it.
    let compact_replacement = replaced_position_deletes
        && rows
            .checked_mul(48)
            .is_some_and(|bytes| bytes <= read_limits.batch_bytes);
    // A lazy build proves retention while translating late input deletes from
    // the same in-memory set, and writes residuals from it, without staging.
    let compact_lazy = compact_replacement && output.lazy_files.is_some();
    let mut compact_positions = None;
    let (scanned_late_delete_rows, mut base_delete_rows_skipped) = if compact_replacement {
        let (positions, scanned) = crate::read_delete_files(
            table,
            &head,
            &data,
            &late_deletes,
            batch_rows,
            DeleteReadLimits {
                max_bytes: policy.max_delete_input_bytes,
                max_rows: rows,
            },
            read_limits,
        )
        .await?;
        if compact_lazy {
            compact_positions = Some(positions);
            (scanned, 0)
        } else {
            let retained = stage_replacement_positions(
                &output,
                &head,
                positions.iter(),
                &late_scan,
                batch_rows,
                table.metadata().default_partition_spec_id(),
            )?;
            (scanned, retained)
        }
    } else {
        let scanned = stage_delete_files(
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
        if replaced_position_deletes {
            validate_retained_deletes(&output, &late_scan)?;
        }
        (scanned, 0)
    };
    let late_scan_finished = Instant::now();

    let lazy = output.lazy_files.take();
    let mut memory_masks = None;
    let (mapping, mapping_lookup, mask_spool, mapped_rows, accepted_rows, masked_rows) =
        if let Some(files) = lazy {
            let spool_started = Instant::now();
            let (lazy, mapped_rows, masked_rows) = if let Some(positions) = &compact_positions {
                let caught = compact_lazy_catch_up(
                    &output,
                    &head,
                    files,
                    positions,
                    live_counts,
                    sequence,
                    base.sequence_number,
                )?;
                base_delete_rows_skipped = caught.retained;
                memory_masks = Some((caught.masks, caught.outputs));
                (caught.lazy, caught.mapped_rows, caught.masked_rows)
            } else {
                lazy_catch_up(
                    &output,
                    files,
                    &late_scan,
                    &output_scan,
                    live_counts,
                    sequence,
                    base.sequence_number,
                    batch_rows,
                )?
            };
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
            // The worker scans inputs in commit order, not path order, so
            // late deletes are read per input file as its mappings begin.
            let mut late = None;
            let mut finished = BTreeSet::<FileId>::new();
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
                    // Each input's mappings are one contiguous run in position order.
                    match &mut previous {
                        Some((file, position)) if *file == old.data_file_id => {
                            ensure!(
                                *position < old.row_position,
                                "worker mappings are not in input order"
                            );
                            *position = old.row_position;
                        }
                        _ => {
                            if let Some((file, _)) = previous.take() {
                                finished.insert(file);
                            }
                            ensure!(
                                !finished.contains(&old.data_file_id),
                                "worker mappings are not in input order"
                            );
                            late = Some(
                                output
                                    .scratch
                                    .file_position_deletes(
                                        &late_scan,
                                        &schema.table_id,
                                        &old.data_file_id,
                                    )
                                    .peekable(),
                            );
                            previous = Some((old.data_file_id.clone(), old.row_position));
                        }
                    }
                    let late = late.as_mut().expect("opened for this mapping's file");
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
    let residuals = match (&compact_positions, &memory_masks) {
        (Some(positions), Some((masks, outputs))) => memory_residual_positions(
            &output,
            &head,
            positions,
            masks,
            outputs,
            &data,
            v3,
            table.metadata().default_partition_spec_id(),
        )?,
        _ => residual_positions(
            &output,
            &late_scan,
            &output_scan,
            &data,
            v3,
            !replaced_position_deletes,
        ),
    };
    (output.delete_files, output.residual_delete_rows) =
        crate::worker::write_residual_positions(table, &output, residuals, config).await?;
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
        replaced_position_deletes,
        compact_replacement,
        base_delete_rows_skipped,
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

/// Staged deletes on the selected inputs only, in path and position order,
/// borrowed from a spool instead of decoded as owned records. Other targets
/// are skipped with one seek each, and each path is read once.
struct InputDeletes<'a> {
    cursor: flow_state_store::PositionDeleteCursor<'a>,
    inputs: &'a BTreeSet<FileId>,
    /// The current accepted target, its encoded key and the position.
    path: FileId,
    key: Vec<u8>,
    position: Option<u64>,
    probe: FileId,
}

impl<'a> InputDeletes<'a> {
    fn new(
        cursor: flow_state_store::PositionDeleteCursor<'a>,
        inputs: &'a BTreeSet<FileId>,
    ) -> Result<Self> {
        let mut deletes = Self {
            cursor,
            inputs,
            path: FileId(String::new()),
            key: Vec::new(),
            position: None,
            probe: FileId(String::new()),
        };
        deletes.settle()?;
        Ok(deletes)
    }

    fn current(&self) -> Option<(&str, u64)> {
        self.position
            .map(|position| (self.path.0.as_str(), position))
    }

    fn advance(&mut self) -> Result<()> {
        self.cursor.advance();
        self.settle()
    }

    /// Stop on the cursor's record if it targets an input, else skip ahead.
    fn settle(&mut self) -> Result<()> {
        loop {
            let Some((key, position)) = self.cursor.current()? else {
                self.position = None;
                return Ok(());
            };
            if !self.key.is_empty() && key == self.key.as_slice() {
                self.position = Some(position);
                return Ok(());
            }
            self.probe.0.clear();
            self.probe.0.push_str(self.cursor.record()?.path()?);
            if self.inputs.contains(&self.probe) {
                self.key.clear();
                self.key.extend_from_slice(key);
                std::mem::swap(&mut self.path, &mut self.probe);
                self.position = Some(position);
                return Ok(());
            }
            self.cursor.skip_target();
        }
    }
}

fn resurrected() -> anyhow::Error {
    CatchUpRejected("replacement deletes resurrect an input row").into()
}

/// `positions` is the reader's sorted, unique effective head set. Prove that
/// every original delete on selected inputs is retained, and omit those base
/// holes from late-mask staging. Unselected targets keep all current deletes.
/// Scratch writes are private; no index deltas or residuals are accepted until
/// the entire proof succeeds.
fn stage_replacement_positions<'a>(
    output: &WorkerOutput,
    head: &SnapshotView,
    positions: impl Iterator<Item = (&'a FileId, u64)>,
    scan: &str,
    batch_rows: usize,
    spec_id: i32,
) -> Result<u64> {
    output.scratch.discard_transaction(scan)?;
    let mut originals = InputDeletes::new(
        output
            .scratch
            .position_delete_cursor(&output.operation_id.0, &output.table_id),
        &output.plan.input_files,
    )?;
    let mut retained = 0;
    let mut pending = Vec::with_capacity(batch_rows);
    // Positions arrive grouped by file; resolve each file's sequence once.
    let mut sequence = None::<(&FileId, i64)>;
    for (file, position) in positions {
        if let Some(order) = originals
            .current()
            .map(|base| base.cmp(&(file.0.as_str(), position)))
        {
            ensure!(order.is_ge(), resurrected());
            if order.is_eq() {
                originals.advance()?;
                retained += 1;
                continue;
            }
        }
        let data_sequence_number = match sequence {
            Some((cached, value)) if std::ptr::eq(cached, file) || cached == file => value,
            _ => {
                let value = head.live_files[&file.0]
                    .sequence_number
                    .ok_or_else(|| anyhow::anyhow!("missing data sequence"))?;
                sequence = Some((file, value));
                value
            }
        };
        pending.push(RowLocation {
            data_file_id: file.clone(),
            row_position: position,
            data_sequence_number,
            spec_id,
            partition: Vec::new(),
            source_commit_lsn: flow_model::PgLsn(0),
            row_version: 0,
            row_fingerprint: [0; 16],
        });
        if pending.len() == batch_rows {
            output
                .scratch
                .put_position_deletes(scan, &output.table_id, pending.drain(..))?;
        }
    }
    ensure!(originals.current().is_none(), resurrected());
    output
        .scratch
        .put_position_deletes(scan, &output.table_id, pending)?;
    Ok(retained)
}

/// Selected data files remain physically identical throughout the ancestry.
/// Their live rows at H must be a subset of the rows read at the build base.
/// Compare the already bounded, disk-backed effective sets; file identities
/// alone cannot prove this after a position-delete consolidation.
fn validate_retained_deletes(output: &WorkerOutput, head_scan: &str) -> Result<()> {
    let cursor = |scan: &str| {
        output
            .scratch
            .position_delete_cursor(scan, &output.table_id)
    };
    let inputs = &output.plan.input_files;
    let mut head = InputDeletes::new(cursor(head_scan), inputs)?;
    let mut originals = InputDeletes::new(cursor(&output.operation_id.0), inputs)?;
    loop {
        let order = match (originals.current(), head.current()) {
            (None, _) => return Ok(()),
            (Some(_), None) => return Err(resurrected()),
            (Some(original), Some(retained)) => retained.cmp(&original),
        };
        match order {
            std::cmp::Ordering::Less => head.advance()?,
            std::cmp::Ordering::Equal => {
                head.advance()?;
                originals.advance()?;
            }
            std::cmp::Ordering::Greater => return Err(resurrected()),
        }
    }
}

/// Per-input late deletes and per-output masks a lazy catch-up translated.
#[derive(Default)]
struct LateMasks {
    late_by_input: BTreeMap<FileId, u64>,
    masked_by_output: BTreeMap<FileId, u64>,
    masked_rows: u64,
}

impl LateMasks {
    /// Record one input's translated late deletes; `per_target` counts them
    /// by index into the remap's targets.
    fn add(&mut self, remap: &flow_state_store::FileRemap, per_target: &[u64]) {
        let late: u64 = per_target.iter().sum();
        if late == 0 {
            return;
        }
        *self.late_by_input.entry(remap.source.clone()).or_default() += late;
        for (target, masked) in remap.targets.iter().zip(per_target) {
            if *masked > 0 {
                *self
                    .masked_by_output
                    .entry(target.data_file_id.clone())
                    .or_default() += masked;
            }
        }
        self.masked_rows += late;
    }

    /// Prove, per input file, that the index at H holds exactly the base
    /// survivors that no late delete removed, and derive output live counts.
    fn finish(
        mut self,
        output: &WorkerOutput,
        files: Vec<flow_state_store::FileRemap>,
        live_counts: &BTreeMap<FileId, u64>,
    ) -> Result<(flow_state_store::LazyRemap, u64, u64)> {
        let survivors: BTreeMap<&FileId, u64> = files
            .iter()
            .map(|remap| (&remap.source, remap.survivors()))
            .collect();
        let mut mapped_rows = 0u64;
        for file in &output.plan.input_files {
            let survivors = survivors.get(file).copied().unwrap_or(0);
            mapped_rows += survivors;
            let late = self.late_by_input.get(file).copied().unwrap_or(0);
            ensure!(
                live_counts.get(file).copied() == Some(survivors - late),
                CatchUpRejected("current index coverage differs from replacement mappings")
            );
        }
        let mut output_live_rows = Vec::with_capacity(output.data_files.len());
        for file in &output.data_files {
            let id = FileId(file.file_path().to_owned());
            let masked = self.masked_by_output.remove(&id).unwrap_or(0);
            let live = file
                .record_count()
                .checked_sub(masked)
                .ok_or_else(|| anyhow::anyhow!("masked rows exceed an output file"))?;
            output_live_rows.push((id, live));
        }
        ensure!(
            self.masked_by_output.is_empty(),
            "lazy remap targets an unknown output file"
        );
        let masked_rows = self.masked_rows;
        Ok((
            flow_state_store::LazyRemap {
                files,
                output_live_rows,
            },
            mapped_rows,
            masked_rows,
        ))
    }
}

fn remaps_by_input<'a>(
    output: &WorkerOutput,
    files: &'a [flow_state_store::FileRemap],
) -> Result<BTreeMap<&'a FileId, &'a flow_state_store::FileRemap>> {
    let remaps: BTreeMap<_, _> = files.iter().map(|remap| (&remap.source, remap)).collect();
    ensure!(
        remaps
            .keys()
            .all(|file| output.plan.input_files.contains(*file)),
        "lazy remap names a file outside the plan"
    );
    Ok(remaps)
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
    let mut late = LateMasks::default();
    {
        let remaps = remaps_by_input(output, &files)?;
        let mut positions = Vec::with_capacity(batch_rows);
        // Late deletes arrive grouped by target in position order: look each
        // target up once, skip unselected ones with one seek, and translate
        // with one translator per input, which decodes each chunk once.
        let mut cursor = output
            .scratch
            .position_delete_cursor(late_scan, &output.table_id);
        let mut path = FileId(String::new());
        let mut key = Vec::new();
        while let Some((target, _)) = cursor.current()? {
            key.clear();
            key.extend_from_slice(target);
            path.0.clear();
            path.0.push_str(cursor.record()?.path()?);
            let Some(&remap) = remaps.get(&path) else {
                cursor.skip_target();
                continue;
            };
            let mut translator = remap.translator();
            let mut per_target = vec![0u64; remap.targets.len()];
            loop {
                let position = match cursor.current()? {
                    Some((target, position)) if target == key.as_slice() => position,
                    _ => break,
                };
                cursor.advance();
                // Positions already deleted at the base were never written.
                let Some((target, row)) = translator.translate_position(position)? else {
                    continue;
                };
                ensure!(
                    sequence.is_some_and(|sequence| sequence >= build_sequence),
                    CatchUpRejected("late delete cannot mask output at its build sequence")
                );
                per_target[target] += 1;
                positions.push(mask_location(&remap.targets[target], row));
                if positions.len() == batch_rows {
                    output.scratch.put_position_deletes(
                        output_scan,
                        &output.table_id,
                        positions.drain(..),
                    )?;
                }
            }
            late.add(remap, &per_target);
        }
        if !positions.is_empty() {
            output.scratch.put_position_deletes(
                output_scan,
                &output.table_id,
                positions.drain(..),
            )?;
        }
    }
    late.finish(output, files, live_counts)
}

/// A position-delete record masking an output row. Delete files record no
/// row identity, so those fields are zero, as in every staged delete.
fn mask_location(target: &flow_state_store::RemapTarget, row_position: u64) -> RowLocation {
    RowLocation {
        data_file_id: target.data_file_id.clone(),
        row_position,
        data_sequence_number: target.data_sequence_number,
        spec_id: target.spec_id,
        partition: target.partition.clone(),
        source_commit_lsn: flow_model::PgLsn(0),
        row_version: 0,
        row_fingerprint: [0; 16],
    }
}

/// What a compact lazy catch-up proved and translated.
struct CompactCatchUp {
    lazy: flow_state_store::LazyRemap,
    mapped_rows: u64,
    masked_rows: u64,
    /// Base deletes on the inputs that the head set retains.
    retained: u64,
    /// Output masks as `(rank in outputs, row)`, sorted and unique.
    masks: Vec<(u32, u64)>,
    /// The remaps' output targets, in path order.
    outputs: Vec<flow_state_store::RemapTarget>,
}

/// [`stage_replacement_positions`] and [`lazy_catch_up`] in one pass over the
/// compact head set, without staging either. A lazy remap records exactly the
/// rows the build wrote, so the positions of an input that no run covers are
/// exactly the build's original deletes. The head set must contain every one
/// of them, or a row the output omits would be live again; this proves the
/// same retention as comparing with the staged originals, without reading
/// them. Each other input position, a late delete, is translated into an
/// output mask. Masks take 16 bytes each and at most one per input position,
/// reserved exactly, so with the head set's vector they stay within the
/// compact path's 48-byte charge per declared position.
fn compact_lazy_catch_up(
    output: &WorkerOutput,
    head: &SnapshotView,
    files: Vec<flow_state_store::FileRemap>,
    positions: &crate::DeletePositions,
    live_counts: &BTreeMap<FileId, u64>,
    sequence: Option<i64>,
    build_sequence: i64,
) -> Result<CompactCatchUp> {
    let inputs = &output.plan.input_files;
    let mut late = LateMasks::default();
    let mut retained = 0u64;
    let mut masks = Vec::new();
    let outputs;
    {
        let remaps = remaps_by_input(output, &files)?;
        let mut by_path = BTreeMap::<&FileId, &flow_state_store::RemapTarget>::new();
        for remap in &files {
            for target in &remap.targets {
                by_path.entry(&target.data_file_id).or_insert(target);
            }
        }
        let ranks = by_path
            .keys()
            .enumerate()
            .map(|(rank, file)| Ok((*file, u32::try_from(rank)?)))
            .collect::<Result<HashMap<_, _>>>()?;
        outputs = by_path
            .into_values()
            .cloned()
            .collect::<Vec<flow_state_store::RemapTarget>>();
        let runs: HashMap<&FileId, &[(u32, u64)]> = positions
            .runs()
            .filter(|(file, _)| inputs.contains(*file))
            .collect();
        masks.reserve_exact(runs.values().map(|run| run.len()).sum());
        for file in inputs {
            let rows = head
                .live_files
                .get(&file.0)
                .ok_or_else(|| anyhow::anyhow!("selected input is not live at the head"))?
                .data_file
                .record_count();
            let run = runs.get(file).copied().unwrap_or(&[]);
            let remap = remaps.get(file).copied();
            let mut translator = remap.map(flow_state_store::FileRemap::translator);
            let target_ranks: Vec<u32> = remap.map_or_else(Vec::new, |remap| {
                remap
                    .targets
                    .iter()
                    .map(|target| ranks[&target.data_file_id])
                    .collect()
            });
            let mut per_target = vec![0u64; target_ranks.len()];
            let mut at = 0usize;
            // Head positions before `limit` inside covered ranges are late
            // deletes; translate each into a mask on its output row.
            let mut translate = |limit: u64, at: &mut usize| -> Result<()> {
                while let Some(&(_, position)) = run.get(*at)
                    && position < limit
                {
                    *at += 1;
                    let Some((target, row)) = translator
                        .as_mut()
                        .map(|translator| translator.translate_position(position))
                        .transpose()?
                        .flatten()
                    else {
                        // Beyond the file's rows; the reader rejects those.
                        continue;
                    };
                    per_target[target] += 1;
                    masks.push((target_ranks[target], row));
                }
                Ok(())
            };
            // The holes between covered ranges: every one must be deleted.
            let mut hole = |start: u64, end: u64, at: &mut usize| -> Result<()> {
                if start == end {
                    return Ok(());
                }
                ensure!(
                    start < end && end <= rows,
                    "lazy remap exceeds its input file"
                );
                let length = usize::try_from(end - start)?;
                // Sorted, unique positions hold all of `start..end` exactly
                // when the next `length` of them begin and end there.
                ensure!(
                    run.get(*at).is_some_and(|&(_, first)| first == start)
                        && run
                            .get(*at + length - 1)
                            .is_some_and(|&(_, last)| last == end - 1),
                    resurrected()
                );
                *at += length;
                retained += end - start;
                Ok(())
            };
            let mut covered_end = 0u64;
            if let Some(remap) = remap {
                remap.for_each_covered(|start, end| {
                    hole(covered_end, start, &mut at)?;
                    translate(end, &mut at)?;
                    covered_end = end;
                    Ok::<(), anyhow::Error>(())
                })?;
            }
            hole(covered_end, rows, &mut at)?;
            translate(u64::MAX, &mut at)?;
            if let Some(remap) = remap {
                late.add(remap, &per_target);
            }
        }
    }
    // Checked once the whole retention proof has passed, as when staging.
    ensure!(
        masks.is_empty() || sequence.is_some_and(|sequence| sequence >= build_sequence),
        CatchUpRejected("late delete cannot mask output at its build sequence")
    );
    masks.sort_unstable();
    let (lazy, mapped_rows, masked_rows) = late.finish(output, files, live_counts)?;
    Ok(CompactCatchUp {
        lazy,
        mapped_rows,
        masked_rows,
        retained,
        masks,
        outputs,
    })
}

/// [`residual_positions`] for a compact lazy catch-up: the head set's
/// positions on unselected targets and the output masks, both in memory.
#[allow(clippy::too_many_arguments)]
fn memory_residual_positions<'a>(
    output: &'a WorkerOutput,
    head: &SnapshotView,
    positions: &'a crate::DeletePositions,
    masks: &'a [(u32, u64)],
    outputs: &'a [flow_state_store::RemapTarget],
    live_data: &'a BTreeSet<FileId>,
    outputs_only: bool,
    spec_id: i32,
) -> Result<ResidualMerge<'a>> {
    let on_output = move |file: &FileId| {
        !outputs_only
            || output
                .data_files
                .iter()
                .any(|data| data.file_path() == file.0)
    };
    let carried = move |file: &FileId| {
        !output.plan.input_files.contains(file) && live_data.contains(file) && on_output(file)
    };
    // Staged head positions carry the head sequence, the default spec and no
    // partition; masks carry their output's.
    let carried_positions = MemoryPositions::new(positions.pairs(), |rank| {
        let path = positions.target(rank);
        Ok(MemoryTarget {
            path: path.clone(),
            data_sequence_number: head
                .live_files
                .get(&path.0)
                .and_then(|entry| entry.sequence_number)
                .ok_or_else(|| anyhow::anyhow!("missing data sequence"))?,
            spec_id,
            partition: Vec::new(),
        })
    })?;
    let mask_positions = MemoryPositions::new(masks, |rank| {
        let target = outputs
            .get(rank as usize)
            .ok_or_else(|| anyhow::anyhow!("mask targets an unknown output"))?;
        Ok(MemoryTarget {
            path: target.data_file_id.clone(),
            data_sequence_number: target.data_sequence_number,
            spec_id: target.spec_id,
            partition: target.partition.clone(),
        })
    })?;
    Ok(ResidualMerge::new(vec![
        ResidualInput::memory(carried_positions, carried),
        ResidualInput::memory(mask_positions, on_output),
    ]))
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
/// When original files were replaced, `late_scan` is the complete head set and
/// base residuals must be excluded: they may no longer apply to other targets.
fn residual_positions<'a>(
    output: &'a WorkerOutput,
    late_scan: &str,
    mask_scan: &str,
    live_data: &'a BTreeSet<FileId>,
    outputs_only: bool,
    retain_base: bool,
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
    let mut inputs = vec![
        ResidualInput::new(cursor(late_scan), carried),
        ResidualInput::new(cursor(mask_scan), on_output),
    ];
    if retain_base {
        inputs.push(ResidualInput::new(cursor(&output.operation_id.0), carried));
    }
    ResidualMerge::new(inputs)
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
) -> Result<(SnapshotView, bool)> {
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
    let mut replaced_position_deletes = false;
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
            if before.content_type() == DataContentType::PositionDeletes
                && before.file_format() == iceberg::spec::DataFileFormat::Parquet
            {
                // Do not trust the snapshot's operation label or a replacement's
                // metadata. Catch-up checks effective base positions against the
                // complete head set before accepting any mappings or residuals.
                replaced_position_deletes = true;
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
    Ok((prior, replaced_position_deletes))
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
                    merge: crate::MergePolicy::default(),
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
                layout: crate::RewriteLayout::CommitOrder,
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
            residual_positions(output, &late, &mask, live, false, true),
            WriterConfig {
                batch_rows: 8192,
                ..WriterConfig::default()
            },
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn replaced_delete_residuals_use_only_the_head_set_and_output_masks() {
        let fixture = residual_fixture(1, false).await;
        let output = &fixture.outputs[0];
        let head = format!("{}-late", output.operation_id.0);
        let mask = format!("{}-output", output.operation_id.0);
        let expected: BTreeSet<_> = [head.as_str(), mask.as_str()]
            .into_iter()
            .flat_map(|scan| output.scratch.position_deletes(scan, &output.table_id))
            .map(|position| position.unwrap())
            .filter(|position| !output.plan.input_files.contains(&position.data_file_id))
            .map(|position| (position.data_file_id, position.row_position))
            .collect();
        let mut actual = BTreeSet::new();
        let mut residuals = residual_positions(output, &head, &mask, &fixture.live, false, false);
        let mut position = crate::residual::empty_location();
        while residuals.next_into(&mut position).unwrap().is_some() {
            actual.insert((position.data_file_id.clone(), position.row_position));
        }
        assert_eq!(actual, expected);
        assert_eq!(actual.len(), 2_000 + MASK_ROWS as usize);
    }

    #[tokio::test]
    async fn compact_replacement_proof_skips_base_holes_and_matches_disk_proof() {
        use iceberg::spec::{
            DataFileBuilder, DataFileFormat, ManifestEntry, ManifestStatus, Struct,
        };
        let fixture = residual_fixture(1, false).await;
        let output = &fixture.outputs[0];
        let mut view = SnapshotView::default();
        for file in &fixture.live {
            let data = DataFileBuilder::default()
                .content(DataContentType::Data)
                .file_path(file.0.clone())
                .file_format(DataFileFormat::Parquet)
                .partition(Struct::empty())
                .partition_spec_id(0)
                .file_size_in_bytes(1 << 20)
                .record_count(20_000)
                .build()
                .unwrap();
            view.live_files.insert(
                file.0.clone(),
                std::sync::Arc::new(
                    ManifestEntry::builder()
                        .status(ManifestStatus::Added)
                        .sequence_number(3)
                        .file_sequence_number(3)
                        .data_file(data)
                        .build(),
                ),
            );
        }
        let base: BTreeSet<_> = output
            .scratch
            .position_deletes(&output.operation_id.0, &output.table_id)
            .map(|position| position.unwrap())
            .filter(|position| output.plan.input_files.contains(&position.data_file_id))
            .map(|position| (position.data_file_id, position.row_position))
            .collect();
        let mut head = base.clone();
        head.extend(
            output
                .scratch
                .position_deletes(&format!("{}-late", output.operation_id.0), &output.table_id)
                .map(|position| position.unwrap())
                .map(|position| (position.data_file_id, position.row_position)),
        );
        assert_eq!(
            stage_replacement_positions(
                output,
                &view,
                head.iter().map(|(file, row)| (file, *row)),
                "compact-proof",
                128,
                0,
            )
            .unwrap(),
            6_000,
        );
        let staged: BTreeSet<_> = output
            .scratch
            .position_deletes("compact-proof", &output.table_id)
            .map(|position| position.unwrap())
            .map(|position| (position.data_file_id, position.row_position))
            .collect();
        assert_eq!(staged, head.difference(&base).cloned().collect());
        let stage_head = |positions: &BTreeSet<(FileId, u64)>| {
            output.scratch.discard_transaction("disk-proof").unwrap();
            let rows: Vec<_> = positions
                .iter()
                .map(|(file, row)| RowLocation {
                    data_file_id: file.clone(),
                    row_position: *row,
                    data_sequence_number: 3,
                    spec_id: 0,
                    partition: Vec::new(),
                    source_commit_lsn: PgLsn(0),
                    row_version: 0,
                    row_fingerprint: [0; 16],
                })
                .collect();
            for batch in rows.chunks(128) {
                output
                    .scratch
                    .put_position_deletes("disk-proof", &output.table_id, batch.iter().cloned())
                    .unwrap();
            }
        };
        stage_head(&head);
        validate_retained_deletes(output, "disk-proof").unwrap();
        for missing in [
            base.first().unwrap(),
            base.iter().nth(3_000).unwrap(),
            base.last().unwrap(),
        ] {
            let mut invalid = head.clone();
            invalid.remove(missing);
            stage_head(&invalid);
            assert!(
                validate_retained_deletes(output, "disk-proof")
                    .unwrap_err()
                    .is::<CatchUpRejected>()
            );
            assert!(
                stage_replacement_positions(
                    output,
                    &view,
                    invalid.iter().map(|(file, row)| (file, *row)),
                    "compact-proof",
                    128,
                    0,
                )
                .unwrap_err()
                .is::<CatchUpRejected>()
            );
        }
    }

    /// Every `(path, row)` in written delete files, with `output` renamed.
    async fn delete_contents(
        table: &Table,
        files: &[iceberg::spec::DataFile],
        output: &str,
    ) -> Vec<(String, i64)> {
        use arrow_array::{Int64Array, StringArray};
        let mut rows = Vec::new();
        for file in files {
            let bytes = table
                .file_io()
                .new_input(file.file_path())
                .unwrap()
                .read()
                .await
                .unwrap();
            let reader =
                parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(bytes)
                    .unwrap()
                    .build()
                    .unwrap();
            for batch in reader {
                let batch = batch.unwrap();
                let paths = batch
                    .column_by_name("file_path")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                let positions = batch
                    .column_by_name("pos")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                for row in 0..batch.num_rows() {
                    let path = paths.value(row);
                    let path = if path == output { "OUTPUT" } else { path };
                    rows.push((path.to_owned(), positions.value(row)));
                }
            }
        }
        rows
    }

    /// Lazy remaps over the fixture's three inputs, interleaved like a merge,
    /// the head set (every original delete on live files plus late ones),
    /// and the live counts at that head.
    struct LazyHead {
        remaps: Vec<flow_state_store::FileRemap>,
        head: BTreeSet<(FileId, u64)>,
        view: SnapshotView,
        counts: BTreeMap<FileId, u64>,
    }

    const INPUT_ROWS: u64 = 6_000;
    const BLOCK: std::ops::Range<u64> = 5_000..5_100;

    fn lazy_head(fixture: &ResidualFixture, output: &WorkerOutput) -> LazyHead {
        use iceberg::spec::{
            DataFileBuilder, DataFileFormat, ManifestEntry, ManifestStatus, Struct,
        };
        // Each input also lost a contiguous block at the base, so the
        // remap leaves multi-row holes as well as single ones.
        for file in &output.plan.input_files {
            output
                .scratch
                .put_position_deletes(
                    &output.operation_id.0,
                    &output.table_id,
                    BLOCK.map(|row| RowLocation {
                        data_file_id: file.clone(),
                        row_position: row,
                        data_sequence_number: 3,
                        spec_id: 0,
                        partition: Vec::new(),
                        source_commit_lsn: PgLsn(0),
                        row_version: 0,
                        row_fingerprint: [0; 16],
                    }),
                )
                .unwrap();
        }
        let spool = |scan: &str| -> BTreeSet<(FileId, u64)> {
            output
                .scratch
                .position_deletes(scan, &output.table_id)
                .map(|position| position.unwrap())
                .filter(|position| fixture.live.contains(&position.data_file_id))
                .map(|position| (position.data_file_id, position.row_position))
                .collect()
        };
        let base = spool(&output.operation_id.0);
        let mut head = base.clone();
        head.extend(spool(&format!("{}-late", output.operation_id.0)));
        let target = output.data_files[0].file_path().to_owned();
        let inputs: Vec<_> = output.plan.input_files.iter().collect();
        let mut remaps = Vec::new();
        let mut counts = BTreeMap::new();
        for (index, file) in inputs.iter().enumerate() {
            let mut remap = flow_state_store::FileRemap::new((*file).clone());
            let mut survivors = 0u64;
            let mut live = 0u64;
            for row in 0..INPUT_ROWS {
                if base.contains(&((*file).clone(), row)) {
                    continue;
                }
                // Interleave the inputs, one row each, like a full merge.
                let replacement = RowLocation {
                    data_file_id: FileId(target.clone()),
                    row_position: survivors * inputs.len() as u64 + index as u64,
                    data_sequence_number: 3,
                    spec_id: 0,
                    partition: Vec::new(),
                    source_commit_lsn: PgLsn(0),
                    row_version: 0,
                    row_fingerprint: [0; 16],
                };
                remap.push(row, &replacement, 4).unwrap();
                survivors += 1;
                live += u64::from(!head.contains(&((*file).clone(), row)));
            }
            remaps.push(remap);
            counts.insert((*file).clone(), live);
        }
        let mut view = SnapshotView::default();
        for file in &fixture.live {
            let data = DataFileBuilder::default()
                .content(DataContentType::Data)
                .file_path(file.0.clone())
                .file_format(DataFileFormat::Parquet)
                .partition(Struct::empty())
                .partition_spec_id(0)
                .file_size_in_bytes(1 << 20)
                .record_count(INPUT_ROWS)
                .build()
                .unwrap();
            view.live_files.insert(
                file.0.clone(),
                std::sync::Arc::new(
                    ManifestEntry::builder()
                        .status(ManifestStatus::Added)
                        .sequence_number(3)
                        .file_sequence_number(3)
                        .data_file(data)
                        .build(),
                ),
            );
        }
        LazyHead {
            remaps,
            head,
            view,
            counts,
        }
    }

    fn delete_positions(head: &BTreeSet<(FileId, u64)>) -> crate::DeletePositions {
        let targets: Vec<FileId> = head
            .iter()
            .map(|(file, _)| file.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let positions = head
            .iter()
            .map(|(file, row)| (targets.binary_search(file).unwrap() as u32, *row))
            .collect();
        crate::DeletePositions::from_sorted(targets, positions)
    }

    /// The in-memory compact path must prove, translate and write exactly
    /// what staging the head set, translating the late spool and merging the
    /// spools does, for remaps that interleave their inputs like a merge.
    #[tokio::test]
    async fn compact_lazy_catch_up_matches_the_staged_path() {
        let fixture = residual_fixture(2, false).await;
        let config = WriterConfig {
            batch_rows: 8192,
            ..WriterConfig::default()
        };
        // The staged path, as for a large replacement or a non-lazy build.
        let staged = &fixture.outputs[0];
        let lazy = lazy_head(&fixture, staged);
        let retained = stage_replacement_positions(
            staged,
            &lazy.view,
            lazy.head.iter().map(|(file, row)| (file, *row)),
            "equal-late",
            128,
            0,
        )
        .unwrap();
        let (staged_remap, staged_mapped, staged_masked) = lazy_catch_up(
            staged,
            lazy.remaps,
            "equal-late",
            "equal-output",
            &lazy.counts,
            Some(4),
            3,
            128,
        )
        .unwrap();
        let (staged_files, staged_residual) = crate::worker::write_residual_positions(
            &fixture.table,
            staged,
            residual_positions(
                staged,
                "equal-late",
                "equal-output",
                &fixture.live,
                false,
                false,
            ),
            config.clone(),
        )
        .await
        .unwrap();

        let compact = &fixture.outputs[1];
        let lazy = lazy_head(&fixture, compact);
        let positions = delete_positions(&lazy.head);
        let caught = compact_lazy_catch_up(
            compact,
            &lazy.view,
            lazy.remaps,
            &positions,
            &lazy.counts,
            Some(4),
            3,
        )
        .unwrap();
        let (compact_files, compact_residual) = crate::worker::write_residual_positions(
            &fixture.table,
            compact,
            memory_residual_positions(
                compact,
                &lazy.view,
                &positions,
                &caught.masks,
                &caught.outputs,
                &fixture.live,
                false,
                0,
            )
            .unwrap(),
            config,
        )
        .await
        .unwrap();

        assert_eq!(caught.retained, retained);
        assert!(retained > 0);
        assert_eq!(
            (caught.mapped_rows, caught.masked_rows),
            (staged_mapped, staged_masked)
        );
        assert!(staged_masked > 0);
        let remaps = |remap: &flow_state_store::LazyRemap| {
            remap
                .files
                .iter()
                .map(|file| (file.source.clone(), file.survivors(), file.runs()))
                .collect::<Vec<_>>()
        };
        assert_eq!(remaps(&caught.lazy), remaps(&staged_remap));
        let live_rows = |remap: &flow_state_store::LazyRemap| {
            remap
                .output_live_rows
                .iter()
                .map(|(_, rows)| *rows)
                .collect::<Vec<_>>()
        };
        assert_eq!(live_rows(&caught.lazy), live_rows(&staged_remap));
        assert_eq!(compact_residual, staged_residual);
        let staged_rows = delete_contents(
            &fixture.table,
            &staged_files,
            staged.data_files[0].file_path(),
        )
        .await;
        let compact_rows = delete_contents(
            &fixture.table,
            &compact_files,
            compact.data_files[0].file_path(),
        )
        .await;
        assert_eq!(compact_rows, staged_rows);
        assert_eq!(
            compact_rows
                .iter()
                .filter(|(path, _)| path == "OUTPUT")
                .count() as u64,
            staged_masked
        );
        assert_eq!(
            compact_files
                .iter()
                .map(|file| file.record_count())
                .collect::<Vec<_>>(),
            staged_files
                .iter()
                .map(|file| file.record_count())
                .collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn compact_lazy_catch_up_rejects_resurrection_and_stale_masks() {
        let fixture = residual_fixture(1, false).await;
        let output = &fixture.outputs[0];
        let lazy = lazy_head(&fixture, output);
        let inputs = &output.plan.input_files;
        let base: Vec<_> = lazy
            .head
            .iter()
            .filter(|(file, row)| inputs.contains(file) && row % 2 == 0 && *row < 4_000)
            .cloned()
            .collect();
        let block = |row: u64| (inputs.iter().nth(1).unwrap().clone(), row);
        for missing in [
            base.first(),
            base.get(base.len() / 2),
            base.last(),
            Some(&block(BLOCK.start)),
            Some(&block(BLOCK.start + 50)),
            Some(&block(BLOCK.end - 1)),
        ] {
            let mut head = lazy.head.clone();
            head.remove(missing.unwrap());
            let error = compact_lazy_catch_up(
                output,
                &lazy.view,
                lazy.remaps.clone(),
                &delete_positions(&head),
                &lazy.counts,
                Some(4),
                3,
            )
            .err()
            .unwrap();
            assert!(error.to_string().contains("resurrect"), "{error:#}");
        }
        // Without its late deletes, an input loses nothing at the build's
        // own sequence; with them, an older delete sequence cannot mask.
        let error = compact_lazy_catch_up(
            output,
            &lazy.view,
            lazy.remaps.clone(),
            &delete_positions(&lazy.head),
            &lazy.counts,
            Some(2),
            3,
        )
        .err()
        .unwrap();
        assert!(error.to_string().contains("build sequence"), "{error:#}");
        let mut counts = lazy.counts.clone();
        *counts.values_mut().next().unwrap() += 1;
        let error = compact_lazy_catch_up(
            output,
            &lazy.view,
            lazy.remaps,
            &delete_positions(&lazy.head),
            &counts,
            Some(4),
            3,
        )
        .err()
        .unwrap();
        assert!(error.is::<CatchUpRejected>(), "{error:#}");
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
                let mut merge =
                    residual_positions(output, &late, &mask, &fixture.live, false, true);
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
