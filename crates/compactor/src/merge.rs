//! k-way merge of sorted rewrite inputs.
//!
//! Every input is sorted in the table's order (its writer verified and stamped
//! it); the merge checks that again for every row it reads. Output rows are in
//! that order, ties broken by input index, which callers give in commit order,
//! and within an input by position. Each input's rows keep their relative
//! order, so the rewrite's lazy remap stays valid; only output positions
//! interleave.
//!
//! The merge picks the input with the smallest head with a loser tree, then
//! gallops: it takes the longest run of that input that sorts at or before the
//! runner-up's head, found by exponential then binary search. Disjoint and
//! clustered inputs therefore move whole slices, and fully interleaved inputs
//! one row per step.

use crate::live::LiveColumns;
use anyhow::{Context, Result};
use arrow_array::{Array, ArrayRef, RecordBatch};
use arrow_row::{OwnedRow, RowConverter, Rows, SortField};
use arrow_schema::SortOptions;
use flow_materializer::{RowLineage, SortSpec};
use flow_model::{FileId, TableSchema};
use std::cmp::Ordering;

/// An input's rows were not in the order its sort-order stamp claims.
#[derive(Debug, thiserror::Error)]
#[error("rewrite input {} is not in the table's sort order", file.0)]
pub struct InputOutOfOrder {
    pub file: FileId,
}

/// Memcomparable sort keys of the table's sort columns.
pub(crate) struct SortKeys {
    converter: RowConverter,
    columns: Vec<usize>,
}

impl SortKeys {
    pub(crate) fn new(schema: &TableSchema, spec: &SortSpec) -> Result<Self> {
        let arrow = flow_materializer::writer_arrow_schema(schema);
        let fields = spec
            .keys
            .iter()
            .map(|key| {
                let field = arrow
                    .fields()
                    .get(key.column)
                    .context("sort column is outside the table schema")?;
                Ok(SortField::new_with_options(
                    field.data_type().clone(),
                    SortOptions {
                        descending: key.descending,
                        nulls_first: key.nulls_first,
                    },
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            converter: RowConverter::new(fields)?,
            columns: spec.keys.iter().map(|key| key.column).collect(),
        })
    }

    pub(crate) fn encode(&self, batch: &RecordBatch) -> Result<Rows> {
        let columns: Vec<ArrayRef> = self
            .columns
            .iter()
            .map(|column| batch.column(*column).clone())
            .collect();
        Ok(self.converter.convert_columns(&columns)?)
    }
}

/// One input batch handed to the merge, with each live row's fingerprint.
pub(crate) struct MergeBatch {
    pub(crate) live: LiveColumns,
    pub(crate) fingerprints: Vec<[u8; 16]>,
}

struct Cursor {
    batch: MergeBatch,
    keys: Rows,
    offset: usize,
}

impl Cursor {
    fn len(&self) -> usize {
        self.batch.live.batch.num_rows()
    }
}

/// A merged output batch.
pub(crate) struct Merged {
    pub(crate) batch: RecordBatch,
    pub(crate) lineage: Option<Vec<RowLineage>>,
    pub(crate) fingerprints: Vec<[u8; 16]>,
    /// Each output row's input index and its position in that input file.
    pub(crate) sources: Vec<(usize, u64)>,
}

/// Counters of one merge.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct MergeStats {
    pub(crate) rows: u64,
    /// Contiguous runs taken from one input; rows / runs is the mean run.
    pub(crate) runs: u64,
    /// Output batches assembled by slicing one input instead of gathering.
    pub(crate) sliced_batches: u64,
    pub(crate) gathered_batches: u64,
}

/// The merge's state. The caller loads a batch for every input that
/// [`Merge::pending`] names, or marks it finished, then takes output with
/// [`Merge::next`], which stops when any input needs its next batch.
pub(crate) struct Merge {
    keys: SortKeys,
    files: Vec<FileId>,
    cursors: Vec<Option<Cursor>>,
    finished: Vec<bool>,
    /// The last key of each input's previous batch, to check order across batches.
    last: Vec<Option<OwnedRow>>,
    /// Loser tree: `tree[0]` is the winner, `tree[1..k]` the losers of each
    /// internal node; leaf `i` sits at virtual node `k + i`.
    tree: Vec<usize>,
    dirty: bool,
    stats: MergeStats,
}

impl Merge {
    /// `files` are the inputs in commit order.
    pub(crate) fn new(keys: SortKeys, files: Vec<FileId>) -> Self {
        let inputs = files.len();
        Self {
            keys,
            files,
            cursors: (0..inputs).map(|_| None).collect(),
            finished: vec![false; inputs],
            last: (0..inputs).map(|_| None).collect(),
            tree: vec![0; inputs.max(1)],
            dirty: true,
            stats: MergeStats::default(),
        }
    }

    pub(crate) fn stats(&self) -> MergeStats {
        self.stats
    }

    /// An input that needs its next batch before the merge can continue.
    pub(crate) fn pending(&self) -> Option<usize> {
        (0..self.files.len()).find(|&input| self.cursors[input].is_none() && !self.finished[input])
    }

    /// The input has no more rows.
    pub(crate) fn finish(&mut self, input: usize) {
        self.finished[input] = true;
        self.dirty = true;
    }

    /// Load `input`'s next batch, checking that its keys never decrease.
    pub(crate) fn push(&mut self, input: usize, batch: MergeBatch) -> Result<()> {
        let rows = batch.live.batch.num_rows();
        anyhow::ensure!(
            rows > 0 && batch.fingerprints.len() == rows && batch.live.positions.len() == rows,
            "merge batch is empty or its row metadata is incomplete"
        );
        let keys = self.keys.encode(&batch.live.batch)?;
        let ordered = self.last[input]
            .as_ref()
            .is_none_or(|last| last.row() <= keys.row(0))
            && (1..rows).all(|row| keys.row(row - 1) <= keys.row(row));
        if !ordered {
            return Err(InputOutOfOrder {
                file: self.files[input].clone(),
            }
            .into());
        }
        self.last[input] = Some(keys.row(rows - 1).owned());
        self.cursors[input] = Some(Cursor {
            batch,
            keys,
            offset: 0,
        });
        self.dirty = true;
        Ok(())
    }

    /// Whether `a`'s head sorts before `b`'s. An input without a head sorts
    /// last; equal keys go to the lower input index.
    fn less(&self, a: usize, b: usize) -> bool {
        match (&self.cursors[a], &self.cursors[b]) {
            (None, _) => false,
            (Some(_), None) => true,
            (Some(left), Some(right)) => {
                match left
                    .keys
                    .row(left.offset)
                    .cmp(&right.keys.row(right.offset))
                {
                    Ordering::Less => true,
                    Ordering::Greater => false,
                    Ordering::Equal => a < b,
                }
            }
        }
    }

    fn rebuild(&mut self) {
        let inputs = self.files.len();
        if inputs <= 1 {
            self.tree[0] = 0;
            return;
        }
        let mut winners = vec![0usize; inputs];
        for node in (1..inputs).rev() {
            let winner = |child: usize, winners: &[usize]| {
                if child >= inputs {
                    child - inputs
                } else {
                    winners[child]
                }
            };
            let (left, right) = (winner(2 * node, &winners), winner(2 * node + 1, &winners));
            let (won, lost) = if self.less(right, left) {
                (right, left)
            } else {
                (left, right)
            };
            self.tree[node] = lost;
            winners[node] = won;
        }
        self.tree[0] = winners[1];
    }

    /// Replay the path of `input`, whose head changed, to the root.
    fn replay(&mut self, input: usize) {
        let inputs = self.files.len();
        let mut winner = input;
        let mut node = (input + inputs) / 2;
        while node >= 1 {
            if self.less(self.tree[node], winner) {
                std::mem::swap(&mut self.tree[node], &mut winner);
            }
            node /= 2;
        }
        self.tree[0] = winner;
    }

    /// The smallest head other than the winner's: one of the losers on the
    /// winner's path.
    fn runner_up(&self, winner: usize) -> Option<usize> {
        let inputs = self.files.len();
        let mut best = None::<usize>;
        let mut node = (winner + inputs) / 2;
        while node >= 1 {
            let candidate = self.tree[node];
            if self.cursors[candidate].is_some()
                && best.is_none_or(|best| self.less(candidate, best))
            {
                best = Some(candidate);
            }
            node /= 2;
        }
        best
    }

    /// How many rows from the winner's offset sort at or before `bound`'s head
    /// (strictly before when `bound` wins ties), by exponential then binary
    /// search. At least one: the winner's head qualifies.
    fn run_length(&self, winner: usize, bound: Option<usize>) -> usize {
        let cursor = self.cursors[winner].as_ref().expect("winner has a head");
        let (start, len) = (cursor.offset, cursor.len());
        let Some(bound) = bound else {
            return len - start;
        };
        let other = self.cursors[bound].as_ref().expect("runner-up has a head");
        let limit = other.keys.row(other.offset);
        let inclusive = winner < bound;
        let fits = |row: usize| match cursor.keys.row(row).cmp(&limit) {
            Ordering::Less => true,
            Ordering::Equal => inclusive,
            Ordering::Greater => false,
        };
        // `low` fits; find the first row after it that does not.
        let (mut low, mut step) = (start, 1usize);
        let mut high = loop {
            let probe = start.saturating_add(step).min(len);
            if probe >= len || !fits(probe) {
                break probe;
            }
            low = probe;
            step = step.saturating_mul(2);
        };
        while high - low > 1 {
            let middle = low + (high - low) / 2;
            if fits(middle) {
                low = middle;
            } else {
                high = middle;
            }
        }
        high = low + 1;
        high - start
    }

    /// Up to `max_rows` merged rows, or `None` when every input is finished.
    /// Stops early when an input runs out of its current batch; load it
    /// ([`Self::pending`]) before calling again.
    pub(crate) fn next(&mut self, max_rows: usize) -> Result<Option<Merged>> {
        anyhow::ensure!(self.pending().is_none(), "merge input needs its next batch");
        if self.cursors.iter().all(Option::is_none) {
            return Ok(None);
        }
        if self.dirty {
            self.rebuild();
            self.dirty = false;
        }
        let mut picks: Vec<(usize, usize, usize)> = Vec::new(); // (input, start, len)
        let mut rows = 0usize;
        let mut exhausted = None;
        while rows < max_rows {
            let winner = self.tree[0];
            if self.cursors[winner].is_none() {
                break;
            }
            let bound = self.runner_up(winner);
            let take = self.run_length(winner, bound).min(max_rows - rows);
            let cursor = self.cursors[winner].as_mut().expect("winner has a head");
            picks.push((winner, cursor.offset, take));
            cursor.offset += take;
            rows += take;
            self.stats.runs += 1;
            if cursor.offset == cursor.len() {
                exhausted = Some(winner);
                break;
            }
            self.replay(winner);
        }
        let merged = self.assemble(&picks, rows)?;
        if let Some(input) = exhausted {
            // Its batch is no longer referenced; the caller loads the next.
            self.cursors[input] = None;
            self.dirty = true;
        }
        self.stats.rows += rows as u64;
        Ok(Some(merged))
    }

    fn assemble(&mut self, picks: &[(usize, usize, usize)], rows: usize) -> Result<Merged> {
        let single = picks.iter().all(|pick| pick.0 == picks[0].0);
        let cursor = |input: usize| {
            self.cursors[input]
                .as_ref()
                .expect("picked input has a batch")
        };
        let mut sources = Vec::with_capacity(rows);
        let mut fingerprints = Vec::with_capacity(rows);
        let has_lineage = cursor(picks[0].0).batch.live.lineage.is_some();
        let mut lineage = has_lineage.then(|| Vec::with_capacity(rows));
        for &(input, start, len) in picks {
            let live = &cursor(input).batch;
            sources.extend(
                live.live.positions[start..start + len]
                    .iter()
                    .map(|position| (input, *position)),
            );
            fingerprints.extend_from_slice(&live.fingerprints[start..start + len]);
            if let Some(lineage) = &mut lineage {
                let values = live
                    .live
                    .lineage
                    .as_ref()
                    .context("inputs disagree on row lineage")?;
                lineage.extend_from_slice(&values[start..start + len]);
            }
        }
        let batch = if single {
            // Consecutive picks of one input are contiguous: slice, no copy.
            let (input, start, _) = picks[0];
            self.stats.sliced_batches += 1;
            cursor(input).batch.live.batch.slice(start, rows)
        } else {
            self.stats.gathered_batches += 1;
            // Gather from the batches the picks reference, in pick order.
            let mut slots: Vec<usize> = Vec::new();
            let mut slot_of = vec![usize::MAX; self.files.len()];
            for &(input, ..) in picks {
                if slot_of[input] == usize::MAX {
                    slot_of[input] = slots.len();
                    slots.push(input);
                }
            }
            let mut indices = Vec::with_capacity(rows);
            for &(input, start, len) in picks {
                indices.extend((start..start + len).map(|row| (slot_of[input], row)));
            }
            let first = &cursor(slots[0]).batch.live.batch;
            let columns = (0..first.num_columns())
                .map(|column| {
                    let arrays: Vec<&dyn Array> = slots
                        .iter()
                        .map(|input| cursor(*input).batch.live.batch.column(column).as_ref())
                        .collect();
                    Ok(arrow_select::interleave::interleave(&arrays, &indices)?)
                })
                .collect::<Result<Vec<_>>>()?;
            RecordBatch::try_new(first.schema(), columns)?
        };
        Ok(Merged {
            batch,
            lineage,
            fingerprints,
            sources,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flow_materializer::{SortKey, rows_to_batch};
    use flow_model::{Column, ColumnType, Row, TableId, Value};

    fn schema() -> TableSchema {
        TableSchema {
            table_id: TableId(1),
            version: 0,
            columns: vec![
                Column {
                    field_id: 1,
                    name: "id".into(),
                    data_type: ColumnType::Int64,
                    nullable: false,
                },
                Column {
                    field_id: 2,
                    name: "group".into(),
                    data_type: ColumnType::String,
                    nullable: true,
                },
            ],
            primary_key: vec![0],
            append_only: false,
        }
    }

    fn spec(keys: &[(usize, bool, bool)]) -> SortSpec {
        SortSpec {
            order_id: 1,
            keys: keys
                .iter()
                .map(|&(column, descending, nulls_first)| SortKey {
                    column,
                    descending,
                    nulls_first,
                })
                .collect(),
        }
    }

    /// Batch of rows with consecutive positions from `first`.
    fn batch(file: &str, rows: &[Row], first: u64) -> MergeBatch {
        let positions: Vec<u64> = (first..first + rows.len() as u64).collect();
        MergeBatch {
            live: LiveColumns {
                file_id: FileId(file.into()),
                batch: rows_to_batch(&schema(), rows).unwrap(),
                positions: positions.clone(),
                lineage: None,
            },
            fingerprints: positions
                .iter()
                .map(|position| {
                    let mut print = [0u8; 16];
                    print[..8].copy_from_slice(&position.to_be_bytes());
                    print[8] = file.as_bytes()[0];
                    print
                })
                .collect(),
        }
    }

    /// Merge `inputs` (each a list of batches of rows), loading batches as asked.
    fn run(
        spec: &SortSpec,
        inputs: Vec<Vec<Vec<Row>>>,
        max_rows: usize,
    ) -> (Vec<(usize, u64, Row)>, MergeStats) {
        let files: Vec<FileId> = (0..inputs.len()).map(|i| FileId(format!("f{i}"))).collect();
        let mut merge = Merge::new(SortKeys::new(&schema(), spec).unwrap(), files);
        let mut queues: Vec<std::collections::VecDeque<Vec<Row>>> =
            inputs.into_iter().map(Into::into).collect();
        let mut next_position = vec![0u64; queues.len()];
        let mut out = Vec::new();
        loop {
            while let Some(input) = merge.pending() {
                match queues[input].pop_front() {
                    Some(rows) => {
                        let first = next_position[input];
                        next_position[input] += rows.len() as u64;
                        merge
                            .push(input, batch(&format!("f{input}"), &rows, first))
                            .unwrap();
                    }
                    None => merge.finish(input),
                }
            }
            let Some(merged) = merge.next(max_rows).unwrap() else {
                break;
            };
            assert!(merged.batch.num_rows() <= max_rows);
            let rows = flow_materializer::rows_from_batch(&schema(), &merged.batch).unwrap();
            assert_eq!(rows.len(), merged.sources.len());
            assert_eq!(rows.len(), merged.fingerprints.len());
            for ((source, row), print) in merged.sources.iter().zip(rows).zip(&merged.fingerprints)
            {
                // Fingerprints travel with their row.
                assert_eq!(u64::from_be_bytes(print[..8].try_into().unwrap()), source.1);
                assert_eq!(print[8], format!("f{}", source.0).as_bytes()[0]);
                out.push((source.0, source.1, row));
            }
        }
        (out, merge.stats())
    }

    fn rows(ids: &[i64]) -> Vec<Row> {
        ids.iter()
            .map(|id| vec![Value::Int64(*id), Value::String(format!("g{}", id % 3))])
            .collect()
    }

    /// Deterministic pseudo-random numbers.
    struct Lcg(u64);
    impl Lcg {
        fn next(&mut self) -> u64 {
            self.0 = self
                .0
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            self.0 >> 33
        }
    }

    #[test]
    fn merges_like_a_stable_sort_for_any_shape_and_batching() {
        let spec = spec(&[(0, false, true)]);
        for seed in 0..60u64 {
            let mut rng = Lcg(seed);
            let inputs = 1 + (seed % 9) as usize;
            // Each input: sorted keys from a shape that is disjoint, clustered
            // or fully interleaved, with duplicates across inputs.
            let shape = seed % 3;
            let mut all: Vec<Vec<i64>> = (0..inputs)
                .map(|input| {
                    let count = (rng.next() % 300) as i64;
                    let mut ids: Vec<i64> = (0..count)
                        .map(|row| match shape {
                            0 => input as i64 * 1_000 + row,
                            1 => input as i64 * 100 + (rng.next() % 400) as i64,
                            _ => (rng.next() % 1_000) as i64 - 500,
                        })
                        .collect();
                    ids.sort();
                    ids
                })
                .collect();
            let batched: Vec<Vec<Vec<Row>>> = all
                .iter()
                .map(|ids| {
                    let mut batches = Vec::new();
                    let mut rest = &ids[..];
                    while !rest.is_empty() {
                        let take = (1 + rng.next() % 64) as usize;
                        let (head, tail) = rest.split_at(take.min(rest.len()));
                        batches.push(rows(head));
                        rest = tail;
                    }
                    batches
                })
                .collect();
            let max_rows = 1 + (rng.next() % 100) as usize;
            let (out, stats) = run(&spec, batched, max_rows);
            // Expected: every row tagged with its input and position, sorted
            // by key, then input, then position (a stable sort of commit order).
            let mut expected: Vec<(i64, usize, u64)> = Vec::new();
            for (input, ids) in all.iter_mut().enumerate() {
                expected.extend(
                    ids.iter()
                        .enumerate()
                        .map(|(position, id)| (*id, input, position as u64)),
                );
            }
            expected.sort();
            let got: Vec<(i64, usize, u64)> = out
                .iter()
                .map(|(input, position, row)| match row[0] {
                    Value::Int64(id) => (id, *input, *position),
                    _ => unreachable!(),
                })
                .collect();
            assert_eq!(got, expected, "seed {seed}");
            assert_eq!(stats.rows, expected.len() as u64);
        }
    }

    /// The merge's memcomparable keys must order rows exactly as the row
    /// comparator that sorts epochs, which the writer's verifier also matches.
    #[test]
    fn row_keys_agree_with_the_epoch_row_comparator() {
        let types = [
            ColumnType::Int64,
            ColumnType::Float64,
            ColumnType::String,
            ColumnType::Decimal {
                precision: 12,
                scale: 2,
            },
            ColumnType::Uuid,
            ColumnType::TimestampTzMicros,
            ColumnType::Bool,
            ColumnType::Int32,
            ColumnType::Binary,
            ColumnType::Date,
            ColumnType::TimestampMicros,
        ];
        let schema = TableSchema {
            table_id: TableId(1),
            version: 0,
            columns: types
                .iter()
                .enumerate()
                .map(|(index, data_type)| Column {
                    field_id: index as i32 + 1,
                    name: format!("c{index}"),
                    data_type: data_type.clone(),
                    nullable: true,
                })
                .collect(),
            primary_key: vec![0],
            append_only: false,
        };
        let mut rng = Lcg(42);
        let mut value = |column: usize| {
            if rng.next().is_multiple_of(6) {
                return Value::Null;
            }
            let draw = rng.next();
            match column {
                0 => Value::Int64(
                    (draw % 7) as i64 - 3
                        + if draw.is_multiple_of(11) {
                            i64::MIN / 2
                        } else {
                            0
                        },
                ),
                1 => Value::Float64(
                    [
                        f64::NAN,
                        -f64::NAN,
                        -0.0,
                        0.0,
                        1.5,
                        -2.25,
                        f64::INFINITY,
                        f64::NEG_INFINITY,
                    ][(draw % 8) as usize],
                ),
                2 => {
                    Value::String(["", "a", "aa", "b", "é", "z", "ab"][(draw % 7) as usize].into())
                }
                3 => Value::Decimal {
                    unscaled: (draw % 9) as i128 - 4,
                    scale: 2,
                },
                4 => Value::Uuid([(draw % 3) as u8; 16]),
                5 => Value::TimestampTzMicros((draw % 5) as i64 - 2),
                6 => Value::Bool(draw.is_multiple_of(2)),
                7 => Value::Int32(
                    (draw % 7) as i32 - 3
                        + if draw.is_multiple_of(13) {
                            i32::MIN / 2
                        } else {
                            0
                        },
                ),
                8 => Value::Binary(
                    [
                        &b""[..],
                        b"\x00",
                        b"\x00\x00",
                        b"\x01",
                        b"\xff",
                        b"\x7f\xff",
                    ][(draw % 6) as usize]
                        .to_vec(),
                ),
                9 => Value::Date((draw % 5) as i32 - 2),
                _ => Value::TimestampMicros((draw % 5) as i64 - 2),
            }
        };
        let rows: Vec<Row> = (0..400)
            .map(|_| (0..types.len()).map(&mut value).collect())
            .collect();
        // LiveInput conforms decoded arrays to the writer schema before
        // passing them to SortKeys (notably Binary becomes LargeBinary).
        let batch = crate::columnar::conform(
            &schema,
            &flow_materializer::writer_arrow_schema(&schema),
            &rows_to_batch(&schema, &rows).unwrap(),
        )
        .unwrap();
        for (column, descending, nulls_first) in (0..types.len()).flat_map(|column| {
            [
                (column, false, true),
                (column, true, false),
                (column, true, true),
            ]
        }) {
            // Lead with one column, then break ties on the others.
            let mut keys = vec![(column, descending, nulls_first)];
            keys.extend(
                (0..types.len())
                    .filter(|other| *other != column)
                    .map(|other| (other, false, true)),
            );
            let spec = spec(&keys);
            let encoded = SortKeys::new(&schema, &spec)
                .unwrap()
                .encode(&batch)
                .unwrap();
            for left in 0..rows.len() {
                let right = (left * 7 + 3) % rows.len();
                assert_eq!(
                    encoded.row(left).cmp(&encoded.row(right)),
                    spec.compare_rows(&rows[left], &rows[right]),
                    "column {column} descending {descending} nulls_first {nulls_first}: {:?} vs {:?}",
                    rows[left],
                    rows[right]
                );
            }
        }
    }

    #[test]
    fn multi_column_descending_and_null_orders_follow_the_spec() {
        // group DESC NULLS FIRST, then id ASC.
        let spec = spec(&[(1, true, true), (0, false, true)]);
        let row = |id: i64, group: Option<&str>| {
            vec![
                Value::Int64(id),
                group.map_or(Value::Null, |group| Value::String(group.into())),
            ]
        };
        let a = vec![row(1, None), row(4, Some("b")), row(2, Some("a"))];
        let b = vec![row(3, None), row(0, Some("c")), row(5, Some("a"))];
        let (out, _) = run(&spec, vec![vec![a], vec![b]], 100);
        let ids: Vec<i64> = out
            .iter()
            .map(|(_, _, row)| match row[0] {
                Value::Int64(id) => id,
                _ => unreachable!(),
            })
            .collect();
        assert_eq!(ids, [1, 3, 0, 4, 2, 5]);
    }

    #[test]
    fn an_input_out_of_order_is_rejected_within_and_across_batches() {
        let spec = spec(&[(0, false, true)]);
        let keys = || SortKeys::new(&schema(), &spec).unwrap();
        let mut merge = Merge::new(keys(), vec![FileId("bad".into())]);
        let error = merge
            .push(0, batch("bad", &rows(&[1, 3, 2]), 0))
            .unwrap_err();
        assert_eq!(
            error.downcast_ref::<InputOutOfOrder>().unwrap().file,
            FileId("bad".into())
        );

        let mut merge = Merge::new(keys(), vec![FileId("bad".into())]);
        merge.push(0, batch("bad", &rows(&[1, 5]), 0)).unwrap();
        let first = merge.next(10).unwrap().unwrap();
        assert_eq!(first.batch.num_rows(), 2);
        assert!(merge.push(0, batch("bad", &rows(&[4]), 2)).is_err());
    }

    #[test]
    fn disjoint_inputs_take_one_run_per_batch_and_slice() {
        let spec = spec(&[(0, false, true)]);
        let inputs = vec![
            vec![rows(&(0..100).collect::<Vec<_>>())],
            vec![rows(&(100..200).collect::<Vec<_>>())],
        ];
        let (out, stats) = run(&spec, inputs, 1_000);
        assert_eq!(out.len(), 200);
        assert_eq!(stats.runs, 2);
        assert_eq!(stats.gathered_batches, 0);
    }
}
