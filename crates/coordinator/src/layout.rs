//! How well a table's live data files follow its declared sort order, from
//! catalog metadata alone: no file is opened and the row index is not read.

use crate::maintenance::file_level;
use flow_compactor::Level;
use flow_iceberg_ext::SnapshotView;
use iceberg::spec::{DataContentType, Datum};
use iceberg::table::Table;
use std::cmp::Ordering;

/// The sort-related shape of one snapshot's live data files.
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize)]
pub struct SortLayout {
    /// The table's default sort order: 0 when unsorted.
    pub sort_order_id: i64,
    pub data_files: usize,
    pub data_rows: u64,
    pub data_bytes: u64,
    /// Files whose `sort_order_id` is the current order.
    pub sorted_files: usize,
    pub sorted_rows: u64,
    pub sorted_bytes: u64,
    /// Bytes outside L0 not stamped with the current order.
    pub unsorted_bytes: u64,
    /// Data files by level: L0, L1, L2.
    pub level_files: [usize; 3],
    /// Overlap of file ranges on the leading sort column (the first
    /// identifier field when the table is unsorted).
    pub overlap: Option<Overlap>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct Overlap {
    pub field_id: i32,
    /// Row-weighted mean number of files whose range overlaps a file's range,
    /// that file included. A point lookup opens at most this many files.
    pub mean_files: f64,
    /// The most file ranges that share one key.
    pub max_files: usize,
    /// Row-weighted share of rows in the files overlapping a file: an upper
    /// bound on the rows a point lookup reads when only file bounds prune.
    pub lookup_rows_ratio: f64,
    /// Files without bounds for the column, such as long strings.
    pub files_without_bounds: usize,
}

impl SortLayout {
    pub fn sorted_rows_ratio(&self) -> f64 {
        if self.data_rows == 0 {
            return 1.0;
        }
        self.sorted_rows as f64 / self.data_rows as f64
    }
}

/// Summarize `view`, the live files of `table`'s current snapshot.
pub fn sort_layout(table: &Table, view: &SnapshotView) -> SortLayout {
    let metadata = table.metadata();
    let order = metadata.default_sort_order();
    let order_id = order.order_id;
    let sorted_id = (!order.fields.is_empty())
        .then(|| i32::try_from(order_id).ok())
        .flatten();
    let field_id = order
        .fields
        .first()
        .map(|field| field.source_id)
        .or_else(|| metadata.current_schema().identifier_field_ids().next());
    let data_prefix = format!("{}/data/", metadata.location().trim_end_matches('/'));
    let mut layout = SortLayout {
        sort_order_id: order_id,
        ..SortLayout::default()
    };
    let mut ranges = Vec::new();
    let mut without_bounds = 0;
    for entry in view
        .live_files
        .values()
        .filter(|entry| entry.content_type() == DataContentType::Data)
    {
        let file = entry.data_file();
        let snapshot = entry.snapshot_id.and_then(|id| metadata.snapshot_by_id(id));
        let summary = snapshot.map(|snapshot| &snapshot.summary().additional_properties);
        let level = file_level(&data_prefix, file.file_path(), summary);
        layout.data_files += 1;
        layout.data_rows += file.record_count();
        layout.data_bytes += file.file_size_in_bytes();
        layout.level_files[match level {
            Level::L0 => 0,
            Level::L1 => 1,
            Level::L2 => 2,
        }] += 1;
        if sorted_id.is_some() && file.sort_order_id() == sorted_id {
            layout.sorted_files += 1;
            layout.sorted_rows += file.record_count();
            layout.sorted_bytes += file.file_size_in_bytes();
        } else if level != Level::L0 {
            layout.unsorted_bytes += file.file_size_in_bytes();
        }
        if let Some(field_id) = field_id {
            match (
                file.lower_bounds().get(&field_id),
                file.upper_bounds().get(&field_id),
            ) {
                (Some(lower), Some(upper)) => {
                    ranges.push((lower.clone(), upper.clone(), file.record_count()));
                }
                _ => without_bounds += 1,
            }
        }
    }
    layout.overlap = field_id.map(|field_id| overlap(field_id, ranges, without_bounds));
    layout
}

fn compare(left: &Datum, right: &Datum) -> Ordering {
    left.partial_cmp(right).unwrap_or(Ordering::Equal)
}

/// Overlap of closed ranges. A range overlaps every range that does not end
/// before it starts or start after it ends, so two sorted endpoint lists give
/// each count in O(log n).
fn overlap(field_id: i32, ranges: Vec<(Datum, Datum, u64)>, without_bounds: usize) -> Overlap {
    let mut starts: Vec<&Datum> = ranges.iter().map(|range| &range.0).collect();
    let mut ends: Vec<&Datum> = ranges.iter().map(|range| &range.1).collect();
    starts.sort_by(|left, right| compare(left, right));
    ends.sort_by(|left, right| compare(left, right));
    let total_rows: u64 = ranges.iter().map(|range| range.2).sum();
    let (mut weighted_files, mut weighted_rows) = (0f64, 0f64);
    // Rows in ranges ordered by start and by end, for the lookup bound.
    let mut start_rows: Vec<(&Datum, u64)> =
        ranges.iter().map(|range| (&range.0, range.2)).collect();
    start_rows.sort_by(|left, right| compare(left.0, right.0));
    let mut end_rows: Vec<(&Datum, u64)> = ranges.iter().map(|range| (&range.1, range.2)).collect();
    end_rows.sort_by(|left, right| compare(left.0, right.0));
    let prefix = |rows: &[(&Datum, u64)]| {
        let mut sums = Vec::with_capacity(rows.len() + 1);
        sums.push(0u64);
        for (_, count) in rows {
            sums.push(sums.last().copied().unwrap_or(0) + count);
        }
        sums
    };
    let start_sums = prefix(&start_rows);
    let end_sums = prefix(&end_rows);
    for (lower, upper, rows) in &ranges {
        // Ranges starting after this one ends, and ranges ending before it starts.
        let after = starts.len()
            - starts.partition_point(|start| compare(start, upper) != Ordering::Greater);
        let before = ends.partition_point(|end| compare(end, lower) == Ordering::Less);
        let files = ranges.len() - after - before;
        let after_rows = total_rows
            - start_sums[start_rows
                .partition_point(|(start, _)| compare(start, upper) != Ordering::Greater)];
        let before_rows =
            end_sums[end_rows.partition_point(|(end, _)| compare(end, lower) == Ordering::Less)];
        let weight = *rows as f64;
        weighted_files += weight * files as f64;
        weighted_rows += weight * (total_rows - after_rows - before_rows) as f64;
    }
    // The deepest point is where the most ranges have started and not ended.
    let mut max_files = 0;
    let mut open = 0usize;
    let mut ends_sorted = ends.iter().peekable();
    for start in &starts {
        while ends_sorted
            .peek()
            .is_some_and(|end| compare(end, start) == Ordering::Less)
        {
            ends_sorted.next();
            open -= 1;
        }
        open += 1;
        max_files = max_files.max(open);
    }
    let denominator = total_rows.max(1) as f64;
    Overlap {
        field_id,
        mean_files: weighted_files / denominator,
        max_files,
        lookup_rows_ratio: weighted_rows / (denominator * denominator),
        files_without_bounds: without_bounds,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ranges(bounds: &[(i64, i64, u64)]) -> Vec<(Datum, Datum, u64)> {
        bounds
            .iter()
            .map(|(lower, upper, rows)| (Datum::long(*lower), Datum::long(*upper), *rows))
            .collect()
    }

    #[test]
    fn disjoint_ranges_overlap_only_themselves() {
        let overlap = overlap(1, ranges(&[(0, 9, 10), (10, 19, 10), (20, 29, 10)]), 0);
        assert_eq!(overlap.mean_files, 1.0);
        assert_eq!(overlap.max_files, 1);
        assert!((overlap.lookup_rows_ratio - 1.0 / 3.0).abs() < 1e-9);
    }

    #[test]
    fn nested_and_touching_ranges_count_as_overlapping() {
        // A spans everything; B and C touch at 10.
        let overlap = overlap(1, ranges(&[(0, 100, 20), (0, 10, 10), (10, 20, 10)]), 2);
        assert_eq!(overlap.max_files, 3);
        // A overlaps all three; B and C overlap all three as well (touching at 10).
        assert_eq!(overlap.mean_files, 3.0);
        assert_eq!(overlap.lookup_rows_ratio, 1.0);
        assert_eq!(overlap.files_without_bounds, 2);
    }

    #[test]
    fn partially_overlapping_ranges() {
        let overlap = overlap(1, ranges(&[(0, 10, 10), (5, 15, 10), (20, 30, 20)]), 0);
        assert_eq!(overlap.max_files, 2);
        // Rows of the first two files see two files; the third sees itself.
        assert!((overlap.mean_files - (10.0 * 2.0 + 10.0 * 2.0 + 20.0) / 40.0).abs() < 1e-9);
        assert!(
            (overlap.lookup_rows_ratio - (10.0 * 20.0 + 10.0 * 20.0 + 20.0 * 20.0) / 1600.0).abs()
                < 1e-9
        );
    }
}
