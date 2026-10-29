//! Sorted union of staged position-delete spools, read without decoding an
//! owned record per position.

use anyhow::{Result, ensure};
use flow_model::{FileId, PgLsn, RowLocation};
use flow_state_store::PositionDeleteCursor;

/// One staged spool and the target files it contributes.
pub(crate) struct ResidualInput<'a> {
    cursor: PositionDeleteCursor<'a>,
    keep: Box<dyn Fn(&FileId) -> bool + Send + 'a>,
    /// Whether `keep` accepted the cursor's current target.
    accepted: bool,
    /// The cursor's position while it is on the current target.
    at: Option<u64>,
}

impl<'a> ResidualInput<'a> {
    pub(crate) fn new(
        cursor: PositionDeleteCursor<'a>,
        keep: impl Fn(&FileId) -> bool + Send + 'a,
    ) -> Self {
        Self {
            cursor,
            keep: Box::new(keep),
            accepted: false,
            at: None,
        }
    }
}

/// Merges spools that are each ordered and unique by physical position, using
/// bytewise path order followed by the ordinal. Positions arrive grouped by
/// target file, so each filter runs once per target and a rejected target is
/// skipped with one seek. Paths are compared once per target, ordinals per
/// position. An equal position in several spools keeps the first one's record.
pub(crate) struct ResidualMerge<'a> {
    inputs: Vec<ResidualInput<'a>>,
    /// The current target and its encoded key part, shared by its positions.
    target: FileId,
    key: Vec<u8>,
    scratch: FileId,
}

impl<'a> ResidualMerge<'a> {
    pub(crate) fn new(inputs: Vec<ResidualInput<'a>>) -> Self {
        Self {
            inputs,
            target: FileId(String::new()),
            key: Vec::new(),
            scratch: FileId(String::new()),
        }
    }

    /// Overwrite `slot` with the next position, reusing its allocations; the
    /// row identity fields, which a delete file does not record, are zero.
    /// Returns whether the position starts a new target, or `None` at the end.
    pub(crate) fn next_into(&mut self, slot: &mut RowLocation) -> Result<Option<bool>> {
        let mut starts = false;
        let (index, position) = loop {
            let next = self
                .inputs
                .iter()
                .enumerate()
                .filter_map(|(index, input)| input.at.map(|position| (index, position)))
                // The first input wins ties.
                .min_by_key(|&(_, position)| position);
            if let Some(next) = next {
                break next;
            }
            if !self.start_target()? {
                return Ok(None);
            }
            starts = true;
        };
        let record = self.inputs[index].cursor.record()?;
        ensure!(
            record.row_position == position,
            "staged position-delete key and record disagree"
        );
        slot.data_file_id.0.clone_from(&self.target.0);
        slot.row_position = position;
        slot.data_sequence_number = record.data_sequence_number;
        slot.spec_id = record.spec_id;
        slot.partition.clear();
        slot.partition.extend_from_slice(record.partition);
        // Delete files record no row identity; leave it undecoded.
        slot.source_commit_lsn = PgLsn(0);
        slot.row_version = 0;
        slot.row_fingerprint = [0; 16];
        for input in &mut self.inputs {
            if input.at == Some(position) {
                input.cursor.advance();
                input.at = match input.cursor.current()? {
                    Some((key, next)) if key == self.key.as_slice() => Some(next),
                    _ => {
                        input.accepted = false;
                        None
                    }
                };
            }
        }
        Ok(Some(starts))
    }

    /// Position every input on its next accepted target and select the
    /// smallest one. Returns false when every input is exhausted.
    fn start_target(&mut self) -> Result<bool> {
        for input in &mut self.inputs {
            while !input.accepted {
                if input.cursor.current()?.is_none() {
                    break;
                }
                self.scratch.0.clear();
                self.scratch.0.push_str(input.cursor.record()?.path()?);
                if (input.keep)(&self.scratch) {
                    input.accepted = true;
                } else {
                    input.cursor.skip_target();
                }
            }
        }
        let mut first = None::<(usize, &[u8])>;
        for (index, input) in self.inputs.iter().enumerate() {
            if let Some((key, _)) = input.cursor.current()?
                && first.is_none_or(|(_, smallest)| key < smallest)
            {
                first = Some((index, key));
            }
        }
        let Some((first, key)) = first else {
            return Ok(false);
        };
        self.key.clear();
        self.key.extend_from_slice(key);
        self.target.0.clear();
        self.target
            .0
            .push_str(self.inputs[first].cursor.record()?.path()?);
        for input in &mut self.inputs {
            input.at = match input.cursor.current()? {
                Some((key, position)) if key == self.key.as_slice() => Some(position),
                _ => None,
            };
        }
        Ok(true)
    }
}

/// A slot for [`ResidualMerge::next_into`].
pub(crate) fn empty_location() -> RowLocation {
    RowLocation {
        data_file_id: FileId(String::new()),
        row_position: 0,
        data_sequence_number: 0,
        spec_id: 0,
        partition: Vec::new(),
        source_commit_lsn: PgLsn(0),
        row_version: 0,
        row_fingerprint: [0; 16],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flow_model::TableId;
    use flow_state_store::{StateStore, StateStoreOptions};
    use std::collections::BTreeMap;

    fn location(path: &str, position: u64, version: u64) -> RowLocation {
        RowLocation {
            data_file_id: FileId(path.into()),
            row_position: position,
            data_sequence_number: version as i64,
            spec_id: version as i32,
            partition: vec![version as u8; version as usize % 3],
            source_commit_lsn: PgLsn(0),
            row_version: 0,
            row_fingerprint: [0; 16],
        }
    }

    #[test]
    fn merge_filters_each_target_and_keeps_the_first_equal_position() {
        let directory = tempfile::tempdir().unwrap();
        let store = StateStore::open(directory.path(), StateStoreOptions::default()).unwrap();
        let table = TableId(1);
        // Each spool's records carry its ordinal as their version.
        let spools: [&[(&str, u64)]; 3] = [
            &[
                ("a", 1),
                ("a", 4),
                ("a\0", 2),
                ("ab", 0),
                ("b", 3),
                ("c", 9),
                ("e", 1),
            ],
            &[("a", 4), ("a", 5), ("a\0", 2), ("b", 1), ("b", 3), ("d", 0)],
            &[("a", 0), ("a", 4), ("ab", 7), ("c", 9), ("d", 0), ("d", 8)],
        ];
        for (ordinal, positions) in spools.iter().enumerate() {
            store
                .put_position_deletes(
                    &format!("spool-{ordinal}"),
                    &table,
                    positions
                        .iter()
                        .map(|(path, position)| location(path, *position, ordinal as u64)),
                )
                .unwrap();
        }
        let rejected: [&[&str]; 3] = [&["ab", "c", "e"], &["ab", "c", "a"], &[]];
        let mut expected = BTreeMap::new();
        for (ordinal, positions) in spools.iter().enumerate() {
            for (path, position) in positions.iter() {
                if !rejected[ordinal].contains(path) {
                    expected
                        .entry((path.to_string(), *position))
                        .or_insert_with(|| location(path, *position, ordinal as u64));
                }
            }
        }
        let mut merge = ResidualMerge::new(
            rejected
                .iter()
                .enumerate()
                .map(|(ordinal, rejected)| {
                    ResidualInput::new(
                        store.position_delete_cursor(&format!("spool-{ordinal}"), &table),
                        move |file: &FileId| !rejected.contains(&file.0.as_str()),
                    )
                })
                .collect(),
        );
        let mut slot = location("stale-allocation", 99, 2);
        let mut merged = Vec::new();
        let mut starts = Vec::new();
        while let Some(start) = merge.next_into(&mut slot).unwrap() {
            if start {
                starts.push(slot.data_file_id.0.clone());
            }
            merged.push(slot.clone());
        }
        assert_eq!(merged, expected.into_values().collect::<Vec<_>>());
        assert_eq!(starts, ["a", "a\0", "ab", "b", "c", "d"]);
        assert!(merge.next_into(&mut slot).unwrap().is_none());
    }
}
