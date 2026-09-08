use crate::{Error, Result};
use flow_model::{FileId, PrimaryKey, RowLocation, TableId};
use flow_state_store::StateStore;
use std::collections::BTreeSet;

/// Match a keyless rewrite against the live row multiset. Equal fingerprints
/// are paired in physical order, retaining every original synthetic identity.
/// Both sides are sorted on disk; memory is limited to one state-store batch.
///
/// Use fresh scratch scan names and pass the returned rows to `reconcile_rewrite`
/// before publishing again. Its prepared transition supplies the durable fence,
/// cardinality checks, output uniqueness, and index compare-and-swap. The caller
/// may discard both scratch scans once that transition has finished.
pub fn match_append_only_rows<'a>(
    store: &StateStore,
    scratch: &'a StateStore,
    scans: (&str, &str),
    table: TableId,
    removed_files: &BTreeSet<FileId>,
    added_rows: impl IntoIterator<Item = flow_state_store::Result<(PrimaryKey, RowLocation)>>,
) -> Result<impl Iterator<Item = flow_state_store::Result<(PrimaryKey, RowLocation)>> + 'a> {
    let (before_scan, after_scan) = scans;
    if before_scan == after_scan {
        return Err(Error::Invalid("multiset scratch scans must be distinct"));
    }
    let mut batch = Vec::with_capacity(scratch.batch_rows());
    for file in removed_files {
        for row in store.file_rows(&table, file) {
            let (position, key) = row?;
            let location = store
                .lookup(&table, &key)?
                .ok_or(Error::LogicalMutation("reverse index has no live key"))?;
            if &location.data_file_id != file || location.row_position != position {
                return Err(Error::LogicalMutation("forward and reverse index disagree"));
            }
            batch.push((key, location));
            if batch.len() == scratch.batch_rows() {
                scratch.put_reconcile_rows(before_scan, &table, batch.drain(..))?;
            }
        }
    }
    scratch.put_reconcile_rows(before_scan, &table, batch.drain(..))?;
    let mut added_count = 0_u64;
    for row in added_rows {
        batch.push(row?);
        added_count = added_count
            .checked_add(1)
            .ok_or(Error::Invalid("row count overflow"))?;
        if batch.len() == scratch.batch_rows() {
            scratch.put_reconcile_rows(after_scan, &table, batch.drain(..))?;
        }
    }
    scratch.put_reconcile_rows(after_scan, &table, batch)?;

    let mut before = scratch.reconcile_rows(before_scan, &table);
    let mut after = scratch.reconcile_rows(after_scan, &table);
    let mut matched = 0_u64;
    let mut finished = false;
    Ok(std::iter::from_fn(move || {
        if finished {
            return None;
        }
        let next = (|| match (before.next().transpose()?, after.next().transpose()?) {
            (Some((identity, expected)), Some((_, replacement)))
                if expected.row_fingerprint == replacement.row_fingerprint =>
            {
                matched += 1;
                Ok(Some((identity, replacement)))
            }
            (None, None) if matched == added_count => Ok(None),
            _ => Err(flow_state_store::Error::InvalidState(
                "external rewrite changed the live row multiset or duplicated an output location"
                    .into(),
            )),
        })();
        match next {
            Ok(Some(row)) => Some(Ok(row)),
            Ok(None) => {
                finished = true;
                None
            }
            Err(error) => {
                finished = true;
                Some(Err(error))
            }
        }
    }))
}
