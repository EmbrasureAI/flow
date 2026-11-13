//! Inputs that a merge found out of order despite their sort-order stamp.

use crate::publication::ReplanRequired;
use flow_compactor::{CompactionPlan, InputOutOfOrder};
use flow_model::TableId;
use std::collections::HashSet;
use std::sync::{Arc, Mutex};

/// Enough for every file of several large tables; past it the set restarts,
/// and a still-present bad file is found again by its next merge.
const MAX_DEMOTED_FILES: usize = 4096;

/// Data files whose writer stamped them with the table's sort order but whose
/// rows a merge found out of order. A plan that includes one concatenates
/// instead of merging while the bounded process-local set remembers it.
/// Its rewrite's output is
/// verified and stamped only if sorted, so the file leaves the table then.
#[derive(Clone, Default)]
pub(crate) struct MergeDemotions(Arc<Mutex<HashSet<String>>>);

impl MergeDemotions {
    /// Turns off the merge of a plan with a demoted input.
    pub(crate) fn apply(&self, table_id: TableId, plan: &mut CompactionPlan) {
        if !plan.merge.enabled {
            return;
        }
        let demoted = self.0.lock().expect("merge demotions poisoned");
        if let Some(file) = plan
            .input_files
            .iter()
            .find(|file| demoted.contains(&file.0))
        {
            plan.merge.enabled = false;
            tracing::info!(
                event = "compaction_merge_skipped",
                table_id = table_id.0,
                file = %file.0,
                "a rewrite input was found out of order before; concatenating"
            );
        }
    }

    /// Records the input of an out-of-order merge and makes the error
    /// retryable: the next plan concatenates. Other errors pass through.
    pub(crate) fn observe(&self, table_id: TableId, error: anyhow::Error) -> anyhow::Error {
        let Some(InputOutOfOrder { file }) = error.downcast_ref::<InputOutOfOrder>() else {
            return error;
        };
        {
            let mut demoted = self.0.lock().expect("merge demotions poisoned");
            if demoted.len() >= MAX_DEMOTED_FILES {
                demoted.clear();
            }
            demoted.insert(file.0.clone());
        }
        tracing::warn!(
            event = "compaction_input_out_of_order",
            table_id = table_id.0,
            file = %file.0,
            "a data file stamped with the table's sort order is out of order; \
             rewrites that include it concatenate"
        );
        metrics::counter!("flow_compaction_merge_demotions_total", "table_id" => table_id.0.to_string())
            .increment(1);
        error.context(ReplanRequired)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flow_model::FileId;

    fn plan(files: &[&str]) -> CompactionPlan {
        CompactionPlan {
            base_snapshot_id: 1,
            schema_id: 0,
            input_files: files.iter().map(|file| FileId((*file).into())).collect(),
            delete_files: Default::default(),
            spec_id: 0,
            partition: vec![],
            output_level: flow_compactor::Level::L1,
            target_file_bytes: 1,
            input_bytes: 0,
            delete_input_bytes: 0,
            delete_input_rows: 0,
            lazy_index_remap: true,
            merge: flow_compactor::MergePolicy {
                enabled: true,
                ..Default::default()
            },
            per_target_deletes: false,
            target_ranges: Default::default(),
            reason: flow_compactor::PlanReason::L0Debt,
        }
    }

    #[test]
    fn an_out_of_order_input_turns_off_merges_that_include_it() {
        let demotions = MergeDemotions::default();
        let other = demotions.observe(TableId(1), anyhow::anyhow!("unrelated"));
        assert!(other.downcast_ref::<ReplanRequired>().is_none());

        let error = anyhow::Error::from(InputOutOfOrder {
            file: FileId("bad".into()),
        })
        .context("building");
        let error = demotions.observe(TableId(1), error);
        assert!(error.downcast_ref::<ReplanRequired>().is_some());

        let mut with = plan(&["good", "bad"]);
        demotions.apply(TableId(1), &mut with);
        assert!(!with.merge.enabled);
        let mut without = plan(&["good", "other"]);
        demotions.apply(TableId(1), &mut without);
        assert!(without.merge.enabled);
    }
}
