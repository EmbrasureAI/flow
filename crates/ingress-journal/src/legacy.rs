//! Read-only compatibility for journal terminals and source-ledger envelopes
//! without mutation counts. Always decode with a bounded codec limit.

use crate::{Result, chunks};
use flow_model::{JournalChunkRef, JournalChunks, PgLsn, SourceId, TableId, TableSchemaVersion};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct SourceTransaction<C = Vec<JournalChunkRef>> {
    pub source_id: SourceId,
    pub xid: u32,
    pub begin_lsn: PgLsn,
    pub commit_lsn: PgLsn,
    pub end_lsn: PgLsn,
    pub commit_timestamp_micros: i64,
    pub schema_versions: Vec<TableSchemaVersion>,
    pub affected_tables: Vec<TableId>,
    pub mutation_chunks: C,
}
pub type BoundedSourceTransaction = SourceTransaction<JournalChunks>;

impl SourceTransaction {
    pub fn into_current(self) -> Result<flow_model::SourceTransaction> {
        let mut chunks = JournalChunks::default();
        for reference in &self.mutation_chunks {
            chunks::include(&mut chunks, reference)?;
        }
        Ok(self.with_chunks(chunks))
    }
}
impl BoundedSourceTransaction {
    pub fn into_current(self) -> Result<flow_model::SourceTransaction> {
        let chunks = self.mutation_chunks.clone();
        Ok(self.with_chunks(chunks))
    }
}
impl<C> SourceTransaction<C> {
    fn with_chunks(self, mutation_chunks: JournalChunks) -> flow_model::SourceTransaction {
        flow_model::SourceTransaction {
            source_id: self.source_id,
            xid: self.xid,
            begin_lsn: self.begin_lsn,
            commit_lsn: self.commit_lsn,
            end_lsn: self.end_lsn,
            commit_timestamp_micros: self.commit_timestamp_micros,
            schema_versions: self.schema_versions,
            affected_tables: self.affected_tables,
            mutation_chunks,
            table_mutation_counts: None,
        }
    }
}
