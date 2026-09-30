use flow_model::{
    JournalChunks, ModelError, PgLsn, SourceId, SourceTransaction, TableId, TableMutationCount,
};

fn transaction(tables: Vec<TableId>) -> SourceTransaction {
    let mut counts: Vec<_> = tables
        .iter()
        .map(|&table_id| TableMutationCount {
            table_id,
            mutations: 1,
        })
        .collect();
    counts.sort_by_key(|count| count.table_id);
    SourceTransaction {
        source_id: SourceId("source".into()),
        xid: 1,
        begin_lsn: PgLsn(1),
        commit_lsn: PgLsn(2),
        end_lsn: PgLsn(3),
        commit_timestamp_micros: 0,
        schema_versions: vec![],
        affected_tables: tables,
        mutation_chunks: JournalChunks::default(),
        table_mutation_counts: Some(counts),
    }
}

#[test]
fn counts_accept_unsorted_affected_tables_but_require_exact_unique_membership() {
    let valid = transaction(vec![TableId(3), TableId(1), TableId(2)]);
    valid.validate_mutation_counts().unwrap();
    for tables in [
        vec![TableId(1), TableId(2)],
        vec![TableId(1), TableId(2), TableId(4)],
        vec![TableId(1), TableId(2), TableId(2)],
    ] {
        let mut invalid = valid.clone();
        invalid.affected_tables = tables;
        assert!(matches!(
            invalid.validate_mutation_counts(),
            Err(ModelError::InvalidMutationCounts)
        ));
    }
    let mut invalid = valid.clone();
    invalid.table_mutation_counts.as_mut().unwrap()[1].table_id = TableId(1);
    assert!(invalid.validate_mutation_counts().is_err());
    let mut overflow = valid;
    overflow.table_mutation_counts.as_mut().unwrap()[0].mutations = u64::MAX;
    assert!(overflow.validate_mutation_counts().is_err());
}

#[test]
fn wide_transaction_counts_accept_reversed_table_order() {
    transaction((0..10_000).rev().map(TableId).collect())
        .validate_mutation_counts()
        .unwrap();
}
