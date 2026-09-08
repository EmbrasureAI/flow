use std::sync::Arc;

use anyhow::{Context, Result, ensure};
use arrow_array::{Array, Int64Array, RecordBatch};
use flow_model::TableSchema;
use iceberg::{
    metadata_columns::{
        RESERVED_COL_NAME_LAST_UPDATED_SEQUENCE_NUMBER, RESERVED_COL_NAME_ROW_ID,
        RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER, RESERVED_FIELD_ID_ROW_ID,
    },
    spec::{NestedField, PrimitiveType, Schema, Type},
};
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

/// Logical row identity survives physical compaction. Null IDs are permitted
/// for pre-upgrade rows and are assigned by the next v3 publication.
#[derive(Debug, Clone, Copy, Default)]
pub struct RowLineage {
    pub row_id: Option<i64>,
    pub last_updated_sequence_number: Option<i64>,
}

pub(crate) fn writer_schema(schema: &TableSchema) -> Result<Schema> {
    let schema = crate::iceberg_schema(schema)?;
    let mut fields = schema.as_struct().fields().to_vec();
    for (id, name) in [
        (RESERVED_FIELD_ID_ROW_ID, RESERVED_COL_NAME_ROW_ID),
        (
            RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER,
            RESERVED_COL_NAME_LAST_UPDATED_SEQUENCE_NUMBER,
        ),
    ] {
        fields.push(Arc::new(NestedField::optional(
            id,
            name,
            Type::Primitive(PrimitiveType::Long),
        )));
    }
    Ok(Schema::builder().with_fields(fields).build()?)
}

/// Resolve inherited values using physical positions before applying deletes.
pub fn read_row_lineage(
    batch: &RecordBatch,
    first_position: u64,
    first_row_id: Option<i64>,
    data_sequence: i64,
) -> Result<Vec<RowLineage>> {
    let column = |id: i32| -> Result<Option<&Int64Array>> {
        let id = id.to_string();
        let schema = batch.schema();
        let mut indices = schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, field)| field.metadata().get(PARQUET_FIELD_ID_META_KEY) == Some(&id));
        let Some((index, _)) = indices.next() else {
            return Ok(None);
        };
        ensure!(indices.next().is_none(), "duplicate row lineage field");
        Ok(Some(
            batch
                .column(index)
                .as_any()
                .downcast_ref::<Int64Array>()
                .context("row lineage must be an Iceberg long")?,
        ))
    };
    let ids = column(RESERVED_FIELD_ID_ROW_ID)?;
    let sequences = column(RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER)?;
    (0..batch.num_rows())
        .map(|index| {
            let explicit = |column: Option<&Int64Array>| {
                column.and_then(|column| (!column.is_null(index)).then(|| column.value(index)))
            };
            let row_id = match explicit(ids) {
                Some(id) => Some(id),
                None => first_row_id
                    .map(|first| {
                        let position = first_position
                            .checked_add(index as u64)
                            .context("row position overflow")?;
                        first
                            .checked_add(i64::try_from(position)?)
                            .context("row ID overflow")
                    })
                    .transpose()?,
            };
            let sequence = explicit(sequences).unwrap_or(data_sequence);
            ensure!(
                row_id.is_none_or(|id| id >= 0) && sequence >= 0,
                "negative row lineage"
            );
            Ok(RowLineage {
                row_id,
                last_updated_sequence_number: Some(sequence),
            })
        })
        .collect()
}
