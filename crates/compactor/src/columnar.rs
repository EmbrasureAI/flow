//! Rewrites scanned Arrow batches without converting them to rows. Keys and
//! fingerprints reproduce `flow_model`'s canonical encoding column by column;
//! the tests check them byte for byte against `TableSchema`.

use anyhow::{Context, Result, ensure};
use arrow_array::{
    Array, ArrayRef, BooleanArray, Date32Array, Decimal128Array, FixedSizeBinaryArray,
    Float64Array, Int32Array, Int64Array, LargeBinaryArray, RecordBatch, StringArray,
    TimestampMicrosecondArray, cast::AsArray, new_null_array,
};
use arrow_schema::{DataType, SchemaRef, TimeUnit};
use flow_model::{ColumnType, PrimaryKey, TableSchema};
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use std::sync::Arc;

/// Select a scan batch's columns by field ID into the table's Arrow schema,
/// accepting what `rows_from_batch` accepts. Missing nullable fields become
/// null; other columns are shared, not copied, unless their Arrow type differs.
pub(crate) fn conform(
    schema: &TableSchema,
    target: &SchemaRef,
    batch: &RecordBatch,
) -> Result<RecordBatch> {
    let batch_schema = batch.schema();
    let columns = schema
        .columns
        .iter()
        .zip(target.fields())
        .map(|(col, field)| {
            let index = batch_schema.fields().iter().position(|f| {
                f.metadata()
                    .get(PARQUET_FIELD_ID_META_KEY)
                    .is_some_and(|id| id == &col.field_id.to_string())
            });
            let Some(index) = index else {
                ensure!(
                    col.nullable,
                    "scan lacks required Iceberg field {}",
                    col.field_id
                );
                return Ok(new_null_array(field.data_type(), batch.num_rows()));
            };
            let array = batch.column(index);
            let compatible = match (&col.data_type, array.data_type()) {
                (ColumnType::Bool, DataType::Boolean)
                | (ColumnType::Int32, DataType::Int32)
                | (ColumnType::Int64, DataType::Int64)
                | (ColumnType::Float64, DataType::Float64)
                | (ColumnType::String, DataType::Utf8)
                | (ColumnType::Binary, DataType::Binary | DataType::LargeBinary)
                | (ColumnType::Date, DataType::Date32)
                | (ColumnType::Uuid, DataType::FixedSizeBinary(16))
                | (ColumnType::TimestampMicros, DataType::Timestamp(TimeUnit::Microsecond, None))
                | (
                    ColumnType::TimestampTzMicros,
                    DataType::Timestamp(TimeUnit::Microsecond, Some(_)),
                ) => true,
                (ColumnType::Decimal { scale, .. }, DataType::Decimal128(_, actual_scale)) => {
                    i8::try_from(*scale).ok() == Some(*actual_scale)
                }
                _ => false,
            };
            ensure!(
                compatible,
                "scan type mismatch for {}: {:?}",
                col.name,
                array.data_type()
            );
            ensure!(
                col.nullable || array.null_count() == 0,
                "null in required field {}",
                col.name
            );
            Ok(if array.data_type() == field.data_type() {
                array.clone()
            } else {
                // Same values under the writer's type: the Parquet reader uses
                // Binary where Iceberg writes LargeBinary, and readers may name
                // UTC or declare decimal precision differently.
                match (array.data_type(), field.data_type()) {
                    (DataType::Binary, _) => Arc::new(
                        array
                            .as_binary::<i32>()
                            .iter()
                            .collect::<LargeBinaryArray>(),
                    ) as ArrayRef,
                    (_, DataType::Timestamp(_, Some(timezone))) => Arc::new(
                        array
                            .as_primitive::<arrow_array::types::TimestampMicrosecondType>()
                            .clone()
                            .with_timezone(timezone.clone()),
                    ),
                    (_, DataType::Decimal128(precision, scale)) => Arc::new(
                        array
                            .as_primitive::<arrow_array::types::Decimal128Type>()
                            .clone()
                            .with_precision_and_scale(*precision, *scale)?,
                    ),
                    (actual, expected) => {
                        anyhow::bail!("cannot conform {actual:?} to {expected:?}")
                    }
                }
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(RecordBatch::try_new(target.clone(), columns)?)
}

enum Column<'a> {
    Bool(&'a BooleanArray),
    Int32(&'a Int32Array),
    Int64(&'a Int64Array),
    Float64(&'a Float64Array),
    String(&'a StringArray),
    Binary(&'a LargeBinaryArray),
    Date(&'a Date32Array),
    Timestamp(&'a TimestampMicrosecondArray),
    TimestampTz(&'a TimestampMicrosecondArray),
    Uuid(&'a FixedSizeBinaryArray),
    Decimal(&'a Decimal128Array, u8),
}

/// Primary keys and row fingerprints of a [`conform`]ed batch.
pub(crate) struct RowEncoder<'a> {
    columns: Vec<(Column<'a>, &'a dyn Array)>,
    primary_key: &'a [usize],
    buffer: Vec<u8>,
}

impl<'a> RowEncoder<'a> {
    pub(crate) fn new(schema: &'a TableSchema, batch: &'a RecordBatch) -> Result<Self> {
        ensure!(
            batch.num_columns() == schema.columns.len(),
            "batch does not match the table schema"
        );
        let columns = schema
            .columns
            .iter()
            .zip(batch.columns())
            .map(|(col, array)| {
                let any = array.as_any();
                let column = match col.data_type {
                    ColumnType::Bool => any.downcast_ref().map(Column::Bool),
                    ColumnType::Int32 => any.downcast_ref().map(Column::Int32),
                    ColumnType::Int64 => any.downcast_ref().map(Column::Int64),
                    ColumnType::Float64 => any.downcast_ref().map(Column::Float64),
                    ColumnType::String => any.downcast_ref().map(Column::String),
                    ColumnType::Binary => any.downcast_ref().map(Column::Binary),
                    ColumnType::Date => any.downcast_ref().map(Column::Date),
                    ColumnType::TimestampMicros => any.downcast_ref().map(Column::Timestamp),
                    ColumnType::TimestampTzMicros => any.downcast_ref().map(Column::TimestampTz),
                    ColumnType::Uuid => any.downcast_ref().map(Column::Uuid),
                    ColumnType::Decimal { scale, .. } => any
                        .downcast_ref()
                        .map(|array| Column::Decimal(array, scale)),
                };
                Ok((
                    column.with_context(|| format!("scan type mismatch for {}", col.name))?,
                    array.as_ref(),
                ))
            })
            .collect::<Result<_>>()?;
        Ok(Self {
            columns,
            primary_key: &schema.primary_key,
            buffer: Vec::new(),
        })
    }

    /// `TableSchema::encode_key` of the row at `index`.
    pub(crate) fn key(&self, index: usize) -> PrimaryKey {
        let mut bytes = vec![1]; // canonical format version
        for &column in self.primary_key {
            self.encode(column, index, &mut bytes);
        }
        PrimaryKey(bytes)
    }

    /// `flow_model::fingerprint` of the row at `index`. Trailing nulls are
    /// omitted, so rows from before a nullable addition keep their identity.
    pub(crate) fn fingerprint(&mut self, index: usize) -> [u8; 16] {
        let end = self
            .columns
            .iter()
            .rposition(|(_, array)| array.is_valid(index))
            .map_or(0, |column| column + 1);
        let mut buffer = std::mem::take(&mut self.buffer);
        buffer.clear();
        buffer.push(1);
        for column in 0..end {
            self.encode(column, index, &mut buffer);
        }
        let mut result = [0; 16];
        result.copy_from_slice(&blake3::hash(&buffer).as_bytes()[..16]);
        self.buffer = buffer;
        result
    }

    fn encode(&self, column: usize, index: usize, out: &mut Vec<u8>) {
        let (column, array) = &self.columns[column];
        if array.is_null(index) {
            out.push(0);
            return;
        }
        match column {
            Column::Bool(array) => out.extend([1, u8::from(array.value(index))]),
            Column::Int32(array) => {
                out.push(2);
                out.extend(array.value(index).to_be_bytes());
            }
            Column::Int64(array) => {
                out.push(3);
                out.extend(array.value(index).to_be_bytes());
            }
            Column::Float64(array) => {
                let value = array.value(index);
                let bits = if value.is_nan() {
                    f64::NAN.to_bits()
                } else if value == 0.0 {
                    0
                } else {
                    value.to_bits()
                };
                out.push(4);
                out.extend(bits.to_be_bytes());
            }
            Column::String(array) => {
                let value = array.value(index);
                out.push(5);
                out.extend((value.len() as u64).to_be_bytes());
                out.extend_from_slice(value.as_bytes());
            }
            Column::Binary(array) => {
                let value = array.value(index);
                out.push(6);
                out.extend((value.len() as u64).to_be_bytes());
                out.extend_from_slice(value);
            }
            Column::Date(array) => {
                out.push(7);
                out.extend(array.value(index).to_be_bytes());
            }
            Column::Timestamp(array) => {
                out.push(8);
                out.extend(array.value(index).to_be_bytes());
            }
            Column::Uuid(array) => {
                out.push(9);
                out.extend_from_slice(array.value(index));
            }
            Column::Decimal(array, scale) => {
                out.extend([10, *scale]);
                out.extend(array.value(index).to_be_bytes());
            }
            Column::TimestampTz(array) => {
                out.push(11);
                out.extend(array.value(index).to_be_bytes());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{Field, Schema};
    use flow_materializer::{rows_from_batch, rows_to_batch, writer_arrow_schema};
    use flow_model::{Column as SchemaColumn, Row, TableId, Value};
    use std::collections::HashMap;

    fn schema() -> TableSchema {
        let types = [
            ColumnType::Int64,
            ColumnType::String,
            ColumnType::Binary,
            ColumnType::Uuid,
            ColumnType::Decimal {
                precision: 38,
                scale: 4,
            },
            ColumnType::Date,
            ColumnType::Bool,
            ColumnType::Int32,
            ColumnType::Float64,
            ColumnType::TimestampMicros,
            ColumnType::TimestampTzMicros,
            ColumnType::String,
        ];
        TableSchema {
            table_id: TableId(1),
            version: 1,
            columns: types
                .into_iter()
                .enumerate()
                .map(|(index, data_type)| SchemaColumn {
                    field_id: index as i32 + 1,
                    name: format!("c{index}"),
                    data_type,
                    // Key columns are required; the rest may be null.
                    nullable: index > 6,
                })
                .collect(),
            primary_key: vec![0, 1, 2, 3, 4, 5, 6],
            append_only: false,
        }
    }

    fn rows() -> Vec<Row> {
        let floats = [0.0, -0.0, f64::NAN, -f64::NAN, f64::INFINITY, -1.5, 1e-300];
        floats
            .iter()
            .enumerate()
            .map(|(index, float)| {
                let i = index as i64;
                let present = |value: Value| if index % 3 == 1 { Value::Null } else { value };
                let mut row = vec![
                    Value::Int64(if index % 2 == 0 {
                        i64::MIN + i
                    } else {
                        i64::MAX - i
                    }),
                    Value::String("é".repeat(index)),
                    Value::Binary(vec![index as u8; index * 3]),
                    Value::Uuid([index as u8; 16]),
                    Value::Decimal {
                        unscaled: if index % 2 == 0 {
                            10i128.pow(38) - 1 - index as i128
                        } else {
                            -(10i128.pow(38) - 1)
                        },
                        scale: 4,
                    },
                    Value::Date(-(index as i32)),
                    Value::Bool(index % 2 == 0),
                    present(Value::Int32(i32::MIN + index as i32)),
                    present(Value::Float64(*float)),
                    present(Value::TimestampMicros(-i)),
                    present(Value::TimestampTzMicros(i << 40)),
                    // Trailing nulls are omitted from fingerprints; interior ones are not.
                    if index % 2 == 0 {
                        Value::Null
                    } else {
                        Value::String(String::new())
                    },
                ];
                if index == 6 {
                    row[7..].fill(Value::Null);
                }
                row
            })
            .collect()
    }

    #[test]
    fn keys_and_fingerprints_match_the_row_encoding() {
        let schema = schema();
        let rows = rows();
        let batch = conform(
            &schema,
            &writer_arrow_schema(&schema),
            &rows_to_batch(&schema, &rows).unwrap(),
        )
        .unwrap();
        let mut encoder = RowEncoder::new(&schema, &batch).unwrap();
        for (index, row) in rows.iter().enumerate() {
            assert_eq!(encoder.key(index), schema.encode_key(row).unwrap());
            assert_eq!(encoder.fingerprint(index), schema.fingerprint(row).unwrap());
        }
        // A keyless schema encodes no key but fingerprints every column.
        let mut keyless = schema.clone();
        keyless.primary_key.clear();
        keyless.append_only = true;
        let mut encoder = RowEncoder::new(&keyless, &batch).unwrap();
        for (index, row) in rows.iter().enumerate() {
            assert_eq!(
                encoder.fingerprint(index),
                keyless.fingerprint(row).unwrap()
            );
        }
    }

    #[test]
    fn conform_accepts_scan_variants_and_matches_row_decoding() {
        let schema = schema();
        let rows = rows();
        let written = rows_to_batch(&schema, &rows).unwrap();
        // An older file lacks the final nullable field; the Parquet reader
        // presents binary as Binary where the writer uses LargeBinary, and a
        // reader may name UTC or declare decimal precision differently.
        let mut fields = Vec::new();
        let mut columns = Vec::new();
        for (index, field) in written.schema().fields().iter().enumerate().take(11) {
            let column = written.column(index);
            let (data_type, column): (DataType, ArrayRef) = match field.data_type() {
                DataType::Timestamp(unit, Some(_)) => (
                    DataType::Timestamp(*unit, Some("UTC".into())),
                    Arc::new(
                        column
                            .as_primitive::<arrow_array::types::TimestampMicrosecondType>()
                            .clone()
                            .with_timezone("UTC"),
                    ),
                ),
                // Iceberg permits widening decimal precision.
                DataType::Decimal128(_, scale) => (
                    DataType::Decimal128(20, *scale),
                    Arc::new(
                        column
                            .as_primitive::<arrow_array::types::Decimal128Type>()
                            .clone()
                            .with_precision_and_scale(20, *scale)
                            .unwrap(),
                    ),
                ),
                other => (other.clone(), column.clone()),
            };
            fields.push(
                Field::new(field.name(), data_type, true).with_metadata(HashMap::from([(
                    PARQUET_FIELD_ID_META_KEY.into(),
                    (index + 1).to_string(),
                )])),
            );
            columns.push(column);
        }
        // Unknown fields, such as Iceberg row lineage, are ignored.
        fields.push(Field::new("extra", DataType::Int64, true));
        columns.push(Arc::new(Int64Array::from(vec![0; rows.len()])));
        let scanned = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
        let conformed = conform(&schema, &writer_arrow_schema(&schema), &scanned).unwrap();
        let expected = rows_from_batch(&schema, &scanned).unwrap();
        // Debug formatting compares NaN values, which `Value` equality cannot.
        assert_eq!(
            format!("{:?}", rows_from_batch(&schema, &conformed).unwrap()),
            format!("{expected:?}")
        );
        let mut encoder = RowEncoder::new(&schema, &conformed).unwrap();
        for (index, row) in expected.iter().enumerate() {
            assert_eq!(encoder.fingerprint(index), schema.fingerprint(row).unwrap());
        }

        let required = scanned.project(&[1, 2, 3, 4, 5, 6]).unwrap();
        assert!(
            conform(&schema, &writer_arrow_schema(&schema), &required)
                .unwrap_err()
                .to_string()
                .contains("lacks required Iceberg field 1")
        );
        let mut nulls = scanned.columns().to_vec();
        nulls[0] = Arc::new(Int64Array::from(vec![None; rows.len()]));
        let nulls = RecordBatch::try_new(scanned.schema(), nulls).unwrap();
        assert!(
            conform(&schema, &writer_arrow_schema(&schema), &nulls)
                .unwrap_err()
                .to_string()
                .contains("null in required field c0")
        );
    }
}
