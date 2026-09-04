use anyhow::{Result, bail, ensure};
use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, FixedSizeBinaryArray,
    Float64Array, Int32Array, Int64Array, LargeBinaryArray, RecordBatch, StringArray,
    TimestampMicrosecondArray,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use flow_model::{ColumnType, Row, TableSchema, Value};
use iceberg::spec::{NestedField, PrimitiveType, Type};
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use std::{borrow::Borrow, collections::HashMap, sync::Arc};

pub fn iceberg_schema(schema: &TableSchema) -> Result<iceberg::spec::Schema> {
    schema.validate()?;
    let fields = schema
        .columns
        .iter()
        .map(|col| {
            let kind = match col.data_type {
                ColumnType::Bool => PrimitiveType::Boolean,
                ColumnType::Int32 => PrimitiveType::Int,
                ColumnType::Int64 => PrimitiveType::Long,
                ColumnType::Float64 => PrimitiveType::Double,
                ColumnType::String => PrimitiveType::String,
                ColumnType::Binary => PrimitiveType::Binary,
                ColumnType::Date => PrimitiveType::Date,
                ColumnType::TimestampMicros => PrimitiveType::Timestamp,
                ColumnType::TimestampTzMicros => PrimitiveType::Timestamptz,
                ColumnType::Uuid => PrimitiveType::Uuid,
                ColumnType::Decimal { precision, scale } => PrimitiveType::Decimal {
                    precision: u32::from(precision),
                    scale: u32::from(scale),
                },
            };
            Arc::new(if col.nullable {
                NestedField::optional(col.field_id, &col.name, Type::Primitive(kind))
            } else {
                NestedField::required(col.field_id, &col.name, Type::Primitive(kind))
            })
        })
        .collect::<Vec<_>>();
    Ok(iceberg::spec::Schema::builder()
        .with_schema_id(i32::try_from(schema.version)?)
        .with_fields(fields)
        .with_identifier_field_ids(
            schema
                .primary_key
                .iter()
                .map(|i| schema.columns[*i].field_id),
        )
        .build()?)
}

pub fn arrow_schema(schema: &TableSchema) -> SchemaRef {
    Arc::new(Schema::new(
        schema
            .columns
            .iter()
            .map(|col| {
                let kind = match col.data_type {
                    ColumnType::Bool => DataType::Boolean,
                    ColumnType::Int32 => DataType::Int32,
                    ColumnType::Int64 => DataType::Int64,
                    ColumnType::Float64 => DataType::Float64,
                    ColumnType::String => DataType::Utf8,
                    ColumnType::Binary => DataType::Binary,
                    ColumnType::Date => DataType::Date32,
                    ColumnType::TimestampMicros => DataType::Timestamp(TimeUnit::Microsecond, None),
                    ColumnType::TimestampTzMicros => {
                        DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into()))
                    }
                    ColumnType::Uuid => DataType::FixedSizeBinary(16),
                    ColumnType::Decimal { precision, scale } => {
                        DataType::Decimal128(precision, scale as i8)
                    }
                };
                Field::new(&col.name, kind, col.nullable).with_metadata(HashMap::from([(
                    PARQUET_FIELD_ID_META_KEY.into(),
                    col.field_id.to_string(),
                )]))
            })
            .collect::<Vec<_>>(),
    ))
}

/// Conversion is column-oriented; callers bound the batch by bytes as well as rows.
pub fn rows_to_batch(schema: &TableSchema, rows: &[impl Borrow<Row>]) -> Result<RecordBatch> {
    for row in rows {
        schema.validate_row(row.borrow())?;
    }
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(schema.columns.len());
    for (index, col) in schema.columns.iter().enumerate() {
        macro_rules! primitive {
            ($variant:ident, $array:ident) => {
                Arc::new(
                    rows.iter()
                        .map(|r| match &r.borrow()[index] {
                            Value::$variant(v) => Some(*v),
                            _ => None,
                        })
                        .collect::<$array>(),
                ) as ArrayRef
            };
        }
        let array = match col.data_type {
            ColumnType::Bool => primitive!(Bool, BooleanArray),
            ColumnType::Int32 => primitive!(Int32, Int32Array),
            ColumnType::Int64 => primitive!(Int64, Int64Array),
            ColumnType::Float64 => primitive!(Float64, Float64Array),
            ColumnType::Date => primitive!(Date, Date32Array),
            ColumnType::TimestampMicros => primitive!(TimestampMicros, TimestampMicrosecondArray),
            ColumnType::TimestampTzMicros => Arc::new(
                rows.iter()
                    .map(|row| match row.borrow()[index] {
                        Value::TimestampTzMicros(value) => Some(value),
                        _ => None,
                    })
                    .collect::<TimestampMicrosecondArray>()
                    .with_timezone("+00:00"),
            ),
            ColumnType::String => Arc::new(
                rows.iter()
                    .map(|r| match &r.borrow()[index] {
                        Value::String(v) => Some(v.as_str()),
                        _ => None,
                    })
                    .collect::<StringArray>(),
            ),
            ColumnType::Binary => Arc::new(
                rows.iter()
                    .map(|r| match &r.borrow()[index] {
                        Value::Binary(v) => Some(v.as_slice()),
                        _ => None,
                    })
                    .collect::<BinaryArray>(),
            ),
            ColumnType::Uuid => Arc::new(FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                rows.iter().map(|r| match &r.borrow()[index] {
                    Value::Uuid(v) => Some(v.as_slice()),
                    _ => None,
                }),
                16,
            )?),
            ColumnType::Decimal { precision, scale } => Arc::new(
                rows.iter()
                    .map(|r| match &r.borrow()[index] {
                        Value::Decimal { unscaled, .. } => Some(*unscaled),
                        _ => None,
                    })
                    .collect::<Decimal128Array>()
                    .with_precision_and_scale(precision, scale as i8)?,
            ),
        };
        arrays.push(array);
    }
    Ok(RecordBatch::try_new(arrow_schema(schema), arrays)?)
}

/// Decode a scan batch by stable field ID. Missing additive nullable fields become null.
/// Used for index reconstruction and external physical-rewrite verification.
pub fn rows_from_batch(schema: &TableSchema, batch: &RecordBatch) -> Result<Vec<Row>> {
    let batch_schema = batch.schema();
    let mut rows = vec![Vec::with_capacity(schema.columns.len()); batch.num_rows()];
    for col in &schema.columns {
        let index = batch_schema.fields().iter().position(|f| {
            f.metadata()
                .get(PARQUET_FIELD_ID_META_KEY)
                .is_some_and(|id| id == &col.field_id.to_string())
        });
        let index = match index {
            Some(i) => i,
            None if col.nullable => {
                for row in &mut rows {
                    row.push(Value::Null);
                }
                continue;
            }
            None => bail!("scan lacks required Iceberg field {}", col.field_id),
        };
        let array = batch.column(index);
        for (i, row) in rows.iter_mut().enumerate() {
            if array.is_null(i) {
                ensure!(col.nullable, "null in required field {}", col.name);
                row.push(Value::Null);
                continue;
            }
            macro_rules! scalar {
                ($array:ty, $variant:ident) => {{
                    let a = array
                        .as_any()
                        .downcast_ref::<$array>()
                        .ok_or_else(|| anyhow::anyhow!("scan type mismatch for {}", col.name))?;
                    Value::$variant(a.value(i))
                }};
            }
            let value = match col.data_type {
                ColumnType::Bool => scalar!(BooleanArray, Bool),
                ColumnType::Int32 => scalar!(Int32Array, Int32),
                ColumnType::Int64 => scalar!(Int64Array, Int64),
                ColumnType::Float64 => scalar!(Float64Array, Float64),
                ColumnType::Date => scalar!(Date32Array, Date),
                ColumnType::TimestampMicros => scalar!(TimestampMicrosecondArray, TimestampMicros),
                ColumnType::TimestampTzMicros => {
                    scalar!(TimestampMicrosecondArray, TimestampTzMicros)
                }
                ColumnType::String => Value::String(
                    array
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or_else(|| anyhow::anyhow!("string type mismatch"))?
                        .value(i)
                        .to_owned(),
                ),
                ColumnType::Binary => {
                    // Parquet's direct reader uses Binary; Iceberg's projected
                    // schema uses LargeBinary. Both represent the same value.
                    let bytes = if let Some(binary) = array.as_any().downcast_ref::<BinaryArray>() {
                        binary.value(i)
                    } else if let Some(binary) = array.as_any().downcast_ref::<LargeBinaryArray>() {
                        binary.value(i)
                    } else {
                        bail!("binary type mismatch: {:?}", array.data_type());
                    };
                    Value::Binary(bytes.to_owned())
                }
                ColumnType::Uuid => Value::Uuid(
                    array
                        .as_any()
                        .downcast_ref::<FixedSizeBinaryArray>()
                        .ok_or_else(|| anyhow::anyhow!("UUID type mismatch"))?
                        .value(i)
                        .try_into()?,
                ),
                ColumnType::Decimal { scale, .. } => Value::Decimal {
                    unscaled: array
                        .as_any()
                        .downcast_ref::<Decimal128Array>()
                        .ok_or_else(|| anyhow::anyhow!("decimal type mismatch"))?
                        .value(i),
                    scale,
                },
            };
            row.push(value);
        }
    }
    Ok(rows)
}
