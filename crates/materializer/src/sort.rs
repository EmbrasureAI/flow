//! Table sort orders: which Iceberg orders Flow writes, how rows compare under
//! them, and how a writer verifies a file before stamping it with the order.
//!
//! Order is a layout property, never a correctness property. A data file
//! carries `sort_order_id` only when every row was checked against the order.
//! Comparison follows Iceberg's ordering, not Flow's canonical key encoding,
//! which sorts negative integers after positive ones and strings by length.

use anyhow::{Context, Result};
use arrow_array::ArrayRef;
use arrow_ord::ord::{DynComparator, make_comparator};
use arrow_schema::SortOptions;
use flow_model::{Row, TableSchema, Value};
use iceberg::spec::{NullOrder, Schema, SortDirection, SortField, SortOrder, Transform};
use parquet::file::metadata::SortingColumn;
use std::cmp::Ordering;

/// One sort column, as an index into [`TableSchema::columns`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SortKey {
    pub column: usize,
    pub descending: bool,
    pub nulls_first: bool,
}

/// An Iceberg sort order that Flow can write.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SortSpec {
    pub order_id: i64,
    pub keys: Vec<SortKey>,
}

/// How a table's default sort order applies to Flow's writers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TableSort {
    /// No declared order: files keep arrival order.
    Unsorted,
    Supported(SortSpec),
    /// A declared order Flow cannot write yet. Files are written unsorted.
    Unsupported {
        order_id: i64,
        reason: String,
    },
}

impl TableSort {
    /// Resolve `order` against the replicated columns. Identity transforms on
    /// any column are supported; other transforms are not yet.
    pub fn resolve(schema: &TableSchema, order: &SortOrder) -> Self {
        if order.fields.is_empty() {
            return Self::Unsorted;
        }
        let mut keys = Vec::with_capacity(order.fields.len());
        for field in &order.fields {
            if field.transform != Transform::Identity {
                return Self::Unsupported {
                    order_id: order.order_id,
                    reason: format!(
                        "transform {} on field {} is not supported",
                        field.transform, field.source_id
                    ),
                };
            }
            let Some(column) = schema
                .columns
                .iter()
                .position(|column| column.field_id == field.source_id)
            else {
                return Self::Unsupported {
                    order_id: order.order_id,
                    reason: format!("field {} is not a replicated column", field.source_id),
                };
            };
            keys.push(SortKey {
                column,
                descending: field.direction == SortDirection::Descending,
                nulls_first: field.null_order == NullOrder::First,
            });
        }
        Self::Supported(SortSpec {
            order_id: order.order_id,
            keys,
        })
    }

    /// The table's current default order.
    pub fn of_table(schema: &TableSchema, metadata: &iceberg::spec::TableMetadata) -> Self {
        Self::resolve(schema, metadata.default_sort_order())
    }

    pub fn spec(&self) -> Option<&SortSpec> {
        match self {
            Self::Supported(spec) => Some(spec),
            _ => None,
        }
    }

    /// The declared order id: 0 when unsorted.
    pub fn order_id(&self) -> i64 {
        match self {
            Self::Unsorted => 0,
            Self::Supported(spec) => spec.order_id,
            Self::Unsupported { order_id, .. } => *order_id,
        }
    }
}

impl SortSpec {
    /// Compare two complete rows as Iceberg orders them.
    pub fn compare_rows(&self, left: &Row, right: &Row) -> Ordering {
        for key in &self.keys {
            let ordering = compare_values(&left[key.column], &right[key.column], key);
            if ordering != Ordering::Equal {
                return ordering;
            }
        }
        Ordering::Equal
    }

    /// Whether every sort value is known. A TOAST value carried forward from
    /// the previous row image is filled in only after collapse.
    pub fn sortable(&self, row: &Row) -> bool {
        self.keys
            .iter()
            .all(|key| !matches!(row[key.column], Value::Unchanged))
    }

    /// Parquet's description of the order, for files written already sorted.
    pub fn sorting_columns(&self) -> Result<Vec<SortingColumn>> {
        self.keys
            .iter()
            .map(|key| {
                Ok(SortingColumn {
                    column_idx: i32::try_from(key.column).context("sort column index")?,
                    descending: key.descending,
                    nulls_first: key.nulls_first,
                })
            })
            .collect()
    }

    pub fn iceberg_order_id(&self) -> Result<i32> {
        i32::try_from(self.order_id).context("sort order ID exceeds Iceberg's data file field")
    }
}

/// Iceberg's order for two values of one column: nulls first or last in
/// absolute terms, descending reversing only non-null values, floats in IEEE
/// total order and strings and binaries by bytes.
fn compare_values(left: &Value, right: &Value, key: &SortKey) -> Ordering {
    match (matches!(left, Value::Null), matches!(right, Value::Null)) {
        (true, true) => Ordering::Equal,
        (true, false) if key.nulls_first => Ordering::Less,
        (true, false) => Ordering::Greater,
        (false, true) if key.nulls_first => Ordering::Greater,
        (false, true) => Ordering::Less,
        (false, false) => {
            let ordering = compare_present(left, right);
            if key.descending {
                ordering.reverse()
            } else {
                ordering
            }
        }
    }
}

fn compare_present(left: &Value, right: &Value) -> Ordering {
    match (left, right) {
        (Value::Bool(left), Value::Bool(right)) => left.cmp(right),
        (Value::Int32(left), Value::Int32(right)) | (Value::Date(left), Value::Date(right)) => {
            left.cmp(right)
        }
        (Value::Int64(left), Value::Int64(right))
        | (Value::TimestampMicros(left), Value::TimestampMicros(right))
        | (Value::TimestampTzMicros(left), Value::TimestampTzMicros(right)) => left.cmp(right),
        (Value::Float64(left), Value::Float64(right)) => left.total_cmp(right),
        (Value::String(left), Value::String(right)) => left.as_bytes().cmp(right.as_bytes()),
        (Value::Binary(left), Value::Binary(right)) => left.cmp(right),
        (Value::Uuid(left), Value::Uuid(right)) => left.cmp(right),
        (
            Value::Decimal { unscaled: left, .. },
            Value::Decimal {
                unscaled: right, ..
            },
        ) => left.cmp(right),
        // Validated rows have one type per column.
        _ => Ordering::Equal,
    }
}

/// An Iceberg sort order over `columns`, in the given directions. Primary
/// key columns are never null, so their null order is Iceberg's default for
/// ascending columns.
pub fn sort_order(schema: &Schema, table: &TableSchema, keys: &[SortKey]) -> Result<SortOrder> {
    let mut builder = SortOrder::builder();
    for key in keys {
        let column = table
            .columns
            .get(key.column)
            .context("sort column is outside the table schema")?;
        builder.with_sort_field(
            SortField::builder()
                .source_id(column.field_id)
                .transform(Transform::Identity)
                .direction(if key.descending {
                    SortDirection::Descending
                } else {
                    SortDirection::Ascending
                })
                .null_order(if key.nulls_first {
                    NullOrder::First
                } else {
                    NullOrder::Last
                })
                .build(),
        );
    }
    if keys.is_empty() {
        return Ok(SortOrder::unsorted_order());
    }
    Ok(builder.with_order_id(1).build(schema)?)
}

/// The primary key, ascending, as Flow declares it on tables it creates.
pub fn primary_key_sort(table: &TableSchema) -> Vec<SortKey> {
    table
        .primary_key
        .iter()
        .map(|&column| SortKey {
            column,
            descending: false,
            nulls_first: true,
        })
        .collect()
}

/// Whether a writer's batches stay in order, within each batch and from the
/// last row of the previous batch. Uses Arrow's comparator, whose nulls,
/// direction and float order match Iceberg's.
pub(crate) struct SortVerifier {
    keys: Vec<(usize, SortOptions)>,
    /// One-row slices of the previous batch's last sort values.
    last: Option<Vec<ArrayRef>>,
}

impl SortVerifier {
    pub(crate) fn new(spec: &SortSpec) -> Self {
        Self {
            keys: spec
                .keys
                .iter()
                .map(|key| {
                    (
                        key.column,
                        SortOptions {
                            descending: key.descending,
                            nulls_first: key.nulls_first,
                        },
                    )
                })
                .collect(),
            last: None,
        }
    }

    /// Start a new file: the next batch is compared with nothing before it.
    pub(crate) fn reset(&mut self) {
        self.last = None;
    }

    /// Whether `columns` continue the order. The columns are a batch in the
    /// writer's column order; a false result leaves the verifier unusable
    /// until [`Self::reset`].
    pub(crate) fn accept(&mut self, columns: &[ArrayRef]) -> Result<bool> {
        let rows = columns.first().map_or(0, |column| column.len());
        if rows == 0 {
            return Ok(true);
        }
        let within = self
            .keys
            .iter()
            .map(|(column, options)| {
                make_comparator(
                    columns[*column].as_ref(),
                    columns[*column].as_ref(),
                    *options,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        if (1..rows).any(|row| lexicographic(&within, row - 1, row) == Ordering::Greater) {
            return Ok(false);
        }
        if let Some(last) = &self.last {
            let across = last
                .iter()
                .zip(&self.keys)
                .map(|(last, (column, options))| {
                    make_comparator(last.as_ref(), columns[*column].as_ref(), *options)
                })
                .collect::<Result<Vec<_>, _>>()?;
            if lexicographic(&across, 0, 0) == Ordering::Greater {
                return Ok(false);
            }
        }
        self.last = Some(
            self.keys
                .iter()
                .map(|(column, _)| columns[*column].slice(rows - 1, 1))
                .collect(),
        );
        Ok(true)
    }
}

fn lexicographic(comparators: &[DynComparator], left: usize, right: usize) -> Ordering {
    for compare in comparators {
        let ordering = compare(left, right);
        if ordering != Ordering::Equal {
            return ordering;
        }
    }
    Ordering::Equal
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::{validated_columns, writer_arrow_schema};
    use flow_model::{Column, ColumnType, TableId};

    fn table(types: &[ColumnType]) -> TableSchema {
        TableSchema {
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
        }
    }

    /// The row comparator used to sort epochs must agree with the Arrow
    /// comparator writers verify with, or a sorted epoch would fail its check.
    #[test]
    fn row_order_matches_the_writer_verifier() {
        let cases: Vec<(ColumnType, Vec<Value>)> = vec![
            (
                ColumnType::Int64,
                vec![
                    Value::Int64(5),
                    Value::Int64(-3),
                    Value::Null,
                    Value::Int64(0),
                    Value::Int64(i64::MIN),
                    Value::Int64(i64::MAX),
                ],
            ),
            (
                ColumnType::Float64,
                vec![
                    Value::Float64(0.0),
                    Value::Float64(-0.0),
                    Value::Float64(f64::NAN),
                    Value::Float64(-f64::NAN),
                    Value::Float64(f64::NEG_INFINITY),
                    Value::Float64(1.5),
                    Value::Null,
                ],
            ),
            (
                ColumnType::String,
                vec![
                    Value::String("b".into()),
                    Value::String("aa".into()),
                    Value::String("".into()),
                    Value::String("é".into()),
                    Value::String("z".into()),
                    Value::Null,
                ],
            ),
            (
                ColumnType::Decimal {
                    precision: 10,
                    scale: 2,
                },
                vec![
                    Value::Decimal {
                        unscaled: -150,
                        scale: 2,
                    },
                    Value::Decimal {
                        unscaled: 20,
                        scale: 2,
                    },
                    Value::Null,
                ],
            ),
            (
                ColumnType::Uuid,
                vec![Value::Uuid([9; 16]), Value::Uuid([0; 16]), Value::Null],
            ),
        ];
        for (data_type, values) in cases {
            let schema = table(&[data_type]);
            for (descending, nulls_first) in
                [(false, true), (false, false), (true, true), (true, false)]
            {
                let spec = SortSpec {
                    order_id: 1,
                    keys: vec![SortKey {
                        column: 0,
                        descending,
                        nulls_first,
                    }],
                };
                let mut rows: Vec<Row> = values.iter().map(|value| vec![value.clone()]).collect();
                rows.sort_by(|left, right| spec.compare_rows(left, right));
                let columns = validated_columns(&schema, &rows, true).unwrap();
                assert_eq!(
                    columns[0].data_type(),
                    writer_arrow_schema(&schema).field(0).data_type()
                );
                let mut verifier = SortVerifier::new(&spec);
                assert!(
                    verifier.accept(&columns).unwrap(),
                    "{rows:?} descending={descending} nulls_first={nulls_first}"
                );
                // Any adjacent swap of unequal rows must be caught.
                for index in 1..rows.len() {
                    if spec.compare_rows(&rows[index - 1], &rows[index]) != Ordering::Equal {
                        let mut swapped = rows.clone();
                        swapped.swap(index - 1, index);
                        let columns = validated_columns(&schema, &swapped, true).unwrap();
                        assert!(!SortVerifier::new(&spec).accept(&columns).unwrap());
                    }
                }
            }
        }
    }

    #[test]
    fn verifier_compares_across_batches_and_resets_per_file() {
        let schema = table(&[ColumnType::Int64, ColumnType::String]);
        let spec = SortSpec {
            order_id: 2,
            keys: vec![
                SortKey {
                    column: 0,
                    descending: false,
                    nulls_first: true,
                },
                SortKey {
                    column: 1,
                    descending: true,
                    nulls_first: false,
                },
            ],
        };
        let batch = |rows: &[(i64, &str)]| {
            let rows: Vec<Row> = rows
                .iter()
                .map(|(id, text)| vec![Value::Int64(*id), Value::String((*text).into())])
                .collect();
            validated_columns(&schema, &rows, true).unwrap()
        };
        let mut verifier = SortVerifier::new(&spec);
        assert!(
            verifier
                .accept(&batch(&[(1, "b"), (1, "a"), (2, "z")]))
                .unwrap()
        );
        assert!(verifier.accept(&batch(&[(2, "y"), (3, "a")])).unwrap());
        assert!(!verifier.accept(&batch(&[(2, "x")])).unwrap());
        verifier.reset();
        assert!(verifier.accept(&batch(&[(2, "x")])).unwrap());
    }

    #[test]
    fn resolves_identity_orders_and_rejects_other_transforms() {
        let mut schema = table(&[ColumnType::Int64, ColumnType::String]);
        schema.columns[0].nullable = false;
        let iceberg = crate::iceberg_schema(&schema).unwrap();
        let order = sort_order(
            &iceberg,
            &schema,
            &[SortKey {
                column: 1,
                descending: true,
                nulls_first: false,
            }],
        )
        .unwrap();
        assert_eq!(
            TableSort::resolve(&schema, &order),
            TableSort::Supported(SortSpec {
                order_id: 1,
                keys: vec![SortKey {
                    column: 1,
                    descending: true,
                    nulls_first: false,
                }],
            })
        );
        assert_eq!(
            TableSort::resolve(&schema, &SortOrder::unsorted_order()),
            TableSort::Unsorted
        );
        let bucketed = SortOrder::builder()
            .with_order_id(3)
            .with_sort_field(
                SortField::builder()
                    .source_id(1)
                    .transform(Transform::Bucket(16))
                    .direction(SortDirection::Ascending)
                    .null_order(NullOrder::First)
                    .build(),
            )
            .build(&iceberg)
            .unwrap();
        assert!(matches!(
            TableSort::resolve(&schema, &bucketed),
            TableSort::Unsupported { order_id: 3, .. }
        ));
    }
}
