use flow_model::{Column, ColumnType, TableId, TableSchema, Value};
use proptest::prelude::*;

fn schema() -> TableSchema {
    TableSchema {
        table_id: TableId(1),
        version: 1,
        columns: vec![
            Column {
                field_id: 1,
                name: "id".into(),
                data_type: ColumnType::Int64,
                nullable: false,
            },
            Column {
                field_id: 2,
                name: "note".into(),
                data_type: ColumnType::String,
                nullable: true,
            },
        ],
        primary_key: vec![0],
        append_only: false,
    }
}

proptest! {
    #[test]
    fn additive_null_projection_preserves_fingerprints(id: i64, note in proptest::option::of(".{0,80}"), tail in 1usize..8) {
        let old = schema();
        let mut next = old.clone();
        next.version += 1;
        for index in 0..tail {
            next.columns.push(Column { field_id: 3 + index as i32, name: format!("extra_{index}"), data_type: ColumnType::String, nullable: true });
        }
        old.validate_successor(&next).unwrap();
        let row = vec![Value::Int64(id), note.map_or(Value::Null, Value::String)];
        let mut projected = row.clone();
        projected.extend(std::iter::repeat_n(Value::Null, tail));
        prop_assert_eq!(old.fingerprint(&row).unwrap(), next.fingerprint(&projected).unwrap());
        prop_assert_eq!(old.encode_key(&row).unwrap(), next.encode_key(&projected).unwrap());
        *projected.last_mut().unwrap() = Value::String(String::new());
        prop_assert_ne!(old.fingerprint(&row).unwrap(), next.fingerprint(&projected).unwrap());
    }

    #[test]
    fn composite_binary_keys_are_unambiguous(a in proptest::collection::vec(any::<u8>(), 0..32), b in proptest::collection::vec(any::<u8>(), 0..32), split in 0usize..65) {
        let schema = TableSchema { table_id: TableId(1), version: 0,
            columns: vec![Column { field_id: 1, name: "a".into(), data_type: ColumnType::Binary, nullable: false },
                Column { field_id: 2, name: "b".into(), data_type: ColumnType::Binary, nullable: false }],
            primary_key: vec![0, 1], append_only: false };
        let joined: Vec<_> = a.iter().chain(&b).copied().collect();
        let split = split.min(joined.len());
        let left = vec![Value::Binary(a.clone()), Value::Binary(b.clone())];
        let right = vec![Value::Binary(joined[..split].to_vec()), Value::Binary(joined[split..].to_vec())];
        prop_assert_eq!(schema.encode_key(&left).unwrap() == schema.encode_key(&right).unwrap(), left == right);
    }
}

/// The key-only schema PostgreSQL capture previously cloned to encode an old
/// key tuple: its columns are the key columns, in primary-key order.
fn key_schema(schema: &TableSchema) -> TableSchema {
    let mut key_schema = schema.clone();
    key_schema.columns = schema
        .primary_key
        .iter()
        .map(|&index| schema.columns[index].clone())
        .collect();
    key_schema.primary_key = (0..key_schema.columns.len()).collect();
    key_schema
}

fn wide_key_schema() -> TableSchema {
    let column = |field_id, name: &str, data_type| Column {
        field_id,
        name: name.into(),
        data_type,
        nullable: false,
    };
    TableSchema {
        table_id: TableId(3),
        version: 2,
        columns: vec![
            column(1, "note", ColumnType::String),
            column(2, "id", ColumnType::Int64),
            column(3, "tenant", ColumnType::Uuid),
            column(
                4,
                "amount",
                ColumnType::Decimal {
                    precision: 10,
                    scale: 2,
                },
            ),
            column(5, "day", ColumnType::Date),
        ],
        // Out of column order, and skipping non-key columns.
        primary_key: vec![2, 1, 4, 3],
        append_only: false,
    }
}

proptest! {
    #[test]
    fn key_values_encode_exactly_like_the_complete_row_and_key_schema_paths(
        note in ".{0,40}",
        id: i64,
        tenant: [u8; 16],
        unscaled in -9_999_999_999i128..=9_999_999_999,
        day: i32,
    ) {
        let schema = wide_key_schema();
        let row = vec![
            Value::String(note),
            Value::Int64(id),
            Value::Uuid(tenant),
            Value::Decimal { unscaled, scale: 2 },
            Value::Date(day),
        ];
        let values: Vec<_> = schema.primary_key.iter().map(|&index| row[index].clone()).collect();
        let encoded = schema.encode_key_values(&values).unwrap();
        prop_assert_eq!(&encoded, &key_schema(&schema).encode_key(&values).unwrap());
        prop_assert_eq!(&encoded, &schema.encode_key(&row).unwrap());
        prop_assert_eq!(&encoded, &schema.encode_partial_key(&row).unwrap());
    }
}

#[test]
fn key_values_are_validated_against_their_key_columns() {
    let schema = wide_key_schema();
    let valid = vec![
        Value::Uuid([7; 16]),
        Value::Int64(1),
        Value::Date(3),
        Value::Decimal {
            unscaled: 5,
            scale: 2,
        },
    ];
    let key_only = key_schema(&schema);
    assert_eq!(
        schema.encode_key_values(&valid).unwrap(),
        key_only.encode_key(&valid).unwrap()
    );
    let mut invalid = vec![
        valid[..3].to_vec(),
        [valid.clone(), vec![Value::Int64(9)]].concat(),
    ];
    for (index, value) in [
        Value::Null,
        Value::Unchanged,
        Value::Int32(1),
        Value::String("x".into()),
    ]
    .into_iter()
    .enumerate()
    {
        let mut values = valid.clone();
        let position = index % values.len();
        values[position] = value;
        invalid.push(values);
    }
    let mut overflow = valid.clone();
    overflow[3] = Value::Decimal {
        unscaled: 10_000_000_000,
        scale: 2,
    };
    invalid.push(overflow);
    for values in invalid {
        assert!(schema.encode_key_values(&values).is_err(), "{values:?}");
        assert!(key_only.encode_key(&values).is_err(), "{values:?}");
    }
    let mut keyless = schema.clone();
    keyless.primary_key.clear();
    keyless.append_only = true;
    assert!(matches!(
        keyless.encode_key_values(&[]),
        Err(flow_model::ModelError::MissingKey)
    ));
}

#[test]
fn partial_rows_accept_unchanged_values_only_outside_the_key() {
    let schema = schema();
    let partial = vec![Value::Int64(4), Value::Unchanged];
    schema.validate_partial_row(&partial).unwrap();
    assert_eq!(
        schema.encode_partial_key(&partial).unwrap(),
        schema
            .encode_key(&vec![Value::Int64(4), Value::Null])
            .unwrap()
    );
    for invalid in [
        vec![Value::Unchanged, Value::Unchanged],
        vec![Value::Null, Value::Unchanged],
        vec![Value::Int64(4), Value::Int64(5)],
        vec![Value::Int64(4)],
    ] {
        assert!(
            schema.validate_partial_row(&invalid).is_err(),
            "{invalid:?}"
        );
    }
    let mut append_only = schema.clone();
    append_only.append_only = true;
    assert!(append_only.validate_partial_row(&partial).is_err());
}

#[test]
fn key_encoding_has_a_stable_versioned_storage_format() {
    let key = schema()
        .encode_key(&vec![Value::Int64(-7), Value::Null])
        .unwrap();
    assert_eq!(key.0, [1, 3, 255, 255, 255, 255, 255, 255, 255, 249]);
}
