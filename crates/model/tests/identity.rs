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

#[test]
fn key_encoding_has_a_stable_versioned_storage_format() {
    let key = schema()
        .encode_key(&vec![Value::Int64(-7), Value::Null])
        .unwrap();
    assert_eq!(key.0, [1, 3, 255, 255, 255, 255, 255, 255, 255, 249]);
}
