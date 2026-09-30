use flow_iceberg_ext::{RowDeltaAction, SnapshotView};
use flow_materializer::{DataWriter, WriterConfig, iceberg_schema, rows_from_batch};
use flow_model::{Column, ColumnType, OperationId, PgLsn, Row, TableId, TableSchema, Value};
use futures::TryStreamExt;
use iceberg::{
    Catalog, CatalogBuilder, NamespaceIdent, TableCreation,
    memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder},
    spec::FormatVersion,
};
use std::collections::HashMap;

#[tokio::test]
async fn supported_types_round_trip_through_parquet_manifests_and_stock_iceberg_scan() {
    let types = [
        ColumnType::Int64,
        ColumnType::Bool,
        ColumnType::Int32,
        ColumnType::Int64,
        ColumnType::Float64,
        ColumnType::String,
        ColumnType::Binary,
        ColumnType::Date,
        ColumnType::TimestampMicros,
        ColumnType::TimestampTzMicros,
        ColumnType::Uuid,
        ColumnType::Decimal {
            precision: 38,
            scale: 9,
        },
    ];
    let schema = TableSchema {
        table_id: TableId(42),
        version: 0,
        columns: types
            .into_iter()
            .enumerate()
            .map(|(index, data_type)| Column {
                field_id: index as i32 + 1,
                name: format!("field_{index}"),
                data_type,
                nullable: index != 0,
            })
            .collect(),
        primary_key: vec![0],
        append_only: false,
    };
    let large_decimal = 10i128.pow(38) - 1;
    let rows: Vec<Row> = vec![
        vec![
            Value::Int64(1),
            Value::Bool(true),
            Value::Int32(i32::MIN),
            Value::Int64(i64::MAX),
            Value::Float64(f64::from_bits(0x7ff8_0000_0000_0042)),
            Value::String("héllo 🦀\0world".into()),
            Value::Binary(vec![0, 255, 128, 0]),
            Value::Date(-719_162),
            Value::TimestampMicros(-123_456_789),
            Value::TimestampTzMicros(-123_456_789),
            Value::Uuid([0xff; 16]),
            Value::Decimal {
                unscaled: -large_decimal,
                scale: 9,
            },
        ],
        vec![
            Value::Int64(2),
            Value::Bool(false),
            Value::Int32(i32::MAX),
            Value::Int64(i64::MIN),
            Value::Float64(-0.0),
            Value::String(String::new()),
            Value::Binary(Vec::new()),
            Value::Date(20_699),
            Value::TimestampMicros(1_800_000_000_123_456),
            Value::TimestampTzMicros(1_800_000_000_123_456),
            Value::Uuid([0; 16]),
            Value::Decimal {
                unscaled: large_decimal,
                scale: 9,
            },
        ],
        vec![
            Value::Int64(3),
            Value::Bool(true),
            Value::Int32(0),
            Value::Int64(0),
            Value::Float64(f64::INFINITY),
            Value::String("plain".into()),
            Value::Binary(vec![42]),
            Value::Date(0),
            Value::TimestampMicros(0),
            Value::TimestampTzMicros(0),
            Value::Uuid([0x12; 16]),
            Value::Decimal {
                unscaled: 0,
                scale: 9,
            },
        ],
        std::iter::once(Value::Int64(4))
            .chain(std::iter::repeat_n(Value::Null, 11))
            .collect(),
    ];
    let catalog = MemoryCatalogBuilder::default()
        .load(
            "typed-rows",
            HashMap::from([(
                MEMORY_CATALOG_WAREHOUSE.into(),
                "memory://typed-warehouse".into(),
            )]),
        )
        .await
        .unwrap();
    let namespace = NamespaceIdent::new("test".into());
    catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .unwrap();
    let empty = catalog
        .create_table(
            &namespace,
            TableCreation::builder()
                .name("all_types".into())
                .schema(iceberg_schema(&schema).unwrap())
                .format_version(FormatVersion::V2)
                .build(),
        )
        .await
        .unwrap();
    let mut writer = DataWriter::new(
        empty.file_io().clone(),
        empty.metadata().location(),
        &OperationId("typed-rows".into()),
        schema.clone(),
        0,
        WriterConfig {
            row_group_rows: 2,
            ..Default::default()
        },
    )
    .unwrap();
    let borrowed = rows[..2].iter().collect::<Vec<_>>();
    writer.write(&borrowed, PgLsn(10)).await.unwrap();
    writer.write(&rows[2..], PgLsn(10)).await.unwrap();
    let files = writer.close().await.unwrap();
    assert_eq!(files.len(), 1);
    assert_eq!(files[0].record_count(), rows.len() as u64);
    let bytes = empty
        .file_io()
        .new_input(files[0].file_path())
        .unwrap()
        .read()
        .await
        .unwrap();
    let parquet =
        parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(bytes).unwrap();
    let uuid = parquet.parquet_schema().column(10);
    assert_eq!(
        uuid.physical_type(),
        parquet::basic::Type::FIXED_LEN_BYTE_ARRAY
    );
    assert_eq!(uuid.type_length(), 16);
    assert_eq!(
        uuid.logical_type_ref(),
        Some(&parquet::basic::LogicalType::Uuid)
    );
    assert_eq!(uuid.self_type().get_basic_info().id(), 11);
    for column in &schema.columns {
        assert_eq!(files[0].value_counts().get(&column.field_id), Some(&4));
        assert_eq!(
            files[0].null_value_counts().get(&column.field_id),
            Some(&u64::from(column.nullable))
        );
    }
    let committed = RowDeltaAction::new(&empty, "typed-rows")
        .add_data_files(files.clone())
        .commit(&catalog, &empty)
        .await
        .unwrap();
    let view = SnapshotView::current(&committed.table).await.unwrap();
    assert_eq!(view.live_files[files[0].file_path()].data_file, files[0]);
    let scan = committed.table.scan().build().unwrap();
    let mut batches = scan.to_arrow().await.unwrap();
    let mut actual = Vec::new();
    while let Some(batch) = batches.try_next().await.unwrap() {
        let fields = batch.schema();
        for column in &schema.columns {
            assert!(
                fields
                    .fields()
                    .iter()
                    .any(|field| field.metadata().get("PARQUET:field_id")
                        == Some(&column.field_id.to_string()))
            );
        }
        actual.extend(rows_from_batch(&schema, &batch).unwrap());
    }
    actual.sort_by_key(|row| match row[0] {
        Value::Int64(id) => id,
        _ => unreachable!(),
    });
    assert_eq!(actual.len(), rows.len());
    for (expected, actual) in rows.iter().zip(&actual) {
        assert_eq!(
            schema.encode_key(expected).unwrap(),
            schema.encode_key(actual).unwrap()
        );
        assert_eq!(
            schema.fingerprint(expected).unwrap(),
            schema.fingerprint(actual).unwrap()
        );
        // Fingerprints normalize NaNs and signed zero; assert every other value
        // directly to catch a mutually consistent encoder/decoder mistake.
        for (expected, actual) in expected.iter().zip(actual) {
            if let (Value::Float64(expected), Value::Float64(actual)) = (expected, actual) {
                assert!(expected == actual || (expected.is_nan() && actual.is_nan()));
            } else {
                assert_eq!(expected, actual);
            }
        }
    }
}

#[tokio::test]
async fn truncated_row_group_bounds_cannot_hide_string_or_binary_matches() {
    use iceberg::{expr::Reference, spec::Datum};
    let temp = tempfile::tempdir().unwrap();
    let catalog = flow_testkit::catalog(temp.path()).await;
    let mut schema = flow_testkit::schema(73);
    schema.columns.push(Column {
        field_id: 3,
        name: "bytes".into(),
        data_type: ColumnType::Binary,
        nullable: true,
    });
    let table = flow_testkit::table(&catalog, &schema).await;
    // Both orderings must invalidate bounds: the exact group can precede or
    // follow an inexact group. NULL groups do not invalidate exact non-null bounds.
    for (case, values) in [
        vec!["m".to_owned(), "a".repeat(100), "z".repeat(100)],
        vec!["z".repeat(100), "a".repeat(100), "m".to_owned()],
    ]
    .into_iter()
    .enumerate()
    {
        let rows = values
            .iter()
            .enumerate()
            .map(|(id, value)| {
                vec![
                    Value::Int64(id as i64),
                    Value::String(value.clone()),
                    Value::Binary(value.as_bytes().to_vec()),
                ]
            })
            .collect::<Vec<_>>();
        let mut writer = DataWriter::new(
            table.file_io().clone(),
            table.metadata().location(),
            &OperationId(format!("bounds-{case}")),
            schema.clone(),
            0,
            WriterConfig {
                row_group_rows: 1,
                ..Default::default()
            },
        )
        .unwrap();
        writer.write(&rows, PgLsn(1)).await.unwrap();
        let files = writer.close().await.unwrap();
        assert_eq!(files.len(), 1);
        for id in [2, 3] {
            assert!(!files[0].lower_bounds().contains_key(&id));
            assert!(!files[0].upper_bounds().contains_key(&id));
        }
        let head = catalog.load_table(table.identifier()).await.unwrap();
        let committed = RowDeltaAction::new(&head, format!("bounds-{case}"))
            .add_data_files(files)
            .commit(&catalog, &head)
            .await
            .unwrap()
            .table;
        for value in [&values[0], &values[1], &values[2]] {
            for predicate in [
                Reference::new("value").equal_to(Datum::string(value)),
                Reference::new("bytes").equal_to(Datum::binary(value.bytes())),
            ] {
                let mut stream = committed
                    .scan()
                    .with_filter(predicate)
                    .build()
                    .unwrap()
                    .to_arrow()
                    .await
                    .unwrap();
                let mut actual = Vec::new();
                while let Some(batch) = stream.try_next().await.unwrap() {
                    actual.extend(rows_from_batch(&schema, &batch).unwrap());
                }
                assert_eq!(actual.len(), case + 1);
                assert!(
                    actual
                        .iter()
                        .all(|row| row[1] == Value::String(value.clone()))
                );
            }
        }
    }
}

#[test]
fn nested_uuid_schema_round_trip_preserves_ids_docs_and_fixed_binary() {
    use iceberg::{
        arrow::{arrow_schema_to_schema, schema_to_arrow_schema},
        spec::{ListType, MapType, NestedField, PrimitiveType, Schema, StructType, Type},
    };
    use std::sync::Arc;
    let uuid = Type::Primitive(PrimitiveType::Uuid);
    let schema = Schema::builder()
        .with_fields(vec![
            Arc::new(NestedField::required(
                1,
                "ids",
                Type::List(ListType {
                    element_field: Arc::new(
                        NestedField::list_element(2, uuid.clone(), false).with_doc("UUID element"),
                    ),
                }),
            )),
            Arc::new(NestedField::required(
                3,
                "map",
                Type::Map(MapType {
                    key_field: Arc::new(NestedField::map_key_element(4, uuid.clone())),
                    value_field: Arc::new(NestedField::map_value_element(5, uuid.clone(), false)),
                }),
            )),
            Arc::new(NestedField::required(
                6,
                "record",
                Type::Struct(StructType::new(vec![
                    Arc::new(NestedField::optional(7, "id", uuid)),
                    Arc::new(NestedField::optional(
                        8,
                        "opaque",
                        Type::Primitive(PrimitiveType::Fixed(16)),
                    )),
                ])),
            )),
        ])
        .build()
        .unwrap();
    let arrow = schema_to_arrow_schema(&schema).unwrap();
    assert_eq!(arrow_schema_to_schema(&arrow).unwrap(), schema);
}
