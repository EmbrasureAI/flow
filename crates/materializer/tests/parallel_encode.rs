//! The parallel Parquet writer must write exactly the bytes of parquet-rs's
//! sequential `AsyncArrowWriter`, across row-group boundaries, byte-limited
//! row groups and nested fields with several leaf columns, and its adaptive
//! policy must keep narrow, dominated and small batches sequential.

use std::sync::Arc;

use arrow_array::builder::{Int32Builder, ListBuilder, MapBuilder, StringBuilder};
use arrow_array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray,
    StructArray,
};
use arrow_schema::{DataType, Field, Fields, Schema};
use iceberg::writer::file_writer::{
    Adaptive, ParallelArrowWriter, parquet_parallel_batches, set_parquet_encode_threads,
};
use parquet::arrow::AsyncArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::{EnabledStatistics, WriterProperties};

const ALWAYS: Adaptive = Adaptive {
    min_batch_bytes: 0,
    min_speedup: 1.0,
};

fn props(max_rows: usize, max_bytes: Option<usize>) -> WriterProperties {
    let builder = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .set_max_row_group_row_count(Some(max_rows))
        .set_statistics_enabled(EnabledStatistics::Page);
    match max_bytes {
        Some(bytes) => builder.set_max_row_group_bytes(Some(bytes)),
        None => builder,
    }
    .build()
}

/// Flat columns of several types, with nulls, plus nested fields whose
/// leaves span several Parquet columns each.
fn batch(rows: usize, seed: i64) -> RecordBatch {
    let ids: ArrayRef = Arc::new(Int64Array::from_iter_values(
        (0..rows as i64).map(|i| seed + i),
    ));
    let mut columns: Vec<(Field, ArrayRef)> =
        vec![(Field::new("id", DataType::Int64, false), ids.clone())];
    for column in 0..14i64 {
        let name = format!("c{column}");
        let value = |i: usize| (i as i64 * 7919 + column * 104_729 + seed) % 100_003;
        let (field, array): (Field, ArrayRef) = match column % 5 {
            0 => (
                Field::new(&name, DataType::Int64, true),
                Arc::new(Int64Array::from_iter(
                    (0..rows).map(|i| (i % 9 != 0).then(|| value(i))),
                )),
            ),
            1 => (
                Field::new(&name, DataType::Int32, false),
                Arc::new(Int32Array::from_iter_values(
                    (0..rows).map(|i| (value(i) % 500) as i32),
                )),
            ),
            2 => (
                Field::new(&name, DataType::Float64, true),
                Arc::new(Float64Array::from_iter(
                    (0..rows).map(|i| (i % 11 != 0).then(|| value(i) as f64 / 3.0)),
                )),
            ),
            3 => (
                Field::new(&name, DataType::Boolean, false),
                Arc::new(BooleanArray::from_iter(
                    (0..rows).map(|i| Some(value(i) % 2 == 0)),
                )),
            ),
            _ => (
                Field::new(&name, DataType::Utf8, true),
                Arc::new(StringArray::from_iter((0..rows).map(|i| {
                    (i % 5 != 0).then(|| format!("value-{}-{}", value(i), "x".repeat(i % 40)))
                }))),
            ),
        };
        columns.push((field, array));
    }
    let point = StructArray::from(vec![
        (
            Arc::new(Field::new("x", DataType::Int64, false)),
            ids.clone(),
        ),
        (
            Arc::new(Field::new("label", DataType::Utf8, true)),
            Arc::new(StringArray::from_iter(
                (0..rows).map(|i| (i % 3 != 0).then(|| format!("p{i}"))),
            )) as ArrayRef,
        ),
    ]);
    columns.push((
        Field::new("point", point.data_type().clone(), false),
        Arc::new(point),
    ));
    let mut list = ListBuilder::new(Int32Builder::new());
    for i in 0..rows {
        if i % 7 == 0 {
            list.append(false);
        } else {
            for value in 0..(i % 4) {
                list.values().append_value((i + value) as i32);
            }
            list.append(true);
        }
    }
    let list = list.finish();
    columns.push((
        Field::new("tags", list.data_type().clone(), true),
        Arc::new(list),
    ));
    let mut map = MapBuilder::new(None, StringBuilder::new(), Int32Builder::new());
    for i in 0..rows {
        for entry in 0..(i % 3) {
            map.keys().append_value(format!("k{entry}"));
            map.values().append_value((i * entry) as i32);
        }
        map.append(true).unwrap();
    }
    let map = map.finish();
    columns.push((
        Field::new("attributes", map.data_type().clone(), false),
        Arc::new(map),
    ));
    let (fields, arrays): (Vec<Field>, Vec<ArrayRef>) = columns.into_iter().unzip();
    RecordBatch::try_new(Arc::new(Schema::new(Fields::from(fields))), arrays).unwrap()
}

async fn sequential(batches: &[RecordBatch], props: WriterProperties) -> Vec<u8> {
    let mut writer =
        AsyncArrowWriter::try_new(Vec::new(), batches[0].schema(), Some(props)).unwrap();
    for batch in batches {
        writer.write(batch).await.unwrap();
    }
    writer.finish().await.unwrap();
    writer.into_inner()
}

async fn parallel(batches: &[RecordBatch], props: WriterProperties, adaptive: Adaptive) -> Vec<u8> {
    let mut writer =
        ParallelArrowWriter::try_new(Vec::new(), batches[0].schema(), props, adaptive).unwrap();
    for batch in batches {
        writer.write(batch).await.unwrap();
    }
    writer.finish().await.unwrap();
    writer.into_inner()
}

#[tokio::test]
async fn parallel_encoding_writes_the_sequential_bytes() {
    set_parquet_encode_threads(4);
    // Uneven batches cross row-group boundaries mid-batch.
    let batches: Vec<_> = [3_000, 700, 5_123, 1, 4_096]
        .into_iter()
        .scan(0i64, |seed, rows| {
            let batch = batch(rows, *seed);
            *seed += rows as i64;
            Some(batch)
        })
        .collect();
    for (max_rows, max_bytes) in [(4_096, None), (1_000, None), (8_192, Some(96 << 10))] {
        let before = parquet_parallel_batches();
        let expected = sequential(&batches, props(max_rows, max_bytes)).await;
        let actual = parallel(&batches, props(max_rows, max_bytes), ALWAYS).await;
        assert!(
            parquet_parallel_batches() > before,
            "the forced policy must encode some batch on helpers"
        );
        assert_eq!(
            actual, expected,
            "max_rows={max_rows} max_bytes={max_bytes:?}"
        );
    }
}

#[test]
fn adaptive_policy_keeps_dominated_and_small_batches_sequential() {
    let adaptive = Adaptive::default();
    // One field holds ~95% of the work, as in a narrow table with a payload.
    assert_eq!(adaptive.helpers_wanted(&[8 << 10, 8 << 10, 300 << 10]), 0);
    // Balanced, but too small to amortize the handoff.
    assert_eq!(adaptive.helpers_wanted(&[4 << 10; 30]), 0);
    // Balanced and large: helpers up to the speedup bound.
    assert_eq!(adaptive.helpers_wanted(&[64 << 10; 30]), 29);
    assert_eq!(
        adaptive.helpers_wanted(&[400 << 10, 100 << 10, 100 << 10, 100 << 10, 100 << 10]),
        1
    );
    assert_eq!(adaptive.helpers_wanted(&[1 << 20]), 0);
}

#[tokio::test]
async fn default_policy_writes_the_sequential_bytes() {
    // Whatever the default policy decides for these batches, the bytes match.
    let batches = vec![batch(2_000, 0), batch(2_000, 2_000)];
    let expected = sequential(&batches, props(4_096, None)).await;
    let actual = parallel(&batches, props(4_096, None), Adaptive::default()).await;
    assert_eq!(actual, expected);
}
