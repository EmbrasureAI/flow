use flow_model::{Value, fingerprint};

#[test]
fn all_value_variants_keep_the_persisted_v1_fingerprint() {
    // Golden derived independently from literal v1 tags, big-endian values,
    // length-delimited UTF-8/binary bytes, and BLAKE3's first 16 bytes.
    let row = vec![
        Value::Null,
        Value::Bool(true),
        Value::Bool(false),
        Value::Int32(i32::MIN),
        Value::Int64(i64::MIN),
        Value::Float64(1.5),
        Value::Float64(f64::from_bits(0xfff0_0000_0000_0001)),
        Value::Float64(-0.0),
        Value::String("é\0abc".into()),
        Value::Binary(vec![0, 1, 127, 255]),
        Value::Date(-123),
        Value::TimestampMicros(-987_654_321),
        Value::Uuid(std::array::from_fn(|i| i as u8)),
        Value::Decimal {
            unscaled: -12345,
            scale: 3,
        },
        Value::TimestampTzMicros(987_654_321),
        Value::Null,
        Value::Null,
    ];
    assert_eq!(
        fingerprint(&row),
        [
            0xed, 0x54, 0x34, 0x00, 0xb4, 0x60, 0xbc, 0x0b, 0x6d, 0x9b, 0xa2, 0x41, 0x10, 0x43,
            0x87, 0x2d
        ]
    );
}

#[test]
fn wide_values_and_nullable_tail_keep_the_persisted_fingerprint() {
    let mut row = vec![
        Value::String("é".repeat(40_000)),
        Value::Null,
        Value::Binary((0..=255).cycle().take(75_052).collect()),
    ];
    // Independent v1 fixture: 1, 5, 80000u64 BE, UTF-8 bytes, 0, 6,
    // 75052u64 BE, binary bytes. Nullable successor fields append no bytes.
    let expected = [
        0x61, 0x5f, 0x2c, 0x8e, 0xd1, 0x5a, 0x44, 0xf9, 0x02, 0xf7, 0x51, 0x98, 0x82, 0x49, 0x2b,
        0x8c,
    ];
    assert_eq!(fingerprint(&row), expected);
    row.extend([Value::Null, Value::Null]);
    assert_eq!(fingerprint(&row), expected);
}

#[test]
fn empty_null_zero_and_nan_normalization_stays_stable() {
    let empty = [
        0x48, 0xfc, 0x72, 0x1f, 0xbb, 0xc1, 0x72, 0xe0, 0x92, 0x5f, 0xa2, 0x7a, 0xf1, 0x67, 0x1d,
        0xe2,
    ];
    assert_eq!(fingerprint(&vec![]), empty);
    assert_eq!(fingerprint(&vec![Value::Null, Value::Null]), empty);
    for value in [0.0, -0.0] {
        assert_eq!(
            fingerprint(&vec![Value::Float64(value)]),
            [
                0x2f, 0x24, 0x34, 0x26, 0xcd, 0x65, 0x32, 0x67, 0x24, 0x83, 0x69, 0x41, 0xd1, 0x76,
                0x09, 0xf5
            ]
        );
    }
    for bits in [
        0x7ff8_0000_0000_0000,
        0x7ff0_0000_0000_0001,
        0xfff0_0000_0000_0001,
    ] {
        assert_eq!(
            fingerprint(&vec![Value::Float64(f64::from_bits(bits))]),
            [
                0x71, 0x4b, 0x60, 0x3f, 0xa1, 0x6d, 0x5f, 0x24, 0x21, 0xc8, 0x47, 0xf6, 0x9a, 0x56,
                0x8e, 0x70
            ]
        );
    }
}
