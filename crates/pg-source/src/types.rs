//! PostgreSQL wire types remain at the source boundary. Iceberg-compatible
//! conversions produce ordinary model values; durable row/key formats do not change.
use crate::{Column, Error, Relation, Result};
use flow_model::{ColumnType, TableSchema, Value};
use std::collections::BTreeMap;
use tokio_postgres::{
    GenericClient,
    types::{Kind, Type},
};

#[derive(Clone, Debug, Default)]
pub struct TypeRegistry(BTreeMap<u32, SourceType>);
#[derive(Clone, Debug)]
enum SourceType {
    Enum,
    Vector,
    Domain { base: u32, modifier: i32 },
    Array(u32),
}

impl TypeRegistry {
    /// Resolve once per catalog observation, never once per row. Limit nesting
    /// and reject unsupported user types rather than guessing their wire format.
    pub async fn fetch(client: &(impl GenericClient + Sync), relation: &Relation) -> Result<Self> {
        let mut types = Self::default();
        let mut pending: Vec<_> = relation.columns.iter().map(|c| (c.type_oid, 0)).collect();
        while let Some((oid, depth)) = pending.pop() {
            if depth > 32 {
                return Err(Error::Config("PostgreSQL type nesting exceeds 32"));
            }
            if types.0.contains_key(&oid) {
                continue;
            }
            if let Some(typ) = Type::from_oid(oid) {
                if let Kind::Array(element) = typ.kind() {
                    pending.push((element.oid(), depth + 1));
                }
                continue;
            }
            let row = client.query_opt("SELECT t.typtype::text, t.typbasetype, t.typtypmod, t.typelem, t.typname, EXISTS (SELECT 1 FROM pg_catalog.pg_depend d JOIN pg_catalog.pg_extension e ON e.oid=d.refobjid WHERE d.classid='pg_catalog.pg_type'::regclass AND d.objid=t.oid AND d.refclassid='pg_catalog.pg_extension'::regclass AND d.deptype='e' AND e.extname='vector') FROM pg_catalog.pg_type t WHERE t.oid=$1", &[&oid]).await?
                .ok_or(Error::Config("PostgreSQL source type disappeared"))?;
            let kind: String = row.get(0);
            let resolved = match kind.as_str() {
                "b" if row.get::<_, String>(4) == "vector" && row.get::<_, bool>(5) => {
                    SourceType::Vector
                }
                "e" => SourceType::Enum,
                "d" => {
                    let base = row.get(1);
                    pending.push((base, depth + 1));
                    SourceType::Domain {
                        base,
                        modifier: row.get(2),
                    }
                }
                "b" if row.get::<_, u32>(3) != 0 => {
                    let element = row.get(3);
                    pending.push((element, depth + 1));
                    SourceType::Array(element)
                }
                _ => return Err(Error::Config("unsupported PostgreSQL user-defined type")),
            };
            types.0.insert(oid, resolved);
        }
        Ok(types)
    }

    pub fn extend(&mut self, other: Self) {
        self.0.extend(other.0);
    }

    fn base(&self, oid: u32, modifier: i32) -> Result<(u32, i32)> {
        let (mut oid, mut modifier) = (oid, modifier);
        for _ in 0..=32 {
            match self.0.get(&oid) {
                Some(SourceType::Domain {
                    base,
                    modifier: domain_modifier,
                }) => {
                    oid = *base;
                    if modifier < 0 {
                        modifier = *domain_modifier;
                    }
                }
                _ => return Ok((oid, modifier)),
            }
        }
        Err(Error::Config(
            "cyclic or excessive PostgreSQL domain nesting",
        ))
    }

    fn element(&self, oid: u32) -> Option<u32> {
        if let Some(SourceType::Array(element)) = self.0.get(&oid) {
            return Some(*element);
        }
        Type::from_oid(oid).and_then(|t| match t.kind() {
            Kind::Array(e) => Some(e.oid()),
            _ => None,
        })
    }

    /// The Flow column type `init` expects for a source column.
    pub fn column_type(&self, column: &Column) -> Result<ColumnType> {
        self.mapped_type(column.type_oid, column.type_modifier, 0)
    }

    fn mapped_type(&self, oid: u32, modifier: i32, depth: usize) -> Result<ColumnType> {
        if depth > 32 {
            return Err(Error::Config("PostgreSQL type nesting exceeds 32"));
        }
        let (oid, modifier) = self.base(oid, modifier)?;
        if let Some(element) = self.element(oid) {
            // Prove element support even when the source array is empty.
            self.mapped_type(element, modifier, depth + 1)?;
            return Ok(ColumnType::String);
        }
        if matches!(
            self.0.get(&oid),
            Some(SourceType::Enum | SourceType::Vector)
        ) {
            return Ok(ColumnType::String);
        }
        crate::schema::column_type(oid, modifier)
    }

    pub fn validate_relation(&self, schema: &TableSchema, relation: &Relation) -> Result<()> {
        if !schema.append_only {
            relation.validate_mutable()?;
        }
        if schema.columns.len() != relation.columns.len() {
            return Err(Error::Config(
                "source column count changed; reconcile schema before capture",
            ));
        }
        for (index, (expected, actual)) in schema.columns.iter().zip(&relation.columns).enumerate()
        {
            let (oid, _) = self.base(actual.type_oid, actual.type_modifier)?;
            // Fixed-width builtins and enums cannot contain external TOAST pointers.
            // Domains inherit their base's representation. Deliberately exclude all
            // varlena types, including bounded varchar and columns SET STORAGE PLAIN:
            // a storage change does not rewrite previously toasted values.
            if !schema.append_only
                && relation.replica_identity == b'd'
                && !matches!(
                    oid,
                    16 | 20 | 21 | 23 | 700 | 701 | 1082 | 1083 | 1114 | 1184 | 2950
                )
                && !matches!(self.0.get(&oid), Some(SourceType::Enum))
            {
                return Err(Error::DefaultIdentity(format!(
                    "table {}.{} column {} has a variable-width or unproven storage representation; REPLICA IDENTITY FULL is required (DEFAULT supports only fixed-width replicated columns and a primary key)",
                    relation.namespace, relation.name, actual.name
                )));
            }
            let inferred = self.column_type(actual)?;
            let string_override =
                expected.data_type == ColumnType::String && matches!(oid, 2950 | 1700);
            if expected.name != actual.name
                || (expected.data_type != inferred
                    && !string_override
                    && !(expected.data_type == ColumnType::Uuid && oid == 2950))
            {
                return Err(Error::Config(
                    "source column name or type differs from configured schema",
                ));
            }
            if schema.primary_key.contains(&index)
                && (self.element(oid).is_some()
                    || matches!(oid, 114 | 3802)
                    || matches!(self.0.get(&oid), Some(SourceType::Vector)))
            {
                return Err(Error::Config(
                    "JSON, array and vector primary keys are unsupported by the string mapping",
                ));
            }
        }
        Ok(())
    }

    pub(crate) fn decode(
        &self,
        kind: &ColumnType,
        oid: u32,
        bytes: &[u8],
        binary: bool,
    ) -> Result<Value> {
        let (oid, _) = self.base(oid, -1)?;
        if *kind == ColumnType::String {
            if oid == 1083 {
                return Ok(Value::String(time_string(bytes, binary)?));
            }
            if matches!(self.0.get(&oid), Some(SourceType::Vector)) {
                return Ok(Value::String(vector_json(bytes, binary)?.to_string()));
            }
            if let Some(element) = self.element(oid) {
                if !binary {
                    return Err(Error::Config("array capture requires binary pgoutput"));
                }
                return Ok(Value::String(self.array_json(element, bytes)?.to_string()));
            }
            if oid == 2950 {
                let uuid = if binary {
                    uuid::Uuid::from_slice(bytes)
                } else {
                    uuid::Uuid::parse_str(text(bytes)?)
                }
                .map_err(value_error)?;
                return Ok(Value::String(uuid.hyphenated().to_string()));
            }
            if matches!(oid, 114 | 3802) {
                let bytes = if binary && oid == 3802 {
                    bytes
                        .strip_prefix(&[1])
                        .ok_or(Error::Config("unsupported JSONB wire version"))?
                } else {
                    bytes
                };
                // Fivetran-style JSON normalization: last duplicate object key wins.
                // arbitrary_precision preserves JSON numbers without f64 conversion.
                let json: serde_json::Value = serde_json::from_slice(bytes).map_err(value_error)?;
                return Ok(Value::String(json.to_string()));
            }
            if oid == 1700 {
                return Ok(Value::String(if binary {
                    numeric_binary(bytes)?
                } else {
                    numeric_text(text(bytes)?)?
                }));
            }
        }
        if binary {
            crate::capture::decode_binary(kind, oid, bytes)
        } else {
            crate::capture::decode_text(kind, oid, bytes)
        }
    }

    fn array_json(&self, element: u32, bytes: &[u8]) -> Result<serde_json::Value> {
        use fallible_iterator::FallibleIterator;
        let array = postgres_protocol::types::array_from_sql(bytes).map_err(value_error)?;
        if array.element_type() != element {
            return Err(Error::Config("array element wire type changed"));
        }
        let dimensions: Vec<_> = array.dimensions().collect().map_err(value_error)?;
        if dimensions.len() > 6 || dimensions.iter().any(|d| d.len <= 0) {
            return Err(Error::Config("PostgreSQL array exceeds six dimensions"));
        }
        let kind = self.mapped_type(element, -1, 0)?;
        let (base, _) = self.base(element, -1)?;
        let mut values = array.values();
        fn nested(
            types: &TypeRegistry,
            dims: &[postgres_protocol::types::ArrayDimension],
            values: &mut postgres_protocol::types::ArrayValues<'_>,
            kind: &ColumnType,
            element: u32,
            base: u32,
        ) -> Result<serde_json::Value> {
            let mut output = Vec::new();
            if let Some((dim, rest)) = dims.split_first() {
                // No reservation from untrusted dimension lengths. Wire framing
                // and the enclosing source-message limit bound decoded values.
                for _ in 0..dim.len {
                    output.push(if !rest.is_empty() {
                        nested(types, rest, values, kind, element, base)?
                    } else {
                        match values
                            .next()
                            .map_err(value_error)?
                            .ok_or(Error::Config("short array payload"))?
                        {
                            None => serde_json::Value::Null,
                            Some(raw) => {
                                if base == 1700 {
                                    let number = numeric_binary(raw)?;
                                    serde_json::from_str(&number)
                                        .unwrap_or(serde_json::Value::String(number))
                                } else if matches!(types.0.get(&base), Some(SourceType::Vector)) {
                                    vector_json(raw, true)?
                                } else if let Some(nested_element) = types.element(base) {
                                    types.array_json(nested_element, raw)?
                                } else if base == 2950 {
                                    serde_json::Value::String(
                                        uuid::Uuid::from_slice(raw)
                                            .map_err(value_error)?
                                            .to_string(),
                                    )
                                } else {
                                    json_value(types.decode(kind, element, raw, true)?, base)?
                                }
                            }
                        }
                    });
                }
            }
            Ok(serde_json::Value::Array(output))
        }
        let result = nested(self, &dimensions, &mut values, &kind, element, base)?;
        if values.next().map_err(value_error)?.is_some() {
            return Err(Error::Config("extra array elements"));
        }
        Ok(result)
    }
}

// Keep time at the source boundary: Iceberg TIME is not supported by Athena,
// and PostgreSQL's valid 24:00:00 cannot be represented by chrono::NaiveTime.
fn time_string(bytes: &[u8], binary: bool) -> Result<String> {
    const DAY_MICROS: i64 = 86_400_000_000;
    let micros = if binary {
        i64::from_be_bytes(
            bytes
                .try_into()
                .map_err(|_| Error::Config("invalid time wire length"))?,
        )
    } else {
        let value = text(bytes)?;
        let (whole, fraction) = value.split_once('.').unwrap_or((value, ""));
        let parts: Vec<_> = whole.split(':').collect();
        if parts.len() != 3
            || parts
                .iter()
                .any(|p| p.len() != 2 || !p.bytes().all(|c| c.is_ascii_digit()))
            || fraction.len() > 6
            || !fraction.bytes().all(|c| c.is_ascii_digit())
        {
            return Err(Error::Config("invalid PostgreSQL time text"));
        }
        let hour: i64 = parts[0].parse().map_err(value_error)?;
        let minute: i64 = parts[1].parse().map_err(value_error)?;
        let second: i64 = parts[2].parse().map_err(value_error)?;
        if minute > 59 || second > 59 {
            return Err(Error::Config("invalid PostgreSQL time fields"));
        }
        let subsecond = if fraction.is_empty() {
            0
        } else {
            fraction.parse::<i64>().map_err(value_error)? * 10i64.pow(6 - fraction.len() as u32)
        };
        (hour * 3600 + minute * 60 + second) * 1_000_000 + subsecond
    };
    if !(0..=DAY_MICROS).contains(&micros) {
        return Err(Error::Config("PostgreSQL time outside 00:00:00..24:00:00"));
    }
    Ok(format_time(micros))
}

fn format_time(micros: i64) -> String {
    format!(
        "{:02}:{:02}:{:02}.{:06}",
        micros / 3_600_000_000,
        micros / 60_000_000 % 60,
        micros / 1_000_000 % 60,
        micros % 1_000_000
    )
}

fn vector_json(bytes: &[u8], binary: bool) -> Result<serde_json::Value> {
    // pgvector vector_send: int16 dimensions, int16 reserved zero, float4[].
    // Promoting f32 to f64 is exact and keeps the stored value in JSON numbers.
    let values: Vec<f32> = if binary {
        if bytes.len() < 4 || bytes[2..4] != [0, 0] {
            return Err(Error::Config("invalid pgvector wire header"));
        }
        let dimensions = usize::from(u16::from_be_bytes(bytes[..2].try_into().unwrap()));
        if !(1..=16_000).contains(&dimensions) || bytes.len() != 4 + dimensions * 4 {
            return Err(Error::Config("invalid pgvector wire dimensions"));
        }
        bytes[4..]
            .chunks_exact(4)
            .map(|v| f32::from_be_bytes(v.try_into().unwrap()))
            .collect()
    } else {
        let values: Vec<f32> = serde_json::from_slice(bytes).map_err(value_error)?;
        if !(1..=16_000).contains(&values.len()) {
            return Err(Error::Config("invalid pgvector text dimensions"));
        }
        values
    };
    if values.iter().any(|v| !v.is_finite()) {
        return Err(Error::Config("pgvector elements must be finite"));
    }
    Ok(serde_json::Value::Array(
        values
            .into_iter()
            .map(|v| serde_json::json!(f64::from(v)))
            .collect(),
    ))
}

fn text(bytes: &[u8]) -> Result<&str> {
    std::str::from_utf8(bytes).map_err(value_error)
}
fn value_error(error: impl std::fmt::Display) -> Error {
    Error::Value(error.to_string())
}

fn json_value(value: Value, oid: u32) -> Result<serde_json::Value> {
    use serde_json::{Value as J, json};
    Ok(match value {
        Value::Null => J::Null,
        Value::Bool(v) => json!(v),
        Value::Int32(v) => json!(v),
        Value::Int64(v) => json!(v),
        Value::Float64(v) => {
            if v.is_finite() {
                json!(v)
            } else {
                J::String(v.to_string())
            }
        }
        Value::String(v) if matches!(oid, 114 | 3802) => {
            serde_json::from_str(&v).map_err(value_error)?
        }
        Value::String(v) => J::String(v),
        Value::Binary(v) => {
            use base64::Engine;
            J::String(base64::engine::general_purpose::STANDARD.encode(v))
        }
        Value::Uuid(v) => J::String(uuid::Uuid::from_bytes(v).to_string()),
        Value::Date(v) => J::String(crate::capture::date_string(v)),
        Value::TimestampMicros(v) | Value::TimestampTzMicros(v) => {
            const DAY_MICROS: i64 = 86_400_000_000;
            // Even i64 microsecond extremes fit in i32 days. Euclidean division
            // keeps the time of day positive for timestamps before the Unix epoch.
            let date = crate::capture::date_string(v.div_euclid(DAY_MICROS) as i32);
            let time = format_time(v.rem_euclid(DAY_MICROS));
            let zone = if oid == 1184 { "Z" } else { "" };
            J::String(format!("{date}T{time}{zone}"))
        }
        Value::Decimal { unscaled, scale } => serde_json::from_str(
            &bigdecimal::BigDecimal::new(unscaled.into(), i64::from(scale)).to_string(),
        )
        .map_err(value_error)?,
    })
}

fn numeric_text(value: &str) -> Result<String> {
    if matches!(value, "NaN" | "Infinity" | "-Infinity") {
        return Ok(value.into());
    }
    let number: bigdecimal::BigDecimal = value.parse().map_err(value_error)?;
    Ok(number.normalized().to_plain_string())
}

fn numeric_binary(bytes: &[u8]) -> Result<String> {
    use bigdecimal::{BigDecimal, num_bigint::BigInt};
    if bytes.len() < 8 || !bytes.len().is_multiple_of(2) {
        return Err(Error::Config("invalid numeric wire header"));
    }
    let count = usize::from(u16::from_be_bytes(bytes[0..2].try_into().unwrap()));
    let weight = i16::from_be_bytes(bytes[2..4].try_into().unwrap());
    let sign = u16::from_be_bytes(bytes[4..6].try_into().unwrap());
    if count * 2 + 8 != bytes.len() {
        return Err(Error::Config("invalid numeric wire length"));
    }
    match sign {
        0xC000 | 0xD000 | 0xF000 if count == 0 => {
            return Ok(match sign {
                0xC000 => "NaN",
                0xD000 => "Infinity",
                _ => "-Infinity",
            }
            .into());
        }
        0 | 0x4000 => {}
        _ => return Err(Error::Config("invalid numeric wire sign")),
    }
    let mut number = BigInt::from(0);
    for pair in bytes[8..].chunks_exact(2) {
        let digit = u16::from_be_bytes(pair.try_into().unwrap());
        if digit >= 10000 {
            return Err(Error::Config("invalid numeric wire digit"));
        }
        number = number * 10000u32 + digit;
    }
    if sign == 0x4000 {
        number = -number;
    }
    Ok(
        BigDecimal::new(number, 4 * (count as i64 - i64::from(weight) - 1))
            .normalized()
            .to_plain_string(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Cell, decode_row};
    use flow_model::{Column as ModelColumn, TableId};

    fn array(oid: u32, dimensions: &[(i32, i32)], values: &[Option<&[u8]>]) -> Vec<u8> {
        let mut bytes = Vec::new();
        for value in [dimensions.len() as i32, 1, oid as i32] {
            bytes.extend(value.to_be_bytes());
        }
        for (length, lower) in dimensions {
            bytes.extend(length.to_be_bytes());
            bytes.extend(lower.to_be_bytes());
        }
        for value in values {
            bytes.extend(value.map_or(-1, |v| v.len() as i32).to_be_bytes());
            if let Some(value) = value {
                bytes.extend(*value);
            }
        }
        bytes
    }

    #[test]
    fn time_text_binary_and_wrappers_preserve_microseconds_and_end_of_day() {
        let mut types = TypeRegistry::default();
        types.0.insert(
            20001,
            SourceType::Domain {
                base: 1083,
                modifier: 6,
            },
        );
        for (source, micros, expected) in [
            ("00:00:00", 0i64, "00:00:00.000000"),
            ("12:34:56.1", 45_296_100_000, "12:34:56.100000"),
            ("23:59:59.999999", 86_399_999_999, "23:59:59.999999"),
            ("24:00:00", 86_400_000_000, "24:00:00.000000"),
        ] {
            for oid in [1083, 20001] {
                for (bytes, binary) in [
                    (micros.to_be_bytes().to_vec(), true),
                    (source.as_bytes().to_vec(), false),
                ] {
                    assert_eq!(
                        types
                            .decode(&ColumnType::String, oid, &bytes, binary)
                            .unwrap(),
                        Value::String(expected.into())
                    );
                }
            }
        }
        for invalid in [
            "24:00:00.000001",
            "25:00:00",
            "12:60:00",
            "12:00:60",
            "12:00:00.0000001",
            "12:00:00+01",
            "-1:00:00",
        ] {
            assert!(time_string(invalid.as_bytes(), false).is_err(), "{invalid}");
        }
        for invalid in [-1i64, 86_400_000_001] {
            assert!(time_string(&invalid.to_be_bytes(), true).is_err());
        }
        assert!(time_string(&[0; 7], true).is_err());
        assert_eq!(
            types
                .array_json(
                    20001,
                    &array(
                        20001,
                        &[(2, 1)],
                        &[Some(&86_400_000_000i64.to_be_bytes()), None]
                    )
                )
                .unwrap(),
            serde_json::json!(["24:00:00.000000", null])
        );
    }

    fn vector(values: &[f32]) -> Vec<u8> {
        let mut bytes = (values.len() as u16).to_be_bytes().to_vec();
        bytes.extend([0, 0]);
        for value in values {
            bytes.extend(value.to_be_bytes());
        }
        bytes
    }

    #[test]
    fn vector_numbers_roundtrip_exactly_with_domain_and_array_wrappers() {
        let mut types = TypeRegistry::default();
        types.0.insert(20010, SourceType::Vector);
        types.0.insert(
            20011,
            SourceType::Domain {
                base: 20010,
                modifier: 4,
            },
        );
        let values = [0.1f32, -0.0, f32::MAX, f32::MIN_POSITIVE, f32::from_bits(1)];
        let binary = vector(&values);
        let text = serde_json::to_vec(&values).unwrap();
        for oid in [20010, 20011] {
            assert_eq!(types.mapped_type(oid, -1, 0).unwrap(), ColumnType::String);
            assert_eq!(
                types
                    .decode(&ColumnType::String, oid, &binary, true)
                    .unwrap(),
                types
                    .decode(&ColumnType::String, oid, &text, false)
                    .unwrap()
            );
        }
        let json = vector_json(&binary, true).unwrap();
        for (number, expected) in json.as_array().unwrap().iter().zip(values) {
            assert_eq!(
                (number.as_f64().unwrap() as f32).to_bits(),
                expected.to_bits()
            );
        }
        assert_eq!(
            types
                .array_json(20011, &array(20011, &[(2, 1)], &[Some(&binary), None]))
                .unwrap(),
            serde_json::json!([json, null])
        );
        for bad in [
            vec![],
            vec![0, 0, 0, 0],
            vec![0, 1, 0, 1, 0, 0, 0, 0],
            vec![0, 1, 0, 0],
            vector(&[f32::NAN]),
            vector(&[f32::INFINITY]),
        ] {
            assert!(vector_json(&bad, true).is_err());
        }
        for bad in ["[]", "[null]", "[[1]]", "[1e100]", "[NaN]"] {
            assert!(vector_json(bad.as_bytes(), false).is_err());
        }
    }

    #[test]
    fn snapshot_binary_and_cdc_text_have_identical_string_keys_and_json_values() {
        let types = TypeRegistry::default();
        let uuid = uuid::Uuid::parse_str("ffffffff-ffff-4fff-8fff-ffffffffffff").unwrap();
        for (oid, binary, text) in [
            (2950, uuid.as_bytes().to_vec(), uuid.to_string()),
            (
                3802,
                b"\x01{\"n\":123456789012345678901234567890.123456789,\"a\":1,\"a\":2}".to_vec(),
                "{\"a\":2,\"n\":123456789012345678901234567890.123456789}".into(),
            ),
            (
                1700,
                vec![0, 2, 0, 0, 0x40, 0, 0, 4, 0, 12, 0x0d, 0x48],
                "-12.3400".into(),
            ),
        ] {
            assert_eq!(
                types
                    .decode(&ColumnType::String, oid, &binary, true)
                    .unwrap(),
                types
                    .decode(&ColumnType::String, oid, text.as_bytes(), false)
                    .unwrap()
            );
        }
        assert_eq!(numeric_text("-0.0000").unwrap(), "0");
        assert_eq!(
            numeric_text("12345678901234567890123456789012345678901234567890.00001").unwrap(),
            "12345678901234567890123456789012345678901234567890.00001"
        );
        assert_eq!(
            numeric_binary(&[0, 0, 0, 0, 0xd0, 0, 0, 0]).unwrap(),
            "Infinity"
        );
    }

    #[test]
    fn date_arrays_support_wide_scalar_dates() {
        let types = TypeRegistry::default();
        // PostgreSQL date_send encodes days since 2000-01-01.
        for (source, days, json) in [
            ("300000-01-01", 108_842_265i32, "+300000-01-01"),
            ("300000-02-29", 108_842_324, "+300000-02-29"),
            ("300000-03-01", 108_842_325, "+300000-03-01"),
            ("4714-11-24 BC", -2_451_545, "-4713-11-24"),
            ("5874897-12-31", 2_145_031_948, "+5874897-12-31"),
        ] {
            let raw = days.to_be_bytes();
            let expected = Value::Date(days + 10_957);
            assert_eq!(
                types
                    .decode(&ColumnType::Date, 1082, source.as_bytes(), false)
                    .unwrap(),
                expected
            );
            assert_eq!(
                types.decode(&ColumnType::Date, 1082, &raw, true).unwrap(),
                expected
            );
            assert_eq!(
                types
                    .decode(
                        &ColumnType::String,
                        1182,
                        &array(1082, &[(2, 1)], &[Some(&raw), None]),
                        true
                    )
                    .unwrap(),
                Value::String(format!(r#"["{json}",null]"#))
            );
        }
    }

    #[test]
    fn timestamp_arrays_support_wide_scalar_timestamps() {
        wide_timestamp_array(ColumnType::TimestampMicros, 1114, 1115, "", "");
    }

    #[test]
    fn timestamptz_arrays_support_wide_scalar_timestamps() {
        wide_timestamp_array(ColumnType::TimestampTzMicros, 1184, 1185, "+00", "Z");
    }

    fn wide_timestamp_array(kind: ColumnType, oid: u32, array_oid: u32, zone: &str, suffix: &str) {
        let types = TypeRegistry::default();
        let micros = 101_537_415i64 * 86_400_000_000;
        let raw = micros.to_be_bytes();
        let expected = if kind == ColumnType::TimestampTzMicros {
            Value::TimestampTzMicros(micros + 946_684_800_000_000)
        } else {
            Value::TimestampMicros(micros + 946_684_800_000_000)
        };
        let source = format!("280000-01-01 00:00:00{zone}");
        assert_eq!(
            types.decode(&kind, oid, source.as_bytes(), false).unwrap(),
            expected
        );
        assert_eq!(types.decode(&kind, oid, &raw, true).unwrap(), expected);
        assert_eq!(
            types
                .decode(
                    &ColumnType::String,
                    array_oid,
                    &array(oid, &[(2, 1)], &[Some(&raw), None]),
                    true
                )
                .unwrap(),
            Value::String(format!(r#"["+280000-01-01T00:00:00.000000{suffix}",null]"#))
        );
    }

    #[test]
    fn temporal_arrays_preserve_existing_json_format() {
        let types = TypeRegistry::default();
        let epoch = chrono::NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
        for (year, month, day) in [
            (-4713, 11, 24),
            (-1, 12, 31),
            (0, 2, 29),
            (1, 1, 1),
            (1900, 3, 1),
            (1969, 12, 31),
            (1970, 1, 1),
            (2000, 2, 29),
            (9999, 12, 31),
            (10000, 1, 1),
            (262142, 12, 31),
        ] {
            let date = chrono::NaiveDate::from_ymd_opt(year, month, day).unwrap();
            let days = i32::try_from((date - epoch).num_days()).unwrap();
            assert_eq!(
                types
                    .decode(
                        &ColumnType::String,
                        1182,
                        &array(1082, &[(1, 1)], &[Some(&days.to_be_bytes())]),
                        true
                    )
                    .unwrap(),
                Value::String(serde_json::json!([date.to_string()]).to_string())
            );
            for time in [0, 45_296_123_456, 86_399_999_999] {
                let micros = i64::from(days) * 86_400_000_000 + time;
                let dt =
                    chrono::DateTime::from_timestamp_micros(micros + 946_684_800_000_000).unwrap();
                for (oid, array_oid, expected) in [
                    (
                        1114,
                        1115,
                        dt.naive_utc().format("%Y-%m-%dT%H:%M:%S%.6f").to_string(),
                    ),
                    (
                        1184,
                        1185,
                        dt.to_rfc3339_opts(chrono::SecondsFormat::Micros, true),
                    ),
                ] {
                    assert_eq!(
                        types
                            .decode(
                                &ColumnType::String,
                                array_oid,
                                &array(oid, &[(1, 1)], &[Some(&micros.to_be_bytes())]),
                                true
                            )
                            .unwrap(),
                        Value::String(serde_json::json!([expected]).to_string())
                    );
                }
            }
        }
    }

    #[test]
    fn temporal_arrays_preserve_scalar_rejections() {
        let types = TypeRegistry::default();
        for (source, days) in [
            ("-infinity", i32::MIN),
            ("infinity", i32::MAX),
            ("4714-11-23 BC", -2_451_546),
            ("5874898-01-01", 2_145_031_949),
        ] {
            let raw = days.to_be_bytes();
            assert!(
                types
                    .decode(&ColumnType::Date, 1082, source.as_bytes(), false)
                    .is_err()
            );
            let scalar = types
                .decode(&ColumnType::Date, 1082, &raw, true)
                .unwrap_err();
            let array = types
                .decode(
                    &ColumnType::String,
                    1182,
                    &array(1082, &[(1, 1)], &[Some(&raw)]),
                    true,
                )
                .unwrap_err();
            assert_eq!(array.to_string(), scalar.to_string());
        }
        for (kind, oid, array_oid) in [
            (ColumnType::TimestampMicros, 1114, 1115),
            (ColumnType::TimestampTzMicros, 1184, 1185),
        ] {
            for micros in [
                i64::MIN,
                i64::MAX,
                -211_813_488_000_000_001,
                9_223_371_331_199_999_999,
            ] {
                let raw = micros.to_be_bytes();
                let scalar = types.decode(&kind, oid, &raw, true).unwrap_err();
                let array = types
                    .decode(
                        &ColumnType::String,
                        array_oid,
                        &array(oid, &[(1, 1)], &[Some(&raw)]),
                        true,
                    )
                    .unwrap_err();
                assert_eq!(array.to_string(), scalar.to_string());
            }
        }
    }

    #[test]
    fn arrays_preserve_nested_values_nulls_and_large_numeric_values() {
        let types = TypeRegistry::default();
        let bytes = array(
            25,
            &[(2, -3), (2, 4)],
            &[
                Some(b"NULL"),
                None,
                Some(b"quote\"\\"),
                Some("世界".as_bytes()),
            ],
        );
        assert_eq!(
            types.array_json(25, &bytes).unwrap(),
            serde_json::json!([["NULL", null], ["quote\"\\", "世界"]])
        );
        assert_eq!(
            types.array_json(25, &array(25, &[], &[])).unwrap(),
            serde_json::json!([])
        );
        let bytes = array(
            3802,
            &[(2, 1)],
            &[Some(b"\x01null"), Some(b"\x01{\"x\":9223372036854775808}")],
        );
        assert_eq!(
            types.array_json(3802, &bytes).unwrap().to_string(),
            "[null,{\"x\":9223372036854775808}]"
        );
        let mut truncated = array(23, &[(1, 1)], &[Some(&1i32.to_be_bytes())]);
        truncated.pop();
        assert!(types.array_json(23, &truncated).is_err());
        assert!(types.array_json(25, &array(23, &[], &[])).is_err());
        assert!(
            types
                .array_json(25, &array(25, &[(i32::MAX, 1), (0, 1)], &[]))
                .is_err()
        );
    }

    #[test]
    fn arrays_of_array_domains_preserve_nested_json_values() {
        let mut types = TypeRegistry::default();
        types.0.insert(
            20001,
            SourceType::Domain {
                base: 1007,
                modifier: -1,
            },
        );
        let inner = array(23, &[(2, 1)], &[Some(&42i32.to_be_bytes()), None]);
        let empty = array(23, &[], &[]);
        let outer = array(20001, &[(3, 1)], &[Some(&inner), None, Some(&empty)]);
        assert_eq!(
            types.array_json(20001, &outer).unwrap(),
            serde_json::json!([[42, null], null, []])
        );
    }

    #[test]
    fn enum_domains_use_underlying_wire_types_and_reject_ambiguous_keys() {
        let mut types = TypeRegistry::default();
        types.0.insert(20000, SourceType::Enum);
        types.0.insert(
            20001,
            SourceType::Domain {
                base: 23,
                modifier: -1,
            },
        );
        types.0.insert(
            20002,
            SourceType::Domain {
                base: 20001,
                modifier: -1,
            },
        );
        assert_eq!(
            types
                .decode(&ColumnType::String, 20000, b"ready", true)
                .unwrap(),
            Value::String("ready".into())
        );
        assert_eq!(
            types
                .decode(&ColumnType::Int32, 20002, &42i32.to_be_bytes(), true)
                .unwrap(),
            Value::Int32(42)
        );
        let mut schema = TableSchema {
            table_id: TableId(1),
            version: 1,
            columns: vec![ModelColumn {
                field_id: 1,
                name: "key".into(),
                data_type: ColumnType::String,
                nullable: false,
            }],
            primary_key: vec![0],
            append_only: false,
        };
        let mut relation = Relation {
            id: 1,
            namespace: "public".into(),
            name: "t".into(),
            replica_identity: b'f',
            columns: vec![Column {
                name: "key".into(),
                type_oid: 2950,
                type_modifier: -1,
                identity: true,
            }],
        };
        let row = decode_row(
            &schema,
            &relation,
            &vec![Cell::Text("FFFFFFFF-FFFF-4FFF-8FFF-FFFFFFFFFFFF".into())],
        )
        .unwrap();
        assert_eq!(
            row,
            vec![Value::String("ffffffff-ffff-4fff-8fff-ffffffffffff".into())]
        );
        let bytes = bincode::serialize(&row).unwrap();
        assert_eq!(
            schema.encode_key(&row).unwrap(),
            schema
                .encode_key(&bincode::deserialize(&bytes).unwrap())
                .unwrap()
        );
        types.0.insert(20010, SourceType::Vector);
        types.0.insert(
            20011,
            SourceType::Domain {
                base: 20010,
                modifier: 3,
            },
        );
        for oid in [1009, 20010, 20011] {
            relation.columns[0].type_oid = oid;
            assert!(types.validate_relation(&schema, &relation).is_err());
        }
        relation.columns[0].type_oid = 1083;
        assert!(types.validate_relation(&schema, &relation).is_ok());
        schema.primary_key.clear();
        schema.append_only = true;
        assert!(types.validate_relation(&schema, &relation).is_ok());
    }
}
