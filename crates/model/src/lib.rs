//! Stable, storage-independent identities and row values shared by the pipeline.
//!
//! Timestamps are microseconds since the Unix epoch; dates are days since it.
//! PostgreSQL's epoch conversion belongs at the source boundary.
use serde::{Deserialize, Serialize};
use std::{collections::HashSet, fmt};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct SourceId(pub String);
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct TableId(pub u32);
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
pub struct PgLsn(pub u64);
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct FileId(pub String);
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct OperationId(pub String);
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct PrimaryKey(pub Vec<u8>);

impl fmt::Display for PgLsn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:X}/{:X}", self.0 >> 32, self.0 & 0xffff_ffff)
    }
}
impl std::str::FromStr for PgLsn {
    type Err = ModelError;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let (hi, lo) = value.split_once('/').ok_or(ModelError::InvalidLsn)?;
        let hi = u32::from_str_radix(hi, 16).map_err(|_| ModelError::InvalidLsn)?;
        let lo = u32::from_str_radix(lo, 16).map_err(|_| ModelError::InvalidLsn)?;
        Ok(Self((u64::from(hi) << 32) | u64::from(lo)))
    }
}
impl OperationId {
    /// Source identity must change when a slot is intentionally reinitialized.
    pub fn epoch(source: &SourceId, table: TableId, first: PgLsn, last: PgLsn) -> Self {
        let mut h = blake3::Hasher::new();
        h.update(b"embrasure-flow/epoch/v1\0");
        h.update(&(source.0.len() as u64).to_be_bytes());
        h.update(source.0.as_bytes());
        h.update(&table.0.to_be_bytes());
        h.update(&first.0.to_be_bytes());
        h.update(&last.0.to_be_bytes());
        Self(h.finalize().to_hex().to_string())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Value {
    Null,
    Bool(bool),
    Int32(i32),
    Int64(i64),
    Float64(f64),
    String(String),
    Binary(Vec<u8>),
    Date(i32),
    TimestampMicros(i64),
    Uuid([u8; 16]),
    Decimal {
        unscaled: i128,
        scale: u8,
    },
    /// An absolute instant, stored as UTC microseconds since the Unix epoch.
    TimestampTzMicros(i64),
}
pub type Row = Vec<Value>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ColumnType {
    Bool,
    Int32,
    Int64,
    Float64,
    String,
    Binary,
    Date,
    TimestampMicros,
    Uuid,
    Decimal { precision: u8, scale: u8 },
    TimestampTzMicros,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Column {
    pub field_id: i32,
    pub name: String,
    pub data_type: ColumnType,
    pub nullable: bool,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableSchema {
    pub table_id: TableId,
    pub version: u32,
    pub columns: Vec<Column>,
    /// Column offsets in source primary-key order, not field IDs.
    pub primary_key: Vec<usize>,
    pub append_only: bool,
}
#[derive(Debug, Error)]
pub enum ModelError {
    #[error("invalid PostgreSQL LSN")]
    InvalidLsn,
    #[error("invalid schema: {0}")]
    InvalidSchema(String),
    #[error("invalid row: {0}")]
    InvalidRow(String),
    #[error("primary key is required")]
    MissingKey,
    #[error("invalid transaction mutation counts")]
    InvalidMutationCounts,
}
impl TableSchema {
    pub fn validate(&self) -> Result<(), ModelError> {
        use std::collections::HashSet;
        let mut ids = HashSet::new();
        let mut names = HashSet::new();
        if self.columns.is_empty() {
            return Err(ModelError::InvalidSchema("empty columns".into()));
        }
        for col in &self.columns {
            if col.field_id <= 0
                || col.field_id >= 2_147_483_445
                || !ids.insert(col.field_id)
                || col.name.is_empty()
                || !names.insert(&col.name)
            {
                return Err(ModelError::InvalidSchema(
                    "field IDs and names must be unique and valid".into(),
                ));
            }
            if let ColumnType::Decimal { precision, scale } = col.data_type
                && (precision == 0 || precision > 38 || scale > precision)
            {
                return Err(ModelError::InvalidSchema(
                    "decimal requires 0 <= scale <= precision <= 38".into(),
                ));
            }
        }
        let mut keys = HashSet::new();
        for key in &self.primary_key {
            let col = self
                .columns
                .get(*key)
                .ok_or_else(|| ModelError::InvalidSchema("key column out of bounds".into()))?;
            if !keys.insert(*key) || col.nullable || matches!(col.data_type, ColumnType::Float64) {
                return Err(ModelError::InvalidSchema(
                    "key columns must be unique, non-nullable and non-floating".into(),
                ));
            }
        }
        if !self.append_only && self.primary_key.is_empty() {
            return Err(ModelError::MissingKey);
        }
        Ok(())
    }
    pub fn validate_row(&self, row: &Row) -> Result<(), ModelError> {
        if row.len() != self.columns.len() {
            return Err(ModelError::InvalidRow("column count differs".into()));
        }
        for (col, value) in self.columns.iter().zip(row) {
            let valid = match (&col.data_type, value) {
                (_, Value::Null) => col.nullable,
                (ColumnType::Bool, Value::Bool(_))
                | (ColumnType::Int32, Value::Int32(_))
                | (ColumnType::Int64, Value::Int64(_))
                | (ColumnType::Float64, Value::Float64(_))
                | (ColumnType::String, Value::String(_))
                | (ColumnType::Binary, Value::Binary(_))
                | (ColumnType::Date, Value::Date(_))
                | (ColumnType::TimestampMicros, Value::TimestampMicros(_))
                | (ColumnType::TimestampTzMicros, Value::TimestampTzMicros(_))
                | (ColumnType::Uuid, Value::Uuid(_)) => true,
                (
                    ColumnType::Decimal { precision, scale },
                    Value::Decimal {
                        unscaled,
                        scale: actual,
                    },
                ) => scale == actual && unscaled.unsigned_abs() < 10u128.pow(u32::from(*precision)),
                _ => false,
            };
            if !valid {
                return Err(ModelError::InvalidRow(format!(
                    "type or nullability mismatch for {}",
                    col.name
                )));
            }
        }
        Ok(())
    }
    pub fn encode_key(&self, row: &Row) -> Result<PrimaryKey, ModelError> {
        self.validate_row(row)?;
        if self.primary_key.is_empty() {
            return Err(ModelError::MissingKey);
        }
        let mut bytes = vec![1]; // canonical format version
        for idx in &self.primary_key {
            encode_value(&row[*idx], |chunk| bytes.extend_from_slice(chunk));
        }
        Ok(PrimaryKey(bytes))
    }
    pub fn fingerprint(&self, row: &Row) -> Result<[u8; 16], ModelError> {
        self.validate_row(row)?;
        Ok(fingerprint(row))
    }
    /// Project a historical row into a compatible nullable-column successor.
    /// Existing fields and key bytes retain their identity and ordering.
    pub fn project_row(&self, mut row: Row, target: &Self) -> Result<Row, ModelError> {
        self.validate_row(&row)?;
        if self != target {
            self.validate_successor(target)?;
        }
        row.resize(target.columns.len(), Value::Null);
        Ok(row)
    }
    /// Only additive nullable columns are safe without an explicit migration.
    pub fn validate_successor(&self, next: &Self) -> Result<(), ModelError> {
        next.validate()?;
        if next.table_id != self.table_id
            || next.version <= self.version
            || next.primary_key != self.primary_key
            || next.append_only != self.append_only
            || !next.columns.starts_with(&self.columns)
            || next.columns[self.columns.len()..]
                .iter()
                .any(|c| !c.nullable)
        {
            return Err(ModelError::InvalidSchema(
                "only additive nullable schema evolution is supported".into(),
            ));
        }
        Ok(())
    }
}

/// Canonical, typed, length-delimited encoding. Never concatenate unframed keys.
fn encode_value(value: &Value, mut emit: impl FnMut(&[u8])) {
    match value {
        Value::Null => emit(&[0]),
        Value::Bool(v) => {
            emit(&[1]);
            emit(&[u8::from(*v)]);
        }
        Value::Int32(v) => {
            emit(&[2]);
            emit(&v.to_be_bytes());
        }
        Value::Int64(v) => {
            emit(&[3]);
            emit(&v.to_be_bytes());
        }
        Value::Float64(v) => {
            emit(&[4]);
            let bits = if v.is_nan() {
                f64::NAN.to_bits()
            } else if *v == 0.0 {
                0
            } else {
                v.to_bits()
            };
            emit(&bits.to_be_bytes());
        }
        Value::String(v) => {
            emit(&[5]);
            emit(&(v.len() as u64).to_be_bytes());
            emit(v.as_bytes());
        }
        Value::Binary(v) => {
            emit(&[6]);
            emit(&(v.len() as u64).to_be_bytes());
            emit(v);
        }
        Value::Date(v) => {
            emit(&[7]);
            emit(&v.to_be_bytes());
        }
        Value::TimestampMicros(v) => {
            emit(&[8]);
            emit(&v.to_be_bytes());
        }
        Value::Uuid(v) => {
            emit(&[9]);
            emit(v);
        }
        Value::Decimal { unscaled, scale } => {
            emit(&[10]);
            emit(&[*scale]);
            emit(&unscaled.to_be_bytes());
        }
        Value::TimestampTzMicros(v) => {
            emit(&[11]);
            emit(&v.to_be_bytes());
        }
    }
}
pub fn fingerprint(row: &Row) -> [u8; 16] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(&[1]);
    // Schema evolution only appends nullable columns. Their implicit nulls must
    // fingerprint identically before and after an old file is projected through
    // the new schema. Interior nulls still encode their position.
    let end = row
        .iter()
        .rposition(|value| !matches!(value, Value::Null))
        .map_or(0, |index| index + 1);
    for value in &row[..end] {
        encode_value(value, |chunk| {
            hasher.update(chunk);
        });
    }
    let mut result = [0; 16];
    result.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
    result
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Mutation {
    pub table_id: TableId,
    pub schema_version: u32,
    pub kind: MutationKind,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MutationKind {
    Insert { row: Row },
    Update { old_key: PrimaryKey, row: Row },
    Delete { key: PrimaryKey },
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JournalChunkRef {
    pub segment: u64,
    pub offset: u64,
    pub length: u32,
}
/// A constant-size handle to a transaction's checksummed journal range.
/// Chunk frames are streamed from disk; no per-chunk metadata is held here.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct JournalChunks {
    pub first: Option<JournalChunkRef>,
    pub last: Option<JournalChunkRef>,
    pub count: u64,
    pub payload_bytes: u64,
    pub checksum: u32,
}
impl JournalChunks {
    pub fn len(&self) -> u64 {
        self.count
    }
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }
    pub fn payload_bytes(&self) -> u64 {
        self.payload_bytes
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableSchemaVersion {
    pub table_id: TableId,
    pub version: u32,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableMutationCount {
    pub table_id: TableId,
    /// Source INSERT, UPDATE or DELETE events, before collapse. A key-changing
    /// UPDATE counts once; its two index changes have their own batch limits.
    pub mutations: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceTransaction {
    pub source_id: SourceId,
    pub xid: u32,
    pub begin_lsn: PgLsn,
    pub commit_lsn: PgLsn,
    /// End of the COMMIT record: the only LSN suitable for acknowledgement.
    pub end_lsn: PgLsn,
    pub commit_timestamp_micros: i64,
    pub schema_versions: Vec<TableSchemaVersion>,
    pub affected_tables: Vec<TableId>,
    pub mutation_chunks: JournalChunks,
    /// Sorted by table ID, with one entry per affected table, including zeroes.
    /// None identifies a legacy terminal; it must never be treated as zero rows.
    #[serde(default)]
    pub table_mutation_counts: Option<Vec<TableMutationCount>>,
}
impl SourceTransaction {
    pub fn mutation_count(&self, table: TableId) -> Option<u64> {
        let counts = self.table_mutation_counts.as_ref()?;
        let index = counts
            .binary_search_by_key(&table, |count| count.table_id)
            .ok()?;
        Some(counts[index].mutations)
    }

    pub fn validate_mutation_counts(&self) -> Result<(), ModelError> {
        if let Some(counts) = &self.table_mutation_counts {
            let affected: HashSet<_> = self.affected_tables.iter().copied().collect();
            if counts.len() != self.affected_tables.len()
                || affected.len() != self.affected_tables.len()
                || counts
                    .windows(2)
                    .any(|pair| pair[0].table_id >= pair[1].table_id)
                || counts
                    .iter()
                    .any(|count| !affected.contains(&count.table_id))
                || counts
                    .iter()
                    .try_fold(0u64, |total, count| total.checked_add(count.mutations))
                    .is_none()
            {
                return Err(ModelError::InvalidMutationCounts);
            }
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RowLocation {
    pub data_file_id: FileId,
    pub row_position: u64,
    pub data_sequence_number: i64,
    pub spec_id: i32,
    pub partition: Vec<u8>,
    pub source_commit_lsn: PgLsn,
    pub row_version: u64,
    pub row_fingerprint: [u8; 16],
}
