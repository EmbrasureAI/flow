use crate::{Error, Result};
use bytes::Bytes;
use flow_model::PgLsn;
use std::collections::{HashMap, HashSet};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Cell {
    Null,
    UnchangedToast,
    Text(Bytes),
    Binary(Bytes),
}
pub type Tuple = Vec<Cell>;

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Column {
    pub name: String,
    pub type_oid: u32,
    pub type_modifier: i32,
    pub identity: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Relation {
    pub id: u32,
    pub namespace: String,
    pub name: String,
    pub replica_identity: u8,
    pub columns: Vec<Column>,
}

impl Relation {
    pub fn validate_schema(&self, schema: &flow_model::TableSchema) -> Result<()> {
        crate::capture::validate_relation(schema, self)
    }
    pub fn validate_mutable(&self) -> Result<()> {
        if self.replica_identity != b'f' {
            return Err(Error::ReplicaIdentity(self.id));
        }
        Ok(())
    }

    /// Call for every replacement row, including append-only inserts. An
    /// unchanged marker is neither NULL nor a serializable placeholder.
    pub fn validate_row(&self, tuple: &Tuple) -> Result<()> {
        if tuple.len() != self.columns.len() {
            return Err(Error::Protocol("tuple column count differs from relation"));
        }
        if tuple.iter().any(|c| matches!(c, Cell::UnchangedToast)) {
            return Err(Error::UnchangedToast(self.id));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SourceEvent {
    /// pgoutput Begin contains the transaction's FINAL LSN, not a begin LSN.
    Begin {
        xid: u32,
        final_lsn: PgLsn,
        commit_timestamp_micros: i64,
    },
    Commit {
        xid: u32,
        commit_lsn: PgLsn,
        end_lsn: PgLsn,
        commit_timestamp_micros: i64,
    },
    Relation(Relation),
    Insert {
        xid: u32,
        subxid: u32,
        relation: u32,
        row: Tuple,
    },
    Update {
        xid: u32,
        subxid: u32,
        relation: u32,
        old: Option<Tuple>,
        old_is_key: bool,
        row: Tuple,
    },
    Delete {
        xid: u32,
        subxid: u32,
        relation: u32,
        old: Tuple,
        old_is_key: bool,
    },
    Truncate {
        xid: u32,
        subxid: u32,
        relations: Vec<u32>,
        cascade: bool,
        restart_identity: bool,
    },
    StreamStart {
        xid: u32,
        first: bool,
    },
    StreamStop,
    Abort {
        xid: u32,
        subxid: u32,
    },
    /// Origin/type metadata does not mutate a replicated table.
    Metadata,
}

pub struct Decoder {
    max_message_bytes: usize,
    normal_xid: Option<u32>,
    stream_xid: Option<u32>,
    streamed: HashSet<u32>,
    relations: HashMap<u32, Relation>,
}

impl Decoder {
    pub fn new(max_message_bytes: usize) -> Self {
        Self {
            max_message_bytes,
            normal_xid: None,
            stream_xid: None,
            streamed: HashSet::new(),
            relations: HashMap::new(),
        }
    }
    pub fn relation(&self, id: u32) -> Option<&Relation> {
        self.relations.get(&id)
    }

    pub fn decode(&mut self, bytes: Bytes) -> Result<SourceEvent> {
        if bytes.len() > self.max_message_bytes {
            return Err(Error::Protocol("message exceeds configured limit"));
        }
        let mut cursor = Cursor { bytes, offset: 0 };
        let tag = cursor.u8()?;
        let event = match tag {
            b'B' => {
                if self.normal_xid.is_some() || self.stream_xid.is_some() {
                    return Err(Error::Protocol("nested Begin"));
                }
                let final_lsn = PgLsn(cursor.u64()?);
                let commit_timestamp_micros = cursor.timestamp()?;
                let xid = cursor.u32()?;
                self.normal_xid = Some(xid);
                SourceEvent::Begin {
                    xid,
                    final_lsn,
                    commit_timestamp_micros,
                }
            }
            b'C' => {
                let xid = self
                    .normal_xid
                    .take()
                    .ok_or(Error::Protocol("Commit without Begin"))?;
                parse_commit(&mut cursor, xid)?
            }
            b'S' => {
                if self.stream_xid.is_some() || self.normal_xid.is_some() {
                    return Err(Error::Protocol("nested StreamStart"));
                }
                let xid = cursor.u32()?;
                let first = match cursor.u8()? {
                    0 => false,
                    1 => true,
                    _ => return Err(Error::Protocol("invalid stream first flag")),
                };
                if first {
                    if self.streamed.len() >= 1024 {
                        return Err(Error::Protocol("too many streamed transactions"));
                    }
                    if !self.streamed.insert(xid) {
                        return Err(Error::Protocol("duplicate first StreamStart"));
                    }
                } else if !self.streamed.contains(&xid) {
                    return Err(Error::Protocol("continuation of unknown transaction"));
                }
                self.stream_xid = Some(xid);
                SourceEvent::StreamStart { xid, first }
            }
            b'E' => {
                self.stream_xid
                    .take()
                    .ok_or(Error::Protocol("StreamStop outside stream"))?;
                SourceEvent::StreamStop
            }
            b'c' => {
                if self.stream_xid.is_some() || self.normal_xid.is_some() {
                    return Err(Error::Protocol("StreamCommit inside active segment"));
                }
                let xid = cursor.u32()?;
                if !self.streamed.remove(&xid) {
                    return Err(Error::Protocol("commit of unknown streamed transaction"));
                }
                parse_commit(&mut cursor, xid)?
            }
            b'A' => {
                let xid = cursor.u32()?;
                let subxid = cursor.u32()?;
                if self.stream_xid.is_some()
                    || self.normal_xid.is_some()
                    || !self.streamed.contains(&xid)
                {
                    return Err(Error::Protocol("abort outside stopped stream"));
                }
                if xid == subxid {
                    self.streamed.remove(&xid);
                }
                SourceEvent::Abort { xid, subxid }
            }
            b'R' => {
                if self.stream_xid.is_some() {
                    cursor.u32()?;
                }
                let id = cursor.u32()?;
                let namespace = cursor.string()?;
                let name = cursor.string()?;
                let replica_identity = cursor.u8()?;
                if !matches!(replica_identity, b'd' | b'n' | b'f' | b'i') {
                    return Err(Error::Protocol("unknown replica identity"));
                }
                let count = cursor.u16()?;
                let mut columns = Vec::with_capacity(count as usize);
                for _ in 0..count {
                    let flags = cursor.u8()?;
                    if flags > 1 {
                        return Err(Error::Protocol("unknown relation column flags"));
                    }
                    columns.push(Column {
                        identity: flags == 1,
                        name: cursor.string()?,
                        type_oid: cursor.u32()?,
                        type_modifier: cursor.u32()? as i32,
                    });
                }
                let relation = Relation {
                    id,
                    namespace,
                    name,
                    replica_identity,
                    columns,
                };
                self.relations.insert(id, relation.clone());
                SourceEvent::Relation(relation)
            }
            b'I' | b'U' | b'D' | b'T' => {
                let (xid, subxid) = self.transaction(&mut cursor)?;
                if tag == b'T' {
                    let count = cursor.u32()? as usize;
                    let flags = cursor.u8()?;
                    if flags & !3 != 0 || count > cursor.remaining() / 4 {
                        return Err(Error::Protocol("invalid truncate"));
                    }
                    let mut relations = Vec::with_capacity(count);
                    for _ in 0..count {
                        relations.push(cursor.u32()?);
                    }
                    SourceEvent::Truncate {
                        xid,
                        subxid,
                        relations,
                        cascade: flags & 1 != 0,
                        restart_identity: flags & 2 != 0,
                    }
                } else {
                    let relation = cursor.u32()?;
                    if !self.relations.contains_key(&relation) {
                        return Err(Error::Protocol("mutation precedes Relation"));
                    }
                    let tuple_tag = cursor.u8()?;
                    match tag {
                        b'I' if tuple_tag == b'N' => SourceEvent::Insert {
                            xid,
                            subxid,
                            relation,
                            row: cursor.tuple()?,
                        },
                        b'U' => {
                            let old = match tuple_tag {
                                b'O' | b'K' => Some(cursor.tuple()?),
                                b'N' => None,
                                _ => return Err(Error::Protocol("invalid update tuple tag")),
                            };
                            if old.is_some() && cursor.u8()? != b'N' {
                                return Err(Error::Protocol("update lacks new tuple"));
                            }
                            SourceEvent::Update {
                                xid,
                                subxid,
                                relation,
                                old,
                                old_is_key: tuple_tag == b'K',
                                row: cursor.tuple()?,
                            }
                        }
                        b'D' if matches!(tuple_tag, b'O' | b'K') => SourceEvent::Delete {
                            xid,
                            subxid,
                            relation,
                            old: cursor.tuple()?,
                            old_is_key: tuple_tag == b'K',
                        },
                        _ => return Err(Error::Protocol("invalid mutation tuple tag")),
                    }
                }
            }
            b'Y' => {
                if self.stream_xid.is_some() {
                    cursor.u32()?;
                }
                cursor.u32()?;
                cursor.string()?;
                cursor.string()?;
                SourceEvent::Metadata
            }
            b'O' => {
                cursor.u64()?;
                cursor.string()?;
                SourceEvent::Metadata
            }
            _ => return Err(Error::Unsupported(tag)),
        };
        if cursor.remaining() != 0 {
            return Err(Error::Protocol("trailing bytes"));
        }
        Ok(event)
    }

    fn transaction(&self, cursor: &mut Cursor) -> Result<(u32, u32)> {
        if let Some(xid) = self.stream_xid {
            return Ok((xid, cursor.u32()?));
        }
        let xid = self
            .normal_xid
            .ok_or(Error::Protocol("mutation outside transaction"))?;
        Ok((xid, xid))
    }
}

fn parse_commit(cursor: &mut Cursor, xid: u32) -> Result<SourceEvent> {
    if cursor.u8()? != 0 {
        return Err(Error::Protocol("unknown commit flags"));
    }
    let commit_lsn = PgLsn(cursor.u64()?);
    let end_lsn = PgLsn(cursor.u64()?);
    if end_lsn <= commit_lsn {
        return Err(Error::Protocol("commit end LSN does not follow commit LSN"));
    }
    Ok(SourceEvent::Commit {
        xid,
        commit_lsn,
        end_lsn,
        commit_timestamp_micros: cursor.timestamp()?,
    })
}

struct Cursor {
    bytes: Bytes,
    offset: usize,
}
impl Cursor {
    fn remaining(&self) -> usize {
        self.bytes.len() - self.offset
    }
    fn take(&mut self, count: usize) -> Result<Bytes> {
        if count > self.remaining() {
            return Err(Error::Protocol("truncated message"));
        }
        let bytes = self.bytes.slice(self.offset..self.offset + count);
        self.offset += count;
        Ok(bytes)
    }
    fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }
    fn u16(&mut self) -> Result<u16> {
        Ok(u16::from_be_bytes(
            self.take(2)?.as_ref().try_into().unwrap(),
        ))
    }
    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_be_bytes(
            self.take(4)?.as_ref().try_into().unwrap(),
        ))
    }
    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_be_bytes(
            self.take(8)?.as_ref().try_into().unwrap(),
        ))
    }
    fn i64(&mut self) -> Result<i64> {
        Ok(self.u64()? as i64)
    }
    fn timestamp(&mut self) -> Result<i64> {
        self.i64()?
            .checked_add(946_684_800_000_000)
            .ok_or(Error::Protocol(
                "Postgres timestamp cannot be represented as Unix microseconds",
            ))
    }
    fn string(&mut self) -> Result<String> {
        let length = self.bytes[self.offset..]
            .iter()
            .position(|&b| b == 0)
            .ok_or(Error::Protocol("unterminated string"))?;
        let bytes = self.take(length)?;
        self.u8()?;
        String::from_utf8(bytes.to_vec()).map_err(|_| Error::Protocol("invalid UTF-8 identifier"))
    }
    fn tuple(&mut self) -> Result<Tuple> {
        let count = self.u16()? as usize;
        if count > self.remaining() {
            return Err(Error::Protocol("invalid tuple field count"));
        }
        let mut cells = Vec::with_capacity(count);
        for _ in 0..count {
            cells.push(match self.u8()? {
                b'n' => Cell::Null,
                b'u' => Cell::UnchangedToast,
                tag @ (b't' | b'b') => {
                    let len = self.u32()? as usize;
                    let bytes = self.take(len)?;
                    if tag == b't' {
                        Cell::Text(bytes)
                    } else {
                        Cell::Binary(bytes)
                    }
                }
                _ => return Err(Error::Protocol("unknown tuple field encoding")),
            });
        }
        Ok(cells)
    }
}
