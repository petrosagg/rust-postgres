use std::collections::HashMap;
use std::io;

use byteorder::{BigEndian, ReadBytesExt};
use bytes::buf::Reader as BufReader;
use bytes::{Buf, Bytes};
use memchr::memchr;

use crate::{Lsn, Oid};
use crate::message::backend::Parse;
use crate::replication::DecodingPlugin;

#[derive(Clone)]
pub struct PgOutput {
    publications: String,
}

impl PgOutput {
    pub fn new(publications: Vec<String>) -> Self {
        Self {
            publications: publications.join(","),
        }
    }
}

impl DecodingPlugin for PgOutput {
    type Message = LogicalReplicationMessage;

    fn name(&self) -> &str {
        "pgoutput"
    }

    fn options(&self) -> HashMap<String, String> {
        let mut opts = HashMap::new();
        // Currently there is only one version
        opts.insert("proto_version".into(), "1".into());
        opts.insert("publication_names".into(), self.publications.clone());
        opts
    }
}

// message tags
const BEGIN_TAG: u8 = b'B';
const COMMIT_TAG: u8 = b'C';
const ORIGIN_TAG: u8 = b'O';
const RELATION_TAG: u8 = b'R';
const TYPE_TAG: u8 = b'Y';
const INSERT_TAG: u8 = b'I';
const UPDATE_TAG: u8 = b'U';
const DELETE_TAG: u8 = b'D';
const TRUNCATE_TAG: u8 = b'T';
const TUPLE_NEW_TAG: u8 = b'N';
const TUPLE_KEY_TAG: u8 = b'K';
const TUPLE_OLD_TAG: u8 = b'O';
const TUPLE_DATA_NULL_TAG: u8 = b'n';
const TUPLE_DATA_TOAST_TAG: u8 = b'u';
const TUPLE_DATA_TEXT_TAG: u8 = b't';

// replica identity tags
const REPLICA_IDENTITY_DEFAULT_TAG: u8 = b'd';
const REPLICA_IDENTITY_NOTHING_TAG: u8 = b'n';
const REPLICA_IDENTITY_FULL_TAG: u8 = b'f';
const REPLICA_IDENTITY_INDEX_TAG: u8 = b'i';

#[non_exhaustive]
pub enum LogicalReplicationMessage {
    Begin(BeginBody),
    Commit(CommitBody),
    Origin(OriginBody),
    Relation(RelationBody),
    Type(TypeBody),
    Insert(InsertBody),
    Update(UpdateBody),
    Delete(DeleteBody),
    Truncate(TruncateBody),
}

impl Parse for LogicalReplicationMessage {
    fn parse_reader(buf: &mut BufReader<Bytes>) -> io::Result<Self> {
        let tag = buf.read_u8()?;

        let logical_replication_message = match tag {
            BEGIN_TAG => Self::Begin(BeginBody {
                final_lsn: buf.read_u64::<BigEndian>()?,
                timestamp: buf.read_i64::<BigEndian>()?,
                xid: buf.read_i32::<BigEndian>()?,
            }),
            COMMIT_TAG => Self::Commit(CommitBody {
                flags: buf.read_i8()?,
                commit_lsn: buf.read_u64::<BigEndian>()?,
                end_lsn: buf.read_u64::<BigEndian>()?,
                timestamp: buf.read_i64::<BigEndian>()?,
            }),
            ORIGIN_TAG => Self::Origin(OriginBody {
                commit_lsn: buf.read_u64::<BigEndian>()?,
                name: get_cstr(buf)?,
            }),
            RELATION_TAG => {
                let rel_id = buf.read_u32::<BigEndian>()?;
                let namespace = get_cstr(buf)?;
                let name = get_cstr(buf)?;
                let replica_identity = match buf.read_u8()? {
                    REPLICA_IDENTITY_DEFAULT_TAG => ReplicaIdentity::Default,
                    REPLICA_IDENTITY_NOTHING_TAG => ReplicaIdentity::Nothing,
                    REPLICA_IDENTITY_FULL_TAG => ReplicaIdentity::Full,
                    REPLICA_IDENTITY_INDEX_TAG => ReplicaIdentity::Index,
                    tag => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            format!("unknown replica identity tag `{}`", tag),
                        ));
                    }
                };
                let column_len = buf.read_i16::<BigEndian>()?;

                let mut columns = Vec::with_capacity(column_len as usize);
                for _ in 0..column_len {
                    columns.push(Column::parse_reader(buf)?);
                }

                Self::Relation(RelationBody {
                    rel_id,
                    namespace,
                    name,
                    replica_identity,
                    columns,
                })
            }
            TYPE_TAG => Self::Type(TypeBody {
                id: buf.read_u32::<BigEndian>()?,
                namespace: get_cstr(buf)?,
                name: get_cstr(buf)?,
            }),
            INSERT_TAG => {
                let rel_id = buf.read_u32::<BigEndian>()?;
                let tag = buf.read_u8()?;

                let tuple = match tag {
                    TUPLE_NEW_TAG => Tuple::parse_reader(buf)?,
                    tag => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            format!("unexpected tuple tag `{}`", tag),
                        ));
                    }
                };

                Self::Insert(InsertBody { rel_id, tuple })
            }
            UPDATE_TAG => {
                let rel_id = buf.read_u32::<BigEndian>()?;
                let tag = buf.read_u8()?;

                let mut key_tuple = None;
                let mut old_tuple = None;

                let new_tuple = match tag {
                    TUPLE_NEW_TAG => Tuple::parse_reader(buf)?,
                    TUPLE_OLD_TAG | TUPLE_KEY_TAG => {
                        if tag == TUPLE_OLD_TAG {
                            old_tuple = Some(Tuple::parse_reader(buf)?);
                        } else {
                            key_tuple = Some(Tuple::parse_reader(buf)?);
                        }

                        match buf.read_u8()? {
                            TUPLE_NEW_TAG => Tuple::parse_reader(buf)?,
                            tag => {
                                return Err(io::Error::new(
                                    io::ErrorKind::InvalidInput,
                                    format!("unexpected tuple tag `{}`", tag),
                                ));
                            }
                        }
                    }
                    tag => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            format!("unknown tuple tag `{}`", tag),
                        ));
                    }
                };

                Self::Update(UpdateBody {
                    rel_id,
                    key_tuple,
                    old_tuple,
                    new_tuple,
                })
            }
            DELETE_TAG => {
                let rel_id = buf.read_u32::<BigEndian>()?;
                let tag = buf.read_u8()?;

                let mut key_tuple = None;
                let mut old_tuple = None;

                match tag {
                    TUPLE_OLD_TAG => old_tuple = Some(Tuple::parse_reader(buf)?),
                    TUPLE_KEY_TAG => key_tuple = Some(Tuple::parse_reader(buf)?),
                    tag => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            format!("unknown tuple tag `{}`", tag),
                        ));
                    }
                }

                Self::Delete(DeleteBody { rel_id, key_tuple, old_tuple } )
            }
            TRUNCATE_TAG => {
                let relation_len = buf.read_i32::<BigEndian>()?;
                let options = buf.read_i8()?;

                let mut rel_ids = Vec::with_capacity(relation_len as usize);
                for _ in 0..relation_len {
                    rel_ids.push(buf.read_u32::<BigEndian>()?);
                }

                Self::Truncate(TruncateBody { options, rel_ids })
            }
            tag => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("unknown replication message tag `{}`", tag),
                ));
            }
        };

        Ok(logical_replication_message)
    }
}

pub struct Tuple(Vec<TupleData>);

impl Tuple {
    #[inline]
    pub fn tuple_data(&self) -> &[TupleData] {
        &self.0
    }
}

impl Parse for Tuple {
    fn parse_reader(buf: &mut BufReader<Bytes>) -> io::Result<Self> {
        let col_len = buf.read_i16::<BigEndian>()?;
        let mut tuple = Vec::with_capacity(col_len as usize);
        for _ in 0..col_len {
            tuple.push(TupleData::parse_reader(buf)?);
        }

        Ok(Tuple(tuple))
    }
}

pub struct Column {
    flags: i8,
    name: Bytes,
    type_id: i32,
    type_modifier: i32,
}

impl Column {
    #[inline]
    pub fn flags(&self) -> i8 {
        self.flags
    }

    #[inline]
    pub fn name(&self) -> io::Result<&str> {
        get_str(&self.name)
    }

    #[inline]
    pub fn type_id(&self) -> i32 {
        self.type_id
    }

    #[inline]
    pub fn type_modifier(&self) -> i32 {
        self.type_modifier
    }
}


impl Parse for Column {
    fn parse_reader(buf: &mut BufReader<Bytes>) -> io::Result<Self> {
        Ok(Self {
            flags: buf.read_i8()?,
            name: get_cstr(buf)?,
            type_id: buf.read_i32::<BigEndian>()?,
            type_modifier: buf.read_i32::<BigEndian>()?,
        })
    }
}

pub enum TupleData {
    Null,
    Toast,
    Text(Bytes),
}

impl Parse for TupleData {
    fn parse_reader(buf: &mut BufReader<Bytes>) -> io::Result<Self> {
        let type_tag = buf.read_u8()?;

        let tuple = match type_tag {
            TUPLE_DATA_NULL_TAG => TupleData::Null,
            TUPLE_DATA_TOAST_TAG => TupleData::Toast,
            TUPLE_DATA_TEXT_TAG => {
                let len = buf.read_i32::<BigEndian>()?;
                TupleData::Text(buf.get_mut().split_to(len as usize))
            }
            tag => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("unknown replication message tag `{}`", tag),
                ));
            }
        };

        Ok(tuple)
    }
}

pub struct BeginBody {
    final_lsn: u64,
    timestamp: i64,
    xid: i32,
}

impl BeginBody {
    #[inline]
    pub fn final_lsn(&self) -> Lsn {
        self.final_lsn.into()
    }

    #[inline]
    pub fn timestamp(&self) -> i64 {
        self.timestamp
    }

    #[inline]
    pub fn xid(&self) -> i32 {
        self.xid
    }
}

pub struct CommitBody {
    flags: i8,
    commit_lsn: u64,
    end_lsn: u64,
    timestamp: i64,
}

impl CommitBody {
    #[inline]
    pub fn commit_lsn(&self) -> Lsn {
        self.commit_lsn.into()
    }

    #[inline]
    pub fn end_lsn(&self) -> Lsn {
        self.end_lsn.into()
    }

    #[inline]
    pub fn timestamp(&self) -> i64 {
        self.timestamp
    }

    #[inline]
    pub fn flags(&self) -> i8 {
        self.flags
    }
}

pub struct OriginBody {
    commit_lsn: u64,
    name: Bytes,
}

impl OriginBody {
    #[inline]
    pub fn commit_lsn(&self) -> Lsn {
        self.commit_lsn.into()
    }

    #[inline]
    pub fn name(&self) -> io::Result<&str> {
        get_str(&self.name)
    }
}

pub enum ReplicaIdentity {
    /// default selection for replica identity (primary key or nothing)
    Default,
    /// no replica identity is logged for this relation
    Nothing,
    /// all columns are logged as replica identity
    Full,
    /// An explicitly chosen candidate key's columns are used as replica identity.
    /// Note this will still be set if the index has been dropped; in that case it
    /// has the same meaning as 'd'.
    Index,
}

pub struct RelationBody {
    rel_id: u32,
    namespace: Bytes,
    name: Bytes,
    replica_identity: ReplicaIdentity,
    columns: Vec<Column>,
}

impl RelationBody {
    #[inline]
    pub fn rel_id(&self) -> u32 {
        self.rel_id
    }

    #[inline]
    pub fn namespace(&self) -> io::Result<&str> {
        get_str(&self.namespace)
    }

    #[inline]
    pub fn name(&self) -> io::Result<&str> {
        get_str(&self.name)
    }

    #[inline]
    pub fn replica_identity(&self) -> &ReplicaIdentity {
        &self.replica_identity
    }

    #[inline]
    pub fn columns(&self) -> &[Column] {
        &self.columns
    }
}

pub struct TypeBody {
    id: u32,
    namespace: Bytes,
    name: Bytes,
}

impl TypeBody {
    #[inline]
    pub fn id(&self) -> Oid {
        self.id
    }

    #[inline]
    pub fn namespace(&self) -> io::Result<&str> {
        get_str(&self.namespace)
    }

    #[inline]
    pub fn name(&self) -> io::Result<&str> {
        get_str(&self.name)
    }
}

pub struct InsertBody {
    rel_id: u32,
    tuple: Tuple,
}

impl InsertBody {
    #[inline]
    pub fn rel_id(&self) -> u32 {
        self.rel_id
    }

    #[inline]
    pub fn tuple(&self) -> &Tuple {
        &self.tuple
    }
}

pub struct UpdateBody {
    rel_id: u32,
    old_tuple: Option<Tuple>,
    key_tuple: Option<Tuple>,
    new_tuple: Tuple,
}

impl UpdateBody {
    #[inline]
    pub fn rel_id(&self) -> u32 {
        self.rel_id
    }

    #[inline]
    pub fn key_tuple(&self) -> Option<&Tuple> {
        self.key_tuple.as_ref()
    }

    #[inline]
    pub fn old_tuple(&self) -> Option<&Tuple> {
        self.old_tuple.as_ref()
    }

    #[inline]
    pub fn new_tuple(&self) -> &Tuple {
        &self.new_tuple
    }
}

pub struct DeleteBody {
    rel_id: u32,
    old_tuple: Option<Tuple>,
    key_tuple: Option<Tuple>,
}

impl DeleteBody {
    #[inline]
    pub fn rel_id(&self) -> u32 {
        self.rel_id
    }

    #[inline]
    pub fn key_tuple(&self) -> Option<&Tuple> {
        self.key_tuple.as_ref()
    }

    #[inline]
    pub fn old_tuple(&self) -> Option<&Tuple> {
        self.old_tuple.as_ref()
    }
}

pub struct TruncateBody {
    options: i8,
    rel_ids: Vec<u32>,
}

impl TruncateBody {
    #[inline]
    pub fn rel_ids(&self) -> &[u32] {
        &self.rel_ids
    }

    #[inline]
    pub fn options(&self) -> i8 {
        self.options
    }
}

#[inline]
fn find_null(buf: &[u8], start: usize) -> io::Result<usize> {
    match memchr(0, &buf[start..]) {
        Some(pos) => Ok(pos + start),
        None => Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "unexpected EOF",
        )),
    }
}

fn get_cstr(r: &mut BufReader<Bytes>) -> io::Result<Bytes> {
    let buf = r.get_mut();
    let end = find_null(buf.chunk(), 0)?;
    Ok(buf.split_to(end + 1))
}

#[inline]
fn get_str(buf: &[u8]) -> io::Result<&str> {
    std::str::from_utf8(buf).map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))
}
