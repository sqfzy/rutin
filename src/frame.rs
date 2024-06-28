#![allow(dead_code)]

use crate::{
    util::{self, atof},
    Int,
};
use ahash::{AHashMap, AHashSet};
use bytes::{Buf, Bytes, BytesMut};
use bytestring::ByteString;
use mlua::{prelude::*, Value};
use num_bigint::BigInt;
use snafu::Snafu;
use std::{hash::Hash, io, iter::Iterator, ops::Range, ptr::slice_from_raw_parts};
use strum::{EnumDiscriminants, IntoStaticStr};
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio_util::{
    bytes::BufMut,
    codec::{Decoder, Encoder},
};
use tracing::instrument;

pub type FrameResult<T> = Result<T, FrameError>;

#[derive(Snafu, Debug)]
pub enum FrameError {
    #[snafu(transparent)]
    Io { source: tokio::io::Error },

    #[snafu(display("incomplete frame"))]
    Incomplete,

    #[snafu(display("invalid format: {}", msg))]
    InvalidFormat { msg: String },
}

const CRLF: &[u8] = b"\r\n";

const SIMPLE_STRING_PREFIX: u8 = b'+';
const ERROR_PREFIX: u8 = b'-';
const INTEGER_PREFIX: u8 = b':';
const BLOB_STRING_PREFIX: u8 = b'$';
const ARRAY_PREFIX: u8 = b'*';
const NULL_PREFIX: u8 = b'_';
const BOOLEAN_PREFIX: u8 = b'#';
const DOUBLE_PREFIX: u8 = b',';
const BIG_NUMBER_PREFIX: u8 = b'(';
const BLOB_ERROR_PREFIX: u8 = b'!';
const VERBATIM_STRING_PREFIX: u8 = b'=';
const MAP_PREFIX: u8 = b'%';
const SET_PREFIX: u8 = b'~';
const PUSH_PREFIX: u8 = b'>';
const CHUNKED_STRING_PREFIX: u8 = b'?';
const CHUNKED_STRING_LENGTH_PREFIX: u8 = b';';

pub type Attributes<B, S> = AHashMap<Resp3<B, S>, Resp3<B, S>>;

#[derive(Clone, Debug, IntoStaticStr, EnumDiscriminants)]
#[strum_discriminants(vis(pub))]
#[strum_discriminants(name(Resp3Type))]
#[strum_discriminants(derive(IntoStaticStr))]
pub enum Resp3<B = Bytes, S = ByteString>
where
    B: AsRef<[u8]> + PartialEq,
    S: AsRef<str> + PartialEq,
{
    // +<str>\r\n
    SimpleString {
        inner: S,
        attributes: Option<Attributes<B, S>>,
    },

    // -<err>\r\n
    SimpleError {
        inner: S,
        attributes: Option<Attributes<B, S>>,
    },

    // :[<+|->]<value>\r\n
    Integer {
        inner: Int,
        attributes: Option<Attributes<B, S>>,
    },

    // $<length>\r\n<data>\r\n
    BlobString {
        inner: B,
        attributes: Option<Attributes<B, S>>,
    },

    // !<length>\r\n<error>\r\n
    BlobError {
        inner: B,
        attributes: Option<Attributes<B, S>>,
    },

    // *<number-of-elements>\r\n<element-1>...<element-n>
    Array {
        inner: Vec<Resp3<B, S>>,
        attributes: Option<Attributes<B, S>>,
    },

    // _\r\n
    Null,

    // #<t|f>\r\n
    Boolean {
        inner: bool,
        attributes: Option<Attributes<B, S>>,
    },

    // ,[<+|->]<integral>[.<fractional>][<E|e>[sign]<exponent>]\r\n
    Double {
        inner: f64,
        attributes: Option<Attributes<B, S>>,
    },

    // ([+|-]<number>\r\n
    BigNumber {
        inner: BigInt,
        attributes: Option<Attributes<B, S>>,
    },

    // =<length>\r\n<encoding>:<data>\r\n
    // 类似于Blob，但是多了一个encoding字段用于指定数据的编码方式
    VerbatimString {
        format: [u8; 3],
        data: B,
        attributes: Option<Attributes<B, S>>,
    },

    // %<number-of-entries>\r\n<key-1><value-1>...<key-n><value-n>
    Map {
        inner: AHashMap<Resp3<B, S>, Resp3<B, S>>,
        attributes: Option<Attributes<B, S>>,
    },

    // ~<number-of-elements>\r\n<element-1>...<element-n>
    Set {
        inner: AHashSet<Resp3<B, S>>,
        attributes: Option<Attributes<B, S>>,
    },

    // ><number-of-elements>\r\n<element-1>...<element-n>
    // 通常用于服务端主动向客户端推送消息
    Push {
        inner: Vec<Resp3<B, S>>,
        attributes: Option<Attributes<B, S>>,
    },

    // $?\r\n;<length>\r\n<data>\r\n;<length>\r\n<data>\r\n...;0\r\n
    ChunkedString(Vec<B>),

    // HELLO <version> <username> <password> <clientname>
    Hello {
        version: Int,
        auth: Option<(B, B)>,
    },
}

impl<B, S> Resp3<B, S>
where
    B: AsRef<[u8]> + PartialEq,
    S: AsRef<str> + PartialEq,
{
    pub fn new_simple_string(string: S) -> Self {
        Resp3::SimpleString {
            inner: string,
            attributes: None,
        }
    }

    pub fn new_simple_error(error: S) -> Self {
        Resp3::SimpleError {
            inner: error,
            attributes: None,
        }
    }

    pub fn new_integer(integer: Int) -> Self {
        Resp3::Integer {
            inner: integer,
            attributes: None,
        }
    }

    pub fn new_blob_string(blob: B) -> Self {
        Resp3::BlobString {
            inner: blob,
            attributes: None,
        }
    }

    pub fn new_array(array: impl Into<Vec<Resp3<B, S>>>) -> Self {
        Resp3::Array {
            inner: array.into(),
            attributes: None,
        }
    }

    pub fn new_null() -> Self {
        Resp3::Null
    }

    pub fn new_boolean(bool: bool) -> Self {
        Resp3::Boolean {
            inner: bool,
            attributes: None,
        }
    }

    pub fn new_double(double: f64) -> Self {
        Resp3::Double {
            inner: double,
            attributes: None,
        }
    }

    pub fn new_big_number(big_num: BigInt) -> Self {
        Resp3::BigNumber {
            inner: big_num,
            attributes: None,
        }
    }

    pub fn new_blob_error(error: B) -> Self {
        Resp3::BlobError {
            inner: error,
            attributes: None,
        }
    }

    pub fn new_verbatim_string(format: [u8; 3], data: B) -> Self {
        Resp3::VerbatimString {
            format,
            data,
            attributes: None,
        }
    }

    pub fn new_map(map: impl Into<AHashMap<Resp3<B, S>, Resp3<B, S>>>) -> Self {
        Resp3::Map {
            inner: map.into(),
            attributes: None,
        }
    }

    pub fn new_set(set: impl Into<AHashSet<Resp3<B, S>>>) -> Self {
        Resp3::Set {
            inner: set.into(),
            attributes: None,
        }
    }

    pub fn new_push(push: impl Into<Vec<Resp3<B, S>>>) -> Self {
        Resp3::Push {
            inner: push.into(),
            attributes: None,
        }
    }

    pub fn new_chunked_string(chunks: impl Into<Vec<B>>) -> Self {
        Resp3::ChunkedString(chunks.into())
    }

    pub fn is_simple_string(&self) -> bool {
        matches!(self, Resp3::SimpleString { .. })
    }

    pub fn is_simple_error(&self) -> bool {
        matches!(self, Resp3::SimpleError { .. })
    }

    pub fn is_integer(&self) -> bool {
        matches!(self, Resp3::Integer { .. })
    }

    pub fn is_blob(&self) -> bool {
        matches!(self, Resp3::BlobString { .. })
    }

    pub fn is_array(&self) -> bool {
        matches!(self, Resp3::Array { .. })
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Resp3::Null)
    }

    pub fn is_boolean(&self) -> bool {
        matches!(self, Resp3::Boolean { .. })
    }

    pub fn is_double(&self) -> bool {
        matches!(self, Resp3::Double { .. })
    }

    pub fn is_big_number(&self) -> bool {
        matches!(self, Resp3::BigNumber { .. })
    }

    pub fn is_blob_error(&self) -> bool {
        matches!(self, Resp3::BlobError { .. })
    }

    pub fn is_verbatim_string(&self) -> bool {
        matches!(self, Resp3::VerbatimString { .. })
    }

    pub fn is_map(&self) -> bool {
        matches!(self, Resp3::Map { .. })
    }

    pub fn is_set(&self) -> bool {
        matches!(self, Resp3::Set { .. })
    }

    pub fn is_push(&self) -> bool {
        matches!(self, Resp3::Push { .. })
    }

    pub fn try_simple_string(&self) -> Option<&S> {
        match self {
            Resp3::SimpleString { inner, .. } => Some(inner),
            _ => None,
        }
    }

    pub fn try_simple_error(&self) -> Option<&S> {
        match self {
            Resp3::SimpleError { inner, .. } => Some(inner),
            _ => None,
        }
    }

    pub fn try_integer(&self) -> Option<Int> {
        match self {
            Resp3::Integer { inner, .. } => Some(*inner),
            _ => None,
        }
    }

    pub fn try_blob(&self) -> Option<&B> {
        match self {
            Resp3::BlobString { inner, .. } => Some(inner),
            _ => None,
        }
    }

    pub fn try_array(&self) -> Option<&Vec<Resp3<B, S>>> {
        match self {
            Resp3::Array { inner, .. } => Some(inner),
            _ => None,
        }
    }

    pub fn try_null(&self) -> Option<()> {
        match self {
            Resp3::Null => Some(()),
            _ => None,
        }
    }

    pub fn try_boolean(&self) -> Option<bool> {
        match self {
            Resp3::Boolean { inner, .. } => Some(*inner),
            _ => None,
        }
    }

    pub fn try_double(&self) -> Option<f64> {
        match self {
            Resp3::Double { inner, .. } => Some(*inner),
            _ => None,
        }
    }

    pub fn try_big_number(&self) -> Option<&BigInt> {
        match self {
            Resp3::BigNumber { inner, .. } => Some(inner),
            _ => None,
        }
    }

    pub fn try_blob_error(&self) -> Option<&B> {
        match self {
            Resp3::BlobError { inner, .. } => Some(inner),
            _ => None,
        }
    }

    pub fn try_verbatim_string(&self) -> Option<(&[u8; 3], &B)> {
        match self {
            Resp3::VerbatimString { format, data, .. } => Some((format, data)),
            _ => None,
        }
    }

    #[allow(clippy::type_complexity)]
    pub fn try_map(&self) -> Option<&AHashMap<Resp3<B, S>, Resp3<B, S>>> {
        match self {
            Resp3::Map { inner, .. } => Some(inner),
            _ => None,
        }
    }

    pub fn try_set(&self) -> Option<&AHashSet<Resp3<B, S>>> {
        match self {
            Resp3::Set { inner, .. } => Some(inner),
            _ => None,
        }
    }

    pub fn try_push(&self) -> Option<&Vec<Resp3<B, S>>> {
        match self {
            Resp3::Push { inner, .. } => Some(inner),
            _ => None,
        }
    }

    pub fn try_simple_string_mut(&mut self) -> Option<&mut S> {
        match self {
            Resp3::SimpleString { inner, .. } => Some(inner),
            _ => None,
        }
    }

    pub fn try_simple_error_mut(&mut self) -> Option<&mut S> {
        match self {
            Resp3::SimpleError { inner, .. } => Some(inner),
            _ => None,
        }
    }

    pub fn try_integer_mut(&mut self) -> Option<&mut Int> {
        match self {
            Resp3::Integer { inner, .. } => Some(inner),
            _ => None,
        }
    }

    pub fn try_blob_mut(&mut self) -> Option<&mut B> {
        match self {
            Resp3::BlobString { inner, .. } => Some(inner),
            _ => None,
        }
    }

    pub fn try_array_mut(&mut self) -> Option<&mut Vec<Resp3<B, S>>> {
        match self {
            Resp3::Array { inner, .. } => Some(inner),
            _ => None,
        }
    }

    pub fn try_boolean_mut(&mut self) -> Option<&mut bool> {
        match self {
            Resp3::Boolean { inner, .. } => Some(inner),
            _ => None,
        }
    }

    pub fn try_double_mut(&mut self) -> Option<&mut f64> {
        match self {
            Resp3::Double { inner, .. } => Some(inner),
            _ => None,
        }
    }

    pub fn try_big_number_mut(&mut self) -> Option<&mut BigInt> {
        match self {
            Resp3::BigNumber { inner, .. } => Some(inner),
            _ => None,
        }
    }

    pub fn try_blob_error_mut(&mut self) -> Option<&mut B> {
        match self {
            Resp3::BlobError { inner, .. } => Some(inner),
            _ => None,
        }
    }

    pub fn try_verbatim_string_mut(&mut self) -> Option<(&mut [u8; 3], &mut B)> {
        match self {
            Resp3::VerbatimString { format, data, .. } => Some((format, data)),
            _ => None,
        }
    }

    #[allow(clippy::type_complexity)]
    pub fn try_map_mut(&mut self) -> Option<&mut AHashMap<Resp3<B, S>, Resp3<B, S>>> {
        match self {
            Resp3::Map { inner, .. } => Some(inner),
            _ => None,
        }
    }

    pub fn try_set_mut(&mut self) -> Option<&mut AHashSet<Resp3<B, S>>> {
        match self {
            Resp3::Set { inner, .. } => Some(inner),
            _ => None,
        }
    }

    pub fn try_push_mut(&mut self) -> Option<&mut Vec<Resp3<B, S>>> {
        match self {
            Resp3::Push { inner, .. } => Some(inner),
            _ => None,
        }
    }

    pub fn as_simple_string_uncheckd(&self) -> &S {
        match self {
            Resp3::SimpleString { inner, .. } => inner,
            _ => panic!("not a simple string"),
        }
    }

    pub fn as_simple_error_uncheckd(&self) -> &S {
        match self {
            Resp3::SimpleError { inner, .. } => inner,
            _ => panic!("not a simple error"),
        }
    }

    pub fn as_integer_uncheckd(&self) -> Int {
        match self {
            Resp3::Integer { inner, .. } => *inner,
            _ => panic!("not an integer"),
        }
    }

    pub fn as_blob_string_uncheckd(&self) -> &B {
        match self {
            Resp3::BlobString { inner, .. } => inner,
            _ => panic!("not a blob string"),
        }
    }

    pub fn as_array_uncheckd(&self) -> &Vec<Resp3<B, S>> {
        match self {
            Resp3::Array { inner, .. } => inner,
            _ => panic!("not an array"),
        }
    }

    pub fn as_null_uncheckd(&self) {
        match self {
            Resp3::Null => {}
            _ => panic!("not a null"),
        }
    }

    pub fn as_boolean_uncheckd(&self) -> bool {
        match self {
            Resp3::Boolean { inner, .. } => *inner,
            _ => panic!("not a boolean"),
        }
    }

    pub fn as_double_uncheckd(&self) -> f64 {
        match self {
            Resp3::Double { inner, .. } => *inner,
            _ => panic!("not a double"),
        }
    }

    pub fn as_big_number_uncheckd(&self) -> &BigInt {
        match self {
            Resp3::BigNumber { inner, .. } => inner,
            _ => panic!("not a big number"),
        }
    }

    pub fn as_bolb_error_uncheckd(&self) -> &B {
        match self {
            Resp3::BlobError { inner, .. } => inner,
            _ => panic!("not a blob error"),
        }
    }

    pub fn as_verbatim_string_uncheckd(&self) -> (&[u8; 3], &B) {
        match self {
            Resp3::VerbatimString { format, data, .. } => (format, data),
            _ => panic!("not a verbatim string"),
        }
    }

    pub fn as_map_uncheckd(&self) -> &AHashMap<Resp3<B, S>, Resp3<B, S>> {
        match self {
            Resp3::Map { inner, .. } => inner,
            _ => panic!("not a map"),
        }
    }

    pub fn as_set_uncheckd(&self) -> &AHashSet<Resp3<B, S>> {
        match self {
            Resp3::Set { inner, .. } => inner,
            _ => panic!("not a set"),
        }
    }

    pub fn as_push_uncheckd(&self) -> &Vec<Resp3<B, S>> {
        match self {
            Resp3::Push { inner, .. } => inner,
            _ => panic!("not a push"),
        }
    }

    pub fn add_attributes(&mut self, attrs: Attributes<B, S>) {
        match self {
            Resp3::SimpleString { attributes, .. }
            | Resp3::SimpleError { attributes, .. }
            | Resp3::Integer { attributes, .. }
            | Resp3::BlobString { attributes, .. }
            | Resp3::Array { attributes, .. }
            | Resp3::Boolean { attributes, .. }
            | Resp3::Double { attributes, .. }
            | Resp3::BigNumber { attributes, .. }
            | Resp3::BlobError { attributes, .. }
            | Resp3::VerbatimString { attributes, .. }
            | Resp3::Map { attributes, .. }
            | Resp3::Set { attributes, .. }
            | Resp3::Push { attributes, .. } => {
                attributes.get_or_insert_with(AHashMap::new).extend(attrs);
            }
            Resp3::Null | Resp3::ChunkedString(_) | Resp3::Hello { .. } => {
                panic!("can't have attributes")
            }
        }
    }

    #[inline]
    pub fn encode(&self) -> BytesMut {
        let mut buf = BytesMut::with_capacity(64);
        self.encode_buf(&mut buf);
        buf.split()
    }

    #[inline]
    pub fn encode_buf(&self, buf: &mut impl BufMut) {
        match self {
            Resp3::SimpleString { inner, attributes } => {
                if let Some(attr) = attributes.as_ref() {
                    encode_attributes(buf, attr)
                }
                buf.put_u8(SIMPLE_STRING_PREFIX);
                buf.put_slice(inner.as_ref().as_bytes());
                buf.put_slice(CRLF);
            }
            Resp3::SimpleError { inner, attributes } => {
                if let Some(attr) = attributes.as_ref() {
                    encode_attributes(buf, attr)
                }
                buf.put_u8(ERROR_PREFIX);
                buf.put_slice(inner.as_ref().as_bytes());
                buf.put_slice(CRLF);
            }
            Resp3::Integer { inner, attributes } => {
                if let Some(attr) = attributes.as_ref() {
                    encode_attributes(buf, attr)
                }
                buf.put_u8(INTEGER_PREFIX);
                buf.put_slice(itoa::Buffer::new().format(*inner).as_bytes());
                buf.put_slice(CRLF);
            }
            Resp3::BlobString { inner, attributes } => {
                if let Some(attr) = attributes.as_ref() {
                    encode_attributes(buf, attr)
                }
                buf.put_u8(BLOB_STRING_PREFIX);
                buf.put_slice(itoa::Buffer::new().format(inner.as_ref().len()).as_bytes());
                buf.put_slice(CRLF);
                buf.put_slice(inner.as_ref());
                buf.put_slice(CRLF);
            }
            Resp3::Array { inner, attributes } => {
                if let Some(attr) = attributes.as_ref() {
                    encode_attributes(buf, attr)
                }
                buf.put_u8(ARRAY_PREFIX);
                buf.put_slice(itoa::Buffer::new().format(inner.len()).as_bytes());
                buf.put_slice(CRLF);
                for frame in inner {
                    frame.encode_buf(buf);
                }
            }
            Resp3::Null => buf.put_slice(b"_\r\n"),
            Resp3::Boolean { inner, attributes } => {
                if let Some(attr) = attributes.as_ref() {
                    encode_attributes(buf, attr)
                }
                buf.put_u8(BOOLEAN_PREFIX);
                buf.put_slice(if *inner { b"t" } else { b"f" });
                buf.put_slice(CRLF);
            }
            Resp3::Double { inner, attributes } => {
                if let Some(attr) = attributes.as_ref() {
                    encode_attributes(buf, attr)
                }
                buf.put_u8(DOUBLE_PREFIX);
                if inner.fract() == 0.0 {
                    buf.put_slice(itoa::Buffer::new().format((*inner) as i64).as_bytes());
                } else {
                    buf.put_slice(ryu::Buffer::new().format(*inner).as_bytes());
                }
                buf.put_slice(CRLF);
            }
            Resp3::BigNumber { inner, attributes } => {
                if let Some(attr) = attributes.as_ref() {
                    encode_attributes(buf, attr)
                }
                buf.put_u8(BIG_NUMBER_PREFIX);
                buf.put_slice(inner.to_str_radix(10).as_bytes());
                buf.put_slice(CRLF);
            }
            Resp3::BlobError { inner, attributes } => {
                if let Some(attr) = attributes.as_ref() {
                    encode_attributes(buf, attr)
                }
                buf.put_u8(BLOB_ERROR_PREFIX);
                buf.put_slice(itoa::Buffer::new().format(inner.as_ref().len()).as_bytes());
                buf.put_slice(CRLF);
                buf.put_slice(inner.as_ref());
                buf.put_slice(CRLF);
            }
            Resp3::VerbatimString {
                format,
                data,
                attributes,
            } => {
                if let Some(attr) = attributes.as_ref() {
                    encode_attributes(buf, attr)
                }
                buf.put_u8(VERBATIM_STRING_PREFIX);
                buf.put_slice(
                    itoa::Buffer::new()
                        .format(data.as_ref().len() + 4)
                        .as_bytes(),
                );
                buf.put_slice(CRLF);
                buf.put_slice(format);
                buf.put_u8(INTEGER_PREFIX);
                buf.put_slice(data.as_ref());
                buf.put_slice(CRLF);
            }
            Resp3::Map { inner, attributes } => {
                if let Some(attr) = attributes.as_ref() {
                    encode_attributes(buf, attr)
                }
                buf.put_u8(MAP_PREFIX);
                buf.put_slice(itoa::Buffer::new().format(inner.len()).as_bytes());
                buf.put_slice(CRLF);
                for (k, v) in inner {
                    k.encode_buf(buf);
                    v.encode_buf(buf);
                }
            }
            Resp3::Set { inner, attributes } => {
                if let Some(attr) = attributes.as_ref() {
                    encode_attributes(buf, attr)
                }
                buf.put_u8(SET_PREFIX);
                buf.put_slice(itoa::Buffer::new().format(inner.len()).as_bytes());
                buf.put_slice(CRLF);
                for frame in inner {
                    frame.encode_buf(buf);
                }
            }
            Resp3::Push { inner, attributes } => {
                if let Some(attr) = attributes.as_ref() {
                    encode_attributes(buf, attr)
                }
                buf.put_u8(PUSH_PREFIX);
                buf.put_slice(itoa::Buffer::new().format(inner.len()).as_bytes());
                buf.put_slice(CRLF);
                for frame in inner {
                    frame.encode_buf(buf);
                }
            }
            Resp3::ChunkedString(chunks) => {
                buf.put_slice(b"$?\r\n");
                for chunk in chunks {
                    buf.put_slice(b";");
                    buf.put_slice(itoa::Buffer::new().format(chunk.as_ref().len()).as_bytes());
                    buf.put_slice(CRLF);
                    buf.put_slice(chunk.as_ref());
                    buf.put_slice(CRLF);
                }
                buf.put_slice(b";0\r\n");
            }
            Resp3::Hello { version, auth } => {
                buf.put_slice(b"HELLO ");
                buf.put_slice(itoa::Buffer::new().format(*version).as_bytes());
                buf.put_u8(b' ');
                if let Some(auth) = auth {
                    buf.put_slice(b"AUTH ");
                    buf.put_slice(auth.0.as_ref());
                    buf.put_u8(b' ');
                    buf.put_slice(auth.1.as_ref());
                }
                buf.put_slice(CRLF);
            }
        }
    }
}

// 解码
impl Resp3<BytesMut, ByteString> {
    #[allow(clippy::multiple_bound_locations)]
    #[inline]
    #[instrument(level = "trace", skip(io_read), err)]
    pub async fn decode_async<R: AsyncRead + Unpin + Send>(
        io_read: &mut R,
        src: &mut BytesMut,
    ) -> FrameResult<Option<Resp3>> {
        if src.is_empty() && io_read.read_buf(src).await? == 0 {
            return Ok(None);
        }

        debug_assert!(!src.is_empty());

        #[inline]
        async fn _decode_async<R: AsyncRead + Unpin + Send>(
            io_read: &mut R,
            src: &mut BytesMut,
        ) -> FrameResult<Resp3> {
            let res = match src.get_u8() {
                SIMPLE_STRING_PREFIX => Resp3::SimpleString {
                    inner: Resp3::decode_string_async(io_read, src).await?,
                    attributes: None,
                },
                ERROR_PREFIX => Resp3::SimpleError {
                    inner: Resp3::decode_string_async(io_read, src).await?,
                    attributes: None,
                },
                INTEGER_PREFIX => Resp3::Integer {
                    inner: Resp3::decode_decimal_async(io_read, src).await?,
                    attributes: None,
                },
                BLOB_STRING_PREFIX => {
                    let line = Resp3::decode_line_async(io_read, src).await?;

                    if Resp3::get(&line, 0..1)?[0] == CHUNKED_STRING_PREFIX {
                        let mut chunks = Vec::new();
                        loop {
                            let mut line = Resp3::decode_line_async(io_read, src).await?;

                            if Resp3::get_u8(&mut line)? != CHUNKED_STRING_LENGTH_PREFIX {
                                return Err(FrameError::InvalidFormat {
                                    msg: "invalid chunk length prefix".to_string(),
                                });
                            }

                            let len = util::atoi(&line).map_err(|_| {
                                io::Error::new(io::ErrorKind::InvalidData, "invalid chunk length")
                            })?;

                            if len == 0 {
                                break;
                            }

                            Resp3::need_bytes_async(io_read, src, len + 2).await?;
                            let res = src.split_to(len);
                            src.advance(2);

                            chunks.push(res.freeze());
                        }

                        Resp3::ChunkedString(chunks)
                    } else {
                        let len = util::atoi(&line).map_err(|_| FrameError::InvalidFormat {
                            msg: "invalid blob string length".to_string(),
                        })?;

                        Resp3::need_bytes_async(io_read, src, len + 2).await?;
                        let res = src.split_to(len);
                        src.advance(2);

                        Resp3::BlobString {
                            inner: res.freeze(),
                            attributes: None,
                        }
                    }
                }
                ARRAY_PREFIX => {
                    let len = Resp3::decode_decimal_async(io_read, src).await? as usize;

                    let mut frames = Vec::with_capacity(len);
                    for _ in 0..len {
                        let frame = Box::pin(_decode_async(io_read, src)).await?;
                        frames.push(frame);
                    }

                    Resp3::Array {
                        inner: frames,
                        attributes: None,
                    }
                }
                NULL_PREFIX => {
                    Resp3::need_bytes_async(io_read, src, 2).await?;
                    src.advance(2);
                    Resp3::Null
                }
                BOOLEAN_PREFIX => {
                    Resp3::need_bytes_async(io_read, src, 3).await?;

                    let b = match src[0] {
                        b't' => true,
                        b'f' => false,
                        _ => {
                            return Err(FrameError::InvalidFormat {
                                msg: "invalid boolean".to_string(),
                            });
                        }
                    };
                    src.advance(3);

                    Resp3::Boolean {
                        inner: b,
                        attributes: None,
                    }
                }
                DOUBLE_PREFIX => {
                    let line = Resp3::decode_line_async(io_read, src).await?;

                    let double = atof(&line)
                        .map_err(|e| FrameError::InvalidFormat { msg: e.to_string() })?;

                    Resp3::Double {
                        inner: double,
                        attributes: None,
                    }
                }
                BIG_NUMBER_PREFIX => {
                    let line = Resp3::decode_line_async(io_read, src).await?;

                    let n = BigInt::parse_bytes(&line, 10).ok_or_else(|| {
                        FrameError::InvalidFormat {
                            msg: "invalid big number".to_string(),
                        }
                    })?;

                    Resp3::BigNumber {
                        inner: n,
                        attributes: None,
                    }
                }
                BLOB_ERROR_PREFIX => {
                    let len = Resp3::decode_length_async(io_read, src).await?;

                    Resp3::need_bytes_async(io_read, src, len + 2).await?;
                    let e = src.split_to(len);
                    src.advance(2);

                    Resp3::BlobError {
                        inner: e.freeze(),
                        attributes: None,
                    }
                }
                VERBATIM_STRING_PREFIX => {
                    let len = Resp3::decode_length_async(io_read, src).await?;

                    Resp3::need_bytes_async(io_read, src, len + 2).await?;

                    let format = src[0..3].try_into().unwrap();
                    src.advance(4);

                    let data = src.split_to(len).freeze();
                    src.advance(2);

                    Resp3::VerbatimString {
                        format,
                        data,
                        attributes: None,
                    }
                }
                MAP_PREFIX => {
                    let len = Resp3::decode_decimal_async(io_read, src).await? as usize;

                    let mut map = AHashMap::with_capacity(len);
                    for _ in 0..len {
                        let k = Box::pin(_decode_async(io_read, src)).await?;
                        let v = Box::pin(_decode_async(io_read, src)).await?;
                        map.insert(k, v);
                    }

                    // map的key由客户端保证唯一
                    Resp3::Map {
                        inner: map,
                        attributes: None,
                    }
                }
                SET_PREFIX => {
                    let len = Resp3::decode_decimal_async(io_read, src).await? as usize;

                    let mut set = AHashSet::with_capacity(len);
                    for _ in 0..len {
                        let frame = Box::pin(_decode_async(io_read, src)).await?;
                        set.insert(frame);
                    }

                    // set的元素由客户端保证唯一
                    Resp3::Set {
                        inner: set,
                        attributes: None,
                    }
                }
                PUSH_PREFIX => {
                    let len = Resp3::decode_decimal_async(io_read, src).await? as usize;

                    let mut frames = Vec::with_capacity(len);
                    for _ in 0..len {
                        let frame = Box::pin(_decode_async(io_read, src)).await?;
                        frames.push(frame);
                    }

                    Resp3::Push {
                        inner: frames,
                        attributes: None,
                    }
                }
                b'H' => {
                    let mut line = Resp3::decode_line_async(io_read, src).await?;

                    let ello = Resp3::decode_until_async(io_read, &mut line, b' ').await?;
                    if ello != b"ELLO".as_slice() {
                        return Err(FrameError::InvalidFormat {
                            msg: "failed to parse hello".to_string(),
                        });
                    }

                    let version =
                        util::atoi(&Resp3::decode_until(&mut line, b' ')?).map_err(|e| {
                            FrameError::InvalidFormat {
                                msg: format!("invalid version: {}", e),
                            }
                        })?;

                    if line.is_empty() {
                        Resp3::Hello {
                            version,
                            auth: None,
                        }
                    } else {
                        let auth = Resp3::decode_until_async(io_read, &mut line, b' ').await?;
                        if auth != b"AUTH".as_slice() {
                            return Err(FrameError::InvalidFormat {
                                msg: "invalid auth".to_string(),
                            });
                        }

                        let username = Resp3::decode_until_async(io_read, &mut line, b' ')
                            .await?
                            .freeze();

                        let password = line.split().freeze();

                        Resp3::Hello {
                            version,
                            auth: Some((username, password)),
                        }
                    }
                }
                prefix => {
                    return Err(FrameError::InvalidFormat {
                        msg: format!("invalid prefix: {}", prefix),
                    });
                }
            };

            Ok(res)
        }

        let res = _decode_async(io_read, src).await?;
        Ok(Some(res))
    }

    #[inline]
    async fn need_bytes_async<R: AsyncRead + Unpin + Send>(
        io_read: &mut R,
        src: &mut BytesMut,
        len: usize,
    ) -> FrameResult<()> {
        while src.len() < len {
            if io_read.read_buf(src).await? == 0 {
                return Err(FrameError::Incomplete);
            }
        }

        Ok(())
    }

    #[inline]
    async fn decode_line_async<R: AsyncRead + Unpin + Send>(
        io_read: &mut R,
        src: &mut BytesMut,
    ) -> FrameResult<BytesMut> {
        loop {
            match Self::decode_line(src) {
                Ok(line) => return Ok(line),
                Err(FrameError::Incomplete) => {
                    if io_read.read_buf(src).await? == 0 {
                        return Err(FrameError::Incomplete);
                    }
                }
                Err(e) => return Err(e),
            }
        }
    }

    #[inline]
    async fn decode_until_async<R: AsyncRead + Unpin + Send>(
        io_read: &mut R,
        src: &mut BytesMut,
        byte: u8,
    ) -> FrameResult<BytesMut> {
        loop {
            if let Some(i) = memchr::memchr(byte, src) {
                let line = src.split_to(i);
                src.advance(1);
                return Ok(line);
            }

            if io_read.read_buf(src).await? == 0 {
                return Err(FrameError::Incomplete);
            }
        }
    }

    #[inline]
    async fn decode_exact_async<R: AsyncRead + Unpin + Send>(
        io_read: &mut R,
        src: &mut BytesMut,
        len: usize,
    ) -> FrameResult<BytesMut> {
        loop {
            if src.len() >= len {
                return Ok(src.split_to(len));
            }

            if io_read.read_buf(src).await? == 0 {
                return Err(FrameError::Incomplete);
            }
        }
    }

    #[inline]
    async fn decode_decimal_async<R: AsyncRead + Unpin + Send>(
        io_read: &mut R,
        src: &mut BytesMut,
    ) -> FrameResult<Int> {
        let line = Resp3::decode_line_async(io_read, src).await?;
        let decimal = util::atoi(&line).map_err(|e| FrameError::InvalidFormat { msg: e })?;
        Ok(decimal)
    }

    #[inline]
    async fn decode_length_async<R: AsyncRead + Unpin + Send>(
        io_read: &mut R,
        src: &mut BytesMut,
    ) -> FrameResult<usize> {
        let line = Resp3::decode_line_async(io_read, src).await?;
        let len = util::atoi(&line).map_err(|e| FrameError::InvalidFormat { msg: e })?;
        Ok(len)
    }

    #[inline]
    async fn decode_string_async<
        R: AsyncRead + Unpin + Send,
        S: AsRef<str> + for<'a> From<&'a str>,
    >(
        io_read: &mut R,
        src: &mut BytesMut,
    ) -> FrameResult<S> {
        let line = Resp3::decode_line_async(io_read, src).await?;
        let string = S::from(
            std::str::from_utf8(&line)
                .map_err(|e| FrameError::InvalidFormat { msg: e.to_string() })?,
        );
        Ok(string)
    }

    #[inline]
    async fn get_u8_async<'a, R: AsyncRead + Unpin + Send>(
        io_read: &'a mut R,
        src: &'a mut BytesMut,
    ) -> FrameResult<u8> {
        loop {
            if !src.is_empty() {
                return Ok(src.get_u8());
            }

            if io_read.read_buf(src).await? == 0 {
                return Err(FrameError::Incomplete);
            }
        }
    }

    #[inline]
    async fn get_async<'a, R: AsyncRead + Unpin + Send>(
        io_read: &'a mut R,
        src: &'a mut BytesMut,
        range: Range<usize>,
    ) -> FrameResult<&'a [u8]> {
        loop {
            if src.len() >= range.end {
                return Ok(&src[range]);
            }

            if io_read.read_buf(src).await? == 0 {
                return Err(FrameError::Incomplete);
            }
        }
    }

    #[inline]
    fn need_bytes(src: &BytesMut, len: usize) -> FrameResult<()> {
        if src.len() < len {
            return Err(FrameError::Incomplete);
        }

        Ok(())
    }

    #[inline]
    fn decode_line(src: &mut BytesMut) -> FrameResult<BytesMut> {
        if let Some(i) = memchr::memchr(b'\n', src) {
            if i > 0 && src[i - 1] == b'\r' {
                let line = src.split_to(i - 1);
                src.advance(2);

                return Ok(line);
            }
        }

        Err(FrameError::Incomplete)
    }

    #[inline]
    fn decode_until(src: &mut BytesMut, byte: u8) -> FrameResult<BytesMut> {
        if let Some(i) = memchr::memchr(byte, src) {
            let line = src.split_to(i);
            src.advance(1);
            return Ok(line);
        }

        Err(FrameError::Incomplete)
    }

    #[inline]
    fn decode_exact(src: &mut BytesMut, len: usize) -> FrameResult<BytesMut> {
        if src.len() >= len {
            Ok(src.split_to(len))
        } else {
            Err(FrameError::Incomplete)
        }
    }

    #[inline]
    fn decode_decimal(src: &mut BytesMut) -> FrameResult<Int> {
        let line = Resp3::decode_line(src)?;
        let decimal = util::atoi(&line).map_err(|e| FrameError::InvalidFormat { msg: e })?;
        Ok(decimal)
    }

    #[inline]
    fn decode_length(src: &mut BytesMut) -> FrameResult<usize> {
        let line = Resp3::decode_line(src)?;
        let len = util::atoi(&line).map_err(|e| FrameError::InvalidFormat { msg: e })?;
        Ok(len)
    }

    #[inline]
    fn decode_string<S: AsRef<str> + for<'a> From<&'a str>>(src: &mut BytesMut) -> FrameResult<S> {
        let line = Resp3::decode_line(src)?;
        let string = S::from(
            std::str::from_utf8(&line)
                .map_err(|e| FrameError::InvalidFormat { msg: e.to_string() })?,
        );
        Ok(string)
    }

    #[inline]
    fn get_u8(src: &mut BytesMut) -> FrameResult<u8> {
        if !src.is_empty() {
            Ok(src.get_u8())
        } else {
            Err(FrameError::Incomplete)
        }
    }

    #[inline]
    fn get(src: &BytesMut, range: Range<usize>) -> FrameResult<&[u8]> {
        if src.len() >= range.end {
            Ok(&src[range])
        } else {
            Err(FrameError::Incomplete)
        }
    }
}

#[derive(Debug, Clone)]
pub struct RESP3Encoder;

impl<B, S> Encoder<Resp3<B, S>> for RESP3Encoder
where
    B: AsRef<[u8]> + PartialEq,
    S: AsRef<str> + PartialEq,
{
    type Error = FrameError;

    fn encode(&mut self, item: Resp3<B, S>, dst: &mut BytesMut) -> Result<(), Self::Error> {
        item.encode_buf(dst);

        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct RESP3Decoder {
    buf: BytesMut,
}

impl Default for RESP3Decoder {
    fn default() -> Self {
        Self {
            buf: BytesMut::with_capacity(1024),
        }
    }
}

impl Decoder for RESP3Decoder {
    type Error = FrameError;
    type Item = Resp3;

    // 如果src中的数据不完整，会引发io::ErrorKind::UnexpectedEof错误
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        self.buf.unsplit(src.split());

        if self.buf.is_empty() {
            return Ok(None);
        }

        let origin = self.buf.as_ptr();

        #[inline]
        fn _decode(
            decoder: &mut RESP3Decoder,
        ) -> Result<<RESP3Decoder as Decoder>::Item, <RESP3Decoder as Decoder>::Error> {
            let src = &mut decoder.buf;

            if src.is_empty() {
                return Err(FrameError::Incomplete);
            }

            let res = match src.get_u8() {
                SIMPLE_STRING_PREFIX => Resp3::SimpleString {
                    inner: Resp3::decode_string(src)?,
                    attributes: None,
                },
                ERROR_PREFIX => Resp3::SimpleError {
                    inner: Resp3::decode_string(src)?,
                    attributes: None,
                },
                INTEGER_PREFIX => Resp3::Integer {
                    inner: Resp3::decode_decimal(src)?,
                    attributes: None,
                },
                BLOB_STRING_PREFIX => {
                    let line = Resp3::decode_line(src)?;

                    if Resp3::get(&line, 0..1)?[0] == CHUNKED_STRING_PREFIX {
                        let mut chunks = Vec::new();
                        loop {
                            let mut line = Resp3::decode_line(src)?;

                            if Resp3::get_u8(&mut line)? != CHUNKED_STRING_LENGTH_PREFIX {
                                return Err(FrameError::InvalidFormat {
                                    msg: "invalid chunk length prefix".to_string(),
                                });
                            }

                            let len = util::atoi(&line).map_err(|_| {
                                io::Error::new(io::ErrorKind::InvalidData, "invalid length")
                            })?;

                            if len == 0 {
                                break;
                            }

                            Resp3::need_bytes(src, len + 2)?;
                            let res = src.split_to(len);
                            src.advance(2);

                            chunks.push(res.freeze());
                        }

                        Resp3::ChunkedString(chunks)
                    } else {
                        let len = util::atoi(&line).map_err(|_| {
                            io::Error::new(io::ErrorKind::InvalidData, "invalid length")
                        })?;

                        Resp3::need_bytes(src, len + 2)?;
                        let res = src.split_to(len);
                        src.advance(2);

                        Resp3::BlobString {
                            inner: res.freeze(),
                            attributes: None,
                        }
                    }
                }
                ARRAY_PREFIX => {
                    let len = Resp3::decode_length(src)?;

                    let mut frames = Vec::with_capacity(len);
                    for _ in 0..len {
                        let frame = _decode(decoder)?;
                        frames.push(frame);
                    }

                    Resp3::Array {
                        inner: frames,
                        attributes: None,
                    }
                }
                NULL_PREFIX => {
                    Resp3::need_bytes(src, 2)?;
                    src.advance(2);
                    Resp3::Null
                }
                BOOLEAN_PREFIX => {
                    Resp3::need_bytes(src, 3)?;

                    let b = match src[0] {
                        b't' => true,
                        b'f' => false,
                        _ => {
                            return Err(FrameError::InvalidFormat {
                                msg: "invalid boolean".to_string(),
                            });
                        }
                    };
                    src.advance(3);

                    Resp3::Boolean {
                        inner: b,
                        attributes: None,
                    }
                }
                DOUBLE_PREFIX => {
                    let line = Resp3::decode_line(src)?;

                    let double = atof(&line)
                        .map_err(|e| FrameError::InvalidFormat { msg: e.to_string() })?;

                    Resp3::Double {
                        inner: double,
                        attributes: None,
                    }
                }
                BIG_NUMBER_PREFIX => {
                    let line = Resp3::decode_line(src)?;

                    let n = BigInt::parse_bytes(&line, 10).ok_or_else(|| {
                        FrameError::InvalidFormat {
                            msg: "invalid big number".to_string(),
                        }
                    })?;

                    Resp3::BigNumber {
                        inner: n,
                        attributes: None,
                    }
                }
                BLOB_ERROR_PREFIX => {
                    let len = Resp3::decode_length(src)?;

                    Resp3::need_bytes(src, len + 2)?;
                    let e = src.split_to(len);
                    src.advance(2);

                    Resp3::BlobError {
                        inner: e.freeze(),

                        attributes: None,
                    }
                }
                VERBATIM_STRING_PREFIX => {
                    let len = Resp3::decode_length(src)?;

                    Resp3::need_bytes(src, len + 2)?;

                    let format = src[0..3].try_into().unwrap();
                    src.advance(4);

                    let data = src.split_to(len - 4).freeze();
                    src.advance(2);

                    Resp3::VerbatimString {
                        format,
                        data,
                        attributes: None,
                    }
                }
                MAP_PREFIX => {
                    let len = Resp3::decode_length(src)?;

                    let mut map = AHashMap::with_capacity(len);
                    for _ in 0..len {
                        let k = _decode(decoder)?;
                        let v = _decode(decoder)?;
                        map.insert(k, v);
                    }

                    // map的key由客户端保证唯一
                    Resp3::Map {
                        inner: map,
                        attributes: None,
                    }
                }
                SET_PREFIX => {
                    let len = Resp3::decode_length(src)?;

                    let mut set = AHashSet::with_capacity(len);
                    for _ in 0..len {
                        let frame = _decode(decoder)?;
                        set.insert(frame);
                    }

                    // set的元素由客户端保证唯一
                    Resp3::Set {
                        inner: set,
                        attributes: None,
                    }
                }
                PUSH_PREFIX => {
                    let len = Resp3::decode_length(src)?;

                    let mut frames = Vec::with_capacity(len);
                    for _ in 0..len {
                        let frame = _decode(decoder)?;
                        frames.push(frame);
                    }

                    Resp3::Push {
                        inner: frames,
                        attributes: None,
                    }
                }
                b'H' => {
                    let mut line = Resp3::decode_line(src)?;

                    let ello = Resp3::decode_until(&mut line, b' ')?;
                    if ello != b"ELLO".as_slice() {
                        return Err(FrameError::InvalidFormat {
                            msg: "expect 'HELLO'".to_string(),
                        });
                    }

                    let version =
                        util::atoi(&Resp3::decode_until(&mut line, b' ')?).map_err(|e| {
                            FrameError::InvalidFormat {
                                msg: format!("invalid version: {}", e),
                            }
                        })?;

                    if line.is_empty() {
                        Resp3::Hello {
                            version,
                            auth: None,
                        }
                    } else {
                        let auth = Resp3::decode_until(&mut line, b' ')?;
                        if auth != b"AUTH".as_slice() {
                            return Err(FrameError::InvalidFormat {
                                msg: "invalid auth".to_string(),
                            });
                        }

                        let username = Resp3::decode_until(&mut line, b' ')?.freeze();

                        let password = line.split().freeze();

                        Resp3::Hello {
                            version,
                            auth: Some((username, password)),
                        }
                    }
                }
                prefix => {
                    return Err(FrameError::InvalidFormat {
                        msg: format!("invalid prefix: {}", prefix),
                    });
                }
            };

            Ok(res)
        }

        let res = _decode(self);
        match res {
            Err(FrameError::Incomplete) => {
                // 恢复消耗的数据
                let consume = unsafe {
                    slice_from_raw_parts(origin, self.buf.as_ptr() as usize - origin as usize)
                        .as_ref()
                        .unwrap()
                };
                let temp = self.buf.split();
                self.buf.put_slice(consume);
                self.buf.unsplit(temp);
                Ok(None)
            }
            res => Ok(Some(res?)),
        }
    }
}

impl<B, S> Hash for Resp3<B, S>
where
    B: AsRef<[u8]> + PartialEq,
    S: AsRef<str> + PartialEq,
{
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        let attrs_hash = |attrs: &Attributes<B, S>| {
            attrs.iter().for_each(|(k, v)| {
                k.hash(state);
                v.hash(state);
            });
        };
        match self {
            Resp3::SimpleString { inner, attributes } => {
                attributes.as_ref().map(attrs_hash);
                inner.as_ref().hash(state);
            }
            Resp3::SimpleError { inner, attributes } => {
                attributes.as_ref().map(attrs_hash);
                inner.as_ref().hash(state);
            }
            Resp3::Integer { inner, attributes } => {
                attributes.as_ref().map(attrs_hash);
                inner.hash(state)
            }
            Resp3::BlobString { inner, attributes } => {
                attributes.as_ref().map(attrs_hash);
                inner.as_ref().hash(state)
            }
            Resp3::Array { inner, attributes } => {
                attributes.as_ref().map(attrs_hash);
                inner.hash(state)
            }
            Resp3::Null => state.write_u8(0),
            Resp3::Boolean { inner, attributes } => {
                attributes.as_ref().map(attrs_hash);
                inner.hash(state)
            }
            Resp3::Double { inner, attributes } => {
                attributes.as_ref().map(attrs_hash);
                inner.to_bits().hash(state);
            }
            Resp3::BigNumber { inner, attributes } => {
                attributes.as_ref().map(attrs_hash);
                inner.hash(state)
            }
            Resp3::BlobError { inner, attributes } => {
                attributes.as_ref().map(attrs_hash);
                inner.as_ref().hash(state)
            }
            Resp3::VerbatimString {
                format,
                data,
                attributes,
            } => {
                attributes.as_ref().map(attrs_hash);
                format.hash(state);
                data.as_ref().hash(state);
            }
            Resp3::Map { inner, attributes } => {
                attributes.as_ref().map(attrs_hash);
                inner.iter().for_each(|(k, v)| {
                    k.hash(state);
                    v.hash(state);
                })
            }
            Resp3::Set { inner, attributes } => {
                attributes.as_ref().map(attrs_hash);
                inner.iter().for_each(|f| f.hash(state))
            }
            Resp3::Push { inner, attributes } => {
                attributes.as_ref().map(attrs_hash);
                inner.hash(state);
            }
            Resp3::ChunkedString(chunks) => chunks.iter().for_each(|c| c.as_ref().hash(state)),
            Resp3::Hello { version, auth } => {
                state.write_u8(1);
                version.hash(state);
                if let Some((username, password)) = auth.as_ref() {
                    username.as_ref().hash(state);
                    password.as_ref().hash(state);
                }
            }
        }
    }
}

impl<B, S> Eq for Resp3<B, S>
where
    B: AsRef<[u8]> + PartialEq,
    S: AsRef<str> + PartialEq,
{
}

impl<B, S> PartialEq for Resp3<B, S>
where
    B: AsRef<[u8]> + PartialEq,
    S: AsRef<str> + PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (
                Resp3::SimpleString {
                    inner: inner1,
                    attributes: attributes1,
                },
                Resp3::SimpleString {
                    inner: inner2,
                    attributes: attributes2,
                },
            ) => inner1 == inner2 && attributes1 == attributes2,
            (
                Resp3::SimpleError {
                    inner: inner1,
                    attributes: attributes1,
                },
                Resp3::SimpleError {
                    inner: inner2,
                    attributes: attributes2,
                },
            ) => inner1 == inner2 && attributes1 == attributes2,
            (
                Resp3::Integer {
                    inner: inner1,
                    attributes: attributes1,
                },
                Resp3::Integer {
                    inner: inner2,
                    attributes: attributes2,
                },
            ) => inner1 == inner2 && attributes1 == attributes2,
            (
                Resp3::BlobString {
                    inner: inner1,
                    attributes: attributes1,
                },
                Resp3::BlobString {
                    inner: inner2,
                    attributes: attributes2,
                },
            ) => inner1 == inner2 && attributes1 == attributes2,
            (
                Resp3::Array {
                    inner: inner1,
                    attributes: attributes1,
                },
                Resp3::Array {
                    inner: inner2,
                    attributes: attributes2,
                },
            ) => inner1 == inner2 && attributes1 == attributes2,
            (Resp3::Null, Resp3::Null) => true,
            (
                Resp3::Boolean {
                    inner: inner1,
                    attributes: attributes1,
                },
                Resp3::Boolean {
                    inner: inner2,
                    attributes: attributes2,
                },
            ) => inner1 == inner2 && attributes1 == attributes2,
            (
                Resp3::Double {
                    inner: inner1,
                    attributes: attributes1,
                },
                Resp3::Double {
                    inner: inner2,
                    attributes: attributes2,
                },
            ) => inner1 == inner2 && attributes1 == attributes2,
            (
                Resp3::BigNumber {
                    inner: inner1,
                    attributes: attributes1,
                },
                Resp3::BigNumber {
                    inner: inner2,
                    attributes: attributes2,
                },
            ) => inner1 == inner2 && attributes1 == attributes2,
            (
                Resp3::BlobError {
                    inner: inner1,
                    attributes: attributes1,
                },
                Resp3::BlobError {
                    inner: inner2,
                    attributes: attributes2,
                },
            ) => inner1 == inner2 && attributes1 == attributes2,
            (
                Resp3::VerbatimString {
                    format: format1,
                    data: data1,
                    attributes: attributes1,
                },
                Resp3::VerbatimString {
                    format: format2,
                    data: data2,
                    attributes: attributes2,
                },
            ) => format1 == format2 && data1 == data2 && attributes1 == attributes2,
            (
                Resp3::Map {
                    inner: inner1,
                    attributes: attributes1,
                },
                Resp3::Map {
                    inner: inner2,
                    attributes: attributes2,
                },
            ) => inner1 == inner2 && attributes1 == attributes2,
            (
                Resp3::Set {
                    inner: inner1,
                    attributes: attributes1,
                },
                Resp3::Set {
                    inner: inner2,
                    attributes: attributes2,
                },
            ) => inner1 == inner2 && attributes1 == attributes2,
            (
                Resp3::Push {
                    inner: inner1,
                    attributes: attributes1,
                },
                Resp3::Push {
                    inner: inner2,
                    attributes: attributes2,
                },
            ) => inner1 == inner2 && attributes1 == attributes2,
            (Resp3::ChunkedString(chunks1), Resp3::ChunkedString(chunks2)) => chunks1 == chunks2,
            (
                Resp3::Hello {
                    version: version1,
                    auth: auth1,
                },
                Resp3::Hello {
                    version: version2,
                    auth: auth2,
                },
            ) => version1 == version2 && auth1 == auth2,
            _ => false,
        }
    }
}

impl<S: AsRef<str> + PartialEq> mlua::IntoLua<'_> for Resp3<Bytes, S> {
    fn into_lua(self, lua: &'_ Lua) -> LuaResult<LuaValue<'_>> {
        match self {
            // SimpleString -> Lua Table { ok: Lua String }
            Resp3::SimpleString { inner, .. } => {
                let table = lua.create_table()?;
                table.set("ok", inner.as_ref())?;
                Ok(LuaValue::Table(table))
            }
            // SimpleError -> Lua Table { err: Lua String }
            Resp3::SimpleError { inner, .. } => {
                let table = lua.create_table()?;
                table.set("err", inner.as_ref())?;
                Ok(LuaValue::Table(table))
            }
            // Integer -> Lua Integer
            Resp3::Integer { inner, .. } => inner.into_lua(lua),
            // BlobString -> Lua String
            Resp3::BlobString { inner, .. } => std::str::from_utf8(&inner)
                .map_err(|_| mlua::Error::FromLuaConversionError {
                    from: "Blob",
                    to: "String",
                    message: Some("invalid utf-8 string".to_string()),
                })?
                .into_lua(lua),
            // Array -> Lua Table(Array)
            Resp3::Array { inner, .. } => inner.into_lua(lua),
            // Null -> Lua Nil
            Resp3::Null => Ok(LuaValue::Nil),
            // Boolean -> Lua Boolean
            Resp3::Boolean { inner, .. } => inner.into_lua(lua),
            // Lua table with a single double field containing a Lua number representing the double value.
            // Double -> Lua Table { double: Lua Number }
            Resp3::Double { inner, .. } => {
                let table = lua.create_table()?;
                table.set("double", inner)?;
                Ok(LuaValue::Table(table))
            }
            // Lua table with a single big_number field containing a Lua string representing the big number value.
            // BigNumber -> Lua Table { big_number: Lua String }
            Resp3::BigNumber { inner, .. } => {
                let n = inner.to_str_radix(10);

                let table = lua.create_table()?;
                table.set("big_number", n)?;
                Ok(LuaValue::Table(table))
            }
            // BlobError -> Lua String
            Resp3::BlobError { inner, .. } => inner.into_lua(lua),
            // Lua table with a single verbatim_string field containing a Lua table with two fields, string and format, representing the verbatim string and its format, respectively.
            // VerbatimString -> Lua Table { verbatim_string: Lua Table { string: Lua String, format: Lua String } }
            Resp3::VerbatimString {
                format: encoding,
                data,
                ..
            } => {
                let data = data.into_lua(lua)?;
                let encoding = encoding.into_lua(lua)?;

                let verbatim_string = lua.create_table()?;
                verbatim_string.set("string", data)?;
                verbatim_string.set("format", encoding)?;

                let table = lua.create_table()?;
                table.set("verbatim_string", verbatim_string)?;
                Ok(LuaValue::Table(table))
            }
            // Lua table with a single map field containing a Lua table representing the fields and values of the map.
            // Map -> Lua Table { map: Lua Table }
            Resp3::Map { inner, .. } => {
                let map_table = lua.create_table()?;

                for (k, v) in inner {
                    let k = k.into_lua(lua)?;
                    let v = v.into_lua(lua)?;
                    map_table.set(k, v)?;
                }

                let table = lua.create_table()?;
                table.set("map", map_table)?;
                Ok(LuaValue::Table(table))
            }
            // Lua table with a single set field containing a Lua table representing the elements of the set as fields, each with the Lua Boolean value of true.
            // Set -> Lua Table { set: Lua Table }
            Resp3::Set { inner, .. } => {
                let set_table = lua.create_table()?;

                for f in inner {
                    let f = f.into_lua(lua)?;
                    set_table.set(f, true)?;
                }

                let table = lua.create_table()?;
                table.set("set", set_table)?;
                Ok(Value::Table(table))
            }
            // // Lua table with a single push field containing a Lua table representing the frames of the push message.
            // // Push -> Lua Table { push: Lua Table }
            // RESP3::Push { inner, .. } => {
            //     let push_table = inner.into_lua(lua)?;
            //
            //     let table = lua.create_table()?;
            //     table.set("push", push_table)?;
            //     Ok(Value::Table(table))
            // }
            // // Lua table with a single chunk field containing a Lua table representing the chunks of the chunked message.
            // // ChunkedString -> Lua Table { chunk: Lua Table }
            // RESP3::ChunkedString(chunks) => {
            //     let chunk_table = lua.create_table()?;
            //     for (i, c) in chunks.iter().enumerate() {
            //         let c = std::str::from_utf8(c)
            //             .map_err(|_| mlua::Error::FromLuaConversionError {
            //                 from: "Blob",
            //                 to: "String",
            //                 message: Some("invalid utf-8 string".to_string()),
            //             })?
            //             .into_lua(lua)?;
            //         chunk_table.set(i + 1, c)?;
            //     }
            //
            //     let table = lua.create_table()?;
            //     table.set("chunk", chunk_table)?;
            //     Ok(Value::Table(table))
            // }
            _ => Err(mlua::Error::FromLuaConversionError {
                from: "RESP3",
                to: "LuaValue",
                message: Some("unsupported RESP3 type".to_string()),
            }),
        }
    }
}

impl FromLua<'_> for Resp3 {
    fn from_lua(value: LuaValue<'_>, _lua: &'_ Lua) -> LuaResult<Self> {
        match value {
            // Lua String -> Blob
            LuaValue::String(s) => Ok(Resp3::BlobString {
                inner: Bytes::copy_from_slice(s.as_bytes()),
                attributes: None,
            }),
            // Lua String -> SimpleError
            LuaValue::Integer(n) => Ok(Resp3::Integer {
                inner: n,
                attributes: None,
            }),
            // Lua Number -> Double
            LuaValue::Number(n) => Ok(Resp3::Double {
                inner: n,
                attributes: None,
            }),
            // Lua Nil -> Null
            LuaValue::Nil => Ok(Resp3::Null),
            // Lua Boolean -> Boolean
            LuaValue::Boolean(b) => Ok(Resp3::Boolean {
                inner: b,
                attributes: None,
            }),
            // Lua Table { ok: Lua String } -> SimpleString
            // Lua Table { err: Lua String } -> SimpleError
            // Lua Table { verbatim_string: Lua Table { string: Lua String, format: Lua String } } -> VerbatimString
            // Lua Table { map: Lua Table } -> Map
            // Lua Table { set: Lua Table } -> Set
            // Lua Table { push: Lua Table } -> Push
            // Lua Table -> Array
            LuaValue::Table(table) => {
                let ok = table.raw_get("ok")?;

                if let LuaValue::String(state) = ok {
                    return Ok(Resp3::SimpleString {
                        inner: state.to_str()?.into(),
                        attributes: None,
                    });
                }

                let err = table.raw_get("err")?;

                if let LuaValue::String(e) = err {
                    return Ok(Resp3::SimpleError {
                        inner: e.to_str()?.into(),
                        attributes: None,
                    });
                }

                let verbatim_string = table.raw_get("verbatim_string")?;

                if let LuaValue::Table(verbatim_string) = verbatim_string {
                    let encoding_table: mlua::Table = verbatim_string.raw_get("format")?;
                    if encoding_table.raw_len() != 3 {
                        return Err(mlua::Error::FromLuaConversionError {
                            from: "table",
                            to: "RESP3::VerbatimString",
                            message: Some("invalid encoding format".to_string()),
                        });
                    }

                    let mut encoding = [0; 3];
                    for (i, pair) in encoding_table.pairs::<usize, u8>().enumerate() {
                        let ele = pair?.1;
                        encoding[i] = ele;
                    }

                    let data_table: mlua::Table = verbatim_string.raw_get("string")?;
                    let mut data = BytesMut::with_capacity(data_table.raw_len());
                    for pair in data_table.pairs::<usize, u8>() {
                        let ele = pair?.1;
                        data.put_u8(ele);
                    }

                    return Ok(Resp3::VerbatimString {
                        format: encoding,
                        data: data.freeze(),
                        attributes: None,
                    });
                }

                let map = table.raw_get("map")?;

                if let LuaValue::Table(map) = map {
                    let mut map_table = AHashMap::new();
                    for pair in map.pairs::<LuaValue, LuaValue>() {
                        let (k, v) = pair?;
                        let k = Resp3::from_lua(k, _lua)?;
                        let v = Resp3::from_lua(v, _lua)?;
                        map_table.insert(k, v);
                    }

                    return Ok(Resp3::Map {
                        inner: map_table,
                        attributes: None,
                    });
                }

                let set = table.raw_get("set")?;

                if let LuaValue::Table(set) = set {
                    let mut set_table = AHashSet::new();
                    for pair in set.pairs::<LuaValue, bool>() {
                        let (f, _) = pair?;
                        let f = Resp3::from_lua(f, _lua)?;
                        set_table.insert(f);
                    }

                    return Ok(Resp3::Set {
                        inner: set_table,
                        attributes: None,
                    });
                }

                // let push = table.raw_get("push")?;
                //
                // if let LuaValue::Table(push) = push {
                //     let mut push_table = Vec::with_capacity(push.raw_len());
                //     for pair in push.pairs::<usize, LuaValue>() {
                //         let ele = pair?.1;
                //         push_table.push(RESP3::from_lua(ele, _lua)?);
                //     }
                //
                //     return Ok(RESP3::Push {
                //         inner: push_table,
                //         attributes: None,
                //     });
                // }
                //
                // let chunk = table.raw_get("chunk")?;
                //
                // if let LuaValue::Table(chunk) = chunk {
                //     let mut chunk_table = Vec::with_capacity(chunk.raw_len());
                //     for pair in chunk.pairs::<usize, LuaValue>() {
                //         let ele = pair?.1;
                //
                //         debug_assert!(ele.is_string());
                //
                //         let ele = ele.as_string().unwrap();
                //         chunk_table.push(Bytes::copy_from_slice(ele.as_bytes()));
                //     }
                //
                //     return Ok(RESP3::ChunkedString(chunk_table));
                // }

                let mut array = Vec::with_capacity(table.raw_len());
                for pair in table.pairs::<usize, Value>() {
                    let ele = pair?.1;
                    array.push(Resp3::from_lua(ele, _lua)?);
                }

                Ok(Resp3::Array {
                    inner: array,
                    attributes: None,
                })
            }
            _ => Err(mlua::Error::FromLuaConversionError {
                from: value.type_name(),
                to: "RESP3",
                message: Some("invalid value type".to_string()),
            }),
        }
    }
}

fn encode_attributes<B, S>(buf: &mut impl BufMut, attr: &Attributes<B, S>)
where
    B: AsRef<[u8]> + PartialEq,
    S: AsRef<str> + PartialEq,
{
    buf.put_u8(b'|');
    buf.put_slice(itoa::Buffer::new().format(attr.len()).as_bytes());
    buf.put_slice(CRLF);
    for (k, v) in attr {
        k.encode_buf(buf);
        v.encode_buf(buf);
    }
}

#[cfg(test)]
mod frame_tests {
    use super::*;

    #[test]
    fn decode_resume() {
        let mut decoder = RESP3Decoder::default();

        let mut src = BytesMut::from("*2\r\n");
        let src_clone = src.clone();

        assert!(decoder.decode(&mut src).unwrap().is_none());

        assert_eq!(decoder.buf, src_clone);
    }

    #[test]
    fn encode_decode_test() {
        let cases = vec![
            (
                Resp3::SimpleString {
                    inner: "OK".into(),
                    attributes: None,
                },
                b"+OK\r\n".to_vec(),
            ),
            (
                Resp3::SimpleError {
                    inner: "ERR".into(),
                    attributes: None,
                },
                b"-ERR\r\n".to_vec(),
            ),
            (
                Resp3::Integer {
                    inner: 42,
                    attributes: None,
                },
                b":42\r\n".to_vec(),
            ),
            (
                Resp3::BlobString {
                    inner: Bytes::from("blob data"),
                    attributes: None,
                },
                b"$9\r\nblob data\r\n".to_vec(),
            ),
            (
                Resp3::Array {
                    inner: vec![
                        Resp3::Integer {
                            inner: 1,
                            attributes: None,
                        },
                        Resp3::Integer {
                            inner: 2,
                            attributes: None,
                        },
                        Resp3::Integer {
                            inner: 3,
                            attributes: None,
                        },
                    ],
                    attributes: None,
                },
                b"*3\r\n:1\r\n:2\r\n:3\r\n".to_vec(),
            ),
            (Resp3::Null, b"_\r\n".to_vec()),
            (
                Resp3::Boolean {
                    inner: true,
                    attributes: None,
                },
                b"#t\r\n".to_vec(),
            ),
            (
                Resp3::Double {
                    inner: 3.15,
                    attributes: None,
                },
                b",3.15\r\n".to_vec(),
            ),
            (
                Resp3::BigNumber {
                    inner: BigInt::from(1234567890),
                    attributes: None,
                },
                b"(1234567890\r\n".to_vec(),
            ),
            (
                Resp3::BlobError {
                    inner: Bytes::from("blob error"),
                    attributes: None,
                },
                b"!10\r\nblob error\r\n".to_vec(),
            ),
            (
                Resp3::VerbatimString {
                    format: *b"txt",
                    data: Bytes::from("Some string"),
                    attributes: None,
                },
                b"=15\r\ntxt:Some string\r\n".to_vec(),
            ),
            (
                Resp3::Map {
                    inner: {
                        let mut map = AHashMap::new();
                        map.insert(
                            Resp3::SimpleString {
                                inner: "key".into(),
                                attributes: None,
                            },
                            Resp3::SimpleString {
                                inner: "value".into(),
                                attributes: None,
                            },
                        );
                        map
                    },
                    attributes: None,
                },
                b"%1\r\n+key\r\n+value\r\n".to_vec(),
            ),
            (
                Resp3::Set {
                    inner: {
                        let mut set = AHashSet::new();
                        set.insert(Resp3::SimpleString {
                            inner: "element".into(),
                            attributes: None,
                        });
                        set
                    },
                    attributes: None,
                },
                b"~1\r\n+element\r\n".to_vec(),
            ),
            (
                Resp3::Push {
                    inner: vec![Resp3::SimpleString {
                        inner: "push".into(),
                        attributes: None,
                    }],
                    attributes: None,
                },
                b">1\r\n+push\r\n".to_vec(),
            ),
            (
                Resp3::ChunkedString(vec![
                    Bytes::from("chunk1"),
                    Bytes::from("chunk2"),
                    Bytes::from("chunk3"),
                ]),
                b"$?\r\n;6\r\nchunk1\r\n;6\r\nchunk2\r\n;6\r\nchunk3\r\n;0\r\n".to_vec(),
            ),
            (
                Resp3::Hello {
                    version: 1,
                    auth: Some(("user".into(), "password".into())),
                },
                b"HELLO 1 AUTH user password\r\n".to_vec(),
            ),
        ];

        for (case, expected_encoding) in cases {
            let mut encoder = RESP3Encoder;
            let mut decoder = RESP3Decoder::default();
            let mut buf = BytesMut::new();

            // Encode the case
            encoder.encode(case.clone(), &mut buf).unwrap();

            // Assert the encoded result is correct
            assert_eq!(
                &buf[..],
                &expected_encoding[..],
                "Encoded result for case {:?} is incorrect",
                case
            );

            // Decode the encoded data
            tracing::info!("parsing {:?}", buf);
            let decoded = decoder.decode(&mut buf).unwrap().unwrap();

            // Assert the decoded value is the same as the original case
            assert_eq!(
                case, decoded,
                "Decoded result for case {:?} does not match the original",
                case
            );
        }
    }
}
