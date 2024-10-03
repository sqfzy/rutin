use crate::{
    error::{RutinError, RutinResult},
    server::using_local_buf,
    util::{self, atof, StaticBytes, StaticStr},
    Int,
};
use ahash::{AHashMap, AHashSet};
use bytes::{Buf, Bytes, BytesMut};
use bytestring::ByteString;
use core::str;
use futures::FutureExt;
use itertools::Itertools;
use mlua::{prelude::*, Value};
use num_bigint::BigInt;
use std::{
    collections::VecDeque,
    fmt::Display,
    hash::Hash,
    intrinsics::{likely, unlikely},
    io::Cursor,
    iter::Iterator,
    mem::transmute,
    ptr::slice_from_raw_parts_mut,
};
use strum::{EnumDiscriminants, IntoStaticStr};
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio_util::{
    bytes::BufMut,
    codec::{Decoder, Encoder},
};
use tracing::{instrument, trace};

pub const CRLF: &[u8] = b"\r\n";

pub const SIMPLE_STRING_PREFIX: u8 = b'+';
pub const SIMPLE_ERROR_PREFIX: u8 = b'-';
pub const INTEGER_PREFIX: u8 = b':';
pub const BLOB_STRING_PREFIX: u8 = b'$';
pub const ARRAY_PREFIX: u8 = b'*';
pub const NULL_PREFIX: u8 = b'_';
pub const BOOLEAN_PREFIX: u8 = b'#';
pub const DOUBLE_PREFIX: u8 = b',';
pub const BIG_NUMBER_PREFIX: u8 = b'(';
pub const BLOB_ERROR_PREFIX: u8 = b'!';
pub const VERBATIM_STRING_PREFIX: u8 = b'=';
pub const MAP_PREFIX: u8 = b'%';
pub const SET_PREFIX: u8 = b'~';
pub const PUSH_PREFIX: u8 = b'>';
pub const CHUNKED_STRING_PREFIX: u8 = b'?';
pub const CHUNKED_STRING_LENGTH_PREFIX: u8 = b';';

pub type CheapResp3 = Resp3<Bytes, ByteString>;
pub type ExpensiveResp3 = Resp3<BytesMut, String>;
/// Safty: 通过extend lifetime 实现zero copy的帧解析，在执行reader_buf.clear()之后即失效，不应当再被使用。
/// **因此所有期望其在clear之后仍然有效的操作都必须复制一份新的数据，并且在使用StaticBytes时不允许执行drop
/// 释放其中的数据**
pub type StaticResp3 = Resp3<StaticBytes, StaticStr>;

// pub type RefMutResp3<'a> = Resp3<&'a mut [u8], &'a mut str>;
// pub type RefResp3<'a> = Resp3<&'a [u8], &'a str>;

pub type Attributes<B, S> = AHashMap<Resp3<B, S>, Resp3<B, S>>;

#[derive(Clone, Debug, PartialEq, Default, IntoStaticStr, EnumDiscriminants)]
#[strum_discriminants(vis(pub), name(Resp3Type))]
pub enum Resp3<B, S>
where
    B: AsRef<[u8]> + PartialEq + std::fmt::Debug,
    S: AsRef<str> + PartialEq + std::fmt::Debug,
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
        inner: VecDeque<Resp3<B, S>>,
        attributes: Option<Attributes<B, S>>,
    },

    #[default]
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
        auth: Option<(S, S)>,
    },
}

impl<B, S> Resp3<B, S>
where
    B: AsRef<[u8]> + PartialEq + std::fmt::Debug,
    S: AsRef<str> + PartialEq + std::fmt::Debug,
{
    // TODO: test
    pub fn size(&self) -> usize {
        fn attributes_size<B, S>(attributes: &Option<Attributes<B, S>>) -> usize
        where
            B: AsRef<[u8]> + PartialEq + std::fmt::Debug,
            S: AsRef<str> + PartialEq + std::fmt::Debug,
        {
            if let Some(attributes) = attributes {
                let mut size = itoa::Buffer::new().format(attributes.len()).len() + 3;
                for (k, v) in attributes {
                    size += k.size() + v.size();
                }
                size
            } else {
                0
            }
        }

        match self {
            Resp3::SimpleString { inner, attributes } => {
                inner.as_ref().len() + 3 + attributes_size(attributes)
            }
            Resp3::SimpleError { inner, attributes } => {
                inner.as_ref().len() + 3 + attributes_size(attributes)
            }
            Resp3::Integer { inner, attributes } => {
                itoa::Buffer::new().format(*inner).len() + 3 + attributes_size(attributes)
            }
            Resp3::BlobString { inner, attributes } => {
                itoa::Buffer::new().format(inner.as_ref().len()).len()
                    + 3
                    + inner.as_ref().len()
                    + 2
                    + attributes_size(attributes)
            }
            Resp3::BlobError { inner, attributes } => {
                itoa::Buffer::new().format(inner.as_ref().len()).len()
                    + 3
                    + inner.as_ref().len()
                    + 2
                    + attributes_size(attributes)
            }
            Resp3::Array { inner, attributes } => {
                let mut size =
                    itoa::Buffer::new().format(inner.len()).len() + 3 + attributes_size(attributes);
                for r in inner {
                    size += r.size();
                }
                size
            }
            Resp3::Null => 3,
            Resp3::Boolean { attributes, .. } => 3 + attributes_size(attributes),
            Resp3::Double { inner, attributes } => {
                ryu::Buffer::new().format(*inner).len() + 3 + attributes_size(attributes)
            }
            Resp3::BigNumber { inner, attributes } => {
                inner.to_str_radix(10).len() + 3 + attributes_size(attributes)
            }
            Resp3::VerbatimString {
                data, attributes, ..
            } => 3 + 1 + 3 + 1 + data.as_ref().len() + 2 + attributes_size(attributes),
            Resp3::Map { inner, attributes } => {
                let mut size =
                    itoa::Buffer::new().format(inner.len()).len() + 3 + attributes_size(attributes);
                for (k, v) in inner {
                    size += k.size() + v.size();
                }

                size
            }
            Resp3::Set { inner, attributes } => {
                let mut size =
                    itoa::Buffer::new().format(inner.len()).len() + 3 + attributes_size(attributes);
                for r in inner {
                    size += r.size();
                }
                size
            }
            Resp3::Push { inner, attributes } => {
                let mut size =
                    itoa::Buffer::new().format(inner.len()).len() + 3 + attributes_size(attributes);
                for r in inner {
                    size += r.size();
                }
                size
            }
            Resp3::ChunkedString(chunks) => {
                let mut size = 3;
                for chunk in chunks {
                    size += 1 + itoa::Buffer::new().format(chunk.as_ref().len()).len() + 2;
                    size += chunk.as_ref().len() + 2;
                }
                size
            }
            Resp3::Hello { version, auth } => {
                let mut size = itoa::Buffer::new().format(*version).len() + 1;
                if let Some((username, password)) = auth {
                    size += username.as_ref().len() + 1 + password.as_ref().len() + 1;
                }
                size
            }
        }
    }

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

    pub fn new_array(array: impl Into<VecDeque<Resp3<B, S>>>) -> Self {
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

    pub fn as_simple_string(&self) -> Option<&S> {
        match self {
            Resp3::SimpleString { inner, .. } => Some(inner),
            _ => None,
        }
    }

    pub fn as_simple_error(&self) -> Option<&S> {
        match self {
            Resp3::SimpleError { inner, .. } => Some(inner),
            _ => None,
        }
    }

    pub fn as_integer(&self) -> Option<Int> {
        match self {
            Resp3::Integer { inner, .. } => Some(*inner),
            _ => None,
        }
    }

    pub fn as_blob_string(&self) -> Option<&B> {
        match self {
            Resp3::BlobString { inner, .. } => Some(inner),
            _ => None,
        }
    }

    pub fn as_array(&self) -> Option<&VecDeque<Resp3<B, S>>> {
        match self {
            Resp3::Array { inner, .. } => Some(inner),
            _ => None,
        }
    }

    pub fn as_null(&self) -> Option<()> {
        match self {
            Resp3::Null => Some(()),
            _ => None,
        }
    }

    pub fn as_boolean(&self) -> Option<bool> {
        match self {
            Resp3::Boolean { inner, .. } => Some(*inner),
            _ => None,
        }
    }

    pub fn as_double(&self) -> Option<f64> {
        match self {
            Resp3::Double { inner, .. } => Some(*inner),
            _ => None,
        }
    }

    pub fn as_big_number(&self) -> Option<&BigInt> {
        match self {
            Resp3::BigNumber { inner, .. } => Some(inner),
            _ => None,
        }
    }

    pub fn as_blob_error(&self) -> Option<&B> {
        match self {
            Resp3::BlobError { inner, .. } => Some(inner),
            _ => None,
        }
    }

    pub fn as_verbatim_string(&self) -> Option<(&[u8; 3], &B)> {
        match self {
            Resp3::VerbatimString { format, data, .. } => Some((format, data)),
            _ => None,
        }
    }

    #[allow(clippy::type_complexity)]
    pub fn as_map(&self) -> Option<&AHashMap<Resp3<B, S>, Resp3<B, S>>> {
        match self {
            Resp3::Map { inner, .. } => Some(inner),
            _ => None,
        }
    }

    pub fn as_set(&self) -> Option<&AHashSet<Resp3<B, S>>> {
        match self {
            Resp3::Set { inner, .. } => Some(inner),
            _ => None,
        }
    }

    pub fn as_push(&self) -> Option<&Vec<Resp3<B, S>>> {
        match self {
            Resp3::Push { inner, .. } => Some(inner),
            _ => None,
        }
    }

    pub fn as_simple_string_mut(&mut self) -> Option<&mut S> {
        match self {
            Resp3::SimpleString { inner, .. } => Some(inner),
            _ => None,
        }
    }

    pub fn as_simple_error_mut(&mut self) -> Option<&mut S> {
        match self {
            Resp3::SimpleError { inner, .. } => Some(inner),
            _ => None,
        }
    }

    pub fn as_integer_mut(&mut self) -> Option<&mut Int> {
        match self {
            Resp3::Integer { inner, .. } => Some(inner),
            _ => None,
        }
    }

    pub fn as_blob_mut(&mut self) -> Option<&mut B> {
        match self {
            Resp3::BlobString { inner, .. } => Some(inner),
            _ => None,
        }
    }

    pub fn as_array_mut(&mut self) -> Option<&mut VecDeque<Resp3<B, S>>> {
        match self {
            Resp3::Array { inner, .. } => Some(inner),
            _ => None,
        }
    }

    pub fn as_boolean_mut(&mut self) -> Option<&mut bool> {
        match self {
            Resp3::Boolean { inner, .. } => Some(inner),
            _ => None,
        }
    }

    pub fn as_double_mut(&mut self) -> Option<&mut f64> {
        match self {
            Resp3::Double { inner, .. } => Some(inner),
            _ => None,
        }
    }

    pub fn as_big_number_mut(&mut self) -> Option<&mut BigInt> {
        match self {
            Resp3::BigNumber { inner, .. } => Some(inner),
            _ => None,
        }
    }

    pub fn as_blob_error_mut(&mut self) -> Option<&mut B> {
        match self {
            Resp3::BlobError { inner, .. } => Some(inner),
            _ => None,
        }
    }

    pub fn as_verbatim_string_mut(&mut self) -> Option<(&mut [u8; 3], &mut B)> {
        match self {
            Resp3::VerbatimString { format, data, .. } => Some((format, data)),
            _ => None,
        }
    }

    #[allow(clippy::type_complexity)]
    pub fn as_map_mut(&mut self) -> Option<&mut AHashMap<Resp3<B, S>, Resp3<B, S>>> {
        match self {
            Resp3::Map { inner, .. } => Some(inner),
            _ => None,
        }
    }

    pub fn as_set_mut(&mut self) -> Option<&mut AHashSet<Resp3<B, S>>> {
        match self {
            Resp3::Set { inner, .. } => Some(inner),
            _ => None,
        }
    }

    pub fn as_push_mut(&mut self) -> Option<&mut Vec<Resp3<B, S>>> {
        match self {
            Resp3::Push { inner, .. } => Some(inner),
            _ => None,
        }
    }

    pub fn as_simple_string_unchecked(&self) -> &S {
        match self {
            Resp3::SimpleString { inner, .. } => inner,
            _ => panic!("not a simple string"),
        }
    }

    pub fn as_simple_error_unchecked(&self) -> &S {
        match self {
            Resp3::SimpleError { inner, .. } => inner,
            _ => panic!("not a simple error"),
        }
    }

    pub fn as_integer_unchecked(&self) -> Int {
        match self {
            Resp3::Integer { inner, .. } => *inner,
            _ => panic!("not an integer"),
        }
    }

    pub fn as_blob_string_unchecked(&self) -> &B {
        match self {
            Resp3::BlobString { inner, .. } => inner,
            _ => panic!("not a blob string"),
        }
    }

    pub fn as_array_unchecked(&self) -> &VecDeque<Resp3<B, S>> {
        match self {
            Resp3::Array { inner, .. } => inner,
            _ => panic!("not an array"),
        }
    }

    pub fn as_null_unchecked(&self) {
        match self {
            Resp3::Null => {}
            _ => panic!("not a null"),
        }
    }

    pub fn as_boolean_unchecked(&self) -> bool {
        match self {
            Resp3::Boolean { inner, .. } => *inner,
            _ => panic!("not a boolean"),
        }
    }

    pub fn as_double_unchecked(&self) -> f64 {
        match self {
            Resp3::Double { inner, .. } => *inner,
            _ => panic!("not a double"),
        }
    }

    pub fn as_big_number_unchecked(&self) -> &BigInt {
        match self {
            Resp3::BigNumber { inner, .. } => inner,
            _ => panic!("not a big number"),
        }
    }

    pub fn as_bolb_error_unchecked(&self) -> &B {
        match self {
            Resp3::BlobError { inner, .. } => inner,
            _ => panic!("not a blob error"),
        }
    }

    pub fn as_verbatim_string_unchecked(&self) -> (&[u8; 3], &B) {
        match self {
            Resp3::VerbatimString { format, data, .. } => (format, data),
            _ => panic!("not a verbatim string"),
        }
    }

    pub fn as_map_unchecked(&self) -> &AHashMap<Resp3<B, S>, Resp3<B, S>> {
        match self {
            Resp3::Map { inner, .. } => inner,
            _ => panic!("not a map"),
        }
    }

    pub fn as_set_unchecked(&self) -> &AHashSet<Resp3<B, S>> {
        match self {
            Resp3::Set { inner, .. } => inner,
            _ => panic!("not a set"),
        }
    }

    pub fn as_push_unchecked(&self) -> &Vec<Resp3<B, S>> {
        match self {
            Resp3::Push { inner, .. } => inner,
            _ => panic!("not a push"),
        }
    }

    pub fn as_simple_string_unchecked_mut(&mut self) -> &mut S {
        match self {
            Resp3::SimpleString { inner, .. } => inner,
            _ => panic!("not a simple string"),
        }
    }

    pub fn as_simple_error_unchecked_mut(&mut self) -> &mut S {
        match self {
            Resp3::SimpleError { inner, .. } => inner,
            _ => panic!("not a simple error"),
        }
    }

    pub fn as_integer_unchecked_mut(&mut self) -> Int {
        match self {
            Resp3::Integer { inner, .. } => *inner,
            _ => panic!("not an integer"),
        }
    }

    pub fn as_blob_string_unchecked_mut(&mut self) -> &mut B {
        match self {
            Resp3::BlobString { inner, .. } => inner,
            _ => panic!("not a blob string"),
        }
    }

    pub fn as_array_unchecked_mut(&mut self) -> &mut VecDeque<Resp3<B, S>> {
        match self {
            Resp3::Array { inner, .. } => inner,
            _ => panic!("not an array"),
        }
    }

    pub fn as_null_unchecked_mut(&mut self) {
        match self {
            Resp3::Null => {}
            _ => panic!("not a null"),
        }
    }

    pub fn as_boolean_unchecked_mut(&mut self) -> bool {
        match self {
            Resp3::Boolean { inner, .. } => *inner,
            _ => panic!("not a boolean"),
        }
    }

    pub fn as_double_unchecked_mut(&mut self) -> f64 {
        match self {
            Resp3::Double { inner, .. } => *inner,
            _ => panic!("not a double"),
        }
    }

    pub fn as_big_number_unchecked_mut(&mut self) -> &mut BigInt {
        match self {
            Resp3::BigNumber { inner, .. } => inner,
            _ => panic!("not a big number"),
        }
    }

    pub fn as_bolb_error_unchecked_mut(&mut self) -> &mut B {
        match self {
            Resp3::BlobError { inner, .. } => inner,
            _ => panic!("not a blob error"),
        }
    }

    pub fn as_verbatim_string_unchecked_mut(&mut self) -> (&mut [u8; 3], &mut B) {
        match self {
            Resp3::VerbatimString { format, data, .. } => (format, data),
            _ => panic!("not a verbatim string"),
        }
    }

    pub fn as_map_unchecked_mut(&mut self) -> &mut AHashMap<Resp3<B, S>, Resp3<B, S>> {
        match self {
            Resp3::Map { inner, .. } => inner,
            _ => panic!("not a map"),
        }
    }

    pub fn as_set_unchecked_mut(&mut self) -> &mut AHashSet<Resp3<B, S>> {
        match self {
            Resp3::Set { inner, .. } => inner,
            _ => panic!("not a set"),
        }
    }

    pub fn as_push_unchecked_mut(&mut self) -> &mut Vec<Resp3<B, S>> {
        match self {
            Resp3::Push { inner, .. } => inner,
            _ => panic!("not a push"),
        }
    }

    pub fn into_simple_string(self) -> Option<S> {
        if let Resp3::SimpleString { inner, .. } = self {
            Some(inner)
        } else {
            None
        }
    }

    pub fn into_simple_error(self) -> Option<S> {
        if let Resp3::SimpleError { inner, .. } = self {
            Some(inner)
        } else {
            None
        }
    }

    pub fn into_integer(self) -> Option<Int> {
        if let Resp3::Integer { inner, .. } = self {
            Some(inner)
        } else {
            None
        }
    }

    pub fn into_blob_string(self) -> Option<B> {
        if let Resp3::BlobString { inner, .. } = self {
            Some(inner)
        } else {
            None
        }
    }

    pub fn into_blob_error(self) -> Option<B> {
        if let Resp3::BlobError { inner, .. } = self {
            Some(inner)
        } else {
            None
        }
    }

    pub fn into_array(self) -> Option<VecDeque<Resp3<B, S>>> {
        if let Resp3::Array { inner, .. } = self {
            Some(inner)
        } else {
            None
        }
    }

    pub fn into_null(self) -> Option<()> {
        if let Resp3::Null = self {
            Some(())
        } else {
            None
        }
    }

    pub fn into_boolean(self) -> Option<bool> {
        if let Resp3::Boolean { inner, .. } = self {
            Some(inner)
        } else {
            None
        }
    }

    pub fn into_double(self) -> Option<f64> {
        if let Resp3::Double { inner, .. } = self {
            Some(inner)
        } else {
            None
        }
    }

    pub fn into_big_number(self) -> Option<BigInt> {
        if let Resp3::BigNumber { inner, .. } = self {
            Some(inner)
        } else {
            None
        }
    }

    pub fn into_verbatim_string(self) -> Option<([u8; 3], B)> {
        if let Resp3::VerbatimString { format, data, .. } = self {
            Some((format, data))
        } else {
            None
        }
    }

    pub fn into_map(self) -> Option<AHashMap<Resp3<B, S>, Resp3<B, S>>> {
        if let Resp3::Map { inner, .. } = self {
            Some(inner)
        } else {
            None
        }
    }

    pub fn into_set(self) -> Option<AHashSet<Resp3<B, S>>> {
        if let Resp3::Set { inner, .. } = self {
            Some(inner)
        } else {
            None
        }
    }

    pub fn into_push(self) -> Option<Vec<Resp3<B, S>>> {
        if let Resp3::Push { inner, .. } = self {
            Some(inner)
        } else {
            None
        }
    }

    pub fn into_chunked_string(self) -> Option<Vec<B>> {
        if let Resp3::ChunkedString(inner) = self {
            Some(inner)
        } else {
            None
        }
    }

    pub fn into_hello(self) -> Option<(Int, Option<(S, S)>)> {
        if let Resp3::Hello { version, auth } = self {
            Some((version, auth))
        } else {
            None
        }
    }

    pub fn into_simple_string_unchecked(self) -> S {
        if let Resp3::SimpleString { inner, .. } = self {
            inner
        } else {
            panic!("not a simple string")
        }
    }

    pub fn into_simple_error_unchecked(self) -> S {
        if let Resp3::SimpleError { inner, .. } = self {
            inner
        } else {
            panic!("not a simple error")
        }
    }

    pub fn into_integer_unchecked(self) -> Int {
        if let Resp3::Integer { inner, .. } = self {
            inner
        } else {
            panic!("not an integer")
        }
    }

    pub fn into_blob_string_unchecked(self) -> B {
        if let Resp3::BlobString { inner, .. } = self {
            inner
        } else {
            panic!("not a blob string")
        }
    }

    pub fn into_blob_error_unchecked(self) -> B {
        if let Resp3::BlobError { inner, .. } = self {
            inner
        } else {
            panic!("not a blob error")
        }
    }

    pub fn into_array_unchecked(self) -> VecDeque<Resp3<B, S>> {
        if let Resp3::Array { inner, .. } = self {
            inner
        } else {
            panic!("not an array")
        }
    }

    pub fn into_null_unchecked(self) {
        if let Resp3::Null = self {
        } else {
            panic!("not a null")
        }
    }

    pub fn into_boolean_unchecked(self) -> bool {
        if let Resp3::Boolean { inner, .. } = self {
            inner
        } else {
            panic!("not a boolean")
        }
    }

    pub fn into_double_unchecked(self) -> f64 {
        if let Resp3::Double { inner, .. } = self {
            inner
        } else {
            panic!("not a double")
        }
    }

    pub fn into_big_number_unchecked(self) -> BigInt {
        if let Resp3::BigNumber { inner, .. } = self {
            inner
        } else {
            panic!("not a big number")
        }
    }

    pub fn into_verbatim_string_unchecked(self) -> ([u8; 3], B) {
        if let Resp3::VerbatimString { format, data, .. } = self {
            (format, data)
        } else {
            panic!("not a verbatim string")
        }
    }

    pub fn into_map_unchecked(self) -> AHashMap<Resp3<B, S>, Resp3<B, S>> {
        if let Resp3::Map { inner, .. } = self {
            inner
        } else {
            panic!("not a map")
        }
    }

    pub fn into_push_unchecked(self) -> Vec<Resp3<B, S>> {
        if let Resp3::Push { inner, .. } = self {
            inner
        } else {
            panic!("not a push")
        }
    }

    pub fn into_chunked_string_unchecked(self) -> Vec<B> {
        if let Resp3::ChunkedString(inner) = self {
            inner
        } else {
            panic!("not a chunked string")
        }
    }

    pub fn into_hello_unchecked(self) -> (Int, Option<(S, S)>) {
        if let Resp3::Hello { version, auth } = self {
            (version, auth)
        } else {
            panic!("not a hello")
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
                buf.put_u8(SIMPLE_ERROR_PREFIX);
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
                    buf.put_slice(auth.0.as_ref().as_bytes());
                    buf.put_u8(b' ');
                    buf.put_slice(auth.1.as_ref().as_bytes());
                }
                buf.put_slice(CRLF);
            }
        }
    }

    #[inline]
    pub fn encode_local_buf(&self) -> BytesMut {
        using_local_buf(|buf| self.encode_buf(buf))
    }
}

impl<B, S> Display for Resp3<B, S>
where
    B: AsRef<[u8]> + PartialEq + std::fmt::Debug,
    S: AsRef<str> + PartialEq + std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Resp3::SimpleString { inner, .. } => write!(f, "{}", inner.as_ref()),
            Resp3::SimpleError { inner, .. } => write!(f, "{}", inner.as_ref()),
            Resp3::Integer { inner, .. } => write!(f, "{}", inner),
            Resp3::BlobString { inner, .. } => {
                write!(f, "{}", String::from_utf8_lossy(inner.as_ref()))
            }
            Resp3::Array { inner, .. } => {
                write!(f, "[")?;
                for frame in inner {
                    write!(f, "{}, ", frame)?;
                }
                write!(f, "]")
            }
            Resp3::Null => write!(f, "null"),
            Resp3::Boolean { inner, .. } => write!(f, "{}", inner),
            Resp3::Double { inner, .. } => write!(f, "{}", inner),
            Resp3::BigNumber { inner, .. } => write!(f, "{}", inner),
            Resp3::BlobError { inner, .. } => {
                write!(f, "{}", String::from_utf8_lossy(inner.as_ref()))
            }
            Resp3::VerbatimString { data, .. } => {
                write!(f, "{}", String::from_utf8_lossy(data.as_ref()))
            }
            Resp3::Map { inner, .. } => {
                write!(f, "{{")?;
                for (k, v) in inner {
                    write!(f, "{}: {}, ", k, v)?;
                }
                write!(f, "}}")
            }
            Resp3::Set { inner, .. } => {
                write!(f, "{{")?;
                for frame in inner {
                    write!(f, "{}, ", frame)?;
                }
                write!(f, "}}")
            }
            Resp3::Push { inner, .. } => {
                write!(f, "[")?;
                for frame in inner {
                    write!(f, "{}, ", frame)?;
                }
                write!(f, "]")
            }
            Resp3::ChunkedString(chunks) => {
                write!(f, "ChunkedString(")?;
                for chunk in chunks {
                    write!(f, "{}, ", String::from_utf8_lossy(chunk.as_ref()))?;
                }
                write!(f, ")")
            }
            Resp3::Hello { version, auth } => {
                write!(f, "Hello({}, {:?})", version, auth)
            }
        }
    }
}

// 解码
impl StaticResp3 {
    #[inline]
    #[instrument(level = "trace", skip(io_read), err)]
    pub async fn decode_async<R: AsyncRead + Unpin>(
        io_read: &mut R,
        src: &mut Cursor<BytesMut>,
    ) -> RutinResult<Option<StaticResp3>> {
        if !src.has_remaining() && io_read.read_buf(src.get_mut()).await? == 0 {
            return Ok(None);
        }

        let res = Self::_decode_async(io_read, src).await?;
        Ok(Some(res))
    }

    #[inline]
    async fn _decode_async<R: AsyncRead + Unpin>(
        io_read: &mut R,
        src: &mut Cursor<BytesMut>,
    ) -> RutinResult<StaticResp3> {
        trace!("reader_buf: {src:?}");

        let line = StaticResp3::decode_line_async(io_read, src).await?;

        let prefix = *line.first().ok_or_else(|| RutinError::InvalidFormat {
            msg: "missing prefix".into(),
        })?;
        let line = line.get_mut(1..).unwrap_or_default();

        let res = match prefix {
            BLOB_STRING_PREFIX => {
                if unlikely(line.first().is_some_and(|ch| *ch == CHUNKED_STRING_PREFIX)) {
                    let mut chunks = Vec::new();

                    loop {
                        let line = StaticResp3::decode_line_async(io_read, src).await?;

                        // if !line
                        //     .get(0)
                        //     .is_some_and(|ch| ch == CHUNKED_STRING_LENGTH_PREFIX)
                        // {
                        //     return Err(RutinError::InvalidFormat {
                        //         msg: "invalid chunk length prefix".into(),
                        //     });
                        // }

                        let len: usize =
                            util::atoi(line.get(1..).unwrap_or_default()).map_err(|_| {
                                RutinError::InvalidFormat {
                                    msg: "invalid chunks length".into(),
                                }
                            })?;

                        if len == 0 {
                            break;
                        }

                        StaticResp3::need_bytes_async(io_read, src, len + 2).await?;
                        let pos = src.position() as usize;
                        let chunk = leak_bytes_mut(&mut src.get_mut()[pos..pos + len]);
                        src.advance(len + 2);

                        chunks.push(chunk);
                    }

                    StaticResp3::ChunkedString(chunks)
                } else {
                    let len: usize = util::atoi(line).map_err(|_| RutinError::InvalidFormat {
                        msg: "invalid blob string length".into(),
                    })?;

                    StaticResp3::need_bytes_async(io_read, src, len + 2).await?;
                    let pos = src.position() as usize;
                    let blob = leak_bytes_mut(&mut src.get_mut()[pos..pos + len]);
                    src.advance(len + 2);

                    StaticResp3::BlobString {
                        inner: blob,
                        attributes: None,
                    }
                }
            }
            ARRAY_PREFIX => {
                let len = util::atoi(line).map_err(|_| RutinError::InvalidFormat {
                    msg: "invalid blob string length".into(),
                })?;

                let mut frames = VecDeque::with_capacity(len);

                for _ in 0..len {
                    let frame = Self::_decode_async(io_read, src).boxed_local().await?;
                    frames.push_back(frame);
                }

                StaticResp3::Array {
                    inner: frames,
                    attributes: None,
                }
            }
            SIMPLE_STRING_PREFIX => {
                let string = str::from_utf8_mut(line).map_err(|e| RutinError::InvalidFormat {
                    msg: format!("invalid simple string: {}", e).into(),
                })?;

                StaticResp3::SimpleString {
                    inner: leak_str_mut(string),
                    attributes: None,
                }
            }
            SIMPLE_ERROR_PREFIX => {
                let error = str::from_utf8_mut(line).map_err(|_| RutinError::InvalidFormat {
                    msg: "invalid simple error".into(),
                })?;

                StaticResp3::SimpleError {
                    inner: leak_str_mut(error),
                    attributes: None,
                }
            }
            INTEGER_PREFIX => {
                let integer = util::atoi(line).map_err(|_| RutinError::InvalidFormat {
                    msg: "invalid integer".into(),
                })?;

                StaticResp3::Integer {
                    inner: integer,
                    attributes: None,
                }
            }
            NULL_PREFIX => StaticResp3::Null,
            BOOLEAN_PREFIX => {
                let b = match line.first() {
                    Some(b't') => true,
                    Some(b'f') => false,
                    _ => {
                        return Err(RutinError::InvalidFormat {
                            msg: "invalid boolean".into(),
                        });
                    }
                };

                StaticResp3::Boolean {
                    inner: b,
                    attributes: None,
                }
            }
            DOUBLE_PREFIX => {
                let double = atof(line).map_err(|_| RutinError::InvalidFormat {
                    msg: "invalid double".into(),
                })?;

                StaticResp3::Double {
                    inner: double,
                    attributes: None,
                }
            }
            BIG_NUMBER_PREFIX => {
                let n = BigInt::parse_bytes(line, 10).ok_or_else(|| RutinError::InvalidFormat {
                    msg: "invalid big number".into(),
                })?;

                StaticResp3::BigNumber {
                    inner: n,
                    attributes: None,
                }
            }
            BLOB_ERROR_PREFIX => {
                let len: usize = util::atoi(line).map_err(|_| RutinError::InvalidFormat {
                    msg: "invalid blob error length".into(),
                })?;

                StaticResp3::need_bytes_async(io_read, src, len + 2).await?;
                let pos = src.position() as usize;
                let error = leak_bytes_mut(&mut src.get_mut()[pos..pos + len]);
                src.advance(len + 2);

                StaticResp3::BlobError {
                    inner: error,
                    attributes: None,
                }
            }
            VERBATIM_STRING_PREFIX => {
                let len: usize = util::atoi(line).map_err(|_| RutinError::InvalidFormat {
                    msg: "invalid verbatim string length".into(),
                })?;

                StaticResp3::need_bytes_async(io_read, src, len + 2).await?;

                let format = src.get_ref()[0..3].try_into().unwrap();
                let pos = src.position() as usize;
                let data = leak_bytes_mut(&mut src.get_mut()[pos..pos + len]);
                src.advance(len + 2);

                StaticResp3::VerbatimString {
                    format,
                    data,
                    attributes: None,
                }
            }
            MAP_PREFIX => {
                let len = util::atoi(line).map_err(|_| RutinError::InvalidFormat {
                    msg: "invalid map length".into(),
                })?;

                let mut map = AHashMap::with_capacity(len);

                for _ in 0..len {
                    let k = Self::_decode_async(io_read, src).boxed_local().await?;
                    let v = Self::_decode_async(io_read, src).boxed_local().await?;
                    map.insert(k, v);
                }

                StaticResp3::Map {
                    inner: map,
                    attributes: None,
                }
            }
            SET_PREFIX => {
                let len = util::atoi(line).map_err(|_| RutinError::InvalidFormat {
                    msg: "invalid set length".into(),
                })?;

                let mut set = AHashSet::with_capacity(len);

                for _ in 0..len {
                    let frame = Self::_decode_async(io_read, src).boxed_local().await?;
                    set.insert(frame);
                }

                StaticResp3::Set {
                    inner: set,
                    attributes: None,
                }
            }
            PUSH_PREFIX => {
                let len = util::atoi(line).map_err(|_| RutinError::InvalidFormat {
                    msg: "invalid push length".into(),
                })?;

                let mut frames = Vec::with_capacity(len);

                for _ in 0..len {
                    let frame = Self::_decode_async(io_read, src).boxed_local().await?;
                    frames.push(frame);
                }

                StaticResp3::Push {
                    inner: frames,
                    attributes: None,
                }
            }
            b'H' => {
                let mut vec = line
                    .splitn(5, |&ch| ch == b' ')
                    .map(|slice| {
                        let len = slice.len();
                        leak_bytes_mut(unsafe {
                            &mut *slice_from_raw_parts_mut(slice.as_ptr() as *mut u8, len)
                        })
                    })
                    .collect_vec();

                if vec.len() == 2 {
                    let protocol_version =
                        util::atoi(&vec[1]).map_err(|_| RutinError::InvalidFormat {
                            msg: "invalid protocol version".into(),
                        })?;

                    StaticResp3::Hello {
                        version: protocol_version,
                        auth: None,
                    }
                } else if vec.len() == 5 {
                    let protocol_version =
                        util::atoi(&vec[1]).map_err(|_| RutinError::InvalidFormat {
                            msg: "invalid protocol version".into(),
                        })?;

                    let password = str::from_utf8_mut(unsafe {
                        vec.pop().unwrap().into_inner_mut_unchecked()
                    })
                    .map_err(|_| RutinError::InvalidFormat {
                        msg: "invalid password format".into(),
                    })?;
                    let username = str::from_utf8_mut(unsafe {
                        vec.pop().unwrap().into_inner_mut_unchecked()
                    })
                    .map_err(|_| RutinError::InvalidFormat {
                        msg: "invalid username format".into(),
                    })?;

                    StaticResp3::Hello {
                        version: protocol_version,
                        auth: Some((leak_str_mut(username), leak_str_mut(password))),
                    }
                } else {
                    return Err(RutinError::InvalidFormat {
                        msg: "invalid hello".into(),
                    });
                }
            }
            prefix => {
                return Err(RutinError::InvalidFormat {
                    msg: format!("invalid prefix: {}", prefix).into(),
                });
            }
        };

        Ok(res)
    }

    #[inline]
    #[instrument(level = "trace", skip(io_read), err)]
    pub async fn decode_buf_async<R: AsyncRead + Unpin>(
        io_read: &mut R,
        src: &mut Cursor<BytesMut>,
        frame_buf: &mut StaticResp3,
    ) -> RutinResult<Option<()>> {
        if !src.has_remaining() && io_read.read_buf(src.get_mut()).await? == 0 {
            return Ok(None);
        }

        Self::_decode_buf_async(io_read, src, frame_buf).await?;
        Ok(Some(()))
    }

    #[inline]
    async fn _decode_buf_async<R: AsyncRead + Unpin>(
        io_read: &mut R,
        src: &mut Cursor<BytesMut>,
        frame_buf: &mut StaticResp3,
    ) -> RutinResult<()> {
        trace!("reader_buf: {src:?}");

        reset_frame_buf(frame_buf);

        let line = StaticResp3::decode_line_async(io_read, src).await?;

        let prefix = *line.first().ok_or_else(|| RutinError::InvalidFormat {
            msg: "missing prefix".into(),
        })?;
        let line = line.get_mut(1..).unwrap_or_default();

        match prefix {
            BLOB_STRING_PREFIX => {
                if unlikely(line.first().is_some_and(|ch| *ch == CHUNKED_STRING_PREFIX)) {
                    if let StaticResp3::ChunkedString(chunks) = frame_buf {
                        loop {
                            let line = StaticResp3::decode_line_async(io_read, src).await?;

                            // if !line
                            //     .get(0)
                            //     .is_some_and(|ch| ch == CHUNKED_STRING_LENGTH_PREFIX)
                            // {
                            //     return Err(RutinError::InvalidFormat {
                            //         msg: "invalid chunk length prefix".into(),
                            //     });
                            // }

                            let len: usize = util::atoi(line.get(1..).unwrap_or_default())
                                .map_err(|_| RutinError::InvalidFormat {
                                    msg: "invalid chunks length".into(),
                                })?;

                            if len == 0 {
                                break;
                            }

                            StaticResp3::need_bytes_async(io_read, src, len + 2).await?;
                            let pos = src.position() as usize;
                            let chunk = leak_bytes_mut(&mut src.get_mut()[pos..pos + len]);
                            src.advance(len + 2);

                            chunks.push(chunk);
                        }
                    } else {
                        let mut chunks = Vec::new();

                        loop {
                            let line = StaticResp3::decode_line_async(io_read, src).await?;

                            // if !line
                            //     .get(0)
                            //     .is_some_and(|ch| ch == CHUNKED_STRING_LENGTH_PREFIX)
                            // {
                            //     return Err(RutinError::InvalidFormat {
                            //         msg: "invalid chunk length prefix".into(),
                            //     });
                            // }

                            let len: usize = util::atoi(line.get(1..).unwrap_or_default())
                                .map_err(|_| RutinError::InvalidFormat {
                                    msg: "invalid chunks length".into(),
                                })?;

                            if len == 0 {
                                break;
                            }

                            StaticResp3::need_bytes_async(io_read, src, len + 2).await?;
                            let pos = src.position() as usize;
                            let chunk = leak_bytes_mut(&mut src.get_mut()[pos..pos + len]);
                            src.advance(len + 2);

                            chunks.push(chunk);
                        }

                        *frame_buf = StaticResp3::ChunkedString(chunks);
                    }
                } else {
                    let len: usize = util::atoi(line).map_err(|_| RutinError::InvalidFormat {
                        msg: "invalid blob string length".into(),
                    })?;

                    StaticResp3::need_bytes_async(io_read, src, len + 2).await?;
                    let pos = src.position() as usize;
                    let blob = leak_bytes_mut(&mut src.get_mut()[pos..pos + len]);
                    src.advance(len + 2);

                    *frame_buf = StaticResp3::BlobString {
                        inner: blob,
                        attributes: None,
                    };
                }
            }
            ARRAY_PREFIX => {
                let len = util::atoi(line).map_err(|_| RutinError::InvalidFormat {
                    msg: "invalid blob string length".into(),
                })?;

                if let StaticResp3::Array { inner: frames, .. } = frame_buf {
                    for _ in 0..len {
                        let frame = Self::_decode_async(io_read, src).await?;
                        frames.push_back(frame);
                    }
                } else {
                    let mut frames = VecDeque::with_capacity(len);

                    for _ in 0..len {
                        let frame = Self::_decode_async(io_read, src).await?;
                        frames.push_back(frame);
                    }

                    *frame_buf = StaticResp3::Array {
                        inner: frames,
                        attributes: None,
                    };
                }
            }
            SIMPLE_STRING_PREFIX => {
                let string = str::from_utf8_mut(line).map_err(|e| RutinError::InvalidFormat {
                    msg: format!("invalid simple string: {}", e).into(),
                })?;

                *frame_buf = StaticResp3::SimpleString {
                    inner: leak_str_mut(string),
                    attributes: None,
                };
            }
            SIMPLE_ERROR_PREFIX => {
                let error = str::from_utf8_mut(line).map_err(|_| RutinError::InvalidFormat {
                    msg: "invalid simple error".into(),
                })?;

                *frame_buf = StaticResp3::SimpleError {
                    inner: leak_str_mut(error),
                    attributes: None,
                };
            }
            INTEGER_PREFIX => {
                let integer = util::atoi(line).map_err(|_| RutinError::InvalidFormat {
                    msg: "invalid integer".into(),
                })?;

                *frame_buf = StaticResp3::Integer {
                    inner: integer,
                    attributes: None,
                };
            }
            NULL_PREFIX => *frame_buf = StaticResp3::Null,
            BOOLEAN_PREFIX => {
                let b = match line.first() {
                    Some(b't') => true,
                    Some(b'f') => false,
                    _ => {
                        return Err(RutinError::InvalidFormat {
                            msg: "invalid boolean".into(),
                        });
                    }
                };

                *frame_buf = StaticResp3::Boolean {
                    inner: b,
                    attributes: None,
                };
            }
            DOUBLE_PREFIX => {
                let double = atof(line).map_err(|_| RutinError::InvalidFormat {
                    msg: "invalid double".into(),
                })?;

                *frame_buf = StaticResp3::Double {
                    inner: double,
                    attributes: None,
                };
            }
            BIG_NUMBER_PREFIX => {
                let n = BigInt::parse_bytes(line, 10).ok_or_else(|| RutinError::InvalidFormat {
                    msg: "invalid big number".into(),
                })?;

                *frame_buf = StaticResp3::BigNumber {
                    inner: n,
                    attributes: None,
                };
            }
            BLOB_ERROR_PREFIX => {
                let len: usize = util::atoi(line).map_err(|_| RutinError::InvalidFormat {
                    msg: "invalid blob error length".into(),
                })?;

                StaticResp3::need_bytes_async(io_read, src, len + 2).await?;
                let pos = src.position() as usize;
                let error = leak_bytes_mut(&mut src.get_mut()[pos..pos + len]);
                src.advance(len + 2);

                *frame_buf = StaticResp3::BlobError {
                    inner: error,
                    attributes: None,
                };
            }
            VERBATIM_STRING_PREFIX => {
                let len: usize = util::atoi(line).map_err(|_| RutinError::InvalidFormat {
                    msg: "invalid verbatim string length".into(),
                })?;

                StaticResp3::need_bytes_async(io_read, src, len + 2).await?;

                let format = src.get_ref()[0..3].try_into().unwrap();
                let pos = src.position() as usize;
                let data = leak_bytes_mut(&mut src.get_mut()[pos..pos + len]);
                src.advance(len + 2);

                *frame_buf = StaticResp3::VerbatimString {
                    format,
                    data,
                    attributes: None,
                };
            }
            MAP_PREFIX => {
                let len = util::atoi(line).map_err(|_| RutinError::InvalidFormat {
                    msg: "invalid map length".into(),
                })?;

                if let StaticResp3::Map { inner: map, .. } = frame_buf {
                    for _ in 0..len {
                        let k = Self::_decode_async(io_read, src).await?;
                        let v = Self::_decode_async(io_read, src).await?;
                        map.insert(k, v);
                    }
                } else {
                    let mut map = AHashMap::with_capacity(len);

                    for _ in 0..len {
                        let k = Self::_decode_async(io_read, src).await?;
                        let v = Self::_decode_async(io_read, src).await?;
                        map.insert(k, v);
                    }

                    *frame_buf = StaticResp3::Map {
                        inner: map,
                        attributes: None,
                    };
                }
            }
            SET_PREFIX => {
                let len = util::atoi(line).map_err(|_| RutinError::InvalidFormat {
                    msg: "invalid set length".into(),
                })?;

                if let StaticResp3::Set { inner: set, .. } = frame_buf {
                    for _ in 0..len {
                        let frame = Self::_decode_async(io_read, src).await?;
                        set.insert(frame);
                    }
                } else {
                    let mut set = AHashSet::with_capacity(len);

                    for _ in 0..len {
                        let frame = Self::_decode_async(io_read, src).await?;
                        set.insert(frame);
                    }

                    *frame_buf = StaticResp3::Set {
                        inner: set,
                        attributes: None,
                    };
                }
            }
            PUSH_PREFIX => {
                let len = util::atoi(line).map_err(|_| RutinError::InvalidFormat {
                    msg: "invalid push length".into(),
                })?;

                if let StaticResp3::Push { inner: frames, .. } = frame_buf {
                    for _ in 0..len {
                        let frame = Self::_decode_async(io_read, src).await?;
                        frames.push(frame);
                    }
                } else {
                    let mut frames = Vec::with_capacity(len);

                    for _ in 0..len {
                        let frame = Self::_decode_async(io_read, src).await?;
                        frames.push(frame);
                    }

                    *frame_buf = StaticResp3::Push {
                        inner: frames,
                        attributes: None,
                    };
                };
            }
            b'H' => {
                let mut vec = line
                    .splitn(5, |&ch| ch == b' ')
                    .map(|slice| {
                        let len = slice.len();
                        leak_bytes_mut(unsafe {
                            &mut *slice_from_raw_parts_mut(slice.as_ptr() as *mut u8, len)
                        })
                    })
                    .collect_vec();

                if vec.len() == 2 {
                    let protocol_version =
                        util::atoi(&vec[1]).map_err(|_| RutinError::InvalidFormat {
                            msg: "invalid protocol version".into(),
                        })?;

                    *frame_buf = StaticResp3::Hello {
                        version: protocol_version,
                        auth: None,
                    };
                } else if vec.len() == 5 {
                    let protocol_version =
                        util::atoi(&vec[1]).map_err(|_| RutinError::InvalidFormat {
                            msg: "invalid protocol version".into(),
                        })?;

                    let password = str::from_utf8_mut(unsafe {
                        vec.pop().unwrap().into_inner_mut_unchecked()
                    })
                    .map_err(|_| RutinError::InvalidFormat {
                        msg: "invalid password format".into(),
                    })?;
                    let username = str::from_utf8_mut(unsafe {
                        vec.pop().unwrap().into_inner_mut_unchecked()
                    })
                    .map_err(|_| RutinError::InvalidFormat {
                        msg: "invalid username format".into(),
                    })?;

                    *frame_buf = StaticResp3::Hello {
                        version: protocol_version,
                        auth: Some((leak_str_mut(username), leak_str_mut(password))),
                    };
                } else {
                    return Err(RutinError::InvalidFormat {
                        msg: "invalid hello".into(),
                    });
                }
            }
            prefix => {
                return Err(RutinError::InvalidFormat {
                    msg: format!("invalid prefix: {}", prefix).into(),
                });
            }
        };

        Ok(())
    }

    #[inline(always)]
    pub async fn need_bytes_async<R: AsyncRead + Unpin>(
        io_read: &mut R,
        src: &mut Cursor<BytesMut>,
        len: usize,
    ) -> RutinResult<()> {
        // println!(
        //     "remaining data: {:?}",
        //     String::from_utf8(src.get_ref()[src.position() as usize..].to_vec()).unwrap()
        // );
        while src.remaining() < len {
            if io_read.read_buf(src.get_mut()).await? == 0 {
                return Err(RutinError::ConnectionReset);
            }
        }

        Ok(())
    }

    #[inline(always)]
    pub async fn decode_line_async<R: AsyncRead + Unpin>(
        io_read: &mut R,
        src: &mut Cursor<BytesMut>,
    ) -> RutinResult<&'static mut [u8]> {
        loop {
            match Self::decode_line(src) {
                Ok(line) => return Ok(line),
                Err(RutinError::Incomplete) => {
                    if io_read.read_buf(src.get_mut()).await? == 0 {
                        return Err(RutinError::ConnectionReset);
                    }
                }
                Err(e) => return Err(e),
            }
        }
    }

    #[inline(always)]
    fn need_bytes(src: &Cursor<BytesMut>, len: usize) -> RutinResult<()> {
        if src.remaining() < len {
            return Err(RutinError::Incomplete);
        }

        Ok(())
    }

    #[inline(always)]
    fn decode_line(src: &mut Cursor<BytesMut>) -> RutinResult<&'static mut [u8]> {
        if let Some(i) = memchr::memchr(b'\n', src.chunk()) {
            if likely(i > 0 && src.chunk()[i - 1] == b'\r') {
                let pos = src.position() as usize;
                let line = unsafe {
                    transmute::<&mut [u8], &'static mut [u8]>(&mut src.get_mut()[pos..pos + i - 1])
                };
                // skip \r\n
                src.advance(i + 1);

                return Ok(line);
            }
        }

        Err(RutinError::Incomplete)
    }
}

#[derive(Debug, Clone)]
pub struct Resp3Encoder;

impl<B, S> Encoder<&Resp3<B, S>> for Resp3Encoder
where
    B: AsRef<[u8]> + PartialEq + std::fmt::Debug,
    S: AsRef<str> + PartialEq + std::fmt::Debug,
{
    type Error = RutinError;

    fn encode(&mut self, item: &Resp3<B, S>, dst: &mut BytesMut) -> Result<(), Self::Error> {
        item.encode_buf(dst);

        Ok(())
    }
}

#[derive(Debug, Clone, Default)]
pub struct Resp3Decoder {
    pos: u64,
}

impl Decoder for Resp3Decoder {
    type Error = RutinError;
    type Item = StaticResp3;

    // 无论是否成功解码，该函数会消耗src中的所有数据
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let mut cursor = Cursor::new(src.split());
        cursor.set_position(self.pos);

        let res = decode(&mut cursor);

        self.pos = cursor.position();
        src.unsplit(cursor.into_inner());

        res
    }
}

impl<B, S> Hash for Resp3<B, S>
where
    B: AsRef<[u8]> + PartialEq + std::fmt::Debug,
    S: AsRef<str> + PartialEq + std::fmt::Debug,
{
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        let attrs_hash = |attrs: &Attributes<B, S>, state: &mut H| {
            attrs.iter().for_each(|(k, v)| {
                k.hash(state);
                v.hash(state);
            });
        };
        match self {
            Resp3::SimpleString { inner, attributes } => {
                SIMPLE_STRING_PREFIX.hash(state);
                inner.as_ref().hash(state);
                if let Some(attr) = attributes.as_ref() {
                    attrs_hash(attr, state)
                }
            }
            Resp3::SimpleError { inner, attributes } => {
                SIMPLE_ERROR_PREFIX.hash(state);
                inner.as_ref().hash(state);
                if let Some(attr) = attributes.as_ref() {
                    attrs_hash(attr, state)
                }
            }
            Resp3::Integer { inner, attributes } => {
                INTEGER_PREFIX.hash(state);
                inner.hash(state);
                if let Some(attr) = attributes.as_ref() {
                    attrs_hash(attr, state)
                }
            }
            Resp3::BlobString { inner, attributes } => {
                BLOB_STRING_PREFIX.hash(state);
                inner.as_ref().hash(state);
                if let Some(attr) = attributes.as_ref() {
                    attrs_hash(attr, state)
                }
            }
            Resp3::Array { inner, attributes } => {
                ARRAY_PREFIX.hash(state);
                inner.hash(state);
                if let Some(attr) = attributes.as_ref() {
                    attrs_hash(attr, state)
                }
            }
            Resp3::Null => NULL_PREFIX.hash(state),
            Resp3::Boolean { inner, attributes } => {
                BOOLEAN_PREFIX.hash(state);
                inner.hash(state);
                if let Some(attr) = attributes.as_ref() {
                    attrs_hash(attr, state)
                }
            }
            Resp3::Double { inner, attributes } => {
                DOUBLE_PREFIX.hash(state);
                inner.to_bits().hash(state);
                if let Some(attr) = attributes.as_ref() {
                    attrs_hash(attr, state)
                }
            }
            Resp3::BigNumber { inner, attributes } => {
                BIG_NUMBER_PREFIX.hash(state);
                inner.hash(state);
                if let Some(attr) = attributes.as_ref() {
                    attrs_hash(attr, state)
                }
            }
            Resp3::BlobError { inner, attributes } => {
                BLOB_ERROR_PREFIX.hash(state);
                inner.as_ref().hash(state);
                if let Some(attr) = attributes.as_ref() {
                    attrs_hash(attr, state)
                }
            }
            Resp3::VerbatimString {
                format,
                data,
                attributes,
            } => {
                VERBATIM_STRING_PREFIX.hash(state);
                format.hash(state);
                data.as_ref().hash(state);
                if let Some(attr) = attributes.as_ref() {
                    attrs_hash(attr, state)
                }
            }
            Resp3::Map { inner, attributes } => {
                MAP_PREFIX.hash(state);
                inner.iter().for_each(|(k, v)| {
                    k.hash(state);
                    v.hash(state);
                });
                if let Some(attr) = attributes.as_ref() {
                    attrs_hash(attr, state)
                }
            }
            Resp3::Set { inner, attributes } => {
                SET_PREFIX.hash(state);
                inner.iter().for_each(|f| f.hash(state));
                if let Some(attr) = attributes.as_ref() {
                    attrs_hash(attr, state)
                }
            }
            Resp3::Push { inner, attributes } => {
                PUSH_PREFIX.hash(state);
                inner.hash(state);
                if let Some(attr) = attributes.as_ref() {
                    attrs_hash(attr, state)
                }
            }
            Resp3::ChunkedString(chunks) => {
                CHUNKED_STRING_PREFIX.hash(state);
                chunks.iter().for_each(|c| c.as_ref().hash(state));
            }
            Resp3::Hello { version, auth } => {
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
    B: AsRef<[u8]> + PartialEq + std::fmt::Debug,
    S: AsRef<str> + PartialEq + std::fmt::Debug,
{
}

impl<B, S> mlua::IntoLua<'_> for Resp3<B, S>
where
    B: AsRef<[u8]> + PartialEq + std::fmt::Debug,
    S: AsRef<str> + PartialEq + std::fmt::Debug,
{
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
            Resp3::BlobString { inner, .. } => lua.create_string(inner).map(LuaValue::String),
            // Array -> Lua Table(Array)
            Resp3::Array { inner, .. } => Vec::from(inner).into_lua(lua),
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
            Resp3::BlobError { inner, .. } => inner.as_ref().into_lua(lua),
            // Lua table with a single verbatim_string field containing a Lua table with two fields, string and format, representing the verbatim string and its format, respectively.
            // VerbatimString -> Lua Table { verbatim_string: Lua Table { string: Lua String, format: Lua String } }
            Resp3::VerbatimString {
                format: encoding,
                data,
                ..
            } => {
                let data = lua.create_string(data)?;
                let encoding = lua.create_string(encoding)?;

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

impl FromLua<'_> for StaticResp3 {
    fn from_lua(value: LuaValue<'_>, lua: &'_ Lua) -> LuaResult<Self> {
        match value {
            // Lua String -> Blob
            LuaValue::String(_) => Ok(Resp3::BlobString {
                inner: StaticBytes::from_lua(value, lua)?,
                attributes: None,
            }),
            // Lua String -> SimpleError
            LuaValue::Integer(n) => Ok(Resp3::Integer {
                inner: n as Int,
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
                        inner: leak_str(state.to_str()?),
                        attributes: None,
                    });
                }

                let err = table.raw_get("err")?;

                if let LuaValue::String(e) = err {
                    return Ok(Resp3::SimpleError {
                        inner: leak_str(e.to_str()?),
                        attributes: None,
                    });
                }

                let verbatim_string = table.raw_get("verbatim_string")?;

                if let LuaValue::Table(verbatim_string) = verbatim_string {
                    let format: mlua::String = verbatim_string.raw_get("format")?;
                    let format = format.as_bytes().try_into().map_err(|_| {
                        mlua::Error::FromLuaConversionError {
                            from: "table",
                            to: "RESP3::VerbatimString",
                            message: Some("invalid encoding format".to_string()),
                        }
                    })?;

                    let data = verbatim_string.raw_get("string")?;

                    return Ok(Resp3::VerbatimString {
                        format,
                        data,
                        attributes: None,
                    });
                }

                let map = table.raw_get("map")?;

                if let LuaValue::Table(map) = map {
                    let mut map_table = AHashMap::new();
                    for pair in map.pairs::<LuaValue, LuaValue>() {
                        let (k, v) = pair?;
                        let k = Resp3::from_lua(k, lua)?;
                        let v = Resp3::from_lua(v, lua)?;
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
                        let f = Resp3::from_lua(f, lua)?;
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

                let mut array = VecDeque::with_capacity(table.raw_len());
                for pair in table.pairs::<usize, Value>() {
                    let ele = pair?.1;
                    array.push_back(Resp3::from_lua(ele, lua)?);
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

impl FromLua<'_> for CheapResp3 {
    fn from_lua(value: LuaValue<'_>, _lua: &'_ Lua) -> LuaResult<Self> {
        match value {
            // Lua String -> Blob
            LuaValue::String(s) => Ok(Resp3::BlobString {
                inner: Bytes::copy_from_slice(s.as_bytes()),
                attributes: None,
            }),
            // Lua String -> SimpleError
            LuaValue::Integer(n) => Ok(Resp3::Integer {
                inner: n as Int,
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
                        inner: ByteString::from(state.to_str()?),
                        attributes: None,
                    });
                }

                let err = table.raw_get("err")?;

                if let LuaValue::String(e) = err {
                    return Ok(Resp3::SimpleError {
                        inner: ByteString::from(e.to_str()?),
                        attributes: None,
                    });
                }

                let verbatim_string = table.raw_get("verbatim_string")?;

                if let LuaValue::Table(verbatim_string) = verbatim_string {
                    let format: mlua::String = verbatim_string.raw_get("format")?;
                    let format = format.as_bytes().try_into().map_err(|_| {
                        mlua::Error::FromLuaConversionError {
                            from: "table",
                            to: "RESP3::VerbatimString",
                            message: Some("invalid encoding format".to_string()),
                        }
                    })?;

                    let data: mlua::String = verbatim_string.raw_get("string")?;

                    return Ok(Resp3::VerbatimString {
                        format,
                        data: Bytes::copy_from_slice(data.as_bytes()),
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

                let mut array = VecDeque::with_capacity(table.raw_len());
                for pair in table.pairs::<usize, Value>() {
                    let ele = pair?.1;
                    array.push_back(Resp3::from_lua(ele, _lua)?);
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
    B: AsRef<[u8]> + PartialEq + std::fmt::Debug,
    S: AsRef<str> + PartialEq + std::fmt::Debug,
{
    buf.put_u8(b'|');
    buf.put_slice(itoa::Buffer::new().format(attr.len()).as_bytes());
    buf.put_slice(CRLF);
    for (k, v) in attr {
        k.encode_buf(buf);
        v.encode_buf(buf);
    }
}

// src中必须包含一个完整的Resp3，否则引发io::ErrorKind::UnexpectedEof错误
pub fn decode(src: &mut Cursor<BytesMut>) -> RutinResult<Option<StaticResp3>> {
    if !src.has_remaining() {
        return Ok(None);
    }

    let pos = src.position();

    #[inline]
    fn _decode(
        src: &mut Cursor<BytesMut>,
    ) -> Result<<Resp3Decoder as Decoder>::Item, <Resp3Decoder as Decoder>::Error> {
        if !src.has_remaining() {
            return Err(RutinError::Incomplete);
        }

        let line = StaticResp3::decode_line(src)?;

        let prefix = *line.first().ok_or_else(|| RutinError::InvalidFormat {
            msg: "missing prefix".into(),
        })?;
        let line = line.get_mut(1..).unwrap_or_default();

        let res = match prefix {
            BLOB_STRING_PREFIX => {
                if unlikely(line.first().is_some_and(|ch| *ch == CHUNKED_STRING_PREFIX)) {
                    let mut chunks = Vec::new();
                    loop {
                        let line = StaticResp3::decode_line(src)?;

                        let len: usize =
                            util::atoi(line.get(1..).unwrap_or_default()).map_err(|_| {
                                RutinError::InvalidFormat {
                                    msg: "invalid chunks length".into(),
                                }
                            })?;

                        if len == 0 {
                            break;
                        }

                        StaticResp3::need_bytes(src, len + 2)?;
                        let pos = src.position() as usize;
                        let chunk = leak_bytes_mut(&mut src.get_mut()[pos..pos + len]);
                        src.advance(len + 2);

                        chunks.push(chunk);
                    }

                    StaticResp3::ChunkedString(chunks)
                } else {
                    let len: usize = util::atoi(line).map_err(|_| RutinError::InvalidFormat {
                        msg: "invalid blob string length".into(),
                    })?;

                    StaticResp3::need_bytes(src, len + 2)?;
                    let pos = src.position() as usize;
                    let blob = leak_bytes_mut(&mut src.get_mut()[pos..pos + len]);
                    src.advance(len + 2);

                    StaticResp3::BlobString {
                        inner: blob,
                        attributes: None,
                    }
                }
            }
            ARRAY_PREFIX => {
                let len: usize = util::atoi(line).map_err(|_| RutinError::InvalidFormat {
                    msg: "invalid blob string length".into(),
                })?;

                let mut frames = VecDeque::with_capacity(len);
                for _ in 0..len {
                    let frame = _decode(src)?;
                    frames.push_back(frame);
                }

                StaticResp3::Array {
                    inner: frames,
                    attributes: None,
                }
            }
            SIMPLE_STRING_PREFIX => {
                let string = str::from_utf8_mut(line).map_err(|e| RutinError::InvalidFormat {
                    msg: format!("invalid simple string: {}", e).into(),
                })?;

                StaticResp3::SimpleString {
                    inner: leak_str_mut(string),
                    attributes: None,
                }
            }
            SIMPLE_ERROR_PREFIX => {
                let error = str::from_utf8_mut(line).map_err(|_| RutinError::InvalidFormat {
                    msg: "invalid simple error".into(),
                })?;

                StaticResp3::SimpleError {
                    inner: leak_str_mut(error),
                    attributes: None,
                }
            }
            INTEGER_PREFIX => {
                let integer = util::atoi(line).map_err(|_| RutinError::InvalidFormat {
                    msg: "invalid integer".into(),
                })?;

                StaticResp3::Integer {
                    inner: integer,
                    attributes: None,
                }
            }
            NULL_PREFIX => StaticResp3::Null,
            BOOLEAN_PREFIX => {
                let b = match line.first() {
                    Some(b't') => true,
                    Some(b'f') => false,
                    _ => {
                        return Err(RutinError::InvalidFormat {
                            msg: "invalid boolean".into(),
                        });
                    }
                };

                StaticResp3::Boolean {
                    inner: b,
                    attributes: None,
                }
            }
            DOUBLE_PREFIX => {
                let double = atof(line).map_err(|_| RutinError::InvalidFormat {
                    msg: "invalid double".into(),
                })?;

                StaticResp3::Double {
                    inner: double,
                    attributes: None,
                }
            }
            BIG_NUMBER_PREFIX => {
                let n = BigInt::parse_bytes(line, 10).ok_or_else(|| RutinError::InvalidFormat {
                    msg: "invalid big number".into(),
                })?;

                StaticResp3::BigNumber {
                    inner: n,
                    attributes: None,
                }
            }
            BLOB_ERROR_PREFIX => {
                let len: usize = util::atoi(line).map_err(|_| RutinError::InvalidFormat {
                    msg: "invalid blob error length".into(),
                })?;

                StaticResp3::need_bytes(src, len + 2)?;
                let pos = src.position() as usize;
                let error = leak_bytes_mut(&mut src.get_mut()[pos..pos + len]);
                src.advance(len + 2);

                StaticResp3::BlobError {
                    inner: error,
                    attributes: None,
                }
            }
            VERBATIM_STRING_PREFIX => {
                let len: usize = util::atoi(line).map_err(|_| RutinError::InvalidFormat {
                    msg: "invalid verbatim string length".into(),
                })?;

                StaticResp3::need_bytes(src, len + 2)?;

                let pos = src.position() as usize;
                let format = src.chunk()[0..3].try_into().unwrap();
                let data = leak_bytes_mut(&mut src.get_mut()[pos + 4..pos + len]);
                src.advance(len + 2);

                StaticResp3::VerbatimString {
                    format,
                    data,
                    attributes: None,
                }
            }
            MAP_PREFIX => {
                let len = util::atoi(line).map_err(|_| RutinError::InvalidFormat {
                    msg: "invalid map length".into(),
                })?;

                let mut map = AHashMap::with_capacity(len);
                for _ in 0..len {
                    let k = _decode(src)?;
                    let v = _decode(src)?;
                    map.insert(k, v);
                }

                // map的key由客户端保证唯一
                StaticResp3::Map {
                    inner: map,
                    attributes: None,
                }
            }
            SET_PREFIX => {
                let len = util::atoi(line).map_err(|_| RutinError::InvalidFormat {
                    msg: "invalid set length".into(),
                })?;

                let mut set = AHashSet::with_capacity(len);
                for _ in 0..len {
                    let frame = _decode(src)?;
                    set.insert(frame);
                }

                // set的元素由客户端保证唯一
                StaticResp3::Set {
                    inner: set,
                    attributes: None,
                }
            }
            PUSH_PREFIX => {
                let len = util::atoi(line).map_err(|_| RutinError::InvalidFormat {
                    msg: "invalid push length".into(),
                })?;

                let mut frames = Vec::with_capacity(len);
                for _ in 0..len {
                    let frame = _decode(src)?;
                    frames.push(frame);
                }

                StaticResp3::Push {
                    inner: frames,
                    attributes: None,
                }
            }
            b'H' => {
                let mut vec = line
                    .splitn(5, |&ch| ch == b' ')
                    .map(|slice| {
                        let len = slice.len();
                        leak_bytes_mut(unsafe {
                            &mut *slice_from_raw_parts_mut(slice.as_ptr() as *mut u8, len)
                        })
                    })
                    .collect_vec();

                if vec.len() == 2 {
                    let protocol_version =
                        util::atoi(&vec[1]).map_err(|_| RutinError::InvalidFormat {
                            msg: "invalid protocol version".into(),
                        })?;

                    StaticResp3::Hello {
                        version: protocol_version,
                        auth: None,
                    }
                } else if vec.len() == 5 {
                    let protocol_version =
                        util::atoi(&vec[1]).map_err(|_| RutinError::InvalidFormat {
                            msg: "invalid protocol version".into(),
                        })?;

                    let password = str::from_utf8_mut(unsafe {
                        vec.pop().unwrap().into_inner_mut_unchecked()
                    })
                    .map_err(|_| RutinError::InvalidFormat {
                        msg: "invalid password format".into(),
                    })?;

                    let username = str::from_utf8_mut(unsafe {
                        vec.pop().unwrap().into_inner_mut_unchecked()
                    })
                    .map_err(|_| RutinError::InvalidFormat {
                        msg: "invalid username format".into(),
                    })?;

                    StaticResp3::Hello {
                        version: protocol_version,
                        auth: Some((leak_str_mut(username), leak_str_mut(password))),
                    }
                } else {
                    return Err(RutinError::InvalidFormat {
                        msg: "invalid hello".into(),
                    });
                }
            }
            prefix => {
                return Err(RutinError::InvalidFormat {
                    msg: format!("invalid prefix: {}", prefix).into(),
                });
            }
        };

        Ok(res)
    }

    match _decode(src) {
        // frame不完整，将游标重置到解析开始的位置
        Err(RutinError::Incomplete) => {
            src.set_position(pos);
            Ok(None)
        }
        res => Ok(Some(res?)),
    }
}

#[inline]
pub fn leak_bytes(t: &[u8]) -> StaticBytes {
    unsafe { std::mem::transmute::<&[u8], &'static [u8]>(t) }.into()
}

#[inline]
pub fn leak_bytes_mut(t: &mut [u8]) -> StaticBytes {
    unsafe { std::mem::transmute::<&mut [u8], &'static mut [u8]>(t) }.into()
}

#[inline]
pub fn leak_str(t: &str) -> StaticStr {
    unsafe { std::mem::transmute::<&str, &'static str>(t) }.into()
}

#[inline]
pub fn leak_str_mut(t: &mut str) -> StaticStr {
    unsafe { std::mem::transmute::<&mut str, &'static mut str>(t) }.into()
}

#[inline]
pub fn reset_frame_buf(frame_buf: &mut StaticResp3) {
    match frame_buf {
        StaticResp3::Array { ref mut inner, .. } => inner.clear(),
        StaticResp3::Map { ref mut inner, .. } => inner.clear(),
        StaticResp3::Set { ref mut inner, .. } => inner.clear(),
        StaticResp3::Push { ref mut inner, .. } => inner.clear(),
        StaticResp3::ChunkedString(ref mut inner) => inner.clear(),
        _ => {}
    }
}

#[cfg(test)]
mod frame_tests {
    use crate::util::test_init;

    use super::*;

    // #[test]
    // fn decode_resume() {
    //     test_init();
    //
    //     let mut decoder = Resp3Decoder::default();
    //
    //     let mut src = BytesMut::from("*2\r\n");
    //     let src_clone = src.clone();
    //
    //     assert!(decoder.decode(&mut src).unwrap().is_none());
    //
    //     assert_eq!(decoder.buf.get_ref(), &src_clone);
    // }

    #[test]
    fn encode_decode_test() {
        test_init();

        let cases = vec![
            (
                StaticResp3::SimpleString {
                    inner: leak_str_mut("OK".to_string().leak()),
                    attributes: None,
                },
                b"+OK\r\n".to_vec(),
            ),
            (
                StaticResp3::SimpleError {
                    inner: leak_str_mut("ERR".to_string().leak()),
                    attributes: None,
                },
                b"-ERR\r\n".to_vec(),
            ),
            (
                StaticResp3::Integer {
                    inner: 42,
                    attributes: None,
                },
                b":42\r\n".to_vec(),
            ),
            (
                StaticResp3::BlobString {
                    inner: b"blob data".as_ref().into(),
                    attributes: None,
                },
                b"$9\r\nblob data\r\n".to_vec(),
            ),
            (
                StaticResp3::Array {
                    inner: vec![
                        StaticResp3::Integer {
                            inner: 1,
                            attributes: None,
                        },
                        StaticResp3::Integer {
                            inner: 2,
                            attributes: None,
                        },
                        StaticResp3::Integer {
                            inner: 3,
                            attributes: None,
                        },
                    ]
                    .into(),
                    attributes: None,
                },
                b"*3\r\n:1\r\n:2\r\n:3\r\n".to_vec(),
            ),
            (StaticResp3::Null, b"_\r\n".to_vec()),
            (
                StaticResp3::Boolean {
                    inner: true,
                    attributes: None,
                },
                b"#t\r\n".to_vec(),
            ),
            (
                StaticResp3::Double {
                    inner: 3.15,
                    attributes: None,
                },
                b",3.15\r\n".to_vec(),
            ),
            (
                StaticResp3::BigNumber {
                    inner: BigInt::from(1234567890),
                    attributes: None,
                },
                b"(1234567890\r\n".to_vec(),
            ),
            (
                StaticResp3::BlobError {
                    inner: b"blob error".as_ref().into(),
                    attributes: None,
                },
                b"!10\r\nblob error\r\n".to_vec(),
            ),
            (
                StaticResp3::VerbatimString {
                    format: *b"txt",
                    data: b"Some string".as_ref().into(),
                    attributes: None,
                },
                b"=15\r\ntxt:Some string\r\n".to_vec(),
            ),
            (
                StaticResp3::Map {
                    inner: {
                        let mut map = AHashMap::new();
                        map.insert(
                            StaticResp3::SimpleString {
                                inner: leak_str_mut("key".to_string().leak()),
                                attributes: None,
                            },
                            StaticResp3::SimpleString {
                                inner: leak_str_mut("value".to_string().leak()),
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
                StaticResp3::Set {
                    inner: {
                        let mut set = AHashSet::new();
                        set.insert(StaticResp3::SimpleString {
                            inner: leak_str_mut("element".to_string().leak()),
                            attributes: None,
                        });
                        set
                    },
                    attributes: None,
                },
                b"~1\r\n+element\r\n".to_vec(),
            ),
            (
                StaticResp3::Push {
                    inner: vec![StaticResp3::SimpleString {
                        inner: leak_str_mut("push".to_string().leak()),
                        attributes: None,
                    }],
                    attributes: None,
                },
                b">1\r\n+push\r\n".to_vec(),
            ),
            (
                StaticResp3::ChunkedString(vec![
                    b"chunk1".as_ref().into(),
                    b"chunk2".as_ref().into(),
                    b"chunk3".as_ref().into(),
                ]),
                b"$?\r\n;6\r\nchunk1\r\n;6\r\nchunk2\r\n;6\r\nchunk3\r\n;0\r\n".to_vec(),
            ),
            (
                StaticResp3::Hello {
                    version: 1,
                    auth: Some((
                        leak_str_mut("user".to_string().leak()),
                        leak_str_mut("password".to_string().leak()),
                    )),
                },
                b"HELLO 1 AUTH user password\r\n".to_vec(),
            ),
        ];

        for (case, expected_encoding) in cases {
            let mut encoder = Resp3Encoder;
            let mut decoder = Resp3Decoder::default();
            let mut buf = BytesMut::new();

            // Encode the case
            encoder.encode(&case, &mut buf).unwrap();

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
