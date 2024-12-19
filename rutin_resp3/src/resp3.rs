#[cfg(feature = "ahash")]
use ahash::RandomState;
#[cfg(not(feature = "ahash"))]
use std::collections::hash_map::RandomState;

#[cfg(feature = "mlua")]
use mlua::{prelude::*, Value};

use num_bigint::BigInt;
use std::{
    collections::{HashMap, HashSet},
    fmt::{Debug, Display},
    hash::Hash,
    iter::Iterator,
};

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

pub type Attributes<B, S> = HashMap<Resp3<B, S>, Resp3<B, S>, RandomState>;

#[derive(Clone)]
pub enum Resp3<B, S> {
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
        inner: i128,
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
        inner: Vec<Self>,
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
        inner: HashMap<Self, Self>,
        attributes: Option<Attributes<B, S>>,
    },

    // ~<number-of-elements>\r\n<element-1>...<element-n>
    Set {
        inner: HashSet<Self>,
        attributes: Option<Attributes<B, S>>,
    },

    // ><number-of-elements>\r\n<element-1>...<element-n>
    // 通常用于服务端主动向客户端推送消息
    Push {
        inner: Vec<Self>,
        attributes: Option<Attributes<B, S>>,
    },

    // $?\r\n;<length>\r\n<data>\r\n;<length>\r\n<data>\r\n...;0\r\n
    ChunkedString(Vec<B>),

    // HELLO <protocol-version> [AUTH <username> <password>]
    Hello {
        version: i128,
        auth: Option<(B, B)>,
    },
}

impl<B, S> Resp3<B, S> {
    pub fn new_simple_string(string: impl Into<S>) -> Self {
        Self::SimpleString {
            inner: string.into(),
            attributes: None,
        }
    }

    pub fn new_simple_error(error: impl Into<S>) -> Self {
        Self::SimpleError {
            inner: error.into(),
            attributes: None,
        }
    }

    pub fn new_integer(integer: impl Into<i128>) -> Self {
        Self::Integer {
            inner: integer.into(),
            attributes: None,
        }
    }

    pub fn new_blob_string(blob: impl Into<B>) -> Self {
        Self::BlobString {
            inner: blob.into(),
            attributes: None,
        }
    }

    pub fn new_array(array: impl Into<Vec<Self>>) -> Self {
        Self::Array {
            inner: array.into(),
            attributes: None,
        }
    }

    pub fn new_null() -> Self {
        Self::Null
    }

    pub fn new_boolean(bool: bool) -> Self {
        Self::Boolean {
            inner: bool,
            attributes: None,
        }
    }

    pub fn new_double(double: f64) -> Self {
        Self::Double {
            inner: double,
            attributes: None,
        }
    }

    pub fn new_big_number(big_num: impl Into<BigInt>) -> Self {
        Self::BigNumber {
            inner: big_num.into(),
            attributes: None,
        }
    }

    pub fn new_blob_error(error: impl Into<B>) -> Self {
        Self::BlobError {
            inner: error.into(),
            attributes: None,
        }
    }

    pub fn new_verbatim_string(format: [u8; 3], data: impl Into<B>) -> Self {
        Self::VerbatimString {
            format,
            data: data.into(),
            attributes: None,
        }
    }

    pub fn new_map(map: impl Into<HashMap<Self, Self>>) -> Self {
        Self::Map {
            inner: map.into(),
            attributes: None,
        }
    }

    pub fn new_set(set: impl Into<HashSet<Self>>) -> Self {
        Self::Set {
            inner: set.into(),
            attributes: None,
        }
    }

    pub fn new_push(push: impl Into<Vec<Self>>) -> Self {
        Self::Push {
            inner: push.into(),
            attributes: None,
        }
    }

    pub fn new_chunked_string(chunks: impl Into<Vec<B>>) -> Self {
        Self::ChunkedString(chunks.into())
    }

    pub fn is_simple_string(&self) -> bool {
        matches!(self, Self::SimpleString { .. })
    }

    pub fn is_simple_error(&self) -> bool {
        matches!(self, Self::SimpleError { .. })
    }

    pub fn is_integer(&self) -> bool {
        matches!(self, Self::Integer { .. })
    }

    pub fn is_blob_string(&self) -> bool {
        matches!(self, Self::BlobString { .. })
    }

    pub fn is_array(&self) -> bool {
        matches!(self, Self::Array { .. })
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    pub fn is_boolean(&self) -> bool {
        matches!(self, Self::Boolean { .. })
    }

    pub fn is_double(&self) -> bool {
        matches!(self, Self::Double { .. })
    }

    pub fn is_big_number(&self) -> bool {
        matches!(self, Self::BigNumber { .. })
    }

    pub fn is_blob_error(&self) -> bool {
        matches!(self, Self::BlobError { .. })
    }

    pub fn is_verbatim_string(&self) -> bool {
        matches!(self, Self::VerbatimString { .. })
    }

    pub fn is_map(&self) -> bool {
        matches!(self, Self::Map { .. })
    }

    pub fn is_set(&self) -> bool {
        matches!(self, Self::Set { .. })
    }

    pub fn is_push(&self) -> bool {
        matches!(self, Self::Push { .. })
    }

    pub fn as_simple_string(&self) -> Option<&S> {
        match self {
            Self::SimpleString { inner, .. } => Some(inner),
            _ => None,
        }
    }

    pub fn as_simple_error(&self) -> Option<&S> {
        match self {
            Self::SimpleError { inner, .. } => Some(inner),
            _ => None,
        }
    }

    pub fn as_integer(&self) -> Option<i128> {
        match self {
            Self::Integer { inner, .. } => Some(*inner),
            _ => None,
        }
    }

    pub fn as_blob_string(&self) -> Option<&B> {
        match self {
            Self::BlobString { inner, .. } => Some(inner),
            _ => None,
        }
    }

    pub fn as_array(&self) -> Option<&Vec<Self>> {
        match self {
            Self::Array { inner, .. } => Some(inner),
            _ => None,
        }
    }

    pub fn as_null(&self) -> Option<()> {
        match self {
            Self::Null => Some(()),
            _ => None,
        }
    }

    pub fn as_boolean(&self) -> Option<bool> {
        match self {
            Self::Boolean { inner, .. } => Some(*inner),
            _ => None,
        }
    }

    pub fn as_double(&self) -> Option<f64> {
        match self {
            Self::Double { inner, .. } => Some(*inner),
            _ => None,
        }
    }

    pub fn as_big_number(&self) -> Option<&BigInt> {
        match self {
            Self::BigNumber { inner, .. } => Some(inner),
            _ => None,
        }
    }

    pub fn as_blob_error(&self) -> Option<&B> {
        match self {
            Self::BlobError { inner, .. } => Some(inner),
            _ => None,
        }
    }

    pub fn as_verbatim_string(&self) -> Option<(&[u8; 3], &B)> {
        match self {
            Self::VerbatimString { format, data, .. } => Some((format, data)),
            _ => None,
        }
    }

    #[allow(clippy::type_complexity)]
    pub fn as_map(&self) -> Option<&HashMap<Self, Self>> {
        match self {
            Self::Map { inner, .. } => Some(inner),
            _ => None,
        }
    }

    pub fn as_set(&self) -> Option<&HashSet<Self>> {
        match self {
            Self::Set { inner, .. } => Some(inner),
            _ => None,
        }
    }

    pub fn as_push(&self) -> Option<&Vec<Self>> {
        match self {
            Self::Push { inner, .. } => Some(inner),
            _ => None,
        }
    }

    pub fn as_simple_string_mut(&mut self) -> Option<&mut S> {
        match self {
            Self::SimpleString { inner, .. } => Some(inner),
            _ => None,
        }
    }

    pub fn as_simple_error_mut(&mut self) -> Option<&mut S> {
        match self {
            Self::SimpleError { inner, .. } => Some(inner),
            _ => None,
        }
    }

    pub fn as_integer_mut(&mut self) -> Option<&mut i128> {
        match self {
            Self::Integer { inner, .. } => Some(inner),
            _ => None,
        }
    }

    pub fn as_blob_string_mut(&mut self) -> Option<&mut B> {
        match self {
            Self::BlobString { inner, .. } => Some(inner),
            _ => None,
        }
    }

    pub fn as_array_mut(&mut self) -> Option<&mut Vec<Self>> {
        match self {
            Self::Array { inner, .. } => Some(inner),
            _ => None,
        }
    }

    pub fn as_boolean_mut(&mut self) -> Option<&mut bool> {
        match self {
            Self::Boolean { inner, .. } => Some(inner),
            _ => None,
        }
    }

    pub fn as_double_mut(&mut self) -> Option<&mut f64> {
        match self {
            Self::Double { inner, .. } => Some(inner),
            _ => None,
        }
    }

    pub fn as_big_number_mut(&mut self) -> Option<&mut BigInt> {
        match self {
            Self::BigNumber { inner, .. } => Some(inner),
            _ => None,
        }
    }

    pub fn as_blob_error_mut(&mut self) -> Option<&mut B> {
        match self {
            Self::BlobError { inner, .. } => Some(inner),
            _ => None,
        }
    }

    pub fn as_verbatim_string_mut(&mut self) -> Option<(&mut [u8; 3], &mut B)> {
        match self {
            Self::VerbatimString { format, data, .. } => Some((format, data)),
            _ => None,
        }
    }

    #[allow(clippy::type_complexity)]
    pub fn as_map_mut(&mut self) -> Option<&mut HashMap<Self, Self>> {
        match self {
            Self::Map { inner, .. } => Some(inner),
            _ => None,
        }
    }

    pub fn as_set_mut(&mut self) -> Option<&mut HashSet<Self>> {
        match self {
            Self::Set { inner, .. } => Some(inner),
            _ => None,
        }
    }

    pub fn as_push_mut(&mut self) -> Option<&mut Vec<Self>> {
        match self {
            Self::Push { inner, .. } => Some(inner),
            _ => None,
        }
    }

    pub fn as_simple_string_unchecked(&self) -> &S {
        match self {
            Self::SimpleString { inner, .. } => inner,
            _ => panic!("not a simple string"),
        }
    }

    pub fn as_simple_error_unchecked(&self) -> &S {
        match self {
            Self::SimpleError { inner, .. } => inner,
            _ => panic!("not a simple error"),
        }
    }

    pub fn as_integer_unchecked(&self) -> i128 {
        match self {
            Self::Integer { inner, .. } => *inner,
            _ => panic!("not an integer"),
        }
    }

    pub fn as_blob_string_unchecked(&self) -> &B {
        match self {
            Self::BlobString { inner, .. } => inner,
            _ => panic!("not a blob string"),
        }
    }

    pub fn as_array_unchecked(&self) -> &Vec<Self> {
        match self {
            Self::Array { inner, .. } => inner,
            _ => panic!("not an array"),
        }
    }

    pub fn as_null_unchecked(&self) {
        match self {
            Self::Null => {}
            _ => panic!("not a null"),
        }
    }

    pub fn as_boolean_unchecked(&self) -> bool {
        match self {
            Self::Boolean { inner, .. } => *inner,
            _ => panic!("not a boolean"),
        }
    }

    pub fn as_double_unchecked(&self) -> f64 {
        match self {
            Self::Double { inner, .. } => *inner,
            _ => panic!("not a double"),
        }
    }

    pub fn as_big_number_unchecked(&self) -> &BigInt {
        match self {
            Self::BigNumber { inner, .. } => inner,
            _ => panic!("not a big number"),
        }
    }

    pub fn as_bolb_error_unchecked(&self) -> &B {
        match self {
            Self::BlobError { inner, .. } => inner,
            _ => panic!("not a blob error"),
        }
    }

    pub fn as_verbatim_string_unchecked(&self) -> (&[u8; 3], &B) {
        match self {
            Self::VerbatimString { format, data, .. } => (format, data),
            _ => panic!("not a verbatim string"),
        }
    }

    pub fn as_map_unchecked(&self) -> &HashMap<Self, Self> {
        match self {
            Self::Map { inner, .. } => inner,
            _ => panic!("not a map"),
        }
    }

    pub fn as_set_unchecked(&self) -> &HashSet<Self> {
        match self {
            Self::Set { inner, .. } => inner,
            _ => panic!("not a set"),
        }
    }

    pub fn as_push_unchecked(&self) -> &Vec<Self> {
        match self {
            Self::Push { inner, .. } => inner,
            _ => panic!("not a push"),
        }
    }

    pub fn as_simple_string_unchecked_mut(&mut self) -> &mut S {
        match self {
            Self::SimpleString { inner, .. } => inner,
            _ => panic!("not a simple string"),
        }
    }

    pub fn as_simple_error_unchecked_mut(&mut self) -> &mut S {
        match self {
            Self::SimpleError { inner, .. } => inner,
            _ => panic!("not a simple error"),
        }
    }

    pub fn as_integer_unchecked_mut(&mut self) -> i128 {
        match self {
            Self::Integer { inner, .. } => *inner,
            _ => panic!("not an integer"),
        }
    }

    pub fn as_blob_string_unchecked_mut(&mut self) -> &mut B {
        match self {
            Self::BlobString { inner, .. } => inner,
            _ => panic!("not a blob string"),
        }
    }

    pub fn as_array_unchecked_mut(&mut self) -> &mut Vec<Self> {
        match self {
            Self::Array { inner, .. } => inner,
            _ => panic!("not an array"),
        }
    }

    pub fn as_null_unchecked_mut(&mut self) {
        match self {
            Self::Null => {}
            _ => panic!("not a null"),
        }
    }

    pub fn as_boolean_unchecked_mut(&mut self) -> bool {
        match self {
            Self::Boolean { inner, .. } => *inner,
            _ => panic!("not a boolean"),
        }
    }

    pub fn as_double_unchecked_mut(&mut self) -> f64 {
        match self {
            Self::Double { inner, .. } => *inner,
            _ => panic!("not a double"),
        }
    }

    pub fn as_big_number_unchecked_mut(&mut self) -> &mut BigInt {
        match self {
            Self::BigNumber { inner, .. } => inner,
            _ => panic!("not a big number"),
        }
    }

    pub fn as_bolb_error_unchecked_mut(&mut self) -> &mut B {
        match self {
            Self::BlobError { inner, .. } => inner,
            _ => panic!("not a blob error"),
        }
    }

    pub fn as_verbatim_string_unchecked_mut(&mut self) -> (&mut [u8; 3], &mut B) {
        match self {
            Self::VerbatimString { format, data, .. } => (format, data),
            _ => panic!("not a verbatim string"),
        }
    }

    pub fn as_map_unchecked_mut(&mut self) -> &mut HashMap<Self, Self> {
        match self {
            Self::Map { inner, .. } => inner,
            _ => panic!("not a map"),
        }
    }

    pub fn as_set_unchecked_mut(&mut self) -> &mut HashSet<Self> {
        match self {
            Self::Set { inner, .. } => inner,
            _ => panic!("not a set"),
        }
    }

    pub fn as_push_unchecked_mut(&mut self) -> &mut Vec<Self> {
        match self {
            Self::Push { inner, .. } => inner,
            _ => panic!("not a push"),
        }
    }

    pub fn into_simple_string(self) -> Option<S> {
        if let Self::SimpleString { inner, .. } = self {
            Some(inner)
        } else {
            None
        }
    }

    pub fn into_simple_error(self) -> Option<S> {
        if let Self::SimpleError { inner, .. } = self {
            Some(inner)
        } else {
            None
        }
    }

    pub fn into_integer(self) -> Option<i128> {
        if let Self::Integer { inner, .. } = self {
            Some(inner)
        } else {
            None
        }
    }

    pub fn into_blob_string(self) -> Option<B> {
        if let Self::BlobString { inner, .. } = self {
            Some(inner)
        } else {
            None
        }
    }

    pub fn into_blob_error(self) -> Option<B> {
        if let Self::BlobError { inner, .. } = self {
            Some(inner)
        } else {
            None
        }
    }

    pub fn into_array(self) -> Option<Vec<Self>> {
        if let Self::Array { inner, .. } = self {
            Some(inner)
        } else {
            None
        }
    }

    pub fn into_null(self) -> Option<()> {
        if let Self::Null = self {
            Some(())
        } else {
            None
        }
    }

    pub fn into_boolean(self) -> Option<bool> {
        if let Self::Boolean { inner, .. } = self {
            Some(inner)
        } else {
            None
        }
    }

    pub fn into_double(self) -> Option<f64> {
        if let Self::Double { inner, .. } = self {
            Some(inner)
        } else {
            None
        }
    }

    pub fn into_big_number(self) -> Option<BigInt> {
        if let Self::BigNumber { inner, .. } = self {
            Some(inner)
        } else {
            None
        }
    }

    pub fn into_verbatim_string(self) -> Option<([u8; 3], B)> {
        if let Self::VerbatimString { format, data, .. } = self {
            Some((format, data))
        } else {
            None
        }
    }

    pub fn into_map(self) -> Option<HashMap<Self, Self>> {
        if let Self::Map { inner, .. } = self {
            Some(inner)
        } else {
            None
        }
    }

    pub fn into_set(self) -> Option<HashSet<Self>> {
        if let Self::Set { inner, .. } = self {
            Some(inner)
        } else {
            None
        }
    }

    pub fn into_push(self) -> Option<Vec<Self>> {
        if let Self::Push { inner, .. } = self {
            Some(inner)
        } else {
            None
        }
    }

    pub fn into_chunked_string(self) -> Option<Vec<B>> {
        if let Self::ChunkedString(inner) = self {
            Some(inner)
        } else {
            None
        }
    }

    pub fn into_hello(self) -> Option<(i128, Option<(B, B)>)> {
        if let Self::Hello { version, auth } = self {
            Some((version, auth))
        } else {
            None
        }
    }

    pub fn into_simple_string_unchecked(self) -> S {
        if let Self::SimpleString { inner, .. } = self {
            inner
        } else {
            panic!("not a simple string")
        }
    }

    pub fn into_simple_error_unchecked(self) -> S {
        if let Self::SimpleError { inner, .. } = self {
            inner
        } else {
            panic!("not a simple error")
        }
    }

    pub fn into_integer_unchecked(self) -> i128 {
        if let Self::Integer { inner, .. } = self {
            inner
        } else {
            panic!("not an integer")
        }
    }

    pub fn into_blob_string_unchecked(self) -> B {
        if let Self::BlobString { inner, .. } = self {
            inner
        } else {
            panic!("not a blob string")
        }
    }

    pub fn into_blob_error_unchecked(self) -> B {
        if let Self::BlobError { inner, .. } = self {
            inner
        } else {
            panic!("not a blob error")
        }
    }

    pub fn into_array_unchecked(self) -> Vec<Self> {
        if let Self::Array { inner, .. } = self {
            inner
        } else {
            panic!("not an array")
        }
    }

    pub fn into_null_unchecked(self) {
        if let Self::Null = self {
        } else {
            panic!("not a null")
        }
    }

    pub fn into_boolean_unchecked(self) -> bool {
        if let Self::Boolean { inner, .. } = self {
            inner
        } else {
            panic!("not a boolean")
        }
    }

    pub fn into_double_unchecked(self) -> f64 {
        if let Self::Double { inner, .. } = self {
            inner
        } else {
            panic!("not a double")
        }
    }

    pub fn into_big_number_unchecked(self) -> BigInt {
        if let Self::BigNumber { inner, .. } = self {
            inner
        } else {
            panic!("not a big number")
        }
    }

    pub fn into_verbatim_string_unchecked(self) -> ([u8; 3], B) {
        if let Self::VerbatimString { format, data, .. } = self {
            (format, data)
        } else {
            panic!("not a verbatim string")
        }
    }

    pub fn into_map_unchecked(self) -> HashMap<Self, Self> {
        if let Self::Map { inner, .. } = self {
            inner
        } else {
            panic!("not a map")
        }
    }

    pub fn into_push_unchecked(self) -> Vec<Self> {
        if let Self::Push { inner, .. } = self {
            inner
        } else {
            panic!("not a push")
        }
    }

    pub fn into_chunked_string_unchecked(self) -> Vec<B> {
        if let Self::ChunkedString(inner) = self {
            inner
        } else {
            panic!("not a chunked string")
        }
    }

    pub fn into_hello_unchecked(self) -> (i128, Option<(B, B)>) {
        if let Self::Hello { version, auth } = self {
            (version, auth)
        } else {
            panic!("not a hello")
        }
    }

    pub fn add_attributes(&mut self, attrs: Attributes<B, S>)
    where
        Self: Hash + Eq,
    {
        match self {
            Self::SimpleString { attributes, .. }
            | Self::SimpleError { attributes, .. }
            | Self::Integer { attributes, .. }
            | Self::BlobString { attributes, .. }
            | Self::Array { attributes, .. }
            | Self::Boolean { attributes, .. }
            | Self::Double { attributes, .. }
            | Self::BigNumber { attributes, .. }
            | Self::BlobError { attributes, .. }
            | Self::VerbatimString { attributes, .. }
            | Self::Map { attributes, .. }
            | Self::Set { attributes, .. }
            | Self::Push { attributes, .. } => {
                if let Some(attr) = attributes.as_mut() {
                    attr.extend(attrs);
                } else {
                    *attributes = Some(attrs);
                }
            }
            Self::Null | Self::ChunkedString(_) | Self::Hello { .. } => {
                panic!("can't have attributes")
            }
        }
    }
}

impl<B, S> Resp3<B, S>
where
    B: AsRef<[u8]>,
    S: AsRef<str>,
{
    // TODO: test
    pub fn size(&self) -> usize {
        fn attributes_size<B, S>(attributes: &Option<Attributes<B, S>>) -> usize
        where
            B: AsRef<[u8]>,
            S: AsRef<str>,
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
            Self::SimpleString { inner, attributes } => {
                inner.as_ref().len() + 3 + attributes_size(attributes)
            }
            Self::SimpleError { inner, attributes } => {
                inner.as_ref().len() + 3 + attributes_size(attributes)
            }
            Self::Integer { inner, attributes } => {
                itoa::Buffer::new().format(*inner).len() + 3 + attributes_size(attributes)
            }
            Self::BlobString { inner, attributes } => {
                itoa::Buffer::new().format(inner.as_ref().len()).len()
                    + 3
                    + inner.as_ref().len()
                    + 2
                    + attributes_size(attributes)
            }
            Self::BlobError { inner, attributes } => {
                itoa::Buffer::new().format(inner.as_ref().len()).len()
                    + 3
                    + inner.as_ref().len()
                    + 2
                    + attributes_size(attributes)
            }
            Self::Array { inner, attributes } => {
                let mut size =
                    itoa::Buffer::new().format(inner.len()).len() + 3 + attributes_size(attributes);
                for r in inner {
                    size += r.size();
                }
                size
            }
            Self::Null => 3,
            Self::Boolean { attributes, .. } => 3 + attributes_size(attributes),
            Self::Double { inner, attributes } => {
                ryu::Buffer::new().format(*inner).len() + 3 + attributes_size(attributes)
            }
            Self::BigNumber { inner, attributes } => {
                inner.to_str_radix(10).len() + 3 + attributes_size(attributes)
            }
            Self::VerbatimString {
                data, attributes, ..
            } => 3 + 1 + 3 + 1 + data.as_ref().len() + 2 + attributes_size(attributes),
            Self::Map { inner, attributes } => {
                let mut size =
                    itoa::Buffer::new().format(inner.len()).len() + 3 + attributes_size(attributes);
                for (k, v) in inner {
                    size += k.size() + v.size();
                }

                size
            }
            Self::Set { inner, attributes } => {
                let mut size =
                    itoa::Buffer::new().format(inner.len()).len() + 3 + attributes_size(attributes);
                for r in inner {
                    size += r.size();
                }
                size
            }
            Self::Push { inner, attributes } => {
                let mut size =
                    itoa::Buffer::new().format(inner.len()).len() + 3 + attributes_size(attributes);
                for r in inner {
                    size += r.size();
                }
                size
            }
            Self::ChunkedString(chunks) => {
                let mut size = 3;
                for chunk in chunks {
                    size += 1 + itoa::Buffer::new().format(chunk.as_ref().len()).len() + 2;
                    size += chunk.as_ref().len() + 2;
                }
                size
            }
            Self::Hello { version, auth } => {
                let mut size = itoa::Buffer::new().format(*version).len() + 1;
                if let Some((username, password)) = auth {
                    size += username.as_ref().len() + 1 + password.as_ref().len() + 1;
                }
                size
            }
        }
    }
}

impl<B, S> Debug for Resp3<B, S>
where
    B: std::fmt::Debug,
    S: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Resp3::SimpleString { inner, attributes } => f
                .debug_struct("SimpleString")
                .field("inner", inner)
                .field("attributes", attributes)
                .finish(),
            Resp3::SimpleError { inner, attributes } => f
                .debug_struct("SimpleError")
                .field("inner", inner)
                .field("attributes", attributes)
                .finish(),
            Resp3::Integer { inner, attributes } => f
                .debug_struct("Integer")
                .field("inner", inner)
                .field("attributes", attributes)
                .finish(),
            Resp3::BlobString { inner, attributes } => f
                .debug_struct("BlobString")
                .field("inner", inner)
                .field("attributes", attributes)
                .finish(),
            Resp3::Array { inner, attributes } => f
                .debug_struct("Array")
                .field("inner", inner)
                .field("attributes", attributes)
                .finish(),
            Resp3::Null => f.debug_tuple("Null").finish(),
            Resp3::Boolean { inner, attributes } => f
                .debug_struct("Boolean")
                .field("inner", inner)
                .field("attributes", attributes)
                .finish(),
            Resp3::Double { inner, attributes } => f
                .debug_struct("Double")
                .field("inner", inner)
                .field("attributes", attributes)
                .finish(),
            Resp3::BigNumber { inner, attributes } => f
                .debug_struct("BigNumber")
                .field("inner", inner)
                .field("attributes", attributes)
                .finish(),
            Resp3::BlobError { inner, attributes } => f
                .debug_struct("BlobError")
                .field("inner", inner)
                .field("attributes", attributes)
                .finish(),
            Resp3::VerbatimString {
                format,
                data,
                attributes,
            } => f
                .debug_struct("VerbatimString")
                .field("format", format)
                .field("data", data)
                .field("attributes", attributes)
                .finish(),
            Resp3::Map { inner, attributes } => f
                .debug_struct("Map")
                .field("inner", inner)
                .field("attributes", attributes)
                .finish(),
            Resp3::Set { inner, attributes } => f
                .debug_struct("Set")
                .field("inner", inner)
                .field("attributes", attributes)
                .finish(),
            Resp3::Push { inner, attributes } => f
                .debug_struct("Push")
                .field("inner", inner)
                .field("attributes", attributes)
                .finish(),
            Resp3::ChunkedString(chunks) => f
                .debug_struct("ChunkedString")
                .field("chunks", chunks)
                .finish(),
            Resp3::Hello { version, auth } => f
                .debug_struct("Hello")
                .field("version", version)
                .field("auth", auth)
                .finish(),
        }
    }
}

impl<B, S> Display for Resp3<B, S>
where
    B: AsRef<[u8]>,
    S: AsRef<str>,
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
                write!(f, "Hello({}", version)?;
                if let Some((username, password)) = auth {
                    write!(
                        f,
                        ", AUTH: {}, {}",
                        String::from_utf8_lossy(username.as_ref()),
                        String::from_utf8_lossy(password.as_ref())
                    )?;
                }
                write!(f, ")")
            }
        }
    }
}

impl<B, S> Hash for Resp3<B, S>
where
    B: Hash,
    S: Hash,
{
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        let attrs_hash = |attrs: &Attributes<B, S>, state: &mut H| {
            attrs.iter().for_each(|(k, v)| {
                k.hash(state);
                v.hash(state);
            });
        };
        match self {
            Self::SimpleString { inner, attributes } => {
                SIMPLE_STRING_PREFIX.hash(state);
                inner.hash(state);
                if let Some(attr) = attributes.as_ref() {
                    attrs_hash(attr, state)
                }
            }
            Self::SimpleError { inner, attributes } => {
                SIMPLE_ERROR_PREFIX.hash(state);
                inner.hash(state);
                if let Some(attr) = attributes.as_ref() {
                    attrs_hash(attr, state)
                }
            }
            Self::Integer { inner, attributes } => {
                INTEGER_PREFIX.hash(state);
                inner.hash(state);
                if let Some(attr) = attributes.as_ref() {
                    attrs_hash(attr, state)
                }
            }
            Self::BlobString { inner, attributes } => {
                BLOB_STRING_PREFIX.hash(state);
                inner.hash(state);
                if let Some(attr) = attributes.as_ref() {
                    attrs_hash(attr, state)
                }
            }
            Self::Array { inner, attributes } => {
                ARRAY_PREFIX.hash(state);
                inner.hash(state);
                if let Some(attr) = attributes.as_ref() {
                    attrs_hash(attr, state)
                }
            }
            Self::Null => NULL_PREFIX.hash(state),
            Self::Boolean { inner, attributes } => {
                BOOLEAN_PREFIX.hash(state);
                inner.hash(state);
                if let Some(attr) = attributes.as_ref() {
                    attrs_hash(attr, state)
                }
            }
            Self::Double { inner, attributes } => {
                DOUBLE_PREFIX.hash(state);
                inner.to_bits().hash(state);
                if let Some(attr) = attributes.as_ref() {
                    attrs_hash(attr, state)
                }
            }
            Self::BigNumber { inner, attributes } => {
                BIG_NUMBER_PREFIX.hash(state);
                inner.hash(state);
                if let Some(attr) = attributes.as_ref() {
                    attrs_hash(attr, state)
                }
            }
            Self::BlobError { inner, attributes } => {
                BLOB_ERROR_PREFIX.hash(state);
                inner.hash(state);
                if let Some(attr) = attributes.as_ref() {
                    attrs_hash(attr, state)
                }
            }
            Self::VerbatimString {
                format,
                data,
                attributes,
            } => {
                VERBATIM_STRING_PREFIX.hash(state);
                format.hash(state);
                data.hash(state);
                if let Some(attr) = attributes.as_ref() {
                    attrs_hash(attr, state)
                }
            }
            Self::Map { inner, attributes } => {
                MAP_PREFIX.hash(state);
                inner.iter().for_each(|(k, v)| {
                    k.hash(state);
                    v.hash(state);
                });
                if let Some(attr) = attributes.as_ref() {
                    attrs_hash(attr, state)
                }
            }
            Self::Set { inner, attributes } => {
                SET_PREFIX.hash(state);
                inner.iter().for_each(|f| f.hash(state));
                if let Some(attr) = attributes.as_ref() {
                    attrs_hash(attr, state)
                }
            }
            Self::Push { inner, attributes } => {
                PUSH_PREFIX.hash(state);
                inner.hash(state);
                if let Some(attr) = attributes.as_ref() {
                    attrs_hash(attr, state)
                }
            }
            Self::ChunkedString(chunks) => {
                CHUNKED_STRING_PREFIX.hash(state);
                chunks.iter().for_each(|c| c.hash(state));
            }
            Self::Hello { version, auth } => {
                version.hash(state);
                if let Some((username, password)) = auth.as_ref() {
                    username.hash(state);
                    password.hash(state);
                }
            }
        }
    }
}

impl<B, S> PartialEq for Resp3<B, S>
where
    B: PartialEq + Hash,
    S: PartialEq + Hash,
{
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::SimpleString { inner: a, .. }, Self::SimpleString { inner: b, .. }) => a == b,
            (Self::SimpleError { inner: a, .. }, Self::SimpleError { inner: b, .. }) => a == b,
            (Self::Integer { inner: a, .. }, Self::Integer { inner: b, .. }) => a == b,
            (Self::BlobString { inner: a, .. }, Self::BlobString { inner: b, .. }) => a == b,
            (Self::Array { inner: a, .. }, Self::Array { inner: b, .. }) => a == b,
            (Self::Null, Self::Null) => true,
            (Self::Boolean { inner: a, .. }, Self::Boolean { inner: b, .. }) => a == b,
            (Self::Double { inner: a, .. }, Self::Double { inner: b, .. }) => a - b < f64::EPSILON,
            (Self::BigNumber { inner: a, .. }, Self::BigNumber { inner: b, .. }) => a == b,
            (Self::BlobError { inner: a, .. }, Self::BlobError { inner: b, .. }) => a == b,
            (
                Self::VerbatimString {
                    format: a, data: b, ..
                },
                Self::VerbatimString {
                    format: c, data: d, ..
                },
            ) => a == c && b == d,
            (Self::Map { inner: a, .. }, Self::Map { inner: b, .. }) => a == b,
            (Self::Set { inner: a, .. }, Self::Set { inner: b, .. }) => a == b,
            (Self::Push { inner: a, .. }, Self::Push { inner: b, .. }) => a == b,
            (Self::ChunkedString(a), Self::ChunkedString(b)) => a == b,
            (
                Self::Hello {
                    version: a,
                    auth: b,
                },
                Self::Hello {
                    version: c,
                    auth: d,
                },
            ) => a == c && b == d,
            _ => false,
        }
    }
}

impl<B, S> Eq for Resp3<B, S> where Self: PartialEq {}

#[cfg(feature = "mlua")]
impl<B, S> mlua::IntoLua for Resp3<B, S>
where
    B: AsRef<[u8]>,
    S: AsRef<str>,
{
    fn into_lua(self, lua: &'_ Lua) -> LuaResult<LuaValue> {
        match self {
            // SimpleString -> Lua Table { ok: Lua String }
            Self::SimpleString { inner, .. } => {
                let table = lua.create_table()?;
                table.set("ok", inner.as_ref())?;
                Ok(LuaValue::Table(table))
            }
            // SimpleError -> Lua Table { err: Lua String }
            Self::SimpleError { inner, .. } => {
                let table = lua.create_table()?;
                table.set("err", inner.as_ref())?;
                Ok(LuaValue::Table(table))
            }
            // Integer -> Lua Integer
            Self::Integer { inner, .. } => inner.into_lua(lua),
            // BlobString -> Lua String
            Self::BlobString { inner, .. } => lua.create_string(inner).map(LuaValue::String),
            // Array -> Lua Table(Array)
            Self::Array { inner, .. } => inner.into_lua(lua),
            // Null -> Lua Nil
            Self::Null => Ok(LuaValue::Nil),
            // Boolean -> Lua Boolean
            Self::Boolean { inner, .. } => inner.into_lua(lua),
            // Lua table with a single double field containing a Lua number representing the double value.
            // Double -> Lua Table { double: Lua Number }
            Self::Double { inner, .. } => {
                let table = lua.create_table()?;
                table.set("double", inner)?;
                Ok(LuaValue::Table(table))
            }
            // Lua table with a single big_number field containing a Lua string representing the big number value.
            // BigNumber -> Lua Table { big_number: Lua String }
            Self::BigNumber { inner, .. } => {
                let n = inner.to_str_radix(10);

                let table = lua.create_table()?;
                table.set("big_number", n)?;
                Ok(LuaValue::Table(table))
            }
            // BlobError -> Lua String
            Self::BlobError { inner, .. } => inner.as_ref().into_lua(lua),
            // Lua table with a single verbatim_string field containing a Lua table with two fields, string and format, representing the verbatim string and its format, respectively.
            // VerbatimString -> Lua Table { verbatim_string: Lua Table { string: Lua String, format: Lua String } }
            Self::VerbatimString {
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
            Self::Map { inner, .. } => {
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
            Self::Set { inner, .. } => {
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
                to: "LuaValue".to_string(),
                message: Some("unsupported RESP3 type".to_string()),
            }),
        }
    }
}

#[cfg(feature = "mlua")]
impl<B, S> FromLua for Resp3<B, S>
where
    B: for<'a> From<&'a [u8]>,
    S: for<'a> From<&'a str>,
    Self: Hash + Eq,
{
    fn from_lua(value: LuaValue, _lua: &'_ Lua) -> LuaResult<Self> {
        match value {
            // Lua String -> Blob
            LuaValue::String(s) => Ok(Self::BlobString {
                inner: B::from(s.as_bytes().as_ref()),
                attributes: None,
            }),
            // Lua Integer -> Integer
            LuaValue::Integer(n) => Ok(Self::Integer {
                inner: n as i128,
                attributes: None,
            }),
            // Lua Number -> Double
            LuaValue::Number(n) => Ok(Self::Double {
                inner: n,
                attributes: None,
            }),
            // Lua Nil -> Null
            LuaValue::Nil => Ok(Self::Null),
            // Lua Boolean -> Boolean
            LuaValue::Boolean(b) => Ok(Self::Boolean {
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

                if let LuaValue::String(s) = ok {
                    return Ok(Self::SimpleString {
                        inner: S::from(s.to_str()?.as_ref()),
                        attributes: None,
                    });
                }

                let err = table.raw_get("err")?;

                if let LuaValue::String(s) = err {
                    return Ok(Self::SimpleError {
                        inner: S::from(s.to_str()?.as_ref()),
                        attributes: None,
                    });
                }

                let verbatim_string = table.raw_get("verbatim_string")?;

                if let LuaValue::Table(verbatim_string) = verbatim_string {
                    let format: mlua::String = verbatim_string.raw_get("format")?;
                    let format = format.as_bytes().as_ref().try_into().map_err(|_| {
                        mlua::Error::FromLuaConversionError {
                            from: "table",
                            to: "RESP3::VerbatimString".to_string(),
                            message: Some("invalid encoding format".to_string()),
                        }
                    })?;

                    let data = if let LuaValue::String(s) = verbatim_string.raw_get("string")? {
                        B::from(s.as_bytes().as_ref())
                    } else {
                        return Err(mlua::Error::FromLuaConversionError {
                            from: "table",
                            to: "RESP3::VerbatimString".to_string(),
                            message: Some("invalid string".to_string()),
                        });
                    };

                    return Ok(Self::VerbatimString {
                        format,
                        data,
                        attributes: None,
                    });
                }

                let map = table.raw_get("map")?;

                if let LuaValue::Table(map) = map {
                    let mut map_table = HashMap::new();
                    for pair in map.pairs::<LuaValue, LuaValue>() {
                        let (k, v) = pair?;
                        let k = Self::from_lua(k, _lua)?;
                        let v = Self::from_lua(v, _lua)?;
                        map_table.insert(k, v);
                    }

                    return Ok(Self::Map {
                        inner: map_table,
                        attributes: None,
                    });
                }

                let set = table.raw_get("set")?;

                if let LuaValue::Table(set) = set {
                    let mut set_table = HashSet::new();
                    for pair in set.pairs::<LuaValue, bool>() {
                        let (f, _) = pair?;
                        let f = Self::from_lua(f, _lua)?;
                        set_table.insert(f);
                    }

                    return Ok(Self::Set {
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
                    array.push(Self::from_lua(ele, _lua)?);
                }

                Ok(Self::Array {
                    inner: array,
                    attributes: None,
                })
            }
            _ => Err(mlua::Error::FromLuaConversionError {
                from: value.type_name(),
                to: "RESP3".to_string(),
                message: Some("invalid value type".to_string()),
            }),
        }
    }
}
