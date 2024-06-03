use crate::{
    util::{self, atof},
    Int,
};
use ahash::{AHashMap, AHashSet};
use bytes::{Buf, Bytes, BytesMut};
use either::{for_both, Either};
use num_bigint::BigInt;
use std::{
    hash::Hash,
    io::{self},
    iter::Iterator,
};
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio_util::bytes::BufMut;

#[derive(Clone, Debug)]
struct Empty;

impl AsRef<[RESP3<RESP3<Empty>>]> for Empty {
    fn as_ref(&self) -> &[RESP3<RESP3<Empty>>] {
        &[]
    }
}

impl AsRef<[RESP3<RESP3<Empty>>]> for RESP3<Empty> {
    fn as_ref(&self) -> &[RESP3<RESP3<Empty>>] {
        &[]
    }
}

type RespMap<V, B, S> =
    Either<Vec<(RESP3<V, B, S>, RESP3<V, B, S>)>, AHashMap<RESP3<V, B, S>, RESP3<V, B, S>>>;

type RespSet<V, B, S> = Either<Vec<RESP3<V, B, S>>, AHashSet<RESP3<V, B, S>>>;

#[derive(Clone, Debug)]
pub enum RESP3<V, B = Bytes, S = String>
where
    B: AsRef<[u8]>,
    S: AsRef<str>,
    V: AsRef<[RESP3<RESP3<Empty>>]>,
{
    // +<str>\r\n
    SimpleString(S),

    // -<err>\r\n
    SimpleError(S),

    // :[<+|->]<value>\r\n
    Integer(Int),

    // $<length>\r\n<data>\r\n
    Bulk(B),

    // *<number-of-elements>\r\n<element-1>...<element-n>
    Array(V),

    // _\r\n
    Null,

    // #<t|f>\r\n
    Boolean(bool),

    // ,[<+|->]<integral>[.<fractional>][<E|e>[sign]<exponent>]\r\n
    Double { double: f64, exponent: Option<Int> },

    // ([+|-]<number>\r\n
    BigNumber(BigInt),

    // !<length>\r\n<error>\r\n
    BulkError(B),

    // =<length>\r\n<encoding>:<data>\r\n
    // 类似于Bulk，但是多了一个encoding字段用于指定数据的编码方式
    VerbatimString { encoding: [u8; 3], data: B },

    // %<number-of-entries>\r\n<key-1><value-1>...<key-n><value-n>
    // Left: 由用户保证传入的key唯一；Right: 由Frame本身保证key唯一
    Map(RespMap<V, B, S>),

    // ~<number-of-elements>\r\n<element-1>...<element-n>
    // Left: 由用户保证传入的元素唯一；Right: 由Frame本身保证元素唯一
    Set(RespSet<V, B, S>),

    // ><number-of-elements>\r\n<element-1>...<element-n>
    // 通常用于服务端主动向客户端推送消息
    Push(V),
}
