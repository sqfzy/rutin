#![allow(dead_code)]
use crate::{cmd::CmdError, server::ServerError, util, Int};
use ahash::{AHashMap, AHashSet};
use bytes::{Buf, Bytes, BytesMut};
use mlua::{prelude, FromLuaMulti, IntoLua, Value};
use num_bigint::BigInt;
use snafu::Snafu;
use std::{
    borrow::Cow,
    io::{self, BufRead, Cursor, Read},
    iter::Iterator,
};
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio_util::{
    bytes::BufMut,
    codec::{Decoder, Encoder},
};

// #define REDISMODULE_REPLY_UNKNOWN -1
// #define REDISMODULE_REPLY_STRING 0
// #define REDISMODULE_REPLY_ERROR 1
// #define REDISMODULE_REPLY_INTEGER 2
// #define REDISMODULE_REPLY_ARRAY 3
// #define REDISMODULE_REPLY_NULL 4
// #define REDISMODULE_REPLY_MAP 5
// #define REDISMODULE_REPLY_SET 6
// #define REDISMODULE_REPLY_BOOL 7
// #define REDISMODULE_REPLY_DOUBLE 8
// #define REDISMODULE_REPLY_BIG_NUMBER 9
// #define REDISMODULE_REPLY_VERBATIM_STRING 10
// #define REDISMODULE_REPLY_ATTRIBUTE 11
// #define REDISMODULE_REPLY_PROMISE 12
//
// Simple strings	RESP2	Simple	+
// Simple Errors	RESP2	Simple	-
// Integers	RESP2	Simple	:
// Bulk strings	RESP2	Aggregate	$
// Arrays	RESP2	Aggregate	*
// Nulls	RESP3	Simple	_
// Booleans	RESP3	Simple	#
// Doubles	RESP3	Simple	,
// Big numbers	RESP3	Simple	(
// Bulk errors	RESP3	Aggregate	!
// Verbatim strings	RESP3	Aggregate	=
// Maps	RESP3	Aggregate	%
// Sets	RESP3	Aggregate	~
// Pushes	RESP3	Aggregate	>
#[derive(Clone, Debug)]
pub enum RESP3<B = Bytes, S = String>
where
    B: AsRef<[u8]>,
    S: AsRef<str>,
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
    Array(Vec<RESP3>),

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
    Map(AHashMap<RESP3, RESP3>),

    // ~<number-of-elements>\r\n<element-1>...<element-n>
    Set(AHashSet<RESP3>),

    // ><number-of-elements>\r\n<element-1>...<element-n>
    // 通常用于服务端主动向客户端推送消息
    Push(Vec<RESP3>),
}

impl RESP3 {
    #[inline]
    pub fn size(&self) -> usize {
        match self {
            RESP3::SimpleString(s) => s.len() + 3, // 1 + s.len() + 2
            RESP3::SimpleError(e) => e.len() + 3,  // 1 + e.len() + 2
            RESP3::Integer(n) => itoa::Buffer::new().format(*n).len() + 3, // 1 + n.len() + 2
            RESP3::Bulk(b) => b.len() + itoa::Buffer::new().format(b.len()).len() + 5, //  1 + len.len() + 2 + len + 2
            RESP3::Null => 3,                                                          // 1 + 2
            RESP3::Array(frames) => {
                itoa::Buffer::new().format(frames.len()).len()
                    + 3
                    + frames.iter().map(|f| f.size()).sum::<usize>()
            }
            RESP3::Boolean(_) => 3, // 1 + 2
            RESP3::Double { double, exponent } => {
                let mut len = 1 + ryu::Buffer::new().format(*double).len();
                if let Some(exponent) = exponent {
                    len += itoa::Buffer::new().format(*exponent).len() + 3; // 1 + exponent.len() + 2
                }
                len
            }
            RESP3::BigNumber(n) => n.to_str_radix(10).len() + 3, // 1 + len + 2
            RESP3::BulkError(e) => e.len() + itoa::Buffer::new().format(e.len()).len() + 5, // 1 + len.len() + 2 + len + 2
            RESP3::VerbatimString { data, .. } => {
                data.len() + itoa::Buffer::new().format(data.len()).len() + 9 // 1 + len.len() + 2 + 3 + 1 + data.len() + 2
            }
            RESP3::Map(map) => {
                itoa::Buffer::new().format(map.len()).len()
                    + 3
                    + map.iter().map(|(k, v)| k.size() + v.size()).sum::<usize>()
            }
            RESP3::Set(set) => {
                itoa::Buffer::new().format(set.len()).len()
                    + 3
                    + set.iter().map(|f| f.size()).sum::<usize>()
            }
            RESP3::Push(frames) => {
                itoa::Buffer::new().format(frames.len()).len()
                    + 3
                    + frames.iter().map(|f| f.size()).sum::<usize>()
            }
        }
    }
}

impl RESP3 {
    #[inline]
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(self.size());
        self.encode_buf(&mut buf);
        buf.freeze()
    }

    #[inline]
    pub fn encode_buf(&self, buf: &mut impl BufMut) {
        match self {
            RESP3::SimpleString(s) => {
                buf.put_u8(b'+');
                buf.put_slice(s.as_bytes());
                buf.put_slice(b"\r\n");
            }
            RESP3::SimpleError(e) => {
                buf.put_u8(b'-');
                buf.put_slice(e.as_bytes());
                buf.put_slice(b"\r\n");
            }
            RESP3::Integer(n) => {
                buf.put_u8(b':');
                buf.put_slice(itoa::Buffer::new().format(*n).as_bytes());
                buf.put_slice(b"\r\n");
            }
            RESP3::Bulk(b) => {
                buf.put_u8(b'$');
                buf.put_slice(itoa::Buffer::new().format(b.len()).as_bytes());
                buf.put_slice(b"\r\n");
                buf.put_slice(b);
                buf.put_slice(b"\r\n");
            }
            RESP3::Array(frames) => {
                buf.put_u8(b'*');
                buf.put_slice(itoa::Buffer::new().format(frames.len()).as_bytes());
                buf.put_slice(b"\r\n");
                for frame in frames {
                    frame.encode_buf(buf);
                }
            }
            RESP3::Null => {
                buf.put_slice(b"_\r\n");
            }
            RESP3::Boolean(b) => {
                buf.put_u8(b'#');
                buf.put_slice(if *b { b"t" } else { b"f" });
                buf.put_slice(b"\r\n");
            }
            RESP3::Double { double, exponent } => {
                buf.put_u8(b',');
                if double.fract() == 0.0 {
                    buf.put_slice(&itoa::Buffer::new().format((*double) as i64).as_bytes());
                } else {
                    buf.put_slice(ryu::Buffer::new().format(*double).as_bytes());
                }
                if let Some(exponent) = exponent {
                    buf.put_u8(b'e');
                    buf.put_slice(itoa::Buffer::new().format(*exponent).as_bytes());
                }
                buf.put_slice(b"\r\n");
            }
            RESP3::BigNumber(n) => {
                buf.put_u8(b'(');
                buf.put_slice(n.to_str_radix(10).as_bytes());
                buf.put_slice(b"\r\n");
            }
            RESP3::BulkError(e) => {
                buf.put_u8(b'!');
                buf.put_slice(itoa::Buffer::new().format(e.len()).as_bytes());
                buf.put_slice(b"\r\n");
                buf.put_slice(e);
                buf.put_slice(b"\r\n");
            }
            RESP3::VerbatimString { encoding, data } => {
                buf.put_u8(b'=');
                buf.put_slice(itoa::Buffer::new().format(data.len()).as_bytes());
                buf.put_slice(b"\r\n");
                buf.put_slice(encoding);
                buf.put_u8(b':');
                buf.put_slice(data);
                buf.put_slice(b"\r\n");
            }
            RESP3::Map(map) => {
                buf.put_u8(b'%');
                buf.put_slice(itoa::Buffer::new().format(map.len()).as_bytes());
                buf.put_slice(b"\r\n");
                for (k, v) in map {
                    k.encode_buf(buf);
                    v.encode_buf(buf);
                }
            }
            RESP3::Set(set) => {
                buf.put_u8(b'~');
                buf.put_slice(itoa::Buffer::new().format(set.len()).as_bytes());
                buf.put_slice(b"\r\n");
                for frame in set {
                    frame.encode_buf(buf);
                }
            }
            RESP3::Push(frames) => {
                buf.put_u8(b'>');
                buf.put_slice(itoa::Buffer::new().format(frames.len()).as_bytes());
                buf.put_slice(b"\r\n");
                for frame in frames {
                    frame.encode_buf(buf);
                }
            }
        }
    }

    #[allow(clippy::multiple_bound_locations)]
    #[async_recursion::async_recursion]
    async fn decode_async<R: AsyncRead + Unpin + Send>(
        io_read: &mut R,
        src: &mut BytesMut,
    ) -> io::Result<Option<RESP3>> {
        if src.is_empty() && io_read.read_buf(src).await? == 0 {
            return Ok(None);
        }

        debug_assert!(!src.is_empty());

        let res = match src.get_u8() {
            b'+' => RESP3::SimpleError(RESP3::decode_string_async(io_read, src).await?),
            b'-' => RESP3::SimpleError(RESP3::decode_string_async(io_read, src).await?),
            b':' => RESP3::Integer(RESP3::decode_decimal_async(io_read, src).await?),
            b'$' => {
                let len = RESP3::decode_length_async(io_read, src).await?;

                if src.remaining() < len + 2 && io_read.read_buf(src).await? == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "incomplete frame",
                    ));
                }

                let res = src.split_to(len);
                src.advance(2);

                RESP3::Bulk(res.freeze())
            }
            b'*' => {
                let len = RESP3::decode_decimal_async(io_read, src).await? as usize;

                let mut frames = Vec::with_capacity(len);
                for _ in 0..len {
                    let frame = RESP3::decode_async(io_read, src)
                        .await?
                        .ok_or(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            "incomplete frame",
                        ))?;
                    frames.push(frame);
                }

                RESP3::Array(frames)
            }
            b'_' => {
                if src.remaining() < 2 && io_read.read_buf(src).await? == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "incomplete frame",
                    ));
                }
                src.advance(2);
                RESP3::Null
            }
            b'#' => {
                if src.remaining() < 3 && io_read.read_buf(src).await? == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "incomplete frame",
                    ));
                }

                let b = match src.get_u8() {
                    b't' => true,
                    b'f' => false,
                    _ => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "invalid boolean value",
                        ));
                    }
                };

                src.advance(2);
                RESP3::Boolean(b)
            }
            b',' => {
                let mut line = RESP3::decode_line_async(io_read, src).await?;

                let integral = if let Some(i) = memchr::memchr(b'.', &line) {
                    line.split_to(i)
                } else {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "invalid double value",
                    ));
                };
                // let mut iter = line.split_(|&b| b == b'e' || b == b'E');
                // let double = iter.next().ok_or(io::Error::new(
                //     io::ErrorKind::InvalidData,
                //     "invalid double value",
                // ))?;
                // let exponent = iter.next().map(|e| {
                //     util::atoi(e)
                //         .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid exponent"))
                // });
                //
                // RESP3::Double {
                //     double: util::atof(double).map_err(|_| {
                //         io::Error::new(io::ErrorKind::InvalidData, "invalid double")
                //     })?,
                //     exponent: exponent.transpose()?,
                // }
                todo!()
            }
            prefix => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "invalid frame prefix",
                ));
            }
        };

        Ok(Some(res))
    }

    async fn decode_line_async<R: AsyncRead + Unpin + Send>(
        io_read: &mut R,
        src: &mut BytesMut,
    ) -> io::Result<BytesMut> {
        loop {
            if let Some(i) = memchr::memchr(b'\n', src) {
                if i > 0 && src[i - 1] == b'\r' {
                    let line = src.split_to(i - 1);
                    src.advance(2);

                    return Ok(line);
                }
            }

            if io_read.read_buf(src).await? == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "incomplete frame",
                ));
            }
        }
    }

    async fn decode_decimal_async<R: AsyncRead + Unpin + Send>(
        io_read: &mut R,
        src: &mut BytesMut,
    ) -> io::Result<Int> {
        let line = RESP3::decode_line_async(io_read, src).await?;
        let decimal = util::atoi(&line)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid integer"))?;
        Ok(decimal)
    }

    async fn decode_length_async<R: AsyncRead + Unpin + Send>(
        io_read: &mut R,
        src: &mut BytesMut,
    ) -> io::Result<usize> {
        let line = RESP3::decode_line_async(io_read, src).await?;
        let len = util::atoi(&line)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid length"))?;
        Ok(len)
    }

    async fn decode_string_async<R: AsyncRead + Unpin + Send>(
        io_read: &mut R,
        src: &mut BytesMut,
    ) -> io::Result<String> {
        let line = RESP3::decode_line_async(io_read, src).await?;
        let string = String::from_utf8(line.to_vec())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid string"))?;
        Ok(string)
    }
}

// fn decode_line<'a>(src: &mut Cursor<&'a [u8]>) -> Option<&'a [u8]> {
//     loop {
//         if let Some(i) = memchr::memchr(b'\n', src.get_ref()) {
//             if i > 0 && src.get_ref()[i - 1] == b'\r' {
//                 // 找到一个完整的行
//                 let line = &src.get_ref()[..i - 1];
//                 src.advance(2);
//
//                 return Some(line);
//             }
//             // 未找到一个完整的行，但src中仍有未查找的数据
//             src.advance(i + 1);
//         } else {
//             // 未找到一个完整的行，且已经查找完src中的所有数据
//             return None;
//         }
//     }
// }
//
// fn decode_decimal(src: &mut Cursor<&[u8]>) -> std::io::Result<Option<Int>> {
//     let line = if let Some(l) = decode_line(src) {
//         l
//     } else {
//         return Ok(None);
//     };
//
//     let decimal = util::atoi(line)
//         .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid integer"))?;
//     Ok(Some(decimal))
// }
//
// fn decode_string(src: &mut Cursor<&[u8]>) -> std::io::Result<Option<String>> {
//     let line = if let Some(l) = decode_line(src) {
//         l
//     } else {
//         return Ok(None);
//     };
//
//     let string = String::from_utf8(line.to_vec())
//         .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid string"))?;
//     Ok(Some(string))
// }
