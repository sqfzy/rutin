// #![allow(dead_code)]
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

type RespMap<B, S> = Either<Vec<(RESP3<B, S>, RESP3<B, S>)>, AHashMap<RESP3<B, S>, RESP3<B, S>>>;
type RespSet<B, S> = Either<Vec<RESP3<B, S>>, AHashSet<RESP3<B, S>>>;

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
    Array(Vec<RESP3<B, S>>),

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
    Map(RespMap<B, S>),

    // ~<number-of-elements>\r\n<element-1>...<element-n>
    // Left: 由用户保证传入的元素唯一；Right: 由Frame本身保证元素唯一
    Set(RespSet<B, S>),

    // ><number-of-elements>\r\n<element-1>...<element-n>
    // 通常用于服务端主动向客户端推送消息
    Push(Vec<RESP3<B, S>>),
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
            RESP3::Map(map) => for_both!(map, map => {
                itoa::Buffer::new().format(map.len()).len()
                    + 3
                    + map.iter().map(|(k, v)| k.size() + v.size()).sum::<usize>()
            }),
            RESP3::Set(set) => for_both!(set, set => {
                itoa::Buffer::new().format(set.len()).len()
                    + 3
                    + set.iter().map(|f| f.size()).sum::<usize>()
            }),
            RESP3::Push(frames) => {
                itoa::Buffer::new().format(frames.len()).len()
                    + 3
                    + frames.iter().map(|f| f.size()).sum::<usize>()
            }
        }
    }

    pub fn on_simple_string() {}
}

impl<B: AsRef<[u8]>, S: AsRef<str>> RESP3<B, S> {
    pub fn as_simple_string(&self) -> Option<&S> {
        match self {
            RESP3::SimpleString(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_simple_error(&self) -> Option<&S> {
        match self {
            RESP3::SimpleError(e) => Some(e),
            _ => None,
        }
    }

    pub fn as_integer(&self) -> Option<Int> {
        match self {
            RESP3::Integer(n) => Some(*n),
            _ => None,
        }
    }

    pub fn as_bulk(&self) -> Option<&B> {
        match self {
            RESP3::Bulk(b) => Some(b),
            _ => None,
        }
    }

    pub fn as_array(&self) -> Option<&Vec<RESP3<B, S>>> {
        match self {
            RESP3::Array(frames) => Some(frames),
            _ => None,
        }
    }

    pub fn as_null(&self) -> Option<()> {
        match self {
            RESP3::Null => Some(()),
            _ => None,
        }
    }

    pub fn as_boolean(&self) -> Option<bool> {
        match self {
            RESP3::Boolean(b) => Some(*b),
            _ => None,
        }
    }

    pub fn as_double(&self) -> Option<(f64, Option<Int>)> {
        match self {
            RESP3::Double { double, exponent } => Some((*double, *exponent)),
            _ => None,
        }
    }

    pub fn as_big_number(&self) -> Option<&BigInt> {
        match self {
            RESP3::BigNumber(n) => Some(n),
            _ => None,
        }
    }

    pub fn as_bulk_error(&self) -> Option<&B> {
        match self {
            RESP3::BulkError(e) => Some(e),
            _ => None,
        }
    }

    pub fn as_verbatim_string(&self) -> Option<(&[u8; 3], &B)> {
        match self {
            RESP3::VerbatimString { encoding, data } => Some((encoding, data)),
            _ => None,
        }
    }

    pub fn as_map(&self) -> Option<&RespMap<B, S>> {
        match self {
            RESP3::Map(map) => Some(map),
            _ => None,
        }
    }

    pub fn as_set(&self) -> Option<&RespSet<B, S>> {
        match self {
            RESP3::Set(set) => Some(set),
            _ => None,
        }
    }

    pub fn as_push(&self) -> Option<&Vec<RESP3<B, S>>> {
        match self {
            RESP3::Push(frames) => Some(frames),
            _ => None,
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
            RESP3::SimpleString(s) => {
                buf.put_u8(b'+');
                buf.put_slice(s.as_ref().as_bytes());
                buf.put_slice(b"\r\n");
            }
            RESP3::SimpleError(e) => {
                buf.put_u8(b'-');
                buf.put_slice(e.as_ref().as_bytes());
                buf.put_slice(b"\r\n");
            }
            RESP3::Integer(n) => {
                buf.put_u8(b':');
                buf.put_slice(itoa::Buffer::new().format(*n).as_bytes());
                buf.put_slice(b"\r\n");
            }
            RESP3::Bulk(b) => {
                buf.put_u8(b'$');
                buf.put_slice(itoa::Buffer::new().format(b.as_ref().len()).as_bytes());
                buf.put_slice(b"\r\n");
                buf.put_slice(b.as_ref());
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
                    buf.put_slice(itoa::Buffer::new().format((*double) as i64).as_bytes());
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
                buf.put_slice(itoa::Buffer::new().format(e.as_ref().len()).as_bytes());
                buf.put_slice(b"\r\n");
                buf.put_slice(e.as_ref());
                buf.put_slice(b"\r\n");
            }
            RESP3::VerbatimString { encoding, data } => {
                buf.put_u8(b'=');
                buf.put_slice(itoa::Buffer::new().format(data.as_ref().len()).as_bytes());
                buf.put_slice(b"\r\n");
                buf.put_slice(encoding);
                buf.put_u8(b':');
                buf.put_slice(data.as_ref());
                buf.put_slice(b"\r\n");
            }
            RESP3::Map(map) => for_both!(
                map,
                map =>  {
                    buf.put_u8(b'%');
                    buf.put_slice(itoa::Buffer::new().format(map.len()).as_bytes());
                    buf.put_slice(b"\r\n");
                    for (k, v) in map {
                        k.encode_buf(buf);
                        v.encode_buf(buf);
                    }
                }
            ),
            RESP3::Set(set) => for_both!(
                set,
                set =>  {
                    buf.put_u8(b'~');
                    buf.put_slice(itoa::Buffer::new().format(set.len()).as_bytes());
                    buf.put_slice(b"\r\n");
                    for frame in set {
                        frame.encode_buf(buf);
                    }
                }
            ),
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
}

impl RESP3 {
    #[allow(clippy::multiple_bound_locations)]
    #[async_recursion::async_recursion]
    pub async fn decode_async<R: AsyncRead + Unpin + Send>(
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
                let line = RESP3::decode_line_async(io_read, src).await?;

                let mut exp_pos = line.len();
                if let Some(i) = memchr::memchr2(b'e', b'E', &line) {
                    exp_pos = i;
                }

                let double = atof(&line[..exp_pos])
                    .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid double"))?;

                let exponent = if exp_pos != line.len() {
                    let exp = util::atoi(&line[exp_pos + 1..]).map_err(|_| {
                        io::Error::new(io::ErrorKind::InvalidData, "invalid exponent")
                    })?;
                    Some(exp)
                } else {
                    None
                };

                RESP3::Double { double, exponent }
            }
            b'(' => {
                let line = RESP3::decode_line_async(io_read, src).await?;
                let n = BigInt::parse_bytes(&line, 10).ok_or(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "invalid big number",
                ))?;
                RESP3::BigNumber(n)
            }
            b'!' => {
                let len = RESP3::decode_length_async(io_read, src).await?;

                if src.remaining() < len + 2 && io_read.read_buf(src).await? == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "incomplete frame",
                    ));
                }

                let e = src.split_to(len);
                src.advance(2);

                RESP3::BulkError(e.freeze())
            }
            b'=' => {
                let len = RESP3::decode_length_async(io_read, src).await?;

                if src.remaining() < len + 2 && io_read.read_buf(src).await? == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "incomplete frame",
                    ));
                }

                let encoding = src[0..3].try_into().unwrap();
                src.advance(4);

                let data = src.split_to(len).freeze();
                src.advance(2);

                RESP3::VerbatimString { encoding, data }
            }
            b'%' => {
                let len = RESP3::decode_decimal_async(io_read, src).await? as usize;

                let mut map = Vec::with_capacity(len);
                for _ in 0..len {
                    let k = RESP3::decode_async(io_read, src)
                        .await?
                        .ok_or(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            "incomplete frame",
                        ))?;
                    let v = RESP3::decode_async(io_read, src)
                        .await?
                        .ok_or(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            "incomplete frame",
                        ))?;
                    map.push((k, v));
                }

                // map的key由客户端保证唯一
                RESP3::Map(Either::Left(map))
            }
            b'~' => {
                let len = RESP3::decode_decimal_async(io_read, src).await? as usize;

                let mut set = Vec::with_capacity(len);
                for _ in 0..len {
                    let frame = RESP3::decode_async(io_read, src)
                        .await?
                        .ok_or(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            "incomplete frame",
                        ))?;
                    set.push(frame);
                }

                // set的元素由客户端保证唯一
                RESP3::Set(Either::Left(set))
            }
            b'>' => {
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

                RESP3::Push(frames)
            }
            prefix => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("invalid frame prefix: {prefix}"),
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

impl<B: AsRef<[u8]>, S: AsRef<str>> Hash for RESP3<B, S> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            RESP3::SimpleString(s) => s.as_ref().hash(state),
            RESP3::SimpleError(e) => e.as_ref().hash(state),
            RESP3::Integer(n) => n.hash(state),
            RESP3::Bulk(b) => b.as_ref().hash(state),
            RESP3::Array(frames) => frames.hash(state),
            RESP3::Null => state.write_u8(0),
            RESP3::Boolean(b) => b.hash(state),
            RESP3::Double { double, exponent } => {
                double.to_bits().hash(state);
                exponent.hash(state);
            }
            RESP3::BigNumber(n) => n.hash(state),
            RESP3::BulkError(e) => e.as_ref().hash(state),
            RESP3::VerbatimString { encoding, data } => {
                encoding.hash(state);
                data.as_ref().hash(state);
            }
            RESP3::Map(map) => for_both!(map, map =>  map.iter().for_each(|(k, v)| {
                k.hash(state);
                v.hash(state);
            })),
            RESP3::Set(set) => set.iter().for_each(|f| f.hash(state)),
            RESP3::Push(frames) => frames.hash(state),
        }
    }
}

impl<B: AsRef<[u8]>, S: AsRef<str>> Eq for RESP3<B, S> {}

impl<B: AsRef<[u8]>, S: AsRef<str>> PartialEq for RESP3<B, S> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (RESP3::SimpleString(s1), RESP3::SimpleString(s2)) => s1.as_ref() == s2.as_ref(),
            (RESP3::SimpleError(e1), RESP3::SimpleError(e2)) => e1.as_ref() == e2.as_ref(),
            (RESP3::Integer(n1), RESP3::Integer(n2)) => n1 == n2,
            (RESP3::Bulk(b1), RESP3::Bulk(b2)) => b1.as_ref() == b2.as_ref(),
            (RESP3::Array(frames1), RESP3::Array(frames2)) => frames1 == frames2,
            (RESP3::Null, RESP3::Null) => true,
            (RESP3::Boolean(b1), RESP3::Boolean(b2)) => b1 == b2,
            (
                RESP3::Double {
                    double: d1,
                    exponent: e1,
                },
                RESP3::Double {
                    double: d2,
                    exponent: e2,
                },
            ) => d1 == d2 && e1 == e2,
            (RESP3::BigNumber(n1), RESP3::BigNumber(n2)) => n1 == n2,
            (RESP3::BulkError(e1), RESP3::BulkError(e2)) => e1.as_ref() == e2.as_ref(),
            (
                RESP3::VerbatimString {
                    encoding: e1,
                    data: d1,
                },
                RESP3::VerbatimString {
                    encoding: e2,
                    data: d2,
                },
            ) => e1 == e2 && d1.as_ref() == d2.as_ref(),
            (RESP3::Map(map1), RESP3::Map(map2)) => map1 == map2,
            (RESP3::Set(set1), RESP3::Set(set2)) => set1 == set2,
            (RESP3::Push(frames1), RESP3::Push(frames2)) => frames1 == frames2,
            _ => false,
        }
    }
}

#[cfg(test)]
mod frame_tests {
    // use crate::server::Handler;
    //
    // use super::*;

    // TODO:
    // #[tokio::test]
    // async fn test_resp3() {
    //     let frame = Frame::Array(vec![
    //         Frame::SimpleString("OK".to_string()),
    //         Frame::Integer(123),
    //         Frame::Bulk(Bytes::from("hello".as_bytes())),
    //         Frame::Null,
    //         Frame::Boolean(true),
    //         Frame::Double {
    //             double: 3.14,
    //             exponent: None,
    //         },
    //         Frame::BigNumber(BigInt::from(1234567890)),
    //         Frame::BulkError(Bytes::from("error".as_bytes())),
    //         Frame::VerbatimString {
    //             encoding: *b"txt",
    //             data: Bytes::from("hello".as_bytes()),
    //         },
    //         Frame::Map(Either::Right(
    //             vec![
    //                 (Frame::SimpleString("key1".to_string()), Frame::Integer(1)),
    //                 (Frame::SimpleString("key2".to_string()), Frame::Integer(2)),
    //             ]
    //             .into_iter()
    //             .collect(),
    //         )),
    //         Frame::Set(Either::Right(
    //             vec![
    //                 Frame::SimpleString("key1".to_string()),
    //                 Frame::SimpleString("key2".to_string()),
    //             ]
    //             .into_iter()
    //             .collect(),
    //         )),
    //         Frame::Push(vec![
    //             Frame::SimpleString("OK".to_string()),
    //             Frame::Integer(123),
    //         ]),
    //     ]);
    //
    //     let mut buf = BytesMut::new();
    //     frame.encode_buf(&mut buf);
    //
    //     let (handler, client) = Handler::new_fake();
    //
    //     let mut src = BytesMut::new();
    //     // let decoded = RESP3::decode_async(&mut cursor, &mut src)
    //     //     .await
    //     //     .unwrap()
    //     //     .unwrap();
    //
    //     // assert_eq!(frame, decoded);
    // }
}
