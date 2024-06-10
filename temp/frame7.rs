use crate::{
    util::{self, atof},
    Int,
};
use ahash::{AHashMap, AHashSet};
use bytes::{Buf, Bytes, BytesMut};
use mlua::prelude::*;
use num_bigint::BigInt;
use std::{
    convert::Infallible,
    fmt::Display,
    hash::Hash,
    io,
    iter::Iterator,
    ptr::{addr_of, slice_from_raw_parts, NonNull},
};
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio_util::{
    bytes::BufMut,
    codec::{Decoder, Encoder},
};
use tracing::instrument;

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
    Map(AHashMap<RESP3<B, S>, RESP3<B, S>>),
    // ~<number-of-elements>\r\n<element-1>...<element-n>
    // Left: 由用户保证传入的元素唯一；Right: 由Frame本身保证元素唯一
    Set(AHashSet<RESP3<B, S>>),

    // ><number-of-elements>\r\n<element-1>...<element-n>
    // 通常用于服务端主动向客户端推送消息
    Push(Vec<RESP3<B, S>>),
}

impl<B, S> RESP3<B, S>
where
    B: AsRef<[u8]>,
    S: AsRef<str>,
{
    #[inline]
    pub fn size(&self) -> usize {
        match self {
            RESP3::SimpleString(s) => s.as_ref().len() + 3, // 1 + s.len() + 2
            RESP3::SimpleError(e) => e.as_ref().len() + 3,  // 1 + e.len() + 2
            RESP3::Integer(n) => itoa::Buffer::new().format(*n).len() + 3, // 1 + n.len() + 2
            RESP3::Bulk(b) => {
                b.as_ref().len() + itoa::Buffer::new().format(b.as_ref().len()).len() + 5
            } //  1 + len.len() + 2 + len + 2
            RESP3::Null => 3,                               // 1 + 2
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
            RESP3::BulkError(e) => {
                e.as_ref().len() + itoa::Buffer::new().format(e.as_ref().len()).len() + 5
            } // 1 + len.len() + 2 + len + 2
            RESP3::VerbatimString { data, .. } => {
                data.as_ref().len() + itoa::Buffer::new().format(data.as_ref().len()).len() + 9
                // 1 + len.len() + 2 + 3 + 1 + data.len() + 2
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

    pub fn is_simple_string(&self) -> bool {
        matches!(self, RESP3::SimpleString(_))
    }

    pub fn is_simple_error(&self) -> bool {
        matches!(self, RESP3::SimpleError(_))
    }

    pub fn is_integer(&self) -> bool {
        matches!(self, RESP3::Integer(_))
    }

    pub fn is_bulk(&self) -> bool {
        matches!(self, RESP3::Bulk(_))
    }

    pub fn is_array(&self) -> bool {
        matches!(self, RESP3::Array(_))
    }

    pub fn is_null(&self) -> bool {
        matches!(self, RESP3::Null)
    }

    pub fn is_boolean(&self) -> bool {
        matches!(self, RESP3::Boolean(_))
    }

    pub fn is_double(&self) -> bool {
        matches!(self, RESP3::Double { .. })
    }

    pub fn is_big_number(&self) -> bool {
        matches!(self, RESP3::BigNumber(_))
    }

    pub fn is_bulk_error(&self) -> bool {
        matches!(self, RESP3::BulkError(_))
    }

    pub fn is_verbatim_string(&self) -> bool {
        matches!(self, RESP3::VerbatimString { .. })
    }

    pub fn is_map(&self) -> bool {
        matches!(self, RESP3::Map(_))
    }

    pub fn is_set(&self) -> bool {
        matches!(self, RESP3::Set(_))
    }

    pub fn is_push(&self) -> bool {
        matches!(self, RESP3::Push(_))
    }

    pub fn try_simple_string(&self) -> Option<&S> {
        match self {
            RESP3::SimpleString(s) => Some(s),
            _ => None,
        }
    }

    pub fn try_simple_error(&self) -> Option<&S> {
        match self {
            RESP3::SimpleError(e) => Some(e),
            _ => None,
        }
    }

    pub fn try_integer(&self) -> Option<Int> {
        match self {
            RESP3::Integer(n) => Some(*n),
            _ => None,
        }
    }

    pub fn try_bulk(&self) -> Option<&B> {
        match self {
            RESP3::Bulk(b) => Some(b),
            _ => None,
        }
    }

    pub fn try_array(&self) -> Option<&Vec<RESP3<B, S>>> {
        match self {
            RESP3::Array(frames) => Some(frames),
            _ => None,
        }
    }

    pub fn try_null(&self) -> Option<()> {
        match self {
            RESP3::Null => Some(()),
            _ => None,
        }
    }

    pub fn try_boolean(&self) -> Option<bool> {
        match self {
            RESP3::Boolean(b) => Some(*b),
            _ => None,
        }
    }

    pub fn try_double(&self) -> Option<(f64, Option<Int>)> {
        match self {
            RESP3::Double { double, exponent } => Some((*double, *exponent)),
            _ => None,
        }
    }

    pub fn try_big_number(&self) -> Option<&BigInt> {
        match self {
            RESP3::BigNumber(n) => Some(n),
            _ => None,
        }
    }

    pub fn try_bulk_error(&self) -> Option<&B> {
        match self {
            RESP3::BulkError(e) => Some(e),
            _ => None,
        }
    }

    pub fn try_verbatim_string(&self) -> Option<(&[u8; 3], &B)> {
        match self {
            RESP3::VerbatimString { encoding, data } => Some((encoding, data)),
            _ => None,
        }
    }

    #[allow(clippy::type_complexity)]
    pub fn try_map(&self) -> Option<&AHashMap<RESP3<B, S>, RESP3<B, S>>> {
        match self {
            RESP3::Map(map) => Some(map),
            _ => None,
        }
    }

    pub fn try_set(&self) -> Option<&AHashSet<RESP3<B, S>>> {
        match self {
            RESP3::Set(set) => Some(set),
            _ => None,
        }
    }

    pub fn try_push(&self) -> Option<&Vec<RESP3<B, S>>> {
        match self {
            RESP3::Push(frames) => Some(frames),
            _ => None,
        }
    }

    pub fn try_simple_string_mut(&mut self) -> Option<&mut S> {
        match self {
            RESP3::SimpleString(s) => Some(s),
            _ => None,
        }
    }

    pub fn try_simple_error_mut(&mut self) -> Option<&mut S> {
        match self {
            RESP3::SimpleError(e) => Some(e),
            _ => None,
        }
    }

    pub fn try_integer_mut(&mut self) -> Option<&mut Int> {
        match self {
            RESP3::Integer(n) => Some(n),
            _ => None,
        }
    }

    pub fn try_bulk_mut(&mut self) -> Option<&mut B> {
        match self {
            RESP3::Bulk(b) => Some(b),
            _ => None,
        }
    }

    pub fn try_array_mut(&mut self) -> Option<&mut Vec<RESP3<B, S>>> {
        match self {
            RESP3::Array(frames) => Some(frames),
            _ => None,
        }
    }

    pub fn try_boolean_mut(&mut self) -> Option<&mut bool> {
        match self {
            RESP3::Boolean(b) => Some(b),
            _ => None,
        }
    }

    pub fn try_double_mut(&mut self) -> Option<(&mut f64, &mut Option<Int>)> {
        match self {
            RESP3::Double { double, exponent } => Some((double, exponent)),
            _ => None,
        }
    }

    pub fn try_big_number_mut(&mut self) -> Option<&mut BigInt> {
        match self {
            RESP3::BigNumber(n) => Some(n),
            _ => None,
        }
    }

    pub fn try_bulk_error_mut(&mut self) -> Option<&mut B> {
        match self {
            RESP3::BulkError(e) => Some(e),
            _ => None,
        }
    }

    pub fn try_verbatim_string_mut(&mut self) -> Option<(&mut [u8; 3], &mut B)> {
        match self {
            RESP3::VerbatimString { encoding, data } => Some((encoding, data)),
            _ => None,
        }
    }

    #[allow(clippy::type_complexity)]
    pub fn try_map_mut(&mut self) -> Option<&mut AHashMap<RESP3<B, S>, RESP3<B, S>>> {
        match self {
            RESP3::Map(map) => Some(map),
            _ => None,
        }
    }

    pub fn try_set_mut(&mut self) -> Option<&mut AHashSet<RESP3<B, S>>> {
        match self {
            RESP3::Set(set) => Some(set),
            _ => None,
        }
    }

    pub fn try_push_mut(&mut self) -> Option<&mut Vec<RESP3<B, S>>> {
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
}

// 解码
impl RESP3<BytesMut, String> {
    #[allow(clippy::multiple_bound_locations)]
    #[inline]
    #[instrument(level = "trace", skip(io_read), err)]
    pub async fn decode_async<R: AsyncRead + Unpin + Send>(
        io_read: &mut R,
        src: &mut BytesMut,
    ) -> io::Result<Option<RESP3>> {
        if src.is_empty() && io_read.read_buf(src).await? == 0 {
            return Ok(None);
        }

        debug_assert!(!src.is_empty());

        let res = match src.get_u8() {
            b'+' => RESP3::SimpleString(RESP3::decode_string_async(io_read, src).await?),
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
                    let frame = Box::pin(RESP3::decode_async(io_read, src)).await?.ok_or(
                        io::Error::new(io::ErrorKind::UnexpectedEof, "incomplete frame"),
                    )?;
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

                let mut map = AHashMap::with_capacity(len);
                for _ in 0..len {
                    let k = Box::pin(RESP3::decode_async(io_read, src)).await?.ok_or(
                        io::Error::new(io::ErrorKind::UnexpectedEof, "incomplete frame"),
                    )?;
                    let v = Box::pin(RESP3::decode_async(io_read, src)).await?.ok_or(
                        io::Error::new(io::ErrorKind::UnexpectedEof, "incomplete frame"),
                    )?;
                    map.insert(k, v);
                }

                // map的key由客户端保证唯一
                RESP3::Map(map)
            }
            b'~' => {
                let len = RESP3::decode_decimal_async(io_read, src).await? as usize;

                let mut set = AHashSet::with_capacity(len);
                for _ in 0..len {
                    let frame = Box::pin(RESP3::decode_async(io_read, src)).await?.ok_or(
                        io::Error::new(io::ErrorKind::UnexpectedEof, "incomplete frame"),
                    )?;
                    set.insert(frame);
                }

                // set的元素由客户端保证唯一
                RESP3::Set(set)
            }
            b'>' => {
                let len = RESP3::decode_decimal_async(io_read, src).await? as usize;

                let mut frames = Vec::with_capacity(len);
                for _ in 0..len {
                    let frame = Box::pin(RESP3::decode_async(io_read, src)).await?.ok_or(
                        io::Error::new(io::ErrorKind::UnexpectedEof, "incomplete frame"),
                    )?;
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

    #[inline]
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

    #[inline]
    async fn decode_decimal_async<R: AsyncRead + Unpin + Send>(
        io_read: &mut R,
        src: &mut BytesMut,
    ) -> io::Result<Int> {
        let line = RESP3::decode_line_async(io_read, src).await?;
        let decimal = util::atoi(&line)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid integer"))?;
        Ok(decimal)
    }

    #[inline]
    async fn decode_length_async<R: AsyncRead + Unpin + Send>(
        io_read: &mut R,
        src: &mut BytesMut,
    ) -> io::Result<usize> {
        let line = RESP3::decode_line_async(io_read, src).await?;
        let len = util::atoi(&line)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid length"))?;
        Ok(len)
    }

    #[inline]
    async fn decode_string_async<R: AsyncRead + Unpin + Send>(
        io_read: &mut R,
        src: &mut BytesMut,
    ) -> io::Result<String> {
        let line = RESP3::decode_line_async(io_read, src).await?;
        let string = String::from_utf8(line.to_vec())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid string"))?;
        Ok(string)
    }

    #[inline]
    fn decode_line(consume: &mut BytesMut, src: &mut BytesMut) -> io::Result<BytesMut> {
        if !consume.is_empty() {
            // 如果再consume中找到完整行，则返回
            if let Some(i) = memchr::memchr(b'\n', consume) {
                if i > 0 && consume[i - 1] == b'\r' {
                    let line = consume.split_to(i - 1);
                    consume.advance(2);

                    return Ok(line);
                }
            }

            // 尝试在src中找到完整行
            if let Some(i) = memchr::memchr(b'\n', src) {
                if i > 0 && src[i - 1] == b'\r' {
                    let mut line = consume.split();
                    line.unsplit(src.split_to(i - 1));

                    src.advance(2);

                    return Ok(line);
                }
            }

            // 都没有找到，则frame不完整
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "incomplete frame",
            ));
        }

        if let Some(i) = memchr::memchr(b'\n', src) {
            if i > 0 && src[i - 1] == b'\r' {
                let line = src.split_to(i - 1);
                src.advance(2);

                return Ok(line);
            }
        }

        Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "incomplete frame",
        ))
    }

    #[inline]
    fn decode_decimal(consume: &mut BytesMut, src: &mut BytesMut) -> io::Result<Int> {
        let line = RESP3::decode_line(consume, src)?;
        let decimal = util::atoi(&line)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid integer"))?;
        Ok(decimal)
    }

    #[inline]
    fn decode_length(consume: &mut BytesMut, src: &mut BytesMut) -> io::Result<usize> {
        let line = RESP3::decode_line(consume, src)?;
        let len = util::atoi(&line)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid length"))?;
        Ok(len)
    }

    #[inline]
    fn decode_string(consume: &mut BytesMut, src: &mut BytesMut) -> io::Result<String> {
        let line = RESP3::decode_line(consume, src)?;
        let string = String::from_utf8(line.to_vec())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid string"))?;
        Ok(string)
    }
}

#[derive(Debug, Clone, Default)]
pub struct RESP3Encoder;

impl Encoder<RESP3> for RESP3Encoder {
    type Error = tokio::io::Error;

    fn encode(&mut self, item: RESP3, dst: &mut BytesMut) -> Result<(), Self::Error> {
        item.encode_buf(dst);

        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct RESP3Decoder {
    consume: BytesMut,
}

impl Default for RESP3Decoder {
    fn default() -> Self {
        Self {
            consume: BytesMut::new(),
        }
    }
}

impl Decoder for RESP3Decoder {
    type Error = tokio::io::Error;
    type Item = RESP3;

    // 如果src中的数据不完整，会引发io::ErrorKind::UnexpectedEof错误
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.is_empty() {
            return Ok(None);
        }

        let origin = src.as_ptr();

        fn _decode(
            decoder: &mut RESP3Decoder,
            src: &mut BytesMut,
        ) -> Result<Option<<RESP3Decoder as Decoder>::Item>, <RESP3Decoder as Decoder>::Error>
        {
            let consume = &mut decoder.consume;

            let prefix = if consume.has_remaining() {
                consume.get_u8()
            } else {
                src.get_u8()
            };

            let res = match prefix {
                b'+' => RESP3::SimpleString(RESP3::decode_string(consume, src)?),
                b'-' => RESP3::SimpleError(RESP3::decode_string(consume, src)?),
                b':' => RESP3::Integer(RESP3::decode_decimal(consume, src)?),
                b'$' => {
                    let len = RESP3::decode_length(consume, src)?;

                    if consume.has_remaining() {
                        if consume.remaining() < len + 2 {
                            let need = len + 2 - consume.remaining();

                            if src.remaining() < need {
                                return Err(io::Error::new(
                                    io::ErrorKind::UnexpectedEof,
                                    "incomplete frame",
                                ));
                            }

                            let mut res = consume.split();
                            res.unsplit(src.split_to(need - 2));
                            src.advance(2);

                            return Ok(Some(RESP3::Bulk(res.freeze())));
                        }

                        let res = consume.split_to(len);
                        consume.advance(2);

                        return Ok(Some(RESP3::Bulk(res.freeze())));
                    }

                    if src.remaining() < len + 2 {
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
                    let len = RESP3::decode_decimal(consume, src)? as usize;

                    let mut frames = Vec::with_capacity(len);
                    for _ in 0..len {
                        let frame = _decode(decoder, src)?.ok_or(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            "incomplete frame",
                        ))?;
                        frames.push(frame);
                    }

                    RESP3::Array(frames)
                }
                b'_' => {
                    if consume.has_remaining() {
                        if consume.remaining() >= 2 {
                            consume.advance(2);

                            return Ok(Some(RESP3::Null));
                        }

                        if consume.remaining() == 1 && src.remaining() > 1 {
                            consume.advance(1);
                            src.advance(1);

                            return Ok(Some(RESP3::Null));
                        }

                        return Err(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            "incomplete frame",
                        ));
                    }

                    if src.remaining() < 2 {
                        return Err(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            "incomplete frame",
                        ));
                    }
                    src.advance(2);
                    RESP3::Null
                }
                b'#' => {
                    if consume.has_remaining() {
                        if consume.remaining() >= 3 {
                            let b = match consume.get_u8() {
                                b't' => true,
                                b'f' => false,
                                _ => {
                                    return Err(io::Error::new(
                                        io::ErrorKind::InvalidData,
                                        "invalid boolean value",
                                    ));
                                }
                            };

                            consume.advance(2);

                            return Ok(Some(RESP3::Boolean(b)));
                        }

                        if consume.remaining() == 2 && src.remaining() > 1 {
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

                            consume.advance(1);
                            src.advance(2);

                            return Ok(Some(RESP3::Boolean(b)));
                        }

                        return Err(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            "incomplete frame",
                        ));
                    }

                    if src.remaining() < 3 {
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
                    let line = RESP3::decode_line(consume, src)?;

                    let mut exp_pos = line.len();
                    if let Some(i) = memchr::memchr2(b'e', b'E', &line) {
                        exp_pos = i;
                    }

                    let double = atof(&line[..exp_pos]).map_err(|_| {
                        io::Error::new(io::ErrorKind::InvalidData, "invalid double")
                    })?;

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
                    let line = RESP3::decode_line(consume, src)?;
                    let n = BigInt::parse_bytes(&line, 10).ok_or(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "invalid big number",
                    ))?;
                    RESP3::BigNumber(n)
                }
                b'!' => {
                    let len = RESP3::decode_length(consume, src)?;

                    if src.remaining() < len + 2 {
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
                    let len = RESP3::decode_length(consume, src)?;

                    if src.remaining() < len + 2 {
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
                    let len = RESP3::decode_decimal(consume, src)? as usize;

                    let mut map = AHashMap::with_capacity(len);
                    for _ in 0..len {
                        let k = _decode(decoder, src)?.ok_or(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            "incomplete frame",
                        ))?;
                        let v = _decode(decoder, src)?.ok_or(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            "incomplete frame",
                        ))?;
                        map.insert(k, v);
                    }

                    // map的key由客户端保证唯一
                    RESP3::Map(map)
                }
                b'~' => {
                    let len = RESP3::decode_decimal(consume, src)? as usize;

                    let mut set = AHashSet::with_capacity(len);
                    for _ in 0..len {
                        let frame = _decode(decoder, src)?.ok_or(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            "incomplete frame",
                        ))?;
                        set.insert(frame);
                    }

                    // set的元素由客户端保证唯一
                    RESP3::Set(set)
                }
                b'>' => {
                    let len = RESP3::decode_decimal(consume, src)? as usize;

                    let mut frames = Vec::with_capacity(len);
                    for _ in 0..len {
                        let frame = _decode(decoder, src)?.ok_or(io::Error::new(
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

        let res = _decode(self, src);
        if res.is_err() {
            let consume = unsafe {
                slice_from_raw_parts(origin, src.as_ptr() as usize - origin as usize)
                    .as_ref()
                    .unwrap()
            };
            // 存储被消耗的数据
            self.consume.put_slice(consume);
        }

        res
    }
}

impl From<RESP3<Bytes, &'static str>> for RESP3<Bytes, String> {
    fn from(value: RESP3<Bytes, &'static str>) -> Self {
        match value {
            RESP3::SimpleString(s) => RESP3::SimpleString(s.to_string()),
            RESP3::SimpleError(e) => RESP3::SimpleError(e.to_string()),
            RESP3::Integer(n) => RESP3::Integer(n),
            RESP3::Bulk(b) => RESP3::Bulk(b),
            RESP3::Array(frames) => RESP3::Array(frames.into_iter().map(Into::into).collect()),
            RESP3::Null => RESP3::Null,
            RESP3::Boolean(b) => RESP3::Boolean(b),
            RESP3::Double { double, exponent } => RESP3::Double { double, exponent },
            RESP3::BigNumber(n) => RESP3::BigNumber(n),
            RESP3::BulkError(e) => RESP3::BulkError(e),
            RESP3::VerbatimString { encoding, data } => RESP3::VerbatimString { encoding, data },
            RESP3::Map(map) => {
                RESP3::Map(map.into_iter().map(|(k, v)| (k.into(), v.into())).collect())
            }
            RESP3::Set(set) => RESP3::Set(set.into_iter().map(Into::into).collect()),
            RESP3::Push(frames) => RESP3::Push(frames.into_iter().map(Into::into).collect()),
        }
    }
}

impl<B, S> Hash for RESP3<B, S>
where
    B: AsRef<[u8]>,
    S: AsRef<str>,
{
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
            RESP3::Map(map) => map.iter().for_each(|(k, v)| {
                k.hash(state);
                v.hash(state);
            }),
            RESP3::Set(set) => set.iter().for_each(|f| f.hash(state)),
            RESP3::Push(frames) => frames.hash(state),
        }
    }
}

impl<B, S> Eq for RESP3<B, S>
where
    B: AsRef<[u8]>,
    S: AsRef<str>,
{
}

impl<B, S> PartialEq for RESP3<B, S>
where
    B: AsRef<[u8]>,
    S: AsRef<str>,
{
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

impl<S: AsRef<str>> mlua::IntoLua<'_> for RESP3<Bytes, S> {
    fn into_lua(self, lua: &'_ Lua) -> LuaResult<LuaValue<'_>> {
        match self {
            // SimpleString -> Lua Table { ok: Lua String }
            RESP3::SimpleString(s) => {
                let table = lua.create_table()?;
                table.set("ok", s.as_ref())?;
                Ok(LuaValue::Table(table))
            }
            // SimpleError -> Lua Table { err: Lua String }
            RESP3::SimpleError(e) => {
                let table = lua.create_table()?;
                table.set("err", e.as_ref())?;
                Ok(LuaValue::Table(table))
            }
            // Integer -> Lua Integer
            RESP3::Integer(n) => n.into_lua(lua),
            // Bulk -> Lua String
            RESP3::Bulk(b) => std::str::from_utf8(&b)
                .map_err(|_| mlua::Error::FromLuaConversionError {
                    from: "Bulk",
                    to: "String",
                    message: Some("invalid utf-8 string".to_string()),
                })?
                .into_lua(lua),
            // Array -> Lua Table(Array)
            RESP3::Array(frames) => frames.into_lua(lua),
            // Null -> Lua Nil
            RESP3::Null => Ok(LuaValue::Nil),
            // Boolean -> Lua Boolean
            RESP3::Boolean(b) => b.into_lua(lua),
            // Lua table with a single double field containing a Lua number representing the double value.
            // Double -> Lua Table { double: Lua Number }
            RESP3::Double {
                mut double,
                exponent,
            } => {
                if let Some(exp) = exponent {
                    double *= 10_f64.powi(exp as i32);
                }

                let table = lua.create_table()?;
                table.set("double", double)?;
                Ok(LuaValue::Table(table))
            }
            // Lua table with a single big_number field containing a Lua string representing the big number value.
            // BigNumber -> Lua Table { big_number: Lua String }
            RESP3::BigNumber(n) => {
                let n = n.to_str_radix(10);

                let table = lua.create_table()?;
                table.set("big_number", n)?;
                Ok(LuaValue::Table(table))
            }
            // BulkError -> Lua String
            RESP3::BulkError(e) => e.into_lua(lua),
            // Lua table with a single verbatim_string field containing a Lua table with two fields, string and format, representing the verbatim string and its format, respectively.
            // VerbatimString -> Lua Table { verbatim_string: Lua Table { string: Lua String, format: Lua String } }
            RESP3::VerbatimString { encoding, data } => {
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
            RESP3::Map(map) => {
                let map_table = lua.create_table()?;

                for (k, v) in map {
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
            RESP3::Set(set) => {
                let set_table = lua.create_table()?;

                for f in set {
                    let f = f.into_lua(lua)?;
                    set_table.set(f, true)?;
                }

                let table = lua.create_table()?;
                table.set("set", set_table)?;
                Ok(mlua::Value::Table(table))
            }
            // Lua table with a single push field containing a Lua table representing the frames of the push message.
            // Push -> Lua Table { push: Lua Table }
            RESP3::Push(push) => {
                let push_table = push.into_lua(lua)?;

                let table = lua.create_table()?;
                table.set("push", push_table)?;
                Ok(mlua::Value::Table(table))
            }
        }
    }
}

impl FromLua<'_> for RESP3 {
    fn from_lua(value: LuaValue<'_>, _lua: &'_ Lua) -> LuaResult<Self> {
        match value {
            // Lua String -> SimpleString
            LuaValue::String(s) => Ok(RESP3::SimpleString(s.to_str()?.to_string())),
            // Lua String -> SimpleError
            LuaValue::Integer(n) => Ok(RESP3::Integer(n)),
            // Lua Number -> Double
            LuaValue::Number(n) => Ok(RESP3::Double {
                double: n,
                exponent: None,
            }),
            // Lua Nil -> Null
            LuaValue::Nil => Ok(RESP3::Null),
            // Lua Boolean -> Boolean
            LuaValue::Boolean(b) => Ok(RESP3::Boolean(b)),
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
                    return Ok(RESP3::SimpleString(state.to_str()?.to_string()));
                }

                let err = table.raw_get("err")?;

                if let LuaValue::String(e) = err {
                    return Ok(RESP3::SimpleError(e.to_str()?.to_string()));
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

                    return Ok(RESP3::VerbatimString {
                        encoding,
                        data: data.freeze(),
                    });
                }

                let map = table.raw_get("map")?;

                if let LuaValue::Table(map) = map {
                    let mut map_table = AHashMap::new();
                    for pair in map.pairs::<LuaValue, LuaValue>() {
                        let (k, v) = pair?;
                        let k = RESP3::from_lua(k, _lua)?;
                        let v = RESP3::from_lua(v, _lua)?;
                        map_table.insert(k, v);
                    }

                    return Ok(RESP3::Map(map_table));
                }

                let set = table.raw_get("set")?;

                if let LuaValue::Table(set) = set {
                    let mut set_table = AHashSet::new();
                    for pair in set.pairs::<LuaValue, bool>() {
                        let (f, _) = pair?;
                        let f = RESP3::from_lua(f, _lua)?;
                        set_table.insert(f);
                    }

                    return Ok(RESP3::Set(set_table));
                }

                let push = table.raw_get("push")?;

                if let LuaValue::Table(push) = push {
                    let mut push_table = Vec::with_capacity(push.raw_len());
                    for pair in push.pairs::<usize, LuaValue>() {
                        let ele = pair?.1;
                        push_table.push(RESP3::from_lua(ele, _lua)?);
                    }

                    return Ok(RESP3::Push(push_table));
                }

                let mut array = Vec::with_capacity(table.raw_len());
                for pair in table.pairs::<usize, mlua::Value>() {
                    let ele = pair?.1;
                    array.push(RESP3::from_lua(ele, _lua)?);
                }

                Ok(RESP3::Array(array))
            }
            _ => Err(mlua::Error::FromLuaConversionError {
                from: value.type_name(),
                to: "RESP3",
                message: Some("invalid value type".to_string()),
            }),
        }
    }
}

#[cfg(test)]
mod frame_tests {
    use super::*;

    #[test]
    fn decode() {
        let mut decoder = RESP3Decoder::default();

        let mut src = BytesMut::from("*2\r\n");
        let src_clone = src.clone();

        decoder.decode(&mut src).unwrap_err();

        assert_eq!(src, src_clone);
    }
}
