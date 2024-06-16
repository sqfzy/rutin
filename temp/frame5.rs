#![allow(dead_code)]
use crate::{cmd::CmdError, server::ServerError, util, Int};
use bytes::{Buf, Bytes, BytesMut};
use mlua::{prelude, FromLuaMulti, IntoLua, Value};
use snafu::Snafu;
use std::{borrow::Cow, io::Cursor, iter::Iterator};
use tokio::io::AsyncReadExt;
use tokio_util::bytes::BufMut;

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
#[derive(Clone, Debug, Default, PartialEq)]
pub enum RESP3 {
    Simple(Cow<'static, str>), // +<str>\r\n
    Error(Cow<'static, str>),  // -<err>\r\n
    Integer(Int),              // :<num>\r\n
    Bulk(Bytes),               // $<len>\r\n<bytes>\r\n
    #[default]
    Null,        // $-1\r\n
    Array(Vec<RESP3>),         // *<len>\r\n<Frame>...
}

impl RESP3 {
    #[inline]
    pub fn size(&self) -> usize {
        match self {
            RESP3::Simple(s) => s.len() + 3,
            RESP3::Error(e) => e.len() + 3,
            RESP3::Integer(n) => itoa::Buffer::new().format(*n).as_bytes().len() + 3,
            RESP3::Bulk(b) => {
                let len = b.len();
                len + itoa::Buffer::new().format(len).as_bytes().len() + 5
            }
            RESP3::Null => 5,
            RESP3::Array(frames) => {
                itoa::Buffer::new().format(frames.len()).as_bytes().len()
                    + 3
                    + frames.iter().map(|f| f.size()).sum::<usize>()
            }
        }
    }

    #[inline]
    pub fn array_len(&self) -> Result<usize, FrameError> {
        match self {
            RESP3::Array(frames) => Ok(frames.len()),
            _ => Err(FrameError::NotArray),
        }
    }

    #[inline]
    pub fn to_raw_in_buf(&self, buf: &mut BytesMut) {
        match self {
            RESP3::Simple(s) => {
                buf.put_u8(b'+');
                buf.extend(s.as_bytes());
                buf.extend(b"\r\n");
            }
            RESP3::Error(e) => {
                buf.put_u8(b'-');
                buf.extend(e.as_bytes());
                buf.extend(b"\r\n");
            }
            RESP3::Integer(n) => {
                buf.put_u8(b':');
                buf.extend(itoa::Buffer::new().format(*n).as_bytes());
                buf.extend(b"\r\n");
            }
            RESP3::Bulk(b) => {
                buf.put_u8(b'$');
                buf.extend(itoa::Buffer::new().format(b.len()).as_bytes());
                buf.extend(b"\r\n");
                buf.extend(b);
                buf.extend(b"\r\n");
            }
            RESP3::Null => {
                buf.extend(b"$-1\r\n");
            }
            RESP3::Array(frames) => {
                buf.put_u8(b'*');
                buf.extend(itoa::Buffer::new().format(frames.len()).as_bytes());
                buf.extend(b"\r\n");
                for frame in frames {
                    frame.to_raw_in_buf(buf);
                }
            }
        }
    }

    #[inline]
    pub fn into_raw_in_buf(self, buf: &mut BytesMut) {
        match self {
            RESP3::Simple(s) => {
                buf.put_u8(b'+');
                buf.extend(s.as_bytes());
                buf.extend(b"\r\n");
            }
            RESP3::Error(e) => {
                buf.put_u8(b'-');
                buf.extend(e.as_bytes());
                buf.extend(b"\r\n");
            }
            RESP3::Integer(n) => {
                buf.put_u8(b':');
                buf.extend(itoa::Buffer::new().format(n).as_bytes());
                buf.extend(b"\r\n");
            }
            RESP3::Bulk(b) => {
                buf.put_u8(b'$');
                buf.extend(itoa::Buffer::new().format(b.len()).as_bytes());
                buf.extend(b"\r\n");
                buf.extend(b);
                buf.extend(b"\r\n");
            }
            RESP3::Null => {
                buf.extend(b"$-1\r\n");
            }
            RESP3::Array(frames) => {
                buf.put_u8(b'*');
                buf.extend(itoa::Buffer::new().format(frames.len()).as_bytes());
                buf.extend(b"\r\n");
                for frame in frames {
                    frame.to_raw_in_buf(buf);
                }
            }
        }
    }

    #[inline]
    pub fn to_raw(&self) -> Bytes {
        let mut raw = BytesMut::with_capacity(self.size());
        match self {
            RESP3::Simple(s) => {
                raw.put_u8(b'+');
                raw.put_slice(s.as_bytes());
                raw.put_slice(b"\r\n");
            }
            RESP3::Error(e) => {
                raw.put_u8(b'-');
                raw.put_slice(e.as_bytes());
                raw.put_slice(b"\r\n");
            }
            RESP3::Integer(n) => {
                raw.put_u8(b':');
                raw.put_slice(itoa::Buffer::new().format(*n).as_bytes());
                raw.put_slice(b"\r\n");
            }
            RESP3::Bulk(b) => {
                raw.put_u8(b'$');
                raw.put_slice(itoa::Buffer::new().format(b.len()).as_bytes());
                raw.put_slice(b"\r\n");
                raw.put_slice(b);
                raw.put_slice(b"\r\n");
            }
            RESP3::Null => {
                raw.put_slice(b"$-1\r\n");
            }
            RESP3::Array(frames) => {
                raw.put_u8(b'*');
                raw.put_slice(itoa::Buffer::new().format(frames.len()).as_bytes());
                raw.put_slice(b"\r\n");
                for frame in frames {
                    raw.extend(frame.to_raw());
                }
            }
        }

        raw.freeze()
    }

    // 解析一个reader中的数据为一个frame，frame应当是完整且格式正确的
    #[inline]
    #[async_recursion::async_recursion]
    pub async fn parse_frame<R>(
        reader: &mut R,
        buf: &mut BytesMut,
    ) -> Result<Option<RESP3>, FrameError>
    where
        R: AsyncReadExt + Unpin + Send,
    {
        if buf.is_empty()
            && reader
                .read_buf(buf)
                .await
                .map_err(|e| FrameError::Other { msg: e.to_string() })?
                == 0
        {
            return Ok(None);
        }

        debug_assert!(!buf.is_empty());

        let res = match buf.get_u8() {
            b'*' => {
                let len = RESP3::parse_decimal(reader, buf).await? as usize;

                let mut frames = Vec::with_capacity(len);
                for _ in 0..len {
                    let frame = RESP3::parse_frame(reader, buf)
                        .await?
                        .ok_or(FrameError::InCompleteFrame)?;
                    frames.push(frame);
                }

                RESP3::Array(frames)
            }
            b'+' => {
                let line = RESP3::parse_line(reader, buf).await?.to_vec();
                RESP3::new_simple_owned(String::from_utf8(line).map_err(|_| {
                    // 不是合法的utf8字符串
                    FrameError::InvalidFormat {
                        msg: "Invalid simple frame format, not utf8".to_string(),
                    }
                })?)
            }
            b'-' => {
                let line = RESP3::parse_line(reader, buf).await?.to_vec();
                RESP3::new_error_owned(String::from_utf8(line).map_err(|_| {
                    // 不是合法的utf8字符串
                    FrameError::InvalidFormat {
                        msg: "Invalid error frame format, not utf8".to_string(),
                    }
                })?)
            }
            b':' => RESP3::Integer(RESP3::parse_decimal(reader, buf).await?),
            b'$' => {
                let len = RESP3::parse_decimal(reader, buf).await?;

                if len == -1 {
                    let res = RESP3::Null;

                    return Ok(Some(res));
                }

                let len: usize = if let Ok(len) = len.try_into() {
                    len
                } else {
                    // 读取的len为负数，说明frame格式错误
                    return Err(FrameError::InvalidFormat {
                        msg: "Invalid bulk frame format, len is negative".to_string(),
                    });
                };

                if buf.remaining() < len + 2 {
                    reader
                        .read_buf(buf)
                        .await
                        .map_err(|_| FrameError::InCompleteFrame)?;
                }

                let res = buf.split_to(len);
                buf.advance(2);

                RESP3::new_bulk_owned(res.freeze())
            }
            prefix => {
                return Err(FrameError::InvalidFormat {
                    msg: format!("Invalid frame prefix: {}", prefix),
                });
            }
        };

        Ok(Some(res))
    }

    pub fn from_raw(raw: &mut BytesMut) -> Result<RESP3, FrameError> {
        let res = match raw.get_u8() {
            b'*' => {
                let len = RESP3::parse_decimal_blocking(raw)? as usize;

                let mut frames = Vec::with_capacity(len);
                for _ in 0..len {
                    let frame = RESP3::from_raw(raw)?;
                    frames.push(frame);
                }

                RESP3::Array(frames)
            }
            b'+' => {
                let line = RESP3::parse_line_blocking(raw)?.to_vec();
                RESP3::new_simple_owned(String::from_utf8(line).map_err(|_| {
                    // 不是合法的utf8字符串
                    FrameError::InvalidFormat {
                        msg: "Invalid simple frame format, not utf8".to_string(),
                    }
                })?)
            }
            b'-' => {
                let line = RESP3::parse_line_blocking(raw)?.to_vec();
                RESP3::new_error_owned(String::from_utf8(line).map_err(|_| {
                    // 不是合法的utf8字符串
                    FrameError::InvalidFormat {
                        msg: "Invalid error frame format, not utf8".to_string(),
                    }
                })?)
            }
            b':' => RESP3::Integer(RESP3::parse_decimal_blocking(raw)?),
            b'$' => {
                let len = RESP3::parse_decimal_blocking(raw)?;

                if len == -1 {
                    let res = RESP3::Null;

                    return Ok(res);
                }

                let len: usize = if let Ok(len) = len.try_into() {
                    len
                } else {
                    // 读取的len为负数，说明frame格式错误
                    return Err(FrameError::InvalidFormat {
                        msg: "Invalid bulk frame format, len is negative".to_string(),
                    });
                };

                if raw.remaining() < len + 2 {
                    return Err(FrameError::InCompleteFrame);
                }

                let res = raw.split_to(len);
                raw.advance(2);

                RESP3::new_bulk_owned(res.freeze())
            }
            prefix => {
                return Err(FrameError::InvalidFormat {
                    msg: format!("Invalid frame prefix: {}", prefix),
                });
            }
        };

        Ok(res)
    }

    #[inline]
    async fn parse_line<R: AsyncReadExt + Unpin>(
        reader: &mut R,
        buf: &mut BytesMut,
    ) -> Result<BytesMut, FrameError> {
        loop {
            if let Some(i) = buf.iter().position(|&b| b == b'\n') {
                if i > 0 && buf[i - 1] == b'\r' {
                    let line = buf.split_to(i - 1);
                    buf.advance(2);

                    return Ok(line);
                }
            }
            if reader
                .read_buf(buf)
                .await
                .map_err(|_| FrameError::InvalidFormat {
                    msg: "Invalid line format".to_string(),
                })?
                == 0
            {
                return Err(FrameError::InCompleteFrame);
            }
        }
    }

    #[inline]
    fn parse_line_blocking(buf: &mut BytesMut) -> Result<BytesMut, FrameError> {
        loop {
            if let Some(i) = buf.iter().position(|&b| b == b'\n') {
                if i > 0 && buf[i - 1] == b'\r' {
                    let line = buf.split_to(i - 1);
                    buf.advance(2);

                    return Ok(line);
                }
            }
        }
    }

    #[inline]
    async fn parse_decimal<R: AsyncReadExt + Unpin>(
        reader: &mut R,
        buf: &mut BytesMut,
    ) -> Result<Int, FrameError> {
        let line = RESP3::parse_line(reader, buf).await?;
        util::atoi(line.as_ref()).map_err(|_| FrameError::InvalidFormat {
            msg: "Invalid integer format".to_string(),
        })
    }

    #[inline]
    fn parse_decimal_blocking(buf: &mut BytesMut) -> Result<Int, FrameError> {
        let line = RESP3::parse_line_blocking(buf)?;
        util::atoi(line.as_ref()).map_err(|_| FrameError::InvalidFormat {
            msg: "Invalid integer format".to_string(),
        })
    }
}

/// Simple变体的new and get方法
impl RESP3 {
    #[inline]
    pub fn new_simple_owned(str: String) -> Self {
        RESP3::Simple(Cow::Owned(str))
    }

    #[inline]
    pub fn new_simple_borrowed(str: &'static str) -> Self {
        RESP3::Simple(Cow::Borrowed(str))
    }

    // 获取simple的引用
    #[inline]
    pub fn on_simple(&self) -> Result<&str, FrameError> {
        match self {
            RESP3::Simple(s) => Ok(s),
            _ => Err(FrameError::NotSimple),
        }
    }

    // 获取simple的可变引用
    #[inline]
    pub fn on_simple_mut(&mut self) -> Result<&mut String, FrameError> {
        match self {
            RESP3::Simple(s) => Ok(s.to_mut()),

            _ => Err(FrameError::NotSimple),
        }
    }

    #[inline]
    pub fn into_simple(self) -> Result<String, FrameError> {
        match self {
            RESP3::Simple(s) => Ok(s.into_owned()),
            _ => Err(FrameError::NotSimple),
        }
    }
}

/// Error变体的new and get方法
impl RESP3 {
    #[inline]
    pub fn new_error_owned(str: String) -> Self {
        RESP3::Error(Cow::Owned(str))
    }

    #[inline]
    pub fn new_error_borrowed(str: &'static str) -> Self {
        RESP3::Error(Cow::Borrowed(str))
    }

    #[inline]
    pub fn on_error(&self) -> Result<&str, FrameError> {
        match self {
            RESP3::Error(e) => Ok(e),
            _ => Err(FrameError::NotError),
        }
    }

    #[inline]
    pub fn on_error_mut(&mut self) -> Result<&mut String, FrameError> {
        match self {
            RESP3::Error(e) => Ok(e.to_mut()),
            _ => Err(FrameError::NotError),
        }
    }

    #[inline]
    pub fn into_error(self) -> Result<String, FrameError> {
        match self {
            RESP3::Error(e) => Ok(e.into_owned()),
            _ => Err(FrameError::NotError),
        }
    }
}

impl RESP3 {
    #[inline]
    pub fn new_integer(num: Int) -> Self {
        RESP3::Integer(num)
    }

    #[inline]
    pub fn on_integer(&self) -> Result<Int, FrameError> {
        match self {
            RESP3::Integer(n) => Ok(*n),
            _ => Err(FrameError::NotInteger),
        }
    }
}

impl RESP3 {
    #[inline]
    pub fn new_null() -> Self {
        RESP3::Null
    }
}

/// Bulk变体的new and get方法
impl RESP3 {
    #[inline]
    pub fn new_bulk_owned(bytes: Bytes) -> Self {
        RESP3::Bulk(bytes)
    }

    #[inline]
    pub fn new_bulk_by_copying(bytes: &[u8]) -> Self {
        RESP3::Bulk(Bytes::copy_from_slice(bytes))
    }

    #[inline]
    pub fn new_bulk_from_static(bytes: &'static [u8]) -> Self {
        RESP3::Bulk(Bytes::from_static(bytes))
    }

    #[inline]
    pub fn on_bulk(&self) -> Result<&Bytes, FrameError> {
        match self {
            RESP3::Bulk(b) => Ok(b),
            _ => Err(FrameError::NotBulk),
        }
    }

    #[inline]
    pub fn to_bulk(&self) -> Result<Bytes, FrameError> {
        match self {
            RESP3::Bulk(b) => Ok(b.clone()),
            _ => Err(FrameError::NotBulk),
        }
    }
}

/// Array变体的new and get方法
impl RESP3 {
    #[inline]
    pub fn new_array(frames: Vec<RESP3>) -> Self {
        RESP3::Array(frames)
    }

    pub fn new_bulks(bytes: &[Bytes]) -> RESP3 {
        RESP3::Array(
            bytes
                .iter()
                .map(|b| RESP3::new_bulk_owned(b.clone()))
                .collect(),
        )
    }

    pub fn new_bulks_by_copying(bytes: &[&[u8]]) -> RESP3 {
        RESP3::Array(
            bytes
                .iter()
                .map(|b| RESP3::new_bulk_by_copying(b))
                .collect(),
        )
    }

    pub fn new_bulks_from_static(bytes: &'static [&'static [u8]]) -> RESP3 {
        RESP3::Array(
            bytes
                .iter()
                .map(|b| RESP3::new_bulk_from_static(b))
                .collect(),
        )
    }

    pub fn on_array(&self) -> Result<&Vec<RESP3>, FrameError> {
        match self {
            RESP3::Array(frames) => Ok(frames),
            _ => Err(FrameError::NotArray),
        }
    }

    // 不能返回Frame<'b>，因为&'a mut T的生命周期对于T是不变的
    pub fn on_array_mut(&mut self) -> Result<&mut Vec<RESP3>, FrameError> {
        match self {
            RESP3::Array(frames) => Ok(frames),
            _ => Err(FrameError::NotArray),
        }
    }

    pub fn into_array(self) -> Result<Vec<RESP3>, FrameError> {
        match self {
            RESP3::Array(frames) => Ok(frames.clone()),
            _ => Err(FrameError::NotArray),
        }
    }
}

impl RESP3 {
    pub fn into_bulks(self) -> Result<Bulks, FrameError> {
        match self {
            RESP3::Array(frames) => Ok(Bulks {
                start: 0,
                end: frames.len() - 1,
                frames,
            }),
            _ => Err(FrameError::NotArray),
        }
    }
}

#[derive(Debug)]
pub struct Bulks {
    start: usize,
    end: usize,
    pub frames: Vec<RESP3>,
}

impl Default for Bulks {
    fn default() -> Self {
        Self {
            start: 1,
            end: 0,
            frames: Vec::new(),
        }
    }
}

impl Bulks {
    #[inline]
    pub fn len(&self) -> usize {
        self.end - self.start + 1
    }

    pub fn is_empty(&self) -> bool {
        self.start > self.end
    }

    pub fn skip(&mut self, n: usize) {
        self.start += n;
    }

    pub fn to_vec(&self) -> Vec<Bytes> {
        self.frames[self.start..]
            .iter()
            .filter_map(|f| match f {
                RESP3::Bulk(b) => Some(b.clone()),
                _ => None,
            })
            .collect()
    }

    pub fn get(&self, index: usize) -> Option<Bytes> {
        if self.start + index > self.end {
            return None;
        }

        let frame = &self.frames[self.start + index];
        match frame {
            RESP3::Bulk(b) => Some(b.clone()),
            _ => None,
        }
    }

    pub fn pop_front(&mut self) -> Option<Bytes> {
        if self.start > self.end {
            return None;
        }

        let frame = &self.frames[self.start];
        self.start += 1;
        match frame {
            RESP3::Bulk(b) => Some(b.clone()),
            _ => None,
        }
    }

    pub fn pop_back(&mut self) -> Option<Bytes> {
        if self.start > self.end {
            return None;
        }

        let frame = &self.frames[self.end];
        self.end -= 1;
        match frame {
            RESP3::Bulk(b) => Some(b.clone()),
            _ => None,
        }
    }

    pub fn swap_remove(&mut self, index: usize) -> Result<Bytes, FrameError> {
        let res = self.frames.swap_remove(index);
        self.end -= 1;
        res.to_bulk()
    }

    pub fn iter(&mut self) -> impl Iterator<Item = &Bytes> {
        self.frames[self.start..].iter().filter_map(|f| match f {
            RESP3::Bulk(b) => Some(b),
            _ => None,
        })
    }

    #[inline]
    pub fn to_raw(self) -> Bytes {
        RESP3::Array(self.frames).to_raw()
    }

    pub fn into_frame(self) -> RESP3 {
        RESP3::Array(self.frames)
    }
}

impl Iterator for Bulks {
    type Item = Bytes;

    fn next(&mut self) -> Option<Self::Item> {
        if self.start > self.end {
            return None;
        }

        let frame = &self.frames[self.start];
        self.start += 1;
        match frame {
            RESP3::Bulk(b) => Some(b.clone()),
            _ => None,
        }
    }
}

impl From<Vec<RESP3>> for Bulks {
    fn from(value: Vec<RESP3>) -> Self {
        Self {
            start: 0,
            end: value.len() - 1,
            frames: value,
        }
    }
}

impl From<&[Bytes]> for Bulks {
    fn from(value: &[Bytes]) -> Self {
        Self {
            start: 0,
            end: value.len() - 1,
            frames: value
                .iter()
                .map(|b| RESP3::new_bulk_owned(b.clone()))
                .collect(),
        }
    }
}

impl From<&[&[u8]]> for Bulks {
    fn from(value: &[&[u8]]) -> Self {
        Self {
            start: 0,
            end: value.len() - 1,
            frames: value
                .iter()
                .map(|b| RESP3::new_bulk_by_copying(b))
                .collect(),
        }
    }
}

impl From<&[&'static str]> for Bulks {
    fn from(value: &[&'static str]) -> Self {
        Self {
            start: 0,
            end: value.len() - 1,
            frames: value
                .iter()
                .map(|b| RESP3::new_bulk_from_static(b.as_bytes()))
                .collect(),
        }
    }
}

impl IntoLua<'_> for RESP3 {
    fn into_lua(
        self,
        lua: &'_ mlua::prelude::Lua,
    ) -> mlua::prelude::LuaResult<mlua::prelude::LuaValue<'_>> {
        match self {
            RESP3::Simple(s) => Ok(Value::String(lua.create_string(s.as_ref())?)),
            RESP3::Error(e) => Err(mlua::Error::RuntimeError(e.into_owned())),
            RESP3::Integer(n) => Ok(Value::Integer(n)),
            RESP3::Bulk(b) => {
                if b.is_empty() {
                    Ok(Value::Boolean(false))
                } else {
                    Ok(Value::String(lua.create_string(b)?))
                }
            }

            RESP3::Null => Ok(Value::Nil),
            RESP3::Array(frames) => {
                let table = lua.create_table()?;
                for (i, frame) in frames.into_iter().enumerate() {
                    table.set(i + 1, frame.into_lua(lua)?)?;
                }
                Ok(Value::Table(table))
            }
        }
    }
}

impl FromLuaMulti<'_> for RESP3 {
    fn from_lua_multi<'lua>(
        mut values: mlua::prelude::LuaMultiValue<'lua>,
        lua: &'lua mlua::prelude::Lua,
    ) -> mlua::prelude::LuaResult<Self> {
        // TODO:
        todo!()
        //     let value = values.remove(0);
        //     match value {
        //         Value::String(s) => Ok(Frame::new_simple_owned(s.to_str()?.to_string())),
        //         Value::Integer(n) => Ok(Frame::Integer(n)),
        //         Value::Nil => Ok(Frame::Null),
        //         Value::Table(t) => {
        //             let mut frames = Vec::new();
        //             for i in 1.. {
        //                 let value = t.get(i)?;
        //                 if value.is_nil() {
        //                     break;
        //                 }
        //                 frames.push(value.to_lua(lua)?);
        //             }
        //
        //             Ok(Frame::Array(frames))
        //         }
        //         _ => Err(mlua::Error::FromLuaConversionError {
        //             from: value.type_name(),
        //             to: "Frame",
        //             message: Some("invalid frame type".to_string()),
        //         }),
        //     }
    }
}

#[derive(Debug, Snafu)]
pub enum FrameError {
    NotSimple,
    NotError,
    NotInteger,
    NotBulk,
    NotNull,
    NotArray,
    Unowned,
    InCompleteFrame,
    #[snafu(display("Invalid frame: {}", msg))]
    InvalidFormat {
        msg: String,
    },
    #[snafu(display("Invalid frame: {}", msg))]
    Other {
        msg: String,
    },
}

impl From<FrameError> for tokio::io::Error {
    fn from(val: FrameError) -> Self {
        tokio::io::Error::new(tokio::io::ErrorKind::InvalidData, val)
    }
}

impl TryFrom<CmdError> for RESP3 {
    type Error = ServerError;

    fn try_from(cmd_err: CmdError) -> Result<Self, ServerError> {
        let frame = match cmd_err {
            CmdError::IoErr { source, loc } => return Err(format!("{}: {}", loc, source).into()),
            // 命令执行失败，向客户端返回错误码
            CmdError::ErrorCode { code } => RESP3::new_integer(code),
            // 命令执行失败，向客户端返回空值
            CmdError::Null => RESP3::new_null(),
            // 命令执行失败，向客户端返回错误信息
            CmdError::Err { source } => RESP3::new_error_owned(source.to_string()),
        };

        Ok(frame)
    }
}

#[cfg(test)]
mod frame_tests {
    use super::*;

    #[test]
    fn size_test() {
        use crate::frame::RESP3;
        let frame1 = RESP3::new_simple_borrowed("OK"); // +OK\r\n
        assert_eq!(frame1.size(), 5);

        let frame2 = RESP3::new_error_borrowed("ERR"); // -ERR\r\n
        assert_eq!(frame2.size(), 6);

        let frame3 = RESP3::new_integer(100); // :100\r\n
        assert_eq!(frame3.size(), 6);

        let frame4 = RESP3::new_bulk_from_static(b"Hello"); // $5\r\nHello\r\n
        assert_eq!(frame4.size(), 11);

        let frame5 = RESP3::new_null(); // $-1\r\n
        assert_eq!(frame5.size(), 5);

        // *5\r\n+OK\r\n-ERR\r\n:100\r\n$5\r\nHello\r\n$-1\r\n
        let frame6 = RESP3::new_array(vec![frame1, frame2, frame3, frame4, frame5]);
        assert_eq!(frame6.size(), 37);
    }
}