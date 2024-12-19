use crate::{error::Resp3Error, resp3::*};
use atoi::atoi;
use bytes::{Buf, BufMut};
use num_bigint::BigInt;
use std::{
    collections::{HashMap, HashSet},
    future::Future,
    hash::Hash,
    intrinsics::{likely, unlikely},
    io::Cursor,
};
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio_util::codec::Decoder;

#[derive(Debug, Clone)]
pub struct Resp3Decoder<B, S> {
    pub pos: u64,
    phantom: std::marker::PhantomData<(B, S)>,
}

impl<B, S> Default for Resp3Decoder<B, S> {
    fn default() -> Self {
        Self {
            pos: 0,
            phantom: std::marker::PhantomData,
        }
    }
}

impl<B, S> Decoder for Resp3Decoder<B, S>
where
    B: for<'a> From<&'a mut [u8]>,
    S: for<'a> From<&'a mut str>,
    Resp3<B, S>: Hash + Eq,
{
    type Item = Resp3<B, S>;
    type Error = Resp3Error;

    fn decode(&mut self, src: &mut bytes::BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if self.pos >= src.len() as u64 {
            src.clear();
            self.pos = 0;
            return Ok(None);
        }

        let mut src = Cursor::new(src);
        src.set_position(self.pos);

        let pos = src.position();

        let res = {
            let mut io_read = tokio::io::empty();
            let mut fut = _decode_async(&mut io_read, &mut src);
            let fut = std::pin::pin!(fut);
            let mut cx = std::task::Context::from_waker(std::task::Waker::noop());

            fut.poll(&mut cx)
        };

        match res {
            std::task::Poll::Ready(res) => {
                self.pos = src.position();
                Ok(Some(res?))
            }
            std::task::Poll::Pending => {
                src.set_position(pos);
                Ok(None)
            }
        }
    }
}

pub async fn decode_async_multi<R, Buffer, B, S>(
    io_read: &mut R,
    src: &mut Cursor<Buffer>,
    max_batch: usize,
) -> Result<Option<Vec<Resp3<B, S>>>, Resp3Error>
where
    R: AsyncRead + Unpin,
    Buffer: BufMut + AsMut<[u8]> + AsRef<[u8]>,
    B: for<'a> From<&'a mut [u8]>,
    S: for<'a> From<&'a mut str>,
    Resp3<B, S>: Hash + Eq,
{
    let mut res = Vec::with_capacity(max_batch);

    decode_async_multi2(io_read, src, &mut res, max_batch).await?;

    Ok(Some(res))
}

pub async fn decode_async_multi2<R, Buffer, B, S>(
    io_read: &mut R,
    src: &mut Cursor<Buffer>,
    buf: &mut impl Resp3Buf<B, S>,
    max_batch: usize,
) -> Result<Option<()>, Resp3Error>
where
    R: AsyncRead + Unpin,
    Buffer: BufMut + AsMut<[u8]> + AsRef<[u8]>,
    B: for<'a> From<&'a mut [u8]>,
    S: for<'a> From<&'a mut str>,
    Resp3<B, S>: Hash + Eq,
{
    for _ in 0..max_batch {
        if let Some(frame) = decode_async(io_read, src).await? {
            buf.push_resp3(frame);
        } else if buf.is_empty() {
            return Ok(None);
        }

        if !src.has_remaining() {
            break;
        }
    }

    Ok(Some(()))
}

pub async fn decode_async<R, Buffer, B, S>(
    io_read: &mut R,
    src: &mut Cursor<Buffer>,
) -> Result<Option<Resp3<B, S>>, Resp3Error>
where
    R: AsyncRead + Unpin,
    Buffer: BufMut + AsMut<[u8]> + AsRef<[u8]>,
    B: for<'a> From<&'a mut [u8]>,
    S: for<'a> From<&'a mut str>,
    Resp3<B, S>: Hash + Eq,
{
    if !src.has_remaining() && io_read.read_buf(src.get_mut()).await? == 0 {
        return Ok(None);
    }

    let res = _decode_async(io_read, src).await?;
    Ok(Some(res))
}

async fn _decode_async<R, Buffer, B, S>(
    io_read: &mut R,
    src: &mut Cursor<Buffer>,
) -> Result<Resp3<B, S>, Resp3Error>
where
    R: AsyncRead + Unpin,
    Buffer: BufMut + AsMut<[u8]> + AsRef<[u8]>,
    B: for<'a> From<&'a mut [u8]>,
    S: for<'a> From<&'a mut str>,
    Resp3<B, S>: Hash + Eq,
{
    let line = decode_line_async(io_read, src).await?;

    let prefix = *line.first().ok_or_else(|| Resp3Error::InvalidFormat {
        msg: "missing prefix".into(),
    })?;
    let line = line.get_mut(1..).unwrap_or_default();

    let res = match prefix {
        BLOB_STRING_PREFIX => {
            if unlikely(line.first().is_some_and(|ch| *ch == CHUNKED_STRING_PREFIX)) {
                let mut chunks = Vec::new();

                loop {
                    let line = decode_line_async(io_read, src).await?;

                    // if !line
                    //     .get(0)
                    //     .is_some_and(|ch| ch == CHUNKED_STRING_LENGTH_PREFIX)
                    // {
                    //     return Err(Resp3Error::InvalidFormat {
                    //         msg: "invalid chunk length prefix".into(),
                    //     });
                    // }

                    let len: usize = atoi(line.get(1..).unwrap_or_default()).ok_or_else(|| {
                        Resp3Error::InvalidFormat {
                            msg: "invalid chunks length".into(),
                        }
                    })?;

                    if len == 0 {
                        break;
                    }

                    need_bytes_async(io_read, src, len + 2).await?;
                    let pos = src.position() as usize;
                    let chunk = B::from(&mut src.get_mut().as_mut()[pos..pos + len]);
                    src.advance(len + 2);

                    chunks.push(chunk);
                }

                Resp3::ChunkedString(chunks)
            } else {
                let len: usize = atoi(line).ok_or_else(|| Resp3Error::InvalidFormat {
                    msg: "invalid blob string length".into(),
                })?;

                need_bytes_async(io_read, src, len + 2).await?;
                let pos = src.position() as usize;
                let blob = B::from(&mut src.get_mut().as_mut()[pos..pos + len]);
                src.advance(len + 2);

                Resp3::BlobString {
                    inner: blob,
                    attributes: None,
                }
            }
        }
        ARRAY_PREFIX => {
            let len = atoi(line).ok_or_else(|| Resp3Error::InvalidFormat {
                msg: "invalid blob string length".into(),
            })?;

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
        SIMPLE_STRING_PREFIX => {
            let string = std::str::from_utf8_mut(line).map_err(|e| Resp3Error::InvalidFormat {
                msg: format!("invalid simple string: {}", e).into(),
            })?;

            Resp3::SimpleString {
                inner: S::from(string),
                attributes: None,
            }
        }
        SIMPLE_ERROR_PREFIX => {
            let error = std::str::from_utf8_mut(line).map_err(|_| Resp3Error::InvalidFormat {
                msg: "invalid simple error".into(),
            })?;

            Resp3::SimpleError {
                inner: S::from(error),
                attributes: None,
            }
        }
        INTEGER_PREFIX => {
            let integer = atoi(line).ok_or_else(|| Resp3Error::InvalidFormat {
                msg: "invalid integer".into(),
            })?;

            Resp3::Integer {
                inner: integer,
                attributes: None,
            }
        }
        NULL_PREFIX => Resp3::Null,
        BOOLEAN_PREFIX => {
            let b = match line.first() {
                Some(b't') => true,
                Some(b'f') => false,
                _ => {
                    return Err(Resp3Error::InvalidFormat {
                        msg: "invalid boolean".into(),
                    });
                }
            };

            Resp3::Boolean {
                inner: b,
                attributes: None,
            }
        }
        DOUBLE_PREFIX => {
            let double = std::str::from_utf8(line)
                .map_err(|_| Resp3Error::InvalidFormat {
                    msg: "invalid double".into(),
                })?
                .parse()
                .map_err(|_| Resp3Error::InvalidFormat {
                    msg: "invalid double".into(),
                })?;

            Resp3::Double {
                inner: double,
                attributes: None,
            }
        }
        BIG_NUMBER_PREFIX => {
            let n = BigInt::parse_bytes(line, 10).ok_or_else(|| Resp3Error::InvalidFormat {
                msg: "invalid big number".into(),
            })?;

            Resp3::BigNumber {
                inner: n,
                attributes: None,
            }
        }
        BLOB_ERROR_PREFIX => {
            let len: usize = atoi(line).ok_or_else(|| Resp3Error::InvalidFormat {
                msg: "invalid blob error length".into(),
            })?;

            need_bytes_async(io_read, src, len + 2).await?;
            let pos = src.position() as usize;
            let error = B::from(&mut src.get_mut().as_mut()[pos..pos + len]);
            src.advance(len + 2);

            Resp3::BlobError {
                inner: error,
                attributes: None,
            }
        }
        VERBATIM_STRING_PREFIX => {
            let len: usize = atoi(line).ok_or_else(|| Resp3Error::InvalidFormat {
                msg: "invalid verbatim string length".into(),
            })?;

            need_bytes_async(io_read, src, len + 2).await?;

            let format = src.chunk()[0..3].try_into().unwrap();
            let pos = src.position() as usize;
            let data = B::from(&mut src.get_mut().as_mut()[pos + 4..pos + len]);
            src.advance(len + 2);

            Resp3::VerbatimString {
                format,
                data,
                attributes: None,
            }
        }
        MAP_PREFIX => {
            let len = atoi(line).ok_or_else(|| Resp3Error::InvalidFormat {
                msg: "invalid map length".into(),
            })?;

            let mut map = HashMap::with_capacity(len);

            for _ in 0..len {
                let k = Box::pin(_decode_async(io_read, src)).await?;
                let v = Box::pin(_decode_async(io_read, src)).await?;
                map.insert(k, v);
            }

            Resp3::Map {
                inner: map,
                attributes: None,
            }
        }
        SET_PREFIX => {
            let len = atoi(line).ok_or_else(|| Resp3Error::InvalidFormat {
                msg: "invalid set length".into(),
            })?;

            let mut set = HashSet::with_capacity(len);

            for _ in 0..len {
                let frame = Box::pin(_decode_async(io_read, src)).await?;
                set.insert(frame);
            }

            Resp3::Set {
                inner: set,
                attributes: None,
            }
        }
        PUSH_PREFIX => {
            let len = atoi(line).ok_or_else(|| Resp3Error::InvalidFormat {
                msg: "invalid push length".into(),
            })?;

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
            let mut auth = None;

            let mut white_spaces = line
                .iter()
                .enumerate()
                .filter_map(|(i, &ch)| if ch == b' ' { Some(i) } else { None })
                .collect::<Vec<usize>>()
                .into_iter();

            let mut start = white_spaces
                .next()
                .ok_or_else(|| Resp3Error::InvalidFormat {
                    msg: "invalid hello".into(),
                })?
                + 1;

            let mut end = white_spaces.next().unwrap_or(line.len());

            let version =
                atoi::<i128>(&line[start..end]).ok_or_else(|| Resp3Error::InvalidFormat {
                    msg: "invalid protocol version".into(),
                })?;

            while let Some(i) = white_spaces.next() {
                start = end + 1;
                end = i;
                let tag = &mut line[start..end];

                tag.make_ascii_uppercase();
                #[allow(clippy::single_match)]
                match tag as &[u8] {
                    b"AUTH" => {
                        start = end + 1;
                        end = white_spaces
                            .next()
                            .ok_or_else(|| Resp3Error::InvalidFormat {
                                msg: "invalid hello, missing username".into(),
                            })?;
                        let username = B::from(&mut line[start..end]);

                        start = end + 1;
                        end = white_spaces.next().unwrap_or(line.len());
                        let password = B::from(&mut line[start..end]);

                        auth = Some((username, password));
                    }
                    _ => {}
                }
            }

            Resp3::Hello { version, auth }
        }
        prefix => {
            return Err(Resp3Error::InvalidFormat {
                msg: format!("invalid prefix: {}", prefix).into(),
            });
        }
    };

    Ok(res)
}

#[inline]
pub async fn decode_line_async<'a, R, B>(
    io_read: &mut R,
    src: &'a mut Cursor<B>,
) -> Result<&'a mut [u8], Resp3Error>
where
    R: AsyncRead + AsyncReadExt + Unpin,
    B: BufMut + AsMut<[u8]> + AsRef<[u8]>,
{
    // FIX: https://github.com/nikomatsakis/nll-rfc/blob/master/0000-nonlexical-lifetimes.md#problem-case-3-conditional-control-flow-across-functions
    // loop {
    //     if let Ok(line) = decode_line(src) {
    //         return Ok(line);
    //     }
    //
    //     if io_read.read_buf(src.get_mut()).await? == 0 {
    //         return Err(tokio::io::Error::new(
    //             tokio::io::ErrorKind::UnexpectedEof,
    //             "unexpected eof",
    //         )
    //         .into());
    //     }
    // }
    loop {
        if let Ok((start, end)) = decode_line2(src) {
            src.advance(end + 2);
            return Ok(&mut src.get_mut().as_mut()[start..start + end]);
        }

        if io_read.read_buf(src.get_mut()).await? == 0 {
            return Err(tokio::io::Error::new(
                tokio::io::ErrorKind::UnexpectedEof,
                "unexpected eof",
            )
            .into());
        }
    }
}

#[inline]
pub async fn decode_line_async2<R, B>(
    io_read: &mut R,
    src: &mut Cursor<B>,
) -> Result<(usize, usize), Resp3Error>
where
    R: AsyncRead + AsyncReadExt + Unpin,
    B: BufMut + AsMut<[u8]> + AsRef<[u8]>,
{
    loop {
        if let Ok(range) = decode_line2(src) {
            return Ok(range);
        }

        if io_read.read_buf(src.get_mut()).await? == 0 {
            return Err(tokio::io::Error::new(
                tokio::io::ErrorKind::UnexpectedEof,
                "unexpected eof",
            )
            .into());
        }
    }
}

#[inline]
pub async fn need_bytes_async<R, B>(
    io_read: &mut R,
    src: &mut Cursor<B>,
    len: usize,
) -> Result<(), Resp3Error>
where
    R: AsyncRead + AsyncReadExt + Unpin,
    B: BufMut + AsRef<[u8]>,
{
    while need_bytes(src, len).is_err() {
        if io_read.read_buf(src.get_mut()).await? == 0 {
            return Err(tokio::io::Error::new(
                tokio::io::ErrorKind::UnexpectedEof,
                "unexpected eof",
            )
            .into());
        }
    }

    Ok(())
}

#[allow(dead_code)]
#[inline]
pub fn decode_line<B>(src: &mut Cursor<B>) -> Result<&mut [u8], Resp3Error>
where
    B: AsMut<[u8]> + AsRef<[u8]>,
{
    let pos = src.position();
    let slice = src.get_mut().as_mut();
    let pos = match usize::try_from(pos) {
        Ok(pos) => usize::min(pos, slice.len()),
        Err(_) => slice.len(),
    };
    let chunk = &mut slice[pos..];

    if let Some(i) = memchr::memchr(b'\n', chunk) {
        if likely(i > 0 && chunk[i - 1] == b'\r') {
            // skip \r\n
            src.advance(i + 1);
            let line = &mut src.get_mut().as_mut()[pos..pos + i - 1];

            return Ok(line);
        }
    }

    Err(Resp3Error::Incomplete)
}

#[inline]
pub fn decode_line2<B>(src: &mut Cursor<B>) -> Result<(usize, usize), Resp3Error>
where
    B: AsMut<[u8]> + AsRef<[u8]>,
{
    let pos = src.position();
    let slice = src.get_mut().as_mut();
    let pos = match usize::try_from(pos) {
        Ok(pos) => usize::min(pos, slice.len()),
        Err(_) => slice.len(),
    };
    let chunk = &mut slice[pos..];

    if let Some(i) = memchr::memchr(b'\n', chunk) {
        if likely(i > 0 && chunk[i - 1] == b'\r') {
            return Ok((pos, i - 1));
        }
    }

    Err(Resp3Error::Incomplete)
}

#[inline]
pub fn need_bytes<B>(src: &Cursor<B>, len: usize) -> Result<(), Resp3Error>
where
    B: AsRef<[u8]>,
{
    if src.remaining() < len {
        return Err(Resp3Error::Incomplete);
    }

    Ok(())
}

pub trait Resp3Buf<B, S> {
    fn push_resp3(&mut self, frame: Resp3<B, S>);
    fn is_empty(&self) -> bool;
}

impl<B, S> Resp3Buf<B, S> for std::collections::VecDeque<Resp3<B, S>> {
    fn push_resp3(&mut self, frame: Resp3<B, S>) {
        self.push_back(frame);
    }

    fn is_empty(&self) -> bool {
        self.is_empty()
    }
}

impl<B, S> Resp3Buf<B, S> for Vec<Resp3<B, S>> {
    fn push_resp3(&mut self, frame: Resp3<B, S>) {
        self.push(frame);
    }

    fn is_empty(&self) -> bool {
        self.is_empty()
    }
}
