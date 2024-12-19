use crate::resp3::*;
use bytes::{BufMut, BytesMut};
use tokio_util::codec::Encoder;

#[derive(Debug, Clone, Copy)]
pub struct InfallibleError;

impl From<tokio::io::Error> for InfallibleError {
    fn from(_: tokio::io::Error) -> Self {
        Self
    }
}

#[derive(Debug, Clone, Default)]
pub struct Resp3Encoder;

impl<B, S> Encoder<Resp3<B, S>> for Resp3Encoder
where
    B: AsRef<[u8]>,
    S: AsRef<str>,
{
    type Error = InfallibleError;

    fn encode(&mut self, item: Resp3<B, S>, dst: &mut BytesMut) -> Result<(), Self::Error> {
        item.to_bytes_buf(dst);
        Ok(())
    }
}

impl<B, S> Resp3<B, S>
where
    B: AsRef<[u8]>,
    S: AsRef<str>,
{
    #[inline]
    pub fn to_bytes<Buffer>(&self) -> Buffer
    where
        Buffer: BufMut + Default,
    {
        let mut buf = Buffer::default();
        self.to_bytes_buf(&mut buf);
        buf
    }

    pub fn to_bytes_buf(&self, buf: &mut impl BufMut) {
        match self {
            Self::SimpleString { inner, attributes } => {
                if let Some(attr) = attributes.as_ref() {
                    encode_attributes(buf, attr)
                }
                buf.put_u8(SIMPLE_STRING_PREFIX);
                buf.put_slice(inner.as_ref().as_bytes());
                buf.put_slice(CRLF);
            }
            Self::SimpleError { inner, attributes } => {
                if let Some(attr) = attributes.as_ref() {
                    encode_attributes(buf, attr)
                }
                buf.put_u8(SIMPLE_ERROR_PREFIX);
                buf.put_slice(inner.as_ref().as_bytes());
                buf.put_slice(CRLF);
            }
            Self::Integer { inner, attributes } => {
                if let Some(attr) = attributes.as_ref() {
                    encode_attributes(buf, attr)
                }
                buf.put_u8(INTEGER_PREFIX);
                buf.put_slice(itoa::Buffer::new().format(*inner).as_bytes());
                buf.put_slice(CRLF);
            }
            Self::BlobString { inner, attributes } => {
                if let Some(attr) = attributes.as_ref() {
                    encode_attributes(buf, attr)
                }
                buf.put_u8(BLOB_STRING_PREFIX);
                buf.put_slice(itoa::Buffer::new().format(inner.as_ref().len()).as_bytes());
                buf.put_slice(CRLF);
                buf.put_slice(inner.as_ref());
                buf.put_slice(CRLF);
            }
            Self::Array { inner, attributes } => {
                if let Some(attr) = attributes.as_ref() {
                    encode_attributes(buf, attr)
                }
                buf.put_u8(ARRAY_PREFIX);
                buf.put_slice(itoa::Buffer::new().format(inner.len()).as_bytes());
                buf.put_slice(CRLF);
                for frame in inner {
                    frame.to_bytes_buf(buf);
                }
            }
            Self::Null => buf.put_slice(b"_\r\n"),
            Self::Boolean { inner, attributes } => {
                if let Some(attr) = attributes.as_ref() {
                    encode_attributes(buf, attr)
                }
                buf.put_u8(BOOLEAN_PREFIX);
                buf.put_slice(if *inner { b"t" } else { b"f" });
                buf.put_slice(CRLF);
            }
            Self::Double { inner, attributes } => {
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
            Self::BigNumber { inner, attributes } => {
                if let Some(attr) = attributes.as_ref() {
                    encode_attributes(buf, attr)
                }
                buf.put_u8(BIG_NUMBER_PREFIX);
                buf.put_slice(inner.to_str_radix(10).as_bytes());
                buf.put_slice(CRLF);
            }
            Self::BlobError { inner, attributes } => {
                if let Some(attr) = attributes.as_ref() {
                    encode_attributes(buf, attr)
                }
                buf.put_u8(BLOB_ERROR_PREFIX);
                buf.put_slice(itoa::Buffer::new().format(inner.as_ref().len()).as_bytes());
                buf.put_slice(CRLF);
                buf.put_slice(inner.as_ref());
                buf.put_slice(CRLF);
            }
            Self::VerbatimString {
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
            Self::Map { inner, attributes } => {
                if let Some(attr) = attributes.as_ref() {
                    encode_attributes(buf, attr)
                }
                buf.put_u8(MAP_PREFIX);
                buf.put_slice(itoa::Buffer::new().format(inner.len()).as_bytes());
                buf.put_slice(CRLF);
                for (k, v) in inner {
                    k.to_bytes_buf(buf);
                    v.to_bytes_buf(buf);
                }
            }
            Self::Set { inner, attributes } => {
                if let Some(attr) = attributes.as_ref() {
                    encode_attributes(buf, attr)
                }
                buf.put_u8(SET_PREFIX);
                buf.put_slice(itoa::Buffer::new().format(inner.len()).as_bytes());
                buf.put_slice(CRLF);
                for frame in inner {
                    frame.to_bytes_buf(buf);
                }
            }
            Self::Push { inner, attributes } => {
                if let Some(attr) = attributes.as_ref() {
                    encode_attributes(buf, attr)
                }
                buf.put_u8(PUSH_PREFIX);
                buf.put_slice(itoa::Buffer::new().format(inner.len()).as_bytes());
                buf.put_slice(CRLF);
                for frame in inner {
                    frame.to_bytes_buf(buf);
                }
            }
            Self::ChunkedString(chunks) => {
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
            Self::Hello { version, auth } => {
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

fn encode_attributes<B, S>(buf: &mut impl BufMut, attr: &Attributes<B, S>)
where
    B: AsRef<[u8]>,
    S: AsRef<str>,
{
    buf.put_u8(b'|');
    buf.put_slice(itoa::Buffer::new().format(attr.len()).as_bytes());
    buf.put_slice(CRLF);
    for (k, v) in attr {
        k.to_bytes_buf(buf);
        v.to_bytes_buf(buf);
    }
}
