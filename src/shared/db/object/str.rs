use crate::{
    error::{RutinError, RutinResult},
    util::to_valid_range,
    Int,
};
use atoi::atoi;
use bytes::{BufMut, Bytes, BytesMut};
use std::mem::size_of;
use strum::{EnumDiscriminants, IntoStaticStr};

pub macro str_as_bytes($str:expr) {
    $str.as_bytes(&mut itoa::Buffer::new())
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, EnumDiscriminants, IntoStaticStr)]
#[strum_discriminants(vis(pub), name(StrType), derive(IntoStaticStr))]
pub enum Str {
    #[strum(serialize = "string::raw")]
    Raw(Bytes),
    #[strum(serialize = "string::int")]
    Int(i128),
}

impl Str {
    pub fn type_str(&self) -> &'static str {
        match self {
            Self::Raw(_) => StrType::Raw.into(),
            Self::Int(_) => StrType::Int.into(),
        }
    }

    pub const fn size(&self) -> u64 {
        match self {
            Self::Raw(b) => (b.len() + size_of::<Bytes>()) as u64,
            Self::Int(_) => size_of::<i128>() as u64,
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Self::Raw(b) => b.len(),
            Self::Int(i) => itoa::Buffer::new().format(*i).len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn set(&mut self, other: impl Into<Str>) -> Str {
        let mut new = other.into();
        std::mem::swap(self, &mut new);
        new
    }

    pub fn is_raw(&self) -> bool {
        matches!(self, Self::Raw(_))
    }

    pub fn is_int(&self) -> bool {
        matches!(self, Self::Int(_))
    }

    /// 获取字串，首个字符索引为1，末尾字符索引为-1
    pub fn get_range<'a>(&'a self, buffer: &'a mut itoa::Buffer, start: Int, end: Int) -> &'a [u8] {
        if let Some((start_index, end_index)) = to_valid_range(start, end, self.len()) {
            match self {
                Self::Raw(b) => b.get(start_index..end_index).unwrap(),
                Self::Int(i) => buffer.format(*i).as_bytes()[start_index..end_index].into(),
            }
        } else {
            b""
        }
    }

    pub fn as_bytes<'a: 'b, 'b>(&'a self, buffer: &'b mut itoa::Buffer) -> &'b [u8] {
        match self {
            Self::Raw(b) => b,
            Self::Int(i) => buffer.format(*i).as_bytes(),
        }
    }

    #[inline]
    pub fn to_bytes(&self) -> Bytes {
        match self {
            Self::Raw(b) => b.clone(),
            Self::Int(i) => Bytes::copy_from_slice(itoa::Buffer::new().format(*i).as_bytes()),
        }
    }

    pub fn to_vec(&self) -> Vec<u8> {
        match self {
            Self::Raw(b) => b.to_vec(),
            Self::Int(i) => itoa::Buffer::new().format(*i).as_bytes().to_vec(),
        }
    }

    pub fn on_raw(&self) -> RutinResult<Bytes> {
        match self {
            Self::Raw(b) => Ok(b.clone()),
            _ => Err(RutinError::TypeErr {
                expected: StrType::Int.into(),
                found: self.type_str(),
            }),
        }
    }

    pub fn on_int(&self) -> RutinResult<Int> {
        match self {
            Self::Int(i) => Ok(*i as Int),
            _ => Err(RutinError::TypeErr {
                expected: StrType::Int.into(),
                found: self.type_str(),
            }),
        }
    }

    pub fn incr_by(&mut self, num: Int) -> RutinResult<Int> {
        match self {
            Self::Int(i) => Ok(i.checked_add(num).ok_or(RutinError::Overflow)? as Int),
            _ => Err(RutinError::TypeErr {
                expected: StrType::Int.into(),
                found: self.type_str(),
            }),
        }
    }

    pub fn decr_by(&mut self, num: Int) -> RutinResult<Int> {
        match self {
            Self::Int(i) => Ok(i.checked_sub(num).ok_or(RutinError::Overflow)? as Int),
            _ => Err(RutinError::TypeErr {
                expected: StrType::Int.into(),
                found: self.type_str(),
            }),
        }
    }

    pub fn append(&mut self, other: &[u8]) {
        match self {
            Self::Raw(b) => {
                let mut buf = BytesMut::with_capacity(b.len() + other.len());
                buf.put_slice(b);
                buf.put_slice(other);

                *self = Self::from(buf.freeze());
            }
            Self::Int(i) => {
                let mut raw = BytesMut::from(itoa::Buffer::new().format(*i).as_bytes());
                raw.put_slice(other);
                // 尝试将新的Str解析为Int，如果解析成功则更新Int的值，否则变为Raw
                if let Some(new_num) = atoi(&raw) {
                    *i = new_num;
                } else {
                    *self = Self::from(raw.freeze());
                }
            }
        }
    }
}

impl From<Bytes> for Str {
    fn from(b: Bytes) -> Self {
        if let Some(i) = atoi(&b) {
            return Str::Int(i);
        }

        Self::Raw(b)
    }
}

impl From<BytesMut> for Str {
    fn from(b: BytesMut) -> Self {
        if let Some(i) = atoi(&b) {
            return Str::Int(i);
        }

        Self::Raw(b.freeze())
    }
}

impl From<&'static str> for Str {
    fn from(s: &'static str) -> Self {
        if let Some(i) = atoi(s.as_bytes()) {
            return Str::Int(i);
        }

        Self::Raw(Bytes::from(s))
    }
}

impl From<&[u8]> for Str {
    fn from(b: &[u8]) -> Self {
        if let Some(i) = atoi::<Int>(b) {
            return Str::Int(i);
        }

        Self::Raw(Bytes::copy_from_slice(b))
    }
}

impl From<&mut [u8]> for Str {
    fn from(b: &mut [u8]) -> Self {
        if let Some(i) = atoi::<Int>(b) {
            return Str::Int(i);
        }

        Self::Raw(Bytes::copy_from_slice(b))
    }
}

impl From<i128> for Str {
    fn from(i: i128) -> Self {
        Self::Int(i)
    }
}

impl From<Str> for Bytes {
    fn from(s: Str) -> Self {
        match s {
            Str::Raw(b) => b,
            Str::Int(i) => Bytes::copy_from_slice(itoa::Buffer::new().format(i).as_bytes()),
        }
    }
}

impl Default for Str {
    fn default() -> Self {
        Str::Int(0)
    }
}

#[cfg(feature = "zeroize")]
impl Drop for Str {
    fn drop(&mut self) {
        use zeroize::Zeroize;

        match self {
            Self::Raw(b) => {
                if b.is_unique()
                    && let Ok(mut b) = b.split_to(b.len()).try_into_mut()
                {
                    b.zeroize();
                }
            }
            Self::Int(i) => i.zeroize(),
        }
    }
}
