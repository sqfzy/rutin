use crate::{shared::db::DbError, util::to_valid_range, Int};
use atoi::atoi;
use bytes::{Bytes, BytesMut};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Str {
    Raw(Bytes),
    Int(IntType),
}

impl Str {
    pub fn type_str(&self) -> &'static str {
        match self {
            Self::Raw(_) => "string",
            Self::Int(_) => "int",
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Self::Raw(b) => b.len(),
            Self::Int(i) => i.as_bytes(&mut itoa::Buffer::new()).len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn set(&mut self, other: Bytes) -> Str {
        if let Some(i) = atoi::<Int>(&other) {
            std::mem::replace(self, Self::Int(i.into()))
        } else {
            std::mem::replace(self, Self::Raw(other))
        }
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
                Self::Int(i) => i.as_bytes(buffer)[start_index..end_index].into(),
            }
        } else {
            b""
        }
    }

    pub fn as_bytes<'a: 'b, 'b>(&'a self, buffer: &'b mut itoa::Buffer) -> &'b [u8] {
        match self {
            Self::Raw(b) => b,
            Self::Int(i) => i.as_bytes(buffer),
        }
    }

    pub fn to_bytes(&self) -> Bytes {
        match self {
            Self::Raw(b) => b.clone(),
            Self::Int(i) => Bytes::copy_from_slice(i.as_bytes(&mut itoa::Buffer::new())),
        }
    }

    pub fn to_vec(&self) -> Vec<u8> {
        match self {
            Self::Raw(b) => b.to_vec(),
            Self::Int(i) => i.as_bytes(&mut itoa::Buffer::new()).to_vec(),
        }
    }

    pub fn on_raw(&self) -> Result<Bytes, DbError> {
        match self {
            Self::Raw(b) => Ok(b.clone()),
            _ => Err(DbError::TypeErr {
                expected: "str::raw",
                found: self.type_str(),
            }),
        }
    }

    pub fn on_int(&self) -> Result<Int, DbError> {
        match self {
            Self::Int(i) => Ok(i.get()),
            _ => Err(DbError::TypeErr {
                expected: "str::int",
                found: self.type_str(),
            }),
        }
    }

    pub fn incr_by(&mut self, delta: Int) -> Result<Int, DbError> {
        match self {
            Self::Int(i) => i.incr_by(delta),
            _ => Err(DbError::TypeErr {
                expected: "str::int",
                found: self.type_str(),
            }),
        }
    }

    pub fn decr_by(&mut self, delta: Int) -> Result<Int, DbError> {
        match self {
            Self::Int(i) => i.decr_by(delta),
            _ => Err(DbError::TypeErr {
                expected: "str::int",
                found: self.type_str(),
            }),
        }
    }

    pub fn append(&mut self, other: Bytes) {
        match self {
            Self::Raw(b) => b.to_vec().extend(other),
            Self::Int(i) => {
                let mut raw = BytesMut::from(i.as_bytes(&mut itoa::Buffer::new()));
                raw.extend(other);
                // 尝试将新的Str解析为Int，如果解析成功则更新Int的值，否则变为Raw
                if let Some(new_num) = atoi(&raw) {
                    i.set(new_num);
                } else {
                    *self = Self::Raw(raw.freeze());
                }
            }
        }
    }
}

impl From<Bytes> for Str {
    fn from(b: Bytes) -> Self {
        if let Some(i) = atoi::<Int>(&b) {
            return Str::Int(i.into());
        }
        Self::Raw(b)
    }
}

impl From<&str> for Str {
    fn from(s: &str) -> Self {
        if let Some(i) = atoi::<Int>(s.as_bytes()) {
            return Str::Int(i.into());
        }
        Self::Raw(Bytes::copy_from_slice(s.as_bytes()))
    }
}

impl From<&[u8]> for Str {
    fn from(b: &[u8]) -> Self {
        if let Some(i) = atoi::<Int>(b) {
            return Str::Int(i.into());
        }
        Self::Raw(Bytes::copy_from_slice(b))
    }
}

impl From<i8> for Str {
    fn from(i: i8) -> Self {
        Self::Int(i.into())
    }
}

impl From<i16> for Str {
    fn from(i: i16) -> Self {
        Self::Int(i.into())
    }
}

impl From<i32> for Str {
    fn from(i: i32) -> Self {
        Self::Int(i.into())
    }
}

impl From<i64> for Str {
    fn from(i: i64) -> Self {
        Self::Int(i.into())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IntType {
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
}

impl From<i8> for IntType {
    fn from(i: i8) -> Self {
        Self::Int8(i)
    }
}

impl From<i16> for IntType {
    fn from(i: i16) -> Self {
        if i > i8::MIN as i16 && i < i8::MAX as i16 {
            Self::Int8(i as i8)
        } else {
            Self::Int16(i)
        }
    }
}

impl From<i32> for IntType {
    fn from(i: i32) -> Self {
        if i > i8::MIN as i32 && i < i8::MAX as i32 {
            Self::Int8(i as i8)
        } else if i > i16::MIN as i32 && i < i16::MAX as i32 {
            Self::Int16(i as i16)
        } else {
            Self::Int32(i)
        }
    }
}

impl From<IntType> for i32 {
    fn from(val: IntType) -> Self {
        match val {
            IntType::Int8(i) => i as i32,
            IntType::Int16(i) => i as i32,
            IntType::Int32(i) => i,
            IntType::Int64(i) => i as i32,
        }
    }
}

impl From<i64> for IntType {
    fn from(i: i64) -> Self {
        if i > i8::MIN as i64 && i < i8::MAX as i64 {
            Self::Int8(i as i8)
        } else if i > i16::MIN as i64 && i < i16::MAX as i64 {
            Self::Int16(i as i16)
        } else if i > i32::MIN as i64 && i < i32::MAX as i64 {
            Self::Int32(i as i32)
        } else {
            Self::Int64(i)
        }
    }
}

impl IntType {
    pub fn get(&self) -> Int {
        match self {
            Self::Int8(i) => *i as Int,
            Self::Int16(i) => *i as Int,
            Self::Int32(i) => *i as Int,
            Self::Int64(i) => *i as Int,
        }
    }

    pub fn set(&mut self, i: Int) {
        if i > i8::MIN as Int && i < i8::MAX as Int {
            *self = Self::Int8(i as i8);
        } else if i > i16::MIN as Int && i < i16::MAX as Int {
            *self = Self::Int16(i as i16);
        } else {
            *self = Self::Int32(i as i32);
        }
    }

    pub fn as_bytes<'a>(&self, buffer: &'a mut itoa::Buffer) -> &'a [u8] {
        match self {
            Self::Int8(i) => buffer.format(*i).as_bytes(),
            Self::Int16(i) => buffer.format(*i).as_bytes(),
            Self::Int32(i) => buffer.format(*i).as_bytes(),
            Self::Int64(i) => buffer.format(*i).as_bytes(),
        }
    }

    fn incr_by(&mut self, delta: Int) -> Result<Int, DbError> {
        // 算出结果
        let res = match self {
            Self::Int8(i) => (*i as Int).checked_add(delta),
            Self::Int16(i) => (*i as Int).checked_add(delta),
            Self::Int32(i) => (*i as Int).checked_add(delta),
            Self::Int64(i) => (*i as Int).checked_add(delta),
        };

        // 根据结果更新原值
        if let Some(res) = res {
            if res < i8::MIN as Int {
                *self = Self::Int8(res as i8);
            } else if res < i16::MIN as Int {
                *self = Self::Int16(res as i16);
            } else if res < i32::MIN as Int {
                *self = Self::Int32(res as i32);
            } else {
                *self = Self::Int64(res);
            }

            Ok(res)
        } else {
            Err(DbError::Overflow)
        }
    }

    fn decr_by(&mut self, delta: Int) -> Result<Int, DbError> {
        // 算出结果
        let res = match self {
            Self::Int8(i) => (*i as Int).checked_sub(delta),
            Self::Int16(i) => (*i as Int).checked_sub(delta),
            Self::Int32(i) => (*i as Int).checked_sub(delta),
            Self::Int64(i) => (*i as Int).checked_sub(delta),
        };

        // 根据结果更新原值
        if let Some(res) = res {
            if res > i8::MIN as Int {
                *self = Self::Int8(res as i8);
            } else if res > i16::MIN as Int {
                *self = Self::Int16(res as i16);
            } else if res > i32::MIN as Int {
                *self = Self::Int32(res as i32);
            } else {
                *self = Self::Int64(res);
            }

            Ok(res)
        } else {
            Err(DbError::Overflow)
        }
    }
}

impl Default for Str {
    fn default() -> Self {
        Str::Raw("".into())
    }
}
