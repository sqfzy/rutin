use crate::shared::db::Str;
use bytes::{Bytes, BytesMut};
use std::{
    cmp::PartialEq,
    fmt::{Debug, Display},
    hash::Hash,
    ops::Deref,
};

#[inline]
pub unsafe fn leak_bytes(b: &[u8]) -> StaticBytes {
    StaticBytes::Const(std::mem::transmute::<&[u8], &'static [u8]>(b))
}

#[inline]
pub unsafe fn leak_bytes_mut(b: &mut [u8]) -> StaticBytes {
    StaticBytes::Mut(std::mem::transmute::<&mut [u8], &'static mut [u8]>(b))
}

#[inline]
pub unsafe fn leak_str(s: &str) -> StaticStr {
    StaticStr::Const(std::mem::transmute::<&str, &'static str>(s))
}

#[inline]
pub unsafe fn leak_str_mut(s: &mut str) -> StaticStr {
    StaticStr::Mut(std::mem::transmute::<&mut str, &'static mut str>(s))
}

#[derive(Debug, Eq)]
pub enum StaticBytes {
    Const(&'static [u8]),
    Mut(&'static mut [u8]),
}

impl StaticBytes {
    /// # Safety
    /// 将其生命周期视为static，但实际的生命周期在执行finish_read_frames()时结束
    pub unsafe fn into_inner(self) -> &'static [u8] {
        match self {
            Self::Const(s) => s,
            Self::Mut(s) => s as &[u8],
        }
    }

    /// # Safety
    /// 将其生命周期视为static，但实际的生命周期在执行finish_read_frames()时结束
    pub unsafe fn into_inner_mut_unchecked(self) -> &'static mut [u8] {
        match self {
            Self::Const(_) => panic!("cannot mutate const bytes"),
            Self::Mut(s) => s,
        }
    }

    pub fn as_mut_unchecked(&mut self) -> &mut [u8] {
        match self {
            Self::Const(_) => panic!("cannot mutate const bytes"),
            Self::Mut(s) => s,
        }
    }
}

impl Hash for StaticBytes {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.as_ref().hash(state)
    }
}

impl<B: AsRef<[u8]>> PartialEq<B> for StaticBytes {
    fn eq(&self, other: &B) -> bool {
        self.as_ref() == other.as_ref()
    }
}

impl From<StaticBytes> for Bytes {
    fn from(value: StaticBytes) -> Self {
        Bytes::copy_from_slice(value.as_ref())
    }
}

impl From<&StaticBytes> for Bytes {
    fn from(value: &StaticBytes) -> Self {
        Bytes::copy_from_slice(value.as_ref())
    }
}

impl From<StaticBytes> for BytesMut {
    fn from(value: StaticBytes) -> Self {
        BytesMut::from(value.as_ref())
    }
}

impl From<StaticBytes> for Str {
    fn from(value: StaticBytes) -> Self {
        Str::from(value.as_ref())
    }
}

impl From<&[u8]> for StaticBytes {
    fn from(value: &[u8]) -> Self {
        unsafe { leak_bytes(value) }
    }
}

impl From<&mut [u8]> for StaticBytes {
    fn from(value: &mut [u8]) -> Self {
        unsafe { leak_bytes_mut(value) }
    }
}

impl From<Vec<u8>> for StaticBytes {
    fn from(value: Vec<u8>) -> Self {
        Self::Mut(value.leak())
    }
}

impl Deref for StaticBytes {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

impl AsRef<[u8]> for StaticBytes {
    fn as_ref(&self) -> &[u8] {
        match self {
            Self::Const(s) => s,
            Self::Mut(s) => s,
        }
    }
}

impl Display for StaticBytes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Const(s) => write!(f, "{}", String::from_utf8_lossy(s)),
            Self::Mut(s) => write!(f, "{}", String::from_utf8_lossy(s)),
        }
    }
}

impl mlua::FromLua<'_> for StaticBytes {
    fn from_lua(value: mlua::Value<'_>, _lua: &'_ mlua::Lua) -> mlua::Result<Self> {
        match value {
            mlua::Value::String(s) => Ok(unsafe { leak_bytes(s.as_bytes()) }),
            _ => Err(mlua::Error::FromLuaConversionError {
                from: value.type_name(),
                to: "StaticBytes",
                message: None,
            }),
        }
    }
}

impl mlua::IntoLua<'_> for StaticBytes {
    fn into_lua(self, lua: &mlua::Lua) -> mlua::Result<mlua::Value> {
        lua.create_string(self.as_ref()).map(mlua::Value::String)
    }
}

#[derive(Debug, Eq)]
pub enum StaticStr {
    Const(&'static str),
    Mut(&'static mut str),
}

impl StaticStr {
    /// # Safety
    /// 将其生命周期视为static，但实际的生命周期在执行finish_read_frames()时结束
    pub unsafe fn into_inner(self) -> &'static str {
        match self {
            Self::Const(s) => s,
            Self::Mut(s) => s,
        }
    }

    /// # Safety
    /// 将其生命周期视为static，但实际的生命周期在执行finish_read_frames()时结束
    pub unsafe fn into_inner_mut_unchecked(self) -> &'static mut str {
        match self {
            Self::Const(_) => panic!("cannot mutate const str"),
            Self::Mut(s) => s,
        }
    }

    pub fn as_mut_unchecked(&mut self) -> &mut str {
        match self {
            Self::Const(_) => panic!("cannot mutate const str"),
            Self::Mut(s) => s,
        }
    }
}

impl Hash for StaticStr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.as_ref().hash(state)
    }
}

impl<S: AsRef<str>> PartialEq<S> for StaticStr {
    fn eq(&self, other: &S) -> bool {
        self.as_ref() == other.as_ref()
    }
}

impl From<&str> for StaticStr {
    fn from(s: &str) -> Self {
        unsafe { leak_str(s) }
    }
}

impl From<&mut str> for StaticStr {
    fn from(s: &mut str) -> Self {
        unsafe { leak_str_mut(s) }
    }
}

impl From<String> for StaticStr {
    fn from(s: String) -> Self {
        Self::Mut(s.leak())
    }
}

impl Deref for StaticStr {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

impl AsRef<str> for StaticStr {
    fn as_ref(&self) -> &str {
        match self {
            Self::Const(s) => s,
            Self::Mut(s) => s,
        }
    }
}

impl Display for StaticStr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Const(s) => write!(f, "{}", s),
            Self::Mut(s) => write!(f, "{}", s),
        }
    }
}
