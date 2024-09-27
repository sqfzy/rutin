use crate::{frame::leak_bytes, shared::db::Str, Key};
use bytes::BytesMut;
use equivalent::Equivalent;
use std::{cmp::PartialEq, ops::Deref};

#[derive(Debug, Hash, PartialEq, Eq)]
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

    pub fn into_uppercase<const L: usize>(mut self) -> Uppercase<L> {
        match &mut self {
            Self::Const(s) => {
                if s.len() > L {
                    panic!("buffer too small");
                }

                let mut buf = [0; L];
                buf[..s.len()].copy_from_slice(s);
                buf.make_ascii_uppercase();
                Uppercase::Const(buf)
            }
            Self::Mut(s) => {
                s.make_ascii_uppercase();
                Uppercase::Mut(self)
            }
        }
    }

    pub fn as_mut_unchecked(&mut self) -> &mut [u8] {
        match self {
            Self::Const(_) => panic!("cannot mutate const bytes"),
            Self::Mut(s) => s,
        }
    }
}

impl Equivalent<Key> for StaticBytes {
    fn equivalent(&self, key: &Key) -> bool {
        self.as_ref() == key
    }
}

impl PartialEq<Key> for StaticBytes {
    fn eq(&self, other: &Key) -> bool {
        self.as_ref() == other
    }
}

impl PartialEq<[u8]> for StaticBytes {
    fn eq(&self, other: &[u8]) -> bool {
        self.as_ref() == other
    }
}

impl From<StaticBytes> for Key {
    fn from(val: StaticBytes) -> Self {
        Key::copy_from_slice(val.as_ref())
    }
}

impl From<&StaticBytes> for Key {
    fn from(value: &StaticBytes) -> Self {
        Key::copy_from_slice(value.as_ref())
    }
}

impl From<StaticBytes> for BytesMut {
    fn from(value: StaticBytes) -> Self {
        BytesMut::from(value.as_ref())
    }
}

impl From<StaticBytes> for Str {
    fn from(value: StaticBytes) -> Self {
        Str::from(Key::copy_from_slice(value.as_ref()))
    }
}

impl From<&'static [u8]> for StaticBytes {
    fn from(s: &'static [u8]) -> Self {
        Self::Const(s)
    }
}

impl From<&'static mut [u8]> for StaticBytes {
    fn from(s: &'static mut [u8]) -> Self {
        Self::Mut(s)
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

impl mlua::FromLua<'_> for StaticBytes {
    fn from_lua(value: mlua::Value<'_>, _lua: &'_ mlua::Lua) -> mlua::Result<Self> {
        match value {
            mlua::Value::String(s) => Ok(leak_bytes(s.as_bytes())),
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

pub enum Uppercase<const L: usize> {
    Const([u8; L]),
    Mut(StaticBytes),
}

impl<const L: usize> Deref for Uppercase<L> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

impl<const L: usize> AsRef<[u8]> for Uppercase<L> {
    fn as_ref(&self) -> &[u8] {
        match self {
            Self::Const(s) => s,
            Self::Mut(s) => s.as_ref(),
        }
    }
}

#[derive(Debug, Hash, PartialEq, Eq)]
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

impl PartialEq<str> for StaticStr {
    fn eq(&self, other: &str) -> bool {
        self.as_ref() == other
    }
}

impl From<&'static str> for StaticStr {
    fn from(s: &'static str) -> Self {
        Self::Const(s)
    }
}

impl From<&'static mut str> for StaticStr {
    fn from(s: &'static mut str) -> Self {
        Self::Mut(s)
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
