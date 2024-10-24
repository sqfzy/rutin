use crate::util::StaticBytes;
use bytes::{Bytes, BytesMut};
use equivalent::Equivalent;
use mlua::FromLua;
use serde::Deserialize;
use std::ops::Deref;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Deserialize)]
pub struct Key(Bytes);

// impl Equivalent<Key> for [u8] {
//     fn equivalent(&self, key: &Key) -> bool {
//         self == key.as_ref()
//     }
// }

impl Equivalent<Key> for Vec<u8> {
    fn equivalent(&self, key: &Key) -> bool {
        self.as_slice() == key.as_ref()
    }
}

impl Equivalent<Key> for Bytes {
    fn equivalent(&self, key: &Key) -> bool {
        self.as_ref() == key.as_ref()
    }
}

impl Equivalent<Key> for BytesMut {
    fn equivalent(&self, key: &Key) -> bool {
        self.as_ref() == key.as_ref()
    }
}

impl Equivalent<Key> for StaticBytes {
    fn equivalent(&self, key: &Key) -> bool {
        self.as_ref() == key.as_ref()
    }
}

impl From<&Key> for Key {
    fn from(value: &Key) -> Self {
        value.clone()
    }
}

impl From<&[u8]> for Key {
    fn from(value: &[u8]) -> Self {
        Self(Bytes::copy_from_slice(value))
    }
}

impl From<&Vec<u8>> for Key {
    fn from(value: &Vec<u8>) -> Self {
        Self(Bytes::from(value.clone()))
    }
}

impl From<&Bytes> for Key {
    fn from(value: &Bytes) -> Self {
        Self(value.clone())
    }
}

impl From<&BytesMut> for Key {
    fn from(value: &BytesMut) -> Self {
        Self(value.clone().freeze())
    }
}

impl From<&StaticBytes> for Key {
    fn from(value: &StaticBytes) -> Self {
        Self(Bytes::copy_from_slice(value.as_ref()))
    }
}

impl From<Vec<u8>> for Key {
    fn from(value: Vec<u8>) -> Self {
        Self(Bytes::from(value))
    }
}

impl From<Bytes> for Key {
    fn from(value: Bytes) -> Self {
        Self(value)
    }
}

impl From<BytesMut> for Key {
    fn from(value: BytesMut) -> Self {
        Self(value.freeze())
    }
}

impl From<StaticBytes> for Key {
    fn from(val: StaticBytes) -> Self {
        Bytes::copy_from_slice(val.as_ref()).into()
    }
}

impl From<Key> for Vec<u8> {
    fn from(value: Key) -> Self {
        value.0.to_vec()
    }
}

impl From<Key> for Bytes {
    fn from(value: Key) -> Self {
        value.0
    }
}

impl From<Key> for BytesMut {
    fn from(value: Key) -> Self {
        value.0.into()
    }
}

impl From<Key> for StaticBytes {
    fn from(value: Key) -> Self {
        StaticBytes::from(value.0.as_ref())
    }
}

impl std::borrow::Borrow<[u8]> for Key {
    fn borrow(&self) -> &[u8] {
        self.0.borrow()
    }
}

impl AsRef<[u8]> for Key {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl From<&'static str> for Key {
    fn from(value: &'static str) -> Self {
        Self(Bytes::from_static(value.as_bytes()))
    }
}

impl PartialEq<&[u8]> for Key {
    fn eq(&self, other: &&[u8]) -> bool {
        self.0.eq(other)
    }
}

impl PartialEq<Bytes> for Key {
    fn eq(&self, other: &Bytes) -> bool {
        self.0.eq(other)
    }
}

impl PartialEq<&str> for Key {
    fn eq(&self, other: &&str) -> bool {
        self.0.eq(other.as_bytes())
    }
}

impl Deref for Key {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

// impl FromLua<'_> for Key {
//     fn from_lua(value: mlua::Value<'_>, _: &'_ mlua::Lua) -> mlua::Result<Self> {
//         if let mlua::Value::String(s) = value {
//             Ok(s.as_bytes().into())
//         } else {
//             Err(mlua::Error::FromLuaConversionError {
//                 from: value.type_name(),
//                 to: "Key",
//                 message: None,
//             })
//         }
//     }
// }
